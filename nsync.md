# nsync 문서 (구현/아키텍처/운영 가이드)

## 1) 개요

`nsync`는 **split-mount 환경**을 위한 MPI 기반 동기화 도구입니다.

`dsync`와 가장 큰 차이는 다음입니다.

- `dsync`: 한 rank가 source와 destination을 모두 볼 수 있어야 함
- `nsync`: source를 보는 rank 집합과 destination을 보는 rank 집합이 달라도 동작

즉, source 쪽 노드와 destination 쪽 노드가 네트워크(MPI)로만 연결되어 있어도 동작하도록 `nsync.c`가 설계되어 있습니다.

## 2) 문제 배경과 설계 목표

### 해결하려는 운영 문제

- source 노드에는 `/src`만 마운트, destination 노드에는 `/dst`만 마운트
- 단일 노드에서 양쪽 경로 비교/복사가 불가능
- cross-pod/cross-node 환경에서 hang 없이 안정적으로 동작해야 함

### 핵심 설계 목표

- 역할 분리(`src`/`dst`)와 안전한 집단 통신(collective) 동기화
- 메타데이터 분산 비교 + 실행 계획 생성
- 실제 파일 데이터는 `src -> dst`로 MPI 스트리밍 복사
- 대규모 트리에서 메모리 상한을 두는 배치 모드(`--batch-files`)

### 2.1 이번 업데이트의 핵심 포인트

- `--chunksize` 제거
  - `nsync`는 file chunk ownership 기반 도구가 아니므로, 사용자 옵션으로 보이는 `--chunksize`를 제거했습니다.
- copy path 고도화
  - 기존의 단순 `length + payload` blocking 복사 대신, framed protocol + nonblocking MPI + buffer reuse 기반 pipeline으로 변경했습니다.
- sparse file 보존
  - `SEEK_DATA/SEEK_HOLE` 우선, 필요 시 보수적인 zero-hole fallback으로 hole을 유지합니다.
- 큰 파일 owner 재배치
  - planner가 만든 large copy action은 relpath hash만 따르지 않고 byte load를 보고 source/destination owner를 다시 배정합니다.
- source-side credits 추가
  - source rank는 request queue를 두고, destination rank들에 대해 file-level in-flight를 제한/스케줄합니다.
  - 단, 같은 source/destination pair는 의도적으로 직렬화하여 deadlock과 random I/O를 피합니다.

## 3) 아키텍처

아키텍처는 크게 5개 평면으로 나뉩니다.

### 3.1 Role Plane

- `--role-mode auto` 또는 `--role-mode map`으로 rank 역할 결정
- `auto`는 `access()` 기반으로 source read 가능 여부 / destination write 가능 여부를 검사
- `map`은 world rank에 명시 매핑

### 3.2 Metadata Plane

- 각 role rank는 자기 쪽 트리만 스캔
- 메타데이터를 `hash(relpath) % world_size` owner로 `MPI_Alltoallv` 재분배
- owner rank에서 source/destination 레코드를 모아 비교 가능 상태로 정렬

### 3.3 Planner Plane

- 경로 단위로 `COPY`, `MKDIR`, `REMOVE`, `SYMLINK_UPDATE`, `META_UPDATE` 액션 생성
- 각 경로에 대해 `src_owner_world`, `dst_owner_world`를 미리 계산해 실행 경로 확정
- large file(`COPY`)는 planner 직후 별도 rebalance pass를 통해 byte-aware owner 재배치 가능

### 3.4 Execution Plane

- planner 액션을 다시 `MPI_Alltoallv`로 재분배
  - source 측: copy serve 담당 액션
  - destination 측: 실제 적용 액션
- copy는 destination이 요청하는 pull 기반 프로토콜
- source는 요청을 queue한 뒤 source-side credit과 destination rank별 활성 상태를 보고 전송 시작
- destination은 copy 파일들을 크기 내림차순으로 launch하여 큰 파일 head-of-line을 줄임

### 3.5 Batch/Spool Plane

- `--batch-files` 모드에서 해시 배치 단위로 순차 처리
- spool 기반 1회 스캔 후 batch load 처리로 메모리 피크를 낮춤

## 4) nsync.c 구현 흐름 (상세 워크플로우)

### 4.1 초기화/옵션 파싱

1. `MPI_Init`, `mfu_init`
2. CLI 옵션 파싱 (`--dryrun`, `--batch-files`, `--delete`, `--contents`, `--role-*` 등)
3. source/destination 인자 검증

### 4.2 역할 할당

1. `nsync_assign_roles()`
2. `auto`면 접근성 검사 결과를 `MPI_Allgather`
3. `map`이면 launcher-console 로그 rank에서 파싱 후 `MPI_Bcast`
4. `MPI_Comm_split`으로 `src_comm`, `dst_comm` 생성
5. 기본 로그에서 rank/role과 함께 hostname을 출력

### 4.3 스캔/재분배/플래닝

1. role별 로컬 스캔 (`nsync_scan_role_path_filtered`)
2. 메타데이터 owner 재분배 (`nsync_metadata_redistribute`, `MPI_Alltoallv`)
3. owner에서 경로별 비교/계획 (`nsync_plan_planner_records`)
4. 비교 정책
   - 일반 파일: 기본 `size + mtime_nsec`
   - `--contents`: SHA256 digest 비교
   - 링크: target 비교
   - 디렉토리: 메타데이터 비교

### 4.4 실행 단계 (Phase 4/5 구현)

1. planner 액션 재분배 (`nsync_actions_redistribute`)
2. large copy action이 있으면 `nsync_rebalance_large_copy_owners()`로 source/destination owner를 재조정
3. destination rank는 `COPY` 액션을 분리해 큰 파일부터 launch
4. source rank는 요청을 queue하고 source-side credit 안에서 파일 전송 시작
5. 파일 데이터는 `file_id + offset + payload/logical length + flags` 프레임으로 송수신
6. destination rank는 `pwrite()` 기반으로 offset write, sparse hole frame은 실제 write 없이 건너뜀
7. 삭제/디렉토리/링크/메타 업데이트를 타입별 순서로 적용
8. 배치 모드에서는 디렉토리 remove/meta를 deferred 리스트에 모아 final pass에서 정리

### 4.5 배치 처리

1. `--batch-files N`으로 batch count 계산
2. spool 기반 1회 스캔 후 batch load 처리
3. 배치별 planner/execute를 순차 처리

### 4.6 종료/요약

1. 전역 reduce로 compare/action/error 집계
2. 실행 요약 로그 출력
3. 메타데이터 best-effort/실패 통계 출력
4. `scan error` 또는 `exec error`가 있으면 non-zero 종료

## 5) 통신/프로토콜 디테일

### 5.1 주요 collective

- `MPI_Bcast`: role map/상태 전파
- `MPI_Allgather`: auto role capability 수집
- `MPI_Allreduce`: 에러/통계 합산, stage 동기화
- `MPI_Alltoall`, `MPI_Alltoallv`: 메타/액션 재분배

### 5.2 copy 데이터 메시지

`nsync.c`의 copy 메시지 태그:

- `NSYNC_MSG_COPY_REQ` (4100): destination -> source, `file_id + relpath` 요청
- `NSYNC_MSG_COPY_RESP` (4101): source -> destination, `file_id + open/status` 응답
- `NSYNC_MSG_COPY_DATA_LEN` (4102): source -> destination, framed 데이터 메시지

데이터 프레임은 다음 필드를 가집니다.

- `file_id`
- `offset`
- `payload length`
- `logical length`
- `flags`

`flags`는 `EOF/HOLE/ERROR`를 표현합니다.

- data frame:
  `payload length == logical length`
- hole frame:
  `payload length == 0`, `logical length > 0`
- end frame:
  `payload length == 0`, `logical length == 0`, `EOF`

복사 경로는 nonblocking MPI + double-buffer pipeline으로 읽기/전송/쓰기 오버랩이 가능하고,
destination은 `pwrite()`로 offset write를 수행합니다.

### 5.3 sparse file 처리

- source:
  `SEEK_DATA/SEEK_HOLE`를 우선 사용해 실제 data extent와 hole extent를 구분
- fallback:
  source 파일이 실제 sparse로 보일 때만 zero-filled chunk를 hole frame으로 대체
- destination:
  먼저 최종 크기로 `ftruncate()`한 뒤 hole frame은 write 없이 유지

즉, logical size는 유지하면서 destination에서 hole이 materialize되지 않도록 설계했습니다.

### 5.4 large file owner rebalance

- 기본 planner owner는 relpath hash 기반입니다.
- 단, 큰 `COPY` 액션은 planner 직후 다시 수집해 source/destination별 누적 byte load를 기준으로 greedy 재배치합니다.
- 기본 threshold는 `64 MiB`이고, `NSYNC_LARGE_FILE_THRESHOLD` 환경변수로 override할 수 있습니다.

### 5.5 credit 모델

- source rank는 request를 바로 모두 시작하지 않고 queue합니다.
- 동시에 active 상태가 될 수 있는 file transfer 수는 source-side credit으로 제한됩니다.
- 기본 credit은 `2`이고, `NSYNC_COPY_FILE_CREDITS` 환경변수로 override할 수 있습니다.
- 같은 source/destination pair에는 동시에 하나의 file transfer만 허용합니다.
  - 이 제약은 same-pair deadlock과 random I/O 증가를 막기 위한 의도적 선택입니다.

## 6) 옵션 설명

| 옵션 | 설명 | 구현상 동작 |
|---|---|---|
| `--dryrun` | 변경 없이 계획만 출력 | planner까지 수행, execute 스킵 |
| `-b, --batch-files N` | 배치 단위 처리 | 메모리 상한 제어 + spool 기반 배치 로드 |
| `-D, --delete` | destination 초과 파일 삭제 | `only-dst` 경로에 `REMOVE` 생성 |
| `-c, --contents` | 파일 내용 비교 | SHA256 digest 계산/비교 |
| `--bufsize SIZE` | I/O 버퍼 크기 | copy/digest read chunk 크기 |
| `--imbalance-threshold R` | batch imbalance ratio 임계값 | 진단 경고 출력 기준, 기본값 `3.0` |
| `--role-mode auto|map` | 역할 결정 방식 | 자동 탐지 또는 명시 맵 |
| `--role-map SPEC` | 역할 맵 | 예: `0-1:src,2-3:dst` |
| `--trace` | 디버그 trace | stage별 per-rank trace 출력 |
| `-q` | 최소 로그 | 출력 억제 |
| `-h` | 도움말 | 사용법 출력 |

진행 로그는 launcher-console 로그 rank에서 **batch 완료 시마다 항상 출력**됩니다 (`-q` 제외).

참고:

- `nsync`는 더 이상 `--chunksize`를 지원하지 않습니다.
- large-file / credit 동작은 현재 환경변수(`NSYNC_LARGE_FILE_THRESHOLD`, `NSYNC_COPY_FILE_CREDITS`)로만 조정됩니다.

## 7) 실행 예제

### 7.1 단일 호스트에서 split 역할 에뮬레이션

```bash
mpirun -np 2 nsync \
  --role-mode map --role-map 0:src,1:dst \
  /tmp/src /tmp/dst
```

### 7.2 실제 cross-pod (source/destination 분리)

```bash
mpirun --allow-run-as-root \
  --prefix /usr/local/openmpi-4.1.8 \
  --host 10.244.0.29,10.244.0.30 \
  -np 2 \
  /usr/local/openmpi-4.1.8/mpifileutils/bin/nsync \
  --role-mode map --role-map 0:src,1:dst \
  /src /dst
```

### 7.3 dryrun + delete 계획 확인

```bash
mpirun -np 2 nsync \
  --role-mode map --role-map 0:src,1:dst \
  --dryrun --delete \
  /src /dst
```

### 7.4 same-size 파일을 내용 기준으로 동기화

```bash
mpirun -np 2 nsync \
  --role-mode map --role-map 0:src,1:dst \
  --contents \
  /src /dst
```

### 7.5 대규모 트리 배치 처리

```bash
mpirun -np 8 nsync \
  --batch-files 200000 \
  --delete \
  /src /dst
```

### 7.6 디버깅(trace) 실행

```bash
mpirun -np 2 nsync \
  --role-mode map --role-map 0:src,1:dst \
  --trace \
  /src /dst
```

## 8) 운영 워크플로우 (권장)

### 8.1 배포 전

1. source/destination 노드 모두 동일한 `nsync` 바이너리 설치
2. OpenMPI 런타임 경로(`--prefix`, `PATH`, `LD_LIBRARY_PATH`) 정합성 확인
3. `--role-mode map`으로 기본 통신/마운트 가정 검증

### 8.2 실행 시

1. 초기에는 `--dryrun`으로 diff/액션 확인
2. 대용량은 `--batch-files` 사용
3. 필요 시 `--trace`로 collective 단계 추적
4. 큰 파일 편중이 의심되면 `NSYNC_LARGE_FILE_THRESHOLD`를 조정해 owner rebalance 범위를 검증
5. source 병렬 파일 수를 조정해야 하면 `NSYNC_COPY_FILE_CREDITS`를 사용

### 8.3 실행 후

1. 요약 로그의 `Metadata diff summary`, `Planned actions`, `Execution completed` 확인
2. `scan error`/`exec error`가 0인지 확인
3. 필요 시 source/destination 샘플 파일 해시 검증

## 9) 장애 대응/트러블슈팅

### 9.1 `orted: command not found`

- `mpirun --prefix <openmpi_prefix>` 사용
- 원격 pod의 PATH/LD_LIBRARY_PATH 확인

### 9.2 auto role 모드 실패

- 한 rank가 source/destination 모두 접근 가능하면 auto 모드가 모호해짐
- 이 경우 `--role-mode map --role-map ...` 사용 권장

### 9.3 scan error 발생

- split-mount 환경에서 source pod에 `/dst`를 만들거나 반대로 접근하면 에러 가능
- source는 `/src`, destination은 `/dst`만 보인다는 전제를 지키고 실행

### 9.4 배치 처리 주의

- `--batch-files` 값을 너무 작게 잡으면 batch 수가 커지고 메타 오버헤드가 증가
- imbalance 경고가 많으면 `--batch-files`를 일방적으로 키우기보다 값을 조정해 batch-to-rank 분포를 확인하는 것이 안전

### 9.5 `--chunksize`가 사라진 이유

- `nsync`는 `dsync`처럼 file chunk를 여러 rank가 직접 나눠 쓰는 구조가 아닙니다.
- 현재 성능 개선은 chunk ownership이 아니라
  - file-level pipeline
  - sparse-aware framed transfer
  - large-file owner rebalance
  - source-side credits
  에서 가져갑니다.

## 10) 현재 구현 범위와 한계

- 구현됨
  - split-mount role model
  - 분산 메타 비교 + MPI copy
  - `--dryrun`, `--delete`, `--contents`
  - `--batch-files` + metadata spool
  - deferred directory finalize
  - sparse file preservation
  - large-file size-aware owner rebalance
  - nonblocking per-file pipeline
  - source-side credit / request queue

- 현재 한계
  - `dsync`식 file chunk parallelization은 미구현
  - 같은 source/destination pair 내부에서는 file transfer를 직렬화
  - large-file rebalance는 greedy byte balancing이며, topology-aware/network-aware scheduling은 아직 없음
  - extreme hash skew에 대한 자동 adaptive split(2차 해시 분할)은 미구현

## 11) 기존 대비 무엇이 바뀌었나

기존 구현 기준:

- `COPY`는 사실상 file-level blocking stream
- sparse file이 destination에서 materialize될 수 있음
- large file owner는 relpath hash에만 의존
- source rank는 요청을 바로 시작하는 단순 serve loop
- 사용자 입장에서는 `--chunksize` 옵션이 보였지만 실제 설계 방향과 맞지 않았음

업데이트 후:

- `--chunksize` 제거
- framed protocol + nonblocking MPI pipeline으로 copy path 변경
- sparse hole preservation 추가
- large `COPY` 액션에 size-aware owner rebalance 추가
- source-side request queue / credit 도입
- 같은 source/destination pair는 안전하게 직렬화, 여러 destination rank에 대해서는 file-level overlap 허용

즉, `nsync`는 여전히 split-mount 철학을 유지하면서도, 큰 파일과 많은 파일 workload에서
`dsync`의 chunk 분할 없이 더 높은 처리량과 더 안정적인 동작을 목표로 업데이트됐습니다.
