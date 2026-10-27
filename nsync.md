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
- 메타데이터 적용은 항목별로 owner(`chown`/`lchown`) → 권한(`chmod`, symlink 제외) → 타임스탬프(`utimensat`) 순으로 수행하며, **source의 atime·mtime을 모두 destination에 보존**한다(symlink는 `AT_SYMLINK_NOFOLLOW`로 링크 자체에 적용)
- 디렉토리 모드 finalize는 **가장 깊은 경로부터(deepest-first)** 적용하고 깊이 단계마다 destination rank 간 `MPI_Barrier`를 둔다 — 부모와 자식이 서로 다른 rank 소유여도, 소유자 실행권이 없는 제한적 디렉토리 모드를 비권한으로 적용할 때 자식 finalize가 `EACCES`로 실패해 과대 권한으로 남는 일을 방지
- destination 최상위 디렉토리 메타데이터는 부분 오류가 있어도 항상 마지막에 finalize한다(트리 일관성 유지)

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
7. 삭제/디렉토리/링크/메타 업데이트를 타입별 순서로 적용 — 모든 destination rank가 자기 타입 교체 `REMOVE`를 끝낸 뒤
   `dst_comm` barrier를 지나야 어떤 rank도 생성(`MKDIR`/`SYMLINK`/`COPY`)을 시작한다. `--delete`의 destination 전용
   `REMOVE`는 barrier 뒤, 그 rank의 복사 앞에 돈다(§13.2)
8. 배치 모드에서는 디렉토리 remove/meta를 deferred 리스트에 모아 final pass에서 정리
9. 실패한 항목은 항목당 1건으로 세고 경로·errno와 함께 기록한다(§13.4)

### 4.5 배치 처리

1. `--batch-files N`으로 batch count 계산 (`0` = 배칭 끔, 기본값과 같음)
2. spool 기반 1회 스캔 후 batch load 처리
3. 배치가 2개 이상이면 첫 배치 전에 타입 교체 선처리(§13.1)
4. 배치별 planner/execute를 순차 처리 — `--max-errors`·꽉 찬 destination이면 남은 배치를 버린다(§13.4)

### 4.6 종료/요약

1. 전역 reduce로 compare/action/error 집계
2. 실패 항목 목록(경로순, 최대 65,536건)을 요약 블록 **앞**에 출력(§13.4)
3. 실행 요약 로그 출력
4. 메타데이터 best-effort/실패 통계 출력
5. `scan error` 또는 `exec error`가 있으면 non-zero 종료

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

`flags`는 `EOF/HOLE/ERROR`를 표현합니다. `EOF|ERROR` 프레임의 `logical length`(EOF에서는 쓰지 않는 칸)에는
source 쪽 읽기 실패의 errno가 실린다 — destination이 그 항목을 경로·errno와 함께 한 번 기록한다.

REQ의 `file_id` bit 63은 **취소**다(§13.3): destination이 쓰지 않을 항목(교체가 거부됐거나 destination 파일을 못 연 경우,
`--max-errors`·꽉 찬 destination으로 멈춘 뒤)은 REQ를 보내되 이 비트를 세우고, source는 파일을 열거나 읽지 않고
`ECANCELED`로 바로 답한다. source가 기다리는 REQ 수(=자기 COPY 수)는 그대로 맞는다.

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
| `-b, --batch-files N` | 배치 단위 처리 (`0` = 배칭 끔, 기본) | 메모리 상한 제어 + spool 기반 배치 로드 |
| `--max-errors N` | 실패 항목 N건이면 중단 (기본 `65000`, `0` = 끝까지) | 배치 사이(전체 실패) + 배치 안(rank별 몫, 실행 실패만), §13.4 |
| `-D, --delete` | destination 초과 파일 삭제 | `only-dst` 경로에 `REMOVE` 생성 |
| `-c, --contents` | 파일 내용 비교 | SHA256 digest 계산/비교 |
| `--bufsize SIZE` | I/O 버퍼 크기 | copy/digest read chunk 크기 |
| `--open-noatime` | source 파일의 접근 시각을 바꾸지 않고 읽음 | 복사·`--contents` 읽기를 `O_NOATIME`으로 연다. 커널이 소유자(또는 CAP_FOWNER)에게만 허용하므로 실행 사용자 소유가 아닌 파일은 `EPERM` 뒤 그냥 다시 열어 복사한다(GNU tar·cp와 같음, 그 수는 끝에 `--open-noatime: N source file(s) not owned …`). destination 쓰기에는 쓰지 않는다 |
| `-s, --direct` | 호환용(효과 없음) | 받기만 하고 `O_DIRECT`로 열지 않는다 — nsync의 프레임 I/O는 정렬되지 않은 버퍼·길이라 `O_DIRECT`면 깨진다. 주면 `--direct has no effect in nsync` 한 줄 |
| `--imbalance-threshold R` | batch imbalance ratio 임계값 | 진단 경고 출력 기준, 기본값 `3.0` |
| `--role-mode auto|map` | 역할 결정 방식 | 자동 탐지 또는 명시 맵 |
| `--role-map SPEC` | 역할 맵 | 예: `0-1:src,2-3:dst` |
| `--trace` | 디버그 trace | stage별 per-rank trace 출력 |
| `--chown USER:GROUP` | destination 소유권 설정 | source가 아니라 지정 uid/gid로 override (§12) |
| `--chmod SPEC` | destination 권한 설정 | octal 또는 `D<oct>,F<oct>`로 dir/file 분리 (§12) |
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
  - `--direct`는 호환용으로 받기만 한다(`O_DIRECT` 미사용 — 정렬 I/O 미구현). 빌드가 `-std=gnu99`만이라 nsync.c 맨 위에서
    `_GNU_SOURCE`를 정의한다(2026-10-27 전에는 없어서 `O_NOATIME`·`O_PATH`가 정의되지 않았고 `--open-noatime`이 무효,
    경로 걷기의 디렉터리 fd가 읽기 전용이었다 — 이제 `O_PATH`라 조상 디렉터리에 검색 권한만 있으면 된다).

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

## 12) `--chown` / `--chmod` (destination 소유권·권한 override)

기본적으로 `nsync`는 source의 ownership·permission·타임스탬프(atime·mtime)를 destination에 그대로 적용한다.
`--chown` / `--chmod`를 주면, **대신 사용자가 지정한 값으로 destination 항목의 소유권·권한을 설정**한다.
override는 **source 쪽 스캔 레코드에만** 적용되고 destination 디스크 데이터에 반영되므로, source는 변경되지 않는다.

### 12.1 `--chown USER:GROUP`

- 이름 또는 숫자 id (`alice:developers` / `1001:1002`), 부분 지정(`alice`=uid만, `:developers`=gid만) 지원.
- 신규 항목, 기존이지만 소유권이 다른 항목, **destination 최상위 디렉토리**, symlink(lchown) 모두 적용된다.
- 이름은 rank 0에서 한 번 조회 후 broadcast하여 전 rank가 동일 id를 사용한다(split 환경의 NSS/LDAP 일관성). 알 수 없는 이름이면 오류 종료.
- 임의 소유자로 변경하려면 destination 쪽 rank가 root(또는 `CAP_CHOWN`)여야 한다. 비권한이면 chown은 건너뛰고 경고를 남기며 정상 종료한다.

### 12.2 `--chmod SPEC`

- **octal 절대값**: `--chmod 0750`(dir·file 동일) 또는 `--chmod D0755,F0644`(디렉토리/파일 분리, 권장). 한쪽만 줘도 된다.
- 특수비트(`4000/2000/1000`)는 4자리 octal로 지정 가능. **symlink는 chmod하지 않는다**(타입 비트 보존).
- **destination 최상위 디렉토리**의 모드도 함께 설정된다(이 디렉토리는 별도 경로로 복원되므로 내부적으로 동일 override를 적용).
- 제한적 디렉토리 모드(예 `D0500`)를 줘도, nsync는 디렉토리를 임시로 쓰기 가능하게 만들어 자식을 모두 채운 뒤, **가장 깊은 경로부터 깊이별 barrier**로 지정 모드를 마무리한다. 따라서 소유자 실행권이 없는 디렉토리 모드를 비권한으로 적용해도 서브트리가 정상 동기화된다.
- 파싱은 로컬·결정적이라 broadcast가 필요 없다. 잘못된 spec은 오류 종료.

### 12.3 예제

```bash
# split-mount: source(w1-3) -> destination(w4-5), 소유권을 지정값으로 (auto role)
mpirun --hostfile hf -np 10 nsync --chown dataadmin:datagrp /cephfs-third/job /cephfs-secondary/job

# 디렉토리 0750 / 파일 0640으로 권한 통일
mpirun --hostfile hf -np 10 nsync --chmod D0750,F0640 /cephfs-third/job /cephfs-secondary/job

# 소유권 + 권한 동시 (role-map 모드)
mpirun -np 2 nsync --role-mode map --role-map 0:src,1:dst \
  --chown 1001:1001 --chmod D0755,F0644 /src /dst
```

> 참고: auto role 모드에서는 destination 디렉토리가 미리 존재해야 역할 판별이 된다(일반 nsync 사용법과 동일).

## 13) 타입 교체·실패 처리 (2026-10-27)

### 13.1 비디렉터리 → 디렉터리 (심링크·파일·FIFO가 있던 자리에 source 디렉터리)

- 배치는 경로마다 자기 전체 경로의 해시로 정해진다. 그래서 `d`를 디렉터리로 바꾸는 `REMOVE+MKDIR`(그 `d`의 배치)보다
  `d/…` 자식의 복사(다른 배치)가 먼저 돌 수 있고, 자식은 O_NOFOLLOW 걷기에서 남은 심링크·파일을 만나 실패한다.
- **선처리**(배치 2개 이상, `--dryrun` 아님): source 디렉터리 목록(배치 재분할 때 함께 기록 — 추가 읽기 없음)을 각 경로의
  부모를 스캔한 destination rank에 보내고, 그 rank는 스캔 때 본 비디렉터리를 담은 블룸 필터(16 MiB)에 걸린 경로만
  확인해 `unlink + mkdir 0700`으로 바꾼다. 모든 rank가 barrier를 지난 뒤 첫 배치가 시작된다. 경로와 함께 source 디렉터리의
  메타(모드·소유·시각, `--chown/--chmod` 반영)를 보내고, 바꾼 자리는 그 메타를 지연 디렉터리 메타로 등록한다 — 그래서 실행이
  그 경로의 배치 전에 멈춰도 root 0700 빈 자리가 아니라 source 메타를 가진 (빈) 디렉터리로 남는다. 그 경로의 배치가 돌면
  `MKDIR`(EEXIST 디렉터리)이 같은 메타를 한 번 더 등록할 뿐이다. 변경이 없는 재동기화에서는 탐침이 거의 0이다
  (로그 `Type-change pre-pass: source directories N, probed P, replaced R, failed F`).
- 선처리의 교체 실패는 즉시 로그하지만 실패 항목으로는 **그 경로의 배치가 돌지 않았을 때만** 센다 — 배치가 돌면 그 배치의
  `REMOVE`가 같은 항목을 만나 보고한다(항목 하나 = 실패 1건). 조기 중단 뒤에는 돌지 않은 배치 몫의 교체 수를
  `Type-change pre-pass: N stale entries in batches that did not run were already replaced …` 줄로 따로 알린다
  (`Planned actions` 줄에는 넣지 않는다 — 그 배치는 계획되지 않았다). 선처리의 `ENOSPC/EDQUOT/EROFS`는 첫 배치를 시작부터
  멈추므로(그 경로의 `REMOVE`가 보고할 기회가 없다) 즉시 실패로 센다.
- 배치가 1개면 선처리가 필요 없다: 한 rank 안에서는 `REMOVE < MKDIR < … < COPY` 순서이고, rank 사이는 §13.2 barrier.

### 13.2 destination rank 사이 순서

- 같은 배치에서도 `REMOVE`(경로 소유 rank)와 자식 생성(자식 경로 소유 rank)은 다른 rank일 수 있다.
  모든 destination rank가 자기 타입 교체 `REMOVE`를 끝낸 뒤 `MPI_Barrier(dst_comm)`을 지나야 생성을 시작한다(배치당 1회).
  copy REQ는 barrier 뒤에만 나가므로 source rank는 이 barrier에 끼지 않는다.
- `--delete`의 destination 전용 `REMOVE`(그 경로와 아래에 생성할 것이 없다)는 barrier 앞에 두지 않는다 — 큰 하위 트리 삭제가
  다른 rank의 생성·복사를 붙잡지 않는다. 그 rank의 복사보다는 먼저 돌아, 비운 공간을 복사가 쓴다. 꽉 찬 destination
  (`ENOSPC/EDQUOT`)으로 인한 정지는 이 삭제를 멈추지 않는다(멈추면 `--delete` 실행이 꽉 찬 destination을 영영 비우지 못한다);
  읽기 전용(`EROFS`)·`--max-errors` 몫 소진은 멈춘다. 배치가 여럿이면 정지 뒤 배치의 삭제는 돌지 않는다(다음 실행이 이어 간다).
  타입 교체 `REMOVE`는 계획 단계에서 표시한다(`REMOVE`의 쓰지 않던 size 필드).
- 큰 `COPY` 재배치(`NSYNC_LARGE_FILE_THRESHOLD`)는 타입 교체 `COPY`(바로 앞이 같은 경로의 `REMOVE`)를 옮기지 않는다.

### 13.3 디렉터리 → 비디렉터리, 제거 규칙

- **`--delete` 없이는 비어 있지 않은 디렉터리를 바꾸지 않는다**(rsync·dsync와 같음): 그 디렉터리는 그대로 두고
  `replace-dir … Directory not empty` 실패 1건으로 보고하며, 같은 경로의 대체(`COPY`는 취소 REQ, `SYMLINK`)는 하지 않는다.
  빈 디렉터리는 바꾼다. `--delete`면 하위까지 지우고 바꾼다.
- `REMOVE`는 계획 시점의 destination 타입일 때만 지운다(그 사이 바뀐 항목 — 예: 선처리가 이미 디렉터리로 바꾼 `d` — 은 둔다).
  부모가 비디렉터리(파일·심링크 — 따라가지 않음)이면 「트리 안에 없음」으로 끝난다.
- 복사가 실패하면 그 전송이 직접 만든·자른 **일반 파일만** 지운다(이전에는 실패한 경로를 재귀 삭제했다).
- 기존 파일은 열 때 자르지 않는다(`O_TRUNC` 없음). source가 스트림을 보내기 시작한 첫 프레임에서(프레임이 없는 빈 파일은
  마무리 때) 자른다 — 그래서 source를 열거나 읽지 못한 `COPY`는 이전 사본을 그대로 남긴다(이전에는 자른 뒤 지웠다).
  자르기 전에 기존 파일의 set-ID 비트와 새 모드가 주지 않는 접근을 뺀다(`old & new & 0777`, 필요할 때만 `fchmod`).

### 13.4 실패 기록과 `--max-errors`

- 항목 하나의 실패는 1건이다: 복사 실패는 destination rank가 단독으로 센다(source의 열기·읽기 실패는 RESP 상태·`EOF|ERROR`
  errno로 넘어옴), 메타데이터 하위 연산 여러 개가 실패해도 1건, 무시 가능한 메타 오류(EPERM 등)는 세지 않는다.
- rank당 앞 16건은 즉시 `Failed <op> \`path\`: <strerror> (errno N)`로, 실행 끝에는 log rank가 모은 전체 목록을 경로순으로
  `Failed items: X (copy a, …); listed below: Y` 다음 줄마다 출력한다. 경로의 제어 문자·`\`·`:`·`=`는 `\xHH`로 쓴다
  (파일 이름으로 `Planned actions:`·`copied-volume=` 같은 요약 줄을 흉내 낼 수 없게).
- 기록 1건 = 512 B 고정 슬롯, 출력 줄도 512 B 이하. 메모리 32 MiB / 512 B = 65,536, 로그 32 MiB / 512 B = 65,536 →
  `--max-errors` 기본 **65,000**. rank당 보관 상한과 log rank가 모으는 목록 상한도 65,536건(32 MiB)이다.
- 판정:
  - 배치 경계: 지금까지의 **모든** 실패(스캔 오류 포함) 누계가 N 이상이면 남은 배치를 버린다
    (`Stopping after batch k/n: X failed item(s) (scan S, other E) reached --max-errors N`).
  - 배치 안(DMS 기본은 배치 하나): destination rank마다 「남은 몫 ÷ destination rank 수(최소 1)」를 나눠 갖고, 이번 배치의
    자기 **실행 실패**가 그 몫에 닿으면 새 작업을 시작하지 않는다(남은 `COPY`는 취소 REQ). 항목은 경로 해시로 rank에 고르게
    퍼지므로 몫이 거의 같이 차서 합계 N 근처에서 멈추고, 넘어도 `max(N, destination rank 수)`를 넘지 않는다
    (이전에는 rank마다 남은 예산 전체를 써서 「rank 수 × N」까지 갔다). 실패가 한 rank에 몰리면 그 rank가 일찍 멈출 수 있다.
  - 스캔 오류는 배치 안 몫을 쓰지 않는다: 어떤 작업보다 먼저 일어나므로, 쓰게 하면 실패하지 않은 배치 전체가 시작도 못 하고
    취소된다(이전에는 spool 모드에서만 그랬다 — `--batch-files 0`과 `1000000`이 같은 입력에서 다르게 돌았다). 두 모드가 같다.
  - `ENOSPC/EDQUOT/EROFS`는 즉시 같은 방식으로 멈추고 그 배치 뒤 전체를 중단한다. 시도하지 않은 작업 수는
    `Not attempted after a stop: N`으로 보인다(같은 경로의 `REMOVE` 실패로 막힌 작업은 이미 보고됐으므로 넣지 않는다).
    실패가 1건이라도 있거나, 실패 없이도 멈춰서 시도하지 않은 작업이 남았으면 rc 1(`Execution stopped early: N action(s)
    not attempted`).
- rank당 보관 상한은 `max(N, 1024)`(최대 65,536)이다 — 작은 N에서도 멈추는 순간 진행 중이던 실패까지 목록에 남는다.
  log rank는 자기 기록 배열을 늘려 그 자리에 모은다(`MPI_IN_PLACE`) — 자기 것 + 사본으로 64 MiB가 되지 않는다.
  보관 메모리는 rank마다 따로다(노드당 rank 수 × 최대 32 MiB).
- source 스캔에 오류가 있으면(읽을 수 없는 하위 트리, 목록 도중 오류) 그 실행에서는 `--delete`를 끈다
  (dsync와 같음 — 안 보인 source 항목의 destination 사본이 「destination 전용」으로 지워지지 않게).
- source의 FIFO·소켓·장치는 계획 단계에서 **건너뛴다**: 아무 작업도 만들지 않고, 그 이름의 destination 항목은 `--delete`여도
  그대로 둔다(source에 그 이름의 항목이 있으므로). rsync(`--specials` 없음)처럼 실패로 세지 않고 rank당 앞 16건을
  `Skipping \`path\`: a FIFO, socket or device in the source is not copied`로, 끝에 `Skipped N special file(s) …`로 알린다.
  이전에는 `REMOVE+COPY`로 계획돼 destination 파일을 지운 뒤 복사가 `ENOTSUP`으로 실패했다(그 전에는 FIFO 열기에서 무한 대기).

### 13.5 복사 루프 비용

- destination은 source별 큐에서 「source당 진행 중 실전송 1건」만 훑고, `file_id = COPY 인덱스 + 1`로 O(1) 조회한다.
  source는 credit 크기의 전송 풀과 destination 수 크기의 대기 큐만 훑는다. 이전에는 배치의 COPY 수에 비례하는 배열을
  매 반복 전부 훑어 한 배치 안에서 O(n²)이었다(작은 파일 2만 개·dst 1: 12.5 s → 1.1 s, 4만 개: 67.8 s → 1.6 s).
- 수신 슬롯은 「source 수 × 파이프라인 깊이(4)」개다 — source당 실전송이 1건이라 그 이상은 메모리만 잡는다(이전에는
  credit 8배를 더 곱해 bufsize 4 MiB·source 2이면 destination rank당 256 MiB를 미리 잡았다 → 32 MiB). source별 수신 중 프레임
  수를 세어 전송 마무리 판정도 O(1)이다.
- 목적지 부모 디렉터리는 `openat2` 한 번(없으면 「열기 먼저, 없을 때만 mkdir」 걷기 — 단계당 1 syscall), 메타는 전송 fd로
  적용한다(경로를 다시 걷지 않는다). 스캔은 디렉터리 fd 기준 `fstatat`이라 항목마다 전체 경로를 다시 해석하지 않는다.

### 13.6 경로 안전 — 어떤 심링크도 따라가지 않는다

- 스캔·source 읽기·destination 쓰기는 모두 root fd 아래에서 경로를 푼다. Linux 5.6+면 `openat2(RESOLVE_BENEATH |
  RESOLVE_NO_SYMLINKS)` 한 번으로 커널이 모든 심링크와 탈출을 거부하고, `openat2`가 없거나 막혀 있으면(ENOSYS, seccomp)
  구성 요소마다 `O_NOFOLLOW`로 걷는다. root 경로 자체는 호출자가 정한 것이라 한 번 보통으로 푼다(root가 심링크인 경우의
  정책은 아직 정하지 않았다 — 리뷰 #4).
- 스캔은 디렉터리를 root fd 아래에서 열어(`fdopendir`) 항목을 그 fd 기준으로 `fstatat`·`readlinkat`·`openat`(--contents)
  한다. 부모를 나열한 뒤 그 디렉터리가 심링크로 바뀌면 거기서 거부된다(이전에는 `opendir(전체 경로)`로 링크 너머를 나열했다).
- source 파일은 source root fd 아래에서 연다. 스캔과 읽기 사이에 디렉터리가 심링크로 바뀌어도 root 실행이 링크 너머
  (예: `/etc`)의 파일을 destination으로 복사하지 않는다 — 그 항목은 `ELOOP` 복사 실패로 보고된다.
- destination 파일은 `O_NONBLOCK|O_NOCTTY`로 열고 일반 파일이 아니면 `ENOTSUP`으로 거부한다 — 그 이름에 끼어든 FIFO가 실행
  전체를 멈추지 않는다. 새 파일은 `O_EXCL`로 **소유자 비트만**(`(mode & 0700) | 0600`, set-ID 없음) 만들고, 데이터를 다 쓴 뒤
  close 전에 fd로 소유자 → 모드 → 시각 순으로 적용한다 — root 실행이 쓰는 동안 root 소유 setuid 파일이 생기는 틈이 없다.
- 소유자를 줄 수 없으면(비 root의 chown EPERM 등) set-ID 비트를 뺀다(cp -p와 같음): 실제 소유자·그룹이 의도와 다르면
  S_ISUID/S_ISGID 없이 `chmod`한다. 디렉터리는 S_ISGID(그룹 상속)를 유지한다.
