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
- 중단 후 재시작 가능한 체크포인트(`.nsync.batch.state`)

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

### 3.4 Execution Plane

- planner 액션을 다시 `MPI_Alltoallv`로 재분배
  - source 측: copy serve 담당 액션
  - destination 측: 실제 적용 액션
- copy는 destination이 요청하는 pull 기반 프로토콜

### 3.5 Batch/Checkpoint Plane

- `--batch-files` 모드에서 해시 배치 단위로 순차 처리
- destination에 `.nsync.batch.state` 기록
- 옵션/경로/hash가 일치하면 resume, 불일치 시 안전하게 무시

## 4) nsync.c 구현 흐름 (상세 워크플로우)

### 4.1 초기화/옵션 파싱

1. `MPI_Init`, `mfu_init`
2. CLI 옵션 파싱 (`--dryrun`, `--batch-files`, `--delete`, `--contents`, `--role-*` 등)
3. source/destination 인자 검증

### 4.2 역할 할당

1. `nsync_assign_roles()`
2. `auto`면 접근성 검사 결과를 `MPI_Allgather`
3. `map`이면 rank 0에서 파싱 후 `MPI_Bcast`
4. `MPI_Comm_split`으로 `src_comm`, `dst_comm` 생성

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
2. source rank는 요청을 받아 파일을 읽어 chunk 전송
3. destination rank는 파일 생성/쓰기/메타 적용
4. 삭제/디렉토리/링크/메타 업데이트를 타입별 순서로 적용
5. 배치 모드에서는 디렉토리 remove/meta를 deferred 리스트에 모아 final pass에서 정리

### 4.5 배치/재시작

1. `--batch-files N`으로 batch count 계산
2. spool 기반 1회 스캔 후 batch load 처리
3. 배치 성공 시 checkpoint 업데이트
4. 재시작 시 checkpoint header 검증
5. 성공 종료 시 checkpoint finalized 후 파일 제거

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

- `NSYNC_MSG_COPY_REQ` (4100): destination -> source, relpath 요청
- `NSYNC_MSG_COPY_RESP` (4101): source -> destination, open 결과(status)
- `NSYNC_MSG_COPY_DATA_LEN` (4102): source -> destination, chunk 크기
- `NSYNC_MSG_COPY_DATA` (4103): source -> destination, chunk payload

종료 조건은 `chunk_len == 0`입니다.

## 6) 옵션 설명

| 옵션 | 설명 | 구현상 동작 |
|---|---|---|
| `--dryrun` | 변경 없이 계획만 출력 | planner까지 수행, execute 스킵 |
| `-b, --batch-files N` | 배치 단위 처리 | 메모리 상한 제어 + checkpoint/resume |
| `-D, --delete` | destination 초과 파일 삭제 | `only-dst` 경로에 `REMOVE` 생성 |
| `-c, --contents` | 파일 내용 비교 | SHA256 digest 계산/비교 |
| `--bufsize SIZE` | I/O 버퍼 크기 | copy/digest read chunk 크기 |
| `--chunksize SIZE` | 최소 작업 크기 | 내부 chunk 정책 기준값 |
| `--progress N` | 진행 로그 주기(초) | rank 0에서 배치 진행률 + 최근 throughput(볼륨/파일 수) 출력, `0`이면 비활성화 |
| `--role-mode auto|map` | 역할 결정 방식 | 자동 탐지 또는 명시 맵 |
| `--role-map SPEC` | 역할 맵 | 예: `0-1:src,2-3:dst` |
| `--trace` | 디버그 trace | stage별 per-rank trace 출력 |
| `-v` | 상세 로그 | 경고/정보 로그 확대 |
| `-q` | 최소 로그 | 출력 억제 |
| `-h` | 도움말 | 사용법 출력 |

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
  --trace -v \
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

### 8.3 실행 후

1. 요약 로그의 `Metadata diff summary`, `Planned actions`, `Execution completed` 확인
2. `scan error`/`exec error`가 0인지 확인
3. 배치 실행 성공 시 `.nsync.batch.state`가 제거되었는지 확인

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

### 9.4 checkpoint 불일치

- 옵션/경로/hash가 다르면 checkpoint는 자동 무시됨
- 로그에 mismatch 이유가 출력되며 0번 배치부터 재시작

## 10) 현재 구현 범위와 한계

- 구현됨
  - split-mount role model
  - 분산 메타 비교 + MPI copy
  - `--dryrun`, `--delete`, `--contents`
  - `--batch-files` + checkpoint/resume
  - deferred directory finalize

- 현재 한계
  - extreme hash skew에 대한 자동 adaptive split(2차 해시 분할)은 미구현
  - resume 테스트는 실행 시간/환경에 따라 타이밍 민감할 수 있음

---

필요하면 다음 단계로 `nsync.md`에 **운영 체크리스트(사전 점검표/장애시 조치표)**를 추가해 runbook 형태로 더 강화할 수 있습니다.
