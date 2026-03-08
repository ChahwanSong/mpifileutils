# `nsync` distributed walk 업데이트 계획

## 1. 목적

현재 `nsync`의 가장 큰 구조적 문제는 scan 단계가 role rank마다 독립적으로 수행된다는 점이다.

- source role rank가 많아질수록 같은 source tree를 반복 scan
- batch mode에서는 count pass + spool pass 때문에 같은 tree를 다시 scan
- 결과적으로 스토리지 metadata operation이 `2 * source_rank_count` 배까지 커짐

이번 계획의 목표는 다음이다.

1. `libcircle` 분산 walk를 도입해 **같은 side의 rank들이 tree를 나눠서 scan**하도록 만든다.
2. **같은 path를 rank마다 다시 scan하지 않도록** 한다.
3. `nsync`의 split-mount 아키텍처와 기존 planner/executor는 최대한 유지한다.
4. 1차 구현에서는 risk를 낮추고, 2차 구현에서 batch mode의 filesystem scan 횟수까지 줄인다.

이 문서는 **구현 plan**만 정리한다. 실제 코드 수정은 다음 프롬프트에서 진행한다.

## 2. 현재 코드의 병목

현재 scan 경로:

- 재귀 scan 본체:
  `src/nsync/nsync.c:1168-1287`
- role 분기:
  `src/nsync/nsync.c:1289-1313`
- batch count scan:
  `src/nsync/nsync.c:1315-1370`
- spool build scan:
  `src/nsync/nsync.c:1691-1707`
- main batch flow:
  `src/nsync/nsync.c:3932-3996`

문제 요약:

- `nsync_scan_role_path_filtered()`는 source role이면 각 source rank가 그냥 `src_path`부터 직접 재귀를 돈다.
- destination role도 동일하게 `dst_path`를 각 destination rank가 직접 재귀 scan한다.
- batch mode에서는 count pass와 spool build pass가 각각 전체 tree를 다시 돈다.

즉 현재 구조는 다음과 같다.

```text
source FS metadata scan cost ~= 2 * source_role_rank_count
destination FS metadata scan cost ~= 2 * destination_role_rank_count
```

배치 여부를 떠나 같은 side의 rank 수가 많아질수록 namespace 부하가 선형으로 커진다.

## 3. 기존 공용 walk 재사용 가능성 및 제약

이미 공용 코드에는 `libcircle` 기반 분산 walk가 있다.

- 공용 walk 진입점:
  `src/common/mfu_flist.h:183-206`
- 구현:
  `src/common/mfu_flist_walk.c:565-668`

이 구현은 장점과 제약이 동시에 있다.

### 장점

- `libcircle` queue로 directory walk를 rank들이 분산 처리
- 각 path를 전역적으로 한 번만 처리하는 구조
- 이미 `mfu_file_t` wrapper와 `mfu_walk_opts_t`를 사용

### 제약

1. 현재 API는 결과를 `mfu_flist`에 쌓는 구조다.
   - `nsync`가 이것을 그대로 쓰면, batch mode의 메모리 상한 목표와 충돌한다.
2. 구현이 `MPI_COMM_WORLD` 전제다.
   - `src/common/mfu_flist_walk.c:601-602`
   - `CIRCLE_init(...)`도 공용 코드에서는 world 기반으로 호출된다.
3. `nsync`는 split-mount 구조라 source rank와 destination rank를 분리해야 한다.
   - source tree를 destination rank가 scan하면 안 된다.
   - destination tree를 source rank가 scan하면 안 된다.

따라서 `mfu_flist_walk_param_paths()`를 `nsync`에서 그대로 호출하는 방식은 적절하지 않다.

## 4. 바로 채택하지 않을 접근

### 접근 A. `mfu_flist_walk_param_paths()`를 그대로 호출한 뒤 `mfu_flist -> nsync_meta` 변환

채택하지 않는다.

이유:

- communicator가 `MPI_COMM_WORLD`에 고정되어 split-mount role 분리가 안 된다.
- `mfu_flist` 전체 materialization으로 메모리 사용량이 커진다.
- 결국 `nsync`가 피하려던 문제를 다시 가져오게 된다.

### 접근 B. `nsync_scan_recursive()`에 rank-hash filter만 넣어서 일부 path만 저장

채택하지 않는다.

이유:

- 현재도 hash filter는 “저장 대상”만 거를 뿐, 재귀 scan 자체는 막지 못한다.
- metadata IOPS 문제의 핵심은 “저장”이 아니라 “같은 tree를 rank마다 다시 읽는 것”이다.

## 5. 권장 설계

권장 설계는 두 단계다.

### 5.1 1차 구현 목표: distributed walk로 rank별 중복 scan 제거

핵심 아이디어:

- source scan은 `src_comm` 안에서만 분산 walk
- destination scan은 `dst_comm` 안에서만 분산 walk
- walk 결과는 `mfu_flist`가 아니라 **streaming callback**으로 `nsync_meta_record` 또는 spool record로 바로 넘김

이 단계만 완료해도 metadata overhead는 크게 줄어든다.

```text
현재 batch mode source scan ~= 2 * Rsrc
1차 구현 후 source scan ~= 2
```

즉 source rank 10개 기준:

- 현재: source tree 20회 full scan
- 1차 구현 후: source tree 2회 full distributed scan

### 5.2 2차 구현 목표: batch mode filesystem scan을 2회 -> 1회로 줄이기

핵심 아이디어:

- 첫 distributed walk에서 metadata를 raw spool에 쓰면서 count도 같이 모은다.
- walk가 끝난 뒤 batch count를 계산한다.
- 그 다음 **filesystem을 다시 읽지 않고**, raw spool을 local disk에서 batch spool로 repartition한다.

이 단계까지 가면 source/destination namespace는 batch mode에서도 side별 1회 scan으로 줄어든다.

```text
2차 구현 후 source scan ~= 1
2차 구현 후 destination scan ~= 1
```

즉 `dsync`와 비슷한 수준의 storage metadata pressure로 내려간다.

## 6. 구현 전략 상세

## 6.1 공용 layer에 communicator-aware streaming walk 추가

새 공용 API를 추가하는 방향을 권장한다.

예상 형태:

```c
typedef int (*mfu_walk_emit_fn)(
    const char* path,
    const struct stat* st,
    void* arg);

int mfu_walk_param_paths_emit(
    uint64_t num,
    const mfu_param_path* params,
    mfu_walk_opts_t* walk_opts,
    mfu_file_t* mfu_file,
    MPI_Comm comm,
    mfu_walk_emit_fn emit_fn,
    void* emit_arg,
    uint64_t* local_items_out,
    int* local_errors_out);
```

설계 원칙:

- 결과를 `mfu_flist`에 쌓지 않는다.
- item 하나를 처리할 때마다 callback으로 바로 넘긴다.
- communicator를 인자로 받아 `src_comm`/`dst_comm`에서 각각 돌 수 있어야 한다.
- `mfu_file_t`와 `mfu_walk_opts_t`는 재사용한다.

권장 구현 위치:

- `src/common/mfu_flist.h`에 API 선언 추가 또는 새 `src/common/mfu_walk_emit.h`
- `src/common/mfu_flist_walk.c` 확장 또는 새 `src/common/mfu_walk_emit.c`

### 왜 공용 layer가 필요한가

- `nsync.c` 안에 `libcircle` walker를 또 복제하면 유지보수 비용이 크다.
- 나중에 `dscan` 같은 다른 도구도 streaming distributed walk를 재사용할 수 있다.
- `mfu_file_t` / DAOS / DFS 같은 filesystem abstraction과도 맞춰가기 쉽다.

## 6.2 communicator 제약 처리

이 항목이 1차 구현의 핵심 리스크다.

현재 공용 walk는 `MPI_COMM_WORLD` 기준으로 작성되어 있다.

- `src/common/mfu_flist_walk.c:601-602`
- `src/common/mfu_flist_walk.c:613-668`

따라서 1차 구현의 첫 작업은 **libcircle walk가 role-specific communicator에서 돌 수 있게 만드는 것**이다.

권장 순서:

1. `mfu_walk_param_paths_emit(..., MPI_Comm comm, ...)` 형태로 common API 설계
2. 공용 walk 구현에서 communicator를 context에 넣고, rank/size/reduce/barrier가 그 communicator를 쓰도록 정리
3. `libcircle` 초기화가 sub-communicator를 직접 받을 수 있는지 확인
4. 직접 지원하지 않으면 최소 patch/wrapper를 추가해 `comm` 기반 init 경로를 만든다

여기서 중요한 점:

- 이 제약이 해결되지 않으면 `nsync`의 split-mount 구조에서는 source와 destination을 같은 world walk에 안전하게 섞을 수 없다.
- 따라서 communicator-aware walk는 “있으면 좋은 최적화”가 아니라 **구조적 prerequisite**다.

## 6.3 `nsync` 전용 scan context 도입

현재 recursive scan의 책임은 너무 많다.

- relpath 생성
- `lstat`
- symlink target read
- `--contents` digest 계산
- filter 적용
- vector push 또는 spool emit
- count 증가

이 책임을 distributed walk callback에서 재사용할 수 있게 context 구조를 만든다.

예상 구조:

```c
typedef struct {
    const nsync_options_t* opts;
    const nsync_role_info_t* role_info;
    const char* root_path;
    nsync_role_t side;
    nsync_meta_vec_t* out;
    int* scan_errors;
    const nsync_scan_filter_t* filter;
    uint64_t* item_count;
    int dirs_only;
    nsync_scan_emit_fn emit_fn;
    void* emit_arg;
    mfu_file_t* mfu_file;
} nsync_scan_ctx_t;
```

새 함수:

- `nsync_scan_emit_from_walk(...)`
- `nsync_scan_role_path_distributed(...)`
- `nsync_fullpath_to_relpath(...)`

역할:

- distributed walk callback가 full path + `struct stat`를 넘기면
- `nsync` 측에서 relpath 변환, symlink/digest 처리, filter/emit/count를 수행

## 6.4 `mfu_file_t`를 `nsync`에도 도입

현재 `nsync` scan은 POSIX `lstat/opendir` 직접 호출이다.

공용 distributed walk를 쓰려면 `mfu_file_t` 핸들이 필요하다.

작업:

- `main()` 초기에 `mfu_file_t* mfu_src_file = mfu_file_new();`
- `mfu_file_t* mfu_dst_file = mfu_file_new();`
- source path/destination path를 `mfu_param_path_set()`로 초기화할 수 있게 정리
- 1차 범위에서는 POSIX 경로만 지원

중요한 판단:

- 이번 변경의 주목적은 split-mount POSIX metadata scan 개선이다.
- DAOS/DFS 확장은 별도 범위로 두고, 1차 구현은 POSIX 기준으로 닫는 것이 안전하다.

## 6.5 1차 구현에서 batch mode는 2-pass 유지

첫 코드 업데이트는 risk를 줄이기 위해 batch mode semantics를 유지한다.

즉:

1. distributed count pass
2. batch count 계산
3. distributed spool build pass
4. 기존 batch loop 그대로 사용

바뀌는 점:

- recursive scan -> distributed scan
- rank마다 full scan -> side communicator 안에서 1회 full scan

유지되는 점:

- `batch_count` 계산 방식
- `nsync_batch_spool_prepare()`
- `nsync_batch_spool_append_record()`
- `nsync_batch_spool_load_batch()`
- planner/executor/wire protocol

이렇게 하면 1차 코드 변경 범위를 scan layer로 한정할 수 있다.

## 6.6 2차 구현에서 batch mode를 single filesystem scan으로 변경

1차 구현이 안정화되면 2차로 넘어간다.

새 batch pipeline:

1. distributed walk 1회
2. item count를 동시에 집계
3. raw scan spool 파일(예: `scan-raw.bin`)에 record append
4. walk 종료 후 `MPI_Allreduce`로 `global_src_items`, `global_dst_items` 계산
5. `batch_count` 산출
6. 각 rank가 자기 raw spool 파일만 읽어서 최종 batch spool(`batch-<id>.bin`)로 local repartition
7. 기존 batch loop 사용

장점:

- filesystem metadata scan 1회
- repartition은 local disk I/O라 namespace/MDS 부하를 다시 만들지 않음

주의점:

- 현재 `nsync_batch_spool_append_record()`는 `batch_count`를 알아야 한다.
- 2차 구현에서는 `raw spool` 포맷과 `final batch spool` 포맷을 분리해야 한다.

권장 판단:

- 다음 프롬프트의 실제 코드 업데이트는 1차 구현까지만 하는 것이 안전하다.
- 2차는 후속 최적화 milestone으로 두는 것이 좋다.

## 7. 파일별 변경 계획

### `src/common/*`

변경 대상:

- `src/common/mfu_flist.h`
- `src/common/mfu_flist_walk.c`
- 필요 시 새 파일:
  - `src/common/mfu_walk_emit.c`
  - `src/common/mfu_walk_emit.h`

변경 내용:

- communicator-aware streaming walk API 추가
- 기존 global/static walker state를 context 구조로 정리
- `mfu_flist` insert 대신 user callback 호출 경로 추가
- error/item count를 callback 기반으로 집계

### `src/nsync/nsync.c`

변경 대상:

- `nsync_scan_recursive()` hot path 대체
- `nsync_scan_role_path_filtered()` 대체 또는 wrapper화
- `nsync_compute_batch_count()`
- `nsync_batch_spool_build()`
- `main()` 초기화부

변경 내용:

- `mfu_file_t` 초기화 추가
- distributed walk backend 추가
- batch count / spool build가 distributed walk를 사용하도록 전환
- recursive scan은 fallback 또는 삭제 후보로 분리

### `test/tests/test_nsync/*`

추가/수정 대상:

- `test/tests/test_nsync/test_matrix.sh`
- `test/tests/test_nsync/test_resume.sh`

추가할 검증:

- `-np 4`, `-np 8` role-map 케이스
- `0-3:src,4-7:dst` 같은 다중 side rank 구성
- batch on/off 모두 결과 동일성 확인
- `--delete`, `--contents`, symlink, empty dst 유지
- batch resume 기존 동작 보존

권장 추가:

- trace 로그에서 source-side local scan count 합이 tree size와 일치하는지 확인하는 케이스
- 동일 tree에서 recursive backend 대비 planner summary가 동일한지 비교하는 developer test

## 8. 단계별 구현 순서

### Step 0. 사전 확인

- communicator-aware `libcircle` init 가능 여부 확인
- 불가능하면 최소 wrapper/patch 범위를 먼저 확정

### Step 1. 공용 distributed emit walker 추가

- callback 기반 API 추가
- communicator 인자 추가
- 간단한 smoke test 또는 임시 test driver로 동작 확인

### Step 2. `nsync` non-batch scan 전환

- `opts.batch_files == 0` 경로부터 distributed walk 사용
- recursive scan 결과와 planner summary 비교

### Step 3. `nsync` batch 2-pass 전환

- distributed count pass
- distributed spool build pass
- 기존 batch loop와 planner/executor 재사용

### Step 4. 다중 rank 회귀 테스트

- `test_matrix.sh`에 4-rank/8-rank 케이스 추가
- source-side repeated scan이 제거되었는지 trace 확인

### Step 5. optional 2차 최적화

- raw spool + local repartition 도입
- batch mode filesystem scan 2회 -> 1회

## 9. 성공 기준

1차 구현 성공 기준:

- source role rank가 10개여도 source tree가 rank마다 다시 scan되지 않음
- batch mode source metadata overhead가 `2 * Rsrc`에서 `2`로 감소
- planner/executor 결과가 기존과 동일
- 기존 `test_nsync` functional behavior 유지

2차 구현 성공 기준:

- batch mode source metadata overhead가 `2`에서 `1`로 추가 감소
- 기존 batch load/execute semantics 유지

## 10. 리스크와 대응

### 리스크 1. libcircle communicator 제약

가장 큰 리스크다.

대응:

- 첫 작업으로 확인
- 필요 시 common layer에서 wrapper/patch를 먼저 만든다

### 리스크 2. `mfu_flist` 없이 callback만으로 기존 semantics 유지

대응:

- relpath/digest/symlink/meta record 생성을 `nsync` callback에 집중
- planner/wire format은 건드리지 않는다

### 리스크 3. destination root missing semantics

현재는 destination root가 없으면 empty tree로 간주한다.

대응:

- distributed walk 시작 전에 role communicator 내 root existence check를 먼저 수행
- dst root `ENOENT`는 empty tree로 broadcast

### 리스크 4. 2차 최적화 범위 확대

raw spool repartition은 꽤 큰 변경이다.

대응:

- 다음 코드 업데이트는 1차 구현까지만 목표로 한다
- 2차는 별도 milestone으로 분리

## 11. 다음 프롬프트에서 바로 구현할 범위

다음 코드 업데이트에서는 아래 범위를 권장한다.

1. common layer에 communicator-aware streaming distributed walk 추가
2. `nsync` non-batch scan을 distributed walk로 전환
3. `nsync` batch mode count pass / spool build pass를 distributed walk로 전환
4. recursive scanner는 fallback로 잠시 남김
5. `test_nsync`에 다중 rank 케이스 추가

이 범위면:

- storage metadata pressure의 핵심 문제를 해결할 수 있고
- planner/executor/wire protocol까지 한 번에 뜯지 않아도 되며
- 이후 2차 최적화(raw spool single-pass)로 자연스럽게 이어갈 수 있다

## 12. 예상 효과

source rank 10개를 예로 들면:

- 현재 batch `nsync`: source tree 약 20회 full scan
- 1차 구현 후: source tree 약 2회 full distributed scan
- 2차 구현 후: source tree 약 1회 full distributed scan

즉 1차 구현만으로도 storage metadata pressure는 대략 `20x -> 2x`로 줄고, 2차까지 가면 `20x -> 1x` 수준까지 내려간다.
