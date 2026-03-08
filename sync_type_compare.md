# `dsync` vs `nsync` 메모리 및 메타데이터 오버헤드 비교

이 문서는 현재 소스 기준으로 `src/dsync/dsync.c`와 `src/nsync/nsync.c`를 비교한다. 과거 문서에 있던 "`nsync --batch-files`는 source rank마다 전체 tree를 다시 2-pass scan한다"는 결론은 더 이상 맞지 않는다. 최근 업데이트로 `nsync`는 다음이 반영되었다.

- role communicator 내부의 분산 directory walk
- batch mode의 single-pass metadata scan + raw spool 생성
- multi-batch의 raw payload 재사용 + batch offset sidecar(`.idx`)
- metadata/action/directory-task 교환 경로의 `uint64_t` logical count 지원

## 핵심 결론

- `dsync --batch-files`는 여전히 copy 단계 batching이다. source/destination 전체 walk, remap, `strmap` 비교, copy/remove list 생성은 모두 batch 전에 끝난다.
- 현재 `nsync --batch-files`는 source side와 destination side를 각각 한 번씩만 분산 scan한다. 과거처럼 source rank마다 같은 tree를 다시 2-pass scan하지 않는다.
- storage metadata operation 관점에서는 현재 `dsync`와 `nsync` 모두 각 side 파일시스템에 대해 대략 1회의 full-tree traversal을 수행한다.
- 메모리 관점에서는 `dsync`가 전체 tree metadata를 RAM에 여러 복사본으로 오래 잡는 구조이고, `nsync`는 batch-sized vector와 local spool 디스크 사용으로 RAM 피크를 낮추는 구조다.
- `nsync.c` 내부의 file/directory/action logical count는 이제 `uint64_t` 기반이다. 과거 `int` count 상한은 제거되었지만, 개별 record 길이와 MPI rank 관련 제약은 남아 있다.

## 1. 분석 기준 코드 경로

### `dsync`

- source walk: `src/dsync/dsync.c:3367-3371`
- destination walk: `src/dsync/dsync.c:3404-3411`
- walk 구현체: `src/common/mfu_flist_walk.c:565-668`, `src/common/mfu_flist_walk.c:698-727`
- walk 중 directory enqueue/stat: `src/common/mfu_flist_walk.c:450-551`
- remap: `src/dsync/dsync.c:3433-3440`, `src/common/mfu_flist.c:1406-1463`
- `strmap` 생성: `src/dsync/dsync.c:3450-3455`, `src/dsync/dsync.c:481-507`
- 비교/계획용 subset list 생성: `src/dsync/dsync.c:1617-1649`
- subset entry deep copy: `src/common/mfu_flist.c:329-351`, `src/common/mfu_flist.c:1291-1299`
- 실제 copy batching: `src/dsync/dsync.c:1397-1404`, `src/common/mfu_flist_copy.c:2497-2586`

### `nsync`

- role-local scan context: `src/nsync/nsync.c:1548-1560`, `src/nsync/nsync.c:1639-1665`
- directory ownership hash: `src/nsync/nsync.c:1667-1675`
- distributed directory walk 본체: `src/nsync/nsync.c:1939-2148`
- directory task exchange: `src/nsync/nsync.c:1813-1937`
- batch count 계산: `src/nsync/nsync.c:2150-2172`
- metadata pack/spool: `src/nsync/nsync.c:2175-2180`, `src/nsync/nsync.c:2452-2819`
- batch load: `src/nsync/nsync.c:2822-2967`
- metadata redistribution: `src/nsync/nsync.c:3021-3169`
- action redistribution: `src/nsync/nsync.c:3308-3461`
- deferred directory finalize: `src/nsync/nsync.c:4027-4305`
- batch main loop: `src/nsync/nsync.c:5126-5182`, `src/nsync/nsync.c:5210-5368`
- framed multi-round exchange helper: `src/nsync/nsync.c:616-639`, `src/nsync/nsync.c:768-925`

## 2. 메타데이터 오버헤드 비교

여기서 말하는 메타데이터 오버헤드는 주로 다음을 뜻한다.

- `lstat/stat`
- `opendir/readdir/closedir`
- directory tree traversal

`--contents` 시 파일 data read와 SHA256 계산은 별도 비용으로 보고, 이 절의 기본 비교에서는 제외한다.

### 2.1 `dsync`

`dsync`는 먼저 source와 destination 전체를 `mfu_flist_walk_param_paths()`로 walk한다.

- source: `src/dsync/dsync.c:3367-3371`
- destination: `src/dsync/dsync.c:3404-3411`

walk 구현은 `libcircle` work queue 기반이다.

- 초기 path enqueue: `src/common/mfu_flist_walk.c:487-495`
- item `stat/lstat`: `src/common/mfu_flist_walk.c:498-551`
- 분산 실행 시작: `src/common/mfu_flist_walk.c:612-668`

즉 `dsync`의 scan metadata pressure는 "rank마다 같은 path를 다시 stat"하는 구조가 아니라, MPI rank들이 queue를 나눠 들고 전체 tree를 한 번 훑는 구조다.

`--batch-files`는 이 단계에 영향을 주지 않는다. batch는 `mfu_flist_copy()` 안에서 이미 만들어진 `src_cp_list`를 잘라 실행할 때만 적용된다.

- batch slice: `src/common/mfu_flist_copy.c:2497-2586`

따라서 `dsync --batch-files`의 storage metadata operation은 여전히 다음과 같다.

- source 파일시스템: full-tree walk 1회
- destination 파일시스템: full-tree walk 1회
- batch 유무와 무관하게 scan/planning metadata load는 거의 동일

### 2.2 현재 `nsync`

현재 `nsync`의 scan은 role communicator 내부 분산 walk다.

- root owner만 `.`를 시작점으로 잡고: `src/nsync/nsync.c:2053-2077`
- 각 directory owner가 `opendir/readdir/lstat`로 자식을 읽은 뒤: `src/nsync/nsync.c:1955-2017`
- child directory를 hash owner에게 넘긴다: `src/nsync/nsync.c:1994-2009`, `src/nsync/nsync.c:2122-2123`

코드 주석 그대로, 각 directory path는 같은 side에서 정확히 한 rank만 scan한다.

- `src/nsync/nsync.c:2020-2022`

즉 현재 `nsync`는 source side에 대해서 `src_comm`, destination side에 대해서 `dst_comm` 안에서 tree를 분할 처리한다. 과거처럼 source role rank마다 source root를 다시 전체 재귀하는 구조가 아니다.

### 2.3 batch mode에서 source/destination 전체 scan을 하는가?

그렇다. 다만 현재는 "한 번만" 한다.

`nsync --batch-files`는 batch loop 전에 `nsync_batch_spool_scan_prepare()`를 호출한다.

- `src/nsync/nsync.c:5131-5136`

이 함수는:

1. distributed scan을 한 번 수행하면서
2. metadata record를 raw spool(`scan-raw.bin`)에 기록하고
3. 동시에 global item count를 집계한 뒤
4. batch count를 계산하고
5. single-batch면 raw spool을 그대로 `batch-0.bin`으로 rename하고
6. multi-batch면 raw payload는 유지한 채 batch별 `.idx` sidecar를 만든다.

관련 코드는:

- single-pass scan + raw record emit: `src/nsync/nsync.c:2777-2784`
- global item count reduce: `src/nsync/nsync.c:2791-2804`
- single-batch rename: `src/nsync/nsync.c:2726-2745`
- multi-batch raw repartition: `src/nsync/nsync.c:2747-2748`, `src/nsync/nsync.c:2648-2716`

즉 현재 `nsync --batch-files`는:

- source 파일시스템: full-tree distributed scan 1회
- destination 파일시스템: full-tree distributed scan 1회
- 이후 batch loop에서는 filesystem을 다시 walk하지 않고 spool에서 현재 batch를 로드

배치 로드는 다음 코드다.

- `src/nsync/nsync.c:5232-5239`
- `src/nsync/nsync.c:2822-2967`

### 2.4 source rank 10개일 때 storage metadata operation 비교

기호:

- `N`: source tree 전체 item 수
- `D`: source tree 전체 directory 수
- `Rsrc`: source side rank 수

source 파일시스템이 source rank 10개에 mount되어 있고, destination은 다른 파일시스템이라고 가정하면:

#### `dsync`

source walk는 `libcircle`로 분산되므로, source filesystem이 보는 메타데이터 작업은 대략:

- `lstat/stat ~= N`
- `opendir ~= D`
- `closedir ~= D`
- `readdir traversal ~= 1 full traversal`

rank 수가 10이어도 source path를 10번 다시 읽는 구조는 아니다.

#### 현재 `nsync --batch-files`

source side distributed walk도 directory ownership을 source rank들 사이에 나눠 처리하므로, source filesystem이 보는 메타데이터 작업은 현재 대략:

- `lstat ~= N`
- `opendir ~= D`
- `closedir ~= D`
- `readdir traversal ~= 1 full traversal`

즉 과거 문서의 "`Rsrc=10`이면 source scan pressure가 `10x` 또는 `20x`" 결론은 더 이상 맞지 않는다. 현재 구현에서는 source filesystem metadata pressure가 `dsync`와 같은 차수로 내려왔다.

### 2.5 metadata 측면의 남는 차이

storage metadata op 수는 비슷해졌지만, scan 제어 방식은 다르다.

- `dsync`는 `MPI_COMM_WORLD` 기반 `libcircle` queue walk다.
- `nsync`는 role-local frontier walk + directory-task exchange다.

따라서 현재 `nsync`는 같은 path를 다시 stat하지 않는 대신, directory frontier마다 collective와 directory-task 교환이 들어간다.

- frontier 종료 조건 reduce: `src/nsync/nsync.c:2089-2095`
- directory-task exchange: `src/nsync/nsync.c:1813-1937`

즉 `nsync`의 남은 추가 비용은 storage metadata operation보다 MPI 제어 트래픽 쪽에 더 가깝다.

## 3. 메모리 오버헤드 비교

## 3.1 `dsync` 메모리 모델

`dsync`는 planning 이전과 planning 중에 whole-tree metadata를 RAM에 오래 유지한다.

주요 구조는 다음과 같다.

- `mfu_flist`
- `strmap`
- 비교/복사/삭제용 subset `mfu_flist`
- metadata refresh용 `strmap`

핵심 구조체:

- `elem_t`: `src/common/mfu_flist_internal.h:27-47`
- `flist_t`: `src/common/mfu_flist_internal.h:58-85`
- `strmap_node`: `src/common/strmap.h:28-37`

### `mfu_flist` 쪽

walk 결과는 linked-list + index-array 형태로 유지된다.

- `flist_t::list_head/list_tail/list_index`: `src/common/mfu_flist_internal.h:71-77`

entry 하나당 기본적으로 다음이 필요하다.

- `elem_t` 본체
- `strdup`된 full path 문자열
- `list_index`의 pointer 1개

또 subset list로 옮길 때 shallow reference가 아니라 deep copy가 일어난다.

- deep copy 구현: `src/common/mfu_flist.c:329-351`
- subset copy 호출: `src/common/mfu_flist.c:1291-1299`

즉 같은 파일이 `src_compare_list`, `src_cp_list` 등에 동시에 들어가면 path와 metadata struct가 그대로 다시 복제된다.

### `strmap` 쪽

`dsync`는 source/destination 각각에 대해 relpath 기반 `strmap`을 만든다.

- `src/dsync/dsync.c:3450-3455`
- `src/dsync/dsync.c:481-507`

`strmap_node`는 key/value 문자열을 각각 `strdup`한다.

- `src/common/strmap.c:26-45`

그래서 `strmap` entry 하나는:

- AVL node 본체
- relpath key 문자열
- state/index를 담는 value 문자열

을 별도로 가진다.

### planning 단계에서 추가로 생기는 복사본

비교 루틴은 여러 subset list를 만든다.

- `src_compare_list`, `dst_compare_list`, `src_cp_list`, `dst_remove_list`: `src/dsync/dsync.c:1617-1625`
- `link-dest` 사용 시 `dst_same_list`, `link_same_list`, `link_dst_list`, `src_real_cp_list`: `src/dsync/dsync.c:1627-1645`
- metadata refresh map: `src/dsync/dsync.c:1647-1649`

즉 `dsync`의 RAM 피크는 단순히 "source flist + destination flist"가 아니다. 실제로는:

- walk flist
- remap 후 flist
- source/destination `strmap`
- 비교/복사/삭제 subset list
- refresh map

이 겹친다.

특히 remap은 새 flist를 만든 뒤 기존 walk flist를 나중에 free한다.

- remap: `src/dsync/dsync.c:3433-3440`
- old list free: `src/dsync/dsync.c:3442-3447`

따라서 remap 구간에는 walk 결과와 remap 결과가 잠깐 함께 존재한다.

### `--batch-files`가 RAM 피크를 거의 줄이지 못하는 이유

`--batch-files`는 `mfu_flist_copy()` 안에서 `src_cp_list`를 잘라 `tmplist`를 만들고 copy하는 데만 적용된다.

- `src/common/mfu_flist_copy.c:2497-2586`

그 시점에는 이미:

- source/destination 전체 walk 완료
- remap 완료
- `strmap` 생성 완료
- compare/copy/remove list 생성 완료

상태다.

즉 `dsync --batch-files`는 execution-phase copy list만 쪼개고, planning RAM 피크는 거의 그대로 남는다.

## 3.2 현재 `nsync` 메모리 모델

현재 `nsync`는 batch mode에서 whole-tree metadata를 RAM에 계속 보관하지 않는다. 대신:

1. distributed scan 중 metadata record를 raw spool로 바로 흘리고
2. batch loop에서 현재 batch만 `local_meta`로 다시 로드하고
3. planner/executor vector를 batch 단위로 만들고
4. batch가 끝나면 대부분 free한다.

scan 단계에서 전체 tree를 위한 in-memory metadata vector를 유지하지 않는다는 점이 `dsync`와 가장 큰 차이다.

### scan/spool 단계

raw spool 생성:

- spool prepare: `src/nsync/nsync.c:2452-2499`
- raw record append: `src/nsync/nsync.c:2610-2646`
- single-pass scan + emit: `src/nsync/nsync.c:2777-2784`

multi-batch에서는 raw payload는 유지하고 batch별 `.idx` 파일에 8-byte offset만 쓴다.

- raw repartition: `src/nsync/nsync.c:2648-2716`
- batch index mode 전환: `src/nsync/nsync.c:2747-2748`
- batch index append: `src/nsync/nsync.c:2589-2607`

따라서 multi-batch의 local tmp usage는 현재 대략:

```text
raw payload bytes
+ 8 * metadata record count
+ batch_fds[] / batch_has_data[] / directory entries / allocator overhead
```

수준이다. 과거처럼 raw payload를 batch payload로 다시 복제하지 않는다.

### batch loop RAM

각 batch마다 잡히는 주요 벡터:

- `local_meta`
- `planner_meta`
- `planned_actions`
- `src_exec_actions`
- `dst_exec_actions`

생성/해제 루프:

- `src/nsync/nsync.c:5210-5318`
- `src/nsync/nsync.c:5337-5368`

메타/액션 벡터는 `uint64_t size/capacity`를 쓰고, `realloc` 전 `size_t` overflow를 체크한다.

- meta vector: `src/nsync/nsync.c:1297-1343`
- action vector: `src/nsync/nsync.c:1351-1397`
- helper: `src/nsync/nsync.c:586-604`

즉 `nsync`의 planner RAM은 기본적으로 batch 크기와 skew에 비례하고, whole-tree item count 전체에 직접 비례하지 않는다.

### spool/wire record 크기

`nsync_meta_pack_size()` 기준 packed metadata payload는:

```text
64B + relpath_len + link_target_len
```

이고,

- `--contents`면 digest field가 `36B` 추가된다: `src/nsync/nsync.c:2175-2180`
- raw spool에는 record length prefix `4B`가 더 붙는다: `src/nsync/nsync.c:2555-2568`

따라서 raw spool record는 대략:

```text
contents off: 68B + relpath_len + link_target_len
contents on : 104B + relpath_len + link_target_len
```

이다.

### 대용량 교환 시 메모리

metadata/action redistribution은 이제 `uint64_t` logical count를 사용하지만, 메모리 사용이 완전히 O(1)은 아니다.

- metadata redistribute: `src/nsync/nsync.c:3021-3169`
- action redistribute: `src/nsync/nsync.c:3308-3461`

현재 구조는:

1. sender가 자신의 local batch payload를 `send_buf` 하나에 pack하고
2. `nsync_exchange_framed_buffers()`가 이를 round 단위로 쪼개 `MPI_Alltoallv` 한다.

즉 sender peak는 대략:

```text
serialized send_buf(total_send)
+ round_send_buf
+ round_recv_buf
+ decoder state
```

가 된다.

다만 round send/recv buffer는 chunk budget으로 제한된다.

- default chunk budget: `64 MiB`: `src/nsync/nsync.c:616-639`
- per-round `Alltoallv` buffer 재사용: `src/nsync/nsync.c:840-922`

즉 예전의 `int` count 기반 one-shot large `Alltoallv`보다는 안전해졌지만, local batch가 매우 큰 rank에서는 `send_buf(total_send)` 자체가 여전히 메모리 압박이 될 수 있다.

## 3.3 `dsync`와 `nsync` 메모리 성격 차이

### `dsync`

- whole-tree metadata를 RAM에 유지
- 동일 entry가 여러 subset list에 deep-copy 될 수 있음
- `strmap`가 key/value 문자열을 다시 복제
- batch는 copy 단계만 잘라서 planning RAM 피크를 거의 줄이지 못함

### `nsync`

- batch mode에서 whole-tree metadata는 raw spool에 먼저 기록
- planner/executor RAM은 주로 현재 batch 단위
- multi-batch는 payload duplication 없이 raw spool + `.idx` sidecar 사용
- 대신 local tmp storage와 serialization buffer 사용이 생김

요약하면:

- `dsync`는 RAM-heavy / disk-light planner
- `nsync`는 RAM-bounded / disk-spool-heavy planner

로 보는 편이 맞다.

## 4. `uint64_t` count 업데이트 영향과 호환성

최근 `nsync.c` 업데이트로 다음 경로의 logical count가 `uint64_t`가 되었다.

- directory task exchange: `src/nsync/nsync.c:1813-1937`
- metadata redistribution: `src/nsync/nsync.c:3021-3169`
- action redistribution: `src/nsync/nsync.c:3308-3461`

관련 helper:

- `nsync_count_to_bytes_checked()`: `src/nsync/nsync.c:596-604`
- `nsync_compute_u64_displacements()`: `src/nsync/nsync.c:641-660`
- `nsync_exchange_framed_buffers()`: `src/nsync/nsync.c:768-925`

내부 mpifileutils 공용 레이어와의 방향성도 맞는다. 예를 들어 `mfu_flist`는 이미 전체 item count와 remap `Alltoall`에 `MPI_UINT64_T`를 사용한다.

- `mfu_flist` global count: `src/common/mfu_flist.c:452-472`
- `mfu_flist_size()`: `src/common/mfu_flist.c:683-687`
- `mfu_flist_remap()` count exchange: `src/common/mfu_flist.c:1421-1463`
- `strmap` size도 `uint64_t`: `src/common/strmap.h:40-44`, `src/common/strmap.h:91-92`

즉 이번 `nsync` 변경은 내부 mpifileutils 타입 체계와 충돌하는 수정이 아니라, 기존 공용 코드 수준에 맞춘 정렬에 가깝다.

## 5. 잔존 리스크와 한계

현재 구현에서 남는 제한은 다음과 같다.

### `dsync`

- `--batch-files`가 planning RAM 피크를 근본적으로 줄이지 못한다.
- remap 전후 flist 공존, `strmap`, subset deep copy 때문에 large tree에서 multi-GB RAM으로 커질 수 있다.
- memory pressure는 batch size보다 전체 tree 규모와 compare 결과에 더 민감하다.

### `nsync`

- storage metadata op 증폭 문제는 해결됐지만, directory frontier마다 collective와 task exchange가 들어간다.
- 매우 넓은 frontier에서는 directory task payload와 scan frontier vector가 순간적으로 커질 수 있다.
- batch RAM은 줄었지만, serialized `send_buf(total_send)`는 여전히 local batch skew에 따라 커질 수 있다.
- deferred directory remove/meta update는 마지막 finalize까지 누적된다.
  - `src/nsync/nsync.c:4027-4085`
  - `src/nsync/nsync.c:4245-4305`
- `batch_count`는 logical로 `uint64_t`지만, local spool bookkeeping 배열은 결국 `size_t`에 들어가야 한다.
  - `src/nsync/nsync.c:2501-2520`

### `nsync`에서 아직 남아 있는 non-`uint64` 제한

- MPI communicator 크기와 rank 번호는 여전히 `int`
- 개별 packed record 길이는 `uint32_t` framing
  - metadata/action path/link payload 하나가 4 GiB를 넘는 경우는 지원하지 않음
  - 관련 코드: `src/nsync/nsync.c:1762-1789`, `src/nsync/nsync.c:2555-2568`, `src/nsync/nsync.c:3185-3267`
- 실제 상한은 이제 대체로 `SIZE_MAX`, RAM, tmp 공간, spool I/O 용량에 의해 결정됨

## 6. 최종 비교표

| 항목 | `dsync` | 현재 `nsync` |
| --- | --- | --- |
| source/destination scan 방식 | `libcircle` 기반 distributed walk | role communicator 기반 distributed frontier walk |
| batch mode의 scan 횟수 | source/dst 각 1회 | source/dst 각 1회 |
| batch가 scan 자체를 줄이는가 | 아니오 | 아니오 |
| batch가 planning RAM을 줄이는가 | 거의 아니오 | 예, 주로 batch 단위로 제한 |
| 주요 메모리 비용 | whole-tree `flist` + `strmap` + subset deep copy | batch vector + raw spool + sidecar + serialized send buffer |
| multi-batch tmp 저장 형태 | 없음 | raw payload + `.idx` sidecar |
| file count logical 상한 | 공용 `mfu_flist`는 이미 `uint64_t` 지향 | `nsync.c`도 scan/plan/exchange 경로가 `uint64_t`로 정렬됨 |
| 남는 큰 리스크 | large tree RAM 피크 | batch skew, frontier exchange, spool/tmp 공간 |

## 7. 결론

- 현재 코드 기준으로 `nsync --batch-files`의 가장 큰 병목이던 "같은 tree를 source rank마다 다시 scan하는 metadata amplification" 문제는 해결되었다.
- 이제 `dsync`와 `nsync`의 storage metadata operation 차이는 예전보다 훨씬 작고, 둘 다 각 side 파일시스템에 대해 대략 1회의 full-tree traversal로 볼 수 있다.
- 대신 메모리/실행 모델은 여전히 크게 다르다.
  - `dsync`는 full-tree RAM planner
  - `nsync`는 batch RAM + spool disk planner
- large tree에서 memory ceiling이 더 중요한 환경이면 현재 구현의 `nsync --batch-files`가 유리하고, local tmp/spool을 최소화하고 단순한 full-tree in-memory compare가 더 맞는 환경이면 `dsync` 쪽이 더 단순하다.
