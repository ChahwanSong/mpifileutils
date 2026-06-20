# dsync 문서 (구현/아키텍처/운영 가이드)

## 1) 개요

`dsync`는 `mpifileutils`의 MPI 기반 동기화 도구다.

source와 destination의 파일 또는 디렉토리 트리를 비교한 뒤, destination이 source와 같아지도록
필요한 항목을 복사하고, 타입이나 크기가 다른 기존 항목을 교체하며, 옵션에 따라 destination에만
있는 항목을 삭제한다. `--contents` 모드에서는 같은 크기의 regular file도 byte 단위로 비교하고,
차이 난 byte range를 destination에 직접 덮어쓸 수 있다.

`rsync -a`에 가까운 목적을 가지지만, 대규모 파일 트리를 MPI rank들이 나누어 처리하도록 설계되어 있다.

## 2) 핵심 특징

- source/destination 파일 또는 디렉토리 트리 동기화
- MPI 기반 병렬 directory walk, 비교, 복사
- 기본 비교 기준: 파일 크기 + mtime
- `--contents` 사용 시 byte-by-byte 내용 비교
- ownership, permission, atime/mtime 등 메타데이터 보존
- `--delete` 사용 시 destination에만 있는 항목 삭제
- `--batch-files` 기반 copy 단계 배치 처리
- 큰 파일 chunk 분할 복사(`--chunksize`)
- sparse file 생성 지원(`--sparse`)
- xattr 복사 정책 선택(`--xattrs`)
- hardlink 기반 증분 백업 지원(`--link-dest`)
- DAOS backend 지원 빌드에서는 DAOS container 동기화 지원

## 3) `nsync`와의 차이

`dsync`는 모든 rank가 source와 destination을 같은 실행 환경에서 볼 수 있다는 전제를 둔다.

- `dsync`
  - 같은 rank 집합이 source와 destination을 모두 walk/compare/copy
  - copy 단계에서 큰 파일을 chunk로 나눠 여러 rank가 병렬 처리 가능
  - `--batch-files`는 scan/planning이 아니라 copy 단계 batching
- `nsync`
  - source만 보는 rank와 destination만 보는 rank가 분리된 split-mount 환경을 목표로 함
  - source/destination 역할을 나눈 뒤 MPI로 메타데이터와 파일 데이터를 교환
  - `dsync`식 file chunk 병렬화 대신 file-level transfer와 role-aware execution을 사용

즉, 양쪽 경로가 모든 rank에서 접근 가능하면 `dsync`가 기본 선택이고, source와 destination mount가
분리되어 있으면 `nsync`가 맞다.

## 4) Command Line Interface

```bash
mpirun -np <N> dsync [options] <SRC> <DEST>
```

필수 인자:

- `SRC`: 기준이 되는 source 파일 또는 디렉토리
- `DEST`: source와 같게 만들 destination 파일 또는 디렉토리

주요 옵션:

| 옵션 | 설명 | 구현상 동작 |
|---|---|---|
| `--dryrun` | 변경 없이 차이만 확인 | 비교는 수행하지만 remove/copy/meta update는 스킵 |
| `-b, --batch-files N` | copy 작업을 최대 N개 파일 단위로 배치 | `mfu_flist_copy()` 내부 copy batching에 적용 |
| `--bufsize SIZE` | I/O buffer 크기 | 기본값은 `MFU_BUFFER_SIZE` 빌드 설정(일반 문서 기준 4 MiB) |
| `--chunksize SIZE` | 큰 파일을 chunk로 나눌 최소 작업 단위 | chunk list 생성 후 rank들이 파일 구간을 나눠 비교/복사 |
| `-X, --xattrs WHICH` | xattr 복사 정책 | `none`, `all`, `libattr`, `non-lustre` |
| `-c, --contents` | 파일 내용을 직접 비교 | 같은 크기 파일도 byte-by-byte 비교 |
| `-D, --delete` | destination에만 있는 항목 삭제 | source walk 오류가 있으면 안전상 자동 비활성화 |
| `-L, --dereference` | symlink target을 따라가서 복사 | source walk/copy에서 link target을 사용 |
| `-P, --no-dereference` | symlink 자체를 복사 | 깨진 link나 접근 불가 target도 link로 취급 가능 |
| `-s, --direct` | `O_DIRECT` 사용 | page cache 영향을 줄이는 copy path |
| `--open-noatime` | `O_NOATIME` 사용 | 파일 open 시 atime 갱신 회피 시도 |
| `--link-dest DIR` | 동일 파일을 새 복사 대신 hardlink로 생성 | 증분 백업용, destination과 같은 filesystem이어야 함 |
| `-S, --sparse` | 가능하면 sparse file 생성 | copy option의 sparse path 활성화 |
| `--chown USER:GROUP` | destination 항목의 소유권을 지정값으로 설정 | source가 아니라 지정 uid/gid로 override (§14) |
| `--chmod SPEC` | destination 항목의 권한 비트를 지정값으로 설정 | octal 또는 `D<oct>,F<oct>`로 dir/file 분리 (§14) |
| `--progress N` | N초마다 진행 로그 출력 | `0`이면 progress 비활성화 |
| `-v, --verbose` | 상세 로그 | 기본도 verbose 수준으로 시작 |
| `-q, --quiet` | 로그 억제 | progress도 비활성화 |
| `-h, --help` | 사용법 출력 | rank 0에서 usage 출력 |

DAOS 지원 빌드에서 추가 옵션:

- `--daos-api DFS|DAOS`: DAOS API 선택. 기본은 container type에 따라 자동 결정.

## 5) 비교 모델

`dsync`는 각 경로를 source root 기준 상대 경로로 맞춰 비교한다.

비교 필드:

- 존재 여부(`EXIST`)
- 타입(`TYPE`)
- 크기(`SIZE`)
- UID/GID
- atime/mtime/ctime
- permission
- ACL
- content

상태 값:

- `COMMON`: source와 destination이 같음
- `DIFFER`: source와 destination이 다름
- `ONLY_SRC`: source에만 있음
- `ONLY_DEST`: destination에만 있음
- `INIT`: 아직 비교되지 않음

기본 content 판정은 regular file에 대해 `size + mtime(+nsec 가능 시)`이다.
`--contents`를 주면 같은 크기 파일도 실제 byte를 읽어 비교한다.

symlink는 타입이 link인 경우 link target 문자열을 비교한다.
directory는 size 비교 대상에서 제외하고, 타입/소유자/권한/시간 메타데이터를 중심으로 비교한다.

## 6) 구현 아키텍처

`dsync`는 `src/dsync/dsync.c`에 구현되어 있으며, 큰 흐름은 다음과 같다.

### 6.1 초기화/옵션 파싱

1. `MPI_Init`, `mfu_init`
2. `mfu_file_t`, `mfu_walk_opts_t`, `mfu_copy_opts_t` 생성
3. CLI 옵션 파싱
4. 기본 copy 옵션 설정
   - `copy_opts->preserve = true`
   - `copy_opts->do_sync = 1`
5. source/destination 인자 검증

### 6.2 경로 준비

1. `mfu_param_path_set()`으로 source/destination path 정규화
2. destination parent가 writable인지 확인
3. `--link-dest`가 있으면 destination과 같은 filesystem에서 hardlink 가능한지 확인
4. source/destination mtime nsec 비교 가능 여부 확인

### 6.3 Walk Phase

1. source tree walk
   - `mfu_flist_walk_param_paths()`
   - source walk 오류가 있으면 `--delete`를 자동 비활성화
2. destination tree walk
   - destination은 항상 dereference하지 않음
3. `--link-dest` 사용 시 link-dest tree도 walk

source walk 결과가 비어 있으면, source 오타로 destination 전체를 지우는 사고를 막기 위해 오류로 종료한다.

### 6.4 Remap Phase

walk 결과 `mfu_flist`를 상대 경로 hash 기준으로 rank에 재분배한다.

```text
owner = hash(relative_path) % world_size
```

이 재분배 덕분에 같은 상대 경로의 source item과 destination item이 같은 rank에서 비교된다.

### 6.5 Compare/Plan Phase

각 rank는 자기에게 배정된 상대 경로들에 대해 source/destination 상태를 `strmap`에 기록한다.

생성되는 주요 list:

- `src_cp_list`: destination에 복사해야 할 source 항목
- `dst_remove_list`: 삭제/교체해야 할 destination 항목
- `src_compare_list`, `dst_compare_list`: 내용 비교가 필요한 항목
- `metadata_refresh`: 내용은 같지만 메타데이터 갱신이 필요한 항목
- `dst_same_list`, `link_same_list`, `link_dst_list`: `--link-dest` 처리용 항목

타입이 다르면 destination 항목을 삭제하고 source 항목을 복사하도록 계획한다.
크기나 mtime이 다르면 기본 모드에서는 content가 다르다고 보고 교체 대상으로 넣는다.
`--contents` 모드에서는 파일 chunk 단위로 실제 데이터를 비교한다. 같은 크기의 regular file에서
byte 차이가 발견되면, `--dryrun`/`--link-dest`가 아닌 일반 실행에서는 비교 중 destination의 해당
range를 source 내용으로 overwrite한다.

### 6.6 Execute Phase

`--dryrun`이 아니면 다음 순서로 destination을 갱신한다.

1. `--delete`가 켜져 있으면 destination에만 있는 항목을 `dst_remove_list`에 추가
2. `dst_remove_list` 삭제 (`mfu_flist_unlink`)
3. `src_cp_list` 복사 (`mfu_flist_copy`)
4. `--link-dest` 사용 시 hardlink 생성 (`mfu_flist_hardlink`)
5. 기존 항목 중 메타데이터만 다른 항목을 `mfu_flist_file_sync_meta()`로 갱신

주의: `--contents` 일반 실행에서는 같은 크기의 파일 내용 차이가 compare phase에서 이미 overwrite될
수 있다. 따라서 모든 data write가 execute phase의 `mfu_flist_copy()`에서만 발생하는 구조는 아니다.

## 7) `--contents`와 chunk 병렬성

`--contents`를 사용하면 regular file 내용을 직접 비교한다.

구현은 `mfu_file_chunk_list_alloc()`으로 파일을 `--chunksize` 단위 chunk list로 나누고,
각 chunk를 rank들이 나누어 `mfu_compare_contents()`로 비교한다.

특징:

- 파일 하나가 커도 여러 rank가 서로 다른 byte range를 비교할 수 있음
- 비교 결과는 file 단위 logical OR로 합쳐짐
- 하나라도 다른 chunk가 있으면 해당 파일은 `CONTENT=DIFFER`
- 일반 실행에서는 다른 chunk를 발견하는 즉시 source buffer를 destination offset에 write할 수 있음
- `--dryrun` 또는 `--link-dest` 모드에서는 비교 중 destination overwrite를 하지 않음

## 8) `--batch-files`의 의미

`--batch-files`는 scan이나 planning을 batch로 나누는 옵션이 아니다.

`dsync`는 먼저 source/destination 전체를 walk하고, 전체 metadata를 remap/compare한 뒤,
실제 copy 단계에서 `src_cp_list`를 N개 파일 단위로 나눠 처리한다.

따라서:

- 메타데이터 walk 비용은 batch 유무와 거의 동일
- planning 메모리 사용량도 whole-tree 기준
- copy 중단 후 재실행 시 이미 복사된 파일은 다음 비교에서 제외될 수 있어 checkpoint처럼 활용 가능

## 9) `--link-dest` 증분 백업

`--link-dest DIR`는 source와 같은 파일이 `DIR`에 있으면 새로 복사하지 않고 destination에 hardlink를 만든다.

예:

```bash
# 최초 백업
mpirun -np 32 dsync /src /backup/full

# 증분 백업
mpirun -np 32 dsync --link-dest /backup/full /src /backup/inc-001
```

동작:

1. source, destination, link-dest를 모두 walk
2. source와 link-dest의 같은 상대 경로를 비교
3. 같은 regular file은 `link_dst_list`에 추가
4. destination에는 `link-dest` 파일을 가리키는 hardlink 생성
5. directory와 symlink는 hardlink 대상이 아니므로 실제 copy 경로를 사용

제약:

- destination과 link-dest가 같은 filesystem이어야 hardlink 가능
- DAOS path와 함께 사용할 수 없음
- 조건이 맞지 않으면 link-dest를 비활성화하고 일반 copy로 fallback

## 10) DAOS 경로

DAOS 지원 빌드에서는 source 또는 destination이 DAOS인 경우 별도 경로로 처리한다.

주요 차이:

- `dsync_daos_setup()`으로 DAOS 연결/API를 결정
- DAOS object list를 walk한 뒤 `mfu_daos_flist_sync()`로 동기화
- DAOS 경로에서는 항상 content 비교 모드로 동작
- `--link-dest`는 DAOS와 함께 사용할 수 없음
- DAOS object API에서는 `--delete`가 지원되지 않음

## 11) 실행 예제

### 11.1 기본 동기화

```bash
mpirun -np 128 dsync /path/to/src /path/to/dst
```

### 11.2 변경 계획만 확인

```bash
mpirun -np 32 dsync --dryrun /path/to/src /path/to/dst
```

### 11.3 destination 초과 항목 삭제

```bash
mpirun -np 64 dsync --delete /path/to/src /path/to/dst
```

### 11.4 내용 기준 비교

```bash
mpirun -np 64 dsync --contents /path/to/src /path/to/dst
```

### 11.5 대규모 copy 배치 처리

```bash
mpirun -np 128 dsync \
  --batch-files 100000 \
  /path/to/src /path/to/dst
```

### 11.6 큰 파일 chunk 크기 조정

```bash
mpirun -np 128 dsync \
  --chunksize 64MB \
  --bufsize 8MB \
  /path/to/src /path/to/dst
```

### 11.7 sparse file 보존

```bash
mpirun -np 64 dsync --sparse /path/to/src /path/to/dst
```

## 12) 운영 워크플로우

권장 순서:

1. 처음에는 `--dryrun`으로 변경 규모와 오류 여부 확인
2. destination을 source와 완전히 같게 만들어야 하면 `--delete` 추가
3. mtime 신뢰가 어렵거나 동일 크기 파일 변조 가능성이 있으면 `--contents` 사용
4. 큰 파일이 많으면 `--chunksize`를 workload에 맞게 조정
5. 파일 수가 매우 많고 장시간 copy가 예상되면 `--batch-files` 사용
6. 증분 백업은 `--link-dest`로 저장 공간 절약 가능 여부 확인
7. 실행 후 로그의 walk/copy/error 요약 확인

주의할 점:

- `--delete`는 source walk가 불완전하면 위험하므로, 코드가 자동으로 비활성화한다.
- source가 비어 있으면 오류로 종료한다.
- `--batch-files`는 메모리 피크를 scan 단계에서 낮추는 옵션이 아니다.
- 모든 rank가 source와 destination을 볼 수 없는 split-mount 환경에서는 `nsync`를 사용해야 한다.

## 13) 현재 구현 범위와 한계

구현됨:

- POSIX source/destination tree 동기화
- MPI 기반 distributed walk/remap/compare
- regular file content 비교와 chunk 병렬 비교
- symlink target 비교
- destination delete/copy/hardlink/meta update
- sparse/direct/noatime/xattr 옵션
- DAOS backend 경로

한계:

- whole-tree metadata를 먼저 RAM에 올려 비교하므로 매우 큰 tree에서는 메모리 압박이 클 수 있음
- `--batch-files`는 copy batching이므로 scan/planning 메모리 사용량을 줄이지 않음
- `--link-dest`는 같은 filesystem hardlink 조건에 의존
- DAOS 경로는 POSIX 경로와 지원 옵션이 다름
- split-mount 환경은 지원 대상이 아니며, 그 경우 `nsync`가 필요

## 14) `--chown` / `--chmod` (destination 소유권·권한 override)

기본적으로 `dsync`는 source의 ownership·permission을 destination에 그대로 보존한다.
`--chown` / `--chmod`를 주면, **보존 대신 사용자가 지정한 값으로 destination 항목의 소유권·권한을 설정**한다.
source 디스크 데이터는 변경되지 않는다(메모리상 메타데이터만 override하여 복사/메타 동기화 경로에 태운다).

### 14.1 `--chown USER:GROUP`

- `USER`/`GROUP`은 **이름 또는 숫자 id** 모두 가능 (`alice:developers` 또는 `1001:1002`).
- **부분 지정**: `--chown alice`(uid만, gid는 source 유지), `--chown :developers`(gid만, uid는 source 유지).
- 신규 복사 항목뿐 아니라, 이미 존재하지만 소유권이 다른 destination 항목도 맞춰진다(재실행 수렴).
- symlink 자체도 `lchown`으로 설정된다(타깃은 따라가지 않음).
- **권한**: 임의의 다른 사용자로 바꾸려면 root(또는 `CAP_CHOWN`)가 필요하다. 비권한으로 실행하면 chown은 조용히 건너뛰고(파일은 실행 사용자 소유로 남음) 종료코드는 0이다.
- 이름은 rank 0에서 한 번 조회해 전 rank에 broadcast하므로(NSS/LDAP 일관성) 모든 rank가 동일 id를 쓴다. 알 수 없는 이름이면 오류로 종료한다.

### 14.2 `--chmod SPEC`

- **octal 절대값**만 지원한다. 두 가지 형식:
  - `--chmod 0750` — 디렉토리·파일에 **동일하게** 적용(파일에 실행비트가 붙거나 디렉토리에서 traverse 비트가 빠질 수 있으니 주의).
  - `--chmod D0755,F0644` — **디렉토리(`D`)와 파일(`F`)을 분리** 지정(권장). 한쪽만 줘도 된다(`D0755`는 디렉토리만, 파일은 source 유지).
- 특수비트(setuid `4000`/setgid `2000`/sticky `1000`)는 4자리 octal로 지정 가능(`F4755`).
- **symlink는 chmod하지 않는다**(타입 비트는 보존). 디렉토리의 타입/실행 라우팅도 보존된다.
- 제한적 디렉토리 모드(예 `D0500`)를 줘도 내부 파일은 정상 생성되고, 동기화 종료 시 디렉토리가 지정 모드로 마무리된다.
- 파싱은 로컬·결정적이라 MPI broadcast가 필요 없다. 잘못된 spec(비-octal, 4자리 초과, 충돌 토큰 등)은 오류로 종료한다.

### 14.3 제약

- `--link-dest`와 함께 사용할 수 없다(하드링크는 link-dest와 inode를 공유하므로 override가 link-dest 트리를 오염시킨다) — 조합 시 오류로 종료.
- DAOS object API 경로에서는 지원하지 않는다(POSIX/DFS 경로에서만 동작).
- `--chown`과 `--chmod`는 함께 쓸 수 있고 서로 독립적이다.

### 14.4 예제

```bash
# destination 전체를 특정 소유자로
mpirun -np 64 dsync --chown dataadmin:datagrp /src /dst

# 디렉토리 0750, 파일 0640으로 권한 통일
mpirun -np 64 dsync --chmod D0750,F0640 /src /dst

# 소유권과 권한을 동시에
mpirun -np 128 dsync --chown 1001:1001 --chmod D0755,F0644 /src /dst

# 그룹만 변경(uid는 source 유지)
mpirun -np 32 dsync --chown :projteam /src /dst
```
