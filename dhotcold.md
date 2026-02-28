# dhotcold

## 개요

`dhotcold`는 입력 `PATH`의 depth-1 디렉토리들을 대상으로 hot/cold 용량을 분석하는 MPI 기반 도구다.

- depth-1 디렉토리 선택: 순차 처리
- 각 depth-1 디렉토리 내부: 재귀 스캔 + MPI 병렬 집계
- 출력: CSV 결과 파일(topdir별 1행 + 헤더 1행)

## 핵심 동작

1. root 권한 검증 (모든 rank)
2. 입력 경로 유효성 검증 (`mfu_param_path_set_all`)
3. rank 0가 output 파일을 `O_TRUNC`로 열어 결과를 기록(기존 파일이면 덮어씀)
4. rank 0가 depth-1 디렉토리 목록 수집 (`--exclude` depth-1 only 적용)
5. 각 topdir에 대해:
   - `--unique-inode` 미사용:
     - libcircle 기반 스트리밍 스캔으로 엔트리를 저장하지 않고 즉시 집계
   - `--unique-inode` 사용:
     - `mfu_flist_walk_path` + `(st_dev, st_ino)` 글로벌 dedup(gather/sort)
   - regular file만 용량 분석 대상
   - `--time-field` 기준으로 cold 판정
   - MPI Allreduce로 총합/히스토그램/에러 합산
   - rank 0가 CSV 결과 1행 기록

## Cold 판정

- 기준: `age_days >= --days`
- `age_days = floor((now - file_time) / 86400)`
- `now`는 rank 0에서 계산 후 MPI Bcast

## 출력 형식

출력 파일은 CSV 형식이며, 첫 줄에 컬럼명이 기록된다.

```csv
directory_path,total_capacity(<unit>),cold_capacity(<unit>),cold_ratio,error_count,histogram_capacity_0(<unit>),histogram_capacity_1(<unit>),histogram_capacity_2(<unit>),histogram_capacity_3(<unit>),histogram_capacity_4(<unit>)
```

필드 설명:

- `directory_path`: 분석한 depth-1 디렉토리 경로
- `total_capacity`: regular file 총 용량
- `cold_capacity`: cold regular file 총 용량
- `cold_ratio`: `cold_capacity / total_capacity` (0~1)
- `error_count`: 해당 topdir 분석 중 누적 오류 수
- `hist0..hist4`: cold 파일 용량 히스토그램(5개 구간)

히스토그램 규칙:

- cold 파일(`age_days >= days`)만 누적
- `delta = age_days - days`
- `bucket_width = ceil(days / 5)` (`days=0`이면 1)
- `bin = min(delta / bucket_width, 4)`

## 옵션

- `-o, --output <file>`: 결과 파일(기존 파일이면 덮어씀)
- `-d, --days <N>`: cold 기준 일수 (기본 30)
- `-u, --unit <B|KB|MB|GB|TB|PB>`: 출력 용량 단위 (기본 B)
- `--print`: rank 0 stdout에 정렬된 테이블 형태로 결과 출력
- `-e, --exclude <regex>`: depth-1 디렉토리 제외 정규식
- `-t, --time-field <atime|mtime|ctime|btime>`: 시간 필드 (기본 atime)
- `--progress <N>`: 진행률 출력 주기(초)
- `--strict`: 에러 발생 topdir에서 즉시 중단
- `--xdev`: topdir와 동일 filesystem만 집계 (기본 활성)
- `--follow-mounts`: `--xdev` 해제
- `--unique-inode`: `(st_dev, st_ino)` 기반 글로벌 dedup (gather/sort)
- `-v, --verbose`: verbose
- `-q, --quiet`: quiet
- `-h, --help`: 도움말

## 에러 처리 정책

- 기본(best-effort): 에러를 `error_count`에 누적하고 가능한 범위 내에서 계속 진행
- `--strict`: 에러가 나온 topdir에서 중단하고 비정상 종료

## 구현 포인트

- 병렬 스캔:
  - 기본: 스트리밍 집계 (메모리 O(1) 수준, 엔트리 비보관)
  - `--unique-inode`: `mfu_flist_walk_path` 사용
- 시간 필드:
  - `atime/mtime/ctime`: `lstat` + `mfu_stat_get_*`
  - `btime`: Linux `statx` 사용
- symlink 미추적: walk 옵션 `dereference=0`
- 출력 병합: rank 0 단일 writer
- 출력 파일: CSV(헤더 포함, comma 구분)

## 사용 예시

```bash
mpirun -np 32 dhotcold \
  --output hotcold.txt \
  --days 30 \
  --time-field atime \
  --exclude ".*ignore.*" \
  /data/lake
```

```bash
mpirun -np 64 dhotcold \
  --output hotcold_mtime_gb.txt \
  --days 60 \
  --time-field mtime \
  --unit GB \
  --follow-mounts \
  /data/lake
```

## 제한 사항

- `--unique-inode`는 글로벌 gather/sort 방식이라 메모리 사용량이 크게 증가할 수 있음
- 매우 큰 입력에서 MPI `int` count 범위를 넘는 경우 rank-local dedup fallback 후 에러 카운트를 증가시킴
- `--xdev` 동작은 모드에 따라 다름:
  - 스트리밍 모드: 다른 filesystem 디렉토리로 재귀 진입을 차단
  - `--unique-inode` 모드: 스캔 후 파일 단위 필터 방식
- `btime`은 Linux/statx 환경에 의존
