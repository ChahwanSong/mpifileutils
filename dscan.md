# dscan

`dscan`은 `mpifileutils`용 MPI 기반 디렉토리 스캔 도구입니다.
대상 디렉토리를 재귀적으로 순회하며 분석 정보를 계산하고 JSON 리포트를 생성합니다.

## 기능 (Features)

- 파일 크기 히스토그램 (크기 버킷별 일반 파일 **개수**)
- atime/mtime/ctime 용량 히스토그램 (나이 버킷별 일반 파일 크기의 **byte 합계**; 파일만)
- atime/mtime/ctime 기준 가장 오래된 top-K
  - 각 항목은 경로, 타입, 타임스탬프, 크기를 포함
  - 디렉토리 크기는 해당 서브트리 내 일반 파일 크기의 합으로 계산
- 손상 경로(broken path) 탐지 목록
  - 비정상 크기 (abnormal size)
  - 누락된 경로 (missing path)
  - 비정상 타임스탬프 (abnormal timestamps)
  - 읽을 수 없는 경로 (unreadable path)
- 파일로 JSON 리포트 출력
- `--print` 옵션으로 선택적 터미널 요약 출력

## 명령줄 인터페이스 (CLI)

```bash
mpirun -np <N> dscan --directory <path> --output <file> [options]
```

필수 옵션:

- `--directory <path>` (`-d`): 스캔할 디렉토리
- `--output <file>` (`-o`): JSON 출력 파일 경로

선택 옵션:

- `--print` (`-p`): 사람이 읽기 좋은 요약을 stdout(rank 0)에 출력
- `--top-k <number>` (`-k`): 타임스탬프 필드별 가장 오래된 top-K 개수 (기본값: `10`)
- `--verbose` (`-v`): 상세 로깅
- `--quiet` (`-q`): 최소 로깅
- `--help` (`-h`): 사용법 출력

## 손상 경로 판정 기준 (Broken Path Criteria)

다음 조건 중 하나 이상에 해당하면 `dscan`은 해당 항목을 손상(broken)으로 표시합니다:

- `abnormal_size`
  - 일반 파일 크기가 `1 PiB`(`2^50` byte)보다 큰 경우
- `missing`
  - 검사 시점에 경로가 존재하지 않는 경우 (`ENOENT`/`ENOTDIR`)
- `abnormal_time`
  - `atime`, `mtime`, `ctime` 중 하나라도 `now - 10년`보다 오래된 경우
  - 또는 타임스탬프가 미래인 경우 (`> now`)
- `unreadable`
  - 파일/디렉토리/심볼릭링크를 열거나 읽을 수 없는 경우

## 출력 형식 (JSON)

최상위 키:

- `directory`: 스캔한 루트 경로
- `generated_at_epoch`: 리포트 생성 시각 (epoch 초)
- `top_k`: 설정된 top-K
- `thresholds`: 검사에 사용된 임계값 상수
- `summary`: 전체 항목 카운터
- `file_size_histogram`: 파일 크기 버킷 개수 (파일 수; `count` 필드)
- `time_histograms`: atime/mtime/ctime 용량 버킷 (파일 크기 byte 합계; `bytes` 필드; 파일만)
- `oldest`: `atime`, `mtime`, `ctime`별 top-K 배열
- `broken_paths`: 사유 라벨이 포함된 손상 항목 배열

### 예시 JSON (요약)

```json
{
  "directory": "/data/project",
  "generated_at_epoch": 1772360000,
  "top_k": 10,
  "thresholds": {
    "abnormal_size_bytes": 1125899906842624,
    "time_past_limit_epoch": 1457000000,
    "time_future_limit_epoch": 1772360000
  },
  "summary": {
    "total_entries": 123456,
    "total_files": 110000,
    "total_directories": 13000,
    "total_symlinks": 400,
    "total_other": 56
  },
  "file_size_histogram": [
    {
      "bucket": "[0,4096]",
      "lower_inclusive": 0,
      "upper_inclusive": 4096,
      "count": 1000
    }
  ],
  "time_histograms": {
    "atime": [
      {
        "bucket": "[0d,1d]",
        "min_age_days": 0,
        "max_age_days": 1,
        "bytes": 81920000
      }
    ],
    "mtime": [],
    "ctime": []
  },
  "oldest": {
    "atime": [
      {
        "path": "/data/project/old/file.bin",
        "type": "file",
        "size_bytes": 4294967296,
        "atime": 1600000000,
        "mtime": 1600000000,
        "ctime": 1600000000
      }
    ],
    "mtime": [],
    "ctime": []
  },
  "broken_paths": [
    {
      "path": "/data/project/bad/file",
      "reasons": ["abnormal_time", "unreadable"]
    }
  ]
}
```

## 구현 아키텍처 (Implementation Architecture)

`dscan`은 `src/dscan/dscan.c`에 구현되어 있으며 `mpifileutils` 도구 패턴을 따릅니다.

### 1) MPI 및 walk 단계

- MPI와 `mfu` 초기화
- `mfu_flist_walk_path`로 대상 디렉토리 순회
- 각 rank는 로컬 메타데이터를 수집:
  - 경로
  - 타입
  - 크기
  - atime/mtime/ctime
  - 손상 플래그(broken flags)

### 2) 분산 gather 단계

- 로컬 레코드를 다음과 같이 직렬화:
  - 고정 크기 메타데이터 레코드 (`dscan_wire_item_t`)
  - 가변 크기 경로 byte 버퍼
- `MPI_Gather` + `MPI_Gatherv`로 모든 로컬 레코드를 rank 0으로 수집

### 3) rank-0 분석 단계

- 전체 항목 목록 재구성
- 파일 경로로부터 디렉토리 집계 크기 계산
- 히스토그램 계산
- 각 타임스탬프 필드별 가장 오래된 top-K 목록 계산
- 손상 경로 인덱스 구성

### 4) 출력 단계

- rank 0이 `--output`에 JSON 리포트 작성
- `--print`가 설정되면 rank 0이 읽기 좋은 요약 출력

## 히스토그램 버킷 (Histogram Buckets)

### 파일 크기 히스토그램 (bytes)

버킷은 다음 상한값을 사용합니다:

- 4 KiB
- 64 KiB
- 1 MiB
- 16 MiB
- 256 MiB
- 1 GiB
- 16 GiB
- 256 GiB
- 4 TiB
- 그리고 마지막 `INF` 버킷 하나

### 시간 히스토그램 (나이, 일 단위)

각 버킷 값은 `atime`/`mtime`/`ctime`이 해당 나이 범위에 속하는 **일반 파일 크기의 byte 합계**(용량)입니다.
일반 파일만 집계하며, 디렉토리와 심볼릭링크는 제외됩니다(JSON 필드는 `count`가 아닌 `bytes`).
반면 "가장 오래된 top-K" 목록에는 디렉토리도 포함됩니다.

나이 버킷은 다음 상한값을 사용합니다:

- 1
- 7
- 30
- 90
- 180
- 365
- 1095
- 3650
- 그리고 마지막 `INF` 버킷 하나

## 빌드 및 실행 (Build and Run)

리포지토리 루트에서:

```bash
cmake -S . -B build
cmake --build build -j
```

실행:

```bash
mpirun -np 8 build/src/dscan/dscan \
  --directory /path/to/scan \
  --output /tmp/dscan_report.json \
  --top-k 20 \
  --print
```

## 참고 (Notes)

- `dscan` 출력 파일은 rank 0이 작성합니다.
- 현재 구현은 최종 집계를 위해 전체 메타데이터를 rank 0으로 수집합니다.
- 매우 큰 규모의 스캔에서는 rank 0의 메모리 부담이 클 수 있습니다.
