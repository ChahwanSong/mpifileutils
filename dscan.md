# dscan

`dscan`은 `mpifileutils`용 MPI 기반 디렉토리 스캔 도구입니다.
대상 디렉토리를 재귀적으로 순회하며 분석 정보를 계산하고 JSON 리포트를 생성합니다.

대규모(수억~10억 항목) 스캔을 위해 스트리밍 방식으로 설계되어 있습니다.
전체 항목 목록을 메모리에 유지하거나 rank 0으로 모으지 않으며,
모든 통계는 고정 크기 누적기(accumulator)로 스캔 중에 갱신됩니다.

## 기능 (Features)

- 파일 크기 히스토그램 (크기 버킷별 일반 파일 **개수**)
- atime/mtime/ctime 용량 히스토그램 (나이 버킷별 일반 파일 크기의 **byte 합계**; 파일만)
- 손상 경로(broken path) 탐지
  - 비정상 크기 (abnormal size)
  - 누락된 경로 (missing path)
  - 비정상 타임스탬프 (abnormal timestamps)
  - 읽을 수 없는 경로 (unreadable path)
  - 전체 개수는 항상 정확히 집계되며, 리포트에 나열되는 목록은
    `--broken-limit`으로 상한이 적용됩니다
- `--batch-files` 기반 배치 진행 (`dsync`/`nsync`와 동일한 옵션 스타일)
  - 약 N개 항목 단위로 배치를 완료할 때마다 진행 로그 출력 (기본값 100만)
- 파일로 JSON 리포트 출력
- `--print` 옵션으로 선택적 터미널 요약 출력

이전 구현 대비 제외된 기능:

- atime/mtime/ctime 기준 가장 오래된 top-K 목록
- 디렉토리 서브트리 크기 집계.
  이 기능은 rank 0에서 파일수 × 경로깊이 × log(디렉토리수) 규모의
  직렬 문자열 비교를 요구해 대규모 스캔의 최대 병목이었습니다.
- 손상 판정을 위한 항목당 재검사 syscall (`lstat` 재호출, `open`/`opendir`/`readlink`).
  손상 판정은 walk 단계에서 수집한 `lstat` 데이터와 스캔 중 발생한
  실제 에러(`opendir`/`lstat` 실패)만으로 수행합니다.

## 명령줄 인터페이스 (CLI)

```bash
mpirun -np <N> dscan --directory <path> --output <file> [options]
```

필수 옵션:

- `--directory <path>` (`-d`): 스캔할 디렉토리
- `--output <file>` (`-o`): JSON 출력 파일 경로

선택 옵션:

- `--print` (`-p`): 사람이 읽기 좋은 요약을 stdout(rank 0)에 출력
- `--batch-files <N>` (`-b`): 약 N개 항목 단위 배치로 진행 상황을 로그 출력
  (기본값: `1000000`; `0`이면 배치 진행 로그 비활성화)
- `--broken-limit <N>`: 리포트에 나열할 손상 경로 최대 개수 (기본값: `100`;
  `0`이면 목록 없이 개수만 집계)
- `--verbose` (`-v`): 상세 로깅
- `--quiet` (`-q`): 최소 로깅
- `--help` (`-h`): 사용법 출력

## 손상 경로 판정 기준 (Broken Path Criteria)

다음 조건 중 하나 이상에 해당하면 `dscan`은 해당 항목을 손상(broken)으로 표시합니다:

- `abnormal_size`
  - 일반 파일 크기가 `1 PiB`(`2^50` byte)보다 큰 경우
- `missing`
  - 스캔 중 `lstat`이 `ENOENT`/`ENOTDIR`로 실패한 경우
- `abnormal_time`
  - `atime`, `mtime`, `ctime` 중 하나라도 `now - 10년`보다 오래된 경우
  - 또는 타임스탬프가 미래인 경우 (`> now`)
- `unreadable`
  - 디렉토리 `opendir`이 실패했거나 `lstat`이 그 외 오류로 실패한 경우

판정은 walk 중 수집된 데이터와 실제 발생한 스캔 오류만 사용합니다.
별도의 재검사 syscall을 수행하지 않으므로, 항목당 메타데이터 연산은
`lstat` 1회입니다(이전 구현은 항목당 최대 3~4회).

`broken_paths_total`은 항상 정확한 전체 개수이며, `broken_paths` 배열은
`--broken-limit` 개수까지만(경로 정렬 순) 포함됩니다.

## 출력 형식 (JSON)

최상위 키:

- `directory`: 스캔한 루트 경로
- `generated_at_epoch`: 리포트 생성 시각 (epoch 초)
- `thresholds`: 검사에 사용된 임계값 상수
- `summary`: 전체 항목 카운터 (`scan_errors` 포함)
- `file_size_histogram`: 파일 크기 버킷 개수 (파일 수; `count` 필드)
- `time_histograms`: atime/mtime/ctime 용량 버킷 (파일 크기 byte 합계; `bytes` 필드; 파일만)
- `broken_paths_total`: 손상 항목 전체 개수 (정확한 값)
- `broken_paths_limit`: 적용된 `--broken-limit`
- `broken_paths`: 사유 라벨이 포함된 손상 항목 배열 (상한 적용)

### 예시 JSON (요약)

```json
{
  "directory": "/data/project",
  "generated_at_epoch": 1772360000,
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
    "total_other": 56,
    "scan_errors": 0
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
  "broken_paths_total": 1,
  "broken_paths_limit": 100,
  "broken_paths": [
    {
      "path": "/data/project/bad/file",
      "reasons": ["abnormal_time"]
    }
  ]
}
```

## 구현 아키텍처 (Implementation Architecture)

`dscan`은 `src/dscan/dscan.c`에 구현되어 있습니다.
`nsync`의 frontier 스캔과 같은 패턴의 분산 스트리밍 walk를 사용합니다.

### 1) 분산 스트리밍 walk 단계

- 디렉토리는 경로 해시(`hash(path) % ranks`)로 소유 rank가 결정됩니다
- 각 rank는 자기 소유 디렉토리를 `opendir`/`readdir`/`lstat`으로 스캔하고,
  발견한 항목을 **즉시** 로컬 누적기에 반영합니다 (전체 목록 미보관):
  - 타입별 카운터, 크기/시간 히스토그램
  - 손상 경로 상한 목록 + 정확한 전체 카운터
- 발견된 하위 디렉토리는 소유 rank로 `MPI_Alltoallv` 라운드 교환을 통해 전달됩니다
- 라운드는 항목 quota와 교환 버퍼 byte 상한(64 MiB)으로 제한되어
  collective 동기화가 짧은 주기로 이루어집니다

### 2) 배치 진행 (`--batch-files`)

- 기본값은 100만 항목이며 `-b 0`으로 비활성화할 수 있습니다
- 라운드 경계마다 전역 스캔 항목 수를 `MPI_Allreduce`로 집계
- 누적 스캔 수가 배치 크기 N을 넘을 때마다 배치 완료로 간주하고
  rank 0(launcher console)에서 진행 로그를 출력:
  누적 항목 수, 처리 속도(entries/s), 대기 중 디렉토리 수, 배치 소요 시간
- 배치 경계는 디렉토리 단위 granularity로 근사됩니다

### 3) 병합 단계 (gather-all 제거)

- 요약 카운터와 모든 히스토그램: 고정 크기 배열 1개를 `MPI_Reduce`(SUM)
- 손상 경로: 각 rank의 상한 목록만 gather, rank 0이 경로 정렬 후
  `--broken-limit`까지 나열 (전체 개수는 reduce로 정확히 집계)

### 4) 출력 단계

- rank 0이 `--output`에 JSON 리포트 작성
- `--print`가 설정되면 rank 0이 읽기 좋은 요약 출력

### 규모 특성

- rank당 메모리: O(broken-limit + 대기 디렉토리 큐) —
  전체 항목 수와 무관 (수십~수백 MB 수준)
- rank 0으로의 통신량: O(broken-limit × ranks + 히스토그램) —
  전체 항목 수와 무관
- 항목당 파일시스템 연산: `lstat` 1회 (+ 디렉토리당 `opendir`/`readdir`)
- 이전 구현의 `MPI_Gatherv` int 카운터 한계(전체 메타데이터 2 GiB,
  약 2천만~4천만 항목에서 실패)가 제거되었습니다

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
  --print
```

멀티노드 실행 (스캔 대상 디렉토리가 모든 노드에 같은 경로로 마운트되어 있어야 합니다):

```bash
mpirun --hostfile hf -np 32 dscan \
  --directory /cephfs/project \
  --output /tmp/dscan_report.json \
  --batch-files 5000000
```

## 참고 (Notes)

- `dscan` 출력 파일은 rank 0이 작성합니다.
- 전체 메타데이터를 rank 0으로 모으지 않으므로, rank 0의 메모리 사용량은
  트리 크기와 무관하게 일정합니다.
- 결과(요약/히스토그램)는 rank 수와 배치 설정에 관계없이 동일합니다.
  단, `broken_paths` **목록**은 전체 손상 수가 `--broken-limit`을 초과하면
  샘플이므로 rank 수에 따라 달라질 수 있습니다 (`broken_paths_total`은 항상 정확).
- 스캔 도중 파일시스템이 변경되면 (entries가 사라지는 등) `missing`
  손상 항목과 `scan_errors`로 나타납니다.
