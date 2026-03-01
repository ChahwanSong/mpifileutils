# dscan

`dscan` is an MPI-based directory scanning tool for `mpifileutils`.
It recursively walks a target directory, computes analytics, and writes a JSON report.

## Features

- File size histogram (regular files)
- atime/mtime/ctime histograms (regular files + directories)
- Oldest top-K by atime/mtime/ctime
  - Each entry includes path, type, timestamps, and size
  - Directory size is computed as the sum of regular-file sizes under that subtree
- Broken path detection list
  - abnormal size
  - missing path
  - abnormal timestamps
  - unreadable path
- JSON report output to file
- Optional pretty-printed terminal output with `--print`

## Command Line Interface

```bash
mpirun -np <N> dscan --directory <path> --output <file> [options]
```

Required options:

- `--directory <path>` (`-d`): directory to scan
- `--output <file>` (`-o`): JSON output file path

Optional options:

- `--print` (`-p`): print human-readable summary on stdout (rank 0)
- `--top-k <number>` (`-k`): top-K oldest entries per timestamp field (default: `10`)
- `--verbose` (`-v`): verbose logging
- `--quiet` (`-q`): quiet logging
- `--help` (`-h`): usage message

## Broken Path Criteria

`dscan` marks an item as broken if one or more of the following conditions are true:

- `abnormal_size`
  - Regular file size is larger than `1 PiB` (`2^50` bytes)
- `missing`
  - Path does not exist at check time (`ENOENT`/`ENOTDIR`)
- `abnormal_time`
  - Any of `atime`, `mtime`, or `ctime` is older than `now - 10 years`
  - Or any timestamp is in the future (`> now`)
- `unreadable`
  - File/directory/symlink cannot be opened/read

## Output Format (JSON)

Top-level keys:

- `directory`: scanned root path
- `generated_at_epoch`: report generation time (epoch seconds)
- `top_k`: configured top-K
- `thresholds`: threshold constants used for checks
- `summary`: total entry counters
- `file_size_histogram`: file-size bucket counts
- `time_histograms`: atime/mtime/ctime bucket counts
- `oldest`: top-K arrays for `atime`, `mtime`, `ctime`
- `broken_paths`: array of broken entries with reason labels

### Example JSON (abridged)

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
        "count": 777
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

## Implementation Architecture

`dscan` is implemented in `src/dscan/dscan.c` and follows the `mpifileutils` tool pattern.

### 1) MPI and walk phase

- Initialize MPI and `mfu`
- Walk the target directory using `mfu_flist_walk_path`
- Each rank collects local metadata:
  - path
  - type
  - size
  - atime/mtime/ctime
  - broken flags

### 2) Distributed gather phase

- Local records are serialized as:
  - fixed-size metadata records (`dscan_wire_item_t`)
  - variable-size path byte buffer
- `MPI_Gather` + `MPI_Gatherv` gathers all local records to rank 0

### 3) Rank-0 analytics phase

- Rebuild full item list
- Compute directory aggregate sizes from file paths
- Compute histograms
- Compute oldest top-K lists for each timestamp field
- Build broken-path index

### 4) Output phase

- Rank 0 writes JSON report to `--output`
- If `--print` is set, rank 0 prints a readable summary

## Histogram Buckets

### File size histogram (bytes)

Buckets use these upper bounds:

- 4 KiB
- 64 KiB
- 1 MiB
- 16 MiB
- 256 MiB
- 1 GiB
- 16 GiB
- 256 GiB
- 4 TiB
- and one final `INF` bucket

### Time histograms (age in days)

Age buckets use these upper bounds:

- 1
- 7
- 30
- 90
- 180
- 365
- 1095
- 3650
- and one final `INF` bucket

## Build and Run

From repository root:

```bash
cmake -S . -B build
cmake --build build -j
```

Run:

```bash
mpirun -np 8 build/src/dscan/dscan \
  --directory /path/to/scan \
  --output /tmp/dscan_report.json \
  --top-k 20 \
  --print
```

## Notes

- `dscan` output file is written by rank 0.
- The current implementation gathers full metadata to rank 0 for final aggregation.
- For very large scans, memory pressure on rank 0 can be significant.
