# nsync tests

This directory contains shell-based functional tests for `nsync`.

## Scripts

- `test_matrix.sh`
  - basic copy/delete/symlink/metadata behavior
  - `--contents` behavior for same-size/same-mtime files
  - `--batch-files` behavior and no-op convergence
- `test_batch_distribution.sh`
  - verifies large `--batch-files` runs stay distributed across 4 ranks without false imbalance warnings
- `test_resume.sh`
  - interruption and restart behavior in batch mode (best-effort checkpoint hit)

## Usage

```bash
test/tests/test_nsync/test_matrix.sh <MFU_BIN_DIR> [SRC_BASE] [DST_BASE] [TREE_PREFIX]
test/tests/test_nsync/test_batch_distribution.sh <MFU_BIN_DIR> [SRC_BASE] [DST_BASE] [TREE_PREFIX]
test/tests/test_nsync/test_resume.sh <MFU_BIN_DIR> [SRC_BASE] [DST_BASE] [TREE_PREFIX]
```

Examples:

```bash
test/tests/test_nsync/test_matrix.sh /usr/local/openmpi-4.1.8/mpifileutils/bin /tmp /tmp nsync_ci
test/tests/test_nsync/test_batch_distribution.sh /usr/local/openmpi-4.1.8/mpifileutils/bin /tmp /tmp nsync_ci
test/tests/test_nsync/test_resume.sh /usr/local/openmpi-4.1.8/mpifileutils/bin /tmp /tmp nsync_ci
```

## Environment overrides

- `NSYNC_USE_AUTO=1`:
  use auto role assignment instead of explicit role map
- `NSYNC_ROLE_MAP`:
  role map when not using auto mode (default: `0:src,1:dst`)
- `MPIEXEC`:
  MPI launcher binary (`mpirun` or `mpiexec`)
- `MPIEXEC_OPTS`:
  extra MPI launcher options (default: `--allow-run-as-root -np 2`, `test_batch_distribution.sh` defaults to `-np 4`)
- `NSYNC_RESUME_KILL_DELAY`:
  seconds before interrupt in `test_resume.sh` (default: `2`)
