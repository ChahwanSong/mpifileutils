# nsync

`nsync` is an MPI-based synchronization tool for split-mount topologies.

Unlike `dsync`, `nsync` does not require every rank to mount both source and
destination paths. It is designed for cases where:

- source path is mounted only on source-role ranks
- destination path is mounted only on destination-role ranks
- source and destination ranks are connected over MPI network

`nsync` compares metadata in parallel, plans actions, and streams file data from
source owners to destination owners over MPI.

## Key Features

- Split-mount aware role model (`src` / `dst`)
- Automatic or explicit role assignment
- Distributed metadata compare and action planning
- Source-to-destination MPI streaming copy path
- `--delete` support for destination-only entries
- `--contents` support (SHA256 digest compare)
- Metadata finalize on destination (mode, uid/gid, timestamps)
- Memory-bounded batch mode (`--batch-files`)
- Batch checkpoint/resume (`.nsync.batch.state`)

## Command Line

```bash
mpirun -np <N> nsync [options] SRC DEST
```

Options:

- `--dryrun`: plan and compare only, no filesystem changes
- `-b, --batch-files <N>`: process entries in deterministic batches
- `-D, --delete`: remove destination entries missing on source
- `-c, --contents`: compare file contents by SHA256
- `--bufsize <SIZE>`: copy/hash buffer size
- `--chunksize <SIZE>`: minimum work chunk size
- `--progress <N>`: progress update interval in seconds
- `--role-mode <auto|map>`: role assignment mode
- `--role-map <SPEC>`: explicit map (example: `0-1:src,2-3:dst`)
- `--trace`: per-rank stage tracing for debug
- `-v, --verbose`: verbose logs
- `-q, --quiet`: minimal logs
- `-h, --help`: usage

## Role Assignment

### Auto mode

In auto mode, each rank probes path accessibility:

- source-readable and not destination-writable -> `src`
- destination-writable and not source-readable -> `dst`

If a rank can access both or neither path, auto mode rejects configuration.

### Map mode

Use `--role-mode map --role-map` to force roles by MPI world rank.
This is useful for local testing and controlled deployments.

Examples:

- `0:src,1:dst`
- `0-3:src,4-7:dst`

## Compare Policy

- Regular file:
  - default: `size + mtime_nsec`
  - with `--contents`: `size + SHA256 digest`
- Symlink: link target string compare
- Directory: metadata compare

## Batch Mode and Checkpoint

`--batch-files N` bounds memory by processing deterministic hash-based batches.

- batch id: `hash(relpath) % batch_count`
- spool files are used for one-scan-per-side batch loading
- per-batch planner and action state is released before next batch

Checkpoint file: `DEST/.nsync.batch.state`

- records source/destination paths, option hash, batch count, progress
- used to resume from last completed batch when configuration matches
- stale/mismatched checkpoint is ignored safely with warning

## Performance Notes

- For very large trees, use `--batch-files` to cap planner memory.
- Increase `--bufsize` on high-bandwidth networks/storage.
- Use `--contents` only when content-level verification is needed.
- Keep binaries and runtime libraries identical across all participating pods/nodes.

## Example: Split Pods

If source and destination are on different pods:

```bash
mpirun --host <src-pod-ip>,<dst-pod-ip> -np 2 \
  nsync /src /dst
```

If auto mode is ambiguous in a shared mount test environment:

```bash
mpirun -np 2 nsync \
  --role-mode map --role-map 0:src,1:dst \
  /tmp/src /tmp/dst
```

