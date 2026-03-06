# nsync Agent Prompt and Implementation Plan

This document is meant to be handed to an AI coding agent.
It includes:
- a copy-paste prompt,
- a practical architecture for split-mount sync,
- an implementation sequence with acceptance criteria,
- and a verification plan.

---

## 1) Copy-Paste Prompt for the AI Agent

Use this prompt as-is:

```text
You are working in the mpifileutils repository.
Implement a new MPI tool named nsync.

Goal:
- nsync synchronizes SRC -> DEST like dsync, but SRC and DEST are mounted on different node sets.
- Source nodes can access SRC only.
- Destination nodes can access DEST only.
- Source and destination nodes are connected by network and are in one MPI job.

Hard constraints:
1) Do not assume a rank can access both SRC and DEST.
2) Do not use dsync's direct compare/copy path that requires both mounts on each rank.
3) Reuse mpifileutils internals where reasonable (logging, utility funcs, flist containers, packing helpers), but implement missing parts directly.
4) Keep changes minimal and incremental. Build and test each phase.

Known codebase caveats (must respect):
- dsync uses mfu_flist_walk_param_paths + mfu_flist_copy, which effectively rely on both paths being usable in the same MPI world context.
- mfu_param_path_check_copy and mfu_flist_walk use MPI_COMM_WORLD assumptions that are not safe for split-mount topology.

Implementation requirements (v1 scope):
- Add tool source at src/nsync/nsync.c.
- Add src/nsync/CMakeLists.txt and wire into src/CMakeLists.txt.
- Add docs at nsync.md (root) and man/nsync.1.
- Update man/CMakeLists.txt and doc/rst/tools.rst to list nsync.

CLI (v1):
- nsync [options] SRC DEST
- --dryrun
- --batch-files <N>
- --delete
- --contents
- --bufsize <SIZE>
- --chunksize <SIZE>
- --progress <N>
- -v/--verbose, -q/--quiet, -h/--help
- --role-mode <auto|map>
- --role-map <spec>   (for dev/testing on one host, e.g. "0-1:src,2-3:dst")

Role model:
- Each rank has role SRC or DST.
- role-mode=auto: detect by local access checks (SRC readable, DEST writable/creatable).
- role-mode=map: parse role-map and assign explicitly by world rank.
- Fail if either role group is empty.

Architecture (must implement):
1) Role/communicator setup
   - Build arrays of SRC world ranks and DST world ranks.
2) Metadata scan
   - Custom scanner for SRC group and DST group (do not depend on mfu_flist_walk for split-mount mode).
   - Produce records keyed by relative path.
3) Distributed compare planning
   - Hash relative path to planner owner rank in MPI_COMM_WORLD.
   - Compare SRC/DST metadata and emit actions: COPY, REMOVE, MKDIR, SYMLINK_UPDATE, META_UPDATE.
4) Data transfer engine
   - SRC owner streams file chunks to DST owner over MPI.
   - DST owner writes file data to DEST path.
   - Use bounded in-flight buffers and robust message tags/types.
5) Metadata finalize
   - Apply mode/uid/gid/timestamps on DEST where required.
6) Error handling
   - Aggregate errors with MPI_Allreduce(min/sum style) and return non-zero on failure.
7) Memory-bounded batching
   - Add dsync-like `--batch-files` processing mode.
   - Ensure peak memory is bounded by per-batch metadata/action windows, not total tree size.

Behavior rules:
- If --contents is OFF: compare regular files via size+mtime_nsec.
- If --contents is ON: compare via SHA256 digest (reuse OpenSSL pattern from ddup).
- If --batch-files is ON: process deterministic key batches and free planner/action state between batches.
- If --delete is ON: remove DEST entries absent in SRC.
- Symlink content compare via link target string.
- Preserve dsync-like logging style where possible.

Out of scope for v1 (explicitly skip):
- --link-dest
- DAOS path support
- xattr/ACL parity with dsync
- advanced restart/checkpointing beyond batch-level resume

Testing requirements:
- Build with cmake and compile nsync.
- Functional tests using local directories and role-map.
- Cases:
  1) basic copy (new files)
  2) metadata-only no-op
  3) --contents catches same-size different-content files
  4) --delete removes destination-only files
  5) symlink update
  6) --batch-files bounds peak memory and preserves correctness
  7) interrupted --batch-files run resumes from last completed batch
- Add shell tests under test/tests/test_nsync/ similar style to dsync tests.

Deliver in small commits per phase, each phase compiling before next.
At the end provide:
- changed files list,
- key design choices and tradeoffs,
- commands used to validate,
- known limitations.
```

---

## 2) Why dsync cannot be reused directly for split mounts

These are the key technical blockers observed in current code:
- `src/dsync/dsync.c` compares source/destination entries after both trees are walked in one MPI world, then calls `mfu_flist_copy` for direct SRC->DEST operations.
- `src/common/mfu_param_path.c:mfu_param_path_check_copy` and `mfu_param_path_set_all` perform world-collective path/stat logic that assumes common accessibility semantics.
- `src/common/mfu_flist_walk.c` is wired around world-collective/libcircle behavior and is not parameterized by subcommunicator.

Conclusion:
- nsync needs a dedicated scan/compare/copy path where source and destination responsibilities are split by rank role.

---

## 3) Recommended v1 Design

### 3.1 Role setup
- Parse `--role-mode`.
- Auto mode per-rank checks:
  - `can_src = access(src_path, R_OK)`
  - `can_dst = (dest exists and writable) OR (parent writable)`
- Classify:
  - SRC-only -> SRC role
  - DST-only -> DST role
  - both/none -> fail in v1 unless role-map is used
- Build:
  - `src_world_ranks[]`, `dst_world_ranks[]`
  - `src_comm`, `dst_comm` via `MPI_Comm_split`

### 3.2 Metadata record
Define a packed wire struct (plus variable-length strings):
- relative path
- type
- size
- mode/uid/gid
- atime/mtime/ctime + nsec
- symlink target (if link)

Use `mfu_pack_uint64/mfu_unpack_uint64` style helpers for explicit wire packing.

### 3.3 Scanning strategy
For v1 simplicity and determinism:
- SRC ranks scan SRC tree and keep entries where `hash(relpath) % src_nranks == src_local_rank`.
- DST ranks scan DEST tree and keep entries where `hash(relpath) % dst_nranks == dst_local_rank`.
- This avoids global queues and still distributes work.

(Tradeoff: each role group may perform redundant directory traversal in very deep trees. Acceptable for v1.)

### 3.4 Compare/planning strategy
- For every metadata record, compute planner owner:
  - `planner = hash(relpath) % world_size`
- Send source and destination records to planner owners.
- Planner merges by key and emits action records:
  - `COPY` (new or changed)
  - `REMOVE` (dest-only, if `--delete`)
  - `MKDIR` / `SYMLINK_UPDATE`
  - `META_UPDATE`

Compute execution owners deterministically:
- `src_owner = src_world_ranks[hash(relpath) % src_nranks]`
- `dst_owner = dst_world_ranks[hash(relpath) % dst_nranks]`

### 3.5 Data transfer protocol
Use explicit message types:
- `MSG_OPEN` (task id, relpath, file size, mode)
- `MSG_DATA` (task id, offset, payload)
- `MSG_CLOSE` (task id, success flag)
- `MSG_ERROR` (task id, errno)

Flow:
- Planner sends COPY tasks to corresponding src/dst owners.
- Source opens SRC file, reads `--bufsize` chunks, sends `MSG_DATA`.
- Destination ensures parent dirs, writes by offset, fsync/close policy per options.

### 3.6 Compare policy
- Regular file:
  - default: size + mtime_nsec
  - `--contents`: SHA256 digest on both sides then compare digest
- Symlink: compare target string
- Directory: metadata compare only

### 3.7 Metadata finalize
After copy/remove:
- apply mode, uid/gid, atime/mtime/ctime to destination entries from source metadata.
- apply directories after file operations (post-order where needed).

### 3.8 Memory-Bounded Batch Strategy (`--batch-files`)
Goal:
- Bound planner/execution memory with very large trees while preserving distributed correctness in split-mount topology.

CLI semantics:
- `--batch-files N`
- `N > 0`: enable batch mode.
- `N == 0` (default): current full-set behavior.

Core approach:
- Use deterministic `batch_id = hash(relpath) % batch_count` so SRC and DST records for the same path always land in the same batch.
- Derive `batch_count = ceil(max(src_items, dst_items) / N)` from global counts.
- Process batches sequentially, freeing metadata/action vectors after each batch.

Pipeline (batch mode):
1) Count pass:
- SRC group counts SRC entries, DST group counts DST entries.
- `MPI_Allreduce` computes global counts and `batch_count`.
2) Spool pass:
- SRC and DST scan again and write packed metadata to local per-batch spool files.
- This trades one extra metadata walk for predictable memory.
3) Batch loop (`b = 0..batch_count-1`):
- Load local spool file for batch `b`.
- Run metadata redistribute (`Alltoallv`) for that batch only.
- Plan actions for that batch only.
- Redistribute actions to src/dst executors.
- Execute COPY/REMOVE/MKDIR/SYMLINK_UPDATE for batch `b`.
- Free all per-batch vectors before moving to `b+1`.
4) Final pass:
- Apply deferred operations that require global ordering safety (directory deletes, final directory metadata updates).

Checkpoint/resume (stability-first):
- Store batch progress file on destination side (e.g., `.nsync.batch.state`).
- Record: src/dst path, key options hash, batch_count, last completed batch.
- Update atomically (`tmp` + `rename`).
- On restart with matching config, skip completed batches; on mismatch, ignore checkpoint with warning.

Correctness/safety notes:
- Do not remove directories inside per-batch remove pass unless children are guaranteed done.
- Prefer deferring directory removes to final depth-descending sweep.
- Keep collective error propagation per batch (`Allreduce`) to avoid cross-rank hangs.

Performance notes:
- Expected memory per rank becomes approximately:
  `O(local_batch_meta + planner_batch_meta + batch_actions + I/O buffers)`.
- Total runtime may increase due to extra scan/spool I/O; this is an intentional memory-vs-time tradeoff.
- Add progress logs: `batch i/N`, cumulative copied items/bytes, per-batch elapsed time.

---

## 4) Implementation Phases (with done criteria)

### Phase 0: Scaffold
Changes:
- `src/nsync/nsync.c` skeleton with MPI init/finalize and usage.
- `src/nsync/CMakeLists.txt` with `MFU_ADD_TOOL(nsync)`.
- add `ADD_SUBDIRECTORY(nsync)` in `src/CMakeLists.txt`.

Done criteria:
- `cmake --build build -j` succeeds.
- `build/src/nsync/nsync --help` works.

### Phase 1: Role and CLI
Changes:
- implement options and role assignment (`auto` + `map`).
- print role counts and fail on invalid role topology.

Done criteria:
- role-map works on single host runs.
- invalid configs fail with clear errors.

### Phase 2: Metadata scan + exchange
Changes:
- implement custom recursive scan for SRC and DST groups.
- implement metadata packing/unpacking and planner redistribution.

Done criteria:
- dry-run prints counts: only-src, only-dst, common, changed.

### Phase 3: Planner + dryrun behavior
Changes:
- action planner and dryrun summary.

Done criteria:
- matches expected file counts on synthetic trees.

### Phase 4: COPY/REMOVE execution
Changes:
- implement transfer protocol and destination write path.
- implement delete path for `--delete`.

Done criteria:
- destination content matches source for tested cases.

### Phase 4.5: Memory-Bounded Batching (`--batch-files`)
Changes:
- add `--batch-files <N>` CLI and validation.
- implement count pass + deterministic batch_id mapping.
- implement local metadata spool files per batch and sequential batch loop.
- run compare/plan/execute per batch with state cleanup between batches.
- add checkpoint file for resume and mismatch detection.
- defer directory-remove/final-dir-metadata operations to safe final pass.

Done criteria:
- peak RSS stays bounded as total file count grows when `N` is fixed.
- kill/restart run resumes from next batch and converges to correct destination state.
- batch mode produces same final tree as non-batch mode on functional tests.

### Phase 5: --contents and metadata finalize
Changes:
- SHA256 compare path.
- metadata apply for mode/timestamps/owner where possible.

Done criteria:
- same-size different-content case only fixed when `--contents` is set.

### Phase 6: Docs and tests
Changes:
- `nsync.md`
- `man/nsync.1`
- update `man/CMakeLists.txt` and `doc/rst/tools.rst`
- add `test/tests/test_nsync/*`

Done criteria:
- docs are coherent.
- tests runnable and passing in local environment.

---

## 5) Suggested Test Matrix

Use role-map to emulate split topology on one host.

1. Basic copy
- src has new files/dirs
- dst empty
- expect dst == src

2. No-op sync
- dst already matches src
- expect zero copy operations

3. Contents compare
- same size + same mtime but different bytes
- without `--contents`: no copy
- with `--contents`: file copied

4. Delete option
- dst has extra files
- with `--delete`: extras removed

5. Symlink behavior
- changed symlink target in src
- expect dst link updated

6. Error aggregation
- inject unreadable source file
- expect non-zero rc and clear log

7. Batch memory bound
- create large synthetic tree
- run with `--batch-files` and verify stable peak memory (per rank)
- verify final dst tree matches baseline run without batching

8. Batch resume
- interrupt run mid-way in batch mode
- rerun with same options
- verify it continues from last completed batch and converges correctly

---

## 6) Risk Register and Mitigations

- Risk: deadlock in bidirectional message flow
  - Mitigation: single-direction source->destination stream, strict message ordering, bounded outstanding ops.

- Risk: parent directory races on destination
  - Mitigation: `mkdir -p` guard before file create/write, accept `EEXIST`.

- Risk: large-memory planner hot spots
  - Mitigation: hash partition planning across all world ranks; avoid rank-0 gather.

- Risk: hash skew causes oversized batches
  - Mitigation: monitor per-batch counts and support adaptive split (secondary hash range) for outlier batches.

- Risk: cross-batch directory ordering bugs (delete/update)
  - Mitigation: defer directory deletes and final directory metadata to dedicated final ordered pass.

- Risk: batch checkpoint drift after option/path changes
  - Mitigation: validate checkpoint header with option+path hash, reject stale checkpoints safely.

- Risk: ownership changes may fail without privilege
  - Mitigation: best-effort with warnings, consistent non-zero error accounting policy.

---

## 7) Definition of Done (v1)

- New `nsync` tool builds and runs.
- Works when no rank has both SRC and DEST mounts.
- Supports `--dryrun`, `--batch-files`, `--delete`, `--contents`, `--role-mode`, `--role-map`.
- Parallel metadata compare and copy over MPI.
- Documentation and basic tests are included.
