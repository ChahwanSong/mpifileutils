# nsync / dsync regression harness

Local tests for the nsync and dsync changes on this branch (92 checks pass). They run in a
development container with the same toolchain as the DMS job image's build stage. No CMakeLists.txt here:
the build does not pick this directory up.

## Setup

```sh
docker build -t mfu-dev test/nsync/dev-image/      # Debian bookworm, openmpi, lwgrp/dtcmp/libcircle; users tuser(4242), vuser(4343)
docker run -d --name mfu-dev --shm-size 1g \
  -v "$PWD":/work/src:ro -v "$PWD/test/nsync":/work/tests mfu-dev sleep infinity
docker exec mfu-dev mfu-build                      # /work/src -> /opt/mpifileutils/bin (incremental)
```

To compare with an older build, install it under `/opt/mfu-base/bin` and run with `BIN=/opt/mfu-base/bin`:
the new checks should fail there.

## Run

```sh
docker exec -e REPS=3 mfu-dev bash /work/tests/run.sh           # all cases (T01-T37); REPS repeats the racy ones
docker exec mfu-dev bash /work/tests/run.sh T23 T24             # by prefix
docker exec -e SIZES="20000 40000" mfu-dev bash /work/tests/scale.sh   # small-file copy time
docker exec mfu-dev bash /work/tests/perf2.sh                   # unchanged resync + type-change pre-pass cost
docker run --rm --tmpfs /mnt/small:rw,size=4m -v "$PWD/test/nsync":/work/tests:ro <image with the build> \
  bash /work/tests/enospc.sh                                     # full destination
```

T33 nearly fills /dev/shm (needs `--shm-size 1g`). T34 compiles `shim-enospc.c` (ENOSPC injected into
mkdirat) inside the container. Case comments in run.sh are in Korean.

Note: nsync.c defines `_GNU_SOURCE` itself (the build is `-std=gnu99` only); `--direct` is accepted and
ignored, `--open-noatime` reads source files O_NOATIME (retried without it on EPERM).

The binaries load libmfu.so from the install prefix, so a baseline under /opt/mfu-base/bin still runs the
new common library: compare dsync behaviour against a separately installed prefix.
