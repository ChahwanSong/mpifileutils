nsync
=====

SYNOPSIS
--------

**nsync [OPTION] SRC DEST**

DESCRIPTION
-----------

Parallel MPI application to synchronize SRC to DEST in split-mount topologies.

`nsync` is intended for environments where source and destination paths are not
mounted on the same ranks. Each rank is assigned a role (`src` or `dst`), and
source-side file data is streamed to destination-side ranks over MPI.

OPTIONS
-------

.. option:: --dryrun

   Show differences and planned actions without modifying destination.

.. option:: -b, --batch-files N

   Process entries in deterministic batches of approximately N items to bound
   planner memory usage.

.. option:: -D, --delete

   Delete destination entries that are missing from source.

.. option:: -c, --contents

   Compare regular file contents with SHA256 digest. Without this option,
   regular files are compared by size and mtime.

.. option:: --bufsize SIZE

   Set I/O and digest buffer size in bytes.

.. option:: --chunksize SIZE

   Set minimum work chunk size in bytes.

.. option:: --imbalance-threshold R

   Batch imbalance ratio threshold for diagnostic warnings.
   Defaults to ``3.0``.

.. option:: --role-mode MODE

   Set role assignment mode. Values: ``auto`` or ``map``.

.. option:: --role-map SPEC

   Explicit role mapping string used with ``--role-mode map``.
   Example: ``0-1:src,2-3:dst``.

.. option:: --trace

   Enable detailed per-rank stage tracing for debugging.

.. option:: -q, --quiet

   Quiet output.
   Progress messages are otherwise printed on the launcher-console log rank
   after each batch completes.

.. option:: -h, --help

   Print usage.

ROLE ASSIGNMENT
---------------

- ``auto`` mode:
  ranks are classified by path accessibility checks.

- ``map`` mode:
  ranks are assigned by explicit world-rank map.

`nsync` fails when one side is empty or role assignment is ambiguous.

COMPARE POLICY
--------------

- Regular files:

  - default: compare ``size + mtime_nsec``
  - with ``--contents``: compare SHA256 digests

- Symlinks: compare link target strings.

- Directories: compare metadata only.

EXAMPLES
--------

1. Split-mount synchronization:

``mpirun -np 2 nsync /src /dst``

2. Explicit roles for local testing:

``mpirun -np 2 nsync --role-mode map --role-map 0:src,1:dst /tmp/src /tmp/dst``

3. Memory-bounded sync with delete and digest compare:

``mpirun -np 8 nsync --batch-files 200000 --delete --contents /src /dst``

SEE ALSO
--------

The mpiFileUtils source code and all documentation may be downloaded
from <https://github.com/hpc/mpifileutils>
