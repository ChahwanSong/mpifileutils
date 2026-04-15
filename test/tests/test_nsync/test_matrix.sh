#!/bin/bash

set -euo pipefail

MFU_TEST_BIN=${MFU_TEST_BIN:-${1:-}}
NSYNC_SRC_BASE=${NSYNC_SRC_BASE:-${2:-/tmp}}
NSYNC_DEST_BASE=${NSYNC_DEST_BASE:-${3:-/tmp}}
NSYNC_TREE_NAME=${NSYNC_TREE_NAME:-${4:-nsync_matrix}}

if [[ -z "${MFU_TEST_BIN}" ]]; then
    echo "Usage: $0 <MFU_BIN_DIR_OR_NSYNC_PATH> [SRC_BASE] [DST_BASE] [TREE_PREFIX]"
    exit 2
fi

resolve_tool()
{
    local input_path=$1
    local tool_name=$2
    if [[ -f "${input_path}" && -x "${input_path}" ]]; then
        echo "${input_path}"
        return 0
    fi
    if [[ -x "${input_path}/${tool_name}" ]]; then
        echo "${input_path}/${tool_name}"
        return 0
    fi
    return 1
}

NSYNC_BIN=$(resolve_tool "${MFU_TEST_BIN}" nsync || true)
if [[ -z "${NSYNC_BIN}" ]]; then
    echo "Failed to resolve nsync binary from: ${MFU_TEST_BIN}"
    exit 2
fi

MPIEXEC=${MPIEXEC:-$(command -v mpirun || command -v mpiexec || true)}
MPIEXEC_OPTS=${MPIEXEC_OPTS:-"--allow-run-as-root -np 2"}

ROLE_ARGS=()
if [[ "${NSYNC_USE_AUTO:-0}" != "1" ]]; then
    NSYNC_ROLE_MAP=${NSYNC_ROLE_MAP:-0:src,1:dst}
    ROLE_ARGS=(--role-mode map --role-map "${NSYNC_ROLE_MAP}")
fi

run_nsync()
{
    if [[ -n "${MPIEXEC}" ]]; then
        # shellcheck disable=SC2086
        ${MPIEXEC} ${MPIEXEC_OPTS} "${NSYNC_BIN}" "${ROLE_ARGS[@]}" "$@"
    else
        "${NSYNC_BIN}" "${ROLE_ARGS[@]}" "$@"
    fi
}

assert_file_equals()
{
    local lhs=$1
    local rhs=$2
    if ! cmp -s "${lhs}" "${rhs}"; then
        echo "ASSERT FAILED: files differ: ${lhs} vs ${rhs}"
        exit 1
    fi
}

assert_exists()
{
    local path=$1
    if [[ ! -e "${path}" ]]; then
        echo "ASSERT FAILED: expected path to exist: ${path}"
        exit 1
    fi
}

assert_not_exists()
{
    local path=$1
    if [[ -e "${path}" ]]; then
        echo "ASSERT FAILED: expected path to not exist: ${path}"
        exit 1
    fi
}

assert_eq()
{
    local expected=$1
    local actual=$2
    local msg=$3
    if [[ "${expected}" != "${actual}" ]]; then
        echo "ASSERT FAILED: ${msg} expected=${expected} actual=${actual}"
        exit 1
    fi
}

hash_regular_files()
{
    local dir=$1
    (
        cd "${dir}" || exit 1
        find . -type f -print0 | sort -z | while IFS= read -r -d '' p; do
            sha256sum "${p}"
        done
    )
}

SRC_ROOT=$(mktemp -d "${NSYNC_SRC_BASE%/}/${NSYNC_TREE_NAME}.src.XXXXXX")
DST_ROOT=$(mktemp -d "${NSYNC_DEST_BASE%/}/${NSYNC_TREE_NAME}.dst.XXXXXX")
trap 'rm -rf "${SRC_ROOT}" "${DST_ROOT}"' EXIT

echo "Using nsync binary: ${NSYNC_BIN}"
echo "Using source root: ${SRC_ROOT}"
echo "Using destination root: ${DST_ROOT}"
if [[ -n "${MPIEXEC}" ]]; then
    echo "Using MPI launcher: ${MPIEXEC} ${MPIEXEC_OPTS}"
else
    echo "No MPI launcher detected, running singleton"
fi

##############################################################################
# Case 1: basic copy/delete/symlink/metadata
##############################################################################
mkdir -p "${SRC_ROOT}/case1/dirA/sub" "${DST_ROOT}/case1/dirA/sub"
printf "alpha\n" > "${SRC_ROOT}/case1/file.txt"
printf "nested\n" > "${SRC_ROOT}/case1/dirA/sub/nested.txt"
printf "meta\n" > "${SRC_ROOT}/case1/meta.txt"
ln -sfn "file.txt" "${SRC_ROOT}/case1/link_to_file"
chmod 0640 "${SRC_ROOT}/case1/meta.txt"
touch -d "2024-04-05 06:07:08" "${SRC_ROOT}/case1/meta.txt"

printf "stale\n" > "${DST_ROOT}/case1/file.txt"
printf "extra\n" > "${DST_ROOT}/case1/extra.txt"
ln -sfn "wrong_target" "${DST_ROOT}/case1/link_to_file"
chmod 0600 "${DST_ROOT}/case1/meta.txt" 2>/dev/null || true
touch -d "2022-01-01 00:00:00" "${DST_ROOT}/case1/meta.txt" 2>/dev/null || true

run_nsync "${SRC_ROOT}/case1" "${DST_ROOT}/case1"

assert_file_equals "${SRC_ROOT}/case1/file.txt" "${DST_ROOT}/case1/file.txt"
assert_file_equals "${SRC_ROOT}/case1/dirA/sub/nested.txt" "${DST_ROOT}/case1/dirA/sub/nested.txt"
assert_exists "${DST_ROOT}/case1/extra.txt"
assert_eq "file.txt" "$(readlink "${DST_ROOT}/case1/link_to_file")" "symlink target should match source"
assert_eq "$(stat -c "%a" "${SRC_ROOT}/case1/meta.txt")" "$(stat -c "%a" "${DST_ROOT}/case1/meta.txt")" "metadata mode should match"

run_nsync --delete "${SRC_ROOT}/case1" "${DST_ROOT}/case1"
assert_not_exists "${DST_ROOT}/case1/extra.txt"

dryrun_out=$(run_nsync --dryrun --delete "${SRC_ROOT}/case1" "${DST_ROOT}/case1" 2>&1)
echo "${dryrun_out}" | grep -q "changed=0"

##############################################################################
# Case 2: --contents behavior
##############################################################################
mkdir -p "${SRC_ROOT}/case2" "${DST_ROOT}/case2"
printf "AAAA" > "${SRC_ROOT}/case2/same_size.txt"
printf "BBBB" > "${DST_ROOT}/case2/same_size.txt"
touch -d "2024-12-12 12:12:12" "${SRC_ROOT}/case2/same_size.txt" "${DST_ROOT}/case2/same_size.txt"

run_nsync "${SRC_ROOT}/case2" "${DST_ROOT}/case2"
assert_eq "BBBB" "$(cat "${DST_ROOT}/case2/same_size.txt")" "without --contents destination data should remain"

run_nsync --contents "${SRC_ROOT}/case2" "${DST_ROOT}/case2"
assert_eq "AAAA" "$(cat "${DST_ROOT}/case2/same_size.txt")" "with --contents destination data should match source"

dryrun_out=$(run_nsync --dryrun --contents "${SRC_ROOT}/case2" "${DST_ROOT}/case2" 2>&1)
echo "${dryrun_out}" | grep -q "changed=0"

##############################################################################
# Case 3: sparse file preservation
##############################################################################
mkdir -p "${SRC_ROOT}/case3" "${DST_ROOT}/case3"
truncate -s $((64 * 1024 * 1024)) "${SRC_ROOT}/case3/sparse.bin"
printf "HEAD" | dd of="${SRC_ROOT}/case3/sparse.bin" bs=1 conv=notrunc status=none
printf "MID" | dd of="${SRC_ROOT}/case3/sparse.bin" bs=1 seek=$((32 * 1024 * 1024)) conv=notrunc status=none
printf "TAIL" | dd of="${SRC_ROOT}/case3/sparse.bin" bs=1 seek=$((64 * 1024 * 1024 - 4)) conv=notrunc status=none

run_nsync "${SRC_ROOT}/case3" "${DST_ROOT}/case3"

assert_file_equals "${SRC_ROOT}/case3/sparse.bin" "${DST_ROOT}/case3/sparse.bin"
src_sparse_size=$(stat -c "%s" "${SRC_ROOT}/case3/sparse.bin")
dst_sparse_size=$(stat -c "%s" "${DST_ROOT}/case3/sparse.bin")
assert_eq "${src_sparse_size}" "${dst_sparse_size}" "sparse file size should match"

src_sparse_blocks=$(stat -c "%b" "${SRC_ROOT}/case3/sparse.bin")
dst_sparse_blocks=$(stat -c "%b" "${DST_ROOT}/case3/sparse.bin")
logical_blocks=$(( (dst_sparse_size + 511) / 512 ))
if [[ "${dst_sparse_blocks}" -ge "${logical_blocks}" ]]; then
    echo "ASSERT FAILED: destination sparse file was materialized: blocks=${dst_sparse_blocks} logical=${logical_blocks}"
    exit 1
fi
if [[ "${dst_sparse_blocks}" -gt $((src_sparse_blocks + 16)) ]]; then
    echo "ASSERT FAILED: destination sparse file block usage regressed: src=${src_sparse_blocks} dst=${dst_sparse_blocks}"
    exit 1
fi

dryrun_out=$(run_nsync --dryrun "${SRC_ROOT}/case3" "${DST_ROOT}/case3" 2>&1)
echo "${dryrun_out}" | grep -q "changed=0"

##############################################################################
# Case 4: --batch-files convergence and no-op
##############################################################################
mkdir -p "${SRC_ROOT}/case4" "${DST_ROOT}/case4"
for d in $(seq 0 19); do
    mkdir -p "${SRC_ROOT}/case4/d${d}"
    for f in $(seq 0 99); do
        printf "d%02d-f%03d\n" "${d}" "${f}" > "${SRC_ROOT}/case4/d${d}/f${f}.txt"
    done
done
printf "big\n" > "${SRC_ROOT}/case4/bigfile.txt"
ln -sfn "d0/f0.txt" "${SRC_ROOT}/case4/symlink_ref"

run_nsync --batch-files 200 "${SRC_ROOT}/case4" "${DST_ROOT}/case4"

src_file_count=$(find "${SRC_ROOT}/case4" -type f | wc -l)
dst_file_count=$(find "${DST_ROOT}/case4" -type f | wc -l)
assert_eq "${src_file_count}" "${dst_file_count}" "batch copy file count"
assert_eq "$(readlink "${SRC_ROOT}/case4/symlink_ref")" "$(readlink "${DST_ROOT}/case4/symlink_ref")" "batch symlink target"

src_hashes=$(mktemp /tmp/nsync.src.hash.XXXXXX)
dst_hashes=$(mktemp /tmp/nsync.dst.hash.XXXXXX)
hash_regular_files "${SRC_ROOT}/case4" | sort > "${src_hashes}"
hash_regular_files "${DST_ROOT}/case4" | sort > "${dst_hashes}"
if ! diff -u "${src_hashes}" "${dst_hashes}" >/dev/null; then
    echo "ASSERT FAILED: batch regular-file hashes differ"
    rm -f "${src_hashes}" "${dst_hashes}"
    exit 1
fi
rm -f "${src_hashes}" "${dst_hashes}"

assert_not_exists "${DST_ROOT}/case4/.nsync.batch.state"
dryrun_out=$(run_nsync --dryrun --batch-files 200 "${SRC_ROOT}/case4" "${DST_ROOT}/case4" 2>&1)
echo "${dryrun_out}" | grep -q "changed=0"

##############################################################################
# Case 5: --ignore-symlinks excludes link paths from sync and delete
##############################################################################
mkdir -p "${SRC_ROOT}/case5" "${DST_ROOT}/case5"
printf "data\n" > "${SRC_ROOT}/case5/data.txt"
ln -sfn "data.txt" "${SRC_ROOT}/case5/link_src"
ln -sfn "dst_target" "${DST_ROOT}/case5/link_dst_only"

run_nsync --ignore-symlinks --delete "${SRC_ROOT}/case5" "${DST_ROOT}/case5"

assert_file_equals "${SRC_ROOT}/case5/data.txt" "${DST_ROOT}/case5/data.txt"
assert_not_exists "${DST_ROOT}/case5/link_src"
assert_exists "${DST_ROOT}/case5/link_dst_only"
assert_eq "dst_target" "$(readlink "${DST_ROOT}/case5/link_dst_only")" "dst-only symlink should remain untouched"

dryrun_out=$(run_nsync --dryrun --ignore-symlinks --delete "${SRC_ROOT}/case5" "${DST_ROOT}/case5" 2>&1)
echo "${dryrun_out}" | grep -q "changed=0"

echo "PASS: nsync matrix tests completed"
