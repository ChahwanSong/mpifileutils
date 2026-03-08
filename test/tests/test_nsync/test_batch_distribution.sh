#!/bin/bash

set -euo pipefail

MFU_TEST_BIN=${MFU_TEST_BIN:-${1:-}}
NSYNC_SRC_BASE=${NSYNC_SRC_BASE:-${2:-/tmp}}
NSYNC_DEST_BASE=${NSYNC_DEST_BASE:-${3:-/tmp}}
NSYNC_TREE_NAME=${NSYNC_TREE_NAME:-${4:-nsync_batch_distribution}}

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

resolve_np()
{
    local np=0
    local opts=" ${MPIEXEC_OPTS} "
    if [[ "${opts}" =~ [[:space:]]-np[[:space:]]+([0-9]+) ]]; then
        np=${BASH_REMATCH[1]}
    elif [[ "${opts}" =~ [[:space:]]-n[[:space:]]+([0-9]+) ]]; then
        np=${BASH_REMATCH[1]}
    fi
    echo "${np}"
}

NSYNC_BIN=$(resolve_tool "${MFU_TEST_BIN}" nsync || true)
if [[ -z "${NSYNC_BIN}" ]]; then
    echo "Failed to resolve nsync binary from: ${MFU_TEST_BIN}"
    exit 2
fi

MPIEXEC=${MPIEXEC:-$(command -v mpirun || command -v mpiexec || true)}
MPIEXEC_OPTS=${MPIEXEC_OPTS:-"--allow-run-as-root -np 4"}

np=$(resolve_np)
if (( np < 4 )); then
    echo "SKIP: test_batch_distribution.sh requires at least 4 MPI ranks"
    exit 0
fi

ROLE_ARGS=()
if [[ "${NSYNC_USE_AUTO:-0}" != "1" ]]; then
    NSYNC_ROLE_MAP=${NSYNC_ROLE_MAP:-0:src,1:src,2:dst,3:dst}
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

SRC_ROOT=$(mktemp -d "${NSYNC_SRC_BASE%/}/${NSYNC_TREE_NAME}.src.XXXXXX")
DST_ROOT=$(mktemp -d "${NSYNC_DEST_BASE%/}/${NSYNC_TREE_NAME}.dst.XXXXXX")
trap 'rm -rf "${SRC_ROOT}" "${DST_ROOT}"' EXIT

echo "Using nsync binary: ${NSYNC_BIN}"
echo "Using source root: ${SRC_ROOT}"
echo "Using destination root: ${DST_ROOT}"
echo "Using MPI launcher: ${MPIEXEC} ${MPIEXEC_OPTS}"

mkdir -p "${SRC_ROOT}/case_batch_distribution"
for d in $(seq 0 15); do
    mkdir -p "${SRC_ROOT}/case_batch_distribution/d${d}"
    for f in $(seq 0 255); do
        printf "dist-d%02d-f%03d\n" "${d}" "${f}" > "${SRC_ROOT}/case_batch_distribution/d${d}/f${f}.txt"
    done
done

src_items=$(find "${SRC_ROOT}/case_batch_distribution" | wc -l)
batch_files=$(( (src_items + 15) / 16 ))
batch_count=$(( (src_items + batch_files - 1) / batch_files ))
if (( batch_count < 4 || batch_count % 4 != 0 )); then
    for candidate in $(seq "${batch_files}" $((batch_files + 128))); do
        batch_count=$(( (src_items + candidate - 1) / candidate ))
        if (( batch_count >= 4 && batch_count % 4 == 0 )); then
            batch_files=${candidate}
            break
        fi
    done
fi

batch_count=$(( (src_items + batch_files - 1) / batch_files ))
if (( batch_count < 4 || batch_count % 4 != 0 )); then
    echo "ASSERT FAILED: could not find batch_files yielding batch_count divisible by 4"
    exit 1
fi

echo "Using batch-files=${batch_files} batch-count=${batch_count}"
run_output=$(run_nsync --dryrun --batch-files "${batch_files}" \
    "${SRC_ROOT}/case_batch_distribution" "${DST_ROOT}/case_batch_distribution" 2>&1)
echo "${run_output}" | grep -q "Batch mode enabled"
if echo "${run_output}" | grep -q "Batch imbalance observed"; then
    echo "ASSERT FAILED: unexpected batch imbalance warning"
    echo "${run_output}"
    exit 1
fi
echo "${run_output}" | grep -q "changed="

echo "PASS: nsync batch distribution test completed"
