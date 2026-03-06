#!/bin/bash

set -euo pipefail

MFU_TEST_BIN=${MFU_TEST_BIN:-${1:-}}
NSYNC_SRC_BASE=${NSYNC_SRC_BASE:-${2:-/tmp}}
NSYNC_DEST_BASE=${NSYNC_DEST_BASE:-${3:-/tmp}}
NSYNC_TREE_NAME=${NSYNC_TREE_NAME:-${4:-nsync_resume}}

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
NSYNC_RESUME_KILL_DELAY=${NSYNC_RESUME_KILL_DELAY:-2}

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

SRC_ROOT=$(mktemp -d "${NSYNC_SRC_BASE%/}/${NSYNC_TREE_NAME}.src.XXXXXX")
DST_ROOT=$(mktemp -d "${NSYNC_DEST_BASE%/}/${NSYNC_TREE_NAME}.dst.XXXXXX")
LOG_FILE=$(mktemp /tmp/nsync_resume.log.XXXXXX)
trap 'rm -rf "${SRC_ROOT}" "${DST_ROOT}" "${LOG_FILE}"' EXIT

echo "Using nsync binary: ${NSYNC_BIN}"
echo "Using source root: ${SRC_ROOT}"
echo "Using destination root: ${DST_ROOT}"
echo "Interruption delay (seconds): ${NSYNC_RESUME_KILL_DELAY}"

mkdir -p "${SRC_ROOT}/case_resume"
for d in $(seq 0 29); do
    mkdir -p "${SRC_ROOT}/case_resume/d${d}"
    for f in $(seq 0 119); do
        printf "resume-d%02d-f%03d\n" "${d}" "${f}" > "${SRC_ROOT}/case_resume/d${d}/f${f}.txt"
    done
done

(
    run_nsync -v --batch-files 120 "${SRC_ROOT}/case_resume" "${DST_ROOT}/case_resume"
) >"${LOG_FILE}" 2>&1 &
sync_pid=$!

sleep "${NSYNC_RESUME_KILL_DELAY}"
if kill -0 "${sync_pid}" 2>/dev/null; then
    kill -TERM "${sync_pid}" 2>/dev/null || true
fi
wait "${sync_pid}" || true

if [[ -f "${DST_ROOT}/case_resume/.nsync.batch.state" ]]; then
    echo "Checkpoint detected after interruption: ${DST_ROOT}/case_resume/.nsync.batch.state"
else
    echo "Checkpoint not found after interruption (run may have completed too quickly)."
fi

run_nsync -v --batch-files 120 "${SRC_ROOT}/case_resume" "${DST_ROOT}/case_resume"

src_count=$(find "${SRC_ROOT}/case_resume" -type f | wc -l)
dst_count=$(find "${DST_ROOT}/case_resume" -type f | wc -l)
if [[ "${src_count}" != "${dst_count}" ]]; then
    echo "ASSERT FAILED: resume file count mismatch src=${src_count} dst=${dst_count}"
    exit 1
fi

sample_src="${SRC_ROOT}/case_resume/d7/f77.txt"
sample_dst="${DST_ROOT}/case_resume/d7/f77.txt"
if ! cmp -s "${sample_src}" "${sample_dst}"; then
    echo "ASSERT FAILED: sample file mismatch after resume"
    exit 1
fi

if [[ -f "${DST_ROOT}/case_resume/.nsync.batch.state" ]]; then
    echo "ASSERT FAILED: checkpoint file should be removed after successful completion"
    exit 1
fi

dryrun_out=$(run_nsync -v --dryrun --batch-files 120 "${SRC_ROOT}/case_resume" "${DST_ROOT}/case_resume" 2>&1)
echo "${dryrun_out}" | grep -q "changed=0"

echo "PASS: nsync resume test completed"
