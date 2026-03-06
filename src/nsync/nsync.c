/*
 * Copyright (c) 2026, nsync contributors.
 *
 * Phase 5 scaffold for nsync:
 * - role assignment (auto/map) and communicator split
 * - metadata scan on source/destination roles
 * - metadata serialization and planner redistribution
 * - planner action generation and dry-run summary
 * - distributed COPY/REMOVE execution between split source/destination roles
 * - memory-bounded batch mode via --batch-files
 * - digest-based contents compare (--contents) via SHA256
 * - metadata finalize (mode/uid/gid/mtime) on destination
 */

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <openssl/evp.h>
#include <openssl/sha.h>

#include "mpi.h"
#include "mfu.h"

typedef enum {
    NSYNC_ROLE_UNSET = 0,
    NSYNC_ROLE_SRC,
    NSYNC_ROLE_DST,
} nsync_role_t;

typedef enum {
    NSYNC_ROLE_MODE_AUTO = 0,
    NSYNC_ROLE_MODE_MAP,
} nsync_role_mode_t;

typedef struct {
    int dryrun;
    uint64_t batch_files;
    int delete;
    int contents;
    uint64_t bufsize;
    uint64_t chunksize;
    int progress_secs;
    nsync_role_mode_t role_mode;
    const char* role_map;
    int trace;
    int verbose;
    int quiet;
} nsync_options_t;

typedef struct {
    nsync_role_t role;
    int src_count;
    int dst_count;
    int src_index;
    int dst_index;
    int* src_world_ranks;
    int* dst_world_ranks;
    MPI_Comm src_comm;
    MPI_Comm dst_comm;
} nsync_role_info_t;

typedef struct {
    char* relpath;
    mfu_filetype type;
    uint64_t mode;
    uint64_t uid;
    uint64_t gid;
    uint64_t size;
    uint64_t mtime;
    uint64_t mtime_nsec;
    char* link_target;
    uint32_t digest_valid;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    nsync_role_t side;
} nsync_meta_record_t;

typedef struct {
    nsync_meta_record_t* records;
    uint64_t size;
    uint64_t capacity;
} nsync_meta_vec_t;

typedef struct {
    uint64_t only_src;
    uint64_t only_dst;
    uint64_t common;
    uint64_t changed;
} nsync_compare_counts_t;

typedef enum {
    NSYNC_ACTION_COPY = 0,
    NSYNC_ACTION_REMOVE,
    NSYNC_ACTION_MKDIR,
    NSYNC_ACTION_SYMLINK_UPDATE,
    NSYNC_ACTION_META_UPDATE,
} nsync_action_type_t;

typedef struct {
    nsync_action_type_t type;
    char* relpath;
    char* link_target;
    int src_owner_world;
    int dst_owner_world;
    uint64_t size;
    uint64_t mode;
    uint64_t uid;
    uint64_t gid;
    uint64_t mtime;
    uint64_t mtime_nsec;
} nsync_action_record_t;

typedef struct {
    nsync_action_record_t* records;
    uint64_t size;
    uint64_t capacity;
} nsync_action_vec_t;

typedef struct {
    uint64_t copy;
    uint64_t remove;
    uint64_t mkdir;
    uint64_t symlink_update;
    uint64_t meta_update;
    uint64_t skipped_only_dst;
} nsync_action_counts_t;

typedef struct {
    int enabled;
    uint64_t batch_count;
    uint64_t batch_id;
} nsync_scan_filter_t;

typedef struct {
    char* src_path;
    char* dst_path;
    uint64_t option_hash;
    uint64_t batch_count;
    uint64_t last_completed;
    int finalized;
} nsync_batch_state_t;

typedef struct {
    char* dir;
    uint64_t batch_count;
    uint64_t start_batch;
    int include_digest;
    int max_open_fds;
    int open_fds;
    int* batch_fds;
    unsigned char* batch_has_data;
    int io_error;
    uint64_t records_written;
    uint64_t bytes_written;
} nsync_batch_spool_t;

enum {
    NSYNC_BATCH_STATE_MATCH = 0,
    NSYNC_BATCH_STATE_MISMATCH_MALFORMED,
    NSYNC_BATCH_STATE_MISMATCH_SRC,
    NSYNC_BATCH_STATE_MISMATCH_DST,
    NSYNC_BATCH_STATE_MISMATCH_OPTIONS,
    NSYNC_BATCH_STATE_MISMATCH_BATCH_COUNT
};

#define NSYNC_BATCH_STATE_MAGIC "nsync-batch-state-v1"
#define NSYNC_BATCH_STATE_FILE ".nsync.batch.state"

static const unsigned char NSYNC_SHA256_EMPTY[SHA256_DIGEST_LENGTH] = {
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
    0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
    0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
    0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55
};

static char* nsync_build_full_path(const char* root, const char* relpath);
static char* nsync_trim_space(char* text);
static int nsync_plan_directory_finalize_records(
    nsync_meta_vec_t* planner,
    const nsync_role_info_t* role_info,
    nsync_action_vec_t* planned_actions);

static void nsync_usage(void)
{
    printf("\n");
    printf("Usage: nsync [options] source target\n");
    printf("\n");
    printf("Options:\n");
    printf("      --dryrun               - show differences, do not modify destination\n");
    printf("  -b, --batch-files <N>      - process entries in batches of approximately N items\n");
    printf("                               (auto checkpoint/resume via " NSYNC_BATCH_STATE_FILE ")\n");
    printf("  -D, --delete               - delete extraneous files from target\n");
    printf("  -c, --contents             - compare file contents instead of size+mtime\n");
    printf("      --bufsize <SIZE>       - I/O buffer size in bytes (default " MFU_BUFFER_SIZE_STR ")\n");
    printf("      --chunksize <SIZE>     - minimum work size per task in bytes (default " MFU_CHUNK_SIZE_STR ")\n");
    printf("      --progress <N>         - print progress every N seconds\n");
    printf("      --role-mode <MODE>     - role assignment mode: auto or map\n");
    printf("      --role-map <SPEC>      - explicit role map (used with --role-mode map)\n");
    printf("      --trace                - print detailed per-rank stage traces (debug)\n");
    printf("  -v, --verbose              - verbose output\n");
    printf("  -q, --quiet                - quiet output\n");
    printf("  -h, --help                 - print usage\n");
    printf("\n");
    fflush(stdout);
}

static void nsync_role_info_init(nsync_role_info_t* info)
{
    info->role = NSYNC_ROLE_UNSET;
    info->src_count = 0;
    info->dst_count = 0;
    info->src_index = -1;
    info->dst_index = -1;
    info->src_world_ranks = NULL;
    info->dst_world_ranks = NULL;
    info->src_comm = MPI_COMM_NULL;
    info->dst_comm = MPI_COMM_NULL;
}

static void nsync_role_info_free(nsync_role_info_t* info)
{
    if (info->src_comm != MPI_COMM_NULL) {
        MPI_Comm_free(&info->src_comm);
        info->src_comm = MPI_COMM_NULL;
    }

    if (info->dst_comm != MPI_COMM_NULL) {
        MPI_Comm_free(&info->dst_comm);
        info->dst_comm = MPI_COMM_NULL;
    }

    mfu_free(&info->src_world_ranks);
    mfu_free(&info->dst_world_ranks);
}

static int nsync_parse_role_mode(const char* arg, nsync_role_mode_t* mode)
{
    if (strcmp(arg, "auto") == 0) {
        *mode = NSYNC_ROLE_MODE_AUTO;
        return 0;
    }

    if (strcmp(arg, "map") == 0) {
        *mode = NSYNC_ROLE_MODE_MAP;
        return 0;
    }

    return -1;
}

static const char* nsync_role_to_string(nsync_role_t role)
{
    switch (role) {
    case NSYNC_ROLE_SRC:
        return "src";
    case NSYNC_ROLE_DST:
        return "dst";
    default:
        return "unset";
    }
}

static void nsync_trace_local(
    const nsync_options_t* opts,
    const char* stage,
    uint64_t v1,
    uint64_t v2)
{
    if (opts == NULL || !opts->trace) {
        return;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    fprintf(stderr,
        "nsync-trace rank=%d stage=%s v1=%" PRIu64 " v2=%" PRIu64 "\n",
        rank, stage, v1, v2);
    fflush(stderr);
}

static int nsync_sync_error_point(
    const nsync_options_t* opts,
    const char* stage,
    int local_error)
{
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (opts != NULL && opts->trace) {
        fprintf(stderr,
            "nsync-trace rank=%d enter-sync stage=%s local_error=%d\n",
            rank, stage, local_error);
        fflush(stderr);
    }

    int global_error = 0;
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    if (opts != NULL && opts->trace) {
        fprintf(stderr,
            "nsync-trace rank=%d leave-sync stage=%s global_error=%d\n",
            rank, stage, global_error);
        fflush(stderr);
    }

    return global_error;
}

static void nsync_batch_state_init(nsync_batch_state_t* state)
{
    state->src_path = NULL;
    state->dst_path = NULL;
    state->option_hash = 0;
    state->batch_count = 0;
    state->last_completed = 0;
    state->finalized = 0;
}

static void nsync_batch_state_free(nsync_batch_state_t* state)
{
    mfu_free(&state->src_path);
    mfu_free(&state->dst_path);
}

static uint64_t nsync_hash64_update_bytes(uint64_t hash, const void* buf, size_t len)
{
    const unsigned char* ptr = (const unsigned char*)buf;
    for (size_t i = 0; i < len; i++) {
        hash ^= (uint64_t)ptr[i];
        hash *= 1099511628211ULL;
    }
    return hash;
}

static uint64_t nsync_hash64_update_u64(uint64_t hash, uint64_t val)
{
    unsigned char bytes[8];
    for (int i = 0; i < 8; i++) {
        bytes[i] = (unsigned char)((val >> (8 * i)) & 0xffULL);
    }
    return nsync_hash64_update_bytes(hash, bytes, sizeof(bytes));
}

static uint64_t nsync_hash64_update_string(uint64_t hash, const char* text)
{
    uint64_t len = 0;
    if (text != NULL) {
        len = (uint64_t)strlen(text);
    }
    hash = nsync_hash64_update_u64(hash, len);
    if (len > 0) {
        hash = nsync_hash64_update_bytes(hash, text, (size_t)len);
    }
    return hash;
}

static int nsync_parse_uint64_str(const char* text, uint64_t* out)
{
    if (text == NULL || out == NULL) {
        return -1;
    }

    errno = 0;
    char* end = NULL;
    unsigned long long val = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }

    *out = (uint64_t)val;
    return 0;
}

static uint64_t nsync_batch_option_hash(
    const nsync_options_t* opts,
    const char* src_path,
    const char* dst_path)
{
    uint64_t hash = 1469598103934665603ULL;
    hash = nsync_hash64_update_string(hash, NSYNC_BATCH_STATE_MAGIC);
    hash = nsync_hash64_update_string(hash, src_path);
    hash = nsync_hash64_update_string(hash, dst_path);
    hash = nsync_hash64_update_u64(hash, (uint64_t)opts->batch_files);
    hash = nsync_hash64_update_u64(hash, (uint64_t)opts->delete);
    hash = nsync_hash64_update_u64(hash, (uint64_t)opts->contents);
    hash = nsync_hash64_update_u64(hash, (uint64_t)opts->bufsize);
    hash = nsync_hash64_update_u64(hash, (uint64_t)opts->chunksize);
    hash = nsync_hash64_update_u64(hash, (uint64_t)opts->role_mode);
    hash = nsync_hash64_update_string(hash, opts->role_map != NULL ? opts->role_map : "");
    return hash;
}

static int nsync_batch_state_read(
    const char* state_file,
    nsync_batch_state_t* state,
    int* found,
    int* malformed)
{
    *found = 0;
    *malformed = 0;

    FILE* fp = fopen(state_file, "r");
    if (fp == NULL) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }

    *found = 1;

    char line[8192];
    if (fgets(line, sizeof(line), fp) == NULL) {
        if (ferror(fp)) {
            fclose(fp);
            return -1;
        }
        *malformed = 1;
        fclose(fp);
        return 0;
    }

    char* magic = nsync_trim_space(line);
    if (strcmp(magic, NSYNC_BATCH_STATE_MAGIC) != 0) {
        *malformed = 1;
    }

    int have_src = 0;
    int have_dst = 0;
    int have_hash = 0;
    int have_batch_count = 0;
    int have_last_completed = 0;
    int have_finalized = 0;

    while (fgets(line, sizeof(line), fp) != NULL) {
        char* trimmed = nsync_trim_space(line);
        if (*trimmed == '\0') {
            continue;
        }

        char* eq = strchr(trimmed, '=');
        if (eq == NULL) {
            *malformed = 1;
            continue;
        }

        *eq = '\0';
        char* key = nsync_trim_space(trimmed);
        char* val = nsync_trim_space(eq + 1);

        if (strcmp(key, "src") == 0) {
            mfu_free(&state->src_path);
            state->src_path = MFU_STRDUP(val);
            have_src = 1;
            continue;
        }
        if (strcmp(key, "dst") == 0) {
            mfu_free(&state->dst_path);
            state->dst_path = MFU_STRDUP(val);
            have_dst = 1;
            continue;
        }
        if (strcmp(key, "option_hash") == 0) {
            if (nsync_parse_uint64_str(val, &state->option_hash) != 0) {
                *malformed = 1;
            } else {
                have_hash = 1;
            }
            continue;
        }
        if (strcmp(key, "batch_count") == 0) {
            if (nsync_parse_uint64_str(val, &state->batch_count) != 0) {
                *malformed = 1;
            } else {
                have_batch_count = 1;
            }
            continue;
        }
        if (strcmp(key, "last_completed") == 0) {
            if (nsync_parse_uint64_str(val, &state->last_completed) != 0) {
                *malformed = 1;
            } else {
                have_last_completed = 1;
            }
            continue;
        }
        if (strcmp(key, "finalized") == 0) {
            uint64_t parsed = 0;
            if (nsync_parse_uint64_str(val, &parsed) != 0 || parsed > 1) {
                *malformed = 1;
            } else {
                state->finalized = (int)parsed;
                have_finalized = 1;
            }
            continue;
        }
    }

    if (ferror(fp)) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    if (!have_src || !have_dst || !have_hash || !have_batch_count || !have_last_completed || !have_finalized) {
        *malformed = 1;
    }

    return 0;
}

static int nsync_batch_state_write(
    const char* state_file,
    const nsync_batch_state_t* state)
{
    size_t path_len = strlen(state_file);
    size_t tmp_len = path_len + 64;
    char* tmp_file = (char*)MFU_MALLOC(tmp_len);
    snprintf(tmp_file, tmp_len, "%s.tmp.%d", state_file, (int)getpid());

    FILE* fp = fopen(tmp_file, "w");
    if (fp == NULL) {
        mfu_free(&tmp_file);
        return -1;
    }

    int rc = 0;
    if (fprintf(fp, "%s\n", NSYNC_BATCH_STATE_MAGIC) < 0 ||
        fprintf(fp, "src=%s\n", state->src_path) < 0 ||
        fprintf(fp, "dst=%s\n", state->dst_path) < 0 ||
        fprintf(fp, "option_hash=%" PRIu64 "\n", state->option_hash) < 0 ||
        fprintf(fp, "batch_count=%" PRIu64 "\n", state->batch_count) < 0 ||
        fprintf(fp, "last_completed=%" PRIu64 "\n", state->last_completed) < 0 ||
        fprintf(fp, "finalized=%d\n", state->finalized) < 0)
    {
        rc = -1;
    }

    if (rc == 0 && fflush(fp) != 0) {
        rc = -1;
    }
    if (rc == 0) {
        int fd = fileno(fp);
        if (fd >= 0 && fsync(fd) != 0) {
            rc = -1;
        }
    }

    if (fclose(fp) != 0) {
        rc = -1;
    }

    if (rc == 0 && rename(tmp_file, state_file) != 0) {
        rc = -1;
    }

    if (rc == 0) {
        char* path_copy = MFU_STRDUP(state_file);
        char* slash = strrchr(path_copy, '/');
        const char* dirpath = ".";
        int dirfd = -1;

        if (slash != NULL) {
            if (slash == path_copy) {
                slash[1] = '\0';
                dirpath = path_copy;
            } else {
                *slash = '\0';
                dirpath = path_copy;
            }
        }

        dirfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dirfd < 0 || fsync(dirfd) != 0) {
            rc = -1;
        }
        if (dirfd >= 0) {
            close(dirfd);
        }
        mfu_free(&path_copy);
    }

    if (rc != 0) {
        unlink(tmp_file);
    }

    mfu_free(&tmp_file);
    return rc;
}

static int nsync_batch_state_match_reason(
    const nsync_batch_state_t* state,
    const char* src_path,
    const char* dst_path,
    uint64_t option_hash,
    uint64_t batch_count)
{
    if (state->src_path == NULL || state->dst_path == NULL || state->batch_count == 0) {
        return NSYNC_BATCH_STATE_MISMATCH_MALFORMED;
    }
    if (strcmp(state->src_path, src_path) != 0) {
        return NSYNC_BATCH_STATE_MISMATCH_SRC;
    }
    if (strcmp(state->dst_path, dst_path) != 0) {
        return NSYNC_BATCH_STATE_MISMATCH_DST;
    }
    if (state->option_hash != option_hash) {
        return NSYNC_BATCH_STATE_MISMATCH_OPTIONS;
    }
    if (state->batch_count != batch_count) {
        return NSYNC_BATCH_STATE_MISMATCH_BATCH_COUNT;
    }
    return NSYNC_BATCH_STATE_MATCH;
}

static const char* nsync_batch_mismatch_reason_string(int reason)
{
    switch (reason) {
    case NSYNC_BATCH_STATE_MISMATCH_MALFORMED:
        return "malformed checkpoint";
    case NSYNC_BATCH_STATE_MISMATCH_SRC:
        return "source path changed";
    case NSYNC_BATCH_STATE_MISMATCH_DST:
        return "destination path changed";
    case NSYNC_BATCH_STATE_MISMATCH_OPTIONS:
        return "options changed";
    case NSYNC_BATCH_STATE_MISMATCH_BATCH_COUNT:
        return "batch-count changed";
    default:
        return "unknown";
    }
}

static char* nsync_trim_space(char* text)
{
    while (*text != '\0' && isspace((unsigned char)*text)) {
        text++;
    }

    char* end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1])) {
        end--;
    }
    *end = '\0';

    return text;
}

static int nsync_parse_nonnegative_int(const char* text, int* value)
{
    errno = 0;
    char* end = NULL;
    long tmp = strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || tmp < 0 || tmp > INT_MAX) {
        return -1;
    }

    *value = (int)tmp;
    return 0;
}

static int nsync_parse_rank_range(const char* text, int* start, int* end)
{
    char* copy = MFU_STRDUP(text);
    char* dash = strchr(copy, '-');

    if (dash == NULL) {
        int idx;
        if (nsync_parse_nonnegative_int(copy, &idx) != 0) {
            mfu_free(&copy);
            return -1;
        }

        *start = idx;
        *end = idx;
        mfu_free(&copy);
        return 0;
    }

    *dash = '\0';
    char* left = nsync_trim_space(copy);
    char* right = nsync_trim_space(dash + 1);

    int lo;
    int hi;
    if (nsync_parse_nonnegative_int(left, &lo) != 0 ||
        nsync_parse_nonnegative_int(right, &hi) != 0 ||
        lo > hi)
    {
        mfu_free(&copy);
        return -1;
    }

    *start = lo;
    *end = hi;
    mfu_free(&copy);
    return 0;
}

static int nsync_parse_role_label(const char* text, nsync_role_t* role)
{
    if (strcasecmp(text, "src") == 0 || strcasecmp(text, "source") == 0) {
        *role = NSYNC_ROLE_SRC;
        return 0;
    }

    if (strcasecmp(text, "dst") == 0 ||
        strcasecmp(text, "dest") == 0 ||
        strcasecmp(text, "destination") == 0)
    {
        *role = NSYNC_ROLE_DST;
        return 0;
    }

    return -1;
}

static int nsync_parse_role_map(const char* spec, int ranks, nsync_role_t* roles)
{
    for (int i = 0; i < ranks; i++) {
        roles[i] = NSYNC_ROLE_UNSET;
    }

    char* copy = MFU_STRDUP(spec);
    char* save = NULL;
    char* token = strtok_r(copy, ",", &save);

    while (token != NULL) {
        char* item = nsync_trim_space(token);
        if (*item == '\0') {
            mfu_free(&copy);
            return -1;
        }

        char* colon = strchr(item, ':');
        if (colon == NULL) {
            mfu_free(&copy);
            return -1;
        }

        *colon = '\0';
        char* range_text = nsync_trim_space(item);
        char* role_text = nsync_trim_space(colon + 1);

        int start;
        int end;
        nsync_role_t role;
        if (nsync_parse_rank_range(range_text, &start, &end) != 0 ||
            nsync_parse_role_label(role_text, &role) != 0 ||
            start < 0 || end >= ranks)
        {
            mfu_free(&copy);
            return -1;
        }

        for (int r = start; r <= end; r++) {
            if (roles[r] != NSYNC_ROLE_UNSET && roles[r] != role) {
                mfu_free(&copy);
                return -1;
            }
            roles[r] = role;
        }

        token = strtok_r(NULL, ",", &save);
    }

    mfu_free(&copy);

    for (int i = 0; i < ranks; i++) {
        if (roles[i] == NSYNC_ROLE_UNSET) {
            return -1;
        }
    }

    return 0;
}

static int nsync_can_read_source(const char* src_path)
{
    if (src_path == NULL || *src_path == '\0') {
        return 0;
    }

    return (access(src_path, R_OK) == 0) ? 1 : 0;
}

static int nsync_can_write_destination(const char* dst_path)
{
    if (dst_path == NULL || *dst_path == '\0') {
        return 0;
    }

    /* For auto role detection in split-mount topology, require destination
     * path itself to exist and be writable on destination ranks. */
    return (access(dst_path, W_OK) == 0) ? 1 : 0;
}

static int nsync_validate_and_count_roles(
    const nsync_options_t* opts,
    const nsync_role_t* all_roles,
    const int* auto_caps,
    int ranks,
    int* src_count,
    int* dst_count)
{
    int local_src = 0;
    int local_dst = 0;
    int invalid = 0;

    for (int i = 0; i < ranks; i++) {
        if (all_roles[i] == NSYNC_ROLE_SRC) {
            local_src++;
        } else if (all_roles[i] == NSYNC_ROLE_DST) {
            local_dst++;
        } else {
            invalid++;
        }
    }

    *src_count = local_src;
    *dst_count = local_dst;

    if (invalid > 0) {
        MFU_LOG(MFU_LOG_ERR, "Role assignment produced %d unassigned rank(s)", invalid);
        if (opts->role_mode == NSYNC_ROLE_MODE_AUTO && auto_caps != NULL) {
            for (int i = 0; i < ranks; i++) {
                if (all_roles[i] == NSYNC_ROLE_UNSET) {
                    int can_src = auto_caps[2 * i];
                    int can_dst = auto_caps[2 * i + 1];
                    MFU_LOG(MFU_LOG_ERR,
                        "Rank %d invalid in auto mode (can_src=%d can_dst=%d). "
                        "Expected exactly one of source/destination accessibility.",
                        i, can_src, can_dst);
                }
            }
        }
        return -1;
    }

    if (local_src == 0 || local_dst == 0) {
        MFU_LOG(MFU_LOG_ERR,
            "Need at least one src rank and one dst rank (src=%d dst=%d)",
            local_src, local_dst);
        return -1;
    }

    return 0;
}

static int nsync_build_role_info(
    const nsync_options_t* opts,
    const nsync_role_t* all_roles,
    int ranks,
    int rank,
    nsync_role_info_t* info)
{
    int src_count = 0;
    int dst_count = 0;
    for (int i = 0; i < ranks; i++) {
        if (all_roles[i] == NSYNC_ROLE_SRC) {
            src_count++;
        } else if (all_roles[i] == NSYNC_ROLE_DST) {
            dst_count++;
        }
    }

    info->src_count = src_count;
    info->dst_count = dst_count;
    info->role = all_roles[rank];

    info->src_world_ranks = (int*)MFU_MALLOC((size_t)src_count * sizeof(int));
    info->dst_world_ranks = (int*)MFU_MALLOC((size_t)dst_count * sizeof(int));

    int src_idx = 0;
    int dst_idx = 0;
    for (int i = 0; i < ranks; i++) {
        if (all_roles[i] == NSYNC_ROLE_SRC) {
            info->src_world_ranks[src_idx] = i;
            if (i == rank) {
                info->src_index = src_idx;
            }
            src_idx++;
        } else if (all_roles[i] == NSYNC_ROLE_DST) {
            info->dst_world_ranks[dst_idx] = i;
            if (i == rank) {
                info->dst_index = dst_idx;
            }
            dst_idx++;
        }
    }

    int src_color = (info->role == NSYNC_ROLE_SRC) ? 1 : MPI_UNDEFINED;
    int dst_color = (info->role == NSYNC_ROLE_DST) ? 1 : MPI_UNDEFINED;
    nsync_trace_local(opts, "roleinfo-pre-src-split", (uint64_t)src_color, (uint64_t)rank);
    MPI_Comm_split(MPI_COMM_WORLD, src_color, rank, &info->src_comm);
    nsync_trace_local(opts, "roleinfo-post-src-split", (uint64_t)(info->src_comm != MPI_COMM_NULL), 0);
    nsync_trace_local(opts, "roleinfo-pre-dst-split", (uint64_t)dst_color, (uint64_t)rank);
    MPI_Comm_split(MPI_COMM_WORLD, dst_color, rank, &info->dst_comm);
    nsync_trace_local(opts, "roleinfo-post-dst-split", (uint64_t)(info->dst_comm != MPI_COMM_NULL), 0);

    return 0;
}

static int nsync_assign_roles(
    const nsync_options_t* opts,
    const char* src_path,
    const char* dst_path,
    nsync_role_info_t* info)
{
    int rank;
    int ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    nsync_role_t* all_roles = (nsync_role_t*)MFU_MALLOC((size_t)ranks * sizeof(nsync_role_t));
    int* auto_caps = NULL;
    int status = 0;
    nsync_trace_local(opts, "assign-start", (uint64_t)opts->role_mode, (uint64_t)ranks);

    if (opts->role_mode == NSYNC_ROLE_MODE_MAP) {
        if (rank == 0) {
            if (nsync_parse_role_map(opts->role_map, ranks, all_roles) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to parse --role-map: %s", opts->role_map);
                status = -1;
            }
        }

        nsync_trace_local(opts, "assign-pre-map-bcast-status", (uint64_t)status, 0);
        MPI_Bcast(&status, 1, MPI_INT, 0, MPI_COMM_WORLD);
        nsync_trace_local(opts, "assign-post-map-bcast-status", (uint64_t)status, 0);
        MPI_Bcast(all_roles, ranks, MPI_INT, 0, MPI_COMM_WORLD);
        nsync_trace_local(opts, "assign-post-map-bcast-roles", (uint64_t)all_roles[rank], 0);
    } else {
        int can_src = nsync_can_read_source(src_path);
        int can_dst = nsync_can_write_destination(dst_path);

        nsync_role_t local_role = NSYNC_ROLE_UNSET;
        if (can_src && !can_dst) {
            local_role = NSYNC_ROLE_SRC;
        } else if (can_dst && !can_src) {
            local_role = NSYNC_ROLE_DST;
        }

        nsync_trace_local(opts, "assign-pre-auto-allgather-role", (uint64_t)local_role, 0);
        MPI_Allgather(&local_role, 1, MPI_INT, all_roles, 1, MPI_INT, MPI_COMM_WORLD);
        nsync_trace_local(opts, "assign-post-auto-allgather-role", (uint64_t)all_roles[rank], 0);

        auto_caps = (int*)MFU_MALLOC((size_t)(2 * ranks) * sizeof(int));
        int local_caps[2] = {can_src, can_dst};
        nsync_trace_local(opts, "assign-pre-auto-allgather-caps", (uint64_t)can_src, (uint64_t)can_dst);
        MPI_Allgather(local_caps, 2, MPI_INT, auto_caps, 2, MPI_INT, MPI_COMM_WORLD);
        nsync_trace_local(opts, "assign-post-auto-allgather-caps", (uint64_t)auto_caps[2 * rank], (uint64_t)auto_caps[2 * rank + 1]);
    }

    int src_count = 0;
    int dst_count = 0;
    if (rank == 0 && status == 0) {
        status = nsync_validate_and_count_roles(opts, all_roles, auto_caps, ranks, &src_count, &dst_count);
    }

    nsync_trace_local(opts, "assign-pre-status-bcast", (uint64_t)status, 0);
    MPI_Bcast(&status, 1, MPI_INT, 0, MPI_COMM_WORLD);
    nsync_trace_local(opts, "assign-post-status-bcast", (uint64_t)status, 0);
    if (status != 0) {
        mfu_free(&all_roles);
        mfu_free(&auto_caps);
        return -1;
    }

    if (rank == 0) {
        const char* mode = (opts->role_mode == NSYNC_ROLE_MODE_AUTO) ? "auto" : "map";
        MFU_LOG(MFU_LOG_INFO, "Role assignment mode=%s src_ranks=%d dst_ranks=%d", mode, src_count, dst_count);
        if (opts->verbose) {
            for (int i = 0; i < ranks; i++) {
                MFU_LOG(MFU_LOG_INFO, "Rank %d role=%s", i, nsync_role_to_string(all_roles[i]));
            }
        }
    }

    nsync_trace_local(opts, "assign-pre-build-role-info", (uint64_t)all_roles[rank], 0);
    nsync_build_role_info(opts, all_roles, ranks, rank, info);
    nsync_trace_local(opts, "assign-post-build-role-info", (uint64_t)info->role, 0);

    mfu_free(&all_roles);
    mfu_free(&auto_caps);
    return 0;
}

static void nsync_meta_record_free(nsync_meta_record_t* rec)
{
    mfu_free(&rec->relpath);
    mfu_free(&rec->link_target);
}

static void nsync_meta_vec_init(nsync_meta_vec_t* vec)
{
    vec->records = NULL;
    vec->size = 0;
    vec->capacity = 0;
}

static int nsync_meta_vec_push(nsync_meta_vec_t* vec, const nsync_meta_record_t* rec)
{
    if (vec->size == vec->capacity) {
        uint64_t new_capacity = (vec->capacity == 0) ? 1024 : vec->capacity * 2;
        nsync_meta_record_t* new_records = (nsync_meta_record_t*)realloc(vec->records,
            (size_t)new_capacity * sizeof(nsync_meta_record_t));
        if (new_records == NULL) {
            return -1;
        }
        vec->records = new_records;
        vec->capacity = new_capacity;
    }

    vec->records[vec->size] = *rec;
    vec->size++;
    return 0;
}

static void nsync_meta_vec_free(nsync_meta_vec_t* vec)
{
    for (uint64_t i = 0; i < vec->size; i++) {
        nsync_meta_record_free(&vec->records[i]);
    }

    free(vec->records);
    vec->records = NULL;
    vec->size = 0;
    vec->capacity = 0;
}

static void nsync_action_record_free(nsync_action_record_t* rec)
{
    mfu_free(&rec->relpath);
    mfu_free(&rec->link_target);
}

static void nsync_action_vec_init(nsync_action_vec_t* vec)
{
    vec->records = NULL;
    vec->size = 0;
    vec->capacity = 0;
}

static int nsync_action_vec_push(nsync_action_vec_t* vec, const nsync_action_record_t* rec)
{
    if (vec->size == vec->capacity) {
        uint64_t new_capacity = (vec->capacity == 0) ? 1024 : vec->capacity * 2;
        nsync_action_record_t* new_records = (nsync_action_record_t*)realloc(vec->records,
            (size_t)new_capacity * sizeof(nsync_action_record_t));
        if (new_records == NULL) {
            return -1;
        }
        vec->records = new_records;
        vec->capacity = new_capacity;
    }

    vec->records[vec->size] = *rec;
    vec->size++;
    return 0;
}

static void nsync_action_vec_free(nsync_action_vec_t* vec)
{
    for (uint64_t i = 0; i < vec->size; i++) {
        nsync_action_record_free(&vec->records[i]);
    }

    free(vec->records);
    vec->records = NULL;
    vec->size = 0;
    vec->capacity = 0;
}

static char* nsync_build_full_path(const char* root, const char* relpath)
{
    if (strcmp(relpath, ".") == 0) {
        return MFU_STRDUP(root);
    }

    size_t root_len = strlen(root);
    size_t rel_len = strlen(relpath);
    size_t len = root_len + 1 + rel_len + 1;
    char* full = (char*)MFU_MALLOC(len);
    snprintf(full, len, "%s/%s", root, relpath);
    return full;
}

static char* nsync_child_relpath(const char* parent_rel, const char* name)
{
    if (strcmp(parent_rel, ".") == 0) {
        return MFU_STRDUP(name);
    }

    size_t parent_len = strlen(parent_rel);
    size_t name_len = strlen(name);
    size_t len = parent_len + 1 + name_len + 1;
    char* child = (char*)MFU_MALLOC(len);
    snprintf(child, len, "%s/%s", parent_rel, name);
    return child;
}

static int nsync_checkpoint_prepare_resume(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    uint64_t option_hash,
    uint64_t batch_count,
    uint64_t* start_batch,
    int* resumed)
{
    *start_batch = 0;
    *resumed = 0;

    if (opts->dryrun || opts->batch_files == 0 || batch_count <= 1) {
        return 0;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int owner = role_info->dst_world_ranks[0];

    uint64_t local_start = 0;
    int local_resumed = 0;
    int local_found = 0;
    int local_mismatch = 0;
    int local_mismatch_reason = NSYNC_BATCH_STATE_MATCH;
    int local_error = 0;

    if (rank == owner) {
        char* state_file = nsync_build_full_path(dst_path, NSYNC_BATCH_STATE_FILE);

        nsync_batch_state_t state;
        nsync_batch_state_init(&state);

        int malformed = 0;
        if (nsync_batch_state_read(state_file, &state, &local_found, &malformed) != 0) {
            local_error = 1;
        } else if (local_found) {
            if (malformed) {
                local_mismatch = 1;
                local_mismatch_reason = NSYNC_BATCH_STATE_MISMATCH_MALFORMED;
            } else {
                local_mismatch_reason = nsync_batch_state_match_reason(
                    &state, src_path, dst_path, option_hash, batch_count);
                if (local_mismatch_reason != NSYNC_BATCH_STATE_MATCH) {
                    local_mismatch = 1;
                } else {
                    if (state.last_completed + 1 < batch_count) {
                        local_start = state.last_completed + 1;
                    } else {
                        /* If finalization wasn't confirmed before interruption,
                         * replay the final batch to reconstruct deferred state. */
                        if (batch_count > 0 && !state.finalized) {
                            local_start = batch_count - 1;
                        } else {
                            local_start = batch_count;
                        }
                    }
                    if (local_start > batch_count) {
                        local_start = batch_count;
                    }
                    local_resumed = (local_start > 0 && local_start < batch_count) ? 1 : 0;
                }
            }
        }

        nsync_batch_state_free(&state);
        mfu_free(&state_file);
    }

    int global_error = 0;
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to read batch checkpoint state");
        }
        return -1;
    }

    MPI_Bcast(&local_start, 1, MPI_UINT64_T, owner, MPI_COMM_WORLD);
    MPI_Bcast(&local_resumed, 1, MPI_INT, owner, MPI_COMM_WORLD);
    MPI_Bcast(&local_found, 1, MPI_INT, owner, MPI_COMM_WORLD);
    MPI_Bcast(&local_mismatch, 1, MPI_INT, owner, MPI_COMM_WORLD);
    MPI_Bcast(&local_mismatch_reason, 1, MPI_INT, owner, MPI_COMM_WORLD);

    *start_batch = local_start;
    *resumed = local_resumed;

    if (rank == 0) {
        if (local_found && local_mismatch) {
            MFU_LOG(MFU_LOG_WARN,
                "Ignoring %s (%s)",
                NSYNC_BATCH_STATE_FILE,
                nsync_batch_mismatch_reason_string(local_mismatch_reason));
        } else if (local_resumed) {
            MFU_LOG(MFU_LOG_INFO,
                "Resuming batch execution from %" PRIu64 " / %" PRIu64 " using %s",
                local_start + 1, batch_count, NSYNC_BATCH_STATE_FILE);
        } else if (local_found && local_start >= batch_count) {
            MFU_LOG(MFU_LOG_INFO,
                "Checkpoint indicates all batches were completed already (%s)",
                NSYNC_BATCH_STATE_FILE);
        }
    }

    return 0;
}

static int nsync_checkpoint_mark_batch_complete(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    uint64_t option_hash,
    uint64_t batch_count,
    uint64_t batch_id)
{
    if (opts->dryrun || opts->batch_files == 0 || batch_count <= 1) {
        return 0;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int owner = role_info->dst_world_ranks[0];

    int local_error = 0;
    if (rank == owner) {
        char* state_file = nsync_build_full_path(dst_path, NSYNC_BATCH_STATE_FILE);
        nsync_batch_state_t state;
        nsync_batch_state_init(&state);
        state.src_path = MFU_STRDUP(src_path);
        state.dst_path = MFU_STRDUP(dst_path);
        state.option_hash = option_hash;
        state.batch_count = batch_count;
        state.last_completed = batch_id;
        state.finalized = 0;

        if (nsync_batch_state_write(state_file, &state) != 0) {
            local_error = 1;
        }

        nsync_batch_state_free(&state);
        mfu_free(&state_file);
    }

    int global_error = 0;
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0 && rank == 0 && !opts->quiet) {
        MFU_LOG(MFU_LOG_WARN, "Failed to update %s; continuing without checkpoint update", NSYNC_BATCH_STATE_FILE);
    }
    return global_error == 0 ? 0 : -1;
}

static int nsync_checkpoint_mark_finalized(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    uint64_t option_hash,
    uint64_t batch_count)
{
    if (opts->dryrun || opts->batch_files == 0 || batch_count <= 1) {
        return 0;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int owner = role_info->dst_world_ranks[0];

    int local_error = 0;
    if (rank == owner) {
        char* state_file = nsync_build_full_path(dst_path, NSYNC_BATCH_STATE_FILE);
        nsync_batch_state_t state;
        nsync_batch_state_init(&state);
        state.src_path = MFU_STRDUP(src_path);
        state.dst_path = MFU_STRDUP(dst_path);
        state.option_hash = option_hash;
        state.batch_count = batch_count;
        state.last_completed = (batch_count > 0) ? (batch_count - 1) : 0;
        state.finalized = 1;

        if (nsync_batch_state_write(state_file, &state) != 0) {
            local_error = 1;
        }

        nsync_batch_state_free(&state);
        mfu_free(&state_file);
    }

    int global_error = 0;
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0 && rank == 0 && !opts->quiet) {
        MFU_LOG(MFU_LOG_WARN, "Failed to finalize %s state record", NSYNC_BATCH_STATE_FILE);
    }
    return global_error == 0 ? 0 : -1;
}

static void nsync_checkpoint_clear(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* dst_path)
{
    if (opts->dryrun || opts->batch_files == 0) {
        return;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int owner = role_info->dst_world_ranks[0];

    int local_error = 0;
    if (rank == owner) {
        char* state_file = nsync_build_full_path(dst_path, NSYNC_BATCH_STATE_FILE);
        int removed = 0;
        if (unlink(state_file) == 0) {
            removed = 1;
        } else if (errno != ENOENT) {
            local_error = 1;
        }

        if (local_error == 0 && removed) {
            char* path_copy = MFU_STRDUP(state_file);
            char* slash = strrchr(path_copy, '/');
            const char* dirpath = ".";
            int dirfd = -1;
            if (slash != NULL) {
                if (slash == path_copy) {
                    slash[1] = '\0';
                    dirpath = path_copy;
                } else {
                    *slash = '\0';
                    dirpath = path_copy;
                }
            }

            dirfd = open(dirpath, O_RDONLY | O_DIRECTORY);
            if (dirfd < 0 || fsync(dirfd) != 0) {
                local_error = 1;
            }
            if (dirfd >= 0) {
                close(dirfd);
            }
            mfu_free(&path_copy);
        }
        mfu_free(&state_file);
    }

    int global_error = 0;
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0 && rank == 0 && !opts->quiet) {
        MFU_LOG(MFU_LOG_WARN, "Failed to clear %s after successful run", NSYNC_BATCH_STATE_FILE);
    }
}

static char* nsync_readlink_target(const char* path, int* status)
{
    *status = 0;

    size_t bufsize = 256;
    while (bufsize < (size_t)INT_MAX) {
        char* buf = (char*)MFU_MALLOC(bufsize);
        ssize_t nread = readlink(path, buf, bufsize - 1);
        if (nread < 0) {
            mfu_free(&buf);
            *status = -1;
            return NULL;
        }

        if ((size_t)nread < (bufsize - 1)) {
            buf[nread] = '\0';
            return buf;
        }

        mfu_free(&buf);
        bufsize *= 2;
    }

    *status = -1;
    return NULL;
}

static size_t nsync_effective_bufsize(uint64_t requested)
{
    size_t bufsize = (requested > 0) ? (size_t)requested : (size_t)MFU_BUFFER_SIZE;
    if (bufsize > (size_t)INT_MAX) {
        bufsize = (size_t)INT_MAX;
    }
    if (bufsize == 0) {
        bufsize = 1;
    }
    return bufsize;
}

static int nsync_compute_sha256_file(
    const char* fullpath,
    size_t bufsize,
    unsigned char digest[SHA256_DIGEST_LENGTH])
{
    int fd = open(fullpath, O_RDONLY);
    if (fd < 0) {
        return -1;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx == NULL) {
        close(fd);
        return -1;
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(ctx);
        close(fd);
        return -1;
    }

    char* buf = (char*)MFU_MALLOC(bufsize);
    int rc = 0;

    while (1) {
        ssize_t nread = read(fd, buf, bufsize);
        if (nread < 0) {
            if (errno == EINTR) {
                continue;
            }
            rc = -1;
            break;
        }

        if (nread == 0) {
            break;
        }

        if (EVP_DigestUpdate(ctx, buf, (size_t)nread) != 1) {
            rc = -1;
            break;
        }
    }

    if (rc == 0) {
        unsigned int digest_len = 0;
        if (EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1 ||
            digest_len != SHA256_DIGEST_LENGTH)
        {
            rc = -1;
        }
    }

    EVP_MD_CTX_free(ctx);
    close(fd);
    mfu_free(&buf);
    return rc;
}

static int nsync_scan_filter_match_relpath(
    const nsync_scan_filter_t* filter,
    const char* relpath)
{
    if (filter == NULL || !filter->enabled) {
        return 1;
    }

    if (filter->batch_count == 0 || filter->batch_id >= filter->batch_count) {
        return 0;
    }

    if (strcmp(relpath, ".") == 0) {
        return filter->batch_id == 0;
    }

    uint32_t hash = mfu_hash_jenkins(relpath, strlen(relpath));
    uint64_t id = (uint64_t)hash % filter->batch_count;
    return id == filter->batch_id;
}

typedef int (*nsync_scan_emit_fn)(const nsync_meta_record_t* rec, void* emit_arg);

static void nsync_scan_recursive(
    const nsync_options_t* opts,
    const char* root,
    const char* relpath,
    nsync_role_t side,
    nsync_meta_vec_t* out,
    int* scan_errors,
    const nsync_scan_filter_t* filter,
    uint64_t* item_count,
    size_t digest_bufsize,
    int dirs_only,
    nsync_scan_emit_fn emit_fn,
    void* emit_arg)
{
    char* fullpath = nsync_build_full_path(root, relpath);

    struct stat st;
    if (lstat(fullpath, &st) != 0) {
        (*scan_errors)++;
        mfu_free(&fullpath);
        return;
    }

    if (item_count != NULL) {
        (*item_count)++;
    }

    mfu_filetype type = mfu_flist_mode_to_filetype(st.st_mode);
    int include = (out != NULL || emit_fn != NULL) ? nsync_scan_filter_match_relpath(filter, relpath) : 0;
    if (dirs_only && type != MFU_TYPE_DIR) {
        include = 0;
    }

    if (include) {
        nsync_meta_record_t rec;
        memset(&rec, 0, sizeof(rec));
        rec.relpath = MFU_STRDUP(relpath);
        rec.type = type;
        rec.mode = (uint64_t)st.st_mode;
        rec.uid = (uint64_t)st.st_uid;
        rec.gid = (uint64_t)st.st_gid;
        rec.size = (uint64_t)st.st_size;
        rec.mtime = (uint64_t)st.st_mtim.tv_sec;
        rec.mtime_nsec = (uint64_t)st.st_mtim.tv_nsec;
        rec.link_target = NULL;
        rec.digest_valid = 0;
        rec.side = side;

        if (rec.type == MFU_TYPE_LINK) {
            int link_status = 0;
            rec.link_target = nsync_readlink_target(fullpath, &link_status);
            if (link_status != 0) {
                (*scan_errors)++;
                rec.link_target = MFU_STRDUP("");
            }
        }

        if (rec.type == MFU_TYPE_FILE && opts->contents) {
            if (rec.size == 0) {
                memcpy(rec.digest, NSYNC_SHA256_EMPTY, SHA256_DIGEST_LENGTH);
                rec.digest_valid = 1;
            } else {
                if (nsync_compute_sha256_file(fullpath, digest_bufsize, rec.digest) == 0) {
                    rec.digest_valid = 1;
                } else {
                    (*scan_errors)++;
                }
            }
        }

        if (out != NULL) {
            if (nsync_meta_vec_push(out, &rec) != 0) {
                (*scan_errors)++;
                nsync_meta_record_free(&rec);
                mfu_free(&fullpath);
                return;
            }
        } else if (emit_fn != NULL) {
            if (emit_fn(&rec, emit_arg) != 0) {
                (*scan_errors)++;
                nsync_meta_record_free(&rec);
                mfu_free(&fullpath);
                return;
            }
            nsync_meta_record_free(&rec);
        }
    }

    if (type == MFU_TYPE_DIR) {
        DIR* dir = opendir(fullpath);
        if (dir == NULL) {
            (*scan_errors)++;
            mfu_free(&fullpath);
            return;
        }

        struct dirent* dent;
        while ((dent = readdir(dir)) != NULL) {
            const char* name = dent->d_name;
            if ((strcmp(name, ".") == 0) || (strcmp(name, "..") == 0)) {
                continue;
            }

            char* child_rel = nsync_child_relpath(relpath, name);
            nsync_scan_recursive(
                opts, root, child_rel, side, out, scan_errors, filter, item_count,
                digest_bufsize, dirs_only, emit_fn, emit_arg);
            mfu_free(&child_rel);
        }

        closedir(dir);
    }

    mfu_free(&fullpath);
}

static void nsync_scan_role_path_filtered(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    nsync_meta_vec_t* out,
    int* scan_errors,
    const nsync_scan_filter_t* filter,
    uint64_t* item_count,
    int dirs_only,
    nsync_scan_emit_fn emit_fn,
    void* emit_arg)
{
    size_t digest_bufsize = nsync_effective_bufsize(opts->bufsize);

    if (role_info->role == NSYNC_ROLE_SRC) {
        nsync_scan_recursive(
            opts, src_path, ".", NSYNC_ROLE_SRC, out, scan_errors, filter, item_count,
            digest_bufsize, dirs_only, emit_fn, emit_arg);
    } else if (role_info->role == NSYNC_ROLE_DST) {
        nsync_scan_recursive(
            opts, dst_path, ".", NSYNC_ROLE_DST, out, scan_errors, filter, item_count,
            digest_bufsize, dirs_only, emit_fn, emit_arg);
    }
}

static int nsync_compute_batch_count(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    uint64_t* batch_count_out,
    uint64_t* global_src_items_out,
    uint64_t* global_dst_items_out,
    int* local_scan_errors_out)
{
    if (batch_count_out == NULL || global_src_items_out == NULL ||
        global_dst_items_out == NULL || local_scan_errors_out == NULL)
    {
        return -1;
    }

    uint64_t local_items = 0;
    int local_scan_errors = 0;
    nsync_scan_role_path_filtered(
        opts, role_info, src_path, dst_path, NULL, &local_scan_errors, NULL, &local_items, 0, NULL, NULL);

    uint64_t local_src_items = 0;
    uint64_t local_dst_items = 0;
    if (role_info->role == NSYNC_ROLE_SRC) {
        local_src_items = local_items;
    } else if (role_info->role == NSYNC_ROLE_DST) {
        local_dst_items = local_items;
    }

    uint64_t global_src_items = 0;
    uint64_t global_dst_items = 0;
    MPI_Allreduce(&local_src_items, &global_src_items, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&local_dst_items, &global_dst_items, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    uint64_t batch_count = 1;
    if (opts->batch_files > 0) {
        uint64_t max_items = global_src_items;
        if (global_dst_items > max_items) {
            max_items = global_dst_items;
        }

        if (max_items > 0) {
            uint64_t div = max_items / opts->batch_files;
            uint64_t mod = max_items % opts->batch_files;
            batch_count = div + ((mod > 0) ? 1 : 0);
            if (batch_count == 0) {
                batch_count = 1;
            }
        }
    }

    *batch_count_out = batch_count;
    *global_src_items_out = global_src_items;
    *global_dst_items_out = global_dst_items;
    *local_scan_errors_out = local_scan_errors;
    return 0;
}

static size_t nsync_meta_pack_size(const nsync_meta_record_t* rec, int include_digest)
{
    uint32_t path_len = (uint32_t)strlen(rec->relpath);
    uint32_t link_len = (rec->link_target != NULL) ? (uint32_t)strlen(rec->link_target) : 0;
    size_t digest_bytes = include_digest ? (4 + SHA256_DIGEST_LENGTH) : 0;
    return 4 + 4 + (6 * 8) + 4 + 4 + digest_bytes + (size_t)path_len + (size_t)link_len;
}

static size_t nsync_meta_pack(char* buf, const nsync_meta_record_t* rec, int include_digest)
{
    uint32_t role_u32 = (uint32_t)rec->side;
    uint32_t type_u32 = (uint32_t)rec->type;
    uint32_t path_len = (uint32_t)strlen(rec->relpath);
    uint32_t link_len = (rec->link_target != NULL) ? (uint32_t)strlen(rec->link_target) : 0;

    char* ptr = buf;
    mfu_pack_uint32(&ptr, role_u32);
    mfu_pack_uint32(&ptr, type_u32);
    mfu_pack_uint64(&ptr, rec->mode);
    mfu_pack_uint64(&ptr, rec->uid);
    mfu_pack_uint64(&ptr, rec->gid);
    mfu_pack_uint64(&ptr, rec->size);
    mfu_pack_uint64(&ptr, rec->mtime);
    mfu_pack_uint64(&ptr, rec->mtime_nsec);
    mfu_pack_uint32(&ptr, path_len);
    mfu_pack_uint32(&ptr, link_len);
    if (include_digest) {
        mfu_pack_uint32(&ptr, rec->digest_valid);
        memcpy(ptr, rec->digest, SHA256_DIGEST_LENGTH);
        ptr += SHA256_DIGEST_LENGTH;
    }

    memcpy(ptr, rec->relpath, (size_t)path_len);
    ptr += path_len;

    if (link_len > 0) {
        memcpy(ptr, rec->link_target, (size_t)link_len);
        ptr += link_len;
    }

    return (size_t)(ptr - buf);
}

static int nsync_meta_unpack(
    const char** pptr,
    const char* end,
    int include_digest,
    nsync_meta_record_t* rec)
{
    const char* ptr = *pptr;

    size_t base_bytes = 4 + 4 + (6 * 8) + 4 + 4 + (include_digest ? (4 + SHA256_DIGEST_LENGTH) : 0);
    if ((size_t)(end - ptr) < base_bytes) {
        return -1;
    }

    uint32_t role_u32;
    uint32_t type_u32;
    uint32_t path_len;
    uint32_t link_len;

    mfu_unpack_uint32(&ptr, &role_u32);
    mfu_unpack_uint32(&ptr, &type_u32);
    mfu_unpack_uint64(&ptr, &rec->mode);
    mfu_unpack_uint64(&ptr, &rec->uid);
    mfu_unpack_uint64(&ptr, &rec->gid);
    mfu_unpack_uint64(&ptr, &rec->size);
    mfu_unpack_uint64(&ptr, &rec->mtime);
    mfu_unpack_uint64(&ptr, &rec->mtime_nsec);
    mfu_unpack_uint32(&ptr, &path_len);
    mfu_unpack_uint32(&ptr, &link_len);
    rec->digest_valid = 0;
    memset(rec->digest, 0, sizeof(rec->digest));
    if (include_digest) {
        mfu_unpack_uint32(&ptr, &rec->digest_valid);
        memcpy(rec->digest, ptr, SHA256_DIGEST_LENGTH);
        ptr += SHA256_DIGEST_LENGTH;
    }

    if ((size_t)(end - ptr) < (size_t)path_len + (size_t)link_len) {
        return -1;
    }

    rec->side = (nsync_role_t)role_u32;
    rec->type = (mfu_filetype)type_u32;
    rec->relpath = (char*)MFU_MALLOC((size_t)path_len + 1);
    memcpy(rec->relpath, ptr, (size_t)path_len);
    rec->relpath[path_len] = '\0';
    ptr += path_len;

    if (link_len > 0) {
        rec->link_target = (char*)MFU_MALLOC((size_t)link_len + 1);
        memcpy(rec->link_target, ptr, (size_t)link_len);
        rec->link_target[link_len] = '\0';
        ptr += link_len;
    } else {
        rec->link_target = NULL;
    }

    *pptr = ptr;
    return 0;
}

static void nsync_batch_spool_init(nsync_batch_spool_t* spool)
{
    memset(spool, 0, sizeof(*spool));
    spool->max_open_fds = 64;
    spool->io_error = 0;
}

static uint64_t nsync_batch_id_from_relpath(const char* relpath, uint64_t batch_count)
{
    if (batch_count == 0) {
        return 0;
    }

    if (strcmp(relpath, ".") == 0) {
        return 0;
    }

    uint32_t hash = mfu_hash_jenkins(relpath, strlen(relpath));
    return (uint64_t)hash % batch_count;
}

static int nsync_batch_spool_path(
    const nsync_batch_spool_t* spool,
    uint64_t batch_id,
    char* path,
    size_t size)
{
    int written = snprintf(path, size, "%s/batch-%" PRIu64 ".bin", spool->dir, batch_id);
    if (written <= 0 || (size_t)written >= size) {
        return -1;
    }
    return 0;
}

static int nsync_batch_spool_write_all(int fd, const char* buf, size_t size)
{
    size_t written = 0;
    while (written < size) {
        ssize_t n = write(fd, buf + written, size - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        written += (size_t)n;
    }
    return 0;
}

static int nsync_batch_spool_read_all(int fd, char* buf, size_t size, int* eof)
{
    *eof = 0;
    size_t got = 0;
    while (got < size) {
        ssize_t n = read(fd, buf + got, size - got);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            if (got == 0) {
                *eof = 1;
                return 0;
            }
            return -1;
        }
        got += (size_t)n;
    }
    return 0;
}

static void nsync_batch_spool_close_open_fds(nsync_batch_spool_t* spool)
{
    if (spool->batch_fds == NULL) {
        return;
    }

    size_t count = (size_t)spool->batch_count;
    for (size_t i = 0; i < count; i++) {
        if (spool->batch_fds[i] >= 0) {
            close(spool->batch_fds[i]);
            spool->batch_fds[i] = -1;
        }
    }
    spool->open_fds = 0;
}

static int nsync_batch_spool_prepare(
    nsync_batch_spool_t* spool,
    uint64_t batch_count,
    uint64_t start_batch,
    int include_digest)
{
    if (batch_count == 0) {
        return -1;
    }

    size_t count = (size_t)batch_count;
    if ((uint64_t)count != batch_count) {
        return -1;
    }

    const char* tmpdir = getenv("TMPDIR");
    if (tmpdir == NULL || *tmpdir == '\0') {
        tmpdir = "/tmp";
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    size_t dir_len = strlen(tmpdir) + 64;
    spool->dir = (char*)MFU_MALLOC(dir_len);
    snprintf(spool->dir, dir_len, "%s/nsync-spool-rank%d-XXXXXX", tmpdir, rank);
    if (mkdtemp(spool->dir) == NULL) {
        mfu_free(&spool->dir);
        return -1;
    }

    spool->batch_count = batch_count;
    spool->start_batch = start_batch;
    spool->include_digest = include_digest;
    spool->open_fds = 0;
    spool->io_error = 0;
    spool->records_written = 0;
    spool->bytes_written = 0;
    spool->batch_fds = (int*)MFU_MALLOC(count * sizeof(int));
    spool->batch_has_data = (unsigned char*)MFU_MALLOC(count * sizeof(unsigned char));
    for (size_t i = 0; i < count; i++) {
        spool->batch_fds[i] = -1;
        spool->batch_has_data[i] = 0;
    }

    return 0;
}

static int nsync_batch_spool_get_append_fd(nsync_batch_spool_t* spool, uint64_t batch_id)
{
    size_t idx = (size_t)batch_id;
    if (spool->batch_fds[idx] >= 0) {
        return spool->batch_fds[idx];
    }

    if (spool->open_fds >= spool->max_open_fds) {
        nsync_batch_spool_close_open_fds(spool);
    }

    char path[PATH_MAX];
    if (nsync_batch_spool_path(spool, batch_id, path, sizeof(path)) != 0) {
        return -1;
    }

    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0) {
        return -1;
    }

    spool->batch_fds[idx] = fd;
    spool->open_fds++;
    return fd;
}

static int nsync_batch_spool_append_record(nsync_batch_spool_t* spool, const nsync_meta_record_t* rec)
{
    uint64_t batch_id = nsync_batch_id_from_relpath(rec->relpath, spool->batch_count);
    if (batch_id >= spool->batch_count) {
        spool->io_error = 1;
        return -1;
    }

    if (batch_id < spool->start_batch) {
        return 0;
    }

    size_t packed_size = nsync_meta_pack_size(rec, spool->include_digest);
    if (packed_size > UINT32_MAX) {
        spool->io_error = 1;
        return -1;
    }

    char* packed = (char*)MFU_MALLOC(packed_size);
    if (nsync_meta_pack(packed, rec, spool->include_digest) != packed_size) {
        spool->io_error = 1;
        mfu_free(&packed);
        return -1;
    }

    int fd = nsync_batch_spool_get_append_fd(spool, batch_id);
    if (fd < 0) {
        spool->io_error = 1;
        mfu_free(&packed);
        return -1;
    }

    uint32_t len = (uint32_t)packed_size;
    int rc = 0;
    if (nsync_batch_spool_write_all(fd, (const char*)&len, sizeof(len)) != 0 ||
        nsync_batch_spool_write_all(fd, packed, packed_size) != 0)
    {
        rc = -1;
        spool->io_error = 1;
    }

    if (rc == 0) {
        spool->batch_has_data[(size_t)batch_id] = 1;
        spool->records_written++;
        spool->bytes_written += (uint64_t)sizeof(len) + (uint64_t)packed_size;
    }

    mfu_free(&packed);
    return rc;
}

static int nsync_batch_spool_scan_emit(const nsync_meta_record_t* rec, void* emit_arg)
{
    nsync_batch_spool_t* spool = (nsync_batch_spool_t*)emit_arg;
    return nsync_batch_spool_append_record(spool, rec);
}

static int nsync_batch_spool_build(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    nsync_batch_spool_t* spool,
    int* local_scan_errors_out)
{
    int local_scan_errors = 0;
    nsync_scan_role_path_filtered(
        opts, role_info, src_path, dst_path,
        NULL, &local_scan_errors, NULL, NULL, 0,
        nsync_batch_spool_scan_emit, spool);

    nsync_batch_spool_close_open_fds(spool);
    *local_scan_errors_out = local_scan_errors;
    return spool->io_error ? -1 : 0;
}

static int nsync_batch_spool_load_batch(
    const nsync_batch_spool_t* spool,
    uint64_t batch_id,
    nsync_meta_vec_t* out)
{
    if (batch_id >= spool->batch_count) {
        return -1;
    }

    if (!spool->batch_has_data[(size_t)batch_id]) {
        return 0;
    }

    char path[PATH_MAX];
    if (nsync_batch_spool_path(spool, batch_id, path, sizeof(path)) != 0) {
        return -1;
    }

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }

    int rc = 0;
    while (1) {
        uint32_t rec_len = 0;
        int eof = 0;
        if (nsync_batch_spool_read_all(fd, (char*)&rec_len, sizeof(rec_len), &eof) != 0) {
            rc = -1;
            break;
        }
        if (eof) {
            break;
        }

        if (rec_len == 0) {
            rc = -1;
            break;
        }

        char* packed = (char*)MFU_MALLOC((size_t)rec_len);
        int payload_eof = 0;
        if (nsync_batch_spool_read_all(fd, packed, (size_t)rec_len, &payload_eof) != 0 || payload_eof) {
            mfu_free(&packed);
            rc = -1;
            break;
        }

        nsync_meta_record_t rec;
        memset(&rec, 0, sizeof(rec));
        const char* ptr = packed;
        const char* end = packed + rec_len;
        if (nsync_meta_unpack(&ptr, end, spool->include_digest, &rec) != 0 || ptr != end) {
            mfu_free(&packed);
            rc = -1;
            break;
        }
        mfu_free(&packed);

        if (nsync_meta_vec_push(out, &rec) != 0) {
            nsync_meta_record_free(&rec);
            rc = -1;
            break;
        }
    }

    close(fd);
    return rc;
}

static void nsync_batch_spool_cleanup(nsync_batch_spool_t* spool)
{
    if (spool == NULL) {
        return;
    }

    nsync_batch_spool_close_open_fds(spool);

    if (spool->dir != NULL) {
        if (spool->batch_has_data != NULL) {
            size_t count = (size_t)spool->batch_count;
            for (size_t i = 0; i < count; i++) {
                if (!spool->batch_has_data[i]) {
                    continue;
                }

                char path[PATH_MAX];
                if (nsync_batch_spool_path(spool, (uint64_t)i, path, sizeof(path)) == 0) {
                    unlink(path);
                }
            }
        }
        rmdir(spool->dir);
    }

    mfu_free(&spool->batch_fds);
    mfu_free(&spool->batch_has_data);
    mfu_free(&spool->dir);
    spool->batch_count = 0;
    spool->start_batch = 0;
    spool->include_digest = 0;
    spool->open_fds = 0;
    spool->io_error = 0;
    spool->records_written = 0;
    spool->bytes_written = 0;
}

static int nsync_owner_rank_for_path(const char* relpath, int ranks)
{
    uint32_t hash = mfu_hash_jenkins(relpath, strlen(relpath));
    return (int)(hash % (uint32_t)ranks);
}

static int nsync_metadata_redistribute(
    const nsync_meta_vec_t* local,
    nsync_meta_vec_t* planner,
    const nsync_options_t* opts)
{
    int rank;
    int ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    int rc = 0;
    int local_error = 0;
    char* send_buf = NULL;
    char* recv_buf = NULL;

    int* send_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* send_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* offsets = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));

    for (int i = 0; i < ranks; i++) {
        send_counts[i] = 0;
        recv_counts[i] = 0;
        send_displs[i] = 0;
        recv_displs[i] = 0;
        offsets[i] = 0;
    }

    nsync_trace_local(opts, "redistribute-start", local->size, 0);

    int include_digest = opts->contents ? 1 : 0;

    for (uint64_t i = 0; i < local->size; i++) {
        const nsync_meta_record_t* rec = &local->records[i];
        int owner = nsync_owner_rank_for_path(rec->relpath, ranks);
        size_t bytes = nsync_meta_pack_size(rec, include_digest);

        if (owner < 0 || owner >= ranks) {
            local_error = 1;
            break;
        }

        if (bytes > (size_t)INT_MAX || send_counts[owner] > INT_MAX - (int)bytes) {
            local_error = 1;
            break;
        }

        send_counts[owner] += (int)bytes;
    }

    if (nsync_sync_error_point(opts, "redistribute-pre-alltoall", local_error) != 0) {
        rc = -1;
        goto cleanup;
    }

    MPI_Alltoall(send_counts, 1, MPI_INT, recv_counts, 1, MPI_INT, MPI_COMM_WORLD);

    int total_send = 0;
    int total_recv = 0;
    for (int i = 0; i < ranks; i++) {
        send_displs[i] = total_send;
        recv_displs[i] = total_recv;

        if (send_counts[i] < 0 || recv_counts[i] < 0 ||
            total_send > INT_MAX - send_counts[i] ||
            total_recv > INT_MAX - recv_counts[i])
        {
            local_error = 1;
            break;
        }

        total_send += send_counts[i];
        total_recv += recv_counts[i];
    }

    nsync_trace_local(opts, "redistribute-post-alltoall", (uint64_t)total_send, (uint64_t)total_recv);

    if (nsync_sync_error_point(opts, "redistribute-post-alltoall-sync", local_error) != 0) {
        rc = -1;
        goto cleanup;
    }

    size_t send_alloc = (total_send > 0) ? (size_t)total_send : 1;
    size_t recv_alloc = (total_recv > 0) ? (size_t)total_recv : 1;
    send_buf = (char*)MFU_MALLOC(send_alloc);
    recv_buf = (char*)MFU_MALLOC(recv_alloc);

    for (int i = 0; i < ranks; i++) {
        offsets[i] = send_displs[i];
    }

    for (uint64_t i = 0; i < local->size; i++) {
        const nsync_meta_record_t* rec = &local->records[i];
        int owner = nsync_owner_rank_for_path(rec->relpath, ranks);
        size_t bytes = nsync_meta_pack_size(rec, include_digest);

        if (owner < 0 || owner >= ranks || offsets[owner] > INT_MAX - (int)bytes) {
            local_error = 1;
            break;
        }

        if (offsets[owner] + (int)bytes > total_send) {
            local_error = 1;
            break;
        }

        char* ptr = send_buf + offsets[owner];
        size_t packed = nsync_meta_pack(ptr, rec, include_digest);
        if (packed != bytes) {
            local_error = 1;
            break;
        }
        offsets[owner] += (int)packed;
    }

    if (nsync_sync_error_point(opts, "redistribute-pre-alltoallv", local_error) != 0) {
        rc = -1;
        goto cleanup;
    }

    MPI_Alltoallv(send_buf, send_counts, send_displs, MPI_BYTE,
                  recv_buf, recv_counts, recv_displs, MPI_BYTE,
                  MPI_COMM_WORLD);

    nsync_trace_local(opts, "redistribute-post-alltoallv", (uint64_t)total_send, (uint64_t)total_recv);

    for (int i = 0; i < ranks && !local_error; i++) {
        const char* ptr = recv_buf + recv_displs[i];
        const char* end = ptr + recv_counts[i];
        while (ptr < end) {
            nsync_meta_record_t rec;
            memset(&rec, 0, sizeof(rec));
            if (nsync_meta_unpack(&ptr, end, include_digest, &rec) != 0) {
                local_error = 1;
                break;
            }

            if (nsync_meta_vec_push(planner, &rec) != 0) {
                nsync_meta_record_free(&rec);
                local_error = 1;
                break;
            }
        }
    }

    nsync_trace_local(opts, "redistribute-post-unpack", planner->size, (uint64_t)local_error);

    if (nsync_sync_error_point(opts, "redistribute-post-unpack-sync", local_error) != 0) {
        rc = -1;
    }

cleanup:
    mfu_free(&send_counts);
    mfu_free(&recv_counts);
    mfu_free(&send_displs);
    mfu_free(&recv_displs);
    mfu_free(&offsets);
    mfu_free(&send_buf);
    mfu_free(&recv_buf);

    (void)rank;
    return rc;
}

enum {
    NSYNC_MSG_COPY_REQ = 4100,
    NSYNC_MSG_COPY_RESP = 4101,
    NSYNC_MSG_COPY_DATA_LEN = 4102,
    NSYNC_MSG_COPY_DATA = 4103
};

static size_t nsync_action_pack_size(const nsync_action_record_t* action)
{
    uint32_t path_len = (uint32_t)strlen(action->relpath);
    uint32_t link_len = (action->link_target != NULL) ? (uint32_t)strlen(action->link_target) : 0;
    return 4 + 4 + 4 + (6 * 8) + 4 + 4 + (size_t)path_len + (size_t)link_len;
}

static size_t nsync_action_pack(char* buf, const nsync_action_record_t* action)
{
    uint32_t type_u32 = (uint32_t)action->type;
    uint32_t src_owner = (uint32_t)(action->src_owner_world + 1);
    uint32_t dst_owner = (uint32_t)(action->dst_owner_world + 1);
    uint32_t path_len = (uint32_t)strlen(action->relpath);
    uint32_t link_len = (action->link_target != NULL) ? (uint32_t)strlen(action->link_target) : 0;

    char* ptr = buf;
    mfu_pack_uint32(&ptr, type_u32);
    mfu_pack_uint32(&ptr, src_owner);
    mfu_pack_uint32(&ptr, dst_owner);
    mfu_pack_uint64(&ptr, action->size);
    mfu_pack_uint64(&ptr, action->mode);
    mfu_pack_uint64(&ptr, action->uid);
    mfu_pack_uint64(&ptr, action->gid);
    mfu_pack_uint64(&ptr, action->mtime);
    mfu_pack_uint64(&ptr, action->mtime_nsec);
    mfu_pack_uint32(&ptr, path_len);
    mfu_pack_uint32(&ptr, link_len);
    memcpy(ptr, action->relpath, (size_t)path_len);
    ptr += path_len;

    if (link_len > 0) {
        memcpy(ptr, action->link_target, (size_t)link_len);
        ptr += link_len;
    }

    return (size_t)(ptr - buf);
}

static int nsync_action_unpack(const char** pptr, const char* end, nsync_action_record_t* action)
{
    memset(action, 0, sizeof(*action));

    const char* ptr = *pptr;
    size_t base_bytes = 4 + 4 + 4 + (6 * 8) + 4 + 4;
    if ((size_t)(end - ptr) < base_bytes) {
        return -1;
    }

    uint32_t type_u32;
    uint32_t src_owner_u32;
    uint32_t dst_owner_u32;
    uint32_t path_len;
    uint32_t link_len;

    mfu_unpack_uint32(&ptr, &type_u32);
    mfu_unpack_uint32(&ptr, &src_owner_u32);
    mfu_unpack_uint32(&ptr, &dst_owner_u32);
    mfu_unpack_uint64(&ptr, &action->size);
    mfu_unpack_uint64(&ptr, &action->mode);
    mfu_unpack_uint64(&ptr, &action->uid);
    mfu_unpack_uint64(&ptr, &action->gid);
    mfu_unpack_uint64(&ptr, &action->mtime);
    mfu_unpack_uint64(&ptr, &action->mtime_nsec);
    mfu_unpack_uint32(&ptr, &path_len);
    mfu_unpack_uint32(&ptr, &link_len);

    if ((size_t)(end - ptr) < (size_t)path_len + (size_t)link_len) {
        return -1;
    }

    action->type = (nsync_action_type_t)type_u32;
    action->src_owner_world = (int)src_owner_u32 - 1;
    action->dst_owner_world = (int)dst_owner_u32 - 1;

    action->relpath = (char*)MFU_MALLOC((size_t)path_len + 1);
    memcpy(action->relpath, ptr, (size_t)path_len);
    action->relpath[path_len] = '\0';
    ptr += path_len;

    if (link_len > 0) {
        action->link_target = (char*)MFU_MALLOC((size_t)link_len + 1);
        memcpy(action->link_target, ptr, (size_t)link_len);
        action->link_target[link_len] = '\0';
        ptr += link_len;
    } else {
        action->link_target = NULL;
    }

    *pptr = ptr;
    return 0;
}

static int nsync_action_owner_for_side(const nsync_action_record_t* action, int to_src_side, int* owner)
{
    if (to_src_side) {
        if (action->type != NSYNC_ACTION_COPY || action->src_owner_world < 0) {
            return 0;
        }
        *owner = action->src_owner_world;
        return 1;
    }

    if (action->dst_owner_world < 0) {
        return 0;
    }
    *owner = action->dst_owner_world;
    return 1;
}

static int nsync_actions_redistribute(
    const nsync_action_vec_t* local_actions,
    int to_src_side,
    nsync_action_vec_t* exec_actions,
    const nsync_options_t* opts)
{
    int rank;
    int ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    int rc = 0;
    int local_error = 0;
    char* send_buf = NULL;
    char* recv_buf = NULL;

    int* send_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* send_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* offsets = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));

    for (int i = 0; i < ranks; i++) {
        send_counts[i] = 0;
        recv_counts[i] = 0;
        send_displs[i] = 0;
        recv_displs[i] = 0;
        offsets[i] = 0;
    }

    nsync_trace_local(opts, to_src_side ? "action-redist-src-start" : "action-redist-dst-start",
        local_actions->size, 0);

    for (uint64_t i = 0; i < local_actions->size; i++) {
        const nsync_action_record_t* action = &local_actions->records[i];
        int owner = -1;
        if (!nsync_action_owner_for_side(action, to_src_side, &owner)) {
            continue;
        }

        size_t bytes = nsync_action_pack_size(action);
        if (owner < 0 || owner >= ranks ||
            bytes > (size_t)INT_MAX || send_counts[owner] > INT_MAX - (int)bytes)
        {
            local_error = 1;
            break;
        }

        send_counts[owner] += (int)bytes;
    }

    if (nsync_sync_error_point(opts,
            to_src_side ? "action-redist-src-pre-alltoall" : "action-redist-dst-pre-alltoall",
            local_error) != 0)
    {
        rc = -1;
        goto cleanup;
    }

    MPI_Alltoall(send_counts, 1, MPI_INT, recv_counts, 1, MPI_INT, MPI_COMM_WORLD);

    int total_send = 0;
    int total_recv = 0;
    for (int i = 0; i < ranks; i++) {
        send_displs[i] = total_send;
        recv_displs[i] = total_recv;
        if (send_counts[i] < 0 || recv_counts[i] < 0 ||
            total_send > INT_MAX - send_counts[i] ||
            total_recv > INT_MAX - recv_counts[i])
        {
            local_error = 1;
            break;
        }
        total_send += send_counts[i];
        total_recv += recv_counts[i];
    }

    if (nsync_sync_error_point(opts,
            to_src_side ? "action-redist-src-post-alltoall-sync" : "action-redist-dst-post-alltoall-sync",
            local_error) != 0)
    {
        rc = -1;
        goto cleanup;
    }

    size_t send_alloc = (total_send > 0) ? (size_t)total_send : 1;
    size_t recv_alloc = (total_recv > 0) ? (size_t)total_recv : 1;
    send_buf = (char*)MFU_MALLOC(send_alloc);
    recv_buf = (char*)MFU_MALLOC(recv_alloc);

    for (int i = 0; i < ranks; i++) {
        offsets[i] = send_displs[i];
    }

    for (uint64_t i = 0; i < local_actions->size; i++) {
        const nsync_action_record_t* action = &local_actions->records[i];
        int owner = -1;
        if (!nsync_action_owner_for_side(action, to_src_side, &owner)) {
            continue;
        }

        size_t bytes = nsync_action_pack_size(action);
        if (owner < 0 || owner >= ranks ||
            offsets[owner] > INT_MAX - (int)bytes ||
            offsets[owner] + (int)bytes > total_send)
        {
            local_error = 1;
            break;
        }

        char* ptr = send_buf + offsets[owner];
        size_t packed = nsync_action_pack(ptr, action);
        if (packed != bytes) {
            local_error = 1;
            break;
        }
        offsets[owner] += (int)packed;
    }

    if (nsync_sync_error_point(opts,
            to_src_side ? "action-redist-src-pre-alltoallv" : "action-redist-dst-pre-alltoallv",
            local_error) != 0)
    {
        rc = -1;
        goto cleanup;
    }

    MPI_Alltoallv(send_buf, send_counts, send_displs, MPI_BYTE,
                  recv_buf, recv_counts, recv_displs, MPI_BYTE,
                  MPI_COMM_WORLD);

    for (int i = 0; i < ranks && !local_error; i++) {
        const char* ptr = recv_buf + recv_displs[i];
        const char* end = ptr + recv_counts[i];
        while (ptr < end) {
            nsync_action_record_t action;
            if (nsync_action_unpack(&ptr, end, &action) != 0) {
                local_error = 1;
                break;
            }

            if (nsync_action_vec_push(exec_actions, &action) != 0) {
                nsync_action_record_free(&action);
                local_error = 1;
                break;
            }
        }
    }

    if (nsync_sync_error_point(opts,
            to_src_side ? "action-redist-src-post-unpack-sync" : "action-redist-dst-post-unpack-sync",
            local_error) != 0)
    {
        rc = -1;
    }

cleanup:
    mfu_free(&send_counts);
    mfu_free(&recv_counts);
    mfu_free(&send_displs);
    mfu_free(&recv_displs);
    mfu_free(&offsets);
    mfu_free(&send_buf);
    mfu_free(&recv_buf);

    (void)rank;
    return rc;
}

static int nsync_relpath_depth(const char* relpath)
{
    if (relpath == NULL || strcmp(relpath, ".") == 0 || *relpath == '\0') {
        return 0;
    }

    int depth = 1;
    for (const char* p = relpath; *p != '\0'; p++) {
        if (*p == '/') {
            depth++;
        }
    }
    return depth;
}

static int nsync_action_exec_priority(const nsync_action_record_t* action)
{
    switch (action->type) {
    case NSYNC_ACTION_REMOVE:
        return 0;
    case NSYNC_ACTION_MKDIR:
        return 1;
    case NSYNC_ACTION_SYMLINK_UPDATE:
        return 2;
    case NSYNC_ACTION_COPY:
        return 3;
    case NSYNC_ACTION_META_UPDATE:
    default:
        return 4;
    }
}

static int nsync_action_exec_sort(const void* a, const void* b)
{
    const nsync_action_record_t* aa = (const nsync_action_record_t*)a;
    const nsync_action_record_t* bb = (const nsync_action_record_t*)b;

    int pa = nsync_action_exec_priority(aa);
    int pb = nsync_action_exec_priority(bb);
    if (pa != pb) {
        return pa - pb;
    }

    int da = nsync_relpath_depth(aa->relpath);
    int db = nsync_relpath_depth(bb->relpath);
    if (aa->type == NSYNC_ACTION_REMOVE) {
        if (da != db) {
            return db - da;
        }
    } else if (aa->type == NSYNC_ACTION_MKDIR) {
        if (da != db) {
            return da - db;
        }
    }

    return strcmp(aa->relpath, bb->relpath);
}

static int nsync_mkdir_one(const char* path, mode_t mode)
{
    if (mkdir(path, mode) == 0) {
        return 0;
    }

    if (errno == EEXIST) {
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            return 0;
        }
    }

    return -1;
}

static int nsync_mkdirs(const char* path, mode_t mode)
{
    if (path == NULL || *path == '\0') {
        return 0;
    }

    char* copy = MFU_STRDUP(path);
    size_t len = strlen(copy);
    if (len == 0) {
        mfu_free(&copy);
        return 0;
    }

    char* p = copy + 1;
    for (; *p != '\0'; p++) {
        if (*p != '/') {
            continue;
        }

        *p = '\0';
        if (*copy != '\0' && nsync_mkdir_one(copy, mode) != 0) {
            mfu_free(&copy);
            return -1;
        }
        *p = '/';
    }

    int rc = 0;
    if (*copy != '\0' && nsync_mkdir_one(copy, mode) != 0) {
        rc = -1;
    }

    mfu_free(&copy);
    return rc;
}

static int nsync_ensure_parent_dirs(const char* path)
{
    char* copy = MFU_STRDUP(path);
    char* slash = strrchr(copy, '/');
    if (slash == NULL || slash == copy) {
        mfu_free(&copy);
        return 0;
    }

    *slash = '\0';
    int rc = nsync_mkdirs(copy, (mode_t)0777);
    mfu_free(&copy);
    return rc;
}

static int nsync_remove_path(const char* fullpath)
{
    struct stat st;
    if (lstat(fullpath, &st) != 0) {
        if (errno == ENOENT) {
            return 0;
        }
        return -1;
    }

    if (S_ISDIR(st.st_mode)) {
        DIR* dir = opendir(fullpath);
        if (dir == NULL) {
            return -1;
        }

        struct dirent* dent;
        while ((dent = readdir(dir)) != NULL) {
            const char* name = dent->d_name;
            if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
                continue;
            }

            char* child = nsync_child_relpath(fullpath, name);
            if (nsync_remove_path(child) != 0) {
                mfu_free(&child);
                closedir(dir);
                return -1;
            }
            mfu_free(&child);
        }

        closedir(dir);
        if (rmdir(fullpath) != 0 && errno != ENOENT) {
            return -1;
        }
        return 0;
    }

    if (unlink(fullpath) != 0) {
        return -1;
    }
    return 0;
}

static int nsync_metadata_errno_ignorable(int err)
{
    if (err == EPERM || err == EACCES || err == ENOSYS || err == ENOTSUP || err == EOPNOTSUPP) {
        return 1;
    }
    return 0;
}

static void nsync_apply_metadata(
    const char* path,
    const nsync_action_record_t* action,
    int nofollow,
    const nsync_options_t* opts,
    int* local_errors)
{
    if (path == NULL || action == NULL || local_errors == NULL) {
        return;
    }

    uid_t uid = (uid_t)action->uid;
    gid_t gid = (gid_t)action->gid;
    int chown_rc = nofollow ? lchown(path, uid, gid) : chown(path, uid, gid);
    if (chown_rc != 0 && !nsync_metadata_errno_ignorable(errno)) {
        (*local_errors)++;
    }

    if (!nofollow) {
        mode_t perms = (mode_t)(action->mode & 07777u);
        if (chmod(path, perms) != 0 && !nsync_metadata_errno_ignorable(errno)) {
            (*local_errors)++;
        }
    }

    struct timespec times[2];
    long nsec = (action->mtime_nsec <= 999999999ULL) ? (long)action->mtime_nsec : 0L;
    time_t sec = (time_t)action->mtime;
    times[0].tv_sec = sec;
    times[0].tv_nsec = nsec;
    times[1].tv_sec = sec;
    times[1].tv_nsec = nsec;

    int flags = nofollow ? AT_SYMLINK_NOFOLLOW : 0;
    if (utimensat(AT_FDCWD, path, times, flags) != 0) {
        if (!nsync_metadata_errno_ignorable(errno)) {
            (*local_errors)++;
        } else if (opts->verbose) {
            MFU_LOG(MFU_LOG_WARN, "Skipping metadata timestamp update for `%s`: %s", path, strerror(errno));
        }
    }
}

static int nsync_write_all(int fd, const char* buf, size_t size)
{
    size_t written = 0;
    while (written < size) {
        ssize_t nwritten = write(fd, buf + written, size - written);
        if (nwritten < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        written += (size_t)nwritten;
    }
    return 0;
}

static int nsync_action_relpath_sort(const void* a, const void* b)
{
    const nsync_action_record_t* aa = (const nsync_action_record_t*)a;
    const nsync_action_record_t* bb = (const nsync_action_record_t*)b;
    return strcmp(aa->relpath, bb->relpath);
}

static const nsync_action_record_t* nsync_find_copy_action_sorted(
    const nsync_action_vec_t* src_actions,
    const char* relpath)
{
    uint64_t lo = 0;
    uint64_t hi = src_actions->size;

    while (lo < hi) {
        uint64_t mid = lo + (hi - lo) / 2;
        const nsync_action_record_t* action = &src_actions->records[mid];
        int cmp = strcmp(action->relpath, relpath);
        if (cmp == 0) {
            if (action->type == NSYNC_ACTION_COPY) {
                return action;
            }
            return NULL;
        }

        if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    return NULL;
}

static size_t nsync_copy_req_pack_size(const char* relpath)
{
    return 4 + strlen(relpath);
}

static size_t nsync_copy_req_pack(char* buf, const char* relpath)
{
    uint32_t path_len = (uint32_t)strlen(relpath);
    char* ptr = buf;
    mfu_pack_uint32(&ptr, path_len);
    memcpy(ptr, relpath, (size_t)path_len);
    ptr += path_len;
    return (size_t)(ptr - buf);
}

static int nsync_copy_req_unpack(const char* buf, size_t bytes, char** relpath)
{
    const char* ptr = buf;
    const char* end = buf + bytes;
    if ((size_t)(end - ptr) < 4) {
        return -1;
    }

    uint32_t path_len = 0;
    mfu_unpack_uint32(&ptr, &path_len);
    if ((size_t)(end - ptr) < (size_t)path_len) {
        return -1;
    }

    *relpath = (char*)MFU_MALLOC((size_t)path_len + 1);
    memcpy(*relpath, ptr, (size_t)path_len);
    (*relpath)[path_len] = '\0';
    return 0;
}

static int nsync_source_send_copy_file(
    const char* src_root,
    const nsync_action_record_t* action,
    int dst_rank,
    const nsync_options_t* opts,
    int* local_errors)
{
    if (dst_rank < 0) {
        (*local_errors)++;
        return -1;
    }

    int status = 0;
    char* src_fullpath = nsync_build_full_path(src_root, action->relpath);
    int fd = open(src_fullpath, O_RDONLY);
    if (fd < 0) {
        status = errno;
        MPI_Send(&status, 1, MPI_INT, dst_rank, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD);
        (*local_errors)++;
        mfu_free(&src_fullpath);
        return -1;
    }

    MPI_Send(&status, 1, MPI_INT, dst_rank, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD);

    size_t bufsize = nsync_effective_bufsize(opts->bufsize);
    char* buf = (char*)MFU_MALLOC(bufsize);

    int io_error = 0;
    while (1) {
        ssize_t nread = read(fd, buf, bufsize);
        if (nread < 0) {
            if (errno == EINTR) {
                continue;
            }
            io_error = 1;
            break;
        }

        uint32_t chunk = (uint32_t)nread;
        MPI_Send(&chunk, 1, MPI_UINT32_T, dst_rank, NSYNC_MSG_COPY_DATA_LEN, MPI_COMM_WORLD);
        if (chunk == 0) {
            break;
        }
        MPI_Send(buf, (int)chunk, MPI_BYTE, dst_rank, NSYNC_MSG_COPY_DATA, MPI_COMM_WORLD);
    }

    if (io_error) {
        uint32_t terminator = 0;
        MPI_Send(&terminator, 1, MPI_UINT32_T, dst_rank, NSYNC_MSG_COPY_DATA_LEN, MPI_COMM_WORLD);
        (*local_errors)++;
    }

    close(fd);
    mfu_free(&buf);
    mfu_free(&src_fullpath);
    return io_error ? -1 : 0;
}

static void nsync_source_copy_service(
    const char* src_root,
    nsync_action_vec_t* src_actions,
    const nsync_options_t* opts,
    int* local_errors)
{
    uint64_t expected = 0;
    for (uint64_t i = 0; i < src_actions->size; i++) {
        if (src_actions->records[i].type == NSYNC_ACTION_COPY) {
            expected++;
        }
    }

    if (src_actions->size > 1) {
        qsort(src_actions->records, (size_t)src_actions->size,
            sizeof(nsync_action_record_t), nsync_action_relpath_sort);
    }

    nsync_trace_local(opts, "copy-src-service-start", expected, 0);

    for (uint64_t req_idx = 0; req_idx < expected; req_idx++) {
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE, NSYNC_MSG_COPY_REQ, MPI_COMM_WORLD, &status);

        int bytes = 0;
        MPI_Get_count(&status, MPI_BYTE, &bytes);
        if (bytes <= 0) {
            int err = EPROTO;
            MPI_Recv(NULL, 0, MPI_BYTE, status.MPI_SOURCE, NSYNC_MSG_COPY_REQ, MPI_COMM_WORLD, &status);
            MPI_Send(&err, 1, MPI_INT, status.MPI_SOURCE, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD);
            (*local_errors)++;
            continue;
        }

        char* req_buf = (char*)MFU_MALLOC((size_t)bytes);
        MPI_Recv(req_buf, bytes, MPI_BYTE, status.MPI_SOURCE, NSYNC_MSG_COPY_REQ, MPI_COMM_WORLD, &status);

        char* relpath = NULL;
        if (nsync_copy_req_unpack(req_buf, (size_t)bytes, &relpath) != 0) {
            int err = EPROTO;
            MPI_Send(&err, 1, MPI_INT, status.MPI_SOURCE, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD);
            (*local_errors)++;
            mfu_free(&req_buf);
            continue;
        }

        const nsync_action_record_t* action = nsync_find_copy_action_sorted(src_actions, relpath);
        if (action == NULL) {
            int err = ENOENT;
            MPI_Send(&err, 1, MPI_INT, status.MPI_SOURCE, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD);
            (*local_errors)++;
        } else {
            nsync_source_send_copy_file(src_root, action, status.MPI_SOURCE, opts, local_errors);
        }

        mfu_free(&relpath);
        mfu_free(&req_buf);
    }
}

static int nsync_destination_receive_copy_file(
    const char* dst_root,
    const nsync_action_record_t* action,
    const nsync_options_t* opts,
    int* local_errors)
{
    if (action->src_owner_world < 0) {
        (*local_errors)++;
        return -1;
    }

    size_t req_bytes = nsync_copy_req_pack_size(action->relpath);
    char* req_buf = (char*)MFU_MALLOC(req_bytes);
    nsync_copy_req_pack(req_buf, action->relpath);
    MPI_Send(req_buf, (int)req_bytes, MPI_BYTE, action->src_owner_world, NSYNC_MSG_COPY_REQ, MPI_COMM_WORLD);
    mfu_free(&req_buf);

    int status = 0;
    MPI_Recv(&status, 1, MPI_INT, action->src_owner_world, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (status != 0) {
        (*local_errors)++;
        return -1;
    }

    char* dst_fullpath = nsync_build_full_path(dst_root, action->relpath);
    if (nsync_ensure_parent_dirs(dst_fullpath) != 0) {
        (*local_errors)++;
    }

    mode_t mode = (mode_t)(action->mode & 07777u);
    if (mode == 0) {
        mode = (mode_t)0644;
    }

    int fd = open(dst_fullpath, O_WRONLY | O_CREAT | O_TRUNC, mode);
    int open_error = (fd < 0);
    if (open_error) {
        (*local_errors)++;
    }

    size_t buf_cap = nsync_effective_bufsize(opts->bufsize);
    char* buf = (char*)MFU_MALLOC(buf_cap);

    int io_error = 0;
    while (1) {
        uint32_t chunk = 0;
        MPI_Recv(&chunk, 1, MPI_UINT32_T, action->src_owner_world, NSYNC_MSG_COPY_DATA_LEN,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (chunk == 0) {
            break;
        }

        if ((size_t)chunk > buf_cap) {
            /* Drain unexpected oversized payload to keep protocol in sync. */
            char* tmp = (char*)MFU_MALLOC((size_t)chunk);
            MPI_Recv(tmp, (int)chunk, MPI_BYTE, action->src_owner_world, NSYNC_MSG_COPY_DATA,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            mfu_free(&tmp);
            (*local_errors)++;
            io_error = 1;
            continue;
        }

        MPI_Recv(buf, (int)chunk, MPI_BYTE, action->src_owner_world, NSYNC_MSG_COPY_DATA,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (!open_error && nsync_write_all(fd, buf, (size_t)chunk) != 0) {
            io_error = 1;
            (*local_errors)++;
            open_error = 1;
        }
    }

    if (fd >= 0) {
        if (close(fd) != 0) {
            io_error = 1;
            (*local_errors)++;
        }
    }

    if (fd >= 0 && !io_error) {
        nsync_apply_metadata(dst_fullpath, action, 0, opts, local_errors);
    }

    mfu_free(&buf);
    mfu_free(&dst_fullpath);
    return io_error ? -1 : 0;
}

static void nsync_deferred_dir_remove_add(
    nsync_action_vec_t* deferred_dir_removes,
    const nsync_action_record_t* action,
    int* local_errors)
{
    if (deferred_dir_removes == NULL) {
        (*local_errors)++;
        return;
    }

    nsync_action_record_t entry;
    memset(&entry, 0, sizeof(entry));
    entry.type = NSYNC_ACTION_REMOVE;
    entry.relpath = MFU_STRDUP(action->relpath);
    entry.link_target = NULL;
    entry.src_owner_world = action->src_owner_world;
    entry.dst_owner_world = action->dst_owner_world;
    entry.size = 0;
    entry.mode = action->mode;
    entry.uid = action->uid;
    entry.gid = action->gid;
    entry.mtime = action->mtime;
    entry.mtime_nsec = action->mtime_nsec;

    if (nsync_action_vec_push(deferred_dir_removes, &entry) != 0) {
        nsync_action_record_free(&entry);
        (*local_errors)++;
    }
}

static void nsync_deferred_dir_meta_add(
    nsync_action_vec_t* deferred_dir_meta_updates,
    const nsync_action_record_t* action,
    int* local_errors)
{
    if (deferred_dir_meta_updates == NULL) {
        (*local_errors)++;
        return;
    }

    nsync_action_record_t entry;
    memset(&entry, 0, sizeof(entry));
    entry.type = NSYNC_ACTION_META_UPDATE;
    entry.relpath = MFU_STRDUP(action->relpath);
    entry.link_target = (action->link_target != NULL) ? MFU_STRDUP(action->link_target) : NULL;
    entry.src_owner_world = action->src_owner_world;
    entry.dst_owner_world = action->dst_owner_world;
    entry.size = action->size;
    entry.mode = action->mode;
    entry.uid = action->uid;
    entry.gid = action->gid;
    entry.mtime = action->mtime;
    entry.mtime_nsec = action->mtime_nsec;

    if (nsync_action_vec_push(deferred_dir_meta_updates, &entry) != 0) {
        nsync_action_record_free(&entry);
        (*local_errors)++;
    }
}

static void nsync_destination_execute_actions(
    const char* dst_root,
    nsync_action_vec_t* dst_actions,
    const nsync_options_t* opts,
    nsync_action_vec_t* deferred_dir_removes,
    nsync_action_vec_t* deferred_dir_meta_updates,
    int* local_errors)
{
    if (dst_actions->size == 0) {
        return;
    }

    qsort(dst_actions->records, (size_t)dst_actions->size, sizeof(nsync_action_record_t), nsync_action_exec_sort);

    for (uint64_t i = 0; i < dst_actions->size; i++) {
        const nsync_action_record_t* action = &dst_actions->records[i];
        if (strcmp(action->relpath, ".") == 0 &&
            (action->type == NSYNC_ACTION_REMOVE || action->type == NSYNC_ACTION_MKDIR))
        {
            continue;
        }

        char* dst_fullpath = nsync_build_full_path(dst_root, action->relpath);
        switch (action->type) {
        case NSYNC_ACTION_REMOVE:
            if (nsync_remove_path(dst_fullpath) != 0 && errno != ENOENT) {
                mode_t mode = (mode_t)action->mode;
                if (opts->batch_files > 0 && S_ISDIR(mode) && errno == ENOTEMPTY) {
                    nsync_deferred_dir_remove_add(deferred_dir_removes, action, local_errors);
                } else {
                    (*local_errors)++;
                }
            }
            break;
        case NSYNC_ACTION_MKDIR: {
            mode_t mode = (mode_t)(action->mode & 07777u);
            if (mode == 0) {
                mode = (mode_t)0777;
            }
            if (nsync_mkdirs(dst_fullpath, mode) != 0) {
                (*local_errors)++;
            } else {
                nsync_deferred_dir_meta_add(deferred_dir_meta_updates, action, local_errors);
            }
            break;
        }
        case NSYNC_ACTION_SYMLINK_UPDATE:
            if (nsync_ensure_parent_dirs(dst_fullpath) != 0) {
                (*local_errors)++;
            }
            if (nsync_remove_path(dst_fullpath) != 0 && errno != ENOENT) {
                (*local_errors)++;
            }
            if (action->link_target == NULL || symlink(action->link_target, dst_fullpath) != 0) {
                (*local_errors)++;
            } else {
                nsync_apply_metadata(dst_fullpath, action, 1, opts, local_errors);
            }
            break;
        case NSYNC_ACTION_COPY:
            nsync_destination_receive_copy_file(dst_root, action, opts, local_errors);
            break;
        case NSYNC_ACTION_META_UPDATE:
            if (S_ISDIR((mode_t)action->mode)) {
                nsync_deferred_dir_meta_add(deferred_dir_meta_updates, action, local_errors);
            } else {
                int nofollow = S_ISLNK((mode_t)action->mode) ? 1 : 0;
                nsync_apply_metadata(dst_fullpath, action, nofollow, opts, local_errors);
            }
            break;
        default:
            break;
        }

        mfu_free(&dst_fullpath);
    }
}

static void nsync_execute_actions_phase4(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_root,
    const char* dst_root,
    nsync_action_vec_t* src_exec_actions,
    nsync_action_vec_t* dst_exec_actions,
    nsync_action_vec_t* deferred_dir_removes,
    nsync_action_vec_t* deferred_dir_meta_updates,
    int* local_errors)
{
    if (role_info->role == NSYNC_ROLE_SRC) {
        nsync_trace_local(opts, "phase4-src-service", src_exec_actions->size, 0);
        nsync_source_copy_service(src_root, src_exec_actions, opts, local_errors);
    } else if (role_info->role == NSYNC_ROLE_DST) {
        nsync_trace_local(opts, "phase4-dst-exec", dst_exec_actions->size, 0);
        nsync_destination_execute_actions(
            dst_root, dst_exec_actions, opts, deferred_dir_removes, deferred_dir_meta_updates, local_errors);
    }
}

static void nsync_finalize_deferred_dir_removes(
    const char* dst_root,
    nsync_action_vec_t* deferred_dir_removes,
    int* local_errors)
{
    if (deferred_dir_removes == NULL || deferred_dir_removes->size == 0) {
        return;
    }

    qsort(
        deferred_dir_removes->records,
        (size_t)deferred_dir_removes->size,
        sizeof(nsync_action_record_t),
        nsync_action_exec_sort);

    const char* last = NULL;
    for (uint64_t i = 0; i < deferred_dir_removes->size; i++) {
        nsync_action_record_t* action = &deferred_dir_removes->records[i];
        if (last != NULL && strcmp(last, action->relpath) == 0) {
            continue;
        }
        last = action->relpath;

        if (strcmp(action->relpath, ".") == 0) {
            continue;
        }

        char* dst_fullpath = nsync_build_full_path(dst_root, action->relpath);
        if (nsync_remove_path(dst_fullpath) != 0 && errno != ENOENT)
        {
            (*local_errors)++;
        }
        mfu_free(&dst_fullpath);
    }
}

static void nsync_finalize_deferred_dir_meta_updates(
    const char* dst_root,
    const nsync_options_t* opts,
    nsync_action_vec_t* deferred_dir_meta_updates,
    int* local_errors)
{
    if (deferred_dir_meta_updates == NULL || deferred_dir_meta_updates->size == 0) {
        return;
    }

    qsort(
        deferred_dir_meta_updates->records,
        (size_t)deferred_dir_meta_updates->size,
        sizeof(nsync_action_record_t),
        nsync_action_relpath_sort);

    const char* last = NULL;
    for (uint64_t i = 0; i < deferred_dir_meta_updates->size; i++) {
        nsync_action_record_t* action = &deferred_dir_meta_updates->records[i];
        if (last != NULL && strcmp(last, action->relpath) == 0) {
            continue;
        }
        last = action->relpath;

        char* dst_fullpath = nsync_build_full_path(dst_root, action->relpath);
        nsync_apply_metadata(dst_fullpath, action, 0, opts, local_errors);
        mfu_free(&dst_fullpath);
    }
}

static int nsync_reconcile_directory_metadata_after_resume(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_root,
    const char* dst_root,
    nsync_action_vec_t* deferred_dir_removes,
    nsync_action_vec_t* deferred_dir_meta_updates,
    int* local_scan_errors_total,
    uint64_t* local_exec_errors_total)
{
    nsync_meta_vec_t local_meta;
    nsync_meta_vec_t planner_meta;
    nsync_action_vec_t planned_actions;
    nsync_action_vec_t src_exec_actions;
    nsync_action_vec_t dst_exec_actions;
    nsync_meta_vec_init(&local_meta);
    nsync_meta_vec_init(&planner_meta);
    nsync_action_vec_init(&planned_actions);
    nsync_action_vec_init(&src_exec_actions);
    nsync_action_vec_init(&dst_exec_actions);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int local_scan_errors = 0;
    nsync_scan_role_path_filtered(
        opts, role_info, src_root, dst_root, &local_meta, &local_scan_errors, NULL, NULL, 1, NULL, NULL);
    *local_scan_errors_total += local_scan_errors;

    int local_has_meta = (local_meta.size > 0) ? 1 : 0;
    int global_has_meta = 0;
    MPI_Allreduce(&local_has_meta, &global_has_meta, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (!global_has_meta) {
        nsync_action_vec_free(&dst_exec_actions);
        nsync_action_vec_free(&src_exec_actions);
        nsync_action_vec_free(&planned_actions);
        nsync_meta_vec_free(&planner_meta);
        nsync_meta_vec_free(&local_meta);
        return 0;
    }

    if (nsync_metadata_redistribute(&local_meta, &planner_meta, opts) != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to redistribute directory metadata during resume reconciliation");
        }
        nsync_action_vec_free(&dst_exec_actions);
        nsync_action_vec_free(&src_exec_actions);
        nsync_action_vec_free(&planned_actions);
        nsync_meta_vec_free(&planner_meta);
        nsync_meta_vec_free(&local_meta);
        return -1;
    }

    int local_plan_error = 0;
    if (nsync_plan_directory_finalize_records(&planner_meta, role_info, &planned_actions) != 0) {
        local_plan_error = 1;
    }
    int global_plan_error = 0;
    MPI_Allreduce(&local_plan_error, &global_plan_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_plan_error != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to plan directory metadata reconciliation actions");
        }
        nsync_action_vec_free(&dst_exec_actions);
        nsync_action_vec_free(&src_exec_actions);
        nsync_action_vec_free(&planned_actions);
        nsync_meta_vec_free(&planner_meta);
        nsync_meta_vec_free(&local_meta);
        return -1;
    }

    int local_has_actions = (planned_actions.size > 0) ? 1 : 0;
    int global_has_actions = 0;
    MPI_Allreduce(&local_has_actions, &global_has_actions, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_has_actions) {
        int local_exec_error = 0;
        int src_redist_rc = nsync_actions_redistribute(&planned_actions, 1, &src_exec_actions, opts);
        int dst_redist_rc = nsync_actions_redistribute(&planned_actions, 0, &dst_exec_actions, opts);
        if (src_redist_rc != 0 || dst_redist_rc != 0) {
            local_exec_error = 1;
        }

        if (nsync_sync_error_point(opts, "resume-dir-meta-post-action-redistribute", local_exec_error) != 0) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to redistribute directory metadata reconciliation actions");
            }
            nsync_action_vec_free(&dst_exec_actions);
            nsync_action_vec_free(&src_exec_actions);
            nsync_action_vec_free(&planned_actions);
            nsync_meta_vec_free(&planner_meta);
            nsync_meta_vec_free(&local_meta);
            return -1;
        }

        int local_exec_errors = 0;
        nsync_execute_actions_phase4(
            opts,
            role_info,
            src_root,
            dst_root,
            &src_exec_actions,
            &dst_exec_actions,
            deferred_dir_removes,
            deferred_dir_meta_updates,
            &local_exec_errors);
        *local_exec_errors_total += (uint64_t)local_exec_errors;
    }

    if (rank == 0 && !opts->quiet) {
        MFU_LOG(MFU_LOG_INFO, "Reconciled directory metadata after checkpoint resume");
    }

    nsync_action_vec_free(&dst_exec_actions);
    nsync_action_vec_free(&src_exec_actions);
    nsync_action_vec_free(&planned_actions);
    nsync_meta_vec_free(&planner_meta);
    nsync_meta_vec_free(&local_meta);
    return 0;
}

static int nsync_restore_destination_root_metadata(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_root,
    const char* dst_root,
    uint64_t* local_exec_errors_total)
{
    if (opts->dryrun || local_exec_errors_total == NULL) {
        return 0;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int src_owner = role_info->src_world_ranks[0];
    int dst_owner = role_info->dst_world_ranks[0];

    uint64_t root_meta[5] = {0, 0, 0, 0, 0};
    int have_root_meta = 0;

    if (rank == src_owner) {
        struct stat st;
        if (lstat(src_root, &st) == 0 && S_ISDIR(st.st_mode)) {
            have_root_meta = 1;
            root_meta[0] = (uint64_t)st.st_mode;
            root_meta[1] = (uint64_t)st.st_uid;
            root_meta[2] = (uint64_t)st.st_gid;
            root_meta[3] = (uint64_t)st.st_mtim.tv_sec;
            root_meta[4] = (uint64_t)st.st_mtim.tv_nsec;
        }
    }

    MPI_Bcast(&have_root_meta, 1, MPI_INT, src_owner, MPI_COMM_WORLD);
    if (!have_root_meta) {
        if (rank == 0 && !opts->quiet) {
            MFU_LOG(MFU_LOG_WARN, "Failed to collect source root metadata for post-checkpoint reconcile");
        }
        if (rank == dst_owner) {
            (*local_exec_errors_total)++;
        }
        return -1;
    }

    MPI_Bcast(root_meta, 5, MPI_UINT64_T, src_owner, MPI_COMM_WORLD);

    if (rank == dst_owner) {
        nsync_action_record_t action;
        memset(&action, 0, sizeof(action));
        action.mode = root_meta[0];
        action.uid = root_meta[1];
        action.gid = root_meta[2];
        action.mtime = root_meta[3];
        action.mtime_nsec = root_meta[4];

        int local_errors = 0;
        nsync_apply_metadata(dst_root, &action, 0, opts, &local_errors);
        *local_exec_errors_total += (uint64_t)local_errors;
    }

    return 0;
}

static int nsync_meta_compare_sort(const void* a, const void* b)
{
    const nsync_meta_record_t* ra = (const nsync_meta_record_t*)a;
    const nsync_meta_record_t* rb = (const nsync_meta_record_t*)b;

    int cmp = strcmp(ra->relpath, rb->relpath);
    if (cmp != 0) {
        return cmp;
    }

    if (ra->side < rb->side) {
        return -1;
    }
    if (ra->side > rb->side) {
        return 1;
    }

    return 0;
}

static void nsync_compare_counts_init(nsync_compare_counts_t* counts)
{
    memset(counts, 0, sizeof(*counts));
}

static void nsync_compare_counts_to_array(const nsync_compare_counts_t* counts, uint64_t out[4])
{
    out[0] = counts->only_src;
    out[1] = counts->only_dst;
    out[2] = counts->common;
    out[3] = counts->changed;
}

static void nsync_compare_counts_from_array(const uint64_t in[4], nsync_compare_counts_t* counts)
{
    counts->only_src = in[0];
    counts->only_dst = in[1];
    counts->common = in[2];
    counts->changed = in[3];
}

static void nsync_compare_counts_add(nsync_compare_counts_t* dst, const nsync_compare_counts_t* src)
{
    dst->only_src += src->only_src;
    dst->only_dst += src->only_dst;
    dst->common += src->common;
    dst->changed += src->changed;
}

static void nsync_action_counts_init(nsync_action_counts_t* counts)
{
    memset(counts, 0, sizeof(*counts));
}

static void nsync_action_counts_to_array(const nsync_action_counts_t* counts, uint64_t out[6])
{
    out[0] = counts->copy;
    out[1] = counts->remove;
    out[2] = counts->mkdir;
    out[3] = counts->symlink_update;
    out[4] = counts->meta_update;
    out[5] = counts->skipped_only_dst;
}

static void nsync_action_counts_from_array(const uint64_t in[6], nsync_action_counts_t* counts)
{
    counts->copy = in[0];
    counts->remove = in[1];
    counts->mkdir = in[2];
    counts->symlink_update = in[3];
    counts->meta_update = in[4];
    counts->skipped_only_dst = in[5];
}

static void nsync_action_counts_add(nsync_action_counts_t* dst, const nsync_action_counts_t* src)
{
    dst->copy += src->copy;
    dst->remove += src->remove;
    dst->mkdir += src->mkdir;
    dst->symlink_update += src->symlink_update;
    dst->meta_update += src->meta_update;
    dst->skipped_only_dst += src->skipped_only_dst;
}

static int nsync_owner_from_group(const int* world_ranks, int count, const char* relpath)
{
    if (count <= 0 || world_ranks == NULL) {
        return -1;
    }

    uint32_t hash = mfu_hash_jenkins(relpath, strlen(relpath));
    int idx = (int)(hash % (uint32_t)count);
    return world_ranks[idx];
}

static int nsync_meta_identity_diff(const nsync_meta_record_t* src, const nsync_meta_record_t* dst)
{
    if (src->mode != dst->mode) {
        return 1;
    }
    if (src->uid != dst->uid) {
        return 1;
    }
    if (src->gid != dst->gid) {
        return 1;
    }
    if (src->mtime != dst->mtime) {
        return 1;
    }
    if (src->mtime_nsec != dst->mtime_nsec) {
        return 1;
    }
    return 0;
}

static int nsync_file_data_diff(
    const nsync_meta_record_t* src,
    const nsync_meta_record_t* dst,
    const nsync_options_t* opts)
{
    if (src->size != dst->size) {
        return 1;
    }

    if (opts->contents) {
        if (!src->digest_valid || !dst->digest_valid) {
            return 1;
        }
        if (memcmp(src->digest, dst->digest, SHA256_DIGEST_LENGTH) != 0) {
            return 1;
        }
        return 0;
    }

    if (src->mtime != dst->mtime) {
        return 1;
    }
    if (src->mtime_nsec != dst->mtime_nsec) {
        return 1;
    }

    return 0;
}

static int nsync_link_target_diff(const nsync_meta_record_t* src, const nsync_meta_record_t* dst)
{
    const char* src_target = (src->link_target != NULL) ? src->link_target : "";
    const char* dst_target = (dst->link_target != NULL) ? dst->link_target : "";
    return strcmp(src_target, dst_target) != 0;
}

static nsync_action_type_t nsync_create_action_for_src(const nsync_meta_record_t* src_rec)
{
    if (src_rec->type == MFU_TYPE_DIR) {
        return NSYNC_ACTION_MKDIR;
    }
    if (src_rec->type == MFU_TYPE_LINK) {
        return NSYNC_ACTION_SYMLINK_UPDATE;
    }
    return NSYNC_ACTION_COPY;
}

static void nsync_action_count_increment(nsync_action_counts_t* counts, nsync_action_type_t type)
{
    switch (type) {
    case NSYNC_ACTION_COPY:
        counts->copy++;
        break;
    case NSYNC_ACTION_REMOVE:
        counts->remove++;
        break;
    case NSYNC_ACTION_MKDIR:
        counts->mkdir++;
        break;
    case NSYNC_ACTION_SYMLINK_UPDATE:
        counts->symlink_update++;
        break;
    case NSYNC_ACTION_META_UPDATE:
        counts->meta_update++;
        break;
    default:
        break;
    }
}

static int nsync_plan_emit_action(
    nsync_action_vec_t* actions,
    nsync_action_counts_t* counts,
    nsync_action_type_t type,
    const char* relpath,
    int src_owner_world,
    int dst_owner_world,
    uint64_t size,
    uint64_t mode,
    uint64_t uid,
    uint64_t gid,
    uint64_t mtime,
    uint64_t mtime_nsec,
    const char* link_target)
{
    nsync_action_record_t action;
    memset(&action, 0, sizeof(action));
    action.type = type;
    action.relpath = MFU_STRDUP(relpath);
    action.link_target = (link_target != NULL) ? MFU_STRDUP(link_target) : NULL;
    action.src_owner_world = src_owner_world;
    action.dst_owner_world = dst_owner_world;
    action.size = size;
    action.mode = mode;
    action.uid = uid;
    action.gid = gid;
    action.mtime = mtime;
    action.mtime_nsec = mtime_nsec;

    if (nsync_action_vec_push(actions, &action) != 0) {
        nsync_action_record_free(&action);
        return -1;
    }

    nsync_action_count_increment(counts, type);
    return 0;
}

static int nsync_plan_create_action(
    nsync_action_vec_t* actions,
    nsync_action_counts_t* counts,
    const char* relpath,
    const nsync_meta_record_t* src_rec,
    int src_owner_world,
    int dst_owner_world)
{
    nsync_action_type_t create_type = nsync_create_action_for_src(src_rec);
    return nsync_plan_emit_action(
        actions, counts, create_type, relpath, src_owner_world, dst_owner_world,
        src_rec->size, src_rec->mode, src_rec->uid, src_rec->gid,
        src_rec->mtime, src_rec->mtime_nsec, src_rec->link_target);
}

static int nsync_plan_planner_records(
    nsync_meta_vec_t* planner,
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    nsync_compare_counts_t* compare_counts,
    nsync_action_vec_t* planned_actions,
    nsync_action_counts_t* action_counts)
{
    nsync_compare_counts_init(compare_counts);
    nsync_action_counts_init(action_counts);

    if (planner->size == 0) {
        return 0;
    }

    qsort(planner->records, (size_t)planner->size, sizeof(nsync_meta_record_t), nsync_meta_compare_sort);

    uint64_t i = 0;
    while (i < planner->size) {
        const char* relpath = planner->records[i].relpath;
        const nsync_meta_record_t* src_rec = NULL;
        const nsync_meta_record_t* dst_rec = NULL;

        while (i < planner->size && strcmp(planner->records[i].relpath, relpath) == 0) {
            if (planner->records[i].side == NSYNC_ROLE_SRC && src_rec == NULL) {
                src_rec = &planner->records[i];
            } else if (planner->records[i].side == NSYNC_ROLE_DST && dst_rec == NULL) {
                dst_rec = &planner->records[i];
            }
            i++;
        }

        int src_owner_world = nsync_owner_from_group(role_info->src_world_ranks, role_info->src_count, relpath);
        int dst_owner_world = nsync_owner_from_group(role_info->dst_world_ranks, role_info->dst_count, relpath);

        if (src_rec != NULL && dst_rec != NULL) {
            int changed = 0;
            compare_counts->common++;

            if (src_rec->type != dst_rec->type) {
                changed = 1;

                if (nsync_plan_emit_action(
                        planned_actions, action_counts, NSYNC_ACTION_REMOVE,
                        relpath, -1, dst_owner_world, 0, dst_rec->mode,
                        dst_rec->uid, dst_rec->gid, dst_rec->mtime, dst_rec->mtime_nsec, NULL) != 0)
                {
                    return -1;
                }

                if (nsync_plan_create_action(
                        planned_actions, action_counts, relpath, src_rec,
                        src_owner_world, dst_owner_world) != 0)
                {
                    return -1;
                }
            } else if (src_rec->type == MFU_TYPE_FILE) {
                if (nsync_file_data_diff(src_rec, dst_rec, opts)) {
                    changed = 1;
                    if (nsync_plan_emit_action(
                            planned_actions, action_counts, NSYNC_ACTION_COPY,
                            relpath, src_owner_world, dst_owner_world,
                            src_rec->size, src_rec->mode,
                            src_rec->uid, src_rec->gid, src_rec->mtime, src_rec->mtime_nsec, NULL) != 0)
                    {
                        return -1;
                    }
                } else if (nsync_meta_identity_diff(src_rec, dst_rec)) {
                    changed = 1;
                    if (nsync_plan_emit_action(
                            planned_actions, action_counts, NSYNC_ACTION_META_UPDATE,
                            relpath, src_owner_world, dst_owner_world,
                            0, src_rec->mode,
                            src_rec->uid, src_rec->gid, src_rec->mtime, src_rec->mtime_nsec, NULL) != 0)
                    {
                        return -1;
                    }
                }
            } else if (src_rec->type == MFU_TYPE_LINK) {
                if (nsync_link_target_diff(src_rec, dst_rec)) {
                    changed = 1;
                    if (nsync_plan_emit_action(
                            planned_actions, action_counts, NSYNC_ACTION_SYMLINK_UPDATE,
                            relpath, src_owner_world, dst_owner_world,
                            0, src_rec->mode,
                            src_rec->uid, src_rec->gid, src_rec->mtime, src_rec->mtime_nsec,
                            src_rec->link_target) != 0)
                    {
                        return -1;
                    }
                } else if (nsync_meta_identity_diff(src_rec, dst_rec)) {
                    changed = 1;
                    if (nsync_plan_emit_action(
                            planned_actions, action_counts, NSYNC_ACTION_META_UPDATE,
                            relpath, src_owner_world, dst_owner_world,
                            0, src_rec->mode,
                            src_rec->uid, src_rec->gid, src_rec->mtime, src_rec->mtime_nsec,
                            src_rec->link_target) != 0)
                    {
                        return -1;
                    }
                }
            } else {
                if (nsync_meta_identity_diff(src_rec, dst_rec) || src_rec->size != dst_rec->size) {
                    changed = 1;
                    if (nsync_plan_emit_action(
                            planned_actions, action_counts, NSYNC_ACTION_META_UPDATE,
                            relpath, src_owner_world, dst_owner_world,
                            0, src_rec->mode,
                            src_rec->uid, src_rec->gid, src_rec->mtime, src_rec->mtime_nsec, NULL) != 0)
                    {
                        return -1;
                    }
                }
            }

            if (changed) {
                compare_counts->changed++;
            }
            continue;
        }

        if (src_rec != NULL) {
            compare_counts->only_src++;
            if (nsync_plan_create_action(
                    planned_actions, action_counts, relpath, src_rec,
                    src_owner_world, dst_owner_world) != 0)
            {
                return -1;
            }
            continue;
        }

        if (dst_rec != NULL) {
            compare_counts->only_dst++;
            if (opts->delete) {
                if (nsync_plan_emit_action(
                        planned_actions, action_counts, NSYNC_ACTION_REMOVE,
                        relpath, -1, dst_owner_world, 0, dst_rec->mode,
                        dst_rec->uid, dst_rec->gid, dst_rec->mtime, dst_rec->mtime_nsec, NULL) != 0)
                {
                    return -1;
                }
            } else {
                action_counts->skipped_only_dst++;
            }
        }
    }

    return 0;
}

static int nsync_plan_directory_finalize_records(
    nsync_meta_vec_t* planner,
    const nsync_role_info_t* role_info,
    nsync_action_vec_t* planned_actions)
{
    if (planner->size == 0) {
        return 0;
    }

    qsort(planner->records, (size_t)planner->size, sizeof(nsync_meta_record_t), nsync_meta_compare_sort);

    nsync_action_counts_t dummy_counts;
    nsync_action_counts_init(&dummy_counts);

    uint64_t i = 0;
    while (i < planner->size) {
        const char* relpath = planner->records[i].relpath;
        const nsync_meta_record_t* src_rec = NULL;
        const nsync_meta_record_t* dst_rec = NULL;

        while (i < planner->size && strcmp(planner->records[i].relpath, relpath) == 0) {
            if (planner->records[i].side == NSYNC_ROLE_SRC && src_rec == NULL) {
                src_rec = &planner->records[i];
            } else if (planner->records[i].side == NSYNC_ROLE_DST && dst_rec == NULL) {
                dst_rec = &planner->records[i];
            }
            i++;
        }

        if (src_rec == NULL || src_rec->type != MFU_TYPE_DIR) {
            continue;
        }

        int src_owner_world = nsync_owner_from_group(role_info->src_world_ranks, role_info->src_count, relpath);
        int dst_owner_world = nsync_owner_from_group(role_info->dst_world_ranks, role_info->dst_count, relpath);

        if (dst_rec == NULL || dst_rec->type != MFU_TYPE_DIR) {
            if (nsync_plan_emit_action(
                    planned_actions, &dummy_counts, NSYNC_ACTION_MKDIR,
                    relpath, src_owner_world, dst_owner_world,
                    0, src_rec->mode, src_rec->uid, src_rec->gid,
                    src_rec->mtime, src_rec->mtime_nsec, NULL) != 0)
            {
                return -1;
            }
            continue;
        }

        if (nsync_meta_identity_diff(src_rec, dst_rec)) {
            if (nsync_plan_emit_action(
                    planned_actions, &dummy_counts, NSYNC_ACTION_META_UPDATE,
                    relpath, src_owner_world, dst_owner_world,
                    0, src_rec->mode, src_rec->uid, src_rec->gid,
                    src_rec->mtime, src_rec->mtime_nsec, NULL) != 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int main(int argc, char** argv)
{
    int rc = 0;

    MPI_Init(&argc, &argv);
    mfu_init();

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    nsync_options_t opts = {
        .dryrun = 0,
        .batch_files = 0,
        .delete = 0,
        .contents = 0,
        .bufsize = MFU_BUFFER_SIZE,
        .chunksize = MFU_CHUNK_SIZE,
        .progress_secs = mfu_progress_timeout,
        .role_mode = NSYNC_ROLE_MODE_AUTO,
        .role_map = NULL,
        .trace = 0,
        .verbose = 0,
        .quiet = 0,
    };

    nsync_batch_spool_t batch_spool;
    nsync_batch_spool_init(&batch_spool);

    int usage = 0;
    int help = 0;
    unsigned long long bytes = 0;
    int option_index = 0;

    static struct option long_options[] = {
        {"dryrun", 0, 0, 'n'},
        {"batch-files", 1, 0, 'b'},
        {"delete", 0, 0, 'D'},
        {"contents", 0, 0, 'c'},
        {"bufsize", 1, 0, 'B'},
        {"chunksize", 1, 0, 'k'},
        {"progress", 1, 0, 'R'},
        {"role-mode", 1, 0, 1000},
        {"role-map", 1, 0, 1001},
        {"trace", 0, 0, 1002},
        {"verbose", 0, 0, 'v'},
        {"quiet", 0, 0, 'q'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0},
    };

    while (1) {
        int c = getopt_long(argc, argv, "b:DcnB:k:R:vqh", long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
        case 'D':
            opts.delete = 1;
            break;
        case 'c':
            opts.contents = 1;
            break;
        case 'n':
            opts.dryrun = 1;
            break;
        case 'b':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to parse --batch-files: %s", optarg);
                }
                usage = 1;
            } else {
                opts.batch_files = (uint64_t)bytes;
            }
            break;
        case 'B':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to parse --bufsize: %s", optarg);
                }
                usage = 1;
            } else {
                opts.bufsize = (uint64_t)bytes;
            }
            break;
        case 'k':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to parse --chunksize: %s", optarg);
                }
                usage = 1;
            } else {
                opts.chunksize = (uint64_t)bytes;
            }
            break;
        case 'R':
            opts.progress_secs = atoi(optarg);
            if (opts.progress_secs < 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Seconds in --progress must be non-negative: %d invalid", opts.progress_secs);
                }
                usage = 1;
            }
            break;
        case 1000:
            if (nsync_parse_role_mode(optarg, &opts.role_mode) != 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Invalid value for --role-mode: %s (expected auto or map)", optarg);
                }
                usage = 1;
            }
            break;
        case 1001:
            opts.role_map = optarg;
            break;
        case 1002:
            opts.trace = 1;
            break;
        case 'v':
            opts.verbose = 1;
            mfu_debug_level = MFU_LOG_VERBOSE;
            break;
        case 'q':
            opts.quiet = 1;
            mfu_debug_level = MFU_LOG_NONE;
            break;
        case 'h':
            usage = 1;
            help = 1;
            break;
        case '?':
        default:
            usage = 1;
            break;
        }
    }

    if (opts.role_mode == NSYNC_ROLE_MODE_MAP && opts.role_map == NULL) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "--role-map is required when --role-mode map is selected");
        }
        usage = 1;
    }

    int numargs = argc - optind;
    if (!help && numargs != 2) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "You must specify a source and destination path.");
        }
        usage = 1;
    }

    if (usage) {
        if (rank == 0) {
            nsync_usage();
        }
        rc = help ? 0 : 1;
        goto cleanup;
    }

    mfu_progress_timeout = opts.progress_secs;

    nsync_role_info_t role_info;
    nsync_role_info_init(&role_info);

    const char* src_path = argv[optind];
    const char* dst_path = argv[optind + 1];

    if (nsync_assign_roles(&opts, src_path, dst_path, &role_info) != 0) {
        rc = 1;
        nsync_role_info_free(&role_info);
        goto cleanup;
    }

    nsync_trace_local(&opts, "main-post-role-assign", (uint64_t)role_info.role, 0);

    uint64_t batch_count = 1;
    uint64_t global_src_items = 0;
    uint64_t global_dst_items = 0;
    uint64_t checkpoint_option_hash = 0;
    uint64_t start_batch = 0;
    int resumed_from_checkpoint = 0;
    int use_batch_spool = 0;
    int local_scan_errors_total = 0;
    if (opts.batch_files > 0) {
        int count_scan_errors = 0;
        if (nsync_compute_batch_count(
                &opts, &role_info, src_path, dst_path,
                &batch_count, &global_src_items, &global_dst_items, &count_scan_errors) != 0)
        {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to compute batching parameters");
            }
            nsync_role_info_free(&role_info);
            rc = 1;
            goto cleanup;
        }

        local_scan_errors_total += count_scan_errors;

        checkpoint_option_hash = nsync_batch_option_hash(&opts, src_path, dst_path);
        if (nsync_checkpoint_prepare_resume(
                &opts, &role_info, src_path, dst_path,
                checkpoint_option_hash, batch_count, &start_batch, &resumed_from_checkpoint) != 0)
        {
            nsync_role_info_free(&role_info);
            rc = 1;
            goto cleanup;
        }

        if (rank == 0) {
            MFU_LOG(MFU_LOG_INFO,
                "Batch mode enabled: batch-files=%" PRIu64
                " batch-count=%" PRIu64
                " src-items=%" PRIu64
                " dst-items=%" PRIu64,
                opts.batch_files, batch_count, global_src_items, global_dst_items);
            if (batch_count > 4096) {
                MFU_LOG(MFU_LOG_WARN,
                    "High batch-count=%" PRIu64
                    " may increase metadata scan overhead; increase --batch-files to reduce batch count.",
                    batch_count);
            }
        }

        if (batch_count > 1 && start_batch < batch_count) {
            if (nsync_batch_spool_prepare(&batch_spool, batch_count, start_batch, opts.contents ? 1 : 0) != 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to initialize batch spool state");
                }
                nsync_role_info_free(&role_info);
                rc = 1;
                goto cleanup;
            }

            int spool_scan_errors = 0;
            if (nsync_batch_spool_build(
                    &opts, &role_info, src_path, dst_path, &batch_spool, &spool_scan_errors) != 0)
            {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to build metadata spool");
                }
                nsync_role_info_free(&role_info);
                rc = 1;
                goto cleanup;
            }
            local_scan_errors_total += spool_scan_errors;

            uint64_t local_spool_arr[2] = {batch_spool.records_written, batch_spool.bytes_written};
            uint64_t global_spool_arr[2] = {0, 0};
            MPI_Allreduce(local_spool_arr, global_spool_arr, 2, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

            if (rank == 0 && !opts.quiet) {
                MFU_LOG(MFU_LOG_INFO,
                    "Batch spool prepared: records=%" PRIu64 " bytes=%" PRIu64 " start-batch=%" PRIu64,
                    global_spool_arr[0], global_spool_arr[1], start_batch);
            }

            use_batch_spool = 1;
        }
    }

    nsync_compare_counts_t local_counts_total;
    nsync_compare_counts_t global_counts;
    nsync_action_counts_t local_action_counts_total;
    nsync_action_counts_t global_action_counts;
    nsync_action_vec_t deferred_dir_removes;
    nsync_action_vec_t deferred_dir_meta_updates;
    nsync_compare_counts_init(&local_counts_total);
    nsync_action_counts_init(&local_action_counts_total);
    nsync_action_vec_init(&deferred_dir_removes);
    nsync_action_vec_init(&deferred_dir_meta_updates);

    uint64_t local_exec_errors_total = 0;

    for (uint64_t batch_id = start_batch; batch_id < batch_count; batch_id++) {
        nsync_meta_vec_t local_meta;
        nsync_meta_vec_t planner_meta;
        nsync_action_vec_t planned_actions;
        nsync_action_vec_t src_exec_actions;
        nsync_action_vec_t dst_exec_actions;
        nsync_meta_vec_init(&local_meta);
        nsync_meta_vec_init(&planner_meta);
        nsync_action_vec_init(&planned_actions);
        nsync_action_vec_init(&src_exec_actions);
        nsync_action_vec_init(&dst_exec_actions);

        int batch_scan_errors = 0;
        int local_exec_errors_batch = 0;
        int global_has_meta = 0;
        if (use_batch_spool) {
            int local_has_meta = batch_spool.batch_has_data[(size_t)batch_id] ? 1 : 0;
            MPI_Allreduce(&local_has_meta, &global_has_meta, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

            if (global_has_meta && local_has_meta) {
                if (nsync_batch_spool_load_batch(&batch_spool, batch_id, &local_meta) != 0) {
                    batch_scan_errors = 1;
                }
            }

            if (nsync_sync_error_point(&opts, "main-post-spool-load", batch_scan_errors) != 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to load metadata spool for batch %" PRIu64, batch_id);
                }
                nsync_action_vec_free(&dst_exec_actions);
                nsync_action_vec_free(&src_exec_actions);
                nsync_action_vec_free(&planned_actions);
                nsync_meta_vec_free(&planner_meta);
                nsync_meta_vec_free(&local_meta);
                nsync_role_info_free(&role_info);
                nsync_action_vec_free(&deferred_dir_removes);
                nsync_action_vec_free(&deferred_dir_meta_updates);
                rc = 1;
                goto cleanup;
            }
        } else {
            nsync_scan_filter_t filter = {
                .enabled = (opts.batch_files > 0 && batch_count > 1) ? 1 : 0,
                .batch_count = batch_count,
                .batch_id = batch_id,
            };
            const nsync_scan_filter_t* filter_ptr = filter.enabled ? &filter : NULL;

            nsync_scan_role_path_filtered(
                &opts, &role_info, src_path, dst_path, &local_meta, &batch_scan_errors, filter_ptr, NULL, 0, NULL, NULL);
            local_scan_errors_total += batch_scan_errors;

            int local_has_meta = (local_meta.size > 0) ? 1 : 0;
            MPI_Allreduce(&local_has_meta, &global_has_meta, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
        }

        nsync_trace_local(&opts, "main-post-scan", local_meta.size, (uint64_t)batch_scan_errors);
        if (!global_has_meta) {
            uint64_t local_batch_errors = (uint64_t)batch_scan_errors;
            uint64_t global_batch_errors = 0;
            MPI_Allreduce(&local_batch_errors, &global_batch_errors, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            if (!opts.dryrun && opts.batch_files > 0 && global_batch_errors == 0) {
                (void)nsync_checkpoint_mark_batch_complete(
                    &opts, &role_info, src_path, dst_path,
                    checkpoint_option_hash, batch_count, batch_id);
            }
            nsync_action_vec_free(&dst_exec_actions);
            nsync_action_vec_free(&src_exec_actions);
            nsync_action_vec_free(&planned_actions);
            nsync_meta_vec_free(&planner_meta);
            nsync_meta_vec_free(&local_meta);
            continue;
        }

        if (nsync_metadata_redistribute(&local_meta, &planner_meta, &opts) != 0) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to redistribute metadata records in batch %" PRIu64, batch_id);
            }
            nsync_action_vec_free(&dst_exec_actions);
            nsync_action_vec_free(&src_exec_actions);
            nsync_action_vec_free(&planned_actions);
            nsync_meta_vec_free(&planner_meta);
            nsync_meta_vec_free(&local_meta);
            nsync_role_info_free(&role_info);
            nsync_action_vec_free(&deferred_dir_removes);
            nsync_action_vec_free(&deferred_dir_meta_updates);
            rc = 1;
            goto cleanup;
        }
        nsync_trace_local(&opts, "main-post-redistribute", planner_meta.size, batch_id);

        nsync_compare_counts_t batch_counts;
        nsync_action_counts_t batch_action_counts;
        int local_plan_error = 0;
        if (nsync_plan_planner_records(
                &planner_meta, &opts, &role_info,
                &batch_counts, &planned_actions, &batch_action_counts) != 0)
        {
            local_plan_error = 1;
        }
        nsync_trace_local(&opts, "main-post-plan-local", planned_actions.size, (uint64_t)local_plan_error);

        int global_plan_error = 0;
        MPI_Allreduce(&local_plan_error, &global_plan_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
        if (global_plan_error != 0) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to generate planner actions in batch %" PRIu64, batch_id);
            }
            nsync_action_vec_free(&dst_exec_actions);
            nsync_action_vec_free(&src_exec_actions);
            nsync_action_vec_free(&planned_actions);
            nsync_meta_vec_free(&planner_meta);
            nsync_meta_vec_free(&local_meta);
            nsync_role_info_free(&role_info);
            nsync_action_vec_free(&deferred_dir_removes);
            nsync_action_vec_free(&deferred_dir_meta_updates);
            rc = 1;
            goto cleanup;
        }

        nsync_compare_counts_add(&local_counts_total, &batch_counts);
        nsync_action_counts_add(&local_action_counts_total, &batch_action_counts);

        int local_has_actions = (planned_actions.size > 0) ? 1 : 0;
        int global_has_actions = 0;
        MPI_Allreduce(&local_has_actions, &global_has_actions, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

        if (!opts.dryrun && global_has_actions) {
            int local_exec_error = 0;
            int src_redist_rc = nsync_actions_redistribute(&planned_actions, 1, &src_exec_actions, &opts);
            int dst_redist_rc = nsync_actions_redistribute(&planned_actions, 0, &dst_exec_actions, &opts);
            if (src_redist_rc != 0 || dst_redist_rc != 0) {
                local_exec_error = 1;
            }

            if (nsync_sync_error_point(&opts, "main-post-action-redistribute", local_exec_error) != 0) {
                if (rank == 0) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to redistribute execution actions in batch %" PRIu64, batch_id);
                }
                nsync_action_vec_free(&dst_exec_actions);
                nsync_action_vec_free(&src_exec_actions);
                nsync_action_vec_free(&planned_actions);
                nsync_meta_vec_free(&planner_meta);
                nsync_meta_vec_free(&local_meta);
                nsync_role_info_free(&role_info);
                nsync_action_vec_free(&deferred_dir_removes);
                nsync_action_vec_free(&deferred_dir_meta_updates);
                rc = 1;
                goto cleanup;
            }

            nsync_execute_actions_phase4(
                &opts,
                &role_info,
                src_path,
                dst_path,
                &src_exec_actions,
                &dst_exec_actions,
                &deferred_dir_removes,
                &deferred_dir_meta_updates,
                &local_exec_errors_batch);
            nsync_trace_local(&opts, "main-post-phase4-exec", (uint64_t)local_exec_errors_batch, batch_id);

            local_exec_errors_total += (uint64_t)local_exec_errors_batch;
        }

        uint64_t local_batch_errors = (uint64_t)batch_scan_errors + (uint64_t)local_exec_errors_batch;
        uint64_t global_batch_errors = 0;
        MPI_Allreduce(&local_batch_errors, &global_batch_errors, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        if (!opts.dryrun && opts.batch_files > 0 && global_batch_errors == 0) {
            (void)nsync_checkpoint_mark_batch_complete(
                &opts, &role_info, src_path, dst_path,
                checkpoint_option_hash, batch_count, batch_id);
        }

        if (rank == 0 && batch_count > 1 && !opts.quiet) {
            MFU_LOG(MFU_LOG_INFO,
                "Completed batch %" PRIu64 "/%" PRIu64 " (local-meta=%" PRIu64 " local-actions=%" PRIu64 ")",
                batch_id + 1, batch_count, local_meta.size, planned_actions.size);
        }

        nsync_action_vec_free(&dst_exec_actions);
        nsync_action_vec_free(&src_exec_actions);
        nsync_action_vec_free(&planned_actions);
        nsync_meta_vec_free(&planner_meta);
        nsync_meta_vec_free(&local_meta);
    }

    if (use_batch_spool) {
        nsync_batch_spool_cleanup(&batch_spool);
        use_batch_spool = 0;
    }

    if (!opts.dryrun && resumed_from_checkpoint) {
        if (nsync_reconcile_directory_metadata_after_resume(
                &opts, &role_info, src_path, dst_path,
                &deferred_dir_removes, &deferred_dir_meta_updates,
                &local_scan_errors_total, &local_exec_errors_total) != 0)
        {
            nsync_role_info_free(&role_info);
            nsync_action_vec_free(&deferred_dir_removes);
            nsync_action_vec_free(&deferred_dir_meta_updates);
            rc = 1;
            goto cleanup;
        }
    }

    if (!opts.dryrun && opts.batch_files > 0 && opts.delete && role_info.role == NSYNC_ROLE_DST) {
        int local_finalize_errors = 0;
        nsync_finalize_deferred_dir_removes(dst_path, &deferred_dir_removes, &local_finalize_errors);
        local_exec_errors_total += (uint64_t)local_finalize_errors;
    }

    if (!opts.dryrun && role_info.role == NSYNC_ROLE_DST) {
        int local_finalize_errors = 0;
        nsync_finalize_deferred_dir_meta_updates(
            dst_path, &opts, &deferred_dir_meta_updates, &local_finalize_errors);
        local_exec_errors_total += (uint64_t)local_finalize_errors;
    }

    uint64_t local_compare_arr[4];
    uint64_t global_compare_arr[4];
    nsync_compare_counts_to_array(&local_counts_total, local_compare_arr);
    nsync_trace_local(
        &opts, "main-pre-compare-allreduce",
        local_compare_arr[0] + local_compare_arr[1],
        local_compare_arr[2] + local_compare_arr[3]);
    MPI_Allreduce(local_compare_arr, global_compare_arr, 4, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    nsync_compare_counts_from_array(global_compare_arr, &global_counts);

    uint64_t local_action_arr[6];
    uint64_t global_action_arr[6];
    nsync_action_counts_to_array(&local_action_counts_total, local_action_arr);
    nsync_trace_local(
        &opts, "main-pre-action-allreduce",
        local_action_arr[0] + local_action_arr[1] + local_action_arr[2],
        local_action_arr[3] + local_action_arr[4] + local_action_arr[5]);
    MPI_Allreduce(local_action_arr, global_action_arr, 6, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    nsync_action_counts_from_array(global_action_arr, &global_action_counts);

    int global_scan_errors = 0;
    nsync_trace_local(&opts, "main-pre-scanerr-allreduce", (uint64_t)local_scan_errors_total, 0);
    MPI_Allreduce(&local_scan_errors_total, &global_scan_errors, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    uint64_t global_exec_errors_pre_checkpoint = 0;
    MPI_Allreduce(
        &local_exec_errors_total, &global_exec_errors_pre_checkpoint, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    if (!opts.dryrun && global_scan_errors == 0 && global_exec_errors_pre_checkpoint == 0) {
        if (opts.batch_files > 0 && batch_count > 1) {
            (void)nsync_checkpoint_mark_finalized(
                &opts, &role_info, src_path, dst_path, checkpoint_option_hash, batch_count);
            nsync_checkpoint_clear(&opts, &role_info, dst_path);
        }

        (void)nsync_restore_destination_root_metadata(
            &opts, &role_info, src_path, dst_path, &local_exec_errors_total);
    }

    uint64_t global_exec_errors = 0;
    MPI_Allreduce(
        &local_exec_errors_total, &global_exec_errors, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    if (rank == 0) {
        const char* role_mode = opts.role_mode == NSYNC_ROLE_MODE_AUTO ? "auto" : "map";
        MFU_LOG(MFU_LOG_INFO, "nsync Phase 5 planner%s complete", opts.dryrun ? " dryrun" : "+execute");
        MFU_LOG(MFU_LOG_INFO, "Requested source: %s", src_path);
        MFU_LOG(MFU_LOG_INFO, "Requested target: %s", dst_path);
        MFU_LOG(MFU_LOG_INFO,
            "Options: dryrun=%d batch-files=%" PRIu64 " delete=%d contents=%d role-mode=%s",
            opts.dryrun, opts.batch_files, opts.delete, opts.contents, role_mode);
        if (opts.role_map != NULL) {
            MFU_LOG(MFU_LOG_INFO, "Role map: %s", opts.role_map);
        }
        if (opts.batch_files > 0) {
            MFU_LOG(MFU_LOG_INFO,
                "Batch summary: batches=%" PRIu64 " src-items=%" PRIu64 " dst-items=%" PRIu64,
                batch_count, global_src_items, global_dst_items);
        }

        MFU_LOG(MFU_LOG_INFO,
            "Metadata diff summary: only-src=%" PRIu64 " only-dst=%" PRIu64
            " common=%" PRIu64 " changed=%" PRIu64,
            global_counts.only_src, global_counts.only_dst,
            global_counts.common, global_counts.changed);

        MFU_LOG(MFU_LOG_INFO,
            "Planned actions: copy=%" PRIu64 " mkdir=%" PRIu64
            " symlink-update=%" PRIu64 " meta-update=%" PRIu64
            " remove=%" PRIu64 " skipped-dst-only=%" PRIu64,
            global_action_counts.copy,
            global_action_counts.mkdir,
            global_action_counts.symlink_update,
            global_action_counts.meta_update,
            global_action_counts.remove,
            global_action_counts.skipped_only_dst);

        if (global_scan_errors > 0) {
            MFU_LOG(MFU_LOG_ERR, "Encountered %d scan error(s)", global_scan_errors);
        }

        if (opts.dryrun) {
            MFU_LOG(MFU_LOG_INFO, "Dryrun enabled: planner generated actions but no filesystem changes were made.");
        }

        if (!opts.dryrun) {
            if (global_exec_errors > 0) {
                MFU_LOG(MFU_LOG_ERR, "Execution completed with %" PRIu64 " error(s)", global_exec_errors);
            } else {
                MFU_LOG(MFU_LOG_INFO, "Execution completed successfully");
            }
        }
    }

    nsync_action_vec_free(&deferred_dir_removes);
    nsync_action_vec_free(&deferred_dir_meta_updates);
    nsync_role_info_free(&role_info);

    if (global_scan_errors > 0 || global_exec_errors > 0) {
        rc = 1;
    }

cleanup:
    nsync_batch_spool_cleanup(&batch_spool);
    mfu_finalize();
    MPI_Finalize();
    return rc;
}
