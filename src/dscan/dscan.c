/* dscan - MPI-based directory scan and report tool.
 *
 * Scalable design notes:
 * - streaming distributed walk: directories are owned by hash and scanned one
 *   frontier round at a time (same pattern as nsync's role scan), entries are
 *   folded into fixed-size local accumulators as they are discovered, so no
 *   rank ever holds the full listing in memory
 * - global statistics (summary counters and histograms) are combined with a
 *   single MPI_Reduce of fixed-size arrays instead of gathering all records
 * - oldest top-K is kept as a bounded per-rank heap and only K entries per
 *   rank travel to rank 0 for the final merge
 * - broken path detection uses the lstat data collected during the walk (no
 *   second lstat/open pass) and the reported list is capped by --broken-limit
 * - memory-bounded batch mode via --batch-files with per-batch progress
 */

#define _GNU_SOURCE

#include <dirent.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "mpi.h"
#include "mfu.h"

#define DSCAN_ABNORMAL_SIZE_BYTES (1ULL << 50) /* 1 PiB */
#define DSCAN_TEN_YEARS_SECONDS (10ULL * 365ULL * 24ULL * 3600ULL)

#define DSCAN_BROKEN_SIZE       (1ULL << 0)
#define DSCAN_BROKEN_MISSING    (1ULL << 1)
#define DSCAN_BROKEN_TIME       (1ULL << 2)
#define DSCAN_BROKEN_UNREADABLE (1ULL << 3)

/* per-round bounds that keep collective exchanges small and frequent */
#define DSCAN_ROUND_ENTRY_QUOTA_DEFAULT (1ULL << 20)
#define DSCAN_ROUND_TASK_BYTES_MAX (64ULL * 1024ULL * 1024ULL)

#define DSCAN_DEFAULT_BROKEN_LIMIT 10000ULL

static const uint64_t DSCAN_SIZE_LIMITS[] = {
    4ULL * 1024ULL,
    64ULL * 1024ULL,
    1ULL * 1024ULL * 1024ULL,
    16ULL * 1024ULL * 1024ULL,
    256ULL * 1024ULL * 1024ULL,
    1ULL * 1024ULL * 1024ULL * 1024ULL,
    16ULL * 1024ULL * 1024ULL * 1024ULL,
    256ULL * 1024ULL * 1024ULL * 1024ULL,
    4ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL,
};

static const uint64_t DSCAN_TIME_DAY_LIMITS[] = {
    1ULL,
    7ULL,
    30ULL,
    90ULL,
    180ULL,
    365ULL,
    1095ULL,
    3650ULL,
};

#define DSCAN_SIZE_BIN_COUNT ((sizeof(DSCAN_SIZE_LIMITS) / sizeof(DSCAN_SIZE_LIMITS[0])) + 1)
#define DSCAN_TIME_BIN_COUNT ((sizeof(DSCAN_TIME_DAY_LIMITS) / sizeof(DSCAN_TIME_DAY_LIMITS[0])) + 1)

typedef enum {
    DSCAN_TOP_ATIME = 0,
    DSCAN_TOP_MTIME,
    DSCAN_TOP_CTIME,
} dscan_top_field_t;

/* one scanned entry as kept in top-K heaps and the merged report */
typedef struct {
    char* path;
    uint64_t type;
    uint64_t size;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
} dscan_entry_t;

typedef struct {
    char* path;
    uint64_t flags;
} dscan_broken_t;

/* fixed-size wire record for the small top-K / broken gathers */
typedef struct {
    uint64_t type;
    uint64_t size;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
    uint64_t path_len; /* including NUL */
} dscan_wire_entry_t;

typedef struct {
    uint64_t flags;
    uint64_t path_len; /* including NUL */
} dscan_wire_broken_t;

/* bounded heap holding the K oldest candidates for one time field */
typedef struct {
    dscan_entry_t* entries;
    uint64_t count;
    uint64_t k;
    dscan_top_field_t field;
} dscan_topk_t;

/* growable list of directory paths pending scan on this rank (LIFO) */
typedef struct {
    char** paths;
    uint64_t count;
    uint64_t cap;
} dscan_dirq_t;

/* growable byte buffer for packed outgoing directory tasks */
typedef struct {
    char* buf;
    uint64_t size;
    uint64_t cap;
} dscan_buf_t;

/* all per-rank accumulators updated while streaming through entries */
typedef struct {
    uint64_t total_files;
    uint64_t total_dirs;
    uint64_t total_links;
    uint64_t total_other;
    uint64_t scan_errors;
    uint64_t broken_total;

    uint64_t size_hist[DSCAN_SIZE_BIN_COUNT];
    uint64_t atime_hist[DSCAN_TIME_BIN_COUNT];
    uint64_t mtime_hist[DSCAN_TIME_BIN_COUNT];
    uint64_t ctime_hist[DSCAN_TIME_BIN_COUNT];

    dscan_topk_t top_atime;
    dscan_topk_t top_mtime;
    dscan_topk_t top_ctime;

    dscan_broken_t* broken;
    uint64_t broken_count;
    uint64_t broken_limit;

    uint64_t now;
} dscan_acc_t;

/* ---------------------------------------------------------------------------
 * small helpers
 * ------------------------------------------------------------------------- */

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dscan [options]\n");
    printf("\n");
    printf("Required options:\n");
    printf("  -d, --directory <path>   - directory to scan\n");
    printf("  -o, --output <file>      - write JSON report file\n");
    printf("\n");
    printf("Optional:\n");
    printf("  -p, --print              - print pretty summary on stdout (rank 0)\n");
    printf("  -k, --top-k <N>          - oldest top-K per time field (default 10)\n");
    printf("  -b, --batch-files <N>    - report progress and account scanning in\n");
    printf("                             batches of approximately N entries\n");
    printf("      --broken-limit <N>   - max broken paths kept in the report\n");
    printf("                             (default %" PRIu64 ", total count is always exact)\n", (uint64_t)DSCAN_DEFAULT_BROKEN_LIMIT);
    printf("  -v, --verbose            - verbose output\n");
    printf("  -q, --quiet              - quiet output\n");
    printf("  -h, --help               - print usage\n");
    printf("\n");
    printf("For more information see dscan.md in this repository.\n");
    printf("\n");
    fflush(stdout);
}

static int parse_uint64(const char* text, uint64_t* value)
{
    errno = 0;
    char* end = NULL;
    unsigned long long tmp = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }

    *value = (uint64_t) tmp;
    return 0;
}

static const char* type_to_string(uint64_t type)
{
    if (type == MFU_TYPE_FILE) {
        return "file";
    }
    if (type == MFU_TYPE_DIR) {
        return "directory";
    }
    if (type == MFU_TYPE_LINK) {
        return "symlink";
    }
    return "other";
}

static uint64_t mode_to_type(mode_t mode)
{
    if (S_ISREG(mode)) {
        return (uint64_t) MFU_TYPE_FILE;
    }
    if (S_ISDIR(mode)) {
        return (uint64_t) MFU_TYPE_DIR;
    }
    if (S_ISLNK(mode)) {
        return (uint64_t) MFU_TYPE_LINK;
    }
    return (uint64_t) MFU_TYPE_UNKNOWN;
}

static uint64_t entry_field_value(const dscan_entry_t* entry, dscan_top_field_t field)
{
    if (field == DSCAN_TOP_ATIME) {
        return entry->atime;
    }
    if (field == DSCAN_TOP_MTIME) {
        return entry->mtime;
    }
    return entry->ctime;
}

/* ascending (field value, path) order shared by heaps and the final merge so
 * results are identical for any rank count */
static int entry_cmp_field(const dscan_entry_t* a, const dscan_entry_t* b, dscan_top_field_t field)
{
    uint64_t va = entry_field_value(a, field);
    uint64_t vb = entry_field_value(b, field);

    if (va < vb) {
        return -1;
    }
    if (va > vb) {
        return 1;
    }

    return strcmp(a->path, b->path);
}

static dscan_top_field_t g_sort_field = DSCAN_TOP_ATIME;

static int entry_qsort_cmp(const void* a, const void* b)
{
    return entry_cmp_field((const dscan_entry_t*) a, (const dscan_entry_t*) b, g_sort_field);
}

static int broken_qsort_cmp(const void* a, const void* b)
{
    const dscan_broken_t* ba = (const dscan_broken_t*) a;
    const dscan_broken_t* bb = (const dscan_broken_t*) b;
    return strcmp(ba->path, bb->path);
}

static uint64_t dscan_hash_path(const char* text)
{
    /* FNV-1a 64-bit */
    uint64_t hash = 1469598103934665603ULL;
    const unsigned char* ptr = (const unsigned char*) text;
    while (*ptr != '\0') {
        hash ^= (uint64_t) (*ptr);
        hash *= 1099511628211ULL;
        ptr++;
    }
    return hash;
}

static uint64_t size_hist_bin(uint64_t size)
{
    uint64_t nlimits = (uint64_t) (sizeof(DSCAN_SIZE_LIMITS) / sizeof(DSCAN_SIZE_LIMITS[0]));
    for (uint64_t i = 0; i < nlimits; i++) {
        if (size <= DSCAN_SIZE_LIMITS[i]) {
            return i;
        }
    }
    return nlimits;
}

static uint64_t time_hist_bin(uint64_t now, uint64_t timestamp)
{
    uint64_t age_days = 0;
    if (timestamp < now) {
        age_days = (now - timestamp) / (24ULL * 3600ULL);
    }

    uint64_t nlimits = (uint64_t) (sizeof(DSCAN_TIME_DAY_LIMITS) / sizeof(DSCAN_TIME_DAY_LIMITS[0]));
    for (uint64_t i = 0; i < nlimits; i++) {
        if (age_days <= DSCAN_TIME_DAY_LIMITS[i]) {
            return i;
        }
    }
    return nlimits;
}

static void format_epoch(uint64_t ts, char* buf, size_t buf_size)
{
    time_t t = (time_t) ts;
    struct tm tmval;

    if (localtime_r(&t, &tmval) == NULL) {
        snprintf(buf, buf_size, "-");
        return;
    }

    if (strftime(buf, buf_size, "%Y-%m-%d %H:%M:%S", &tmval) == 0) {
        snprintf(buf, buf_size, "-");
    }
}

/* ---------------------------------------------------------------------------
 * top-K heap (max-heap over the shared ordering, root = worst kept entry)
 * ------------------------------------------------------------------------- */

static void topk_init(dscan_topk_t* topk, uint64_t k, dscan_top_field_t field)
{
    topk->entries = (k > 0) ? (dscan_entry_t*) MFU_MALLOC((size_t)k * sizeof(dscan_entry_t)) : NULL;
    topk->count = 0;
    topk->k = k;
    topk->field = field;
}

static void topk_free(dscan_topk_t* topk)
{
    for (uint64_t i = 0; i < topk->count; i++) {
        mfu_free(&topk->entries[i].path);
    }
    mfu_free(&topk->entries);
    topk->count = 0;
}

static void topk_sift_up(dscan_topk_t* topk, uint64_t idx)
{
    while (idx > 0) {
        uint64_t parent = (idx - 1) / 2;
        if (entry_cmp_field(&topk->entries[idx], &topk->entries[parent], topk->field) <= 0) {
            break;
        }
        dscan_entry_t tmp = topk->entries[idx];
        topk->entries[idx] = topk->entries[parent];
        topk->entries[parent] = tmp;
        idx = parent;
    }
}

static void topk_sift_down(dscan_topk_t* topk, uint64_t idx)
{
    while (1) {
        uint64_t left = 2 * idx + 1;
        uint64_t right = 2 * idx + 2;
        uint64_t largest = idx;

        if (left < topk->count &&
            entry_cmp_field(&topk->entries[left], &topk->entries[largest], topk->field) > 0)
        {
            largest = left;
        }
        if (right < topk->count &&
            entry_cmp_field(&topk->entries[right], &topk->entries[largest], topk->field) > 0)
        {
            largest = right;
        }
        if (largest == idx) {
            break;
        }

        dscan_entry_t tmp = topk->entries[idx];
        topk->entries[idx] = topk->entries[largest];
        topk->entries[largest] = tmp;
        idx = largest;
    }
}

static void topk_insert(
    dscan_topk_t* topk,
    const char* path,
    uint64_t type,
    uint64_t size,
    uint64_t atime,
    uint64_t mtime,
    uint64_t ctime)
{
    if (topk->k == 0) {
        return;
    }

    dscan_entry_t candidate;
    candidate.path = (char*) path; /* borrowed for comparison only */
    candidate.type = type;
    candidate.size = size;
    candidate.atime = atime;
    candidate.mtime = mtime;
    candidate.ctime = ctime;

    if (topk->count < topk->k) {
        dscan_entry_t* slot = &topk->entries[topk->count];
        *slot = candidate;
        slot->path = MFU_STRDUP(path);
        topk->count++;
        topk_sift_up(topk, topk->count - 1);
        return;
    }

    if (entry_cmp_field(&candidate, &topk->entries[0], topk->field) < 0) {
        mfu_free(&topk->entries[0].path);
        topk->entries[0] = candidate;
        topk->entries[0].path = MFU_STRDUP(path);
        topk_sift_down(topk, 0);
    }
}

/* ---------------------------------------------------------------------------
 * directory queue and outgoing task buffers
 * ------------------------------------------------------------------------- */

static void dirq_init(dscan_dirq_t* q)
{
    q->paths = NULL;
    q->count = 0;
    q->cap = 0;
}

static void dirq_push_take(dscan_dirq_t* q, char* path)
{
    if (q->count == q->cap) {
        uint64_t new_cap = (q->cap == 0) ? 64 : q->cap * 2;
        char** grown = (char**) MFU_MALLOC((size_t)new_cap * sizeof(char*));
        if (q->count > 0) {
            memcpy(grown, q->paths, (size_t)q->count * sizeof(char*));
        }
        mfu_free(&q->paths);
        q->paths = grown;
        q->cap = new_cap;
    }
    q->paths[q->count++] = path;
}

static char* dirq_pop(dscan_dirq_t* q)
{
    if (q->count == 0) {
        return NULL;
    }
    q->count--;
    return q->paths[q->count];
}

static void dirq_free(dscan_dirq_t* q)
{
    for (uint64_t i = 0; i < q->count; i++) {
        mfu_free(&q->paths[i]);
    }
    mfu_free(&q->paths);
    q->count = 0;
    q->cap = 0;
}

static void buf_init(dscan_buf_t* b)
{
    b->buf = NULL;
    b->size = 0;
    b->cap = 0;
}

static void buf_reserve(dscan_buf_t* b, uint64_t need)
{
    if (b->size + need <= b->cap) {
        return;
    }
    uint64_t new_cap = (b->cap == 0) ? 4096 : b->cap;
    while (b->size + need > new_cap) {
        new_cap *= 2;
    }
    char* grown = (char*) MFU_MALLOC((size_t)new_cap);
    if (b->size > 0) {
        memcpy(grown, b->buf, (size_t)b->size);
    }
    mfu_free(&b->buf);
    b->buf = grown;
    b->cap = new_cap;
}

static void buf_append_path(dscan_buf_t* b, const char* path)
{
    uint32_t len = (uint32_t) strlen(path);
    buf_reserve(b, sizeof(uint32_t) + (uint64_t)len);
    memcpy(b->buf + b->size, &len, sizeof(uint32_t));
    b->size += sizeof(uint32_t);
    memcpy(b->buf + b->size, path, len);
    b->size += len;
}

static void buf_reset(dscan_buf_t* b)
{
    b->size = 0;
}

static void buf_free(dscan_buf_t* b)
{
    mfu_free(&b->buf);
    b->size = 0;
    b->cap = 0;
}

/* ---------------------------------------------------------------------------
 * streaming accumulation
 * ------------------------------------------------------------------------- */

static void acc_init(dscan_acc_t* acc, uint64_t top_k, uint64_t broken_limit, uint64_t now)
{
    memset(acc, 0, sizeof(*acc));

    topk_init(&acc->top_atime, top_k, DSCAN_TOP_ATIME);
    topk_init(&acc->top_mtime, top_k, DSCAN_TOP_MTIME);
    topk_init(&acc->top_ctime, top_k, DSCAN_TOP_CTIME);

    acc->broken = (broken_limit > 0) ? (dscan_broken_t*) MFU_MALLOC((size_t)broken_limit * sizeof(dscan_broken_t)) : NULL;
    acc->broken_count = 0;
    acc->broken_limit = broken_limit;

    acc->now = now;
}

static void acc_free(dscan_acc_t* acc)
{
    topk_free(&acc->top_atime);
    topk_free(&acc->top_mtime);
    topk_free(&acc->top_ctime);

    for (uint64_t i = 0; i < acc->broken_count; i++) {
        mfu_free(&acc->broken[i].path);
    }
    mfu_free(&acc->broken);
    acc->broken_count = 0;
}

static void acc_add_broken(dscan_acc_t* acc, const char* path, uint64_t flags)
{
    acc->broken_total++;
    if (acc->broken_count < acc->broken_limit) {
        acc->broken[acc->broken_count].path = MFU_STRDUP(path);
        acc->broken[acc->broken_count].flags = flags;
        acc->broken_count++;
    }
}

/* brokenness derived from the walk's lstat data only -- no extra syscalls */
static uint64_t stat_broken_flags(const dscan_acc_t* acc, uint64_t type, uint64_t size, uint64_t atime, uint64_t mtime, uint64_t ctime)
{
    uint64_t flags = 0;

    if (type == MFU_TYPE_FILE && size > DSCAN_ABNORMAL_SIZE_BYTES) {
        flags |= DSCAN_BROKEN_SIZE;
    }

    uint64_t now = acc->now;
    uint64_t past_limit = (now > DSCAN_TEN_YEARS_SECONDS) ? (now - DSCAN_TEN_YEARS_SECONDS) : 0ULL;
    if (atime < past_limit || atime > now ||
        mtime < past_limit || mtime > now ||
        ctime < past_limit || ctime > now)
    {
        flags |= DSCAN_BROKEN_TIME;
    }

    return flags;
}

static void acc_emit_entry(dscan_acc_t* acc, const char* path, const struct stat* st)
{
    uint64_t type = mode_to_type(st->st_mode);
    uint64_t size = (uint64_t) st->st_size;
    uint64_t atime = (uint64_t) st->st_atime;
    uint64_t mtime = (uint64_t) st->st_mtime;
    uint64_t ctime = (uint64_t) st->st_ctime;

    if (type == MFU_TYPE_FILE) {
        acc->total_files++;
        acc->size_hist[size_hist_bin(size)]++;

        /* time histograms accumulate file capacity (bytes), files only --
         * directories hold no file data and are excluded */
        acc->atime_hist[time_hist_bin(acc->now, atime)] += size;
        acc->mtime_hist[time_hist_bin(acc->now, mtime)] += size;
        acc->ctime_hist[time_hist_bin(acc->now, ctime)] += size;
    } else if (type == MFU_TYPE_DIR) {
        acc->total_dirs++;
    } else if (type == MFU_TYPE_LINK) {
        acc->total_links++;
    } else {
        acc->total_other++;
    }

    /* candidate set for the "oldest by" top-K spans files and directories */
    if (type == MFU_TYPE_FILE || type == MFU_TYPE_DIR) {
        topk_insert(&acc->top_atime, path, type, size, atime, mtime, ctime);
        topk_insert(&acc->top_mtime, path, type, size, atime, mtime, ctime);
        topk_insert(&acc->top_ctime, path, type, size, atime, mtime, ctime);
    }

    uint64_t flags = stat_broken_flags(acc, type, size, atime, mtime, ctime);
    if (flags != 0) {
        acc_add_broken(acc, path, flags);
    }
}

/* ---------------------------------------------------------------------------
 * distributed streaming walk with batch accounting
 * ------------------------------------------------------------------------- */

typedef struct {
    int rank;
    int ranks;
    uint64_t batch_files;
    uint64_t round_quota;
} dscan_walk_opts_t;

static char* build_child_path(const char* parent, const char* name)
{
    size_t parent_len = strlen(parent);
    int parent_is_rootfs = (parent_len == 1 && parent[0] == '/');
    size_t name_len = strlen(name);
    size_t total = parent_len + 1 + name_len + 1;

    char* path = (char*) MFU_MALLOC(total);
    if (parent_is_rootfs) {
        snprintf(path, total, "/%s", name);
    } else {
        snprintf(path, total, "%s/%s", parent, name);
    }
    return path;
}

/* scan one directory, streaming children into the accumulators and routing
 * child directories to their owner rank */
static void walk_scan_directory(
    const dscan_walk_opts_t* wopts,
    dscan_acc_t* acc,
    const char* dir_path,
    dscan_dirq_t* queue,
    dscan_buf_t* outbufs,
    uint64_t* scanned,
    uint64_t* out_bytes)
{
    DIR* dir = opendir(dir_path);
    if (dir == NULL) {
        acc->scan_errors++;
        acc_add_broken(acc, dir_path, DSCAN_BROKEN_UNREADABLE);
        return;
    }

    struct dirent* dent;
    while ((dent = readdir(dir)) != NULL) {
        const char* name = dent->d_name;
        if ((strcmp(name, ".") == 0) || (strcmp(name, "..") == 0)) {
            continue;
        }

        char* child = build_child_path(dir_path, name);

        struct stat st;
        if (lstat(child, &st) != 0) {
            acc->scan_errors++;
            uint64_t flags = (errno == ENOENT || errno == ENOTDIR) ? DSCAN_BROKEN_MISSING : DSCAN_BROKEN_UNREADABLE;
            acc_add_broken(acc, child, flags);
            mfu_free(&child);
            continue;
        }

        (*scanned)++;
        acc_emit_entry(acc, child, &st);

        if (S_ISDIR(st.st_mode)) {
            int owner = (int) (dscan_hash_path(child) % (uint64_t)wopts->ranks);
            if (owner == wopts->rank) {
                dirq_push_take(queue, child);
                child = NULL;
            } else {
                buf_append_path(&outbufs[owner], child);
                *out_bytes += sizeof(uint32_t) + strlen(child);
            }
        }

        mfu_free(&child);
    }

    closedir(dir);
}

/* exchange packed directory tasks; received tasks are appended to queue.
 * returns 0 on success, non-zero on a local fatal error. */
static int walk_exchange_tasks(
    const dscan_walk_opts_t* wopts,
    dscan_buf_t* outbufs,
    dscan_dirq_t* queue)
{
    int ranks = wopts->ranks;

    uint64_t* send_bytes = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t* recv_bytes = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    for (int r = 0; r < ranks; r++) {
        send_bytes[r] = outbufs[r].size;
    }

    MPI_Alltoall(send_bytes, 1, MPI_UINT64_T, recv_bytes, 1, MPI_UINT64_T, MPI_COMM_WORLD);

    int fatal = 0;
    uint64_t send_total = 0;
    uint64_t recv_total = 0;
    for (int r = 0; r < ranks; r++) {
        send_total += send_bytes[r];
        recv_total += recv_bytes[r];
    }
    if (send_total > (uint64_t)INT_MAX || recv_total > (uint64_t)INT_MAX) {
        /* cannot happen with the per-round byte cap, but guard anyway */
        fatal = 1;
    }

    int global_fatal = 0;
    MPI_Allreduce(&fatal, &global_fatal, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_fatal != 0) {
        mfu_free(&send_bytes);
        mfu_free(&recv_bytes);
        return 1;
    }

    int* send_counts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_counts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* send_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

    char* send_buf = (send_total > 0) ? (char*) MFU_MALLOC((size_t)send_total) : NULL;
    char* recv_buf = (recv_total > 0) ? (char*) MFU_MALLOC((size_t)recv_total) : NULL;

    uint64_t send_off = 0;
    uint64_t recv_off = 0;
    for (int r = 0; r < ranks; r++) {
        send_counts[r] = (int) send_bytes[r];
        recv_counts[r] = (int) recv_bytes[r];
        send_displs[r] = (int) send_off;
        recv_displs[r] = (int) recv_off;

        if (send_bytes[r] > 0) {
            memcpy(send_buf + send_off, outbufs[r].buf, (size_t)send_bytes[r]);
        }
        send_off += send_bytes[r];
        recv_off += recv_bytes[r];

        buf_reset(&outbufs[r]);
    }

    MPI_Alltoallv(
        send_buf, send_counts, send_displs, MPI_BYTE,
        recv_buf, recv_counts, recv_displs, MPI_BYTE,
        MPI_COMM_WORLD);

    int rc = 0;
    uint64_t off = 0;
    while (off + sizeof(uint32_t) <= recv_total) {
        uint32_t len;
        memcpy(&len, recv_buf + off, sizeof(uint32_t));
        off += sizeof(uint32_t);

        if (off + (uint64_t)len > recv_total) {
            rc = 1;
            break;
        }

        char* path = (char*) MFU_MALLOC((size_t)len + 1);
        memcpy(path, recv_buf + off, len);
        path[len] = '\0';
        off += len;

        dirq_push_take(queue, path);
    }
    if (rc == 0 && off != recv_total) {
        rc = 1;
    }

    mfu_free(&send_buf);
    mfu_free(&recv_buf);
    mfu_free(&send_counts);
    mfu_free(&recv_counts);
    mfu_free(&send_displs);
    mfu_free(&recv_displs);
    mfu_free(&send_bytes);
    mfu_free(&recv_bytes);

    return rc;
}

/* run the distributed walk; every rank must call this (collective).
 * returns 0 on success. */
static int walk_directory_streaming(
    const dscan_walk_opts_t* wopts,
    dscan_acc_t* acc,
    const char* root,
    uint64_t* out_total_scanned,
    uint64_t* out_batches)
{
    dscan_dirq_t queue;
    dirq_init(&queue);

    dscan_buf_t* outbufs = (dscan_buf_t*) MFU_MALLOC((size_t)wopts->ranks * sizeof(dscan_buf_t));
    for (int r = 0; r < wopts->ranks; r++) {
        buf_init(&outbufs[r]);
    }

    /* the owner of the root path emits it and seeds its queue; the root was
     * already validated as a directory on every rank */
    int root_owner = (int) (dscan_hash_path(root) % (uint64_t)wopts->ranks);
    uint64_t local_scanned_round = 0;

    if (wopts->rank == root_owner) {
        struct stat st;
        if (lstat(root, &st) == 0) {
            local_scanned_round++;
            acc_emit_entry(acc, root, &st);
            dirq_push_take(&queue, MFU_STRDUP(root));
        } else {
            acc->scan_errors++;
            acc_add_broken(acc, root, DSCAN_BROKEN_UNREADABLE);
        }
    }

    double t_start = MPI_Wtime();
    double t_batch = t_start;

    uint64_t total_scanned = 0;
    uint64_t batch_scanned = 0;
    uint64_t batch_id = 0;
    int rc = 0;

    while (1) {
        /* scan directories until the round quota or task-byte cap is hit */
        uint64_t out_bytes = 0;
        while (queue.count > 0 &&
               local_scanned_round < wopts->round_quota &&
               out_bytes < DSCAN_ROUND_TASK_BYTES_MAX)
        {
            char* dir_path = dirq_pop(&queue);
            walk_scan_directory(wopts, acc, dir_path, &queue, outbufs, &local_scanned_round, &out_bytes);
            mfu_free(&dir_path);
        }

        if (walk_exchange_tasks(wopts, outbufs, &queue) != 0) {
            rc = 1;
        }

        uint64_t local_stats[3];
        uint64_t global_stats[3];
        local_stats[0] = local_scanned_round;
        local_stats[1] = queue.count;
        local_stats[2] = (uint64_t) rc;
        MPI_Allreduce(local_stats, global_stats, 3, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

        local_scanned_round = 0;

        if (global_stats[2] != 0) {
            rc = 1;
            break;
        }

        total_scanned += global_stats[0];
        batch_scanned += global_stats[0];

        if (wopts->batch_files > 0) {
            while (batch_scanned >= wopts->batch_files) {
                batch_scanned -= wopts->batch_files;
                batch_id++;
                if (wopts->rank == 0 && mfu_debug_level >= MFU_LOG_INFO) {
                    double t_now = MPI_Wtime();
                    double elapsed = t_now - t_start;
                    double rate = (elapsed > 0.0) ? ((double) total_scanned / elapsed) : 0.0;
                    MFU_LOG(MFU_LOG_INFO,
                        "[batch %" PRIu64 "] scanned %" PRIu64 " entries total "
                        "(%.0f entries/s, %" PRIu64 " dirs queued, batch took %.2fs)",
                        batch_id, total_scanned, rate, global_stats[1], t_now - t_batch);
                    t_batch = t_now;
                }
            }
        }

        if (global_stats[1] == 0) {
            break;
        }
    }

    if (wopts->rank == 0 && rc == 0 && mfu_debug_level >= MFU_LOG_INFO) {
        double elapsed = MPI_Wtime() - t_start;
        double rate = (elapsed > 0.0) ? ((double) total_scanned / elapsed) : 0.0;
        MFU_LOG(MFU_LOG_INFO,
            "walk complete: %" PRIu64 " entries in %.2fs (%.0f entries/s)",
            total_scanned, elapsed, rate);
    }

    for (int r = 0; r < wopts->ranks; r++) {
        buf_free(&outbufs[r]);
    }
    mfu_free(&outbufs);
    dirq_free(&queue);

    *out_total_scanned = total_scanned;
    *out_batches = (wopts->batch_files > 0) ? batch_id + ((batch_scanned > 0) ? 1 : 0) : 1;

    return rc;
}

/* ---------------------------------------------------------------------------
 * merge phase: reduce histograms, gather small top-K / broken samples
 * ------------------------------------------------------------------------- */

/* gather at most K entries from every rank to rank 0; on rank 0 returns a
 * merged array (paths owned by the array) sorted by the shared ordering and
 * truncated to K */
static dscan_entry_t* gather_and_merge_topk(
    const dscan_topk_t* topk,
    int rank,
    int ranks,
    uint64_t k,
    uint64_t* out_count,
    int* out_rc)
{
    *out_count = 0;

    uint64_t local_count = topk->count;
    uint64_t local_path_bytes = 0;
    for (uint64_t i = 0; i < local_count; i++) {
        local_path_bytes += strlen(topk->entries[i].path) + 1;
    }

    dscan_wire_entry_t* local_wires = (local_count > 0) ?
        (dscan_wire_entry_t*) MFU_MALLOC((size_t)local_count * sizeof(dscan_wire_entry_t)) : NULL;
    char* local_paths = (local_path_bytes > 0) ? (char*) MFU_MALLOC((size_t)local_path_bytes) : NULL;

    uint64_t path_off = 0;
    for (uint64_t i = 0; i < local_count; i++) {
        const dscan_entry_t* e = &topk->entries[i];
        uint64_t plen = strlen(e->path) + 1;
        local_wires[i].type = e->type;
        local_wires[i].size = e->size;
        local_wires[i].atime = e->atime;
        local_wires[i].mtime = e->mtime;
        local_wires[i].ctime = e->ctime;
        local_wires[i].path_len = plen;
        memcpy(local_paths + path_off, e->path, (size_t)plen);
        path_off += plen;
    }

    uint64_t* counts = NULL;
    uint64_t* path_counts = NULL;
    if (rank == 0) {
        counts = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
        path_counts = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    }

    MPI_Gather(&local_count, 1, MPI_UINT64_T, counts, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    MPI_Gather(&local_path_bytes, 1, MPI_UINT64_T, path_counts, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    dscan_wire_entry_t* all_wires = NULL;
    char* all_paths = NULL;
    int* wire_recvcounts = NULL;
    int* wire_displs = NULL;
    int* path_recvcounts = NULL;
    int* path_displs = NULL;
    uint64_t total_count = 0;
    uint64_t total_path_bytes = 0;

    int fatal = 0;
    if (rank == 0) {
        wire_recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        wire_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        path_recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        path_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

        uint64_t wire_off = 0;
        uint64_t poff = 0;
        for (int r = 0; r < ranks; r++) {
            uint64_t wire_bytes = counts[r] * sizeof(dscan_wire_entry_t);
            wire_displs[r] = (int) wire_off;
            wire_recvcounts[r] = (int) wire_bytes;
            wire_off += wire_bytes;

            path_displs[r] = (int) poff;
            path_recvcounts[r] = (int) path_counts[r];
            poff += path_counts[r];

            total_count += counts[r];
        }
        total_path_bytes = poff;

        if (wire_off > (uint64_t)INT_MAX || total_path_bytes > (uint64_t)INT_MAX) {
            MFU_LOG(MFU_LOG_ERR, "top-K merge data exceeds MPI int range; lower --top-k");
            fatal = 1;
        }

        if (!fatal) {
            all_wires = (total_count > 0) ?
                (dscan_wire_entry_t*) MFU_MALLOC((size_t)total_count * sizeof(dscan_wire_entry_t)) : NULL;
            all_paths = (total_path_bytes > 0) ? (char*) MFU_MALLOC((size_t)total_path_bytes) : NULL;
        }
    }

    MPI_Bcast(&fatal, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (fatal) {
        *out_rc = 1;
        mfu_free(&local_wires);
        mfu_free(&local_paths);
        mfu_free(&counts);
        mfu_free(&path_counts);
        mfu_free(&wire_recvcounts);
        mfu_free(&wire_displs);
        mfu_free(&path_recvcounts);
        mfu_free(&path_displs);
        return NULL;
    }

    MPI_Gatherv(
        local_wires, (int)(local_count * sizeof(dscan_wire_entry_t)), MPI_BYTE,
        all_wires, wire_recvcounts, wire_displs, MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Gatherv(
        local_paths, (int)local_path_bytes, MPI_BYTE,
        all_paths, path_recvcounts, path_displs, MPI_BYTE, 0, MPI_COMM_WORLD);

    dscan_entry_t* merged = NULL;
    if (rank == 0 && total_count > 0) {
        merged = (dscan_entry_t*) MFU_MALLOC((size_t)total_count * sizeof(dscan_entry_t));

        uint64_t poff = 0;
        for (uint64_t i = 0; i < total_count; i++) {
            merged[i].type = all_wires[i].type;
            merged[i].size = all_wires[i].size;
            merged[i].atime = all_wires[i].atime;
            merged[i].mtime = all_wires[i].mtime;
            merged[i].ctime = all_wires[i].ctime;
            merged[i].path = MFU_STRDUP(all_paths + poff);
            poff += all_wires[i].path_len;
        }

        g_sort_field = topk->field;
        qsort(merged, (size_t)total_count, sizeof(dscan_entry_t), entry_qsort_cmp);

        uint64_t keep = (k < total_count) ? k : total_count;
        for (uint64_t i = keep; i < total_count; i++) {
            mfu_free(&merged[i].path);
        }
        *out_count = keep;
    }

    mfu_free(&local_wires);
    mfu_free(&local_paths);
    mfu_free(&counts);
    mfu_free(&path_counts);
    mfu_free(&wire_recvcounts);
    mfu_free(&wire_displs);
    mfu_free(&path_recvcounts);
    mfu_free(&path_displs);
    mfu_free(&all_wires);
    mfu_free(&all_paths);

    return merged;
}

/* gather the capped broken-path samples to rank 0, sorted by path and
 * truncated to broken_limit */
static dscan_broken_t* gather_broken(
    const dscan_acc_t* acc,
    int rank,
    int ranks,
    uint64_t broken_limit,
    uint64_t* out_count,
    int* out_rc)
{
    *out_count = 0;

    uint64_t local_count = acc->broken_count;
    uint64_t local_path_bytes = 0;
    for (uint64_t i = 0; i < local_count; i++) {
        local_path_bytes += strlen(acc->broken[i].path) + 1;
    }

    dscan_wire_broken_t* local_wires = (local_count > 0) ?
        (dscan_wire_broken_t*) MFU_MALLOC((size_t)local_count * sizeof(dscan_wire_broken_t)) : NULL;
    char* local_paths = (local_path_bytes > 0) ? (char*) MFU_MALLOC((size_t)local_path_bytes) : NULL;

    uint64_t path_off = 0;
    for (uint64_t i = 0; i < local_count; i++) {
        uint64_t plen = strlen(acc->broken[i].path) + 1;
        local_wires[i].flags = acc->broken[i].flags;
        local_wires[i].path_len = plen;
        memcpy(local_paths + path_off, acc->broken[i].path, (size_t)plen);
        path_off += plen;
    }

    uint64_t* counts = NULL;
    uint64_t* path_counts = NULL;
    if (rank == 0) {
        counts = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
        path_counts = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    }

    MPI_Gather(&local_count, 1, MPI_UINT64_T, counts, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    MPI_Gather(&local_path_bytes, 1, MPI_UINT64_T, path_counts, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    dscan_wire_broken_t* all_wires = NULL;
    char* all_paths = NULL;
    int* wire_recvcounts = NULL;
    int* wire_displs = NULL;
    int* path_recvcounts = NULL;
    int* path_displs = NULL;
    uint64_t total_count = 0;
    uint64_t total_path_bytes = 0;

    int fatal = 0;
    if (rank == 0) {
        wire_recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        wire_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        path_recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        path_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

        uint64_t wire_off = 0;
        uint64_t poff = 0;
        for (int r = 0; r < ranks; r++) {
            uint64_t wire_bytes = counts[r] * sizeof(dscan_wire_broken_t);
            wire_displs[r] = (int) wire_off;
            wire_recvcounts[r] = (int) wire_bytes;
            wire_off += wire_bytes;

            path_displs[r] = (int) poff;
            path_recvcounts[r] = (int) path_counts[r];
            poff += path_counts[r];

            total_count += counts[r];
        }
        total_path_bytes = poff;

        if (wire_off > (uint64_t)INT_MAX || total_path_bytes > (uint64_t)INT_MAX) {
            MFU_LOG(MFU_LOG_ERR, "broken-path merge data exceeds MPI int range; lower --broken-limit");
            fatal = 1;
        }

        if (!fatal) {
            all_wires = (total_count > 0) ?
                (dscan_wire_broken_t*) MFU_MALLOC((size_t)total_count * sizeof(dscan_wire_broken_t)) : NULL;
            all_paths = (total_path_bytes > 0) ? (char*) MFU_MALLOC((size_t)total_path_bytes) : NULL;
        }
    }

    MPI_Bcast(&fatal, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (fatal) {
        *out_rc = 1;
        mfu_free(&local_wires);
        mfu_free(&local_paths);
        mfu_free(&counts);
        mfu_free(&path_counts);
        mfu_free(&wire_recvcounts);
        mfu_free(&wire_displs);
        mfu_free(&path_recvcounts);
        mfu_free(&path_displs);
        return NULL;
    }

    MPI_Gatherv(
        local_wires, (int)(local_count * sizeof(dscan_wire_broken_t)), MPI_BYTE,
        all_wires, wire_recvcounts, wire_displs, MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Gatherv(
        local_paths, (int)local_path_bytes, MPI_BYTE,
        all_paths, path_recvcounts, path_displs, MPI_BYTE, 0, MPI_COMM_WORLD);

    dscan_broken_t* merged = NULL;
    if (rank == 0 && total_count > 0) {
        merged = (dscan_broken_t*) MFU_MALLOC((size_t)total_count * sizeof(dscan_broken_t));

        uint64_t poff = 0;
        for (uint64_t i = 0; i < total_count; i++) {
            merged[i].flags = all_wires[i].flags;
            merged[i].path = MFU_STRDUP(all_paths + poff);
            poff += all_wires[i].path_len;
        }

        qsort(merged, (size_t)total_count, sizeof(dscan_broken_t), broken_qsort_cmp);

        uint64_t keep = (broken_limit < total_count) ? broken_limit : total_count;
        for (uint64_t i = keep; i < total_count; i++) {
            mfu_free(&merged[i].path);
        }
        *out_count = keep;
    }

    mfu_free(&local_wires);
    mfu_free(&local_paths);
    mfu_free(&counts);
    mfu_free(&path_counts);
    mfu_free(&wire_recvcounts);
    mfu_free(&wire_displs);
    mfu_free(&path_recvcounts);
    mfu_free(&path_displs);
    mfu_free(&all_wires);
    mfu_free(&all_paths);

    return merged;
}

/* ---------------------------------------------------------------------------
 * report output
 * ------------------------------------------------------------------------- */

static void json_write_escaped(FILE* out, const char* text)
{
    fputc('"', out);

    const unsigned char* ptr = (const unsigned char*) text;
    while (*ptr != '\0') {
        unsigned char c = *ptr;

        if (c == '"') {
            fputs("\\\"", out);
        } else if (c == '\\') {
            fputs("\\\\", out);
        } else if (c == '\b') {
            fputs("\\b", out);
        } else if (c == '\f') {
            fputs("\\f", out);
        } else if (c == '\n') {
            fputs("\\n", out);
        } else if (c == '\r') {
            fputs("\\r", out);
        } else if (c == '\t') {
            fputs("\\t", out);
        } else if (c < 0x20) {
            fprintf(out, "\\u%04x", (unsigned int)c);
        } else {
            fputc((int) c, out);
        }

        ptr++;
    }

    fputc('"', out);
}

static void write_broken_reasons_json(FILE* out, uint64_t flags)
{
    int first = 1;

    fputc('[', out);

    if ((flags & DSCAN_BROKEN_SIZE) != 0) {
        if (!first) {
            fputs(", ", out);
        }
        json_write_escaped(out, "abnormal_size");
        first = 0;
    }
    if ((flags & DSCAN_BROKEN_MISSING) != 0) {
        if (!first) {
            fputs(", ", out);
        }
        json_write_escaped(out, "missing");
        first = 0;
    }
    if ((flags & DSCAN_BROKEN_TIME) != 0) {
        if (!first) {
            fputs(", ", out);
        }
        json_write_escaped(out, "abnormal_time");
        first = 0;
    }
    if ((flags & DSCAN_BROKEN_UNREADABLE) != 0) {
        if (!first) {
            fputs(", ", out);
        }
        json_write_escaped(out, "unreadable");
        first = 0;
    }

    fputc(']', out);
}

static void print_broken_reasons_text(uint64_t flags)
{
    int first = 1;

    if ((flags & DSCAN_BROKEN_SIZE) != 0) {
        if (!first) {
            printf(",");
        }
        printf("abnormal_size");
        first = 0;
    }
    if ((flags & DSCAN_BROKEN_MISSING) != 0) {
        if (!first) {
            printf(",");
        }
        printf("missing");
        first = 0;
    }
    if ((flags & DSCAN_BROKEN_TIME) != 0) {
        if (!first) {
            printf(",");
        }
        printf("abnormal_time");
        first = 0;
    }
    if ((flags & DSCAN_BROKEN_UNREADABLE) != 0) {
        if (!first) {
            printf(",");
        }
        printf("unreadable");
        first = 0;
    }
}

static void print_size_bucket_label(uint64_t bucket)
{
    uint64_t nlimits = (uint64_t) (sizeof(DSCAN_SIZE_LIMITS) / sizeof(DSCAN_SIZE_LIMITS[0]));

    if (bucket == 0) {
        double low_v;
        double high_v;
        const char* low_u;
        const char* high_u;
        mfu_format_bytes(0ULL, &low_v, &low_u);
        mfu_format_bytes(DSCAN_SIZE_LIMITS[0], &high_v, &high_u);
        printf("[%.2f %s, %.2f %s]", low_v, low_u, high_v, high_u);
        return;
    }

    if (bucket < nlimits) {
        uint64_t low = DSCAN_SIZE_LIMITS[bucket - 1] + 1ULL;
        uint64_t high = DSCAN_SIZE_LIMITS[bucket];

        double low_v;
        double high_v;
        const char* low_u;
        const char* high_u;
        mfu_format_bytes(low, &low_v, &low_u);
        mfu_format_bytes(high, &high_v, &high_u);
        printf("[%.2f %s, %.2f %s]", low_v, low_u, high_v, high_u);
        return;
    }

    uint64_t low = DSCAN_SIZE_LIMITS[nlimits - 1] + 1ULL;
    double low_v;
    const char* low_u;
    mfu_format_bytes(low, &low_v, &low_u);
    printf("[%.2f %s, INF]", low_v, low_u);
}

static void print_time_bucket_label(uint64_t bucket)
{
    uint64_t nlimits = (uint64_t) (sizeof(DSCAN_TIME_DAY_LIMITS) / sizeof(DSCAN_TIME_DAY_LIMITS[0]));

    if (bucket == 0) {
        printf("[0d, %" PRIu64 "d]", DSCAN_TIME_DAY_LIMITS[0]);
        return;
    }

    if (bucket < nlimits) {
        uint64_t low = DSCAN_TIME_DAY_LIMITS[bucket - 1] + 1ULL;
        uint64_t high = DSCAN_TIME_DAY_LIMITS[bucket];
        printf("[%" PRIu64 "d, %" PRIu64 "d]", low, high);
        return;
    }

    uint64_t low = DSCAN_TIME_DAY_LIMITS[nlimits - 1] + 1ULL;
    printf("[%" PRIu64 "d, INF]", low);
}

static void print_top_section(const char* title, const dscan_entry_t* entries, uint64_t count)
{
    printf("%s\n", title);
    if (count == 0) {
        printf("  (none)\n");
    }
    for (uint64_t i = 0; i < count; i++) {
        const dscan_entry_t* e = &entries[i];
        char at[64];
        char mt[64];
        char ct[64];
        format_epoch(e->atime, at, sizeof(at));
        format_epoch(e->mtime, mt, sizeof(mt));
        format_epoch(e->ctime, ct, sizeof(ct));

        double size_v;
        const char* size_u;
        mfu_format_bytes(e->size, &size_v, &size_u);

        printf("  %3" PRIu64 ". %s | %s | %.2f %s | atime=%s | mtime=%s | ctime=%s\n",
            i + 1,
            type_to_string(e->type),
            e->path,
            size_v,
            size_u,
            at,
            mt,
            ct);
    }
    printf("\n");
}

static void print_pretty_report(
    const char* directory,
    uint64_t top_k,
    uint64_t total_items,
    uint64_t total_files,
    uint64_t total_dirs,
    uint64_t total_links,
    uint64_t total_other,
    uint64_t scan_errors,
    const uint64_t* size_hist,
    const uint64_t* atime_hist,
    const uint64_t* mtime_hist,
    const uint64_t* ctime_hist,
    const dscan_entry_t* top_atime,
    uint64_t top_atime_count,
    const dscan_entry_t* top_mtime,
    uint64_t top_mtime_count,
    const dscan_entry_t* top_ctime,
    uint64_t top_ctime_count,
    const dscan_broken_t* broken,
    uint64_t broken_count,
    uint64_t broken_total)
{
    printf("\n");
    printf("dscan report\n");
    printf("directory: %s\n", directory);
    printf("top-k: %" PRIu64 "\n", top_k);
    printf("\n");

    printf("summary\n");
    printf("  total_entries    : %" PRIu64 "\n", total_items);
    printf("  total_files      : %" PRIu64 "\n", total_files);
    printf("  total_directories: %" PRIu64 "\n", total_dirs);
    printf("  total_symlinks   : %" PRIu64 "\n", total_links);
    printf("  total_other      : %" PRIu64 "\n", total_other);
    printf("  scan_errors      : %" PRIu64 "\n", scan_errors);
    printf("\n");

    printf("file size histogram\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_SIZE_BIN_COUNT; i++) {
        printf("  ");
        print_size_bucket_label(i);
        printf(" => %" PRIu64 "\n", size_hist[i]);
    }
    printf("\n");

    printf("atime histogram by capacity in bytes (files)\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_TIME_BIN_COUNT; i++) {
        printf("  ");
        print_time_bucket_label(i);
        printf(" => %" PRIu64 "\n", atime_hist[i]);
    }
    printf("\n");

    printf("mtime histogram by capacity in bytes (files)\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_TIME_BIN_COUNT; i++) {
        printf("  ");
        print_time_bucket_label(i);
        printf(" => %" PRIu64 "\n", mtime_hist[i]);
    }
    printf("\n");

    printf("ctime histogram by capacity in bytes (files)\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_TIME_BIN_COUNT; i++) {
        printf("  ");
        print_time_bucket_label(i);
        printf(" => %" PRIu64 "\n", ctime_hist[i]);
    }
    printf("\n");

    print_top_section("oldest by atime", top_atime, top_atime_count);
    print_top_section("oldest by mtime", top_mtime, top_mtime_count);
    print_top_section("oldest by ctime", top_ctime, top_ctime_count);

    printf("broken paths\n");
    printf("  count: %" PRIu64 "%s\n", broken_total,
        (broken_count < broken_total) ? " (list truncated by --broken-limit)" : "");
    for (uint64_t i = 0; i < broken_count; i++) {
        printf("  - %s [", broken[i].path);
        print_broken_reasons_text(broken[i].flags);
        printf("]\n");
    }
    printf("\n");
}

static void write_json_top_array(
    FILE* out,
    const char* key,
    const dscan_entry_t* entries,
    uint64_t count)
{
    fprintf(out, "    \"%s\": [\n", key);
    for (uint64_t i = 0; i < count; i++) {
        const dscan_entry_t* e = &entries[i];

        fprintf(out, "      {\n");

        fprintf(out, "        \"path\": ");
        json_write_escaped(out, e->path);
        fprintf(out, ",\n");

        fprintf(out, "        \"type\": ");
        json_write_escaped(out, type_to_string(e->type));
        fprintf(out, ",\n");

        fprintf(out, "        \"size_bytes\": %" PRIu64 ",\n", e->size);
        fprintf(out, "        \"atime\": %" PRIu64 ",\n", e->atime);
        fprintf(out, "        \"mtime\": %" PRIu64 ",\n", e->mtime);
        fprintf(out, "        \"ctime\": %" PRIu64 "\n", e->ctime);

        fprintf(out, "      }%s\n", (i + 1 == count) ? "" : ",");
    }
    fprintf(out, "    ]");
}

static int write_json_report(
    const char* output_file,
    const char* directory,
    uint64_t generated_at,
    uint64_t top_k,
    uint64_t total_items,
    uint64_t total_files,
    uint64_t total_dirs,
    uint64_t total_links,
    uint64_t total_other,
    uint64_t scan_errors,
    const uint64_t* size_hist,
    const uint64_t* atime_hist,
    const uint64_t* mtime_hist,
    const uint64_t* ctime_hist,
    const dscan_entry_t* top_atime,
    uint64_t top_atime_count,
    const dscan_entry_t* top_mtime,
    uint64_t top_mtime_count,
    const dscan_entry_t* top_ctime,
    uint64_t top_ctime_count,
    const dscan_broken_t* broken,
    uint64_t broken_count,
    uint64_t broken_total,
    uint64_t broken_limit,
    uint64_t past_limit,
    uint64_t future_limit)
{
    FILE* out = fopen(output_file, "w");
    if (out == NULL) {
        return -1;
    }

    fprintf(out, "{\n");

    fprintf(out, "  \"directory\": ");
    json_write_escaped(out, directory);
    fprintf(out, ",\n");

    fprintf(out, "  \"generated_at_epoch\": %" PRIu64 ",\n", generated_at);
    fprintf(out, "  \"top_k\": %" PRIu64 ",\n", top_k);

    fprintf(out, "  \"thresholds\": {\n");
    fprintf(out, "    \"abnormal_size_bytes\": %" PRIu64 ",\n", (uint64_t)DSCAN_ABNORMAL_SIZE_BYTES);
    fprintf(out, "    \"time_past_limit_epoch\": %" PRIu64 ",\n", past_limit);
    fprintf(out, "    \"time_future_limit_epoch\": %" PRIu64 "\n", future_limit);
    fprintf(out, "  },\n");

    fprintf(out, "  \"summary\": {\n");
    fprintf(out, "    \"total_entries\": %" PRIu64 ",\n", total_items);
    fprintf(out, "    \"total_files\": %" PRIu64 ",\n", total_files);
    fprintf(out, "    \"total_directories\": %" PRIu64 ",\n", total_dirs);
    fprintf(out, "    \"total_symlinks\": %" PRIu64 ",\n", total_links);
    fprintf(out, "    \"total_other\": %" PRIu64 ",\n", total_other);
    fprintf(out, "    \"scan_errors\": %" PRIu64 "\n", scan_errors);
    fprintf(out, "  },\n");

    fprintf(out, "  \"file_size_histogram\": [\n");
    uint64_t size_limits = (uint64_t) (sizeof(DSCAN_SIZE_LIMITS) / sizeof(DSCAN_SIZE_LIMITS[0]));
    for (uint64_t i = 0; i < (uint64_t)DSCAN_SIZE_BIN_COUNT; i++) {
        uint64_t lower = (i == 0) ? 0ULL : (DSCAN_SIZE_LIMITS[i - 1] + 1ULL);
        bool has_upper = (i < size_limits);

        fprintf(out, "    {\n");

        fprintf(out, "      \"bucket\": ");
        if (has_upper) {
            char label[128];
            snprintf(label, sizeof(label), "[%" PRIu64 ",%" PRIu64 "]", lower, DSCAN_SIZE_LIMITS[i]);
            json_write_escaped(out, label);
        } else {
            char label[128];
            snprintf(label, sizeof(label), "[%" PRIu64 ",INF]", lower);
            json_write_escaped(out, label);
        }
        fprintf(out, ",\n");

        fprintf(out, "      \"lower_inclusive\": %" PRIu64 ",\n", lower);
        if (has_upper) {
            fprintf(out, "      \"upper_inclusive\": %" PRIu64 ",\n", DSCAN_SIZE_LIMITS[i]);
        } else {
            fprintf(out, "      \"upper_inclusive\": null,\n");
        }
        fprintf(out, "      \"count\": %" PRIu64 "\n", size_hist[i]);

        fprintf(out, "    }%s\n", (i + 1 == (uint64_t)DSCAN_SIZE_BIN_COUNT) ? "" : ",");
    }
    fprintf(out, "  ],\n");

    fprintf(out, "  \"time_histograms\": {\n");
    const char* time_keys[3] = {"atime", "mtime", "ctime"};
    const uint64_t* time_hists[3] = {atime_hist, mtime_hist, ctime_hist};
    uint64_t time_limits = (uint64_t) (sizeof(DSCAN_TIME_DAY_LIMITS) / sizeof(DSCAN_TIME_DAY_LIMITS[0]));

    for (int t = 0; t < 3; t++) {
        fprintf(out, "    \"%s\": [\n", time_keys[t]);
        for (uint64_t i = 0; i < (uint64_t)DSCAN_TIME_BIN_COUNT; i++) {
            uint64_t min_age = (i == 0) ? 0ULL : (DSCAN_TIME_DAY_LIMITS[i - 1] + 1ULL);
            bool has_max = (i < time_limits);

            fprintf(out, "      {\n");

            fprintf(out, "        \"bucket\": ");
            if (has_max) {
                char label[128];
                snprintf(label, sizeof(label), "[%" PRIu64 "d,%" PRIu64 "d]", min_age, DSCAN_TIME_DAY_LIMITS[i]);
                json_write_escaped(out, label);
            } else {
                char label[128];
                snprintf(label, sizeof(label), "[%" PRIu64 "d,INF]", min_age);
                json_write_escaped(out, label);
            }
            fprintf(out, ",\n");

            fprintf(out, "        \"min_age_days\": %" PRIu64 ",\n", min_age);
            if (has_max) {
                fprintf(out, "        \"max_age_days\": %" PRIu64 ",\n", DSCAN_TIME_DAY_LIMITS[i]);
            } else {
                fprintf(out, "        \"max_age_days\": null,\n");
            }
            fprintf(out, "        \"bytes\": %" PRIu64 "\n", time_hists[t][i]);

            fprintf(out, "      }%s\n", (i + 1 == (uint64_t)DSCAN_TIME_BIN_COUNT) ? "" : ",");
        }
        fprintf(out, "    ]%s\n", (t == 2) ? "" : ",");
    }
    fprintf(out, "  },\n");

    fprintf(out, "  \"oldest\": {\n");
    write_json_top_array(out, "atime", top_atime, top_atime_count);
    fprintf(out, ",\n");
    write_json_top_array(out, "mtime", top_mtime, top_mtime_count);
    fprintf(out, ",\n");
    write_json_top_array(out, "ctime", top_ctime, top_ctime_count);
    fprintf(out, "\n");
    fprintf(out, "  },\n");

    fprintf(out, "  \"broken_paths_total\": %" PRIu64 ",\n", broken_total);
    fprintf(out, "  \"broken_paths_limit\": %" PRIu64 ",\n", broken_limit);
    fprintf(out, "  \"broken_paths\": [\n");
    for (uint64_t i = 0; i < broken_count; i++) {
        fprintf(out, "    {\n");

        fprintf(out, "      \"path\": ");
        json_write_escaped(out, broken[i].path);
        fprintf(out, ",\n");

        fprintf(out, "      \"reasons\": ");
        write_broken_reasons_json(out, broken[i].flags);
        fprintf(out, "\n");

        fprintf(out, "    }%s\n", (i + 1 == broken_count) ? "" : ",");
    }
    fprintf(out, "  ]\n");

    fprintf(out, "}\n");

    fclose(out);
    return 0;
}

/* ---------------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------------- */

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    mfu_init();

    int rank;
    int ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    mfu_debug_level = MFU_LOG_VERBOSE;

    int rc = 0;
    int usage = 0;
    int help_requested = 0;

    char* directory = NULL;
    char* output_file = NULL;
    uint64_t top_k = 10;
    uint64_t batch_files = 0;
    uint64_t broken_limit = DSCAN_DEFAULT_BROKEN_LIMIT;
    int print_pretty = 0;

    static struct option long_options[] = {
        {"directory", required_argument, 0, 'd'},
        {"output", required_argument, 0, 'o'},
        {"print", no_argument, 0, 'p'},
        {"top-k", required_argument, 0, 'k'},
        {"batch-files", required_argument, 0, 'b'},
        {"broken-limit", required_argument, 0, 'B'},
        {"verbose", no_argument, 0, 'v'},
        {"quiet", no_argument, 0, 'q'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int option_index = 0;
    int c;
    while ((c = getopt_long(argc, argv, "d:o:pk:b:vqh", long_options, &option_index)) != -1) {
        switch (c) {
            case 'd':
                directory = MFU_STRDUP(optarg);
                break;
            case 'o':
                output_file = MFU_STRDUP(optarg);
                break;
            case 'p':
                print_pretty = 1;
                break;
            case 'k':
                if (parse_uint64(optarg, &top_k) != 0 || top_k == 0) {
                    usage = 1;
                }
                break;
            case 'b':
                if (parse_uint64(optarg, &batch_files) != 0 || batch_files == 0) {
                    usage = 1;
                }
                break;
            case 'B':
                if (parse_uint64(optarg, &broken_limit) != 0) {
                    usage = 1;
                }
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = 0;
                break;
            case 'h':
                help_requested = 1;
                break;
            case '?':
            default:
                usage = 1;
                break;
        }
    }

    if (optind < argc) {
        usage = 1;
    }

    if (help_requested) {
        if (rank == 0) {
            print_usage();
        }

        mfu_free(&directory);
        mfu_free(&output_file);
        mfu_finalize();
        MPI_Finalize();
        return 0;
    }

    if (directory == NULL || output_file == NULL) {
        usage = 1;
    }

    if (usage) {
        if (rank == 0) {
            print_usage();
        }

        mfu_free(&directory);
        mfu_free(&output_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* strip trailing slashes so path building and hashing are consistent */
    size_t dir_len = strlen(directory);
    while (dir_len > 1 && directory[dir_len - 1] == '/') {
        directory[dir_len - 1] = '\0';
        dir_len--;
    }

    struct stat root_stat;
    if (stat(directory, &root_stat) != 0 || !S_ISDIR(root_stat.st_mode)) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "--directory must point to an existing directory: %s", directory);
        }
        mfu_free(&directory);
        mfu_free(&output_file);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    /* use one consistent notion of "now" across all ranks */
    uint64_t now = (uint64_t) time(NULL);
    MPI_Bcast(&now, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    dscan_acc_t acc;
    acc_init(&acc, top_k, broken_limit, now);

    dscan_walk_opts_t wopts;
    wopts.rank = rank;
    wopts.ranks = ranks;
    wopts.batch_files = batch_files;
    wopts.round_quota = DSCAN_ROUND_ENTRY_QUOTA_DEFAULT;
    if (batch_files > 0) {
        uint64_t per_rank = batch_files / (uint64_t)ranks;
        if (per_rank == 0) {
            per_rank = 1;
        }
        if (per_rank < wopts.round_quota) {
            wopts.round_quota = per_rank;
        }
    }

    uint64_t total_scanned = 0;
    uint64_t batches = 0;
    if (walk_directory_streaming(&wopts, &acc, directory, &total_scanned, &batches) != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to walk directory: %s", directory);
        }
        rc = 1;
        goto cleanup;
    }

    {
        /* combine fixed-size statistics with a single reduce */
        enum {
            RED_FILES = 0,
            RED_DIRS,
            RED_LINKS,
            RED_OTHER,
            RED_SCAN_ERRORS,
            RED_BROKEN_TOTAL,
            RED_HIST_BASE,
        };
        const uint64_t red_count = RED_HIST_BASE +
            DSCAN_SIZE_BIN_COUNT + 3 * DSCAN_TIME_BIN_COUNT;

        uint64_t* red_local = (uint64_t*) MFU_MALLOC((size_t)red_count * sizeof(uint64_t));
        uint64_t* red_global = (uint64_t*) MFU_MALLOC((size_t)red_count * sizeof(uint64_t));

        uint64_t off = RED_HIST_BASE;
        red_local[RED_FILES] = acc.total_files;
        red_local[RED_DIRS] = acc.total_dirs;
        red_local[RED_LINKS] = acc.total_links;
        red_local[RED_OTHER] = acc.total_other;
        red_local[RED_SCAN_ERRORS] = acc.scan_errors;
        red_local[RED_BROKEN_TOTAL] = acc.broken_total;
        memcpy(&red_local[off], acc.size_hist, sizeof(acc.size_hist));
        off += DSCAN_SIZE_BIN_COUNT;
        memcpy(&red_local[off], acc.atime_hist, sizeof(acc.atime_hist));
        off += DSCAN_TIME_BIN_COUNT;
        memcpy(&red_local[off], acc.mtime_hist, sizeof(acc.mtime_hist));
        off += DSCAN_TIME_BIN_COUNT;
        memcpy(&red_local[off], acc.ctime_hist, sizeof(acc.ctime_hist));

        MPI_Reduce(red_local, red_global, (int)red_count, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);

        /* small gathers: at most K entries and broken-limit paths per rank */
        uint64_t top_atime_count = 0;
        uint64_t top_mtime_count = 0;
        uint64_t top_ctime_count = 0;
        dscan_entry_t* top_atime = gather_and_merge_topk(&acc.top_atime, rank, ranks, top_k, &top_atime_count, &rc);
        dscan_entry_t* top_mtime = gather_and_merge_topk(&acc.top_mtime, rank, ranks, top_k, &top_mtime_count, &rc);
        dscan_entry_t* top_ctime = gather_and_merge_topk(&acc.top_ctime, rank, ranks, top_k, &top_ctime_count, &rc);

        uint64_t broken_count = 0;
        dscan_broken_t* broken = gather_broken(&acc, rank, ranks, broken_limit, &broken_count, &rc);

        if (rank == 0 && rc == 0) {
            uint64_t total_files = red_global[RED_FILES];
            uint64_t total_dirs = red_global[RED_DIRS];
            uint64_t total_links = red_global[RED_LINKS];
            uint64_t total_other = red_global[RED_OTHER];
            uint64_t scan_errors = red_global[RED_SCAN_ERRORS];
            uint64_t broken_total = red_global[RED_BROKEN_TOTAL];
            uint64_t total_items = total_files + total_dirs + total_links + total_other;

            const uint64_t* size_hist = &red_global[RED_HIST_BASE];
            const uint64_t* atime_hist = size_hist + DSCAN_SIZE_BIN_COUNT;
            const uint64_t* mtime_hist = atime_hist + DSCAN_TIME_BIN_COUNT;
            const uint64_t* ctime_hist = mtime_hist + DSCAN_TIME_BIN_COUNT;

            uint64_t past_limit = (now > DSCAN_TEN_YEARS_SECONDS) ? (now - DSCAN_TEN_YEARS_SECONDS) : 0ULL;
            uint64_t future_limit = now;

            if (write_json_report(
                    output_file,
                    directory,
                    now,
                    top_k,
                    total_items,
                    total_files,
                    total_dirs,
                    total_links,
                    total_other,
                    scan_errors,
                    size_hist,
                    atime_hist,
                    mtime_hist,
                    ctime_hist,
                    top_atime,
                    top_atime_count,
                    top_mtime,
                    top_mtime_count,
                    top_ctime,
                    top_ctime_count,
                    broken,
                    broken_count,
                    broken_total,
                    broken_limit,
                    past_limit,
                    future_limit) != 0)
            {
                MFU_LOG(MFU_LOG_ERR, "Failed to write output file: %s", output_file);
                rc = 1;
            }

            if (rc == 0 && print_pretty) {
                print_pretty_report(
                    directory,
                    top_k,
                    total_items,
                    total_files,
                    total_dirs,
                    total_links,
                    total_other,
                    scan_errors,
                    size_hist,
                    atime_hist,
                    mtime_hist,
                    ctime_hist,
                    top_atime,
                    top_atime_count,
                    top_mtime,
                    top_mtime_count,
                    top_ctime,
                    top_ctime_count,
                    broken,
                    broken_count,
                    broken_total);
            }
        }

        for (uint64_t i = 0; i < top_atime_count; i++) {
            mfu_free(&top_atime[i].path);
        }
        for (uint64_t i = 0; i < top_mtime_count; i++) {
            mfu_free(&top_mtime[i].path);
        }
        for (uint64_t i = 0; i < top_ctime_count; i++) {
            mfu_free(&top_ctime[i].path);
        }
        for (uint64_t i = 0; i < broken_count; i++) {
            mfu_free(&broken[i].path);
        }
        mfu_free(&top_atime);
        mfu_free(&top_mtime);
        mfu_free(&top_ctime);
        mfu_free(&broken);
        mfu_free(&red_local);
        mfu_free(&red_global);
    }

    MPI_Bcast(&rc, 1, MPI_INT, 0, MPI_COMM_WORLD);

cleanup:
    acc_free(&acc);

    mfu_free(&directory);
    mfu_free(&output_file);

    mfu_finalize();
    MPI_Finalize();

    return rc;
}
