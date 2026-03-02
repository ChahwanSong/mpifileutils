#define _GNU_SOURCE

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

typedef struct {
    uint64_t type;
    uint64_t size;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
    uint64_t path_len;
    uint64_t broken_flags;
} dscan_wire_item_t;

typedef struct {
    char* path;
    uint64_t type;
    uint64_t size;
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
    uint64_t broken_flags;
} dscan_item_t;

typedef struct {
    const char* path;
    uint64_t item_index;
    uint64_t size;
} dscan_dir_agg_t;

static const dscan_item_t* g_sort_items = NULL;
static dscan_top_field_t g_sort_field = DSCAN_TOP_ATIME;
static const dscan_dir_agg_t* g_sort_dirs = NULL;

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
    printf("  -k, --top-k <N>          - oldest top-K directories per time field (default 10)\n");
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

static uint64_t top_field_value(const dscan_item_t* item, dscan_top_field_t field)
{
    if (field == DSCAN_TOP_ATIME) {
        return item->atime;
    }
    if (field == DSCAN_TOP_MTIME) {
        return item->mtime;
    }
    return item->ctime;
}

static int cmp_top_indices(const void* a, const void* b)
{
    uint64_t ia = *(const uint64_t*) a;
    uint64_t ib = *(const uint64_t*) b;

    const dscan_item_t* item_a = &g_sort_items[ia];
    const dscan_item_t* item_b = &g_sort_items[ib];

    uint64_t va = top_field_value(item_a, g_sort_field);
    uint64_t vb = top_field_value(item_b, g_sort_field);

    if (va < vb) {
        return -1;
    }
    if (va > vb) {
        return 1;
    }

    return strcmp(item_a->path, item_b->path);
}

static int cmp_dir_agg_by_path(const void* a, const void* b)
{
    const dscan_dir_agg_t* da = (const dscan_dir_agg_t*) a;
    const dscan_dir_agg_t* db = (const dscan_dir_agg_t*) b;
    return strcmp(da->path, db->path);
}

static int cmp_dir_index_by_path_len_desc(const void* a, const void* b)
{
    uint64_t ia = *(const uint64_t*) a;
    uint64_t ib = *(const uint64_t*) b;

    size_t la = strlen(g_sort_dirs[ia].path);
    size_t lb = strlen(g_sort_dirs[ib].path);

    if (la > lb) {
        return -1;
    }
    if (la < lb) {
        return 1;
    }
    return strcmp(g_sort_dirs[ia].path, g_sort_dirs[ib].path);
}

static int find_dir_index(const dscan_dir_agg_t* dirs, uint64_t count, const char* path)
{
    uint64_t low = 0;
    uint64_t high = count;

    while (low < high) {
        uint64_t mid = low + (high - low) / 2;
        int cmp = strcmp(path, dirs[mid].path);
        if (cmp == 0) {
            return (int) mid;
        }
        if (cmp < 0) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }

    return -1;
}

static char* get_parent_path_dup(const char* path)
{
    if (path == NULL || path[0] == '\0') {
        return NULL;
    }

    size_t end = strlen(path);
    while (end > 1 && path[end - 1] == '/') {
        end--;
    }

    if (end == 1 && path[0] == '/') {
        return NULL;
    }

    size_t pos = end;
    while (pos > 0 && path[pos - 1] != '/') {
        pos--;
    }

    if (pos == 0) {
        return NULL;
    }

    size_t len = (pos == 1) ? 1 : (pos - 1);
    char* parent = (char*) MFU_MALLOC(len + 1);
    memcpy(parent, path, len);
    parent[len] = '\0';
    return parent;
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

static int is_readable_path(const char* path, uint64_t type)
{
    if (type == MFU_TYPE_FILE) {
        int fd = open(path, O_RDONLY);
        if (fd < 0) {
            return 0;
        }
        close(fd);
        return 1;
    }

    if (type == MFU_TYPE_DIR) {
        DIR* dir = opendir(path);
        if (dir == NULL) {
            return 0;
        }
        closedir(dir);
        return 1;
    }

    if (type == MFU_TYPE_LINK) {
        char tmp[2];
        if (readlink(path, tmp, sizeof(tmp)) < 0) {
            return 0;
        }
        return 1;
    }

    return access(path, R_OK) == 0;
}

static uint64_t compute_broken_flags(
    const char* path,
    uint64_t type,
    uint64_t size,
    uint64_t atime,
    uint64_t mtime,
    uint64_t ctime,
    uint64_t now)
{
    uint64_t flags = 0;

    if (type == MFU_TYPE_FILE && size > DSCAN_ABNORMAL_SIZE_BYTES) {
        flags |= DSCAN_BROKEN_SIZE;
    }

    uint64_t past_limit = (now > DSCAN_TEN_YEARS_SECONDS) ? (now - DSCAN_TEN_YEARS_SECONDS) : 0ULL;
    if (atime < past_limit || atime > now ||
        mtime < past_limit || mtime > now ||
        ctime < past_limit || ctime > now)
    {
        flags |= DSCAN_BROKEN_TIME;
    }

    struct stat st;
    if (lstat(path, &st) != 0) {
        if (errno == ENOENT || errno == ENOTDIR) {
            flags |= DSCAN_BROKEN_MISSING;
        } else {
            flags |= DSCAN_BROKEN_UNREADABLE;
        }
        return flags;
    }

    if (!is_readable_path(path, type)) {
        flags |= DSCAN_BROKEN_UNREADABLE;
    }

    return flags;
}

static uint64_t* build_topk(
    const dscan_item_t* items,
    const uint64_t* candidates,
    uint64_t candidate_count,
    uint64_t topk,
    dscan_top_field_t field,
    uint64_t* out_count)
{
    *out_count = 0;

    if (candidate_count == 0 || topk == 0) {
        return NULL;
    }

    uint64_t* sorted = (uint64_t*) MFU_MALLOC((size_t)candidate_count * sizeof(uint64_t));
    memcpy(sorted, candidates, (size_t)candidate_count * sizeof(uint64_t));

    g_sort_items = items;
    g_sort_field = field;
    qsort(sorted, (size_t)candidate_count, sizeof(uint64_t), cmp_top_indices);

    uint64_t out_n = (topk < candidate_count) ? topk : candidate_count;
    uint64_t* out = (uint64_t*) MFU_MALLOC((size_t)out_n * sizeof(uint64_t));
    memcpy(out, sorted, (size_t)out_n * sizeof(uint64_t));

    *out_count = out_n;
    mfu_free(&sorted);
    return out;
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

static void print_pretty_report(
    const char* directory,
    uint64_t top_k,
    uint64_t total_items,
    uint64_t total_files,
    uint64_t total_dirs,
    uint64_t total_links,
    uint64_t total_other,
    const dscan_item_t* items,
    const uint64_t* effective_sizes,
    const uint64_t* size_hist,
    const uint64_t* atime_hist,
    const uint64_t* mtime_hist,
    const uint64_t* ctime_hist,
    const uint64_t* top_atime,
    uint64_t top_atime_count,
    const uint64_t* top_mtime,
    uint64_t top_mtime_count,
    const uint64_t* top_ctime,
    uint64_t top_ctime_count,
    const uint64_t* broken_indices,
    uint64_t broken_count)
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
    printf("\n");

    printf("file size histogram\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_SIZE_BIN_COUNT; i++) {
        printf("  ");
        print_size_bucket_label(i);
        printf(" => %" PRIu64 "\n", size_hist[i]);
    }
    printf("\n");

    printf("atime histogram (files + directories)\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_TIME_BIN_COUNT; i++) {
        printf("  ");
        print_time_bucket_label(i);
        printf(" => %" PRIu64 "\n", atime_hist[i]);
    }
    printf("\n");

    printf("mtime histogram (files + directories)\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_TIME_BIN_COUNT; i++) {
        printf("  ");
        print_time_bucket_label(i);
        printf(" => %" PRIu64 "\n", mtime_hist[i]);
    }
    printf("\n");

    printf("ctime histogram (files + directories)\n");
    for (uint64_t i = 0; i < (uint64_t)DSCAN_TIME_BIN_COUNT; i++) {
        printf("  ");
        print_time_bucket_label(i);
        printf(" => %" PRIu64 "\n", ctime_hist[i]);
    }
    printf("\n");

    printf("oldest directories by atime\n");
    if (top_atime_count == 0) {
        printf("  (none)\n");
    }
    for (uint64_t i = 0; i < top_atime_count; i++) {
        uint64_t idx = top_atime[i];
        const dscan_item_t* item = &items[idx];
        char at[64];
        char mt[64];
        char ct[64];
        format_epoch(item->atime, at, sizeof(at));
        format_epoch(item->mtime, mt, sizeof(mt));
        format_epoch(item->ctime, ct, sizeof(ct));

        double size_v;
        const char* size_u;
        mfu_format_bytes(effective_sizes[idx], &size_v, &size_u);

        printf("  %3" PRIu64 ". %s | %s | %.2f %s | atime=%s | mtime=%s | ctime=%s\n",
            i + 1,
            type_to_string(item->type),
            item->path,
            size_v,
            size_u,
            at,
            mt,
            ct);
    }
    printf("\n");

    printf("oldest directories by mtime\n");
    if (top_mtime_count == 0) {
        printf("  (none)\n");
    }
    for (uint64_t i = 0; i < top_mtime_count; i++) {
        uint64_t idx = top_mtime[i];
        const dscan_item_t* item = &items[idx];
        char at[64];
        char mt[64];
        char ct[64];
        format_epoch(item->atime, at, sizeof(at));
        format_epoch(item->mtime, mt, sizeof(mt));
        format_epoch(item->ctime, ct, sizeof(ct));

        double size_v;
        const char* size_u;
        mfu_format_bytes(effective_sizes[idx], &size_v, &size_u);

        printf("  %3" PRIu64 ". %s | %s | %.2f %s | atime=%s | mtime=%s | ctime=%s\n",
            i + 1,
            type_to_string(item->type),
            item->path,
            size_v,
            size_u,
            at,
            mt,
            ct);
    }
    printf("\n");

    printf("oldest directories by ctime\n");
    if (top_ctime_count == 0) {
        printf("  (none)\n");
    }
    for (uint64_t i = 0; i < top_ctime_count; i++) {
        uint64_t idx = top_ctime[i];
        const dscan_item_t* item = &items[idx];
        char at[64];
        char mt[64];
        char ct[64];
        format_epoch(item->atime, at, sizeof(at));
        format_epoch(item->mtime, mt, sizeof(mt));
        format_epoch(item->ctime, ct, sizeof(ct));

        double size_v;
        const char* size_u;
        mfu_format_bytes(effective_sizes[idx], &size_v, &size_u);

        printf("  %3" PRIu64 ". %s | %s | %.2f %s | atime=%s | mtime=%s | ctime=%s\n",
            i + 1,
            type_to_string(item->type),
            item->path,
            size_v,
            size_u,
            at,
            mt,
            ct);
    }
    printf("\n");

    printf("broken paths\n");
    printf("  count: %" PRIu64 "\n", broken_count);
    for (uint64_t i = 0; i < broken_count; i++) {
        uint64_t idx = broken_indices[i];
        const dscan_item_t* item = &items[idx];
        printf("  - %s [", item->path);
        print_broken_reasons_text(item->broken_flags);
        printf("]\n");
    }
    printf("\n");
}

static void write_json_top_array(
    FILE* out,
    const char* key,
    const dscan_item_t* items,
    const uint64_t* effective_sizes,
    const uint64_t* indices,
    uint64_t count)
{
    fprintf(out, "    \"%s\": [\n", key);
    for (uint64_t i = 0; i < count; i++) {
        uint64_t idx = indices[i];
        const dscan_item_t* item = &items[idx];

        fprintf(out, "      {\n");

        fprintf(out, "        \"path\": ");
        json_write_escaped(out, item->path);
        fprintf(out, ",\n");

        fprintf(out, "        \"type\": ");
        json_write_escaped(out, type_to_string(item->type));
        fprintf(out, ",\n");

        fprintf(out, "        \"size_bytes\": %" PRIu64 ",\n", effective_sizes[idx]);
        fprintf(out, "        \"atime\": %" PRIu64 ",\n", item->atime);
        fprintf(out, "        \"mtime\": %" PRIu64 ",\n", item->mtime);
        fprintf(out, "        \"ctime\": %" PRIu64 "\n", item->ctime);

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
    const dscan_item_t* items,
    const uint64_t* effective_sizes,
    const uint64_t* size_hist,
    const uint64_t* atime_hist,
    const uint64_t* mtime_hist,
    const uint64_t* ctime_hist,
    const uint64_t* top_atime,
    uint64_t top_atime_count,
    const uint64_t* top_mtime,
    uint64_t top_mtime_count,
    const uint64_t* top_ctime,
    uint64_t top_ctime_count,
    const uint64_t* broken_indices,
    uint64_t broken_count,
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
    fprintf(out, "    \"total_other\": %" PRIu64 "\n", total_other);
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
            fprintf(out, "        \"count\": %" PRIu64 "\n", time_hists[t][i]);

            fprintf(out, "      }%s\n", (i + 1 == (uint64_t)DSCAN_TIME_BIN_COUNT) ? "" : ",");
        }
        fprintf(out, "    ]%s\n", (t == 2) ? "" : ",");
    }
    fprintf(out, "  },\n");

    fprintf(out, "  \"oldest\": {\n");
    write_json_top_array(out, "atime", items, effective_sizes, top_atime, top_atime_count);
    fprintf(out, ",\n");
    write_json_top_array(out, "mtime", items, effective_sizes, top_mtime, top_mtime_count);
    fprintf(out, ",\n");
    write_json_top_array(out, "ctime", items, effective_sizes, top_ctime, top_ctime_count);
    fprintf(out, "\n");
    fprintf(out, "  },\n");

    fprintf(out, "  \"broken_paths\": [\n");
    for (uint64_t i = 0; i < broken_count; i++) {
        uint64_t idx = broken_indices[i];
        const dscan_item_t* item = &items[idx];

        fprintf(out, "    {\n");

        fprintf(out, "      \"path\": ");
        json_write_escaped(out, item->path);
        fprintf(out, ",\n");

        fprintf(out, "      \"reasons\": ");
        write_broken_reasons_json(out, item->broken_flags);
        fprintf(out, "\n");

        fprintf(out, "    }%s\n", (i + 1 == broken_count) ? "" : ",");
    }
    fprintf(out, "  ]\n");

    fprintf(out, "}\n");

    fclose(out);
    return 0;
}

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
    int print_pretty = 0;

    static struct option long_options[] = {
        {"directory", required_argument, 0, 'd'},
        {"output", required_argument, 0, 'o'},
        {"print", no_argument, 0, 'p'},
        {"top-k", required_argument, 0, 'k'},
        {"verbose", no_argument, 0, 'v'},
        {"quiet", no_argument, 0, 'q'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    int option_index = 0;
    int c;
    while ((c = getopt_long(argc, argv, "d:o:pk:vqh", long_options, &option_index)) != -1) {
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

    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();
    mfu_file_t* mfu_file = mfu_file_new();
    mfu_flist flist = mfu_flist_new();

    int walk_rc = mfu_flist_walk_path(directory, walk_opts, flist, mfu_file);
    if (walk_rc != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to walk directory: %s", directory);
        }
        rc = 1;
        goto cleanup_local;
    }

    uint64_t local_count = mfu_flist_size(flist);
    uint64_t now = (uint64_t) time(NULL);

    dscan_wire_item_t* local_wires = NULL;
    char* local_paths = NULL;
    uint64_t local_path_bytes = 0;

    if (local_count > 0) {
        local_wires = (dscan_wire_item_t*) MFU_MALLOC((size_t)local_count * sizeof(dscan_wire_item_t));

        for (uint64_t i = 0; i < local_count; i++) {
            const char* path = mfu_flist_file_get_name(flist, i);
            uint64_t type = (uint64_t) mfu_flist_file_get_type(flist, i);
            uint64_t size = mfu_flist_file_get_size(flist, i);
            uint64_t atime = mfu_flist_file_get_atime(flist, i);
            uint64_t mtime = mfu_flist_file_get_mtime(flist, i);
            uint64_t ctime = mfu_flist_file_get_ctime(flist, i);

            uint64_t path_len = (uint64_t)strlen(path) + 1ULL;

            local_wires[i].type = type;
            local_wires[i].size = size;
            local_wires[i].atime = atime;
            local_wires[i].mtime = mtime;
            local_wires[i].ctime = ctime;
            local_wires[i].path_len = path_len;
            local_wires[i].broken_flags = compute_broken_flags(path, type, size, atime, mtime, ctime, now);

            local_path_bytes += path_len;
        }

        local_paths = (char*) MFU_MALLOC((size_t)local_path_bytes);

        uint64_t path_off = 0;
        for (uint64_t i = 0; i < local_count; i++) {
            const char* path = mfu_flist_file_get_name(flist, i);
            uint64_t path_len = local_wires[i].path_len;
            memcpy(local_paths + path_off, path, (size_t)path_len);
            path_off += path_len;
        }
    }

    int local_overflow = 0;
    if (local_count > (uint64_t)INT_MAX / sizeof(dscan_wire_item_t)) {
        local_overflow = 1;
    }
    if (local_path_bytes > (uint64_t)INT_MAX) {
        local_overflow = 1;
    }

    int global_overflow = 0;
    MPI_Allreduce(&local_overflow, &global_overflow, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_overflow != 0) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Data volume too large for MPI_Gatherv int counters");
        }
        rc = 1;
        mfu_free(&local_paths);
        mfu_free(&local_wires);
        goto cleanup_local;
    }

    uint64_t* all_item_counts = NULL;
    uint64_t* all_path_counts = NULL;

    if (rank == 0) {
        all_item_counts = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
        all_path_counts = (uint64_t*) MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    }

    MPI_Gather(&local_count, 1, MPI_UINT64_T, all_item_counts, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    MPI_Gather(&local_path_bytes, 1, MPI_UINT64_T, all_path_counts, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    int local_item_bytes = (int)(local_count * (uint64_t)sizeof(dscan_wire_item_t));
    int local_path_bytes_i = (int)local_path_bytes;

    dscan_wire_item_t* gathered_wires = NULL;
    char* gathered_paths = NULL;

    int* item_recvcounts = NULL;
    int* item_displs = NULL;
    int* path_recvcounts = NULL;
    int* path_displs = NULL;

    dscan_item_t* items = NULL;
    uint64_t* effective_sizes = NULL;

    uint64_t* candidate_indices = NULL;
    uint64_t* broken_indices = NULL;

    uint64_t* top_atime = NULL;
    uint64_t* top_mtime = NULL;
    uint64_t* top_ctime = NULL;
    uint64_t top_atime_count = 0;
    uint64_t top_mtime_count = 0;
    uint64_t top_ctime_count = 0;

    uint64_t size_hist[DSCAN_SIZE_BIN_COUNT];
    uint64_t atime_hist[DSCAN_TIME_BIN_COUNT];
    uint64_t mtime_hist[DSCAN_TIME_BIN_COUNT];
    uint64_t ctime_hist[DSCAN_TIME_BIN_COUNT];

    memset(size_hist, 0, sizeof(size_hist));
    memset(atime_hist, 0, sizeof(atime_hist));
    memset(mtime_hist, 0, sizeof(mtime_hist));
    memset(ctime_hist, 0, sizeof(ctime_hist));

    uint64_t total_items = 0;
    uint64_t total_files = 0;
    uint64_t total_dirs = 0;
    uint64_t total_links = 0;
    uint64_t total_other = 0;

    uint64_t candidate_count = 0;
    uint64_t broken_count = 0;

    if (rank == 0) {
        item_recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        item_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        path_recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
        path_displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

        uint64_t total_item_bytes = 0;
        uint64_t total_path_bytes = 0;

        for (int r = 0; r < ranks; r++) {
            uint64_t item_bytes_r = all_item_counts[r] * (uint64_t)sizeof(dscan_wire_item_t);
            if (item_bytes_r > (uint64_t)INT_MAX || total_item_bytes > (uint64_t)INT_MAX || total_item_bytes + item_bytes_r > (uint64_t)INT_MAX) {
                MFU_LOG(MFU_LOG_ERR, "Rank %d item data exceeds MPI int range for Gatherv", r);
                rc = 1;
                break;
            }

            if (all_path_counts[r] > (uint64_t)INT_MAX || total_path_bytes > (uint64_t)INT_MAX || total_path_bytes + all_path_counts[r] > (uint64_t)INT_MAX) {
                MFU_LOG(MFU_LOG_ERR, "Rank %d path data exceeds MPI int range for Gatherv", r);
                rc = 1;
                break;
            }

            item_displs[r] = (int)total_item_bytes;
            item_recvcounts[r] = (int)item_bytes_r;
            total_item_bytes += item_bytes_r;

            path_displs[r] = (int)total_path_bytes;
            path_recvcounts[r] = (int)all_path_counts[r];
            total_path_bytes += all_path_counts[r];

            total_items += all_item_counts[r];
        }

        if (rc == 0) {
            if (total_items > 0) {
                gathered_wires = (dscan_wire_item_t*) MFU_MALLOC((size_t)total_items * sizeof(dscan_wire_item_t));
            }
            if (total_path_bytes > 0) {
                gathered_paths = (char*) MFU_MALLOC((size_t)total_path_bytes);
            }
        }
    }

    MPI_Bcast(&rc, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (rc != 0) {
        mfu_free(&local_paths);
        mfu_free(&local_wires);
        goto cleanup_root_alloc;
    }

    MPI_Gatherv(
        local_wires,
        local_item_bytes,
        MPI_BYTE,
        gathered_wires,
        item_recvcounts,
        item_displs,
        MPI_BYTE,
        0,
        MPI_COMM_WORLD
    );

    MPI_Gatherv(
        local_paths,
        local_path_bytes_i,
        MPI_BYTE,
        gathered_paths,
        path_recvcounts,
        path_displs,
        MPI_BYTE,
        0,
        MPI_COMM_WORLD
    );

    mfu_free(&local_paths);
    mfu_free(&local_wires);

    if (rank == 0) {
        items = (total_items > 0) ? (dscan_item_t*) MFU_CALLOC((size_t)total_items, sizeof(dscan_item_t)) : NULL;

        uint64_t path_offset = 0;
        uint64_t gathered_total_path_bytes = 0;
        for (int r = 0; r < ranks; r++) {
            gathered_total_path_bytes += all_path_counts[r];
        }

        for (uint64_t i = 0; i < total_items; i++) {
            if (path_offset + gathered_wires[i].path_len > gathered_total_path_bytes) {
                rc = 1;
                MFU_LOG(MFU_LOG_ERR, "Corrupt gathered path buffer while rebuilding records");
                break;
            }

            items[i].path = MFU_STRDUP(gathered_paths + path_offset);
            items[i].type = gathered_wires[i].type;
            items[i].size = gathered_wires[i].size;
            items[i].atime = gathered_wires[i].atime;
            items[i].mtime = gathered_wires[i].mtime;
            items[i].ctime = gathered_wires[i].ctime;
            items[i].broken_flags = gathered_wires[i].broken_flags;

            path_offset += gathered_wires[i].path_len;
        }

        if (rc == 0) {
            effective_sizes = (total_items > 0) ? (uint64_t*) MFU_MALLOC((size_t)total_items * sizeof(uint64_t)) : NULL;
            for (uint64_t i = 0; i < total_items; i++) {
                effective_sizes[i] = items[i].size;
            }

            uint64_t dir_count = 0;
            for (uint64_t i = 0; i < total_items; i++) {
                if (items[i].type == MFU_TYPE_DIR) {
                    dir_count++;
                }
            }

            dscan_dir_agg_t* dir_aggs = NULL;
            int* dir_parents = NULL;
            uint64_t* dir_order = NULL;
            if (dir_count > 0) {
                dir_aggs = (dscan_dir_agg_t*) MFU_MALLOC((size_t)dir_count * sizeof(dscan_dir_agg_t));
                dir_parents = (int*) MFU_MALLOC((size_t)dir_count * sizeof(int));
                dir_order = (uint64_t*) MFU_MALLOC((size_t)dir_count * sizeof(uint64_t));

                uint64_t d = 0;
                for (uint64_t i = 0; i < total_items; i++) {
                    if (items[i].type == MFU_TYPE_DIR) {
                        dir_aggs[d].path = items[i].path;
                        dir_aggs[d].item_index = i;
                        dir_aggs[d].size = 0;
                        d++;
                    }
                }

                qsort(dir_aggs, (size_t)dir_count, sizeof(dscan_dir_agg_t), cmp_dir_agg_by_path);

                for (uint64_t i = 0; i < dir_count; i++) {
                    dir_order[i] = i;
                    dir_parents[i] = -1;

                    char* parent = get_parent_path_dup(dir_aggs[i].path);
                    while (parent != NULL) {
                        int parent_idx = find_dir_index(dir_aggs, dir_count, parent);
                        if (parent_idx >= 0 && parent_idx != (int)i) {
                            dir_parents[i] = parent_idx;
                            break;
                        }

                        char* next_parent = get_parent_path_dup(parent);
                        mfu_free(&parent);
                        parent = next_parent;
                    }
                    mfu_free(&parent);
                }

                for (uint64_t i = 0; i < total_items; i++) {
                    if (items[i].type != MFU_TYPE_FILE) {
                        continue;
                    }

                    char* parent = get_parent_path_dup(items[i].path);
                    while (parent != NULL) {
                        int idx = find_dir_index(dir_aggs, dir_count, parent);
                        if (idx >= 0) {
                            dir_aggs[idx].size += items[i].size;
                            break;
                        }

                        char* next_parent = get_parent_path_dup(parent);
                        mfu_free(&parent);
                        parent = next_parent;
                    }
                    mfu_free(&parent);
                }

                g_sort_dirs = dir_aggs;
                qsort(dir_order, (size_t)dir_count, sizeof(uint64_t), cmp_dir_index_by_path_len_desc);

                for (uint64_t i = 0; i < dir_count; i++) {
                    uint64_t child_idx = dir_order[i];
                    int parent_idx = dir_parents[child_idx];
                    if (parent_idx >= 0) {
                        dir_aggs[parent_idx].size += dir_aggs[child_idx].size;
                    }
                }

                for (uint64_t i = 0; i < dir_count; i++) {
                    effective_sizes[dir_aggs[i].item_index] = dir_aggs[i].size;
                }

                mfu_free(&dir_order);
                mfu_free(&dir_parents);
                mfu_free(&dir_aggs);
            }

            for (uint64_t i = 0; i < total_items; i++) {
                if (items[i].type == MFU_TYPE_FILE) {
                    total_files++;
                    uint64_t bin = size_hist_bin(items[i].size);
                    size_hist[bin]++;
                } else if (items[i].type == MFU_TYPE_DIR) {
                    total_dirs++;
                } else if (items[i].type == MFU_TYPE_LINK) {
                    total_links++;
                } else {
                    total_other++;
                }

                if (items[i].type == MFU_TYPE_FILE || items[i].type == MFU_TYPE_DIR) {
                    atime_hist[time_hist_bin(now, items[i].atime)]++;
                    mtime_hist[time_hist_bin(now, items[i].mtime)]++;
                    ctime_hist[time_hist_bin(now, items[i].ctime)]++;
                }

                if (items[i].type == MFU_TYPE_DIR) {
                    candidate_count++;
                }

                if (items[i].broken_flags != 0) {
                    broken_count++;
                }
            }

            candidate_indices = (candidate_count > 0) ? (uint64_t*) MFU_MALLOC((size_t)candidate_count * sizeof(uint64_t)) : NULL;
            broken_indices = (broken_count > 0) ? (uint64_t*) MFU_MALLOC((size_t)broken_count * sizeof(uint64_t)) : NULL;

            uint64_t cidx = 0;
            uint64_t bidx = 0;
            for (uint64_t i = 0; i < total_items; i++) {
                if (items[i].type == MFU_TYPE_DIR) {
                    candidate_indices[cidx++] = i;
                }
                if (items[i].broken_flags != 0) {
                    broken_indices[bidx++] = i;
                }
            }

            top_atime = build_topk(items, candidate_indices, candidate_count, top_k, DSCAN_TOP_ATIME, &top_atime_count);
            top_mtime = build_topk(items, candidate_indices, candidate_count, top_k, DSCAN_TOP_MTIME, &top_mtime_count);
            top_ctime = build_topk(items, candidate_indices, candidate_count, top_k, DSCAN_TOP_CTIME, &top_ctime_count);
        }

        if (rc == 0) {
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
                    items,
                    effective_sizes,
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
                    broken_indices,
                    broken_count,
                    past_limit,
                    future_limit) != 0)
            {
                MFU_LOG(MFU_LOG_ERR, "Failed to write output file: %s", output_file);
                rc = 1;
            }
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
                items,
                effective_sizes,
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
                broken_indices,
                broken_count);
        }
    }

    MPI_Bcast(&rc, 1, MPI_INT, 0, MPI_COMM_WORLD);

cleanup_root_alloc:
    mfu_free(&all_item_counts);
    mfu_free(&all_path_counts);

    mfu_free(&item_recvcounts);
    mfu_free(&item_displs);
    mfu_free(&path_recvcounts);
    mfu_free(&path_displs);

    mfu_free(&gathered_wires);
    mfu_free(&gathered_paths);

    mfu_free(&candidate_indices);
    mfu_free(&broken_indices);
    mfu_free(&top_atime);
    mfu_free(&top_mtime);
    mfu_free(&top_ctime);

    mfu_free(&effective_sizes);

    if (items != NULL) {
        for (uint64_t i = 0; i < total_items; i++) {
            mfu_free(&items[i].path);
        }
        mfu_free(&items);
    }

cleanup_local:
    mfu_flist_free(&flist);
    mfu_walk_opts_delete(&walk_opts);
    mfu_file_delete(&mfu_file);

    mfu_free(&directory);
    mfu_free(&output_file);

    mfu_finalize();
    MPI_Finalize();

    return rc;
}
