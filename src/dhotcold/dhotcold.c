#define _GNU_SOURCE

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <regex.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "mpi.h"
#include "mfu.h"
#include "libcircle.h"

#ifndef AT_NO_AUTOMOUNT
#define AT_NO_AUTOMOUNT 0
#endif

#define DHOTCOLD_HIST_BINS (5)
#define DHOTCOLD_PRINT_MAX_PATH_WIDTH (80)
#define DHOTCOLD_PRINT_COLS (5 + DHOTCOLD_HIST_BINS)

typedef enum {
    TIME_FIELD_ATIME = 0,
    TIME_FIELD_MTIME,
    TIME_FIELD_CTIME,
    TIME_FIELD_BTIME,
} dhotcold_time_field_t;

typedef enum {
    OUTPUT_UNIT_B = 0,
    OUTPUT_UNIT_KB,
    OUTPUT_UNIT_MB,
    OUTPUT_UNIT_GB,
    OUTPUT_UNIT_TB,
    OUTPUT_UNIT_PB,
} dhotcold_output_unit_t;

typedef struct {
    uint64_t total_bytes;
    uint64_t cold_bytes;
    uint64_t error_count;
    uint64_t hist_bytes[DHOTCOLD_HIST_BINS];
} dhotcold_stats_t;

typedef struct {
    uint64_t dev;
    uint64_t ino;
    uint64_t size;
    uint64_t age_days;
} inode_metric_t;

typedef struct {
    inode_metric_t* data;
    size_t count;
    size_t cap;
} inode_metric_vec_t;

typedef struct {
    char** data;
    size_t count;
    size_t cap;
} string_list_t;

typedef struct {
    char* directory_path;
    double total_out;
    double cold_out;
    double ratio;
    uint64_t error_count;
    double hist_out[DHOTCOLD_HIST_BINS];
} dhotcold_result_row_t;

typedef struct {
    dhotcold_result_row_t* data;
    size_t count;
    size_t cap;
} dhotcold_result_vec_t;

static void print_usage(void)
{
    printf("\n");
    printf("Usage: dhotcold [options] <path>\n");
    printf("\n");
    printf("Options:\n");
    printf("  -o, --output <file>       - write per-topdir analysis to file (overwrite if exists)\n");
    printf("  -d, --days <N>            - cold threshold in days (default 30)\n");
    printf("  -u, --unit <B|KB|MB|GB|TB|PB>\n");
    printf("                            - output capacity unit (default B)\n");
    printf("  -e, --exclude <regex>     - exclude depth-1 topdir by regex (depth-1 only)\n");
    printf("  -t, --time-field <atime|mtime|ctime|btime>\n");
    printf("                            - timestamp field used for cold classification (default atime)\n");
    printf("      --progress <N>        - print walk progress every N seconds (default 10)\n");
    printf("      --strict              - stop after first topdir with analysis errors\n");
    printf("      --xdev                - include only files on same filesystem as each topdir (default)\n");
    printf("      --follow-mounts       - disable --xdev behavior\n");
    printf("      --unique-inode        - dedup hardlinks by (st_dev, st_ino), global gather/sort\n");
    printf("      --print               - print aligned table to stdout on rank 0\n");
    printf("  -v, --verbose             - verbose output\n");
    printf("  -q, --quiet               - quiet output\n");
    printf("  -h, --help                - print usage\n");
    printf("\n");
    printf("Output file format (CSV):\n");
    printf("directory_path,total_capacity(<unit>),cold_capacity(<unit>),cold_ratio,error_count,histogram_capacity_0(<unit>),...,histogram_capacity_4(<unit>)\n");
    printf("\n");
    fflush(stdout);
}

static void stats_init(dhotcold_stats_t* stats)
{
    memset(stats, 0, sizeof(*stats));
}

static bool is_dot_or_dotdot(const char* name)
{
    return (strcmp(name, ".") == 0 || strcmp(name, "..") == 0);
}

static char* join_path(const char* dir, const char* name)
{
    size_t dirlen = strlen(dir);
    size_t namelen = strlen(name);
    int need_slash = (dirlen > 0 && dir[dirlen - 1] != '/');

    size_t len = dirlen + (size_t)need_slash + namelen + 1;
    char* path = (char*) MFU_MALLOC(len);
    snprintf(path, len, "%s%s%s", dir, need_slash ? "/" : "", name);
    return path;
}

static int parse_uint64(const char* text, uint64_t* val)
{
    errno = 0;
    char* end = NULL;
    unsigned long long tmp = strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }
    *val = (uint64_t) tmp;
    return 0;
}

static int parse_int_nonnegative(const char* text, int* val)
{
    errno = 0;
    char* end = NULL;
    long tmp = strtol(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || tmp < 0 || tmp > INT_MAX) {
        return -1;
    }
    *val = (int) tmp;
    return 0;
}

static int parse_output_unit(const char* text, dhotcold_output_unit_t* unit)
{
    if (strcasecmp(text, "B") == 0 || strcasecmp(text, "BYTE") == 0 || strcasecmp(text, "BYTES") == 0) {
        *unit = OUTPUT_UNIT_B;
        return 0;
    }
    if (strcasecmp(text, "KB") == 0) {
        *unit = OUTPUT_UNIT_KB;
        return 0;
    }
    if (strcasecmp(text, "MB") == 0) {
        *unit = OUTPUT_UNIT_MB;
        return 0;
    }
    if (strcasecmp(text, "GB") == 0) {
        *unit = OUTPUT_UNIT_GB;
        return 0;
    }
    if (strcasecmp(text, "TB") == 0) {
        *unit = OUTPUT_UNIT_TB;
        return 0;
    }
    if (strcasecmp(text, "PB") == 0) {
        *unit = OUTPUT_UNIT_PB;
        return 0;
    }
    return -1;
}

static int parse_time_field(const char* text, dhotcold_time_field_t* field)
{
    if (strcasecmp(text, "atime") == 0) {
        *field = TIME_FIELD_ATIME;
        return 0;
    }
    if (strcasecmp(text, "mtime") == 0) {
        *field = TIME_FIELD_MTIME;
        return 0;
    }
    if (strcasecmp(text, "ctime") == 0) {
        *field = TIME_FIELD_CTIME;
        return 0;
    }
    if (strcasecmp(text, "btime") == 0) {
        *field = TIME_FIELD_BTIME;
        return 0;
    }
    return -1;
}

static double output_unit_divisor(dhotcold_output_unit_t unit)
{
    switch (unit) {
        case OUTPUT_UNIT_KB:
            return 1024.0;
        case OUTPUT_UNIT_MB:
            return 1024.0 * 1024.0;
        case OUTPUT_UNIT_GB:
            return 1024.0 * 1024.0 * 1024.0;
        case OUTPUT_UNIT_TB:
            return 1024.0 * 1024.0 * 1024.0 * 1024.0;
        case OUTPUT_UNIT_PB:
            return 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0;
        case OUTPUT_UNIT_B:
        default:
            return 1.0;
    }
}

static double bytes_to_output_unit(uint64_t bytes, dhotcold_output_unit_t unit)
{
    double div = output_unit_divisor(unit);
    return (double) bytes / div;
}

static const char* output_unit_label(dhotcold_output_unit_t unit)
{
    switch (unit) {
        case OUTPUT_UNIT_KB:
            return "KB";
        case OUTPUT_UNIT_MB:
            return "MB";
        case OUTPUT_UNIT_GB:
            return "GB";
        case OUTPUT_UNIT_TB:
            return "TB";
        case OUTPUT_UNIT_PB:
            return "PB";
        case OUTPUT_UNIT_B:
        default:
            return "B";
    }
}

static void result_vec_init(dhotcold_result_vec_t* vec)
{
    vec->data = NULL;
    vec->count = 0;
    vec->cap = 0;
}

static void result_vec_free(dhotcold_result_vec_t* vec)
{
    if (vec == NULL) {
        return;
    }

    size_t i;
    for (i = 0; i < vec->count; i++) {
        mfu_free(&vec->data[i].directory_path);
    }
    mfu_free(&vec->data);
    vec->count = 0;
    vec->cap = 0;
}

static void result_vec_append(
    dhotcold_result_vec_t* vec,
    const char* path,
    double total_out,
    double cold_out,
    double ratio,
    uint64_t error_count,
    const double hist_out[DHOTCOLD_HIST_BINS])
{
    if (vec->count == vec->cap) {
        size_t new_cap = (vec->cap == 0) ? 64 : (vec->cap * 2);
        dhotcold_result_row_t* new_data = (dhotcold_result_row_t*) MFU_MALLOC(new_cap * sizeof(dhotcold_result_row_t));
        if (vec->count > 0) {
            memcpy(new_data, vec->data, vec->count * sizeof(dhotcold_result_row_t));
        }
        mfu_free(&vec->data);
        vec->data = new_data;
        vec->cap = new_cap;
    }

    dhotcold_result_row_t* row = &vec->data[vec->count];
    row->directory_path = MFU_STRDUP(path);
    row->total_out = total_out;
    row->cold_out = cold_out;
    row->ratio = ratio;
    row->error_count = error_count;
    int b;
    for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
        row->hist_out[b] = hist_out[b];
    }
    vec->count++;
}

static void csv_write_escaped(FILE* fp, const char* text)
{
    const char* s = (text != NULL) ? text : "";
    fputc('"', fp);
    while (*s != '\0') {
        if (*s == '"') {
            fputc('"', fp);
        }
        fputc(*s, fp);
        s++;
    }
    fputc('"', fp);
}

static int write_csv_header(FILE* fp, dhotcold_output_unit_t unit)
{
    const char* u = output_unit_label(unit);
    if (fprintf(fp, "directory_path,total_capacity(%s),cold_capacity(%s),cold_ratio,error_count",
                u, u) < 0) {
        return -1;
    }

    int b;
    for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
        if (fprintf(fp, ",histogram_capacity_%d(%s)", b, u) < 0) {
            return -1;
        }
    }
    if (fputc('\n', fp) == EOF) {
        return -1;
    }
    if (fflush(fp) != 0) {
        return -1;
    }
    return 0;
}

static int write_csv_row(FILE* fp, const dhotcold_result_row_t* row)
{
    csv_write_escaped(fp, row->directory_path);
    if (fprintf(fp, ",%.6f,%.6f,%.10f,%" PRIu64,
                row->total_out, row->cold_out, row->ratio, row->error_count) < 0) {
        return -1;
    }

    int b;
    for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
        if (fprintf(fp, ",%.6f", row->hist_out[b]) < 0) {
            return -1;
        }
    }

    if (fputc('\n', fp) == EOF) {
        return -1;
    }
    return 0;
}

static size_t max_size(size_t a, size_t b)
{
    return (a > b) ? a : b;
}

static void print_repeated_char(char ch, size_t count)
{
    size_t i;
    for (i = 0; i < count; i++) {
        putchar(ch);
    }
}

static void print_table_border(const size_t widths[DHOTCOLD_PRINT_COLS])
{
    putchar('+');
    int c;
    for (c = 0; c < DHOTCOLD_PRINT_COLS; c++) {
        print_repeated_char('-', widths[c] + 2);
        putchar('+');
    }
    putchar('\n');
}

static void print_table_row(const char* cols[DHOTCOLD_PRINT_COLS], const size_t widths[DHOTCOLD_PRINT_COLS])
{
    int c;
    putchar('|');
    for (c = 0; c < DHOTCOLD_PRINT_COLS; c++) {
        const char* text = (cols[c] != NULL) ? cols[c] : "";
        size_t len = strlen(text);
        printf(" %s", text);
        if (widths[c] > len) {
            print_repeated_char(' ', widths[c] - len);
        }
        printf(" |");
    }
    putchar('\n');
}

static void format_path_for_table(const char* path, size_t width, char* out, size_t out_size)
{
    const char* text = (path != NULL) ? path : "";
    size_t len = strlen(text);

    if (width == 0 || out_size == 0) {
        return;
    }

    if (len <= width) {
        snprintf(out, out_size, "%s", text);
        return;
    }

    if (width <= 3) {
        snprintf(out, out_size, "%.*s", (int)width, text);
        return;
    }

    snprintf(out, out_size, "%.*s...", (int)(width - 3), text);
}

static void print_results_table(const dhotcold_result_vec_t* rows, dhotcold_output_unit_t unit)
{
    const char* u = output_unit_label(unit);

    char header_total[32];
    char header_cold[32];
    char header_hist[DHOTCOLD_HIST_BINS][48];
    snprintf(header_total, sizeof(header_total), "Total(%s)", u);
    snprintf(header_cold, sizeof(header_cold), "Cold(%s)", u);

    int b;
    for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
        snprintf(header_hist[b], sizeof(header_hist[b]), "Hist%d(%s)", b, u);
    }

    const char* headers[DHOTCOLD_PRINT_COLS];
    headers[0] = "Directory";
    headers[1] = header_total;
    headers[2] = header_cold;
    headers[3] = "ColdRatio";
    headers[4] = "Errors";
    for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
        headers[5 + b] = header_hist[b];
    }

    size_t widths[DHOTCOLD_PRINT_COLS];
    int c;
    for (c = 0; c < DHOTCOLD_PRINT_COLS; c++) {
        widths[c] = strlen(headers[c]);
    }

    char tmp[64];
    size_t i;
    for (i = 0; i < rows->count; i++) {
        const dhotcold_result_row_t* row = &rows->data[i];
        size_t plen = strlen(row->directory_path);
        if (plen > DHOTCOLD_PRINT_MAX_PATH_WIDTH) {
            plen = DHOTCOLD_PRINT_MAX_PATH_WIDTH;
        }
        widths[0] = max_size(widths[0], plen);

        snprintf(tmp, sizeof(tmp), "%.6f", row->total_out);
        widths[1] = max_size(widths[1], strlen(tmp));

        snprintf(tmp, sizeof(tmp), "%.6f", row->cold_out);
        widths[2] = max_size(widths[2], strlen(tmp));

        snprintf(tmp, sizeof(tmp), "%.10f", row->ratio);
        widths[3] = max_size(widths[3], strlen(tmp));

        snprintf(tmp, sizeof(tmp), "%" PRIu64, row->error_count);
        widths[4] = max_size(widths[4], strlen(tmp));

        for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
            snprintf(tmp, sizeof(tmp), "%.6f", row->hist_out[b]);
            widths[5 + b] = max_size(widths[5 + b], strlen(tmp));
        }
    }

    printf("\n");
    print_table_border(widths);
    print_table_row(headers, widths);
    print_table_border(widths);

    for (i = 0; i < rows->count; i++) {
        const dhotcold_result_row_t* row = &rows->data[i];
        char path_buf[DHOTCOLD_PRINT_MAX_PATH_WIDTH + 4];
        char total_buf[64];
        char cold_buf[64];
        char ratio_buf[64];
        char err_buf[64];
        char hist_buf[DHOTCOLD_HIST_BINS][64];

        format_path_for_table(row->directory_path, widths[0], path_buf, sizeof(path_buf));
        snprintf(total_buf, sizeof(total_buf), "%.6f", row->total_out);
        snprintf(cold_buf, sizeof(cold_buf), "%.6f", row->cold_out);
        snprintf(ratio_buf, sizeof(ratio_buf), "%.10f", row->ratio);
        snprintf(err_buf, sizeof(err_buf), "%" PRIu64, row->error_count);
        for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
            snprintf(hist_buf[b], sizeof(hist_buf[b]), "%.6f", row->hist_out[b]);
        }

        const char* cols[DHOTCOLD_PRINT_COLS];
        cols[0] = path_buf;
        cols[1] = total_buf;
        cols[2] = cold_buf;
        cols[3] = ratio_buf;
        cols[4] = err_buf;
        for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
            cols[5 + b] = hist_buf[b];
        }

        print_table_row(cols, widths);
    }

    print_table_border(widths);
    printf("\n");
}

static void string_list_init(string_list_t* list)
{
    list->data = NULL;
    list->count = 0;
    list->cap = 0;
}

static void string_list_free(string_list_t* list)
{
    if (list == NULL) {
        return;
    }

    size_t i;
    for (i = 0; i < list->count; i++) {
        mfu_free(&list->data[i]);
    }
    mfu_free(&list->data);
    list->count = 0;
    list->cap = 0;
}

static void string_list_append(string_list_t* list, const char* value)
{
    if (list->count == list->cap) {
        size_t new_cap = (list->cap == 0) ? 16 : list->cap * 2;
        char** new_data = (char**) MFU_MALLOC(new_cap * sizeof(char*));
        if (list->count > 0) {
            memcpy(new_data, list->data, list->count * sizeof(char*));
        }
        mfu_free(&list->data);
        list->data = new_data;
        list->cap = new_cap;
    }

    list->data[list->count] = MFU_STRDUP(value);
    list->count++;
}

static void inode_vec_init(inode_metric_vec_t* vec)
{
    vec->data = NULL;
    vec->count = 0;
    vec->cap = 0;
}

static void inode_vec_free(inode_metric_vec_t* vec)
{
    if (vec == NULL) {
        return;
    }
    mfu_free(&vec->data);
    vec->count = 0;
    vec->cap = 0;
}

static void inode_vec_append(inode_metric_vec_t* vec, const inode_metric_t* item)
{
    if (vec->count == vec->cap) {
        size_t new_cap = (vec->cap == 0) ? 1024 : vec->cap * 2;
        inode_metric_t* new_data = (inode_metric_t*) MFU_MALLOC(new_cap * sizeof(inode_metric_t));
        if (vec->count > 0) {
            memcpy(new_data, vec->data, vec->count * sizeof(inode_metric_t));
        }
        mfu_free(&vec->data);
        vec->data = new_data;
        vec->cap = new_cap;
    }

    vec->data[vec->count] = *item;
    vec->count++;
}

static int inode_metric_cmp(const void* a, const void* b)
{
    const inode_metric_t* x = (const inode_metric_t*) a;
    const inode_metric_t* y = (const inode_metric_t*) b;

    if (x->dev < y->dev) {
        return -1;
    }
    if (x->dev > y->dev) {
        return 1;
    }
    if (x->ino < y->ino) {
        return -1;
    }
    if (x->ino > y->ino) {
        return 1;
    }
    return 0;
}

static uint64_t compute_age_days(uint64_t now_secs, uint64_t file_secs)
{
    if (file_secs >= now_secs) {
        return 0;
    }
    uint64_t delta = now_secs - file_secs;
    return delta / (24ULL * 3600ULL);
}

static void stats_add_record(dhotcold_stats_t* stats, uint64_t size, uint64_t age_days, uint64_t cold_days)
{
    stats->total_bytes += size;

    if (age_days >= cold_days) {
        stats->cold_bytes += size;

        uint64_t span_days = (cold_days == 0) ? 1 : cold_days;
        uint64_t bucket_width = (span_days + (uint64_t)DHOTCOLD_HIST_BINS - 1) / (uint64_t)DHOTCOLD_HIST_BINS;
        if (bucket_width == 0) {
            bucket_width = 1;
        }

        uint64_t delta = age_days - cold_days;
        uint64_t bucket = delta / bucket_width;
        if (bucket >= (uint64_t)DHOTCOLD_HIST_BINS) {
            bucket = (uint64_t)DHOTCOLD_HIST_BINS - 1;
        }

        stats->hist_bytes[bucket] += size;
    }
}

static int get_selected_time(
    const char* path,
    const struct stat* st,
    dhotcold_time_field_t field,
    uint64_t* secs,
    uint64_t* nsecs)
{
    switch (field) {
        case TIME_FIELD_ATIME:
            mfu_stat_get_atimes(st, secs, nsecs);
            return 0;
        case TIME_FIELD_MTIME:
            mfu_stat_get_mtimes(st, secs, nsecs);
            return 0;
        case TIME_FIELD_CTIME:
            mfu_stat_get_ctimes(st, secs, nsecs);
            return 0;
        case TIME_FIELD_BTIME:
#ifdef __linux__
            {
                struct statx stx;
                memset(&stx, 0, sizeof(stx));
                int rc = statx(AT_FDCWD, path, AT_SYMLINK_NOFOLLOW | AT_NO_AUTOMOUNT, STATX_BTIME, &stx);
                if (rc != 0) {
                    return -1;
                }
                if ((stx.stx_mask & STATX_BTIME) == 0) {
                    return -1;
                }
                *secs = (uint64_t) stx.stx_btime.tv_sec;
                *nsecs = (uint64_t) stx.stx_btime.tv_nsec;
                return 0;
            }
#else
            (void) path;
            (void) st;
            return -1;
#endif
        default:
            return -1;
    }
}

static void reduce_stats(const dhotcold_stats_t* local, dhotcold_stats_t* global)
{
    uint64_t local_vals[3 + DHOTCOLD_HIST_BINS];
    uint64_t global_vals[3 + DHOTCOLD_HIST_BINS];

    local_vals[0] = local->total_bytes;
    local_vals[1] = local->cold_bytes;
    local_vals[2] = local->error_count;

    int i;
    for (i = 0; i < DHOTCOLD_HIST_BINS; i++) {
        local_vals[3 + i] = local->hist_bytes[i];
    }

    MPI_Allreduce(local_vals, global_vals, 3 + DHOTCOLD_HIST_BINS, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    global->total_bytes = global_vals[0];
    global->cold_bytes = global_vals[1];
    global->error_count = global_vals[2];
    for (i = 0; i < DHOTCOLD_HIST_BINS; i++) {
        global->hist_bytes[i] = global_vals[3 + i];
    }
}

static int enumerate_topdirs(
    const char* root,
    int use_exclude,
    const regex_t* exclude_re,
    string_list_t* topdirs,
    uint64_t* error_count)
{
    DIR* dir = opendir(root);
    if (dir == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open `%s' (errno=%d %s)", root, errno, strerror(errno));
        return -1;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        if (is_dot_or_dotdot(entry->d_name)) {
            continue;
        }

        char* child = join_path(root, entry->d_name);

        struct stat st;
        if (lstat(child, &st) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to stat depth-1 entry `%s' (errno=%d %s)",
                    child, errno, strerror(errno));
            (*error_count)++;
            mfu_free(&child);
            continue;
        }

        if (!S_ISDIR(st.st_mode)) {
            mfu_free(&child);
            continue;
        }

        if (use_exclude && regexec(exclude_re, child, 0, NULL, 0) == 0) {
            mfu_free(&child);
            continue;
        }

        string_list_append(topdirs, child);
        mfu_free(&child);
    }

    closedir(dir);
    return 0;
}

/* globals for streaming topdir analysis */
static const char* stream_topdir = NULL;
static mfu_file_t* stream_mfu_file = NULL;
static dhotcold_stats_t* stream_stats = NULL;
static dhotcold_time_field_t stream_time_field = TIME_FIELD_ATIME;
static uint64_t stream_now_secs = 0;
static uint64_t stream_cold_days = 0;
static int stream_xdev_enabled = 0;
static int stream_have_top_dev = 0;
static dev_t stream_top_dev = 0;
static int stream_result = 0;
static double stream_reduce_start = 0.0;
static uint64_t stream_reduce_items = 0;

static int stream_build_path(char* path, size_t path_len, const char* dir, const char* name)
{
    size_t dir_len = strlen(dir);
    int new_len;
    if ((dir_len > 0) && dir[dir_len - 1] == '/') {
        new_len = snprintf(path, path_len, "%s%s", dir, name);
    } else {
        new_len = snprintf(path, path_len, "%s/%s", dir, name);
    }

    if (new_len < 0 || (size_t)new_len >= path_len) {
        return -1;
    }
    return 0;
}

static void stream_enqueue_dir_entries(const char* dir, CIRCLE_handle* handle)
{
    DIR* dirp = mfu_file_opendir(dir, stream_mfu_file);
    if (dirp == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open directory `%s' (errno=%d %s)",
                dir, errno, strerror(errno));
        stream_stats->error_count++;
        stream_result = -1;
        return;
    }

    while (1) {
        errno = 0;
        struct dirent* entry = mfu_file_readdir(dirp, stream_mfu_file);
        if (entry == NULL) {
            if (errno != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read directory `%s' (errno=%d %s)",
                        dir, errno, strerror(errno));
                stream_stats->error_count++;
                stream_result = -1;
            }
            break;
        }

        if (is_dot_or_dotdot(entry->d_name)) {
            continue;
        }

        char newpath[CIRCLE_MAX_STRING_LEN];
        if (stream_build_path(newpath, CIRCLE_MAX_STRING_LEN, dir, entry->d_name) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Path name is too long: `%s/%s'", dir, entry->d_name);
            stream_stats->error_count++;
            stream_result = -1;
            continue;
        }
        handle->enqueue(newpath);
    }

    mfu_file_closedir(dirp, stream_mfu_file);
}

static void stream_walk_create(CIRCLE_handle* handle)
{
    handle->enqueue((char*)stream_topdir);
}

static void stream_walk_process(CIRCLE_handle* handle)
{
    char path[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(path);

    struct stat st;
    if (mfu_file_lstat(path, &st, stream_mfu_file) != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to stat `%s' (errno=%d %s)", path, errno, strerror(errno));
        stream_stats->error_count++;
        stream_result = -1;
        return;
    }

    stream_reduce_items++;

    if (stream_xdev_enabled && stream_have_top_dev && st.st_dev != stream_top_dev) {
        return;
    }

    if (S_ISDIR(st.st_mode)) {
        stream_enqueue_dir_entries(path, handle);
        return;
    }

    if (!S_ISREG(st.st_mode)) {
        return;
    }

    uint64_t tsec, tnsec;
    if (get_selected_time(path, &st, stream_time_field, &tsec, &tnsec) != 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to read %s for `%s'",
                (stream_time_field == TIME_FIELD_BTIME) ? "btime" : "time", path);
        stream_stats->error_count++;
        stream_result = -1;
        return;
    }
    (void) tnsec;

    uint64_t age_days = compute_age_days(stream_now_secs, tsec);
    uint64_t size = (uint64_t) st.st_size;
    stats_add_record(stream_stats, size, age_days, stream_cold_days);
}

static void stream_reduce_init(void)
{
    CIRCLE_reduce(&stream_reduce_items, sizeof(stream_reduce_items));
}

static void stream_reduce_exec(const void* buf1, size_t size1, const void* buf2, size_t size2)
{
    (void) size1;
    (void) size2;
    const uint64_t* a = (const uint64_t*) buf1;
    const uint64_t* b = (const uint64_t*) buf2;
    uint64_t val = a[0] + b[0];
    CIRCLE_reduce(&val, sizeof(val));
}

static void stream_reduce_fini(const void* buf, size_t size)
{
    (void) size;
    const uint64_t* a = (const uint64_t*) buf;
    uint64_t val = a[0];

    double now = MPI_Wtime();
    double secs = now - stream_reduce_start;
    double rate = 0.0;
    if (secs > 0.0) {
        rate = (double) val / secs;
    }

    MFU_LOG(MFU_LOG_INFO, "Scanned %" PRIu64 " items in %.3lf secs (%.3lf items/sec) ...",
            val, secs, rate);
}

static int analyze_topdir_streaming(
    const char* topdir,
    mfu_file_t* mfu_file,
    uint64_t now_secs,
    uint64_t cold_days,
    dhotcold_time_field_t time_field,
    int xdev_enabled,
    dhotcold_stats_t* out)
{
    stats_init(out);

    dev_t top_dev = 0;
    int have_top_dev = 0;
    if (xdev_enabled) {
        struct stat top_st;
        if (lstat(topdir, &top_st) == 0) {
            top_dev = top_st.st_dev;
            have_top_dev = 1;
        } else {
            MFU_LOG(MFU_LOG_ERR, "Failed to stat topdir `%s' (errno=%d %s)",
                    topdir, errno, strerror(errno));
            out->error_count++;
        }
    }

    stream_topdir = topdir;
    stream_mfu_file = mfu_file;
    stream_stats = out;
    stream_time_field = time_field;
    stream_now_secs = now_secs;
    stream_cold_days = cold_days;
    stream_xdev_enabled = xdev_enabled;
    stream_have_top_dev = have_top_dev;
    stream_top_dev = top_dev;
    stream_result = 0;
    stream_reduce_items = 0;

    double start_walk = MPI_Wtime();
    stream_reduce_start = start_walk;

    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_TERM_TREE);
    CIRCLE_enable_logging(CIRCLE_LOG_WARN);
    CIRCLE_cb_create(&stream_walk_create);
    CIRCLE_cb_process(&stream_walk_process);
    CIRCLE_cb_reduce_init(&stream_reduce_init);
    CIRCLE_cb_reduce_op(&stream_reduce_exec);
    CIRCLE_cb_reduce_fini(&stream_reduce_fini);

    int reduce_secs = 0;
    if (mfu_progress_timeout > 0) {
        reduce_secs = mfu_progress_timeout;
    }
    CIRCLE_set_reduce_period(reduce_secs);

    CIRCLE_begin();
    CIRCLE_finalize();

    uint64_t local_items = stream_reduce_items;
    uint64_t global_items = 0;
    MPI_Allreduce(&local_items, &global_items, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        double time_diff = MPI_Wtime() - start_walk;
        double rate = 0.0;
        if (time_diff > 0.0) {
            rate = (double) global_items / time_diff;
        }
        MFU_LOG(MFU_LOG_INFO, "Scanned %" PRIu64 " items in %.3lf seconds (%.3lf items/sec)",
                global_items, time_diff, rate);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    int all_rc = 0;
    MPI_Allreduce(&stream_result, &all_rc, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    return all_rc;
}

static int analyze_topdir_with_flist(
    const char* topdir,
    mfu_file_t* mfu_file,
    mfu_walk_opts_t* walk_opts,
    uint64_t now_secs,
    uint64_t cold_days,
    dhotcold_time_field_t time_field,
    int xdev_enabled,
    int unique_inode,
    dhotcold_stats_t* out)
{
    stats_init(out);

    mfu_flist flist = mfu_flist_new();
    int walk_rc = mfu_flist_walk_path(topdir, walk_opts, flist, mfu_file);
    if (walk_rc != 0) {
        out->error_count++;
    }

    dev_t top_dev = 0;
    int have_top_dev = 0;
    if (xdev_enabled) {
        struct stat top_st;
        if (lstat(topdir, &top_st) == 0) {
            top_dev = top_st.st_dev;
            have_top_dev = 1;
        } else {
            MFU_LOG(MFU_LOG_ERR, "Failed to stat topdir `%s' (errno=%d %s)",
                    topdir, errno, strerror(errno));
            out->error_count++;
        }
    }

    inode_metric_vec_t metrics;
    inode_vec_init(&metrics);

    uint64_t count = mfu_flist_size(flist);
    uint64_t idx;
    for (idx = 0; idx < count; idx++) {
        mode_t mode = (mode_t) mfu_flist_file_get_mode(flist, idx);
        if (!S_ISREG(mode)) {
            continue;
        }

        const char* path = mfu_flist_file_get_name(flist, idx);

        struct stat st;
        if (lstat(path, &st) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to stat `%s' (errno=%d %s)", path, errno, strerror(errno));
            out->error_count++;
            continue;
        }

        if (!S_ISREG(st.st_mode)) {
            continue;
        }

        if (xdev_enabled && have_top_dev && st.st_dev != top_dev) {
            continue;
        }

        uint64_t tsec, tnsec;
        if (get_selected_time(path, &st, time_field, &tsec, &tnsec) != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to read %s for `%s'", (time_field == TIME_FIELD_BTIME) ? "btime" : "time", path);
            out->error_count++;
            continue;
        }

        (void) tnsec;
        uint64_t age_days = compute_age_days(now_secs, tsec);
        uint64_t size = (uint64_t) st.st_size;

        if (unique_inode) {
            inode_metric_t rec;
            rec.dev = (uint64_t) st.st_dev;
            rec.ino = (uint64_t) st.st_ino;
            rec.size = size;
            rec.age_days = age_days;
            inode_vec_append(&metrics, &rec);
        } else {
            stats_add_record(out, size, age_days, cold_days);
        }
    }

    if (unique_inode) {
        int rank, ranks;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        MPI_Comm_size(MPI_COMM_WORLD, &ranks);

        int local_bytes = 0;
        if (metrics.count > (size_t)INT_MAX / sizeof(inode_metric_t)) {
            local_bytes = -1;
        } else {
            local_bytes = (int)(metrics.count * sizeof(inode_metric_t));
        }

        int local_size_ok = (local_bytes >= 0) ? 1 : 0;
        int all_size_ok = 0;
        MPI_Allreduce(&local_size_ok, &all_size_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);

        if (!all_size_ok) {
            /* fallback to rank-local dedup if global gather cannot be represented in MPI int counts */
            out->error_count++;

            qsort(metrics.data, metrics.count, sizeof(inode_metric_t), inode_metric_cmp);
            size_t i;
            for (i = 0; i < metrics.count; i++) {
                if (i > 0 &&
                    metrics.data[i].dev == metrics.data[i - 1].dev &&
                    metrics.data[i].ino == metrics.data[i - 1].ino) {
                    continue;
                }
                stats_add_record(out, metrics.data[i].size, metrics.data[i].age_days, cold_days);
            }
        } else {
            int* recvcounts = NULL;
            int* displs = NULL;
            inode_metric_t* gathered = NULL;
            int total_bytes = 0;

            if (rank == 0) {
                recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
            }

            MPI_Gather(&local_bytes, 1, MPI_INT, recvcounts, 1, MPI_INT, 0, MPI_COMM_WORLD);

            if (rank == 0) {
                displs = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
                displs[0] = 0;
                total_bytes = recvcounts[0];
                int r;
                for (r = 1; r < ranks; r++) {
                    displs[r] = displs[r - 1] + recvcounts[r - 1];
                    total_bytes += recvcounts[r];
                }

                if (total_bytes > 0) {
                    gathered = (inode_metric_t*) MFU_MALLOC((size_t)total_bytes);
                }
            }

            MPI_Gatherv(
                metrics.data, local_bytes, MPI_BYTE,
                gathered, recvcounts, displs, MPI_BYTE,
                0, MPI_COMM_WORLD
            );

            if (rank == 0 && total_bytes > 0) {
                size_t total_records = (size_t)total_bytes / sizeof(inode_metric_t);
                qsort(gathered, total_records, sizeof(inode_metric_t), inode_metric_cmp);

                size_t unique_records = 0;
                size_t i;
                for (i = 0; i < total_records; i++) {
                    if (i > 0 &&
                        gathered[i].dev == gathered[i - 1].dev &&
                        gathered[i].ino == gathered[i - 1].ino) {
                        continue;
                    }
                    unique_records++;
                    stats_add_record(out, gathered[i].size, gathered[i].age_days, cold_days);
                }

                if (mfu_debug_level >= MFU_LOG_VERBOSE) {
                    MFU_LOG(MFU_LOG_INFO, "Unique inode reduction for `%s': records=%zu unique=%zu",
                            topdir, total_records, unique_records);
                }
            }

            if (rank == 0) {
                mfu_free(&gathered);
                mfu_free(&displs);
                mfu_free(&recvcounts);
            }
        }
    }

    inode_vec_free(&metrics);
    mfu_flist_free(&flist);
    return 0;
}

static int analyze_topdir(
    const char* topdir,
    mfu_file_t* mfu_file,
    mfu_walk_opts_t* walk_opts,
    uint64_t now_secs,
    uint64_t cold_days,
    dhotcold_time_field_t time_field,
    int xdev_enabled,
    int unique_inode,
    dhotcold_stats_t* out)
{
    if (unique_inode) {
        return analyze_topdir_with_flist(
            topdir, mfu_file, walk_opts,
            now_secs, cold_days, time_field,
            xdev_enabled, unique_inode, out
        );
    }

    (void) walk_opts;
    return analyze_topdir_streaming(
        topdir, mfu_file,
        now_secs, cold_days, time_field, xdev_enabled, out
    );
}

static FILE* create_output_file_rank0(const char* output)
{
    int fd = open(output, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open output file `%s' (errno=%d %s)", output, errno, strerror(errno));
        return NULL;
    }

    FILE* fp = fdopen(fd, "w");
    if (fp == NULL) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open stream for output file `%s' (errno=%d %s)", output, errno, strerror(errno));
        close(fd);
        return NULL;
    }

    return fp;
}

int main(int argc, char** argv)
{
    int i;
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            const char* env_ompi_rank = getenv("OMPI_COMM_WORLD_RANK");
            const char* env_pmi_rank = getenv("PMI_RANK");
            if ((env_ompi_rank == NULL || strcmp(env_ompi_rank, "0") == 0) &&
                (env_pmi_rank == NULL || strcmp(env_pmi_rank, "0") == 0)) {
                print_usage();
            }
            return 0;
        }
    }

    int rc = 0;

    MPI_Init(&argc, &argv);
    mfu_init();

    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    mfu_debug_level = MFU_LOG_VERBOSE;

    char* output = NULL;
    char* exclude_regex = NULL;
    uint64_t cold_days = 30;
    dhotcold_output_unit_t output_unit = OUTPUT_UNIT_B;
    dhotcold_time_field_t time_field = TIME_FIELD_ATIME;
    int strict = 0;
    int xdev = 1;
    int unique_inode = 0;
    int print_table = 0;
    int progress = mfu_progress_timeout;

    int usage = 0;
    int help_requested = 0;
    int option_index = 0;
    static struct option long_options[] = {
        {"output",        1, 0, 'o'},
        {"days",          1, 0, 'd'},
        {"unit",          1, 0, 'u'},
        {"exclude",       1, 0, 'e'},
        {"time-field",    1, 0, 't'},
        {"progress",      1, 0, 'p'},
        {"strict",        0, 0, 's'},
        {"xdev",          0, 0, 'x'},
        {"follow-mounts", 0, 0, 'm'},
        {"unique-inode",  0, 0, 'i'},
        {"print",         0, 0, 'P'},
        {"verbose",       0, 0, 'v'},
        {"quiet",         0, 0, 'q'},
        {"help",          0, 0, 'h'},
        {0, 0, 0, 0}
    };

    while (1) {
        int c = getopt_long(argc, argv, "o:d:u:e:t:p:sxmiPvqh", long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'o':
                mfu_free(&output);
                output = MFU_STRDUP(optarg);
                break;
            case 'd':
                if (parse_uint64(optarg, &cold_days) != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Invalid --days value: %s", optarg);
                    }
                    usage = 1;
                }
                break;
            case 'u':
                if (parse_output_unit(optarg, &output_unit) != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Invalid --unit value: %s", optarg);
                    }
                    usage = 1;
                }
                break;
            case 'e':
                mfu_free(&exclude_regex);
                exclude_regex = MFU_STRDUP(optarg);
                break;
            case 't':
                if (parse_time_field(optarg, &time_field) != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Invalid --time-field value: %s", optarg);
                    }
                    usage = 1;
                }
                break;
            case 'p':
                if (parse_int_nonnegative(optarg, &progress) != 0) {
                    if (rank == 0) {
                        MFU_LOG(MFU_LOG_ERR, "Invalid --progress value: %s", optarg);
                    }
                    usage = 1;
                }
                break;
            case 's':
                strict = 1;
                break;
            case 'x':
                xdev = 1;
                break;
            case 'm':
                xdev = 0;
                break;
            case 'i':
                unique_inode = 1;
                break;
            case 'P':
                print_table = 1;
                break;
            case 'v':
                mfu_debug_level = MFU_LOG_VERBOSE;
                break;
            case 'q':
                mfu_debug_level = 0;
                break;
            case 'h':
                usage = 1;
                help_requested = 1;
                break;
            case '?':
            default:
                usage = 1;
                break;
        }
    }

#ifndef __linux__
    if (time_field == TIME_FIELD_BTIME) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "--time-field=btime is only supported on Linux/statx builds");
        }
        usage = 1;
    }
#endif

    if ((argc - optind) != 1) {
        usage = 1;
    }

    if (!help_requested && output == NULL) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "--output is required");
        }
        usage = 1;
    }

    if (usage) {
        if (rank == 0) {
            print_usage();
        }
        mfu_free(&exclude_regex);
        mfu_free(&output);
        mfu_finalize();
        MPI_Finalize();
        return help_requested ? 0 : 1;
    }

    mfu_progress_timeout = progress;

    int is_root = (geteuid() == 0) ? 1 : 0;
    int all_root = 0;
    MPI_Allreduce(&is_root, &all_root, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_root) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "dhotcold must run as root on all ranks");
        }
        mfu_free(&exclude_regex);
        mfu_free(&output);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    mfu_file_t* mfu_file = mfu_file_new();

    const char* in_root = argv[optind];
    mfu_param_path root_param;
    memset(&root_param, 0, sizeof(root_param));

    mfu_param_path_set_all(1, &in_root, &root_param, mfu_file, true);

    int root_valid = 1;
    if (!root_param.path_stat_valid) {
        root_valid = 0;
    } else if (!S_ISDIR(root_param.path_stat.st_mode)) {
        root_valid = 0;
    }

    int all_root_valid = 0;
    MPI_Allreduce(&root_valid, &all_root_valid, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (!all_root_valid) {
        if (rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Input path must exist and be a directory: %s", in_root);
        }
        mfu_param_path_free_all(1, &root_param);
        mfu_file_delete(&mfu_file);
        mfu_free(&exclude_regex);
        mfu_free(&output);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    FILE* out_fp = NULL;
    int output_ok = 1;
    if (rank == 0) {
        out_fp = create_output_file_rank0(output);
        if (out_fp == NULL) {
            output_ok = 0;
        } else {
            if (write_csv_header(out_fp, output_unit) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write CSV header to `%s'", output);
                fclose(out_fp);
                out_fp = NULL;
                output_ok = 0;
            }
        }
    }
    MPI_Bcast(&output_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!output_ok) {
        if (out_fp != NULL) {
            fclose(out_fp);
        }
        mfu_param_path_free_all(1, &root_param);
        mfu_file_delete(&mfu_file);
        mfu_free(&exclude_regex);
        mfu_free(&output);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    uint64_t now_secs = 0;
    if (rank == 0) {
        now_secs = (uint64_t) time(NULL);
    }
    MPI_Bcast(&now_secs, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    string_list_t topdirs;
    string_list_init(&topdirs);

    regex_t exclude_re;
    int use_exclude = 0;
    int regex_ok = 1;
    if (rank == 0 && exclude_regex != NULL) {
        int re_rc = regcomp(&exclude_re, exclude_regex, REG_EXTENDED | REG_NOSUB);
        if (re_rc != 0) {
            char msg[256];
            regerror(re_rc, &exclude_re, msg, sizeof(msg));
            MFU_LOG(MFU_LOG_ERR, "Invalid exclude regex `%s': %s", exclude_regex, msg);
            regex_ok = 0;
        } else {
            use_exclude = 1;
        }
    }
    MPI_Bcast(&regex_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!regex_ok) {
        if (rank == 0 && use_exclude) {
            regfree(&exclude_re);
        }
        if (out_fp != NULL) {
            fclose(out_fp);
        }
        mfu_param_path_free_all(1, &root_param);
        mfu_file_delete(&mfu_file);
        mfu_free(&exclude_regex);
        mfu_free(&output);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    uint64_t discovery_errors = 0;
    int discover_ok = 1;
    if (rank == 0) {
        if (enumerate_topdirs(root_param.path, use_exclude, &exclude_re, &topdirs, &discovery_errors) != 0) {
            discover_ok = 0;
        }
    }

    MPI_Bcast(&discover_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!discover_ok) {
        if (rank == 0 && use_exclude) {
            regfree(&exclude_re);
        }
        if (out_fp != NULL) {
            fclose(out_fp);
        }
        string_list_free(&topdirs);
        mfu_param_path_free_all(1, &root_param);
        mfu_file_delete(&mfu_file);
        mfu_free(&exclude_regex);
        mfu_free(&output);
        mfu_finalize();
        MPI_Finalize();
        return 1;
    }

    uint64_t num_topdirs = 0;
    if (rank == 0) {
        num_topdirs = (uint64_t) topdirs.count;
    }
    MPI_Bcast(&num_topdirs, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Depth-1 directories selected: %" PRIu64, num_topdirs);
        if (discovery_errors > 0) {
            MFU_LOG(MFU_LOG_INFO, "Depth-1 discovery errors: %" PRIu64, discovery_errors);
        }
        if (unique_inode) {
            MFU_LOG(MFU_LOG_INFO, "--unique-inode enabled (global dedup; higher memory usage)");
        } else {
            MFU_LOG(MFU_LOG_INFO, "Streaming aggregation enabled (no per-entry flist retention)");
        }
    }

    mfu_walk_opts_t* walk_opts = mfu_walk_opts_new();
    walk_opts->use_stat = 1;
    walk_opts->dereference = 0;
    walk_opts->no_atime = 0;

    dhotcold_result_vec_t result_rows;
    result_vec_init(&result_rows);

    char* recv_topdir = NULL;
    int recv_topdir_cap = 0;

    uint64_t processed = 0;
    uint64_t topdir_with_errors = 0;

    uint64_t idx;
    for (idx = 0; idx < num_topdirs; idx++) {
        int pathlen = 0;
        if (rank == 0) {
            size_t len = strlen(topdirs.data[idx]) + 1;
            if (len > (size_t)INT_MAX) {
                MFU_LOG(MFU_LOG_ERR, "Topdir path too long: %s", topdirs.data[idx]);
                pathlen = 0;
            } else {
                pathlen = (int)len;
            }
        }

        MPI_Bcast(&pathlen, 1, MPI_INT, 0, MPI_COMM_WORLD);
        if (pathlen <= 1) {
            rc = 1;
            break;
        }

        if (rank != 0 && pathlen > recv_topdir_cap) {
            mfu_free(&recv_topdir);
            recv_topdir = (char*) MFU_MALLOC((size_t)pathlen);
            recv_topdir_cap = pathlen;
        }

        if (rank == 0) {
            MPI_Bcast(topdirs.data[idx], pathlen, MPI_CHAR, 0, MPI_COMM_WORLD);
        } else {
            MPI_Bcast(recv_topdir, pathlen, MPI_CHAR, 0, MPI_COMM_WORLD);
        }

        const char* topdir = (rank == 0) ? topdirs.data[idx] : recv_topdir;

        dhotcold_stats_t local_stats;
        dhotcold_stats_t global_stats;
        stats_init(&local_stats);
        stats_init(&global_stats);

        (void) analyze_topdir(
            topdir,
            mfu_file,
            walk_opts,
            now_secs,
            cold_days,
            time_field,
            xdev,
            unique_inode,
            &local_stats
        );

        reduce_stats(&local_stats, &global_stats);

        int output_failed = 0;
        if (rank == 0) {
            dhotcold_result_row_t row;
            row.directory_path = (char*)topdir;
            row.total_out = bytes_to_output_unit(global_stats.total_bytes, output_unit);
            row.cold_out = bytes_to_output_unit(global_stats.cold_bytes, output_unit);
            row.ratio = 0.0;
            if (global_stats.total_bytes > 0) {
                row.ratio = (double)global_stats.cold_bytes / (double)global_stats.total_bytes;
            }
            row.error_count = global_stats.error_count;

            int b;
            for (b = 0; b < DHOTCOLD_HIST_BINS; b++) {
                row.hist_out[b] = bytes_to_output_unit(global_stats.hist_bytes[b], output_unit);
            }

            if (write_csv_row(out_fp, &row) != 0 || fflush(out_fp) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write output row for `%s'", topdir);
                output_failed = 1;
                rc = 1;
            }

            if (print_table) {
                result_vec_append(
                    &result_rows,
                    topdir,
                    row.total_out,
                    row.cold_out,
                    row.ratio,
                    row.error_count,
                    row.hist_out
                );
            }
        }

        processed++;
        if (global_stats.error_count > 0) {
            topdir_with_errors++;
        }

        int stop = 0;
        if (output_failed) {
            stop = 1;
        }
        if (strict && global_stats.error_count > 0) {
            if (rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Strict mode stopping on topdir `%s' with %" PRIu64 " errors",
                        topdir, global_stats.error_count);
            }
            stop = 1;
            rc = 1;
        }

        MPI_Bcast(&stop, 1, MPI_INT, 0, MPI_COMM_WORLD);
        if (stop) {
            break;
        }
    }

    if (rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Processed topdirs: %" PRIu64, processed);
        MFU_LOG(MFU_LOG_INFO, "Topdirs with errors: %" PRIu64, topdir_with_errors);
        if (print_table) {
            print_results_table(&result_rows, output_unit);
        }
    }

    mfu_free(&recv_topdir);

    if (rank == 0 && use_exclude) {
        regfree(&exclude_re);
    }
    string_list_free(&topdirs);

    if (out_fp != NULL) {
        fclose(out_fp);
    }

    result_vec_free(&result_rows);

    mfu_walk_opts_delete(&walk_opts);
    mfu_param_path_free_all(1, &root_param);
    mfu_file_delete(&mfu_file);

    mfu_free(&exclude_regex);
    mfu_free(&output);

    mfu_finalize();
    MPI_Finalize();
    return rc;
}
