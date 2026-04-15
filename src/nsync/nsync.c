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
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
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

typedef enum {
    NSYNC_HASH_DOMAIN_SCAN    = 0x5343414eU, /* SCAN */
    NSYNC_HASH_DOMAIN_BATCH   = 0x42415443U, /* BATC */
    NSYNC_HASH_DOMAIN_PLANNER = 0x504c4e52U, /* PLNR */
    NSYNC_HASH_DOMAIN_SRC     = 0x5352434fU, /* SRCO */
    NSYNC_HASH_DOMAIN_DST     = 0x4453544fU, /* DSTO */
} nsync_hash_domain_t;

typedef struct {
    int dryrun;
    uint64_t batch_files;
    int delete;
    int contents;
    int ignore_symlinks;
    uint64_t bufsize;
    int direct;
    int open_noatime;
    nsync_role_mode_t role_mode;
    const char* role_map;
    int trace;
    int quiet;
    int log_rank;
    double imbalance_threshold;
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
    const nsync_action_record_t** records;
    uint64_t size;
    uint64_t capacity;
} nsync_action_ptr_vec_t;

typedef struct {
    uint64_t copy;
    uint64_t remove;
    uint64_t mkdir;
    uint64_t symlink_update;
    uint64_t meta_update;
    uint64_t skipped_only_dst;
} nsync_action_counts_t;

typedef struct {
    uint64_t chown_ignored;
    uint64_t chmod_ignored;
    uint64_t utime_ignored;
    uint64_t chown_failed;
    uint64_t chmod_failed;
    uint64_t utime_failed;
} nsync_meta_apply_stats_t;

typedef struct {
    int enabled;
    uint64_t batch_count;
    uint64_t batch_id;
} nsync_scan_filter_t;

typedef struct {
    char* dir;
    char* raw_path;
    uint64_t batch_count;
    int include_digest;
    int batch_index_mode;
    int max_open_fds;
    int open_fds;
    int raw_fd;
    int raw_has_data;
    int* batch_fds;
    unsigned char* batch_has_data;
    int io_error;
    uint64_t raw_records_written;
    uint64_t raw_bytes_written;
    uint64_t records_written;
    uint64_t bytes_written;
} nsync_batch_spool_t;

typedef struct {
    double start_time;
    double last_print_time;
    uint64_t total_actions;
    uint64_t total_copy_files;
    uint64_t total_copy_bytes;
    uint64_t last_actions;
    uint64_t last_copy_files;
    uint64_t last_copy_bytes;
} nsync_progress_state_t;

enum {
    NSYNC_COPY_PIPELINE_DEPTH = 4,
    NSYNC_COPY_FILE_CREDITS = 8,
};

enum {
    NSYNC_COPY_FRAME_FLAG_END = 0x1U,
    NSYNC_COPY_FRAME_FLAG_HOLE = 0x2U,
    NSYNC_COPY_FRAME_FLAG_ERROR = 0x4U,
};

typedef struct {
    uint64_t file_id;
    uint64_t offset;
    uint32_t data_length;
    uint32_t logical_length;
    uint32_t flags;
} nsync_copy_frame_header_t;

typedef struct {
    int busy;
    char* wire_buf;
    size_t capacity;
    size_t bytes;
    MPI_Request req;
} nsync_copy_send_slot_t;

typedef struct {
    int active;
    uint64_t file_id;
    int dst_rank;
    int fd;
    uint64_t file_size;
    uint64_t next_offset;
    uint64_t data_extent_end;
    int sparse_seek_mode;
    int zero_hole_fallback;
    int end_sent;
    int completed;
    int failed;
    nsync_copy_send_slot_t slots[NSYNC_COPY_PIPELINE_DEPTH];
} nsync_source_transfer_t;

typedef struct {
    int active;
    uint64_t file_id;
    int dst_rank;
    const nsync_action_record_t* action;
} nsync_source_request_t;

typedef struct {
    int busy;
    char* wire_buf;
    size_t capacity;
    size_t bytes;
    int src_rank;
    MPI_Request req;
} nsync_copy_recv_slot_t;

typedef struct {
    int active;
    int completed;
    int response_received;
    int response_error;
    int end_received;
    int transfer_error;
    int open_error;
    uint64_t file_id;
    int src_rank;
    int fd;
    uint64_t copied_bytes;
    char* dst_fullpath;
    const nsync_action_record_t* action;
} nsync_destination_transfer_t;

static const unsigned char NSYNC_SHA256_EMPTY[SHA256_DIGEST_LENGTH] = {
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
    0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
    0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
    0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55
};

static char* nsync_build_full_path(const char* root, const char* relpath);
static char* nsync_trim_space(char* text);

static void nsync_usage(void)
{
    printf("\n");
    printf("Usage: nsync [options] source target\n");
    printf("\n");
    printf("Options:\n");
    printf("      --dryrun               - show differences, do not modify destination\n");
    printf("  -b, --batch-files <N>      - process entries in batches of approximately N items\n");
    printf("  -D, --delete               - delete extraneous files from target\n");
    printf("  -c, --contents             - compare file contents instead of size+mtime\n");
    printf("      --ignore-symlinks      - ignore symlink entries during scan/compare/apply\n");
    printf("  -s, --direct               - open files with O_DIRECT\n");
    printf("      --open-noatime         - open files with O_NOATIME\n");
    printf("      --bufsize <SIZE>       - I/O buffer size in bytes (default " MFU_BUFFER_SIZE_STR ")\n");
    printf("      --imbalance-threshold <R>\n");
    printf("                             - batch imbalance ratio threshold (default 3.0)\n");
    printf("      --role-mode <MODE>     - role assignment mode: auto or map\n");
    printf("      --role-map <SPEC>      - explicit role map (used with --role-mode map)\n");
    printf("      --trace                - print detailed per-rank stage traces (debug)\n");
    printf("  -q, --quiet                - quiet output\n");
    printf("  -h, --help                 - print usage\n");
    printf("\n");
    printf("Progress is always printed on the launcher-console log rank at each batch completion\n");
    printf("(disabled only with -q).\n");
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

static int nsync_sockaddr_equal(const struct sockaddr* a, const struct sockaddr* b)
{
    if (a == NULL || b == NULL || a->sa_family != b->sa_family) {
        return 0;
    }

    if (a->sa_family == AF_INET) {
        const struct sockaddr_in* ia = (const struct sockaddr_in*)a;
        const struct sockaddr_in* ib = (const struct sockaddr_in*)b;
        return memcmp(&ia->sin_addr, &ib->sin_addr, sizeof(struct in_addr)) == 0;
    }

    if (a->sa_family == AF_INET6) {
        const struct sockaddr_in6* ia = (const struct sockaddr_in6*)a;
        const struct sockaddr_in6* ib = (const struct sockaddr_in6*)b;
        return memcmp(&ia->sin6_addr, &ib->sin6_addr, sizeof(struct in6_addr)) == 0;
    }

    return 0;
}

static int nsync_hosts_resolve_match(const char* host_a, const char* host_b)
{
    if (host_a == NULL || host_b == NULL || host_a[0] == '\0' || host_b[0] == '\0') {
        return 0;
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_ADDRCONFIG;

    struct addrinfo* list_a = NULL;
    struct addrinfo* list_b = NULL;
    int rc_a = getaddrinfo(host_a, NULL, &hints, &list_a);
    if (rc_a != 0) {
        hints.ai_flags = 0;
        rc_a = getaddrinfo(host_a, NULL, &hints, &list_a);
    }
    if (rc_a != 0 || list_a == NULL) {
        return 0;
    }

    int rc_b = getaddrinfo(host_b, NULL, &hints, &list_b);
    if (rc_b != 0) {
        hints.ai_flags = 0;
        rc_b = getaddrinfo(host_b, NULL, &hints, &list_b);
    }
    if (rc_b != 0 || list_b == NULL) {
        freeaddrinfo(list_a);
        return 0;
    }

    int matched = 0;
    for (const struct addrinfo* a = list_a; a != NULL && !matched; a = a->ai_next) {
        for (const struct addrinfo* b = list_b; b != NULL; b = b->ai_next) {
            if (nsync_sockaddr_equal(a->ai_addr, b->ai_addr)) {
                matched = 1;
                break;
            }
        }
    }

    freeaddrinfo(list_a);
    freeaddrinfo(list_b);
    return matched;
}

static int nsync_extract_uri_host(const char* uri, char* host_out, size_t host_len)
{
    if (uri == NULL || host_out == NULL || host_len == 0) {
        return -1;
    }
    host_out[0] = '\0';

    const char* start = strstr(uri, "://");
    if (start != NULL) {
        start += 3;
    } else {
        start = strchr(uri, ';');
        if (start != NULL) {
            start++;
        } else {
            start = uri;
        }
    }

    while (*start == ' ') {
        start++;
    }

    const char* end = start;
    if (*start == '[') {
        start++;
        end = strchr(start, ']');
        if (end == NULL) {
            return -1;
        }
    } else {
        while (*end != '\0' && *end != ':' && *end != ',' && *end != ';' && *end != '/') {
            end++;
        }
    }

    size_t len = (size_t)(end - start);
    if (len == 0 || len >= host_len) {
        return -1;
    }

    memcpy(host_out, start, len);
    host_out[len] = '\0';
    return 0;
}

static int nsync_host_matches_local(const char* launcher_host)
{
    if (launcher_host == NULL || launcher_host[0] == '\0') {
        return 0;
    }

    const char* local_uri = getenv("OMPI_MCA_orte_local_daemon_uri");
    if (local_uri != NULL && local_uri[0] != '\0') {
        char local_uri_host[NI_MAXHOST];
        if (nsync_extract_uri_host(local_uri, local_uri_host, sizeof(local_uri_host)) == 0) {
            if (strcasecmp(launcher_host, local_uri_host) == 0) {
                return 1;
            }
            if (nsync_hosts_resolve_match(launcher_host, local_uri_host)) {
                return 1;
            }
        }
    }

    char local_hostname[HOST_NAME_MAX + 1];
    if (gethostname(local_hostname, sizeof(local_hostname)) == 0) {
        local_hostname[HOST_NAME_MAX] = '\0';
        if (strcasecmp(launcher_host, local_hostname) == 0) {
            return 1;
        }
        if (nsync_hosts_resolve_match(launcher_host, local_hostname)) {
            return 1;
        }
    }

    return 0;
}

static int nsync_select_log_rank(void)
{
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Comm shared_comm = MPI_COMM_NULL;
    MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, rank, MPI_INFO_NULL, &shared_comm);
    int local_rank = -1;
    if (shared_comm != MPI_COMM_NULL) {
        MPI_Comm_rank(shared_comm, &local_rank);
    }

    int candidate = INT_MAX;
    char launcher_host[NI_MAXHOST];
    const char* hnp_uri = getenv("OMPI_MCA_orte_hnp_uri");
    if (nsync_extract_uri_host(hnp_uri, launcher_host, sizeof(launcher_host)) == 0 &&
        nsync_host_matches_local(launcher_host))
    {
        if (local_rank == 0) {
            candidate = rank;
        }
    }

    if (shared_comm != MPI_COMM_NULL) {
        MPI_Comm_free(&shared_comm);
    }

    int log_rank = INT_MAX;
    MPI_Allreduce(&candidate, &log_rank, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
    if (log_rank == INT_MAX) {
        log_rank = 0;
    }

    return log_rank;
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

static void nsync_batch_monitor_skew(
    const nsync_options_t* opts,
    uint64_t batch_id,
    uint64_t batch_count,
    const char* metric_name,
    uint64_t local_count)
{
    if (opts == NULL || opts->batch_files == 0 || batch_count <= 1 || metric_name == NULL) {
        return;
    }

    if (opts->quiet) {
        return;
    }

    uint64_t global_sum = 0;
    uint64_t global_max = 0;
    MPI_Allreduce(&local_count, &global_sum, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(&local_count, &global_max, 1, MPI_UINT64_T, MPI_MAX, MPI_COMM_WORLD);

    int rank;
    int ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    if (rank != opts->log_rank || ranks <= 0 || global_sum == 0) {
        return;
    }

    double avg = (double)global_sum / (double)ranks;
    if (avg <= 0.0) {
        return;
    }

    double threshold = opts->imbalance_threshold;
    if (!isfinite(threshold) || threshold < 1.0) {
        threshold = 1.0;
    }

    double ratio = (double)global_max / avg;
    if (ratio >= threshold && global_max >= 256) {
        MFU_LOG(MFU_LOG_WARN,
            "Batch imbalance observed for %s at batch %" PRIu64 "/%" PRIu64
            " (sum=%" PRIu64 " max=%" PRIu64 " avg=%.2f ratio=%.2f threshold=%.2f). "
            "Consider adjusting --batch-files.",
            metric_name, batch_id + 1, batch_count, global_sum, global_max, avg, ratio, threshold);
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

typedef int (*nsync_frame_unpack_fn)(const char* payload, size_t payload_size, void* arg);

typedef struct {
    nsync_frame_unpack_fn unpack_fn;
    void* unpack_arg;
    char* pending;
    size_t pending_size;
    size_t pending_capacity;
} nsync_frame_decoder_t;

static int nsync_u64_to_size_checked(uint64_t value, size_t* out)
{
    if (value > (uint64_t)SIZE_MAX) {
        return -1;
    }

    *out = (size_t)value;
    return 0;
}

static int nsync_count_to_bytes_checked(uint64_t count, size_t elem_size, size_t* out)
{
    if (elem_size != 0 && count > (uint64_t)(SIZE_MAX / elem_size)) {
        return -1;
    }

    *out = (size_t)count * elem_size;
    return 0;
}

static uint32_t nsync_hash_bytes_with_domain(
    const char* key,
    size_t len,
    nsync_hash_domain_t domain)
{
    uint32_t hash = 0;
    uint32_t domain_value = (uint32_t)domain;
    unsigned char domain_bytes[4];
    domain_bytes[0] = (unsigned char)(domain_value & 0xffu);
    domain_bytes[1] = (unsigned char)((domain_value >> 8) & 0xffu);
    domain_bytes[2] = (unsigned char)((domain_value >> 16) & 0xffu);
    domain_bytes[3] = (unsigned char)((domain_value >> 24) & 0xffu);

    for (size_t i = 0; i < sizeof(domain_bytes); i++) {
        hash += domain_bytes[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }

    for (size_t i = 0; i < len; i++) {
        hash += (unsigned char)key[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }

    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

static int nsync_hash_bucket_len(
    const char* key,
    size_t len,
    int buckets,
    nsync_hash_domain_t domain)
{
    if (buckets <= 1) {
        return 0;
    }

    uint32_t hash = nsync_hash_bytes_with_domain(key, len, domain);
    return (int)(hash % (uint32_t)buckets);
}

static int nsync_hash_bucket(
    const char* key,
    int buckets,
    nsync_hash_domain_t domain)
{
    return nsync_hash_bucket_len(key, strlen(key), buckets, domain);
}

static int nsync_frame_wire_bytes(size_t payload_size, uint64_t* wire_bytes)
{
    if (payload_size > UINT32_MAX || wire_bytes == NULL) {
        return -1;
    }

    *wire_bytes = (uint64_t)sizeof(uint32_t) + (uint64_t)payload_size;
    return 0;
}

static uint64_t nsync_exchange_chunk_bytes(void)
{
    const uint64_t default_bytes = 64ULL * 1024ULL * 1024ULL;
    uint64_t chunk_bytes = default_bytes;

    /* Hidden override to force chunk-split paths during functional tests. */
    const char* env = getenv("NSYNC_EXCHANGE_CHUNK_BYTES");
    if (env != NULL && *env != '\0') {
        errno = 0;
        char* end = NULL;
        unsigned long long parsed = strtoull(env, &end, 10);
        if (errno == 0 && end != env && *end == '\0' && parsed > 0) {
            chunk_bytes = (uint64_t)parsed;
        }
    }

    if (chunk_bytes > (uint64_t)INT_MAX) {
        chunk_bytes = (uint64_t)INT_MAX;
    }
    if (chunk_bytes == 0) {
        chunk_bytes = 1;
    }
    return chunk_bytes;
}

static int nsync_compute_u64_displacements(
    const uint64_t* counts,
    int count,
    uint64_t* displs,
    uint64_t* total_out)
{
    uint64_t total = 0;
    for (int i = 0; i < count; i++) {
        if (displs != NULL) {
            displs[i] = total;
        }
        if (UINT64_MAX - total < counts[i]) {
            return -1;
        }
        total += counts[i];
    }

    *total_out = total;
    return 0;
}

static size_t nsync_frame_pack(char* buf, const char* payload, size_t payload_size)
{
    uint32_t len = (uint32_t)payload_size;
    char* ptr = buf;
    mfu_pack_uint32(&ptr, len);
    memmove(ptr, payload, payload_size);
    ptr += payload_size;
    return (size_t)(ptr - buf);
}

static void nsync_frame_decoder_init(
    nsync_frame_decoder_t* decoder,
    nsync_frame_unpack_fn unpack_fn,
    void* unpack_arg)
{
    decoder->unpack_fn = unpack_fn;
    decoder->unpack_arg = unpack_arg;
    decoder->pending = NULL;
    decoder->pending_size = 0;
    decoder->pending_capacity = 0;
}

static void nsync_frame_decoder_free(nsync_frame_decoder_t* decoder)
{
    mfu_free(&decoder->pending);
    decoder->pending_size = 0;
    decoder->pending_capacity = 0;
}

static int nsync_frame_decoder_reserve(nsync_frame_decoder_t* decoder, size_t need)
{
    if (need <= decoder->pending_capacity) {
        return 0;
    }

    size_t new_capacity = (decoder->pending_capacity > 0) ? decoder->pending_capacity : 4096;
    while (new_capacity < need) {
        if (new_capacity > (SIZE_MAX / 2)) {
            new_capacity = need;
            break;
        }
        new_capacity *= 2;
    }

    char* new_pending = (char*)realloc(decoder->pending, new_capacity);
    if (new_pending == NULL) {
        return -1;
    }

    decoder->pending = new_pending;
    decoder->pending_capacity = new_capacity;
    return 0;
}

static int nsync_frame_decoder_feed(
    nsync_frame_decoder_t* decoder,
    const char* buf,
    size_t size)
{
    if (size == 0) {
        return 0;
    }

    if (decoder->pending_size > SIZE_MAX - size) {
        return -1;
    }

    size_t need = decoder->pending_size + size;
    if (nsync_frame_decoder_reserve(decoder, need) != 0) {
        return -1;
    }

    memcpy(decoder->pending + decoder->pending_size, buf, size);
    decoder->pending_size = need;

    size_t consumed = 0;
    while (decoder->pending_size - consumed >= sizeof(uint32_t)) {
        const char* ptr = decoder->pending + consumed;
        uint32_t payload_len = 0;
        mfu_unpack_uint32(&ptr, &payload_len);

        uint64_t frame_size_u64 = (uint64_t)sizeof(uint32_t) + (uint64_t)payload_len;
        if (frame_size_u64 > (uint64_t)(decoder->pending_size - consumed)) {
            break;
        }

        size_t frame_size = (size_t)frame_size_u64;
        if (decoder->unpack_fn(ptr, (size_t)payload_len, decoder->unpack_arg) != 0) {
            return -1;
        }
        consumed += frame_size;
    }

    if (consumed > 0) {
        memmove(decoder->pending, decoder->pending + consumed, decoder->pending_size - consumed);
        decoder->pending_size -= consumed;
    }

    return 0;
}

static int nsync_frame_decoder_finish(const nsync_frame_decoder_t* decoder)
{
    return decoder->pending_size == 0 ? 0 : -1;
}

static int nsync_exchange_framed_buffers(
    MPI_Comm comm,
    int rank,
    int ranks,
    const uint64_t* send_counts,
    const uint64_t* recv_counts,
    const uint64_t* send_displs,
    const char* send_buf,
    nsync_frame_unpack_fn unpack_fn,
    void* unpack_arg)
{
    uint64_t chunk_limit = nsync_exchange_chunk_bytes();
    uint64_t peer_cap = chunk_limit;
    if (ranks > 1) {
        peer_cap = chunk_limit / (uint64_t)ranks;
        if (peer_cap == 0) {
            peer_cap = 1;
        }
    }

    int* send_round_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_round_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* send_round_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_round_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
    uint64_t* send_offsets = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t* recv_offsets = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    nsync_frame_decoder_t* decoders =
        (nsync_frame_decoder_t*)MFU_MALLOC((size_t)ranks * sizeof(nsync_frame_decoder_t));
    char* round_send_buf = NULL;
    char* round_recv_buf = NULL;
    int local_error = 0;

    for (int i = 0; i < ranks; i++) {
        send_round_counts[i] = 0;
        recv_round_counts[i] = 0;
        send_round_displs[i] = 0;
        recv_round_displs[i] = 0;
        send_offsets[i] = 0;
        recv_offsets[i] = 0;
        nsync_frame_decoder_init(&decoders[i], unpack_fn, unpack_arg);
    }

    while (1) {
        uint64_t round_send_total_u64 = 0;
        uint64_t round_recv_total_u64 = 0;
        int local_more = 0;
        int global_error = 0;

        for (int i = 0; i < ranks; i++) {
            uint64_t send_remaining = send_counts[i] - send_offsets[i];
            uint64_t recv_remaining = recv_counts[i] - recv_offsets[i];
            uint64_t send_chunk_u64 = (send_remaining > peer_cap) ? peer_cap : send_remaining;
            uint64_t recv_chunk_u64 = (recv_remaining > peer_cap) ? peer_cap : recv_remaining;

            send_round_counts[i] = (int)send_chunk_u64;
            recv_round_counts[i] = (int)recv_chunk_u64;
            send_round_displs[i] = (int)round_send_total_u64;
            recv_round_displs[i] = (int)round_recv_total_u64;
            round_send_total_u64 += send_chunk_u64;
            round_recv_total_u64 += recv_chunk_u64;

            if (send_remaining > 0 || recv_remaining > 0) {
                local_more = 1;
            }
        }

        int global_more = 0;
        MPI_Allreduce(&local_more, &global_more, 1, MPI_INT, MPI_MAX, comm);
        if (global_more == 0) {
            break;
        }

        size_t round_send_total = 0;
        size_t round_recv_total = 0;
        if (nsync_u64_to_size_checked(round_send_total_u64, &round_send_total) != 0 ||
            nsync_u64_to_size_checked(round_recv_total_u64, &round_recv_total) != 0)
        {
            local_error = 1;
        }

        char* new_round_send_buf = round_send_buf;
        char* new_round_recv_buf = round_recv_buf;
        if (!local_error) {
            new_round_send_buf = (char*)realloc(round_send_buf, round_send_total > 0 ? round_send_total : 1);
            new_round_recv_buf = (char*)realloc(round_recv_buf, round_recv_total > 0 ? round_recv_total : 1);
        }
        if (!local_error && (new_round_send_buf == NULL || new_round_recv_buf == NULL)) {
            local_error = 1;
        } else if (!local_error) {
            round_send_buf = new_round_send_buf;
            round_recv_buf = new_round_recv_buf;
        }

        for (int i = 0; i < ranks; i++) {
            if (local_error) {
                break;
            }
            if (send_round_counts[i] == 0) {
                continue;
            }

            size_t src_index = 0;
            if (nsync_u64_to_size_checked(send_displs[i] + send_offsets[i], &src_index) != 0) {
                local_error = 1;
                break;
            }

            memcpy(
                round_send_buf + send_round_displs[i],
                send_buf + src_index,
                (size_t)send_round_counts[i]);
        }

        MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, comm);
        if (global_error != 0) {
            local_error = 1;
            break;
        }

        MPI_Alltoallv(
            round_send_buf, send_round_counts, send_round_displs, MPI_BYTE,
            round_recv_buf, recv_round_counts, recv_round_displs, MPI_BYTE,
            comm);

        for (int i = 0; i < ranks; i++) {
            send_offsets[i] += (uint64_t)send_round_counts[i];
            recv_offsets[i] += (uint64_t)recv_round_counts[i];

            if (!local_error && recv_round_counts[i] > 0 &&
                nsync_frame_decoder_feed(
                    &decoders[i],
                    round_recv_buf + recv_round_displs[i],
                    (size_t)recv_round_counts[i]) != 0)
            {
                local_error = 1;
            }
        }
    }

    for (int i = 0; i < ranks; i++) {
        if (!local_error && nsync_frame_decoder_finish(&decoders[i]) != 0) {
            local_error = 1;
        }
        nsync_frame_decoder_free(&decoders[i]);
    }

    mfu_free(&send_round_counts);
    mfu_free(&recv_round_counts);
    mfu_free(&send_round_displs);
    mfu_free(&recv_round_displs);
    mfu_free(&send_offsets);
    mfu_free(&recv_offsets);
    mfu_free(&decoders);
    mfu_free(&round_send_buf);
    mfu_free(&round_recv_buf);

    (void)rank;
    return local_error == 0 ? 0 : -1;
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
    int validator_rank = opts->log_rank;
    nsync_trace_local(opts, "assign-start", (uint64_t)opts->role_mode, (uint64_t)ranks);

    if (opts->role_mode == NSYNC_ROLE_MODE_MAP) {
        if (rank == validator_rank) {
            if (nsync_parse_role_map(opts->role_map, ranks, all_roles) != 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to parse --role-map: %s", opts->role_map);
                status = -1;
            }
        }

        nsync_trace_local(opts, "assign-pre-map-bcast-status", (uint64_t)status, 0);
        MPI_Bcast(&status, 1, MPI_INT, validator_rank, MPI_COMM_WORLD);
        nsync_trace_local(opts, "assign-post-map-bcast-status", (uint64_t)status, 0);
        MPI_Bcast(all_roles, ranks, MPI_INT, validator_rank, MPI_COMM_WORLD);
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
    if (rank == validator_rank && status == 0) {
        status = nsync_validate_and_count_roles(opts, all_roles, auto_caps, ranks, &src_count, &dst_count);
    }

    nsync_trace_local(opts, "assign-pre-status-bcast", (uint64_t)status, 0);
    MPI_Bcast(&status, 1, MPI_INT, validator_rank, MPI_COMM_WORLD);
    nsync_trace_local(opts, "assign-post-status-bcast", (uint64_t)status, 0);
    if (status != 0) {
        mfu_free(&all_roles);
        mfu_free(&auto_caps);
        return -1;
    }

    char local_hostname[HOST_NAME_MAX + 1];
    if (gethostname(local_hostname, sizeof(local_hostname)) == 0) {
        local_hostname[HOST_NAME_MAX] = '\0';
    } else {
        snprintf(local_hostname, sizeof(local_hostname), "unknown");
    }

    size_t host_entry_len = (size_t)HOST_NAME_MAX + 1;
    char* gathered_hosts = NULL;
    if (rank == opts->log_rank) {
        gathered_hosts = (char*)MFU_MALLOC((size_t)ranks * host_entry_len);
    }

    MPI_Gather(
        local_hostname, (int)host_entry_len, MPI_CHAR,
        gathered_hosts, (int)host_entry_len, MPI_CHAR,
        opts->log_rank, MPI_COMM_WORLD);

    if (rank == opts->log_rank) {
        const char* mode = (opts->role_mode == NSYNC_ROLE_MODE_AUTO) ? "auto" : "map";
        MFU_LOG(MFU_LOG_INFO, "Role assignment mode=%s src_ranks=%d dst_ranks=%d", mode, src_count, dst_count);
        for (int i = 0; i < ranks; i++) {
            const char* host = gathered_hosts + ((size_t)i * host_entry_len);
            if (host[0] == '\0') {
                host = "unknown";
            }
            MFU_LOG(MFU_LOG_INFO, "Rank %d role=%s host=%s", i, nsync_role_to_string(all_roles[i]), host);
        }
    }

    mfu_free(&gathered_hosts);

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
        uint64_t new_capacity = 1024;
        if (vec->capacity > 0) {
            if (vec->capacity > (UINT64_MAX / 2)) {
                return -1;
            }
            new_capacity = vec->capacity * 2;
        }

        size_t alloc_bytes = 0;
        if (nsync_count_to_bytes_checked(new_capacity, sizeof(nsync_meta_record_t), &alloc_bytes) != 0) {
            return -1;
        }

        nsync_meta_record_t* new_records = (nsync_meta_record_t*)realloc(vec->records, alloc_bytes);
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
        uint64_t new_capacity = 1024;
        if (vec->capacity > 0) {
            if (vec->capacity > (UINT64_MAX / 2)) {
                return -1;
            }
            new_capacity = vec->capacity * 2;
        }

        size_t alloc_bytes = 0;
        if (nsync_count_to_bytes_checked(new_capacity, sizeof(nsync_action_record_t), &alloc_bytes) != 0) {
            return -1;
        }

        nsync_action_record_t* new_records = (nsync_action_record_t*)realloc(vec->records, alloc_bytes);
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

static void nsync_action_ptr_vec_init(nsync_action_ptr_vec_t* vec)
{
    vec->records = NULL;
    vec->size = 0;
    vec->capacity = 0;
}

static int nsync_action_ptr_vec_push(nsync_action_ptr_vec_t* vec, const nsync_action_record_t* rec)
{
    if (vec->size == vec->capacity) {
        uint64_t new_capacity = (vec->capacity > 0) ? (vec->capacity * 2) : 128;
        size_t alloc_bytes = 0;
        if (nsync_count_to_bytes_checked(new_capacity, sizeof(*vec->records), &alloc_bytes) != 0) {
            return -1;
        }

        const nsync_action_record_t** new_records =
            (const nsync_action_record_t**)realloc(vec->records, alloc_bytes);
        if (new_records == NULL) {
            return -1;
        }
        vec->records = new_records;
        vec->capacity = new_capacity;
    }

    vec->records[vec->size] = rec;
    vec->size++;
    return 0;
}

static void nsync_action_ptr_vec_free(nsync_action_ptr_vec_t* vec)
{
    free(vec->records);
    vec->records = NULL;
    vec->size = 0;
    vec->capacity = 0;
}

static uint64_t nsync_min_u64(uint64_t a, uint64_t b)
{
    return (a < b) ? a : b;
}

static uint64_t nsync_copy_large_file_threshold(void)
{
    const uint64_t default_bytes = 64ULL * 1024ULL * 1024ULL;
    uint64_t threshold = default_bytes;
    const char* env = getenv("NSYNC_LARGE_FILE_THRESHOLD");
    if (env != NULL && *env != '\0') {
        errno = 0;
        char* end = NULL;
        unsigned long long parsed = strtoull(env, &end, 10);
        if (errno == 0 && end != env && *end == '\0' && parsed > 0) {
            threshold = (uint64_t)parsed;
        }
    }
    return threshold;
}

static int nsync_copy_file_credit_limit(void)
{
    int credits = NSYNC_COPY_FILE_CREDITS;
    const char* env = getenv("NSYNC_COPY_FILE_CREDITS");
    if (env != NULL && *env != '\0') {
        errno = 0;
        char* end = NULL;
        long parsed = strtol(env, &end, 10);
        if (errno == 0 && end != env && *end == '\0' && parsed > 0 && parsed <= INT_MAX) {
            credits = (int)parsed;
        }
    }
    return credits;
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

static int nsync_open_readonly(const char* path, const nsync_options_t* opts)
{
    int flags = O_RDONLY;
    if (opts->open_noatime) {
#ifdef O_NOATIME
        flags |= O_NOATIME;
#endif
    }
    if (opts->direct) {
#ifdef O_DIRECT
        flags |= O_DIRECT;
#endif
    }
    return open(path, flags);
}

static int nsync_open_writeonly_create_trunc(const char* path, mode_t mode, const nsync_options_t* opts)
{
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (opts->open_noatime) {
#ifdef O_NOATIME
        flags |= O_NOATIME;
#endif
    }
    if (opts->direct) {
#ifdef O_DIRECT
        flags |= O_DIRECT;
#endif
    }
    return open(path, flags, mode);
}

static int nsync_compute_sha256_file(
    const char* fullpath,
    size_t bufsize,
    const nsync_options_t* opts,
    unsigned char digest[SHA256_DIGEST_LENGTH])
{
    int fd = nsync_open_readonly(fullpath, opts);
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

    uint32_t hash = nsync_hash_bytes_with_domain(relpath, strlen(relpath), NSYNC_HASH_DOMAIN_BATCH);
    uint64_t id = (uint64_t)hash % filter->batch_count;
    return id == filter->batch_id;
}

typedef int (*nsync_scan_emit_fn)(const nsync_meta_record_t* rec, void* emit_arg);

typedef struct {
    char** paths;
    uint64_t size;
    uint64_t capacity;
} nsync_dir_task_vec_t;

typedef struct {
    nsync_role_t side;
    const char* root;
    MPI_Comm comm;
    int comm_rank;
    int comm_size;
} nsync_scan_role_ctx_t;

static void nsync_dir_task_vec_init(nsync_dir_task_vec_t* vec)
{
    vec->paths = NULL;
    vec->size = 0;
    vec->capacity = 0;
}

static void nsync_dir_task_vec_free(nsync_dir_task_vec_t* vec)
{
    if (vec == NULL) {
        return;
    }

    for (uint64_t i = 0; i < vec->size; i++) {
        mfu_free(&vec->paths[i]);
    }

    mfu_free(&vec->paths);
    vec->size = 0;
    vec->capacity = 0;
}

static int nsync_dir_task_vec_push_take(nsync_dir_task_vec_t* vec, char* relpath)
{
    if (vec->size == vec->capacity) {
        uint64_t new_capacity = 64;
        if (vec->capacity > 0) {
            if (vec->capacity > (UINT64_MAX / 2)) {
                return -1;
            }
            new_capacity = vec->capacity * 2;
        }

        size_t alloc_bytes = 0;
        if (nsync_count_to_bytes_checked(new_capacity, sizeof(char*), &alloc_bytes) != 0) {
            return -1;
        }

        char** new_paths = (char**)realloc(vec->paths, alloc_bytes);
        if (new_paths == NULL) {
            return -1;
        }
        vec->paths = new_paths;
        vec->capacity = new_capacity;
    }

    vec->paths[vec->size] = relpath;
    vec->size++;
    return 0;
}

static int nsync_dir_task_vec_push_copy(nsync_dir_task_vec_t* vec, const char* relpath)
{
    char* copy = MFU_STRDUP(relpath);
    if (nsync_dir_task_vec_push_take(vec, copy) != 0) {
        mfu_free(&copy);
        return -1;
    }
    return 0;
}

static int nsync_dir_task_vec_append_take_all(
    nsync_dir_task_vec_t* dst,
    nsync_dir_task_vec_t* src)
{
    for (uint64_t i = 0; i < src->size; i++) {
        char* relpath = src->paths[i];
        if (nsync_dir_task_vec_push_take(dst, relpath) != 0) {
            return -1;
        }
        src->paths[i] = NULL;
    }

    src->size = 0;
    return 0;
}

static int nsync_scan_role_context_init(
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    nsync_scan_role_ctx_t* ctx)
{
    memset(ctx, 0, sizeof(*ctx));
    ctx->side = role_info->role;

    if (role_info->role == NSYNC_ROLE_SRC) {
        ctx->root = src_path;
        ctx->comm = role_info->src_comm;
        ctx->comm_rank = role_info->src_index;
        ctx->comm_size = role_info->src_count;
        return 0;
    }

    if (role_info->role == NSYNC_ROLE_DST) {
        ctx->root = dst_path;
        ctx->comm = role_info->dst_comm;
        ctx->comm_rank = role_info->dst_index;
        ctx->comm_size = role_info->dst_count;
        return 0;
    }

    return -1;
}

static int nsync_owner_index_in_group(const char* relpath, int ranks)
{
    return nsync_hash_bucket(relpath, ranks, NSYNC_HASH_DOMAIN_SCAN);
}

static int nsync_scan_emit_path(
    const nsync_options_t* opts,
    const char* fullpath,
    const char* relpath,
    const struct stat* st,
    nsync_role_t side,
    nsync_meta_vec_t* out,
    int* scan_errors,
    const nsync_scan_filter_t* filter,
    size_t digest_bufsize,
    int dirs_only,
    nsync_scan_emit_fn emit_fn,
    void* emit_arg)
{
    if (out == NULL && emit_fn == NULL) {
        return 0;
    }

    mfu_filetype type = mfu_flist_mode_to_filetype(st->st_mode);
    if (opts->ignore_symlinks && type == MFU_TYPE_LINK) {
        return 0;
    }

    int include = nsync_scan_filter_match_relpath(filter, relpath);
    if (dirs_only && type != MFU_TYPE_DIR) {
        include = 0;
    }

    if (!include) {
        return 0;
    }

    nsync_meta_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.relpath = MFU_STRDUP(relpath);
    rec.type = type;
    rec.mode = (uint64_t)st->st_mode;
    rec.uid = (uint64_t)st->st_uid;
    rec.gid = (uint64_t)st->st_gid;
    rec.size = (uint64_t)st->st_size;
    rec.mtime = (uint64_t)st->st_mtim.tv_sec;
    rec.mtime_nsec = (uint64_t)st->st_mtim.tv_nsec;
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
            if (nsync_compute_sha256_file(fullpath, digest_bufsize, opts, rec.digest) == 0) {
                rec.digest_valid = 1;
            } else {
                (*scan_errors)++;
            }
        }
    }

    if (out != NULL) {
        if (nsync_meta_vec_push(out, &rec) != 0) {
            nsync_meta_record_free(&rec);
            return -1;
        }
    } else if (emit_fn != NULL) {
        if (emit_fn(&rec, emit_arg) != 0) {
            nsync_meta_record_free(&rec);
            return -1;
        }
        nsync_meta_record_free(&rec);
    }

    return 0;
}

static size_t nsync_dir_task_pack_size(const char* relpath)
{
    return 4 + strlen(relpath);
}

static size_t nsync_dir_task_pack(char* buf, const char* relpath)
{
    uint32_t path_len = (uint32_t)strlen(relpath);
    char* ptr = buf;
    mfu_pack_uint32(&ptr, path_len);
    memcpy(ptr, relpath, (size_t)path_len);
    ptr += path_len;
    return (size_t)(ptr - buf);
}

static int nsync_dir_task_unpack(const char** pptr, const char* end, char** relpath)
{
    const char* ptr = *pptr;
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
    ptr += path_len;
    *pptr = ptr;
    return 0;
}

static int nsync_dir_task_unpack_payload(const char* payload, size_t payload_size, void* arg)
{
    nsync_dir_task_vec_t* recv_tasks = (nsync_dir_task_vec_t*)arg;
    const char* ptr = payload;
    const char* end = payload + payload_size;
    char* relpath = NULL;

    if (nsync_dir_task_unpack(&ptr, end, &relpath) != 0 || ptr != end) {
        mfu_free(&relpath);
        return -1;
    }

    if (nsync_dir_task_vec_push_take(recv_tasks, relpath) != 0) {
        mfu_free(&relpath);
        return -1;
    }

    return 0;
}

static int nsync_dir_task_exchange(
    const nsync_scan_role_ctx_t* ctx,
    nsync_dir_task_vec_t* send_tasks,
    nsync_dir_task_vec_t* recv_tasks)
{
    int comm_size = ctx->comm_size;
    MPI_Comm comm = ctx->comm;
    int rc = 0;
    int local_error = 0;
    char* send_buf = NULL;
    uint64_t* send_counts = (uint64_t*)MFU_MALLOC((size_t)comm_size * sizeof(uint64_t));
    uint64_t* recv_counts = (uint64_t*)MFU_MALLOC((size_t)comm_size * sizeof(uint64_t));
    uint64_t* send_displs = (uint64_t*)MFU_MALLOC((size_t)comm_size * sizeof(uint64_t));
    uint64_t total_send = 0;
    uint64_t total_recv = 0;

    for (int i = 0; i < comm_size; i++) {
        send_counts[i] = 0;
        recv_counts[i] = 0;
        send_displs[i] = 0;
    }

    for (int i = 0; i < comm_size; i++) {
        for (uint64_t j = 0; j < send_tasks[i].size; j++) {
            uint64_t wire_bytes = 0;
            if (nsync_frame_wire_bytes(nsync_dir_task_pack_size(send_tasks[i].paths[j]), &wire_bytes) != 0 ||
                UINT64_MAX - send_counts[i] < wire_bytes)
            {
                local_error = 1;
                break;
            }
            send_counts[i] += wire_bytes;
        }
        if (local_error) {
            break;
        }
    }

    int global_error = 0;
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, comm);
    if (global_error != 0) {
        rc = -1;
        goto cleanup;
    }

    MPI_Alltoall(send_counts, 1, MPI_UINT64_T, recv_counts, 1, MPI_UINT64_T, comm);

    if (nsync_compute_u64_displacements(send_counts, comm_size, send_displs, &total_send) != 0 ||
        nsync_compute_u64_displacements(recv_counts, comm_size, NULL, &total_recv) != 0)
    {
        local_error = 1;
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, comm);
    if (global_error != 0) {
        rc = -1;
        goto cleanup;
    }

    size_t send_alloc = 0;
    if (nsync_u64_to_size_checked(total_send, &send_alloc) != 0) {
        local_error = 1;
    }

    if (!local_error) {
        send_buf = (char*)MFU_MALLOC(send_alloc > 0 ? send_alloc : 1);
    }

    for (int i = 0; i < comm_size && !local_error; i++) {
        uint64_t offset = send_displs[i];
        for (uint64_t j = 0; j < send_tasks[i].size; j++) {
            const char* relpath = send_tasks[i].paths[j];
            size_t payload_bytes = nsync_dir_task_pack_size(relpath);
            uint64_t wire_bytes = 0;
            if (nsync_frame_wire_bytes(payload_bytes, &wire_bytes) != 0 ||
                UINT64_MAX - offset < wire_bytes ||
                offset + wire_bytes > total_send)
            {
                local_error = 1;
                break;
            }

            size_t write_offset = 0;
            if (nsync_u64_to_size_checked(offset, &write_offset) != 0) {
                local_error = 1;
                break;
            }

            char* ptr = send_buf + write_offset;
            size_t packed = nsync_dir_task_pack(ptr + sizeof(uint32_t), relpath);
            size_t framed = nsync_frame_pack(ptr, ptr + sizeof(uint32_t), packed);
            if (packed != payload_bytes || (uint64_t)framed != wire_bytes) {
                local_error = 1;
                break;
            }
            offset += wire_bytes;
        }
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, comm);
    if (global_error != 0) {
        rc = -1;
        goto cleanup;
    }

    if (nsync_exchange_framed_buffers(
            comm, ctx->comm_rank, comm_size,
            send_counts, recv_counts, send_displs, send_buf,
            nsync_dir_task_unpack_payload, recv_tasks) != 0)
    {
        local_error = 1;
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, comm);
    if (global_error != 0) {
        rc = -1;
    }

cleanup:
    mfu_free(&send_counts);
    mfu_free(&recv_counts);
    mfu_free(&send_displs);
    mfu_free(&send_buf);
    return rc;
}

static void nsync_scan_process_directory(
    const nsync_options_t* opts,
    const nsync_scan_role_ctx_t* ctx,
    const char* relpath,
    nsync_meta_vec_t* out,
    int* scan_errors,
    const nsync_scan_filter_t* filter,
    uint64_t* item_count,
    size_t digest_bufsize,
    int dirs_only,
    nsync_scan_emit_fn emit_fn,
    void* emit_arg,
    nsync_dir_task_vec_t* next_local,
    nsync_dir_task_vec_t* next_remote,
    int* local_fatal)
{
    char* fullpath = nsync_build_full_path(ctx->root, relpath);
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
        char* child_fullpath = nsync_build_full_path(ctx->root, child_rel);
        struct stat st;
        if (lstat(child_fullpath, &st) != 0) {
            (*scan_errors)++;
            mfu_free(&child_fullpath);
            mfu_free(&child_rel);
            continue;
        }

        if (item_count != NULL) {
            (*item_count)++;
        }

        if (nsync_scan_emit_path(
                opts, child_fullpath, child_rel, &st, ctx->side,
                out, scan_errors, filter, digest_bufsize, dirs_only, emit_fn, emit_arg) != 0)
        {
            *local_fatal = 1;
            mfu_free(&child_fullpath);
            mfu_free(&child_rel);
            break;
        }

        if (S_ISDIR(st.st_mode)) {
            int owner = nsync_owner_index_in_group(child_rel, ctx->comm_size);
            int push_rc;
            if (owner == ctx->comm_rank) {
                push_rc = nsync_dir_task_vec_push_take(next_local, child_rel);
            } else {
                push_rc = nsync_dir_task_vec_push_take(&next_remote[owner], child_rel);
            }

            if (push_rc != 0) {
                *local_fatal = 1;
                mfu_free(&child_rel);
                mfu_free(&child_fullpath);
                break;
            }
            child_rel = NULL;
        }

        mfu_free(&child_fullpath);
        mfu_free(&child_rel);
    }

    closedir(dir);
    mfu_free(&fullpath);
}

/* Scan one directory frontier at a time inside the role communicator so each
 * directory path is owned and scanned by exactly one rank on that side. */
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
    if (role_info->role != NSYNC_ROLE_SRC && role_info->role != NSYNC_ROLE_DST) {
        return;
    }

    nsync_scan_role_ctx_t ctx;
    if (nsync_scan_role_context_init(role_info, src_path, dst_path, &ctx) != 0 ||
        ctx.comm == MPI_COMM_NULL || ctx.root == NULL)
    {
        (*scan_errors)++;
        return;
    }

    size_t digest_bufsize = nsync_effective_bufsize(opts->bufsize);

    nsync_dir_task_vec_t frontier;
    nsync_dir_task_vec_init(&frontier);

    int local_fatal = 0;
    int root_owner = nsync_owner_index_in_group(".", ctx.comm_size);
    if (ctx.comm_rank == root_owner) {
        char* root_fullpath = nsync_build_full_path(ctx.root, ".");
        struct stat st;
        if (lstat(root_fullpath, &st) != 0) {
            /* Missing destination root means an empty target tree on first sync. */
            if (!(ctx.side == NSYNC_ROLE_DST && errno == ENOENT)) {
                (*scan_errors)++;
            }
        } else {
            if (item_count != NULL) {
                (*item_count)++;
            }

            if (nsync_scan_emit_path(
                    opts, root_fullpath, ".", &st, ctx.side,
                    out, scan_errors, filter, digest_bufsize, dirs_only, emit_fn, emit_arg) != 0)
            {
                local_fatal = 1;
            } else if (S_ISDIR(st.st_mode)) {
                if (nsync_dir_task_vec_push_copy(&frontier, ".") != 0) {
                    local_fatal = 1;
                }
            }
        }
        mfu_free(&root_fullpath);
    }

    int global_fatal = 0;
    MPI_Allreduce(&local_fatal, &global_fatal, 1, MPI_INT, MPI_MAX, ctx.comm);
    if (global_fatal != 0) {
        (*scan_errors)++;
        nsync_dir_task_vec_free(&frontier);
        return;
    }

    while (1) {
        uint64_t local_frontier = frontier.size;
        uint64_t global_frontier = 0;
        MPI_Allreduce(&local_frontier, &global_frontier, 1, MPI_UINT64_T, MPI_SUM, ctx.comm);
        if (global_frontier == 0) {
            break;
        }

        nsync_dir_task_vec_t next_local;
        nsync_dir_task_vec_init(&next_local);

        nsync_dir_task_vec_t* next_remote =
            (nsync_dir_task_vec_t*)MFU_MALLOC((size_t)ctx.comm_size * sizeof(nsync_dir_task_vec_t));
        for (int i = 0; i < ctx.comm_size; i++) {
            nsync_dir_task_vec_init(&next_remote[i]);
        }

        local_fatal = 0;
        for (uint64_t i = 0; i < frontier.size; i++) {
            nsync_scan_process_directory(
                opts, &ctx, frontier.paths[i], out, scan_errors, filter, item_count,
                digest_bufsize, dirs_only, emit_fn, emit_arg,
                &next_local, next_remote, &local_fatal);
            if (local_fatal) {
                break;
            }
        }

        MPI_Allreduce(&local_fatal, &global_fatal, 1, MPI_INT, MPI_MAX, ctx.comm);
        if (global_fatal == 0) {
            nsync_dir_task_vec_t received_remote;
            nsync_dir_task_vec_init(&received_remote);

            if (nsync_dir_task_exchange(&ctx, next_remote, &received_remote) != 0 ||
                nsync_dir_task_vec_append_take_all(&next_local, &received_remote) != 0)
            {
                global_fatal = 1;
            }

            nsync_dir_task_vec_free(&received_remote);
            MPI_Allreduce(&global_fatal, &local_fatal, 1, MPI_INT, MPI_MAX, ctx.comm);
            global_fatal = local_fatal;
        }

        for (int i = 0; i < ctx.comm_size; i++) {
            nsync_dir_task_vec_free(&next_remote[i]);
        }
        mfu_free(&next_remote);

        nsync_dir_task_vec_free(&frontier);
        frontier = next_local;

        if (global_fatal != 0) {
            (*scan_errors)++;
            break;
        }
    }

    nsync_dir_task_vec_free(&frontier);
}

static uint64_t nsync_compute_batch_count(
    const nsync_options_t* opts,
    uint64_t global_src_items,
    uint64_t global_dst_items)
{
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

    return batch_count;
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

typedef struct {
    nsync_meta_vec_t* planner;
    int include_digest;
} nsync_meta_unpack_arg_t;

static int nsync_meta_unpack_payload(const char* payload, size_t payload_size, void* arg)
{
    nsync_meta_unpack_arg_t* unpack_arg = (nsync_meta_unpack_arg_t*)arg;
    const char* ptr = payload;
    const char* end = payload + payload_size;
    nsync_meta_record_t rec;
    memset(&rec, 0, sizeof(rec));

    if (nsync_meta_unpack(&ptr, end, unpack_arg->include_digest, &rec) != 0 || ptr != end) {
        nsync_meta_record_free(&rec);
        return -1;
    }

    if (nsync_meta_vec_push(unpack_arg->planner, &rec) != 0) {
        nsync_meta_record_free(&rec);
        return -1;
    }

    return 0;
}

static int nsync_meta_unpack_relpath(
    const char* packed,
    size_t packed_size,
    int include_digest,
    const char** relpath_out,
    uint32_t* relpath_len_out)
{
    const char* ptr = packed;
    const char* end = packed + packed_size;

    size_t base_bytes = 4 + 4 + (6 * 8) + 4 + 4 + (include_digest ? (4 + SHA256_DIGEST_LENGTH) : 0);
    if ((size_t)(end - ptr) < base_bytes) {
        return -1;
    }

    uint32_t tmp32;
    uint32_t path_len;
    uint32_t link_len;
    uint64_t tmp64;

    mfu_unpack_uint32(&ptr, &tmp32);
    mfu_unpack_uint32(&ptr, &tmp32);
    mfu_unpack_uint64(&ptr, &tmp64);
    mfu_unpack_uint64(&ptr, &tmp64);
    mfu_unpack_uint64(&ptr, &tmp64);
    mfu_unpack_uint64(&ptr, &tmp64);
    mfu_unpack_uint64(&ptr, &tmp64);
    mfu_unpack_uint64(&ptr, &tmp64);
    mfu_unpack_uint32(&ptr, &path_len);
    mfu_unpack_uint32(&ptr, &link_len);

    if (include_digest) {
        mfu_unpack_uint32(&ptr, &tmp32);
        if ((size_t)(end - ptr) < SHA256_DIGEST_LENGTH) {
            return -1;
        }
        ptr += SHA256_DIGEST_LENGTH;
    }

    if ((size_t)(end - ptr) < (size_t)path_len + (size_t)link_len) {
        return -1;
    }

    *relpath_out = ptr;
    *relpath_len_out = path_len;
    return 0;
}

static void nsync_batch_spool_init(nsync_batch_spool_t* spool)
{
    memset(spool, 0, sizeof(*spool));
    spool->max_open_fds = 64;
    spool->raw_fd = -1;
    spool->io_error = 0;
}

static uint64_t nsync_batch_id_from_relpath_len(const char* relpath, size_t relpath_len, uint64_t batch_count)
{
    if (batch_count == 0) {
        return 0;
    }

    if (relpath_len == 1 && relpath[0] == '.') {
        return 0;
    }

    uint32_t hash = nsync_hash_bytes_with_domain(relpath, relpath_len, NSYNC_HASH_DOMAIN_BATCH);
    return (uint64_t)hash % batch_count;
}

static int nsync_batch_spool_path(
    const nsync_batch_spool_t* spool,
    uint64_t batch_id,
    char* path,
    size_t size)
{
    const char* suffix = spool->batch_index_mode ? ".idx" : ".bin";
    int written = snprintf(path, size, "%s/batch-%" PRIu64 "%s", spool->dir, batch_id, suffix);
    if (written <= 0 || (size_t)written >= size) {
        return -1;
    }
    return 0;
}

static void nsync_batch_spool_close_raw_fd(nsync_batch_spool_t* spool)
{
    if (spool->raw_fd >= 0) {
        close(spool->raw_fd);
        spool->raw_fd = -1;
    }
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
    int include_digest)
{
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

    size_t raw_path_len = strlen(spool->dir) + strlen("/scan-raw.bin") + 1;
    spool->raw_path = (char*)MFU_MALLOC(raw_path_len);
    snprintf(spool->raw_path, raw_path_len, "%s/scan-raw.bin", spool->dir);

    spool->raw_fd = open(spool->raw_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (spool->raw_fd < 0) {
        unlink(spool->raw_path);
        rmdir(spool->dir);
        mfu_free(&spool->raw_path);
        mfu_free(&spool->dir);
        return -1;
    }

    spool->batch_count = 0;
    spool->include_digest = include_digest;
    spool->batch_index_mode = 0;
    spool->open_fds = 0;
    spool->io_error = 0;
    spool->raw_has_data = 0;
    spool->raw_records_written = 0;
    spool->raw_bytes_written = 0;
    spool->records_written = 0;
    spool->bytes_written = 0;
    spool->batch_fds = NULL;
    spool->batch_has_data = NULL;

    return 0;
}

static int nsync_batch_spool_prepare_batches(nsync_batch_spool_t* spool, uint64_t batch_count)
{
    if (batch_count == 0) {
        return -1;
    }

    size_t count = (size_t)batch_count;
    if ((uint64_t)count != batch_count) {
        return -1;
    }

    spool->batch_count = batch_count;
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

static int nsync_batch_spool_write_record_fd(
    int fd,
    const char* packed,
    size_t packed_size,
    uint64_t* bytes_written_out)
{
    if (packed_size > UINT32_MAX) {
        return -1;
    }

    uint32_t len = (uint32_t)packed_size;
    if (nsync_batch_spool_write_all(fd, (const char*)&len, sizeof(len)) != 0 ||
        nsync_batch_spool_write_all(fd, packed, packed_size) != 0)
    {
        return -1;
    }

    if (bytes_written_out != NULL) {
        *bytes_written_out += (uint64_t)sizeof(len) + (uint64_t)packed_size;
    }

    return 0;
}

static int nsync_batch_spool_write_offset_fd(
    int fd,
    uint64_t offset,
    uint64_t* bytes_written_out)
{
    if (nsync_batch_spool_write_all(fd, (const char*)&offset, sizeof(offset)) != 0) {
        return -1;
    }

    if (bytes_written_out != NULL) {
        *bytes_written_out += (uint64_t)sizeof(offset);
    }

    return 0;
}

static int nsync_batch_spool_append_batch_index(
    nsync_batch_spool_t* spool,
    uint64_t batch_id,
    uint64_t raw_offset)
{
    int fd = nsync_batch_spool_get_append_fd(spool, batch_id);
    if (fd < 0) {
        spool->io_error = 1;
        return -1;
    }

    if (nsync_batch_spool_write_offset_fd(fd, raw_offset, &spool->bytes_written) != 0) {
        spool->io_error = 1;
        return -1;
    }

    spool->batch_has_data[(size_t)batch_id] = 1;
    spool->records_written++;
    return 0;
}

static int nsync_batch_spool_append_raw_record(nsync_batch_spool_t* spool, const nsync_meta_record_t* rec)
{
    if (spool->raw_fd < 0) {
        spool->io_error = 1;
        return -1;
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

    int rc = nsync_batch_spool_write_record_fd(spool->raw_fd, packed, packed_size, &spool->raw_bytes_written);
    if (rc == 0) {
        spool->raw_has_data = 1;
        spool->raw_records_written++;
    } else {
        spool->io_error = 1;
    }

    mfu_free(&packed);
    return rc;
}

static int nsync_batch_spool_scan_emit(const nsync_meta_record_t* rec, void* emit_arg)
{
    nsync_batch_spool_t* spool = (nsync_batch_spool_t*)emit_arg;
    return nsync_batch_spool_append_raw_record(spool, rec);
}

static int nsync_batch_spool_repartition_raw(nsync_batch_spool_t* spool)
{
    if (!spool->raw_has_data) {
        return 0;
    }

    int fd = open(spool->raw_path, O_RDONLY);
    if (fd < 0) {
        spool->io_error = 1;
        return -1;
    }

    int rc = 0;
    uint64_t raw_offset = 0;
    while (1) {
        uint32_t rec_len = 0;
        int eof = 0;
        if (nsync_batch_spool_read_all(fd, (char*)&rec_len, sizeof(rec_len), &eof) != 0) {
            rc = -1;
            spool->io_error = 1;
            break;
        }
        if (eof) {
            break;
        }

        uint64_t rec_offset = raw_offset;
        raw_offset += (uint64_t)sizeof(rec_len) + (uint64_t)rec_len;

        if (rec_len == 0) {
            rc = -1;
            spool->io_error = 1;
            break;
        }

        char* packed = (char*)MFU_MALLOC((size_t)rec_len);
        int payload_eof = 0;
        if (nsync_batch_spool_read_all(fd, packed, (size_t)rec_len, &payload_eof) != 0 || payload_eof) {
            mfu_free(&packed);
            rc = -1;
            spool->io_error = 1;
            break;
        }

        const char* relpath = NULL;
        uint32_t relpath_len = 0;
        if (nsync_meta_unpack_relpath(packed, (size_t)rec_len, spool->include_digest, &relpath, &relpath_len) != 0) {
            mfu_free(&packed);
            rc = -1;
            spool->io_error = 1;
            break;
        }

        uint64_t batch_id = nsync_batch_id_from_relpath_len(relpath, (size_t)relpath_len, spool->batch_count);
        if (batch_id >= spool->batch_count ||
            nsync_batch_spool_append_batch_index(spool, batch_id, rec_offset) != 0)
        {
            mfu_free(&packed);
            rc = -1;
            break;
        }

        mfu_free(&packed);
    }

    close(fd);
    nsync_batch_spool_close_open_fds(spool);

    return (rc == 0 && !spool->io_error) ? 0 : -1;
}

static int nsync_batch_spool_finalize(nsync_batch_spool_t* spool, uint64_t batch_count)
{
    if (nsync_batch_spool_prepare_batches(spool, batch_count) != 0) {
        spool->io_error = 1;
        return -1;
    }

    if (batch_count == 1) {
        spool->batch_index_mode = 0;
        spool->records_written = spool->raw_records_written;
        spool->bytes_written = spool->raw_bytes_written;

        if (spool->raw_has_data) {
            char path[PATH_MAX];
            if (nsync_batch_spool_path(spool, 0, path, sizeof(path)) != 0 ||
                rename(spool->raw_path, path) != 0)
            {
                spool->io_error = 1;
                return -1;
            }
            spool->batch_has_data[0] = 1;
        } else if (spool->raw_path != NULL) {
            unlink(spool->raw_path);
        }

        return 0;
    }

    spool->batch_index_mode = 1;
    return nsync_batch_spool_repartition_raw(spool);
}

static int nsync_batch_spool_scan_prepare(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_path,
    const char* dst_path,
    nsync_batch_spool_t* spool,
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

    int local_error = 0;
    if (nsync_batch_spool_prepare(spool, opts->contents ? 1 : 0) != 0) {
        local_error = 1;
    }

    if (nsync_sync_error_point(opts, "batch-spool-prepare", local_error) != 0) {
        return -1;
    }

    uint64_t local_items = 0;
    int local_scan_errors = 0;
    nsync_scan_role_path_filtered(
        opts, role_info, src_path, dst_path,
        NULL, &local_scan_errors, NULL, &local_items, 0,
        nsync_batch_spool_scan_emit, spool);

    nsync_batch_spool_close_raw_fd(spool);

    local_error = spool->io_error ? 1 : 0;
    if (nsync_sync_error_point(opts, "batch-spool-post-scan", local_error) != 0) {
        return -1;
    }

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

    uint64_t batch_count = nsync_compute_batch_count(opts, global_src_items, global_dst_items);
    if (nsync_batch_spool_finalize(spool, batch_count) != 0) {
        local_error = 1;
    } else {
        local_error = 0;
    }

    if (nsync_sync_error_point(opts, "batch-spool-finalize", local_error) != 0) {
        return -1;
    }

    *batch_count_out = batch_count;
    *global_src_items_out = global_src_items;
    *global_dst_items_out = global_dst_items;
    *local_scan_errors_out = local_scan_errors;
    return 0;
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

    if (spool->batch_index_mode) {
        int idx_fd = open(path, O_RDONLY);
        if (idx_fd < 0) {
            if (errno == ENOENT) {
                return 0;
            }
            return -1;
        }

        int raw_fd = open(spool->raw_path, O_RDONLY);
        if (raw_fd < 0) {
            close(idx_fd);
            return -1;
        }

        int rc = 0;
        while (1) {
            uint64_t raw_offset = 0;
            int eof = 0;
            if (nsync_batch_spool_read_all(idx_fd, (char*)&raw_offset, sizeof(raw_offset), &eof) != 0) {
                rc = -1;
                break;
            }
            if (eof) {
                break;
            }

            if (raw_offset > (uint64_t)LLONG_MAX ||
                lseek(raw_fd, (off_t)raw_offset, SEEK_SET) == (off_t)-1)
            {
                rc = -1;
                break;
            }

            uint32_t rec_len = 0;
            int len_eof = 0;
            if (nsync_batch_spool_read_all(raw_fd, (char*)&rec_len, sizeof(rec_len), &len_eof) != 0 || len_eof ||
                rec_len == 0)
            {
                rc = -1;
                break;
            }

            char* packed = (char*)MFU_MALLOC((size_t)rec_len);
            int payload_eof = 0;
            if (nsync_batch_spool_read_all(raw_fd, packed, (size_t)rec_len, &payload_eof) != 0 || payload_eof) {
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

        close(raw_fd);
        close(idx_fd);
        return rc;
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

    nsync_batch_spool_close_raw_fd(spool);
    nsync_batch_spool_close_open_fds(spool);

    if (spool->dir != NULL) {
        if (spool->raw_path != NULL) {
            unlink(spool->raw_path);
        }
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
    mfu_free(&spool->raw_path);
    mfu_free(&spool->dir);
    spool->batch_count = 0;
    spool->include_digest = 0;
    spool->batch_index_mode = 0;
    spool->open_fds = 0;
    spool->raw_fd = -1;
    spool->raw_has_data = 0;
    spool->io_error = 0;
    spool->raw_records_written = 0;
    spool->raw_bytes_written = 0;
    spool->records_written = 0;
    spool->bytes_written = 0;
}

static int nsync_owner_rank_for_path(const char* relpath, int ranks)
{
    return nsync_hash_bucket(relpath, ranks, NSYNC_HASH_DOMAIN_PLANNER);
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
    uint64_t* send_counts = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t* recv_counts = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t* send_displs = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t total_send = 0;
    uint64_t total_recv = 0;

    for (int i = 0; i < ranks; i++) {
        send_counts[i] = 0;
        recv_counts[i] = 0;
        send_displs[i] = 0;
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

        uint64_t wire_bytes = 0;
        if (nsync_frame_wire_bytes(bytes, &wire_bytes) != 0 || UINT64_MAX - send_counts[owner] < wire_bytes) {
            local_error = 1;
            break;
        }

        send_counts[owner] += wire_bytes;
    }

    if (nsync_sync_error_point(opts, "redistribute-pre-alltoall", local_error) != 0) {
        rc = -1;
        goto cleanup;
    }

    MPI_Alltoall(send_counts, 1, MPI_UINT64_T, recv_counts, 1, MPI_UINT64_T, MPI_COMM_WORLD);

    if (nsync_compute_u64_displacements(send_counts, ranks, send_displs, &total_send) != 0 ||
        nsync_compute_u64_displacements(recv_counts, ranks, NULL, &total_recv) != 0)
    {
        local_error = 1;
    }

    nsync_trace_local(opts, "redistribute-post-alltoall", total_send, total_recv);

    if (nsync_sync_error_point(opts, "redistribute-post-alltoall-sync", local_error) != 0) {
        rc = -1;
        goto cleanup;
    }

    size_t send_alloc = 0;
    if (nsync_u64_to_size_checked(total_send, &send_alloc) != 0) {
        local_error = 1;
    }
    if (!local_error) {
        send_buf = (char*)MFU_MALLOC(send_alloc > 0 ? send_alloc : 1);
    }

    for (uint64_t i = 0; i < local->size; i++) {
        const nsync_meta_record_t* rec = &local->records[i];
        int owner = nsync_owner_rank_for_path(rec->relpath, ranks);
        size_t bytes = nsync_meta_pack_size(rec, include_digest);
        uint64_t wire_bytes = 0;

        if (owner < 0 || owner >= ranks || nsync_frame_wire_bytes(bytes, &wire_bytes) != 0) {
            local_error = 1;
            break;
        }

        if (UINT64_MAX - send_displs[owner] < wire_bytes || send_displs[owner] + wire_bytes > total_send) {
            local_error = 1;
            break;
        }

        size_t write_offset = 0;
        if (nsync_u64_to_size_checked(send_displs[owner], &write_offset) != 0) {
            local_error = 1;
            break;
        }

        char* ptr = send_buf + write_offset;
        size_t packed = nsync_meta_pack(ptr + sizeof(uint32_t), rec, include_digest);
        size_t framed = nsync_frame_pack(ptr, ptr + sizeof(uint32_t), packed);
        if (packed != bytes || (uint64_t)framed != wire_bytes) {
            local_error = 1;
            break;
        }
        send_displs[owner] += wire_bytes;
    }

    if (nsync_sync_error_point(opts, "redistribute-pre-alltoallv", local_error) != 0) {
        rc = -1;
        goto cleanup;
    }

    for (int i = ranks - 1; i > 0; i--) {
        send_displs[i] = send_displs[i - 1];
    }
    if (ranks > 0) {
        send_displs[0] = 0;
    }

    nsync_meta_unpack_arg_t unpack_arg = {
        .planner = planner,
        .include_digest = include_digest,
    };
    if (nsync_exchange_framed_buffers(
            MPI_COMM_WORLD, rank, ranks,
            send_counts, recv_counts, send_displs, send_buf,
            nsync_meta_unpack_payload, &unpack_arg) != 0)
    {
        local_error = 1;
    }

    nsync_trace_local(opts, "redistribute-post-alltoallv", total_send, total_recv);

    nsync_trace_local(opts, "redistribute-post-unpack", planner->size, (uint64_t)local_error);

    if (nsync_sync_error_point(opts, "redistribute-post-unpack-sync", local_error) != 0) {
        rc = -1;
    }

cleanup:
    mfu_free(&send_counts);
    mfu_free(&recv_counts);
    mfu_free(&send_displs);
    mfu_free(&send_buf);

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

static int nsync_action_unpack_payload(const char* payload, size_t payload_size, void* arg)
{
    nsync_action_vec_t* exec_actions = (nsync_action_vec_t*)arg;
    const char* ptr = payload;
    const char* end = payload + payload_size;
    nsync_action_record_t action;
    memset(&action, 0, sizeof(action));

    if (nsync_action_unpack(&ptr, end, &action) != 0 || ptr != end) {
        nsync_action_record_free(&action);
        return -1;
    }

    if (nsync_action_vec_push(exec_actions, &action) != 0) {
        nsync_action_record_free(&action);
        return -1;
    }

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
    uint64_t* send_counts = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t* recv_counts = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t* send_displs = (uint64_t*)MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t total_send = 0;
    uint64_t total_recv = 0;

    for (int i = 0; i < ranks; i++) {
        send_counts[i] = 0;
        recv_counts[i] = 0;
        send_displs[i] = 0;
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
        uint64_t wire_bytes = 0;
        if (owner < 0 || owner >= ranks ||
            nsync_frame_wire_bytes(bytes, &wire_bytes) != 0 ||
            UINT64_MAX - send_counts[owner] < wire_bytes)
        {
            local_error = 1;
            break;
        }

        send_counts[owner] += wire_bytes;
    }

    if (nsync_sync_error_point(opts,
            to_src_side ? "action-redist-src-pre-alltoall" : "action-redist-dst-pre-alltoall",
            local_error) != 0)
    {
        rc = -1;
        goto cleanup;
    }

    MPI_Alltoall(send_counts, 1, MPI_UINT64_T, recv_counts, 1, MPI_UINT64_T, MPI_COMM_WORLD);

    if (nsync_compute_u64_displacements(send_counts, ranks, send_displs, &total_send) != 0 ||
        nsync_compute_u64_displacements(recv_counts, ranks, NULL, &total_recv) != 0)
    {
        local_error = 1;
    }

    if (nsync_sync_error_point(opts,
            to_src_side ? "action-redist-src-post-alltoall-sync" : "action-redist-dst-post-alltoall-sync",
            local_error) != 0)
    {
        rc = -1;
        goto cleanup;
    }

    size_t send_alloc = 0;
    if (nsync_u64_to_size_checked(total_send, &send_alloc) != 0) {
        local_error = 1;
    }
    if (!local_error) {
        send_buf = (char*)MFU_MALLOC(send_alloc > 0 ? send_alloc : 1);
    }

    for (uint64_t i = 0; i < local_actions->size; i++) {
        const nsync_action_record_t* action = &local_actions->records[i];
        int owner = -1;
        if (!nsync_action_owner_for_side(action, to_src_side, &owner)) {
            continue;
        }

        size_t bytes = nsync_action_pack_size(action);
        uint64_t wire_bytes = 0;
        if (owner < 0 || owner >= ranks ||
            nsync_frame_wire_bytes(bytes, &wire_bytes) != 0 ||
            UINT64_MAX - send_displs[owner] < wire_bytes ||
            send_displs[owner] + wire_bytes > total_send)
        {
            local_error = 1;
            break;
        }

        size_t write_offset = 0;
        if (nsync_u64_to_size_checked(send_displs[owner], &write_offset) != 0) {
            local_error = 1;
            break;
        }

        char* ptr = send_buf + write_offset;
        size_t packed = nsync_action_pack(ptr + sizeof(uint32_t), action);
        size_t framed = nsync_frame_pack(ptr, ptr + sizeof(uint32_t), packed);
        if (packed != bytes || (uint64_t)framed != wire_bytes) {
            local_error = 1;
            break;
        }
        send_displs[owner] += wire_bytes;
    }

    if (nsync_sync_error_point(opts,
            to_src_side ? "action-redist-src-pre-alltoallv" : "action-redist-dst-pre-alltoallv",
            local_error) != 0)
    {
        rc = -1;
        goto cleanup;
    }

    for (int i = ranks - 1; i > 0; i--) {
        send_displs[i] = send_displs[i - 1];
    }
    if (ranks > 0) {
        send_displs[0] = 0;
    }

    if (nsync_exchange_framed_buffers(
            MPI_COMM_WORLD, rank, ranks,
            send_counts, recv_counts, send_displs, send_buf,
            nsync_action_unpack_payload, exec_actions) != 0)
    {
        local_error = 1;
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
    mfu_free(&send_buf);

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
    int* local_errors,
    nsync_meta_apply_stats_t* meta_stats)
{
    if (path == NULL || action == NULL || local_errors == NULL) {
        return;
    }

    uid_t uid = (uid_t)action->uid;
    gid_t gid = (gid_t)action->gid;
    int chown_rc = nofollow ? lchown(path, uid, gid) : chown(path, uid, gid);
    if (chown_rc != 0) {
        int chown_err = errno;
        if (nsync_metadata_errno_ignorable(chown_err)) {
            if (meta_stats != NULL) {
                meta_stats->chown_ignored++;
            }
            MFU_LOG(MFU_LOG_WARN, "Skipping owner update for `%s`: %s", path, strerror(chown_err));
        } else {
            if (meta_stats != NULL) {
                meta_stats->chown_failed++;
            }
            (*local_errors)++;
            MFU_LOG(MFU_LOG_WARN, "Failed owner update for `%s`: %s", path, strerror(chown_err));
        }
    }

    if (!nofollow) {
        mode_t perms = (mode_t)(action->mode & 07777u);
        if (chmod(path, perms) != 0) {
            int chmod_err = errno;
            if (nsync_metadata_errno_ignorable(chmod_err)) {
                if (meta_stats != NULL) {
                    meta_stats->chmod_ignored++;
                }
                MFU_LOG(MFU_LOG_WARN, "Skipping mode update for `%s`: %s", path, strerror(chmod_err));
            } else {
                if (meta_stats != NULL) {
                    meta_stats->chmod_failed++;
                }
                (*local_errors)++;
                MFU_LOG(MFU_LOG_WARN, "Failed mode update for `%s`: %s", path, strerror(chmod_err));
            }
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
        int utime_err = errno;
        if (!nsync_metadata_errno_ignorable(utime_err)) {
            if (meta_stats != NULL) {
                meta_stats->utime_failed++;
            }
            (*local_errors)++;
            MFU_LOG(MFU_LOG_WARN, "Failed metadata timestamp update for `%s`: %s", path, strerror(utime_err));
        } else {
            if (meta_stats != NULL) {
                meta_stats->utime_ignored++;
            }
            MFU_LOG(MFU_LOG_WARN, "Skipping metadata timestamp update for `%s`: %s", path, strerror(utime_err));
        }
    }
}

static int nsync_u64_to_offt_checked(uint64_t value, off_t* out)
{
    if (value > (uint64_t)LLONG_MAX) {
        return -1;
    }

    *out = (off_t)value;
    return 0;
}

static int nsync_pwrite_all(int fd, const char* buf, size_t size, uint64_t offset)
{
    size_t written = 0;
    while (written < size) {
        off_t write_off = 0;
        if (nsync_u64_to_offt_checked(offset + (uint64_t)written, &write_off) != 0) {
            return -1;
        }

        ssize_t nwritten = pwrite(fd, buf + written, size - written, write_off);
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

static int nsync_is_all_null(const char* buf, size_t size)
{
    for (size_t i = 0; i < size; i++) {
        if (buf[i] != '\0') {
            return 0;
        }
    }
    return 1;
}

static int nsync_action_relpath_sort(const void* a, const void* b)
{
    const nsync_action_record_t* aa = (const nsync_action_record_t*)a;
    const nsync_action_record_t* bb = (const nsync_action_record_t*)b;
    return strcmp(aa->relpath, bb->relpath);
}

static int nsync_action_ptr_copy_size_desc_sort(const void* a, const void* b)
{
    const nsync_action_record_t* aa = *(const nsync_action_record_t* const*)a;
    const nsync_action_record_t* bb = *(const nsync_action_record_t* const*)b;
    if (aa->size != bb->size) {
        return (aa->size > bb->size) ? -1 : 1;
    }
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

static size_t nsync_copy_req_pack_size(uint64_t file_id, const char* relpath)
{
    (void)file_id;
    return 8 + 4 + strlen(relpath);
}

static size_t nsync_copy_req_pack(char* buf, uint64_t file_id, const char* relpath)
{
    uint32_t path_len = (uint32_t)strlen(relpath);
    char* ptr = buf;
    mfu_pack_uint64(&ptr, file_id);
    mfu_pack_uint32(&ptr, path_len);
    memcpy(ptr, relpath, (size_t)path_len);
    ptr += path_len;
    return (size_t)(ptr - buf);
}

static int nsync_copy_req_unpack(const char* buf, size_t bytes, uint64_t* file_id, char** relpath)
{
    const char* ptr = buf;
    const char* end = buf + bytes;
    if ((size_t)(end - ptr) < 12) {
        return -1;
    }

    mfu_unpack_uint64(&ptr, file_id);
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

static size_t nsync_copy_resp_pack_size(void)
{
    return 8 + 4;
}

static size_t nsync_copy_resp_pack(char* buf, uint64_t file_id, int status)
{
    char* ptr = buf;
    mfu_pack_uint64(&ptr, file_id);
    mfu_pack_uint32(&ptr, (uint32_t)(int32_t)status);
    return (size_t)(ptr - buf);
}

static int nsync_copy_resp_unpack(const char* buf, size_t bytes, uint64_t* file_id, int* status)
{
    const char* ptr = buf;
    const char* end = buf + bytes;
    if ((size_t)(end - ptr) < nsync_copy_resp_pack_size()) {
        return -1;
    }

    uint32_t status_u32 = 0;
    mfu_unpack_uint64(&ptr, file_id);
    mfu_unpack_uint32(&ptr, &status_u32);
    *status = (int)(int32_t)status_u32;
    return 0;
}

static size_t nsync_copy_frame_header_pack_size(void)
{
    return 8 + 8 + 4 + 4 + 4;
}

static size_t nsync_copy_frame_pack_size(uint32_t data_len)
{
    return nsync_copy_frame_header_pack_size() + (size_t)data_len;
}

static size_t nsync_copy_frame_pack(
    char* buf,
    uint64_t file_id,
    uint64_t offset,
    uint32_t flags,
    uint32_t logical_len,
    const char* data,
    uint32_t data_len)
{
    char* ptr = buf;
    mfu_pack_uint64(&ptr, file_id);
    mfu_pack_uint64(&ptr, offset);
    mfu_pack_uint32(&ptr, data_len);
    mfu_pack_uint32(&ptr, logical_len);
    mfu_pack_uint32(&ptr, flags);
    if (data_len > 0) {
        memcpy(ptr, data, (size_t)data_len);
        ptr += data_len;
    }
    return (size_t)(ptr - buf);
}

static int nsync_copy_frame_unpack(
    const char* buf,
    size_t bytes,
    nsync_copy_frame_header_t* header,
    const char** data_out)
{
    const char* ptr = buf;
    const char* end = buf + bytes;
    if ((size_t)(end - ptr) < nsync_copy_frame_header_pack_size()) {
        return -1;
    }

    mfu_unpack_uint64(&ptr, &header->file_id);
    mfu_unpack_uint64(&ptr, &header->offset);
    mfu_unpack_uint32(&ptr, &header->data_length);
    mfu_unpack_uint32(&ptr, &header->logical_length);
    mfu_unpack_uint32(&ptr, &header->flags);
    if ((header->flags & NSYNC_COPY_FRAME_FLAG_HOLE) != 0) {
        if (header->data_length != 0) {
            return -1;
        }
    } else if ((header->flags & NSYNC_COPY_FRAME_FLAG_END) == 0 &&
               header->data_length != header->logical_length)
    {
        return -1;
    }
    if ((header->flags & NSYNC_COPY_FRAME_FLAG_END) != 0 && header->data_length != 0) {
        return -1;
    }
    if ((size_t)(end - ptr) != (size_t)header->data_length) {
        return -1;
    }

    *data_out = ptr;
    return 0;
}

static int nsync_copy_send_slot_init(nsync_copy_send_slot_t* slot, size_t bufsize)
{
    size_t capacity = nsync_copy_frame_pack_size((uint32_t)bufsize);
    slot->wire_buf = (char*)MFU_MALLOC(capacity);
    if (slot->wire_buf == NULL) {
        return -1;
    }
    slot->capacity = capacity;
    slot->bytes = 0;
    slot->busy = 0;
    slot->req = MPI_REQUEST_NULL;
    return 0;
}

static void nsync_copy_send_slot_free(nsync_copy_send_slot_t* slot)
{
    if (slot->busy) {
        MPI_Wait(&slot->req, MPI_STATUS_IGNORE);
    }
    mfu_free(&slot->wire_buf);
    slot->capacity = 0;
    slot->bytes = 0;
    slot->busy = 0;
    slot->req = MPI_REQUEST_NULL;
}

static void nsync_source_transfer_mark_failed(
    nsync_source_transfer_t* transfer,
    int* local_errors)
{
    if (!transfer->failed) {
        (*local_errors)++;
    }
    transfer->failed = 1;
}

static int nsync_source_transfer_init(
    nsync_source_transfer_t* transfer,
    const char* src_root,
    const nsync_action_record_t* action,
    uint64_t file_id,
    int dst_rank,
    const nsync_options_t* opts)
{
    memset(transfer, 0, sizeof(*transfer));
    transfer->fd = -1;
    transfer->file_id = file_id;
    transfer->dst_rank = dst_rank;
    transfer->file_size = action->size;

    char* src_fullpath = nsync_build_full_path(src_root, action->relpath);
    transfer->fd = nsync_open_readonly(src_fullpath, opts);
    mfu_free(&src_fullpath);
    if (transfer->fd < 0) {
        return errno;
    }

    struct stat st;
    if (fstat(transfer->fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) {
        uint64_t allocated_bytes = (uint64_t)st.st_blocks * 512ULL;
        if (allocated_bytes < transfer->file_size) {
            transfer->zero_hole_fallback = 1;
        }
    }

    size_t bufsize = nsync_effective_bufsize(opts->bufsize);
    for (int i = 0; i < NSYNC_COPY_PIPELINE_DEPTH; i++) {
        if (nsync_copy_send_slot_init(&transfer->slots[i], bufsize) != 0) {
            for (int j = 0; j < i; j++) {
                nsync_copy_send_slot_free(&transfer->slots[j]);
            }
            close(transfer->fd);
            transfer->fd = -1;
            return ENOMEM;
        }
    }

#if defined(SEEK_DATA) && defined(SEEK_HOLE)
    transfer->sparse_seek_mode = (transfer->file_size > 0) ? 1 : 0;
#else
    transfer->sparse_seek_mode = 0;
#endif
    transfer->active = 1;
    return 0;
}

static void nsync_source_transfer_free(nsync_source_transfer_t* transfer)
{
    if (transfer->fd >= 0) {
        close(transfer->fd);
        transfer->fd = -1;
    }
    for (int i = 0; i < NSYNC_COPY_PIPELINE_DEPTH; i++) {
        nsync_copy_send_slot_free(&transfer->slots[i]);
    }
    transfer->active = 0;
}

static int nsync_source_transfer_has_busy_slots(const nsync_source_transfer_t* transfer)
{
    for (int i = 0; i < NSYNC_COPY_PIPELINE_DEPTH; i++) {
        if (transfer->slots[i].busy) {
            return 1;
        }
    }
    return 0;
}

static int nsync_source_active_transfer_count(
    const nsync_source_transfer_t* transfers,
    uint64_t count)
{
    int active = 0;
    for (uint64_t i = 0; i < count; i++) {
        if (transfers[i].active) {
            active++;
        }
    }
    return active;
}

static int nsync_source_has_active_transfer_for_dst(
    const nsync_source_transfer_t* transfers,
    uint64_t count,
    int dst_rank)
{
    for (uint64_t i = 0; i < count; i++) {
        if (transfers[i].active && transfers[i].dst_rank == dst_rank) {
            return 1;
        }
    }
    return 0;
}

static int nsync_source_transfer_prepare_next_message(
    nsync_source_transfer_t* transfer,
    nsync_copy_send_slot_t* slot,
    const nsync_options_t* opts,
    int* local_errors)
{
    (void)opts;

    if (transfer->end_sent) {
        return 0;
    }

    size_t header_size = nsync_copy_frame_header_pack_size();
    size_t bufsize = slot->capacity - header_size;
    while (1) {
        if (transfer->failed || transfer->next_offset >= transfer->file_size) {
            uint32_t flags = NSYNC_COPY_FRAME_FLAG_END;
            if (transfer->failed) {
                flags |= NSYNC_COPY_FRAME_FLAG_ERROR;
            }
            slot->bytes = nsync_copy_frame_pack(
                slot->wire_buf,
                transfer->file_id,
                transfer->next_offset,
                flags,
                0,
                NULL,
                0);
            transfer->end_sent = 1;
            return 1;
        }

        if (transfer->data_extent_end > transfer->next_offset) {
            uint64_t remaining = transfer->data_extent_end - transfer->next_offset;
            size_t read_size = (size_t)nsync_min_u64(remaining, (uint64_t)bufsize);
            off_t read_off = 0;
            if (nsync_u64_to_offt_checked(transfer->next_offset, &read_off) != 0) {
                nsync_source_transfer_mark_failed(transfer, local_errors);
                continue;
            }

            ssize_t nread = pread(transfer->fd, slot->wire_buf + header_size, read_size, read_off);
            if (nread <= 0) {
                nsync_source_transfer_mark_failed(transfer, local_errors);
                continue;
            }

            slot->bytes = nsync_copy_frame_pack(
                slot->wire_buf,
                transfer->file_id,
                transfer->next_offset,
                0,
                (uint32_t)nread,
                slot->wire_buf + header_size,
                (uint32_t)nread);
            transfer->next_offset += (uint64_t)nread;
            return 1;
        }

        if (transfer->sparse_seek_mode) {
#if defined(SEEK_DATA) && defined(SEEK_HOLE)
            off_t current_off = 0;
            if (nsync_u64_to_offt_checked(transfer->next_offset, &current_off) != 0) {
                nsync_source_transfer_mark_failed(transfer, local_errors);
                continue;
            }

            errno = 0;
            off_t data_off = lseek(transfer->fd, current_off, SEEK_DATA);
            if (data_off == (off_t)-1) {
                if (errno == ENXIO) {
                    uint64_t hole_len = transfer->file_size - transfer->next_offset;
                    uint32_t hole_chunk = (uint32_t)nsync_min_u64(hole_len, (uint64_t)UINT32_MAX);
                    slot->bytes = nsync_copy_frame_pack(
                        slot->wire_buf,
                        transfer->file_id,
                        transfer->next_offset,
                        NSYNC_COPY_FRAME_FLAG_HOLE,
                        hole_chunk,
                        NULL,
                        0);
                    transfer->next_offset += (uint64_t)hole_chunk;
                    return 1;
                }
                if (errno == EINVAL || errno == ENOTSUP || errno == EOPNOTSUPP) {
                    transfer->sparse_seek_mode = 0;
                    continue;
                }
                nsync_source_transfer_mark_failed(transfer, local_errors);
                continue;
            }

            if ((uint64_t)data_off > transfer->next_offset) {
                uint64_t hole_len = (uint64_t)data_off - transfer->next_offset;
                uint32_t hole_chunk = (uint32_t)nsync_min_u64(hole_len, (uint64_t)UINT32_MAX);
                slot->bytes = nsync_copy_frame_pack(
                    slot->wire_buf,
                    transfer->file_id,
                    transfer->next_offset,
                    NSYNC_COPY_FRAME_FLAG_HOLE,
                    hole_chunk,
                    NULL,
                    0);
                transfer->next_offset += (uint64_t)hole_chunk;
                return 1;
            }

            errno = 0;
            off_t hole_off = lseek(transfer->fd, data_off, SEEK_HOLE);
            if (hole_off == (off_t)-1 || hole_off < data_off) {
                if (errno == EINVAL || errno == ENOTSUP || errno == EOPNOTSUPP) {
                    transfer->sparse_seek_mode = 0;
                    continue;
                }
                nsync_source_transfer_mark_failed(transfer, local_errors);
                continue;
            }

            transfer->data_extent_end = (uint64_t)hole_off;
            continue;
#else
            transfer->sparse_seek_mode = 0;
            continue;
#endif
        }

        uint64_t remaining = transfer->file_size - transfer->next_offset;
        size_t read_size = (size_t)nsync_min_u64(remaining, (uint64_t)bufsize);
        off_t read_off = 0;
        if (nsync_u64_to_offt_checked(transfer->next_offset, &read_off) != 0) {
            nsync_source_transfer_mark_failed(transfer, local_errors);
            continue;
        }

        ssize_t nread = pread(transfer->fd, slot->wire_buf + header_size, read_size, read_off);
        if (nread <= 0) {
            nsync_source_transfer_mark_failed(transfer, local_errors);
            continue;
        }

        uint32_t flags = 0;
        const char* data = slot->wire_buf + header_size;
        if (transfer->zero_hole_fallback && nsync_is_all_null(data, (size_t)nread)) {
            flags |= NSYNC_COPY_FRAME_FLAG_HOLE;
            data = NULL;
        }
        slot->bytes = nsync_copy_frame_pack(
            slot->wire_buf,
            transfer->file_id,
            transfer->next_offset,
            flags,
            (uint32_t)nread,
            data,
            (flags & NSYNC_COPY_FRAME_FLAG_HOLE) ? 0U : (uint32_t)nread);
        transfer->next_offset += (uint64_t)nread;
        return 1;
    }
}

static int nsync_source_transfer_progress(
    nsync_source_transfer_t* transfer,
    const nsync_options_t* opts,
    int* local_errors)
{
    int made_progress = 0;
    for (int i = 0; i < NSYNC_COPY_PIPELINE_DEPTH; i++) {
        nsync_copy_send_slot_t* slot = &transfer->slots[i];
        if (!slot->busy) {
            continue;
        }

        int complete = 0;
        MPI_Test(&slot->req, &complete, MPI_STATUS_IGNORE);
        if (complete) {
            slot->busy = 0;
            slot->req = MPI_REQUEST_NULL;
            made_progress = 1;
        }
    }

    for (int i = 0; i < NSYNC_COPY_PIPELINE_DEPTH; i++) {
        nsync_copy_send_slot_t* slot = &transfer->slots[i];
        if (slot->busy) {
            continue;
        }
        if (nsync_source_transfer_prepare_next_message(transfer, slot, opts, local_errors) != 1) {
            continue;
        }

        MPI_Isend(
            slot->wire_buf,
            (int)slot->bytes,
            MPI_BYTE,
            transfer->dst_rank,
            NSYNC_MSG_COPY_DATA_LEN,
            MPI_COMM_WORLD,
            &slot->req);
        slot->busy = 1;
        made_progress = 1;
    }

    if (transfer->end_sent && !nsync_source_transfer_has_busy_slots(transfer)) {
        transfer->completed = 1;
    }
    return made_progress;
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

    nsync_source_transfer_t* transfers = NULL;
    nsync_source_request_t* pending = NULL;
    if (expected > 0) {
        transfers = (nsync_source_transfer_t*)calloc((size_t)expected, sizeof(*transfers));
        pending = (nsync_source_request_t*)calloc((size_t)expected, sizeof(*pending));
    }
    if (expected > 0 && (transfers == NULL || pending == NULL)) {
        free(transfers);
        free(pending);
        (*local_errors)++;
        return;
    }

    int file_credits = nsync_copy_file_credit_limit();
    uint64_t received = 0;
    uint64_t completed = 0;
    while (completed < expected) {
        int made_progress = 0;

        for (uint64_t i = 0; i < expected; i++) {
            nsync_source_transfer_t* transfer = &transfers[i];
            if (!transfer->active) {
                continue;
            }

            if (nsync_source_transfer_progress(transfer, opts, local_errors)) {
                made_progress = 1;
            }
            if (transfer->completed) {
                nsync_trace_local(opts, "copy-src-done", transfer->file_id, (uint64_t)transfer->dst_rank);
                nsync_source_transfer_free(transfer);
                completed++;
                made_progress = 1;
            }
        }

        int active_count = nsync_source_active_transfer_count(transfers, expected);
        for (uint64_t i = 0; i < expected && active_count < file_credits; i++) {
            nsync_source_request_t* req = &pending[i];
            if (!req->active || nsync_source_has_active_transfer_for_dst(transfers, expected, req->dst_rank)) {
                continue;
            }

            uint64_t transfer_idx = expected;
            for (uint64_t idx = 0; idx < expected; idx++) {
                if (!transfers[idx].active) {
                    transfer_idx = idx;
                    break;
                }
            }
            if (transfer_idx >= expected) {
                break;
            }

            int resp_status = nsync_source_transfer_init(
                &transfers[transfer_idx], src_root, req->action, req->file_id, req->dst_rank, opts);
            if (resp_status == 0) {
                nsync_trace_local(opts, "copy-src-start", req->file_id, (uint64_t)req->dst_rank);
            }
            if (resp_status != 0) {
                (*local_errors)++;
                completed++;
            } else {
                active_count++;
            }

            char resp_buf[16];
            size_t resp_bytes = nsync_copy_resp_pack(resp_buf, req->file_id, resp_status);
            MPI_Send(
                resp_buf,
                (int)resp_bytes,
                MPI_BYTE,
                req->dst_rank,
                NSYNC_MSG_COPY_RESP,
                MPI_COMM_WORLD);

            req->active = 0;
            req->action = NULL;
            made_progress = 1;
        }

        if (received < expected) {
            int flag = 0;
            MPI_Status probe_status;
            MPI_Iprobe(MPI_ANY_SOURCE, NSYNC_MSG_COPY_REQ, MPI_COMM_WORLD, &flag, &probe_status);
            if (flag) {
                int bytes = 0;
                MPI_Get_count(&probe_status, MPI_BYTE, &bytes);
                char* req_buf = (char*)MFU_MALLOC((size_t)bytes);
                MPI_Recv(
                    req_buf,
                    bytes,
                    MPI_BYTE,
                    probe_status.MPI_SOURCE,
                    NSYNC_MSG_COPY_REQ,
                    MPI_COMM_WORLD,
                    MPI_STATUS_IGNORE);

                uint64_t file_id = 0;
                char* relpath = NULL;
                int resp_status = EPROTO;
                if (bytes > 0 &&
                    nsync_copy_req_unpack(req_buf, (size_t)bytes, &file_id, &relpath) == 0)
                {
                    const nsync_action_record_t* action =
                        nsync_find_copy_action_sorted(src_actions, relpath);
                    if (action != NULL) {
                        uint64_t pending_idx = expected;
                        for (uint64_t idx = 0; idx < expected; idx++) {
                            if (!pending[idx].active) {
                                pending_idx = idx;
                                break;
                            }
                        }
                        if (pending_idx < expected) {
                            pending[pending_idx].active = 1;
                            pending[pending_idx].file_id = file_id;
                            pending[pending_idx].dst_rank = probe_status.MPI_SOURCE;
                            pending[pending_idx].action = action;
                            nsync_trace_local(opts, "copy-src-queue", file_id, (uint64_t)probe_status.MPI_SOURCE);
                            resp_status = 0;
                        } else {
                            resp_status = EBUSY;
                            (*local_errors)++;
                        }
                    } else {
                        resp_status = ENOENT;
                        (*local_errors)++;
                    }
                } else {
                    (*local_errors)++;
                }

                if (resp_status != 0) {
                    char resp_buf[16];
                    size_t resp_bytes = nsync_copy_resp_pack(resp_buf, file_id, resp_status);
                    MPI_Send(
                        resp_buf,
                        (int)resp_bytes,
                        MPI_BYTE,
                        probe_status.MPI_SOURCE,
                        NSYNC_MSG_COPY_RESP,
                        MPI_COMM_WORLD);
                    completed++;
                }
                received++;
                mfu_free(&relpath);
                mfu_free(&req_buf);
                made_progress = 1;
            }
        }

        if (!made_progress) {
            for (uint64_t i = 0; i < expected; i++) {
                nsync_source_transfer_t* transfer = &transfers[i];
                if (!transfer->active) {
                    continue;
                }
                for (int slot_idx = 0; slot_idx < NSYNC_COPY_PIPELINE_DEPTH; slot_idx++) {
                    nsync_copy_send_slot_t* slot = &transfer->slots[slot_idx];
                    if (slot->busy) {
                        MPI_Wait(&slot->req, MPI_STATUS_IGNORE);
                        slot->busy = 0;
                        slot->req = MPI_REQUEST_NULL;
                        made_progress = 1;
                        break;
                    }
                }
                if (made_progress) {
                    break;
                }
            }

            if (made_progress) {
                continue;
            }

            if (received < expected) {
                MPI_Status probe_status;
                MPI_Probe(MPI_ANY_SOURCE, NSYNC_MSG_COPY_REQ, MPI_COMM_WORLD, &probe_status);
                continue;
            }
        }
    }

    for (uint64_t i = 0; i < expected; i++) {
        if (transfers[i].active) {
            nsync_source_transfer_free(&transfers[i]);
        }
    }
    free(transfers);
    free(pending);
}

static int nsync_copy_recv_slot_init(nsync_copy_recv_slot_t* slot, size_t bufsize)
{
    size_t capacity = nsync_copy_frame_pack_size((uint32_t)bufsize);
    slot->wire_buf = (char*)MFU_MALLOC(capacity);
    if (slot->wire_buf == NULL) {
        return -1;
    }
    slot->capacity = capacity;
    slot->bytes = 0;
    slot->busy = 0;
    slot->src_rank = -1;
    slot->req = MPI_REQUEST_NULL;
    return 0;
}

static void nsync_copy_recv_slot_free(nsync_copy_recv_slot_t* slot)
{
    if (slot->busy) {
        MPI_Wait(&slot->req, MPI_STATUS_IGNORE);
    }
    mfu_free(&slot->wire_buf);
    slot->capacity = 0;
    slot->bytes = 0;
    slot->busy = 0;
    slot->src_rank = -1;
    slot->req = MPI_REQUEST_NULL;
}

static int nsync_copy_recv_slots_busy_for_source(
    const nsync_copy_recv_slot_t* slots,
    uint64_t slot_count,
    int src_rank)
{
    for (uint64_t i = 0; i < slot_count; i++) {
        if (slots[i].busy && slots[i].src_rank == src_rank) {
            return 1;
        }
    }
    return 0;
}

static nsync_destination_transfer_t* nsync_destination_find_transfer(
    nsync_destination_transfer_t* transfers,
    uint64_t count,
    uint64_t file_id)
{
    for (uint64_t i = 0; i < count; i++) {
        if (transfers[i].active && transfers[i].file_id == file_id) {
            return &transfers[i];
        }
    }
    return NULL;
}

static int nsync_destination_start_copy_transfer(
    const char* dst_root,
    const nsync_action_record_t* action,
    uint64_t file_id,
    nsync_destination_transfer_t* transfer,
    const nsync_options_t* opts,
    int* local_errors)
{
    memset(transfer, 0, sizeof(*transfer));
    transfer->active = 1;
    transfer->file_id = file_id;
    transfer->src_rank = action->src_owner_world;
    transfer->fd = -1;
    transfer->action = action;
    transfer->dst_fullpath = nsync_build_full_path(dst_root, action->relpath);

    if (nsync_ensure_parent_dirs(transfer->dst_fullpath) != 0) {
        (*local_errors)++;
        transfer->open_error = 1;
    }

    mode_t mode = (mode_t)(action->mode & 07777u);
    if (mode == 0) {
        mode = (mode_t)0644;
    }

    if (!transfer->open_error) {
        transfer->fd = nsync_open_writeonly_create_trunc(transfer->dst_fullpath, mode, opts);
        if (transfer->fd < 0) {
            (*local_errors)++;
            transfer->open_error = 1;
        }
    }

    if (transfer->fd >= 0) {
        off_t file_size = 0;
        if (nsync_u64_to_offt_checked(action->size, &file_size) != 0 ||
            ftruncate(transfer->fd, file_size) != 0)
        {
            (*local_errors)++;
            close(transfer->fd);
            transfer->fd = -1;
            transfer->open_error = 1;
        }
    }

    size_t req_bytes = nsync_copy_req_pack_size(file_id, action->relpath);
    char* req_buf = (char*)MFU_MALLOC(req_bytes);
    nsync_copy_req_pack(req_buf, file_id, action->relpath);
    MPI_Send(req_buf, (int)req_bytes, MPI_BYTE, action->src_owner_world, NSYNC_MSG_COPY_REQ, MPI_COMM_WORLD);
    mfu_free(&req_buf);
    return 0;
}

static void nsync_destination_finalize_copy_transfer(
    nsync_destination_transfer_t* transfer,
    const nsync_options_t* opts,
    int* local_errors,
    nsync_meta_apply_stats_t* meta_stats,
    uint64_t* done_actions,
    uint64_t* done_copy_files,
    uint64_t* done_copy_bytes)
{
    int success = 1;
    if (!transfer->response_received || transfer->response_error || transfer->transfer_error || transfer->open_error) {
        success = 0;
    }

    if (transfer->fd >= 0) {
        off_t file_size = 0;
        if (success &&
            (nsync_u64_to_offt_checked(transfer->action->size, &file_size) != 0 ||
             ftruncate(transfer->fd, file_size) != 0))
        {
            (*local_errors)++;
            success = 0;
        }

        if (close(transfer->fd) != 0) {
            (*local_errors)++;
            success = 0;
        }
        transfer->fd = -1;
    }

    if (success) {
        nsync_apply_metadata(transfer->dst_fullpath, transfer->action, 0, opts, local_errors, meta_stats);
        if (done_actions != NULL) {
            (*done_actions)++;
        }
        if (done_copy_files != NULL) {
            (*done_copy_files)++;
        }
        if (done_copy_bytes != NULL) {
            (*done_copy_bytes) += transfer->copied_bytes;
        }
    } else if (transfer->dst_fullpath != NULL) {
        (void)nsync_remove_path(transfer->dst_fullpath);
    }

    nsync_trace_local(opts, "copy-dst-finalize", transfer->file_id, (uint64_t)success);

    mfu_free(&transfer->dst_fullpath);
    transfer->completed = 1;
    transfer->active = 0;
}

static void nsync_destination_handle_copy_response(
    nsync_destination_transfer_t* transfers,
    uint64_t transfer_count,
    const nsync_options_t* opts,
    int* local_errors)
{
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD, &status);
    int bytes = 0;
    MPI_Get_count(&status, MPI_BYTE, &bytes);
    if (bytes <= 0) {
        MPI_Recv(NULL, 0, MPI_BYTE, status.MPI_SOURCE, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        (*local_errors)++;
        return;
    }

    char* buf = (char*)MFU_MALLOC((size_t)bytes);
    MPI_Recv(buf, bytes, MPI_BYTE, status.MPI_SOURCE, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    uint64_t file_id = 0;
    int resp_status = EPROTO;
    if (nsync_copy_resp_unpack(buf, (size_t)bytes, &file_id, &resp_status) != 0) {
        mfu_free(&buf);
        (*local_errors)++;
        return;
    }

    nsync_destination_transfer_t* transfer =
        nsync_destination_find_transfer(transfers, transfer_count, file_id);
    if (transfer == NULL) {
        mfu_free(&buf);
        (*local_errors)++;
        return;
    }

    transfer->response_received = 1;
    nsync_trace_local(opts, "copy-dst-response", file_id, (uint64_t)(uint32_t)resp_status);
    if (resp_status != 0) {
        transfer->response_error = 1;
        transfer->end_received = 1;
        (*local_errors)++;
    }
    mfu_free(&buf);
}

static void nsync_destination_handle_copy_frame(
    nsync_destination_transfer_t* transfers,
    uint64_t transfer_count,
    nsync_copy_recv_slot_t* slot,
    const nsync_options_t* opts,
    int* local_errors)
{
    nsync_copy_frame_header_t header;
    const char* data = NULL;
    if (nsync_copy_frame_unpack(slot->wire_buf, slot->bytes, &header, &data) != 0) {
        (*local_errors)++;
        return;
    }

    nsync_destination_transfer_t* transfer =
        nsync_destination_find_transfer(transfers, transfer_count, header.file_id);
    if (transfer == NULL) {
        (*local_errors)++;
        return;
    }

    if (header.flags & NSYNC_COPY_FRAME_FLAG_ERROR) {
        transfer->transfer_error = 1;
        (*local_errors)++;
    }

    if ((header.flags & NSYNC_COPY_FRAME_FLAG_HOLE) == 0 &&
        header.data_length > 0 &&
        transfer->fd >= 0 &&
        !transfer->open_error)
    {
        if (nsync_pwrite_all(transfer->fd, data, (size_t)header.data_length, header.offset) != 0) {
            (*local_errors)++;
            transfer->transfer_error = 1;
        }
    }

    if ((header.flags & NSYNC_COPY_FRAME_FLAG_END) == 0) {
        transfer->copied_bytes += (uint64_t)header.logical_length;
    }

    if (header.flags & NSYNC_COPY_FRAME_FLAG_END) {
        transfer->end_received = 1;
        nsync_trace_local(opts, "copy-dst-end", header.file_id, transfer->copied_bytes);
    }
}

static void nsync_destination_execute_copy_actions(
    const char* dst_root,
    const nsync_action_ptr_vec_t* copy_actions,
    const nsync_options_t* opts,
    int* local_errors,
    nsync_meta_apply_stats_t* meta_stats,
    uint64_t* done_actions,
    uint64_t* done_copy_files,
    uint64_t* done_copy_bytes)
{
    if (copy_actions->size == 0) {
        return;
    }

    int world_size = 0;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    int file_credits = nsync_copy_file_credit_limit();

    uint64_t unique_sources = 0;
    int* seen_sources = (int*)calloc((size_t)world_size, sizeof(int));
    if (seen_sources == NULL) {
        (*local_errors)++;
        return;
    }
    for (uint64_t i = 0; i < copy_actions->size; i++) {
        int src_rank = copy_actions->records[i]->src_owner_world;
        if (src_rank >= 0 && src_rank < world_size && !seen_sources[src_rank]) {
            seen_sources[src_rank] = 1;
            unique_sources++;
        }
    }
    free(seen_sources);

    uint64_t recv_slot_count = nsync_min_u64(
        copy_actions->size,
        unique_sources * (uint64_t)file_credits * (uint64_t)NSYNC_COPY_PIPELINE_DEPTH);
    if (recv_slot_count == 0) {
        recv_slot_count = 1;
    }

    size_t bufsize = nsync_effective_bufsize(opts->bufsize);
    nsync_copy_recv_slot_t* recv_slots =
        (nsync_copy_recv_slot_t*)calloc((size_t)recv_slot_count, sizeof(*recv_slots));
    nsync_destination_transfer_t* transfers =
        (nsync_destination_transfer_t*)calloc((size_t)copy_actions->size, sizeof(*transfers));
    int* per_source_active = (int*)calloc((size_t)world_size, sizeof(int));
    unsigned char* launched = (unsigned char*)calloc((size_t)copy_actions->size, sizeof(unsigned char));
    if (recv_slots == NULL || transfers == NULL || per_source_active == NULL || launched == NULL) {
        (*local_errors)++;
        free(recv_slots);
        free(transfers);
        free(per_source_active);
        free(launched);
        return;
    }

    int recv_init_error = 0;
    for (uint64_t i = 0; i < recv_slot_count; i++) {
        if (nsync_copy_recv_slot_init(&recv_slots[i], bufsize) != 0) {
            (*local_errors)++;
            recv_slot_count = i;
            recv_init_error = 1;
            break;
        }
    }
    if (recv_init_error) {
        for (uint64_t i = 0; i < recv_slot_count; i++) {
            nsync_copy_recv_slot_free(&recv_slots[i]);
        }
        free(recv_slots);
        free(transfers);
        free(per_source_active);
        free(launched);
        return;
    }

    uint64_t completed = 0;
    uint64_t next_file_id = 1;
    while (completed < copy_actions->size) {
        int made_progress = 0;

        for (uint64_t i = 0; i < copy_actions->size; i++) {
            const nsync_action_record_t* action = copy_actions->records[i];
            int src_rank = action->src_owner_world;
            if (launched[i] || src_rank < 0 || src_rank >= world_size ||
                per_source_active[src_rank] >= 1)
            {
                continue;
            }

            if (nsync_destination_start_copy_transfer(
                    dst_root, action, next_file_id, &transfers[i], opts, local_errors) != 0)
            {
                (*local_errors)++;
                continue;
            }
            nsync_trace_local(opts, "copy-dst-launch", next_file_id, (uint64_t)src_rank);
            launched[i] = 1;
            per_source_active[src_rank]++;
            next_file_id++;
            made_progress = 1;
        }

        while (1) {
            int flag = 0;
            MPI_Status status;
            MPI_Iprobe(MPI_ANY_SOURCE, NSYNC_MSG_COPY_RESP, MPI_COMM_WORLD, &flag, &status);
            if (!flag) {
                break;
            }
            nsync_destination_handle_copy_response(transfers, copy_actions->size, opts, local_errors);
            made_progress = 1;
        }

        for (uint64_t i = 0; i < recv_slot_count; i++) {
            nsync_copy_recv_slot_t* slot = &recv_slots[i];
            if (!slot->busy) {
                continue;
            }

            int complete = 0;
            MPI_Test(&slot->req, &complete, MPI_STATUS_IGNORE);
            if (complete) {
                slot->busy = 0;
                slot->req = MPI_REQUEST_NULL;
                nsync_destination_handle_copy_frame(transfers, copy_actions->size, slot, opts, local_errors);
                slot->bytes = 0;
                slot->src_rank = -1;
                made_progress = 1;
            }
        }

        for (uint64_t i = 0; i < recv_slot_count; i++) {
            nsync_copy_recv_slot_t* slot = &recv_slots[i];
            if (slot->busy) {
                continue;
            }

            int flag = 0;
            MPI_Status status;
            MPI_Iprobe(MPI_ANY_SOURCE, NSYNC_MSG_COPY_DATA_LEN, MPI_COMM_WORLD, &flag, &status);
            if (!flag) {
                break;
            }

            int bytes = 0;
            MPI_Get_count(&status, MPI_BYTE, &bytes);
            if ((size_t)bytes > slot->capacity) {
                char* new_buf = (char*)realloc(slot->wire_buf, (size_t)bytes);
                if (new_buf == NULL) {
                    (*local_errors)++;
                    char* drain = (char*)MFU_MALLOC((size_t)bytes);
                    MPI_Recv(drain, bytes, MPI_BYTE, status.MPI_SOURCE, NSYNC_MSG_COPY_DATA_LEN,
                        MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    mfu_free(&drain);
                    continue;
                }
                slot->wire_buf = new_buf;
                slot->capacity = (size_t)bytes;
            }

            MPI_Irecv(
                slot->wire_buf,
                bytes,
                MPI_BYTE,
                status.MPI_SOURCE,
                NSYNC_MSG_COPY_DATA_LEN,
                MPI_COMM_WORLD,
                &slot->req);
            slot->busy = 1;
            slot->bytes = (size_t)bytes;
            slot->src_rank = status.MPI_SOURCE;
            made_progress = 1;
            break;
        }

        for (uint64_t i = 0; i < copy_actions->size; i++) {
            nsync_destination_transfer_t* transfer = &transfers[i];
            if (!transfer->active ||
                !transfer->response_received ||
                !transfer->end_received ||
                nsync_copy_recv_slots_busy_for_source(recv_slots, recv_slot_count, transfer->src_rank))
            {
                continue;
            }

            nsync_destination_finalize_copy_transfer(
                transfer, opts, local_errors, meta_stats,
                done_actions, done_copy_files, done_copy_bytes);
            if (transfer->src_rank >= 0 && transfer->src_rank < world_size && per_source_active[transfer->src_rank] > 0) {
                per_source_active[transfer->src_rank]--;
            }
            completed++;
            made_progress = 1;
        }

        if (!made_progress) {
            int waited = 0;
            for (uint64_t i = 0; i < recv_slot_count; i++) {
                nsync_copy_recv_slot_t* slot = &recv_slots[i];
                if (!slot->busy) {
                    continue;
                }
                MPI_Wait(&slot->req, MPI_STATUS_IGNORE);
                slot->busy = 0;
                slot->req = MPI_REQUEST_NULL;
                nsync_destination_handle_copy_frame(transfers, copy_actions->size, slot, opts, local_errors);
                slot->bytes = 0;
                slot->src_rank = -1;
                waited = 1;
                break;
            }

            if (!waited) {
                MPI_Status status;
                MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            }
        }
    }

    for (uint64_t i = 0; i < recv_slot_count; i++) {
        nsync_copy_recv_slot_free(&recv_slots[i]);
    }
    for (uint64_t i = 0; i < copy_actions->size; i++) {
        if (transfers[i].active) {
            nsync_destination_finalize_copy_transfer(
                &transfers[i], opts, local_errors, meta_stats,
                done_actions, done_copy_files, done_copy_bytes);
        }
    }

    free(recv_slots);
    free(transfers);
    free(per_source_active);
    free(launched);
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
    int* local_errors,
    nsync_meta_apply_stats_t* meta_stats,
    uint64_t* done_actions,
    uint64_t* done_copy_files,
    uint64_t* done_copy_bytes)
{
    if (dst_actions->size == 0) {
        return;
    }

    qsort(dst_actions->records, (size_t)dst_actions->size, sizeof(nsync_action_record_t), nsync_action_exec_sort);

    nsync_action_ptr_vec_t copy_actions;
    nsync_action_ptr_vec_init(&copy_actions);

    uint64_t local_done_actions = 0;
    uint64_t local_done_copy_files = 0;
    uint64_t local_done_copy_bytes = 0;

    for (uint64_t i = 0; i < dst_actions->size; i++) {
        const nsync_action_record_t* action = &dst_actions->records[i];
        if (strcmp(action->relpath, ".") == 0 &&
            (action->type == NSYNC_ACTION_REMOVE || action->type == NSYNC_ACTION_MKDIR))
        {
            continue;
        }

        int action_completed = 0;
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
            } else {
                action_completed = 1;
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
                action_completed = 1;
            }
            break;
        }
        case NSYNC_ACTION_SYMLINK_UPDATE: {
            int errors_before = *local_errors;
            if (nsync_ensure_parent_dirs(dst_fullpath) != 0) {
                (*local_errors)++;
            }
            if (nsync_remove_path(dst_fullpath) != 0 && errno != ENOENT) {
                (*local_errors)++;
            }
            if (action->link_target == NULL || symlink(action->link_target, dst_fullpath) != 0) {
                (*local_errors)++;
            } else {
                nsync_apply_metadata(dst_fullpath, action, 1, opts, local_errors, meta_stats);
            }
            if (*local_errors == errors_before) {
                action_completed = 1;
            }
            break;
        }
        case NSYNC_ACTION_COPY:
            if (nsync_action_ptr_vec_push(&copy_actions, action) != 0) {
                (*local_errors)++;
            }
            break;
        case NSYNC_ACTION_META_UPDATE:
            if (S_ISDIR((mode_t)action->mode)) {
                nsync_deferred_dir_meta_add(deferred_dir_meta_updates, action, local_errors);
            } else {
                int errors_before = *local_errors;
                int nofollow = S_ISLNK((mode_t)action->mode) ? 1 : 0;
                nsync_apply_metadata(dst_fullpath, action, nofollow, opts, local_errors, meta_stats);
                if (*local_errors == errors_before) {
                    action_completed = 1;
                }
            }
            break;
        default:
            break;
        }

        if (action_completed) {
            local_done_actions++;
        }
        mfu_free(&dst_fullpath);
    }

    if (copy_actions.size > 1) {
        qsort(copy_actions.records, (size_t)copy_actions.size, sizeof(*copy_actions.records),
            nsync_action_ptr_copy_size_desc_sort);
    }

    nsync_destination_execute_copy_actions(
        dst_root,
        &copy_actions,
        opts,
        local_errors,
        meta_stats,
        &local_done_actions,
        &local_done_copy_files,
        &local_done_copy_bytes);
    nsync_action_ptr_vec_free(&copy_actions);

    if (done_actions != NULL) {
        (*done_actions) += local_done_actions;
    }
    if (done_copy_files != NULL) {
        (*done_copy_files) += local_done_copy_files;
    }
    if (done_copy_bytes != NULL) {
        (*done_copy_bytes) += local_done_copy_bytes;
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
    int* local_errors,
    nsync_meta_apply_stats_t* meta_stats,
    uint64_t* completed_actions,
    uint64_t* completed_copy_files,
    uint64_t* completed_copy_bytes)
{
    if (completed_actions != NULL) {
        *completed_actions = 0;
    }
    if (completed_copy_files != NULL) {
        *completed_copy_files = 0;
    }
    if (completed_copy_bytes != NULL) {
        *completed_copy_bytes = 0;
    }

    if (role_info->role == NSYNC_ROLE_SRC) {
        nsync_trace_local(opts, "phase4-src-service", src_exec_actions->size, 0);
        nsync_source_copy_service(src_root, src_exec_actions, opts, local_errors);
    } else if (role_info->role == NSYNC_ROLE_DST) {
        nsync_trace_local(opts, "phase4-dst-exec", dst_exec_actions->size, 0);
        nsync_destination_execute_actions(
            dst_root, dst_exec_actions, opts, deferred_dir_removes, deferred_dir_meta_updates, local_errors, meta_stats,
            completed_actions, completed_copy_files, completed_copy_bytes);
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
    int* local_errors,
    nsync_meta_apply_stats_t* meta_stats)
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
        nsync_apply_metadata(dst_fullpath, action, 0, opts, local_errors, meta_stats);
        mfu_free(&dst_fullpath);
    }
}

static int nsync_restore_destination_root_metadata(
    const nsync_options_t* opts,
    const nsync_role_info_t* role_info,
    const char* src_root,
    const char* dst_root,
    uint64_t* local_exec_errors_total,
    nsync_meta_apply_stats_t* meta_stats)
{
    if (opts->dryrun || local_exec_errors_total == NULL) {
        return 0;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int src_owner = role_info->src_world_ranks[0];
    int dst_owner = role_info->dst_world_ranks[0];

    uint64_t root_meta[5] = {0, 0, 0, 0, 0};
    int restore_root_meta = 0;
    int root_meta_error = 0;

    if (rank == src_owner) {
        struct stat st;
        if (lstat(src_root, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                restore_root_meta = 1;
                root_meta[0] = (uint64_t)st.st_mode;
                root_meta[1] = (uint64_t)st.st_uid;
                root_meta[2] = (uint64_t)st.st_gid;
                root_meta[3] = (uint64_t)st.st_mtim.tv_sec;
                root_meta[4] = (uint64_t)st.st_mtim.tv_nsec;
            }
        } else {
            root_meta_error = 1;
        }
    }

    MPI_Bcast(&restore_root_meta, 1, MPI_INT, src_owner, MPI_COMM_WORLD);
    MPI_Bcast(&root_meta_error, 1, MPI_INT, src_owner, MPI_COMM_WORLD);

    if (!restore_root_meta) {
        if (!root_meta_error) {
            return 0;
        }

        if (rank == opts->log_rank && !opts->quiet) {
            MFU_LOG(MFU_LOG_WARN, "Failed to collect source root metadata for destination root restore");
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
        nsync_apply_metadata(dst_root, &action, 0, opts, &local_errors, meta_stats);
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

static uint64_t nsync_action_counts_total_actions(const nsync_action_counts_t* counts)
{
    return counts->copy + counts->remove + counts->mkdir + counts->symlink_update + counts->meta_update;
}

static uint64_t nsync_action_vec_copy_bytes(const nsync_action_vec_t* actions)
{
    uint64_t total = 0;
    for (uint64_t i = 0; i < actions->size; i++) {
        const nsync_action_record_t* action = &actions->records[i];
        if (action->type == NSYNC_ACTION_COPY) {
            total += action->size;
        }
    }
    return total;
}

static void nsync_progress_state_init(nsync_progress_state_t* state)
{
    memset(state, 0, sizeof(*state));
    double now = MPI_Wtime();
    state->start_time = now;
    state->last_print_time = now;
}

static void nsync_progress_log_console(
    const nsync_options_t* opts,
    uint64_t batch_id,
    uint64_t batch_count,
    uint64_t total_actions,
    uint64_t total_copy_files,
    uint64_t total_copy_bytes,
    uint64_t recent_actions,
    uint64_t recent_copy_files,
    uint64_t recent_copy_bytes,
    double elapsed_secs,
    double recent_secs)
{
    double percent = 100.0;
    if (batch_count > 0) {
        percent = 100.0 * (double)(batch_id + 1) / (double)batch_count;
    }

    double total_copy_val = 0.0;
    const char* total_copy_units = "B";
    mfu_format_bytes(total_copy_bytes, &total_copy_val, &total_copy_units);

    double recent_copy_val = 0.0;
    const char* recent_copy_units = "B";
    mfu_format_bytes(recent_copy_bytes, &recent_copy_val, &recent_copy_units);

    if (opts->dryrun) {
        MFU_LOG(MFU_LOG_INFO,
            "Progress %.1f%% batch %" PRIu64 "/%" PRIu64
            " planned-actions=%" PRIu64 " planned-copy-files=%" PRIu64 " planned-volume=%.3lf %s",
            percent, batch_id + 1, batch_count,
            total_actions, total_copy_files, total_copy_val, total_copy_units);
        return;
    }

    double recent_file_rate = 0.0;
    double recent_bw = 0.0;
    double avg_file_rate = 0.0;
    double avg_bw = 0.0;
    if (recent_secs > 0.0) {
        recent_file_rate = (double)recent_copy_files / recent_secs;
        recent_bw = (double)recent_copy_bytes / recent_secs;
    }
    if (elapsed_secs > 0.0) {
        avg_file_rate = (double)total_copy_files / elapsed_secs;
        avg_bw = (double)total_copy_bytes / elapsed_secs;
    }

    double recent_bw_val = 0.0;
    const char* recent_bw_units = "B/s";
    mfu_format_bw(recent_bw, &recent_bw_val, &recent_bw_units);

    double avg_bw_val = 0.0;
    const char* avg_bw_units = "B/s";
    mfu_format_bw(avg_bw, &avg_bw_val, &avg_bw_units);

    MFU_LOG(MFU_LOG_INFO,
        "Progress %.1f%% batch %" PRIu64 "/%" PRIu64
        " actions=%" PRIu64 " copied-files=%" PRIu64 " copied-volume=%.3lf %s"
        " recent(actions=%" PRIu64 " files=%" PRIu64 " volume=%.3lf %s, %.2lf files/s, %.3lf %s over %.3lf s)"
        " avg(%.2lf files/s, %.3lf %s)",
        percent, batch_id + 1, batch_count,
        total_actions, total_copy_files, total_copy_val, total_copy_units,
        recent_actions, recent_copy_files, recent_copy_val, recent_copy_units,
        recent_file_rate, recent_bw_val, recent_bw_units, recent_secs,
        avg_file_rate, avg_bw_val, avg_bw_units);
}

static void nsync_progress_batch_update(
    const nsync_options_t* opts,
    nsync_progress_state_t* progress_state,
    uint64_t batch_id,
    uint64_t batch_count,
    uint64_t global_actions,
    uint64_t global_copy_files,
    uint64_t global_copy_bytes)
{
    if (opts->quiet) {
        return;
    }

    progress_state->total_actions += global_actions;
    progress_state->total_copy_files += global_copy_files;
    progress_state->total_copy_bytes += global_copy_bytes;

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank != opts->log_rank) {
        return;
    }

    double now = MPI_Wtime();
    uint64_t recent_actions = progress_state->total_actions - progress_state->last_actions;
    uint64_t recent_copy_files = progress_state->total_copy_files - progress_state->last_copy_files;
    uint64_t recent_copy_bytes = progress_state->total_copy_bytes - progress_state->last_copy_bytes;

    double elapsed_secs = now - progress_state->start_time;
    double recent_secs = now - progress_state->last_print_time;
    if (recent_secs < 0.0) {
        recent_secs = 0.0;
    }

    nsync_progress_log_console(
        opts, batch_id, batch_count,
        progress_state->total_actions,
        progress_state->total_copy_files,
        progress_state->total_copy_bytes,
        recent_actions,
        recent_copy_files,
        recent_copy_bytes,
        elapsed_secs,
        recent_secs);

    progress_state->last_print_time = now;
    progress_state->last_actions = progress_state->total_actions;
    progress_state->last_copy_files = progress_state->total_copy_files;
    progress_state->last_copy_bytes = progress_state->total_copy_bytes;
}

static void nsync_meta_apply_stats_init(nsync_meta_apply_stats_t* stats)
{
    memset(stats, 0, sizeof(*stats));
}

static void nsync_meta_apply_stats_to_array(const nsync_meta_apply_stats_t* stats, uint64_t out[6])
{
    out[0] = stats->chown_ignored;
    out[1] = stats->chmod_ignored;
    out[2] = stats->utime_ignored;
    out[3] = stats->chown_failed;
    out[4] = stats->chmod_failed;
    out[5] = stats->utime_failed;
}

static void nsync_meta_apply_stats_from_array(const uint64_t in[6], nsync_meta_apply_stats_t* stats)
{
    stats->chown_ignored = in[0];
    stats->chmod_ignored = in[1];
    stats->utime_ignored = in[2];
    stats->chown_failed = in[3];
    stats->chmod_failed = in[4];
    stats->utime_failed = in[5];
}

static int nsync_owner_from_group(
    const int* world_ranks,
    int count,
    const char* relpath,
    nsync_hash_domain_t domain)
{
    if (count <= 0 || world_ranks == NULL) {
        return -1;
    }

    int idx = nsync_hash_bucket(relpath, count, domain);
    return world_ranks[idx];
}

typedef struct {
    int planner_rank;
    uint64_t local_index;
    uint64_t size;
    char* relpath;
    int src_owner_world;
    int dst_owner_world;
} nsync_large_copy_owner_record_t;

static size_t nsync_large_copy_owner_record_pack_size(const nsync_action_record_t* action, uint64_t local_index)
{
    (void)local_index;
    return 8 + 8 + 4 + strlen(action->relpath);
}

static size_t nsync_large_copy_owner_record_pack(
    char* buf,
    const nsync_action_record_t* action,
    uint64_t local_index)
{
    uint32_t path_len = (uint32_t)strlen(action->relpath);
    char* ptr = buf;
    mfu_pack_uint64(&ptr, local_index);
    mfu_pack_uint64(&ptr, action->size);
    mfu_pack_uint32(&ptr, path_len);
    memcpy(ptr, action->relpath, (size_t)path_len);
    ptr += path_len;
    return (size_t)(ptr - buf);
}

static int nsync_large_copy_owner_record_unpack(
    const char** pptr,
    const char* end,
    nsync_large_copy_owner_record_t* rec,
    int planner_rank)
{
    const char* ptr = *pptr;
    if ((size_t)(end - ptr) < 20) {
        return -1;
    }

    uint32_t path_len = 0;
    mfu_unpack_uint64(&ptr, &rec->local_index);
    mfu_unpack_uint64(&ptr, &rec->size);
    mfu_unpack_uint32(&ptr, &path_len);
    if ((size_t)(end - ptr) < (size_t)path_len) {
        return -1;
    }

    rec->planner_rank = planner_rank;
    rec->relpath = (char*)MFU_MALLOC((size_t)path_len + 1);
    memcpy(rec->relpath, ptr, (size_t)path_len);
    rec->relpath[path_len] = '\0';
    ptr += path_len;
    rec->src_owner_world = -1;
    rec->dst_owner_world = -1;
    *pptr = ptr;
    return 0;
}

static size_t nsync_large_copy_owner_update_pack_size(void)
{
    return 8 + 4 + 4;
}

static size_t nsync_large_copy_owner_update_pack(char* buf, uint64_t local_index, int src_owner_world, int dst_owner_world)
{
    char* ptr = buf;
    mfu_pack_uint64(&ptr, local_index);
    mfu_pack_uint32(&ptr, (uint32_t)(src_owner_world + 1));
    mfu_pack_uint32(&ptr, (uint32_t)(dst_owner_world + 1));
    return (size_t)(ptr - buf);
}

static int nsync_large_copy_owner_update_unpack(
    const char** pptr,
    const char* end,
    uint64_t* local_index,
    int* src_owner_world,
    int* dst_owner_world)
{
    const char* ptr = *pptr;
    if ((size_t)(end - ptr) < nsync_large_copy_owner_update_pack_size()) {
        return -1;
    }

    uint32_t src_u32 = 0;
    uint32_t dst_u32 = 0;
    mfu_unpack_uint64(&ptr, local_index);
    mfu_unpack_uint32(&ptr, &src_u32);
    mfu_unpack_uint32(&ptr, &dst_u32);
    *src_owner_world = (int)src_u32 - 1;
    *dst_owner_world = (int)dst_u32 - 1;
    *pptr = ptr;
    return 0;
}

static int nsync_large_copy_owner_record_sort(const void* a, const void* b)
{
    const nsync_large_copy_owner_record_t* aa = (const nsync_large_copy_owner_record_t*)a;
    const nsync_large_copy_owner_record_t* bb = (const nsync_large_copy_owner_record_t*)b;
    if (aa->size != bb->size) {
        return (aa->size > bb->size) ? -1 : 1;
    }
    return strcmp(aa->relpath, bb->relpath);
}

static int nsync_large_copy_pick_min_loaded(const int* world_ranks, int count, const uint64_t* loads)
{
    int best = world_ranks[0];
    uint64_t best_load = loads[best];
    for (int i = 1; i < count; i++) {
        int rank = world_ranks[i];
        uint64_t load = loads[rank];
        if (load < best_load || (load == best_load && rank < best)) {
            best = rank;
            best_load = load;
        }
    }
    return best;
}

static int nsync_rebalance_large_copy_owners(
    nsync_action_vec_t* planned_actions,
    const nsync_role_info_t* role_info)
{
    int rank = 0;
    int ranks = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    if (role_info->src_count <= 1 && role_info->dst_count <= 1)
    {
        return 0;
    }

    int local_error = 0;
    int global_error = 0;
    uint64_t threshold = nsync_copy_large_file_threshold();
    uint64_t* local_src_bytes = (uint64_t*)calloc((size_t)ranks, sizeof(uint64_t));
    uint64_t* local_dst_bytes = (uint64_t*)calloc((size_t)ranks, sizeof(uint64_t));
    uint64_t* global_src_bytes = (uint64_t*)calloc((size_t)ranks, sizeof(uint64_t));
    uint64_t* global_dst_bytes = (uint64_t*)calloc((size_t)ranks, sizeof(uint64_t));
    if (local_src_bytes == NULL || local_dst_bytes == NULL || global_src_bytes == NULL || global_dst_bytes == NULL) {
        local_error = 1;
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0) {
        free(local_src_bytes);
        free(local_dst_bytes);
        free(global_src_bytes);
        free(global_dst_bytes);
        return -1;
    }

    size_t local_large_bytes = 0;
    for (uint64_t i = 0; i < planned_actions->size; i++) {
        nsync_action_record_t* action = &planned_actions->records[i];
        if (action->type != NSYNC_ACTION_COPY) {
            continue;
        }

        int large_file = (action->size >= threshold) &&
            (role_info->src_count > 1 || role_info->dst_count > 1);
        if (large_file) {
            size_t record_bytes = nsync_large_copy_owner_record_pack_size(action, i);
            if (SIZE_MAX - local_large_bytes < record_bytes) {
                local_error = 1;
                break;
            }
            local_large_bytes += record_bytes;
        } else {
            if (action->src_owner_world >= 0) {
                local_src_bytes[action->src_owner_world] += action->size;
            }
            if (action->dst_owner_world >= 0) {
                local_dst_bytes[action->dst_owner_world] += action->size;
            }
        }
    }

    if (local_large_bytes > (size_t)INT_MAX) {
        local_error = 1;
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0) {
        free(local_src_bytes);
        free(local_dst_bytes);
        free(global_src_bytes);
        free(global_dst_bytes);
        return -1;
    }

    MPI_Allreduce(local_src_bytes, global_src_bytes, ranks, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    MPI_Allreduce(local_dst_bytes, global_dst_bytes, ranks, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    free(local_src_bytes);
    free(local_dst_bytes);

    int local_send_bytes = (int)local_large_bytes;
    char dummy = '\0';
    char* packed = NULL;
    if (local_send_bytes > 0) {
        packed = (char*)MFU_MALLOC((size_t)local_send_bytes);
        if (packed == NULL) {
            local_error = 1;
        } else {
            char* ptr = packed;
            for (uint64_t i = 0; i < planned_actions->size; i++) {
                nsync_action_record_t* action = &planned_actions->records[i];
                if (action->type == NSYNC_ACTION_COPY &&
                    action->size >= threshold &&
                    (role_info->src_count > 1 || role_info->dst_count > 1))
                {
                    ptr += nsync_large_copy_owner_record_pack(ptr, action, i);
                }
            }
        }
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0) {
        mfu_free(&packed);
        free(global_src_bytes);
        free(global_dst_bytes);
        return -1;
    }

    int* recv_counts = NULL;
    int* recv_displs = NULL;
    char* recv_buf = NULL;
    if (rank == 0) {
        recv_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
        recv_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
        if (recv_counts == NULL || recv_displs == NULL) {
            local_error = 1;
        }
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0) {
        mfu_free(&packed);
        mfu_free(&recv_counts);
        mfu_free(&recv_displs);
        free(global_src_bytes);
        free(global_dst_bytes);
        return -1;
    }

    MPI_Gather(&local_send_bytes, 1, MPI_INT, recv_counts, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int root_error = 0;
    if (rank == 0) {
        int total_recv = 0;
        for (int i = 0; i < ranks; i++) {
            recv_displs[i] = total_recv;
            total_recv += recv_counts[i];
        }
        recv_buf = (char*)MFU_MALLOC((size_t)(total_recv > 0 ? total_recv : 1));
        if (recv_buf == NULL) {
            root_error = 1;
        }
    }

    MPI_Bcast(&root_error, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (root_error != 0) {
        mfu_free(&packed);
        mfu_free(&recv_counts);
        mfu_free(&recv_displs);
        mfu_free(&recv_buf);
        free(global_src_bytes);
        free(global_dst_bytes);
        return -1;
    }

    MPI_Gatherv(
        (local_send_bytes > 0) ? packed : &dummy,
        local_send_bytes,
        MPI_BYTE,
        recv_buf,
        recv_counts,
        recv_displs,
        MPI_BYTE,
        0,
        MPI_COMM_WORLD);
    mfu_free(&packed);

    int* update_counts = NULL;
    int* update_displs = NULL;
    char* update_send_buf = NULL;
    int local_update_bytes = 0;
    if (rank == 0) {
        nsync_large_copy_owner_record_t* records = NULL;
        uint64_t record_count = 0;
        uint64_t record_capacity = 0;
        for (int planner_rank = 0; planner_rank < ranks && root_error == 0; planner_rank++) {
            const char* ptr = recv_buf + recv_displs[planner_rank];
            const char* end = ptr + recv_counts[planner_rank];
            while (ptr < end) {
                if (record_count == record_capacity) {
                    uint64_t new_capacity = (record_capacity > 0) ? (record_capacity * 2) : 128;
                    nsync_large_copy_owner_record_t* new_records =
                        (nsync_large_copy_owner_record_t*)realloc(records, (size_t)new_capacity * sizeof(*records));
                    if (new_records == NULL) {
                        root_error = 1;
                        break;
                    }
                    records = new_records;
                    record_capacity = new_capacity;
                }
                if (nsync_large_copy_owner_record_unpack(&ptr, end, &records[record_count], planner_rank) != 0) {
                    root_error = 1;
                    break;
                }
                record_count++;
            }
        }

        if (record_count > 1 && root_error == 0) {
            qsort(records, (size_t)record_count, sizeof(*records), nsync_large_copy_owner_record_sort);
        }

        if (root_error == 0) {
            update_counts = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
            update_displs = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
            if (update_counts == NULL || update_displs == NULL) {
                root_error = 1;
            }
        }

        if (root_error == 0) {
            for (int i = 0; i < ranks; i++) {
                update_counts[i] = 0;
                update_displs[i] = 0;
            }

            for (uint64_t i = 0; i < record_count; i++) {
                records[i].src_owner_world = (role_info->src_count > 1) ?
                    nsync_large_copy_pick_min_loaded(role_info->src_world_ranks, role_info->src_count, global_src_bytes) :
                    role_info->src_world_ranks[0];
                records[i].dst_owner_world = (role_info->dst_count > 1) ?
                    nsync_large_copy_pick_min_loaded(role_info->dst_world_ranks, role_info->dst_count, global_dst_bytes) :
                    role_info->dst_world_ranks[0];

                global_src_bytes[records[i].src_owner_world] += records[i].size;
                global_dst_bytes[records[i].dst_owner_world] += records[i].size;
                update_counts[records[i].planner_rank] += (int)nsync_large_copy_owner_update_pack_size();
            }

            int total_update_bytes = 0;
            for (int i = 0; i < ranks; i++) {
                update_displs[i] = total_update_bytes;
                total_update_bytes += update_counts[i];
            }

            update_send_buf = (char*)MFU_MALLOC((size_t)(total_update_bytes > 0 ? total_update_bytes : 1));
            int* update_offsets = (int*)MFU_MALLOC((size_t)ranks * sizeof(int));
            if (update_send_buf == NULL || update_offsets == NULL) {
                root_error = 1;
                mfu_free(&update_offsets);
            } else {
                memcpy(update_offsets, update_displs, (size_t)ranks * sizeof(int));
                for (uint64_t i = 0; i < record_count; i++) {
                    int planner_rank = records[i].planner_rank;
                    int offset = update_offsets[planner_rank];
                    offset += (int)nsync_large_copy_owner_update_pack(
                        update_send_buf + offset,
                        records[i].local_index,
                        records[i].src_owner_world,
                        records[i].dst_owner_world);
                    update_offsets[planner_rank] = offset;
                }
                mfu_free(&update_offsets);
                local_update_bytes = update_counts[0];
            }
        }

        for (uint64_t i = 0; i < record_count; i++) {
            mfu_free(&records[i].relpath);
        }
        free(records);
    }

    MPI_Bcast(&root_error, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (root_error != 0) {
        mfu_free(&recv_counts);
        mfu_free(&recv_displs);
        mfu_free(&recv_buf);
        mfu_free(&update_counts);
        mfu_free(&update_displs);
        mfu_free(&update_send_buf);
        free(global_src_bytes);
        free(global_dst_bytes);
        return -1;
    }

    MPI_Scatter(update_counts, 1, MPI_INT, &local_update_bytes, 1, MPI_INT, 0, MPI_COMM_WORLD);
    char* local_update_buf = (char*)MFU_MALLOC((size_t)(local_update_bytes > 0 ? local_update_bytes : 1));
    local_error = (local_update_buf == NULL) ? 1 : 0;
    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    if (global_error != 0) {
        mfu_free(&local_update_buf);
        mfu_free(&recv_counts);
        mfu_free(&recv_displs);
        mfu_free(&recv_buf);
        mfu_free(&update_counts);
        mfu_free(&update_displs);
        mfu_free(&update_send_buf);
        free(global_src_bytes);
        free(global_dst_bytes);
        return -1;
    }

    MPI_Scatterv(
        update_send_buf,
        update_counts,
        update_displs,
        MPI_BYTE,
        local_update_buf,
        local_update_bytes,
        MPI_BYTE,
        0,
        MPI_COMM_WORLD);

    const char* update_ptr = local_update_buf;
    const char* update_end = local_update_buf + local_update_bytes;
    while (update_ptr < update_end) {
        uint64_t local_index = 0;
        int src_owner_world = -1;
        int dst_owner_world = -1;
        if (nsync_large_copy_owner_update_unpack(
                &update_ptr, update_end, &local_index, &src_owner_world, &dst_owner_world) != 0 ||
            local_index >= planned_actions->size)
        {
            local_error = 1;
            break;
        }

        planned_actions->records[local_index].src_owner_world = src_owner_world;
        planned_actions->records[local_index].dst_owner_world = dst_owner_world;
    }

    MPI_Allreduce(&local_error, &global_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    mfu_free(&local_update_buf);
    mfu_free(&recv_counts);
    mfu_free(&recv_displs);
    mfu_free(&recv_buf);
    mfu_free(&update_counts);
    mfu_free(&update_displs);
    mfu_free(&update_send_buf);
    free(global_src_bytes);
    free(global_dst_bytes);
    return (global_error == 0) ? 0 : -1;
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

        int src_owner_world = nsync_owner_from_group(
            role_info->src_world_ranks, role_info->src_count, relpath, NSYNC_HASH_DOMAIN_SRC);
        int dst_owner_world = nsync_owner_from_group(
            role_info->dst_world_ranks, role_info->dst_count, relpath, NSYNC_HASH_DOMAIN_DST);

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

int main(int argc, char** argv)
{
    int rc = 0;

    MPI_Init(&argc, &argv);
    mfu_init();
    mfu_debug_level = MFU_LOG_VERBOSE;

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    nsync_options_t opts = {
        .dryrun = 0,
        .batch_files = 0,
        .delete = 0,
        .contents = 0,
        .ignore_symlinks = 0,
        .bufsize = MFU_BUFFER_SIZE,
        .direct = 0,
        .open_noatime = 0,
        .role_mode = NSYNC_ROLE_MODE_AUTO,
        .role_map = NULL,
        .trace = 0,
        .quiet = 0,
        .log_rank = 0,
        .imbalance_threshold = 3.0,
    };

    opts.log_rank = nsync_select_log_rank();

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
        {"ignore-symlinks", 0, 0, 1004},
        {"direct", 0, 0, 's'},
        {"open-noatime", 0, 0, 'U'},
        {"bufsize", 1, 0, 'B'},
        {"imbalance-threshold", 1, 0, 1003},
        {"role-mode", 1, 0, 1000},
        {"role-map", 1, 0, 1001},
        {"trace", 0, 0, 1002},
        {"quiet", 0, 0, 'q'},
        {"help", 0, 0, 'h'},
        {0, 0, 0, 0},
    };

    while (1) {
        int c = getopt_long(argc, argv, "b:DcnsUB:qh", long_options, &option_index);
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
        case 1004:
            opts.ignore_symlinks = 1;
            break;
        case 'n':
            opts.dryrun = 1;
            break;
        case 's':
            opts.direct = 1;
            if (rank == opts.log_rank) {
                MFU_LOG(MFU_LOG_INFO, "Using O_DIRECT");
            }
            break;
        case 'U':
            opts.open_noatime = 1;
            if (rank == opts.log_rank) {
                MFU_LOG(MFU_LOG_INFO, "Using O_NOATIME");
            }
            break;
        case 'b':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == opts.log_rank) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to parse --batch-files: %s", optarg);
                }
                usage = 1;
            } else {
                opts.batch_files = (uint64_t)bytes;
            }
            break;
        case 'B':
            if (mfu_abtoull(optarg, &bytes) != MFU_SUCCESS || bytes == 0) {
                if (rank == opts.log_rank) {
                    MFU_LOG(MFU_LOG_ERR, "Failed to parse --bufsize: %s", optarg);
                }
                usage = 1;
            } else {
                opts.bufsize = (uint64_t)bytes;
            }
            break;
        case 1000:
            if (nsync_parse_role_mode(optarg, &opts.role_mode) != 0) {
                if (rank == opts.log_rank) {
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
        case 1003: {
            errno = 0;
            char* end = NULL;
            double threshold = strtod(optarg, &end);
            if (errno != 0 || end == optarg || *end != '\0' || !isfinite(threshold) || threshold < 1.0) {
                if (rank == opts.log_rank) {
                    MFU_LOG(
                        MFU_LOG_ERR,
                        "Invalid --imbalance-threshold: %s (must be a finite number >= 1.0)",
                        optarg);
                }
                usage = 1;
            } else {
                opts.imbalance_threshold = threshold;
            }
            break;
        }
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
        if (rank == opts.log_rank) {
            MFU_LOG(MFU_LOG_ERR, "--role-map is required when --role-mode map is selected");
        }
        usage = 1;
    }

    int numargs = argc - optind;
    if (!help && numargs != 2) {
        if (rank == opts.log_rank) {
            MFU_LOG(MFU_LOG_ERR, "You must specify a source and destination path.");
        }
        usage = 1;
    }

    if (usage) {
        if (rank == opts.log_rank) {
            nsync_usage();
        }
        rc = help ? 0 : 1;
        goto cleanup;
    }

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
    int use_batch_spool = 0;
    int local_scan_errors_total = 0;
    if (opts.batch_files > 0) {
        int batch_scan_errors = 0;
        if (nsync_batch_spool_scan_prepare(
                &opts, &role_info, src_path, dst_path,
                &batch_spool, &batch_count,
                &global_src_items, &global_dst_items, &batch_scan_errors) != 0)
        {
            if (rank == opts.log_rank) {
                MFU_LOG(MFU_LOG_ERR, "Failed to prepare batch metadata spool");
            }
            nsync_role_info_free(&role_info);
            rc = 1;
            goto cleanup;
        }

        local_scan_errors_total += batch_scan_errors;

        if (rank == opts.log_rank) {
            MFU_LOG(MFU_LOG_INFO,
                "Batch mode enabled: batch-files=%" PRIu64
                " batch-count=%" PRIu64
                " src-items=%" PRIu64
                " dst-items=%" PRIu64,
                opts.batch_files, batch_count, global_src_items, global_dst_items);
            if (batch_count > 4096) {
                MFU_LOG(MFU_LOG_WARN,
                    "High batch-count=%" PRIu64
                    " may increase local spool and planner overhead; increase --batch-files to reduce batch count.",
                    batch_count);
            }
        }

        uint64_t local_spool_arr[4] = {
            batch_spool.raw_records_written,
            batch_spool.raw_bytes_written,
            batch_spool.records_written,
            batch_spool.bytes_written
        };
        uint64_t global_spool_arr[4] = {0, 0, 0, 0};
        MPI_Allreduce(local_spool_arr, global_spool_arr, 4, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

        if (rank == opts.log_rank && !opts.quiet) {
            MFU_LOG(MFU_LOG_INFO,
                "Batch spool prepared: scan-records=%" PRIu64
                " scan-bytes=%" PRIu64
                " batch-records=%" PRIu64
                " batch-sidecar-bytes=%" PRIu64,
                global_spool_arr[0], global_spool_arr[1],
                global_spool_arr[2], global_spool_arr[3]);
        }

        use_batch_spool = 1;
    }

    nsync_compare_counts_t local_counts_total;
    nsync_compare_counts_t global_counts;
    nsync_action_counts_t local_action_counts_total;
    nsync_action_counts_t global_action_counts;
    nsync_meta_apply_stats_t local_meta_apply_stats;
    nsync_meta_apply_stats_t global_meta_apply_stats;
    nsync_action_vec_t deferred_dir_removes;
    nsync_action_vec_t deferred_dir_meta_updates;
    nsync_compare_counts_init(&local_counts_total);
    nsync_action_counts_init(&local_action_counts_total);
    nsync_meta_apply_stats_init(&local_meta_apply_stats);
    nsync_meta_apply_stats_init(&global_meta_apply_stats);
    nsync_action_vec_init(&deferred_dir_removes);
    nsync_action_vec_init(&deferred_dir_meta_updates);

    uint64_t local_exec_errors_total = 0;
    nsync_progress_state_t progress_state;
    nsync_progress_state_init(&progress_state);
    if (rank == opts.log_rank && !opts.quiet) {
        MFU_LOG(
            MFU_LOG_INFO,
            "Progress logging enabled on console log rank %d (per batch)",
            opts.log_rank);
    }

    for (uint64_t batch_id = 0; batch_id < batch_count; batch_id++) {
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
        int stop_after_batch_error = 0;
        uint64_t local_batch_actions = 0;
        uint64_t local_batch_copy_files = 0;
        uint64_t local_batch_copy_bytes = 0;
        uint64_t local_batch_exec_actions = 0;
        uint64_t local_batch_exec_copy_files = 0;
        uint64_t local_batch_exec_copy_bytes = 0;
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
                if (rank == opts.log_rank) {
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
            uint64_t local_batch_summary[4] = {
                local_batch_errors, local_batch_actions, local_batch_copy_files, local_batch_copy_bytes
            };
            uint64_t global_batch_summary[4] = {0, 0, 0, 0};
            MPI_Allreduce(local_batch_summary, global_batch_summary, 4, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
            uint64_t global_batch_errors = global_batch_summary[0];
            if (!opts.dryrun && global_batch_errors > 0) {
                stop_after_batch_error = 1;
            }
            nsync_progress_batch_update(
                &opts, &progress_state, batch_id, batch_count,
                global_batch_summary[1], global_batch_summary[2], global_batch_summary[3]);
            nsync_action_vec_free(&dst_exec_actions);
            nsync_action_vec_free(&src_exec_actions);
            nsync_action_vec_free(&planned_actions);
            nsync_meta_vec_free(&planner_meta);
            nsync_meta_vec_free(&local_meta);
            if (stop_after_batch_error) {
                if (rank == opts.log_rank) {
                    MFU_LOG(MFU_LOG_WARN,
                        "Stopping after batch %" PRIu64 "/%" PRIu64 " due to errors",
                        batch_id + 1, batch_count);
                }
                break;
            }
            continue;
        }

        if (nsync_metadata_redistribute(&local_meta, &planner_meta, &opts) != 0) {
            if (rank == opts.log_rank) {
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
        nsync_batch_monitor_skew(&opts, batch_id, batch_count, "planner-meta", planner_meta.size);

        nsync_compare_counts_t batch_counts;
        nsync_action_counts_t batch_action_counts;
        int local_plan_error = 0;
        if (nsync_plan_planner_records(
                &planner_meta, &opts, &role_info,
                &batch_counts, &planned_actions, &batch_action_counts) != 0)
        {
            local_plan_error = 1;
        }
        int global_plan_error = 0;
        MPI_Allreduce(&local_plan_error, &global_plan_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
        if (global_plan_error != 0) {
            if (rank == opts.log_rank) {
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

        if (nsync_rebalance_large_copy_owners(&planned_actions, &role_info) != 0) {
            local_plan_error = 1;
        }
        MPI_Allreduce(&local_plan_error, &global_plan_error, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
        if (global_plan_error != 0) {
            if (rank == opts.log_rank) {
                MFU_LOG(MFU_LOG_ERR, "Failed to rebalance large copy ownership in batch %" PRIu64, batch_id);
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

        nsync_trace_local(&opts, "main-post-plan-local", planned_actions.size, 0);
        nsync_batch_monitor_skew(&opts, batch_id, batch_count, "planned-actions", planned_actions.size);
        local_batch_actions = nsync_action_counts_total_actions(&batch_action_counts);
        local_batch_copy_files = batch_action_counts.copy;
        local_batch_copy_bytes = nsync_action_vec_copy_bytes(&planned_actions);

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
                if (rank == opts.log_rank) {
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
                &local_exec_errors_batch,
                &local_meta_apply_stats,
                &local_batch_exec_actions,
                &local_batch_exec_copy_files,
                &local_batch_exec_copy_bytes);
            nsync_trace_local(&opts, "main-post-phase4-exec", (uint64_t)local_exec_errors_batch, batch_id);

            local_exec_errors_total += (uint64_t)local_exec_errors_batch;
        }

        uint64_t local_progress_actions = local_batch_actions;
        uint64_t local_progress_copy_files = local_batch_copy_files;
        uint64_t local_progress_copy_bytes = local_batch_copy_bytes;
        if (!opts.dryrun) {
            local_progress_actions = local_batch_exec_actions;
            local_progress_copy_files = local_batch_exec_copy_files;
            local_progress_copy_bytes = local_batch_exec_copy_bytes;
        }

        uint64_t local_batch_errors = (uint64_t)batch_scan_errors + (uint64_t)local_exec_errors_batch;
        uint64_t local_batch_summary[4] = {
            local_batch_errors, local_progress_actions, local_progress_copy_files, local_progress_copy_bytes
        };
        uint64_t global_batch_summary[4] = {0, 0, 0, 0};
        MPI_Allreduce(local_batch_summary, global_batch_summary, 4, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
        uint64_t global_batch_errors = global_batch_summary[0];
        if (!opts.dryrun && global_batch_errors > 0) {
            stop_after_batch_error = 1;
        }

        nsync_progress_batch_update(
            &opts, &progress_state, batch_id, batch_count,
            global_batch_summary[1], global_batch_summary[2], global_batch_summary[3]);

        nsync_action_vec_free(&dst_exec_actions);
        nsync_action_vec_free(&src_exec_actions);
        nsync_action_vec_free(&planned_actions);
        nsync_meta_vec_free(&planner_meta);
        nsync_meta_vec_free(&local_meta);
        if (stop_after_batch_error) {
            if (rank == opts.log_rank) {
                MFU_LOG(MFU_LOG_WARN,
                    "Stopping after batch %" PRIu64 "/%" PRIu64 " due to errors",
                    batch_id + 1, batch_count);
            }
            break;
        }
    }

    if (use_batch_spool) {
        nsync_batch_spool_cleanup(&batch_spool);
        use_batch_spool = 0;
    }

    if (!opts.dryrun && opts.batch_files > 0 && opts.delete && role_info.role == NSYNC_ROLE_DST) {
        int local_finalize_errors = 0;
        nsync_finalize_deferred_dir_removes(dst_path, &deferred_dir_removes, &local_finalize_errors);
        local_exec_errors_total += (uint64_t)local_finalize_errors;
    }

    if (!opts.dryrun && role_info.role == NSYNC_ROLE_DST) {
        int local_finalize_errors = 0;
        nsync_finalize_deferred_dir_meta_updates(
            dst_path, &opts, &deferred_dir_meta_updates, &local_finalize_errors, &local_meta_apply_stats);
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

    uint64_t global_exec_errors_before_finalize = 0;
    MPI_Allreduce(
        &local_exec_errors_total, &global_exec_errors_before_finalize, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    if (!opts.dryrun && global_scan_errors == 0 && global_exec_errors_before_finalize == 0) {
        (void)nsync_restore_destination_root_metadata(
            &opts, &role_info, src_path, dst_path, &local_exec_errors_total, &local_meta_apply_stats);
    }

    uint64_t global_exec_errors = 0;
    MPI_Allreduce(
        &local_exec_errors_total, &global_exec_errors, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    uint64_t local_meta_apply_arr[6];
    uint64_t global_meta_apply_arr[6];
    nsync_meta_apply_stats_to_array(&local_meta_apply_stats, local_meta_apply_arr);
    MPI_Allreduce(local_meta_apply_arr, global_meta_apply_arr, 6, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    nsync_meta_apply_stats_from_array(global_meta_apply_arr, &global_meta_apply_stats);

    if (rank == opts.log_rank) {
        const char* role_mode = opts.role_mode == NSYNC_ROLE_MODE_AUTO ? "auto" : "map";
        MFU_LOG(MFU_LOG_INFO, "nsync Phase 5 planner%s complete", opts.dryrun ? " dryrun" : "+execute");
        MFU_LOG(MFU_LOG_INFO, "Requested source: %s", src_path);
        MFU_LOG(MFU_LOG_INFO, "Requested target: %s", dst_path);
        MFU_LOG(MFU_LOG_INFO,
            "Options: dryrun=%d batch-files=%" PRIu64
            " delete=%d contents=%d role-mode=%s imbalance-threshold=%.2f",
            opts.dryrun, opts.batch_files, opts.delete, opts.contents, role_mode, opts.imbalance_threshold);
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

        uint64_t metadata_ignored =
            global_meta_apply_stats.chown_ignored +
            global_meta_apply_stats.chmod_ignored +
            global_meta_apply_stats.utime_ignored;
        uint64_t metadata_failed =
            global_meta_apply_stats.chown_failed +
            global_meta_apply_stats.chmod_failed +
            global_meta_apply_stats.utime_failed;
        if (metadata_ignored > 0) {
            MFU_LOG(MFU_LOG_WARN,
                "Metadata best-effort applied with %" PRIu64 " ignored operation(s): "
                "chown=%" PRIu64 " chmod=%" PRIu64 " utime=%" PRIu64,
                metadata_ignored,
                global_meta_apply_stats.chown_ignored,
                global_meta_apply_stats.chmod_ignored,
                global_meta_apply_stats.utime_ignored);
        }
        if (metadata_failed > 0) {
            MFU_LOG(MFU_LOG_ERR,
                "Metadata apply had %" PRIu64 " hard failure(s): "
                "chown=%" PRIu64 " chmod=%" PRIu64 " utime=%" PRIu64,
                metadata_failed,
                global_meta_apply_stats.chown_failed,
                global_meta_apply_stats.chmod_failed,
                global_meta_apply_stats.utime_failed);
        }

        if (global_scan_errors > 0) {
            MFU_LOG(MFU_LOG_ERR, "Encountered %d scan error(s)", global_scan_errors);
        }

        if (opts.dryrun) {
            MFU_LOG(MFU_LOG_INFO, "Dryrun enabled: planner generated actions but no filesystem changes were made.");
        }

        if (!opts.dryrun) {
            if (global_scan_errors > 0 || global_exec_errors > 0) {
                MFU_LOG(MFU_LOG_ERR,
                    "Execution completed with errors: scan=%d exec=%" PRIu64,
                    global_scan_errors, global_exec_errors);
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
