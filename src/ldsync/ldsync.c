#define _GNU_SOURCE

#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static void print_usage(void)
{
    printf("Usage: ldsync [dsync-options] [ldsync-options] SRC DEST\n");
    printf("\n");
    printf("ldsync resolves the source path symbolic link chain up to N hops before\n");
    printf("executing dsync with the resulting source path. Default N is 1.\n");
    printf("\n");
    printf("ldsync options:\n");
    printf("  -N, --source-link-depth N  Follow source symlink at most N times (default: 1)\n");
    printf("  -h, --help                 Print this usage and exit\n");
    printf("\n");
    printf("Notes:\n");
    printf("  - ldsync parses its own options only before '--'.\n");
    printf("  - Use '--' to force remaining args to dsync unchanged.\n");
    printf("\n");
    printf("Examples:\n");
    printf("  ldsync /src-link /dest\n");
    printf("  ldsync -N 3 /src-link /dest\n");
    printf("  ldsync --source-link-depth=0 /src-link /dest\n");
    printf("  ldsync -- --help\n");
}

static int parse_nonnegative_int(const char* value, int* out)
{
    if (value == NULL || value[0] == '\0') {
        return -1;
    }

    errno = 0;
    char* endptr = NULL;
    long parsed = strtol(value, &endptr, 10);
    if (errno != 0 || endptr == value || *endptr != '\0' || parsed < 0 || parsed > INT_MAX) {
        return -1;
    }

    *out = (int) parsed;
    return 0;
}

static char* xstrdup(const char* s)
{
    char* copy = strdup(s);
    if (copy == NULL) {
        fprintf(stderr, "ldsync: strdup failed (errno=%d %s)\n", errno, strerror(errno));
    }
    return copy;
}

static char* join_path(const char* base, const char* rel)
{
    if (rel[0] == '/') {
        return xstrdup(rel);
    }

    char* base_copy = xstrdup(base);
    if (base_copy == NULL) {
        return NULL;
    }

    char* parent = dirname(base_copy);
    if (parent == NULL) {
        free(base_copy);
        return NULL;
    }

    size_t parent_len = strlen(parent);
    size_t rel_len = strlen(rel);
    size_t need_sep = (parent_len > 0 && parent[parent_len - 1] != '/') ? 1 : 0;

    char* joined = (char*) malloc(parent_len + need_sep + rel_len + 1);
    if (joined == NULL) {
        fprintf(stderr, "ldsync: malloc failed (errno=%d %s)\n", errno, strerror(errno));
        free(base_copy);
        return NULL;
    }

    memcpy(joined, parent, parent_len);
    size_t off = parent_len;
    if (need_sep) {
        joined[off++] = '/';
    }
    memcpy(joined + off, rel, rel_len);
    joined[off + rel_len] = '\0';

    free(base_copy);
    return joined;
}

static int follow_one_symlink(const char* path, char** out_next, bool* out_followed)
{
    struct stat st;
    if (lstat(path, &st) != 0) {
        if (errno == ENOENT || errno == ENOTDIR) {
            *out_followed = false;
            *out_next = xstrdup(path);
            return (*out_next != NULL) ? 0 : -1;
        }

        fprintf(stderr, "ldsync: lstat failed on `%s' (errno=%d %s)\n", path, errno, strerror(errno));
        return -1;
    }

    if (!S_ISLNK(st.st_mode)) {
        *out_followed = false;
        *out_next = xstrdup(path);
        return (*out_next != NULL) ? 0 : -1;
    }

    size_t bufsize = 256;
    char* target = NULL;
    while (1) {
        char* tmp = (char*) realloc(target, bufsize);
        if (tmp == NULL) {
            fprintf(stderr, "ldsync: realloc failed (errno=%d %s)\n", errno, strerror(errno));
            free(target);
            return -1;
        }
        target = tmp;

        ssize_t nread = readlink(path, target, bufsize - 1);
        if (nread < 0) {
            fprintf(stderr, "ldsync: readlink failed on `%s' (errno=%d %s)\n", path, errno, strerror(errno));
            free(target);
            return -1;
        }

        if ((size_t)nread < bufsize - 1) {
            target[nread] = '\0';
            break;
        }

        if (bufsize > SIZE_MAX / 2) {
            fprintf(stderr, "ldsync: symlink target too large for `%s'\n", path);
            free(target);
            return -1;
        }

        bufsize *= 2;
    }

    char* next = join_path(path, target);
    free(target);
    if (next == NULL) {
        return -1;
    }

    *out_followed = true;
    *out_next = next;
    return 0;
}

static char* resolve_source_path(const char* src, int max_depth)
{
    char* current = xstrdup(src);
    if (current == NULL) {
        return NULL;
    }

    for (int i = 0; i < max_depth; i++) {
        char* next = NULL;
        bool followed = false;
        int rc = follow_one_symlink(current, &next, &followed);
        if (rc != 0) {
            free(current);
            return NULL;
        }

        free(current);
        current = next;

        if (!followed) {
            break;
        }
    }

    return current;
}

static int parse_wrapper_options(
    int argc,
    char** argv,
    int* max_depth,
    bool* skip_arg,
    int* src_index,
    int* dst_index)
{
    bool parse_wrapper = true;

    for (int i = 1; i < argc; i++) {
        const char* arg = argv[i];

        if (parse_wrapper) {
            if (strcmp(arg, "--") == 0) {
                parse_wrapper = false;
                continue;
            }

            if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
                print_usage();
                return 1;
            }

            if (strcmp(arg, "-N") == 0 || strcmp(arg, "--source-link-depth") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "ldsync: missing value for %s\n", arg);
                    return -1;
                }
                skip_arg[i] = true;
                skip_arg[i + 1] = true;

                if (parse_nonnegative_int(argv[++i], max_depth) != 0) {
                    fprintf(stderr, "ldsync: invalid non-negative integer for %s: %s\n", arg, argv[i]);
                    return -1;
                }
                continue;
            }

            if (strncmp(arg, "--source-link-depth=", 20) == 0) {
                skip_arg[i] = true;
                if (parse_nonnegative_int(arg + 20, max_depth) != 0) {
                    fprintf(stderr, "ldsync: invalid non-negative integer for --source-link-depth: %s\n", arg + 20);
                    return -1;
                }
                continue;
            }

            if (strncmp(arg, "-N", 2) == 0 && arg[2] != '\0') {
                skip_arg[i] = true;
                if (parse_nonnegative_int(arg + 2, max_depth) != 0) {
                    fprintf(stderr, "ldsync: invalid non-negative integer for -N: %s\n", arg + 2);
                    return -1;
                }
                continue;
            }
        }
    }

    *src_index = -1;
    *dst_index = -1;
    for (int i = argc - 1; i >= 1; i--) {
        if (skip_arg[i]) {
            continue;
        }
        if (strcmp(argv[i], "--") == 0) {
            continue;
        }

        if (*dst_index < 0) {
            *dst_index = i;
            continue;
        }

        *src_index = i;
        break;
    }

    if (*src_index < 0 || *dst_index < 0) {
        for (int i = 1; i < argc; i++) {
            if (skip_arg[i]) {
                continue;
            }
            if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
                *src_index = -1;
                *dst_index = -1;
                return 0;
            }
        }
        fprintf(stderr, "ldsync: SRC and DEST are required\n");
        return -1;
    }

    return 0;
}

int main(int argc, char** argv)
{
    if (argc < 2) {
        print_usage();
        return 1;
    }

    int max_depth = 1;
    int src_index = -1;
    int dst_index = -1;
    bool* skip_arg = (bool*) calloc((size_t) argc, sizeof(bool));
    if (skip_arg == NULL) {
        fprintf(stderr, "ldsync: calloc failed (errno=%d %s)\n", errno, strerror(errno));
        return 1;
    }

    int parse_rc = parse_wrapper_options(argc, argv, &max_depth, skip_arg, &src_index, &dst_index);
    if (parse_rc > 0) {
        free(skip_arg);
        return 0;
    }
    if (parse_rc < 0) {
        print_usage();
        free(skip_arg);
        return 1;
    }

    char* resolved_src = NULL;
    if (src_index >= 0) {
        resolved_src = resolve_source_path(argv[src_index], max_depth);
        if (resolved_src == NULL) {
            free(skip_arg);
            return 1;
        }
    }

    char** pass_argv = (char**) calloc((size_t)argc + 1, sizeof(char*));
    if (pass_argv == NULL) {
        fprintf(stderr, "ldsync: calloc failed (errno=%d %s)\n", errno, strerror(errno));
        free(skip_arg);
        free(resolved_src);
        return 1;
    }

    int out = 0;
    pass_argv[out++] = (char*) "dsync";

    for (int i = 1; i < argc; i++) {
        if (skip_arg[i]) {
            continue;
        }

        if (i == src_index && resolved_src != NULL) {
            pass_argv[out++] = resolved_src;
        } else {
            pass_argv[out++] = argv[i];
        }
    }

    pass_argv[out] = NULL;

    execvp("dsync", pass_argv);

    fprintf(stderr, "ldsync: failed to execute dsync (errno=%d %s)\n", errno, strerror(errno));
    free(skip_arg);
    free(pass_argv);
    free(resolved_src);
    return 1;
}
