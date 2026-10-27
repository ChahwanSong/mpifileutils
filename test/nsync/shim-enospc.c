#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

/* fault injection: mkdirat(dirfd, NAME) fails with ENOSPC when NAME == $SHIM_ENOSPC_NAME */
int mkdirat(int dirfd, const char* path, mode_t mode)
{
    static int (*real)(int, const char*, mode_t) = NULL;
    if (real == NULL) {
        real = (int (*)(int, const char*, mode_t))dlsym(RTLD_NEXT, "mkdirat");
    }
    const char* name = getenv("SHIM_ENOSPC_NAME");
    if (name != NULL && strcmp(path, name) == 0) {
        errno = ENOSPC;
        return -1;
    }
    return real(dirfd, path, mode);
}
