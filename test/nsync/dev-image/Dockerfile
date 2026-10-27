# nsync/dsync 수정용 개발 컨테이너 — DMS deploy/docker/Dockerfile.mpifileutils stage 1 과 같은 툴체인·의존 버전.
FROM debian:bookworm
ARG LWGRP_REF=v1.0.6
ARG DTCMP_REF=v1.1.5
ARG LIBCIRCLE_REF=v0.3
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates cmake g++ gcc git libbz2-dev libopenmpi-dev libssl-dev make openmpi-bin uuid-dev zlib1g-dev \
    rsync python3 attr strace \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git clone --depth 1 --branch "${LWGRP_REF}" https://github.com/llnl/lwgrp.git \
    && git clone --depth 1 --branch "${DTCMP_REF}" https://github.com/llnl/dtcmp.git \
    && git clone --depth 1 --branch "${LIBCIRCLE_REF}" https://github.com/hpc/libcircle.git \
    && sed -i 's@#include "../config/config.h"@@g' /src/lwgrp/src/lwgrp_internal.h \
    && sed -i 's@#include <config.h>@@g' /src/libcircle/libcircle/lib.h \
    && mkdir /release && cp -a /src/lwgrp /src/dtcmp /src/libcircle /release/ \
    && useradd -m -u 4242 tuser && useradd -m -u 4343 vuser
ENV OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 \
    OMPI_MCA_rmaps_base_oversubscribe=1 OMPI_MCA_btl_vader_single_copy_mechanism=none \
    PATH=/opt/mpifileutils/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    LD_LIBRARY_PATH=/opt/mpifileutils/lib
COPY mfu-build.sh /usr/local/bin/mfu-build
RUN chmod 755 /usr/local/bin/mfu-build
WORKDIR /work
CMD ["sleep", "infinity"]
