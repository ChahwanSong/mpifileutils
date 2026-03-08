# Install 

```shell

set -eux;

apt-get update

# MPI
export OPENMPI_VERSION_MAJOR=4.1 
export OPENMPI_VERSION_MINOR=8
export OPENMPI_INSTALL_PREFIX=/usr/local
export DEBIAN_FRONTEND=noninteractive
export LANG=C.UTF-8 LC_ALL=C.UTF-8

set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    gcc \
    g++ \
    libhwloc-dev \
    libnuma-dev \
    curl \
    wget \
    rsync \
    git \
    tzdata \
    locales \
    openssl \
    openssh-server \
    openssh-client \
    rdma-core \
    unzip \
    xz-utils \
    tar \
    bzip2 \
    gnupg \
    netbase \
    iproute2 \
    iputils-ping \
    net-tools \
    libibverbs-dev \
    ibverbs-utils \
    infiniband-diags \
    perftest \
    libtool \
    ethtool \
    autoconf \
    automake \
    dnsutils \
    python3 \
    libattr1-dev \
    libbz2-dev \
    libcap-dev \
    libssl-dev \
    libarchive-dev \
    python3-venv \
    python3-pip \
    python-is-python3 \
    pkg-config \
    cmake \
    cmake-curses-gui \
    ninja-build \
    gdb \
    strace \
    ltrace \
    libc6 \
    libstdc++6 \
    libssl3 \
    vim \
    htop \
    jq \
    procps

# Preliminaries
set -eux; \
    apt-get install -y \
    libevent-dev \
    libhwloc-dev \
    libucx0 \
    libucx-dev \
    ucx-utils;

# MPI & UCX
set -eux; \
    UCX_PREFIX=/usr; \
    UCX_MULTIARCH="$(dpkg-architecture -qDEB_HOST_MULTIARCH)"; \
    echo "Detected multiarch: ${UCX_MULTIARCH}"; \
    echo "UCX include: ${UCX_PREFIX}/include/ucx"; \
    echo "UCX libdir: /usr/lib/${UCX_MULTIARCH}"; \
    cd /tmp && \
    wget https://download.open-mpi.org/release/open-mpi/v${OPENMPI_VERSION_MAJOR}/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}.tar.gz && \
    tar -xzf openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}.tar.gz && \
    cd openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR} && \
    ./configure \
    --prefix=${OPENMPI_INSTALL_PREFIX}/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR} \
    --with-ucx=${UCX_PREFIX} \
    --with-ucx-libdir=/usr/lib/${UCX_MULTIARCH}; \
    make -j$(nproc) && \
    make install

export OPENMPI_VERSION_MAJOR=4.1 
export OPENMPI_VERSION_MINOR=8
export OPENMPI_INSTALL_PREFIX=/usr/local
export DEBIAN_FRONTEND=noninteractive
export LANG=C.UTF-8 LC_ALL=C.UTF-8

export PATH="${OPENMPI_INSTALL_PREFIX}/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}/bin:${PATH}"
export LD_LIBRARY_PATH="${OPENMPI_INSTALL_PREFIX}/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}/lib"
export OMPI_ALLOW_RUN_AS_ROOT=1 
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 
export OMPI=/usr/local/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}

# install check
mpirun --version; \
  pkg-config --modversion ucx; \
  ompi_info --param pml all | grep -i ucx || true; \
  ompi_info --param osc all | grep -i ucx || true


# --------------- # install libcircle --> lwgrp --> DTCMP --> mpifileutils # --------------- 
set -eux; \
  export MFU_DEPS="${OPENMPI_INSTALL_PREFIX}/mfu-deps"; \
  mkdir -p $MFU_DEPS; \
  cd $MFU_DEPS; \
  wget https://github.com/hpc/libcircle/releases/download/v0.3/libcircle-0.3.0.tar.gz; \
  tar -zxf libcircle-0.3.0.tar.gz; \
  cd libcircle-0.3.0; \
  ./configure --prefix=$MFU_DEPS CC=mpicc; \
  make -j$(nproc); \
  make install; \
  cd $MFU_DEPS; \
  wget https://github.com/llnl/lwgrp/releases/download/v1.0.4/lwgrp-1.0.4.tar.gz; \
  tar -zxf lwgrp-1.0.4.tar.gz; \
  cd lwgrp-1.0.4; \
  ./configure --prefix=$MFU_DEPS CC=mpicc; \
  make -j$(nproc); \
  make install; \
  cd $MFU_DEPS; \
  wget https://github.com/llnl/dtcmp/releases/download/v1.1.4/dtcmp-1.1.4.tar.gz; \
  tar -zxf dtcmp-1.1.4.tar.gz; \
  cd dtcmp-1.1.4; \
  ./configure --prefix=$MFU_DEPS --with-lwgrp=$MFU_DEPS CC=mpicc; \
  make -j$(nproc); \
  make install; \
  cd /opt; \
  git clone https://github.com/ChahwanSong/mpifileutils.git; \
  cd mpifileutils; \
  rm -rf build; \
  mkdir build && cd build; \
  cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=${OPENMPI_INSTALL_PREFIX}/mpifileutils \
  -DWITH_DTCMP_PREFIX=$MFU_DEPS \
  -DWITH_LibCircle_PREFIX=$MFU_DEPS; \
  make -j$(nproc); \
  make install; \
  MFU_DEPS=${OPENMPI_INSTALL_PREFIX}/mfu-deps; \
  PATH=${OPENMPI_INSTALL_PREFIX}/mpifileutils/bin:$PATH; \
  LD_LIBRARY_PATH=${OPENMPI_INSTALL_PREFIX}/mfu-deps/lib:${OPENMPI_INSTALL_PREFIX}/mpifileutils/lib:$LD_LIBRARY_PATH


# SSH
mkdir -p /var/run/sshd
ssh-keygen -A
sed -i 's/#PermitEmptyPasswords no/PermitEmptyPasswords yes/' /etc/ssh/sshd_config
sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
passwd -d root
nohup /usr/sbin/sshd -D > /var/log/sshd.log 2>&1 &


```

# Source
```shell
export OPENMPI_VERSION_MAJOR=4.1 
export OPENMPI_VERSION_MINOR=8
export OPENMPI_INSTALL_PREFIX=/usr/local
export DEBIAN_FRONTEND=noninteractive
export LANG=C.UTF-8 LC_ALL=C.UTF-8

export PATH="${OPENMPI_INSTALL_PREFIX}/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}/bin:${PATH}"
export LD_LIBRARY_PATH="${OPENMPI_INSTALL_PREFIX}/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}/lib"
export OMPI_ALLOW_RUN_AS_ROOT=1 
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 
export OMPI=/usr/local/openmpi-${OPENMPI_VERSION_MAJOR}.${OPENMPI_VERSION_MINOR}
export PATH=${OPENMPI_INSTALL_PREFIX}/mpifileutils/bin:$PATH; \
export LD_LIBRARY_PATH=${OPENMPI_INSTALL_PREFIX}/mfu-deps/lib:${OPENMPI_INSTALL_PREFIX}/mpifileutils/lib:$LD_LIBRARY_PATH
export MFU_DEPS="${OPENMPI_INSTALL_PREFIX}/mfu-deps";
```