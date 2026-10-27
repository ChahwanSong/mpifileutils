#!/bin/sh
# 증분 빌드: /work/src(포크 작업 사본, 읽기 전용 마운트) → /release/mpifileutils → /src/build → /opt/mpifileutils.
# DMS Dockerfile 의 sed·CMake 조정과 같다. 경고는 nsync.c·dsync.c 것만 따로 보여 준다.
set -eu
rsync -a --delete --exclude .git /work/src/ /release/mpifileutils/
sed -i 's@ADD_SUBDIRECTORY(common)@#ADD_SUBDIRECTORY(common)@g' /release/mpifileutils/src/CMakeLists.txt
cp /release/mpifileutils/dist/CMakeLists.txt /release/CMakeLists.txt.new
rm -rf /release/cmake && cp -a /release/mpifileutils/cmake /release/cmake
sed -i 's@ADD_SUBDIRECTORY(mpifileutils/test)@#ADD_SUBDIRECTORY(mpifileutils/test)@g' /release/CMakeLists.txt.new
sed -i '/mpifileutils\/src\/common\/mfu_pred.h/a\  mpifileutils/src/common/mfu_proc.h' /release/CMakeLists.txt.new
sed -i '/mpifileutils\/src\/common\/mfu_pred.c/a\  mpifileutils/src/common/mfu_proc.c' /release/CMakeLists.txt.new
if ! cmp -s /release/CMakeLists.txt.new /release/CMakeLists.txt 2>/dev/null; then mv /release/CMakeLists.txt.new /release/CMakeLists.txt; else rm /release/CMakeLists.txt.new; fi
mkdir -p /src/build && cd /src/build
if [ ! -f Makefile ]; then
  cmake /release -DCMAKE_INSTALL_PREFIX=/opt/mpifileutils -DENABLE_LIBARCHIVE=OFF -DENABLE_XATTRS=OFF -DENABLE_LUSTRE=OFF \
    -DENABLE_GPFS=OFF -DENABLE_HPSS=OFF -DENABLE_DAOS=OFF -DCMAKE_C_FLAGS="-Wall -Wextra -Wno-unused-parameter -Wno-sign-compare" >/tmp/cmake.log 2>&1 \
    || { tail -30 /tmp/cmake.log; exit 1; }
fi
make -j8 install >/tmp/make.log 2>&1 || { grep -E "error|Error" /tmp/make.log | head -40; tail -20 /tmp/make.log; exit 1; }
echo "build ok"
grep -E "(nsync|dsync)\.c:[0-9]+:[0-9]+: warning" /tmp/make.log | sort -u | head -40 || true
