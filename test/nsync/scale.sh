#!/bin/bash
# 복사 루프 스케일링: 새 목적지로 작은 파일 N 개(디렉터리 N/10) — BIN 별 소요 시간. 결과를 줄마다 /work/tests/scale.out 에.
BIN=${BIN:-/opt/mpifileutils/bin}; ROLES=${ROLES:-0:src,1:dst}; OUT=/work/tests/scale.out
np=$(echo "$ROLES" | tr ',' '\n' | wc -l)
for N in ${SIZES:-10000 20000 40000}; do
  W=/tmp/scale$N
  if [ ! -d $W/src ]; then
    mkdir -p $W/src
    python3 - "$W/src" "$N" <<'EOF'
import os, sys
src, n = sys.argv[1], int(sys.argv[2])
for i in range(n):
    p = os.path.join(src, "d%05d" % (i // 10))
    os.makedirs(p, exist_ok=True)
    with open(os.path.join(p, "f%d" % i), "w") as fh:
        fh.write("x" * 64)
EOF
  fi
  rm -rf $W/dst
  t0=$(date +%s.%N)
  timeout 1800 mpirun -np $np $BIN/nsync --role-mode map --role-map $ROLES -q $W/src $W/dst >/dev/null 2>&1; rc=$?
  t1=$(date +%s.%N)
  echo "$(basename $(dirname $BIN)) roles=$ROLES N=$N rc=$rc seconds=$(awk "BEGIN{printf \"%.3f\", $t1 - $t0}")" | tee -a $OUT
done
