#!/bin/bash
# 재동기화(타입 교체 0건) 시간: 디렉터리 D·파일 D*F. 배치 여럿(선처리 게이트 통과) — 수정 전후 + 선처리 줄.
D=${D:-10000}; F=${F:-10}; ROLES=${ROLES:-0:src,1:src,2:dst,3:dst}; OUT=/work/tests/perf2.out
W=/tmp/perf2; SRC=$W/src
if [ ! -d $SRC ]; then
  mkdir -p $SRC
  python3 - "$SRC" "$D" "$F" <<'EOF'
import os, sys
src, d, f = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(d):
    p = os.path.join(src, "a%03d" % (i % 100), "b%05d" % i)
    os.makedirs(p, exist_ok=True)
    for j in range(f):
        with open(os.path.join(p, "f%02d" % j), "w") as fh:
            fh.write("x" * 64)
EOF
fi
np=$(echo "$ROLES" | tr ',' '\n' | wc -l)
for BIN in /opt/mfu-base/bin /opt/mpifileutils/bin; do
  rm -rf $W/dst
  timeout 1800 mpirun -np $np $BIN/nsync --role-mode map --role-map $ROLES -q $SRC $W/dst >/dev/null 2>&1
  for opt in "--batch-files 20000" ""; do
    t0=$(date +%s.%N)
    timeout 1800 mpirun -np $np $BIN/nsync --role-mode map --role-map $ROLES $opt $SRC $W/dst > $W/log 2>&1; rc=$?
    t1=$(date +%s.%N)
    echo "$(basename $(dirname $BIN)) resync opts='$opt' rc=$rc seconds=$(echo "$t1 - $t0" | bc) $(grep -o 'Type-change pre-pass.*' $W/log)" | tee -a $OUT
  done
done
