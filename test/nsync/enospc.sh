#!/bin/bash
# ENOSPC: 4 MiB tmpfs(/mnt/small) 목적지로 200 KiB 파일 60개(12 MiB). 기대: rc 1, 'destination is full' 로 남은 배치 중단,
# 시도하지 않은 작업 보고, 같은 배치의 남은 COPY 는 취소(소스가 끝까지 읽어 보내지 않음).
BIN=${BIN:-/opt/mpifileutils/bin}
S=/tmp/enosrc; rm -rf $S /mnt/small/*; mkdir -p $S
for i in $(seq -w 1 60); do head -c 204800 /dev/urandom > $S/f$i; done
for opt in "--batch-files 5" ""; do
  rm -rf /mnt/small/dst
  timeout 300 mpirun -np 3 $BIN/nsync --role-mode map --role-map 0:src,1:src,2:dst $opt $S /mnt/small/dst > /tmp/enospc.log 2>&1; rc=$?
  echo "== opts='$opt' rc=$rc copied=$(ls /mnt/small/dst 2>/dev/null | wc -l)"
  grep -E "Stopping|Not attempted|Failed items|Execution completed|Batch mode" /tmp/enospc.log | cut -c1-200
done
