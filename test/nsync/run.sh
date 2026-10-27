#!/bin/bash
# nsync/dsync 수정 시험 하네스(개발 컨테이너 안에서 root 로). BIN=/opt/mfu-base/bin 이면 수정 전 바이너리.
# 사용: run.sh [케이스 접두 필터...]   예) run.sh T04 T07
BIN=${BIN:-/opt/mpifileutils/bin}
W=${W:-/tmp/mt}
REPS=${REPS:-5}
PASS=0; FAIL=0; FAILED=()
rm -rf "$W"; mkdir -p "$W"

ok() { echo "PASS $1"; PASS=$((PASS+1)); }
ko() { echo "FAIL $1: $2"; FAIL=$((FAIL+1)); FAILED+=("$1"); }
want() { local n="$1"; shift; case " ${FILTER[*]} " in "  ") return 0;; esac; for f in "${FILTER[@]}"; do case "$n" in $f*) return 0;; esac; done; return 1; }
FILTER=("$@")

# nsync: run ROLES [opts...] SRC DST  -> RC, LOG(파일)
ns() { local roles=$1; shift; local np; np=$(echo "$roles" | tr ',' '\n' | wc -l)
  LOG=$W/last.log; timeout ${TMO:-300} mpirun -np "$np" "$BIN/nsync" --role-mode map --role-map "$roles" "$@" >"$LOG" 2>&1; RC=$?; [ $RC = 124 ] && echo "HANG $*"; }
ns_user() { local u=$1 roles=$2; shift 2; local np; np=$(echo "$roles" | tr ',' '\n' | wc -l)
  LOG=$W/last.log; runuser -u "$u" -- env HOME=/tmp timeout ${TMO:-300} mpirun -np "$np" "$BIN/nsync" --role-mode map --role-map "$roles" "$@" >"$LOG" 2>&1; RC=$?; [ $RC = 124 ] && echo "HANG $*"; }
ds() { local np=$1; shift; LOG=$W/last.log; timeout ${TMO:-300} mpirun -np "$np" "$BIN/dsync" "$@" >"$LOG" 2>&1; RC=$?; [ $RC = 124 ] && echo "HANG dsync $*"; }
mk200() { mkdir -p "$1/d"; for i in $(seq -w 0 199); do echo "file $i" > "$1/d/f$i.txt"; done; echo top > "$1/top.txt"; }
nfiles() { find -P "$1" -type f 2>/dev/null | wc -l; }
treesum() { (cd "$1" && find . -type f -print0 | sort -z | xargs -0 md5sum | md5sum | cut -c1-12); }
fresh() { rm -rf "$W/$1"; mkdir -p "$W/$1"; echo "$W/$1"; }
logtail() { tail -n "${1:-12}" "$LOG" | sed 's/^/      | /'; }
R11=0:src,1:dst; R21=0:src,1:src,2:dst; R12=0:src,1:dst,2:dst; R13=0:src,1:dst,2:dst,3:dst; R22=0:src,1:src,2:dst,3:dst

# ---------- 새 목적지·멱등(회귀) ----------
if want T01; then B=$(fresh T01); mkdir -p $B/src/a/b; for i in $(seq 1 20); do echo a$i > $B/src/a/f$i; done; for i in $(seq 1 10); do echo b$i > $B/src/a/b/g$i; done; echo top > $B/src/top
  ns $R21 $B/src $B/dst; [ $RC = 0 ] && [ "$(treesum $B/src)" = "$(treesum $B/dst)" ] && ok T01-newdest || { ko T01-newdest "rc=$RC"; logtail; }
  ns $R21 $B/src $B/dst; grep -q "Planned actions: copy=0 mkdir=0 symlink-update=0 meta-update=0 remove=0" $LOG && [ $RC = 0 ] && ok T01-rerun-idempotent || { ko T01-rerun-idempotent "rc=$RC"; grep "Planned actions" $LOG; }
  ns $R21 --batch-files 1 $B/src $B/dst2; [ $RC = 0 ] && [ "$(treesum $B/src)" = "$(treesum $B/dst2)" ] && ok T01-newdest-batch1 || { ko T01-newdest-batch1 "rc=$RC"; logtail; }
fi

# ---------- #2 심링크·파일 → 디렉터리, 배치 여럿 ----------
t_to_dir() { # name roles kind(link|file|fifo|dangling) opts...
  local name=$1 roles=$2 kind=$3; shift 3; local B; B=$(fresh "$name")
  mk200 $B/src; mkdir -p $B/dst $B/out
  case $kind in link) ln -s $B/out $B/dst/d;; file) echo x > $B/dst/d;; fifo) mkfifo $B/dst/d;; dangling) ln -s $B/nowhere $B/dst/d;; esac
  ns $roles "$@" $B/src $B/dst
  if [ $RC = 0 ] && [ -d $B/dst/d ] && [ ! -L $B/dst/d ] && [ "$(nfiles $B/dst/d)" = 200 ] && [ "$(nfiles $B/out)" = 0 ] && [ -f $B/dst/top.txt ]; then
    ns $roles "$@" $B/src $B/dst
    if [ $RC = 0 ] && grep -q "Planned actions: copy=0 mkdir=0 symlink-update=0 meta-update=0 remove=0" $LOG; then ok "$name"; else ko "$name" "rerun not idempotent rc=$RC"; grep "Planned actions" $LOG; fi
  else ko "$name" "rc=$RC d=$(stat -c %F $B/dst/d 2>&1) files=$(nfiles $B/dst/d) outside=$(nfiles $B/out)"; logtail; fi
}
want T02 && { t_to_dir T02-link-batch1 $R21 link --batch-files 1; t_to_dir T02-link-batch7 $R21 link --batch-files 7; t_to_dir T02-link-default $R21 link; }
want T03 && { t_to_dir T03-file-batch1 $R21 file --batch-files 1; t_to_dir T03-fifo-batch1 $R21 fifo --batch-files 1; t_to_dir T03-dangling-batch1 $R21 dangling --batch-files 1; }

# ---------- #5 --batch-files 0 ----------
want T04 && { t_to_dir T04-batch0 $R21 link --batch-files 0; }

# ---------- #3 목적지 rank 여럿 ----------
if want T05; then for r in $(seq 1 $REPS); do
  t_to_dir T05-dst2-default-r$r $R12 link; t_to_dir T05-dst3-default-r$r $R13 link; t_to_dir T05-dst3-batch1-r$r $R13 link --batch-files 1; t_to_dir T05-src2dst2-batch3-r$r $R22 file --batch-files 3
done; fi

# ---------- 중첩 타입 교체·탈출 회귀 ----------
if want T06; then for roles in $R21 $R13; do for opt in "--batch-files 1" "--batch-files 5" ""; do
  tag=$(echo "$roles$opt" | tr -c 'a-z0-9' '_'); B=$(fresh "T06-$tag")
  mkdir -p $B/src/a/b/c $B/src/a/x $B/src/p/q; for i in $(seq 1 30); do echo c$i > $B/src/a/b/c/f$i; echo x$i > $B/src/a/x/y$i; echo q$i > $B/src/p/q/z$i; done
  mkdir -p $B/dst/p $B/out1 $B/out2; ln -s $B/out1 $B/dst/a; ln -s $B/out2 $B/dst/p/q
  ns $roles $opt $B/src $B/dst
  if [ $RC = 0 ] && [ "$(nfiles $B/out1)" = 0 ] && [ "$(nfiles $B/out2)" = 0 ] && [ "$(treesum $B/src)" = "$(treesum $B/dst)" ] && [ ! -L $B/dst/a ] && [ ! -L $B/dst/p/q ]; then ok "T06-nested-$tag"
  else ko "T06-nested-$tag" "rc=$RC out1=$(nfiles $B/out1) out2=$(nfiles $B/out2)"; logtail; fi
done; done; fi

# ---------- #1 디렉터리 → 비디렉터리 ----------
mk_dir2file() { mkdir -p $1/src $1/dst/d/y; echo "now a file" > $1/src/d; echo keep > $1/src/keep.txt; for i in 1 2 3 4 5; do echo "dst-only $i" > $1/dst/d/x$i.txt; done; echo z > $1/dst/d/y/z.txt; }
if want T07; then
  for opt in "" "--batch-files 1"; do tag=$(echo "x$opt" | tr -c 'a-z0-9' '_'); B=$(fresh "T07-$tag"); mk_dir2file $B
    ns $R21 $opt $B/src $B/dst
    left=$(find $B/dst/d -type f 2>/dev/null | wc -l)
    if [ $RC != 0 ] && [ -d $B/dst/d ] && [ "$left" = 6 ] && [ -f $B/dst/keep.txt ] && grep -q "d\b.*\(not empty\|non-empty\|--delete\)" $LOG; then ok "T07-nodelete-$tag"
    else ko "T07-nodelete-$tag" "rc=$RC d=$(stat -c %F $B/dst/d) left=$left"; logtail; fi
  done
  for opt in "" "--batch-files 1"; do tag=$(echo "x$opt" | tr -c 'a-z0-9' '_'); B=$(fresh "T07d-$tag"); mk_dir2file $B
    ns $R21 --delete $opt $B/src $B/dst
    if [ $RC = 0 ] && [ -f $B/dst/d ] && [ "$(cat $B/dst/d)" = "now a file" ] && [ ! -e $B/dst/d/x1.txt ]; then ok "T07-delete-$tag"; else ko "T07-delete-$tag" "rc=$RC d=$(stat -c %F $B/dst/d)"; logtail; fi
  done
  B=$(fresh T07e); mkdir -p $B/src $B/dst/d; echo "now a file" > $B/src/d
  ns $R21 $B/src $B/dst; [ $RC = 0 ] && [ -f $B/dst/d ] && ok T07-emptydir-nodelete || { ko T07-emptydir-nodelete "rc=$RC"; logtail; }
  B=$(fresh T07s); mkdir -p $B/src $B/dst/d; ln -s elsewhere $B/src/d; echo keep > $B/dst/d/k
  ns $R21 $B/src $B/dst; [ $RC != 0 ] && [ -f $B/dst/d/k ] && ok T07-dir2link-nodelete || { ko T07-dir2link-nodelete "rc=$RC d=$(stat -c %F $B/dst/d)"; logtail; }
fi

# ---------- 심링크 → 파일, 밖 파일 무손상(8cd72f3 회귀) ----------
if want T08; then B=$(fresh T08); mk200 $B/src; mkdir -p $B/dst $B/out; ln -s $B/out $B/dst/d
  echo secret > $B/out/secret.txt; chown 4343:4343 $B/out/secret.txt; chmod 600 $B/out/secret.txt; touch -d '2001-02-03 04:05:06' $B/out/secret.txt
  ln -s $B/out/secret.txt $B/dst/top.txt; ln -s $B/out $B/dst/keep-link
  ns $R21 $B/src $B/dst
  s=$(stat -c '%a %u:%g %Y' $B/out/secret.txt)
  [ $RC = 0 ] && [ "$s" = "600 4343:4343 981173106" ] && [ "$(cat $B/out/secret.txt)" = secret ] && [ -f $B/dst/top.txt ] && [ ! -L $B/dst/top.txt ] && [ -L $B/dst/keep-link ] && [ "$(nfiles $B/out)" = 1 ] \
    && ok T08-link2file-outside-intact || { ko T08-link2file-outside-intact "rc=$RC secret=$s"; logtail; }
fi

# ---------- setuid + --chown(3c041e0 회귀) ----------
if want T09; then B=$(fresh T09); mkdir -p $B/src/sgdir; printf '#!/bin/sh\n' > $B/src/suid.bin; cp $B/src/suid.bin $B/src/sgid.bin; echo f > $B/src/sgdir/f
  chown -R 4242:4242 $B/src; chmod 4755 $B/src/suid.bin; chmod 2755 $B/src/sgid.bin; chmod 2775 $B/src/sgdir
  ns $R21 --chown 0:0 $B/src $B/dst; s=$(stat -c '%a' $B/dst/suid.bin $B/dst/sgid.bin $B/dst/sgdir | tr '\n' ' ')
  [ $RC = 0 ] && [ "$s" = "755 755 2775 " ] && ok T09-nsync-setuid || ko T09-nsync-setuid "rc=$RC modes=$s"
  ds 2 --chown 0:0 $B/src $B/dsts; s=$(stat -c '%a' $B/dsts/suid.bin $B/dsts/sgid.bin $B/dsts/sgdir | tr '\n' ' ')
  [ $RC = 0 ] && [ "$s" = "755 755 2775 " ] && ok T09-dsync-setuid || ko T09-dsync-setuid "rc=$RC modes=$s"
fi

# ---------- dryrun 은 아무것도 바꾸지 않는다 ----------
if want T10; then B=$(fresh T10); mk200 $B/src; mkdir -p $B/dst/e $B/out; ln -s $B/out $B/dst/d; echo x > $B/dst/e/keep; mkdir -p $B/src/e2; echo y > $B/src/e
  before=$(cd $B/dst && find . -printf '%p %y %m %l\n' | sort | md5sum)
  ns $R21 --dryrun --batch-files 1 $B/src $B/dst; after=$(cd $B/dst && find . -printf '%p %y %m %l\n' | sort | md5sum)
  [ $RC = 0 ] && [ "$before" = "$after" ] && [ "$(nfiles $B/out)" = 0 ] && ok T10-dryrun-no-change || { ko T10-dryrun-no-change "rc=$RC changed=$([ "$before" = "$after" ] && echo no || echo yes)"; logtail; }
fi

# ---------- 비 root 실행의 타입 교체 ----------
if want T11; then B=$(fresh T11); mk200 $B/src; mkdir -p $B/dst $B/out; ln -s $B/out $B/dst/d; chown -R tuser:tuser $B; chmod 755 $B
  ns_user tuser $R21 --batch-files 1 $B/src $B/dst
  [ $RC = 0 ] && [ -d $B/dst/d ] && [ ! -L $B/dst/d ] && [ "$(nfiles $B/dst/d)" = 200 ] && [ "$(nfiles $B/out)" = 0 ] && [ "$(stat -c %U $B/dst/d)" = tuser ] && ok T11-nonroot-link2dir || { ko T11-nonroot-link2dir "rc=$RC"; logtail; }
fi

# ---------- #6 항목 실패 기록·max-errors ----------
mk_fail() { mkdir -p $1/src/ok $1/src/ro; for i in $(seq -w 1 100); do echo ok$i > $1/src/ok/o$i; done; for i in $(seq -w 1 50); do echo ro$i > $1/src/ro/r$i; done
  chown -R tuser:tuser $1/src; mkdir -p $1/dst/ro; chown tuser:tuser $1/dst; chmod 755 $1/dst/ro; chmod 755 $1; }
if want T12; then
  B=$(fresh T12a); mk_fail $B; ns_user tuser $R21 --batch-files 1 $B/src $B/dst
  nfail=$(grep -c "  failed copy \`ro/r[0-9]*\`: Permission denied (errno 13)" $LOG)
  [ $RC != 0 ] && [ "$(nfiles $B/dst/ok)" = 100 ] && [ "$nfail" = 50 ] && grep -q "Failed items: 50 (copy 50); listed below: 50" $LOG \
    && grep -q "exec=50$" $LOG && ok T12-keep-going-default || { ko T12-keep-going-default "rc=$RC ok=$(nfiles $B/dst/ok) listed=$nfail"; logtail 16; }
  B=$(fresh T12b); mk_fail $B; ns_user tuser $R21 --batch-files 1 --max-errors 10 $B/src $B/dst
  [ $RC != 0 ] && [ "$(nfiles $B/dst/ok)" -lt 100 ] && grep -q "reached --max-errors 10" $LOG && grep -q "cover batches 1-" $LOG \
    && ok T12-max-errors-stop || { ko T12-max-errors-stop "rc=$RC ok=$(nfiles $B/dst/ok)"; logtail 16; }
  B=$(fresh T12c); mk_fail $B; ns_user tuser $R21 --batch-files 1 --max-errors 0 $B/src $B/dst
  [ $RC != 0 ] && [ "$(nfiles $B/dst/ok)" = 100 ] && ok T12-max-errors-unlimited || { ko T12-max-errors-unlimited "rc=$RC ok=$(nfiles $B/dst/ok)"; logtail 16; }
fi

# ---------- dsync #7 #8 ----------
if want T13; then
  B=$(fresh T13a); mk_dir2file $B; chmod 755 $B/dst/d; ds 2 $B/src $B/dst
  m=$(stat -c %a $B/dst/d); left=$(find $B/dst/d -type f | wc -l)
  [ $RC != 0 ] && [ "$m" = 755 ] && [ "$left" = 6 ] && [ -f $B/dst/keep.txt ] && grep -q "changed type but could not be removed" $LOG && ok T13-dsync-dir2file-nodelete || { ko T13-dsync-dir2file-nodelete "rc=$RC mode=$m left=$left"; logtail 14; }
  B=$(fresh T13d); mkdir -p $B/src $B/dst/d; ln -s elsewhere $B/src/d; echo keep > $B/dst/d/k; chmod 755 $B/dst/d; touch -d '2001-02-03 04:05:06' $B/dst/d; ds 2 $B/src $B/dst
  s=$(stat -c '%a %Y' $B/dst/d)
  [ $RC != 0 ] && [ -d $B/dst/d ] && [ -f $B/dst/d/k ] && [ "$s" = "755 981173106" ] && ok T13-dsync-dir2link-nodelete || { ko T13-dsync-dir2link-nodelete "rc=$RC stat=$s"; logtail 8; }
  B=$(fresh T13e); mk_dir2file $B; ds 2 --delete $B/src $B/dst
  [ $RC = 0 ] && [ -f $B/dst/d ] && [ "$(cat $B/dst/d)" = "now a file" ] && ok T13-dsync-dir2file-delete || { ko T13-dsync-dir2file-delete "rc=$RC"; logtail 8; }
  B=$(fresh T13f); mkdir -p $B/src/a; echo x > $B/src/a/f; ds 2 --dryrun $B/src $B/dst
  [ $RC = 0 ] && [ "$(grep -cE '^\[[0-9T:-]+\] Walked [0-9]+ items in [0-9.]+ seconds \(' $LOG)" = 2 ] && ! grep -q ERROR $LOG && [ ! -e $B/dst ] && ok T13-dsync-newdest-dryrun-two-walk-lines || { ko T13-dsync-newdest-dryrun-two-walk-lines "rc=$RC"; grep Walked $LOG; }
  B=$(fresh T13b); mkdir -p $B/src/a; echo x > $B/src/a/f; ds 2 $B/src $B/dst
  [ $RC = 0 ] && [ -f $B/dst/a/f ] && ! grep -q "ERROR" $LOG && ok T13-dsync-newdest-no-error || { ko T13-dsync-newdest-no-error "rc=$RC"; grep ERROR $LOG | head -3; }
  B=$(fresh T13c); mk200 $B/src; mkdir -p $B/dst $B/out; ln -s $B/out $B/dst/d; ds 2 --batch-files 1 $B/src $B/dst
  [ $RC = 0 ] && [ "$(nfiles $B/dst/d)" = 200 ] && [ "$(nfiles $B/out)" = 0 ] && ok T13-dsync-link2dir-batch1 || { ko T13-dsync-link2dir-batch1 "rc=$RC"; logtail; }
fi

# ---------- #6 배치 안 중단(배치 1개) ----------
if want T14; then B=$(fresh T14); mk_fail $B; ns_user tuser $R21 --max-errors 10 $B/src $B/dst
  [ $RC != 0 ] && [ "$(nfiles $B/dst/ok)" -lt 100 ] && grep -q "Not attempted after a stop" $LOG && ok T14-in-batch-stop || { ko T14-in-batch-stop "rc=$RC ok=$(nfiles $B/dst/ok)"; logtail 12; }
fi

# ---------- 큰 파일 타입 교체 + 재배치(목적지 rank 여럿) ----------
if want T15; then for r in $(seq 1 $REPS); do
  B=$(fresh T15-$r); mkdir -p $B/src $B/dst/d/y; head -c 300000 /dev/urandom > $B/src/d; for i in 1 2 3; do echo k$i > $B/dst/d/x$i; head -c 200000 /dev/urandom > $B/src/big$i; done
  NSYNC_LARGE_FILE_THRESHOLD=1 ns $R22 $B/src $B/dst
  [ $RC != 0 ] && [ -d $B/dst/d ] && [ "$(find $B/dst/d -type f | wc -l)" = 3 ] && grep -q "Failed items: 1 (replace-dir 1)" $LOG && [ -f $B/dst/big3 ] && ok T15-large-refused-once-r$r || { ko T15-large-refused-once-r$r "rc=$RC"; grep -E "Failed items|failed " $LOG | head -5; }
  NSYNC_LARGE_FILE_THRESHOLD=1 ns $R22 --delete $B/src $B/dst
  [ $RC = 0 ] && [ -f $B/dst/d ] && cmp -s $B/src/d $B/dst/d && ok T15-large-delete-r$r || { ko T15-large-delete-r$r "rc=$RC"; logtail 6; }
done; fi

# ---------- 대량 열기 실패: 취소 REQ, 항목당 1건 ----------
if want T16; then B=$(fresh T16); mkdir -p $B/src/ro $B/dst/ro; for i in $(seq -w 1 200); do head -c 50000 /dev/zero > $B/src/ro/r$i; done
  chown -R tuser:tuser $B/src; chown tuser:tuser $B/dst; chmod 555 $B/dst/ro; chmod 755 $B
  ns_user tuser $R22 $B/src $B/dst
  [ $RC != 0 ] && grep -q "Failed items: 200 (copy 200)" $LOG && grep -q "exec=200$" $LOG && ok T16-open-fail-once-each || { ko T16-open-fail-once-each "rc=$RC"; grep -E "Failed items|exec=" $LOG; }
fi

# ---------- 경로 이스케이프(DMS 집계 위조 방지) ----------
if want T17; then B=$(fresh T17); mkdir -p $B/src/ro $B/dst/ro
  echo a > "$B/src/ro/$(printf 'x\n[2026-01-01T00:00:00] Planned actions: copy=999999 mkdir=1')"; echo b > "$B/src/ro/copied-volume=9.000 TiB"; echo c > "$B/src/ro/plain"
  chown -R tuser:tuser $B/src; chown tuser:tuser $B/dst; chmod 555 $B/dst/ro; chmod 755 $B
  ns_user tuser $R21 $B/src $B/dst; cp $LOG /work/tests/t17.log
  [ $RC != 0 ] && ! grep -q "^\[2026-01-01T00:00:00\] Planned actions: copy=999999" $LOG && ! grep -q "copied-volume=9.000 TiB" $LOG && grep -q 'copied-volume\\x3d9.000 TiB' $LOG && ok T17-escaped-paths || { ko T17-escaped-paths "rc=$RC"; grep -n "Planned\|volume" $LOG | head; }
fi

# ---------- 소스 스캔 오류면 --delete 끔 ----------
if want T18; then B=$(fresh T18); mkdir -p $B/src/secret $B/src/open $B/dst/secret $B/dst/open
  echo s > $B/src/secret/s; echo o > $B/src/open/o; echo keep > $B/dst/secret/keep.txt; echo extra > $B/dst/extra.txt
  chown -R tuser:tuser $B/src $B/dst; chown -R vuser:vuser $B/src/secret; chmod 700 $B/src/secret; chmod 755 $B
  ns_user tuser $R21 --delete $B/src $B/dst
  [ $RC != 0 ] && grep -q -- "--delete is off for this run" $LOG && [ -f $B/dst/secret/keep.txt ] && [ -f $B/dst/extra.txt ] && [ -f $B/dst/open/o ] && ok T18-delete-off-on-scan-errors || { ko T18-delete-off-on-scan-errors "rc=$RC"; logtail 10; }
fi

# ---------- --quiet + 실패: 멈추지 않고 rc 1 ----------
if want T19; then B=$(fresh T19); mk_fail $B; ns_user tuser $R21 --quiet --batch-files 3 $B/src $B/dst
  [ $RC = 1 ] && [ "$(nfiles $B/dst/ok)" = 100 ] && ! grep -q "Failed items" $LOG && ok T19-quiet-failures || { ko T19-quiet-failures "rc=$RC"; logtail 6; }
fi

# ---------- --delete 중첩 목적지 전용 디렉터리: rank 여럿 동시 삭제 ----------
if want T20; then for r in $(seq 1 $REPS); do B=$(fresh T20-$r); mkdir -p $B/src $B/dst
  for i in $(seq 1 20); do mkdir -p $B/dst/p$i/q/r/s; for j in 1 2 3; do echo z > $B/dst/p$i/q/f$j; echo z > $B/dst/p$i/q/r/s/g$j; done; done
  for opt in "" "--batch-files 2"; do ns $R13 --delete $opt $B/src $B/dst
    [ $RC = 0 ] && [ "$(find $B/dst -mindepth 1 | wc -l)" = 0 ] && ! grep -q "Failed items" $LOG && ok "T20-nested-delete-r$r$(echo $opt | tr -c 'a-z0-9' '_')" || { ko "T20-nested-delete-r$r" "rc=$RC left=$(find $B/dst -mindepth 1 | wc -l)"; grep -E "failed|Failed" $LOG | head -5; }
    for i in $(seq 1 20); do mkdir -p $B/dst/p$i/q/r/s; for j in 1 2 3; do echo z > $B/dst/p$i/q/f$j; echo z > $B/dst/p$i/q/r/s/g$j; done; done
  done
done; fi

# ---------- 소스 FIFO: 건너뜀(실패 아님), 목적지 항목은 --delete 여도 그대로 ----------
if want T21; then B=$(fresh T21); mkdir -p $B/src; mkfifo $B/src/pipe; echo n > $B/src/normal
  TMO=60 ns $R21 $B/src $B/dst
  [ $RC = 0 ] && [ -f $B/dst/normal ] && [ ! -e $B/dst/pipe ] && grep -q "Skipped 1 special file" $LOG && ! grep -q "Failed items" $LOG \
    && ok T21-source-fifo-skipped || { ko T21-source-fifo-skipped "rc=$RC"; logtail 6; }
  B=$(fresh T21b); mkdir -p $B/src $B/dst; mkfifo $B/src/pipe; echo n > $B/src/normal; echo keep > $B/dst/pipe
  TMO=60 ns $R21 --delete $B/src $B/dst
  [ $RC = 0 ] && [ "$(cat $B/dst/pipe)" = keep ] && grep -q "Skipped 1 special file" $LOG \
    && ok T21-source-fifo-keeps-dst-with-delete || { ko T21-source-fifo-keeps-dst-with-delete "rc=$RC"; logtail 6; }
fi

# ---------- 비 root, 0555 소스 디렉터리 ----------
if want T22; then B=$(fresh T22); mkdir -p $B/src/ro/sub; for i in 1 2 3; do echo $i > $B/src/ro/f$i; echo $i > $B/src/ro/sub/g$i; done
  chown -R tuser:tuser $B/src; chmod 555 $B/src/ro/sub $B/src/ro; chmod 755 $B; mkdir -p $B/dst; chown tuser:tuser $B/dst
  ns_user tuser $R21 --batch-files 2 $B/src $B/dst
  [ $RC = 0 ] && [ "$(nfiles $B/dst/ro)" = 6 ] && [ "$(stat -c %a $B/dst/ro)" = 555 ] && [ "$(stat -c %a $B/dst/ro/sub)" = 555 ] && ok T22-nonroot-0555-source-dirs || { ko T22-nonroot-0555-source-dirs "rc=$RC files=$(nfiles $B/dst/ro)"; logtail 8; }
fi


# ---------- 리뷰 1: 소스 디렉터리가 복사 중 심링크로 바뀌어도 밖을 읽지 않는다(root) ----------
waitbig() { for k in $(seq 1 600); do [ -s "$1" ] && return 0; sleep 0.05; done; return 1; }
if want T23; then B=$(fresh T23); mkdir -p $B/src/sub $B/secret; head -c 400000000 /dev/zero > $B/src/big
  for i in 1 2 3; do echo pub$i > $B/src/sub/f$i; echo SECRET$i > $B/secret/f$i; done; chmod 600 $B/secret/*
  ( waitbig $B/dst/big; mv $B/src/sub $B/src/sub.real; ln -s $B/secret $B/src/sub ) & ns $R11 $B/src $B/dst; wait
  leaked=$(cat $B/dst/sub/f* 2>/dev/null | grep -c SECRET)
  [ "$leaked" = 0 ] && [ $RC != 0 ] && grep -q "failed copy \`sub/f1\`" $LOG && ok T23-source-swap-no-leak || { ko T23-source-swap-no-leak "rc=$RC leaked=$leaked"; logtail 8; }
fi

# ---------- 리뷰 2: 복사 직전 목적지 이름이 FIFO 가 돼도 멈추지 않는다 ----------
if want T24; then B=$(fresh T24); mkdir -p $B/src; head -c 400000000 /dev/zero > $B/src/big; echo late > $B/src/late.txt
  ( waitbig $B/dst/big; mkfifo $B/dst/late.txt ) & TMO=120 ns $R11 $B/src $B/dst; wait
  [ $RC = 1 ] && [ -p $B/dst/late.txt ] && grep -q "failed copy \`late.txt\`" $LOG && ok T24-dst-fifo-no-hang || { ko T24-dst-fifo-no-hang "rc=$RC"; logtail 6; }
fi

# ---------- 리뷰 3: 소스를 못 읽은 COPY 는 기존 사본을 남긴다(비 root) ----------
if want T25; then B=$(fresh T25); mkdir -p $B/src $B/dst; echo NEW-CONTENT-LONGER > $B/src/f.txt; chmod 600 $B/src/f.txt
  echo OLD > $B/dst/f.txt; touch -d 2001-01-01 $B/dst/f.txt; echo new2 > $B/src/g.txt
  chown tuser:tuser $B/dst $B/dst/f.txt; chmod 755 $B $B/src
  ns_user tuser $R21 $B/src $B/dst
  [ $RC = 1 ] && [ "$(cat $B/dst/f.txt)" = OLD ] && [ "$(cat $B/dst/g.txt)" = new2 ] && grep -q "failed copy \`f.txt\`: Permission denied" $LOG \
    && ok T25-failed-copy-keeps-old || { ko T25-failed-copy-keeps-old "rc=$RC dst=$(cat $B/dst/f.txt 2>&1)"; logtail 6; }
fi

# ---------- 리뷰 5: set-ID 비트는 의도한 소유자에게만 ----------
if want T26; then B=$(fresh T26); mkdir -p $B/src $B/dst; cp /bin/true $B/src/suid; chmod 4755 $B/src/suid; chown tuser:tuser $B/dst; chmod 755 $B $B/src
  ns_user tuser $R21 $B/src $B/dst
  [ $RC = 0 ] && [ "$(stat -c '%a %U' $B/dst/suid)" = "755 tuser" ] && ok T26-setuid-dropped-owner-differs || { ko T26-setuid-dropped-owner-differs "rc=$RC $(stat -c '%a %U' $B/dst/suid)"; logtail 6; }
  B=$(fresh T26b); mkdir -p $B/src; cp /bin/true $B/src/suid; chown tuser:tuser $B/src/suid; chmod 4755 $B/src/suid; cp /bin/true $B/src/sgid; chmod 2755 $B/src/sgid
  ns $R21 $B/src $B/dst
  [ $RC = 0 ] && [ "$(stat -c '%a %U' $B/dst/suid)" = "4755 tuser" ] && [ "$(stat -c '%a %U' $B/dst/sgid)" = "2755 root" ] \
    && ok T26-setid-kept-for-owner || { ko T26-setid-kept-for-owner "rc=$RC $(stat -c '%a %U' $B/dst/suid $B/dst/sgid | tr '\n' ' ')"; logtail 6; }
fi

# ---------- 리뷰 4: dsync 남은 심링크 아래로 하위 항목을 복사하지 않는다(비 root) ----------
if want T27; then for np in 2 3; do B=$(fresh T27-$np); mkdir -p $B/src/P/X/sub $B/dst/P $B/out; echo a > $B/src/P/X/a; echo b > $B/src/P/X/sub/b
  ln -s $B/out $B/dst/P/X; chown -R tuser:tuser $B/src $B/out; chown tuser:tuser $B $B/dst; chmod 755 $B $B/dst/P
  LOG=$W/last.log; runuser -u tuser -- env HOME=/tmp timeout 300 mpirun -np $np $BIN/dsync $B/src $B/dst >$LOG 2>&1; RC=$?
  [ $RC != 0 ] && [ "$(find $B/out -mindepth 1 | wc -l)" = 0 ] && [ -L $B/dst/P/X ] && ok T27-dsync-no-copy-below-survivor-np$np \
    || { ko T27-dsync-no-copy-below-survivor-np$np "rc=$RC out=$(find $B/out -mindepth 1 | wc -l)"; logtail 8; }
done; fi

# ---------- 리뷰 4b: dsync --link-dest 도 남은 디렉터리에 메타를 찍지 않는다(root) ----------
if want T28; then B=$(fresh T28); mkdir -p $B/src $B/link $B/dst/X; echo data > $B/src/X; chmod 600 $B/src/X; touch -d 2001-02-03 $B/src/X
  cp -p $B/src/X $B/link/X; echo child > $B/dst/X/child; chmod 755 $B/dst/X; touch -d 2020-01-01 $B/dst/X
  ds 2 --link-dest $B/link $B/src $B/dst
  [ $RC != 0 ] && [ -d $B/dst/X ] && [ "$(stat -c %a $B/dst/X)" = 755 ] && [ "$(stat -c %Y $B/dst/X)" = "$(date -d 2020-01-01 +%s)" ] \
    && ok T28-dsync-linkdest-survivor-untouched || { ko T28-dsync-linkdest-survivor-untouched "rc=$RC $(stat -c '%a %y' $B/dst/X)"; logtail 8; }
fi

# ---------- 리뷰 7: 조기 중단에도 선처리 자리 디렉터리는 소스 메타를 갖는다 ----------
if want T29; then B=$(fresh T29); mkdir -p $B/src $B/dst
  for i in $(seq -w 1 40); do mkdir $B/src/x$i; echo k > $B/src/x$i/k; chown -R tuser:tuser $B/src/x$i; chmod 751 $B/src/x$i; echo stale > $B/dst/x$i
    echo f > $B/src/f$i; mkdir -p $B/dst/f$i; echo c > $B/dst/f$i/c; done
  ns $R12 --batch-files 20 --max-errors 1 $B/src $B/dst
  bad=$(for i in $(seq -w 1 40); do d=$B/dst/x$i; { [ -d $d ] && [ "$(stat -c '%a %U' $d)" = "751 tuser" ]; } || echo $i; done | wc -l)
  [ $RC = 1 ] && [ "$bad" = 0 ] && grep -q "Stopping after batch" $LOG && grep -q "stale entr" $LOG \
    && ok T29-prepass-meta-on-early-stop || { ko T29-prepass-meta-on-early-stop "rc=$RC bad=$bad"; grep -E "pre-pass|Stopping|Planned" $LOG; }
fi

# ---------- 리뷰 8: 배치 하나에서 --max-errors 는 rank 수 x N 이 아니다 ----------
if want T30; then B=$(fresh T30); mkdir -p $B/src $B/dst
  for i in $(seq -w 1 60); do echo f > $B/src/d$i; mkdir -p $B/dst/d$i; echo c > $B/dst/d$i/c; done
  ns $R13 --batch-files 1000000 --max-errors 5 $B/src $B/dst
  nf=$(sed -n 's/.*Failed items: \([0-9]*\).*/\1/p' $LOG)
  [ $RC = 1 ] && [ -n "$nf" ] && [ "$nf" -le 5 ] && grep -q "Not attempted after a stop" $LOG && ok T30-max-errors-total-not-per-rank || { ko T30-max-errors-total-not-per-rank "rc=$RC failed=$nf"; grep -E "Failed items|Not attempted" $LOG; }
fi

# ---------- 리뷰 9: 스캔 오류는 배치 안 예산을 쓰지 않는다(두 모드 같음) ----------
if want T31; then for bf in 0 1000000; do B=$(fresh T31-$bf); mkdir -p $B/src $B/dst; for i in 1 2 3 4 5; do mkdir $B/src/u$i; chmod 000 $B/src/u$i; done
  for i in $(seq -w 1 10); do echo $i > $B/src/f$i; done; chown tuser:tuser $B/src $B/src/f* $B/dst; chmod 755 $B
  ns_user tuser $R12 --batch-files $bf --max-errors 3 $B/src $B/dst
  [ $RC = 1 ] && [ "$(ls $B/dst | grep -c '^f')" = 10 ] && grep -q "Failed items: 5 (scan 5)" $LOG && ok T31-scan-errors-keep-budget-bf$bf \
    || { ko T31-scan-errors-keep-budget-bf$bf "rc=$RC files=$(ls $B/dst | grep -c '^f')"; grep -E "Failed items|Not attempted|Stopping" $LOG; }
  chmod 755 $B/src/u* 2>/dev/null; chmod -R u+rwx $B/dst 2>/dev/null
done; fi

# ---------- --delete: dst 전용 삭제가 배리어 뒤·복사 앞에 돌아도 결과가 같다 ----------
if want T32; then B=$(fresh T32); mkdir -p $B/src/keep $B/dst/gone/deep/er; for i in $(seq 1 300); do echo $i > $B/src/keep/k$i; echo $i > $B/dst/gone/deep/er/g$i; echo $i > $B/dst/old$i; done
  ns $R13 --delete $B/src $B/dst
  [ $RC = 0 ] && [ ! -e $B/dst/gone ] && [ "$(ls $B/dst | wc -l)" = 1 ] && [ "$(nfiles $B/dst/keep)" = 300 ] && ok T32-delete-dst-only-with-copies || { ko T32-delete-dst-only-with-copies "rc=$RC left=$(ls $B/dst | wc -l)"; logtail 6; }
fi

# ---------- 리뷰 2차: --delete 는 꽉 찬 목적지를 비울 수 있다(삭제가 복사보다 먼저, ENOSPC 정지 제외) ----------
if want T33; then S=$W/T33src; D=/dev/shm/T33dst; rm -rf $S $D; mkdir -p $S $D; head -c $((20*1024*1024)) /dev/urandom > $S/new.bin
  avail=$(df --output=avail -B1 /dev/shm | tail -1); fallocate -l $((avail - 5*1024*1024)) $D/old.bin
  ns $R11 --delete $S $D
  [ $RC = 0 ] && [ ! -e $D/old.bin ] && cmp -s $S/new.bin $D/new.bin && ok T33-delete-frees-full-destination || { ko T33-delete-frees-full-destination "rc=$RC dst=$(ls $D | tr '\n' ' ')"; logtail 6; }
  rm -rf $D $S
fi

# ---------- 리뷰 2차: 선처리 ENOSPC 로 아무것도 못 한 실행은 rc 0 이 아니다 ----------
if want T34; then cc -shared -fPIC -o $W/shim.so /work/tests/shim-enospc.c -ldl || ko T34-build "cc"
  for name in d1 d4 d5; do B=$(fresh T34-$name); mkdir -p $B/src/$name $B/dst $B/out
    for i in 1 2 3 4 5 6 7 8; do echo f$i > $B/src/$name/f$i; echo t$i > $B/src/top$i; done; ln -s $B/out $B/dst/$name
    LOG=$W/last.log; timeout 120 mpirun -np 2 -x LD_PRELOAD=$W/shim.so -x SHIM_ENOSPC_NAME=$name $BIN/nsync --role-mode map --role-map $R11 \
      --batch-files 4 $B/src $B/dst >$LOG 2>&1; RC=$?
    [ $RC = 1 ] && grep -q "failed type-change \`$name\`: No space left" $LOG && [ "$(nfiles $B/out)" = 0 ] && ok T34-prepass-enospc-rc1-$name \
      || { ko T34-prepass-enospc-rc1-$name "rc=$RC"; grep -E "pre-pass|Stopping|Not attempted|Failed items|Execution" $LOG; }
  done
fi

# ---------- --open-noatime: 소스 atime 보존(소유자·root), 남의 파일은 그냥 읽어 복사 ----------
atime_of() { stat -c %X "$1"; }
mk_at() { mkdir -p $1/src $1/dst; for i in 1 2 3; do echo a$i > $1/src/f$i; done; touch -a -d 2001-01-01 $1/src/f*; chmod 755 $1 $1/src; }
OLD=$(date -d 2001-01-01 +%s)
if want T35; then
  B=$(fresh T35a); mk_at $B; ns $R11 $B/src $B/dst
  [ $RC = 0 ] && [ "$(atime_of $B/src/f1)" != "$OLD" ] && ok T35-control-atime-updated || { ko T35-control-atime-updated "rc=$RC atime=$(atime_of $B/src/f1)"; }
  B=$(fresh T35b); mk_at $B; ns $R11 --open-noatime $B/src $B/dst
  [ $RC = 0 ] && [ "$(atime_of $B/src/f1)" = "$OLD" ] && [ "$(nfiles $B/dst)" = 3 ] && grep -q "open-noatime=1" $LOG && ok T35-root-keeps-atime || { ko T35-root-keeps-atime "rc=$RC atime=$(atime_of $B/src/f1)"; logtail 4; }
  B=$(fresh T35c); mk_at $B; chown -R tuser:tuser $B; touch -a -d 2001-01-01 $B/src/f*; ns_user tuser $R11 --open-noatime $B/src $B/dst
  [ $RC = 0 ] && [ "$(atime_of $B/src/f1)" = "$OLD" ] && ok T35-owner-keeps-atime || { ko T35-owner-keeps-atime "rc=$RC atime=$(atime_of $B/src/f1)"; logtail 4; }
  B=$(fresh T35d); mk_at $B; chown tuser $B $B/dst; ns_user tuser $R11 --open-noatime $B/src $B/dst
  [ $RC = 0 ] && [ "$(nfiles $B/dst)" = 3 ] && grep -q "3 source file(s) not owned by this run were read without O_NOATIME" $LOG \
    && ok T35-not-owner-still-copies || { ko T35-not-owner-still-copies "rc=$RC files=$(nfiles $B/dst)"; logtail 6; }
fi

# ---------- --direct: nsync 에서는 받기만 하고 쓰지 않는다(정렬 안 된 크기도 그대로 복사) ----------
if want T36; then B=$(fresh T36); mkdir -p $B/src; head -c 12345 /dev/urandom > $B/src/odd; head -c 1048577 /dev/urandom > $B/src/big; echo t > $B/src/tiny
  ns $R21 --direct $B/src $B/dst
  [ $RC = 0 ] && [ "$(treesum $B/src)" = "$(treesum $B/dst)" ] && grep -q "\-\-direct has no effect in nsync" $LOG && ok T36-direct-is-accepted-noop || { ko T36-direct-is-accepted-noop "rc=$RC"; logtail 6; }
fi

# ---------- dsync --open-noatime: root 는 atime 보존, 비 root 의 남의 파일은 그냥 읽어 복사 ----------
if want T37; then
  B=$(fresh T37a); mk_at $B; ds 2 --open-noatime $B/src $B/dst
  [ $RC = 0 ] && [ "$(atime_of $B/src/f1)" = "$OLD" ] && [ "$(treesum $B/src)" = "$(treesum $B/dst)" ] && ok T37-dsync-root-keeps-atime || { ko T37-dsync-root-keeps-atime "rc=$RC"; logtail 4; }
  B=$(fresh T37b); mk_at $B; chown tuser $B $B/dst
  LOG=$W/last.log; runuser -u tuser -- env HOME=/tmp timeout 300 mpirun -np 2 $BIN/dsync --open-noatime $B/src $B/dst >$LOG 2>&1; RC=$?
  [ "$(treesum $B/src)" = "$(treesum $B/dst)" ] && ! grep -q "Failed to open input file" $LOG && ok T37-dsync-not-owner-still-copies || { ko T37-dsync-not-owner-still-copies "rc=$RC"; grep -m3 -E "ERROR" $LOG; }
fi

# ---------- dsync --contents --open-noatime: 남의 파일 비교도 O_NOATIME 없이 다시 연다(mfu_compare_contents) ----------
if want T38; then B=$(fresh T38); mkdir -p $B/src $B/dst; for i in 1 2 3; do echo "new $i" > $B/src/f$i; echo "old $i" > $B/dst/f$i; done
  chmod 755 $B $B/src; chmod 644 $B/src/*; chown tuser $B $B/dst $B/dst/*
  LOG=$W/last.log; runuser -u tuser -- env HOME=/tmp timeout 300 mpirun -np 2 $BIN/dsync --contents --open-noatime $B/src $B/dst >$LOG 2>&1; RC=$?
  [ "$(cat $B/dst/f2)" = "new 2" ] && ! grep -q "Failed to open source file\|Failed to open destination file" $LOG \
    && ok T38-dsync-contents-noatime-not-owner || { ko T38-dsync-contents-noatime-not-owner "rc=$RC dst=$(cat $B/dst/f2)"; grep -m3 ERROR $LOG; }
fi

# ---------- drm·dsync --delete: 지우지 못한 항목이 있으면 실패, "Removed N" 은 지운 수 ----------
dr_user() { local u=$1; shift; LOG=$W/last.log; runuser -u "$u" -- env HOME=/tmp timeout ${TMO:-300} mpirun -np 2 "$BIN/drm" "$@" >"$LOG" 2>&1; RC=$?; }
if want T39; then B=$(fresh T39); mkdir -p $B/t/mine $B/t/root; for i in 1 2 3; do echo m$i > $B/t/mine/m$i; echo r$i > $B/t/root/r$i; done
  chown -R tuser:tuser $B/t; chown root:root $B/t/root; chmod 755 $B $B/t/root
  dr_user tuser $B/t
  # 9 항목(t, mine, root, m1-3, r1-3): mine·m1-3 은 지워지고(4), root 아래 r1-3 은 unlink 거부(3), root·t 는 비지 않아 rmdir 실패(2)
  [ $RC != 0 ] && [ ! -e $B/t/mine ] && [ "$(nfiles $B/t/root)" = 3 ] && grep -q "Removed 4 items in" $LOG && grep -q "Remove failed for 5 of 9 items" $LOG \
    && ok T39-drm-partial-fails || { ko T39-drm-partial-fails "rc=$RC"; grep -E "Removed|Remove failed" $LOG; }
  B=$(fresh T39b); mkdir -p $B/t/a; echo x > $B/t/a/x; dr_user root $B/t
  [ $RC = 0 ] && [ ! -e $B/t ] && grep -q "Removed 3 items in" $LOG && ! grep -q "Remove failed" $LOG && ok T39-drm-all-removed || { ko T39-drm-all-removed "rc=$RC"; logtail 4; }
fi
if want T40; then B=$(fresh T40); mkdir -p $B/src $B/dst/extra; echo k > $B/src/k; echo e > $B/dst/extra/e; chown -R tuser $B; chown root $B/dst/extra; chmod 755 $B $B/dst/extra
  LOG=$W/last.log; runuser -u tuser -- env HOME=/tmp timeout 300 mpirun -np 2 $BIN/dsync --delete $B/src $B/dst >$LOG 2>&1; RC=$?
  [ $RC != 0 ] && [ -f $B/dst/k ] && [ -f $B/dst/extra/e ] && grep -q "Remove failed for 2 of 2 items" $LOG && ok T40-dsync-delete-leftover-fails || { ko T40-dsync-delete-leftover-fails "rc=$RC"; grep -E "Removed|Remove failed|ERROR" $LOG | head -5; }
fi

if want T42; then B=$(fresh T42); mkdir -p $B/p/src; echo a > $B/p/src/a; chown -R tuser $B/p/src; chmod 755 $B $B/p
  LOG=$W/last.log; runuser -u tuser -- env HOME=/tmp timeout 300 mpirun -np 2 $BIN/dsync $B/p/src $B/p/dst >$LOG 2>&1; RC=$?
  [ $RC != 0 ] && [ ! -e $B/p/dst ] && grep -q "Destination parent directory is not writable" $LOG && ok T42-dsync-parent-not-writable-fails || { ko T42-dsync-parent-not-writable-fails "rc=$RC"; logtail 4; }
fi
if want T43; then B=$(fresh T43); mkdir -p $B/src $B/dst; echo a > $B/src/a; mkfifo $B/src/fifo; ds 2 $B/src $B/dst
  [ $RC = 0 ] && [ -f $B/dst/a ] && [ ! -e $B/dst/fifo ] && grep -q "Skipped 1 special file(s) in the source" $LOG && ! grep -q "Failed to change" $LOG \
    && ok T43-dsync-special-skipped-quietly || { ko T43-dsync-special-skipped-quietly "rc=$RC"; grep -E "Skipped|ERROR" $LOG | head -4; }
fi
# ---------- nsync "Completed actions": 계획이 아니라 실제로 된 수 ----------
if want T41; then B=$(fresh T41); mkdir -p $B/src/d $B/dst/gone; for i in 1 2 3; do echo s$i > $B/src/d/f$i; done; ln -s f1 $B/src/d/l; echo old > $B/dst/gone/o
  ns $R21 --delete $B/src $B/dst
  [ $RC = 0 ] && grep -q "Completed actions: copy=3 mkdir=1 symlink-update=1 meta-update=[0-9]* remove=2 copied-bytes=9$" $LOG && ok T41-completed-all || { ko T41-completed-all "rc=$RC"; grep -E "Planned|Completed" $LOG; }
  ns $R21 --dryrun $B/src $B/dst2
  ! grep -q "Completed actions" $LOG && ok T41-dryrun-no-completed || { ko T41-dryrun-no-completed "rc=$RC"; }
  B=$(fresh T41c); mk_fail $B; ns_user tuser $R21 --batch-files 1 $B/src $B/dst
  [ $RC != 0 ] && grep -q "Planned actions: copy=150 " $LOG && grep -q "Completed actions: copy=100 " $LOG && ok T41-completed-excludes-failed || { ko T41-completed-excludes-failed "rc=$RC"; grep -E "Planned|Completed" $LOG; }
fi

echo "==== PASS=$PASS FAIL=$FAIL ${FAILED[*]}"
