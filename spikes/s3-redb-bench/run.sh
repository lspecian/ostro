#!/usr/bin/env bash
# Spike S3 runner: builds the bench, captures the environment, runs every redb
# durability tier N times and the kill -9 crash sanity test x5.
#
# Usage: run.sh [--quick] [DB_DIR]
#   DB_DIR   directory for database files (default ${TMPDIR:-/tmp}/ostro-s3).
#            Must be on the Linux filesystem; paths under /mnt/ are refused.
#   --quick  smoke mode: 20k commands / 2k warm-up, 1 rep, 1 crash test,
#            output to docs/spikes/data/S3/quick.jsonl (not committed).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA="$REPO_ROOT/docs/spikes/data/S3"

QUICK=0
DB_DIR=""
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
    *) DB_DIR="$arg" ;;
  esac
done
DB_DIR="${DB_DIR:-${TMPDIR:-/tmp}/ostro-s3}"
case "$DB_DIR" in
  /mnt/*) echo "refusing DB_DIR under /mnt (WSL2 9p distorts fsync): $DB_DIR" >&2; exit 2 ;;
esac
case "$(realpath -m "$DB_DIR")/" in
  "$REPO_ROOT"/*) echo "refusing DB_DIR inside the repo: $DB_DIR" >&2; exit 2 ;;
esac
mkdir -p "$DB_DIR" "$DATA"
FSTYPE="$(df --output=fstype "$DB_DIR" | tail -1 | tr -d ' ')"
case "$FSTYPE" in
  tmpfs|ramfs|9p|drvfs) echo "refusing DB_DIR on $FSTYPE (fsync would be meaningless or distorted): $DB_DIR" >&2; exit 2 ;;
esac

(cd "$SCRIPT_DIR" && cargo +1.100.0 build --release)
BIN="$SCRIPT_DIR/target/release/s3-redb-bench"

TIERS=(t1 t2 t3 t2-2pc t2-qr)
if [[ $QUICK -eq 1 ]]; then
  COMMANDS=20000; WARMUP=2000; REPS=1; CRASH_RUNS=1
  RESULTS="$DATA/quick.jsonl"; CRASH="$DATA/quick.jsonl"; ENV_OUT="$DATA/quick-env.txt"
  : > "$RESULTS"
else
  COMMANDS=200000; WARMUP=20000; REPS=3; CRASH_RUNS=5
  RESULTS="$DATA/results.jsonl"; CRASH="$DATA/crash.jsonl"; ENV_OUT="$DATA/env.txt"
  : > "$RESULTS"; : > "$CRASH"
fi

{
  echo "# S3 environment captured $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "## uname -a"; uname -a
  echo "## /proc/version"; cat /proc/version
  echo "## WSL"; (grep -qi microsoft /proc/version && echo "WSL2 detected (ext4 on VHDX; fsync semantics pass through Hyper-V virtual disk)") || echo "not WSL"
  echo "## nproc"; nproc
  echo "## cpu"; grep -m1 'model name' /proc/cpuinfo || true
  echo "## free -m"; free -m
  echo "## db dir"; echo "$DB_DIR"
  echo "## df -T"; df -T "$DB_DIR"
  echo "## findmnt"; findmnt -T "$DB_DIR" || true
  echo "## lsblk"; lsblk -o NAME,SIZE,TYPE,ROTA,MOUNTPOINTS 2>/dev/null || lsblk || true
  echo "## rustc"; rustc +1.100.0 -V
  echo "## loadavg at start"; cat /proc/loadavg
} > "$ENV_OUT"

for rep in $(seq 1 "$REPS"); do
  for t in "${TIERS[@]}"; do
    echo "== tier $t rep $rep (load: $(cut -d' ' -f1-3 /proc/loadavg))" >&2
    "$BIN" bench --tier "$t" --dir "$DB_DIR" --commands "$COMMANDS" --warmup "$WARMUP" \
      --batch 64 --checkpoint-ms 100 --checkpoint-batches 256 --keys random \
      | sed "s/^{/{\"rep\":$rep,/" >> "$RESULTS"
  done
done

for i in $(seq 1 "$CRASH_RUNS"); do
  echo "== crash-test $i" >&2
  "$BIN" crash-test --dir "$DB_DIR" --after-ms 3000 --tier t2 --run "$i" >> "$CRASH"
done

echo "## loadavg at end" >> "$ENV_OUT"; cat /proc/loadavg >> "$ENV_OUT"
echo "done: $RESULTS $CRASH" >&2
