#!/usr/bin/env bash
# Spike S3 quiet-machine confirmation (plan 01-08).
#
# Usage: confirm.sh [DB_DIR]
#   Builds with --features fjall and writes:
#   docs/spikes/data/S3/confirm.jsonl        3 interleaved reps of
#       {redb t2, fjall t2, redb t2 --rmw, fjall t2 --rmw}  (200k cmds, 20k warm-up,
#       B=64, checkpoint 100 ms / 256 batches; --rmw adds --preload 1000000)
#   docs/spikes/data/S3/crash-confirm.jsonl  5 kill -9 crash tests per engine (t2)
# Before each decision-critical run it waits (max 120 s) for the 1-min load to drop
# below 2.0, and records the load either way.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA="$REPO_ROOT/docs/spikes/data/S3"
DB_DIR="${1:-${TMPDIR:-/tmp}/ostro-s3}"
case "$DB_DIR" in
  /mnt/*) echo "refusing DB_DIR under /mnt: $DB_DIR" >&2; exit 2 ;;
esac
case "$(realpath -m "$DB_DIR")/" in
  "$REPO_ROOT"/*) echo "refusing DB_DIR inside the repo: $DB_DIR" >&2; exit 2 ;;
esac
mkdir -p "$DB_DIR" "$DATA"
FSTYPE="$(df --output=fstype "$DB_DIR" | tail -1 | tr -d ' ')"
case "$FSTYPE" in
  tmpfs|ramfs|9p|drvfs) echo "refusing DB_DIR on $FSTYPE: $DB_DIR" >&2; exit 2 ;;
esac

(cd "$SCRIPT_DIR" && cargo +1.100.0 build --release --features fjall)
BIN="$SCRIPT_DIR/target/release/s3-redb-bench"
OUT="$DATA/confirm.jsonl"; CRASH="$DATA/crash-confirm.jsonl"
: > "$OUT"; : > "$CRASH"

wait_quiet() {
  local i=0
  while awk '{exit !($1 >= 2.0)}' /proc/loadavg && [[ $i -lt 24 ]]; do
    sleep 5; i=$((i + 1))
  done
}

echo "## confirm run (01-08) $(date -u +%Y-%m-%dT%H:%M:%SZ) loadavg $(cat /proc/loadavg) db $DB_DIR ($FSTYPE)" >> "$DATA/env.txt"
COMMON=(--tier t2 --dir "$DB_DIR" --commands 200000 --warmup 20000 --batch 64
        --checkpoint-ms 100 --checkpoint-batches 256 --keys random)
for rep in 1 2 3; do
  for spec in "redb plain" "fjall plain" "redb rmw" "fjall rmw"; do
    set -- $spec
    extra=()
    [[ $2 == rmw ]] && extra=(--rmw --new-ratio 0.25 --preload 1000000)
    wait_quiet
    echo "== $1 t2 $2 rep $rep (load: $(cut -d' ' -f1-3 /proc/loadavg))" >&2
    "$BIN" bench --engine "$1" "${COMMON[@]}" "${extra[@]}" \
      | jq -c --argjson rep "$rep" --arg wl "$2" '{rep: $rep, workload: $wl} + .' >> "$OUT"
  done
done

for engine in redb fjall; do
  for i in 1 2 3 4 5; do
    wait_quiet
    echo "== crash-test $engine $i" >&2
    "$BIN" crash-test --engine "$engine" --dir "$DB_DIR" --after-ms 3000 --tier t2 --run "$i" >> "$CRASH"
  done
done
echo "## confirm end $(date -u +%Y-%m-%dT%H:%M:%SZ) loadavg $(cat /proc/loadavg)" >> "$DATA/env.txt"
echo "done: $OUT $CRASH" >&2
