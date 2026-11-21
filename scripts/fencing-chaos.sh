#!/usr/bin/env bash
# QA-03 fencing chaos campaign (plan 07-21, D-W11).
#
# Builds the failpoint-enabled release test binary once, runs every pending shard
# (resumable: a shard with a shard-NN.json in the state dir is skipped), then aggregates
# the shard files into <state dir>/report.json and report.txt.
#
#   OSTRO_CHAOS_RUNS=10000 OSTRO_CHAOS_SHARDS=20 OSTRO_CHAOS_PARALLEL=4 \
#       bash scripts/fencing-chaos.sh
#
# Knobs (passed through to crates/ostro/tests/fencing_chaos.rs):
#   OSTRO_CHAOS_RUNS (10000), OSTRO_CHAOS_SHARDS (20), OSTRO_CHAOS_PARALLEL (4),
#   OSTRO_CHAOS_STATE_DIR (target/fencing-chaos), OSTRO_CHAOS_SEED, OSTRO_CHAOS_SHARD_IDS,
#   OSTRO_CHAOS_SUBMIT_SECS (60). CARGO_TARGET_DIR defaults to target/failpoints.
#
# Exits non-zero when any shard fails or the report does not pass. To re-run a failed
# shard, delete its shard-NN.json (its evidence stays in shard-NN/ until overwritten).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target/failpoints}"
export OSTRO_CHAOS_RUNS="${OSTRO_CHAOS_RUNS:-10000}"
export OSTRO_CHAOS_SHARDS="${OSTRO_CHAOS_SHARDS:-20}"
export OSTRO_CHAOS_PARALLEL="${OSTRO_CHAOS_PARALLEL:-4}"
export OSTRO_CHAOS_STATE_DIR="${OSTRO_CHAOS_STATE_DIR:-$root/target/fencing-chaos}"
if [[ -n "${OSTRO_CHAOS_SEED:-}" ]]; then export OSTRO_CHAOS_SEED; fi
if [[ -n "${OSTRO_CHAOS_SHARD_IDS:-}" ]]; then export OSTRO_CHAOS_SHARD_IDS; fi
if [[ -n "${OSTRO_CHAOS_SUBMIT_SECS:-}" ]]; then export OSTRO_CHAOS_SUBMIT_SECS; fi

test_cmd=(cargo test --release -p ostro --features failpoints --test fencing_chaos --locked)

echo "fencing-chaos: building (CARGO_TARGET_DIR=$CARGO_TARGET_DIR)"
"${test_cmd[@]}" --no-run

mkdir -p "$OSTRO_CHAOS_STATE_DIR"
echo "fencing-chaos: runs=$OSTRO_CHAOS_RUNS shards=$OSTRO_CHAOS_SHARDS parallel=$OSTRO_CHAOS_PARALLEL state=$OSTRO_CHAOS_STATE_DIR"

campaign_rc=0
"${test_cmd[@]}" -- --ignored --nocapture --exact campaign || campaign_rc=$?

report_rc=0
"${test_cmd[@]}" -- --ignored --nocapture --exact report || report_rc=$?

if [[ -s "$OSTRO_CHAOS_STATE_DIR/report.txt" ]]; then
    cat "$OSTRO_CHAOS_STATE_DIR/report.txt"
fi
if (( campaign_rc != 0 || report_rc != 0 )); then
    echo "fencing-chaos: FAIL (campaign exit $campaign_rc, report exit $report_rc)" >&2
    exit 1
fi
echo "fencing-chaos: PASS ($OSTRO_CHAOS_STATE_DIR/report.json)"
