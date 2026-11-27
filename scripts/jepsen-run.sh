#!/usr/bin/env bash
# QA-05 Jepsen-style runs (14-22, D-P14-19/20): reproducible entry point.
#
#   scripts/jepsen-run.sh smoke [--seed S] [--secs N]       # default seed 1401, 180 s
#   scripts/jepsen-run.sh gate  [--seed S] [--secs N]       # seeds 1402,1403 x 1800 s
#   scripts/jepsen-run.sh long  [--seed S] [--secs N]       # one seed (1405) x 4 h
#   scripts/jepsen-run.sh <mode> --hosts h1,h2,h3           # real machines: prints the
#                                                           # procedure, runs nothing
#
# Every run starts 3 `ostro server` processes (RF 3) on 127.r.r.n loopback aliases behind
# the Rust chaos proxy with `[auth] mode = "token"` (bootstrap admin bearer from
# OSTRO_BOOTSTRAP_TOKEN = OSTRO_JEPSEN_TOKEN, or a fresh one) and `[audit] enabled = true`,
# and injects partitions, kill -9, clock skew and disk stalls (crates/ostro-jepsen,
# tests/jepsen_smoke.rs and tests/jepsen_gate.rs). Reports: target/jepsen/<seed>/
# (OSTRO_JEPSEN_OUT overrides the root); the gate also writes target/jepsen/gate-summary.txt.
#
# Environment passed through: OSTRO_JEPSEN_SEED / OSTRO_JEPSEN_SEEDS / OSTRO_JEPSEN_SECS
# (the flags set them), OSTRO_JEPSEN_TOKEN, OSTRO_JEPSEN_OUT, OSTRO_PROC_ROOT (cluster
# data dirs; default /dev/shm: use a disk-backed dir for `long`), OSTRO_FAILPOINT_BIN
# (skip the build and use this binary), CARGO_BUILD_JOBS.
set -euo pipefail

usage() {
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

[ $# -ge 1 ] || usage
mode=$1
shift
seed=""
secs=""
hosts=""
while [ $# -gt 0 ]; do
  case "$1" in
    --seed) seed=${2:?--seed needs a value}; shift 2 ;;
    --secs) secs=${2:?--secs needs a value}; shift 2 ;;
    --hosts) hosts=${2:?--hosts needs a value}; shift 2 ;;
    -h|--help) usage ;;
    *) echo "jepsen-run: unknown argument $1" >&2; usage ;;
  esac
done

case "$mode" in
  smoke|gate|long) ;;
  *) echo "jepsen-run: unknown mode $mode (smoke|gate|long)" >&2; usage ;;
esac

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

if [ -n "$hosts" ]; then
  IFS=, read -r -a h <<<"$hosts"
  if [ "${#h[@]}" -ne 3 ]; then
    echo "jepsen-run: --hosts needs exactly three hosts (got ${#h[@]})" >&2
    exit 2
  fi
  cat <<EOF
human_needed: QA-05 on three real machines (${h[0]}, ${h[1]}, ${h[2]}) - not executed.

1. Build once: cargo build -p ostro --features failpoints --target-dir target/failpoints --locked --release
   and copy target/failpoints/release/ostro to /usr/local/bin/ostro on each host.
2. On each host i (1..3) write /etc/ostro/node.toml:
     [node]    node_id = i, data_dir = "/var/lib/ostro", partitions = 4
     [api]     listen = "<host i LAN ip>:8080"
     [cluster] listen = "<host i LAN ip>:7600", advertise = ["<host i LAN ip>"],
               cluster_id = "jepsen-<seed>", tls.ca_cert_file / tls.ca_key_file = the same CA
     [[cluster.peers]] one per other host: id = j, addr = "<fourth host>:<proxy port i->j>"
     [replication] rf = 3, min_insync = 2
     [auth]    mode = "token"      [audit] enabled = true
   and start: OSTRO_BOOTSTRAP_TOKEN=<bearer> FAILPOINTS= ostro server -c /etc/ostro/node.toml
3. On a fourth host run the chaos proxy with one listener per directed link i->j
   (6 links) forwarding to <host j>:7600, so every peer connection crosses it; run the
   workload, the nemeses (proxy rules, kill -9 / restart over ssh, the failpoints-only
   /v1/admin/test/{clock,failpoints} endpoints with the bearer) and the checkers from
   there. ostro-jepsen drives ProcCluster processes only; the remote driver is not
   implemented (14-22 runs on loopback: one machine, three processes).
4. Keep every node's /var/lib/ostro (logs + audit) after the run; verify each audit
   journal offline: ostro audit verify --data-dir /var/lib/ostro
EOF
  exit 0
fi

case "$mode" in
  smoke) test_name=jepsen_smoke; default_secs=180 ;;
  gate) test_name=jepsen_gate; default_secs=1800 ;;
  long) test_name=jepsen_gate; default_secs=14400 ;;
esac
export OSTRO_JEPSEN_SECS=${secs:-${OSTRO_JEPSEN_SECS:-$default_secs}}
case "$mode" in
  smoke)
    export OSTRO_JEPSEN_SEED=${seed:-${OSTRO_JEPSEN_SEED:-1401}}
    ;;
  gate)
    if [ -n "$seed" ]; then
      export OSTRO_JEPSEN_SEEDS=$seed
    else
      export OSTRO_JEPSEN_SEEDS=${OSTRO_JEPSEN_SEEDS:-1402,1403}
    fi
    ;;
  long)
    export OSTRO_JEPSEN_SEEDS=${seed:-${OSTRO_JEPSEN_SEED:-1405}}
    ;;
esac

if [ -z "${OSTRO_FAILPOINT_BIN:-}" ]; then
  cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
  export OSTRO_FAILPOINT_BIN=$root/target/failpoints/debug/ostro
fi
echo "jepsen-run: mode=$mode secs=$OSTRO_JEPSEN_SECS seed(s)=${OSTRO_JEPSEN_SEED:-${OSTRO_JEPSEN_SEEDS:-}}"
echo "jepsen-run: binary $OSTRO_FAILPOINT_BIN sha256 $(sha256sum "$OSTRO_FAILPOINT_BIN" | cut -d' ' -f1)"
echo "jepsen-run: HEAD $(git rev-parse HEAD 2>/dev/null || echo unknown)"
echo "jepsen-run: load at start: $(uptime); nproc $(nproc); $(free -m | awk '/^Mem:/ {print "free " $7 " MB of " $2 " MB"}')"
status=0
cargo test --release -p ostro-jepsen --locked --test "$test_name" \
  -- --ignored --nocapture --test-threads=1 || status=$?
echo "jepsen-run: load at end: $(uptime); $(free -m | awk '/^Mem:/ {print "free " $7 " MB of " $2 " MB"}')"
if [ "$mode" != smoke ]; then
  out=${OSTRO_JEPSEN_OUT:-$root/target/jepsen}
  echo "jepsen-run: gate summary $out/gate-summary.txt"
fi
exit "$status"
