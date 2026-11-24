#!/usr/bin/env bash
# Phase 12 grep gates, shared by scripts/ci-local.sh ("one log path + P12 grep gates") and the
# "Phase 12 grep gates" step of .github/workflows/ci.yml.
#
# 12-08 (D5 / docs/04 §4 rule 1): one partition log implementation. The former RF=1 type
# name must not come back, and neither the log crate nor the cluster crate may branch on the
# replication factor (RF=1 is a replica set of one, not a second path).
# 12-18 adds: no SetPartitionLeader proposal in ostro-server / ostro-cluster (comments
# allowed), no legacy self-leader path in ostro-partition, the Phase 12 clock gate (time only
# through the injected Clock; a line may opt out with a `clock-exempt:` comment), and no
# bincode / object_store in Cargo.lock (chrono and bincode are also banned for normal
# dependencies by deny.toml).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# gate_hits <grep args...>: grep's matches; "no match" (status 1) is empty, errors fail.
gate_hits() {
  local out rc=0
  out="$(grep "$@")" || rc=$?
  if [[ $rc -gt 1 ]]; then echo "grep $* failed ($rc)" >&2; return 2; fi
  printf '%s' "$out"
}

check_one_log_path() {
  local hits rc=0
  hits=$(gate_hits -rn "Rf1Log" crates/) || return 1
  if [ -n "$hits" ]; then echo "Rf1Log is gone (use PartitionLog):"; echo "$hits"; rc=1; fi
  hits=$(gate_hits -rnE "rf *== *1|replicas\.len\(\) *== *1" crates/ostro-log/src crates/ostro-cluster/src) || return 1
  if [ -n "$hits" ]; then echo "RF-specific branch in ostro-log / ostro-cluster:"; echo "$hits"; rc=1; fi
  hits=$(gate_hits -rn "SetPartitionLeader" crates/ostro-server/src crates/ostro-cluster/src) || return 1
  hits=$(grep -vE '^[^:]+:[0-9]+: *//' <<<"$hits" || true)
  if [ -n "$hits" ]; then echo "SetPartitionLeader proposal (Phase 12: ElectLeader only):"; echo "$hits"; rc=1; fi
  hits=$(gate_hits -rn "legacy path" crates/ostro-partition/src) || return 1
  if [ -n "$hits" ]; then echo "legacy self-leader path in ostro-partition:"; echo "$hits"; rc=1; fi
  hits=$(gate_hits -rnE "SystemTime::now\(|SystemClock\b|Instant::now\(" crates/ostro-cluster/src \
    crates/ostro-partition/src/roles.rs crates/ostro-partition/src/replica_services.rs \
    crates/ostro-log/src/partition_log.rs crates/ostro-log/src/replica crates/ostro-server/src/cluster.rs \
    crates/ostro-backup/src/cluster.rs) || return 1
  hits=$(grep -v "clock-exempt:" <<<"$hits" || true)
  if [ -n "$hits" ]; then echo "wall clock outside the injected Clock (clock gate):"; echo "$hits"; rc=1; fi
  # chrono and bincode are banned for normal dependencies by deny.toml (cargo deny step);
  # bincode and object_store must not appear in the lock file at all.
  hits=$(gate_hits -nE '^name = "(bincode|object_store)"$' Cargo.lock) || return 1
  if [ -n "$hits" ]; then echo "forbidden crate in Cargo.lock:"; echo "$hits"; rc=1; fi
  return $rc
}

check_one_log_path
