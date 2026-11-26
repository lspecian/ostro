#!/usr/bin/env bash
# Phase 13 grep gates (13-20), shared by scripts/ci-local.sh (check_p13_gates, step "P13 grep
# gates") and the "Phase 13 grep gates" step of the lint job in .github/workflows/ci.yml.
# They add to scripts/p12-grep-gates.sh and loosen none of it:
#   1. ENG-10 (13-07): no hard-coded `journal@0.1.0"` literal in the ostro-engine host outside
#      the constant table. Interface names derive from WIT_0_1_LATEST through ostro_iface;
#      a file's `#[cfg(test)]` module (from the first column-0 `#[cfg(test)]` on) and comment
#      lines are not production code and are not scanned.
#   2. No Chronik text in the vendored raft driver (13-05). The driver test that asserts the
#      remedy text does NOT contain it (`!<x>.contains("CHRONIK")`) is the only allowed hit.
#   3. A joiner never forms a cluster (13-09): no InitCluster in crates/ostro-server/src/p13/joiner.rs.
#   4. The clock gate over the Phase 13 server modules (crates/ostro-server/src/route and
#      crates/ostro-server/src/p13): time only through the injected Clock. Comment lines are
#      not scanned; a line opts out with a `clock-exempt:` comment on the line itself or in the
#      comment block directly above it. (crates/ostro-cluster/src, which holds compat, upgrade,
#      handoff, shutdown, membership and placement, is already covered by the Phase 12 gate,
#      as is SetPartitionLeader for all of crates/ostro-server/src.)
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

# prod_lines <file...>: "file:line:text" for every line before the file's first column-0
# `#[cfg(test)]`, comment lines (`//`, `//!`, `///`) dropped.
prod_lines() {
  awk '
    FNR == 1 { in_test = 0 }
    /^#\[cfg\(test\)\]/ { in_test = 1 }
    in_test { next }
    $0 ~ /^[[:space:]]*\/\// { next }
    { print FILENAME ":" FNR ":" $0 }
  ' "$@"
}

# clock_hits <dir...>: wall-clock calls not covered by a clock-exempt comment (same line, or
# the comment block directly above).
clock_hits() {
  local files
  mapfile -t files < <(find "$@" -name '*.rs' -type f | sort)
  [[ ${#files[@]} -eq 0 ]] && { echo "clock gate: no files under $*" >&2; return 2; }
  awk '
    FNR == 1 { exempt = 0 }
    $0 ~ /^[[:space:]]*\/\// { if ($0 ~ /clock-exempt:/) exempt = 1; next }
    {
      if ($0 ~ /SystemTime::now\(|SystemClock([^A-Za-z0-9_]|$)|Instant::now\(/ && !exempt && $0 !~ /clock-exempt:/)
        print FILENAME ":" FNR ":" $0
      exempt = 0
    }
  ' "${files[@]}"
}

check_p13_gates() {
  local hits rc=0 host_files
  mapfile -t host_files < <(find crates/ostro-engine/src/host -name '*.rs' -type f | sort)
  [[ ${#host_files[@]} -gt 0 ]] || { echo "no files under crates/ostro-engine/src/host"; return 1; }
  hits=$(prod_lines "${host_files[@]}" | grep -F 'journal@0.1.0"' || true)
  if [ -n "$hits" ]; then echo "hard-coded journal@0.1.0 interface name in the engine host (use ostro_iface / WIT_0_1_LATEST):"; echo "$hits"; rc=1; fi

  hits=$(gate_hits -rn "CHRONIK" vendor/ostro-raft-driver/src) || return 1
  hits=$(grep -vE '![[:space:]]*[A-Za-z_][A-Za-z0-9_]*\.contains\("CHRONIK"\)' <<<"$hits" || true)
  if [ -n "$hits" ]; then echo "Chronik text in the vendored raft driver:"; echo "$hits"; rc=1; fi

  [[ -f crates/ostro-server/src/p13/joiner.rs ]] || { echo "crates/ostro-server/src/p13/joiner.rs is missing"; return 1; }
  hits=$(gate_hits -n "InitCluster" crates/ostro-server/src/p13/joiner.rs) || return 1
  if [ -n "$hits" ]; then echo "InitCluster reachable from the joiner (a joiner never forms a cluster):"; echo "$hits"; rc=1; fi

  hits=$(clock_hits crates/ostro-server/src/route crates/ostro-server/src/p13) || return 1
  if [ -n "$hits" ]; then echo "wall clock outside the injected Clock (P13 clock gate):"; echo "$hits"; rc=1; fi
  echo "INFO: P13 clock gate scanned crates/ostro-server/src/route and crates/ostro-server/src/p13 ($(grep -rlE 'clock-exempt:' crates/ostro-server/src/route crates/ostro-server/src/p13 | wc -l) files with clock-exempt lines)"
  return $rc
}

check_p13_gates
