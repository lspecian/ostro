#!/usr/bin/env bash
# Forbidden-crate check shared by scripts/ci-local.sh and .github/workflows/ci.yml (P8-D17).
#
# Usage: bash scripts/forbidden-crates.sh             # check `cargo tree --workspace -e normal`
#        bash scripts/forbidden-crates.sh --self-test # check the matcher against fixture lines
#
# The match is anchored on the crate name: a name starts at the beginning of the line or after
# whitespace / a tree-drawing character, and is followed by " v<digit>". So
# `metrics-exporter-prometheus v0.18.3` passes while `prometheus v0.14.0` fails. The
# `prometheus` crate is also banned by exact name in deny.toml (ci/deny-fixtures/ban-prometheus).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FORBIDDEN_RE='(^|[[:space:]─])(chronik[A-Za-z0-9_-]*|tokio-uring|prometheus) v[0-9]'

# forbidden_hits: reads `cargo tree` lines on stdin, prints the forbidden ones (none = empty).
forbidden_hits() {
  grep -E "$FORBIDDEN_RE" || true
}

self_test() {
  local failures=0 line hits
  local -a pass=(
    '├── metrics-exporter-prometheus v0.18.3'
    '│   │   └── metrics-exporter-prometheus v0.18.3 (*)'
    'metrics-exporter-prometheus v0.18.3'
    '├── tokio v1.54.0'
  )
  local -a fail=(
    '├── prometheus v0.13.4'
    'prometheus v0.14.0'
    '│   └── chronik-storage v0.1.0'
    '└── tokio-uring v0.5.0'
  )
  for line in "${pass[@]}"; do
    hits="$(printf '%s\n' "$line" | forbidden_hits)"
    if [[ -n "$hits" ]]; then
      echo "self-test FAIL: matched an allowed line: $line"
      failures=$((failures + 1))
    fi
  done
  for line in "${fail[@]}"; do
    hits="$(printf '%s\n' "$line" | forbidden_hits)"
    if [[ -z "$hits" ]]; then
      echo "self-test FAIL: missed a forbidden line: $line"
      failures=$((failures + 1))
    fi
  done
  if [[ "$failures" -ne 0 ]]; then
    echo "forbidden-crates self-test: ${failures} failure(s)"
    return 1
  fi
  echo "forbidden-crates self-test: ${#pass[@]} allowed and ${#fail[@]} forbidden fixtures OK"
}

check_tree() {
  local tree hits
  # Capture first, so a cargo failure fails the check instead of being masked by the pipe.
  tree="$(cargo tree --workspace -e normal --locked)"
  hits="$(printf '%s\n' "$tree" | forbidden_hits)"
  if [[ -n "$hits" ]]; then
    echo "forbidden crates in the dependency tree:"
    echo "$hits"
    return 1
  fi
  echo "no chronik / tokio-uring / prometheus in cargo tree"
}

case "${1:-}" in
  --self-test) self_test ;;
  "") check_tree ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac
