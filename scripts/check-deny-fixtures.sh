#!/usr/bin/env bash
# Negative test for the cargo-deny policy (FOUND-04): each fixture under
# ci/deny-fixtures/ must be REJECTED by the root deny.toml for the expected reason.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

failures=0

# check_fixture <name> <deny check kind> <required regex> [<required regex>...]
check_fixture() {
  local name="$1" kind="$2"
  shift 2
  local manifest="ci/deny-fixtures/${name}/Cargo.toml"
  local output code=0
  output="$(cargo deny --manifest-path "$manifest" --config deny.toml check "$kind" 2>&1)" || code=$?

  if [[ "$code" -eq 0 ]]; then
    echo "FAIL: ${name}: cargo deny check ${kind} accepted the fixture (exit 0)"
    failures=$((failures + 1))
    return
  fi

  local pattern
  for pattern in "$@"; do
    if ! grep -Eq -- "$pattern" <<<"$output"; then
      echo "FAIL: ${name}: rejected (exit ${code}) but output did not match /${pattern}/"
      echo "$output" | sed 's/^/    /'
      failures=$((failures + 1))
      return
    fi
  done

  echo "PASS: ${name}: rejected by cargo deny check ${kind} (exit ${code})"
}

check_fixture dup-prost bans 'duplicate' "prost"
check_fixture bad-licence licenses 'GPL-3\.0-only' 'rejected|not explicitly allowed'

if [[ "$failures" -ne 0 ]]; then
  echo "${failures} deny fixture check(s) failed"
  exit 1
fi
echo "All deny fixtures rejected as expected"
