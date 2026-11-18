#!/usr/bin/env bash
# Local CI gate: mirrors .github/workflows/ci.yml (lint job + linux-x86_64-musl leg).
#
# Usage: bash scripts/ci-local.sh [--no-musl] [--no-musl-test]
#   --no-musl       skip the cargo-zigbuild x86_64 musl leg entirely
#   --no-musl-test  build the musl release binary but skip `cargo-zigbuild test`
#
# Requires: rustup toolchain from rust-toolchain.toml, cargo-deny 0.21.0, jq,
# cargo-zigbuild 0.24.1 + zig 0.17.1 (on PATH or via `pip install ziglang`).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_MUSL=1
RUN_MUSL_TEST=1
for arg in "$@"; do
  case "$arg" in
    --no-musl) RUN_MUSL=0 ;;
    --no-musl-test) RUN_MUSL_TEST=0 ;;
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

MUSL_TARGET="x86_64-unknown-linux-musl"
# Crates with a `failpoints` feature whose fault-injection suites run in the
# "failpoint suites" step (single-threaded: the fail registry is global).
# Later Phase 2 plans append ostro-log, ostro-partition, ostro-blob.
FAILPOINT_CRATES=(ostro-wal ostro-blob)
WORKFLOW=".github/workflows/ci.yml"

declare -a NAMES=() RESULTS=() TIMES=()

record() { NAMES+=("$1"); RESULTS+=("$2"); TIMES+=("$3"); }

# step <name> <command...>: runs the command, records PASS/FAIL and duration.
step() {
  local name="$1"; shift
  echo
  echo "==> ${name}"
  local start=$SECONDS
  if "$@"; then
    record "$name" PASS "$((SECONDS - start))s"
  else
    record "$name" FAIL "$((SECONDS - start))s"
  fi
}

skip() {
  echo
  echo "==> $1 (SKIP: $2)"
  record "$1" SKIP "-"
}

check_toolchain() {
  local v
  v="$(rustc --version)"
  echo "$v"
  [[ "$v" == *"1.100.0"* ]] || { echo "expected rustc 1.100.0"; return 1; }
}

check_clippy() {
  cargo clippy --workspace --all-targets --locked \
    --exclude ostro-raft-rs --exclude ostro-raft-proto \
    --exclude ostro-raft-driver --exclude ostro-wal \
    -- -D warnings
}

check_build() {
  local out
  out="$(cargo build --workspace --locked 2>&1)" || { echo "$out"; return 1; }
  echo "$out" | tail -n 3
  # Warning-free build, including workspace-manifest warnings (unused pins).
  if grep -q '^warning' <<<"$out"; then
    echo "$out" | grep -A5 '^warning'
    echo "build produced warnings"
    return 1
  fi
}

check_failpoints() {
  local c
  for c in "${FAILPOINT_CRATES[@]}"; do
    # ostro-wal is vendored and excluded from workspace clippy; lint the rest.
    if [[ "$c" != "ostro-wal" ]]; then
      cargo clippy -p "$c" --features failpoints --all-targets --locked -- -D warnings || return 1
    fi
    cargo test -p "$c" --features failpoints --locked -- --test-threads=1 || return 1
  done
}

check_forbidden_crates() {
  local hits
  hits="$(cargo tree --workspace -e normal --locked | grep -E 'chronik|tokio-uring|prometheus ' || true)"
  if [[ -n "$hits" ]]; then
    echo "forbidden crates in the dependency tree:"
    echo "$hits"
    return 1
  fi
  echo "no chronik / tokio-uring / prometheus in cargo tree"
}

check_workflow() {
  if command -v actionlint >/dev/null 2>&1; then
    actionlint "$WORKFLOW"
  elif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    docker run --rm -v "$ROOT":/repo -w /repo rhysd/actionlint:latest "$WORKFLOW"
  else
    echo "actionlint unavailable (no binary, docker not running); YAML parse fallback"
    local py=(python3)
    if ! python3 -c 'import yaml' >/dev/null 2>&1; then
      command -v uv >/dev/null 2>&1 || { echo "need PyYAML or uv"; return 1; }
      py=(uv run --quiet --no-project --with pyyaml python3)
    fi
    "${py[@]}" - "$WORKFLOW" <<'PY'
import re, sys, yaml
path = sys.argv[1]
doc = yaml.safe_load(open(path))
assert isinstance(doc, dict) and "jobs" in doc, "workflow has no jobs"
bad = []
for job, spec in doc["jobs"].items():
    for st in spec.get("steps", []):
        uses = st.get("uses")
        if uses and not uses.startswith("./") and not re.search(r"@[0-9a-f]{40}$", uses):
            bad.append(f"{job}: {uses}")
if bad:
    sys.exit("actions not pinned to a 40-hex SHA:\n  " + "\n  ".join(bad))
print(f"{path}: YAML ok, {len(doc['jobs'])} jobs, all actions SHA-pinned")
PY
  fi
}

# cargo-zigbuild finds `python3 -m ziglang` on its own when zig is not on PATH.
check_zig() {
  if command -v zig >/dev/null 2>&1; then
    echo "zig $(zig version)"
  elif python3 -m ziglang version >/dev/null 2>&1; then
    echo "zig $(python3 -m ziglang version) (python3 -m ziglang)"
  else
    echo "zig not found (install zig 0.17.1 or: pip install --user ziglang==0.17.1)"
    return 1
  fi
  command -v cargo-zigbuild >/dev/null 2>&1 || { echo "cargo-zigbuild not found"; return 1; }
  cargo-zigbuild --version
}

check_musl_release() {
  cargo zigbuild --release --locked -p ostro --target "$MUSL_TARGET" || return 1
  local bin="target/${MUSL_TARGET}/release/ostro" desc info
  desc="$(file "$bin")"
  echo "$desc"
  if ! grep -Eq 'statically linked|static-pie linked' <<<"$desc"; then
    echo "binary is not statically linked"
    return 1
  fi
  info="$("$bin" build-info)" || return 1
  echo "$info"
  grep -q 'allocator=mimalloc' <<<"$info" || { echo "allocator is not mimalloc"; return 1; }
  grep -q "target=${MUSL_TARGET}" <<<"$info" || { echo "unexpected target"; return 1; }
}

check_musl_test() {
  # `cargo zigbuild test` is not a subcommand; the test runner is `cargo-zigbuild test`.
  cargo-zigbuild test --workspace --locked --target "$MUSL_TARGET"
}

step "toolchain 1.100.0" check_toolchain
step "cargo fmt --check" cargo fmt --all --check
step "cargo clippy -D warnings" check_clippy
step "cargo build (warning-free)" check_build
step "cargo test" cargo test --workspace --locked
step "failpoint suites" check_failpoints
step "cargo deny check" cargo deny check
step "check-vendored.sh" bash scripts/check-vendored.sh
step "check-deny-fixtures.sh" bash scripts/check-deny-fixtures.sh
step "no chronik/tokio-uring/prometheus" check_forbidden_crates
if [[ -f "$WORKFLOW" ]]; then
  step "workflow validation" check_workflow
else
  skip "workflow validation" "$WORKFLOW not present"
fi
if [[ "$RUN_MUSL" -eq 1 ]]; then
  step "zig + cargo-zigbuild present" check_zig
  step "musl release (static, mimalloc)" check_musl_release
  if [[ "$RUN_MUSL_TEST" -eq 1 ]]; then
    step "musl zigbuild test" check_musl_test
  else
    skip "musl zigbuild test" "--no-musl-test"
  fi
else
  skip "zig + cargo-zigbuild present" "--no-musl"
  skip "musl release (static, mimalloc)" "--no-musl"
  skip "musl zigbuild test" "--no-musl"
fi

echo
echo "================ ci-local summary ================"
fails=0
for i in "${!NAMES[@]}"; do
  printf '%-36s %-5s %s\n' "${NAMES[$i]}" "${RESULTS[$i]}" "${TIMES[$i]}"
  [[ "${RESULTS[$i]}" == FAIL ]] && fails=$((fails + 1))
done
echo "=================================================="
if [[ "$fails" -gt 0 ]]; then
  echo "${fails} step(s) FAILED"
  exit 1
fi
echo "all steps passed"
