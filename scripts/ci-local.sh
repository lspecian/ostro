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
# Keep in sync with the "Failpoint suites" step of .github/workflows/ci.yml (the
# "failpoint list sync" step checks it): each entry runs
#   cargo test -p <crate> --features failpoints [--test <target>] --locked -- --test-threads=1
# `<crate>:<target>` restricts a crate whose full suite would take over 5 min single-threaded
# to its failpoint test target (plan 05-18); the whole crate is still linted with the feature.
FAILPOINT_CRATES=(ostro-wal ostro-blob ostro-log ostro-partition ostro-crashtest ostro-meta ostro-engine:aot_failpoints ostro-server:failpoints_phase5 ostro-backup)
# Crates linted with `--features failpoints` that have no failpoint suite of their own
# (the `ostro` binary forwards the feature to ostro-server; plan 05-18).
FAILPOINT_LINT_ONLY=(ostro)
# ostro-crashtest's crash loops are #[ignore] (slow, spawn processes), so this gate only
# lints them and runs the crate's unit tests. Run them explicitly (02-VALIDATION.md):
#   OSTRO_CRASH_ITERS=50 cargo test --release -p ostro-crashtest --test crash_loop \
#     -- --ignored --nocapture
#   OSTRO_CRASH_ITERS=50 cargo test --release -p ostro-crashtest --features failpoints \
#     --test crash_loop -- --ignored --nocapture crash_loop_io_errors
#   PROPTEST_CASES=1000 cargo test --release -p ostro-partition --test rebuild_digest
# Phase gate: OSTRO_CRASH_ITERS=500 (crash_loop_kill9) + PROPTEST_CASES=1000 rebuild_digest.
# Measurements (plan 02-16, #[ignore], print tables, run on a quiet machine):
#   cargo test --release -p ostro-crashtest --test reopen_time -- --ignored --nocapture \
#     --test-threads=1
#   cargo test --release -p ostro-crashtest --test apply_throughput -- --ignored --nocapture \
#     --test-threads=1
# Whole-server crash campaign (plan 05-19, QA-01): it drives the real binary built with
# every failpoint compiled in (meta::, aot::, executor::, dispatcher:: from plan 05-18, plus
# wal::, state::, blob::), configured through the FAILPOINTS env var. Build it out of the
# way of target/release:
#   cargo build --release -p ostro --features failpoints --target-dir target/failpoints
# (`target/failpoints/release/ostro build-info` then ends in `failpoints=on`; the campaign
# test builds it itself when it is missing). The "server campaign smoke" step below runs 20
# kills from a wiped target/campaign-smoke; the timing run uses target/campaign-timing and
# the 05-20 gate target/campaign-gate (never share a state dir: the runner refuses to resume
# a shard file from another SHARDS/FAULTS/seed). Commands: crates/ostro-crashtest/tests/
# server_campaign.rs.
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

# The failpoint test command of one FAILPOINT_CRATES entry (`crate` or `crate:target`),
# word per line; the "Failpoint suites" step of ci.yml must run exactly these.
failpoint_test_cmd() {
  local crate="${1%%:*}" target=""
  [[ "$1" == *:* ]] && target="${1#*:}"
  printf '%s\n' cargo test -p "$crate" --features failpoints
  [[ -n "$target" ]] && printf '%s\n' --test "$target"
  printf '%s\n' --locked -- --test-threads=1
}

# FAILPOINT_CRATES and the ci.yml "Failpoint suites" step list the same commands, in order.
check_failpoint_sync() {
  local want got c
  want="$(for c in "${FAILPOINT_CRATES[@]}"; do failpoint_test_cmd "$c" | paste -sd' '; done)"
  got="$(awk '
    /name: Failpoint suites/ { in_step = 1; next }
    in_step && /^ *- / { exit }
    in_step && /^ *cargo test / { sub(/^ +/, ""); print }
  ' "$WORKFLOW")"
  if [[ "$want" != "$got" ]]; then
    echo "FAILPOINT_CRATES and the ci.yml Failpoint suites step differ:"
    diff <(echo "$want") <(echo "$got") || true
    return 1
  fi
  echo "$want"
}

# QA-01 whole-server kill campaign smoke (plan 05-19): 20 kill -9 faults against the
# failpoint binary from a freshly wiped state dir, then the aggregate report (fails on any
# lost acked run / duplicate journal entry / wrong result). Keep in sync with the "Server
# campaign smoke" step of .github/workflows/ci.yml.
check_campaign_smoke() {
  rm -rf target/campaign-smoke
  OSTRO_CAMPAIGN_STATE_DIR=target/campaign-smoke OSTRO_CAMPAIGN_FAULTS=20 \
    cargo test --release -p ostro-crashtest --features failpoints --test server_campaign --locked -- --ignored --nocapture campaign || return 1
  OSTRO_CAMPAIGN_STATE_DIR=target/campaign-smoke OSTRO_CAMPAIGN_FAULTS=20 \
    cargo test --release -p ostro-crashtest --features failpoints --test server_campaign --locked -- --ignored --nocapture report
}

check_failpoints() {
  local c crate cmd
  for c in "${FAILPOINT_CRATES[@]}" "${FAILPOINT_LINT_ONLY[@]}"; do
    crate="${c%%:*}"
    # ostro-wal is vendored and excluded from workspace clippy; lint the rest.
    if [[ "$crate" != "ostro-wal" ]]; then
      cargo clippy -p "$crate" --features failpoints --all-targets --locked -- -D warnings || return 1
    fi
  done
  for c in "${FAILPOINT_CRATES[@]}"; do
    mapfile -t cmd < <(failpoint_test_cmd "$c")
    "${cmd[@]}" || return 1
  done
}

# Rust guest SDK (plan 04-20): a separate cargo workspace (sdks/rust, no committed lock,
# so no --locked). Host tests include the trybuild macro diagnostics; the runtime crate
# must build and lint clean for wasm32-wasip2. Keep in sync with the SDK steps of
# .github/workflows/ci.yml (lint job + linux-x86_64-gnu leg).
SDK_MANIFEST="sdks/rust/Cargo.toml"

check_sdk_lint() {
  cargo fmt --manifest-path "$SDK_MANIFEST" --all --check || return 1
  cargo clippy --manifest-path "$SDK_MANIFEST" --all-targets -- -D warnings || return 1
  cargo clippy --manifest-path "$SDK_MANIFEST" --target wasm32-wasip2 -- -D warnings
}

check_sdk_test() {
  cargo test --manifest-path "$SDK_MANIFEST" || return 1
  cargo build --manifest-path "$SDK_MANIFEST" --target wasm32-wasip2
}

# Test guests (guests/, incl. the SDK-built sdk-*): the same build the integration
# tests' guest_wasm() helper runs, made an explicit step so a broken guest fails here
# with the compiler output instead of inside a test panic.
check_guests() {
  cargo build --release --target wasm32-wasip2 \
    --manifest-path guests/Cargo.toml --target-dir target/guests
}

# Example workflow project (plan 05-10): examples/order-saga is a standalone cargo project
# (own [workspace], path dependency on sdks/rust/ostro-sdk, no committed lock, so no
# --locked). It must stay fmt-clean and build/lint warning-free for wasm32-wasip2. Keep in
# sync with the "order-saga example" step of .github/workflows/ci.yml.
ORDER_SAGA_MANIFEST="examples/order-saga/Cargo.toml"

# WF-04 1,000-sleeper suite (plan 05-15): #[ignore]d in the parallel workspace run, own
# process, one thread (it measures process RSS). Keep in sync with ci.yml.
check_sleepers() {
  cargo test -p ostro-server --test semantics_sleepers --locked -- --ignored --test-threads=1 || return 1
}

check_order_saga() {
  cargo fmt --manifest-path "$ORDER_SAGA_MANIFEST" --check || return 1
  cargo build --release --target wasm32-wasip2 \
    --manifest-path "$ORDER_SAGA_MANIFEST" --target-dir target/order-saga || return 1
  cargo clippy --target wasm32-wasip2 \
    --manifest-path "$ORDER_SAGA_MANIFEST" --target-dir target/order-saga -- -D warnings
}

# Shared with the ci.yml "No chronik / tokio-uring / prometheus in the tree" step (P8-D17): an
# anchored crate-name match, so metrics-exporter-prometheus passes and prometheus fails.
check_forbidden_crates() {
  bash scripts/forbidden-crates.sh --self-test && bash scripts/forbidden-crates.sh
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
  local bin="${CARGO_TARGET_DIR:-target}/${MUSL_TARGET}/release/ostro" desc info
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
step "sdk workspace fmt + clippy" check_sdk_lint
step "sdk workspace test + wasm build" check_sdk_test
step "guests build (wasm32-wasip2)" check_guests
step "order-saga example" check_order_saga
step "cargo test" cargo test --workspace --locked
step "semantics sleepers (serial)" check_sleepers
step "failpoint list sync (ci.yml)" check_failpoint_sync
step "failpoint suites" check_failpoints
step "server campaign smoke (20 kills)" check_campaign_smoke
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
