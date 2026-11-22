#!/usr/bin/env bash
# Local CI gate: mirrors .github/workflows/ci.yml (lint job + linux-x86_64-musl leg).
#
# Usage: bash scripts/ci-local.sh [--no-musl] [--no-musl-test] [--no-polyglot] [--no-go]
#   --no-musl       skip the cargo-zigbuild x86_64 musl leg entirely
#   --no-musl-test  build the musl release binary but skip `cargo-zigbuild test`
#   --no-polyglot   skip the Python/TypeScript SDK + polyglot conformance block (SKIP rows)
#                   (also the Python/TS client tests and the ostro-build polyglot step, 10-02)
#   --no-go         skip the Go client tests (clients/go, 10-02); otherwise Go must be on PATH
#
# Requires: rustup toolchain from rust-toolchain.toml, cargo-deny 0.21.0, jq,
# cargo-zigbuild 0.24.1 + zig 0.17.1 (on PATH or via `pip install ziglang`).
# Polyglot block (09-17): Node 24 + npm, uv 0.7.15 and a Python 3.12 that `uv python find`
# sees (skip only with --no-polyglot).
# Clients (10-02): Go 1.23+ for clients/go (skip only with --no-go).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_MUSL=1
RUN_MUSL_TEST=1
RUN_POLYGLOT=1
RUN_GO=1
for arg in "$@"; do
  case "$arg" in
    --no-musl) RUN_MUSL=0 ;;
    --no-musl-test) RUN_MUSL_TEST=0 ;;
    --no-polyglot) RUN_POLYGLOT=0 ;;
    --no-go) RUN_GO=0 ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
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
# `ostro:<target>` entries are the spawned-binary Phase 6 crash tests (06-17 fx_redrive,
# 06-18 fx_sql_crash); the crate is linted once (check_failpoints de-duplicates).
# Phase 12 (12-01): ostro-cluster (repl::/fetch::/isr::/install:: sites) and ostro-repl-conformance
# (mutant matrix, fault legs) are appended last. 09-17 appends ostro-server:children_failpoints
# (09-08's child:: failpoints).
FAILPOINT_CRATES=(ostro-wal ostro-blob ostro-log ostro-partition ostro-crashtest ostro-meta ostro-engine:aot_failpoints ostro-server:failpoints_phase5 ostro-backup ostro-bridge ostro:fx_redrive ostro:fx_sql_crash ostro-worker ostro-cluster ostro-repl-conformance ostro-server:children_failpoints ostro-server:rerun_rearm ostro-server:dlq_api ostro-server:bulk_ops ostro:ops_kill)
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

# Phase 6 external services (06-18, D-FX5). The DB / S3 legs read these variables and SKIP
# loudly when they are unset; ci.yml sets them on the linux-x86_64-gnu leg (postgres:17 and
# mysql:8.4 service containers, SeaweedFS via docker run).
#   OSTRO_TEST_PG_URL     postgres://<user>:<password>@127.0.0.1:5432/postgres[?sslmode=disable]
#   OSTRO_TEST_MYSQL_URL  mysql://<user>:<password>@127.0.0.1:3306/<db>?ssl-mode=REQUIRED
#   OSTRO_TEST_S3_URL, OSTRO_TEST_S3_ACCESS_KEY, OSTRO_TEST_S3_SECRET_KEY, OSTRO_TEST_S3_BUCKET
# A password in a URL must be at least 8 bytes (it travels as a sealed secret).
#
# Local credentials live in an untracked `.ostro-test-env` at the repo root (gitignored),
# one `KEY=value` per line, `#` comments and blank lines ignored, no quoting, no shell
# expansion (the value is everything after the first `=`), e.g.
#   OSTRO_TEST_PG_URL=postgres://ostro:<password>@127.0.0.1:5432/postgres?sslmode=disable
# Only OSTRO_TEST_* keys are read and exported. ci-local refuses to run if the file is
# tracked by git.
TEST_ENV_FILE=".ostro-test-env"
if git ls-files --error-unmatch "$TEST_ENV_FILE" >/dev/null 2>&1; then
  echo "error: $TEST_ENV_FILE is tracked by git; it holds local credentials." >&2
  echo "       git rm --cached $TEST_ENV_FILE (it is in .gitignore)" >&2
  exit 1
fi
if [[ -f "$TEST_ENV_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "${line//[[:space:]]/}" || "$line" =~ ^[[:space:]]*# ]] && continue
    if [[ "$line" =~ ^(OSTRO_TEST_[A-Z0-9_]+)=(.*)$ ]]; then
      export "${BASH_REMATCH[1]}=${BASH_REMATCH[2]}"
    else
      echo "warning: $TEST_ENV_FILE: ignoring a line that is not OSTRO_TEST_<NAME>=value" >&2
    fi
  done < "$TEST_ENV_FILE"
  echo "loaded $TEST_ENV_FILE (values not shown)"
fi

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

# Phase 7 (07-22): the spawned-binary suites worker_smoke, worker_e2e, worker_failover,
# flow_e2e, keda_sim and cli_p7 are not #[ignore]d, so the "cargo test" step runs them
# (each binary serialises its own tests). What it misses are the failpoint-only legs (the
# SIGSTOP zombie and the 100-run QA-03 chaos smoke, both need the failpoint `ostro` binary)
# and the offline manifest check. Keep in sync with the "Phase 7 failpoint legs +
# manifests" step of .github/workflows/ci.yml. The 10,000-run QA-03 campaign is
# `OSTRO_CHAOS_RUNS=10000 OSTRO_CHAOS_SHARDS=20 bash scripts/fencing-chaos.sh` (gate only).
check_phase7() {
  local out
  out="$(cargo test -p ostro --features failpoints --test worker_failover --test fencing_chaos --locked -- --test-threads=1 sigstop_zombie chaos_smoke 2>&1)" || { echo "$out"; return 1; }
  sed -n -E '/^test |test result/p' <<<"$out"
  # Both targets must run their leg (a renamed test would otherwise pass vacuously).
  [[ "$(grep -Ec 'test result: ok\. [1-9][0-9]* passed' <<<"$out")" -eq 2 ]] || { echo "a Phase 7 failpoint leg ran no test"; return 1; }
  bash deploy/check-manifests.sh
}

check_failpoints() {
  local c crate cmd
  local -A linted=()
  for c in "${FAILPOINT_CRATES[@]}" "${FAILPOINT_LINT_ONLY[@]}"; do
    crate="${c%%:*}"
    # A crate listed more than once (`crate:target` entries) is linted once.
    [[ -n "${linted[$crate]:-}" ]] && continue
    linted[$crate]=1
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

# Phase 12 (12-08, D5 / docs/04 §4 rule 1): one partition log implementation. The former
# RF=1 type name must not come back, and the log crate must not branch on the replication
# factor (RF=1 is a replica set of one, not a second path).
check_one_log_path() {
  local hits rc=0
  hits=$(grep -rn "Rf1Log" crates/ || true)
  if [ -n "$hits" ]; then echo "Rf1Log is gone (use PartitionLog):"; echo "$hits"; rc=1; fi
  hits=$(grep -rnE "rf *== *1|replicas\.len\(\) *== *1" crates/ostro-log/src || true)
  if [ -n "$hits" ]; then echo "RF-specific branch in ostro-log:"; echo "$hits"; rc=1; fi
  return $rc
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

# Polyglot block (plan 09-17, D-P9-12/D-P9-28): Python and TypeScript SDK host tests plus
# the polyglot conformance suites. Keep in sync with the `polyglot` job of
# .github/workflows/ci.yml. `cargo test -p ostro-conformance --features polyglot` runs every
# test target of the crate, i.e. polyglot_smoke, polyglot_engine, py_sdk_build,
# ts_sdk_build, polyglot_python (09-14), polyglot_ts (09-15), polyglot_cross (09-16, whose
# full_table re-runs every py/ts scenario; kept although it overlaps the two per-language
# targets, for the cross-language table) and rebuild (C-REBUILD-01, 09-17). The crash leg
# (09-18) and the resource measurements (09-19) stay #[ignore]. Gate-only commands:
#   bash sdks/scripts/repro-check.sh all          # 3 builds per language, sha256 (ci.yml runs it)
#   OSTRO_COMPONENTIZE_PY="$(bash sdks/scripts/build-componentize-py-fork.sh --print)" \
#     bash sdks/scripts/repro-check.sh py         # after building the D-P9-3 fork (docs/sdk-builds.md)
#   cargo test -p ostro-conformance --features polyglot -- --ignored   # crash leg + resources
check_polyglot_toolchains() {
  local v ok=0
  if command -v node >/dev/null 2>&1; then
    v="$(node --version)"; echo "node $v"
    [[ "$v" == v24.* ]] || { echo "need Node 24 (found $v)"; ok=1; }
  else
    echo "node not found (need Node 24)"; ok=1
  fi
  if command -v npm >/dev/null 2>&1; then echo "npm $(npm --version)"; else echo "npm not found"; ok=1; fi
  if command -v uv >/dev/null 2>&1; then
    v="$(uv --version | awk '{print $2}')"; echo "uv $v"
    [[ "$v" == "0.7.15" ]] || { echo "need uv 0.7.15 exactly (D-P9-28, found $v)"; ok=1; }
    uv python find 3.12 || { echo "uv finds no Python 3.12 (uv python install 3.12)"; ok=1; }
  else
    echo "uv not found (need uv 0.7.15)"; ok=1
  fi
  return "$ok"
}

check_python_sdk() {
  (cd sdks/python && PYTHONDONTWRITEBYTECODE=1 uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q)
}

# Node 24's default reporter prints "ℹ pass N"; the TAP reporter prints "# pass N".
check_ts_sdk() {
  local out
  # sdks/ts has no dependencies (nothing to install); the pinned build toolchain is
  # installed when missing or older than its lock (as build-guest.sh does).
  local stamp="sdks/scripts/toolchain/node_modules/.package-lock.json"
  if [[ ! -f "$stamp" || "sdks/scripts/toolchain/package-lock.json" -nt "$stamp" ]]; then
    (cd sdks/scripts/toolchain && npm ci --silent --no-audit --no-fund) || return 1
  fi
  out="$(node --test --test-reporter=tap sdks/ts/test/ 2>&1)" || { echo "$out"; return 1; }
  grep -E '^# (tests|pass|fail)' <<<"$out"
  grep -Eq '^# pass [1-9][0-9]*$' <<<"$out" && grep -Eq '^# fail 0$' <<<"$out"
}

check_polyglot_conformance() {
  cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=2
}

check_polyglot_clippy() {
  cargo clippy -p ostro-conformance --features polyglot --all-targets --locked -- -D warnings
}

# Phase 10 clients + ostro-build polyglot (plan 10-02). Keep in sync with the "Clients ..." and
# "ostro-build polyglot" steps of the `polyglot` job in .github/workflows/ci.yml. Until 10-04
# lands a client directory its step records a visible SKIP "scaffold pending (10-04)"; 10-20
# asserts no such SKIP remains. The client tests spawn the debug `ostro` binary (OSTRO_BIN).
ostro_debug_bin() {
  local dir="${CARGO_TARGET_DIR:-target}"
  [[ "$dir" == /* ]] || dir="$ROOT/$dir"
  cargo build -p ostro --locked >&2 || return 1
  echo "$dir/debug/ostro"
}

check_clients_python() {
  local bin
  bin="$(ostro_debug_bin)" || return 1
  (cd clients/python && OSTRO_BIN="$bin" PYTHONDONTWRITEBYTECODE=1 \
    uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q)
}

check_clients_ts() {
  local bin out v
  v="$(node --version 2>/dev/null)" || { echo "node not found (need Node 24)"; return 1; }
  [[ "$v" == v24.* ]] || { echo "need Node 24 (found $v)"; return 1; }
  bin="$(ostro_debug_bin)" || return 1
  # A directory argument only works with a test/index.js (as sdks/ts has); the glob does not
  # need one (Node expands it itself).
  out="$(OSTRO_BIN="$bin" node --test --test-reporter=tap 'clients/ts/test/**/*.test.js' 2>&1)" || { echo "$out"; return 1; }
  grep -E '^# (tests|pass|fail)' <<<"$out"
  grep -Eq '^# pass [1-9][0-9]*$' <<<"$out" && grep -Eq '^# fail 0$' <<<"$out"
}

check_clients_go() {
  local bin
  if ! command -v go >/dev/null 2>&1; then
    echo "###################################################################"
    echo "go not found: clients/go needs Go 1.23+ (or pass --no-go)"
    echo "###################################################################"
    return 1
  fi
  go version
  bin="$(ostro_debug_bin)" || return 1
  (cd clients/go && go vet ./... && OSTRO_BIN="$bin" go test ./...)
}

# `cargo test -p ostro --features polyglot` would re-run every spawned-binary suite of the
# `ostro` crate single-threaded, so only its polyglot targets run here (parity, replay_cli,
# template_tests, added by later Phase 10 plans); a target not yet present is reported.
OSTRO_POLYGLOT_TESTS=(parity replay_cli template_tests)

check_ostro_build_polyglot() {
  local t
  local -a targets=()
  cargo clippy -p ostro-build --features polyglot --all-targets --locked -- -D warnings || return 1
  cargo clippy -p ostro --features polyglot --all-targets --locked -- -D warnings || return 1
  cargo test -p ostro-build --features polyglot --locked -- --test-threads=2 || return 1
  for t in "${OSTRO_POLYGLOT_TESTS[@]}"; do
    if [[ -f "crates/ostro/tests/$t.rs" ]]; then
      targets+=(--test "$t")
    else
      echo "ostro polyglot target $t: not present yet"
    fi
  done
  if [[ "${#targets[@]}" -gt 0 ]]; then
    cargo test -p ostro --features polyglot "${targets[@]}" --locked -- --test-threads=1 || return 1
  fi
}

# client_step <name> <marker file> <fn>: the step, or the 10-04 pending SKIP.
client_step() {
  if [[ -f "$2" ]]; then
    step "$1" "$3"
  else
    skip "$1" "scaffold pending (10-04)"
  fi
}

# "Phase 6 external services" banner (06-18): one loud SKIP line per unset variable, each
# also recorded as a SKIP row in the summary. Variable names match ci.yml.
external_services_banner() {
  local var legs any=0
  local -a vars=(OSTRO_TEST_PG_URL OSTRO_TEST_MYSQL_URL OSTRO_TEST_S3_URL)
  local -A what=(
    [OSTRO_TEST_PG_URL]="ostro-bridge sql_pg, ostro fx_sql_crash pg_after_commit/pg_before_user_stmt"
    [OSTRO_TEST_MYSQL_URL]="ostro-bridge sql_mysql, ostro fx_sql_crash mysql_after_commit/mysql_before_user_stmt"
    [OSTRO_TEST_S3_URL]="ostro-objstore s3, ostro-bridge object S3 legs, ostro-server fx_object_e2e S3 legs"
  )
  echo
  echo "==> Phase 6 external services"
  for var in "${vars[@]}"; do
    legs="${what[$var]}"
    if [[ -n "${!var:-}" ]]; then
      echo "    set: ${var} (${legs})"
    else
      any=1
      echo "    ###################################################################"
      echo "    SKIP (human_needed): ${var} unset — ${legs}"
      echo "    ###################################################################"
      record "external: ${var}" SKIP "-"
    fi
  done
  if [[ "$any" -eq 1 ]]; then
    echo "    (set them in ${TEST_ENV_FILE} or the environment; see the comment at the top)"
  fi
}

step "toolchain 1.100.0" check_toolchain
step "cargo fmt --check" cargo fmt --all --check
step "cargo clippy -D warnings" check_clippy
step "cargo build (warning-free)" check_build
step "sdk workspace fmt + clippy" check_sdk_lint
step "sdk workspace test + wasm build" check_sdk_test
step "guests build (wasm32-wasip2)" check_guests
step "order-saga example" check_order_saga
external_services_banner
step "cargo test" cargo test --workspace --locked
if [[ "$RUN_POLYGLOT" -eq 1 ]]; then
  step "polyglot toolchains present" check_polyglot_toolchains
  step "python sdk tests" check_python_sdk
  step "ts sdk tests" check_ts_sdk
  step "polyglot conformance" check_polyglot_conformance
  step "clippy (polyglot feature)" check_polyglot_clippy
else
  skip "polyglot toolchains present" "--no-polyglot"
  skip "python sdk tests" "--no-polyglot"
  skip "ts sdk tests" "--no-polyglot"
  skip "polyglot conformance" "--no-polyglot"
  skip "clippy (polyglot feature)" "--no-polyglot"
fi
# Phase 10 (10-02): client test kits and the ostro-build polyglot tests.
if [[ "$RUN_POLYGLOT" -eq 1 ]]; then
  client_step "clients python" clients/python/pyproject.toml check_clients_python
  client_step "clients ts" clients/ts/package.json check_clients_ts
  step "ostro-build polyglot" check_ostro_build_polyglot
else
  skip "clients python" "--no-polyglot"
  skip "clients ts" "--no-polyglot"
  skip "ostro-build polyglot" "--no-polyglot"
fi
if [[ "$RUN_GO" -eq 1 ]]; then
  client_step "clients go" clients/go/go.mod check_clients_go
else
  skip "clients go" "--no-go"
fi
step "semantics sleepers (serial)" check_sleepers
step "failpoint list sync (ci.yml)" check_failpoint_sync
step "failpoint suites" check_failpoints
step "server campaign smoke (20 kills)" check_campaign_smoke
step "phase 7 failpoint legs + manifests" check_phase7
step "cargo deny check" cargo deny check
step "check-vendored.sh" bash scripts/check-vendored.sh
step "check-deny-fixtures.sh" bash scripts/check-deny-fixtures.sh
step "no chronik/tokio-uring/prometheus" check_forbidden_crates
step "one partition log path (12-08)" check_one_log_path
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
