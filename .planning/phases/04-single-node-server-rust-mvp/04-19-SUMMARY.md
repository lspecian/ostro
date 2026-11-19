---
phase: 04-single-node-server-rust-mvp
plan: 19
subsystem: cli
tags: [cli, binary, dev, server, config, signals, startup, openapi]
requires:
  - phase: 04-02
    provides: ServerConfig profiles, load_layered (base < TOML < env < overrides), to_toml_string, init_tracing
  - phase: 04-17
    provides: Server::start -> ServerHandle (local_addr, data_dir, is_ephemeral, timings, wait, shutdown), tmpfs ephemeral dir
  - phase: 04-08
    provides: ostro_api::openapi(), UI shell at GET /
provides:
  - "`ostro dev [--ephemeral] [--listen] [--data-dir]`, `ostro server [-c/--config] [--print-config] [--listen] [--data-dir]`, `ostro openapi`, `ostro build-info` (unchanged)"
  - "Ready line on stdout: `NAME listening on http://ADDR (data: DIR[, ephemeral]) ready in N ms`"
  - "crates/ostro/tests/common: spawned-binary harness (Spawned, ReadyLine, free_addr, SERIAL, OSTRO_BIN override, OSTRO_STARTUP_BUDGET_MS)"
affects: [04-20, 10-sdks, 11-ui, 14-release]
tech-stack:
  added: [ostro-server, tokio, serde_json, tracing in crates/ostro; dev: reqwest, tempfile, tonic, ostro-proto]
  patterns:
    - "Signal handlers (SIGINT + SIGTERM) registered before Server::start, so a signal during startup is delivered right after it and the ephemeral dir is still removed"
    - "One-line `ostro: error: ...` on stderr + exit 1 for every startup/config failure"
    - "Spawned-binary tests scrub inherited OSTRO_* env and parse the ready line; one server at a time per test process (tokio Mutex)"
key-files:
  created:
    - crates/ostro/tests/common/mod.rs
    - crates/ostro/tests/dev_startup.rs
    - crates/ostro/tests/server_config.rs
  modified:
    - crates/ostro/Cargo.toml
    - crates/ostro/src/main.rs
    - crates/ostro/tests/cli.rs
    - Cargo.lock
key-decisions:
  - "`ostro dev` reads OSTRO_<SECTION>__* env (no config file); `--listen` has no clap default (dev_defaults supplies 127.0.0.1:7700) so OSTRO_API__LISTEN is not shadowed; OSTRO_LISTEN is the flag's env alias"
  - "`ostro server` without --config/OSTRO_CONFIG reads no file (no implicit search path); an explicit file must exist"
  - "Ready-line N = milliseconds since process start (includes config load, runtime build and Server::start); the server's own StartupTimings.total stays in the info log"
  - "Removed the ostro-engine/ostro-partition anchor deps: both arrive through ostro-server"
requirements-completed: [CLI-01, CLI-02]
duration: 45min
completed: 2026-11-19
---

# Phase 4 Plan 19: `ostro dev` / `ostro server` binary Summary

**The `ostro` binary is now the product entry point: `ostro dev` boots the full single-node server (meta, partitions, dispatcher with the embedded engine executor, REST + gRPC + UI shell on one port) with zero config, `--ephemeral` runs on a tmpfs dir removed on SIGINT/SIGTERM, `ostro server` layers TOML < OSTRO_* env < flags with `--print-config`, and `ostro openapi` prints the served document. Startup is ~75 ms ephemeral and ~300 ms warm (release, P=16), both asserted < 1 s through the spawned binary.**

## Performance

- **Duration:** about 45 min of work, plus ~6 min musl release build and the full ci-local run
- **Tasks:** 2
- **Files:** 3 created, 4 modified

## Startup numbers (P=16, WSL2, spawn -> `/readyz` 200)

| Mode | Debug binary | Release binary (musl, static, mimalloc) |
|------|--------------|------------------------------------------|
| `--ephemeral` (tmpfs) | 108-114 ms | 75-76 ms |
| Warm restart (on-disk `/tmp`) | 322-328 ms | 292 ms quiet; 732 ms once while a release build ran |
| Cold first run (on disk, measured only) | 1.6 s in-test; `cold_first_run_measurement` min 1697 / median 2128 ms (machine loaded by a parallel release build) | min 1557 / median 1624 ms (5 runs) |
| `ostro server -c x.toml`, P=4, first run on disk | 914 ms | - |

Breakdown of a release warm restart (server log): meta leader 14 ms, runtime 3 ms, `Node::open` 116-157 ms, total 273-310 ms. The remaining ~140 ms is `record_leaders` (16 sequential `SetPartitionLeader` meta proposals, one fsync each). Cold first run is dominated by `Node::open` (1432 ms: about 2k fsyncs creating 16 partitions on WSL2), matching the research estimate. The locked decision holds: the < 1 s target applies to warm restart and `--ephemeral`, and cold is reported only.

## Accomplishments

- **main.rs**
  - clap subcommands `dev`, `server`, `openapi`, `build-info`. The build-info output format is unchanged, so the CI musl check still passes.
  - `dev`: base `dev_defaults()` plus OSTRO_* env and flag overrides. `--ephemeral` sets `node.ephemeral = true`.
  - `server`: base `server_defaults()`. `-c/--config` (env `OSTRO_CONFIG`) must exist when given. `--print-config` prints `to_toml_string()` and exits 0 without starting the server or touching the data dir.
  - Multi-thread tokio runtime, then `init_tracing(&cfg.log)`, then `Server::start`, then the ready line (flushed), then `select!` over SIGINT, SIGTERM and `handle.wait()`, then `handle.shutdown().await`. A clean shutdown exits 0. A failure prints one stderr line and exits 1.
  - Non-unix builds fall back to `ctrl_c`.
- **tests/cli.rs** (5 tests): `--help` lists dev/server/openapi/build-info. `ostro openapi` parses as JSON, contains `/v1/workflows/{name}/runs`, and equals `ostro_api::openapi()`, which is the same document `/openapi.json` serialises. A missing `--config` file produces a one-line error naming the path.
- **tests/dev_startup.rs** (4 tests + 1 ignored):
  - `ephemeral_under_1s`: the data dir is under `/dev/shm`, `./.ostro` is untouched, SIGINT exits 0 and the dir is gone.
  - `warm_restart_under_1s`: the cold start is printed. The warm start is asserted, and on it `GET /` returns text/html linking `/openapi.json` and gRPC health reports SERVING on the same port. SIGTERM exits 0 and the dir is kept.
  - `default_data_dir_is_dot_ostro_and_persists`: two starts with no flags in a temp cwd.
  - `busy_port_fails_with_one_line`
  - `cold_first_run_measurement` (`#[ignore]`): 5 fresh dirs, prints min and median. `OSTRO_COLD_DIR` picks the filesystem.
- **tests/server_config.rs** (4 tests):
  - `print_config_defaults`: listen `0.0.0.0:7700`, partitions 16, data_dir `./ostro-data`, lease_ttl `30s`. No data dir is created.
  - `toml_then_env_then_flags`: covers TOML, `OSTRO_CONFIG`, env over TOML (lease_ttl `5s`, listen), and flags over both (`--listen`, `--data-dir`).
  - `live_start_with_toml`: P=4, `/readyz` 200, SIGTERM exits 0. Restarting the same dir with `OSTRO_NODE__PARTITIONS=8` gives a one-line error naming 4 and 8.
  - `bad_config_fails`: an unknown TOML key (named in the error, for both print and start), an unknown sectioned env key, a bad duration and a missing file.

## Task Commits

1. **Task 1: clap subcommands, config layering, signals, openapi**: `bb4b5c5` (feat), with Cargo.lock
2. **Task 2: spawned-binary CLI-01/CLI-02 suites**: `fe10e7f` (test)

## Verification

- `cargo test -p ostro --locked`: cli 5, dev_startup 4 (+1 ignored), server_config 4. All green, with both the debug binary and `OSTRO_BIN=<musl release>` (twice).
- `cargo clippy -p ostro --all-targets --locked -- -D warnings` and `cargo fmt --check`: clean.
- Full `bash scripts/ci-local.sh`, including the musl legs, with `CARGO_TARGET_DIR=target/wt-04-19`: see "CI" below.

## CI

The full `bash scripts/ci-local.sh`, musl legs included, passed every step at HEAD 5b64ea3 (04-18 complete) with `CARGO_TARGET_DIR=target/wt-04-19`:

| Step | Result | Time |
|------|--------|------|
| toolchain | PASS | |
| fmt | PASS | |
| clippy | PASS | |
| warning-free build | PASS | |
| `cargo test` | PASS | 381 s |
| failpoint suites | PASS | 331 s |
| deny, check-vendored, deny-fixtures | PASS | |
| forbidden crates | PASS | |
| workflow validation | PASS | |
| zig present | PASS | |
| musl release (static, `build-info` reports `allocator=mimalloc target=x86_64-unknown-linux-musl`) | PASS | |
| musl zigbuild test | PASS | 283 s |

No `Node::open timed out` flake. An earlier full run, while 04-18 was still editing ostro-server, failed only the warning-free build step. That step's log showed no warnings, and running the build again gave rc 0 with no warnings. Every other step in that run passed, including both musl legs.

## Deviations from Plan

**1. Task 2 was not a strict RED/GREEN cycle.** Task 1 already implements every behaviour the Task 2 tests check, because the plan puts the whole binary in Task 1. So the spawned-binary tests passed on their first run, and no separate RED commit was possible.

**2. A few extra tests beyond the plan list:** busy port, the default `./.ostro` dir, `OSTRO_CONFIG`, partition mismatch through the binary, an unknown env key and a bad duration. They are cheap, and they prove the one-line error contract named in the plan.

**3. Ready-line format.** The plan's `(data: DIR|ephemeral)` became `(data: DIR[, ephemeral])`, so the ephemeral tmpfs path is printed too. The test needs that path to check that the dir is removed.

**4. Release number taken from the musl release binary.** I timed the musl static binary built by the CI leg, instead of a separate `cargo build -p ostro --release`. It is the shipped artefact, and this saved a second fat-LTO build.

## Hand-off: 04-20 (e2e via `ostro dev`)

- Reuse `crates/ostro/tests/common/mod.rs`:
  - `dev(cwd, &["--ephemeral", "--listen", &addr])`, then `Spawned::spawn`, then `wait_readyz(&http(), addr)`, then `ready_line(T)`, and finally `stop_clean("INT")`.
  - Hold `SERIAL.lock().await` around a spawned server.
  - `command()` scrubs inherited OSTRO_* and RUST_LOG. Add env with `cmd.env(...)`, for example `OSTRO_NODE__PARTITIONS=4` for speed or `OSTRO_LOG__LEVEL=warn` for quieter output.
- Log output goes to stdout (pretty in dev), and the harness drains it. The ready line is the only line `ReadyLine::parse` accepts. Errors go to stderr; read them with `Spawned::stderr()`.
- Deploying a guest needs the `guests/` build: copy `guest_wasm` from `crates/ostro-server/tests/common/mod.rs`. A dev-dep on ostro-server's test helpers is not possible.
- Optional speed-up, not needed for CLI-01: `record_leaders` (~140 ms of a warm restart) proposes 16 `SetPartitionLeader` sequentially. Batching or pipelining them in `server.rs` would bring warm restart close to `Node::open` alone.

## Self-Check: PASSED

- FOUND: crates/ostro/src/main.rs, tests/common/mod.rs, tests/dev_startup.rs, tests/server_config.rs, tests/cli.rs
- FOUND: commits bb4b5c5, fe10e7f
