---
plan: followups1
phase: 10-developer-cli-test-kits
type: follow-up batch (small independent fixes)
commits:
  - 305941c fix: server/worker logs go to stderr; stdout carries only the ready line
  - bebb9a0 fix: export! is a host no-op; the Rust template drops its wasm32 guard
  - 3b5b785 docs: OpenAPI scheduled/bulk-retry descriptions
  - be1c864 docs: musl binary size breakdown
completed: 2026-11-23
---

# followups1: four small follow-ups

## 1. Logs on stderr
`ostro dev` / `ostro server` / `ostro worker` printed their tracing output (about 1600 INFO
lines at startup) on stdout, ahead of the frozen ready line. `ostro_server::logging` now writes
the fmt layer to stderr, with ANSI colours only when stderr is a terminal. stdout carries only
the ready lines (`... listening on ...`, `ostro worker ready: ...`) and machine-readable
output. The ready-line format is unchanged.
- New test `ready_line::dev_stdout_has_no_log_lines_before_the_ready_line` (RUST_LOG=info:
  the ready line is the only stdout line before readiness, and INFO lines are on stderr).
- `dev_startup::busy_port_fails_with_one_line` now strips log lines before counting
  (`common::without_logs`), because a failing start may log to stderr before its one-line
  error.
- All four kits already drained stderr; only their comments changed.
  `scripts/clean-machine.sh` parses the ready line from stdout and keeps working.
- Green: ready_line (7), dev_startup, cli, cli_verbs, server_config, data_dir_lock,
  build_info, json_contract, time_skipping_cli, replay_cli, backup_cli, cli_phase8,
  cli_p11, completions, e2e_dev, worker_smoke; ostro-client kit (`--features testing`);
  Python kit (25), TS kit (26), Go kit; ostro-server logging unit tests; clippy.

## 2. `export!` on host targets
`ostro_sdk::export!` emits the WIT exports only under `cfg(target_arch = "wasm32")`. On any
other target it expands to a `const` that type-checks each listed type against `Workflow`.
The Rust template now calls it without a guard, and the init file list is unchanged.
- I reproduced the problem first: a cdylib+rlib copy of the template, built against the
  old SDK with no guard, fails to link on the host (rust-lld version-script error). Against
  the new SDK it links.
- New test `sdks/rust/ostro-sdk/tests/host_export.rs`. Also green: the SDK workspace tests
  (incl. trybuild), clippy on the host and on wasm32-wasip2, fmt, a wasm32 guest build
  (sdk-native), `templates_build::rust_template_builds` (wasm build + describe) and
  `template_tests::rust_template_test_passes` (host `cargo test` of the template).

## 3. OpenAPI descriptions
- `RunStatusDto` (enum-level, since utoipa drops unit-variant docs) and `domain::RunStatus`:
  `scheduled` covers a run waiting for its start delay **and** a run suspended on a wait.
- `BulkAction::Retry` now uses the CLI wording ("resume in place or restart as a new
  linked run").
- In the snapshot only two description strings changed; there is no schema change.

## 4. musl size breakdown
See `docs/binary-size.md`. **The binary is now 59.09 MB, leaving 0.9 MB of headroom** (it was
56.2 MB at the Phase 10 gate). The largest groups are the Ostro crates (12.9 MB),
wasmtime+cranelift (9.3 MB, including Pulley at about 1.45 MB, which cannot be disabled), core
and drop glue (7.9 MB), the async/HTTP stack (5.4 MB) and unwind tables (5.8 MB of
sections). Recommended next steps are per-crate `opt-level = "s"` for cold crates and
splitting the 1.25 MB of utoipa-axum route builders. Nothing was applied.

## Open
- The size gate is close: the next plan that adds code should apply option 1 or 2 from
  docs/binary-size.md first.
