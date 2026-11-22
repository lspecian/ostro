---
phase: 09-polyglot-sdks-conformance
plan: 13
subsystem: conformance
tags: [conformance, server-leg, children, wf-06, sdk-05, api, can, effects, wave-4]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-05/09-08 children bridge, 09-06 Rust SDK children/Saga/idempotency_key, 09-11 spec/projection/compare/langs + conf-rust"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "MockProvider (/pay, charges/requests per key), egress allow_private + per-module egress policy (06-17 approach)"
provides:
  - "ostro_conformance::server_leg: ServerHarness, run_server_spec, idempotency_key_for, module_name, substitute (role-tagged workflow names)"
  - "conf-rust: 15 server-leg workflows (rs_conf_parent_*, rs_conf_leaf_*, rs_conf_sig_api, rs_conf_token_api, rs_conf_fx_http, rs_conf_saga_http)"
  - "conformance/spec/server: 14 specs (C-CHILD-01..07, 09, 10, 11, C-SIG-API-01, C-TOKEN-API-01, C-CAN-02, C-FX-01)"
  - "crates/ostro-conformance/tests/server_leg.rs: 5 family tests + catalogue-ownership test + conf_saga_http smoke"
affects: [09-14, 09-15, 09-16, 09-18, 09-20]
tech-stack:
  added: []
  patterns:
    - "Role-tagged workflow refs in spec inputs (wf:/wf2:) resolved per language role, so cross-language specs need no schema change"
    - "Target paths (root, child, child#2, child.child) resolved through the deterministic child_run_id of the N-th ChildStarted"
key-files:
  created:
    - crates/ostro-conformance/src/server_leg.rs
    - crates/ostro-conformance/tests/server_leg.rs
    - conformance/spec/server/*.json (14)
  modified:
    - crates/ostro-conformance/src/lib.rs
    - guests/conf-rust/src/lib.rs
decisions:
  - "Server-leg-specific checks live in expect extras (descendants, chain_runs, fx_pay, variants); spec.rs and SCHEMA.md are untouched"
  - "Roles are positional (langs[0] root, langs[1] children, langs[2] grandchildren) with wf:/wfN: refs in inputs instead of a roles field"
  - "Journals/records are read from the partitions in-process; every control-plane action goes through REST"
metrics:
  duration: "~1 h"
  completed: 2026-11-22
---

# Phase 9 Plan 13: Server leg harness + Rust pass Summary

An in-process server harness drives a real `ostro-server` through its REST API. With it, the children bridge of 09-05/09-08 is proven end to end with Rust guests: attached, detached, failure mapping, cancel-op, parent close, terminate and run timeout, continue-as-new child, 3-level cancel, resume after close and cascade vs shielding. API signals and tokens, the continue-as-new result long-poll and an HTTP effect with `ctx.idempotency_key` against the MockProvider are proven too. All 14 specs pass 100% for Rust. No server bug was found, and no product code changed.

## Commits

| Task | Commit | Content |
|------|--------|---------|
| 1 | `064922b` | `server_leg.rs` harness, `lib.rs` module, 15 conf-rust workflows |
| 2 | `de97e48` | 14 server spec files, `tests/server_leg.rs` |

## Harness API (`ostro_conformance::server_leg`)

- `ServerHarness::start()`: `ServerConfig::dev_defaults()` with `node.ephemeral = true` (tmpfs TempDir), `api.listen` and `cluster.listen` on `127.0.0.1:0`, `node.partitions = 4`, and `egress.allow_private = ["127.0.0.0/8"]`. It then calls `Server::start` and waits for `/readyz`, and starts a loopback `MockProvider` alongside.
- `shutdown()`
- `deploy(wasm, name) -> sha hex`: `PUT /v1/modules/{sha}?name=` plus `PUT /v1/egress/{name}` allowing only the mock origin. Idempotent.
- `deploy_lang(lang)`: builds via `langs::module_for` and deploys once per harness as `conf-rust` / `conf-py` / `conf-ts`.
- Run control:
  - `start_run(workflow, input) -> RunId`
  - `signal`
  - `complete_token`
  - `published_token`
  - `cancel(run, reason)`
  - `terminate(run, reason)`
  - `retry`
  - `result(run, limit) -> Outcome`: long-poll with `follow_continuations=true`.
- Reads:
  - `list_runs(workflow)`
  - `history(run) -> Vec<Event>`: partition read.
  - `info(run) -> RunInfo`: status as the REST spelling, parent, continued_from/to, cancel_reason.
  - `chain(run)`
  - `chain_status`
  - `list_children(parent) -> Vec<ChildEdge>`
  - `resolve(root, path)`
  - `edges()`
- `check_children_unique()`: lists runs per child workflow and asserts exactly one run per (parent, op). That run must have the deterministic id, and no other first-generation run may carry that parent link (CAN successors are excluded).
- `run_spec(spec, langs)` and `run_server_spec(spec, langs)`. The second starts its own harness, runs the uniqueness check and shuts down. Both return the engine leg's `RunReport` / `Failure`, so 09-14/09-15 can reuse the same comparison code.
- Script `api` ops: `signal`, `token`, `cancel`, `terminate`, `retry`, `wait_fact`, `wait_status`, `assert_live`, `sleep`. Plain `signal` and `cancel` steps hit the root.
- `expect` extras: `descendants` (status, projection, cancel_reason, parent link, result), `chain_runs`, `fx_pay`, `variants`. Expected `"@<path>"` strings are replaced by run ids.
- Facts normalise API-completed tokens to `<token>`.
- Always checked: at most one `ChildCompleted` per op in every journal.

**Egress for the mock:** the 06-17 approach. `[egress] allow_private = ["127.0.0.0/8"]` is set only on the harness config, and the module's egress policy allows exactly `http://127.0.0.1:<mock port>`. No SSRF default is loosened globally.

## Per-scenario timings (Rust, debug build, 3 consecutive runs identical within ±20 ms)

| Scenario | Time | Notes |
|----------|------|-------|
| C-CHILD-01 | 0.22 s | includes first deploy |
| C-CHILD-02 | 0.01 s | |
| C-CHILD-03 | 2.05 s | 2 s "still running after parent close" pause |
| C-CHILD-04 | 1.03 s | parent sleeps 1 s before cancel-op |
| C-CHILD-05 | 1.03 s | parent sleeps 1 s before returning |
| C-CHILD-06 | 2.16 s | terminate + run_timeout 2 s variant |
| C-CHILD-07 | 0.03 s | |
| C-CHILD-09 | 0.03 s | 3 levels cancelled well inside 10 s |
| C-CHILD-10 | 2.20 s | Cancelled + child_terminated variants (2 s shielded child) |
| C-CHILD-11 | 1.04 s | |
| C-SIG-API-01 | 0.20 s | |
| C-TOKEN-API-01 | 0.03 s | |
| C-CAN-02 | 0.02 s | |
| C-FX-01 | 0.13 s | |

The whole `server_leg` binary takes 6.3 s with `--test-threads=2`. The slowest test (`children_resume_shielding` / `children_lifecycle`) is under 5 s.

## Scenario notes (the reviewed expectations)

- **C-CHILD-03:**
  - The op result is `{"run_id": "@child"}` (the deterministic id).
  - The child has no parent link.
  - Child status after root Completed plus 2 s is still live.
  - The cleanup cancel uses reason `conformance_cleanup`, and that is the child's first CancelRequested, so no parent_closed reached it.
- **C-CHILD-04:** the root projection is `child#1, timer#1, cancel-op#1, terminal:completed`. The child is cancelled with reason `parent_cancel_op` and its `step:cleanup` ran.
- **C-CHILD-05:** the child is cancelled with reason `parent_closed`, and cleanup ran.
- **C-CHILD-06:** terminate gives `{"caught":"child_terminated"}` (child `terminated`). The `run_timeout_ms: 2000` variant gives `{"caught":"timeout"}` (child `timed_out`).
- **C-CHILD-07:** the child chain projects `continued_as_new, continued_as_new, completed`, and the parent has exactly one ChildCompleted.
- **C-CHILD-09:** root `cancelled`. Mid and leaf are both `cancelled`, and the leaf ran `step:cleanup`.
- **C-CHILD-10:** the root projection is `child#1, terminal:terminated, terminal:completed`. A resumed run's journal continues after `RunTerminated`; this is the real RetryRun-resume shape.
  - Base case gives `{"caught":"cancelled"}`.
  - Variant: `conf_leaf_echo` with a 2 s *shielded* delay ignores the cascade's cancel and completes while the parent is terminated, which gives `{"caught":"child_terminated"}`.
  - Exactly one ChildCompleted in both cases.
  - W-b (retry after the retention window) is not re-tested here; it is covered by 09-08's synthetic test.
- **C-CHILD-11:** the result is `{"first":"cancelled","cleanup_child":true}`.
  - The first child is cancelled by the 05-08 cancel-request cascade with the user's reason (`conformance`), which shows that shielding does not stop the cascade.
  - The child started in the shielded cleanup runs during the parent's 1 s shielded sleep, and is cancelled with `parent_closed` once the parent closes.
- **C-TOKEN-API-01:** the token is read from `published_status.token` and completed via `/v1/tokens/complete`. The `status:` fact appears as `status:{"token":"<token>"}`.
- **C-CAN-02:** `chain_runs: 3`, and the long-poll answers for the last successor, not the root.
- **C-FX-01:** `mock.charges(idempotency_key_for(root, "pay")) == 1`, the same for requests, and the response `body.key` equals that key. The harness's `idempotency_key_for` is checked against `conformance/vectors/idempotency.json`.

## Deviations from Plan

1. **[Implemented semantics, 09-08 deviation 2] C-CHILD-10 child cancel reason.** The plan says the child receives `CancelRun parent_closed` when the parent is terminated. Hard stops keep their own reason, so the spec asserts `cancel_reason: "conformance"`, the terminate reason passed through REST.
2. **[Rule 1, harness] Chain following.** `continued_to` is cleared on a ContinuedAsNew predecessor once `chain_done` arrives (09-08). So `ServerHarness::chain` falls back to the listed run whose `continued_from` points back. The first C-CHILD-07 run exposed this.
3. **Schema extensions without touching `spec.rs`/SCHEMA.md** (neither is in `files_modified`):
   - Roles are positional (`langs[0..]`) with `wf:`/`wfN:` input refs, instead of a `roles` field.
   - Server-only checks are `expect` extras.
   - Documented in the `server_leg` module docs. 09-16 should use `run_spec(spec, &[parent, child, grandchild])`.
4. **`conf_fx_http` sends the key through the HTTP builder's `idempotency_key(..)`.** The connector sends it as the `Idempotency-Key` header, which is what the mock dedupes on. It is not a hand-set header, which could duplicate the header. Python/TS mirror this, and the projection's `effect:http` args include the key.
5. **Parent `delay_ms`** (default 1000) in `conf_parent_cancel_child`/`conf_parent_close`, so the child is sleeping before cancel-op / close. `conf_leaf_echo` accepts `delay_ms` (shielded sleep) and `conf_leaf_sleep` accepts `ms`. These were needed for C-CHILD-10's child_terminated variant and documented in the contract table.
6. **`bash scripts/ci-local.sh --no-musl` not run** (disk-budget brief). The following ran instead: `ostro-conformance` lib, `spec_catalogue`, `engine_leg` and `server_leg` tests, plus warning-free build and clippy `-D warnings` for `ostro-conformance` and clippy for `conf-rust`. The coordinator / 09-20 gate should run ci-local.

## Server bugs

None. Every scenario passed against the existing children code; no edit to `children.rs`, `derive.rs`, `sink.rs` or `apply/*`.

## Verification

- `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests -p conf-rust`: ok.
- `cargo build -p ostro-conformance --tests --locked`: no warnings.
- `cargo clippy -p ostro-conformance --all-targets --locked -- -D warnings`: clean. `cargo clippy -p conf-rust --target wasm32-wasip2 -- -D warnings`: clean.
- `cargo test -p ostro-conformance --locked --test server_leg -- --test-threads=2`: **7 passed** (3 consecutive runs).
- `ls conformance/spec/server/*.json | wc -l` = 14.
- `cargo test -p ostro-conformance --locked --lib --test spec_catalogue --test engine_leg`: green (lib 12, includes the new server_leg unit tests).

## Open issues / notes

- 09-14/09-15 can run the same specs with `OSTRO_CONF_LANGS=py|ts`. The test file's `cross_compare` compares status and root facts only, because results with run ids differ per run.
- C-SAGA-03 (09-16): `conf_saga_http` is smoke-tested here. 4 keys `a`, `b`, `refund-b`, `refund-a` give one charge each, effects run in that order, and the run fails `book_c_failed`. The spec itself is 09-16's.

## Self-Check: PASSED

- Files exist on main: `crates/ostro-conformance/src/server_leg.rs`, `crates/ostro-conformance/tests/server_leg.rs`, 14 × `conformance/spec/server/*.json`.
- Commits on main: `064922b`, `de97e48`.
