---
phase: 09-polyglot-sdks-conformance
plan: 11
subsystem: conformance-harness
tags: [conformance, engine-leg, projection, canonical-json, scheduler, rust-reference, wave-3]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-02 ostro-conformance crate (spec model, catalogue, polyglot::build), catalogue.json + SCHEMA.md"
  - phase: 09-polyglot-sdks-conformance
    provides: "09-06 Rust SDK Saga, random_f64, idempotency_key_for; sdk-saga shapes"
provides:
  - "ostro_conformance::canon (canonical JSON), projection (journal -> facts/values/inputs), compare (diffs, byte-identical journals)"
  - "ostro_conformance::langs: re-exported spec::Lang, OSTRO_CONF_LANGS selection (polyglot-gated), rs_/py_/ts_ naming, module_for"
  - "ostro_conformance::engine_leg::{run_spec, execute, cross_compare}: engine-leg scheduler with strict per-language checks"
  - "guests conf-rust / conf-rust-v2: 26 reference workflows rs_conf_* (contract table in conf-rust/src/lib.rs)"
  - "conformance/spec/engine/*.json for all 27 engine scenarios; Rust passes 27/27"
affects: [09-13, 09-14, 09-15, 09-16, 09-17, 09-18, 09-20]
tech-stack:
  added: []
  patterns:
    - "harness owns time: settable clock + hot_wait 0, so every step backoff is a StepBackoff eviction and every timer is fired by the scheduler"
    - "one Runtime per language class, modules prepared once per process, scenarios serialised on a process lock"
    - "shared guest bodies via #[path] include (conf-rust/src/shared.rs) so v2 differs only where the scenario needs it"
key-files:
  created:
    - crates/ostro-conformance/src/canon.rs
    - crates/ostro-conformance/src/projection.rs
    - crates/ostro-conformance/src/compare.rs
    - crates/ostro-conformance/src/langs.rs
    - crates/ostro-conformance/src/engine_leg.rs
    - crates/ostro-conformance/tests/harness_selftest.rs
    - crates/ostro-conformance/tests/engine_leg.rs
    - guests/conf-rust/Cargo.toml
    - guests/conf-rust/src/lib.rs
    - guests/conf-rust/src/shared.rs
    - guests/conf-rust-v2/Cargo.toml
    - guests/conf-rust-v2/src/lib.rs
    - conformance/spec/engine/ (27 files)
  modified:
    - crates/ostro-conformance/src/lib.rs
key-decisions:
  - "Projection = ordered facts (the SCHEMA vocabulary, what expect.projection lists) + ordered canonical values (step outcomes, child inputs, cancel-op targets, effect args, terminal outcome) + input multiset keyed by ordinal labels"
  - "Retry timing: the engine runs with hot_wait = 0, so each backoff is a requeue; the scheduler advances the clock to retry_at and attempt k observes max(last ts, retry_at) (D22): conf_retry sees 0, 1000, 3000 ms and succeeds on attempt 3 with succeed_after_ms 2500"
  - "Signals are delivered as the leader does: SignalPayload appended, then SignalMatched pairing open waits (oldest op first) with unmatched payloads (FIFO per name); before_start signals are buffered at seq 1"
  - "Wait timeouts (signal waits, await tokens) are pending timers due at event ts + timeout_ms and complete as WaitTimedOut"
  - "conf_patch / conf_ndet v1 sleep 1 s between steps a and b so swap_module lands mid-run (a complete v1 journal would replay without exercising the branch)"
  - "Run seed: spec seed u64 LE in bytes 0..8, chain generation u32 LE in bytes 8..12; successors of a continue-as-new chain get run id <run id>.<generation>"
  - "C-VAL-01, C-STEP-03 and C-STEP-04 pin the Rust values; C-RAND-01/02 and C-NATIVE-01 assert seed sensitivity (differs_across_seeds) instead"
requirements-completed: [SDK-05, WF-07]
duration: ~1 h 15 min
completed: 2026-11-21
---

# Phase 9 Plan 11: Conformance harness (engine leg) Summary

**The language-neutral conformance harness: canonical JSON, a normalised journal projection with a readable comparator, an engine-leg scheduler on ostro-engine with strict per-language checks, the Rust reference guests and spec files for all 27 engine-leg scenarios. Rust passes 27 of 27.**

## Commits
- `e2bdde8` feat(09-11): canonical JSON, journal projection, comparator and language registry (6 files)
- `48804a4` feat(09-11): Rust conformance guests conf-rust and conf-rust-v2 (5 files)
- `d8d5271` feat(09-11): engine-leg runner, 27 engine spec files, Rust passes 100% (30 files)

## Workflow contract (shared by Rust, Python 09-14 and TS 09-15)

Exported as `rs_<base>` / `py_<base>` / `ts_<base>`. Step names are explicit and language-neutral. "1 h" is 3600 s of workflow time.

| Base name | Input | Behaviour | Output |
|-----------|-------|-----------|--------|
| `conf_steps` | any | steps `a`, `b`, `c` (each returns its own name) | `{"a":"a","b":"b","c":"c"}` |
| `conf_step_fail` | any | step `boom` fails non-retryable `boom`, caught | `{"caught":"boom"}` |
| `conf_step_rng` | any | step `draw` returns `random_u64()` drawn inside the body (step-local stream) as a decimal string | `{"value":"<u64>"}` |
| `conf_step_sibling` | any | step `s`; then the sibling work: ctx `random_u64()` and a 1 s sleep | `{"step":"s","sibling_rand":"<u64>"}` |
| `conf_native` | any | native time, std `HashMap` order (16 keys), `RandomState` hash of `"x"`, in the body and in step `observe` | `{"outside":{..},"inside":{..}}` (per-language values) |
| `conf_retry` | `{"succeed_after_ms":N}` | step `flaky`, retry 3 / 1 s / 60 s / 2.0, fails `transient` while less than N ms of workflow time passed since `ctx.now()` before the step | `{"elapsed_ms":3000}` for N = 2500 |
| `conf_retry_exhaust` | any | step `always`, retry 2 / 1 s, always `transient` | run fails `transient` |
| `conf_timer` | any | sleep 1 h | `{"slept":true}` |
| `conf_timer_vs_signal` | any | select(sleep 1 h, signal `go`), no loser cancel | `{"winner":"signal","payload":<p>}` |
| `conf_signals` | any | wait signal `go` twice | `{"got":[<p1>,<p2>]}` |
| `conf_signal_timeout` | any | wait signal `never`, 10 s timeout | `{"timeout":true}` |
| `conf_gather` | any | join(sleep 1 s, sleep 2 s, signal `go`) | `{"signal":<p>}` |
| `conf_race_cancel` | any | select(sleep 1 h, signal `go`); loser cancelled with cancel-op, its wait gives `Err(cancelled)` | `{"winner":"signal","loser_cancelled":true}` |
| `conf_cancel_wait` | any | sleep 1 h; a run cancel ends the run cancelled | cancelled |
| `conf_cancel_shielded` | any | sleep 1 h; on cancel shielded step `cleanup`, then cancelled | cancelled |
| `conf_cancel_timer` | any | start a 1 h timer, cancel-op it, wait it: `Err(cancelled)` | `{"cancelled_op":true}` |
| `conf_patch` | any | v1: step `a`, sleep 1 s, step `b`; v2: `patched("x") ? a2 : a`, sleep 1 s, `b` | `["a","b"]` / `["a2","b"]` |
| `conf_ndet` | any | v1: `a`, sleep 1 s, `b`; v2: `b`, sleep 1 s, `a` | v1 journal on v2 = non-determinism |
| `conf_rand` | any | 3 x `random_u64()` (decimal), then `uuid_v7()` | `{"u64":[..],"uuid":".."}` |
| `conf_rand_native` | any | `random_bytes(8)` (hex), then `random_f64()` | `{"bytes":"..","f64":<f>}` |
| `conf_values` | any | in order: `random_u64()` (decimal), `random_f64()`, `uuid_v7()`, `idempotency_key("x")` | the four values (C-VAL-01) |
| `conf_can` | `{"n":N,"generation"?:k}` | continue-as-new `n-1`, `generation+1` until 0 | `{"generation":k}` |
| `conf_token` | any | create an await token (no timeout), wait it | `{"token_payload":<p>}` |
| `conf_status` | any | `set_status({"stage":"one"})`, `set_status({"stage":"two","n":2})` | `{"done":true}` |
| `conf_saga_three` | any | `book_a`, `book_b`, `book_c` fails `book_c_failed`; compensations `cancel_b`, `cancel_a` | run fails `book_c_failed` |
| `conf_saga_cancel` | any | `book_a`, `book_b`, sleep 1 h; on cancel `cancel_b`, `cancel_a` shielded | cancelled |

## Projection vocabulary (as implemented)

**Facts** (ordered; `expect.projection`): `step:<name>`, `retry:<name>#<attempt>`, `timer#N`, `signal:<name>#N` (N per name), `token#N`, `child:<base>#N` (prefix stripped, N per base), `cancel-op#N`, `effect:<http|sql|object>#N`, `patch:<id>`, `status:<canonical json>`, `terminal:<completed|failed|cancelled|continued_as_new|terminated>`. A fact is emitted at the command's journal position (scheduling order). Dropped: `RunStarted`, `WaitResolved`, `SignalPayload`, ts, hashes, fuel.

**Values** (ordered, compared across languages unless the scenario is a per-language probe): `step:<name>=<json>` / `step:<name>!<failure>`, `child:<base>#N<-<input json>`, `cancel-op#N-><target label>`, `effect:<kind>#N=<language-neutral args>` (method/url/header names/idempotency key; connection/statement/param count; connection/op/key), `terminal=<output>` / `terminal!<failure>` / `terminal->continue:<input>`.

**Inputs** (multiset): `fired:timer#N`, `timeout:<label>`, `<signal label>=<payload json>` (from `SignalMatched` or `SignalReceived`), `token#N=<json>`, `<child label>=<outcome>`, `<effect label>=<outcome>`, `cancel`.

Rust's re-issued `wait([op, CANCEL])` and the Python/TS remembered cancellation differ only in `WaitResolved` events, which the projection drops (proven in the self-test).

## Engine-leg scheduler (D-P9-22 as implemented)
After each activation: Completed/Failed end the run; ContinuedAsNew starts the successor; NonDeterminism ends with `non_determinism`; `StepBackoff{retry_at}` advances the clock and re-activates; `WaitUnsatisfied` → (1) match buffered signals, (2) deliver the next script step if its precondition holds, (3) fire the earliest pending timer / wait timeout, (4) otherwise fail as stuck. Strict checks: every scenario runs twice and the encoded journals must be byte-identical; every journal of the chain is replayed on the module the run ended on and must append 0 events. The `differs_across_seeds` extra re-runs with seed + 1 and requires a different canonical result. Unknown `expect` keys fail the scenario.

## Per-scenario runtimes (Rust, debug build, final run)

All 27 pass. The whole `engine_leg` binary takes 0.26 s after the guest build; the first scenario per thread pays the module compile (~120 ms).

| Scenario | Status | Activations | Time |
|----------|--------|-------------|------|
| C-STEP-01 / 02 / 03 / 04 | completed | 1 / 1 / 1 / 2 | 4.0 / 4.4 / 4.8 / 5.3 ms |
| C-NATIVE-01 | completed | 1 | 5.6 ms |
| C-RETRY-01 / 02 | completed / failed | 3 / 2 | 6.3 / 5.4 ms |
| C-TIMER-01 / 02 | completed | 2 / 2 | 4.4 / 4.5 ms |
| C-SIG-01 / 02 | completed | 3 / 2 | 6.7 / 5.2 ms |
| C-RACE-01 / 02 | completed | 4 / 2 | 10.8 / 6.4 ms |
| C-CANCEL-01 / 02 / 03 | cancelled / cancelled / completed | 2 / 2 / 1 | 121.5 (compile) / 6.3 / 73.1 (compile) ms |
| C-PATCH-01 / 02 | completed | 3 / 2 | 7.1 / 7.4 ms |
| C-NDET-01 | non_determinism | 2 | 70.8 ms (v2 compile) |
| C-RAND-01 / 02 | completed | 1 / 1 | 6.6 / 5.6 ms |
| C-VAL-01 | completed | 1 | 3.5 ms |
| C-CAN-01 | completed (3-run chain) | 3 | 123.0 ms (compile) |
| C-TOKEN-01 | completed | 2 | 5.0 ms |
| C-STATUS-01 | completed | 1 | 4.1 ms |
| C-SAGA-01 / 02 | failed / cancelled | 1 / 2 | 5.6 / 6.5 ms |

## Spec decisions a reviewer should know
- **C-VAL-01** pins, for seed 42 and run id `01890000-0000-7000-8000-000000000001`: `random_u64` `16357943517170225811`, `random_f64` `0.8693496500947742`, `uuid_v7` `018bcfe5-6800-7e6d-b89c-fc7a01337ce6`, `idempotency_key("x")` `ostro-43ff999e924f58fa3b089b017dab68df`. I checked the key against an independent Python sha256 of the P-11 formula, and `random_f64` against `(u >> 11) * 2^-53` of the second draw.
- **Streams observed:** the uuid stream is separate from `random_u64` (the same uuid follows 3 draws in `conf_rand` and 2 in `conf_values`), and a draw inside a step body does not consume the ctx stream (C-STEP-04's `sibling_rand` is the first ctx draw; C-STEP-03's value differs). Python/TS must reproduce this split.
- **C-NDET-01** expects the projection `["step:a","timer#1"]` (the journal at detection, no terminal), and strict replay is skipped for a non-determinism ending.
- **C-CAN-01** runs a 3-run chain. Its projection is the concatenation: two `terminal:continued_as_new`, then `terminal:completed`.
- **C-TIMER-02** leaves the losing timer unfired. **C-RACE-02** cancels it (cancel-op).

## Verification
- `cargo test -p ostro-conformance --locked --test harness_selftest`: `test result: ok. 23 passed`.
- `cargo test -p ostro-conformance --locked --test engine_leg -- --test-threads=2`: `test result: ok. 16 passed` (27 scenarios).
- `cargo test -p ostro-conformance --locked --test spec_catalogue`: `ok. 4 passed; 1 ignored`. Engine files count 27.
- Full `cargo test -p ostro-conformance --locked`: lib 8, engine_leg 16, harness_selftest 23, spec_catalogue 4 (+1 ignored), wit_copies 4, polyglot_smoke 0 (feature off). All ok.
- `cargo clippy -p ostro-conformance --all-targets --locked -- -D warnings`, with and without `--features polyglot`: clean. `cargo fmt -p ostro-conformance -- --check`: clean.
- Guests: the full `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` (the ci-local step) builds. `cargo clippy` of conf-rust and conf-rust-v2 with `-D warnings` is clean, and rustfmt is clean.
- `OSTRO_CONF_LANGS=rust,py` with `--features polyglot` fails with `py conformance guest .../conformance/guests/python does not exist yet (added by plan 09-14)`. Without the feature, `OSTRO_CONF_LANGS=rust,ts` fails with the `--features polyglot` hint.
- **Not run:** `bash scripts/ci-local.sh --no-musl`. Other Phase 9 executors and a CI fix agent were running in parallel, and this plan touches only the ostro-conformance crate, two new guests and spec files (as in 09-06). The plan-end gate is left to 09-20 or the orchestrator.

## Deviations from Plan
1. **[Rule 3] Extra file `guests/conf-rust/src/shared.rs`.** It holds the shared workflow bodies. `conf-rust-v2` includes it with `#[path]`, so v2 really "re-exports" v1 except `conf_patch`/`conf_ndet`. A cdylib cannot be a dependency of another cdylib without duplicating the component exports. The file is inside the plan-owned guest directory.
2. **[Rule 1 - plan defect] `conf_patch` and `conf_ndet` v1 sleep 1 s between `a` and `b`.** With only steps a, b the v1 run completes in one activation, so `swap_module` could never land mid-run. Replaying a complete journal does not exercise the branch, and a reordered v2 would never even run. v2 keeps the same sleep.
3. **SCHEMA.md was not edited** (it is not in files_modified). The retry timing mechanism and the scheduler rules are documented in `engine_leg.rs`, in the `conf-rust` contract table and here.
4. **[Brief]** The ROADMAP tick and the STATE Current Position line are made in the docs commit, built from the current main blobs.

## Issues Encountered
None blocking. Every spec passed on its first harness run except the four probe results (C-VAL-01, C-STEP-03, C-STEP-04, C-RAND-01). For those I set a deliberately wrong result to read the actual values from the failure report, then reviewed and pinned them (C-RAND-01 stays seed-sensitive, with no pinned result).

## Next Phase Readiness
- 09-14/09-15: add `conformance/guests/{python,python-v2,ts,ts-v2}` mirroring the table, then run `OSTRO_CONF_LANGS=rust,py` (or `ts`, `ts-sm`) with `--features polyglot`. `cross_compare` checks status, canonical result and projection against Rust.
- 09-13/09-16/09-18 can reuse `projection`, `compare`, `canon` and `langs::workflow_name` for the server and crash legs.

## Self-Check: PASSED
- FOUND: all key-files paths above at main (27 spec files under conformance/spec/engine/).
- FOUND: e2bdde8, 48804a4, d8d5271 are ancestors of main.
