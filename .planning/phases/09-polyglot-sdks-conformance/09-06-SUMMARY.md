---
phase: 09-polyglot-sdks-conformance
plan: 06
subsystem: sdks
tags: [rust-sdk, children, saga, random_f64, vectors, wit-vendoring, wave-2]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-01 PHASE-9 GATE (P9 names child-parent/child-leaf/sdk-saga, child_terminated; sdks/rust/** owned by 09-06)"
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-06 SDK cancellation surface (shielded, Op::wait observing CANCELLATION, continue-as-new)"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-09 effects.rs + idempotency_key (P-11)"
provides:
  - "Rust SDK child workflows: ChildOptions, ChildRun, CHILD_TERMINATED, Ctx::start_child / try_start_child"
  - "Rust SDK Saga helper (LIFO, shielded, continues past failures, details on the error)"
  - "WorkflowError::with_details / details() (JSON error-info.details reach the host)"
  - "Ctx::random_f64 + pure f64_from_u64; pure idempotency_key_for; ostro_sdk::codec::{encode_json, decode_json}"
  - "sdks/rust/ostro-sdk/wit/runtime.wit vendored (bindings path \"wit\"), guarded by tests/wit_copy.rs"
  - "conformance/vectors/{idempotency,random_f64,json}.json + README with the generator"
  - "guests child-parent, child-leaf, sdk-saga; crates/ostro-engine/tests/sdk_children_saga.rs (14 tests)"
affects: [09-11, 09-12, 09-13, 09-16, 09-20]
tech-stack:
  added: []
  patterns:
    - "Cross-SDK vectors: {version:1, cases:[{input, expected}]}; u64/f64 bits as strings for JS; hex bytes authoritative"
    - "Child-workflow parents take the child workflow name from their input (cross-language chains reuse them)"
key-files:
  created:
    - sdks/rust/ostro-sdk/wit/runtime.wit
    - sdks/rust/ostro-sdk/src/children.rs
    - sdks/rust/ostro-sdk/src/saga.rs
    - sdks/rust/ostro-sdk/tests/wit_copy.rs
    - sdks/rust/ostro-sdk/tests/vectors.rs
    - conformance/vectors/README.md
    - conformance/vectors/idempotency.json
    - conformance/vectors/random_f64.json
    - conformance/vectors/json.json
    - guests/child-parent/Cargo.toml
    - guests/child-parent/src/lib.rs
    - guests/child-leaf/Cargo.toml
    - guests/child-leaf/src/lib.rs
    - guests/sdk-saga/Cargo.toml
    - guests/sdk-saga/src/lib.rs
    - crates/ostro-engine/tests/sdk_children_saga.rs
  modified:
    - sdks/rust/ostro-sdk/src/bindings.rs
    - sdks/rust/ostro-sdk/src/ctx.rs
    - sdks/rust/ostro-sdk/src/effects.rs
    - sdks/rust/ostro-sdk/src/lib.rs
    - sdks/rust/ostro-sdk/src/error.rs
key-decisions:
  - "Saga details shape: {\"compensated\": [names that succeeded, in run order], \"compensation_failures\": [{compensation, error:{kind, message, retryable}}]}; earlier error details kept under \"details\""
  - "WorkflowError gained private JSON details (with_details/details); error.rs edit is outside files_modified but inside 09-06's sdks/rust/** ownership"
  - "start_child returns Op<T> (plan signature) and panics on unencodable input; try_start_child returns Result for callers that want the error"
  - "idempotency_key_for is the canonical pure function; the 06-09 idempotency_key stays as an alias"
  - "ostro_sdk::codec::{encode_json, decode_json} made public so the JSON vectors test the SDK's own codec, not serde_json directly"
requirements-completed: [WF-06, WF-10, SDK-04]
duration: ~1 h
completed: 2026-11-21
---

# Phase 9 Plan 06: Rust SDK children, saga, random_f64 and cross-SDK vectors Summary

**The Rust reference SDK can now start, await, detach and cancel child workflows (PF-W7 contract), compensate with a LIFO shielded saga, and draw `random_f64`. Three independently generated vector files pin the idempotency key, `random_f64` and JSON encodings for the Python and TS SDKs. The SDK binds a vendored, byte-identical copy of the frozen WIT.**

## Performance

- 3 tasks, 3 task commits, plus the docs commit for this SUMMARY.
- Started 2026-11-21 ~19:55Z.

## Public API added (ostro-sdk)

```rust
// children.rs (re-exported at the crate root; ChildOptions/ChildRun also in prelude)
pub const CHILD_TERMINATED: &str = "child_terminated";
pub struct ChildOptions { pub queue: Option<String>, pub version: Option<u32>, pub detached: bool, pub run_timeout: Option<Duration> }
impl ChildOptions { pub fn new() -> Self; pub fn queue(self, impl Into<String>) -> Self; pub fn version(self, u32) -> Self; pub fn detached(self) -> Self; pub fn run_timeout(self, Duration) -> Self }
pub struct ChildRun { pub run_id: String }            // Serialize + Deserialize
impl StepError { pub fn is_child_terminated(&self) -> bool }

// ctx.rs
impl Ctx {
    pub fn start_child<T: DeserializeOwned, I: Serialize + ?Sized>(&self, workflow: &str, input: &I, opts: ChildOptions) -> Op<T>;
    pub fn try_start_child<T: DeserializeOwned, I: Serialize + ?Sized>(&self, workflow: &str, input: &I, opts: ChildOptions) -> Result<Op<T>, StepError>;
    pub fn random_f64(&self) -> f64;
}
pub fn f64_from_u64(u: u64) -> f64;                   // (u >> 11) as f64 * 2^-53

// effects.rs (re-exported at the crate root)
pub fn idempotency_key_for(run_id: &str, name: &str) -> String;   // P-11, 38 chars; idempotency_key = alias

// lib.rs
pub mod codec { pub fn encode_json<T: Serialize + ?Sized>(&T) -> Result<Vec<u8>, StepError>; pub fn decode_json<T: DeserializeOwned>(&[u8]) -> Result<T, StepError>; }

// saga.rs (Saga also in prelude)
pub struct Saga<'a>;
impl<'a> Saga<'a> {
    pub fn new(ctx: &'a Ctx) -> Self;
    pub fn add(&mut self, name: impl Into<String>, f: impl FnOnce(&Ctx) -> Result<(), WorkflowError> + 'a);
    pub fn len(&self) -> usize; pub fn is_empty(&self) -> bool;
    pub fn compensate(self) -> SagaReport;            // LIFO, inside ctx.shielded, past failures
    pub fn fail_with(self, err: impl Into<WorkflowError>) -> WorkflowError;  // compensate + details
}
pub struct SagaReport { pub compensated: Vec<String>, pub failures: Vec<CompensationFailure> }  // is_clean(), to_json()
pub struct CompensationFailure { pub compensation: String, pub error: WorkflowError }

// error.rs
impl WorkflowError { pub fn with_details<T: Serialize + ?Sized>(self, &T) -> Self; pub fn details(&self) -> Option<serde_json::Value> }
```

`Ctx::saga()` was not added. `Saga::new(ctx)` is enough.

## Vectors

- Files: `conformance/vectors/idempotency.json` (12 cases), `random_f64.json` (15 cases) and `json.json` (26 cases: 17 encode, 4 encode_f64, 5 decode).
- The generator snippet (Python stdlib only, run with `uv run --no-project --python 3.12 python -I gen_vectors.py conformance/vectors`) is in **`conformance/vectors/README.md`**, together with the format.
- Re-running the generator as extracted from the README reproduces all three files byte for byte.
- `idempotency_key_for` lives in `sdks/rust/ostro-sdk/src/effects.rs` and is re-exported as `ostro_sdk::idempotency_key_for`. `Ctx::idempotency_key` calls it.

## Guests (workflow names and inputs)

| Guest | Workflow | Input | Result |
|-------|----------|-------|--------|
| child-parent | `parent_attached` | `{"workflow", "input"?, "queue"?, "version"?, "run_timeout_ms"?}` (same for all parents) | `{"child": <output>}`; a child error fails the run with it |
| child-parent | `parent_fail_child` | as above | `{"caught": "<kind>"}` or `{"ok": <output>}` |
| child-parent | `parent_detached` | as above (forced detached) | `{"detached": "<run_id>"}` |
| child-parent | `parent_cancel_child` | as above | `{"caught": "cancelled"}` (`OpCancelled` journaled) |
| child-parent | `parent_close` | as above | `{"started": true}` with no wait |
| child-leaf | `leaf_echo` | any | `{"echo": <input>}` |
| child-leaf | `leaf_fail` | any | fails non-retryable `boom` |
| child-leaf | `leaf_sleep` | any | 1 h sleep. On cancel it runs the shielded step `cleanup` and ends cancelled. Otherwise `{"slept": true}` |
| child-leaf | `leaf_can` | `{"n", "generation"?}` | continue-as-new `n-1`/`generation+1` until 0, then `{"done": true, "generation": k}` |
| sdk-saga | `saga_three` | any | fails `book_c_failed` after `cancel_b` and `cancel_a` run, details `{"compensated": ["cancel_b","cancel_a"], "compensation_failures": []}` |
| sdk-saga | `saga_comp_fails` | any | fails `book_c_failed`; `cancel_b` fails, `cancel_a` runs; the details list the failure |
| sdk-saga | `saga_cancelled` | any | after a cancel during the 1 h sleep, `cancel_b` and `cancel_a` run shielded, then the run is cancelled |
| sdk-saga | `saga_ok` | any | `{"booked": ["a","b","c"], "pending_compensations": 2}` |

## Task Commits

1. **Task 1: vendored WIT, vectors, random_f64, idempotency_key_for**: `f546238`
2. **Task 2: start_child + ChildOptions, child guests, engine tests**: `044c33b`
3. **Task 3: Saga helper, sdk-saga guest, engine tests**: `0ade145`

## Verification

- **Task 1:**
  - `cmp` of the vendored WIT passes.
  - No `../../../wit` is left in `bindings.rs`.
  - `--test wit_copy`: 2 passed. `--test vectors`: 3 passed.
  - The three vector files pass the version/case-count check.
  - The `sdk-basic` wasm sha256 is the same before and after the bindings path change: `ff50b435…a86d7`.
- **Task 2:**
  - `child-parent` and `child-leaf` build for wasm32-wasip2.
  - `cargo test -p ostro-engine --locked --test sdk_children_saga -- children`: `test result: ok. 10 passed`.
  - SDK clippy `-D warnings` is clean.
- **Task 3:**
  - `sdk-saga` builds.
  - `-- saga`: `test result: ok. 4 passed`.
  - The full `sdk_children_saga` run: `14 passed`.
  - `cargo test --manifest-path sdks/rust/Cargo.toml` is green (lib 35, effects 9, vectors 3, wit_copy 2, macros 7 + trybuild 1).
  - SDK clippy (host and wasm32-wasip2) with `-D warnings` is clean.
- **09-02's `ostro-conformance --test wit_copies`** on main with this plan's copy: 4 passed, including `rust_sdk_copy_is_byte_identical_when_present`.
- **Full `cargo test -p ostro-engine --locked`** on main `0ade145`: EXIT=0. All 29 test binaries report `test result: ok`, 281 tests passed, including `sdk_cancellation`, `sdk_guests`, `determinism` and `phase5_guests`. The full guests workspace build and the order-saga example (fmt, wasm build, clippy `-D warnings`) also passed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing functionality] WorkflowError had no `details`**
- **Found during:** Task 3.
- **Issue:** D-P9-18 requires every compensation failure to be attached to the error's `details`, but `WorkflowError` could not carry WIT `error-info.details`, and `failure_of` always sent `details: None`.
- **Fix:** Added a private JSON `details` field with `with_details` / `details()`, sent to the host as an `application/json` payload on retryable and non-retryable failures. The reserved timeout and cancelled failures have no details field in the WIT.
- **Files modified:** `sdks/rust/ostro-sdk/src/error.rs`. This file is not in `files_modified`, but `sdks/rust/**` is 09-06-only per 09-DECISIONS.
- **Commit:** `0ade145`

**2. [Rule 3 - Blocking] Public JSON codec entry points**
- **Found during:** Task 1.
- **Issue:** The JSON vectors must test the SDK's own encoder and decoder, but `encode_value` and `decode_payload` are `pub(crate)`, and `tests/vectors.rs` is an integration test.
- **Fix:** Added `ostro_sdk::codec::{encode_json, decode_json}` in `lib.rs`. They are thin wrappers around the existing codec.
- **Commit:** `f546238`

### Other notes
- **TDD:** the vector and engine tests were written together with the code, and each passed on its first run. There are no separate RED commits; each task is one `feat` commit, as the plan's "Commit each task separately" asks.
- **Extra tests beyond the behaviour lines:**
  - option mapping (queue/version/run_timeout) and empty child output → `null`;
  - a child failure propagated through `?` fails the parent run;
  - `leaf_*` behaviour, including the shielded `cleanup` after a cancel and the `leaf_can` continuation input;
  - SDK unit tests for children decoding, saga LIFO/shielded/continue-past-failure, and error details.
- **Gate:** `bash scripts/ci-local.sh --no-musl` (about 1 h 45 min) was not run, because four other Phase 9 executors and a CI fix agent were running in parallel.
  - Instead, the ci-local steps this plan touches were run one by one: SDK fmt + clippy (host and wasm32-wasip2) + test + wasm build, the full guests build, order-saga, root `cargo fmt --check`, clippy `-D warnings` on the new engine test target, the full `ostro-engine` suite, and `ostro-conformance --test wit_copies`.
  - The plan's end gate, ci-local, is left to the phase gate (09-20) or the orchestrator.

## Issues Encountered
None.

## Next Phase Readiness
- 09-13 (children e2e) can use the `child-parent`/`child-leaf` guests and `start_child` as they are. The parents take the child workflow name from their input, so they work for cross-language chains.
- 09-12 (Python/TS parity) loads `conformance/vectors/*.json` and mirrors `Saga` (details shape above), `random_f64` and `ChildOptions`.
- 09-11/09-16 (conformance) can rely on the `sdk-saga` journal order and failure details.

## Self-Check: PASSED

All 16 created files are present on main. The task commits f546238, 044c33b and 0ade145 are ancestors of main.
