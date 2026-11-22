# Ostro Rust SDK

Write durable Ostro workflows in plain Rust. A workflow is a `cdylib` built with

```sh
cargo build --release --target wasm32-wasip2
```

(plain cargo + wit-bindgen `=0.63.0`; no cargo-component) against the WIT world
`ostro:runtime/workflow@0.1.0` (`wit/ostro-runtime-0.1.0/runtime.wit`).

This directory is its own cargo workspace, separate from the server workspace:

```sh
cargo build --target wasm32-wasip2 --manifest-path sdks/rust/Cargo.toml
cargo test  --manifest-path sdks/rust/Cargo.toml -p ostro-sdk   # host-side unit tests
```

## Crates

| Crate | What |
|-------|------|
| `ostro-sdk` | Runtime: `Ctx`, typed durable steps (`run_step`), op handles (`sleep`, `wait_signal`, `create_await_token`, `http_fetch`), `join!` / `select!`, cancellation and `shielded`, continue-as-new, the `Workflow` trait and `export!` registration |
| `ostro-sdk-macros` | `#[workflow]` / `#[step]` sugar over `ostro-sdk` (plan 04-06) |

## Minimal workflow

`Cargo.toml`:

```toml
[package]
name = "greeter"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
ostro-sdk = { path = "../sdks/rust/ostro-sdk" }
serde = { version = "1", features = ["derive"] }
```

`src/lib.rs`:

```rust
use std::time::Duration;
use ostro_sdk::prelude::*;
use serde_json::Value;

#[derive(Deserialize)]
pub struct Input { name: String }

pub struct Greeter;

impl Workflow for Greeter {
    const NAME: &'static str = "greeter";
    type Input = Input;
    type Output = String;

    fn run(ctx: &Ctx, input: Input) -> Result<String, WorkflowError> {
        // A durable step: runs at most once to a recorded outcome; replays return it.
        let greeting: String = run_step("greet", &StepOptions::default(), || {
            Ok(format!("hello, {}", input.name))
        })?;

        // A durable timer; join!/select! wait on several ops at once.
        ctx.sleep(Duration::from_secs(1)).wait()?;

        ctx.set_status(&json!({ "greeted": true }))?;
        ostro_sdk::info!(ctx, "greeted {}", input.name); // suppressed on replay
        Ok(greeting)
    }
}

// Explicit registration (no ctor magic): implements describe() and run(name).
ostro_sdk::export!(Greeter);
```

## Semantics

These are the frozen WIT 0.1 semantics (Phase 5) as the Rust SDK exposes them.

### Steps and errors

- **Steps** (`run_step::<T>(name, &opts, body)`): the output `T` is journaled as
  `application/json`. On replay the recorded outcome is decoded and the body is not run.
  When the host answers a retryable failure with `retry-now`, the body is re-run in the
  same activation (the closure is `FnMut`). The live return value is decoded from the
  journaled bytes, so it is identical to what replay returns.
- **Open-step rule (PF-W1)**: while a step body runs live, every journaled call (another
  step, any effect including `wait`, `ctx.patched`, `ctx.set_status`) is a protocol
  violation that fails the activation. Reads (`ctx.info()`, `ctx.now()`, `random_*`,
  `uuid_v7`, `is_replaying`, `continue_as_new_suggested`) and logging are allowed.
- **Errors**: `StepError { kind, message, retryable }`. Kinds `timeout` and `cancelled`
  are reserved for the host's `failure::timeout` / `failure::cancelled`. A recorded
  payload that cannot be decoded is a non-retryable `invalid_json` error.
- **Payloads (PF-W5)**: inputs, outputs, step results, signal and token payloads are
  `application/json`, UTF-8 without a BOM. An empty payload decodes as JSON `null`, so
  `()` and `Option<T>` inputs accept an empty body.

### Determinism

- Use `ctx.now()`, `ctx.random_u64()`, `ctx.random_bytes(n)` and `ctx.uuid_v7()`. Rust std
  is also deterministic inside Ostro (`SystemTime::now()` = logical time, std `HashMap`
  and `rand` seeded from the journal through the host WASI shim; `Instant` = logical time
  since the run was accepted). Real I/O belongs in steps or effects.
- **Random streams (PF-W9)**: `random_u64` and `random_bytes` share one stream;
  `uuid_v7` draws from its own. Adding a `uuid_v7()` call does not shift later
  `random_u64()` values, and vice versa.
- **Step-local RNG (D7)**: randomness drawn while a step body runs live comes from a
  stream keyed on that step's journal seq (`sha256("ostro/step-rng/v1" || seed || seq)`),
  not from the run-level streams. Each retry attempt of a step gets a fresh stream, and
  values drawn outside steps are the same whether earlier steps ran live or replayed.
- **`ctx.info().attempt` (F4)** is the activation attempt. It increases on every
  re-dispatch (failover, lease expiry, worker loss, restart) and is **not** stable across
  replays. Use it for logging and metrics only, never for control flow.

### Versioning: `ctx.patched(change_id)` (D6)

- The first call for a change id journals a patch marker in new runs and returns `true`;
  a run recorded before the patch existed returns `false` and takes the old path.
- Memoised per change id: later calls with the same id return the first answer.
- Markers the code never queries are skipped on replay, so a `patched` check can be
  deleted once no run that needs the old path is still open.

### Waits, signals and await tokens

- An effect call returns an `Op<T>` and journals its scheduling event; nothing blocks
  until `op.wait()`, `join!` (all) or `select!` (first). Completions come back in journal
  order. `select!` borrows its arguments, so losers can still be waited on or cancelled
  with `ctx.cancel(&op)`; waiting on a cancelled op returns `Err(cancelled)` unless it had
  already completed (the first completion in journal order wins).
- **Signals are FIFO** (`ctx.wait_signal::<T>(name, timeout)`): one signal satisfies one
  wait. Signals sent before any wait are buffered per name in arrival order, and several
  waits on one name are served oldest op first. A timeout completes the op with
  `Err(timeout)`.
- **Await tokens** (`ctx.create_await_token::<T>(timeout)`): returns an `AwaitToken<T>`
  with `.token()` (hand it to the external system, which presents it to
  `POST /v1/tokens/complete`) and `.op()` (wait on it; the payload is JSON-decoded into
  `T`). Tokens are **one-shot**: the first completion wins and later completions are
  rejected. A timeout completes the op with `Err(timeout)`. The token is a bearer
  credential; keep it out of URLs and logs.
- **Signal and token waits always evict** the activation (it is unloaded and resumed by
  replay when the input arrives); they are never hot-parked, whatever the timeout. Only
  short timers and in-flight effects may be hot-parked.

### Cancellation (D11) and `shielded`

- A graceful cancel (`POST /v1/runs/{id}/cancel`) completes the run's cancellation op.
  Every SDK wait observes it by default: `op.wait()` issues
  `wait([op, CANCELLATION], any)` and returns `Err(StepError::cancelled())` if the cancel
  is journaled before the op completes. `join!` resolves every op still pending at the
  cancel to `Err(cancelled)`; `select!` returns index `N` (the argument count) with the
  cancellation completion, which decodes as `Err(cancelled)` with any op.
- Propagating it with `?` ends the run as **Cancelled**. An op that completed before the
  cancel still returns its own result.
- Cleanup and compensation go inside `ctx.shielded(|| ...)`: waits there use the plain
  op list and run to completion after a cancel. Sections nest.

  ```rust
  match ship(ctx, &order) {
      Err(e) if e.is_cancelled() => {
          ctx.shielded(|| refund(ctx, &order))?;   // runs even though the run is cancelled
          Err(e.into())
      }
      r => r.map_err(Into::into),
  }
  ```
- Waiting on `ctx.cancellation()` directly, or passing it to `select!`, handles the cancel
  explicitly; such waits are not wrapped a second time.
- The cancellation op is part of each journaled wait, so moving a wait into or out of a
  `shielded` section is a code change like any other (use `patched` for running
  workflows).
- Terminate and the run timeout are hard stops: no guest code runs, shielded or not.

### Continue-as-new

- `return Err(ctx.continue_as_new(&next_input));` ends the run as **ContinuedAsNew** and
  starts a fresh run of the same workflow with `next_input` and an empty journal. To
  switch workflow or pin a version:
  `Err(ContinueAsNew::new(&input)?.workflow("other").version(2).into())`.
- The request travels through `Err` so `?` can short-circuit from helpers, but it is not
  a failure: the SDK maps it to `run-result::continue-as-new`.
- `ctx.continue_as_new_suggested()` turns true when the journal is large enough that the
  workflow should continue. The successor sees the old run id in
  `ctx.info().continued_from`.

### Current server limitations

- None for this SDK surface since Phase 9: effects are bridged (Phase 6), await tokens and
  continue-as-new run end to end (Phase 5), and child workflows run on the server
  (Phase 9, below). The note that used to stand here predates those phases.

## Children, saga, random_f64

Added in Phase 9 (plan 09-06). The same surface exists in the Python and TypeScript SDKs,
and all three pass the same conformance scenarios ([docs/conformance.md](../../docs/conformance.md)).

### Child workflows (WF-06)

```rust
use ostro_sdk::prelude::*;
use serde_json::Value;

// Attached: the op completes with the child's output.
let out: Value = ctx
    .start_child("charge", &input, ChildOptions::new().queue("payments").version(2))
    .wait()?;

// Detached: completes with the child's run id once the start is flushed.
let run: ChildRun = ctx.start_child("audit", &input, ChildOptions::new().detached()).wait()?;

// cancel-op on a child (D5): the op ends Err(cancelled) and the child gets a graceful cancel.
let op = ctx.start_child::<Value, _>("long", &input, ChildOptions::new());
ctx.cancel(&op);
assert!(op.wait().unwrap_err().is_cancelled());
```

- `ChildOptions::new()` with `queue(..)`, `version(..)`, `detached()` and
  `run_timeout(Duration)`. Without them the child runs the workflow's Active version on its
  default queue, inheriting the tenant and trace context. `try_start_child` returns a
  non-retryable `invalid_json` error (and starts nothing) when the input cannot be
  JSON-encoded; `start_child` panics in that case.
- The child's run id is derived from (parent run, op) (D-P9-7), so a re-delivered start
  never creates a second child.
- **Outcome mapping (PF-W7):** Completed → `Ok(output)`; Failed → `Err(<the child's
  failure>)`; Cancelled → `Err(cancelled)`; TimedOut → `Err(timeout)`; Terminated →
  `Err(non-retryable{kind: "child_terminated"})`, recognised by
  `StepError::is_child_terminated()` (`ostro_sdk::CHILD_TERMINATED`). A child that
  continues-as-new is followed to the last run of its chain.
- When the parent reaches any terminal status, every attached child still running gets a
  graceful cancel (reason `parent_closed`). Detached children are never cancelled with the
  parent. A shielded wait on an in-flight child still returns `Err(cancelled)` when the
  parent is cancelled, because the child run itself is cancelled (the cancel cascade beats
  shielding, D-P9-26).
- An unknown child workflow or version fails the op with non-retryable
  `unknown_workflow`.

Full semantics, including resume after close and continue-as-new chains:
[docs/children.md](../../docs/children.md).

### Saga (WF-10)

```rust
let mut saga = Saga::new(ctx);
book_a(ctx)?;
saga.add("cancel_a", |ctx| cancel_a(ctx));
book_b(ctx)?;
saga.add("cancel_b", |ctx| cancel_b(ctx));
if let Err(e) = book_c(ctx) {
    return Err(saga.fail_with(e)); // runs cancel_b, then cancel_a, re-raises e with details
}
```

- `compensate()` runs the compensations sequentially, last registered first (LIFO),
  inside `ctx.shielded`, so a cancelled workflow still compensates. It continues past a
  failing compensation and returns a `SagaReport { compensated, failures }`.
- `fail_with(err)` compensates and returns `err` with
  `{"compensated": [...], "compensation_failures": [{"compensation", "error": {kind,
  message, retryable}}]}` in its details (`WorkflowError::with_details` / `details()`);
  earlier details stay under `"details"`. Parallel compensation is out of scope.

### `random_f64` and cross-SDK values

- `ctx.random_f64()` is `(random_u64() >> 11) as f64 * 2^-53` (`ostro_sdk::f64_from_u64`),
  the same mapping as Python `ctx.random_f64()` and TS `ctx.randomF64()`. Inside a step it
  draws from the step-local stream (D7).
- `idempotency_key_for(run_id, name)` is the P-11 key that `ctx.idempotency_key(name)`
  uses. `ostro_sdk::codec::{encode_json, decode_json}` expose the SDK's JSON codec.
- The vectors in `conformance/vectors/` (`idempotency.json`, `random_f64.json`,
  `json.json`) pin these functions for all three SDKs (`cargo test --manifest-path
  sdks/rust/Cargo.toml -p ostro-sdk --test vectors`). For the same seed and run id,
  `random_u64`, `random_f64`, `uuid_v7` and `idempotency_key` are byte-identical across
  Rust, Python and TS (C-VAL-01).
- The crate vendors the frozen WIT at `ostro-sdk/wit/runtime.wit` (D-P9-29), checked
  byte-identical by `tests/wit_copy.rs`.
