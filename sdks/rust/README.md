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

- The server does not bridge effects yet: `http_fetch` and child ops complete with a
  non-retryable `unsupported` failure until Phase 6.
- Timers, signals and cancellation work end to end. Server-side await-token completion
  and continue-as-new successors land with the rest of Phase 5 (the SDK surface above is
  final; the engine already executes both).
