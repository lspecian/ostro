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
| `ostro-sdk` | Runtime: `Ctx`, typed durable steps (`run_step`), op handles (`sleep`, `wait_signal`, `http_fetch`), `join!` / `select!`, the `Workflow` trait and `export!` registration |
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

- **Steps** (`run_step::<T>(name, &opts, body)`): the output `T` is journaled as
  `application/json`. On replay the recorded outcome is decoded and the body is not run.
  When the host answers a retryable failure with `retry-now`, the body is re-run in the
  same activation (the closure is `FnMut`). The live return value is decoded from the
  journaled bytes, so it is identical to what replay returns.
- **Errors**: `StepError { kind, message, retryable }`. Kinds `timeout` and `cancelled`
  are reserved for the host's `failure::timeout` / `failure::cancelled`. A recorded
  payload that cannot be decoded is a non-retryable `invalid_json` error.
- **Determinism**: use `ctx.now()`, `ctx.random_u64()`, `ctx.uuid_v7()`. Rust std is also
  deterministic inside Ostro (`SystemTime::now()` = logical time, std `HashMap` and
  `rand` seeded from the journal via the host WASI shim; `Instant` = logical time since
  the run was accepted). Real I/O belongs in steps or effects.
- **Phase 4 limitation**: the single-node server does not bridge effects yet;
  `http_fetch` (and child/await-token ops) complete with a non-retryable
  `unsupported` failure until Phase 6. Timers and signals work.
