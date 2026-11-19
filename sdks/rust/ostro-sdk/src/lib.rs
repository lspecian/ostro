//! # ostro-sdk
//!
//! Rust guest SDK for [Ostro](https://github.com/) durable workflows. Workflows are plain
//! Rust compiled with `cargo build --target wasm32-wasip2` (wit-bindgen, no
//! cargo-component) against the WIT world `ostro:runtime/workflow@0.1.0`.
//!
//! ## Determinism
//!
//! A workflow body is re-executed from the start on replay; every non-deterministic input
//! must come from the journal. Use [`Ctx`] for time, randomness and ids. Rust std is also
//! deterministic inside Ostro: `SystemTime::now()` returns logical (journaled) time, std
//! `HashMap` seeds and `rand`/`getrandom` draw from the run's journaled seed through the
//! host's deterministic WASI shim (WF-07), and `Instant` is logical time since the run was
//! accepted. Real I/O must go through durable steps or effects.
//!
//! ## Cancellation
//!
//! Waits observe a graceful cancel by default and return `Err(StepError::cancelled())`
//! (D11); cleanup code runs inside [`Ctx::shielded`]. See [`ops`] and the SDK README.

#![warn(missing_docs)]

#[doc(hidden)]
#[allow(missing_docs, clippy::all)]
pub mod bindings;

pub mod ctx;
pub mod error;
pub mod ops;
pub mod registry;
pub mod step;

pub use ctx::{AwaitToken, Ctx, Header, HttpRequest, HttpResponse, Level, RunInfo};
pub use error::{ContinueAsNew, StepError, WorkflowError};
pub use ops::{CANCELLATION, Completion, Op, OpId, is_cancellation};

#[doc(hidden)]
pub use serde as __serde;
#[doc(hidden)]
pub use serde_json as __serde_json;

pub use registry::Workflow;

/// `#[workflow(name = "...", version = N, ...)]` on a
/// `fn(ctx: &Ctx, input: In) -> Result<Out, E>` generates a unit struct named after the
/// function in UpperCamelCase plus `Workflow` (`fn order` -> `OrderWorkflow`) that
/// implements [`Workflow`]; register it with [`export!`]. See `ostro-sdk-macros`.
pub use ostro_sdk_macros::workflow;

/// `#[step(...)]` turns a `fn(args) -> Result<T, StepError>` into a durable step: calling
/// it runs (or replays) the body through [`run_step`] with the declared options. See
/// `ostro-sdk-macros`.
pub use ostro_sdk_macros::step;
pub use step::{RetryPolicy, StepOptions, StepOptionsBuilder, run_step};

/// Everything a workflow module usually needs: `use ostro_sdk::prelude::*;`.
pub mod prelude {
    pub use crate::ctx::{AwaitToken, Ctx, Level, RunInfo};
    pub use crate::error::{ContinueAsNew, StepError, WorkflowError};
    pub use crate::ops::{Completion, Op};
    pub use crate::registry::Workflow;
    pub use crate::step::{StepOptions, run_step};
    pub use crate::{join, select};
    pub use ostro_sdk_macros::{step, workflow};
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::json;
}
