//! Rust conformance reference workflows, v2 (plan 09-11, SDK-05).
//!
//! Same exports as `conf-rust` (the shared bodies are included from
//! `conf-rust/src/shared.rs`), except the v2 bodies of `conf_patch` and `conf_ndet`; the
//! contract table is in `conf-rust/src/lib.rs`.
//!
//! - `conf_patch` v2: `if ctx.patched("x") { step a2 } else { step a }`, sleep 1 s, step
//!   `b`. A v1 journal takes the old branch (no marker, C-PATCH-01); a fresh run journals
//!   `PatchMarker("x")` and takes the new branch (C-PATCH-02).
//! - `conf_ndet` v2: steps reordered (`b`, sleep 1 s, `a`), so a v1 journal replayed here
//!   is non-determinism (C-NDET-01).

#[path = "../../conf-rust/src/shared.rs"]
mod shared;

use std::time::Duration;

use ostro_sdk::prelude::*;
use serde_json::Value;
pub use shared::*;

#[workflow(name = "rs_conf_patch", version = 1)]
pub fn rs_conf_patch(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let first = if ctx.patched("x") {
        named("a2", "a2")?
    } else {
        named("a", "a")?
    };
    ctx.sleep(Duration::from_secs(1)).wait()?;
    let b = named("b", "b")?;
    Ok(json!([first, b]))
}

#[workflow(name = "rs_conf_ndet", version = 1)]
pub fn rs_conf_ndet(ctx: &Ctx, _input: Value) -> Result<Value, WorkflowError> {
    let b = named("b", "b")?;
    ctx.sleep(Duration::from_secs(1)).wait()?;
    let a = named("a", "a")?;
    Ok(json!([b, a]))
}

ostro_sdk::export!(
    RsConfStepsWorkflow,
    RsConfStepFailWorkflow,
    RsConfStepRngWorkflow,
    RsConfStepSiblingWorkflow,
    RsConfNativeWorkflow,
    RsConfRetryWorkflow,
    RsConfRetryExhaustWorkflow,
    RsConfTimerWorkflow,
    RsConfTimerVsSignalWorkflow,
    RsConfSignalsWorkflow,
    RsConfSignalTimeoutWorkflow,
    RsConfGatherWorkflow,
    RsConfRaceCancelWorkflow,
    RsConfCancelWaitWorkflow,
    RsConfCancelShieldedWorkflow,
    RsConfCancelTimerWorkflow,
    RsConfPatchWorkflow,
    RsConfNdetWorkflow,
    RsConfRandWorkflow,
    RsConfRandNativeWorkflow,
    RsConfValuesWorkflow,
    RsConfCanWorkflow,
    RsConfTokenWorkflow,
    RsConfStatusWorkflow,
    RsConfSagaThreeWorkflow,
    RsConfSagaCancelWorkflow,
);
