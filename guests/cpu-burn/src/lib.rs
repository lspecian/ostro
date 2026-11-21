//! Phase 7 test guest (plan 07-07, D-W15e): CPU load for spill tests (WORK-04).
//!
//! Workflow `cpu-burn` v1. Input `{"ms": u64, "iters_per_ms": u64?}`: one step `burn` runs
//! a busy loop of `ms × iters_per_ms` iterations (default 250,000 per ms, roughly a
//! millisecond of CPU each on current hardware; fuel-limited like any guest code).
//!
//! Output: `{"started_ms", "ended_ms", "iters"}`. The times are the run's deterministic
//! (journaled) clock read before and after the step, so e2e tests measure execution
//! intervals from run results instead of non-journaled grants (W3).

use std::time::UNIX_EPOCH;

use ostro_sdk::prelude::*;

/// Default busy-loop iterations per requested millisecond.
const ITERS_PER_MS: u64 = 250_000;

/// `cpu-burn` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub ms: u64,
    #[serde(default)]
    pub iters_per_ms: Option<u64>,
}

/// `cpu-burn` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub started_ms: u64,
    pub ended_ms: u64,
    pub iters: u64,
}

#[step]
fn burn(iters: u64) -> Result<u64, StepError> {
    let mut x: u64 = 0;
    for i in 0..iters {
        x = std::hint::black_box(x.wrapping_mul(31).wrapping_add(i));
    }
    Ok(std::hint::black_box(x) & 1 | iters)
}

fn ms(ctx: &Ctx) -> u64 {
    ctx.now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[workflow(name = "cpu-burn", version = 1)]
pub fn cpu_burn(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let started_ms = ms(ctx);
    let iters = input
        .ms
        .saturating_mul(input.iters_per_ms.unwrap_or(ITERS_PER_MS));
    let _ = burn(iters)?;
    let ended_ms = ms(ctx);
    Ok(Output {
        started_ms,
        ended_ms,
        iters,
    })
}

ostro_sdk::export!(CpuBurnWorkflow);
