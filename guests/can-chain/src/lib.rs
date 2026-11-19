//! Continue-as-new chain guest (plan 05-10): workflow `can-chain` v1.
//!
//! Input `{"n": u64, "max": u64}`. While `n < max` the run ends with continue-as-new into
//! `{"n": n + 1, "max": max}` (same workflow, same version); once `n >= max` it completes
//! with output `n`. A chain started at `{"n": 0, "max": k}` therefore produces `k`
//! `ContinuedAsNew` runs followed by one run completing with `k`.

use ostro_sdk::prelude::*;

/// `can-chain` input (also the continuation input).
#[derive(Debug, Serialize, Deserialize)]
pub struct Input {
    pub n: u64,
    pub max: u64,
}

#[workflow(name = "can-chain", version = 1)]
pub fn can_chain(ctx: &Ctx, input: Input) -> Result<u64, WorkflowError> {
    if input.n < input.max {
        return Err(ctx.continue_as_new(&Input {
            n: input.n + 1,
            max: input.max,
        }));
    }
    Ok(input.n)
}

ostro_sdk::export!(CanChainWorkflow);
