//! Perf guest (plan 05-10, used by the 05-16 perf profiles A/B): workflow `trivial-steps` v1.
//!
//! Input `{"n": u32}`. Runs `n` durable steps named `s<i>` (`i` in `0..n`), each returning
//! its index, with no sleeps, signals or effects, so the whole run completes in a single
//! activation. Output: `n` (the number of steps run, checked against the sum of indices).

use ostro_sdk::prelude::*;

/// `trivial-steps` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub n: u32,
}

#[workflow(name = "trivial-steps", version = 1)]
pub fn trivial_steps(_ctx: &Ctx, input: Input) -> Result<u32, WorkflowError> {
    let opts = StepOptions::default();
    let mut sum: u64 = 0;
    for i in 0..input.n {
        let v: u32 = run_step(&format!("s{i}"), &opts, || Ok(i))?;
        sum += u64::from(v);
    }
    let want = u64::from(input.n) * u64::from(input.n.saturating_sub(1)) / 2;
    if sum != want {
        return Err(WorkflowError::non_retryable(
            "bad_sum",
            format!("sum {sum} != {want}"),
        ));
    }
    Ok(input.n)
}

ostro_sdk::export!(TrivialStepsWorkflow);
