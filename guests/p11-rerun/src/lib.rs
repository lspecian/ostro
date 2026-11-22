//! Rerun test guest (plan 11-07, OPS-02), exercised by
//! `crates/ostro-server/tests/rerun_rearm.rs` and the 11-10 control-plane tests.
//! `guests/p11-rerun-v2` is the "fixed" module: identical, except that `s3` never fails.
//!
//! `pipeline` (version 1) input `{fail_at: u32, sleep_between_ms: u64, effect_url?: string,
//! put_run_id_in_body: bool}` (all optional, default 0 / none / false):
//!
//! 1. step `s1` (returns 1);
//! 2. when `effect_url` is set: an HTTP `POST` to it with the JSON body `{"run_id": <id>}`
//!    when `put_run_id_in_body` (the run id from `run-info`), else `{}`;
//! 3. step `s2` (returns 2);
//! 4. when `sleep_between_ms > 0`: a durable sleep (`TimerStarted`) of that length;
//! 5. steps `s3`, `s4`, `s5` (returning 3, 4, 5).
//!
//! Step `s<i>` fails non-retryably (`StepCompleted(Err)`, then `RunFailed`) when
//! `i == fail_at`. The result is `5`.

use std::time::Duration;

use ostro_sdk::prelude::*;

/// `pipeline` input.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Input {
    pub fail_at: u32,
    pub sleep_between_ms: u64,
    pub effect_url: Option<String>,
    pub put_run_id_in_body: bool,
}

/// Whether step `i` fails for `fail_at` (this module: every `i == fail_at`).
fn fails(i: u32, fail_at: u32) -> bool {
    i == fail_at
}

fn step(i: u32, fail_at: u32) -> Result<u32, WorkflowError> {
    let r: u32 = run_step(&format!("s{i}"), &StepOptions::default(), || {
        if fails(i, fail_at) {
            Err(StepError::non_retryable("asked", format!("s{i} fails")))
        } else {
            Ok(i)
        }
    })?;
    Ok(r)
}

#[workflow(name = "pipeline", version = 1)]
pub fn pipeline(ctx: &Ctx, input: Input) -> Result<u32, WorkflowError> {
    step(1, input.fail_at)?;
    if let Some(url) = &input.effect_url {
        let body = if input.put_run_id_in_body {
            json!({ "run_id": ctx.info().run_id })
        } else {
            json!({})
        };
        ctx.http().post(url.clone()).json(&body)?.send().wait()?;
    }
    step(2, input.fail_at)?;
    if input.sleep_between_ms > 0 {
        ctx.sleep(Duration::from_millis(input.sleep_between_ms))
            .wait()?;
    }
    let mut last = 0;
    for i in 3..=5 {
        last = step(i, input.fail_at)?;
    }
    Ok(last)
}

ostro_sdk::export!(PipelineWorkflow);
