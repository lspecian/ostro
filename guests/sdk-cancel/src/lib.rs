//! SDK test guest (plan 05-06): D11 cancellation surfacing, `shielded` cleanup, `select!`
//! loser cancel, await tokens and continue-as-new, exported as workflow `sdk-cancel` v1.
//!
//! Input `{"mode": ...}`:
//! - `wait_sleep`: `sleep(10 s).wait()`; returns `"cancelled"` if the run's cancel is
//!   journaled first, `"slept"` if the timer fires first.
//! - `shielded_sleep`: the 10 s sleep is cancelled; an unshielded 100 ms wait then fails
//!   with `cancelled` at once, and the same 100 ms sleep inside `ctx.shielded` completes
//!   (cleanup); returns `"cleaned"`.
//! - `select_loser`: `select!` over 50 ms and 10 s sleeps; the loser is cancelled with
//!   `ctx.cancel` and its wait gives `Err(cancelled)`; returns `"loser_cancelled"`.
//! - `token`: waits on an await token (60 s timeout); returns `"token:<payload JSON>"`.
//! - `continue`: continues as new with `{"mode":"wait_sleep"}`.
//! - `continue_as`: continues as new through the `ContinueAsNew` builder, naming the
//!   workflow and version explicitly.

use std::time::Duration;

use ostro_sdk::prelude::*;

/// `sdk-cancel` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub mode: String,
}

fn unexpected(what: &str, detail: impl std::fmt::Debug) -> WorkflowError {
    WorkflowError::non_retryable("unexpected", format!("{what}: {detail:?}"))
}

#[workflow(name = "sdk-cancel", version = 1)]
pub fn sdk_cancel(ctx: &Ctx, input: Input) -> Result<String, WorkflowError> {
    match input.mode.as_str() {
        "wait_sleep" => match ctx.sleep(Duration::from_secs(10)).wait() {
            Ok(()) => Ok("slept".into()),
            Err(e) if e.is_cancelled() => Ok("cancelled".into()),
            Err(e) => Err(e.into()),
        },
        "shielded_sleep" => {
            match ctx.sleep(Duration::from_secs(10)).wait() {
                Err(e) if e.is_cancelled() => {}
                other => return Err(unexpected("work wait", other)),
            }
            // Unshielded, every later wait observes the (already journaled) cancel.
            match ctx.sleep(Duration::from_millis(100)).wait() {
                Err(e) if e.is_cancelled() => {}
                other => return Err(unexpected("unshielded wait after cancel", other)),
            }
            // Shielded cleanup runs to completion.
            ctx.shielded(|| ctx.sleep(Duration::from_millis(100)).wait())?;
            Ok("cleaned".into())
        }
        "select_loser" => {
            let short = ctx.sleep(Duration::from_millis(50));
            let long = ctx.sleep(Duration::from_secs(10));
            let (winner, c) = ostro_sdk::select!(short, long);
            if winner != 0 {
                return Err(unexpected("select winner", winner));
            }
            short.decode(c)?;
            ctx.cancel(&long);
            match long.wait() {
                Err(e) if e.is_cancelled() => Ok("loser_cancelled".into()),
                other => Err(unexpected("loser wait", other)),
            }
        }
        "token" => {
            let t = ctx.create_await_token::<serde_json::Value>(Some(Duration::from_secs(60)));
            if t.token().is_empty() {
                return Err(unexpected("token", "empty"));
            }
            let v = t.op().wait()?;
            Ok(format!("token:{v}"))
        }
        "continue" => Err(ctx.continue_as_new(&json!({ "mode": "wait_sleep" }))),
        "continue_as" => Err(ContinueAsNew::new(&json!({ "mode": "token" }))?
            .workflow("sdk-cancel")
            .version(1)
            .into()),
        other => Err(WorkflowError::non_retryable(
            "bad_mode",
            format!("unknown mode {other:?}"),
        )),
    }
}

ostro_sdk::export!(SdkCancelWorkflow);
