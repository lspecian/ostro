//! Step retry probe (plan 04-07, WF-02): workflow `retry-probe` runs ONE step `probe`
//! whose retry policy and failure behaviour are selected by the UTF-8 input mode:
//!
//! - `exhaust`: policy max 4, initial 10 ms, coeff 2, max 40 ms; always fails
//!   `retryable("transient")` (3 in-activation retries, then the failure is recorded).
//! - `recover:N`: same policy; fails while the activation-local body counter < N.
//! - `fatal`: fails `retryable("fatal")` with `fatal` in `non-retryable-kinds`.
//! - `nonretryable`: fails `non-retryable("permanent")`.
//! - `requeue`: policy max 3, initial 5 s, max 5 s (> `engine.hot_wait`); always fails.
//! - `nopolicy`: no retry policy; fails once.
//!
//! Raw wit-bindgen (no SDK dependency). The guest loops on `retry-now`, re-running the
//! body in the same activation. The run always completes with a summary of the step's
//! journaled outcome: `ok:<data>` or `err:<kind>`.

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use std::sync::atomic::{AtomicU32, Ordering};

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::journal::{self, CompletionAck, StepStart};
use ostro::runtime::types::{ErrorInfo, Failure, Payload, RetryPolicy, StepOptions};

const TEXT: &str = "text/plain";

/// Body executions in this activation (a fresh instance per activation).
static BODY_RUNS: AtomicU32 = AtomicU32::new(0);

fn err(kind: &str) -> Failure {
    Failure::Retryable(ErrorInfo {
        kind: kind.to_string(),
        message: format!("{kind} failure"),
        details: None,
    })
}

fn policy(max: u32, initial: u64, max_interval: u64, non_retryable: &[&str]) -> RetryPolicy {
    RetryPolicy {
        max_attempts: max,
        initial_interval_ms: initial,
        max_interval_ms: max_interval,
        backoff_coefficient: 2.0,
        non_retryable_kinds: non_retryable.iter().map(|s| s.to_string()).collect(),
    }
}

fn options(mode: &str) -> StepOptions {
    let retry = match mode {
        "fatal" => Some(policy(4, 10, 40, &["fatal"])),
        "nonretryable" | "exhaust" => Some(policy(4, 10, 40, &[])),
        "requeue" => Some(policy(3, 5_000, 5_000, &[])),
        "nopolicy" => None,
        m if m.starts_with("recover:") => Some(policy(4, 10, 40, &[])),
        _ => None,
    };
    StepOptions {
        retry,
        timeout_ms: None,
        fuel: None,
    }
}

/// One execution of the step body.
fn body(mode: &str) -> Result<Payload, Failure> {
    let run = BODY_RUNS.fetch_add(1, Ordering::SeqCst) + 1;
    match mode {
        "fatal" => Err(err("fatal")),
        "nonretryable" => Err(Failure::NonRetryable(ErrorInfo {
            kind: "permanent".to_string(),
            message: "permanent failure".to_string(),
            details: None,
        })),
        m if m.starts_with("recover:") => {
            let n: u32 = m["recover:".len()..].parse().unwrap_or(0);
            if run <= n {
                Err(err("transient"))
            } else {
                Ok(Payload {
                    content_type: TEXT.to_string(),
                    data: format!("recovered after {}", run - 1).into_bytes(),
                })
            }
        }
        _ => Err(err("transient")),
    }
}

fn summary(r: &Result<Payload, Failure>) -> String {
    match r {
        Ok(p) => format!("ok:{}", String::from_utf8_lossy(&p.data)),
        Err(Failure::Retryable(e)) => format!("err:retryable:{}", e.kind),
        Err(Failure::NonRetryable(e)) => format!("err:non-retryable:{}", e.kind),
        Err(Failure::Timeout) => "err:timeout".to_string(),
        Err(Failure::Cancelled) => "err:cancelled".to_string(),
    }
}

fn probe(input: Payload) -> Result<RunResult, Failure> {
    let mode = String::from_utf8_lossy(&input.data).into_owned();
    let outcome = match journal::begin_step("probe", &options(&mode)) {
        StepStart::Replayed(r) => r,
        StepStart::Execute(s) => loop {
            let r = body(&mode);
            let ack = s.complete(match &r {
                Ok(p) => Ok(p),
                Err(f) => Err(f),
            });
            match ack {
                CompletionAck::Recorded => break r,
                CompletionAck::RetryNow => continue,
            }
        },
    };
    Ok(RunResult::Completed(Payload {
        content_type: TEXT.to_string(),
        data: summary(&outcome).into_bytes(),
    }))
}

struct Probe;

impl Guest for Probe {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "retry-probe".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        match name.as_str() {
            "retry-probe" => probe(input),
            other => Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: format!("unknown workflow: {other}"),
                details: None,
            })),
        }
    }
}

export!(Probe);
