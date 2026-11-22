//! ENG-1 probe (plan 09-07): exports `ostro:runtime/workflows@0.1.0` plus an inline
//! `exports` interface with componentize-py 0.25.1's exact `init` shape, so deploy
//! validation must accept it like a real Python component.
//!
//! Workflows (all v1):
//! - `exports-shape-ok`: one step `probe` recording the input; completes with it.
//! - `abandon-open`: begins step `left-open` and returns while it is still open (the
//!   handle is leaked, so no drop reaches the host); completes with the input.
//! - `abandon-drop`: begins step `dropped`, drops it uncompleted, then runs step `after`
//!   to completion; completes with its output.
//!
//! The two `abandon-*` workflows are the 09-07 replay regression: the live run journals
//! nothing for the abandoned step, and replaying that journal must not report
//! nondeterminism.

wit_bindgen::generate!({
    path: ["../../wit/ostro-runtime-0.1.0", "wit"],
    world: "ostro-probe:shape/probe",
    generate_all,
});

use exports::exports::{Guest as InitGuest, Symbols};
use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::journal::{self, StepStart};
use ostro::runtime::types::{ErrorInfo, Failure, Payload, StepOptions};

struct Probe;

fn opts() -> StepOptions {
    StepOptions {
        retry: None,
        timeout_ms: None,
        fuel: None,
    }
}

fn step(name: &str, value: &Payload) -> Result<Payload, Failure> {
    match journal::begin_step(name, &opts()) {
        StepStart::Replayed(r) => r,
        StepStart::Execute(s) => {
            s.complete(Ok(value));
            Ok(value.clone())
        }
    }
}

impl Guest for Probe {
    fn describe() -> Vec<WorkflowMeta> {
        ["exports-shape-ok", "abandon-open", "abandon-drop"]
            .into_iter()
            .map(|name| WorkflowMeta {
                name: name.to_string(),
                version: 1,
                default_queue: None,
                run_timeout_ms: None,
            })
            .collect()
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        match name.as_str() {
            "exports-shape-ok" => step("probe", &input).map(RunResult::Completed),
            "abandon-open" => {
                if let StepStart::Execute(s) = journal::begin_step("left-open", &opts()) {
                    std::mem::forget(s);
                }
                Ok(RunResult::Completed(input))
            }
            "abandon-drop" => {
                if let StepStart::Execute(s) = journal::begin_step("dropped", &opts()) {
                    drop(s);
                }
                step("after", &input).map(RunResult::Completed)
            }
            _ => Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: name,
                details: None,
            })),
        }
    }
}

impl InitGuest for Probe {
    fn init(_app_name: String, _symbols: Symbols, _stub_wasi: bool) -> Result<(), String> {
        Ok(())
    }
}

export!(Probe);
