//! Smoke guest: proves WIT 0.1 compiles on a Rust guest. Calls no imports.

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::types::{ErrorInfo, Failure, Payload};

struct Smoke;

impl Guest for Smoke {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "smoke".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: Some(60_000),
        }]
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        if name == "smoke" {
            Ok(RunResult::Completed(input))
        } else {
            Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: format!("unknown workflow: {name}"),
                details: None,
            }))
        }
    }
}

export!(Smoke);
