//! Forbidden-WASI guest (plan 03-04): reads a file through std::fs, so the component
//! imports `wasi:filesystem/*`. Deploy-time validation must reject it.

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::types::{ErrorInfo, Failure, Payload};

struct Forbidden;

impl Guest for Forbidden {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "wasi-forbidden".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    fn run(_name: String, _input: Payload) -> Result<RunResult, Failure> {
        match std::fs::read_to_string("/etc/hostname") {
            Ok(s) => Ok(RunResult::Completed(Payload {
                content_type: "text/plain".to_string(),
                data: s.into_bytes(),
            })),
            Err(e) => Err(Failure::NonRetryable(ErrorInfo {
                kind: "io".to_string(),
                message: e.to_string(),
                details: None,
            })),
        }
    }
}

export!(Forbidden);
