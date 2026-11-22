//! ENG-1 negative probe (plan 09-07): like `exports-shape-ok`, but the inline `exports`
//! interface's `init` takes `stub-wasi: u32` instead of `bool`. Deploy validation must
//! reject the `exports` export with an "unexpected type" violation.

wit_bindgen::generate!({
    path: ["../../wit/ostro-runtime-0.1.0", "wit"],
    world: "ostro-probe:shape/probe",
    generate_all,
});

use exports::exports::{Guest as InitGuest, Symbols};
use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::types::{Failure, Payload};

struct Probe;

impl Guest for Probe {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "exports-shape-bad".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    fn run(_name: String, input: Payload) -> Result<RunResult, Failure> {
        Ok(RunResult::Completed(input))
    }
}

impl InitGuest for Probe {
    fn init(_app_name: String, _symbols: Symbols, _stub_wasi: u32) -> Result<(), String> {
        Ok(())
    }
}

export!(Probe);
