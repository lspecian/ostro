//! 13-07 (ENG-10) fixture guest whose imports mix two WIT major lines: `ostro:runtime/log`
//! at 0.1.0 and at the test-only 0.2.0 (`crates/ostro-engine/fixtures/wit-next`). Upload
//! must reject it (`MixedWitLines`); it is never instantiated.

wit_bindgen::generate!({
    path: [
        "../../wit/ostro-runtime-0.1.0",
        "../../crates/ostro-engine/fixtures/wit-next",
        "wit",
    ],
    world: "p13:mixed/mixed",
    generate_all,
});

use exports::ostro::runtime0_1_0::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime0_1_0::types::{Failure, Payload};

struct W;

impl Guest for W {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "mixed".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    fn run(_name: String, input: Payload) -> Result<RunResult, Failure> {
        ostro::runtime0_1_0::log::emit(ostro::runtime0_1_0::log::Level::Info, "a", &[]);
        ostro::runtime0_2_0::log::emit(ostro::runtime0_2_0::log::Level::Info, "b", &[]);
        Ok(RunResult::Completed(input))
    }
}

export!(W);
