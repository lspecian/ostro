//! 13-07 (ENG-10) fixture guest built against the test-only `ostro:runtime@0.2.0` package
//! (`crates/ostro-engine/fixtures/wit-next`, a second WIT major line). Workflow `w020` reads
//! `context.info` (whose `run-info` changed in 0.2: `region`), logs it and runs one durable
//! step returning the region (output `<region>:<input>`).

wit_bindgen::generate!({
    path: "../../crates/ostro-engine/fixtures/wit-next",
    world: "workflow",
});

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::context;
use ostro::runtime::journal::{self, CompletionAck, StepStart};
use ostro::runtime::log::{self, Level};
use ostro::runtime::types::{ErrorInfo, Failure, Payload, StepOptions};

struct W;

fn step(name: &str, out: Payload) -> Result<Payload, Failure> {
    let opts = StepOptions {
        retry: None,
        timeout_ms: None,
        fuel: None,
    };
    match journal::begin_step(name, &opts) {
        StepStart::Replayed(r) => r,
        StepStart::Execute(s) => loop {
            match s.complete(Ok(&out)) {
                CompletionAck::Recorded => break Ok(out),
                CompletionAck::RetryNow => continue,
            }
        },
    }
}

impl Guest for W {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "w020".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        if name != "w020" {
            return Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: name,
                details: None,
            }));
        }
        let info = context::info();
        log::emit(
            Level::Info,
            "w020",
            &[(
                "region".to_string(),
                info.region.clone().unwrap_or_default(),
            )],
        );
        let out = step(
            "s0",
            Payload {
                content_type: "text/plain".to_string(),
                data: info.region.unwrap_or_default().into_bytes(),
            },
        )?;
        let mut data = out.data;
        data.extend_from_slice(b":");
        data.extend_from_slice(&input.data);
        Ok(RunResult::Completed(Payload {
            content_type: "text/plain".to_string(),
            data,
        }))
    }
}

export!(W);
