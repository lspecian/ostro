//! 13-07 (ENG-10) fixture guest built against the test-only `ostro:runtime@0.1.1` package
//! (`crates/ostro-engine/fixtures/wit-0.1.1`). Workflow `w011` calls the one function 0.1.1
//! adds (`journal.note-hint`, level 2), reads `context.info` and runs one durable step, so
//! its minimum level is 2 and it exercises the shadowed `begin-step` / `step.complete`.

wit_bindgen::generate!({
    path: "../../crates/ostro-engine/fixtures/wit-0.1.1",
    world: "workflow",
});

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::context;
use ostro::runtime::journal::{self, CompletionAck, StepStart};
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
            name: "w011".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        if name != "w011" {
            return Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: name,
                details: None,
            }));
        }
        journal::note_hint("0.1.1 guest");
        let info = context::info();
        let out = step(
            "s0",
            Payload {
                content_type: "text/plain".to_string(),
                data: info.workflow.into_bytes(),
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
