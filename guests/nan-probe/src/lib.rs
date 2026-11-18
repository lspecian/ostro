//! NaN probe guest (ENG-01): float ops on a non-canonical NaN must yield canonical
//! NaN bits under the engine's NaN canonicalisation. Calls no imports.

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use core::hint::black_box;

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::types::{ErrorInfo, Failure, Payload};

struct NanProbe;

fn fail(kind: &str, message: String) -> Failure {
    Failure::NonRetryable(ErrorInfo {
        kind: kind.to_string(),
        message,
        details: None,
    })
}

impl Guest for NanProbe {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "nan-probe".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    /// `run("nan", 8 LE bytes of f64 bits)` returns 16 LE bytes:
    /// bits of `x + 1.0` then bits of `x * 2.0`.
    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        if name != "nan" {
            return Err(fail(
                "unknown-workflow",
                format!("unknown workflow: {name}"),
            ));
        }
        let bytes: [u8; 8] = input
            .data
            .as_slice()
            .try_into()
            .map_err(|_| fail("bad-input", "expected 8 bytes".to_string()))?;
        let x = f64::from_bits(u64::from_le_bytes(bytes));
        let sum = black_box(black_box(x) + black_box(1.0f64));
        let prod = black_box(black_box(x) * black_box(2.0f64));
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&sum.to_bits().to_le_bytes());
        data.extend_from_slice(&prod.to_bits().to_le_bytes());
        Ok(RunResult::Completed(Payload {
            content_type: "application/octet-stream".to_string(),
            data,
        }))
    }
}

export!(NanProbe);
