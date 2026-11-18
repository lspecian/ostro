// Shared body of the `det` determinism workflow (included by det-basic and, in 03-09,
// det-divergent). Requires `const DIVERGE: bool` in the including crate.
//
// Body: native SystemTime + std HashMap + rand (all derived from the journal's logical
// time and the run's rng seed via the WASI shim), steps, a durable sleep + wait,
// a patch gate and a status update. The output is the concatenation of every step's
// journaled bytes (length-prefixed), so record and replay must produce identical output.
//
// When DIVERGE is true the input payload is a UTF-8 mode string selecting one divergence:
// `changed-args`, `extra`, `missing`, `reordered`, `early-return`. When DIVERGE is false
// the input is ignored and the basic path always runs.

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use std::collections::HashMap;
use std::time::SystemTime;

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::context;
use ostro::runtime::effects::{self, WaitMode};
use ostro::runtime::journal::{self, StepStart};
use ostro::runtime::types::{ErrorInfo, Failure, Payload, StepOptions};

const BYTES: &str = "application/octet-stream";

/// Run (or replay) a step whose output bytes come from `f`; returns the journaled bytes.
fn step(name: &str, f: impl FnOnce() -> Vec<u8>) -> Vec<u8> {
    let opts = StepOptions {
        retry: None,
        timeout_ms: None,
        fuel: None,
    };
    match journal::begin_step(name, &opts) {
        StepStart::Replayed(Ok(p)) => p.data,
        StepStart::Replayed(Err(_)) => panic!("step {name} recorded a failure"),
        StepStart::Execute(s) => {
            let data = f();
            let p = Payload {
                content_type: BYTES.to_string(),
                data: data.clone(),
            };
            s.complete(Ok(&p));
            data
        }
    }
}

fn push(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn mode(input: &Payload) -> String {
    if DIVERGE {
        String::from_utf8_lossy(&input.data).into_owned()
    } else {
        String::new()
    }
}

fn det(input: Payload) -> Result<RunResult, Failure> {
    let mode = mode(&input);
    let mut out = Vec::new();

    // (1) native wall clock: the journal's logical time.
    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // (2) std HashMap (RandomState keys) filled with rand keys (WASI random stream).
    let mut map: HashMap<u64, u32> = HashMap::new();
    for i in 0..16u32 {
        map.insert(rand::random::<u64>(), i);
    }
    let order: Vec<u64> = map.keys().copied().collect();

    // (3) step s1.
    let s1 = step("s1", || {
        let mut v = Vec::new();
        v.extend_from_slice(&now_secs.to_le_bytes());
        for k in order.iter().take(4) {
            v.extend_from_slice(&k.to_le_bytes());
        }
        v.extend_from_slice(&rand::random::<u64>().to_le_bytes());
        v.extend_from_slice(&context::random_u64().to_le_bytes());
        v
    });
    push(&mut out, &s1);

    // (4) durable sleep + wait.
    let t = effects::sleep(5_000);
    effects::wait(&[t], WaitMode::All);

    // (5) step s2 (variants: changed args / extra step before it).
    if mode == "extra" {
        push(&mut out, &step("extra", || b"extra".to_vec()));
    }
    let s2_name = if mode == "changed-args" { "s2x" } else { "s2" };
    let s2 = step(s2_name, || {
        let mut v = context::uuid_v7().into_bytes();
        let n = context::now();
        v.extend_from_slice(&n.seconds.to_le_bytes());
        v.extend_from_slice(&n.nanoseconds.to_le_bytes());
        v
    });
    push(&mut out, &s2);
    if mode == "early-return" {
        return Ok(RunResult::Completed(Payload {
            content_type: BYTES.to_string(),
            data: out,
        }));
    }

    // (6) loop steps (variants: missing loop-1 / loop-1 before loop-0).
    let order: Vec<u32> = match mode.as_str() {
        "missing" => vec![0, 2],
        "reordered" => vec![1, 0, 2],
        _ => vec![0, 1, 2],
    };
    for i in order {
        let b = step(&format!("loop-{i}"), || context::random_bytes(16));
        push(&mut out, &b);
    }

    // (7) patch gate.
    if context::patched("v2") {
        push(&mut out, &step("patched-step", || b"patched".to_vec()));
    }

    // (8) status.
    context::set_status("{\"phase\":\"done\"}");

    // (9) output = every step's journaled bytes.
    Ok(RunResult::Completed(Payload {
        content_type: BYTES.to_string(),
        data: out,
    }))
}

struct Det;

impl Guest for Det {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "det".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: Some(600_000),
        }]
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        match name.as_str() {
            "det" => det(input),
            other => Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: format!("unknown workflow: {other}"),
                details: None,
            })),
        }
    }
}

export!(Det);
