//! Hostile-guest sandbox (plan 03-10, ENG-05 / ENG-06).
//!
//! Workflows:
//! - `loop`: completes step `before`, then spins forever without calling any import.
//! - `bomb`: allocates and touches 1 MiB chunks forever, keeping every chunk alive.
//! - `deep`: unbounded recursion with a 256-byte frame, no imports.
//! - `deep-call`: input = `depth: u32 LE ‖ selector: u8`. Recurses exactly `depth`
//!   frames (same frame as `deep`), then calls host imports with maximum-size
//!   arguments chosen by `selector` (see [`call_imports`]) and completes with
//!   `completed`.
//! - `step-fuel`: step `ok` completes, then step `hot` (budget `fuel = 1_000_000`)
//!   loops forever inside its body.
//!
//! `build.rs` raises the shadow stack to 16 MiB so the native Wasm stack limit
//! (`max_wasm_stack`, 512 KiB) is what deep recursion hits.

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use std::collections::HashMap;
use std::hint::black_box;
use std::time::SystemTime;

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::context;
use ostro::runtime::effects::{self, Header, HttpRequest, WaitMode};
use ostro::runtime::journal::{self, StepStart};
use ostro::runtime::log::{self, Level};
use ostro::runtime::types::{ErrorInfo, Failure, Payload, StepOptions};

const BYTES: &str = "application/octet-stream";
const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;

const WORKFLOWS: &[&str] = &["loop", "bomb", "deep", "deep-call", "step-fuel"];

fn payload(data: Vec<u8>) -> Payload {
    Payload {
        content_type: BYTES.to_string(),
        data,
    }
}

fn opts(fuel: Option<u64>) -> StepOptions {
    StepOptions {
        retry: None,
        timeout_ms: None,
        fuel,
    }
}

/// Run (or replay) a step whose body is `f`.
fn step(name: &str, fuel: Option<u64>, f: impl FnOnce() -> Vec<u8>) -> Vec<u8> {
    match journal::begin_step(name, &opts(fuel)) {
        StepStart::Replayed(Ok(p)) => p.data,
        StepStart::Replayed(Err(_)) => panic!("step {name} recorded a failure"),
        StepStart::Execute(s) => {
            let data = f();
            s.complete(Ok(&payload(data.clone())));
            data
        }
    }
}

fn spin() -> ! {
    loop {
        black_box(());
    }
}

fn wf_loop() -> Result<RunResult, Failure> {
    step("before", None, || b"before".to_vec());
    spin()
}

fn wf_bomb() -> Result<RunResult, Failure> {
    let mut keep: Vec<Vec<u8>> = Vec::new();
    loop {
        let mut chunk = vec![0u8; MIB];
        for i in (0..chunk.len()).step_by(4 * KIB) {
            chunk[i] = 1;
        }
        keep.push(black_box(chunk));
    }
}

/// Unbounded recursion; the `n == u64::MAX` exit is opaque to the optimiser.
#[inline(never)]
fn deep(n: u64) -> u64 {
    let frame = black_box([n as u8; 256]);
    if black_box(n == u64::MAX) {
        return 0;
    }
    let r = deep(n + 1);
    black_box(r.wrapping_add(u64::from(frame[(n & 0xff) as usize])))
}

/// Recurse exactly `n` frames (same frame shape as [`deep`]) and run `bottom` there.
#[inline(never)]
fn descend(n: u32, bottom: &mut dyn FnMut()) -> u64 {
    let frame = black_box([n as u8; 256]);
    if n == 0 {
        bottom();
        return u64::from(frame[0]);
    }
    let r = descend(n - 1, bottom);
    black_box(r.wrapping_add(u64::from(frame[(n & 0xff) as usize])))
}

/// Host imports with maximum-size arguments, by selector.
fn call_imports(selector: u8) {
    match selector {
        0 => {}
        1 => {
            // begin-step with a 64 KiB name, then complete with a 1 MiB payload.
            let name = "s".repeat(64 * KIB);
            step(&name, None, || vec![0xab; MIB]);
        }
        2 => {
            black_box(context::random_bytes(65_536));
        }
        3 => {
            // Exactly 65,536 bytes of JSON (the set-status cap).
            let json = format!("\"{}\"", "a".repeat(65_536 - 2));
            context::set_status(&json);
        }
        4 => {
            let message = "m".repeat(512 * KIB);
            let fields: Vec<(String, String)> =
                (0..64).map(|i| (format!("k{i}"), "v".repeat(64))).collect();
            log::emit(Level::Info, &message, &fields);
        }
        5 => {
            let headers: Vec<Header> = (0..256)
                .map(|i| Header {
                    name: format!("x-h-{i}"),
                    value: "v".repeat(64),
                })
                .collect();
            black_box(effects::http_fetch(&HttpRequest {
                method: "POST".to_string(),
                url: "https://example.test/deep".to_string(),
                headers,
                body: Some(vec![0x5a; MIB]),
                timeout_ms: None,
                idempotency_key: None,
                retry: None,
                redact: Vec::new(),
            }));
        }
        6 => {
            // 4096 timers then one wait: live, this evicts (WaitUnsatisfied).
            let ops: Vec<u64> = (0..4096).map(|_| effects::sleep(1_000)).collect();
            black_box(effects::wait(&ops, WaitMode::All));
        }
        7 => {
            black_box(context::uuid_v7());
            black_box(context::now());
            black_box(context::info());
        }
        8 => {
            println!("{}", "p".repeat(64 * KIB));
            black_box(SystemTime::now());
            let mut m: HashMap<u64, u64> = HashMap::new();
            m.insert(black_box(1), 2);
            black_box(m);
        }
        255 => {
            for s in 1..=8 {
                call_imports(s);
            }
        }
        other => panic!("unknown selector {other}"),
    }
}

fn bad_input(msg: &str) -> Failure {
    Failure::NonRetryable(ErrorInfo {
        kind: "bad-input".to_string(),
        message: msg.to_string(),
        details: None,
    })
}

fn wf_deep_call(input: &Payload) -> Result<RunResult, Failure> {
    let d = &input.data;
    if d.len() != 5 {
        return Err(bad_input("deep-call input = depth u32 LE ‖ selector u8"));
    }
    let depth = u32::from_le_bytes([d[0], d[1], d[2], d[3]]);
    let selector = d[4];
    black_box(descend(depth, &mut || call_imports(selector)));
    Ok(RunResult::Completed(payload(b"completed".to_vec())))
}

fn wf_step_fuel() -> Result<RunResult, Failure> {
    step("ok", None, || b"ok".to_vec());
    step("hot", Some(1_000_000), || spin());
    Ok(RunResult::Completed(payload(b"unreachable".to_vec())))
}

struct Sandbox;

impl Guest for Sandbox {
    fn describe() -> Vec<WorkflowMeta> {
        WORKFLOWS
            .iter()
            .map(|n| WorkflowMeta {
                name: (*n).to_string(),
                version: 1,
                default_queue: None,
                run_timeout_ms: None,
            })
            .collect()
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        match name.as_str() {
            "loop" => wf_loop(),
            "bomb" => wf_bomb(),
            "deep" => Ok(RunResult::Completed(payload(
                deep(black_box(0)).to_le_bytes().to_vec(),
            ))),
            "deep-call" => wf_deep_call(&input),
            "step-fuel" => wf_step_fuel(),
            other => Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: format!("unknown workflow: {other}"),
                details: None,
            })),
        }
    }
}

export!(Sandbox);
