//! WASI probe guest (plan 03-04): exercises the std/rand surfaces that pull in WASI
//! imports so the deterministic shim and the import snapshot can be tested.
//! `run(name)` dispatches to one probe and returns its result as text.

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use std::collections::HashMap;
use std::io::Read;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use exports::ostro::runtime::workflows::{Guest, RunResult, WorkflowMeta};
use ostro::runtime::types::{ErrorInfo, Failure, Payload};

struct Probe;

fn text(s: String) -> Result<RunResult, Failure> {
    Ok(RunResult::Completed(Payload {
        content_type: "text/plain".to_string(),
        data: s.into_bytes(),
    }))
}

fn probe(name: &str) -> Option<String> {
    Some(match name {
        "time" => {
            let secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let a = Instant::now();
            let b = Instant::now();
            format!(
                "{{\"secs\":{secs},\"elapsed_ns\":{}}}",
                b.duration_since(a).as_nanos()
            )
        }
        "hashmap" => {
            let mut m = HashMap::new();
            for c in 'a'..='z' {
                m.insert(c.to_string(), c as u32);
            }
            m.keys().cloned().collect::<Vec<_>>().join("")
        }
        "rand" => (0..4)
            .map(|_| rand::random::<u64>().to_string())
            .collect::<Vec<_>>()
            .join(","),
        "print" => {
            println!("hello");
            eprintln!("oops");
            "printed".to_string()
        }
        "print-big" => {
            let line = "x".repeat(1023);
            for _ in 0..1024 {
                println!("{line}");
            }
            "printed-big".to_string()
        }
        "env" => {
            let vars = std::env::vars().count();
            let args = std::env::args().count();
            let cwd = std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|e| format!("error: {e}"));
            format!("{{\"vars\":{vars},\"args\":{args},\"cwd\":\"{cwd}\"}}")
        }
        "sleep" => {
            std::thread::sleep(Duration::from_secs(3600));
            "slept".to_string()
        }
        "exit" => std::process::exit(3),
        "stdin" => {
            let mut buf = Vec::new();
            let _ = std::io::stdin().read_to_end(&mut buf);
            format!("{{\"stdin_len\":{}}}", buf.len())
        }
        _ => return None,
    })
}

impl Guest for Probe {
    fn describe() -> Vec<WorkflowMeta> {
        vec![WorkflowMeta {
            name: "wasi-probe".to_string(),
            version: 1,
            default_queue: None,
            run_timeout_ms: None,
        }]
    }

    fn run(name: String, _input: Payload) -> Result<RunResult, Failure> {
        match probe(&name) {
            Some(out) => text(out),
            None => Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-probe".to_string(),
                message: format!("unknown probe: {name}"),
                details: None,
            })),
        }
    }
}

export!(Probe);
