//! Maximum Wasm stack depth vs host-call native stack (plan 03-10, ENG-06).
//!
//! Async host imports run on the activation's fiber stack (`engine.async_stack_size`),
//! the same stack the guest's Wasm frames use. Wasmtime stops Wasm frames at
//! `engine.max_wasm_stack`, but host code called from the deepest allowed Wasm frame
//! only has `async_stack_size − max_wasm_stack` of native stack left. If a host import
//! needs more than that, it runs into the fiber's guard page and the whole process
//! dies (SIGSEGV/SIGABRT): no trap, no classification, every run on the node is lost.
//!
//! Phase 3 research measured this on Wasmtime 50 with a 2 MiB fiber stack and a
//! 512 KiB Wasm stack: a host import using ~1.4 MiB of native stack survived, ~1.7 MiB
//! aborted the process. Ostro therefore ships `async_stack_size = 4 MiB` with
//! `max_wasm_stack = 512 KiB` (3.5 MiB of host headroom), and host imports must stay
//! thin: no deep recursion, large stack buffers or heavy work inside `poll`; anything
//! heavy goes to a separate task / `spawn_blocking`.
//!
//! `stack_depth_every_import_at_max_depth` binary-searches, per import selector of the
//! `sandbox` guest's `deep-call` workflow, the deepest recursion that does not overflow,
//! and calls the imports with maximum-size arguments there (and at d−1, d+1). Reaching
//! the assertions at all proves the process survived.
//!
//! `stack_depth_negative_control` proves the test can see the hazard: it re-executes
//! this test binary (so an abort cannot kill the runner) with a fiber stack only
//! 16 KiB larger than `max_wasm_stack`, where host calls at max depth are expected to
//! abort the child.

mod common;

use std::sync::Arc;

use ostro_engine::config::EngineConfig;
use ostro_engine::error::AttemptFailure;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, RunSpec, Script, drive_to_completion};

const SELECTORS: [u8; 10] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 255];
const PROBE_START: u32 = 1_000;
const PROBE_CAP: u32 = 1 << 24;

fn runtime(cfg: EngineConfig) -> Runtime {
    Runtime::new(
        cfg,
        RuntimeOptions {
            clock: Arc::new(FakeClock::new(DEFAULT_ACCEPTED_AT)),
            token_key: [3; 32],
        },
    )
    .expect("runtime")
}

fn is_overflow(o: &ActivationOutcome) -> bool {
    matches!(
        o,
        ActivationOutcome::AttemptFailed(AttemptFailure::StackOverflow)
    )
}

/// One `deep-call` run (fresh activations, timers fired so selector 6 completes).
async fn run_deep(rt: &Runtime, p: &Prepared, depth: u32, selector: u8) -> ActivationOutcome {
    let mut input = depth.to_le_bytes().to_vec();
    input.push(selector);
    let run = RunRef {
        partition: 1,
        run_id: format!("deep-{selector}-{depth}"),
    };
    let d = drive_to_completion(
        rt,
        p,
        RunSpec::new(run, "deep-call", &input),
        Script::new().fire_timers(true),
    )
    .await;
    d.outcome
}

/// Largest depth whose run does not end in `StackOverflow`: exponential probe from
/// 1,000, then binary search.
async fn max_depth(rt: &Runtime, p: &Prepared, selector: u8) -> u32 {
    let (mut lo, mut hi) = (0u32, PROBE_START);
    loop {
        if is_overflow(&run_deep(rt, p, hi, selector).await) {
            break;
        }
        lo = hi;
        hi = hi.checked_mul(2).expect("depth overflow");
        assert!(
            hi <= PROBE_CAP,
            "selector {selector}: no overflow up to {PROBE_CAP}"
        );
    }
    // Invariant: lo does not overflow (0 = trivially), hi overflows.
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if is_overflow(&run_deep(rt, p, mid, selector).await) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    lo
}

fn acceptable(o: &ActivationOutcome) -> bool {
    matches!(
        o,
        ActivationOutcome::Completed { .. }
            | ActivationOutcome::Evicted { .. }
            | ActivationOutcome::AttemptFailed(AttemptFailure::StackOverflow)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stack_depth_every_import_at_max_depth() {
    let cfg = EngineConfig::default();
    assert_eq!(cfg.async_stack_size, 4 * 1024 * 1024);
    assert_eq!(cfg.max_wasm_stack, 512 * 1024);
    let rt = runtime(cfg);
    let p = rt
        .prepare(&common::guest_wasm("sandbox"))
        .expect("prepare sandbox");

    for sel in SELECTORS {
        let d = max_depth(&rt, &p, sel).await;
        assert!(d > 0, "selector {sel}: even depth 1 overflows");
        for depth in [d - 1, d, d + 1] {
            let o = run_deep(&rt, &p, depth, sel).await;
            assert!(
                acceptable(&o),
                "selector {sel} depth {depth}: unexpected outcome {o:?}"
            );
            if depth <= d {
                assert!(!is_overflow(&o), "selector {sel} depth {depth} overflowed");
            }
        }
        println!("STACK_DEPTH selector={sel} max_depth={d}");
    }
}

/// Child half of the negative control (run only via `stack_depth_negative_control`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child process of stack_depth_negative_control"]
async fn stack_depth_child() {
    let base = EngineConfig::default();
    let headroom = 16 * 1024;
    let cfg = EngineConfig {
        async_stack_size: base.max_wasm_stack + headroom,
        ..base
    };
    let async_stack = cfg.async_stack_size;
    let rt = runtime(cfg);
    println!("CHILD async_stack_size={async_stack} headroom={headroom}");
    let p = rt
        .prepare(&common::guest_wasm("sandbox"))
        .expect("prepare sandbox");
    let d0 = max_depth(&rt, &p, 0).await;
    println!("CHILD selector=0 max_depth={d0}");
    // Every import, as deep as the guest's own frames allow: host calls then run with
    // at most `headroom` of native stack. Expected to abort the process.
    let d = max_depth(&rt, &p, 255).await;
    let o = run_deep(&rt, &p, d, 255).await;
    println!("CHILD SURVIVED selector=255 max_depth={d} outcome={o:?}");
}

#[cfg(unix)]
#[test]
fn stack_depth_negative_control() {
    use std::os::unix::process::ExitStatusExt;
    use std::process::Command;

    if std::env::var_os("OSTRO_STACK_CHILD").is_some() {
        return;
    }
    let exe = std::env::current_exe().expect("current_exe");
    let out = Command::new(exe)
        .args([
            "--exact",
            "stack_depth_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("OSTRO_STACK_CHILD", "1")
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let tail = |s: &str| s.lines().rev().take(15).collect::<Vec<_>>().join("\n");
    println!("child status: {:?}", out.status);
    println!("child stdout (tail, reversed):\n{}", tail(&stdout));
    println!("child stderr (tail, reversed):\n{}", tail(&stderr));

    match out.status.signal() {
        Some(sig)
            if sig == libc_sig::SIGABRT || sig == libc_sig::SIGSEGV || sig == libc_sig::SIGBUS =>
        {
            println!("NEGATIVE CONTROL ABORTED (signal {sig}) with 16 KiB host headroom");
        }
        Some(sig) => panic!("child died with unexpected signal {sig}"),
        None => {
            assert!(
                stdout.contains("CHILD SURVIVED"),
                "child neither aborted nor completed: {:?}\n{stderr}",
                out.status
            );
            println!("NEGATIVE CONTROL DID NOT ABORT (headroom=16384)");
            assert!(
                std::env::var("OSTRO_STACK_STRICT").as_deref() != Ok("1"),
                "OSTRO_STACK_STRICT=1: the negative control must abort"
            );
        }
    }
}

/// Signal numbers (Linux and macOS agree on SIGABRT/SIGSEGV; a guard-page hit can
/// also surface as SIGBUS on macOS).
#[cfg(unix)]
mod libc_sig {
    pub const SIGABRT: i32 = 6;
    pub const SIGSEGV: i32 = 11;
    #[cfg(target_os = "macos")]
    pub const SIGBUS: i32 = 10;
    #[cfg(not(target_os = "macos"))]
    pub const SIGBUS: i32 = 7;
}
