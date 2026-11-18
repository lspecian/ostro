//! Resource governance for hostile guests (plan 03-10, ENG-05).
//!
//! An infinite loop, a memory bomb and unbounded recursion each end only their own
//! attempt, with the stable reason code from `activation::classify`:
//! fuel → `FuelExhausted`, epoch deadline → `Timeout`, limiter breach →
//! `MemoryExceeded`, Wasm stack limit → `StackOverflow`. A per-step fuel budget trips
//! only inside that step. Hostile runs sharing one Runtime with a healthy det-basic
//! run leave its output unchanged.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::AttemptFailure;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, RunSpec, Script, drive_to_completion,
};
use ostro_types::journal::{Event, EventBody};

const MIB: u64 = 1024 * 1024;

/// Runtime over `cfg` with a frozen clock, so journals (and outputs) do not depend
/// on how clock calls of concurrent runs interleave.
fn runtime(cfg: EngineConfig) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            cfg,
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [7; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn prepare(rt: &Runtime, guest: &str) -> Arc<Prepared> {
    Arc::new(
        rt.prepare(&common::guest_wasm(guest))
            .unwrap_or_else(|e| panic!("prepare {guest}: {e}")),
    )
}

fn run_ref(id: &str) -> RunRef {
    RunRef {
        partition: 5,
        run_id: id.to_string(),
    }
}

async fn drive(rt: &Runtime, p: &Prepared, id: &str, workflow: &str) -> Drive {
    drive_to_completion(
        rt,
        p,
        RunSpec::new(run_ref(id), workflow, b""),
        Script::new(),
    )
    .await
}

fn completed_steps(journal: &[Event]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.clone()),
            _ => None,
        })
        .collect()
}

fn assert_failed(d: &Drive, expected: &AttemptFailure) {
    assert_eq!(
        d.outcome,
        ActivationOutcome::AttemptFailed(expected.clone()),
        "journal steps: {:?}",
        completed_steps(&d.journal)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_loop_fuel() {
    let rt = runtime(EngineConfig {
        activation_fuel: 100_000_000,
        ..EngineConfig::default()
    });
    let p = prepare(&rt, "sandbox");
    let d = drive(&rt, &p, "loop-fuel", "loop").await;
    assert_failed(&d, &AttemptFailure::FuelExhausted);
    assert_eq!(d.activations, 1);
    assert_eq!(
        completed_steps(&d.journal),
        vec!["before".to_string()],
        "the step completed before the loop is flushed on attempt failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_loop_epoch() {
    let rt = runtime(EngineConfig {
        activation_fuel: u64::MAX / 2,
        activation_timeout: Duration::from_millis(200),
        ..EngineConfig::default()
    });
    let p = prepare(&rt, "sandbox");
    let t0 = Instant::now();
    let d = drive(&rt, &p, "loop-epoch", "loop").await;
    let took = t0.elapsed();
    assert_failed(&d, &AttemptFailure::Timeout);
    assert!(took < Duration::from_secs(2), "timeout took {took:?}");
    assert_eq!(completed_steps(&d.journal), vec!["before".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_memory_bomb() {
    let mut cfg = EngineConfig::default();
    cfg.pool.max_memory_bytes = 64 * MIB;
    let rt = runtime(cfg);
    let p = prepare(&rt, "sandbox");
    let baseline = rt.engine().budget().used();
    let d = drive(&rt, &p, "bomb", "bomb").await;
    assert_failed(&d, &AttemptFailure::MemoryExceeded);
    assert_eq!(
        rt.engine().budget().used(),
        baseline,
        "the node memory budget returns to baseline once the Store is dropped"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_deep_recursion() {
    let rt = runtime(EngineConfig::default());
    let p = prepare(&rt, "sandbox");
    let d = drive(&rt, &p, "deep", "deep").await;
    assert_failed(&d, &AttemptFailure::StackOverflow);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_step_fuel() {
    // A huge activation budget: only the step's own 1,000,000 cap can trip.
    let rt = runtime(EngineConfig {
        activation_fuel: u64::MAX / 2,
        activation_timeout: Duration::from_secs(60),
        ..EngineConfig::default()
    });
    let p = prepare(&rt, "sandbox");
    let t0 = Instant::now();
    let d = drive(&rt, &p, "step-fuel", "step-fuel").await;
    assert_failed(&d, &AttemptFailure::FuelExhausted);
    assert!(
        t0.elapsed() < Duration::from_secs(30),
        "the step cap tripped, not the activation fuel or timeout"
    );
    assert_eq!(
        completed_steps(&d.journal),
        vec!["ok".to_string()],
        "the earlier step is kept, the hot step never completes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sandbox_isolation() {
    // Solo reference on a fresh Runtime.
    let solo_rt = runtime(EngineConfig::default());
    let solo_p = prepare(&solo_rt, "det-basic");
    let solo = drive_to_completion(
        &solo_rt,
        &solo_p,
        RunSpec::new(run_ref("det-iso"), "det", b""),
        Script::new(),
    )
    .await;
    let ActivationOutcome::Completed { output: solo_out } = solo.outcome.clone() else {
        panic!("solo det-basic: {:?}", solo.outcome);
    };

    // One shared Runtime: per-activation fuel is huge, so the "fuel" loop is the
    // step-fuel workflow (a loop stopped by its step's fuel budget) and the plain
    // loop is stopped by the 1 s epoch deadline; the bomb meets a 64 MiB cap.
    let mut cfg = EngineConfig {
        activation_fuel: u64::MAX / 2,
        activation_timeout: Duration::from_secs(1),
        ..EngineConfig::default()
    };
    cfg.pool.max_memory_bytes = 64 * MIB;
    let rt = runtime(cfg);
    let sandbox = prepare(&rt, "sandbox");
    let det = prepare(&rt, "det-basic");

    let hostile = [
        ("iso-fuel", "step-fuel", AttemptFailure::FuelExhausted),
        ("iso-epoch", "loop", AttemptFailure::Timeout),
        ("iso-bomb", "bomb", AttemptFailure::MemoryExceeded),
        ("iso-deep", "deep", AttemptFailure::StackOverflow),
    ];
    let mut handles = Vec::new();
    for (id, wf, expected) in hostile {
        let (rt, p) = (rt.clone(), sandbox.clone());
        handles.push(tokio::spawn(async move {
            (drive(&rt, &p, id, wf).await, expected)
        }));
    }
    let healthy = {
        let (rt, p) = (rt.clone(), det.clone());
        tokio::spawn(async move {
            drive_to_completion(
                &rt,
                &p,
                RunSpec::new(run_ref("det-iso"), "det", b""),
                Script::new(),
            )
            .await
        })
    };

    let healthy = healthy.await.expect("healthy task");
    assert_eq!(
        healthy.outcome,
        ActivationOutcome::Completed {
            output: solo_out.clone()
        },
        "det-basic next to hostile runs completes with its solo output"
    );
    for h in handles {
        let (d, expected) = h.await.expect("hostile task");
        assert_failed(&d, &expected);
    }

    // The shared engine is still healthy afterwards.
    let again = drive_to_completion(
        &rt,
        &det,
        RunSpec::new(run_ref("det-iso"), "det", b""),
        Script::new(),
    )
    .await;
    assert_eq!(
        again.outcome,
        ActivationOutcome::Completed { output: solo_out }
    );
}
