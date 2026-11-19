//! WF-04 gauge (plan 05-03): `Runtime::live_activations()` counts activations running
//! right now and returns to 0 on every exit path: completion, eviction (immediate or
//! after a hot park), a failed attempt, a panic inside the activation future, and the
//! future being dropped.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, ActivationSink, AppendError, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec};
use ostro_types::journal::{Event, RngSeed};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;

const RUNS: usize = 50;

/// A dedicated runtime per test: the gauge is per runtime.
fn runtime(hot_wait: Duration) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait,
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [3; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn prepare(rt: &Runtime) -> Arc<Prepared> {
    // det-basic: a step then a 5 s durable sleep.
    Arc::new(
        rt.prepare(&common::guest_wasm("det-basic"))
            .expect("prepare det-basic"),
    )
}

fn spec(i: usize) -> RunSpec {
    let mut s = RunSpec::new(
        RunRef {
            partition: 1,
            run_id: format!("live-{i}"),
        },
        "det",
        b"",
    );
    s.seed = RngSeed([i as u8; 32]);
    s
}

fn activate_req(
    p: &Prepared,
    sink: &FakeSink,
    i: usize,
    inputs: Option<tokio::sync::mpsc::UnboundedReceiver<Event>>,
    cancel: CancellationToken,
) -> Activate {
    let s = spec(i);
    sink.seed(
        s.run.clone(),
        vec![s.run_started_event(&p.module_hash_hex)],
        1,
    );
    Activate {
        run: s.run.clone(),
        attempt: 1,
        module_sha: p.sha,
        journal: sink.journal(&s.run),
        inputs,
        cancel,
    }
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// 50 concurrent sleeping activations park hot (> 0 while running), then evict on
/// cancel: the gauge returns to 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sleepers_count_while_parked_and_drop_to_zero() {
    let rt = runtime(Duration::from_secs(600));
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());
    assert_eq!(rt.live_activations(), 0);
    let mut handles = Vec::new();
    let mut senders: Vec<UnboundedSender<Event>> = Vec::new();
    let mut cancels = Vec::new();
    for i in 0..RUNS {
        let (tx, rx) = unbounded_channel();
        let cancel = CancellationToken::new();
        let act = activate_req(&p, &sink, i, Some(rx), cancel.clone());
        senders.push(tx);
        cancels.push(cancel);
        let (rt, p, sink) = (rt.clone(), p.clone(), sink.clone());
        handles.push(tokio::spawn(
            async move { rt.activate(&p, act, sink).await },
        ));
    }
    wait_until("all runs parked", || rt.parked().parked_count() == RUNS).await;
    assert_eq!(rt.live_activations(), RUNS as u64, "> 0 during the run");
    for c in &cancels {
        c.cancel();
    }
    for h in handles {
        assert_eq!(
            h.await.expect("join"),
            ActivationOutcome::Evicted {
                reason: EvictReason::External
            }
        );
    }
    assert_eq!(rt.live_activations(), 0);
    assert_eq!(rt.parked().parked_count(), 0);
    drop(senders);
}

/// Without an input channel every sleeper evicts at once; the gauge is 0 afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evicting_sleepers_leave_zero() {
    let rt = runtime(Duration::from_secs(2));
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());
    let mut handles = Vec::new();
    for i in 0..RUNS {
        let act = activate_req(&p, &sink, i, None, CancellationToken::new());
        let (rt, p, sink) = (rt.clone(), p.clone(), sink.clone());
        handles.push(tokio::spawn(
            async move { rt.activate(&p, act, sink).await },
        ));
    }
    let mut peak = 0;
    for h in handles {
        peak = peak.max(rt.live_activations());
        assert_eq!(
            h.await.expect("join"),
            ActivationOutcome::Evicted {
                reason: EvictReason::WaitUnsatisfied
            }
        );
    }
    assert!(peak <= RUNS as u64);
    assert_eq!(rt.live_activations(), 0);
}

/// A failed attempt (bad journal) and a dropped activation future both release.
#[tokio::test]
async fn failed_and_dropped_activations_release() {
    let rt = runtime(Duration::from_secs(600));
    let p = prepare(&rt);
    let sink = Arc::new(FakeSink::new());

    // Bad journal: no RunStarted.
    let outcome = rt
        .activate(
            &p,
            Activate {
                run: spec(0).run,
                attempt: 1,
                module_sha: p.sha,
                journal: Vec::new(),
                inputs: None,
                cancel: CancellationToken::new(),
            },
            sink.clone(),
        )
        .await;
    assert!(
        matches!(
            outcome,
            ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(_))
        ),
        "{outcome:?}"
    );
    assert_eq!(rt.live_activations(), 0);

    // Dropped while parked hot.
    let (tx, rx) = unbounded_channel();
    let act = activate_req(&p, &sink, 1, Some(rx), CancellationToken::new());
    let (rt2, p2, sink2) = (rt.clone(), p.clone(), sink.clone());
    let h = tokio::spawn(async move { rt2.activate(&p2, act, sink2).await });
    wait_until("parked", || rt.parked().parked_count() == 1).await;
    assert_eq!(rt.live_activations(), 1);
    h.abort();
    assert!(h.await.expect_err("aborted").is_cancelled());
    wait_until("released", || rt.live_activations() == 0).await;
    drop(tx);
}

/// A sink whose `done` panics: the panic unwinds through the activation future.
struct PanickingSink(FakeSink);

#[async_trait::async_trait]
impl ActivationSink for PanickingSink {
    async fn append(
        &self,
        run: &RunRef,
        attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError> {
        self.0.append(run, attempt, from_seq, events).await
    }

    async fn fetch_blob(&self, sha: &[u8; 32]) -> Result<Vec<u8>, AppendError> {
        self.0.fetch_blob(sha).await
    }

    async fn done(&self, _run: &RunRef, _attempt: u32, _outcome: ActivationOutcome) {
        panic!("sink done panics (test)");
    }
}

#[tokio::test]
async fn panic_unwind_releases() {
    let rt = runtime(Duration::from_secs(2));
    let p = prepare(&rt);
    let sink = Arc::new(PanickingSink(FakeSink::new()));
    let act = activate_req(&p, &sink.0, 0, None, CancellationToken::new());
    let (rt2, p2) = (rt.clone(), p.clone());
    let h = tokio::spawn(async move { rt2.activate(&p2, act, sink).await });
    assert!(h.await.expect_err("panicked").is_panic());
    assert_eq!(rt.live_activations(), 0);
}
