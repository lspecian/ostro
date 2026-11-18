//! Hot wait vs evict and node-budget LRU eviction (plan 03-11, ENG-09).
//!
//! - A wait whose ops can all resolve within `engine.hot_wait` parks after flushing
//!   its intents and resumes in the same activation when the input arrives.
//! - A wait that cannot resolve within the window evicts immediately; a parked wait
//!   whose input does not arrive evicts with `HotWaitElapsed`.
//! - Hot-waited and evict-and-replay runs journal the same events.
//! - Over the node memory budget the least recently parked activation is evicted
//!   first (`BudgetPressure`); running activations never are; many concurrent runs
//!   complete correctly under a small budget.
//!
//! det-basic sleeps 5 s of logical time, so it is hot-waitable only with
//! `hot_wait >= 5 s`. All runtimes use a frozen `FakeClock` so journals do not depend
//! on how many activations ran.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::EvictReason;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, FakeSink, RunSpec, Script, drive_to_completion,
};
use ostro_types::journal::{Event, EventBody, EventKind, RngSeed, encode_event};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn runtime(hot_wait: Duration, budget: Option<u64>) -> Arc<Runtime> {
    let mut cfg = EngineConfig {
        hot_wait,
        ..EngineConfig::default()
    };
    if let Some(b) = budget {
        cfg.node_memory_budget = b;
    }
    Arc::new(
        Runtime::new(
            cfg,
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [9; 32],
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

fn det_spec(id: &str, seed: u8) -> RunSpec {
    let mut spec = RunSpec::new(run_ref(id), "det", b"");
    spec.seed = RngSeed([seed; 32]);
    spec
}

fn completed(d: &Drive) -> Vec<u8> {
    match &d.outcome {
        ActivationOutcome::Completed { output } => output.data.clone(),
        other => panic!("not completed: {other:?} (outcomes {:?})", d.outcomes),
    }
}

/// `(seq, kind, hash, encoded bytes)` of one event.
type Shape = (u64, EventKind, Option<[u8; 16]>, Vec<u8>);

/// [`Shape`] per event, with `ts_nanos` and the step's
/// `fuel_used` zeroed. Both are per-execution measurements, not recorded results:
/// fuel differs slightly when the guest heap differs (s1 ran live in the same instance
/// vs was replayed), while the recorded outcome bytes must be identical.
fn shape(journal: &[Event]) -> Vec<Shape> {
    journal
        .iter()
        .map(|e| {
            let mut z = e.clone();
            z.ts_nanos = 0;
            if let EventBody::StepCompleted(s) = &mut z.body {
                s.fuel_used = 0;
            }
            (
                e.seq,
                e.body.kind(),
                e.body.command_meta().map(|m| m.hash.0),
                encode_event(&z).1,
            )
        })
        .collect()
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A directly driven activation with an input channel whose sender is held (no input
/// ever arrives unless the test sends one).
struct Parked {
    handle: JoinHandle<ActivationOutcome>,
    cancel: CancellationToken,
    _tx: UnboundedSender<Event>,
}

fn start(rt: &Arc<Runtime>, p: &Arc<Prepared>, sink: &Arc<FakeSink>, spec: RunSpec) -> Parked {
    let run = spec.run.clone();
    sink.seed(
        run.clone(),
        vec![spec.run_started_event(&p.module_hash_hex)],
        1,
    );
    let (tx, rx) = unbounded_channel();
    let cancel = CancellationToken::new();
    let act = Activate {
        run: run.clone(),
        attempt: 1,
        module_sha: p.sha,
        journal: sink.journal(&run),
        inputs: Some(rx),
        cancel: cancel.clone(),
    };
    let (rt, p, sink) = (rt.clone(), p.clone(), sink.clone());
    let handle = tokio::spawn(async move { rt.activate(&p, act, sink).await });
    Parked {
        handle,
        cancel,
        _tx: tx,
    }
}

#[tokio::test]
async fn hot_wait_short_timer_one_activation() {
    let rt = runtime(Duration::from_secs(10), None);
    let p = prepare(&rt, "det-basic");
    let d = drive_to_completion(
        &rt,
        &p,
        det_spec("hot-1", 1),
        Script::new().live(Duration::from_millis(50)),
    )
    .await;
    completed(&d);
    assert_eq!(d.activations, 1, "resumed in the same activation");
    assert_eq!(d.outcomes.len(), 1);
    assert_eq!(rt.parked().parked_count(), 0);
    // The intents were flushed before parking, the rest after the run returned.
    assert_eq!(d.appended_batches, 2);
}

#[tokio::test]
async fn hot_wait_long_sleep_evicts_immediately() {
    let rt = runtime(Duration::from_secs(1), None);
    let p = prepare(&rt, "det-basic");
    let sink = Arc::new(FakeSink::new());
    let started = Instant::now();
    let a = start(&rt, &p, &sink, det_spec("cold-1", 1));
    let outcome = a.handle.await.expect("join");
    let took = started.elapsed();
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    assert!(took < Duration::from_millis(200), "evicted in {took:?}");

    // Re-dispatch (live delivery on, but the 5 s timer is outside the 1 s window).
    let d = drive_to_completion(
        &rt,
        &p,
        det_spec("cold-2", 1),
        Script::new().live(Duration::from_millis(20)),
    )
    .await;
    completed(&d);
    assert_eq!(
        d.outcomes[0],
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    assert_eq!(d.activations, 2);
}

#[tokio::test]
async fn hot_wait_elapsed_evicts() {
    // wit-surface's first wait is on three effects (in flight = hot-waitable).
    // Live completions are delayed far beyond the window, i.e. no live delivery.
    let rt = runtime(Duration::from_millis(300), None);
    let p = prepare(&rt, "wit-surface");
    let started = Instant::now();
    let d = drive_to_completion(
        &rt,
        &p,
        RunSpec::new(run_ref("elapsed"), "surface", b""),
        Script::new().live(Duration::from_secs(3_600)),
    )
    .await;
    let took = started.elapsed();
    completed(&d);
    assert_eq!(
        d.outcomes[0],
        ActivationOutcome::Evicted {
            reason: EvictReason::HotWaitElapsed
        }
    );
    // The re-dispatch fires every open timer (incl. the 1 s sleep of the second wait)
    // and completes.
    assert_eq!(d.activations, 2);
    assert!(
        took >= Duration::from_millis(250) && took < Duration::from_secs(2),
        "parked for {took:?}"
    );
}

#[tokio::test]
async fn hot_wait_journal_equivalence() {
    let hot_rt = runtime(Duration::from_secs(10), None);
    let hot_p = prepare(&hot_rt, "det-basic");
    let hot = drive_to_completion(
        &hot_rt,
        &hot_p,
        det_spec("eq", 3),
        Script::new().live(Duration::from_millis(30)),
    )
    .await;
    assert_eq!(hot.activations, 1);

    // Evict-and-replay: no input channel.
    let cold_rt = runtime(Duration::from_secs(10), None);
    let cold_p = prepare(&cold_rt, "det-basic");
    let cold = drive_to_completion(&cold_rt, &cold_p, det_spec("eq", 3), Script::new()).await;
    assert_eq!(cold.activations, 2);

    assert_eq!(completed(&hot), completed(&cold));
    assert_eq!(shape(&hot.journal), shape(&cold.journal));
    // With the frozen clock even the timestamps agree.
    let ts = |d: &Drive| d.journal.iter().map(|e| e.ts_nanos).collect::<Vec<_>>();
    assert_eq!(ts(&hot), ts(&cold));
}

#[tokio::test]
async fn hot_wait_external_cancel() {
    let rt = runtime(Duration::from_secs(10), None);
    let p = prepare(&rt, "det-basic");
    let sink = Arc::new(FakeSink::new());
    let a = start(&rt, &p, &sink, det_spec("ext", 1));
    let parked = rt.parked().clone();
    wait_until("park", Duration::from_secs(5), || {
        parked.parked_count() == 1
    })
    .await;
    // Parked after flushing its intents (s1 + TimerStarted).
    assert_eq!(sink.journal(&run_ref("ext")).len(), 3);
    a.cancel.cancel();
    let outcome = a.handle.await.expect("join");
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::External
        }
    );
    assert_eq!(parked.parked_count(), 0);
    assert_eq!(rt.engine().budget().used(), 0);
}

/// Memory one parked det-basic activation charges to the node budget.
async fn det_basic_charge() -> u64 {
    let rt = runtime(Duration::from_secs(10), None);
    let p = prepare(&rt, "det-basic");
    let sink = Arc::new(FakeSink::new());
    let a = start(&rt, &p, &sink, det_spec("probe", 1));
    let parked = rt.parked().clone();
    wait_until("probe park", Duration::from_secs(5), || {
        parked.parked_count() == 1
    })
    .await;
    let used = rt.engine().budget().used();
    a.cancel.cancel();
    a.handle.await.expect("join");
    assert!(used > 0, "det-basic charges the node budget");
    used
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_wait_budget_lru() {
    let c = det_basic_charge().await;
    // Room for three parked instances, not four.
    let rt = runtime(Duration::from_secs(10), Some(3 * c + c / 2));
    let p = prepare(&rt, "det-basic");
    let sink = Arc::new(FakeSink::new());
    let parked = rt.parked().clone();
    let budget = rt.engine().budget().clone();

    let mut runs = Vec::new();
    for i in 1..=3u8 {
        runs.push(start(&rt, &p, &sink, det_spec(&format!("lru-{i}"), 1)));
        let want = usize::from(i);
        wait_until("park", Duration::from_secs(5), || {
            parked.parked_count() == want
        })
        .await;
    }
    assert!(
        !budget.is_over(),
        "3 parked fit: {} / {}",
        budget.used(),
        budget.limit()
    );

    // The 4th (running) activation pushes the budget over.
    let fourth = start(&rt, &p, &sink, det_spec("lru-4", 1));

    let first = runs.remove(0);
    let outcome = tokio::time::timeout(Duration::from_secs(5), first.handle)
        .await
        .expect("first parked evicted")
        .expect("join");
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::BudgetPressure
        },
        "least recently parked goes first"
    );
    assert!(
        runs.iter().all(|r| !r.handle.is_finished()),
        "only the oldest was evicted"
    );

    // The 4th either parks (budget back under) or evicts cold; never for budget.
    wait_until("4th settles", Duration::from_secs(5), || {
        parked.parked_count() == 3 || fourth.handle.is_finished()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for r in &runs {
        assert!(!r.handle.is_finished(), "later parked runs survive");
    }
    fourth.cancel.cancel();
    let out4 = fourth.handle.await.expect("join");
    assert_ne!(
        out4,
        ActivationOutcome::Evicted {
            reason: EvictReason::BudgetPressure
        }
    );
    for r in runs {
        r.cancel.cancel();
        assert_eq!(
            r.handle.await.expect("join"),
            ActivationOutcome::Evicted {
                reason: EvictReason::External
            }
        );
    }
    assert_eq!(parked.parked_count(), 0);
    assert_eq!(budget.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_budget_many_runs() {
    const RUNS: u8 = 50;
    let c = det_basic_charge().await;

    // Reference outputs: solo evict-and-replay runs, one at a time.
    let solo_rt = runtime(Duration::from_secs(10), None);
    let solo_p = prepare(&solo_rt, "det-basic");
    let mut expected = Vec::new();
    for i in 0..RUNS {
        let d = drive_to_completion(
            &solo_rt,
            &solo_p,
            det_spec(&format!("many-{i}"), i),
            Script::new(),
        )
        .await;
        expected.push(completed(&d));
    }

    let rt = runtime(Duration::from_secs(10), Some(10 * c));
    let p = prepare(&rt, "det-basic");
    let mut tasks = Vec::new();
    for i in 0..RUNS {
        let (rt, p) = (rt.clone(), p.clone());
        tasks.push(tokio::spawn(async move {
            drive_to_completion(
                &rt,
                &p,
                det_spec(&format!("many-{i}"), i),
                Script::new().live(Duration::from_millis(20)),
            )
            .await
        }));
    }
    let mut budget_evictions = 0;
    let mut hot = 0;
    for (i, t) in tasks.into_iter().enumerate() {
        let d = tokio::time::timeout(Duration::from_secs(60), t)
            .await
            .expect("run finished")
            .expect("join");
        assert_eq!(completed(&d), expected[i], "run {i} output");
        budget_evictions += d
            .outcomes
            .iter()
            .filter(|o| {
                **o == ActivationOutcome::Evicted {
                    reason: EvictReason::BudgetPressure,
                }
            })
            .count();
        hot += usize::from(d.activations == 1);
    }
    println!(
        "50 runs under 10x budget: {hot} hot (1 activation), {budget_evictions} budget evictions"
    );
    let budget = rt.engine().budget().clone();
    wait_until("budget released", Duration::from_secs(5), || {
        budget.used() == 0
    })
    .await;
    assert_eq!(rt.parked().parked_count(), 0);
}
