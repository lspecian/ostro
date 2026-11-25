//! EngineExecutor end to end in one process (plan 04-16; WF-02, DISP-01): dispatch ->
//! RunStarted -> engine -> Completed, suspension and timer wake-up, step backoff requeue,
//! attempt retries then Failed, hot input forwarding, restart replay, ordered input bridge.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Opts, T, TestNode, count, guest_wasm, rref};
use ostro_engine::error::AttemptFailure;
use ostro_engine::sink::{ActivationOutcome, ActivationSink};
use ostro_partition::store::values::RunStatus;
use ostro_server::executor::{JobRecord, input_bridge};
use ostro_server::run_started::{build_run_started, input_payload};
use ostro_server::sink::PartitionSink;
use ostro_types::journal::{
    CommandHash, CommandMeta, Event, EventBody, EventKind, TimerFired, TimerStarted,
};
use ostro_types::{PartitionId, RunId};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("json result")
}

fn step_names(journal: &[Event]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_basic_runs_to_completion() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new()).await;
    let sha = n.install_module(&guest_wasm("sdk-basic")).await;
    let run = n
        .start("sdk-basic", 1, br#"{"name":"ostro","n":10}"#, &sha)
        .await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    assert!(rec.completed_at_ms.is_some());
    let out = n.result(&run).await.expect("result stored");
    assert_eq!(
        json(&out),
        json(br#"{"greeting":"hello, ostro","sum":55,"steps":2}"#)
    );
    let j = n.journal(&run).await;
    match &j[0].body {
        EventBody::RunStarted(rs) => {
            assert_eq!(j[0].seq, 0);
            assert_eq!(rs.module_hash, ostro_blob::hex(&sha));
            assert_eq!(rs.workflow, "sdk-basic");
            assert_eq!(rs.input.data, br#"{"name":"ostro","n":10}"#);
        }
        other => panic!("journal[0] is not RunStarted: {other:?}"),
    }
    assert_eq!(count(&j, EventKind::RunStarted), 1);
    assert_eq!(count(&j, EventKind::StepCompleted), 2);
    assert_eq!(count(&j, EventKind::RunCompleted), 1);
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sleep_suspends_then_timer_wakes_and_completes() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new().hot_wait(Duration::from_millis(100))).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let run = n.start("retry-probe", 1, b"sleep:1000", &sha).await;
    // Suspended (Scheduled) while the timer is pending.
    n.wait_until(&run, T, |r| r.status == RunStatus::Scheduled)
        .await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    assert_eq!(n.result(&run).await.as_deref(), Some(&b"slept:1000"[..]));
    let j = n.journal(&run).await;
    assert_eq!(count(&j, EventKind::TimerStarted), 1);
    assert_eq!(count(&j, EventKind::TimerFired), 1);
    assert_eq!(step_names(&j), ["before", "after"]);
    // Two grants: the first activation suspended, the wake-up was re-dispatched.
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(2));
    let jobs = n.jobs_of(&run);
    assert!(
        jobs.iter().any(|j| matches!(
            j.activation,
            Some(ActivationOutcome::Evicted {
                reason: ostro_engine::error::EvictReason::WaitUnsatisfied
            })
        )),
        "jobs {jobs:?}"
    );
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn step_backoff_requeues_until_attempts_exhausted() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new()).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    // Policy max 3, 5 s backoff (> hot_wait 2 s): two StepBackoff evictions.
    let run = n.start("retry-probe", 1, b"requeue", &sha).await;
    n.wait_until(&run, T, |r| r.status == RunStatus::Ready)
        .await;
    let rec = n.wait_terminal(&run, Duration::from_secs(60)).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    assert_eq!(
        n.result(&run).await.as_deref(),
        Some(&b"err:retryable:transient"[..])
    );
    let j = n.journal(&run).await;
    // Total attempts == max (3): two StepAttemptFailed + the recorded failure.
    assert_eq!(count(&j, EventKind::StepAttemptFailed), 2);
    assert_eq!(step_names(&j), ["probe"]);
    let backoffs = n
        .jobs_of(&run)
        .iter()
        .filter(|j| {
            matches!(
                j.activation,
                Some(ActivationOutcome::Evicted {
                    reason: ostro_engine::error::EvictReason::StepBackoff { .. }
                })
            )
        })
        .count();
    assert_eq!(backoffs, 2);
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trapping_attempts_retry_then_fail_the_run() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new()).await;
    // Blob only: the executor loads and prepares the module itself.
    let sha = n.put_blob(&guest_wasm("retry-probe"));
    assert!(n.runtime.prepared(&sha).is_none());
    let run = n.start("retry-probe", 1, b"trap", &sha).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Failed);
    let v = json(&n.result(&run).await.expect("failure result"));
    assert_eq!(v["reason_code"], "trap", "{v}");
    assert_eq!(v["kind"], "trap", "{v}");
    assert!(v["message"].as_str().unwrap_or("").contains("trap"), "{v}");
    // A new attempt token per retry, max_attempt_failures (3) attempts in total.
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(3));
    // The executor records a job after `drive` returns, which can be after the final
    // AttemptFailed was applied and the run went terminal: wait for the third record.
    let is_trap = |j: &JobRecord| {
        matches!(
            j.activation,
            Some(ActivationOutcome::AttemptFailed(AttemptFailure::Trap(_)))
        )
    };
    n.wait_job(&run, T, |j| j.token.n == 3 && is_trap(j)).await;
    let failed: Vec<_> = n.jobs_of(&run).into_iter().filter(|j| is_trap(j)).collect();
    assert_eq!(failed.len(), 3, "{failed:?}");
    let tokens: Vec<u32> = failed.iter().map(|j| j.token.n).collect();
    assert_eq!(tokens, [1, 2, 3]);
    // The executor wrote no journal event for the failure.
    let j = n.journal(&run).await;
    assert_eq!(count(&j, EventKind::RunFailed), 0);
    assert_eq!(count(&j, EventKind::RunStarted), 1);
    assert!(n.runtime.prepared(&sha).is_some());
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_module_fails_the_run() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new()).await;
    let run = n.start("retry-probe", 1, b"quick", &[0x11; 32]).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(rec.status, RunStatus::Failed);
    let v = json(&n.result(&run).await.expect("failure result"));
    assert_eq!(v["reason_code"], "module_not_found", "{v}");
    assert!(n.journal(&run).await.is_empty());
    assert_eq!(n.activations(), 0);
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_input_resolves_wait_in_same_activation() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new().hot_wait(Duration::from_secs(2))).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let run = n.start("retry-probe", 1, b"sleep:300", &sha).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    let j = n.journal(&run).await;
    assert_eq!(count(&j, EventKind::TimerFired), 1);
    assert_eq!(step_names(&j), ["before", "after"]);
    // TimerFired went dispatcher -> lease inputs -> bridge -> Activate.inputs: one grant,
    // one activation.
    assert_eq!(rec.current_attempt.map(|a| a.1), Some(1));
    assert_eq!(n.activations(), 1, "jobs {:?}", n.jobs_of(&run));
    n.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_mid_run_replays_completed_steps() {
    let dir = ostro_testkit::fs::tempdir();
    let opts = Opts::new().hot_wait(Duration::from_millis(100));
    let n = TestNode::open(dir.path(), opts).await;
    let sha = n.install_module(&guest_wasm("retry-probe")).await;
    let run = n.start("retry-probe", 1, b"sleep:1500", &sha).await;
    n.wait_event(&run, EventKind::StepCompleted, T).await;
    let (rt, hooks, opts) = n.stop().await;

    let n = TestNode::open_with(dir.path(), opts, rt, hooks).await;
    let rec = n.wait_terminal(&run, T).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "jobs {:?}",
        n.jobs_of(&run)
    );
    let j = n.journal(&run).await;
    assert_eq!(
        step_names(&j),
        ["before", "after"],
        "each step journaled once"
    );
    assert_eq!(count(&j, EventKind::RunStarted), 1);
    assert_eq!(count(&j, EventKind::RunCompleted), 1);
    n.stop().await;
}

fn meta() -> CommandMeta {
    CommandMeta {
        hash_ver: 1,
        hash: CommandHash([7; 16]),
    }
}

/// The ordered bridge: wake-ups from the dispatcher (seqs) and from the sink (events) that
/// arrive out of seq order still reach the engine in journal order, exactly once; inputs
/// already in the loaded journal are skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn input_bridge_forwards_inputs_in_journal_order() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new()).await;
    let p = PartitionId(0);
    let h = n.handle(p);
    // A run that the dispatcher never sees as Ready (far future) holding our own grant.
    let run = RunId::new_v7(p);
    let start = ostro_types::command::StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "default".into(),
        input: Some(ostro_types::PayloadRef::inline(bytes::Bytes::from_static(
            b"{}",
        ))),
        ready_at_ms: u64::MAX / 2,
        requested_at_ms: 1_700_000_000_000,
        rng_seed: vec![1; 32],
        module_sha: vec![0xab; 32],
        ..Default::default()
    };
    h.submit_one(ostro_types::Command::start_run(start))
        .await
        .expect("start");
    let token = h.grant_attempt(run).await.expect("grant");
    let sink = Arc::new(PartitionSink::new(h.clone(), run, token, None, None));
    let rec = n.record(&run);
    let rs = build_run_started(&rec, input_payload(b"{}".to_vec()), "ab").expect("rs");
    let ev = |seq: u64, body: EventBody| Event {
        seq,
        ts_nanos: 1_700_000_000_000_000_000,
        body,
    };
    let far = 4_000_000_000_000_000_000;
    let mut events = vec![rs];
    for seq in 1..=3 {
        events.push(ev(
            seq,
            EventBody::TimerStarted(TimerStarted {
                meta: meta(),
                fire_at_nanos: far,
            }),
        ));
    }
    sink.append(&rref(&run), token.n, 0, events)
        .await
        .expect("append");
    // Input at seq 4 is "in the loaded journal"; 5 and 6 arrive later.
    let fired = |seq: u64, op: u64| ev(seq, EventBody::TimerFired(TimerFired { op }));
    sink.append(&rref(&run), token.n, 4, vec![fired(4, 1)])
        .await
        .expect("append 4");
    let (seq_tx, seq_rx) = mpsc::unbounded_channel();
    let (sink_tx, sink_rx) = mpsc::unbounded_channel();
    let (eng_tx, mut eng_rx) = mpsc::unbounded_channel();
    let bridge = tokio::spawn(input_bridge(h.clone(), run, 5, seq_rx, sink_rx, eng_tx));
    sink.append(&rref(&run), token.n, 5, vec![fired(5, 2), fired(6, 3)])
        .await
        .expect("append 5,6");
    // Stale dispatcher hint (already loaded), then the sink's seq 6 BEFORE the dispatcher's 5.
    seq_tx.send(4).expect("send");
    sink_tx.send(fired(6, 3)).expect("send");
    seq_tx.send(5).expect("send");
    let mut got = Vec::new();
    for _ in 0..2 {
        let e = timeout(T, eng_rx.recv())
            .await
            .expect("bridge timed out")
            .expect("bridge open");
        got.push(e.seq);
    }
    assert_eq!(got, [5, 6]);
    drop(seq_tx);
    drop(sink_tx);
    timeout(T, bridge)
        .await
        .expect("bridge exits")
        .expect("join");
    assert!(eng_rx.recv().await.is_none(), "no duplicate forwarded");
    n.stop().await;
}

/// Graceful handoff (13-11): once the partition starts quiescing, the input bridge stops
/// and closes the engine's hot-wait input stream (a parked activation is then evicted at
/// once); a bridge started while quiescing stops at once too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn input_bridge_closes_on_handoff_quiescing() {
    let dir = ostro_testkit::fs::tempdir();
    let n = TestNode::open(dir.path(), Opts::new()).await;
    let p = PartitionId(0);
    let h = n.handle(p);
    let run = RunId::new_v7(p);
    let epoch = h.epoch();
    assert!(epoch.0 > 0, "the node leads partition 0");

    let (_seq_tx, seq_rx) = mpsc::unbounded_channel::<u64>();
    let (_sink_tx, sink_rx) = mpsc::unbounded_channel();
    let (eng_tx, mut eng_rx) = mpsc::unbounded_channel();
    let bridge = tokio::spawn(input_bridge(h.clone(), run, 1, seq_rx, sink_rx, eng_tx));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !bridge.is_finished(),
        "the bridge runs while the partition is not quiescing"
    );

    assert!(h.handoff().begin(epoch));
    timeout(T, bridge)
        .await
        .expect("the bridge stops when quiescing starts")
        .expect("join");
    assert!(
        timeout(T, eng_rx.recv()).await.expect("closed").is_none(),
        "the engine's input stream is closed"
    );

    // Started while quiescing: stops at once.
    let (_seq_tx, seq_rx) = mpsc::unbounded_channel::<u64>();
    let (_sink_tx, sink_rx) = mpsc::unbounded_channel();
    let (eng_tx, mut eng_rx) = mpsc::unbounded_channel();
    timeout(T, input_bridge(h.clone(), run, 1, seq_rx, sink_rx, eng_tx))
        .await
        .expect("returns at once");
    assert!(eng_rx.recv().await.is_none());

    // Abort: a new bridge runs normally again.
    h.handoff().clear(epoch);
    let (_seq_tx, seq_rx) = mpsc::unbounded_channel::<u64>();
    let (_sink_tx, sink_rx) = mpsc::unbounded_channel();
    let (eng_tx, _eng_rx) = mpsc::unbounded_channel();
    let bridge = tokio::spawn(input_bridge(h.clone(), run, 1, seq_rx, sink_rx, eng_tx));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!bridge.is_finished(), "the bridge runs after an abort");
    bridge.abort();
    n.stop().await;
}
