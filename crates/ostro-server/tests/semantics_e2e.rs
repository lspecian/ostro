//! Phase-level workflow semantics end to end through the real in-process server (plan
//! 05-15): WF-03 gather/race replay identity under concurrent signals and a restart,
//! signals with timeouts and buffering (WF-05), patched in-flight code (WF-08), run and
//! schedule-to-start timeouts (WF-09), cancel / terminate / retry incl. resume after a
//! timeout (API-05), await tokens (API-07), D21 large payloads, continue-as-new chains
//! and retention expiry (OPS-06). The 1,000-sleeper WF-04 proof lives in
//! `semantics_sleepers.rs` (own process, serial CI step).
//!
//! REST only (REST/gRPC parity is proven by `lifecycle_api.rs`). The API derives a run's
//! `run_timeout_ms` from the workflow's declared metadata only and none of the test guests
//! declares a short one, so the run-timeout scenarios submit their `StartRun` straight to
//! the partition (`TestServer::start_raw`); everything after the start (execution, the
//! timeout firing, retry, signals, results) goes through the server and its API.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::FakeSink;
use ostro_partition::store::values::RunStatus;
use ostro_server::config::ServerConfig;
use ostro_types::RunId;
use ostro_types::journal::{Event, EventBody, EventKind};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const LIMIT: Duration = Duration::from_secs(30);
const MIB: usize = 1024 * 1024;

// ---------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------

/// A server with `engine.hot_wait` 50 ms (sleeps and waits evict at once, so "sleeping"
/// means `Scheduled`) and `tweak` applied.
async fn server_with(tweak: impl FnOnce(&mut ServerConfig)) -> TestServer {
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_millis(50).into();
        tweak(c);
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

async fn server() -> TestServer {
    server_with(|_| {}).await
}

fn id_of(v: &Value) -> RunId {
    v["run_id"]
        .as_str()
        .expect("run_id")
        .parse()
        .expect("run id")
}

async fn result_of(s: &TestServer, run: &RunId) -> Value {
    let b = s.result_bytes(run).await.expect("result");
    serde_json::from_slice(&b).expect("json result")
}

async fn signal(s: &TestServer, run: &RunId, name: &str, payload: Value) -> (StatusCode, Value) {
    s.post_json(
        &format!("/v1/runs/{run}/signals/{name}"),
        &json!({"payload": payload}),
    )
    .await
}

async fn signal_ok(s: &TestServer, run: &RunId, name: &str, payload: Value) {
    let (st, v) = signal(s, run, name, payload).await;
    assert_eq!(st, StatusCode::OK, "signal {run}/{name}: {v}");
}

async fn lifecycle(s: &TestServer, run: &RunId, op: &str) -> (StatusCode, Value) {
    s.post_json(&format!("/v1/runs/{run}/{op}"), &json!({}))
        .await
}

async fn lifecycle_ok(s: &TestServer, run: &RunId, op: &str) -> Value {
    let (st, v) = lifecycle(s, run, op).await;
    assert_eq!(st, StatusCode::OK, "{op} {run}: {v}");
    v
}

fn err_reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

/// Suspended on a timer or wait: journal started, not running.
async fn wait_sleeping(s: &TestServer, run: &RunId) {
    s.wait_record(run, LIMIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
}

fn kinds(j: &[Event]) -> Vec<EventKind> {
    j.iter().map(|e| e.body.kind()).collect()
}

fn n_kind(j: &[Event], k: EventKind) -> usize {
    j.iter().filter(|e| e.body.kind() == k).count()
}

fn step_names(j: &[Event]) -> Vec<String> {
    j.iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.clone()),
            _ => None,
        })
        .collect()
}

fn terminated_reasons(j: &[Event]) -> Vec<String> {
    j.iter()
        .filter_map(|e| match &e.body {
            EventBody::RunTerminated(t) => Some(t.reason.clone()),
            _ => None,
        })
        .collect()
}

fn wait_resolved(j: &[Event]) -> Vec<(u64, Vec<u64>)> {
    j.iter()
        .filter_map(|e| match &e.body {
            EventBody::WaitResolved(w) => Some((e.seq, w.completed.clone())),
            _ => None,
        })
        .collect()
}

/// Wait until the run's live activation count on the server is 0.
async fn wait_no_live(s: &TestServer) {
    let t0 = Instant::now();
    while s.h().runtime().live_activations() > 0 {
        assert!(t0.elapsed() < LIMIT, "an activation is still live");
        sleep(Duration::from_millis(10)).await;
    }
}

/// Replay `journal` of `run` on a fresh Runtime (fresh engine, empty caches, another
/// token key) with a recording sink: `(outcome, appended events, the sink's journal)`.
async fn replay_fresh(
    guest: &str,
    run: &RunId,
    journal: Vec<Event>,
) -> (ActivationOutcome, usize, Vec<Event>) {
    let rt = Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            token_key: [0x5a; 32],
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime");
    let p = rt
        .prepare_cached(&guest_wasm(guest))
        .await
        .expect("prepare");
    let rref = RunRef {
        partition: run.partition().0,
        run_id: run.to_string(),
    };
    let sink = Arc::new(FakeSink::new());
    sink.seed(rref.clone(), journal.clone(), 1);
    let outcome = rt
        .activate(
            &p,
            Activate {
                run: rref.clone(),
                attempt: 1,
                module_sha: p.sha,
                journal,
                inputs: None,
                cancel: CancellationToken::new(),
            },
            sink.clone(),
        )
        .await;
    (outcome, sink.appended_events(&rref), sink.journal(&rref))
}

/// Replay must append nothing and produce the stored result bytes.
async fn assert_replays_identically(s: &TestServer, guest: &str, run: &RunId) {
    let journal = s.journal(run).await;
    let stored = s.result_bytes(run).await.expect("stored result");
    let (outcome, appended, recorded) = replay_fresh(guest, run, journal.clone()).await;
    assert_eq!(appended, 0, "{run}: replay appended events");
    let ActivationOutcome::Completed { output } = outcome else {
        panic!("{run}: replay outcome {outcome:?}");
    };
    assert_eq!(
        output.data.as_slice(),
        stored.as_slice(),
        "{run}: output bytes"
    );
    assert_eq!(recorded, journal, "{run}: recorded journal differs");
}

/// splitmix64 (test-side jitter/shape source; no `rand` dependency).
struct Mix(u64);

impl Mix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

// ---------------------------------------------------------------------------------
// WF-03: gather/race replay identity under concurrent signals + a forced restart
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gather_race() {
    // Default hot wait (the run parks hot and is killed by the restart mid-activation).
    let mut s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.deploy_guest("race-fuzz", None).await;
    let names = ["a", "b", "a", "c"];
    let mut mid_run = 0;
    for seed in [3u64, 17, 42, 99, 1234] {
        let mut mix = Mix(seed);
        // N=6 sleeps, mixed equal and unequal durations in 50..=400 ms; the 400 ms one
        // keeps the run alive past the restart point.
        let a = mix.range(50, 400);
        let b = mix.range(50, 400);
        let sleeps = vec![50, 50, a, a, b, 400];
        let run = s
            .start_rest(
                "race-fuzz",
                json!({"input": {"seed": seed, "sleeps_ms": sleeps, "signals": names, "nest": true}}),
            )
            .await;
        // K=4 signal waits; 4 concurrent senders with random jitter.
        let mut senders = Vec::new();
        for (i, name) in names.iter().enumerate() {
            let http = s.http.clone();
            let url = s.url(&format!("/v1/runs/{run}/signals/{name}"));
            let jitter = Duration::from_millis(mix.range(0, 150));
            senders.push(tokio::spawn(async move {
                sleep(jitter).await;
                let r = http
                    .post(url)
                    .json(&json!({"payload": {"sender": i}}))
                    .send()
                    .await
                    .expect("signal");
                assert_eq!(r.status(), StatusCode::OK);
            }));
        }
        for t in senders {
            t.await.expect("sender");
        }
        // Under heavy machine load the run may already be done; at least one seed must be
        // restarted mid-run (asserted after the loop).
        if !s.run_record(&run).status.is_terminal() {
            mid_run += 1;
        }
        s.restart().await;
        s.wait_ready(Duration::from_secs(10)).await;
        let rec = s.wait_record(&run, LIMIT, |r| r.status.is_terminal()).await;
        assert_eq!(rec.status, RunStatus::Completed, "seed {seed}");

        let journal = s.journal(&run).await;
        let out = result_of(&s, &run).await;
        let ops = out["ops"].as_array().expect("ops");
        assert_eq!(ops.len(), 10, "seed {seed}: every op reported once: {out}");
        assert!(
            !wait_resolved(&journal).is_empty(),
            "seed {seed}: no WaitResolved"
        );
        assert_eq!(n_kind(&journal, EventKind::SignalPayload), 4, "seed {seed}");
        // Fresh Runtime + recording sink: same bytes, zero appends, same WaitResolved.
        let stored = s.result_bytes(&run).await.expect("stored");
        let (outcome, appended, recorded) = replay_fresh("race-fuzz", &run, journal.clone()).await;
        assert_eq!(appended, 0, "seed {seed}: replay appended events");
        let ActivationOutcome::Completed { output } = outcome else {
            panic!("seed {seed}: replay outcome {outcome:?}");
        };
        assert_eq!(output.data.as_slice(), stored.as_slice(), "seed {seed}");
        assert_eq!(
            wait_resolved(&recorded),
            wait_resolved(&journal),
            "seed {seed}"
        );
    }
    println!("WF-03: {mid_run}/5 seeds restarted mid-run");
    assert!(mid_run >= 1, "no seed was restarted mid-run");
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// WF-05: signal timeout, signal after the timeout, buffered FIFO
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_timeout() {
    let mut s = server().await;
    s.deploy_guest("signals", None).await;

    // Timeout: the guest sees Err(Timeout) and returns "timeout".
    let run = s
        .start_rest(
            "signals",
            json!({"input": {"mode": "timeout", "timeout_ms": 200}}),
        )
        .await;
    s.wait_status(&run, RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &run).await, json!("timeout"));
    let j = s.journal(&run).await;
    assert_eq!(n_kind(&j, EventKind::WaitTimedOut), 1, "{:?}", kinds(&j));
    // A signal after the timeout: the run is terminal, so 409 run_terminal (05-13: apply
    // checks terminal first; nothing is buffered on a terminal run).
    let (st, v) = signal(&s, &run, "x", json!("late")).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(err_reason(&v), "run_terminal", "{v}");
    assert_eq!(s.journal(&run).await.len(), j.len(), "nothing journaled");

    // Signals sent before the wait exists are buffered and consumed FIFO.
    let run = s
        .start_rest(
            "signals",
            json!({"input": {"mode": "before", "sleep_ms": 1500}}),
        )
        .await;
    wait_sleeping(&s, &run).await;
    signal_ok(&s, &run, "x", json!("first")).await;
    signal_ok(&s, &run, "x", json!("second")).await;
    s.wait_status(&run, RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &run).await, json!("first"));
    let j = s.journal(&run).await;
    let payloads: Vec<u64> = j
        .iter()
        .filter(|e| e.body.kind() == EventKind::SignalPayload)
        .map(|e| e.seq)
        .collect();
    let wait = j
        .iter()
        .find(|e| e.body.kind() == EventKind::SignalWaitStarted)
        .expect("wait")
        .seq;
    assert_eq!(payloads.len(), 2);
    assert!(
        payloads.iter().all(|p| *p < wait),
        "buffered before the wait"
    );
    let matched: Vec<(u64, u64)> = j
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::SignalMatched(m) => Some((m.op, m.payload_seq)),
            _ => None,
        })
        .collect();
    assert_eq!(
        matched,
        vec![(wait, payloads[0])],
        "the oldest buffered signal"
    );
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// WF-08: patched in-flight
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn patched_in_flight() {
    let mut s = server().await;
    // patch-v1 and patch-v2 both export patch-demo@1; bindings are write-once per tenant,
    // so v2 is deployed under tenant `acme` and v3 (the control) under `beta` (their blobs
    // are then in the shared store).
    s.deploy_guest("patch-v1", None).await;
    let v2 = s.deploy_guest("patch-v2", Some("acme")).await;
    let v3 = s.deploy_guest("patch-v3", Some("beta")).await;

    let old = s.start_rest("patch-demo", json!({"input": null})).await;
    // Control: patch-v3 runs C unconditionally, so a v1 journal replayed on it diverges.
    // Its failure proves the override really swaps the module in this harness.
    let control = s.start_rest("patch-demo", json!({"input": null})).await;
    wait_sleeping(&s, &old).await;
    wait_sleeping(&s, &control).await;
    assert_eq!(
        step_names(&s.journal(&old).await),
        vec!["step_a".to_owned()]
    );
    // Restart with the in-flight runs re-pinned to v2 (and v3 for the control).
    let target = old;
    s.hooks.module_override.set(Some(Arc::new(move |r: &RunId| {
        if *r == target {
            Some(v2)
        } else if *r == control {
            Some(v3)
        } else {
            None
        }
    })));
    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.wait_status(&old, RunStatus::Completed, LIMIT).await;
    assert_eq!(
        result_of(&s, &old).await,
        json!(["A", "B"]),
        "old path on v2"
    );
    let j = s.journal(&old).await;
    assert_eq!(n_kind(&j, EventKind::PatchMarker), 0, "{:?}", kinds(&j));
    assert!(
        s.jobs_of(&old).iter().any(|j| j.activation.is_some()),
        "the run was activated after the restart"
    );
    let rec = s
        .wait_record(&control, LIMIT, |r| r.status.is_terminal())
        .await;
    assert_eq!(
        rec.status,
        RunStatus::Failed,
        "the v3 control must diverge: {}",
        rec.terminal_reason
    );
    assert!(rec.resumable, "nondeterminism is an executor-level failure");

    // A fresh run on v2 takes the new path with exactly one marker.
    let fresh = s
        .start_rest("patch-demo", json!({"input": null, "tenant": "acme"}))
        .await;
    s.wait_status(&fresh, RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &fresh).await, json!(["A", "C", "B"]));
    let j = s.journal(&fresh).await;
    assert_eq!(n_kind(&j, EventKind::PatchMarker), 1, "{:?}", kinds(&j));
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// WF-09: run timeout and schedule-to-start
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_timeouts() {
    let mut s = server().await;
    let sha = s.deploy_guest("timeouts", None).await;

    // Sleeping run.
    let run = s
        .start_raw("timeouts", 1, br#"{"mode":"sleep_forever"}"#, &sha, |r| {
            r.run_timeout_ms = 500;
        })
        .await;
    let rec = s.wait_status(&run, RunStatus::TimedOut, LIMIT).await;
    assert_eq!(rec.terminal_reason, "run_timeout");
    assert!(rec.resumable);
    let j = s.journal(&run).await;
    assert_eq!(
        kinds(&j).last(),
        Some(&EventKind::RunTerminated),
        "{:?}",
        kinds(&j)
    );
    assert_eq!(terminated_reasons(&j), vec!["run_timeout".to_owned()]);
    let (st, v) = s.get_json(&format!("/v1/runs/{run}")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["status"], "timed_out", "{v}");

    // Running run (one long activation): TimedOut and the activation is revoked.
    let run = s
        .start_raw(
            "timeouts",
            1,
            br#"{"mode":"busy_steps","n":50000000}"#,
            &sha,
            |r| r.run_timeout_ms = 1_000,
        )
        .await;
    let t0 = Instant::now();
    while s.h().runtime().live_activations() == 0 {
        assert!(t0.elapsed() < LIMIT, "busy run never activated");
        sleep(Duration::from_millis(10)).await;
    }
    let rec = s.wait_status(&run, RunStatus::TimedOut, LIMIT).await;
    assert_eq!(rec.terminal_reason, "run_timeout");
    wait_no_live(&s).await;
    s.shutdown().await;
}

/// D24: the journal of a run that never activated is exactly `RunStarted` (seq 0) then
/// `RunTerminated{reason}` (seq 1): no attempt was granted and no step ran.
async fn assert_never_started_then(s: &TestServer, run: &RunId, reason: &str) {
    let j = s.journal(run).await;
    assert_eq!(
        kinds(&j),
        vec![EventKind::RunStarted, EventKind::RunTerminated],
        "never started"
    );
    assert_eq!(
        j.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![0, 1],
        "contiguous from seq 0"
    );
    assert_eq!(terminated_reasons(&j), vec![reason.to_owned()]);
    assert!(
        s.run_record(run).current_attempt.is_none(),
        "never granted, so no step ran"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_to_start_without_capacity() {
    // No executor capacity: runs stay Ready, never granted.
    let mut s = server_with(|c| c.executor.slots = 0).await;
    s.deploy_guest("timeouts", None).await;
    let run = s
        .start_rest(
            "timeouts",
            json!({"input": {"mode": "sleep_ms", "n": 10}, "schedule_to_start_timeout": "300ms"}),
        )
        .await;
    let (_, v) = s.get_json(&format!("/v1/runs/{run}")).await;
    assert_eq!(v["schedule_to_start_ms"], 300, "{v}");
    let t0 = Instant::now();
    let rec = s.wait_status(&run, RunStatus::TimedOut, LIMIT).await;
    assert_eq!(rec.terminal_reason, "schedule_to_start");
    assert!(t0.elapsed() < Duration::from_secs(10));
    // D24 (05-25, amends D3): the API journals RunStarted at seq 0 inside StartRun, so a
    // never-started run's journal is RunStarted then the terminal marker, and no step ran.
    assert_never_started_then(&s, &run, "schedule_to_start").await;
    assert_eq!(rec.next_seq, 2);
    let (_, v) = s.get_json(&format!("/v1/runs/{run}")).await;
    assert_eq!(v["status"], "timed_out");

    // Without a schedule-to-start timeout the run just waits.
    let waiting = s
        .start_rest("timeouts", json!({"input": {"mode": "sleep_ms", "n": 10}}))
        .await;
    // Terminate before start (D24): RunStarted (seq 0) then RunTerminated (seq 1); the
    // "RunTerminated only when next_seq > 0" rule now always journals it.
    lifecycle_ok(&s, &waiting, "terminate").await;
    let rec = s.wait_status(&waiting, RunStatus::Terminated, LIMIT).await;
    assert_eq!(rec.next_seq, 2);
    assert_never_started_then(&s, &waiting, &rec.terminal_reason).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_to_start_with_capacity_never_fires() {
    let mut s = server().await;
    s.deploy_guest("timeouts", None).await;
    // Started at once: the 1 s s2s is disarmed by the start, the 1.5 s sleep completes.
    let run = s
        .start_rest(
            "timeouts",
            json!({"input": {"mode": "sleep_ms", "n": 1500}, "schedule_to_start_timeout": "1s"}),
        )
        .await;
    s.wait_status(&run, RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &run).await, json!("slept"));
    // Delayed start (1.5 s) with a 1 s s2s: measured from ready_at, not from the request
    // (from the request it would fire before the run is even ready).
    let run = s
        .start_rest(
            "timeouts",
            json!({"input": {"mode": "sleep_ms", "n": 10}, "delay_ms": 1500, "schedule_to_start_timeout": "1s"}),
        )
        .await;
    let rec = s.wait_record(&run, LIMIT, |r| r.status.is_terminal()).await;
    assert_eq!(rec.status, RunStatus::Completed, "{}", rec.terminal_reason);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// API-05: cancel runs guest cleanup, terminate runs no guest code, retry modes
// ---------------------------------------------------------------------------------

fn saga_input(id: &str) -> Value {
    json!({"input": {"id": id, "steps": 3, "sleep_ms": 60_000, "signal_timeout_ms": 60_000}})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_cleanup() {
    let mut s = server().await;
    s.deploy_guest("crash-saga", None).await;
    let run = s.start_rest("crash-saga", saga_input("c1")).await;
    wait_sleeping(&s, &run).await;
    assert_eq!(
        step_names(&s.journal(&run).await),
        ["step0", "step1", "step2"]
    );
    lifecycle_ok(&s, &run, "cancel").await;
    s.wait_status(&run, RunStatus::Cancelled, LIMIT).await;
    assert_eq!(
        step_names(&s.journal(&run).await),
        ["step0", "step1", "step2", "undo2", "undo1", "undo0"],
        "shielded compensation ran after the cancel"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_runs_no_guest_code() {
    let mut s = server().await;
    s.deploy_guest("crash-saga", None).await;
    let run = s.start_rest("crash-saga", saga_input("t1")).await;
    wait_sleeping(&s, &run).await;
    let steps = step_names(&s.journal(&run).await);
    assert_eq!(steps, ["step0", "step1", "step2"]);
    let v = lifecycle_ok(&s, &run, "terminate").await;
    assert_eq!(v["status"], "terminated");
    s.wait_status(&run, RunStatus::Terminated, LIMIT).await;
    let activations = s
        .hooks
        .activations
        .load(std::sync::atomic::Ordering::Relaxed);
    sleep(Duration::from_millis(500)).await;
    let j = s.journal(&run).await;
    assert_eq!(step_names(&j), steps, "step count frozen, no compensation");
    assert_eq!(kinds(&j).last(), Some(&EventKind::RunTerminated));
    assert_eq!(
        s.hooks
            .activations
            .load(std::sync::atomic::Ordering::Relaxed),
        activations,
        "no activation after the terminate"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_after_signal_wait_timeout() {
    // B2a: TimedOut while waiting on a signal -> resumed -> signalled -> completes.
    let mut s = server().await;
    let sha = s.deploy_guest("signals", None).await;
    let run = s
        .start_raw("signals", 1, br#"{"mode":"long_wait"}"#, &sha, |r| {
            r.run_timeout_ms = 300;
        })
        .await;
    let rec = s.wait_status(&run, RunStatus::TimedOut, LIMIT).await;
    assert_eq!(rec.terminal_reason, "run_timeout");
    let op = s
        .journal(&run)
        .await
        .iter()
        .find(|e| e.body.kind() == EventKind::SignalWaitStarted)
        .expect("the wait was journaled before the timeout")
        .seq;

    let v = lifecycle_ok(&s, &run, "retry").await;
    assert_eq!(v["mode"], "resumed", "{v}");
    assert_eq!(v["run_id"], run.to_string());
    // The first activation after the resume re-arms the wait, then suspends on it.
    s.wait_record(&run, LIMIT, |r| {
        r.open_waits == 1 && r.status == RunStatus::Scheduled && r.resumed_count == 1
    })
    .await;
    signal_ok(&s, &run, "x", json!(7)).await;
    let rec = s.wait_status(&run, RunStatus::Completed, LIMIT).await;
    assert_eq!(rec.resumed_count, 1);
    assert_eq!(result_of(&s, &run).await, json!(7));

    let j = s.journal(&run).await;
    let payload = j
        .iter()
        .find(|e| e.body.kind() == EventKind::SignalPayload)
        .expect("SignalPayload")
        .seq;
    let matched: Vec<(u64, u64)> = j
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::SignalMatched(m) => Some((m.op, m.payload_seq)),
            _ => None,
        })
        .collect();
    assert_eq!(
        matched,
        vec![(op, payload)],
        "the original wait op is matched"
    );
    assert!(
        !j.iter().any(|e| matches!(&e.body,
            EventBody::TimerFired(t) if t.op == op)),
        "no TimerFired for the signal op"
    );
    assert!(
        !j.iter().any(|e| matches!(&e.body,
            EventBody::WaitTimedOut(w) if w.op == op)),
        "no WaitTimedOut for the signal op"
    );
    assert_eq!(terminated_reasons(&j), vec!["run_timeout".to_owned()]);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_after_sleep_timeout() {
    // B2b: TimedOut while sleeping; resumed after the sleep's due time passed: the re-armed
    // past-due timer fires at once and no second run timeout fires.
    let mut s = server().await;
    let sha = s.deploy_guest("timeouts", None).await;
    let t0 = Instant::now();
    let run = s
        .start_raw(
            "timeouts",
            1,
            br#"{"mode":"sleep_ms","n":800}"#,
            &sha,
            |r| {
                r.run_timeout_ms = 200;
            },
        )
        .await;
    let rec = s.wait_status(&run, RunStatus::TimedOut, LIMIT).await;
    assert_eq!(rec.terminal_reason, "run_timeout");
    let wait = Duration::from_millis(1_200).saturating_sub(t0.elapsed());
    sleep(wait).await;

    let v = lifecycle_ok(&s, &run, "retry").await;
    assert_eq!(v["mode"], "resumed", "{v}");
    assert_eq!(v["run_id"], run.to_string());
    let t1 = Instant::now();
    let rec = s
        .wait_record(&run, LIMIT, |r| {
            r.status.is_terminal() && r.resumed_count == 1
        })
        .await;
    assert_eq!(rec.status, RunStatus::Completed, "{}", rec.terminal_reason);
    assert!(t1.elapsed() < Duration::from_secs(5), "{:?}", t1.elapsed());
    assert_eq!(result_of(&s, &run).await, json!("slept"));
    let j = s.journal(&run).await;
    assert_eq!(
        terminated_reasons(&j),
        vec!["run_timeout".to_owned()],
        "no new run timeout after the resume"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_restart_links_a_new_run() {
    let mut s = server().await;
    s.deploy_guest("sdk-basic", None).await;
    let run = s
        .start_rest("sdk-basic", json!({"input": {"wrong": true}}))
        .await;
    let rec = s.wait_status(&run, RunStatus::Failed, LIMIT).await;
    assert!(!rec.resumable);
    let v = lifecycle_ok(&s, &run, "retry").await;
    assert_eq!(v["mode"], "restarted", "{v}");
    let new = id_of(&v);
    assert_ne!(new, run);
    assert_eq!(s.run_record(&new).retry_of, Some(run));
    s.wait_status(&new, RunStatus::Failed, LIMIT).await;
    // D24 (05-25): restart's StartRun carries RunStarted (seq 0) with the predecessor's
    // input and no continue-as-new link.
    let j = s.journal(&new).await;
    let EventBody::RunStarted(rs) = &j[0].body else {
        panic!("seq 0 is not RunStarted: {:?}", kinds(&j))
    };
    assert_eq!(j[0].seq, 0);
    assert_eq!(rs.workflow, "sdk-basic");
    assert_eq!(rs.continued_from, None);
    assert_eq!(
        serde_json::from_slice::<Value>(&rs.input.data).expect("json input"),
        json!({"wrong": true})
    );
    assert_eq!(n_kind(&j, EventKind::RunStarted), 1);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// B1: signals sent right after start, before any activation
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_immediately_after_start() {
    // No capacity, so nothing activates until the restart with slots.
    let mut s = server_with(|c| c.executor.slots = 0).await;
    s.deploy_guest("signals", None).await;
    let run = s
        .start_rest("signals", json!({"input": {"mode": "fifo"}}))
        .await;
    for p in ["a", "b", "c"] {
        signal_ok(&s, &run, "x", json!(p)).await;
    }
    // D24 (05-25): RunStarted is journaled at seq 0 by StartRun, so the signals sent before
    // any activation are journaled directly at seq 1..=3, in arrival order (no B1 Pending
    // slot on this path).
    let rec = s.run_record(&run);
    assert!(rec.current_attempt.is_none(), "not activated yet");
    assert_eq!(rec.next_seq, 4, "RunStarted + three SignalPayloads");
    let pre = s.journal(&run).await;
    assert_eq!(
        kinds(&pre),
        vec![
            EventKind::RunStarted,
            EventKind::SignalPayload,
            EventKind::SignalPayload,
            EventKind::SignalPayload
        ]
    );
    // "Unpause": restart with capacity.
    s.cfg.executor.slots = 64;
    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.wait_status(&run, RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &run).await, json!(["a", "b", "c"]));

    let j = s.journal(&run).await;
    assert_eq!(j[0].body.kind(), EventKind::RunStarted);
    let payloads: Vec<u64> = j
        .iter()
        .filter(|e| e.body.kind() == EventKind::SignalPayload)
        .map(|e| e.seq)
        .collect();
    assert_eq!(payloads.len(), 3, "{:?}", kinds(&j));
    assert_eq!(
        payloads,
        vec![1, 2, 3],
        "journaled at seq >= 1 before activation"
    );
    let matched: Vec<(u64, u64)> = j
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::SignalMatched(m) => Some((e.seq, m.payload_seq)),
            _ => None,
        })
        .collect();
    assert_eq!(
        matched.iter().map(|m| m.1).collect::<Vec<_>>(),
        payloads,
        "matched in arrival order"
    );
    for (m, p) in matched {
        assert!(m > p, "SignalMatched {m} after its SignalPayload {p}");
    }
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// API-07 tokens and D21 large payloads
// ---------------------------------------------------------------------------------

async fn complete_token(s: &TestServer, token: &str, payload: Value) -> (StatusCode, Value) {
    s.post_json(
        "/v1/tokens/complete",
        &json!({"token": token, "payload": payload}),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokens() {
    let mut s = server().await;
    s.deploy_guest("tokens", None).await;
    let run = s.start_rest("tokens", json!({"input": {}})).await;
    let token = s.published_token(&run.to_string(), LIMIT).await;
    let (st, v) = complete_token(&s, &token, json!({"ok": 1})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    s.wait_status(&run, RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &run).await, json!({"ok": 1}));
    let (st, v) = complete_token(&s, &token, json!(2)).await;
    assert_eq!(st, StatusCode::CONFLICT, "duplicate: {v}");

    let run = s
        .start_rest("tokens", json!({"input": {"timeout_ms": 200}}))
        .await;
    let token = s.published_token(&run.to_string(), LIMIT).await;
    s.wait_status(&run, RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &run).await, json!("timeout"));
    let (st, v) = complete_token(&s, &token, json!(3)).await;
    assert_eq!(st, StatusCode::CONFLICT, "expired: {v}");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_payloads() {
    let mut s = server().await;
    s.deploy_guest("signals", None).await;
    s.deploy_guest("tokens", None).await;

    let sig_run = s
        .start_rest("signals", json!({"input": {"mode": "long_wait"}}))
        .await;
    s.wait_record(&sig_run, LIMIT, |r| {
        r.open_waits == 1 && r.status == RunStatus::Scheduled
    })
    .await;
    let big_sig = json!({"sig": "s".repeat(MIB)});
    signal_ok(&s, &sig_run, "x", big_sig.clone()).await;

    let tok_run = s.start_rest("tokens", json!({"input": {}})).await;
    let token = s.published_token(&tok_run.to_string(), LIMIT).await;
    let big_tok = json!({"tok": "t".repeat(MIB)});
    let (st, v) = complete_token(&s, &token, big_tok.clone()).await;
    assert_eq!(st, StatusCode::OK, "{v}");

    s.wait_status(&sig_run, RunStatus::Completed, LIMIT).await;
    s.wait_status(&tok_run, RunStatus::Completed, LIMIT).await;
    let sig_bytes = serde_json::to_vec(&big_sig).expect("json");
    let tok_bytes = serde_json::to_vec(&big_tok).expect("json");
    assert_eq!(s.result_bytes(&sig_run).await.expect("result"), sig_bytes);
    assert_eq!(s.result_bytes(&tok_run).await.expect("result"), tok_bytes);

    // Restart, then replay both from their (blob-backed) journals.
    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;
    assert_replays_identically(&s, "signals", &sig_run).await;
    assert_replays_identically(&s, "tokens", &tok_run).await;
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Continue-as-new chains
// ---------------------------------------------------------------------------------

/// Follow `continued_to` from `first`: every run of the chain that exists so far, in
/// order. A successor is created by the predecessor's outbox `StartRun` after
/// `continued_to` is committed, so a link may point at a run that does not exist yet.
fn chain(s: &TestServer, first: &RunId) -> Vec<RunId> {
    let mut out = vec![*first];
    while let Some(next) = s.run_record(out.last().expect("run")).continued_to {
        let exists = s
            .handle_of(&next)
            .read_run(&next)
            .expect("read run")
            .is_some();
        if !exists {
            break;
        }
        out.push(next);
    }
    out
}

async fn assert_chain(s: &TestServer, first: &RunId, runs: usize, n_final: u64) {
    let c = chain(s, first);
    assert_eq!(c.len(), runs, "{c:?}");
    for (i, run) in c.iter().enumerate() {
        let rec = s.run_record(run);
        if i > 0 {
            assert_eq!(rec.continued_from, Some(c[i - 1]), "lineage of {run}");
        }
        if i + 1 < c.len() {
            assert_eq!(rec.status, RunStatus::ContinuedAsNew, "{run}");
            let (_, v) = s.get_json(&format!("/v1/runs/{run}")).await;
            assert_eq!(v["result"]["next_run_id"], c[i + 1].to_string(), "{v}");
        } else {
            assert_eq!(rec.status, RunStatus::Completed, "{run}");
            assert_eq!(result_of(s, run).await, json!(n_final));
        }
    }
    // Exactly-once successors: no other can-chain run exists.
    let mut listed = s.list_all("workflow=can-chain").await;
    listed.sort();
    let mut want: Vec<String> = c.iter().map(ToString::to_string).collect();
    want.sort();
    assert_eq!(listed, want, "no duplicate successor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continue_as_new() {
    let mut s = server().await;
    s.deploy_guest("can-chain", None).await;

    // {n: 1, max: 4}: runs n = 1, 2, 3 continue as new, n = 4 completes with 4.
    let first = s
        .start_rest("can-chain", json!({"input": {"n": 1, "max": 4}}))
        .await;
    let (st, v) = s
        .get_json(&format!(
            "/v1/runs/{first}/result?wait=10s&follow_continuations=true"
        ))
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["completed"], true, "{v}");
    assert_eq!(v["result"], 4, "{v}");
    assert_eq!(v["status"], "completed");
    assert_ne!(
        v["run_id"],
        first.to_string(),
        "answered by the last successor"
    );
    assert_chain(&s, &first, 4, 4).await;

    // A restart in the middle of a longer chain: once it has at least 3 runs, appends
    // are gated (`pause_before_append` with 0 permits) so the chain stalls part-way; the
    // restart revokes the stalled job, the gate is lifted, and the chain completes with
    // exactly one successor per hop (outbox exactly-once).
    const MAX: u64 = 30;
    let second = s
        .start_rest("can-chain", json!({"input": {"n": 1, "max": MAX}}))
        .await;
    let t0 = Instant::now();
    while chain(&s, &second).len() < 3 {
        assert!(t0.elapsed() < LIMIT, "the chain did not progress");
        sleep(Duration::from_millis(1)).await;
    }
    *s.hooks
        .pause_before_append
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(Arc::new(Semaphore::new(0)));
    sleep(Duration::from_millis(300)).await;
    let mid = chain(&s, &second);
    let last = s.run_record(mid.last().expect("run"));
    assert!(
        last.status != RunStatus::Completed,
        "the chain finished before the restart ({} runs)",
        mid.len()
    );
    s.restart().await;
    *s.hooks
        .pause_before_append
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = None;
    s.wait_ready(Duration::from_secs(10)).await;
    let (st, v) = s
        .get_json(&format!(
            "/v1/runs/{second}/result?wait=30s&follow_continuations=true"
        ))
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["result"], MAX, "{v}");
    let c = chain(&s, &second);
    println!(
        "continue-as-new: stalled at {} of {MAX} runs, restarted, completed",
        mid.len()
    );
    assert_eq!(c.len() as u64, MAX, "{c:?}");
    let mut all = s.list_all("workflow=can-chain").await;
    all.sort();
    let mut want: Vec<String> = chain(&s, &first)
        .iter()
        .chain(c.iter())
        .map(ToString::to_string)
        .collect();
    want.sort();
    assert_eq!(
        all, want,
        "exactly one successor per hop across the restart"
    );
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// OPS-06: retention expiry and dedupe reuse
// ---------------------------------------------------------------------------------

/// Poll until `GET /v1/runs/{run}` is 404; returns how long it took.
async fn wait_expired(s: &TestServer, run: &RunId, limit: Duration) -> Duration {
    let t0 = Instant::now();
    loop {
        let (st, _) = s.get_json(&format!("/v1/runs/{run}")).await;
        if st == StatusCode::NOT_FOUND {
            return t0.elapsed();
        }
        assert!(t0.elapsed() < limit, "{run} not expired after {limit:?}");
        sleep(Duration::from_millis(50)).await;
    }
}

async fn assert_gone(s: &TestServer, run: &RunId) {
    let (st, v) = s.get_json(&format!("/v1/runs/{run}")).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "GET {v}");
    let (st, v) = s.get_json(&format!("/v1/runs/{run}/history")).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "history {v}");
    let listed = s.list_all("workflow=signals").await;
    assert!(!listed.contains(&run.to_string()), "still listed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention() {
    let mut s = server().await;
    s.deploy_guest("signals", None).await;
    let (st, v) = s
        .http
        .put(s.url("/v1/workflows/signals/policy"))
        .json(&json!({"retention": "1s"}))
        .send()
        .await
        .map(|r| (r.status(), r))
        .expect("PUT policy");
    assert_eq!(st, StatusCode::OK, "{:?}", v.text().await);

    // Completed with idempotency key K, expires after ~1 s, K then creates a new run.
    let body = json!({"input": {"mode": "timeout", "timeout_ms": 10}, "idempotency_key": "K"});
    let old = s.start_rest("signals", body.clone()).await;
    let rec = s.wait_status(&old, RunStatus::Completed, LIMIT).await;
    assert_eq!(rec.retention_ms, 1_000, "policy-resolved retention");
    let (st, v) = s.post_start("signals", &body).await;
    assert_eq!(st, StatusCode::OK, "dedupe before expiry: {v}");
    assert_eq!(id_of(&v), old);
    let took = wait_expired(&s, &old, Duration::from_secs(10)).await;
    println!("OPS-06: 1 s retention expired {took:?} after completion was observed");
    assert_gone(&s, &old).await;
    let new = s.start_rest("signals", body.clone()).await;
    assert_ne!(
        new, old,
        "the idempotency key creates a new run after expiry"
    );
    s.wait_status(&new, RunStatus::Completed, LIMIT).await;

    // A run retried (resumed) before its expiry is not deleted by the old expiry timer.
    let resumed = s
        .start_rest("signals", json!({"input": {"mode": "long_wait"}}))
        .await;
    wait_sleeping(&s, &resumed).await;
    lifecycle_ok(&s, &resumed, "terminate").await;
    s.wait_status(&resumed, RunStatus::Terminated, LIMIT).await;
    let v = lifecycle_ok(&s, &resumed, "retry").await;
    assert_eq!(v["mode"], "resumed");
    sleep(Duration::from_millis(2_500)).await;
    let (st, v) = s.get_json(&format!("/v1/runs/{resumed}")).await;
    assert_eq!(st, StatusCode::OK, "deleted by the stale expiry timer: {v}");
    assert!(!s.run_record(&resumed).status.is_terminal());
    s.wait_record(&resumed, LIMIT, |r| {
        r.open_waits == 1 && r.status == RunStatus::Scheduled
    })
    .await;
    signal_ok(&s, &resumed, "x", json!("late")).await;
    s.wait_status(&resumed, RunStatus::Completed, LIMIT).await;
    wait_expired(&s, &resumed, Duration::from_secs(10)).await;
    assert_gone(&s, &resumed).await;

    // Restart: the expiry state is rebuilt. A 4 s run expires after the restart; the
    // expired runs stay gone and the live dedupe row still answers K.
    let restarted = s
        .start_rest(
            "signals",
            json!({"input": {"mode": "timeout", "timeout_ms": 10}, "retention": "4s"}),
        )
        .await;
    s.wait_status(&restarted, RunStatus::Completed, LIMIT).await;
    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;
    assert_gone(&s, &old).await;
    assert_gone(&s, &resumed).await;
    let (st, v) = s.post_start("signals", &body).await;
    assert!(
        st == StatusCode::OK && id_of(&v) == new || st == StatusCode::CREATED,
        "K after restart: {st} {v}"
    );
    wait_expired(&s, &restarted, Duration::from_secs(15)).await;
    assert_gone(&s, &restarted).await;
    s.shutdown().await;
}
