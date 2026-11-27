//! Remote execution end to end (plan 07-12, WORK-01/03/04/06, D-W3, D-W27): an in-process
//! server whose dispatchers place `workers` queues on in-process `ostro_worker`s (or raw
//! protocol clients) through `NodePlacement` + `PlacementExecutor` + `RemoteRunner`.
//!
//! - a run executed on a worker journals exactly what the same run journals embedded
//!   (`journal_shape`: kinds, command hashes, step and run outcomes), including step
//!   retries and attempt-failure retries; both paths build their sinks through
//!   `EngineExecutor::sink_for` with the same bridge instance;
//! - a worker killed mid-activation abandons the attempt (no failure counted) and the run
//!   completes on a second worker;
//! - `StartEffect` is verified on the leader: zombie tokens (after a revoke) get
//!   FAILED_PRECONDITION, mismatched redacted names and non-placeholder `{{` values are
//!   rejected, and the mock provider sees no request;
//! - a canary secret in a templated, redacted header reaches the provider but no message
//!   on the worker channel, in either direction;
//! - selectors route a `gpu=true` queue only to the labelled worker.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7_workers.rs"]
mod p7_workers;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{
    BridgeTestOpts, ServerOpts, TestServer, guest_wasm, now_ms, start_server_with_bridge, tmpfs_dir,
};
use ostro_meta::records::PLACEMENT_WORKERS;
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, QueuePolicy};
use ostro_partition::leader::placement::ReservationKind;
use ostro_partition::store::values::RunStatus;
use ostro_partition::{ApplyOutcome, ExecOutcome};
use ostro_server::workers::service::WireDir;
use ostro_testkit::canary::{Canary, Quiet, spawn_quiet};
use ostro_types::command::{PAUSE_REASON_OPERATOR, PauseRun};
use ostro_types::journal::{
    CommandArgs, CommandHash, CommandMeta, EffectRequest, EffectScheduled, Event, EventBody,
    HASH_VER, Header, HttpRequest, OpCancelled, TimerFired, TimerStarted, command_hash,
    encode_event,
};
use ostro_types::{Command, RunId};
use ostro_worker_proto::v1;
use p7_workers::{ServerWorkers, T, TestWorker, journal_shape};
use serde_json::json;
use sha2::{Digest, Sha256};

const LIMIT: Duration = Duration::from_secs(90);

async fn server() -> TestServer {
    let s = TestServer::start_with(ServerOpts::default(), |_| {}).await;
    s.wait_ready(Duration::from_secs(20)).await;
    s
}

async fn deploy(s: &TestServer, guest: &str) -> [u8; 32] {
    let wasm = guest_wasm(guest);
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{hex}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    sha
}

async fn put(s: &TestServer, record: MetaRecord) {
    let meta = s.h().meta();
    let key = record.key();
    let expected_version = meta.view().read(|st| st.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version,
            record,
        })
        .await
        .expect("meta put");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

/// `queue` runs on workers matching `selector`.
async fn workers_queue(s: &TestServer, queue: &str, selector: &[&str]) {
    put(
        s,
        MetaRecord::QueuePolicy(QueuePolicy {
            queue: queue.into(),
            placement: Some(PLACEMENT_WORKERS),
            selector: selector.iter().map(|e| (*e).to_owned()).collect(),
            ..QueuePolicy::default()
        }),
    )
    .await;
}

async fn start(s: &TestServer, workflow: &str, input: &[u8], sha: &[u8; 32], queue: &str) -> RunId {
    let q = queue.to_owned();
    s.start_raw(workflow, 1, input, sha, move |st| st.queue = q)
        .await
}

async fn terminal(s: &TestServer, run: &RunId) -> ostro_partition::store::values::RunRecord {
    s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await
}

fn remote_jobs(s: &TestServer, run: &RunId) -> Vec<ostro_server::executor::JobRecord> {
    s.jobs_of(run)
}

/// [`journal_shape`] without the timing-dependent interleaving of timer inputs: when
/// `TimerFired` lands relative to the guest's wait decides how many `WaitResolved` a
/// `join!` journals, in any two executions (embedded or not). Seqs are dropped.
///
/// An op id is the seq of the event that scheduled it, so it shifts with that same
/// interleaving, and so does the `CancelOp { op }` command hash (`op` is hashed). An
/// `OpCancelled` is therefore compared by its target's position in the timing-free
/// shape, after checking that its hash is the canonical `CancelOp` hash of its op.
fn timing_free_shape(j: &[Event]) -> Vec<String> {
    let kept: Vec<(String, &Event)> = journal_shape(j)
        .into_iter()
        .zip(j)
        .filter(|(_, e)| {
            !matches!(
                e.body,
                EventBody::TimerFired(_) | EventBody::WaitResolved(_)
            )
        })
        .collect();
    kept.iter()
        .map(|(s, e)| {
            if let EventBody::OpCancelled(c) = &e.body {
                let want = command_hash(c.meta.hash_ver, &CommandArgs::CancelOp { op: c.op })
                    .expect("cancel-op hash");
                assert_eq!(c.meta.hash, want, "OpCancelled hash of op {}", c.op);
                let target = kept
                    .iter()
                    .position(|(_, t)| t.seq == c.op)
                    .unwrap_or_else(|| panic!("OpCancelled targets unknown op {}", c.op));
                return format!("OpCancelled op=#{target} hash_ver={}", c.meta.hash_ver);
            }
            s.split_once(' ')
                .map(|(_, r)| r.to_owned())
                .unwrap_or_else(|| s.clone())
        })
        .collect()
}

/// The CI flake behind `timing_free_shape`'s `OpCancelled` rule (ci-fix3): the same
/// `sdk-basic` tail with one more `TimerFired` journaled before the `select!` timers
/// shifts both timer seqs, so the `CancelOp` hash differs; the shapes must still match,
/// and a cancel of the wrong timer must not.
#[test]
fn timing_free_shape_ignores_timer_input_seq_shift() {
    let meta = |b: u8| CommandMeta {
        hash_ver: HASH_VER,
        hash: CommandHash([b; 16]),
    };
    let cancel = |op: u64| {
        EventBody::OpCancelled(OpCancelled {
            meta: CommandMeta {
                hash_ver: HASH_VER,
                hash: command_hash(HASH_VER, &CommandArgs::CancelOp { op }).expect("hash"),
            },
            op,
        })
    };
    let timer = |b: u8| {
        EventBody::TimerStarted(TimerStarted {
            meta: meta(b),
            fire_at_nanos: 0,
        })
    };
    let fired = |op: u64| EventBody::TimerFired(TimerFired { op });
    let journal = |bodies: Vec<EventBody>| -> Vec<Event> {
        bodies
            .into_iter()
            .enumerate()
            .map(|(i, body)| Event {
                seq: i as u64 + 1,
                ts_nanos: 0,
                body,
            })
            .collect()
    };
    // seq: 1 a, 2 b, 3 fired(1), [4 fired(2)], short, long, fired(short), cancel(long).
    let fast = journal(vec![
        timer(1),
        timer(2),
        fired(1),
        timer(3),
        timer(4),
        fired(4),
        cancel(5),
    ]);
    let slow = journal(vec![
        timer(1),
        timer(2),
        fired(1),
        fired(2),
        timer(3),
        timer(4),
        fired(5),
        cancel(6),
    ]);
    assert_ne!(journal_shape(&fast), journal_shape(&slow));
    assert_eq!(timing_free_shape(&fast), timing_free_shape(&slow));
    let wrong = journal(vec![
        timer(1),
        timer(2),
        fired(1),
        timer(3),
        timer(4),
        fired(5),
        cancel(4),
    ]);
    assert_ne!(timing_free_shape(&fast), timing_free_shape(&wrong));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_run_journal_equals_embedded() {
    let s = server().await;
    let sha = deploy(&s, "sdk-basic").await;
    let lj = deploy(&s, "long-journal").await;
    workers_queue(&s, "wq", &[]).await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let mut w = ws.spawn("w1", 2, &[]).await;
    // Steps only: the journal is fully deterministic, compared event for event.
    let steps = br#"{"steps": 40, "result_bytes": 4096}"#;
    let la = start(&s, "long-journal", steps, &lj, "default").await;
    let lb = start(&s, "long-journal", steps, &lj, "wq").await;
    for r in [&la, &lb] {
        let rec = terminal(&s, r).await;
        assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    }
    assert_eq!(
        journal_shape(&s.journal(&la).await),
        journal_shape(&s.journal(&lb).await)
    );
    assert_eq!(s.result_bytes(&la).await, s.result_bytes(&lb).await);
    // sdk-basic (steps, timers, select, cancel, status): equal up to timer-input timing.
    let input = br#"{"name":"ostro","n":10}"#;
    let a = start(&s, "sdk-basic", input, &sha, "default").await;
    let b = start(&s, "sdk-basic", input, &sha, "wq").await;
    let ra = terminal(&s, &a).await;
    let rb = terminal(&s, &b).await;
    assert_eq!(ra.status, RunStatus::Completed, "embedded: {ra:?}");
    assert_eq!(rb.status, RunStatus::Completed, "remote: {rb:?}");
    let ja = s.journal(&a).await;
    let jb = s.journal(&b).await;
    assert_eq!(timing_free_shape(&ja), timing_free_shape(&jb));
    assert_eq!(s.result_bytes(&a).await, s.result_bytes(&b).await);
    // b really ran on the worker.
    let started = w
        .wait_event(T, |e| {
            matches!(e, ostro_worker::WorkerEvent::ActivationStarted { run_id, .. } if *run_id == b.to_string())
        })
        .await;
    drop(started);
    assert!(s.hooks.remote.activations.load(Ordering::Relaxed) >= 1);
    assert!(
        !remote_jobs(&s, &b).is_empty(),
        "the remote runner recorded b's jobs"
    );
    // One construction site, one bridge (B3).
    let bridge = Arc::as_ptr(s.h().bridge()) as usize;
    let sinks = s.hooks.remote.sinks.lock().expect("sinks").clone();
    assert!(
        sinks.iter().any(|(k, _)| *k == ReservationKind::Embedded),
        "{sinks:?}"
    );
    assert!(
        sinks.iter().any(|(k, _)| *k == ReservationKind::Worker),
        "{sinks:?}"
    );
    assert!(
        sinks.iter().all(|(_, b)| *b == Some(bridge)),
        "every sink uses the server's bridge: {sinks:?}"
    );
    drop(w);
    drop(ws);
    let mut s = s;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_behave_identically() {
    let s = server().await;
    let sha = deploy(&s, "retry-probe").await;
    workers_queue(&s, "wq", &[]).await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let _w = ws.spawn("w1", 4, &[]).await;
    // Step retries inside the activation (StepAttemptFailed events), and attempt-level
    // failures (a trap outside any step: Retry with backoff, then CompleteRun{Failed}).
    for mode in ["exhaust", "recover:2", "trap"] {
        let a = start(&s, "retry-probe", mode.as_bytes(), &sha, "default").await;
        let b = start(&s, "retry-probe", mode.as_bytes(), &sha, "wq").await;
        let ra = terminal(&s, &a).await;
        let rb = terminal(&s, &b).await;
        assert_eq!(ra.status, rb.status, "{mode}: {ra:?} vs {rb:?}");
        assert_eq!(ra.attempt_failures, rb.attempt_failures, "{mode}");
        assert_eq!(
            journal_shape(&s.journal(&a).await),
            journal_shape(&s.journal(&b).await),
            "{mode}"
        );
        assert_eq!(s.result_bytes(&a).await, s.result_bytes(&b).await, "{mode}");
        if mode == "trap" {
            assert!(rb.attempt_failures >= 2, "trap retried remotely: {rb:?}");
            assert!(
                remote_jobs(&s, &b)
                    .iter()
                    .any(|j| matches!(j.outcome, ExecOutcome::Retry { .. })),
                "the remote attempt failure went through the shared mapper"
            );
        }
    }
    let mut s = s;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_loss_regrants_without_failure() {
    let s = server().await;
    let sha = deploy(&s, "cpu-burn").await;
    workers_queue(&s, "wq", &[]).await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let mut w1 = ws.spawn_isolated("w1", 1, &[]).await;
    let run = start(&s, "cpu-burn", br#"{"ms": 1500}"#, &sha, "wq").await;
    let rid = run.to_string();
    w1.wait_event(LIMIT, |e| {
        matches!(e, ostro_worker::WorkerEvent::ActivationStarted { run_id, .. } if *run_id == rid)
    })
    .await;
    w1.kill();
    let jobs = |s: &TestServer| remote_jobs(s, &run);
    p7_workers::wait_for(T, "the attempt is abandoned", || {
        jobs(&s).iter().any(|j| j.outcome == ExecOutcome::Abandoned)
    })
    .await;
    let _w2 = ws.spawn("w2", 1, &[]).await;
    let rec = terminal(&s, &run).await;
    assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    assert_eq!(
        rec.attempt_failures, 0,
        "stream loss is not an attempt failure"
    );
    let js = jobs(&s);
    let lost = js
        .iter()
        .find(|j| j.outcome == ExecOutcome::Abandoned)
        .expect("abandoned job");
    let last = js.last().expect("jobs");
    assert!(
        last.token.n > lost.token.n,
        "re-granted n+1: lost {:?}, last {:?}",
        lost.token,
        last.token
    );
    let mut s = s;
    s.shutdown().await;
}

/// ci-fix7: a worker `Fenced` that the leader never caused (the lease is not revoked and no
/// append of the job was answered `fenced`) re-grants `n + 1`. Before, the dispatcher took
/// it for a newer lease owning the run and dropped its lease without re-granting: the run
/// stayed `Running` with nothing queued or leased (a pre-ci-fix7 worker fenced its own
/// final flush on a drain, keda_sim's pool at 0 with a short run open).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unprovoked_worker_fence_regrants() {
    let s = server().await;
    let sha = deploy(&s, "cpu-burn").await;
    workers_queue(&s, "wq", &[]).await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let mut w = ws.raw("raw-fence", 1).await;
    let run = start(&s, "cpu-burn", br#"{"ms": 1}"#, &sha, "wq").await;
    let act = |m: v1::server_msg::Msg| match m {
        v1::server_msg::Msg::Activate(a) => a,
        _ => unreachable!(),
    };
    let first = act(next_msg(&mut w, "Activate", |m| {
        matches!(m, v1::server_msg::Msg::Activate(_))
    })
    .await);
    w.send(v1::worker_msg::Msg::Done(v1::Done {
        run: first.run.clone(),
        token: first.token,
        outcome: ostro_worker_proto::WireOutcome::Fenced.encode(),
        engine_end_seq: first.journal_len,
    }));
    let second = act(next_msg(&mut w, "the re-grant's Activate", |m| {
        matches!(m, v1::server_msg::Msg::Activate(_))
    })
    .await);
    let n = |a: &v1::Activate| a.token.expect("token").n;
    assert!(
        n(&second) > n(&first),
        "re-granted n+1: {first:?} then {second:?}"
    );
    let js = remote_jobs(&s, &run);
    assert!(
        js.iter().any(|j| j.outcome == ExecOutcome::Abandoned),
        "the fenced attempt is abandoned, not dropped: {js:?}"
    );
    let rec = s
        .wait_record(&run, LIMIT, |r| r.status == RunStatus::Running)
        .await;
    assert_eq!(rec.attempt_failures, 0, "{rec:?}");
    let mut s = s;
    s.shutdown().await;
}

// ------------------------------------------------------------------- StartEffect checks

/// A request-counting HTTP mock (quiet thread).
struct Mock {
    addr: SocketAddr,
    hits: Arc<AtomicUsize>,
    keys: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    _task: tokio::task::JoinHandle<()>,
}

async fn mock_start() -> Mock {
    use axum::body::Body;
    use axum::extract::Request;
    use axum::http::Response;
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = l.local_addr().expect("addr");
    let hits = Arc::new(AtomicUsize::new(0));
    let keys = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (h, k) = (Arc::clone(&hits), Arc::clone(&keys));
    let app = axum::Router::new().fallback(move |req: Request| {
        let (h, k) = (Arc::clone(&h), Arc::clone(&k));
        async move {
            h.fetch_add(1, Ordering::SeqCst);
            if let Some(v) = req.headers().get("x-api-key") {
                k.lock().expect("keys").push(v.as_bytes().to_vec());
            }
            Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(Body::from(r#"{"ok":true}"#))
                .expect("response")
        }
    });
    let task = tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    Mock {
        addr,
        hits,
        keys,
        _task: task,
    }
}

fn quiet_mock() -> Quiet<Mock> {
    spawn_quiet(mock_start())
}

async fn next_msg(
    w: &mut TestWorker,
    what: &str,
    f: impl Fn(&v1::server_msg::Msg) -> bool,
) -> v1::server_msg::Msg {
    let deadline = tokio::time::Instant::now() + T;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match w.next(left).await {
            Some(Ok(m)) if f(&m) => return m,
            Some(Ok(_)) => {}
            other => panic!("waiting for {what}: {other:?}"),
        }
    }
}

fn effect_event(seq: u64, url: &str) -> Event {
    Event {
        seq,
        ts_nanos: now_ms() * 1_000_000,
        body: EventBody::EffectScheduled(EffectScheduled {
            meta: CommandMeta {
                hash_ver: HASH_VER,
                hash: CommandHash([7; 16]),
            },
            request: EffectRequest::Http(HttpRequest {
                method: "POST".into(),
                url: format!("{url}/pay"),
                headers: vec![
                    Header {
                        name: "Idempotency-Key".into(),
                        value: "k-1".into(),
                    },
                    Header {
                        name: "Authorization".into(),
                        value: "[redacted]".into(),
                    },
                ],
                body: None,
                timeout_ms: None,
                idempotency_key: Some("k-1".into()),
                redact: vec!["Authorization".into()],
            }),
        }),
    }
}

fn start_effect(
    w: &TestWorker,
    act: &v1::Activate,
    op: u64,
    values: &[(&str, &str)],
) -> v1::StartEffectRequest {
    v1::StartEffectRequest {
        session_id: w.session_id(),
        run: act.run.clone(),
        token: act.token,
        op,
        redrive: false,
        redacted_values: values
            .iter()
            .map(|(n, v)| v1::Header {
                name: (*n).into(),
                value: (*v).into(),
            })
            .collect(),
        http_retry: None,
    }
}

fn rejected(r: &Result<tonic::Response<v1::StartEffectResponse>, tonic::Status>) -> String {
    match r {
        Ok(resp) => match &resp.get_ref().result {
            Some(v1::start_effect_response::Result::Rejected(m)) => m.clone(),
            other => panic!("expected a rejection, got {other:?}"),
        },
        Err(s) => panic!("expected a rejection, got status {s:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_effect_is_verified_on_the_leader() {
    let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let mock = quiet_mock();
    let url = format!("http://{}", mock.addr);
    let sha = deploy(&s, "fx-canary").await;
    workers_queue(&s, "wq", &[]).await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let mut w = ws.raw("raw1", 1).await;
    let run = start(&s, "fx-canary", b"{}", &sha, "wq").await;
    let act = match next_msg(&mut w, "Activate", |m| {
        matches!(m, v1::server_msg::Msg::Activate(_))
    })
    .await
    {
        v1::server_msg::Msg::Activate(a) => a,
        _ => unreachable!(),
    };
    // The leader journaled RunStarted before activating.
    assert_eq!(act.journal_len, 1);
    assert_eq!(act.module_sha, sha.to_vec());
    // The "worker" appends an EffectScheduled at seq 1.
    let (kind, bytes) = encode_event(&effect_event(1, &url));
    let frame = v1::AppendFrame {
        session_id: w.session_id(),
        run: act.run.clone(),
        token: act.token,
        from_seq: 1,
        event_index: 0,
        chunk: bytes,
        event_end: true,
        last: true,
        kind,
    };
    let r = w
        .client
        .append(tonic::codegen::tokio_stream::iter(vec![frame]))
        .await
        .expect("append")
        .into_inner();
    assert_eq!(r.result, Some(v1::append_result::Result::Next(2)));
    // Mismatched redacted names.
    let mut c = w.client.clone();
    for values in [
        vec![],
        vec![("X-Other", "v")],
        vec![("Authorization", "a"), ("Idempotency-Key", "b")],
    ] {
        let r = c.start_effect(start_effect(&w, &act, 1, &values)).await;
        let m = rejected(&r);
        assert!(m.contains("redacted header names"), "{m}");
    }
    // `{{` that is not a well-formed placeholder (W6).
    for v in [
        "{{secret:PAY_TOKEN}}",
        "Bearer {{oops",
        "{{secret:__ostro/token-key}}",
        "{{x}}",
    ] {
        let r = c
            .start_effect(start_effect(&w, &act, 1, &[("Authorization", v)]))
            .await;
        let m = rejected(&r);
        assert!(m.contains("template in redacted value"), "{v}: {m}");
    }
    // Not an effect / no such op.
    for op in [0, 7] {
        let e = c
            .start_effect(start_effect(&w, &act, op, &[("Authorization", "x")]))
            .await
            .expect_err("not an effect");
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "op {op}: {e:?}");
    }
    // Revoke (operator pause): the runner sends Cancel and the old token is a zombie.
    let pause = Command::pause_run(PauseRun {
        run_id: run.as_bytes().to_vec(),
        reason: PAUSE_REASON_OPERATOR,
        detail: "07-12 zombie test".into(),
        token: None,
        requested_at_ms: now_ms(),
    });
    let o = s.handle_of(&run).submit_one(pause).await.expect("pause");
    assert_eq!(o, ApplyOutcome::Accepted);
    next_msg(&mut w, "Cancel", |m| {
        matches!(m, v1::server_msg::Msg::Cancel(_))
    })
    .await;
    let e = c
        .start_effect(start_effect(
            &w,
            &act,
            1,
            &[("Authorization", "Bearer literal")],
        ))
        .await
        .expect_err("zombie");
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
    // Appends of the zombie are fenced too.
    let (kind, bytes) = encode_event(&effect_event(2, &url));
    let frame = v1::AppendFrame {
        session_id: w.session_id(),
        run: act.run.clone(),
        token: act.token,
        from_seq: 2,
        event_index: 0,
        chunk: bytes,
        event_end: true,
        last: true,
        kind,
    };
    let r = w
        .client
        .append(tonic::codegen::tokio_stream::iter(vec![frame]))
        .await
        .expect("append")
        .into_inner();
    assert_eq!(r.result, Some(v1::append_result::Result::Fenced(true)));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        mock.hits.load(Ordering::SeqCst),
        0,
        "the provider saw no request"
    );
    drop(w);
    drop(ws);
    let mut s = s;
    s.shutdown().await;
}

// ------------------------------------------------------------------- secrets (D-W3)

async fn put_ok(s: &TestServer, path: &str, body: serde_json::Value) {
    let r = s
        .http
        .put(s.url(path))
        .json(&body)
        .send()
        .await
        .expect("PUT");
    assert_eq!(r.status(), reqwest::StatusCode::OK, "PUT {path}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secret_never_on_stream() {
    let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let canary = Canary::new();
    let mock = quiet_mock();
    let base = format!("http://{}", mock.addr);
    let module = s.deploy_named("fx-canary", "fx-canary", None).await;
    put_ok(
        &s,
        "/v1/secrets/canary-key",
        json!({ "value": canary.expose_str() }),
    )
    .await;
    put_ok(
        &s,
        "/v1/grants",
        json!({ "module": module, "secret": "canary-key", "origins": [base], "body": false }),
    )
    .await;
    put_ok(
        &s,
        &format!("/v1/egress/{module}"),
        json!({ "allow": [base] }),
    )
    .await;
    workers_queue(&s, "wq", &[]).await;
    s.hooks.remote.tap.store(true, Ordering::Release);
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let _w = ws.spawn("w1", 2, &[]).await;
    let run = s
        .start_rest(
            "fx-canary",
            json!({
                "input": {
                    "echo_url": format!("{base}/echo"),
                    "secret": "canary-key",
                    "header": "X-Api-Key",
                    "redact": true
                },
                "queue": "wq"
            }),
        )
        .await;
    let rec = terminal(&s, &run).await;
    assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    // The provider got the secret (expanded on the leader).
    let keys = mock.keys.lock().expect("keys").clone();
    assert_eq!(
        keys,
        vec![canary.expose().to_vec()],
        "the provider received the secret"
    );
    // The run went through the remote path, effect included.
    let recs = s.hooks.remote.records();
    assert!(
        recs.iter()
            .any(|r| r.what == "start_effect" && r.dir == WireDir::In)
    );
    assert!(
        recs.iter()
            .any(|r| r.what == "append" && r.dir == WireDir::In)
    );
    assert!(recs.iter().any(|r| r.dir == WireDir::Out));
    // No message in either direction carries the secret (raw, base64, hex, percent, JSON).
    let mut hits = Vec::new();
    for (i, r) in recs.iter().enumerate() {
        hits.extend(canary.scan_bytes(&format!("{i} {:?} {}", r.dir, r.what), &r.bytes));
    }
    // Journal pages and blobs served to the worker are journal rows: scan them too.
    for ev in s.journal(&run).await {
        let (_, bytes) = encode_event(&ev);
        hits.extend(canary.scan_bytes(&format!("journal row {}", ev.seq), &bytes));
    }
    assert!(hits.is_empty(), "canary on the worker channel: {hits:?}");
    let mut s = s;
    s.shutdown().await;
}

// ------------------------------------------------------------------- selectors

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selector_routes_gpu_queue_to_the_labelled_worker() {
    let s = server().await;
    let sha = deploy(&s, "sdk-basic").await;
    workers_queue(&s, "gpuq", &["gpu=true"]).await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let mut plain = ws.spawn("plain", 4, &[("zone", "a")]).await;
    let mut gpu = ws.spawn("gpu1", 1, &[("gpu", "true")]).await;
    let input = br#"{"name":"g","n":3}"#;
    let runs = [
        start(&s, "sdk-basic", input, &sha, "gpuq").await,
        start(&s, "sdk-basic", input, &sha, "gpuq").await,
    ];
    for r in &runs {
        let rec = terminal(&s, r).await;
        assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    }
    let rid = runs[0].to_string();
    gpu.wait_event(T, |e| {
        matches!(e, ostro_worker::WorkerEvent::ActivationStarted { run_id, .. } if *run_id == rid)
    })
    .await;
    while let Ok(e) = plain.events.try_recv() {
        assert!(
            !matches!(e, ostro_worker::WorkerEvent::ActivationStarted { .. }),
            "the unlabelled worker ran gpu work: {e:?}"
        );
    }
    let mut s = s;
    s.shutdown().await;
}

// ------------------------------------------------------------------- await tokens (D-W27)

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn await_tokens_are_minted_on_the_leader() {
    await_tokens_case(server().await).await;
}

/// p14-fixups-2: the leader re-mints with the key opened from a sealed `__ostro/token-key`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn await_tokens_are_minted_on_the_leader_with_a_sealed_token_key() {
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        let p = c.node.data_dir.join("test-sealing.key");
        std::fs::write(&p, [0x3cu8; 32]).expect("sealing key");
        c.secrets.sealing_key_file = Some(p);
    })
    .await;
    s.wait_ready(Duration::from_secs(20)).await;
    let deadline = std::time::Instant::now() + LIMIT; // clock-exempt: test deadline
    while !s.h().meta().view().read(|m| {
        matches!(
            m.get(&ostro_meta::MetaKey::Secret(ostro_meta::TOKEN_KEY_SECRET.into()))
                .map(|v| v.value),
            Some(MetaRecord::Secret(ref r)) if r.key_id.starts_with("aes256gcm:")
        )
    }) {
        assert!(std::time::Instant::now() < deadline, "token key not sealed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(s.h().meta().view().read(|m| m.token_key()), None);
    await_tokens_case(s).await;
}

async fn await_tokens_case(s: TestServer) {
    s.deploy_guest("tokens", None).await;
    workers_queue(&s, "wq", &[]).await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let _w = ws.spawn("w1", 1, &[]).await;
    let run = s
        .start_rest("tokens", json!({ "input": {}, "queue": "wq" }))
        .await;
    let token = s.published_token(&run.to_string(), LIMIT).await;
    // The worker's process-local mint was replaced by the leader's before it was journaled.
    assert!(s.hooks.remote.remints.load(Ordering::Relaxed) >= 1);
    let journaled: Vec<String> = s
        .journal(&run)
        .await
        .into_iter()
        .filter_map(|e| match e.body {
            EventBody::AwaitTokenCreated(t) => Some(t.token),
            _ => None,
        })
        .collect();
    assert_eq!(
        journaled,
        vec![token.clone()],
        "one leader token, published as journaled"
    );
    // ...so it verifies on the leader.
    let (st, v) = s
        .post_json(
            "/v1/tokens/complete",
            &json!({ "token": token, "payload": { "ok": 1 } }),
        )
        .await;
    assert_eq!(st, reqwest::StatusCode::OK, "{v}");
    let rec = terminal(&s, &run).await;
    assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    let out: serde_json::Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json");
    assert_eq!(out, json!({ "ok": 1 }));
    let mut s = s;
    s.shutdown().await;
}
