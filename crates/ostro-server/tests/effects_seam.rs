//! Server effect seam end to end, in process (plan 06-15; seam from 06-14).
//!
//! Every test runs a bridge-enabled server ([`start_server_with_bridge`], its own data dir
//! and tokio runtime) against its own `MockProvider` on loopback:
//!
//! - **intent before I/O**: `EffectScheduled` is durable before the provider sees the
//!   request (failpoint variant: the request is held at `bridge::http::before_send`);
//! - **journal delivery**: the guest's output equals the journaled `EffectCompleted`
//!   response, with the secret scrubbed in both;
//! - **B2**: a 1 MiB response completes through a blob `PayloadRef` and the guest receives
//!   every byte; replay yields the stored output;
//! - **children**: a child start still completes `unsupported`;
//! - **replay**: a journal that already holds `EffectCompleted` replays without I/O;
//! - **B1**: after a crash, an uncompleted effect is re-driven with the same
//!   `Idempotency-Key`, never repaired as `unsupported` (failpoint variant);
//! - **crash wake**: a run evicted while its effect was in flight is woken after a crash
//!   and completes with exactly one charge.
//!
//! Crashes use [`TestServer::crash`] (W10). No test holds a partition handle across it.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use common::{
    BridgeTestOpts, T, TestServer, guest_wasm, put_connection, put_egress, put_grant,
    put_sealed_secret, rref, start_server_with_bridge, tmpfs_dir,
};
use ostro_bridge::http::derive_key;
use ostro_bridge::origin::OriginPattern;
use ostro_bridge::secrets::GrantSpec;
use ostro_engine::config::EngineConfig;
use ostro_engine::error::EvictReason;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::ActivationOutcome;
use ostro_engine::testkit::replay_journal;
use ostro_partition::query;
use ostro_partition::store::values::RunStatus;
use ostro_server::sink::{CHILDREN_UNSUPPORTED, UNSUPPORTED_KIND};
use ostro_testkit::mock_provider::MockProvider;
use ostro_types::RunId;
use ostro_types::journal::{Event, EventBody, EventKind, Failure, OpOutput, decode_result};
use ostro_types::payload::payload_ref::Kind;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};

/// Canary secret value (never expected in a journal or an output).
const CANARY: &str = "canary-6f1e2d3c4b5a69788796a5b4";

/// Generous limit for a run reaching a terminal state.
const RUN_LIMIT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

/// A bridge server (loopback allowed, test sealing key) on a fresh tmpfs dir.
async fn server() -> TestServer {
    start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await
}

/// Deploy `guest` under its own name with egress to the mock; returns the module name.
async fn deploy_with_egress(s: &TestServer, guest: &str, mock: &MockProvider) -> String {
    let module = s.deploy_named(guest, guest, None).await;
    put_egress(s, &module, &[&mock.base_url()]).await;
    module
}

/// One fx-http request (`POST` bodies are empty JSON objects).
fn req(method: &str, url: String) -> Value {
    let mut r = json!({ "method": method, "url": url });
    if method == "POST" {
        r["headers"] = json!([["content-type", "application/json"]]);
        r["body_b64"] = json!(B64.encode(b"{}"));
    }
    r
}

/// `req` with a retry policy (in-progress 409s and 5xx are retried).
fn req_retry(method: &str, url: String) -> Value {
    let mut r = req(method, url);
    r["retry"] = json!({
        "max_attempts": 40,
        "initial_interval_ms": 250,
        "max_interval_ms": 1000,
        "backoff_coefficient": 2.0,
    });
    r
}

/// Start an fx-http run over REST.
async fn start_fx_http(s: &TestServer, requests: Vec<Value>) -> RunId {
    s.start_rest(
        "fx-http",
        json!({ "input": { "requests": requests, "parallel": false } }),
    )
    .await
}

/// Wait for a terminal status and require `Completed`.
async fn wait_completed(s: &TestServer, run: &RunId) {
    let r = s
        .wait_record(run, RUN_LIMIT, |r| {
            !matches!(
                r.status,
                RunStatus::Scheduled | RunStatus::Ready | RunStatus::Running
            )
        })
        .await;
    if r.status != RunStatus::Completed {
        let j = s.journal(run).await;
        panic!(
            "run {run} ended {:?}; journal:\n{}",
            r.status,
            dump_journal(&j)
        );
    }
}

/// The run's result JSON.
async fn result_json(s: &TestServer, run: &RunId) -> Value {
    let b = s.result_bytes(run).await.expect("stored result");
    serde_json::from_slice(&b).expect("result json")
}

/// Ops (seqs) of the run's `EffectScheduled` events.
fn effect_ops(j: &[Event]) -> Vec<u64> {
    j.iter()
        .filter(|e| matches!(e.body, EventBody::EffectScheduled(_)))
        .map(|e| e.seq)
        .collect()
}

/// The decoded outcome of the `EffectCompleted` for `op`, if journaled.
fn effect_outcome(j: &[Event], op: u64) -> Option<Result<OpOutput, Failure>> {
    j.iter().find_map(|e| match &e.body {
        EventBody::EffectCompleted(c) if c.op == op => {
            Some(decode_result(&c.outcome).expect("decode effect outcome"))
        }
        _ => None,
    })
}

/// Every `EffectCompleted` that carries the `unsupported` failure.
fn unsupported_effects(j: &[Event]) -> Vec<u64> {
    j.iter()
        .filter_map(|e| match &e.body {
            EventBody::EffectCompleted(c) => {
                let r: Result<OpOutput, Failure> =
                    decode_result(&c.outcome).expect("decode effect outcome");
                match r {
                    Err(Failure::NonRetryable(i) | Failure::Retryable(i))
                        if i.kind == UNSUPPORTED_KIND =>
                    {
                        Some(c.op)
                    }
                    _ => None,
                }
            }
            _ => None,
        })
        .collect()
}

/// One line per journal event (kind, seq, ts, and the effect outcome when decodable).
fn dump_journal(j: &[Event]) -> String {
    let mut out = String::new();
    for e in j {
        let extra = match &e.body {
            EventBody::EffectCompleted(c) => {
                match decode_result::<Result<OpOutput, Failure>>(&c.outcome) {
                    Ok(Ok(OpOutput::Http(h))) => {
                        format!(" op {} -> http {} ({} bytes)", c.op, h.status, h.body.len())
                    }
                    Ok(other) => format!(" op {} -> {other:?}", c.op),
                    Err(err) => format!(" op {} -> undecodable: {err}", c.op),
                }
            }
            other => format!(" {}", other.summary()),
        };
        out.push_str(&format!(
            "  #{} {:?} ts={}{}\n",
            e.seq,
            e.body.kind(),
            e.ts_nanos,
            extra
        ));
    }
    out
}

/// The idempotency key the connector derives for `run`'s op `op`.
fn key_of(run: &RunId, op: u64) -> String {
    derive_key(&run.to_string(), op)
}

/// Poll the journal until it holds an `EffectScheduled`; returns its op.
async fn wait_effect_scheduled(s: &TestServer, run: &RunId) -> u64 {
    let deadline = Instant::now() + T;
    loop {
        if let Some(op) = effect_ops(&s.journal(run).await).first() {
            return *op;
        }
        assert!(Instant::now() < deadline, "no EffectScheduled for {run}");
        sleep(Duration::from_millis(5)).await;
    }
}

/// Poll `f` until it holds or `limit` passes.
async fn wait_for(what: &str, limit: Duration, f: impl Fn() -> bool) {
    let deadline = Instant::now() + limit;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(5)).await;
    }
}

fn wall_nanos() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("nanos")
}

/// Replay `journal` in a fresh engine with a `FakeSink` (no effect driver, no provider).
async fn replay_offline(
    guest: &str,
    run: &RunId,
    journal: Vec<Event>,
) -> (ActivationOutcome, usize) {
    let rt = Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            token_key: [0x15; 32],
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime");
    let p = rt
        .prepare_cached(&guest_wasm(guest))
        .await
        .expect("prepare");
    replay_journal(&rt, &p, rref(run), journal).await
}

/// Finished activations of `run` that evicted because the hot wait elapsed (executor job
/// records; each re-activation is a new job).
fn hot_wait_evictions(s: &TestServer, run: &RunId) -> usize {
    s.jobs_of(run)
        .iter()
        .filter(|j| {
            matches!(
                j.activation,
                Some(ActivationOutcome::Evicted {
                    reason: EvictReason::HotWaitElapsed
                })
            )
        })
        .count()
}

/// Wait until an activation of `run` evicted with the hot wait elapsed.
async fn wait_for_evict(s: &TestServer, run: &RunId) {
    wait_for("a HotWaitElapsed eviction", T, || {
        hot_wait_evictions(s, run) >= 1
    })
    .await;
}

/// A loopback TCP forwarder to `upstream` that keeps each upstream connection open until
/// the upstream response ends, even after the client side closed. It models a provider
/// that finishes a request whose client vanished (the mock's server drops a handler whose
/// connection closes, which would leave its `/pay` key in progress forever after a crash).
/// Returns its base URL.
async fn detached_proxy(upstream: std::net::SocketAddr) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy");
    let addr = listener.local_addr().expect("proxy addr");
    tokio::spawn(async move {
        while let Ok((down, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(up) = tokio::net::TcpStream::connect(upstream).await else {
                    return;
                };
                let (mut down_r, mut down_w) = down.into_split();
                let (mut up_r, mut up_w) = up.into_split();
                // Client -> provider; the upstream write half is kept open (never
                // shut down) so the provider never sees a client close.
                let to_up = tokio::spawn(async move {
                    let mut buf = vec![0u8; 16 * 1024];
                    loop {
                        match down_r.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if up_w.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    up_w
                });
                // Provider -> client; once the client is gone, keep draining (bounded).
                let mut buf = vec![0u8; 16 * 1024];
                let mut client_open = true;
                loop {
                    let read = tokio::time::timeout(Duration::from_secs(30), up_r.read(&mut buf));
                    match read.await {
                        Ok(Ok(n)) if n > 0 => {
                            if client_open && down_w.write_all(&buf[..n]).await.is_err() {
                                client_open = false;
                            }
                        }
                        _ => break,
                    }
                }
                drop(down_w);
                if let Ok(up_w) = to_up.await {
                    drop(up_w);
                }
            });
        }
    });
    format!("http://{addr}")
}

/// The guest's first fx-http result item.
fn first_result(out: &Value) -> &Value {
    &out["results"][0]
}

fn body_of(item: &Value) -> Vec<u8> {
    B64.decode(item["body_b64"].as_str().expect("body_b64"))
        .expect("base64 body")
}

// ---------------------------------------------------------------------------------
// Task 1: intent before I/O, journal delivery, 1 MiB blob outcome, regressions
// ---------------------------------------------------------------------------------

/// The `EffectScheduled` event is journaled before the provider receives the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intent_before_io() {
    let mock = MockProvider::start().await;
    let s = server().await;
    deploy_with_egress(&s, "fx-http", &mock).await;

    let run = start_fx_http(&s, vec![req("POST", format!("{}/pay", mock.base_url()))]).await;
    // Upper bound of the provider's receive time: the first poll that sees the request.
    wait_for("the /pay request", T, || mock.requests_to("/pay") > 0).await;
    let seen_at = wall_nanos();

    wait_completed(&s, &run).await;
    let j = s.journal(&run).await;
    let ops = effect_ops(&j);
    assert_eq!(ops.len(), 1, "{}", dump_journal(&j));
    let scheduled = j.iter().find(|e| e.seq == ops[0]).expect("event");
    assert!(
        scheduled.ts_nanos <= seen_at,
        "EffectScheduled ts {} after the provider saw the request ({seen_at})",
        scheduled.ts_nanos
    );
    let key = key_of(&run, ops[0]);
    assert_eq!(mock.requests(&key), 1, "one request with the derived key");
    assert_eq!(mock.charges(&key), 1);
    assert!(
        matches!(effect_outcome(&j, ops[0]), Some(Ok(OpOutput::Http(ref h))) if h.status == 200),
        "{}",
        dump_journal(&j)
    );
}

/// With the send held at `bridge::http::before_send`, the intent is already journaled and
/// the provider has seen nothing; after release the charge happens exactly once.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intent_before_io_failpoint() {
    let _scenario = fail::FailScenario::setup();
    let mock = MockProvider::start().await;
    let s = server().await;
    deploy_with_egress(&s, "fx-http", &mock).await;

    fail::cfg("bridge::http::before_send", "return(wait:1500)").expect("failpoint");
    let run = start_fx_http(&s, vec![req("POST", format!("{}/pay", mock.base_url()))]).await;
    let op = wait_effect_scheduled(&s, &run).await;
    assert_eq!(
        mock.requests_to("/pay"),
        0,
        "the provider was contacted while the send was held, after EffectScheduled"
    );
    // Still held a little later: the intent is durable well before I/O.
    sleep(Duration::from_millis(300)).await;
    assert_eq!(mock.requests_to("/pay"), 0);
    assert!(
        effect_outcome(&s.journal(&run).await, op).is_none(),
        "completed while held"
    );

    wait_completed(&s, &run).await;
    fail::cfg("bridge::http::before_send", "off").expect("failpoint off");
    let key = key_of(&run, op);
    assert_eq!(mock.requests_to("/pay"), 1);
    assert_eq!(mock.charges(&key), 1);
    let out = result_json(&s, &run).await;
    assert_eq!(first_result(&out)["status"], 200, "{out}");
}

/// The guest's output equals the journaled response, and a templated secret echoed back
/// by the provider is `[REDACTED]` in both.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_delivered() {
    let mock = MockProvider::start().await;
    let s = server().await;
    let module = deploy_with_egress(&s, "fx-canary", &mock).await;
    put_sealed_secret(&s, "_/tok", CANARY.as_bytes()).await;
    put_grant(
        &s,
        GrantSpec {
            module: module.clone(),
            secret: "tok".into(),
            origins: vec![OriginPattern::parse(&mock.base_url()).expect("origin")],
            body: false,
        },
    )
    .await;

    let run = s
        .start_rest(
            "fx-canary",
            json!({ "input": {
                "echo_url": format!("{}/echo", mock.base_url()),
                "secret": "tok",
                "header": "X-Api-Key",
            }}),
        )
        .await;
    wait_completed(&s, &run).await;
    assert_eq!(mock.requests_to("/echo"), 1);

    let out = result_json(&s, &run).await;
    let observed = &out["observed"];
    assert_eq!(observed["status"], 200, "{out}");
    let guest_body = body_of(observed);

    let j = s.journal(&run).await;
    let ops = effect_ops(&j);
    assert_eq!(ops.len(), 1, "{}", dump_journal(&j));
    let Some(Ok(OpOutput::Http(h))) = effect_outcome(&j, ops[0]) else {
        panic!("no http EffectCompleted:\n{}", dump_journal(&j));
    };
    assert_eq!(h.status, 200);
    assert_eq!(
        guest_body, h.body,
        "the guest received exactly the journaled response body"
    );

    let echoed: Value = serde_json::from_slice(&h.body).expect("echo json");
    let api_key = echoed["headers"]
        .as_array()
        .expect("headers")
        .iter()
        .find(|kv| {
            kv[0]
                .as_str()
                .is_some_and(|k| k.eq_ignore_ascii_case("x-api-key"))
        })
        .unwrap_or_else(|| panic!("x-api-key not echoed: {echoed}"));
    assert_eq!(api_key[1], "[REDACTED]", "{echoed}");

    // The canary appears nowhere: not in the journal, not in the stored output.
    let raw = s.result_bytes(&run).await.expect("result");
    assert!(!String::from_utf8_lossy(&raw).contains(CANARY));
    for e in &j {
        let dbg = format!("{e:?}");
        assert!(!dbg.contains(CANARY), "canary in journal event #{}", e.seq);
    }
    if let EventBody::EffectCompleted(c) = &j
        .iter()
        .find(|e| matches!(&e.body, EventBody::EffectCompleted(c) if c.op == ops[0]))
        .expect("completion")
        .body
    {
        let bytes = &c.outcome.bytes;
        assert!(
            !bytes.windows(CANARY.len()).any(|w| w == CANARY.as_bytes()),
            "canary bytes in the EffectCompleted outcome"
        );
    }
}

/// B2: a 1 MiB response completes through a blob outcome and reaches the guest intact;
/// replaying the journal yields the stored output without contacting the provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_outcome_blob() {
    const N: usize = 1_048_576;
    let mock = MockProvider::start().await;
    let s = server().await;
    deploy_with_egress(&s, "fx-http", &mock).await;
    put_connection(
        &s,
        &mock.base_url(),
        "http-target",
        json!({ "v": 1, "max_response_bytes": 2 * N }),
    )
    .await;

    let run = start_fx_http(
        &s,
        vec![req("GET", format!("{}/big?bytes={N}", mock.base_url()))],
    )
    .await;
    wait_completed(&s, &run).await;
    assert_eq!(mock.requests_to("/big"), 1);

    let out = result_json(&s, &run).await;
    let item = first_result(&out);
    assert_eq!(item["status"], 200, "error item: {}", item["error"]);
    let body = body_of(item);
    assert_eq!(body.len(), N);
    let want: [u8; 32] = Sha256::digest(vec![b'x'; N]).into();
    let got: [u8; 32] = Sha256::digest(&body).into();
    assert_eq!(got, want, "sha256 of the guest's body");

    // The EffectCompleted journal row is a blob reference.
    let j = s.journal(&run).await;
    let ops = effect_ops(&j);
    assert_eq!(ops.len(), 1);
    let completed_seq = j
        .iter()
        .find(|e| matches!(&e.body, EventBody::EffectCompleted(c) if c.op == ops[0]))
        .expect("EffectCompleted")
        .seq;
    let rows = {
        let h = s.handle_of(&run);
        query::journal_rows(&h.reader(), &run, 0, 4096).expect("journal rows")
    };
    let (_, kind, payload) = rows
        .iter()
        .find(|(seq, _, _)| *seq == completed_seq)
        .expect("row");
    assert_eq!(*kind, EventKind::EffectCompleted as u32);
    match &payload.kind {
        Some(Kind::Blob(b)) => assert!(b.len > N as u64, "blob len {}", b.len),
        other => panic!("EffectCompleted row is not a blob: {other:?}"),
    }
    let Some(Ok(OpOutput::Http(h))) = effect_outcome(&j, ops[0]) else {
        panic!("no http outcome:\n{}", dump_journal(&j));
    };
    assert_eq!(h.body.len(), N);

    // Replay (a fresh activation over the journal) yields the identical output.
    let stored = s.result_bytes(&run).await.expect("result");
    let (outcome, appended) = replay_offline("fx-http", &run, j).await;
    match outcome {
        ActivationOutcome::Completed { output } => assert!(
            output.data == stored,
            "replayed output differs from the stored result"
        ),
        other => panic!("replay: {other:?}"),
    }
    assert_eq!(appended, 0, "replay appended events");
    assert_eq!(mock.requests_to("/big"), 1, "replay contacted the provider");
}

/// A child start still completes with `unsupported`; effects of the same run go through
/// the bridge and are never completed `unsupported`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn children_still_unsupported() {
    let s = server().await;
    s.deploy_guest("wit-surface", None).await;
    let run = s.start_rest("surface", json!({})).await;
    wait_completed(&s, &run).await;

    let j = s.journal(&run).await;
    let child_op = j
        .iter()
        .find(|e| matches!(e.body, EventBody::ChildStarted(_)))
        .unwrap_or_else(|| panic!("no ChildStarted:\n{}", dump_journal(&j)))
        .seq;
    let outcome = j
        .iter()
        .find_map(|e| match &e.body {
            EventBody::ChildCompleted(c) if c.op == child_op => Some(&c.outcome),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no ChildCompleted:\n{}", dump_journal(&j)));
    let r: Result<OpOutput, Failure> = decode_result(outcome).expect("decode");
    match r {
        Err(Failure::NonRetryable(i)) => {
            assert_eq!(i.kind, UNSUPPORTED_KIND);
            assert_eq!(i.message, CHILDREN_UNSUPPORTED);
        }
        other => panic!("child outcome {other:?}"),
    }
    assert_eq!(effect_ops(&j).len(), 3, "{}", dump_journal(&j));
    assert!(
        unsupported_effects(&j).is_empty(),
        "an effect was completed unsupported:\n{}",
        dump_journal(&j)
    );
}

/// A run whose activation evicted (hot wait elapsed) after its first effect completed is
/// re-activated while the second is in flight; every replay serves the first effect from
/// the journal (no second request), and an offline replay of the finished journal
/// contacts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_journal_no_contact() {
    let mock = MockProvider::start().await;
    let s = server().await;
    deploy_with_egress(&s, "fx-http", &mock).await;

    // The 3 s /pay outlives the 2 s hot wait: the activation evicts (HotWaitElapsed) with
    // /echo completed and is re-activated at once (the run stays Running: an in-flight
    // effect is hot-waitable, so it never suspends to Scheduled).
    let run = start_fx_http(
        &s,
        vec![
            req("GET", format!("{}/echo", mock.base_url())),
            req("POST", format!("{}/pay?delay_ms=3000", mock.base_url())),
        ],
    )
    .await;
    wait_for("the /pay request", T, || mock.requests_to("/pay") == 1).await;
    wait_completed(&s, &run).await;
    assert!(
        hot_wait_evictions(&s, &run) >= 1,
        "no activation evicted while /pay was in flight"
    );

    assert_eq!(mock.requests_to("/echo"), 1, "/echo re-sent on replay");
    assert_eq!(mock.requests_to("/pay"), 1);
    assert_eq!(mock.total_requests(), 2);
    let out = result_json(&s, &run).await;
    assert_eq!(out["results"][0]["status"], 200, "{out}");
    assert_eq!(out["results"][1]["status"], 200, "{out}");

    let j = s.journal(&run).await;
    assert_eq!(
        j.iter()
            .filter(|e| matches!(e.body, EventBody::EffectCompleted(_)))
            .count(),
        2,
        "{}",
        dump_journal(&j)
    );
    let stored = s.result_bytes(&run).await.expect("result");
    let before = mock.total_requests();
    let (outcome, appended) = replay_offline("fx-http", &run, j).await;
    match outcome {
        ActivationOutcome::Completed { output } => assert_eq!(output.data, stored),
        other => panic!("replay: {other:?}"),
    }
    assert_eq!(appended, 0);
    assert_eq!(
        mock.total_requests(),
        before,
        "replay contacted the provider"
    );
}

// ---------------------------------------------------------------------------------
// Task 2: B1 crash re-drive and crash wake
// ---------------------------------------------------------------------------------

/// B1: the provider charged, the completion never reached the journal, the node crashed.
/// After restart nothing is repaired as `unsupported`: the effect is re-driven with the
/// same key (the provider replays the stored response) and the run completes with one
/// charge.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncompleted_effect_redriven_not_repaired() {
    let _scenario = fail::FailScenario::setup();
    let mock = MockProvider::start().await;
    let mut s = server().await;
    deploy_with_egress(&s, "fx-http", &mock).await;

    fail::cfg("bridge::effect::before_complete_submit", "return(hang)").expect("failpoint");
    let run = start_fx_http(&s, vec![req("POST", format!("{}/pay", mock.base_url()))]).await;
    let op = wait_effect_scheduled(&s, &run).await;
    let key = key_of(&run, op);
    wait_for("the charge", T, || mock.charges(&key) == 1).await;
    // The completion is held: nothing journaled for the op.
    sleep(Duration::from_millis(200)).await;
    assert!(effect_outcome(&s.journal(&run).await, op).is_none());

    s.crash().await;
    fail::cfg("bridge::effect::before_complete_submit", "off").expect("failpoint off");
    s.restart().await;
    s.wait_ready(T).await;

    // repair_unsupported ran at journal load and journaled nothing for the op.
    let j = s.journal(&run).await;
    assert!(
        unsupported_effects(&j).is_empty(),
        "B1: an in-flight effect was repaired as unsupported:\n{}",
        dump_journal(&j)
    );

    wait_completed(&s, &run).await;
    let j = s.journal(&run).await;
    println!("B1 journal after restart:\n{}", dump_journal(&j));
    assert!(
        unsupported_effects(&j).is_empty(),
        "B1: unsupported completion:\n{}",
        dump_journal(&j)
    );
    match effect_outcome(&j, op) {
        Some(Ok(OpOutput::Http(h))) => {
            assert_eq!(h.status, 200);
            assert!(
                h.headers
                    .iter()
                    .any(|hd| hd.name.eq_ignore_ascii_case("idempotent-replayed")),
                "the re-drive got the provider's replayed response"
            );
        }
        other => panic!("op {op} outcome {other:?}:\n{}", dump_journal(&j)),
    }
    assert_eq!(
        j.iter()
            .filter(|e| matches!(&e.body, EventBody::EffectCompleted(c) if c.op == op))
            .count(),
        1
    );
    assert_eq!(mock.requests(&key), 2, "re-driven once with the same key");
    assert_eq!(mock.requests_to("/pay"), 2, "no request under another key");
    assert_eq!(mock.charges(&key), 1, "exactly one charge");
    let out = result_json(&s, &run).await;
    assert_eq!(first_result(&out)["status"], 200, "{out}");
}

/// A run evicted while its `/pay` was in flight (5 s, longer than the 2 s hot wait) is
/// woken after a crash (LeaderAnnounced), re-drives the same key and completes with one
/// charge. The re-drive may meet the provider's in-progress 409 (the first request is
/// still sleeping), which the guest's retry policy retries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evicted_inflight_wakes() {
    let mock = MockProvider::start().await;
    let proxy = detached_proxy(mock.addr()).await;
    let mut s = server().await;
    let module = s.deploy_named("fx-http", "fx-http", None).await;
    put_egress(&s, &module, &[&proxy]).await;

    let run = start_fx_http(
        &s,
        vec![req_retry("POST", format!("{proxy}/pay?delay_ms=5000"))],
    )
    .await;
    let op = wait_effect_scheduled(&s, &run).await;
    let key = key_of(&run, op);
    wait_for("the /pay request", T, || mock.requests(&key) == 1).await;
    let sent = Instant::now();
    wait_for_evict(&s, &run).await;
    assert!(
        sent.elapsed() < Duration::from_millis(4500),
        "eviction came too late to crash before the provider responds"
    );
    assert_eq!(mock.charges(&key), 1);
    assert!(effect_outcome(&s.journal(&run).await, op).is_none());

    s.crash().await;
    s.restart().await;
    s.wait_ready(T).await;

    wait_completed(&s, &run).await;
    let j = s.journal(&run).await;
    println!("evicted run journal after restart:\n{}", dump_journal(&j));
    assert!(unsupported_effects(&j).is_empty(), "{}", dump_journal(&j));
    match effect_outcome(&j, op) {
        Some(Ok(OpOutput::Http(h))) => assert_eq!(h.status, 200),
        other => panic!("op {op} outcome {other:?}:\n{}", dump_journal(&j)),
    }
    assert!(mock.requests(&key) >= 2, "re-driven with the same key");
    assert_eq!(
        mock.requests_to("/pay"),
        mock.requests(&key),
        "every /pay request carried the same key"
    );
    assert_eq!(mock.charges(&key), 1, "exactly one charge");
    let out = result_json(&s, &run).await;
    assert_eq!(first_result(&out)["status"], 200, "{out}");
}
