//! API-05 / API-06 / API-07 against the real in-process server (plan 05-13): signal,
//! cancel, terminate, retry (resume vs restart), await-token completion and the long-poll
//! result, each scenario run over **both** REST and gRPC, plus `dry_run` proofs (journal
//! length and run record unchanged) and a log-capture check that token text never reaches
//! a log line.
//!
//! Long-poll tests are named `long_poll_*` (`cargo test --test lifecycle_api long_poll`).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_api::ErrorCode;
use ostro_api::domain::AwaitResultRequest;
use ostro_api::grpc::status_details;
use ostro_partition::query;
use ostro_partition::store::values::RunStatus;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use ostro_proto::api::v1::tokens_client::TokensClient;
use ostro_types::journal::EventKind;
use ostro_types::payload::payload_ref::Kind as PayloadKind;
use ostro_types::{PartitionId, RunId};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tonic::transport::Channel;

const LIMIT: Duration = Duration::from_secs(30);
const MIB: usize = 1024 * 1024;

// ---------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

async fn server() -> TestServer {
    let s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

async fn deploy(s: &TestServer, guest: &str, tenant: Option<&str>) {
    let wasm = guest_wasm(guest);
    let sha = sha_hex(&wasm);
    let q = tenant.map(|t| format!("?tenant={t}")).unwrap_or_default();
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}{q}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
}

/// REST start; returns the run id.
async fn start(s: &TestServer, workflow: &str, body: Value) -> String {
    let r = s
        .http
        .post(s.url(&format!("/v1/workflows/{workflow}/runs")))
        .json(&body)
        .send()
        .await
        .expect("POST run");
    let st = r.status();
    let v: Value = r.json().await.expect("json");
    assert_eq!(st, StatusCode::CREATED, "start {workflow}: {v}");
    v["run_id"].as_str().expect("run_id").to_owned()
}

fn rid(id: &str) -> RunId {
    id.parse().expect("run id")
}

/// A well-formed run id that does not exist.
fn unknown_run() -> String {
    RunId::new(PartitionId(0), uuid::Uuid::now_v7()).to_string()
}

/// `(run record, journal length)`: what a dry run must leave untouched.
async fn snapshot(s: &TestServer, id: &str) -> (ostro_partition::store::values::RunRecord, usize) {
    let run = rid(id);
    (s.run_record(&run), s.journal(&run).await.len())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tr {
    Rest,
    Grpc,
}

const BOTH: [Tr; 2] = [Tr::Rest, Tr::Grpc];

/// Ok body, or `(error code wire name, details)`; identical shapes on both transports.
type Out = Result<Value, (String, Value)>;

fn code(o: &Out) -> &str {
    &o.as_ref().expect_err("expected an error").0
}

fn reason(o: &Out) -> Value {
    o.as_ref().expect_err("expected an error").1["reason"].clone()
}

struct Api {
    t: Tr,
    base: String,
    http: reqwest::Client,
    runs: RunsClient<Channel>,
    tokens: TokensClient<Channel>,
}

fn grpc_err(e: tonic::Status) -> (String, Value) {
    (
        ErrorCode::from_grpc_code(e.code()).as_str().to_owned(),
        status_details(&e).unwrap_or(Value::Null),
    )
}

fn pb_status(i: i32) -> String {
    pb::RunStatus::try_from(i)
        .expect("status")
        .as_str_name()
        .trim_start_matches("RUN_STATUS_")
        .to_lowercase()
}

fn embed(b: &Option<Vec<u8>>) -> (bool, Value) {
    match b {
        Some(b) => (true, serde_json::from_slice(b).expect("json")),
        None => (false, Value::Null),
    }
}

impl Api {
    async fn new(s: &TestServer, t: Tr) -> Self {
        let ch = s.channel().await;
        Self {
            t,
            base: s.url(""),
            http: reqwest::Client::new(),
            runs: RunsClient::new(ch.clone()).max_decoding_message_size(64 * MIB),
            tokens: TokensClient::new(ch),
        }
    }

    async fn rest(&self, req: reqwest::RequestBuilder) -> Out {
        let r = req.send().await.expect("request");
        let ok = r.status().is_success();
        let v: Value = r.json().await.expect("json");
        if ok {
            Ok(v)
        } else {
            Err((
                v["error"]["code"].as_str().expect("code").to_owned(),
                v["error"]["details"].clone(),
            ))
        }
    }

    async fn signal(
        &mut self,
        run: &str,
        name: &str,
        payload: Value,
        request_id: Option<&str>,
        dry_run: bool,
    ) -> Out {
        match self.t {
            Tr::Rest => {
                let body =
                    json!({"payload": payload, "request_id": request_id, "dry_run": dry_run});
                let url = format!("{}/v1/runs/{run}/signals/{name}", self.base);
                self.rest(self.http.post(url).json(&body)).await
            }
            Tr::Grpc => {
                let r = self
                    .runs
                    .signal(pb::SignalRunRequest {
                        run_id: run.into(),
                        name: name.into(),
                        payload_json: payload.to_string().into_bytes(),
                        request_id: request_id.map(str::to_owned),
                        dry_run,
                    })
                    .await
                    .map_err(grpc_err)?
                    .into_inner();
                Ok(json!({"accepted": r.accepted, "duplicate": r.duplicate, "dry_run": r.dry_run}))
            }
        }
    }

    async fn cancel(&mut self, run: &str, reason: Option<&str>, dry_run: bool) -> Out {
        match self.t {
            Tr::Rest => {
                let url = format!("{}/v1/runs/{run}/cancel", self.base);
                self.rest(
                    self.http
                        .post(url)
                        .json(&json!({"reason": reason, "dry_run": dry_run})),
                )
                .await
            }
            Tr::Grpc => {
                let r = self
                    .runs
                    .cancel(pb::CancelRunRequest {
                        run_id: run.into(),
                        reason: reason.map(str::to_owned),
                        dry_run,
                    })
                    .await
                    .map_err(grpc_err)?
                    .into_inner();
                Ok(json!({"already_requested": r.already_requested, "dry_run": r.dry_run}))
            }
        }
    }

    async fn terminate(&mut self, run: &str, reason: Option<&str>, dry_run: bool) -> Out {
        match self.t {
            Tr::Rest => {
                let url = format!("{}/v1/runs/{run}/terminate", self.base);
                self.rest(
                    self.http
                        .post(url)
                        .json(&json!({"reason": reason, "dry_run": dry_run})),
                )
                .await
            }
            Tr::Grpc => {
                let r = self
                    .runs
                    .terminate(pb::TerminateRunRequest {
                        run_id: run.into(),
                        reason: reason.map(str::to_owned),
                        dry_run,
                    })
                    .await
                    .map_err(grpc_err)?
                    .into_inner();
                Ok(json!({"status": pb_status(r.status), "dry_run": r.dry_run}))
            }
        }
    }

    async fn retry(&mut self, run: &str, dry_run: bool) -> Out {
        match self.t {
            Tr::Rest => {
                let url = format!("{}/v1/runs/{run}/retry", self.base);
                self.rest(self.http.post(url).json(&json!({"dry_run": dry_run})))
                    .await
            }
            Tr::Grpc => {
                let r = self
                    .runs
                    .retry(pb::RetryRunRequest {
                        run_id: run.into(),
                        dry_run,
                    })
                    .await
                    .map_err(grpc_err)?
                    .into_inner();
                let mode = pb::RetryMode::try_from(r.mode)
                    .expect("mode")
                    .as_str_name()
                    .trim_start_matches("RETRY_MODE_")
                    .to_lowercase();
                Ok(json!({"mode": mode, "run_id": r.run_id, "dry_run": r.dry_run}))
            }
        }
    }

    /// Normalised: `{run_id, status, completed, has_result, result, has_failure, failure}`.
    async fn result(&mut self, run: &str, wait_ms: u64, follow: bool) -> Out {
        match self.t {
            Tr::Rest => {
                let url = format!(
                    "{}/v1/runs/{run}/result?wait={wait_ms}ms&follow_continuations={follow}",
                    self.base
                );
                let v = self.rest(self.http.get(url)).await?;
                Ok(json!({
                    "run_id": v["run_id"], "status": v["status"], "completed": v["completed"],
                    "has_result": v.get("result").is_some(), "result": v["result"],
                    "has_failure": v.get("failure").is_some(), "failure": v["failure"],
                }))
            }
            Tr::Grpc => {
                let r = self
                    .runs
                    .await_result(pb::AwaitResultRequest {
                        run_id: run.into(),
                        wait_ms,
                        follow_continuations: follow,
                    })
                    .await
                    .map_err(grpc_err)?
                    .into_inner();
                let (hr, result) = embed(&r.result_json);
                let (hf, failure) = embed(&r.failure_json);
                Ok(json!({
                    "run_id": r.run_id, "status": pb_status(r.status), "completed": r.completed,
                    "has_result": hr, "result": result, "has_failure": hf, "failure": failure,
                }))
            }
        }
    }

    async fn token(&mut self, token: &str, payload: Value, dry_run: bool) -> Out {
        match self.t {
            Tr::Rest => {
                let url = format!("{}/v1/tokens/complete", self.base);
                self.rest(
                    self.http
                        .post(url)
                        .json(&json!({"token": token, "payload": payload, "dry_run": dry_run})),
                )
                .await
            }
            Tr::Grpc => {
                let r = self
                    .tokens
                    .complete(pb::CompleteTokenRequest {
                        token: token.into(),
                        payload_json: payload.to_string().into_bytes(),
                        dry_run,
                    })
                    .await
                    .map_err(grpc_err)?
                    .into_inner();
                Ok(json!({"run_id": r.run_id, "accepted": r.accepted, "dry_run": r.dry_run}))
            }
        }
    }
}

/// Wait until a `signals`-style run is suspended on its wait (one open wait).
async fn wait_parked(s: &TestServer, id: &str) {
    s.wait_record(&rid(id), LIMIT, |r| {
        r.open_waits == 1 && r.status == RunStatus::Scheduled
    })
    .await;
}

/// Wait until a sleeping run is suspended (journal started, not running).
async fn wait_sleeping(s: &TestServer, id: &str) {
    s.wait_record(&rid(id), LIMIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
}

async fn result_of(s: &TestServer, id: &str) -> Value {
    let b = s.result_bytes(&rid(id)).await.expect("result");
    serde_json::from_slice(&b).expect("json result")
}

// ---------------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_rest_and_grpc() {
    let mut s = server().await;
    deploy(&s, "signals", None).await;
    for t in BOTH {
        let mut api = Api::new(&s, t).await;

        // A sleeping run is woken by a signal.
        let id = start(&s, "signals", json!({"input": {"mode": "long_wait"}})).await;
        wait_parked(&s, &id).await;
        let before = snapshot(&s, &id).await;
        let r = api
            .signal(&id, "x", json!({"dry": t == Tr::Rest}), None, true)
            .await;
        assert_eq!(
            r,
            Ok(json!({"accepted": true, "duplicate": false, "dry_run": true})),
            "{t:?}"
        );
        assert_eq!(
            snapshot(&s, &id).await,
            before,
            "{t:?}: dry run changed the run"
        );
        let r = api
            .signal(&id, "x", json!({"via": format!("{t:?}")}), None, false)
            .await;
        assert_eq!(
            r,
            Ok(json!({"accepted": true, "duplicate": false, "dry_run": false}))
        );
        s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
        assert_eq!(result_of(&s, &id).await, json!({"via": format!("{t:?}")}));

        // Terminal: 409 run_terminal (with the status); unknown: 404.
        let r = api.signal(&id, "x", json!(1), None, false).await;
        assert_eq!(code(&r), "conflict", "{t:?}: {r:?}");
        assert_eq!(reason(&r), "run_terminal");
        assert_eq!(r.as_ref().unwrap_err().1["status"], "completed");
        let r = api.signal(&id, "x", json!(1), None, true).await;
        assert_eq!(reason(&r), "run_terminal", "dry run validates the same");
        let r = api.signal(&unknown_run(), "x", json!(1), None, false).await;
        assert_eq!(code(&r), "not_found", "{t:?}: {r:?}");
        let r = api
            .signal(&id, &"n".repeat(257), json!(1), None, false)
            .await;
        assert_eq!(code(&r), "invalid_argument");

        // request_id dedupe (D13): one SignalPayload + SignalMatched pair for two sends.
        let id = start(&s, "signals", json!({"input": {"mode": "fifo"}})).await;
        wait_parked(&s, &id).await;
        let r = api.signal(&id, "x", json!("a"), Some("r1"), false).await;
        assert_eq!(r.expect("first")["duplicate"], false);
        let r = api
            .signal(&id, "x", json!("a-again"), Some("r1"), false)
            .await;
        assert_eq!(r.expect("second")["duplicate"], true, "{t:?}");
        // The match re-activates the run; let it park on its second wait.
        wait_parked(&s, &id).await;
        let before = snapshot(&s, &id).await;
        let r = api.signal(&id, "x", json!("dry"), Some("r1"), true).await;
        assert_eq!(
            r,
            Ok(json!({"accepted": true, "duplicate": true, "dry_run": true}))
        );
        assert_eq!(snapshot(&s, &id).await, before);
        let kinds = s.history_kinds(&rid(&id)).await;
        let n = |k| kinds.iter().filter(|x| **x == k).count();
        assert_eq!(n(EventKind::SignalPayload), 1, "{kinds:?}");
        assert_eq!(n(EventKind::SignalMatched), 1, "{kinds:?}");
        api.signal(&id, "x", json!("b"), Some("r2"), false)
            .await
            .expect("b");
        api.signal(&id, "x", json!("c"), None, false)
            .await
            .expect("c");
        s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
        assert_eq!(result_of(&s, &id).await, json!(["a", "b", "c"]));
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_1mib_payload_is_blob_offloaded() {
    let mut s = server().await;
    deploy(&s, "signals", None).await;
    let mut api = Api::new(&s, Tr::Rest).await;
    let id = start(&s, "signals", json!({"input": {"mode": "long_wait"}})).await;
    wait_parked(&s, &id).await;
    let big = Value::String("s".repeat(MIB));
    api.signal(&id, "x", big.clone(), None, false)
        .await
        .expect("signal");
    s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
    assert_eq!(result_of(&s, &id).await, big);
    // D21: the journaled SignalPayload row is a blob ref (above the 256 KiB inline limit).
    let run = rid(&id);
    let journal = s.journal(&run).await;
    let seq = journal
        .iter()
        .find(|e| e.body.kind() == EventKind::SignalPayload)
        .expect("SignalPayload")
        .seq;
    let h = s.handle_of(&run);
    let rows = query::journal_rows(&h.reader(), &run, seq, 1).expect("row");
    assert!(
        matches!(rows[0].2.kind, Some(PayloadKind::Blob(_))),
        "the 1 MiB signal event must be blob-offloaded"
    );
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Cancel / terminate
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_rest_and_grpc() {
    let mut s = server().await;
    deploy(&s, "signals", None).await;
    deploy(&s, "sdk-cancel", None).await;
    for t in BOTH {
        let mut api = Api::new(&s, t).await;

        // A waiting run observes the cancel and ends Cancelled (D11).
        let id = start(&s, "signals", json!({"input": {"mode": "long_wait"}})).await;
        wait_parked(&s, &id).await;
        api.cancel(&id, Some("stop"), false).await.expect("cancel");
        s.wait_status(&rid(&id), RunStatus::Cancelled, LIMIT).await;
        let r = api.cancel(&id, None, false).await;
        assert_eq!(reason(&r), "run_terminal", "{t:?}: {r:?}");
        let r = api.cancel(&unknown_run(), None, false).await;
        assert_eq!(code(&r), "not_found");

        // Shielded cleanup runs after the cancel (sdk-cancel completes "cleaned").
        let id = start(
            &s,
            "sdk-cancel",
            json!({"input": {"mode": "shielded_sleep"}}),
        )
        .await;
        wait_sleeping(&s, &id).await;
        api.cancel(&id, None, false).await.expect("cancel");
        s.wait_record(&rid(&id), LIMIT, |r| r.status.is_terminal())
            .await;
        assert_eq!(result_of(&s, &id).await, json!("cleaned"), "{t:?}");
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_before_start_is_flag_only_and_idempotent() {
    // No capacity (`executor.slots = 0`): a cancel at seq 0 sets the flag and wakes the run,
    // which is then never granted, so the repeat is deterministic.
    let mut s = TestServer::start_with(ServerOpts::default(), |c| c.executor.slots = 0).await;
    s.wait_ready(Duration::from_secs(10)).await;
    deploy(&s, "signals", None).await;
    for t in BOTH {
        let mut api = Api::new(&s, t).await;
        let id = start(&s, "signals", json!({"input": {"mode": "long_wait"}})).await;
        let before = snapshot(&s, &id).await;
        assert_eq!(before.1, 0, "never started");
        let r = api.cancel(&id, Some("dry"), true).await;
        assert_eq!(
            r,
            Ok(json!({"already_requested": false, "dry_run": true})),
            "{t:?}"
        );
        assert_eq!(snapshot(&s, &id).await, before);
        let r = api.cancel(&id, Some("first"), false).await;
        assert_eq!(r, Ok(json!({"already_requested": false, "dry_run": false})));
        let r = api.cancel(&id, Some("again"), false).await;
        assert_eq!(
            r,
            Ok(json!({"already_requested": true, "dry_run": false})),
            "{t:?}"
        );
        let r = api.cancel(&id, None, true).await;
        assert_eq!(r, Ok(json!({"already_requested": true, "dry_run": true})));
        let rec = s.run_record(&rid(&id));
        assert!(rec.cancel_requested);
        assert_eq!(rec.cancel_reason, "first");
        assert_eq!(rec.next_seq, 0, "nothing is journaled before RunStarted");
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_rest_and_grpc() {
    let mut s = server().await;
    deploy(&s, "timeouts", None).await;
    for t in BOTH {
        let mut api = Api::new(&s, t).await;

        // A sleeping run: dry run changes nothing; terminate; again = 409.
        let id = start(&s, "timeouts", json!({"input": {"mode": "sleep_forever"}})).await;
        wait_sleeping(&s, &id).await;
        let before = snapshot(&s, &id).await;
        let r = api.terminate(&id, Some("dry"), true).await;
        assert_eq!(
            r,
            Ok(json!({"status": "terminated", "dry_run": true})),
            "{t:?}"
        );
        assert_eq!(snapshot(&s, &id).await, before);
        let r = api.terminate(&id, Some("ops"), false).await;
        assert_eq!(r, Ok(json!({"status": "terminated", "dry_run": false})));
        let rec = s.wait_status(&rid(&id), RunStatus::Terminated, LIMIT).await;
        assert_eq!(rec.terminal_reason, "ops");
        let kinds = s.history_kinds(&rid(&id)).await;
        assert_eq!(kinds.last(), Some(&EventKind::RunTerminated), "{kinds:?}");
        let r = api.terminate(&id, None, false).await;
        assert_eq!(code(&r), "conflict");
        assert_eq!(reason(&r), "run_terminal");
        let r = api.terminate(&id, None, true).await;
        assert_eq!(reason(&r), "run_terminal");

        // A busy run (one long activation) is stopped.
        let id = start(
            &s,
            "timeouts",
            json!({"input": {"mode": "busy_steps", "n": 50_000_000u64}}),
        )
        .await;
        s.wait_status(&rid(&id), RunStatus::Running, LIMIT).await;
        api.terminate(&id, None, false)
            .await
            .expect("terminate busy");
        s.wait_status(&rid(&id), RunStatus::Terminated, LIMIT).await;
        let t0 = Instant::now();
        while s.h().runtime().live_activations() > 0 {
            assert!(t0.elapsed() < LIMIT, "the busy activation did not stop");
            sleep(Duration::from_millis(10)).await;
        }
        // busy_steps journals nothing until its first flush; D3 writes RunTerminated only
        // after seq 0, so the history is empty or ends with the marker.
        let kinds = s.history_kinds(&rid(&id)).await;
        assert!(
            kinds.is_empty() || kinds.last() == Some(&EventKind::RunTerminated),
            "{kinds:?}"
        );
        let r = api
            .retry(&id, true)
            .await
            .expect("terminated runs are resumable");
        assert_eq!(r["mode"], "resumed");
    }
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Retry
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_resume_restart_and_completed_rest_and_grpc() {
    // A short hot wait so the 1.5 s sleep evicts (Scheduled) instead of parking hot.
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_millis(50).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    deploy(&s, "timeouts", None).await;
    deploy(&s, "sdk-basic", None).await;
    deploy(&s, "sdk-basic", Some("acme")).await;
    let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    for t in BOTH {
        let mut api = Api::new(&s, t).await;

        // Completed: 409 run_completed. Live: 409 run_not_terminal. Unknown: 404.
        let id = start(&s, "sdk-echo", json!({"input": [1]})).await;
        s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
        let r = api.retry(&id, false).await;
        assert_eq!(reason(&r), "run_completed", "{t:?}: {r:?}");
        let r = api.retry(&id, true).await;
        assert_eq!(reason(&r), "run_completed");
        let r = api.retry(&unknown_run(), false).await;
        assert_eq!(code(&r), "not_found");

        // Resumable (Terminated): resumed in place, same id, then completes.
        let id = start(
            &s,
            "timeouts",
            json!({"input": {"mode": "sleep_ms", "n": 1500}}),
        )
        .await;
        wait_sleeping(&s, &id).await;
        let r = api.retry(&id, false).await;
        assert_eq!(reason(&r), "run_not_terminal", "{t:?}: {r:?}");
        api.terminate(&id, None, false).await.expect("terminate");
        s.wait_status(&rid(&id), RunStatus::Terminated, LIMIT).await;
        let before = snapshot(&s, &id).await;
        let r = api.retry(&id, true).await;
        assert_eq!(
            r,
            Ok(json!({"mode": "resumed", "run_id": id, "dry_run": true}))
        );
        assert_eq!(snapshot(&s, &id).await, before);
        let r = api.retry(&id, false).await;
        assert_eq!(
            r,
            Ok(json!({"mode": "resumed", "run_id": id, "dry_run": false})),
            "{t:?}"
        );
        let rec = s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
        assert_eq!(rec.resumed_count, 1);
        assert_eq!(result_of(&s, &id).await, json!("slept"));

        // Guest-terminal failure: restarted as a new linked run (F11: tenant, tags,
        // traceparent copied; stored qualified workflow/queue; retry_of = old).
        let id = start(
            &s,
            "sdk-basic",
            json!({"input": {"wrong": true}, "tenant": "acme", "tags": ["t1", "t2"], "traceparent": tp}),
        )
        .await;
        let old = s.wait_status(&rid(&id), RunStatus::Failed, LIMIT).await;
        assert!(!old.resumable, "a guest RunFailed is not resumable");
        let before = snapshot(&s, &id).await;
        let r = api.retry(&id, true).await;
        assert_eq!(
            r,
            Ok(json!({"mode": "restarted", "run_id": id, "dry_run": true}))
        );
        assert_eq!(snapshot(&s, &id).await, before);
        let r = api.retry(&id, false).await.expect("restart");
        assert_eq!(r["mode"], "restarted");
        assert_eq!(r["dry_run"], false);
        let new_id = r["run_id"].as_str().expect("id").to_owned();
        assert_ne!(new_id, id);
        let rec = s.run_record(&rid(&new_id));
        assert_eq!(rec.retry_of, Some(rid(&id)));
        assert_eq!(rec.tenant.as_deref(), Some("acme"));
        assert_eq!(rec.tags, vec!["t1".to_owned(), "t2".to_owned()]);
        assert_eq!(rec.traceparent.as_deref(), Some(tp));
        assert_eq!(rec.workflow, "acme/sdk-basic");
        assert_eq!(rec.queue, "acme/default");
        assert_eq!(rec.workflow_version, old.workflow_version);
        assert_eq!(rec.module_sha, old.module_sha);
        assert_eq!(rec.retention_ms, old.retention_ms);
        // Same input: it fails the same way.
        s.wait_status(&rid(&new_id), RunStatus::Failed, LIMIT).await;
        assert_eq!(
            s.handle_of(&rid(&new_id))
                .fetch_payload(
                    &query::run_payload(
                        &s.handle_of(&rid(&new_id)).reader(),
                        &rid(&new_id),
                        query::PAYLOAD_INPUT
                    )
                    .expect("read")
                    .expect("input")
                )
                .await
                .expect("fetch")
                .as_ref(),
            br#"{"wrong":true}"#
        );
    }
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Await tokens
// ---------------------------------------------------------------------------------

/// Flip the last character of a token (still well formed, wrong MAC).
fn tamper(token: &str) -> String {
    let mut c: Vec<char> = token.chars().collect();
    let last = c.len() - 1;
    c[last] = if c[last] == 'a' { 'b' } else { 'a' };
    c.into_iter().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_completion_rest_and_grpc() {
    let mut s = server().await;
    deploy(&s, "tokens", None).await;
    let key = s.h().token_key();
    for t in BOTH {
        let mut api = Api::new(&s, t).await;
        let id = start(&s, "tokens", json!({"input": {}})).await;
        let token = s.published_token(&id, LIMIT).await;
        wait_parked(&s, &id).await;

        // Dry run: format, MAC and open wait checked; nothing changes.
        let before = snapshot(&s, &id).await;
        let r = api.token(&token, json!("dry"), true).await;
        assert_eq!(
            r,
            Ok(json!({"run_id": id, "accepted": true, "dry_run": true})),
            "{t:?}"
        );
        assert_eq!(snapshot(&s, &id).await, before);

        // Bad tokens: 400 token_invalid, never echoed.
        for bad in [tamper(&token), "ot1_zzzz".to_owned(), "nope".to_owned()] {
            for dry in [false, true] {
                let r = api.token(&bad, json!(1), dry).await;
                assert_eq!(code(&r), "invalid_argument", "{t:?} {bad}: {r:?}");
                assert_eq!(reason(&r), "token_invalid");
                assert!(!format!("{r:?}").contains(&bad));
            }
        }
        // A correctly signed token whose partition does not match its run: 400.
        let parts = ostro_engine::token::parse(&token).expect("parse");
        let wrong = ostro_engine::token::mint(parts.partition ^ 1, &parts.run_id, parts.op, &key);
        let r = api.token(&wrong, json!(1), false).await;
        assert_eq!(reason(&r), "token_invalid", "{t:?}: {r:?}");
        // A valid token for a run that does not exist: 404.
        let ghost = rid(&unknown_run());
        let minted = ostro_engine::token::mint(ghost.partition().0, &ghost.to_string(), 3, &key);
        let r = api.token(&minted, json!(1), false).await;
        assert_eq!(code(&r), "not_found", "{t:?}: {r:?}");

        // 1 MiB payload (D21: blob-offloaded TokenCompleted event); the run completes with
        // identical bytes. Two concurrent completions: exactly one wins, the other is 409
        // (token_already_completed with the run's status, or run_terminal if the run
        // already finished).
        let payload = json!({"big": "t".repeat(MIB), "via": format!("{t:?}")});
        let mut api2 = Api::new(&s, t).await;
        let (a, b) = tokio::join!(
            api.token(&token, payload.clone(), false),
            api2.token(&token, payload.clone(), false)
        );
        let (won, lost) = if a.is_ok() { (a, b) } else { (b, a) };
        assert_eq!(
            won,
            Ok(json!({"run_id": id, "accepted": true, "dry_run": false})),
            "{t:?}"
        );
        assert_eq!(code(&lost), "conflict", "{t:?}: {lost:?}");
        let why = reason(&lost);
        assert!(
            why == "token_already_completed" || why == "run_terminal",
            "{lost:?}"
        );
        if why == "token_already_completed" {
            assert!(
                lost.as_ref().unwrap_err().1["status"].is_string(),
                "{lost:?}"
            );
        }
        s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
        assert_eq!(result_of(&s, &id).await, payload);
        let kinds = s.history_kinds(&rid(&id)).await;
        assert_eq!(
            kinds
                .iter()
                .filter(|k| **k == EventKind::TokenCompleted)
                .count(),
            1
        );
        let r = api.token(&token, json!(2), false).await;
        assert_eq!(code(&r), "conflict");

        // Expired (WaitTimedOut journaled): 409.
        let id = start(&s, "tokens", json!({"input": {"timeout_ms": 200}})).await;
        let token = s.published_token(&id, LIMIT).await;
        s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
        assert_eq!(result_of(&s, &id).await, json!("timeout"));
        let kinds = s.history_kinds(&rid(&id)).await;
        assert!(kinds.contains(&EventKind::WaitTimedOut), "{kinds:?}");
        let r = api.token(&token, json!(1), false).await;
        assert_eq!(code(&r), "conflict", "{t:?}: {r:?}");
        let r = api.token(&token, json!(1), true).await;
        assert_eq!(code(&r), "conflict");
    }
    s.shutdown().await;
}

/// A `MakeWriter` collecting every formatted log line.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Capture {
        self.clone()
    }
}

/// Install (once per test process) a TRACE-level subscriber writing into a buffer.
fn capture_logs() -> Capture {
    static CAP: OnceLock<Capture> = OnceLock::new();
    CAP.get_or_init(|| {
        let cap = Capture::default();
        let sub = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(cap.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::set_global_default(sub).expect("global subscriber");
        cap
    })
    .clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_text_never_reaches_the_logs() {
    let cap = capture_logs();
    let mut s = server().await;
    deploy(&s, "tokens", None).await;
    let id = start(&s, "tokens", json!({"input": {}})).await;
    let token = s.published_token(&id, LIMIT).await;
    wait_parked(&s, &id).await;
    for t in BOTH {
        let mut api = Api::new(&s, t).await;
        let _ = api.token(&tamper(&token), json!(1), false).await;
        let _ = api.token(&token, json!(1), true).await;
    }
    let mut api = Api::new(&s, Tr::Grpc).await;
    api.token(&token, json!("done"), false)
        .await
        .expect("complete");
    s.wait_status(&rid(&id), RunStatus::Completed, LIMIT).await;
    let _ = api.token(&token, json!(1), false).await;
    s.shutdown().await;
    let logs = String::from_utf8_lossy(&cap.0.lock().expect("lock")).into_owned();
    assert!(!logs.is_empty(), "the capture saw no log lines at all");
    assert!(
        logs.contains(&id) || logs.contains("ostro server started"),
        "the capture did not see this server's logs"
    );
    let body = token.trim_start_matches("ot1_");
    assert!(!logs.contains(&token), "token text found in the logs");
    assert!(!logs.contains(body), "token body found in the logs");
    assert!(
        !logs.contains(&tamper(&token)),
        "tampered token found in the logs"
    );
}

// ---------------------------------------------------------------------------------
// Long-poll
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_returns_on_completion_not_at_the_timeout() {
    let mut s = server().await;
    deploy(&s, "timeouts", None).await;
    for t in BOTH {
        let mut api = Api::new(&s, t).await;
        let id = start(
            &s,
            "timeouts",
            json!({"input": {"mode": "sleep_ms", "n": 300}}),
        )
        .await;
        let t0 = Instant::now();
        let r = api.result(&id, 30_000, false).await.expect("result");
        let took = t0.elapsed();
        assert_eq!(r["completed"], true, "{t:?}: {r}");
        assert_eq!(r["status"], "completed");
        assert_eq!(r["result"], "slept");
        assert_eq!(r["has_failure"], false);
        assert!(
            took >= Duration::from_millis(250),
            "{t:?}: returned before the sleep ended ({took:?})"
        );
        assert!(
            took < Duration::from_secs(2),
            "{t:?}: returned after {took:?}, not on completion"
        );
        // A terminal run answers at once.
        let t0 = Instant::now();
        let again = api.result(&id, 30_000, false).await.expect("again");
        assert!(t0.elapsed() < Duration::from_secs(1));
        assert_eq!(again, r);
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_times_out_with_completed_false() {
    let mut s = server().await;
    deploy(&s, "timeouts", None).await;
    deploy(&s, "sdk-basic", None).await;
    let id = start(&s, "timeouts", json!({"input": {"mode": "sleep_forever"}})).await;
    wait_sleeping(&s, &id).await;
    let mut outs = Vec::new();
    for t in BOTH {
        let mut api = Api::new(&s, t).await;
        let t0 = Instant::now();
        let r = api.result(&id, 200, false).await.expect("result");
        let took = t0.elapsed();
        assert_eq!(r["completed"], false, "{t:?}: {r}");
        assert_eq!(r["status"], "scheduled");
        assert_eq!(r["has_result"], false);
        assert!(took >= Duration::from_millis(190), "{t:?}: {took:?}");
        assert!(took < Duration::from_millis(1500), "{t:?}: {took:?}");
        // wait=0 answers immediately.
        let t0 = Instant::now();
        let now = api.result(&id, 0, false).await.expect("now");
        assert!(t0.elapsed() < Duration::from_millis(500));
        assert_eq!(now["completed"], false);
        // Unknown run: 404.
        let r = api.result(&unknown_run(), 100, false).await;
        assert_eq!(code(&r), "not_found");
        outs.push(r.unwrap_err());
        // A failed run: failure JSON, no result (identical on both transports).
        let f = start(&s, "sdk-basic", json!({"input": {"wrong": true}})).await;
        let r = api.result(&f, 30_000, false).await.expect("failed");
        assert_eq!(r["status"], "failed");
        assert!(r["failure"]["reason_code"].is_string(), "{r}");
        assert_eq!(r["has_result"], false);
    }
    assert_eq!(outs[0], outs[1], "404 details differ between transports");
    assert_eq!(s.h().control().watchers().waiters(), 0);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_no_lost_wakeup_when_completion_races_registration() {
    let s = server().await;
    deploy(&s, "sdk-basic", None).await;
    let s = Arc::new(s);
    // 200 iterations in 10 concurrent lanes: start, then immediately long-poll (10 s). A
    // completion that lands between registration and the read, or before either, must
    // never leave a request waiting for the full timeout.
    let mut lanes = Vec::new();
    for lane in 0..10u32 {
        let s = Arc::clone(&s);
        lanes.push(tokio::spawn(async move {
            let mut rest = Api::new(&s, Tr::Rest).await;
            let mut grpc = Api::new(&s, Tr::Grpc).await;
            let mut worst = Duration::ZERO;
            for i in 0..20u32 {
                let id = start(&s, "sdk-echo", json!({"input": {"lane": lane, "i": i}})).await;
                let api = if i % 2 == 0 { &mut rest } else { &mut grpc };
                let t0 = Instant::now();
                let r = api.result(&id, 10_000, false).await.expect("result");
                let took = t0.elapsed();
                assert_eq!(r["completed"], true, "lane {lane} iteration {i}: {r}");
                assert_eq!(r["result"], json!({"lane": lane, "i": i}));
                assert!(
                    took < Duration::from_secs(8),
                    "lane {lane} iteration {i} waited {took:?}"
                );
                worst = worst.max(took);
            }
            worst
        }));
    }
    let mut worst = Duration::ZERO;
    for l in lanes {
        worst = worst.max(l.await.expect("lane"));
    }
    eprintln!("long-poll race: slowest of 200 answers took {worst:?}");
    assert_eq!(s.h().control().watchers().waiters(), 0, "no waiter leaked");
    let mut s = Arc::try_unwrap(s).unwrap_or_else(|_| panic!("server still shared"));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_follows_continuations_with_transport_parity() {
    let mut s = server().await;
    deploy(&s, "can-chain", None).await;
    let id = start(&s, "can-chain", json!({"input": {"n": 0, "max": 3}})).await;
    let mut outs = Vec::new();
    for t in BOTH {
        let mut api = Api::new(&s, t).await;
        let r = api.result(&id, 30_000, true).await.expect("follow");
        assert_eq!(r["completed"], true, "{t:?}: {r}");
        assert_eq!(r["status"], "completed");
        assert_eq!(r["result"], 3);
        assert_ne!(r["run_id"], json!(id), "answered for the last successor");
        // Without follow: the first run is ContinuedAsNew with {"next_run_id"}.
        let first = api.result(&id, 30_000, false).await.expect("no follow");
        assert_eq!(first["status"], "continued_as_new", "{first}");
        assert_eq!(first["run_id"], json!(id));
        assert!(first["result"]["next_run_id"].is_string(), "{first}");
        assert_eq!(first["has_failure"], false);
        outs.push((r, first));
    }
    assert_eq!(outs[0], outs[1], "REST and gRPC answers differ");
    // The chain is exactly 3 continuations long.
    let mut cur = id.clone();
    let mut hops = 0;
    loop {
        let rec = s.run_record(&rid(&cur));
        match rec.continued_to {
            Some(next) => {
                assert_eq!(rec.status, RunStatus::ContinuedAsNew);
                cur = next.to_string();
                hops += 1;
            }
            None => break,
        }
    }
    assert_eq!(hops, 3);
    assert_eq!(json!(cur), outs[0].0["run_id"]);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_waiter_is_dropped_with_its_request() {
    let mut s = server().await;
    deploy(&s, "timeouts", None).await;
    let id = start(&s, "timeouts", json!({"input": {"mode": "sleep_forever"}})).await;
    wait_sleeping(&s, &id).await;
    let cp = Arc::clone(s.h().control());
    let watchers = || cp.watchers().waiters();
    assert_eq!(watchers(), 0);

    // In process: the request future is dropped (what a disconnect does to a handler).
    let task = {
        let cp = Arc::clone(&cp);
        let id = id.clone();
        tokio::spawn(async move {
            use ostro_api::ControlPlane;
            cp.await_result(AwaitResultRequest {
                run_id: id,
                wait_ms: 30_000,
                follow_continuations: false,
            })
            .await
        })
    };
    let t0 = Instant::now();
    while watchers() == 0 {
        assert!(t0.elapsed() < LIMIT, "the long-poll never registered");
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(watchers(), 1);
    task.abort();
    let _ = task.await;
    assert_eq!(watchers(), 0, "an aborted long-poll left its waiter behind");
    assert_eq!(cp.watchers().watched_runs(), 0);

    // Over HTTP: a client that gives up closes the connection; the server-side waiter goes.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()
        .expect("client");
    let r = client
        .get(s.url(&format!("/v1/runs/{id}/result?wait=30s")))
        .send()
        .await;
    assert!(r.is_err(), "the client timeout should fire first");
    let t0 = Instant::now();
    while watchers() > 0 {
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "a disconnected HTTP long-poll kept its waiter"
        );
        sleep(Duration::from_millis(10)).await;
    }

    // A completion wakes and removes every waiter of the run (several concurrent polls).
    let mut polls = Vec::new();
    for _ in 0..3 {
        let cp = Arc::clone(&cp);
        let id = id.clone();
        polls.push(tokio::spawn(async move {
            use ostro_api::ControlPlane;
            cp.await_result(AwaitResultRequest {
                run_id: id,
                wait_ms: 30_000,
                follow_continuations: false,
            })
            .await
        }));
    }
    let t0 = Instant::now();
    while watchers() < 3 {
        assert!(t0.elapsed() < LIMIT);
        sleep(Duration::from_millis(5)).await;
    }
    let mut api = Api::new(&s, Tr::Rest).await;
    api.terminate(&id, None, false).await.expect("terminate");
    for p in polls {
        let r = tokio::time::timeout(Duration::from_secs(5), p)
            .await
            .expect("woken by the terminal transition")
            .expect("join")
            .expect("result");
        assert!(r.completed);
        assert_eq!(r.failure, Some(json!({"reason_code": ""})));
    }
    assert_eq!(watchers(), 0);
    drop(cp);
    s.shutdown().await;
}
