//! Phase 13 `routing` scenarios (tier B, 13-12; API-08 / D-P13-14): a request sent to any
//! node reaches the owning partition's leader (or fans out) with single-node results.
//!
//! - `parity_through_every_node`: the parity transcript (core run API, lists with paging,
//!   error answers, policies) through every node of a 3-node RF=3 cluster, REST and gRPC,
//!   equals the transcript of a single node (run ids, timestamps and the per-execution tag
//!   normalised).
//! - `not_leader_retry_after_move`: a partition's leader resigns (`ResignLeader`, the
//!   controller elects another ISR member) while a third node keeps taking keyed starts and
//!   reads for it: every call succeeds (at most one re-route each).
//! - `offline_partition_503`: the partition's whole ISR is dead: every live node answers
//!   REST 503 + `Retry-After: 1` / gRPC `UNAVAILABLE` (`partition_offline`).
//! - `unkeyed_start_zero_hops_when_local`: a node that leads a partition places unkeyed
//!   starts locally (forward counter unchanged); a node that leads none forwards them with
//!   the pin, and every start lands (the pin is honoured from the node identity).
//! - `list_fanout_complete`: runs on every partition; `list_runs` through any node returns
//!   all of them in the single-node order, also page by page.
//! - `hop_limit_no_loop` (`--features failpoints`): with `route::stale_view` every node
//!   believes another replica leads: the call ends in a bounded 503 after at most two
//!   forwards, no loop.
//! - `internal_consumers_route`: webhook deliveries (keyed starts) and schedule triggers
//!   whose partitions are led elsewhere create their runs; the cron driver fires on the
//!   owner partition.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_api::ControlPlane;
use ostro_api::domain::{
    CancelRequest, CompleteTokenRequest, HistoryRequest, ListRunsRequest, RunPolicy, SignalRequest,
    StartRunRequest,
};
use ostro_api::grpc_client::GrpcControlPlane;
use ostro_server::route::RouteStats;
use ostro_types::{Epoch, PartitionCount, PartitionId, RunId};
use serde_json::{Value, json};

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, SESSION, blackhole, deploy, fail, key_for, pause, published,
    published_all, same_set, wait_assignment, wait_meta_leader,
};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{InProcCluster, TEST_WORKFLOW};
use crate::harness::{ApiClient, ClusterHarness, ClusterSpec};

type Run = for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>;

/// Deadline of the routing scenarios (the parity one boots a second, single-node cluster).
const DEADLINE: Duration = Duration::from_secs(300);

/// Every `routing` scenario.
pub fn all() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "routing",
        tier: Tier::B,
        deadline: DEADLINE,
        spec: rf3,
        run,
    };
    #[allow(unused_mut)]
    let mut v = vec![
        s("parity_through_every_node", parity_through_every_node),
        s("not_leader_retry_after_move", not_leader_retry_after_move),
        s("offline_partition_503", offline_partition_503),
        s(
            "unkeyed_start_zero_hops_when_local",
            unkeyed_start_zero_hops_when_local,
        ),
        s("list_fanout_complete", list_fanout_complete),
        s("internal_consumers_route", internal_consumers_route),
    ];
    #[cfg(feature = "failpoints")]
    v.push(s("hop_limit_no_loop", hop_limit_no_loop));
    v
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 1312,
        ..ClusterSpec::default()
    }
}

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

fn stats(c: &InProcCluster, node: u64) -> Arc<RouteStats> {
    c.handle(node)
        .and_then(|h| h.p13())
        .and_then(ostro_server::route::stats)
        .unwrap_or_else(|| panic!("harness: node {node} has no routing state"))
}

fn forwarded(c: &InProcCluster, node: u64) -> u64 {
    stats(c, node).forwarded()
}

fn count(c: &InProcCluster) -> PartitionCount {
    PartitionCount::new(c.spec().partitions).expect("partitions > 0")
}

/// A gRPC client of `node`'s API listener.
fn grpc(api: &ApiClient) -> GrpcControlPlane {
    let ch = tonic::transport::Endpoint::from_shared(api.base_url.clone())
        .expect("API url")
        .connect_lazy();
    GrpcControlPlane::new(ch)
}

/// `method path` with an optional JSON body: `(status, body, Retry-After)`.
async fn rest(
    api: &ApiClient,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> (u16, Value, Option<String>) {
    let mut req = api
        .http
        .request(method.clone(), format!("{}{path}", api.base_url))
        .timeout(Duration::from_secs(30));
    if let Some(b) = body {
        req = req.json(&b);
    }
    let r = req
        .send()
        .await
        .unwrap_or_else(|e| panic!("harness: {method} {path}: {e}"));
    let st = r.status().as_u16();
    let retry = r
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let b = r.bytes().await.unwrap_or_default();
    (st, serde_json::from_slice(&b).unwrap_or(Value::Null), retry)
}

/// Poll `GET /v1/runs/{id}` through `api` until completed.
async fn wait_done(api: &ApiClient, run: &str) -> Result<(), ScenarioFailure> {
    api.wait_run(run, ACK_T)
        .await
        .map(|_| ())
        .map_err(|e| fail("availability", format!("run {run}: {e}")))
}

/// Deploy the test module and wait until every node's metadata shows the workflow active
/// (a node's meta reads may trail the meta leader until 13-13's read barrier).
async fn deploy_everywhere(c: &InProcCluster) -> ScenarioResult {
    deploy(c).await?;
    for node in c.node_ids() {
        let api = c.client(node);
        let seen = super::placement::support::poll_async(CHANGE_T, || {
            let api = api.clone();
            async move {
                let (st, v, _) = rest(&api, reqwest::Method::GET, "/v1/workflows", None).await;
                let list = v.get("workflows").unwrap_or(&v);
                let active = list.as_array().is_some_and(|a| {
                    a.iter().any(|w| {
                        w["name"] == TEST_WORKFLOW && w["active_version"].as_u64().is_some()
                    })
                });
                (st == 200 && active).then_some(())
            }
        })
        .await;
        if seen.is_none() {
            return Err(fail(
                "availability",
                format!("node {node}: the deployed workflow is not visible"),
            ));
        }
    }
    Ok(())
}

/// A process-unique tag.
fn tag(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    format!("{prefix}{}", N.fetch_add(1, Ordering::Relaxed))
}

/// Replace run ids (in first-seen order), the tag and every `*_ms` number.
struct Normaliser {
    tag: String,
    runs: Vec<String>,
}

impl Normaliser {
    fn new(tag: &str) -> Self {
        Self {
            tag: tag.to_owned(),
            runs: Vec::new(),
        }
    }

    fn run(&mut self, id: &str) {
        if !id.is_empty() && !self.runs.iter().any(|r| r == id) {
            self.runs.push(id.to_owned());
        }
    }

    fn text(&self, s: &str) -> String {
        let mut out = s.replace(&self.tag, "TAG");
        for (i, r) in self.runs.iter().enumerate() {
            out = out.replace(r, &format!("R{i}"));
        }
        zero_ms(&out)
    }

    fn json(&self, v: &Value) -> Value {
        match v {
            Value::Object(m) => Value::Object(
                m.iter()
                    .map(|(k, v)| {
                        let v = if k.ends_with("_ms") && (v.is_number() || v.is_null()) {
                            json!(v.is_number())
                        } else if v.as_str().is_some_and(is_timestamp) {
                            json!("T")
                        } else {
                            self.json(v)
                        };
                        (k.clone(), v)
                    })
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(|v| self.json(v)).collect()),
            Value::String(s) => Value::String(self.text(s)),
            other => other.clone(),
        }
    }
}

/// An RFC 3339 UTC timestamp (`2026-11-25T01:18:53.523Z`).
fn is_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 20 && b[4] == b'-' && b[7] == b'-' && b[10] == b'T' && s.ends_with('Z')
}

/// `<x>_ms: 123` / `<x>_ms: Some(123)` in Debug text → `0`.
fn zero_ms(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("_ms: ") {
        let (head, tail) = rest.split_at(i + "_ms: ".len());
        out.push_str(head);
        let tail = match tail.strip_prefix("Some(") {
            Some(t) => {
                out.push_str("Some(");
                t
            }
            None => tail,
        };
        let digits = tail.chars().take_while(char::is_ascii_digit).count();
        if digits > 0 {
            out.push('0');
        }
        rest = &tail[digits..];
    }
    out.push_str(rest);
    out
}

/// A well-formed run id that does not exist, on partition `p`.
fn absent_run(p: u16) -> String {
    RunId::new_v7(PartitionId(p)).to_string()
}

/// The REST parity transcript through `api` (one line per step).
async fn rest_transcript(api: &ApiClient, partitions: u16) -> Result<Vec<String>, ScenarioFailure> {
    use reqwest::Method as M;
    let t = tag("parity-rest-");
    let mut n = Normaliser::new(&t);
    let mut raw: Vec<(String, u16, Value)> = Vec::new();
    let wf = TEST_WORKFLOW;
    let start = |input: Value, key: Option<String>| {
        let mut b = json!({ "input": input, "tags": [t.clone()] });
        if let Some(k) = key {
            b["idempotency_key"] = json!(k);
        }
        b
    };
    let (st, v, _) = rest(api, M::GET, "/v1/workflows", None).await;
    raw.push(("list workflows".into(), st, v));
    let (st, v1, _) = rest(
        api,
        M::POST,
        &format!("/v1/workflows/{wf}/runs"),
        Some(start(json!({ "n": 1 }), Some(format!("{t}-k1")))),
    )
    .await;
    let r1 = v1["run_id"].as_str().unwrap_or_default().to_owned();
    n.run(&r1);
    raw.push(("start keyed".into(), st, v1));
    let (st, v, _) = rest(
        api,
        M::POST,
        &format!("/v1/workflows/{wf}/runs"),
        Some(start(json!({ "n": 1 }), Some(format!("{t}-k1")))),
    )
    .await;
    raw.push(("start keyed again".into(), st, v));
    let (st, v2, _) = rest(
        api,
        M::POST,
        &format!("/v1/workflows/{wf}/runs"),
        Some(start(json!({ "n": 2 }), None)),
    )
    .await;
    let r2 = v2["run_id"].as_str().unwrap_or_default().to_owned();
    n.run(&r2);
    raw.push(("start unkeyed".into(), st, v2));
    if r1.is_empty() || r2.is_empty() {
        return Err(fail(
            "parity",
            format!("{}: starts failed: {raw:?}", api.base_url),
        ));
    }
    wait_done(api, &r1).await?;
    wait_done(api, &r2).await?;
    for (label, path) in [
        ("get run", format!("/v1/runs/{r1}")),
        ("history", format!("/v1/runs/{r1}/history")),
        ("result", format!("/v1/runs/{r1}/result")),
        ("list by tag", format!("/v1/runs?tag={t}")),
        ("list newest", format!("/v1/runs?tag={t}&order=newest")),
    ] {
        let (st, v, _) = rest(api, M::GET, &path, None).await;
        raw.push((label.into(), st, v));
    }
    // Paging: one run per page.
    let (st, p1, _) = rest(api, M::GET, &format!("/v1/runs?tag={t}&limit=1"), None).await;
    let tok = p1["next_page_token"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    raw.push(("list page 1".into(), st, p1));
    let (st, p2, _) = rest(
        api,
        M::GET,
        &format!("/v1/runs?tag={t}&limit=1&page_token={tok}"),
        None,
    )
    .await;
    raw.push(("list page 2".into(), st, p2));
    let (st, v, _) = rest(
        api,
        M::POST,
        &format!("/v1/runs/{r1}/signals/go"),
        Some(json!({ "payload": {} })),
    )
    .await;
    raw.push(("signal a completed run".into(), st, v));
    let (st, v, _) = rest(
        api,
        M::POST,
        &format!("/v1/runs/{r1}/cancel"),
        Some(json!({})),
    )
    .await;
    raw.push(("cancel a completed run".into(), st, v));
    for p in 0..partitions {
        let id = absent_run(p);
        n.run(&id);
        let (st, v, _) = rest(api, M::GET, &format!("/v1/runs/{id}"), None).await;
        raw.push((format!("absent run on p{p}"), st, v));
    }
    let (st, v, _) = rest(api, M::GET, "/v1/runs/not-a-run-id", None).await;
    raw.push(("malformed run id".into(), st, v));
    let (st, v, _) = rest(
        api,
        M::POST,
        "/v1/tokens/complete",
        Some(json!({ "token": "ot1_aaaa", "payload": {} })),
    )
    .await;
    raw.push(("garbage token".into(), st, v));
    let (st, v, _) = rest(
        api,
        M::PUT,
        &format!("/v1/workflows/{wf}/policy"),
        Some(json!({ "retention": "24h" })),
    )
    .await;
    raw.push(("put policy".into(), st, v));
    let (st, v, _) = rest(api, M::GET, &format!("/v1/workflows/{wf}/policy"), None).await;
    raw.push(("get policy".into(), st, v));
    Ok(raw
        .into_iter()
        .map(|(label, st, v)| format!("{label}: {st} {}", n.json(&v)))
        .collect())
}

/// The gRPC parity transcript through `api` (domain results, Debug-formatted).
async fn grpc_transcript(api: &ApiClient, partitions: u16) -> Result<Vec<String>, ScenarioFailure> {
    let cp = grpc(api);
    let t = tag("parity-grpc-");
    let mut n = Normaliser::new(&t);
    let mut raw: Vec<(String, String)> = Vec::new();
    let start = |input: &str, key: Option<String>| StartRunRequest {
        workflow: TEST_WORKFLOW.into(),
        input_json: input.as_bytes().to_vec(),
        tags: vec![t.clone()],
        idempotency_key: key,
        ..StartRunRequest::default()
    };
    raw.push((
        "list workflows".into(),
        format!("{:?}", cp.list_workflows().await),
    ));
    let s1 = cp
        .start_run(start(r#"{"n":1}"#, Some(format!("{t}-k1"))))
        .await;
    let r1 = s1.as_ref().map(|r| r.run_id.clone()).unwrap_or_default();
    n.run(&r1);
    raw.push(("start keyed".into(), format!("{s1:?}")));
    let again = cp
        .start_run(start(r#"{"n":1}"#, Some(format!("{t}-k1"))))
        .await;
    raw.push(("start keyed again".into(), format!("{again:?}")));
    let s2 = cp.start_run(start(r#"{"n":2}"#, None)).await;
    let r2 = s2.as_ref().map(|r| r.run_id.clone()).unwrap_or_default();
    n.run(&r2);
    raw.push(("start unkeyed".into(), format!("{s2:?}")));
    if r1.is_empty() || r2.is_empty() {
        return Err(fail(
            "parity",
            format!("{} (gRPC): starts failed: {raw:?}", api.base_url),
        ));
    }
    wait_done(api, &r1).await?;
    wait_done(api, &r2).await?;
    raw.push((
        "get run".into(),
        format!("{:?}", cp.get_run(r1.clone()).await),
    ));
    // History details are JSON bytes (timestamps inside): compared as normalised JSON.
    let history = cp
        .run_history(HistoryRequest {
            run_id: r1.clone(),
            from_seq: 0,
            limit: 0,
        })
        .await
        .map(|h| {
            let events: Vec<String> = h
                .events
                .iter()
                .map(|e| {
                    let d: Value = serde_json::from_slice(&e.details_json).unwrap_or(Value::Null);
                    format!(
                        "{} {} {:?} {:?} {}",
                        e.seq,
                        e.kind,
                        e.summary,
                        e.attempt,
                        n.json(&d)
                    )
                })
                .collect();
            (events, h.next_from_seq)
        });
    raw.push(("history".into(), format!("{history:?}")));
    let by_tag = |limit: u32, page_token: Option<String>| ListRunsRequest {
        tag: Some(t.clone()),
        tags: vec![t.clone()],
        limit,
        page_token,
        ..ListRunsRequest::default()
    };
    raw.push((
        "list by tag".into(),
        format!("{:?}", cp.list_runs(by_tag(0, None)).await),
    ));
    let p1 = cp.list_runs(by_tag(1, None)).await;
    let tok = p1.as_ref().ok().and_then(|p| p.next_page_token.clone());
    raw.push(("list page 1".into(), format!("{p1:?}")));
    raw.push((
        "list page 2".into(),
        format!("{:?}", cp.list_runs(by_tag(1, tok)).await),
    ));
    raw.push((
        "signal a completed run".into(),
        format!(
            "{:?}",
            cp.signal_run(SignalRequest {
                run_id: r1.clone(),
                name: "go".into(),
                payload: json!({}),
                request_id: None,
                dry_run: false,
            })
            .await
        ),
    ));
    raw.push((
        "cancel a completed run".into(),
        format!(
            "{:?}",
            cp.cancel_run(CancelRequest {
                run_id: r1.clone(),
                reason: None,
                dry_run: false,
            })
            .await
        ),
    ));
    for p in 0..partitions {
        let id = absent_run(p);
        n.run(&id);
        raw.push((
            format!("absent run on p{p}"),
            format!("{:?}", cp.get_run(id).await),
        ));
    }
    raw.push((
        "malformed run id".into(),
        format!("{:?}", cp.get_run("not-a-run-id".into()).await),
    ));
    raw.push((
        "garbage token".into(),
        format!(
            "{:?}",
            cp.complete_token(CompleteTokenRequest {
                token: "ot1_aaaa".into(),
                payload: json!({}),
                dry_run: false,
            })
            .await
            .map(|r| r.accepted)
        ),
    ));
    raw.push((
        "put policy".into(),
        format!(
            "{:?}",
            cp.set_workflow_policy(
                TEST_WORKFLOW.into(),
                RunPolicy {
                    retention_ms: Some(86_400_000),
                    schedule_to_start_ms: None,
                },
            )
            .await
        ),
    ));
    raw.push((
        "get policy".into(),
        format!("{:?}", cp.get_workflow_policy(TEST_WORKFLOW.into()).await),
    ));
    Ok(raw
        .into_iter()
        .map(|(label, s)| format!("{label}: {}", n.text(&s)))
        .collect())
}

fn first_diff(want: &[String], got: &[String]) -> String {
    for (i, (w, g)) in want.iter().zip(got).enumerate() {
        if w != g {
            return format!("step {i}:\n  single-node: {w}\n  this node:   {g}");
        }
    }
    format!("lengths {} vs {}", want.len(), got.len())
}

// ---------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------

/// The parity transcript through every node, REST and gRPC, equals a single node's.
fn parity_through_every_node(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let partitions = c.spec().partitions;
        // The single-node reference (same partition count).
        let single_spec = ClusterSpec {
            partitions,
            seed: 13120,
            ..ClusterSpec::single()
        };
        let single = InProcCluster::start(single_spec)
            .await
            .unwrap_or_else(|e| panic!("harness: single-node reference: {e}"));
        let api = single.client(1);
        let reference = async {
            let up = super::placement::support::poll_async(CHANGE_T, || {
                let api = api.clone();
                async move { api.deploy_test_module().await.ok() }
            })
            .await;
            if up.is_none() {
                return Err(fail("availability", "single node: deploy failed"));
            }
            Ok((
                rest_transcript(&api, partitions).await?,
                grpc_transcript(&api, partitions).await?,
            ))
        }
        .await;
        single.stop().await;
        let (want_rest, want_grpc) = reference?;

        deploy_everywhere(c).await?;
        for node in c.node_ids() {
            let api = c.client(node);
            let got = rest_transcript(&api, partitions).await?;
            if got != want_rest {
                return Err(fail(
                    "parity",
                    format!("node {node} REST: {}", first_diff(&want_rest, &got)),
                ));
            }
            let got = grpc_transcript(&api, partitions).await?;
            if got != want_grpc {
                return Err(fail(
                    "parity",
                    format!("node {node} gRPC: {}", first_diff(&want_grpc, &got)),
                ));
            }
        }
        // The transcripts crossed nodes: something was forwarded.
        let total: u64 = c.node_ids().into_iter().map(|n| forwarded(c, n)).sum();
        if total == 0 {
            return Err(fail(
                "routing",
                "no call was forwarded across three nodes (every partition led everywhere?)",
            ));
        }
        Ok(())
    })
}

/// A leader resigns while a third node keeps calling: every call succeeds, except a 503
/// with `Retry-After` while the partition has no leader (the move window: from the
/// resignation until the new leader is published, plus two session timeouts for the
/// caller's view).
fn not_leader_retry_after_move(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        // clock-exempt: test timeline (move window bounds).
        use std::time::Instant;
        deploy_everywhere(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = published_all(c)
            .into_iter()
            .find(|a| a.leader != 0 && a.isr.len() == 3)
            .ok_or_else(|| fail("placement", "no partition with a full ISR"))?;
        let (p, leader) = (a.partition, a.leader);
        // The caller: a node that is neither the leader nor (preferably) the meta leader.
        let caller = c
            .node_ids()
            .into_iter()
            .filter(|n| *n != leader)
            .max_by_key(|n| *n != meta)
            .expect("three nodes");
        let api = c.client(caller);
        let handle = c
            .handle(leader)
            .and_then(|h| h.p13())
            .cloned()
            .expect("leader running");
        let rerouted_before = stats(c, caller)
            .rerouted
            .load(std::sync::atomic::Ordering::Relaxed);
        let c_ref: &InProcCluster = c;
        let calls = async {
            let mut errors: Vec<(Instant, u16, Option<String>, String)> = Vec::new();
            let mut ok = 0usize;
            for i in 0..40 {
                let key = key_for(c_ref, p, "move");
                let (st, v, retry) = rest(
                    &api,
                    reqwest::Method::POST,
                    &format!("/v1/workflows/{TEST_WORKFLOW}/runs"),
                    Some(json!({ "input": { "i": i }, "idempotency_key": key })),
                )
                .await;
                if (200..300).contains(&st) {
                    ok += 1;
                    let run = v["run_id"].as_str().unwrap_or_default().to_owned();
                    let (st, v, retry) =
                        rest(&api, reqwest::Method::GET, &format!("/v1/runs/{run}"), None).await;
                    if st != 200 {
                        errors.push((Instant::now(), st, retry, format!("GET {run}: {v}")));
                    }
                } else {
                    errors.push((Instant::now(), st, retry, format!("start {i}: {v}")));
                }
                pause(Duration::from_millis(50)).await;
            }
            (ok, errors)
        };
        let resign = async {
            pause(Duration::from_millis(300)).await;
            let t = Instant::now();
            handle
                .cluster
                .assignments()
                .resign(PartitionId(p), Epoch(a.epoch))
                .await;
            let moved = super::placement::support::poll(CHANGE_T, || {
                let x = published(c_ref, p);
                (x.leader != 0 && x.epoch > a.epoch).then_some(Instant::now())
            })
            .await;
            (t, moved)
        };
        let ((ok, errors), (resigned_at, moved_at)) = tokio::join!(calls, resign);
        let moved = wait_assignment(c, p, "moved off the resigned leader", CHANGE_T, |x| {
            x.leader != 0 && x.epoch > a.epoch
        })
        .await?;
        let moved_at = moved_at.ok_or_else(|| fail("election", "no new leader published"))?;
        let window_end = moved_at + SESSION * 2;
        let outside: Vec<String> = errors
            .iter()
            .filter(|(t, st, retry, _)| {
                !(*st == 503
                    && retry.as_deref() == Some("1")
                    && *t >= resigned_at
                    && *t <= window_end)
            })
            .map(|(t, st, retry, d)| {
                format!(
                    "{st} (Retry-After {retry:?}) at +{:?} from the resignation: {d}",
                    t.saturating_duration_since(resigned_at)
                )
            })
            .collect();
        if !outside.is_empty() {
            return Err(fail(
                "routing",
                format!(
                    "partition {p}: leader {leader} -> {} (epoch {} -> {}, published +{:?}), caller node {caller}: {} of {} calls failed outside the move window: {outside:?}",
                    moved.leader,
                    a.epoch,
                    moved.epoch,
                    moved_at.saturating_duration_since(resigned_at),
                    outside.len(),
                    ok + errors.len()
                ),
            ));
        }
        let rerouted = stats(c, caller)
            .rerouted
            .load(std::sync::atomic::Ordering::Relaxed)
            - rerouted_before;
        println!(
            "not_leader_retry_after_move: p{p} leader {leader} -> {} epoch {} -> {} (published +{:?}), caller {caller}: {ok} ok, {} in-window 503s, {rerouted} re-routes",
            moved.leader,
            a.epoch,
            moved.epoch,
            moved_at.saturating_duration_since(resigned_at),
            errors.len()
        );
        Ok(())
    })
}

/// The whole ISR of a partition is dead: every live node answers 503 + Retry-After.
fn offline_partition_503(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_everywhere(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = published_all(c)
            .into_iter()
            .find(|a| a.leader != 0 && a.leader != meta)
            .ok_or_else(|| fail("placement", "no partition led off the meta leader"))?;
        let (p, leader) = (a.partition, a.leader);
        let run = super::placement::support::append_acked(c, p, 1, ACK_T).await?;
        let run_id = run[0].run_id.clone();
        for f in a.replicas.iter().filter(|r| **r != leader) {
            blackhole(c, *f, leader, RpcKind::Fetch);
        }
        wait_assignment(
            c,
            p,
            "shrunk to the leader alone",
            LAG_MAX + CHANGE_T,
            |x| same_set(&x.isr, &[leader]),
        )
        .await?;
        c.kill(leader).await;
        wait_assignment(c, p, "offline", SESSION + CHANGE_T, |x| x.leader == 0).await?;
        // Let every live node's view catch up.
        pause(SESSION * 2).await;
        for node in c.node_ids().into_iter().filter(|n| *n != leader) {
            let api = c.client(node);
            let key = key_for(c, p, "offline");
            let (st, v, retry) = rest(
                &api,
                reqwest::Method::POST,
                &format!("/v1/workflows/{TEST_WORKFLOW}/runs"),
                Some(json!({ "input": {}, "idempotency_key": key })),
            )
            .await;
            if st != 503 || retry.as_deref() != Some("1") {
                return Err(fail(
                    "routing",
                    format!(
                        "node {node}: a start on offline partition {p} answered {st} (Retry-After {retry:?}): {v}"
                    ),
                ));
            }
            if v["error"]["details"]["reason"] != "partition_offline" {
                return Err(fail(
                    "routing",
                    format!("node {node}: 503 without reason partition_offline: {v}"),
                ));
            }
            let (st, v, retry) = rest(
                &api,
                reqwest::Method::GET,
                &format!("/v1/runs/{run_id}"),
                None,
            )
            .await;
            if st != 503 || retry.as_deref() != Some("1") {
                return Err(fail(
                    "routing",
                    format!("node {node}: GET a run of offline partition {p}: {st} {retry:?} {v}"),
                ));
            }
            let e = grpc(&api)
                .get_run(run_id.clone())
                .await
                .err()
                .ok_or_else(|| fail("routing", format!("node {node}: gRPC get_run succeeded")))?;
            if e.grpc_code() != tonic::Code::Unavailable {
                return Err(fail(
                    "routing",
                    format!("node {node}: gRPC answered {e:?}, expected UNAVAILABLE"),
                ));
            }
        }
        Ok(())
    })
}

/// Unkeyed starts: zero hops on a node that leads a partition; forwarded with the pin from
/// a node that leads none.
fn unkeyed_start_zero_hops_when_local(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_everywhere(c).await?;
        let count = count(c);
        let led_by = |c: &InProcCluster, n: u64| -> Vec<u16> {
            published_all(c)
                .into_iter()
                .filter(|a| a.leader == n)
                .map(|a| a.partition)
                .collect()
        };
        let starts = |api: ApiClient, k: usize| async move {
            let mut ids = Vec::new();
            for i in 0..k {
                let (st, v, _) = rest(
                    &api,
                    reqwest::Method::POST,
                    &format!("/v1/workflows/{TEST_WORKFLOW}/runs"),
                    Some(json!({ "input": { "i": i } })),
                )
                .await;
                if !(200..300).contains(&st) {
                    return Err(fail("routing", format!("unkeyed start {i}: {st} {v}")));
                }
                ids.push(v["run_id"].as_str().unwrap_or_default().to_owned());
            }
            Ok(ids)
        };
        // A node that leads at least one partition: zero hops, a led partition every time.
        let local = c
            .node_ids()
            .into_iter()
            .find(|n| !led_by(c, *n).is_empty())
            .ok_or_else(|| fail("placement", "no node leads a partition"))?;
        let before = forwarded(c, local);
        let led = led_by(c, local);
        for id in starts(c.client(local), 8).await? {
            let run: RunId = id
                .parse()
                .map_err(|e| fail("routing", format!("run id {id}: {e}")))?;
            if !led.contains(&run.partition().0) {
                return Err(fail(
                    "routing",
                    format!(
                        "node {local} leads {led:?} but placed an unkeyed start on partition {}",
                        run.partition().0
                    ),
                ));
            }
        }
        let after = forwarded(c, local);
        if after != before {
            return Err(fail(
                "routing",
                format!(
                    "node {local}: unkeyed starts forwarded {} times",
                    after - before
                ),
            ));
        }
        // A node that leads none: resign its partitions first.
        let none = c
            .node_ids()
            .into_iter()
            .min_by_key(|n| led_by(c, *n).len())
            .expect("nodes");
        let h = c
            .handle(none)
            .and_then(|h| h.p13())
            .cloned()
            .expect("running");
        for p in led_by(c, none) {
            let e = published(c, p).epoch;
            h.cluster
                .assignments()
                .resign(PartitionId(p), Epoch(e))
                .await;
            wait_assignment(c, p, "moved off the node", CHANGE_T, |x| {
                x.leader != 0 && x.leader != none
            })
            .await?;
        }
        let views_settled = super::placement::support::poll(CHANGE_T, || {
            c.handle(none)
                .map(|h| {
                    ostro_partition::roles::PartitionRoles::led(h.node())
                        .borrow()
                        .is_empty()
                })
                .unwrap_or(false)
                .then_some(())
        })
        .await;
        if views_settled.is_none() {
            return Err(fail(
                "routing",
                format!("node {none} still leads a partition"),
            ));
        }
        let before = forwarded(c, none);
        let ids = starts(c.client(none), 8).await?;
        let after = forwarded(c, none);
        if after - before < ids.len() as u64 {
            return Err(fail(
                "routing",
                format!(
                    "node {none} leads nothing but forwarded only {} of {} unkeyed starts",
                    after - before,
                    ids.len()
                ),
            ));
        }
        for id in &ids {
            let run: RunId = id
                .parse()
                .map_err(|e| fail("routing", format!("run id {id}: {e}")))?;
            if !count.contains(run.partition()) {
                return Err(fail("routing", format!("run {id} outside the partitions")));
            }
            wait_done(&c.client(none), id).await?;
        }
        Ok(())
    })
}

/// Runs on every partition: `list_runs` through any node lists all of them, in order.
fn list_fanout_complete(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_everywhere(c).await?;
        let parts = c.spec().partitions;
        let mut started = Vec::new();
        let any = c.client(c.node_ids()[0]);
        for p in 0..parts {
            for _ in 0..3 {
                let key = key_for(c, p, "fan");
                let ack = any
                    .start_run(TEST_WORKFLOW, json!({ "k": key }), Some(&key))
                    .await
                    .map_err(|e| fail("availability", format!("start on p{p}: {e}")))?;
                started.push(ack.run_id);
            }
        }
        // The single-node merge order: (uuid, partition) ascending.
        let mut want = started.clone();
        want.sort_by_key(|id| {
            let r: RunId = id.parse().expect("run id");
            (r.uuid().as_u128(), r.partition().0)
        });
        let leaders: BTreeMap<u16, u64> = published_all(c)
            .into_iter()
            .map(|a| (a.partition, a.leader))
            .collect();
        for node in c.node_ids() {
            let api = c.client(node);
            let (st, v, _) = rest(&api, reqwest::Method::GET, "/v1/runs?limit=1000", None).await;
            let got: Vec<String> = v["runs"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|r| r["run_id"].as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default();
            if st != 200 || got != want {
                return Err(fail(
                    "routing",
                    format!(
                        "node {node} (leaders {leaders:?}): list_runs {st}: got {} runs {got:?}, want {want:?}",
                        got.len()
                    ),
                ));
            }
            // Page by page (limit 5): the concatenation is the same list, no duplicates.
            let mut paged = Vec::new();
            let mut token: Option<String> = None;
            for _ in 0..(want.len() + 2) {
                let path = match &token {
                    Some(t) => format!("/v1/runs?limit=5&page_token={t}"),
                    None => "/v1/runs?limit=5".to_owned(),
                };
                let (st, v, _) = rest(&api, reqwest::Method::GET, &path, None).await;
                if st != 200 {
                    return Err(fail(
                        "routing",
                        format!("node {node}: page {path}: {st} {v}"),
                    ));
                }
                for r in v["runs"].as_array().into_iter().flatten() {
                    paged.push(r["run_id"].as_str().unwrap_or_default().to_owned());
                }
                token = v["next_page_token"].as_str().map(str::to_owned);
                if token.is_none() {
                    break;
                }
            }
            if paged != want {
                return Err(fail(
                    "routing",
                    format!("node {node}: paged listing {paged:?}, want {want:?}"),
                ));
            }
        }
        Ok(())
    })
}

/// Webhook deliveries and schedule triggers whose partitions are led elsewhere create runs.
fn internal_consumers_route(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_everywhere(c).await?;
        let node = c.node_ids()[0];
        let api = c.client(node);
        let leaders: BTreeMap<u16, u64> = published_all(c)
            .into_iter()
            .map(|a| (a.partition, a.leader))
            .collect();
        // Webhook: keyed starts (idempotency from the body); deliveries through `node`.
        let (st, v, _) = rest(
            &api,
            reqwest::Method::POST,
            "/v1/webhooks",
            Some(json!({
                "name": "rt-hook",
                "action": { "kind": "start", "workflow": TEST_WORKFLOW, "input": "body" },
                "verifier": { "kind": "none" },
                "idempotency": "json:/id",
            })),
        )
        .await;
        if st != 201 {
            return Err(fail("routing", format!("create webhook: {st} {v}")));
        }
        let mut elsewhere = 0usize;
        let mut known = std::collections::BTreeSet::new();
        for i in 0..16 {
            let r = api
                .http
                .post(format!("{}/hooks/rt-hook", api.base_url))
                .header("content-type", "application/json")
                .body(format!(r#"{{"id":"delivery-{i}"}}"#))
                .send()
                .await
                .unwrap_or_else(|e| panic!("harness: deliver: {e}"));
            let st = r.status();
            let v: Value = r.json().await.unwrap_or(Value::Null);
            if !st.is_success() {
                return Err(fail(
                    "routing",
                    format!("delivery {i} through node {node}: {st} {v}"),
                ));
            }
            let id = v["run_id"].as_str().unwrap_or_default().to_owned();
            let run: RunId = id
                .parse()
                .map_err(|e| fail("routing", format!("delivery {i}: run id {id:?}: {e} ({v})")))?;
            if leaders.get(&run.partition().0) != Some(&node) {
                elsewhere += 1;
            }
            wait_done(&api, &id).await?;
            known.insert(id);
        }
        if elsewhere == 0 {
            return Err(fail(
                "routing",
                format!("no delivery landed on a partition led elsewhere (leaders {leaders:?})"),
            ));
        }
        // Schedule: an owner partition led elsewhere; a manual trigger through `node`
        // (routed to the owner) and the cron driver's own fires.
        let count = count(c);
        let name = (0..1000)
            .map(|i| format!("rt-cron-{i}"))
            .find(|n| {
                let owner = ostro_types::routing::schedule_owner(n, count);
                leaders.get(&owner.0).is_some_and(|l| *l != node && *l != 0)
            })
            .ok_or_else(|| fail("placement", "no schedule name owned elsewhere"))?;
        let owner = ostro_types::routing::schedule_owner(&name, count);
        let (st, v, _) = rest(
            &api,
            reqwest::Method::POST,
            "/v1/schedules",
            Some(json!({
                "name": name,
                "workflow": TEST_WORKFLOW,
                "cron": "* * * * * *",
                "input": { "cron": true },
            })),
        )
        .await;
        if st != 201 {
            return Err(fail("routing", format!("create schedule: {st} {v}")));
        }
        let (st, v, _) = rest(
            &api,
            reqwest::Method::POST,
            &format!("/v1/schedules/{name}/trigger"),
            Some(json!({ "request_id": "rt-1" })),
        )
        .await;
        let id = v["run_id"].as_str().unwrap_or_default().to_owned();
        if st != 200 || id.is_empty() {
            return Err(fail(
                "routing",
                format!("trigger through node {node}: {st} {v}"),
            ));
        }
        let run: RunId = id
            .parse()
            .map_err(|e| fail("routing", format!("trigger run id {id:?}: {e}")))?;
        if run.partition() != owner {
            return Err(fail(
                "routing",
                format!(
                    "trigger run on partition {}, owner {}",
                    run.partition().0,
                    owner.0
                ),
            ));
        }
        wait_done(&api, &id).await?;
        known.insert(id);
        // A cron fire on the owner partition (a run that is neither a delivery nor the
        // manual trigger), listed through `node`.
        let known = &known;
        let fired = super::placement::support::poll_async(Duration::from_secs(15), || {
            let api = api.clone();
            async move {
                let (st, v, _) =
                    rest(&api, reqwest::Method::GET, "/v1/runs?limit=1000", None).await;
                (st == 200).then_some(())?;
                v["runs"]
                    .as_array()?
                    .iter()
                    .filter_map(|r| r["run_id"].as_str())
                    .filter(|r| !known.contains(*r))
                    .filter_map(|r| r.parse::<RunId>().ok())
                    .any(|r| r.partition() == owner)
                    .then_some(())
            }
        })
        .await;
        if fired.is_none() {
            return Err(fail(
                "routing",
                format!(
                    "schedule {name}: no cron fire listed on owner partition {}",
                    owner.0
                ),
            ));
        }
        Ok(())
    })
}

/// With `route::stale_view` every node believes another replica leads: bounded 503, at
/// most two forwards, no loop.
#[cfg(feature = "failpoints")]
fn hop_limit_no_loop(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_everywhere(c).await?;
        let a = published_all(c)
            .into_iter()
            .find(|a| a.leader != 0 && a.replicas.len() == 3)
            .ok_or_else(|| fail("placement", "no led RF=3 partition"))?;
        let p = a.partition;
        let run = super::placement::support::append_acked(c, p, 1, ACK_T).await?;
        let run_id = run[0].run_id.clone();
        let node = a.leader;
        let before: BTreeMap<u64, u64> = c
            .node_ids()
            .into_iter()
            .map(|n| (n, forwarded(c, n)))
            .collect();
        struct Guard;
        impl Drop for Guard {
            fn drop(&mut self) {
                fail::remove("route::stale_view");
            }
        }
        fail::cfg("route::stale_view", "return")
            .unwrap_or_else(|e| panic!("harness: failpoint: {e}"));
        let guard = Guard;
        let api = c.client(node);
        // clock-exempt: test bound
        let t0 = std::time::Instant::now();
        let (st, v, retry) = rest(
            &api,
            reqwest::Method::GET,
            &format!("/v1/runs/{run_id}"),
            None,
        )
        .await;
        let took = t0.elapsed();
        drop(guard);
        let after: BTreeMap<u64, u64> = c
            .node_ids()
            .into_iter()
            .map(|n| (n, forwarded(c, n)))
            .collect();
        let total: u64 = after.iter().map(|(n, f)| f - before[n]).sum();
        if st != 503 || retry.as_deref() != Some("1") {
            return Err(fail(
                "routing",
                format!("stale views: GET answered {st} (Retry-After {retry:?}): {v}"),
            ));
        }
        if total == 0 || total > 2 {
            return Err(fail(
                "routing",
                format!("stale views: {total} forwards (before {before:?}, after {after:?})"),
            ));
        }
        if took > Duration::from_secs(10) {
            return Err(fail(
                "routing",
                format!("stale views: 503 only after {took:?}"),
            ));
        }
        Ok(())
    })
}
