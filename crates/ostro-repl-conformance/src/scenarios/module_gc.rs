//! Phase 13 `module_gc` scenarios (tier B, 13-15; CLUS-07 / D-P13-20): a 3-node RF=3
//! cluster with `guests/p11-rerun` (`pipeline` v1) and `guests/p11-rerun-v2` (`pipeline`
//! v2). GC rounds run through the node's Phase 13 context
//! (`ostro_server::p13::module_gc::run_with`) with a short grace ([`GRACE`] = 2 s, so the
//! two scans are 1 s apart); the configured `[modules]` defaults (10 min) keep the periodic
//! loop out of the way.
//!
//! - `never_deletes_module_of_live_run`: v1 runs (sleeping, i.e. suspended on a timer, and
//!   short ones) on every partition; v2 activated, v1 retired; GC rounds repeat from
//!   rotating nodes while any v1 run is live: v1 is never deleted in a round that ended
//!   with a live v1 run; once all completed it is deleted (Module + WorkflowVersion gone on
//!   every node), and a Phase 8 blob GC pass then removes the blob on every node. Oracle
//!   `p13_gc::no_module_gone_for_live`.
//! - `protects_retryable_terminal_runs`: a Failed v1 run keeps v1 with protection on; with
//!   `protect_resumable = false` v1 is deleted; a later `retry_run` of the Failed run is
//!   recorded (its answer and the run's end) and must not end live forever.
//! - `retired_refuses_new_starts`: after retire, explicit-version starts and activations of
//!   v1 are 409 `module_retired` from every node.
//! - `gc_survives_leader_changes`: a live v1 run; scan 2 of a round is held while the
//!   leader of the run's partition changes, then goes to the old leader at the old epoch,
//!   which answers `uncovered`: the round is postponed (`postponed` asserted) and the next
//!   round (new leader) reports v1 referenced. v1 is never deleted.
//! - `active_module_not_retirable`: retiring the active version's module is 409
//!   `module_active` from every node.
//!
//! The start-during-GC race needs 13-17's executor `module_gone` path and is 13-18's.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_api::phase13::GcRunReport;
use ostro_cluster::contracts::{ModuleRefsSource, RefsAnswer};
use ostro_server::p13::P13Ctx;
use ostro_server::p13::module_gc::{GcOptions, ModuleGc, NodeOpsRefs, NodeRefs, run_with};
use ostro_server::services::blob_gc::BlobGc;
use ostro_types::{Epoch, PartitionCount, PartitionId, RunId};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::sync::{Mutex, oneshot};
use tokio::time::Instant;

use super::placement::support::fail;
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::{InProcCluster, TERMINAL, guest_wasm};
use crate::harness::{ClusterHarness, ClusterSpec, HarnessError};
use crate::oracle::p13_gc::{RetireEvent, RunResult, no_module_gone_for_live};

/// The GC grace of every scenario round (scans 1 s apart).
pub const GRACE: Duration = Duration::from_secs(2);
/// The workflow of both modules.
pub(super) const WF: &str = "pipeline";
/// Bound on one REST answer with retries, a run's end, a leadership change.
pub(super) const OP_T: Duration = Duration::from_secs(60);
/// Poll step.
pub(super) const STEP: Duration = Duration::from_millis(100);
const ORACLE: &str = "p13_gc";

/// Every `module_gc` scenario.
pub fn all() -> Vec<Scenario> {
    let s =
        |name, run: for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>| Scenario {
            name,
            group: "module_gc",
            tier: Tier::B,
            deadline: Duration::from_secs(300),
            spec: rf3,
            run,
        };
    vec![
        s(
            "never_deletes_module_of_live_run",
            never_deletes_module_of_live_run,
        ),
        s(
            "protects_retryable_terminal_runs",
            protects_retryable_terminal_runs,
        ),
        s("retired_refuses_new_starts", retired_refuses_new_starts),
        s("gc_survives_leader_changes", gc_survives_leader_changes),
        s("active_module_not_retirable", active_module_not_retirable),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 1315,
        ..ClusterSpec::default()
    }
}

// ------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------

pub(super) fn hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

fn harness_err<T>(what: &str, r: Result<T, HarnessError>) -> T {
    r.unwrap_or_else(|e| panic!("harness: {what}: {e}"))
}

pub(super) fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

fn running(c: &InProcCluster) -> Vec<u64> {
    c.node_ids()
        .into_iter()
        .filter(|n| c.handle(*n).is_some())
        .collect()
}

fn ctx(c: &InProcCluster, node: u64) -> Arc<P13Ctx> {
    Arc::clone(
        c.handle(node)
            .and_then(|h| h.p13())
            .unwrap_or_else(|| panic!("node {node}: no Phase 13 context")),
    )
}

pub(super) async fn send(rb: reqwest::RequestBuilder, what: &str) -> (StatusCode, Value) {
    let r = rb
        .send()
        .await
        .unwrap_or_else(|e| panic!("harness: {what}: {e}"));
    let st = r.status();
    let b = r.bytes().await.unwrap_or_default();
    (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// `PUT /v1/modules/{sha}` of `guest` on `node`.
pub(super) async fn deploy(c: &InProcCluster, node: u64, guest: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let wasm = harness_err("guest build", guest_wasm(guest));
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let cl = c.client(node);
    let (st, v) = send(
        cl.http
            .put(format!("{}/v1/modules/{}", cl.base_url, hex(&sha)))
            .header("content-type", "application/wasm")
            .body(wasm),
        "PUT module",
    )
    .await;
    assert!(st.is_success(), "harness: deploy {guest}: {st} {v}");
    sha
}

pub(super) async fn delete_module(
    c: &InProcCluster,
    node: u64,
    sha: &[u8; 32],
) -> (StatusCode, Value) {
    let cl = c.client(node);
    send(
        cl.http
            .delete(format!("{}/v1/modules/{}", cl.base_url, hex(sha))),
        "DELETE module",
    )
    .await
}

/// Retire `sha` on `node`, waiting out the feature gate (the cluster auto-finalizes level 2
/// once every member advertised its version).
pub(super) async fn retire(c: &InProcCluster, node: u64, sha: &[u8; 32]) -> (StatusCode, Value) {
    let deadline = Instant::now() + OP_T;
    loop {
        let (st, v) = delete_module(c, node, sha).await;
        if reason(&v) != "feature_not_enabled" || Instant::now() >= deadline {
            return (st, v);
        }
        tokio::time::sleep(STEP).await;
    }
}

pub(super) async fn activate(c: &InProcCluster, node: u64, version: u32) -> (StatusCode, Value) {
    let cl = c.client(node);
    send(
        cl.http
            .put(format!("{}/v1/workflows/{WF}/active", cl.base_url))
            .json(&json!({ "version": version })),
        "PUT active",
    )
    .await
}

/// Every running node applied everything committed so far (ReadIndex barrier on each).
pub(super) async fn barrier_all(c: &InProcCluster) {
    for n in running(c) {
        if let Some(h) = c.handle(n) {
            harness_err(
                "meta barrier",
                h.meta()
                    .read_index_barrier()
                    .await
                    .map_err(|e| HarnessError::Other(format!("node {n}: {e}"))),
            );
        }
    }
}

/// Start `pipeline` (`version`, `input`) on partition `p` through its leader (keyed
/// routing); retries across leader moves. Returns the run id.
pub(super) async fn start_on(
    c: &InProcCluster,
    p: u16,
    version: Option<u32>,
    input: Value,
) -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    let count = PartitionCount::new(c.spec().partitions).expect("partitions");
    let key = loop {
        let k = format!("gc-{}-{}", c.spec().seed, N.fetch_add(1, Ordering::Relaxed));
        if ostro_types::routing::partition_for_key(WF, k.as_bytes(), count).0 == p {
            break k;
        }
    };
    let mut body = json!({ "input": input, "idempotency_key": key });
    if let Some(v) = version {
        body["version"] = json!(v);
    }
    let deadline = Instant::now() + OP_T;
    loop {
        let leader = c.assignment(p).await.leader;
        if leader != 0 && c.handle(leader).is_some() {
            let cl = c.client(leader);
            let (st, v) = send(
                cl.http
                    .post(format!("{}/v1/workflows/{WF}/runs", cl.base_url))
                    .json(&body),
                "POST run",
            )
            .await;
            if st.is_success() {
                return v["run_id"].as_str().expect("run_id").to_owned();
            }
            assert!(
                Instant::now() < deadline,
                "harness: start on partition {p}: {st} {v}"
            );
        }
        tokio::time::sleep(STEP).await;
    }
}

/// `GET /v1/runs/{id}` from the leader of the run's partition (retried).
pub(super) async fn get_run(c: &InProcCluster, run_id: &str) -> Value {
    let id: RunId = run_id.parse().expect("run id");
    let deadline = Instant::now() + OP_T;
    loop {
        let leader = c.assignment(id.partition().0).await.leader;
        if leader != 0 && c.handle(leader).is_some() {
            let (st, v) = harness_err(
                "GET run",
                c.client(leader)
                    .get_json(&format!("/v1/runs/{run_id}"))
                    .await,
            );
            if st.is_success() {
                return v;
            }
        }
        assert!(Instant::now() < deadline, "harness: GET run {run_id}");
        tokio::time::sleep(STEP).await;
    }
}

fn is_terminal(run: &Value) -> bool {
    run["status"]
        .as_str()
        .is_some_and(|s| TERMINAL.contains(&s))
}

/// The runs of `ids` that are not terminal.
async fn live_runs(c: &InProcCluster, ids: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for id in ids {
        if !is_terminal(&get_run(c, id).await) {
            out.push(id.clone());
        }
    }
    out
}

pub(super) async fn wait_terminal(
    c: &InProcCluster,
    run_id: &str,
    limit: Duration,
) -> Option<Value> {
    let deadline = Instant::now() + limit;
    loop {
        let v = get_run(c, run_id).await;
        if is_terminal(&v) {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(STEP).await;
    }
}

pub(super) async fn gc_round(c: &InProcCluster, node: u64, protect_resumable: bool) -> GcRunReport {
    let ctx = ctx(c, node);
    harness_err(
        "gc round",
        run_with(
            &ctx,
            GcOptions {
                grace: GRACE,
                protect_resumable,
            },
        )
        .await
        .map_err(|e| HarnessError::Other(format!("node {node}: {e}"))),
    )
}

/// Rounds from `node` until `sha` is a candidate (its grace has passed).
pub(super) async fn round_as_candidate(
    c: &InProcCluster,
    node: u64,
    sha: &[u8; 32],
    protect_resumable: bool,
) -> GcRunReport {
    let deadline = Instant::now() + OP_T;
    loop {
        let rep = gc_round(c, node, protect_resumable).await;
        if rep.candidates.contains(&hex(sha)) {
            return rep;
        }
        assert!(
            Instant::now() < deadline,
            "harness: {} never a candidate",
            hex(sha)
        );
        tokio::time::sleep(STEP).await;
    }
}

/// Whether any running node still has module `sha` or `pipeline@version` (after a barrier).
pub(super) async fn module_present(c: &InProcCluster, sha: &[u8; 32], version: u32) -> Vec<u64> {
    barrier_all(c).await;
    running(c)
        .into_iter()
        .filter(|n| {
            c.handle(*n).is_some_and(|h| {
                h.meta()
                    .view()
                    .read(|s| s.module(sha).is_some() || s.workflow(WF, version).is_some())
            })
        })
        .collect()
}

/// Deploy v1 and v2 on node 1 (v1 is active after its first upload).
pub(super) async fn deploy_both(c: &InProcCluster) -> ([u8; 32], [u8; 32]) {
    let v1 = deploy(c, 1, "p11-rerun").await;
    let v2 = deploy(c, 1, "p11-rerun-v2").await;
    (v1, v2)
}

async fn activate_v2_retire_v1(c: &InProcCluster, v1: &[u8; 32]) -> ScenarioResult {
    let (st, v) = activate(c, 1, 2).await;
    if !st.is_success() {
        return Err(fail(ORACLE, format!("activate v2: {st} {v}")));
    }
    let (st, v) = retire(c, 1, v1).await;
    if st != StatusCode::OK {
        return Err(fail(ORACLE, format!("retire v1: {st} {v}")));
    }
    Ok(())
}

/// Every run's final answer. `GET /v1/runs/{id}` carries the failure detail in `failure`
/// (a JSON string), not in `error`, which `RunResult::from_json` reads; fold it in so the
/// oracle sees real failure details (13-18 note).
async fn results(c: &InProcCluster, ids: &[String]) -> Vec<RunResult> {
    let mut out = Vec::new();
    for id in ids {
        let v = get_run(c, id).await;
        let mut r = RunResult::from_json(&v);
        if r.error.is_none() {
            r.error = match &v["failure"] {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                other => Some(other.to_string()),
            };
        }
        out.push(r);
    }
    out
}

fn oracle(events: &[RetireEvent], results: &[RunResult]) -> ScenarioResult {
    no_module_gone_for_live(events, results).into_result()
}

// ------------------------------------------------------------------------------------
// Scenarios
// ------------------------------------------------------------------------------------

fn never_deletes_module_of_live_run(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (v1, _v2) = deploy_both(c).await;
        let mut ids = Vec::new();
        for p in 0..c.spec().partitions {
            // Suspended on a durable timer for a while, then completes.
            ids.push(
                start_on(
                    c,
                    p,
                    Some(1),
                    json!({ "sleep_between_ms": 5_000 + 1_500 * u64::from(p) }),
                )
                .await,
            );
            // Short runs (live at retire time or already done).
            ids.push(start_on(c, p, Some(1), json!({ "sleep_between_ms": 500 })).await);
        }
        let ev = activate_v2_retire_v1_listing(c, &v1, &ids).await?;
        println!(
            "module_gc: retired v1 with live runs {:?}",
            ev.non_terminal_runs
        );
        let events = vec![ev];
        let nodes = running(c);
        let mut round = 0usize;
        let deadline = Instant::now() + Duration::from_secs(120);
        let deleted = loop {
            let node = nodes[round % nodes.len()];
            round += 1;
            let rep = gc_round(c, node, true).await;
            let live = live_runs(c, &ids).await;
            let gone = rep.deleted.contains(&hex(&v1));
            if gone && !live.is_empty() {
                return Err(fail(
                    ORACLE,
                    format!(
                        "round {round} on node {node} deleted v1 while runs {live:?} were live: {rep:?}"
                    ),
                ));
            }
            if !live.is_empty()
                && rep.candidates.contains(&hex(&v1))
                && !rep.postponed
                && !rep.referenced.contains(&hex(&v1))
            {
                return Err(fail(
                    ORACLE,
                    format!(
                        "round {round}: v1 not reported referenced while {live:?} live: {rep:?}"
                    ),
                ));
            }
            if gone {
                break round;
            }
            tokio::time::sleep(STEP).await;
            if Instant::now() >= deadline {
                return Err(fail(
                    ORACLE,
                    format!("v1 not deleted within 120 s after its runs ended (last {rep:?})"),
                ));
            }
        };
        println!("module_gc: v1 deleted in round {deleted}");
        let present = module_present(c, &v1, 1).await;
        if !present.is_empty() {
            return Err(fail(
                ORACLE,
                format!("v1 records still on nodes {present:?}"),
            ));
        }
        // Every v1 run completed; none failed with module_gone.
        let res = results(c, &ids).await;
        if let Some(r) = res.iter().find(|r| r.status != "completed") {
            return Err(fail(ORACLE, format!("v1 run ended {r:?}")));
        }
        oracle(&events, &res)?;
        // Phase 8 blob GC sweeps the blob on every node now that no Module record names it
        // (blob path: `<data>/blobs/<hex[0:2]>/<hex>`; at least one node must have held it,
        // so a wrong path cannot pass vacuously).
        let hx = hex(&v1);
        let mut held = 0;
        for n in running(c) {
            let h = c.handle(n).expect("running");
            let node = h.node();
            let path = h.data_dir().join("blobs").join(&hx[..2]).join(&hx);
            if path.is_file() {
                held += 1;
            }
            let blobs = Arc::clone(node.handle(PartitionId(0)).expect("partition 0").blobs());
            let gc = harness_err(
                "blob gc",
                BlobGc::new(
                    blobs,
                    h.data_dir().to_path_buf(),
                    Duration::ZERO,
                    Arc::clone(h.clock()),
                    None,
                )
                .map_err(HarnessError::Other),
            );
            let rep = harness_err(
                "blob gc pass",
                gc.pass(node, h.meta().view())
                    .await
                    .map_err(HarnessError::Other),
            );
            if path.is_file() {
                return Err(fail(
                    ORACLE,
                    format!("node {n}: v1 blob survived a blob GC pass: {rep:?}"),
                ));
            }
        }
        if held == 0 {
            return Err(fail(ORACLE, format!("no node held the v1 blob at {hx}")));
        }
        Ok(())
    })
}

/// Activate v2, list v1's live runs among `ids`, retire v1.
async fn activate_v2_retire_v1_listing(
    c: &InProcCluster,
    v1: &[u8; 32],
    ids: &[String],
) -> Result<RetireEvent, ScenarioFailure> {
    let (st, v) = activate(c, 1, 2).await;
    if !st.is_success() {
        return Err(fail(ORACLE, format!("activate v2: {st} {v}")));
    }
    let live = live_runs(c, ids).await;
    let (st, v) = retire(c, 1, v1).await;
    if st != StatusCode::OK {
        return Err(fail(ORACLE, format!("retire v1: {st} {v}")));
    }
    Ok(RetireEvent {
        module_sha: hex(v1),
        non_terminal_runs: live,
    })
}

fn protects_retryable_terminal_runs(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (v1, _v2) = deploy_both(c).await;
        let failed = start_on(c, 1, Some(1), json!({ "fail_at": 3 })).await;
        let rec = wait_terminal(c, &failed, OP_T)
            .await
            .unwrap_or_else(|| panic!("harness: run {failed} did not end"));
        if rec["status"] != "failed" {
            return Err(fail(ORACLE, format!("fail_at run ended {rec}")));
        }
        let ev = activate_v2_retire_v1_listing(c, &v1, std::slice::from_ref(&failed)).await?;
        // Protection on (the default): the Failed run keeps v1, on every node's round.
        for node in running(c) {
            let rep = round_as_candidate(c, node, &v1, true).await;
            if rep.deleted.contains(&hex(&v1)) || !rep.referenced.contains(&hex(&v1)) {
                return Err(fail(
                    ORACLE,
                    format!("node {node}: a Failed (retryable) run did not keep v1: {rep:?}"),
                ));
            }
        }
        // Protection off: v1 goes.
        let rep = round_as_candidate(c, 1, &v1, false).await;
        if !rep.deleted.contains(&hex(&v1)) {
            return Err(fail(
                ORACLE,
                format!("protect_resumable=false kept v1: {rep:?}"),
            ));
        }
        let present = module_present(c, &v1, 1).await;
        if !present.is_empty() {
            return Err(fail(
                ORACLE,
                format!("v1 records still on nodes {present:?}"),
            ));
        }
        // A later retry of the Failed run: recorded, and it must not leave a run live
        // forever.
        // Until 13-12 routes the restart, a restart submitted to a partition the serving
        // node does not lead answers 503 `not_leader`; the new run's partition is random,
        // so retry a bounded number of times.
        let id: RunId = failed.parse().expect("run id");
        let mut attempt = 0;
        let (st, v) = loop {
            attempt += 1;
            let leader = c.assignment(id.partition().0).await.leader;
            let cl = c.client(leader);
            let (st, v) = send(
                cl.http
                    .post(format!("{}/v1/runs/{failed}/retry", cl.base_url))
                    .json(&json!({})),
                "POST retry",
            )
            .await;
            if !(st == StatusCode::SERVICE_UNAVAILABLE && reason(&v) == "not_leader")
                || attempt >= 40
            {
                break (st, v);
            }
            tokio::time::sleep(STEP).await;
        };
        println!("module_gc: retry of {failed} after v1 GC (attempt {attempt}): {st} {v}");
        let mut runs = vec![failed.clone()];
        if st.is_success() {
            let target = v["run_id"].as_str().unwrap_or(&failed).to_owned();
            match wait_terminal(c, &target, OP_T).await {
                Some(end) => println!("module_gc: retried run {target} ended {end}"),
                None => {
                    return Err(fail(
                        ORACLE,
                        format!("retried run {target} still live {OP_T:?} after v1 was deleted"),
                    ));
                }
            }
            if target != failed {
                runs.push(target);
            }
        } else if st.is_server_error() && st != StatusCode::SERVICE_UNAVAILABLE {
            return Err(fail(ORACLE, format!("retry after GC: {st} {v}")));
        }
        oracle(&[ev], &results(c, &runs).await)
    })
}

fn retired_refuses_new_starts(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (v1, _v2) = deploy_both(c).await;
        activate_v2_retire_v1(c, &v1).await?;
        barrier_all(c).await;
        for node in running(c) {
            let cl = c.client(node);
            let (st, v) = send(
                cl.http
                    .post(format!("{}/v1/workflows/{WF}/runs", cl.base_url))
                    .json(&json!({ "input": {}, "version": 1 })),
                "POST run",
            )
            .await;
            if (st, reason(&v)) != (StatusCode::CONFLICT, "module_retired") {
                return Err(fail(
                    ORACLE,
                    format!("node {node}: start on retired v1 answered {st} {v}"),
                ));
            }
            let (st, v) = activate(c, node, 1).await;
            if (st, reason(&v)) != (StatusCode::CONFLICT, "module_retired") {
                return Err(fail(
                    ORACLE,
                    format!("node {node}: activation of retired v1 answered {st} {v}"),
                ));
            }
        }
        // The active version (v2) still starts and completes.
        let run = start_on(c, 0, None, json!({})).await;
        match wait_terminal(c, &run, OP_T).await {
            Some(v) if v["status"] == "completed" && v["version"] == 2 => Ok(()),
            other => Err(fail(ORACLE, format!("v2 run after retire: {other:?}"))),
        }
    })
}

/// A [`NodeRefs`] over the real `NodeOps` client that holds the first call of scan 2 until
/// released.
struct GatedRefs {
    inner: NodeOpsRefs,
    calls: AtomicUsize,
    gate_at: usize,
    reached: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl NodeRefs for GatedRefs {
    fn module_refs(
        &self,
        node: u64,
        shas: Vec<[u8; 32]>,
        partitions: Vec<(PartitionId, Epoch)>,
        protect_resumable: bool,
    ) -> BoxFuture<'_, Result<RefsAnswer, String>> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == self.gate_at {
                if let Some(tx) = self.reached.lock().await.take() {
                    let _ = tx.send(());
                }
                if let Some(rx) = self.release.lock().await.take() {
                    let _ = rx.await;
                }
            }
            self.inner
                .module_refs(node, shas, partitions, protect_resumable)
                .await
        })
    }
}

fn gc_survives_leader_changes(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (v1, _v2) = deploy_both(c).await;
        let p = 2u16;
        let live = start_on(c, p, Some(1), json!({ "sleep_between_ms": 600_000 })).await;
        let ev = activate_v2_retire_v1_listing(c, &v1, std::slice::from_ref(&live)).await?;
        // Wait out the grace so the gated round has v1 as a candidate.
        round_as_candidate(c, 1, &v1, true).await;

        let before = c.assignment(p).await;
        let old_leader = before.leader;
        let gc_node = *running(c)
            .iter()
            .find(|n| **n != old_leader)
            .expect("another node");
        let gctx = ctx(c, gc_node);
        let leaders: BTreeSet<u64> = gctx
            .meta
            .view()
            .read(|s| s.partitions().map(|a| a.value.leader).collect());
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let refs = GatedRefs {
            inner: NodeOpsRefs::new(
                gctx.node_id(),
                Arc::clone(&gctx.module_refs) as Arc<dyn ModuleRefsSource>,
                Arc::clone(gctx.cluster.transport()),
            ),
            calls: AtomicUsize::new(0),
            // Scan 1 asks each distinct leader once; the next call opens scan 2.
            gate_at: leaders.len(),
            reached: Mutex::new(Some(reached_tx)),
            release: Mutex::new(Some(release_rx)),
        };
        let gc = ModuleGc::new(
            Arc::clone(&gctx.meta),
            Arc::new(refs),
            Arc::clone(&gctx.clock),
        );
        let round = tokio::spawn(async move {
            gc.round(GcOptions {
                grace: GRACE,
                protect_resumable: true,
            })
            .await
        });
        // Scan 2 has read its (now stale) assignment view and is held: move partition p.
        if tokio::time::timeout(OP_T, reached_rx).await.is_err() {
            return Err(fail(ORACLE, "the gated round never reached scan 2"));
        }
        c.isolate(old_leader);
        let deadline = Instant::now() + OP_T;
        loop {
            let a = c.assignment(p).await;
            if a.leader != 0 && a.leader != old_leader && a.epoch > before.epoch {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "harness: partition {p} never moved"
            );
            tokio::time::sleep(STEP).await;
        }
        c.heal(old_leader);
        // The old leader stepped down at the old epoch.
        let deadline = Instant::now() + OP_T;
        while c
            .handle(old_leader)
            .expect("running")
            .node()
            .role(PartitionId(p))
            .is_some_and(|r| r.borrow().leading_epoch() == Some(before.epoch))
        {
            assert!(
                Instant::now() < deadline,
                "harness: old leader kept partition {p}"
            );
            tokio::time::sleep(STEP).await;
        }
        let _ = release_tx.send(());
        let rep = match round.await {
            Ok(Ok(rep)) => rep,
            Ok(Err(e)) => panic!("harness: gated round: {e}"),
            Err(e) => panic!("harness: gated round task: {e}"),
        };
        println!("module_gc: gated round across the leader change: {rep:?}");
        if !rep.postponed || rep.deleted.contains(&hex(&v1)) {
            return Err(fail(
                ORACLE,
                format!("a leader change between the scans did not postpone the round: {rep:?}"),
            ));
        }
        // The next rounds ask the new leader: v1 stays referenced.
        let deadline = Instant::now() + OP_T;
        let rep = loop {
            let rep = gc_round(c, gc_node, true).await;
            if !rep.postponed || Instant::now() >= deadline {
                break rep;
            }
            tokio::time::sleep(STEP).await;
        };
        if rep.postponed || !rep.referenced.contains(&hex(&v1)) || rep.deleted.contains(&hex(&v1)) {
            return Err(fail(ORACLE, format!("after the leader change: {rep:?}")));
        }
        if module_present(c, &v1, 1).await.len() != running(c).len() {
            return Err(fail(ORACLE, "v1 records missing after the leader change"));
        }
        let res = results(c, std::slice::from_ref(&live)).await;
        if res.iter().any(|r| TERMINAL.contains(&r.status.as_str())) {
            return Err(fail(ORACLE, format!("the live v1 run ended: {res:?}")));
        }
        oracle(&[ev], &res)
    })
}

fn active_module_not_retirable(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let v1 = deploy(c, 1, "p11-rerun").await;
        for node in running(c) {
            let (st, v) = retire(c, node, &v1).await;
            if (st, reason(&v)) != (StatusCode::CONFLICT, "module_active")
                || v["error"]["details"]["workflows"] != json!([{"name": WF, "version": 1}])
            {
                return Err(fail(
                    ORACLE,
                    format!("node {node}: retire of the active module answered {st} {v}"),
                ));
            }
        }
        if module_present(c, &v1, 1).await.len() != running(c).len() {
            return Err(fail(ORACLE, "the active module lost records"));
        }
        Ok(())
    })
}
