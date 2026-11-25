//! Phase 13 `migrate` scenarios (tier B, 13-17; CLUS-07 / D-P13-21): `POST /v1/runs/migrate`
//! on a 3-node RF=3 cluster with `guests/p10-replay-v{1,2,3}` (`replay_demo`: v1 runs steps
//! `a`, `b`, then sleeps; v2 swaps the steps, so a v1 journal past the first step diverges;
//! v3 swaps them behind `patched("reorder")`, so v1 journals replay clean).
//!
//! Every scenario ends with the upgrade oracle (`p13_upgrade::no_gated_before_finalize_from`:
//! no `RepinRun` on disk before the meta entry that finalized level 2) and the Phase 12
//! replica oracle (`replica::identical_below_hw`: the repins are byte-identical on every
//! in-sync replica), and checks the repinned pins in every replica's state.
//!
//! | scenario | claim |
//! |---|---|
//! | `passing_runs_repinned_cluster_wide` | issued at a node that leads no partition (2 partitions, 3 nodes): every passing run on every partition is repinned; two identical dry runs at different nodes answer the same report |
//! | `diverging_runs_never_repinned` | mixed runs (past the first step: diverge on v2; waiting for a start delay: replay clean) → only the passing ones are repinned; the failed ones keep v1 and complete on v1 |
//! | `concurrent_growth_seq_mismatch` | a run that receives a signal between its replay and the submit (the `before_submit` test hook) → `seq_mismatch`, pin unchanged; a second migrate repins it. Under `migrate::mutant::skip_seq_check` (failpoints build) this scenario fails (`tests/migrate.rs`) |
//! | `busy_runs_reported` | `[engine] hot_wait` 60 s (nodes restarted with it), runs sleeping 20 s: hot-parked in the engine, they are `busy` and keep v1; they finish in process and complete on v1, and a later migrate finds nothing to move |
//!
//! [`refused_before_finalize`] (`migrate_refused_before_finalize`) owns its cluster: three
//! nodes started at the emulated level 1, two upgraded, one left at level 1 → a submitting
//! migrate is 409 `feature_not_enabled` at every node and nothing reaches a partition log;
//! the dry run works; after the last upgrade the cluster finalizes and the migrate repins.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_server::control::migrate::{BeforeSubmitHook, set_before_submit_hook};
use ostro_server::p13::P13Ctx;
use ostro_types::{PartitionCount, PartitionId, RunId};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::Instant;

use super::placement::support::{fail, settle};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::{InProcCluster, guest_wasm};
use crate::harness::{ApiClient, ClusterHarness, ClusterSpec, DataDirView, HarnessError};
use crate::oracle::disk::scan_partition;
use crate::oracle::p13_upgrade::{no_gated_before_finalize_from, no_gated_partition_commands};
use crate::oracle::replica;

/// The workflow of every module.
const WF: &str = "replay_demo";
/// Bound on one REST answer with retries, a run state, a leadership change.
const OP_T: Duration = Duration::from_secs(90);
/// Poll step.
const STEP: Duration = Duration::from_millis(100);
const ORACLE: &str = "migrate";
/// `[engine] hot_wait` of `busy_runs_reported`.
const LONG_HOT_WAIT: Duration = Duration::from_secs(60);
/// The sleep of a hot-parked run (below [`LONG_HOT_WAIT`], so the engine parks it).
const PARKED_SLEEP_S: u64 = 20;

/// Every `migrate` scenario.
pub fn all() -> Vec<Scenario> {
    let s = |name,
             spec: fn() -> ClusterSpec,
             run: for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>| {
        Scenario {
            name,
            group: "migrate",
            tier: Tier::B,
            deadline: Duration::from_secs(420),
            spec,
            run,
        }
    };
    vec![
        s(
            "passing_runs_repinned_cluster_wide",
            rf3_two_partitions,
            passing_runs_repinned_cluster_wide,
        ),
        s(
            "diverging_runs_never_repinned",
            rf3,
            diverging_runs_never_repinned,
        ),
        s(
            "concurrent_growth_seq_mismatch",
            rf3,
            concurrent_growth_seq_mismatch,
        ),
        s("busy_runs_reported", rf3, busy_runs_reported),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 1317,
        ..ClusterSpec::default()
    }
}

/// Two partitions on three nodes: at least one node leads none.
fn rf3_two_partitions() -> ClusterSpec {
    ClusterSpec {
        seed: 13170,
        partitions: 2,
        ..ClusterSpec::default()
    }
}

// ------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------

fn hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

fn running(c: &InProcCluster) -> Vec<u64> {
    c.node_ids()
        .into_iter()
        .filter(|n| c.handle(*n).is_some())
        .collect()
}

async fn send(rb: reqwest::RequestBuilder, what: &str) -> (StatusCode, Value) {
    let r = rb
        .send()
        .await
        .unwrap_or_else(|e| panic!("harness: {what}: {e}"));
    let st = r.status();
    let b = r.bytes().await.unwrap_or_default();
    (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// `PUT /v1/modules/{sha}` of `guest` through `node`.
async fn deploy(c: &InProcCluster, node: u64, guest: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let wasm = guest_wasm(guest).unwrap_or_else(|e| panic!("harness: guest {guest}: {e}"));
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

/// Deploy v1 (active), v2 and v3; every running node then sees v3.
async fn deploy_all(c: &InProcCluster) -> ([u8; 32], [u8; 32], [u8; 32]) {
    let v1 = deploy(c, 1, "p10-replay-v1").await;
    let v2 = deploy(c, 1, "p10-replay-v2").await;
    let v3 = deploy(c, 1, "p10-replay-v3").await;
    barrier_all(c).await;
    (v1, v2, v3)
}

/// Every running node applied everything committed so far (ReadIndex barrier on each).
async fn barrier_all(c: &InProcCluster) {
    for n in running(c) {
        if let Some(h) = c.handle(n) {
            h.meta()
                .read_index_barrier()
                .await
                .unwrap_or_else(|e| panic!("harness: node {n}: meta barrier: {e}"));
        }
    }
}

/// Start `replay_demo` with `body` (plus a key hashing to partition `p`) through `node`
/// (routed); retries 503s. Returns the run id.
async fn start_on(c: &InProcCluster, node: u64, p: u16, mut body: Value) -> RunId {
    static N: AtomicUsize = AtomicUsize::new(0);
    let count = PartitionCount::new(c.spec().partitions).expect("partitions");
    let key = loop {
        let k = format!(
            "mig-{}-{}",
            c.spec().seed,
            N.fetch_add(1, Ordering::Relaxed)
        );
        if ostro_types::routing::partition_for_key(WF, k.as_bytes(), count).0 == p {
            break k;
        }
    };
    body["idempotency_key"] = json!(key);
    let deadline = Instant::now() + OP_T;
    loop {
        let cl = c.client(node);
        let (st, v) = send(
            cl.http
                .post(format!("{}/v1/workflows/{WF}/runs", cl.base_url))
                .json(&body),
            "POST run",
        )
        .await;
        if st.is_success() {
            return v["run_id"]
                .as_str()
                .expect("run_id")
                .parse()
                .expect("run id");
        }
        assert!(
            Instant::now() < deadline,
            "harness: start on partition {p}: {st} {v}"
        );
        tokio::time::sleep(STEP).await;
    }
}

/// The record of `run` on `node`'s partition state (leader or follower).
fn record_on(c: &InProcCluster, node: u64, run: &RunId) -> Option<RunRecord> {
    c.handle(node)
        .and_then(|h| h.node().handle(run.partition()))
        .and_then(|ph| ph.read_run(run).ok().flatten())
}

/// The record on the partition's published leader.
async fn leader_record(c: &InProcCluster, run: &RunId) -> Option<RunRecord> {
    let leader = c.assignment(run.partition().0).await.leader;
    record_on(c, leader, run)
}

/// Wait until the leader's record satisfies `f`.
async fn wait_record(
    c: &InProcCluster,
    run: &RunId,
    what: &str,
    f: impl Fn(&RunRecord) -> bool,
) -> Result<RunRecord, ScenarioFailure> {
    let deadline = Instant::now() + OP_T;
    loop {
        if let Some(r) = leader_record(c, run).await
            && f(&r)
        {
            return Ok(r);
        }
        if Instant::now() >= deadline {
            let r = leader_record(c, run).await;
            return Err(fail(
                "harness",
                format!(
                    "run {run}: {what} not reached within {OP_T:?}: {:?}",
                    r.map(|r| (r.status, r.next_seq))
                ),
            ));
        }
        tokio::time::sleep(STEP).await;
    }
}

/// `next_seq` once the run journaled RunStarted, step a, step b and its timer.
const SLEEPING_SEQ: u64 = 4;

/// Sleeping runs, one per partition in `parts`, suspended on their timer.
async fn sleeping_runs(
    c: &InProcCluster,
    parts: impl IntoIterator<Item = u16>,
    sleep_s: u64,
) -> Result<Vec<RunId>, ScenarioFailure> {
    let mut runs = Vec::new();
    for p in parts {
        runs.push(start_on(c, 1, p, json!({"input": {"sleep_s": sleep_s}})).await);
    }
    for r in &runs {
        wait_record(c, r, "suspended on its timer", |rec| {
            rec.status == RunStatus::Scheduled && rec.next_seq >= SLEEPING_SEQ
        })
        .await?;
    }
    Ok(runs)
}

fn body(from: u32, to: u32, dry_run: bool) -> Value {
    json!({"workflow": WF, "from": {"version": from}, "to_version": to, "dry_run": dry_run})
}

/// `POST /v1/runs/migrate` through `node`.
async fn migrate(c: &InProcCluster, node: u64, b: &Value) -> (StatusCode, Value) {
    let cl = c.client(node);
    send(
        cl.http
            .post(format!("{}/v1/runs/migrate", cl.base_url))
            .json(b),
        "POST migrate",
    )
    .await
}

/// A migrate that must succeed: retries `feature_not_enabled` (the cluster finalizes level
/// 2 once every member advertised its version) and router 503s.
async fn migrate_ok(c: &InProcCluster, node: u64, b: &Value) -> Result<Value, ScenarioFailure> {
    let deadline = Instant::now() + OP_T;
    loop {
        let (st, v) = migrate(c, node, b).await;
        if st == StatusCode::OK {
            return Ok(v);
        }
        let retry = reason(&v) == "feature_not_enabled" || st == StatusCode::SERVICE_UNAVAILABLE;
        if st == StatusCode::SERVICE_UNAVAILABLE {
            eprintln!("migrate: {b} via node {node}: retrying after {st} {v}");
        }
        if !retry || Instant::now() >= deadline {
            return Err(fail(
                ORACLE,
                format!("migrate {b} via node {node}: {st} {v}"),
            ));
        }
        tokio::time::sleep(STEP).await;
    }
}

fn ids(v: &Value, field: &str) -> BTreeSet<String> {
    v[field]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| {
                    x.as_str()
                        .map(str::to_owned)
                        .or_else(|| x["run_id"].as_str().map(str::to_owned))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn set(runs: &[RunId]) -> BTreeSet<String> {
    runs.iter().map(RunId::to_string).collect()
}

fn expect_ids(rep: &Value, field: &str, want: &[RunId]) -> ScenarioResult {
    let got = ids(rep, field);
    if got == set(want) {
        Ok(())
    } else {
        Err(fail(
            ORACLE,
            format!("{field}: want {:?}, got {got:?} (report {rep})", set(want)),
        ))
    }
}

/// Every running replica of `run`'s partition shows the pin `sha` (followers apply the
/// same command: the repin is identical everywhere).
async fn pinned_everywhere(c: &InProcCluster, run: &RunId, sha: [u8; 32]) -> ScenarioResult {
    let replicas = c.assignment(run.partition().0).await.replicas;
    let deadline = Instant::now() + OP_T;
    loop {
        let pins: Vec<(u64, Option<[u8; 32]>)> = replicas
            .iter()
            .filter(|n| c.handle(**n).is_some())
            .map(|n| (*n, record_on(c, *n, run).and_then(|r| r.module_sha)))
            .collect();
        if !pins.is_empty() && pins.iter().all(|(_, s)| *s == Some(sha)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(fail(
                "replica",
                format!(
                    "run {run}: pins on the replicas {:?}, want {}",
                    pins.iter()
                        .map(|(n, s)| (*n, s.map(|s| hex(&s)[..8].to_owned())))
                        .collect::<Vec<_>>(),
                    &hex(&sha)[..8]
                ),
            ));
        }
        tokio::time::sleep(STEP).await;
    }
}

/// The end-of-scenario oracles: no `RepinRun` before finalization, byte-identical replicas
/// below HW.
async fn oracles(c: &InProcCluster) -> ScenarioResult {
    let Some(n) = running(c).first().copied() else {
        return Err(fail("harness", "no running node"));
    };
    let meta = c.meta_log(n).await;
    let r = no_gated_before_finalize_from(1, &meta, &scans_all(c));
    if !r.is_ok() {
        return Err(fail("p13_upgrade", r.violations.join("; ")));
    }
    for p in 0..c.spec().partitions {
        if let Some((_, hw, members)) = settle(c, p).await? {
            let views: Vec<DataDirView> = members.iter().map(|n| c.node_disk(*n)).collect();
            replica::identical_below_hw(&views, PartitionId(p), hw).into_result()?;
        }
    }
    Ok(())
}

/// Clears the process-wide before-submit hook on drop.
struct HookGuard;
impl Drop for HookGuard {
    fn drop(&mut self) {
        set_before_submit_hook(None);
    }
}

/// Install a hook that signals `run` through `api` and waits until every replica in
/// `nodes` journaled it and the run is suspended again.
fn signal_hook(api: ApiClient, nodes: Vec<Arc<P13Ctx>>, run: RunId) -> HookGuard {
    let hook: BeforeSubmitHook = Arc::new(move |passed: Vec<String>| {
        let api = api.clone();
        let nodes = nodes.clone();
        Box::pin(async move {
            if !passed.contains(&run.to_string()) {
                return;
            }
            let read = |ctx: &Arc<P13Ctx>| {
                ctx.node()
                    .and_then(|n| n.handle(run.partition()))
                    .and_then(|ph| ph.read_run(&run).ok().flatten())
            };
            let before = nodes
                .iter()
                .filter_map(read)
                .map(|r| r.next_seq)
                .max()
                .unwrap_or(0);
            let r = api
                .post_json(
                    &format!("/v1/runs/{run}/signals/poke"),
                    &json!({"payload": 1}),
                )
                .await;
            assert!(
                matches!(&r, Ok((st, _)) if st.is_success()),
                "harness: signal {run}: {r:?}"
            );
            let deadline = Instant::now() + OP_T;
            loop {
                let recs: Vec<RunRecord> = nodes.iter().filter_map(read).collect();
                if !recs.is_empty()
                    && recs
                        .iter()
                        .all(|r| r.next_seq > before && r.status == RunStatus::Scheduled)
                {
                    return;
                }
                assert!(Instant::now() < deadline, "harness: signal never journaled");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    });
    set_before_submit_hook(Some(hook));
    HookGuard
}

// ------------------------------------------------------------------------------------
// Scenarios
// ------------------------------------------------------------------------------------

fn passing_runs_repinned_cluster_wide(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (_v1, _v2, v3) = deploy_all(c).await;
        finalized_everywhere(c).await?;
        let mut runs = sleeping_runs(c, 0..c.spec().partitions, 60).await?;
        runs.extend(sleeping_runs(c, 0..c.spec().partitions, 60).await?);
        let mut leaders = BTreeSet::new();
        for p in 0..c.spec().partitions {
            leaders.insert(c.assignment(p).await.leader);
        }
        let idle = running(c)
            .into_iter()
            .find(|n| !leaders.contains(n))
            .ok_or_else(|| fail("harness", format!("every node leads: {leaders:?}")))?;
        let other = running(c)
            .into_iter()
            .find(|n| *n != idle)
            .expect("a second node");

        // Two identical dry runs at different nodes: the same report.
        let d1 = migrate_ok(c, idle, &body(1, 3, true)).await?;
        let d2 = migrate_ok(c, other, &body(1, 3, true)).await?;
        if d1 != d2 {
            return Err(fail(
                ORACLE,
                format!("dry runs differ: node {idle}: {d1} / node {other}: {d2}"),
            ));
        }
        expect_ids(&d1, "passed", &runs)?;
        expect_ids(&d1, "repinned", &[])?;

        let rep = migrate_ok(c, idle, &body(1, 3, false)).await?;
        expect_ids(&rep, "passed", &runs)?;
        expect_ids(&rep, "repinned", &runs)?;
        for r in &runs {
            pinned_everywhere(c, r, v3).await?;
        }
        oracles(c).await
    })
}

fn diverging_runs_never_repinned(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (v1, v2, _v3) = deploy_all(c).await;
        finalized_everywhere(c).await?;
        let parts = 0..c.spec().partitions;
        let diverging = sleeping_runs(c, parts.clone(), 45).await?;
        // Waiting for a 10 min start delay: journal = RunStarted only, which v2 replays.
        let mut delayed = Vec::new();
        for p in parts {
            delayed.push(
                start_on(
                    c,
                    2,
                    p,
                    json!({"input": {"sleep_s": 1}, "delay_ms": 600_000}),
                )
                .await,
            );
        }
        // Precondition: every delayed run is in its leader's state.
        for r in &delayed {
            wait_record(c, r, "applied on the leader", |_| true).await?;
        }
        let dry = migrate_ok(c, 2, &body(1, 2, true)).await?;
        expect_ids(&dry, "passed", &delayed)?;
        expect_ids(&dry, "failed", &diverging)?;
        let rep = migrate_ok(c, 2, &body(1, 2, false)).await?;
        if ids(&rep, "repinned") != set(&delayed) {
            let mut recs = Vec::new();
            for r in &delayed {
                let leader = c.assignment(r.partition().0).await.leader;
                recs.push(format!(
                    "{r}@{leader}: {:?}",
                    record_on(c, leader, r).map(|x| (
                        x.status,
                        x.workflow,
                        x.workflow_version,
                        x.next_seq,
                        x.module_sha.map(|s| hex(&s)[..8].to_owned())
                    ))
                ));
            }
            return Err(fail(
                ORACLE,
                format!(
                    "delayed runs not repinned (v1 {}, v2 {}): {recs:?}; report {rep}",
                    &hex(&v1)[..8],
                    &hex(&v2)[..8]
                ),
            ));
        }
        expect_ids(&rep, "failed", &diverging)?;
        for f in rep["failed"].as_array().into_iter().flatten() {
            if f["seq"].as_u64().unwrap_or(0) == 0 || f["detail"].as_str().unwrap_or("").is_empty()
            {
                return Err(fail(ORACLE, format!("failure without divergence: {f}")));
            }
        }
        for r in &delayed {
            pinned_everywhere(c, r, v2).await?;
        }
        // The diverging runs keep v1 and complete on it.
        for r in &diverging {
            let rec = wait_record(c, r, "completed", |rec| rec.status.is_terminal()).await?;
            if rec.status != RunStatus::Completed || rec.module_sha != Some(v1) {
                return Err(fail(
                    ORACLE,
                    format!(
                        "diverging run {r}: {:?} on {:?}, want completed on v1",
                        rec.status,
                        rec.module_sha.map(|s| hex(&s))
                    ),
                ));
            }
            pinned_everywhere(c, r, v1).await?;
        }
        oracles(c).await
    })
}

/// Public for the mutant test (`tests/migrate.rs`).
pub fn concurrent_growth_seq_mismatch(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (v1, _v2, v3) = deploy_all(c).await;
        finalized_everywhere(c).await?;
        let runs = sleeping_runs(c, [0], 600).await?;
        let run = runs[0];
        let nodes: Vec<Arc<P13Ctx>> = running(c)
            .into_iter()
            .filter_map(|n| c.handle(n).and_then(|h| h.p13()).map(Arc::clone))
            .collect();
        let rep = {
            let _g = signal_hook(c.client(3), nodes, run);
            migrate_ok(c, 1, &body(1, 3, false)).await?
        };
        expect_ids(&rep, "passed", &runs)?;
        expect_ids(&rep, "seq_mismatch", &runs)?;
        expect_ids(&rep, "repinned", &[])?;
        pinned_everywhere(c, &run, v1).await?;
        // Migrate again: the grown journal replays clean and the run is repinned (it may be
        // busy for a moment if the signal woke it).
        let deadline = Instant::now() + OP_T;
        loop {
            let rep = migrate_ok(c, 2, &body(1, 3, false)).await?;
            if ids(&rep, "repinned").contains(&run.to_string()) {
                break;
            }
            if Instant::now() >= deadline {
                return Err(fail(
                    ORACLE,
                    format!("never repinned after the race: {rep}"),
                ));
            }
            tokio::time::sleep(STEP).await;
        }
        pinned_everywhere(c, &run, v3).await?;
        oracles(c).await
    })
}

fn busy_runs_reported(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        for n in c.node_ids() {
            c.config_mut(n).engine.hot_wait = LONG_HOT_WAIT.into();
            c.restart(n)
                .await
                .unwrap_or_else(|e| panic!("harness: restart {n}: {e}"));
        }
        wait_ready(c).await?;
        let (v1, _v2, _v3) = deploy_all(c).await;
        finalized_everywhere(c).await?;
        let mut runs = Vec::new();
        for p in 0..c.spec().partitions {
            runs.push(start_on(c, 1, p, json!({"input": {"sleep_s": PARKED_SLEEP_S}})).await);
        }
        // Parked in the engine's hot wait: Running (leased) with the timer journaled.
        for r in &runs {
            wait_record(c, r, "hot-parked", |rec| {
                rec.status == RunStatus::Running && rec.next_seq >= SLEEPING_SEQ
            })
            .await?;
        }
        let rep = migrate_ok(c, 1, &body(1, 3, false)).await?;
        expect_ids(&rep, "busy", &runs)?;
        expect_ids(&rep, "repinned", &[])?;
        for r in &runs {
            pinned_everywhere(c, r, v1).await?;
        }
        // A hot wait parks only waits shorter than `hot_wait`, so the parked runs finish
        // their sleep in process and complete, never repinned, on v1; a later migrate
        // finds nothing left to move.
        for r in &runs {
            let rec = wait_record(c, r, "completed", |rec| rec.status.is_terminal()).await?;
            if rec.status != RunStatus::Completed || rec.module_sha != Some(v1) {
                return Err(fail(
                    ORACLE,
                    format!("busy run {r}: {:?} on {:?}", rec.status, rec.module_sha),
                ));
            }
        }
        let rep = migrate_ok(c, 2, &body(1, 3, true)).await?;
        expect_ids(&rep, "passed", &[])?;
        oracles(c).await
    })
}

/// Every running node sees the cluster finalized at level 2. A submitting migrate checks
/// the gate on every leg after its own meta barrier: one racing the finalization can be
/// refused by a leg whose barrier came first while another leg already repinned (the call
/// answers 409 and a rerun reports only the rest), so the scenarios start after it.
async fn finalized_everywhere(c: &InProcCluster) -> ScenarioResult {
    wait_level(c, 2).await
}

/// Every node answers `/readyz` and every partition has a leader.
async fn wait_ready(c: &InProcCluster) -> ScenarioResult {
    let deadline = Instant::now() + OP_T;
    loop {
        let mut ok = true;
        for n in running(c) {
            ok &= c.client(n).ready().await;
        }
        for p in 0..c.spec().partitions {
            ok &= c.assignment(p).await.leader != 0;
        }
        if ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(fail("harness", "cluster not ready after the restarts"));
        }
        tokio::time::sleep(STEP).await;
    }
}

/// `migrate_refused_before_finalize` on its own cluster: `nodes` nodes started at the
/// emulated level 1 (`version_gate::start_level1_cluster`), then every node but the last
/// upgraded to this build's level, so one node is still at level 1 and the cluster stays
/// finalized at 1. A submitting migrate is then 409 `feature_not_enabled` at every node and
/// no `RepinRun` reaches any partition log, while the dry run works at every node. After
/// the last node's upgrade the cluster finalizes level 2 and the same migrate repins; the
/// upgrade oracle checks that no `RepinRun` is on disk before the finalizing meta entry.
/// With `nodes == 1` there is no upgrade step and no repin.
pub async fn refused_before_finalize(nodes: usize) -> ScenarioResult {
    let mut c = super::version_gate::start_level1_cluster(nodes)
        .await
        .map_err(|e: HarnessError| fail("harness", e.to_string()))?;
    let r = refused_body(&mut c).await;
    c.stop().await;
    r
}

fn level_of(c: &InProcCluster, node: u64) -> Option<u32> {
    super::version_gate::finalized(c, node)
}

async fn wait_level(c: &InProcCluster, level: u32) -> ScenarioResult {
    let deadline = Instant::now() + OP_T;
    loop {
        let l: Vec<(u64, Option<u32>)> = running(c)
            .into_iter()
            .map(|n| (n, level_of(c, n)))
            .collect();
        if !l.is_empty() && l.iter().all(|(_, v)| *v == Some(level)) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(fail("finalized", format!("not at level {level}: {l:?}")));
        }
        tokio::time::sleep(STEP).await;
    }
}

fn scans_all(c: &InProcCluster) -> Vec<crate::oracle::disk::LogScan> {
    let mut scans = Vec::new();
    for node in c.node_ids() {
        let view = c.node_disk(node);
        for p in 0..c.spec().partitions {
            if let Ok(s) = scan_partition(&view, PartitionId(p)) {
                scans.push(s);
            }
        }
    }
    scans
}

async fn refused_body(c: &mut InProcCluster) -> ScenarioResult {
    use crate::harness::p13::P13Harness;
    let (v1, _v2, v3) = deploy_all(c).await;
    let runs = sleeping_runs(c, 0..c.spec().partitions.min(2), 600).await?;
    let ids = c.node_ids();
    let (last, rest) = ids.split_last().expect("nodes");
    for n in rest {
        c.upgrade(*n, None)
            .await
            .map_err(|e| fail("harness", format!("upgrade {n}: {e}")))?;
    }
    wait_ready(c).await?;
    barrier_all(c).await;
    wait_level(c, 1).await?;
    for n in running(c) {
        let (st, v) = migrate(c, n, &body(1, 3, false)).await;
        if st != StatusCode::CONFLICT || reason(&v) != "feature_not_enabled" {
            return Err(fail(
                "p13_upgrade",
                format!("node {n}: submitting migrate before finalize: {st} {v}"),
            ));
        }
        let (st, v) = migrate(c, n, &body(1, 3, true)).await;
        if st != StatusCode::OK {
            return Err(fail(ORACLE, format!("node {n}: dry run: {st} {v}")));
        }
        expect_ids(&v, "passed", &runs)?;
    }
    for r in &runs {
        pinned_everywhere(c, r, v1).await?;
    }
    let r = no_gated_partition_commands(&scans_all(c));
    if !r.is_ok() {
        return Err(fail("p13_upgrade", r.violations.join("; ")));
    }
    if rest.is_empty() {
        return Ok(());
    }
    // The last node's upgrade lets the cluster finalize level 2; the migrate now repins.
    c.upgrade(*last, None)
        .await
        .map_err(|e| fail("harness", format!("upgrade {last}: {e}")))?;
    wait_ready(c).await?;
    wait_level(c, 2).await?;
    let rep = migrate_ok(c, *last, &body(1, 3, false)).await?;
    expect_ids(&rep, "repinned", &runs)?;
    for r in &runs {
        pinned_everywhere(c, r, v3).await?;
    }
    let Some(n) = running(c).first().copied() else {
        return Err(fail("harness", "no running node"));
    };
    let meta = c.meta_log(n).await;
    let r = no_gated_before_finalize_from(1, &meta, &scans_all(c));
    if !r.is_ok() {
        return Err(fail("p13_upgrade", r.violations.join("; ")));
    }
    Ok(())
}
