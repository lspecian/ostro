//! Phase 13 `version_gate` scenarios (tier B; 13-14, CLUS-06, D-P13-3/4/17).
//!
//! Every scenario builds its own cluster through the Phase 13 lifecycle: one node started at
//! the emulated level 1 (`start_single_with(.., Some(1))`, so the cluster is finalized at 1)
//! and `nodes − 1` more joined at level 1 (`add_node`, the real join flow). Upgrades are
//! [`P13Harness::upgrade`]: a graceful restart of one node from its data dir at
//! `level_max = None` (this build's level, 2) or back at `Some(1)`. A continuous workload
//! ([`Workload`]) starts runs round robin on the running nodes throughout; every acked run
//! must complete (zero failed runs). The upgrade oracle
//! (`oracle::p13_upgrade::no_gated_before_finalize_from(1, ..)`) checks the meta log and
//! every partition log at the end: no gated record (ext 100/101/102, AlterReplicas,
//! RepinRun) at or before the entry that finalized level 2.
//!
//! | Scenario | Claim |
//! |---|---|
//! | [`auto_finalize_after_last_upgrade`] | finalized stays 1 until the last node runs level 2, then becomes 2 within 5 s |
//! | [`hold_keeps_window_open`] | a hold set before the last upgrade keeps finalized at 1; `release` finalizes |
//! | [`manual_finalize_refused_until_all_support`] | `finalize` with one node at level 1 leaves the cluster at 1; after its upgrade it answers 200 at level 2 |
//! | [`gated_upload_refused_before_finalize`] | the upload gate refuses a level-2 module on every node before finalization (409 `feature_not_enabled`) and accepts it after; a level-1 module uploads throughout |
//! | [`downgrade_before_finalize_ok`] | with a hold, an upgraded node restarted at level 1 serves again |
//! | [`downgrade_after_finalize_refused`] | after finalization a node restarted at level 1 fails its start with the "finalized by a newer Ostro" message before any partition file changes |
//!
//! The scenarios own their cluster's lifecycle (single start + joins at an emulated level),
//! which the runner's static `ClusterHarness::start` cannot provide, so [`all`] stays empty
//! and `tests/version_gate.rs` runs them directly through [`run`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use ostro_types::PartitionId;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use super::{Scenario, ScenarioFailure, ScenarioResult};
use crate::harness::inproc::{InProcCluster, TERMINAL, TEST_WORKFLOW};
use crate::harness::p13::{NodeOpts, P13Harness};
use crate::harness::{ApiClient, ClusterHarness, ClusterSpec, HarnessError};
use crate::oracle::disk::scan_partition;
use crate::oracle::p13_upgrade::no_gated_before_finalize_from;

/// Nodes of every version-gate cluster.
pub const NODES: usize = 3;
/// Bound on auto-finalization after the last node reports level 2.
pub const FINALIZE_WITHIN: Duration = Duration::from_secs(5);
/// How long "finalized stays at 1" is sampled where nothing should change.
pub const STAYS_FOR: Duration = Duration::from_secs(4);
/// Bound on one workload run.
const RUN_T: Duration = Duration::from_secs(120);

/// Every `version_gate` scenario in the runner's registry: none (module docs).
pub fn all() -> Vec<Scenario> {
    vec![]
}

/// A version-gate scenario body over its own cluster.
pub type GateFn = for<'a> fn(
    &'a mut InProcCluster,
    &'a Workload,
) -> futures_util::future::BoxFuture<'a, ScenarioResult>;

/// The six scenarios by name.
pub fn scenarios() -> Vec<(&'static str, GateFn)> {
    vec![
        ("auto_finalize_after_last_upgrade", |c, w| {
            Box::pin(auto_finalize_after_last_upgrade(c, w))
        }),
        ("hold_keeps_window_open", |c, w| {
            Box::pin(hold_keeps_window_open(c, w))
        }),
        ("manual_finalize_refused_until_all_support", |c, w| {
            Box::pin(manual_finalize_refused_until_all_support(c, w))
        }),
        ("gated_upload_refused_before_finalize", |c, w| {
            Box::pin(gated_upload_refused_before_finalize(c, w))
        }),
        ("downgrade_before_finalize_ok", |c, w| {
            Box::pin(downgrade_before_finalize_ok(c, w))
        }),
        ("downgrade_after_finalize_refused", |c, w| {
            Box::pin(downgrade_after_finalize_refused(c, w))
        }),
    ]
}

fn fail(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

fn harness(e: HarnessError) -> ScenarioFailure {
    fail("harness", e.to_string())
}

// ------------------------------------------------------------------------- cluster

/// Start the level-1 cluster of `nodes` nodes (module docs) and deploy the test module.
pub async fn start_level1_cluster(nodes: usize) -> Result<InProcCluster, HarnessError> {
    let mut c = InProcCluster::start_single_with(ClusterSpec::single(), Some(1)).await?;
    for _ in 1..nodes {
        c.add_node(NodeOpts {
            level_max: Some(1),
            advertise: true,
        })
        .await?;
    }
    if nodes > 1 {
        settle_voters(&c, nodes).await?;
    }
    c.client(1).deploy_test_module().await?;
    Ok(c)
}

/// Wait until the meta group has the largest odd voter count `<= nodes`, no joint
/// configuration and every member `active` (13-09's reconciler promotes joined learners),
/// so a restart of any one node keeps a quorum.
async fn settle_voters(c: &InProcCluster, nodes: usize) -> Result<(), HarnessError> {
    let want = if nodes % 2 == 1 { nodes } else { nodes - 1 };
    // clock-exempt: test timeout
    let t0 = tokio::time::Instant::now();
    let mut last = Value::Null;
    loop {
        let leader = meta_leader(c).unwrap_or(1);
        if let Ok(st) = c.cluster_status(leader).await {
            let len = |k: &str| st[k].as_array().map_or(0, Vec::len);
            let all_active = st["members"]
                .as_array()
                .is_some_and(|m| m.len() == nodes && m.iter().all(|x| x["state"] == "active"));
            if len("voters") == want && len("voters_outgoing") == 0 && all_active {
                return Ok(());
            }
            last = st;
        }
        if t0.elapsed() > crate::harness::inproc::MEMBERSHIP_T {
            return Err(HarnessError::Other(format!(
                "membership did not settle at {want} voters: {last}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Run `body` on a fresh level-1 cluster of `nodes` nodes with the workload and the upgrade
/// oracle (module docs); the cluster is stopped afterwards whatever the result.
pub async fn run(nodes: usize, body: GateFn) -> ScenarioResult {
    let mut c = start_level1_cluster(nodes).await.map_err(harness)?;
    let w = Workload::start(&c);
    let r = body(&mut c, &w).await;
    let wr = w.finish().await;
    let oracle = upgrade_oracle(&c).await;
    c.stop().await;
    r?;
    wr?;
    oracle
}

/// The finalized level in `node`'s meta view (`None` = not running).
pub fn finalized(c: &InProcCluster, node: u64) -> Option<u32> {
    c.handle(node)
        .map(|h| h.meta().view().read(ostro_cluster::compat::finalized_level))
}

/// The running nodes' finalized levels.
fn levels(c: &InProcCluster) -> BTreeMap<u64, u32> {
    c.node_ids()
        .into_iter()
        .filter_map(|n| finalized(c, n).map(|l| (n, l)))
        .collect()
}

/// Wait until every running node sees `level`; how long it took.
async fn wait_finalized(
    c: &InProcCluster,
    level: u32,
    limit: Duration,
) -> Result<Duration, ScenarioFailure> {
    // clock-exempt: test timeout
    let t0 = tokio::time::Instant::now();
    loop {
        let l = levels(c);
        if !l.is_empty() && l.values().all(|v| *v == level) {
            return Ok(t0.elapsed());
        }
        if t0.elapsed() >= limit {
            return Err(fail(
                "finalized",
                format!("not finalized at {level} on every node within {limit:?}: {l:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Sample for `d` that every running node stays at `level`.
async fn stays_finalized(c: &InProcCluster, level: u32, d: Duration) -> ScenarioResult {
    // clock-exempt: test timeout
    let t0 = tokio::time::Instant::now();
    while t0.elapsed() < d {
        let l = levels(c);
        if l.values().any(|v| *v != level) {
            return Err(fail(
                "finalized",
                format!("finalized left {level} early: {l:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

/// The running meta leader (manual finalize runs there; 13-04).
fn meta_leader(c: &InProcCluster) -> Option<u64> {
    c.node_ids()
        .into_iter()
        .find(|n| c.handle(*n).is_some_and(|h| h.meta().is_leader()))
}

async fn post(
    api: &ApiClient,
    path: &str,
) -> Result<(reqwest::StatusCode, Value), ScenarioFailure> {
    api.post_json(path, &json!({})).await.map_err(harness)
}

/// Upgrade every node but the last to this build's level; the last node's id.
async fn upgrade_all_but_last(c: &mut InProcCluster) -> Result<u64, ScenarioFailure> {
    let ids = c.node_ids();
    let (last, rest) = ids.split_last().expect("nodes");
    for n in rest {
        c.upgrade(*n, None).await.map_err(harness)?;
        stays_finalized(c, 1, Duration::from_millis(500)).await?;
    }
    Ok(*last)
}

// ------------------------------------------------------------------------- workload

/// Runs started round robin on the running nodes (module docs).
pub struct Workload {
    clients: Arc<Mutex<Vec<ApiClient>>>,
    acked: Arc<Mutex<Vec<(ApiClient, String)>>>,
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl Workload {
    fn start(c: &InProcCluster) -> Self {
        let clients = Arc::new(Mutex::new(Self::running(c)));
        let acked = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let task = {
            let (clients, acked, stop) =
                (Arc::clone(&clients), Arc::clone(&acked), Arc::clone(&stop));
            tokio::spawn(async move {
                let mut i: u64 = 0;
                while !stop.load(Ordering::SeqCst) {
                    let list = clients.lock().expect("clients").clone();
                    if let Some(api) = list.get(usize::try_from(i).unwrap_or(0) % list.len().max(1))
                        && let Ok(ack) = api
                            .start_run(TEST_WORKFLOW, json!({ "gate": i }), None)
                            .await
                    {
                        acked.lock().expect("acked").push((api.clone(), ack.run_id));
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            })
        };
        Self {
            clients,
            acked,
            stop,
            task,
        }
    }

    fn running(c: &InProcCluster) -> Vec<ApiClient> {
        c.node_ids()
            .into_iter()
            .filter(|n| c.handle(*n).is_some())
            .map(|n| c.client(n))
            .collect()
    }

    /// Point the workload at the nodes running now (after a restart).
    pub fn refresh(&self, c: &InProcCluster) {
        *self.clients.lock().expect("clients") = Self::running(c);
    }

    /// Stop starting runs; every acked run must complete (read from a running node).
    async fn finish(self) -> ScenarioResult {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.task.await;
        let acked = std::mem::take(&mut *self.acked.lock().expect("acked"));
        let clients = self.clients.lock().expect("clients").clone();
        if acked.is_empty() {
            return Err(fail("workload", "no run was acknowledged"));
        }
        // A run is read from whichever running node answers for its partition.
        let mut bad = Vec::new();
        // clock-exempt: test timeout
        let t0 = tokio::time::Instant::now();
        for (_, run_id) in &acked {
            let status = loop {
                let mut found = None;
                for api in &clients {
                    if let Ok((st, v)) = api.get_json(&format!("/v1/runs/{run_id}")).await
                        && st.is_success()
                        && let Some(s) = v["status"].as_str()
                        && TERMINAL.contains(&s)
                    {
                        found = Some(s.to_owned());
                        break;
                    }
                }
                if found.is_some() || t0.elapsed() > RUN_T {
                    break found;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            match status.as_deref() {
                Some("completed") => {}
                other => bad.push(format!("{run_id}: {other:?}")),
            }
        }
        if bad.is_empty() {
            Ok(())
        } else {
            Err(fail(
                "workload",
                format!(
                    "{} of {} runs did not complete: {bad:?}",
                    bad.len(),
                    acked.len()
                ),
            ))
        }
    }
}

/// The upgrade oracle over the lowest running node's meta log and every node's partition
/// logs.
async fn upgrade_oracle(c: &InProcCluster) -> ScenarioResult {
    let Some(n) = c.node_ids().into_iter().find(|n| c.handle(*n).is_some()) else {
        return Err(fail(
            "p13_upgrade",
            "no running node to read the meta log from",
        ));
    };
    let meta = c.meta_log(n).await;
    let mut scans = Vec::new();
    for node in c.node_ids() {
        let view = c.node_disk(node);
        for p in 0..c.spec().partitions {
            if let Ok(s) = scan_partition(&view, PartitionId(p)) {
                scans.push(s);
            }
        }
    }
    let r = no_gated_before_finalize_from(1, &meta, &scans);
    if r.is_ok() {
        Ok(())
    } else {
        Err(fail("p13_upgrade", r.violations.join("; ")))
    }
}

// ------------------------------------------------------------------------- scenarios

/// Finalized stays 1 until the last node runs level 2, then becomes 2 within
/// [`FINALIZE_WITHIN`].
pub async fn auto_finalize_after_last_upgrade(
    c: &mut InProcCluster,
    w: &Workload,
) -> ScenarioResult {
    let last = upgrade_all_but_last(c).await?;
    w.refresh(c);
    stays_finalized(c, 1, STAYS_FOR).await?;
    c.upgrade(last, None).await.map_err(harness)?;
    w.refresh(c);
    let took = wait_finalized(c, 2, FINALIZE_WITHIN).await?;
    eprintln!("version_gate: auto-finalized {took:?} after the last node restarted at level 2");
    Ok(())
}

/// A hold before the last upgrade keeps finalized at 1; `release` finalizes.
pub async fn hold_keeps_window_open(c: &mut InProcCluster, w: &Workload) -> ScenarioResult {
    let last = upgrade_all_but_last(c).await?;
    w.refresh(c);
    let (st, v) = post(&c.client(1), "/v1/cluster/upgrade/hold").await?;
    if !st.is_success() {
        return Err(fail("hold", format!("hold: {st} {v}")));
    }
    c.upgrade(last, None).await.map_err(harness)?;
    w.refresh(c);
    stays_finalized(c, 1, STAYS_FOR).await?;
    let (st, v) = post(&c.client(last), "/v1/cluster/upgrade/release").await?;
    if !st.is_success() {
        return Err(fail("release", format!("release: {st} {v}")));
    }
    let took = wait_finalized(c, 2, FINALIZE_WITHIN).await?;
    eprintln!("version_gate: finalized {took:?} after release");
    Ok(())
}

/// `finalize` with one node at level 1 leaves the cluster at 1; after its upgrade it answers
/// 200 at level 2.
pub async fn manual_finalize_refused_until_all_support(
    c: &mut InProcCluster,
    w: &Workload,
) -> ScenarioResult {
    let last = upgrade_all_but_last(c).await?;
    w.refresh(c);
    let leader = meta_leader(c).ok_or_else(|| fail("finalize", "no meta leader"))?;
    let (st, v) = post(&c.client(leader), "/v1/cluster/upgrade/finalize").await?;
    // 13-04: with a member at level 1 the decision is Stay (target 1): the answer is either
    // a refusal or the unchanged status; the level must not move.
    if st.is_success() && v["finalized"].as_u64() != Some(1) {
        return Err(fail(
            "finalize",
            format!("finalized with a level-1 member: {v}"),
        ));
    }
    stays_finalized(c, 1, Duration::from_secs(1)).await?;
    c.upgrade(last, None).await.map_err(harness)?;
    w.refresh(c);
    // The finalizer may win the race; the manual call must then report level 2 as well.
    // clock-exempt: test timeout
    let t0 = tokio::time::Instant::now();
    loop {
        let leader = meta_leader(c).ok_or_else(|| fail("finalize", "no meta leader"))?;
        let (st, v) = post(&c.client(leader), "/v1/cluster/upgrade/finalize").await?;
        if st == reqwest::StatusCode::OK && v["finalized"].as_u64() == Some(2) {
            break;
        }
        if t0.elapsed() > Duration::from_secs(30) {
            return Err(fail(
                "finalize",
                format!("finalize after the last upgrade: {st} {v}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    wait_finalized(c, 2, FINALIZE_WITHIN).await?;
    Ok(())
}

/// The gate on every node: a level-2 selection refused before finalization, accepted after;
/// a real level-1 module uploads on every node throughout.
pub async fn gated_upload_refused_before_finalize(
    c: &mut InProcCluster,
    w: &Workload,
) -> ScenarioResult {
    let gate_on_every_node = |c: &InProcCluster, want_ok: bool| -> ScenarioResult {
        for n in c.node_ids() {
            let Some(h) = c.handle(n) else { continue };
            let r = ostro_server::control::modules::upload_gate_for(h.meta().view(), "0.1", 2);
            match (r, want_ok) {
                (Ok(ext), true) if !ext.is_empty() => {}
                (Err(e), false)
                    if e.code == ostro_api::ErrorCode::Conflict
                        && e.details.as_ref().is_some_and(|d| {
                            d["reason"]
                                == ostro_server::control::modules::REASON_FEATURE_NOT_ENABLED
                        }) => {}
                (r, _) => {
                    return Err(fail(
                        "upload_gate",
                        format!("node {n}: level-2 module, want ok={want_ok}: {r:?}"),
                    ));
                }
            }
        }
        Ok(())
    };
    let level1_upload_everywhere = |c: &InProcCluster| {
        let clients: Vec<ApiClient> = c
            .node_ids()
            .into_iter()
            .filter(|n| c.handle(*n).is_some())
            .map(|n| c.client(n))
            .collect();
        async move {
            for api in clients {
                api.deploy_test_module().await.map_err(harness)?;
            }
            Ok::<(), ScenarioFailure>(())
        }
    };
    gate_on_every_node(c, false)?;
    level1_upload_everywhere(c).await?;
    let last = upgrade_all_but_last(c).await?;
    w.refresh(c);
    gate_on_every_node(c, false)?;
    c.upgrade(last, None).await.map_err(harness)?;
    w.refresh(c);
    wait_finalized(c, 2, FINALIZE_WITHIN).await?;
    gate_on_every_node(c, true)?;
    level1_upload_everywhere(c).await?;
    Ok(())
}

/// With a hold, an upgraded node restarted at level 1 serves again.
pub async fn downgrade_before_finalize_ok(c: &mut InProcCluster, w: &Workload) -> ScenarioResult {
    let (st, v) = post(&c.client(1), "/v1/cluster/upgrade/hold").await?;
    if !st.is_success() {
        return Err(fail("hold", format!("hold: {st} {v}")));
    }
    for n in c.node_ids() {
        c.upgrade(n, None).await.map_err(harness)?;
    }
    w.refresh(c);
    stays_finalized(c, 1, Duration::from_secs(2)).await?;
    let back = *c.node_ids().last().expect("nodes");
    c.upgrade(back, Some(1)).await.map_err(harness)?;
    w.refresh(c);
    let api = c.client(back);
    let ack = api
        .start_run(TEST_WORKFLOW, json!({ "downgraded": true }), None)
        .await
        .map_err(harness)?;
    let s = api.wait_run(&ack.run_id, RUN_T).await.map_err(harness)?;
    if s != "completed" {
        return Err(fail(
            "downgrade",
            format!("run on the downgraded node: {s}"),
        ));
    }
    stays_finalized(c, 1, Duration::from_secs(1)).await
}

/// Every partition file of `root` (everything outside `meta/`, the AOT `cache/` and `tls/`,
/// which a start may rewrite before the fence: the imported CA), with its mtime and length.
fn partition_files(root: &Path) -> BTreeMap<PathBuf, (SystemTime, u64)> {
    fn walk(dir: &Path, root: &Path, out: &mut BTreeMap<PathBuf, (SystemTime, u64)>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            let rel = p.strip_prefix(root).unwrap_or(&p).to_path_buf();
            let top = rel.components().next().map(|c| c.as_os_str().to_owned());
            if top
                .as_deref()
                .is_some_and(|t| t == "meta" || t == "cache" || t == "tls")
            {
                continue;
            }
            let Ok(md) = e.metadata() else { continue };
            if md.is_dir() {
                walk(&p, root, out);
            } else if let Ok(m) = md.modified() {
                out.insert(rel, (m, md.len()));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// After finalization a node restarted at level 1 fails to start with the "finalized by a
/// newer Ostro" message before any partition file changes.
pub async fn downgrade_after_finalize_refused(
    c: &mut InProcCluster,
    w: &Workload,
) -> ScenarioResult {
    for n in c.node_ids() {
        c.upgrade(n, None).await.map_err(harness)?;
    }
    w.refresh(c);
    wait_finalized(c, 2, FINALIZE_WITHIN).await?;
    eprintln!("version_gate: downgrade_after: finalized at 2");
    let victim = *c.node_ids().last().expect("nodes");
    // Stop it first so its files are quiescent, snapshot, then start it at level 1.
    c.kill(victim).await;
    c.heal(victim);
    w.refresh(c);
    let root = c.node_disk(victim).root;
    let before = partition_files(&root);
    eprintln!(
        "version_gate: downgrade_after: {} partition files snapshotted",
        before.len()
    );
    match c.upgrade(victim, Some(1)).await {
        Ok(()) => {
            return Err(fail(
                "boot_fence",
                "a level-1 binary started on data finalized at level 2",
            ));
        }
        Err(e) => {
            let msg = e.to_string();
            if !msg.contains("finalized by a newer Ostro") {
                return Err(fail("boot_fence", format!("unexpected start error: {msg}")));
            }
        }
    }
    eprintln!("version_gate: downgrade_after: level-1 start refused");
    let after = partition_files(&root);
    if before != after {
        let changed: Vec<String> = after
            .iter()
            .filter(|(p, v)| before.get(*p) != Some(*v))
            .map(|(p, _)| p.display().to_string())
            .chain(
                before
                    .keys()
                    .filter(|p| !after.contains_key(*p))
                    .map(|p| format!("{} (removed)", p.display())),
            )
            .collect();
        return Err(fail(
            "boot_fence",
            format!("the refused start touched partition files: {changed:?}"),
        ));
    }
    if before.is_empty() {
        return Err(fail("boot_fence", "no partition file to compare (vacuous)"));
    }
    // Back at this build's level it serves again.
    c.upgrade(victim, None).await.map_err(harness)?;
    w.refresh(c);
    eprintln!("version_gate: downgrade_after: restarted at level 2");
    Ok(())
}
