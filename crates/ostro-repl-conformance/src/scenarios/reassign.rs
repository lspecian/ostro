//! Phase 13 `reassign` scenarios (tier B, 13-10; CLUS-05, D-P13-11/12).
//!
//! Partition reassignment on the Phase 12 ISR log under a continuous workload: runs are
//! started round robin on the running nodes every 100 ms ([`Load`]); every acknowledged run
//! must complete (zero failed / cancelled runs) and, where the scenario claims it, the gap
//! between two acknowledged starts never exceeds [`STALL_MAX`]. Moves go through the real
//! placement controller (13-10) on the meta leader, the real `AlterReplicas` apply and the
//! real NodeOps handoff (13-11). Every scenario ends with the Phase 12 oracles that hold
//! across reassignments: replica identity below the HW over each partition's live ISR, and
//! acknowledged ⇒ on disk on ≥ min_insync nodes; the LA-rule / ISR-only election audits run
//! on the committed meta log where it is not compacted (13-09 note: a learner join compacts
//! the leader's log, and those audits read log entries only; 13-18 follow-up). The upgrade
//! oracle (`no_gated_before_finalize_from`) runs where the cluster starts below level 2.
//!
//! | scenario | what |
//! |----------|------|
//! | `rf_raised_on_join` | 1 static node (RF 1) + policy rf 3 / min_insync 2 + two joins → every partition replicas 3, ISR 3, min_insync 2 |
//! | `drain_evacuates_replicas` | 4 nodes RF 3, drain node 4 → it holds no replica and leads nothing |
//! | `explicit_reassign_and_cancel` | `POST /v1/cluster/reassign` moves one partition (incl. a leader move); a second one is cancelled during CatchUp → its old replicas are restored |
//! | `controller_failover_mid_move` | the meta leader is killed while a move waits in CatchUp → the new controller completes it |
//! | `move_blocked_before_finalize` | a level-1 cluster: reassign / rebalance answer 409 `feature_not_enabled`; no AlterReplicas in the meta log (upgrade oracle) |
//! | `removed_replica_stops_and_cleans` | a replica removed by Shrink goes Offline (no fetch, not served) and keeps its data inert (Phase 12 behaviour, recorded) |
//! | `readded_replica_truncates` | the removed replica is added back after the partition moved on: it catches up and replica identity holds below the HW |
//! | `force_removed_dead_node_evacuated` | a dead node `--force`-removed while holding replicas → the controller re-places its replicas on live members (W7) |

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_meta::ext::{ReplicationPolicy, get_reassignment, put_replication_policy};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult, PartitionAssignment};
use ostro_partition::PartitionRole;
use ostro_types::PartitionId;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::placement::support::{
    ACK_T, append_acked, blackhole, clear, deploy, fail, published, published_all, read_dump,
    settle, wait_meta_leader,
};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster, TERMINAL, TEST_WORKFLOW};
use crate::harness::p13::{NodeOpts, P13Harness};
use crate::harness::{ApiClient, ClusterHarness, ClusterSpec, DataDirView};
use crate::oracle::disk::{acked_on_disk_with, scan_partition};
use crate::oracle::{election, la_rule, replica};

const DEADLINE: Duration = Duration::from_secs(420);
/// Bound on one reassignment converging.
pub(super) const MOVE_T: Duration = Duration::from_secs(90);
/// Longest gap between two acknowledged workload starts where a scenario claims no stall.
pub const STALL_MAX: Duration = Duration::from_secs(2);
/// Bound on one workload run completing.
const RUN_T: Duration = Duration::from_secs(120);

fn spec4() -> ClusterSpec {
    ClusterSpec {
        nodes: 4,
        rf: 3,
        min_insync: 2,
        seed: 1310,
        ..ClusterSpec::default()
    }
}

fn single() -> ClusterSpec {
    ClusterSpec {
        seed: 1310,
        ..ClusterSpec::single()
    }
}

fn scenario(
    name: &'static str,
    spec: fn() -> ClusterSpec,
    run: for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>,
) -> Scenario {
    Scenario {
        name,
        group: "reassign",
        tier: Tier::B,
        deadline: DEADLINE,
        spec,
        run,
    }
}

/// Every `reassign` scenario.
pub fn all() -> Vec<Scenario> {
    vec![
        scenario("rf_raised_on_join", single, rf_raised_on_join),
        scenario("drain_evacuates_replicas", spec4, drain_evacuates_replicas),
        scenario(
            "explicit_reassign_and_cancel",
            spec4,
            explicit_reassign_and_cancel,
        ),
        scenario(
            "controller_failover_mid_move",
            spec4,
            controller_failover_mid_move,
        ),
        scenario(
            "move_blocked_before_finalize",
            single,
            move_blocked_before_finalize,
        ),
        scenario(
            "removed_replica_stops_and_cleans",
            spec4,
            removed_replica_stops_and_cleans,
        ),
        scenario(
            "readded_replica_truncates",
            spec4,
            readded_replica_truncates,
        ),
        scenario(
            "force_removed_dead_node_evacuated",
            spec4,
            force_removed_dead_node_evacuated,
        ),
    ]
}

// ------------------------------------------------------------------------- helpers
// (13-16: the `pub(super)` helpers and [`Load`] are shared with `promotion`.)

fn sorted(v: &[u64]) -> Vec<u64> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v
}

pub(super) fn no_ext(a: &PartitionAssignment) -> bool {
    matches!(get_reassignment(&a.ext), Ok(None))
}

pub(super) fn running(c: &InProcCluster) -> Vec<u64> {
    c.node_ids()
        .into_iter()
        .filter(|n| c.handle(*n).is_some())
        .collect()
}

/// Wait until every running node sees the cluster finalized at level 2 (the auto-finalizer
/// raises a fresh cluster once every member advertised its NodeVersion; 13-04).
pub(super) async fn wait_level2(c: &InProcCluster) -> ScenarioResult {
    let t0 = Instant::now(); // clock-exempt: test timeout
    loop {
        let levels: Vec<Option<u32>> = running(c)
            .into_iter()
            .map(|n| super::version_gate::finalized(c, n))
            .collect();
        if !levels.is_empty() && levels.iter().all(|l| *l == Some(2)) {
            return Ok(());
        }
        if t0.elapsed() > Duration::from_secs(30) {
            return Err(fail(
                "reassign",
                format!("cluster not finalized at level 2 within 30 s: {levels:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll the published assignments until `f` holds (bounded by `limit`); how long it took.
pub(super) async fn wait_parts(
    c: &InProcCluster,
    what: &str,
    limit: Duration,
    f: impl Fn(&[PartitionAssignment]) -> bool,
) -> Result<Duration, ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: scenario timing
    loop {
        wait_meta_leader(c).await?;
        let parts = published_all(c);
        if f(&parts) {
            return Ok(t0.elapsed());
        }
        if t0.elapsed() >= limit {
            let brief: Vec<_> = parts
                .iter()
                .map(|a| {
                    (
                        a.partition,
                        a.epoch,
                        a.leader,
                        a.replicas.clone(),
                        a.isr.clone(),
                        a.min_insync,
                        get_reassignment(&a.ext).ok().flatten().map(|r| r.target),
                    )
                })
                .collect();
            return Err(fail(
                "reassign",
                format!("{what} not reached within {limit:?}: {brief:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Write `ReplicationPolicy` (ext 99) on the cluster record through the meta leader.
pub(super) async fn set_policy(c: &InProcCluster, rf: u8, min_insync: u8) -> ScenarioResult {
    let leader = wait_meta_leader(c).await?;
    let meta = c.handle(leader).expect("leader").meta();
    for _ in 0..10 {
        let Some((mut info, version)) = meta
            .view()
            .read(|s| s.cluster().map(|v| (v.value.clone(), v.version)))
        else {
            return Err(fail("reassign", "no cluster record"));
        };
        info.ext = put_replication_policy(
            &info.ext,
            &ReplicationPolicy {
                rf,
                min_insync,
                max_voters: 5,
            },
        )
        .map_err(|e| fail("reassign", format!("policy ext: {e}")))?;
        match meta
            .propose(MetaCommand::Put {
                key: MetaKey::Cluster,
                expected_version: Some(version),
                record: MetaRecord::Cluster(info),
            })
            .await
        {
            Ok(MetaResult::Ok { .. }) => return Ok(()),
            Ok(MetaResult::StaleVersion { .. }) => continue,
            other => return Err(fail("reassign", format!("set policy: {other:?}"))),
        }
    }
    Err(fail("reassign", "set policy: kept racing"))
}

/// `POST /v1/cluster/reassign` on `node`.
async fn reassign(
    c: &InProcCluster,
    node: u64,
    partition: u16,
    replicas: &[u64],
    cancel: bool,
) -> Result<(reqwest::StatusCode, Value), ScenarioFailure> {
    c.client(node)
        .post_json(
            "/v1/cluster/reassign",
            &json!({ "partition": partition, "replicas": replicas, "cancel": cancel }),
        )
        .await
        .map_err(|e| fail("reassign", format!("POST /v1/cluster/reassign: {e}")))
}

pub(super) async fn reassign_ok(
    c: &InProcCluster,
    node: u64,
    partition: u16,
    replicas: &[u64],
    cancel: bool,
) -> ScenarioResult {
    let (st, v) = reassign(c, node, partition, replicas, cancel).await?;
    if st.is_success() {
        Ok(())
    } else {
        Err(fail(
            "reassign",
            format!("reassign p{partition} → {replicas:?} (cancel {cancel}): {st} {v}"),
        ))
    }
}

/// Hold `node`'s replication fetches (it stays out of the ISR: CatchUp waits).
fn hold_fetch(c: &InProcCluster, node: u64) {
    for to in c.node_ids() {
        if to != node {
            blackhole(c, node, to, RpcKind::Fetch);
            blackhole(c, node, to, RpcKind::FetchSnapshot);
        }
    }
}

fn release_fetch(c: &InProcCluster, node: u64) {
    for to in c.node_ids() {
        if to != node {
            clear(c, node, to);
        }
    }
}

/// The node of `ids` that is not the meta leader and not in `avoid` (lowest first).
pub(super) fn pick_node(c: &InProcCluster, avoid: &[u64]) -> Option<u64> {
    let leader = super::placement::support::meta_leader(c);
    running(c)
        .into_iter()
        .find(|n| Some(*n) != leader && !avoid.contains(n))
}

/// The Phase 12 oracles that hold across reassignments (module docs).
pub(super) async fn reassign_oracles(c: &InProcCluster, acks: &[Ack]) -> ScenarioResult {
    wait_meta_leader(c).await?;
    for p in 0..c.spec().partitions {
        if let Some((_, hw, members)) = settle(c, p).await? {
            let views: Vec<DataDirView> = members.iter().map(|n| c.node_disk(*n)).collect();
            replica::identical_below_hw(&views, PartitionId(p), hw).into_result()?;
        }
    }
    let views: Vec<DataDirView> = c.node_ids().iter().map(|n| c.node_disk(*n)).collect();
    let in_state = |n: u64, p: PartitionId, run: &ostro_types::RunId| {
        c.handle(n)
            .and_then(|h| h.node().handle(p))
            .is_some_and(|ph| matches!(ph.read_run(run), Ok(Some(_))))
    };
    acked_on_disk_with(&views, acks, c.spec().min_insync, in_state).into_result()?;
    let leader = wait_meta_leader(c).await?;
    let dump = read_dump(c, leader);
    // A snapshot that still has every partition unplaced (the bootstrap snapshot) leaves
    // every election in the log; a compaction past elections does not (13-09 note).
    let elections_in_log = dump.snapshot_state.partitions().all(|p| p.value.epoch == 0);
    if elections_in_log {
        for v in &views {
            for p in 0..c.spec().partitions {
                la_rule::check(v, PartitionId(p), &dump.entries).into_result()?;
            }
        }
        election::isr_only_from(dump.snapshot_state.clone(), &dump.entries).into_result()?;
    } else {
        println!(
            "meta log compacted past elections at {} on node {leader}: LA-rule / election audits skipped (13-09 note)",
            dump.snapshot_index
        );
    }
    Ok(())
}

/// Acknowledged runs on every led partition (feeds the on-disk oracle).
pub(super) async fn acked_everywhere(c: &InProcCluster, acks: &mut Vec<Ack>) -> ScenarioResult {
    for p in 0..c.spec().partitions {
        if published(c, p).leader != 0 {
            acks.extend(append_acked(c, p, 2, ACK_T).await?);
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------- workload

/// Runs started round robin on the given nodes every 100 ms (module docs).
pub(super) struct Load {
    clients: Arc<Mutex<Vec<ApiClient>>>,
    acked: Arc<Mutex<Vec<(Instant, String)>>>,
    stop: Arc<AtomicBool>,
    task: JoinHandle<()>,
    t0: Instant,
}

impl Load {
    pub(super) fn start(c: &InProcCluster) -> Self {
        let clients = Arc::new(Mutex::new(Self::clients_of(c, &[])));
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
                    {
                        let started = tokio::time::timeout(
                            Duration::from_secs(5),
                            api.start_run(TEST_WORKFLOW, json!({ "reassign": i }), None),
                        )
                        .await;
                        if let Ok(Ok(ack)) = started {
                            acked
                                .lock()
                                .expect("acked")
                                .push((Instant::now(), ack.run_id)); // clock-exempt: stall measurement
                        }
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };
        Self {
            clients,
            acked,
            stop,
            task,
            t0: Instant::now(), // clock-exempt: stall measurement
        }
    }

    fn clients_of(c: &InProcCluster, exclude: &[u64]) -> Vec<ApiClient> {
        running(c)
            .into_iter()
            .filter(|n| !exclude.contains(n))
            .map(|n| c.client(n))
            .collect()
    }

    /// Point the workload at the running nodes except `exclude`.
    pub(super) fn refresh(&self, c: &InProcCluster, exclude: &[u64]) {
        *self.clients.lock().expect("clients") = Self::clients_of(c, exclude);
    }

    pub(super) async fn wait_acked(&self, k: usize) -> ScenarioResult {
        let base = self.acked.lock().expect("acked").len();
        let t0 = Instant::now(); // clock-exempt: test timeout
        while self.acked.lock().expect("acked").len() < base + k {
            if t0.elapsed() > RUN_T {
                return Err(fail("workload", format!("{k} runs not acknowledged")));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(())
    }

    /// Stop; every acked run must complete. Returns (acked runs, longest gap between two
    /// acknowledged starts, incl. the start of the workload).
    pub(super) async fn finish(
        self,
        c: &InProcCluster,
    ) -> Result<(usize, Duration), ScenarioFailure> {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.task.await;
        let acked = std::mem::take(&mut *self.acked.lock().expect("acked"));
        if acked.is_empty() {
            return Err(fail("workload", "no run was acknowledged"));
        }
        let mut gap = Duration::ZERO;
        let mut prev = self.t0;
        for (t, _) in &acked {
            gap = gap.max(t.saturating_duration_since(prev));
            prev = *t;
        }
        let clients = Self::clients_of(c, &[]);
        let mut bad = Vec::new();
        let t0 = Instant::now(); // clock-exempt: test timeout
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
            if status.as_deref() != Some("completed") {
                bad.push(format!("{run_id}: {status:?}"));
            }
        }
        if bad.is_empty() {
            Ok((acked.len(), gap))
        } else {
            Err(fail(
                "workload",
                format!(
                    "{} of {} acknowledged runs did not complete: {bad:?}",
                    bad.len(),
                    acked.len()
                ),
            ))
        }
    }
}

pub(super) fn no_stall(gap: Duration) -> ScenarioResult {
    if gap > STALL_MAX {
        Err(fail(
            "availability",
            format!("writes stalled {gap:?} (> {STALL_MAX:?}) during the reassignment"),
        ))
    } else {
        Ok(())
    }
}

// ------------------------------------------------------------------------- scenarios

fn rf_raised_on_join(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await?;
        set_policy(c, 3, 2).await?;
        let load = Load::start(c);
        load.wait_acked(3).await?;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        for _ in 0..2 {
            c.add_node(NodeOpts::default())
                .await
                .map_err(|e| fail("reassign", format!("add_node: {e}")))?;
            load.refresh(c, &[]);
        }
        let joined = t0.elapsed();
        wait_parts(c, "every partition at RF 3", MOVE_T, |parts| {
            parts.iter().all(|a| {
                a.replicas.len() == 3 && a.isr.len() == 3 && a.min_insync == 2 && no_ext(a)
            })
        })
        .await?;
        println!(
            "rf_raised_on_join: two joins took {joined:?}; RF 1 → 3 on every partition {:?} after the first join started",
            t0.elapsed()
        );
        load.wait_acked(3).await?;
        let (n, gap) = load.finish(c).await?;
        println!("rf_raised_on_join: {n} runs acknowledged, longest gap {gap:?}");
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await?;
        no_stall(gap)
    })
}

fn drain_evacuates_replicas(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let load = Load::start(c);
        load.refresh(c, &[4]);
        load.wait_acked(3).await?;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        c.drain(4)
            .await
            .map_err(|e| fail("reassign", format!("drain 4: {e}")))?;
        wait_parts(c, "node 4 holds and leads nothing", MOVE_T, |parts| {
            parts.iter().all(|a| {
                !a.replicas.contains(&4) && !a.isr.contains(&4) && a.leader != 4 && no_ext(a)
            })
        })
        .await?;
        println!(
            "drain_evacuates_replicas: node 4 empty in {:?}",
            t0.elapsed()
        );
        for a in published_all(c) {
            if a.replicas.len() != 3 || a.min_insync != 2 {
                return Err(fail("reassign", format!("after the drain: {a:?}")));
            }
        }
        load.wait_acked(3).await?;
        let (n, gap) = load.finish(c).await?;
        println!("drain_evacuates_replicas: {n} runs acknowledged, longest gap {gap:?}");
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await?;
        no_stall(gap)
    })
}

/// A partition whose replicas exclude exactly one running node; `(partition, outsider)`.
pub(super) fn partition_with_outsider(
    c: &InProcCluster,
    skip: &[u16],
) -> Option<(PartitionAssignment, u64)> {
    let nodes = running(c);
    published_all(c).into_iter().find_map(|a| {
        if skip.contains(&a.partition) || a.leader == 0 {
            return None;
        }
        let out: Vec<u64> = nodes
            .iter()
            .copied()
            .filter(|n| !a.replicas.contains(n))
            .collect();
        (out.len() == 1).then(|| (a, out[0]))
    })
}

fn explicit_reassign_and_cancel(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        let load = Load::start(c);
        load.wait_acked(3).await?;
        // Move p to [d, b, c]: the leader a leaves (LeaderMove through the NodeOps handoff).
        let (a, d) = partition_with_outsider(c, &[]).ok_or_else(|| fail("reassign", "no p"))?;
        let target: Vec<u64> = std::iter::once(d)
            .chain(a.replicas.iter().copied().filter(|r| *r != a.leader))
            .collect();
        let via = pick_node(c, &[]).unwrap_or(1);
        let t0 = Instant::now(); // clock-exempt: scenario timing
        reassign_ok(c, via, a.partition, &target, false).await?;
        let p = a.partition;
        let tgt = target.clone();
        wait_parts(c, "explicit move done", MOVE_T, move |parts| {
            let x = &parts[usize::from(p)];
            sorted(&x.replicas) == sorted(&tgt) && tgt.contains(&x.leader) && no_ext(x)
        })
        .await?;
        println!(
            "explicit_reassign_and_cancel: p{p} {:?} → {target:?} in {:?}",
            a.replicas,
            t0.elapsed()
        );
        // Cancel: q → [a, b, d'] with d' held out of the ISR, then cancel before LeaderMove.
        let (q, d2) =
            partition_with_outsider(c, &[p]).ok_or_else(|| fail("reassign", "no second p"))?;
        let removed = *q
            .replicas
            .iter()
            .rev()
            .find(|r| **r != q.leader)
            .expect("rf 3");
        let target2: Vec<u64> = q
            .replicas
            .iter()
            .copied()
            .filter(|r| *r != removed)
            .chain(std::iter::once(d2))
            .collect();
        hold_fetch(c, d2);
        reassign_ok(c, via, q.partition, &target2, false).await?;
        let qp = q.partition;
        wait_parts(c, "second move in flight", MOVE_T, move |parts| {
            !no_ext(&parts[usize::from(qp)])
        })
        .await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mid = published(c, qp);
        if mid.leader != q.leader || mid.isr.contains(&d2) {
            release_fetch(c, d2);
            return Err(fail(
                "reassign",
                format!("CatchUp did not hold while {d2} could not fetch: {mid:?}"),
            ));
        }
        reassign_ok(c, via, qp, &[], true).await?;
        let orig = q.replicas.clone();
        wait_parts(
            c,
            "cancel restored the old replicas",
            MOVE_T,
            move |parts| {
                let x = &parts[usize::from(qp)];
                sorted(&x.replicas) == sorted(&orig) && no_ext(x)
            },
        )
        .await?;
        release_fetch(c, d2);
        // Nothing left to cancel: 409.
        let (st, v) = reassign(c, via, qp, &[], true).await?;
        if st.as_u16() != 409 {
            return Err(fail("reassign", format!("second cancel: {st} {v}")));
        }
        let (n, gap) = load.finish(c).await?;
        println!("explicit_reassign_and_cancel: {n} runs acknowledged, longest gap {gap:?}");
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await
    })
}

fn controller_failover_mid_move(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_level2(c).await?;
        let m = wait_meta_leader(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let load = Load::start(c);
        load.refresh(c, &[m]);
        load.wait_acked(3).await?;
        // A partition where the meta leader m is a non-leading replica: move it to the
        // outsider d (held in CatchUp), removing m.
        let nodes = running(c);
        let a = published_all(c)
            .into_iter()
            .find(|a| a.leader != m && a.replicas.contains(&m))
            .ok_or_else(|| fail("reassign", "no partition with the meta leader as follower"))?;
        let d = *nodes
            .iter()
            .find(|n| !a.replicas.contains(n))
            .expect("4 nodes, rf 3");
        let target: Vec<u64> = a
            .replicas
            .iter()
            .copied()
            .filter(|r| *r != m)
            .chain(std::iter::once(d))
            .collect();
        hold_fetch(c, d);
        let via = nodes.iter().copied().find(|n| *n != m).expect("node");
        reassign_ok(c, via, a.partition, &target, false).await?;
        let p = a.partition;
        wait_parts(c, "move in CatchUp", MOVE_T, move |parts| {
            !no_ext(&parts[usize::from(p)])
        })
        .await?;
        c.kill(m).await;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        let new = wait_meta_leader(c).await?;
        if new == m {
            return Err(fail("reassign", "the killed node still leads meta"));
        }
        release_fetch(c, d);
        let tgt = target.clone();
        wait_parts(
            c,
            "move completed by the new controller",
            MOVE_T,
            move |parts| {
                let x = &parts[usize::from(p)];
                sorted(&x.replicas) == sorted(&tgt) && sorted(&x.isr) == sorted(&tgt) && no_ext(x)
            },
        )
        .await?;
        println!(
            "controller_failover_mid_move: p{p} → {target:?} completed {:?} after killing meta leader {m} (new leader {new})",
            t0.elapsed()
        );
        c.restart(m)
            .await
            .map_err(|e| fail("reassign", format!("restart {m}: {e}")))?;
        let (n, gap) = load.finish(c).await?;
        println!("controller_failover_mid_move: {n} runs acknowledged, longest gap {gap:?}");
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await
    })
}

fn move_blocked_before_finalize(_c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        // Its own cluster: one node at the emulated level 1 + two level-1 joins (finalized 1).
        let mut c = super::version_gate::start_level1_cluster(3)
            .await
            .map_err(|e| fail("reassign", format!("level-1 cluster: {e}")))?;
        let r = blocked_body(&mut c).await;
        c.stop().await;
        r
    })
}

/// A gated refusal: 409 with reason `feature_not_enabled` (13-18 aligned every gate on 409,
/// D-P13-17; the cluster API answered 412 before).
fn feature_not_enabled(st: reqwest::StatusCode, v: &Value) -> bool {
    st.as_u16() == 409 && v["error"]["details"]["reason"] == "feature_not_enabled"
}

async fn blocked_body(c: &mut InProcCluster) -> ScenarioResult {
    wait_meta_leader(c).await?;
    let nodes = running(c);
    let target = nodes.clone();
    for n in &nodes {
        let (st, v) = reassign(c, *n, 0, &target, false).await?;
        if !feature_not_enabled(st, &v) {
            return Err(fail(
                "p13_upgrade",
                format!("node {n}: reassign before finalize answered {st} {v}"),
            ));
        }
        let (st, v) = c
            .client(*n)
            .post_json("/v1/cluster/rebalance", &json!({}))
            .await
            .map_err(|e| fail("reassign", format!("rebalance: {e}")))?;
        if !feature_not_enabled(st, &v) {
            return Err(fail(
                "p13_upgrade",
                format!("node {n}: rebalance before finalize answered {st} {v}"),
            ));
        }
    }
    // The controller runs on the meta leader at level 1 too: give it a few ticks.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let meta = c.meta_log(nodes[0]).await;
    if let Some(e) = meta
        .iter()
        .find(|e| matches!(e.command, MetaCommand::AlterReplicas { .. }))
    {
        return Err(fail(
            "p13_upgrade",
            format!(
                "AlterReplicas committed before finalization at index {}",
                e.index
            ),
        ));
    }
    let mut scans = Vec::new();
    for node in c.node_ids() {
        let view = c.node_disk(node);
        for p in 0..c.spec().partitions {
            if let Ok(s) = scan_partition(&view, PartitionId(p)) {
                scans.push(s);
            }
        }
    }
    let r = crate::oracle::p13_upgrade::no_gated_before_finalize_from(1, &meta, &scans);
    if r.is_ok() {
        Ok(())
    } else {
        Err(fail("p13_upgrade", r.violations.join("; ")))
    }
}

/// Move a partition off node `n` (its last non-leading replica); `(partition, n, target)`.
async fn shrink_off(c: &InProcCluster) -> Result<(u16, u64, Vec<u64>), ScenarioFailure> {
    let (a, d) = partition_with_outsider(c, &[]).ok_or_else(|| fail("reassign", "no p"))?;
    let n = *a
        .replicas
        .iter()
        .rev()
        .find(|r| **r != a.leader)
        .expect("rf 3");
    let target: Vec<u64> = a
        .replicas
        .iter()
        .copied()
        .filter(|r| *r != n)
        .chain(std::iter::once(d))
        .collect();
    let via = pick_node(c, &[]).unwrap_or(1);
    reassign_ok(c, via, a.partition, &target, false).await?;
    let p = a.partition;
    let tgt = target.clone();
    wait_parts(c, "shrink removed the replica", MOVE_T, move |parts| {
        let x = &parts[usize::from(p)];
        sorted(&x.replicas) == sorted(&tgt) && no_ext(x)
    })
    .await?;
    Ok((p, n, target))
}

/// `n`'s role for `p`.
fn role(c: &InProcCluster, n: u64, p: u16) -> Option<PartitionRole> {
    c.handle(n)
        .and_then(|h| h.node().role(PartitionId(p)))
        .map(|r| *r.borrow())
}

/// `n`'s durable end of `p` (offline scan).
fn disk_end(c: &InProcCluster, n: u64, p: u16) -> Result<u64, ScenarioFailure> {
    let s = scan_partition(&c.node_disk(n), PartitionId(p))
        .map_err(|e| fail("disk", format!("node {n} p{p}: {e}")))?;
    Ok(s.records.len() as u64)
}

/// The removed replica: Offline (no fetcher, not served), data retained inert.
async fn removed_checks(c: &InProcCluster, p: u16, n: u64, acks: &mut Vec<Ack>) -> ScenarioResult {
    let t0 = Instant::now(); // clock-exempt: test timeout
    while role(c, n, p) != Some(PartitionRole::Offline) {
        if t0.elapsed() > Duration::from_secs(20) {
            return Err(fail(
                "reassign",
                format!("node {n} p{p} role {:?} after its removal", role(c, n, p)),
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let before = disk_end(c, n, p)?;
    if before == 0 {
        return Err(fail("reassign", format!("node {n} p{p}: no data before")));
    }
    // More acknowledged writes on p: the removed replica neither fetches nor serves them.
    acks.extend(append_acked(c, p, 3, ACK_T).await?);
    tokio::time::sleep(Duration::from_secs(1)).await; // > session_timeout (600 ms)
    let after = disk_end(c, n, p)?;
    if after != before {
        return Err(fail(
            "reassign",
            format!("removed replica {n} p{p} changed on disk: {before} → {after} records"),
        ));
    }
    if role(c, n, p) != Some(PartitionRole::Offline) {
        return Err(fail("reassign", format!("node {n} p{p} left Offline")));
    }
    let led = c
        .handle(n)
        .map(|h| {
            h.node()
                .roles()
                .led()
                .borrow()
                .iter()
                .any(|(q, _)| q.0 == p)
        })
        .unwrap_or(false);
    if led {
        return Err(fail("reassign", format!("removed replica {n} serves p{p}")));
    }
    println!(
        "removed replica {n} p{p}: Offline, {before} records retained inert (Phase 12: data kept, never deleted)"
    );
    Ok(())
}

fn removed_replica_stops_and_cleans(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let load = Load::start(c);
        load.wait_acked(3).await?;
        let (p, n, _) = shrink_off(c).await?;
        removed_checks(c, p, n, &mut acks).await?;
        let (cnt, gap) = load.finish(c).await?;
        println!("removed_replica_stops_and_cleans: {cnt} runs acknowledged, longest gap {gap:?}");
        reassign_oracles(c, &acks).await
    })
}

fn readded_replica_truncates(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let load = Load::start(c);
        load.wait_acked(3).await?;
        let (p, n, target) = shrink_off(c).await?;
        removed_checks(c, p, n, &mut acks).await?;
        // Leadership moves while n is out (a new epoch n never saw), more writes land.
        let cur = published(c, p);
        let to = *cur
            .isr
            .iter()
            .find(|r| **r != cur.leader)
            .ok_or_else(|| fail("reassign", format!("p{p} has a single ISR member")))?;
        let via = pick_node(c, &[]).unwrap_or(1);
        // Move the leader with a target that drops the old leader (LeaderMove, new epoch).
        let old_leader = cur.leader;
        let mid: Vec<u64> = std::iter::once(to)
            .chain(
                target
                    .iter()
                    .copied()
                    .filter(|r| *r != old_leader && *r != to),
            )
            .collect();
        let mut mid_full = mid.clone();
        if mid_full.len() < 2 {
            mid_full.push(old_leader);
        }
        reassign_ok(c, via, p, &mid_full, false).await?;
        let m2 = mid_full.clone();
        wait_parts(c, "leader moved while n is out", MOVE_T, move |parts| {
            let x = &parts[usize::from(p)];
            sorted(&x.replicas) == sorted(&m2) && x.leader == to && no_ext(x)
        })
        .await?;
        acks.extend(append_acked(c, p, 3, ACK_T).await?);
        // Re-add n (and the old leader): n catches up from its stale log.
        let back: Vec<u64> = std::iter::once(to)
            .chain(std::iter::once(n))
            .chain(mid_full.iter().copied().filter(|r| *r != to && *r != n))
            .take(3)
            .collect();
        let t0 = Instant::now(); // clock-exempt: scenario timing
        reassign_ok(c, via, p, &back, false).await?;
        let b2 = back.clone();
        wait_parts(c, "n re-added and in the ISR", MOVE_T, move |parts| {
            let x = &parts[usize::from(p)];
            sorted(&x.replicas) == sorted(&b2) && x.isr.contains(&n) && no_ext(x)
        })
        .await?;
        println!(
            "readded_replica_truncates: node {n} back in p{p}'s ISR {:?} after {:?}",
            back,
            t0.elapsed()
        );
        if role(c, n, p).is_none_or(|r| matches!(r, PartitionRole::Offline)) {
            return Err(fail(
                "reassign",
                format!("re-added {n} p{p} is not following"),
            ));
        }
        let (cnt, gap) = load.finish(c).await?;
        println!("readded_replica_truncates: {cnt} runs acknowledged, longest gap {gap:?}");
        // Replica identity below the HW over the ISR (incl. n) is the truncation check.
        reassign_oracles(c, &acks).await
    })
}

fn force_removed_dead_node_evacuated(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let dead = pick_node(c, &[]).ok_or_else(|| fail("reassign", "no follower node"))?;
        let held: BTreeSet<u16> = published_all(c)
            .iter()
            .filter(|a| a.replicas.contains(&dead))
            .map(|a| a.partition)
            .collect();
        if held.is_empty() {
            return Err(fail("reassign", format!("node {dead} holds no replica")));
        }
        let load = Load::start(c);
        load.refresh(c, &[dead]);
        load.wait_acked(3).await?;
        c.kill(dead).await;
        tokio::time::sleep(Duration::from_secs(3)).await; // liveness declares it dead
        c.remove(dead, true)
            .await
            .map_err(|e| fail("reassign", format!("force remove {dead}: {e}")))?;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        wait_parts(
            c,
            "the dead node's replicas re-placed",
            MOVE_T,
            move |parts| {
                parts.iter().all(|a| {
                    !a.replicas.contains(&dead)
                        && !a.isr.contains(&dead)
                        && a.replicas.len() == 3
                        && no_ext(a)
                })
            },
        )
        .await?;
        println!(
            "force_removed_dead_node_evacuated: {} partitions of node {dead} re-placed in {:?}",
            held.len(),
            t0.elapsed()
        );
        let (cnt, gap) = load.finish(c).await?;
        println!("force_removed_dead_node_evacuated: {cnt} runs acknowledged, longest gap {gap:?}");
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await
    })
}
