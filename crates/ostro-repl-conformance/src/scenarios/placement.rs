//! CLUS-01 core group `placement` (12-06): a fresh RF=3 cluster places every partition on
//! three distinct replicas, preferred leader first, leaders spread within one; an RF=1 node
//! places every partition on itself.
//!
//! This module also hosts [`support`], the measured helpers the 12-06 core groups share
//! (`scenarios/mod.rs` is owned by 12-02, so the helpers live in the first core group).

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::BoxFuture;

use super::{DEFAULT_DEADLINE, Scenario, ScenarioResult, Tier};
use crate::harness::ClusterSpec;
use crate::harness::inproc::InProcCluster;
use support::{fail, final_checks, published, read_dump};

/// The `placement` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "placement_rf3_three_replicas",
            group: "placement",
            tier: Tier::B,
            deadline: DEFAULT_DEADLINE,
            spec: rf3,
            run: placement_rf3_three_replicas,
        },
        Scenario {
            name: "placement_rf1_single_node",
            group: "placement",
            tier: Tier::B,
            deadline: DEFAULT_DEADLINE,
            spec: ClusterSpec::single,
            run: placement_rf1_single_node,
        },
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 601,
        ..ClusterSpec::default()
    }
}

/// B2: every partition, on every node's committed meta state, has `rf` distinct replicas
/// at epoch >= 1 with `leader == replicas[0]`; leaders are spread within one.
fn placement_rf3_three_replicas(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let spec = c.spec().clone();
        let nodes = crate::harness::ClusterHarness::node_ids(&*c);
        for n in &nodes {
            let dump = read_dump(c, *n);
            let mut leaders: BTreeMap<u64, usize> = nodes.iter().map(|n| (*n, 0)).collect();
            let mut seen = 0usize;
            for a in dump.state.partitions().map(|v| &v.value) {
                seen += 1;
                let at = format!("node {n} partition {}", a.partition);
                let mut distinct = a.replicas.clone();
                distinct.sort_unstable();
                distinct.dedup();
                if a.replicas.len() != spec.rf || distinct.len() != spec.rf {
                    return Err(fail(
                        "placement",
                        format!(
                            "{at}: replicas {:?} are not {} distinct nodes",
                            a.replicas, spec.rf
                        ),
                    ));
                }
                if let Some(bad) = a.replicas.iter().find(|r| !nodes.contains(r)) {
                    return Err(fail(
                        "placement",
                        format!("{at}: replica {bad} is not a cluster node ({nodes:?})"),
                    ));
                }
                if a.epoch < 1 {
                    return Err(fail("placement", format!("{at}: epoch {} < 1", a.epoch)));
                }
                if a.replicas.first() != Some(&a.leader) {
                    return Err(fail(
                        "placement",
                        format!(
                            "{at}: leader {} is not the preferred replica (replicas {:?})",
                            a.leader, a.replicas
                        ),
                    ));
                }
                *leaders.entry(a.leader).or_default() += 1;
            }
            if seen != usize::from(spec.partitions) {
                return Err(fail(
                    "placement",
                    format!(
                        "node {n}: {seen} partitions placed, expected {}",
                        spec.partitions
                    ),
                ));
            }
            let max = leaders.values().max().copied().unwrap_or(0);
            let min = leaders.values().min().copied().unwrap_or(0);
            if max - min > 1 {
                return Err(fail(
                    "placement",
                    format!("node {n}: leaders per node {leaders:?} are not spread within one"),
                ));
            }
        }
        final_checks(c, true).await
    })
}

/// RF=1: every partition is `replicas [1]`, `isr [1]`, leader 1, epoch >= 1, and a workload
/// is acknowledged onto disk under the LA rule (not pending: passes on the current build).
fn placement_rf1_single_node(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let spec = c.spec().clone();
        for p in 0..spec.partitions {
            let a = published(c, p);
            if a.replicas != [1] || a.isr != [1] || a.leader != 1 || a.epoch < 1 {
                return Err(fail(
                    "placement",
                    format!(
                        "partition {p}: replicas {:?} isr {:?} leader {} epoch {}, expected [1] [1] 1 >=1",
                        a.replicas, a.isr, a.leader, a.epoch
                    ),
                ));
            }
        }
        final_checks(c, true).await
    })
}

/// Measured helpers shared by the 12-06 core groups. Everything is read from disk (verified
/// WAL scans, the committed meta log) or checked through an oracle; the one self-report
/// used is the leader's HW from `GET /v1/admin/replication`, and it is only ever compared
/// against measured durable ends.
///
/// Harness-side failures in a body (I/O, a task that cannot be joined) panic: the runner
/// reports them as `HarnessError::Panic`, never as red evidence. Behaviour the cluster owes
/// (an ack within a bound, a leader within a bound) fails with a named
/// [`ScenarioFailure`](crate::scenarios::ScenarioFailure).
pub(crate) mod support {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use ostro_meta::{MetaCommand, PartitionAssignment};
    use ostro_types::{Offset, PartitionCount, PartitionId};
    use serde_json::{Value, json};
    use tokio::task::JoinHandle;
    use tokio::time::Instant;

    use crate::harness::fault::{Fault, RpcKind};
    use crate::harness::inproc::{Ack, InProcCluster, MetaLogDump, TEST_WORKFLOW, read_meta_log};
    use crate::harness::{ApiClient, ClusterHarness, DataDirView};
    use crate::oracle::disk::{LogScan, acked_on_disk, scan_partition};
    use crate::oracle::{election, la_rule, replica};
    use crate::scenarios::{ScenarioFailure, ScenarioResult};

    /// `[replication] replica_lag_max` of the tier-B node config.
    pub(crate) const LAG_MAX: Duration = Duration::from_secs(2);
    /// `[replication] session_timeout` of the tier-B node config.
    pub(crate) const SESSION: Duration = Duration::from_millis(600);
    /// Bound on one acknowledged append on a healthy partition.
    pub(crate) const ACK_T: Duration = Duration::from_secs(15);
    /// Bound on a leadership change or an ISR change.
    pub(crate) const CHANGE_T: Duration = Duration::from_secs(20);
    /// Bound on replicas settling (equal durable ends, HW at the end).
    pub(crate) const SETTLE_T: Duration = Duration::from_secs(30);
    /// Poll step.
    pub(crate) const STEP: Duration = Duration::from_millis(50);

    /// A named measured failure.
    pub(crate) fn fail(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
        ScenarioFailure {
            oracle,
            detail: detail.into(),
        }
    }

    /// Unwrap a harness-side result; an error panics (reported as a harness error).
    pub(crate) fn harness<T, E: std::fmt::Display>(what: &str, r: Result<T, E>) -> T {
        match r {
            Ok(v) => v,
            Err(e) => panic!("harness: {what}: {e}"),
        }
    }

    /// `node`'s committed meta log, decoded offline (panics when unreadable: an oracle never
    /// runs on a silently empty log).
    pub(crate) fn read_dump(c: &InProcCluster, node: u64) -> MetaLogDump {
        let dir = c.node_disk(node).root.join("meta");
        harness(&format!("node {node} meta log"), read_meta_log(&dir))
    }

    /// Nodes currently running.
    pub(crate) fn live(c: &InProcCluster) -> Vec<u64> {
        c.node_ids()
            .into_iter()
            .filter(|n| c.handle(*n).is_some())
            .collect()
    }

    /// The running node that is the metadata Raft leader, if any.
    pub(crate) fn meta_leader(c: &InProcCluster) -> Option<u64> {
        live(c)
            .into_iter()
            .find(|n| c.handle(*n).is_some_and(|h| h.meta().is_leader()))
    }

    /// Wait for a metadata leader.
    pub(crate) async fn wait_meta_leader(c: &InProcCluster) -> Result<u64, ScenarioFailure> {
        poll(CHANGE_T, || meta_leader(c)).await.ok_or_else(|| {
            fail(
                "availability",
                format!("no meta leader within {CHANGE_T:?}"),
            )
        })
    }

    /// The node whose committed meta log is authoritative right now: the meta leader, else
    /// the first live node.
    fn authority(c: &InProcCluster) -> u64 {
        meta_leader(c)
            .or_else(|| live(c).first().copied())
            .expect("harness: no live node")
    }

    /// Partition `p`'s committed assignment, measured from the authoritative node's meta log
    /// on disk (an empty record when absent).
    pub(crate) fn published(c: &InProcCluster, p: u16) -> PartitionAssignment {
        read_dump(c, authority(c))
            .state
            .partition(p)
            .map(|v| v.value.clone())
            .unwrap_or_else(|| PartitionAssignment {
                partition: p,
                ..PartitionAssignment::default()
            })
    }

    /// Every partition's committed assignment.
    pub(crate) fn published_all(c: &InProcCluster) -> Vec<PartitionAssignment> {
        (0..c.spec().partitions).map(|p| published(c, p)).collect()
    }

    /// The meta leader's committed log, as the oracles want it.
    pub(crate) fn meta_dump(c: &InProcCluster) -> MetaLogDump {
        read_dump(c, authority(c))
    }

    /// Offline verified scan of `node`'s partition `p`.
    pub(crate) fn scan(c: &InProcCluster, node: u64, p: u16) -> Result<LogScan, ScenarioFailure> {
        scan_partition(&c.node_disk(node), PartitionId(p)).map_err(|e| {
            fail(
                "disk",
                format!("node {node} partition {p}: scan failed: {e}"),
            )
        })
    }

    /// `node`'s measured durable end of `p` (0 for an empty log).
    pub(crate) fn durable_end(
        c: &InProcCluster,
        node: u64,
        p: u16,
    ) -> Result<Offset, ScenarioFailure> {
        Ok(scan(c, node, p)?.durable_end().unwrap_or(Offset::ZERO))
    }

    /// The HW `node` reports for `p` in `GET /v1/admin/replication` (`None` when the route
    /// or the field is absent).
    pub(crate) async fn reported_hw(c: &InProcCluster, node: u64, p: u16) -> Option<Offset> {
        let v = c.admin_status(node).await;
        v["partitions"]
            .as_array()?
            .iter()
            .find(|e| e["partition"].as_u64() == Some(u64::from(p)))?["hw"]
            .as_u64()
            .map(Offset)
    }

    /// The current leader of `p` (published) and its reported HW.
    pub(crate) async fn leader_hw(
        c: &InProcCluster,
        p: u16,
    ) -> Result<(u64, Offset), ScenarioFailure> {
        let a = published(c, p);
        if a.leader == 0 {
            return Err(fail("availability", format!("partition {p} has no leader")));
        }
        match reported_hw(c, a.leader, p).await {
            Some(hw) => Ok((a.leader, hw)),
            None => Err(fail(
                "status",
                format!("partition {p}: leader {} reports no HW", a.leader),
            )),
        }
    }

    /// One HW-bound sample: `node`'s durable end measured before and after reading the
    /// leader's HW. Only a sample where both reads agree is meaningful (`None` otherwise):
    /// then the durable end held for the whole time the HW was read.
    pub(crate) async fn hw_vs_durable(
        c: &InProcCluster,
        p: u16,
        leader: u64,
        node: u64,
    ) -> Result<Option<(Offset, Offset)>, ScenarioFailure> {
        let before = durable_end(c, node, p)?;
        let Some(hw) = reported_hw(c, leader, p).await else {
            return Err(fail(
                "status",
                format!("partition {p}: leader {leader} reports no HW"),
            ));
        };
        let after = durable_end(c, node, p)?;
        Ok((before == after).then_some((hw, before)))
    }

    /// Poll `f` every [`STEP`] until it yields a value or `limit` passes.
    pub(crate) async fn poll<T>(limit: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
        // clock-exempt: test deadline
        let end = Instant::now() + limit;
        loop {
            if let Some(v) = f() {
                return Some(v);
            }
            if Instant::now() >= end {
                return None;
            }
            tokio::time::sleep(STEP).await;
        }
    }

    /// [`poll`] with an async probe.
    pub(crate) async fn poll_async<T, F>(limit: Duration, mut f: impl FnMut() -> F) -> Option<T>
    where
        F: Future<Output = Option<T>>,
    {
        // clock-exempt: test deadline
        let end = Instant::now() + limit;
        loop {
            if let Some(v) = f().await {
                return Some(v);
            }
            if Instant::now() >= end {
                return None;
            }
            tokio::time::sleep(STEP).await;
        }
    }

    /// Sleep (test pacing only).
    pub(crate) async fn pause(d: Duration) {
        // clock-exempt: test pacing
        tokio::time::sleep(d).await;
    }

    /// Wait until `p`'s published assignment satisfies `pred`.
    pub(crate) async fn wait_assignment(
        c: &InProcCluster,
        p: u16,
        what: &str,
        limit: Duration,
        pred: impl Fn(&PartitionAssignment) -> bool,
    ) -> Result<PartitionAssignment, ScenarioFailure> {
        let got = poll(limit, || {
            let a = published(c, p);
            pred(&a).then_some(a)
        })
        .await;
        got.ok_or_else(|| {
            let a = published(c, p);
            fail(
                "election",
                format!(
                    "partition {p}: not {what} within {limit:?} (leader {} epoch {} isr {:?})",
                    a.leader, a.epoch, a.isr
                ),
            )
        })
    }

    /// Same members, any order.
    pub(crate) fn same_set(a: &[u64], b: &[u64]) -> bool {
        let (mut a, mut b) = (a.to_vec(), b.to_vec());
        a.sort_unstable();
        b.sort_unstable();
        a == b
    }

    /// Fault helpers on the shared fault layer.
    pub(crate) fn blackhole(c: &InProcCluster, from: u64, to: u64, kind: RpcKind) {
        c.net().faults().set(from, to, kind, Fault::Blackhole);
    }

    /// Delay one RPC kind on a directed link.
    pub(crate) fn delay(c: &InProcCluster, from: u64, to: u64, kind: RpcKind, ms: u64) {
        c.net().faults().set(from, to, kind, Fault::Delay(ms));
    }

    /// Remove every rule of a directed link.
    pub(crate) fn clear(c: &InProcCluster, from: u64, to: u64) {
        c.net().faults().clear_link(from, to);
    }

    // ---------------------------------------------------------------------------------
    // Workloads
    // ---------------------------------------------------------------------------------

    /// Deploy the test module (meta leader first, then any live node) and wait until the
    /// workflow is visible.
    pub(crate) async fn deploy(c: &InProcCluster) -> ScenarioResult {
        let first = wait_meta_leader(c).await?;
        let mut order = vec![first];
        order.extend(live(c).into_iter().filter(|n| *n != first));
        let order = &order;
        let done = poll_async(CHANGE_T, || async move {
            for n in order {
                if c.client(*n).deploy_test_module().await.is_ok() {
                    return Some(());
                }
            }
            None
        })
        .await;
        done.ok_or_else(|| fail("availability", "the test module could not be deployed"))
    }

    /// A fresh idempotency key routed to partition `p` (keyed routing).
    pub(crate) fn key_for(c: &InProcCluster, p: u16, tag: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
        loop {
            let i = N.fetch_add(1, Ordering::Relaxed);
            let key = format!("{tag}-{}-{i}", c.cluster_id());
            if ostro_types::routing::partition_for_key(TEST_WORKFLOW, key.as_bytes(), count).0 == p
            {
                return key;
            }
        }
    }

    /// How one start request ended.
    #[derive(Debug)]
    pub(crate) enum Start {
        /// Acknowledged (200/201 with a run id).
        Acked(Ack),
        /// Answered with an error status.
        Rejected(u16, String),
        /// No answer (timeout, connection error).
        NoAnswer(String),
    }

    impl Start {
        /// One line for failure details.
        pub(crate) fn describe(&self) -> String {
            match self {
                Start::Acked(a) => format!("acknowledged {}", a.run_id),
                Start::Rejected(st, body) => format!("rejected {st}: {body}"),
                Start::NoAnswer(e) => format!("no answer: {e}"),
            }
        }
    }

    /// `POST /v1/workflows/<test>/runs` with idempotency key `key`, bounded by `limit`.
    pub(crate) async fn start_keyed(api: &ApiClient, key: &str, limit: Duration) -> Start {
        let body = json!({ "input": { "k": key }, "idempotency_key": key });
        let sent = api
            .http
            .post(format!(
                "{}/v1/workflows/{TEST_WORKFLOW}/runs",
                api.base_url
            ))
            .timeout(limit)
            .json(&body)
            .send()
            .await;
        let r = match sent {
            Ok(r) => r,
            Err(e) => return Start::NoAnswer(e.to_string()),
        };
        let st = r.status();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        if !st.is_success() {
            return Start::Rejected(st.as_u16(), v.to_string());
        }
        let Some(run_id) = v["run_id"].as_str() else {
            return Start::Rejected(st.as_u16(), format!("no run_id in {v}"));
        };
        match run_id.parse::<ostro_types::RunId>() {
            Ok(id) => Start::Acked(Ack {
                run_id: run_id.to_owned(),
                partition: id.partition().0,
            }),
            Err(e) => Start::Rejected(st.as_u16(), format!("run id {run_id}: {e}")),
        }
    }

    /// Start `k` runs on partition `p` through its current leader; each must be
    /// acknowledged within `limit` (rejections and leader moves are retried with the same
    /// idempotency key).
    pub(crate) async fn append_acked(
        c: &InProcCluster,
        p: u16,
        k: usize,
        limit: Duration,
    ) -> Result<Vec<Ack>, ScenarioFailure> {
        let mut acks = Vec::with_capacity(k);
        for _ in 0..k {
            let key = key_for(c, p, "ack");
            // clock-exempt: test deadline
            let end = Instant::now() + limit;
            let mut last = String::from("no leader");
            loop {
                let leader = published(c, p).leader;
                if leader != 0 && c.handle(leader).is_some() {
                    let left = end.saturating_duration_since(Instant::now()).max(STEP);
                    match start_keyed(&c.client(leader), &key, left).await {
                        Start::Acked(a) => {
                            acks.push(a);
                            break;
                        }
                        other => last = other.describe(),
                    }
                }
                if Instant::now() >= end {
                    return Err(fail(
                        "availability",
                        format!(
                            "partition {p}: run {key} not acknowledged within {limit:?}: {last}"
                        ),
                    ));
                }
                pause(STEP * 2).await;
            }
        }
        Ok(acks)
    }

    /// Fire `k` start requests at `node` for partition `p` without waiting (each bounded by
    /// `limit`). Returns the keys and the request tasks.
    pub(crate) fn spawn_starts(
        c: &InProcCluster,
        node: u64,
        p: u16,
        k: usize,
        tag: &str,
        limit: Duration,
    ) -> (Vec<String>, Vec<JoinHandle<Start>>) {
        let api = c.client(node);
        let keys: Vec<String> = (0..k).map(|_| key_for(c, p, tag)).collect();
        let tasks = keys
            .iter()
            .map(|key| {
                let (api, key) = (api.clone(), key.clone());
                tokio::spawn(async move { start_keyed(&api, &key, limit).await })
            })
            .collect();
        (keys, tasks)
    }

    /// Join request tasks (a panicked task is a harness error).
    pub(crate) async fn join_starts(tasks: Vec<JoinHandle<Start>>) -> Vec<Start> {
        let mut out = Vec::with_capacity(tasks.len());
        for t in tasks {
            out.push(harness("start request task", t.await));
        }
        out
    }

    /// Whether `needle` occurs in `hay`.
    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
    }

    /// Whether the scanned log holds a command carrying idempotency key `key` below `below`.
    pub(crate) fn holds_key(scan: &LogScan, key: &str, below: Offset) -> bool {
        scan.commands()
            .any(|(off, cmd)| off < below && contains(cmd, key.as_bytes()))
    }

    /// Wait until `node`'s log of `p` holds every key in `keys` (unreplicated appends have
    /// landed on the leader's disk).
    pub(crate) async fn wait_keys_on_disk(
        c: &InProcCluster,
        node: u64,
        p: u16,
        keys: &[String],
        limit: Duration,
    ) -> Result<(), ScenarioFailure> {
        let landed = poll(limit, || {
            let s = scan(c, node, p).ok()?;
            keys.iter()
                .all(|k| holds_key(&s, k, Offset(u64::MAX)))
                .then_some(())
        })
        .await;
        landed.ok_or_else(|| {
            fail(
                "disk",
                format!("node {node} partition {p}: the {} appended commands never reached its log within {limit:?}", keys.len()),
            )
        })
    }

    /// No node's committed log (below the HW) holds any of `keys`.
    pub(crate) fn none_committed(
        c: &InProcCluster,
        p: u16,
        hw: Offset,
        keys: &[String],
    ) -> ScenarioResult {
        for n in c.node_ids() {
            let s = scan(c, n, p)?;
            if let Some(k) = keys.iter().find(|k| holds_key(&s, k, hw)) {
                return Err(fail(
                    "replica",
                    format!(
                        "node {n} partition {p}: unacknowledged divergent command {k} is below HW {}",
                        hw.0
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Wait until every acknowledged run is terminal `completed` on its partition's leader.
    pub(crate) async fn wait_completed(
        c: &InProcCluster,
        acks: &[Ack],
        limit: Duration,
    ) -> ScenarioResult {
        for a in acks {
            let done = poll_async(limit, || async move {
                let leader = published(c, a.partition).leader;
                if leader == 0 || c.handle(leader).is_none() {
                    return None;
                }
                let (st, v) = c
                    .client(leader)
                    .get_json(&format!("/v1/runs/{}", a.run_id))
                    .await
                    .ok()?;
                (st.is_success() && v["status"] == "completed").then_some(())
            })
            .await;
            if done.is_none() {
                return Err(fail(
                    "availability",
                    format!(
                        "acknowledged run {} (partition {}) not completed within {limit:?}",
                        a.run_id, a.partition
                    ),
                ));
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------------------------------
    // End-of-scenario oracles
    // ---------------------------------------------------------------------------------

    /// Wait until partition `p` is settled: its live ISR members' measured durable ends all
    /// equal the leader's, and the leader's HW is that end (RF=1 with no status route: the
    /// leader's durable end). Two equal consecutive samples are required. Returns
    /// `(leader, hw, live ISR members)`; `None` leader when `p` is offline.
    pub(crate) async fn settle(
        c: &InProcCluster,
        p: u16,
    ) -> Result<Option<(u64, Offset, Vec<u64>)>, ScenarioFailure> {
        let mut prev: Option<(u64, Offset, Vec<u64>)> = None;
        // clock-exempt: test deadline
        let end = Instant::now() + SETTLE_T;
        loop {
            let a = published(c, p);
            if a.leader == 0 {
                return Ok(None);
            }
            let members: Vec<u64> = a
                .isr
                .iter()
                .copied()
                .filter(|n| c.handle(*n).is_some())
                .collect();
            let mut ends = BTreeMap::new();
            for n in &members {
                ends.insert(*n, durable_end(c, *n, p)?);
            }
            let leader_end = ends.get(&a.leader).copied();
            let hw = match reported_hw(c, a.leader, p).await {
                Some(hw) => Some(hw),
                None if a.replicas.len() == 1 => leader_end,
                None => {
                    return Err(fail(
                        "status",
                        format!("partition {p}: leader {} reports no HW", a.leader),
                    ));
                }
            };
            let sample = match (leader_end, hw) {
                (Some(e), Some(hw)) if hw == e && ends.values().all(|x| *x == e) => {
                    Some((a.leader, hw, members.clone()))
                }
                _ => None,
            };
            let last = format!(
                "leader {} isr {:?} durable ends {ends:?} hw {hw:?}",
                a.leader, a.isr
            );
            if sample.is_some() && sample == prev {
                return Ok(sample);
            }
            prev = sample;
            if Instant::now() >= end {
                return Err(fail(
                    "replica",
                    format!("partition {p}: replicas did not settle within {SETTLE_T:?}: {last}"),
                ));
            }
            pause(Duration::from_millis(150)).await;
        }
    }

    /// The oracles every core scenario ends with: replica identity below the HW over each
    /// partition's live ISR, the LA rule on every node's log, the ISR-only election audit of
    /// the committed meta log, and (for `acks`) acknowledged ⇒ on disk on >= `min_insync`
    /// nodes. With `workload`, first deploys the test module and appends two acknowledged
    /// runs to every partition (so the disk oracle never passes vacuously).
    pub(crate) async fn final_checks(c: &InProcCluster, workload: bool) -> ScenarioResult {
        final_checks_with(c, workload, &[]).await
    }

    /// [`final_checks`] plus earlier acknowledgements `acks`.
    pub(crate) async fn final_checks_with(
        c: &InProcCluster,
        workload: bool,
        acks: &[Ack],
    ) -> ScenarioResult {
        wait_meta_leader(c).await?;
        let mut acks = acks.to_vec();
        if workload {
            deploy(c).await?;
            for p in 0..c.spec().partitions {
                if published(c, p).leader != 0 {
                    acks.extend(append_acked(c, p, 2, ACK_T).await?);
                }
            }
        }
        for p in 0..c.spec().partitions {
            if let Some((_, hw, members)) = settle(c, p).await? {
                let views: Vec<DataDirView> = members.iter().map(|n| c.node_disk(*n)).collect();
                replica::identical_below_hw(&views, PartitionId(p), hw).into_result()?;
            }
        }
        let views: Vec<DataDirView> = c.node_ids().iter().map(|n| c.node_disk(*n)).collect();
        acked_on_disk(&views, &acks, c.spec().min_insync).into_result()?;
        let dump = meta_dump(c);
        for v in &views {
            for p in 0..c.spec().partitions {
                la_rule::check(v, PartitionId(p), &dump.entries).into_result()?;
            }
        }
        election::isr_only_from(dump.snapshot_state.clone(), &dump.entries).into_result()
    }

    /// The election audit alone (run first where a wrong leader is the expected symptom, so
    /// the failure names the election oracle).
    pub(crate) fn election_audit(c: &InProcCluster) -> ScenarioResult {
        let dump = meta_dump(c);
        election::isr_only_from(dump.snapshot_state, &dump.entries).into_result()
    }

    /// Committed `ElectLeader` proposals for `p` (index, epoch, leader), applied or not.
    pub(crate) fn elections(c: &InProcCluster, p: u16) -> Vec<(u64, u32, u64)> {
        meta_dump(c)
            .entries
            .iter()
            .filter_map(|e| match &e.command {
                MetaCommand::ElectLeader {
                    partition,
                    epoch,
                    leader,
                    ..
                } if *partition == p => Some((e.index, *epoch, *leader)),
                _ => None,
            })
            .collect()
    }

    /// A partition (published) satisfying `pred`, lowest id first.
    pub(crate) fn pick(
        c: &InProcCluster,
        what: &str,
        pred: impl Fn(&PartitionAssignment) -> bool,
    ) -> Result<PartitionAssignment, ScenarioFailure> {
        published_all(c)
            .into_iter()
            .find(|a| pred(a))
            .ok_or_else(|| fail("placement", format!("no partition {what}")))
    }

    /// Await a future under a bound; `None` on timeout.
    pub(crate) async fn within<F: Future>(limit: Duration, f: F) -> Option<F::Output> {
        // clock-exempt: test deadline
        tokio::time::timeout(limit, f).await.ok()
    }
}

/// Deadline of the core scenarios that kill, restart or wait out ISR changes.
pub(crate) const LONG_DEADLINE: Duration = Duration::from_secs(240);

/// A scenario body (the [`Scenario::run`] type).
pub(crate) type Run = for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>;
