//! CLUS-02 SC2 with real process death (12-17, tier C): the `process` scenario group runs on
//! [`ProcCluster`] (real `ostro server` processes behind the chaos proxy, `kill -9` is
//! SIGKILL), where an in-process "crash" would keep the page cache and the threads
//! (research Pitfall 8).
//!
//! - [`process_leader_dies_with_unreplicated_writes`]: the followers' links to the leader
//!   are blackholed (no FIN, no RST: nothing it appends can be fetched), the leader takes K
//!   unacknowledged commands onto its own disk and is `kill -9`'d; after the failover and its
//!   restart its divergent tail is truncated: replicas byte-identical below the HW, none of
//!   the K commands below the HW on any node, every acknowledged run on disk on
//!   `min_insync` nodes, the LA rule on every log and an ISR-only election audit.
//! - [`follower_power_loss`]: follower G is `kill -9`'d (the ISR becomes {leader, F}),
//!   follower F is restarted with `wal::write=return` (its WAL writer fails every write
//!   before writing a byte, so nothing it fetches from then on reaches its files), load is
//!   offered, F and then the leader are `kill -9`'d, G and F restart without failpoints. On
//!   a correct build F never counts as durable, so nothing is acknowledged that only the
//!   leader holds; with `repl::mutant::count_nondurable_leo` (follower and leader) the
//!   leader acknowledges on F's fetched LEO, F is elected from the ISR without those
//!   records, and the disk oracle reports the acknowledged loss. (A `pause` failpoint would
//!   block the process's runtime worker threads and starve its metadata Raft; the EIO
//!   before the write has the same on-disk effect without blocking.)
//!
//! [`tools`] are the measured tier-C helpers these scenarios share with the measurement
//! tests (`failover_p99`, `bench_rf3`, `cluster_campaign`): committed assignments from the
//! nodes' meta logs on disk, the meta leader from `GET /v1/admin/replication`, keyed starts
//! that follow 503 leader hints, settle and the end-of-scenario oracles.

use std::time::Duration;

use futures_util::future::BoxFuture;

use super::runner::{Outcome, run_on_cluster};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::inproc::Ack;
use crate::harness::process::{ProcCluster, ProcOptions};
use crate::harness::{ClusterHarness, ClusterSpec};
use crate::oracle::cluster_formed::FORMATION_DEADLINE;
use crate::oracle::disk::acked_on_disk;
use tools::{
    ACK_T, CHANGE_T, Start, append_acked, deploy, fail, final_checks, join_starts, lag_max,
    none_committed, pick, published, reported_hw, session, settle, spawn_starts, wait_assignment,
    wait_completed, wait_keys_on_disk, wait_meta_leader,
};

/// Deadline of a tier-C scenario (process restarts, ISR changes, settle).
pub const PROC_DEADLINE: Duration = Duration::from_secs(300);

/// The `process` scenarios (tier C).
pub fn scenarios() -> Vec<Scenario<ProcCluster>> {
    vec![
        Scenario {
            name: "process_leader_dies_with_unreplicated_writes",
            group: "process",
            tier: Tier::C,
            deadline: PROC_DEADLINE,
            spec: divergence_spec,
            run: process_leader_dies_with_unreplicated_writes,
        },
        Scenario {
            name: "follower_power_loss",
            group: "process",
            tier: Tier::C,
            deadline: PROC_DEADLINE,
            spec: power_loss_spec,
            run: follower_power_loss,
        },
    ]
}

/// The tier-C scenario named `name`.
pub fn scenario(name: &str) -> Option<Scenario<ProcCluster>> {
    scenarios().into_iter().find(|s| s.name == name)
}

/// Why a tier-C test cannot run on this host (`None` = it can): no failpoint binary, or no
/// `127.r.r.x` loopback aliases (macOS).
pub fn skip_reason() -> Option<String> {
    if crate::harness::process::failpoint_bin().is_none() {
        return Some(crate::harness::process::SKIP_NO_BIN.to_owned());
    }
    if !crate::harness::process::loopback_aliases_supported() {
        return Some("SKIP: loopback aliases 127.r.r.x cannot be bound on this host".into());
    }
    None
}

/// Run tier-C scenario `s` once on a fresh cluster started with `opts` (per-process
/// failpoints): the measured formation precondition, the body under its deadline, then the
/// cluster is stopped. A failing run keeps its directory (node logs) and prints the path.
pub async fn run_proc(s: &Scenario<ProcCluster>, opts: ProcOptions) -> Outcome {
    let mut c = match ProcCluster::start_with((s.spec)(), opts).await {
        Ok(c) => c,
        Err(e) => return Outcome::Harness(e),
    };
    let outcome = run_on_cluster(&mut c, s, FORMATION_DEADLINE).await;
    if matches!(outcome, Outcome::Passed) {
        c.stop().await;
    } else if std::env::var("OSTRO_PROC_KEEP").is_ok_and(|v| v == "1") {
        let kept = c.stop_keeping_dirs().keep();
        println!(
            "{}: cluster kept for post-mortem: {}",
            s.name,
            kept.display()
        );
    } else {
        for n in c.node_ids() {
            println!(
                "--- {} node {n} log tail ---\n{}",
                s.name,
                c.log_tail(n, 40)
            );
        }
        c.stop().await;
    }
    outcome
}

fn divergence_spec() -> ClusterSpec {
    ClusterSpec {
        seed: 1701,
        ..ClusterSpec::default()
    }
}

fn power_loss_spec() -> ClusterSpec {
    ClusterSpec {
        seed: 1702,
        ..ClusterSpec::default()
    }
}

/// Unacknowledged appends of the divergent leader.
const K: usize = 3;
/// Bound on the divergent appends reaching the leader's own disk.
const LAND_T: Duration = Duration::from_secs(5);
/// Bound on each divergent / power-loss request.
const DIV_T: Duration = Duration::from_secs(6);

/// The `FAILPOINTS` spec `base` plus `extra` (`;`-separated, as the `fail` crate reads it).
pub fn with_failpoint(base: Option<&str>, extra: &str) -> String {
    match base {
        Some(b) if !b.is_empty() => format!("{b};{extra}"),
        _ => extra.to_owned(),
    }
}

fn restart_or_panic(r: Result<(), crate::harness::HarnessError>, node: u64) {
    if let Err(e) = r {
        panic!("harness: restart node {node}: {e}");
    }
}

/// Split joined requests: acknowledged ones join `acks`; the keys of the others are
/// returned (they must never be committed).
fn split(keys: Vec<String>, results: Vec<Start>, acks: &mut Vec<Ack>) -> Vec<String> {
    let mut unacked = Vec::new();
    for (k, r) in keys.into_iter().zip(results) {
        match r {
            Start::Acked(a) => acks.push(a),
            _ => unacked.push(k),
        }
    }
    unacked
}

/// SC2 with `kill -9` (module docs).
fn process_leader_dies_with_unreplicated_writes(
    c: &mut ProcCluster,
) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c, "smoke").await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta && a.isr.len() == a.replicas.len()
        })
        .await?;
        let (p, leader) = (a.partition, a.leader);
        let followers: Vec<u64> = a
            .replicas
            .iter()
            .copied()
            .filter(|r| *r != leader)
            .collect();
        let mut acks = append_acked(c, p, 3, ACK_T).await?;
        // Nothing the leader appends from now on can be fetched: every follower's link to
        // it stops forwarding (no FIN, no RST; a parked long-poll answer is held too).
        for f in &followers {
            c.proxy().blackhole(*f, leader);
        }
        let (keys, tasks) = spawn_starts(c, leader, p, K, "pdiv", DIV_T);
        wait_keys_on_disk(c, leader, p, &keys, LAND_T).await?;
        c.kill(leader).await;
        wait_assignment(c, p, "failed over", session(c) + CHANGE_T, |x| {
            x.leader != 0 && x.leader != leader && x.epoch > a.epoch
        })
        .await?;
        let divergent = split(keys, join_starts(tasks).await, &mut acks);
        acks.extend(append_acked(c, p, 3, ACK_T).await?);
        for f in &followers {
            c.proxy().heal(*f, leader);
        }
        restart_or_panic(c.restart(leader).await, leader);
        // The old leader rejoins the ISR (it truncated its tail to do so), then nothing
        // divergent is below the HW anywhere.
        if let Err(timeout) = wait_assignment(
            c,
            p,
            "rejoined by the old leader",
            lag_max(c) + CHANGE_T,
            |x| x.isr.contains(&leader),
        )
        .await
        {
            let now = published(c, p).leader;
            if now != 0
                && let Some(hw) = reported_hw(c, now, p).await
            {
                none_committed(c, p, hw, &divergent)?;
            }
            let views: Vec<_> = c.node_ids().iter().map(|n| c.node_disk(*n)).collect();
            acked_on_disk(&views, &acks, c.spec().min_insync).into_result()?;
            return Err(timeout);
        }
        match settle(c, p).await? {
            Some((_, hw, _)) => none_committed(c, p, hw, &divergent)?,
            None => return Err(fail("availability", format!("partition {p} is offline"))),
        }
        wait_completed(c, &acks, CHANGE_T).await?;
        final_checks(c, &acks).await
    })
}

/// The power-loss-like follower crash (module docs).
fn follower_power_loss(c: &mut ProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c, "smoke").await?;
        let meta = wait_meta_leader(c).await?;
        // The partition the meta leader leads: neither follower is then the metadata
        // leader, so taking them down never stalls the controller that has to react.
        let a = pick(c, "led by the meta leader with a full ISR", |a| {
            a.leader == meta && a.isr.len() == a.replicas.len() && a.replicas.len() >= 3
        })
        .await?;
        let (p, leader) = (a.partition, a.leader);
        let followers: Vec<u64> = a
            .replicas
            .iter()
            .copied()
            .filter(|r| *r != leader)
            .collect();
        let (f, g) = (followers[0], followers[1]);
        let base_f = c.failpoints(f).map(str::to_owned);
        let mut acks = append_acked(c, p, 3, ACK_T).await?;

        // G is down: the ISR is {leader, F}, so every acknowledgement needs F.
        c.kill(g).await;
        wait_assignment(
            c,
            p,
            "shrunk without the dead follower",
            session(c) + CHANGE_T,
            |x| x.leader == leader && !x.isr.contains(&g),
        )
        .await?;
        // F restarts with its WAL writer failing every write before it writes anything
        // (`wal::write` EIO): from now on nothing F appends reaches its files.
        c.set_failpoints(
            f,
            Some(&with_failpoint(base_f.as_deref(), "wal::write=return")),
        );
        restart_or_panic(c.restart(f).await, f);
        wait_assignment(c, p, "led with F in the ISR", lag_max(c) + CHANGE_T, |x| {
            x.leader == leader && x.isr.contains(&f)
        })
        .await?;
        let (keys, tasks) = spawn_starts(c, leader, p, 5, "ploss", DIV_T);
        let _unacked = split(keys, join_starts(tasks).await, &mut acks);
        let before = published(c, p);
        // Power loss of F (nothing it fetched since its restart is on its disk), then the
        // leader dies too. G comes back for the metadata quorum (it is not in the ISR, so it
        // can never be elected), F restarts without the failpoint.
        c.kill(f).await;
        c.kill(leader).await;
        c.set_failpoints(f, base_f.as_deref());
        restart_or_panic(c.restart_many(&[g, f]).await, f);
        // F is elected when it is still in the ISR; otherwise the partition goes offline
        // (the dead leader was its only ISR member). Either way: nothing acknowledged may be
        // missing from min_insync data dirs, and a new leader holds every acknowledged run.
        let after = wait_assignment(c, p, "re-led or offline", session(c) + CHANGE_T, |x| {
            x.epoch > before.epoch && x.leader != leader
        })
        .await?;
        let views: Vec<_> = c.node_ids().iter().map(|n| c.node_disk(*n)).collect();
        acked_on_disk(&views, &acks, c.spec().min_insync).into_result()?;
        if after.leader != 0 {
            let s = tools::scan(c, after.leader, p)?;
            for ack in &acks {
                if !s.has_start_run(&ack.id()) {
                    return Err(fail(
                        "disk",
                        format!(
                            "acked run {} (partition {p}) is missing from the log of the new leader {}",
                            ack.run_id, after.leader
                        ),
                    ));
                }
            }
        }
        // Everyone back: the cluster converges and every check holds.
        restart_or_panic(c.restart(leader).await, leader);
        wait_assignment(c, p, "led again", session(c) + CHANGE_T, |x| x.leader != 0).await?;
        wait_completed(c, &acks, CHANGE_T).await?;
        final_checks(c, &acks).await
    })
}

/// Measured tier-C helpers (module docs). Everything is read from disk (verified WAL scans,
/// the committed meta logs) or through the public API; harness-side failures panic (the
/// runner reports a `HarnessError`), behaviour the cluster owes fails with a named
/// [`ScenarioFailure`](crate::scenarios::ScenarioFailure).
pub mod tools {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use ostro_meta::PartitionAssignment;
    use ostro_types::{Offset, PartitionCount, PartitionId};
    use serde_json::{Value, json};
    use tokio::task::JoinHandle;
    use tokio::time::Instant;

    use crate::harness::inproc::{Ack, MetaLogDump, guest_wasm, read_meta_log};
    use crate::harness::process::ProcCluster;
    use crate::harness::{ApiClient, ClusterHarness, DataDirView};
    use crate::oracle::disk::{LogScan, acked_on_disk, scan_partition};
    use crate::oracle::{election, la_rule, replica};
    use crate::scenarios::{ScenarioFailure, ScenarioResult};

    /// Bound on one acknowledged append on a healthy partition.
    pub const ACK_T: Duration = Duration::from_secs(15);
    /// Bound on a leadership or ISR change (beyond the configured timeouts).
    pub const CHANGE_T: Duration = Duration::from_secs(20);
    /// Bound on replicas settling.
    pub const SETTLE_T: Duration = Duration::from_secs(30);
    /// Poll step (the failover client polls at most this far apart).
    pub const STEP: Duration = Duration::from_millis(50);

    /// A named measured failure.
    pub fn fail(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
        ScenarioFailure {
            oracle,
            detail: detail.into(),
        }
    }

    /// `[replication] session_timeout` the nodes run with.
    pub fn session(c: &ProcCluster) -> Duration {
        c.config(1).replication.session_timeout.get()
    }

    /// `[replication] replica_lag_max` the nodes run with.
    pub fn lag_max(c: &ProcCluster) -> Duration {
        c.config(1).replication.replica_lag_max.get()
    }

    /// Nodes whose process runs.
    pub fn live(c: &ProcCluster) -> Vec<u64> {
        c.node_ids()
            .into_iter()
            .filter(|n| c.pid(*n).is_some())
            .collect()
    }

    /// `node`'s committed meta log, decoded offline (`None` while unreadable).
    pub fn dump(c: &ProcCluster, node: u64) -> Option<MetaLogDump> {
        read_meta_log(&c.node_disk(node).root.join("meta")).ok()
    }

    /// The meta log with the highest commit among the nodes (live nodes first; a killed
    /// node's copy is only as new as its death). Panics when no copy is readable.
    pub fn best_dump(c: &ProcCluster) -> MetaLogDump {
        let up = live(c);
        let down: Vec<u64> = c
            .node_ids()
            .into_iter()
            .filter(|n| !up.contains(n))
            .collect();
        up.into_iter()
            .chain(down)
            .filter_map(|n| dump(c, n))
            .max_by_key(|d| d.commit)
            .expect("harness: no readable meta log")
    }

    /// Partition `p`'s committed assignment (empty record when absent).
    pub fn published(c: &ProcCluster, p: u16) -> PartitionAssignment {
        best_dump(c)
            .state
            .partition(p)
            .map(|v| v.value.clone())
            .unwrap_or_else(|| PartitionAssignment {
                partition: p,
                ..PartitionAssignment::default()
            })
    }

    /// Every partition's committed assignment.
    pub fn published_all(c: &ProcCluster) -> Vec<PartitionAssignment> {
        let d = best_dump(c);
        (0..c.spec().partitions)
            .map(|p| {
                d.state
                    .partition(p)
                    .map(|v| v.value.clone())
                    .unwrap_or_else(|| PartitionAssignment {
                        partition: p,
                        ..PartitionAssignment::default()
                    })
            })
            .collect()
    }

    /// A partition satisfying `pred`, lowest id first, waiting up to [`CHANGE_T`] for one
    /// (followers join the ISR as they first fetch).
    pub async fn pick(
        c: &ProcCluster,
        what: &str,
        pred: impl Fn(&PartitionAssignment) -> bool,
    ) -> Result<PartitionAssignment, ScenarioFailure> {
        poll(CHANGE_T, || published_all(c).into_iter().find(|a| pred(a)))
            .await
            .ok_or_else(|| {
                fail(
                    "placement",
                    format!("no partition {what} within {CHANGE_T:?}"),
                )
            })
    }

    /// `(meta leader, meta term)` as `node` reports it (`None` when it does not answer).
    pub async fn meta_view(c: &ProcCluster, node: u64) -> Option<(u64, u64)> {
        let v = c.admin_status(node).await;
        Some((v["meta"]["leader"].as_u64()?, v["meta"]["term"].as_u64()?))
    }

    /// The live node that reports itself as the metadata leader (highest term wins).
    pub async fn meta_leader(c: &ProcCluster) -> Option<u64> {
        let mut best: Option<(u64, u64)> = None;
        for n in live(c) {
            if let Some((l, t)) = meta_view(c, n).await
                && l == n
                && best.is_none_or(|(_, bt)| t > bt)
            {
                best = Some((n, t));
            }
        }
        best.map(|(n, _)| n)
    }

    /// Wait for a metadata leader.
    pub async fn wait_meta_leader(c: &ProcCluster) -> Result<u64, ScenarioFailure> {
        // clock-exempt: test deadline
        let end = Instant::now() + CHANGE_T;
        loop {
            if let Some(m) = meta_leader(c).await {
                return Ok(m);
            }
            if Instant::now() >= end {
                return Err(fail(
                    "availability",
                    format!("no meta leader within {CHANGE_T:?}"),
                ));
            }
            pause(STEP).await;
        }
    }

    /// Sleep (test pacing only).
    pub async fn pause(d: Duration) {
        // clock-exempt: test pacing
        tokio::time::sleep(d).await;
    }

    /// Poll `f` every [`STEP`] until it yields a value or `limit` passes.
    pub async fn poll<T>(limit: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
        // clock-exempt: test deadline
        let end = Instant::now() + limit;
        loop {
            if let Some(v) = f() {
                return Some(v);
            }
            if Instant::now() >= end {
                return None;
            }
            pause(STEP).await;
        }
    }

    /// [`poll`] with an async probe.
    pub async fn poll_async<T, F>(limit: Duration, mut f: impl FnMut() -> F) -> Option<T>
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
            pause(STEP).await;
        }
    }

    /// Wait until `p`'s committed assignment satisfies `pred`.
    pub async fn wait_assignment(
        c: &ProcCluster,
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

    /// Upload guest `guest` (`guests/<guest>`) through any live node until one accepts it.
    pub async fn deploy(c: &ProcCluster, guest: &str) -> ScenarioResult {
        let wasm = match guest_wasm(guest) {
            Ok(w) => w,
            Err(e) => panic!("harness: guest {guest}: {e}"),
        };
        let sha = sha_hex(&wasm);
        let done = poll_async(CHANGE_T, || {
            let wasm = wasm.clone();
            let sha = sha.clone();
            async move {
                for n in live(c) {
                    let api = c.client(n);
                    let r = api
                        .http
                        .put(format!("{}/v1/modules/{sha}", api.base_url))
                        .header("content-type", "application/wasm")
                        .body(wasm.clone())
                        .send()
                        .await;
                    if r.is_ok_and(|r| r.status().is_success()) {
                        return Some(());
                    }
                }
                None
            }
        })
        .await;
        done.ok_or_else(|| {
            fail(
                "availability",
                format!("module {guest} could not be deployed"),
            )
        })
    }

    fn sha_hex(b: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(b)
            .iter()
            .map(|x| format!("{x:02x}"))
            .collect()
    }

    /// A fresh idempotency key routing `workflow` to partition `p`.
    pub fn key_for(c: &ProcCluster, workflow: &str, p: u16, tag: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
        loop {
            let i = N.fetch_add(1, Ordering::Relaxed);
            let key = format!("{tag}-{}-{}-{i}", c.cluster_id(), c.r());
            if ostro_types::routing::partition_for_key(workflow, key.as_bytes(), count).0 == p {
                return key;
            }
        }
    }

    /// How one start request ended.
    #[derive(Debug)]
    pub enum Start {
        /// Acknowledged (200/201 with a run id).
        Acked(Ack),
        /// Answered with an error status (and the body).
        Rejected(u16, Value),
        /// No answer (timeout, connection error).
        NoAnswer(String),
    }

    impl Start {
        /// One line for failure details.
        pub fn describe(&self) -> String {
            match self {
                Start::Acked(a) => format!("acknowledged {}", a.run_id),
                Start::Rejected(st, body) => format!("rejected {st}: {body}"),
                Start::NoAnswer(e) => format!("no answer: {e}"),
            }
        }

        /// The `leader_addr` hint of a 503 `not_leader` answer.
        pub fn leader_hint(&self) -> Option<String> {
            match self {
                Start::Rejected(503, v) => leader_hint(v),
                _ => None,
            }
        }
    }

    /// `error.details.leader_addr` of a 503 `not_leader` body (D-P12-21).
    pub fn leader_hint(body: &Value) -> Option<String> {
        body["error"]["details"]["leader_addr"]
            .as_str()
            .map(str::to_owned)
    }

    /// `POST {base}/v1/workflows/{workflow}/runs` with idempotency key `key`.
    pub async fn start_at(
        http: &reqwest::Client,
        base: &str,
        workflow: &str,
        input: Value,
        key: &str,
        limit: Duration,
    ) -> Start {
        let body = json!({ "input": input, "idempotency_key": key });
        let sent = http
            .post(format!("{base}/v1/workflows/{workflow}/runs"))
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
            return Start::Rejected(st.as_u16(), v);
        }
        let Some(run_id) = v["run_id"].as_str() else {
            return Start::Rejected(st.as_u16(), v);
        };
        match run_id.parse::<ostro_types::RunId>() {
            Ok(id) => Start::Acked(Ack {
                run_id: run_id.to_owned(),
                partition: id.partition().0,
            }),
            Err(_) => Start::Rejected(st.as_u16(), v),
        }
    }

    /// A client that follows 503 `not_leader` hints (`leader_addr`) and rotates over the
    /// nodes' API addresses when a node does not answer. Clones share the current target.
    #[derive(Clone, Debug)]
    pub struct HintClient {
        http: reqwest::Client,
        bases: std::sync::Arc<Vec<String>>,
        cur: std::sync::Arc<std::sync::Mutex<String>>,
    }

    impl HintClient {
        /// A client over every node's API, starting at `first`.
        pub fn new(c: &ProcCluster, first: u64) -> Self {
            let bases: Vec<String> = c.node_ids().iter().map(|n| c.client(*n).base_url).collect();
            let cur = c.client(first).base_url;
            Self {
                http: reqwest::Client::new(),
                bases: std::sync::Arc::new(bases),
                cur: std::sync::Arc::new(std::sync::Mutex::new(cur)),
            }
        }

        /// The current target.
        pub fn current(&self) -> String {
            self.cur.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }

        fn follow(&self, hint: Option<String>, failed: &str) {
            let mut cur = self.cur.lock().unwrap_or_else(|e| e.into_inner());
            if *cur != failed {
                return;
            }
            match hint {
                Some(addr) => *cur = format!("http://{addr}"),
                None => {
                    let i = self.bases.iter().position(|b| b == failed).unwrap_or(0);
                    *cur = self.bases[(i + 1) % self.bases.len()].clone();
                }
            }
        }

        /// One start attempt at the current target; a hint or a silent node moves the target.
        pub async fn start(
            &self,
            workflow: &str,
            input: Value,
            key: &str,
            limit: Duration,
        ) -> Start {
            let base = self.current();
            let r = start_at(&self.http, &base, workflow, input, key, limit).await;
            match &r {
                Start::Acked(_) => {}
                Start::Rejected(503, _) => self.follow(r.leader_hint(), &base),
                Start::Rejected(..) => {}
                Start::NoAnswer(_) => self.follow(None, &base),
            }
            r
        }

        /// One `GET {path}` at the current target (`None` = no answer); hints are followed.
        pub async fn get(&self, path: &str, limit: Duration) -> Option<(u16, Value)> {
            let base = self.current();
            let sent = self
                .http
                .get(format!("{base}{path}"))
                .timeout(limit)
                .send()
                .await;
            let Ok(r) = sent else {
                self.follow(None, &base);
                return None;
            };
            let st = r.status().as_u16();
            let v: Value = r.json().await.unwrap_or(Value::Null);
            if st == 503 {
                self.follow(leader_hint(&v), &base);
            }
            Some((st, v))
        }
    }

    /// Start of the `smoke` workflow on `api` with key `key`.
    pub async fn start_keyed(api: &ApiClient, key: &str, limit: Duration) -> Start {
        start_at(
            &api.http,
            &api.base_url,
            "smoke",
            json!({ "k": key }),
            key,
            limit,
        )
        .await
    }

    /// Start `k` `smoke` runs on partition `p` through its committed leader; each must be
    /// acknowledged within `limit` (retried with the same key).
    pub async fn append_acked(
        c: &ProcCluster,
        p: u16,
        k: usize,
        limit: Duration,
    ) -> Result<Vec<Ack>, ScenarioFailure> {
        let mut acks = Vec::with_capacity(k);
        for _ in 0..k {
            let key = key_for(c, "smoke", p, "ack");
            // clock-exempt: test deadline
            let end = Instant::now() + limit;
            let mut last = String::from("no leader");
            loop {
                let leader = published(c, p).leader;
                if leader != 0 && c.pid(leader).is_some() {
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

    /// Fire `k` `smoke` starts at `node` for partition `p` without waiting.
    pub fn spawn_starts(
        c: &ProcCluster,
        node: u64,
        p: u16,
        k: usize,
        tag: &str,
        limit: Duration,
    ) -> (Vec<String>, Vec<JoinHandle<Start>>) {
        let api = c.client(node);
        let keys: Vec<String> = (0..k).map(|_| key_for(c, "smoke", p, tag)).collect();
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
    pub async fn join_starts(tasks: Vec<JoinHandle<Start>>) -> Vec<Start> {
        let mut out = Vec::with_capacity(tasks.len());
        for t in tasks {
            match t.await {
                Ok(s) => out.push(s),
                Err(e) => panic!("harness: start request task: {e}"),
            }
        }
        out
    }

    /// Offline verified scan of `node`'s partition `p`.
    pub fn scan(c: &ProcCluster, node: u64, p: u16) -> Result<LogScan, ScenarioFailure> {
        scan_partition(&c.node_disk(node), PartitionId(p)).map_err(|e| {
            fail(
                "disk",
                format!("node {node} partition {p}: scan failed: {e}"),
            )
        })
    }

    /// `node`'s measured durable end of `p` (0 for an empty log).
    pub fn durable_end(c: &ProcCluster, node: u64, p: u16) -> Result<Offset, ScenarioFailure> {
        Ok(scan(c, node, p)?.durable_end().unwrap_or(Offset::ZERO))
    }

    /// The HW `node` reports for `p` in `GET /v1/admin/replication`.
    pub async fn reported_hw(c: &ProcCluster, node: u64, p: u16) -> Option<Offset> {
        partition_status(c, node, p).await?["hw"]
            .as_u64()
            .map(Offset)
    }

    /// `node`'s `GET /v1/admin/replication` row of partition `p`.
    pub async fn partition_status(c: &ProcCluster, node: u64, p: u16) -> Option<Value> {
        let v = c.admin_status(node).await;
        v["partitions"]
            .as_array()?
            .iter()
            .find(|e| e["partition"].as_u64() == Some(u64::from(p)))
            .cloned()
    }

    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
    }

    /// Whether the scanned log holds a command carrying `key` below `below`.
    pub fn holds_key(scan: &LogScan, key: &str, below: Offset) -> bool {
        scan.commands()
            .any(|(off, cmd)| off < below && contains(cmd, key.as_bytes()))
    }

    /// Wait until `node`'s log of `p` holds every key in `keys`.
    pub async fn wait_keys_on_disk(
        c: &ProcCluster,
        node: u64,
        p: u16,
        keys: &[String],
        limit: Duration,
    ) -> ScenarioResult {
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
                format!(
                    "node {node} partition {p}: the {} appended commands never reached its log within {limit:?}",
                    keys.len()
                ),
            )
        })
    }

    /// No node's log below `hw` holds any of `keys`.
    pub fn none_committed(c: &ProcCluster, p: u16, hw: Offset, keys: &[String]) -> ScenarioResult {
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

    /// `GET {path}` on `p`'s committed leader.
    async fn get_on_leader(c: &ProcCluster, p: u16, path: &str) -> Option<Value> {
        let leader = published(c, p).leader;
        if leader == 0 || c.pid(leader).is_none() {
            return None;
        }
        let (st, v) = c.client(leader).get_json(path).await.ok()?;
        st.is_success().then_some(v)
    }

    /// Wait until every acknowledged run is terminal `completed` on its partition's leader.
    pub async fn wait_completed(c: &ProcCluster, acks: &[Ack], limit: Duration) -> ScenarioResult {
        for a in acks {
            let path = format!("/v1/runs/{}", a.run_id);
            let path = &path;
            let done = poll_async(limit, || async move {
                let v = get_on_leader(c, a.partition, path).await?;
                (v["status"] == "completed").then_some(())
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

    /// Wait until `p` is settled: the live ISR members' measured durable ends all equal the
    /// leader's and the leader's reported HW is that end, twice in a row. `None` leader when
    /// `p` is offline.
    pub async fn settle(
        c: &ProcCluster,
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
                .filter(|n| c.pid(*n).is_some())
                .collect();
            let mut ends = BTreeMap::new();
            for n in &members {
                ends.insert(*n, durable_end(c, *n, p)?);
            }
            let leader_end = ends.get(&a.leader).copied();
            let hw = reported_hw(c, a.leader, p).await;
            let sample = match (leader_end, hw) {
                (Some(e), Some(hw)) if hw == e && ends.values().all(|x| *x == e) => {
                    Some((a.leader, hw, members.clone()))
                }
                _ => None,
            };
            if sample.is_some() && sample == prev {
                return Ok(sample);
            }
            let last = format!(
                "leader {} isr {:?} durable ends {ends:?} hw {hw:?}",
                a.leader, a.isr
            );
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

    /// The end-of-scenario oracles: replica identity below the HW over each partition's
    /// live ISR, acknowledged ⇒ on disk on `min_insync` nodes (the tier-C logs are never
    /// trimmed: 250 MB segments), the LA rule on every node's log and the ISR-only election
    /// audit of the committed meta log.
    pub async fn final_checks(c: &ProcCluster, acks: &[Ack]) -> ScenarioResult {
        wait_meta_leader(c).await?;
        for p in 0..c.spec().partitions {
            if let Some((_, hw, members)) = settle(c, p).await? {
                let views: Vec<DataDirView> = members.iter().map(|n| c.node_disk(*n)).collect();
                replica::identical_below_hw(&views, PartitionId(p), hw).into_result()?;
            }
        }
        let views: Vec<DataDirView> = c.node_ids().iter().map(|n| c.node_disk(*n)).collect();
        acked_on_disk(&views, acks, c.spec().min_insync).into_result()?;
        let dump = best_dump(c);
        for v in &views {
            for p in 0..c.spec().partitions {
                la_rule::check(v, PartitionId(p), &dump.entries).into_result()?;
            }
        }
        election::isr_only_from(dump.snapshot_state.clone(), &dump.entries).into_result()
    }
}
