//! Phase 13 `handoff` scenarios (tier B, 13-11; D-P13-13, CLUS-06): graceful leader
//! handoff and controlled shutdown on a 3-node RF=3 cluster under a mixed workload.
//!
//! The workload mixes three guests: `smoke` (completes at once), `sdk-basic` (steps,
//! `join!`/`select!` over short durable sleeps that park hot) and `signals` in mode
//! `before` (a durable sleep, then a signal wait that evicts; the scenario sends the
//! signal). Every run is started with an idempotency key routed to a chosen partition,
//! retried through the partition's current leader. Besides the Phase 12 oracles
//! (`final_checks_with`: replica identity below the HW, the LA rule, the ISR-only election
//! audit, acknowledged ⇒ on disk), every scenario ends with the **run-outcome** check:
//! every started run completes (`completed`), none ends `failed`, `cancelled`,
//! `terminated` or `timed_out` by the system.
//!
//! - `handoff_under_load_no_failed_runs`: 50 handoffs of random partitions to random ISR
//!   members through `NodeOps.HandOff` (sent to the leader as a node identity), each
//!   overlapping a burst of starts on that partition. Records moved / aborted counts and the
//!   per-handoff unavailability (handoff call start → the next acknowledged start on the
//!   partition), p50 / p99 / max.
//! - `handoff_aborts_when_target_lags`: the target's `Fetch` is blackholed while the leader
//!   appends; the handoff aborts (or is refused once the ISR shrank), the epoch does not
//!   move and the old leader keeps serving.
//! - `controlled_shutdown_moves_everything`: a graceful stop (no isolation; the harness's
//!   `upgrade(node, None)` restart) of the node leading the most partitions (the metadata
//!   leader on a tie): every partition it led was handed off before it exited (an applied
//!   `ElectLeader{reason: resign, prev_leader: node}` per partition), metadata leadership
//!   moved if it held it, no failed run. Records the stop + restart time.
//! - `restart_cycle_no_drain`: graceful restart of each node in turn with runs in flight
//!   (the rolling-restart skeleton the tier-C upgrade test builds on), waiting for the full
//!   ISR between restarts: no failed or cancelled run.

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_meta::ext::ELECTION_REASON_RESIGN;
use ostro_meta::{MetaCommand, MetaResult};
use ostro_proto::cluster::v1::node_ops_client::NodeOpsClient;
use ostro_proto::cluster::v1::{HandOffRequest, HandOffResponse};
use ostro_types::PartitionCount;
use serde_json::{Value, json};
use tokio::time::Instant;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, STEP, Start, append_acked, blackhole, clear, fail, final_checks_with,
    harness, live, meta_dump, meta_leader, pause, poll_async, published, wait_meta_leader,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster, TERMINAL, guest_wasm};
use crate::harness::p13::P13Harness;
use crate::harness::{ApiClient, ClusterHarness, ClusterSpec};

/// The `handoff` scenarios.
pub fn all() -> Vec<Scenario> {
    let s = |name, run: Run, deadline| Scenario {
        name,
        group: "handoff",
        tier: Tier::B,
        deadline,
        spec,
        run,
    };
    vec![
        s(
            "handoff_under_load_no_failed_runs",
            handoff_under_load_no_failed_runs,
            Duration::from_secs(600),
        ),
        s(
            "handoff_aborts_when_target_lags",
            handoff_aborts_when_target_lags,
            LONG_DEADLINE,
        ),
        s(
            "controlled_shutdown_moves_everything",
            controlled_shutdown_moves_everything,
            LONG_DEADLINE,
        ),
        s(
            "restart_cycle_no_drain",
            restart_cycle_no_drain,
            Duration::from_secs(600),
        ),
    ]
}

fn spec() -> ClusterSpec {
    ClusterSpec {
        seed: 1311,
        ..ClusterSpec::default()
    }
}

/// Handoffs in `handoff_under_load_no_failed_runs`.
const HANDOFFS: usize = 50;
/// Bound on every started run reaching a terminal status at the end.
const OUTCOME_T: Duration = Duration::from_secs(120);
/// Bound on one `NodeOps.HandOff` call (grace + catch-up + election, generously).
const HANDOFF_CALL_T: Duration = Duration::from_secs(60);

// ------------------------------------------------------------------------------------
// Workload
// ------------------------------------------------------------------------------------

/// Which guest a run uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// `smoke`: completes at once.
    Smoke,
    /// `sdk-basic`: steps and short durable sleeps (hot waits).
    Steps,
    /// `signals` mode `before`: a sleep, then a signal wait (the scenario signals it).
    Signal,
}

impl Kind {
    fn workflow(self) -> &'static str {
        match self {
            Kind::Smoke => "smoke",
            Kind::Steps => "sdk-basic",
            Kind::Signal => "signals",
        }
    }

    fn input(self, key: &str) -> Value {
        match self {
            Kind::Smoke => json!({ "k": key }),
            Kind::Steps => json!({ "name": key, "n": 3 }),
            Kind::Signal => json!({ "mode": "before", "sleep_ms": 200 }),
        }
    }
}

/// One started (acknowledged) run.
#[derive(Clone, Debug)]
struct Started {
    ack: Ack,
    kind: Kind,
}

/// The scenario's workload: every run it started and the signals still to send.
struct Load {
    runs: Vec<Started>,
    unsignalled: Vec<Ack>,
    n: u64,
    rng: u64,
}

impl Load {
    fn new(seed: u64) -> Self {
        Self {
            runs: Vec::new(),
            unsignalled: Vec::new(),
            n: 0,
            rng: seed | 1,
        }
    }

    /// xorshift64: deterministic choices from the spec seed.
    fn next(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn pick(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// A fresh idempotency key routing `wf` to partition `p`.
    fn key_for(&mut self, c: &InProcCluster, wf: &str, p: u16) -> String {
        let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
        loop {
            self.n += 1;
            let key = format!("ho-{}-{}", c.cluster_id(), self.n);
            if ostro_types::routing::partition_for_key(wf, key.as_bytes(), count).0 == p {
                return key;
            }
        }
    }

    /// Start one `kind` run on partition `p` through its current leader (retried with the
    /// same idempotency key across leader moves), acknowledged within [`ACK_T`].
    async fn start(&mut self, c: &InProcCluster, kind: Kind, p: u16) -> ScenarioResult {
        let wf = kind.workflow();
        let key = self.key_for(c, wf, p);
        let input = kind.input(&key);
        let end = Instant::now() + ACK_T;
        let mut last = String::from("no leader");
        loop {
            let leader = published(c, p).leader;
            if leader != 0 && c.handle(leader).is_some() {
                let left = end.saturating_duration_since(Instant::now()).max(STEP);
                match start_wf(&c.client(leader), wf, &input, &key, left).await {
                    Start::Acked(ack) => {
                        if kind == Kind::Signal {
                            self.unsignalled.push(ack.clone());
                        }
                        self.runs.push(Started { ack, kind });
                        return Ok(());
                    }
                    other => last = other.describe(),
                }
            }
            if Instant::now() >= end {
                return Err(fail(
                    "availability",
                    format!(
                        "partition {p}: {wf} run {key} not acknowledged within {ACK_T:?}: {last}"
                    ),
                ));
            }
            pause(STEP * 2).await;
        }
    }

    /// One run of every kind on partition `p`.
    async fn burst(&mut self, c: &InProcCluster, p: u16) -> ScenarioResult {
        for kind in [Kind::Steps, Kind::Signal, Kind::Smoke] {
            self.start(c, kind, p).await?;
        }
        Ok(())
    }

    /// A burst on every partition.
    async fn round(&mut self, c: &InProcCluster) -> ScenarioResult {
        for p in 0..c.spec().partitions {
            self.burst(c, p).await?;
        }
        Ok(())
    }

    /// Signal every waiting `signals` run (through its partition's current leader).
    async fn signal_all(&mut self, c: &InProcCluster) -> ScenarioResult {
        for ack in std::mem::take(&mut self.unsignalled) {
            let path = format!("/v1/runs/{}/signals/x", ack.run_id);
            let end = Instant::now() + ACK_T;
            let mut last = String::from("no leader");
            loop {
                let leader = published(c, ack.partition).leader;
                if leader != 0 && c.handle(leader).is_some() {
                    match c
                        .client(leader)
                        .post_json(&path, &json!({ "payload": "go" }))
                        .await
                    {
                        Ok((st, _)) if st.is_success() => break,
                        // An earlier attempt was applied but its answer was lost (a leader
                        // change mid-request answers 503 "outcome unknown"): the run consumed
                        // the signal and completed. The outcome oracle checks its status.
                        Ok((st, v))
                            if st.as_u16() == 409
                                && v["error"]["details"]["reason"] == "run_terminal" =>
                        {
                            break;
                        }
                        Ok((st, v)) => last = format!("node {leader}: {st} {v}"),
                        Err(e) => last = format!("node {leader}: {e}"),
                    }
                }
                if Instant::now() >= end {
                    let status = match c
                        .client(published(c, ack.partition).leader.max(1))
                        .get_json(&format!("/v1/runs/{}", ack.run_id))
                        .await
                    {
                        Ok((_, v)) => v.to_string(),
                        Err(e) => e.to_string(),
                    };
                    return Err(fail(
                        "availability",
                        format!(
                            "run {} (partition {}): signal not accepted within {ACK_T:?}: last answer {last}; run {status}",
                            ack.run_id, ack.partition
                        ),
                    ));
                }
                pause(STEP * 2).await;
            }
        }
        Ok(())
    }

    fn acks(&self) -> Vec<Ack> {
        self.runs.iter().map(|r| r.ack.clone()).collect()
    }

    /// The run-outcome oracle: every started run ends `completed`; none failed, cancelled,
    /// terminated or timed out.
    async fn outcomes(&self, c: &InProcCluster) -> ScenarioResult {
        let mut by_status: BTreeMap<String, usize> = BTreeMap::new();
        for r in &self.runs {
            let path = format!("/v1/runs/{}", r.ack.run_id);
            let path = &path;
            let p = r.ack.partition;
            let status = poll_async(OUTCOME_T, || async move {
                let leader = published(c, p).leader;
                if leader == 0 || c.handle(leader).is_none() {
                    return None;
                }
                let (st, v) = c.client(leader).get_json(path).await.ok()?;
                let s = v["status"].as_str()?.to_owned();
                (st.is_success() && TERMINAL.contains(&s.as_str())).then_some(s)
            })
            .await;
            let Some(status) = status else {
                return Err(fail(
                    "run_outcome",
                    format!(
                        "{} run {} (partition {p}) not terminal within {OUTCOME_T:?}",
                        r.kind.workflow(),
                        r.ack.run_id
                    ),
                ));
            };
            if status != "completed" {
                return Err(fail(
                    "run_outcome",
                    format!(
                        "{} run {} (partition {p}) ended {status}: a handoff or a graceful restart must never fail, cancel or terminate a run",
                        r.kind.workflow(),
                        r.ack.run_id
                    ),
                ));
            }
            *by_status.entry(status).or_default() += 1;
        }
        println!("run outcomes: {by_status:?} over {} runs", self.runs.len());
        Ok(())
    }
}

/// `POST /v1/workflows/{wf}/runs` with idempotency key `key`, bounded by `limit`.
async fn start_wf(api: &ApiClient, wf: &str, input: &Value, key: &str, limit: Duration) -> Start {
    let body = json!({ "input": input, "idempotency_key": key });
    let sent = api
        .http
        .post(format!("{}/v1/workflows/{wf}/runs", api.base_url))
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

/// `PUT /v1/modules/{sha}` of guest `name` on `api`.
async fn put_module(api: &ApiClient, name: &str) -> bool {
    use sha2::{Digest, Sha256};
    let wasm = harness(&format!("guest {name}"), guest_wasm(name));
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    api.http
        .put(format!("{}/v1/modules/{hex}", api.base_url))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .is_ok_and(|r| r.status().is_success())
}

/// Deploy the workload guests (meta leader first, then any live node).
async fn deploy_guests(c: &InProcCluster) -> ScenarioResult {
    let first = wait_meta_leader(c).await?;
    let mut order = vec![first];
    order.extend(live(c).into_iter().filter(|n| *n != first));
    for name in ["smoke", "sdk-basic", "signals"] {
        let order = &order;
        let done = poll_async(CHANGE_T, || async move {
            for n in order {
                if put_module(&c.client(*n), name).await {
                    return Some(());
                }
            }
            None
        })
        .await;
        if done.is_none() {
            return Err(fail(
                "availability",
                format!("guest {name} could not be deployed"),
            ));
        }
    }
    Ok(())
}

/// `NodeOps.HandOff` to `leader` as node `caller`.
async fn hand_off(
    c: &InProcCluster,
    caller: u64,
    leader: u64,
    p: u16,
    to: u64,
    epoch: u32,
) -> Result<HandOffResponse, String> {
    let mut client = NodeOpsClient::new(c.channel_as(caller, leader));
    let req = HandOffRequest {
        partition: u32::from(p),
        to,
        epoch,
    };
    match tokio::time::timeout(HANDOFF_CALL_T, client.hand_off(req)).await {
        Ok(Ok(r)) => Ok(r.into_inner()),
        Ok(Err(s)) => Err(format!("{:?}: {}", s.code(), s.message())),
        Err(_) => Err(format!("no answer within {HANDOFF_CALL_T:?}")),
    }
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

// ------------------------------------------------------------------------------------
// Scenarios
// ------------------------------------------------------------------------------------

/// 50 handoffs under a mixed workload: no acknowledged run lost, no run failed.
fn handoff_under_load_no_failed_runs(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_guests(c).await?;
        let mut load = Load::new(c.spec().seed);
        load.round(c).await?;
        let parts = c.spec().partitions as usize;
        let (mut moved, mut aborted, mut refused) = (0usize, Vec::new(), Vec::new());
        let mut unavailable = Vec::new();
        let mut attempts = 0usize;
        while moved + aborted.len() + refused.len() < HANDOFFS {
            attempts += 1;
            if attempts > HANDOFFS * 4 {
                return Err(fail(
                    "handoff",
                    format!(
                        "only {} of {HANDOFFS} handoffs could even be attempted (no leader / no other ISR member)",
                        moved + aborted.len() + refused.len()
                    ),
                ));
            }
            let p = load.pick(parts) as u16;
            let a = published(c, p);
            let targets: Vec<u64> = a
                .isr
                .iter()
                .copied()
                .filter(|n| *n != a.leader && c.handle(*n).is_some())
                .collect();
            if a.leader == 0 || c.handle(a.leader).is_none() || targets.is_empty() {
                pause(STEP * 4).await;
                continue;
            }
            let to = targets[load.pick(targets.len())];
            let t0 = Instant::now();
            let (r, started) = tokio::join!(hand_off(c, to, a.leader, p, to, a.epoch), async {
                // Starts racing the handoff (frozen partition: 503 leader_changed, retried).
                let mut l = Load::new(load.next());
                let r = l.burst(c, p).await;
                (r, l)
            });
            let (burst, l) = started;
            burst?;
            load.runs.extend(l.runs);
            load.unsignalled.extend(l.unsignalled);
            match r {
                Ok(resp) if resp.moved => {
                    moved += 1;
                    if resp.new_epoch <= a.epoch {
                        return Err(fail(
                            "handoff",
                            format!(
                                "partition {p}: moved at epoch {} <= {}",
                                resp.new_epoch, a.epoch
                            ),
                        ));
                    }
                    let mut one = Load::new(load.next());
                    one.start(c, Kind::Smoke, p).await?;
                    unavailable.push(t0.elapsed());
                    load.runs.extend(one.runs);
                }
                Ok(resp) => aborted.push(format!("p{p} -> {to}: {}", resp.abort_reason)),
                Err(e) => refused.push(format!("p{p} -> {to}: {e}")),
            }
            if (moved + aborted.len() + refused.len()) % 5 == 0 {
                load.signal_all(c).await?;
            }
        }
        load.signal_all(c).await?;
        unavailable.sort_unstable();
        println!(
            "handoff_under_load: moved {moved}/{HANDOFFS}, aborted {}, refused {}; unavailability p50 {:?} p99 {:?} max {:?}; aborts {aborted:?}; refusals {refused:?}",
            aborted.len(),
            refused.len(),
            percentile(&unavailable, 0.5),
            percentile(&unavailable, 0.99),
            unavailable.last().copied().unwrap_or_default(),
        );
        if moved < HANDOFFS / 2 {
            return Err(fail(
                "handoff",
                format!(
                    "only {moved} of {HANDOFFS} healthy handoffs moved leadership; aborts {aborted:?}; refusals {refused:?}"
                ),
            ));
        }
        load.outcomes(c).await?;
        final_checks_with(c, false, &load.acks()).await
    })
}

/// A lagging target: the handoff aborts (or is refused), the epoch stays, the old leader
/// keeps serving.
fn handoff_aborts_when_target_lags(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_guests(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = super::placement::support::pick(c, "led with a full ISR", |a| {
            a.leader != 0 && a.isr.len() == 3
        })?;
        let (p, leader) = (a.partition, a.leader);
        let lagging = a
            .isr
            .iter()
            .copied()
            .find(|n| *n != leader)
            .expect("ISR of 3");
        let caller = if meta != leader { meta } else { lagging };
        let mut acks = append_acked(c, p, 2, ACK_T).await?;
        let before = published(c, p);
        blackhole(c, lagging, leader, RpcKind::Fetch);
        // The leader's end moves past what the blackholed follower has while the handoff
        // goes out, well before the ISR could shrink it out (replica_lag_max): the
        // executor's catch-up wait is what refuses it. The racing start may meet the write
        // fence (503 leader_changed) and is retried until the abort lifts it.
        let t0 = Instant::now();
        let (more, (r, took)) = tokio::join!(append_acked(c, p, 1, ACK_T), async {
            pause(Duration::from_millis(100)).await;
            let t0 = Instant::now();
            let r = hand_off(c, caller, leader, p, lagging, before.epoch).await;
            (r, t0.elapsed())
        });
        acks.extend(more?);
        let since_blackhole = t0.elapsed();
        let verdict = match &r {
            Ok(resp) if resp.moved => {
                return Err(fail(
                    "handoff",
                    format!(
                        "partition {p}: leadership moved to node {lagging}, whose fetch was blackholed (behind the leader's end), at epoch {}",
                        resp.new_epoch
                    ),
                ));
            }
            Ok(resp) => format!("aborted: {}", resp.abort_reason),
            Err(e) if e.contains("is not in the ISR") => format!("refused: {e}"),
            Err(e) => {
                return Err(fail(
                    "handoff",
                    format!(
                        "partition {p}: unexpected answer to a handoff to a lagging target: {e}"
                    ),
                ));
            }
        };
        println!(
            "handoff_aborts_when_target_lags: {verdict} after {took:?} ({since_blackhole:?} since the blackhole)"
        );
        let now = published(c, p);
        if now.leader != leader || now.epoch != before.epoch {
            return Err(fail(
                "handoff",
                format!(
                    "partition {p}: after the abort the assignment moved to leader {} epoch {} (was {leader} at {})",
                    now.leader, now.epoch, before.epoch
                ),
            ));
        }
        // The old leader keeps serving (admission and appends back on).
        acks.extend(append_acked(c, p, 3, ACK_T).await?);
        clear(c, lagging, leader);
        let mut load = Load::new(c.spec().seed);
        load.burst(c, p).await?;
        load.signal_all(c).await?;
        load.runs.extend(acks.into_iter().map(|ack| Started {
            ack,
            kind: Kind::Smoke,
        }));
        load.outcomes(c).await?;
        final_checks_with(c, false, &load.acks()).await
    })
}

/// A graceful stop of the busiest leader hands every partition off before it exits.
fn controlled_shutdown_moves_everything(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_guests(c).await?;
        let meta = wait_meta_leader(c).await?;
        let mut load = Load::new(c.spec().seed);
        load.round(c).await?;
        let mut led: BTreeMap<u64, Vec<u16>> = BTreeMap::new();
        for p in 0..c.spec().partitions {
            let a = published(c, p);
            if a.leader != 0 {
                led.entry(a.leader).or_default().push(p);
            }
        }
        let most = led.values().map(Vec::len).max().unwrap_or(0);
        let node = led
            .iter()
            .filter(|(_, ps)| ps.len() == most)
            .map(|(n, _)| *n)
            .max_by_key(|n| (*n == meta, std::cmp::Reverse(*n)))
            .ok_or_else(|| fail("placement", "no partition has a leader"))?;
        let parts = led.get(&node).cloned().unwrap_or_default();
        let was_meta = node == meta;
        let from_index = meta_dump(c).entries.last().map_or(0, |e| e.index);
        // Runs in flight on the partitions it leads.
        for p in &parts {
            load.burst(c, *p).await?;
        }
        let t0 = Instant::now();
        harness(
            &format!("graceful restart of node {node}"),
            c.upgrade(node, None).await,
        );
        let took = t0.elapsed();
        wait_meta_leader(c).await?;
        let dump = meta_dump(c);
        for p in &parts {
            let handed = dump.entries.iter().any(|e| {
                e.index > from_index
                    && matches!(e.applied_result, MetaResult::Ok { .. })
                    && matches!(
                        &e.command,
                        MetaCommand::ElectLeader { partition, leader, info, .. }
                            if partition == p
                                && *leader != node
                                && *leader != 0
                                && info.reason == ELECTION_REASON_RESIGN
                                && info.prev_leader == node
                    )
            });
            if !handed {
                let seen: Vec<String> = dump
                    .entries
                    .iter()
                    .filter(|e| e.index > from_index)
                    .filter_map(|e| match &e.command {
                        MetaCommand::ElectLeader {
                            partition,
                            epoch,
                            leader,
                            info,
                            ..
                        } if partition == p => Some(format!(
                            "index {} epoch {epoch} leader {leader} reason {} prev {} -> {:?}",
                            e.index, info.reason, info.prev_leader, e.applied_result
                        )),
                        _ => None,
                    })
                    .collect();
                return Err(fail(
                    "handoff",
                    format!(
                        "partition {p}: node {node} led it and stopped gracefully, but no handoff election moved it first; elections since: {seen:?}"
                    ),
                ));
            }
        }
        if was_meta && meta_leader(c) == Some(node) {
            return Err(fail(
                "handoff",
                format!(
                    "node {node} held metadata leadership and still holds it after its graceful stop"
                ),
            ));
        }
        println!(
            "controlled_shutdown_moves_everything: node {node} (meta leader: {was_meta}) handed off {parts:?}; stop + restart took {took:?}"
        );
        load.signal_all(c).await?;
        load.round(c).await?;
        load.signal_all(c).await?;
        load.outcomes(c).await?;
        final_checks_with(c, false, &load.acks()).await
    })
}

/// Restart every node in turn (graceful), runs in flight: no failed or cancelled run.
fn restart_cycle_no_drain(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy_guests(c).await?;
        let mut load = Load::new(c.spec().seed);
        load.round(c).await?;
        let mut times = Vec::new();
        for node in c.node_ids() {
            load.round(c).await?;
            let t0 = Instant::now();
            harness(
                &format!("graceful restart of node {node}"),
                c.upgrade(node, None).await,
            );
            times.push((node, t0.elapsed()));
            wait_meta_leader(c).await?;
            load.signal_all(c).await?;
            // The restarted node is back in every ISR before the next one goes.
            let full = poll_async(CHANGE_T + LAG_MAX * 4, || async {
                (0..c.spec().partitions)
                    .all(|p| {
                        let a = published(c, p);
                        a.leader != 0 && a.isr.len() == a.replicas.len()
                    })
                    .then_some(())
            })
            .await;
            if full.is_none() {
                return Err(fail(
                    "availability",
                    format!("node {node} did not rejoin every ISR after its graceful restart"),
                ));
            }
        }
        println!("restart_cycle_no_drain: stop + restart times {times:?}");
        load.round(c).await?;
        load.signal_all(c).await?;
        load.outcomes(c).await?;
        final_checks_with(c, false, &load.acks()).await
    })
}
