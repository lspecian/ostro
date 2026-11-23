//! CLUS-02 group `fetch_path` (12-07): the follower fetch loop.
//!
//! - **RP-12, separate timeouts:** a fetch that takes longer than `fetch_max_wait` to be
//!   answered still completes and is used. Tier B cannot stall the leader's disk read without
//!   a failpoint (failpoints live only in `tests/mutants.rs`), so every `Fetch` of one
//!   follower is held by the fault layer for several times `fetch_max_wait`. A follower that
//!   bounded its RPC by the idle wait would never complete a fetch; a correct one stays in
//!   the ISR and reaches the leader's log end.
//! - **Pitfall 6, maximum-size envelope:** a single envelope of almost
//!   `DEFAULT_MAX_ENVELOPE_BYTES` (4 MiB) replicates. It is appended through the leader's
//!   partition handle (one `submit` = one envelope; the API never batches that much) while
//!   the follower is cut off, so its catch-up fetch must carry the envelope plus more, above
//!   tonic's default 4 MiB decode limit.
//! - **Batching on user partitions:** under API load, with each fetch held a little, one
//!   fetch response carries at least two envelopes: the follower's measured log grows by two
//!   or more records between two samples taken closer together than one fetch round.

use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use ostro_types::command::{ConflictPolicy, StartRun};
use ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES;
use ostro_types::{Command, Offset, PartitionId, PayloadRef, RunId};
use tokio::time::Instant;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, Start, append_acked, blackhole, clear, delay, deploy, durable_end,
    fail, final_checks_with, join_starts, pause, pick, poll, published, scan, spawn_starts, within,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster, TEST_WORKFLOW};
use crate::harness::{ClusterHarness as _, ClusterSpec};

/// The `fetch_path` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, spec, run: Run| Scenario {
        name,
        group: "fetch_path",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec,
        run,
    };
    vec![
        s(
            "slow_read_longer_than_max_wait_completes",
            spec_slow,
            slow_read_longer_than_max_wait_completes,
        ),
        s(
            "max_size_envelope_replicates",
            spec_max,
            max_size_envelope_replicates,
        ),
        s(
            "batching_on_user_partitions",
            spec_batch,
            batching_on_user_partitions,
        ),
    ]
}

fn spec_slow() -> ClusterSpec {
    ClusterSpec {
        seed: 721,
        ..ClusterSpec::default()
    }
}

fn spec_max() -> ClusterSpec {
    ClusterSpec {
        seed: 722,
        ..ClusterSpec::default()
    }
}

fn spec_batch() -> ClusterSpec {
    ClusterSpec {
        seed: 723,
        ..ClusterSpec::default()
    }
}

/// The follower whose fetches are slowed or cut.
const SLOW: u64 = 3;
/// `[replication] fetch_max_wait` of the tier-B node config.
const MAX_WAIT: Duration = Duration::from_millis(100);
/// Per-fetch hold for RP-12: four times the idle wait, well inside `replica_lag_max`.
const HOLD_MS: u64 = 400;
/// Bound on the slow follower reaching the leader's end.
const CATCHUP_T: Duration = Duration::from_secs(30);

/// A partition led by a node other than [`SLOW`] and replicated on it.
fn followed_by_slow(
    c: &InProcCluster,
) -> Result<ostro_meta::PartitionAssignment, super::ScenarioFailure> {
    pick(c, "led elsewhere and replicated on node 3", |a| {
        a.leader != 0 && a.leader != SLOW && a.replicas.contains(&SLOW)
    })
}

/// Wait until `node`'s measured durable end of `p` reaches `target`.
async fn reaches(
    c: &InProcCluster,
    node: u64,
    p: u16,
    target: Offset,
    limit: Duration,
    why: &str,
) -> ScenarioResult {
    let got = poll(limit, || {
        durable_end(c, node, p)
            .ok()
            .filter(|e| *e >= target)
            .map(|_| ())
    })
    .await;
    got.ok_or_else(|| {
        let at = durable_end(c, node, p).map(|e| e.0).unwrap_or_default();
        fail(
            "fetch_path",
            format!(
                "partition {p}: node {node} stuck at {at} below {} for {limit:?}: {why}",
                target.0
            ),
        )
    })
}

/// RP-12: every fetch of node 3 is held [`HOLD_MS`] (> `fetch_max_wait`) before the leader
/// sees it; node 3 still replicates everything and never leaves the ISR.
fn slow_read_longer_than_max_wait_completes(
    c: &mut InProcCluster,
) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        assert!(Duration::from_millis(HOLD_MS) > MAX_WAIT * 2);
        deploy(c).await?;
        let a = followed_by_slow(c)?;
        let (p, leader) = (a.partition, a.leader);
        let mut acks = append_acked(c, p, 2, ACK_T).await?;
        delay(c, SLOW, leader, RpcKind::Fetch, HOLD_MS);
        // clock-exempt: test window
        let end = Instant::now() + Duration::from_secs(6);
        while Instant::now() < end {
            acks.extend(append_acked(c, p, 1, ACK_T).await?);
            let isr = published(c, p).isr;
            if !isr.contains(&SLOW) {
                return Err(fail(
                    "fetch_path",
                    format!(
                        "partition {p}: node {SLOW} left the ISR {isr:?} while its fetches took {HOLD_MS} ms (> fetch_max_wait {MAX_WAIT:?}, < replica_lag_max {LAG_MAX:?})"
                    ),
                ));
            }
            pause(Duration::from_millis(200)).await;
        }
        let target = durable_end(c, leader, p)?;
        reaches(
            c,
            SLOW,
            p,
            target,
            CATCHUP_T,
            "fetches answered after fetch_max_wait were not used",
        )
        .await?;
        clear(c, SLOW, leader);
        final_checks_with(c, false, &acks).await
    })
}

/// Inline input per command of the big envelope (below the 256 KiB inline limit).
const BIG_INPUT: usize = 240 * 1024;
/// Commands in the big envelope: as many [`BIG_INPUT`]s as fit with 64 KiB to spare.
const BIG_CMDS: usize = (DEFAULT_MAX_ENVELOPE_BYTES - 64 * 1024) / (BIG_INPUT + 512);

fn big_start(p: u16, i: usize) -> Command {
    let run = RunId::new_v7(PartitionId(p));
    let fill = b'a' + (i % 26) as u8;
    Command::start_run(StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: TEST_WORKFLOW.into(),
        workflow_version: 1,
        queue: "default".into(),
        input: Some(PayloadRef::inline(Bytes::from(vec![fill; BIG_INPUT]))),
        idempotency_key: Vec::new(),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: String::new(),
        priority: 0,
        ready_at_ms: 0,
        requested_at_ms: 0,
        parent: None,
        rng_seed: vec![7; 32],
        ..Default::default()
    })
}

/// Pitfall 6: node 3 is cut off while the leader commits (with the other follower) one
/// envelope of almost 4 MiB and a few small ones; healed, node 3 must fetch past it.
fn max_size_envelope_replicates(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let a = followed_by_slow(c)?;
        let (p, leader) = (a.partition, a.leader);
        let mut acks: Vec<Ack> = append_acked(c, p, 2, ACK_T).await?;
        blackhole(c, SLOW, leader, RpcKind::Fetch);
        let handle = c
            .handle(leader)
            .and_then(|h| h.node().handle(PartitionId(p)))
            .unwrap_or_else(|| panic!("harness: leader {leader} has no handle for {p}"));
        let cmds: Vec<Command> = (0..BIG_CMDS).map(|i| big_start(p, i)).collect();
        match within(CHANGE_T, handle.submit(cmds)).await {
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                return Err(fail(
                    "fetch_path",
                    format!(
                        "partition {p}: leader {leader} rejected a {BIG_CMDS}-command envelope: {e}"
                    ),
                ));
            }
            None => {
                return Err(fail(
                    "fetch_path",
                    format!("partition {p}: the big envelope did not commit within {CHANGE_T:?}"),
                ));
            }
        }
        acks.extend(append_acked(c, p, 3, ACK_T).await?);
        let leader_scan = scan(c, leader, p)?;
        let biggest = leader_scan
            .records
            .iter()
            .max_by_key(|r| r.envelope.len())
            .unwrap_or_else(|| panic!("harness: leader {leader} log of {p} is empty"));
        if biggest.envelope.len() < 3 * 1024 * 1024 {
            panic!(
                "harness: the largest envelope is {} bytes, not near the 4 MiB maximum",
                biggest.envelope.len()
            );
        }
        let target = leader_scan.durable_end().unwrap_or(Offset::ZERO);
        tracing::info!(
            bytes = biggest.envelope.len(),
            base = biggest.base.0,
            "max-size envelope committed"
        );
        clear(c, SLOW, leader);
        reaches(
            c,
            SLOW,
            p,
            target,
            CATCHUP_T,
            "a fetch carrying a maximum-size envelope never succeeded",
        )
        .await?;
        final_checks_with(c, false, &acks).await
    })
}

/// Concurrent API starts per batching round.
const LOAD: usize = 40;

/// Under API load, with node 3's fetches held [`HOLD_MS`], one fetch response carries two
/// or more envelopes.
fn batching_on_user_partitions(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let a = followed_by_slow(c)?;
        let (p, leader) = (a.partition, a.leader);
        let mut acks: Vec<Ack> = append_acked(c, p, 2, ACK_T).await?;
        delay(c, SLOW, leader, RpcKind::Fetch, HOLD_MS);
        let mut prev = (Instant::now(), scan(c, SLOW, p)?.records.len());
        let mut best = 0usize;
        let mut tasks = Vec::new();
        // clock-exempt: test window
        let end = Instant::now() + Duration::from_secs(8);
        while Instant::now() < end {
            if tasks.len() < 4 * LOAD {
                tasks.extend(spawn_starts(c, leader, p, LOAD / 4, "batch", CATCHUP_T).1);
            }
            pause(Duration::from_millis(50)).await;
            let now = (Instant::now(), scan(c, SLOW, p)?.records.len());
            // Two fetch responses are at least HOLD_MS apart: a jump inside a shorter
            // window came from one response.
            if now.0.duration_since(prev.0) < Duration::from_millis(HOLD_MS - 50) {
                best = best.max(now.1.saturating_sub(prev.1));
            }
            prev = now;
        }
        clear(c, SLOW, leader);
        for s in join_starts(tasks).await {
            if let Start::Acked(a) = s {
                acks.push(a);
            }
        }
        if best < 2 {
            return Err(fail(
                "fetch_path",
                format!(
                    "partition {p}: no fetch of node {SLOW} carried more than {best} envelope(s) under load"
                ),
            ));
        }
        let status = c.admin_status(leader).await;
        if let Some(cap) = status["partitions"].as_array().and_then(|ps| {
            ps.iter()
                .find(|e| e["partition"].as_u64() == Some(u64::from(p)))
                .and_then(|e| e["max_envelopes_per_fetch"].as_u64())
        }) && cap < 2
        {
            return Err(fail(
                "status",
                format!("partition {p}: max_envelopes_per_fetch {cap} < 2"),
            ));
        }
        final_checks_with(c, false, &acks).await
    })
}
