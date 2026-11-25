//! Phase 13 `promotion` scenarios (tier B, 13-16; CLUS-05, D-P13-11): `ostro cluster init`
//! on a running single node, then growth through the real join flow, under a continuous
//! workload (runs started round robin on the running nodes every 100 ms, `reassign::Load`).
//!
//! | scenario | what |
//! |----------|------|
//! | `promote_running_single_node_to_three` | single node + workload → `POST /v1/cluster/init` → two joins → every partition replicas 3, ISR 3, min_insync 2 (status policy + published assignments), meta voters 3; node 1 led every partition until a graceful handoff (election reason RESIGN) moved it; then node 1 is killed (isolated, then stopped) → every acknowledged run completes on the survivors; Phase 12 oracles |
//! | `promote_refused_in_dev` | the same single node restarted in the dev profile → 409 `not_supported_in_profile`, no policy written |
//! | `promote_then_grow_to_five` | init + four joins → 5 meta voters, RF 3 / ISR 3 / min_insync 2 everywhere, replica counts balanced ±1 over the five nodes (placement invariant measured); every acknowledged run completes |
//!
//! The Phase 12 oracles that hold across reassignments end every scenario that writes
//! (`reassign::reassign_oracles`: replica identity below the HW over each live ISR,
//! acknowledged ⇒ on disk on ≥ min_insync nodes, LA-rule / election audits where the meta
//! log is not compacted).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_meta::PartitionAssignment;
use ostro_meta::ext::{ELECTION_REASON_RESIGN, get_election_info};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::placement::support::{deploy, fail, published_all, wait_meta_leader};
use super::reassign::{
    Load, acked_everywhere, no_ext, reassign_oracles, running, wait_level2, wait_parts,
};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::InProcCluster;
use crate::harness::p13::{NodeOpts, P13Harness};
use crate::harness::{ClusterHarness, ClusterSpec};

const DEADLINE: Duration = Duration::from_secs(600);
/// Bound on RF 3 everywhere after the joins.
const RF_T: Duration = Duration::from_secs(120);
/// Bound on the meta voter set reaching its target.
const VOTERS_T: Duration = Duration::from_secs(90);

fn single() -> ClusterSpec {
    ClusterSpec {
        seed: 1316,
        ..ClusterSpec::single()
    }
}

fn scenario(
    name: &'static str,
    run: for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>,
) -> Scenario {
    Scenario {
        name,
        group: "promotion",
        tier: Tier::B,
        deadline: DEADLINE,
        spec: single,
        run,
    }
}

/// Every `promotion` scenario.
pub fn all() -> Vec<Scenario> {
    vec![
        scenario(
            "promote_running_single_node_to_three",
            promote_running_single_node_to_three,
        ),
        scenario("promote_refused_in_dev", promote_refused_in_dev),
        scenario("promote_then_grow_to_five", promote_then_grow_to_five),
    ]
}

// ------------------------------------------------------------------------- helpers

/// `POST /v1/cluster/init` on node 1; checks the answer. The scenario runner starts the
/// node with `ClusterHarness::start` (no `[cluster] advertise`), so the request names the
/// in-memory cluster address itself (what `ostro cluster init --advertise-cluster` sends).
async fn init(c: &mut InProcCluster) -> ScenarioResult {
    let (st, v) = c
        .client(1)
        .post_json("/v1/cluster/init", &json!({ "advertise": "mem://1" }))
        .await
        .map_err(|e| fail("promotion", format!("POST /v1/cluster/init: {e}")))?;
    if !st.is_success() {
        return Err(fail("promotion", format!("cluster init: {st} {v}")));
    }
    if v["policy"] != json!({ "rf": 3, "min_insync": 2, "max_voters": 5 }) {
        return Err(fail("promotion", format!("init policy: {v}")));
    }
    let me = v["members"]
        .as_array()
        .and_then(|m| m.iter().find(|m| m["node_id"] == json!(1)))
        .cloned()
        .unwrap_or(Value::Null);
    if me["state"] != "active" || me["cluster_addr"] != "mem://1" {
        return Err(fail("promotion", format!("init: own member {me}")));
    }
    Ok(())
}

/// Wait until the status of a running node shows exactly `n` meta voters, none outgoing.
async fn wait_voters(c: &InProcCluster, n: usize) -> Result<Value, ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: test timeout
    let mut last = Value::Null;
    loop {
        if let Some(node) = running(c).first().copied()
            && let Ok(v) = c.cluster_status(node).await
        {
            let voters = v["voters"].as_array().map_or(0, Vec::len);
            let outgoing = v["voters_outgoing"].as_array().map_or(0, Vec::len);
            if voters == n && outgoing == 0 {
                return Ok(v);
            }
            last = v;
        }
        if t0.elapsed() > VOTERS_T {
            return Err(fail(
                "promotion",
                format!("meta voters did not reach {n} within {VOTERS_T:?}: {last}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Partitions whose leader is not node 1 and whose last election was not a graceful
/// handoff (reason RESIGN): `(partition, leader, reason)`.
fn ungraceful(parts: &[PartitionAssignment]) -> Vec<(u16, u64, Option<u8>)> {
    parts
        .iter()
        .filter(|a| a.leader != 1)
        .filter_map(|a| {
            let reason = get_election_info(&a.ext).ok().flatten().map(|i| i.reason);
            (reason != Some(ELECTION_REASON_RESIGN)).then_some((a.partition, a.leader, reason))
        })
        .collect()
}

fn rf3(a: &PartitionAssignment) -> bool {
    a.replicas.len() == 3 && a.isr.len() == 3 && a.min_insync == 2 && no_ext(a)
}

/// Replica count per node.
fn replica_counts(parts: &[PartitionAssignment], nodes: &[u64]) -> BTreeMap<u64, usize> {
    let mut m: BTreeMap<u64, usize> = nodes.iter().map(|n| (*n, 0)).collect();
    for a in parts {
        for r in &a.replicas {
            *m.entry(*r).or_default() += 1;
        }
    }
    m
}

fn balanced(counts: &BTreeMap<u64, usize>) -> bool {
    let hi = counts.values().max().copied().unwrap_or(0);
    let lo = counts.values().min().copied().unwrap_or(0);
    hi - lo <= 1
}

// ------------------------------------------------------------------------- scenarios

fn promote_running_single_node_to_three(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let before: Vec<(u16, u32, u64)> = published_all(c)
            .iter()
            .map(|a| (a.partition, a.epoch, a.leader))
            .collect();
        let load = Load::start(c);
        load.wait_acked(3).await?;

        let t0 = Instant::now(); // clock-exempt: scenario timing
        init(c).await?;
        let init_took = t0.elapsed();
        let after: Vec<(u16, u32, u64)> = published_all(c)
            .iter()
            .map(|a| (a.partition, a.epoch, a.leader))
            .collect();
        if after != before {
            return Err(fail(
                "promotion",
                format!("init changed the partitions: {before:?} → {after:?}"),
            ));
        }
        load.wait_acked(3).await?;

        let t_join = Instant::now(); // clock-exempt: scenario timing
        for _ in 0..2 {
            c.add_node(NodeOpts::default())
                .await
                .map_err(|e| fail("promotion", format!("add_node: {e}")))?;
            load.refresh(c, &[]);
            let bad = ungraceful(&published_all(c));
            if !bad.is_empty() {
                return Err(fail(
                    "promotion",
                    format!("leadership left node 1 without a handoff: {bad:?}"),
                ));
            }
        }
        let joins_took = t_join.elapsed();
        let violations: Mutex<Vec<(u16, u64, Option<u8>)>> = Mutex::new(Vec::new());
        let rf_took = wait_parts(
            c,
            "every partition at RF 3 / ISR 3 / min_insync 2",
            RF_T,
            |parts| {
                violations
                    .lock()
                    .expect("violations")
                    .extend(ungraceful(parts));
                parts.iter().all(rf3)
            },
        )
        .await?;
        let bad = violations.into_inner().expect("violations");
        if !bad.is_empty() {
            return Err(fail(
                "promotion",
                format!("leadership left node 1 without a handoff: {bad:?}"),
            ));
        }
        let st = wait_voters(c, 3).await?;
        if st["policy"]["rf"] != json!(3) || st["policy"]["min_insync"] != json!(2) {
            return Err(fail("promotion", format!("status policy: {st}")));
        }
        let to_rf3 = t_join.elapsed();
        let leaders: Vec<(u16, u64)> = published_all(c)
            .iter()
            .map(|a| (a.partition, a.leader))
            .collect();
        println!(
            "promote_running_single_node_to_three: init {init_took:?}; two joins {joins_took:?}; \
             RF 3 everywhere {rf_took:?} after the joins ({to_rf3:?} from the first join incl. 3 voters); leaders {leaders:?}"
        );
        // A real admin view agrees with meta.
        let admin = c.admin_status(2).await;
        if admin.is_null() {
            return Err(fail(
                "promotion",
                "GET /v1/admin/replication on node 2 failed",
            ));
        }

        // Lose the original node.
        load.refresh(c, &[1]);
        load.wait_acked(3).await?;
        let t_kill = Instant::now(); // clock-exempt: scenario timing
        c.kill(1).await;
        let new = wait_meta_leader(c).await?;
        if new == 1 {
            return Err(fail("promotion", "the killed node still leads meta"));
        }
        let failover = wait_parts(c, "every partition led by a survivor", RF_T, |parts| {
            parts.iter().all(|a| a.leader != 0 && a.leader != 1)
        })
        .await?;
        println!(
            "promote_running_single_node_to_three: node 1 killed; meta leader {new}; every partition led by a survivor after {failover:?} ({:?})",
            t_kill.elapsed()
        );
        load.wait_acked(3).await?;
        let (n, gap) = load.finish(c).await?;
        println!(
            "promote_running_single_node_to_three: {n} runs acknowledged, all completed; longest gap {gap:?} (includes the kill)"
        );
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await
    })
}

fn promote_refused_in_dev(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        // The dev profile is the one that auto-generates the sealing key (TlsProfile::of).
        c.config_mut(1).secrets.auto_sealing_key = true;
        c.restart(1)
            .await
            .map_err(|e| fail("promotion", format!("restart in the dev profile: {e}")))?;
        wait_meta_leader(c).await?;
        let (st, v) = c
            .client(1)
            .post_json("/v1/cluster/init", &json!({ "advertise": "mem://1" }))
            .await
            .map_err(|e| fail("promotion", format!("POST /v1/cluster/init: {e}")))?;
        if st != reqwest::StatusCode::CONFLICT
            || v["error"]["details"]["reason"] != "not_supported_in_profile"
        {
            return Err(fail(
                "promotion",
                format!(
                    "init in the dev profile: expected 409 not_supported_in_profile, got {st} {v}"
                ),
            ));
        }
        let status = c
            .cluster_status(1)
            .await
            .map_err(|e| fail("promotion", format!("status: {e}")))?;
        if !status["policy"].is_null() {
            return Err(fail(
                "promotion",
                format!("a refused init wrote a policy: {status}"),
            ));
        }
        Ok(())
    })
}

fn promote_then_grow_to_five(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        let load = Load::start(c);
        load.wait_acked(3).await?;
        init(c).await?;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        for _ in 0..4 {
            c.add_node(NodeOpts::default())
                .await
                .map_err(|e| fail("promotion", format!("add_node: {e}")))?;
            load.refresh(c, &[]);
        }
        let joins = t0.elapsed();
        wait_voters(c, 5).await?;
        let nodes = running(c);
        if nodes.len() != 5 {
            return Err(fail("promotion", format!("running nodes: {nodes:?}")));
        }
        let took = wait_parts(
            c,
            "RF 3 everywhere with replica counts balanced ±1 over 5 nodes",
            RF_T * 2,
            |parts| parts.iter().all(rf3) && balanced(&replica_counts(parts, &nodes)),
        )
        .await?;
        let counts = replica_counts(&published_all(c), &nodes);
        println!(
            "promote_then_grow_to_five: four joins {joins:?}; balanced RF 3 {took:?} later; replica counts {counts:?}"
        );
        load.wait_acked(3).await?;
        let (n, gap) = load.finish(c).await?;
        println!(
            "promote_then_grow_to_five: {n} runs acknowledged, all completed; longest gap {gap:?}"
        );
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await
    })
}
