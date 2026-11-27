//! `kill -9` nemeses (14-09, D-P14-19) on [`ProcCluster`]: real process death (`SIGKILL` of
//! our own child, `waitpid` must report signal 9) and a restart from the retained data dir
//! and config after a seeded delay. No root, no Docker.
//!
//! At most a minority is down at once: a single-node kill is skipped (and reported) while
//! another node is down. [`quorum_loss`] is the explicit exception (two of three nodes): the
//! checkers then expect unavailability during the fault, never data loss.

use std::time::Duration;

use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::process::{KillOutcome, ProcCluster};
use ostro_repl_conformance::scenarios::process::tools;

/// Whom to kill.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum KillTarget {
    /// The leader of a partition.
    Leader,
    /// A follower of a partition.
    Follower,
    /// The metadata (Raft) leader.
    MetaLeader,
    /// Any node.
    Any,
}

impl KillTarget {
    /// Every target.
    pub const ALL: [KillTarget; 4] = [
        KillTarget::Leader,
        KillTarget::Follower,
        KillTarget::MetaLeader,
        KillTarget::Any,
    ];

    /// Schedule / report name.
    pub fn name(self) -> &'static str {
        match self {
            KillTarget::Leader => "kill:leader",
            KillTarget::Follower => "kill:follower",
            KillTarget::MetaLeader => "kill:meta_leader",
            KillTarget::Any => "kill:any",
        }
    }
}

fn nth(nodes: &[u64], i: u64) -> u64 {
    nodes[(i % nodes.len() as u64) as usize]
}

/// Resolve `target` with choice `pick` to a running node and a description.
pub async fn choose(c: &ProcCluster, target: KillTarget, pick: u64) -> Option<(u64, String)> {
    let live = tools::live(c);
    if live.is_empty() {
        return None;
    }
    let p = (pick % u64::from(c.spec().partitions.max(1))) as u16;
    let found = match target {
        KillTarget::Leader => {
            let l = tools::published(c, p).leader;
            live.contains(&l)
                .then(|| (l, format!("partition {p} leader")))
        }
        KillTarget::Follower => {
            let a = tools::published(c, p);
            let f: Vec<u64> = a
                .replicas
                .iter()
                .copied()
                .filter(|n| *n != a.leader && live.contains(n))
                .collect();
            (!f.is_empty()).then(|| (nth(&f, pick / 7), format!("partition {p} follower")))
        }
        KillTarget::MetaLeader => tools::meta_leader(c)
            .await
            .map(|n| (n, "meta leader".to_owned())),
        KillTarget::Any => None,
    };
    Some(found.unwrap_or_else(|| (nth(&live, pick), "node".to_owned())))
}

/// `kill -9` `node` (it must die by signal 9 or have exited already).
pub async fn kill(c: &mut ProcCluster, node: u64) -> Result<KillOutcome, String> {
    match c.kill9(node).await {
        Ok(KillOutcome::NotRunning) => Err(format!("node {node} was not running")),
        Ok(o) => Ok(o),
        Err(e) => Err(format!("kill -9 node {node}: {e}")),
    }
}

/// Restart `node` from its data dir (its links are healed first); how long until ready.
pub async fn restart(c: &mut ProcCluster, node: u64) -> Result<Duration, String> {
    c.restart(node)
        .await
        .map_err(|e| format!("restart node {node}: {e}\n{}", c.log_tail(node, 30)))?;
    Ok(c.timings().last_start)
}

/// Kill a majority (two of three nodes, chosen by `pick`); the killed nodes.
pub async fn quorum_loss(c: &mut ProcCluster, pick: u64) -> Result<Vec<u64>, String> {
    let nodes = c.node_ids();
    let keep = nth(&nodes, pick);
    let victims: Vec<u64> = nodes.into_iter().filter(|n| *n != keep).collect();
    for n in &victims {
        kill(c, *n).await?;
    }
    Ok(victims)
}

/// Restart several nodes at once (a quorum must boot together).
pub async fn restart_many(c: &mut ProcCluster, nodes: &[u64]) -> Result<Duration, String> {
    c.restart_many(nodes)
        .await
        .map_err(|e| format!("restart {nodes:?}: {e}"))?;
    Ok(c.timings().last_start)
}
