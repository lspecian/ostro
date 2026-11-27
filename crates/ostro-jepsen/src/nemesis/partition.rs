//! Network partition nemeses (14-09, D-P14-19) on the Phase 12 chaos proxy: every
//! node-to-node link of a [`ProcCluster`] goes through a `ChaosProxy` listener, so a
//! partition is a proxy rule, never root, netns or Docker. Client traffic (the API
//! listeners) is not proxied: clients keep reaching every live node, and the nodes cut off
//! from the majority must refuse or forward what they cannot serve.
//!
//! | mode | effect (3 nodes) |
//! |------|------------------|
//! | [`PartitionMode::MajorityMinority`] | one node isolated from the other two |
//! | [`PartitionMode::Bridge`] | two nodes cut from each other; the bridge sees both |
//! | [`PartitionMode::OneWay`] | connections `a → b` refused and reset; `b → a` intact |
//! | [`PartitionMode::LeaderIsolation`] | the metadata leader or a partition leader isolated |
//!
//! Targets are chosen at injection time from the schedule's `pick` (the schedule itself is
//! seeded; who leads at that moment is not).

use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::scenarios::process::tools;

/// A partition shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionMode {
    /// Isolate one node (minority) from the rest (majority).
    MajorityMinority,
    /// Cut two nodes from each other; a third (the bridge) still sees both.
    Bridge,
    /// Block `a → b` only.
    OneWay,
    /// Isolate the current metadata leader or a partition leader.
    LeaderIsolation,
}

impl PartitionMode {
    /// Every mode.
    pub const ALL: [PartitionMode; 4] = [
        PartitionMode::MajorityMinority,
        PartitionMode::Bridge,
        PartitionMode::OneWay,
        PartitionMode::LeaderIsolation,
    ];

    /// Schedule / report name.
    pub fn name(self) -> &'static str {
        match self {
            PartitionMode::MajorityMinority => "partition:majority_minority",
            PartitionMode::Bridge => "partition:bridge",
            PartitionMode::OneWay => "partition:one_way",
            PartitionMode::LeaderIsolation => "partition:leader_isolation",
        }
    }
}

fn nth(nodes: &[u64], i: u64) -> u64 {
    nodes[(i % nodes.len() as u64) as usize]
}

/// Isolate `node` from every other node; a description of the cut.
fn isolate(c: &mut ProcCluster, node: u64, why: &str) -> String {
    c.isolate(node);
    let rest: Vec<u64> = c.node_ids().into_iter().filter(|n| *n != node).collect();
    format!("{why} {node} isolated from {rest:?}")
}

/// Inject `mode` with target choice `pick`; the resolved target.
pub async fn inject(c: &mut ProcCluster, mode: PartitionMode, pick: u64) -> Result<String, String> {
    let nodes = c.node_ids();
    if nodes.len() < 3 {
        return Err(format!(
            "{}: needs 3 nodes, cluster has {}",
            mode.name(),
            nodes.len()
        ));
    }
    Ok(match mode {
        PartitionMode::MajorityMinority => isolate(c, nth(&nodes, pick), "minority"),
        PartitionMode::Bridge => {
            let b = nth(&nodes, pick);
            let rest: Vec<u64> = nodes.iter().copied().filter(|n| *n != b).collect();
            let (x, y) = (rest[0], rest[1]);
            c.partition(x, y);
            format!("bridge {b}: {x} x {y}")
        }
        PartitionMode::OneWay => {
            let a = nth(&nodes, pick);
            let rest: Vec<u64> = nodes.iter().copied().filter(|n| *n != a).collect();
            let b = nth(&rest, pick / nodes.len() as u64);
            c.proxy().cut(a, b);
            format!("one-way {a} -> {b} cut")
        }
        PartitionMode::LeaderIsolation => {
            let leader = if pick.is_multiple_of(2) {
                tools::meta_leader(c)
                    .await
                    .map(|n| (n, "meta leader".to_owned()))
            } else {
                let p = ((pick / 2) % u64::from(c.spec().partitions)) as u16;
                let l = tools::published(c, p).leader;
                (l != 0 && c.pid(l).is_some()).then(|| (l, format!("partition {p} leader")))
            };
            match leader {
                Some((n, why)) => isolate(c, n, &why),
                None => isolate(c, nth(&nodes, pick), "no leader found; minority"),
            }
        }
    })
}

/// Clear every proxy rule (every link of every node).
pub fn heal(c: &mut ProcCluster) {
    c.proxy().heal_all();
    for n in c.node_ids() {
        c.heal(n);
    }
}
