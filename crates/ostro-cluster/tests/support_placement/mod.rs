//! Placement controller test bed (13-10): an in-process 3-voter metadata group (the 13-09
//! `support_membership` router), one `PlacementController` per meta node ticked by the test,
//! partition-holding members 11.. (the meta voters' own records are Joining, so they are
//! neither placement members nor evacuated), a fake partition layer that publishes an honest
//! ISR (replicas that are live and not lagging, plus the leader) through `AlterIsr`, and a
//! fake `HandoffClient` that elects the target with the ISR unchanged (the 13-11 rule).

#![allow(dead_code)]

#[path = "../support_membership/mod.rs"]
pub mod support_membership;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use ostro_cluster::contracts::{HandoffClient, HandoffError, HandoffOutcome};
use ostro_cluster::placement::controller::{
    PlacementConfig, PlacementController, TickReport, observe,
};
use ostro_meta::ext::{
    ClusterVersion, ELECTION_REASON_RESIGN, ElectionInfo, MEMBER_JOINING, ReplicationPolicy,
    put_cluster_version, put_replication_policy,
};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, PartitionAssignment};
use ostro_partition::{Clock as _, ManualClock};
use ostro_types::{Epoch, PartitionId};
pub use support_membership::Group;

pub const P: u16 = 4;
pub const WITHIN: Duration = Duration::from_secs(60);

/// Elects `to` at `epoch + 1` with the ISR unchanged (13-11's ElectLeader shape).
pub struct FakeHandoff {
    pub meta: Mutex<Option<Arc<MetaHandle>>>,
    pub calls: Mutex<Vec<(u16, u64, u64)>>,
}

impl FakeHandoff {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            meta: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
        })
    }
}

impl HandoffClient for FakeHandoff {
    fn hand_off(
        &self,
        leader: u64,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>> {
        self.calls.lock().unwrap().push((p.0, leader, to));
        let meta = self.meta.lock().unwrap().clone();
        Box::pin(async move {
            let meta = meta.ok_or_else(|| HandoffError::Transport("no meta".into()))?;
            let a = meta
                .view()
                .read(|s| s.partition(p.0).map(|v| v.value.clone()))
                .ok_or_else(|| HandoffError::Meta("no partition".into()))?;
            if a.epoch != epoch.0 || a.leader != leader {
                return Err(HandoffError::NotLeader {
                    partition: p.0,
                    epoch: epoch.0,
                });
            }
            if !a.isr.contains(&to) {
                return Err(HandoffError::TargetNotInIsr {
                    partition: p.0,
                    target: to,
                });
            }
            let r = meta
                .propose(MetaCommand::ElectLeader {
                    partition: p.0,
                    epoch: epoch.0 + 1,
                    leader: to,
                    isr: a.isr.clone(),
                    expected_isr_version: a.isr_version,
                    info: ElectionInfo {
                        reason: ELECTION_REASON_RESIGN,
                        elected_at_ms: 1,
                        prev_leader: leader,
                    },
                })
                .await
                .map_err(|e| HandoffError::Meta(e.to_string()))?;
            match r {
                MetaResult::Ok { .. } => Ok(HandoffOutcome::Moved {
                    new_epoch: Epoch(epoch.0 + 1),
                }),
                other => Ok(HandoffOutcome::Aborted {
                    reason: format!("{other:?}"),
                }),
            }
        })
    }
}

/// The test bed.
pub struct Bed {
    pub g: Group,
    pub ctl: BTreeMap<u64, PlacementController>,
    pub handoff: Arc<FakeHandoff>,
    pub clock: Arc<ManualClock>,
    /// Partition nodes whose fetch never catches up (kept out of the ISR).
    pub lagging: Arc<Mutex<BTreeSet<u64>>>,
    /// Partition nodes that are down (out of the ISR, not live).
    pub down: Arc<Mutex<BTreeSet<u64>>>,
    pub cfg: PlacementConfig,
    /// Most partitions with ext 100 at once (observed after every round).
    pub max_in_flight: usize,
    /// Rounds in which some partition had |ISR| < min_insync (writes would stop).
    pub stalled: Vec<String>,
}

pub fn assignment(
    p: u16,
    leader: u64,
    replicas: &[u64],
    isr: &[u64],
    min: u8,
) -> PartitionAssignment {
    PartitionAssignment {
        partition: p,
        leader,
        epoch: 1,
        isr: isr.to_vec(),
        replicas: replicas.to_vec(),
        isr_version: 0,
        min_insync: min,
        ext: Vec::new(),
    }
}

impl Bed {
    /// A group (meta voters 1, 2, 3, records Joining), `ClusterInfo` with `P` partitions,
    /// partition nodes `members` (Active), the partitions placed as `placed` (epoch 1),
    /// finalized at `finalized` (None = absent = LEVEL_MIN), optional policy.
    pub async fn new(
        members: &[u64],
        placed: Vec<PartitionAssignment>,
        finalized: Option<u32>,
        policy: Option<ReplicationPolicy>,
    ) -> Bed {
        let g = Group::new(&[1, 2, 3]).await;
        for id in [1, 2, 3] {
            g.put_member(id, Some(MEMBER_JOINING)).await;
        }
        for &id in members {
            g.put_member(id, None).await;
        }
        let leader = g.leader_handle().await;
        let r = leader
            .propose(MetaCommand::InitCluster {
                cluster_id: "c".into(),
                node_id: members[0],
                partitions: P,
                created_at_ms: 1,
                token_key: [1; 32],
            })
            .await
            .unwrap();
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
        let mut bed = Bed {
            g,
            ctl: BTreeMap::new(),
            handoff: FakeHandoff::new(),
            clock: Arc::new(ManualClock::new(1_000_000)),
            lagging: Arc::new(Mutex::new(BTreeSet::new())),
            down: Arc::new(Mutex::new(BTreeSet::new())),
            cfg: PlacementConfig {
                max_concurrent_moves: 2,
                leader_rebalance_interval: Duration::from_secs(300),
                interval: Duration::from_millis(20),
                level_max: ostro_types::compat::LEVEL,
            },
            max_in_flight: 0,
            stalled: Vec::new(),
        };
        bed.set_cluster(finalized, policy).await;
        for a in placed {
            bed.put_partition(a).await;
        }
        *bed.handoff.meta.lock().unwrap() = Some(leader);
        for id in [1, 2, 3] {
            bed.add_controller(id);
        }
        bed
    }

    pub fn add_controller(&mut self, id: u64) {
        let down = Arc::clone(&self.down);
        let c = PlacementController::new(
            Arc::clone(&self.g.nodes[&id]),
            self.handoff.clone(),
            self.cfg.clone(),
            self.clock.clone(),
        )
        .with_live_source(Arc::new(move |u: &BTreeSet<u64>| {
            let down = down.lock().unwrap();
            u.iter().copied().filter(|id| !down.contains(id)).collect()
        }));
        self.ctl.insert(id, c);
    }

    pub async fn set_cluster(&self, finalized: Option<u32>, policy: Option<ReplicationPolicy>) {
        let leader = self.g.leader_handle().await;
        let (mut info, version) = leader
            .view()
            .read(|s| s.cluster().map(|c| (c.value.clone(), c.version)))
            .unwrap();
        if let Some(f) = finalized {
            info.ext = put_cluster_version(
                &info.ext,
                &ClusterVersion {
                    finalized: f,
                    hold: None,
                    finalized_at_ms: 1,
                },
            )
            .unwrap();
        }
        if let Some(p) = policy {
            info.ext = put_replication_policy(&info.ext, &p).unwrap();
        }
        let r = leader
            .propose(MetaCommand::Put {
                key: MetaKey::Cluster,
                expected_version: Some(version),
                record: MetaRecord::Cluster(info),
            })
            .await
            .unwrap();
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    }

    pub async fn put_partition(&self, a: PartitionAssignment) {
        let leader = self.g.leader_handle().await;
        let cur = leader
            .view()
            .read(|s| s.partition(a.partition).map(|v| v.version))
            .unwrap();
        let r = leader
            .propose(MetaCommand::Put {
                key: MetaKey::Partition(a.partition),
                expected_version: Some(cur),
                record: MetaRecord::Partition(a),
            })
            .await
            .unwrap();
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    }

    pub async fn leader(&self) -> Arc<MetaHandle> {
        self.g.leader_handle().await
    }

    /// A meta node that is not the leader.
    pub async fn follower(&self) -> Arc<MetaHandle> {
        let l = self.g.leader().await;
        let id = *self
            .g
            .nodes
            .keys()
            .find(|id| **id != l)
            .expect("a follower");
        Arc::clone(&self.g.nodes[&id])
    }

    pub async fn partitions(&self) -> Vec<PartitionAssignment> {
        self.leader()
            .await
            .view()
            .read(|s| s.partitions().map(|v| v.value.clone()).collect())
    }

    pub async fn part(&self, p: u16) -> PartitionAssignment {
        self.leader()
            .await
            .view()
            .read(|s| s.partition(p).unwrap().value.clone())
    }

    /// The fake partition layer: publish the honest ISR of every led partition.
    pub async fn progress(&self) {
        let leader = self.leader().await;
        let parts = leader
            .view()
            .read(|s| s.partitions().map(|v| v.value.clone()).collect::<Vec<_>>());
        let lagging = self.lagging.lock().unwrap().clone();
        let down = self.down.lock().unwrap().clone();
        for a in parts {
            if a.epoch == 0 || a.leader == 0 || down.contains(&a.leader) {
                continue;
            }
            let isr: Vec<u64> = a
                .replicas
                .iter()
                .copied()
                .filter(|r| *r == a.leader || (!lagging.contains(r) && !down.contains(r)))
                .collect();
            if isr != a.isr {
                let _ = leader
                    .propose(MetaCommand::AlterIsr {
                        partition: a.partition,
                        epoch: a.epoch,
                        expected_isr_version: a.isr_version,
                        isr,
                    })
                    .await;
            }
        }
    }

    /// One round: partition progress, a tick of every controller, a short pause for spawned
    /// handoffs; then the invariants (in-flight bound, writes flowing) are sampled.
    pub async fn round(&mut self, force: bool) -> Vec<TickReport> {
        self.progress().await;
        let mut reports = Vec::new();
        for c in self.ctl.values() {
            let r = c.tick(force).await;
            if r.leader {
                reports.push(r);
            }
        }
        self.clock.advance(500);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let leader = self.leader().await;
        let obs = leader.view().read(observe);
        let in_flight = obs
            .partitions
            .iter()
            .filter(|p| p.reassignment.is_some())
            .count();
        self.max_in_flight = self.max_in_flight.max(in_flight);
        for p in &obs.partitions {
            if p.isr.len() < usize::from(p.min_insync) {
                self.stalled.push(format!(
                    "p{} isr {:?} < min_insync {}",
                    p.partition, p.isr, p.min_insync
                ));
            }
        }
        reports
    }

    /// Rounds until `f` holds on the leader's partitions (bounded).
    pub async fn drive_until(
        &mut self,
        what: &str,
        mut f: impl FnMut(&[PartitionAssignment]) -> bool,
    ) -> Vec<TickReport> {
        let deadline = Instant::now() + WITHIN;
        let mut log = Vec::new();
        loop {
            log.extend(self.round(false).await);
            let parts = self.partitions().await;
            if f(&parts) {
                return log;
            }
            assert!(
                Instant::now() < deadline,
                "timed out driving to {what}: {parts:#?}"
            );
        }
    }

    /// Kill meta node `id` (its controller first).
    pub async fn kill_meta(&mut self, id: u64) {
        self.ctl.remove(&id);
        self.g.kill(id).await;
        let leader = self.leader().await;
        *self.handoff.meta.lock().unwrap() = Some(leader);
    }

    pub async fn stop(mut self) {
        self.ctl.clear();
        *self.handoff.meta.lock().unwrap() = None;
        self.g.stop_all().await;
    }
}

/// Sorted copy.
pub fn sorted(v: &[u64]) -> Vec<u64> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v
}

pub fn now_ms(clock: &ManualClock) -> u64 {
    clock.now_ms()
}
