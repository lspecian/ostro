//! The partition controller (12-10, research Pattern 4, D-P12-7/9/14).
//!
//! [`Controller::run`] runs on every node but acts only while the node leads the metadata
//! Raft group (it goes idle on leadership loss and re-seeds liveness when it gains it). Each
//! tick it reads the meta partition assignments and:
//!
//! 1. **places** every epoch-0 partition once `ClusterInfo` exists, for any voter count
//!    ([`crate::bootstrap`]: `Put(Partition)` CAS from version 0);
//! 2. runs the pure 12-04 planner [`failover::plan`] over (assignment, live set, resigned
//!    node) and turns its answer into `ElectLeader` (`Elect`, `Offline`) or `AlterIsr`
//!    (`Shrink`) carrying the current epoch and `isr_version`.
//!
//! It never proposes the legacy partition-leader fence command, and `Put(Partition)` only
//! for the bootstrap.
//! Meta apply re-validates every command (12-05), so a planner bug surfaces as `Invalid`:
//! logged at error and never retried with the same command.
//!
//! Fresh terms ([`request_fresh_term`]) and resignations ([`resign`]) are not part of the
//! loop: the partition role lifecycle (12-13) calls them through `AssignmentSource`.
//!
//! Timing: wall-clock values (election timestamps, liveness windows) come from the injected
//! [`Clock`]; the loop period is a tokio interval (`ControllerConfig::tick`).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use ostro_log::replica::failover::{self, AssignmentIn, Plan};
use ostro_meta::ext::{
    ELECTION_REASON_FAILOVER, ELECTION_REASON_OFFLINE, ELECTION_REASON_RESIGN,
    ELECTION_REASON_STARTUP, ElectionInfo, get_election_info,
};
use ostro_meta::{
    MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult, PartitionAssignment,
};
use ostro_partition::Clock;
use tokio::sync::{Semaphore, broadcast};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::bootstrap;
use crate::liveness::LiveSource;

/// Controller tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControllerConfig {
    /// Loop period (default 100 ms).
    pub tick: Duration,
    /// Bootstrap replication factor (`[replication] rf`, clamped to the voter count).
    pub rf: usize,
    /// Bootstrap `min_insync` (`[replication] min_insync`, clamped to `1..=rf`).
    pub min_insync: u8,
    /// Proposals in flight at once (one per partition at most).
    pub max_in_flight: usize,
    /// How long a resigned leader is skipped by the planner (default 10 s). Afterwards it
    /// may be elected again if it is live and still in the ISR (it restarted healthy, or
    /// resigns again).
    pub resign_hold: Duration,
}

impl ControllerConfig {
    /// Defaults with the given bootstrap seeds.
    pub fn new(rf: usize, min_insync: u8) -> ControllerConfig {
        ControllerConfig {
            tick: Duration::from_millis(100),
            rf,
            min_insync,
            max_in_flight: 32,
            resign_hold: Duration::from_secs(10),
        }
    }
}

/// One proposal the controller made, with the assignment it planned from and the verdict
/// (published on [`Controller::subscribe`]; tests audit it, 12-14 may count it).
#[derive(Clone, Debug)]
pub struct ProposalRecord {
    /// The proposing node.
    pub proposer: u64,
    /// The command.
    pub cmd: MetaCommand,
    /// The assignment the command was planned from.
    pub before: PartitionAssignment,
    /// The meta verdict, or the proposal error.
    pub result: Result<MetaResult, String>,
}

/// The meta-leader partition controller.
pub struct Controller {
    meta: Arc<MetaHandle>,
    live: Arc<dyn LiveSource>,
    cfg: ControllerConfig,
    clock: Arc<dyn Clock>,
    events: broadcast::Sender<ProposalRecord>,
}

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Controller")
            .field("node", &self.meta.node_id())
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

/// Per-term loop bookkeeping.
#[derive(Default)]
struct LoopState {
    /// Partitions with a proposal in flight.
    in_flight: HashSet<u16>,
    /// Partition → epoch at which `Offline` was proposed (once per epoch).
    offline: HashMap<u16, u32>,
    /// Commands meta rejected as `Invalid` (never retried as-is).
    invalid: HashSet<String>,
    /// Partitions already logged as "not re-placed".
    warned_rf: HashSet<u16>,
}

/// Bound on the read barrier a new meta leader takes before planning.
const BARRIER_TIMEOUT: Duration = Duration::from_secs(2);

/// What a finished proposal task hands back.
type Done = (u16, String, ProposalRecord);

/// Identity of a command for the `Invalid` memory (timestamps excluded).
fn command_key(cmd: &MetaCommand) -> String {
    match cmd {
        MetaCommand::ElectLeader {
            partition,
            epoch,
            leader,
            isr,
            expected_isr_version,
            ..
        } => format!("elect p{partition} e{epoch} l{leader} {isr:?} v{expected_isr_version}"),
        MetaCommand::AlterIsr {
            partition,
            epoch,
            expected_isr_version,
            isr,
        } => format!("alter p{partition} e{epoch} {isr:?} v{expected_isr_version}"),
        MetaCommand::Put {
            record: MetaRecord::Partition(p),
            ..
        } => format!("place p{} {:?}", p.partition, p.replicas),
        other => format!("{other:?}"),
    }
}

#[cfg(feature = "failpoints")]
fn before_propose() -> Result<(), String> {
    fail::fail_point!("elect::before_propose", |_| Err(
        "injected failure at elect::before_propose".to_string()
    ));
    Ok(())
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn before_propose() -> Result<(), String> {
    Ok(())
}

impl Controller {
    /// A controller for the node owning `meta`. `live` is the shared
    /// [`crate::liveness::LivenessHandle`] in production.
    pub fn new(
        meta: Arc<MetaHandle>,
        live: Arc<dyn LiveSource>,
        cfg: ControllerConfig,
        clock: Arc<dyn Clock>,
    ) -> Controller {
        Controller {
            meta,
            live,
            cfg,
            clock,
            events: broadcast::channel(1024).0,
        }
    }

    /// Every proposal this controller makes, with its verdict.
    pub fn subscribe(&self) -> broadcast::Receiver<ProposalRecord> {
        self.events.subscribe()
    }

    /// Run until `shutdown`. In-flight proposals are abandoned on shutdown (every command
    /// is CAS-guarded, so an abandoned one that still applies is harmless).
    pub async fn run(self, shutdown: CancellationToken) {
        let mut st = LoopState::default();
        let mut tasks: JoinSet<Done> = JoinSet::new();
        let permits = Arc::new(Semaphore::new(self.cfg.max_in_flight.max(1)));
        let mut leading = false;
        let mut ticker = tokio::time::interval(self.cfg.tick.max(Duration::from_millis(1)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Ok(done) = done {
                        self.finish(&mut st, done);
                    }
                    continue;
                }
                _ = ticker.tick() => {}
                () = self.live.wake(), if leading => {}
            }
            if !self.meta.is_leader() {
                if leading {
                    tracing::info!(
                        node = self.meta.node_id(),
                        "controller inactive (meta leadership lost)"
                    );
                }
                leading = false;
                continue;
            }
            if !leading {
                // A fresh meta leader may not have applied the previous term's committed
                // tail yet: plan only from a view that includes it.
                let barrier = tokio::time::timeout(BARRIER_TIMEOUT, read_barrier(&self.meta));
                let ok = tokio::select! {
                    () = shutdown.cancelled() => break,
                    r = barrier => matches!(r, Ok(Ok(()))),
                };
                if !ok {
                    tracing::debug!(
                        node = self.meta.node_id(),
                        "controller read barrier did not complete; retrying next tick"
                    );
                    continue;
                }
                if !self.meta.is_leader() {
                    continue;
                }
                leading = true;
                st.offline.clear();
                st.warned_rf.clear();
                self.live.on_became_controller();
                tracing::info!(
                    node = self.meta.node_id(),
                    "controller active (meta leader)"
                );
            }
            self.tick(&mut st, &mut tasks, &permits).await;
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    async fn tick(&self, st: &mut LoopState, tasks: &mut JoinSet<Done>, permits: &Arc<Semaphore>) {
        let status = self.meta.raft().status();
        let (partition_count, parts) = self.meta.view().read(|s| {
            (
                s.cluster().map(|c| c.value.partitions),
                s.partitions().map(|v| v.value.clone()).collect::<Vec<_>>(),
            )
        });
        let mut voters = status.voters.clone();
        voters.sort_unstable();
        let mut universe: BTreeSet<u64> = voters.iter().copied().collect();
        universe.extend(status.learners.iter().copied());
        for a in &parts {
            universe.extend(a.replicas.iter().copied());
            universe.extend(a.isr.iter().copied());
            if a.leader != 0 {
                universe.insert(a.leader);
            }
        }
        universe.remove(&0);
        let live = self.live.poll(self.meta.raft(), &universe).await;
        if !self.meta.is_leader() {
            return;
        }
        let now = self.clock.now_ms();
        let placement = partition_count
            .map(|n| bootstrap::placement(&voters, n, self.cfg.rf))
            .unwrap_or_default();
        for a in parts {
            let p = a.partition;
            if st.in_flight.contains(&p) {
                continue;
            }
            let Some(cmd) = self.decide(st, &a, &live, &placement, now) else {
                continue;
            };
            let key = command_key(&cmd);
            if st.invalid.contains(&key) {
                continue;
            }
            let Ok(permit) = Arc::clone(permits).try_acquire_owned() else {
                break;
            };
            if let MetaCommand::ElectLeader {
                leader: 0, epoch, ..
            } = cmd
            {
                st.offline.insert(p, epoch.saturating_sub(1));
            }
            st.in_flight.insert(p);
            let meta = Arc::clone(&self.meta);
            let proposer = self.meta.node_id();
            tasks.spawn(async move {
                let _permit = permit;
                let result = match before_propose() {
                    Err(e) => Err(e),
                    Ok(()) => meta.propose(cmd.clone()).await.map_err(|e| e.to_string()),
                };
                (
                    p,
                    key,
                    ProposalRecord {
                        proposer,
                        cmd,
                        before: a,
                        result,
                    },
                )
            });
        }
    }

    /// The command for one assignment this tick, if any.
    fn decide(
        &self,
        st: &mut LoopState,
        a: &PartitionAssignment,
        live: &BTreeSet<u64>,
        placement: &[Vec<u64>],
        now: u64,
    ) -> Option<MetaCommand> {
        let p = a.partition;
        if a.epoch == 0 {
            // Unplaced: placement only (no failover, no fresh term at epoch 0).
            let replicas = placement.get(usize::from(p))?.clone();
            let record = bootstrap::bootstrap_record(a, replicas, self.cfg.min_insync, now)?;
            return Some(bootstrap::bootstrap_command(record));
        }
        if a.replicas.len() < self.cfg.rf && st.warned_rf.insert(p) {
            tracing::info!(
                partition = p,
                replicas = ?a.replicas,
                rf = self.cfg.rf,
                "partition at epoch >= 1 with fewer replicas than rf is not re-placed (grow with `ostro cluster init`, Phase 13)"
            );
        }
        let info = get_election_info(&a.ext).ok().flatten();
        let resigned = info
            .filter(|i| {
                a.leader == 0
                    && i.reason == ELECTION_REASON_RESIGN
                    && now
                        < i.elected_at_ms
                            .saturating_add(self.cfg.resign_hold.as_millis() as u64)
            })
            .map(|i| i.prev_leader);
        let input = AssignmentIn {
            partition: p,
            leader: a.leader,
            epoch: a.epoch,
            replicas: a.replicas.clone(),
            isr: a.isr.clone(),
            isr_version: a.isr_version,
        };
        let next_epoch = a.epoch.checked_add(1)?;
        match failover::plan(&input, live, resigned) {
            Plan::Elect { leader, isr } => {
                let after_resign =
                    a.leader == 0 && info.is_some_and(|i| i.reason == ELECTION_REASON_RESIGN);
                Some(MetaCommand::ElectLeader {
                    partition: p,
                    epoch: next_epoch,
                    leader,
                    isr,
                    expected_isr_version: a.isr_version,
                    info: ElectionInfo {
                        reason: if after_resign {
                            ELECTION_REASON_RESIGN
                        } else {
                            ELECTION_REASON_FAILOVER
                        },
                        elected_at_ms: now,
                        prev_leader: if a.leader != 0 {
                            a.leader
                        } else {
                            info.map_or(0, |i| i.prev_leader)
                        },
                    },
                })
            }
            Plan::Shrink { isr } => Some(MetaCommand::AlterIsr {
                partition: p,
                epoch: a.epoch,
                expected_isr_version: a.isr_version,
                isr,
            }),
            Plan::Offline { keep } => {
                if st.offline.get(&p) == Some(&a.epoch) {
                    return None;
                }
                Some(MetaCommand::ElectLeader {
                    partition: p,
                    epoch: next_epoch,
                    leader: 0,
                    isr: keep,
                    expected_isr_version: a.isr_version,
                    info: ElectionInfo {
                        reason: ELECTION_REASON_OFFLINE,
                        elected_at_ms: now,
                        prev_leader: a.leader,
                    },
                })
            }
            Plan::Nothing => None,
        }
    }

    fn finish(&self, st: &mut LoopState, (p, key, rec): Done) {
        st.in_flight.remove(&p);
        let failed_offline = matches!(rec.cmd, MetaCommand::ElectLeader { leader: 0, .. })
            && !matches!(rec.result, Ok(MetaResult::Ok { .. }));
        if failed_offline {
            st.offline.remove(&p);
        }
        match &rec.result {
            Ok(MetaResult::Ok { version }) => {
                tracing::info!(partition = p, version, cmd = %key, "controller proposal applied");
            }
            Ok(MetaResult::StaleVersion { current }) => {
                tracing::debug!(partition = p, current, cmd = %key, "controller proposal stale; re-reading");
            }
            Ok(MetaResult::Invalid(msg)) => {
                tracing::error!(
                    partition = p,
                    cmd = ?rec.cmd,
                    reason = %msg,
                    "controller proposal rejected as invalid (planner bug?); not retrying it"
                );
                st.invalid.insert(key);
            }
            Ok(other) => {
                tracing::warn!(partition = p, result = ?other, cmd = %key, "controller proposal not applied");
            }
            Err(e) => {
                tracing::warn!(partition = p, error = %e, cmd = %key, "controller proposal failed; retrying next tick");
            }
        }
        let _ = self.events.send(rec);
    }
}

/// A linearizable read point for the local metadata view: once this returns `Ok`, the view
/// holds every entry the metadata group committed before the call.
///
/// It proposes a command that can never change state (`Delete` of `Member(0)` at version
/// 0: node ids start at 1 and record versions at 1, so the CAS never matches and apply
/// answers `NotFound`/`StaleVersion`). `MetaHandle::propose` returns only once this node
/// applied the entry's index (forwarded proposals wait for it), and Raft commits the entry
/// after everything committed earlier. A node that just regained meta contact (or a meta
/// leader that was just elected and has not applied the previous term's tail yet) can read
/// a view that is many entries behind; deciding an `ElectLeader` from that view proposes
/// from an ISR that is no longer published (12-16, `isolated_leader_rejoin_truncates`).
pub async fn read_barrier(meta: &MetaHandle) -> Result<(), MetaError> {
    meta.propose(MetaCommand::Delete {
        key: MetaKey::Member(0),
        expected_version: 0,
    })
    .await
    .map(|_| ())
}

/// The outcome of [`request_fresh_term`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshTerm {
    /// `ElectLeader{epoch, leader = self}` applied.
    Elected {
        /// The new epoch.
        epoch: u32,
    },
    /// Meta already names this node leader at an epoch ≥ `min_epoch`; nothing proposed.
    AlreadyFresh {
        /// The current epoch.
        epoch: u32,
    },
    /// The partition is at epoch 0 (placement comes first); nothing proposed.
    Unplaced,
    /// Meta names another leader (`0` = offline); nothing proposed, stay follower.
    NotLeader {
        /// The current leader.
        leader: u64,
    },
    /// This node is not in the published ISR; nothing proposed, stay follower.
    NotInIsr,
    /// Meta has no such partition.
    UnknownPartition,
}

/// Retries of a fresh-term proposal that lost a CAS race.
const FRESH_TERM_ATTEMPTS: usize = 10;

/// A fresh leader epoch for `self_id` on `partition` (backs `AssignmentSource::request_term`;
/// called by the role lifecycle when meta names this node leader at an epoch ≤ the local
/// log's latest, with `min_epoch = latest + 1`).
///
/// A proposal is decided from a view at least as new as the call ([`read_barrier`] before
/// the first one): a node that was cut off decides from the current ISR, never from the one
/// it last saw. The no-op answers may come from the local view as it is.
/// No-op at epoch 0, when another node leads, when this node is not in the ISR, or when the
/// current epoch already reaches `min_epoch`. Otherwise proposes (forwarded if needed)
/// `ElectLeader{max(epoch + 1, min_epoch), leader = self, isr = current, expected
/// isr_version, info Startup}`, re-reading and retrying on `StaleVersion`.
pub async fn request_fresh_term(
    meta: &MetaHandle,
    self_id: u64,
    partition: u16,
    min_epoch: u32,
    clock: &dyn Clock,
) -> Result<FreshTerm, MetaError> {
    let mut fresh = false;
    for _ in 0..FRESH_TERM_ATTEMPTS {
        let Some(cur) = meta
            .view()
            .read(|s| s.partition(partition).map(|v| v.value.clone()))
        else {
            return Ok(FreshTerm::UnknownPartition);
        };
        if cur.epoch == 0 {
            return Ok(FreshTerm::Unplaced);
        }
        if cur.leader != self_id {
            return Ok(FreshTerm::NotLeader { leader: cur.leader });
        }
        if !cur.isr.contains(&self_id) {
            return Ok(FreshTerm::NotInIsr);
        }
        if cur.epoch >= min_epoch {
            return Ok(FreshTerm::AlreadyFresh { epoch: cur.epoch });
        }
        if !fresh {
            // About to propose: decide again from a view at least as new as this call.
            read_barrier(meta).await?;
            fresh = true;
            continue;
        }
        let epoch = cur
            .epoch
            .checked_add(1)
            .ok_or_else(|| MetaError::Rejected(format!("partition {partition} epoch overflow")))?
            .max(min_epoch);
        let cmd = MetaCommand::ElectLeader {
            partition,
            epoch,
            leader: self_id,
            isr: cur.isr.clone(),
            expected_isr_version: cur.isr_version,
            info: ElectionInfo {
                reason: ELECTION_REASON_STARTUP,
                elected_at_ms: clock.now_ms(),
                prev_leader: cur.leader,
            },
        };
        match meta.propose(cmd).await? {
            MetaResult::Ok { .. } => {
                tracing::info!(partition, epoch, node = self_id, "fresh leader term");
                return Ok(FreshTerm::Elected { epoch });
            }
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => continue,
            MetaResult::NotFound => return Ok(FreshTerm::UnknownPartition),
            MetaResult::Invalid(msg) => {
                // Apply re-validates ISR membership: re-read to tell "dropped from the ISR
                // meanwhile" from a real rejection.
                let still = meta.view().read(|s| {
                    s.partition(partition)
                        .is_some_and(|v| v.value.isr.contains(&self_id))
                });
                if !still {
                    return Ok(FreshTerm::NotInIsr);
                }
                return Err(MetaError::Rejected(msg));
            }
        }
    }
    Err(MetaError::Rejected(format!(
        "partition {partition}: fresh term kept losing CAS races"
    )))
}

/// `self_id`, the leader of `partition` at `epoch`, steps down (backs
/// `AssignmentSource::resign`, used when its log fail-stops). The controller then elects
/// the next live ISR member, or takes the partition offline.
pub async fn resign(
    meta: &MetaHandle,
    self_id: u64,
    partition: u16,
    epoch: u32,
    clock: &dyn Clock,
) -> Result<MetaResult, MetaError> {
    let cmd = MetaCommand::ResignLeader {
        partition,
        epoch,
        node: self_id,
        info: ElectionInfo {
            reason: ELECTION_REASON_RESIGN,
            elected_at_ms: clock.now_ms(),
            prev_leader: self_id,
        },
    };
    let r = meta.propose(cmd).await?;
    tracing::info!(partition, epoch, node = self_id, result = ?r, "resigned partition leadership");
    Ok(r)
}
