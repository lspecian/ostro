//! The meta leader's membership driver (13-09, D-P13-8/9).
//!
//! Every node runs the loop; it acts only while this node leads meta and only once dynamic
//! membership is in use (some `Member` record carries a MemberState, ext 97): a Phase 12
//! static cluster that never joined or drained a node is left exactly as configured.
//!
//! One tick (default every 500 ms, and on every applied meta change):
//!
//! 1. observe: Member records + MemberState (absent = Active), `raft_detail()` (voters,
//!    learners, outgoing voters, pending conf change, per-peer progress), the live set
//!    ([`LiveSource`], default: raft `recent_active` + self), `promote_lag`,
//!    `ReplicationPolicy.max_voters` (ext 99, default 5);
//! 2. bookkeeping, at most one meta write per tick, in this order:
//!    - an orphan Joining member (no join/resume request and no liveness contact for
//!      `join_orphan_timeout`, measured from the later of `MemberState.changed_at_ms` and the
//!      last time this leader saw it live) → RemoveNode if it is in the conf, then MemberState
//!      Removed (plan checks W8, NW1);
//!    - a Joining learner that caught up (`matched ≥ commit − promote_lag`, no pending
//!      snapshot) → MemberState Active;
//!    - a Draining member that holds no replica and leads no partition → MemberState Removed
//!      (`request_remove --force` on a dead node writes Removed itself, W7);
//! 3. `reconcile::next` → one conf change: `AddLearner` / `RemoveLearner` = a single-change
//!    `ConfChangeV2`; `Joint{add, remove, demote}` = one `ConfChangeV2` with
//!    `transition = Auto` (AddNode / RemoveNode / AddLearnerNode); `LeaveJoint` = an empty
//!    `ConfChangeV2`; `TransferLeader` = `MetaHandle::transfer_leader`;
//! 4. a Removed member that left the conf gets one best-effort `Membership.Removed` notice
//!    ([`RemovedNotifier`]).
//!
//! Mutants (failpoints build, D-P13-25): `member::mutant::skip_quorum_check` makes the driver
//! report every voter live; `member::mutant::no_leave_joint` drops `LeaveJoint`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_meta::ext::{
    MEMBER_ACTIVE, MEMBER_DRAINING, MEMBER_JOINING, MEMBER_REMOVED, MemberState, get_member_state,
    get_replication_policy, put_member_state,
};
use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult, MetaState};
use ostro_partition::Clock;
use ostro_raft_driver::raft::eraftpb::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::reconcile::{self, MemberObs, ReconcileInput, Step};

/// `member::mutant::skip_quorum_check` (RESERVATIONS §11).
pub const MUTANT_SKIP_QUORUM_CHECK: &str = "member::mutant::skip_quorum_check";
/// `member::mutant::no_leave_joint` (RESERVATIONS §11).
pub const MUTANT_NO_LEAVE_JOINT: &str = "member::mutant::no_leave_joint";

/// Gauge: metadata voters (as seen by the meta leader).
pub const VOTERS_GAUGE: &str = "ostro_cluster_voters";
/// Gauge: metadata learners.
pub const LEARNERS_GAUGE: &str = "ostro_cluster_learners";
/// Counter: conf changes proposed by the driver, label `kind`.
pub const CONF_CHANGES_TOTAL: &str = "ostro_cluster_conf_changes_total";

/// Default `ReplicationPolicy.max_voters`.
pub const DEFAULT_MAX_VOTERS: u8 = 5;

#[cfg(feature = "failpoints")]
fn mutant(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn mutant(_name: &str) -> bool {
    false
}

/// Membership loop configuration (`[cluster]` keys, D-P13-26).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipConfig {
    /// `promote_lag` (entries).
    pub promote_lag: u64,
    /// `join_orphan_timeout`.
    pub join_orphan_timeout: std::time::Duration,
    /// Reconcile period.
    pub interval: std::time::Duration,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            promote_lag: 64,
            join_orphan_timeout: std::time::Duration::from_secs(25 * 3600),
            interval: std::time::Duration::from_millis(500),
        }
    }
}

/// Which of `ids` are live (the server passes its liveness sampler; the default is raft's
/// `recent_active` on the leader plus the leader itself).
pub type LiveSource = Arc<dyn Fn(&BTreeSet<u64>) -> BTreeSet<u64> + Send + Sync>;

/// Best-effort `Membership.Removed` to a removed node (`(node_id) -> future`).
pub type RemovedNotifier = Arc<
    dyn Fn(u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync,
>;

/// The MemberState of `m` (absent = Active: a Phase 12 member; unknown values as-is).
pub fn member_state_of(m: &ostro_meta::Member) -> u8 {
    match get_member_state(&m.ext) {
        Ok(Some(s)) => s.state,
        _ => MEMBER_ACTIVE,
    }
}

/// Whether dynamic membership is in use: some Member record carries a MemberState.
pub fn dynamic_membership(state: &MetaState) -> bool {
    state
        .members()
        .any(|m| matches!(get_member_state(&m.value.ext), Ok(Some(_))))
}

/// `name` of a MemberState value (`joining` / `active` / `draining` / `removed`).
pub fn state_name(state: u8) -> &'static str {
    match state {
        MEMBER_JOINING => "joining",
        MEMBER_ACTIVE => "active",
        MEMBER_DRAINING => "draining",
        MEMBER_REMOVED => "removed",
        _ => "unknown",
    }
}

/// Why a MemberState write was refused.
#[derive(Debug, thiserror::Error)]
pub enum StateWriteError {
    /// No such member.
    #[error("node {0} is not a member")]
    NotMember(u64),
    /// A tombstone is never changed.
    #[error("node {0} was removed (tombstone)")]
    Removed(u64),
    /// Meta refused / failed.
    #[error("{0}")]
    Meta(#[from] MetaError),
}

/// Set member `id`'s MemberState to `state` (read-modify-write under CAS; every other ext
/// entry is kept; a Removed tombstone is never changed). `Ok(false)` = already in `state`.
pub async fn set_member_state(
    meta: &MetaHandle,
    id: u64,
    state: u8,
    now_ms: u64,
) -> Result<bool, StateWriteError> {
    for _ in 0..8 {
        let Some(cur) = meta.view().read(|s| s.member(id).cloned()) else {
            return Err(StateWriteError::NotMember(id));
        };
        let current = member_state_of(&cur.value);
        if current == state && get_member_state(&cur.value.ext).ok().flatten().is_some() {
            return Ok(false);
        }
        if current == MEMBER_REMOVED {
            return Err(StateWriteError::Removed(id));
        }
        let ext = put_member_state(
            &cur.value.ext,
            &MemberState {
                state,
                changed_at_ms: now_ms,
            },
        )
        .map_err(|e| MetaError::Rejected(format!("member {id} ext: {e}")))?;
        let mut record = cur.value.clone();
        record.ext = ext;
        let res = meta
            .propose(MetaCommand::Put {
                key: MetaKey::Member(id),
                expected_version: Some(cur.version),
                record: MetaRecord::Member(record),
            })
            .await?;
        match res {
            MetaResult::Ok { .. } => {
                tracing::info!(node = id, state = state_name(state), "member state changed");
                return Ok(true);
            }
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => continue,
            MetaResult::NotFound => return Err(StateWriteError::NotMember(id)),
            MetaResult::Invalid(m) => return Err(MetaError::Rejected(m).into()),
        }
    }
    Err(MetaError::Rejected(format!("member {id} kept changing under the state write")).into())
}

/// Whether `id` still holds a replica of, or leads, any partition.
pub fn holds_partitions(state: &MetaState, id: u64) -> bool {
    state.partitions().any(|p| {
        p.value.leader == id || p.value.replicas.contains(&id) || p.value.isr.contains(&id)
    })
}

/// `ConfChangeV2` of `(type, node)` changes (`Auto` transition; one change = simple).
pub fn conf_change(changes: &[(ConfChangeType, u64)]) -> ConfChangeV2 {
    let changes = changes
        .iter()
        .map(|(t, id)| {
            let mut s = ConfChangeSingle::default();
            s.set_change_type(*t);
            s.node_id = *id;
            s
        })
        .collect();
    let mut cc = ConfChangeV2 {
        changes,
        ..ConfChangeV2::default()
    };
    cc.set_transition(ConfChangeTransition::Auto);
    cc
}

/// The `ConfChangeV2` of a reconciler step (`None` for `TransferLeader` / `None`).
pub fn step_conf_change(step: &Step) -> Option<ConfChangeV2> {
    match step {
        Step::LeaveJoint => Some(ConfChangeV2::default()),
        Step::AddLearner(id) => Some(conf_change(&[(ConfChangeType::AddLearnerNode, *id)])),
        Step::RemoveLearner(id) => Some(conf_change(&[(ConfChangeType::RemoveNode, *id)])),
        Step::Joint {
            add_voters,
            remove_voters,
            demote,
        } => {
            let mut v = Vec::new();
            v.extend(add_voters.iter().map(|id| (ConfChangeType::AddNode, *id)));
            v.extend(
                remove_voters
                    .iter()
                    .map(|id| (ConfChangeType::RemoveNode, *id)),
            );
            v.extend(
                demote
                    .iter()
                    .map(|id| (ConfChangeType::AddLearnerNode, *id)),
            );
            Some(conf_change(&v))
        }
        Step::TransferLeader(_) | Step::None => None,
    }
}

fn step_kind(step: &Step) -> &'static str {
    match step {
        Step::LeaveJoint => "leave_joint",
        Step::AddLearner(_) => "add_learner",
        Step::RemoveLearner(_) => "remove_learner",
        Step::Joint { .. } => "joint",
        Step::TransferLeader(_) => "transfer_leader",
        Step::None => "none",
    }
}

/// What one [`MembershipService::tick`] did (tests, logs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TickOutcome {
    /// Not the meta leader.
    NotLeader,
    /// Leader, but no Member record carries a MemberState yet (Phase 12 static cluster).
    Static,
    /// Nothing to do.
    Idle,
    /// A MemberState was written (`(node, new state)`).
    State(u64, u8),
    /// A conf change / transfer was proposed and succeeded.
    Step(Step),
    /// A proposal failed (logged; retried next tick).
    Failed(String),
}

/// Driver state kept across ticks (on the current leader only).
#[derive(Debug, Default)]
struct DriverMemory {
    /// Last time (ms, injected clock) each node was seen live by this leader.
    last_live: BTreeMap<u64, u64>,
    /// When this node became the leader (the orphan clock starts no earlier).
    leader_since: Option<u64>,
    /// Removed nodes already notified.
    notified: BTreeSet<u64>,
}

/// The membership loop (runs on every node, acts only as meta leader).
pub struct MembershipService {
    meta: Arc<MetaHandle>,
    cfg: MembershipConfig,
    clock: Option<Arc<dyn Clock>>,
    live: Option<LiveSource>,
    notifier: Option<RemovedNotifier>,
    memory: Mutex<DriverMemory>,
}

impl std::fmt::Debug for MembershipService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MembershipService")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl MembershipService {
    /// A loop over `meta`.
    pub fn new(meta: Arc<MetaHandle>, cfg: MembershipConfig) -> Self {
        Self {
            meta,
            cfg,
            clock: None,
            live: None,
            notifier: None,
            memory: Mutex::new(DriverMemory::default()),
        }
    }

    /// The one injectable clock (MemberState timestamps, orphan timeout). Without one the
    /// driver never tombstones orphans and stamps `changed_at_ms = 0`.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// The live-set source (the server's liveness sampler).
    pub fn with_live_source(mut self, live: LiveSource) -> Self {
        self.live = Some(live);
        self
    }

    /// The `Membership.Removed` sender.
    pub fn with_removed_notifier(mut self, notifier: RemovedNotifier) -> Self {
        self.notifier = Some(notifier);
        self
    }

    fn now_ms(&self) -> u64 {
        self.clock.as_ref().map_or(0, |c| c.now_ms())
    }

    fn memory(&self) -> std::sync::MutexGuard<'_, DriverMemory> {
        self.memory.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Start the loop: a tick every `interval` and on every applied meta change, until
    /// `stop`.
    pub fn start(self, stop: CancellationToken) -> JoinHandle<()> {
        let svc = Arc::new(self);
        tokio::spawn(async move {
            let mut applied = svc.meta.view().subscribe();
            loop {
                let out = svc.tick().await;
                if let TickOutcome::Failed(e) = &out {
                    tracing::debug!(error = %e, "membership tick failed; retrying");
                }
                let wait = match out {
                    // Something happened: look again soon.
                    TickOutcome::State(..) | TickOutcome::Step(_) => Duration::from_millis(20),
                    _ => svc.cfg.interval,
                };
                tokio::select! {
                    _ = stop.cancelled() => return,
                    r = applied.changed() => if r.is_err() { return },
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        })
    }

    /// One driver step (see the module docs).
    pub async fn tick(&self) -> TickOutcome {
        let meta = &self.meta;
        let detail = meta.raft_detail();
        if !detail.is_leader {
            *self.memory() = DriverMemory::default();
            return TickOutcome::NotLeader;
        }
        let me = detail.id;
        let now = self.now_ms();
        let (members, dynamic, max_voters, holding) = meta.view().read(|s| {
            let members: BTreeMap<u64, (u8, u64, bool)> = s
                .members()
                .map(|m| {
                    let st = get_member_state(&m.value.ext).ok().flatten();
                    (
                        m.value.node_id,
                        (
                            st.map_or(MEMBER_ACTIVE, |x| x.state),
                            st.map_or(0, |x| x.changed_at_ms),
                            false,
                        ),
                    )
                })
                .collect();
            let max_voters = s
                .cluster()
                .and_then(|c| get_replication_policy(&c.value.ext).ok().flatten())
                .map(|p| p.max_voters)
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_MAX_VOTERS);
            let holding: BTreeSet<u64> = members
                .keys()
                .copied()
                .filter(|id| holds_partitions(s, *id))
                .collect();
            (members, dynamic_membership(s), max_voters, holding)
        });
        metrics::gauge!(VOTERS_GAUGE).set(detail.voters.len() as f64);
        metrics::gauge!(LEARNERS_GAUGE).set(detail.learners.len() as f64);
        if !dynamic {
            return TickOutcome::Static;
        }
        let progress: BTreeMap<u64, &ostro_meta::PeerProgress> =
            detail.progress.iter().map(|p| (p.id, p)).collect();
        let universe: BTreeSet<u64> = members.keys().copied().collect();
        let mut live: BTreeSet<u64> = match &self.live {
            Some(f) => f(&universe),
            None => detail
                .progress
                .iter()
                .filter(|p| p.recent_active)
                .map(|p| p.id)
                .collect(),
        };
        live.insert(me);
        let in_conf = |id: u64| {
            detail.voters.contains(&id)
                || detail.learners.contains(&id)
                || detail.voters_outgoing.contains(&id)
                || detail.learners_next.contains(&id)
        };
        {
            let mut mem = self.memory();
            let since = *mem.leader_since.get_or_insert(now);
            let _ = since;
            for id in &live {
                mem.last_live.insert(*id, now);
            }
        }

        // 2. bookkeeping (one write per tick).
        if self.clock.is_some() {
            let timeout =
                u64::try_from(self.cfg.join_orphan_timeout.as_millis()).unwrap_or(u64::MAX);
            let orphan = {
                let mem = self.memory();
                let since = mem.leader_since.unwrap_or(now);
                members
                    .iter()
                    .filter(|(_, (st, _, _))| *st == MEMBER_JOINING)
                    .find(|(id, (_, changed, _))| {
                        let seen = mem.last_live.get(id).copied().unwrap_or(since).max(since);
                        let last = seen.max(*changed);
                        !live.contains(id) && now.saturating_sub(last) >= timeout
                    })
                    .map(|(id, _)| *id)
            };
            if let Some(id) = orphan {
                if in_conf(id) {
                    tracing::warn!(
                        node = id,
                        "orphan joiner: removing it from the metadata group"
                    );
                    let cc = conf_change(&[(ConfChangeType::RemoveNode, id)]);
                    return self.propose(&Step::RemoveLearner(id), cc).await;
                }
                tracing::warn!(node = id, "orphan joiner: tombstoning its member record");
                return self.write_state(id, MEMBER_REMOVED, now).await;
            }
        }
        let commit = detail.commit;
        let caught_up = |id: u64| {
            progress.get(&id).is_some_and(|p| {
                p.pending_snapshot == 0 && p.matched.saturating_add(self.cfg.promote_lag) >= commit
            })
        };
        if let Some(id) = members.iter().find_map(|(id, (st, _, _))| {
            (*st == MEMBER_JOINING
                && (detail.voters.contains(id)
                    || (detail.learners.contains(id) && caught_up(*id) && live.contains(id))))
            .then_some(*id)
        }) {
            return self.write_state(id, MEMBER_ACTIVE, now).await;
        }
        if let Some(id) = members.iter().find_map(|(id, (st, _, _))| {
            (*st == MEMBER_DRAINING && !holding.contains(id) && *id != me).then_some(*id)
        }) {
            return self.write_state(id, MEMBER_REMOVED, now).await;
        }

        // 3. one conf change.
        let skip_quorum = mutant(MUTANT_SKIP_QUORUM_CHECK);
        let input = ReconcileInput {
            leader: me,
            voters: detail.voters.clone(),
            voters_outgoing: detail.voters_outgoing.clone(),
            learners: detail.learners.clone(),
            members: members
                .iter()
                .map(|(id, (st, _, _))| MemberObs {
                    id: *id,
                    state: *st,
                    live: live.contains(id)
                        || (skip_quorum
                            && (detail.voters.contains(id) || detail.voters_outgoing.contains(id))),
                    matched: progress.get(id).map_or(0, |p| p.matched),
                    pending_snapshot: progress.get(id).is_some_and(|p| p.pending_snapshot != 0),
                })
                .collect(),
            commit,
            pending_conf_change: detail.pending_conf,
            promote_lag: self.cfg.promote_lag,
            max_voters,
        };
        let step = reconcile::next(&input);
        match &step {
            Step::None => {}
            Step::LeaveJoint if mutant(MUTANT_NO_LEAVE_JOINT) => {
                tracing::warn!("mutant {MUTANT_NO_LEAVE_JOINT}: dropping LeaveJoint");
            }
            Step::TransferLeader(to) => {
                metrics::counter!(CONF_CHANGES_TOTAL, "kind" => step_kind(&step)).increment(1);
                tracing::info!(to, "membership: transferring meta leadership first");
                return match meta.transfer_leader(*to).await {
                    Ok(()) => TickOutcome::Step(step),
                    Err(e) => TickOutcome::Failed(format!("transfer to {to}: {e}")),
                };
            }
            other => {
                if let Some(cc) = step_conf_change(other) {
                    return self.propose(other, cc).await;
                }
            }
        }

        // 4. notify removed nodes that left the conf.
        let to_notify: Vec<u64> = {
            let mut mem = self.memory();
            members
                .iter()
                .filter(|(id, (st, _, _))| *st == MEMBER_REMOVED && !in_conf(**id) && **id != me)
                .map(|(id, _)| *id)
                .filter(|id| mem.notified.insert(*id))
                .collect()
        };
        if let Some(n) = &self.notifier {
            for id in to_notify {
                tracing::info!(node = id, "membership: notifying removed node");
                n(id).await;
            }
        }
        TickOutcome::Idle
    }

    async fn write_state(&self, id: u64, state: u8, now: u64) -> TickOutcome {
        match set_member_state(&self.meta, id, state, now).await {
            Ok(_) => TickOutcome::State(id, state),
            Err(e) => TickOutcome::Failed(format!("member {id} -> {}: {e}", state_name(state))),
        }
    }

    async fn propose(&self, step: &Step, cc: ConfChangeV2) -> TickOutcome {
        metrics::counter!(CONF_CHANGES_TOTAL, "kind" => step_kind(step)).increment(1);
        tracing::info!(step = ?step, "membership: proposing conf change");
        match self.meta.propose_conf_change(cc).await {
            Ok(_) => TickOutcome::Step(step.clone()),
            Err(e) => TickOutcome::Failed(format!("{step:?}: {e}")),
        }
    }
}
