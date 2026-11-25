//! The meta leader's placement controller (13-10, D-P13-11/12, CLUS-05).
//!
//! Every node runs the loop; it acts only while this node leads meta (the Phase 12
//! controller pattern: CAS on `isr_version`, re-read on `StaleVersion`, idle on leadership
//! loss). All reassignment state lives in meta (replicas, ISR, leader, `min_insync`, ext 100),
//! so a new meta leader resumes every in-flight move where the old one stopped.
//!
//! # One tick (every `PlacementConfig::interval`, and at once on a [`PlacementTrigger`])
//!
//! 1. **observe** ([`observe`]): Member records (MemberState, absent = Active), the placed
//!    partitions (epoch ≥ 1; epoch-0 partitions belong to the bootstrap placement and are
//!    never touched, D-P13-29c), `ReplicationPolicy` (ext 99) and every in-flight
//!    `Reassignment` (ext 100). Placement members = **Active** members only: Joining,
//!    Draining and Removed (incl. `--force`-removed dead nodes, plan check W7) are excluded,
//!    so the planner moves their replicas away.
//! 2. **drive** every in-flight reassignment one step ([`reassign::step`], the pure 13-06
//!    machine `next_step_at`): `Expand` / `Shrink` → `MetaCommand::AlterReplicas` with
//!    `expected_isr_version` (after `compat::require(AlterReplicas)`, and
//!    `ReassignmentExt` when it writes ext 100); `CatchUp` → wait for the Phase 12 ISR expand;
//!    `LeaderMove{to}` → [`HandoffClient::hand_off`] (13-11 NodeOps, retried every 2 s).
//!    A target that names a member which is no longer Active is re-planned.
//! 3. **start** new reassignments, at most `max_concurrent_moves` in flight in total:
//!    [`plan::target_replicas`] over the Active members at the effective RF. With a
//!    `ReplicationPolicy` (or on an explicit rebalance) every planned move may start (RF
//!    raise on growth, balance); without one (a Phase 12 static cluster) only partitions with
//!    a replica on a Draining / Removed member are moved (evacuation), so a static cluster
//!    that never drains a node is left exactly as placed.
//! 4. **balance** leaders: every `leader_rebalance_interval` (or on a trigger), when no move
//!    is in flight, [`leaders::decide`] over the published ISR → at most one
//!    `HandoffClient::hand_off` to an in-ISR preferred replica.
//!
//! # Effective policy ([`effective_policy`])
//!
//! `ReplicationPolicy{rf, min_insync}` when present (effective RF = min(rf, Active members),
//! applied by the planner). Absent → the Phase 12 bootstrap seeds as they were placed: rf =
//! the largest replica count and min_insync = the largest `min_insync` over the placed
//! partitions. `min_insync` rises only in Shrink, capped at |target| (plan check W5).
//!
//! # Blocked partitions
//!
//! `next_step` answers `None` while `min_insync > |ISR|` for a partition with nothing in
//! flight (the Expand would be refused): such a partition cannot start a move until its ISR
//! recovers. The tick reports it ([`TickReport::blocked`], [`blocked_partitions`]), logs it
//! and exports `ostro_cluster_reassignments_blocked`.
//!
//! # Explicit requests
//!
//! [`request_reassign`] (any node; proposals forward to the meta leader) validates the target
//! and records the reassignment through its first Expand; `cancel` re-targets the in-flight
//! one at its original replicas (allowed only before LeaderMove, [`reassign::cancel_target`]).
//! [`request_rebalance`] wakes the local controller (a forced plan + balancer pass); on a
//! node that is not the meta leader it starts the planned replica moves itself (the leader's
//! controller drives them; the leader balancer runs only on the meta leader).
//!
//! Mutant (failpoints build, D-P13-25): `reassign::mutant::raise_min_insync_early` makes
//! Expand send `min_insync = policy` instead of the current value.
//!
//! Metrics: `ostro_cluster_reassignments_in_flight` (gauge), `ostro_cluster_moves_total{result}`
//! (counter: `started`, `step`, `completed`, `stale`, `rejected`, `refused`, `handoff_moved`,
//! `handoff_failed`), `ostro_cluster_reassignments_blocked` (gauge).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_meta::ext::{
    MEMBER_ACTIVE, MEMBER_DRAINING, MEMBER_REMOVED, ReplicationPolicy, get_reassignment,
    get_replication_policy,
};
use ostro_meta::{MetaCommand, MetaHandle, MetaResult, MetaState};
use ostro_partition::Clock;
use ostro_types::compat::Feature;
use ostro_types::{Epoch, PartitionId};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::leaders::{self, LeaderObs};
use super::plan;
use super::reassign::{self, PartitionObs, ReassignStep};
use crate::compat;
use crate::contracts::{HandoffClient, HandoffOutcome};
use crate::membership::service::{LiveSource, member_state_of};

/// `reassign::mutant::raise_min_insync_early` (RESERVATIONS §11, D-P13-25).
pub const MUTANT_RAISE_MIN_INSYNC_EARLY: &str = "reassign::mutant::raise_min_insync_early";

/// Gauge: reassignments in flight (partitions carrying ext 100).
pub const IN_FLIGHT_GAUGE: &str = "ostro_cluster_reassignments_in_flight";
/// Counter: controller moves, label `result`.
pub const MOVES_TOTAL: &str = "ostro_cluster_moves_total";
/// Gauge: partitions that cannot start a move (`min_insync > |ISR|`).
pub const BLOCKED_GAUGE: &str = "ostro_cluster_reassignments_blocked";

/// A LeaderMove handoff is retried this long after the previous attempt.
const HANDOFF_RETRY_MS: u64 = 2_000;
/// `StaleVersion` retries of one explicit request.
const REQUEST_RETRIES: usize = 8;

#[cfg(feature = "failpoints")]
fn mutant(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn mutant(_name: &str) -> bool {
    false
}

/// Placement controller configuration (`[cluster]` keys, D-P13-26).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementConfig {
    /// `max_concurrent_moves`.
    pub max_concurrent_moves: usize,
    /// `leader_rebalance_interval`.
    pub leader_rebalance_interval: Duration,
    /// Controller period.
    pub interval: Duration,
    /// This node's `level_max` (the `compat::require(AlterReplicas)` gate, 13-02).
    pub level_max: u32,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            max_concurrent_moves: 2,
            leader_rebalance_interval: Duration::from_secs(300),
            interval: Duration::from_millis(500),
            level_max: ostro_types::compat::level_max(),
        }
    }
}

// ---- observation (pure) ------------------------------------------------------------------

/// What the controller reads from meta in one tick.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterObs {
    /// Every member record: `(node_id, MemberState)` (absent = Active), ascending ids.
    pub members: Vec<(u64, u8)>,
    /// Placed partitions (epoch ≥ 1), ascending.
    pub partitions: Vec<PartitionObs>,
    /// Ids of the epoch-0 (unplaced) partitions, never touched.
    pub unplaced: Vec<u16>,
    /// `ReplicationPolicy` (ext 99), if set.
    pub policy: Option<ReplicationPolicy>,
}

impl ClusterObs {
    /// Active members (the placement members), ascending.
    pub fn active(&self) -> Vec<u64> {
        self.members
            .iter()
            .filter(|(_, s)| *s == MEMBER_ACTIVE)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Members whose replicas must move away (Draining, Removed).
    pub fn evacuating(&self) -> BTreeSet<u64> {
        self.members
            .iter()
            .filter(|(_, s)| *s == MEMBER_DRAINING || *s == MEMBER_REMOVED)
            .map(|(id, _)| *id)
            .collect()
    }

    /// The partition observation of `p`, if placed.
    pub fn partition(&self, p: u16) -> Option<&PartitionObs> {
        self.partitions.iter().find(|x| x.partition == p)
    }
}

/// The placement view of `state` (see the module docs). An undecodable ext 100 reads as
/// "no reassignment" (logged).
pub fn observe(state: &MetaState) -> ClusterObs {
    let members = state
        .members()
        .map(|m| (m.value.node_id, member_state_of(&m.value)))
        .collect();
    let mut partitions = Vec::new();
    let mut unplaced = Vec::new();
    for v in state.partitions() {
        let a = &v.value;
        if a.epoch == 0 {
            unplaced.push(a.partition);
            continue;
        }
        let reassignment = match get_reassignment(&a.ext) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(partition = a.partition, error = %e, "undecodable reassignment ext; ignoring it");
                None
            }
        };
        partitions.push(PartitionObs {
            partition: a.partition,
            epoch: a.epoch,
            leader: a.leader,
            replicas: a.replicas.clone(),
            isr: a.isr.clone(),
            isr_version: a.isr_version,
            min_insync: a.min_insync,
            reassignment,
        });
    }
    let policy = state
        .cluster()
        .and_then(|c| get_replication_policy(&c.value.ext).ok().flatten());
    ClusterObs {
        members,
        partitions,
        unplaced,
        policy,
    }
}

/// The effective `(rf, min_insync)` (module docs): the policy when present, else the Phase 12
/// bootstrap seeds as placed (largest replica count / largest `min_insync`). Both ≥ 1.
pub fn effective_policy(obs: &ClusterObs) -> (u8, u8) {
    if let Some(p) = obs.policy {
        return (p.rf.max(1), p.min_insync.max(1));
    }
    let rf = obs
        .partitions
        .iter()
        .map(|p| p.replicas.len())
        .max()
        .unwrap_or(1);
    let min_insync = obs
        .partitions
        .iter()
        .map(|p| p.min_insync)
        .max()
        .unwrap_or(1);
    (
        u8::try_from(rf).unwrap_or(u8::MAX).max(1),
        min_insync.max(1),
    )
}

/// Partitions that cannot start a move because `min_insync > |ISR|` and nothing is in flight
/// for them (the Expand would be refused): `(partition, reason)`.
pub fn blocked_partitions(obs: &ClusterObs) -> Vec<(u16, String)> {
    obs.partitions
        .iter()
        .filter(|p| p.reassignment.is_none() && usize::from(p.min_insync) > p.isr.len())
        .map(|p| {
            (
                p.partition,
                format!(
                    "min_insync {} > ISR {:?}: waiting for the ISR to recover",
                    p.min_insync, p.isr
                ),
            )
        })
        .collect()
}

/// The planner's targets for the partitions the controller may start moving now: every
/// planned change with `auto`, else only partitions holding a replica on an evacuating
/// member. In-flight partitions count at their target (so the plan accounts for them) but
/// are never returned.
pub fn planned_moves(obs: &ClusterObs, auto: bool) -> BTreeMap<u16, Vec<u64>> {
    let (rf, _) = effective_policy(obs);
    let current: BTreeMap<u16, Vec<u64>> = obs
        .partitions
        .iter()
        .map(|p| {
            let at = p
                .reassignment
                .as_ref()
                .map_or_else(|| p.replicas.clone(), |r| r.target.clone());
            (p.partition, at)
        })
        .collect();
    let active = obs.active();
    let evacuating = obs.evacuating();
    plan::target_replicas(&current, &active, rf)
        .into_iter()
        .filter(|(p, _)| {
            obs.partition(*p).is_some_and(|x| {
                x.reassignment.is_none()
                    && (auto || x.replicas.iter().any(|r| evacuating.contains(r)))
            })
        })
        .collect()
}

/// The `AlterReplicas` for `step` on `p` (`None` for steps that propose nothing). Expand keeps
/// the current ISR and `min_insync` (Pitfall 6: never raised before the target is in the ISR)
/// — except under the `raise_min_insync_early` mutant.
pub fn alter_command(p: &PartitionObs, step: &ReassignStep, policy_min: u8) -> Option<MetaCommand> {
    match step {
        ReassignStep::Expand {
            replicas,
            reassignment,
        } => {
            let min_insync = if mutant(MUTANT_RAISE_MIN_INSYNC_EARLY) {
                policy_min
            } else {
                p.min_insync
            };
            Some(MetaCommand::AlterReplicas {
                partition: p.partition,
                epoch: p.epoch,
                expected_isr_version: p.isr_version,
                replicas: replicas.clone(),
                isr: p.isr.clone(),
                min_insync,
                reassignment: Some(reassignment.clone()),
            })
        }
        ReassignStep::Shrink {
            replicas,
            isr,
            min_insync,
        } => Some(MetaCommand::AlterReplicas {
            partition: p.partition,
            epoch: p.epoch,
            expected_isr_version: p.isr_version,
            replicas: replicas.clone(),
            isr: isr.clone(),
            min_insync: *min_insync,
            reassignment: None,
        }),
        _ => None,
    }
}

/// The proposer-side gate of `cmd` (D-P13-3 rule 1): `AlterReplicas`, plus `ReassignmentExt`
/// when it writes ext 100.
fn gate(
    meta: &MetaHandle,
    cmd: &MetaCommand,
    level_max: u32,
) -> Result<(), compat::FeatureNotEnabled> {
    compat::require(meta.view(), Feature::AlterReplicas, level_max)?;
    if let MetaCommand::AlterReplicas {
        reassignment: Some(_),
        ..
    } = cmd
    {
        compat::require(meta.view(), Feature::ReassignmentExt, level_max)?;
    }
    Ok(())
}

fn step_name(step: &ReassignStep) -> &'static str {
    match step {
        ReassignStep::Expand { .. } => "expand",
        ReassignStep::CatchUp => "catch_up",
        ReassignStep::LeaderMove { .. } => "leader_move",
        ReassignStep::Shrink { .. } => "shrink",
        ReassignStep::None => "none",
    }
}

// ---- the controller ----------------------------------------------------------------------

/// One thing a tick did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// An `AlterReplicas` was proposed for `partition` (`step` = `expand` / `shrink`).
    Proposed {
        partition: u16,
        step: &'static str,
        result: Result<MetaResult, String>,
    },
    /// The gate refused an `AlterReplicas` (cluster not finalized at the feature's level).
    Refused { partition: u16, reason: String },
    /// A handoff was started (`LeaderMove`, or the leader balancer when `balance`).
    HandOff {
        partition: u16,
        to: u64,
        balance: bool,
    },
}

/// The result of one tick.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TickReport {
    /// This node led meta during the tick.
    pub leader: bool,
    /// What the tick did.
    pub actions: Vec<Action>,
    /// Reassignments in flight after the tick (as observed at its start, plus started ones,
    /// minus completed ones).
    pub in_flight: usize,
    /// Partitions waiting on `CatchUp`.
    pub catching_up: Vec<u16>,
    /// Partitions that cannot start a move ([`blocked_partitions`]).
    pub blocked: Vec<u16>,
}

/// Controller state kept across ticks (on the current meta leader only).
#[derive(Debug, Default)]
struct Memory {
    /// Last LeaderMove handoff attempt per partition (ms).
    handoff_at: BTreeMap<u16, u64>,
    /// Handoffs still running.
    handoffs_running: BTreeSet<u16>,
    /// Last leader-balancer pass (ms); set when leadership is gained.
    balanced_at: Option<u64>,
}

struct Inner {
    meta: Arc<MetaHandle>,
    handoff: Arc<dyn HandoffClient>,
    cfg: PlacementConfig,
    clock: Arc<dyn Clock>,
    live: Option<LiveSource>,
    memory: Mutex<Memory>,
}

/// The placement controller (runs on every node, acts only as meta leader).
pub struct PlacementController {
    inner: Arc<Inner>,
    trigger: Option<PlacementTrigger>,
}

impl std::fmt::Debug for PlacementController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlacementController")
            .field("cfg", &self.inner.cfg)
            .finish_non_exhaustive()
    }
}

impl PlacementController {
    /// A controller over `meta`, moving leaders through `handoff`, timed by `clock` (the
    /// one injectable clock: `started_at_ms`, handoff retry, balancer interval).
    pub fn new(
        meta: Arc<MetaHandle>,
        handoff: Arc<dyn HandoffClient>,
        cfg: PlacementConfig,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                meta,
                handoff,
                cfg,
                clock,
                live: None,
                memory: Mutex::new(Memory::default()),
            }),
            trigger: None,
        }
    }

    fn inner_mut(&mut self) -> &mut Inner {
        Arc::get_mut(&mut self.inner).expect("builder methods run before start")
    }

    /// The live-set source for the leader balancer (default: raft `recent_active` on the
    /// meta leader plus itself).
    pub fn with_live_source(mut self, live: LiveSource) -> Self {
        self.inner_mut().live = Some(live);
        self
    }

    /// Wake the loop through `trigger` (`POST /v1/cluster/rebalance`): a forced plan +
    /// balancer pass.
    pub fn with_trigger(mut self, trigger: PlacementTrigger) -> Self {
        self.trigger = Some(trigger);
        self
    }

    /// One tick (module docs); `force` = an explicit rebalance (plan every move and run the
    /// balancer now).
    pub async fn tick(&self, force: bool) -> TickReport {
        self.inner.tick(force).await
    }

    /// Start the loop: a tick every `interval` and on every trigger, until `stop`.
    pub fn start(self, stop: CancellationToken) -> JoinHandle<()> {
        let inner = self.inner;
        let trigger = self.trigger.unwrap_or_default();
        tokio::spawn(async move {
            let mut force = false;
            loop {
                let report = inner.tick(force).await;
                force = false;
                let wait = if report.actions.is_empty() {
                    inner.cfg.interval
                } else {
                    // Something happened: look again soon.
                    inner.cfg.interval.min(Duration::from_millis(100))
                };
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = trigger.notified() => force = true,
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        })
    }
}

impl Inner {
    fn memory(&self) -> std::sync::MutexGuard<'_, Memory> {
        self.memory.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn live_set(&self, universe: &BTreeSet<u64>) -> BTreeSet<u64> {
        let mut live = match &self.live {
            Some(f) => f(universe),
            None => self
                .meta
                .raft_detail()
                .progress
                .iter()
                .filter(|p| p.recent_active)
                .map(|p| p.id)
                .collect(),
        };
        live.insert(self.meta.node_id());
        live
    }

    async fn propose(
        &self,
        p: &PartitionObs,
        step: &ReassignStep,
        cmd: MetaCommand,
        actions: &mut Vec<Action>,
    ) -> bool {
        if let Err(e) = gate(&self.meta, &cmd, self.cfg.level_max) {
            metrics::counter!(MOVES_TOTAL, "result" => "refused").increment(1);
            tracing::debug!(partition = p.partition, error = %e, "AlterReplicas refused by the feature gate");
            actions.push(Action::Refused {
                partition: p.partition,
                reason: e.detail(),
            });
            return false;
        }
        let r = self.meta.propose(cmd).await;
        let ok = matches!(r, Ok(MetaResult::Ok { .. }));
        let label = match (&r, step) {
            (Ok(MetaResult::Ok { .. }), ReassignStep::Expand { .. })
                if p.reassignment.is_none() =>
            {
                "started"
            }
            (Ok(MetaResult::Ok { .. }), ReassignStep::Shrink { .. }) => "completed",
            (Ok(MetaResult::Ok { .. }), _) => "step",
            (Ok(MetaResult::StaleVersion { .. }), _) => "stale",
            _ => "rejected",
        };
        metrics::counter!(MOVES_TOTAL, "result" => label).increment(1);
        match &r {
            Ok(MetaResult::Ok { .. }) => tracing::info!(
                partition = p.partition,
                step = step_name(step),
                "reassignment step applied"
            ),
            Ok(MetaResult::StaleVersion { .. }) => tracing::debug!(
                partition = p.partition,
                "reassignment step stale; re-reading next tick"
            ),
            other => tracing::warn!(
                partition = p.partition,
                step = step_name(step),
                result = ?other,
                "reassignment step not applied"
            ),
        }
        actions.push(Action::Proposed {
            partition: p.partition,
            step: step_name(step),
            result: r.map_err(|e| e.to_string()),
        });
        ok
    }

    /// Start a handoff of `p` to `to` in the background (at most one per partition).
    fn hand_off(self: &Arc<Self>, p: &PartitionObs, to: u64, balance: bool, now: u64) -> bool {
        {
            let mut mem = self.memory();
            if mem.handoffs_running.contains(&p.partition) {
                return false;
            }
            if !balance
                && mem
                    .handoff_at
                    .get(&p.partition)
                    .is_some_and(|at| now.saturating_sub(*at) < HANDOFF_RETRY_MS)
            {
                return false;
            }
            mem.handoffs_running.insert(p.partition);
            mem.handoff_at.insert(p.partition, now);
        }
        let client = Arc::clone(&self.handoff);
        let (partition, leader, epoch) = (p.partition, p.leader, p.epoch);
        let me = Arc::downgrade(self);
        tokio::spawn(async move {
            let r = client
                .hand_off(leader, PartitionId(partition), to, Epoch(epoch))
                .await;
            match &r {
                Ok(HandoffOutcome::Moved { new_epoch }) => {
                    metrics::counter!(MOVES_TOTAL, "result" => "handoff_moved").increment(1);
                    tracing::info!(
                        partition,
                        to,
                        epoch = new_epoch.0,
                        balance,
                        "leader handed off"
                    );
                }
                other => {
                    metrics::counter!(MOVES_TOTAL, "result" => "handoff_failed").increment(1);
                    tracing::info!(partition, to, balance, result = ?other, "leader handoff did not move");
                }
            }
            if let Some(inner) = me.upgrade() {
                inner.memory().handoffs_running.remove(&partition);
            }
        });
        true
    }

    async fn tick(self: &Arc<Self>, force: bool) -> TickReport {
        let mut report = TickReport::default();
        if !self.meta.is_leader() {
            let mut mem = self.memory();
            mem.handoff_at.clear();
            mem.balanced_at = None;
            return report;
        }
        report.leader = true;
        let now = self.clock.now_ms();
        let obs = self.meta.view().read(observe);
        let (_, policy_min) = effective_policy(&obs);
        let auto = force || obs.policy.is_some();
        let active = obs.active();
        let mut actions = Vec::new();
        let mut in_flight = 0usize;

        // 2. drive the in-flight reassignments.
        let replanned = planned_moves_for_retarget(&obs);
        for p in obs.partitions.iter().filter(|p| p.reassignment.is_some()) {
            let ext_target = p
                .reassignment
                .as_ref()
                .map(|r| r.target.clone())
                .unwrap_or_default();
            let target = if ext_target.iter().all(|t| active.contains(t)) || active.is_empty() {
                ext_target
            } else {
                replanned.get(&p.partition).cloned().unwrap_or(ext_target)
            };
            let step = reassign::next_step_at(p, &target, policy_min, now);
            match &step {
                ReassignStep::CatchUp => {
                    in_flight += 1;
                    report.catching_up.push(p.partition);
                }
                ReassignStep::LeaderMove { to } => {
                    in_flight += 1;
                    if self.hand_off(p, *to, false, now) {
                        actions.push(Action::HandOff {
                            partition: p.partition,
                            to: *to,
                            balance: false,
                        });
                    }
                }
                ReassignStep::Expand { .. } | ReassignStep::Shrink { .. } => {
                    let shrink = matches!(step, ReassignStep::Shrink { .. });
                    let cmd = alter_command(p, &step, policy_min).expect("proposing step");
                    let ok = self.propose(p, &step, cmd, &mut actions).await;
                    if !(shrink && ok) {
                        in_flight += 1;
                    }
                }
                ReassignStep::None => in_flight += 1,
            }
        }

        // 3. start new reassignments (≤ max_concurrent_moves in flight).
        let blocked = blocked_partitions(&obs);
        report.blocked = blocked.iter().map(|(p, _)| *p).collect();
        for (p, why) in &blocked {
            tracing::debug!(partition = p, reason = %why, "reassignment blocked");
        }
        metrics::gauge!(BLOCKED_GAUGE).set(blocked.len() as f64);
        if in_flight < self.cfg.max_concurrent_moves {
            for (part, target) in planned_moves(&obs, auto) {
                if in_flight >= self.cfg.max_concurrent_moves {
                    break;
                }
                let Some(p) = obs.partition(part) else {
                    continue;
                };
                let step = reassign::next_step_at(p, &target, policy_min, now);
                if let Some(cmd) = alter_command(p, &step, policy_min) {
                    if self.propose(p, &step, cmd, &mut actions).await {
                        in_flight += 1;
                    } else if actions
                        .last()
                        .is_some_and(|a| matches!(a, Action::Refused { .. }))
                    {
                        // The gate refuses every AlterReplicas alike: stop for this tick.
                        break;
                    }
                }
            }
        }
        report.in_flight = in_flight;
        metrics::gauge!(IN_FLIGHT_GAUGE).set(in_flight as f64);

        // 4. leader balancer (one move per interval, nothing in flight).
        let interval_ms =
            u64::try_from(self.cfg.leader_rebalance_interval.as_millis()).unwrap_or(u64::MAX);
        let due = {
            let mut mem = self.memory();
            let since = *mem.balanced_at.get_or_insert(now);
            force || (obs.policy.is_some() && now.saturating_sub(since) >= interval_ms)
        };
        if due && in_flight == 0 {
            self.memory().balanced_at = Some(now);
            let parts: Vec<LeaderObs> = obs
                .partitions
                .iter()
                .map(|p| LeaderObs {
                    partition: p.partition,
                    replicas: p.replicas.clone(),
                    isr: p.isr.clone(),
                    leader: p.leader,
                })
                .collect();
            let universe: BTreeSet<u64> = obs.members.iter().map(|(id, _)| *id).collect();
            let live: BTreeSet<u64> = self
                .live_set(&universe)
                .into_iter()
                .filter(|id| active.contains(id))
                .collect();
            if let Some(m) = leaders::decide(&parts, &live, active.len())
                && let Some(p) = obs.partition(m.partition)
                && self.hand_off(p, m.to, true, now)
            {
                actions.push(Action::HandOff {
                    partition: m.partition,
                    to: m.to,
                    balance: true,
                });
            }
        }
        report.actions = actions;
        report
    }
}

/// Targets for in-flight reassignments whose target names a member that is no longer
/// Active: the planner's answer over the current replicas (not the stale target).
fn planned_moves_for_retarget(obs: &ClusterObs) -> BTreeMap<u16, Vec<u64>> {
    let (rf, _) = effective_policy(obs);
    let current: BTreeMap<u16, Vec<u64>> = obs
        .partitions
        .iter()
        .map(|p| (p.partition, p.replicas.clone()))
        .collect();
    let mut out = plan::target_replicas(&current, &obs.active(), rf);
    // A partition the planner leaves unchanged keeps its current replicas as the target.
    for (p, r) in current {
        out.entry(p).or_insert(r);
    }
    out
}

/// Wakes the placement controller for an immediate plan + balancer pass.
#[derive(Clone, Debug, Default)]
pub struct PlacementTrigger(Arc<tokio::sync::Notify>);

impl PlacementTrigger {
    /// A fresh trigger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request a pass.
    pub fn notify(&self) {
        self.0.notify_one();
    }

    /// Wait for a request.
    pub async fn notified(&self) {
        self.0.notified().await;
    }
}

/// Why an explicit placement request was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// The request is invalid (unknown partition, target not Active, duplicates, ...).
    #[error("invalid placement request: {0}")]
    Invalid(String),
    /// The cluster level does not allow it yet (`feature_not_enabled`).
    #[error(transparent)]
    FeatureNotEnabled(#[from] crate::compat::FeatureNotEnabled),
    /// Nothing to cancel, or the reassignment is past LeaderMove.
    #[error("{0}")]
    Conflict(String),
    /// Meta refused or failed.
    #[error("meta: {0}")]
    Meta(String),
    /// Not available before 13-10 (kept for the 13-02 API mapping; never returned).
    #[error("Phase 13: implemented in 13-10")]
    NotImplemented,
}

/// `POST /v1/cluster/reassign`: start a reassignment of `partition` to `target`, or cancel
/// the in-flight one (`cancel`, allowed before LeaderMove). Runs on any node (the proposal
/// forwards to the meta leader); the meta leader's controller drives the rest.
///
/// Validation: the gate (`AlterReplicas` at the finalized level), the partition exists and is
/// placed (epoch ≥ 1), and for a move: |target| ≥ 1, no duplicates, every target member
/// Active. A target equal to the current replicas with nothing in flight is a no-op; one
/// equal to the in-flight target is accepted as is. `min_insync > |ISR|` → `Conflict`
/// (retry once the ISR recovers).
pub async fn request_reassign(
    meta: &MetaHandle,
    partition: u16,
    target: Vec<u64>,
    cancel: bool,
    level_max: u32,
    now_ms: u64,
) -> Result<(), PlacementError> {
    compat::require(meta.view(), Feature::AlterReplicas, level_max)?;
    compat::require(meta.view(), Feature::ReassignmentExt, level_max)?;
    meta.read_index_barrier()
        .await
        .map_err(|e| PlacementError::Meta(e.to_string()))?;
    for _ in 0..REQUEST_RETRIES {
        let (obs, exists) = meta
            .view()
            .read(|s| (observe(s), s.partition(partition).is_some()));
        if !exists {
            return Err(PlacementError::Invalid(format!(
                "unknown partition {partition}"
            )));
        }
        let Some(p) = obs.partition(partition).cloned() else {
            return Err(PlacementError::Invalid(format!(
                "partition {partition} is not placed yet (epoch 0)"
            )));
        };
        let (_, policy_min) = effective_policy(&obs);
        let target = if cancel {
            if p.reassignment.is_none() {
                return Err(PlacementError::Conflict(format!(
                    "partition {partition} has no reassignment in flight"
                )));
            }
            match reassign::cancel_target(&p) {
                Some(t) => t,
                None => {
                    return Err(PlacementError::Conflict(format!(
                        "partition {partition}: the reassignment is past LeaderMove and can no longer be cancelled"
                    )));
                }
            }
        } else {
            validate_target(&obs, &target)?;
            target.clone()
        };
        let step = reassign::next_step_at(&p, &target, policy_min, now_ms);
        let cmd = match &step {
            ReassignStep::Expand { .. } => alter_command(&p, &step, policy_min),
            ReassignStep::None if p.reassignment.is_none() && p.replicas == target => {
                return Ok(());
            }
            ReassignStep::None => {
                return Err(PlacementError::Conflict(format!(
                    "partition {partition}: min_insync {} > ISR {:?}; retry once the ISR recovers",
                    p.min_insync, p.isr
                )));
            }
            // Already in flight towards this target: the controller drives it.
            _ => return Ok(()),
        };
        let Some(cmd) = cmd else { return Ok(()) };
        match meta.propose(cmd).await {
            Ok(MetaResult::Ok { .. }) => {
                metrics::counter!(MOVES_TOTAL, "result" => if cancel { "cancel" } else { "started" })
                    .increment(1);
                return Ok(());
            }
            Ok(MetaResult::StaleVersion { .. }) => {
                tokio::task::yield_now().await;
                continue;
            }
            Ok(other) => return Err(PlacementError::Meta(format!("{other:?}"))),
            Err(e) => return Err(PlacementError::Meta(e.to_string())),
        }
    }
    Err(PlacementError::Conflict(format!(
        "partition {partition} kept changing; retry"
    )))
}

fn validate_target(obs: &ClusterObs, target: &[u64]) -> Result<(), PlacementError> {
    if target.is_empty() {
        return Err(PlacementError::Invalid(
            "the target replica set is empty".into(),
        ));
    }
    for (i, t) in target.iter().enumerate() {
        if target[..i].contains(t) {
            return Err(PlacementError::Invalid(format!(
                "the target {target:?} lists node {t} twice"
            )));
        }
        match obs.members.iter().find(|(id, _)| id == t) {
            Some((_, s)) if *s == MEMBER_ACTIVE => {}
            Some((_, s)) => {
                return Err(PlacementError::Invalid(format!(
                    "node {t} is {} (the target must be Active members)",
                    crate::membership::service::state_name(*s)
                )));
            }
            None => {
                return Err(PlacementError::Invalid(format!("node {t} is not a member")));
            }
        }
    }
    Ok(())
}

/// `POST /v1/cluster/rebalance`: an immediate plan + balancer pass. Wakes the local
/// controller through `trigger`; when this node is not the meta leader it also starts the
/// planned replica moves itself (bounded by the default `max_concurrent_moves`), which the
/// meta leader's controller then drives. The leader balancer runs on the meta leader only.
///
/// `now_ms` (the node's injected clock, as [`request_reassign`]) stamps `started_at_ms` of
/// the moves it starts (13-18: it used `SystemClock`).
pub async fn request_rebalance(
    meta: &MetaHandle,
    trigger: &PlacementTrigger,
    level_max: u32,
    now_ms: u64,
) -> Result<(), PlacementError> {
    compat::require(meta.view(), Feature::AlterReplicas, level_max)?;
    compat::require(meta.view(), Feature::ReassignmentExt, level_max)?;
    trigger.notify();
    if meta.is_leader() {
        return Ok(());
    }
    meta.read_index_barrier()
        .await
        .map_err(|e| PlacementError::Meta(e.to_string()))?;
    let obs = meta.view().read(observe);
    let (_, policy_min) = effective_policy(&obs);
    let mut in_flight = obs
        .partitions
        .iter()
        .filter(|p| p.reassignment.is_some())
        .count();
    let max = PlacementConfig::default().max_concurrent_moves;
    let now = now_ms;
    for (part, target) in planned_moves(&obs, true) {
        if in_flight >= max {
            break;
        }
        let Some(p) = obs.partition(part) else {
            continue;
        };
        let step = reassign::next_step_at(p, &target, policy_min, now);
        if let Some(cmd) = alter_command(p, &step, policy_min) {
            match meta.propose(cmd).await {
                Ok(MetaResult::Ok { .. }) => {
                    metrics::counter!(MOVES_TOTAL, "result" => "started").increment(1);
                    in_flight += 1;
                }
                Ok(_) => {}
                Err(e) => return Err(PlacementError::Meta(e.to_string())),
            }
        }
    }
    Ok(())
}
