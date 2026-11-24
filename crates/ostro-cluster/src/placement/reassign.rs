//! Phase 13 (13-06): the pure reassignment state machine (D-P13-12, KIP-455 shape).
//!
//! The state lives only in meta (replicas, ISR, leader, `min_insync`, ext 100), so the
//! machine is a function of one [`PartitionObs`] and survives meta failover:
//!
//! | observation | step |
//! |-------------|------|
//! | epoch 0, empty target, or `replicas == target` with no ext | [`ReassignStep::None`] |
//! | no ext, or ext for another target | [`ReassignStep::Expand`] (`replicas = current ∪ target`) — `None` while `min_insync > |isr|` (that `AlterReplicas` would be refused) |
//! | ext for this target, target ⊄ published ISR | [`ReassignStep::CatchUp`] |
//! | target ⊆ ISR, live leader ∉ target | [`ReassignStep::LeaderMove`] to the first target member |
//! | target ⊆ ISR, leader ∈ target or offline | [`ReassignStep::Shrink`] |
//!
//! Safety (model-checked in `tests/reassign_model.rs`): every emitted `AlterReplicas` keeps
//! `isr ⊆ replicas`, the leader in both, and `1 ≤ min_insync ≤ |isr|`; Expand carries the
//! current ISR and `min_insync` unchanged, so only Shrink raises `min_insync`, and only to
//! [`shrink_min_insync`] = `min(max(current, min(policy, |target|)), |target|)` once the
//! whole target is in the published ISR (plan check W5). Cancel ([`cancel_target`]) is the
//! same machine re-targeted at the original replicas, allowed only while the leader is still
//! one of them (before LeaderMove).

use ostro_meta::ext::Reassignment;

/// One partition as the reassignment machine sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionObs {
    /// Partition.
    pub partition: u16,
    /// Leader epoch (0 = unplaced: never touched).
    pub epoch: u32,
    /// Leader (0 = offline).
    pub leader: u64,
    /// Current replicas.
    pub replicas: Vec<u64>,
    /// Published ISR.
    pub isr: Vec<u64>,
    /// `isr_version` (the CAS token of AlterReplicas).
    pub isr_version: u64,
    /// `min_insync`.
    pub min_insync: u8,
    /// In-flight reassignment (ext 100).
    pub reassignment: Option<Reassignment>,
}

/// The next step of one partition's reassignment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReassignStep {
    /// `AlterReplicas` with `replicas = old ∪ target` and the reassignment ext.
    Expand {
        replicas: Vec<u64>,
        reassignment: Reassignment,
    },
    /// Wait for the adding replicas to join the ISR.
    CatchUp,
    /// Graceful handoff to `to` (leader ∉ target).
    LeaderMove { to: u64 },
    /// `AlterReplicas` with `replicas = target`, `isr ∩ target`, `min_insync`, ext cleared.
    Shrink {
        replicas: Vec<u64>,
        isr: Vec<u64>,
        min_insync: u8,
    },
    /// Nothing to do.
    None,
}

/// The next step for `p` towards `target` (policy `min_insync`); a new reassignment's
/// `started_at_ms` is 0 (see [`next_step_at`]).
pub fn next_step(p: &PartitionObs, target: &[u64], policy_min_insync: u8) -> ReassignStep {
    next_step_at(p, target, policy_min_insync, 0)
}

/// [`next_step`] stamping `started_at_ms = now_ms` on a new reassignment ext.
pub fn next_step_at(
    p: &PartitionObs,
    target: &[u64],
    policy_min_insync: u8,
    now_ms: u64,
) -> ReassignStep {
    let target = dedup(target);
    if p.epoch == 0 || target.is_empty() {
        return ReassignStep::None;
    }
    let in_flight = p
        .reassignment
        .as_ref()
        .is_some_and(|r| dedup(&r.target) == target)
        && target.iter().all(|t| p.replicas.contains(t));
    if !in_flight {
        if p.reassignment.is_none() && p.replicas == target {
            return ReassignStep::None;
        }
        if usize::from(p.min_insync) > p.isr.len() {
            // AlterReplicas requires min_insync ≤ |isr|; wait for the ISR to recover.
            return ReassignStep::None;
        }
        let original = original_replicas(p);
        let mut replicas = p.replicas.clone();
        for t in &target {
            if !replicas.contains(t) {
                replicas.push(*t);
            }
        }
        let reassignment = Reassignment {
            adding: replicas
                .iter()
                .copied()
                .filter(|r| !original.contains(r))
                .collect(),
            removing: original
                .iter()
                .copied()
                .filter(|r| !target.contains(r))
                .collect(),
            target,
            started_at_ms: now_ms,
        };
        return ReassignStep::Expand {
            replicas,
            reassignment,
        };
    }
    if !target.iter().all(|t| p.isr.contains(t)) {
        return ReassignStep::CatchUp;
    }
    if p.leader != 0 && !target.contains(&p.leader) {
        return ReassignStep::LeaderMove { to: target[0] };
    }
    ReassignStep::Shrink {
        min_insync: shrink_min_insync(p.min_insync, policy_min_insync, target.len()),
        isr: target.clone(),
        replicas: target,
    }
}

/// The replica set a cancel restores: the replicas before the in-flight reassignment
/// (`replicas \ ext.adding`). `None` when nothing is in flight or the leader has already
/// moved off the original replicas (cancel is allowed only before LeaderMove). Driving
/// [`next_step`] towards the returned set re-targets and then shrinks back to it.
pub fn cancel_target(p: &PartitionObs) -> Option<Vec<u64>> {
    p.reassignment.as_ref()?;
    let original = original_replicas(p);
    if original.is_empty() || (p.leader != 0 && !original.contains(&p.leader)) {
        return None;
    }
    Some(original)
}

/// The Shrink `min_insync` (plan check W5): `min(max(current, min(policy, |target|)),
/// |target|)`, at least 1. Never below `min(policy, |target|)`, never above `|target|`.
pub fn shrink_min_insync(current: u8, policy: u8, target_len: usize) -> u8 {
    let t = u8::try_from(target_len).unwrap_or(u8::MAX).max(1);
    current.max(policy.min(t)).min(t).max(1)
}

/// Replicas before the in-flight reassignment (all replicas when none is in flight).
fn original_replicas(p: &PartitionObs) -> Vec<u64> {
    match &p.reassignment {
        Some(r) => p
            .replicas
            .iter()
            .copied()
            .filter(|x| !r.adding.contains(x))
            .collect(),
        None => p.replicas.clone(),
    }
}

/// `v` without repeated ids (first occurrence kept).
fn dedup(v: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(v.len());
    for x in v {
        if !out.contains(x) {
            out.push(*x);
        }
    }
    out
}
