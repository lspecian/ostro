//! Published-ISR rules (KIP-497 maximal ISR, D-P12-5).
//!
//! The leader's ISR is the **published** one (meta), never the assignment's replicas. A
//! change goes through one pending `AlterIsr` at a time; while it is pending the leader
//! counts the *maximal* ISR for the HW: the published set plus a pending add, and a pending
//! remove still counts until meta committed it (so a rejected shrink can never have let the
//! HW pass the removed replica's LEO).

use ostro_types::Offset;

use super::tracker::ReplicaTracker;
use crate::mutants;

/// One proposed (not yet committed) ISR change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingChange {
    /// Add the replica to the ISR.
    Add(u64),
    /// Remove the replica from the ISR.
    Remove(u64),
}

/// The leader's view of the ISR: the published set, its meta CAS token and at most one
/// in-flight `AlterIsr`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IsrState {
    /// The published ISR (meta).
    pub published: Vec<u64>,
    /// Meta CAS token of `published`.
    pub version: u64,
    /// The in-flight `AlterIsr`, if any.
    pub pending: Option<PendingChange>,
}

impl IsrState {
    /// State with no pending change.
    pub fn new(published: Vec<u64>, version: u64) -> Self {
        Self {
            published,
            version,
            pending: None,
        }
    }

    /// The ISR `change` would publish.
    pub fn proposed(&self, change: PendingChange) -> Vec<u64> {
        let mut out = self.published.clone();
        match change {
            PendingChange::Add(r) => {
                if !out.contains(&r) {
                    out.push(r);
                }
            }
            PendingChange::Remove(r) => out.retain(|x| *x != r),
        }
        out
    }

    /// Start an `AlterIsr`; `false` (and no change) while another one is pending.
    pub fn propose(&mut self, change: PendingChange) -> bool {
        if self.pending.is_some() {
            return false;
        }
        self.pending = Some(change);
        true
    }

    /// Meta committed the pending change at `version`.
    pub fn commit(&mut self, version: u64) {
        if let Some(change) = self.pending.take() {
            self.published = self.proposed(change);
        }
        self.version = version;
    }

    /// Meta rejected the pending change: drop it, the published set is unchanged.
    pub fn reject(&mut self) {
        self.pending = None;
    }

    /// Adopt a published ISR from meta (election, controller shrink). A pending change
    /// stays pending; its CAS will fail on the version and be rejected.
    pub fn set_published(&mut self, published: Vec<u64>, version: u64) {
        self.published = published;
        self.version = version;
    }
}

/// KIP-497 maximal ISR: published ∪ pending add; a pending remove still counts.
pub fn maximal(s: &IsrState) -> Vec<u64> {
    let mut out = s.published.clone();
    match s.pending {
        Some(PendingChange::Add(r)) if !out.contains(&r) => out.push(r),
        Some(PendingChange::Remove(r)) if super::mutant(mutants::SHRINK_BEFORE_COMMIT) => {
            out.retain(|x| *x != r);
        }
        _ => {}
    }
    out
}

/// Whether `replica` may join the ISR (Kafka `isFollowerInSync` + KIP-101 epoch rule):
/// it fetched, caught up within `lag_max_ms`, and its durable LEO reached
/// `max(hw, epoch_start)` (the start offset of the leader's current epoch, so a joining
/// replica already holds this leader's first record). A replica a published ISR removed
/// joins again only after a caught-up observation newer than that removal (14-33,
/// [`ReplicaTracker::on_isr_published`]): stale progress never re-adds it.
pub fn should_expand(
    t: &ReplicaTracker,
    replica: u64,
    hw: Offset,
    epoch_start: Offset,
    now_ms: u64,
    lag_max_ms: u64,
) -> bool {
    if super::mutant(mutants::ISR_FROM_ASSIGNMENT) {
        return t.replicas().contains(&replica);
    }
    if t.removed_at_version(replica).is_some() {
        return false;
    }
    let (Some(durable), Some(_), Some(lag)) = (
        t.durable_leo(replica),
        t.last_fetch_ms(replica),
        t.lag_ms(replica, now_ms),
    ) else {
        return false;
    };
    durable >= hw.max(epoch_start) && lag <= lag_max_ms
}

/// Whether `replica` should be proposed for removal: it has not caught up for longer than
/// `lag_max_ms` (Kafka `replica.lag.time.max.ms`). Never the leader.
pub fn should_shrink(t: &ReplicaTracker, replica: u64, now_ms: u64, lag_max_ms: u64) -> bool {
    replica != t.self_id()
        && t.lag_ms(replica, now_ms)
            .is_some_and(|lag| lag > lag_max_ms)
}

/// The next `AlterIsr` to propose, or `None` (one pending change at a time). Shrinks come
/// first (published members in `replicas` order), then expansions of non-members.
pub fn next_change(
    t: &ReplicaTracker,
    s: &IsrState,
    replicas: &[u64],
    hw: Offset,
    epoch_start: Offset,
    now_ms: u64,
    lag_max_ms: u64,
) -> Option<PendingChange> {
    if s.pending.is_some() {
        return None;
    }
    let shrink = replicas
        .iter()
        .copied()
        .filter(|r| s.published.contains(r))
        .find(|r| should_shrink(t, *r, now_ms, lag_max_ms));
    if let Some(r) = shrink
        && s.published.len() > 1
    {
        return Some(PendingChange::Remove(r));
    }
    replicas
        .iter()
        .copied()
        .filter(|r| !s.published.contains(r))
        .find(|r| should_expand(t, *r, hw, epoch_start, now_ms, lag_max_ms))
        .map(PendingChange::Add)
}
