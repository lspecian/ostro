//! Fetch divergence check (KIP-595 `DivergingEpoch` inside Fetch, KIP-279 iteration,
//! KIP-101 no truncate-to-HW; D-P12-6).

use ostro_types::{Epoch, Offset};

use crate::replicated::EpochHistory;

/// The leader's verdict on a follower's `(fetch_offset, last_fetched_epoch)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchCheck {
    /// The follower's log is a prefix of the leader's: serve from `fetch_offset`.
    Ok,
    /// The follower must truncate: the leader's `epoch` (largest it knows `<=` the
    /// follower's) ended at `end`.
    Diverging {
        /// Epoch answered by the leader.
        epoch: Epoch,
        /// Where that epoch ended on the leader.
        end: Offset,
    },
    /// The follower cannot be repaired by truncation (offset below the leader's log start,
    /// or an epoch older than the leader's retained history): install a snapshot.
    SnapshotRequired,
}

/// The epoch in force at `offset` per `h` (the first entry when `offset` precedes it,
/// i.e. a seeded/trimmed history).
fn epoch_at(h: &EpochHistory, offset: Offset) -> Option<Epoch> {
    h.0.iter()
        .rev()
        .find(|(_, s)| *s <= offset)
        .or_else(|| h.0.first())
        .map(|(e, _)| *e)
}

/// Leader-side check of a follower fetch (KIP-595).
///
/// - `fetch_offset < log_start` → `SnapshotRequired`.
/// - `fetch_offset == log_start` with no epoch, or with `last_fetched_epoch <=` the epoch in
///   force at `log_start` → `Ok` (W5: an installed follower anchored at the snapshot offset
///   never loops, even after the leader trimmed its history below `log_start`).
/// - otherwise the leader answers `end_offset_for_epoch_at(last_fetched_epoch, leo)`:
///   UNDEFINED → `SnapshotRequired` (never truncate-to-HW, KIP-101); the same epoch ending
///   at or after `fetch_offset` → `Ok`; anything else → `Diverging{epoch, end}`.
/// - no epoch above `log_start`: a follower holding records always knows their epoch, so
///   this cannot be validated → `SnapshotRequired`.
pub fn check(
    h: &EpochHistory,
    log_start: Offset,
    leo: Offset,
    fetch_offset: Offset,
    last_fetched_epoch: Option<Epoch>,
) -> FetchCheck {
    if fetch_offset < log_start {
        return FetchCheck::SnapshotRequired;
    }
    let Some(e) = last_fetched_epoch else {
        return if fetch_offset == log_start {
            FetchCheck::Ok
        } else {
            FetchCheck::SnapshotRequired
        };
    };
    if fetch_offset == log_start && epoch_at(h, log_start).is_none_or(|at| e <= at) {
        return FetchCheck::Ok;
    }
    match h.end_offset_for_epoch_at(e, leo) {
        None => FetchCheck::SnapshotRequired,
        Some((epoch, end)) if epoch == e && fetch_offset <= end => FetchCheck::Ok,
        Some((epoch, end)) => FetchCheck::Diverging { epoch, end },
    }
}

/// Follower-side truncation point for a `Diverging{epoch, end}` answer (KIP-279):
/// `min(end, own end of that epoch)`, never above `own_leo`. The follower truncates there,
/// re-fetches with its new last epoch, and repeats until `Ok`.
///
/// When the follower holds no epoch `<= diverging_epoch`, it truncates to where its own
/// history begins (its records all belong to epochs the leader does not have).
pub fn follower_truncation_point(
    own: &EpochHistory,
    own_leo: Offset,
    diverging_epoch: Epoch,
    leader_end: Offset,
) -> Offset {
    let own_end = match own.end_offset_for_epoch_at(diverging_epoch, own_leo) {
        Some((_, end)) => end,
        None => own
            .0
            .iter()
            .find(|(e, _)| *e > diverging_epoch)
            .map_or(own_leo, |(_, s)| *s),
    };
    leader_end.min(own_end).min(own_leo)
}
