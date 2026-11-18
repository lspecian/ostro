// Ported from chronik-stream@08715f4 crates/chronik-server/src/replication/leader_epoch.rs; modified by Ostro (see VENDORED.md).
//! Leader epoch history (Kafka `LeaderEpochFileCache` semantics).
//!
//! A leader epoch increments every time a partition changes leader, and every envelope carries
//! the epoch of the leader that wrote it, so the log records its own leadership history. That
//! is what lets two replicas find *where* they diverged (Phase 12) instead of guessing: a
//! follower asks "where did epoch 4 end?", the leader answers "at 100", and the follower
//! truncates to 100, discarding exactly the records that were never replicated.
//!
//! The history is **derived from the log**: recovery rebuilds it from the epochs stamped on
//! the envelopes, and every append extends it.
//!
//! Ostro changes from upstream: Ostro's `Epoch`/`Offset` newtypes (u32/u64) replace
//! `i32`/`i64`; Kafka's `(UNDEFINED_EPOCH, UNDEFINED_OFFSET)` answer is `None`; the unstamped
//! (`-1`) epoch case is gone (every envelope carries an epoch); the per-topic `DashMap` store
//! and its checkpoint files are not ported (the history is rebuilt from the WAL at open).

use ostro_types::{Epoch, Offset};

/// The offset at which a leader epoch began.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEntry {
    /// The epoch.
    pub epoch: Epoch,
    /// First offset written by this epoch's leader.
    pub start_offset: Offset,
}

/// One partition's leadership history.
///
/// Entries are kept strictly increasing in both epoch and start offset.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EpochCache {
    entries: Vec<EpochEntry>,
}

impl EpochCache {
    /// Empty history.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Build from `(epoch, start)` pairs in log order (e.g. `RecoveryReport::epochs`).
    pub fn from_pairs(pairs: &[(Epoch, Offset)]) -> Self {
        let mut c = Self::new();
        for &(e, s) in pairs {
            c.assign(e, s);
        }
        c
    }

    /// Whether no epoch has been recorded.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries, oldest first.
    pub fn entries(&self) -> &[EpochEntry] {
        &self.entries
    }

    /// The entries as `(epoch, start)` pairs.
    pub fn to_pairs(&self) -> Vec<(Epoch, Offset)> {
        self.entries
            .iter()
            .map(|e| (e.epoch, e.start_offset))
            .collect()
    }

    /// The most recent epoch seen in the log.
    pub fn latest_epoch(&self) -> Option<Epoch> {
        self.entries.last().map(|e| e.epoch)
    }

    /// Record that `epoch` begins at `start_offset`.
    ///
    /// Called on every append with the batch's epoch; a no-op when the epoch has not changed.
    /// An epoch older than the latest is refused (the log would have been rewritten beneath
    /// us; the caller truncates first). A new epoch starting at or before an existing entry
    /// supersedes it. Returns whether the history changed.
    pub fn assign(&mut self, epoch: Epoch, start_offset: Offset) -> bool {
        if self.entries.last().is_some_and(|last| epoch <= last.epoch) {
            return false;
        }
        self.entries.retain(|e| e.start_offset < start_offset);
        self.entries.push(EpochEntry {
            epoch,
            start_offset,
        });
        true
    }

    /// The epoch in force at `offset`, if known.
    pub fn epoch_for_offset(&self, offset: Offset) -> Option<Epoch> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.start_offset <= offset)
            .map(|e| e.epoch)
    }

    /// Answer `OffsetForLeaderEpoch`: where did `requested` end? Kafka semantics exactly:
    ///
    /// - the **latest** epoch ends at the log end offset (it has not ended);
    /// - an **earlier** epoch ends where the next epoch began, answered with the largest
    ///   known epoch `<= requested`;
    /// - an epoch **newer than anything here**, or **older than anything still retained**,
    ///   is UNDEFINED (`None`): answering with a real number would tell a follower to discard
    ///   a correct log or keep a divergent one.
    pub fn end_offset_for_epoch(
        &self,
        requested: Epoch,
        log_end_offset: Offset,
    ) -> Option<(Epoch, Offset)> {
        let latest = self.entries.last()?.epoch;
        if requested == latest {
            return Some((requested, log_end_offset));
        }
        let previous = self.entries.iter().rev().find(|e| e.epoch <= requested);
        let subsequent = self.entries.iter().find(|e| e.epoch > requested);
        match (previous, subsequent) {
            (Some(p), Some(s)) => Some((p.epoch, s.start_offset)),
            _ => None,
        }
    }

    /// Drop history at or above `end_offset` after the log was truncated there. Returns
    /// whether the history changed.
    pub fn truncate_from_end(&mut self, end_offset: Offset) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.start_offset < end_offset);
        self.entries.len() != before
    }

    /// Drop history below `start_offset` after retention advanced the log start. The entry
    /// covering `start_offset` is kept and clamped, so the epoch of the oldest surviving
    /// record stays answerable.
    pub fn truncate_from_start(&mut self, start_offset: Offset) {
        if start_offset == Offset::ZERO || self.entries.is_empty() {
            return;
        }
        let covering = self
            .entries
            .iter()
            .rev()
            .find(|e| e.start_offset <= start_offset)
            .copied();
        self.entries.retain(|e| e.start_offset > start_offset);
        if let Some(mut entry) = covering {
            entry.start_offset = start_offset;
            self.entries.insert(0, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(entries: &[(u32, u64)]) -> EpochCache {
        let mut c = EpochCache::new();
        for &(epoch, start) in entries {
            c.assign(Epoch(epoch), Offset(start));
        }
        c
    }

    fn entry(epoch: u32, start: u64) -> EpochEntry {
        EpochEntry {
            epoch: Epoch(epoch),
            start_offset: Offset(start),
        }
    }

    fn end(c: &EpochCache, e: u32, leo: u64) -> Option<(u32, u64)> {
        c.end_offset_for_epoch(Epoch(e), Offset(leo))
            .map(|(e, o)| (e.0, o.0))
    }

    #[test]
    fn a_steady_leader_records_one_entry() {
        let mut c = EpochCache::new();
        for offset in [0, 10, 20, 30, 40] {
            c.assign(Epoch(5), Offset(offset));
        }
        assert_eq!(c.entries(), &[entry(5, 0)]);
        assert_eq!(c.latest_epoch(), Some(Epoch(5)));
    }

    #[test]
    fn each_leadership_change_starts_a_new_entry() {
        let c = cache(&[(1, 0), (2, 50), (5, 90)]);
        assert_eq!(c.entries(), &[entry(1, 0), entry(2, 50), entry(5, 90)]);
    }

    #[test]
    fn the_current_epoch_ends_at_the_log_end() {
        let c = cache(&[(1, 0), (4, 100)]);
        assert_eq!(end(&c, 4, 250), Some((4, 250)));
    }

    #[test]
    fn an_earlier_epoch_ends_where_the_next_began() {
        let c = cache(&[(1, 0), (2, 100), (3, 175)]);
        assert_eq!(end(&c, 1, 300), Some((1, 100)));
        assert_eq!(end(&c, 2, 300), Some((2, 175)));
    }

    #[test]
    fn a_skipped_epoch_is_answered_with_the_previous_known_epoch() {
        // Epoch 3 never wrote; it "ended" where epoch 5 began, answered as epoch 2.
        let c = cache(&[(1, 0), (2, 50), (5, 90)]);
        assert_eq!(end(&c, 3, 200), Some((2, 90)));
    }

    #[test]
    fn a_returning_leader_is_told_where_its_epoch_ended() {
        let new_leader = cache(&[(3, 0), (4, 60), (5, 100)]);
        assert_eq!(end(&new_leader, 4, 140), Some((4, 100)));
    }

    #[test]
    fn an_unknown_future_epoch_is_undefined_not_guessed() {
        let c = cache(&[(1, 0), (2, 100)]);
        assert_eq!(end(&c, 9, 500), None);
    }

    #[test]
    fn an_epoch_older_than_retained_history_is_undefined() {
        let mut c = cache(&[(1, 0), (5, 100), (6, 200)]);
        c.truncate_from_start(Offset(150));
        assert_eq!(end(&c, 1, 400), None);
    }

    #[test]
    fn an_empty_history_answers_undefined() {
        let c = EpochCache::new();
        assert_eq!(end(&c, 3, 100), None);
        assert_eq!(c.latest_epoch(), None);
    }

    #[test]
    fn a_new_epoch_supersedes_entries_it_overwrites() {
        let mut c = cache(&[(1, 0), (2, 100), (3, 150)]);
        c.assign(Epoch(4), Offset(100));
        assert_eq!(c.entries(), &[entry(1, 0), entry(4, 100)]);
        assert_eq!(end(&c, 1, 200), Some((1, 100)));
    }

    #[test]
    fn a_stale_epoch_is_refused() {
        let mut c = cache(&[(5, 0)]);
        assert!(!c.assign(Epoch(3), Offset(50)));
        assert_eq!(c.entries(), &[entry(5, 0)]);
    }

    #[test]
    fn truncating_the_log_end_drops_the_history_above_it() {
        let mut c = cache(&[(1, 0), (2, 100), (3, 200)]);
        assert!(c.truncate_from_end(Offset(150)));
        assert_eq!(c.entries(), &[entry(1, 0), entry(2, 100)]);
        assert_eq!(c.latest_epoch(), Some(Epoch(2)));
    }

    #[test]
    fn retention_keeps_the_epoch_covering_the_new_log_start() {
        let mut c = cache(&[(1, 0), (2, 100), (3, 200)]);
        c.truncate_from_start(Offset(150));
        assert_eq!(c.entries(), &[entry(2, 150), entry(3, 200)]);
        assert_eq!(c.epoch_for_offset(Offset(150)), Some(Epoch(2)));
    }

    #[test]
    fn offsets_map_back_to_the_epoch_in_force() {
        let c = cache(&[(1, 0), (4, 100), (7, 250)]);
        assert_eq!(c.epoch_for_offset(Offset(0)), Some(Epoch(1)));
        assert_eq!(c.epoch_for_offset(Offset(99)), Some(Epoch(1)));
        assert_eq!(c.epoch_for_offset(Offset(100)), Some(Epoch(4)));
        assert_eq!(c.epoch_for_offset(Offset(300)), Some(Epoch(7)));
    }

    #[test]
    fn pairs_round_trip() {
        let pairs = vec![(Epoch(1), Offset(0)), (Epoch(3), Offset(40))];
        assert_eq!(EpochCache::from_pairs(&pairs).to_pairs(), pairs);
    }
}
