//! Leader-side follower progress (D-P12-5, B5).
//!
//! Per replica the tracker keeps the **durable** LEO the replica reported (fsynced, capped at
//! its fetch offset), its last fetch offset, and when it last *caught up*. Caught-up follows
//! Kafka's `lastCaughtUpTimeMs` with one Ostro rule (B5): a fetch counts as caught up when its
//! `fetch_offset` reaches the `leader_durable_end` the leader returned in its **previous**
//! response to that replica (what the follower could have had), never the moving LEO, which a
//! follower under continuous appends can never equal at fetch time.

use ostro_types::Offset;

use crate::hw::ReplicaProgress;
use crate::mutants;
use crate::replicated::FollowerStatus;

/// One follower row of [`ReplicaTracker::status`] (the replication status endpoint row).
pub type FollowerStatusRow = FollowerStatus;

#[derive(Clone, Debug)]
struct Entry {
    replica: u64,
    durable_leo: Offset,
    fetch_offset: Offset,
    last_caught_up_ms: u64,
    last_fetch_ms: Option<u64>,
    /// `leader_durable_end` sampled for the previous response to this replica.
    prev_response_end: Option<Offset>,
}

/// Measured progress of every assigned replica, as seen by the leader.
#[derive(Clone, Debug)]
pub struct ReplicaTracker {
    self_id: u64,
    entries: Vec<Entry>,
}

impl ReplicaTracker {
    /// Tracker for a leader `self_id` serving `replicas` (self included or not; it is added).
    ///
    /// The leader's own durable LEO starts at `leader_leo`. Followers start at offset 0 and,
    /// like Kafka's new-leader reset, as caught up at `now_ms` (so a fresh leader does not
    /// shrink its ISR before followers had `replica_lag_max` to fetch). Followers that never
    /// fetched are never expanded into the ISR (see [`isr::should_expand`](super::isr::should_expand)).
    pub fn new(self_id: u64, replicas: &[u64], leader_leo: Offset, now_ms: u64) -> Self {
        let mut ids: Vec<u64> = Vec::with_capacity(replicas.len() + 1);
        if !replicas.contains(&self_id) {
            ids.push(self_id);
        }
        for r in replicas {
            if !ids.contains(r) {
                ids.push(*r);
            }
        }
        let entries = ids
            .into_iter()
            .map(|replica| {
                let is_self = replica == self_id;
                Entry {
                    replica,
                    durable_leo: if is_self { leader_leo } else { Offset::ZERO },
                    fetch_offset: if is_self { leader_leo } else { Offset::ZERO },
                    last_caught_up_ms: now_ms,
                    last_fetch_ms: is_self.then_some(now_ms),
                    prev_response_end: None,
                }
            })
            .collect();
        Self { self_id, entries }
    }

    /// The leader's node id.
    pub fn self_id(&self) -> u64 {
        self.self_id
    }

    /// Every tracked replica id, leader included.
    pub fn replicas(&self) -> Vec<u64> {
        self.entries.iter().map(|e| e.replica).collect()
    }

    fn entry(&self, replica: u64) -> Option<&Entry> {
        self.entries.iter().find(|e| e.replica == replica)
    }

    fn entry_mut(&mut self, replica: u64) -> Option<&mut Entry> {
        self.entries.iter_mut().find(|e| e.replica == replica)
    }

    /// The leader's own log became durable up to `durable_end` (WAL commit callback, F12a).
    /// Never moves backwards.
    pub fn on_local_durable(&mut self, durable_end: Offset) {
        if let Some(e) = self.entry_mut(self.self_id) {
            e.durable_leo = e.durable_leo.max(durable_end);
            e.fetch_offset = e.fetch_offset.max(durable_end);
        }
    }

    /// Record a follower `Fetch` that passed the divergence check (D-P12-5).
    ///
    /// - `durable_leo` counts only up to `fetch_offset` (a follower cannot have fsynced
    ///   what it has not got) and never decreases (see [`on_truncate`](Self::on_truncate)).
    /// - Caught up when `fetch_offset >=` the `leader_durable_end` sampled for the
    ///   **previous** response to this replica (B5), or `>=` the current one.
    /// - `leader_durable_end_now` is the leader durable end sampled **before** the read that
    ///   serves this response (W3): appends landing between that snapshot and the read do not
    ///   move the follower's next target.
    ///
    /// Unknown replicas are ignored.
    pub fn on_fetch(
        &mut self,
        replica: u64,
        fetch_offset: Offset,
        durable_leo: Offset,
        leader_durable_end_now: Offset,
        now_ms: u64,
    ) {
        if replica == self.self_id {
            return;
        }
        let Some(e) = self.entry_mut(replica) else {
            return;
        };
        let reported = if super::mutant(mutants::COUNT_NONDURABLE_LEO) {
            fetch_offset
        } else {
            durable_leo.min(fetch_offset)
        };
        e.durable_leo = e.durable_leo.max(reported);
        e.fetch_offset = fetch_offset;
        let caught_up = e.prev_response_end.is_some_and(|prev| fetch_offset >= prev)
            || fetch_offset >= leader_durable_end_now;
        if caught_up {
            e.last_caught_up_ms = e.last_caught_up_ms.max(now_ms);
        }
        e.prev_response_end = Some(leader_durable_end_now);
        e.last_fetch_ms = Some(now_ms);
    }

    /// The follower was told to truncate (diverging answer): forget its durable progress and
    /// response target; the next accepted fetch re-establishes them.
    pub fn on_truncate(&mut self, replica: u64) {
        if replica == self.self_id {
            return;
        }
        if let Some(e) = self.entry_mut(replica) {
            e.durable_leo = Offset::ZERO;
            e.fetch_offset = Offset::ZERO;
            e.prev_response_end = None;
        }
    }

    /// One [`ReplicaProgress`] per replica (`leo` = durable LEO). `in_isr` is `false` here;
    /// [`watermark::next_hw`](super::watermark::next_hw) sets it from the ISR state.
    pub fn progress(&self) -> Vec<ReplicaProgress> {
        self.entries
            .iter()
            .map(|e| ReplicaProgress {
                node: e.replica,
                leo: e.durable_leo,
                in_isr: false,
            })
            .collect()
    }

    /// The durable LEO counted for `replica`.
    pub fn durable_leo(&self, replica: u64) -> Option<Offset> {
        self.entry(replica).map(|e| e.durable_leo)
    }

    /// The last fetch offset of `replica`.
    pub fn fetch_offset(&self, replica: u64) -> Option<Offset> {
        self.entry(replica).map(|e| e.fetch_offset)
    }

    /// When `replica` last fetched, if it ever did (the leader counts as fetched).
    pub fn last_fetch_ms(&self, replica: u64) -> Option<u64> {
        self.entry(replica).and_then(|e| e.last_fetch_ms)
    }

    /// Time since `replica` last caught up (`0` for the leader itself).
    pub fn lag_ms(&self, replica: u64, now_ms: u64) -> Option<u64> {
        if replica == self.self_id {
            return self.entry(replica).map(|_| 0);
        }
        self.entry(replica)
            .map(|e| now_ms.saturating_sub(e.last_caught_up_ms))
    }

    /// Follower rows (leader excluded), `in_sync = fetched && lag <= lag_max_ms` (measured,
    /// not the published ISR). A replica that never fetched is not in sync: its join grace
    /// only delays a shrink, it is not progress (12-16; the status row used to say
    /// `in_sync: true` for a node that never ran).
    pub fn status(&self, now_ms: u64, lag_max_ms: u64) -> Vec<FollowerStatusRow> {
        self.entries
            .iter()
            .filter(|e| e.replica != self.self_id)
            .map(|e| FollowerStatus {
                replica: e.replica,
                durable_leo: e.durable_leo,
                fetch_offset: e.fetch_offset,
                last_caught_up_ms: e.last_caught_up_ms,
                in_sync: e.last_fetch_ms.is_some()
                    && now_ms.saturating_sub(e.last_caught_up_ms) <= lag_max_ms,
            })
            .collect()
    }
}
