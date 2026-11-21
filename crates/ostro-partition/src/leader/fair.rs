//! Fair selection of the next ready entry (D-W10, plan 07-05).
//!
//! Three levels, outermost first:
//!
//! 1. **Queues: weighted deficit round robin.** The queue whose turn it is may serve up to
//!    `weight` entries (quantum = [`FairQueues::set_weight`], default 1, minimum 1) before
//!    the turn passes to the next non-empty queue in name order. With every queue
//!    backlogged, queue shares are proportional to their weights (within one pick per
//!    round). A queue that is skipped ([`FairQueues::skip_queue`], for the rest of the
//!    round) or parked ([`FairQueues::park_queue`], until [`FairQueues::unpark_queues`])
//!    loses the rest of its turn.
//! 2. **Strict priority bands** within a queue: the highest band that has a servable key.
//! 3. **Round robin across concurrency keys** within a band (quantum 1). Each key is FIFO
//!    by `(ready_at, run)`. The empty key `""` is one key, so unkeyed work is one FIFO.
//!    Keys are served in rounds: every key carries a service rank (incremented when it is
//!    served; a key that becomes active starts at the band's current round), and the band
//!    serves the key with the lowest `(rank, head ready_at, head run)`. With every key
//!    backlogged that is plain round robin (a hot key gets one pick per round); with
//!    single-entry keys it is exactly the 04-14 order `(ready_at, run)`.
//!
//! A parked key ([`FairQueues::park_key`], from `Admit::KeyBlocked`) leaves its band until
//! its `until_ms` passes ([`FairQueues::unpark_due`]) or [`FairQueues::unpark_all`]; other
//! keys of the band keep being served.
//!
//! Pure: no I/O and no clock reads (time is the caller's clock milliseconds), BTreeMap /
//! BTreeSet only. Insert, remove and take are O(log n); [`FairQueues::next_candidate`] is
//! O(log n) per queue and band it looks at.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use ostro_types::RunId;

/// One ready entry as the fair selector sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FairEntry {
    pub queue: String,
    pub priority: i32,
    pub key: String,
    pub ready_at_ms: u64,
    pub run: RunId,
}

/// The entry [`FairQueues::next_candidate`] proposes; pass it to [`FairQueues::take`] once
/// admission and placement granted it.
pub type Candidate = FairEntry;

/// `(rank, head ready_at, head run, key)`: a key's position in its band.
type Slot = (u64, u64, RunId, String);

#[derive(Debug, Default)]
struct KeyState {
    /// FIFO `(ready_at, run)`.
    items: BTreeSet<(u64, RunId)>,
    /// Service rank (rounds served).
    rank: u64,
    /// The key's tuple in `Band::order` while it is active (non-empty, not parked).
    slot: Option<Slot>,
}

#[derive(Debug, Default)]
struct Band {
    keys: BTreeMap<String, KeyState>,
    /// Active keys by `(rank, head ready_at, head run)`.
    order: BTreeSet<Slot>,
    /// Rank of the key served last: a key that becomes active starts here.
    round: u64,
}

impl Band {
    /// Re-file `key` in `order` (after its items, rank or parked state changed).
    fn refile(&mut self, key: &str, parked: bool) {
        let Some(ks) = self.keys.get_mut(key) else {
            return;
        };
        if let Some(old) = ks.slot.take() {
            self.order.remove(&old);
        }
        if parked {
            return;
        }
        if let Some(&(at, run)) = ks.items.first() {
            let slot = (ks.rank, at, run, key.to_owned());
            self.order.insert(slot.clone());
            ks.slot = Some(slot);
        }
    }

    /// Activate `key` (it became non-empty or was unparked): it joins the current round.
    fn activate(&mut self, key: &str) {
        let round = self.round;
        if let Some(ks) = self.keys.get_mut(key)
            && ks.slot.is_none()
        {
            ks.rank = ks.rank.max(round);
        }
        self.refile(key, false);
    }

    /// First active key whose head is due at `now_ms`.
    fn candidate(&self, now_ms: u64) -> Option<(&str, u64, RunId)> {
        self.order
            .iter()
            .find(|(_, at, _, _)| *at <= now_ms)
            .map(|(_, at, run, key)| (key.as_str(), *at, *run))
    }
}

#[derive(Debug, Default)]
struct Queue {
    bands: BTreeMap<Reverse<i32>, Band>,
    len: usize,
    /// Every entry of the queue by `(ready_at, run)` (oldest ready time for the gauges).
    ats: BTreeSet<(u64, RunId)>,
}

/// Where a run is filed: `(queue, priority, key, ready_at)`.
type Loc = (String, i32, String, u64);

/// Weighted DRR across queues, strict priority bands, round robin across keys.
#[derive(Debug, Default)]
pub struct FairQueues {
    queues: BTreeMap<String, Queue>,
    index: BTreeMap<RunId, Loc>,
    weights: BTreeMap<String, u32>,
    /// The queue whose turn it is and the picks left in the turn.
    turn: Option<(String, u64)>,
    skipped: BTreeSet<String>,
    parked_queues: BTreeSet<String>,
    /// Parked `(queue, key) -> until_ms`.
    parked_keys: BTreeMap<(String, String), Option<u64>>,
    /// Timed parks `(until_ms, queue, key)`.
    park_until: BTreeSet<(u64, String, String)>,
}

impl FairQueues {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn contains(&self, run: &RunId) -> bool {
        self.index.contains_key(run)
    }

    /// Entries per non-empty queue.
    pub fn per_queue_counts(&self) -> BTreeMap<String, usize> {
        self.queues
            .iter()
            .map(|(q, s)| (q.clone(), s.len))
            .collect()
    }

    /// Names of the non-empty queues.
    pub fn queue_names(&self) -> Vec<String> {
        self.queues.keys().cloned().collect()
    }

    /// Oldest `ready_at_ms` per non-empty queue.
    pub fn per_queue_oldest(&self) -> BTreeMap<String, u64> {
        self.queues
            .iter()
            .filter_map(|(q, s)| s.ats.first().map(|(at, _)| (q.clone(), *at)))
            .collect()
    }

    /// Entries per non-empty queue that flow control did not hold back in the last round:
    /// not in a skipped queue (`QueueBlocked` / `Paused`) and not in a parked key
    /// (`KeyBlocked`). A placement-parked queue still counts (admissible, no capacity: the
    /// gauges' `ready_unblocked`, input of worker demand).
    pub fn per_queue_unblocked(&self) -> BTreeMap<String, usize> {
        let mut out = BTreeMap::new();
        for (q, s) in &self.queues {
            let n = if self.skipped.contains(q) {
                0
            } else {
                let lo = (q.clone(), String::new());
                let parked: usize = self
                    .parked_keys
                    .range(lo..)
                    .take_while(|((pq, _), _)| pq == q)
                    .map(|((_, k), _)| {
                        s.bands
                            .values()
                            .filter_map(|b| b.keys.get(k))
                            .map(|ks| ks.items.len())
                            .sum::<usize>()
                    })
                    .sum();
                s.len.saturating_sub(parked)
            };
            out.insert(q.clone(), n);
        }
        out
    }

    /// DRR quantum of `queue` (clamped to >= 1).
    pub fn set_weight(&mut self, queue: &str, weight: u32) {
        let w = weight.max(1);
        if w == 1 {
            self.weights.remove(queue);
        } else {
            self.weights.insert(queue.to_owned(), w);
        }
    }

    fn weight(&self, queue: &str) -> u64 {
        u64::from(self.weights.get(queue).copied().unwrap_or(1))
    }

    fn key_parked(&self, queue: &str, key: &str) -> bool {
        self.parked_keys
            .contains_key(&(queue.to_owned(), key.to_owned()))
    }

    /// File `e` (replacing an earlier entry of the same run).
    pub fn insert(&mut self, e: FairEntry) {
        self.remove(&e.run);
        let parked = self.key_parked(&e.queue, &e.key);
        let q = self.queues.entry(e.queue.clone()).or_default();
        let band = q.bands.entry(Reverse(e.priority)).or_default();
        let ks = band.keys.entry(e.key.clone()).or_default();
        ks.items.insert((e.ready_at_ms, e.run));
        q.len += 1;
        q.ats.insert((e.ready_at_ms, e.run));
        if parked {
            band.refile(&e.key, true);
        } else {
            band.activate(&e.key);
        }
        self.index
            .insert(e.run, (e.queue, e.priority, e.key, e.ready_at_ms));
    }

    /// Remove `run`; returns its entry.
    pub fn remove(&mut self, run: &RunId) -> Option<FairEntry> {
        let (queue, priority, key, at) = self.index.remove(run)?;
        let parked = self.key_parked(&queue, &key);
        if let Some(q) = self.queues.get_mut(&queue) {
            if let Some(band) = q.bands.get_mut(&Reverse(priority)) {
                if let Some(ks) = band.keys.get_mut(&key) {
                    ks.items.remove(&(at, *run));
                }
                band.refile(&key, parked);
                if band.keys.get(&key).is_some_and(|ks| ks.items.is_empty()) {
                    band.keys.remove(&key);
                }
                if band.keys.is_empty() {
                    q.bands.remove(&Reverse(priority));
                }
            }
            q.len = q.len.saturating_sub(1);
            q.ats.remove(&(at, *run));
            if q.len == 0 {
                self.queues.remove(&queue);
                // A parked key of an emptied queue has nothing left to hold back.
                self.drop_parks_of(&queue);
            }
        }
        if !self.key_has_items(&queue, &key) {
            self.unpark_key_record(&queue, &key);
        }
        Some(FairEntry {
            queue,
            priority,
            key,
            ready_at_ms: at,
            run: *run,
        })
    }

    fn key_has_items(&self, queue: &str, key: &str) -> bool {
        self.queues
            .get(queue)
            .is_some_and(|q| q.bands.values().any(|b| b.keys.contains_key(key)))
    }

    fn drop_parks_of(&mut self, queue: &str) {
        let keys: Vec<String> = self
            .parked_keys
            .range((queue.to_owned(), String::new())..)
            .take_while(|((pq, _), _)| pq == queue)
            .map(|((_, k), _)| k.clone())
            .collect();
        for k in keys {
            self.unpark_key_record(queue, &k);
        }
    }

    /// Forget a key park (no refile).
    fn unpark_key_record(&mut self, queue: &str, key: &str) {
        if let Some(Some(until)) = self.parked_keys.remove(&(queue.to_owned(), key.to_owned())) {
            self.park_until
                .remove(&(until, queue.to_owned(), key.to_owned()));
        }
    }

    /// Best entry of `queue` due at `now_ms`.
    fn queue_candidate(&self, queue: &str, now_ms: u64) -> Option<Candidate> {
        let q = self.queues.get(queue)?;
        q.bands.iter().find_map(|(Reverse(p), band)| {
            band.candidate(now_ms).map(|(key, at, run)| FairEntry {
                queue: queue.to_owned(),
                priority: *p,
                key: key.to_owned(),
                ready_at_ms: at,
                run,
            })
        })
    }

    /// Queue names in turn order: the current turn's queue first, then by name, wrapping.
    fn turn_order(&self) -> Vec<String> {
        let start = self.turn.as_ref().map(|(q, _)| q.clone());
        match start {
            Some(s) => self
                .queues
                .range::<String, _>((Bound::Included(&s), Bound::Unbounded))
                .chain(
                    self.queues
                        .range::<String, _>((Bound::Unbounded, Bound::Excluded(&s))),
                )
                .map(|(k, _)| k.clone())
                .collect(),
            None => self.queues.keys().cloned().collect(),
        }
    }

    /// The next entry to try (due at `now_ms`), or `None` once every non-empty queue is
    /// skipped, parked or has nothing servable. Does not remove it and does not move the
    /// turn: call [`Self::take`] after admission and placement, or skip / park its queue
    /// or key.
    pub fn next_candidate(&mut self, now_ms: u64) -> Option<Candidate> {
        for q in self.turn_order() {
            if self.skipped.contains(&q) || self.parked_queues.contains(&q) {
                continue;
            }
            if let Some(c) = self.queue_candidate(&q, now_ms) {
                return Some(c);
            }
        }
        None
    }

    /// Remove the candidate (it was granted) and charge its queue and key one pick.
    pub fn take(&mut self, c: &Candidate) -> Option<FairEntry> {
        let (queue, priority, key, _) = self.index.get(&c.run)?.clone();
        // Charge the key before removing the entry: the band's round moves to its rank.
        if let Some(band) = self
            .queues
            .get_mut(&queue)
            .and_then(|q| q.bands.get_mut(&Reverse(priority)))
            && let Some(ks) = band.keys.get_mut(&key)
        {
            band.round = band.round.max(ks.rank);
            ks.rank = ks.rank.saturating_add(1);
        }
        let e = self.remove(&c.run)?;
        // Served out of turn (the turn's queue was skipped or parked): the turn moves here.
        if self.turn.as_ref().is_none_or(|(t, _)| *t != queue) {
            let w = self.weight(&queue);
            self.turn = Some((queue.clone(), w));
        }
        let pass = match &mut self.turn {
            Some((_, left)) => {
                *left = left.saturating_sub(1);
                *left == 0 || !self.queues.contains_key(&queue)
            }
            None => false,
        };
        if pass {
            self.pass_turn(&queue);
        }
        Some(e)
    }

    /// Give the turn to the queue after `queue` (name order, wrapping).
    fn pass_turn(&mut self, queue: &str) {
        let next = self
            .queues
            .range::<str, _>((Bound::Excluded(queue), Bound::Unbounded))
            .next()
            .or_else(|| self.queues.iter().next())
            .map(|(k, _)| k.clone());
        self.turn = next.map(|n| {
            let w = self.weight(&n);
            (n, w)
        });
    }

    /// Start a new dispatch round: queues skipped in the last round are tried again.
    pub fn begin_round(&mut self) {
        self.skipped.clear();
    }

    /// Skip `queue` for the rest of the round (`QueueBlocked`, `Paused`). The turn only
    /// moves when another queue's entry is taken, so probing a blocked queue costs it
    /// nothing.
    pub fn skip_queue(&mut self, queue: &str) {
        self.skipped.insert(queue.to_owned());
    }

    /// Skip `queue` until [`Self::unpark_queues`] (placement had no capacity).
    pub fn park_queue(&mut self, queue: &str) {
        self.parked_queues.insert(queue.to_owned());
    }

    /// Park `key` of `queue` until `until_ms` (clock ms; `None` = until
    /// [`Self::unpark_all`]). Other keys of the queue are still served.
    pub fn park_key(&mut self, queue: &str, key: &str, until_ms: Option<u64>) {
        self.unpark_key_record(queue, key);
        self.parked_keys
            .insert((queue.to_owned(), key.to_owned()), until_ms);
        if let Some(u) = until_ms {
            self.park_until
                .insert((u, queue.to_owned(), key.to_owned()));
        }
        if let Some(q) = self.queues.get_mut(queue) {
            for band in q.bands.values_mut() {
                band.refile(key, true);
            }
        }
    }

    fn unpark_key(&mut self, queue: &str, key: &str) {
        self.unpark_key_record(queue, key);
        if let Some(q) = self.queues.get_mut(queue) {
            for band in q.bands.values_mut() {
                band.activate(key);
            }
        }
    }

    /// Unpark keys whose `until_ms` passed.
    pub fn unpark_due(&mut self, now_ms: u64) {
        while let Some((at, q, k)) = self.park_until.first().cloned() {
            if at > now_ms {
                break;
            }
            self.unpark_key(&q, &k);
        }
    }

    /// Earliest `until_ms` of a parked key.
    pub fn next_unpark_at(&self) -> Option<u64> {
        self.park_until.first().map(|(at, _, _)| *at)
    }

    /// Unpark every queue (a placement `changed()` bump).
    pub fn unpark_queues(&mut self) {
        self.parked_queues.clear();
    }

    /// Unpark every key (a flow-control `changed()` bump).
    pub fn unpark_keys(&mut self) {
        let keys: Vec<(String, String)> = self.parked_keys.keys().cloned().collect();
        for (q, k) in keys {
            self.unpark_key(&q, &k);
        }
    }

    /// Unpark every queue and key.
    pub fn unpark_all(&mut self) {
        self.unpark_queues();
        self.unpark_keys();
    }

    /// Whether any queue is parked (placement had no capacity).
    pub fn has_parked_queues(&self) -> bool {
        !self.parked_queues.is_empty()
    }

    /// Whether any queue or key is parked.
    pub fn any_parked(&self) -> bool {
        !self.parked_queues.is_empty() || !self.parked_keys.is_empty()
    }

    /// Drop every entry and park (weights stay).
    pub fn clear(&mut self) {
        let weights = std::mem::take(&mut self.weights);
        *self = Self {
            weights,
            ..Self::default()
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn rid(i: u64) -> RunId {
        let mut b = [0u8; 18];
        b[10..].copy_from_slice(&i.to_be_bytes());
        RunId::from_bytes(&b).expect("18 bytes")
    }

    fn e(i: u64, q: &str, p: i32, k: &str, at: u64) -> FairEntry {
        FairEntry {
            queue: q.into(),
            priority: p,
            key: k.into(),
            ready_at_ms: at,
            run: rid(i),
        }
    }

    /// Take candidates until none is left (nothing skipped or parked).
    fn drain(f: &mut FairQueues, now: u64) -> Vec<FairEntry> {
        let mut out = Vec::new();
        while let Some(c) = f.next_candidate(now) {
            out.push(f.take(&c).expect("candidate present"));
        }
        out
    }

    #[test]
    fn weights_share_three_to_one() {
        let mut f = FairQueues::new();
        f.set_weight("a", 3);
        for i in 0..1000 {
            f.insert(e(i, "a", 0, "", i));
            f.insert(e(10_000 + i, "b", 0, "", i));
        }
        let picks: Vec<String> = (0..400)
            .map(|_| {
                let c = f.next_candidate(u64::MAX).expect("backlogged");
                f.take(&c).expect("present").queue
            })
            .collect();
        let a = picks.iter().filter(|q| *q == "a").count();
        assert_eq!(a, 300, "3:1 over 400 picks");
        // Per round (4 picks): exactly three of a, one of b.
        for w in picks.chunks(4) {
            assert_eq!(w.iter().filter(|q| *q == "a").count(), 3, "{w:?}");
        }
    }

    /// Capacity for one pick at a time (the turn's queue is parked after every pick until
    /// capacity returns): shares still follow the weights.
    #[test]
    fn weights_hold_under_one_pick_per_round() {
        let mut f = FairQueues::new();
        f.set_weight("a", 3);
        for i in 0..200 {
            f.insert(e(i, "a", 0, "", i));
            f.insert(e(10_000 + i, "b", 0, "", i));
        }
        let mut picks = Vec::new();
        for _ in 0..200 {
            f.unpark_queues();
            f.begin_round();
            let c = f.next_candidate(u64::MAX).expect("backlogged");
            picks.push(f.take(&c).expect("present").queue);
            // No capacity left: every further candidate is parked.
            while let Some(c) = f.next_candidate(u64::MAX) {
                f.park_queue(&c.queue);
            }
        }
        let a = picks.iter().filter(|q| *q == "a").count();
        assert_eq!(a, 150, "{picks:?}");
    }

    #[test]
    fn priority_bands_are_strict() {
        let mut f = FairQueues::new();
        f.insert(e(1, "q", 0, "x", 1));
        f.insert(e(2, "q", 5, "y", 9));
        f.insert(e(3, "q", 5, "z", 8));
        f.insert(e(4, "q", -1, "", 0));
        f.insert(e(5, "q", 0, "", 2));
        let order: Vec<u64> = drain(&mut f, 100)
            .iter()
            .map(|e| u64::from(e.run.as_bytes()[17]))
            .collect();
        assert_eq!(order, vec![3, 2, 1, 5, 4]);
    }

    #[test]
    fn hot_key_does_not_starve_other_keys() {
        let mut f = FairQueues::new();
        for i in 0..1000 {
            f.insert(e(i, "q", 0, "hot", i));
        }
        for k in 0..9u64 {
            f.insert(e(5000 + k, "q", 0, &format!("k{k}"), 2000 + k));
        }
        let first: Vec<String> = (0..10)
            .map(|_| {
                let c = f.next_candidate(u64::MAX).expect("c");
                f.take(&c).expect("t").key
            })
            .collect();
        for k in 0..9 {
            assert!(first.contains(&format!("k{k}")), "{first:?}");
        }
        assert_eq!(first.iter().filter(|k| *k == "hot").count(), 1);
    }

    #[test]
    fn fifo_within_a_key() {
        let mut f = FairQueues::new();
        f.insert(e(3, "q", 0, "k", 30));
        f.insert(e(1, "q", 0, "k", 10));
        f.insert(e(4, "q", 0, "k", 10));
        f.insert(e(2, "q", 0, "k", 20));
        let order: Vec<RunId> = drain(&mut f, 100).iter().map(|e| e.run).collect();
        assert_eq!(order, vec![rid(1), rid(4), rid(2), rid(3)]);
    }

    #[test]
    fn keys_round_robin_when_backlogged() {
        let mut f = FairQueues::new();
        for i in 0..3 {
            f.insert(e(i, "q", 0, "a", i));
            f.insert(e(10 + i, "q", 0, "b", 100 + i));
        }
        let keys: Vec<String> = drain(&mut f, u64::MAX).into_iter().map(|e| e.key).collect();
        assert_eq!(keys, vec!["a", "b", "a", "b", "a", "b"]);
    }

    #[test]
    fn parked_key_is_skipped_until_due_or_unpark_all() {
        let mut f = FairQueues::new();
        f.insert(e(1, "q", 0, "a", 1));
        f.insert(e(2, "q", 0, "a", 2));
        f.insert(e(3, "q", 0, "b", 3));
        let c = f.next_candidate(10).expect("c");
        assert_eq!(c.key, "a");
        f.park_key("q", "a", Some(50));
        assert_eq!(f.next_unpark_at(), Some(50));
        assert_eq!(f.per_queue_unblocked()["q"], 1);
        let c = f.next_candidate(10).expect("b still served");
        assert_eq!(c.key, "b");
        f.take(&c);
        assert!(f.next_candidate(10).is_none(), "only parked work left");
        f.unpark_due(49);
        assert!(f.next_candidate(49).is_none());
        f.unpark_due(50);
        assert_eq!(f.next_candidate(50).map(|c| c.run), Some(rid(1)));
        // Untimed park until unpark_all; new entries of a parked key stay parked.
        f.park_key("q", "a", None);
        f.insert(e(4, "q", 0, "a", 0));
        assert!(f.next_candidate(100).is_none());
        assert!(f.any_parked());
        f.unpark_all();
        assert_eq!(f.next_candidate(100).map(|c| c.run), Some(rid(4)));
    }

    #[test]
    fn skipped_and_parked_queues() {
        let mut f = FairQueues::new();
        f.insert(e(1, "a", 0, "", 1));
        f.insert(e(2, "b", 0, "", 1));
        let c = f.next_candidate(10).expect("a");
        assert_eq!(c.queue, "a");
        f.park_queue("a");
        let c = f.next_candidate(10).expect("b");
        assert_eq!(c.queue, "b");
        f.skip_queue("b");
        assert!(f.next_candidate(10).is_none());
        // The placement-parked queue is still admissible work; the skipped one is not.
        assert_eq!(f.per_queue_unblocked()["a"], 1);
        assert_eq!(f.per_queue_unblocked()["b"], 0);
        assert!(f.has_parked_queues());
        assert_eq!(f.per_queue_oldest()["a"], 1);
        f.begin_round();
        assert_eq!(f.next_candidate(10).map(|c| c.queue), Some("b".into()));
        f.unpark_queues();
        f.begin_round();
        assert_eq!(f.len(), 2);
        assert_eq!(drain(&mut f, 10).len(), 2);
    }

    #[test]
    fn not_yet_due_entries_are_not_candidates() {
        let mut f = FairQueues::new();
        f.insert(e(1, "q", 0, "a", 100));
        f.insert(e(2, "q", 0, "b", 5));
        assert_eq!(f.next_candidate(10).map(|c| c.run), Some(rid(2)));
        f.remove(&rid(2));
        assert!(f.next_candidate(10).is_none());
        assert_eq!(f.next_candidate(100).map(|c| c.run), Some(rid(1)));
    }

    /// Unkeyed single-queue work orders exactly like 04-14: (priority desc, ready_at, run).
    #[test]
    fn unkeyed_single_queue_matches_0414_order() {
        let mut f = FairQueues::new();
        let mut want = Vec::new();
        for i in 0..200u64 {
            let p = i32::try_from(i % 7).expect("small") - 3;
            let at = (i * 37) % 50;
            f.insert(e(i, "q", p, "", at));
            want.push((Reverse(p), at, rid(i)));
        }
        want.sort();
        let got: Vec<RunId> = drain(&mut f, u64::MAX).iter().map(|e| e.run).collect();
        assert_eq!(got, want.into_iter().map(|(_, _, r)| r).collect::<Vec<_>>());
    }

    /// 04-14's dispatcher test order with distinct keys: single-entry keys are served by
    /// (priority, ready_at, run) too.
    #[test]
    fn single_entry_keys_keep_ready_order() {
        let mut f = FairQueues::new();
        f.insert(e(1, "q", 0, "k-a", 10));
        f.insert(e(2, "q", 5, "k-b", 20));
        f.insert(e(3, "q", 0, "", 5));
        let order: Vec<RunId> = drain(&mut f, 100).iter().map(|e| e.run).collect();
        assert_eq!(order, vec![rid(2), rid(3), rid(1)]);
    }

    #[test]
    fn round_robin_across_queues_with_weight_one() {
        let mut f = FairQueues::new();
        f.insert(e(1, "a", 0, "", 1));
        f.insert(e(2, "a", 0, "", 2));
        f.insert(e(3, "b", 0, "", 1));
        f.insert(e(4, "b", 0, "", 2));
        let order: Vec<RunId> = drain(&mut f, 10).iter().map(|e| e.run).collect();
        assert_eq!(order, vec![rid(1), rid(3), rid(2), rid(4)]);
    }

    #[test]
    fn insert_replaces_and_clear_keeps_weights() {
        let mut f = FairQueues::new();
        f.set_weight("a", 2);
        f.insert(e(1, "a", 0, "k", 1));
        f.insert(e(1, "b", 3, "j", 2));
        assert_eq!(f.len(), 1);
        assert_eq!(f.per_queue_counts().get("b"), Some(&1));
        assert!(!f.per_queue_counts().contains_key("a"));
        f.clear();
        assert!(f.is_empty());
        assert_eq!(f.weight("a"), 2);
        f.set_weight("a", 0);
        assert_eq!(f.weight("a"), 1, "clamped to 1");
    }

    proptest! {
        #[test]
        fn take_returns_each_entry_exactly_once(
            ops in proptest::collection::vec(
                (0u8..4, 0u64..40, 0u8..3, -2i32..3, 0u8..4, 0u64..20),
                1..200,
            ),
        ) {
            let mut f = FairQueues::new();
            let mut live: BTreeSet<RunId> = BTreeSet::new();
            let mut taken: BTreeSet<RunId> = BTreeSet::new();
            for (op, id, q, p, k, at) in ops {
                let run = rid(id);
                match op {
                    0 | 1 => {
                        f.set_weight(&format!("q{q}"), u32::from(k) + 1);
                        f.insert(e(id, &format!("q{q}"), p, &format!("k{k}"), at));
                        live.insert(run);
                        taken.remove(&run);
                    }
                    2 => {
                        prop_assert_eq!(f.remove(&run).is_some(), live.remove(&run));
                    }
                    _ => {
                        if let Some(c) = f.next_candidate(u64::MAX) {
                            prop_assert!(live.contains(&c.run));
                            let t = f.take(&c).expect("candidate present");
                            prop_assert!(taken.insert(t.run), "returned twice");
                            live.remove(&t.run);
                        }
                    }
                }
                prop_assert_eq!(f.len(), live.len());
            }
            while let Some(c) = f.next_candidate(u64::MAX) {
                let t = f.take(&c).expect("present");
                prop_assert!(live.remove(&t.run), "returned twice or unknown");
            }
            prop_assert!(live.is_empty(), "entries never returned: {}", live.len());
            prop_assert!(f.is_empty());
        }
    }
}
