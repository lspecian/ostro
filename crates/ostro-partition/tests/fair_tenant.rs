//! Tenant level of the fair scheduler (plan 14-06, D-P14-14, DISP-06): pure model tests.
//!
//! - With one tenant the dispatch sequence equals the Phase 7 selector for random
//!   backlogs and random flow-control answers (skip / park / gate / hold of the proposed
//!   candidate, as the dispatcher answers; unpark, ungate and release calls at any time). The Phase 7 implementation is kept below
//!   verbatim as the reference ([`phase7`], `leader/fair.rs` before 14-06).
//! - With weights `w_i` and saturated backlogs, each tenant's dispatched count after any
//!   number of picks is within one quantum of its weighted share.
//! - Weights are clamped to `1..=10_000` (W16): weight 0 and absurd values never stall or
//!   starve the ring.
//! - A blocked tenant is never dispatched while blocked and resumes after a release, its
//!   `until` or a `changed()` bump; a weight-1 tenant is never starved (10,000 rounds).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;

use ostro_partition::leader::fair::{
    FairEntry, FairQueues, TENANT_WEIGHT_MAX, TenantPolicy, tenant_key,
};
use ostro_types::RunId;
use proptest::prelude::*;

/// The Phase 7 selector (`crates/ostro-partition/src/leader/fair.rs` at the 14-06 base,
/// code only, unchanged): the single-tenant reference.
#[allow(dead_code, clippy::all)]
mod phase7 {

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
        /// F7: the run's first activation (throttles apply only to these, D-W13).
        pub first_activation: bool,
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
        /// The re-activations among `items` (`first_activation == false`).
        reacts: BTreeSet<(u64, RunId)>,
        /// Service rank (rounds served).
        rank: u64,
        /// The key's tuple in `Band::order` while it is active (not parked, and non-empty,
        /// counting only re-activations while the key is gated).
        slot: Option<Slot>,
        /// The key's tuple in `Band::react_order` (head of `reacts`) while not parked.
        rslot: Option<Slot>,
        /// First activations of this key are gated ([`FairQueues::gate_first_key`]).
        gated: bool,
    }

    impl KeyState {
        fn firsts(&self) -> usize {
            self.items.len() - self.reacts.len()
        }
    }

    #[derive(Debug, Default)]
    struct Band {
        keys: BTreeMap<String, KeyState>,
        /// Active keys by `(rank, head ready_at, head run)`.
        order: BTreeSet<Slot>,
        /// Keys with re-activations by `(rank, head re-activation ready_at, run)`: the order
        /// used while the queue's first activations are gated.
        react_order: BTreeSet<Slot>,
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
            if let Some(old) = ks.rslot.take() {
                self.react_order.remove(&old);
            }
            if parked {
                return;
            }
            let head = if ks.gated {
                ks.reacts.first()
            } else {
                ks.items.first()
            };
            if let Some(&(at, run)) = head {
                let slot = (ks.rank, at, run, key.to_owned());
                self.order.insert(slot.clone());
                ks.slot = Some(slot);
            }
            if let Some(&(at, run)) = ks.reacts.first() {
                let slot = (ks.rank, at, run, key.to_owned());
                self.react_order.insert(slot.clone());
                ks.rslot = Some(slot);
            }
        }

        /// Activate `key` (it became non-empty or was unparked): it joins the current round.
        fn activate(&mut self, key: &str) {
            let round = self.round;
            if let Some(ks) = self.keys.get_mut(key)
                && ks.slot.is_none()
                && ks.rslot.is_none()
            {
                ks.rank = ks.rank.max(round);
            }
            self.refile(key, false);
        }

        /// First active key whose head is due at `now_ms` (re-activations only while the
        /// queue's first activations are gated).
        fn candidate(&self, now_ms: u64, first_gated: bool) -> Option<(&str, u64, RunId)> {
            let order = if first_gated {
                &self.react_order
            } else {
                &self.order
            };
            order
                .iter()
                .find(|(_, at, _, _)| *at <= now_ms)
                .map(|(_, at, run, key)| (key.as_str(), *at, *run))
        }
    }

    #[derive(Debug, Default)]
    struct Queue {
        bands: BTreeMap<Reverse<i32>, Band>,
        len: usize,
        /// First activations among them.
        firsts: usize,
        /// Every entry of the queue by `(ready_at, run)` (oldest ready time for the gauges).
        ats: BTreeSet<(u64, RunId)>,
    }

    /// Where a run is filed: `(queue, priority, key, ready_at, first_activation)`.
    type Loc = (String, i32, String, u64, bool);

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
        /// Entries held out of the selection (workflow paused), by run.
        held: BTreeMap<RunId, FairEntry>,
        /// Held entries per queue by `(ready_at, run)`.
        held_q: BTreeMap<String, BTreeSet<(u64, RunId)>>,
        /// Queues whose first activations are gated -> `until_ms`.
        gated_queues: BTreeMap<String, u64>,
        /// Keys whose first activations are gated -> `until_ms`.
        gated_keys: BTreeMap<(String, String), u64>,
        /// Gate deadlines `(until_ms, queue, key)`; `None` = the queue gate.
        gate_until: BTreeSet<(u64, String, Option<String>)>,
    }

    impl FairQueues {
        pub fn new() -> Self {
            Self::default()
        }

        /// Number of entries.
        pub fn len(&self) -> usize {
            self.index.len() + self.held.len()
        }

        pub fn is_empty(&self) -> bool {
            self.index.is_empty() && self.held.is_empty()
        }

        pub fn contains(&self, run: &RunId) -> bool {
            self.index.contains_key(run) || self.held.contains_key(run)
        }

        /// Entries per non-empty queue (held entries included).
        pub fn per_queue_counts(&self) -> BTreeMap<String, usize> {
            let mut out: BTreeMap<String, usize> = self
                .queues
                .iter()
                .map(|(q, s)| (q.clone(), s.len))
                .collect();
            for (q, h) in &self.held_q {
                *out.entry(q.clone()).or_default() += h.len();
            }
            out
        }

        /// Names of the non-empty queues.
        pub fn queue_names(&self) -> Vec<String> {
            self.queues.keys().cloned().collect()
        }

        /// Oldest `ready_at_ms` per non-empty queue (held entries included).
        pub fn per_queue_oldest(&self) -> BTreeMap<String, u64> {
            let mut out: BTreeMap<String, u64> = self
                .queues
                .iter()
                .filter_map(|(q, s)| s.ats.first().map(|(at, _)| (q.clone(), *at)))
                .collect();
            for (q, h) in &self.held_q {
                if let Some(&(at, _)) = h.first() {
                    out.entry(q.clone())
                        .and_modify(|o| *o = (*o).min(at))
                        .or_insert(at);
                }
            }
            out
        }

        /// Entries per non-empty queue that flow control did not hold back in the last round:
        /// not in a skipped queue (`QueueBlocked` / queue `Paused`), not in a parked key
        /// (`KeyBlocked`) and not held (workflow `Paused`). A placement-parked queue still
        /// counts (admissible, no capacity: the gauges' `ready_unblocked`, input of worker
        /// demand).
        pub fn per_queue_unblocked(&self) -> BTreeMap<String, usize> {
            let mut out = BTreeMap::new();
            for (q, s) in &self.queues {
                let n = if self.skipped.contains(q) {
                    0
                } else {
                    // (items, first activations) of `k` across the queue's bands.
                    let count = |k: &str| {
                        s.bands
                            .values()
                            .filter_map(|b| b.keys.get(k))
                            .fold((0, 0), |(n, f), ks| (n + ks.items.len(), f + ks.firsts()))
                    };
                    let lo = (q.clone(), String::new());
                    let (mut parked, mut parked_firsts) = (0usize, 0usize);
                    for ((_, k), _) in self
                        .parked_keys
                        .range(lo.clone()..)
                        .take_while(|((pq, _), _)| pq == q)
                    {
                        let (n, f) = count(k);
                        parked += n;
                        parked_firsts += f;
                    }
                    let gated = if self.gated_queues.contains_key(q) {
                        s.firsts.saturating_sub(parked_firsts)
                    } else {
                        self.gated_keys
                            .range(lo..)
                            .take_while(|((gq, _), _)| gq == q)
                            .filter(|((_, k), _)| !self.key_parked(q, k))
                            .map(|((_, k), _)| count(k).1)
                            .sum()
                    };
                    s.len.saturating_sub(parked + gated)
                };
                out.insert(q.clone(), n);
            }
            for q in self.held_q.keys() {
                out.entry(q.clone()).or_insert(0);
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
            let gated = self
                .gated_keys
                .contains_key(&(e.queue.clone(), e.key.clone()));
            let q = self.queues.entry(e.queue.clone()).or_default();
            let band = q.bands.entry(Reverse(e.priority)).or_default();
            let ks = band.keys.entry(e.key.clone()).or_default();
            ks.gated = gated;
            ks.items.insert((e.ready_at_ms, e.run));
            if e.first_activation {
                q.firsts += 1;
            } else {
                ks.reacts.insert((e.ready_at_ms, e.run));
            }
            q.len += 1;
            q.ats.insert((e.ready_at_ms, e.run));
            if parked {
                band.refile(&e.key, true);
            } else {
                band.activate(&e.key);
            }
            self.index.insert(
                e.run,
                (
                    e.queue,
                    e.priority,
                    e.key,
                    e.ready_at_ms,
                    e.first_activation,
                ),
            );
        }

        /// Remove `run` (queued or held); returns its entry.
        pub fn remove(&mut self, run: &RunId) -> Option<FairEntry> {
            if let Some(e) = self.held.remove(run) {
                if let Some(h) = self.held_q.get_mut(&e.queue) {
                    h.remove(&(e.ready_at_ms, *run));
                    if h.is_empty() {
                        self.held_q.remove(&e.queue);
                    }
                }
                return Some(e);
            }
            let (queue, priority, key, at, first) = self.index.remove(run)?;
            let parked = self.key_parked(&queue, &key);
            if let Some(q) = self.queues.get_mut(&queue) {
                if let Some(band) = q.bands.get_mut(&Reverse(priority)) {
                    if let Some(ks) = band.keys.get_mut(&key) {
                        ks.items.remove(&(at, *run));
                        ks.reacts.remove(&(at, *run));
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
                if first {
                    q.firsts = q.firsts.saturating_sub(1);
                }
                q.ats.remove(&(at, *run));
                if q.len == 0 {
                    self.queues.remove(&queue);
                    // A parked key or gate of an emptied queue has nothing left to hold back.
                    self.drop_parks_of(&queue);
                    self.ungate_queue(&queue);
                }
            }
            if !self.key_has_items(&queue, &key) {
                self.unpark_key_record(&queue, &key);
                self.ungate_key(&queue, &key);
            }
            Some(FairEntry {
                queue,
                priority,
                key,
                ready_at_ms: at,
                run: *run,
                first_activation: first,
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
            let gated: Vec<String> = self
                .gated_keys
                .range((queue.to_owned(), String::new())..)
                .take_while(|((gq, _), _)| gq == queue)
                .map(|((_, k), _)| k.clone())
                .collect();
            for k in gated {
                self.ungate_key(queue, &k);
            }
        }

        /// Forget a key park (no refile).
        fn unpark_key_record(&mut self, queue: &str, key: &str) {
            if let Some(Some(until)) = self.parked_keys.remove(&(queue.to_owned(), key.to_owned()))
            {
                self.park_until
                    .remove(&(until, queue.to_owned(), key.to_owned()));
            }
        }

        /// Best entry of `queue` due at `now_ms`.
        fn queue_candidate(&self, queue: &str, now_ms: u64) -> Option<Candidate> {
            let q = self.queues.get(queue)?;
            let gated = self.gated_queues.contains_key(queue);
            q.bands.iter().find_map(|(Reverse(p), band)| {
                band.candidate(now_ms, gated)
                    .map(|(key, at, run)| FairEntry {
                        queue: queue.to_owned(),
                        priority: *p,
                        key: key.to_owned(),
                        ready_at_ms: at,
                        run,
                        first_activation: !band
                            .keys
                            .get(key)
                            .is_some_and(|ks| ks.reacts.contains(&(at, run))),
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
            let (queue, priority, key, _, _) = self.index.get(&c.run)?.clone();
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

        /// Hold the candidate out of the selection until [`Self::release_held`] (its workflow
        /// is paused). The rest of its key, band and queue stays servable; neither the key nor
        /// the queue's turn is charged.
        pub fn hold(&mut self, c: &Candidate) {
            if self.held.contains_key(&c.run) {
                return;
            }
            if let Some(e) = self.remove(&c.run) {
                self.held_q
                    .entry(e.queue.clone())
                    .or_default()
                    .insert((e.ready_at_ms, e.run));
                self.held.insert(e.run, e);
            }
        }

        /// Put every held entry back into the selection (a flow-control `changed()` bump).
        pub fn release_held(&mut self) {
            let held = std::mem::take(&mut self.held);
            self.held_q.clear();
            for e in held.into_values() {
                self.insert(e);
            }
        }

        /// Whether any entry is held.
        pub fn has_held(&self) -> bool {
            !self.held.is_empty()
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
            while let Some((at, q, k)) = self.gate_until.first().cloned() {
                if at > now_ms {
                    break;
                }
                match k {
                    None => self.ungate_queue(&q),
                    Some(k) => self.ungate_key(&q, &k),
                }
            }
        }

        /// Earliest `until_ms` of a parked key or a first-activation gate.
        pub fn next_unpark_at(&self) -> Option<u64> {
            let park = self.park_until.first().map(|(at, _, _)| *at);
            let gate = self.gate_until.first().map(|(at, _, _)| *at);
            match (park, gate) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            }
        }

        /// Hold back the first activations of `queue` until `until_ms` (clock ms) or
        /// [`Self::ungate_all`] (a throttle blocked a first activation, D-W13). Its
        /// re-activations stay servable in the same round; nothing is charged. A later gate
        /// replaces the deadline.
        pub fn gate_first(&mut self, queue: &str, until_ms: u64) {
            self.ungate_queue(queue);
            if !self.queues.contains_key(queue) {
                return;
            }
            self.gated_queues.insert(queue.to_owned(), until_ms);
            self.gate_until.insert((until_ms, queue.to_owned(), None));
        }

        /// Hold back the first activations of `key` of `queue` until `until_ms` or
        /// [`Self::ungate_all`] (a key throttle blocked a first activation, D-W13). The key's
        /// re-activations and every other key stay servable.
        pub fn gate_first_key(&mut self, queue: &str, key: &str, until_ms: u64) {
            let k = (queue.to_owned(), key.to_owned());
            if let Some(old) = self.gated_keys.remove(&k) {
                self.gate_until
                    .remove(&(old, queue.to_owned(), Some(key.to_owned())));
            }
            if !self.key_has_items(queue, key) {
                return;
            }
            self.gated_keys.insert(k, until_ms);
            self.gate_until
                .insert((until_ms, queue.to_owned(), Some(key.to_owned())));
            self.set_key_gated(queue, key, true);
        }

        fn set_key_gated(&mut self, queue: &str, key: &str, gated: bool) {
            let parked = self.key_parked(queue, key);
            if let Some(q) = self.queues.get_mut(queue) {
                for band in q.bands.values_mut() {
                    let Some(ks) = band.keys.get_mut(key) else {
                        continue;
                    };
                    ks.gated = gated;
                    if parked {
                        band.refile(key, true);
                    } else {
                        band.activate(key);
                    }
                }
            }
        }

        fn ungate_queue(&mut self, queue: &str) {
            if let Some(until) = self.gated_queues.remove(queue) {
                self.gate_until.remove(&(until, queue.to_owned(), None));
            }
        }

        fn ungate_key(&mut self, queue: &str, key: &str) {
            if let Some(until) = self.gated_keys.remove(&(queue.to_owned(), key.to_owned())) {
                self.gate_until
                    .remove(&(until, queue.to_owned(), Some(key.to_owned())));
                self.set_key_gated(queue, key, false);
            }
        }

        /// Lift every first-activation gate (a flow-control `changed()` bump).
        pub fn ungate_all(&mut self) {
            self.gated_queues.clear();
            let keys: Vec<(String, String)> = self.gated_keys.keys().cloned().collect();
            for (q, k) in keys {
                self.ungate_key(&q, &k);
            }
            self.gate_until.clear();
        }

        /// Whether `queue`'s first activations are gated.
        pub fn is_first_gated(&self, queue: &str) -> bool {
            self.gated_queues.contains_key(queue)
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

        /// Unpark every queue and key, lift every gate and release every held entry.
        pub fn unpark_all(&mut self) {
            self.unpark_queues();
            self.unpark_keys();
            self.ungate_all();
            self.release_held();
        }

        /// Whether any queue is parked (placement had no capacity).
        pub fn has_parked_queues(&self) -> bool {
            !self.parked_queues.is_empty()
        }

        /// Whether any queue or key is parked.
        pub fn any_parked(&self) -> bool {
            !self.parked_queues.is_empty() || !self.parked_keys.is_empty()
        }

        /// Drop every entry (held ones too) and park (weights stay).
        pub fn clear(&mut self) {
            let weights = std::mem::take(&mut self.weights);
            *self = Self {
                weights,
                ..Self::default()
            };
        }
    }
}

fn rid(i: u64) -> RunId {
    let mut b = [0u8; 18];
    b[10..].copy_from_slice(&i.to_be_bytes());
    RunId::from_bytes(&b).expect("18 bytes")
}

fn e(i: u64, q: &str, p: i32, k: &str, at: u64, first: bool) -> FairEntry {
    FairEntry {
        queue: q.into(),
        priority: p,
        key: k.into(),
        ready_at_ms: at,
        run: rid(i),
        first_activation: first,
    }
}

fn to_ref(x: &FairEntry) -> phase7::FairEntry {
    phase7::FairEntry {
        queue: x.queue.clone(),
        priority: x.priority,
        key: x.key.clone(),
        ready_at_ms: x.ready_at_ms,
        run: x.run,
        first_activation: x.first_activation,
    }
}

fn from_ref(x: &phase7::FairEntry) -> FairEntry {
    FairEntry {
        queue: x.queue.clone(),
        priority: x.priority,
        key: x.key.clone(),
        ready_at_ms: x.ready_at_ms,
        run: x.run,
        first_activation: x.first_activation,
    }
}

/// One random operation of the single-tenant comparison.
#[derive(Clone, Debug)]
enum Op {
    Insert {
        id: u64,
        q: u8,
        p: i32,
        k: u8,
        at: u64,
        first: bool,
    },
    Remove(u64),
    /// Ask for the next candidate at `now` and answer it as the dispatcher does: take it
    /// (granted), hold it (workflow paused), skip or park its queue, park its key, or
    /// gate its queue's / key's first activations (`until` for timed answers).
    Next {
        now: u64,
        answer: u8,
        until: u64,
    },
    UnparkDue(u64),
    BeginRound,
    ReleaseHeld,
    UngateAll,
    UnparkQueues,
    UnparkKeys,
    UnparkAll,
    Weight {
        q: u8,
        w: u32,
    },
    Clear,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (0u64..60, 0u8..3, -1i32..2, 0u8..4, 0u64..30, any::<bool>()).prop_map(
            |(id, q, p, k, at, first)| Op::Insert { id, q, p, k, at, first }
        ),
        1 => (0u64..60).prop_map(Op::Remove),
        12 => (0u64..40, 0u8..14, 1u64..40)
            .prop_map(|(now, answer, until)| Op::Next { now, answer, until }),
        1 => (0u64..40).prop_map(Op::UnparkDue),
        2 => Just(Op::BeginRound),
        1 => Just(Op::ReleaseHeld),
        1 => Just(Op::UngateAll),
        1 => Just(Op::UnparkQueues),
        1 => Just(Op::UnparkKeys),
        1 => Just(Op::UnparkAll),
        1 => (0u8..3, 0u32..5).prop_map(|(q, w)| Op::Weight { q, w }),
        1 => Just(Op::Clear),
    ]
}

fn qn(q: u8) -> String {
    format!("q{q}")
}

fn kn(k: u8) -> String {
    if k == 0 {
        String::new()
    } else {
        format!("k{k}")
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// One tenant (the default tenant or a named one): every observable of the new
    /// scheduler equals the Phase 7 selector after every operation.
    #[test]
    fn one_tenant_matches_phase7(
        named in any::<bool>(),
        ops in proptest::collection::vec(op(), 1..250),
    ) {
        let tenant = if named { "acme" } else { "" };
        let mut new = FairQueues::new();
        let mut old = phase7::FairQueues::new();
        for o in ops {
            match o {
                Op::Insert { id, q, p, k, at, first } => {
                    let x = e(id, &qn(q), p, &kn(k), at, first);
                    old.insert(to_ref(&x));
                    new.insert_in(tenant, x);
                }
                Op::Remove(id) => {
                    let a = old.remove(&rid(id)).map(|x| from_ref(&x));
                    prop_assert_eq!(new.remove(&rid(id)), a);
                }
                Op::Next { now, answer, until } => {
                    let a = old.next_candidate(now).map(|x| from_ref(&x));
                    let b = new.next_candidate(now);
                    prop_assert_eq!(&b, &a);
                    if let Some(c) = b {
                        let (q, k) = (c.queue.clone(), c.key.clone());
                        match answer {
                            0 => { old.hold(&to_ref(&c)); new.hold(&c); }
                            1 => { old.skip_queue(&q); new.skip_queue(&q); }
                            2 => { old.park_queue(&q); new.park_queue(&q); }
                            3 => { old.park_key(&q, &k, None); new.park_key(&q, &k, None); }
                            4 => {
                                old.park_key(&q, &k, Some(until));
                                new.park_key(&q, &k, Some(until));
                            }
                            5 => { old.gate_first(&q, until); new.gate_first(&q, until); }
                            6 => {
                                old.gate_first_key(&q, &k, until);
                                new.gate_first_key(&q, &k, until);
                            }
                            _ => {
                                let ta = old.take(&to_ref(&c)).map(|x| from_ref(&x));
                                prop_assert_eq!(new.take(&c), ta);
                            }
                        }
                    }
                }
                Op::UnparkDue(now) => { old.unpark_due(now); new.unpark_due(now); }
                Op::BeginRound => { old.begin_round(); new.begin_round(); }
                Op::ReleaseHeld => { old.release_held(); new.release_held(); }
                Op::UngateAll => { old.ungate_all(); new.ungate_all(); }
                Op::UnparkQueues => { old.unpark_queues(); new.unpark_queues(); }
                Op::UnparkKeys => { old.unpark_keys(); new.unpark_keys(); }
                Op::UnparkAll => { old.unpark_all(); new.unpark_all(); }
                Op::Weight { q, w } => { old.set_weight(&qn(q), w); new.set_weight(&qn(q), w); }
                Op::Clear => { old.clear(); new.clear(); }
            }
            prop_assert_eq!(new.len(), old.len());
            prop_assert_eq!(new.per_queue_counts(), old.per_queue_counts());
            prop_assert_eq!(new.per_queue_oldest(), old.per_queue_oldest());
            prop_assert_eq!(new.per_queue_unblocked(), old.per_queue_unblocked());
            prop_assert_eq!(new.queue_names(), old.queue_names());
            prop_assert_eq!(new.next_unpark_at(), old.next_unpark_at());
            prop_assert_eq!(new.has_held(), old.has_held());
            prop_assert_eq!(new.has_parked_queues(), old.has_parked_queues());
        }
        // Drain both: the remaining order is identical too.
        old.unpark_all();
        new.unpark_all();
        loop {
            old.begin_round();
            new.begin_round();
            let a = old.next_candidate(u64::MAX).map(|x| from_ref(&x));
            let b = new.next_candidate(u64::MAX);
            prop_assert_eq!(&b, &a);
            let Some(c) = b else { break };
            let ta = old.take(&to_ref(&c)).map(|x| from_ref(&x));
            prop_assert_eq!(new.take(&c), ta);
        }
        prop_assert!(new.is_empty());
    }

    /// Saturated backlogs, weights `w_i`: after any number of picks each tenant's count is
    /// within one quantum of its weighted share.
    #[test]
    fn weighted_shares_within_one_quantum(
        weights in proptest::collection::vec(1u32..9, 2..6),
        picks in 1usize..400,
    ) {
        let mut f = FairQueues::new();
        let tenants: Vec<String> = (0..weights.len()).map(|i| format!("t{i}")).collect();
        let per = 500u64;
        let mut id = 0;
        for t in &tenants {
            for _ in 0..per {
                f.insert_in(t, e(id, "default", 0, "", 0, true));
                id += 1;
            }
        }
        let w: BTreeMap<String, u32> =
            tenants.iter().cloned().zip(weights.iter().copied()).collect();
        f.refresh_tenant_weights(&|t: &str| w.get(t).copied().unwrap_or(1));
        let mut got: BTreeMap<String, u64> = BTreeMap::new();
        for _ in 0..picks {
            let c = f.next_candidate(u64::MAX).expect("saturated");
            let t = f.tenant_of(&c.run).expect("filed").to_owned();
            f.take(&c).expect("taken");
            *got.entry(t).or_default() += 1;
        }
        let total: u64 = weights.iter().map(|&x| u64::from(x)).sum();
        for (t, wi) in &w {
            let share = picks as f64 * f64::from(*wi) / total as f64;
            let n = got.get(t).copied().unwrap_or(0) as f64;
            prop_assert!(
                (n - share).abs() <= f64::from(*wi),
                "tenant {} weight {}: {} picks vs share {:.1} ({:?})", t, wi, n, share, got
            );
        }
    }
}

/// Take `n` picks; returns the tenant of each.
fn picks(f: &mut FairQueues, n: usize, now: u64) -> Vec<String> {
    let mut out = Vec::new();
    for _ in 0..n {
        let Some(c) = f.next_candidate(now) else {
            break;
        };
        out.push(f.tenant_of(&c.run).expect("filed").to_owned());
        f.take(&c).expect("taken");
    }
    out
}

fn backlog(f: &mut FairQueues, tenant: &str, from: u64, n: u64) {
    for i in from..from + n {
        f.insert_in(tenant, e(i, "default", 0, "", 0, true));
    }
}

#[test]
fn weights_one_two_four_interleave_exactly() {
    let mut f = FairQueues::new();
    backlog(&mut f, "a", 0, 100);
    backlog(&mut f, "b", 1000, 100);
    backlog(&mut f, "c", 2000, 100);
    let w = |t: &str| match t {
        "a" => 1,
        "b" => 2,
        _ => 4,
    };
    f.refresh_tenant_weights(&w);
    let order = picks(&mut f, 14, u64::MAX);
    assert_eq!(
        order,
        [
            "a", "b", "b", "c", "c", "c", "c", "a", "b", "b", "c", "c", "c", "c"
        ],
    );
}

#[test]
fn weight_zero_and_absurd_weights_are_clamped() {
    let mut f = FairQueues::new();
    f.set_tenant_weight("zero", 0);
    assert_eq!(f.tenant_weight("zero"), 1, "0 clamps to 1");
    f.set_tenant_weight("huge", u32::MAX);
    assert_eq!(f.tenant_weight("huge"), u64::from(TENANT_WEIGHT_MAX));
    // A zero-weight tenant is still served (never stalls the ring); the huge one gets at
    // most 10,000 picks per turn, so the small one is served within 10,001 picks.
    backlog(&mut f, "huge", 0, 20_000);
    backlog(&mut f, "zero", 100_000, 5);
    let order = picks(&mut f, 10_001, u64::MAX);
    assert!(order.iter().any(|t| t == "zero"), "weight-0 tenant starved");
    assert!(
        order.iter().filter(|t| *t == "huge").count() <= 10_000,
        "huge weight not clamped"
    );
}

#[test]
fn blocked_tenant_is_not_dispatched_until_released() {
    let mut f = FairQueues::new();
    backlog(&mut f, "a", 0, 10);
    backlog(&mut f, "b", 100, 10);
    // Block a with no deadline (suspended / concurrency).
    f.block_tenant("a", None);
    assert!(f.is_tenant_blocked("a"));
    let order = picks(&mut f, 5, u64::MAX);
    assert!(order.iter().all(|t| t == "b"), "{order:?}");
    // A blocked tenant's entries are held back from the gauges' unblocked count.
    assert_eq!(f.per_queue_unblocked().get("default"), Some(&5));
    // A changed() bump (release_held) puts it back.
    f.release_held();
    let order = picks(&mut f, 2, u64::MAX);
    assert_eq!(order, ["a", "b"]);
    // Explicit release.
    f.block_tenant("b", None);
    assert_eq!(picks(&mut f, 1, u64::MAX), ["a"]);
    f.release_tenant("b");
    assert!(picks(&mut f, 4, u64::MAX).contains(&"b".to_owned()));
}

#[test]
fn timed_block_resumes_at_until() {
    let mut f = FairQueues::new();
    backlog(&mut f, "a", 0, 3);
    f.block_tenant("a", Some(50));
    assert_eq!(f.next_unpark_at(), Some(50));
    assert_eq!(f.next_candidate(10), None, "blocked until 50");
    f.unpark_due(49);
    assert_eq!(f.next_candidate(49), None);
    f.unpark_due(50);
    assert!(!f.is_tenant_blocked("a"));
    assert_eq!(f.next_unpark_at(), None);
    assert_eq!(picks(&mut f, 3, 50), ["a", "a", "a"]);
    // Blocking an empty tenant is a no-op (nothing to hold back).
    f.block_tenant("a", Some(99));
    assert!(!f.is_tenant_blocked("a"));
}

#[test]
fn weight_one_tenant_is_never_starved() {
    let mut f = FairQueues::new();
    let rounds = 10_000u64;
    let w = |t: &str| match t {
        "big" => 5,
        "mid" => 3,
        _ => 1,
    };
    backlog(&mut f, "big", 0, 5 * rounds);
    backlog(&mut f, "mid", 1_000_000, 3 * rounds);
    backlog(&mut f, "small", 2_000_000, rounds);
    f.refresh_tenant_weights(&w);
    let order = picks(&mut f, usize::try_from(9 * rounds).expect("fits"), u64::MAX);
    let mut gap = 0usize;
    let mut max_gap = 0usize;
    let mut small = 0u64;
    for t in &order {
        if t == "small" {
            small += 1;
            gap = 0;
        } else {
            gap += 1;
            max_gap = max_gap.max(gap);
        }
    }
    assert_eq!(small, rounds, "one pick per round for the weight-1 tenant");
    assert!(max_gap <= 8, "weight-1 tenant waited {max_gap} picks");
}

#[test]
fn queue_weights_still_apply_inside_a_tenant() {
    let mut f = FairQueues::new();
    for i in 0..40 {
        f.insert_in("a", e(i, "qa", 0, "", 0, true));
        f.insert_in("a", e(100 + i, "qb", 0, "", 0, true));
        f.insert_in("b", e(200 + i, "qc", 0, "", 0, true));
    }
    f.set_weight("qa", 3);
    let mut order = Vec::new();
    for _ in 0..16 {
        let c = f.next_candidate(u64::MAX).expect("backlogged");
        order.push(c.queue.clone());
        f.take(&c).expect("taken");
    }
    // Tenants alternate (weight 1 each); inside a, qa:qb = 3:1.
    let a: Vec<&str> = order
        .iter()
        .map(String::as_str)
        .filter(|q| *q != "qc")
        .collect();
    assert_eq!(order.iter().filter(|q| *q == "qc").count(), 8);
    assert_eq!(a, ["qa", "qa", "qa", "qb", "qa", "qa", "qa", "qb"]);
}

#[test]
fn tenant_key_rules() {
    assert_eq!(tenant_key(Some("acme"), "acme/wf"), "acme");
    assert_eq!(tenant_key(Some("acme"), "wf"), "acme");
    assert_eq!(
        tenant_key(None, "acme/wf"),
        "acme",
        "P14-F9: qualified workflow"
    );
    assert_eq!(tenant_key(None, "wf"), "");
    assert_eq!(tenant_key(Some(""), "wf"), "");
    assert_eq!(tenant_key(Some("_"), "acme/wf"), "");
    assert_eq!(tenant_key(None, "Bad/wf"), "", "not a tenant id");
}

#[test]
fn tenant_policy_is_a_trait_object() {
    struct Fixed;
    impl TenantPolicy for Fixed {
        fn tenant_weight(&self, _: &str) -> u32 {
            3
        }
    }
    let mut f = FairQueues::new();
    backlog(&mut f, "x", 0, 2);
    f.refresh_tenant_weights(&Fixed);
    assert_eq!(f.tenant_weight("x"), 3);
}
