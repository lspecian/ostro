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
//! A held entry ([`FairQueues::hold`], from a workflow-scoped `Admit::Paused`) leaves the
//! selection on its own until [`FairQueues::release_held`] (the next flow-control
//! `changed()` bump): every other entry of its key, band and queue keeps being served, and
//! holding charges neither the key nor the queue's turn. Held entries still count in
//! [`FairQueues::len`], [`FairQueues::per_queue_counts`] and
//! [`FairQueues::per_queue_oldest`], not in [`FairQueues::per_queue_unblocked`].
//!
//! A first-activation gate ([`FairQueues::gate_first`] for a queue,
//! [`FairQueues::gate_first_key`] for one key, from a throttle `QueueBlocked` /
//! `KeyBlocked` on a first activation, plan 07-23, D-W13) holds back only the **first
//! activations** of that queue (key) until its `until_ms` passes
//! ([`FairQueues::unpark_due`]) or [`FairQueues::ungate_all`] (the next flow-control
//! `changed()` bump). Re-activations of the gated queue (key) stay servable in the same
//! round, in their usual fair order: every band keeps a second order over its keys' heads
//! of re-activations only, so a queue gate costs O(log n) to set or lift and the candidate
//! search under it is the same O(log n) per band. Gated first activations count in
//! [`FairQueues::len`] / [`FairQueues::per_queue_counts`] /
//! [`FairQueues::per_queue_oldest`], not in [`FairQueues::per_queue_unblocked`].
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

/// One tenant's selection (the Phase 7 selector, unchanged): weighted DRR across queues,
/// strict priority bands, round robin across keys.
#[derive(Debug, Default)]
struct QueueSet {
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

impl QueueSet {
    pub fn is_empty(&self) -> bool {
        self.index.is_empty() && self.held.is_empty()
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
        if let Some(Some(until)) = self.parked_keys.remove(&(queue.to_owned(), key.to_owned())) {
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
}

/// Bounds of a tenant's DRR quantum (W16): a stored weight of 0 or an absurd value can
/// never stall or starve the tenant ring.
pub const TENANT_WEIGHT_MIN: u32 = 1;
/// Upper bound of a tenant's DRR quantum (W16).
pub const TENANT_WEIGHT_MAX: u32 = 10_000;

/// The tenant level's weight lookup (D-P14-14): the DRR quantum of a tenant
/// (`Tenant.weight`, default 1), clamped to [`TENANT_WEIGHT_MIN`]..=[`TENANT_WEIGHT_MAX`]
/// by the scheduler.
pub trait TenantPolicy {
    fn tenant_weight(&self, tenant: &str) -> u32;
}

impl<F: Fn(&str) -> u32> TenantPolicy for F {
    fn tenant_weight(&self, tenant: &str) -> u32 {
        self(tenant)
    }
}

/// The fair-scheduler tenant of an entry (D-P14-14, P14-F9): `tenant` when set (the
/// default tenant's spellings `""` and `_` are `""`), else the D23 tenant prefix of a
/// qualified `workflow` (`<tenant>/<name>`), else `""` (the default tenant).
pub fn tenant_key<'a>(tenant: Option<&'a str>, workflow: &'a str) -> &'a str {
    match tenant {
        Some("" | "_") => "",
        Some(t) => t,
        None => match workflow.split_once('/') {
            Some((t, _)) if is_tenant_id(t) => t,
            _ => "",
        },
    }
}

/// D23 tenant-id grammar `^[a-z0-9][a-z0-9-]{0,62}$` (ostro-meta's `is_tenant_id`; this
/// crate does not depend on ostro-meta).
fn is_tenant_id(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// Weighted DRR across tenants (D-P14-14), then per tenant the Phase 7 selector (weighted
/// DRR across queues, strict priority bands, round robin across keys).
///
/// **Tenant level.** The tenant whose turn it is may serve up to its quantum
/// ([`Self::set_tenant_weight`] / [`Self::refresh_tenant_weights`], clamped to
/// `1..=10_000`, default 1) entries before the turn passes to the next tenant with queued
/// entries in name order (the default tenant `""` first). A blocked tenant
/// ([`Self::block_tenant`], from `Admit::TenantBlocked`) leaves the ring until
/// [`Self::release_tenant`], its `until_ms` ([`Self::unpark_due`]) or the next flow-control
/// `changed()` bump ([`Self::release_held`] / [`Self::unpark_all`]); like a skipped queue,
/// it loses the rest of its turn only when another tenant's entry is taken.
///
/// **One tenant** (every entry inserted with [`Self::insert`], or all with the same
/// tenant): the order is exactly the Phase 7 order.
///
/// Queue-, key- and gate-level calls (`skip_queue`, `park_queue`, `park_key`,
/// `gate_first`, `gate_first_key` and the unpark/ungate calls) apply to every tenant's
/// selector: queue and key limits, pauses and placement are node-wide.
#[derive(Debug, Default)]
pub struct FairQueues {
    /// Per-tenant selectors (kept while leading; a tenant's selector keeps its Phase 7
    /// turn state when it runs empty, so one tenant behaves exactly like Phase 7).
    sets: BTreeMap<String, QueueSet>,
    /// Tenant of every queued or held entry.
    tenant_of: BTreeMap<RunId, String>,
    /// Queue weights (seed of a new tenant's selector).
    weights: BTreeMap<String, u32>,
    /// Tenant quanta other than 1.
    tenant_weights: BTreeMap<String, u32>,
    /// The tenant whose turn it is and the picks left in the turn.
    turn: Option<(String, u64)>,
    /// Node-wide queue/key state that seeds a new tenant's selector (so a tenant that
    /// appears after a skip / park behaves as if it had been there): queues skipped this
    /// round, placement-parked queues, parked keys -> `until_ms`.
    skipped: BTreeSet<String>,
    parked_queues: BTreeSet<String>,
    parked_keys: BTreeMap<(String, String), Option<u64>>,
    /// Blocked tenants -> `until_ms`.
    blocked: BTreeMap<String, Option<u64>>,
    /// Timed tenant blocks `(until_ms, tenant)`.
    block_until: BTreeSet<(u64, String)>,
}

impl FairQueues {
    pub fn new() -> Self {
        Self::default()
    }

    fn set_mut(&mut self, tenant: &str) -> &mut QueueSet {
        if !self.sets.contains_key(tenant) {
            let mut set = QueueSet {
                weights: self.weights.clone(),
                skipped: self.skipped.clone(),
                parked_queues: self.parked_queues.clone(),
                ..QueueSet::default()
            };
            for ((q, k), until) in &self.parked_keys {
                set.park_key(q, k, *until);
            }
            self.sets.insert(tenant.to_owned(), set);
        }
        self.sets.get_mut(tenant).expect("inserted above")
    }

    /// Number of entries (held ones included).
    pub fn len(&self) -> usize {
        self.tenant_of.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tenant_of.is_empty()
    }

    pub fn contains(&self, run: &RunId) -> bool {
        self.tenant_of.contains_key(run)
    }

    /// The tenant `run` is filed under.
    pub fn tenant_of(&self, run: &RunId) -> Option<&str> {
        self.tenant_of.get(run).map(String::as_str)
    }

    /// Tenants with queued or held entries.
    pub fn tenant_names(&self) -> Vec<String> {
        self.sets
            .iter()
            .filter(|(_, s)| !s.is_empty())
            .map(|(t, _)| t.clone())
            .collect()
    }

    /// Entries per non-empty queue (held entries included).
    pub fn per_queue_counts(&self) -> BTreeMap<String, usize> {
        let mut out = BTreeMap::new();
        for s in self.sets.values() {
            for (q, n) in s.per_queue_counts() {
                *out.entry(q).or_default() += n;
            }
        }
        out
    }

    /// Names of the non-empty queues.
    pub fn queue_names(&self) -> Vec<String> {
        let names: BTreeSet<String> = self.sets.values().flat_map(QueueSet::queue_names).collect();
        names.into_iter().collect()
    }

    /// Oldest `ready_at_ms` per non-empty queue (held entries included).
    pub fn per_queue_oldest(&self) -> BTreeMap<String, u64> {
        let mut out: BTreeMap<String, u64> = BTreeMap::new();
        for s in self.sets.values() {
            for (q, at) in s.per_queue_oldest() {
                out.entry(q)
                    .and_modify(|o| *o = (*o).min(at))
                    .or_insert(at);
            }
        }
        out
    }

    /// Entries per non-empty queue that flow control did not hold back in the last round
    /// (Phase 7 rules per tenant; every entry of a blocked tenant is held back).
    pub fn per_queue_unblocked(&self) -> BTreeMap<String, usize> {
        let mut out: BTreeMap<String, usize> = BTreeMap::new();
        for (t, s) in &self.sets {
            let blocked = self.blocked.contains_key(t);
            for (q, n) in s.per_queue_unblocked() {
                *out.entry(q).or_default() += if blocked { 0 } else { n };
            }
        }
        out
    }

    /// DRR quantum of `queue` (clamped to >= 1), in every tenant.
    pub fn set_weight(&mut self, queue: &str, weight: u32) {
        let w = weight.max(1);
        if w == 1 {
            self.weights.remove(queue);
        } else {
            self.weights.insert(queue.to_owned(), w);
        }
        for s in self.sets.values_mut() {
            s.set_weight(queue, w);
        }
    }

    #[cfg(test)]
    fn weight(&self, queue: &str) -> u64 {
        u64::from(self.weights.get(queue).copied().unwrap_or(1))
    }

    /// DRR quantum of `tenant`, clamped to `1..=10_000` (W16).
    pub fn set_tenant_weight(&mut self, tenant: &str, weight: u32) {
        let w = weight.clamp(TENANT_WEIGHT_MIN, TENANT_WEIGHT_MAX);
        if w == 1 {
            self.tenant_weights.remove(tenant);
        } else {
            self.tenant_weights.insert(tenant.to_owned(), w);
        }
    }

    /// Re-read the quantum of every tenant with entries from `policy`.
    pub fn refresh_tenant_weights(&mut self, policy: &dyn TenantPolicy) {
        for t in self.tenant_names() {
            let w = policy.tenant_weight(&t);
            self.set_tenant_weight(&t, w);
        }
    }

    /// The (clamped) DRR quantum of `tenant`.
    pub fn tenant_weight(&self, tenant: &str) -> u64 {
        u64::from(self.tenant_weights.get(tenant).copied().unwrap_or(1))
    }

    /// File `e` under the default tenant (replacing an earlier entry of the same run).
    pub fn insert(&mut self, e: FairEntry) {
        self.insert_in("", e);
    }

    /// File `e` under `tenant` (replacing an earlier entry of the same run).
    pub fn insert_in(&mut self, tenant: &str, e: FairEntry) {
        self.remove(&e.run);
        self.tenant_of.insert(e.run, tenant.to_owned());
        self.set_mut(tenant).insert(e);
    }

    /// Remove `run` (queued or held); returns its entry.
    pub fn remove(&mut self, run: &RunId) -> Option<FairEntry> {
        let t = self.tenant_of.remove(run)?;
        let e = self.sets.get_mut(&t).and_then(|s| s.remove(run));
        if !self.has_queued(&t) {
            self.release_tenant(&t);
        }
        e
    }

    /// Whether `tenant` has entries in the selection (held ones not counted).
    fn has_queued(&self, tenant: &str) -> bool {
        self.sets.get(tenant).is_some_and(|s| !s.queues.is_empty())
    }

    /// Tenant names with queued entries in turn order: the current turn's tenant first,
    /// then by name, wrapping.
    fn turn_order(&self) -> Vec<String> {
        let queued = |(t, s): (&String, &QueueSet)| (!s.queues.is_empty()).then(|| t.clone());
        match self.turn.as_ref().map(|(t, _)| t.clone()) {
            Some(start) => self
                .sets
                .range::<String, _>((Bound::Included(&start), Bound::Unbounded))
                .chain(
                    self.sets
                        .range::<String, _>((Bound::Unbounded, Bound::Excluded(&start))),
                )
                .filter_map(queued)
                .collect(),
            None => self.sets.iter().filter_map(queued).collect(),
        }
    }

    /// The next entry to try (due at `now_ms`), or `None` once every tenant is blocked or
    /// has nothing servable. Does not remove it and does not move a turn: call
    /// [`Self::take`] after admission and placement, or skip / park / block what held it.
    pub fn next_candidate(&mut self, now_ms: u64) -> Option<Candidate> {
        for t in self.turn_order() {
            if self.blocked.contains_key(&t) {
                continue;
            }
            if let Some(c) = self.sets.get_mut(&t).and_then(|s| s.next_candidate(now_ms)) {
                return Some(c);
            }
        }
        None
    }

    /// Remove the candidate (it was granted) and charge its tenant, queue and key one pick.
    pub fn take(&mut self, c: &Candidate) -> Option<FairEntry> {
        let t = self.tenant_of.get(&c.run)?.clone();
        let e = self.sets.get_mut(&t)?.take(c)?;
        self.tenant_of.remove(&c.run);
        // Served out of turn (the turn's tenant was blocked or had nothing servable).
        if self.turn.as_ref().is_none_or(|(cur, _)| *cur != t) {
            self.turn = Some((t.clone(), self.tenant_weight(&t)));
        }
        let pass = match &mut self.turn {
            Some((_, left)) => {
                *left = left.saturating_sub(1);
                *left == 0
            }
            None => false,
        };
        if pass || !self.has_queued(&t) {
            self.pass_turn(&t);
        }
        if !self.has_queued(&t) {
            self.release_tenant(&t);
        }
        Some(e)
    }

    /// Give the turn to the tenant with queued entries after `tenant` (name order,
    /// wrapping; `tenant` itself when it is the only one).
    fn pass_turn(&mut self, tenant: &str) {
        let queued = |(t, s): (&String, &QueueSet)| (!s.queues.is_empty()).then(|| t.clone());
        let next = self
            .sets
            .range::<str, _>((Bound::Excluded(tenant), Bound::Unbounded))
            .find_map(queued)
            .or_else(|| self.sets.iter().find_map(queued));
        self.turn = next.map(|n| {
            let w = self.tenant_weight(&n);
            (n, w)
        });
    }

    /// Start a new dispatch round: queues skipped in the last round are tried again.
    pub fn begin_round(&mut self) {
        self.skipped.clear();
        for s in self.sets.values_mut() {
            s.begin_round();
        }
    }

    /// Skip `queue` for the rest of the round (`QueueBlocked`, `Paused`).
    pub fn skip_queue(&mut self, queue: &str) {
        self.skipped.insert(queue.to_owned());
        for s in self.sets.values_mut() {
            s.skip_queue(queue);
        }
    }

    /// Hold the candidate out of the selection until [`Self::release_held`] (its workflow
    /// is paused).
    pub fn hold(&mut self, c: &Candidate) {
        if let Some(t) = self.tenant_of.get(&c.run).cloned()
            && let Some(s) = self.sets.get_mut(&t)
        {
            s.hold(c);
        }
    }

    /// Put every held entry back into the selection and release every blocked tenant (a
    /// flow-control `changed()` bump).
    pub fn release_held(&mut self) {
        for s in self.sets.values_mut() {
            s.release_held();
        }
        self.release_tenants();
    }

    /// Whether any entry is held.
    pub fn has_held(&self) -> bool {
        self.sets.values().any(QueueSet::has_held)
    }

    /// Skip `queue` until [`Self::unpark_queues`] (placement had no capacity).
    pub fn park_queue(&mut self, queue: &str) {
        self.parked_queues.insert(queue.to_owned());
        for s in self.sets.values_mut() {
            s.park_queue(queue);
        }
    }

    /// Park `key` of `queue` until `until_ms` (`None` = until [`Self::unpark_all`]).
    pub fn park_key(&mut self, queue: &str, key: &str, until_ms: Option<u64>) {
        self.parked_keys
            .insert((queue.to_owned(), key.to_owned()), until_ms);
        for s in self.sets.values_mut() {
            s.park_key(queue, key, until_ms);
        }
    }

    /// Unpark keys, lift gates and release tenant blocks whose `until_ms` passed.
    pub fn unpark_due(&mut self, now_ms: u64) {
        self.parked_keys
            .retain(|_, until| until.is_none_or(|u| u > now_ms));
        for s in self.sets.values_mut() {
            s.unpark_due(now_ms);
        }
        while let Some((at, t)) = self.block_until.first().cloned() {
            if at > now_ms {
                break;
            }
            self.release_tenant(&t);
        }
    }

    /// Earliest `until_ms` of a parked key, a first-activation gate or a tenant block.
    pub fn next_unpark_at(&self) -> Option<u64> {
        self.sets
            .values()
            .filter_map(QueueSet::next_unpark_at)
            .chain(self.block_until.first().map(|(at, _)| *at))
            .min()
    }

    /// Hold back the first activations of `queue` until `until_ms` or
    /// [`Self::ungate_all`].
    pub fn gate_first(&mut self, queue: &str, until_ms: u64) {
        for s in self.sets.values_mut() {
            s.gate_first(queue, until_ms);
        }
    }

    /// Hold back the first activations of `key` of `queue` until `until_ms` or
    /// [`Self::ungate_all`].
    pub fn gate_first_key(&mut self, queue: &str, key: &str, until_ms: u64) {
        for s in self.sets.values_mut() {
            s.gate_first_key(queue, key, until_ms);
        }
    }

    /// Lift every first-activation gate (a flow-control `changed()` bump).
    pub fn ungate_all(&mut self) {
        for s in self.sets.values_mut() {
            s.ungate_all();
        }
    }

    /// Whether `queue`'s first activations are gated (in any tenant).
    pub fn is_first_gated(&self, queue: &str) -> bool {
        self.sets.values().any(|s| s.is_first_gated(queue))
    }

    /// Unpark every queue (a placement `changed()` bump).
    pub fn unpark_queues(&mut self) {
        self.parked_queues.clear();
        for s in self.sets.values_mut() {
            s.unpark_queues();
        }
    }

    /// Unpark every key (a flow-control `changed()` bump).
    pub fn unpark_keys(&mut self) {
        self.parked_keys.clear();
        for s in self.sets.values_mut() {
            s.unpark_keys();
        }
    }

    /// Unpark every queue and key, lift every gate, release every held entry and every
    /// blocked tenant.
    pub fn unpark_all(&mut self) {
        self.parked_queues.clear();
        self.parked_keys.clear();
        for s in self.sets.values_mut() {
            s.unpark_all();
        }
        self.release_tenants();
    }

    /// Whether any queue is parked (placement had no capacity).
    pub fn has_parked_queues(&self) -> bool {
        !self.parked_queues.is_empty() || self.sets.values().any(QueueSet::has_parked_queues)
    }

    /// Whether any queue or key is parked.
    pub fn any_parked(&self) -> bool {
        !self.parked_queues.is_empty() || self.sets.values().any(QueueSet::any_parked)
    }

    /// Take `tenant` out of the ring until [`Self::release_tenant`], `until_ms` (clock ms;
    /// `None` = until the next `changed()` bump) or [`Self::unpark_all`]
    /// (`Admit::TenantBlocked`). Its entries stay queued (held, never failed); a later
    /// block replaces the deadline.
    pub fn block_tenant(&mut self, tenant: &str, until_ms: Option<u64>) {
        self.release_tenant(tenant);
        if !self.has_queued(tenant) {
            return;
        }
        self.blocked.insert(tenant.to_owned(), until_ms);
        if let Some(u) = until_ms {
            self.block_until.insert((u, tenant.to_owned()));
        }
    }

    /// Put `tenant` back into the ring.
    pub fn release_tenant(&mut self, tenant: &str) {
        if let Some(Some(u)) = self.blocked.remove(tenant) {
            self.block_until.remove(&(u, tenant.to_owned()));
        }
    }

    /// Put every blocked tenant back into the ring.
    pub fn release_tenants(&mut self) {
        self.blocked.clear();
        self.block_until.clear();
    }

    /// Whether `tenant` is blocked.
    pub fn is_tenant_blocked(&self, tenant: &str) -> bool {
        self.blocked.contains_key(tenant)
    }

    /// Drop every entry (held ones too), park and block (queue and tenant weights stay).
    pub fn clear(&mut self) {
        let weights = std::mem::take(&mut self.weights);
        let tenant_weights = std::mem::take(&mut self.tenant_weights);
        *self = Self {
            weights,
            tenant_weights,
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
            first_activation: true,
        }
    }

    /// A re-activation (`first_activation == false`).
    fn re(i: u64, q: &str, p: i32, k: &str, at: u64) -> FairEntry {
        FairEntry {
            first_activation: false,
            ..e(i, q, p, k, at)
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

    /// A held entry (workflow paused) leaves only itself out: the rest of its key and
    /// queue is still served, other queues keep their turns, and release puts it back.
    #[test]
    fn held_entry_skips_only_itself() {
        let mut f = FairQueues::new();
        f.insert(e(1, "q", 0, "", 1)); // paused workflow, head of the shared key
        f.insert(e(2, "q", 0, "", 2));
        f.insert(e(3, "q", 0, "", 3));
        f.insert(e(4, "r", 0, "", 1));
        let c = f.next_candidate(10).expect("c");
        assert_eq!(c.run, rid(1));
        f.hold(&c);
        f.hold(&c); // idempotent
        assert_eq!(f.len(), 4);
        assert!(f.contains(&rid(1)));
        assert!(f.has_held());
        assert_eq!(f.per_queue_counts()["q"], 3);
        assert_eq!(f.per_queue_unblocked()["q"], 2);
        assert_eq!(f.per_queue_oldest()["q"], 1);
        // Holding moved no turn: q is served, then r, then q.
        let order: Vec<RunId> = drain(&mut f, 10).iter().map(|e| e.run).collect();
        assert_eq!(order, vec![rid(2), rid(4), rid(3)]);
        assert!(
            f.next_candidate(10).is_none(),
            "only the held entry is left"
        );
        assert!(!f.is_empty());
        assert_eq!(f.per_queue_counts()["q"], 1);
        assert_eq!(f.per_queue_unblocked()["q"], 0);
        f.begin_round();
        assert!(f.next_candidate(10).is_none(), "a new round keeps it held");
        f.release_held();
        assert!(!f.has_held());
        assert_eq!(f.next_candidate(10).map(|c| c.run), Some(rid(1)));
        // A held entry can be removed or replaced.
        let c = f.next_candidate(10).expect("c");
        f.hold(&c);
        assert_eq!(f.remove(&rid(1)).map(|e| e.run), Some(rid(1)));
        assert!(f.is_empty());
        assert!(f.per_queue_counts().is_empty());
        f.insert(e(5, "q", 0, "", 1));
        let c = f.next_candidate(10).expect("c");
        f.hold(&c);
        f.insert(e(5, "q", 0, "", 2));
        assert!(!f.has_held(), "re-insert replaces the held entry");
        assert_eq!(f.len(), 1);
        let c = f.next_candidate(10).expect("c");
        f.hold(&c);
        f.unpark_all();
        assert_eq!(f.next_candidate(10).map(|c| c.run), Some(rid(5)));
        let c = f.next_candidate(10).expect("c");
        f.hold(&c);
        f.clear();
        assert!(f.is_empty() && !f.has_held());
    }

    /// A queue gate (throttled first activation, 07-23) holds back only the queue's first
    /// activations: a re-activation behind them on the same key is served at once, other
    /// queues keep their turns, and the gate lifts at `until_ms` or on `ungate_all`.
    #[test]
    fn queue_gate_holds_only_first_activations() {
        let mut f = FairQueues::new();
        f.insert(e(1, "q", 0, "", 1)); // throttled first activation, head of the FIFO
        f.insert(re(2, "q", 0, "", 2)); // re-activation behind it, same key
        f.insert(e(3, "q", 0, "", 3));
        f.insert(re(4, "q", 0, "k", 4));
        f.insert(e(5, "r", 0, "", 1));
        let c = f.next_candidate(10).expect("c");
        assert_eq!((c.run, c.first_activation), (rid(1), true));
        f.gate_first("q", 50);
        assert!(f.is_first_gated("q"));
        assert_eq!(f.next_unpark_at(), Some(50));
        assert_eq!(f.len(), 5);
        assert_eq!(f.per_queue_counts()["q"], 4);
        assert_eq!(f.per_queue_unblocked()["q"], 2, "only the re-activations");
        assert_eq!(f.per_queue_oldest()["q"], 1);
        let c = f.next_candidate(10).expect("re-activation");
        assert_eq!((c.run, c.first_activation), (rid(2), false));
        let order: Vec<RunId> = drain(&mut f, 10).iter().map(|e| e.run).collect();
        assert_eq!(order, vec![rid(2), rid(5), rid(4)]);
        assert!(f.next_candidate(10).is_none(), "first activations gated");
        f.begin_round();
        assert!(f.next_candidate(10).is_none(), "a new round keeps the gate");
        // A new first activation joins the gated ones; a new re-activation is served.
        f.insert(e(6, "q", 0, "", 0));
        f.insert(re(7, "q", 0, "", 9));
        assert_eq!(f.next_candidate(10).map(|c| c.run), Some(rid(7)));
        let c = f.next_candidate(10).expect("c");
        f.take(&c);
        f.unpark_due(49);
        assert!(f.next_candidate(49).is_none());
        f.unpark_due(50);
        assert!(!f.is_first_gated("q"));
        assert_eq!(f.next_unpark_at(), None);
        assert_eq!(f.next_candidate(50).map(|c| c.run), Some(rid(6)));
        // ungate_all (a changed() bump) lifts it before the deadline.
        f.gate_first("q", 1_000);
        assert!(f.next_candidate(50).is_none());
        f.ungate_all();
        assert_eq!(f.next_candidate(50).map(|c| c.run), Some(rid(6)));
        // A later gate replaces the deadline; emptying the queue drops the gate.
        f.gate_first("q", 1_000);
        f.gate_first("q", 2_000);
        assert_eq!(f.next_unpark_at(), Some(2_000));
        f.remove(&rid(6));
        f.remove(&rid(1));
        f.remove(&rid(3));
        assert!(f.is_empty());
        assert!(!f.is_first_gated("q"));
        assert_eq!(f.next_unpark_at(), None);
        f.gate_first("gone", 5);
        assert!(!f.is_first_gated("gone"), "no gate on an empty queue");
    }

    /// A key gate (key throttle) holds back only that key's first activations; its
    /// re-activations and other keys are served; a park of the same key still wins.
    #[test]
    fn key_gate_holds_only_the_keys_first_activations() {
        let mut f = FairQueues::new();
        f.insert(e(1, "q", 0, "a", 1));
        f.insert(re(2, "q", 0, "a", 2));
        f.insert(e(3, "q", 0, "b", 3));
        f.insert(e(4, "q", 0, "a", 4));
        assert_eq!(f.next_candidate(10).map(|c| c.run), Some(rid(1)));
        f.gate_first_key("q", "a", 40);
        assert_eq!(f.next_unpark_at(), Some(40));
        assert_eq!(f.per_queue_unblocked()["q"], 2);
        let order: Vec<RunId> = drain(&mut f, 10).iter().map(|e| e.run).collect();
        assert_eq!(order, vec![rid(2), rid(3)]);
        assert!(f.next_candidate(10).is_none());
        f.insert(re(5, "q", 0, "a", 5));
        f.park_key("q", "a", None);
        assert!(f.next_candidate(10).is_none(), "parked wins over the gate");
        assert_eq!(f.per_queue_unblocked()["q"], 0);
        f.unpark_keys();
        assert_eq!(f.next_candidate(10).map(|c| c.run), Some(rid(5)));
        let c = f.next_candidate(10).expect("c");
        f.take(&c);
        f.unpark_due(40);
        assert_eq!(f.next_candidate(40).map(|c| c.run), Some(rid(1)));
        // The last entry of a gated key leaving drops the gate.
        f.gate_first_key("q", "a", 90);
        f.remove(&rid(1));
        f.remove(&rid(4));
        assert_eq!(f.next_unpark_at(), None);
        f.insert(e(6, "q", 0, "a", 1));
        assert_eq!(f.next_candidate(10).map(|c| c.run), Some(rid(6)));
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
                (0u8..6, 0u64..40, 0u8..3, -2i32..3, 0u8..4, 0u64..20),
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
                        let mut x = e(id, &format!("q{q}"), p, &format!("k{k}"), at);
                        x.first_activation = op == 0;
                        f.insert(x);
                        live.insert(run);
                        taken.remove(&run);
                    }
                    2 => {
                        prop_assert_eq!(f.remove(&run).is_some(), live.remove(&run));
                    }
                    3 => {
                        // Throttle gates (07-23); `at` 0 lifts every gate (changed()).
                        if at == 0 {
                            f.ungate_all();
                        } else if k < 2 {
                            f.gate_first(&format!("q{q}"), at);
                        } else {
                            f.gate_first_key(&format!("q{q}"), &format!("k{k}"), at);
                        }
                        f.unpark_due(id % 20);
                    }
                    _ => {
                        if let Some(c) = f.next_candidate(u64::MAX) {
                            prop_assert!(live.contains(&c.run));
                            if c.first_activation {
                                prop_assert!(!f.is_first_gated(&c.queue), "gated first activation proposed");
                            }
                            if k == 0 {
                                // Workflow paused: held, not returned.
                                f.hold(&c);
                                prop_assert!(f.contains(&c.run));
                            } else {
                                let t = f.take(&c).expect("candidate present");
                                prop_assert!(taken.insert(t.run), "returned twice");
                                live.remove(&t.run);
                            }
                        }
                    }
                }
                prop_assert_eq!(f.len(), live.len());
            }
            f.release_held();
            f.ungate_all();
            prop_assert_eq!(f.next_unpark_at(), None);
            while let Some(c) = f.next_candidate(u64::MAX) {
                let t = f.take(&c).expect("present");
                prop_assert!(live.remove(&t.run), "returned twice or unknown");
            }
            prop_assert!(live.is_empty(), "entries never returned: {}", live.len());
            prop_assert!(f.is_empty());
        }
    }
}
