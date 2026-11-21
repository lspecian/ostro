//! Node-wide concurrency counters and the hand-rolled GCRA (plan 07-08, D-W13, D-W24).
//!
//! # GCRA
//!
//! One theoretical arrival time (TAT) per queue and per (queue, key), in **nanoseconds**
//! (`now_ms × 10⁶` from the injected clock, D-W26). For a [`RateSpec`] `{limit, period_ms,
//! burst}` with `limit` and `burst` normalised to `max(1, value)`:
//!
//! - `T = period_ns / limit` (emission interval; at least 1 ns, so 5000/s is 200 µs and
//!   never rounds to 0), `τ = T × (burst − 1)` (burst tolerance).
//! - A request at `now` conforms iff `now ≥ TAT − τ`; otherwise it may go at `TAT − τ`.
//! - A conforming request advances `TAT = max(TAT, now) + T`.
//! - `period_ms == 0` is invalid and the spec is ignored (no limit; the policy API
//!   validates `period_ms > 0`).
//!
//! # Dispatcher admission (tentative, W2)
//!
//! [`Counters::try_admit`] checks, in order, queue concurrency, key concurrency and (first
//! activations only) the queue and key throttles. Both throttle TATs are checked before
//! either is advanced, and neither moves unless both conform. A success increments the
//! queue and key `running` counters and advances the TATs, remembering for each TAT the
//! previous value and the value it wrote ([`Charge`]). The caller wraps the charge in a
//! `FlowPermit`: committed after `Placement::reserve` returned `Granted`, it only releases
//! the counters when the lease ends ([`Counters::release`]); dropped uncommitted it rolls
//! back ([`Counters::rollback`]): counters decrement, and each TAT is restored to the
//! remembered previous value **if it still equals the value this charge wrote**, otherwise
//! (a later admission advanced it meanwhile) it is reduced by at most `T` but never below
//! `max(previous, now)`, the conservative choice.
//!
//! # StartRun rate limit
//!
//! [`RateCells::admit`] is the same GCRA over separate cells (`rate_limit` /
//! `key_rate_limit`), checked and committed in one call under one lock: both TATs are
//! advanced only if both conform (no partial charge).
//!
//! # Memory bounds
//!
//! Queue cells are kept per queue name (bounded by configuration). Key cells are bounded
//! by `[flow] max_tracked_keys` per table (counters/throttles, rate limits): a key cell
//! with no running activation whose TAT is in the past carries no state (an absent cell
//! behaves the same) and is evicted after `[flow] idle_key_evict` of inactivity, or at once
//! when the table is over the cap. If the table is still over the cap, the idle cells
//! used least recently are evicted even with a future TAT (lossy: such a key may start
//! early). Cells with running activations are never evicted; their number is bounded by
//! the node's leases.

use std::collections::BTreeMap;

use ostro_meta::RateSpec;

/// Nanoseconds per millisecond.
pub const NS_PER_MS: u64 = 1_000_000;

/// Clock milliseconds to GCRA nanoseconds.
pub fn ms_to_ns(ms: u64) -> u64 {
    ms.saturating_mul(NS_PER_MS)
}

/// GCRA nanoseconds to clock milliseconds, rounded up.
pub fn ns_to_ms_ceil(ns: u64) -> u64 {
    ns.div_ceil(NS_PER_MS)
}

/// A normalised GCRA rate: emission interval `t_ns` and burst tolerance `tau_ns`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Gcra {
    pub t_ns: u64,
    pub tau_ns: u64,
}

impl Gcra {
    /// `None` for an invalid spec (`period_ms == 0`).
    pub fn from_spec(spec: &RateSpec) -> Option<Self> {
        if spec.period_ms == 0 {
            return None;
        }
        let limit = u64::from(spec.limit.max(1));
        let burst = u64::from(spec.burst.max(1));
        let t_ns = (ms_to_ns(spec.period_ms) / limit).max(1);
        Some(Self {
            t_ns,
            tau_ns: t_ns.saturating_mul(burst - 1),
        })
    }

    /// `Ok` when a request at `now_ns` conforms to `tat`, else `Err(earliest instant)`.
    pub fn conforms(&self, tat: u64, now_ns: u64) -> Result<(), u64> {
        let allowed = tat.saturating_sub(self.tau_ns);
        if now_ns >= allowed {
            Ok(())
        } else {
            Err(allowed)
        }
    }

    /// The TAT after a conforming request at `now_ns`.
    pub fn advance(&self, tat: u64, now_ns: u64) -> u64 {
        tat.max(now_ns).saturating_add(self.t_ns)
    }

    /// The TAT after rolling back a charge that moved it `prev -> written`, when it is
    /// `cur` now (module docs).
    pub fn rollback(&self, cur: u64, prev: u64, written: u64, now_ns: u64) -> u64 {
        if cur == written {
            prev
        } else {
            cur.saturating_sub(self.t_ns).max(prev.max(now_ns)).min(cur)
        }
    }
}

/// Bounds of the key tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyBounds {
    pub max_tracked_keys: usize,
    pub idle_evict_ms: u64,
}

impl Default for KeyBounds {
    fn default() -> Self {
        Self {
            max_tracked_keys: 100_000,
            idle_evict_ms: 600_000,
        }
    }
}

/// What one admission asks for (from the queue policy).
#[derive(Clone, Debug, Default)]
pub struct AdmitReq<'a> {
    pub queue: &'a str,
    /// `""` = unkeyed (never key-limited).
    pub key: &'a str,
    pub concurrency: Option<u32>,
    pub key_concurrency: Option<u32>,
    /// Throttles apply only when `first_activation`.
    pub throttle: Option<Gcra>,
    pub key_throttle: Option<Gcra>,
    pub first_activation: bool,
}

/// Why an admission was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Block {
    /// Queue concurrency reached.
    QueueConcurrency,
    /// Key concurrency reached.
    KeyConcurrency,
    /// Queue throttle: may go at this instant (ns).
    QueueThrottle(u64),
    /// Key throttle: may go at this instant (ns).
    KeyThrottle(u64),
}

/// A throttle TAT move remembered for rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TatMove {
    pub gcra: Gcra,
    pub prev: u64,
    pub written: u64,
}

/// A tentative (then committed) admission charge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Charge {
    pub queue: String,
    /// `None` for an unkeyed entry.
    pub key: Option<String>,
    pub queue_tat: Option<TatMove>,
    pub key_tat: Option<TatMove>,
}

#[derive(Debug, Default)]
struct KeyCell {
    running: u32,
    tat: u64,
    last_ms: u64,
}

#[derive(Debug, Default)]
struct QueueCell {
    running: u32,
    tat: u64,
    keys: BTreeMap<String, KeyCell>,
}

/// Node-wide concurrency counters + throttle TATs (one instance behind one mutex, shared
/// by every partition dispatcher of the node).
#[derive(Debug, Default)]
pub struct Counters {
    queues: BTreeMap<String, QueueCell>,
    tracked_keys: usize,
    bounds: KeyBounds,
    next_sweep_ms: u64,
}

impl Counters {
    pub fn new(bounds: KeyBounds) -> Self {
        Self {
            bounds,
            ..Self::default()
        }
    }

    pub fn set_bounds(&mut self, bounds: KeyBounds) {
        self.bounds = bounds;
    }

    /// Activations of `queue` currently charged.
    pub fn running(&self, queue: &str) -> u32 {
        self.queues.get(queue).map_or(0, |q| q.running)
    }

    /// Activations of `(queue, key)` currently charged.
    pub fn key_running(&self, queue: &str, key: &str) -> u32 {
        self.queues
            .get(queue)
            .and_then(|q| q.keys.get(key))
            .map_or(0, |k| k.running)
    }

    /// Key cells currently tracked.
    pub fn tracked_keys(&self) -> usize {
        self.tracked_keys
    }

    /// Check and tentatively charge one admission at `now_ms` (module docs).
    pub fn try_admit(&mut self, r: &AdmitReq<'_>, now_ms: u64) -> Result<Charge, Block> {
        self.maybe_sweep(now_ms);
        let now_ns = ms_to_ns(now_ms);
        let keyed = !r.key.is_empty();
        let (q_running, q_tat) = self
            .queues
            .get(r.queue)
            .map_or((0, 0), |q| (q.running, q.tat));
        if r.concurrency.is_some_and(|c| q_running >= c) {
            return Err(Block::QueueConcurrency);
        }
        let (k_running, k_tat) = if keyed {
            self.queues
                .get(r.queue)
                .and_then(|q| q.keys.get(r.key))
                .map_or((0, 0), |k| (k.running, k.tat))
        } else {
            (0, 0)
        };
        if keyed && r.key_concurrency.is_some_and(|c| k_running >= c) {
            return Err(Block::KeyConcurrency);
        }
        let q_throttle = r.throttle.filter(|_| r.first_activation);
        let k_throttle = r.key_throttle.filter(|_| r.first_activation && keyed);
        if let Some(g) = q_throttle
            && let Err(at) = g.conforms(q_tat, now_ns)
        {
            return Err(Block::QueueThrottle(at));
        }
        if let Some(g) = k_throttle
            && let Err(at) = g.conforms(k_tat, now_ns)
        {
            return Err(Block::KeyThrottle(at));
        }
        // Every check passed: charge.
        let q = self.queues.entry(r.queue.to_owned()).or_default();
        q.running += 1;
        let queue_tat = q_throttle.map(|g| {
            let written = g.advance(q.tat, now_ns);
            let m = TatMove {
                gcra: g,
                prev: q.tat,
                written,
            };
            q.tat = written;
            m
        });
        let mut key_tat = None;
        let mut inserted = false;
        if keyed {
            let k = match q.keys.get_mut(r.key) {
                Some(k) => k,
                None => {
                    inserted = true;
                    q.keys.entry(r.key.to_owned()).or_default()
                }
            };
            k.running += 1;
            k.last_ms = now_ms;
            key_tat = k_throttle.map(|g| {
                let written = g.advance(k.tat, now_ns);
                let m = TatMove {
                    gcra: g,
                    prev: k.tat,
                    written,
                };
                k.tat = written;
                m
            });
        }
        if inserted {
            self.tracked_keys += 1;
            if self.tracked_keys > self.bounds.max_tracked_keys {
                self.enforce_cap(now_ms);
            }
        }
        Ok(Charge {
            queue: r.queue.to_owned(),
            key: keyed.then(|| r.key.to_owned()),
            queue_tat,
            key_tat,
        })
    }

    /// The lease of a committed charge ended: release its counters.
    pub fn release(&mut self, c: &Charge, now_ms: u64) {
        self.undo(c, now_ms, false);
    }

    /// An uncommitted charge was dropped: release the counters and restore the TATs.
    pub fn rollback(&mut self, c: &Charge, now_ms: u64) {
        self.undo(c, now_ms, true);
    }

    fn undo(&mut self, c: &Charge, now_ms: u64, tats: bool) {
        let now_ns = ms_to_ns(now_ms);
        let Some(q) = self.queues.get_mut(&c.queue) else {
            return;
        };
        q.running = q.running.saturating_sub(1);
        if tats && let Some(m) = c.queue_tat {
            q.tat = m.gcra.rollback(q.tat, m.prev, m.written, now_ns);
        }
        if let Some(key) = &c.key
            && let Some(k) = q.keys.get_mut(key)
        {
            k.running = k.running.saturating_sub(1);
            k.last_ms = k.last_ms.max(now_ms);
            if tats && let Some(m) = c.key_tat {
                k.tat = m.gcra.rollback(k.tat, m.prev, m.written, now_ns);
            }
        }
    }

    fn maybe_sweep(&mut self, now_ms: u64) {
        if now_ms < self.next_sweep_ms {
            return;
        }
        self.next_sweep_ms = now_ms.saturating_add((self.bounds.idle_evict_ms / 2).max(1_000));
        let idle = self.bounds.idle_evict_ms;
        let now_ns = ms_to_ns(now_ms);
        let mut removed = 0;
        for q in self.queues.values_mut() {
            q.keys.retain(|_, k| {
                let keep =
                    k.running > 0 || k.tat > now_ns || k.last_ms.saturating_add(idle) > now_ms;
                if !keep {
                    removed += 1;
                }
                keep
            });
        }
        self.tracked_keys -= removed;
    }

    fn enforce_cap(&mut self, now_ms: u64) {
        let now_ns = ms_to_ns(now_ms);
        let max = self.bounds.max_tracked_keys;
        // Lossless first: idle cells whose TAT is in the past carry no state.
        let mut removed = 0;
        for q in self.queues.values_mut() {
            q.keys.retain(|_, k| {
                let keep = k.running > 0 || k.tat > now_ns;
                if !keep {
                    removed += 1;
                }
                keep
            });
        }
        self.tracked_keys -= removed;
        if self.tracked_keys <= max {
            return;
        }
        // Lossy: the least recently used idle cells.
        let mut idle: Vec<(u64, String, String)> = self
            .queues
            .iter()
            .flat_map(|(qn, q)| {
                q.keys
                    .iter()
                    .filter(|(_, k)| k.running == 0)
                    .map(move |(kn, k)| (k.last_ms, qn.clone(), kn.clone()))
            })
            .collect();
        idle.sort();
        for (_, qn, kn) in idle {
            if self.tracked_keys <= max {
                break;
            }
            if let Some(q) = self.queues.get_mut(&qn)
                && q.keys.remove(&kn).is_some()
            {
                self.tracked_keys -= 1;
            }
        }
    }
}

#[derive(Debug, Default)]
struct RateKey {
    tat: u64,
    last_ms: u64,
}

#[derive(Debug, Default)]
struct RateQueue {
    tat: u64,
    keys: BTreeMap<String, RateKey>,
}

/// StartRun rate-limit cells (one instance behind one mutex).
#[derive(Debug, Default)]
pub struct RateCells {
    queues: BTreeMap<String, RateQueue>,
    tracked_keys: usize,
    bounds: KeyBounds,
    next_sweep_ms: u64,
}

impl RateCells {
    pub fn new(bounds: KeyBounds) -> Self {
        Self {
            bounds,
            ..Self::default()
        }
    }

    pub fn set_bounds(&mut self, bounds: KeyBounds) {
        self.bounds = bounds;
    }

    /// Key cells currently tracked.
    pub fn tracked_keys(&self) -> usize {
        self.tracked_keys
    }

    /// Check `queue` (and `(queue, key)` for a non-empty key) and charge both only if both
    /// conform. `Err(earliest instant, ns)` otherwise.
    pub fn admit(
        &mut self,
        queue: &str,
        key: &str,
        rate: Option<Gcra>,
        key_rate: Option<Gcra>,
        now_ms: u64,
    ) -> Result<(), u64> {
        let key_rate = key_rate.filter(|_| !key.is_empty());
        if rate.is_none() && key_rate.is_none() {
            return Ok(());
        }
        self.maybe_sweep(now_ms);
        let now_ns = ms_to_ns(now_ms);
        let q_tat = self.queues.get(queue).map_or(0, |q| q.tat);
        let k_tat = self
            .queues
            .get(queue)
            .and_then(|q| q.keys.get(key))
            .map_or(0, |k| k.tat);
        let q_ok = rate.map_or(Ok(()), |g| g.conforms(q_tat, now_ns));
        let k_ok = key_rate.map_or(Ok(()), |g| g.conforms(k_tat, now_ns));
        match (q_ok, k_ok) {
            (Ok(()), Ok(())) => {}
            (Err(a), Err(b)) => return Err(a.max(b)),
            (Err(a), _) | (_, Err(a)) => return Err(a),
        }
        let q = self.queues.entry(queue.to_owned()).or_default();
        if let Some(g) = rate {
            q.tat = g.advance(q.tat, now_ns);
        }
        if let Some(g) = key_rate {
            let inserted = !q.keys.contains_key(key);
            let k = q.keys.entry(key.to_owned()).or_default();
            k.tat = g.advance(k.tat, now_ns);
            k.last_ms = now_ms;
            if inserted {
                self.tracked_keys += 1;
                if self.tracked_keys > self.bounds.max_tracked_keys {
                    self.enforce_cap(now_ms);
                }
            }
        }
        Ok(())
    }

    fn maybe_sweep(&mut self, now_ms: u64) {
        if now_ms < self.next_sweep_ms {
            return;
        }
        self.next_sweep_ms = now_ms.saturating_add((self.bounds.idle_evict_ms / 2).max(1_000));
        let idle = self.bounds.idle_evict_ms;
        let now_ns = ms_to_ns(now_ms);
        let mut removed = 0;
        for q in self.queues.values_mut() {
            q.keys.retain(|_, k| {
                let keep = k.tat > now_ns || k.last_ms.saturating_add(idle) > now_ms;
                if !keep {
                    removed += 1;
                }
                keep
            });
        }
        self.tracked_keys -= removed;
    }

    fn enforce_cap(&mut self, now_ms: u64) {
        let now_ns = ms_to_ns(now_ms);
        let max = self.bounds.max_tracked_keys;
        let mut removed = 0;
        for q in self.queues.values_mut() {
            q.keys.retain(|_, k| {
                let keep = k.tat > now_ns;
                if !keep {
                    removed += 1;
                }
                keep
            });
        }
        self.tracked_keys -= removed;
        if self.tracked_keys <= max {
            return;
        }
        let mut all: Vec<(u64, String, String)> = self
            .queues
            .iter()
            .flat_map(|(qn, q)| {
                q.keys
                    .iter()
                    .map(move |(kn, k)| (k.last_ms, qn.clone(), kn.clone()))
            })
            .collect();
        all.sort();
        for (_, qn, kn) in all {
            if self.tracked_keys <= max {
                break;
            }
            if let Some(q) = self.queues.get_mut(&qn)
                && q.keys.remove(&kn).is_some()
            {
                self.tracked_keys -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(limit: u32, period_ms: u64, burst: u32) -> RateSpec {
        RateSpec {
            limit,
            period_ms,
            burst,
        }
    }

    fn g(limit: u32, period_ms: u64, burst: u32) -> Gcra {
        Gcra::from_spec(&spec(limit, period_ms, burst)).expect("valid spec")
    }

    #[test]
    fn gcra_burst_then_spacing() {
        // 5 per second, burst 3: T = 200 ms, tau = 400 ms.
        let r = g(5, 1_000, 3);
        assert_eq!(r.t_ns, 200 * NS_PER_MS);
        assert_eq!(r.tau_ns, 400 * NS_PER_MS);
        let now = ms_to_ns(10_000);
        let mut tat = 0;
        for _ in 0..3 {
            r.conforms(tat, now).expect("burst conforms");
            tat = r.advance(tat, now);
        }
        // The fourth must wait one emission interval.
        assert_eq!(r.conforms(tat, now), Err(now + 200 * NS_PER_MS));
        let later = now + 200 * NS_PER_MS;
        r.conforms(tat, later).expect("one interval later");
        tat = r.advance(tat, later);
        assert_eq!(r.conforms(tat, later), Err(later + 200 * NS_PER_MS));
    }

    #[test]
    fn gcra_normalises_zero_limit_and_burst_and_rejects_zero_period() {
        let r = g(0, 1_000, 0);
        assert_eq!(r, g(1, 1_000, 1));
        assert_eq!(r.tau_ns, 0);
        assert!(Gcra::from_spec(&spec(5, 0, 1)).is_none());
    }

    #[test]
    fn gcra_high_limit_does_not_round_to_zero() {
        // 5000/s burst 1: 200 us spacing; 5000 admits fit in exactly one second.
        let r = g(5_000, 1_000, 1);
        assert_eq!(r.t_ns, 200_000);
        let start = ms_to_ns(1_000);
        let mut tat = 0;
        let mut admitted = 0u32;
        let mut now = start;
        // Poll every 100 us over one second.
        while now < start + 1_000 * NS_PER_MS {
            if r.conforms(tat, now).is_ok() {
                tat = r.advance(tat, now);
                admitted += 1;
            }
            now += 100_000;
        }
        assert_eq!(admitted, 5_000);
    }

    fn req<'a>(q: &'a str, k: &'a str) -> AdmitReq<'a> {
        AdmitReq {
            queue: q,
            key: k,
            first_activation: true,
            ..AdmitReq::default()
        }
    }

    #[test]
    fn concurrency_counts_and_releases() {
        let mut c = Counters::new(KeyBounds::default());
        let r = AdmitReq {
            concurrency: Some(2),
            key_concurrency: Some(1),
            ..req("q", "a")
        };
        let a = c.try_admit(&r, 1).expect("first");
        assert_eq!(c.try_admit(&r, 1), Err(Block::KeyConcurrency));
        let b = c
            .try_admit(
                &AdmitReq {
                    key: "b",
                    ..r.clone()
                },
                1,
            )
            .expect("other key");
        assert_eq!(
            c.try_admit(
                &AdmitReq {
                    key: "c",
                    ..r.clone()
                },
                1
            ),
            Err(Block::QueueConcurrency)
        );
        assert_eq!((c.running("q"), c.key_running("q", "a")), (2, 1));
        c.release(&a, 2);
        c.release(&b, 2);
        assert_eq!(c.running("q"), 0);
        c.try_admit(&r, 3).expect("released");
    }

    #[test]
    fn unkeyed_entries_are_never_key_limited() {
        let mut c = Counters::new(KeyBounds::default());
        let r = AdmitReq {
            key_concurrency: Some(1),
            key_throttle: Some(g(1, 60_000, 1)),
            ..req("q", "")
        };
        for _ in 0..5 {
            c.try_admit(&r, 1).expect("unkeyed");
        }
        assert_eq!(c.tracked_keys(), 0);
    }

    #[test]
    fn throttle_applies_only_to_first_activations() {
        let mut c = Counters::new(KeyBounds::default());
        let r = AdmitReq {
            throttle: Some(g(1, 1_000, 1)),
            ..req("q", "")
        };
        c.try_admit(&r, 1_000).expect("first");
        assert_eq!(
            c.try_admit(&r, 1_000),
            Err(Block::QueueThrottle(ms_to_ns(2_000)))
        );
        let re = AdmitReq {
            first_activation: false,
            ..r.clone()
        };
        c.try_admit(&re, 1_000)
            .expect("re-activation is not throttled");
        c.try_admit(&r, 2_000).expect("after one interval");
    }

    #[test]
    fn throttle_checks_both_tats_before_advancing_either() {
        let mut c = Counters::new(KeyBounds::default());
        let r = AdmitReq {
            throttle: Some(g(10, 1_000, 2)),
            key_throttle: Some(g(1, 1_000, 1)),
            ..req("q", "k")
        };
        c.try_admit(&r, 1_000).expect("first");
        // Key throttle refuses: the queue TAT must not move.
        assert_eq!(
            c.try_admit(&r, 1_000),
            Err(Block::KeyThrottle(ms_to_ns(2_000)))
        );
        // Queue burst 2: one more (other key) still fits, so the refusal did not charge it.
        c.try_admit(
            &AdmitReq {
                key: "j",
                ..r.clone()
            },
            1_000,
        )
        .expect("queue TAT untouched by the key refusal");
    }

    #[test]
    fn rollback_restores_tat_and_counters_commit_sticks() {
        let mut c = Counters::new(KeyBounds::default());
        let r = AdmitReq {
            concurrency: Some(1),
            throttle: Some(g(1, 1_000, 1)),
            key_throttle: Some(g(1, 1_000, 1)),
            ..req("q", "k")
        };
        let ch = c.try_admit(&r, 1_000).expect("admit");
        c.rollback(&ch, 1_000);
        assert_eq!((c.running("q"), c.key_running("q", "k")), (0, 0));
        // Fully restored: an immediate admission conforms again.
        let ch = c.try_admit(&r, 1_000).expect("after rollback");
        // Committed (released, not rolled back): the TAT stays.
        c.release(&ch, 1_000);
        assert_eq!(c.running("q"), 0);
        assert_eq!(
            c.try_admit(&r, 1_000),
            Err(Block::QueueThrottle(ms_to_ns(2_000)))
        );
    }

    #[test]
    fn rollback_after_a_later_charge_is_conservative() {
        let r = g(1, 1_000, 2); // T = 1 s, tau = 1 s
        let now = ms_to_ns(10_000);
        let tat1 = r.advance(0, now); // 11 s
        let tat2 = r.advance(tat1, now); // 12 s (a later charge)
        // Rolling back the first charge: TAT moved past `written`; reduce by at most T, not
        // below max(prev, now).
        assert_eq!(r.rollback(tat2, 0, tat1, now), tat1);
        // Never below now.
        assert_eq!(r.rollback(now + 500, 0, 1, now), now);
        // Never raised.
        assert_eq!(r.rollback(5, 0, 7, now), 5);
    }

    #[test]
    fn idle_keys_are_evicted_and_the_cap_holds() {
        let bounds = KeyBounds {
            max_tracked_keys: 10,
            idle_evict_ms: 1_000,
        };
        let mut c = Counters::new(bounds);
        let held: Vec<Charge> = (0..5)
            .map(|i| {
                let k = format!("held{i}");
                c.try_admit(&req("q", &k), 0).expect("admit")
            })
            .collect();
        for i in 0..50 {
            let k = format!("k{i}");
            let ch = c.try_admit(&req("q", &k), 0).expect("admit");
            c.release(&ch, 0);
            assert!(c.tracked_keys() <= 10, "cap exceeded: {}", c.tracked_keys());
        }
        // Running cells survive eviction.
        for i in 0..5 {
            assert_eq!(c.key_running("q", &format!("held{i}")), 1);
        }
        // After the idle window only the running cells remain.
        c.try_admit(&req("q", ""), 5_000).expect("sweep tick");
        assert_eq!(c.tracked_keys(), 5);
        for ch in &held {
            c.release(ch, 5_000);
        }
    }

    #[test]
    fn rate_cells_commit_both_or_neither() {
        let mut r = RateCells::new(KeyBounds::default());
        let q = Some(g(3, 60_000, 3));
        let k = Some(g(1, 60_000, 1));
        r.admit("q", "a", q, k, 1_000).expect("1");
        // Key refuses: the queue cell must not be charged.
        assert!(r.admit("q", "a", q, k, 1_000).is_err());
        r.admit("q", "b", q, k, 1_000).expect("2");
        r.admit("q", "c", q, k, 1_000).expect("3");
        // Queue exhausted (3 charged, not 4).
        let until = r.admit("q", "d", q, k, 1_000).expect_err("4th");
        assert_eq!(until, ms_to_ns(1_000 + 20_000));
        // Unkeyed starts skip the key cell.
        assert!(r.admit("q", "", q, k, 1_000).is_err());
        assert_eq!(r.tracked_keys(), 3);
    }
}
