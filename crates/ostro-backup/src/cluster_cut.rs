//! Consistent-cut search over per-partition cross-partition cursor histories (plan 14-29,
//! D-P14-45): the cluster replacement of the single-node HW cut.
//!
//! A cluster bucket holds one chain per partition, each ending at its own head `end`, with no
//! common cut. Restoring every partition at its head can break Phase 8 Pitfall 1 for a pair
//! `A -> B`:
//!
//! ```text
//! outbox_truncated[A->B](c_A)  <=  inbound_seen_B[A](c_B)  <  outbox_next_seq[A->B](c_A)
//! ```
//!
//! (B saw a message A has not sent at its cut; or A dropped outbox rows B has not seen, so they
//! can never be redelivered). [`search`] finds the **greatest** cut `{c_p}` with
//! `floor_p <= c_p <= end_p` that satisfies Pitfall 1 for every ordered pair, from per-offset
//! step functions of the three cursors (recorded by replaying each partition's chain,
//! `ostro_partition::replay_observer`, and compressed by [`steps_from_replay`]).
//!
//! # Algorithm (greatest fixpoint)
//!
//! Start at `c_p = end_p` and repeat until nothing changes, for every ordered pair `(A, B)`:
//!
//! 1. if `seen_B[A](c_B) >= next[A->B](c_A)`, lower `c_B` to the largest offset in
//!    `[floor_B, c_B]` where `seen_B[A] < next[A->B](c_A)`;
//! 2. if `truncated[A->B](c_A) > seen_B[A](c_B)`, lower `c_A` to the largest offset in
//!    `[floor_A, c_A]` where `truncated[A->B] <= seen_B[A](c_B)`.
//!
//! No such offset means the partition would be lowered below its restorable floor:
//! [`CutError::NoConsistentCut`]. Every lowering is forced: the cursors are monotone in the
//! offset, so for any consistent cut `d <= c`, rule 1 keeps `d_B <= c_B'` (because
//! `seen_B[A](d_B) < next[A->B](d_A) <= next[A->B](c_A)`) and rule 2 keeps `d_A <= c_A'`
//! (because `truncated[A->B](d_A) <= seen_B[A](d_B) <= seen_B[A](c_B)`). The loop only lowers
//! and stops, so its result is consistent and dominates every consistent cut below the heads
//! (consistent + maximal + bounded; property-tested in `tests/cluster_cut.rs`).
//!
//! Offsets are cut positions: the value of a step function at `c` is the cursor after applying
//! every command below `c` (`cursor.next == c`), i.e. a partition restored at `c` holds
//! `[.., c)`.

use std::collections::{BTreeMap, BTreeSet};

pub use ostro_partition::replay_observer::CursorSample;

/// Default of `outbox_next_seq[->q]` (sequence numbers start at 1).
pub const NEXT_DEFAULT: u64 = 1;
/// Default of `outbox_truncated[->q]` and `inbound_seen[<-q]`.
pub const ZERO_DEFAULT: u64 = 0;

/// A step function of one cursor over cut offsets: sorted `(offset, value)` entries; the value
/// at `c` is the value of the last entry with `offset <= c`, else the cursor's default.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepFn(pub Vec<(u64, u64)>);

impl StepFn {
    /// The value at cut `c`.
    pub fn at(&self, c: u64, default: u64) -> u64 {
        let i = self.0.partition_point(|(o, _)| *o <= c);
        if i == 0 { default } else { self.0[i - 1].1 }
    }

    /// The largest `c` in `[lo, hi]` whose value satisfies `ok`, if any (the value is constant
    /// between entries, so this scans intervals from `hi` down).
    pub fn largest_where(
        &self,
        lo: u64,
        hi: u64,
        default: u64,
        ok: impl Fn(u64) -> bool,
    ) -> Option<u64> {
        if lo > hi {
            return None;
        }
        let mut top = hi;
        let mut i = self.0.partition_point(|(o, _)| *o <= hi);
        loop {
            // The interval holding `top` starts at entry i-1 (or at 0 with the default).
            let (start, v) = if i == 0 { (0, default) } else { self.0[i - 1] };
            if ok(v) {
                return Some(top);
            }
            if start <= lo || i == 0 {
                return None;
            }
            top = start - 1;
            i -= 1;
        }
    }

    /// Append `(offset, value)` unless the value did not change (entries only on change).
    pub fn push_change(&mut self, offset: u64, value: u64, default: u64) {
        let prev = self.0.last().map_or(default, |(_, v)| *v);
        if prev == value {
            return;
        }
        match self.0.last_mut() {
            Some(last) if last.0 == offset => last.1 = value,
            _ => self.0.push((offset, value)),
        }
    }

    /// The smallest entry offset above `c`, if any.
    pub fn next_step_after(&self, c: u64) -> Option<u64> {
        let i = self.0.partition_point(|(o, _)| *o <= c);
        self.0.get(i).map(|(o, _)| *o)
    }
}

/// One partition's restorable range and cross-partition cursor histories.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CursorSteps {
    /// Lowest restorable cut (the oldest reachable remote snapshot / chain start).
    pub floor: u64,
    /// Highest restorable cut (the head's `end`).
    pub end: u64,
    /// `outbox_next_seq[->q]` by target `q` (default 1).
    pub next: BTreeMap<u16, StepFn>,
    /// `outbox_truncated[->q]` by target `q` (default 0).
    pub truncated: BTreeMap<u16, StepFn>,
    /// `inbound_seen[<-q]` by source `q` (default 0).
    pub seen: BTreeMap<u16, StepFn>,
}

impl CursorSteps {
    /// `outbox_next_seq[->to](c)`.
    pub fn next_at(&self, to: u16, c: u64) -> u64 {
        self.next
            .get(&to)
            .map_or(NEXT_DEFAULT, |s| s.at(c, NEXT_DEFAULT))
    }

    /// `outbox_truncated[->to](c)`.
    pub fn truncated_at(&self, to: u16, c: u64) -> u64 {
        self.truncated
            .get(&to)
            .map_or(ZERO_DEFAULT, |s| s.at(c, ZERO_DEFAULT))
    }

    /// `inbound_seen[<-from](c)`.
    pub fn seen_at(&self, from: u16, c: u64) -> u64 {
        self.seen
            .get(&from)
            .map_or(ZERO_DEFAULT, |s| s.at(c, ZERO_DEFAULT))
    }

    /// Every entry offset of every cursor of this partition, ascending.
    pub fn step_offsets(&self) -> BTreeSet<u64> {
        self.next
            .values()
            .chain(self.truncated.values())
            .chain(self.seen.values())
            .flat_map(|s| s.0.iter().map(|(o, _)| *o))
            .collect()
    }
}

/// Why [`search`] lowered a partition's cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lowering {
    /// Rule 1: the partition had seen a message from `from` that `from` had not sent at its
    /// cut.
    Unsent {
        /// The sender.
        from: u16,
    },
    /// Rule 2: the partition had truncated its outbox to `to` beyond what `to` had seen at its
    /// cut.
    Untruncated {
        /// The receiver.
        to: u16,
    },
}

/// The result of [`search`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Cut {
    /// The chosen cut per partition.
    pub offsets: BTreeMap<u16, u64>,
    /// Every lowering in order: `(partition, from, to, reason)`.
    pub lowered: Vec<(u16, u64, u64, Lowering)>,
}

/// [`search`] failures.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CutError {
    /// Pitfall 1 cannot hold without lowering these partitions below their floors.
    #[error("no consistent cut: partitions {partitions:?} would be lowered below their floor")]
    NoConsistentCut {
        /// The partitions whose floor would be crossed.
        partitions: Vec<u16>,
    },
}

/// Pitfall 1 at `cut` for every ordered pair (the semantics of the Phase 8 restore check).
pub fn check(parts: &BTreeMap<u16, CursorSteps>, cut: &BTreeMap<u16, u64>) -> Result<(), String> {
    for (&a, sa) in parts {
        let ca = cut
            .get(&a)
            .copied()
            .ok_or_else(|| format!("no cut for {a}"))?;
        for (&b, sb) in parts {
            if a == b {
                continue;
            }
            let cb = cut
                .get(&b)
                .copied()
                .ok_or_else(|| format!("no cut for {b}"))?;
            let next = sa.next_at(b, ca);
            let truncated = sa.truncated_at(b, ca);
            let seen = sb.seen_at(a, cb);
            if seen >= next {
                return Err(format!(
                    "partition {b}@{cb} saw seq {seen} from partition {a}@{ca}, whose next seq is {next}"
                ));
            }
            if truncated > seen {
                return Err(format!(
                    "partition {a}@{ca} truncated its outbox to {b} up to {truncated}, but {b}@{cb} only saw {seen}"
                ));
            }
        }
    }
    Ok(())
}

/// The greatest consistent cut at or below every head end (see the module docs).
pub fn search(parts: &BTreeMap<u16, CursorSteps>) -> Result<Cut, CutError> {
    let mut cut = Cut {
        offsets: parts.iter().map(|(p, s)| (*p, s.end)).collect(),
        lowered: Vec::new(),
    };
    let below: Vec<u16> = parts
        .iter()
        .filter(|(_, s)| s.floor > s.end)
        .map(|(p, _)| *p)
        .collect();
    if !below.is_empty() {
        return Err(CutError::NoConsistentCut { partitions: below });
    }
    let fail = |p: u16| CutError::NoConsistentCut {
        partitions: vec![p],
    };
    loop {
        let mut changed = false;
        for (&a, sa) in parts {
            for (&b, sb) in parts {
                if a == b {
                    continue;
                }
                // Rule 1: B saw what A has not sent at c_A.
                let (ca, cb) = (cut.offsets[&a], cut.offsets[&b]);
                let next = sa.next_at(b, ca);
                if sb.seen_at(a, cb) >= next {
                    let seen = sb.seen.get(&a).cloned().unwrap_or_default();
                    let to = seen
                        .largest_where(sb.floor, cb, ZERO_DEFAULT, |v| v < next)
                        .ok_or_else(|| fail(b))?;
                    cut.offsets.insert(b, to);
                    cut.lowered.push((b, cb, to, Lowering::Unsent { from: a }));
                    changed = true;
                }
                // Rule 2: A truncated what B has not seen at c_B.
                let (ca, cb) = (cut.offsets[&a], cut.offsets[&b]);
                let seen = sb.seen_at(a, cb);
                if sa.truncated_at(b, ca) > seen {
                    let tr = sa.truncated.get(&b).cloned().unwrap_or_default();
                    let to = tr
                        .largest_where(sa.floor, ca, ZERO_DEFAULT, |v| v <= seen)
                        .ok_or_else(|| fail(a))?;
                    cut.offsets.insert(a, to);
                    cut.lowered
                        .push((a, ca, to, Lowering::Untruncated { to: b }));
                    changed = true;
                }
            }
        }
        if !changed {
            return Ok(cut);
        }
    }
}

fn push_map(
    out: &mut BTreeMap<u16, StepFn>,
    keys: &BTreeSet<u16>,
    at: u64,
    m: &BTreeMap<u16, u64>,
    default: u64,
) {
    for k in keys {
        let v = m.get(k).copied().unwrap_or(default);
        out.entry(*k).or_default().push_change(at, v, default);
    }
}

/// Compress a replay's samples (ascending `at`; the first one is the start state at `floor`)
/// into [`CursorSteps`] with entries only where a value changed. A key absent from a sample
/// has its default value there.
pub fn steps_from_replay(samples: &[CursorSample], floor: u64, end: u64) -> CursorSteps {
    let mut steps = CursorSteps {
        floor,
        end,
        ..CursorSteps::default()
    };
    let keys = |f: fn(&CursorSample) -> &BTreeMap<u16, u64>| -> BTreeSet<u16> {
        samples.iter().flat_map(|s| f(s).keys().copied()).collect()
    };
    let (kn, kt, ks) = (keys(|s| &s.next), keys(|s| &s.truncated), keys(|s| &s.seen));
    for s in samples {
        push_map(&mut steps.next, &kn, s.at, &s.next, NEXT_DEFAULT);
        push_map(&mut steps.truncated, &kt, s.at, &s.truncated, ZERO_DEFAULT);
        push_map(&mut steps.seen, &ks, s.at, &s.seen, ZERO_DEFAULT);
    }
    steps.next.retain(|_, s| !s.0.is_empty());
    steps.truncated.retain(|_, s| !s.0.is_empty());
    steps.seen.retain(|_, s| !s.0.is_empty());
    steps
}
