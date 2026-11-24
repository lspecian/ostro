//! Phase 13 (13-06): pure target replica sets (D-P13-12).
//!
//! [`target_replicas`] is a greedy minimal-move planner that always starts from the current
//! assignment (never from scratch). No I/O, no clock: deterministic for a given input.
//!
//! Passes, each touching only what it must:
//! 1. **clean:** drop non-members (Draining/Removed/unknown nodes) and duplicates; trim
//!    partitions above the effective RF, dropping replicas on the most loaded nodes and
//!    keeping the preferred (first) replica where possible;
//! 2. **fill:** add the least loaded member (ties by id) to partitions below the effective RF;
//! 3. **replica balance:** while the replica counts of two members differ by more than one,
//!    move one replica from the most to the least loaded member (same list position, so the
//!    preferred leader role moves with it);
//! 4. **preferred-leader balance:** while the first-replica counts differ by more than one,
//!    swap the most-preferred node `hi` with the least-preferred `lo` inside a partition
//!    that holds both, or — when none exists — swap `hi`/`lo` membership between a partition
//!    led by `hi` (without `lo`) and one holding `lo` as a follower (without `hi`). Both
//!    keep every replica count; a valid swap always exists while the spread exceeds one.
//!
//! An already balanced, valid assignment yields no moves.

use std::collections::{BTreeMap, BTreeSet};

/// Target replica set per partition for `members` (Active, non-Removed ids) at replication
/// factor `rf` (effective RF = min(rf, members)), starting from `current`.
///
/// Returns only partitions whose replica list changes (order included: the first replica
/// is the preferred leader). Every returned list holds distinct members, exactly
/// `min(max(rf, 1), |members|)` of them; replica counts and preferred-leader counts differ
/// by at most one across `members`; nodes outside `members` end with no replicas. Empty
/// when `members` is empty.
pub fn target_replicas(
    current: &BTreeMap<u16, Vec<u64>>,
    members: &[u64],
    rf: u8,
) -> BTreeMap<u16, Vec<u64>> {
    let ms: BTreeSet<u64> = members.iter().copied().collect();
    if ms.is_empty() {
        return BTreeMap::new();
    }
    let eff = usize::from(rf.max(1)).min(ms.len());
    let mut plan: BTreeMap<u16, Vec<u64>> = current
        .iter()
        .map(|(p, r)| {
            let mut seen = BTreeSet::new();
            let r: Vec<u64> = r
                .iter()
                .copied()
                .filter(|n| ms.contains(n) && seen.insert(*n))
                .collect();
            (*p, r)
        })
        .collect();

    let mut load = Load::new(&ms, &plan);
    trim(&mut plan, &mut load, eff);
    fill(&mut plan, &mut load, &ms, eff);
    balance_replicas(&mut plan, &mut load);
    balance_leaders(&mut plan, &mut load);

    plan.into_iter()
        .filter(|(p, r)| current.get(p) != Some(r))
        .collect()
}

/// Replica and preferred-leader counts per member.
struct Load {
    replicas: BTreeMap<u64, usize>,
    leads: BTreeMap<u64, usize>,
}

impl Load {
    fn new(ms: &BTreeSet<u64>, plan: &BTreeMap<u16, Vec<u64>>) -> Self {
        let mut load = Load {
            replicas: ms.iter().map(|m| (*m, 0)).collect(),
            leads: ms.iter().map(|m| (*m, 0)).collect(),
        };
        for r in plan.values() {
            for n in r {
                *load.replicas.get_mut(n).expect("member") += 1;
            }
            if let Some(first) = r.first() {
                *load.leads.get_mut(first).expect("member") += 1;
            }
        }
        load
    }

    /// `(min, max)` `(node, count)` of `counts`; ties go to the smallest id.
    fn extremes(counts: &BTreeMap<u64, usize>) -> ((u64, usize), (u64, usize)) {
        let mut lo: Option<(u64, usize)> = None;
        let mut hi: Option<(u64, usize)> = None;
        for (&n, &c) in counts {
            if lo.is_none_or(|(_, l)| c < l) {
                lo = Some((n, c));
            }
            if hi.is_none_or(|(_, h)| c > h) {
                hi = Some((n, c));
            }
        }
        (lo.expect("members"), hi.expect("members"))
    }

    fn set_first(&mut self, old: Option<u64>, new: Option<u64>) {
        if let Some(o) = old {
            *self.leads.get_mut(&o).expect("member") -= 1;
        }
        if let Some(n) = new {
            *self.leads.get_mut(&n).expect("member") += 1;
        }
    }
}

/// Trims partitions above `eff`, dropping the replica on the most loaded node (never the
/// preferred replica while another can go).
fn trim(plan: &mut BTreeMap<u16, Vec<u64>>, load: &mut Load, eff: usize) {
    for r in plan.values_mut() {
        while r.len() > eff {
            let (idx, _) = r
                .iter()
                .enumerate()
                .skip(1)
                .max_by_key(|(i, n)| (load.replicas[*n], *i))
                .expect("len > eff ≥ 1");
            let n = r.remove(idx);
            *load.replicas.get_mut(&n).expect("member") -= 1;
        }
    }
}

/// Fills partitions below `eff` with the least loaded member not already present.
fn fill(plan: &mut BTreeMap<u16, Vec<u64>>, load: &mut Load, ms: &BTreeSet<u64>, eff: usize) {
    for r in plan.values_mut() {
        while r.len() < eff {
            let n = ms
                .iter()
                .copied()
                .filter(|n| !r.contains(n))
                .min_by_key(|n| (load.replicas[n], *n))
                .expect("eff ≤ |members|");
            let old_first = r.first().copied();
            r.push(n);
            *load.replicas.get_mut(&n).expect("member") += 1;
            if old_first.is_none() {
                load.set_first(None, Some(n));
            }
        }
    }
}

/// Moves replicas from the most to the least loaded member until counts differ by ≤ 1.
fn balance_replicas(plan: &mut BTreeMap<u16, Vec<u64>>, load: &mut Load) {
    loop {
        let ((lo, lo_c), (hi, hi_c)) = Load::extremes(&load.replicas);
        if hi_c <= lo_c + 1 {
            return;
        }
        // Prefer a partition where `hi` is a follower (the preferred leader stays put);
        // `count(hi) > count(lo)` guarantees some partition holds `hi` but not `lo`.
        let pick = plan
            .iter()
            .filter(|(_, r)| r.contains(&hi) && !r.contains(&lo))
            .min_by_key(|(p, r)| (r.first() == Some(&hi), **p))
            .map(|(p, _)| *p)
            .expect("a partition holds hi but not lo");
        let r = plan.get_mut(&pick).expect("present");
        let idx = r.iter().position(|n| *n == hi).expect("holds hi");
        r[idx] = lo;
        *load.replicas.get_mut(&hi).expect("member") -= 1;
        *load.replicas.get_mut(&lo).expect("member") += 1;
        if idx == 0 {
            load.set_first(Some(hi), Some(lo));
        }
    }
}

/// Swaps preferred-leader roles until first-replica counts differ by ≤ 1, keeping every
/// replica count.
fn balance_leaders(plan: &mut BTreeMap<u16, Vec<u64>>, load: &mut Load) {
    loop {
        let ((lo, lo_c), (hi, hi_c)) = Load::extremes(&load.leads);
        if hi_c <= lo_c + 1 {
            return;
        }
        // Direct: a partition led by `hi` that also holds `lo` → swap their positions.
        let direct = plan
            .iter()
            .find(|(_, r)| r.first() == Some(&hi) && r.contains(&lo))
            .map(|(p, _)| *p);
        if let Some(p) = direct {
            let r = plan.get_mut(&p).expect("present");
            let j = r.iter().position(|n| *n == lo).expect("holds lo");
            r.swap(0, j);
            load.set_first(Some(hi), Some(lo));
            continue;
        }
        // Cross: `lo` follows somewhere without `hi` (it has more follower slots than `hi`
        // has in partitions holding `lo`), and every partition led by `hi` lacks `lo`.
        let led = plan
            .iter()
            .find(|(_, r)| r.first() == Some(&hi))
            .map(|(p, _)| *p)
            .expect("hi leads ≥ 2 partitions");
        let follows = plan
            .iter()
            .find(|(_, r)| r.first() != Some(&lo) && r.contains(&lo) && !r.contains(&hi))
            .map(|(p, _)| *p)
            .expect("lo follows a partition without hi");
        let a = plan.get_mut(&led).expect("present");
        a[0] = lo;
        let b = plan.get_mut(&follows).expect("present");
        let j = b.iter().position(|n| *n == lo).expect("holds lo");
        b[j] = hi;
        load.set_first(Some(hi), Some(lo));
    }
}
