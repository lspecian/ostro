//! High watermark advance (KIP-497 maximal ISR + `min_insync` freeze, D-P12-5).
//!
//! [`compute_hw`] stays the single HW minimum; [`next_hw`] chooses *who* counts (the maximal
//! ISR), freezes the HW below `min_insync`, and keeps it monotone on one leader.

use ostro_types::Offset;

use super::isr::{IsrState, maximal};
use crate::hw::{ReplicaProgress, compute_hw};
use crate::mutants;

/// Whether appends are accepted: `|published ISR| >= min_insync` (Kafka
/// `NotEnoughReplicas`). Pending adds do not count.
pub fn append_allowed(s: &IsrState, min_insync: usize) -> bool {
    if super::mutant(mutants::IGNORE_MIN_INSYNC) {
        return true;
    }
    s.published.len() >= min_insync
}

/// The next HW: `max(prev_hw, min(durable LEO over maximal ISR))`, or `prev_hw` while
/// `|published| < min_insync`.
///
/// `progress` holds measured durable LEOs (its `in_isr` flags are ignored and recomputed
/// from `s`); a maximal-ISR member without a progress row counts as offset 0, so it holds
/// the HW rather than being skipped.
pub fn next_hw(
    prev_hw: Offset,
    progress: &[ReplicaProgress],
    s: &IsrState,
    min_insync: usize,
) -> Offset {
    if super::mutant(mutants::HW_LEADER_LEO) {
        // The leader's durable LEO is the largest one it tracks.
        return progress.iter().map(|p| p.leo).max().unwrap_or(prev_hw);
    }
    if s.published.len() < min_insync && !super::mutant(mutants::IGNORE_MIN_INSYNC) {
        return prev_hw;
    }
    let members = maximal(s);
    if members.is_empty() {
        return prev_hw;
    }
    let mut rows: Vec<ReplicaProgress> = progress
        .iter()
        .map(|p| ReplicaProgress {
            in_isr: members.contains(&p.node),
            ..*p
        })
        .collect();
    for m in &members {
        if !progress.iter().any(|p| p.node == *m) {
            rows.push(ReplicaProgress {
                node: *m,
                leo: Offset::ZERO,
                in_isr: true,
            });
        }
    }
    compute_hw(&rows).max(prev_hw)
}
