//! The permanent mutant matrix (12-07, D-P12-19, CLUS-01 "shown to fail on a deliberately
//! broken build").
//!
//! Every name in [`ostro_log::mutants::ALL`] maps to the scenarios that must fail when that
//! failpoint is enabled. `tests/mutants.rs` (feature `failpoints`, single-threaded, one whole
//! cluster at a time) runs each non-pending tier-B mapping twice: without the mutant (must
//! pass) and with `fail::cfg(<mutant>, "return")` (must fail with a measured
//! `ScenarioFailure`; a harness error or deadline never counts as the mutant biting).
//! Pending scenarios are reported `SKIP (pending)`; with `OSTRO_REPL_REQUIRE_ALL=1` a pending
//! skip is a failure. `count_nondurable_leo` is tier C only: v1 followers fetch after
//! durability, so only a per-process WAL pause plus `kill -9` separates the fetched LEO from
//! the durable one (12-17 `process_mutants.rs`).

use ostro_log::mutants;

/// One mutant and the scenarios that catch it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutantMap {
    /// The failpoint name (`repl::mutant::*`, from [`ostro_log::mutants`]).
    pub mutant: &'static str,
    /// Tier-B scenarios (registered in [`crate::scenarios::all`]) that must fail with it.
    pub tier_b: &'static [&'static str],
    /// The tier-C scenario that catches it when tier B cannot (12-17).
    pub tier_c: Option<&'static str>,
}

const ISR_FROM_ASSIGNMENT: &[&str] = &["isr_empty_follower_not_in_isr"];
const HW_LEADER_LEO: &[&str] = &["leader_dies_with_unreplicated_writes", "hw_is_min_over_isr"];
const COUNT_NONDURABLE_LEO: &[&str] = &[];
const SKIP_EPOCH_CHECK: &[&str] = &["fetch_with_stale_epoch_fenced"];
const NO_TRUNCATE: &[&str] = &["leader_dies_with_unreplicated_writes"];
const UNCLEAN_ELECTION: &[&str] = &["failover_elects_isr_member", "whole_isr_dead_goes_offline"];
const IGNORE_MIN_INSYNC: &[&str] = &["writes_stop_below_min_insync"];
const SHRINK_BEFORE_COMMIT: &[&str] = &["isr_slow_follower_shrinks"];
const UPLOAD_ABOVE_HW: &[&str] = &["remote_chain_never_above_hw"];
const RF1_BYPASS: &[&str] = &["one_path_rf1_hw_through_tracker"];

/// The matrix, in [`ostro_log::mutants::ALL`] order.
pub const MAP: &[MutantMap] = &[
    MutantMap {
        mutant: mutants::ISR_FROM_ASSIGNMENT,
        tier_b: ISR_FROM_ASSIGNMENT,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::HW_LEADER_LEO,
        tier_b: HW_LEADER_LEO,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::COUNT_NONDURABLE_LEO,
        tier_b: COUNT_NONDURABLE_LEO,
        tier_c: Some("follower_power_loss"),
    },
    MutantMap {
        mutant: mutants::SKIP_EPOCH_CHECK,
        tier_b: SKIP_EPOCH_CHECK,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::NO_TRUNCATE,
        tier_b: NO_TRUNCATE,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::UNCLEAN_ELECTION,
        tier_b: UNCLEAN_ELECTION,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::IGNORE_MIN_INSYNC,
        tier_b: IGNORE_MIN_INSYNC,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::SHRINK_BEFORE_COMMIT,
        tier_b: SHRINK_BEFORE_COMMIT,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::UPLOAD_ABOVE_HW,
        tier_b: UPLOAD_ABOVE_HW,
        tier_c: None,
    },
    MutantMap {
        mutant: mutants::RF1_BYPASS,
        tier_b: RF1_BYPASS,
        tier_c: None,
    },
];

/// `(mutant failpoint name, tier-B scenarios that must fail with it)`: the 12-02 view of
/// [`MAP`] (kept for the registry checks in `tests/skeleton.rs`).
pub const MATRIX: &[(&str, &[&str])] = &[
    (mutants::ISR_FROM_ASSIGNMENT, ISR_FROM_ASSIGNMENT),
    (mutants::HW_LEADER_LEO, HW_LEADER_LEO),
    (mutants::COUNT_NONDURABLE_LEO, COUNT_NONDURABLE_LEO),
    (mutants::SKIP_EPOCH_CHECK, SKIP_EPOCH_CHECK),
    (mutants::NO_TRUNCATE, NO_TRUNCATE),
    (mutants::UNCLEAN_ELECTION, UNCLEAN_ELECTION),
    (mutants::IGNORE_MIN_INSYNC, IGNORE_MIN_INSYNC),
    (mutants::SHRINK_BEFORE_COMMIT, SHRINK_BEFORE_COMMIT),
    (mutants::UPLOAD_ABOVE_HW, UPLOAD_ABOVE_HW),
    (mutants::RF1_BYPASS, RF1_BYPASS),
];

/// The mapping of `mutant`.
pub fn entry(mutant: &str) -> Option<&'static MutantMap> {
    MAP.iter().find(|m| m.mutant == mutant)
}

/// Total `(mutant, tier-B scenario)` pairs.
pub fn tier_b_pairs() -> usize {
    MAP.iter().map(|m| m.tier_b.len()).sum()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::scenarios::{self, Tier};

    #[test]
    fn mutants_map_covers_exactly_all() {
        let keys: BTreeSet<&str> = MAP.iter().map(|m| m.mutant).collect();
        let all: BTreeSet<&str> = mutants::ALL.into_iter().collect();
        assert_eq!(keys, all);
        assert_eq!(MAP.len(), mutants::ALL.len(), "one entry per mutant");
        let order: Vec<&str> = MAP.iter().map(|m| m.mutant).collect();
        assert_eq!(order, mutants::ALL, "MAP follows ALL order");
    }

    #[test]
    fn mutants_tier_b_pairs_are_registered_tier_b_scenarios() {
        assert_eq!(tier_b_pairs(), 11);
        let all = scenarios::all();
        for m in MAP {
            assert!(
                !m.tier_b.is_empty() || m.tier_c.is_some(),
                "{} has no scenario",
                m.mutant
            );
            for name in m.tier_b {
                let s = all
                    .iter()
                    .find(|s| s.name == *name)
                    .unwrap_or_else(|| panic!("{}: {name} is not a scenario", m.mutant));
                assert_eq!(s.tier, Tier::B, "{name}");
            }
        }
        let cn = entry(mutants::COUNT_NONDURABLE_LEO).expect("mapped");
        assert!(cn.tier_b.is_empty());
        assert_eq!(cn.tier_c, Some("follower_power_loss"));
    }

    #[test]
    fn mutants_matrix_mirrors_map() {
        assert_eq!(MATRIX.len(), MAP.len());
        for ((name, list), m) in MATRIX.iter().zip(MAP) {
            assert_eq!(*name, m.mutant);
            assert_eq!(*list, m.tier_b);
        }
    }
}
