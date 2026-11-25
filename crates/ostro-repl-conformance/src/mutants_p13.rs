//! The Phase 13 mutant matrix (D-P13-25, 13-18): each of the eight
//! `ostro_types::compat::MUTANTS` failpoints → the tier-B scenarios that must catch it.
//!
//! `tests/mutants_p13.rs` (feature `failpoints`, one test, one whole cluster at a time) runs,
//! per mutant, every mapped scenario without the mutant (must pass), then with
//! `fail::cfg(<mutant>, "return")` until one fails with a measured `ScenarioFailure`
//! (printed `FAILED as expected`; a harness error or a deadline never counts as biting). A
//! mutant none of whose scenarios bites, or with no runnable scenario (`SKIP`), fails the
//! matrix when `OSTRO_P13_REQUIRE_ALL=1` (13-20 makes that mandatory in CI).
//!
//! Scenario names resolve through [`find`]: the registered tier-B scenarios
//! ([`crate::scenarios::all`]; `tests/p13_skeleton.rs` requires every name there) plus
//! [`wrappers`] (none today).
//!
//! | mutant | scenarios | why they catch it |
//! |--------|-----------|-------------------|
//! | `upgrade::mutant::emit_ungated` | `move_blocked_before_finalize`, `migrate_refused_before_finalize` | `compat::require` lets every feature through: a level-1 cluster's reassign / rebalance / migrate are no longer refused 409, AlterReplicas / RepinRun reach the logs before finalization (upgrade oracle). The upload gate compares `min_level > finalized` itself, so 13-14's `gated_upload_refused_before_finalize` is not mapped |
//! | `upgrade::mutant::reject_and_continue` | `reject_continue_never_applies_newer_commands` | a newer-format command is rejected `Invalid(Empty)` and apply moves on instead of fail-stopping (`no_reject_continue` over the replayed outcome stream) |
//! | `member::mutant::skip_quorum_check` | `force_remove_dead_node` | the meta leader counts every member live: the removal that would leave no live quorum is no longer refused |
//! | `member::mutant::no_leave_joint` | `join_grows_to_five_voters`, `meta_leader_removal_transfers_first` | the joint configuration is never left (5 voters never settle; `joint_left_within`) |
//! | `reassign::mutant::raise_min_insync_early` | `rf_raised_on_join`, `promote_running_single_node_to_three` | the Expand from RF 1 with min_insync 2 is refused at apply: RF never rises (13-10, 13-16) |
//! | `route::mutant::serve_without_lease` | `zombie_leader_never_serves_stale` | the zombie keeps serving reads past its lease: the RYW checker reports a stale read |
//! | `gc::mutant::single_scan` | `gc_survives_leader_changes` | scan 2 never runs: the round is not postponed across the leader change between the scans |
//! | `migrate::mutant::skip_seq_check` | `concurrent_growth_seq_mismatch` | a run that grew since the replay check is repinned instead of reported `seq_mismatch` |

use crate::scenarios::{self, Scenario};

/// One mutant and the scenarios that catch it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutantMap {
    /// The failpoint name (one of `ostro_types::compat::MUTANTS`).
    pub mutant: &'static str,
    /// Tier-B scenarios ([`find`]) of which at least one must fail with it.
    pub scenarios: &'static [&'static str],
}

/// The matrix, in `ostro_types::compat::MUTANTS` order (module docs).
pub const MAP: &[MutantMap] = &[
    MutantMap {
        mutant: "upgrade::mutant::emit_ungated",
        scenarios: &[
            "move_blocked_before_finalize",
            "migrate_refused_before_finalize",
        ],
    },
    MutantMap {
        mutant: "upgrade::mutant::reject_and_continue",
        scenarios: &["reject_continue_never_applies_newer_commands"],
    },
    MutantMap {
        mutant: "member::mutant::skip_quorum_check",
        scenarios: &["force_remove_dead_node"],
    },
    MutantMap {
        mutant: "member::mutant::no_leave_joint",
        scenarios: &[
            "join_grows_to_five_voters",
            "meta_leader_removal_transfers_first",
        ],
    },
    MutantMap {
        mutant: "reassign::mutant::raise_min_insync_early",
        scenarios: &["rf_raised_on_join", "promote_running_single_node_to_three"],
    },
    MutantMap {
        mutant: "route::mutant::serve_without_lease",
        scenarios: &["zombie_leader_never_serves_stale"],
    },
    MutantMap {
        mutant: "gc::mutant::single_scan",
        scenarios: &["gc_survives_leader_changes"],
    },
    MutantMap {
        mutant: "migrate::mutant::skip_seq_check",
        scenarios: &["concurrent_growth_seq_mismatch"],
    },
];

/// `(mutant, scenarios)` pairs (the 13-02 stub's shape).
pub fn matrix() -> Vec<(&'static str, Vec<&'static str>)> {
    MAP.iter()
        .map(|m| (m.mutant, m.scenarios.to_vec()))
        .collect()
}

/// The mapping of `mutant`.
pub fn entry(mutant: &str) -> Option<&'static MutantMap> {
    MAP.iter().find(|m| m.mutant == mutant)
}

/// Matrix-only scenarios (bodies that start their own cluster and are not in the
/// registry). None today: 13-17's `migrate_refused_before_finalize` is registered in
/// `scenarios::migrate::all` (13-18), as `tests/p13_skeleton.rs` requires of matrix names.
pub fn wrappers() -> Vec<Scenario> {
    Vec::new()
}

/// A tier-B scenario by name: the registry first, then [`wrappers`].
pub fn find(name: &str) -> Option<Scenario> {
    scenarios::all()
        .into_iter()
        .chain(wrappers())
        .find(|s| s.name == name)
}

#[cfg(test)]
mod tests {
    use ostro_types::compat::MUTANTS;

    use super::*;
    use crate::scenarios::Tier;

    #[test]
    fn map_covers_exactly_the_eight_mutants_in_order() {
        let names: Vec<&str> = MAP.iter().map(|m| m.mutant).collect();
        assert_eq!(names, MUTANTS, "MAP follows ostro_types::compat::MUTANTS");
        assert_eq!(matrix().len(), 8);
    }

    #[test]
    fn every_mapped_scenario_resolves_to_a_tier_b_scenario() {
        for m in MAP {
            assert!(!m.scenarios.is_empty(), "{} has no scenario", m.mutant);
            for name in m.scenarios {
                let s = find(name).unwrap_or_else(|| panic!("{}: no scenario {name}", m.mutant));
                assert_eq!(s.tier, Tier::B, "{name}");
            }
        }
    }

    #[test]
    fn mutant_names_match_their_sites() {
        assert_eq!(
            MAP[0].mutant,
            ostro_cluster::compat::MUTANT_EMIT_UNGATED,
            "emit_ungated"
        );
        assert_eq!(
            MAP[1].mutant,
            ostro_partition::apply::MUTANT_REJECT_AND_CONTINUE
        );
        assert_eq!(
            MAP[2].mutant,
            ostro_cluster::membership::service::MUTANT_SKIP_QUORUM_CHECK
        );
        assert_eq!(
            MAP[3].mutant,
            ostro_cluster::membership::service::MUTANT_NO_LEAVE_JOINT
        );
        assert_eq!(
            MAP[4].mutant,
            ostro_cluster::placement::controller::MUTANT_RAISE_MIN_INSYNC_EARLY
        );
        assert_eq!(
            MAP[5].mutant,
            ostro_server::route::consistency::MUTANT_SERVE_WITHOUT_LEASE
        );
        assert_eq!(
            MAP[6].mutant,
            ostro_server::p13::module_gc::MUTANT_SINGLE_SCAN
        );
        assert_eq!(
            MAP[7].mutant,
            ostro_server::control::migrate::MUTANT_SKIP_SEQ_CHECK
        );
    }
}
