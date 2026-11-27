//! The Phase 13 conformance seam links (13-02): the P13Harness API, the topic scenario lists
//! and the P13 mutant matrix are callable, and every name stays unique.

use std::collections::BTreeSet;

use ostro_repl_conformance::harness::p13::NodeOpts;
use ostro_repl_conformance::{mutants_p13, scenarios};

#[test]
fn p13_topic_lists_are_callable() {
    let topics = [
        scenarios::membership::all(),
        scenarios::reassign::all(),
        scenarios::handoff::all(),
        scenarios::routing::all(),
        scenarios::ryw::all(),
        scenarios::version_gate::all(),
        scenarios::module_gc::all(),
        scenarios::promotion::all(),
        scenarios::migrate::all(),
        scenarios::integration_p13::all(),
    ];
    let all: BTreeSet<&str> = scenarios::all().iter().map(|s| s.name).collect();
    for s in topics.iter().flatten() {
        assert!(
            all.contains(s.name),
            "{} is not in scenarios::all()",
            s.name
        );
    }
    let process: BTreeSet<&str> = scenarios::all_process().iter().map(|s| s.name).collect();
    for s in scenarios::process_p13::all() {
        assert!(
            process.contains(s.name),
            "{} is not in all_process()",
            s.name
        );
    }
}

#[test]
fn p13_mutant_matrix_names_registered_scenarios() {
    let all: BTreeSet<&str> = scenarios::all().iter().map(|s| s.name).collect();
    let process: BTreeSet<&str> = scenarios::all_process().iter().map(|s| s.name).collect();
    for (mutant, names) in mutants_p13::matrix() {
        // 14-33: Phase 14 plans append their mutants after the eight Phase 13 ones.
        assert!(
            ostro_types::compat::MUTANTS.contains(&mutant)
                || mutants_p13::P14_MUTANTS.contains(&mutant),
            "{mutant} is not a Phase 13 mutant (nor a Phase 14 addition)"
        );
        for n in names {
            assert!(
                all.contains(n) || process.contains(n),
                "{mutant}: {n} is not a scenario"
            );
        }
    }
}

#[test]
fn node_opts_default_is_this_build() {
    let o = NodeOpts::default();
    assert_eq!((o.level_max, o.advertise), (None, false));
}
