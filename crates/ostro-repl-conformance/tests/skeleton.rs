//! The conformance crate links and its registry is callable (12-02).

use ostro_repl_conformance::harness::ClusterSpec;
use ostro_repl_conformance::{mutants, pending, scenarios};

#[test]
fn registry_is_callable_and_names_are_unique() {
    let all = scenarios::all();
    let mut names = std::collections::BTreeSet::new();
    for s in &all {
        assert!(names.insert(s.name), "duplicate scenario {}", s.name);
    }
    // Every pending entry and mutant-matrix entry names a registered scenario.
    for list in pending::all_lists() {
        for name in list {
            assert!(names.contains(name), "pending {name} is not a scenario");
        }
    }
    for (_, list) in mutants::MATRIX {
        for name in *list {
            assert!(
                names.contains(name),
                "mutant scenario {name} is not a scenario"
            );
        }
    }
    assert!(!pending::is_pending("no-such-scenario"));
}

#[test]
fn spec_defaults() {
    let s = ClusterSpec::default();
    assert_eq!((s.nodes, s.rf, s.min_insync, s.partitions), (3, 3, 2, 4));
    assert_eq!(ClusterSpec::single().rf, 1);
    assert_eq!(scenarios::DEFAULT_DEADLINE.as_secs(), 120);
}

/// Run by 12-17 / 12-18: every pending list is empty at the end of Phase 12.
#[test]
#[ignore = "run by 12-17/12-18 once every scenario is integrated"]
fn no_pending_left() {
    for list in pending::all_lists() {
        assert!(list.is_empty(), "still pending: {list:?}");
    }
}
