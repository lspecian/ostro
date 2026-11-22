//! Permanent replication failpoint mutants (D-P12-19).
//!
//! Each name is a `fail` failpoint that, when enabled, makes one replication rule
//! deliberately wrong. The plan owning the code path places the site (listed per
//! name) with a unit-level "mutant bites" test; `ostro-repl-conformance`'s
//! `tests/mutants.rs` (12-07) runs each mutant's scenarios without it (must pass)
//! and with it (must fail). The names are fixed here (12-02) and never renamed.

/// The leader takes its ISR from the assignment's `replicas` instead of the
/// published ISR. Placed by 12-04 in `replica/isr.rs`.
pub const ISR_FROM_ASSIGNMENT: &str = "repl::mutant::isr_from_assignment";

/// The HW follows the leader's own LEO, ignoring followers. Placed by 12-04 in
/// `replica/watermark.rs`.
pub const HW_LEADER_LEO: &str = "repl::mutant::hw_leader_leo";

/// A follower's fetch offset (not its fsynced `durable_leo`) counts for the HW.
/// Placed by 12-04 in `replica/tracker.rs` (leader half) and by 12-11 in
/// ostro-cluster `fetcher.rs` (follower half: report the unsynced LEO).
pub const COUNT_NONDURABLE_LEO: &str = "repl::mutant::count_nondurable_leo";

/// The leader skips the KIP-320 epoch fencing / divergence check on `Fetch`.
/// Placed by 12-11 in ostro-cluster `fetch.rs`.
pub const SKIP_EPOCH_CHECK: &str = "repl::mutant::skip_epoch_check";

/// The follower ignores `diverging` and never truncates. Placed by 12-11 in
/// ostro-cluster `fetcher.rs`.
pub const NO_TRUNCATE: &str = "repl::mutant::no_truncate";

/// Failover may elect a replica outside the ISR. Placed by 12-04 in
/// `replica/failover.rs`.
pub const UNCLEAN_ELECTION: &str = "repl::mutant::unclean_election";

/// `min_insync` is ignored: the HW advances (12-04, `replica/watermark.rs`) and
/// appends are accepted (12-08, the partition log's append path) with a too-small ISR.
pub const IGNORE_MIN_INSYNC: &str = "repl::mutant::ignore_min_insync";

/// A shrunk-away replica stops counting for the HW before meta committed the
/// shrink. Placed by 12-04 in `replica/watermark.rs`.
pub const SHRINK_BEFORE_COMMIT: &str = "repl::mutant::shrink_before_commit";

/// The bottomless uploader uploads above the HW. Placed by 12-15 in
/// ostro-backup `uploader.rs`.
pub const UPLOAD_ABOVE_HW: &str = "repl::mutant::upload_above_hw";

/// RF=1 takes a separate shortcut path (HW published straight from the WAL
/// callback, not through the replica tracker). Placed by 12-08 in the partition log.
pub const RF1_BYPASS: &str = "repl::mutant::rf1_bypass";

/// Every mutant name, in declaration order.
pub const ALL: [&str; 10] = [
    ISR_FROM_ASSIGNMENT,
    HW_LEADER_LEO,
    COUNT_NONDURABLE_LEO,
    SKIP_EPOCH_CHECK,
    NO_TRUNCATE,
    UNCLEAN_ELECTION,
    IGNORE_MIN_INSYNC,
    SHRINK_BEFORE_COMMIT,
    UPLOAD_ABOVE_HW,
    RF1_BYPASS,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutant_names_are_unique_and_prefixed() {
        let mut seen = std::collections::BTreeSet::new();
        for name in ALL {
            assert!(name.starts_with("repl::mutant::"), "{name}");
            assert!(
                name["repl::mutant::".len()..]
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "{name}"
            );
            assert!(seen.insert(name), "duplicate {name}");
        }
        assert_eq!(seen.len(), 10);
    }
}
