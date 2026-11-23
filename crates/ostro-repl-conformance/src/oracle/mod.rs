//! Measured replication oracles (12-03). Each returns a report naming the culprit; a
//! scenario turns a failed oracle into a `ScenarioFailure { oracle, detail }`.
//!
//! Every oracle reads measured state, never a node's claim about itself: offline verified
//! WAL scans ([`disk::scan_partition`]), state-store digests opened on copies
//! ([`standby`]), the committed metadata Raft log (`harness::inproc::read_meta_log`), the
//! remote bucket ([`objstore`]). The one oracle that reads a self-report ([`status`])
//! checks it against measured values.
//!
//! | oracle | inputs | checks |
//! |--------|--------|--------|
//! | [`disk::acked_on_disk`] | data dirs, acks, `min_insync` | every acked run's command is in >= `min_insync` nodes' verified WAL |
//! | [`disk::acked_on_disk_with`] | as above + a state-store probe | same, except a copy whose WAL retention trimmed may evidence a run by its checkpointed state store (user-approved 2026-11-23) |
//! | [`replica::identical_below_hw`] | data dirs, partition, HW | envelopes below HW byte-identical on every node (F1) |
//! | [`standby::equal_state`] | data dirs, partition | equal `applied_offset` ⇒ equal `state_hash` and per-table digests |
//! | [`la_rule::check`] | data dir, partition, meta log | each epoch starts with `LeaderAnnounced{e}` of meta's leader for e |
//! | [`election::isr_only`] | meta log | every proposed election stays inside the published ISR; no re-placement; no `SetPartitionLeader` after bootstrap |
//! | [`objstore::chain_equals_committed`] | bucket, partition, committed records, HW | the reachable chain equals the committed log prefix byte for byte |
//! | [`objstore::nothing_above`] | bucket, partition, HW | no chunk object ends above HW |
//! | [`cluster_formed::check`] | the running cluster, spec | meta voters, RF, epochs, leaders, LA (measured precondition) |
//! | [`status::honest`] | `/admin` JSON, meta ISR, measured LEOs, HW | the reported ISR is the published one and every member really holds HW |

pub mod cluster_formed;
pub mod disk;
pub mod election;
pub mod la_rule;
pub mod objstore;
pub mod replica;
pub mod standby;
pub mod status;

use crate::scenarios::{ScenarioFailure, ScenarioResult};

/// Longest detail kept in a [`ScenarioFailure`] (the full list stays in the report).
const MAX_DETAIL: usize = 2000;

/// What one oracle found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// Oracle name (`"disk"`, `"replica"`, ...).
    pub oracle: &'static str,
    /// One line per violation, naming the culprit (node, partition, offset, run id).
    pub violations: Vec<String>,
}

impl Report {
    /// An empty (passing) report.
    pub fn new(oracle: &'static str) -> Self {
        Self {
            oracle,
            violations: Vec::new(),
        }
    }

    /// Record a violation.
    pub fn violation(&mut self, v: impl Into<String>) {
        self.violations.push(v.into());
    }

    /// No violation.
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }

    /// Whether any violation contains `needle` (self-tests).
    pub fn mentions(&self, needle: &str) -> bool {
        self.violations.iter().any(|v| v.contains(needle))
    }

    /// `Ok` or a `ScenarioFailure { oracle, detail }` listing the violations.
    pub fn into_result(self) -> ScenarioResult {
        if self.violations.is_empty() {
            return Ok(());
        }
        let mut detail = self.violations.join("; ");
        if detail.len() > MAX_DETAIL {
            let mut cut = MAX_DETAIL;
            while !detail.is_char_boundary(cut) {
                cut -= 1;
            }
            detail.truncate(cut);
            detail.push_str(&format!(" … ({} violations)", self.violations.len()));
        }
        Err(ScenarioFailure {
            oracle: self.oracle,
            detail,
        })
    }
}

impl std::fmt::Display for Report {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.violations.is_empty() {
            write!(f, "{}: ok", self.oracle)
        } else {
            write!(f, "{}: {}", self.oracle, self.violations.join("; "))
        }
    }
}

/// Copy a directory tree (oracles open state stores on copies, never on a node's dir).
pub(crate) fn copy_dir(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dst = to.join(entry.file_name());
        if ty.is_dir() {
            copy_dir(&entry.path(), &dst)?;
        } else if ty.is_file() {
            // A file removed by a live node between listing and copy is skipped.
            match std::fs::copy(entry.path(), &dst) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}
