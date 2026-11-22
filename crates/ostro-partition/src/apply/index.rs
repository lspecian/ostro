//! `x_run_index` maintenance hooks (Phase 11, OPS-05, D-P11-9/D-P11-23; RESERVATIONS.md §6
//! derived keyspaces, PF-11-2). Contract: 11-02; implemented by 11-05.
//!
//! The derived keyspace sits outside TABLES, the digest, OSNP and blob GC, so these hooks can
//! never change the state hash. They only write (no reads, so apply's read budget is
//! unchanged) and write nothing for Ready/Running transitions. Every key comes from
//! [`derived::run_index_rows`] (the rebuild and the property test use the same function), and
//! writes are idempotent so they converge with a concurrent rebuild.

use ostro_types::RunId;

use crate::store::ApplyBatch;
use crate::store::derived;
use crate::store::values::RunRecord;

/// A run was created (`start_run`, every creation path): All, Tag, Workflow, Tenant rows.
pub(crate) fn on_start(b: &mut ApplyBatch<'_>, run: &RunId, rec: &RunRecord) {
    for k in derived::start_rows(run, rec) {
        b.insert_derived(k);
    }
}

/// A run reached a terminal status (`rec.status` already set): its Status row.
pub(crate) fn on_terminal(b: &mut ApplyBatch<'_>, run: &RunId, rec: &RunRecord) {
    if let Some(k) = derived::status_row(run, rec) {
        b.insert_derived(k);
    }
}

/// A terminal run is resumed in place (`RetryRun`; `rec.status` still the terminal status):
/// its Status row goes, the other rows stay.
pub(crate) fn on_resume(b: &mut ApplyBatch<'_>, run: &RunId, rec: &RunRecord) {
    if let Some(k) = derived::status_row(run, rec) {
        b.remove_derived(k);
    }
}

/// The run's state is deleted (retention expiry, at the real delete point): every row.
pub(crate) fn on_expire(b: &mut ApplyBatch<'_>, run: &RunId, rec: &RunRecord) {
    for k in derived::run_index_rows(run, rec) {
        b.remove_derived(k);
    }
}
