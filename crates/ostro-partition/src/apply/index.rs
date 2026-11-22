//! `x_run_index` maintenance hooks (Phase 11, D-P11-23; RESERVATIONS.md §6 derived keyspaces,
//! PF-11-2). Contract: 11-02 (no-op stubs); implemented by 11-05.
//!
//! The derived keyspace sits outside TABLES, the digest, OSNP and blob GC, so these hooks can
//! never change the state hash.
// The hooks are wired into the run lifecycle by 11-05.
#![allow(dead_code)]

use ostro_types::RunId;

use crate::store::ApplyBatch;
use crate::store::values::RunRecord;

/// A run was created (`StartRun` accepted). No-op until 11-05.
pub(crate) fn on_start(_b: &mut ApplyBatch<'_>, _run: &RunId, _rec: &RunRecord) {}

/// A run reached a terminal status. No-op until 11-05.
pub(crate) fn on_terminal(_b: &mut ApplyBatch<'_>, _run: &RunId, _rec: &RunRecord) {}

/// A terminal run was resumed (`RetryRun`). No-op until 11-05.
pub(crate) fn on_resume(_b: &mut ApplyBatch<'_>, _run: &RunId, _rec: &RunRecord) {}

/// A terminal run's retention expired and its state was deleted. No-op until 11-05.
pub(crate) fn on_expire(_b: &mut ApplyBatch<'_>, _run: &RunId) {}
