//! `Cmd::RepinRun` (tag 43; CLUS-07, D-P13-21; RESERVATIONS §1 P13 block), semantics 13-17.
//!
//! Submitted by `ostro migrate-runs` (`ostro-server` `control/migrate.rs`) only for runs
//! whose journal replayed cleanly against the target module; `expected_next_seq` is the
//! length of the journal that was replayed, so the apply is the compare-and-set that makes
//! "repin only after a passing replay" hold by construction.
//!
//! Checks, in this order (validation before the single read, nothing written on a reject):
//!
//! | condition | outcome |
//! |---|---|
//! | run id not 18 bytes / names another partition | `Invalid(BadRunId)` / `WrongPartition` |
//! | `from_sha` or `to_sha` empty | `Invalid(MissingField)` |
//! | `from_sha` or `to_sha` not 32 bytes | `Invalid(BadLength)` |
//! | no run record | `RunNotFound` |
//! | terminal run | `RunTerminal` |
//! | `RunRecord.module_sha != from_sha` | `ModuleMismatch` (index 23, code 39) |
//! | `RunRecord.next_seq != expected_next_seq` | `SeqMismatch{expected: next_seq}` |
//! | the run holds a lease (status `Running`, i.e. its `running` row exists) | `RunBusy` (index 24, code 40) |
//! | otherwise | `Accepted`: `RunRecord.module_sha = to_sha`, nothing else changes |
//!
//! The `running` row exists exactly while the status is `Running` (`run.rs` invariant,
//! 04-12), so the lease check reads the status of the record already loaded instead of a
//! second point read. A hot-parked activation (Phase 3 hot wait: the engine waits in
//! process instead of suspending) keeps its run `Running` until it is evicted, so it is
//! `RunBusy` too. A paused run that is not `Running` can be repinned (the pause mark stays).
//!
//! No action is emitted and no other row is touched: journal, timers, waits, ready row,
//! run index and `next_seq` are unchanged. The outcome codes are folded into the state hash
//! like every outcome. No clocks (`requested_at_ms` is informational).

use ostro_types::command::{MODULE_SHA_LEN, RepinRun};

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::run::{load_run, local_run, store_run};
use crate::store::ApplyBatch;
use crate::store::values::RunStatus;

fn reject(r: RejectReason) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(r))
}

/// A 32-byte sha field.
fn sha(b: &[u8]) -> Result<[u8; 32], RejectReason> {
    match b.len() {
        0 => Err(RejectReason::Invalid(InvalidCode::MissingField)),
        MODULE_SHA_LEN => {
            let mut a = [0u8; 32];
            a.copy_from_slice(b);
            Ok(a)
        }
        _ => Err(RejectReason::Invalid(InvalidCode::BadLength)),
    }
}

/// `Cmd::RepinRun` (module docs). One read (the run record), one write on success.
pub(crate) fn apply_repin_run(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    cmd: &RepinRun,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &cmd.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    let (from, to) = match (sha(&cmd.from_sha), sha(&cmd.to_sha)) {
        (Ok(f), Ok(t)) => (f, t),
        (Err(r), _) | (_, Err(r)) => return reject(r),
    };
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    if rec.module_sha != Some(from) {
        return reject(RejectReason::ModuleMismatch);
    }
    if rec.next_seq != cmd.expected_next_seq {
        return reject(RejectReason::SeqMismatch {
            expected: rec.next_seq,
        });
    }
    if rec.status == RunStatus::Running {
        return reject(RejectReason::RunBusy);
    }
    rec.module_sha = Some(to);
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}
