//! Dead-letter marks: `Cmd::DlqUpdate` (tag 45) (Phase 11, OPS-04, D-P11-11; RESERVATIONS.md
//! P11 block). Format contract: 11-02; semantics: 11-08. See `docs/storage-format.md`
//! "Phase 11 additions".
//!
//! The dead-letter queue is a *view*: failed / timed-out runs minus their marks. The marks
//! (hook started, dismissed, requeued) live in one postcard [`DlqRow`] per run in the `dlq`
//! keyspace (11), key `run_key(run)`. Only this command writes that keyspace; no other command
//! creates a dlq row (retention's existing prefix delete removes it with the run).
//!
//! Rules (validation first, then 2 reads: the run record and the dlq row):
//! - Malformed: a run id that is not 18 bytes, no `op`, a dismiss reason > 1024 bytes, a hook
//!   run id that is not 18 bytes, a requeue mode > 2, a restart/rerun `new_run_id` that is not
//!   18 bytes or a resume with a non-empty `new_run_id` → `Invalid(BadDlqUpdate)`. A run on
//!   another partition → `WrongPartition`; an unknown run → `RunNotFound`.
//! - `HookStarted`, `Dismissed` and `Requeued{restart|rerun}` need status `Failed` or
//!   `TimedOut` and `episode_completed_at_ms == completed_at_ms` (W5) of the run, else
//!   `Invalid(BadDlqUpdate)`. `Requeued{resume}` is accepted for any status (the resume already
//!   moved the run out of `Failed`); on a `Failed`/`TimedOut` run its episode must match too.
//! - Episodes: when the stored row names a different episode, it is replaced by a fresh row for
//!   the command's episode before merging, so marks of an earlier failure never hide a later
//!   one.
//! - Merge: `HookStarted` sets `hook_run` once (the same run again is a no-op, a different run
//!   → `Invalid(BadDlqUpdate)`); `Dismissed` sets `dismissed_*`; `Requeued` sets `requeued_*`
//!   (`requeued_to` = None for a resume). A merge that changes nothing writes nothing.
//! - `Accepted`, no actions. Never reads payloads or blobs; never touches the runs row. No
//!   clocks: times come from the command.

use ostro_types::RunId;
use ostro_types::command::{DlqUpdate, dlq_update};

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::run::load_run;
use crate::store::keys::run_key;
use crate::store::values::{self, DlqRow, RunStatus};
use crate::store::{ApplyBatch, Table};

fn bad() -> ApplyResult {
    Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
        InvalidCode::BadDlqUpdate,
    )))
}

/// The statically validated mark.
enum Mark {
    Hook([u8; 18]),
    Dismiss {
        reason: String,
        at_ms: u64,
    },
    Requeue {
        mode: u8,
        to: Option<[u8; 18]>,
        at_ms: u64,
    },
}

impl Mark {
    /// Whether the run must be in a failure state (`Failed` / `TimedOut`).
    fn needs_failure(&self) -> bool {
        !matches!(self, Mark::Requeue { mode: 0, .. })
    }
}

fn id18(b: &[u8]) -> Option<[u8; 18]> {
    RunId::from_bytes(b).ok().map(|r| *r.as_bytes())
}

fn parse(cmd: &DlqUpdate) -> Option<Mark> {
    if cmd.check_limits().is_err() {
        return None;
    }
    Some(match cmd.op.as_ref()? {
        dlq_update::Op::HookStarted(h) => Mark::Hook(id18(&h.hook_run_id)?),
        dlq_update::Op::Dismissed(d) => Mark::Dismiss {
            reason: d.reason.clone(),
            at_ms: d.at_ms,
        },
        dlq_update::Op::Requeued(r) => {
            let to = match r.mode {
                dlq_update::REQUEUE_RESUME if r.new_run_id.is_empty() => None,
                dlq_update::REQUEUE_RESTART | dlq_update::REQUEUE_RERUN => {
                    Some(id18(&r.new_run_id)?)
                }
                _ => return None,
            };
            Mark::Requeue {
                mode: u8::try_from(r.mode).ok()?,
                to,
                at_ms: r.at_ms,
            }
        }
    })
}

/// `Cmd::DlqUpdate`: upserts the run's [`DlqRow`] (see the module doc for the rules).
pub(crate) fn apply_dlq_update(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    cmd: &DlqUpdate,
    _actions: &mut Vec<Action>,
) -> ApplyResult {
    let Ok(run) = RunId::from_bytes(&cmd.run_id) else {
        return bad();
    };
    if run.partition() != cur.partition {
        return Ok(ApplyOutcome::Rejected(RejectReason::WrongPartition));
    }
    let Some(mark) = parse(cmd) else {
        return bad();
    };
    let Some(rec) = load_run(b, &run)? else {
        return Ok(ApplyOutcome::Rejected(RejectReason::RunNotFound));
    };
    let failed = matches!(rec.status, RunStatus::Failed | RunStatus::TimedOut);
    let episode_matches = cmd.episode_completed_at_ms == rec.completed_at_ms.unwrap_or(0);
    if mark.needs_failure() && !failed {
        return bad();
    }
    if failed && !episode_matches {
        return bad();
    }

    let key = run_key(&run);
    let stored: Option<DlqRow> = b
        .get(Table::Dlq, &key)?
        .map(|v| values::decode(&v))
        .transpose()?;
    let mut row = match &stored {
        Some(r) if r.episode_completed_at_ms == cmd.episode_completed_at_ms => r.clone(),
        _ => DlqRow {
            episode_completed_at_ms: cmd.episode_completed_at_ms,
            ..DlqRow::default()
        },
    };
    match mark {
        Mark::Hook(h) => match row.hook_run {
            Some(existing) if existing != h => return bad(),
            _ => row.hook_run = Some(h),
        },
        Mark::Dismiss { reason, at_ms } => {
            row.dismissed_at_ms = Some(at_ms);
            row.dismiss_reason = Some(reason);
        }
        Mark::Requeue { mode, to, at_ms } => {
            row.requeued_to = to;
            row.requeued_at_ms = Some(at_ms);
            row.requeue_mode = Some(mode);
        }
    }
    if stored.as_ref() != Some(&row) {
        b.insert(Table::Dlq, key, values::encode(&row));
    }
    Ok(ApplyOutcome::Accepted)
}
