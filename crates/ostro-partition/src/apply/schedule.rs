//! `Cmd::ScheduleFire` (tag 27): cron fires with a durable per-schedule watermark in
//! keyspace 16 `schedule_state` (P8-D10, plan 08-07; TRIG-02, TRIG-03).
//!
//! The schedule's owner partition (`routing::schedule_owner(name, P)`) applies every fire.
//! Its runs are started in the same partition, so overlap is evaluated locally against the
//! schedule row's `last_run`.
//!
//! **Validation** (before any read or write), in order:
//! - unknown `kind`, empty or > [`MAX_SCHEDULE_NAME_LEN`] name, `fire_time_ms == 0`, a
//!   `Fire`/`Manual` without `start` or an `Advance` with one, a `Manual` with an empty
//!   or over-64-byte `request_id`, an `overlap` outside `0..=4`, a `start.idempotency_key` other
//!   than `cron_idempotency_key(schedule, fire_time)` (`Fire`) or
//!   `cron_manual_idempotency_key(schedule, request_id)` (`Manual`), or a
//!   `start.conflict_policy` other than `ReturnExisting` → `Invalid(BadScheduleFire)`;
//! - `schedule_owner(schedule, P) != this partition` → `Invalid(ScheduleOwnerMismatch)`;
//! - a malformed `start.run_id` → `Invalid(BadRunId)`, one routed elsewhere →
//!   `WrongPartition`.
//!
//! **Incarnation (W4).** The row records the `schedule_created_at_ms` it belongs to. A fire of a
//! newer incarnation resets the row (watermark 0, no `last_run`, no buffered fire, counters 0)
//! in the same batch. A fire of an older incarnation (stale, in flight across a delete) is
//! `Duplicate { existing: None }` and writes nothing. Reverse rows (`0x02 ‖ run`) of the old
//! incarnation are not scanned: [`on_run_terminal`] ignores a reverse row whose incarnation
//! differs from the schedule row's.
//!
//! **Semantics** (`W` = watermark, `ft` = `fire_time_ms`):
//! - `Advance`: `ft <= W` → `Duplicate { existing: None }`, `duplicates += 1`. Otherwise
//!   `W = ft`, `skipped += skipped` → `Accepted`.
//! - `Fire` with `ft <= W` → `Duplicate { existing }`, `duplicates += 1`, no start. `existing`
//!   is `last_run`, else the run under the fire's `__cron/` dedupe row, else `None`.
//! - `Fire`/`Manual` otherwise: `last_run` absent or terminal, or overlap `AllowAll` → start
//!   the run through the internal StartRun path (`run::start_run`, the `__cron/` dedupe row is
//!   the second guard). A start sets `last_run`, `fired += 1`, writes the reverse row and
//!   drops a pending buffered fire (`skipped += 1`): a started fire supersedes it.
//!   With `last_run` live:
//!   - `Manual` under `Skip`/`BufferOne` whose `__cron/` dedupe row exists (a retried
//!     `request_id`): `Duplicate { existing }`, `duplicates += 1`, checked before the policy;
//!   - `Skip`: `skipped += 1`, no start;
//!   - `BufferOne`: `buffered = this fire` (a replaced buffered fire counts `skipped += 1`);
//!     the last run's terminal transition starts it ([`on_run_terminal`]);
//!   - `CancelOther` / `TerminateOther`: start, then the internal `RequestCancel` (graceful,
//!     D3) / `TerminateRun` of the last run, reason [`OVERLAP_REASON`], at
//!     `start.requested_at_ms`.
//! - `Fire` moves `W` to `ft` in the same batch in every non-duplicate case (start, skip,
//!   buffer, and a start answered `Duplicate` by the dedupe row). `Manual` never moves `W`.
//! - The start's own outcome passes through: a rejected start rejects the whole fire with
//!   nothing written; a start answered `Duplicate` by its dedupe row (a repeated manual
//!   `request_id`, or a fire time an earlier incarnation used) is `Duplicate { existing }`
//!   with `duplicates += 1`.
//!
//! **Continue-as-new (known limitation).** Continue-as-new chains are not tracked as
//! `last_run`: when a schedule-started run continues as new, its terminal transition clears
//! `last_run` (or starts the buffered fire), so overlap policies no longer see the
//! continuation and a later fire can overlap it.
//!
//! **Reads:** state row 1; `+1` last-run record when `last_run` is set and the fire is not a
//! duplicate; `+1` dedupe row (the StartRun read, or the `existing` lookup of a duplicate fire
//! without `last_run`, or the repeated-`request_id` lookup of a `Manual` under a live
//! `Skip`/`BufferOne`); `CancelOther`/`TerminateOther` add the cancel/terminate reads
//! (record + children/waits scans). The terminal hook reads nothing for runs without a
//! `__cron/` idempotency key, 1 (reverse row) for a schedule run without the row, 2 with the
//! row, and 3 when it starts a buffered fire.

use ostro_types::RunId;
use ostro_types::command::{
    ConflictPolicy, MAX_REQUEST_ID_LEN, OverlapPolicy, RequestCancel, ScheduleFire,
    ScheduleFireKind, StartRun, TerminateRun, cron_idempotency_key, cron_manual_idempotency_key,
};
use ostro_types::routing::schedule_owner;
use prost::Message;

use super::cursor::Cursor;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::{lifecycle, run, waits};
use crate::store::keys::{dedupe_key, schedule_run_key, schedule_state_key};
use crate::store::values::{self, BufferedFire, RunStatus, ScheduleRunRow, ScheduleStateRow};
use crate::store::{ApplyBatch, StoreError, Table};

/// Longest accepted schedule name (bytes).
pub(crate) const MAX_SCHEDULE_NAME_LEN: usize = 256;

/// Idempotency-key prefix of every schedule-started run; gates the terminal hook.
pub(crate) const CRON_KEY_PREFIX: &[u8] = b"__cron/";

/// Reason of the cancel / terminate issued by `CancelOther` / `TerminateOther`.
pub(crate) const OVERLAP_REASON: &str = "schedule_overlap";

fn reject(r: RejectReason) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(r))
}

fn bad() -> RejectReason {
    RejectReason::Invalid(InvalidCode::BadScheduleFire)
}

/// Validated shape of a fire.
struct Fire<'a> {
    kind: ScheduleFireKind,
    policy: OverlapPolicy,
    start: Option<&'a StartRun>,
}

fn check<'a>(cur: &Cursor, s: &'a ScheduleFire) -> Result<Fire<'a>, RejectReason> {
    let kind = ScheduleFireKind::try_from(s.kind).map_err(|_| bad())?;
    if s.schedule.is_empty() || s.schedule.len() > MAX_SCHEDULE_NAME_LEN || s.fire_time_ms == 0 {
        return Err(bad());
    }
    let start = match (kind, &s.start) {
        (ScheduleFireKind::Advance, None) => None,
        (ScheduleFireKind::Fire | ScheduleFireKind::Manual, Some(st)) => Some(st),
        _ => return Err(bad()),
    };
    if kind == ScheduleFireKind::Manual
        && (s.request_id.is_empty() || s.request_id.len() > MAX_REQUEST_ID_LEN)
    {
        return Err(bad());
    }
    let policy = s.overlap_policy().ok_or_else(bad)?;
    if let Some(st) = start {
        let key = match kind {
            ScheduleFireKind::Manual => cron_manual_idempotency_key(&s.schedule, &s.request_id),
            _ => cron_idempotency_key(&s.schedule, s.fire_time_ms),
        };
        if st.idempotency_key != key.as_bytes()
            || st.conflict_policy != ConflictPolicy::ReturnExisting as i32
        {
            return Err(bad());
        }
    }
    if schedule_owner(&s.schedule, cur.count) != cur.partition {
        return Err(RejectReason::Invalid(InvalidCode::ScheduleOwnerMismatch));
    }
    if let Some(st) = start {
        run::local_run(cur, &st.run_id)?;
    }
    Ok(Fire {
        kind,
        policy,
        start,
    })
}

fn load_state(b: &mut ApplyBatch<'_>, name: &str) -> Result<Option<ScheduleStateRow>, StoreError> {
    b.get(Table::ScheduleState, &schedule_state_key(name))?
        .map(|v| values::decode(&v))
        .transpose()
}

fn store_state(b: &mut ApplyBatch<'_>, name: &str, row: &ScheduleStateRow) {
    b.insert(
        Table::ScheduleState,
        schedule_state_key(name),
        values::encode(row),
    );
}

fn put_reverse(b: &mut ApplyBatch<'_>, run: &RunId, name: &str, created: u64) {
    let rr = ScheduleRunRow {
        schedule: name.to_string(),
        schedule_created_at_ms: created,
    };
    b.insert(
        Table::ScheduleState,
        schedule_run_key(run),
        values::encode(&rr),
    );
}

/// The dedupe `(workflow, key)` of a validated schedule start.
fn dedupe_of(st: &StartRun) -> (String, Vec<u8>) {
    (st.workflow.clone(), st.idempotency_key.clone())
}

/// Applies a `ScheduleFire` (module docs).
pub(crate) fn apply_schedule_fire(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    s: &ScheduleFire,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let f = match check(cur, s) {
        Ok(f) => f,
        Err(r) => return reject(r),
    };
    let name = s.schedule.as_str();
    let ft = s.fire_time_ms;

    // Read 1: the schedule row, reset on a newer incarnation (W4).
    let mut row = match load_state(b, name)? {
        Some(r) if r.schedule_created_at_ms > s.schedule_created_at_ms => {
            // A stale in-flight fire of a deleted incarnation.
            return Ok(ApplyOutcome::Duplicate { existing: None });
        }
        Some(r) if r.schedule_created_at_ms == s.schedule_created_at_ms => r,
        _ => ScheduleStateRow {
            schedule_created_at_ms: s.schedule_created_at_ms,
            ..ScheduleStateRow::default()
        },
    };
    let moves_watermark = f.kind != ScheduleFireKind::Manual;

    let Some(st) = f.start else {
        // Advance (catch-up Skip).
        if ft <= row.watermark_ms {
            row.duplicates += 1;
            store_state(b, name, &row);
            return Ok(ApplyOutcome::Duplicate { existing: None });
        }
        row.watermark_ms = ft;
        row.skipped += u64::from(s.skipped);
        store_state(b, name, &row);
        return Ok(ApplyOutcome::Accepted);
    };

    if moves_watermark && ft <= row.watermark_ms {
        let existing = match row.last_run {
            Some(r) => Some(r),
            None => {
                let (wf, key) = dedupe_of(st);
                match b.get(Table::Dedupe, &dedupe_key(&wf, &key))? {
                    Some(v) => Some(
                        RunId::from_bytes(&v)
                            .map_err(|e| StoreError::Codec(format!("dedupe row: {e}")))?,
                    ),
                    None => None,
                }
            }
        };
        row.duplicates += 1;
        store_state(b, name, &row);
        return Ok(ApplyOutcome::Duplicate { existing });
    }

    // Read 2 (when set): is the last run still live?
    let live_last = match row.last_run {
        Some(r) => run::load_run(b, &r)?
            .is_some_and(|rec| !rec.status.is_terminal())
            .then_some(r),
        None => None,
    };

    if let Some(_last) = live_last {
        // A repeated manual `request_id` is answered by its dedupe row before the overlap
        // policy: a retried trigger-now while its own run is live returns that run instead
        // of being skipped or buffered. (Started policies reach `start_run`, whose dedupe
        // check runs before any cancel/terminate.)
        if !moves_watermark && matches!(f.policy, OverlapPolicy::Skip | OverlapPolicy::BufferOne) {
            let (wf, key) = dedupe_of(st);
            if let Some(v) = b.get(Table::Dedupe, &dedupe_key(&wf, &key))? {
                let existing = RunId::from_bytes(&v)
                    .map_err(|e| StoreError::Codec(format!("dedupe row: {e}")))?;
                row.duplicates += 1;
                store_state(b, name, &row);
                return Ok(ApplyOutcome::Duplicate {
                    existing: Some(existing),
                });
            }
        }
        match f.policy {
            OverlapPolicy::Skip => {
                row.skipped += 1;
                if moves_watermark {
                    row.watermark_ms = ft;
                }
                store_state(b, name, &row);
                return Ok(ApplyOutcome::Accepted);
            }
            OverlapPolicy::BufferOne => {
                if row.buffered.is_some() {
                    row.skipped += 1;
                }
                row.buffered = Some(BufferedFire {
                    fire_time_ms: ft,
                    start: st.encode_to_vec(),
                });
                if moves_watermark {
                    row.watermark_ms = ft;
                }
                store_state(b, name, &row);
                return Ok(ApplyOutcome::Accepted);
            }
            OverlapPolicy::AllowAll
            | OverlapPolicy::CancelOther
            | OverlapPolicy::TerminateOther => {}
        }
    }

    // Start (read: the dedupe row). A rejected start writes nothing.
    let started = run::start_run(b, cur, st, Some(dedupe_of(st)), actions)?;
    match started {
        ApplyOutcome::Accepted => {}
        ApplyOutcome::Duplicate { existing } => {
            row.duplicates += 1;
            if moves_watermark {
                row.watermark_ms = ft;
            }
            store_state(b, name, &row);
            return Ok(ApplyOutcome::Duplicate { existing });
        }
        rejected @ ApplyOutcome::Rejected(_) => return Ok(rejected),
    }
    let new_run = run::local_run(cur, &st.run_id)
        .map_err(|_| StoreError::Codec("validated schedule start run id".into()))?;
    if row.buffered.take().is_some() {
        row.skipped += 1;
    }
    row.last_run = Some(new_run);
    row.fired += 1;
    if moves_watermark {
        row.watermark_ms = ft;
    }
    // The row is stored before the cancel/terminate below, so the old run's terminal hook
    // sees `last_run == new_run` and leaves the schedule alone.
    store_state(b, name, &row);
    put_reverse(b, &new_run, name, row.schedule_created_at_ms);

    if let Some(last) = live_last {
        let at = st.requested_at_ms;
        // Outcomes ignored: the run is live and local, so both are Accepted (or a repeat
        // cancel, Duplicate); either way the fire itself is Accepted.
        match f.policy {
            OverlapPolicy::CancelOther => {
                let rc = RequestCancel {
                    run_id: last.as_bytes().to_vec(),
                    reason: OVERLAP_REASON.to_string(),
                    requested_at_ms: at,
                };
                waits::request_cancel(b, cur, &rc, actions)?;
            }
            OverlapPolicy::TerminateOther => {
                let t = TerminateRun {
                    run_id: last.as_bytes().to_vec(),
                    reason: OVERLAP_REASON.to_string(),
                    terminated_at_ms: at,
                };
                lifecycle::terminate_run(b, cur, &t, actions)?;
            }
            _ => {}
        }
    }
    Ok(ApplyOutcome::Accepted)
}

/// Terminal hook for a schedule-started run (called by `terminal::on_terminal` for runs whose
/// idempotency key starts with `__cron/`, after the status is set).
///
/// Reads the `0x02 ‖ run` reverse row (1 read; absent → done). Present: deletes it, then reads
/// the schedule row. If that row is the same incarnation and its `last_run` is `run`:
/// - a buffered fire is started (decoded `StartRun`, internal StartRun path, same batch):
///   `last_run` = the new run with its reverse row, `fired += 1`; a start answered
///   `Duplicate` counts `duplicates += 1`, a rejected one `skipped += 1`; `buffered` cleared;
/// - else, for `ContinuedAsNew`, `last_run` is cleared (chains are not tracked).
pub(crate) fn on_run_terminal(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    run: &RunId,
    status: RunStatus,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let rk = schedule_run_key(run);
    let Some(v) = b.get(Table::ScheduleState, &rk)? else {
        return Ok(());
    };
    let rr: ScheduleRunRow = values::decode(&v)?;
    b.remove(Table::ScheduleState, rk);
    let Some(mut row) = load_state(b, &rr.schedule)? else {
        return Ok(());
    };
    if row.schedule_created_at_ms != rr.schedule_created_at_ms || row.last_run != Some(*run) {
        return Ok(());
    }
    match row.buffered.take() {
        Some(buf) => {
            let st = StartRun::decode(buf.start.as_slice())
                .map_err(|e| StoreError::Codec(format!("buffered schedule start: {e}")))?;
            match run::start_run(b, cur, &st, Some(dedupe_of(&st)), actions)? {
                ApplyOutcome::Accepted => {
                    let new_run = RunId::from_bytes(&st.run_id)
                        .map_err(|e| StoreError::Codec(format!("buffered start run id: {e}")))?;
                    row.last_run = Some(new_run);
                    row.fired += 1;
                    put_reverse(b, &new_run, &rr.schedule, row.schedule_created_at_ms);
                }
                ApplyOutcome::Duplicate { .. } => {
                    row.duplicates += 1;
                    if status == RunStatus::ContinuedAsNew {
                        row.last_run = None;
                    }
                }
                ApplyOutcome::Rejected(_) => {
                    row.skipped += 1;
                    if status == RunStatus::ContinuedAsNew {
                        row.last_run = None;
                    }
                }
            }
        }
        None if status == RunStatus::ContinuedAsNew => row.last_run = None,
        None => return Ok(()),
    }
    store_state(b, &rr.schedule, &row);
    Ok(())
}
