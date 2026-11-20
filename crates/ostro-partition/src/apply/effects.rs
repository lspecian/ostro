//! In-flight host effects: `effects` rows, `RegisterEffect`/`CancelEffect` intents and the
//! attempt-independent `CompleteEffect` command (plan **06-03**; FX-01, FX-03; P-8, P-9,
//! P-10).
//!
//! - **Rows.** An in-flight http/sql/object effect is the `effects` row
//!   `effect_key(run, op) = run ‖ op BE -> EffectRow{kind, scheduled_at_ms}` (keyspace #12,
//!   RESERVATIONS §6; derived state, D18). The executor's derive step sends
//!   `RegisterEffect{op, kind, scheduled_at_ms}` with the `EffectScheduled` event and
//!   `CancelEffect{op}` when the guest drops the op, so apply never decodes a journal payload.
//! - **Completion (P-10, B2).** The bridge completes an effect only through
//!   `Cmd::CompleteEffect{run_id, op, outcome, completed_at_ms}`. `outcome` is already the
//!   encoded `EffectCompleted{op, outcome}` event payload; apply journals it verbatim as
//!   `JournalEvent{seq: next_seq, kind: 65, payload: outcome}` (inline or blob ref, never
//!   decoded or fetched) with [`journal_encoded`] and wakes the run. The row is the token,
//!   exactly like a `waits` row for `CompleteToken`: the first completion removes it, so a
//!   second completion (or one after `CancelEffect`) finds nothing and is `Duplicate`.
//!   Completions carry no attempt token, so they survive activation eviction and leader
//!   changes.
//! - **Cleanup (P-8).** A terminal transition of a run that is **not** `resumable` deletes all
//!   of the run's rows ([`delete_run_effects`], called by `terminal::on_terminal`). Resumable
//!   terminals (Terminated, TimedOut, executor-level failures) keep them, so a run resumed by
//!   `RetryRun` still accepts the completion of an effect that was in flight when it stopped.
//!   Retention expiry deletes them with the run (`retention.rs`).
//! - **Leader-start wake (P-9, Pitfall 1).** When a `LeaderAnnounced` is accepted (the epoch
//!   gate in `apply/mod.rs`), [`wake_effect_runs`] gives every `Scheduled` (suspended), not
//!   paused run that has open rows a ready row with EnqueueReady semantics, so its executor
//!   re-activates and re-drives the effect. No pre-Phase-6 log has effects rows, so this is a
//!   no-op (one empty scan, no writes) for every older log and its digest.
//!
//! **Read counts** (asserted in `tests/apply_effects.rs` with the `ApplyBatch` read counter;
//! the documented exceptions are listed in docs/storage-format.md "Phase 6 additions"):
//!
//! | Command / hook | Keyspace reads |
//! |---|---|
//! | `RegisterEffect` / `CancelEffect` intent | 0 beyond `EventsAppended`'s (blind write / delete) |
//! | `CompleteEffect` | run record + effects point read = 2 (1 when rejected before the row read) |
//! | non-resumable terminal transition | + 1 effects prefix scan by run |
//! | `LeaderAnnounced` (accepted) | 1 full effects scan + 1 run-record read per distinct run with rows |

use ostro_types::RunId;
use ostro_types::command::{CancelEffect, CompleteEffect, RegisterEffect, is_reserved_op};
use ostro_types::journal::EventKind;
use ostro_types::live::EffectKind;

use super::cursor::Cursor;
use super::inbound::make_ready;
use super::inputs::journal_encoded;
use super::outcome::{Action, ApplyOutcome, ApplyResult, InvalidCode, RejectReason};
use super::run::{load_run, local_run, store_run};
use crate::store::keys::{decode_effect_key, effect_key, effect_run_prefix};
use crate::store::values::{self, EffectRow, RunRecord, RunStatus};
use crate::store::{ApplyBatch, StoreError, Table};

fn reject(r: RejectReason) -> ApplyResult {
    Ok(ApplyOutcome::Rejected(r))
}

fn bad_completion() -> RejectReason {
    RejectReason::Invalid(InvalidCode::BadEffectCompletion)
}

/// Highest valid `RegisterEffect.kind` (`EffectKind::Object`).
const MAX_EFFECT_KIND: u32 = EffectKind::Object as u32;

// ------------------------------------------------------------------ intents

/// Validation of a `RegisterEffect` intent before any write of its `EventsAppended`
/// (`run::check_intents`): a known kind (`<= EffectKind::Object`) and an op outside the
/// apply-reserved range, else `BadIntent`. No state access.
pub(crate) fn check_register_effect(r: &RegisterEffect) -> Result<(), RejectReason> {
    if r.kind > MAX_EFFECT_KIND || is_reserved_op(r.op) {
        return Err(RejectReason::Invalid(InvalidCode::BadIntent));
    }
    Ok(())
}

/// Applies a validated `RegisterEffect` intent (`run.rs` write loop, after the batch's events
/// moved `next_seq`).
///
/// - `op >= rec.next_seq` (the op is not journaled): no effect, the same deterministic drop as
///   `waits::register_wait` (the write loop cannot reject).
/// - Otherwise blind-inserts `EffectRow{kind, scheduled_at_ms}`. A repeat for the same op
///   carries the same `EffectScheduled` facts, so the row stays byte-identical (idempotent).
///
/// **Contract with the submitter** (as for `RegisterWait`): apply cannot tell a never
/// registered op from a completed one, so the executor only registers ops without a
/// completion in the journal.
///
/// **Reads:** 0 (blind write).
pub(crate) fn register_effect(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &RunRecord,
    r: &RegisterEffect,
) {
    if r.op >= rec.next_seq {
        return;
    }
    // Validated by `check_register_effect`: kind <= 2 fits in a u8.
    let kind = u8::try_from(r.kind).unwrap_or(u8::MAX);
    b.insert(
        Table::Effects,
        effect_key(run, r.op),
        values::encode(&EffectRow {
            kind,
            scheduled_at_ms: r.scheduled_at_ms,
        }),
    );
}

/// Applies a `CancelEffect` intent: blind delete of the row (a missing row is fine). After it
/// a `CompleteEffect` for the op is `Duplicate`. **Reads:** 0.
pub(crate) fn cancel_effect(b: &mut ApplyBatch<'_>, run: &RunId, c: &CancelEffect) {
    b.remove(Table::Effects, effect_key(run, c.op));
}

// ------------------------------------------------------------------ CompleteEffect

/// `Cmd::CompleteEffect` (P-10): first completion wins, attempt-independent.
///
/// Validate everything, then write:
/// - bad / foreign run id → `Invalid(BadRunId)` / `WrongPartition`;
/// - no `outcome` → `Invalid(BadEffectCompletion)`; an inline outcome over 256 KiB →
///   `PayloadTooLarge` (W5; larger outcomes travel as blob refs);
/// - unknown run → `RunNotFound`; terminal run → `RunTerminal`;
/// - `op >= next_seq` (never journaled, so never scheduled) → `Invalid(BadEffectCompletion)`;
/// - row present → `JournalEvent{seq: next_seq, kind: 65, payload: outcome}` stored verbatim
///   (never decoded), the row deleted, the run woken (`InputAppended` if Running,
///   ready row + `EnqueueReady` if Scheduled) → `Accepted`;
/// - row absent → `Duplicate` (already completed or cancelled), nothing written.
///
/// **Reads: 2** (run record + effects point read; asserted exception to the one-read rule).
/// Rejections before the row read cost 1 (or 0 for structural ones).
pub(crate) fn apply_complete_effect(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    c: &CompleteEffect,
    actions: &mut Vec<Action>,
) -> ApplyResult {
    let run = match local_run(cur, &c.run_id) {
        Ok(r) => r,
        Err(r) => return reject(r),
    };
    let Some(outcome) = &c.outcome else {
        return reject(bad_completion());
    };
    if outcome.is_oversize_inline() {
        return reject(RejectReason::PayloadTooLarge);
    }
    let Some(mut rec) = load_run(b, &run)? else {
        return reject(RejectReason::RunNotFound);
    };
    if rec.status.is_terminal() {
        return reject(RejectReason::RunTerminal);
    }
    if c.op >= rec.next_seq {
        return reject(bad_completion());
    }
    let key = effect_key(&run, c.op);
    if b.get(Table::Effects, &key)?.is_none() {
        return Ok(ApplyOutcome::Duplicate { existing: None });
    }
    b.remove(Table::Effects, key);
    journal_encoded(
        b,
        &run,
        &mut rec,
        EventKind::EffectCompleted,
        outcome,
        true,
        actions,
    )?;
    store_run(b, &run, &rec);
    Ok(ApplyOutcome::Accepted)
}

// ------------------------------------------------------------------ hooks

/// Deletes every `effects` row of `run`: one prefix scan by run, then blind deletes.
///
/// Called by `terminal::on_terminal` for non-resumable terminal transitions (P-8) and by
/// retention expiry. **Reads:** 1 (asserted exception, docs/storage-format.md).
pub(crate) fn delete_run_effects(b: &mut ApplyBatch<'_>, run: &RunId) -> Result<(), StoreError> {
    for (k, _) in b.range_prefix(Table::Effects, &effect_run_prefix(run))? {
        b.remove(Table::Effects, k);
    }
    Ok(())
}

/// Leader-start wake (P-9): scans the whole `effects` keyspace in key order (runs grouped,
/// ascending) and, for each distinct run whose status is `Scheduled` and which is not paused,
/// inserts the ready row exactly as `EnqueueReady` does (ready at `rec.created_at_ms`, the
/// earliest time, as `journal_encoded` wakes) with `Action::EnqueueReady`. Runs that are
/// Ready, Running, paused or terminal (resumable terminals keep rows) are not touched. A row
/// whose run record is missing is skipped (cannot happen: expiry deletes both).
///
/// Called only for an **accepted** `LeaderAnnounced` (epoch gate in `apply/mod.rs`).
///
/// **Reads:** 1 full effects scan + 1 run-record read per distinct run with rows (asserted
/// exception). With an empty keyspace: the scan only, no writes, so every pre-Phase-6 digest
/// is unchanged.
pub(crate) fn wake_effect_runs(
    b: &mut ApplyBatch<'_>,
    actions: &mut Vec<Action>,
) -> Result<(), StoreError> {
    let rows = b.range_prefix(Table::Effects, &[])?;
    let mut last: Option<RunId> = None;
    for (k, _) in rows {
        let (run, _) = decode_effect_key(&k)
            .ok_or_else(|| StoreError::Codec("malformed effects key".into()))?;
        if last == Some(run) {
            continue;
        }
        last = Some(run);
        let Some(mut rec) = load_run(b, &run)? else {
            continue;
        };
        if rec.status != RunStatus::Scheduled || rec.pause.is_some() {
            continue;
        }
        let at = rec.created_at_ms;
        make_ready(b, &run, &mut rec, at, actions);
        store_run(b, &run, &rec);
    }
    Ok(())
}
