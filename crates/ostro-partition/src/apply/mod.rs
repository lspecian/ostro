//! The pure, deterministic apply core (STOR-05, STOR-10, STOR-11, STOR-14).
//!
//! [`apply_command`] is a pure function of (store state, cursor, offset,
//! envelope epoch, command bytes). It takes no clock, no randomness and no
//! I/O besides the [`ApplyBatch`] overlay, and iterates only ordered
//! collections (crate `clippy.toml` bans clocks and `HashMap`/`HashSet`).
//! Timestamps come from commands.
//!
//! - **Epoch gate (first):** `LeaderAnnounced{epoch}` is accepted iff
//!   `epoch > cursor.epoch` and the envelope epoch equals it. Every other
//!   command needs `envelope epoch == cursor.epoch`, else
//!   `Rejected(StaleEpoch)`.
//! - **Validation at apply, all-or-nothing:** handlers check everything
//!   before the first write, so a rejection never leaves partial writes.
//! - **Rejections are deterministic no-ops** that still consume their offset
//!   and are folded into the rolling hash
//!   `H_n = blake3(H_{n-1} ‖ offset_le ‖ outcome_code ‖ cmd_bytes)`
//!   (outcome codes: see [`outcome`]).
//! - **Read budget:** hot-path commands (`StartRun`, `AttemptGranted`,
//!   `EventsAppended`) issue at most one keyspace point read each.
//!   `TimerFired` reads at most twice (timer row, then run record);
//!   `OutboxTruncated` is blind (zero reads); `InboundBatch` reads per
//!   message (see `inbound`).
//! - **No chunk leakage (Pitfall 9):** nothing depends on batch boundaries;
//!   [`apply_chunk`] is a plain loop over [`apply_command`].

pub mod cursor;
mod inbound;
mod inputs;
mod lifecycle;
mod outbox;
pub mod outcome;
mod retention;
pub(crate) mod run;
// Phase 8 (08-01 seam): `schedule.rs` is filled by 08-07.
mod schedule;
mod terminal;
mod timer;
mod waits;

use ostro_log::VerifiedBatch;
use ostro_types::command::{Cmd, decode_command};
use ostro_types::{Epoch, Offset};

pub use cursor::Cursor;
pub use outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};

use crate::store::{ApplyBatch, StoreError};

/// Timer op reserved for a delayed run start (`ready_at_ms > requested_at_ms`). Defined next
/// to the other reserved timer ops in `ostro_types::command` (plan 05-01); same value.
pub use ostro_types::command::START_DELAY_OP;

/// One step of the rolling hash:
/// `blake3(prev ‖ off.0.to_le_bytes() ‖ [code] ‖ cmd_bytes)`.
pub fn fold_hash(prev: &[u8; 32], off: Offset, code: u8, cmd_bytes: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(prev);
    h.update(&off.0.to_le_bytes());
    h.update(&[code]);
    h.update(cmd_bytes);
    *h.finalize().as_bytes()
}

/// Applies the command at `off` (which must equal `cur.next`), folds it into
/// the rolling hash and advances the cursor. `Err` only for store failures,
/// which are fatal for the partition.
///
/// # Panics
///
/// If `off != cur.next` (an apply-loop bug).
pub fn apply_command(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    off: Offset,
    env_epoch: Epoch,
    cmd_bytes: &[u8],
    actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    assert_eq!(
        off, cur.next,
        "apply offset must be the cursor's next offset"
    );
    let outcome = dispatch(b, cur, off, env_epoch, cmd_bytes, actions)?;
    cur.hash = fold_hash(&cur.hash, off, outcome.code(), cmd_bytes);
    cur.next = off.next();
    Ok(outcome)
}

fn dispatch(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    off: Offset,
    env_epoch: Epoch,
    cmd_bytes: &[u8],
    actions: &mut Vec<Action>,
) -> Result<ApplyOutcome, StoreError> {
    let Ok(cmd) = decode_command(cmd_bytes) else {
        return Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
            InvalidCode::Decode,
        )));
    };
    let Some(cmd) = cmd.cmd else {
        return Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
            InvalidCode::Empty,
        )));
    };

    let current = cur.epoch();
    if let Cmd::LeaderAnnounced(la) = &cmd {
        if la.epoch > current.0 && env_epoch.0 == la.epoch {
            cur.meta.epoch = la.epoch;
            cur.meta.epoch_history.push((la.epoch, off.0));
            return Ok(ApplyOutcome::Accepted);
        }
        return Ok(ApplyOutcome::Rejected(RejectReason::StaleEpoch { current }));
    }
    if env_epoch != current {
        return Ok(ApplyOutcome::Rejected(RejectReason::StaleEpoch { current }));
    }

    match &cmd {
        Cmd::LeaderAnnounced(_) => unreachable!("handled by the epoch gate"),
        Cmd::StartRun(s) => {
            let dedupe = (!s.idempotency_key.is_empty())
                .then(|| (s.workflow.clone(), s.idempotency_key.clone()));
            run::start_run(b, cur, s, dedupe, actions)
        }
        Cmd::AttemptGranted(g) => run::apply_attempt_granted(b, cur, g, actions),
        Cmd::EventsAppended(e) => run::apply_events_appended(b, cur, e, actions),
        Cmd::TimerFired(t) => timer::apply_timer_fired(b, cur, t, actions),
        Cmd::InboundBatch(ib) => inbound::apply_inbound_batch(b, cur, ib, actions),
        Cmd::OutboxTruncated(t) => outbox::apply_outbox_truncated(b, cur, t),
        // Phase 5 (plan 05-02 seams): `waits.rs` is filled by 05-08, `lifecycle.rs` by 05-09.
        // The stubs reject with Invalid(BadIntent) until then.
        Cmd::SignalRun(s) => waits::signal_run(b, cur, s, actions),
        Cmd::RequestCancel(r) => waits::request_cancel(b, cur, r, actions),
        Cmd::CompleteToken(c) => waits::complete_token(b, cur, c, actions),
        Cmd::TerminateRun(t) => lifecycle::terminate_run(b, cur, t, actions),
        Cmd::RetryRun(r) => lifecycle::retry_run(b, cur, r, actions),
        // Phase 8: implemented in 08-07 (rejects with Invalid(NotImplemented) until then).
        Cmd::ScheduleFire(s) => schedule::apply_schedule_fire(b, cur, s, actions),
        // Phase 6: implemented in 06-03 (temporary reject arm).
        Cmd::CompleteEffect(_) => Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
            InvalidCode::NotImplemented,
        ))),
    }
}

/// Applies every command of `batches` in order, each with its batch's
/// envelope epoch. Returns one `(offset, outcome)` per command.
pub fn apply_chunk(
    b: &mut ApplyBatch<'_>,
    cur: &mut Cursor,
    batches: &[VerifiedBatch],
    actions: &mut Vec<Action>,
) -> Result<Vec<(Offset, ApplyOutcome)>, StoreError> {
    let mut out = Vec::with_capacity(batches.iter().map(|vb| vb.commands.len()).sum());
    for vb in batches {
        for (off, bytes) in vb.commands_with_offsets() {
            let o = apply_command(b, cur, off, vb.epoch, bytes, actions)?;
            out.push((off, o));
        }
    }
    Ok(out)
}
