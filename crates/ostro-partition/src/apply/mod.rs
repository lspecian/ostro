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
//! - **No chunk leakage (Pitfall 9):** nothing depends on batch boundaries;
//!   [`apply_chunk`] is a plain loop over [`apply_command`].

pub mod cursor;
mod inbound;
mod outbox;
pub mod outcome;
mod run;
mod timer;

use ostro_log::VerifiedBatch;
use ostro_types::command::{Cmd, decode_command};
use ostro_types::{Epoch, Offset};

pub use cursor::Cursor;
pub use outcome::{Action, ApplyOutcome, InvalidCode, RejectReason};

use crate::store::{ApplyBatch, StoreError};

/// Timer op reserved for a delayed run start (`ready_at_ms > requested_at_ms`).
pub const START_DELAY_OP: u64 = u64::MAX;

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
        Cmd::AttemptGranted(g) => run::apply_attempt_granted(b, cur, g),
        Cmd::EventsAppended(e) => run::apply_events_appended(b, cur, e, actions),
        Cmd::TimerFired(t) => timer::apply_timer_fired(b, cur, t, actions),
        Cmd::InboundBatch(ib) => inbound::apply_inbound_batch(b, cur, ib, actions),
        Cmd::OutboxTruncated(t) => outbox::apply_outbox_truncated(b, cur, t),
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
