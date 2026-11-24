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
//!   message (see `inbound`). Phase 6 exceptions (`effects`): `CompleteEffect`
//!   reads 2 (run record + effects row); an accepted `LeaderAnnounced` scans
//!   the effects keyspace once plus one record read per run with rows. Phase 8 exception
//!   (`schedule`): `ScheduleFire` reads the schedule row, the last run's record and the
//!   dedupe row (plus the cancel/terminate reads under `CancelOther`/`TerminateOther`).
//! - **No chunk leakage (Pitfall 9):** nothing depends on batch boundaries;
//!   [`apply_chunk`] is a plain loop over [`apply_command`].

pub mod cursor;
// Phase 11 (11-02 stubs, D-P11-23): `dlq.rs` filled by 11-08, `index.rs` (x_run_index hooks)
// by 11-05, `rerun.rs` (rerun prefix) by 11-07.
mod dlq;
mod effects;
mod inbound;
mod index;
mod inputs;
mod lifecycle;
mod outbox;
pub mod outcome;
mod pause;
mod rerun;
// Phase 13 (13-01 stub, 13-17 semantics): `Cmd::RepinRun` (43).
mod repin;
mod retention;
pub(crate) mod run;
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
    // P13-F1 rule 2: a command written by a newer Ostro halts the partition (`Err`).
    let Some(cmd) = newer_format(cmd, cmd_bytes)? else {
        return Ok(ApplyOutcome::Rejected(RejectReason::Invalid(
            InvalidCode::Empty,
        )));
    };

    let current = cur.epoch();
    if let Cmd::LeaderAnnounced(la) = &cmd {
        if la.epoch > current.0 && env_epoch.0 == la.epoch {
            cur.meta.epoch = la.epoch;
            cur.meta.epoch_history.push((la.epoch, off.0));
            // P-9 (06-03): suspended runs with in-flight effects wake on leader start. One
            // effects scan (+ a record read per run with rows); a no-op without rows.
            effects::wake_effect_runs(b, actions)?;
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
        // Phase 8 (08-07): watermark, overlap, manual and buffered fires (`schedule.rs`).
        Cmd::ScheduleFire(s) => schedule::apply_schedule_fire(b, cur, s, actions),
        // Phase 6 (06-03): first completion wins, 2 reads (`effects.rs`).
        Cmd::CompleteEffect(c) => effects::apply_complete_effect(b, cur, c, actions),
        // Phase 7 (07-03 routing stubs; implemented in 07-04): pause / resume (`pause.rs`).
        Cmd::PauseRun(p) => pause::apply_pause_run(b, cur, p, actions),
        Cmd::ResumeRun(r) => pause::apply_resume_run(b, cur, r, actions),
        // Phase 11 (11-02 contract, 11-08 semantics): dead-letter marks (`dlq.rs`).
        Cmd::DlqUpdate(d) => dlq::apply_dlq_update(b, cur, d, actions),
        // Phase 13 (13-01 contract, 13-17 semantics): repin a run (`repin.rs`).
        Cmd::RepinRun(r) => repin::apply_repin_run(b, cur, r, actions),
    }
}

/// The `reject_and_continue` mutant (RESERVATIONS §11): unknown input is rejected as
/// `Invalid(Empty)` and apply continues (the pre-13-04 behaviour).
pub const MUTANT_REJECT_AND_CONTINUE: &str = "upgrade::mutant::reject_and_continue";

#[cfg(feature = "failpoints")]
fn reject_and_continue() -> bool {
    fail::eval(MUTANT_REJECT_AND_CONTINUE, |_| ()).is_some()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn reject_and_continue() -> bool {
    false
}

/// P13-F1 rule 2 (D-P13-6): fail-stop on a command written by a newer Ostro.
///
/// - The prost re-encoding is shorter than the bytes: the command carries fields this build
///   does not know (prost skips them on decode). Prost re-encodes every frozen golden and
///   every format-corpus command to exactly its bytes (`tests/apply_newer_format.rs`), so a
///   length mismatch means unknown input, never a re-encoding artefact.
/// - No known oneof variant in non-empty bytes: an unknown `Cmd` tag.
/// - `level-emulation`: a command of a feature above `level_max()` (an emulated older
///   binary treats it as unknown).
///
/// `Ok(None)` = genuinely empty bytes (`Rejected(Invalid(Empty))`, unchanged).
fn newer_format(cmd: ostro_types::Command, cmd_bytes: &[u8]) -> Result<Option<Cmd>, StoreError> {
    use prost::Message;
    let unknown = if cmd.cmd.is_none() && !cmd_bytes.is_empty() {
        Some("unknown command tag".to_string())
    } else if cmd.encoded_len() != cmd_bytes.len() {
        Some("command carries fields unknown to this version".to_string())
    } else if let Some(f) = cmd.cmd.as_ref().and_then(gated_feature)
        && f.introduced_at() > ostro_types::compat::level_max()
    {
        Some(format!(
            "command {} needs feature level {} (this binary supports up to {})",
            f.name(),
            f.introduced_at(),
            ostro_types::compat::level_max()
        ))
    } else {
        None
    };
    match unknown {
        Some(_) if reject_and_continue() => Ok(None),
        Some(why) => Err(StoreError::NewerFormat(why)),
        None => Ok(cmd.cmd),
    }
}

/// The gated feature of a known command (`ostro_types::compat::feature_for_cmd_tag`).
fn gated_feature(cmd: &Cmd) -> Option<ostro_types::compat::Feature> {
    match cmd {
        Cmd::RepinRun(_) => ostro_types::compat::feature_for_cmd_tag(43),
        _ => None,
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
