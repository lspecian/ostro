//! The engine seam for host I/O (Phase 6, plan 06-06): [`EffectDriver`].
//!
//! The engine never performs effect I/O itself. When a run schedules an effect
//! (`http-fetch`, `sql-query`, `object-store`) the host keeps the guest's full request as
//! a [`LiveEffect`] and hands it to the activation's driver, which the sink supplies
//! through [`ActivationSink::effect_driver`](crate::sink::ActivationSink::effect_driver)
//! (P-1; `None` keeps the Phase 4/5 behaviour: nothing is ever started).
//!
//! Rules (frozen for Phase 6 and later phases):
//!
//! - **FX-01: I/O only after commit.** [`EffectDriver::start`] is called from
//!   `HostState::flush` only after the append carrying the op's `EffectScheduled` returned
//!   `Ok`. A failed, conflicting or fenced append starts nothing; the pending requests of
//!   that batch are dropped with the activation.
//! - **F1: never re-send from the journal.** The journal's `HttpRequest` mirror has no
//!   retry policy, and the values of headers named in `http-request.redact` are journaled
//!   as `"[redacted]"`. A re-drive therefore needs the guest's *re-issued* request: when a
//!   replaying activation re-issues an effect whose `EffectScheduled` has no completion
//!   (and no `OpCancelled`) in the loaded journal, the host calls `start` with
//!   `redrive = true`, the re-issued full request and the journaled event's timestamp. An
//!   op whose completion is in the loaded journal is never started. No leader-side sweeper
//!   may re-send from the journal alone.
//! - **Idempotent per `(run, op)`.** A live start and a later re-drive of the same op (or
//!   two re-drives from successive activations) may both reach the driver; the second call
//!   must attach to the running task, never send twice.
//! - **Non-blocking.** `start` and `cancel` are synchronous and are called on the
//!   activation's task; the driver spawns its own work.
//! - **Cancellation.** `cancel-op` on an effect calls [`EffectDriver::cancel`] after the
//!   `OpCancelled` append commits (best effort: the side effect may already have happened).
//!   An effect scheduled and cancelled in the same committed batch is never started.
//!
//! Phase 7: remote workers supply their own `EffectDriver` and reuse
//! [`ostro_types::live`] unchanged.

use ostro_types::live::LiveEffect;

use crate::sink::RunRef;

/// Starts and cancels effect I/O for activations (implemented by the effects bridge).
pub trait EffectDriver: Send + Sync {
    /// Called after `EffectScheduled(op)` is committed (live, `redrive = false`) or found
    /// without completion while replaying (`redrive = true`).
    ///
    /// Must be idempotent per `(run, op)`: a second call attaches to the running task.
    fn start(&self, run: &RunRef, attempt: u32, effect: LiveEffect);

    /// Best effort: cancel the effect `op` of `run` (after its `OpCancelled` committed).
    /// Default no-op.
    fn cancel(&self, _run: &RunRef, _op: u64) {}
}
