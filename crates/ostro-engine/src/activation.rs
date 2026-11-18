//! Activation driver (plan 03-08): one guest run attempt against an [`ActivationSink`]
//! (ENG-03, ENG-05, ENG-09).
//!
//! An activation instantiates the prepared component in a fresh store over the
//! committed journal, calls `run`, flushes the live events it journaled and reports
//! exactly one [`ActivationOutcome`] via `sink.done`. A wait that cannot be satisfied
//! either parks hot (when `Activate.inputs` is provided and the wait can resolve
//! within `engine.hot_wait`, see `host::effects`) or traps with [`Evict`]; the driver
//! flushes the pending events after the trap unwinds and drops the store, and the next
//! dispatch replays the (longer) journal.
//!
//! Every attempt-ending error is classified by [`classify`] into an
//! [`AttemptFailure`] reason code; a failure ends only this attempt (the store is
//! dropped, the memory budget released).

use std::sync::Arc;
use std::time::Instant;

use ostro_types::journal::Failure;
use tracing::Instrument;
use wasmtime::{Store, Trap};

use crate::engine::configure_store;
use crate::error::{
    AttemptFailure, Evict, EvictReason, Fenced, GuestExit, NonDeterminism, ProtocolViolation,
};
use crate::host::bindings::ostro::runtime::types as wit_types;
use crate::host::state::{HostInit, HostState, HotWait, RunReturn, run_return_from_wit};
use crate::limits::{Breach, HasLimits};
use crate::replay::ReplayError;
use crate::runtime::{Prepared, Runtime};
use crate::sink::{Activate, ActivationOutcome, ActivationSink, AppendError};

/// Result of classifying an error returned by `call_run` (a trap).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Classified {
    /// Not a failure: the activation is evicted and the run resumes later.
    Evict(EvictReason),
    /// The attempt failed.
    Failure(AttemptFailure),
    /// A flush inside the guest call (before parking a hot wait) was fenced.
    Fenced,
}

/// Classify a `call_run` error.
///
/// Order: [`NonDeterminism`] → [`Evict`] → [`Fenced`] → [`ProtocolViolation`] → [`GuestExit`] →
/// limiter breach (`Breach::Memory`/`Table` → `MemoryExceeded`; a memory cap breach
/// is a limiter error, not a [`Trap`]) → `Trap::OutOfFuel` → `FuelExhausted`,
/// `Trap::Interrupt` → `Timeout`, `Trap::StackOverflow` → `StackOverflow` →
/// otherwise `Trap(message)` with the guest's stderr tail appended (a Rust panic
/// message reaches stderr before the `unreachable` trap).
pub fn classify(e: &wasmtime::Error, breach: Option<Breach>, stderr_tail: &str) -> Classified {
    if let Some(nd) = e.downcast_ref::<NonDeterminism>() {
        return Classified::Failure(AttemptFailure::NonDeterminism(nd.clone()));
    }
    if let Some(ev) = e.downcast_ref::<Evict>() {
        return Classified::Evict(ev.reason);
    }
    if e.downcast_ref::<Fenced>().is_some() {
        return Classified::Fenced;
    }
    if let Some(pv) = e.downcast_ref::<ProtocolViolation>() {
        return Classified::Failure(AttemptFailure::ProtocolViolation(pv.0.clone()));
    }
    if let Some(GuestExit(code)) = e.downcast_ref::<GuestExit>() {
        return Classified::Failure(AttemptFailure::GuestExit(*code));
    }
    if breach.is_some() || e.downcast_ref::<crate::error::MemoryExceeded>().is_some() {
        return Classified::Failure(AttemptFailure::MemoryExceeded);
    }
    match e.downcast_ref::<Trap>() {
        Some(Trap::OutOfFuel) => return Classified::Failure(AttemptFailure::FuelExhausted),
        Some(Trap::Interrupt) => return Classified::Failure(AttemptFailure::Timeout),
        Some(Trap::StackOverflow) => return Classified::Failure(AttemptFailure::StackOverflow),
        _ => {}
    }
    let mut msg = format!("{e:#}");
    let tail = stderr_tail.trim_end();
    if !tail.is_empty() {
        msg.push_str("; stderr: ");
        msg.push_str(tail);
    }
    Classified::Failure(AttemptFailure::Trap(msg))
}

/// Stable metrics label of an outcome.
pub fn outcome_code(o: &ActivationOutcome) -> &'static str {
    match o {
        ActivationOutcome::Completed { .. } => "completed",
        ActivationOutcome::Failed { .. } => "failed",
        ActivationOutcome::ContinuedAsNew { .. } => "continued_as_new",
        ActivationOutcome::Evicted { .. } => "evicted",
        ActivationOutcome::AttemptFailed(f) => f.reason_code(),
        ActivationOutcome::Fenced => "fenced",
    }
}

/// Map a failed flush to the activation outcome.
fn flush_error_outcome(e: AppendError) -> ActivationOutcome {
    match e {
        AppendError::Fenced => ActivationOutcome::Fenced,
        // The next dispatch replays the committed journal.
        AppendError::Conflict { .. } => ActivationOutcome::Evicted {
            reason: EvictReason::AppendConflict,
        },
        AppendError::Unavailable(_) => ActivationOutcome::Evicted {
            reason: EvictReason::External,
        },
    }
}

/// Flush pending events, then report `ok`; a flush error replaces it.
async fn flush_then(st: &mut HostState, ok: ActivationOutcome) -> ActivationOutcome {
    match st.flush().await {
        Ok(()) => ok,
        Err(e) => flush_error_outcome(e),
    }
}

fn replay_failure(e: ReplayError) -> AttemptFailure {
    match e {
        ReplayError::NonDeterminism(nd) => AttemptFailure::NonDeterminism(nd),
        ReplayError::Protocol(p) => AttemptFailure::ProtocolViolation(p.0),
        ReplayError::BadJournal(m) => {
            AttemptFailure::ProtocolViolation(format!("bad journal: {m}"))
        }
    }
}

/// Run one activation and report its outcome via `sink.done` exactly once.
pub(crate) async fn run_activation(
    rt: &Runtime,
    p: &Prepared,
    act: Activate,
    sink: Arc<dyn ActivationSink>,
) -> ActivationOutcome {
    let span = tracing::info_span!(
        "activation",
        partition = act.run.partition,
        run = %act.run.run_id,
        attempt = act.attempt,
    );
    let run = act.run.clone();
    let attempt = act.attempt;
    let started = Instant::now();
    let outcome = execute(rt, p, act, sink.clone()).instrument(span).await;
    metrics::counter!("ostro_activation_outcomes_total", "outcome" => outcome_code(&outcome))
        .increment(1);
    metrics::histogram!("ostro_activation_duration_seconds")
        .record(started.elapsed().as_secs_f64());
    sink.done(&run, attempt, outcome.clone()).await;
    outcome
}

/// The activation body; the store is dropped (memory budget released) when it returns.
async fn execute(
    rt: &Runtime,
    p: &Prepared,
    act: Activate,
    sink: Arc<dyn ActivationSink>,
) -> ActivationOutcome {
    let engine = rt.engine();
    let cfg = engine.config();
    let Activate {
        run,
        attempt,
        journal,
        inputs,
        cancel,
        ..
    } = act;
    // Without an input channel every unsatisfied wait evicts.
    let hot = inputs.map(|inputs| HotWait {
        inputs,
        cancel: cancel.clone(),
        parked: rt.parked().clone(),
        hot_wait: cfg.hot_wait,
    });
    let state = match HostState::new(HostInit {
        run,
        attempt,
        module_hash_hex: p.module_hash_hex.clone(),
        journal,
        clock: rt.opts.clock.clone(),
        limiter: engine.new_limiter(),
        wall_deadline: Instant::now() + cfg.activation_timeout,
        token_key: rt.opts.token_key,
        continue_as_new_events: cfg.continue_as_new_events,
        sink: Some(sink),
        hot,
    }) {
        Ok(s) => s,
        Err(e) => {
            return ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(match e {
                ReplayError::BadJournal(m) => format!("bad journal: {m}"),
                other => format!("bad journal: {other}"),
            }));
        }
    };
    let (workflow, input) = {
        let rs = state.replay().run();
        (
            rs.workflow.clone(),
            wit_types::Payload {
                content_type: rs.input.content_type.clone(),
                data: rs.input.data.clone(),
            },
        )
    };
    let mut store = Store::new(engine.engine(), state);
    if let Err(e) = configure_store(&mut store, cfg.activation_fuel) {
        return ActivationOutcome::AttemptFailed(AttemptFailure::Trap(format!("{e:#}")));
    }
    let wf = match p.pre.instantiate_async(&mut store).await {
        Ok(wf) => wf,
        Err(e) => {
            return ActivationOutcome::AttemptFailed(AttemptFailure::Trap(
                engine.explain_instantiate_error(&e),
            ));
        }
    };

    let result = {
        let call = wf
            .ostro_runtime_workflows()
            .call_run(&mut store, &workflow, &input);
        // Cancellation wins ties: an already-cancelled activation never runs guest code.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            r = call => Some(r),
        }
    };

    let st = store.data_mut();
    let outcome = match result {
        // External eviction: the call future (and its fiber) is already dropped.
        None => {
            flush_then(
                st,
                ActivationOutcome::Evicted {
                    reason: EvictReason::External,
                },
            )
            .await
        }
        Some(Ok(r)) => {
            let ret: Result<RunReturn, Failure> = run_return_from_wit(r);
            match st.record_terminal(&ret) {
                Err(ReplayError::NonDeterminism(nd)) => {
                    // A diverged attempt never extends the journal.
                    ActivationOutcome::AttemptFailed(AttemptFailure::NonDeterminism(nd))
                }
                Err(e) => {
                    let f = replay_failure(e);
                    flush_then(st, ActivationOutcome::AttemptFailed(f)).await
                }
                Ok(()) => {
                    let ok = match ret {
                        Ok(RunReturn::Completed(output)) => ActivationOutcome::Completed { output },
                        Ok(RunReturn::ContinueAsNew(continuation)) => {
                            ActivationOutcome::ContinuedAsNew { continuation }
                        }
                        Err(failure) => ActivationOutcome::Failed { failure },
                    };
                    flush_then(st, ok).await
                }
            }
        }
        Some(Err(e)) => {
            let breach = st.limiter().breach;
            match classify(&e, breach, &st.stderr_tail()) {
                Classified::Evict(reason) => {
                    flush_then(st, ActivationOutcome::Evicted { reason }).await
                }
                // The attempt lost ownership: nothing more may be appended.
                Classified::Fenced => ActivationOutcome::Fenced,
                Classified::Failure(f @ AttemptFailure::NonDeterminism(_)) => {
                    ActivationOutcome::AttemptFailed(f)
                }
                // Best effort: completed live steps survive into the retry.
                Classified::Failure(f) => flush_then(st, ActivationOutcome::AttemptFailed(f)).await,
            }
        }
    };
    if let ActivationOutcome::AttemptFailed(f) = &outcome {
        tracing::warn!(reason = f.reason_code(), detail = %f.dlq_reason(), "activation attempt failed");
    }
    drop(store);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(e: wasmtime::Error) -> Classified {
        classify(&e, None, "")
    }

    #[test]
    fn classify_typed_errors() {
        let nd = NonDeterminism {
            seq: 4,
            expected: "a".into(),
            actual: "b".into(),
        };
        assert_eq!(
            class(wasmtime::Error::new(nd.clone())),
            Classified::Failure(AttemptFailure::NonDeterminism(nd))
        );
        assert_eq!(
            class(wasmtime::Error::new(Evict {
                reason: EvictReason::WaitUnsatisfied
            })),
            Classified::Evict(EvictReason::WaitUnsatisfied)
        );
        assert_eq!(
            class(wasmtime::Error::new(ProtocolViolation("twice".into()))),
            Classified::Failure(AttemptFailure::ProtocolViolation("twice".into()))
        );
        assert_eq!(class(wasmtime::Error::new(Fenced)), Classified::Fenced);
        assert_eq!(
            class(wasmtime::Error::new(GuestExit(7))),
            Classified::Failure(AttemptFailure::GuestExit(7))
        );
    }

    #[test]
    fn classify_context_wrapped() {
        let e = wasmtime::Error::new(Evict {
            reason: EvictReason::WaitUnsatisfied,
        })
        .context("while calling wait");
        assert_eq!(class(e), Classified::Evict(EvictReason::WaitUnsatisfied));
    }

    #[test]
    fn classify_limits_and_traps() {
        assert_eq!(
            classify(
                &wasmtime::format_err!("grow failed"),
                Some(Breach::Memory),
                ""
            ),
            Classified::Failure(AttemptFailure::MemoryExceeded)
        );
        assert_eq!(
            classify(&wasmtime::format_err!("table"), Some(Breach::Table), ""),
            Classified::Failure(AttemptFailure::MemoryExceeded)
        );
        assert_eq!(
            class(wasmtime::Error::new(crate::error::MemoryExceeded {
                requested: 2,
                limit: 1
            })),
            Classified::Failure(AttemptFailure::MemoryExceeded)
        );
        assert_eq!(
            class(wasmtime::Error::new(Trap::OutOfFuel)),
            Classified::Failure(AttemptFailure::FuelExhausted)
        );
        assert_eq!(
            class(wasmtime::Error::new(Trap::Interrupt)),
            Classified::Failure(AttemptFailure::Timeout)
        );
        assert_eq!(
            class(wasmtime::Error::new(Trap::StackOverflow)),
            Classified::Failure(AttemptFailure::StackOverflow)
        );
    }

    #[test]
    fn classify_other_trap_appends_stderr() {
        let c = classify(
            &wasmtime::Error::new(Trap::UnreachableCodeReached),
            None,
            "thread panicked at src/lib.rs:1:1:\nboom\n",
        );
        match c {
            Classified::Failure(AttemptFailure::Trap(msg)) => {
                assert!(msg.contains("unreachable"), "{msg}");
                assert!(msg.ends_with("stderr: thread panicked at src/lib.rs:1:1:\nboom"));
            }
            other => panic!("{other:?}"),
        }
        match class(wasmtime::format_err!("plain")) {
            Classified::Failure(AttemptFailure::Trap(msg)) => assert_eq!(msg, "plain"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn flush_errors_map_to_outcomes() {
        assert_eq!(
            flush_error_outcome(AppendError::Fenced),
            ActivationOutcome::Fenced
        );
        assert_eq!(
            flush_error_outcome(AppendError::Conflict { expected: 3 }),
            ActivationOutcome::Evicted {
                reason: EvictReason::AppendConflict
            }
        );
        assert_eq!(
            flush_error_outcome(AppendError::Unavailable("x".into())),
            ActivationOutcome::Evicted {
                reason: EvictReason::External
            }
        );
    }

    #[test]
    fn outcome_codes() {
        assert_eq!(
            outcome_code(&ActivationOutcome::AttemptFailed(AttemptFailure::Timeout)),
            "timeout"
        );
        assert_eq!(outcome_code(&ActivationOutcome::Fenced), "fenced");
    }
}
