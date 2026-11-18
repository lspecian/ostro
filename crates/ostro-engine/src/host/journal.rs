//! Host impl of `ostro:runtime/journal` (WIT-01): durable steps as a `resource step`.
//!
//! `begin-step` hashes `CommandArgs::Step { name }` and asks the replay core: a
//! recorded `StepCompleted` is decoded from its exact journaled bytes (ENG-04) and
//! returned as `replayed`; otherwise a seq is reserved and a [`StepRep`] handle is
//! returned as `execute`. While that step is open, every journaled command is a
//! protocol violation (the replay core's step guard). `complete` journals the outcome at
//! the reserved seq. `drop` is observability only: an uncompleted step is abandoned and
//! counted, and correctness never depends on drop (TS never drops completed steps).
//!
//! Per-step fuel (ENG-05): [`build_linker`](crate::host::state::build_linker) shadows
//! `begin-step` and `[method]step.complete` with store-aware functions that call
//! [`HostState::begin_step_impl`] / [`HostState::complete_step_impl`] with the store's
//! fuel. The bindgen trait methods below are the store-less fallbacks (fuel unknown).
//!
//! Step retry (WF-02, plan 04-07): a failed live attempt is evaluated with
//! [`retry::decide`] against the step's policy. A retry due within `engine.hot_wait`
//! journals `StepAttemptFailed(InActivation)`, flushes it, sleeps the delay in the host
//! and returns `retry-now` (the guest re-runs the body on the same handle, now bound to
//! the next seq). A longer delay journals `StepAttemptFailed(Requeue)` and traps
//! `Evict { StepBackoff { retry_at_nanos } }` (the driver flushes after unwind). A
//! re-activation that resumes a step whose last attempt's `retry_at` is still in the
//! future re-evicts with `StepBackoff` without running the body. Replayed attempt
//! chains are consumed by the replay core and never sleep.
//!
//! Step timeouts and per-step fuel (WF-02, plan 04-10): a live attempt with `timeout-ms`
//! gets a wall-clock deadline (`begin + timeout`, re-armed on `retry-now`) that the
//! store's epoch-deadline callback compares against; when it passes, the guest traps
//! with [`StepTimedOut`](crate::error::StepTimedOut). A body that exhausts its step fuel
//! budget traps `OutOfFuel`. Both unwind the guest, so the retry cannot happen in place:
//! after the unwind the driver calls [`HostState::fail_interrupted_step`], which records
//! the open attempt's failure through the same [`retry::decide`] path (`Timeout` is
//! retryable; a fuel breach is `NonRetryable { kind: "fuel_exhausted" }`). A retry
//! journals `StepAttemptFailed(Requeue)` and evicts `StepBackoff { retry_at }`; no retry
//! left journals `StepCompleted(Err(..))` and evicts `StepInterrupted` (re-activate at
//! once; the run continues from replay).

use std::time::{Duration, Instant};

use ostro_types::journal::{
    AttemptBackoff, CommandArgs, CommandMeta, ErrorInfo, EventBody, Failure, HASH_VER, Payload,
    StepAttemptFailed, StepCompleted, command_hash, decode_result, encode_result,
};
use wasmtime::component::Resource;

use crate::env::GuestEnv;
use crate::error::{Evict, EvictReason};
use crate::host::bindings::StepRep;
use crate::host::bindings::ostro::runtime::journal::{CompletionAck, Host, HostStep, StepStart};
use crate::host::bindings::ostro::runtime::types::{self as wit_types, StepOptions};
use crate::host::effects::{convert, flush_trap};
use crate::host::state::{HostState, OpenStep, StepFuel, trap, violation};
use crate::replay::{ReplayError, StepBegin};
use crate::retry::{self, RetryDecision, RetryPolicy};

fn retry_policy_from_wit(p: wit_types::RetryPolicy) -> RetryPolicy {
    RetryPolicy {
        max_attempts: p.max_attempts,
        initial_interval_ms: p.initial_interval_ms,
        max_interval_ms: p.max_interval_ms,
        backoff_coefficient: p.backoff_coefficient,
        non_retryable_kinds: p.non_retryable_kinds,
    }
}

fn evict<T>(reason: EvictReason) -> wasmtime::Result<T> {
    Err(wasmtime::Error::new(Evict { reason }))
}

/// Wall-clock deadline of an attempt starting now (`None`: no timeout or overflow).
fn attempt_deadline(timeout_ms: Option<u64>) -> Option<Instant> {
    timeout_ms.and_then(|t| Instant::now().checked_add(Duration::from_millis(t)))
}

/// Why the open live step's guest code was unwound (plan 04-10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepInterrupt {
    /// The epoch callback trapped with `StepTimedOut { seq }`.
    Timeout { seq: u64 },
    /// `Trap::OutOfFuel` while the step's fuel budget was the binding limit.
    FuelExhausted,
}

fn step_command_meta(name: &str) -> Result<CommandMeta, ReplayError> {
    let hash = command_hash(HASH_VER, &CommandArgs::Step { name })
        .map_err(|e| ReplayError::BadJournal(format!("step hash: {e:?}")))?;
    Ok(CommandMeta {
        hash_ver: HASH_VER,
        hash,
    })
}

impl HostState {
    /// `begin-step` with the store's remaining fuel (`None` if fuel is unavailable).
    ///
    /// Returns the guest result and, for a live step with `opts.fuel`, the fuel to set
    /// for the step body (`min(budget, remaining)`).
    pub(crate) fn begin_step_impl(
        &mut self,
        name: String,
        opts: StepOptions,
        fuel: Option<u64>,
    ) -> wasmtime::Result<(StepStart, Option<u64>)> {
        let hash = self.hash(&CommandArgs::Step { name: &name })?;
        let begin = self
            .replay
            .begin_step(hash, &|| format!("step {name:?}"))
            .map_err(trap)?;
        match begin {
            StepBegin::Replayed(ev) => {
                let EventBody::StepCompleted(sc) = &ev.body else {
                    return Err(violation(format!(
                        "bad journal: begin-step matched {}",
                        ev.body.summary()
                    )));
                };
                let outcome: Result<Payload, Failure> =
                    decode_result(&sc.outcome).map_err(|e| {
                        violation(format!(
                            "bad journal: step {:?} outcome at seq {}: {e:?}",
                            sc.name, ev.seq
                        ))
                    })?;
                Ok((
                    StepStart::Replayed(convert::step_outcome_to_wit(outcome)),
                    None,
                ))
            }
            StepBegin::Live {
                seq,
                attempt,
                last_retry_at_nanos,
            } => {
                // Resuming a step whose next attempt is not due yet: hand the run back
                // to the scheduler without running the body (nothing is journaled).
                if let Some(at) = last_retry_at_nanos
                    && at > self.replay.live_now_nanos()
                {
                    self.replay.abandon_step(seq);
                    return evict(EvictReason::StepBackoff { retry_at_nanos: at });
                }
                let (start, outer, set) = match (fuel, opts.fuel) {
                    (Some(rem), Some(budget)) => {
                        let s = budget.min(rem);
                        (s, Some(rem), Some(s))
                    }
                    (Some(rem), None) => (rem, None, None),
                    (None, _) => (0, None, None),
                };
                self.step_fuel = fuel.map(|_| StepFuel { seq, start, outer });
                let rep = StepRep {
                    seq,
                    name,
                    fuel_before: fuel,
                    attempt,
                    retry: opts.retry.map(retry_policy_from_wit),
                    timeout_ms: opts.timeout_ms,
                    fuel_budget: opts.fuel,
                };
                self.open_step = Some(OpenStep {
                    deadline: attempt_deadline(rep.timeout_ms),
                    rep: rep.clone(),
                });
                let rep = GuestEnv::table(self).push(rep)?;
                Ok((StepStart::Execute(rep), set))
            }
        }
    }

    /// `[method]step.complete` with the store's remaining fuel.
    ///
    /// Returns the ack and the fuel to set: when the step ran under a fuel budget, the
    /// outer fuel to restore (`outer remaining - fuel used`) after a recorded
    /// completion, or the next attempt's budget after `retry-now`. A failure is
    /// evaluated against the step's retry policy (see the module docs); only steps with
    /// a policy read the clock.
    pub(crate) async fn complete_step_impl(
        &mut self,
        step: Resource<StepRep>,
        outcome: Result<wit_types::Payload, wit_types::Failure>,
        fuel: Option<u64>,
    ) -> wasmtime::Result<(CompletionAck, Option<u64>)> {
        let rep = GuestEnv::table(self).get(&step)?.clone();
        if self.completed_steps.contains(&rep.seq) {
            return Err(violation("step completed twice"));
        }
        let sf = self.step_fuel.take_if(|s| s.seq == rep.seq);
        let (fuel_used, restore) = match (sf, fuel) {
            (Some(sf), Some(now)) => {
                let used = sf.start.saturating_sub(now);
                (used, sf.outer.map(|o| o.saturating_sub(used)))
            }
            _ => (0, None),
        };
        let hash = command_hash(HASH_VER, &CommandArgs::Step { name: &rep.name })
            .map_err(|e| violation(format!("step hash: {e:?}")))?;
        let meta = CommandMeta {
            hash_ver: HASH_VER,
            hash,
        };
        let outcome = convert::step_outcome_from_wit(outcome);
        if let (Err(failure), Some(policy)) = (&outcome, &rep.retry) {
            let now = self.replay.live_now_nanos();
            if let RetryDecision::Retry {
                retry_at_nanos,
                delay,
                backoff,
            } = retry::decide(Some(policy), rep.attempt, failure, now, self.retry_window)
            {
                let next = self
                    .replay
                    .record_attempt_failed(
                        rep.seq,
                        EventBody::StepAttemptFailed(StepAttemptFailed {
                            meta,
                            name: rep.name.clone(),
                            attempt: rep.attempt,
                            failure: failure.clone(),
                            retry_at_nanos,
                            backoff,
                            fuel_used,
                        }),
                    )
                    .map_err(trap)?;
                {
                    let r = GuestEnv::table(self).get_mut(&step)?;
                    r.seq = next;
                    r.attempt = rep.attempt.saturating_add(1);
                    r.fuel_before = fuel;
                }
                if backoff == AttemptBackoff::Requeue {
                    self.open_step = None;
                    // The driver flushes the attempt failure after the trap unwinds.
                    metrics::counter!("ostro_step_retries_total", "backoff" => "requeue")
                        .increment(1);
                    return evict(EvictReason::StepBackoff { retry_at_nanos });
                }
                // Re-arm per-step fuel accounting for the next attempt.
                let set = match (sf, fuel) {
                    (Some(sf), Some(now)) => match (sf.outer, rep.fuel_budget) {
                        (Some(outer), Some(budget)) => {
                            let outer = outer.saturating_sub(fuel_used);
                            let start = budget.min(outer);
                            self.step_fuel = Some(StepFuel {
                                seq: next,
                                start,
                                outer: Some(outer),
                            });
                            Some(start)
                        }
                        _ => {
                            self.step_fuel = Some(StepFuel {
                                seq: next,
                                start: now,
                                outer: None,
                            });
                            None
                        }
                    },
                    _ => None,
                };
                // The leader must know about the failed attempt before we wait.
                self.flush().await.map_err(flush_trap)?;
                let cancel = self
                    .retry_cancel
                    .clone()
                    .or_else(|| self.hot.as_ref().map(|h| h.cancel.clone()));
                retry_sleep(delay, cancel).await?;
                // The next attempt's timeout starts when its body starts (after the sleep).
                let next_rep = GuestEnv::table(self).get(&step)?.clone();
                self.open_step = Some(OpenStep {
                    deadline: attempt_deadline(next_rep.timeout_ms),
                    rep: next_rep,
                });
                metrics::counter!("ostro_step_retries_total", "backoff" => "in_activation")
                    .increment(1);
                return Ok((CompletionAck::RetryNow, set));
            }
        }
        let recorded = encode_result(&outcome);
        self.replay
            .complete_step(
                rep.seq,
                EventBody::StepCompleted(StepCompleted {
                    meta,
                    name: rep.name,
                    outcome: recorded,
                    fuel_used,
                }),
            )
            .map_err(trap)?;
        self.completed_steps.insert(rep.seq);
        self.open_step.take_if(|o| o.rep.seq == rep.seq);
        Ok((CompletionAck::Recorded, restore))
    }

    /// Whether an `OutOfFuel` trap is attributable to the open step's fuel budget: a
    /// budget was applied and it was lower than the outer remaining fuel (if the budget
    /// was not the binding limit, the activation's own fuel ran out).
    pub(crate) fn step_fuel_breach(&self) -> bool {
        match (&self.open_step, self.step_fuel) {
            (Some(o), Some(sf)) => {
                sf.seq == o.rep.seq && sf.outer.is_some_and(|out| sf.start < out)
            }
            _ => false,
        }
    }

    /// Record the failure of the open live step whose guest code was unwound by its
    /// timeout or fuel budget (called by the activation driver after the trap, with the
    /// store's remaining fuel).
    ///
    /// Returns `None` if the interrupt does not belong to the open step (the caller then
    /// classifies the trap as an attempt failure). Otherwise the failure goes through
    /// [`retry::decide`] with a zero in-activation window (the guest stack is gone, so a
    /// retry always re-dispatches): `StepAttemptFailed(Requeue)` -> `StepBackoff`, or
    /// `StepCompleted(Err(..))` -> `StepInterrupted`. The caller flushes.
    pub(crate) fn fail_interrupted_step(
        &mut self,
        cause: StepInterrupt,
        fuel: Option<u64>,
    ) -> Option<Result<EvictReason, ReplayError>> {
        let open = self.open_step.clone()?;
        let rep = open.rep;
        let failure = match cause {
            StepInterrupt::Timeout { seq } if seq == rep.seq => Failure::Timeout,
            StepInterrupt::Timeout { .. } => return None,
            StepInterrupt::FuelExhausted if self.step_fuel_breach() => {
                let budget = rep.fuel_budget.unwrap_or_default();
                Failure::NonRetryable(ErrorInfo {
                    kind: "fuel_exhausted".to_string(),
                    message: format!("step fuel budget of {budget} exhausted"),
                    details: None,
                })
            }
            StepInterrupt::FuelExhausted => return None,
        };
        self.open_step = None;
        let fuel_used = match (self.step_fuel.take_if(|s| s.seq == rep.seq), fuel) {
            (Some(sf), Some(now)) => sf.start.saturating_sub(now),
            (Some(sf), None) => sf.start,
            _ => 0,
        };
        Some(self.record_interrupted(rep, failure, fuel_used))
    }

    fn record_interrupted(
        &mut self,
        rep: StepRep,
        failure: Failure,
        fuel_used: u64,
    ) -> Result<EvictReason, ReplayError> {
        let meta = step_command_meta(&rep.name)?;
        let now = self.replay.live_now_nanos();
        metrics::counter!(
            "ostro_step_interrupts_total",
            "cause" => if failure == Failure::Timeout { "timeout" } else { "fuel" }
        )
        .increment(1);
        if let RetryDecision::Retry { retry_at_nanos, .. } = retry::decide(
            rep.retry.as_ref(),
            rep.attempt,
            &failure,
            now,
            Duration::ZERO,
        ) {
            self.replay.record_attempt_failed(
                rep.seq,
                EventBody::StepAttemptFailed(StepAttemptFailed {
                    meta,
                    name: rep.name,
                    attempt: rep.attempt,
                    failure,
                    retry_at_nanos,
                    // The guest stack was unwound: the retry always re-dispatches.
                    backoff: AttemptBackoff::Requeue,
                    fuel_used,
                }),
            )?;
            metrics::counter!("ostro_step_retries_total", "backoff" => "requeue").increment(1);
            return Ok(EvictReason::StepBackoff { retry_at_nanos });
        }
        self.replay.complete_step(
            rep.seq,
            EventBody::StepCompleted(StepCompleted {
                meta,
                name: rep.name,
                outcome: encode_result(&Err::<Payload, Failure>(failure)),
                fuel_used,
            }),
        )?;
        self.completed_steps.insert(rep.seq);
        Ok(EvictReason::StepInterrupted)
    }
}

/// Sleep an in-activation retry delay; external eviction (cancel) wins.
async fn retry_sleep(
    delay: std::time::Duration,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> wasmtime::Result<()> {
    match cancel {
        Some(c) => tokio::select! {
            biased;
            _ = c.cancelled() => evict(EvictReason::External),
            _ = tokio::time::sleep(delay) => Ok(()),
        },
        None => {
            tokio::time::sleep(delay).await;
            Ok(())
        }
    }
}

impl Host for HostState {
    async fn begin_step(&mut self, name: String, opts: StepOptions) -> wasmtime::Result<StepStart> {
        // Store-less fallback; the production linker shadows this with a fuel-aware one.
        self.begin_step_impl(name, opts, None).map(|(s, _)| s)
    }
}

impl HostStep for HostState {
    async fn seq(&mut self, step: Resource<StepRep>) -> wasmtime::Result<u64> {
        Ok(GuestEnv::table(self).get(&step)?.seq)
    }

    async fn complete(
        &mut self,
        step: Resource<StepRep>,
        outcome: Result<wit_types::Payload, wit_types::Failure>,
    ) -> wasmtime::Result<CompletionAck> {
        // Store-less fallback; the production linker shadows this with a fuel-aware one.
        self.complete_step_impl(step, outcome, None)
            .await
            .map(|(a, _)| a)
    }

    async fn drop(&mut self, step: Resource<StepRep>) -> wasmtime::Result<()> {
        let rep = GuestEnv::table(self).delete(step)?;
        if self.completed_steps.contains(&rep.seq) {
            return Ok(());
        }
        self.replay.abandon_step(rep.seq);
        self.open_step.take_if(|o| o.rep.seq == rep.seq);
        if let Some(sf) = self.step_fuel.take_if(|s| s.seq == rep.seq)
            && let Some(outer) = sf.outer
        {
            // No store access here: charge the whole step budget (conservative) and
            // restore the outer fuel at the next store-aware call.
            self.fuel_restore = Some(outer.saturating_sub(sf.start));
        }
        metrics::counter!("ostro_step_abandoned_total").increment(1);
        tracing::debug!(
            target: "ostro::engine",
            run = %self.run.run_id,
            seq = rep.seq,
            step = %rep.name,
            "step dropped without completion (abandoned)"
        );
        Ok(())
    }
}
