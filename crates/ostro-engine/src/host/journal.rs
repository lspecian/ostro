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

use ostro_types::journal::{
    AttemptBackoff, CommandArgs, CommandMeta, EventBody, Failure, HASH_VER, Payload,
    StepAttemptFailed, StepCompleted, command_hash, decode_result, encode_result,
};
use wasmtime::component::Resource;

use crate::env::GuestEnv;
use crate::error::{Evict, EvictReason};
use crate::host::bindings::StepRep;
use crate::host::bindings::ostro::runtime::journal::{CompletionAck, Host, HostStep, StepStart};
use crate::host::bindings::ostro::runtime::types::{self as wit_types, StepOptions};
use crate::host::effects::{convert, flush_trap};
use crate::host::state::{HostState, StepFuel, trap, violation};
use crate::replay::StepBegin;
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
                let rep = GuestEnv::table(self).push(StepRep {
                    seq,
                    name,
                    fuel_before: fuel,
                    attempt,
                    retry: opts.retry.map(retry_policy_from_wit),
                    timeout_ms: opts.timeout_ms,
                    fuel_budget: opts.fuel,
                })?;
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
        Ok((CompletionAck::Recorded, restore))
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
