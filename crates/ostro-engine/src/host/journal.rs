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

use ostro_types::journal::{
    CommandArgs, CommandMeta, EventBody, Failure, HASH_VER, Payload, StepCompleted, command_hash,
    decode_result, encode_result,
};
use wasmtime::component::Resource;

use crate::env::GuestEnv;
use crate::host::bindings::StepRep;
use crate::host::bindings::ostro::runtime::journal::{CompletionAck, Host, HostStep, StepStart};
use crate::host::bindings::ostro::runtime::types::{self as wit_types, StepOptions};
use crate::host::effects::convert;
use crate::host::state::{HostState, StepFuel, trap, violation};
use crate::replay::StepBegin;

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
            StepBegin::Live { seq } => {
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
                })?;
                Ok((StepStart::Execute(rep), set))
            }
        }
    }

    /// `[method]step.complete` with the store's remaining fuel.
    ///
    /// Returns the ack and, when the step ran under a fuel budget, the outer fuel to
    /// restore (`outer remaining - fuel used`). `retry-now` is never returned in Phase 3:
    /// in-activation retry policy is WF-02 (Phase 4).
    pub(crate) fn complete_step_impl(
        &mut self,
        step: Resource<StepRep>,
        outcome: Result<wit_types::Payload, wit_types::Failure>,
        fuel: Option<u64>,
    ) -> wasmtime::Result<(CompletionAck, Option<u64>)> {
        let rep = GuestEnv::table(self).get(&step)?.clone();
        if self.completed_steps.contains(&rep.seq) {
            return Err(violation("step completed twice"));
        }
        let (fuel_used, restore) = match (self.step_fuel.take_if(|s| s.seq == rep.seq), fuel) {
            (Some(sf), Some(now)) => {
                let used = sf.start.saturating_sub(now);
                (used, sf.outer.map(|o| o.saturating_sub(used)))
            }
            _ => (0, None),
        };
        let hash = command_hash(HASH_VER, &CommandArgs::Step { name: &rep.name })
            .map_err(|e| violation(format!("step hash: {e:?}")))?;
        let recorded = encode_result(&convert::step_outcome_from_wit(outcome));
        self.replay
            .complete_step(
                rep.seq,
                EventBody::StepCompleted(StepCompleted {
                    meta: CommandMeta {
                        hash_ver: HASH_VER,
                        hash,
                    },
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
        self.complete_step_impl(step, outcome, None).map(|(a, _)| a)
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
