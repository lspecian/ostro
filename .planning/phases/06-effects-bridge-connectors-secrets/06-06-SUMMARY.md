---
phase: 06-effects-bridge-connectors-secrets
plan: 06
subsystem: engine
tags: [effects, engine-seam, redaction, replay, redrive, fx-01]
requires:
  - "06-02: ostro_types::live::{LiveEffect, EffectRetry, EffectKind} (redacting Debug)"
  - "05-03: completion index (first completion wins, OpCancelled completes its op)"
provides:
  - "ostro_engine::EffectDriver (trait) + ostro_engine::LiveEffect re-export"
  - "ActivationSink::effect_driver() default None (P-1)"
  - "HostState: start after commit in flush, replay re-drive in schedule_effect, cancel after OpCancelled commit"
  - "Redacted journal copy for http-request.redact header names (\"[redacted]\")"
  - "testkit FakeSink::with_effect_driver(Arc<dyn EffectDriver>)"
affects: [06-07, 06-10, 06-14, 06-15, 06-17, Phase 7 workers]
tech-stack:
  added: []
  patterns:
    - "Collect the batch's EffectScheduled / OpCancelled ops before append; act on them only after append returns Ok"
    - "Live request and journaled request are separate values; the hash is computed over the live one (header values are not hash inputs)"
key-files:
  created:
    - crates/ostro-engine/src/effects_driver.rs
    - crates/ostro-engine/tests/effects_driver.rs
  modified:
    - crates/ostro-engine/src/lib.rs
    - crates/ostro-engine/src/sink.rs
    - crates/ostro-engine/src/testkit.rs
    - crates/ostro-engine/src/host/effects.rs
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-engine/tests/host_adapter.rs
key-decisions:
  - "Redaction applies with or without a driver (security property); pending_effects is filled only when a driver is present, so the no-driver path does no extra work and journals identically to the driver path"
  - "Replay re-drive is called at the re-issued call (schedule_effect), not at flush: the EffectScheduled is already durable"
  - "An effect scheduled and cancelled in the same committed batch is neither started nor cancelled"
  - "Replayed OpCancelled events never call driver.cancel (only live cancels committed by this activation do)"
  - "scheduled_ts_nanos is taken from the live command's event ts at schedule time (identical to the committed event's ts) and from the journaled event on replay"
metrics:
  duration: "~60 min"
  completed: 2026-11-20
---

# Phase 6 Plan 06: Engine Seam Summary

**This plan adds the engine half of the effect lifecycle. `EffectDriver` is attached through `ActivationSink::effect_driver()` (default `None`). The host keeps the guest's full request as a `LiveEffect` (real header values, `retry` mapped to `EffectRetry`) and journals a copy whose `redact`-named header values are `"[redacted]"`; the command hash is unchanged. The driver is started only after the append carrying `EffectScheduled` returns `Ok` (FX-01). A replaying activation re-drives re-issued effects that have no completion in the loaded journal with `redrive = true` (FX-03, F1). `cancel-op` on an effect calls `driver.cancel` after the `OpCancelled` commit. Proven with 12 fake-driver tests.**

## Final API (for 06-07 / 06-14 / Phase 7)

```rust
// crates/ostro-engine/src/effects_driver.rs (re-exported as ostro_engine::EffectDriver)
pub trait EffectDriver: Send + Sync {
    fn start(&self, run: &RunRef, attempt: u32, effect: LiveEffect);
    fn cancel(&self, _run: &RunRef, _op: u64) {}
}
// crates/ostro-engine/src/sink.rs, ActivationSink (default method, P-1)
fn effect_driver(&self) -> Option<std::sync::Arc<dyn EffectDriver>> { None }
// testkit (feature "testkit")
impl FakeSink { pub fn with_effect_driver(self, driver: Arc<dyn EffectDriver>) -> Self }
// ostro_engine::LiveEffect = ostro_types::live::LiveEffect
```

## Call sites (W9)

- **Append + start:** `HostState::flush` in `crates/ostro-engine/src/host/state.rs`. `sink.append(&self.run, self.attempt, from_seq, pending)` is at **state.rs:341**. `driver.start` (live, `redrive = false`) is at state.rs:351 and `driver.cancel` at state.rs:354, both after `r?`. The append site did not move in Phase 5.
- **Replay re-drive:** `HostState::schedule_effect` in `crates/ostro-engine/src/host/effects.rs:94`. `driver.start(.., redrive = true)` is at effects.rs:134, called when the command is `Replayed` and `replay.completion_for(op)` is `None`. The completion index is built from the whole loaded journal, so an `EffectCompleted` or `OpCancelled` anywhere in it suppresses the re-drive.
- **Driver lookup:** `HostState::new` calls `init.sink.effect_driver()` once per activation and caches it in `HostState.driver`.
- **Redaction:** `convert::redact_http_request` (effects.rs). The `redact` match is ASCII case-insensitive. Templates such as `{{secret:..}}` are journaled verbatim unless their header is named in `redact`.

## Tasks

| Task | Name | Commit |
|------|------|--------|
| 1 | EffectDriver trait, sink default method, LiveEffect capture with redacted journal copy | eb67d8a |
| 2 | Start after commit (live) and re-drive on replay | eb67d8a |

## Tests (`crates/ostro-engine/tests/effects_driver.rs`, 12)

**Direct `HostState` tests:**
- `redacted_journal_copy_and_full_live_request`: the journal holds `"[redacted]"` and the template verbatim; the driver gets the real values and the retry policy; nothing starts before flush; the journaled hash equals the hash of both the live and the redacted request.
- `command_hash_ignores_redacted_values`
- `sql_and_object_requests_are_kept_as_is`
- `failed_append_starts_nothing`: both `Fenced` and `Conflict`; a later flush does not start the dropped requests.
- `replay_redrives_only_uncompleted_effects`: two activations; the completed op is not started; re-drives carry the real headers, the retry policy and the journaled ts; the replay flush appends 0 events.
- `replay_never_redrives_cancelled_effects`
- `cancel_op_cancels_after_commit`: also covers scheduling and cancelling in the same batch, which never starts.
- `no_driver_identical_journal`: the journal is byte-equal with and without a driver.

**Activation tests** (`wit-surface` `surface`, production activation driver):
- `start_only_after_append_returns`: a gated sink blocks the append; the driver sees nothing until it is released, then exactly one start per effect; X-Token is `[redacted]` in the journal and the real template in the driver.
- `fenced_activation_starts_nothing`
- `evict_then_replay_redrives_open_effects`
- `hot_wait_resolves_from_inputs_with_driver`

## Verification

- `cargo test -p ostro-engine --test effects_driver --locked`: test result: ok. 12 passed.
- `cargo test -p ostro-engine --locked`: every binary ok. Includes replay_corpus (5 passed; the corpus files are untouched: 30 entries / 377 events), host_adapter 7, hot_wait 7, wit_surface 17 and lib 84.
- `cargo test -p ostro-server --test executor_phase5 --locked`: 12 passed (also drives wit-surface).
- `cargo build --workspace --all-targets --locked`: 0 warnings, on the merged main (85754a1, after 06-05 landed).
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean.
- runtime.wit, the journal codec, EVENT_CODEC_VER and the hash inputs are unchanged. No Cargo files were touched.
- `bash scripts/ci-local.sh` was not run in full, because parallel executors were building; the workspace build, workspace clippy and the engine tests above cover the files this plan touched.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `tests/host_adapter.rs` asserted the old verbatim journaling of a redacted header**
- **Found during:** Task 1
- **Issue:** `host_surface_*` asserted `r.headers[0].value == "{{secret:tok}}"` for wit-surface's `X-Token`, which the guest names in `redact`. This plan deliberately changes that value to `"[redacted]"` (W3/F2), so the old assertion contradicts the plan.
- **Fix:** the assertion now expects `"[redacted]"`, and that the un-redacted `Accept` value is still verbatim. The file is outside files_modified; this one assertion is the only change to it.
- **Files modified:** crates/ostro-engine/tests/host_adapter.rs
- **Commit:** eb67d8a

**2. [Process] Tasks 1 and 2 landed in one commit**
- The two tasks share the same four source files (effects.rs, state.rs, testkit, the test file), and each half alone does not exercise the seam end to end. They were implemented and verified together and committed once (eb67d8a).

**3. [Test design] Hot-wait test does not fire timers live**
- With `Script::live` + `fire_timers(true)`, the fake sink commits timer fires concurrently with the activation's own appends. The final flush then hits `AppendConflict`, a FakeSink race unrelated to the driver. The test therefore completes the effects live, asserts that the effect wait resolved hot (one committed `WaitResolved` naming the 3 effect ops) and expects `HotWaitElapsed` on the second wait.

## Notes for 06-14

- Override `effect_driver()` in `PartitionSink`. Because `HostState::new` reads it once, a driver swap only takes effect for new activations.
- `start` is called on the activation task while it holds `&mut HostState`. It must not block (spawn) and must be idempotent per `(run, op)`: a live start in activation N and a re-drive in N+1 can both arrive while the first task still runs.
- Without a driver (children, or a node without bridge services) the "unsupported" completion path in the server sink is untouched.

## Self-Check: PASSED

- FOUND: crates/ostro-engine/src/effects_driver.rs
- FOUND: crates/ostro-engine/tests/effects_driver.rs
- FOUND: eb67d8a in main
