---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 12
subsystem: engine
tags: [wit, behaviour-tests, cancel, continue-as-new, await-token, set-status, replay, fake-sink]
requires:
  - "03-07: wit-surface guest (12 workflows), HostState/HostInit, build_linker, token::{parse, verify}"
  - "03-08: Runtime/RuntimeOptions, testkit drive_to_completion/replay_journal/FakeSink/FakeClock"
provides:
  - "crates/ostro-engine/tests/wit_surface.rs: 17 WIT-01..06 + ENG-04 behaviour tests (03-11 regression gate)"
affects: [03-11, phase-04, phase-05]
tech-stack:
  added: []
  patterns:
    - "Frozen FakeClock (step 0) so outputs/journals are independent of activation count"
    - "Token-only caller: parse(token) -> RunRef + op, commit TokenCompleted without any other run knowledge"
    - "Direct HostState replay (production linker) to read the host-published status"
key-files:
  created:
    - crates/ostro-engine/tests/wit_surface.rs
  modified: []
key-decisions:
  - "continue-as-new-suggested threshold test uses continue_as_new_events = 1 (suggested) vs 2 / 10,000 (not): `can` checks the flag with a 1-event journal, so the plan's threshold 3 could never flip"
  - "is-replaying asserted as (activations > 1) in the surface dump: on the evict-and-replay path the dump is produced by a replaying activation; it reads false once a run completes live"
requirements-completed: [WIT-02, WIT-03, WIT-04, WIT-05]
duration: 20min
completed: 2026-11-18
---

# Phase 3 Plan 12: WIT 0.1 Behaviour Tests Summary

Phase 3 success criterion 5 is now covered by behaviour tests, not only by compiling the WIT. Every `ostro:runtime@0.1.0` import and both exports run through the activation driver on the FakeSink and replay with zero appends. The suite covers:
- graceful cancel with a journaled cleanup step;
- continue-as-new and its suggestion flag;
- await tokens completed by a caller that holds only the token;
- set-status published from the journal;
- failure kinds, the run-info deadline and run-timeout-ms;
- exact effect bytes.

No host bugs were found.

## What was built

`crates/ostro-engine/tests/wit_surface.rs` has 17 tests. They use a shared Runtime with a frozen FakeClock and token key `[9;32]`. Runs use `RunRef{partition: 7, run_id: "run-wit-1"}` with deadline = accepted_at + 30 s.

**Task 1 (WIT-01/02/03)**
- `wit_surface_every_import`:
  - **Context dump:** info, now (= accepted_at), random-u64/bytes, a uuid-v7 with version nibble 7, is-replaying, `patched = true`, and the step result.
  - **Op ids:** each of the 8 op ids (http/sql/object/sleep/until/signal/child/token) equals the seq of its scheduling event.
  - **Event kinds:** PatchMarker, StatusPublished, StepCompleted, SignalWait, Child, Token, OpCancelled and RunCompleted each appear once, with 2 WaitResolved.
  - **Completions:** scripted http/sql/object/timer completions reach the guest.
  - **Status:** `latest_status` is `{"surface":1}`.
  - **Replay:** identical, 0 appends.
- `wit_surface_describe_and_exports`: `describe` lists all 12 workflows in order. Only `surface` has `run_timeout_ms = Some(30000)`.
- `cancel_cleanup_runs_after_cancel`: the journal is exactly `RunStarted, TimerStarted, CancelRequested, WaitResolved{[u64::MAX]}, StepCompleted("cleanup" -> "cleaned"), RunFailed(Cancelled)`. Outcome `Failed{Cancelled}`; replay is identical with 0 appends.
- `cancel_without_request_completes`: the 1 h timer alone resolves the wait, giving `"slept"`. No CancelRequested is recorded.
- `terminate_is_not_guest_visible`: no non-comment line of the WIT mentions `terminate`, and `cancellation: func` is present.
- `continue_as_new_outcome`:
  - counter 0 gives `ContinuedAsNew{input: 1u32 LE, workflow: None, version: None}`, and the journal ends with RunContinuedAsNew. Replay appends 0.
  - counter 3 gives Completed.
- `continue_as_new_suggested_threshold`: with threshold 1 the flag is true. With 2 or the default 10,000 it is false. Replay reproduces the value.

**Task 2 (WIT-04/05/06 + ENG-04)**
- `await_token_roundtrip`:
  - The token has the `ot1_` prefix and parses to (7, "run-wit-1", op = the AwaitTokenCreated seq). It verifies with `[9;32]` and not with `[8;32]`.
  - A manual leader loop completes the run using **only the token**: `parse` gives the RunRef and op, and the caller commits `TokenCompleted`. The run completes with "hello".
- `await_token_replay_without_key`:
  - Replay on a Runtime with key `[1;32]` gives the identical outcome and token string, with 0 appends.
  - A fresh run on that key mints a different MAC over the same (partition, run, op).
- `set_status_journaled_once`: statuses are journaled as `["a", "a!"]`, `latest_status` is "a!", and replay appends 0.
- `set_status_changed_text_no_divergence`:
  - Edited the journal's StatusPublished JSON to "x"/"y". Replay still completes, with 0 appends.
  - A direct `HostState` replay through the production linker reports `latest_status == "y"` and has 0 pending events.
- `non_retryable_failure`: `Failed{NonRetryable{kind:"bad-input"}}`, and the journal ends with RunFailed.
- `step_failure_recorded`: StepCompleted decodes to `Err(Retryable{kind:"flaky"})` and the workflow fails Retryable. Replay returns the recorded failure.
- `run_info_deadline`:
  - The output contains run-id, workflow, version 1, attempt 1, module-hash = `Prepared.module_hash_hex`, and `deadline=1700000030.0`.
  - Without a deadline the output shows `deadline=none`.
- `effect_bytes_exact`:
  - The HTTP response has a 4 KiB non-UTF-8 body and the headers `set-cookie=b=2, x-z, set-cookie=a=1`, in that order.
  - The guest's dump line equals the line computed from the decoded `EffectCompleted` bytes.
  - Replay is identical.
- `effect_unsupported_default`: with no rules, all three effects return `failed non-retryable:unsupported` and the child completion decodes the same way. The run still completes.
- `nested_step_is_protocol_violation`: `AttemptFailed(ProtocolViolation("..inside an open step"))` with `retryable() == false`, and no TimerStarted is journaled.

## Verification

- `cargo test -p ostro-engine --test wit_surface`: all 17 pass.
- `cargo clippy -p ostro-engine --test wit_surface --all-features -- -D warnings` is clean, and `cargo fmt -p ostro-engine --check` reports nothing for this file.
- `bash scripts/ci-local.sh --no-musl`: every gate passes (fmt, clippy, build, test 40 s, failpoints, deny, vendored, deny fixtures, workflow validation).

## Deviations from Plan

### Test-expectation adjustments (not host bugs)

**1. continue-as-new-suggested threshold**
- **Found during:** Task 1
- **Issue:** `can` reads `continue-as-new-suggested` before it journals anything, so the journal has 1 event (RunStarted). `ReplayState::continue_as_new_suggested` returns `last_cmd_seq + 1 >= threshold`, so with the plan's threshold of 3 the flag can never be true for this guest. The host behaviour is correct (1 event < 3).
- **Fix:** Threshold 1 gives true; thresholds 2 and 10,000 give false. Each journal replays to the same value.
- **Commit:** addac00

**2. is-replaying in the surface dump**
- **Found during:** Task 1
- **Issue:** On the evict-and-replay path, the activation that completes the run replays every recorded command, so its dump shows `is-replaying true`. No live activation of `surface` produces output. That would need either a guest change (outside this plan's files) or a completion that happens in the live activation.
- **Fix:** The test asserts `is-replaying == (activations > 1)`. It stays correct if 03-11 lets a drive complete in one live activation.
- **Commit:** addac00

**3. Partition-qualified run id**
- **Issue:** `run-info.run-id` is the plain run id ("run-wit-1"). The partition qualification is carried by `RunRef` and the await token.
- **Fix:** The test asserts the plain id and checks the partition through the token. No host change was made.

## Known Stubs

None in this plan. These tests run on the evict-and-replay path, because hot wait (03-11) is not wired yet; 03-11 re-runs them as a regression gate.

## Requirements

- **WIT-02:** complete. A graceful cancel is observed through the cancellation op and followed by a journaled cleanup step. Terminate is absent from the guest WIT; hard terminate is a host/API operation.
- **WIT-03:** complete. The first-class continue-as-new outcome and the suggestion threshold are proven. Starting the new run from the outcome is the partition's job (Phase 4 wiring).
- **WIT-04:** complete. The token embeds partition, run and op, and a token-only caller completes it. The client API for completion is tracked separately as API-07.
- **WIT-05:** complete at the contract and engine level. Status is journaled, and the published status comes from the journal. Reading it over the HTTP API is tracked as API-04 (Phase 4).
- **WIT-01, WIT-06, ENG-04:** already complete; these tests add behavioural coverage.

## Commits

- addac00 test(03-12): WIT-01/02/03 behaviour - every import, cancel cleanup, continue-as-new
- cd6cfa6 test(03-12): WIT-04/05/06 + exact effect bytes - tokens, status, failures, deadlines

## Self-Check: PASSED

`crates/ostro-engine/tests/wit_surface.rs` exists; commits addac00 and cd6cfa6 are in git log.
