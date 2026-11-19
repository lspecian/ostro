---
phase: 05-workflow-semantics-core-hardening
plan: 10
subsystem: guests
tags: [guests, race-fuzz, signals, d21, timeouts, await-tokens, continue-as-new, crash-saga, perf, order-saga, ci]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-03 first-completion-wins / cancel-op / WaitTimedOut / SignalMatched engine semantics; 05-06 SDK await tokens, continue-as-new, D11 + shielded"
provides:
  - "guests race-fuzz, signals, timeouts, tokens, can-chain, crash-saga, trivial-steps (workflow name == crate name, v1)"
  - "crates/ostro-engine/tests/phase5_guests.rs: hand-committed D21 signal inputs, race-fuzz 20-seed live/replay + first-completion consistency check, per-guest behaviour"
  - "crash-saga output documented as a pure function of input + accepted go/extra signals (05-19 checker)"
  - "examples/order-saga standalone project (success criterion 1) + README quickstart"
  - "ci-local step \"order-saga example\" + ci.yml linux-gnu step (fmt, wasm32-wasip2 release build, clippy -D warnings)"
affects: [05-11, 05-12, 05-14, 05-15, 05-16, 05-17, 05-19]

tech-stack:
  added: []
  patterns:
    - "Engine guest suites feed signals as SignalPayload + SignalMatched{op, payload_seq} (never SignalReceived); buffered signals = SignalPayload in an earlier batch, SignalMatched once the wait exists"
    - "Heterogeneous select!/join! in a guest: an enum of typed ops and a small match per type combination (the macros need concrete Op<T>)"
    - "Guest-side shape generator (splitmix64 from the input seed, not ctx.random) so the guest's behaviour is a function of its input"

key-files:
  created:
    - guests/race-fuzz/{Cargo.toml,src/lib.rs}
    - guests/signals/{Cargo.toml,src/lib.rs}
    - guests/timeouts/{Cargo.toml,src/lib.rs}
    - guests/tokens/{Cargo.toml,src/lib.rs}
    - guests/can-chain/{Cargo.toml,src/lib.rs}
    - guests/crash-saga/{Cargo.toml,src/lib.rs}
    - guests/trivial-steps/{Cargo.toml,src/lib.rs}
    - crates/ostro-engine/tests/phase5_guests.rs
    - examples/order-saga/{Cargo.toml,.gitignore,README.md,src/lib.rs}
  modified:
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - .planning/ROADMAP.md

key-decisions:
  - "sdk_cancellation.rs unchanged: select_loser was never #[ignore]d (05-06) and passes"
  - "phase5_guests.rs uses its own evict/re-activate driver instead of testkit::drive_to_completion, because the testkit scripts signals as legacy SignalReceived (testkit.rs is outside this plan)"
  - "crash-saga retry failures use a per-instance counter with max_attempts 10 and 1..10 ms in-activation backoff; crashes mid-retry restart the count but cannot change the output (only exhaustion, documented as Failed)"
  - "order-saga: fail_at=\"charge_payment\" fails retryably (exercises the 3-attempt policy), other step names fail non-retryably; cancellation wins -> ctx.cancel(&approval) so a late signal is not consumed"
  - "order-saga CI step also runs cargo fmt --check, and builds into target/order-saga (like target/guests) so it never mixes with the root target dir"
  - "REQUIREMENTS.md unchanged: WF-03/05/08/09 still need server-side proof (05-08/05-09 apply, 05-15 e2e)"

requirements-completed: []

duration: ~75min
completed: 2026-11-19
---

# Phase 5 Plan 10: Test Guests and order-saga Summary

**Seven SDK-built test guests cover every Phase 5 semantic:**
- `race-fuzz`: race ordering with nested `select!`/`join!` and cancelled losers;
- `signals`: FIFO, timeout, buffered-before-wait and long wait;
- `timeouts`: run-timeout workloads;
- `tokens`: await tokens published through the run status;
- `can-chain`: continue-as-new chain;
- `crash-saga`: the crash-campaign workload, whose output is documented as a pure function;
- `trivial-steps`: the perf guest.

**They are proven on the engine with D21 signal inputs.** `examples/order-saga` is a standalone project with 5 steps, a retry policy, a timer, an approval signal raced with cancellation, and shielded reverse compensations. ci-local and CI build and lint it warning-free.

## Performance

- **Duration:** about 75 min, including the final gate.
- **Completed:** 2026-11-19.
- **Tasks:** 2/2.
- **Files:** 19 created, 2 modified (plus ROADMAP.md).

## Accomplishments

- **race-fuzz** `{seed, sleeps_ms, signals, nest}`.
  - Ops are created up front. Signal waits get seeded timeouts (50..500 ms), so every op completes.
  - A seeded generator consumes the ops in rounds: `wait`, `select` (the loser gets `ctx.cancel`, then a wait), `join` and `select_join` (the select loser is joined with a third op, not cancelled).
  - The output lists the rounds, the observed order and `{label, op, outcome}`.
- **signals** modes:
  - `fifo`: 3 waits, returns an array.
  - `timeout`: 200 ms default, returns `"timeout"` or the payload.
  - `before`: sleeps (default 500 ms), then waits; a signal sent during the sleep is buffered.
  - `long_wait`: no timeout.
- **timeouts** modes: `sleep_forever` (365-day sleeps in a loop), `sleep_ms` and `busy_steps`.
- **tokens** `{timeout_ms?}`: calls `set_status({"token": t})`, then returns the payload, `"timeout"`, or ends Cancelled (D11).
- **can-chain** `{n, max}`: continues as new with `{n+1, max}` until `n >= max`, then completes with `n`.
- **crash-saga**: the module header documents the output function, covering completed, compensated, cancelled-with-shielded-undo and retry-exhaustion. Cleanup always runs inside `ctx.shielded`.
- **trivial-steps** `{n}`: steps `s0..s{n-1}` returning their index; output `n`.
- **phase5_guests.rs** (13 tests). Its driver commits inputs in an explicit order:
  - timers: `TimerFired` stamped no earlier than `fire_at`;
  - signals: `SignalPayload` + `SignalMatched`, or a buffered `SignalPayload` followed by a later `SignalMatched`;
  - timeouts: `WaitTimedOut`;
  - tokens: `TokenCompleted`;
  - cancels: `CancelRequested`.

  It feeds no `SignalReceived`, and every journal replays with 0 appends.
- **order-saga.** Behaviour on the engine was checked ad hoc against the built `order_saga.wasm` with the same driver. That test was temporary and not committed:

  | Scenario | Steps | Result |
  |----------|-------|--------|
  | approve | 5 forward steps | completed, `ship-o-1` |
  | reject | `[refund_payment, release_inventory]` | compensated, `rejected` |
  | approval timeout | `[refund_payment, release_inventory]` | compensated, `approval_timeout` |
  | cancel | refund, then release | Failed `Cancelled` |
  | `fail_at=charge_payment` | 2 StepAttemptFailed + Err, then `[release_inventory]` | compensated |
  | `fail_at=create_shipment` | refund, then release | compensated |
  | `fail_at=reserve_inventory` | no compensations | compensated |

  All replayed with 0 appends.
- **CI.** ci-local has a new `check_order_saga` step after the guests build: fmt `--check`, wasm32-wasip2 release build, clippy `-D warnings`, `--target-dir target/order-saga`. ci.yml mirrors it in the linux-x86_64-gnu leg, right after the SDK and guests build step. `FAILPOINT_CRATES` and the failpoint step are untouched.

## Task Commits

1. **Task 1: test guests and engine-level suite**: `ce0cc53` (test). The guests were written first and the suite asserts their specified behaviour. All 13 tests passed on the first run; RED was skipped because the guests and the suite were new together. One coverage assertion failed until the race inputs were widened (see Deviations).
2. **Task 2: order-saga, ci-local and ci.yml**: `1a6775c` (feat).

## Verification

- `cargo test -p ostro-engine --test phase5_guests --test sdk_cancellation --locked`: 13 + 8 passed.
  - **race-fuzz, 20 seeds:**
    - each run completes;
    - every started op is reported exactly once;
    - each outcome equals what the op's **first completion in journal order** implies (TimerFired → `ok`, SignalMatched → `ok:<payload>`, WaitTimedOut → `timeout`, guest OpCancelled → `cancelled`);
    - replay appends 0 events;
    - re-driving with the same arrival order gives a byte-identical journal;
    - across the seeds, every round shape and every outcome class is covered (asserted).
  - **signals:** `fifo` gives `["a","b","c"]` from 1 immediate and 2 buffered matches (3 SignalPayload, 3 SignalMatched, 0 SignalReceived). `before` returns a payload buffered during the sleep (the SignalPayload precedes the SignalWaitStarted). `timeout` gives `"timeout"` with `timeout_ms = Some(200)`. Signal then stale WaitTimedOut gives `42` (first wins). `long_wait` has `timeout_ms = None`.
  - **tokens:** `TokenCompleted` returns the payload, and the status is `{"token":"<token>"}`. `WaitTimedOut` gives `"timeout"`. A cancel gives Failed `Cancelled`.
  - **can-chain:** `{0,3}` gives 3 `ContinuedAsNew` (one `RunContinuedAsNew` each), then completes with `3`.
  - **timeouts:** `sleep_ms` gives `"slept"`. `busy_steps` n=25 gives 25 steps in 1 activation. `sleep_forever` survives 5 fires, then a cancel gives Cancelled with 6 timers.
  - **trivial-steps** n=10: 1 activation, 10 StepCompleted `s0..s9`.
  - **crash-saga:**
    - `0120` gives the documented completed JSON with 3 StepAttemptFailed (go payload, or go = null on timeout).
    - `01x` gives compensated `failed_at 2`, `undone [1,0]`, with steps `step0..2, undo1, undo0`.
    - `steps 2` gives `bad_input`.
    - A cancel during the sleep gives Cancelled with `undo2, undo1, undo0`.
- `cargo clippy -p ostro-engine --all-targets --locked -- -D warnings` is clean, and `cargo fmt --all --check` is clean.
- The new guests are clippy-clean (`-D warnings`, wasm32-wasip2) and rustfmt-clean.
- order-saga: the plan's verify command passed (release build, clippy `-D warnings`, grep in both CI files).

## Final gate

`CARGO_TARGET_DIR=target/wt-05-10 bash scripts/ci-local.sh --no-musl` ran in a detached scratch worktree at **`1a6775c`** (both task commits on top of 05-24's `aed4ff6`). Parallel executors' gates were running on the machine at the same time (load average 20–27).

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  1s
cargo clippy -D warnings             PASS  56s
cargo build (warning-free)           PASS  134s
sdk workspace fmt + clippy           PASS  24s
sdk workspace test + wasm build      PASS  24s
guests build (wasm32-wasip2)         PASS  3s
order-saga example                   PASS  22s   (new step)
cargo test                           FAIL  128s  (load flakes, see below)
failpoint suites                     FAIL  34s   (ostro-wal first_write flake, see below)
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  1s
check-deny-fixtures.sh               PASS  0s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  1s
zig/musl legs                        SKIP  (--no-musl)
```

Both failures are on the known-flake list, and both are in code this plan did not touch.

- **`cargo test`: `ostro --test dev_startup`.** `warm_restart_under_1s` and `default_data_dir_is_dot_ostro_and_persists` failed.
  - Rerun alone at load about 21, `warm_restart_under_1s` failed 4 times in a row, measuring 1.09 s against the 1 s budget. The other dev_startup tests passed.
  - At load about 6 the whole file passed 4/4 (1 measurement test ignored).
- **Failpoint step: ostro-wal `group_commit::queue_creation_tests::the_first_write_to_every_new_partition_commits`.** It failed in the gate, and the step stopped there. Rerun alone, both `--features failpoints --lib` (73/73) and the plain `first_write` filter (2/2) passed.
- **Failpoint suites rerun in full**, same worktree and target, clippy plus tests for each crate in `FAILPOINT_CRATES`: ostro-wal, ostro-blob, ostro-log, ostro-partition and ostro-crashtest all exited 0. Clippy `-D warnings` was clean for all of them.
- **`cargo test --workspace --locked --no-fail-fast`**, same worktree: **986 passed, 3 failed, 10 ignored** over 128 result blocks.
  - 1 failure is the same `warm_restart_under_1s` timing failure. It passed later at low load, as above.
  - 2 failures are in vendored `ostro-raft-driver`: `isolated_leader_steps_down_and_its_proposal_is_lost` and `lagging_node_catches_up_by_snapshot`, each `unwrap()` on `Err(OutcomeUnknown)` under load. This is not on the known-flake list. This plan touched no raft code, and rerun alone the crate passed 26/26.
- No new root dependencies; the root Cargo.lock is unchanged. No chrono, bincode or wasmtime `cache`. No ordinals were allocated. `FAILPOINT_CRATES` is unchanged.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Test coverage] Race-fuzz inputs widened**
- **Found during:** Task 1.
- **Issue:** With 1–4 sleeps, no seed in 1..=20 produced a `select_join` round, so the shape-coverage assertion failed.
- **Fix:** The test generates 2–5 sleeps (plus 0–3 signals).
- **Commit:** `ce0cc53`.

### Not changed (listed in files_modified)

- `crates/ostro-engine/tests/sdk_cancellation.rs`: `select_loser` was never ignored and passes. Nothing to change.

### Process notes

- **Shared scratch worktree.** The session scratchpad is shared with the parallel executors. My first scratch worktree (`scratchpad/wt`, created by me with `git worktree add`) was then also used by the 05-23 executor: its uncommitted engine edits and `step_retry_clock.rs` appeared there.
  - I moved my work to `scratchpad/wt-05-10-exec` and removed only my own untracked files from `wt`. I did not touch 05-23's edits or remove that worktree, because 05-23 may still be using it. It needs removing (`git worktree remove`) once 05-23 is done.
  - Nothing of mine was committed from it.
- **Failed first commit attempt.** It used an unquoted path variable under zsh, which does not word-split. `git add` failed, so nothing was committed and the shared index was not touched. Both commits then went through a bash helper with a private `GIT_INDEX_FILE` seeded by `read-tree HEAD`, followed by `git reset -q -- <paths>` and a `git show --stat HEAD` check. Each commit contains only this plan's files.

## Issues Encountered

None beyond the above.

## STATE.md notes (not applied; for the orchestrator)

- 05-10 complete. Seven Phase 5 test guests plus `phase5_guests.rs` (13 tests); `examples/order-saga` is wired into ci-local and ci.yml.
- Decision: engine guest suites feed D21 `SignalPayload` + `SignalMatched`. `testkit::Rule::Signal` still produces legacy `SignalReceived`; whoever owns testkit.rs may migrate it.
- Housekeeping: remove the scratch worktree `scratchpad/wt` (shared with 05-23) once 05-23 is done.

## Hand-off

- **05-11 (server seam, resume re-arm):**
  - Use `signals` `long_wait` / `before` and `tokens` for re-arm tests. Signal and token waits always evict, so every resume goes through re-arm.
  - `tokens` publishes the token via `set_status`, so a test can read it from `GET /v1/runs/{id}` (latest status) without journal access.
  - `crash-saga` has every wait kind (timer, signal with timeout, a `join!` over 2 timers and a signal), all in one run.
- **05-12 (replay corpus):**
  - Good seed sources, all recorded with the D11 SDK:
    - `race-fuzz` (any seed; the shape depends only on the input, so it is reproducible);
    - `crash-saga` (completed, compensated and cancelled variants);
    - `can-chain` (a RunContinuedAsNew tail);
    - `signals` `fifo` (buffered SignalMatched with `payload_seq` pointing back).
  - Each produces journals with D21 kinds 96/97 and WaitTimedOut 70.
  - The `phase5_guests.rs` driver (`drive`, `commit`, `In`) shows how to build such journals without a server.
- **05-15:** `signals` modes map to the e2e cases: `fifo`, `timeout`, `before` (signal during sleep) and `long_wait` (resume after restart). `timeouts` `sleep_forever` / `busy_steps` cover the run timeout. `can-chain` covers successor runs.
- **05-16:** `trivial-steps` `{n}` completes in one activation with n StepCompleted events (profiles A/B).
- **05-17:** order-saga builds to `examples/order-saga/target/wasm32-wasip2/release/order_saga.wasm`.
  - The README quickstart uses `ostro deploy … --activate`, `run`, `signal <run> approve --payload true` and `result <run> --wait`. Adjust the README if the 05-14 verbs differ.
  - Expected outputs: completed `{"status":"completed","order_id":"o-1","shipment_id":"ship-o-1"}`; rejected `{"status":"compensated",…,"reason":"rejected","compensations":["refund_payment","release_inventory"]}`.
- **05-19:** the crash-saga output function is documented in the guest header.
  - `go` and `extra` are the payloads of the first signal of each name *matched* to those waits, or `null` on timeout.
  - Retry failures never change the output; only exhaustion does, giving Failed `transient` (needs at least 10 failed attempts on one step).
  - `steps` must be in 3..=8, and `fail_pattern` chars must be `0-3` or `x`.

## Next Phase Readiness

Every Phase 5 semantic has an SDK guest exercising it. The order-saga example exists, builds, and is gated in ci-local and CI.

## Self-Check: PASSED

**PASSED.**

- All created files were FOUND at HEAD: the 7 guest sources, `phase5_guests.rs` and the order-saga files (`lib.rs` is 260 lines, above the 80-line minimum).
- Both commits were FOUND: `ce0cc53` and `1a6775c`.
- `order-saga` appears in both `scripts/ci-local.sh` and `.github/workflows/ci.yml`.
