---
phase: 05-workflow-semantics-core-hardening
plan: 03
subsystem: engine
tags: [replay, cancel-op, wait-timeout, signals, d21, patched, proptest, gauge, await-tokens]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 journal kinds 70/71/96/97, completes_op() for 70/97, WaitTimedOut -> Err(Timeout) arm, temporary SignalMatched bad-journal arm"
provides:
  - "Completion rules frozen with WIT 0.1: first completion in journal order wins; OpCancelled -> Err(Cancelled) (D5); WaitTimedOut -> Err(Timeout) (D4); SignalMatched{op,payload_seq} -> payload of the SignalPayload at payload_seq (D21); SignalPayload alone completes nothing; legacy SignalReceived still completes"
  - "ReplayState op -> completion-seq index (BTreeMap), filled on load, deliver_inputs and live appends; completion_for/completion_seq/completion_outcome"
  - "deliver_inputs accepts SignalPayload (96) and RunTerminated (71) as non-completing inputs (W4)"
  - "hot_waitable: signal and token waits always evict, whatever the timeout (W12), documented"
  - "patched(id) memoised per change-id; recorded PatchMarkers the code does not query are skipped (D6)"
  - "Runtime::live_activations() gauge (WF-04)"
  - "ostro_engine::token::{parse, verify, TokenError, TokenParts, mint} Result API for 05-13"
  - "guests patch-v1/v2/v3 (workflow patch-demo v1) and tests/patching.rs; tests/live_activations.rs; gather_race_prop proptest"
affects: [05-05, 05-06, 05-08, 05-09, 05-10, 05-11, 05-12, 05-13, 05-15, 05-23]

tech-stack:
  added: []
  patterns:
    - "Guest-visible completion outcomes are built in the pure replay core (ReplayState::completion_outcome); the host only converts to WIT"
    - "Per-runtime gauge through a drop guard around Runtime::activate (covers return, panic unwind and dropped futures)"
    - "Proptest case count from OSTRO_PROP_CASES (default 256)"

key-files:
  created:
    - crates/ostro-engine/tests/patching.rs
    - crates/ostro-engine/tests/live_activations.rs
    - guests/patch-v1/Cargo.toml
    - guests/patch-v1/src/lib.rs
    - guests/patch-v2/Cargo.toml
    - guests/patch-v2/src/lib.rs
    - guests/patch-v3/Cargo.toml
    - guests/patch-v3/src/lib.rs
  modified:
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/replay/wait.rs
    - crates/ostro-engine/src/replay/mod.rs
    - crates/ostro-engine/src/replay/token.rs
    - crates/ostro-engine/src/replay/tests.rs
    - crates/ostro-engine/src/host/effects.rs
    - crates/ostro-engine/src/host/context.rs
    - crates/ostro-engine/src/runtime.rs
    - crates/ostro-engine/src/lib.rs
    - .planning/ROADMAP.md

key-decisions:
  - "OpCancelled is a completion target: the completion index takes the first of {input with completes_op, guest OpCancelled{op}} per op; later events for that op are ignored"
  - "SignalMatched.payload_seq must name an earlier SignalPayload event in the same journal (payload_seq < matched seq); anything else is BadJournal, which the host traps as a ProtocolViolation"
  - "Unqueried-marker skip applies before matching any non-Patch command, inside patched(id) for markers of other ids, and at finish(); a marker for a queried id is only ever consumed by its first query"
  - "replay_property no longer includes CommandKind::Patch: since D6 a deleted/moved marker is skipped, not a divergence; dedicated patched_* tests cover markers"
  - "Token API: new Result functions parse_token/verify_token(key, token) re-exported as ostro_engine::token::{parse, verify}; the Phase 3 replay::token::{parse -> Option, verify(token, key) -> bool} stay as thin wrappers because wit_surface.rs and host_adapter.rs (outside this plan) use them"
  - "live_activations counts in Runtime::activate (the only caller of run_activation), so activation.rs is unchanged"
  - "No REQUIREMENTS.md change: WF-03/WF-04/WF-08 are proven at engine level only; 05-10 (race-fuzz), the WF-04 1,000-run test and the server-level patching proof (05-11/05-15) complete them"

patterns-established:
  - "completion_outcome(op) -> Result<Result<OpOutput, Failure>, ReplayError> is the single place that maps completion events to guest results"

requirements-completed: []

duration: ~45min
completed: 2026-11-19
---

# Phase 5 Plan 03: Engine Semantics Summary

**The replay core now completes ops by first-completion-in-journal-order through an O(log n) op index. `cancel-op` yields `Err(cancelled)`, `WaitTimedOut` yields `Err(timeout)`, and D21 `SignalMatched` resolves its payload from the referenced `SignalPayload`. `patched(id)` is memoised with an unqueried-marker skip, proven on patch-v1/v2/v3 guests. The engine also gains a `live_activations` gauge and a public Result-based token API. Snapshots are untouched and nothing was re-blessed.**

## Performance

- **Duration:** about 45 min of implementation, plus the final `ci-local --no-musl` run.
- **Started:** about 2026-11-19T06:50Z.
- **Completed:** 2026-11-19.
- **Tasks:** 3/3.
- **Files:** 8 created, 9 engine files modified, plus ROADMAP.md.

## Accomplishments

- **Completion rules (D4, D5, D21).** These live in `replay/wait.rs` (module doc plus `completion_outcome`). `effects.rs::completion` now only converts to WIT. The temporary 05-01 `SignalMatched` bad-journal arm is gone.
- **Op completion index.** `ReplayState.completions: BTreeMap<OpId, u64>` is filled in `new`, `deliver_inputs` and `append_live`. `completion_for` is a map lookup, and the new `completion_seq` helper exposes the seq.
- **W4.** `deliver_inputs` accepts kinds 96 and 71 as non-completing inputs, and kinds 70 and 97 as completing ones. Guest commands are still rejected.
- **W12.** `hot_waitable_op` has explicit arms that return false for `SignalWaitStarted` and `AwaitTokenCreated`. The behaviour is documented on `hot_waitable` and `park_or_evict` and covered by a unit test (1 ms signal timeout, token with and without a timeout).
- **D6 patched.** The memo map lives in `ReplayState`. `skip_unqueried_markers` runs in `match_next` (for non-Patch kinds), in `patched` (for markers of other ids) and in `finish`.
- **WF-04 gauge.** `Runtime::live_activations()` is backed by a `LiveGuard` drop guard.
- **Token API.** `ostro_engine::token::{parse, verify}` with `TokenError::{BadPrefix, Malformed, BadMac}`. No error text contains the token.

## Task Commits

1. **Task 1: completion semantics, op index and gather/race proptest**: `80ca356` (feat)
2. **Task 2: patched memo, marker skip, patch guests and patching suite**: `5773c11` (feat)
3. **Task 3: live_activations gauge and public token parse/verify**: `e4cd2ea` (feat)

TDD: Task 1's mutation check is described under Verification. Task 2's existing `replay_property` failed (RED) as soon as the skip rule existed. This was expected: a deleted marker is no longer a divergence. The fix is described under Decisions.

## Verification

- `cargo test -p ostro-engine --lib --locked`: 83 tests pass. They include:
  - `cancelled_sleep_waited_all_is_err_cancelled`, `input_before_cancel_wins` and `cancel_before_input_wins_and_later_input_is_ignored`;
  - `wait_timed_out_is_err_timeout_for_signal_and_token`;
  - `signal_matched_completes_with_referenced_payload` (buffered payload 20 events before the wait) and `signal_matched_bad_payload_seq_is_bad_journal` (non-payload, later and past-the-end seqs);
  - `deliver_inputs_accepts_non_completing_kinds` (SignalPayload, then RunTerminated, then SignalMatched; replay equals live) and `legacy_signal_received_and_token_completed_still_complete`;
  - `completion_index_is_a_lookup_not_a_scan` (50,001-event journal; 100k lookups checked against a scan and well under the 5 s bound);
  - `gather_race_prop`, `patched_is_memoised_per_change_id`, `unqueried_markers_are_skipped`, `patched_v1_v2_v3_lifecycle`, `signal_and_token_waits_are_never_hot_waitable` and `token_result_api`.
- **gather_race_prop** generates:
  - 1–6 ops (sleep, signal, token) with fates Complete, Buffered, TimedOut or Never;
  - stale duplicate completions and noise SignalPayloads;
  - random arrival order and chunk sizes;
  - 1–9 random steps of `wait(All|Any)` or `cancel-op`;
  - random hot-retry or evict-and-replay re-activations.

  It asserts that live and replay traces (WaitResolved lists plus outcomes) are identical, that replay appends nothing, that every resolved list is in completion-seq order, that error outcomes match the first completing event, and that the index equals a journal scan for every seq. It passed with `OSTRO_PROP_CASES=20000`.

  **Mutation check:** changing the index to last-wins (`insert` instead of `or_insert`) made the property fail at once (shrunk to sleep → fire → wait → cancel → wait). The change was reverted and its regression seed discarded.
- `--test determinism --test wit_surface --test hot_wait --test step_retry`: all pass. `git diff --name-only crates/ostro-engine/tests/snapshots` is empty, so det-basic was **not re-blessed**.
- `--test patching` (4 tests):
  - v1 journal on v2 (complete, parked then resumed, and still parked): old path, no marker, no divergence;
  - fresh v2: `["A","C","B"]` with exactly one `add-c` marker, and v2 replaying v2 appends 0 events;
  - v2 journal on v3 (complete, and parked then resumed): marker skipped, `["A","C","B"]`;
  - control: a v1 journal on v3 is `NonDeterminism`.
- `--test live_activations` (4 tests):
  - 50 concurrent det-basic sleepers parked hot give gauge == 50, and 0 after cancel (Evicted External);
  - 50 immediate evictions end at 0;
  - a bad journal (AttemptFailed) and an aborted parked future both release;
  - a panicking sink `done` unwinds and releases.
- `cargo clippy -p ostro-engine --all-targets --locked -- -D warnings` is clean and `cargo fmt --check` is clean.
- `sdk_guests` was not run per-task, as the plan instructs (05-06 owns it in this wave). It does run inside the ci-local gate below.

## Final gate

`CARGO_TARGET_DIR=target/wt-05-03 bash scripts/ci-local.sh --no-musl` ran in a detached scratch worktree at code HEAD **`e4cd2ea`**, so other agents' uncommitted edits were excluded. Five or more CI gates from parallel executors were running on the machine at the same time.

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  0s
cargo clippy -D warnings             PASS  43s
cargo build (warning-free)           PASS  64s
sdk workspace fmt + clippy           PASS  27s
sdk workspace test + wasm build      PASS  26s
guests build (wasm32-wasip2)         PASS  17s
cargo test                           PASS  720s
failpoint suites                     FAIL  633s   (1 timing flake, see below)
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  2s
check-deny-fixtures.sh               PASS  0s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  1s
zig/musl legs                        SKIP  (--no-musl)
```

- **Flaky timing test:** `ostro-partition` `tests/rebuild_digest.rs::live_state_equals_rebuilt_state` failed under `--features failpoints` with `node open timed out: Elapsed(())` (rebuild_digest.rs:193, the per-open `timeout(T, Node::open)`). The cause is machine load from the parallel gates. `ostro-partition` does not depend on `ostro-engine`, and this plan touched no partition code.
- **Rerun alone in the same worktree:** `cargo test -p ostro-partition --features failpoints --locked --test rebuild_digest -- --test-threads=1` passed 3/3 (256 s). Every other failpoint suite passed in the full run.
- The full `cargo test` step passed, including all engine suites, `sdk_guests` and the server e2e tests.
- No new dependencies (no chrono, bincode or wasmtime `cache`), and Cargo.lock is unchanged.
- `FAILPOINT_CRATES` is unchanged.
- No journal kind or Cmd tag was added and `EVENT_CODEC_VER` is unchanged.
- No snapshot was re-blessed.

## Decisions Made

See `key-decisions` in the frontmatter. One more note for reviewers: `skip_unqueried_markers` advances logical time (`last_cmd_ts`/`last_cmd_seq`) exactly as consuming the marker did in the original run. That keeps `now()` and `continue_as_new_suggested` aligned with the recording at the next matched command.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] The token Result API keeps the Phase 3 forms**
- **Found during:** Task 3.
- **Issue:** The plan asks for `parse -> Result` and `verify(key, &str) -> Result`. `tests/wit_surface.rs` and `tests/host_adapter.rs` use `replay::token::{parse -> Option, verify(token, key) -> bool}`. Neither file is in this plan's `files_modified`.
- **Fix:** The new functions `parse_token` and `verify_token(key, token)` are re-exported as `ostro_engine::token::{parse, verify}`, which is the path 05-13 uses. The old functions became one-line wrappers.
- **Commit:** `e4cd2ea`.

**2. [Rule 1 - Test semantics] Patch removed from `replay_property`'s generic kinds**
- **Found during:** Task 2.
- **Issue:** The property expects every single-command mutation to diverge at its seq. D6 makes a deleted or moved `PatchMarker` skippable, so that expectation no longer holds for markers.
- **Fix:** `KINDS` now has 12 entries without `Patch`, with a doc comment explaining why. Marker behaviour is covered by the new `patched_*` and `unqueried_markers_are_skipped` tests. That includes a real divergence behind a skipped marker still being reported at its seq.
- **Commit:** `5773c11`.

**3. [Rule 3 - Testability] `hot_waitable` split into `hot_waitable_op`**
- Building a `HostState` needs a full store, so the per-op decision was factored into a free function to unit test W12 directly. Behaviour is unchanged for timers and effects.
- **Commit:** `80ca356`.

### Process incident (no lasting effect)

- My first Task 2 commit (`95d7d8a`) used the shared git index. That index still held **stale pre-`a82df07` blobs** for files that 05-07 had committed from a private index: STATE.md, ostro-meta and ostro-server/control/modules.rs. Committing would have **reverted 05-07's work**: 13 foreign files, -876 lines.
- I caught it from the commit's file list straight away. I undid it with `git reset --soft HEAD~1`, which keeps both the index and the working tree, and unstaged only my own paths. I re-committed from a private index as `5773c11`.
- The reflog shows that nothing was committed on top of `95d7d8a`. `git diff a82df07 5773c11 -- .planning crates/ostro-meta crates/ostro-server` is empty.
- Every later commit, including the docs commit, uses a private index (`GIT_INDEX_FILE`, `read-tree HEAD`, add my paths only).
- **Advice for other agents:** the shared index can be stale. Never `git add` and `git commit` with the default index.

## Issues Encountered

None beyond the items above.

## Hand-off to dependants

- **05-09 (terminate/retry/continue-as-new) and 05-11 (re-arm, executor):**
  - A resumed run's re-arm should treat an op as open iff `ReplayState::completion_for(op)` is `None`. This now includes guest `OpCancelled`, so a cancelled signal or token op must **not** be re-armed. Equivalently, scan for `completion_target`. The function is `pub(super)` in `replay::wait`; if you need it outside the engine, add a public wrapper in the engine rather than duplicating the rule.
  - `RunTerminated` (71) can be delivered to a hot-parked activation and is ignored, since it completes nothing.
- **05-08 (apply signals/tokens/waits):**
  - The engine requires `SignalMatched.payload_seq < SignalMatched.seq` and that seq to be a `SignalPayload` event in the same run's journal. Otherwise the activation fails with ProtocolViolation `bad journal: signal-matched op …`.
  - Write SignalPayload and SignalMatched in one batch for immediate matches. For buffered matches, use the inbox slot's `payload_seq`.
  - A `WaitTimedOut` after a `SignalMatched` (or after a guest cancel) for the same op is harmless (first wins), but apply should still not produce one.
- **05-10 (phase5 guests, race-fuzz, `sdk_cancellation`):**
  - `ctx.cancel(&op)` followed by waiting on that op now returns `Err(Cancelled)` immediately; it no longer hangs.
  - `select!` losers can be cancelled.
  - Signal and token waits never hot-park, so they always evict.
  - Use `OSTRO_PROP_CASES` for proptest case counts.
- **05-12:**
  - `Runtime::live_activations()` gives the per-runtime number of activations currently running, hot-parked ones included. Combine it with `runtime.parked().parked_count()` for the "1,000 sleeping runs hold no memory" assertion. Both must be 0 once all runs are Scheduled.
- **05-13 (token API):**
  - Use `ostro_engine::token::verify(&key, token) -> Result<TokenParts, TokenError>`. The variants are `BadPrefix`, `Malformed` and `BadMac`; map all three to 400 `token_invalid`.
  - `parse` returns routing fields without a MAC check.
  - Neither `TokenError` nor `TokenParts` contains token text or MAC bytes. Keep token bodies out of URLs and logs.
- **05-23 (cursor/context/sdk_guests):**
  - `ReplayState` gained `completions` and `patched` fields, plus a new private helper `skip_unqueried_markers`.
  - `match_next` skips markers for every non-Patch kind, and `finish()` skips trailing markers.
  - `is_replaying()` and `next_command_hash_ver()` were deliberately left unchanged and still see unconsumed markers. If 05-23 changes either, keep `patched()` able to see its own marker.
- **05-05 (WIT docs):** document as frozen semantics:
  - `cancel-op` completes the op with `failure::cancelled`, unless an input completed it first (first completion in journal order wins);
  - `wait-signal`/`create-await-token` timeouts complete with `failure::timeout`;
  - signal and token waits always evict, never hot-park;
  - `patched` is memoised per change-id, and an unqueried marker is skipped on replay.
- **05-06 (SDK):**
  - No engine change is needed for D11. `wait([op, CANCELLATION], Any)` works as before.
  - `tests/patching.rs` asserts that a resumed v1 activation appends `[WaitResolved, StepCompleted, RunCompleted]`. If D11 makes `Op::wait()` emit a different number of guest commands, update that assertion: the patching suite is mine, but it reads the SDK's shape.

## Next Phase Readiness

The engine semantics that freeze with WIT 0.1 are in place. 05-08 can rely on the SignalMatched/WaitTimedOut handling, 05-13 on the token API, and 05-12 on the gauge.

## Self-Check: PASSED

**PASSED.**

- All created files were FOUND: patching.rs, live_activations.rs, the patch-v1/v2/v3 sources and this SUMMARY.
- All three commits were FOUND: `80ca356`, `5773c11`, `e4cd2ea`.
- Each artifact contains its check string: `gather_race_prop` in replay/tests.rs, `pub fn live_activations` in runtime.rs, `OpCancelled` in host/effects.rs, `pub fn verify` in replay/token.rs.
