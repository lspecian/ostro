---
phase: 05-workflow-semantics-core-hardening
plan: 08
subsystem: partition-apply
tags: [signals, waits, await-tokens, wait-timeout, graceful-cancel, d21, b1, w5, pf-f2, fifo, rebuild-digest]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 contract (Cmd 8/9/12, Intent 7/8, kinds 70/96/97, limits); 05-02 waits/signal_seen keyspaces, SignalSlot, journal_input/journal_encoded, terminal hook, waits.rs seams, gen_waits hook; 05-03 engine SignalMatched/WaitTimedOut completion rules"
provides:
  - "waits.rs: signal_run, inbound_signal (wait match + B1), on_first_events (B1 conversion), check_register_wait/register_wait (idempotent, FIFO match from the inbox), check_cancel_wait/cancel_wait, try_wait_timeout (WaitTimedOut), complete_token (D21 verbatim), request_cancel + inbound_cancel (flag, CancelRequested once, cascade, PF-F2 forwarding)"
  - "inbound CancelRun goes through waits::inbound_cancel; the legacy InboxMsg::CancelRequested write is gone"
  - "tests/apply_waits.rs (26 tests) and the gen_waits rebuild_digest generator"
affects: [05-11, 05-12, 05-13, 05-15, 05-16, 05-17, 05-19, 05-20, phase-9]

tech-stack:
  added: []
  patterns:
    - "open_waits as an exact read guard: every wait-row read and the waits-by-name prefix are skipped when open_waits == 0"
    - "The wait row is the completion token: whichever of signal/token/timeout/CancelWait applies first removes row + index + timer row"

key-files:
  created:
    - crates/ostro-partition/tests/apply_waits.rs
    - .planning/phases/05-workflow-semantics-core-hardening/05-08-SUMMARY.md
  modified:
    - crates/ostro-partition/src/apply/waits.rs
    - crates/ostro-partition/src/apply/inbound.rs
    - crates/ostro-partition/tests/common/gen_waits.rs
    - crates/ostro-partition/tests/apply_seams.rs
    - crates/ostro-partition/tests/apply_contract_p5.rs
    - crates/ostro-partition/tests/apply_cross.rs
    - .planning/ROADMAP.md
    - .planning/REQUIREMENTS.md

key-decisions:
  - "Wait-row reads and the waits-by-name prefix are skipped when open_waits == 0, so a sleep TimerFired keeps the 04-13 two-read budget and a signal to a run without waits costs one read"
  - "A matched signal costs one more point read than the 05-02 budget table lists (the matched wait row; its timeout_due_ms locates the timer row, the FIFO index value is empty by the documented format)"
  - "RegisterWait for an op >= next_seq is a deterministic no-op (the write-loop seam cannot reject); reserved ops and unknown kinds are rejected BadIntent in check_register_wait"
  - "on_first_events journals pending signals in arrival order (inbox counter n), not key (name) order"
  - "Inbound Signal at next_seq == 0 still wakes a Scheduled run (Phase 2 rule, apply_cross test); SignalRun at next_seq == 0 does not (plan B1). Neither wakes a Scheduled run for a signal nobody waits for"
  - "Cancel at next_seq == 0 sets the flag and wakes a Scheduled run, journals nothing"
  - "SignalMatched ts: SignalRun.sent_at_ms / RegisterWait.at_ms; inbound Signal (no time) uses rec.created_at_ms. Inbound CancelRun likewise"

patterns-established:
  - "Read-guard on an exact counter (open_waits) to keep hot paths at their old read budget"

requirements-completed: []

duration: ~2h40m
completed: 2026-11-19
---

# Phase 5 Plan 08: Signals, Waits, Await Tokens and Graceful Cancel (apply) Summary

**Apply now runs the whole wait lifecycle. A submitter-derived `RegisterWait`/`CancelWait` registry, FIFO per name; D21 signals (`SignalPayload` verbatim, then apply's own `SignalMatched`, or a buffered inbox slot matched later at registration); B1 pending slots journaled after the seq-0 batch; `WaitTimedOut` timeouts with exactly one completion per op; verbatim `TokenCompleted` tokens; graceful cancel journaled once (flag only before seq 0) with the children cascade and PF-F2 forwarding along continue-as-new chains; W5 oversize rejects. Apply never decodes or fetches an event payload.**

## Performance

- **Duration:** about 2 h 40 min. Much of it was test wall time under a load average of 10–25, with 4+ parallel executors.
- **Started:** 2026-11-19T07:50:00Z (approx.)
- **Completed:** 2026-11-19T10:32Z
- **Tasks:** 3 of 3
- **Files:** 1 created (`apply_waits.rs`), 7 code files modified, plus this SUMMARY and the ROADMAP/REQUIREMENTS hunks

## Task Commits

1. **Tasks 1 + 2: waits registry, signals (SignalRun + inbound, B1), wait timeouts, CancelWait, CompleteToken, RequestCancel + inbound CancelRun, PF-F2 forwarding, apply_waits suite, tripwire retirements:** `aee02e2` (feat). Tasks 1 and 2 share `waits.rs` and were verified together, so they are one commit.
2. **Task 3: gen_waits rebuild_digest generator:** `1070fe0` (test)

TDD note: the behaviour tests in `apply_waits.rs` were written together with the implementation, as in 05-02. A FIFO mutation check stood in for a RED step: changing both oldest-first lookups to `.last()` failed 3 tests (`signal_matches_the_oldest_wait_fifo`, `signals_before_waits_are_consumed_in_send_order` and the B1 test). The change was reverted.

## What landed

**waits.rs**
- **Op registry.** `register_wait` is idempotent: an existing row changes nothing (no second timer, `open_waits` unchanged). For a signal wait it first checks the inbox for the oldest slot with that name, and on a hit journals `SignalMatched{op, payload_seq}` at `at_ms` and writes no wait or timer row. Otherwise it inserts the row, the FIFO index (signals only) and the timer. A past-due timer is still inserted, and the timer service fires it as `WaitTimedOut`. `cancel_wait` removes the row, index and timer, and an absent wait is a no-op. `check_register_wait` rejects `FieldTooLong`, an unknown kind and a reserved op (`BadIntent`). `check_cancel_wait` rejects a reserved op.
- **Signals.** `signal_run` and `inbound_signal` share `deliver_signal`:
  - at `next_seq == 0`: a `Pending` slot, with nothing journaled;
  - otherwise: `SignalPayload` is journaled verbatim. The oldest wait for the name then gets `SignalMatched`, its row and timer are removed and the run is woken. With no wait, a `Journaled` slot is stored, and a Running run gets `InputAppended` for the payload seq while a Scheduled run is not woken.
  - SignalRun dedupe goes through `signal_seen`.
- **B1.** `on_first_events` journals every `Pending` slot in arrival order and rewrites it to `Journaled`, before the batch's intents run, so a `RegisterWait` in the same batch matches it.
- **Timeouts.** `try_wait_timeout` journals `WaitTimedOut{op}` when the wait row exists and returns `None` otherwise. A signal, token or `CancelWait` that applies first removes the timer row, so a late fire is `Duplicate` at timer.rs's row check.
- **Tokens.** `complete_token` journals the encoded `TokenCompleted` verbatim. A repeat, or a completion after a timeout, is `Duplicate`. A signal-kind wait, or an op at or past `next_seq`, is `Invalid(BadIntent)`. A missing event is `MissingField`. Terminal and unknown runs get `RunTerminal` and `RunNotFound`.
- **Cancel.** `request_cancel` and `inbound_cancel` share `cancel()`:
  - it sets the flag and reason;
  - it journals `CancelRequested` at `requested_at_ms`, except at `next_seq == 0`, where only the flag is set and a Scheduled run is woken;
  - it emits `Action::CancelRequested` and cascades `CancelRun` to children, scanning only when `children > 0`;
  - a repeat is `Duplicate`;
  - a terminal `ContinuedAsNew` run with `continued_to` forwards `CancelRun` through the outbox (PF-F2), with the target validated by `msg_target` first;
  - any other terminal run is `RunTerminal`.
- **W5.** An inline event over 256 KiB is `PayloadTooLarge` for SignalRun and CompleteToken. For inbound Signal, the existing `check_msg` rejects the whole batch (tested). A blob ref of any size is accepted and journaled verbatim.

**Read counts** (doc table at the top of waits.rs; each one asserted in apply_waits.rs):

| Path | Reads |
|---|---|
| SignalRun | run; + `signal_seen` if a request_id is set; + waits-by-name prefix if `next_seq > 0 && open_waits > 0`; + the matched wait row on a match (1..=4) |
| RegisterWait (on top of EventsAppended's run read) | wait row if `open_waits > 0`; + inbox-by-name prefix for signal waits (0..=2) |
| CancelWait | wait row if `open_waits > 0` |
| CompleteToken | run; + wait row if `open_waits > 0` |
| RequestCancel | run; + children prefix if `children > 0` |
| TimerFired (guest op) | timer row + run (timer.rs); + wait row if `open_waits > 0` |
| on_first_events | inbox run prefix if `inbox_next > 0` |

**inbound.rs:** `Kind::CancelRun` now calls `waits::inbound_cancel`. The legacy `cancel_run`, which pushed `InboxMsg::CancelRequested`, is removed.

**gen_waits.rs:** `WaitOp::{Register, Signal, Token, Cancel, Fire}`, with the options listed in the module doc. Register, Token and Fire prefer granted runs. A temporary outcome tally over 6 cases (instrumentation not committed) showed accepted RegisterWait batches, accepted SignalRun (B1 and matches), accepted RequestCancel, CompleteToken Duplicate/Invalid and TimerFired Duplicate.

## Decisions Made

See `key-decisions` in the frontmatter. The orchestrator asked for STATE notes in this SUMMARY rather than through gsd-tools mutators; they are under "STATE notes" below.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1: Bug] A wait-row read on every guest timer broke the 04-13 `TimerFired ≤ 2 reads` budget**
- **Found during:** Task 1, in the full partition suite.
- **Issue:** `try_wait_timeout` read the wait row for every guest-op fire, including sleeps. `apply_timer_input` (2 tests) and `apply_cross::timer_fire_once` assert ≤ 2 reads.
- **Fix:** `RunRecord.open_waits` is exact, so every wait-row read and the waits-by-name prefix are skipped when it is 0. Sleeps of runs without waits keep 2 reads, and a signal to such a run costs 1 read. A new test, `sleep_timer_with_an_open_wait_still_journals_timer_fired`, covers the `None` path with an open wait (3 reads).
- **Files:** `waits.rs`, `apply_waits.rs`. **Commit:** `aee02e2`.

**2. [Rule 2: Correctness] RegisterWait/CancelWait in the apply-reserved op range are rejected `BadIntent`**
- A wait timer at `RUN_TIMEOUT_OP` etc. would be routed to lifecycle by timer.rs. `check_register_wait` and `check_cancel_wait` reject reserved ops, as `check_intents` already does for timers. Tested in `register_wait_validation`.

**3. [Rule 2] The PF-F2 forward target is validated with `msg_target` before `emit`**
- `emit` returns a fatal `StoreError` for a target outside the partition count. A bad `continued_to` is treated as a plain terminal run (`RunTerminal` / drop) instead.

### Plan conflicts resolved (documented, not silent)

**4. [Rule 3: Blocking] `op >= next_seq` in RegisterWait is a no-op, not `Rejected(Invalid)`**
- `check_register_wait(cur, w)` has no run record, and its call site is in run.rs, which is not mine. The write loop cannot reject without partial writes, so an op past the journal end is dropped deterministically. This is documented on `register_wait` and pinned by `register_wait_for_an_unjournaled_op_is_dropped`. If a hard reject is wanted, 05-11 (which owns run.rs in wave 4) can pass the post-events `next_seq` into `check_register_wait`.

**5. A matched signal costs one more read than the 05-02 budget table lists**
- The FIFO index row value is empty (documented in storage-format §13), so the matched op's `timeout_due_ms`, which locates its timer row, needs a point read of the wait row. SignalRun is therefore 1..=4 reads (table above), and every count is asserted. **05-20 should update storage-format §13's read-budget table** with the waits.rs table, including the `open_waits == 0` guards.

**6. Pending slots are journaled in arrival order (inbox counter `n`), not `(name, n)` key order**
- Per-name FIFO holds either way. Arrival order makes the history show send order.

**7. Wake rules on the no-wait paths**
- SignalRun at `next_seq == 0` does not wake (plan B1).
- Inbound Signal at `next_seq == 0` keeps the Phase 2 / 05-02 wake of a Scheduled run, because `apply_cross::inbound_signal_wakes_scheduled_run` pins it.
- A journaled signal that nobody waits for never wakes a Scheduled run (plan text).
- Cancel at `next_seq == 0` sets the flag and wakes a Scheduled run, but journals nothing (the old inbound wake behaviour is kept).

**8. Tripwire files outside `files_modified` (designed hand-off, minimal edits)**
- 05-01's `apply_contract_p5.rs` says "each `*_until_implemented` assertion is replaced by the owning plan". 05-02's `apply_seams.rs` names 05-08 as the plan that extends the inbound-signal tripwire. `apply_cross.rs` is in the plan's own Task 2 verify command and asserted the removed legacy cancel row.
- No parallel plan owns these files. 05-22's concurrent `apply_cross.rs` hunk is in a different test and merged cleanly.
- What changed:
  - **`apply_contract_p5.rs`:** removed the SignalRun/RequestCancel/CompleteToken commands and the RegisterWait/CancelWait intents from the two tripwire lists. The 05-09 entries stay.
  - **`apply_seams.rs`:** removed the 05-08 commands from `phase5_commands_reach_their_stubs_and_change_nothing` and retired `phase5_intents_reach_their_stubs_and_change_nothing`, leaving a comment pointer. `inbound_signal_buffers_without_waits_until_implemented` is renamed to `inbound_signal_buffers_without_waits`. Under B1 the earlier Pending slot is now journaled at seq 2 after the seq-0 batch, so the next signal lands at seq 3.
  - **`apply_cross.rs` `inbound_cancel_cascade`:** there is no legacy inbox row any more. The parent's journal is empty, so the test asserts flag only and 0 `CancelRequested` events.
- 05-09 will drop its own entries from the same two tripwire tests.

**9. Tasks 1 and 2 are one commit** (shared `waits.rs`; see Task Commits).

## Verification

- `cargo test -p ostro-partition --test apply_waits --locked`: 26 passed.
- `cargo test -p ostro-partition --locked --no-fail-fast` on the final base `64eecf2` (with 05-22): all 24 binaries green, including `apply_ready_attempts`, `shuffler`, `shuffler_exactly_once`, `timers` and `apply_timer_input`.
- **rebuild_digest (case count unchanged):**
  - 221 s with the generator at load average 10–17;
  - 237 s for the baseline with the generator stubbed to `None` (load 10–17, same session);
  - 260 s under load average about 23;
  - 179 s at load about 7 on the final base.
  - The runtime increase is about 1.0–1.1x, well under the 2x limit.
- `cargo test -p ostro-partition --features failpoints --locked -- --test-threads=1`: all green.
- `cargo clippy -p ostro-partition --all-targets [--features failpoints] --locked -- -D warnings`: clean. `cargo fmt --check`: clean.
- **`CARGO_TARGET_DIR=target/wt-05-08 bash scripts/ci-local.sh --no-musl`** in a detached scratch worktree at `1070fe0`: **all steps passed** on the first run (1563 s). The steps were fmt, clippy, the warning-free build, SDK, guests, order-saga, `cargo test` (workspace), the failpoint suites, deny, vendored, deny-fixtures, the chronik grep and workflow validation; musl was skipped by the flag. None of the known flaky tests (dev_startup, server_config live_start_with_toml, ostro-wal first_write) failed, so no rerun was needed.

## Interface for 05-11 (server seam, derive and re-arm)

- **Derive.** `SignalWaitStarted` and `AwaitTokenCreated` become `RegisterWait{op: seq, kind, name, timeout_due_ms, at_ms: event ts in ms}`. `cancel-op` on a signal or token op becomes `CancelWait{op}`. An op at or past the batch's journal end is silently dropped, so always register in the same batch as the event, or later.
- **Re-arm after resume.** Send `RegisterWait` only for ops with no completion in the journal (`ReplayState::completion_for(op) == None`, guest `OpCancelled` included). Repeats are harmless (idempotent), but apply cannot detect an already-satisfied op and would re-insert it. That case is pinned by `register_wait_for_a_satisfied_op_reinserts_documented_contract`.
- **Never put `RegisterWait` and `Suspend` in one `EventsAppended`.** A registration-time match journals past the executor's end. The separate `Suspend` CAS then fails, and the executor re-activates (Pitfall 2). `sink.rs` already sends Suspend on its own.
- **Cancel at `next_seq == 0`.** Apply sets only `cancel_requested` and `cancel_reason` and journals nothing. RunStarted synthesis must read the flag, and 05-11 journals `CancelRequested{reason: rec.cancel_reason}` right after RunStarted.
- **Inputs a running activation now receives through `InputAppended`:** `SignalPayload` (96) with no match, `SignalPayload` + `SignalMatched` (97), `WaitTimedOut` (70), `TokenCompleted` (68) and `CancelRequested` (69). B1 conversion in the seq-0 batch also emits `InputAppended` per pending payload, so the executor's next append conflicts and re-activates.
- **Timestamps.** `SignalMatched` from an inbound outbox Signal, and `CancelRequested` from an inbound CancelRun, are stamped `rec.created_at_ms` because the outbox messages carry no time. The engine's logical clock must take max(previous, ts); 05-23's D22 rule already does this for retries.

## Hand-off notes

- **05-12 (replay corpus):** signal journals are `SignalPayload`, then `SignalMatched{op, payload_seq < seq}` (immediate match), or a `SignalPayload` far earlier with `SignalMatched` at registration. B1 journals put pending payloads right after the seq-0 batch's events, in arrival order. `WaitTimedOut` never follows a match for the same op, and `TokenCompleted` is journaled at most once per op. These shapes are good corpus seeds. `apply_waits.rs` builds them with opaque payloads; use real encoded events from the SDK guests.
- **05-16 (start path):** call `waits::on_first_events(b, &run, &mut rec, actions)` right after `StartRun.run_started` writes seq 0. It reads one inbox prefix only when `inbox_next > 0`; a fresh StartRun has `inbox_next == 0`, so the call is free. If RunStarted is written at StartRun, signals can no longer arrive before seq 0, and the Pending path becomes dead for those runs, which is harmless. Keep the run.rs call for the EventsAppended path. The `FIRST_EVENTS_CALLS` counter is still incremented.
- **05-13 (API):** outcome mapping:
  - SignalRun: `Duplicate` means the request_id was seen; `RunNotFound`/`RunTerminal` as usual; `PayloadTooLarge` only if prepare skipped blob offload.
  - CompleteToken: `Duplicate` → 409 `token_already_completed`; `Invalid(BadIntent)` → no such token op, or not a token wait.
  - RequestCancel: `Duplicate` means already requested; `Accepted` is also returned for a forward along a continue-as-new chain.
- **05-15 (e2e):** 1 MiB signal/token payloads go through as blob refs; apply never fetches them. A signal sent right after start lands in the B1 Pending path.
- **05-20 (gate):** update the storage-format §13 read-budget table from the table at the top of waits.rs. `values.rs`'s `InboxMsg::CancelRequested` doc ("still written until 05-08") is stale: the variant is now never written but stays decodable. Flip RESERVATIONS rows SignalRun / RequestCancel / CompleteToken / RegisterWait / CancelWait / WaitTimedOut / SignalPayload / SignalMatched to `exists`.

## STATE notes (for the orchestrator; no gsd-tools mutators were run)

- Position: 05-08 complete (wave 3). Phase 5: 13/24 SUMMARYs.
- Decisions to record:
  - waits.rs uses `open_waits` as an exact read guard;
  - SignalRun is 1..=4 reads;
  - RegisterWait for a not-yet-journaled op is a no-op;
  - pending signals are journaled in arrival order;
  - cancel at seq 0 is flag only, plus a wake.
- No blockers.

## Self-Check: PASSED

- The three code files exist: `crates/ostro-partition/src/apply/waits.rs` (811 lines; min 200), `crates/ostro-partition/tests/apply_waits.rs` and `crates/ostro-partition/tests/common/gen_waits.rs`.
- Both commits were found on main: `aee02e2` and `1070fe0`.
- The key-link patterns are present: `journal_encoded` (7 occurrences in waits.rs) and `wait_name_prefix` (2).
- `bash scripts/ci-local.sh --no-musl` passed every step at `1070fe0`.
