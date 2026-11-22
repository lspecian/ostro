---
phase: 09-polyglot-sdks-conformance
plan: 08
subsystem: children
tags: [children, wf-06, apply, derive, sink, pf-f3, b3, w1, w3, failpoints, wave-3]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-05 children 1/2: child_run_id, resolve_child, plan_child_repairs, ChildJournal marker (D-P9-31), parent journaling"
  - phase: 05-workflow-semantics-core-hardening
    provides: "terminal hook (05-02), cancel cascade + CAN forwarding (05-08), hard stop / RetryRun / EXPIRE (05-09), journal-load re-arm (05-11)"
provides:
  - "ostro_types::ids::child_run_uuid (D-P9-7 uuid part, shared by apply and children::child_run_id)"
  - "apply: parent-close cascade (terminal::close_children), no children row for detached starts, duplicate-StartChild re-completion / CAN forward, ChildCompleted row guard with chain uuid match, chain_done, deferred EXPIRE of CAN child links"
  - "server: OpCancelled(child) -> CancelRun parent_cancel_op (ChildCtx::child_run), detached {run_id} completion, full repair table (detached, W-b child_terminated, cancelled ops skipped), child:: failpoints"
affects: [09-13, 09-14, 09-15, 09-18, 09-20]
tech-stack:
  added: []
  patterns:
    - "Chain bookkeeping without a new field: continued_to.is_some() = chain live, next_seq = 0 on a ContinuedAsNew record = expiry deferred"
    - "CancelRun reason strings as message sub-kinds (parent_closed, parent_cancel_op, chain_done)"
key-files:
  created:
    - crates/ostro-partition/tests/children_close.rs
    - crates/ostro-partition/tests/children_chain.rs
    - crates/ostro-partition/tests/children_common/mod.rs
    - crates/ostro-server/tests/children_detached.rs
    - crates/ostro-server/tests/children_resume.rs
    - crates/ostro-server/tests/children_failpoints.rs
    - crates/ostro-server/tests/children_fx/mod.rs
  modified:
    - crates/ostro-types/src/ids.rs
    - crates/ostro-partition/src/apply/terminal.rs
    - crates/ostro-partition/src/apply/lifecycle.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/inbound.rs
    - crates/ostro-partition/src/apply/retention.rs
    - crates/ostro-partition/tests/apply_lifecycle.rs
    - crates/ostro-server/src/children.rs
    - crates/ostro-server/src/derive.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/tests/children_derive.rs
key-decisions:
  - "A ChildCompleted with no matching children row keeps the v0.1 legacy inbox path (not a drop; freeze rule, PF-F2 test); journaling needs and consumes a marked row, so exactly-once journaling holds; no counter decrement for a missing row"
  - "Hard stops (Terminated/TimedOut) keep 05-09's cascade and its hard-stop reason (e.g. the terminate reason, run_timeout) instead of parent_closed, so the 05-09 outbox order (ChildCompleted to the grandparent first) and its tests stay unchanged; the cascade now also removes the rows"
  - "Deferred-expiry mark = next_seq 0 on a ContinuedAsNew record with a parent (journal deleted); exactly one deferral, the bound EXPIRE at completed_at + 2 x retention deletes fully"
  - "chain_done is never a cancel request: live runs, non-CAN terminal runs and CAN runs without continued_to drop it (a user cancel reason equal to chain_done, cascaded to a child, is dropped too: documented limit)"
  - "W-b uses the ts of the journal's last RunTerminated event (RetryRun clears completed_at_ms) plus the parent's retention_ms"
  - "plan_child_repairs treats OpCancelled ops as closed (no re-send for a cancelled child op after a resume)"
requirements-completed: [WF-06]
duration: ~5h (incl. two full partition suite runs and PROPTEST_CASES=128 rebuild_digest, 830 s)
completed: 2026-11-22
---

# Phase 9 Plan 08: Children 2/2 Summary

**Cancel-op and parent close cancel children, detached children complete at once with their run id, a resumed parent's repair re-sends its starts and terminal children re-complete event-less, the parent accepts a completion only with a matching children row (continue-as-new successors through `child_run_uuid`), and chain predecessors stay forwardable until `chain_done`.**

## Performance

- Tasks: 3/3 (Tasks 1 and 2 in one commit, see Deviations)
- Commits: `d3f5d2d` (apply, Tasks 1+2), `e8e7e3d` (fix: no-row ChildCompleted keeps the v0.1 path), `62bc4d2` (server, Task 3), docs commit last

## What was built

### Apply (`d3f5d2d`)

- **`ids::child_run_uuid(parent, op)`**: the D-P9-7 digest's uuid part (version 8, variant `10`). Golden vectors equal 09-05's ids (`0071t7sbx8jw72bzhfyq2k5jaj6zw`, `002yhpcepx40309vk1cdaptr0q16j`, `0001t7sbx8jw72bzhfyq2k5jaj6zw`). `children::child_run_id` calls it, so there is one definition.
- **Cascade helper reused/shared**: `terminal::close_children` (`crates/ostro-partition/src/apply/terminal.rs:89`) sends a graceful `CancelRun{reason}` to every children row, removes the rows and sets `children = 0`. The terminal hook (`terminal.rs:76`) calls it with `parent_closed` for Completed, Failed, Cancelled and ContinuedAsNew when `children > 0` (no scan otherwise). 05-09's `lifecycle::cascade_cancel` (`lifecycle.rs:146`) now delegates to it, so hard stops cascade with the hard-stop reason after the parent's own `ChildCompleted` and remove the rows too. The 05-08 cancel-request cascade (live parent) is unchanged and keeps the rows.
- **Detached rows**: `run.rs` and the CAN batch (`lifecycle.rs`) insert a children row only when the StartChild has a `ParentRef`. A `ChildJournal` after a detached start is `Invalid(BadIntent)`. This fixes 09-05 open issue (3).
- **Duplicate-StartChild rules (B3)** (`inbound.rs:250`): a StartChild whose run exists and whose `RunRecord.parent == (parent, op)` is handled by child state. Terminal (Completed, Failed, Cancelled, Terminated, TimedOut): outbox `ChildCompleted{parent, child, op, status, result: None, event: None}`. ContinuedAsNew with `continued_to`: the StartChild is forwarded with `run_id = continued_to`. Live child, parent mismatch, or detached duplicate: drop. Cost: one extra point read.
- **Row-guard lookup** (`inbound.rs` `child_completed` + `chain_row` at `:316`): the exact `(parent, child)` row is tried first. Only on a miss, and only when `children > 0`, one prefix scan looks for the row whose child uuid equals `child_run_uuid(parent, op)`. The matched row's value decides the outcome: `[1]` journals (09-05), an unmarked row takes the legacy inbox path. The matched row is removed and the counter decremented, so a re-completion never journals twice. A uuid (chain) match also emits `CancelRun{original child, "chain_done"}`. This addresses 09-05 open issues (1) and (2).
- **No matching row: the v0.1 path, not a drop** (see Deviations 1). The completion keeps the legacy inbox row and wake. The only change: no row removal and no counter decrement for a row that does not exist (counter drift fix).
- **chain_done / deferred expiry** (`inbound.rs:336`, `retention.rs:97-150`):
  - A ContinuedAsNew run that receives `chain_done` forwards it to `continued_to`. If its expiry was not deferred, it clears `continued_to`; if it was deferred (kept record), it deletes itself and cancels the bound timer.
  - EXPIRE of a ContinuedAsNew run with a parent and `continued_to` is deferred. Only its events and payloads are deleted, `next_seq` and `status_seq` are reset, and EXPIRE is re-armed at `completed_at + 2 × retention`. The EXPIRE at that bound deletes the record fully, even if `chain_done` never arrived.
  - While a record is kept, other CancelRuns are still forwarded (05-08 path).
  - Top-level chains expire as before.

### Server (`62bc4d2`, Task 3)

- `derive.rs`: `ChildCtx` gains `child_run: &dyn Fn(u64) -> Option<RunId>`. An `OpCancelled` of an attached `ChildStarted` (same batch or prior lookup) maps to `SendMessage(CancelRun{child_run_id(run, op, P), "parent_cancel_op"})`. A detached child maps to nothing.
- `children.rs`: the repair table, `detached_completion`, `expired_child_completion`, `last_closed_ms`, and the consts `CHILD_TERMINATED_KIND` and `REASON_PARENT_CANCEL_OP`.

  | child | children row | target now | repair |
  |---|---|---|---|
  | attached | present | — | none (never re-decided) |
  | attached | absent | resolvable | re-send StartChild + `ChildJournal` |
  | attached | absent | unresolvable | `unknown_workflow` once |
  | attached | absent, parent closed > retention ago (W-b) | — | `Err(non-retryable child_terminated)`, no re-send |
  | detached | — | resolvable | re-send StartChild (duplicate-safe) + `{run_id}` completion |
  | detached | — | unresolvable | `unknown_workflow` once |
  | any | op has `OpCancelled` | — | none |

- `sink.rs`:
  - Detached `{run_id}` completions are journaled right after the flush, together with the other post-flush completions (hot-wait channel too).
  - Every repair action is executed on journal load.
  - Failpoint locations (feature `failpoints`, `return` gives `Unavailable`):
    - `child::parent_event_before_flush`: `crates/ostro-server/src/sink.rs:788`
    - `child::start_child_after_flush`: `sink.rs:862`, after the accepted flush of any batch with a `ChildStarted` (resolved or not), before post-flush completions
    - `child::detached_after_flush`: `sink.rs:869`

### CancelRun reason strings

`parent_closed` (parent-close cascade, executor/CAN terminals), `parent_cancel_op` (cancel-op of an attached child), `chain_done` (chain bookkeeping, never a cancel). Hard stops keep their own reason. None of these is a registry space, and no field was added.

## Tests

- `ostro-partition/tests/children_close.rs` (7):
  - every terminal (6 statuses) cancels and removes both rows with the right reason;
  - +1 read only with children;
  - an ended child drops the late cancel;
  - detached gets no row (plain and CAN batch);
  - a marker after a detached start is BadIntent;
  - hard stop order (ChildCompleted first).
- `ostro-partition/tests/children_chain.rs` (15):
  - duplicate start for every terminal status, live, other parent/op/detached, CAN forward then re-completion from the successor;
  - row guard: exact once, no-match never journaled (v0.1 inbox path, no counter drift), chain uuid match + chain_done + no leak;
  - chain_done forward/clear, chain_done elsewhere dropped, deferral until chain_done, bounded deferral, top-level unchanged, parent close mid-chain through the kept predecessor, end-to-end chain;
  - a two-partition Node workload (attached, detached, CAN chain, re-sent start, terminate cascade + RetryRun + re-send) whose live state equals the state rebuilt from the log.
- `ostro-server/tests/children_detached.rs` (9): cancel-op derive (same batch, prior, detached), sink cancel-op reaches the child, detached `{run_id}` completion on the hot-wait channel with no row, and one test per repair-table row (incl. W-b both sides and the cancelled-op skip).
- `ostro-server/tests/children_resume.rs` (3): C-CHILD-10 on a 2-partition node with the child on partition 1:
  - (i) child Cancelled gives `Err(Cancelled)`;
  - (ii) child Completed while the parent was terminal gives `Err(child_terminated)`;
  - (iii) child still running: the duplicate is dropped, the row comes back and its own completion is journaled.
  - In every case there is exactly one completion and the parent completes.
- `ostro-server/tests/children_failpoints.rs` (4, `--features failpoints --test-threads=1`): `start_child_after_flush` (one child, re-append conflicts), `detached_after_flush` (`{run_id}` once), `parent_event_before_flush` (identical bytes, one parent completion), and W1 deploy-between (unresolvable target, crash, deploy, repair starts the real child, no `unknown_workflow`).
- Results:
  - `cargo test -p ostro-partition -p ostro-types` all binaries green, incl. format_corpus (the v0.1 corpus rebuilds unchanged), apply_lifecycle, apply_retention, children_apply;
  - `PROPTEST_CASES=128 rebuild_digest` 7/7;
  - clippy `-D warnings` clean for ostro-partition/ostro-types and ostro-server, with and without `failpoints`;
  - `cargo test -p ostro-server` green, incl. `executor_phase5` 12/12 after `e8e7e3d`; `children_failpoints` 4/4 with `--features failpoints --test-threads=1`;
  - **`bash scripts/ci-local.sh --no-musl` at `62bc4d2`: all steps passed.** cargo test took 2798 s, failpoint suites 2318 s, campaign smoke 20 kills 730 s; externals and musl were SKIP.

## Deviations from Plan

1. **[Freeze rule] No-row ChildCompleted keeps the v0.1 semantics instead of a per-message drop.** With the drop as planned, `executor_phase5::can_chain_keeps_the_parent_and_reports_once_pf_f2` (Phase 5, PF-F2) failed. That test sends a chain-end completion to a parent with no children row and expects the legacy inbox row. The coordinator rule is "never change existing v0.1 command semantics", so a completion without any matching row keeps the legacy inbox row and wake. Exactly-once journaling still holds, because journaling requires a marked row and consumes it. The only side effect is a stray legacy inbox row plus a wake on a P9 parent for a stray re-completion. One small v0.1 change remains, as a Rule 1 counter-drift fix: when no row exists, nothing is removed and `children` is not decremented. In v0.1 the decrement was blind, which is invisible when `children == 0`, as in that test and in the corpus.
2. **[Rule 1] Hard-stop cascade reason.** The plan says `parent_closed` for Terminated/TimedOut too. Doing that would reorder 05-09's outbox (the cascade would precede the parent notification) and change its reason, breaking `apply_lifecycle::terminate_notifies_the_parent_and_cancels_children_gracefully`. Hard stops therefore keep the hard-stop reason, and the 05-09 cascade now removes the rows (`lifecycle.rs`, a file outside `files_modified`: `cascade_cancel` delegates to `terminal::close_children`, plus the detached guard in the CAN batch).
3. **[Scope] Tasks 1 and 2 in one commit.** `inbound.rs` interleaves the row guard (Task 1) and chain_done (Task 2), so the apply work landed as one commit.
4. **[Rule 3] Test fixes outside `files_modified`:**
   - `apply_lifecycle.rs`: the grandchild start gets a ParentRef, because a parentless start no longer writes a row.
   - `children_derive.rs`: the new `ChildCtx::child_run` field, and an unresolvable detached child now gets `unknown_workflow`.
   - New shared helpers `tests/children_common/mod.rs` (partition) and `tests/children_fx/mod.rs` (server).
5. **rebuild_digest generators not extended.** `rebuild_digest.rs` and `tests/common` are outside `files_modified`. `PROPTEST_CASES=128 rebuild_digest` stays green. The children workload is covered by the live == rebuilt Node test in `children_chain.rs` instead.
6. **W-b test uses a synthetic journal** (`RunTerminated` at an old ts). A really terminated parent with a short retention would be expired by its own EXPIRE before it could resume, so W-b is a safety net for the gap between the due time and the timer firing.
7. **`child::start_child_after_flush` also fires for batches whose child target was unresolvable**, which is what the W1 deploy-between test needs: a crash after the flush but before `unknown_workflow` is journaled.

## Open issues / notes

- `chain_done` is a reserved CancelRun reason by convention only. A user cancel with exactly that reason cascades to children as `chain_done` and they drop it.
- A duplicate StartChild forwarded to an expired successor (or re-sent to an expired child) creates a fresh run with that id. This is the same class as D-P9-24's "child may have expired" limit.
- A cancelled attached child op (OpCancelled) keeps its children row. The child's later completion is journaled for an op the engine already resolved as `Err(Cancelled)` (first completion wins in the engine). No apply-side row removal exists without a new intent.
- `children_failpoints` is not part of ci-local's "failpoint suites" step: `FAILPOINT_CRATES` restricts ostro-server to `failpoints_phase5`, and ci-local.sh/ci.yml belong to 09-02/09-17. 09-17 should add `ostro-server:children_failpoints` to both lists. Until then, run it by hand with `cargo test -p ostro-server --features failpoints --test children_failpoints -- --test-threads=1`.
- docs/storage-format.md was not touched (not in `files_modified`). 09-20 docs should mention the row guard, the detached-row rule and the deferred-expiry mark next to §18.

## Self-Check: PASSED

- Files: every created file listed in key-files exists on main.
- Commits on main: `d3f5d2d`, `e8e7e3d`, `62bc4d2`.
