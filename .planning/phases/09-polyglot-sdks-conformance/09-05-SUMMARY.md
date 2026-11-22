---
phase: 09-polyglot-sdks-conformance
plan: 05
subsystem: children
tags: [children, wf-06, derive, sink, apply, pf-f1, d21, wave-2]
requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "PF-F1 CompleteRun.parent_event (5) / outbox ChildCompleted.event (6), 05-02 journal_encoded/journal_input, 05-11 ModuleResolver/PolicyResolver"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-14 sink shape (bridge, unsupported_input returning None for bridged effects)"
provides:
  - "crates/ostro-server/src/children.rs: child_run_id (D-P9-7), resolve_child/ChildTarget (D-P9-8), child_start/ChildParts, parent_event_bytes/parent_outcome (PF-F1), plan_child_repairs/ChildRepair (W1, D-P9-25; pub(crate) for 09-08), unknown_workflow_completion"
  - "derive::derive_with_children + ChildCtx (prepared child StartRuns, child parent_event), DeriveError::UnexpectedParentEvent, ModuleResolver::default_queue (provided method; MetaModules overrides)"
  - "sink: child starts resolved/prepared before the append, unknown_workflow after the flush, parent_event on a child's guest terminal, child repair on journal load; no more unsupported children"
  - "Derived intent ChildJournal = tag 19 (P9 fallback slot) + CHILD_ROW_JOURNAL; parent apply journals ChildCompleted (verbatim event or apply-encoded hard stop) for marked children rows"
affects: [09-08, 09-13, 09-14, 09-15, 09-18, 09-20]
tech-stack:
  added: []
  patterns:
    - "Opt-in format extension: a new intent marks a state row; apply branches on the row so v0.1 commands keep their semantics"
    - "derive stays pure: the sink pre-resolves children (resolver, prepare_payload, getrandom) and passes lookups"
key-files:
  created:
    - crates/ostro-server/src/children.rs
    - crates/ostro-server/tests/children_derive.rs
    - crates/ostro-partition/tests/children_apply.rs
  modified:
    - crates/ostro-types/src/command.rs
    - crates/ostro-partition/src/apply/inbound.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/lifecycle.rs
    - crates/ostro-partition/examples/gen_format_corpus.rs
    - crates/ostro-server/src/derive.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/tests/sink.rs
    - crates/ostro-server/tests/derive_phase5.rs
    - crates/ostro-server/tests/effects_seam.rs
    - .planning/RESERVATIONS.md
    - .planning/phases/09-polyglot-sdks-conformance/09-DECISIONS.md
    - docs/storage-format.md
key-decisions:
  - "D-P9-31 (coordinator decision, option (a)): child-completion journaling is opt-in via Derived intent ChildJournal = tag 19 (P9 fallback slot) marking the parent's children row; unmarked rows keep the v0.1 inbox row, so format v0.1 stays frozen and the corpus rebuilds unchanged"
  - "Apply-encoded hard-stop events use ts = the parent's created_at_ms (the inbound message has no time; same rule as inbound CancelRequested)"
  - "Child workflow and queue names are qualified with the parent's tenant (D23); queue = options, else the version's default queue, else \"default\"; run_timeout = options, else the version's describe() default"
  - "Child StartRun inherits tenant, traceparent and priority; retention / schedule-to-start come from the PolicyResolver"
  - "Repair skips detached children (09-08 owns their completion); an unreadable children row is treated as present (never re-decide on doubt)"
requirements-completed: []
duration: ~3h (incl. two long partition suite runs)
completed: 2026-11-22
---

# Phase 9 Plan 05: Children 1/2 (server bridge, derive + apply) Summary

**Deterministic child starts (sha256 child ids, ModuleResolver target resolution), `unknown_workflow` instead of `unsupported`, the child's PF-F1 `parent_event`, and parent-side kind-67 journaling behind an opt-in `ChildJournal` marker (intent 19) so storage format v0.1 stays frozen.**

## Performance

- Tasks: 2/2
- Commits: Task 1 `aa1f97d`, Task 2 `48dc10a`, docs commit last

## What was built

### Task 1 (partition): opt-in marker + parent journaling (`aa1f97d`)

- **Freeze conflict and decision (D-P9-31).** The plan as written (journal every inbound `ChildCompleted` as an event) would change the apply semantics of commands in the frozen v0.1 corpus (`InboundBatch.ChildCompleted` with inline and blob `event`, applied as legacy inbox rows). Coordinator decision (option (a)): journaling is opt-in. New `Derived` intent **`ChildJournal{child_run bytes 1}` = tag 19** (P9 fallback intent slot; RESERVATIONS §3 row flipped to `exists (P9, 09-05)`, 20–22 stay reserved). It must directly follow the `SendMessage(StartChild)` of the same child in the same batch (else `Invalid(BadIntent)`), and is also honoured in a continue-as-new batch (`lifecycle.rs` arm). Apply rewrites that children row from empty to `CHILD_ROW_JOURNAL` (`[1]`).
- `apply/inbound.rs::child_completed`: one extra point read (the children row). Marked row → `journal_encoded(kind 67, event verbatim, wake)` when `event` is present, else `journal_input` of an apply-encoded `ChildCompleted{op, Err(failure)}` (Terminated → non-retryable `child_terminated` "child run was terminated"; TimedOut → `Timeout`; Cancelled → `Cancelled`; Completed/Failed (09-08 re-completion) → `child_terminated` "child completed while the parent was terminal"; anything else → `child_terminated`). **Hard-stop ts choice:** the parent's `created_at_ms` (the message has no time; same as inbound CancelRequested). Unmarked/missing row → the unchanged legacy `InboxMsg::ChildCompleted` row. In both cases the row is removed and `rec.children` decremented; terminal/unknown parents are per-message drops.
- **Fate of `InboxMsg::ChildCompleted`:** still written, but only for unmarked rows (v0.1 commands); never written for Phase 9 children. The variant stays as is (no `#[doc(hidden)]`), since it remains a live v0.1 path.
- The `event` size check stays at its source (`any_oversize` on the child's `CompleteRun.parent_event`); `check_msg` was not tightened, because that would change InboundBatch validation for v0.1 commands.
- docs/storage-format.md §18 documents the addition.

### Task 2 (server): children.rs, derive, sink (`48dc10a`)

- **`child_run_id` golden vectors** (parent = partition 3 ‖ 16 × `0x11`):
  - op 7, P 16 → `0071t7sbx8jw72bzhfyq2k5jaj6zw`
  - op 8, P 16 → `002yhpcepx40309vk1cdaptr0q16j`
  - op 7, P 1 → `0001t7sbx8jw72bzhfyq2k5jaj6zw`
  - uuid version nibble 8 and variant `10` checked for several (op, P) pairs.
- **How intents receives prepared children:** `derive::derive_with_children(events, DeriveCtx { prior, result, successor }, ChildCtx { starts: &dyn Fn(u64) -> Option<StartRun>, parent_event: Option<PayloadRef> }) -> Result<Vec<Derived>, DeriveError>`. `derive`/`intents` keep their signatures and call it with no children. A `ChildStarted` at `s` with a prepared start maps to `SendMessage(StartChild)` plus `ChildJournal` when attached; with no prepared start it maps to nothing. `parent_event` is set on `CompleteRun` for `RunCompleted`/`RunFailed` only; passing one for a continue-as-new or a non-terminal batch is `DeriveError::UnexpectedParentEvent`.
- **Sink:** `prepare_children` resolves every `ChildStarted` (`resolve_child` through the configured `ModuleResolver`), prepares the input (`prepare_payload`, blob > 256 KiB), draws a fresh `getrandom` seed and builds the `StartRun` (`child_start`). Unresolvable targets become `ChildCompleted{s, Err(non-retryable unknown_workflow)}`, journaled right after the flush through the old post-flush path (skipped in terminal batches). `parent_event`: when `RunRecord.parent` (read at construction) is set and the batch ends with a guest terminal, the sink builds `parent_event_bytes(op, parent_outcome(ev), ev.ts_nanos)` and prepares it (300 KiB → blob). `unsupported_completion` no longer has a `ChildStarted` arm.
- **Repair (W1, D-P9-25):** `repair_unsupported` → `repair_children` → `plan_child_repairs(run, P, tenant, journal, has_row, resolver)`. A children row means skip. With no row, a target that resolves now gets `Resend`, submitted as an intents-only `EventsAppended` (`events = []`, `SendMessage(StartChild)` + `ChildJournal`). A target that still cannot be resolved gets `UnknownWorkflow`, journaled once. The plan is empty for terminal journals, and detached children are skipped (09-08).

## Tests

- `crates/ostro-partition/tests/children_apply.rs` (10): intent tag-19 encoding pin (`9a0100`); marker placement rules; Running parent: verbatim event, `InputAppended`, `next_seq + 1`, row removed, counter −1, no inbox row; Scheduled → Ready + `EnqueueReady`; Ready: event only; 1 MiB blob `PayloadRef` journaled byte-for-byte (no blob store in the harness, so no fetch was possible); event-less mapping for all 5 statuses, decoded with the journal codec, with ts = `created_at_ms`; unmarked row keeps the legacy inbox row; terminal/unknown parent drop consumes the seq, and a redelivered seq is `Duplicate`; end to end on one partition (child `CompleteRun.parent_event` → outbox → parent journal).
- `crates/ostro-server/tests/children_derive.rs` (12): golden vectors; pure derive (start + marker, detached without marker, unresolved → nothing); parent_event on guest terminals only and the CAN error; Node-backed sink tests with a fake resolver: attached start (qualified workflow, active version, module, default queue, ParentRef, tenant, traceparent, priority, fresh seed, options run timeout, input bytes), crash-style re-derive (a fresh sink re-appends → `Conflict`; a re-delivered start with the same id → `Duplicate`, exactly one child), explicit version/queue + detached, unknown workflow/version → `unknown_workflow` after the flush, Completed → `Ok(Child)` in the parent journal with the child's terminal ts, Failed → `Err(failure)`, guest-cancelled → `Err(Cancelled)`, 300 KiB output → blob parent event with bytes equal to `parent_event_bytes`, continue-as-new child → no ChildCompleted and the successor carries the parent link, repair re-send (intents-only append leaves `next_seq` unchanged, row re-inserted and marked, second load does nothing), repair `unknown_workflow` exactly once (detached skipped).
- **Existing tests updated deliberately** (children are no longer `unsupported`; without a resolver they are now `unknown_workflow`):
  - `ostro-server/tests/sink.rs`: `child_gets_unsupported_completion_input` → `child_gets_unknown_workflow_completion_input`; `bridge_repair_still_completes_children` → `bridge_repair_completes_unresolvable_children`.
  - `ostro-server/tests/derive_phase5.rs`: `effects_and_children_still_complete_unsupported` → `effects_unsupported_and_unresolvable_children_complete`.
  - `ostro-server/tests/effects_seam.rs`: `children_still_unsupported` → `unresolvable_child_completes_unknown_workflow` (wit-surface starts the undeployed `child-wf`).
  - No partition test expected an inbox row for a marked child, so no partition test changed. The v0.1 inbox path is untouched.
- Results: `cargo test -p ostro-partition -p ostro-types` all 50 binaries green (format_corpus and trim_safety included); `PROPTEST_CASES=128 rebuild_digest` 7/7 + `format_corpus` 2/2; `cargo clippy -p ostro-partition -p ostro-types --all-targets -D warnings` clean; `cargo test -p ostro-server --no-fail-fast` all 52 binaries green (464 tests, children_derive 12/12); on main at `48dc10a`: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -D warnings` and `cargo clippy -p ostro-partition -p ostro-server --features failpoints --all-targets -D warnings` clean.

## Deviations from Plan

1. **[Rule 4, decided by the coordinator] Opt-in marker (intent 19)** instead of unconditional journaling. This adds one ordinal, where the plan said "no ordinal". Extra files beyond `files_modified`: `ostro-types/src/command.rs` (variant + message + const), `apply/run.rs` (validation + `mark_child_journal`), `apply/lifecycle.rs` (one CAN arm), `examples/gen_format_corpus.rs` (exhaustive `intent_name` arm; the corpus itself is not regenerated), `docs/storage-format.md` §18, RESERVATIONS, and the 09-DECISIONS row D-P9-31.
2. **[Rule 3] `ModuleResolver::default_queue`**: a provided trait method returning `None` by default, overridden by `MetaModules`. It is needed for D-P9-8's default queue, and it leaves the 5 `ResolvedWorkflow` literal sites unchanged.
3. **[Rule 3] Existing server tests** expecting `unsupported` children were updated (listed above). These are test files outside `files_modified`.
4. **[Scope] `check_msg` not tightened** for `event` (see Task 1). This keeps v0.1 validation unchanged.
5. **`scripts/ci-local.sh --no-musl` not run.** It builds `target/guests` and `target/order-saga` inside the worktree (extra target dirs, against the disk-budget brief). Workspace fmt/clippy (plus the failpoints feature) and the full partition/types/server suites stand in for it. The coordinator or 09-20 gate should run ci-local.
6. TDD order: the partition tests were written together with the implementation. Their first run failed (the missing oneof tag 19 in `#[prost(oneof … tags)]`), which was the RED step.

## Open issues / notes for 09-08

- Re-sends (resume-after-close, D-P9-24) must keep emitting `ChildJournal` after each attached `StartChild`. The children-row guard and chain matching (D-P9-27) should read the row value: only `[1]` rows journal.
- With the current `run.rs`, a detached `StartChild` still inserts an (unmarked) children row. 09-08 adds that guard. Until then a detached child's completion never arrives (no ParentRef), so the row only leaks.
- Continue-as-new children: the final `ChildCompleted` of a chain comes from the successor, whose run id differs from the marked row's key, so today it takes the legacy inbox path. 09-08 (D-P9-27) must match the row through `child_run_uuid(parent, op)` and journal it.

## Self-Check: PASSED
