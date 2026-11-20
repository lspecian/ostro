---
phase: 06-effects-bridge-connectors-secrets
plan: 03
subsystem: partition-apply
tags: [effects, apply, keyspace, completion, leader-wake, rebuild-digest]
requires:
  - "06-02 contract: Cmd::CompleteEffect (13, B2 encoded EffectCompleted payload), Intent RegisterEffect/CancelEffect (9/10), InvalidCode::BadEffectCompletion (15), complete_effect_payload"
  - "05-02 journal_encoded (D21), terminal hook; 05-08 CompleteToken shape; 05-09 retention expiry"
provides:
  - "effects rows in keyspace #12: effect_key(run, op) = run ‖ op BE -> EffectRow{kind: u8, scheduled_at_ms: u64}"
  - "apply/effects.rs: check_register_effect, register_effect, cancel_effect, apply_complete_effect, delete_run_effects, wake_effect_runs"
  - "query::effect_row / query::open_effects read helpers (06-14 SendGate)"
  - "P-8 resumable-aware terminal cleanup, P-9 LeaderAnnounced wake"
affects: [06-07, 06-14, 06-16]
tech-stack:
  added: []
  patterns:
    - "row-as-token first-wins completion (same as waits rows for CompleteToken)"
    - "documented read-budget exceptions asserted with the ApplyBatch read counter"
key-files:
  created:
    - crates/ostro-partition/src/apply/effects.rs
    - crates/ostro-partition/tests/apply_effects.rs
    - crates/ostro-partition/tests/common/gen_effects.rs
  modified:
    - crates/ostro-partition/src/apply/mod.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/terminal.rs
    - crates/ostro-partition/src/apply/retention.rs
    - crates/ostro-partition/src/store/keys.rs
    - crates/ostro-partition/src/store/values.rs
    - crates/ostro-partition/src/query.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-partition/tests/rebuild_digest.rs
    - crates/ostro-partition/tests/{apply_core,apply_lifecycle,apply_phase4,apply_seams}.rs
    - docs/storage-format.md
key-decisions:
  - "RegisterEffect for an op >= next_seq is dropped deterministically (mirrors RegisterWait); kind > 2 or a reserved op is BadIntent"
  - "RegisterEffect is a blind insert (0 reads): a repeat carries the same EffectScheduled facts, so the row stays byte-identical"
  - "CompleteEffect: missing outcome -> BadEffectCompletion, oversize inline -> PayloadTooLarge (W5), both before the run read"
  - "LeaderAnnounced wake readies runs at rec.created_at_ms (same as journal_encoded's wake) and skips paused runs (pause wins)"
  - "Non-resumable terminals always pay the 1 effects prefix scan (RunRecord gains no counter field: the freeze forbids new fields)"
metrics:
  duration: "~70 min"
  completed: 2026-11-20
---

# Phase 6 Plan 03: Partition effects apply Summary

**In-flight effects are now durable partition state. `RegisterEffect`/`CancelEffect` write and delete `effects` rows in keyspace #12. `CompleteEffect` is first-wins and attempt-independent: it journals the bridge's encoded `EffectCompleted` (kind 65) verbatim, inline or as a blob ref, deletes the row and wakes the run. Non-resumable terminals and retention expiry clean the rows up; resumable terminals keep them (P-8). An accepted `LeaderAnnounced` wakes suspended runs that have open effects (P-9).**

## Read helpers (for 06-14)

```rust
// crates/ostro-partition/src/query.rs
pub fn effect_row(r: &StoreReader, run: &RunId, op: u64) -> Result<Option<EffectRow>, StoreError>;
pub fn open_effects(r: &StoreReader, run: &RunId) -> Result<Vec<(u64, EffectRow)>, StoreError>; // op order

// crates/ostro-partition/src/store/values.rs
pub struct EffectRow { pub kind: u8 /* EffectKind */, pub scheduled_at_ms: u64 }

// crates/ostro-partition/src/store/keys.rs
pub fn effect_key(run: &RunId, op: u64) -> Vec<u8>;      // run (18 B) ‖ op BE
pub fn effect_run_prefix(run: &RunId) -> Vec<u8>;
pub fn decode_effect_key(k: &[u8]) -> Option<(RunId, u64)>;
```

## Where LeaderAnnounced is applied

The LeaderAnnounced acceptance lives in `crates/ostro-partition/src/apply/mod.rs::dispatch`, in the epoch gate. Once the epoch is accepted (`cur.meta.epoch` and `epoch_history` updated), and before the early `return Ok(Accepted)`, it calls `effects::wake_effect_runs(b, actions)`. A rejected (stale) announcement does nothing: 0 reads, 0 writes (tested).

## Semantics

- **CompleteEffect**, checked in this order:
  - `BadRunId` / `WrongPartition`;
  - no outcome → `Invalid(BadEffectCompletion)`;
  - inline outcome over 256 KiB → `PayloadTooLarge`;
  - `RunNotFound`, then `RunTerminal`;
  - `op >= next_seq` → `Invalid(BadEffectCompletion)`;
  - row present → journal at `next_seq` (kind 65, payload byte-identical), delete the row, wake (`InputAppended` if Running, `EnqueueReady` if Scheduled) → `Accepted`;
  - row absent → `Duplicate` with nothing written. This covers a second completion and a completion after `CancelEffect`.
- **P-8:** `terminal::on_terminal` calls `delete_run_effects` when `!rec.resumable`. Every caller sets `resumable` before the hook:
  - run.rs CompleteRun;
  - lifecycle terminate/timeouts (`true`);
  - continue-as-new (`false`).

  Retention expiry calls `delete_run_effects` (it already scanned `effects` by run prefix).
- **P-9:** a full effects scan in key order. For each distinct run: read its record; if it is `Scheduled` and has no `pause`, apply `make_ready` at `created_at_ms` and store it. With no rows: 1 read, 0 overlay writes, so the state equals what the pre-06-03 code produced (asserted with `ApplyBatch::len() == 0`). The format corpus v0.1 stays green.

## Read counts (asserted in tests/apply_effects.rs)

| Path | Reads |
|---|---|
| RegisterEffect / CancelEffect | 0 beyond EventsAppended (total 1) |
| CompleteEffect (accepted or Duplicate) | 2 |
| CompleteEffect rejected at the record stage | 1 |
| Non-resumable terminal EventsAppended (no waits) | 2 (record + effects scan) |
| Resumable terminal | 1 (no scan) |
| Accepted LeaderAnnounced | 1 + distinct runs with rows |
| Stale LeaderAnnounced | 0 |

## Verification

- `cargo test -p ostro-partition --test apply_effects --locked`: 14 passed.
- `--test format_corpus`: 2 passed (no re-bless). `--test rebuild_digest`: 4 passed (proptest with the effects hook, 3 fixed scenarios including the new `fixed_scenario_effects`; 480 s).
- `cargo test -p ostro-partition --features failpoints --locked --no-fail-fast -- --test-threads=1` (pre-rebase): 32 binaries passed on the first run. The 3 binaries with stale read-count assertions failed (`apply_lifecycle`, `apply_phase4`, `apply_seams`); after updating those assertions they passed (18 + 19 + 9).
- After rebasing onto main b5e3500 (which carries the 08-02 snapshot and 08-08 node changes): clippy `-p ostro-partition --all-targets --features failpoints -D warnings` is clean, `cargo build --workspace --locked` has 0 warnings, and `cargo test -p ostro-partition --locked --no-fail-fast` passed: 33 binaries ok, 299 tests passed, 0 failed.
- Earlier, on base 8e1ba80: clippy `-D warnings` (with and without failpoints) and the workspace build were clean.
- `grep -q "Phase 6 additions" docs/storage-format.md`: ok.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] `apply/run.rs` edited (not in files_modified)**
- 06-02's temporary reject arms for RegisterEffect/CancelEffect are in `run.rs` (`check_intents` and the write loop), as the 06-02 SUMMARY's owner table says.
- Replacing them is this plan's job. The edit is 4 lines, plus the `effects` import.

**2. [Rule 1 - Expected consequence] Four existing read-count assertions updated (test files outside files_modified)**
- Files: `tests/apply_core.rs` (`reads_budget`: `<= 1` → `<= 2`), `tests/apply_lifecycle.rs` (continue-as-new: 1 → 2), `tests/apply_phase4.rs` (`running_row_lifecycle_grant_and_complete`: 1 → 2) and `tests/apply_seams.rs` (`complete_run_without_waits_reads_no_waits`: 1 → 2).
- Each asserted the read count of a non-resumable terminal transition. Those transitions now carry the plan-mandated "+1 effects prefix scan by run" exception.
- The read could not be avoided: skipping it would need a counter in `RunRecord`, and the freeze forbids new fields.
- Each updated assertion carries a comment that cites 06-03/P-8.

**3. storage-format.md section placement**
- The Phase 6 additions are a new **§16**, after the Phase 8 §15, which already sat below the frozen §14.
- The FROZEN v0.1 header and §1–§14 are untouched, including the §6 row for keyspace 12.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| (earlier attempt) | fa2df6a | docs(06-03): flip Phase 6 Cmd 13, Intent 9/10 and InvalidCode 15 to exists |
| 1+2 | dc8c0da | feat(06-03): effects rows, RegisterEffect/CancelEffect, first-wins CompleteEffect, P-8 cleanup and P-9 leader wake |
| 3 | b5e3500 | test(06-03): rebuild_digest effects generator, P-8 survivor invariant, storage-format Phase 6 additions |

Tasks 1 and 2 share `effects.rs`, `mod.rs` and `apply_effects.rs`, so they went into one commit.

## Self-Check: PASSED

- FOUND: crates/ostro-partition/src/apply/effects.rs, tests/apply_effects.rs, tests/common/gen_effects.rs
- FOUND: commits fa2df6a, dc8c0da, b5e3500 on main
