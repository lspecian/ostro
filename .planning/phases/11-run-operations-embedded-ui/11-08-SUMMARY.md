---
phase: 11-run-operations-embedded-ui
plan: 08
subsystem: partition apply / store values / storage format doc
tags: [dlq, OPS-04, episodes, rebuild_digest, storage-format]
requires:
  - 11-02 (Cmd 45 DlqUpdate contract, InvalidCode 30 BadDlqUpdate, apply/dlq.rs stub)
  - 11-05 (x_run_index, documented here)
provides:
  - apply_dlq_update (Cmd 45 semantics)
  - store::values::DlqRow (postcard, keyspace 11)
  - docs/storage-format.md "Phase 11 additions"
affects: [11-10, 11-11, 11-12, 11-13, 11-14]
tech-stack:
  added: []
  patterns: ["episode-scoped marks: row reset when the command names a different failure episode"]
key-files:
  created:
    - crates/ostro-partition/tests/apply_dlq.rs
  modified:
    - crates/ostro-partition/src/apply/dlq.rs
    - crates/ostro-partition/src/store/values.rs
    - crates/ostro-partition/tests/rebuild_digest.rs
    - crates/ostro-partition/tests/p11_format.rs
    - crates/ostro-partition/src/apply/mod.rs (comment)
    - crates/ostro-partition/src/store/blobrefs.rs (doc table row)
    - docs/storage-format.md
decisions:
  - "A malformed DlqUpdate run id is Invalid(BadDlqUpdate), not BadRunId (the plan truth wins over the 'record which' option)"
  - "Requeued{resume} must carry an empty new_run_id (else BadDlqUpdate) and stores requeued_to = None"
  - "On a Failed/TimedOut run every op, resume included, must name the current episode; on a live run a resume mark is accepted with any episode"
  - "Episode reset is 'stored episode != command episode' (not '<'): completed_at_ms comes from submitter clocks and need not grow"
  - "Dismissed and Requeued overwrite their fields (last mark wins); HookStarted is set-once (a different hook -> BadDlqUpdate)"
  - "An accepted update that changes nothing writes nothing (0 overlay entries)"
metrics:
  duration: "~1h45m"
  completed: 2026-11-22
---

# Phase 11 Plan 08: DLQ marks and Phase 11 storage format Summary

`Cmd 45 DlqUpdate` now maintains one postcard `DlqRow` per run in the `dlq` keyspace (11). Marks are scoped to one failure episode and re-applying one is idempotent. Malformed or out-of-state updates are rejected deterministically. `docs/storage-format.md` has the single "Phase 11 additions" section: Cmd 45 + DlqRow, StartRun 40/41, JournalEvent.4, x_run_index as landed by 11-05, and the `__ostro/` system tags. It was appended after the frozen text, with no frozen line edited.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | b31fa68 | `feat(11-08)`: DlqUpdate apply and DlqRow marks (apply, value, tests, generator) |
| 2 | 0021b46 | `docs(11-08)`: storage format Phase 11 additions |

## Rejection mapping

| Case | Outcome |
|------|---------|
| run id not 18 bytes | `Invalid(BadDlqUpdate)` |
| run on another partition | `WrongPartition` |
| `op` missing | `Invalid(BadDlqUpdate)` |
| dismiss reason > 1024 bytes | `Invalid(BadDlqUpdate)` (1024 accepted) |
| hook run id not 18 bytes | `Invalid(BadDlqUpdate)` |
| requeue mode > 2 | `Invalid(BadDlqUpdate)` |
| restart/rerun `new_run_id` not 18 bytes | `Invalid(BadDlqUpdate)` |
| resume with non-empty `new_run_id` | `Invalid(BadDlqUpdate)` |
| unknown run | `RunNotFound` |
| HookStarted / Dismissed / Requeued{1,2} on a run not Failed/TimedOut | `Invalid(BadDlqUpdate)` |
| episode != current `completed_at_ms` on a Failed/TimedOut run | `Invalid(BadDlqUpdate)` |
| second HookStarted with a different hook run | `Invalid(BadDlqUpdate)` |

Read budget: 0 reads when rejected before the run read, 1 when rejected after it, 2 when accepted (run record + dlq row). All rejections fold into the rolling hash as code 26 (Invalid) or their RejectReason code, exactly like other commands. Asserted in `rejections_fold_into_the_rolling_hash_and_write_nothing`.

## Generator weights (rebuild_digest)

- `Op::Dlq(DlqOp)` has weight **1** in `op_strategy`. The existing weights sum to 39 and are unchanged.
- Inside `DlqOp`:
  - `fail`: `Some` 60% (Completed 20% of those, else Failed), so the CompleteRun is submitted first with `completed_at_ms = 1000 + n`, unique per op, which gives every re-failure a new episode.
  - `kind`: uniform over the 3 ops. `mode`: uniform 0..4 (3 is invalid).
  - `episode`: current 6/8, stale 1/8, zero 1/8.
  - `bad`: 15% (17-byte run id, no op, long reason, bad hook/target id).
  - Three possible hook ids, so a conflicting second hook can happen. `twice` 30%, `retry` 30% (RetryRun after the mark).
- `fixed_scenario_dlq`: 24 runs, all three ops each, mixed valid/stale/malformed, resumes and second failure episodes, one restart. It asserts at least 10 accepted DlqUpdates (a process-wide counter) on top of live == rebuilt.

## Deviations from Plan

**1. [Rule 3 - Blocking] `tests/p11_format.rs` updated** (not in `files_modified`). Its 11-02 test asserted the `Invalid(NotImplemented)` stub. It now asserts the routing reaches `apply_dlq_update`: a non-failed run gives `BadDlqUpdate`, an unknown run gives `RunNotFound`, and in both cases there is 1 read, 0 writes and the hash fold is correct.

**2. Comment-only edits outside `files_modified`:** the `apply/mod.rs` routing comment (stub → semantics), and the `store/blobrefs.rs` doc table row for `dlq` ("no rows written" → postcard DlqRow, no refs).

**3. Workflow per the executor brief**, overriding the plan's `/tmp/ostro-wt-11-08` and private index: a scratch worktree detached at main, build slot `target/slot-2`, and main moved by CAS. Rebased onto 11-05/11-09/09-19/eng2-mem (clean) and then onto 11-07/11-11/09-20: a textual conflict with 09-20's §18.1 in docs/storage-format.md was resolved by keeping both (my section after §18.1); the StartRun 40/41 text was aligned with the landed apply/rerun.rs (field 40 non-empty with 41 = 0 is rejected; prefix events need a payload). The x_run_index doc section was written from the landed `store/derived.rs` and 11-05-SUMMARY, at the coordinator's request.

**4.** The section heading is `## Phase 11 additions (post-freeze, additive)` without a § number, as the plan's verify grep requires. The frozen title line (line 1) was not edited.

**5.** `ostro-types/src/command.rs` module doc still says "Apply rejects it with Invalid(NotImplemented) until 11-08". That is historical and accurate, and the file is not owned here, so it was left as is.

## Tests

- Pre-rebase (base 34807f2): `cargo test -p ostro-partition --locked --no-fail-fast` exit 0, 47 test binaries ok, 441 passed, 0 failed. `cargo clippy -p ostro-partition --all-targets -- -D warnings` and the same with `--features failpoints`: clean.
- Post-rebase (onto 762bbc5, after 11-05/11-09/09-19; re-checked on d35e6cb after 11-07/11-11/09-20: workspace check, both clippy runs, apply_dlq, p11_format, format_corpus, rebuild_digest fixed scenarios 5/5): `cargo check --workspace --all-targets --locked` clean (no warnings); both partition clippy runs (plain and failpoints) clean; `cargo fmt --check -p ostro-partition` clean. Targeted: `apply_dlq` 7 passed, `p11_format` 2, `apply_retention` 5, `format_corpus` 2 (v0.1 corpus rebuilds unchanged), `rebuild_digest` 8 (incl. `fixed_scenario_dlq` and the proptest with the DlqUpdate generator, 480 s), `run_index` 14.
- `docs/storage-format.md`: `git diff -U0` has no removed lines (frozen text untouched); the plan's grep checks (`## Phase 11 additions`, `x_run_index`, `BadRerunPrefix`, `__ostro/rerun_from=`) pass.
- Not run: full `bash scripts/ci-local.sh` (shared machine, several agents building).

## Self-Check: PASSED
