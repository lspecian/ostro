---
phase: 11-run-operations-embedded-ui
plan: 07
subsystem: partition apply / server executor + sink / guests
tags: [rerun, OPS-02, UI-01, cut-safety, re-arm, replay-markers, failpoints]
requires:
  - 11-02 (StartRun 40/41, JournalEvent.4, InvalidCode 29, apply/rerun.rs stub)
  - 11-05 (index on_start hook in the shared start_run)
  - 11-06 (live-only run limits)
provides:
  - apply rerun path (rerun::check + rerun::journal_prefix) gated on rerun_from_seq != 0
  - ostro_server::rerun::{analyze_cut, safe_cuts, CutPlan, CutUnsafe, StepCut, step_cuts, resolve_step, rerun_from_tag}
  - durable rerun re-arm in the shared executor prelude (embedded + RemoteRunner), failpoint rerun::before_rearm
  - JournalEvent.attempt = epoch << 32 | n on every sink-submitted event
  - guests p11-rerun (pipeline v1) and p11-rerun-v2 (pipeline v2)
affects: [11-10, 11-11, 11-15, 11-16, 11-14]
tech-stack:
  added: []
  patterns: ["validate-before-read for StartRun extensions", "one-pass interval cover for cut safety"]
key-files:
  created:
    - crates/ostro-partition/tests/apply_rerun.rs
    - crates/ostro-server/tests/rerun_rearm.rs
    - guests/p11-rerun/{Cargo.toml,src/lib.rs}
    - guests/p11-rerun-v2/{Cargo.toml,src/lib.rs}
  modified:
    - crates/ostro-partition/src/apply/{rerun,run}.rs
    - crates/ostro-server/src/{rerun,executor,sink}.rs
decisions:
  - "Apply validation (rerun::check) is split from journaling (rerun::journal_prefix): the check runs in start_run's validation block before the dedupe/run point read, so a bad prefix writes nothing; journal_prefix is infallible and returns ()"
  - "The rerun re-arm sits in EngineExecutor::prelude, which the RemoteRunner already calls before Activate, so workers/remote_executor.rs needed no edit (one call site covers both paths)"
  - "Valid cuts are 1 ..= index of the first terminal event (the prefix never contains 26/27/28/71) and <= journal length; cut = terminal index + 1 or beyond is terminal_or_out_of_range"
  - "step_cuts lists step ops (a StepAttemptFailed chain + its StepCompleted is one op, keyed by command hash) and the other scheduling commands (timer, effect, signal wait, child, await token; step None, occurrence 0)"
  - "rerun_from_tag accepts only a single tag with a canonical decimal value (the form apply accepts)"
  - "Kill test runs in-process (D-P11-32 alternative): the failpoint ends the job Released before the re-arm, leaving the run Running as a crash would; then restart on the same data dir"
metrics:
  duration: "~2h15m"
  completed: 2026-11-22
---

# Phase 11 Plan 07: Rerun below the control plane Summary

A StartRun with `rerun_from_seq` (41) != 0 now journals `run_started` plus `rerun_prefix` (40) verbatim as events `0..cut`, sets `next_seq = cut` and `status_seq`, and activates like any other start. The server has a pure cut-safety analyzer with a per-op view. Every activation of a tagged rerun re-arms its open timers and waits until the first live event exists, and that holds across a crash. The sink stamps exact replay markers.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 221e4c6 | `feat(11-07)`: apply verbatim rerun prefixes |
| 2 | 03e1acd | `feat(11-07)`: rerun cut-safety analyzer and rerun guests |
| 3 | 11c0c46 | `feat(11-07)`: durable rerun re-arm and exact replay-marker stamps |

(Hashes as landed on main.)

## run.rs restructuring (shared file)

`crates/ostro-partition/src/apply/run.rs` has two hunks plus one visibility change:
- line ~302 (validation block, before `check_run_started` and before the point read): `if let Err(r) = super::rerun::check(s) { return reject(r); }`. It comes before `check_run_started`, so a rerun with a malformed `run_started` reports `BadRerunPrefix` and not that check's own code.
- line ~385, the journaling step: `if s.rerun_from_seq != 0 { rerun::journal_prefix(..); waits::on_first_events(..)?; } else if let Some(ev) = &s.run_started { <unchanged> }`. A single call line was not possible because the rerun branch replaces the seq-0 journaling instead of adding to it.
- `check_run_started` is now `pub(crate)`, so rerun.rs can reuse it for the seq-0 event.

The 11-05 `index::on_start` hook and everything after it are untouched. A StartRun without 40/41 takes exactly the old path. `format_corpus`, `rebuild_digest`, `apply_run_started`, `p11_format`, `apply_core` and `apply_lifecycle` all pass.

## Executor / sink hunks

- `crates/ostro-server/src/executor.rs:971-975` (line numbers at the worktree base), step 2c in `EngineExecutor::prelude`, after the B2 resume re-arm and before the journal load: `if crate::rerun::prearm_due(record, sink.next_seq()) { crate::rerun::before_rearm()?; self.rearm(handle, run, sink).await?; }`. It reuses the existing `rearm` (journal read, `derive::rearm_unresolved`, zero-event `EventsAppended` at the journal end). Module doc lines 37-39 were updated as well. The 10-06 `// clock-exempt` annotations are intact.
- `workers/remote_executor.rs`: **no edit**. `RemoteRunner` calls the shared `prelude` at line 454, before it sends `Activate`, so worker-queue reruns re-arm through the same line. `rerun_on_a_worker_queue_rearms_through_the_remote_runner` proves it.
- `crates/ostro-server/src/sink.rs:401-417`, in `PartitionSink::encode`: `attempt = (epoch << 32) | n` when `p11_feature_enabled(JournalAttempt)`. Every sink submission goes through `encode`, including the engine appends, the repair or unsupported completions, and the remote path (`remote_executor.rs:717` appends through the same `PartitionSink`). No `JournalEvent` is built anywhere else in the server's execution path. Apply-originated inputs keep 0.

## Analyzer pairing source

The analyzer mirrors `derive::rearm_unresolved`: an op is the command event at seq `op`, and it is closed by the first input with `EventBody::completes_op() == op` or by an `OpCancelled{op}`. A `SignalPayload` at seq `p` is closed by the first `SignalMatched{payload_seq: p}`. An unsafe op (child, effect, await token, unmatched signal) started at `s` and closed at `e` blocks the cuts `s < c <= e`. `safe_cuts` builds a difference array over all cuts in one pass, so `nearest_safe_seq` and `step_cuts` stay O(n) per journal and never become O(n²). The cross-check test runs `rearm_unresolved` on the same prefix and asserts that its intent count equals `open_timers + open_waits`.

## Kill test timings

`crash_before_rearm_still_fires_the_timer_after_restart` (feature `failpoints`, `--test-threads=1`):
- Blocked phase: 2.01 s. This is mostly the deliberate 2 s window that shows a past-due timer did not fire while the re-arm was skipped.
- Restart to completion: 178 ms.
- The whole `rerun_rearm` failpoint binary takes 4.06 s for 3 tests.

## Tests

- `cargo test -p ostro-partition --test apply_rerun`: 6 passed. The tests cover:
  - the verbatim copy of a 6-event source (byte-identical rows, `next_seq 6`, `status_seq Some(4)`, Ready, EnqueueReady first, 11-05 index rows present, attempt stamps kept, rebuild digest equal);
  - cut = 1;
  - blob refs;
  - dedupe returning `ReturnExisting`;
  - 20 invalid cases, each 0 overlay writes and no actions;
  - starts without the fields.
- `cargo test -p ostro-partition --lib --test apply_run_started --test rebuild_digest --test format_corpus --test p11_format --test apply_core --test apply_lifecycle`: all ok. `rebuild_digest` passed 7 tests in 604 s, under shared load.
- `cargo test -p ostro-server --lib -- rerun::`: 9 passed.
- `cargo test -p ostro-server --test rerun_rearm`: 2 passed (embedded and worker queue).
- `cargo test -p ostro-server --features failpoints --test rerun_rearm -- --test-threads=1`: 3 passed.
- Mutation check: with `prearm_due` forced to false, the embedded rerun test times out (the run never completes).
- Guests: `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests -p p11-rerun -p p11-rerun-v2` passes.
- `cargo clippy -p ostro-server -p ostro-partition --all-targets --locked -- -D warnings`: clean, both with and without `--features failpoints`.
- `cargo fmt --check` (both crates and the two guests): clean.
- `cargo test -p ostro-server --locked --no-fail-fast`: exit 0, 59 test binaries ok, 507 passed, 0 failed (includes `sink`, `executor*`, `resume_rearm`, `worker_remote`, `limits_run`, `semantics_e2e`, `p11_contract`; the sink attempt stamp changed no existing assertion).

## Deviations from Plan

**1. [Interface] `journal_prefix` signature.** The plan asked for `journal_prefix(b, run, rec, cmd) -> Result<(), ApplyOutcome>`, called after the RunRecord is built. If it validated at that point, a rejection would come after the dedupe row was already inserted. Validation therefore moved into `rerun::check(&StartRun) -> Result<(), RejectReason>`, which runs in the validation block before any read or write. `journal_prefix(b, run, rec, s)` returns `()`.

**2. [Rule 3] No `rerun::prearm(rec, journal, token, submit)` helper and no `remote_executor.rs` edit.** The embedded executor and the RemoteRunner already share `EngineExecutor::prelude` (07-12). One call there covers both paths and reuses the existing `rearm` (journal load and CAS submit). The shared pieces in `rerun.rs` are `prearm_due` and the `before_rearm` failpoint.

**3. Kill test in-process, not a spawned binary.** D-P11-32 allows "or runs in-process with the failpoints feature". `rerun::before_rearm` set to `return` ends the job `Released` before the re-arm and leaves the run `Running`. The test then restarts the server on the same data dir. `cargo build -p ostro --features failpoints --target-dir target/failpoints` was not run: this saves disk on a shared machine, and nothing in this plan spawns the binary.

**4. "No re-execution" proof.** There is no per-step counter. The rerun's events `0..cut` are asserted byte-identical to the source rows, and the rerun journal holds exactly one `StepCompleted` per step `s1..s5`. A re-executed copied step would show up as a second `StepCompleted` or as a nondeterminism failure.

**5. "A non-rerun run never triggers re-arm"** is covered by the `prearm_due` unit test (no tag, mismatched `next_seq`, `next_seq` 0) and by the source run in the integration test (it has no tag). No runtime counter was added.

**6. Workflow per the executor brief.** The plan's `/tmp/ostro-wt-11-07` and private index were replaced by the brief's worktree, build slot `target/slot-3`, and main moved by CAS.

**7. Intermediate commit warning.** In commit 2, `prearm_due` / `before_rearm` have no caller until commit 3, which adds the executor call site. Only the final tree was checked to be warning-free.

## Open items

- `ostro-types/src/command.rs` line 110 still says "Apply ignores them until 11-07". The file is outside this plan's `files_modified`; 11-08 (docs) or a later contract touch can drop that sentence.
- 11-10 builds the real StartRun: the copied rows, `retry_of`, the tag, `rng_seed` from the source and the target module. `tests/rerun_rearm.rs::submit_rerun` is a working reference.

## Self-Check: PASSED
- apply_rerun.rs, rerun_rearm.rs, guests/p11-rerun{,-v2} exist; 221e4c6, 03e1acd, 11c0c46 on the branch.
