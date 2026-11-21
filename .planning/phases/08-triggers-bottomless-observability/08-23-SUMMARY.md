---
phase: 08-triggers-bottomless-observability
plan: 23
subsystem: bottomless
tags: [gap-closure, sc3, back-02, rpo, uploader, parallel, cadence, webhooks]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: "08-13 uploader; 08-18 restore; 08-20 restore_rpo; 08-15 webhooks controller; 08-VERIFICATION gap (SC3 RPO bound) + webhook_tolerance warning"
provides:
  - "Uploader::run on a fixed seal_interval/2 cadence from round start (UploaderConfig::round_interval)"
  - "bounded parallel partition + blob uploads per round (UploaderConfig::upload_concurrency / concurrency(P), DEFAULT_UPLOAD_CONCURRENCY = 8; [backup] upload_concurrency, 0 = min(P, 8))"
  - "restore_rpo asserts loss_window_ms <= SEAL_MS + LOSS_SLACK_MS (500 ms)"
  - "[triggers] webhook_tolerance wired as the replay-window fallback (control/webhooks.rs effective_tolerance_s)"
  - "restore drains its JoinSets before returning an error"
affects: [08 re-verification, 12 (replication reuses the uploader seam)]

tech-stack:
  added: []
  patterns:
    - "Per-partition work moved into owned JoinSet tasks (PartCtx + PartState in/out) bounded by a Semaphore; the manifest is assembled only after every task joined"
    - "tokio::time::interval_at + MissedTickBehavior::Delay for a start-to-start cadence"

key-files:
  created:
    - .planning/phases/08-triggers-bottomless-observability/08-23-SUMMARY.md
  modified:
    - crates/ostro-backup/src/uploader.rs
    - crates/ostro-backup/src/restore.rs
    - crates/ostro-backup/src/config.rs
    - crates/ostro-backup/src/archive.rs
    - crates/ostro-backup/tests/uploader.rs
    - crates/ostro-backup/tests/common/mod.rs
    - crates/ostro-server/src/config.rs
    - crates/ostro-server/src/services/uploader.rs
    - crates/ostro-server/src/control/webhooks.rs
    - crates/ostro-crashtest/tests/restore_rpo.rs
    - docs/bottomless.md
    - docs/webhooks.md
    - .planning/phases/08-triggers-bottomless-observability/08-DECISIONS.md
    - .planning/phases/08-triggers-bottomless-observability/08-VALIDATION.md

key-decisions:
  - "P8-D23 revised: RPO = end-to-end data-loss window <= seal_interval (SC3/BACK-02 unchanged; option B of 08-VERIFICATION)"
  - "Round cadence = seal_interval / 2 from round start; loss <= seal/2 + upload latency"
  - "upload_concurrency default 0 = auto min(P, 8); blobs bounded the same way and still before any chunk; meta PUT overlaps partitions"
  - "LOSS_SLACK_MS = 500 ms on top of the 1 s seal (client-side ack timestamp, scheduling, manifest fsyncs); not widened for load"
  - "webhook_tolerance applies only to Stripe/Slack/Standard Webhooks with tolerance_s = 0; generic HMAC keeps 0 = no window"

requirements-completed: [BACK-02]

duration: ~75 min
completed: 2026-11-21
---

# Phase 8 Plan 23: RPO gap closure (fixed-cadence, parallel uploader) Summary

**The bottomless uploader now starts a round every `seal_interval / 2` (from round start) and uploads partitions in parallel (default `min(P, 8)`) against one cut. restore_rpo asserts the end-to-end data-loss window. Measured at load 19–33: p50 552 ms / max 1,049 ms at seal 1 s, down from p50 1,680 / max 2,449 ms.**

## Uploader design (08-13 → 08-23)

- **Cadence.** `Uploader::run` uses `tokio::time::interval_at(now + p, p)` with `p = round_interval() = max(seal_interval / 2, 10 ms)` and `MissedTickBehavior::Delay`.
  - A round starts one period after the previous round *started*.
  - An overrunning round is followed at once by the next one.
  - After a failed round the uploader sleeps a back-off (period, doubling, capped at 30 s) and then calls `reset_immediately()`.
  - It stops for good when fenced.
- **Round structure.** The order of steps is unchanged.
  1. Applied pairs, then `log_cut()`, then `cut_at_ms`, then the after-cut hook.
  2. Blobs. Their PUTs now run in a `JoinSet` bounded by `concurrency(P)`, and all of them finish before any chunk.
  3. Partition tasks. `sess.parts` is taken out of the session. Each started partition becomes an owned task (`PartCtx { node, store, max_chunk_bytes }` plus its own `PartState`) that runs steps 3–5 against `cut[p]`: 5a snapshot uploads, B4 chain start, chunks `[uploaded_end, cut_p)`, and 5b B3 snapshot reference. A `Semaphore(concurrency)` bounds the tasks. The meta snapshot is captured after the cut, and its PUT runs through `tokio::join!` alongside the partition tasks.
  4. After every task has joined, the states go back into the session and the outcomes are summed into the report: chunks, bytes, snapshots, `changed`, `snapshot_changed`, and the skipped partitions (sorted).
  5. From step 7 on, the code is untouched: entries from the single cut, `Manifest::validate`, `check_before_manifest` (LOCK), the `upload::before_manifest` failpoint, the If-Match / If-None-Match manifest PUT, the gen manifest, upload floors only after the durable manifest, retention after the manifest, and metrics.
- **Failure.** The first partition error aborts the rest and fails the round. `round()` already drops the session, so the next round re-reads `manifest/current`. Immutable re-puts are idempotent, and nothing is published without a manifest.
- **Bound.** The loss window is at most `seal/2 + U`, so it stays within `seal_interval` while the upload latency `U <= seal/2`.

## Measurements

**restore_rpo** (nextest crash profile, release failpoint binary, P=16, seal 1 s, 10 kill/restore iterations, `file://` bucket):
- Load: 1-minute average 19.25–33.34 during the run (START 19.25, END 29.97). A 10k crash campaign and Phase 7 work were running at the same time.
- Data-loss window: p50 **552 ms**, max **1,049 ms**. It is asserted to be <= 1,500 ms on every iteration. 9 of 10 iterations were below 1,000 ms.
  - Per iteration: 1049, 449, 552, 769, 445, 454, 294, 261, 1042, 730 ms.
- Cut lag: p50 280 ms, max 412 ms.
- Upload latency of the restored round: p50 217 ms, max 576 ms. Across all 98 sampled rounds: p50 421 ms, max 1,958 ms.
- 18,276 acks; every pre-cut ack was restored. The backup file round trip passed (16 partitions, 1,682 runs, 17.3 s). PASS in 291 s.

**cut_lag_per_tick_at_p16** (08-13 test, LocalFs, seal 250 ms): upload latency p50 225 ms, max 662 ms; max cut lag at manifest time 632 ms. With sequential uploads, 08-13 had measured p50 1.02 s, max 9.73 s.

**run_starts_rounds_on_a_fixed_cadence** (rounds held 150 ms, period 200 ms): the gaps between cut reads were 199–201 ms, with one at 254–273 ms. The old schedule would have given about 550 ms.

## Tasks / commits

1. **d0f13e2** `feat(08-23)`: fixed-cadence parallel uploader, `upload_concurrency` config, restore JoinSet drain, and two new uploader tests.
2. **5ca919d** `fix(08-23)`: `[triggers] webhook_tolerance` fallback with a unit test, plus docs/webhooks.md.
3. **085ce10** `test(08-23)`: restore_rpo asserts the loss window.
4. docs commit: P8-D23 in 08-DECISIONS, docs/bottomless.md, 08-VALIDATION, this SUMMARY, ROADMAP and STATE.

## Verification

- `cargo test -p ostro-backup --features failpoints -- --test-threads=1`, all green:
  - lib 10, chain_start 4, cut_consistency 2, fencing 5 (s3_fencing is a SKIP, human_needed), remote_gc 6, restore 6 (+1 ignored), uploader 10.
- `cargo test -p ostro-server --lib -- webhooks`: 6 passed, including `webhook_tolerance_is_the_fallback_for_timestamped_kinds`.
- `cargo test -p ostro-server --test bottomless_service --test webhooks --test config`: 1 + 5 + 33 passed.
- `cargo nextest run -P crash -p ostro-crashtest --run-ignored only -E 'test(restore_rpo)'`: PASS (see above).
- `cargo clippy -p ostro-server -p ostro-backup -p ostro-crashtest -p ostro --all-targets -- -D warnings`: clean. Also clean: `cargo clippy -p ostro-backup --features failpoints --all-targets -- -D warnings`.
- `cargo fmt --check` on the touched crates: clean. The release failpoint binary built with `--locked`, and no Cargo.toml or Cargo.lock changed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] restore returned while partition/blob tasks were still writing into the data dir**
- **Found during:** Task 1 (`ostro-backup --test restore`).
- **Issue:** `an_interrupted_restore_leaves_a_directory_the_next_restore_refuses` failed twice: `remove_dir_all` returned `DirectoryNotEmpty` after the `restore::mid_partition` error. Returning an error dropped the `JoinSet`. That aborts the async tasks, but their `spawn_blocking` writes kept running. The bug was pre-existing; it passed once on the 08-13 code, and the new uploader changed the fixture's timing enough to expose the race.
- **Fix:** both restore `JoinSet` loops now drain every task before returning the first error. The `mid_partition` failpoint is evaluated through a closure, so the drain still happens.
- **Files modified:** crates/ostro-backup/src/restore.rs
- **Commit:** d0f13e2

**2. [Rule 2] `[backup] upload_concurrency` config key.** This adds a field to `BackupConfig` and to the server's `BackupSection`, with default 0 = auto. The brief asked for "a bounded concurrency setting", and this is that setting.

## Follow-ups

- The quiet-machine re-run of restore_rpo is listed in 08-VALIDATION (Human-needed #7). It passed under load, so this only confirms the numbers.
- PUT cost: a busy partition can now seal two chunks per `seal_interval`. The note is in the docs/bottomless.md Cost section.
- 08-VERIFICATION.md was not edited. The verifier should re-run for the SC3 gap.

## Self-Check: PASSED
