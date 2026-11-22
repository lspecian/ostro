---
phase: 11-run-operations-embedded-ui
plan: 13
subsystem: server bulk jobs (registry, runner, controllers)
tags: [bulk, OPS-03, gcra, backpressure, checkpoints, failpoints]
requires:
  - 11-04 (BulkRegistry::new, services/bulk.rs + p11_bulk.rs stubs, [bulk] config)
  - 11-09 (REST/gRPC bulk routes, bulk_too_many_jobs 429, validation)
  - 11-10 (p11_rerun::rerun_run, StartRun rate-limit admission)
  - 11-11 (p11_search::list_runs_indexed, scan path)
  - 11-12 (p11_dlq::requeue_dlq, dlq filter, dlq_row)
provides:
  - services::bulk::{BulkRegistry::{create, list, get, cancel, resume_all, bulk_dir, job_path}, JobFile, Pacer, signal_request_id, dedupe_key, action_name, spawn}
  - control::p11_bulk::{create_bulk, list_bulk, get_bulk, cancel_bulk} (real)
  - failpoints bulk::after_op, bulk::before_checkpoint
  - metrics ostro_bulk_ops_total{action,result}, ostro_bulk_jobs{state}
affects: [11-14 (CLI bulk verbs), 11-16 (UI bulk pages now hit a real server), 11-18 (FAILPOINT_CRATES, docs)]
tech-stack:
  added: []
  patterns:
    - "private per-job GCRA (flow::limits::Gcra) driven by the injected clock; sleeps on the runtime timer"
    - "pinned cursor mode + persisted processed set: resume never re-applies an op"
key-files:
  created:
    - crates/ostro-server/tests/bulk_ops.rs
  modified:
    - crates/ostro-server/src/services/bulk.rs
    - crates/ostro-server/src/control/p11_bulk.rs
    - crates/ostro-server/tests/p11_contract.rs
decisions:
  - "Cursor mode is index (newest first via list_runs_indexed) when every partition's run index is Ready at creation, else scan (control-plane list_runs, oldest first). Stored as cursor_mode 'index' | 'scan'; the prefix is a pure function of the stored filter, so it is not stored separately. An index-pinned job that finds an index Rebuilding (or gets a c1. fallback token) waits 250 ms and retries, never switching"
  - "Retry = resume for resumable runs (natural idempotency: a resumed run is not terminal -> 409 -> skipped), restart with dedupe __bulk/<job>/<run> for non-resumable ones. The restart is a copy of p11_dlq's restart_with_key (private there) plus Phase 7 StartRun admission, so a queue rate limit answers 429 and the runner waits retry_after_ms"
  - "Rerun passes idempotency_key __bulk/<job>/<run>; 11-10 wraps it to __rerun/<run>/__bulk/<job>/<run>. Lineage exclusion matches __bulk/<job>/ anywhere in the dedupe key, or retry_of in the processed set"
  - "Outcome classes: 409 (run no longer eligible: terminal, not a DLQ member, not terminal for rerun) and 404 -> skipped; 409 rerun_cut_unsafe / rerun_nondeterministic and every other error -> failed (first 100 reasons kept); 429 -> wait retry_after_ms and retry the same run (ostro_bulk_ops_total{result=rate_limited}); 503 -> retried up to 20 times at 250 ms. Cancel with already_requested -> skipped; signal duplicate -> done"
  - "bulk_too_many_matches is 400 with details {reason: bulk_too_many_matches, limit: bulk_matches, max, actual} where actual = max + 1 (counting stops there); a dry run reports it with the same 400"
  - "max_in_flight: request value (default [bulk] max_in_flight) clamped to 1..=64; rate_per_sec 0 is 400, above max_rate is capped"
  - "Crash simulation is in-process (D-P11-32 alternative, as 11-07/11-12): a firing bulk:: failpoint stops the runner at once without writing; the test then restarts the server on the same data dir. No spawned binary, so target/failpoints is not built"
  - "Signal request id over the 64-byte cap = 'blk~' + first 60 hex digits of sha256('<job>/<run>') (job ids are 40 bytes, so real ids always take this form)"
metrics:
  duration: "~1h15m"
  completed: 2026-11-22
---

# Phase 11 Plan 13: Bulk operations Summary

`POST /v1/bulk` now runs real node-local jobs. The selection has a snapshot bound and a pinned cursor. Each op is paced by a private Phase 7 GCRA and limited by `max_in_flight`. A job pauses while the target partition's ready backlog is above `backlog_high`, and it waits out 429s. Each run gets at most one effect, and jobs checkpoint to `<data>/bulk/<job>.json` and resume after a restart or crash without duplicating an effect.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | d5b023c | `feat(11-13)`: bulk operations with snapshot bound, pacing and backpressure (registry, runner, checkpoint/resume code, controllers, Task 1 tests, p11_contract update) |
| 2 | 1b8d1fb | `feat(11-13)`: bulk job checkpoints and crash resume (Task 2 tests: cadence/atomicity, processed set, restart listing, failpoint crash tests) |

The runner's checkpoint and resume code is one piece with the rest of `services/bulk.rs`, so it landed with Task 1. The Task 2 commit adds the tests that prove it.

## Job file format (`version: 1`)

```json
{
  "version": 1,
  "spec": { "action": ..., "filter": ..., "rate_per_sec": 20, "max_in_flight": 4, "dry_run": false },
  "job_id": "blk_<uuidv7>",
  "created_at_ms": 0,
  "state": "pending | running | paused_backpressure | done | cancelled | failed",
  "cursor_mode": "index | scan",
  "cursor": "i1.t.<ms>.<run> | c1.<..> | null",
  "pending_in_page": ["<run>", ...],
  "processed": ["<run>", ...],
  "done": 0, "skipped": 0, "failed": [{"run_id": "", "reason": ""}], "failed_count": 0,
  "matched": 0, "rate_per_sec": 20, "max_in_flight": 4,
  "selection_done": false, "started_at_ms": null, "finished_at_ms": null
}
```

- **Write:** `<job>.json.tmp`, then write, `fsync`, `rename`, and an `fsync` of the directory. Leftover `.tmp` files are removed at reload.
- **When it is written:**
  - at creation, synchronously, before the 201;
  - at runner start;
  - on state transitions and on cancel;
  - while running, every `checkpoint_ops` finished ops or every `checkpoint_every` when something changed;
  - at the final state.
- **`pending_in_page`** = in-flight ids plus queued ids, so in-flight ops are re-checked on resume. The processed set skips ids it already holds.
- **At service start:** `resume_all` reloads every file. Finished jobs are listed. `pending`, `running` and `paused_backpressure` jobs go back to `pending` and get a runner. Files of another version, files that do not parse, and ids without the `blk_` prefix are ignored with a WARN.

## Pacing measurements

- `Pacer` (rate 10/s, burst 2) over a `ManualClock`, 40 ops: starts at 0, 0, 100, 200, ... 3,800 ms. Each gap after the burst is exactly 100 ms, and the last start is at **3,800 ms** logical.
- 429 path: 6 restarts on a queue with `rate_limit {2/s, burst 1}` and a job rate of 100/s take **2.54 s**. No failures, and `ostro_bulk_ops_total{action="retry",result="rate_limited"}` ≥ 1.
- The tests use a `ManualClock`, not paused tokio time: ostro-server has no tokio `test-util`, and only 11-03 edits Cargo files.

## Backlog accessor

`query::ready_entries(reader, backlog_high + 1).len() > backlog_high` on the target run's partition. These are the store `ready` rows that 11-11's `/v1/queues` and `/v1/partitions` count. A reading is cached for 250 ms per partition. While the backlog is over the limit the job is `paused_backpressure` (checkpointed) and re-checks every 250 ms. Test: P=1, `backlog_high = 5`, 10 ready runs on a paused queue → the job stays paused with 0 ops for over 500 ms; after the queue is resumed it completes 3 restarts.

## Crash-test timings (`--features failpoints --test-threads=1`, in-process)

| Test | Crash phase | Restart → done |
|---|---|---|
| `crash_after_op_...`: 60 cancels, `bulk::after_op` = `25*off->return`, checkpoint every 10 ops (crash at op 26, last checkpoint at 20) | 688 ms | 421 ms |
| `crash_before_checkpoint_...`: 12 restarts, `bulk::before_checkpoint` = `1*off->return`, checkpoint every 3 ops (crash after 3 ops, nothing checkpointed) | 549 ms | 302 ms |

The results:
- **after_op:** `done + skipped == 60`, and `CancelRequested` is journaled exactly once per run.
- **before_checkpoint:** every source run has exactly one restarted run, with dedupe `__bulk/<job>/<run>`.

## Tests

`tests/bulk_ops.rs` has 13 tests, 2 of them gated on `failpoints`:
- **dry run:** 30 matched, 20 samples from the set, no job, nothing changed;
- **cancel:** cancel 30 → done 30, and the same filter again → skipped 30;
- **list and file:** list order, 404 for an unknown job, the job file contents, the metrics;
- **snapshot bound:** a rerun over a tag filter → exactly 100 runs carry the tag. Each new run reruns one source, has key `__rerun/<src>/__bulk/<job>/<src>` and completes on v2;
- **pacing;** signal id cap; backpressure; 429 restarts;
- **signal:** a re-send with `<job>/<run>`'s id is `duplicate: true`;
- **requeue** writes Requeued marks and the sources leave the DLQ; pause/resume sets and clears the pause mark;
- **limits:** 101 matches > 100 gives 400 for both dry and real runs; a 5th job gets 429 `bulk_too_many_jobs` with `retry_after_ms`; eta is shown while running; after cancel no op starts for 1.5 s; after the cancels a new job fits;
- **checkpoints:** 70k reads of the job file in a loop never see a partial file. The running checkpoints show progress {0, 10, 20, 30, 40} with `checkpoint_ops = 10`;
- **processed set:** after a graceful shutdown the file is edited to re-deliver every run (cursor null). The resumed job ends with exactly 20 done and 0 skipped, so no second op ran;
- **restart listing:** after another restart the done job is still listed, and a `version: 2` file is ignored;
- **crash tests** (above).

Results:
- `cargo test -p ostro-server --test bulk_ops --locked`: 11 passed.
- `--features failpoints -- --test-threads=1 crash`: 2 passed.
- `cargo clippy -p ostro-server --all-targets -D warnings`: clean, with and without `failpoints`.
- Full `cargo test -p ostro-server --locked --no-fail-fast` (on 64a1594 + these changes): 66 test binaries, 559 passed, 0 failed. `cargo check --workspace --all-targets --locked`: clean after rebasing onto d28e7cb (11-14 bulk CLI).
- `--features failpoints -- --test-threads=1` (whole `bulk_ops`): 13 passed.
- No new `SystemTime::now` / `Instant::now` / `SystemClock` in `services/bulk.rs` or `control/p11_bulk.rs`.

## Deviations from Plan

1. **[Rule 3] `tests/p11_contract.rs` updated** (outside `files_modified`). It asserted that `list_bulk` was still the 11-13 stub; it now expects an empty job list.
2. **[Rule 3] Restart with a dedupe key is copied, not shared.** `p11_dlq::restart_with_key` is private and `p11_dlq.rs` is not in this plan's files. The bulk copy also adds the StartRun rate-limit admission, which the 429 behaviour needs.
3. **Crash tests run in-process** (D-P11-32 alternative, like 11-07/11-12). `cargo build -p ostro --features failpoints --target-dir target/failpoints` was not run, because nothing spawns the binary.
4. **Pacing test uses `ManualClock`, not paused tokio time** (no tokio `test-util` in ostro-server).
5. **Commit split:** the checkpoint and resume code is in the Task 1 commit (one runner); Task 2 adds the proving tests.
6. **Workflow per the executor brief:** worktree `wt-11-13`, slot-2 target, and CAS `update-ref`, instead of `/tmp/ostro-wt-11-13` with a private index.

## Open items

- **11-18:** add `ostro-server:bulk_ops` (features `failpoints`, `--test-threads=1`, filter `crash`) to `FAILPOINT_CRATES` in `scripts/ci-local.sh` and to the ci.yml "Failpoint suites" step.
- **Published limits:** the bulk caps (`[bulk] max_jobs` / `max_matches` / `max_rate`) are still not in `docs/limits.md` or `/v1/limits`. The plan does not call them published limits; adding them means changing the doc and the endpoint together (`limits_doc` enforces this).
- **Phase 13:** jobs are node-local. Cluster forwarding of `/v1/bulk` belongs to Phase 13 (D-P11-20).
- **Memory:** for very large jobs (100k runs) the processed set is rewritten in full at every checkpoint (several MB per second). That is acceptable at the caps, but compaction could come later.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/services/bulk.rs, crates/ostro-server/src/control/p11_bulk.rs, crates/ostro-server/tests/bulk_ops.rs
- FOUND: d5b023c, 1b8d1fb on main
