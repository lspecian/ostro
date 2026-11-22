---
phase: 11-run-operations-embedded-ui
plan: 12
subsystem: server controllers + leader services (DLQ, on_failure hooks)
tags: [dlq, on_failure, OPS-04, episodes, ext-tags, failpoints]
requires:
  - 11-04 (P11Ctx, p11_dlq / onfailure / dlq_hook stubs, [dlq] config)
  - 11-05 (x_run_index, query::index_page / index_state)
  - 11-08 (Cmd 45 DlqUpdate apply, DlqRow in keyspace 11, episode rule)
  - 11-09 (REST/gRPC DLQ + failure-policy routes)
  - 11-11 (search calls is_dlq_member; i1.d. DLQ token)
provides:
  - control::p11_dlq::{list_dlq, requeue_dlq, dismiss_dlq, get_failure_policy, set_failure_policy, is_dlq_member, member, current_marks, dlq_row, failure_reason, submit_mark}
  - onfailure::{OnFailure, DlqPolicy, EXT_ON_FAILURE (144), EXT_DLQ_POLICY (145), resolve, dlq_policy, hook_input, hook_dedupe_key, requeue_dedupe_key, is_hook_run}
  - services::dlq_hook::{spawn, drive, catch_up, HookOutcome, CatchUpStats, CATCHUP_BUDGET, REBUILD_FALLBACK}
  - guests/p11-hook (w, w_slow, on_fail, always_fail)
affects: [11-10 (rerun requeue end-to-end), 11-13 (bulk requeue / dlq filter), 11-14 (CLI), 11-16 (UI DLQ page), 11-18 (docs, FAILPOINT_CRATES entry)]
tech-stack:
  added: []
  patterns:
    - "episode-scoped marks: a dlq row counts only while episode_completed_at_ms == RunRecord.completed_at_ms"
    - "two-command idempotent sequences (StartRun with a dedupe key, then DlqUpdate) instead of atomicity"
key-files:
  created:
    - crates/ostro-server/tests/dlq_api.rs
    - guests/p11-hook/Cargo.toml
    - guests/p11-hook/src/lib.rs
  modified:
    - crates/ostro-server/src/control/p11_dlq.rs
    - crates/ostro-server/src/onfailure.rs
    - crates/ostro-server/src/services/dlq_hook.rs
    - crates/ostro-types/src/command.rs (module doc comments only)
decisions:
  - "Membership also requires the queue's DlqPolicy.enabled (ext 145); include_dismissed shows dismissed AND requeued members with their marks"
  - "DLQ order = run index order (created_at_ms desc), the 11-11 'd' token kind; reason filter is a prefix match applied in memory"
  - "Reason = terminal_reason, else '<reason_code>: <message>' from the failure JSON (guest failures are 'workflow_failed: <msg>'); blob results > 64 KiB are not loaded"
  - "Restart requeue dedupe key = __dlq/<run>/<resumed_count + 1>, placed by partition_for_key(workflow, key); once Requeued{restart|rerun} exists for the episode, a repeat returns it without submitting"
  - "Resume requeue writes Requeued{resume} (current episode) before the RetryRun; resume of a non-resumable member is 409 details.reason run_not_resumable"
  - "Hook runs start through the control plane's start_run (D23 qualification, rate limit admission, ReturnExisting dedupe); the hook tag is added server-side"
  - "Catch-up walks the whole Failed/TimedOut index prefixes (budget 100,000 rows per partition) and filters by completed_at_ms, because the index is ordered by created_at_ms, not by failure time"
  - "The 60 s rebuild fallback counts the backoff sleeps it took (no clock read), so a manual test clock cannot stall it"
metrics:
  duration: "~1h30m"
  completed: 2026-11-22
---

# Phase 11 Plan 12: DLQ server and on_failure hook service Summary

Failed and timed-out runs now show up in `GET /v1/dlq` with a reason. They can be dismissed, or requeued by resume, restart or rerun. A per-workflow or per-queue `on_failure` policy, stored in meta ext tags 144/145, makes the leader start exactly one hook run per failure episode. That holds across a crash between the start and the mark, a failing hook never fires another hook, and failures missed while the node was down are re-driven at leader start.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 6b16199 | `feat(11-12)`: DLQ view, requeue and dismiss; on_failure policy ext tags (+ stale command.rs comments) |
| 2 | 57dd1cf | `feat(11-12)`: on_failure hook service with exactly-once start and loop guard (+ guest, tests) |

## Reason extraction

1. If `RunRecord.terminal_reason` is non-empty, it is the reason (`run_timeout`, `schedule_to_start`, an operator reason).
2. Otherwise the stored result payload (`PAYLOAD_RESULT`) is read as the failure JSON `{"reason_code","kind","message"}` and rendered as `"<reason_code>: <message>"`. A guest `RunFailed` gives `workflow_failed: <message>`; executor failures give e.g. `nondeterminism: <detail>` or `limit_exceeded: ...`.
3. If there is no payload or it is not JSON, the reason is `failed` (or `timed_out` for a `TimedOut` run).

The `reason` filter is a prefix match on this string, applied in memory.

**Blob handling:** inline results are parsed in place. A blob-backed result is fetched only when `BlobRef.len <= 64 KiB` (`MAX_REASON_BLOB`). A larger one reads `"<n>-byte failure result (not loaded)"`, so a list page never loads more than 64 KiB per entry. A blob fetch error falls back to `failed`.

## Membership and listing

- **Member:** `Failed | TimedOut`, the queue's `DlqPolicy.enabled` (ext 145, absent = enabled), and no `dismissed_at_ms` / `requeued_to` in the `dlq` row for the **current episode** (`episode_completed_at_ms == completed_at_ms`; a stale row is not membership, per 11-08).
- **`is_dlq_member`** does exactly this with one point read. 11-11's `dlq=true` search now hides dismissed and requeued runs in both orders (asserted).
- **Listing:** k-way merge of the `Status(Failed)` and `Status(TimedOut)` index prefixes of every partition, newest `created_at_ms` first. Token `i1.d.<created_at_ms>.<run>`. At most `SCAN_BUDGET` rows are examined per call. While the index is rebuilding, a request without a token uses the budgeted oldest-first scan (`c1.` tokens), and a request with an `i1.` token gets 503 `index_rebuilding` (the 11-11 rule).
- **Marks:** every `DlqUpdate` carries the run's current `completed_at_ms`, and each one is gated by `p11_feature_enabled(DlqUpdate)`. The ext 144/145 writes are gated by `FailurePolicyExt`. Every `at_ms` comes from `P11Ctx.cc.clock`.

## Hook service and catch-up behaviour

- **Live path:** the service subscribes to every ready partition's action stream and handles `RunCompleted` (see `drive`, in order):
  1. Read the run; skip it unless it is `Failed`/`TimedOut`.
  2. Loop guard: skip a run that carries `__ostro/on_failure_of=`.
  3. Resolve the policy (the workflow's 144 wins over the queue's 144; the queue's 145 must be enabled).
  4. Skip if the run already has `HookStarted` for this episode.
  5. Start the hook through `start_run`, with dedupe `__onfail/<run>/<resumed_count>`, `ReturnExisting`, and the tag `__ostro/on_failure_of=<run>`. The input is `{run_id, workflow, version, status, reason, failed_at_ms, tags, input}`, where `input` is the source input JSON only when `include_input` is set, else `null`.
  6. Failpoint `dlq::hook_after_start`.
  7. Write `DlqUpdate HookStarted`.
- **Catch-up:** this pass runs once at service start (leader start).
  - For each partition, it walks both status prefixes newest first, up to 100,000 rows. It re-drives every failed run whose `completed_at_ms >= now - hook_catchup` (the queue's 145 value, else `[dlq] hook_catchup`) and that has no `HookStarted` for its episode.
  - While the index is `Rebuilding`, it retries with backoff (250 ms, doubling up to 5 s). After `REBUILD_FALLBACK` (60 s) it scans `runs` instead (budgeted).
  - `catch_up(ctx, fallback, token)` is public, so tests can pass a short fallback.
- **Metrics:** `ostro_dlq_hooks_total{result=started|existing|error}` and `ostro_dlq_requeues_total{mode=resume|restart|rerun}`.

## Failpoint test timings

`hook_exactly_once_across_a_crash_after_start` (`--features failpoints --test-threads=1`, in-process per D-P11-32):
- Failpoint phase (failure → hook run started, `HookStarted` skipped, plus a 300 ms check window): 316 ms.
- Restart → catch-up writes `HookStarted` for the same hook run: 180 ms.
- The 6 `hook` tests in the failpoints build take 15.3 s in total.

## Tests

- `cargo test -p ostro-server --test dlq_api --locked`: **10 passed** (6.7 s).
  - `view_*`: 3 failures (guest step error, run timeout, nondeterminism), newest first, reasons, resumable, tags; reason/tag/workflow filters; 2+1 pagination; the scan fallback with markers deleted and 503 for an `i1.` token; rebuild.
  - `dismiss_*`: dismiss + idempotent re-dismiss; the include_dismissed view; 11-11 search hides the run; Completed run → 409 `dlq_not_member`. Episode case: dismissed → resumed → refailed → back in the DLQ without the dismiss mark.
  - `requeue_*`:
    - restart: dry run; restart with `__dlq/<run>/1`, `retry_of` and tags copied; re-issue returns the same run; `requeued_to` shown; the view hides it.
    - resume: on a non-resumable run → 409; auto resume (same id) completes after the fix.
    - rerun: delegation only (501 `lands_in: 11-10` now, 2xx accepted after 11-10).
    - requeue metrics.
  - `policy_*`: workflow PUT/GET round trip; RunPolicy (`retention 1h`) unchanged; tag 150 preserved next to 144; stale `version_tag` → 409; queue `dlq` round trip via 145; a disabled queue has no members; unknown workflow → 404.
  - `hook_*`:
    - exactly one hook with the exact documented input, `hook_run_id` on the entry, `started` metric;
    - loop guard (a single `always_fail` run after 2 s);
    - a second episode gets a second hook (`__onfail/<run>/1`);
    - a disabled queue gets no hook;
    - catch-up window after a restart (3 s window: old run skipped, new run hooked);
    - scan fallback while rebuilding (4 partitions fall back, 1 started; a second pass drives nothing).
- `cargo test -p ostro-server --features failpoints --test dlq_api --locked -- --test-threads=1 hook`: **6 passed** (including the crash test).
- `cargo test -p ostro-server --lib --test search_api --test p11_contract --test lifecycle_api --test runs_api`: green (155 lib tests, including the new `onfailure` / `p11_dlq` unit tests).
- `cargo test -p ostro-server --locked --no-fail-fast` (before the rebase; only unrelated commits landed in between): exit 0, 64 test binaries, 534 passed, 0 failed. After rebasing onto be63860: `cargo check --workspace --all-targets --locked` clean, clippy clean, `dlq_api` 10 passed (the dlq_hook action filter was added after the full run and re-tested with `dlq_api` + clippy).
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`, with and without `--features failpoints`: clean. `cargo fmt` (ostro-server and the guest): clean.
- CLOCK_GATE_SCOPED on the three server files: no `SystemTime::now` / `SystemClock` / `Instant::now`.

## Deviations from Plan

1. **[Rule 3] No `FakeClock` in the server tests.** The guests' durable sleeps and run timeouts run on the server clock, so the hook and catch-up tests use the system clock with short windows (a 3 s `hook_catchup`, a 300 ms rebuild fallback passed to `catch_up`). Server logic reads time only through the injected clock (DLQ `at_ms`, the catch-up cutoff), and the fallback wait counts its own sleeps.
2. **Rebuild test without a paused rebuild hook.** The markers are deleted on the live server (the index stays `Rebuilding`, because nothing rebuilds it while the node runs) and `dlq_hook::catch_up` is called directly with a short fallback. A real restart would race the Leader-start rebuild (same approach as 11-11).
3. **Kill test runs in-process** (D-P11-32 alternative, as in 11-07). `cargo build -p ostro --features failpoints --target-dir target/failpoints` was not run (shared disk), because nothing spawns the binary.
4. **Commit split.** Task 1's commit holds the controllers and the codec. `tests/dlq_api.rs` (Task 1 and Task 2 tests) lands with Task 2, because the tests need the `p11-hook` guest and `dlq_hook::catch_up`.
5. **Small extra:** `crates/ostro-types/src/command.rs` module/struct doc comments no longer say "NotImplemented until 11-08" / "Apply ignores them until 11-07" (comment-only, coordinator request).
6. **Workflow per the executor brief:** worktree `wt-11-12`, slot-2, CAS update-ref, instead of `/tmp/ostro-wt-11-12` and a private index.

## Open items

- **11-18:** add `ostro-server:dlq_api` (features failpoints, `--test-threads=1`, filter `hook`) to `FAILPOINT_CRATES` in `scripts/ci-local.sh` and to the ci.yml "Failpoint suites" step. Without it, the crash test runs only when someone enables the feature.
- **11-10:** the rerun requeue path is wired (`p11_rerun::rerun_run` with idempotency key `__dlq/<run>/<n>`, then `Requeued{rerun}`). The end-to-end test belongs to 11-10/11-18.
- Hook starts go through StartRun rate-limit admission. A 429 counts as `error`, and the next leader start's catch-up retries it. There is no in-process retry loop.
- With Phase 12 replication, `dlq_hook::spawn` must run only on leader slots, like the other leader services. Today every ready slot is a leader (RF1).

## Self-Check: PASSED

- FOUND: crates/ostro-server/tests/dlq_api.rs, guests/p11-hook/{Cargo.toml,src/lib.rs}, control/p11_dlq.rs, onfailure.rs, services/dlq_hook.rs
- FOUND: 6b16199, 57dd1cf
