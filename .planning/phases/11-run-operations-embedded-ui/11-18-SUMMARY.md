---
phase: 11-run-operations-embedded-ui
plan: 18
subsystem: phase-gate
tags: [e2e, kill-tests, replay-corpus, docs, ci, reservations, ui, rerun, dlq, bulk, limits]
requires: [11-01, 11-02, 11-03, 11-04, 11-05, 11-06, 11-07, 11-08, 11-09, 11-10, 11-11, 11-12, 11-13, 11-14, 11-15, 11-16, 11-17]
provides:
  - spawned-binary SC1-SC5 evidence (ops_e2e, ui_e2e) and failpoint-binary kill tests (ops_kill)
  - replay-corpus entries rerun-fixed-from-step, rerun-rearmed-timer
  - docs/operations.md, docs/ui.md
  - Phase 11 failpoint suites in FAILPOINT_CRATES / ci.yml
  - RESERVATIONS P11 flip, REQUIREMENTS ticks, 11-VALIDATION results
affects: [phase-10 (clock sweep, ostro-client), phase-12/13 (leader-only dlq_hook, bulk forwarding), phase-14 (UiAuth)]
tech-stack:
  added: []
  patterns: [restartable spawned node helper (P11Node), browser-like UI client over reqwest (CSRF cookie + htmx headers)]
key-files:
  created:
    - crates/ostro/tests/ops_e2e.rs
    - crates/ostro/tests/ops_kill.rs
    - crates/ostro/tests/ui_e2e.rs
    - docs/operations.md
    - docs/ui.md
    - tests/replay-corpus/rerun-fixed-from-step/
    - tests/replay-corpus/rerun-rearmed-timer/
  modified:
    - crates/ostro/tests/common/p11.rs
    - crates/ostro-server/src/control/p11_dlq.rs
    - crates/ostro-server/src/services/bulk.rs
    - crates/ostro-server/src/control/p11_bulk.rs
    - crates/ostro-server/tests/bulk_ops.rs
    - crates/ostro-server/examples/export_run.rs
    - tests/replay-corpus/README.md
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - .planning/RESERVATIONS.md
    - .planning/REQUIREMENTS.md
    - .planning/STATE.md
    - .planning/ROADMAP.md
    - .planning/phases/11-run-operations-embedded-ui/11-VALIDATION.md
    - .planning/phases/11-run-operations-embedded-ui/11-DECISIONS.md
decisions:
  - "Kill tests in a separate failpoints-only file (ops_kill.rs) so the failpoint step runs only them; FAILPOINT_CRATES runs whole test targets (no name filter in the entry format)"
  - "Bulk API (create/list/get/cancel) waits for the job reload after a restart instead of changing readiness"
  - "No ostro.api.v1 field numbers invented for RunInfo.retry_of/status_seq or a RunPage order-fallback flag (no API-proto block in RESERVATIONS): follow-up"
  - "Gate run as ci-local --no-musl-test (coordinator asked for --no-musl; this adds the musl release build for the NFR-2.1 size)"
metrics:
  duration: "~4 h (incl. 2 h 58 min gate)"
  completed: 2026-11-22
---

# Phase 11 Plan 18: PHASE-11 GATE Summary

All five Phase 11 success criteria pass through the shipped `ostro` binary. The kill tests pass on the failpoint binary, the rerun journals replay from the corpus, and the full `ci-local` gate passed at b460ad7. The gate also found and fixed two product bugs in Phase 11 files: a doubled DLQ reason prefix (11-12) and a bulk list that could come back empty right after a restart (11-13, coordinator request).

## Gate verdict

**PASS (automated)**, with human_needed browser checks listed in 11-VALIDATION "Manual-Only".

One plan-specified check is red for reasons outside Phase 11: the strict CLOCK_GATE (details under Open issues).

## Success criteria evidence

| SC | Evidence (spawned binary unless noted) | Result |
|----|----------------------------------------|--------|
| SC1 | `ops_e2e::sc1_dlq_on_failure_requeue_and_fixed_rerun`. The run that failed at s3 is in `GET /v1/dlq` and `ostro dlq ls` with reason `workflow_failed: s3 fails`. Exactly one `on_fail` hook run exists, tagged `__ostro/on_failure_of=<run>`, with the documented input, and there is still exactly one after 2 s. `ostro dlq requeue --mode restart` starts a new linked run with the tags copied, and the source leaves the default view. After deploying v2, `ostro rerun --from-step s3` completes with result 5: one `step_completed` per step s1..s5, and prefix events 1..cut have the same kind and details as the source. Resume mode is covered in SC3. | ok |
| SC2 | `ops_e2e::sc2_bulk_retry_and_rerun_paced_bounded_and_searchable`. 60 failed runs are tagged `batch`. `ostro bulk retry --rate 20` matched 60, made 60 restarts and is done. `ostro bulk rerun --from-step s3 --rate 20` matched 120 (the snapshot) and created exactly 120 reruns, all completed on v2. Pacing after the burst of 10: 50 ops spanned 2450 ms and 110 ops spanned 5451 ms, which is ≥ 0.9 × (k−1)/rate. `GET /v1/runs?tags=batch&order=newest&limit=25` returned 10 pages, each ≤ 25, newest first, no duplicates, complete. `ostro ls --tag batch --newest`. | ok |
| SC3 | `ops_e2e::sc3_limits_errors_resumable_failures_and_published_table`, on `ostro server` with `[limits]` 20 / 3 / 64KiB. `limits-history(30)` fails with `limit_exceeded: history_events 21 > 20`; `retry --dry-run` reports `resumed`; the journal stops at 20 events. `limits-children(4)` fails with `limit_exceeded: children…` (children are bridged at HEAD). A 100 KiB input returns 413 `{reason: limit_exceeded, limit: payload_bytes, max: 65536, actual}`. A rerun from seq 25 returns 413 `rerun_prefix_too_large` with `limit: history_events` (CLI exit 1, and REST). After a restart with the defaults, `ostro retry` and `ostro dlq requeue --mode resume` both complete with 32 events, and the children run completes. `GET /v1/limits` matches the `docs/limits.md` rows. | ok |
| SC4 | `ui_e2e::ui_pages_and_actions_change_real_state`: `/` returns 303 to `/ui/`. The 14 pages, the 4 fragments and the htmx asset return 200 with the exact D-P11-15 CSP, nosniff, DENY and no-referrer, and show real ids, the module sha and timeline steps. `ui_inventory_views_on_a_live_server_with_a_worker`: TLS `ostro server`, a real `ostro worker` and a cron schedule. Modules, workflows, queues, schedules, workers and ≥ 4 healthy partitions all show live data. | ok |
| SC5 | Same test. Same-origin posts with the CSRF token, sent both htmx-style (`HX-Request` + `X-Ostro-Csrf`) and as plain forms (`csrf` field), each change real state: terminate, cancel, pause + resume a run, pause + resume the `default` queue, retry (HX-Redirect to the restart), rerun preview (verdict ok, cut seq 3, pipeline@2, no run created) then submit (v2 completes), DLQ requeue, bulk preview (no job), bulk create (4 runs cancelled), cancel of a slow job, and schedule pause/resume on the TLS server. A cross-site post, `Host: evil.example` and a tokenless post each return 403, and state is unchanged. | ok |

## Kill tests (`cargo test -p ostro --features failpoints --test ops_kill -- --test-threads=1`)

| Test | Failpoint | Result |
|------|-----------|--------|
| `kill_rerun_before_rearm` | `rerun::before_rearm=return` | Blocked 2.06 s with no live event after the cut; kill -9 and restart; completed 244 ms after restart, one `timer_fired`, one completion per step |
| `kill_hook_after_start` | `dlq::hook_after_start=return` | The hook run started with no HookStarted mark; kill -9 and restart; the catch-up marked the same hook run; exactly one hook |
| `kill_bulk_after_op` | `bulk::after_op=25*off->return` | Stalled at 26 ops with checkpoint done 20; kill -9 and restart; job done, 60 cancelled, `cancel_requested` once per run |

The in-process crash suites `rerun_rearm` (3), `dlq_api` (11) and `bulk_ops` (13) are green with `--features failpoints --test-threads=1`. All four now run in the ci-local/ci.yml failpoint step.

## Replay corpus

- `rerun-fixed-from-step`: 7 events, completed.
- `rerun-rearmed-timer`: 10 events, completed. The cut is TimerStarted + 1, so the timer is re-armed and fires.

Both were exported from a throwaway `ostro dev` data dir. The guests were built with `scripts/corpus-guest-build.sh`, and the `--check` scan was clean. `cargo test -p ostro-engine --test replay_corpus`: 6 passed. The corpus is 6.0 MB.

These are the first entries whose `journal.bin` records carry `JournalEvent.attempt`, which is why `export_run` had to be fixed (see Deviations).

## ci-local gate (`bash scripts/ci-local.sh --no-musl-test`, HEAD b460ad7, 16:04–19:03 UTC)

All steps passed. Step timings:

| Step | Time |
|------|------|
| fmt | 2 s |
| clippy | 41 s |
| build | 70 s |
| sdk fmt+clippy | 23 s |
| sdk test | 26 s |
| guests | 1 s |
| order-saga | 23 s |
| **cargo test** | **4261 s** |
| polyglot toolchains / python / ts | 1 / 3 / 6 s |
| polyglot conformance | 346 s |
| clippy polyglot | 45 s |
| semantics sleepers | 61 s |
| failpoint list sync | 0 s |
| **failpoint suites** | **4530 s** |
| server campaign smoke | 669 s |
| phase 7 failpoint legs | 52 s |
| cargo deny | 2 s |
| check-vendored | 1 s |
| deny fixtures | 0 s |
| forbidden crates | 1 s |
| workflow validation | 2 s |
| zig present | 0 s |
| **musl release (static, mimalloc)** | **513 s** |

- SKIP: the musl zigbuild test (`--no-musl-test`), and the external PG/MySQL/S3 legs (no `.ostro-test-env`).
- No step flaked. The machine was shared with the 09-20 and 12-0x agents.
- The bulk fix (487fce6) landed after the gate. It was re-verified on its own: `bulk_ops` 3× plus the failpoints run, `p11_contract`, `ops_e2e`, `ui_e2e`, `ops_kill`, and clippy on ostro-server.

## Binary size (NFR-2.1)

- The musl release binary is **53,113,136 B (50.7 MiB)**, static, mimalloc and stripped. That is under 60 MB.
- It is +9.08 MB over the 44,033,456 B recorded at the 06-20/08-22 gates. That growth spans Phases 7, 9, 11 and 12.
- 11-01 recorded no pre-Phase-11 size, so the ostro-ui delta cannot be isolated (the binary is stripped). What can be measured: ostro-ui embeds 61,050 B of assets (htmx 52,182 B plus app.css/js), and its templates (41,997 B of source) are compiled into code.

## Reservations check (`11-18-rescheck.py`, after the flip)

```
Cmd tags in code: [1..13, 20, 21, 27, 45] -> all `exists` (Cmd 45 exists (P11, 11-02; apply 11-08))
PASS  P11 Cmd 46-49 unused; RES Cmd 46-49 reserved P11
PASS  StartRun 40/41 in code (max tag 41), 42/43 unused; RES 40/41 exists, 42–43 reserved P11
PASS  JournalEvent.4 attempt fixed64 in code; RES exists
PASS  InvalidCode 29 = BadRerunPrefix, 30 = BadDlqUpdate, 31 unused; RES 29/30 exists
PASS  ext 144/145 in code, 146-159 unused; RES 144/145 exists (P11, 11-12), 146–159 reserved P11
PASS  x_run_index in code; RES exists (P11, 11-05)
PASS  section 11: /v1/runs/{id}/rerun-cuts, ostro rerun/dlq/bulk/limits, __ostro/on_failure_of=, __ostro/rerun_from=, ostro_csrf, X-Ostro-Csrf
RESERVATIONS CHECK: PASS
```

What the flip changed in RESERVATIONS.md:

- Every Phase 11 §11 row that shipped now reads `exists (P11, <plan>)`.
- `ostro_ui_` is unused and stays reserved.
- New row: CLI verbs (11-14).
- New `details.reason`: `index_rebuilding` (11-11).
- The Phase 11 end-gate paragraph records the proto-field follow-up.

## Requirement ticks

OPS-02, OPS-03, OPS-04, OPS-05, OPS-07, UI-01, UI-02 and UI-03 are ticked, each with evidence in the traceability table. UI-01..03 were ticked on the automated HTTP evidence; the browser checks are human_needed. None was left open.

## human_needed (11-VALIDATION Manual-Only)

1. Real-browser render of every `/ui` page in Firefox and Chromium.
2. Zero CSP console errors on every page and action.
3. htmx swaps and polling:
   - the run status every 2 s, stopping at terminal
   - OOB action buttons
   - `hx-swap="delete"` on DLQ rows
   - `HX-Redirect`
   - the retargeted `#flash`
   - the 2 s bulk progress poll stopping at done
   - the 5 s partition poll
   - inventory row swaps (`closest tr`)
4. `hx-confirm` dialogs: terminate and the destructive actions, worker drain, schedule trigger.
5. The no-JS form paths for actions and inventory, each landing via 303.
6. Keyboard navigation and visible focus.
7. CSRF/DNS-rebinding from a real foreign origin.
8. The UI through a non-loopback hostname with `[ui] allowed_hosts`, plus the Phase 14 WARN.
9. Remote CI legs after the first push.
10. User review items: D-P11-3/8, D-P11-13, D-P11-14, D-P11-17, and the D-P11-17 amendment.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug] DLQ reason repeated its code** (found by SC3). A limit failure read `limit_exceeded: limit_exceeded: history_events 21 > 20`. `reason_from_failure_json` now keeps a message that already starts with `<code>:` as it is, and a unit regression test covers it. Commit `fix(11-12)` d33a289.

**2. [Rule 3 - Blocking] `export_run` could not export any post-11-07 run.** Since 11-07, stored `Events` rows carry `JournalEvent.attempt`, so the byte check failed with "journal.bin record != stored Events row". The exporter now reads field 4 from each row with a small protobuf walk (no prost dependency) and writes it into `journal.bin`, so the records stay the exact row bytes, as the corpus README specifies. The replay test decodes the field unchanged.

This file (a dev tool) is outside the Phase 11 files. It was fixed rather than stopped on, because nothing about the frozen format changes. The fix is in the corpus commit 954c9ff.

**3. [Coordinator request] Bulk API vs job reload.** After a restart, `GET /v1/bulk` could list nothing until `resume_all` finished. The fix:

- `BulkRegistry` gains a `loaded` watch.
- create/list/get/cancel await `wait_loaded()`: at most 10 s, and immediately when no bulk service runs.
- Readiness is unchanged.
- The 068a3db polling workaround is replaced by a direct read plus `is_loaded()`.

The same commit fixes the `dry_run_cancel_and_second_job_skips` flake. `ostro_bulk_jobs{state}` is an absolute per-registry gauge on the process-global recorder, so parallel in-process servers overwrite it; the test now asserts only that the series is present. Commit `fix(11-13)` 487fce6.

### Recorded deviations

- **Kill tests location.** They are in `ops_kill.rs`, not a `kill` filter in `ops_e2e`. They use the `cargo test --features failpoints` binary rather than a `target/failpoints` build, to save disk on a shared machine.
- **SC2 counts.** A bulk `retry` of a guest failure restarts each run, and the restarts inherit the tag. The rerun job over the same tag therefore matched 120 runs (the snapshot) and created exactly 120. The plan's "exactly 60" holds per job and per snapshot. Pacing is asserted on the aggregate span after the burst. Individual gaps can come in under 1/rate because ops are applied in commit batches (13 and 32 short gaps were recorded, while the spans matched the rate exactly).
- **SC3 config.** SC3 uses `ostro server --config` because `ostro dev` takes no config file.
- **Gate flags.** The gate ran `ci-local --no-musl-test`; the coordinator asked for `--no-musl`, and this flag adds the musl release build so the size could be recorded. `PROPTEST_CASES=1000 run_index` was not re-run.
- **ci.** No new step was added. ops_e2e, ui_e2e and cli_p11 run in `cargo test --workspace`. FAILPOINT_CRATES gained `ostro-server:rerun_rearm`, `ostro-server:dlq_api`, `ostro-server:bulk_ops` and `ostro:ops_kill`, and `check_failpoint_sync` is green.
- **Workflow.** The executor brief replaced the plan's B1 procedure: scratchpad worktree, slot-2, and a CAS on main.

## Open issues

- **Strict CLOCK_GATE is red** (plan STOP condition; nothing was edited). It reports 33 pre-existing hits in Phase 2/4/5/6/7/8/9 files:
  - `executor.rs`, `server.rs`, `effects.rs`, `sink.rs`, `tls/{ca,acme}.rs`, `workers/spill.rs`
  - `leader/{shuffler,dispatcher}.rs`, `machine.rs`, `node.rs`, `partition lib.rs`, `backup uploader.rs`

  Phase 11 added none: a scoped check over every `(11-NN)` commit found 0. The fix belongs to Phase 10's 10-06 clock-exempt sweep, and Phase 10 has not executed. Recorded as a FOLLOW-UP in STATE.
- **API fields without reserved numbers:**
  - `RunInfo.retry_of` and `status_seq` (11-10/11-15).
  - A `RunPage` order-fallback flag with the `X-Ostro-Order-Fallback` header (11-11).

  No API-proto block exists in RESERVATIONS, so none were invented. Recorded as a FOLLOW-UP. docs/operations.md describes the fallback: oldest order and `c1.` tokens; an `i1.` token gets 503 `index_rebuilding`.
- **Phase 12/13 coordination** (recorded in STATE Blockers and 11-DECISIONS): `dlq_hook::spawn` runs on every slot and bulk jobs are node-local. Both must become leader-only, or be forwarded, once followers exist. No Phase 12 code was changed.
- **D-P11-17 amendment** (dev UI is on only for loopback) is on the USER REVIEW list.
- **Published-limits gap:** `MAX_SEARCH_TAGS` and the bulk caps are not in docs/limits.md or `/v1/limits` (FOLLOW-UP).
- **CLI help text:** `ostro bulk retry --help` says "Retry (resume)", but a guest failure is restarted. This is cosmetic, in an 11-14 file, and was left as is.

## Commits

- d33a289 fix(11-12): DLQ reason does not repeat a code the failure message already carries
- 0f3823b test(11-18): Phase 11 spawned-binary e2e and kill tests
- 954c9ff test(11-18): replay-corpus entries from reruns
- 65ce427 docs(11-18): operations and UI guides
- b460ad7 ci(11-18): Phase 11 failpoint suites
- 487fce6 fix(11-13): bulk API waits for the job reload after a restart; process-global gauge in a test
- docs(11-18): Phase 11 gate — reservations, requirements, validation (this commit)

## Self-Check: PASSED

- The files exist: ops_e2e.rs, ops_kill.rs, ui_e2e.rs, docs/operations.md, docs/ui.md and both corpus entries.
- Commits d33a289, 0f3823b, 954c9ff, 65ce427, b460ad7 and 487fce6 are on main.
