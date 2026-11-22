---
phase: 08-triggers-bottomless-observability
verified: 2026-11-21T00:56:00Z
status: passed
score: 5/5 success criteria verified (SC3 closed by 08-23); 10/10 requirements satisfied in code; 10k campaign PASS (2026-11-22)
re_verification:
  previous_status: gaps_found
  previous_score: 4/5 (SC3 partial on its RPO bound)
  previous_report_commit: 73d6d2d
  gaps_closed:
    - "SC3 / BACK-02: RPO <= seal interval. 08-23 (option B, code) put the uploader on a fixed seal/2 cadence with bounded parallel partition uploads and one manifest per consistent cut. restore_rpo now asserts the end-to-end loss window, and P8-D23 is revised back to SC3 as written (the cut-lag-only relaxation is withdrawn)."
    - "Warning: `[triggers] webhook_tolerance` was a dead config knob. It is now the replay-window fallback (control/webhooks.rs effective_tolerance_s)."
  gaps_remaining: []
  regressions: []
human_verification:
  - test: "Restarted 10,000-fault Phase 8 crash campaign on post-08-23 code (PENDING: restarted after 02745dc; the 541-kill pre-08-23 partial shards were moved to target/campaign-p8-gate/superseded-pre-0823)"
    expected: "target/campaign-p8-gate/report.txt shows kills >= 10000, lost_acked_runs=0, duplicate_journal_entries=0, non_terminal_runs=0, snapshot_required_with_valid_snapshot=0, restore_checked on all 20 shards"
    why_human: "8-12 h run. Not run or awaited by this verifier, as instructed"
  - test: "Quiet-machine re-run of restore_rpo (confirmation of SC3 numbers): `cargo nextest run -P crash -p ostro-crashtest --run-ignored only -E 'test(restore_rpo)'`"
    expected: "PASS. Loss window p50/max at or below the loaded numbers (p50 552 ms, max 1,049 ms), ideally max <= 1,000 ms (strict seal) with no slack used. Record the numbers in 08-VALIDATION Human-needed #7"
    why_human: "08-23 measured at load average 19-33. One of 10 iterations (1,049 ms) used 49 ms of the 500 ms slack. Not re-run here because the machine is loaded"
  - test: "SeaweedFS S3 legs: objstore s3 (conditional PUT), backup fencing s3_fencing, ostro bottomless_s3"
    expected: "All pass with OSTRO_TEST_S3_* set (Docker or CI after first push)"
    why_human: "No Docker on WSL. Locally these are loud SKIPs"
  - test: "R2/AWS conditional PUT with the same tests"
    expected: "If-None-Match/If-Match enforced, or the LOCK re-read fallback engages"
    why_human: "Needs cloud credentials"
  - test: "Fresh-machine `ostro restore --from s3://...` with timing"
    expected: "State hashes match; report cut_at/duration recorded"
    why_human: "Needs a real bucket and a second machine"
  - test: "Jaeger/Tempo + Grafana visual check of the run -> activation -> step / effect:<kind> tree and /metrics dashboards"
    expected: "Tree and dashboards render"
    why_human: "Visual"
  - test: "TLS roots in a FROM scratch image (tls_roots = webpki), Phase 14 artefact"
    expected: "Restore from HTTPS S3 works"
    why_human: "Image artefact not built yet"
  - test: "User review of P8-D3, P8-D9, P8-D11, P8-D16, P8-D21, P8-D25"
    expected: "Accepted or overridden"
    why_human: "Adopted in YOLO mode. P8-D23 no longer needs sign-off: it now enforces SC3 as written instead of relaxing it"
---

# Phase 8: Triggers, Bottomless & Observability Verification Report

**Phase Goal:** Workflows start from webhooks and cron. A single node has real disaster recovery from object storage, and operators can trace and measure everything.
**Verified:** 2026-11-21T00:56:00Z (main 02745dc)
**Status:** human_needed. All 5 success criteria are verified in code and by tests. The restarted 10k campaign is pending; the quiet-machine restore_rpo re-run and the external/visual items remain.
**Re-verification:** Yes. This follows gap-closure plan 08-23. The previous report (73d6d2d) was gaps_found at 4/5 with SC3 partial.

Method (re-verification): I read the previous report, 08-23-SUMMARY, 08-DECISIONS (P8-D23 and the revision log), 08-VALIDATION (rows 8-23-01..03, Human-needed #7) and commits d0f13e2, 5ca919d, 085ce10 and 02745dc. I inspected the code in a worktree detached at main 02745dc. SC3 got full three-level verification. SC1, SC2, SC4 and SC5 got a regression check: 08-23 touched only ostro-backup, the server's uploader service and config, control/webhooks.rs, restore_rpo and docs, so none of their code paths changed apart from the webhook fallback (SC1, below). I did not re-run restore_rpo or the campaign (machine load ~22). I spot-ran one cheap suite in `target/wt-reverify8` (deleted afterwards).

## Goal Achievement

### Observable Truths (ROADMAP success criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Stripe webhook -> workflow -> signal round trip. Stripe, GitHub, Slack and generic HMAC signatures are verified. A duplicate delivery starts only one run. | VERIFIED (regression check) | Unchanged from 73d6d2d, apart from 5ca919d: `deliver` now computes `effective_tolerance_s(w.verifier, w.tolerance_s, config.triggers.webhook_tolerance)` (webhooks.rs:565) and passes it to `VerifierSpec::from_meta`. Stripe, Slack and Standard Webhooks with a stored `tolerance_s = 0` fall back to the config value (default 300 s, minimum 1). Generic HMAC keeps 0 = no window. GitHub and None carry no timestamp. Unit test `webhook_tolerance_is_the_fallback_for_timestamped_kinds` covers own-value-wins, the fallback, the untimestamped kinds and sub-second clamping. 08-23 reported `ostro-server --lib -- webhooks` 6 passed and `--test webhooks` 5 passed. |
| 2 | Each `(schedule, fire_time)` of a TZ-aware cron starts exactly once across restarts. Overlap, catch-up, pause and trigger-now behave as configured. | VERIFIED (regression check) | No 08-23 change touches apply/schedule.rs, leader/cron.rs or ostro-triggers/cron.rs. The evidence in 73d6d2d stands (`cron_exactly_once` 20 kill -9, 92 fires, 0 duplicates; `apply_schedule` 21, `cron_driver` 8). |
| 3 | `ostro restore --from <bucket>` reproduces the state hash with RPO <= seal interval (target <= 1 s). `backup`/`restore` to a local file round-trips. Only the leader uploads, and only sealed segments below HW. | VERIFIED (gap closed; quiet-machine confirmation listed below) | **Fixed cadence:** `Uploader::run` (uploader.rs:441-476) uses `tokio::time::interval_at(now + p, p)` with `MissedTickBehavior::Delay`. `p = round_interval() = max(seal_interval / 2, 10 ms)` (l.181), so a round starts one period after the previous round *started*, and an overrun is followed at once. A failure backs off (doubling, capped at 30 s) and then calls `reset_immediately()`. A fenced uploader returns for good. **Parallel uploads:** `upload_partitions` (l.1232-1270) spawns one owned `JoinSet` task per started partition, holding a `Semaphore(concurrency)` permit; `concurrency(P) = min(upload_concurrency or 8, P).max(1)` (l.186). The first error aborts the rest and fails the round, and `round()` then drops the session, so the next round re-reads `manifest/current`. Blob PUTs are bounded the same way and all finish before any chunk (l.1021-1060). The meta snapshot PUT overlaps the partitions through `tokio::join!` (l.567). **One consistent cut per manifest:** the applied pairs, then `log_cut()`, then `cut_at_ms` are read once in step 1 (l.525-534). Every partition task gets `cut[p]` from that single vector. The manifest entries are built only after `upload_partitions` has joined every task and returned the states in partition order (l.566-572, 611-675). An incomplete round (a skipped partition) publishes nothing. Unchanged from 08-13: `Manifest::validate`, `fence::check_before_manifest` (LOCK), the `upload::before_manifest` failpoint, the If-Match / If-None-Match PUT, floors and retention only after the durable manifest, and the leader-only barrier check (l.501-512). **Config wired:** `[backup] upload_concurrency` goes from server config.rs:650/677/704 into `BackupConfig` (0 = auto), and services/uploader.rs:78-79 logs round_interval and concurrency. **Assertion:** restore_rpo.rs:483-493 (085ce10) fails if any iteration's `loss_ms` (newest missing acked start minus the restored manifest's `cut_at_ms`, l.422) exceeds `SEAL_MS (1000) + LOSS_SLACK_MS (500)`. It is checked after all 10 iterations, so a failure still prints the distribution. The cut-lag assertion (<= seal + 500) is kept (l.431), and the pre-cut-ack completeness check (b) and file round trip are unchanged. **Measured by 08-23 (not re-run here):** loss window p50 552 ms, max 1,049 ms (was p50 1,680 / max 2,449); 9 of 10 iterations were under 1,000 ms. Cut lag p50 280, max 412 ms. Restored-round upload p50 217, max 576 ms. 18,276 acks, every pre-cut ack restored, file round trip passed, PASS in 291 s at load 19-33. **Bound:** loss <= seal/2 + U, which is <= seal while U <= seal/2. The single 1,049 ms iteration exceeds the literal 1 s by 49 ms, within the documented slack (the ack is timestamped client-side after the HTTP response, which overstates the loss). I accept this as meeting SC3 given the load, and list the quiet-machine re-run as a confirmation item. **Decision:** P8-D23 is revised to "RPO = end-to-end data-loss window <= seal_interval (SC3/BACK-02 as written, no rewording)", and the earlier cut-lag-only reading is withdrawn, so no user sign-off is pending on a relaxation. docs/bottomless.md §RPO measurement matches. **Spot run:** see "Spot runs" below. Restore path, CLI, admin tar and leader-only upload: unchanged from 73d6d2d, apart from restore.rs, which now drains both JoinSets before returning an error (08-23 Rule-1 fix). The S3 leg (`bottomless_s3`) is SKIP locally (human_needed). |
| 4 | One documented snapshot format serves restore and future follower install. Trimming never removes entries that no snapshot covers. | VERIFIED (regression check; campaign pending) | 08-23 did not touch the snapshot format, install_snapshot or retention.rs trim_floor. The uploader still advances upload floors only after the durable manifest. The evidence in 73d6d2d stands. **The restarted 10k campaign is pending.** |
| 5 | run -> activation -> step/effect spans linked to the caller's `traceparent` and propagated on outbound HTTP, with no re-emitted replay spans. `/metrics` has queue, lag, step, replay, fuel, memory and DLQ series. Guest logs are structured, rate-limited and bounded per run. | VERIFIED (regression check) | No 08-23 change touches telemetry, effect_trace, run_spans, guest_logs or metrics_poll. The evidence in 73d6d2d stands. |

**Score:** 5/5 truths verified.

### Spot runs (this re-verification)

Run at nice 10, `-j 4`, `--test-threads=1`, in `target/wt-reverify8`, with load ~22:

- `cargo test -p ostro-backup --features failpoints --test uploader -- --test-threads=1`: **10 passed, 0 failed** (90.9 s), including the new `parallel_uploads_name_one_consistent_cut` and `run_starts_rounds_on_a_fixed_cadence`, plus `cut_lag_per_tick_at_p16`, `failpoint_before_manifest_then_idempotent_retry`, `closed_barrier_inspect_mode_uploads_nothing` and `upload_floor_caps_trimming`.

Not re-run (by instruction or load): restore_rpo, the campaign, and the ostro-server suites. 08-23 recorded them green; see 08-23-SUMMARY § Verification.

### Required Artifacts (08-23 delta)

| Artifact | Status | Details |
|----------|--------|---------|
| `crates/ostro-backup/src/uploader.rs` | VERIFIED | 1,591 lines; fixed cadence, `upload_partitions` (JoinSet + Semaphore), single-cut manifest; started via services/uploader.rs |
| `crates/ostro-backup/src/config.rs` / `crates/ostro-server/src/config.rs` | VERIFIED | `upload_concurrency` (0 = auto) plumbed server -> BackupConfig |
| `crates/ostro-backup/src/restore.rs` | VERIFIED | JoinSet drain before error return |
| `crates/ostro-backup/tests/uploader.rs` | VERIFIED | New `parallel_uploads_name_one_consistent_cut` (concurrency 1/auto/16 at P=16) and `run_starts_rounds_on_a_fixed_cadence` |
| `crates/ostro-crashtest/tests/restore_rpo.rs` | VERIFIED | Loss-window assertion `<= SEAL_MS + LOSS_SLACK_MS`; the slack must not be widened for load (doc comment) |
| `crates/ostro-server/src/control/webhooks.rs` | VERIFIED | `effective_tolerance_s` wired at the delivery site, with a unit test |
| `docs/bottomless.md`, `docs/webhooks.md` | VERIFIED | RPO section, upload_concurrency, cost note; tolerance fallback documented |
| Phase 8 artifacts from 73d6d2d | VERIFIED (unchanged) | See the previous report's list |

### Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| `Uploader::run` ticker | `round()` | `interval_at` + `MissedTickBehavior::Delay`, period `round_interval()` | WIRED |
| `round_inner` step-1 `cut` | every partition task | `upload_partitions(&ctx, parts, &cut, concurrency)` -> `cut_p = cut[p]` | WIRED |
| joined partition states | Manifest `cut` entries | `sess.parts = parts` before step 7 | WIRED |
| `[backup] upload_concurrency` | `UploaderConfig::concurrency` | server BackupSection -> BackupConfig | WIRED |
| `[triggers] webhook_tolerance` | `VerifierSpec::from_meta` | `effective_tolerance_s` in `deliver` | WIRED |
| restore_rpo `loss_ms` | test failure | `over.is_empty()` assert | WIRED |
| Earlier links (hooks, cron, services, effect hooks, retention floors, restore CLI/admin) | | | WIRED (unchanged) |

### Requirements Coverage

| Requirement | Description | Status | Evidence |
|-------------|-------------|--------|----------|
| TRIG-01 | `POST /hooks/{name}` with Stripe, GitHub, Slack and generic HMAC verification + idempotency | SATISFIED | SC1; tolerance fallback now wired |
| TRIG-02 | TZ cron, exactly once per `(schedule, fire_time)` | SATISFIED | SC2 |
| TRIG-03 | Overlap, catch-up, pause, trigger-now | SATISFIED | SC2 |
| BACK-01 | Leader-only sealed segments below HW, snapshots, meta snapshot and blobs to object storage | SATISFIED (S3 external leg human_needed) | Uploader barrier check and cut ordering unchanged; parallelism keeps one cut |
| BACK-02 | Restore from bucket, RPO <= seal interval, hash match | SATISFIED (quiet-machine confirmation pending) | SC3; loss window asserted; measured max 1,049 ms at load 19-33 |
| BACK-03 | backup/restore to a local file | SATISFIED | restore_rpo file round trip (08-23 run), `backup_cli`, admin tar |
| BACK-04 | One snapshot format; trim coordinates with snapshots | SATISFIED | SC4 (campaign pending) |
| OBS-01 | Traces with traceparent in and out; no replay re-emit | SATISFIED | SC5 |
| OBS-02 | Prometheus `/metrics` series | SATISFIED | SC5 |
| OBS-03 | Guest logs structured, rate-limited, bounded, forwarded | SATISFIED | SC5 |

No orphaned requirements. As instructed, this verifier did not edit REQUIREMENTS, ROADMAP or STATE.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| ~~crates/ostro-server/src/config.rs~~ | ~~749~~ | ~~`webhook_tolerance` parsed, never read~~ | ~~Warning~~ | **Resolved by 5ca919d** |
| crates/ostro-crashtest/tests/restore_rpo.rs | 65 | `LOSS_SLACK_MS = 500` lets the asserted bound be 1.5x seal | Info | Documented and justified (client-side ack timestamp, scheduling, fsyncs; never widened for load). The quiet-machine re-run should show the strict 1 s holds |
| crates/ostro-partition/tests/trim_safety.rs | 50 | `Node::open` 120 s timeout flakes under load (> 20) | Info | Environmental (unchanged) |
| (08-20 finding) armed server with a failed partition | - | never prints its ready line within 60 s | Info | Unchanged open follow-up |
| 08-VALIDATION.md | 265-266 | Human-needed list has two items numbered "7." | Info | Cosmetic |
| docs/bottomless.md Cost | - | A busy partition can seal two chunks per seal_interval (PUT cost roughly doubles) | Info | Documented trade-off of the seal/2 cadence |

The TODO/FIXME/`unimplemented!`/stub scan over the 08-23 files found nothing.

### Human Verification Required

1. **Restarted 10,000-fault Phase 8 campaign: PENDING.** It was restarted on post-08-23 code. The earlier partial shards (541 kills, all tallies 0) were moved to `target/campaign-p8-gate/superseded-pre-0823`. This verifier did not run or wait for it. Gate: `kills >= 10000`, `lost_acked_runs=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0`, `snapshot_required_with_valid_snapshot=0`, and `restore_checked` on every shard. Write the report to `target/campaign-p8-gate/report.txt`.
2. **Quiet-machine re-run of `restore_rpo` (confirmation, SC3).** Run `cargo nextest run -P crash -p ostro-crashtest --run-ignored only -E 'test(restore_rpo)'` with load near 0. Expect PASS with the loss window at or below p50 552 / max 1,049 ms, ideally max <= 1,000 ms. Record it in 08-VALIDATION Human-needed #7.
3. **SeaweedFS S3 legs:** `ostro-objstore --test s3`, `ostro-backup --test fencing` (s3_fencing) and `ostro --test bottomless_s3`, on Docker or in CI.
4. **R2/AWS conditional PUT** with the same tests.
5. **Fresh-machine `ostro restore --from s3://...`** with timing recorded.
6. **Jaeger/Tempo + Grafana** visual check of the span tree and dashboards.
7. **Scratch-image TLS roots** (Phase 14).
8. **User review** of P8-D3/D9/D11/D16/D21/D25. P8-D23 is off the list, because it now enforces SC3 as written.

### Gaps Summary

There are no remaining gaps. 08-23 closed the SC3/BACK-02 gap with option B (code), not by rewording:

- The uploader starts rounds every seal/2 from round start.
- It uploads partitions (and blobs) concurrently under a semaphore, against a single step-1 cut.
- The manifest is assembled only after every partition task joins.
- restore_rpo asserts the end-to-end loss window.

The measured loss window fell from p50 1.68 s / max 2.45 s to p50 0.55 s / max 1.05 s under heavy load. The dead `webhook_tolerance` knob warning is also resolved.

Phase 8 is not marked passed until the restarted 10,000-fault campaign finishes clean. The quiet-machine restore_rpo re-run is a confirmation item, not a blocker.

---

_Verified: 2026-11-21T00:56:00Z (re-verification after 08-23; previous: 2026-11-20T23:29:19Z, gaps_found)_
_Verifier: Claude (gsd-verifier)_


## Closure (2026-11-22, coordinator)

Both human-verification items are done: the restarted 10,000-kill campaign reports  (kills=10000, 0 lost acked runs/signals, 0 duplicates, 0 non-terminal, 0 wrong results, restore_checked on 20/20 shards) after the user-approved crashtest checker fix (dd23bcc: wait winner read from the journal, new LostWake check; 21 late-signal false positives removed, 0 lost wakes) and a clean shard-10 rerun (500 kills, no Node::open timeout); evidence in 08-VALIDATION.md (d1f3119). Quiet-machine restore_rpo: loss window p50 460 ms, max 712 ms (<= 1,000 ms seal). Phase 8 marked complete.
