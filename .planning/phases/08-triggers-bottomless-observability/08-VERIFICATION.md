---
phase: 08-triggers-bottomless-observability
verified: 2026-11-20T23:29:19Z
status: gaps_found
score: 4/5 success criteria verified (SC3 partial on its RPO bound); 10/10 requirements have implementation evidence
gaps:
  - truth: "SC3 / BACK-02: `ostro restore --from <bucket>` reproduces the state hash with RPO <= the segment seal interval (target <= 1 s)"
    status: partial
    reason: >
      The restore works and the state hash is verified. The RPO bound as written is not met.
      At seal_interval = 1 s, 08-20 restore_rpo measured a data-loss window (newest lost ack
      minus cut_at_ms) of p50 1,680 ms and max 2,449 ms over 10 iterations. Cut lag was p50
      1,044 ms and max 1,203 ms. The test asserts cut lag <= seal + 500 ms slack, not
      <= seal. P8-D23 redefined SC3's "RPO" as cut lag only, and docs/bottomless.md states
      "RPO ~ seal_interval + upload latency". P8-D23 was adopted in YOLO mode and is not on the
      user-review list (P8-D3/D9/D11/D16/D21/D25), so the success criterion was relaxed
      without sign-off.
    artifacts:
      - path: "crates/ostro-crashtest/tests/restore_rpo.rs"
        issue: "asserts cut_lag_ms <= SEAL_MS + SLACK_MS (500 ms); the loss window is only reported, never asserted"
      - path: "crates/ostro-backup/src/uploader.rs"
        issue: "one sequential round per seal_interval, so the worst case is the seal interval plus the round's upload latency (p50 ~700 ms, max 2,030 ms)"
      - path: ".planning/phases/08-triggers-bottomless-observability/08-DECISIONS.md"
        issue: "P8-D23 reinterprets SC3 but is not in the user-review list"
    missing:
      - "Option A (decision only): the user ratifies P8-D23. ROADMAP SC3 and REQUIREMENTS BACK-02 are reworded to 'cut lag <= seal_interval; data-loss window ~ seal_interval + upload latency (measured p50 1.7 s / max 2.4 s at seal 1 s)'. Add P8-D23 to the 08-DECISIONS user-review list."
      - "Option B (code): make the measured loss window <= seal_interval. Start each round on a fixed cadence rather than after the previous round finishes, upload partitions in parallel within a round (08-13 noted uploads are still sequential per partition), and/or use an effective cut cadence of seal_interval/2. Then add an assertion `loss_window_ms <= SEAL_MS + slack` to restore_rpo and re-run it on a quiet machine."
human_verification:
  - test: "Finish the 10,000-fault Phase 8 crash campaign (PENDING: running now in target/campaign-p8-gate)"
    expected: "report.txt shows kills >= 10000, lost_acked_runs=0, duplicate_journal_entries=0, non_terminal_runs=0, snapshot_required_with_valid_snapshot=0, restore_checked on all 20 shards"
    why_human: "8-12 h run. At verification time the shard files still showed 541 kills (shards 0-5 partial, all violation tallies 0)"
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
  - test: "User review of P8-D3, P8-D9, P8-D11, P8-D16, P8-D21, P8-D25, plus P8-D23 (see gap)"
    expected: "Accepted or overridden"
    why_human: "Adopted in YOLO mode"
---

# Phase 8: Triggers, Bottomless & Observability Verification Report

**Phase Goal:** Workflows start from webhooks and cron. A single node has real disaster recovery from object storage, and operators can trace and measure everything.
**Verified:** 2026-11-20T23:29:19Z (main 8e6ccd7)
**Status:** gaps_found. One partial criterion: the SC3 RPO bound. The 10k campaign is pending, plus human-needed items.
**Re-verification:** No (initial verification)

Method: I read the 22 PLAN/SUMMARY files, 08-DECISIONS, 08-VALIDATION and the post-plan fix notes in 08-07, 08-12, 08-15 and 08-20. I inspected the code in a worktree detached at main. I spot-ran cheap suites in `target/wt-verify8` (deleted afterwards), one at a time, at nice 10 with `-j 4`, while the campaign was loading the machine (load 22-38).

## Goal Achievement

### Observable Truths (ROADMAP success criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Stripe webhook -> workflow -> signal round trip. Stripe, GitHub, Slack and generic HMAC signatures are verified. A duplicate delivery starts only one run. | VERIFIED | `ostro-triggers/src/webhook.rs` has 5 verifier kinds (Stripe, GitHub, Slack, GenericHmac, StandardWebhooks), all compared via `Mac::verify_slice` (constant time), with a replay window. `control/webhooks.rs` verifies the raw body before parsing (the type system enforces it), returns 401 with `details.reason`, starts with idempotency key `__hook/<hook>/<key>` under `ConflictPolicy::ReturnExisting`, sends the signal with `request_id` = delivery key, and since the 2123663 fix answers a post-terminal redelivery 200 duplicate via the SignalSeen row. `POST /hooks/{name}` is in `ostro-api/src/phase8.rs`. Test `triggers_e2e::stripe_round_trip` asserts: same run on re-delivery, `duplicate=true`, exactly one `SignalPayload`, tampered = 401. `github_slack_hmac_hooks` asserts exactly 3 runs, tampered = 401, and the Slack challenge is echoed. **Spot run:** `ostro-triggers` 5 + 22 (cron) + 33 (webhook_verify) passed. |
| 2 | Each `(schedule, fire_time)` of a TZ-aware cron starts exactly once across restarts. Overlap, catch-up, pause and trigger-now behave as configured. | VERIFIED | `apply/schedule.rs` (Cmd 27): a durable watermark in keyspace 16, `ft <= W` -> Duplicate, the `__cron/` dedupe row as a second guard, Skip/BufferOne/CancelOther/TerminateOther/AllowAll, incarnation reset, and the 826c592 manual-retry-before-overlap fix. `leader/cron.rs` is spawned from `leader/mod.rs:127` and configured in `node.rs:427`; it uses `plan_catchup` and keeps the newest fires. `ostro-triggers/src/cron.rs` uses croner on the same TZif bytes as jiff 1.0; tests cover NY/Berlin DST gap and overlap. The CLI has create/pause/resume/trigger. `triggers_e2e::cron_cli_policies` exercises Skip, trigger while live = 409, pause, trigger-now with request-id dedupe, resume catch-up One, and downtime catch-up. The gate re-ran `cron_exactly_once` (20 kill -9, 92 fires, 0 duplicates): PASS. **Spot run:** `apply_schedule` 21 passed, `cron_driver` 8 passed. |
| 3 | `ostro restore --from <bucket>` reproduces the state hash with RPO <= seal interval (target <= 1 s). `backup`/`restore` to a local file round-trips. Only the leader uploads, and only sealed segments below HW. | PARTIAL | The restore path is substantive: `ostro-backup/src/restore.rs` does `Manifest::validate`, `install_snapshot` (OSNP), a backward chain walk + `verify_chain`, two-step in-files replay with a `state_hash` compare (HashMismatch names the partition), and meta bootstrap. The CLI is wired in `crates/ostro/src/main.rs` (Backup/Restore), and online `GET /v1/admin/backup` streams a tar through the same code path (`control/admin.rs`). The uploader checks `barrier.is_open()` on every started partition and returns `barrier_closed` otherwise, so only the leader uploads. It takes the HW cut after reading applied (W2), uploads chunks `[uploaded_end, cut)`, and is started by `services::Services::start`. Gate `restore_rpo` PASS: every pre-cut ack restored over 10 iterations, plus a file round trip (16 partitions, 2,045 runs). **The RPO bound is not met as written:** the loss window was p50 1.68 s / max 2.45 s and cut lag p50 1.04 s at seal 1 s. See the gap. The S3 leg (`bottomless_s3`) is SKIP locally (human_needed). |
| 4 | One documented snapshot format serves restore and future follower install. Trimming never removes entries that no snapshot covers. | VERIFIED | `docs/snapshot-format.md` OSNP v1 has a header, frames, trailer/digest, staged install, the remote key, §9 streaming and §11 Phase 12 checklist. The same `ostro_partition::store::snapshot::install_snapshot` is used by the open path and by restore. `leader/retention.rs::trim_floor = min(ckpt, local, upload?, retention?)`, where `local` = the oldest retained snapshot's applied offset (0 = no trim) and `upload` is initialised to `log_start` when backup is on. The gate re-ran `crash_loop_snapshots`: PASS. The 200-fault Phase 8 campaign smoke passed with 0 violations and 4 bucket restores verified. **Spot run:** `snapshot` 15 passed (1 ignored). `trim_safety` 5/6 passed at load ~38; `sc4_random_snapshot_trim_kill_sequences` failed with `node open timed out` (OPEN_T 120 s) after 1,011 s. Its persisted shrunk case replayed alone **passed** (62 s), so this is the known load flake (open timeouts when load > 20), not a trim-safety violation. The gate run passed it on a quiet machine. **The 10k campaign is pending.** |
| 5 | run -> activation -> step/effect spans linked to the caller's `traceparent` and propagated on outbound HTTP, with no re-emitted replay spans. `/metrics` has queue, lag, step, replay, fuel, memory and DLQ series. Guest logs are structured, rate-limited and bounded per run. | VERIFIED | `ostro-engine/src/telemetry.rs`: deterministic ids; step spans only for live-journaled steps after commit; `workflow.run` emitted from post-apply RunCompleted (`services/run_spans.rs`). `effect_trace.rs` TraceEffectHooks is installed in `server.rs:325` when a tracer provider exists; it puts the traceparent only on the live request copy, so the journal is unchanged. Metric names were found in code: `ostro_queue_ready`, `ostro_queue_lag_seconds` (dispatcher), `ostro_partition_apply_lag`, `ostro_partition_hw_lag`, `ostro_isr_size`, `ostro_raft_*` (metrics_poll), `ostro_steps_total`, `ostro_fuel_consumed_total` (activation), `ostro_replay_events_total`, `ostro_instance_memory_bytes`, `ostro_dlq_total`. `services/guest_logs.rs` has a per-run bucket (50/s, burst 500), a per-run cap (10k lines / 10 MiB), a node bucket, an LRU ring and JSON + OTLP LogRecords. Gate tests `observability_e2e` (caller trace parenting; 3 activations / 2 step spans unchanged after restart; 524 LogRecords = accepted; the metric series present), `outbound_traceparent` (`BRIDGE_TRACEPARENT_LEG: wired`) and `otlp_canary` (0 canary hits in 32.8 MB of export) passed in the full ci-local. Not re-run here because of load. |

**Score:** 4/5 truths verified (SC3 partial on its quantitative bound only).

### Required Artifacts (spot list)

| Artifact | Status | Details |
|----------|--------|---------|
| `crates/ostro-triggers/src/{webhook,cron,tz,extract}.rs` | VERIFIED | 1,800 lines; used by control/webhooks.rs and leader/cron.rs |
| `crates/ostro-partition/src/apply/schedule.rs`, `leader/{cron,snapshot,retention}.rs` | VERIFIED | Spawned from leader/mod.rs; tests green |
| `crates/ostro-backup/src/{uploader,restore,archive,chunk,manifest,fence,remote_gc}.rs` | VERIFIED | Uploader spawned via services; restore wired to the CLI and admin |
| `crates/ostro-objstore/src/{local,s3}.rs` | VERIFIED (S3 external leg human_needed) | LocalFs + rusty-s3; s3s-fs in-process conformance green at the gate |
| `crates/ostro-server/src/control/{schedules,webhooks,run_logs,admin}.rs` | VERIFIED | Routes in ostro-api/phase8.rs |
| `crates/ostro-server/src/services/{uploader,blob_gc,metrics_poll,run_spans,guest_logs}.rs` | VERIFIED | All started by `Services::start` |
| `crates/ostro-server/src/effect_trace.rs` | VERIFIED | Installed in server.rs |
| `docs/{snapshot-format,bottomless,webhooks,cron,metrics,observability}.md` | VERIFIED | Present and substantive |

### Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| `POST /hooks/{name}` | `webhook::verify` -> StartRun / signal | control/webhooks.rs `deliver` | WIRED |
| leader/cron.rs | `Cmd::ScheduleFire` apply | `leader/mod.rs:127 cron::spawn` | WIRED |
| services::Services::start | uploader / blob_gc / metrics_poll / run_spans | `server.rs` services seam | WIRED |
| ServerEffectDriver | TraceEffectHooks | `server.rs:325 set_hooks` (traces on) | WIRED |
| retention pass | snapshot / upload floors | `FloorWatches` watch channels | WIRED |
| `ostro restore` / `GET /v1/admin/backup` | `ostro_backup::restore` / `backup_node_to_tar` | main.rs, control/admin.rs | WIRED |

### Requirements Coverage

| Requirement | Description | Status | Evidence |
|-------------|-------------|--------|----------|
| TRIG-01 | `POST /hooks/{name}` with Stripe, GitHub, Slack and generic HMAC verification + idempotency | SATISFIED | SC1 row |
| TRIG-02 | TZ cron, exactly once per `(schedule, fire_time)` | SATISFIED | SC2 row; `cron_exactly_once` |
| TRIG-03 | Overlap, catch-up, pause, trigger-now | SATISFIED | SC2 row |
| BACK-01 | Leader-only sealed segments below HW, snapshots, meta snapshot and blobs to object storage | SATISFIED (S3 external leg human_needed) | Uploader steps 1-8; blob GC with delayed remote deletion |
| BACK-02 | Restore from bucket, RPO <= seal interval, hash match | PARTIAL | Hash match verified; RPO bound relaxed by P8-D23 (gap) |
| BACK-03 | backup/restore to a local file | SATISFIED | `backup_cli` (2), restore_rpo file round trip, online admin tar |
| BACK-04 | One snapshot format; trim coordinates with snapshots | SATISFIED | SC4 row (campaign pending) |
| OBS-01 | Traces with traceparent in and out; no replay re-emit | SATISFIED | SC5 row |
| OBS-02 | Prometheus `/metrics` series | SATISFIED | SC5 row; docs/metrics.md (64 series) |
| OBS-03 | Guest logs structured, rate-limited, bounded, forwarded | SATISFIED | SC5 row |

No orphaned requirements: every Phase 8 ID in REQUIREMENTS.md is claimed by a plan. The REQUIREMENTS checkboxes still read Pending; this verifier did not edit them, as instructed.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| crates/ostro-server/src/config.rs | 749 | `[triggers] webhook_tolerance` parsed, never read (verifier falls back to `DEFAULT_TOLERANCE_S = 300`) | Warning | Dead config knob; documented in docs/webhooks.md. Fix: pass it as the fallback in `VerifierSpec::from_meta` when `tolerance_s == 0`, or remove the key |
| crates/ostro-partition/tests/trim_safety.rs | 50 | `Node::open` 120 s timeout flakes under load (> 20) | Info | Environmental; the shrunk case passes alone |
| (08-20 finding) armed server with a failed partition | - | never prints its ready line within 60 s | Info | Failpoint-armed scenario only; clean restarts fine; open follow-up |
| pre-Phase-8 code (executor, server bootstrap, dispatcher, shuffler, machine) | - | direct time reads | Info | Phase 10 F5 follow-up, out of Phase 8 scope |

The TODO/FIXME/`unimplemented!`/stub scan over the Phase 8 source files found nothing.

### Human Verification Required

1. **10,000-fault Phase 8 campaign: PENDING.** It is running now with state in `target/campaign-p8-gate`. At verification time the shard files showed 541 kills (shards 0-5) with every violation tally at 0. Gate condition: `kills >= 10000`, `lost_acked_runs=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0`, `snapshot_required_with_valid_snapshot=0`, and `restore_checked` on every shard. Write the report to `target/campaign-p8-gate/report.txt`.
2. **SeaweedFS S3 legs:** `ostro-objstore --test s3`, `ostro-backup --test fencing` (s3_fencing) and `ostro --test bottomless_s3`, on Docker or in CI after the first push.
3. **R2/AWS conditional PUT** with the same tests.
4. **Fresh-machine `ostro restore --from s3://...`** with timing recorded.
5. **Jaeger/Tempo + Grafana** visual check of the span tree and dashboards.
6. **Scratch-image TLS roots** (Phase 14).
7. **User review** of P8-D3/D9/D11/D16/D21/D25 and **P8-D23** (tied to the gap).

### Gaps Summary

One gap, and it is a definition gap more than missing code. Bottomless restore exists, is wired and is verified end to end (hash-checked, every pre-cut ack restored). The literal SC3/BACK-02 bound "RPO <= segment seal interval (target <= 1 s)" is not met, though. The measured worst-case loss is seal + upload latency (max 2.45 s at seal 1 s). The plan-time decision P8-D23 narrowed "RPO" to cut lag and still allowed 500 ms slack on it, without user sign-off. Close the gap either by user ratification and rewording (Option A) or by an uploader cadence/parallelism change plus a loss-window assertion (Option B).

Separately, Phase 8 cannot be marked passed until the 10,000-fault campaign finishes clean. All other success criteria are verified in code and by green tests from the gate and from spot runs.

---

_Verified: 2026-11-20T23:29:19Z_
_Verifier: Claude (gsd-verifier)_
