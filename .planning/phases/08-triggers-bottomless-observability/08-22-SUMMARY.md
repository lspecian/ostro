---
phase: 08-triggers-bottomless-observability
plan: 22
subsystem: phase-gate
tags: [phase-gate, ci-local, campaign, reservations, clock-gate, docs, trig-01, trig-02, trig-03, back-01, back-02, back-03, back-04, obs-01, obs-02, obs-03]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: "08-01..08-21 (triggers, bottomless, observability, SC1-SC5 e2e, Phase 8 campaign scenarios, B2 outbound traceparent, OTLP canary)"
provides:
  - "OTLP span layer excludes the exporter's own crates (22d323a)"
  - "clock gate: every Phase 8 time read is the injected clock or `// clock-exempt:`"
  - "RESERVATIONS Phase 8 names flipped to exists (P8, 08-NN); unused P8 slots reserved"
  - "docs finalised: storage-format §15.1, snapshot-format v1 (Phase 12 checklist), bottomless, observability, metrics, webhooks, docs/03"
  - "08-DECISIONS Spike outcomes; 08-VALIDATION filled (Gate results, Human-needed, corrected campaign command)"
affects: [phase-8-verification, 10, 12]

tech-stack:
  added: []
  patterns:
    - "Phase gate clock grep: SystemTime::now/SystemClock/Instant::now in Phase 8 code only in clock.rs or with a trailing `// clock-exempt: <reason>`"

key-files:
  created: []
  modified:
    - crates/ostro-server/src/logging.rs (gate fix, 08-04)
    - crates/ostro-server/src/server.rs (clock-exempt, 08-04)
    - crates/ostro-server/src/services/guest_logs.rs (clock-exempt, 08-17)
    - crates/ostro-backup/src/restore.rs (clock-exempt, 08-18)
    - crates/ostro-server/src/services/blob_gc.rs (clock-exempt, 08-19)
    - crates/ostro-server/src/control/watch.rs (clock-exempt, 05-13)
    - .planning/RESERVATIONS.md
    - .planning/phases/08-triggers-bottomless-observability/08-DECISIONS.md
    - .planning/phases/08-triggers-bottomless-observability/08-VALIDATION.md
    - .planning/STATE.md
    - docs/storage-format.md
    - docs/snapshot-format.md
    - docs/bottomless.md
    - docs/metrics.md
    - docs/observability.md
    - docs/webhooks.md
    - docs/03-architecture.md

key-decisions:
  - "OBS-01 is green: 08-21 wired B2 (TraceEffectHooks, BRIDGE_TRACEPARENT_LEG: wired), so there is no blocking follow-up"
  - "The 10,000-fault Phase 8 campaign is human_needed and resumable: 541 of 10,000 kills (shards 0-5, partial) with 0 violations; the session cannot run the remaining ~8-12 h"
  - "Pre-Phase-8 direct time reads (executor, server bootstrap, effects, dispatcher, shuffler, machine, node) are a Phase 10 F5 follow-up and are not edited at this gate"
  - "08-VALIDATION's campaign command now sets OSTRO_CAMPAIGN_PHASE8=1 and runs shard batches (08-20 command); without the flag only the 05-19 mix runs"

requirements-completed: [TRIG-01, TRIG-02, TRIG-03, BACK-01, BACK-02, BACK-03, BACK-04, OBS-01, OBS-02, OBS-03]

duration: 2h25m (this run; gate 2h03m, loops 6.6 min)
completed: 2026-11-20
---

# Phase 8 Plan 22: Phase 8 Gate Summary

**Phase 8 gate: one full musl ci-local run, all 21 steps PASS (plus the two external MySQL/S3 SKIP rows, human_needed) with the PG legs really run against a private PG 14. The clock gate passes on Phase 8 code. The RESERVATIONS re-check passes (66 checks) with the shipped P8 names flipped, and the docs and spike outcomes are folded in. The 10,000-fault Phase 8 campaign is human_needed: 541 kills so far, 0 violations, resumable.**

## Performance

- **Completed:** 2026-11-20
- **Tasks:** 2/2 (the campaign item is human_needed)
- A machine restart cut off the earlier attempt during its final ci-local run. The gate fixes, the RESERVATIONS re-check and the docs had already landed by then. This run made one full ci-local pass, shared with 06-20, on main `5faa97c`. It then ran the `#[ignore]` loops and filled VALIDATION, STATE and this SUMMARY.

## Gate table (ci-local, full, musl)

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | 0 s |
| cargo fmt --check | PASS | 2 s |
| cargo clippy -D warnings | PASS | 55 s |
| cargo build (warning-free) | PASS | 86 s |
| sdk workspace fmt + clippy | PASS | 21 s |
| sdk workspace test + wasm build | PASS | 23 s |
| guests build (wasm32-wasip2) | PASS | 12 s |
| order-saga example | PASS | 21 s |
| external: OSTRO_TEST_MYSQL_URL | SKIP (human_needed) | - |
| external: OSTRO_TEST_S3_URL | SKIP (human_needed) | - |
| cargo test | PASS (1,959 passed) | 2164 s |
| semantics sleepers (serial) | PASS | 39 s |
| failpoint list sync (ci.yml) | PASS | 0 s |
| failpoint suites | PASS (956 passed) | 2093 s |
| server campaign smoke (20 kills) | PASS | 413 s |
| cargo deny check | PASS | 1 s |
| check-vendored.sh | PASS | 1 s |
| check-deny-fixtures.sh | PASS | 1 s |
| no chronik/tokio-uring/prometheus | PASS | 0 s |
| workflow validation | PASS | 2 s |
| zig + cargo-zigbuild present | PASS | 0 s |
| musl release (static, mimalloc) | PASS | 311 s |
| musl zigbuild test | PASS (1,959 passed) | 2146 s |

`all steps passed`, exit 0. Run on main `5faa97c` (worktree, fresh `CARGO_TARGET_DIR`), 2026-11-20 21:50–23:53 CET (about 2 h 03 min). `OSTRO_TEST_PG_URL` was set (banner: `set: OSTRO_TEST_PG_URL (ostro-bridge sql_pg, ostro fx_sql_crash pg_after_commit/pg_before_user_stmt)`), pointing at a private throwaway PostgreSQL 14.24 cluster (127.0.0.1:55468, scram, data dir in the session scratchpad, loaded via the untracked `.ostro-test-env`; nothing committed).

PG legs that really ran:
- `ostro-bridge --test sql_pg`: 10 passed in `cargo test` and 10 passed in `musl zigbuild test`; 13 passed in `failpoint suites` (with the three `failpoints::pg_*` cases).
- `ostro --features failpoints --test fx_sql_crash`: `pg_after_commit` and `pg_before_user_stmt` passed in `failpoint suites`. Re-run alone with `--nocapture`: `N=50 held in 452 ms, restart (readyz) 164 ms, all terminal 471 ms after kill -9` and `N=50 held in 533 ms, restart 170 ms, all terminal 470 ms`.
- The PG server recorded 1,344 committed transactions and 2,506 inserted tuples. The tests drop their own schemas.

The MySQL legs (`sql_mysql`, `fx_sql_crash mysql_*`) and the S3 legs passed as loud SKIPs.

Campaign smoke: `kills=20 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`, acked_runs=231, RESULT: PASS.

The musl release binary (`x86_64-unknown-linux-musl/release/ostro`) is 44,033,456 bytes (42.0 MiB), static, mimalloc. No 06-02 size baseline was recorded, so this number is the baseline from now on.

No step failed and no test needed a re-run.




## Dependency policy

- `cargo deny check`: PASS (ci-local step). The ci-local "no chronik/tokio-uring/prometheus" step covers `bash scripts/forbidden-crates.sh` and passed.
- `cargo tree --workspace -e normal --locked`: no `chrono`, `zstd-sys`, `openssl-sys` or `object_store`. Duplicates are the expected jiff 0.2/1.0 pair (P8-D9) plus the warned duplicates that were already there (base64, digest/sha2, hashbrown, rand, syn, thiserror, tower-http and others; deny warns and does not fail on them).
- FAILPOINT_CRATES equals the ci.yml "Failpoint suites" step, according to the "failpoint list sync (ci.yml)" step. It includes `ostro-backup` (08-01), `ostro:fx_redrive` and `ostro:fx_sql_crash` (06-18).

## Clock gate (Phase 10 F5)

- The plan's verify grep returns no hits. It covers `crates/ostro-backup/src`, `ostro-triggers/src`, `ostro-objstore/src`, `ostro-server/src/{services,control,telemetry.rs,effect_trace.rs}` and `ostro-partition/src/{leader/snapshot.rs,leader/cron.rs,apply/schedule.rs}`.
- Exemptions in Phase 8 code (`// clock-exempt:`):
  - `ostro-backup/src/restore.rs` ×5: RestoreReport.durations;
  - `uploader.rs`: upload latency metric (W8);
  - `ostro-objstore/src/s3.rs`: transport retry backoff;
  - `leader/snapshot.rs`: metric duration only;
  - `store/snapshot.rs`: spike timing;
  - `ostro-blob/src/lib.rs`: filesystem mtime domain;
  - `services/blob_gc.rs` ×4: the pass duration metric and three test wait deadlines;
  - `services/guest_logs.rs`: default only, with the injected clock supplied by server.rs;
  - `services/metrics_poll.rs`: sampling cadence;
  - `telemetry.rs`: recorder maintenance cadence;
  - `server.rs:195`: the ServerOptions default clock (the injection root);
  - `control/watch.rs` ×3: long-poll deadlines (05-13 code).
- The wide grep still has hits, all in **pre-Phase-8 code**. They are listed as a **Phase 10 follow-up** and were not edited:
  - `ostro-server/src/executor.rs:455,467,476,694` (heartbeat/deadline, Phase 3/5);
  - `server.rs:254,277,282,331,350,417,873` (04-17 bootstrap StartupTimings; line 253 carries a clock-exempt comment above the read, not trailing);
  - `effects.rs:338,471` (06-14 span timing; the exempt comment is on the line above, not trailing);
  - `ostro-partition/src/leader/{shuffler.rs:132,143, dispatcher.rs:255,479,485,1390}`;
  - `machine.rs:135,507,562` (02-11);
  - `lib.rs:36` and `node.rs:33,97,109` (the `SystemClock` re-export and default).

## Gate fixes (separate commits, owning plan)

| Commit | Owner | Fix |
|--------|-------|-----|
| 22d323a | 08-04 | OTLP span layer skips the exporter's own crates (`opentelemetry*`, `tonic`, `h2`, `tower`); test `span_layer_exclusions` (08-21 deviation 5) |
| 53116bc | 08-04 | clock-exempt the ServerOptions default clock (injection root) |
| 08e2593 | 08-17 | GuestLogSink::new default clock annotated; server.rs supplies the injected clock |
| 2d1238c | 08-18 | RestoreReport duration reads annotated |
| 879ff44 | 08-19 | blob GC test wait deadlines annotated |
| a452f46 | 05-13 | await_result long-poll deadline annotated (pre-Phase-8; Phase 10 review item) |


These landed on main while the gate was being prepared. They come from other plans and are covered by this gate's run:
- 826c592 (08-07): a manual trigger retry is answered from its dedupe row before overlap Skip/BufferOne is applied.
- 2123663 (08-15): a signal hook redelivered after the run finished gets a 200 duplicate.
- eb89cea (06-11): PG numeric keeps its scale.
- 7aa47f9: server tests use a shared OPEN_T of 120 s.
- 6983bb2: the pipelined-grants test race fix.

## RESERVATIONS (04cedcb)

- The reservations script passes: **RESERVATIONS CHECK: PASS (66 checks)**. Every Cmd tag, keyspace and InvalidCode in code is `exists` or a placeholder.
- Cmd 27 ScheduleFire, keyspace 16 schedule_state and InvalidCode 22/23 were flipped by 08-01 and are confirmed here.
- The §11 names that shipped now read `exists (P8, <plan>)`:
  - the hook route and the `__cron/` / `__hook/` namespaces;
  - the six config sections;
  - the failpoint prefixes `snapshot::`, `trim::`, `upload::`, `restore::` and `blobgc::`;
  - the REST paths and the gRPC services Schedules/Webhooks/RunLogs;
  - node.toml `incarnation` / `restored_from`;
  - the bottomless object prefixes;
  - the data-dir entries;
  - `ostro.schedule`, `OSTRO_BACKUP_*`, and the crates.
- Unused slots stay `reserved P8`, including the `cron::` failpoint prefix, which has no site.

## Crash campaign (Phase 8 scenarios, 10,000 faults): human_needed

- State dir: `target/campaign-p8-gate` (main checkout). It was configured with SHARDS=20, FAULTS=10000, seed 1305, `phase8: true`, and a quota of 500 kills per shard.
- Progress before the restart: shards 0–5 only (the first batch), all partial. Kills per shard: 73 / 95 / 90 / 94 / 99 / 90, **541 total**, about 1,700 s of kill time per shard.
  - Every shard tally: `lost_acked_runs=0`, `lost_acked_signals=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0`, `wrong_results=0`, `other_violations=0`; no failures or anomalies; `self_exits=0`; `variant_fallbacks=0`.
  - Phase 8 sites hit: `snapshot::before_rename` (return/sleep), `snapshot::before_dir_fsync`, `snapshot::install_before_swap=1*return`, `trim::after_delete`, `upload::before_manifest` (return/sleep), `blobgc::before_unlink`.
- The remaining ~9,460 kills project to about 8–12 h (08-20 projection: 12–24 s per kill). That does not fit this session alongside the shared gate. `target/campaign-p8-gate/HUMAN_NEEDED` records the shard state and the resume command (08-VALIDATION "Human-needed"):

```text
export OSTRO_CAMPAIGN_PHASE8=1 OSTRO_CAMPAIGN_STATE_DIR=target/campaign-p8-gate \
       OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 OSTRO_CAMPAIGN_PARALLEL=6
for ids in 0-5 6-11 12-17 18-19; do
  OSTRO_CAMPAIGN_SHARD_IDS=$ids cargo test --release -p ostro-crashtest --features failpoints \
      --test server_campaign --locked -- --ignored --nocapture campaign
done
cargo test --release -p ostro-crashtest --features failpoints --test server_campaign --locked \
    -- --ignored --nocapture report > target/campaign-p8-gate/report.txt
```

- Gate when finished: `kills >= 10000`, `lost_acked_runs=0`, `duplicate_journal_entries=0`, `non_terminal_runs=0`, `snapshot_required_with_valid_snapshot=0`, and `restore_checked` on every shard.
- Smaller-scale evidence already in hand: the 08-20 Phase 8 campaign smoke (200 faults, 4 shards: PASS, 0 violations, 4 bucket restores verified) and this gate's ci-local "server campaign smoke (20 kills)".

## `#[ignore]` loops (re-run at the gate)

`cargo nextest run -P crash -p ostro-crashtest --run-ignored only --locked -E 'test(=cron_exactly_once) | test(=restore_rpo) | test(=crash_loop_snapshots)'` ran on the gate build (release failpoint binary, quiet machine, after ci-local): **3/3 passed in 398.5 s**.

| Loop | Result |
|------|--------|
| `crash_loop_snapshots` (08-09 snapshot mode) | PASS, 127.7 s |
| `cron_exactly_once` (20 kill -9 restarts, every fire_time exactly once) | PASS, 117.7 s |
| `restore_rpo` (10 kill/restore iterations, every pre-cut ack restored) | PASS, 153.1 s |

## Docs and decisions (5faa97c)

- **storage-format:**
  - §1/§10/§15 data-dir tree: `.old-pNNNN`, `.tmp-*`, `backup/gc-ledger`;
  - the trim floor `min(ckpt, local, upload?, retention?)` with `log_start <= oldest retained snapshot`;
  - §15.1, the final on-disk rules.
- **snapshot-format:** "v1, designed for Phase 12 follower install", with a remote-layout cross-reference and a §11 Phase 12 checklist (frame streaming, resumption, epoch history, divergent-tail truncation locally and in object storage, HW publication).
- **bottomless:** `[snapshots]` and `[blob_gc]` keys checked against config.rs, plus the measured RPO, upload latency and restore timings.
- **observability:** span-layer exclusions, the `effect:<kind>` install condition, and outbound traceparent (B2).
- **metrics:** 64 series re-checked against the code, no gaps.
- **webhooks:** `[triggers] webhook_tolerance` is parsed but not wired (a finding).
- **docs/03:** OSNP + OSEG + ostro-objstore replace the deprecated `chronik-storage::object_store` and periodic-fjall-snapshot statements.
- **08-DECISIONS "Spike outcomes":** croner resolution, ingestion vs WriteBatch, s3s-fs, conditional PUT and signing, OTel features, meta bootstrap, B6.

## OBS-01 (B2)

08-21 recorded `BRIDGE_TRACEPARENT_LEG: wired`. TraceEffectHooks puts the traceparent on the live HTTP copy and emits an `effect:<kind>` span, and the journal is unchanged. There is no STATE.md BLOCKING follow-up. OBS-01 therefore counts as complete, with outbound evidence in `outbound_traceparent` and `observability_e2e`.

## Task Commits

1. **Task 1 (gate):**
   - gate fixes: 22d323a, 53116bc, 08e2593, 2d1238c, 879ff44, a452f46;
   - RESERVATIONS: 04cedcb;
   - ci-local and loops: this run.
2. **Task 2 (docs/DECISIONS):** 5faa97c. VALIDATION and STATE: the 08-22 completion commit (VALIDATION, SUMMARY, STATE, ROADMAP).

## Deviations from Plan

1. **The campaign is human_needed rather than complete.** The plan allows this, provided it is recorded with explicit numbers and a resume command. 541 of 10,000 kills have run, with 0 violations.
2. **One shared ci-local run with 06-20.** Both plans' gate steps ran in a single full musl ci-local on the final code. Commits after `5faa97c` are docs/planning only.
3. **08-VALIDATION campaign command corrected.** It now sets `OSTRO_CAMPAIGN_PHASE8=1` and runs the shard batches with a `report` redirect (08-20 note).
4. **Wide clock-grep hits in pre-Phase-8 code** are listed as a Phase 10 follow-up, as the plan directs, and were not edited.


## Human-needed

The full list, with commands, is in 08-VALIDATION.md under "Human-needed":
1. the 10,000-fault campaign remainder (resume command above);
2. SeaweedFS conditional PUT and the S3 legs (`objstore s3`, `backup fencing`, `bottomless_s3`) on Docker or in CI after the first push;
3. R2/AWS conditional PUT;
4. a fresh-machine `ostro restore` from real S3, with timing;
5. a Jaeger/Tempo + Grafana visual check;
6. the scratch-image TLS roots (Phase 14);
7. user review of P8-D3/D9/D11/D16/D21.

The Phase 6 canary leg and the outbound traceparent leg are **not** pending: 08-21 ran both.

## Follow-ups

- **Phase 10 F5:** the pre-Phase-8 direct time reads listed above.
- **An armed server with a failed partition never prints its ready line** within 60 s (08-20 finding, still open; clean restarts are unaffected).
- **`[triggers] webhook_tolerance`** is parsed but not wired (docs/webhooks.md finding). Per-webhook `tolerance_s` applies.
- **08-18:** restored nodes have no local `.osnap` until the snapshot service takes one, and copying the installed remote snapshot would shorten that window. Restore time is dominated by fjall install fsyncs.
- **08-19:** the mark is consistent per partition only, not across partitions, and the ReputGuard set grows in memory (32 B per remotely deleted sha) until restart.
- **P8-D9:** croner pulls jiff 0.2 next to jiff 1.0 until croner supports jiff 1.

## Next Phase Readiness

Phase 8 is ready for `/gsd:verify-work`, with the campaign remainder human_needed. Per the brief, it was not marked complete here.
