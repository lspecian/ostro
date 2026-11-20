---
phase: 08-triggers-bottomless-observability
plan: 20
subsystem: testing
tags: [sc1, sc2, sc3, sc4, webhooks, cron, bottomless, restore, rpo, crash-campaign, seaweedfs]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: "08-12 CLI verbs; 08-13 uploader; 08-14 cron driver + schedules controller; 08-15 webhooks controller; 08-16 metrics; 08-17 guest logs; 08-18 restore + backup files; 08-19 blob GC; 05-19 whole-server campaign"
provides:
  - "guests/hook-order (Stripe checkout session -> wait signal `paid` -> {session, paid_at, payment})"
  - "crates/ostro-server/tests/triggers_e2e.rs: stripe_round_trip, github_slack_hmac_hooks, cron_cli_policies (SC1, SC2/TRIG-03 via the spawned CLI)"
  - "crates/ostro-crashtest/tests/cron_exactly_once.rs (#[ignore], crash profile, SC2 kill loop)"
  - "crates/ostro-crashtest/tests/restore_rpo.rs (#[ignore], crash profile, SC3 RPO loop + backup file round trip)"
  - "crates/ostro/tests/bottomless_s3.rs + ci.yml 'bottomless restore leg against S3 (SeaweedFS)' step"
  - "ostro_crashtest::campaign Phase 8 scenarios (OSTRO_CAMPAIGN_PHASE8=1): PHASE8_MENU, phase8_setup, pick_variant_cfg, restore_bucket; workload::{phase8_env, plant_orphan_blobs, drop_partition_states, has_local_snapshot, TRIM_WAIT}; ServerTarget::spawn_env"
affects: [08-22 gate]

tech-stack:
  added: []
  patterns:
    - "Crash loops read the dead data dir offline (Inspect node + runs_page) for exact idempotency-key evidence"
    - "Campaign config overrides via OSTRO_<SECTION>__<KEY> env on the spawned server (spawn_env)"

key-files:
  created:
    - guests/hook-order/Cargo.toml
    - guests/hook-order/src/lib.rs
    - crates/ostro-server/tests/triggers_e2e.rs
    - crates/ostro-crashtest/tests/cron_exactly_once.rs
    - crates/ostro-crashtest/tests/restore_rpo.rs
    - crates/ostro/tests/bottomless_s3.rs
  modified:
    - crates/ostro-crashtest/src/campaign.rs
    - crates/ostro-crashtest/src/workload.rs
    - crates/ostro-crashtest/src/server_target.rs
    - .github/workflows/ci.yml

key-decisions:
  - "Phase 8 campaign scenarios are opt-in (OSTRO_CAMPAIGN_PHASE8=1) and part of the shard resume identity; with it off the 05-19 variant mix is bit-identical (unit-tested)"
  - "Cut lag (P8-D23) measured from the file:// bucket: min(kill, first object of the in-flight round) - mtime(manifest/current); an over-estimate, asserted <= seal + 500 ms"
  - "Acked-before-cut criterion: ack_ms + 50 ms < cut_at_ms (acks arrive after apply; the cut is read before cut_at_ms)"
  - "Trim scenario waits for the fixed 60 s leader retention interval (no server knob); blob GC scenario plants 20 old orphan blobs so the first pass has unlink work; install scenario drops every state/pNNNN and arms 1*return"
  - "restore_rpo / cron_exactly_once / campaign spawn `ostro restore` and `ostro dev` through the failpoint binary, so ostro-crashtest gains no dependency (no Cargo change)"

requirements-completed: [TRIG-01, TRIG-02, TRIG-03, BACK-01, BACK-02, BACK-03, BACK-04]

duration: ~3h
completed: 2026-11-20
---

# Phase 8 Plan 20: End-to-end proofs SC1–SC4 Summary

**SC1 (Stripe checkout -> payment round trip, duplicates applying nothing, GitHub/Slack/HMAC hooks, tampered = 401 and no run), SC2 (CLI-driven overlap/trigger/pause/resume/catch-up plus 20 kill -9 restarts with every fire_time exactly once), SC3 (10 kill/restore iterations with every pre-cut ack restored, cut lag within seal + slack, backup file round trip; S3 leg wired in CI) and SC4 (snapshot/trim/upload/GC/install kills in the 05-19 campaign, 200-fault smoke green with 0 lost acked runs and rebuild-digest equality) each have an automated proof.**

## SC1 / SC2: crates/ostro-server/tests/triggers_e2e.rs (3 passed, 17 s)

- `stripe_round_trip`: webhook A (Stripe, `start hook-order`, input `/data/object`, idempotency `json:/data/object/id`) and B (Stripe, `signal paid`, `run_by idempotency_pointer /data/object/id`). A: created; A again (re-signed): same run, `created=false, duplicate=true`. Run publishes `{session}` and waits. B: applied; immediate B redelivery: 200 duplicate (SignalSeen) or 409 `run_terminal`; after completion B again: 409 `run_terminal`; A again: same run. Result `{"session":"cs_test_a1","paid_at":1700000123,"payment":"cs_test_a1"}`, exactly one run, exactly one `SignalPayload` in the journal, tampered B = 401.
- `github_slack_hmac_hooks`: tampered first (401, no run), valid (created), duplicate, tampered again (401); exactly 3 runs with the echoed bodies; Slack challenge echoed as text/plain, wrong signature 401.
- `cron_cli_policies` (spawned `ostro schedules ... --json`, the binary built once into the same target dir): create 1 s `America/New_York` schedule overlap Skip / catch-up One, `get`/`ls` show it; Skip keeps exactly one live run over 2.5 s; `trigger` while live = exit 1, 409; `pause`, no fires for 2.5 s; `trigger --request-id` while paused starts a manual run; a retry with the same request id after it finished returns the same run; `resume` fires exactly one catch-up instant near the resume time; second schedule (AllowAll / One, sdk-echo): stop 4 s + restart = at most one fire inside the downtime, then every second again with no duplicate fire time.

## SC2 kill loop: cron_exactly_once (crash profile)

`cargo nextest run -P crash -p ostro-crashtest --run-ignored only --locked cron_exactly_once` (release failpoint binary, unarmed): **PASS, 123 s**.

| kills | fires | duplicates | span | downtime total | missed (all inside downtimes) | gaps | downtimes with a gap |
|---|---|---|---|---|---|---|---|
| 20 | 92 | 0 | 117 s | 46 s | 25 | 13 | 13 (max 1 gap each = catch-up One) |

(An earlier run without the 0–4 s extra downtime: 20 kills, 79 fires, 0 duplicates, 1 gap.)

## SC3: restore_rpo (crash profile) — RPO table

`cargo nextest run -P crash -p ostro-crashtest --run-ignored only --locked restore_rpo`: **PASS, 193 s**. P=16, seal 1 s, snapshots 2 s / 64 KiB, 6 clients of crash-saga starts, kill at 3–7 s, release binary, load average ~10 from parallel executors.

| iter | gen | acked | missing (all acked after the cut) | cut lag ms | upload latency ms | loss window ms | kill − cut ms |
|---|---|---|---|---|---|---|---|
| 0 | 3 | 1967 | 447 | 523 | 645 | 1168 | 1168 |
| 1 | 4 | 2531 | 243 | 74 | 591 | 661 | 665 |
| 2 | 2 | 1847 | 895 | 1203 | 760 | 2449 | 2458 |
| 3 | 2 | 1667 | 708 | 1088 | 697 | 1785 | 1785 |
| 4 | 3 | 2366 | 648 | 1044 | 582 | 1680 | 1680 |
| 5 | 1 | 1225 | 883 | 1056 | 599 | 2308 | 2308 |
| 6 | 2 | 1401 | 436 | 427 | 764 | 1192 | 1191 |
| 7 | 2 | 1400 | 470 | 508 | 681 | 1188 | 1189 |
| 8 | 1 | 1163 | 829 | 1064 | 648 | 2219 | 2221 |
| 9 | 10 | 6812 | 390 | 573 | 849 | 1422 | 1422 |

- **Cut lag** (asserted ≤ seal 1000 ms + 500 ms slack): p50 **1044 ms**, max **1203 ms**. The p50 slightly above 1 s is the over-estimating bound (first object of the in-flight round) plus tokio sleep jitter.
- **Upload latency** (restored round, manifest/current mtime − cut_at_ms): p50 **681 ms**, max **849 ms**; all rounds from `ostro_backup_upload_seconds` (31 samples): p50 **733 ms**, max **2030 ms**.
- **Data-loss window** (newest lost ack − cut_at_ms): p50 **1680 ms**, max **2449 ms** ≈ seal + upload latency (P8-D23).
- (a) every restore exit 0 (state hashes + consistency); (b) no ack older than the cut was missing in any iteration (22,379 acks, 5,949 newer than the cut); (d) every partition's cut_offset ≤ the killed source's recovered applied offset, the leader's metrics show chunk bytes, an Inspect open of the killed dir leaves the bucket listing unchanged.
- Backup file round trip (iteration 9): `ostro backup --to f.tar --server <live>` then `ostro restore --from f.tar`: 16 partitions, 2,045 runs, restore 13.4 s.
- Uploads are still sequential per partition (08-13); not needed: cut lag holds at seal 1 s.

## SC3 S3 leg

`crates/ostro/tests/bottomless_s3.rs`: server backing up to `s3://$OSTRO_TEST_S3_BUCKET/bottomless-s3/<pid>-<nanos>` (endpoint, path style, allow_http, creds), 8 runs, SIGKILL, `ostro restore --from s3://... --endpoint --region --allow-http --json` (creds from `OSTRO_BACKUP_*` env), 4 partitions verified, no secret in output, a server on the restored dir serves all runs. ci.yml: new step "bottomless restore leg against S3 (SeaweedFS)" right after the objstore S3 step (gnu leg, fails if the env is missing). **Locally: `SKIP (human_needed): OSTRO_TEST_S3_URL unset`** (no Docker) — human_needed / CI.

## SC4: Phase 8 campaign scenarios

`OSTRO_CAMPAIGN_PHASE8=1` (see `campaign.rs` module docs): every shard server runs with `phase8_env` (snapshots 1 s / 500 ms min / 32 KiB / keep 2; backup `file://<shard>/bucket`, seal 500 ms, remote keep 2; blob GC 2 s, grace 30 s). Mix: plain 50 / failpoint 18 / enospc 7 / phase8 25. Menu: `snapshot::before_rename=return`, `snapshot::before_dir_fsync=return`, `snapshot::before_rename=sleep(300)`, `trim::after_delete=return` (waits ≤ 75 s for the 60 s retention pass), `upload::before_manifest=return`, `upload::before_manifest=sleep(300)`, `blobgc::before_unlink=sleep(200)` (after planting 20 old orphans), `snapshot::install_before_swap=1*return` (after dropping every state/pNNNN). New invariants: clean restart never logs `snapshot required`; `rebuild_and_compare` after every phase8 kill; end-of-shard `ostro restore --from file://<shard>/bucket` must pass (`restore_checked`, report fails otherwise).

### 200-fault smoke (4 shards × 50, parallel 4, seed 0x0820, debug harness, release failpoint binary)

Recorded run (final code, wiped `target/campaign-p8-smoke`, 1,361 s wall):

```text
kills=200 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0
faults_target=200 shards=4/4 incomplete=[] by_variant=plain:95 failpoint:43 enospc:10 phase8:52 failpoint_hits=259 extra_kills=109 rebuilds=61 acked_runs=5567 phase8_scenarios=true
shard  kills  plain  fp  enospc  hits  extra  rebuilds  runs  kill_s  s/kill  final
    0     50     22  11       2    81     29        17  1648    1198   23.96  ok
    1     50     24  10       1    95     27        18  2031    1309   26.18  ok
    2     50     25  10       3    12     26        14   740     745   14.91  ok
    3     50     24  12       4    71     27        12  1148     671   13.42  ok
phase8 sites: snapshot::before_rename=return 8, snapshot::before_rename=sleep(300) 8,
  snapshot::before_dir_fsync=return 4, trim::after_delete=return 6,
  upload::before_manifest=return 9, upload::before_manifest=sleep(300) 8,
  blobgc::before_unlink=sleep(200) 7, snapshot::install_before_swap=1*return 2
bucket restores: generation 71 / 130 / 5 / 46, 9.7-12.2 s each, all verified
variant_fallbacks 0, self_exits 0, no 'snapshot required' on any clean restart
RESULT: PASS
```

Commands: `OSTRO_CAMPAIGN_PHASE8=1 OSTRO_CAMPAIGN_STATE_DIR=target/campaign-p8-smoke OSTRO_CAMPAIGN_FAULTS=200 OSTRO_CAMPAIGN_SHARDS=4 OSTRO_CAMPAIGN_PARALLEL=4 OSTRO_CAMPAIGN_SEED=0x0820 OSTRO_FAILPOINT_BIN=<release failpoint ostro> cargo test -p ostro-crashtest --test server_campaign --locked -- --ignored --nocapture campaign`, then the same with `report`.

First run (menu with `install_before_swap=return`): `kills=200 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`, by_variant plain 96 / failpoint 43 / enospc 10 / phase8 51, failpoint_hits 278, rebuilds 60, acked runs 5,484, 4 bucket restores ok (generations 50/4/68/135, 9.9–14.9 s), 1,243 s wall, RESULT: PASS. Phase 8 sites: snapshot::before_rename=return 8, before_rename=sleep 8, before_dir_fsync 4, trim 6, upload return 9, upload sleep 8, blobgc 7, install 1 (+1 plain fallback: with every install failing, the trimmed partition 13 reported `snapshot required (applied 0 below log start 62, no usable local snapshot)` on the **armed** server, which never printed its ready line, 5 × 60 s; the clean restart then installed fine). That is why the entry is now `1*return`.

### Gate command (08-22, 10,000 faults)

```text
export OSTRO_CAMPAIGN_PHASE8=1 OSTRO_CAMPAIGN_STATE_DIR=target/campaign-p8-gate \
       OSTRO_CAMPAIGN_FAULTS=10000 OSTRO_CAMPAIGN_SHARDS=20 OSTRO_CAMPAIGN_PARALLEL=6
for ids in 0-5 6-11 12-17 18-19; do
  OSTRO_CAMPAIGN_SHARD_IDS=$ids cargo test --release -p ostro-crashtest --features failpoints \
      --test server_campaign --locked -- --ignored --nocapture campaign
done
cargo test --release -p ostro-crashtest --features failpoints --test server_campaign --locked -- --ignored --nocapture report
```

Note: 08-VALIDATION's gate command lacks `OSTRO_CAMPAIGN_PHASE8=1`; without it the campaign runs the 05-19 mix only. Projection: the smoke ran 12–24 s per kill (trim waits, rebuild after every phase8 kill, a loaded machine), so 500 kills per shard ≈ 2–3 h per batch of 6, ≈ 8–12 h total; resumable per batch.

## Task Commits

1. **Task 1 (hook-order guest, triggers_e2e, cron_exactly_once):** `7fd745e`
2. **Task 2 (restore_rpo, bottomless_s3, campaign scenarios, ci.yml):** `4ccdbdb`

## Verification

- `cargo test -p ostro-server --test triggers_e2e --locked`: ok, 3 passed.
- `cargo build -p ostro-crashtest --tests --locked`: ok; `cargo test -p ostro-crashtest --lib`: 9 passed (incl. the two new campaign tests).
- `cargo test -p ostro --test bottomless_s3 --locked`: ok, 1 passed (SKIP, human_needed).
- cron_exactly_once and restore_rpo: see above (PASS).
- `cargo clippy -p ostro-crashtest --all-targets --locked -D warnings` (with and without `--features failpoints`), `-p ostro-server --test triggers_e2e`, `-p ostro --test bottomless_s3`: clean. rustfmt clean on every touched file; `cargo fmt` clean for the guest. ci.yml parses (yaml).
- `cargo test -p ostro-crashtest --locked`: every binary ok (lib 9, check_controls 16, the rest ignored by design); `cargo test -p ostro-crashtest --features failpoints --locked -- --test-threads=1`: same, ok.
- **Not run:** `cargo test --workspace` and `bash scripts/ci-local.sh --no-musl` (06-20's final ci-local gate and 08-21 were running; the brief keeps test runs sequential and builds capped). Gate at 08-22.

## Deviations from Plan

1. **[Rule 3] `crates/ostro-crashtest/src/server_target.rs` (not in files_modified):** `ServerTarget::spawn_env` (extra env after the `OSTRO_*` strip) so campaign/RPO servers get `OSTRO_<SECTION>__<KEY>` config; `spawn` delegates to it. 17 lines.
2. **`guests/Cargo.lock` not committed:** it is gitignored (guests/Cargo.toml: "guests/Cargo.lock is not committed").
3. **CI backend is SeaweedFS, not MinIO** (P8-D25, user direction); the plan text still says MinIO.
4. **(d) "follower/Inspect uploads nothing"** is shown as: single-node leader metrics show chunk uploads, and an Inspect (closed-barrier) open of the killed dir leaves the bucket byte-identical; the per-round `barrier_closed` refusal stays covered by 08-13's `tests/uploader.rs`. Chunk end ≤ HW = restored cut_offset ≤ recovered applied offset per partition.
5. **Trim scenario cost:** retention has no server knob (fixed 60 s leader interval), so each trim iteration takes ~65–75 s.
6. **cron_cli_policies uses the in-process TestServer's partition reader** for fire-time evidence (idempotency keys are not exposed over REST).

## Findings (not fixed, out of scope)

- **Trigger-now retry under overlap Skip:** a repeated `--request-id` while the manual run it started is still live answers 409 `schedule_overlap` instead of the existing run (apply checks overlap before the manual dedupe row, `apply/schedule.rs`). After the run ends the retry returns the same run. Candidate fix: look up the manual dedupe row before the overlap check.
- **Signal hook redelivery after the run finished** answers 409 `run_terminal` (complete_token maps the same case to 200 duplicate). Providers retry non-2xx for days; consider mapping it to 200 duplicate when the request id was seen.
- **Armed server with a failed partition never prints its ready line** within 60 s (install_before_swap=return scenario); clean restarts are unaffected.

## Self-Check: PASSED

## Post-plan fixes

Two of the findings above are fixed:
- 826c592 `fix(08-07)`: a trigger-now retry under overlap Skip/BufferOne returns the existing run. `cron_cli_policies` now also retries while the manual run is live.
- 2123663 `fix(08-15)`: a signal hook redelivered after the run finished gets a 200 duplicate when its signal applied, and 409 `run_terminal` otherwise.

The "armed server with a failed partition" finding is still open.
