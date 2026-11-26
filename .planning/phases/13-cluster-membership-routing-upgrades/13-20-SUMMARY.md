---
phase: 13-cluster-membership-routing-upgrades
plan: 20
subsystem: Phase 13 gate (CI wiring, grep gates, docs, reservations, requirements, validation)
tags: [phase-gate, ci, mutants, wit-next-fixture, tier-c, grep-gates, docs, reservations, requirements]
requires:
  - phase: 13-01..13-19
    provides: "every Phase 13 feature, the P13 mutant matrix (13-18), tier C (13-19)"
provides:
  - "ci-local + ci.yml: Phase 13 mutant matrix (OSTRO_P13_REQUIRE_ALL=1, >= 8 bites, no SKIP), engine wit-next-fixture test + clippy, Phase 13 tier C step, P13 grep gates, FAILPOINT_CRATES += ostro-server:{p13_boot_modes,p13_module_gc,p13_promote} (in sync)"
  - "scripts/p13-grep-gates.sh"
  - "docs/cluster-operations.md, docs/compat.md, docs/upgrade-runbook.md; Phase 13 sections in replication.md and storage-format.md (section 19)"
  - "RESERVATIONS P13 rows exist; REQUIREMENTS CLUS-05/06/07, ENG-10, API-08 complete with evidence; 13-VALIDATION filled (nyquist_compliant); 13-DECISIONS execution-time amendments; STATE user-review outcomes"
affects: [phase-13-verification, phase-14]
tech-stack:
  added: []
  patterns:
    - "Phase 13 grep gates in one shared script (ci-local + ci.yml lint job), like p12-grep-gates.sh; production-only scan (stops at a column-0 #[cfg(test)]) and a clock-exempt comment block directly above a line"
key-files:
  created:
    - scripts/p13-grep-gates.sh
    - docs/cluster-operations.md
    - docs/compat.md
    - docs/upgrade-runbook.md
  modified:
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - docs/replication.md
    - docs/storage-format.md
    - docs/metrics.md
    - docs/workers.md
    - docs/workflow-semantics.md
    - docs/bottomless.md
    - .planning/RESERVATIONS.md
    - .planning/REQUIREMENTS.md
    - .planning/STATE.md
    - .planning/ROADMAP.md
    - .planning/phases/13-cluster-membership-routing-upgrades/13-VALIDATION.md
    - .planning/phases/13-cluster-membership-routing-upgrades/13-DECISIONS.md
key-decisions:
  - "D-13-20-1: the P13 grep gates live in scripts/p13-grep-gates.sh (shared by ci-local check_p13_gates and the ci.yml lint job); scripts/p12-grep-gates.sh is untouched (no gate loosened). SetPartitionLeader over crates/ostro-server/src and the clock gate over crates/ostro-cluster/src were already covered by the P12 gates, so the P13 clock gate adds crates/ostro-server/src/{route,p13}"
  - "D-13-20-2: the vendored-driver Chronik gate allows exactly the negative assertion `!<x>.contains(\"CHRONIK\")` in the 13-05 driver test (the only hit); the journal@0.1.0 gate scans production code only (up to the first column-0 #[cfg(test)])"
  - "D-13-20-3: the tier-C step runs in ci-local (plan check group lint) and in ci.yml with OSTRO_UPGRADE_ITERS=3 and a 50-fault campaign, child runs on (the p13-children fix d33fce7 landed during this plan, so the temporary OSTRO_P13_CHILD_RUNS=0 was removed again in 7942813); the >= 10,000-fault campaign stays a manual gate run"
requirements-completed: [CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08]
duration: "about 6 h wall clock (two 80-min plan checks, a 3.6 h campaign, re-runs)"
completed: 2026-11-26
---

# Phase 13 Plan 20: Phase gate Summary

**The Phase 13 mutant matrix (8/8 bites, no SKIP), the engine `wit-next-fixture` suite, the tier-C multi-process step and new Phase 13 grep gates are now required in both `scripts/ci-local.sh` and `.github/workflows/ci.yml` (only added or tightened checks). Three new failpoint suites run in both, kept in sync. The full plan check at the final code commit 7942813 (after the child-run fix) is green apart from load-timeout flakes that pass on re-run. Tier C passes with child runs on. The gate campaign reached 1,110 faults before the pod's `/dev/shm` filled up (HUMAN_NEEDED). Operator docs, RESERVATIONS, REQUIREMENTS (all five Phase 13 requirements ticked with evidence), VALIDATION, DECISIONS and the user's review outcomes are recorded. Phase 13 is NOT marked complete here (orchestrator verification pending).**

## Commits

| commit | what |
|---|---|
| 9055589 | ci(13-20): Phase 13 mutant matrix + fixture suite + grep gates required |
| fd7b9df | docs(13-20): cluster operations, compatibility, upgrade runbook |
| 26ece7e | docs(13-20): user review outcomes of 2026-11-26 in STATE |
| 52290de | docs(13-20): RESERVATIONS P13 rows flipped to exists |
| 7942813 | ci(13-20): Phase 13 tier C runs with child runs on (TODO(p13-children) removed after d33fce7) |
| (final) | docs(13-20): REQUIREMENTS, VALIDATION, DECISIONS, SUMMARY, ROADMAP tick, STATE line |

## CI wiring (ci-local and ci.yml, same commands)

| step | ci-local | ci.yml |
|---|---|---|
| Failpoint suites | `FAILPOINT_CRATES += ostro-server:p13_boot_modes ostro-server:p13_module_gc ostro-server:p13_promote` | three `cargo test -p ostro-server --features failpoints --test p13_* --locked -- --test-threads=1` lines; `failpoint list sync` PASS |
| Phase 13 mutant matrix | `check_p13_mutants` | test job, linux-x86_64-gnu: `OSTRO_P13_REQUIRE_ALL=1 ... --test mutants_p13`, `test result: ok. [1-9]`, `>= 8` "FAILED as expected", `! grep -q SKIP` |
| WIT fixture lines (ENG-10) | `check_wit_next_fixture` | `cargo test -p ostro-engine --features wit-next-fixture --locked` (non-zero test count) + clippy `-D warnings` |
| Phase 13 tier C (multi-process) | `check_p13_tier_c` | failpoint binary in `target/failpoints`, then the seven tier-C tests `--release --ignored --test-threads=1 --nocapture`, `OSTRO_UPGRADE_ITERS=3`, `OSTRO_CAMPAIGN_FAULTS=50` |
| P13 grep gates | `check_p13_gates` -> `bash scripts/p13-grep-gates.sh` | lint job: same script |

`scripts/p13-grep-gates.sh` checks:
- no `journal@0.1.0"` literal in production code under `crates/ostro-engine/src/host/`;
- no Chronik text in `vendor/ostro-raft-driver/src` except the 13-05 negative assertion;
- no `InitCluster` in `crates/ostro-server/src/p13/joiner.rs`;
- the clock gate over `crates/ostro-server/src/{route,p13}`: no `SystemTime::now(`, `SystemClock` or `Instant::now(` unless a `clock-exempt:` comment is on the line or in the comment block directly above.

A negative self-test on a scratch copy (one injected violation per gate) failed all four with the expected message. The clean tree passes.

## Gate report

| gate | commit | result |
|---|---|---|
| plan check `ostro-k8s-ci --parallel` (ci-local incl. musl) | 9055589 | all PASS except `cargo test` and `musl zigbuild test`, red only on ostro-partition `trim_safety` (6 tests) / `snapshot_tail`: "node open timed out" under load. Fail-fast skipped the rest of each shard. **Re-run with `--no-fail-fast` (slot 1): both alone PASS, whole test2 shard PASS, whole musl2 shard PASS** |
| plan check at the final code commit | 7942813 | all PASS (incl. musl zigbuild test, musl size 57.7 MB) except `cargo test`: red only on ostro-partition `cron_driver::catch_up_skip_advances_the_watermark_without_runs` ("timed out after 10s waiting for 1 cron runs", test2 pod at 27 GiB peak). Re-run: PASS (cron_driver alone PASS; whole test2 shard with --no-fail-fast PASS, 373 test binaries ok; 20261126-104011-s0-7942813b-cmd.log) |
| P13 mutant matrix | both | 8 / 8 "FAILED as expected", 0 SKIP |
| Phase 12 mutant matrix, failpoint suites (incl. the 3 new), campaign smoke, phase 7 legs, deny, vendored, P12 + P13 grep gates, workflow validation | both | PASS |
| tier C (ci-local step) | 9055589 (child runs off), 7942813 (child runs on) | 7 / 7 PASS both times (`upgrade_from_prev` = SKIP line) |
| gate campaign `OSTRO_CAMPAIGN_FAULTS=10000`, 4 h box, `OSTRO_P13_REPORT_DIR=/tmp/ostro-p13-gate` | b47721e (child runs off; started before the fix) | **HUMAN_NEEDED: 1,110 faults reached** (phase A, finalized 1, 3 h 38 min, every 10-fault batch disk check green); `/dev/shm` in the pod full (fjall ENOSPC; partition logs never trimmed, `floor=0`), node 1 could not restart, harness aborted before the final oracles. Not an oracle failure |

Logs: `target/k8s-ci-logs/20261126-065142-par-9055589a-*.log`, `20261126-085958-s1-9055589a-cmd.log` (re-run), `20261126-091656-par-7942813b-*.log`, `20261126-065118-s0-b47721e0-cmd.log` (campaign).

## Measurements (7942813, child runs on, tier C in the 4-CPU lint pod on dell-1 under CI load)

```
process_promotion: time-to-RF-3 2.994620042s from the first join (two joins 1.01936206s, then RF 3 / ISR 3 / min_insync 2 + 3 voters after 1.975256252s)
  kill -9 node 1: every partition led by a survivor after 1.312194525s; 362 runs completed, lost 0
process_rolling_upgrade (OSTRO_UPGRADE_ITERS=3): 7907 acknowledged runs, lost 0, duplicate journal entries 0
  finalized 2 on every node 542-569 ms after the last restart; upgrade oracles pass (84-105 meta entries, ~42k apply outcomes per iteration)
  controlled shutdown (SIGTERM -> exit): p50 666ms / p99 781ms (n=9); restart -> ready p50 768ms / p99 851ms (n=9)
  handoff unavailability per partition per restart: p50 276ms / p99 2.00s (n=36)
  first activation after an upgraded restart: p50 464ms / p99 522ms (n=36); second run p50 465ms / p99 519ms; baseline p50 455ms / p99 1.69s
process_ryw: 9000 ops, 0 RYW violations; read p50 3.2ms p99 20.6ms, write p50 6.5ms p99 21.2ms
process_downgrade: release -> finalized 2 after 298ms; level-1 start after finalize refused by the boot fence (exit 1); 392 runs, lost 0
process_shutdown (10 SIGTERMs): exit p50 216ms / p99 1.51s; unavailability led p50 650ms / p99 1.37s (n=25), followed p50 642ms / p99 1.09s (n=15); 25/25 handed off RESIGN
cluster_campaign_p13 (50 faults): 1597 acks, lost 0; membership + upgrade oracles pass (220 meta entries)
```

The AOT pre-warm keeps the first activation through a restarted node within about 10 ms (p50) of the baseline. The followed-partition stall on controlled shutdown (deferred, p13-bugs2 bug 4) is visible: followed partitions are unavailable as long as led ones.

## Requirement evidence map

| req | evidence (full text in REQUIREMENTS.md traceability) |
|---|---|
| CLUS-05 | driver membership tests, reconcile_model, membership + reassign + promotion scenarios, p13_boot_modes / p13_promote (now required), tier C promotion, campaign membership oracles, 3 membership mutants bite |
| CLUS-06 | gate/finalizer/fences + fail-stop, worker_compat + version_gate, tier C rolling upgrade x3 with child runs on (7942813, 0 lost), downgrade fences, 2 upgrade mutants bite; real N-1 human_needed |
| CLUS-07 | module GC (two scans, single_scan mutant), RepinRun + migrate-runs (+ already_on_target), module_gone, migrate after finalization in every tier-C upgrade iteration, 2 mutants bite |
| ENG-10 | linker_set incl. the 0.2 fixture (now a required CI step), upload gate 409 + ext 101, journal literal grep gate |
| API-08 | routing parity through every node, lease/ReadIndex/tokens, keyed restarts, cross-node children, tier C RYW 0 violations, serve_without_lease bites |

## Docs

- `docs/cluster-operations.md` covers:
  - init/join (token, CA pin, marker-first), voter policy, remove/drain/tombstones, `--force`, quorum refusal, wiped-node remedy;
  - reassign/rebalance with `max_concurrent_moves`, and the P13-F7 skew note;
  - handoff and controlled shutdown, with the measured table and the **deferred follower-ISR stall**;
  - routing: hop limit, 503 + `Retry-After`, unkeyed placement, **keyed restarts (`NodeOps.StartRestart`)** and **cross-node children (`NodeOps.DeliverInbound`)**;
  - RYW: lease, ReadIndex, `ostro-consistency` / `ostro-read-after`, `strict_reads` with measured cost;
  - status fields, the config reference and Phase 13 metrics.
- `docs/compat.md` covers:
  - feature levels, the gate registry and **409 `feature_not_enabled` for every gate**;
  - machinery tags 96–99, P13-F1 rules 1–6 as implemented (re-encode identity), `auto_finalize` / hold / finalize, fences with their messages;
  - **ext 101 only for non-baseline modules**, **`[workers] version_policy` and the GoAway reasons**;
  - `OSTRO_TEST_LEVEL_MAX` emulation, real N−1 status and the mutants.
- `docs/upgrade-runbook.md` covers:
  - rolling upgrade steps and the measured table, with the **pre-warm effect**;
  - the downgrade window and what is refused before finalize;
  - `migrate-runs`: the **report format including `already_on_target`**, the limit split, P13-F3 `run-info.module-hash`;
  - module retire / GC and **`module_gone`**.
- `docs/replication.md`:
  - a Phase 13 section, with the forward references replaced;
  - **tier-C env vars** in §8.1.
- `docs/storage-format.md` §19: RepinRun apply order with RejectReason 23/24, AlterReplicas, ext 96–102 value structs, P13-F1 marked implemented, `cluster-seeds.toml`.
- Other docs:
  - `metrics.md`: `module_gone` added to the `ostro_dlq_total` reasons, plus a Phase 13 series pointer;
  - `workers.md`: `version_policy`;
  - `workflow-semantics.md`: re-pin now exists;
  - `bottomless.md`: cluster restore moved into 1.0.

## Tracking files

- RESERVATIONS:
  - Cmd 43, RejectReason 23/24, MetaCommand 8, ext 96–102 and §11 names are now `exists (P13)`. Cmd 44, MetaCommand 9–11 and tags 103–111 are `reserved P13, unused`.
  - New rows: the non-mutant P13 failpoints incl. `gc::start_hold`; `module_active` and `not_supported_in_profile`; `OSTRO_P13_MUTANT`; and the 13-19 env vars (`ostro-deadline-ms` already had a row).
  - `ELECTION_REASON_HANDOFF` was not added, so nothing is reserved for it.
- REQUIREMENTS: CLUS-05/06/07, ENG-10 and API-08 are ticked, with traceability rows naming test names, commits and log paths. CLUS-06 was held back until tier C passed with child runs on at 7942813.
- 13-VALIDATION:
  - all 56 task rows green; `nyquist_compliant: true`, `wave_0_complete: true`;
  - gate evidence table, measurements and the human_needed list added.
- 13-DECISIONS: execution-time amendments table (each with its plan), including the d33fce7 root cause.
- STATE, in its own docs commit (26ece7e): every `[USER REVIEW]` / `[USER DECISION]` todo covered by the 2026-11-26 decisions log is marked `[x]` with its outcome:
  - 26 confirmed;
  - P8-D11, D-P12-3 and D-P10-11 changed;
  - dev-mode replacement decided as option (a).

  New todos: the Kafka/Chronik trigger and whole-cluster restore (Phase 14 scope adds), the own file watcher, dev-ephemeral module replacement, the webhook test-key split, the fsync-free deferral, and a Phase 14 replanning pass. P8-D21 is not in the log and stays open.

## Deviations from Plan

1. **[Process] Paths and runner.** Worktree `~/.local/share/ostro-orchestrator/wt/13-20`, slot-3 target, cluster runs through `ostro-k8s-ci` (executor brief), not the plan's /tmp paths and local ci-local. Reports were written in the pods and are pasted above.
2. **[Scope, orchestrator] ci.yml edited directly** (authorised for this plan); only added or tightened steps.
3. **[Rule 3] Shared grep-gate script** `scripts/p13-grep-gates.sh` (not in `files_modified`), so ci-local and ci.yml cannot drift; P12 script untouched. Two gate adaptations (D-13-20-2): the Chronik gate allows the existing negative assertion; the journal gate skips test modules (two `journal@0.1.0` literals sit in `linker_set.rs` unit tests).
4. **[Scope] Tier-C step added to per-push CI** (orchestrator item). In the plan check it runs in the catch-all lint group (about 20–50 min); the ≥ 10,000 campaign stays manual.
5. **[Temporary] `OSTRO_P13_CHILD_RUNS=0` with TODO(p13-children)** in 9055589, removed in 7942813 once d33fce7 landed (orchestrator instruction).
6. **[Gate] Campaign HUMAN_NEEDED at 1,110 faults**, short of the plan's target, ended by pod `/dev/shm` exhaustion, not the time box. It ran at b47721e with child runs off because it started before the fix.
7. **[Extra docs]** metrics.md, workers.md, workflow-semantics.md and bottomless.md forward references updated (outside `files_modified`, docs only).

## Known load-sensitive failures seen (pass on re-run / alone; not edited)

- ostro-partition `trim_safety` (on the list) and `snapshot_tail::snapshot_plus_tail_equals_full_replay` ("node open timed out"): **snapshot_tail is new to the list**.
- ostro-partition `cron_driver::catch_up_skip_advances_the_watermark_without_runs` (10 s wait under load): **new to the list**.
- ostro-engine `hot_wait::hot_wait_long_sleep_evicts_immediately` (518 ms vs a 200 ms bound under load; listed by the orchestrator).

## Open / human_needed

- Phase 13 campaign beyond 1,110 faults: needs a bigger `/dev/shm` or a disk-backed `OSTRO_PROC_ROOT`, at a commit with d33fce7 and child runs on. The partition logs never trimmed in the campaign (`floor=0`); worth a look as possible unbounded growth under the harness.
- Real N−1 upgrade (`OSTRO_PREV_BIN`) after the first release tag.
- Quiet-node re-measurement; remote GitHub CI legs for the new steps after a push.
- Deferred: controlled-shutdown follower-ISR stall (p13-bugs2 bug 4); joined-node chaos-proxy routing in tier C.
- Phase 14 follow-ups (STATE Blockers/Concerns): seal `__ostro/cluster-ca`, enforce scoped tokens on forwarded calls via `ostro-principal`, a Jepsen clock-skew nemesis against leases, the RF=3 benchmark gate.
- macOS-only (deferred, Linux first): the ostro-worker `protocol::drain_flushes...` guest helper does not build `trivial_steps` in a fresh worktree (13-14).
