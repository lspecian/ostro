---
phase: 14-multi-tenancy-security-1-0-release
plan: 09
subsystem: ostro-jepsen (Jepsen-style harness core, QA-05)
tags: [p14, jepsen, nemesis, chaos-proxy, kill-9, checkers, fencing, consistency-tokens, qa-05]
requires:
  - phase: 14-01
    provides: "crates/ostro-jepsen skeleton + manifest (conformance, client, server, partition, log, types, tokio, serde, clap, tempfile, rand); OSTRO_JEPSEN_* env names (RESERVATIONS)"
  - phase: 14-01b
    provides: "gate part 2 (no seam needed by this plan)"
  - phase: 12/13
    provides: "ProcCluster + ChaosProxy (tier C), oracle::disk::scan_partition, replica/la_rule oracles, process tools (settle, published, meta_leader), ostro-consistency tokens (13-13), any-node routing (13-12)"
provides:
  - "ostro_jepsen::history: ClientOp / NemesisEvent / History (JSONL), PartitionToken"
  - "ostro_jepsen::checker::{lost, duplicate, fencing} over a decoded LogExport (+ Verdict); fencing::reapply_scan (production apply on a fresh store)"
  - "ostro_jepsen::workload: crash-saga clients with ok/fail/info outcomes and consistency tokens"
  - "ostro_jepsen::nemesis::{partition, kill}"
  - "ostro_jepsen::run: RunSpec, Schedule, NemesisKind (+ Nemesis trait for 14-10), run()"
  - "ostro_jepsen::report: target/jepsen/<seed>/ (workspace root; OSTRO_JEPSEN_OUT)"
  - "tests/checker_selftest.rs (10 tests), tests/harness_smoke.rs (#[ignore])"
affects: [14-10, 14-22, 14-23, 14-27]
tech-stack:
  added: []
  patterns:
    - "checkers are pure functions over a decoded log model (LogExport) + the client history; a cluster run only produces the inputs"
    - "fencing proven against the production apply: every replica log re-applied on a fresh store; an independent fencing model audits every accepted command"
    - "schedule = pure function of the seed (schedule.json written before the run); targets resolved at injection time and logged in nemesis.jsonl"
key-files:
  created:
    - crates/ostro-jepsen/tests/checker_selftest.rs
    - crates/ostro-jepsen/tests/harness_smoke.rs
  modified:
    - crates/ostro-jepsen/src/{history,report,run,workload}.rs
    - crates/ostro-jepsen/src/checker/{mod,lost,duplicate,fencing}.rs
    - crates/ostro-jepsen/src/nemesis/{partition,kill}.rs
    - crates/ostro-jepsen/src/lib.rs (dependency anchors of used crates removed, per the p14 notes)
key-decisions:
  - "The consistency token's offset is the answering leader's applied cursor after the write (13-13), an upper bound of the write's own offset: the fencing checker matches the acknowledged command (run id / signal request id) below the token offset and requires it in a record of an epoch <= the token epoch, instead of 'the entry at that exact offset'"
  - "Outcomes: 2xx ok; 4xx or never connected fail; timeout / broken connection / 5xx info (a forwarded write can be applied and still answer 503)"
  - "The crash-saga workload is re-implemented in ostro-jepsen (ostro-crashtest is not a dependency; no Cargo edits): inputs and expected results are a pure function of (seed, key); runs with a fatal step get no signals; 3% get a cancel instead of 'extra'"
  - "'Second invocation with the same seed' = a second schedule generation from the same RunSpec compared byte for byte with the run's schedule.json (a second 3-minute cluster run adds nothing to that property)"
  - "One-way partition = the proxy cuts connections a -> b (refused and reset) while b -> a connections keep flowing (the proxied links are TCP connections, so direction = who dials)"
  - "Faults run one at a time (never more than one node down) except the explicit NemesisKind::QuorumLoss (not in the smoke menu)"
requirements-completed: []
duration: ~3h
completed: 2026-11-27
---

# Phase 14 Plan 09: Jepsen-style harness core Summary

**A seeded, reproducible fault harness on the Phase 12/13 tier-C cluster (3 `ostro server` processes, RF 3, behind the Rust chaos proxy on `127.r.r.n`): 8 crash-saga clients with consistency tokens, partition and `kill -9` nemeses, and lost / duplicate / fencing checkers that are proven to bite (self-tests for every anomaly class, the fencing rule checked against the production apply on re-applied logs). The 180 s smoke (seed 1409) passed on the cluster: 1,275 acknowledged runs, 22 faults, 3,633 consistency tokens checked, 67,179 log commands re-applied, 0 violations.**

QA-05 is not ticked here (14-27's, D-P14-35).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 | 268c81f | history + report + checkers (lost, duplicate, fencing) + `checker_selftest` |
| 2 | 3c12a29 | workload, partition / kill nemeses, seeded runs, `harness_smoke`, lib.rs anchors |
| fix | a885319 | fencing: acked command matched by epoch (benign token race), self-test message |

## Smoke report (cluster, ostro-k8s-ci slot 1, tree of 3c12a29)

```
ostro-jepsen run: seed 1409 | 180 s of faults | 3 nodes, 4 partitions | PASS
binary: target/failpoints/debug/ostro
ops: cancel/info 1, cancel/ok 33, result/fail 163, result/info 381, result/ok 3643,
     signal/fail 121, signal/info 240, signal/ok 2325, start/fail 60, start/info 151, start/ok 1275
faults: kill:any 2, kill:follower 2, kill:leader 1, kill:meta_leader 3, partition:bridge 2,
        partition:leader_isolation 4, partition:majority_minority 7, partition:one_way 1
timings (ms): start 3851, warmup 10001, faults 180001, heal 9, quiesce 12789, export 1816, checks 245
lost: PASS (acked_signals=2325, acked_starts=1275, cancelled=33, completed=1242, failed_starts=60,
            info_starts=151, keys_in_log=1275, runs_checked=1275)
duplicate: PASS (journal_entries=60426, journals_checked=1275, records_compared=67032)
fencing: PASS (commands_reapplied=67179, entries_compared=44786, epochs=42,
               leader_announcements=126, tokens_checked=3633)
test result: ok. 1 passed (208.78 s)
```

Every acknowledged write carried a partition token (3,633 = 1,275 starts + 2,325 signals +
33 cancels; `acked_writes_without_token` = 0). Kill restarts took 1.07–2.02 s. No stale
command reached a log in this run (`stale_rejected` = 0): the stale path is proven by the
self-tests, including one through the production apply.

**Rerun at the final commit a885319** (slot 1, the plan's Task 2 verify command): PASS in
239.65 s, same seed so the same 22 faults by kind; 1,217 acknowledged runs (1,187 completed,
30 cancelled), 2,224 acknowledged signals, 3,471 tokens checked, 64,077 commands re-applied,
61 epochs / 183 leader announcements, 0 violations; `report.json` present.

## Harness API (for 14-10 / 14-22 / 14-23)

- `run::RunSpec::new(seed, duration, nemeses)` / `RunSpec::from_env(default_seed, default_secs, nemeses)`
  (`OSTRO_JEPSEN_SEED`, `OSTRO_JEPSEN_SECS`, `OSTRO_JEPSEN_TOKEN`). Public fields: `warmup`
  (10 s), `interval` (2–6 s between faults), `hold` (1–8 s), `clients` (8), `auth`,
  `quiesce_cap` (120 s), `partitions` (4), `out_dir` (`report::seed_dir(seed)`),
  `proc_options` (`ProcOptions`: env / `FAILPOINTS` for every node, e.g. 14-10's test
  endpoints).
- `run::NemesisKind::{Partition(PartitionMode), Kill(KillTarget), QuorumLoss, Custom(Arc<dyn Nemesis>)}`;
  menus `partitions()`, `kills()`, `partitions_and_kills()`.
- `run::Nemesis` (14-10 implements it in `nemesis/{clock,disk}.rs`): `name()`,
  `inject(&mut ProcCluster, pick) -> Result<target, String>`, `heal(&mut ProcCluster)`;
  boxed non-`Send` futures (`run::LocalBoxFuture`). A heal error aborts the fault phase
  (harness verdict).
- `run::run(spec).await -> Result<RunReport, String>`: `Err` only if the cluster cannot start
  or the guest cannot be deployed; checker failures are verdicts (`report.passed`).
  `RunReport::verdict("lost" | "duplicate" | "fencing" | "harness")`, `Verdict::count(..)`.
- `run::RunSpec::schedule()` -> `Schedule` (`schedule.json`); same seed -> same bytes.
- Files in `target/jepsen/<seed>/` (workspace root, `OSTRO_JEPSEN_OUT` overrides):
  `schedule.json` (before the run), `history.jsonl` + `nemesis.jsonl` (streamed),
  `report.json`, `report.txt`. A failing run keeps the cluster directory (path in the notes).
- Checkers for offline use: `checker::lost::check_lost(LostInput{ops, runs, expected, logs})`,
  `checker::duplicate::check_duplicates(&logs, &runs)`, `checker::fencing::check_fencing(&logs, &ops)`,
  `checker::fencing::reapply_scan(&LogScan, PartitionCount)`, `checker::from_scan(&LogScan, in_isr)`.
- 14-22 with auth on: pass the token via `OSTRO_JEPSEN_TOKEN` (or `spec.auth`); the deploy
  and every client / observation request carry it. ProcCluster nodes still start with
  `auth.mode = none` unless `proc_options.env` sets it (e.g. `OSTRO_AUTH__MODE`), so 14-22
  owns the bootstrap of a token on the harness cluster.
- 14-23 (bench/soak) can reuse `workload::Workload` / `Target` / `plan_input` /
  `expected_output`.

## Checkers (what each rules out)

- **lost**: every `ok` start's run exists after quiescence, has an accepted `StartRun` in the
  re-applied logs, is terminal with exactly one terminal journal event and ends `completed`
  with `expected_output(input)` (or `cancelled` after a cancel attempt, or `failed` with the
  crash-saga retries-exhausted shape); acknowledged signal request ids journaled exactly
  once; no idempotency key acknowledged as, or started in the logs as, two runs (`info`
  starts never counted as lost).
- **duplicate**: replica records byte-identical below the settled HW (ISR members hold all
  of it), no `(run, seq)` twice among accepted `EventsAppended`, API journals contiguous
  from 0 with one `run_started`, at most one terminal event, unique step completions and
  signal request ids; plus the Phase 12 `replica` oracle over the same scans.
- **fencing**: `LeaderAnnounced` epochs strictly increasing per log, one announcing node per
  epoch across replicas, no divergent command below HW, every acknowledged token's command
  present below its offset in a record of epoch <= the token's (a later-epoch record in its
  place = "acked offset overwritten"), and the offline re-apply audited by an independent
  fencing model (stale `LeaderAnnounced`, foreign envelope epoch, grant not above the
  current attempt, append / fenced pause not of the current attempt -> violation when
  accepted, counted when rejected); plus the Phase 12 `la_rule` oracle.

Self-tests (`checker_selftest`, 10): clean run passes every checker with exact counts; lost
ack (missing run, non-terminal run; an `info` start is not lost); wrong result; two runs for
one key; duplicate seq (log and API journal); divergent replica entry; two leaders in one
epoch (+ non-increasing epochs); acked offset overwritten after truncation (+ token beyond
every log); stale token accepted (+ old envelope epoch accepted); token parsing; the
production apply through `reapply_scan` (a stale grant rejected `StaleEpoch`, trimmed log
refused).

## Gate

`ostro-k8s-ci --parallel` on the worktree at a885319 (run p012334-3362, lane 2, wall 49m40s),
then slot-1 (dell-2) reruns for every step the parallel run could not finish:

| Step(s) | Parallel run | Rerun on slot 1 |
|---------|--------------|-----------------|
| cargo test | test1 shard PASS 909 s; test2 shard: pod never started (dell-1 unreachable) | full `cargo test` PASS 3407 s |
| failpoint suites | fp1 PASS, fp2 PASS; fp3 FAIL: `ostro fx_redrive::suspended_inflight_wakes` "no ready line within 60 s" (server start under load), suites after it not run | fx_redrive alone 2x PASS (4/4, 19 s); the remaining fp3 suites (ostro:fx_sql_crash .. ostro-server:audit_journal) PASS 459 s |
| replication mutant matrix, server campaign smoke (20 kills) | PASS 295 s / 1050 s | — |
| zig present, musl release | PASS / PASS 898 s | — |
| musl binary size limit | **FAIL: 60.37 MB (60,370,032 bytes) > 60 MB** | not rerun: pre-existing, see below |
| musl zigbuild test | musl1 shard FAIL: `ostro-meta membership_handle::barrier_on_another_voter_observes_the_write` (`applied() >= idx`), rest of the shard not run; musl2 shard: pod never started | full `musl zigbuild test` PASS 3572 s (membership_handle passed); gnu membership_handle alone 2x PASS |
| poly group (polyglot toolchains, python/ts sdk, polyglot conformance, clippy polyglot, clients python/ts/go) | pod never started | all PASS |
| lint group (toolchain, fmt, clippy, build, sdk, guests, order-saga, semantics sleepers, failpoint list sync, Phase 13 mutant matrix, wit-next fixtures, Phase 13 tier C, phase 7 legs, deny, vendored, deny fixtures, banned crates, P12/P13 grep gates, workflow validation) + ostro-build polyglot | pods Pending on dell-1, never started | all PASS (Phase 13 tier C 1051 s, Phase 13 mutant matrix 619 s, ostro-build polyglot 387 s) |

Every step is green except the musl size limit, which is **not caused by this plan**: the
`ostro` binary does not depend on `ostro-jepsen` (no crate of the binary changed in
268c81f..a885319), and 14-31's plan gate at b3e7208 measured the same 60,370,032 bytes
(`target/k8s-ci-logs/20261127-003721-par-b3e72086-musl1.log`; 58.86 MB at cbae331 before it).
14-34 owns the 70 MB raise (D-P14-54). The two load failures (fx_redrive server start, the
musl membership_handle barrier) touch no 14-09 code and passed alone / in the reruns;
`membership_handle::barrier_on_another_voter_observes_the_write` is not in the brief's
known-flake list (new load-only observation).

## Deviations from Plan

1. **[Rule 1 - Bug, own code] Token race in the fencing check** (found while reviewing the
   token source after the first smoke): the first version flagged any record of a later
   epoch below an acknowledged token offset. The token is read after the write returns, so a
   node can legitimately have applied a successor's records by then. Fixed in a885319: the
   acknowledged command itself must be present in a record of epoch <= the token's; a
   missing command next to later-epoch records is reported as "acked offset overwritten".
2. **Token semantics** (plan said "an entry at that offset"): matched below the applied-cursor
   offset, see key decisions. The plan's intent (an acknowledged write is never truncated or
   rewritten by a later leader) is what is checked.
3. **`lib.rs`** (not in files_modified): the `use <crate> as _;` anchors of the crates this
   plan now uses were removed, as the p14 orchestrator notes ask (clap, ostro_client,
   ostro_log stay anchored for 14-10 / 14-23).
4. **Crash-saga workload re-implemented** in `workload.rs` (no `ostro-crashtest` dependency,
   no Cargo edits); same guest, same documented output function.

## Product anomalies

None found (no STOP).

## Self-Check

PASSED: the 12 plan files exist; 268c81f, 3c12a29, a885319 are on main; `pub fn check_fencing`
in checker/fencing.rs, `OSTRO_JEPSEN_SEED` and `ProcCluster` in run.rs; checker_selftest 10/10
(local, slot-1 target dir); clippy -D warnings clean for ostro-jepsen (all targets).

Workflow hunks: none (no ci.yml / ci-local change; the smoke is `#[ignore]`, 14-22/14-27
wire the Jepsen smoke into CI).
