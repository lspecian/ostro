---
status: verifying
trigger: "Fix the failures of the first remote GitHub CI run (44932519644, main 6ec19c2)"
created: 2026-11-21T18:15:00Z
updated: 2026-11-21T18:45:00Z
---

## Current Focus

hypothesis: four independent root causes (below), all fixed locally; CI re-run pending
test: push main, watch the new run
expecting: all four test legs reach past the previously failing binaries
next_action: push, `gh run watch`, triage any newly exposed failure

## Symptoms

expected: run 44932519644 green on all legs
actual:
- test (macos-aarch64) 136932117512: cli_bridge.rs:115 `argv ""` in bridge_verbs_round_trip_without_leaking_values
- test (linux-x86_64-gnu) 136932117678: sql_mysql.rs:246 unwrap on MySqlDatabaseError 1295 HY000
  "This command is not supported in the prepared statement protocol yet"
- test (linux-aarch64-musl) 136932117753: worker_e2e.rs:476 paging "same event shape embedded and remote"
- test (linux-x86_64-musl) 136932117601: ostro-engine determinism golden_det_basic_journal snapshot,
  only line 0 differs: `RunStarted ... 156 95042388e40780cf` (snapshot) vs `... 156 9514da9dd994edc8` (CI)
errors: see above
reproduction: CI run 44932519644 job logs
started: first remote CI run (the repo had only run ci-local before)

## Eliminated

- hypothesis: paging failure is an aarch64 behaviour difference
  evidence: the two event lists hold the same multiset (2000 tick, 1 big, signal_wait_started,
  signal_payload, signal_matched, wait_resolved, run_completed); only signal_payload moves (index 1
  remote vs 2003 embedded). The test posts the signal right after start, racing the run's steps;
  remote runs wait for worker pickup, so the buffered signal lands right after run_started.
  timestamp: 2026-11-21T18:25Z
- hypothesis: golden_det RunStarted mismatch is a determinism (arch / libc) bug
  evidence: every other journal line (step / timer / wait hashes) matches; RunStarted carries
  module_hash = sha256(wasm). A plain guest build under two CARGO_HOME spellings of the same dir
  gives two different det_basic.wasm hashes (193d6d48... vs 0584619c...); the path-neutral build
  (scripts/corpus-guest-build.sh) gives f74c58eb... for both, also with a different target dir.
  timestamp: 2026-11-21T18:35Z

## Evidence

- timestamp: 2026-11-21T18:16Z
  checked: sqlx-mysql 0.9.1 src/connection/executor.rs run()
  found: arguments Some(..) -> COM_STMT_PREPARE/EXECUTE; None (raw_sql / &str) -> COM_QUERY.
    `sqlx::query(..)` always carries arguments, so the test's ddl() helper prepared
    `CREATE FUNCTION ... BEGIN SIGNAL ...; END`, which MySQL refuses in the PS protocol (ER 1295).
  implication: test-helper bug, not product; reproduced against a private MySQL 8.4.10
    (user-space tarball, scratchpad); `sqlx::raw_sql` fixes it, all 8 sql_mysql tests pass.
- timestamp: 2026-11-21T18:20Z
  checked: cli_bridge.rs piped(): argv read from /proc/<pid>/cmdline
  found: macOS has no procfs -> unwrap_or_default -> "" -> assert fails.
  implication: use `ps -ww -o args= -p <pid>` off Linux (same kernel argv, KERN_PROCARGS2);
    the leak check keeps running on macOS.
- timestamp: 2026-11-21T18:25Z
  checked: worker_e2e long_run()
  found: signal posted immediately after start (race), see Eliminated.
  implication: post the signal after the run's `signal_wait_started` is in history, for both
    placements; the strict shape equality is kept.
- timestamp: 2026-11-21T18:33Z
  checked: target/guests det_basic.wasm strings
  found: /home/<user>/.cargo/registry/... and /home/<user>/.rustup/... panic-location paths.
  implication: golden snapshot pinned the blessing host's home dir. determinism.rs now prepares
    det-basic / det-divergent from the path-neutral build (target/guests-corpus); snapshot line 0
    re-pinned to the path-neutral module (6b85acf4d2a49a7e), every other line unchanged.
- timestamp: 2026-11-21T18:40Z
  checked: fx_sql_crash mysql legs against the private MySQL 8.4.10
  found: first run (concurrent with other heavy tests): mysql_before_user_stmt timed out with the
    data_locks count = 0 for 60 s. Five later runs (full suite x3, mysql legs x2, one under
    concurrent worker_e2e load) all passed; data_locks rows show 50 GRANTED TABLE IX locks in the
    test database as the query expects.
  implication: not reproduced; left unchanged, watch CI.

## Resolution

root_cause: |
  1. macOS: test read /proc/<pid>/cmdline (Linux-only).
  2. MySQL: test ddl() helper used sqlx::query (always prepared on MySQL); CREATE FUNCTION is
     not allowed in the prepared-statement protocol (ER_UNSUPPORTED_PS 1295).
  3. paging: test raced the signal post against the run's steps; signal_payload position is
     timing-dependent, not an aarch64 difference.
  4. golden_det: guest built without path remapping; module_hash in RunStarted depended on the
     build host's cargo / rustup home.
fix: test-side fixes for all four (see files_changed)
verification: local: sql_mysql 8/8 vs MySQL 8.4.10; fx_sql_crash 4/4 (repeat); cli_bridge ok;
  worker_e2e paging ok; ostro-engine determinism 10/10. CI pending.
files_changed:
  - crates/ostro/tests/cli_bridge.rs
  - crates/ostro-bridge/tests/sql_mysql.rs
  - crates/ostro/tests/worker_e2e.rs
  - crates/ostro-engine/tests/common/mod.rs
  - crates/ostro-engine/tests/determinism.rs
  - crates/ostro-engine/tests/snapshots/determinism__golden_det_basic_journal.snap
