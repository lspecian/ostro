---
status: verifying
trigger: "Fix the failures of the first remote GitHub CI run (44932519644, main 6ec19c2)"
created: 2026-11-21T18:15:00Z
updated: 2026-11-21T19:35:00Z
---

## Current Focus

hypothesis: golden_det must pin committed module bytes (fresh builds are host-triple dependent)
test: run 3 (after run 44935982335) on all legs
expecting: ostro-engine passes on every leg; remaining failures are sustained_overload
  (x86_64-musl) and macOS ephemeral_under_1s, both reported for a decision
next_action: push, watch, report

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

- timestamp: 2026-11-21T19:10Z
  checked: run 44935982335 (fixes 1-4)
  found: cli_bridge (macOS), sql_mysql 8/8 (gnu, real MySQL 8.4), paging (both musl legs) pass.
    New: aarch64-musl golden_det RunStarted f7de433a73938b78 vs path-neutral x86 6b85acf4d2a49a7e;
    gnu replay_corpus det_basic_entry_equals_golden_snapshot fails because the committed corpus
    det-basic entry (module.wasm sha 193d6d48..., the plain local build) renders the ORIGINAL
    snapshot line 0 (95042388e40780cf).
  implication: path remapping is not enough. cargo hashes the `host:` line of `rustc -vV` into
    the -C metadata of host units (proc macros, build scripts), and dependents mix their deps'
    metadata in, so StableCrateIds and the wasm bytes differ per build host
    (cargo src/compiler/build_runner/compilation_files.rs hash_rustc_version; reproduced locally:
    a different __CARGO_DEFAULT_LIB_METADATA seed changes det_basic.wasm). Fix: the golden
    recording runs the committed corpus module (snapshot restored to the original), and the
    fresh build must match it on every line except RunStarted (module_hash). Neutral-build
    helper dropped.
- timestamp: 2026-11-21T19:10Z
  checked: run 44935982335 x86_64-musl worker_failover sustained_overload
  found: "more embedded runs than slots": 2 of 50 samples are (3, 1); every sample totals 4
    (= embedded 2 + worker 2 slots). embedded is computed as running-list count minus
    demand.running_on_workers, two separate requests.
  implication: consistent with a non-atomic sample (a worker run leaving running_on_workers
    between / before its status leaves `running`), but a real embedded over-admission while a
    worker slot is free cannot be ruled out from the data. Not changed; reported for a decision.
- timestamp: 2026-11-21T19:10Z
  checked: run 44935982335 macos-aarch64 dev_startup ephemeral_under_1s
  found: 5/5 samples 2.58-2.92 s to /readyz (ready line within ~10 ms of it); warm_restart_under_1s
    passes on the same runner; Linux legs pass.
  implication: macOS-specific cost of a fresh data dir (unmeasured here; suspect: F_FULLFSYNC,
    which std's sync_all uses on macOS, on the init path). Needs a decision / a macOS profile.
- timestamp: 2026-11-21T19:10Z
  checked: tests/replay-corpus/det-basic/module.wasm strings
  found: embeds /home/<user>/.cargo/... and /home/<user>/.rustup/... paths (recorded with a plain
    build before corpus-guest-build.sh existed). Entries are "never edit or re-record".
  implication: observation only; reported.

## Resolution

root_cause: |
  1. macOS: test read /proc/<pid>/cmdline (Linux-only).
  2. MySQL: test ddl() helper used sqlx::query (always prepared on MySQL); CREATE FUNCTION is
     not allowed in the prepared-statement protocol (ER_UNSUPPORTED_PS 1295).
  3. paging: test raced the signal post against the run's steps; signal_payload position is
     timing-dependent, not an aarch64 difference.
  4. golden_det: pinned a freshly built guest; its bytes (module_hash in RunStarted) depend on
     the build host (paths and host triple in cargo's -C metadata).
fix: test-side fixes; 4 now records the committed corpus module and compares the fresh build
  on every line but RunStarted
verification: local: sql_mysql 8/8 vs MySQL 8.4.10; fx_sql_crash 4/4 (repeat); cli_bridge ok
  (Linux and the ps branch); paging ok; determinism 10/10; replay_corpus 5/5. CI: 1-3 green in
  run 44935982335; 4 pending.
files_changed:
  - crates/ostro/tests/cli_bridge.rs
  - crates/ostro-bridge/tests/sql_mysql.rs
  - crates/ostro/tests/worker_e2e.rs
  - crates/ostro-engine/tests/determinism.rs
