---
phase: 06-effects-bridge-connectors-secrets
plan: 18
subsystem: effects-bridge
tags: [sql, exactly-once, fx-04, sc2, kill-9, failpoints, ci, postgres, mysql]
requires:
  - "06-09 fx-sql guest (workflow fx-sql)"
  - "06-11 SqlConnector exactly-once protocol + failpoints bridge::sql::{before_user_stmt, after_commit}"
  - "06-15 / 06-16 seam + REST admin (secrets, connections, egress)"
provides:
  - "crates/ostro/tests/fx_sql_crash.rs: SC2 kill -9 test, pg_after_commit, pg_before_user_stmt, mysql_after_commit, mysql_before_user_stmt"
  - "ci.yml postgres:17 + mysql:8.4 service containers (gnu leg), OSTRO_TEST_PG_URL / OSTRO_TEST_MYSQL_URL export"
  - "FAILPOINT_CRATES entries ostro:fx_redrive, ostro:fx_sql_crash"
  - "ci-local Phase 6 external-services banner + .ostro-test-env loader"
affects: [06-20]
tech-stack:
  added: []
  patterns:
    - "Spawned-binary SQL tests drive the database only through fx-sql runs (exactly_once=false for setup and polling), because crates/ostro has no sqlx/testkit dev-dependency"
    - "Matrix-conditional service containers: image is '' on legs that must not start them"
key-files:
  created:
    - crates/ostro/tests/fx_sql_crash.rs
  modified:
    - .github/workflows/ci.yml
    - scripts/ci-local.sh
    - .gitignore
key-decisions:
  - "CI DB passwords are ostro-ci-pw (11 bytes), not ostro: connection passwords travel as sealed secrets, and those must be at least 8 bytes (06-11 tests assert it)"
  - "S3: reuse 08-03's Start SeaweedFS step and OSTRO_TEST_S3_* unchanged (D-FX10); no MinIO"
  - "DB URLs are exported by a gnu-leg step to GITHUB_ENV, not job env, so the macOS/musl legs keep SKIPping the DB legs"
  - ".ostro-test-env is parsed (OSTRO_TEST_* KEY=value only), not sourced, so URL characters such as & and ? are taken literally"
metrics:
  duration: "~2 h (about 1 h of it the ci-local failpoint suites)"
  completed: 2026-11-20
---

# Phase 6 Plan 18: E2E SC2 (SQL kill -9 between COMMIT and effect_completed) Summary

**The test holds 50 exactly-once `INSERT` runs at `bridge::sql::after_commit` in a spawned `ostro dev`. At that point the user transaction has committed and `effect_completed` has not been journaled. It then kills the node with `kill -9` and restarts it without failpoints. All 50 runs complete with exactly 50 user rows, one per run. Each run's stored token result is byte-identical to the completion the guest decoded from the journal. The same scenario at `before_user_stmt` (token inserted, nothing committed) also ends with exactly 50 rows. The PG legs pass locally. The MySQL legs are wired to a `mysql:8.4` service container in CI and are human_needed until the first push.**

## Tasks

| Task | Name | Commit |
| ---- | ---- | ------ |
| 1 | SC2 crash test (PG + MySQL) | a6143f5 |
| 2 | CI service containers + ci-local env gating | d5d5bd7 |

## PG timings (local)

These ran against a **private, throwaway PostgreSQL 14 cluster**: the distro's `/usr/lib/postgresql/14/bin` binaries, `initdb` in the session scratchpad, 127.0.0.1:55418, scram password auth, `max_connections=200`. The system PG on :5432 is still unusable, because it has no known credentials (06-11). Nothing about this cluster is committed. In the URL form `postgres://ostro:<pw>@127.0.0.1:55418/postgres?sslmode=disable`, the PG legs really ran:

| Leg | N | all held | restart (readyz) | all terminal after kill -9 | total |
|-----|---|----------|------------------|----------------------------|-------|
| pg_after_commit (quiet run) | 50 | 0.78 s | 0.29 s | 0.59 s | 4.7 s |
| pg_before_user_stmt (quiet run) | 50 | 0.91 s | 0.28 s | 0.58 s | 4.8 s |
| pg_after_commit (load ~10, during other builds) | 50 | 3.8 s | 3.7 s | 4.4 s | 15.9 s |
| pg_before_user_stmt (load ~10) | 50 | 5.6 s | 8.4 s | 14.7 s | 42.9 s |

The suite ran 5 times in a row with no failure. Each test drops its `t_<id>` schema.

## MySQL status

**human_needed.** No mysqld is available locally. `mysql_after_commit` and `mysql_before_user_stmt` print the loud SKIP banner and pass. In CI they run on the gnu leg against `mysql:8.4` (`OSTRO_TEST_MYSQL_URL=mysql://root:ostro-ci-pw@127.0.0.1:3306/ostro?ssl-mode=REQUIRED`). The MySQL-specific parts have never been run: the `?` placeholders, the `HEX(result)` read and the `performance_schema.data_locks` IX-lock count used to detect "held at before_user_stmt".

## What the test asserts (per leg)

1. Setup goes over REST to `ostro dev --data-dir`, with the dev sealing key persisted in the data dir:
   - sealed secret `db/orders-pass` (only when the URL has a password);
   - connection `admin` (URL database, `manage_schema: false`), which creates and drops the test scope;
   - connection `orders` (PG `"schema": t_<id>`, or MySQL database `t_<id>`; `pool_max: 60`);
   - egress `fx-sql` → `connection:admin`, `connection:orders`.
2. 50 runs each make one exactly-once insert with their own key `k-NNN` and amount.
3. Held. For `after_commit`: `count(*) FROM ostro_effects WHERE result IS NOT NULL = 50` and `orders = 50`. For `before_user_stmt`: 50 distinct transactions hold `RowExclusiveLock` on `ostro_effects` (PG `pg_locks`), while the visible `ostro_effects` count and `orders` count are both 0. No run is terminal and no `effect_completed` exists yet. All polling goes through `orders` with `exactly_once=false` while the 50 effects are held, which proves the pool stayed usable (W4).
4. kill -9, then a restart without FAILPOINTS on the same data dir.
5. All 50 runs `completed` with output `{"results":[{"columns":[],"rows":[],"rows_affected":1}]}`. Each run has exactly 1 `effect_scheduled` (connection `orders`) and 1 `effect_completed`. `orders` holds exactly the 50 (key, amount) pairs. `ostro_effects` holds exactly one row per started run id, and its stored result is `010001000001`.

## Deviations from Plan

### Auto-fixed / adapted

**1. [Rule 3 - Blocking] Journaled result bytes are compared through the guest output, not the history API**
- **Issue:** the REST history shows only `{"op"}` for `effect_completed`, and `crates/ostro` has no ostro-types dev-dependency to decode the journal (only 06-02 edits Cargo files).
- **Fix:** the guest's output is decoded from the journaled completion bytes (P-10). The test asserts that every output is the decoding of `postcard(Ok(OpOutput::Sql(SqlResult{[],[],1})))`, and that every stored `ostro_effects.result` is exactly `RESULT_CODEC_VER || those bytes` (`010001000001`). Together these tie journal and DB byte for byte.

**2. [Rule 3 - Blocking] No testkit `db` helpers in crates/ostro tests**
- **Issue:** `ostro_testkit::db` and sqlx are not dev-dependencies of `ostro`.
- **Fix:** the test parses the URL itself (same form as `parse_url`). It creates and drops the per-test PG schema or MySQL database, creates the table, and runs every count query through fx-sql runs with `exactly_once=false`.

**3. [Rule 1 - Bug in plan] CI passwords**
- **Issue:** the plan's `POSTGRES_PASSWORD=ostro` / `MYSQL_ROOT_PASSWORD=ostro` is 5 bytes, below the 8-byte sealed-secret minimum. Every templated DB password would be refused, and 06-11's sql_pg/sql_mysql tests assert the minimum and would panic.
- **Fix:** `ostro-ci-pw` for both.

**4. [Rule 3] MinIO → SeaweedFS**
- The plan text predates D-FX10. 08-03 had already added the `Start SeaweedFS` step and the `OSTRO_TEST_S3_*` export, which are reused unchanged. No MinIO was introduced.

**5. [Rule 3] FAILPOINT_CRATES had no `ostro` suite**
- **Issue:** `ostro` was lint-only, so neither `fx_sql_crash` nor 06-17's `fx_redrive` (also `#![cfg(feature = "failpoints")]`) ran anywhere in CI.
- **Fix:** added `ostro:fx_redrive ostro:fx_sql_crash` to FAILPOINT_CRATES and the matching ci.yml lines. `check_failpoints` now lints each crate once.

**6. Service containers on a matrix job**
- **Issue:** service containers are Linux-only, and `services:` is job-level.
- **Fix:** the image is `''` on every leg except linux-x86_64-gnu (the matrix-conditional skip). The URLs are exported by a gnu-only step to `$GITHUB_ENV`, not job env, so other legs never see them. The empty-image skip is a GitHub runner behaviour that has not been verified on a real push yet (human_needed with the first CI run).

**7. Plan verify command defect**
- **Issue:** the plan's loop greps `cargo test -p <entry> --features failpoints` with the raw entry. For the existing `crate:target` entries (`ostro-engine:aot_failpoints` and others) that can never match.
- **Fix:** verification used the corrected form (`-p ${c%%:*} … [--test ${c#*:}]`) and ci-local's own `failpoint list sync` step. Both passed.

## Verification

- `cargo test -p ostro --features failpoints --test fx_sql_crash --locked -- --test-threads=1 --nocapture`:
  - with the private PG URL: **4 passed** (2 PG legs really ran, 2 MySQL SKIP);
  - without the URL: 4 passed (all SKIP, loud banner).
- `cargo clippy -p ostro --features failpoints --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean.
- Corrected plan verify (grep checks + FAILPOINT sync) passed. The `.ostro-test-env` loader was exercised: an `OSTRO_TEST_PG_URL` containing `?…&…` loaded verbatim, a non-`OSTRO_TEST_*` line was warned about and ignored, and a tracked file made the script exit 1.
- `bash scripts/ci-local.sh --no-musl`, run with `.ostro-test-env` pointing at the private PG: every step PASS except two:
  - **cargo test FAIL**:
    - `ostro --test cli_phase8 round_trip_against_ostro_dev` failed with `schedules create: unexpected error: {"error":{"code":"invalid_argument","message":"workflow \"greet\" does not exist"}}`. This is a Phase 8 test (08-12) that expects "not implemented" from a controller 08-14 has since implemented. It fails standalone on main and is unrelated to this plan.
    - The 06-11 `sql_pg` failures below.
  - **failpoint suites FAIL**: it stopped at `ostro-bridge` `sql_pg` (same two failures), so the later entries were re-run by hand:
    - `cargo test -p ostro-bridge --features failpoints` without the PG URL: all ok;
    - `ostro:fx_redrive`: 4 passed;
    - `ostro:fx_sql_crash` with PG: 4 passed.
  - External-services banner output: `set: OSTRO_TEST_PG_URL`, plus `SKIP (human_needed): OSTRO_TEST_MYSQL_URL unset — …` and `SKIP (human_needed): OSTRO_TEST_S3_URL unset — …`, both recorded as SKIP rows.

## Deferred Issues (out of scope, found by the first real PG run of 06-11's tests)

`crates/ostro-bridge/tests/sql_pg.rs` has never run against a server before (06-11 SKIPped). Against PG 14 it has two failures:

```
---- pg_schema_isolates_connections stdout ----
thread 'pg_schema_isolates_connections' panicked at crates/ostro-bridge/tests/sql_pg.rs:785:5:
assertion `left == right` failed
  left: 1
 right: 0
---- pg_value_round_trip stdout ----
thread 'pg_value_round_trip' panicked at crates/ostro-bridge/tests/sql_pg.rs:638:5:
assertion `left == right` failed
  left: [[..., Numeric("12345678901234567890.1250"), ...]]
 right: [[..., Numeric("12345678901234567890.125"), ...]]
```

The other 11 (13 with failpoints) passed. These look like test-expectation issues: PG keeps numeric scale, and the per-schema `ostro_effects` DDL count. They need a follow-up in 06-11's files. CI will hit them on the gnu leg as soon as `OSTRO_TEST_PG_URL` is set there. **This means CI's `cargo test` step will fail on the gnu leg until they are fixed.**

## human_needed

- MySQL legs of fx_sql_crash, and 06-11's sql_mysql, against `mysql:8.4` on the first CI push.
- The empty-image service skip on the macOS / musl legs (first CI push).
- PG legs on the system PG (:5432) need a credentialed `.ostro-test-env`. They were proven here on a private PG 14 cluster.

## Self-Check: PASSED

- crates/ostro/tests/fx_sql_crash.rs, .github/workflows/ci.yml, scripts/ci-local.sh and .gitignore are on main.
- Commits a6143f5 and d5d5bd7 are on main.
