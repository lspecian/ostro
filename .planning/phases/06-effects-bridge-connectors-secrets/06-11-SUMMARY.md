---
phase: 06-effects-bridge-connectors-secrets
plan: 11
subsystem: effects-bridge
tags: [sql, exactly-once, fx-04, sqlx, postgres, mysql, gc, testkit-db]
requires:
  - "06-04 egress (policy::allows_connection, P-7)"
  - "06-05 secrets (SecretResolver::resolve_operator, template::{parse, expand}, Scrubber)"
  - "06-07 driver core (Connector/EffectCtx/SendGate/Target, outcome::{encode_outcome, non_retryable, json_payload}, ConnectorsConfig, QuotaSpec)"
provides:
  - "ostro_bridge::sql::{SqlConnector, SqlConfig}: Connector for EffectKind::Sql"
  - "ostro_bridge::sql::classify::{gate, IMPLICIT_COMMIT, PG_NO_TRANSACTION}: the exactly-once allow-list"
  - "ostro_bridge::sql::exactly_once::{effect_key, redrive_expired, encode_stored, decode_stored, from_stored, Reconcile, FP_BEFORE_USER_STMT, FP_AFTER_COMMIT}"
  - "ostro_bridge::sql::pool::ddl: the pinned token-table DDL"
  - "ostro_bridge::sql::gc::{GcHandle, gc_sql, GC_BATCH, GC_INTERVAL} + SqlConnector::{gc_once, spawn_gc}"
  - "ostro_bridge::sql::config::{SqlConnConfig, Dialect, SslMode}: sql-postgres / sql-mysql v1 connection config"
  - "ostro_testkit::db::{pg, mysql, PgTestDb, MyTestDb, UrlParts, parse_url} (feature db)"
affects: [06-14, 06-18, 06-20]
tech-stack:
  added: []
  patterns:
    - "Everything that can fail without a database (policy, config, gate, params, W12 horizon, send gate) runs before a pool is touched"
    - "Exactly-once protocol generated per dialect by one macro (PG and MySQL share the exact step order)"
    - "PG user statements carry a trailing /* ostro:<param types> */ comment so sqlx's SQL-keyed statement cache never reuses a statement prepared for other parameter types"
key-files:
  created:
    - crates/ostro-bridge/src/sql/config.rs
    - crates/ostro-bridge/src/sql/classify.rs
    - crates/ostro-bridge/src/sql/value.rs
    - crates/ostro-bridge/src/sql/pool.rs
    - crates/ostro-bridge/src/sql/exactly_once.rs
    - crates/ostro-bridge/src/sql/gc.rs
    - crates/ostro-bridge/tests/sql_pg.rs
    - crates/ostro-bridge/tests/sql_mysql.rs
  modified:
    - crates/ostro-bridge/src/sql/mod.rs
    - crates/ostro-testkit/src/db.rs
key-decisions:
  - "Stored token result = codec_ver (1 byte) || RecordedResult.bytes: the exact completion bytes, so live and re-drive completions are byte-identical (persistent contract, pinned)"
  - "Effect key sha256(\"ostro/sql-effect/v1\" || cluster_id || run_id || op_le) pinned by two Python-hashlib vectors"
  - "Token DDL adds a created_at index (PG: separate CREATE INDEX IF NOT EXISTS; MySQL: KEY inside CREATE TABLE) so batched GC never scans the table; column layout exactly as planned"
  - "The policy check runs before the connection lookup: an unauthorised module gets egress_denied and cannot probe which connections exist"
  - "Connection passwords must be {{secret:NAME}} templates; a literal password is connection_unknown (it would sit in plain text in meta)"
  - "PG ssl_mode defaults to prefer (libpq default); MySQL defaults to required (D-FX2)"
  - "W12 horizon applies to exactly-once effects only; exactly_once=false is documented at-least-once"
  - "Reconcile loop: a re-run that fails Retryable before COMMIT is retried inside the loop, never completed Retryable (B4)"
  - "Columns come from preparing the statement; statements without result columns use execute (server rows_affected), others fetch_all (rows_affected = row count)"
metrics:
  duration: "~85 min"
  completed: 2026-11-20
---

# Phase 6 Plan 11: SQL Connector Summary

**`SqlConnector` runs `sql-execute` effects on named PostgreSQL and MySQL connections through lazily built sqlx 0.9.1 pools. Exactly-once statements must pass an allow-list gate that refuses every implicit-commit, transaction-control and write-outside-transaction form before anything runs. They then execute inside the token-first transaction in `ostro_effects`. Failpoints sit before the user statement and after COMMIT, and the connection goes back to the pool before the after-COMMIT failpoint fires. Once COMMIT has been sent, a bounded reconcile loop resolves the outcome and never completes Retryable. A re-drive older than the retention horizon completes `effect_outcome_unknown` without connecting. Old token rows are deleted by batched GC.**

## Local database status

| Leg | Status |
|-----|--------|
| PostgreSQL 14 at 127.0.0.1:5432 | Server is up, but **no working URL was found (P-17)**. `postgres://postgres@127.0.0.1:5432/postgres` and `postgres://lspecian@127.0.0.1:5432/postgres` both fail with `fe_sendauth: no password supplied`. `PGPASSWORD` is unset, there is no `~/.pgpass` and no untracked `.ostro-test-env`. The Unix socket gives `role "lspecian" does not exist` and `Peer authentication failed for user "postgres"`. The PG DB tests compiled and ran, and **SKIPped** (`SKIP: OSTRO_TEST_PG_URL unset`). **human_needed.** |
| MySQL 8.4 | **SKIP**: `OSTRO_TEST_MYSQL_URL` unset (human_needed / CI service, D-FX5) |

The URL form the tests expect is `OSTRO_TEST_PG_URL=postgres://<user>:<password>@127.0.0.1:5432/postgres`, with an optional `?sslmode=disable`. Never commit the password. A templated password must be at least 8 bytes (the secrets minimum), and the tests assert this. MySQL uses `mysql://<user>:<password>@127.0.0.1:3306/mysql?ssl-mode=REQUIRED`.

To close the PG leg, run:
`OSTRO_TEST_PG_URL=… cargo test -p ostro-bridge --test sql_pg --locked -- --test-threads=1` and
`OSTRO_TEST_PG_URL=… cargo test -p ostro-bridge --features failpoints --test sql_pg --locked -- --test-threads=1`.

## Token-table DDL (persistent contract, pinned by `pool::tests::ddl_is_pinned`)

PostgreSQL:
```sql
CREATE TABLE IF NOT EXISTS ostro_effects (effect_key bytea PRIMARY KEY, run_id text NOT NULL, op bigint NOT NULL, result bytea, created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS ostro_effects_created_at ON ostro_effects (created_at);
```
MySQL:
```sql
CREATE TABLE IF NOT EXISTS ostro_effects (effect_key BINARY(32) PRIMARY KEY, run_id VARCHAR(64) NOT NULL, op BIGINT UNSIGNED NOT NULL, result LONGBLOB, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), KEY ostro_effects_created_at (created_at)) ENGINE=InnoDB;
```
The DDL runs when the pool is built, outside any effect transaction. With `manage_schema = false` it is logged at info level instead.

## Pinned effect-key vectors

`sha256("ostro/sql-effect/v1" || cluster_id || run_id || op.to_le_bytes())`:

| cluster_id | run_id | op | key |
|---|---|---|---|
| `c1` | `0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b` | 1 | `74342fdf6c604ee6c26bf2e51daf8d49a5d8c125cfe7eab5f201c834faa066c5` |
| `cluster-a` | `run-1` | 0x0102030405060708 | `3f3716faef97c69c8d359603d15f651ea8088b71450d58a14be6fa3df6d77cf6` |

## Final API

```rust
// ostro_bridge::sql
pub struct SqlConfig { max_rows, max_bytes, effects_retention, backend_ttl, reconcile: Reconcile }  // from_connectors(&ConnectorsConfig), Default
pub struct SqlConnector;
impl SqlConnector {
    pub fn new(meta: Arc<dyn MetaView>, resolver: SecretResolver, cluster_id: impl Into<String>, cfg: SqlConfig) -> Self;
    pub fn effect_key(&self, run_id, op) -> [u8; 32];
    pub async fn gc_once(&self, retention: Duration) -> u64;
    pub fn spawn_gc(self: &Arc<Self>, retention: Duration) -> GcHandle;   // GcHandle::stop().await; Drop cancels
}
impl Connector for SqlConnector { /* kind Sql; target = {connection name, config rate/burst quota} */ }
pub fn is_retryable_db(sqlstate: Option<&str>, errno: Option<u16>) -> bool;
pub mod classify     { gate(stmt, Dialect, allow_call) -> Result<(), Failure>; IMPLICIT_COMMIT; PG_NO_TRANSACTION }
pub mod config       { SqlConnConfig::parse(name, kind, bytes); Dialect; SslMode; KIND_POSTGRES/KIND_MYSQL; is_identifier }
pub mod value        { to_params; bind_pg/bind_mysql; pg_class/mysql_class; decode_pg_row/decode_mysql_row; Collector }
pub mod exactly_once { effect_key; redrive_expired; encode_stored/decode_stored/from_stored; Reconcile{attempts 5, 200ms, 5s};
                       execute(entry, job, rc); FP_BEFORE_USER_STMT; FP_AFTER_COMMIT; REDRIVE_MARGIN = 1h }
pub mod pool         { ddl(Dialect, table); DbPool; PoolEntry; PoolCache }
pub mod gc           { gc_sql; gc_entry; GC_BATCH = 1000; GC_INTERVAL = 10 min }
```

## sqlx 0.9.1 API names used (verified in the registry source)

- `Pool::begin() -> Transaction<'static, DB>`, `Transaction::{commit, rollback}` (both consume `self`).
- `sqlx::query(impl SqlSafeStr)` with `AssertSqlSafe(String)`; `Query::bind` encodes immediately.
- `Executor::{prepare, prepare_with(SqlStr, &[TypeInfo])}`; `Statement::columns()`.
- `query_scalar::<_, Option<Vec<u8>>>(..).fetch_optional`.
- `sqlx::Error::{Database, Io, PoolTimedOut, PoolClosed, WorkerCrashed, Protocol, BeginFailed, ..}` (non-exhaustive); `DatabaseError::{code, message, is_unique_violation, kind}`; `(dyn DatabaseError)::try_downcast_ref::<MySqlDatabaseError>()` then `.number()`.
- `PgConnectOptions::{new_without_pgpass, options([("search_path", s)]), ssl_root_cert}`; `MySqlConnectOptions::{ssl_mode, ssl_ca}`.
- `PgTypeInfo::with_oid(Oid(0))` for untyped NULL; `PgTypeKind::Enum`.
- `sqlx::types::{BigDecimal, JsonRawValue, Uuid, time::{OffsetDateTime, PrimitiveDateTime}}`.

## Verification

All results are from the worktree rebased onto main at 6e86dc2.

- `cargo test -p ostro-bridge --lib --locked -- sql::`: **32 passed**. Covers the gate tables, bypass forms, value mapping, config, the DDL/key/stored-bytes pins, the horizon, error mapping and redaction.
- `cargo test -p ostro-bridge --test sql_pg --locked -- --test-threads=1`: 10 passed. 2 non-DB tests really ran: policy/unknown connection/gate/abandon, and the W12 horizon against a refused port. The 8 PG tests SKIPped.
- `cargo test -p ostro-bridge --features failpoints --test sql_pg --locked -- --test-threads=1`: 13 passed, with the same split (the 3 failpoint PG tests SKIPped).
- `cargo test -p ostro-bridge --test sql_mysql --locked -- --test-threads=1`: 8 passed (all SKIP). With `--features failpoints`: 10 passed (all SKIP).
- Full `cargo test -p ostro-bridge --locked -- --test-threads=1`, all ok: lib 142, driver 14, egress_client 5, http_connector 24, ratelimit 4, secrets 10, sql_mysql 8, sql_pg 10, ssrf_table 5. With `--features failpoints`: driver 15, http_connector 25, sql_mysql 10, sql_pg 13, all ok.
- `cargo test -p ostro-testkit --features db,mock-http,canary --locked`: 16 passed.
- `cargo clippy -p ostro-bridge -p ostro-testkit --all-targets --all-features --locked -- -D warnings`: clean. `-p ostro-bridge --no-default-features`: clean. The build is warning-free.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug] sqlx statement-cache type clash on PostgreSQL**
- **Found during:** Task 2 (reading the sqlx-postgres executor source).
- **Issue:** sqlx caches prepared statements per connection, keyed by SQL text only. If the same SQL runs once with a NULL parameter (inferred `int4`) and later with an `Int8` parameter, the second run sends 8-byte binary data to a statement prepared for `int4`.
- **Fix:** each PG user statement gets a trailing `\n/* ostro:<param type tags> */` comment and is prepared with the exact bind types. `pg_value_round_trip` asserts NULL → Int8 → NULL on the same SQL.
- **Files:** `sql/exactly_once.rs`.

**2. [Rule 3 - Blocking] No `Stream` trait available to ostro-bridge**
- **Issue:** `futures-core` is not a direct dependency, and only 06-02 edits Cargo files. So `fetch_many` (rows plus rows_affected in one streamed pass) cannot be polled.
- **Fix:** the statement is prepared to learn its columns. With no columns it runs `execute` (server rows_affected). With columns it runs `fetch_all`, and the caps are checked while decoding.
- **Consequence:** an oversized result is fully fetched into memory before `response_too_large`. The cap still rolls back, but it does not bound memory during the fetch. A follow-up could add `futures-core` (06-02 scope) to stream with an early stop.

**3. [Rule 2 - Missing critical] `created_at` index on the token table**
- **Fix:** without it, batched GC scans the whole table every 10 minutes. The index is added and pinned in the DDL test. The column layout is unchanged.

**4. [Rule 2 - Security] Policy check before the existence check**
- **Fix:** an unauthorised module gets `egress_denied` whether or not the connection exists.

### Other notes

- `exactly_once = false` is plain autocommit and documented as at-least-once. A cap failure on that path cannot undo the statement.
- Pre-1970 timestamps cannot be represented (`Timestamptz.seconds` is `u64`), so they decode as `sql_unsupported_type`.
- `PgConnectOptions::new_without_pgpass` still reads libpq env vars, including `PGPASSWORD`, for a connection without a password template. This is harmless for the server (operator-owned env) and is noted for 06-20 docs.
- Real two-transaction deadlock tests are included for both dialects. On PG the effect starts waiting first, so its `deadlock_timeout` check fires first. On MySQL the test transaction modifies 50 filler rows, so InnoDB picks the effect as the victim. Signalled 40001/40P01 (PG) and 40001/1205 (MySQL) cover the mapping deterministically.

## Deferred / human_needed

- **The PG leg is mandatory locally (plan, D-FX5), but it has not run.** A credentialed `OSTRO_TEST_PG_URL` is needed (see the table above). Every PG integration test is written and compiles, and none has been executed against a server yet.
- MySQL leg: CI service (06-18 wires the ci.yml services).

## Self-Check: PASSED

- All 10 files in files_modified exist on main.
- Commits 6532976 (Task 1) and 6e86dc2 (Task 2) are on main.
