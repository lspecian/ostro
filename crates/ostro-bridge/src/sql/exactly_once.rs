//! The token-first exactly-once protocol (FX-04, P-19) and the statement runner.
//!
//! ```text
//! key = sha256("ostro/sql-effect/v1" ‖ cluster_id ‖ run_id ‖ op_le)          (32 bytes)
//! 1. BEGIN
//! 2. INSERT INTO ostro_effects(effect_key, run_id, op)  -- token FIRST: concurrent dupes block
//!      unique violation (PG 23505 / MySQL 1062) → ROLLBACK; SELECT result:
//!        NULL → NonRetryable{sql_exactly_once_violated}; else the stored bytes, verbatim
//! 3. failpoint bridge::sql::before_user_stmt
//! 4. the user statement (sqlx::query with binds; rows capped → ROLLBACK, response_too_large)
//! 5. UPDATE ostro_effects SET result = stored(encode_outcome(Ok(Sql(result))))
//! 6. COMMIT
//! 7. the connection goes back to the pool, then failpoint bridge::sql::after_commit (W4)
//! ```
//!
//! **Stored bytes** ([`encode_stored`]): `codec_ver (1 byte) ‖ RecordedResult.bytes`, exactly
//! the completion the driver submits, so live and re-drive completions are byte-identical.
//! This layout is a persistent contract (pinned by tests).
//!
//! **After COMMIT (B4).** Once COMMIT has been sent, the outcome is never completed
//! Retryable. A COMMIT error, a lost connection or `after_commit=return(error)` starts a
//! bounded reconcile loop with the same key on a fresh pooled connection (default 5 attempts,
//! backoff 200 ms doubling to 5 s): a stored result → returned verbatim; a NULL result →
//! `sql_exactly_once_violated`; no row (the COMMIT never landed) → the whole protocol runs
//! again (safe: nothing was committed). When the loop runs out the effect completes
//! `NonRetryable{effect_outcome_unknown}`. Errors **before** COMMIT stay as mapped (40001,
//! 40P01, 1213 and 1205 are Retryable with the same key).
//!
//! **Re-drive horizon (W12).** A re-drive later than `sql_effects_retention` minus
//! [`REDRIVE_MARGIN`] after `scheduled_ts` completes `effect_outcome_unknown` without touching
//! the database ([`redrive_expired`]): the token row may already be garbage-collected, so
//! running the statement again could apply it twice.
//!
//! **Statement runner.** Columns come from preparing the statement. A statement without
//! result columns runs with `execute` (rows_affected from the server); one with columns runs
//! with `fetch_all` and `rows_affected` is the row count. The row and byte caps are checked
//! while decoding (rows are fetched in full first: ostro-bridge has no `Stream` dependency).
//! PostgreSQL statements carry a trailing `/* ostro:<param types> */` comment so sqlx's
//! per-connection statement cache (keyed by SQL text) never reuses a statement prepared for
//! other parameter types.

use std::time::Duration;

use ostro_types::journal::{
    ErrorInfo, Failure, OpOutput, RecordedResult, SqlResult, decode_result,
};
use sha2::{Digest, Sha256};
use sqlx::mysql::{MySqlConnection, MySqlPool};
use sqlx::postgres::{PgConnection, PgPool, PgTypeInfo, Postgres};
use sqlx::types::time::OffsetDateTime;
use sqlx::types::{BigDecimal, JsonRawValue};
use sqlx::{AssertSqlSafe, Column, Executor, SqlSafeStr, Statement, Type};

use super::config::Dialect;
use super::is_unique_violation;
use super::pool::{DbPool, PoolEntry};
use super::value::{Collector, Param, bind_mysql, bind_pg, decode_mysql_row, decode_pg_row};
use crate::bridge_fail;
use crate::failpoints;
use crate::kinds::{
    EFFECT_OUTCOME_UNKNOWN, RESPONSE_TOO_LARGE, SQL_ERROR, SQL_EXACTLY_ONCE_VIOLATED,
};
use crate::outcome::{encode_outcome, non_retryable};

/// Failpoint between the token insert and the user statement.
pub const FP_BEFORE_USER_STMT: &str = "bridge::sql::before_user_stmt";
/// Failpoint after COMMIT, once the connection is back in the pool (exactly-once path only).
pub const FP_AFTER_COMMIT: &str = "bridge::sql::after_commit";
/// Domain prefix of the effect key.
pub const KEY_DOMAIN: &[u8] = b"ostro/sql-effect/v1";
/// Safety margin subtracted from `sql_effects_retention` for the re-drive horizon (W12).
pub const REDRIVE_MARGIN: Duration = Duration::from_secs(3600);

/// The effect key: `sha256("ostro/sql-effect/v1" ‖ cluster_id ‖ run_id ‖ op_le)`. A
/// persistent contract (pinned by test vectors).
pub fn effect_key(cluster_id: &str, run_id: &str, op: u64) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(KEY_DOMAIN);
    h.update(cluster_id.as_bytes());
    h.update(run_id.as_bytes());
    h.update(op.to_le_bytes());
    h.finalize().into()
}

/// Whether a re-drive is past the horizon (W12): `redrive` and `now - scheduled >
/// retention - 1h`.
pub fn redrive_expired(
    redrive: bool,
    scheduled_ts_nanos: u64,
    now_nanos: u64,
    retention: Duration,
) -> bool {
    if !redrive {
        return false;
    }
    let horizon = retention.saturating_sub(REDRIVE_MARGIN).as_nanos();
    u128::from(now_nanos.saturating_sub(scheduled_ts_nanos)) > horizon
}

/// The `result` column bytes of a recorded outcome: `codec_ver ‖ bytes`.
pub fn encode_stored(r: &RecordedResult) -> Vec<u8> {
    let mut out = Vec::with_capacity(r.bytes.len() + 1);
    out.push(r.codec_ver);
    out.extend_from_slice(&r.bytes);
    out
}

/// Inverse of [`encode_stored`]; `None` for an empty column.
pub fn decode_stored(b: &[u8]) -> Option<RecordedResult> {
    let (&codec_ver, bytes) = b.split_first()?;
    Some(RecordedResult {
        codec_ver,
        bytes: bytes.to_vec(),
    })
}

/// `NonRetryable{sql_exactly_once_violated}`.
pub fn violated(message: &str) -> Failure {
    non_retryable(SQL_EXACTLY_ONCE_VIOLATED, message, None)
}

/// `NonRetryable{effect_outcome_unknown, details: {op, connection}}`.
pub fn outcome_unknown(message: &str, op: u64, connection: &str) -> Failure {
    non_retryable(
        EFFECT_OUTCOME_UNKNOWN,
        message,
        Some(serde_json::json!({ "op": op, "connection": connection })),
    )
}

/// The outcome recorded in a token row's `result` bytes.
pub fn from_stored(b: &[u8]) -> Result<OpOutput, Failure> {
    let rec =
        decode_stored(b).ok_or_else(|| violated("the stored exactly-once result is empty"))?;
    match decode_result::<Result<OpOutput, Failure>>(&rec) {
        Ok(r) => r,
        Err(e) => Err(violated(&format!(
            "the stored exactly-once result could not be decoded: {e}"
        ))),
    }
}

/// One statement execution.
#[derive(Debug)]
pub struct Job<'a> {
    /// Effect key.
    pub key: [u8; 32],
    /// Run id.
    pub run_id: &'a str,
    /// Op id.
    pub op: u64,
    /// The user statement.
    pub statement: &'a str,
    /// Validated parameters.
    pub params: &'a [Param],
    /// Row cap.
    pub max_rows: u64,
    /// Byte cap.
    pub max_bytes: u64,
}

/// A statement failed.
#[derive(Debug)]
pub enum StmtError {
    /// Database or transport error (mapped by the caller).
    Db(sqlx::Error),
    /// A guest-visible failure (cap exceeded, unsupported type).
    Fail(Failure),
}

impl From<sqlx::Error> for StmtError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

fn too_large(job: &Job<'_>) -> StmtError {
    StmtError::Fail(non_retryable(
        RESPONSE_TOO_LARGE,
        format!(
            "the result exceeds the cap of {} rows or {} bytes",
            job.max_rows, job.max_bytes
        ),
        Some(serde_json::json!({ "max_rows": job.max_rows, "max_bytes": job.max_bytes })),
    ))
}

fn collect<R>(
    job: &Job<'_>,
    columns: Vec<String>,
    rows: &[R],
    decode: fn(&R) -> Result<Vec<ostro_types::journal::SqlValue>, Failure>,
) -> Result<SqlResult, StmtError> {
    if rows.len() as u64 > job.max_rows {
        return Err(too_large(job));
    }
    let mut c = Collector::new(job.max_rows, job.max_bytes, &columns);
    if c.over() {
        return Err(too_large(job));
    }
    for r in rows {
        let values = decode(r).map_err(StmtError::Fail)?;
        c.push(values).map_err(|_| too_large(job))?;
    }
    Ok(SqlResult {
        columns,
        rows_affected: rows.len() as u64,
        rows: c.rows,
    })
}

fn pg_param_type(p: &Param) -> (&'static str, PgTypeInfo) {
    match p {
        Param::Null => ("n", PgTypeInfo::with_oid(sqlx::postgres::types::Oid(0))),
        Param::Bool(_) => ("b", <bool as Type<Postgres>>::type_info()),
        Param::I64(_) => ("i8", <i64 as Type<Postgres>>::type_info()),
        Param::F64(_) => ("f8", <f64 as Type<Postgres>>::type_info()),
        Param::Numeric(_) => ("num", <BigDecimal as Type<Postgres>>::type_info()),
        Param::Text(_) => ("t", <String as Type<Postgres>>::type_info()),
        Param::Bytes(_) => ("by", <Vec<u8> as Type<Postgres>>::type_info()),
        Param::Json(_) => ("j", <Box<JsonRawValue> as Type<Postgres>>::type_info()),
        Param::Tstz(_) => ("ts", <OffsetDateTime as Type<Postgres>>::type_info()),
    }
}

/// Runs the user statement on a PostgreSQL connection (inside the effect transaction or in
/// autocommit).
pub async fn stmt_pg(conn: &mut PgConnection, job: &Job<'_>) -> Result<SqlResult, StmtError> {
    let (sig, types): (Vec<&str>, Vec<PgTypeInfo>) = job.params.iter().map(pg_param_type).unzip();
    let sql = format!("{}\n/* ostro:{} */", job.statement, sig.join(","));
    let stmt = (&mut *conn)
        .prepare_with(AssertSqlSafe(sql.clone()).into_sql_str(), &types)
        .await?;
    let columns: Vec<String> = stmt.columns().iter().map(|c| c.name().to_owned()).collect();
    let mut q = sqlx::query(AssertSqlSafe(sql));
    for p in job.params {
        q = bind_pg(q, p);
    }
    if columns.is_empty() {
        let r = q.execute(&mut *conn).await?;
        return Ok(SqlResult {
            columns,
            rows: Vec::new(),
            rows_affected: r.rows_affected(),
        });
    }
    let rows = q.fetch_all(&mut *conn).await?;
    collect(job, columns, &rows, decode_pg_row)
}

/// Runs the user statement on a MySQL connection.
pub async fn stmt_mysql(conn: &mut MySqlConnection, job: &Job<'_>) -> Result<SqlResult, StmtError> {
    let stmt = (&mut *conn)
        .prepare(AssertSqlSafe(job.statement.to_owned()).into_sql_str())
        .await?;
    let columns: Vec<String> = stmt.columns().iter().map(|c| c.name().to_owned()).collect();
    let mut q = sqlx::query(AssertSqlSafe(job.statement.to_owned()));
    for p in job.params {
        q = bind_mysql(q, p);
    }
    if columns.is_empty() {
        let r = q.execute(&mut *conn).await?;
        return Ok(SqlResult {
            columns,
            rows: Vec::new(),
            rows_affected: r.rows_affected(),
        });
    }
    let rows = q.fetch_all(&mut *conn).await?;
    collect(job, columns, &rows, decode_mysql_row)
}

/// Token-table statements of one connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenSql {
    /// `INSERT` of the token row.
    pub insert: String,
    /// `UPDATE` of the result.
    pub update: String,
    /// `SELECT` of the result.
    pub select: String,
}

impl TokenSql {
    /// The statements for `table` (a validated identifier).
    pub fn new(dialect: Dialect, table: &str) -> Self {
        match dialect {
            Dialect::Postgres => Self {
                insert: format!("INSERT INTO {table} (effect_key, run_id, op) VALUES ($1, $2, $3)"),
                update: format!("UPDATE {table} SET result = $1 WHERE effect_key = $2"),
                select: format!("SELECT result FROM {table} WHERE effect_key = $1"),
            },
            Dialect::Mysql => Self {
                insert: format!("INSERT INTO {table} (effect_key, run_id, op) VALUES (?, ?, ?)"),
                update: format!("UPDATE {table} SET result = ? WHERE effect_key = ?"),
                select: format!("SELECT result FROM {table} WHERE effect_key = ?"),
            },
        }
    }
}

/// The outcome of one protocol run.
#[derive(Debug)]
pub enum Attempt {
    /// Decided before COMMIT was sent (or a stored result was found).
    Final(Result<OpOutput, Failure>),
    /// COMMIT was sent and its outcome is unknown.
    CommitUnknown(String),
}

/// The token row: `None` = no row; `Some(None)` = a row with a NULL result.
pub async fn lookup(
    entry: &PoolEntry,
    key: &[u8; 32],
) -> Result<Option<Option<Vec<u8>>>, sqlx::Error> {
    let sql = TokenSql::new(entry.cfg.dialect, &entry.cfg.effects_table).select;
    match &entry.pool {
        DbPool::Pg(p) => {
            sqlx::query_scalar::<_, Option<Vec<u8>>>(AssertSqlSafe(sql))
                .bind(key.to_vec())
                .fetch_optional(p)
                .await
        }
        DbPool::My(p) => {
            sqlx::query_scalar::<_, Option<Vec<u8>>>(AssertSqlSafe(sql))
                .bind(key.to_vec())
                .fetch_optional(p)
                .await
        }
    }
}

async fn on_duplicate(entry: &PoolEntry, key: &[u8; 32]) -> Attempt {
    Attempt::Final(match lookup(entry, key).await {
        Ok(Some(Some(b))) => from_stored(&b),
        Ok(Some(None)) => Err(violated(
            "the exactly-once token row exists without a result: the statement may have run \
             outside the token transaction",
        )),
        // GC removed the row between the insert and the read: nothing committed by us.
        Ok(None) => Err(Failure::Retryable(ErrorInfo {
            kind: SQL_ERROR.to_owned(),
            message: "the exactly-once token row vanished; retry".to_owned(),
            details: None,
        })),
        Err(e) => Err(entry.failure(&e)),
    })
}

fn failpoint_error(name: &str) -> Failure {
    Failure::Retryable(ErrorInfo {
        kind: SQL_ERROR.to_owned(),
        message: format!("failpoint {name}"),
        details: None,
    })
}

macro_rules! protocol_fn {
    ($name:ident, $pool:ty, $stmt:ident, $op:expr) => {
        async fn $name(entry: &PoolEntry, pool: &$pool, job: &Job<'_>) -> Attempt {
            let sql = TokenSql::new(entry.cfg.dialect, &entry.cfg.effects_table);
            let mut tx = match pool.begin().await {
                Ok(t) => t,
                Err(e) => return Attempt::Final(Err(entry.failure(&e))),
            };
            let inserted = sqlx::query(AssertSqlSafe(sql.insert))
                .bind(job.key.to_vec())
                .bind(job.run_id.to_owned())
                .bind($op(job.op))
                .execute(&mut *tx)
                .await;
            if let Err(e) = inserted {
                let _ = tx.rollback().await;
                if is_unique_violation(&e) {
                    return on_duplicate(entry, &job.key).await;
                }
                return Attempt::Final(Err(entry.failure(&e)));
            }
            if let Some(a) = bridge_fail!(FP_BEFORE_USER_STMT) {
                if failpoints::apply(a).await.is_err() {
                    let _ = tx.rollback().await;
                    return Attempt::Final(Err(failpoint_error(FP_BEFORE_USER_STMT)));
                }
            }
            let result = match $stmt(&mut tx, job).await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.rollback().await;
                    return Attempt::Final(Err(match e {
                        StmtError::Db(e) => entry.failure(&e),
                        StmtError::Fail(f) => f,
                    }));
                }
            };
            let out: Result<OpOutput, Failure> = Ok(OpOutput::Sql(result));
            let stored = encode_stored(&encode_outcome(&out));
            let updated = sqlx::query(AssertSqlSafe(sql.update))
                .bind(stored)
                .bind(job.key.to_vec())
                .execute(&mut *tx)
                .await;
            if let Err(e) = updated {
                let _ = tx.rollback().await;
                return Attempt::Final(Err(entry.failure(&e)));
            }
            // From here on the outcome may be committed (B4): never Retryable.
            if let Err(e) = tx.commit().await {
                return Attempt::CommitUnknown(entry.redact(&e.to_string()));
            }
            // `commit` consumed the transaction: its connection is back in the pool before
            // the failpoint can hang (W4).
            if let Some(a) = bridge_fail!(FP_AFTER_COMMIT) {
                if failpoints::apply(a).await.is_err() {
                    return Attempt::CommitUnknown(format!("failpoint {FP_AFTER_COMMIT}"));
                }
            }
            Attempt::Final(out)
        }
    };
}

protocol_fn!(run_pg, PgPool, stmt_pg, |op: u64| op as i64);
protocol_fn!(run_mysql, MySqlPool, stmt_mysql, |op: u64| op);

/// One run of the token protocol.
pub async fn run(entry: &PoolEntry, job: &Job<'_>) -> Attempt {
    match &entry.pool {
        DbPool::Pg(p) => run_pg(entry, p, job).await,
        DbPool::My(p) => run_mysql(entry, p, job).await,
    }
}

/// Reconcile settings (P-19).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reconcile {
    /// Attempts after a COMMIT with unknown outcome (default 5).
    pub attempts: u32,
    /// First backoff (default 200 ms).
    pub initial: Duration,
    /// Backoff cap (default 5 s).
    pub max: Duration,
}

impl Default for Reconcile {
    fn default() -> Self {
        Self {
            attempts: 5,
            initial: Duration::from_millis(200),
            max: Duration::from_secs(5),
        }
    }
}

/// The exactly-once execution: the protocol, then the reconcile loop if COMMIT's outcome is
/// unknown.
pub async fn execute(
    entry: &PoolEntry,
    job: &Job<'_>,
    rc: &Reconcile,
) -> Result<OpOutput, Failure> {
    let why = match run(entry, job).await {
        Attempt::Final(r) => return r,
        Attempt::CommitUnknown(why) => why,
    };
    tracing::warn!(
        connection = %entry.name,
        op = job.op,
        reason = %why,
        "sql exactly-once: COMMIT outcome unknown; reconciling through the token row"
    );
    let mut delay = rc.initial;
    for attempt in 1..=rc.attempts {
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(rc.max);
        match lookup(entry, &job.key).await {
            Ok(Some(Some(b))) => return from_stored(&b),
            Ok(Some(None)) => {
                return Err(violated(
                    "the exactly-once token row was committed without a result",
                ));
            }
            Ok(None) => match run(entry, job).await {
                Attempt::Final(Ok(o)) => return Ok(o),
                Attempt::Final(Err(f @ Failure::NonRetryable(_))) => return Err(f),
                // Never complete Retryable once a COMMIT was sent (B4): try again.
                Attempt::Final(Err(f)) => {
                    tracing::warn!(connection = %entry.name, op = job.op, attempt, failure = ?f,
                        "sql exactly-once: reconcile re-run failed before COMMIT");
                }
                Attempt::CommitUnknown(why) => {
                    tracing::warn!(connection = %entry.name, op = job.op, attempt, reason = %why,
                        "sql exactly-once: reconcile re-run COMMIT outcome unknown");
                }
            },
            Err(e) => {
                tracing::warn!(connection = %entry.name, op = job.op, attempt,
                    error = %entry.redact(&e.to_string()),
                    "sql exactly-once: reconcile lookup failed");
            }
        }
    }
    Err(outcome_unknown(
        "the COMMIT outcome of the exactly-once statement could not be determined",
        job.op,
        &entry.name,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::journal::{SqlValue, encode_result};

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    // Persistent contract: the key derivation never changes (vectors from Python hashlib).
    #[test]
    fn effect_key_vectors() {
        assert_eq!(
            hex(&effect_key("c1", "0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b", 1)),
            "74342fdf6c604ee6c26bf2e51daf8d49a5d8c125cfe7eab5f201c834faa066c5"
        );
        assert_eq!(
            hex(&effect_key("cluster-a", "run-1", 0x0102_0304_0506_0708)),
            "3f3716faef97c69c8d359603d15f651ea8088b71450d58a14be6fa3df6d77cf6"
        );
        assert_ne!(effect_key("c", "r", 1), effect_key("c", "r", 2));
    }

    #[test]
    fn stored_bytes_are_codec_ver_then_bytes() {
        let out: Result<OpOutput, Failure> = Ok(OpOutput::Sql(SqlResult {
            columns: vec!["a".into()],
            rows: vec![vec![SqlValue::Int8(1)]],
            rows_affected: 1,
        }));
        let rec = encode_result(&out);
        let stored = encode_stored(&rec);
        assert_eq!(stored[0], rec.codec_ver);
        assert_eq!(&stored[1..], &rec.bytes[..]);
        assert_eq!(decode_stored(&stored), Some(rec.clone()));
        let back = from_stored(&stored).unwrap();
        assert_eq!(encode_outcome(&Ok(back)), rec, "live == re-drive bytes");
        assert_eq!(decode_stored(&[]), None);
        let Err(Failure::NonRetryable(e)) = from_stored(&[]) else {
            panic!()
        };
        assert_eq!(e.kind, SQL_EXACTLY_ONCE_VIOLATED);
        let Err(Failure::NonRetryable(e)) = from_stored(&[99, 1, 2]) else {
            panic!()
        };
        assert_eq!(e.kind, SQL_EXACTLY_ONCE_VIOLATED);
    }

    #[test]
    fn redrive_horizon() {
        const H: u64 = 3_600_000_000_000;
        let week = Duration::from_secs(168 * 3600);
        // Not a re-drive: never expired.
        assert!(!redrive_expired(false, 0, 1000 * H, week));
        // Within retention - 1h.
        assert!(!redrive_expired(true, 10 * H, 10 * H + 167 * H, week));
        // Past it.
        assert!(redrive_expired(true, 10 * H, 10 * H + 167 * H + 1, week));
        // Clock skew (scheduled in the future): not expired.
        assert!(!redrive_expired(true, 10 * H, 5 * H, week));
        // Retention under the margin: any elapsed time is past the horizon.
        assert!(redrive_expired(true, 0, 1, Duration::from_secs(1800)));
    }

    #[test]
    fn token_sql_per_dialect() {
        let pg = TokenSql::new(Dialect::Postgres, "ostro_effects");
        assert_eq!(
            pg.insert,
            "INSERT INTO ostro_effects (effect_key, run_id, op) VALUES ($1, $2, $3)"
        );
        let my = TokenSql::new(Dialect::Mysql, "fx");
        assert_eq!(my.select, "SELECT result FROM fx WHERE effect_key = ?");
        assert_eq!(my.update, "UPDATE fx SET result = ? WHERE effect_key = ?");
    }

    #[test]
    fn reconcile_defaults() {
        let r = Reconcile::default();
        assert_eq!(r.attempts, 5);
        assert_eq!(r.initial, Duration::from_millis(200));
        assert_eq!(r.max, Duration::from_secs(5));
        let Failure::NonRetryable(e) = outcome_unknown("m", 7, "db") else {
            panic!()
        };
        assert_eq!(e.kind, EFFECT_OUTCOME_UNKNOWN);
        let d: serde_json::Value = serde_json::from_slice(&e.details.unwrap().data).unwrap();
        assert_eq!(d, serde_json::json!({"op": 7, "connection": "db"}));
    }
}
