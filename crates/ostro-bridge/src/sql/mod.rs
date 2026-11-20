//! The SQL connector (06-11, FX-04): PostgreSQL and MySQL pools per named connection, the
//! exactly-once statement allow-list, dynamic value mapping and the token-first exactly-once
//! protocol in `ostro_effects`.
//!
//! - [`config`]: the `sql-postgres` / `sql-mysql` v1 connection config.
//! - [`classify`]: the exactly-once statement gate (allow-list).
//! - [`value`]: `SqlValue` ⇄ sqlx binds and column decoding.
//! - [`pool`]: lazily built pools, password resolution, the token-table DDL.
//! - [`exactly_once`]: the effect key, the token protocol, the reconcile loop (B4) and the
//!   re-drive horizon (W12).
//! - [`gc`]: batched GC of old token rows.
//!
//! # Execution order ([`SqlConnector`])
//!
//! 1. The module must hold `connection:<name>` in its egress policy (P-7) → else
//!    `egress_denied`. The connection must exist with a SQL kind and a valid config → else
//!    `connection_unknown`.
//! 2. Exactly-once statements pass [`classify::gate`] → else `sql_not_transactional`.
//!    Parameters are converted ([`value::to_params`]). Nothing has run yet.
//! 3. Exactly-once re-drives past the horizon complete `effect_outcome_unknown` without
//!    touching the database (W12).
//! 4. The send gate is re-checked (`Abandon` → `Failure::Cancelled`, Pitfall 7).
//! 5. The pool is fetched or built (passwords resolve here).
//! 6. Exactly-once: [`exactly_once::execute`]. Otherwise the statement runs in autocommit
//!    mode on a pooled connection.
//!
//! # `exactly_once = false`
//!
//! Autocommit execution, **at-least-once** on re-drive: a statement whose effect was applied
//! before a crash runs again. Any statement is allowed. The `bridge::sql::*` failpoints are on
//! the exactly-once path only. A result over the row/byte cap fails `response_too_large`,
//! but the statement has already been applied (there is no transaction to roll back).
//!
//! # Errors
//!
//! Before COMMIT: PostgreSQL `40001`/`40P01` and MySQL `1213`/`1205` → `Retryable{sql_error}`
//! (same key on retry) with `sqlstate`/`errno` in details; transport errors (I/O, pool
//! timeout) → `Retryable{sql_error}`; other database errors → `NonRetryable{sql_error,
//! details: {sqlstate, message}}`. After COMMIT: see [`exactly_once`]. Messages are the
//! database's text with the connection password (and its encodings) and URL credentials
//! removed.

pub mod classify;
pub mod config;
pub mod exactly_once;
pub mod gc;
pub mod pool;
pub mod value;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ostro_meta::records::Connection;
use ostro_types::journal::{EffectRequest, ErrorInfo, Failure, OpOutput, SqlRequest};
use ostro_types::live::{EffectKind, LiveEffect};
use sqlx::mysql::MySqlDatabaseError;

use crate::config::ConnectorsConfig;
use crate::connector::{Connector, EffectCtx, GateDecision, SendGate, Target};
use crate::egress::policy::allows_connection;
use crate::kinds::{CONNECTION_UNKNOWN, EGRESS_DENIED, SQL_ERROR, UNSUPPORTED_OP};
use crate::meta_view::MetaView;
use crate::outcome::{json_payload, non_retryable};
use crate::secrets::SecretResolver;

pub use config::{Dialect, SqlConnConfig};
pub use exactly_once::{
    FP_AFTER_COMMIT, FP_BEFORE_USER_STMT, Reconcile, effect_key, redrive_expired,
};
pub use gc::GcHandle;

use exactly_once::Job;
use pool::{DbPool, PoolCache, PoolEntry};

/// Node-level SQL connector settings.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlConfig {
    /// Default row cap (`[connectors] sql_max_rows`).
    pub max_rows: u64,
    /// Default byte cap (`[connectors] sql_max_bytes`).
    pub max_bytes: u64,
    /// `[connectors] sql_effects_retention` (GC and the re-drive horizon).
    pub effects_retention: Duration,
    /// `[connectors] backend_ttl`: pools older than this are rebuilt.
    pub backend_ttl: Duration,
    /// The reconcile loop after a COMMIT with unknown outcome.
    pub reconcile: Reconcile,
}

impl Default for SqlConfig {
    fn default() -> Self {
        Self::from_connectors(&ConnectorsConfig::default())
    }
}

impl SqlConfig {
    /// The settings from `[connectors]`.
    pub fn from_connectors(c: &ConnectorsConfig) -> Self {
        Self {
            max_rows: c.sql_max_rows,
            max_bytes: c.sql_max_bytes,
            effects_retention: c.sql_effects_retention,
            backend_ttl: c.backend_ttl,
            reconcile: Reconcile::default(),
        }
    }
}

/// The SQL connector ([`EffectKind::Sql`]).
pub struct SqlConnector {
    meta: Arc<dyn MetaView>,
    resolver: SecretResolver,
    cluster_id: String,
    cfg: SqlConfig,
    pools: PoolCache,
}

impl std::fmt::Debug for SqlConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlConnector")
            .field("cluster_id", &self.cluster_id)
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl SqlConnector {
    /// A connector reading connections and egress policies from `meta` and resolving
    /// connection passwords through `resolver`. `cluster_id` is part of every effect key.
    pub fn new(
        meta: Arc<dyn MetaView>,
        resolver: SecretResolver,
        cluster_id: impl Into<String>,
        cfg: SqlConfig,
    ) -> Self {
        Self {
            meta,
            resolver,
            cluster_id: cluster_id.into(),
            cfg,
            pools: PoolCache::default(),
        }
    }

    /// The node-level settings.
    pub fn config(&self) -> &SqlConfig {
        &self.cfg
    }

    /// The effect key of `(run_id, op)` on this cluster.
    pub fn effect_key(&self, run_id: &str, op: u64) -> [u8; 32] {
        effect_key(&self.cluster_id, run_id, op)
    }

    /// The connection `name` for `module` (P-7), with its parsed config.
    fn connection(&self, module: &str, name: &str) -> Result<(Connection, SqlConnConfig), Failure> {
        if !allows_connection(self.meta.egress_policy(module).as_ref(), name) {
            return Err(non_retryable(
                EGRESS_DENIED,
                format!("module {module:?} may not use connection {name:?}"),
                Some(serde_json::json!({ "connection": name })),
            ));
        }
        let conn = self
            .meta
            .connection(name)
            .filter(|c| Dialect::from_kind(&c.kind).is_some())
            .ok_or_else(|| {
                non_retryable(
                    CONNECTION_UNKNOWN,
                    format!("no SQL connection named {name:?}"),
                    Some(serde_json::json!({ "connection": name })),
                )
            })?;
        let cfg = SqlConnConfig::parse(&conn.name, &conn.kind, &conn.config)
            .map_err(|e| e.to_failure())?;
        Ok((conn, cfg))
    }

    async fn run(
        &self,
        ctx: &EffectCtx,
        effect: &LiveEffect,
        req: &SqlRequest,
        gate: &dyn SendGate,
    ) -> Result<OpOutput, Failure> {
        let (conn, cfg) = self.connection(&ctx.module, &req.connection)?;
        if req.exactly_once {
            classify::gate(&req.statement, cfg.dialect, cfg.allow_call)?;
        }
        let params = value::to_params(&req.params)?;
        if req.exactly_once
            && redrive_expired(
                effect.redrive,
                effect.scheduled_ts_nanos,
                now_nanos(),
                self.cfg.effects_retention,
            )
        {
            return Err(exactly_once::outcome_unknown(
                "re-drive older than the ostro_effects retention",
                effect.op,
                &conn.name,
            ));
        }
        if gate.check(ctx.partition, &ctx.run_id, effect.op).await == GateDecision::Abandon {
            ctx.cancel.cancel();
            return Err(Failure::Cancelled);
        }
        let entry = self
            .pools
            .get(&conn, &cfg, &self.resolver, self.cfg.backend_ttl)
            .await?;
        let job = Job {
            key: self.effect_key(&ctx.run_id, effect.op),
            run_id: &ctx.run_id,
            op: effect.op,
            statement: &req.statement,
            params: &params,
            max_rows: cfg.max_rows.unwrap_or(self.cfg.max_rows),
            max_bytes: cfg.max_bytes.unwrap_or(self.cfg.max_bytes),
        };
        if req.exactly_once {
            exactly_once::execute(&entry, &job, &self.cfg.reconcile).await
        } else {
            autocommit(&entry, &job).await
        }
    }
}

async fn autocommit(entry: &PoolEntry, job: &Job<'_>) -> Result<OpOutput, Failure> {
    let r = match &entry.pool {
        DbPool::Pg(p) => {
            let mut c = p.acquire().await.map_err(|e| entry.failure(&e))?;
            exactly_once::stmt_pg(&mut c, job).await
        }
        DbPool::My(p) => {
            let mut c = p.acquire().await.map_err(|e| entry.failure(&e))?;
            exactly_once::stmt_mysql(&mut c, job).await
        }
    };
    r.map(OpOutput::Sql).map_err(|e| match e {
        exactly_once::StmtError::Db(e) => entry.failure(&e),
        exactly_once::StmtError::Fail(f) => f,
    })
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

#[async_trait]
impl Connector for SqlConnector {
    fn kind(&self) -> EffectKind {
        EffectKind::Sql
    }

    fn target(&self, _ctx: &EffectCtx, effect: &LiveEffect) -> Option<Target> {
        let EffectRequest::Sql(req) = &effect.request else {
            return None;
        };
        let quota = self
            .meta
            .connection(&req.connection)
            .and_then(|c| SqlConnConfig::parse(&c.name, &c.kind, &c.config).ok())
            .and_then(|c| c.quota);
        Some(Target {
            key: req.connection.clone(),
            quota,
        })
    }

    async fn execute(
        &self,
        ctx: &EffectCtx,
        effect: &LiveEffect,
        gate: &dyn SendGate,
    ) -> Result<OpOutput, Failure> {
        let EffectRequest::Sql(req) = &effect.request else {
            return Err(non_retryable(
                UNSUPPORTED_OP,
                "the SQL connector only executes sql-execute effects",
                None,
            ));
        };
        self.run(ctx, effect, req, gate).await
    }
}

/// Serialization failures, deadlocks and lock-wait timeouts: retryable with the same key.
pub fn is_retryable_db(sqlstate: Option<&str>, errno: Option<u16>) -> bool {
    matches!(sqlstate, Some("40001" | "40P01")) || matches!(errno, Some(1213 | 1205))
}

/// Whether `e` is a unique violation (PostgreSQL 23505, MySQL 1062).
pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    let Some(db) = e.as_database_error() else {
        return false;
    };
    db.is_unique_violation()
        || db.code().as_deref() == Some("23505")
        || db
            .try_downcast_ref::<MySqlDatabaseError>()
            .is_some_and(|m| m.number() == 1062)
}

/// Maps a sqlx error raised **before** COMMIT (see the module docs); `redact` cleans
/// messages.
pub(crate) fn map_error(e: &sqlx::Error, redact: impl Fn(&str) -> String) -> Failure {
    let info = |message: String, details: Option<serde_json::Value>| ErrorInfo {
        kind: SQL_ERROR.to_owned(),
        message,
        details: details.as_ref().map(json_payload),
    };
    match e {
        sqlx::Error::Database(db) => {
            let sqlstate = db.code().map(|c| c.into_owned());
            let errno = db
                .try_downcast_ref::<MySqlDatabaseError>()
                .map(|m| m.number());
            let message = redact(db.message());
            let mut details = serde_json::json!({ "sqlstate": sqlstate, "message": message });
            if let Some(n) = errno {
                details["errno"] = n.into();
            }
            let i = info(message, Some(details));
            if is_retryable_db(sqlstate.as_deref(), errno) {
                Failure::Retryable(i)
            } else {
                Failure::NonRetryable(i)
            }
        }
        sqlx::Error::Io(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Protocol(_)
        | sqlx::Error::BeginFailed => Failure::Retryable(info(redact(&e.to_string()), None)),
        _ => Failure::NonRetryable(info(redact(&e.to_string()), None)),
    }
}

/// `s` with the credentials of every `scheme://user:pass@host` authority replaced by `***`.
pub(crate) fn redact_urls(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find("://") {
        let (head, tail) = rest.split_at(i + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| {
                matches!(c, '/' | '"' | '\'' | '?' | '#' | '>' | ')') || c.is_whitespace()
            })
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority.rfind('@') {
            Some(at) => {
                out.push_str("***");
                out.push_str(&authority[at..]);
            }
            None => out.push_str(authority),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[derive(Debug)]
    struct FakeDbError {
        code: &'static str,
        message: &'static str,
        unique: bool,
    }

    impl std::fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }

    impl std::error::Error for FakeDbError {}

    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            self.message
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.code))
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            if self.unique {
                sqlx::error::ErrorKind::UniqueViolation
            } else {
                sqlx::error::ErrorKind::Other
            }
        }
    }

    fn db(code: &'static str, message: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError {
            code,
            message,
            unique: code == "23505",
        }))
    }

    #[test]
    fn retryable_classes() {
        assert!(is_retryable_db(Some("40001"), None));
        assert!(is_retryable_db(Some("40P01"), None));
        assert!(is_retryable_db(Some("HY000"), Some(1205)));
        assert!(is_retryable_db(Some("40001"), Some(1213)));
        assert!(!is_retryable_db(Some("23505"), None));
        assert!(!is_retryable_db(Some("42601"), Some(1064)));
        assert!(!is_retryable_db(None, None));
    }

    #[test]
    fn error_mapping() {
        let id = |s: &str| s.to_owned();
        let Failure::Retryable(e) = map_error(&db("40P01", "deadlock detected"), id) else {
            panic!("deadlock is retryable")
        };
        assert_eq!(e.kind, SQL_ERROR);
        let d: serde_json::Value = serde_json::from_slice(&e.details.unwrap().data).unwrap();
        assert_eq!(d["sqlstate"], "40P01");

        let Failure::NonRetryable(e) = map_error(&db("42601", "syntax error"), id) else {
            panic!("syntax error is final")
        };
        let d: serde_json::Value = serde_json::from_slice(&e.details.unwrap().data).unwrap();
        assert_eq!(
            d,
            serde_json::json!({"sqlstate": "42601", "message": "syntax error"})
        );

        assert!(matches!(
            map_error(&sqlx::Error::PoolTimedOut, id),
            Failure::Retryable(_)
        ));
        assert!(matches!(
            map_error(&sqlx::Error::Io(std::io::Error::other("reset")), id),
            Failure::Retryable(_)
        ));
        assert!(matches!(
            map_error(&sqlx::Error::ColumnNotFound("x".into()), id),
            Failure::NonRetryable(_)
        ));
        let redacted = map_error(&db("28P01", "auth failed for hunter22hunter22"), |s| {
            s.replace("hunter22hunter22", "[REDACTED]")
        });
        assert!(!format!("{redacted:?}").contains("hunter22"));
    }

    #[test]
    fn unique_violation_detection() {
        assert!(is_unique_violation(&db("23505", "duplicate key")));
        assert!(!is_unique_violation(&db("40001", "serialization")));
        assert!(!is_unique_violation(&sqlx::Error::PoolClosed));
    }

    #[test]
    fn url_credentials_are_redacted() {
        assert_eq!(
            redact_urls("connect postgres://u:secretpw@db:5432/app failed"),
            "connect postgres://***@db:5432/app failed"
        );
        assert_eq!(
            redact_urls("a mysql://root@h/x and http://h/p"),
            "a mysql://***@h/x and http://h/p"
        );
        assert_eq!(redact_urls("no url here"), "no url here");
        assert_eq!(redact_urls("x://"), "x://");
    }

    #[test]
    fn sql_config_from_connectors() {
        let c = SqlConfig::default();
        assert_eq!(c.max_rows, 10_000);
        assert_eq!(c.max_bytes, 4 * 1024 * 1024);
        assert_eq!(c.effects_retention, Duration::from_secs(168 * 3600));
        assert_eq!(c.reconcile, Reconcile::default());
    }
}
