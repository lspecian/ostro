//! Lazily built connection pools, one per named connection.
//!
//! A pool is built on first use and rebuilt when the connection's kind or config bytes change
//! or when it is older than `[connectors] backend_ttl` (so rotated passwords are picked up).
//! Building one:
//!
//! 1. resolves the `{{secret:..}}` placeholders of `password` through
//!    [`SecretResolver::resolve_operator`] (P-7; operator-owned, no module grant);
//! 2. sets TLS from `ssl_mode` with the native roots plus `ca_file` (D-FX2) and, on
//!    PostgreSQL, `search_path` from `schema` as a connect option, so every pooled connection
//!    uses it (W3);
//! 3. with `manage_schema` (default), runs the token-table DDL ([`ddl`]) on its own,
//!    **outside** any effect transaction (on MySQL DDL commits implicitly). With
//!    `manage_schema = false` the DDL is logged at info level for the operator instead.
//!
//! The password reaches sqlx's connect options (sqlx keeps it as a `String`; honest limit:
//! not zeroized there) and a [`Scrubber`] used to redact error messages. It never reaches a
//! log line, a `Debug` impl or a guest-visible message.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_meta::records::Connection;
use ostro_types::journal::Failure;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlSslMode};
use sqlx::pool::PoolOptions;
use sqlx::postgres::{PgConnectOptions, PgPool, PgSslMode};
use sqlx::{AssertSqlSafe, MySql, Postgres};

use super::config::{Dialect, SqlConnConfig, SslMode};
use super::{map_error, redact_urls};
use crate::kinds::CONNECTION_UNKNOWN;
use crate::outcome::non_retryable;
use crate::secrets::{Scrubber, SecretResolver, SecretValue, template};

/// How long a pool waits for a free connection.
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);

/// The token-table DDL of `dialect` for table `table` (a validated identifier). The column
/// layout is a persistent contract (pinned by tests); the `created_at` index serves GC.
pub fn ddl(dialect: Dialect, table: &str) -> Vec<String> {
    match dialect {
        Dialect::Postgres => vec![
            format!(
                "CREATE TABLE IF NOT EXISTS {table} (effect_key bytea PRIMARY KEY, \
                 run_id text NOT NULL, op bigint NOT NULL, result bytea, \
                 created_at timestamptz NOT NULL DEFAULT now())"
            ),
            format!("CREATE INDEX IF NOT EXISTS {table}_created_at ON {table} (created_at)"),
        ],
        Dialect::Mysql => vec![format!(
            "CREATE TABLE IF NOT EXISTS {table} (effect_key BINARY(32) PRIMARY KEY, \
             run_id VARCHAR(64) NOT NULL, op BIGINT UNSIGNED NOT NULL, result LONGBLOB, \
             created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
             KEY {table}_created_at (created_at)) ENGINE=InnoDB"
        )],
    }
}

/// A pool of either dialect.
#[derive(Clone, Debug)]
pub enum DbPool {
    /// PostgreSQL.
    Pg(PgPool),
    /// MySQL.
    My(MySqlPool),
}

/// A built pool with the config it was built from.
pub struct PoolEntry {
    /// The pool.
    pub pool: DbPool,
    /// The parsed config.
    pub cfg: SqlConnConfig,
    /// Connection name.
    pub name: String,
    kind: String,
    raw: Vec<u8>,
    built: Instant,
    scrub: Scrubber,
}

impl std::fmt::Debug for PoolEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolEntry")
            .field("name", &self.name)
            .field("dialect", &self.cfg.dialect)
            .finish_non_exhaustive()
    }
}

impl PoolEntry {
    /// `msg` with the resolved password (and its encodings) and URL credentials removed.
    pub fn redact(&self, msg: &str) -> String {
        redact_urls(&self.scrub.scrub_str(msg))
    }

    /// Maps a sqlx error raised before COMMIT (see [`map_error`]), redacted.
    pub fn failure(&self, e: &sqlx::Error) -> Failure {
        map_error(e, |m| self.redact(m))
    }
}

/// The per-connection pool cache.
#[derive(Default)]
pub struct PoolCache {
    entries: tokio::sync::Mutex<HashMap<String, Arc<PoolEntry>>>,
}

impl std::fmt::Debug for PoolCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolCache").finish_non_exhaustive()
    }
}

impl PoolCache {
    /// The pool of `conn` (parsed as `cfg`), building or rebuilding it when needed.
    pub async fn get(
        &self,
        conn: &Connection,
        cfg: &SqlConnConfig,
        resolver: &SecretResolver,
        ttl: Duration,
    ) -> Result<Arc<PoolEntry>, Failure> {
        let mut map = self.entries.lock().await;
        if let Some(e) = map.get(&conn.name)
            && e.kind == conn.kind
            && e.raw == conn.config
            && e.built.elapsed() < ttl
        {
            return Ok(e.clone());
        }
        let entry = Arc::new(build(conn, cfg, resolver).await?);
        map.insert(conn.name.clone(), entry.clone());
        Ok(entry)
    }

    /// Every built pool (GC walks these).
    pub async fn entries(&self) -> Vec<Arc<PoolEntry>> {
        self.entries.lock().await.values().cloned().collect()
    }
}

fn config_failure(name: &str, reason: String) -> Failure {
    non_retryable(
        CONNECTION_UNKNOWN,
        format!("connection {name:?}: {reason}"),
        Some(serde_json::json!({ "connection": name })),
    )
}

async fn resolve_password(
    name: &str,
    template_str: &str,
    resolver: &SecretResolver,
) -> Result<(String, Vec<SecretValue>), Failure> {
    let placeholders =
        template::parse(template_str).map_err(|e| config_failure(name, e.to_string()))?;
    let mut values = BTreeMap::new();
    for p in &placeholders {
        if !values.contains_key(&p.name) {
            values.insert(p.name.clone(), resolver.resolve_operator(&p.name).await?);
        }
    }
    let expanded = template::expand(template_str, &values);
    let password = std::str::from_utf8(&expanded)
        .map_err(|_| config_failure(name, "the resolved password is not UTF-8".into()))?
        .to_owned();
    Ok((password, values.into_values().collect()))
}

async fn build(
    conn: &Connection,
    cfg: &SqlConnConfig,
    resolver: &SecretResolver,
) -> Result<PoolEntry, Failure> {
    let (password, secrets) = match &cfg.password {
        Some(t) => {
            let (p, s) = resolve_password(&conn.name, t, resolver).await?;
            (Some(p), s)
        }
        None => (None, Vec::new()),
    };
    let scrub = Scrubber::new(&secrets.iter().collect::<Vec<_>>());
    let pool = match cfg.dialect {
        Dialect::Postgres => {
            let mut o = PgConnectOptions::new_without_pgpass()
                .host(&cfg.host)
                .port(cfg.port)
                .username(&cfg.user)
                .database(&cfg.database)
                .application_name("ostro")
                .ssl_mode(match cfg.ssl_mode {
                    SslMode::Disable => PgSslMode::Disable,
                    SslMode::Prefer => PgSslMode::Prefer,
                    SslMode::Require => PgSslMode::Require,
                    SslMode::VerifyCa => PgSslMode::VerifyCa,
                    SslMode::VerifyFull => PgSslMode::VerifyFull,
                });
            if let Some(p) = &password {
                o = o.password(p);
            }
            if let Some(ca) = &cfg.ca_file {
                o = o.ssl_root_cert(ca);
            }
            if let Some(schema) = &cfg.schema {
                o = o.options([("search_path", schema.as_str())]);
            }
            DbPool::Pg(
                PoolOptions::<Postgres>::new()
                    .max_connections(cfg.pool_max)
                    .acquire_timeout(ACQUIRE_TIMEOUT)
                    .connect_lazy_with(o),
            )
        }
        Dialect::Mysql => {
            let mut o = MySqlConnectOptions::new()
                .host(&cfg.host)
                .port(cfg.port)
                .username(&cfg.user)
                .database(&cfg.database)
                .ssl_mode(match cfg.ssl_mode {
                    SslMode::Disable => MySqlSslMode::Disabled,
                    SslMode::Prefer => MySqlSslMode::Preferred,
                    SslMode::Require => MySqlSslMode::Required,
                    SslMode::VerifyCa => MySqlSslMode::VerifyCa,
                    SslMode::VerifyFull => MySqlSslMode::VerifyIdentity,
                });
            if let Some(p) = &password {
                o = o.password(p);
            }
            if let Some(ca) = &cfg.ca_file {
                o = o.ssl_ca(ca);
            }
            DbPool::My(
                PoolOptions::<MySql>::new()
                    .max_connections(cfg.pool_max)
                    .acquire_timeout(ACQUIRE_TIMEOUT)
                    .connect_lazy_with(o),
            )
        }
    };
    drop(password);
    let entry = PoolEntry {
        pool,
        cfg: cfg.clone(),
        name: conn.name.clone(),
        kind: conn.kind.clone(),
        raw: conn.config.clone(),
        built: Instant::now(),
        scrub,
    };
    let statements = ddl(cfg.dialect, &cfg.effects_table);
    if cfg.manage_schema {
        for s in statements {
            let r = match &entry.pool {
                DbPool::Pg(p) => sqlx::query(AssertSqlSafe(s)).execute(p).await.map(drop),
                DbPool::My(p) => sqlx::query(AssertSqlSafe(s)).execute(p).await.map(drop),
            };
            r.map_err(|e| entry.failure(&e))?;
        }
    } else {
        tracing::info!(
            connection = %conn.name,
            ddl = %statements.join(";\n"),
            "manage_schema = false: create the exactly-once token table with this DDL"
        );
    }
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The token-table layout is a persistent contract (06-11): never change these strings.
    #[test]
    fn ddl_is_pinned() {
        assert_eq!(
            ddl(Dialect::Postgres, "ostro_effects"),
            vec![
                "CREATE TABLE IF NOT EXISTS ostro_effects (effect_key bytea PRIMARY KEY, \
                 run_id text NOT NULL, op bigint NOT NULL, result bytea, \
                 created_at timestamptz NOT NULL DEFAULT now())"
                    .to_owned(),
                "CREATE INDEX IF NOT EXISTS ostro_effects_created_at ON ostro_effects (created_at)"
                    .to_owned(),
            ]
        );
        assert_eq!(
            ddl(Dialect::Mysql, "ostro_effects"),
            vec![
                "CREATE TABLE IF NOT EXISTS ostro_effects (effect_key BINARY(32) PRIMARY KEY, \
                 run_id VARCHAR(64) NOT NULL, op BIGINT UNSIGNED NOT NULL, result LONGBLOB, \
                 created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
                 KEY ostro_effects_created_at (created_at)) ENGINE=InnoDB"
                    .to_owned()
            ]
        );
    }
}
