//! Env-gated PostgreSQL / MySQL test databases (Phase 6, plan 06-11, feature `db`).
//!
//! - [`pg`] reads `OSTRO_TEST_PG_URL`, creates a schema `t_<uuid-simple>` and returns a
//!   [`PgTestDb`] whose [`pool`](PgTestDb::pool) uses that schema as its `search_path`.
//! - [`mysql`] reads `OSTRO_TEST_MYSQL_URL` (with `ssl-mode=REQUIRED` in CI), creates a
//!   database `t_<uuid-simple>` and returns a [`MyTestDb`].
//!
//! When the variable is unset (or empty) both print a loud
//! `SKIP: <VAR> unset (...)` line and return `None`, so the calling test passes without a
//! server (human_needed locally for MySQL; CI service containers run them, D-FX5).
//!
//! The schema / database is dropped on `Drop`, from a separate thread with its own runtime
//! (Drop can run inside or outside a tokio runtime). The URL holds credentials: never print
//! it; use [`UrlParts`] to build connection configs.

use std::str::FromStr;
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::{AssertSqlSafe, Executor};

/// PostgreSQL URL variable.
pub const PG_URL_ENV: &str = "OSTRO_TEST_PG_URL";
/// MySQL URL variable.
pub const MYSQL_URL_ENV: &str = "OSTRO_TEST_MYSQL_URL";

/// The URL in `var`, or `None` after printing the SKIP line.
pub fn env_url(var: &str, why: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_owned()),
        _ => {
            eprintln!("SKIP: {var} unset ({why})");
            None
        }
    }
}

/// The parts of a `scheme://user:password@host:port/database?k=v` URL (percent-decoded).
#[derive(Clone, PartialEq, Eq)]
pub struct UrlParts {
    /// Host.
    pub host: String,
    /// Port, if given.
    pub port: Option<u16>,
    /// User name.
    pub user: String,
    /// Password, if given.
    pub password: Option<String>,
    /// Database, if given.
    pub database: Option<String>,
    /// Query parameters.
    pub params: Vec<(String, String)>,
}

impl std::fmt::Debug for UrlParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UrlParts")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("database", &self.database)
            .field("params", &self.params)
            .finish()
    }
}

impl UrlParts {
    /// The value of query parameter `k`.
    pub fn param(&self, k: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.as_str())
    }
}

fn hex_val(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let (Some(h), Some(l)) = (hex_val(b[i + 1]), hex_val(b[i + 2]))
        {
            out.push(h << 4 | l);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parses a database URL; `None` if it has no `scheme://`.
pub fn parse_url(url: &str) -> Option<UrlParts> {
    let (_, rest) = url.split_once("://")?;
    let (main, query) = match rest.split_once('?') {
        Some((m, q)) => (m, q),
        None => (rest, ""),
    };
    let (authority, path) = match main.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (main, None),
    };
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    let (user, password) = match userinfo {
        Some(u) => match u.split_once(':') {
            Some((n, p)) => (percent_decode(n), Some(percent_decode(p))),
            None => (percent_decode(u), None),
        },
        None => (String::new(), None),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || hostport.starts_with('[') => {
            (h.to_owned(), Some(p.parse().ok()?))
        }
        _ => (hostport.to_owned(), None),
    };
    let params = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(kv), String::new()),
        })
        .collect();
    Some(UrlParts {
        host,
        port,
        user,
        password,
        database: path.filter(|p| !p.is_empty()).map(percent_decode),
        params,
    })
}

fn unique_name() -> String {
    format!("t_{}", uuid::Uuid::now_v7().simple())
}

/// Runs `stmt` on a fresh connection from a separate thread (Drop cleanup).
fn cleanup_blocking<F>(f: F)
where
    F: FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + 'static,
{
    let t = std::thread::spawn(move || {
        if let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            rt.block_on(async {
                let _ = tokio::time::timeout(Duration::from_secs(30), f()).await;
            });
        }
    });
    let _ = t.join();
}

/// A PostgreSQL schema `t_<uuid>` for one test.
pub struct PgTestDb {
    url: String,
    parts: UrlParts,
    schema: String,
    pool: PgPool,
}

impl std::fmt::Debug for PgTestDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgTestDb")
            .field("schema", &self.schema)
            .field("parts", &self.parts)
            .finish_non_exhaustive()
    }
}

/// A fresh PostgreSQL schema, or `None` (after a SKIP line) when `OSTRO_TEST_PG_URL` is
/// unset. Panics if the server is unreachable: a set variable means the leg must run.
pub async fn pg() -> Option<PgTestDb> {
    let url = env_url(PG_URL_ENV, "PostgreSQL legs need a reachable server")?;
    let parts = parse_url(&url).expect("OSTRO_TEST_PG_URL is not a postgres:// URL");
    let opts = PgConnectOptions::from_str(&url).expect("OSTRO_TEST_PG_URL does not parse");
    let schema = unique_name();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts.clone())
        .await
        .unwrap_or_else(|e| panic!("cannot connect to OSTRO_TEST_PG_URL: {e}"));
    admin
        .execute(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await
        .unwrap_or_else(|e| panic!("CREATE SCHEMA {schema}: {e}"));
    admin.close().await;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_lazy_with(opts.options([("search_path", schema.as_str())]));
    Some(PgTestDb {
        url,
        parts,
        schema,
        pool,
    })
}

impl PgTestDb {
    /// The schema name (`t_<uuid-simple>`), for a connection config's `schema` field.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// The URL parts (host, port, user, password, database).
    pub fn parts(&self) -> &UrlParts {
        &self.parts
    }

    /// A pool whose `search_path` is [`schema`](Self::schema).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

impl Drop for PgTestDb {
    fn drop(&mut self) {
        let url = self.url.clone();
        let schema = self.schema.clone();
        cleanup_blocking(move || {
            Box::pin(async move {
                if let Ok(p) = PgPool::connect(&url).await {
                    let _ = p
                        .execute(AssertSqlSafe(format!(
                            "DROP SCHEMA IF EXISTS {schema} CASCADE"
                        )))
                        .await;
                    p.close().await;
                }
            })
        });
    }
}

/// A MySQL database `t_<uuid>` for one test.
pub struct MyTestDb {
    url: String,
    parts: UrlParts,
    database: String,
    pool: MySqlPool,
}

impl std::fmt::Debug for MyTestDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MyTestDb")
            .field("database", &self.database)
            .field("parts", &self.parts)
            .finish_non_exhaustive()
    }
}

/// A fresh MySQL database, or `None` (after a SKIP line) when `OSTRO_TEST_MYSQL_URL` is
/// unset. Panics if the server is unreachable.
pub async fn mysql() -> Option<MyTestDb> {
    let url = env_url(MYSQL_URL_ENV, "human_needed / CI service")?;
    let parts = parse_url(&url).expect("OSTRO_TEST_MYSQL_URL is not a mysql:// URL");
    let opts = MySqlConnectOptions::from_str(&url).expect("OSTRO_TEST_MYSQL_URL does not parse");
    let database = unique_name();
    let admin = MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(opts.clone())
        .await
        .unwrap_or_else(|e| panic!("cannot connect to OSTRO_TEST_MYSQL_URL: {e}"));
    admin
        .execute(AssertSqlSafe(format!("CREATE DATABASE {database}")))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE {database}: {e}"));
    admin.close().await;
    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .connect_lazy_with(opts.database(&database));
    Some(MyTestDb {
        url,
        parts,
        database,
        pool,
    })
}

impl MyTestDb {
    /// The database name (`t_<uuid-simple>`).
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The URL parts (host, port, user, password).
    pub fn parts(&self) -> &UrlParts {
        &self.parts
    }

    /// A pool using [`database`](Self::database).
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}

impl Drop for MyTestDb {
    fn drop(&mut self) {
        let url = self.url.clone();
        let database = self.database.clone();
        cleanup_blocking(move || {
            Box::pin(async move {
                if let Ok(p) = MySqlPool::connect(&url).await {
                    let _ = p
                        .execute(AssertSqlSafe(format!("DROP DATABASE IF EXISTS {database}")))
                        .await;
                    p.close().await;
                }
            })
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_urls() {
        let p = parse_url("postgres://ostro:p%40ss%3Aw@127.0.0.1:5432/postgres?sslmode=disable")
            .unwrap();
        assert_eq!(p.host, "127.0.0.1");
        assert_eq!(p.port, Some(5432));
        assert_eq!(p.user, "ostro");
        assert_eq!(p.password.as_deref(), Some("p@ss:w"));
        assert_eq!(p.database.as_deref(), Some("postgres"));
        assert_eq!(p.param("SSLMODE"), Some("disable"));
        assert!(
            !format!("{p:?}").contains("p@ss"),
            "Debug redacts the password"
        );

        let p = parse_url("mysql://root@db/x").unwrap();
        assert_eq!((p.host.as_str(), p.port, p.password), ("db", None, None));
        let p = parse_url("postgres://h").unwrap();
        assert_eq!((p.user.as_str(), p.database), ("", None));
        assert!(parse_url("not a url").is_none());
        assert!(parse_url("postgres://h:notaport/x").is_none());
    }

    #[test]
    fn percent_decoding() {
        assert_eq!(percent_decode("a%20b%2"), "a b%2");
        assert_eq!(percent_decode("%zz"), "%zz");
        assert_eq!(percent_decode("%41"), "A");
    }

    #[test]
    fn unique_names_are_identifiers() {
        let n = unique_name();
        assert!(n.starts_with("t_") && n.len() == 34, "{n}");
        assert!(n.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'));
        assert_ne!(unique_name(), n);
    }

    #[tokio::test]
    async fn skips_when_unset() {
        // Only meaningful when the variables are unset; otherwise the helpers connect.
        if std::env::var(PG_URL_ENV).is_err() {
            assert!(pg().await.is_none());
        }
        if std::env::var(MYSQL_URL_ENV).is_err() {
            assert!(mysql().await.is_none());
        }
    }
}
