//! The `sql-postgres` / `sql-mysql` connection config (v1).
//!
//! ```json
//! {"v":1, "host":"db.internal", "port":5432, "database":"app", "schema":"public",
//!  "user":"ostro", "password":"{{secret:db-pass}}", "ssl_mode":"require", "ca_file":"/etc/ca.pem",
//!  "pool_max":10, "effects_table":"ostro_effects", "manage_schema":true, "allow_call":false,
//!  "max_rows":10000, "max_bytes":4194304, "rate":"100/s", "burst":100}
//! ```
//!
//! - `schema` is PostgreSQL only. It becomes the `search_path` connect option, so every pooled
//!   connection uses it (W3).
//! - `password` must contain `{{secret:NAME}}` placeholders. They resolve through
//!   [`SecretResolver::resolve_operator`](crate::secrets::SecretResolver::resolve_operator)
//!   when the pool is built (P-7). A literal password is refused: it would sit in plain text
//!   in the meta store.
//! - `ssl_mode`: PostgreSQL `disable | prefer | require | verify-ca | verify-full` (default
//!   `prefer`, as libpq); MySQL `disabled | preferred | required | verify_ca | verify_identity`
//!   (default `required`, D-FX2). TLS roots are the native roots plus `ca_file`.
//! - `effects_table` and `schema` are interpolated into SQL text, so they must be plain
//!   identifiers (`[A-Za-z_][A-Za-z0-9_]{0,62}`).
//! - `max_rows` / `max_bytes` override the node's `[connectors] sql_max_rows` /
//!   `sql_max_bytes`.
//!
//! Unknown keys, a missing or unknown `"v"` and invalid values are `connection_unknown`, so a
//! typo can never fall back to a default silently.

use std::path::PathBuf;

use ostro_types::journal::Failure;
use serde::Deserialize;

use crate::kinds::CONNECTION_UNKNOWN;
use crate::outcome::non_retryable;
use crate::ratelimit::QuotaSpec;
use crate::secrets::template;

/// Connection kind of a PostgreSQL connection.
pub const KIND_POSTGRES: &str = "sql-postgres";
/// Connection kind of a MySQL connection.
pub const KIND_MYSQL: &str = "sql-mysql";
/// Default token table name.
pub const DEFAULT_EFFECTS_TABLE: &str = "ostro_effects";
/// Default pool size.
pub const DEFAULT_POOL_MAX: u32 = 10;

/// SQL dialect of a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Dialect {
    /// PostgreSQL (`sql-postgres`).
    Postgres,
    /// MySQL 8.0 / 8.4 (`sql-mysql`).
    Mysql,
}

impl Dialect {
    /// The dialect of connection kind `kind`.
    pub fn from_kind(kind: &str) -> Option<Self> {
        match kind {
            KIND_POSTGRES => Some(Self::Postgres),
            KIND_MYSQL => Some(Self::Mysql),
            _ => None,
        }
    }

    /// The connection kind string.
    pub const fn kind(self) -> &'static str {
        match self {
            Self::Postgres => KIND_POSTGRES,
            Self::Mysql => KIND_MYSQL,
        }
    }

    const fn default_port(self) -> u16 {
        match self {
            Self::Postgres => 5432,
            Self::Mysql => 3306,
        }
    }
}

/// TLS mode, normalised across dialects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SslMode {
    /// No TLS.
    Disable,
    /// TLS if the server offers it.
    Prefer,
    /// TLS required, certificate not verified.
    Require,
    /// TLS required, certificate chain verified.
    VerifyCa,
    /// TLS required, chain and host name verified.
    VerifyFull,
}

impl SslMode {
    fn parse(dialect: Dialect, s: &str) -> Option<Self> {
        Some(match (dialect, s) {
            (Dialect::Postgres, "disable") | (Dialect::Mysql, "disabled") => Self::Disable,
            (Dialect::Postgres, "prefer") | (Dialect::Mysql, "preferred") => Self::Prefer,
            (Dialect::Postgres, "require") | (Dialect::Mysql, "required") => Self::Require,
            (Dialect::Postgres, "verify-ca") | (Dialect::Mysql, "verify_ca") => Self::VerifyCa,
            (Dialect::Postgres, "verify-full") | (Dialect::Mysql, "verify_identity") => {
                Self::VerifyFull
            }
            _ => return None,
        })
    }

    const fn default_for(dialect: Dialect) -> Self {
        match dialect {
            Dialect::Postgres => Self::Prefer,
            Dialect::Mysql => Self::Require,
        }
    }
}

/// A parsed SQL connection config. Holds the password **template**, never a value.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlConnConfig {
    /// Dialect (from the connection kind).
    pub dialect: Dialect,
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// PostgreSQL `search_path` (W3).
    pub schema: Option<String>,
    /// User name.
    pub user: String,
    /// Password template (`{{secret:NAME}}`).
    pub password: Option<String>,
    /// TLS mode.
    pub ssl_mode: SslMode,
    /// Extra CA bundle (PEM).
    pub ca_file: Option<PathBuf>,
    /// Pool size.
    pub pool_max: u32,
    /// Token table name.
    pub effects_table: String,
    /// Create the token table when the pool is built.
    pub manage_schema: bool,
    /// Allow `CALL` in exactly-once mode.
    pub allow_call: bool,
    /// Row cap override.
    pub max_rows: Option<u64>,
    /// Byte cap override.
    pub max_bytes: Option<u64>,
    /// Rate-limit quota of the connection.
    pub quota: Option<QuotaSpec>,
}

/// The connection config is invalid (`connection_unknown`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("connection {connection:?}: {reason}")]
pub struct ConfigError {
    /// Connection name.
    pub connection: String,
    /// What is wrong.
    pub reason: String,
}

impl ConfigError {
    /// `NonRetryable{connection_unknown}`.
    pub fn to_failure(&self) -> Failure {
        non_retryable(
            CONNECTION_UNKNOWN,
            self.to_string(),
            Some(serde_json::json!({ "connection": self.connection })),
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    v: u32,
    host: String,
    port: Option<u16>,
    database: String,
    schema: Option<String>,
    user: String,
    password: Option<String>,
    ssl_mode: Option<String>,
    ca_file: Option<PathBuf>,
    pool_max: Option<u32>,
    effects_table: Option<String>,
    manage_schema: Option<bool>,
    allow_call: Option<bool>,
    max_rows: Option<u64>,
    max_bytes: Option<u64>,
    rate: Option<String>,
    burst: Option<u32>,
}

/// Whether `s` is a plain SQL identifier safe to interpolate (`[A-Za-z_][A-Za-z0-9_]{0,62}`).
pub fn is_identifier(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'_')
}

impl SqlConnConfig {
    /// Parses the config of connection `name` of kind `kind`.
    pub fn parse(name: &str, kind: &str, config: &[u8]) -> Result<Self, ConfigError> {
        let err = |reason: String| ConfigError {
            connection: name.to_owned(),
            reason,
        };
        let dialect = Dialect::from_kind(kind)
            .ok_or_else(|| err(format!("kind {kind:?} is not a SQL connection")))?;
        // Check `v` first so an unknown version reports as such, whatever else changed.
        let value: serde_json::Value =
            serde_json::from_slice(config).map_err(|e| err(format!("config is not JSON: {e}")))?;
        match value.get("v").and_then(serde_json::Value::as_u64) {
            Some(1) => {}
            Some(v) => return Err(err(format!("unknown config version v={v}"))),
            None => return Err(err("config has no \"v\"".into())),
        }
        let raw: Raw =
            serde_json::from_value(value).map_err(|e| err(format!("invalid config: {e}")))?;
        debug_assert_eq!(raw.v, 1);
        if raw.host.trim().is_empty() || raw.database.is_empty() || raw.user.is_empty() {
            return Err(err("host, database and user must be non-empty".into()));
        }
        if let Some(s) = &raw.schema {
            if dialect != Dialect::Postgres {
                return Err(err("schema is PostgreSQL only (use database)".into()));
            }
            if !is_identifier(s) {
                return Err(err(format!("schema {s:?} is not a plain identifier")));
            }
        }
        if let Some(p) = &raw.password {
            match template::parse(p) {
                Ok(ph) if !ph.is_empty() => {}
                Ok(_) => {
                    return Err(err(
                        "password must be a {{secret:NAME}} template, not a literal".into(),
                    ));
                }
                Err(e) => return Err(err(format!("password template: {e}"))),
            }
        }
        let ssl_mode = match &raw.ssl_mode {
            None => SslMode::default_for(dialect),
            Some(s) => SslMode::parse(dialect, s)
                .ok_or_else(|| err(format!("unknown ssl_mode {s:?} for {}", dialect.kind())))?,
        };
        let effects_table = raw
            .effects_table
            .unwrap_or_else(|| DEFAULT_EFFECTS_TABLE.to_owned());
        if !is_identifier(&effects_table) {
            return Err(err(format!(
                "effects_table {effects_table:?} is not a plain identifier"
            )));
        }
        let pool_max = raw.pool_max.unwrap_or(DEFAULT_POOL_MAX);
        if pool_max == 0 {
            return Err(err("pool_max must be at least 1".into()));
        }
        if raw.max_rows == Some(0) || raw.max_bytes == Some(0) {
            return Err(err("max_rows and max_bytes must be non-zero".into()));
        }
        let quota = match (&raw.rate, raw.burst) {
            (None, None) => None,
            (None, Some(_)) => return Err(err("burst without rate".into())),
            (Some(r), b) => {
                Some(QuotaSpec::parse_with_burst(r, b).map_err(|e| err(e.to_string()))?)
            }
        };
        Ok(Self {
            dialect,
            host: raw.host.trim().to_owned(),
            port: raw.port.unwrap_or(dialect.default_port()),
            database: raw.database,
            schema: raw.schema,
            user: raw.user,
            password: raw.password,
            ssl_mode,
            ca_file: raw.ca_file,
            pool_max,
            effects_table,
            manage_schema: raw.manage_schema.unwrap_or(true),
            allow_call: raw.allow_call.unwrap_or(false),
            max_rows: raw.max_rows,
            max_bytes: raw.max_bytes,
            quota,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(kind: &str, json: &str) -> Result<SqlConnConfig, ConfigError> {
        SqlConnConfig::parse("db", kind, json.as_bytes())
    }

    #[test]
    fn config_defaults() {
        let c = parse(
            KIND_POSTGRES,
            r#"{"v":1,"host":"h","database":"d","user":"u"}"#,
        )
        .unwrap();
        assert_eq!(c.dialect, Dialect::Postgres);
        assert_eq!(c.port, 5432);
        assert_eq!(c.ssl_mode, SslMode::Prefer);
        assert_eq!(c.pool_max, 10);
        assert_eq!(c.effects_table, "ostro_effects");
        assert!(c.manage_schema);
        assert!(!c.allow_call);
        assert_eq!((c.max_rows, c.max_bytes, c.quota), (None, None, None));
        assert_eq!((c.schema, c.password, c.ca_file), (None, None, None));

        let m = parse(
            KIND_MYSQL,
            r#"{"v":1,"host":"h","database":"d","user":"u"}"#,
        )
        .unwrap();
        assert_eq!(m.dialect, Dialect::Mysql);
        assert_eq!(m.port, 3306);
        assert_eq!(
            m.ssl_mode,
            SslMode::Require,
            "D-FX2: MySQL defaults to required"
        );
    }

    #[test]
    fn config_every_key() {
        let c = parse(
            KIND_POSTGRES,
            r#"{"v":1,"host":" db ","port":6543,"database":"app","schema":"s1","user":"u",
                "password":"{{secret:db-pass}}","ssl_mode":"verify-full","ca_file":"/ca.pem",
                "pool_max":3,"effects_table":"fx_tokens","manage_schema":false,"allow_call":true,
                "max_rows":5,"max_bytes":1024,"rate":"10/s","burst":4}"#,
        )
        .unwrap();
        assert_eq!(c.host, "db");
        assert_eq!(c.port, 6543);
        assert_eq!(c.schema.as_deref(), Some("s1"));
        assert_eq!(c.password.as_deref(), Some("{{secret:db-pass}}"));
        assert_eq!(c.ssl_mode, SslMode::VerifyFull);
        assert_eq!(c.ca_file, Some(PathBuf::from("/ca.pem")));
        assert_eq!(c.pool_max, 3);
        assert_eq!(c.effects_table, "fx_tokens");
        assert!(!c.manage_schema && c.allow_call);
        assert_eq!((c.max_rows, c.max_bytes), (Some(5), Some(1024)));
        assert_eq!(
            c.quota,
            Some(QuotaSpec {
                per_second: 10,
                burst: 4
            })
        );
    }

    #[test]
    fn config_ssl_modes_per_dialect() {
        for (kind, s, want) in [
            (KIND_POSTGRES, "disable", SslMode::Disable),
            (KIND_POSTGRES, "prefer", SslMode::Prefer),
            (KIND_POSTGRES, "require", SslMode::Require),
            (KIND_POSTGRES, "verify-ca", SslMode::VerifyCa),
            (KIND_POSTGRES, "verify-full", SslMode::VerifyFull),
            (KIND_MYSQL, "disabled", SslMode::Disable),
            (KIND_MYSQL, "preferred", SslMode::Prefer),
            (KIND_MYSQL, "required", SslMode::Require),
            (KIND_MYSQL, "verify_ca", SslMode::VerifyCa),
            (KIND_MYSQL, "verify_identity", SslMode::VerifyFull),
        ] {
            let j = format!(r#"{{"v":1,"host":"h","database":"d","user":"u","ssl_mode":"{s}"}}"#);
            assert_eq!(parse(kind, &j).unwrap().ssl_mode, want, "{kind} {s}");
        }
        // The other dialect's spelling is refused.
        let j = r#"{"v":1,"host":"h","database":"d","user":"u","ssl_mode":"required"}"#;
        assert!(parse(KIND_POSTGRES, j).is_err());
    }

    #[test]
    fn config_rejections_are_connection_unknown() {
        for (kind, bad) in [
            (
                KIND_POSTGRES,
                r#"{"v":2,"host":"h","database":"d","user":"u"}"#,
            ),
            (KIND_POSTGRES, r#"{"host":"h","database":"d","user":"u"}"#),
            (KIND_POSTGRES, r#"not json"#),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","typo":1}"#,
            ),
            (KIND_POSTGRES, r#"{"v":1,"host":"h","database":"d"}"#),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"","database":"d","user":"u"}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","password":"hunter22"}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","password":"{{secret:x"}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","effects_table":"x; DROP"}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","schema":"a\"b"}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","pool_max":0}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","max_rows":0}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","burst":3}"#,
            ),
            (
                KIND_POSTGRES,
                r#"{"v":1,"host":"h","database":"d","user":"u","rate":"fast"}"#,
            ),
            (
                KIND_MYSQL,
                r#"{"v":1,"host":"h","database":"d","user":"u","schema":"s"}"#,
            ),
            ("object", r#"{"v":1,"host":"h","database":"d","user":"u"}"#),
        ] {
            let e = parse(kind, bad).expect_err(bad);
            let Failure::NonRetryable(info) = e.to_failure() else {
                panic!("non-retryable expected")
            };
            assert_eq!(info.kind, CONNECTION_UNKNOWN, "{bad}");
            assert!(
                !info.message.contains("hunter22"),
                "no password in messages"
            );
        }
        let e = parse(
            KIND_POSTGRES,
            r#"{"v":7,"host":"h","database":"d","user":"u"}"#,
        )
        .unwrap_err();
        assert!(e.reason.contains("v=7"), "{e}");
    }

    #[test]
    fn identifiers() {
        for ok in ["ostro_effects", "_x", "T1", &"a".repeat(63)] {
            assert!(is_identifier(ok), "{ok}");
        }
        for bad in ["", "1a", "a-b", "a b", "a;b", "\"a\"", &"a".repeat(64), "é"] {
            assert!(!is_identifier(bad), "{bad}");
        }
    }
}
