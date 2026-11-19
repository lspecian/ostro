//! Layered server configuration (CLI-02).
//!
//! Precedence, lowest to highest:
//! 1. a base profile ([`ServerConfig::dev_defaults`] / [`ServerConfig::server_defaults`]),
//! 2. an optional TOML file,
//! 3. `OSTRO_<SECTION>__<KEY>` environment variables (`__` separates nesting levels),
//! 4. explicit [`Overrides`] (CLI flags).
//!
//! Unknown keys are rejected everywhere they can only be a typo: in the TOML file, and
//! in environment variables that name a known section. Environment variables with the
//! `OSTRO_` prefix that do not name a section (e.g. `OSTRO_DET_ITERS`) are not config
//! and are ignored, because the process environment is a shared namespace.
//!
//! Durations are friendly strings (`"30s"`, `"250ms"`, `"1m 30s"`) parsed and printed
//! with `jiff::fmt::friendly`.

use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use config::{Config, Environment, File, FileFormat};
use jiff::fmt::friendly::{SpanParser, SpanPrinter};
use ostro_engine::config::EngineConfig;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const MIB: u64 = 1024 * 1024;

/// Environment variable prefix (followed by `_`).
pub const ENV_PREFIX: &str = "OSTRO";
/// Nesting separator inside environment variable names.
pub const ENV_SEPARATOR: &str = "__";
/// Top-level sections; only `OSTRO_<SECTION>__*` variables are read.
pub const SECTIONS: &[&str] = &["node", "api", "engine", "executor", "log", "runs"];

static PARSER: SpanParser = SpanParser::new();
static PRINTER: SpanPrinter = SpanPrinter::new();

/// A `std::time::Duration` that (de)serialises as a friendly string (`"30s"`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FriendlyDuration(pub Duration);

impl FriendlyDuration {
    /// The wrapped duration.
    pub fn get(self) -> Duration {
        self.0
    }

    /// `from_secs` convenience for defaults.
    pub const fn from_secs(s: u64) -> Self {
        Self(Duration::from_secs(s))
    }

    /// `from_millis` convenience for defaults.
    pub const fn from_millis(ms: u64) -> Self {
        Self(Duration::from_millis(ms))
    }
}

impl From<Duration> for FriendlyDuration {
    fn from(d: Duration) -> Self {
        Self(d)
    }
}

impl From<FriendlyDuration> for Duration {
    fn from(d: FriendlyDuration) -> Self {
        d.0
    }
}

impl fmt::Display for FriendlyDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&PRINTER.unsigned_duration_to_string(&self.0))
    }
}

impl FromStr for FriendlyDuration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        PARSER
            .parse_unsigned_duration(s.trim())
            .map(Self)
            .map_err(|e| {
                format!(
                    "invalid duration {s:?} (expected e.g. \"30s\", \"250ms\", \"1m 30s\"): {e}"
                )
            })
    }
}

impl Serialize for FriendlyDuration {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for FriendlyDuration {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// `[node]`: identity and on-disk layout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeSection {
    /// Data directory (WAL, state, blobs, meta).
    pub data_dir: PathBuf,
    /// Fixed partition count (P=16 single node).
    pub partitions: u16,
    /// Node id (1 for a single node).
    pub node_id: u64,
    /// Run from a throwaway temp directory (`--ephemeral`, wired in 04-19).
    pub ephemeral: bool,
}

impl Default for NodeSection {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./ostro-data"),
            partitions: 16,
            node_id: 1,
            ephemeral: false,
        }
    }
}

/// `[api]`: the one-port HTTP + gRPC listener.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiSection {
    /// Listen address for HTTP/1.1, h2c and gRPC.
    pub listen: SocketAddr,
    /// Maximum deployable module size in bytes (default 64 MiB).
    pub max_module_bytes: u64,
    /// Maximum request body for non-deploy requests in bytes (default 4 MiB).
    pub max_request_bytes: u64,
}

impl Default for ApiSection {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 7700)),
            max_module_bytes: 64 * MIB,
            max_request_bytes: 4 * MIB,
        }
    }
}

/// `[engine]`: the subset of [`EngineConfig`] exposed to operators.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineSection {
    /// `engine.hot_wait` (default 2 s).
    pub hot_wait: FriendlyDuration,
    /// `engine.activation_timeout` (default 60 s).
    pub activation_timeout: FriendlyDuration,
    /// `engine.activation_fuel` (default 10,000,000,000).
    pub activation_fuel: u64,
    /// `engine.compile_permits` (default 2).
    pub compile_permits: usize,
    /// `engine.node_memory_budget` in bytes (default 1 GiB).
    pub node_memory_budget: u64,
    /// `engine.continue_as_new_events` (default 10,000).
    pub continue_as_new_events: u64,
    /// `engine.pool.total_instances` (default 1000).
    pub pool_total_instances: u32,
}

impl Default for EngineSection {
    fn default() -> Self {
        let e = EngineConfig::default();
        Self {
            hot_wait: e.hot_wait.into(),
            activation_timeout: e.activation_timeout.into(),
            activation_fuel: e.activation_fuel,
            compile_permits: e.compile_permits,
            node_memory_budget: e.node_memory_budget,
            continue_as_new_events: e.continue_as_new_events,
            pool_total_instances: e.pool.total_instances,
        }
    }
}

impl EngineSection {
    /// Map onto an [`EngineConfig`], starting from its defaults.
    pub fn to_engine_config(&self) -> EngineConfig {
        let d = EngineConfig::default();
        let mut pool = d.pool.clone();
        pool.total_instances = self.pool_total_instances;
        EngineConfig {
            hot_wait: self.hot_wait.get(),
            activation_timeout: self.activation_timeout.get(),
            activation_fuel: self.activation_fuel,
            compile_permits: self.compile_permits,
            node_memory_budget: self.node_memory_budget,
            continue_as_new_events: self.continue_as_new_events,
            pool,
            ..d
        }
    }
}

/// `[executor]`: step executor slots and leases.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutorSection {
    /// Concurrent activation slots (default 64).
    pub slots: u32,
    /// Attempt lease TTL (default 30 s).
    pub lease_ttl: FriendlyDuration,
    /// Lease heartbeat period; `None` = `lease_ttl / 3`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<FriendlyDuration>,
    /// Consecutive failed attempts before a run is failed (default 3).
    pub max_attempt_failures: u32,
    /// Backoff between failed attempts (default 1 s).
    pub retry_backoff: FriendlyDuration,
}

impl Default for ExecutorSection {
    fn default() -> Self {
        Self {
            slots: 64,
            lease_ttl: FriendlyDuration::from_secs(30),
            heartbeat: None,
            max_attempt_failures: 3,
            retry_backoff: FriendlyDuration::from_secs(1),
        }
    }
}

impl ExecutorSection {
    /// Heartbeat period: explicit value or `lease_ttl / 3`.
    pub fn effective_heartbeat(&self) -> Duration {
        self.heartbeat
            .map(FriendlyDuration::get)
            .unwrap_or(self.lease_ttl.get() / 3)
    }
}

/// Default `[runs] retention` of the `ostro server` profile (D9: 7 days, written in hours
/// because jiff's friendly parser rejects `7d` for unsigned durations).
pub const SERVER_RETENTION: FriendlyDuration = FriendlyDuration::from_secs(168 * 3600);
/// Default `[runs] retention` of the `ostro dev` profile (D9: 24 h).
pub const DEV_RETENTION: FriendlyDuration = FriendlyDuration::from_secs(24 * 3600);

/// `[runs]`: defaults of the per-run policies (D9). A start request, then the
/// `WorkflowPolicy`, then the `QueuePolicy` meta records override them
/// ([`crate::retention`]).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunsSection {
    /// How long a terminal run is kept before expiry deletes it (default `168h` for the
    /// server profile, `24h` for dev). Must be non-zero.
    pub retention: FriendlyDuration,
    /// Optional cap on every resolved retention (request and policies included).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retention: Option<FriendlyDuration>,
}

impl Default for RunsSection {
    fn default() -> Self {
        Self {
            retention: SERVER_RETENTION,
            max_retention: None,
        }
    }
}

/// Log output format.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Multi-line human format (dev default).
    Pretty,
    /// Single-line format (server default; JSON is not enabled to keep the
    /// tracing-subscriber feature set small).
    #[default]
    Compact,
}

/// `[log]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LogSection {
    /// `EnvFilter` directive used when `RUST_LOG` is unset (default "info").
    pub level: String,
    /// Output format.
    pub format: LogFormat,
}

impl Default for LogSection {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            format: LogFormat::Compact,
        }
    }
}

/// The effective server configuration. `Default` = [`ServerConfig::server_defaults`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub node: NodeSection,
    pub api: ApiSection,
    pub engine: EngineSection,
    pub executor: ExecutorSection,
    pub log: LogSection,
    pub runs: RunsSection,
}

impl ServerConfig {
    /// `ostro dev` profile: loopback, `./.ostro`, pretty logs, 24 h retention.
    pub fn dev_defaults() -> Self {
        let mut c = Self::default();
        c.api.listen = SocketAddr::from(([127, 0, 0, 1], 7700));
        c.node.data_dir = PathBuf::from("./.ostro");
        c.log.format = LogFormat::Pretty;
        c.runs.retention = DEV_RETENTION;
        c
    }

    /// `ostro server` profile: all interfaces, `./ostro-data`, compact logs, 168 h retention.
    pub fn server_defaults() -> Self {
        Self::default()
    }

    /// Render as TOML (for `--print-config`); durations as friendly strings.
    pub fn to_toml_string(&self) -> String {
        toml_render(self)
    }
}

/// Explicit overrides from CLI flags; they win over every other layer.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Overrides {
    pub listen: Option<SocketAddr>,
    pub data_dir: Option<PathBuf>,
}

/// Where `OSTRO_*` variables come from.
#[derive(Clone, Debug, Default)]
pub enum EnvSource {
    /// The process environment.
    #[default]
    Process,
    /// An explicit map (tests; never touches the process environment).
    Map(HashMap<String, String>),
}

impl EnvSource {
    /// The `OSTRO_<SECTION>__*` subset, keyed by the original variable name.
    fn sectioned(&self) -> config::Map<String, String> {
        let all: Vec<(String, String)> = match self {
            Self::Process => std::env::vars().collect(),
            Self::Map(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        };
        let mut out = config::Map::new();
        for (k, v) in all {
            let Some(rest) = k.strip_prefix(ENV_PREFIX).and_then(|r| r.strip_prefix('_')) else {
                continue;
            };
            let Some((section, _)) = rest.split_once(ENV_SEPARATOR) else {
                continue;
            };
            if SECTIONS.contains(&section.to_ascii_lowercase().as_str()) {
                out.insert(k, v);
            }
        }
        out
    }
}

/// Configuration loading failure.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// An explicitly requested config file does not exist.
    #[error("config file not found: {}", .0.display())]
    FileNotFound(PathBuf),
    /// Parsing / merging / validation failure (message names the key or path).
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

impl From<config::ConfigError> for ConfigError {
    fn from(e: config::ConfigError) -> Self {
        Self::Invalid(e.to_string())
    }
}

/// Load `base` < `file` (TOML) < `env` < `overrides`.
///
/// `explicit_file`: the file was named by the user (`--config`), so it must exist;
/// otherwise a missing file is skipped (default search location).
pub fn load_layered(
    base: ServerConfig,
    file: Option<&Path>,
    explicit_file: bool,
    env: &EnvSource,
    overrides: &Overrides,
) -> Result<ServerConfig, ConfigError> {
    let mut b = Config::builder().add_source(Config::try_from(&base)?);
    if let Some(path) = file {
        if explicit_file && !path.is_file() {
            return Err(ConfigError::FileNotFound(path.to_path_buf()));
        }
        b = b.add_source(
            File::from(path)
                .format(FileFormat::Toml)
                .required(explicit_file),
        );
    }
    b = b.add_source(
        Environment::with_prefix(ENV_PREFIX)
            .prefix_separator("_")
            .separator(ENV_SEPARATOR)
            .try_parsing(true)
            .source(Some(env.sectioned())),
    );
    let mut cfg: ServerConfig = b.build()?.try_deserialize()?;
    if cfg.runs.retention.get().is_zero() {
        return Err(ConfigError::Invalid(
            "runs.retention must be non-zero (terminal runs always expire, D9)".into(),
        ));
    }
    if let Some(listen) = overrides.listen {
        cfg.api.listen = listen;
    }
    if let Some(dir) = &overrides.data_dir {
        cfg.node.data_dir = dir.clone();
    }
    Ok(cfg)
}

/// Minimal TOML renderer for the config shape (tables of scalars), via serde_json so
/// no TOML serializer dependency is needed.
fn toml_render(cfg: &ServerConfig) -> String {
    use std::fmt::Write as _;

    let v = serde_json::to_value(cfg).expect("ServerConfig serialises");
    let mut out = String::new();
    let serde_json::Value::Object(sections) = v else {
        unreachable!("ServerConfig is a struct")
    };
    for name in SECTIONS {
        let Some(serde_json::Value::Object(fields)) = sections.get(*name) else {
            continue;
        };
        if !out.is_empty() {
            out.push('\n');
        }
        let _ = writeln!(out, "[{name}]");
        for (k, val) in fields {
            let rendered = match val {
                serde_json::Value::Null => continue,
                // JSON string escapes are valid TOML basic-string escapes for the
                // characters serde_json emits.
                serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_) => val.to_string(),
                other => unreachable!("non-scalar config value {other}"),
            };
            let _ = writeln!(out, "{k} = {rendered}");
        }
    }
    out
}
