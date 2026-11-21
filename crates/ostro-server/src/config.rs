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
pub const SECTIONS: &[&str] = &[
    "node",
    "api",
    "engine",
    "executor",
    "log",
    "runs",
    // Phase 8 (P8-D22, RESERVATIONS §11), parsed since 08-04.
    "telemetry",
    "snapshots",
    "backup",
    "blob_gc",
    "triggers",
    "guest_logs",
    // Phase 6 (RESERVATIONS §11), parsed since 06-14.
    "secrets",
    "egress",
    "connectors",
];

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
                // jiff's friendly parser rejects calendar units (`d`, `w`) for unsigned
                // durations (P8-D22): point at the hour form instead.
                let hint = if s.chars().any(|c| matches!(c, 'd' | 'w')) {
                    " (days/weeks are not accepted; write hours, e.g. \"24h\")"
                } else {
                    ""
                };
                format!(
                    "invalid duration {s:?} (expected e.g. \"30s\", \"250ms\", \"1m 30s\"){hint}: {e}"
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
    /// Single-line format (server default).
    #[default]
    Compact,
    /// One JSON object per line (`timestamp`, `level`, `target`, `fields`, span context);
    /// for log shippers (P8-D16, plan 08-04).
    Json,
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
    // Phase 8 sections (P8-D22, plan 08-04).
    pub telemetry: TelemetrySection,
    pub snapshots: SnapshotsSection,
    pub backup: BackupSection,
    pub blob_gc: BlobGcSection,
    pub triggers: TriggersSection,
    pub guest_logs: GuestLogsSection,
    // Phase 6 sections (RESERVATIONS §11, plan 06-14). Unknown keys inside them are
    // ignored (D18 node.toml rule).
    pub secrets: SecretsSection,
    pub egress: EgressSection,
    pub connectors: ConnectorsSection,
}

impl ServerConfig {
    /// `ostro dev` profile: loopback, `./.ostro`, pretty logs, 24 h retention.
    pub fn dev_defaults() -> Self {
        let mut c = Self::default();
        c.api.listen = SocketAddr::from(([127, 0, 0, 1], 7700));
        c.node.data_dir = PathBuf::from("./.ostro");
        c.log.format = LogFormat::Pretty;
        c.runs.retention = DEV_RETENTION;
        // P8-D22: dev never backs up by default (it is already the default; spelled out
        // so a future server-profile default cannot leak into `ostro dev`). Snapshots keep
        // their defaults.
        c.backup = BackupSection::default();
        // 06-DECISIONS: dev auto-generates `<data_dir>/sealing.key` when no key is
        // configured; the server profile refuses to store secrets instead.
        c.secrets.auto_sealing_key = true;
        c
    }

    /// `ostro server` profile: all interfaces, `./ostro-data`, compact logs, 168 h retention.
    pub fn server_defaults() -> Self {
        Self::default()
    }

    /// Render as TOML (for `--print-config`); durations as friendly strings. Secrets
    /// (backup credentials, OTLP header values) are printed as `<redacted>`.
    pub fn to_toml_string(&self) -> String {
        toml_render(self)
    }

    /// The node log format: `[telemetry] log_format` when set, else `[log] format`.
    pub fn effective_log_format(&self) -> LogFormat {
        self.telemetry.log_format.unwrap_or(self.log.format)
    }

    /// `[log]` with [`Self::effective_log_format`] applied (what the node logger uses).
    pub fn effective_log_section(&self) -> LogSection {
        LogSection {
            level: self.log.level.clone(),
            format: self.effective_log_format(),
        }
    }

    /// Resolve `env:NAME` secret references against `env` and validate the Phase 8
    /// sections. Called by [`load_layered`].
    fn resolve_and_validate(&mut self, env: &EnvSource) -> Result<(), ConfigError> {
        for (key, slot) in [
            ("backup.access_key_id", &mut self.backup.access_key_id),
            (
                "backup.secret_access_key",
                &mut self.backup.secret_access_key,
            ),
            ("backup.session_token", &mut self.backup.session_token),
        ] {
            if let Some(secret) = slot {
                secret.resolve(key, env)?;
            }
        }
        for (name, value) in self.telemetry.otlp_headers.iter_mut() {
            value.resolve(&format!("telemetry.otlp_headers.{name}"), env)?;
        }
        let ratio = self.telemetry.sample_ratio;
        if !(0.0..=1.0).contains(&ratio) {
            return Err(ConfigError::Invalid(format!(
                "telemetry.sample_ratio must be within 0.0..=1.0 (got {ratio})"
            )));
        }
        if self.telemetry.service_name.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "telemetry.service_name must not be empty".into(),
            ));
        }
        if let Some(ep) = &self.telemetry.otlp_endpoint
            && !(ep.starts_with("http://") || ep.starts_with("https://"))
        {
            return Err(ConfigError::Invalid(format!(
                "telemetry.otlp_endpoint must be an http:// or https:// URL (got {ep:?})"
            )));
        }
        if self.backup.tls_roots.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "backup.tls_roots must be \"platform\", \"webpki\" or a PEM file path".into(),
            ));
        }
        if self.backup.seal_interval.get().is_zero() {
            return Err(ConfigError::Invalid(
                "backup.seal_interval must be non-zero".into(),
            ));
        }
        if self.snapshots.keep == 0 {
            return Err(ConfigError::Invalid(
                "snapshots.keep must be at least 1".into(),
            ));
        }
        // Phase 6 sections: parse into the bridge types once so every bad value fails at
        // load, naming its section.
        self.secrets.to_bridge()?;
        self.egress.to_bridge()?;
        self.connectors.to_bridge()?;
        Ok(())
    }

    /// The base URL of local presigned object URLs: `[connectors] public_base_url`, else
    /// `http://<API listen address>` (`bound` is the address actually bound; an unspecified
    /// IP such as `0.0.0.0` becomes loopback).
    pub fn public_base_url(&self, bound: SocketAddr) -> String {
        if let Some(u) = &self.connectors.public_base_url {
            return u.trim_end_matches('/').to_owned();
        }
        let mut addr = bound;
        if addr.ip().is_unspecified() {
            addr.set_ip(match addr {
                SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
                SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
            });
        }
        format!("http://{addr}")
    }
}

// ---------------------------------------------------------------------------------
// Phase 8 sections (P8-D22, RESERVATIONS §11; parsed since plan 08-04)
// ---------------------------------------------------------------------------------

/// A secret config value: an inline value or an `env:NAME` reference resolved at load.
/// `Debug` and serialisation (`--print-config`) print `<redacted>`.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct SecretString(String);

impl SecretString {
    /// The prefix of an environment reference (`env:AWS_SECRET_ACCESS_KEY`).
    pub const ENV_PREFIX: &'static str = "env:";
    /// What `Debug` / `--print-config` show instead of the value.
    pub const REDACTED: &'static str = "<redacted>";

    pub fn new(v: impl Into<String>) -> Self {
        Self(v.into())
    }

    /// The (resolved) secret value.
    pub fn expose(&self) -> &str {
        &self.0
    }

    fn resolve(&mut self, key: &str, env: &EnvSource) -> Result<(), ConfigError> {
        if let Some(name) = self.0.strip_prefix(Self::ENV_PREFIX) {
            let name = name.trim();
            match env.lookup(name) {
                Some(v) => self.0 = v,
                None => {
                    return Err(ConfigError::Invalid(format!(
                        "{key}: environment variable {name} (referenced as env:{name}) is not set"
                    )));
                }
            }
        }
        Ok(())
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::REDACTED)
    }
}

impl Serialize for SecretString {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(Self::REDACTED)
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d).map(Self)
    }
}

/// OTLP transport. Only gRPC (over the locked tonic) in Phase 8 (P8-D13).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtlpProtocol {
    #[default]
    Grpc,
}

/// `[telemetry]`: node log format, OTLP traces/logs export and Prometheus `/metrics`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetrySection {
    /// Node log format override (`pretty` / `compact` / `json`); unset = `[log] format`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_format: Option<LogFormat>,
    /// OTLP collector endpoint (`http://host:4317`); unset = no OTLP export.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
    /// OTLP transport (`grpc` only).
    pub otlp_protocol: OtlpProtocol,
    /// Extra gRPC metadata sent with every export (values may be `env:NAME`).
    pub otlp_headers: std::collections::BTreeMap<String, SecretString>,
    /// Export traces (when `otlp_endpoint` is set).
    pub traces: bool,
    /// Export node logs as OTLP LogRecords (when `otlp_endpoint` is set).
    pub logs: bool,
    /// Root-span sampling ratio, `ParentBased(TraceIdRatioBased(ratio))` (default 1.0).
    pub sample_ratio: f64,
    /// `service.name` resource attribute (default `ostro`).
    pub service_name: String,
    /// Serve Prometheus `GET /metrics` (default true).
    pub metrics: bool,
}

impl Default for TelemetrySection {
    fn default() -> Self {
        Self {
            log_format: None,
            otlp_endpoint: None,
            otlp_protocol: OtlpProtocol::Grpc,
            otlp_headers: std::collections::BTreeMap::new(),
            traces: true,
            logs: true,
            sample_ratio: 1.0,
            service_name: "ostro".to_owned(),
            metrics: true,
        }
    }
}

/// `[snapshots]` → [`ostro_partition::leader::snapshot::SnapshotConfig`] (P8-D1/D2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SnapshotsSection {
    pub enabled: bool,
    /// Snapshot at least this often while the partition changes (default `1h`).
    pub interval: FriendlyDuration,
    /// Never snapshot more often than this (default `60s`).
    pub min_interval: FriendlyDuration,
    /// Snapshot early after this many appended bytes (default 256 MiB). The TOML key is
    /// `bytes` (P8-D22).
    #[serde(rename = "bytes")]
    pub bytes_threshold: u64,
    /// Local snapshots kept per partition (default 2).
    pub keep: usize,
}

impl Default for SnapshotsSection {
    fn default() -> Self {
        let d = ostro_partition::leader::snapshot::SnapshotConfig::default();
        Self {
            enabled: d.enabled,
            interval: d.interval.into(),
            min_interval: d.min_interval.into(),
            bytes_threshold: d.bytes_threshold,
            keep: d.keep,
        }
    }
}

impl SnapshotsSection {
    pub fn to_snapshot_config(&self) -> ostro_partition::leader::snapshot::SnapshotConfig {
        ostro_partition::leader::snapshot::SnapshotConfig {
            enabled: self.enabled,
            interval: self.interval.get(),
            min_interval: self.min_interval.get(),
            bytes_threshold: self.bytes_threshold,
            keep: self.keep,
        }
    }
}

/// S3 addressing style in `[backup] url_style`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UrlStyleSetting {
    #[default]
    Path,
    Virtual,
}

/// `[backup]` → [`ostro_backup::config::BackupConfig`] (bottomless, P8-D3..D5). Off unless
/// `url` is set. Credentials may be `env:NAME` references and are never printed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackupSection {
    /// `s3://bucket/prefix` or `file:///path`; unset = backup off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// RPO target (default `1s`): bound on the data-loss window; the uploader cuts every
    /// half of it (P8-D23).
    pub seal_interval: FriendlyDuration,
    /// Partitions uploaded concurrently per round; 0 = auto (`min(P, 8)`).
    pub upload_concurrency: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    pub url_style: UrlStyleSetting,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<SecretString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<SecretString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_token: Option<SecretString>,
    pub allow_http: bool,
    /// `platform` (default), `webpki`, or a PEM bundle path.
    pub tls_roots: String,
    /// Remote snapshots kept per partition (default 3).
    pub remote_snapshots_keep: usize,
    /// Take over a prefix locked by another incarnation (`--takeover`).
    pub takeover: bool,
}

impl Default for BackupSection {
    fn default() -> Self {
        let d = ostro_backup::config::BackupConfig::default();
        Self {
            url: None,
            seal_interval: d.seal_interval.into(),
            upload_concurrency: d.upload_concurrency,
            region: None,
            endpoint: None,
            url_style: UrlStyleSetting::Path,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            allow_http: d.allow_http,
            tls_roots: "platform".to_owned(),
            remote_snapshots_keep: d.remote_snapshots_keep,
            takeover: d.takeover,
        }
    }
}

impl BackupSection {
    /// Whether bottomless backup is configured.
    pub fn enabled(&self) -> bool {
        self.url.is_some()
    }

    pub fn to_backup_config(&self) -> ostro_backup::config::BackupConfig {
        use ostro_backup::config::{BackupConfig, TlsRoots, UrlStyle};
        let secret = |s: &Option<SecretString>| s.as_ref().map(|v| v.expose().to_owned());
        BackupConfig {
            url: self.url.clone(),
            seal_interval: self.seal_interval.get(),
            upload_concurrency: self.upload_concurrency,
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            url_style: match self.url_style {
                UrlStyleSetting::Path => UrlStyle::Path,
                UrlStyleSetting::Virtual => UrlStyle::Virtual,
            },
            access_key_id: secret(&self.access_key_id),
            secret_access_key: secret(&self.secret_access_key),
            session_token: secret(&self.session_token),
            allow_http: self.allow_http,
            tls_roots: match self.tls_roots.trim() {
                "platform" => TlsRoots::Platform,
                "webpki" => TlsRoots::Webpki,
                path => TlsRoots::File(PathBuf::from(path)),
            },
            remote_snapshots_keep: self.remote_snapshots_keep,
            takeover: self.takeover,
        }
    }
}

/// `[blob_gc]`: local blob garbage collection (P8-D7, implemented by 08-19).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlobGcSection {
    pub enabled: bool,
    /// Mark-and-sweep period (default `1h`).
    pub interval: FriendlyDuration,
    /// Unreferenced blobs younger than this are kept (default `1h`).
    pub grace: FriendlyDuration,
}

impl Default for BlobGcSection {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: FriendlyDuration::from_secs(3600),
            grace: FriendlyDuration::from_secs(3600),
        }
    }
}

/// `[triggers]`: webhook limits and cron catch-up (P8-D10..D12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TriggersSection {
    /// Maximum webhook body in bytes (default 1 MiB).
    pub webhook_body_limit: u64,
    /// Signature timestamp tolerance (default `300s`).
    pub webhook_tolerance: FriendlyDuration,
    /// Most missed cron fires replayed by catch-up `All` (default 100).
    pub cron_catchup_cap: u32,
}

impl Default for TriggersSection {
    fn default() -> Self {
        Self {
            webhook_body_limit: MIB,
            webhook_tolerance: FriendlyDuration::from_secs(300),
            cron_catchup_cap: ostro_partition::leader::cron::CronConfig::default().catchup_cap,
        }
    }
}

impl TriggersSection {
    pub fn to_cron_config(&self) -> ostro_partition::leader::cron::CronConfig {
        ostro_partition::leader::cron::CronConfig {
            catchup_cap: self.cron_catchup_cap,
        }
    }
}

/// `[guest_logs]`: guest log rate limits, lifetime caps and the ring buffer (P8-D16,
/// implemented by 08-17).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GuestLogsSection {
    /// Per-run token bucket refill (default 50 lines/s).
    pub rate_per_sec: u32,
    /// Per-run token bucket burst (default 500 lines).
    pub burst: u32,
    /// Per-run lifetime line cap (default 10,000; in memory, best-effort across restarts).
    pub run_lines_cap: u64,
    /// Per-run lifetime byte cap (default 10 MiB).
    pub run_bytes_cap: u64,
    /// Lines kept per run in the ring buffer (default 1,000).
    pub ring_lines: u32,
    /// Node-wide byte budget of all ring buffers, LRU over runs (default 64 MiB).
    pub ring_budget_bytes: u64,
    /// Node-wide token bucket (default 5,000 lines/s).
    pub node_rate_per_sec: u32,
}

impl Default for GuestLogsSection {
    fn default() -> Self {
        Self {
            rate_per_sec: 50,
            burst: 500,
            run_lines_cap: 10_000,
            run_bytes_cap: 10 * MIB,
            ring_lines: 1_000,
            ring_budget_bytes: 64 * MIB,
            node_rate_per_sec: 5_000,
        }
    }
}

// ---------------------------------------------------------------------------------
// Phase 6 sections (RESERVATIONS §11; parsed since plan 06-14)
//
// The bridge owns the real types (`ostro_bridge::config::{SecretsConfig, EgressConfig,
// ConnectorsConfig}`), which are `Deserialize`-only and reject unknown keys. The layered
// loader needs `Serialize` (the profile is the base layer) and node.toml must ignore unknown
// keys (D18), so these mirror sections hold the same keys with the same defaults and are
// converted with `to_bridge()` (serde, so the bridge's own parsers and validation apply).
// ---------------------------------------------------------------------------------

/// A byte size: an integer or a string such as `"4MiB"` (the bridge's `parse_bytes`).
/// Serialised as an integer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// The size in bytes.
    pub fn get(self) -> u64 {
        self.0
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = ByteSize;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte count or a size string such as \"4MiB\"")
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<ByteSize, E> {
                Ok(ByteSize(v))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<ByteSize, E> {
                u64::try_from(v)
                    .map(ByteSize)
                    .map_err(|_| E::custom(format!("invalid byte size {v}: negative")))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<ByteSize, E> {
                ostro_bridge::config::parse_bytes(v)
                    .map(ByteSize)
                    .map_err(|e| E::custom(format!("invalid byte size {v:?}: {e}")))
            }
        }
        d.deserialize_any(V)
    }
}

/// `[secrets]` (06-DECISIONS): provider order, the sealing key and the remote providers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SecretsSection {
    /// Provider lookup order (`sealed`, `env`, `file`, `vault`, `k8s`).
    pub providers: Vec<String>,
    /// File holding the sealing key (32 raw bytes or base64). `OSTRO_SEALING_KEY` wins.
    pub sealing_key_file: Option<PathBuf>,
    /// Env provider prefix (default `OSTRO_SECRET_`).
    pub env_prefix: String,
    /// File provider directory (the provider is skipped while unset).
    pub file_dir: Option<PathBuf>,
    /// Generate `<data_dir>/sealing.key` (0600) when no key is configured: on in the dev
    /// profile, off in the server profile (which then refuses to store secrets but runs).
    pub auto_sealing_key: bool,
    /// `[secrets.vault]`; the provider exists only when this table is present.
    pub vault: Option<VaultSection>,
    /// `[secrets.k8s]`; the provider exists only when this table is present.
    pub k8s: Option<K8sSection>,
}

impl Default for SecretsSection {
    fn default() -> Self {
        Self {
            providers: vec!["sealed".into(), "env".into(), "file".into()],
            sealing_key_file: None,
            env_prefix: "OSTRO_SECRET_".into(),
            file_dir: None,
            auto_sealing_key: false,
            vault: None,
            k8s: None,
        }
    }
}

/// `[secrets.vault]` (unset keys take the bridge defaults).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct VaultSection {
    pub addr: Option<String>,
    pub mount: Option<String>,
    /// `token` | `approle` | `kubernetes`.
    pub auth: Option<String>,
    pub token_file: Option<PathBuf>,
    pub role_id: Option<String>,
    pub secret_id_file: Option<PathBuf>,
    pub k8s_role: Option<String>,
    pub cache_ttl: Option<FriendlyDuration>,
}

/// `[secrets.k8s]` (unset keys take the bridge defaults).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct K8sSection {
    pub namespace: Option<String>,
    pub kubeconfig: Option<PathBuf>,
    pub cache_ttl: Option<FriendlyDuration>,
}

/// `[egress]` (06-DECISIONS): the SSRF gate and outbound HTTP limits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EgressSection {
    /// Private CIDRs effects may reach (default none: deny-by-default, Pitfall 8).
    pub allow_private: Vec<String>,
    pub connect_timeout: FriendlyDuration,
    pub default_timeout: FriendlyDuration,
    pub max_response_bytes: ByteSize,
    pub hard_max_response_bytes: ByteSize,
    pub max_redirects: u8,
    /// Longest wait for a rate-limit permit (passed to `Bridge::new`).
    pub rate_max_wait: FriendlyDuration,
    pub idempotency_window: FriendlyDuration,
    pub safety_margin: FriendlyDuration,
}

impl Default for EgressSection {
    fn default() -> Self {
        Self {
            allow_private: Vec::new(),
            connect_timeout: FriendlyDuration::from_secs(5),
            default_timeout: FriendlyDuration::from_secs(30),
            max_response_bytes: ByteSize(4 * MIB),
            hard_max_response_bytes: ByteSize(64 * MIB),
            max_redirects: 5,
            rate_max_wait: FriendlyDuration::from_secs(30),
            idempotency_window: FriendlyDuration::from_secs(24 * 3600),
            safety_margin: FriendlyDuration::from_secs(5 * 60),
        }
    }
}

/// `[connectors]` (06-DECISIONS): SQL/object caps, presign and the default rate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectorsSection {
    pub sql_max_rows: u64,
    pub sql_max_bytes: ByteSize,
    pub sql_effects_retention: FriendlyDuration,
    pub object_max_get_bytes: ByteSize,
    pub presign_default_expiry: FriendlyDuration,
    pub backend_ttl: FriendlyDuration,
    /// Default per-target rate (`"100/s"`); unset = unlimited.
    pub default_rate: Option<String>,
    /// Base of local presigned URLs; unset = the API listen address
    /// ([`ServerConfig::public_base_url`]).
    pub public_base_url: Option<String>,
}

impl Default for ConnectorsSection {
    fn default() -> Self {
        Self {
            sql_max_rows: 10_000,
            sql_max_bytes: ByteSize(4 * MIB),
            sql_effects_retention: FriendlyDuration::from_secs(168 * 3600),
            object_max_get_bytes: ByteSize(16 * MIB),
            presign_default_expiry: FriendlyDuration::from_secs(15 * 60),
            backend_ttl: FriendlyDuration::from_secs(5 * 60),
            default_rate: None,
            public_base_url: None,
        }
    }
}

/// `value` without `null` members (unset keys take the bridge defaults).
fn strip_nulls(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(m) => serde_json::Value::Object(
            m.into_iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k, strip_nulls(v)))
                .collect(),
        ),
        other => other,
    }
}

/// Serialise a mirror section and parse it as the bridge type (minus `drop` keys).
fn to_bridge_type<T: serde::de::DeserializeOwned>(
    section: &str,
    value: &impl Serialize,
    drop: &[&str],
) -> Result<T, ConfigError> {
    let mut v = strip_nulls(
        serde_json::to_value(value)
            .map_err(|e| ConfigError::Invalid(format!("[{section}]: {e}")))?,
    );
    if let serde_json::Value::Object(m) = &mut v {
        for k in drop {
            m.remove(*k);
        }
    }
    serde_json::from_value(v).map_err(|e| ConfigError::Invalid(format!("[{section}]: {e}")))
}

impl SecretsSection {
    /// The bridge's `[secrets]` type, validated.
    pub fn to_bridge(&self) -> Result<ostro_bridge::config::SecretsConfig, ConfigError> {
        let c: ostro_bridge::config::SecretsConfig =
            to_bridge_type("secrets", self, &["auto_sealing_key"])?;
        c.validate()
            .map_err(|e| ConfigError::Invalid(format!("[secrets]: {e}")))?;
        Ok(c)
    }
}

impl EgressSection {
    /// The bridge's `[egress]` type, validated.
    pub fn to_bridge(&self) -> Result<ostro_bridge::config::EgressConfig, ConfigError> {
        let c: ostro_bridge::config::EgressConfig = to_bridge_type("egress", self, &[])?;
        c.validate()
            .map_err(|e| ConfigError::Invalid(format!("[egress]: {e}")))?;
        Ok(c)
    }
}

impl ConnectorsSection {
    /// The bridge's `[connectors]` type, validated (`public_base_url` stays as configured;
    /// see [`ServerConfig::public_base_url`] for the effective value).
    pub fn to_bridge(&self) -> Result<ostro_bridge::config::ConnectorsConfig, ConfigError> {
        let c: ostro_bridge::config::ConnectorsConfig = to_bridge_type("connectors", self, &[])?;
        c.validate()
            .map_err(|e| ConfigError::Invalid(format!("[connectors]: {e}")))?;
        Ok(c)
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
    /// One variable (for `env:NAME` secret references).
    fn lookup(&self, name: &str) -> Option<String> {
        match self {
            Self::Process => std::env::var(name).ok(),
            Self::Map(m) => m.get(name).cloned(),
        }
    }

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
    cfg.resolve_and_validate(env)?;
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
                // `[telemetry] otlp_headers`: an inline table of strings.
                // `[secrets] providers`, `[egress] allow_private`: arrays of strings.
                serde_json::Value::Array(_) => val.to_string(),
                // `[telemetry] otlp_headers` and `[secrets.vault]` / `[secrets.k8s]`: an
                // inline table of scalars (unset keys omitted).
                serde_json::Value::Object(map) => {
                    let items: Vec<String> = map
                        .iter()
                        .filter(|(_, v)| !v.is_null())
                        .map(|(k, v)| format!("{} = {v}", serde_json::Value::String(k.clone())))
                        .collect();
                    format!("{{ {} }}", items.join(", "))
                }
            };
            let _ = writeln!(out, "{k} = {rendered}");
        }
    }
    out
}
