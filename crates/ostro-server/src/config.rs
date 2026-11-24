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
    // Phase 7 (RESERVATIONS §11), parsed since 07-02. `[api.tls]` lives inside `[api]`.
    "workers",
    "cluster",
    "flow",
    // Phase 11 (D-P11-25, RESERVATIONS §11), parsed since 11-04.
    "limits",
    "ui",
    "bulk",
    "dlq",
    // Phase 12 (D-P12-10/12, RESERVATIONS §11), parsed since 12-02. The peer keys live in
    // `[cluster]`.
    "replication",
    // Phase 10 (D-P10-6/19, RESERVATIONS §11), parsed since 10-10.
    "dev",
    // Phase 13 (D-P13-23, RESERVATIONS §11), parsed since 13-02. The other Phase 13 keys
    // live in `[cluster]`, `[api]` and `[workers]`.
    "modules",
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
    /// `[api.tls]`: client-listener TLS (Phase 7, SEC-01; implemented by 07-06/07-10).
    pub tls: ApiTlsSection,
    /// Phase 13 (D-P13-14, 13-12): where a start without an idempotency key or run id is
    /// placed: `local` (a partition this node leads, zero hops) or `uniform`.
    pub unkeyed_start_placement: UnkeyedStartPlacement,
    /// Phase 13 (D-P13-15, 13-13): serve partition reads only after a full barrier instead
    /// of under the leader lease (default false).
    pub strict_reads: bool,
    /// Phase 13 (D-P13-15/26, 13-13): leader-lease safety margin as a fraction of
    /// `[replication] session_timeout` (0 < m < 0.5, default 0.2).
    pub lease_margin: f64,
}

impl Default for ApiSection {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 7700)),
            max_module_bytes: 64 * MIB,
            max_request_bytes: 4 * MIB,
            tls: ApiTlsSection::default(),
            unkeyed_start_placement: UnkeyedStartPlacement::default(),
            strict_reads: false,
            lease_margin: 0.2,
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
    /// `engine.class` (plan 09-07): guest language class sizing the pool, `rust` (default),
    /// `python`, `typescript` or `mixed`.
    pub class: EngineClass,
    /// `engine.pool.total_core_instances` (plan 09-07): override of the pool's core-instance
    /// total and of the weighted admission budget (default: `pool_total_instances` × the
    /// class's core instances per instance).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_total_core_instances: Option<u32>,
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
            class: EngineClass::default(),
            pool_total_core_instances: e.pool.total_core_instances,
        }
    }
}

impl EngineSection {
    /// Map onto an [`EngineConfig`], starting from its defaults.
    pub fn to_engine_config(&self) -> EngineConfig {
        let d = EngineConfig::default();
        let mut pool = d.pool.clone();
        pool.total_instances = self.pool_total_instances;
        pool.total_core_instances = self.pool_total_core_instances;
        EngineConfig {
            class: self.class.into(),
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

/// `[engine] class` values (plan 09-07); an unknown value is a config error listing these.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineClass {
    #[default]
    Rust,
    Python,
    Typescript,
    Mixed,
}

impl<'de> Deserialize<'de> for EngineClass {
    /// From the config spelling; the error names the allowed values (the `config` crate's
    /// own enum error does not).
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use ostro_engine::config::LanguageClass as L;
        let s = String::deserialize(d)?;
        Ok(match s.parse::<L>().map_err(serde::de::Error::custom)? {
            L::Rust => Self::Rust,
            L::Python => Self::Python,
            L::TypeScript => Self::Typescript,
            L::Mixed => Self::Mixed,
        })
    }
}

impl From<EngineClass> for ostro_engine::config::LanguageClass {
    fn from(c: EngineClass) -> Self {
        match c {
            EngineClass::Rust => Self::Rust,
            EngineClass::Python => Self::Python,
            EngineClass::Typescript => Self::TypeScript,
            EngineClass::Mixed => Self::Mixed,
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
    // Phase 7 sections (RESERVATIONS §11, plan 07-02); `[api.tls]` is `api.tls`.
    pub workers: WorkersSection,
    pub cluster: ClusterSection,
    pub flow: FlowSection,
    // Phase 11 sections (D-P11-25, plan 11-04).
    pub limits: LimitsSection,
    pub ui: UiSection,
    pub bulk: BulkSection,
    pub dlq: DlqSection,
    // Phase 12 section (D-P12-10, plan 12-02).
    pub replication: ReplicationSection,
    // Phase 10 section (D-P10-6, plan 10-10).
    pub dev: DevSection,
    // Phase 13 section (D-P13-23, plan 13-02).
    pub modules: ModulesSection,
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
        // D-W16: an ephemeral loopback cluster port, so parallel `ostro dev` never collide.
        c.cluster.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        // D-P11-17: the dev profile serves the UI because its listener is loopback
        // (`[ui] enabled` stays unset, so a printed config reloads identically on either
        // profile).
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

    /// The engine configuration: `[engine]` mapped by [`EngineSection::to_engine_config`]
    /// plus `[limits]` as `run_limits` (D-P11-13).
    pub fn to_engine_config(&self) -> EngineConfig {
        EngineConfig {
            run_limits: self.limits.to_run_limits(),
            ..self.engine.to_engine_config()
        }
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
        // Phase 7 sections (07-02).
        self.validate_phase7()?;
        // Phase 11 sections (11-04).
        self.validate_phase11()?;
        // Phase 12 (12-02).
        self.validate_phase12()?;
        // Phase 13 (13-02).
        self.validate_phase13()?;
        Ok(())
    }

    /// Phase 13 value checks (the D-P13-23 keys in `[cluster]`, `[api]`, `[modules]`;
    /// 13-02, defaults D-P13-26).
    pub fn validate_phase13(&self) -> Result<(), ConfigError> {
        let c = &self.cluster;
        for (key, d) in [
            ("cluster.join_timeout", c.join_timeout),
            ("cluster.join_orphan_timeout", c.join_orphan_timeout),
            ("cluster.handoff_grace", c.handoff_grace),
            ("cluster.handoff_catchup_timeout", c.handoff_catchup_timeout),
            (
                "cluster.shutdown_handoff_timeout",
                c.shutdown_handoff_timeout,
            ),
            (
                "cluster.leader_rebalance_interval",
                c.leader_rebalance_interval,
            ),
            ("modules.gc_interval", self.modules.gc_interval),
        ] {
            if d.get().is_zero() {
                return Err(ConfigError::Invalid(format!("{key} must be non-zero")));
            }
        }
        // D-P13-8 (W8): an orphan joiner is tombstoned only after its join token expired.
        if c.join_orphan_timeout.get() < self.workers.join_token_ttl.get() {
            return Err(ConfigError::Invalid(format!(
                "cluster.join_orphan_timeout ({}) must be at least the join token lifetime workers.join_token_ttl ({})",
                c.join_orphan_timeout, self.workers.join_token_ttl
            )));
        }
        if c.max_concurrent_moves == 0 {
            return Err(ConfigError::Invalid(
                "cluster.max_concurrent_moves must be at least 1".into(),
            ));
        }
        if c.meta_max_message_bytes < 4 * MIB {
            return Err(ConfigError::Invalid(format!(
                "cluster.meta_max_message_bytes must be at least 4 MiB ({}; got {})",
                4 * MIB,
                c.meta_max_message_bytes
            )));
        }
        if usize::try_from(c.meta_max_message_bytes).is_err() {
            return Err(ConfigError::Invalid(format!(
                "cluster.meta_max_message_bytes ({}) does not fit this platform's usize",
                c.meta_max_message_bytes
            )));
        }
        let m = self.api.lease_margin;
        if !(m > 0.0 && m < 0.5) {
            return Err(ConfigError::Invalid(format!(
                "api.lease_margin must be within 0 < m < 0.5 (got {m})"
            )));
        }
        Ok(())
    }

    /// Phase 12 checks (`[replication]`, the `[cluster]` peer keys; 12-02, D-P12-14 B3).
    ///
    /// A static cluster (`[[cluster.peers]]` non-empty) needs `cluster_id`,
    /// `[cluster.tls] ca_cert_file` and `ca_key_file`; peer ids are unique, non-zero and
    /// never this node's own `[node] node_id`.
    pub fn validate_phase12(&self) -> Result<(), ConfigError> {
        let c = &self.cluster;
        if let Some(id) = &c.cluster_id
            && id.trim().is_empty()
        {
            return Err(ConfigError::Invalid(
                "cluster.cluster_id must not be empty when set".into(),
            ));
        }
        if !c.peers.is_empty() {
            let mut missing = Vec::new();
            if c.cluster_id.is_none() {
                missing.push("cluster.cluster_id");
            }
            if c.tls.ca_cert_file.is_none() {
                missing.push("cluster.tls.ca_cert_file");
            }
            if c.tls.ca_key_file.is_none() {
                missing.push("cluster.tls.ca_key_file");
            }
            if !missing.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "cluster.peers is set (static cluster), so {} must also be set",
                    missing.join(", ")
                )));
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for peer in &c.peers {
            if peer.id == 0 {
                return Err(ConfigError::Invalid(
                    "cluster.peers: id 0 is reserved (no-leader sentinel)".into(),
                ));
            }
            if peer.id == self.node.node_id {
                return Err(ConfigError::Invalid(format!(
                    "cluster.peers lists this node's own node.node_id {}; list only the other nodes",
                    peer.id
                )));
            }
            if !seen.insert(peer.id) {
                return Err(ConfigError::Invalid(format!(
                    "cluster.peers: duplicate id {}",
                    peer.id
                )));
            }
            if peer.addr.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "cluster.peers: peer {} needs a non-empty addr",
                    peer.id
                )));
            }
        }
        let r = &self.replication;
        for (key, d) in [
            ("replication.session_timeout", r.session_timeout),
            ("replication.replica_lag_max", r.replica_lag_max),
            ("replication.meta_tick_interval", r.meta_tick_interval),
            ("replication.fetch_max_wait", r.fetch_max_wait),
            ("replication.fetch_read_timeout", r.fetch_read_timeout),
        ] {
            if d.get().is_zero() {
                return Err(ConfigError::Invalid(format!("{key} must be non-zero")));
            }
        }
        if r.meta_heartbeat_tick == 0 || r.meta_heartbeat_tick >= r.meta_election_tick {
            return Err(ConfigError::Invalid(format!(
                "replication.meta_heartbeat_tick ({}) must be at least 1 and below replication.meta_election_tick ({})",
                r.meta_heartbeat_tick, r.meta_election_tick
            )));
        }
        if r.fetch_max_bytes.get() == 0 {
            return Err(ConfigError::Invalid(
                "replication.fetch_max_bytes must be at least 1".into(),
            ));
        }
        if r.rf == Some(0) {
            return Err(ConfigError::Invalid(
                "replication.rf must be at least 1".into(),
            ));
        }
        if r.min_insync == Some(0) {
            return Err(ConfigError::Invalid(
                "replication.min_insync must be at least 1".into(),
            ));
        }
        if let (Some(rf), Some(min)) = (r.rf, r.min_insync)
            && min > rf
        {
            return Err(ConfigError::Invalid(format!(
                "replication.min_insync ({min}) must not exceed replication.rf ({rf})"
            )));
        }
        Ok(())
    }

    /// Phase 11 value checks (`[limits]`, `[bulk]`, `[dlq]`; 11-04).
    pub fn validate_phase11(&self) -> Result<(), ConfigError> {
        let l = &self.limits;
        for (key, v) in [
            ("limits.max_history_events", l.max_history_events),
            ("limits.max_children_per_run", l.max_children_per_run),
            ("limits.max_payload_bytes", l.max_payload_bytes.get()),
        ] {
            if v == 0 {
                return Err(ConfigError::Invalid(format!("{key} must be at least 1")));
            }
        }
        if l.max_payload_bytes.get() > self.api.max_request_bytes {
            return Err(ConfigError::Invalid(format!(
                "limits.max_payload_bytes ({}) must not exceed api.max_request_bytes ({})",
                l.max_payload_bytes.get(),
                self.api.max_request_bytes
            )));
        }
        let b = &self.bulk;
        for (key, v) in [
            ("bulk.max_jobs", u64::from(b.max_jobs)),
            ("bulk.max_matches", b.max_matches),
            ("bulk.default_rate", u64::from(b.default_rate)),
            ("bulk.max_rate", u64::from(b.max_rate)),
            ("bulk.burst", u64::from(b.burst)),
            ("bulk.max_in_flight", u64::from(b.max_in_flight)),
            ("bulk.backlog_high", b.backlog_high),
            ("bulk.checkpoint_ops", b.checkpoint_ops),
        ] {
            if v == 0 {
                return Err(ConfigError::Invalid(format!("{key} must be at least 1")));
            }
        }
        if b.default_rate > b.max_rate {
            return Err(ConfigError::Invalid(format!(
                "bulk.default_rate ({}) must not exceed bulk.max_rate ({})",
                b.default_rate, b.max_rate
            )));
        }
        for (key, d) in [
            ("bulk.checkpoint_every", b.checkpoint_every),
            ("dlq.hook_catchup", self.dlq.hook_catchup),
        ] {
            if d.get().is_zero() {
                return Err(ConfigError::Invalid(format!("{key} must be non-zero")));
            }
        }
        Ok(())
    }

    /// Phase 7 value checks (the TOML/env layer already rejects unknown keys and modes).
    fn validate_phase7(&self) -> Result<(), ConfigError> {
        let w = &self.workers;
        if w.max_frame_bytes == 0 || w.max_frame_bytes > MAX_WORKER_FRAME_BYTES {
            return Err(ConfigError::Invalid(format!(
                "workers.max_frame_bytes must be within 1..={MAX_WORKER_FRAME_BYTES} (got {})",
                w.max_frame_bytes
            )));
        }
        if w.max_append_bytes < w.max_frame_bytes {
            return Err(ConfigError::Invalid(
                "workers.max_append_bytes must be at least workers.max_frame_bytes".into(),
            ));
        }
        for (key, d) in [
            ("workers.heartbeat", w.heartbeat),
            ("workers.keepalive", w.keepalive),
            ("workers.client_cert_ttl", w.client_cert_ttl),
            ("workers.join_token_ttl", w.join_token_ttl),
            ("workers.spill.sustain", w.spill.sustain),
            ("workers.spill.cooldown", w.spill.cooldown),
            ("api.tls.reload_poll", self.api.tls.reload_poll),
            ("flow.idle_key_evict", self.flow.idle_key_evict),
        ] {
            if d.get().is_zero() {
                return Err(ConfigError::Invalid(format!("{key} must be non-zero")));
            }
        }
        for (key, v) in [
            ("workers.spill.cpu_pct", w.spill.cpu_pct),
            ("workers.spill.slots_pct", w.spill.slots_pct),
            ("workers.spill.mem_pct", w.spill.mem_pct),
            ("workers.spill.hysteresis_pct", w.spill.hysteresis_pct),
        ] {
            if !(1..=100).contains(&v) {
                return Err(ConfigError::Invalid(format!(
                    "{key} must be within 1..=100 (got {v})"
                )));
            }
        }
        if self.flow.default_queue_weight == 0 {
            return Err(ConfigError::Invalid(
                "flow.default_queue_weight must be at least 1".into(),
            ));
        }
        if self.flow.max_tracked_keys == 0 {
            return Err(ConfigError::Invalid(
                "flow.max_tracked_keys must be at least 1".into(),
            ));
        }
        let tls = &self.api.tls;
        if tls.mode == ApiTlsMode::Files && (tls.cert_file.is_none() || tls.key_file.is_none()) {
            return Err(ConfigError::Invalid(
                "api.tls.mode = \"files\" needs api.tls.cert_file and api.tls.key_file".into(),
            ));
        }
        if tls.mode == ApiTlsMode::Acme && tls.acme.domains.is_empty() {
            return Err(ConfigError::Invalid(
                "api.tls.mode = \"acme\" needs at least one api.tls.acme.domains entry".into(),
            ));
        }
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

// ---------------------------------------------------------------------------------
// Phase 7 sections (RESERVATIONS §11; parsed since plan 07-02, implemented by 07-06..07-16)
// ---------------------------------------------------------------------------------

/// Upper bound of `[workers] max_frame_bytes` (the tonic message limit is 4 MiB, D-W2).
pub const MAX_WORKER_FRAME_BYTES: u64 = 4 * MIB;

/// Where runs execute by default (`[workers] default_placement`, `QueuePolicy.placement`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlacementSetting {
    /// On this node's engine only.
    #[default]
    Embedded,
    /// On remote workers only.
    Workers,
    /// Embedded first, spilling to workers under load (D-W9).
    Auto,
}

/// `[workers.spill]`: spill hysteresis thresholds (D-W9).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpillSection {
    /// Process CPU (getrusage, normalised by `available_parallelism`) entry threshold, %.
    pub cpu_pct: u32,
    /// Embedded slot use entry threshold, %.
    pub slots_pct: u32,
    /// Wasm memory (engine budget) entry threshold, %.
    pub mem_pct: u32,
    /// Oldest ready entry older than this enters spilling.
    pub lag: FriendlyDuration,
    /// A CPU signal must hold this long.
    pub sustain: FriendlyDuration,
    /// Minimum dwell and exit window.
    pub cooldown: FriendlyDuration,
    /// Exit requires every signal below `entry * hysteresis_pct / 100`.
    pub hysteresis_pct: u32,
}

impl Default for SpillSection {
    fn default() -> Self {
        Self {
            cpu_pct: 70,
            slots_pct: 90,
            mem_pct: 85,
            lag: FriendlyDuration::from_secs(5),
            sustain: FriendlyDuration::from_secs(10),
            cooldown: FriendlyDuration::from_secs(30),
            hysteresis_pct: 85,
        }
    }
}

/// `[workers]`: the remote worker protocol and placement (D-W2, D-W4, D-W9, D-W16).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkersSection {
    /// Serve the worker protocol on `[cluster] listen` (default true in both profiles).
    pub enabled: bool,
    /// Largest protocol frame in bytes (default 1 MiB; at most 4 MiB).
    pub max_frame_bytes: u64,
    /// Largest reassembled append in bytes (default 64 MiB).
    pub max_append_bytes: u64,
    /// Session heartbeat; the effective value is `min(heartbeat, lease_ttl / 3)` (W4).
    pub heartbeat: FriendlyDuration,
    /// h2 keepalive interval.
    pub keepalive: FriendlyDuration,
    /// Refuse workers of another Ostro version (default true).
    pub require_same_version: bool,
    /// Lifetime of join-issued client certificates (default 24 h).
    pub client_cert_ttl: FriendlyDuration,
    /// Default lifetime of `ostro worker token create` tokens (default 24 h).
    pub join_token_ttl: FriendlyDuration,
    /// Placement of queues without a `QueuePolicy.placement` (default `embedded`).
    pub default_placement: PlacementSetting,
    /// `[workers.spill]`.
    pub spill: SpillSection,
    /// Phase 13 (D-P13-18, 13-14): worker admission by version. `compatible` (default)
    /// admits a worker whose feature level and WIT lines fit the cluster level and the
    /// module; `exact` keeps the Phase 7 same-version rule (`require_same_version`).
    pub version_policy: VersionPolicy,
}

impl Default for WorkersSection {
    fn default() -> Self {
        Self {
            enabled: true,
            max_frame_bytes: MIB,
            max_append_bytes: 64 * MIB,
            heartbeat: FriendlyDuration::from_secs(10),
            keepalive: FriendlyDuration::from_secs(5),
            require_same_version: true,
            client_cert_ttl: FriendlyDuration::from_secs(24 * 3600),
            join_token_ttl: FriendlyDuration::from_secs(24 * 3600),
            default_placement: PlacementSetting::Embedded,
            spill: SpillSection::default(),
            version_policy: VersionPolicy::default(),
        }
    }
}

impl WorkersSection {
    /// `Welcome.heartbeat_ms`: `min(heartbeat, lease_ttl / 3)` (W4).
    pub fn effective_heartbeat(&self, lease_ttl: Duration) -> Duration {
        self.heartbeat.get().min(lease_ttl / 3)
    }
}

/// `[cluster.tls]`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterTlsSection {
    /// External CA whose certificates are accepted as worker identities (cert-manager
    /// path, D-W25); unset = only join-issued certificates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ca: Option<PathBuf>,
    /// Phase 12 (D-P12-14): the shared cluster CA certificate (PEM), imported with
    /// `ClusterCa::import_pem`. Required with `[[cluster.peers]]`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_cert_file: Option<PathBuf>,
    /// Phase 12: the shared cluster CA private key (PEM). Required with `[[cluster.peers]]`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_key_file: Option<PathBuf>,
}

/// One `[[cluster.peers]]` entry (Phase 12, D-P12-12): another voter of a static cluster.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PeerConfig {
    /// The peer's `[node] node_id` (non-zero, unique, never this node's own id).
    pub id: u64,
    /// The peer's cluster listener (`host:port`).
    pub addr: String,
    /// The peer's dedicated replication listener, when it runs one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repl_addr: Option<String>,
}

/// `[cluster]`: the cluster listener (worker protocol, mTLS always; D-W16).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterSection {
    /// Listen address (server `0.0.0.0:7701`, dev `127.0.0.1:0`).
    pub listen: SocketAddr,
    /// Names / addresses advertised to workers (and in the node cert SANs); empty = the
    /// bound address.
    pub advertise: Vec<String>,
    /// `[cluster.tls]`.
    pub tls: ClusterTlsSection,
    /// Phase 12 (D-P12-14): the cluster id. Required with `peers`; otherwise resolved from
    /// node.toml / the CA / meta.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    /// Phase 12: an optional second listener serving only `Replication` (RP-10).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_listen: Option<SocketAddr>,
    /// Phase 12: the other voters of a static cluster (`[[cluster.peers]]`); empty = single
    /// node.
    pub peers: Vec<PeerConfig>,
    // Phase 13 keys (D-P13-23, defaults D-P13-26; 13-02 parses, the owning plans consume).
    /// Finalize the cluster feature level automatically once every member supports it
    /// (D-P13-4, 13-04; default true).
    pub auto_finalize: bool,
    /// A joiner fails loudly when meta has no leader after this long (D-P13-9, 13-09;
    /// default 120 s).
    pub join_timeout: FriendlyDuration,
    /// A Joining member without contact for this long is tombstoned (D-P13-8, 13-09;
    /// default 25 h; at least `[workers] join_token_ttl`).
    pub join_orphan_timeout: FriendlyDuration,
    /// A learner is promoted once it is within this many entries of the commit index
    /// (D-P13-8, 13-09; default 64).
    pub promote_lag: u64,
    /// Handoff: wait this long for in-flight activations (D-P13-13, 13-11; default 2 s).
    pub handoff_grace: FriendlyDuration,
    /// Handoff: wait this long for the target to catch up (13-11; default 10 s).
    pub handoff_catchup_timeout: FriendlyDuration,
    /// Controlled shutdown: hand off for at most this long (13-11; default 30 s).
    pub shutdown_handoff_timeout: FriendlyDuration,
    /// Preferred-leader balancing period (D-P13-12, 13-10; default 5 m).
    pub leader_rebalance_interval: FriendlyDuration,
    /// Concurrent partition moves of the placement controller (13-10; default 2).
    pub max_concurrent_moves: u32,
    /// MetaPeer gRPC encode/decode limit in bytes (Pitfall 4, 13-05; default 256 MiB).
    pub meta_max_message_bytes: u64,
}

impl Default for ClusterSection {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 7701)),
            advertise: Vec::new(),
            tls: ClusterTlsSection::default(),
            cluster_id: None,
            replication_listen: None,
            peers: Vec::new(),
            auto_finalize: true,
            join_timeout: FriendlyDuration::from_secs(120),
            join_orphan_timeout: FriendlyDuration::from_secs(25 * 3600),
            promote_lag: 64,
            handoff_grace: FriendlyDuration::from_secs(2),
            handoff_catchup_timeout: FriendlyDuration::from_secs(10),
            shutdown_handoff_timeout: FriendlyDuration::from_secs(30),
            leader_rebalance_interval: FriendlyDuration::from_secs(300),
            max_concurrent_moves: 2,
            meta_max_message_bytes: 256 * MIB,
        }
    }
}

/// `[replication]` (Phase 12, D-P12-10): replication and metadata Raft timings. `rf` and
/// `min_insync` are bootstrap seeds only (research OQ3); unset = `min(3, voters)` and
/// `2` when rf ≥ 3, else `1`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationSection {
    /// A partition leader without meta contact for this long self-demotes; the controller's
    /// liveness window.
    pub session_timeout: FriendlyDuration,
    /// A follower not caught up within this long leaves the ISR.
    pub replica_lag_max: FriendlyDuration,
    /// Metadata Raft tick.
    pub meta_tick_interval: FriendlyDuration,
    /// Metadata Raft election timeout in ticks.
    pub meta_election_tick: u32,
    /// Metadata Raft heartbeat interval in ticks (below `meta_election_tick`).
    pub meta_heartbeat_tick: u32,
    /// Fetch long-poll idle wait.
    pub fetch_max_wait: FriendlyDuration,
    /// Server-side bound of one fetch read (never cut by `fetch_max_wait`).
    pub fetch_read_timeout: FriendlyDuration,
    /// Fetch response size cap.
    pub fetch_max_bytes: ByteSize,
    /// Bootstrap replication factor seed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rf: Option<u8>,
    /// Bootstrap minimum ISR seed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_insync: Option<u8>,
}

impl Default for ReplicationSection {
    fn default() -> Self {
        Self {
            session_timeout: FriendlyDuration::from_secs(3),
            replica_lag_max: FriendlyDuration::from_secs(10),
            meta_tick_interval: FriendlyDuration::from_millis(100),
            meta_election_tick: 10,
            meta_heartbeat_tick: 2,
            fetch_max_wait: FriendlyDuration::from_millis(500),
            fetch_read_timeout: FriendlyDuration::from_secs(5),
            fetch_max_bytes: ByteSize(8 * MIB),
            rf: None,
            min_insync: None,
        }
    }
}

/// `[api.tls] mode` (D-W8).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApiTlsMode {
    /// Loopback plaintext; non-loopback self-signed in dev; the server profile refuses a
    /// non-loopback listener without `files`/`acme` unless `allow_self_signed`.
    #[default]
    Auto,
    Off,
    SelfSigned,
    Files,
    Acme,
}

/// `[api.tls.acme] challenge`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AcmeChallenge {
    /// TLS-ALPN-01 on the client listener (needs :443).
    #[default]
    #[serde(rename = "tls-alpn-01")]
    TlsAlpn01,
    /// HTTP-01 on `http01_listen` (needs :80).
    #[serde(rename = "http-01")]
    Http01,
}

/// Let's Encrypt production directory (the `[api.tls.acme] directory` default).
pub const LETS_ENCRYPT_DIRECTORY: &str = "https://acme-v02.api.letsencrypt.org/directory";

/// `[api.tls.acme]` (07-10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AcmeSection {
    pub domains: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// ACME directory URL (default Let's Encrypt production).
    pub directory: String,
    /// Extra CA bundle trusted for the directory (Pebble, private ACME).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directory_ca_file: Option<PathBuf>,
    /// Account + certificate cache; unset = `<data_dir>/tls/acme`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_dir: Option<PathBuf>,
    pub challenge: AcmeChallenge,
    /// HTTP-01 responder address.
    pub http01_listen: SocketAddr,
}

impl Default for AcmeSection {
    fn default() -> Self {
        Self {
            domains: Vec::new(),
            email: None,
            directory: LETS_ENCRYPT_DIRECTORY.to_owned(),
            directory_ca_file: None,
            cache_dir: None,
            challenge: AcmeChallenge::TlsAlpn01,
            http01_listen: SocketAddr::from(([0, 0, 0, 0], 80)),
        }
    }
}

/// `[api.tls]`: client-listener TLS (SEC-01, D-W7/D-W8; 07-06/07-10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiTlsSection {
    pub mode: ApiTlsMode,
    /// PEM certificate chain (`files` mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    /// PEM private key (`files` mode).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    /// Let the server profile serve a self-signed certificate on a non-loopback listener.
    pub allow_self_signed: bool,
    /// `files` mode mtime poll period.
    pub reload_poll: FriendlyDuration,
    /// `[api.tls.acme]`.
    pub acme: AcmeSection,
}

impl Default for ApiTlsSection {
    fn default() -> Self {
        Self {
            mode: ApiTlsMode::Auto,
            cert_file: None,
            key_file: None,
            allow_self_signed: false,
            reload_poll: FriendlyDuration::from_secs(30),
            acme: AcmeSection::default(),
        }
    }
}

/// `[flow]`: node flow-control bookkeeping (DISP-03/04/05; 07-08).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FlowSection {
    /// Most (queue, key) counters / GCRA cells kept; the idlest are evicted first.
    pub max_tracked_keys: u64,
    /// Evict an idle (queue, key) entry after this long.
    pub idle_key_evict: FriendlyDuration,
    /// DRR weight of queues without `QueuePolicy.weight`.
    pub default_queue_weight: u32,
}

impl Default for FlowSection {
    fn default() -> Self {
        Self {
            max_tracked_keys: 100_000,
            idle_key_evict: FriendlyDuration::from_secs(600),
            default_queue_weight: 1,
        }
    }
}

// ---------------------------------------------------------------------------------
// Phase 11 sections (D-P11-25; plan 11-04)
// ---------------------------------------------------------------------------------

/// Environment overlay of `[ui] allowed_hosts` (comma-separated; D-P11-25).
pub const UI_ALLOWED_HOSTS_ENV: &str = "OSTRO_UI_ALLOWED_HOSTS";

/// `[limits]`: per-run limits (OPS-07, D-P11-13), mapped onto
/// [`ostro_engine::RunLimits`] (`EngineConfig.run_limits`). Enforced only on new live
/// commands (11-06 engine, 11-11 API caps), never during replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsSection {
    /// Journal events per run (default 50,000).
    pub max_history_events: u64,
    /// Child runs started by one run (default 10,000).
    pub max_children_per_run: u64,
    /// One payload (input, output, signal, step result; default `"4MiB"`). Must not
    /// exceed `[api] max_request_bytes`.
    pub max_payload_bytes: ByteSize,
}

impl Default for LimitsSection {
    fn default() -> Self {
        let d = ostro_engine::RunLimits::default();
        Self {
            max_history_events: d.max_history_events,
            max_children_per_run: d.max_children_per_run,
            max_payload_bytes: ByteSize(d.max_payload_bytes),
        }
    }
}

impl LimitsSection {
    /// As the engine's [`ostro_engine::RunLimits`].
    pub fn to_run_limits(&self) -> ostro_engine::RunLimits {
        ostro_engine::RunLimits {
            max_history_events: self.max_history_events,
            max_children_per_run: self.max_children_per_run,
            max_payload_bytes: self.max_payload_bytes.get(),
        }
    }
}

/// `[ui]`: the embedded operator UI (D-P11-17).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UiSection {
    /// Serve `/ui`. Unset (both profiles): on exactly when the API listener is loopback,
    /// which the dev profile's is ([`UiSection::effective_enabled`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Extra `Host` values accepted besides the bound address and loopback names
    /// (overlay: `OSTRO_UI_ALLOWED_HOSTS`, comma-separated).
    pub allowed_hosts: Vec<String>,
}

impl UiSection {
    /// Whether the UI is served for an API listener on `listen`: the explicit
    /// `enabled`, else whether `listen` is loopback.
    pub fn effective_enabled(&self, listen: SocketAddr) -> bool {
        self.enabled.unwrap_or_else(|| listen.ip().is_loopback())
    }
}

/// `[bulk]`: node-local bulk operation jobs (OPS-03, D-P11-20; implemented by 11-13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BulkSection {
    /// Concurrent jobs per node (default 4).
    pub max_jobs: u32,
    /// Runs one job may select (default 100,000).
    pub max_matches: u64,
    /// Ops per second when the request names none (default 20).
    pub default_rate: u32,
    /// Highest accepted rate (default 500).
    pub max_rate: u32,
    /// GCRA burst (default 10).
    pub burst: u32,
    /// Concurrent ops per job (default 4).
    pub max_in_flight: u32,
    /// Target-partition ready backlog that pauses a job (default 10,000).
    pub backlog_high: u64,
    /// Checkpoint cadence (default 1 s).
    pub checkpoint_every: FriendlyDuration,
    /// Checkpoint after this many ops (default 500).
    pub checkpoint_ops: u64,
}

impl Default for BulkSection {
    fn default() -> Self {
        Self {
            max_jobs: 4,
            max_matches: 100_000,
            default_rate: 20,
            max_rate: 500,
            burst: 10,
            max_in_flight: 4,
            backlog_high: 10_000,
            checkpoint_every: FriendlyDuration::from_secs(1),
            checkpoint_ops: 500,
        }
    }
}

/// `[dlq]`: DLQ and on_failure hooks (OPS-04/05, D-P11-9..12; implemented by 11-12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DlqSection {
    /// Default on_failure hook catch-up window at leader start (default 24 h).
    pub hook_catchup: FriendlyDuration,
    /// Run on_failure hooks at all (default true).
    pub hooks_enabled: bool,
}

impl Default for DlqSection {
    fn default() -> Self {
        Self {
            hook_catchup: FriendlyDuration::from_secs(24 * 3600),
            hooks_enabled: true,
        }
    }
}

// ---------------------------------------------------------------------------------
// Phase 13 (D-P13-23, RESERVATIONS §11; parsed since plan 13-02)
// ---------------------------------------------------------------------------------

/// `[api] unkeyed_start_placement` (D-P13-14, 13-12).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnkeyedStartPlacement {
    /// A partition this node leads (zero hops); uniform when it leads none.
    #[default]
    Local,
    /// A uniformly random partition (may forward).
    Uniform,
}

/// `[workers] version_policy` (D-P13-18, 13-14).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VersionPolicy {
    /// Admit compatible worker versions (feature level, WIT lines, event codec).
    #[default]
    Compatible,
    /// The Phase 7 rule: the same Ostro and Wasmtime version only.
    Exact,
}

/// `[modules]`: module retirement and garbage collection (D-P13-20, 13-15).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModulesSection {
    /// Module GC period on the meta leader (default 10 m).
    pub gc_interval: FriendlyDuration,
    /// A module unreferenced for this long may be collected (default 10 m).
    pub gc_grace: FriendlyDuration,
    /// Keep modules referenced by resumable terminal runs (retry in place) (default true).
    pub gc_protect_resumable: bool,
}

impl Default for ModulesSection {
    fn default() -> Self {
        Self {
            gc_interval: FriendlyDuration::from_secs(600),
            gc_grace: FriendlyDuration::from_secs(600),
            gc_protect_resumable: true,
        }
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
            // String-list keys take a comma-separated value (`OSTRO_EGRESS__ALLOW_PRIVATE=
            // 127.0.0.0/8,::1/128`); every other key stays a scalar (10-17).
            .list_separator(",")
            .with_list_parse_key("egress.allow_private")
            .source(Some(env.sectioned())),
    );
    let mut cfg: ServerConfig = b.build()?.try_deserialize()?;
    if cfg.runs.retention.get().is_zero() {
        return Err(ConfigError::Invalid(
            "runs.retention must be non-zero (terminal runs always expire, D9)".into(),
        ));
    }
    if let Some(hosts) = env.lookup(UI_ALLOWED_HOSTS_ENV) {
        cfg.ui.allowed_hosts = hosts
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .map(str::to_owned)
            .collect();
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
                // Phase 12 `[cluster] peers`: an array of inline tables.
                serde_json::Value::Array(_) => inline_table(val),
                // `[telemetry] otlp_headers` and `[secrets.vault]` / `[secrets.k8s]`: an
                // inline table of scalars (unset keys omitted).
                // Phase 7 `[api] tls` / `[cluster] tls` / `[workers] spill` nest one more
                // level (`tls.acme`): rendered recursively as inline tables.
                serde_json::Value::Object(_) => inline_table(val),
            };
            let _ = writeln!(out, "{k} = {rendered}");
        }
    }
    out
}

/// A JSON value as a TOML inline value: objects become inline tables (recursively, unset
/// keys omitted), everything else is its JSON form (valid TOML for strings, numbers,
/// booleans and arrays of scalars).
fn inline_table(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let items: Vec<String> = map
                .iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| {
                    format!(
                        "{} = {}",
                        serde_json::Value::String(k.clone()),
                        inline_table(v)
                    )
                })
                .collect();
            format!("{{ {} }}", items.join(", "))
        }
        serde_json::Value::Array(items) => {
            let items: Vec<String> = items.iter().map(inline_table).collect();
            format!("[{}]", items.join(", "))
        }
        other => other.to_string(),
    }
}

// --- Phase 10 (10-10) ---

/// `[dev] time_skipping` (D-P10-6): the dev clock mode. Default `off`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeSkipping {
    /// No dev clock (the server runs the plain clock path).
    #[default]
    Off,
    /// Skip to the next due timer whenever the node is idle (10-13).
    Auto,
    /// Time moves only through `POST /v1/dev/time/skip`.
    Manual,
}

impl TimeSkipping {
    /// The API mode of a dev clock, `None` when off.
    pub fn mode(self) -> Option<ostro_api::domain::DevTimeMode> {
        match self {
            TimeSkipping::Off => None,
            TimeSkipping::Auto => Some(ostro_api::domain::DevTimeMode::Auto),
            TimeSkipping::Manual => Some(ostro_api::domain::DevTimeMode::Manual),
        }
    }

    /// Wire name: `off`, `auto` or `manual`.
    pub fn as_str(self) -> &'static str {
        match self {
            TimeSkipping::Off => "off",
            TimeSkipping::Auto => "auto",
            TimeSkipping::Manual => "manual",
        }
    }
}

/// `[dev]` (D-P10-6): dev-only knobs. Only `ostro dev --ephemeral` may turn them on
/// ([`ServerConfig::validate_time_skipping`]).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DevSection {
    /// Dev clock time skipping (`off` | `auto` | `manual`; `OSTRO_DEV__TIME_SKIPPING`).
    pub time_skipping: TimeSkipping,
}

impl ServerConfig {
    /// Time skipping (D-P10-6) is allowed only on an ephemeral `ostro dev` node with
    /// embedded execution only. `server_profile`: the node runs as `ostro server`. Each
    /// error names `--time-skipping` and the reason. Off always passes.
    pub fn validate_time_skipping(&self, server_profile: bool) -> Result<(), ConfigError> {
        if self.dev.time_skipping == TimeSkipping::Off {
            return Ok(());
        }
        if server_profile {
            return Err(ConfigError::Invalid(
                "--time-skipping ([dev] time_skipping) is dev only: `ostro server` refuses it; use `ostro dev --ephemeral --time-skipping`".into(),
            ));
        }
        if !self.node.ephemeral {
            return Err(ConfigError::Invalid(
                "--time-skipping needs --ephemeral: skipped time is written into the journal, so it is refused on a persistent data directory".into(),
            ));
        }
        if self.workers.enabled {
            return Err(ConfigError::Invalid(
                "--time-skipping cannot be used with a worker listener ([workers] enabled = true): only embedded executors follow the dev clock".into(),
            ));
        }
        if self.workers.default_placement != PlacementSetting::Embedded {
            return Err(ConfigError::Invalid(format!(
                "--time-skipping cannot be used with remote placement ([workers] default_placement = \"{}\"): only embedded executors follow the dev clock",
                match self.workers.default_placement {
                    PlacementSetting::Workers => "workers",
                    _ => "auto",
                }
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod dev_section_tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvSource {
        EnvSource::Map(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        )
    }

    #[test]
    fn dev_time_skipping_defaults_off_and_reads_env() {
        assert_eq!(ServerConfig::default().dev.time_skipping, TimeSkipping::Off);
        assert_eq!(
            ServerConfig::dev_defaults().dev.time_skipping,
            TimeSkipping::Off
        );
        for (raw, want) in [
            ("auto", TimeSkipping::Auto),
            ("manual", TimeSkipping::Manual),
            ("off", TimeSkipping::Off),
        ] {
            let c = load_layered(
                ServerConfig::dev_defaults(),
                None,
                false,
                &env(&[("OSTRO_DEV__TIME_SKIPPING", raw)]),
                &Overrides::default(),
            )
            .expect("load");
            assert_eq!(c.dev.time_skipping, want, "{raw}");
        }
        assert!(
            load_layered(
                ServerConfig::dev_defaults(),
                None,
                false,
                &env(&[("OSTRO_DEV__TIME_SKIPPING", "sometimes")]),
                &Overrides::default(),
            )
            .is_err()
        );
        assert!(
            load_layered(
                ServerConfig::dev_defaults(),
                None,
                false,
                &env(&[("OSTRO_DEV__BOGUS", "1")]),
                &Overrides::default(),
            )
            .is_err(),
            "[dev] keeps deny_unknown_fields"
        );
    }

    #[test]
    fn egress_allow_private_env_is_a_comma_list() {
        let load = |v: &str| {
            load_layered(
                ServerConfig::dev_defaults(),
                None,
                false,
                &env(&[("OSTRO_EGRESS__ALLOW_PRIVATE", v)]),
                &Overrides::default(),
            )
        };
        let c = load("127.0.0.0/8").expect("one CIDR");
        assert_eq!(c.egress.allow_private, vec!["127.0.0.0/8".to_owned()]);
        let c = load("127.0.0.0/8,::1/128").expect("two CIDRs");
        assert_eq!(
            c.egress.allow_private,
            vec!["127.0.0.0/8".to_owned(), "::1/128".to_owned()]
        );
        // Scalar keys are unaffected.
        let c = load_layered(
            ServerConfig::dev_defaults(),
            None,
            false,
            &env(&[("OSTRO_RUNS__RETENTION", "48h")]),
            &Overrides::default(),
        )
        .expect("scalar");
        assert_eq!(
            c.runs.retention.get(),
            std::time::Duration::from_secs(48 * 3600)
        );
    }

    #[test]
    fn validate_time_skipping_rules() {
        let mut c = ServerConfig::dev_defaults();
        c.validate_time_skipping(true).expect("off always passes");
        c.dev.time_skipping = TimeSkipping::Auto;
        let msg = |c: &ServerConfig, server| {
            c.validate_time_skipping(server)
                .expect_err("refused")
                .to_string()
        };
        assert!(msg(&c, false).contains("--ephemeral"));
        c.node.ephemeral = true;
        assert!(msg(&c, true).contains("ostro server"));
        assert!(
            msg(&c, false).contains("worker listener"),
            "workers on by default"
        );
        c.workers.enabled = false;
        c.validate_time_skipping(false)
            .expect("ephemeral, embedded-only dev");
        c.workers.default_placement = PlacementSetting::Workers;
        let m = msg(&c, false);
        assert!(
            m.contains("remote placement") && m.contains("--time-skipping"),
            "{m}"
        );
        assert_eq!(TimeSkipping::Manual.as_str(), "manual");
        assert_eq!(
            TimeSkipping::Auto.mode(),
            Some(ostro_api::domain::DevTimeMode::Auto)
        );
        assert_eq!(TimeSkipping::Off.mode(), None);
    }
}
