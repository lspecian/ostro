//! Layered ServerConfig: defaults < TOML file < OSTRO_* env < explicit overrides (CLI-02).

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

use ostro_server::config::{EnvSource, LogFormat, Overrides, ServerConfig, load_layered};

fn env(pairs: &[(&str, &str)]) -> EnvSource {
    EnvSource::Map(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect::<HashMap<_, _>>(),
    )
}

fn no_env() -> EnvSource {
    env(&[])
}

fn write_toml(dir: &tempfile::TempDir, body: &str) -> PathBuf {
    let p = dir.path().join("ostro.toml");
    std::fs::write(&p, body).expect("write toml");
    p
}

#[test]
fn dev_defaults() {
    let c = ServerConfig::dev_defaults();
    assert_eq!(
        c.api.listen,
        "127.0.0.1:7700".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(c.node.data_dir, PathBuf::from("./.ostro"));
    assert_eq!(c.node.partitions, 16);
    assert_eq!(c.node.node_id, 1);
    assert!(!c.node.ephemeral);
    assert_eq!(c.log.format, LogFormat::Pretty);
}

#[test]
fn server_defaults() {
    let c = ServerConfig::server_defaults();
    assert_eq!(c.api.listen, "0.0.0.0:7700".parse::<SocketAddr>().unwrap());
    assert_eq!(c.node.data_dir, PathBuf::from("./ostro-data"));
    assert_eq!(c.node.partitions, 16);
    assert_eq!(c.log.format, LogFormat::Compact);
}

#[test]
fn section_defaults() {
    let c = ServerConfig::default();
    assert_eq!(c.executor.lease_ttl.get(), Duration::from_secs(30));
    assert_eq!(c.executor.heartbeat, None);
    assert_eq!(c.executor.effective_heartbeat(), Duration::from_secs(10));
    assert_eq!(c.executor.max_attempt_failures, 3);
    assert_eq!(c.executor.slots, 64);
    assert_eq!(c.api.max_module_bytes, 64 * 1024 * 1024);
    assert_eq!(c.engine.hot_wait.get(), Duration::from_secs(2));
}

#[test]
fn zero_config_is_pure_defaults() {
    let c = load_layered(
        ServerConfig::dev_defaults(),
        None,
        false,
        &no_env(),
        &Overrides::default(),
    )
    .expect("load");
    assert_eq!(c, ServerConfig::dev_defaults());
}

#[test]
fn optional_missing_file_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.toml");
    let c = load_layered(
        ServerConfig::dev_defaults(),
        Some(&missing),
        false,
        &no_env(),
        &Overrides::default(),
    )
    .expect("load");
    assert_eq!(c, ServerConfig::dev_defaults());
}

#[test]
fn toml_overrides_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(
        &dir,
        r#"
[node]
partitions = 8

[api]
listen = "127.0.0.1:9000"

[executor]
lease_ttl = "45s"
heartbeat = "250ms"
"#,
    );
    let c = load_layered(
        ServerConfig::dev_defaults(),
        Some(&p),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .expect("load");
    assert_eq!(c.node.partitions, 8);
    assert_eq!(
        c.api.listen,
        "127.0.0.1:9000".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(c.executor.lease_ttl.get(), Duration::from_secs(45));
    assert_eq!(c.executor.effective_heartbeat(), Duration::from_millis(250));
    // untouched fields keep the base
    assert_eq!(c.node.data_dir, PathBuf::from("./.ostro"));
    assert_eq!(c.executor.slots, 64);
}

#[test]
fn env_overrides_toml() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(
        &dir,
        "[executor]\nlease_ttl = \"30s\"\n[api]\nlisten = \"127.0.0.1:9000\"\n",
    );
    let c = load_layered(
        ServerConfig::dev_defaults(),
        Some(&p),
        true,
        &env(&[
            ("OSTRO_EXECUTOR__LEASE_TTL", "5s"),
            ("OSTRO_API__LISTEN", "127.0.0.1:9100"),
            ("OSTRO_NODE__PARTITIONS", "4"),
            ("OSTRO_API__MAX_MODULE_BYTES", "1048576"),
            // not sectioned / not ours: ignored, never an unknown-key error
            ("OSTRO_DET_ITERS", "5"),
            ("PATH", "/bin"),
        ]),
        &Overrides::default(),
    )
    .expect("load");
    assert_eq!(c.executor.lease_ttl.get(), Duration::from_secs(5));
    assert_eq!(
        c.api.listen,
        "127.0.0.1:9100".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(c.node.partitions, 4);
    assert_eq!(c.api.max_module_bytes, 1_048_576);
}

#[test]
fn overrides_win_over_everything() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(
        &dir,
        "[api]\nlisten = \"127.0.0.1:9000\"\n[node]\ndata_dir = \"/toml\"\n",
    );
    let c = load_layered(
        ServerConfig::dev_defaults(),
        Some(&p),
        true,
        &env(&[
            ("OSTRO_API__LISTEN", "127.0.0.1:9100"),
            ("OSTRO_NODE__DATA_DIR", "/env"),
        ]),
        &Overrides {
            listen: Some("127.0.0.1:9200".parse().unwrap()),
            data_dir: Some(PathBuf::from("/cli")),
        },
    )
    .expect("load");
    assert_eq!(
        c.api.listen,
        "127.0.0.1:9200".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(c.node.data_dir, PathBuf::from("/cli"));
}

#[test]
fn missing_explicit_file_names_path() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent.toml");
    let err = load_layered(
        ServerConfig::dev_defaults(),
        Some(&missing),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("absent.toml"), "{err}");
}

#[test]
fn unknown_toml_key_names_key() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(&dir, "[executor]\nlease_tttl = \"5s\"\n");
    let err = load_layered(
        ServerConfig::dev_defaults(),
        Some(&p),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("lease_tttl"), "{err}");
}

#[test]
fn unknown_toml_section_names_key() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(&dir, "[nodes]\npartitions = 4\n");
    let err = load_layered(
        ServerConfig::dev_defaults(),
        Some(&p),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("nodes"), "{err}");
}

#[test]
fn unknown_env_key_in_known_section_is_rejected() {
    let err = load_layered(
        ServerConfig::dev_defaults(),
        None,
        false,
        &env(&[("OSTRO_EXECUTOR__LEASE_TTLL", "5s")]),
        &Overrides::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("lease_ttll"), "{err}");
}

#[test]
fn bad_duration_names_field() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(&dir, "[executor]\nlease_ttl = \"5 parsecs\"\n");
    let err = load_layered(
        ServerConfig::dev_defaults(),
        Some(&p),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("lease_ttl"), "{err}");
    assert!(err.contains("5 parsecs"), "{err}");
}

#[test]
fn bad_duration_from_env_names_field() {
    let err = load_layered(
        ServerConfig::dev_defaults(),
        None,
        false,
        &env(&[("OSTRO_ENGINE__HOT_WAIT", "soon")]),
        &Overrides::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("hot_wait"), "{err}");
}

#[test]
fn effective_config_round_trips_through_toml() {
    let mut c = ServerConfig::server_defaults();
    c.executor.heartbeat = Some(Duration::from_millis(250).into());
    let s = c.to_toml_string();
    assert!(s.contains("lease_ttl = \"30s\""), "{s}");
    assert!(s.contains("heartbeat = \"250ms\""), "{s}");
    assert!(s.contains("hot_wait = \"2s\""), "{s}");

    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(&dir, &s);
    let back = load_layered(
        ServerConfig::dev_defaults(),
        Some(&p),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .expect("reload printed config");
    assert_eq!(back, c);
}

#[test]
fn engine_section_maps_onto_engine_config() {
    let mut c = ServerConfig::default();
    c.engine.hot_wait = Duration::from_millis(500).into();
    c.engine.activation_timeout = Duration::from_secs(5).into();
    let e = c.engine.to_engine_config();
    assert_eq!(e.hot_wait, Duration::from_millis(500));
    assert_eq!(e.activation_timeout, Duration::from_secs(5));
    let d = ostro_engine::config::EngineConfig::default();
    assert_eq!(e.async_stack_size, d.async_stack_size);
}

// ---------------------------------------------------------------- [runs] (05-11, D9)

const HOUR: Duration = Duration::from_secs(3600);

#[test]
fn runs_retention_defaults_per_profile() {
    assert_eq!(
        ServerConfig::server_defaults().runs.retention.get(),
        168 * HOUR
    );
    assert_eq!(ServerConfig::dev_defaults().runs.retention.get(), 24 * HOUR);
    assert_eq!(ServerConfig::default().runs.max_retention, None);
}

#[test]
fn runs_retention_env_override() {
    let c = load_layered(
        ServerConfig::server_defaults(),
        None,
        false,
        &env(&[("OSTRO_RUNS__RETENTION", "48h")]),
        &Overrides::default(),
    )
    .expect("load");
    assert_eq!(c.runs.retention.get(), 48 * HOUR);
}

#[test]
fn runs_section_from_toml_and_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let f = write_toml(
        &dir,
        "[runs]\nretention = \"72h\"\nmax_retention = \"720h\"\n",
    );
    let c = load_layered(
        ServerConfig::dev_defaults(),
        Some(&f),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .expect("load");
    assert_eq!(c.runs.retention.get(), 72 * HOUR);
    assert_eq!(c.runs.max_retention.map(|d| d.get()), Some(720 * HOUR));
    // --print-config output loads back to the same config.
    let s = c.to_toml_string();
    assert!(s.contains("[runs]"), "{s}");
    let back = load_layered(
        ServerConfig::server_defaults(),
        Some(&write_toml(&dir, &s)),
        true,
        &no_env(),
        &Overrides::default(),
    )
    .expect("reload");
    assert_eq!(back.runs, c.runs);
}

#[test]
fn runs_unknown_key_and_zero_retention_are_rejected() {
    let err = load_layered(
        ServerConfig::server_defaults(),
        None,
        false,
        &env(&[("OSTRO_RUNS__RETENTON", "48h")]),
        &Overrides::default(),
    )
    .expect_err("unknown key");
    assert!(err.to_string().contains("retenton"), "{err}");
    let err = load_layered(
        ServerConfig::server_defaults(),
        None,
        false,
        &env(&[("OSTRO_RUNS__RETENTION", "0s")]),
        &Overrides::default(),
    )
    .expect_err("zero retention");
    assert!(err.to_string().contains("runs.retention"), "{err}");
    // Days are rejected by the friendly parser (W11): write hours.
    let err = load_layered(
        ServerConfig::server_defaults(),
        None,
        false,
        &env(&[("OSTRO_RUNS__RETENTION", "7d")]),
        &Overrides::default(),
    );
    assert!(err.is_err(), "7d is not an unsigned duration: {err:?}");
}

// ---------------------------------------------------------------- Phase 8 sections (08-04)

fn load_toml(body: &str) -> Result<ServerConfig, String> {
    load_toml_env(body, &no_env())
}

fn load_toml_env(body: &str, env: &EnvSource) -> Result<ServerConfig, String> {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(&dir, body);
    load_layered(
        ServerConfig::server_defaults(),
        Some(&p),
        true,
        env,
        &Overrides::default(),
    )
    .map_err(|e| e.to_string())
}

#[test]
fn phase8_defaults_match_p8_d22() {
    use ostro_server::config::OtlpProtocol;
    let c = ServerConfig::server_defaults();
    // [telemetry]
    assert_eq!(c.telemetry.log_format, None);
    assert_eq!(c.telemetry.otlp_endpoint, None);
    assert_eq!(c.telemetry.otlp_protocol, OtlpProtocol::Grpc);
    assert!(c.telemetry.otlp_headers.is_empty());
    assert!(c.telemetry.traces && c.telemetry.logs && c.telemetry.metrics);
    assert_eq!(c.telemetry.sample_ratio, 1.0);
    assert_eq!(c.telemetry.service_name, "ostro");
    // [snapshots] -> SnapshotConfig
    let s = c.snapshots.to_snapshot_config();
    assert_eq!(s, ostro_partition::leader::snapshot::SnapshotConfig::default());
    assert!(s.enabled);
    assert_eq!(s.interval, HOUR);
    assert_eq!(s.min_interval, Duration::from_secs(60));
    assert_eq!(s.bytes_threshold, 268_435_456);
    assert_eq!(s.keep, 2);
    // [backup] -> BackupConfig (off)
    let b = c.backup.to_backup_config();
    assert!(!c.backup.enabled() && !b.enabled());
    assert_eq!(b, ostro_backup::config::BackupConfig::default());
    // [blob_gc]
    assert!(c.blob_gc.enabled);
    assert_eq!(c.blob_gc.interval.get(), HOUR);
    assert_eq!(c.blob_gc.grace.get(), HOUR);
    // [triggers] -> CronConfig
    assert_eq!(c.triggers.webhook_body_limit, 1_048_576);
    assert_eq!(c.triggers.webhook_tolerance.get(), Duration::from_secs(300));
    assert_eq!(c.triggers.to_cron_config().catchup_cap, 100);
    // [guest_logs] (P8-D16)
    let g = &c.guest_logs;
    assert_eq!((g.rate_per_sec, g.burst), (50, 500));
    assert_eq!((g.run_lines_cap, g.run_bytes_cap), (10_000, 10 * 1024 * 1024));
    assert_eq!(g.ring_lines, 1_000);
    assert_eq!(g.node_rate_per_sec, 5_000);
}

#[test]
fn dev_profile_disables_backup_and_keeps_snapshot_defaults() {
    let d = ServerConfig::dev_defaults();
    assert!(!d.backup.enabled());
    assert_eq!(d.snapshots, ServerConfig::server_defaults().snapshots);
}

#[test]
fn phase8_sections_parse_from_toml() {
    use ostro_backup::config::{TlsRoots, UrlStyle};
    let c = load_toml(
        r#"
[telemetry]
log_format = "json"
otlp_endpoint = "http://127.0.0.1:4317"
otlp_protocol = "grpc"
otlp_headers = { "x-team" = "core" }
traces = true
logs = false
sample_ratio = 0.25
service_name = "ostro-test"
metrics = false

[snapshots]
enabled = false
interval = "30m"
min_interval = "10s"
bytes = 1024
keep = 3

[backup]
url = "s3://bucket/prefix"
seal_interval = "2s"
region = "eu-west-1"
endpoint = "http://127.0.0.1:9000"
url_style = "virtual"
access_key_id = "AKIA-INLINE"
secret_access_key = "inline-secret"
allow_http = true
tls_roots = "webpki"
remote_snapshots_keep = 5
takeover = true

[blob_gc]
enabled = false
interval = "2h"
grace = "90m"

[triggers]
webhook_body_limit = 2048
webhook_tolerance = "60s"
cron_catchup_cap = 7

[guest_logs]
rate_per_sec = 10
burst = 20
run_lines_cap = 30
run_bytes_cap = 40
ring_lines = 50
ring_budget_bytes = 60
node_rate_per_sec = 70
"#,
    )
    .expect("load");
    assert_eq!(c.effective_log_format(), LogFormat::Json);
    assert_eq!(c.effective_log_section().format, LogFormat::Json);
    assert_eq!(
        c.telemetry.otlp_endpoint.as_deref(),
        Some("http://127.0.0.1:4317")
    );
    assert_eq!(c.telemetry.otlp_headers["x-team"].expose(), "core");
    assert!(!c.telemetry.logs && !c.telemetry.metrics);
    assert_eq!(c.telemetry.sample_ratio, 0.25);
    assert_eq!(c.telemetry.service_name, "ostro-test");

    let s = c.snapshots.to_snapshot_config();
    assert!(!s.enabled);
    assert_eq!(s.interval, Duration::from_secs(1800));
    assert_eq!(s.min_interval, Duration::from_secs(10));
    assert_eq!(s.bytes_threshold, 1024, "the `bytes` key maps to bytes_threshold");
    assert_eq!(s.keep, 3);

    let b = c.backup.to_backup_config();
    assert!(b.enabled());
    assert_eq!(b.url.as_deref(), Some("s3://bucket/prefix"));
    assert_eq!(b.seal_interval, Duration::from_secs(2));
    assert_eq!(b.region.as_deref(), Some("eu-west-1"));
    assert_eq!(b.url_style, UrlStyle::Virtual);
    assert_eq!(b.access_key_id.as_deref(), Some("AKIA-INLINE"));
    assert_eq!(b.secret_access_key.as_deref(), Some("inline-secret"));
    assert_eq!(b.session_token, None);
    assert!(b.allow_http && b.takeover);
    assert_eq!(b.tls_roots, TlsRoots::Webpki);
    assert_eq!(b.remote_snapshots_keep, 5);

    assert!(!c.blob_gc.enabled);
    assert_eq!(c.blob_gc.interval.get(), 2 * HOUR);
    assert_eq!(c.blob_gc.grace.get(), Duration::from_secs(5400));
    assert_eq!(c.triggers.webhook_body_limit, 2048);
    assert_eq!(c.triggers.webhook_tolerance.get(), Duration::from_secs(60));
    assert_eq!(c.triggers.to_cron_config().catchup_cap, 7);
    let g = &c.guest_logs;
    assert_eq!(
        (
            g.rate_per_sec,
            g.burst,
            g.run_lines_cap,
            g.run_bytes_cap,
            g.ring_lines,
            g.ring_budget_bytes,
            g.node_rate_per_sec
        ),
        (10, 20, 30, 40, 50, 60, 70)
    );
}

#[test]
fn phase8_unknown_keys_are_rejected_and_named() {
    for (section, key) in [
        ("telemetry", "otlp_endpiont"),
        ("snapshots", "intervall"),
        ("backup", "secret_key"),
        ("blob_gc", "graze"),
        ("triggers", "cron_cap"),
        ("guest_logs", "rate"),
    ] {
        let err = load_toml(&format!("[{section}]\n{key} = 1\n")).unwrap_err();
        assert!(err.contains(key), "[{section}] {key}: {err}");
    }
}

#[test]
fn phase8_unknown_env_key_is_rejected() {
    let err = load_layered(
        ServerConfig::server_defaults(),
        None,
        false,
        &env(&[("OSTRO_BLOB_GC__GRAZE", "1h")]),
        &Overrides::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("graze"), "{err}");
    let c = load_layered(
        ServerConfig::server_defaults(),
        None,
        false,
        &env(&[
            ("OSTRO_SNAPSHOTS__INTERVAL", "2h"),
            ("OSTRO_TELEMETRY__SAMPLE_RATIO", "0.5"),
        ]),
        &Overrides::default(),
    )
    .expect("env");
    assert_eq!(c.snapshots.interval.get(), 2 * HOUR);
    assert_eq!(c.telemetry.sample_ratio, 0.5);
}

#[test]
fn phase8_bad_durations_name_the_key() {
    for (section, key) in [
        ("snapshots", "interval"),
        ("snapshots", "min_interval"),
        ("backup", "seal_interval"),
        ("blob_gc", "grace"),
        ("triggers", "webhook_tolerance"),
    ] {
        let err = load_toml(&format!("[{section}]\n{key} = \"1d\"\n")).unwrap_err();
        assert!(err.contains(key), "{section}.{key}: {err}");
        assert!(err.contains("1d"), "{section}.{key}: {err}");
        assert!(err.contains("24h"), "the hint names the hour form: {err}");
    }
}

#[test]
fn phase8_value_validation() {
    let err = load_toml("[telemetry]\nsample_ratio = 1.5\n").unwrap_err();
    assert!(err.contains("telemetry.sample_ratio"), "{err}");
    let err = load_toml("[telemetry]\notlp_protocol = \"http\"\n").unwrap_err();
    assert!(err.contains("otlp_protocol") || err.contains("http"), "{err}");
    let err = load_toml("[telemetry]\notlp_endpoint = \"127.0.0.1:4317\"\n").unwrap_err();
    assert!(err.contains("telemetry.otlp_endpoint"), "{err}");
    let err = load_toml("[snapshots]\nkeep = 0\n").unwrap_err();
    assert!(err.contains("snapshots.keep"), "{err}");
    let err = load_toml("[log]\nformat = \"xml\"\n").unwrap_err();
    assert!(err.contains("xml"), "{err}");
    let c = load_toml("[log]\nformat = \"json\"\n").expect("json log format");
    assert_eq!(c.log.format, LogFormat::Json);
    assert_eq!(c.effective_log_format(), LogFormat::Json);
}

#[test]
fn backup_secrets_resolve_env_refs_and_never_print() {
    let c = load_toml_env(
        r#"
[backup]
url = "s3://b/p"
access_key_id = "env:CANARY_AKID"
secret_access_key = "env:CANARY_SECRET"
session_token = "inline-canary-token"

[telemetry]
otlp_headers = { authorization = "env:CANARY_OTLP" }
"#,
        &env(&[
            ("CANARY_AKID", "AKIACANARY"),
            ("CANARY_SECRET", "canary-secret-value"),
            ("CANARY_OTLP", "Bearer canary-otlp"),
        ]),
    )
    .expect("load");
    let b = c.backup.to_backup_config();
    assert_eq!(b.access_key_id.as_deref(), Some("AKIACANARY"));
    assert_eq!(b.secret_access_key.as_deref(), Some("canary-secret-value"));
    assert_eq!(b.session_token.as_deref(), Some("inline-canary-token"));
    assert_eq!(
        c.telemetry.otlp_headers["authorization"].expose(),
        "Bearer canary-otlp"
    );
    let printed = format!("{c:?}\n{}", c.to_toml_string());
    for secret in [
        "AKIACANARY",
        "canary-secret-value",
        "inline-canary-token",
        "canary-otlp",
    ] {
        assert!(!printed.contains(secret), "{secret} leaked:\n{printed}");
    }
    assert!(printed.contains("<redacted>"), "{printed}");

    let err = load_toml_env(
        "[backup]\nsecret_access_key = \"env:CANARY_MISSING\"\n",
        &no_env(),
    )
    .unwrap_err();
    assert!(err.contains("backup.secret_access_key"), "{err}");
    assert!(err.contains("CANARY_MISSING"), "{err}");
}

#[test]
fn phase8_sections_round_trip_through_toml() {
    let mut c = ServerConfig::server_defaults();
    c.telemetry.otlp_endpoint = Some("http://collector:4317".into());
    c.telemetry.log_format = Some(LogFormat::Json);
    c.telemetry.sample_ratio = 0.5;
    c.snapshots.interval = Duration::from_secs(7200).into();
    c.backup.url = Some("file:///backups".into());
    c.backup.remote_snapshots_keep = 4;
    c.blob_gc.grace = Duration::from_secs(600).into();
    c.triggers.cron_catchup_cap = 9;
    c.guest_logs.burst = 77;
    let s = c.to_toml_string();
    for section in [
        "[telemetry]",
        "[snapshots]",
        "[backup]",
        "[blob_gc]",
        "[triggers]",
        "[guest_logs]",
    ] {
        assert!(s.contains(section), "{section} missing:\n{s}");
    }
    let back = load_toml(&s).expect("reload printed config");
    assert_eq!(back, c);
}
