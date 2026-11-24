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
    assert_eq!(
        s,
        ostro_partition::leader::snapshot::SnapshotConfig::default()
    );
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
    assert_eq!(
        (g.run_lines_cap, g.run_bytes_cap),
        (10_000, 10 * 1024 * 1024)
    );
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
    assert_eq!(
        s.bytes_threshold, 1024,
        "the `bytes` key maps to bytes_threshold"
    );
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
    assert!(
        err.contains("otlp_protocol") || err.contains("http"),
        "{err}"
    );
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

// ---------------------------------------------------------------- Phase 6 sections (06-14)

#[test]
fn phase6_defaults_per_profile() {
    for (name, c) in [
        ("server", ServerConfig::server_defaults()),
        ("dev", ServerConfig::dev_defaults()),
    ] {
        let s = c.secrets.to_bridge().expect("secrets");
        assert_eq!(
            s.providers.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
            ["sealed", "env", "file"],
            "{name}"
        );
        assert_eq!(s.env_prefix, "OSTRO_SECRET_");
        assert!(s.sealing_key_file.is_none() && s.vault.is_none() && s.k8s.is_none());
        let e = c.egress.to_bridge().expect("egress");
        assert!(e.allow_private.is_empty(), "{name}: deny-by-default");
        assert_eq!(e.connect_timeout, Duration::from_secs(5));
        assert_eq!(e.default_timeout, Duration::from_secs(30));
        assert_eq!(e.max_response_bytes, 4 * 1024 * 1024);
        assert_eq!(e.hard_max_response_bytes, 64 * 1024 * 1024);
        assert_eq!(e.max_redirects, 5);
        assert_eq!(e.rate_max_wait, Duration::from_secs(30));
        assert_eq!(e.idempotency_window, Duration::from_secs(24 * 3600));
        assert_eq!(e.safety_margin, Duration::from_secs(300));
        assert_eq!(e, ostro_bridge::config::EgressConfig::default());
        let k = c.connectors.to_bridge().expect("connectors");
        assert_eq!(
            k,
            ostro_bridge::config::ConnectorsConfig::default(),
            "{name}"
        );
        // public_base_url defaults to the API listen address (unspecified -> loopback).
        assert_eq!(
            c.public_base_url(c.api.listen),
            "http://127.0.0.1:7700",
            "{name}"
        );
    }
    assert!(ServerConfig::dev_defaults().secrets.auto_sealing_key);
    assert!(!ServerConfig::server_defaults().secrets.auto_sealing_key);
    let bound: SocketAddr = "127.0.0.1:41234".parse().unwrap();
    assert_eq!(
        ServerConfig::server_defaults().public_base_url(bound),
        "http://127.0.0.1:41234"
    );
}

#[test]
fn phase6_full_example_parses() {
    let c = load_toml(
        r#"
[secrets]
providers = ["vault", "sealed", "env", "file", "k8s"]
sealing_key_file = "/etc/ostro/sealing.key"
env_prefix = "MY_SECRET_"
file_dir = "/run/secrets"
auto_sealing_key = true

[secrets.vault]
addr = "https://vault.example.com:8200"
mount = "kv"
auth = "approle"
role_id = "r-1"
secret_id_file = "/run/vault/secret-id"
cache_ttl = "1m"

[secrets.k8s]
namespace = "ostro"
cache_ttl = "10s"

[egress]
allow_private = ["10.0.0.0/8", "127.0.0.0/8"]
connect_timeout = "2s"
default_timeout = "1m"
max_response_bytes = "8MiB"
hard_max_response_bytes = 33554432
max_redirects = 2
rate_max_wait = "5s"
idempotency_window = "48h"
safety_margin = "10m"

[connectors]
sql_max_rows = 500
sql_max_bytes = "1MiB"
sql_effects_retention = "72h"
object_max_get_bytes = "2MiB"
presign_default_expiry = "5m"
backend_ttl = "1m"
default_rate = "50/s"
public_base_url = "https://ostro.example.com/"
"#,
    )
    .expect("full example");
    let s = c.secrets.to_bridge().expect("secrets");
    assert_eq!(
        s.providers.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
        ["vault", "sealed", "env", "file", "k8s"]
    );
    assert_eq!(s.env_prefix, "MY_SECRET_");
    assert_eq!(
        s.file_dir.as_deref(),
        Some(std::path::Path::new("/run/secrets"))
    );
    let v = s.vault.expect("vault");
    assert_eq!(v.addr, "https://vault.example.com:8200");
    assert_eq!(v.mount, "kv");
    assert_eq!(v.cache_ttl, Duration::from_secs(60));
    assert_eq!(s.k8s.expect("k8s").namespace.as_deref(), Some("ostro"));
    assert!(c.secrets.auto_sealing_key);
    let e = c.egress.to_bridge().expect("egress");
    assert_eq!(e.allow_private.len(), 2);
    assert_eq!(e.connect_timeout, Duration::from_secs(2));
    assert_eq!(e.default_timeout, Duration::from_secs(60));
    assert_eq!(e.max_response_bytes, 8 * 1024 * 1024);
    assert_eq!(e.hard_max_response_bytes, 32 * 1024 * 1024);
    assert_eq!(e.max_redirects, 2);
    assert_eq!(e.rate_max_wait, Duration::from_secs(5));
    assert_eq!(e.idempotency_window, Duration::from_secs(48 * 3600));
    let k = c.connectors.to_bridge().expect("connectors");
    assert_eq!(k.sql_max_rows, 500);
    assert_eq!(k.sql_max_bytes, 1024 * 1024);
    assert_eq!(k.sql_effects_retention, Duration::from_secs(72 * 3600));
    assert_eq!(k.presign_default_expiry, Duration::from_secs(300));
    assert_eq!(k.default_rate.expect("rate").per_second, 50);
    assert_eq!(
        c.public_base_url("0.0.0.0:7700".parse().unwrap()),
        "https://ostro.example.com"
    );

    // The printed config keeps the new sections and reloads to the same value.
    let printed = c.to_toml_string();
    for section in ["[secrets]", "[egress]", "[connectors]"] {
        assert!(printed.contains(section), "{section} missing:\n{printed}");
    }
    assert_eq!(load_toml(&printed).expect("reload printed config"), c);
}

#[test]
fn phase6_unknown_keys_are_ignored() {
    let c = load_toml(
        r#"
[secrets]
future_key = 1
[secrets.vault]
addr = "http://127.0.0.1:8200"
future_vault_key = "x"
[egress]
not_yet = true
[connectors]
someday = "maybe"
"#,
    )
    .expect("unknown keys under the Phase 6 sections are ignored (D18)");
    assert_eq!(c.egress, ServerConfig::server_defaults().egress);
    c.secrets.to_bridge().expect("secrets still convert");
    let c = load_toml_env(
        "",
        &env(&[
            ("OSTRO_EGRESS__FUTURE_KNOB", "1"),
            ("OSTRO_CONNECTORS__SQL_MAX_ROWS", "7"),
        ]),
    )
    .expect("env");
    assert_eq!(c.connectors.sql_max_rows, 7);
}

#[test]
fn phase6_bad_values_name_the_key() {
    for (section, key) in [
        ("egress", "connect_timeout"),
        ("egress", "rate_max_wait"),
        ("connectors", "sql_effects_retention"),
        ("connectors", "backend_ttl"),
    ] {
        let err = load_toml(&format!("[{section}]\n{key} = \"5 parsecs\"\n")).unwrap_err();
        assert!(err.contains(key), "{section}.{key}: {err}");
        assert!(err.contains("5 parsecs"), "{section}.{key}: {err}");
    }
    let err = load_toml("[egress]\nmax_response_bytes = \"4XB\"\n").unwrap_err();
    assert!(err.contains("max_response_bytes"), "{err}");
    let err = load_toml("[egress]\nallow_private = [\"not-a-cidr\"]\n").unwrap_err();
    assert!(err.contains("egress"), "{err}");
    let err = load_toml("[connectors]\ndefault_rate = \"10/h\"\n").unwrap_err();
    assert!(err.contains("connectors"), "{err}");
    let err = load_toml("[connectors]\npublic_base_url = \"ftp://x\"\n").unwrap_err();
    assert!(err.contains("public_base_url"), "{err}");
    let err = load_toml("[secrets]\nproviders = [\"sealed\", \"bogus\"]\n").unwrap_err();
    assert!(err.contains("secrets"), "{err}");
}

// ---------------------------------------------------------------- Phase 7 sections (07-02)

#[test]
fn phase7_defaults_per_profile() {
    use ostro_server::config::{AcmeChallenge, ApiTlsMode, PlacementSetting};
    for (name, c) in [
        ("dev", ServerConfig::dev_defaults()),
        ("server", ServerConfig::server_defaults()),
    ] {
        let w = &c.workers;
        assert!(w.enabled, "{name}");
        assert_eq!(w.max_frame_bytes, 1024 * 1024, "{name}");
        assert_eq!(w.max_append_bytes, 64 * 1024 * 1024, "{name}");
        assert_eq!(w.heartbeat.get(), Duration::from_secs(10), "{name}");
        assert_eq!(w.keepalive.get(), Duration::from_secs(5), "{name}");
        assert!(w.require_same_version, "{name}");
        assert_eq!(w.client_cert_ttl.get(), Duration::from_secs(24 * 3600));
        assert_eq!(w.join_token_ttl.get(), Duration::from_secs(24 * 3600));
        assert_eq!(w.default_placement, PlacementSetting::Embedded);
        assert_eq!(
            (w.spill.cpu_pct, w.spill.slots_pct, w.spill.mem_pct),
            (70, 90, 85)
        );
        assert_eq!(w.spill.lag.get(), Duration::from_secs(5));
        assert_eq!(w.spill.sustain.get(), Duration::from_secs(10));
        assert_eq!(w.spill.cooldown.get(), Duration::from_secs(30));
        assert_eq!(w.spill.hysteresis_pct, 85);
        // W4: min(heartbeat, lease_ttl / 3).
        assert_eq!(
            w.effective_heartbeat(c.executor.lease_ttl.get()),
            Duration::from_secs(10)
        );
        assert_eq!(
            w.effective_heartbeat(Duration::from_secs(15)),
            Duration::from_secs(5)
        );

        let t = &c.api.tls;
        assert_eq!(t.mode, ApiTlsMode::Auto, "{name}: D-W8");
        assert_eq!((t.cert_file.as_ref(), t.key_file.as_ref()), (None, None));
        assert!(!t.allow_self_signed);
        assert_eq!(t.reload_poll.get(), Duration::from_secs(30));
        assert!(t.acme.domains.is_empty());
        assert_eq!(
            t.acme.directory,
            "https://acme-v02.api.letsencrypt.org/directory"
        );
        assert_eq!(t.acme.cache_dir, None, "unset = <data_dir>/tls/acme");
        assert_eq!(t.acme.challenge, AcmeChallenge::TlsAlpn01);
        assert_eq!(t.acme.http01_listen, "0.0.0.0:80".parse().unwrap());

        assert!(c.cluster.advertise.is_empty());
        assert_eq!(c.cluster.tls.client_ca, None);

        assert_eq!(c.flow.max_tracked_keys, 100_000);
        assert_eq!(c.flow.idle_key_evict.get(), Duration::from_secs(600));
        assert_eq!(c.flow.default_queue_weight, 1);
    }
    // D-W16: an ephemeral loopback cluster port in dev, 7701 on every interface in server.
    assert_eq!(
        ServerConfig::dev_defaults().cluster.listen,
        "127.0.0.1:0".parse::<SocketAddr>().unwrap()
    );
    assert_eq!(
        ServerConfig::server_defaults().cluster.listen,
        "0.0.0.0:7701".parse::<SocketAddr>().unwrap()
    );
}

#[test]
fn phase7_sections_parse_from_toml() {
    use ostro_server::config::{AcmeChallenge, ApiTlsMode, PlacementSetting};
    let c = load_toml(
        r#"
[api.tls]
mode = "acme"
allow_self_signed = true
reload_poll = "10s"

[api.tls.acme]
domains = ["ostro.example.com"]
email = "ops@example.com"
directory = "https://localhost:14000/dir"
directory_ca_file = "/etc/pebble/ca.pem"
cache_dir = "/var/lib/ostro/acme"
challenge = "http-01"
http01_listen = "0.0.0.0:8080"

[workers]
enabled = false
max_frame_bytes = 65536
max_append_bytes = 1048576
heartbeat = "2s"
default_placement = "auto"

[workers.spill]
cpu_pct = 60
cooldown = "1m"

[cluster]
listen = "10.0.0.5:7801"
advertise = ["ostro-0.ostro.svc", "10.0.0.5"]

[cluster.tls]
client_ca = "/etc/ostro/worker-ca.pem"

[flow]
max_tracked_keys = 10
idle_key_evict = "30s"
default_queue_weight = 3
"#,
    )
    .expect("parse");
    assert_eq!(c.api.tls.mode, ApiTlsMode::Acme);
    assert!(c.api.tls.allow_self_signed);
    assert_eq!(c.api.tls.reload_poll.get(), Duration::from_secs(10));
    assert_eq!(c.api.tls.acme.domains, ["ostro.example.com"]);
    assert_eq!(c.api.tls.acme.email.as_deref(), Some("ops@example.com"));
    assert_eq!(c.api.tls.acme.challenge, AcmeChallenge::Http01);
    assert_eq!(
        c.api.tls.acme.http01_listen,
        "0.0.0.0:8080".parse().unwrap()
    );
    assert_eq!(
        c.api.tls.acme.cache_dir.as_deref(),
        Some(std::path::Path::new("/var/lib/ostro/acme"))
    );
    assert!(!c.workers.enabled);
    assert_eq!(c.workers.max_frame_bytes, 65536);
    assert_eq!(c.workers.heartbeat.get(), Duration::from_secs(2));
    assert_eq!(c.workers.default_placement, PlacementSetting::Auto);
    assert_eq!(c.workers.spill.cpu_pct, 60);
    assert_eq!(c.workers.spill.cooldown.get(), Duration::from_secs(60));
    assert_eq!(
        c.workers.spill.slots_pct, 90,
        "untouched keys keep defaults"
    );
    assert_eq!(c.cluster.listen, "10.0.0.5:7801".parse().unwrap());
    assert_eq!(c.cluster.advertise, ["ostro-0.ostro.svc", "10.0.0.5"]);
    assert_eq!(
        c.cluster.tls.client_ca.as_deref(),
        Some(std::path::Path::new("/etc/ostro/worker-ca.pem"))
    );
    assert_eq!(c.flow.max_tracked_keys, 10);
    assert_eq!(c.flow.idle_key_evict.get(), Duration::from_secs(30));
    assert_eq!(c.flow.default_queue_weight, 3);
}

#[test]
fn phase7_env_overrides() {
    use ostro_server::config::ApiTlsMode;
    let c = load_toml_env(
        "",
        &env(&[
            ("OSTRO_WORKERS__MAX_FRAME_BYTES", "131072"),
            ("OSTRO_WORKERS__ENABLED", "false"),
            ("OSTRO_CLUSTER__LISTEN", "127.0.0.1:7999"),
            ("OSTRO_FLOW__DEFAULT_QUEUE_WEIGHT", "4"),
            ("OSTRO_API__TLS__MODE", "self-signed"),
        ]),
    )
    .expect("env");
    assert_eq!(c.workers.max_frame_bytes, 131072);
    assert!(!c.workers.enabled);
    assert_eq!(c.cluster.listen, "127.0.0.1:7999".parse().unwrap());
    assert_eq!(c.flow.default_queue_weight, 4);
    assert_eq!(c.api.tls.mode, ApiTlsMode::SelfSigned);
}

#[test]
fn phase7_invalid_values_are_rejected() {
    for (body, needle) in [
        ("[api.tls]\nmode = \"tls13\"\n", "tls13"),
        ("[api.tls]\nmodez = \"off\"\n", "modez"),
        ("[api.tls.acme]\nchallenge = \"dns-01\"\n", "dns-01"),
        ("[workers]\ndefault_placement = \"gpu\"\n", "gpu"),
        ("[workers]\nmax_frame_bytes = 0\n", "max_frame_bytes"),
        ("[workers]\nmax_frame_bytes = 8388608\n", "max_frame_bytes"),
        (
            "[workers]\nmax_frame_bytes = 1048576\nmax_append_bytes = 1024\n",
            "max_append_bytes",
        ),
        ("[workers]\nheartbeat = \"0s\"\n", "workers.heartbeat"),
        ("[workers.spill]\ncpu_pct = 0\n", "cpu_pct"),
        ("[workers.spill]\nhysteresis_pct = 101\n", "hysteresis_pct"),
        ("[workers.spill]\nbogus = 1\n", "bogus"),
        ("[cluster]\nlisten = \"not an addr\"\n", "listen"),
        ("[cluster.tls]\nca = \"x\"\n", "ca"),
        ("[flow]\ndefault_queue_weight = 0\n", "default_queue_weight"),
        ("[flow]\nunknown = 1\n", "unknown"),
        ("[api.tls]\nmode = \"files\"\n", "cert_file"),
        ("[api.tls]\nmode = \"acme\"\n", "domains"),
    ] {
        let err = load_toml(body).expect_err(body);
        assert!(err.contains(needle), "{body:?}: {err}");
    }
    let err = load_toml_env("", &env(&[("OSTRO_API__TLS__MODE", "bogus")])).unwrap_err();
    assert!(err.contains("bogus"), "{err}");
}

#[test]
fn phase7_sections_round_trip_through_toml_and_print() {
    let mut c = ServerConfig::server_defaults();
    c.api.tls.acme.domains = vec!["a.example".into(), "b.example".into()];
    c.api.tls.acme.email = Some("ops@example.com".into());
    c.api.tls.cert_file = Some("/etc/ostro/tls.crt".into());
    c.cluster.advertise = vec!["ostro-0".into()];
    c.cluster.tls.client_ca = Some("/etc/ostro/ca.pem".into());
    c.workers.spill.lag = Duration::from_millis(1500).into();
    c.flow.idle_key_evict = Duration::from_secs(90).into();
    let s = c.to_toml_string();
    for needle in ["[workers]", "[cluster]", "[flow]", "tls = {", "spill = {"] {
        assert!(s.contains(needle), "{needle} missing:\n{s}");
    }
    assert!(s.contains("\"a.example\""), "{s}");
    assert!(s.contains("\"challenge\" = \"tls-alpn-01\""), "{s}");
    assert!(s.contains("\"mode\" = \"auto\""), "{s}");
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

// --------------------------------------------------------------- Phase 12 sections (12-02)

fn load_profile(base: ServerConfig, body: &str) -> Result<ServerConfig, String> {
    let dir = tempfile::tempdir().unwrap();
    let p = write_toml(&dir, body);
    load_layered(base, Some(&p), true, &no_env(), &Overrides::default()).map_err(|e| e.to_string())
}

fn both_profiles() -> [ServerConfig; 2] {
    [
        ServerConfig::dev_defaults(),
        ServerConfig::server_defaults(),
    ]
}

const STATIC_CLUSTER: &str = r#"
[node]
node_id = 1

[cluster]
cluster_id = "prod-eu"
replication_listen = "0.0.0.0:7702"

[[cluster.peers]]
id = 2
addr = "ostro-1:7701"
repl_addr = "ostro-1:7702"

[[cluster.peers]]
id = 3
addr = "ostro-2:7701"

[cluster.tls]
ca_cert_file = "/etc/ostro/ca.crt"
ca_key_file = "/etc/ostro/ca.key"

[replication]
session_timeout = "2s"
meta_election_tick = 6
rf = 3
min_insync = 2
"#;

#[test]
fn phase12_defaults_match_d_p12_10() {
    for c in both_profiles() {
        let r = &c.replication;
        assert_eq!(r.session_timeout.get(), Duration::from_secs(3));
        assert_eq!(r.replica_lag_max.get(), Duration::from_secs(10));
        assert_eq!(r.meta_tick_interval.get(), Duration::from_millis(100));
        assert_eq!(r.meta_election_tick, 10);
        assert_eq!(r.meta_heartbeat_tick, 2);
        assert_eq!(r.fetch_max_wait.get(), Duration::from_millis(500));
        assert_eq!(r.fetch_read_timeout.get(), Duration::from_secs(5));
        assert_eq!(r.fetch_max_bytes.get(), 8 * 1024 * 1024);
        assert_eq!((r.rf, r.min_insync), (None, None));
        assert!(c.cluster.peers.is_empty());
        assert_eq!(c.cluster.cluster_id, None);
        assert_eq!(c.cluster.replication_listen, None);
        assert_eq!(c.cluster.tls.ca_cert_file, None);
        assert_eq!(c.cluster.tls.ca_key_file, None);
        c.validate_phase12().expect("defaults are valid");
    }
}

#[test]
fn phase12_static_cluster_parses_in_both_profiles() {
    for base in both_profiles() {
        let c = load_profile(base, STATIC_CLUSTER).expect("static cluster config");
        assert_eq!(c.cluster.cluster_id.as_deref(), Some("prod-eu"));
        assert_eq!(
            c.cluster.replication_listen,
            Some("0.0.0.0:7702".parse::<SocketAddr>().unwrap())
        );
        assert_eq!(c.cluster.peers.len(), 2);
        assert_eq!(c.cluster.peers[0].id, 2);
        assert_eq!(c.cluster.peers[0].addr, "ostro-1:7701");
        assert_eq!(
            c.cluster.peers[0].repl_addr.as_deref(),
            Some("ostro-1:7702")
        );
        assert_eq!(c.cluster.peers[1].repl_addr, None);
        assert_eq!(
            c.cluster.tls.ca_cert_file,
            Some(PathBuf::from("/etc/ostro/ca.crt"))
        );
        assert_eq!(c.replication.session_timeout.get(), Duration::from_secs(2));
        assert_eq!(c.replication.meta_election_tick, 6);
        assert_eq!(
            (c.replication.rf, c.replication.min_insync),
            (Some(3), Some(2))
        );
        // Untouched keys keep their defaults.
        assert_eq!(c.replication.replica_lag_max.get(), Duration::from_secs(10));
    }
}

#[test]
fn phase12_peers_require_cluster_id_and_ca_files() {
    let peers = "[[cluster.peers]]\nid = 2\naddr = \"n2:7701\"\n";
    let ca = "[cluster.tls]\nca_cert_file = \"ca.crt\"\nca_key_file = \"ca.key\"\n";
    for base in both_profiles() {
        let err = load_profile(
            base.clone(),
            &format!("[cluster]\ncluster_id = \"c\"\n{peers}"),
        )
        .unwrap_err();
        assert!(err.contains("cluster.tls.ca_cert_file"), "{err}");
        assert!(err.contains("cluster.tls.ca_key_file"), "{err}");
        let err = load_profile(base.clone(), &format!("{peers}{ca}")).unwrap_err();
        assert!(err.contains("cluster.cluster_id"), "{err}");
        let err = load_profile(
            base.clone(),
            &format!(
                "[cluster]\ncluster_id = \"c\"\n{peers}[cluster.tls]\nca_cert_file = \"ca.crt\"\n"
            ),
        )
        .unwrap_err();
        assert!(err.contains("ca_key_file"), "{err}");
        assert!(!err.contains("ca_cert_file"), "{err}");
        load_profile(base, &format!("[cluster]\ncluster_id = \"c\"\n{peers}{ca}"))
            .expect("complete static cluster");
    }
}

#[test]
fn phase12_peer_ids_are_unique_nonzero_and_not_self() {
    let head = "[node]\nnode_id = 1\n[cluster]\ncluster_id = \"c\"\n[cluster.tls]\nca_cert_file = \"a\"\nca_key_file = \"b\"\n";
    for base in both_profiles() {
        for (peers, needle) in [
            ("[[cluster.peers]]\nid = 0\naddr = \"x:1\"\n", "id 0"),
            (
                "[[cluster.peers]]\nid = 1\naddr = \"x:1\"\n",
                "own node.node_id 1",
            ),
            (
                "[[cluster.peers]]\nid = 2\naddr = \"x:1\"\n[[cluster.peers]]\nid = 2\naddr = \"y:1\"\n",
                "duplicate id 2",
            ),
            ("[[cluster.peers]]\nid = 2\naddr = \"\"\n", "non-empty addr"),
            (
                "[[cluster.peers]]\nid = 2\naddr = \"x:1\"\nbogus = 1\n",
                "bogus",
            ),
        ] {
            let err = load_profile(base.clone(), &format!("{head}{peers}")).unwrap_err();
            assert!(err.contains(needle), "{peers:?}: {err}");
        }
    }
}

#[test]
fn phase12_replication_values_are_validated() {
    for base in both_profiles() {
        for (body, needle) in [
            (
                "[replication]\nsession_timeout = \"0s\"\n",
                "replication.session_timeout",
            ),
            (
                "[replication]\nfetch_max_wait = \"0s\"\n",
                "replication.fetch_max_wait",
            ),
            (
                "[replication]\nmeta_heartbeat_tick = 10\n",
                "meta_heartbeat_tick",
            ),
            (
                "[replication]\nmeta_heartbeat_tick = 0\n",
                "meta_heartbeat_tick",
            ),
            ("[replication]\nfetch_max_bytes = 0\n", "fetch_max_bytes"),
            ("[replication]\nrf = 0\n", "replication.rf"),
            ("[replication]\nmin_insync = 0\n", "replication.min_insync"),
            ("[replication]\nrf = 1\nmin_insync = 2\n", "must not exceed"),
            ("[replication]\nbogus = 1\n", "bogus"),
            ("[cluster]\ncluster_id = \" \"\n", "cluster.cluster_id"),
        ] {
            let err = load_profile(base.clone(), body).expect_err(body);
            assert!(err.contains(needle), "{body:?}: {err}");
        }
    }
    let c = load_toml_env(
        "",
        &env(&[
            ("OSTRO_REPLICATION__RF", "3"),
            ("OSTRO_REPLICATION__FETCH_MAX_BYTES", "4MiB"),
        ]),
    )
    .expect("env overrides");
    assert_eq!(c.replication.rf, Some(3));
    assert_eq!(c.replication.fetch_max_bytes.get(), 4 * 1024 * 1024);
}

#[test]
fn phase12_sections_round_trip_through_toml_and_print() {
    for base in both_profiles() {
        let c = load_profile(base.clone(), STATIC_CLUSTER).unwrap();
        let s = c.to_toml_string();
        assert!(s.contains("[replication]"), "{s}");
        assert!(s.contains("peers = [{"), "{s}");
        let back = load_profile(base, &s).expect("reload printed config");
        assert_eq!(back, c);
    }
}

// --------------------------------------------------------------- Phase 13 keys (13-02)

#[test]
fn phase13_defaults_match_d_p13_26_in_both_profiles() {
    use ostro_server::config::{UnkeyedStartPlacement, VersionPolicy};
    for c in both_profiles() {
        let k = &c.cluster;
        assert!(k.auto_finalize);
        assert_eq!(k.join_timeout.get(), Duration::from_secs(120));
        assert_eq!(k.join_orphan_timeout.get(), Duration::from_secs(25 * 3600));
        assert_eq!(k.promote_lag, 64);
        assert_eq!(k.handoff_grace.get(), Duration::from_secs(2));
        assert_eq!(k.handoff_catchup_timeout.get(), Duration::from_secs(10));
        assert_eq!(k.shutdown_handoff_timeout.get(), Duration::from_secs(30));
        assert_eq!(k.leader_rebalance_interval.get(), Duration::from_secs(300));
        assert_eq!(k.max_concurrent_moves, 2);
        assert_eq!(k.meta_max_message_bytes, 268_435_456);
        assert_eq!(c.api.unkeyed_start_placement, UnkeyedStartPlacement::Local);
        assert!(!c.api.strict_reads);
        assert!((c.api.lease_margin - 0.2).abs() < f64::EPSILON);
        assert_eq!(c.modules.gc_interval.get(), Duration::from_secs(600));
        assert_eq!(c.modules.gc_grace.get(), Duration::from_secs(600));
        assert!(c.modules.gc_protect_resumable);
        assert_eq!(c.workers.version_policy, VersionPolicy::Compatible);
        c.validate_phase13().expect("defaults are valid");
    }
}

const PHASE13_KEYS: &str = r#"
[cluster]
auto_finalize = false
join_timeout = "30s"
join_orphan_timeout = "48h"
promote_lag = 16
handoff_grace = "500ms"
handoff_catchup_timeout = "5s"
shutdown_handoff_timeout = "1m"
leader_rebalance_interval = "10m"
max_concurrent_moves = 4
meta_max_message_bytes = 67108864

[api]
unkeyed_start_placement = "uniform"
strict_reads = true
lease_margin = 0.3

[modules]
gc_interval = "1m"
gc_grace = "2m"
gc_protect_resumable = false

[workers]
version_policy = "exact"
"#;

#[test]
fn phase13_keys_parse_and_round_trip_in_both_profiles() {
    use ostro_server::config::{UnkeyedStartPlacement, VersionPolicy};
    for base in both_profiles() {
        let c = load_profile(base.clone(), PHASE13_KEYS).expect("phase 13 keys");
        let k = &c.cluster;
        assert!(!k.auto_finalize);
        assert_eq!(k.join_timeout.get(), Duration::from_secs(30));
        assert_eq!(k.join_orphan_timeout.get(), Duration::from_secs(48 * 3600));
        assert_eq!(k.promote_lag, 16);
        assert_eq!(k.handoff_grace.get(), Duration::from_millis(500));
        assert_eq!(k.handoff_catchup_timeout.get(), Duration::from_secs(5));
        assert_eq!(k.shutdown_handoff_timeout.get(), Duration::from_secs(60));
        assert_eq!(k.leader_rebalance_interval.get(), Duration::from_secs(600));
        assert_eq!(k.max_concurrent_moves, 4);
        assert_eq!(k.meta_max_message_bytes, 64 * 1024 * 1024);
        assert_eq!(
            c.api.unkeyed_start_placement,
            UnkeyedStartPlacement::Uniform
        );
        assert!(c.api.strict_reads);
        assert!((c.api.lease_margin - 0.3).abs() < f64::EPSILON);
        assert_eq!(c.modules.gc_interval.get(), Duration::from_secs(60));
        assert_eq!(c.modules.gc_grace.get(), Duration::from_secs(120));
        assert!(!c.modules.gc_protect_resumable);
        assert_eq!(c.workers.version_policy, VersionPolicy::Exact);
        // The profile's own keys are untouched.
        assert_eq!(c.api.listen, base.api.listen);
        let s = c.to_toml_string();
        assert!(s.contains("[modules]"), "{s}");
        let back = load_profile(base, &s).expect("reload printed config");
        assert_eq!(back, c);
    }
}

#[test]
fn phase13_invalid_values_are_rejected_and_named() {
    for base in both_profiles() {
        for (body, needle) in [
            ("[api]\nlease_margin = 0.0\n", "api.lease_margin"),
            ("[api]\nlease_margin = 0.5\n", "api.lease_margin"),
            ("[api]\nunkeyed_start_placement = \"random\"\n", "random"),
            ("[workers]\nversion_policy = \"loose\"\n", "loose"),
            ("[cluster]\njoin_timeout = \"0s\"\n", "cluster.join_timeout"),
            (
                "[cluster]\njoin_orphan_timeout = \"1h\"\n",
                "cluster.join_orphan_timeout",
            ),
            (
                "[cluster]\nmax_concurrent_moves = 0\n",
                "cluster.max_concurrent_moves",
            ),
            (
                "[cluster]\nmeta_max_message_bytes = 1024\n",
                "cluster.meta_max_message_bytes",
            ),
            ("[modules]\ngc_interval = \"0s\"\n", "modules.gc_interval"),
            ("[modules]\nbogus = 1\n", "bogus"),
        ] {
            let err = load_profile(base.clone(), body).expect_err(body);
            assert!(err.contains(needle), "{body:?}: {err}");
        }
    }
}

#[test]
fn phase13_env_overrides() {
    let c = load_toml_env(
        "",
        &env(&[
            ("OSTRO_CLUSTER__JOIN_TIMEOUT", "45s"),
            ("OSTRO_API__STRICT_READS", "true"),
            ("OSTRO_MODULES__GC_GRACE", "30m"),
        ]),
    )
    .expect("env overrides");
    assert_eq!(c.cluster.join_timeout.get(), Duration::from_secs(45));
    assert!(c.api.strict_reads);
    assert_eq!(c.modules.gc_grace.get(), Duration::from_secs(1800));
}
