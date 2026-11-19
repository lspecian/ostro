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
