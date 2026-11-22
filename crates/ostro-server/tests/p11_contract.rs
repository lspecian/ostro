//! Phase 11 contract (11-04): the `[limits]`, `[ui]`, `[bulk]`, `[dlq]` config sections,
//! the `p11` seam (P11Ctx on the control plane, extension slots, emission gate) and the
//! UI router merge.

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use ostro_api::ErrorCode;
use ostro_api::phase11::{InventoryApi, OpsApi};
use ostro_server::config::{EnvSource, Overrides, ServerConfig, load_layered};
use ostro_server::p11::{P11Controllers, P11Feature, extensions, p11_feature_enabled, ui_config};

use p11::P11Server;

fn env(pairs: &[(&str, &str)]) -> EnvSource {
    EnvSource::Map(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect::<HashMap<_, _>>(),
    )
}

fn write_toml(dir: &tempfile::TempDir, body: &str) -> PathBuf {
    let p = dir.path().join("ostro.toml");
    std::fs::write(&p, body).expect("write toml");
    p
}

fn load(body: &str, e: &EnvSource) -> Result<ServerConfig, String> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = write_toml(&dir, body);
    load_layered(
        ServerConfig::server_defaults(),
        Some(&path),
        true,
        e,
        &Overrides::default(),
    )
    .map_err(|err| err.to_string())
}

#[test]
fn phase11_section_defaults() {
    let c = ServerConfig::server_defaults();
    assert_eq!(c.limits.max_history_events, 50_000);
    assert_eq!(c.limits.max_children_per_run, 10_000);
    assert_eq!(c.limits.max_payload_bytes.get(), 4 * 1024 * 1024);
    assert_eq!(c.ui.enabled, None);
    assert!(c.ui.allowed_hosts.is_empty());
    let b = &c.bulk;
    assert_eq!(
        (b.max_jobs, b.max_matches, b.default_rate, b.max_rate),
        (4, 100_000, 20, 500)
    );
    assert_eq!((b.burst, b.max_in_flight), (10, 4));
    assert_eq!(b.backlog_high, 10_000);
    assert_eq!(b.checkpoint_every.get(), Duration::from_secs(1));
    assert_eq!(b.checkpoint_ops, 500);
    assert_eq!(c.dlq.hook_catchup.get(), Duration::from_secs(24 * 3600));
    assert!(c.dlq.hooks_enabled);
    assert_eq!(ServerConfig::dev_defaults().ui.enabled, None);
    // [limits] maps onto EngineConfig.run_limits.
    assert_eq!(
        c.to_engine_config().run_limits,
        ostro_engine::RunLimits::default()
    );
}

#[test]
fn phase11_sections_parse_from_toml() {
    let c = load(
        r#"
[limits]
max_history_events = 20
max_payload_bytes = "1MiB"

[ui]
enabled = true
allowed_hosts = ["ostro.internal"]

[bulk]
max_jobs = 2
checkpoint_every = "250ms"

[dlq]
hook_catchup = "1h"
hooks_enabled = false
"#,
        &env(&[]),
    )
    .expect("load");
    assert_eq!(c.limits.max_history_events, 20);
    assert_eq!(c.limits.max_payload_bytes.get(), 1024 * 1024);
    let rl = c.to_engine_config().run_limits;
    assert_eq!(rl.max_history_events, 20);
    assert_eq!(rl.max_payload_bytes, 1024 * 1024);
    assert_eq!(rl.max_children_per_run, 10_000);
    assert_eq!(c.ui.enabled, Some(true));
    assert_eq!(c.ui.allowed_hosts, vec!["ostro.internal".to_owned()]);
    assert_eq!(c.bulk.max_jobs, 2);
    assert_eq!(c.bulk.checkpoint_every.get(), Duration::from_millis(250));
    assert_eq!(c.dlq.hook_catchup.get(), Duration::from_secs(3600));
    assert!(!c.dlq.hooks_enabled);
    // --print-config round trip keeps the Phase 11 sections.
    let printed = c.to_toml_string();
    for section in ["[limits]", "[ui]", "[bulk]", "[dlq]"] {
        assert!(printed.contains(section), "{section} missing:\n{printed}");
    }
    let again = load(&printed, &env(&[])).expect("reload printed config");
    assert_eq!(again.limits, c.limits);
    assert_eq!(again.ui, c.ui);
    assert_eq!(again.bulk, c.bulk);
    assert_eq!(again.dlq, c.dlq);
}

#[test]
fn unknown_keys_in_phase11_sections_are_rejected() {
    for section in ["limits", "ui", "bulk", "dlq"] {
        let err = load(&format!("[{section}]\nbogus_key = 1\n"), &env(&[]))
            .expect_err("unknown key must fail");
        assert!(err.contains("bogus_key"), "[{section}]: {err}");
    }
}

#[test]
fn bad_values_name_the_key() {
    let err = load("[bulk]\ncheckpoint_every = \"soon\"\n", &env(&[])).expect_err("duration");
    assert!(err.contains("checkpoint_every"), "{err}");
    let err = load("[dlq]\nhook_catchup = \"later\"\n", &env(&[])).expect_err("duration");
    assert!(err.contains("hook_catchup"), "{err}");
    let err = load("[limits]\nmax_payload_bytes = \"lots\"\n", &env(&[])).expect_err("size");
    assert!(err.contains("max_payload_bytes"), "{err}");
    let err = load("[bulk]\ndefault_rate = 600\n", &env(&[])).expect_err("rate");
    assert!(
        err.contains("bulk.default_rate") && err.contains("bulk.max_rate"),
        "{err}"
    );
    let err = load("[limits]\nmax_history_events = 0\n", &env(&[])).expect_err("zero");
    assert!(err.contains("limits.max_history_events"), "{err}");
}

#[test]
fn payload_limit_must_not_exceed_request_limit() {
    let err = load(
        "[api]\nmax_request_bytes = 1048576\n[limits]\nmax_payload_bytes = \"2MiB\"\n",
        &env(&[]),
    )
    .expect_err("payload > request must fail");
    assert!(
        err.contains("limits.max_payload_bytes") && err.contains("api.max_request_bytes"),
        "{err}"
    );
    // Equal is fine.
    load(
        "[api]\nmax_request_bytes = 1048576\n[limits]\nmax_payload_bytes = \"1MiB\"\n",
        &env(&[]),
    )
    .expect("equal limits load");
}

#[test]
fn env_overlays_phase11_sections() {
    let c = load(
        "",
        &env(&[
            ("OSTRO_UI_ALLOWED_HOSTS", "a.example, b.example:7700,,"),
            ("OSTRO_LIMITS__MAX_HISTORY_EVENTS", "123"),
            ("OSTRO_BULK__MAX_RATE", "100"),
        ]),
    )
    .expect("load");
    assert_eq!(
        c.ui.allowed_hosts,
        vec!["a.example".to_owned(), "b.example:7700".to_owned()]
    );
    assert_eq!(c.limits.max_history_events, 123);
    assert_eq!(c.bulk.max_rate, 100);
}

#[test]
fn ui_enabled_defaults_follow_profile_and_listener() {
    let mut c = ServerConfig::server_defaults();
    assert!(!ui_config(&c).enabled, "server profile on 0.0.0.0");
    c.api.listen = "127.0.0.1:0".parse().expect("addr");
    assert!(ui_config(&c).enabled, "server profile on loopback");
    c.api.listen = "[::1]:0".parse().expect("addr");
    assert!(ui_config(&c).enabled, "server profile on ::1");
    c.api.listen = "0.0.0.0:7700".parse().expect("addr");
    c.ui.enabled = Some(true);
    assert!(ui_config(&c).enabled, "explicit on (WARNs at startup)");
    let mut d = ServerConfig::dev_defaults();
    assert!(ui_config(&d).enabled, "dev profile");
    d.ui.enabled = Some(false);
    assert!(!ui_config(&d).enabled, "dev explicit off");
}

#[test]
fn every_phase11_emission_is_enabled_until_phase_13() {
    for f in P11Feature::ALL {
        assert!(p11_feature_enabled(f), "{f:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn seam_is_wired_after_start_and_released_at_shutdown() {
    let mut s = P11Server::builder()
        .with_limits(|l| l.max_history_events = 777)
        .start()
        .await;
    let ctx = s.h().control().p11().expect("P11Ctx set by p11::wire");
    assert_eq!(ctx.limits.max_history_events, 777);
    assert_eq!(ctx.bulk.config(), &s.cfg.bulk);
    assert_eq!(ctx.bulk.data_dir(), s.data_dir());

    // The extension slots p11::wire installs.
    let ext = extensions(&ctx.cc, &ctx);
    assert!(ext.ops.is_some() && ext.inventory.is_some());
    assert!(ext.schedules.is_some(), "Phase 8 slots kept");

    // Controllers delegate to the control::p11_* stubs.
    let c = P11Controllers(ctx.clone());
    let err = c.limits().await.expect_err("stub");
    assert_eq!(err.code, ErrorCode::Unimplemented);
    assert_eq!(err.details.as_ref().expect("details")["lands_in"], "11-11");
    let err = c.list_bulk().await.expect_err("stub");
    assert_eq!(err.details.as_ref().expect("details")["lands_in"], "11-13");
    let err = c.list_partitions().await.expect_err("stub");
    assert_eq!(err.code, ErrorCode::Unimplemented);
    drop((c, ext, ctx));

    // The UI merge shadows no existing route.
    let (st, v) = s.get_json("/v1/runs").await;
    assert_eq!(st, reqwest::StatusCode::OK, "{v}");
    let r = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    // 11-09: with the UI enabled (loopback listener) `GET /` answers 303 -> /ui/.
    let r = s.http.get(s.url("/")).send().await.expect("GET /");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    assert_eq!(r.url().path(), "/ui/", "root redirects to the UI");
    let r = s
        .http
        .get(s.url("/healthz"))
        .send()
        .await
        .expect("GET /healthz");
    assert_eq!(r.status(), reqwest::StatusCode::OK);

    // No reference cycle: shutdown releases meta and the node (restart reopens the dir),
    // and the restarted server is wired again.
    let control = std::sync::Arc::downgrade(s.h().control());
    s.restart().await;
    assert!(
        control.upgrade().is_none(),
        "the control plane (and its meta handle) must be released at shutdown"
    );
    assert!(s.h().control().p11().is_some(), "wired after restart");
    let control = std::sync::Arc::downgrade(s.h().control());
    s.shutdown().await;
    assert!(control.upgrade().is_none(), "released at shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn server_starts_with_phase11_overrides() {
    let mut s = P11Server::builder()
        .partitions(2)
        .with_ui(|u| u.enabled = Some(false))
        .with_bulk(|b| b.max_jobs = 1)
        .start()
        .await;
    let ctx = s.h().control().p11().expect("P11Ctx");
    assert_eq!(ctx.bulk.config().max_jobs, 1);
    assert_eq!(ctx.cfg.ui.enabled, Some(false));
    drop(ctx);
    let (st, _) = s.get_json("/v1/runs").await;
    assert_eq!(st, reqwest::StatusCode::OK);
    s.shutdown().await;
}
