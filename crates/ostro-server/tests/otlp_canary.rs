//! D-FX6 OTLP canary leg (plan 08-21): a canary secret used by the Phase 8 code paths (and
//! by the Phase 6 fx-canary scenario) never appears in anything exported over OTLP.
//!
//! One server, node logs at TRACE (compiler IR targets excepted, as in 06-19), OTLP traces
//! and logs to the in-process collector, which is registered as the Phase 6 canary
//! harness sink `otlp` (`ostro_testkit::canary::register_sink`). The canary is:
//!
//! - a sealed meta secret (`canary-key`, set over REST);
//! - the `secret_ref` of a Stripe webhook, driven with valid, tampered, unsigned and stale
//!   deliveries;
//! - `[backup] secret_access_key` of the running server (LocalFs URL, so the key is loaded
//!   but unused), and `env:CANARY` in a restore config loaded through `load_layered` and
//!   used for a real restore of the server's backup prefix;
//! - the `{{secret:canary-key}}` header of an fx-canary HTTP effect (06-19's scenario,
//!   with 08-21's outbound traceparent and effect spans active).
//!
//! Schedule CRUD runs too. Then the collector's raw export (every request's `Debug`
//! rendering) and every decoded span/log string (names, bodies, attribute keys and values,
//! resources) are scanned for the canary in every harness encoding (raw, hex, base64 at
//! three alignments, percent-encoded, JSON-escaped): zero hits. The backup prefix and the
//! restored data directory are scanned as well.
//!
//! One test function: the server installs the process-global subscriber and tracer
//! provider.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::Response;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use common::fx::{configure, deploy_fx, wait_terminal};
use common::phase8::sign_stripe;
use common::{BridgeTestOpts, start_server_with_bridge, tmpfs_dir};
use ostro_backup::restore::{RestoreOptions, RestoreSource, restore};
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_server::config::{EnvSource, Overrides, ServerConfig, load_layered};
use ostro_testkit::canary::{
    Canary, EXCLUDED_TARGET_PREFIXES, Quiet, Report, register_sink, sink_names, spawn_quiet,
};
use ostro_testkit::otlp::Collector;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

const RUN_LIMIT: Duration = Duration::from_secs(60);
const WAIT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------------
// Recording echo mock (runs under spawn_quiet: it legitimately receives the canary)
// ---------------------------------------------------------------------------------

#[derive(Default)]
struct MockState {
    api_keys: Mutex<Vec<Vec<u8>>>,
}

struct Mock {
    addr: SocketAddr,
    state: Arc<MockState>,
    task: JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Mock {
    async fn start() -> Mock {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let addr = l.local_addr().expect("addr");
        let state = Arc::new(MockState::default());
        let app = Router::new().fallback(echo).with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(l, app).await;
        });
        Mock { addr, state, task }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn api_keys(&self) -> Vec<Vec<u8>> {
        self.state.api_keys.lock().unwrap().clone()
    }
}

/// `/echo`: 200 JSON `{"headers": [[name, value], ..]}` (MockProvider shape).
async fn echo(State(st): State<Arc<MockState>>, req: Request) -> Response<Body> {
    if let Some(k) = req.headers().get("x-api-key") {
        st.api_keys.lock().unwrap().push(k.as_bytes().to_vec());
    }
    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                String::from_utf8_lossy(v.as_bytes()).into_owned(),
            )
        })
        .collect();
    Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(Body::from(json!({ "headers": headers }).to_string()))
        .expect("response")
}

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

fn now_s() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}

/// Targets kept at INFO although everything else is at TRACE: the OTLP exporter's own gRPC
/// transport (`h2`, `tonic`, `tower`: at TRACE their spans feed the span exporter back into
/// itself, hundreds of thousands of spans that overflow the batch queues and drop the
/// records this test needs) and the WAL group-commit loop (a TRACE line per tick, sizes
/// only). The outbound HTTP path of the bridge (`hyper`, `reqwest`) stays at TRACE.
const QUIET_TARGETS: &[&str] = &["h2", "tonic", "tower", "ostro_wal::group_commit"];

/// `[log] level`: TRACE for everything but the compiler IR targets (06-19 deviation 1:
/// cranelift/regalloc2 TRACE dumps of guest code, which holds no secret, are gigabytes)
/// and [`QUIET_TARGETS`].
fn trace_level() -> String {
    let mut d = vec!["trace".to_owned()];
    d.extend(QUIET_TARGETS.iter().map(|t| format!("{t}=info")));
    d.extend(EXCLUDED_TARGET_PREFIXES.iter().map(|t| format!("{t}=warn")));
    d.join(",")
}

/// Every decoded string of the export: span names, attribute keys/values, resources, log
/// bodies, severities and attributes.
fn decoded(c: &Collector) -> Vec<u8> {
    let mut out = Vec::new();
    let mut push = |s: &str| {
        out.extend_from_slice(s.as_bytes());
        out.push(b'\n');
    };
    for s in c.spans() {
        push(&s.name);
        push(&s.scope);
        for (k, v) in s.attributes.iter().chain(s.resource.iter()) {
            push(k);
            push(v);
        }
    }
    for l in c.logs() {
        push(&l.body);
        push(&l.scope);
        push(&l.severity_text);
        for (k, v) in l.attributes.iter().chain(l.resource.iter()) {
            push(k);
            push(v);
        }
    }
    out
}

fn contains(hay: &[u8], needle: &str) -> bool {
    hay.windows(needle.len()).any(|w| w == needle.as_bytes())
}

// ---------------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn otlp_export_never_carries_the_canary() {
    let canary = Canary::new();
    // Non-vacuity of the scanner itself: the raw value and its base64 are found.
    assert!(!canary.scan_bytes("control", canary.expose()).is_empty());

    let (addr, collector) = Collector::start().await;
    let sink = collector.clone();
    register_sink("otlp", Box::new(move || sink.all_bytes()));
    assert!(sink_names().contains(&"otlp".to_owned()));

    let mock: Quiet<Mock> = spawn_quiet(Mock::start());
    let remote = tempfile::tempdir().expect("backup prefix");
    let extra = format!(
        "[log]\nlevel = \"{}\"\n\n[telemetry]\notlp_endpoint = \"http://{addr}\"\n\
         service_name = \"ostro-otlp-canary\"\n\n[backup]\nurl = \"file://{}\"\n\
         seal_interval = \"300ms\"\nsecret_access_key = \"{}\"\n",
        trace_level(),
        remote.path().display(),
        canary.expose_str()
    );
    let mut s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            extra_toml: extra,
            ..BridgeTestOpts::default()
        },
    )
    .await;
    assert!(
        s.cfg.backup.enabled(),
        "backup configured with the canary key"
    );

    // ---- sealed meta secret + fx-canary grant/egress, sdk-basic for the hooks/schedules.
    let canary_mod = deploy_fx(&s, "fx-canary").await;
    s.deploy_guest("sdk-basic", None).await;
    configure(&s)
        .secret("canary-key", canary.expose_str())
        .grant(&canary_mod, "canary-key", &[&mock.base_url()], false)
        .egress(&canary_mod, &[&mock.base_url()])
        .apply()
        .await;

    // ---- Stripe webhook keyed by the canary secret: valid, tampered, unsigned, stale.
    let (st, v) = s
        .create_webhook(&json!({
            "name": "pay",
            "action": {"kind": "start", "workflow": "sdk-echo", "input": "/data/object"},
            "verifier": {"kind": "stripe", "secret_ref": "canary-key"},
        }))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let key = canary.expose();
    let body = br#"{"id":"evt_canary_1","data":{"object":{"amount":1}}}"#;
    let (st, _, b) = s
        .deliver_hook("pay", &sign_stripe(key, now_s(), body), body)
        .await;
    assert_eq!(st, StatusCode::OK, "{b:?}");
    let hook: Value = serde_json::from_slice(&b).expect("hook json");
    let hook_run = hook["run_id"].as_str().expect("run_id").to_owned();
    s.wait_run_status(&hook_run, "completed", RUN_LIMIT).await;
    let (st, _, _) = s
        .deliver_hook("pay", &sign_stripe(key, now_s(), body), body)
        .await;
    assert_eq!(st, StatusCode::OK, "redelivery");
    let tampered = br#"{"id":"evt_canary_1","data":{"object":{"amount":2}}}"#;
    let (st, _, _) = s
        .deliver_hook("pay", &sign_stripe(key, now_s(), body), tampered)
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED, "bad signature");
    let (st, _, _) = s.deliver_hook("pay", &[], body).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED, "unsigned");
    let stale = br#"{"id":"evt_canary_2","data":{"object":{"amount":3}}}"#;
    let (st, _, _) = s
        .deliver_hook("pay", &sign_stripe(key, now_s() - 3_600, stale), stale)
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED, "stale");
    let (st, v) = s.get_json("/v1/webhooks/pay").await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        canary
            .scan_bytes("webhook get", v.to_string().as_bytes())
            .is_empty(),
        "webhook GET leaked"
    );

    // ---- schedule CRUD.
    let sched = json!({
        "name": "canary-sched", "workflow": "sdk-echo", "cron": "0 0 9 * * MON",
        "timezone": "UTC", "input": {"x": 1}, "overlap": "skip", "catchup": "skip",
    });
    let (st, v) = s.create_schedule(&sched).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let tag = v["version_tag"].as_u64().expect("version_tag");
    for p in ["/v1/schedules", "/v1/schedules/canary-sched"] {
        let (st, v) = s.get_json(p).await;
        assert_eq!(st, StatusCode::OK, "GET {p}: {v}");
    }
    for p in ["pause", "resume"] {
        let (st, v) = s
            .post_json(&format!("/v1/schedules/canary-sched/{p}"), &json!({}))
            .await;
        assert_eq!(st, StatusCode::OK, "{p}: {v}");
    }
    let (_, v) = s.get_json("/v1/schedules/canary-sched").await;
    let tag = v["version_tag"].as_u64().unwrap_or(tag);
    let mut upd = sched.clone();
    upd["cron"] = json!("0 30 9 * * MON");
    upd["version_tag"] = json!(tag);
    let (st, v) = s.put_json("/v1/schedules/canary-sched", &upd).await;
    assert_eq!(st, StatusCode::OK, "update: {v}");
    let tag = v["version_tag"].as_u64().expect("tag");
    let (st, v) = s
        .delete_json(&format!("/v1/schedules/canary-sched?version_tag={tag}"))
        .await;
    assert!(st.is_success(), "delete: {st} {v}");

    // ---- the Phase 6 fx-canary scenario: {{secret:canary-key}} as X-Api-Key.
    let run = s
        .start_rest(
            "fx-canary",
            json!({"input": {"echo_url": format!("{}/echo", mock.base_url()),
                             "secret": "canary-key"}}),
        )
        .await;
    let info = wait_terminal(&s, &run, RUN_LIMIT).await;
    assert_eq!(
        serde_json::to_value(info.status).expect("status"),
        "completed",
        "{info:?}"
    );
    assert_eq!(
        mock.api_keys(),
        vec![canary.expose().to_vec()],
        "the provider received exactly the canary (the effect really ran)"
    );
    let out = s.result_bytes(&run).await.expect("result");
    assert!(canary.scan_bytes("guest output", &out).is_empty());
    let out: Value = serde_json::from_slice(&out).expect("guest output json");
    let echoed = STANDARD
        .decode(out["observed"]["body_b64"].as_str().expect("observed body"))
        .expect("base64 body");
    assert!(contains(&echoed, "[REDACTED]"), "echo scrubbed: {out}");

    // ---- restore config: `env:CANARY`, loaded through load_layered, used for a restore.
    let cfg_dir = tempfile::tempdir().expect("cfg dir");
    let toml = cfg_dir.path().join("restore.toml");
    std::fs::write(
        &toml,
        format!(
            "[backup]\nurl = \"file://{}\"\nsecret_access_key = \"env:CANARY\"\n",
            remote.path().display()
        ),
    )
    .expect("write restore toml");
    let env = EnvSource::Map(HashMap::from([(
        "CANARY".to_owned(),
        canary.expose_str().to_owned(),
    )]));
    let rcfg = load_layered(
        ServerConfig::server_defaults(),
        Some(&toml),
        true,
        &env,
        &Overrides::default(),
    )
    .expect("restore config");
    let bcfg = rcfg.backup.to_backup_config();
    assert_eq!(
        bcfg.secret_access_key.as_deref(),
        Some(canary.expose_str()),
        "env:CANARY resolved"
    );
    tracing::info!(target: "ostro::test", backup = ?rcfg.backup, config = ?bcfg,
        "otlp canary: restore config loaded");
    tracing::debug!(target: "ostro::test", print_config = %rcfg.to_toml_string(),
        "otlp canary: print-config");
    let store = LocalFs::new(remote.path()).expect("localfs");
    let deadline = Instant::now() + WAIT;
    while store
        .get(ostro_backup::manifest::CURRENT_KEY)
        .await
        .expect("get manifest")
        .is_none()
    {
        assert!(Instant::now() < deadline, "no manifest uploaded");
        sleep(Duration::from_millis(100)).await;
    }
    let restored = tempfile::tempdir().expect("restore target");
    let rep = restore(
        RestoreSource::Url {
            url: bcfg.url.clone().expect("url"),
            options: ostro_backup::fence::store_options(&bcfg),
        },
        RestoreOptions::new(restored.path()),
    )
    .await
    .expect("restore with the env:CANARY config");
    tracing::info!(target: "ostro::test", report = ?rep, "otlp canary: restore done");

    // ---- flush and wait for the export to cover every path above.
    s.h().telemetry().force_flush().await;
    let run_s = run.to_string();
    collector
        .wait_for(
            |c| {
                let d = decoded(c);
                contains(&d, "otlp canary: restore done")
                    && contains(&d, &hook_run)
                    && contains(&d, &run_s)
                    && c.spans().iter().any(|sp| sp.name == "effect:http")
            },
            WAIT,
        )
        .await;
    s.h().telemetry().force_flush().await;
    let spans = collector.spans().len();
    let logs = collector.logs().len();
    assert!(spans > 0 && logs > 100, "spans {spans}, logs {logs}");
    let dec = decoded(&collector);
    assert!(contains(&dec, "otlp canary: restore config loaded"));
    let mut by_scope: HashMap<String, usize> = HashMap::new();
    for l in collector.logs() {
        *by_scope.entry(l.scope).or_default() += 1;
    }
    let mut top: Vec<(String, usize)> = by_scope.into_iter().collect();
    top.sort_by_key(|e| std::cmp::Reverse(e.1));
    top.truncate(8);
    println!("busiest log scopes: {top:?}");

    // ---- the scan.
    let raw = collector.all_bytes();
    let mut report = Report::new(&canary);
    report
        .sinks()
        .bytes("otlp decoded strings", &dec)
        .tree("backup prefix", remote.path())
        .tree("restored data dir", restored.path());
    let summary = report.summary();
    println!(
        "OTLP canary scan: {spans} spans, {logs} log records, {} raw export bytes, {} decoded \
         bytes, {} needles\n{summary}",
        raw.len(),
        dec.len(),
        canary.needles().len()
    );
    report.assert_clean();
    println!("PHASE6_CANARY_LEG: run (fx-canary through the harness sink \"otlp\"): clean");

    s.shutdown().await;
}
