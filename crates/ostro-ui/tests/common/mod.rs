//! Shared fixtures of the ostro-ui integration tests (11-15): the UI router over the
//! 11-09 fakes, deterministic seeding, and a oneshot request helper.

#![allow(dead_code)] // each test binary uses a subset

use std::sync::Arc;

use axum::body::Body;
use http::{HeaderMap, Request, StatusCode};
use ostro_api::ControlPlane;
use ostro_api::domain::{RunStatus, StartRunRequest, UploadModule};
use ostro_api::fake::{FakeControlPlane, FakeOps, FakePhase8, fake_module, sha256_hex};
use ostro_api::phase8::{LogLevelDto, RunLogLine};
use ostro_api::phase11::DlqEntry;
use ostro_api::rest::RunStatusDto;
use ostro_ui::auth::AllowAll;
use ostro_ui::{UiConfig, UiDeps};
use tower::ServiceExt;

/// A fixed CSRF token: requests carry it as the `ostro_csrf` cookie, so rendered pages
/// are deterministic.
pub const TOKEN: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// The Host every allowed request sends.
pub const HOST: &str = "127.0.0.1:7700";
/// A hostile tag.
pub const EVIL_TAG: &str = "<script>alert(1)</script>";
/// A hostile failure reason.
pub const EVIL_REASON: &str = "\"><img src=x onerror=alert(1)>";

/// The UI over fakes.
pub struct Ui {
    pub router: axum::Router,
    pub cp: Arc<FakeControlPlane>,
    pub ops: Arc<FakeOps>,
    pub p8: Arc<FakePhase8>,
}

/// Which extension slots the fixture fills.
#[derive(Clone, Copy)]
pub struct Slots {
    pub ops: bool,
    pub logs: bool,
}

impl Default for Slots {
    fn default() -> Self {
        Slots {
            ops: true,
            logs: true,
        }
    }
}

/// The UI config of the fixture: enabled, bound to 127.0.0.1:7700, `ops.example`
/// allowed, the guard probe mounted.
pub fn cfg() -> UiConfig {
    UiConfig {
        enabled: true,
        allowed_hosts: vec!["ops.example".to_owned()],
        bound_addr: Some("10.1.2.3:7700".parse().expect("addr")),
        test_probe: true,
    }
}

/// Build the UI with `slots` over fresh fakes (no runs).
pub fn ui_with(slots: Slots) -> Ui {
    let cp = Arc::new(FakeControlPlane::new());
    let ops = FakeOps::new();
    let p8 = FakePhase8::new(Arc::clone(&cp));
    let mut ext = if slots.logs {
        p8.extensions()
    } else {
        ostro_api::ApiExtensions::default()
    };
    if slots.ops {
        ops.install(&mut ext);
    }
    let router = ostro_ui::router(UiDeps {
        cp: Arc::clone(&cp) as Arc<dyn ControlPlane>,
        ext,
        cfg: cfg(),
        auth: Arc::new(AllowAll),
    });
    Ui {
        router,
        cp,
        ops,
        p8,
    }
}

/// [`ui_with`] every slot.
pub fn ui() -> Ui {
    ui_with(Slots::default())
}

/// Ids of the seeded runs.
pub struct Seeded {
    /// greet, completed, tags env=prod + team=a, published status, result.
    pub completed: String,
    /// greet, running, tag env=prod.
    pub running: String,
    /// billing, failed with [`EVIL_REASON`], hostile tag.
    pub failed: String,
    /// greet, ready, a rerun of `completed` from seq 3.
    pub rerun: String,
    /// billing, completed, the on_failure hook of `failed`.
    pub hook: String,
}

async fn start(cp: &FakeControlPlane, workflow: &str, queue: &str, tags: &[&str]) -> String {
    cp.start_run(StartRunRequest {
        workflow: workflow.to_owned(),
        input_json: br#"{"n":1}"#.to_vec(),
        queue: Some(queue.to_owned()),
        tags: tags.iter().map(|t| (*t).to_owned()).collect(),
        ..StartRunRequest::default()
    })
    .await
    .expect("start")
    .run_id
}

/// Upload the greet/billing module and seed five runs deterministically.
pub async fn seed(ui: &Ui) -> Seeded {
    let cp = &ui.cp;
    let wasm = fake_module(&[("greet", 1), ("billing", 2)]);
    cp.upload_module(UploadModule {
        sha256: sha256_hex(&wasm),
        wasm,
        language: None,
        tenant: None,
        name: None,
    })
    .await
    .expect("upload");
    let completed = start(cp, "greet", "default", &["env=prod", "team=a"]).await;
    let running = start(cp, "greet", "default", &["env=prod"]).await;
    let failed = start(cp, "billing", "payments", &[EVIL_TAG]).await;
    let rerun = start(
        cp,
        "greet",
        "default",
        &["env=prod", "__ostro/rerun_from=3"],
    )
    .await;
    let on_failure_tag = format!("__ostro/on_failure_of={failed}");
    let hook = start(cp, "billing", "payments", &[on_failure_tag.as_str()]).await;

    cp.set_run_outcome(&completed, RunStatus::Running, None, None, None)
        .expect("running");
    cp.set_run_outcome(
        &completed,
        RunStatus::Completed,
        Some(br#"{"greeting":"hello <b>world</b>"}"#.to_vec()),
        None,
        Some(br#"{"progress":100,"note":"<i>done</i>"}"#.to_vec()),
    )
    .expect("completed");
    cp.set_run_outcome(
        &running,
        RunStatus::Running,
        None,
        None,
        Some(br#"{"progress":40}"#.to_vec()),
    )
    .expect("running");
    cp.set_run_outcome(
        &failed,
        RunStatus::Failed,
        None,
        Some(EVIL_REASON.to_owned()),
        None,
    )
    .expect("failed");
    cp.set_run_outcome(
        &hook,
        RunStatus::Completed,
        Some(b"null".to_vec()),
        None,
        None,
    )
    .expect("hook done");

    // Exact replay markers on the completed run: seq 1 by attempt 1.1, seq 2..3 by 1.2.
    cp.set_history_attempt(&completed, 1, Some((1 << 32) | 1));
    cp.set_history_attempt(&completed, 2, Some((1 << 32) | 1));
    cp.set_history_attempt(&completed, 3, Some((1 << 32) | 2));

    ui.ops.push_dlq(DlqEntry {
        run_id: failed.clone(),
        workflow: "billing".to_owned(),
        version: 2,
        queue: "payments".to_owned(),
        status: RunStatusDto::Failed,
        reason: EVIL_REASON.to_owned(),
        failed_at_ms: 1_700_000_100_000,
        tags: vec![EVIL_TAG.to_owned()],
        resumable: false,
        hook_run_id: Some(hook.clone()),
        dismissed_at_ms: None,
        dismiss_reason: None,
        requeued_to: None,
        requeued_at_ms: None,
    });
    ui.p8.push_log(
        &running,
        RunLogLine {
            ts_ms: 1_700_000_050_000,
            level: LogLevelDto::Warn,
            message: "charging <card>".to_owned(),
            fields: serde_json::json!({"amount": 12, "html": "<b>x</b>"}),
            attempt: 1,
        },
    );
    ui.p8.push_log(
        &running,
        RunLogLine {
            ts_ms: 1_700_000_051_000,
            level: LogLevelDto::Info,
            message: "done".to_owned(),
            fields: serde_json::json!({}),
            attempt: 1,
        },
    );
    Seeded {
        completed,
        running,
        failed,
        rerun,
        hook,
    }
}

/// A response with its body as text.
pub struct Resp {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
}

impl Resp {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

/// Send `req` through `router`.
pub async fn send(router: &axum::Router, req: Request<Body>) -> Resp {
    let resp = router.clone().oneshot(req).await.expect("infallible");
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = axum::body::to_bytes(resp.into_body(), 16 << 20)
        .await
        .expect("body");
    Resp {
        status,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

/// An allowed GET of `path` carrying the fixed CSRF cookie.
pub fn get(path: &str) -> Request<Body> {
    Request::builder()
        .uri(path)
        .header("host", HOST)
        .header("cookie", format!("ostro_csrf={TOKEN}"))
        .body(Body::empty())
        .expect("request")
}

/// GET `path` on `ui`.
pub async fn get_ok(ui: &Ui, path: &str) -> Resp {
    let r = send(&ui.router, get(path)).await;
    assert_eq!(r.status, StatusCode::OK, "{path}: {}", r.body);
    r
}
