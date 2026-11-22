//! UI actions (11-16, UI-02): run lifecycle, DLQ requeue/dismiss, rerun-from-step and
//! bulk jobs against the fakes. Every action: the trait is called once with the right
//! arguments, htmx gets a fragment (or `HX-Redirect`), a plain form gets 303, the guard
//! and the `operate` scope refuse without calling anything.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use common::{HOST, Resp, Seeded, TOKEN, Ui, get_ok, seed, send, ui};
use http::{Request, StatusCode};
use ostro_api::ControlPlane;
use ostro_api::domain::{HistoryRequest, RunStatus, StartRunRequest};
use ostro_api::fake::FakeOps;
use ostro_api::phase11::{
    DlqDismissRequest, DlqEntry, DlqListRequest, DlqPage, DlqRequeueRequest, DlqRequeueResponse,
    OpsApi, RequeueMode, RerunFrom,
};
use ostro_api::rest::RunStatusDto;
use ostro_api::{ApiError, ApiExtensions};
use ostro_ui::UiDeps;
use ostro_ui::auth::{UiAuth, UiPrincipal, UiReject};

// ---------------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------------

fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn form_body(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// How a mutating request arrives.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Via {
    /// htmx: `HX-Request` + `X-Ostro-Csrf` header token.
    Htmx,
    /// Plain form post: `csrf` field.
    Form,
    /// A cross-site form post with a valid token.
    CrossSite,
}

fn post(via: Via, path: &str, fields: &[(&str, &str)]) -> Request<Body> {
    let mut all: Vec<(&str, &str)> = fields.to_vec();
    if via != Via::Htmx {
        all.push(("csrf", TOKEN));
    }
    let mut b = Request::builder()
        .method("POST")
        .uri(path)
        .header("host", HOST)
        .header("cookie", format!("ostro_csrf={TOKEN}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .header(
            "sec-fetch-site",
            if via == Via::CrossSite {
                "cross-site"
            } else {
                "same-origin"
            },
        );
    if via == Via::Htmx {
        b = b.header("hx-request", "true").header("x-ostro-csrf", TOKEN);
    }
    b.body(Body::from(form_body(&all))).expect("request")
}

async fn act(ui: &Ui, via: Via, path: &str, fields: &[(&str, &str)]) -> Resp {
    send(&ui.router, post(via, path, fields)).await
}

fn assert_html(r: &Resp, what: &str) {
    assert_eq!(r.status, StatusCode::OK, "{what}: {}", r.body);
    assert_eq!(
        r.header("content-type"),
        Some("text/html; charset=utf-8"),
        "{what}"
    );
}

fn assert_303(r: &Resp, to: &str, what: &str) {
    assert_eq!(r.status, StatusCode::SEE_OTHER, "{what}: {}", r.body);
    assert_eq!(r.header("location"), Some(to), "{what}");
}

async fn kinds(ui: &Ui, run_id: &str) -> Vec<String> {
    ui.cp
        .run_history(HistoryRequest {
            run_id: run_id.to_owned(),
            from_seq: 0,
            limit: 0,
        })
        .await
        .expect("history")
        .events
        .into_iter()
        .map(|e| e.kind)
        .collect()
}

fn count(kinds: &[String], kind: &str) -> usize {
    kinds.iter().filter(|k| *k == kind).count()
}

async fn status(ui: &Ui, run_id: &str) -> RunStatus {
    ui.cp.get_run(run_id.to_owned()).await.expect("run").status
}

/// A fresh greet run in `status`.
async fn run_in(ui: &Ui, status: RunStatus) -> String {
    let id = ui
        .cp
        .start_run(StartRunRequest {
            workflow: "greet".to_owned(),
            input_json: b"{}".to_vec(),
            ..StartRunRequest::default()
        })
        .await
        .expect("start")
        .run_id;
    if status != RunStatus::Ready {
        ui.cp
            .set_run_outcome(&id, status, None, None, None)
            .expect("outcome");
    }
    id
}

async fn seeded() -> (Ui, Seeded) {
    let ui = ui();
    let s = seed(&ui).await;
    (ui, s)
}

fn runs_path(id: &str, action: &str) -> String {
    format!("/ui/runs/{id}/{action}")
}

// ---------------------------------------------------------------------------------
// Run lifecycle
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn lifecycle_htmx_actions_call_the_trait_and_return_the_status_fragment() {
    let (ui, s) = seeded().await;
    let id = &s.running;

    let t0 = ui.cp.clock_ticks();
    let r = act(
        &ui,
        Via::Htmx,
        &runs_path(id, "pause"),
        &[("detail", "maintenance <now>")],
    )
    .await;
    assert_html(&r, "pause");
    assert!(r.body.contains(r#"<section id="run-status""#), "{}", r.body);
    assert!(
        r.body
            .contains(r#"id="run-actions" class="card actions" hx-swap-oob="true""#)
    );
    assert!(
        r.body.contains("maintenance &#60;now&#62;") || r.body.contains("maintenance &lt;now&gt;")
    );
    assert!(
        r.body.contains(&format!("{}/resume", runs_path_base(id))),
        "resume offered"
    );
    assert!(
        !r.body.contains(&format!("{}/pause\"", runs_path_base(id))),
        "pause hidden"
    );
    assert!(r.body.contains(&format!(r#"name="csrf" value="{TOKEN}""#)));
    assert_eq!(
        ui.cp.run_pause(id),
        Some(Some("maintenance <now>".to_owned()))
    );
    assert_eq!(ui.cp.clock_ticks(), t0 + 1, "exactly one mutating call");

    let r = act(&ui, Via::Htmx, &runs_path(id, "resume"), &[]).await;
    assert_html(&r, "resume");
    assert_eq!(ui.cp.run_pause(id), None);
    assert_eq!(count(&kinds(&ui, id).await, "RunResumed"), 1);

    let r = act(
        &ui,
        Via::Htmx,
        &runs_path(id, "signal"),
        &[("name", "approve"), ("payload", r#"{"ok":true}"#)],
    )
    .await;
    assert_html(&r, "signal");
    assert_eq!(
        ui.cp.signals(id),
        vec![("approve".to_owned(), serde_json::json!({"ok": true}))]
    );
    // An empty payload is JSON null.
    let r = act(
        &ui,
        Via::Htmx,
        &runs_path(id, "signal"),
        &[("name", "ping")],
    )
    .await;
    assert_html(&r, "signal null");
    assert_eq!(
        ui.cp.signals(id)[1],
        ("ping".to_owned(), serde_json::Value::Null)
    );

    let r = act(
        &ui,
        Via::Htmx,
        &runs_path(id, "cancel"),
        &[("reason", "op")],
    )
    .await;
    assert_html(&r, "cancel");
    assert_eq!(ui.cp.cancel_requested(id), Some(Some("op".to_owned())));
    assert_eq!(count(&kinds(&ui, id).await, "CancelRequested"), 1);

    let r = act(
        &ui,
        Via::Htmx,
        &runs_path(id, "terminate"),
        &[("reason", "stuck")],
    )
    .await;
    assert_html(&r, "terminate");
    assert_eq!(status(&ui, id).await, RunStatus::Terminated);
    assert!(r.body.contains("badge terminated"), "{}", r.body);
    assert!(
        r.body.contains("No actions apply"),
        "terminal run: no buttons"
    );
    let k = kinds(&ui, id).await;
    assert_eq!(count(&k, "RunTerminated"), 1);

    // Retry of a resumable run resumes in place: the status fragment.
    let r = act(&ui, Via::Htmx, &runs_path(id, "retry"), &[]).await;
    assert_html(&r, "retry resume");
    assert_eq!(status(&ui, id).await, RunStatus::Ready);
    // Retry of a non-resumable failed run restarts: HX-Redirect to the new run.
    let r = act(&ui, Via::Htmx, &runs_path(&s.failed, "retry"), &[]).await;
    assert_eq!(r.status, StatusCode::OK);
    let to = r.header("hx-redirect").expect("hx-redirect").to_owned();
    assert!(
        to.starts_with("/ui/runs/") && !to.ends_with(&s.failed),
        "{to}"
    );
}

fn runs_path_base(id: &str) -> String {
    format!("/ui/runs/{id}")
}

#[tokio::test]
async fn lifecycle_form_posts_redirect_to_the_run_page() {
    let (ui, s) = seeded().await;
    let id = &s.running;
    let page = runs_path_base(id);
    for (action, fields) in [
        ("pause", vec![]),
        ("resume", vec![]),
        ("signal", vec![("name", "go"), ("payload", "1")]),
        ("cancel", vec![]),
        ("terminate", vec![("reason", "done")]),
        ("retry", vec![]),
    ] {
        let t0 = ui.cp.clock_ticks();
        let r = act(&ui, Via::Form, &runs_path(id, action), &fields).await;
        assert_303(&r, &page, action);
        assert_eq!(ui.cp.clock_ticks(), t0 + 1, "{action}: one call");
    }
    assert_eq!(
        ui.cp.signals(id),
        vec![("go".to_owned(), serde_json::json!(1))]
    );
    // Restart lands on the new run.
    let r = act(&ui, Via::Form, &runs_path(&s.failed, "retry"), &[]).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    assert_ne!(
        r.header("location"),
        Some(runs_path_base(&s.failed).as_str())
    );
}

#[tokio::test]
async fn lifecycle_cross_site_and_tokenless_posts_are_403_without_a_call() {
    let (ui, s) = seeded().await;
    for action in ["pause", "resume", "signal", "cancel", "terminate", "retry"] {
        let t0 = ui.cp.clock_ticks();
        let fields = [("name", "x"), ("reason", "y")];
        let r = act(&ui, Via::CrossSite, &runs_path(&s.running, action), &fields).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{action}");
        // Same origin but no token at all.
        let req = Request::builder()
            .method("POST")
            .uri(runs_path(&s.running, action))
            .header("host", HOST)
            .header("cookie", format!("ostro_csrf={TOKEN}"))
            .header("sec-fetch-site", "same-origin")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("name=x"))
            .expect("request");
        assert_eq!(send(&ui.router, req).await.status, StatusCode::FORBIDDEN);
        assert_eq!(ui.cp.clock_ticks(), t0, "{action}: nothing called");
    }
    assert_eq!(status(&ui, &s.running).await, RunStatus::Running);
}

#[tokio::test]
async fn lifecycle_api_errors_render_inline_or_on_the_error_page() {
    let (ui, s) = seeded().await;
    // Terminate a terminal run: 409 page for a form, #flash for htmx.
    let r = act(&ui, Via::Form, &runs_path(&s.completed, "terminate"), &[]).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert!(r.body.contains("is already completed"), "{}", r.body);
    assert!(r.body.contains("error-message"));
    let r = act(&ui, Via::Htmx, &runs_path(&s.completed, "terminate"), &[]).await;
    assert_html(&r, "htmx error");
    assert_eq!(r.header("hx-retarget"), Some("#flash"));
    assert_eq!(r.header("hx-reswap"), Some("outerHTML"));
    assert!(
        r.body
            .starts_with(r#"<div id="flash" class="flash" role="alert">"#),
        "{}",
        r.body
    );
    assert!(r.body.contains("is already completed"));

    // Invalid JSON payload: 400 without calling the trait.
    let t0 = ui.cp.clock_ticks();
    let r = act(
        &ui,
        Via::Form,
        &runs_path(&s.running, "signal"),
        &[("name", "go"), ("payload", "{nope")],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.body.contains("payload is not valid JSON"), "{}", r.body);
    let r = act(&ui, Via::Form, &runs_path(&s.running, "signal"), &[]).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.body.contains("signal name is required"));
    assert_eq!(ui.cp.clock_ticks(), t0);
    assert!(ui.cp.signals(&s.running).is_empty());

    // Unknown run: 404.
    let r = act(&ui, Via::Form, &runs_path("run-999999", "cancel"), &[]).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn lifecycle_buttons_follow_the_run_state() {
    let (ui, s) = seeded().await;
    let has = |body: &str, id: &str, action: &str| {
        body.contains(&format!(r#"action="/ui/runs/{id}/{action}""#))
    };

    let r = get_ok(&ui, &runs_path_base(&s.completed)).await;
    for a in ["cancel", "terminate", "retry", "signal", "pause", "resume"] {
        assert!(!has(&r.body, &s.completed, a), "completed: {a}");
    }
    assert!(r.body.contains("No actions apply"));

    let r = get_ok(&ui, &runs_path_base(&s.running)).await;
    for a in ["cancel", "terminate", "signal", "pause"] {
        assert!(has(&r.body, &s.running, a), "running: {a}");
    }
    for a in ["retry", "resume"] {
        assert!(!has(&r.body, &s.running, a), "running: {a}");
    }
    // Destructive actions confirm (htmx); the plain form submits directly.
    assert!(
        r.body
            .contains(&format!(r#"hx-confirm="Cancel run {}?""#, s.running))
    );
    assert!(
        r.body
            .contains(&format!(r#"hx-confirm="Terminate run {} now?"#, s.running))
    );
    assert_eq!(r.body.matches("hx-confirm=").count(), 2, "{}", r.body);

    ui.cp
        .pause_run(s.running.clone(), None)
        .await
        .expect("pause");
    let r = get_ok(&ui, &runs_path_base(&s.running)).await;
    assert!(has(&r.body, &s.running, "resume"));
    assert!(!has(&r.body, &s.running, "pause"));

    let r = get_ok(&ui, &runs_path_base(&s.failed)).await;
    assert!(has(&r.body, &s.failed, "retry"));
    assert!(!has(&r.body, &s.failed, "cancel"));
    let timed_out = run_in(&ui, RunStatus::TimedOut).await;
    let r = get_ok(&ui, &runs_path_base(&timed_out)).await;
    assert!(has(&r.body, &timed_out, "retry"));
    let cancelled = run_in(&ui, RunStatus::Cancelled).await;
    let r = get_ok(&ui, &runs_path_base(&cancelled)).await;
    assert!(!has(&r.body, &cancelled, "retry"));
}

// ---------------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------------

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = Result<T, ApiError>> + Send + 'a>>;

/// [`FakeOps`] plus a record of every requeue / dismiss request (the fake keeps only
/// their effect).
struct RecordingOps {
    inner: Arc<FakeOps>,
    requeues: Mutex<Vec<DlqRequeueRequest>>,
    dismisses: Mutex<Vec<DlqDismissRequest>>,
}

// `OpsApi` is an `#[async_trait]` trait; ostro-ui has no async-trait dev-dependency
// (only 11-03 edits Cargo files), so the boxed signatures are spelled out.
impl OpsApi for RecordingOps {
    fn list_dlq<'a, 'b>(&'a self, req: DlqListRequest) -> BoxFut<'b, DlqPage>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.inner.list_dlq(req))
    }

    fn requeue_dlq<'a, 'b>(&'a self, req: DlqRequeueRequest) -> BoxFut<'b, DlqRequeueResponse>
    where
        'a: 'b,
        Self: 'b,
    {
        self.requeues.lock().expect("lock").push(req.clone());
        Box::pin(self.inner.requeue_dlq(req))
    }

    fn dismiss_dlq<'a, 'b>(&'a self, req: DlqDismissRequest) -> BoxFut<'b, DlqEntry>
    where
        'a: 'b,
        Self: 'b,
    {
        self.dismisses.lock().expect("lock").push(req.clone());
        Box::pin(self.inner.dismiss_dlq(req))
    }
}

fn dlq_entry(run_id: &str, resumable: bool) -> DlqEntry {
    DlqEntry {
        run_id: run_id.to_owned(),
        workflow: "billing".to_owned(),
        version: 2,
        queue: "payments".to_owned(),
        status: RunStatusDto::Failed,
        reason: "boom".to_owned(),
        failed_at_ms: 1_700_000_000_000,
        tags: Vec::new(),
        resumable,
        hook_run_id: None,
        dismissed_at_ms: None,
        dismiss_reason: None,
        requeued_to: None,
        requeued_at_ms: None,
    }
}

/// The UI over `ui.cp` with a [`RecordingOps`] around `ui.ops`.
fn recording(ui: &Ui, auth: Arc<dyn UiAuth>) -> (axum::Router, Arc<RecordingOps>) {
    let rec = Arc::new(RecordingOps {
        inner: Arc::clone(&ui.ops),
        requeues: Mutex::new(Vec::new()),
        dismisses: Mutex::new(Vec::new()),
    });
    let ext = ApiExtensions {
        ops: Some(Arc::clone(&rec) as Arc<dyn OpsApi>),
        ..ApiExtensions::default()
    };
    let router = ostro_ui::router(UiDeps {
        cp: Arc::clone(&ui.cp) as Arc<dyn ControlPlane>,
        ext,
        cfg: common::cfg(),
        auth,
    });
    (router, rec)
}

#[tokio::test]
async fn dlq_requeue_modes_call_the_trait_and_land_on_the_resulting_run() {
    let (ui, s) = seeded().await;
    for (id, resumable) in [
        ("run-dlq-a", true),
        ("run-dlq-b", false),
        ("run-dlq-c", false),
    ] {
        ui.ops.push_dlq(dlq_entry(id, resumable));
    }
    let (router, rec) = recording(&ui, Arc::new(ostro_ui::auth::AllowAll));
    let go = |via, path: String, fields: Vec<(&'static str, &'static str)>| {
        let router = router.clone();
        async move { send(&router, post(via, &path, &fields)).await }
    };

    // auto on a non-resumable entry restarts: a new run.
    let r = go(
        Via::Form,
        format!("/ui/dlq/{}/requeue", s.failed),
        vec![("mode", "auto")],
    )
    .await;
    assert_eq!(r.status, StatusCode::SEE_OTHER, "{}", r.body);
    let to = r.header("location").expect("location").to_owned();
    assert!(to.starts_with("/ui/runs/run-requeue-"), "{to}");
    assert_eq!(
        ui.ops.dlq_entry(&s.failed).and_then(|e| e.requeued_to),
        to.strip_prefix("/ui/runs/").map(str::to_owned)
    );

    // resume keeps the run id (htmx: HX-Redirect).
    let r = go(
        Via::Htmx,
        "/ui/dlq/run-dlq-a/requeue".to_owned(),
        vec![("mode", "resume")],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.header("hx-redirect"), Some("/ui/runs/run-dlq-a"));

    // rerun from a step.
    let r = go(
        Via::Form,
        "/ui/dlq/run-dlq-b/requeue".to_owned(),
        vec![("mode", "rerun"), ("step", "s3")],
    )
    .await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    // rerun without a cut: 400, no call.
    let r = go(
        Via::Form,
        "/ui/dlq/run-dlq-c/requeue".to_owned(),
        vec![("mode", "rerun")],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    let got: Vec<(String, RequeueMode, bool)> = rec
        .requeues
        .lock()
        .expect("lock")
        .iter()
        .map(|r| (r.run_id.clone(), r.mode.clone(), r.dry_run))
        .collect();
    assert_eq!(
        got,
        vec![
            (s.failed.clone(), RequeueMode::Auto, false),
            ("run-dlq-a".to_owned(), RequeueMode::Resume, false),
            (
                "run-dlq-b".to_owned(),
                RequeueMode::Rerun {
                    from: RerunFrom::Step {
                        name: "s3".to_owned(),
                        occurrence: 1
                    }
                },
                false
            ),
        ]
    );

    // A non-member: the API's 409 is shown.
    let r = go(
        Via::Form,
        "/ui/dlq/run-dlq-a/requeue".to_owned(),
        vec![("mode", "auto")],
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert!(r.body.contains("is not in the DLQ"), "{}", r.body);
}

#[tokio::test]
async fn dlq_dismiss_removes_the_row() {
    let (ui, s) = seeded().await;
    ui.ops.push_dlq(dlq_entry("run-dlq-z", false));
    let (router, rec) = recording(&ui, Arc::new(ostro_ui::auth::AllowAll));

    let page = send(&router, common::get("/ui/dlq")).await;
    assert_eq!(page.status, StatusCode::OK);
    let failed_href = format!(r#"action="/ui/dlq/{}/dismiss""#, s.failed);
    assert!(page.body.contains(&failed_href), "{}", page.body);
    assert!(
        page.body
            .contains(r#"hx-target="closest tr" hx-swap="delete""#)
    );

    // A reason is required.
    let r = send(
        &router,
        post(Via::Form, &format!("/ui/dlq/{}/dismiss", s.failed), &[]),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    let r = send(
        &router,
        post(
            Via::Form,
            &format!("/ui/dlq/{}/dismiss", s.failed),
            &[("reason", "known flake")],
        ),
    )
    .await;
    assert_303(&r, "/ui/dlq", "dismiss");
    let r = send(
        &router,
        post(Via::Htmx, "/ui/dlq/run-dlq-z/dismiss", &[("reason", "dup")]),
    )
    .await;
    assert_html(&r, "htmx dismiss");
    assert!(r.body.is_empty(), "htmx deletes the row: {}", r.body);

    let got: Vec<(String, String)> = rec
        .dismisses
        .lock()
        .expect("lock")
        .iter()
        .map(|d| (d.run_id.clone(), d.reason.clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            (s.failed.clone(), "known flake".to_owned()),
            ("run-dlq-z".to_owned(), "dup".to_owned())
        ]
    );
    assert_eq!(
        ui.ops.dlq_entry(&s.failed).and_then(|e| e.dismiss_reason),
        Some("known flake".to_owned())
    );
    let page = send(&router, common::get("/ui/dlq")).await;
    assert!(!page.body.contains(&failed_href));
    assert!(!page.body.contains("run-dlq-z"));
    assert!(page.body.contains("The dead-letter queue is empty."));
}

// ---------------------------------------------------------------------------------
// The operate scope
// ---------------------------------------------------------------------------------

/// A principal holding only `read`.
struct ReadOnly;

impl UiAuth for ReadOnly {
    fn principal(&self, _parts: &http::request::Parts) -> Result<UiPrincipal, UiReject> {
        Ok(UiPrincipal {
            scopes: vec!["read".to_owned()],
        })
    }
}

/// Every mutating route of this plan with a valid body.
fn mutations(s: &Seeded) -> Vec<(String, Vec<(&'static str, &'static str)>)> {
    let mut out: Vec<(String, Vec<(&'static str, &'static str)>)> = Vec::new();
    for a in ["pause", "resume", "cancel", "retry"] {
        out.push((runs_path(&s.running, a), vec![]));
    }
    out.push((runs_path(&s.running, "terminate"), vec![("reason", "x")]));
    out.push((runs_path(&s.running, "signal"), vec![("name", "go")]));
    out.push((
        format!("/ui/dlq/{}/requeue", s.failed),
        vec![("mode", "auto")],
    ));
    out.push((
        format!("/ui/dlq/{}/dismiss", s.failed),
        vec![("reason", "x")],
    ));
    out
}

#[tokio::test]
async fn scope_operate_is_required_on_every_mutation_while_pages_render() {
    let (ui, s) = seeded().await;
    let (router, rec) = recording(&ui, Arc::new(ReadOnly));
    for (path, fields) in mutations(&s) {
        for via in [Via::Htmx, Via::Form] {
            let t0 = ui.cp.clock_ticks();
            let r = send(&router, post(via, &path, &fields)).await;
            assert_eq!(
                r.status,
                StatusCode::FORBIDDEN,
                "{via:?} {path}: {}",
                r.body
            );
            assert!(r.body.contains("missing scope `operate`"), "{path}");
            assert_eq!(ui.cp.clock_ticks(), t0, "{path}: nothing called");
        }
    }
    assert!(rec.requeues.lock().expect("lock").is_empty());
    assert!(rec.dismisses.lock().expect("lock").is_empty());
    for path in [
        "/ui/runs".to_owned(),
        "/ui/dlq".to_owned(),
        runs_path_base(&s.running),
    ] {
        let r = send(&router, common::get(&path)).await;
        assert_eq!(r.status, StatusCode::OK, "{path}");
    }
}
