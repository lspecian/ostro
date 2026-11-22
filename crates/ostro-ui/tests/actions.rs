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
    BulkAction, BulkCreateRequest, BulkFilter, DlqDismissRequest, DlqEntry, DlqListRequest,
    DlqPage, DlqRequeueRequest, DlqRequeueResponse, OpsApi, RequeueMode, RerunCut, RerunCutsView,
    RerunFrom, RerunRequest, RerunVersion, reason, reason_error, rerun_cut_unsafe,
};
use ostro_api::rest::RunStatusDto;
use ostro_api::{ApiError, ApiExtensions, ErrorCode};
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
    out.push((runs_path(&s.completed, "rerun/preview"), vec![("seq", "2")]));
    out.push((runs_path(&s.completed, "rerun"), vec![("seq", "2")]));
    out.push(("/ui/bulk/preview".to_owned(), vec![("action", "cancel")]));
    out.push(("/ui/bulk".to_owned(), vec![("action", "cancel")]));
    out.push(("/ui/bulk/blk_0000000000000001/cancel".to_owned(), vec![]));
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
        "/ui/bulk".to_owned(),
        runs_path(&s.completed, "rerun?from_seq=2"),
    ] {
        let r = send(&router, common::get(&path)).await;
        assert_eq!(r.status, StatusCode::OK, "{path}");
    }
}

// ---------------------------------------------------------------------------------
// Rerun from a step
// ---------------------------------------------------------------------------------

/// The `<section id="rerun-preview">` of a page or fragment.
fn preview_of(body: &str) -> &str {
    let start = body
        .find(r#"<section id="rerun-preview""#)
        .expect("preview section");
    let end = body[start..].find("</section>").expect("section end") + start + 10;
    &body[start..end]
}

#[tokio::test]
async fn rerun_form_renders_the_cut_and_version_choice() {
    let (ui, s) = seeded().await;
    ui.ops.set_cuts(RerunCutsView {
        run_id: s.completed.clone(),
        cuts: vec![RerunCut {
            seq: 2,
            step: Some("charge".to_owned()),
            occurrence: 1,
            safe: true,
            reason: None,
            nearest_safe_seq: None,
        }],
    });
    let base = runs_path_base(&s.completed);
    // The timeline's "Rerun from here" link (11-15).
    let r = get_ok(&ui, &format!("{base}/rerun?from_seq=2")).await;
    assert!(
        r.body
            .contains(r#"<input name="seq" inputmode="numeric" value="2">"#),
        "{}",
        r.body
    );
    assert!(
        r.body
            .contains("seq 2: step charge (occurrence 1), a safe cut")
    );
    assert!(r.body.contains(r#"value="active" checked"#));
    assert!(
        r.body
            .contains(&format!(r#"formaction="{base}/rerun/preview""#))
    );
    assert!(r.body.contains(&format!(r#"action="{base}/rerun""#)));
    assert!(r.body.contains(&format!(r#"name="csrf" value="{TOKEN}""#)));
    assert!(r.body.contains("Preview runs the replay check"));

    let r = get_ok(
        &ui,
        &format!("{base}/rerun?step=s3&occurrence=2&version=source"),
    )
    .await;
    assert!(r.body.contains(r#"<input name="step" value="s3">"#));
    assert!(r.body.contains(r#"value="2" placeholder="1""#));
    assert!(r.body.contains(r#"value="source" checked"#));
    // A live run says "terminate first".
    let r = get_ok(&ui, &runs_path(&s.running, "rerun?seq=1")).await;
    assert!(r.body.contains("terminate it first"));
    // Unknown run: 404.
    let r = send(&ui.router, common::get("/ui/runs/run-999999/rerun")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rerun_preview_is_a_dry_run_and_renders_verdict_cut_and_copied_events() {
    let (ui, s) = seeded().await;
    let path = runs_path(&s.completed, "rerun/preview");
    let r = act(
        &ui,
        Via::Htmx,
        &path,
        &[("seq", "3"), ("step", ""), ("version", "source")],
    )
    .await;
    assert_html(&r, "htmx preview");
    assert_eq!(
        ui.ops.reruns(),
        vec![RerunRequest {
            run_id: s.completed.clone(),
            from: RerunFrom::Seq(3),
            version: RerunVersion::Source,
            idempotency_key: None,
            dry_run: true,
        }]
    );
    assert!(
        r.body.starts_with(r#"<section id="rerun-preview""#),
        "{}",
        r.body
    );
    insta::assert_snapshot!("rerun_preview_ok", r.body);

    // Without JavaScript: the whole form page with the preview.
    let r = act(
        &ui,
        Via::Form,
        &path,
        &[
            ("step", "charge"),
            ("occurrence", "2"),
            ("version", "number"),
            ("version_number", "4"),
        ],
    )
    .await;
    assert_html(&r, "form preview");
    assert!(r.body.contains("<h1>Rerun "), "full page");
    assert!(preview_of(&r.body).contains("Preview: ok"));
    assert!(
        r.body.contains(r#"value="number" checked"#),
        "form echoes the choice"
    );
    let last = ui.ops.reruns().pop().expect("rerun");
    assert_eq!(
        last.from,
        RerunFrom::Step {
            name: "charge".to_owned(),
            occurrence: 2
        }
    );
    assert_eq!(last.version, RerunVersion::Version(4));
    assert!(last.dry_run);

    // No cut, bad version: 400 without a call.
    for fields in [
        vec![("version", "active")],
        vec![("seq", "x")],
        vec![("seq", "2"), ("version", "number")],
        vec![("step", "a"), ("occurrence", "0")],
    ] {
        let r = act(&ui, Via::Form, &path, &fields).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{fields:?}: {}", r.body);
    }
    assert_eq!(ui.ops.reruns().len(), 2);
}

#[tokio::test]
async fn rerun_submit_redirects_to_the_new_run() {
    let (ui, s) = seeded().await;
    let path = runs_path(&s.completed, "rerun");
    let r = act(&ui, Via::Form, &path, &[("seq", "2")]).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER, "{}", r.body);
    let to = r.header("location").expect("location");
    assert!(to.starts_with("/ui/runs/run-rerun-"), "{to}");
    let r = act(&ui, Via::Htmx, &path, &[("step", "s3")]).await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(
        r.header("hx-redirect")
            .expect("hx-redirect")
            .starts_with("/ui/runs/run-rerun-")
    );
    let reruns = ui.ops.reruns();
    assert_eq!(reruns.len(), 2);
    assert!(
        reruns
            .iter()
            .all(|r| !r.dry_run && r.version == RerunVersion::Active)
    );
}

#[tokio::test]
async fn rerun_refusals_render_their_details() {
    let (ui, s) = seeded().await;
    let base = runs_path_base(&s.completed);
    let preview = runs_path(&s.completed, "rerun/preview");

    ui.ops.fail(
        "rerun_run",
        Some(rerun_cut_unsafe(
            5,
            "inside an uncommitted effect <x>",
            Some(3),
        )),
    );
    let r = act(
        &ui,
        Via::Htmx,
        &preview,
        &[("seq", "5"), ("version", "source")],
    )
    .await;
    assert_html(&r, "unsafe htmx");
    assert!(
        r.body.contains("Refused: rerun_cut_unsafe (409)"),
        "{}",
        r.body
    );
    assert!(
        r.body.contains("inside an uncommitted effect &#60;x&#62;") || r.body.contains("&lt;x&gt;")
    );
    assert!(
        r.body.contains(&format!(
            r#"href="{base}/rerun?seq=3&#38;version=source">Use the nearest safe step (seq 3)"#
        )),
        "{}",
        r.body
    );
    insta::assert_snapshot!("rerun_preview_unsafe", r.body);
    let r = act(&ui, Via::Form, &preview, &[("seq", "5")]).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert!(preview_of(&r.body).contains("Use the nearest safe step (seq 3)"));
    // Submitting is refused the same way; htmx retargets the preview region.
    let r = act(
        &ui,
        Via::Htmx,
        &runs_path(&s.completed, "rerun"),
        &[("seq", "5")],
    )
    .await;
    assert_html(&r, "unsafe submit");
    assert_eq!(r.header("hx-retarget"), Some("#rerun-preview"));
    assert_eq!(r.header("hx-reswap"), Some("outerHTML"));
    assert!(r.body.contains("rerun_cut_unsafe"));

    ui.ops.fail(
        "rerun_run",
        Some(reason_error(
            ErrorCode::Conflict,
            reason::RERUN_NONDETERMINISTIC,
            "the copied prefix does not replay on version 2",
            serde_json::json!({"seq": 7, "detail": "activity order changed"}),
        )),
    );
    let r = act(&ui, Via::Htmx, &preview, &[("seq", "9")]).await;
    assert!(r.body.contains("Refused: rerun_nondeterministic (409)"));
    assert!(
        r.body.contains("At seq 7: activity order changed"),
        "{}",
        r.body
    );

    ui.ops.fail(
        "rerun_run",
        Some(ApiError::run_not_terminal(&s.running, RunStatus::Running)),
    );
    let r = act(&ui, Via::Htmx, &preview, &[("seq", "1")]).await;
    assert!(r.body.contains("Refused: run_not_terminal (409)"));
    assert!(
        r.body
            .contains(&format!(r#"<a href="{base}">terminate first</a>"#)),
        "{}",
        r.body
    );

    // A server without the rerun implementation: the API's 501 is shown as is.
    ui.ops.fail(
        "rerun_run",
        Some(ApiError::new(
            ErrorCode::Unimplemented,
            "rerun_run lands in 11-10",
        )),
    );
    let r = act(&ui, Via::Form, &preview, &[("seq", "1")]).await;
    assert_eq!(r.status, StatusCode::NOT_IMPLEMENTED);
    assert!(preview_of(&r.body).contains("rerun_run lands in 11-10"));
    assert!(preview_of(&r.body).contains("(501)"));
}

// ---------------------------------------------------------------------------------
// Bulk jobs
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn bulk_link_carries_the_run_list_filter_into_the_form() {
    let (ui, _s) = seeded().await;
    let r = get_ok(
        &ui,
        "/ui/runs?workflow=greet&status=failed&tags=env%3Dprod&order=newest",
    )
    .await;
    assert!(
        r.body.contains(
            r#"href="/ui/bulk?workflow=greet&#38;status=failed&#38;tags=env%3Dprod">Bulk…</a>"#
        ),
        "{}",
        r.body
    );
    let r = get_ok(&ui, "/ui/bulk?workflow=greet&status=failed&tags=env%3Dprod").await;
    assert!(r.body.contains(r#"<input name="workflow" value="greet">"#));
    assert!(
        r.body
            .contains(r#"<option value="failed" selected>failed</option>"#)
    );
    assert!(r.body.contains(r#"<input name="tags" value="env=prod">"#));
    assert!(r.body.contains(r#"action="/ui/bulk/preview""#));
    assert!(r.body.contains("No bulk jobs."));
}

fn bulk_fields() -> Vec<(&'static str, &'static str)> {
    vec![
        ("workflow", "greet"),
        ("status", "failed"),
        ("tags", "env=prod"),
        ("tags", ""),
        ("action", "terminate"),
        ("reason", "cleanup"),
        ("rate", "5"),
        ("payload", ""),
    ]
}

#[tokio::test]
async fn bulk_preview_is_a_dry_run_with_estimate_sample_and_confirm() {
    let (ui, s) = seeded().await;
    ui.ops
        .set_bulk_matches(vec![s.failed.clone(), s.running.clone()]);
    let r = act(&ui, Via::Form, "/ui/bulk/preview", &bulk_fields()).await;
    assert_html(&r, "bulk preview");
    assert_eq!(
        ui.ops.bulk_requests(),
        vec![BulkCreateRequest {
            action: BulkAction::Terminate {
                reason: "cleanup".to_owned()
            },
            filter: BulkFilter {
                workflow: Some("greet".to_owned()),
                status: Some(RunStatusDto::Failed),
                tags: vec!["env=prod".to_owned()],
                ..BulkFilter::default()
            },
            rate_per_sec: Some(5),
            max_in_flight: None,
            dry_run: true,
        }]
    );
    assert!(
        r.body.contains("matches about <strong>2</strong> runs"),
        "{}",
        r.body
    );
    assert!(r.body.contains(&format!(
        r#"<a class="mono" href="/ui/runs/{0}">{0}</a>"#,
        s.failed
    )));
    assert!(
        r.body
            .contains(r#"hx-confirm="Apply terminate to about 2 runs?""#)
    );
    for (k, v) in [
        ("workflow", "greet"),
        ("tags", "env=prod"),
        ("reason", "cleanup"),
        ("rate", "5"),
    ] {
        assert!(
            r.body
                .contains(&format!(r#"<input type="hidden" name="{k}" value="{v}">"#)),
            "{k}"
        );
    }
    assert!(
        !r.body.contains(r#"type="hidden" name="payload""#),
        "empty fields dropped"
    );
    assert!(
        r.body
            .contains(r#"href="/ui/bulk?workflow=greet&#38;status=failed"#),
        "edit link"
    );
    assert!(
        ui.ops.list_bulk().await.expect("jobs").is_empty(),
        "nothing created"
    );
}

#[tokio::test]
async fn bulk_actions_parse_into_requests_and_bad_forms_are_400() {
    let (ui, _s) = seeded().await;
    let cases: Vec<(Vec<(&str, &str)>, BulkAction)> = vec![
        (vec![("action", "cancel")], BulkAction::Cancel),
        (vec![("action", "retry")], BulkAction::Retry),
        (vec![("action", "pause")], BulkAction::Pause),
        (vec![("action", "resume")], BulkAction::Resume),
        (
            vec![("action", "rerun"), ("step", "s3"), ("version", "source")],
            BulkAction::Rerun {
                from: RerunFrom::Step {
                    name: "s3".to_owned(),
                    occurrence: 1,
                },
                version: RerunVersion::Source,
            },
        ),
        (
            vec![("action", "requeue"), ("mode", "rerun"), ("seq", "4")],
            BulkAction::Requeue {
                mode: RequeueMode::Rerun {
                    from: RerunFrom::Seq(4),
                },
            },
        ),
        (
            vec![("action", "requeue")],
            BulkAction::Requeue {
                mode: RequeueMode::Auto,
            },
        ),
        (
            vec![
                ("action", "signal"),
                ("signal_name", "go"),
                ("payload", "[1,2]"),
            ],
            BulkAction::Signal {
                name: "go".to_owned(),
                payload: serde_json::json!([1, 2]),
            },
        ),
    ];
    for (fields, want) in &cases {
        let mut f = fields.clone();
        f.push(("dlq", "true"));
        let r = act(&ui, Via::Form, "/ui/bulk/preview", &f).await;
        assert_html(&r, &format!("{fields:?}"));
        let got = ui.ops.bulk_requests().pop().expect("request");
        assert_eq!(&got.action, want, "{fields:?}");
        assert!(got.filter.dlq);
        assert_eq!(got.rate_per_sec, None);
    }
    for bad in [
        vec![],
        vec![("action", "explode")],
        vec![("action", "terminate")],
        vec![("action", "signal")],
        vec![
            ("action", "signal"),
            ("signal_name", "go"),
            ("payload", "{"),
        ],
        vec![("action", "rerun")],
        vec![("action", "cancel"), ("rate", "0")],
        vec![("action", "cancel"), ("status", "nope")],
    ] {
        let r = act(&ui, Via::Form, "/ui/bulk/preview", &bad).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{bad:?}: {}", r.body);
    }
    assert_eq!(
        ui.ops.bulk_requests().len(),
        cases.len(),
        "bad forms call nothing"
    );
}

#[tokio::test]
async fn bulk_create_progress_polls_until_done_and_cancel() {
    let (ui, s) = seeded().await;
    ui.ops
        .set_bulk_matches(vec![s.failed.clone(), s.running.clone()]);
    let r = act(&ui, Via::Form, "/ui/bulk", &bulk_fields()).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER, "{}", r.body);
    let page = r.header("location").expect("location").to_owned();
    assert!(page.starts_with("/ui/bulk/blk_"), "{page}");
    let job = page.trim_start_matches("/ui/bulk/").to_owned();
    let req = ui.ops.bulk_requests().pop().expect("request");
    assert!(!req.dry_run);
    assert_eq!(req.rate_per_sec, Some(5));

    let r = get_ok(&ui, &page).await;
    assert!(
        r.body.contains(&format!(
            r#"hx-get="{page}/progress" hx-trigger="every 2s" hx-swap="outerHTML""#
        )),
        "{}",
        r.body
    );
    assert!(r.body.contains(r#"<progress max="2" value="0">"#));
    assert!(
        r.body
            .contains(&format!(r#"hx-confirm="Cancel bulk job {job}?"#))
    );

    // The jobs list.
    let r = get_ok(&ui, "/ui/bulk").await;
    assert!(r.body.contains(&format!(r#"<a href="{page}">{job}</a>"#)));
    assert!(r.body.contains("0 done, 0 skipped of 2"));

    // Progress: still running, then done (polling stops, no cancel).
    ui.ops.set_bulk_progress(&job, 1, 0);
    let r = get_ok(&ui, &format!("{page}/progress")).await;
    assert!(r.body.starts_with(r#"<section id="bulk-progress""#));
    assert!(r.body.contains("every 2s") && r.body.contains(r#"value="1""#));
    ui.ops.set_bulk_progress(&job, 1, 1);
    let r = get_ok(&ui, &format!("{page}/progress")).await;
    assert!(r.body.contains("badge done"));
    assert!(!r.body.contains("hx-trigger"), "{}", r.body);
    assert!(!r.body.contains("/cancel"));

    // htmx create + cancel: HX-Redirect, then the progress fragment, cancelled.
    let r = act(&ui, Via::Htmx, "/ui/bulk", &[("action", "pause")]).await;
    let to = r.header("hx-redirect").expect("hx-redirect").to_owned();
    let r = act(&ui, Via::Htmx, &format!("{to}/cancel"), &[]).await;
    assert_html(&r, "htmx cancel");
    assert!(r.body.contains("badge cancelled"));
    assert!(!r.body.contains("hx-trigger"));
    // A plain form cancel lands on the job page.
    let r = act(&ui, Via::Form, &format!("{to}/cancel"), &[]).await;
    assert_303(&r, &to, "form cancel");
    // Unknown job: 404 page.
    let r = send(&ui.router, common::get("/ui/bulk/blk_nope")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bulk_api_errors_are_surfaced() {
    let (ui, _s) = seeded().await;
    // A server whose bulk API is not implemented yet (11-13): 501 shown as is.
    ui.ops.fail(
        "create_bulk",
        Some(ApiError::new(
            ErrorCode::Unimplemented,
            "create_bulk lands in 11-13",
        )),
    );
    ui.ops.fail(
        "list_bulk",
        Some(ApiError::new(
            ErrorCode::Unimplemented,
            "list_bulk lands in 11-13",
        )),
    );
    let r = act(&ui, Via::Form, "/ui/bulk/preview", &[("action", "cancel")]).await;
    assert_eq!(r.status, StatusCode::NOT_IMPLEMENTED);
    assert!(r.body.contains("create_bulk lands in 11-13"));
    let r = act(&ui, Via::Htmx, "/ui/bulk", &[("action", "cancel")]).await;
    assert_eq!(r.header("hx-retarget"), Some("#flash"));
    assert!(r.body.contains("create_bulk lands in 11-13"));
    let r = get_ok(&ui, "/ui/bulk").await;
    assert!(
        r.body
            .contains("Bulk jobs unavailable (501): list_bulk lands in 11-13")
    );
    // Too many jobs: the API's 429.
    ui.ops.fail(
        "create_bulk",
        Some(ostro_api::phase11::bulk_too_many_jobs(4, 1000)),
    );
    let r = act(&ui, Via::Form, "/ui/bulk", &[("action", "cancel")]).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    // No ops slot at all.
    let bare = common::ui_with(common::Slots {
        ops: false,
        logs: true,
    });
    let r = get_ok(&bare, "/ui/bulk").await;
    assert!(
        r.body
            .contains("Bulk operations are not available on this server.")
    );
}
