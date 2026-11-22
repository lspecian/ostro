//! Mutating `/ui` actions (plan 11-16, UI-02, D-P11-15/16): run lifecycle (cancel,
//! terminate, retry, signal, pause, resume), rerun-from-step with a dry-run preview,
//! DLQ requeue/dismiss, and bulk jobs.
//!
//! Every mutating handler takes [`Operator`] (the `operate` scope, Phase 14 seam) and,
//! last, [`SameOriginMutation`] (Host + same-origin + CSRF double submit). Each action
//! is a plain HTML form with a `csrf` field (works without JavaScript: 303 to the
//! result page) that htmx enhances (`hx-post`, header token): htmx requests get a
//! fragment, or `HX-Redirect` when the result is another page. Destructive actions
//! (cancel, terminate, bulk create and bulk cancel) carry `hx-confirm`; without
//! JavaScript the form submits directly (the terminate reason field is the deliberate
//! step).
//!
//! Errors: a plain form post renders the error page with the API's status; an htmx
//! request gets `200` with the `#flash` region (`HX-Retarget: #flash`), because htmx
//! does not swap 4xx/5xx responses. Rerun refusals render their details in the preview
//! region instead.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use http::header::HeaderMap;
use http::{HeaderValue, StatusCode, Uri};
use ostro_api::ApiError;
use ostro_api::domain::{
    CancelRequest, RetryRequest, RunInfo, RunStatus, SignalRequest, TerminateRequest,
};
use ostro_api::phase11::{
    BulkAction, BulkCreateRequest, BulkFilter, BulkJobView, DlqDismissRequest, DlqRequeueRequest,
    RequeueMode, RerunFrom, RerunRequest, RerunResponse, RerunVersion, reason,
};
use ostro_api::rest::RunStatusDto;

use crate::auth::{Operator, Reader};
use crate::pages::{
    Params, StatusFragment, StatusView, UiError, UiState, fmt_duration, fmt_ts, href, href_encode,
    page_response, param, params, parse_run_filter, run_href,
};
use crate::security::{self, SameOriginMutation};

/// The action routes (merged by [`crate::router`]).
pub(crate) fn routes() -> axum::Router<UiState> {
    axum::Router::new()
        .route("/ui/runs/{run_id}/cancel", post(cancel))
        .route("/ui/runs/{run_id}/terminate", post(terminate))
        .route("/ui/runs/{run_id}/retry", post(retry))
        .route("/ui/runs/{run_id}/signal", post(signal))
        .route("/ui/runs/{run_id}/pause", post(pause))
        .route("/ui/runs/{run_id}/resume", post(resume))
        .route("/ui/dlq/{run_id}/requeue", post(dlq_requeue))
        .route("/ui/dlq/{run_id}/dismiss", post(dlq_dismiss))
        .route(
            "/ui/runs/{run_id}/rerun",
            get(rerun_form).post(rerun_submit),
        )
        .route("/ui/runs/{run_id}/rerun/preview", post(rerun_preview))
        .route("/ui/bulk", get(bulk_page).post(bulk_create))
        .route("/ui/bulk/preview", post(bulk_preview))
        .route("/ui/bulk/{job_id}", get(bulk_job))
        .route("/ui/bulk/{job_id}/progress", get(bulk_progress))
        .route("/ui/bulk/{job_id}/cancel", post(bulk_cancel))
}

// ---------------------------------------------------------------------------------
// Shared response helpers
// ---------------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "ui/_flash.html")]
struct FlashFragment {
    message: String,
}

fn hx_header(resp: &mut Response, name: &'static str, value: &str) {
    if let Ok(v) = HeaderValue::from_str(value) {
        resp.headers_mut().insert(name, v);
    }
}

/// An action failure: the error page (plain form) or the `#flash` region (htmx).
fn fail(m: &SameOriginMutation, e: UiError) -> Response {
    if !m.is_htmx() {
        return e.into_response();
    }
    let message = e.message();
    let html = FlashFragment { message }
        .render()
        .unwrap_or_else(|_| "<div id=\"flash\" class=\"flash\">error</div>".to_owned());
    let mut resp = Html(html).into_response();
    hx_header(&mut resp, "hx-retarget", "#flash");
    hx_header(&mut resp, "hx-reswap", "outerHTML");
    resp
}

/// Navigate to `location`: 303 for a plain form, `HX-Redirect` for htmx.
fn go(m: &SameOriginMutation, location: &str) -> Response {
    if m.is_htmx() {
        let mut resp = StatusCode::OK.into_response();
        hx_header(&mut resp, "hx-redirect", location);
        resp
    } else {
        Redirect::to(location).into_response()
    }
}

/// The CSRF token of a validated mutation (its `ostro_csrf` cookie).
fn csrf_of(headers: &HeaderMap) -> String {
    security::cookie_token(headers).unwrap_or_default()
}

/// Trimmed, non-empty form field.
fn field<'a>(m: &'a SameOriginMutation, name: &str) -> Option<&'a str> {
    m.field(name).map(str::trim).filter(|v| !v.is_empty())
}

fn ops(st: &UiState) -> Result<&std::sync::Arc<dyn ostro_api::phase11::OpsApi>, UiError> {
    st.deps.ext.ops.as_ref().ok_or_else(|| {
        UiError::NotFound("run operations are not available on this server".to_owned())
    })
}

fn parse_json(name: &str, raw: Option<&str>) -> Result<serde_json::Value, UiError> {
    match raw {
        None => Ok(serde_json::Value::Null),
        Some(s) => serde_json::from_str(s)
            .map_err(|e| UiError::BadRequest(format!("{name} is not valid JSON: {e}"))),
    }
}

// ---------------------------------------------------------------------------------
// Run lifecycle
// ---------------------------------------------------------------------------------

/// Which lifecycle buttons a run shows (`_run_actions.html`).
pub(crate) struct ActionsView {
    pub(crate) base: String,
    pub(crate) run_id: String,
    pub(crate) can_cancel: bool,
    pub(crate) can_terminate: bool,
    pub(crate) can_retry: bool,
    pub(crate) can_signal: bool,
    pub(crate) can_pause: bool,
    pub(crate) can_resume: bool,
}

impl ActionsView {
    pub(crate) fn of(run: &RunInfo) -> Self {
        let live = !run.status.is_terminal();
        let paused = run.pause.is_some();
        ActionsView {
            base: run_href(&run.run_id),
            run_id: run.run_id.clone(),
            can_cancel: live,
            can_terminate: live,
            // retry_run resumes or restarts a terminal run that did not succeed; a paused
            // (live) run is continued with Resume.
            can_retry: matches!(run.status, RunStatus::Failed | RunStatus::TimedOut),
            can_signal: live,
            can_pause: live && !paused,
            can_resume: live && paused,
        }
    }

    pub(crate) fn any(&self) -> bool {
        self.can_cancel
            || self.can_terminate
            || self.can_retry
            || self.can_signal
            || self.can_pause
            || self.can_resume
    }
}

/// The htmx answer of a lifecycle action: the status card plus the action buttons
/// (out of band).
#[derive(Template)]
#[template(path = "ui/_run_actions.html")]
struct ActionsFragment {
    actions: ActionsView,
    csrf: String,
    oob: bool,
}

/// After a lifecycle action on `run_id`: the status fragment (+ OOB actions) for htmx,
/// 303 to the run page for a plain form.
async fn respond(
    st: &UiState,
    m: &SameOriginMutation,
    headers: &HeaderMap,
    run_id: &str,
) -> Response {
    if !m.is_htmx() {
        return Redirect::to(&run_href(run_id)).into_response();
    }
    let run = match st.deps.cp.get_run(run_id.to_owned()).await {
        Ok(r) => r,
        Err(e) => return fail(m, e.into()),
    };
    let status = StatusFragment {
        status: StatusView::of(&run),
    }
    .render();
    let actions = ActionsFragment {
        actions: ActionsView::of(&run),
        csrf: csrf_of(headers),
        oob: true,
    }
    .render();
    match (status, actions) {
        (Ok(s), Ok(a)) => Html(format!("{s}\n{a}")).into_response(),
        (Err(e), _) | (_, Err(e)) => fail(m, e.into()),
    }
}

async fn finish<T>(
    st: &UiState,
    m: &SameOriginMutation,
    headers: &HeaderMap,
    run_id: &str,
    r: Result<T, UiError>,
) -> Response {
    match r {
        Ok(_) => respond(st, m, headers, run_id).await,
        Err(e) => fail(m, e),
    }
}

/// `POST /ui/runs/{run_id}/cancel` (optional `reason`).
async fn cancel(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let r = st
        .deps
        .cp
        .cancel_run(CancelRequest {
            run_id: run_id.clone(),
            reason: field(&m, "reason").map(str::to_owned),
            dry_run: false,
        })
        .await
        .map_err(UiError::from);
    finish(&st, &m, &headers, &run_id, r).await
}

/// `POST /ui/runs/{run_id}/terminate` (form `reason`).
async fn terminate(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let r = st
        .deps
        .cp
        .terminate_run(TerminateRequest {
            run_id: run_id.clone(),
            reason: field(&m, "reason").map(str::to_owned),
            dry_run: false,
        })
        .await
        .map_err(UiError::from);
    finish(&st, &m, &headers, &run_id, r).await
}

/// `POST /ui/runs/{run_id}/retry`: a restart lands on the new run.
async fn retry(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let r = st
        .deps
        .cp
        .retry_run(RetryRequest {
            run_id: run_id.clone(),
            dry_run: false,
        })
        .await;
    match r {
        Ok(res) if res.run_id != run_id => go(&m, &run_href(&res.run_id)),
        Ok(_) => respond(&st, &m, &headers, &run_id).await,
        Err(e) => fail(&m, e.into()),
    }
}

/// `POST /ui/runs/{run_id}/signal` (form `name`, `payload` JSON; empty = `null`).
async fn signal(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let Some(name) = field(&m, "name").map(str::to_owned) else {
        return fail(
            &m,
            UiError::BadRequest("signal name is required".to_owned()),
        );
    };
    let payload = match parse_json("payload", field(&m, "payload")) {
        Ok(p) => p,
        Err(e) => return fail(&m, e),
    };
    let r = st
        .deps
        .cp
        .signal_run(SignalRequest {
            run_id: run_id.clone(),
            name,
            payload,
            request_id: None,
            dry_run: false,
        })
        .await
        .map_err(UiError::from);
    finish(&st, &m, &headers, &run_id, r).await
}

/// `POST /ui/runs/{run_id}/pause` (optional `detail`).
async fn pause(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let r = st
        .deps
        .cp
        .pause_run(run_id.clone(), field(&m, "detail").map(str::to_owned))
        .await
        .map_err(UiError::from);
    finish(&st, &m, &headers, &run_id, r).await
}

/// `POST /ui/runs/{run_id}/resume`.
async fn resume(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let r = st
        .deps
        .cp
        .resume_run(run_id.clone())
        .await
        .map_err(UiError::from);
    finish(&st, &m, &headers, &run_id, r).await
}

// ---------------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------------

/// The cut named by `seq`, or `step` + `occurrence` (default 1); `None` when neither.
fn parse_cut(q: &Params) -> Result<Option<RerunFrom>, UiError> {
    if let Some(s) = param(q, "seq") {
        let seq = s
            .parse::<u64>()
            .map_err(|_| UiError::BadRequest(format!("seq must be a number, got {s}")))?;
        return Ok(Some(RerunFrom::Seq(seq)));
    }
    if let Some(name) = param(q, "step") {
        let occurrence =
            match param(q, "occurrence") {
                Some(o) => o.parse::<u32>().ok().filter(|o| *o >= 1).ok_or_else(|| {
                    UiError::BadRequest(format!("occurrence must be >= 1, got {o}"))
                })?,
                None => 1,
            };
        return Ok(Some(RerunFrom::Step {
            name: name.to_owned(),
            occurrence,
        }));
    }
    Ok(None)
}

/// The requeue `mode` field (default `auto`); `rerun` needs a cut.
fn parse_requeue_mode(q: &Params) -> Result<RequeueMode, UiError> {
    match param(q, "mode").unwrap_or("auto") {
        "auto" => Ok(RequeueMode::Auto),
        "resume" => Ok(RequeueMode::Resume),
        "restart" => Ok(RequeueMode::Restart),
        "rerun" => match parse_cut(q)? {
            Some(from) => Ok(RequeueMode::Rerun { from }),
            None => Err(UiError::BadRequest(
                "requeue mode rerun needs a step or seq".to_owned(),
            )),
        },
        other => Err(UiError::BadRequest(format!("unknown requeue mode {other}"))),
    }
}

/// The form fields of a mutation as query-style pairs.
fn fields_of(m: &SameOriginMutation) -> Params {
    m.fields().to_vec()
}

/// `POST /ui/dlq/{run_id}/requeue` (form `mode` = auto|resume|restart|rerun, `step` /
/// `occurrence` / `seq` for rerun): lands on the resulting run.
async fn dlq_requeue(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    m: SameOriginMutation,
) -> Response {
    let mode = match parse_requeue_mode(&fields_of(&m)) {
        Ok(mode) => mode,
        Err(e) => return fail(&m, e),
    };
    let ops = match ops(&st) {
        Ok(o) => o,
        Err(e) => return fail(&m, e),
    };
    match ops
        .requeue_dlq(DlqRequeueRequest {
            run_id: run_id.clone(),
            mode,
            dry_run: false,
        })
        .await
    {
        Ok(res) => go(
            &m,
            &run_href(res.new_run_id.as_deref().unwrap_or(&res.run_id)),
        ),
        Err(e) => fail(&m, e.into()),
    }
}

/// `POST /ui/dlq/{run_id}/dismiss` (form `reason`): htmx removes the row (empty 200,
/// `hx-swap="delete"`); a plain form goes back to the DLQ tab.
async fn dlq_dismiss(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    m: SameOriginMutation,
) -> Response {
    let Some(reason) = field(&m, "reason").map(str::to_owned) else {
        return fail(
            &m,
            UiError::BadRequest("a dismiss reason is required".to_owned()),
        );
    };
    let ops = match ops(&st) {
        Ok(o) => o,
        Err(e) => return fail(&m, e),
    };
    match ops.dismiss_dlq(DlqDismissRequest { run_id, reason }).await {
        Ok(_) if m.is_htmx() => Html(String::new()).into_response(),
        Ok(_) => Redirect::to("/ui/dlq").into_response(),
        Err(e) => fail(&m, e.into()),
    }
}

// ---------------------------------------------------------------------------------
// Rerun from a step
// ---------------------------------------------------------------------------------

/// `version` = `active` | `source` | `number` (+ `version_number`).
fn parse_version(q: &Params) -> Result<RerunVersion, UiError> {
    match param(q, "version").unwrap_or("active") {
        "active" => Ok(RerunVersion::Active),
        "source" => Ok(RerunVersion::Source),
        "number" => {
            let n = param(q, "version_number").unwrap_or_default();
            n.parse::<u32>()
                .ok()
                .filter(|v| *v >= 1)
                .map(RerunVersion::Version)
                .ok_or_else(|| {
                    UiError::BadRequest(format!("version number must be >= 1, got {n:?}"))
                })
        }
        other => Err(UiError::BadRequest(format!(
            "unknown version choice {other}"
        ))),
    }
}

/// The rerun form's echo of its fields.
pub(crate) struct RerunFormView {
    base: String,
    run_id: String,
    workflow: String,
    version: u32,
    status: &'static str,
    terminal: bool,
    seq: String,
    step: String,
    occurrence: String,
    version_choice: String,
    version_number: String,
    cut_note: Option<String>,
}

impl RerunFormView {
    fn new(run: &RunInfo, q: &Params) -> Self {
        let seq = param(q, "seq")
            .or_else(|| param(q, "from_seq"))
            .unwrap_or_default();
        RerunFormView {
            base: run_href(&run.run_id),
            run_id: run.run_id.clone(),
            workflow: run.workflow.clone(),
            version: run.version,
            status: run.status.as_str(),
            terminal: run.status.is_terminal(),
            seq: seq.to_owned(),
            step: param(q, "step").unwrap_or_default().to_owned(),
            occurrence: param(q, "occurrence").unwrap_or_default().to_owned(),
            version_choice: param(q, "version").unwrap_or("active").to_owned(),
            version_number: param(q, "version_number").unwrap_or_default().to_owned(),
            cut_note: None,
        }
    }
}

/// A successful (dry-run) rerun.
pub(crate) struct RerunOk {
    verdict: String,
    cut_seq: u64,
    copied_events: u64,
    workflow: String,
    version: u32,
}

/// A rerun the API refused, with what the operator can do about it.
pub(crate) struct Refusal {
    status: u16,
    code: String,
    message: String,
    seq: Option<u64>,
    detail: Option<String>,
    nearest: Option<(u64, String)>,
    terminate_href: Option<String>,
}

/// The preview region (`_rerun_preview.html`).
#[derive(Default)]
pub(crate) struct PreviewView {
    ok: Option<RerunOk>,
    refusal: Option<Refusal>,
}

impl PreviewView {
    fn ok(r: RerunResponse) -> Self {
        PreviewView {
            ok: Some(RerunOk {
                verdict: r.verdict,
                cut_seq: r.cut_seq,
                copied_events: r.copied_events,
                workflow: r.workflow,
                version: r.version,
            }),
            refusal: None,
        }
    }

    fn refused(e: &ApiError, run_id: &str, q: &Params) -> Self {
        let d = e.details.as_ref();
        let why = d.and_then(|d| d.get("reason")).and_then(|v| v.as_str());
        let num = |k: &str| d.and_then(|d| d.get(k)).and_then(serde_json::Value::as_u64);
        let text = |k: &str| {
            d.and_then(|d| d.get(k))
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        };
        let base = run_href(run_id);
        let mut r = Refusal {
            status: e.code.http_status().as_u16(),
            code: why.unwrap_or(e.code.as_str()).to_owned(),
            message: e.message.clone(),
            seq: None,
            detail: None,
            nearest: None,
            terminate_href: None,
        };
        match why {
            Some(reason::RERUN_CUT_UNSAFE) => {
                r.seq = num("seq");
                r.detail = text("detail");
                r.nearest = num("nearest_safe_seq").map(|n| {
                    let n_s = n.to_string();
                    let mut pairs: Vec<(&str, &str)> = vec![("seq", &n_s)];
                    for k in ["version", "version_number"] {
                        if let Some(v) = param(q, k) {
                            pairs.push((k, v));
                        }
                    }
                    (n, href(&format!("{base}/rerun"), &pairs))
                });
            }
            Some(reason::RERUN_NONDETERMINISTIC) => {
                r.seq = num("seq");
                r.detail = text("detail");
            }
            Some(reason::RUN_NOT_TERMINAL) => r.terminate_href = Some(base),
            _ => {}
        }
        PreviewView {
            ok: None,
            refusal: Some(r),
        }
    }
}

#[derive(Template)]
#[template(path = "ui/rerun.html")]
struct RerunPage {
    title: String,
    csrf: String,
    nav: &'static str,
    form: RerunFormView,
    preview: PreviewView,
}

#[derive(Template)]
#[template(path = "ui/_rerun_preview.html")]
struct PreviewFragment {
    preview: PreviewView,
}

/// A note on the selected cut from `OpsApi::analyze_rerun` (best effort).
async fn cut_note(st: &UiState, form: &RerunFormView) -> Option<String> {
    let seq: u64 = form.seq.parse().ok()?;
    let ops = st.deps.ext.ops.as_ref()?;
    let cuts = ops.analyze_rerun(form.run_id.clone()).await.ok()?;
    let c = cuts.cuts.into_iter().find(|c| c.seq == seq)?;
    let step = c
        .step
        .map(|s| format!("step {s} (occurrence {})", c.occurrence))
        .unwrap_or_else(|| "no step".to_owned());
    Some(if c.safe {
        format!("seq {seq}: {step}, a safe cut")
    } else {
        format!(
            "seq {seq}: {step}, not a safe cut: {}",
            c.reason.unwrap_or_default()
        )
    })
}

async fn render_rerun_page(
    st: &UiState,
    run_id: &str,
    q: &Params,
    csrf: String,
    preview: PreviewView,
) -> Result<String, UiError> {
    let run = st.deps.cp.get_run(run_id.to_owned()).await?;
    let mut form = RerunFormView::new(&run, q);
    form.cut_note = cut_note(st, &form).await;
    Ok(RerunPage {
        title: format!("Rerun {}", run.run_id),
        csrf,
        nav: "runs",
        form,
        preview,
    }
    .render()?)
}

/// `GET /ui/runs/{run_id}/rerun?seq=N | from_seq=N | step=S&occurrence=K`: the form.
async fn rerun_form(
    State(st): State<UiState>,
    _reader: Reader,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<Params>,
) -> Result<Response, UiError> {
    if st.deps.ext.ops.is_none() {
        return Err(UiError::NotFound(
            "rerun is not available on this server".to_owned(),
        ));
    }
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    let html = render_rerun_page(&st, &run_id, &q, csrf, PreviewView::default()).await?;
    Ok(page_response(html, set_cookie))
}

fn rerun_request(run_id: &str, q: &Params, dry_run: bool) -> Result<RerunRequest, UiError> {
    let from = parse_cut(q)?.ok_or_else(|| {
        UiError::BadRequest("choose a cut: a step (and occurrence) or a seq".to_owned())
    })?;
    Ok(RerunRequest {
        run_id: run_id.to_owned(),
        from,
        version: parse_version(q)?,
        idempotency_key: None,
        dry_run,
    })
}

/// A refused rerun: the preview fragment (htmx, retargeted) or the rerun page with the
/// API's status (plain form).
async fn refused(
    st: &UiState,
    m: &SameOriginMutation,
    headers: &HeaderMap,
    run_id: &str,
    q: &Params,
    e: &ApiError,
) -> Response {
    let preview = PreviewView::refused(e, run_id, q);
    if m.is_htmx() {
        let mut resp = match (PreviewFragment { preview }).render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => return fail(m, err.into()),
        };
        hx_header(&mut resp, "hx-retarget", "#rerun-preview");
        hx_header(&mut resp, "hx-reswap", "outerHTML");
        return resp;
    }
    match render_rerun_page(st, run_id, q, csrf_of(headers), preview).await {
        Ok(html) => (e.code.http_status(), Html(html)).into_response(),
        Err(err) => fail(m, err),
    }
}

/// `POST /ui/runs/{run_id}/rerun/preview`: `rerun_run{dry_run: true}`.
async fn rerun_preview(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let q = fields_of(&m);
    let (ops, req) = match ops(&st).and_then(|o| Ok((o, rerun_request(&run_id, &q, true)?))) {
        Ok(v) => v,
        Err(e) => return fail(&m, e),
    };
    match ops.rerun_run(req).await {
        Ok(res) => {
            let preview = PreviewView::ok(res);
            let html = if m.is_htmx() {
                (PreviewFragment { preview })
                    .render()
                    .map_err(UiError::from)
            } else {
                render_rerun_page(&st, &run_id, &q, csrf_of(&headers), preview).await
            };
            match html {
                Ok(h) => Html(h).into_response(),
                Err(e) => fail(&m, e),
            }
        }
        Err(e) => refused(&st, &m, &headers, &run_id, &q, &e).await,
    }
}

/// `POST /ui/runs/{run_id}/rerun`: `rerun_run{dry_run: false}`, then the new run.
async fn rerun_submit(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let q = fields_of(&m);
    let (ops, req) = match ops(&st).and_then(|o| Ok((o, rerun_request(&run_id, &q, false)?))) {
        Ok(v) => v,
        Err(e) => return fail(&m, e),
    };
    match ops.rerun_run(req).await {
        Ok(res) => go(&m, &run_href(res.new_run_id.as_deref().unwrap_or(&run_id))),
        Err(e) => refused(&st, &m, &headers, &run_id, &q, &e).await,
    }
}

// ---------------------------------------------------------------------------------
// Bulk jobs
// ---------------------------------------------------------------------------------

/// Bulk actions in the form's select (value, label).
const BULK_ACTIONS: [(&str, &str); 8] = [
    ("cancel", "cancel"),
    ("terminate", "terminate"),
    ("retry", "retry"),
    ("rerun", "rerun from step"),
    ("requeue", "requeue (DLQ)"),
    ("pause", "pause"),
    ("resume", "resume"),
    ("signal", "signal"),
];

/// The bulk form's fields, echoed back (`GET /ui/bulk?...` prefill and the preview's
/// "edit" link).
pub(crate) struct BulkFormView {
    workflow: String,
    queue: String,
    statuses: Vec<(&'static str, bool)>,
    tags: Vec<String>,
    dlq: bool,
    actions: Vec<(&'static str, &'static str, bool)>,
    reason: String,
    signal_name: String,
    payload: String,
    step: String,
    occurrence: String,
    seq: String,
    version: String,
    version_number: String,
    mode: String,
    rate: String,
}

fn is_true(q: &Params, name: &str) -> bool {
    matches!(param(q, name), Some("true" | "on" | "1"))
}

impl BulkFormView {
    fn new(q: &Params) -> Self {
        let p = |k: &str| param(q, k).unwrap_or_default().to_owned();
        let status = param(q, "status");
        let action = param(q, "action").unwrap_or("cancel");
        BulkFormView {
            workflow: p("workflow"),
            queue: p("queue"),
            statuses: RunStatus::ALL
                .iter()
                .map(|s| (s.as_str(), status == Some(s.as_str())))
                .collect(),
            tags: params(q, "tags"),
            dlq: is_true(q, "dlq"),
            actions: BULK_ACTIONS
                .iter()
                .map(|(v, l)| (*v, *l, *v == action))
                .collect(),
            reason: p("reason"),
            signal_name: p("signal_name"),
            payload: p("payload"),
            step: p("step"),
            occurrence: p("occurrence"),
            seq: p("seq"),
            version: param(q, "version").unwrap_or("active").to_owned(),
            version_number: p("version_number"),
            mode: param(q, "mode").unwrap_or("auto").to_owned(),
            rate: p("rate"),
        }
    }
}

/// Parse the bulk form into a request (`dry_run` false).
fn parse_bulk(q: &Params) -> Result<BulkCreateRequest, UiError> {
    let f = parse_run_filter(q)?;
    let filter = BulkFilter {
        workflow: f.workflow,
        status: f.status.map(RunStatusDto::from),
        queue: f.queue,
        tags: f.tags,
        dlq: is_true(q, "dlq"),
        ..BulkFilter::default()
    };
    let need = |k: &str, what: &str| {
        param(q, k)
            .map(str::to_owned)
            .ok_or_else(|| UiError::BadRequest(format!("{what} is required")))
    };
    let action = match param(q, "action") {
        None => return Err(UiError::BadRequest("choose a bulk action".to_owned())),
        Some("cancel") => BulkAction::Cancel,
        Some("terminate") => BulkAction::Terminate {
            reason: need("reason", "a terminate reason")?,
        },
        Some("retry") => BulkAction::Retry,
        Some("rerun") => BulkAction::Rerun {
            from: parse_cut(q)?.ok_or_else(|| {
                UiError::BadRequest("rerun needs a step (and occurrence) or a seq".to_owned())
            })?,
            version: parse_version(q)?,
        },
        Some("requeue") => BulkAction::Requeue {
            mode: parse_requeue_mode(q)?,
        },
        Some("pause") => BulkAction::Pause,
        Some("resume") => BulkAction::Resume,
        Some("signal") => BulkAction::Signal {
            name: need("signal_name", "a signal name")?,
            payload: parse_json("payload", param(q, "payload"))?,
        },
        Some(other) => return Err(UiError::BadRequest(format!("unknown bulk action {other}"))),
    };
    let rate_per_sec = match param(q, "rate") {
        None => None,
        Some(r) => Some(
            r.parse::<u32>()
                .ok()
                .filter(|r| *r >= 1)
                .ok_or_else(|| UiError::BadRequest(format!("rate must be >= 1, got {r}")))?,
        ),
    };
    Ok(BulkCreateRequest {
        action,
        filter,
        rate_per_sec,
        max_in_flight: None,
        dry_run: false,
    })
}

/// One run link.
pub(crate) struct RunLink {
    text: String,
    href: String,
}

fn run_link(id: &str) -> RunLink {
    RunLink {
        text: id.to_owned(),
        href: run_href(id),
    }
}

/// A bulk job's progress (`_bulk_progress.html`).
pub(crate) struct JobView {
    job_id: String,
    href: String,
    progress_href: String,
    cancel_href: String,
    state: String,
    action: String,
    matched: u64,
    done: u64,
    skipped: u64,
    failed_count: u64,
    failed: Vec<(RunLink, String)>,
    rate: u32,
    created: String,
    started: Option<String>,
    finished: Option<String>,
    eta: Option<String>,
    active: bool,
    progress_value: u64,
    progress_max: u64,
}

impl JobView {
    fn of(j: BulkJobView) -> Self {
        let base = format!("/ui/bulk/{}", href_encode(&j.job_id));
        JobView {
            progress_href: format!("{base}/progress"),
            cancel_href: format!("{base}/cancel"),
            href: base,
            active: matches!(
                j.state.as_str(),
                "pending" | "running" | "paused_backpressure"
            ),
            progress_value: (j.done + j.skipped + j.failed_count).min(j.matched.max(1)),
            progress_max: j.matched.max(1),
            job_id: j.job_id,
            state: j.state,
            action: j.action,
            matched: j.matched,
            done: j.done,
            skipped: j.skipped,
            failed_count: j.failed_count,
            failed: j
                .failed
                .into_iter()
                .map(|f| (run_link(&f.run_id), f.reason))
                .collect(),
            rate: j.rate_per_sec,
            created: fmt_ts(j.created_at_ms),
            started: j.started_at_ms.map(fmt_ts),
            finished: j.finished_at_ms.map(fmt_ts),
            eta: j.eta_ms.map(fmt_duration),
        }
    }
}

/// The dry run of a bulk form.
pub(crate) struct BulkPreview {
    matched_estimate: u64,
    sample: Vec<RunLink>,
    action: String,
    hidden: Vec<(String, String)>,
    edit_href: String,
}

#[derive(Template)]
#[template(path = "ui/bulk.html")]
struct BulkPage {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    /// `list` (jobs + new-job form), `preview` or `job`.
    mode: &'static str,
    form: BulkFormView,
    jobs: Vec<JobView>,
    jobs_error: Option<String>,
    preview: Option<BulkPreview>,
    job: Option<JobView>,
}

impl BulkPage {
    fn new(title: &str, csrf: String, available: bool, mode: &'static str, q: &Params) -> Self {
        BulkPage {
            title: title.to_owned(),
            csrf,
            nav: "runs",
            available,
            mode,
            form: BulkFormView::new(q),
            jobs: Vec::new(),
            jobs_error: None,
            preview: None,
            job: None,
        }
    }
}

#[derive(Template)]
#[template(path = "ui/_bulk_progress.html")]
struct ProgressFragment {
    job: JobView,
    csrf: String,
}

/// `GET /ui/bulk?<run-list filter>`: the jobs and a new-job form prefilled from the
/// filter (the run list's "Bulk…" link).
async fn bulk_page(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<Params>,
) -> Result<Response, UiError> {
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    let mut page = BulkPage::new(
        "Bulk operations",
        csrf,
        st.deps.ext.ops.is_some(),
        "list",
        &q,
    );
    if let Some(ops) = &st.deps.ext.ops {
        match ops.list_bulk().await {
            Ok(mut jobs) => {
                jobs.sort_by(|a, b| {
                    b.created_at_ms
                        .cmp(&a.created_at_ms)
                        .then_with(|| b.job_id.cmp(&a.job_id))
                });
                page.jobs = jobs.into_iter().map(JobView::of).collect();
            }
            Err(e) => {
                page.jobs_error = Some(format!(
                    "Bulk jobs unavailable ({}): {}",
                    e.code.http_status().as_u16(),
                    e.message
                ));
            }
        }
    }
    Ok(page_response(page.render()?, set_cookie))
}

/// `POST /ui/bulk/preview`: `create_bulk{dry_run: true}`: estimate, sample and the
/// confirm form.
async fn bulk_preview(
    State(st): State<UiState>,
    _op: Operator,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let q = fields_of(&m);
    let (ops, mut req) = match ops(&st).and_then(|o| Ok((o, parse_bulk(&q)?))) {
        Ok(v) => v,
        Err(e) => return fail(&m, e),
    };
    req.dry_run = true;
    let res = match ops.create_bulk(req).await {
        Ok(r) => r,
        Err(e) => return fail(&m, e.into()),
    };
    let hidden: Vec<(String, String)> = q
        .iter()
        .filter(|(k, v)| k != "csrf" && !v.trim().is_empty())
        .cloned()
        .collect();
    let pairs: Vec<(&str, &str)> = hidden
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let mut page = BulkPage::new("Bulk preview", csrf_of(&headers), true, "preview", &q);
    page.preview = Some(BulkPreview {
        matched_estimate: res.matched_estimate,
        sample: res.sample.iter().map(|id| run_link(id)).collect(),
        action: param(&q, "action").unwrap_or_default().to_owned(),
        edit_href: href("/ui/bulk", &pairs),
        hidden,
    });
    match page.render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => fail(&m, e.into()),
    }
}

/// `POST /ui/bulk`: create the job, then its page.
async fn bulk_create(State(st): State<UiState>, _op: Operator, m: SameOriginMutation) -> Response {
    let q = fields_of(&m);
    let (ops, req) = match ops(&st).and_then(|o| Ok((o, parse_bulk(&q)?))) {
        Ok(v) => v,
        Err(e) => return fail(&m, e),
    };
    match ops.create_bulk(req).await {
        Ok(res) => match res.job_id {
            Some(job) => go(&m, &format!("/ui/bulk/{}", href_encode(&job))),
            None => fail(
                &m,
                UiError::Internal("create_bulk returned no job id".to_owned()),
            ),
        },
        Err(e) => fail(&m, e.into()),
    }
}

/// `GET /ui/bulk/{job_id}`: the job page.
async fn bulk_job(
    State(st): State<UiState>,
    _reader: Reader,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    let job = ops(&st)?.get_bulk(job_id).await?;
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    let mut page = BulkPage::new(
        &format!("Bulk job {}", job.job_id),
        csrf,
        true,
        "job",
        &Vec::new(),
    );
    page.job = Some(JobView::of(job));
    Ok(page_response(page.render()?, set_cookie))
}

/// `GET /ui/bulk/{job_id}/progress`: the progress fragment (polls while active).
async fn bulk_progress(
    State(st): State<UiState>,
    _reader: Reader,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    let job = ops(&st)?.get_bulk(job_id).await?;
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    let html = ProgressFragment {
        job: JobView::of(job),
        csrf,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

/// `POST /ui/bulk/{job_id}/cancel`: the progress fragment (htmx) or 303 to the job.
async fn bulk_cancel(
    State(st): State<UiState>,
    _op: Operator,
    Path(job_id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let ops = match ops(&st) {
        Ok(o) => o,
        Err(e) => return fail(&m, e),
    };
    match ops.cancel_bulk(job_id).await {
        Ok(job) if m.is_htmx() => {
            let html = ProgressFragment {
                job: JobView::of(job),
                csrf: csrf_of(&headers),
            }
            .render();
            match html {
                Ok(h) => Html(h).into_response(),
                Err(e) => fail(&m, e.into()),
            }
        }
        Ok(job) => Redirect::to(&format!("/ui/bulk/{}", href_encode(&job.job_id))).into_response(),
        Err(e) => fail(&m, e.into()),
    }
}
