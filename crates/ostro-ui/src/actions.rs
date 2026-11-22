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
use axum::extract::{Path, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::post;
use http::header::HeaderMap;
use http::{HeaderValue, StatusCode};
use ostro_api::domain::{
    CancelRequest, RetryRequest, RunInfo, RunStatus, SignalRequest, TerminateRequest,
};
use ostro_api::phase11::{DlqDismissRequest, DlqRequeueRequest, RequeueMode, RerunFrom};

use crate::auth::Operator;
use crate::pages::{StatusFragment, StatusView, UiError, UiState, run_href};
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

/// The cut named by `seq`, or `step` + `occurrence` (default 1).
fn parse_cut(get: impl Fn(&str) -> Option<String>) -> Result<Option<RerunFrom>, UiError> {
    if let Some(s) = get("seq") {
        let seq = s
            .parse::<u64>()
            .map_err(|_| UiError::BadRequest(format!("seq must be a number, got {s}")))?;
        return Ok(Some(RerunFrom::Seq(seq)));
    }
    if let Some(name) = get("step") {
        let occurrence =
            match get("occurrence") {
                Some(o) => o.parse::<u32>().ok().filter(|o| *o >= 1).ok_or_else(|| {
                    UiError::BadRequest(format!("occurrence must be >= 1, got {o}"))
                })?,
                None => 1,
            };
        return Ok(Some(RerunFrom::Step { name, occurrence }));
    }
    Ok(None)
}

fn form_cut(m: &SameOriginMutation) -> Result<Option<RerunFrom>, UiError> {
    parse_cut(|k| field(m, k).map(str::to_owned))
}

fn parse_requeue_mode(m: &SameOriginMutation) -> Result<RequeueMode, UiError> {
    match field(m, "mode").unwrap_or("auto") {
        "auto" => Ok(RequeueMode::Auto),
        "resume" => Ok(RequeueMode::Resume),
        "restart" => Ok(RequeueMode::Restart),
        "rerun" => match form_cut(m)? {
            Some(from) => Ok(RequeueMode::Rerun { from }),
            None => Err(UiError::BadRequest(
                "requeue mode rerun needs a step or seq".to_owned(),
            )),
        },
        other => Err(UiError::BadRequest(format!("unknown requeue mode {other}"))),
    }
}

/// `POST /ui/dlq/{run_id}/requeue` (form `mode` = auto|resume|restart|rerun, `step` /
/// `occurrence` / `seq` for rerun): lands on the resulting run.
async fn dlq_requeue(
    State(st): State<UiState>,
    _op: Operator,
    Path(run_id): Path<String>,
    m: SameOriginMutation,
) -> Response {
    let mode = match parse_requeue_mode(&m) {
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
