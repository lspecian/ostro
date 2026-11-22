//! The run-centric pages (plan 11-15, UI-01): run list with filters and tags, the DLQ
//! tab, run detail (published status, lineage, result/failure), and the status,
//! timeline and logs fragments.
//!
//! Every page renders server-side from the same traits REST uses
//! ([`ostro_api::ControlPlane::list_runs`], `get_run`, `run_history`,
//! [`ostro_api::phase11::OpsApi`], the Phase 8 `run_logs` slot); no page calls `/v1`
//! over HTTP. askama auto-escapes every value (`.html` templates).

use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use http::header::{self, HeaderMap, HeaderValue};
use http::{StatusCode, Uri};
use ostro_api::domain::{
    HistoryEvent, HistoryRequest, ListOrder, ListRunsRequest, RunInfo, RunStatus,
};
use ostro_api::phase8::{LogLevelDto, RunLogsRequest};
use ostro_api::phase11::{DlqListRequest, RerunCutsView, SYSTEM_TAG_PREFIX};
use ostro_api::{ApiError, ErrorCode};

use crate::UiDeps;
use crate::auth::Reader;
use crate::security::{self, HostAllowlist};
use crate::timeline::{Timeline, TimelineCtx, build_timeline};

/// Router state of every `/ui` handler.
#[derive(Clone)]
pub(crate) struct UiState {
    /// The traits and configuration.
    pub deps: Arc<UiDeps>,
    /// The Host / `:authority` allowlist (D-P11-15).
    pub hosts: Arc<HostAllowlist>,
}

impl UiState {
    pub(crate) fn new(deps: UiDeps) -> Self {
        let hosts = HostAllowlist::new(deps.cfg.bound_addr, &deps.cfg.allowed_hosts);
        UiState {
            deps: Arc::new(deps),
            hosts: Arc::new(hosts),
        }
    }
}

/// System tag naming a rerun's cut (D-P11-6).
const RERUN_FROM_TAG: &str = "__ostro/rerun_from=";
/// System tag naming the run an on_failure hook run handles (D-P11-25).
const ON_FAILURE_OF_TAG: &str = "__ostro/on_failure_of=";
/// Run list page size.
const PAGE_SIZE: u32 = 50;
/// Most history events the timeline loads.
const MAX_TIMELINE_EVENTS: usize = 5_000;
/// Log lines the logs tab asks for.
const LOG_LINES: u32 = 200;

// ---------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------

/// A failed `/ui` request, rendered as `error.html` with the matching status.
#[derive(Debug)]
pub(crate) enum UiError {
    /// A trait call failed: its HTTP status and message.
    Api(ApiError),
    /// The guard or the auth seam refused the request.
    Forbidden(String),
    /// Unknown page, run or asset.
    NotFound(String),
    /// Malformed query or form.
    BadRequest(String),
    /// The auth seam refused with its own status (Phase 14: 401).
    Rejected(StatusCode, String),
    /// Rendering or RNG failure.
    Internal(String),
}

impl From<ApiError> for UiError {
    fn from(e: ApiError) -> Self {
        if e.code == ErrorCode::NotFound {
            UiError::NotFound(e.message)
        } else {
            UiError::Api(e)
        }
    }
}

impl From<askama::Error> for UiError {
    fn from(e: askama::Error) -> Self {
        UiError::Internal(format!("render: {e}"))
    }
}

impl From<crate::auth::UiReject> for UiError {
    fn from(r: crate::auth::UiReject) -> Self {
        UiError::Rejected(r.status, r.message)
    }
}

impl UiError {
    /// The message the error page shows.
    pub(crate) fn message(&self) -> String {
        self.status_and_message().1
    }

    fn status_and_message(&self) -> (StatusCode, String) {
        match self {
            UiError::Api(e) => (e.code.http_status(), e.message.clone()),
            UiError::Forbidden(m) => (StatusCode::FORBIDDEN, format!("Forbidden: {m}")),
            UiError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            UiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            UiError::Rejected(s, m) => (*s, m.clone()),
            UiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
        }
    }
}

#[derive(Template)]
#[template(path = "ui/error.html")]
struct ErrorPage {
    title: String,
    csrf: String,
    nav: &'static str,
    message: String,
}

impl IntoResponse for UiError {
    fn into_response(self) -> Response {
        let (status, message) = self.status_and_message();
        let page = ErrorPage {
            title: format!(
                "{} {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Error")
            ),
            csrf: String::new(),
            nav: "",
            message,
        };
        let body = page
            .render()
            .unwrap_or_else(|_| "error page failed to render".to_owned());
        (status, Html(body)).into_response()
    }
}

// ---------------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------------

/// `YYYY-MM-DD HH:MM:SSZ` (UTC) of ms since the Unix epoch.
pub(crate) fn fmt_ts(ms: u64) -> String {
    let secs = ms / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// `850 ms`, `1.5 s`, `3m 07s`, `2h 05m`.
pub(crate) fn fmt_duration(ms: u64) -> String {
    match ms {
        0..1_000 => format!("{ms} ms"),
        1_000..60_000 => format!("{}.{} s", ms / 1000, (ms % 1000) / 100),
        60_000..3_600_000 => format!("{}m {:02}s", ms / 60_000, (ms % 60_000) / 1000),
        _ => format!("{}h {:02}m", ms / 3_600_000, (ms % 3_600_000) / 60_000),
    }
}

/// Percent-encodes everything but RFC 3986 unreserved characters (path segments and
/// query values).
pub(crate) fn href_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// `base?k=v&...` with every pair encoded; `base` alone when empty.
pub(crate) fn href(base: &str, pairs: &[(&str, &str)]) -> String {
    let q: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("{}={}", href_encode(k), href_encode(v)))
        .collect();
    if q.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", q.join("&"))
    }
}

pub(crate) fn run_href(run_id: &str) -> String {
    format!("/ui/runs/{}", href_encode(run_id))
}

/// Pretty JSON of `bytes`, or the lossy text when it is not JSON.
fn pretty_json(bytes: &[u8]) -> String {
    match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_default(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

// ---------------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------------

pub(crate) type Params = Vec<(String, String)>;

/// The first non-empty value of `name`.
pub(crate) fn param<'a>(q: &'a Params, name: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, v)| k == name && !v.trim().is_empty())
        .map(|(_, v)| v.trim())
}

/// Every non-empty value of `name`, deduplicated, in order.
pub(crate) fn params(q: &Params, name: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for (k, v) in q {
        let v = v.trim();
        if k == name && !v.is_empty() && !out.iter().any(|o| o == v) {
            out.push(v.to_owned());
        }
    }
    out
}

/// An HTML page response, with the CSRF `Set-Cookie` when a token was minted.
pub(crate) fn page_response(html: String, set_cookie: Option<HeaderValue>) -> Response {
    let mut resp = Html(html).into_response();
    if let Some(c) = set_cookie {
        resp.headers_mut().insert(header::SET_COOKIE, c);
    }
    resp
}

// ---------------------------------------------------------------------------------
// Run list
// ---------------------------------------------------------------------------------

/// A tag chip linking to the filtered list.
pub(crate) struct TagChip {
    text: String,
    href: String,
    system: bool,
}

fn chips(tags: &[String], base: &str) -> Vec<TagChip> {
    tags.iter()
        .map(|t| TagChip {
            text: t.clone(),
            href: href(base, &[("tags", t)]),
            system: t.starts_with(SYSTEM_TAG_PREFIX),
        })
        .collect()
}

struct RunFilters {
    workflow: String,
    queue: String,
    status: String,
    tags: Vec<String>,
    newest: bool,
}

struct RunRow {
    run_id: String,
    href: String,
    workflow: String,
    version: u32,
    status: &'static str,
    queue: String,
    created: String,
    tags: Vec<TagChip>,
}

#[derive(Template)]
#[template(path = "ui/runs.html")]
struct RunsPage {
    title: String,
    csrf: String,
    nav: &'static str,
    f: RunFilters,
    statuses: Vec<(&'static str, bool)>,
    rows: Vec<RunRow>,
    next_href: Option<String>,
    first_href: Option<String>,
    bulk_href: String,
}

/// The run-list filter of a query or form (`workflow`, `queue`, `status`, `tags`);
/// shared with the bulk form (11-16).
pub(crate) struct RunFilterQuery {
    pub(crate) workflow: Option<String>,
    pub(crate) queue: Option<String>,
    pub(crate) status: Option<RunStatus>,
    pub(crate) tags: Vec<String>,
}

/// Parse the run-list filter out of `q`.
pub(crate) fn parse_run_filter(q: &Params) -> Result<RunFilterQuery, UiError> {
    let status = match param(q, "status") {
        Some(s) => Some(
            RunStatus::parse(s)
                .ok_or_else(|| UiError::BadRequest(format!("unknown status {s}")))?,
        ),
        None => None,
    };
    let tags = ostro_api::phase11::search_tags(None, params(q, "tags")).map_err(UiError::Api)?;
    Ok(RunFilterQuery {
        workflow: param(q, "workflow").map(str::to_owned),
        queue: param(q, "queue").map(str::to_owned),
        status,
        tags,
    })
}

/// `GET /ui/` and `GET /ui/runs`.
pub(crate) async fn runs(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<Params>,
) -> Result<Response, UiError> {
    let RunFilterQuery {
        workflow,
        queue,
        status,
        tags,
    } = parse_run_filter(&q)?;
    let newest = param(&q, "order") == Some("newest");
    let page_token = param(&q, "page_token").map(str::to_owned);
    let req = ListRunsRequest {
        workflow: workflow.clone(),
        status,
        queue: queue.clone(),
        tag: tags.first().cloned(),
        limit: PAGE_SIZE,
        page_token: page_token.clone(),
        tags: tags.clone(),
        order: if newest {
            ListOrder::Newest
        } else {
            ListOrder::Oldest
        },
        ..ListRunsRequest::default()
    };
    let page = st.deps.cp.list_runs(req).await?;
    let f = RunFilters {
        workflow: workflow.unwrap_or_default(),
        queue: queue.unwrap_or_default(),
        status: status.map(|s| s.as_str().to_owned()).unwrap_or_default(),
        tags,
        newest,
    };
    let mut keep: Vec<(&str, &str)> = Vec::new();
    for (k, v) in [
        ("workflow", &f.workflow),
        ("queue", &f.queue),
        ("status", &f.status),
    ] {
        if !v.is_empty() {
            keep.push((k, v));
        }
    }
    for t in &f.tags {
        keep.push(("tags", t));
    }
    if newest {
        keep.push(("order", "newest"));
    }
    let next_href = page.next_page_token.as_deref().map(|t| {
        let mut p = keep.clone();
        p.push(("page_token", t));
        href("/ui/runs", &p)
    });
    let first_href = page_token.map(|_| href("/ui/runs", &keep));
    let bulk_href = href(
        "/ui/bulk",
        &keep
            .iter()
            .copied()
            .filter(|(k, _)| *k != "order")
            .collect::<Vec<_>>(),
    );
    let rows = page
        .runs
        .into_iter()
        .map(|r| RunRow {
            href: run_href(&r.run_id),
            run_id: r.run_id,
            workflow: r.workflow,
            version: r.version,
            status: r.status.as_str(),
            queue: r.queue,
            created: fmt_ts(r.created_at_ms),
            tags: chips(&r.tags, "/ui/runs"),
        })
        .collect();
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    let statuses = RunStatus::ALL
        .iter()
        .map(|s| (s.as_str(), status == Some(*s)))
        .collect();
    let html = RunsPage {
        title: "Runs".to_owned(),
        csrf,
        nav: "runs",
        f,
        statuses,
        rows,
        next_href,
        first_href,
        bulk_href,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

/// `GET /ui` → `/ui/`.
pub(crate) async fn ui_root() -> Redirect {
    Redirect::permanent("/ui/")
}

// ---------------------------------------------------------------------------------
// DLQ tab
// ---------------------------------------------------------------------------------

struct DlqFilters {
    workflow: String,
    queue: String,
    reason: String,
    tags: Vec<String>,
    include_dismissed: bool,
}

struct Link {
    text: String,
    href: String,
}

struct DlqRow {
    run_id: String,
    href: String,
    href_id: String,
    workflow: String,
    version: u32,
    queue: String,
    status: String,
    reason: String,
    failed: String,
    resumable: bool,
    hook: Option<Link>,
    requeued_to: Option<Link>,
    dismissed: Option<String>,
    tags: Vec<TagChip>,
}

#[derive(Template)]
#[template(path = "ui/dlq.html")]
struct DlqPageTpl {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    f: DlqFilters,
    rows: Vec<DlqRow>,
    next_href: Option<String>,
}

/// `GET /ui/dlq`: the DLQ tab (`OpsApi::list_dlq`); "not available" without the slot.
pub(crate) async fn dlq(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<Params>,
) -> Result<Response, UiError> {
    let tags = ostro_api::phase11::search_tags(None, params(&q, "tags")).map_err(UiError::Api)?;
    let f = DlqFilters {
        workflow: param(&q, "workflow").unwrap_or_default().to_owned(),
        queue: param(&q, "queue").unwrap_or_default().to_owned(),
        reason: param(&q, "reason").unwrap_or_default().to_owned(),
        tags,
        include_dismissed: param(&q, "include_dismissed") == Some("true"),
    };
    let (mut rows, mut next_href) = (Vec::new(), None);
    let available = st.deps.ext.ops.is_some();
    if let Some(ops) = &st.deps.ext.ops {
        let non_empty = |s: &str| (!s.is_empty()).then(|| s.to_owned());
        let page = ops
            .list_dlq(DlqListRequest {
                workflow: non_empty(&f.workflow),
                queue: non_empty(&f.queue),
                reason: non_empty(&f.reason),
                tags: f.tags.clone(),
                include_dismissed: f.include_dismissed,
                limit: PAGE_SIZE,
                page_token: param(&q, "page_token").map(str::to_owned),
            })
            .await?;
        rows = page
            .entries
            .into_iter()
            .map(|e| DlqRow {
                href: run_href(&e.run_id),
                href_id: href_encode(&e.run_id),
                status: domain_status(e.status).as_str().to_owned(),
                failed: fmt_ts(e.failed_at_ms),
                hook: e.hook_run_id.map(|h| Link {
                    href: run_href(&h),
                    text: h,
                }),
                requeued_to: e.requeued_to.map(|h| Link {
                    href: run_href(&h),
                    text: h,
                }),
                dismissed: e.dismissed_at_ms.map(|at| match e.dismiss_reason {
                    Some(r) => format!("{} ({r})", fmt_ts(at)),
                    None => fmt_ts(at),
                }),
                tags: chips(&e.tags, "/ui/dlq"),
                run_id: e.run_id,
                workflow: e.workflow,
                version: e.version,
                queue: e.queue,
                reason: e.reason,
                resumable: e.resumable,
            })
            .collect();
        next_href = page.next_page_token.as_deref().map(|t| {
            let mut p: Vec<(&str, &str)> = Vec::new();
            for (k, v) in [
                ("workflow", &f.workflow),
                ("queue", &f.queue),
                ("reason", &f.reason),
            ] {
                if !v.is_empty() {
                    p.push((k, v));
                }
            }
            for tg in &f.tags {
                p.push(("tags", tg));
            }
            if f.include_dismissed {
                p.push(("include_dismissed", "true"));
            }
            p.push(("page_token", t));
            href("/ui/dlq", &p)
        });
    }
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    let html = DlqPageTpl {
        title: "Dead-letter queue".to_owned(),
        csrf,
        nav: "dlq",
        available,
        f,
        rows,
        next_href,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

fn domain_status(s: ostro_api::rest::RunStatusDto) -> RunStatus {
    s.into()
}

// ---------------------------------------------------------------------------------
// Run detail and fragments
// ---------------------------------------------------------------------------------

/// The status card (the `_run_status.html` fragment), polled while non-terminal.
pub(crate) struct StatusView {
    status_href: String,
    status: &'static str,
    terminal: bool,
    attempt: Option<u32>,
    attempt_failures: u32,
    published: Option<String>,
    paused: Option<String>,
    completed: Option<String>,
}

impl StatusView {
    pub(crate) fn of(run: &RunInfo) -> Self {
        StatusView {
            status_href: format!("{}/status", run_href(&run.run_id)),
            status: run.status.as_str(),
            terminal: run.status.is_terminal(),
            attempt: run.attempt,
            attempt_failures: run.attempt_failures,
            published: run.published_status_json.as_deref().map(pretty_json),
            paused: run.pause.as_ref().map(|p| {
                if p.detail.is_empty() {
                    p.reason.clone()
                } else {
                    format!("{}: {}", p.reason, p.detail)
                }
            }),
            completed: run.completed_at_ms.map(fmt_ts),
        }
    }
}

/// One lineage line: `text` then an optional link.
struct Lineage {
    text: String,
    link: Option<Link>,
}

struct RunView {
    run_id: String,
    href: String,
    workflow: String,
    version: u32,
    queue: String,
    module_sha256: String,
    tenant: Option<String>,
    priority: i32,
    created: String,
    result: Option<String>,
    failure: Option<String>,
    tags: Vec<TagChip>,
}

/// The timeline fragment's data.
pub(crate) struct TimelineView {
    tl: Timeline,
    refresh_href: String,
    note: Option<String>,
    truncated: bool,
}

struct LogRow {
    ts: String,
    level: &'static str,
    attempt: u32,
    message: String,
    fields: String,
}

/// The logs fragment's data.
pub(crate) struct LogsView {
    available: bool,
    lines: Vec<LogRow>,
    dropped: u64,
}

#[derive(Template)]
#[template(path = "ui/run_detail.html")]
struct RunDetailPage {
    title: String,
    csrf: String,
    nav: &'static str,
    run: RunView,
    status: StatusView,
    lineage: Vec<Lineage>,
    tab: &'static str,
    timeline: Option<TimelineView>,
    logs: Option<LogsView>,
    actions: crate::actions::ActionsView,
    oob: bool,
}

#[derive(Template)]
#[template(path = "ui/_run_status.html")]
pub(crate) struct StatusFragment {
    pub(crate) status: StatusView,
}

#[derive(Template)]
#[template(path = "ui/_timeline.html")]
struct TimelineFragment {
    timeline: TimelineView,
}

#[derive(Template)]
#[template(path = "ui/_logs.html")]
struct LogsFragment {
    logs: LogsView,
}

/// The cut of a rerun (`__ostro/rerun_from=<cut>`).
fn rerun_cut(run: &RunInfo) -> Option<u64> {
    run.tags
        .iter()
        .find_map(|t| t.strip_prefix(RERUN_FROM_TAG))
        .and_then(|v| v.parse().ok())
}

/// The run a hook run handles (`__ostro/on_failure_of=<run>`).
fn on_failure_of(run: &RunInfo) -> Option<&str> {
    run.tags
        .iter()
        .find_map(|t| t.strip_prefix(ON_FAILURE_OF_TAG))
}

/// `retry_of` from the RunStarted details, when the history carries it. The API does not
/// expose `retry_of` on [`RunInfo`] (11-15 open issue), so this is best effort.
fn retry_of(first: Option<&HistoryEvent>) -> Option<String> {
    let ev = first?;
    let v: serde_json::Value = serde_json::from_slice(&ev.details_json).ok()?;
    v.get("retry_of")?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn lineage(run: &RunInfo, source: Option<&str>) -> Vec<Lineage> {
    let mut out = Vec::new();
    let link = |id: &str| Link {
        text: id.to_owned(),
        href: run_href(id),
    };
    match (rerun_cut(run), source) {
        (Some(cut), Some(src)) => out.push(Lineage {
            text: format!("rerun from seq {cut} of"),
            link: Some(link(src)),
        }),
        (Some(cut), None) => out.push(Lineage {
            text: format!("rerun from seq {cut} (source run not reported by the API)"),
            link: None,
        }),
        (None, Some(src)) => out.push(Lineage {
            text: "retry of".to_owned(),
            link: Some(link(src)),
        }),
        (None, None) => {}
    }
    if let Some(of) = on_failure_of(run) {
        out.push(Lineage {
            text: "on_failure hook for".to_owned(),
            link: Some(link(of)),
        });
    }
    out
}

/// Every history event (up to [`MAX_TIMELINE_EVENTS`]); `true` when truncated.
async fn full_history(st: &UiState, run_id: &str) -> Result<(Vec<HistoryEvent>, bool), UiError> {
    let mut events = Vec::new();
    let mut from_seq = 0;
    loop {
        let page = st
            .deps
            .cp
            .run_history(HistoryRequest {
                run_id: run_id.to_owned(),
                from_seq,
                limit: 0,
            })
            .await?;
        events.extend(page.events);
        match page.next_from_seq {
            Some(n) if events.len() < MAX_TIMELINE_EVENTS && n > from_seq => from_seq = n,
            Some(_) => return Ok((events, true)),
            None => return Ok((events, false)),
        }
    }
}

/// The timeline of `run` and the rerun/retry source its RunStarted event names.
async fn timeline_view(
    st: &UiState,
    run: &RunInfo,
) -> Result<(TimelineView, Option<String>), UiError> {
    let (events, truncated) = full_history(st, &run.run_id).await?;
    let source = retry_of(events.first());
    let (cuts, note): (Option<RerunCutsView>, Option<String>) = match &st.deps.ext.ops {
        None => (
            None,
            Some("Rerun is not available on this server.".to_owned()),
        ),
        Some(_) if !run.status.is_terminal() => (
            None,
            Some("Rerun from a step is available once the run is terminal.".to_owned()),
        ),
        Some(ops) => match ops.analyze_rerun(run.run_id.clone()).await {
            Ok(v) => (Some(v), None),
            Err(e) => (None, Some(format!("Rerun cuts unavailable: {}", e.message))),
        },
    };
    let ctx = TimelineCtx {
        run_id: &run.run_id,
        rerun_cut: rerun_cut(run),
        source: source.as_deref(),
        cuts: cuts.as_ref(),
    };
    let view = TimelineView {
        tl: build_timeline(&events, &ctx),
        refresh_href: format!("{}/timeline", run_href(&run.run_id)),
        note,
        truncated,
    };
    Ok((view, source))
}

async fn logs_view(st: &UiState, run_id: &str) -> Result<LogsView, UiError> {
    let Some(api) = &st.deps.ext.run_logs else {
        return Ok(LogsView {
            available: false,
            lines: Vec::new(),
            dropped: 0,
        });
    };
    let resp = api
        .run_logs(RunLogsRequest {
            run_id: run_id.to_owned(),
            limit: LOG_LINES,
            level: None,
        })
        .await?;
    Ok(LogsView {
        available: true,
        dropped: resp.dropped,
        lines: resp
            .lines
            .into_iter()
            .map(|l| LogRow {
                ts: fmt_ts(l.ts_ms),
                level: match l.level {
                    LogLevelDto::Trace => "trace",
                    LogLevelDto::Debug => "debug",
                    LogLevelDto::Info => "info",
                    LogLevelDto::Warn => "warn",
                    LogLevelDto::Error => "error",
                },
                attempt: l.attempt,
                message: l.message,
                fields: match &l.fields {
                    serde_json::Value::Object(m) if m.is_empty() => String::new(),
                    serde_json::Value::Null => String::new(),
                    v => v.to_string(),
                },
            })
            .collect(),
    })
}

/// `GET /ui/runs/{run_id}` (`?tab=timeline|logs`).
pub(crate) async fn run_detail(
    State(st): State<UiState>,
    _reader: Reader,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<Params>,
) -> Result<Response, UiError> {
    let run = st.deps.cp.get_run(run_id).await?;
    let tab = if param(&q, "tab") == Some("logs") {
        "logs"
    } else {
        "timeline"
    };
    let (timeline, logs, source) = if tab == "logs" {
        let first = st
            .deps
            .cp
            .run_history(HistoryRequest {
                run_id: run.run_id.clone(),
                from_seq: 0,
                limit: 1,
            })
            .await?;
        let source = retry_of(first.events.first());
        (None, Some(logs_view(&st, &run.run_id).await?), source)
    } else {
        let (tv, source) = timeline_view(&st, &run).await?;
        (Some(tv), None, source)
    };
    let (csrf, set_cookie) = security::csrf_token(&headers, &uri)?;
    let html = RunDetailPage {
        title: format!("Run {}", run.run_id),
        csrf,
        nav: "runs",
        status: StatusView::of(&run),
        actions: crate::actions::ActionsView::of(&run),
        oob: false,
        lineage: lineage(&run, source.as_deref()),
        tab,
        timeline,
        logs,
        run: RunView {
            href: run_href(&run.run_id),
            run_id: run.run_id.clone(),
            workflow: run.workflow.clone(),
            version: run.version,
            queue: run.queue.clone(),
            module_sha256: run.module_sha256.clone(),
            tenant: run.tenant.clone(),
            priority: run.priority,
            created: fmt_ts(run.created_at_ms),
            result: run.result_json.as_deref().map(pretty_json),
            failure: run.failure.clone(),
            tags: chips(&run.tags, "/ui/runs"),
        },
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

/// `GET /ui/runs/{run_id}/status`: the status card fragment.
pub(crate) async fn run_status(
    State(st): State<UiState>,
    _reader: Reader,
    Path(run_id): Path<String>,
) -> Result<Response, UiError> {
    let run = st.deps.cp.get_run(run_id).await?;
    let html = StatusFragment {
        status: StatusView::of(&run),
    }
    .render()?;
    Ok(Html(html).into_response())
}

/// `GET /ui/runs/{run_id}/timeline`: the timeline fragment.
pub(crate) async fn run_timeline(
    State(st): State<UiState>,
    _reader: Reader,
    Path(run_id): Path<String>,
) -> Result<Response, UiError> {
    let run = st.deps.cp.get_run(run_id).await?;
    let html = TimelineFragment {
        timeline: timeline_view(&st, &run).await?.0,
    }
    .render()?;
    Ok(Html(html).into_response())
}

/// `GET /ui/runs/{run_id}/logs`: the logs fragment.
pub(crate) async fn run_logs(
    State(st): State<UiState>,
    _reader: Reader,
    Path(run_id): Path<String>,
) -> Result<Response, UiError> {
    let run = st.deps.cp.get_run(run_id).await?;
    let html = LogsFragment {
        logs: logs_view(&st, &run.run_id).await?,
    }
    .render()?;
    Ok(Html(html).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_and_durations() {
        assert_eq!(fmt_ts(0), "1970-01-01 00:00:00Z");
        assert_eq!(fmt_ts(1_700_000_000_000), "2023-11-14 22:13:20Z");
        assert_eq!(fmt_ts(951_782_400_000), "2000-02-29 00:00:00Z");
        assert_eq!(fmt_duration(999), "999 ms");
        assert_eq!(fmt_duration(61_000), "1m 01s");
        assert_eq!(fmt_duration(7_500_000), "2h 05m");
    }

    #[test]
    fn hrefs_encode_hostile_values() {
        assert_eq!(href_encode("a b/<c>&\"'"), "a%20b%2F%3Cc%3E%26%22%27");
        assert_eq!(
            href("/ui/runs", &[("tags", "x=y"), ("order", "newest")]),
            "/ui/runs?tags=x%3Dy&order=newest"
        );
    }
}
