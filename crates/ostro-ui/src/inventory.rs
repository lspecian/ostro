//! Inventory pages (plan 11-17, UI-03, D-P11-19) and their operator actions: modules,
//! workflows and versions, queues, schedules, workers and partition health.
//!
//! Every page renders server-side from the same traits REST uses:
//! [`ostro_api::phase11::InventoryApi`] (`list_modules`, `list_queues`,
//! `list_partitions`), [`ostro_api::ControlPlane`] (`list_workflows`, Phase 7
//! `list_workers` and the queue/workflow pause/resume and worker drain calls) and the
//! Phase 8 [`ostro_api::phase8::ScheduleApi`] slot. A missing extension slot, or a
//! Phase 7 method the control plane does not implement, renders "not available" (200),
//! never a 500.
//!
//! Actions follow 11-16: [`Operator`] (the `operate` scope) plus [`SameOriginMutation`]
//! (Host + same-origin + CSRF). A plain form post answers 303 to the page (a schedule
//! trigger: to the started run); htmx gets the refreshed table row (`hx-target="closest
//! tr"`), or `200` + `HX-Retarget: #flash` on error. Drain and trigger carry
//! `hx-confirm`.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use http::header::HeaderMap;
use http::{HeaderValue, Uri};
use ostro_api::domain::WorkflowInfo;
use ostro_api::phase7::WorkerInfo;
use ostro_api::phase8::ScheduleDto;
use ostro_api::phase11::{ListModulesRequest, PartitionView, QueueView};
use ostro_api::{ApiError, ErrorCode};

use crate::auth::{Operator, Reader};
use crate::pages::{Params, UiError, UiState, fmt_ts, href, href_encode, page_response, param};
use crate::security::{self, SameOriginMutation};

/// Rendered for a value the server does not expose.
const DASH: &str = "\u{2014}";
/// Module page size.
const MODULE_PAGE: u32 = 50;
/// Width of a partition health bar (SVG user units).
const BAR_WIDTH: u64 = 120;

/// The inventory routes (merged by [`crate::router`]).
pub(crate) fn routes() -> axum::Router<UiState> {
    axum::Router::new()
        .route("/ui/modules", get(modules))
        .route("/ui/workflows", get(workflows))
        .route("/ui/workflows/{w}/pause", post(workflow_pause))
        .route("/ui/workflows/{w}/resume", post(workflow_resume))
        .route("/ui/queues", get(queues))
        .route("/ui/queues/{q}/pause", post(queue_pause))
        .route("/ui/queues/{q}/resume", post(queue_resume))
        .route("/ui/schedules", get(schedules))
        .route("/ui/schedules/{name}/pause", post(schedule_pause))
        .route("/ui/schedules/{name}/resume", post(schedule_resume))
        .route("/ui/schedules/{name}/trigger", post(schedule_trigger))
        .route("/ui/workers", get(workers))
        .route("/ui/workers/{id}/drain", post(worker_drain))
        .route("/ui/partitions", get(partitions))
        .route("/ui/partitions/fragment", get(partitions_fragment))
}

// ---------------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------------

/// A call the server cannot serve at all: the method is not implemented, or a Phase 7
/// method still answers its "lands in 07-14" placeholder.
fn not_available(e: &ApiError) -> bool {
    e.code == ErrorCode::Unimplemented
        || (e.code == ErrorCode::Unavailable && e.message == ostro_api::phase7::P7_PENDING)
}

/// `Ok(None)` when the call is [`not_available`].
fn available<T>(r: Result<T, ApiError>) -> Result<Option<T>, UiError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if not_available(&e) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn dash<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map_or_else(|| DASH.to_owned(), |v| v.to_string())
}

fn dash_ts(v: Option<u64>) -> String {
    v.map_or_else(|| DASH.to_owned(), fmt_ts)
}

/// The first 12 characters of a content hash.
fn short_sha(sha: &str) -> String {
    sha.chars().take(12).collect()
}

/// `512 B`, `1.5 KiB`, `3.2 MiB`, `1.1 GiB`.
fn fmt_bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut unit = 0;
    let mut scaled = n;
    while scaled >= 1024 * 1024 && unit + 1 < UNITS.len() {
        scaled /= 1024;
        unit += 1;
    }
    // One decimal from integer arithmetic.
    let tenths = scaled * 10 / 1024;
    format!("{}.{} {}", tenths / 10, tenths % 10, UNITS[unit])
}

/// A link (escaped text, encoded href).
pub(crate) struct Link {
    pub(crate) text: String,
    pub(crate) href: String,
}

fn runs_of_workflow(name: &str) -> Link {
    Link {
        text: name.to_owned(),
        href: href("/ui/runs", &[("workflow", name)]),
    }
}

fn runs_of_queue(name: &str) -> String {
    href("/ui/runs", &[("queue", name)])
}

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

/// An action failure: the error page (plain form) or the `#flash` region (htmx; htmx
/// does not swap 4xx/5xx answers).
fn fail(m: &SameOriginMutation, e: UiError) -> Response {
    if !m.is_htmx() {
        return e.into_response();
    }
    let html = FlashFragment {
        message: e.message(),
    }
    .render()
    .unwrap_or_else(|_| "<div id=\"flash\" class=\"flash\">error</div>".to_owned());
    let mut resp = Html(html).into_response();
    hx_header(&mut resp, "hx-retarget", "#flash");
    hx_header(&mut resp, "hx-reswap", "outerHTML");
    resp
}

/// A rendered row fragment, or [`fail`] when rendering failed.
fn row_html(m: &SameOriginMutation, r: Result<String, askama::Error>) -> Response {
    match r {
        Ok(html) => Html(html).into_response(),
        Err(e) => fail(m, e.into()),
    }
}

/// The CSRF token of a validated mutation (its `ostro_csrf` cookie).
fn csrf_of(headers: &HeaderMap) -> String {
    security::cookie_token(headers).unwrap_or_default()
}

/// The CSRF token for a page (minting the cookie when absent).
fn page_csrf(headers: &HeaderMap, uri: &Uri) -> Result<(String, Option<HeaderValue>), UiError> {
    security::csrf_token(headers, uri)
}

// ---------------------------------------------------------------------------------
// Modules
// ---------------------------------------------------------------------------------

struct ModuleRow {
    sha: String,
    short: String,
    size: String,
    workflows: Vec<Link>,
    uploaded: String,
}

#[derive(Template)]
#[template(path = "ui/modules.html")]
struct ModulesPage {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    rows: Vec<ModuleRow>,
    next_href: Option<String>,
}

/// `GET /ui/modules` (`?page_token=`): stored modules, newest first on the real server.
async fn modules(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
    Query(q): Query<Params>,
) -> Result<Response, UiError> {
    let mut page = None;
    if let Some(inv) = &st.deps.ext.inventory {
        page = available(
            inv.list_modules(ListModulesRequest {
                limit: MODULE_PAGE,
                page_token: param(&q, "page_token").map(str::to_owned),
            })
            .await,
        )?;
    }
    let available = page.is_some();
    let page = page.unwrap_or_default();
    let rows = page
        .modules
        .into_iter()
        .map(|m| ModuleRow {
            short: short_sha(&m.sha256),
            size: fmt_bytes(m.size_bytes),
            workflows: m.workflows.iter().map(|w| runs_of_workflow(w)).collect(),
            uploaded: dash_ts(m.uploaded_at_ms),
            sha: m.sha256,
        })
        .collect();
    let next_href = page
        .next_page_token
        .as_deref()
        .map(|t| href("/ui/modules", &[("page_token", t)]));
    let (csrf, set_cookie) = page_csrf(&headers, &uri)?;
    let html = ModulesPage {
        title: "Modules".to_owned(),
        csrf,
        nav: "modules",
        available,
        rows,
        next_href,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

// ---------------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------------

struct VersionRow {
    version: u32,
    active: bool,
    sha: String,
    short: String,
    queue: String,
    timeout: String,
}

pub(crate) struct WorkflowRow {
    name: String,
    href_id: String,
    runs: Link,
    active: String,
    versions: Vec<VersionRow>,
    /// Known only right after a pause/resume (the workflows list carries no pause
    /// state); `None` offers both buttons.
    paused: Option<bool>,
}

impl WorkflowRow {
    fn of(w: WorkflowInfo, paused: Option<bool>) -> Self {
        let versions = w
            .versions
            .iter()
            .rev()
            .map(|v| VersionRow {
                version: v.version,
                active: w.active_version == Some(v.version),
                short: short_sha(&v.module_sha256),
                sha: v.module_sha256.clone(),
                queue: v.default_queue.clone().unwrap_or_else(|| DASH.to_owned()),
                timeout: v
                    .run_timeout_ms
                    .map_or_else(|| DASH.to_owned(), crate::pages::fmt_duration),
            })
            .collect();
        WorkflowRow {
            href_id: href_encode(&w.name),
            runs: runs_of_workflow(&w.name),
            active: w
                .active_version
                .map_or_else(|| DASH.to_owned(), |v| format!("v{v}")),
            name: w.name,
            versions,
            paused,
        }
    }
}

#[derive(Template)]
#[template(path = "ui/workflows.html")]
struct WorkflowsPage {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    rows: Vec<WorkflowRow>,
}

#[derive(Template)]
#[template(path = "ui/_workflow_row.html")]
struct WorkflowRowFragment {
    r: WorkflowRow,
    csrf: String,
}

/// `GET /ui/workflows`: workflows, their versions and the active version.
async fn workflows(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    let list = available(st.deps.cp.list_workflows().await)?;
    let available = list.is_some();
    let rows = list
        .unwrap_or_default()
        .into_iter()
        .map(|w| WorkflowRow::of(w, None))
        .collect();
    let (csrf, set_cookie) = page_csrf(&headers, &uri)?;
    let html = WorkflowsPage {
        title: "Workflows".to_owned(),
        csrf,
        nav: "workflows",
        available,
        rows,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

async fn workflow_set_paused(
    st: &UiState,
    headers: &HeaderMap,
    m: &SameOriginMutation,
    name: String,
    pause: bool,
) -> Response {
    let r = if pause {
        st.deps.cp.pause_workflow(name.clone()).await
    } else {
        st.deps.cp.resume_workflow(name.clone()).await
    };
    let state = match r {
        Ok(s) => s,
        Err(e) => return fail(m, e.into()),
    };
    if !m.is_htmx() {
        return Redirect::to("/ui/workflows").into_response();
    }
    let info = match st.deps.cp.list_workflows().await {
        Ok(list) => list.into_iter().find(|w| w.name == name),
        Err(e) => return fail(m, e.into()),
    };
    let info = info.unwrap_or(WorkflowInfo {
        name,
        active_version: None,
        versions: Vec::new(),
        record_version: 0,
    });
    row_html(
        m,
        WorkflowRowFragment {
            r: WorkflowRow::of(info, Some(state.paused)),
            csrf: csrf_of(headers),
        }
        .render(),
    )
}

/// `POST /ui/workflows/{w}/pause`.
async fn workflow_pause(
    State(st): State<UiState>,
    _op: Operator,
    Path(w): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    workflow_set_paused(&st, &headers, &m, w, true).await
}

/// `POST /ui/workflows/{w}/resume`.
async fn workflow_resume(
    State(st): State<UiState>,
    _op: Operator,
    Path(w): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    workflow_set_paused(&st, &headers, &m, w, false).await
}

// ---------------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------------

pub(crate) struct QueueRow {
    name: String,
    href_id: String,
    runs_href: String,
    paused: bool,
    ready: String,
    running: String,
    demand: String,
    flow: String,
}

/// `k=v, k=v` of a flow object (nested values as compact JSON); `—` when unset.
fn flow_summary(flow: Option<&serde_json::Value>) -> String {
    match flow {
        None | Some(serde_json::Value::Null) => DASH.to_owned(),
        Some(serde_json::Value::Object(map)) if map.is_empty() => DASH.to_owned(),
        Some(serde_json::Value::Object(map)) => map
            .iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, v)| match v {
                serde_json::Value::String(s) => format!("{k}={s}"),
                other => format!("{k}={other}"),
            })
            .collect::<Vec<_>>()
            .join(", "),
        Some(other) => other.to_string(),
    }
}

impl QueueRow {
    fn of(q: &QueueView) -> Self {
        QueueRow {
            name: q.name.clone(),
            href_id: href_encode(&q.name),
            runs_href: runs_of_queue(&q.name),
            paused: q.paused,
            ready: q.ready.to_string(),
            running: q.running.to_string(),
            demand: dash(q.demand),
            flow: flow_summary(q.flow.as_ref()),
        }
    }

    /// A queue the inventory does not list (yet): counts unknown.
    fn bare(name: &str, paused: bool) -> Self {
        QueueRow {
            name: name.to_owned(),
            href_id: href_encode(name),
            runs_href: runs_of_queue(name),
            paused,
            ready: DASH.to_owned(),
            running: DASH.to_owned(),
            demand: DASH.to_owned(),
            flow: DASH.to_owned(),
        }
    }
}

#[derive(Template)]
#[template(path = "ui/queues.html")]
struct QueuesPage {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    rows: Vec<QueueRow>,
}

#[derive(Template)]
#[template(path = "ui/_queue_row.html")]
struct QueueRowFragment {
    r: QueueRow,
    csrf: String,
}

/// `GET /ui/queues`: queues with paused state, counts, demand and flow.
async fn queues(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    let mut list = None;
    if let Some(inv) = &st.deps.ext.inventory {
        list = available(inv.list_queues().await)?;
    }
    let available = list.is_some();
    let rows = list.unwrap_or_default().iter().map(QueueRow::of).collect();
    let (csrf, set_cookie) = page_csrf(&headers, &uri)?;
    let html = QueuesPage {
        title: "Queues".to_owned(),
        csrf,
        nav: "queues",
        available,
        rows,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

async fn queue_set_paused(
    st: &UiState,
    headers: &HeaderMap,
    m: &SameOriginMutation,
    name: String,
    pause: bool,
) -> Response {
    let r = if pause {
        st.deps.cp.pause_queue(name.clone()).await
    } else {
        st.deps.cp.resume_queue(name.clone()).await
    };
    let state = match r {
        Ok(s) => s,
        Err(e) => return fail(m, e.into()),
    };
    if !m.is_htmx() {
        return Redirect::to("/ui/queues").into_response();
    }
    // The counts come from the inventory; the pause state is the call's answer.
    let mut row = QueueRow::bare(&name, state.paused);
    if let Some(inv) = &st.deps.ext.inventory
        && let Ok(list) = inv.list_queues().await
        && let Some(q) = list.iter().find(|q| q.name == name)
    {
        row = QueueRow::of(q);
        row.paused = state.paused;
    }
    row_html(
        m,
        QueueRowFragment {
            r: row,
            csrf: csrf_of(headers),
        }
        .render(),
    )
}

/// `POST /ui/queues/{q}/pause`.
async fn queue_pause(
    State(st): State<UiState>,
    _op: Operator,
    Path(q): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    queue_set_paused(&st, &headers, &m, q, true).await
}

/// `POST /ui/queues/{q}/resume`.
async fn queue_resume(
    State(st): State<UiState>,
    _op: Operator,
    Path(q): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    queue_set_paused(&st, &headers, &m, q, false).await
}

// ---------------------------------------------------------------------------------
// Schedules
// ---------------------------------------------------------------------------------

pub(crate) struct ScheduleRow {
    name: String,
    href_id: String,
    workflow: Link,
    version: String,
    cron: String,
    timezone: String,
    paused: bool,
    next: String,
    last: String,
    /// The run a trigger just started.
    started: Option<Link>,
}

impl ScheduleRow {
    fn of(s: ScheduleDto, started: Option<String>) -> Self {
        ScheduleRow {
            href_id: href_encode(&s.name),
            workflow: runs_of_workflow(&s.workflow),
            version: s
                .version
                .map_or_else(|| "active version".to_owned(), |v| format!("v{v}")),
            timezone: if s.timezone.is_empty() {
                "UTC".to_owned()
            } else {
                s.timezone.clone()
            },
            paused: s.paused,
            next: if s.paused {
                "paused".to_owned()
            } else {
                dash_ts(s.next_fire_at_ms)
            },
            last: dash_ts(s.last_fire_at_ms),
            started: started.map(|id| Link {
                href: crate::pages::run_href(&id),
                text: id,
            }),
            name: s.name,
            cron: s.cron,
        }
    }
}

#[derive(Template)]
#[template(path = "ui/schedules.html")]
struct SchedulesPage {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    rows: Vec<ScheduleRow>,
}

#[derive(Template)]
#[template(path = "ui/_schedule_row.html")]
struct ScheduleRowFragment {
    r: ScheduleRow,
    csrf: String,
}

fn schedule_api(
    st: &UiState,
) -> Result<&std::sync::Arc<dyn ostro_api::phase8::ScheduleApi>, UiError> {
    st.deps
        .ext
        .schedules
        .as_ref()
        .ok_or_else(|| UiError::NotFound("schedules are not available on this server".to_owned()))
}

/// `GET /ui/schedules`: schedules with next and last fire (Phase 8 slot).
async fn schedules(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    let mut list = None;
    if let Some(api) = &st.deps.ext.schedules {
        list = available(api.list_schedules().await)?;
    }
    let available = list.is_some();
    let rows = list
        .unwrap_or_default()
        .into_iter()
        .map(|s| ScheduleRow::of(s, None))
        .collect();
    let (csrf, set_cookie) = page_csrf(&headers, &uri)?;
    let html = SchedulesPage {
        title: "Schedules".to_owned(),
        csrf,
        nav: "schedules",
        available,
        rows,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

fn schedule_row(
    m: &SameOriginMutation,
    headers: &HeaderMap,
    s: ScheduleDto,
    started: Option<String>,
) -> Response {
    row_html(
        m,
        ScheduleRowFragment {
            r: ScheduleRow::of(s, started),
            csrf: csrf_of(headers),
        }
        .render(),
    )
}

async fn schedule_set_paused(
    st: &UiState,
    headers: &HeaderMap,
    m: &SameOriginMutation,
    name: String,
    pause: bool,
) -> Response {
    let api = match schedule_api(st) {
        Ok(a) => a,
        Err(e) => return fail(m, e),
    };
    let r = if pause {
        api.pause_schedule(name).await
    } else {
        api.resume_schedule(name).await
    };
    match r {
        Ok(_) if !m.is_htmx() => Redirect::to("/ui/schedules").into_response(),
        Ok(s) => schedule_row(m, headers, s, None),
        Err(e) => fail(m, e.into()),
    }
}

/// `POST /ui/schedules/{name}/pause`.
async fn schedule_pause(
    State(st): State<UiState>,
    _op: Operator,
    Path(name): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    schedule_set_paused(&st, &headers, &m, name, true).await
}

/// `POST /ui/schedules/{name}/resume`.
async fn schedule_resume(
    State(st): State<UiState>,
    _op: Operator,
    Path(name): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    schedule_set_paused(&st, &headers, &m, name, false).await
}

/// `POST /ui/schedules/{name}/trigger`: fire now. A plain form lands on the started
/// run; htmx gets the row with a link to it.
async fn schedule_trigger(
    State(st): State<UiState>,
    _op: Operator,
    Path(name): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    let api = match schedule_api(&st) {
        Ok(a) => a,
        Err(e) => return fail(&m, e),
    };
    let run_id = match api.trigger_schedule(name.clone(), None).await {
        Ok(id) => id,
        Err(e) => return fail(&m, e.into()),
    };
    if !m.is_htmx() {
        return Redirect::to(&crate::pages::run_href(&run_id)).into_response();
    }
    match api.get_schedule(name).await {
        Ok(s) => schedule_row(&m, &headers, s, Some(run_id)),
        Err(e) => fail(&m, e.into()),
    }
}

// ---------------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------------

pub(crate) struct WorkerRow {
    id: String,
    href_id: String,
    labels: Vec<String>,
    draining: bool,
    leases: u32,
    slots: String,
    node: u64,
    connected: String,
}

impl WorkerRow {
    fn of(w: WorkerInfo) -> Self {
        WorkerRow {
            href_id: href_encode(&w.worker_id),
            labels: w.labels.iter().map(|(k, v)| format!("{k}={v}")).collect(),
            draining: w.draining,
            leases: w.leases,
            slots: format!("{} free of {}", w.free_slots, w.slots),
            node: w.node_id,
            connected: fmt_ts(w.connected_at_ms),
            id: w.worker_id,
        }
    }
}

#[derive(Template)]
#[template(path = "ui/workers.html")]
struct WorkersPage {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    rows: Vec<WorkerRow>,
}

#[derive(Template)]
#[template(path = "ui/_worker_row.html")]
struct WorkerRowFragment {
    r: WorkerRow,
    csrf: String,
}

/// `GET /ui/workers`: connected workers (Phase 7 `list_workers`).
async fn workers(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    let list = available(st.deps.cp.list_workers().await)?;
    let available = list.is_some();
    let rows = list
        .unwrap_or_default()
        .into_iter()
        .map(WorkerRow::of)
        .collect();
    let (csrf, set_cookie) = page_csrf(&headers, &uri)?;
    let html = WorkersPage {
        title: "Workers".to_owned(),
        csrf,
        nav: "workers",
        available,
        rows,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

/// `POST /ui/workers/{id}/drain` (idempotent).
async fn worker_drain(
    State(st): State<UiState>,
    _op: Operator,
    Path(id): Path<String>,
    headers: HeaderMap,
    m: SameOriginMutation,
) -> Response {
    if let Err(e) = st.deps.cp.drain_worker(id.clone()).await {
        return fail(&m, e.into());
    }
    if !m.is_htmx() {
        return Redirect::to("/ui/workers").into_response();
    }
    let worker = match st.deps.cp.list_workers().await {
        Ok(list) => list.into_iter().find(|w| w.worker_id == id),
        Err(e) => return fail(&m, e.into()),
    };
    match worker {
        Some(w) => row_html(
            &m,
            WorkerRowFragment {
                r: WorkerRow::of(w),
                csrf: csrf_of(&headers),
            }
            .render(),
        ),
        None => fail(&m, UiError::NotFound(format!("worker {id} disconnected"))),
    }
}

// ---------------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------------

struct PartitionRow {
    id: u16,
    role: String,
    leader: String,
    epoch: String,
    applied: String,
    log_end: String,
    lag: String,
    ready: String,
    running: String,
    healthy: bool,
    detail: String,
    /// Applied / log end as an SVG bar width (0..=[`BAR_WIDTH`]); `None` when either
    /// offset is unknown.
    bar: Option<u64>,
}

impl PartitionRow {
    fn of(p: PartitionView) -> Self {
        let bar = match (p.applied_offset, p.log_end_offset) {
            (Some(_), Some(0)) => Some(BAR_WIDTH),
            (Some(a), Some(e)) => Some(a.min(e).saturating_mul(BAR_WIDTH) / e),
            _ => None,
        };
        let lag = match (p.applied_offset, p.log_end_offset) {
            (Some(a), Some(e)) => e.saturating_sub(a).to_string(),
            _ => DASH.to_owned(),
        };
        PartitionRow {
            id: p.id,
            role: p.role,
            leader: dash(p.leader),
            epoch: dash(p.epoch),
            applied: dash(p.applied_offset),
            log_end: dash(p.log_end_offset),
            lag,
            ready: dash(p.ready),
            running: dash(p.running),
            healthy: p.healthy,
            detail: p.detail.unwrap_or_default(),
            bar,
        }
    }
}

#[derive(Template)]
#[template(path = "ui/partitions.html")]
struct PartitionsPage {
    title: String,
    csrf: String,
    nav: &'static str,
    available: bool,
    rows: Vec<PartitionRow>,
    bar_width: u64,
}

#[derive(Template)]
#[template(path = "ui/_partitions.html")]
struct PartitionsFragment {
    available: bool,
    rows: Vec<PartitionRow>,
    bar_width: u64,
}

async fn partition_rows(st: &UiState) -> Result<Option<Vec<PartitionRow>>, UiError> {
    let Some(inv) = &st.deps.ext.inventory else {
        return Ok(None);
    };
    Ok(available(inv.list_partitions().await)?
        .map(|ps| ps.into_iter().map(PartitionRow::of).collect()))
}

/// `GET /ui/partitions`: partition role, offsets and health (polls every 5 s).
async fn partitions(
    State(st): State<UiState>,
    _reader: Reader,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, UiError> {
    let rows = partition_rows(&st).await?;
    let (csrf, set_cookie) = page_csrf(&headers, &uri)?;
    let html = PartitionsPage {
        title: "Partitions".to_owned(),
        csrf,
        nav: "partitions",
        available: rows.is_some(),
        rows: rows.unwrap_or_default(),
        bar_width: BAR_WIDTH,
    }
    .render()?;
    Ok(page_response(html, set_cookie))
}

/// `GET /ui/partitions/fragment`: the partition table (the page's 5 s poll target).
async fn partitions_fragment(
    State(st): State<UiState>,
    _reader: Reader,
) -> Result<Response, UiError> {
    let rows = partition_rows(&st).await?;
    let html = PartitionsFragment {
        available: rows.is_some(),
        rows: rows.unwrap_or_default(),
        bar_width: BAR_WIDTH,
    }
    .render()?;
    Ok(Html(html).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_and_flow_formatting() {
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1536), "1.5 KiB");
        assert_eq!(fmt_bytes(3 * 1024 * 1024 + 200 * 1024), "3.1 MiB");
        assert_eq!(flow_summary(None), DASH);
        assert_eq!(flow_summary(Some(&serde_json::json!({}))), DASH);
        assert_eq!(
            flow_summary(Some(
                &serde_json::json!({"concurrency": 4, "placement": "workers", "x": null})
            )),
            "concurrency=4, placement=workers"
        );
    }

    #[test]
    fn partition_bar_geometry() {
        let p = |a: Option<u64>, e: Option<u64>| PartitionView {
            id: 0,
            role: "leader".to_owned(),
            leader: None,
            epoch: None,
            applied_offset: a,
            log_end_offset: e,
            ready: None,
            running: None,
            healthy: true,
            detail: None,
        };
        assert_eq!(PartitionRow::of(p(Some(50), Some(100))).bar, Some(60));
        assert_eq!(PartitionRow::of(p(Some(0), Some(0))).bar, Some(BAR_WIDTH));
        assert_eq!(
            PartitionRow::of(p(Some(200), Some(100))).bar,
            Some(BAR_WIDTH)
        );
        assert_eq!(PartitionRow::of(p(None, Some(100))).bar, None);
        assert_eq!(PartitionRow::of(p(None, Some(100))).lag, DASH);
    }

    #[test]
    fn phase7_placeholder_is_not_available() {
        assert!(not_available(&ApiError::unavailable(
            ostro_api::phase7::P7_PENDING
        )));
        assert!(!not_available(&ApiError::unavailable("busy")));
        assert!(not_available(&ApiError::new(ErrorCode::Unimplemented, "x")));
    }
}
