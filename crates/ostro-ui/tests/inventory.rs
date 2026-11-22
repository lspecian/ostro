//! Inventory views and actions (11-17, UI-03, UI-02) against the fakes: modules,
//! workflows/versions (+pause/resume), queues (+pause/resume), schedules
//! (+pause/resume/trigger), workers (+drain) and partition health. Every view renders
//! "not available" (200) without its slot; every action is guarded (same origin, CSRF
//! token, `operate` scope), answers a row fragment to htmx and 303 to a plain form.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
use common::{EVIL_TAG, HOST, Resp, TOKEN, Ui, get_ok, seed, seed_inventory, send, ui, ui_bare};
use http::{Request, StatusCode};
use ostro_api::domain::{
    ActivateWorkflow, HistoryPage, HistoryRequest, ListRunsRequest, ModuleInfo, RunInfo, RunPage,
    StartRunRequest, StartRunResult, UploadModule, UploadModuleResult, WorkflowInfo,
};
use ostro_api::fake::FakeControlPlane;
use ostro_api::phase8::ScheduleApi;
use ostro_api::{ApiError, ControlPlane, ErrorCode};
use ostro_ui::UiDeps;
use ostro_ui::auth::{AllowAll, UiAuth, UiPrincipal, UiReject};

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

/// The `<main>` element: the page minus the shared layout head.
fn main_of(html: &str) -> &str {
    let start = html.find("<main").expect("main");
    let end = html.rfind("</main>").expect("/main") + "</main>".len();
    &html[start..end]
}

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

/// How a mutating request arrives.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Via {
    /// htmx: `HX-Request` + `X-Ostro-Csrf` header token.
    Htmx,
    /// Plain form post with the `csrf` field.
    Form,
    /// A cross-site form post with a valid token.
    CrossSite,
    /// A same-origin form post without any token.
    NoToken,
}

fn post(via: Via, path: &str) -> Request<Body> {
    let body = if matches!(via, Via::Form | Via::CrossSite) {
        format!("csrf={}", enc(TOKEN))
    } else {
        String::new()
    };
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
    b.body(Body::from(body)).expect("request")
}

async fn act(router: &axum::Router, via: Via, path: &str) -> Resp {
    send(router, post(via, path)).await
}

fn assert_row(r: &Resp, what: &str) {
    assert_eq!(r.status, StatusCode::OK, "{what}: {}", r.body);
    assert_eq!(
        r.header("content-type"),
        Some("text/html; charset=utf-8"),
        "{what}"
    );
    assert!(r.body.starts_with("<tr id="), "{what}: {}", r.body);
    assert!(r.header("hx-retarget").is_none(), "{what}");
}

fn assert_303(r: &Resp, to: &str, what: &str) {
    assert_eq!(r.status, StatusCode::SEE_OTHER, "{what}: {}", r.body);
    assert_eq!(r.header("location"), Some(to), "{what}");
}

/// An htmx failure: 200 into `#flash`.
fn assert_flash(r: &Resp, needle: &str, what: &str) {
    assert_eq!(r.status, StatusCode::OK, "{what}: {}", r.body);
    assert_eq!(r.header("hx-retarget"), Some("#flash"), "{what}");
    assert!(r.body.contains(needle), "{what}: {}", r.body);
}

/// Cross-site and tokenless posts of `path` are 403 and change nothing (fake clock).
async fn assert_guarded(ui: &Ui, path: &str) {
    for via in [Via::CrossSite, Via::NoToken] {
        let t0 = ui.cp.clock_ticks();
        let r = act(&ui.router, via, path).await;
        assert_eq!(
            r.status,
            StatusCode::FORBIDDEN,
            "{via:?} {path}: {}",
            r.body
        );
        assert_eq!(ui.cp.clock_ticks(), t0, "{via:?} {path}: nothing called");
    }
}

async fn seeded() -> (Ui, common::Inventory) {
    let ui = ui();
    seed(&ui).await;
    let inv = seed_inventory(&ui).await;
    (ui, inv)
}

async fn schedule(ui: &Ui, name: &str) -> ostro_api::phase8::ScheduleDto {
    ui.p8.get_schedule(name.to_owned()).await.expect("schedule")
}

async fn run_count(ui: &Ui) -> usize {
    ui.cp
        .list_runs(ListRunsRequest {
            limit: 1000,
            ..ListRunsRequest::default()
        })
        .await
        .expect("runs")
        .runs
        .len()
}

// ---------------------------------------------------------------------------------
// Navigation
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn nav_links_every_view() {
    let (ui, _) = seeded().await;
    let r = get_ok(&ui, "/ui/modules").await;
    let start = r.body.find("<header").expect("header");
    let end = r.body.find("</header>").expect("/header") + "</header>".len();
    insta::assert_snapshot!("layout_nav", &r.body[start..end]);
    for (path, label) in [
        ("/ui/runs", "Runs"),
        ("/ui/dlq", "DLQ"),
        ("/ui/bulk", "Bulk"),
        ("/ui/modules", "Modules"),
        ("/ui/workflows", "Workflows"),
        ("/ui/queues", "Queues"),
        ("/ui/schedules", "Schedules"),
        ("/ui/workers", "Workers"),
        ("/ui/partitions", "Partitions"),
    ] {
        let page = get_ok(&ui, path).await;
        assert!(
            page.body.contains(&format!(r#"<a href="{path}""#)),
            "{path} linked"
        );
        if path != "/ui/bulk" {
            assert!(
                page.body.contains(&format!(
                    r#"<a href="{path}" aria-current="page">{label}</a>"#
                )),
                "{path} is the current page"
            );
        }
    }
}

// ---------------------------------------------------------------------------------
// Modules
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn modules_page_lists_sha_size_workflows_and_upload_time() {
    let (ui, inv) = seeded().await;
    let r = get_ok(&ui, "/ui/modules").await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("modules", html);
    // Short sha shown, full sha in the title.
    assert!(html.contains(&format!(
        r#"<td class="mono" title="{}">{}</td>"#,
        inv.module_sha,
        &inv.module_sha[..12]
    )));
    assert!(html.contains("1.5 KiB") && html.contains("3.0 MiB"));
    assert!(html.contains(r#"href="/ui/runs?workflow=greet""#));
    assert!(html.contains("2023-11-14 22:13:20Z"));
    // Hostile module names are escaped.
    assert!(!r.body.contains(EVIL_TAG));
    // The page token reaches list_modules (the fake pages by sha).
    let r = get_ok(&ui, &format!("/ui/modules?page_token={}", "e".repeat(64))).await;
    assert!(r.body.contains(&"f".repeat(12)));
    assert!(!r.body.contains(&inv.module_sha[..12]));
}

// ---------------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn workflows_page_shows_versions_and_the_active_one() {
    let (ui, _) = seeded().await;
    let r = get_ok(&ui, "/ui/workflows").await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("workflows", html);
    assert!(
        html.contains(
            r#"<li class="active"><span class="mono">v1</span> <strong>(active)</strong>"#
        )
    );
    assert!(html.contains(r#"<a href="/ui/runs?workflow=billing""#));
    // Pause state is unknown on the list: both buttons.
    assert!(html.contains(r#"action="/ui/workflows/greet/pause""#));
    assert!(html.contains(r#"action="/ui/workflows/greet/resume""#));
}

#[tokio::test]
async fn workflows_pause_and_resume_are_guarded_actions() {
    let (ui, _) = seeded().await;
    let path = "/ui/workflows/greet/pause";
    assert_guarded(&ui, path).await;
    let t0 = ui.cp.clock_ticks();
    let r = act(&ui.router, Via::Htmx, path).await;
    assert_row(&r, "pause");
    assert_eq!(ui.cp.clock_ticks(), t0 + 1, "pause_workflow called once");
    assert!(
        r.body
            .contains(r#"<span class="badge paused">paused</span>"#)
    );
    assert!(!r.body.contains(r#"action="/ui/workflows/greet/pause""#));
    assert!(r.body.contains(r#"action="/ui/workflows/greet/resume""#));
    assert!(r.body.contains(&format!(r#"name="csrf" value="{TOKEN}""#)));
    let r = act(&ui.router, Via::Form, "/ui/workflows/greet/resume").await;
    assert_303(&r, "/ui/workflows", "resume form");
    assert_eq!(ui.cp.clock_ticks(), t0 + 2, "resume_workflow called once");
    let r = act(&ui.router, Via::Htmx, "/ui/workflows/greet/resume").await;
    assert_row(&r, "idempotent resume");
    assert!(r.body.contains("resumed"));
    // Unknown workflow: flash for htmx, 404 page for a form.
    let r = act(&ui.router, Via::Htmx, "/ui/workflows/nope/pause").await;
    assert_flash(&r, "workflow nope not found", "unknown htmx");
    let r = act(&ui.router, Via::Form, "/ui/workflows/nope/pause").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------------
// Partitions
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn partitions_page_and_fragment_render_health_with_svg_bars() {
    let (ui, _) = seeded().await;
    let r = get_ok(&ui, "/ui/partitions").await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("partitions", html);
    // 90 of 120 applied: 90/120 of the 120-unit bar.
    assert!(html.contains(r#"<rect class="fill" x="0" y="0" width="90" height="10">"#));
    assert!(html.contains(r#"<tr class="unhealthy">"#));
    assert!(html.contains("apply loop stalled &#60;x&#62;"));
    // Unknown fields render as a dash, never as 0.
    let follower = &html[html.find(r#"<tr class="unhealthy">"#).expect("row")..];
    assert!(follower.contains("<td class=\"mono\">\u{2014}</td>"));
    assert!(!follower.contains("<svg"));
    assert!(!r.body.contains("style="));
    // The fragment is the poll target and keeps polling every 5 s.
    let f = get_ok(&ui, "/ui/partitions/fragment").await;
    assert!(f.body.starts_with(
        r#"<div id="partitions" hx-get="/ui/partitions/fragment" hx-trigger="every 5s" hx-swap="outerHTML">"#
    ));
    assert!(!f.body.contains("<html"));
    assert!(html.contains(f.body.trim()));
}

// ---------------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn workers_page_lists_labels_state_and_leases() {
    let (ui, _) = seeded().await;
    let r = get_ok(&ui, "/ui/workers").await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("workers", html);
    assert!(
        html.contains(r#"<span class="chip">gpu=a100</span><span class="chip">region=eu</span>"#)
    );
    assert!(html.contains(r#"action="/ui/workers/wrk-1/drain""#));
    assert!(html.contains(r#"hx-confirm="Drain worker wrk-1?"#));
    // An already draining worker has no drain button.
    assert!(!html.contains(r#"action="/ui/workers/wrk-2/drain""#));
}

#[tokio::test]
async fn workers_drain_is_a_guarded_action() {
    let (ui, _) = seeded().await;
    let path = "/ui/workers/wrk-1/drain";
    assert_guarded(&ui, path).await;
    let t0 = ui.cp.clock_ticks();
    let r = act(&ui.router, Via::Htmx, path).await;
    assert_row(&r, "drain");
    assert_eq!(ui.cp.clock_ticks(), t0 + 1, "drain_worker called once");
    assert!(
        r.body
            .contains(r#"<span class="badge draining">draining</span>"#)
    );
    assert!(!r.body.contains("<form"));
    let r = act(&ui.router, Via::Form, path).await;
    assert_303(&r, "/ui/workers", "drain form");
    assert_eq!(ui.cp.clock_ticks(), t0 + 1, "idempotent: no second tick");
    let r = act(&ui.router, Via::Htmx, "/ui/workers/wrk-9/drain").await;
    assert_flash(&r, "worker wrk-9 not found", "unknown");
    let r = act(&ui.router, Via::Form, "/ui/workers/wrk-9/drain").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn queues_page_lists_state_counts_demand_and_flow() {
    let (ui, _) = seeded().await;
    let r = get_ok(&ui, "/ui/queues").await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("queues", html);
    assert!(html.contains("concurrency=4, placement=workers"));
    assert!(html.contains(r#"action="/ui/queues/default/pause""#));
    assert!(html.contains(r#"action="/ui/queues/payments/resume""#));
    assert!(html.contains(r#"href="/ui/runs?queue=payments""#));
}

#[tokio::test]
async fn queues_pause_and_resume_are_guarded_actions() {
    let (ui, _) = seeded().await;
    let path = "/ui/queues/default/pause";
    assert_guarded(&ui, path).await;
    let t0 = ui.cp.clock_ticks();
    let r = act(&ui.router, Via::Htmx, path).await;
    assert_row(&r, "pause");
    assert_eq!(ui.cp.clock_ticks(), t0 + 1, "pause_queue called once");
    // The row keeps the inventory counts and shows the new state.
    assert!(
        r.body
            .contains(r#"<span class="badge paused">paused</span>"#)
    );
    assert!(r.body.contains("<td>3</td>"), "{}", r.body);
    assert!(r.body.contains(r#"action="/ui/queues/default/resume""#));
    let r = act(&ui.router, Via::Form, "/ui/queues/default/resume").await;
    assert_303(&r, "/ui/queues", "resume form");
    assert_eq!(ui.cp.clock_ticks(), t0 + 2, "resume_queue called once");
    // A queue the inventory does not list yet: counts unknown.
    let r = act(&ui.router, Via::Htmx, "/ui/queues/fresh/pause").await;
    assert_row(&r, "unlisted queue");
    assert!(r.body.contains("<td>\u{2014}</td>"));
}

// ---------------------------------------------------------------------------------
// Schedules
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn schedules_page_lists_cron_timezone_and_fires() {
    let (ui, _) = seeded().await;
    let r = get_ok(&ui, "/ui/schedules").await;
    let html = main_of(&r.body);
    insta::assert_snapshot!("schedules", html);
    assert!(html.contains("Europe/Berlin") && html.contains("<td>UTC</td>"));
    assert!(html.contains(r#"hx-confirm="Start a run of schedule nightly now?""#));
    assert!(html.contains(r#"action="/ui/schedules/nightly/pause""#));
    assert!(html.contains(r#"action="/ui/schedules/hourly/resume""#));
}

#[tokio::test]
async fn schedules_pause_resume_and_trigger_are_guarded_actions() {
    let (ui, _) = seeded().await;
    for a in ["pause", "resume", "trigger"] {
        let path = format!("/ui/schedules/nightly/{a}");
        let (tag, runs) = (
            schedule(&ui, "nightly").await.version_tag,
            run_count(&ui).await,
        );
        assert_guarded(&ui, &path).await;
        assert_eq!(schedule(&ui, "nightly").await.version_tag, tag, "{a}");
        assert_eq!(run_count(&ui).await, runs, "{a}");
    }
    let r = act(&ui.router, Via::Htmx, "/ui/schedules/nightly/pause").await;
    assert_row(&r, "pause");
    assert!(schedule(&ui, "nightly").await.paused);
    assert_eq!(schedule(&ui, "nightly").await.version_tag, Some(2));
    assert!(
        r.body
            .contains(r#"<span class="badge paused">paused</span>"#)
    );
    assert!(r.body.contains(r#"action="/ui/schedules/nightly/resume""#));
    let r = act(&ui.router, Via::Form, "/ui/schedules/nightly/resume").await;
    assert_303(&r, "/ui/schedules", "resume form");
    assert!(!schedule(&ui, "nightly").await.paused);
    assert_eq!(schedule(&ui, "nightly").await.version_tag, Some(3));

    // Trigger via htmx: the row links the started run.
    let runs = run_count(&ui).await;
    let r = act(&ui.router, Via::Htmx, "/ui/schedules/nightly/trigger").await;
    assert_row(&r, "trigger");
    assert_eq!(
        run_count(&ui).await,
        runs + 1,
        "trigger_schedule called once"
    );
    let started = r
        .body
        .find("started</span> <a class=\"mono\" href=\"/ui/runs/")
        .expect("link");
    let id = r.body[started..]
        .split("href=\"/ui/runs/")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .expect("run id")
        .to_owned();
    let run = ui.cp.get_run(id.clone()).await.expect("started run");
    assert_eq!(run.workflow, "greet");
    // Trigger via a plain form lands on the run.
    let r = act(&ui.router, Via::Form, "/ui/schedules/hourly/trigger").await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    let loc = r.header("location").expect("location");
    assert!(
        loc.starts_with("/ui/runs/") && loc != format!("/ui/runs/{id}"),
        "{loc}"
    );
    assert_eq!(run_count(&ui).await, runs + 2);
    // Unknown schedule.
    let r = act(&ui.router, Via::Htmx, "/ui/schedules/nope/trigger").await;
    assert_flash(&r, "schedule nope not found", "unknown");
    let r = act(&ui.router, Via::Form, "/ui/schedules/nope/pause").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------------
// "Not available"
// ---------------------------------------------------------------------------------

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = Result<T, ApiError>> + Send + 'a>>;

/// A control plane with only the required methods (delegating to the fake): every
/// Phase 7 call answers the default "lands in 07-14" placeholder, and the workflow list
/// is Unimplemented.
struct BareCp(Arc<FakeControlPlane>);

// `ControlPlane` is an `#[async_trait]` trait; ostro-ui has no async-trait
// dev-dependency, so the boxed signatures are spelled out.
impl ControlPlane for BareCp {
    fn upload_module<'a, 'b>(&'a self, req: UploadModule) -> BoxFut<'b, UploadModuleResult>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.0.upload_module(req))
    }
    fn get_module<'a, 'b>(&'a self, sha256: String) -> BoxFut<'b, ModuleInfo>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.0.get_module(sha256))
    }
    fn list_workflows<'a, 'b>(&'a self) -> BoxFut<'b, Vec<WorkflowInfo>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async { Err(ApiError::new(ErrorCode::Unimplemented, "no workflows")) })
    }
    fn activate_workflow<'a, 'b>(&'a self, req: ActivateWorkflow) -> BoxFut<'b, WorkflowInfo>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.0.activate_workflow(req))
    }
    fn start_run<'a, 'b>(&'a self, req: StartRunRequest) -> BoxFut<'b, StartRunResult>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.0.start_run(req))
    }
    fn get_run<'a, 'b>(&'a self, run_id: String) -> BoxFut<'b, RunInfo>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.0.get_run(run_id))
    }
    fn run_history<'a, 'b>(&'a self, req: HistoryRequest) -> BoxFut<'b, HistoryPage>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.0.run_history(req))
    }
    fn list_runs<'a, 'b>(&'a self, req: ListRunsRequest) -> BoxFut<'b, RunPage>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(self.0.list_runs(req))
    }
    fn ready(&self) -> bool {
        true
    }
}

const VIEWS: [(&str, &str); 6] = [
    ("/ui/modules", "Modules are not available on this server."),
    (
        "/ui/workflows",
        "Workflows are not available on this server.",
    ),
    ("/ui/queues", "Queues are not available on this server."),
    (
        "/ui/schedules",
        "Schedules are not available on this server.",
    ),
    ("/ui/workers", "Workers are not available on this server."),
    (
        "/ui/partitions",
        "Partition health is not available on this server.",
    ),
];

#[tokio::test]
async fn inventory_views_render_not_available_without_their_slots() {
    // No extension slots, Phase 7 placeholders and an Unimplemented workflow list.
    let bare = ui_bare();
    let router = ostro_ui::router(UiDeps {
        cp: Arc::new(BareCp(Arc::clone(&bare.cp))),
        ext: ostro_api::ApiExtensions::default(),
        cfg: common::cfg(),
        auth: Arc::new(AllowAll),
    });
    for (path, text) in VIEWS {
        let r = send(&router, common::get(path)).await;
        assert_eq!(r.status, StatusCode::OK, "{path}: {}", r.body);
        assert!(r.body.contains(text), "{path}: {}", r.body);
        assert!(!r.body.contains("<table"), "{path}");
    }
    let r = send(&router, common::get("/ui/partitions/fragment")).await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.body.contains("Partition health is not available"));
    assert!(
        !r.body.contains("hx-trigger"),
        "no polling without the slot"
    );
    // Actions on a server without the calls: flash for htmx, the API status for forms.
    let r = act(&router, Via::Htmx, "/ui/workers/wrk-1/drain").await;
    assert_flash(&r, ostro_api::phase7::P7_PENDING, "drain");
    let r = act(&router, Via::Form, "/ui/queues/default/pause").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    let r = act(&router, Via::Htmx, "/ui/schedules/nightly/trigger").await;
    assert_flash(&r, "schedules are not available", "trigger");

    // The inventory slot present but its methods Unimplemented: "not available" too.
    let ui = ui();
    for m in ["list_modules", "list_queues", "list_partitions"] {
        ui.inventory.fail(
            m,
            Some(ApiError::new(
                ErrorCode::Unimplemented,
                format!("{m} lands in 11-11"),
            )),
        );
    }
    for (path, text) in [VIEWS[0], VIEWS[2], VIEWS[5]] {
        let r = get_ok(&ui, path).await;
        assert!(r.body.contains(text), "{path}");
    }
    // A real failure is shown with its status (not hidden as "not available").
    ui.inventory.fail(
        "list_queues",
        Some(ApiError::unavailable("partition 3 has no leader")),
    );
    let r = send(&ui.router, common::get("/ui/queues")).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(r.body.contains("partition 3 has no leader"));
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

/// No scope at all.
struct DenyAll;

impl UiAuth for DenyAll {
    fn principal(&self, _parts: &http::request::Parts) -> Result<UiPrincipal, UiReject> {
        Ok(UiPrincipal { scopes: Vec::new() })
    }
}

const MUTATIONS: [&str; 8] = [
    "/ui/workflows/greet/pause",
    "/ui/workflows/greet/resume",
    "/ui/queues/default/pause",
    "/ui/queues/payments/resume",
    "/ui/schedules/nightly/pause",
    "/ui/schedules/hourly/resume",
    "/ui/schedules/nightly/trigger",
    "/ui/workers/wrk-1/drain",
];

#[tokio::test]
async fn scope_operate_is_required_on_every_inventory_action() {
    let (ui, _) = seeded().await;
    let mut ext = ui.p8.extensions();
    ui.inventory.install(&mut ext);
    for (auth, pages_ok) in [
        (Arc::new(ReadOnly) as Arc<dyn UiAuth>, true),
        (Arc::new(DenyAll) as Arc<dyn UiAuth>, false),
    ] {
        let router = ostro_ui::router(UiDeps {
            cp: Arc::clone(&ui.cp) as Arc<dyn ControlPlane>,
            ext: ext.clone(),
            cfg: common::cfg(),
            auth,
        });
        let runs = run_count(&ui).await;
        for path in MUTATIONS {
            for via in [Via::Htmx, Via::Form] {
                let t0 = ui.cp.clock_ticks();
                let r = act(&router, via, path).await;
                assert_eq!(
                    r.status,
                    StatusCode::FORBIDDEN,
                    "{via:?} {path}: {}",
                    r.body
                );
                assert!(r.body.contains("missing scope"), "{path}");
                assert_eq!(ui.cp.clock_ticks(), t0, "{path}: nothing called");
            }
        }
        assert_eq!(run_count(&ui).await, runs, "no trigger");
        assert!(!schedule(&ui, "nightly").await.paused);
        assert!(schedule(&ui, "hourly").await.paused);
        for (path, _) in VIEWS {
            let r = send(&router, common::get(path)).await;
            let want = if pages_ok {
                StatusCode::OK
            } else {
                StatusCode::FORBIDDEN
            };
            assert_eq!(r.status, want, "{path}");
        }
    }
}
