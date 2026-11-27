//! 14-03 Task 3: the guard decorator (D-P14-6, B2, N2).
//!
//! - completeness: every `Method` is served by exactly one guarded wrapper method;
//! - deny by default: outside any `PRINCIPAL` scope every guarded call is 401 and the inner
//!   method never runs; inside a `System` scope it passes through;
//! - passthrough equivalence with the 14-01b passthrough hooks (fakes behind both);
//! - hook order, observed through the guard's TRACE events (no test-only hook seam);
//! - a released context answers 503;
//! - full server: `[auth] mode = "none"` serves REST and a Phase 11 UI page with real run
//!   data behind the real guard; token mode refuses a missing bearer and accepts the
//!   bootstrap bearer.

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_api::auth::{self, Method, Principal};
use ostro_api::domain::{ListRunsRequest, RunInfo, StartRunRequest, UploadModule};
use ostro_api::fake::{
    FakeCluster, FakeControlPlane, FakeInventory, FakeModuleLifecycle, FakeOps, FakePhase8,
    fake_module, sha256_hex,
};
use ostro_api::phase8::ScheduleDto;
use ostro_api::phase11::ListModulesRequest;
use ostro_api::phase14::{ApiTokensApi, AuditApi, AuditQuery, SecurityApi, TenantsApi};
use ostro_api::{ApiError, ApiExtensions, ControlPlane, ErrorCode};
use ostro_meta::MetaHandle;
use ostro_partition::Clock;
use ostro_partition::clock::ManualClock;
use ostro_server::config::{AuthMode, ServerConfig};
use ostro_server::p14::guard::{self, TRACE_TARGET};
use ostro_server::p14::{self, Guarded, P14Init};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};

use common::{ServerOpts, TestServer};

// ---------------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------------

/// A control plane that counts every call that reaches it, delegating to the fake.
#[derive(Default)]
struct Counting {
    fake: FakeControlPlane,
    calls: AtomicUsize,
}

impl Counting {
    fn hit(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ControlPlane for Counting {
    async fn upload_module(
        &self,
        req: UploadModule,
    ) -> Result<ostro_api::domain::UploadModuleResult, ApiError> {
        self.hit();
        self.fake.upload_module(req).await
    }
    async fn get_module(&self, sha256: String) -> Result<ostro_api::domain::ModuleInfo, ApiError> {
        self.hit();
        self.fake.get_module(sha256).await
    }
    async fn list_workflows(&self) -> Result<Vec<ostro_api::domain::WorkflowInfo>, ApiError> {
        self.hit();
        self.fake.list_workflows().await
    }
    async fn activate_workflow(
        &self,
        req: ostro_api::domain::ActivateWorkflow,
    ) -> Result<ostro_api::domain::WorkflowInfo, ApiError> {
        self.hit();
        self.fake.activate_workflow(req).await
    }
    async fn start_run(
        &self,
        req: StartRunRequest,
    ) -> Result<ostro_api::domain::StartRunResult, ApiError> {
        self.hit();
        self.fake.start_run(req).await
    }
    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        self.hit();
        self.fake.get_run(run_id).await
    }
    async fn run_history(
        &self,
        req: ostro_api::domain::HistoryRequest,
    ) -> Result<ostro_api::domain::HistoryPage, ApiError> {
        self.hit();
        self.fake.run_history(req).await
    }
    async fn list_runs(
        &self,
        req: ListRunsRequest,
    ) -> Result<ostro_api::domain::RunPage, ApiError> {
        self.hit();
        self.fake.list_runs(req).await
    }
    fn ready(&self) -> bool {
        self.fake.ready()
    }
    async fn cancel_run(
        &self,
        r: ostro_api::domain::CancelRequest,
    ) -> Result<ostro_api::domain::CancelResponse, ApiError> {
        self.hit();
        self.fake.cancel_run(r).await
    }
    async fn list_secrets(
        &self,
        tenant: Option<String>,
    ) -> Result<Vec<ostro_api::domain::SecretInfo>, ApiError> {
        self.hit();
        self.fake.list_secrets(tenant).await
    }
}

/// Phase 14 slots before their plans: every method is the trait default (Unimplemented).
struct Empty;
impl TenantsApi for Empty {}
impl ApiTokensApi for Empty {}
impl AuditApi for Empty {}
impl SecurityApi for Empty {}

struct Fixture {
    routed: Arc<Counting>,
    routed_ext: ApiExtensions,
    guarded: Guarded,
    ctx: Arc<p14::P14Ctx>,
    _dir: tempfile::TempDir,
}

fn full_ext(cp: &Arc<FakeControlPlane>) -> ApiExtensions {
    let mut ext = FakePhase8::new(Arc::clone(cp)).extensions();
    ext.ops = Some(FakeOps::new());
    ext.inventory = Some(FakeInventory::new());
    ext.cluster = Some(FakeCluster::new());
    ext.module_lifecycle = Some(FakeModuleLifecycle::new());
    ext.tenants = Some(Arc::new(Empty));
    ext.api_tokens = Some(Arc::new(Empty));
    ext.audit = Some(Arc::new(Empty));
    ext.security = Some(Arc::new(Empty));
    ext
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = Arc::new(MetaHandle::start(dir.path(), 1).await.expect("meta"));
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_000));
    let mut cfg = ServerConfig::server_defaults();
    cfg.auth.mode = AuthMode::None;
    let ctx = p14::build_ctx(P14Init {
        cfg: Arc::new(cfg),
        meta,
        clock,
        node_id: 1,
        data_dir: dir.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("build_ctx");
    let routed = Arc::new(Counting::default());
    let routed_ext = full_ext(&Arc::new(FakeControlPlane::new()));
    let guarded = p14::wrap_ingress(
        &ctx,
        Arc::clone(&routed) as Arc<dyn ControlPlane>,
        routed_ext.clone(),
    );
    Fixture {
        routed,
        routed_ext,
        guarded,
        ctx,
        _dir: dir,
    }
}

fn code<T: std::fmt::Debug>(r: &Result<T, ApiError>) -> Option<ErrorCode> {
    r.as_ref().err().map(|e| e.code)
}

/// One call per guarded wrapper (and a few more on the control plane): each result's error
/// code (`None` = Ok).
async fn probe_all(g: &Guarded) -> Vec<(&'static str, Option<ErrorCode>)> {
    let c = &g.control;
    let e = &g.ext;
    vec![
        ("list_workflows", code(&c.list_workflows().await)),
        ("get_run", code(&c.get_run("r-1".into()).await)),
        (
            "list_runs",
            code(&c.list_runs(ListRunsRequest::default()).await),
        ),
        (
            "start_run",
            code(
                &c.start_run(StartRunRequest {
                    workflow: "wf".into(),
                    ..StartRunRequest::default()
                })
                .await,
            ),
        ),
        ("list_secrets", code(&c.list_secrets(None).await)),
        (
            "schedules",
            code(&e.schedules.as_ref().unwrap().list_schedules().await),
        ),
        (
            "webhooks",
            code(&e.webhooks.as_ref().unwrap().list_webhooks().await),
        ),
        (
            "run_logs",
            code(
                &e.run_logs
                    .as_ref()
                    .unwrap()
                    .run_logs(ostro_api::phase8::RunLogsRequest {
                        run_id: "r-1".into(),
                        limit: 10,
                        level: None,
                    })
                    .await,
            ),
        ),
        ("admin", code(&e.admin.as_ref().unwrap().backup().await)),
        ("ops", code(&e.ops.as_ref().unwrap().limits().await)),
        (
            "inventory",
            code(
                &e.inventory
                    .as_ref()
                    .unwrap()
                    .list_modules(ListModulesRequest::default())
                    .await,
            ),
        ),
        ("cluster", code(&e.cluster.as_ref().unwrap().status().await)),
        (
            "module_lifecycle",
            code(&e.module_lifecycle.as_ref().unwrap().run_module_gc().await),
        ),
        (
            "tenants",
            code(&e.tenants.as_ref().unwrap().list_tenants().await),
        ),
        (
            "api_tokens",
            code(&e.api_tokens.as_ref().unwrap().whoami().await),
        ),
        (
            "audit",
            code(&e.audit.as_ref().unwrap().query(AuditQuery::default()).await),
        ),
        (
            "security",
            code(&e.security.as_ref().unwrap().get_module_policy().await),
        ),
    ]
}

// ---------------------------------------------------------------------------------
// Completeness
// ---------------------------------------------------------------------------------

#[test]
fn every_method_is_guarded_exactly_once() {
    let guarded = guard::guarded_methods();
    let unique: BTreeSet<Method> = guarded.iter().copied().collect();
    assert_eq!(unique.len(), guarded.len(), "a method is guarded twice");
    let all: BTreeSet<Method> = Method::all().iter().copied().collect();
    let missing: Vec<_> = all.difference(&unique).collect();
    let extra: Vec<_> = unique.difference(&all).collect();
    assert!(missing.is_empty(), "unguarded methods: {missing:?}");
    assert!(extra.is_empty(), "unknown methods: {extra:?}");
    // The compile-time match agrees with the wrapper each method is listed in.
    let lists: [(&str, &[Method]); 14] = [
        ("GuardedControlPlane", guard::GuardedControlPlane::METHODS),
        ("GuardedSchedules", guard::GuardedSchedules::METHODS),
        ("GuardedWebhooks", guard::GuardedWebhooks::METHODS),
        ("GuardedHooks", guard::GuardedHooks::METHODS),
        ("GuardedRunLogs", guard::GuardedRunLogs::METHODS),
        ("GuardedAdmin", guard::GuardedAdmin::METHODS),
        ("GuardedOps", guard::GuardedOps::METHODS),
        ("GuardedInventory", guard::GuardedInventory::METHODS),
        ("GuardedCluster", guard::GuardedCluster::METHODS),
        (
            "GuardedModuleLifecycle",
            guard::GuardedModuleLifecycle::METHODS,
        ),
        ("GuardedTenants", guard::GuardedTenants::METHODS),
        ("GuardedApiTokens", guard::GuardedApiTokens::METHODS),
        ("GuardedAudit", guard::GuardedAudit::METHODS),
        ("GuardedSecurity", guard::GuardedSecurity::METHODS),
    ];
    for (name, methods) in lists {
        for m in methods {
            assert_eq!(guard::wrapper_of(*m), name, "{m:?}");
            // Each wrapper serves exactly its trait's inventory rows.
            let api = m.spec().api;
            assert_eq!(
                name.trim_start_matches("Guarded"),
                wrapper_api(api),
                "{m:?}"
            );
        }
    }
    assert_eq!(guard::wrapper_of(Method::Ready), "GuardedControlPlane");
}

fn wrapper_api(api: &str) -> &str {
    match api {
        "ScheduleApi" => "Schedules",
        "WebhookApi" => "Webhooks",
        "HookIngress" => "Hooks",
        "RunLogsApi" => "RunLogs",
        "AdminApi" => "Admin",
        "OpsApi" => "Ops",
        "InventoryApi" => "Inventory",
        "ClusterApi" => "Cluster",
        "ModuleLifecycleApi" => "ModuleLifecycle",
        "TenantsApi" => "Tenants",
        "ApiTokensApi" => "ApiTokens",
        "AuditApi" => "Audit",
        "SecurityApi" => "Security",
        other => other,
    }
}

// ---------------------------------------------------------------------------------
// Deny by default (B2)
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calls_outside_any_principal_scope_are_401_and_never_reach_inner() {
    let f = fixture().await;
    assert_eq!(auth::current(), Principal::anonymous());
    for (what, c) in probe_all(&f.guarded).await {
        assert_eq!(c, Some(ErrorCode::Unauthenticated), "{what}");
    }
    assert_eq!(
        f.routed.calls.load(Ordering::SeqCst),
        0,
        "inner was invoked"
    );
    // Spawned work loses the task-local: still denied.
    let g = f.guarded.clone();
    let r = auth::scope(Principal::system(), async move {
        tokio::spawn(async move { g.control.list_workflows().await })
            .await
            .unwrap()
    })
    .await;
    assert_eq!(code(&r), Some(ErrorCode::Unauthenticated));
    assert_eq!(f.routed.calls.load(Ordering::SeqCst), 0);
    // `ready` is a pass-through probe.
    assert_eq!(f.guarded.control.ready(), f.routed.ready());

    // Inside a System (or Dev, or token) scope the same calls pass through. (14-15: scopes
    // are enforced, so the token holds system admin to pass every probe.)
    for p in [
        Principal::system(),
        Principal::dev(),
        Principal::token(
            "otk_aaaaaaaaaaaaaaaa",
            None,
            ostro_api::auth::ScopeSet::admin(),
        ),
    ] {
        let results = auth::scope(p.clone(), probe_all(&f.guarded)).await;
        for (what, c) in results {
            assert_ne!(c, Some(ErrorCode::Unauthenticated), "{what} as {p:?}");
        }
    }
    assert!(f.routed.calls.load(Ordering::SeqCst) >= 3 * 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_released_context_answers_503() {
    let f = fixture().await;
    let weak = Arc::downgrade(&f.ctx);
    let g = guard::wrap_weak(
        weak,
        Arc::clone(&f.routed) as Arc<dyn ControlPlane>,
        ApiExtensions::default(),
    );
    drop(f.guarded);
    let ctx = f.ctx;
    drop(ctx);
    let r = auth::scope(Principal::system(), g.control.list_workflows()).await;
    assert_eq!(code(&r), Some(ErrorCode::Unavailable));
    assert_eq!(f.routed.calls.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------------
// Passthrough equivalence (14-01b hooks)
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_instances_behave_like_the_routed_ones() {
    let f = fixture().await;
    let g = f.guarded.clone();
    let routed: Arc<dyn ControlPlane> = f.routed.clone();
    auth::scope(Principal::system(), async move {
        // Same sequence through both: upload through the guard, read through both.
        let wasm = fake_module(&[("orders", 1), ("billing", 2)]);
        let sha = sha256_hex(&wasm);
        let up = g
            .control
            .upload_module(UploadModule {
                sha256: sha.clone(),
                wasm,
                language: None,
                tenant: None,
                name: None,
                signatures: Vec::new(),
            })
            .await
            .expect("upload");
        assert_eq!(up.module.sha256, sha);
        assert_eq!(
            format!("{:?}", g.control.list_workflows().await),
            format!("{:?}", routed.list_workflows().await)
        );
        assert_eq!(
            format!("{:?}", g.control.get_module(sha.clone()).await),
            format!("{:?}", routed.get_module(sha.clone()).await)
        );
        let started = g
            .control
            .start_run(StartRunRequest {
                workflow: "orders".into(),
                input_json: b"{}".to_vec(),
                ..StartRunRequest::default()
            })
            .await
            .expect("start");
        assert_eq!(
            format!("{:?}", g.control.get_run(started.run_id.clone()).await),
            format!("{:?}", routed.get_run(started.run_id.clone()).await)
        );
        assert_eq!(
            format!(
                "{:?}",
                g.control.list_runs(ListRunsRequest::default()).await
            ),
            format!("{:?}", routed.list_runs(ListRunsRequest::default()).await)
        );
        // Errors map identically.
        assert_eq!(
            g.control.get_run("missing".into()).await.unwrap_err(),
            routed.get_run("missing".into()).await.unwrap_err()
        );

        // One call per extension trait through both.
        let (ge, re) = (&g.ext, &f.routed_ext);
        let sched = ScheduleDto {
            name: "nightly".into(),
            workflow: "orders".into(),
            cron: "0 0 * * *".into(),
            ..ScheduleDto::default()
        };
        let a = ge.schedules.as_ref().unwrap().create_schedule(sched).await;
        assert_eq!(
            format!("{a:?}"),
            format!(
                "{:?}",
                re.schedules
                    .as_ref()
                    .unwrap()
                    .get_schedule("nightly".into())
                    .await
            )
        );
        assert_eq!(
            format!("{:?}", ge.ops.as_ref().unwrap().limits().await),
            format!("{:?}", re.ops.as_ref().unwrap().limits().await)
        );
        assert_eq!(
            format!("{:?}", ge.inventory.as_ref().unwrap().list_queues().await),
            format!("{:?}", re.inventory.as_ref().unwrap().list_queues().await)
        );
        assert_eq!(
            format!("{:?}", ge.cluster.as_ref().unwrap().status().await),
            format!("{:?}", re.cluster.as_ref().unwrap().status().await)
        );
        assert_eq!(
            format!(
                "{:?}",
                ge.module_lifecycle.as_ref().unwrap().run_module_gc().await
            ),
            format!(
                "{:?}",
                re.module_lifecycle.as_ref().unwrap().run_module_gc().await
            )
        );
        assert_eq!(
            format!("{:?}", ge.tenants.as_ref().unwrap().list_tenants().await),
            format!("{:?}", re.tenants.as_ref().unwrap().list_tenants().await)
        );
        assert_eq!(
            format!(
                "{:?}",
                ge.security.as_ref().unwrap().get_module_policy().await
            ),
            format!(
                "{:?}",
                re.security.as_ref().unwrap().get_module_policy().await
            )
        );
        assert_eq!(ge.hook_body_limit, re.hook_body_limit);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_slots_stay_empty() {
    let f = fixture().await;
    let g = p14::wrap_ingress(
        &f.ctx,
        Arc::clone(&f.routed) as Arc<dyn ControlPlane>,
        ApiExtensions::default(),
    );
    assert!(g.ext.schedules.is_none() && g.ext.ops.is_none() && g.ext.cluster.is_none());
    assert!(g.ext.tenants.is_none() && g.ext.security.is_none());
    assert!(!Arc::ptr_eq(
        &g.control,
        &(Arc::clone(&f.routed) as Arc<dyn ControlPlane>)
    ));
}

// ---------------------------------------------------------------------------------
// Hook order through tracing capture
// ---------------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Stages(Arc<Mutex<Vec<(String, String)>>>);

struct StageVisitor {
    method: String,
    stage: String,
}

impl Visit for StageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "stage" {
            self.stage = value.to_owned();
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "method" => self.method = format!("{value:?}"),
            "stage" => self.stage = format!("{value:?}").trim_matches('"').to_owned(),
            _ => {}
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Stages {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != TRACE_TARGET {
            return;
        }
        let mut v = StageVisitor {
            method: String::new(),
            stage: String::new(),
        };
        event.record(&mut v);
        self.0.lock().unwrap().push((v.method, v.stage));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn hooks_run_in_the_fixed_order() {
    let stages = Stages::default();
    let subscriber = tracing_subscriber::registry().with(stages.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    let f = fixture().await;
    // Other tests of this binary hit the guard's callsites concurrently on threads without a
    // subscriber, and a callsite registered then may cache "never". Warm every guard callsite
    // up once (denied + allowed call), then recompute the cached interest: from here on all
    // of them are registered and see this thread's subscriber.
    let _ = f.guarded.ext.ops.as_ref().unwrap().limits().await;
    let _ = auth::scope(
        Principal::system(),
        f.guarded.ext.ops.as_ref().unwrap().limits(),
    )
    .await;
    tracing::callsite::rebuild_interest_cache();
    stages.0.lock().unwrap().clear();

    // Denied: nothing but the denial.
    let _ = f
        .guarded
        .control
        .cancel_run(ostro_api::domain::CancelRequest {
            run_id: "r-1".into(),
            reason: None,
            dry_run: false,
        })
        .await;
    assert_eq!(
        *stages.0.lock().unwrap(),
        vec![("CancelRun".to_owned(), "deny_anonymous".to_owned())]
    );
    stages.0.lock().unwrap().clear();

    // Allowed: authz → tenant_before → audit_reserve → inner → audit_after → tenant_after.
    let _ = auth::scope(
        Principal::system(),
        f.guarded.ext.ops.as_ref().unwrap().limits(),
    )
    .await;
    let got: Vec<String> = stages
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|(m, s)| {
            assert_eq!(m, "Limits");
            s.clone()
        })
        .collect();
    assert_eq!(
        got,
        [
            "authz",
            "tenant_before",
            "audit_reserve",
            "inner",
            "audit_after",
            "tenant_after"
        ]
    );
}

// ---------------------------------------------------------------------------------
// Full server behind the real guard
// ---------------------------------------------------------------------------------

const LIMIT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mode_none_serves_rest_and_the_ui_with_real_runs() {
    let s = TestServer::start_with(ServerOpts::default(), |c| c.auth.mode = AuthMode::None).await;
    s.wait_ready(LIMIT).await;
    s.deploy_guest("sdk-basic", None).await;
    let run = s
        .start_rest("sdk-echo", serde_json::json!({"input": {"i": 7}}))
        .await;
    // REST through the 14-02 layer (dev principal) and the guard.
    let (st, v) = s.get_json(&format!("/v1/runs/{run}")).await;
    assert_eq!(st, reqwest::StatusCode::OK, "{v}");
    // A Phase 11 UI page (the /ui scope layer runs it as the dev principal).
    let r = s
        .http
        .get(s.url("/ui/runs"))
        .send()
        .await
        .expect("GET /ui/runs");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    let body = r.text().await.expect("body");
    assert!(body.contains(&run.to_string()), "run {run} not on /ui/runs");
    // No admin token is bootstrapped in mode none.
    assert!(!s.h().data_dir().join("admin.token").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_mode_needs_the_bearer() {
    let s = TestServer::start_with(ServerOpts::default(), |c| c.auth.mode = AuthMode::Token).await;
    s.wait_ready(LIMIT).await;
    let r = s.http.get(s.url("/v1/runs")).send().await.expect("GET");
    assert_eq!(r.status(), reqwest::StatusCode::UNAUTHORIZED);
    let r = s
        .http
        .get(s.url("/v1/runs"))
        .bearer_auth("ostro_otk_aaaaaaaaaaaaaaaa_garbage")
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), reqwest::StatusCode::UNAUTHORIZED);
    let bearer = std::fs::read_to_string(s.h().data_dir().join("admin.token")).expect("token");
    let r = s
        .http
        .get(s.url("/v1/runs"))
        .bearer_auth(bearer.trim())
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    // Probes stay unauthenticated.
    let r = s.http.get(s.url("/healthz")).send().await.expect("GET");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
}
