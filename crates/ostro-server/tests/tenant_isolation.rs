//! 14-16 Task 1: tenant isolation (SEC-02, D-P14-7, D-P14-9, D-P14-38, B7, W11).
//!
//! - every guarded method accepts its request and response types under a tenant principal
//!   (`p14::tenant_scope` downcasts per method; the sweep calls all of them over the
//!   ostro-api fakes);
//! - full server in `[auth] mode = "token"` with tenants `a` and `b` (meta records) and
//!   bearer tokens per tenant plus a system `read` token:
//!   - the same workflow name and idempotency key in `a` and `b` are two runs; the same
//!     module bytes uploaded by both register both tenants (ext 114 lists both);
//!   - lists (runs, workflows, modules, queues, schedules, webhooks, secrets, grants,
//!     connections, egress, DLQ, bulk jobs) hold only the caller's items with unqualified
//!     names; the system token sees both tenants with qualified names;
//!   - `b` on `a`'s run id (every run-id method, DLQ entries, await token, bulk job) is 404
//!     with the body of a missing run, and `a`'s run is unchanged;
//!   - qualified names are 400, foreign tenant fields 403, system-only methods 403; an
//!     upload naming another tenant leaves no records;
//!   - every `StartRun` site keeps the tenant: retry, rerun, bulk rerun, child,
//!     on-failure hook, continue-as-new (cron and webhook: `tenant_triggers.rs`).

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ostro_api::auth::{self, Method, Principal, ScopeSet};
use ostro_api::domain::{
    AwaitResultRequest, CancelRequest, CompleteTokenRequest, ConnectionInfo, EgressInfo, GrantInfo,
    HistoryRequest, ListRunsRequest, PresignMethod, PresignedObjectRequest, ReplayRequest,
    ReplaySelect, RetryRequest, RunPolicy, SignalRequest, StartRunRequest, TerminateRequest,
};
use ostro_api::fake::{
    FakeCluster, FakeControlPlane, FakeInventory, FakeModuleLifecycle, FakeOps, FakePhase8,
};
use ostro_api::phase7::{CreateWorkerTokenRequest, JoinRequest as WorkerJoin, QueueFlow};
use ostro_api::phase8::{
    HookRequest, RunLogsRequest, ScheduleDto, VerifierDto, WebhookActionDto, WebhookDto,
};
use ostro_api::phase11::{
    BulkAction, BulkCreateRequest, BulkFilter, DlqDismissRequest, DlqListRequest,
    DlqRequeueRequest, FailurePolicy, ListModulesRequest, PolicyScope, RequeueMode, RerunFrom,
    RerunRequest, RerunVersion,
};
use ostro_api::phase13::{
    InitRequest, JoinRequest as NodeJoin, MigrateRunsRequest, NodeTokenRequest, ReassignRequest,
    VersionOrSha,
};
use ostro_api::phase14::{
    ApiTokensApi, AttachSignaturesRequest, AuditApi, AuditQuery, CreateTokenRequest, ModulePolicy,
    RotateTokenRequest, SecurityApi, TenantPatch, TenantSpec, TenantsApi, TrustedKeySpec,
};
use ostro_api::{ApiError, ApiExtensions, ControlPlane};
use ostro_meta::ext::{ClusterVersion, get_module_uploaders, put_cluster_version};
use ostro_meta::{
    ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, Tenant,
};
use ostro_partition::Clock;
use ostro_partition::clock::ManualClock;
use ostro_partition::store::values::RunStatus;
use ostro_server::config::{AuthMode, ServerConfig};
use ostro_server::p14::{self, P14Init, tokens};
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};

use common::{BridgeTestOpts, TestServer, bridge_server_config, guest_wasm, tmpfs_dir};

const LIMIT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------------
// Part 1: the request/response type sweep over the fakes
// ---------------------------------------------------------------------------------

struct Empty;
impl TenantsApi for Empty {}
impl ApiTokensApi for Empty {}
impl AuditApi for Empty {}
impl SecurityApi for Empty {}

fn tenant(t: &str) -> Principal {
    Principal::token("otk_sweep", Some(t.to_owned()), ScopeSet::admin())
}

fn unit<T>(r: Result<T, ApiError>) -> Result<(), ApiError> {
    r.map(|_| ())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_method_scopes_its_request_and_response_types() {
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
    let fake = Arc::new(FakeControlPlane::new());
    let mut ext: ApiExtensions = FakePhase8::new(Arc::clone(&fake)).extensions();
    ext.ops = Some(FakeOps::new());
    ext.inventory = Some(FakeInventory::new());
    ext.cluster = Some(FakeCluster::new());
    ext.module_lifecycle = Some(FakeModuleLifecycle::new());
    ext.tenants = Some(Arc::new(Empty));
    ext.api_tokens = Some(Arc::new(Empty));
    ext.audit = Some(Arc::new(Empty));
    ext.security = Some(Arc::new(Empty));
    let g = p14::wrap_ingress(&ctx, fake as Arc<dyn ControlPlane>, ext);

    let results: Vec<(Method, Result<(), ApiError>)> = auth::scope(tenant("a"), async {
        let c = &g.control;
        let e = &g.ext;
        let s = e.schedules.as_ref().expect("schedules");
        let w = e.webhooks.as_ref().expect("webhooks");
        let h = e.hooks.as_ref().expect("hooks");
        let l = e.run_logs.as_ref().expect("run_logs");
        let ad = e.admin.as_ref().expect("admin");
        let o = e.ops.as_ref().expect("ops");
        let i = e.inventory.as_ref().expect("inventory");
        let cl = e.cluster.as_ref().expect("cluster");
        let ml = e.module_lifecycle.as_ref().expect("module_lifecycle");
        let te = e.tenants.as_ref().expect("tenants");
        let tk = e.api_tokens.as_ref().expect("api_tokens");
        let au = e.audit.as_ref().expect("audit");
        let se = e.security.as_ref().expect("security");
        let sha = "ab".repeat(32);
        let run = "r-1".to_owned();
        use Method::*;
        vec![
            (
                UploadModule,
                unit(
                    c.upload_module(ostro_api::domain::UploadModule {
                        sha256: sha.clone(),
                        wasm: bytes::Bytes::new(),
                        language: None,
                        tenant: None,
                        name: Some("m".into()),
                        signatures: vec![],
                    })
                    .await,
                ),
            ),
            (GetModule, unit(c.get_module(sha.clone()).await)),
            (ListWorkflows, unit(c.list_workflows().await)),
            (
                ActivateWorkflow,
                unit(
                    c.activate_workflow(ostro_api::domain::ActivateWorkflow {
                        name: "w".into(),
                        version: 1,
                        expected_version: None,
                    })
                    .await,
                ),
            ),
            (
                StartRun,
                unit(
                    c.start_run(StartRunRequest {
                        workflow: "w".into(),
                        ..Default::default()
                    })
                    .await,
                ),
            ),
            (GetRun, unit(c.get_run(run.clone()).await)),
            (
                RunHistory,
                unit(
                    c.run_history(HistoryRequest {
                        run_id: run.clone(),
                        from_seq: 0,
                        limit: 10,
                    })
                    .await,
                ),
            ),
            (
                ListRuns,
                unit(c.list_runs(ListRunsRequest::default()).await),
            ),
            (
                SignalRun,
                unit(
                    c.signal_run(SignalRequest {
                        run_id: run.clone(),
                        name: "x".into(),
                        payload: Value::Null,
                        request_id: None,
                        dry_run: false,
                    })
                    .await,
                ),
            ),
            (
                CancelRun,
                unit(
                    c.cancel_run(CancelRequest {
                        run_id: run.clone(),
                        reason: None,
                        dry_run: false,
                    })
                    .await,
                ),
            ),
            (
                TerminateRun,
                unit(
                    c.terminate_run(TerminateRequest {
                        run_id: run.clone(),
                        reason: None,
                        dry_run: false,
                    })
                    .await,
                ),
            ),
            (
                RetryRun,
                unit(
                    c.retry_run(RetryRequest {
                        run_id: run.clone(),
                        dry_run: false,
                    })
                    .await,
                ),
            ),
            (
                AwaitResult,
                unit(
                    c.await_result(AwaitResultRequest {
                        run_id: run.clone(),
                        wait_ms: 0,
                        follow_continuations: false,
                    })
                    .await,
                ),
            ),
            (
                CompleteToken,
                unit(
                    c.complete_token(CompleteTokenRequest {
                        token: "ot1_garbage".into(),
                        payload: Value::Null,
                        dry_run: false,
                    })
                    .await,
                ),
            ),
            (
                SetWorkflowPolicy,
                unit(
                    c.set_workflow_policy("w".into(), RunPolicy::default())
                        .await,
                ),
            ),
            (
                GetWorkflowPolicy,
                unit(c.get_workflow_policy("w".into()).await),
            ),
            (
                SetQueuePolicy,
                unit(c.set_queue_policy("q".into(), RunPolicy::default()).await),
            ),
            (GetQueuePolicy, unit(c.get_queue_policy("q".into()).await)),
            (
                PutSecret,
                unit(
                    c.put_secret(ostro_api::domain::PutSecret {
                        name: "s".into(),
                        value: b"0123456789abcdef".to_vec(),
                        tenant: None,
                    })
                    .await,
                ),
            ),
            (ListSecrets, unit(c.list_secrets(None).await)),
            (DeleteSecret, unit(c.delete_secret("s".into(), None).await)),
            (
                PutGrant,
                unit(
                    c.put_grant(GrantInfo {
                        id: String::new(),
                        module: "m".into(),
                        secret: "s".into(),
                        origins: vec!["https://x.example".into()],
                        body: false,
                    })
                    .await,
                ),
            ),
            (ListGrants, unit(c.list_grants(Some("m".into())).await)),
            (DeleteGrant, unit(c.delete_grant("secret:m:s".into()).await)),
            (
                PutConnection,
                unit(
                    c.put_connection(ConnectionInfo {
                        name: "db".into(),
                        kind: "sql-postgres".into(),
                        config: Value::Null,
                    })
                    .await,
                ),
            ),
            (GetConnection, unit(c.get_connection("db".into()).await)),
            (ListConnections, unit(c.list_connections().await)),
            (
                DeleteConnection,
                unit(c.delete_connection("db".into()).await),
            ),
            (
                PutEgress,
                unit(
                    c.put_egress(EgressInfo {
                        module: "m".into(),
                        allow: vec![],
                    })
                    .await,
                ),
            ),
            (GetEgress, unit(c.get_egress("m".into()).await)),
            (ListEgress, unit(c.list_egress().await)),
            (DeleteEgress, unit(c.delete_egress("m".into()).await)),
            (
                PresignedObject,
                unit(
                    c.presigned_object(PresignedObjectRequest {
                        connection: "c".into(),
                        key: "k".into(),
                        method: PresignMethod::Get,
                        exp: 0,
                        sig: String::new(),
                        body: None,
                        content_type: None,
                    })
                    .await,
                ),
            ),
            (GetQueueFlow, unit(c.get_queue_flow("q".into()).await)),
            (
                PutQueueFlow,
                unit(
                    c.put_queue_flow("q".into(), QueueFlow::default(), None)
                        .await,
                ),
            ),
            (PauseQueue, unit(c.pause_queue("q".into()).await)),
            (ResumeQueue, unit(c.resume_queue("q".into()).await)),
            (PauseWorkflow, unit(c.pause_workflow("w".into()).await)),
            (ResumeWorkflow, unit(c.resume_workflow("w".into()).await)),
            (PauseRun, unit(c.pause_run(run.clone(), None).await)),
            (ResumeRun, unit(c.resume_run(run.clone()).await)),
            (QueueDemand, unit(c.queue_demand("q".into()).await)),
            (ListWorkers, unit(c.list_workers().await)),
            (DrainWorker, unit(c.drain_worker("w1".into()).await)),
            (
                CreateWorkerToken,
                unit(
                    c.create_worker_token(CreateWorkerTokenRequest::default())
                        .await,
                ),
            ),
            (ListWorkerTokens, unit(c.list_worker_tokens().await)),
            (
                RevokeWorkerToken,
                unit(c.revoke_worker_token("t".into()).await),
            ),
            (
                JoinWorker,
                unit(
                    c.join_worker(WorkerJoin {
                        token: String::new(),
                        csr_pem: String::new(),
                        worker_id: String::new(),
                        labels: Default::default(),
                    })
                    .await,
                ),
            ),
            (DevTime, unit(c.dev_time().await)),
            (
                DevSkip,
                unit(c.dev_skip(ostro_api::domain::DevSkip::By { ms: 1 }).await),
            ),
            (
                ReplayModule,
                unit(
                    c.replay_module(ReplayRequest {
                        module_sha256: sha.clone(),
                        select: ReplaySelect::Runs(vec![run.clone()]),
                        allow_missing: false,
                    })
                    .await,
                ),
            ),
            (ListSchedules, unit(s.list_schedules().await)),
            (GetSchedule, unit(s.get_schedule("n".into()).await)),
            (
                CreateSchedule,
                unit(
                    s.create_schedule(ScheduleDto {
                        name: "n".into(),
                        workflow: "w".into(),
                        ..Default::default()
                    })
                    .await,
                ),
            ),
            (
                UpdateSchedule,
                unit(
                    s.update_schedule(ScheduleDto {
                        name: "n".into(),
                        workflow: "w".into(),
                        ..Default::default()
                    })
                    .await,
                ),
            ),
            (
                DeleteSchedule,
                unit(s.delete_schedule("n".into(), None).await),
            ),
            (PauseSchedule, unit(s.pause_schedule("n".into()).await)),
            (ResumeSchedule, unit(s.resume_schedule("n".into()).await)),
            (
                TriggerSchedule,
                unit(s.trigger_schedule("n".into(), None).await),
            ),
            (ListWebhooks, unit(w.list_webhooks().await)),
            (GetWebhook, unit(w.get_webhook("h".into()).await)),
            (CreateWebhook, unit(w.create_webhook(webhook("h")).await)),
            (UpdateWebhook, unit(w.update_webhook(webhook("h")).await)),
            (
                DeleteWebhook,
                unit(w.delete_webhook("h".into(), None).await),
            ),
            (
                DeliverHook,
                unit(
                    h.deliver(HookRequest {
                        name: "h".into(),
                        headers: Default::default(),
                        body: bytes::Bytes::new(),
                    })
                    .await,
                ),
            ),
            (
                RunLogs,
                unit(
                    l.run_logs(RunLogsRequest {
                        run_id: run.clone(),
                        ..Default::default()
                    })
                    .await,
                ),
            ),
            (Backup, unit(ad.backup().await)),
            (
                RerunRun,
                unit(
                    o.rerun_run(RerunRequest {
                        run_id: run.clone(),
                        from: RerunFrom::Seq(1),
                        version: RerunVersion::Active,
                        idempotency_key: None,
                        dry_run: true,
                    })
                    .await,
                ),
            ),
            (AnalyzeRerun, unit(o.analyze_rerun(run.clone()).await)),
            (
                ListDlq,
                unit(
                    o.list_dlq(DlqListRequest {
                        workflow: Some("w".into()),
                        ..Default::default()
                    })
                    .await,
                ),
            ),
            (
                RequeueDlq,
                unit(
                    o.requeue_dlq(DlqRequeueRequest {
                        run_id: run.clone(),
                        mode: RequeueMode::Auto,
                        dry_run: true,
                    })
                    .await,
                ),
            ),
            (
                DismissDlq,
                unit(
                    o.dismiss_dlq(DlqDismissRequest {
                        run_id: run.clone(),
                        reason: "x".into(),
                    })
                    .await,
                ),
            ),
            (
                CreateBulk,
                unit(
                    o.create_bulk(BulkCreateRequest {
                        action: BulkAction::Cancel,
                        filter: BulkFilter {
                            workflow: Some("w".into()),
                            ..Default::default()
                        },
                        rate_per_sec: None,
                        max_in_flight: None,
                        dry_run: true,
                    })
                    .await,
                ),
            ),
            (ListBulk, unit(o.list_bulk().await)),
            (GetBulk, unit(o.get_bulk("blk_x".into()).await)),
            (CancelBulk, unit(o.cancel_bulk("blk_x".into()).await)),
            (Limits, unit(o.limits().await)),
            (
                GetFailurePolicy,
                unit(
                    o.get_failure_policy(PolicyScope::Workflow("w".into()))
                        .await,
                ),
            ),
            (
                SetFailurePolicy,
                unit(
                    o.set_failure_policy(PolicyScope::Queue("q".into()), FailurePolicy::default())
                        .await,
                ),
            ),
            (
                ListModules,
                unit(i.list_modules(ListModulesRequest::default()).await),
            ),
            (ListQueues, unit(i.list_queues().await)),
            (ListPartitions, unit(i.list_partitions().await)),
            (ClusterInit, unit(cl.init(InitRequest::default()).await)),
            (ClusterJoin, unit(cl.join(NodeJoin::default()).await)),
            (ClusterStatus, unit(cl.status().await)),
            (
                CreateNodeToken,
                unit(cl.create_node_token(NodeTokenRequest::default()).await),
            ),
            (ClusterRemove, unit(cl.remove(2, false).await)),
            (ClusterDrain, unit(cl.drain(2).await)),
            (
                ClusterReassign,
                unit(cl.reassign(ReassignRequest::default()).await),
            ),
            (ClusterRebalance, unit(cl.rebalance().await)),
            (UpgradeStatus, unit(cl.upgrade_status().await)),
            (UpgradeFinalize, unit(cl.upgrade_finalize().await)),
            (UpgradeHold, unit(cl.upgrade_hold().await)),
            (UpgradeRelease, unit(cl.upgrade_release().await)),
            (RetireModule, unit(ml.retire_module(sha.clone()).await)),
            (RunModuleGc, unit(ml.run_module_gc().await)),
            (
                MigrateRuns,
                unit(
                    ml.migrate_runs(MigrateRunsRequest {
                        workflow: "w".into(),
                        from: VersionOrSha::Version(1),
                        to_version: 2,
                        filter: None,
                        limit: None,
                        dry_run: true,
                    })
                    .await,
                ),
            ),
            (
                CreateTenant,
                unit(te.create_tenant(TenantSpec::default()).await),
            ),
            (GetTenant, unit(te.get_tenant("a".into()).await)),
            (ListTenants, unit(te.list_tenants().await)),
            (
                UpdateTenant,
                unit(te.update_tenant("a".into(), TenantPatch::default()).await),
            ),
            (SuspendTenant, unit(te.suspend_tenant("a".into()).await)),
            (ResumeTenant, unit(te.resume_tenant("a".into()).await)),
            (DeleteTenant, unit(te.delete_tenant("a".into()).await)),
            (
                CreateApiToken,
                unit(tk.create_token(CreateTokenRequest::default()).await),
            ),
            (ListApiTokens, unit(tk.list_tokens(None).await)),
            (RevokeApiToken, unit(tk.revoke_token("otk_x".into()).await)),
            (
                RotateApiToken,
                unit(
                    tk.rotate_token("otk_x".into(), RotateTokenRequest::default())
                        .await,
                ),
            ),
            (WhoAmI, unit(tk.whoami().await)),
            (QueryAudit, unit(au.query(AuditQuery::default()).await)),
            (GetModulePolicy, unit(se.get_module_policy().await)),
            (
                SetModulePolicy,
                unit(se.set_module_policy(ModulePolicy::default()).await),
            ),
            (
                AddTrustedKey,
                unit(se.add_trusted_key(TrustedKeySpec::default()).await),
            ),
            (
                RemoveTrustedKey,
                unit(se.remove_trusted_key("k".into()).await),
            ),
            (
                AttachSignatures,
                unit(
                    se.attach_signatures(AttachSignaturesRequest {
                        sha256: sha.clone(),
                        signatures: vec![],
                    })
                    .await,
                ),
            ),
            (GetSealingStatus, unit(se.get_sealing_status().await)),
            (Reseal, unit(se.reseal().await)),
        ]
    })
    .await;

    // Every method but the unguarded `ready` was called exactly once.
    let called: Vec<Method> = results.iter().map(|(m, _)| *m).collect();
    let set: BTreeSet<Method> = called.iter().copied().collect();
    assert_eq!(set.len(), called.len(), "a method was swept twice");
    let want: BTreeSet<Method> = Method::all()
        .iter()
        .copied()
        .filter(|m| *m != Method::Ready)
        .collect();
    assert_eq!(set, want, "the sweep must call every guarded method");
    for (m, r) in &results {
        if let Err(e) = r {
            assert!(
                !e.message.contains("tenant_scope: unexpected"),
                "{m:?}: {}",
                e.message
            );
        }
    }
    // System-only methods are refused for tenant principals (14-15: the scope check
    // answers first with `missing_scope`, the tenant hook's `forbidden_tenant` backs it).
    for (m, r) in &results {
        if m.spec().tenant == ostro_api::auth::TenantClass::SystemOnly {
            let e = r.as_ref().expect_err("system-only must fail");
            assert!(
                matches!(e.reason(), Some("forbidden_tenant" | "missing_scope")),
                "{m:?}: {e:?}"
            );
        }
    }
}

fn webhook(name: &str) -> WebhookDto {
    WebhookDto {
        name: name.into(),
        action: WebhookActionDto::Start {
            workflow: "w".into(),
            version: None,
            input: "body".into(),
        },
        verifier: VerifierDto::default(),
        idempotency: None,
        version_tag: None,
    }
}

// ---------------------------------------------------------------------------------
// Part 2: full server, two tenants
// ---------------------------------------------------------------------------------

/// A REST client with one bearer.
#[derive(Clone)]
struct Api {
    http: reqwest::Client,
    base: String,
    bearer: String,
}

impl Api {
    async fn call(
        &self,
        m: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut rb = self
            .http
            .request(m, format!("{}{path}", self.base))
            .bearer_auth(&self.bearer);
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        let r = rb.send().await.expect("request");
        let st = r.status();
        let text = r.text().await.expect("body");
        let v = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        (st, v)
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.call(reqwest::Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }

    async fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.call(reqwest::Method::PUT, path, Some(body)).await
    }

    /// `PUT /v1/modules/{sha}[?tenant=]`.
    async fn upload(&self, guest: &str, tenant: Option<&str>) -> (StatusCode, Value, [u8; 32]) {
        let wasm = guest_wasm(guest);
        let sha: [u8; 32] = Sha256::digest(&wasm).into();
        let q = tenant.map(|t| format!("?tenant={t}")).unwrap_or_default();
        let r = self
            .http
            .put(format!("{}/v1/modules/{}{q}", self.base, hex(&sha)))
            .bearer_auth(&self.bearer)
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        let st = r.status();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        (st, v, sha)
    }

    async fn deploy(&self, guest: &str) -> [u8; 32] {
        let (st, v, sha) = self.upload(guest, None).await;
        assert!(st.is_success(), "deploy {guest}: {st} {v}");
        sha
    }

    /// Start expecting a new run; its id.
    async fn start(&self, workflow: &str, body: Value) -> RunId {
        let (st, v) = self
            .post(&format!("/v1/workflows/{workflow}/runs"), body)
            .await;
        assert_eq!(st, StatusCode::CREATED, "start {workflow}: {v}");
        v["run_id"]
            .as_str()
            .expect("run_id")
            .parse()
            .expect("run id")
    }

    /// The await token a `tokens` run publishes in its status.
    async fn published_token(&self, run: &RunId) -> String {
        let deadline = Instant::now() + LIMIT;
        loop {
            let (_, v) = self.get(&format!("/v1/runs/{run}")).await;
            if let Some(t) = v["published_status"]["token"].as_str() {
                return t.to_owned();
            }
            assert!(Instant::now() < deadline, "no token published: {v}");
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Every run id of `GET /v1/runs?{filter}` (all pages).
    async fn run_ids(&self, filter: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = format!("/v1/runs?limit=1000&{filter}");
            if let Some(t) = &token {
                q.push_str(&format!("&page_token={t}"));
            }
            let (st, v) = self.get(&q).await;
            assert_eq!(st, StatusCode::OK, "{q}: {v}");
            for r in v["runs"].as_array().expect("runs") {
                out.push(r["run_id"].as_str().expect("id").to_owned());
            }
            match v["next_page_token"].as_str().filter(|t| !t.is_empty()) {
                Some(t) => token = Some(t.to_owned()),
                None => return out,
            }
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

async fn propose(meta: &MetaHandle, key: MetaKey, record: MetaRecord) {
    let cur = meta.view().read(|s| s.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version: cur,
            record,
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

/// A fresh token stored in `meta`; returns its bearer.
async fn put_token(meta: &MetaHandle, scopes: &[&str], tenant: Option<&str>) -> String {
    let (id, secret, bearer) = tokens::generate_os().expect("generate");
    let rec = ApiToken {
        id: id.as_str().to_owned(),
        hash: tokens::hash(&secret),
        scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
        tenant: tenant.map(str::to_owned),
        created_at_ms: 1,
        expires_at_ms: None,
        ext: Vec::new(),
    };
    propose(meta, MetaKey::Token(rec.id.clone()), MetaRecord::Token(rec)).await;
    bearer.expose().to_owned()
}

/// Finalize feature level 2 (ext 114 is written from there on).
async fn set_finalized(meta: &MetaHandle, finalized: u32) {
    let cur = meta
        .view()
        .read(|s| s.cluster().cloned())
        .expect("cluster info");
    let mut info: ClusterInfo = cur.value;
    info.ext = put_cluster_version(
        &info.ext,
        &ClusterVersion {
            finalized,
            hold: None,
            finalized_at_ms: 1,
        },
    )
    .expect("ext 98");
    propose(meta, MetaKey::Cluster, MetaRecord::Cluster(info)).await;
}

struct Fx {
    s: TestServer,
    a: Api,
    b: Api,
    /// System scope, `read`.
    sys: Api,
    /// The bootstrap admin (system scope).
    admin: Api,
}

async fn fixture() -> Fx {
    let dir = tmpfs_dir();
    let mut cfg = bridge_server_config(
        dir.path(),
        &BridgeTestOpts {
            allow_private_loopback: false,
            ..BridgeTestOpts::default()
        },
    );
    cfg.auth.mode = AuthMode::Token;
    cfg.engine.hot_wait = Duration::from_millis(50).into();
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(LIMIT).await;
    let meta = s.h().meta();
    set_finalized(meta, 2).await;
    for t in ["a", "b"] {
        propose(
            meta,
            MetaKey::Tenant(t.into()),
            MetaRecord::Tenant(Tenant {
                id: t.into(),
                created_at_ms: 1,
                ..Tenant::default()
            }),
        )
        .await;
    }
    let base = s.url("");
    let base = base.trim_end_matches('/').to_owned();
    let api = |bearer: String| Api {
        http: reqwest::Client::new(),
        base: base.clone(),
        bearer,
    };
    let admin = std::fs::read_to_string(s.h().data_dir().join("admin.token")).expect("admin");
    Fx {
        a: api(put_token(meta, &["admin"], Some("a")).await),
        b: api(put_token(meta, &["admin"], Some("b")).await),
        sys: api(put_token(meta, &["read"], None).await),
        admin: api(admin.trim().to_owned()),
        s,
    }
}

async fn terminal(s: &TestServer, run: &RunId) -> ostro_partition::store::values::RunRecord {
    s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await
}

fn names(v: &Value, field: &str) -> Vec<String> {
    v.as_array()
        .unwrap_or_else(|| panic!("array expected: {v}"))
        .iter()
        .map(|x| x[field].as_str().unwrap_or_default().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn names_keys_and_lists_are_per_tenant() {
    let Fx {
        s,
        a,
        b,
        sys,
        admin,
    } = fixture().await;

    // Same module bytes from a, then b: both tenants are registered, ext 114 lists both.
    let sha = a.deploy("p11-hook").await;
    assert_eq!(b.deploy("p11-hook").await, sha);
    let uploaders = s
        .h()
        .meta()
        .view()
        .read(|st| st.module(&sha).map(|m| get_module_uploaders(&m.value.ext)))
        .expect("module")
        .expect("ext 114 decodes")
        .expect("ext 114 written");
    assert_eq!(uploaders.tenants, ["a", "b"]);
    let (st, m) = a.get(&format!("/v1/modules/{}", hex(&sha))).await;
    assert_eq!(st, StatusCode::OK, "{m}");
    let wf: BTreeSet<String> = names(&m["workflows"], "name").into_iter().collect();
    assert!(
        wf.contains("w") && wf.iter().all(|n| !n.contains('/')),
        "{m}"
    );
    let (_, m) = sys.get(&format!("/v1/modules/{}", hex(&sha))).await;
    let wf: BTreeSet<String> = names(&m["workflows"], "name").into_iter().collect();
    assert!(wf.contains("a/w") && wf.contains("b/w"), "{m}");

    // Same workflow name + idempotency key: two runs, one per tenant.
    let body = json!({"input": {}, "idempotency_key": "k1"});
    let ra = a.start("w", body.clone()).await;
    let rb = b.start("w", body.clone()).await;
    assert_ne!(ra, rb);
    let (st, again) = a.post("/v1/workflows/w/runs", body.clone()).await;
    assert!(st.is_success(), "{again}");
    assert_eq!(
        again["run_id"],
        ra.to_string(),
        "dedupe stays inside the tenant"
    );
    for (run, t) in [(&ra, "a"), (&rb, "b")] {
        let rec = terminal(&s, run).await;
        assert_eq!(rec.tenant.as_deref(), Some(t));
        assert_eq!(rec.workflow, format!("{t}/w"));
    }

    // Run lists: own runs only, unqualified; system sees both qualified.
    assert_eq!(a.run_ids("").await, [ra.to_string()]);
    assert_eq!(b.run_ids("order=newest").await, [rb.to_string()]);
    let (_, page) = a.get("/v1/runs").await;
    assert_eq!(page["runs"][0]["workflow"], "w", "{page}");
    let all: BTreeSet<String> = sys.run_ids("").await.into_iter().collect();
    assert!(all.contains(&ra.to_string()) && all.contains(&rb.to_string()));
    let (_, page) = sys.get("/v1/runs?tenant=a").await;
    assert_eq!(names(&page["runs"], "workflow"), ["a/w"]);

    // Meta lists.
    let (_, v) = a.get("/v1/workflows").await;
    let wf = names(&v["workflows"], "name");
    assert!(
        wf.contains(&"w".to_owned()) && wf.iter().all(|n| !n.contains('/')),
        "{v}"
    );
    let (_, v) = sys.get("/v1/workflows").await;
    assert!(
        v.to_string().contains("\"a/w\"") && v.to_string().contains("\"b/w\""),
        "{v}"
    );
    let (_, v) = a.get("/v1/modules").await;
    assert!(
        !v.to_string().contains("b/") && !v.to_string().contains("a/"),
        "{v}"
    );
    assert!(v.to_string().contains("\"w\""), "{v}");
    let (_, v) = a.get("/v1/queues").await;
    assert!(
        !v.to_string().contains("b/") && !v.to_string().contains("a/"),
        "{v}"
    );

    for api in [&a, &b] {
        let (st, v) = api
            .post(
                "/v1/schedules",
                json!({"name": "nightly", "workflow": "w", "cron": "0 0 3 * * *",
                       "input": {}, "paused": true}),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
        assert_eq!(
            (v["name"].as_str(), v["workflow"].as_str()),
            (Some("nightly"), Some("w"))
        );
        let (st, v) = api
            .post(
                "/v1/webhooks",
                json!({"name": "orders", "action": {"kind": "start", "workflow": "w", "input": "body"}}),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
        assert_eq!(v["name"], "orders");
        let (st, v) = api
            .put("/v1/secrets/api-key", json!({"value": "0123456789abcdef"}))
            .await;
        assert!(st.is_success(), "{v}");
        let (st, v) = api
            .put(
                "/v1/grants",
                json!({"module": "m", "secret": "api-key", "origins": ["https://x.example"]}),
            )
            .await;
        assert!(st.is_success(), "{v}");
        assert_eq!(
            (v["module"].as_str(), v["id"].as_str()),
            (Some("m"), Some("secret:m:api-key"))
        );
        let (st, v) = api
            .put("/v1/egress/m", json!({"allow": ["https://x.example"]}))
            .await;
        assert!(st.is_success(), "{v}");
        assert_eq!(v["module"], "m");
    }
    let (st, v) = admin
        .put(
            "/v1/connections/https://db.example",
            json!({"kind": "http-target", "config": {"v": 1}}),
        )
        .await;
    assert!(st.is_success(), "system admin creates a connection: {v}");

    for (api, mine) in [(&a, "a"), (&b, "b")] {
        let other = if mine == "a" { "b" } else { "a" };
        for path in [
            "/v1/schedules",
            "/v1/webhooks",
            "/v1/secrets",
            "/v1/grants",
            "/v1/egress",
        ] {
            let (st, v) = api.get(path).await;
            assert_eq!(st, StatusCode::OK, "{path}: {v}");
            let text = v.to_string();
            assert!(
                !text.contains(&format!("{other}/")) && !text.contains(&format!("{mine}/")),
                "{path}: {text}"
            );
        }
        let (_, v) = api.get("/v1/schedules").await;
        assert_eq!(names(&v["schedules"], "name"), ["nightly"], "{v}");
        // 14-17: connections are cluster-scoped; authz refuses tenant admins outright.
        let (st, v) = api.get("/v1/connections").await;
        assert_eq!(
            (st, reason(&v)),
            (StatusCode::FORBIDDEN, "missing_scope"),
            "connections are cluster-scoped: {v}"
        );
        assert!(!v.to_string().contains("db.example"), "{v}");
    }
    let (_, v) = sys.get("/v1/schedules").await;
    let mut got = names(&v["schedules"], "name");
    got.sort();
    assert_eq!(got, ["a/nightly", "b/nightly"]);
    let stored: Vec<String> = s
        .h()
        .meta()
        .view()
        .read(|st| st.secrets().map(|x| x.value.name.clone()).collect());
    assert!(
        stored.contains(&"a/api-key".into()) && stored.contains(&"b/api-key".into()),
        "{stored:?}"
    );
    let grants: Vec<String> = s
        .h()
        .meta()
        .view()
        .read(|st| st.grants().map(|g| g.value.id.clone()).collect());
    assert!(grants.contains(&"secret:a/m:api-key".into()), "{grants:?}");

    // Connections: system admins only (14-17 authz; 14-16's tenant hook stays behind it).
    let (st, v) = a.get("/v1/connections/https://db.example").await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::FORBIDDEN, "missing_scope"),
        "{v}"
    );
    let (st, v) = a
        .put(
            "/v1/connections/https://evil.example",
            json!({"kind": "http-target", "config": {"v": 1}}),
        )
        .await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::FORBIDDEN, "missing_scope"),
        "{v}"
    );

    // Qualified names are 400, foreign tenant fields 403, system-only methods 403.
    let (st, v) = a.get("/v1/runs?workflow=b/w").await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::BAD_REQUEST, "invalid_name"),
        "{v}"
    );
    let (st, v) = a.get("/v1/runs?workflow=a/w").await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "even the own prefix: {v}");
    let (st, v) = a
        .post(
            "/v1/schedules",
            json!({"name": "a/x", "workflow": "w", "cron": "0 0 3 * * *", "input": {}}),
        )
        .await;
    // (The REST adapter already refuses a '/' in a schedule name.)
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = a
        .post(
            "/v1/schedules",
            json!({"name": "x", "workflow": "b/w", "cron": "0 0 3 * * *", "input": {}}),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "nested workflow name: {v}");
    assert!(!v.to_string().contains("\"b/w\" is"), "{v}");
    let (st, v) = a
        .put(
            "/v1/workflows/always_fail/failure-policy",
            json!({"on_failure": {"workflow": "b/on_fail"}}),
        )
        .await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::BAD_REQUEST, "invalid_name"),
        "nested hook name: {v}"
    );
    let (st, v) = a
        .post("/v1/workflows/w/runs", json!({"input": {}, "tenant": "b"}))
        .await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::FORBIDDEN, "forbidden_tenant"),
        "{v}"
    );
    assert!(!v.to_string().contains("b/"), "{v}");
    let (st, v) = a.get("/v1/runs?tenant=b").await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::FORBIDDEN, "forbidden_tenant"),
        "{v}"
    );
    for path in ["/v1/workers", "/v1/partitions", "/v1/cluster/status"] {
        let (st, v) = a.get(path).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{path}: {v}");
    }

    // B7: b uploads new bytes naming tenant a: refused, nothing stored.
    let (st, v, tsha) = b.upload("timeouts", Some("a")).await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::FORBIDDEN, "forbidden_tenant"),
        "{v}"
    );
    let (module, versions) = s.h().meta().view().read(|st| {
        (
            st.module(&tsha).is_some(),
            st.workflows()
                .filter(|w| w.value.module_sha256 == tsha)
                .count(),
        )
    });
    assert!(
        !module && versions == 0,
        "a refused upload leaves no records"
    );
    // ... and naming a stored sha for tenant a: no new versions or uploaders.
    let (st, v, _) = b.upload("p11-hook", Some("a")).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    let after = s
        .h()
        .meta()
        .view()
        .read(|st| st.module(&sha).map(|m| get_module_uploaders(&m.value.ext)))
        .expect("module")
        .expect("decode")
        .expect("ext");
    assert_eq!(after.tenants, ["a", "b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_run_ids_are_404_and_leave_the_run_untouched() {
    let Fx { s, a, b, .. } = fixture().await;
    a.deploy("signals").await;
    a.deploy("tokens").await;
    a.deploy("p11-hook").await;
    let run = a
        .start("signals", json!({"input": {"mode": "long_wait"}}))
        .await;
    // The long wait evicts: snapshot once the run is parked (Scheduled).
    s.wait_record(&run, LIMIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
    sleep(Duration::from_millis(300)).await;
    let status_before = s.run_record(&run).status;
    let history_before = s.history_kinds(&run).await;

    // The body of a genuinely missing run, for comparison.
    let ghost = RunId::new(run.partition(), uuid::Uuid::now_v7());
    let (st, missing) = b.get(&format!("/v1/runs/{ghost}")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let want = missing
        .to_string()
        .replace(&ghost.to_string(), &run.to_string());

    let r = run.to_string();
    let probes: Vec<(reqwest::Method, String, Option<Value>)> = vec![
        (reqwest::Method::GET, format!("/v1/runs/{r}"), None),
        (reqwest::Method::GET, format!("/v1/runs/{r}/history"), None),
        (reqwest::Method::GET, format!("/v1/runs/{r}/result"), None),
        (reqwest::Method::GET, format!("/v1/runs/{r}/logs"), None),
        (
            reqwest::Method::GET,
            format!("/v1/runs/{r}/rerun-cuts"),
            None,
        ),
        (
            reqwest::Method::POST,
            format!("/v1/runs/{r}/signals/x"),
            Some(json!({"payload": 1})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/runs/{r}/cancel"),
            Some(json!({})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/runs/{r}/terminate"),
            Some(json!({})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/runs/{r}/retry"),
            Some(json!({})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/runs/{r}/pause"),
            Some(json!({})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/runs/{r}/resume"),
            Some(json!({})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/runs/{r}/rerun"),
            Some(json!({"from": {"seq": 1}})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/dlq/{r}/requeue"),
            Some(json!({})),
        ),
        (
            reqwest::Method::POST,
            format!("/v1/dlq/{r}/dismiss"),
            Some(json!({"reason": "x"})),
        ),
    ];
    for (m, path, body) in probes {
        let (st, v) = b.call(m.clone(), &path, body).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "{m} {path}: {v}");
        assert_eq!(v.to_string(), want, "{m} {path}: the body of a missing run");
        assert!(!v.to_string().contains("a/"), "{path}: {v}");
    }
    assert_eq!(s.run_record(&run).status, status_before);
    assert_eq!(
        s.history_kinds(&run).await,
        history_before,
        "a's run untouched"
    );
    // The run still waits for its signal: a delivers it and the run completes.
    let (st, v) = a
        .post(&format!("/v1/runs/{run}/signals/x"), json!({"payload": 7}))
        .await;
    assert!(st.is_success(), "{v}");
    assert_eq!(terminal(&s, &run).await.status, RunStatus::Completed);

    // Await token of a's run completed with b's token: 404, then a completes it.
    let trun = a.start("tokens", json!({"input": {}})).await;
    let token = a.published_token(&trun).await;
    let (st, v) = b
        .post("/v1/tokens/complete", json!({"token": token, "payload": 1}))
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    assert!(!v.to_string().contains("a/"), "{v}");
    let (st, v) = a
        .post("/v1/tokens/complete", json!({"token": token, "payload": 1}))
        .await;
    assert!(st.is_success(), "{v}");
    assert_eq!(terminal(&s, &trun).await.status, RunStatus::Completed);

    // DLQ: a's failure is listed for a (unqualified), never for b.
    let failed = a.start("w", json!({"input": {"fail": true}})).await;
    assert_eq!(terminal(&s, &failed).await.status, RunStatus::Failed);
    let deadline = Instant::now() + LIMIT;
    loop {
        let (_, v) = a.get("/v1/dlq").await;
        if let Some(e) = v["entries"].as_array().and_then(|es| {
            es.iter()
                .find(|e| e["run_id"] == failed.to_string())
                .cloned()
        }) {
            assert_eq!(e["workflow"], "w", "{e}");
            break;
        }
        assert!(Instant::now() < deadline, "a's DLQ entry: {v}");
        sleep(Duration::from_millis(50)).await;
    }
    let (_, v) = b.get("/v1/dlq").await;
    assert!(!v.to_string().contains(&failed.to_string()), "{v}");
    for path in [
        format!("/v1/dlq/{failed}/requeue"),
        format!("/v1/dlq/{failed}/dismiss"),
    ] {
        let (st, v) = b.post(&path, json!({"reason": "x"})).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "{path}: {v}");
    }

    // Bulk jobs: b cannot see, get or cancel a's job.
    let (st, v) = a
        .post(
            "/v1/bulk",
            json!({"action": "cancel", "filter": {"tags": ["nothing-here"]}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let job = v["job_id"].as_str().expect("job").to_owned();
    let (st, v) = b.get(&format!("/v1/bulk/{job}")).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let (st, v) = b.post(&format!("/v1/bulk/{job}/cancel"), json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let (_, v) = b.get("/v1/bulk").await;
    assert!(!v.to_string().contains(&job), "{v}");
    let (st, v) = a.get(&format!("/v1/bulk/{job}")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (_, v) = a.get("/v1/bulk").await;
    assert!(v.to_string().contains(&job), "{v}");

    // Nested queue names come back unqualified.
    let (st, v) = a.post("/v1/queues/q1/pause", json!({})).await;
    assert!(st.is_success(), "{v}");
    assert_eq!(v["name"], "q1", "{v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_start_site_keeps_the_tenant() {
    let Fx { s, a, b, .. } = fixture().await;
    for g in ["p11-hook", "can-chain", "child-parent", "child-leaf"] {
        a.deploy(g).await;
    }
    b.deploy("p11-hook").await;
    let in_a = |rec: &ostro_partition::store::values::RunRecord, wf: &str| {
        assert_eq!(rec.tenant.as_deref(), Some("a"), "{wf}: {rec:?}");
        assert_eq!(rec.workflow, format!("a/{wf}"));
    };

    // Retry / restart.
    let failed = a.start("w", json!({"input": {"fail": true}})).await;
    assert_eq!(terminal(&s, &failed).await.status, RunStatus::Failed);
    let (st, v) = a.post(&format!("/v1/runs/{failed}/retry"), json!({})).await;
    assert!(st.is_success(), "{v}");
    let retried: RunId = v["run_id"].as_str().expect("run_id").parse().expect("id");
    in_a(&s.run_record(&retried), "w");

    // Rerun.
    let (st, v) = a
        .post(
            &format!("/v1/runs/{failed}/rerun"),
            json!({"from": {"seq": 1}}),
        )
        .await;
    assert!(st.is_success(), "{v}");
    let rerun: RunId = v["new_run_id"]
        .as_str()
        .expect("new_run_id")
        .parse()
        .expect("id");
    in_a(&s.run_record(&rerun), "w");

    // Continue-as-new: every run of the chain is a's.
    let chain = a
        .start("can-chain", json!({"input": {"n": 0, "max": 2}}))
        .await;
    let first = terminal(&s, &chain).await;
    in_a(&first, "can-chain");
    let deadline = Instant::now() + LIMIT;
    let ids = loop {
        let ids = a.run_ids("workflow=can-chain").await;
        if ids.len() == 3 {
            break ids;
        }
        assert!(Instant::now() < deadline, "chain runs: {ids:?}");
        sleep(Duration::from_millis(50)).await;
    };
    for id in ids {
        in_a(&s.run_record(&id.parse().expect("id")), "can-chain");
    }

    // Child start.
    let parent = a
        .start(
            "parent_attached",
            json!({"input": {"workflow": "leaf_echo", "input": 1}}),
        )
        .await;
    assert_eq!(terminal(&s, &parent).await.status, RunStatus::Completed);
    let children = a.run_ids("workflow=leaf_echo").await;
    assert_eq!(children.len(), 1, "{children:?}");
    in_a(
        &s.run_record(&children[0].parse().expect("id")),
        "leaf_echo",
    );

    // On-failure hook.
    let (st, v) = a
        .put(
            "/v1/workflows/always_fail/failure-policy",
            json!({"on_failure": {"workflow": "on_fail"}}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["on_failure"]["workflow"], "on_fail", "stripped: {v}");
    let doomed = a.start("always_fail", json!({"input": {}})).await;
    assert_eq!(terminal(&s, &doomed).await.status, RunStatus::Failed);
    let deadline = Instant::now() + LIMIT;
    let hook: RunId = loop {
        let (_, v) = a.get("/v1/dlq").await;
        let hook = v["entries"].as_array().and_then(|es| {
            es.iter()
                .find(|e| e["run_id"] == doomed.to_string())
                .and_then(|e| e["hook_run_id"].as_str().map(str::to_owned))
        });
        if let Some(h) = hook {
            break h.parse().expect("hook id");
        }
        assert!(Instant::now() < deadline, "hook run: {v}");
        sleep(Duration::from_millis(50)).await;
    };
    in_a(&terminal(&s, &hook).await, "on_fail");

    // Bulk rerun: the selection holds only a's runs (b's tagged failure is not touched).
    let mut mine = Vec::new();
    for _ in 0..2 {
        let r = a
            .start("w", json!({"input": {"fail": true}, "tags": ["br"]}))
            .await;
        assert_eq!(terminal(&s, &r).await.status, RunStatus::Failed);
        mine.push(r);
    }
    let theirs = b
        .start("w", json!({"input": {"fail": true}, "tags": ["br"]}))
        .await;
    assert_eq!(terminal(&s, &theirs).await.status, RunStatus::Failed);
    let b_runs_before = b.run_ids("").await.len();
    let (st, v) = a
        .post(
            "/v1/bulk",
            json!({"action": {"rerun": {"from": {"seq": 1}}}, "filter": {"tags": ["br"]}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["matched_estimate"], 2, "only a's runs match: {v}");
    let job = v["job_id"].as_str().expect("job").to_owned();
    let deadline = Instant::now() + LIMIT;
    loop {
        let (_, v) = a.get(&format!("/v1/bulk/{job}")).await;
        if v["state"] == "done" {
            assert_eq!(v["done"], 2, "{v}");
            break;
        }
        assert!(Instant::now() < deadline, "bulk job: {v}");
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        b.run_ids("").await.len(),
        b_runs_before,
        "b's runs untouched"
    );
    let a_runs = a.run_ids("workflow=w").await;
    for id in a_runs {
        in_a(&s.run_record(&id.parse().expect("id")), "w");
    }
}

/// p14-fixups-2: `ModulePolicy.tenant` over REST and gRPC. A system admin sets any tenant's
/// requirement (`true` tightens, `false` clears) without touching the cluster policy; a
/// tenant admin tightens only its own tenant (the field is forced to it), never relaxes,
/// never names another tenant and never sets the cluster policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_module_policy_over_rest_and_grpc() {
    let mut fx = fixture().await;
    let path = "/v1/security/modules/policy";

    // System admin tightens a: a's unsigned upload is refused, b's is not.
    let (st, v) = fx
        .admin
        .put(path, json!({"require_signed_modules": true, "tenant": "a"}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["tenant"], "a");
    assert_eq!(v["require_signed_modules"], true);
    let (st, v, _) = fx.a.upload("smoke", None).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    assert_eq!(reason(&v), "signature_required");
    let (st, v, _) = fx.b.upload("smoke", None).await;
    assert!(st.is_success(), "b is not tightened: {st} {v}");
    // The cluster policy is unchanged.
    let (st, v) = fx.admin.get(path).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["require_signed_modules"], false, "{v}");
    assert!(v.get("tenant").is_none(), "{v}");

    // System admin clears a's tightening.
    let (st, v) = fx
        .admin
        .put(
            path,
            json!({"require_signed_modules": false, "tenant": "a"}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["require_signed_modules"], false);
    let (st, v, _) = fx.a.upload("smoke", None).await;
    assert!(st.is_success(), "a relaxed: {st} {v}");

    // Tenant admin b tightens its own tenant (no tenant field: forced to b).
    let (st, v) =
        fx.b.put(path, json!({"require_signed_modules": true}))
            .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["tenant"], "b");
    assert_eq!(v["require_signed_modules"], true);
    let (st, v, _) = fx.b.upload("signals", None).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    let (st, v) =
        fx.b.put(path, json!({"require_signed_modules": true, "tenant": "b"}))
            .await;
    assert_eq!(st, StatusCode::OK, "own tenant named: {v}");
    // ... but never relaxes, never names another tenant, never sends keys.
    let (st, v) =
        fx.b.put(path, json!({"require_signed_modules": false}))
            .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "relax: {v}");
    let (st, v) =
        fx.b.put(path, json!({"require_signed_modules": true, "tenant": "a"}))
            .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "other tenant: {v}");
    assert_eq!(reason(&v), "forbidden_tenant");
    let (st, v) =
        fx.b.put(
            path,
            json!({"require_signed_modules": true, "trusted_keys": [
                {"alg": "minisign", "key_id": "auto", "public_key": "x"}
            ]}),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "keys: {v}");
    // A system read token cannot set anything.
    let (st, v) = fx
        .sys
        .put(path, json!({"require_signed_modules": true, "tenant": "a"}))
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "read token: {v}");
    assert_eq!(reason(&v), "missing_scope");
    // a stays relaxed, the cluster stays off.
    let (st, v, _) = fx.a.upload("signals", None).await;
    assert!(st.is_success(), "{st} {v}");

    // gRPC: the system admin clears b's tightening; the answer carries the tenant.
    use ostro_proto::api::v1 as pb;
    let mut c = pb::security_client::SecurityClient::new(fx.s.channel().await);
    let mut req = tonic::Request::new(pb::SetModulePolicyRequest {
        policy: Some(pb::ModulePolicy {
            require_signed_modules: false,
            trusted_keys: vec![],
            tenant: Some("b".into()),
        }),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", fx.admin.bearer)
            .parse()
            .expect("header"),
    );
    let p = c
        .set_module_policy(req)
        .await
        .expect("grpc set")
        .into_inner();
    assert_eq!(p.tenant.as_deref(), Some("b"));
    assert!(!p.require_signed_modules);
    let (st, v, _) = fx.b.upload("signals", None).await;
    assert!(st.is_success(), "b relaxed over gRPC: {st} {v}");
    drop(c);
    fx.s.shutdown().await;
}
