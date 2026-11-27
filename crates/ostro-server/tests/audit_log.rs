//! 14-18 Task 1 + 2: audit capture (SEC-04, D-P14-11/12/13).
//!
//! - **Sweep** (`every_audited_method_records_exactly_once`): every guarded method is called
//!   once through the guard over the ostro-api fakes (all Phase 7/8/11/13/14 slots, the
//!   14-02 Phase 14 fakes) with the real 14-04 journal behind `P14Ctx.audit`. Each audited
//!   method leaves exactly one record (its `classify` action, the actor, the target, `ok` or
//!   `failed:<code>`); reads, starts, signals and token completions leave none. The
//!   complete D-P14-13 table against the real 14-17/14-19 implementations is 14-26's.
//! - **Server** (`server_actions_*`, `[auth] mode = "token"`): a real deploy, cancel, failed
//!   cancel and secret write are each recorded once on the node with actor, target and
//!   outcome; reads, starts and signals are not; authentication and authorization failures
//!   are coalesced summaries with a count; the journal verifies (`verify_dir` and the
//!   `ostro audit verify` entry point `audit::verify::offline`) and a tampered line is
//!   reported by its seq.
//! - **Failpoints** (`--features failpoints`, `--test-threads=1`): with `audit::writer_stall`
//!   an audited cancel answers 503 `audit_unavailable` and the run keeps running while a
//!   non-audited read still answers; with `audit::before_fsync` a cancel that executed
//!   answers 500 `audit_commit_failed` (`details.executed = true`) and later audited calls
//!   are refused before acting.
//! - **Secret access** (`secret_access_*`, Task 2): 20 resolutions of one secret for one
//!   (run, module, origin) are one coalesced record with `count = 20`; another origin is a
//!   separate record; the canary value never appears in the journal.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ostro_api::auth::{self, Method, Principal, ScopeSet};
use ostro_api::domain::{
    AwaitResultRequest, CancelRequest, CompleteTokenRequest, ConnectionInfo, EgressInfo, GrantInfo,
    HistoryRequest, ListRunsRequest, PresignMethod, PresignedObjectRequest, ReplayRequest,
    ReplaySelect, RetryRequest, RunPolicy, SignalRequest, StartRunRequest, TerminateRequest,
};
use ostro_api::fake::{
    FakeCluster, FakeControlPlane, FakeInventory, FakeModuleLifecycle, FakeOps, FakeP14, FakePhase8,
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
    AttachSignaturesRequest, AuditQuery, CreateTokenRequest, ModulePolicy, RotateTokenRequest,
    TenantPatch, TenantSpec, TrustedKeySpec,
};
use ostro_api::{ApiError, ApiExtensions, ControlPlane};
use ostro_meta::{ApiToken, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_partition::Clock;
use ostro_partition::clock::ManualClock;
use ostro_server::audit::segment::list_segments;
use ostro_server::audit::verify::{VerifyArgs, offline, verify_dir};
use ostro_server::config::{AuthMode, ServerConfig};
use ostro_server::p14::audit_hooks::classify;
use ostro_server::p14::{self, P14Init, tokens};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use common::{BridgeTestOpts, TestServer, bridge_server_config, guest_wasm, tmpfs_dir};

const LIMIT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------------
// Journal helpers
// ---------------------------------------------------------------------------------

/// Every record of the journal in `dir`, oldest first (the newest segment may end in a
/// line being written: it is skipped).
fn journal(dir: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    for (_, path) in list_segments(dir).expect("list segments") {
        let text = std::fs::read_to_string(&path).expect("read segment");
        for line in text.lines() {
            if let Ok(v) = serde_json::from_str::<Value>(line) {
                out.push(v);
            }
        }
    }
    out
}

/// The non-control records of `dir`.
fn actions(dir: &Path) -> Vec<Value> {
    journal(dir)
        .into_iter()
        .filter(|r| {
            !matches!(
                r["action"].as_str(),
                Some("segment_open" | "audit_recovered" | "retention_trim")
            )
        })
        .collect()
}

fn with_action<'a>(recs: &'a [Value], action: &str) -> Vec<&'a Value> {
    recs.iter().filter(|r| r["action"] == action).collect()
}

fn unit<T>(r: Result<T, ApiError>) -> Result<(), ApiError> {
    r.map(|_| ())
}

// ---------------------------------------------------------------------------------
// Part 1: every guarded method once, through the guard, over the fakes
// ---------------------------------------------------------------------------------

/// A system admin token (nothing is refused by authz or the tenant hook).
fn auditor() -> Principal {
    Principal::token("otk_auditor", None, ScopeSet::admin())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_audited_method_records_exactly_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = Arc::new(MetaHandle::start(dir.path(), 1).await.expect("meta"));
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_000));
    let mut cfg = ServerConfig::server_defaults();
    cfg.auth.mode = AuthMode::Token;
    let ctx = p14::build_ctx(P14Init {
        cfg: Arc::new(cfg),
        meta,
        clock,
        node_id: 1,
        data_dir: dir.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("build_ctx");
    let stop = CancellationToken::new();
    let writer = ostro_server::audit::writer::start(&ctx, stop.clone()).expect("audit on");
    let audit_dir = dir.path().join("audit");
    let fake = Arc::new(FakeControlPlane::new());
    let mut ext: ApiExtensions = FakePhase8::new(Arc::clone(&fake)).extensions();
    ext.ops = Some(FakeOps::new());
    ext.inventory = Some(FakeInventory::new());
    ext.cluster = Some(FakeCluster::new());
    ext.module_lifecycle = Some(FakeModuleLifecycle::new());
    FakeP14::new().install(&mut ext);
    let g = p14::wrap_ingress(&ctx, fake as Arc<dyn ControlPlane>, ext);
    let before = actions(&audit_dir).len();
    assert_eq!(before, 0, "a fresh journal holds only control records");

    let results: Vec<(Method, Result<(), ApiError>)> = auth::scope(auditor(), async {
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
        ]
    })
    .await;

    // Every guarded method was called once; the audited ones left exactly one record each.
    assert_eq!(
        results.len(),
        Method::all().len() - 1,
        "every method but `ready`"
    );
    let recs = actions(&audit_dir);
    let mut by_action: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
    for r in &recs {
        by_action
            .entry(r["action"].as_str().expect("action").to_owned())
            .or_default()
            .push(r);
    }
    let mut audited = 0;
    for (m, res) in &results {
        let Some(action) = classify(*m) else {
            continue;
        };
        audited += 1;
        let got = by_action.get(action).map(Vec::as_slice).unwrap_or_default();
        assert_eq!(got.len(), 1, "{m:?} ({action}): {got:?}");
        let r = got[0];
        assert_eq!(r["actor"], "otk_auditor", "{m:?}");
        assert_eq!(r["actor_kind"], "token", "{m:?}");
        assert_eq!(r["details"]["method"], m.name(), "{m:?}");
        assert_eq!(r["request_id"], Value::Null, "{m:?}");
        match res {
            Ok(()) => assert_eq!(r["outcome"], json!({"result": "ok"}), "{m:?}"),
            Err(e) => {
                let want = if e.code == ostro_api::ErrorCode::PermissionDenied {
                    "denied"
                } else {
                    "failed"
                };
                assert_eq!(r["outcome"]["result"], want, "{m:?}: {e:?}");
                assert_eq!(r["outcome"]["reason"], e.code.as_str(), "{m:?}");
            }
        }
    }
    // Nothing else was recorded (reads, starts, signals, completions, `join_worker`).
    assert_eq!(
        recs.len(),
        audited,
        "one record per audited call: {recs:#?}"
    );
    assert!(audited >= 60, "{audited} audited methods swept");
    // Targets: run id, sha, names, scopes; never a request body.
    let target = |a: &str| {
        by_action[a][0]["target"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    };
    assert_eq!(target("run.cancel"), "r-1");
    assert_eq!(target("run.terminate"), "r-1");
    assert_eq!(target("module.upload"), "ab".repeat(32));
    assert_eq!(target("workflow.activate"), "w@1");
    assert_eq!(target("webhook.create"), "h");
    assert_eq!(target("cluster.drain"), "node:2");
    assert_eq!(target("failure_policy.set"), "queue:q");
    verify_dir(&audit_dir).expect("the journal verifies");
    stop.cancel();
    writer.await.expect("writer task");
}

// ---------------------------------------------------------------------------------
// Part 2: a token-mode server
// ---------------------------------------------------------------------------------

/// A REST client with one bearer.
#[derive(Clone)]
struct Api {
    http: reqwest::Client,
    base: String,
    bearer: Option<String>,
}

impl Api {
    async fn call(
        &self,
        m: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut rb = self.http.request(m, format!("{}{path}", self.base));
        if let Some(b) = &self.bearer {
            rb = rb.bearer_auth(b);
        }
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

    /// `PUT /v1/modules/{sha}`; the sha.
    async fn deploy(&self, guest: &str) -> String {
        let wasm = guest_wasm(guest);
        let sha: [u8; 32] = Sha256::digest(&wasm).into();
        let hex: String = sha.iter().map(|x| format!("{x:02x}")).collect();
        let mut rb = self
            .http
            .put(format!("{}/v1/modules/{hex}", self.base))
            .header("content-type", "application/wasm")
            .body(wasm);
        if let Some(b) = &self.bearer {
            rb = rb.bearer_auth(b);
        }
        let r = rb.send().await.expect("PUT module");
        assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
        hex
    }

    /// Start `workflow` expecting a new run; its id.
    async fn start(&self, workflow: &str, input: Value) -> String {
        let (st, v) = self
            .post(
                &format!("/v1/workflows/{workflow}/runs"),
                json!({ "input": input }),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "start {workflow}: {v}");
        v["run_id"].as_str().expect("run_id").to_owned()
    }

    async fn status(&self, run: &str) -> String {
        let (st, v) = self.get(&format!("/v1/runs/{run}")).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        v["status"].as_str().unwrap_or_default().to_owned()
    }

    /// Wait until the run exists and is not terminal (a waiting run may show `scheduled`).
    async fn wait_live(&self, run: &str) {
        let deadline = Instant::now() + LIMIT;
        loop {
            let (st, v) = self.get(&format!("/v1/runs/{run}")).await;
            if st == StatusCode::OK && live(v["status"].as_str().unwrap_or_default()) {
                return;
            }
            assert!(Instant::now() < deadline, "run {run}: {st} {v}");
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_status(&self, run: &str, want: &str) {
        let deadline = Instant::now() + LIMIT;
        loop {
            if self.status(run).await == want {
                return;
            }
            assert!(Instant::now() < deadline, "run {run} never reached {want}");
            sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Whether a run status is not terminal.
fn live(status: &str) -> bool {
    !matches!(
        status,
        "" | "completed" | "failed" | "cancelled" | "terminated" | "timed_out" | "continued_as_new"
    )
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

/// A fresh token stored in `meta`: `(id, bearer)`.
async fn put_token(meta: &MetaHandle, scopes: &[&str]) -> (String, String) {
    let (id, secret, bearer) = tokens::generate_os().expect("generate");
    let rec = ApiToken {
        id: id.as_str().to_owned(),
        hash: tokens::hash(&secret),
        scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
        tenant: None,
        created_at_ms: 1,
        expires_at_ms: None,
        ext: Vec::new(),
    };
    propose(meta, MetaKey::Token(rec.id.clone()), MetaRecord::Token(rec)).await;
    (id.as_str().to_owned(), bearer.expose().to_owned())
}

struct Fx {
    s: TestServer,
    admin: Api,
    admin_id: String,
    read: Api,
    read_id: String,
    anon: Api,
    audit_dir: PathBuf,
}

async fn fixture(tweak: impl FnOnce(&mut ServerConfig)) -> Fx {
    let dir = tmpfs_dir();
    let mut cfg = bridge_server_config(dir.path(), &BridgeTestOpts::default());
    cfg.auth.mode = AuthMode::Token;
    cfg.engine.hot_wait = Duration::from_millis(50).into();
    tweak(&mut cfg);
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(LIMIT).await;
    let meta = s.h().meta();
    let base = s.url("").trim_end_matches('/').to_owned();
    let api = |bearer: Option<String>| Api {
        http: reqwest::Client::new(),
        base: base.clone(),
        bearer,
    };
    let (admin_id, admin) = put_token(meta, &["admin"]).await;
    let (read_id, read) = put_token(meta, &["read"]).await;
    let audit_dir = s.h().data_dir().join("audit");
    Fx {
        admin: api(Some(admin)),
        admin_id,
        read: api(Some(read)),
        read_id,
        anon: api(None),
        audit_dir,
        s,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_actions_are_recorded_once_with_actor_target_and_outcome() {
    let Fx {
        mut s,
        admin,
        admin_id,
        read,
        read_id,
        anon,
        audit_dir,
    } = fixture(|_| {}).await;
    let base = actions(&audit_dir).len();

    // Deploy: one module.upload.
    let sha = admin.deploy("signals").await;
    // A start and a signal are not audited; a cancel is.
    let run = admin.start("signals", json!({"mode": "long_wait"})).await;
    admin.wait_live(&run).await;
    let (st, v) = admin
        .post(
            &format!("/v1/runs/{run}/signal"),
            json!({"name": "y", "payload": 1}),
        )
        .await;
    assert!(st.is_success(), "signal: {st} {v}");
    let (st, v) = admin
        .post(
            &format!("/v1/runs/{run}/cancel"),
            json!({"reason": "audit test"}),
        )
        .await;
    assert!(st.is_success(), "cancel: {st} {v}");
    admin.wait_status(&run, "cancelled").await;
    // A failed audited action: cancel of an unknown run.
    let ghost = "0190d0e6-0000-7000-8000-000000000000";
    let (st, _) = admin
        .post(&format!("/v1/runs/{ghost}/cancel"), json!({}))
        .await;
    assert!(!st.is_success());
    // A secret write: the name is recorded, never the value.
    let canary = "CANARY-audit-log-6f1d-secret-value";
    let (st, v) = admin
        .put("/v1/secrets/audit-canary", json!({ "value": canary }))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    // Reads produce nothing.
    for p in [
        "/v1/runs?limit=10",
        "/v1/workflows",
        "/v1/secrets",
        "/v1/cluster/status",
    ] {
        let (st, v) = admin.get(p).await;
        assert!(st.is_success(), "{p}: {st} {v}");
    }
    let (st, _) = admin.get(&format!("/v1/runs/{run}")).await;
    assert!(st.is_success());

    let recs: Vec<Value> = actions(&audit_dir).into_iter().skip(base).collect();
    let names: Vec<&str> = recs.iter().filter_map(|r| r["action"].as_str()).collect();
    assert_eq!(
        names,
        ["module.upload", "run.cancel", "run.cancel", "secret.set"],
        "{recs:#?}"
    );
    for r in &recs {
        assert_eq!(r["actor"], admin_id.as_str());
        assert_eq!(r["actor_kind"], "token");
        assert_eq!(r["tenant"], Value::Null);
        assert_eq!(r["node"], recs[0]["node"], "recorded on the ingress node");
    }
    assert_eq!(recs[0]["target"], sha.as_str());
    assert_eq!(recs[0]["outcome"], json!({"result": "ok"}));
    assert_eq!(recs[1]["target"], run.as_str());
    assert_eq!(recs[1]["outcome"], json!({"result": "ok"}));
    assert_eq!(recs[2]["target"], ghost);
    assert_eq!(recs[2]["outcome"]["result"], "failed");
    assert_eq!(recs[3]["target"], "audit-canary");

    // Authentication failures (bad, unknown and missing bearers) and an authorization
    // failure (a read token writing a secret): coalesced, recorded at the flush.
    let bad = Api {
        bearer: Some("not-a-token".into()),
        ..anon.clone()
    };
    for _ in 0..3 {
        let (st, _) = bad.get("/v1/runs").await;
        assert_eq!(st, StatusCode::UNAUTHORIZED);
    }
    let (st, _) = anon.get("/v1/runs").await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    for _ in 0..2 {
        let (st, v) = read
            .put("/v1/secrets/x", json!({ "value": "12345678" }))
            .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
        assert_eq!(reason(&v), "missing_scope");
    }
    s.shutdown().await;
    let all = actions(&audit_dir);
    let auth: Vec<&Value> = with_action(&all, "auth.failure");
    let malformed: Vec<&&Value> = auth.iter().filter(|r| r["actor"] == "malformed").collect();
    assert_eq!(malformed.len(), 1, "{auth:#?}");
    assert_eq!(malformed[0]["count"], 3);
    assert_eq!(malformed[0]["actor_kind"], "anonymous");
    assert_eq!(malformed[0]["outcome"]["result"], "denied");
    assert_eq!(
        auth.iter().filter(|r| r["actor"] == "missing").count(),
        1,
        "{auth:#?}"
    );
    let denied = with_action(&all, "authz.denied");
    assert_eq!(denied.len(), 1, "{denied:#?}");
    assert_eq!(denied[0]["actor"], read_id.as_str());
    assert_eq!(denied[0]["count"], 2);
    assert_eq!(denied[0]["outcome"]["reason"], "missing_scope");

    // No secret material anywhere in the journal.
    for (_, path) in list_segments(&audit_dir).expect("segments") {
        let text = std::fs::read_to_string(path).expect("read");
        assert!(!text.contains(canary), "secret value in the journal");
        assert!(!text.contains("12345678"), "secret value in the journal");
    }

    // The chain verifies, through `verify_dir` and the `ostro audit verify` entry point.
    verify_dir(&audit_dir).expect("verify");
    let data_dir = audit_dir.parent().expect("data dir").to_path_buf();
    offline(VerifyArgs {
        data_dir: data_dir.clone(),
        config: None,
    })
    .expect("offline verify");
    // Tamper with the secret.set record: verification names its seq.
    let seq = recs[3]["seq"].as_u64().expect("seq");
    let (_, last) = list_segments(&audit_dir)
        .expect("segments")
        .into_iter()
        .rfind(|(first, _)| *first <= seq)
        .expect("segment of the record");
    let text = std::fs::read_to_string(&last).expect("read");
    let tampered = text.replacen(
        "\"target\":\"audit-canary\"",
        "\"target\":\"audit-canarY\"",
        1,
    );
    assert_ne!(text, tampered, "the record is in that segment");
    std::fs::write(&last, tampered).expect("write");
    let e = verify_dir(&audit_dir).expect_err("tampered");
    assert!(format!("{e}").contains(&seq.to_string()), "{e}");
    assert!(
        offline(VerifyArgs {
            data_dir,
            config: None,
        })
        .is_err()
    );
}

// ---------------------------------------------------------------------------------
// Part 3: failpoints (fail closed, W8)
// ---------------------------------------------------------------------------------

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writer_stall_refuses_audited_calls_before_acting() {
        let _fs = fail::FailScenario::setup();
        let Fx {
            mut s,
            admin,
            audit_dir,
            ..
        } = fixture(|c| c.audit.reserve_timeout = Duration::from_millis(300).into()).await;
        admin.deploy("signals").await;
        let run = admin.start("signals", json!({"mode": "long_wait"})).await;
        admin.wait_live(&run).await;
        let before = actions(&audit_dir).len();
        fail::cfg("audit::writer_stall", "return").expect("cfg");
        sleep(Duration::from_millis(100)).await;
        let (st, v) = admin
            .post(&format!("/v1/runs/{run}/cancel"), json!({}))
            .await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        assert_eq!(reason(&v), "audit_unavailable");
        assert!(
            v["error"]["details"]["retry_after_ms"].as_u64().is_some(),
            "{v}"
        );
        // Not executed; a non-audited read still answers.
        assert!(
            live(&admin.status(&run).await),
            "the refused cancel did not execute"
        );
        let (st, _) = admin.get("/v1/runs?limit=5").await;
        assert_eq!(st, StatusCode::OK);
        fail::remove("audit::writer_stall");
        sleep(Duration::from_millis(100)).await;
        assert_eq!(actions(&audit_dir).len(), before, "nothing was recorded");
        let (st, v) = admin
            .post(&format!("/v1/runs/{run}/cancel"), json!({}))
            .await;
        assert!(st.is_success(), "{st} {v}");
        admin.wait_status(&run, "cancelled").await;
        assert_eq!(with_action(&actions(&audit_dir), "run.cancel").len(), 1);
        s.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commit_failure_after_execution_is_500_and_fails_closed() {
        let _fs = fail::FailScenario::setup();
        let Fx { mut s, admin, .. } = fixture(|c| {
            c.audit.reserve_timeout = Duration::from_millis(300).into();
        })
        .await;
        admin.deploy("signals").await;
        let a = admin.start("signals", json!({"mode": "long_wait"})).await;
        let b = admin.start("signals", json!({"mode": "long_wait"})).await;
        admin.wait_live(&a).await;
        admin.wait_live(&b).await;
        fail::cfg("audit::before_fsync", "return").expect("cfg");
        let (st, v) = admin.post(&format!("/v1/runs/{a}/cancel"), json!({})).await;
        fail::remove("audit::before_fsync");
        assert_eq!(st, StatusCode::INTERNAL_SERVER_ERROR, "{v}");
        assert_eq!(reason(&v), "audit_commit_failed");
        assert_eq!(v["error"]["details"]["executed"], true);
        // It did execute.
        admin.wait_status(&a, "cancelled").await;
        // The writer is poisoned: the next audited call is refused before acting.
        let (st, v) = admin.post(&format!("/v1/runs/{b}/cancel"), json!({})).await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{v}");
        assert_eq!(reason(&v), "audit_unavailable");
        assert!(
            live(&admin.status(&b).await),
            "the refused cancel did not execute"
        );
        s.shutdown().await;
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
