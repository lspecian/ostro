//! The production gRPC client is lossless (13-03, D-P13-14): one transcript over every
//! trait method that has a gRPC twin — the 04-11 core (modules, workflows, runs, tokens,
//! policies), the Phase 6 bridge, Phase 7 queues / workers / pause, the Phase 8 slots, the
//! Phase 11 run operations and inventory, and the Phase 13 cluster / module lifecycle —
//! runs twice against identically seeded fakes:
//!
//! 1. directly against the fakes, and
//! 2. as `GrpcControlPlane -> in-process tonic server (ostro_api::router_with) -> fakes`.
//!
//! Each step records `format!("{:?}", result)`, so values and errors (code, message and
//! JSON details) must match exactly. The REST-only methods (D-P10-18, presign, hooks,
//! backup) are excluded from the transcript and pinned to `unimplemented` separately.
//! A second test proves request metadata and the consistency token cross the client.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_api::domain::*;
use ostro_api::fake::{
    FakeCluster, FakeControlPlane, FakeInventory, FakeMigrateOutcome, FakeModuleLifecycle, FakeOps,
    FakePhase8, fake_module, sha256_hex,
};
use ostro_api::grpc_client::GrpcControlPlane;
use ostro_api::phase7::{
    CreateWorkerTokenRequest, JoinRequest as WorkerJoin, QueueFlow, RateSpecDto, WorkerInfo,
};
use ostro_api::phase8::{
    AdminApi, CatchupPolicyDto, HookIngress, LogLevelDto, OverlapPolicyDto, RunLogLine, RunLogsApi,
    RunLogsRequest, ScheduleApi, ScheduleDto, VerifierDto, WebhookActionDto, WebhookApi,
    WebhookDto,
};
use ostro_api::phase11::*;
use ostro_api::phase13::*;
use ostro_api::request_meta::{self, RequestMeta, TrustedNode};
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane, ErrorCode};
use serde_json::json;
use tonic::transport::Channel;

const SHA_OLD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// One side of the comparison: every trait object the transcript calls.
#[derive(Clone)]
struct Target {
    cp: Arc<dyn ControlPlane>,
    schedules: Arc<dyn ScheduleApi>,
    webhooks: Arc<dyn WebhookApi>,
    logs: Arc<dyn RunLogsApi>,
    ops: Arc<dyn OpsApi>,
    inventory: Arc<dyn InventoryApi>,
    cluster: Arc<dyn ClusterApi>,
    lifecycle: Arc<dyn ModuleLifecycleApi>,
}

/// The seeded fakes of one side (side effects between steps go to them).
struct Fakes {
    cp: Arc<FakeControlPlane>,
    p8: Arc<FakePhase8>,
    ops: Arc<FakeOps>,
    cluster: Arc<FakeCluster>,
    ext: ApiExtensions,
}

impl Fakes {
    fn new() -> Fakes {
        let cp = Arc::new(FakeControlPlane::new());
        let p8 = FakePhase8::new(cp.clone());
        let mut ext = p8.extensions();
        let ops = FakeOps::new();
        ops.install(&mut ext);
        ops.set_cuts(RerunCutsView {
            run_id: "run-old".into(),
            cuts: vec![
                RerunCut {
                    seq: 3,
                    step: Some("charge".into()),
                    occurrence: 1,
                    safe: true,
                    reason: None,
                    nearest_safe_seq: None,
                },
                RerunCut {
                    seq: 5,
                    step: None,
                    occurrence: 1,
                    safe: false,
                    reason: Some("effect in flight".into()),
                    nearest_safe_seq: Some(3),
                },
            ],
        });
        for (id, resumable) in [("run-f1", true), ("run-f2", false)] {
            ops.push_dlq(DlqEntry {
                run_id: id.into(),
                workflow: "greet".into(),
                version: 1,
                queue: "default".into(),
                status: ostro_api::rest::RunStatusDto::Failed,
                reason: "boom".into(),
                failed_at_ms: 1_700_000_000_000,
                tags: vec!["t1".into()],
                resumable,
                hook_run_id: None,
                dismissed_at_ms: None,
                dismiss_reason: None,
                requeued_to: None,
                requeued_at_ms: None,
            });
        }
        ops.set_bulk_matches(vec!["run-a".into(), "run-b".into()]);
        let inv = FakeInventory::new();
        inv.install(&mut ext);
        inv.set_modules(vec![ModuleSummary {
            sha256: SHA_OLD.into(),
            size_bytes: 42,
            uploaded_at_ms: Some(7),
            workflows: vec!["greet@1".into()],
        }]);
        inv.set_queues(vec![QueueView {
            name: "default".into(),
            paused: false,
            ready: 2,
            running: 1,
            demand: Some(3),
            flow: Some(json!({"concurrency": 4})),
        }]);
        inv.set_partitions(vec![PartitionView {
            id: 3,
            role: "leader".into(),
            leader: Some(1),
            epoch: Some(2),
            applied_offset: Some(10),
            log_end_offset: Some(11),
            ready: Some(1),
            running: None,
            healthy: true,
            detail: None,
        }]);
        let cluster = FakeCluster::new();
        cluster.install(&mut ext);
        let lifecycle = FakeModuleLifecycle::new();
        lifecycle.install(&mut ext);
        lifecycle.add_module(SHA_OLD, &[("greet", 0)], false);
        lifecycle.add_run("run-m1", "greet", FakeMigrateOutcome::Pass);
        lifecycle.add_run("run-m2", "greet", FakeMigrateOutcome::SeqMismatch);
        Fakes {
            cp,
            p8,
            ops,
            cluster,
            ext,
        }
    }

    fn direct(&self) -> Target {
        Target {
            cp: self.cp.clone(),
            schedules: self.ext.schedules.clone().expect("schedules"),
            webhooks: self.ext.webhooks.clone().expect("webhooks"),
            logs: self.ext.run_logs.clone().expect("run logs"),
            ops: self.ext.ops.clone().expect("ops"),
            inventory: self.ext.inventory.clone().expect("inventory"),
            cluster: self.ext.cluster.clone().expect("cluster"),
            lifecycle: self.ext.module_lifecycle.clone().expect("lifecycle"),
        }
    }
}

struct Server {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(cp: Arc<dyn ControlPlane>, ext: ApiExtensions, trusted: Option<u64>) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let mut app = ostro_api::router_with(cp, ApiConfig::default(), ext);
    if let Some(node) = trusted {
        app = app.layer(axum::middleware::from_fn(
            move |mut req: axum::extract::Request, next: axum::middleware::Next| {
                req.extensions_mut().insert(TrustedNode(node));
                next.run(req)
            },
        ));
    }
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    Server { addr, task }
}

async fn client(addr: SocketAddr) -> GrpcControlPlane {
    let ch = Channel::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("h2c connect");
    GrpcControlPlane::new(ch)
}

fn remote(c: &GrpcControlPlane) -> Target {
    let c = Arc::new(c.clone());
    Target {
        cp: c.clone(),
        schedules: c.clone(),
        webhooks: c.clone(),
        logs: c.clone(),
        ops: c.clone(),
        inventory: c.clone(),
        cluster: c.clone(),
        lifecycle: c,
    }
}

struct Transcript(Vec<(String, String)>);

impl Transcript {
    fn rec<T: std::fmt::Debug>(&mut self, label: &str, r: &Result<T, ApiError>) {
        self.0.push((label.to_owned(), format!("{r:?}")));
    }
}

fn ok_or_panic<T>(label: &str, r: Result<T, ApiError>) -> T {
    r.unwrap_or_else(|e| panic!("{label}: {e:?}"))
}

/// The scenario (side effects on `f` happen identically on both sides).
async fn scenario(t: &Target, f: &Fakes) -> Transcript {
    let mut out = Transcript(Vec::new());
    let o = &mut out;

    // -- 04-11 core: modules, workflows, runs, tokens, policies --------------------
    let wasm = fake_module(&[("greet", 1), ("other", 1)]);
    let sha = sha256_hex(&wasm);
    let up = |sha: &str, wasm: &bytes::Bytes| UploadModule {
        sha256: sha.to_owned(),
        wasm: wasm.clone(),
        language: Some("rust".into()),
        tenant: None,
        name: Some("greeter".into()),
    };
    o.rec("upload", &t.cp.upload_module(up(&sha, &wasm)).await);
    o.rec("upload again", &t.cp.upload_module(up(&sha, &wasm)).await);
    o.rec(
        "upload bad sha",
        &t.cp.upload_module(up(SHA_OLD, &wasm)).await,
    );
    o.rec("get module", &t.cp.get_module(sha.clone()).await);
    o.rec("get module 404", &t.cp.get_module(SHA_OLD.into()).await);
    o.rec("list workflows", &t.cp.list_workflows().await);
    let act = |version, expected_version| ActivateWorkflow {
        name: "greet".into(),
        version,
        expected_version,
    };
    o.rec("activate", &t.cp.activate_workflow(act(1, None)).await);
    o.rec("activate 404", &t.cp.activate_workflow(act(9, None)).await);
    o.rec(
        "activate stale",
        &t.cp.activate_workflow(act(1, Some(999))).await,
    );
    let start = |key: &str, policy, dry_run| StartRunRequest {
        workflow: "greet".into(),
        input_json: br#"{"a":1}"#.to_vec(),
        tags: vec!["t1".into(), "t2".into()],
        priority: Some(3),
        idempotency_key: Some(key.into()),
        conflict_policy: policy,
        retention_ms: Some(60_000),
        dry_run,
        ..StartRunRequest::default()
    };
    let r1 =
        t.cp.start_run(start("k1", ConflictPolicy::ReturnExisting, false))
            .await;
    o.rec("start", &r1);
    let r1 = ok_or_panic("start", r1).run_id;
    o.rec(
        "start existing",
        &t.cp
            .start_run(start("k1", ConflictPolicy::ReturnExisting, false))
            .await,
    );
    o.rec(
        "start reject",
        &t.cp
            .start_run(start("k1", ConflictPolicy::Reject, false))
            .await,
    );
    o.rec(
        "start dry",
        &t.cp
            .start_run(start("k9", ConflictPolicy::ReturnExisting, true))
            .await,
    );
    o.rec(
        "start unknown",
        &t.cp
            .start_run(StartRunRequest {
                workflow: "nope".into(),
                input_json: b"null".to_vec(),
                ..StartRunRequest::default()
            })
            .await,
    );
    o.rec("get run", &t.cp.get_run(r1.clone()).await);
    o.rec("get run 404", &t.cp.get_run("run-nope".into()).await);
    o.rec(
        "history",
        &t.cp
            .run_history(HistoryRequest {
                run_id: r1.clone(),
                from_seq: 0,
                limit: 50,
            })
            .await,
    );
    o.rec(
        "list runs",
        &t.cp
            .list_runs(ListRunsRequest {
                workflow: Some("greet".into()),
                limit: 10,
                tag: Some("t1".into()),
                tags: vec!["t1".into()],
                order: ListOrder::Newest,
                ..ListRunsRequest::default()
            })
            .await,
    );
    let sig = |rid: Option<&str>| SignalRequest {
        run_id: r1.clone(),
        name: "go".into(),
        payload: json!({"n": 1}),
        request_id: rid.map(str::to_owned),
        dry_run: false,
    };
    o.rec("signal", &t.cp.signal_run(sig(Some("q1"))).await);
    o.rec("signal dup", &t.cp.signal_run(sig(Some("q1"))).await);
    o.rec(
        "pause run",
        &t.cp.pause_run(r1.clone(), Some("ops".into())).await,
    );
    o.rec("get paused", &t.cp.get_run(r1.clone()).await);
    o.rec("resume run", &t.cp.resume_run(r1.clone()).await);
    o.rec(
        "await running",
        &t.cp
            .await_result(AwaitResultRequest {
                run_id: r1.clone(),
                wait_ms: 0,
                follow_continuations: false,
            })
            .await,
    );
    f.cp.set_run_outcome(
        &r1,
        RunStatus::Completed,
        Some(br#"{"ok":true}"#.to_vec()),
        None,
        Some(br#"{"stage":"done"}"#.to_vec()),
    )
    .expect("outcome");
    o.rec(
        "await done",
        &t.cp
            .await_result(AwaitResultRequest {
                run_id: r1.clone(),
                wait_ms: 10,
                follow_continuations: true,
            })
            .await,
    );
    o.rec("get done", &t.cp.get_run(r1.clone()).await);
    o.rec(
        "retry completed",
        &t.cp
            .retry_run(RetryRequest {
                run_id: r1.clone(),
                dry_run: false,
            })
            .await,
    );
    let r2 = ok_or_panic(
        "start r2",
        t.cp.start_run(start("k2", ConflictPolicy::ReturnExisting, false))
            .await,
    )
    .run_id;
    let term = |dry_run| TerminateRequest {
        run_id: r2.clone(),
        reason: Some("ops".into()),
        dry_run,
    };
    o.rec("terminate dry", &t.cp.terminate_run(term(true)).await);
    o.rec(
        "cancel",
        &t.cp
            .cancel_run(CancelRequest {
                run_id: r2.clone(),
                reason: None,
                dry_run: false,
            })
            .await,
    );
    o.rec("terminate", &t.cp.terminate_run(term(false)).await);
    o.rec("terminate again", &t.cp.terminate_run(term(false)).await);
    f.cp.set_resumable(&r2, true);
    o.rec(
        "retry",
        &t.cp
            .retry_run(RetryRequest {
                run_id: r2.clone(),
                dry_run: false,
            })
            .await,
    );
    let token = f.cp.mint_token(&r2, 1);
    let tok = |token: &str| CompleteTokenRequest {
        token: token.to_owned(),
        payload: json!({"approved": true}),
        dry_run: false,
    };
    o.rec("complete token", &t.cp.complete_token(tok(&token)).await);
    o.rec(
        "complete token again",
        &t.cp.complete_token(tok(&token)).await,
    );
    o.rec(
        "complete bad token",
        &t.cp.complete_token(tok("ot1_nope")).await,
    );
    let pol = RunPolicy {
        retention_ms: Some(1000),
        schedule_to_start_ms: None,
    };
    o.rec(
        "set wf policy",
        &t.cp.set_workflow_policy("greet".into(), pol).await,
    );
    o.rec(
        "get wf policy",
        &t.cp.get_workflow_policy("greet".into()).await,
    );
    o.rec(
        "set queue policy",
        &t.cp.set_queue_policy("default".into(), pol).await,
    );
    o.rec(
        "get queue policy",
        &t.cp.get_queue_policy("default".into()).await,
    );

    // -- Phase 6 bridge -------------------------------------------------------------
    o.rec(
        "put secret",
        &t.cp
            .put_secret(PutSecret {
                name: "api_key".into(),
                value: b"supersecretvalue".to_vec(),
                tenant: None,
            })
            .await,
    );
    o.rec(
        "put short secret",
        &t.cp
            .put_secret(PutSecret {
                name: "short".into(),
                value: b"x".to_vec(),
                tenant: None,
            })
            .await,
    );
    o.rec("list secrets", &t.cp.list_secrets(None).await);
    let grant = GrantInfo {
        id: String::new(),
        module: sha.clone(),
        secret: "api_key".into(),
        origins: vec!["https://api.example.com".into()],
        body: false,
    };
    let g = t.cp.put_grant(grant).await;
    o.rec("put grant", &g);
    o.rec("list grants", &t.cp.list_grants(Some(sha.clone())).await);
    if let Ok(g) = g {
        o.rec("delete grant", &t.cp.delete_grant(g.id).await);
    }
    o.rec(
        "delete secret",
        &t.cp.delete_secret("api_key".into(), None).await,
    );
    o.rec(
        "delete secret 404",
        &t.cp.delete_secret("api_key".into(), None).await,
    );
    let conn = ConnectionInfo {
        name: "pg".into(),
        kind: "sql-postgres".into(),
        config: json!({"v": 1, "url": "postgres://db/app"}),
    };
    o.rec("put connection", &t.cp.put_connection(conn).await);
    o.rec(
        "put bad connection",
        &t.cp
            .put_connection(ConnectionInfo {
                name: "x".into(),
                kind: "nope".into(),
                config: json!({}),
            })
            .await,
    );
    o.rec("get connection", &t.cp.get_connection("pg".into()).await);
    o.rec("list connections", &t.cp.list_connections().await);
    o.rec(
        "delete connection",
        &t.cp.delete_connection("pg".into()).await,
    );
    let egress = EgressInfo {
        module: sha.clone(),
        allow: vec!["https://api.example.com".into(), "connection:pg".into()],
    };
    o.rec("put egress", &t.cp.put_egress(egress).await);
    o.rec("get egress", &t.cp.get_egress(sha.clone()).await);
    o.rec("list egress", &t.cp.list_egress().await);
    o.rec("delete egress", &t.cp.delete_egress(sha.clone()).await);

    // -- Phase 7 queues, workers, pause ----------------------------------------------
    o.rec("get flow", &t.cp.get_queue_flow("default".into()).await);
    let flow = QueueFlow {
        concurrency: Some(4),
        selector: vec!["zone=a".into()],
        rate_limit: Some(RateSpecDto {
            limit: 10,
            period_ms: 1000,
            burst: 2,
        }),
        ..QueueFlow::default()
    };
    o.rec(
        "put flow",
        &t.cp
            .put_queue_flow("default".into(), flow.clone(), None)
            .await,
    );
    o.rec(
        "put flow stale",
        &t.cp.put_queue_flow("default".into(), flow, Some(99)).await,
    );
    o.rec("pause queue", &t.cp.pause_queue("default".into()).await);
    o.rec("resume queue", &t.cp.resume_queue("default".into()).await);
    o.rec("pause workflow", &t.cp.pause_workflow("greet".into()).await);
    o.rec(
        "resume workflow",
        &t.cp.resume_workflow("greet".into()).await,
    );
    o.rec("demand", &t.cp.queue_demand("default".into()).await);
    f.cp.add_worker(WorkerInfo {
        worker_id: "w1".into(),
        labels: [("zone".to_owned(), "a".to_owned())].into_iter().collect(),
        slots: 4,
        free_slots: 3,
        leases: 1,
        connected_at_ms: 5,
        node_id: 1,
        draining: false,
    });
    o.rec("list workers", &t.cp.list_workers().await);
    o.rec("drain worker", &t.cp.drain_worker("w1".into()).await);
    o.rec("drain worker 404", &t.cp.drain_worker("w9".into()).await);
    let created =
        t.cp.create_worker_token(CreateWorkerTokenRequest {
            ttl_ms: Some(60_000),
        })
        .await;
    o.rec("create worker token", &created);
    o.rec("list worker tokens", &t.cp.list_worker_tokens().await);
    o.rec(
        "join worker bad token",
        &t.cp
            .join_worker(WorkerJoin {
                token: "wkr_nope.x".into(),
                csr_pem: "csr".into(),
                worker_id: "w2".into(),
                labels: Default::default(),
            })
            .await,
    );
    if let Ok(c) = created {
        o.rec(
            "revoke worker token",
            &t.cp.revoke_worker_token(c.token_id).await,
        );
    }

    // -- Phase 8 slots ----------------------------------------------------------------
    let sched = ScheduleDto {
        name: "nightly".into(),
        workflow: "greet".into(),
        version: None,
        cron: "0 3 * * *".into(),
        timezone: "UTC".into(),
        input: json!({"x": 1}),
        paused: false,
        overlap: OverlapPolicyDto::Skip,
        catchup: CatchupPolicyDto::One,
        catchup_window_ms: Some(60_000),
        start_at_ms: None,
        end_at_ms: None,
        version_tag: None,
        next_fire_at_ms: None,
        last_fire_at_ms: None,
    };
    let created = t.schedules.create_schedule(sched.clone()).await;
    o.rec("create schedule", &created);
    o.rec(
        "create schedule dup",
        &t.schedules.create_schedule(sched).await,
    );
    o.rec("list schedules", &t.schedules.list_schedules().await);
    o.rec(
        "get schedule",
        &t.schedules.get_schedule("nightly".into()).await,
    );
    o.rec(
        "get schedule 404",
        &t.schedules.get_schedule("nope".into()).await,
    );
    if let Ok(mut s) = created {
        s.cron = "0 4 * * *".into();
        s.next_fire_at_ms = None;
        s.last_fire_at_ms = None;
        o.rec("update schedule", &t.schedules.update_schedule(s).await);
    }
    o.rec(
        "pause schedule",
        &t.schedules.pause_schedule("nightly".into()).await,
    );
    o.rec(
        "resume schedule",
        &t.schedules.resume_schedule("nightly".into()).await,
    );
    o.rec(
        "trigger schedule",
        &t.schedules
            .trigger_schedule("nightly".into(), Some("req-1".into()))
            .await,
    );
    o.rec(
        "delete schedule stale",
        &t.schedules
            .delete_schedule("nightly".into(), Some(999))
            .await,
    );
    o.rec(
        "delete schedule",
        &t.schedules.delete_schedule("nightly".into(), None).await,
    );
    let hook = WebhookDto {
        name: "stripe".into(),
        action: WebhookActionDto::Start {
            workflow: "greet".into(),
            version: None,
            input: "body".into(),
        },
        verifier: VerifierDto::default(),
        idempotency: Some("json:/id".into()),
        version_tag: None,
    };
    let created = t.webhooks.create_webhook(hook).await;
    o.rec("create webhook", &created);
    o.rec("list webhooks", &t.webhooks.list_webhooks().await);
    o.rec(
        "get webhook",
        &t.webhooks.get_webhook("stripe".into()).await,
    );
    if let Ok(mut w) = created {
        w.idempotency = None;
        o.rec("update webhook", &t.webhooks.update_webhook(w).await);
    }
    o.rec(
        "delete webhook",
        &t.webhooks.delete_webhook("stripe".into(), None).await,
    );
    o.rec(
        "delete webhook 404",
        &t.webhooks.delete_webhook("stripe".into(), None).await,
    );
    for (i, level) in [LogLevelDto::Info, LogLevelDto::Warn]
        .into_iter()
        .enumerate()
    {
        f.p8.push_log(
            &r1,
            RunLogLine {
                ts_ms: 10 + i as u64,
                level,
                message: format!("line {i}"),
                fields: json!({"i": i}),
                attempt: 1,
            },
        );
    }
    let logs = |level| RunLogsRequest {
        run_id: r1.clone(),
        limit: 10,
        level,
    };
    o.rec("run logs", &t.logs.run_logs(logs(None)).await);
    o.rec(
        "run logs warn",
        &t.logs.run_logs(logs(Some(LogLevelDto::Warn))).await,
    );

    // -- Phase 11 run operations and inventory ---------------------------------------
    let rerun = |key: Option<&str>, dry_run| RerunRequest {
        run_id: "run-old".into(),
        from: RerunFrom::Step {
            name: "charge".into(),
            occurrence: 1,
        },
        version: RerunVersion::Source,
        idempotency_key: key.map(str::to_owned),
        dry_run,
    };
    o.rec("rerun dry", &t.ops.rerun_run(rerun(None, true)).await);
    o.rec("rerun", &t.ops.rerun_run(rerun(Some("rk"), false)).await);
    o.rec(
        "rerun existing",
        &t.ops.rerun_run(rerun(Some("rk"), false)).await,
    );
    o.rec("rerun cuts", &t.ops.analyze_rerun("run-old".into()).await);
    o.rec(
        "rerun cuts 404",
        &t.ops.analyze_rerun("run-nope".into()).await,
    );
    o.rec(
        "list dlq",
        &t.ops
            .list_dlq(DlqListRequest {
                workflow: Some("greet".into()),
                tags: vec!["t1".into()],
                limit: 1,
                ..DlqListRequest::default()
            })
            .await,
    );
    o.rec(
        "requeue dlq",
        &t.ops
            .requeue_dlq(DlqRequeueRequest {
                run_id: "run-f1".into(),
                mode: RequeueMode::Auto,
                dry_run: false,
            })
            .await,
    );
    o.rec(
        "dismiss dlq",
        &t.ops
            .dismiss_dlq(DlqDismissRequest {
                run_id: "run-f2".into(),
                reason: "known".into(),
            })
            .await,
    );
    o.rec(
        "dismiss dlq again",
        &t.ops
            .dismiss_dlq(DlqDismissRequest {
                run_id: "run-f2".into(),
                reason: "known".into(),
            })
            .await,
    );
    let bulk = |dry_run| BulkCreateRequest {
        action: BulkAction::Signal {
            name: "nudge".into(),
            payload: json!({"k": "v"}),
        },
        filter: BulkFilter {
            workflow: Some("greet".into()),
            status: Some(ostro_api::rest::RunStatusDto::Running),
            ..BulkFilter::default()
        },
        rate_per_sec: Some(5),
        max_in_flight: None,
        dry_run,
    };
    o.rec("bulk dry", &t.ops.create_bulk(bulk(true)).await);
    let job = t.ops.create_bulk(bulk(false)).await;
    o.rec("bulk", &job);
    o.rec("list bulk", &t.ops.list_bulk().await);
    if let Ok(BulkCreateResponse {
        job_id: Some(id), ..
    }) = job
    {
        o.rec("get bulk", &t.ops.get_bulk(id.clone()).await);
        o.rec("cancel bulk", &t.ops.cancel_bulk(id).await);
    }
    o.rec("limits", &t.ops.limits().await);
    let fp = FailurePolicy {
        on_failure: Some(OnFailurePolicy {
            workflow: "notify".into(),
            queue: None,
            include_input: true,
        }),
        // A DLQ policy is per queue (adapter check); the workflow scope carries none.
        dlq: None,
        version_tag: None,
    };
    let scope = || PolicyScope::Workflow("greet".into());
    o.rec(
        "set failure policy",
        &t.ops.set_failure_policy(scope(), fp.clone()).await,
    );
    o.rec(
        "get failure policy",
        &t.ops.get_failure_policy(scope()).await,
    );
    o.rec(
        "set queue failure policy",
        &t.ops
            .set_failure_policy(
                PolicyScope::Queue("default".into()),
                FailurePolicy {
                    on_failure: None,
                    dlq: Some(DlqPolicyDto {
                        enabled: true,
                        hook_catchup_ms: 1000,
                    }),
                    version_tag: None,
                },
            )
            .await,
    );
    o.rec(
        "set failure policy stale",
        &t.ops
            .set_failure_policy(
                PolicyScope::Workflow("greet".into()),
                FailurePolicy {
                    version_tag: Some(99),
                    ..fp
                },
            )
            .await,
    );
    o.rec(
        "list modules",
        &t.inventory
            .list_modules(ListModulesRequest {
                limit: 10,
                page_token: None,
            })
            .await,
    );
    o.rec("list queues", &t.inventory.list_queues().await);
    o.rec("list partitions", &t.inventory.list_partitions().await);
    // Structured error details survive the client (D-P12-21 not_leader, rate limits).
    f.ops.fail(
        "limits",
        Some(
            ApiError::unavailable("partition 3 is led by node 2").with_details(json!({
                "reason": "not_leader", "partition": 3, "leader": 2,
                "leader_addr": "10.0.0.2:7071", "epoch": 9
            })),
        ),
    );
    o.rec("limits not_leader", &t.ops.limits().await);
    f.ops
        .fail("limits", Some(ApiError::rate_limited("slow down", 1500)));
    o.rec("limits rate_limited", &t.ops.limits().await);
    f.ops.fail("limits", None);

    // -- Phase 13 cluster and module lifecycle -----------------------------------------
    o.rec("cluster status", &t.cluster.status().await);
    o.rec(
        "token before init",
        &t.cluster
            .create_node_token(NodeTokenRequest { ttl_secs: None })
            .await,
    );
    o.rec(
        "init",
        &t.cluster
            .init(InitRequest {
                advertise: Some("10.0.0.1:7071".into()),
                rf: Some(3),
                min_insync: Some(2),
            })
            .await,
    );
    o.rec("init again", &t.cluster.init(InitRequest::default()).await);
    let tok = t
        .cluster
        .create_node_token(NodeTokenRequest { ttl_secs: Some(60) })
        .await;
    o.rec("node token", &tok);
    let join = |token: String| JoinRequest {
        token,
        csr_pem: "csr".into(),
        advertise: Advertise {
            api: "10.0.0.2:7070".into(),
            cluster: "10.0.0.2:7071".into(),
            repl: Some("10.0.0.2:7072".into()),
        },
        node_version: NodeVersionDto {
            ostro_version: "0.1.0".into(),
            level_min: 1,
            level_max: 2,
            wasmtime: "50.0.2".into(),
            event_codec_ver: 1,
            wit_lines: vec!["0.1".into()],
        },
        resume_node_id: None,
    };
    if let Ok(tok) = tok {
        o.rec("join", &t.cluster.join(join(tok.token.clone())).await);
        o.rec("join reused token", &t.cluster.join(join(tok.token)).await);
    }
    f.cluster.promote(2);
    let reassign = |replicas: Vec<u64>, cancel| ReassignRequest {
        partition: 5,
        replicas,
        cancel,
    };
    o.rec(
        "reassign",
        &t.cluster.reassign(reassign(vec![1, 2], false)).await,
    );
    o.rec(
        "reassign busy",
        &t.cluster.reassign(reassign(vec![2], false)).await,
    );
    o.rec(
        "reassign cancel",
        &t.cluster.reassign(reassign(vec![], true)).await,
    );
    o.rec("rebalance", &t.cluster.rebalance().await);
    o.rec("drain", &t.cluster.drain(2).await);
    o.rec("drain 404", &t.cluster.drain(9).await);
    o.rec("remove", &t.cluster.remove(2, true).await);
    o.rec("remove last voter", &t.cluster.remove(1, false).await);
    o.rec("upgrade status", &t.cluster.upgrade_status().await);
    o.rec("upgrade hold", &t.cluster.upgrade_hold().await);
    o.rec("upgrade finalize held", &t.cluster.upgrade_finalize().await);
    o.rec("upgrade release", &t.cluster.upgrade_release().await);
    o.rec("upgrade finalize", &t.cluster.upgrade_finalize().await);
    o.rec("retire", &t.lifecycle.retire_module(SHA_OLD.into()).await);
    o.rec(
        "retire again",
        &t.lifecycle.retire_module(SHA_OLD.into()).await,
    );
    o.rec("gc", &t.lifecycle.run_module_gc().await);
    let migrate = |from, dry_run| MigrateRunsRequest {
        workflow: "greet".into(),
        from,
        to_version: 1,
        filter: Some("t1".into()),
        limit: Some(10),
        dry_run,
    };
    o.rec(
        "migrate dry",
        &t.lifecycle
            .migrate_runs(migrate(VersionOrSha::Version(0), true))
            .await,
    );
    o.rec(
        "migrate",
        &t.lifecycle
            .migrate_runs(migrate(VersionOrSha::Sha(SHA_OLD.into()), false))
            .await,
    );
    out
}

#[tokio::test]
async fn parity_through_grpc_client() {
    // Side 1: the fakes directly.
    let direct = Fakes::new();
    let want = scenario(&direct.direct(), &direct).await;

    // Side 2: GrpcControlPlane -> tonic server -> identical fakes.
    let served = Fakes::new();
    let srv = serve(served.cp.clone(), served.ext.clone(), None).await;
    let c = client(srv.addr).await;
    let got = scenario(&remote(&c), &served).await;

    assert!(want.0.len() >= 130, "transcript has {} steps", want.0.len());
    for ((label, w), (_, g)) in want.0.iter().zip(&got.0) {
        assert_eq!(w, g, "step {label:?} differs through the gRPC client");
    }
    assert_eq!(want.0.len(), got.0.len());
    // The transcript exercised both outcomes, mostly successes.
    let errs: Vec<&str> = want
        .0
        .iter()
        .filter(|(_, s)| s.starts_with("Err("))
        .map(|(l, _)| l.as_str())
        .collect();
    eprintln!("{} steps, {} errors: {errs:?}", want.0.len(), errs.len());
    assert!(
        errs.len() >= 20 && errs.len() * 3 < want.0.len(),
        "{errs:?}"
    );
}

#[tokio::test]
async fn rest_only_methods_answer_unimplemented_through_the_client() {
    let f = Fakes::new();
    let srv = serve(f.cp.clone(), f.ext.clone(), None).await;
    let c = client(srv.addr).await;
    let mut errs = vec![
        c.dev_time().await.map(|_| ()).expect_err("dev_time"),
        c.dev_skip(DevSkip::By { ms: 1 })
            .await
            .map(|_| ())
            .expect_err("dev_skip"),
        AdminApi::backup(&c).await.map(|_| ()).expect_err("backup"),
        HookIngress::deliver(
            &c,
            ostro_api::phase8::HookRequest {
                name: "h".into(),
                headers: Default::default(),
                body: bytes::Bytes::new(),
            },
        )
        .await
        .map(|_| ())
        .expect_err("deliver"),
    ];
    errs.push(
        c.replay_module(ReplayRequest {
            module_sha256: SHA_OLD.into(),
            select: ReplaySelect::Runs(vec!["r".into()]),
            allow_missing: false,
        })
        .await
        .map(|_| ())
        .expect_err("replay"),
    );
    for e in errs {
        assert_eq!(e.code, ErrorCode::Unimplemented, "{e:?}");
        assert!(e.message.contains("REST-only"), "{}", e.message);
    }
    assert!(c.ready());
}

/// Records what the server-side call saw.
#[derive(Default)]
struct Probe {
    seen: Mutex<Vec<(RequestMeta, Instant)>>,
}

#[async_trait::async_trait]
impl ClusterApi for Probe {
    async fn status(&self) -> Result<ClusterStatus, ApiError> {
        self.seen
            .lock()
            .expect("lock")
            .push((RequestMeta::current().expect("scope"), Instant::now()));
        request_meta::set_consistency("m:77".into());
        Ok(ClusterStatus::default())
    }
}

#[tokio::test]
async fn client_propagates_request_meta_deadline_and_consistency() {
    let probe = Arc::new(Probe::default());
    let ext = ApiExtensions {
        cluster: Some(probe.clone() as Arc<dyn ClusterApi>),
        ..ApiExtensions::default()
    };
    let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
    // The server side is a cluster listener: the caller is node 3.
    let srv = serve(cp, ext, Some(3)).await;
    let c = client(srv.addr).await;

    let sent = RequestMeta {
        hop: 1,
        partition_pin: Some(6),
        partitions: Some(vec![6, 7]),
        principal: Some("user:carol".into()),
        read_after: Some("p6:4:120".into()),
        from_node: Some(99),
        deadline: Some(Instant::now() + Duration::from_secs(4)),
    };
    let (r, token) =
        request_meta::scope_with_consistency(sent.clone(), ClusterApi::status(&c)).await;
    r.expect("status");
    assert_eq!(token.as_deref(), Some("m:77"), "consistency copied back");
    let (seen, at) = probe.seen.lock().expect("lock").pop().expect("recorded");
    assert_eq!(seen.hop, 1);
    assert_eq!(seen.partition_pin, Some(6));
    assert_eq!(seen.partitions, Some(vec![6, 7]));
    assert_eq!(seen.principal.as_deref(), Some("user:carol"));
    assert_eq!(seen.read_after.as_deref(), Some("p6:4:120"));
    // `from_node` is the listener's peer identity, never the caller's claim.
    assert_eq!(seen.from_node, Some(3));
    let left = seen.deadline.expect("deadline").duration_since(at);
    assert!(
        left <= Duration::from_secs(4) && left > Duration::from_secs(3),
        "remaining deadline propagated: {left:?}"
    );

    // Outside any scope: no ostro headers, no deadline.
    ClusterApi::status(&c).await.expect("status");
    let (seen, _) = probe.seen.lock().expect("lock").pop().expect("recorded");
    assert_eq!((seen.hop, seen.read_after, seen.deadline), (0, None, None));

    // A spent deadline never reaches the server.
    let spent = RequestMeta {
        deadline: Some(Instant::now()),
        ..RequestMeta::default()
    };
    let e = request_meta::scope(spent, ClusterApi::status(&c))
        .await
        .expect_err("spent");
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert!(probe.seen.lock().expect("lock").is_empty());

    // An unreachable peer is `unavailable` (13-12 retries / answers 503).
    drop(srv);
    let dead = GrpcControlPlane::new(Channel::from_static("http://127.0.0.1:1").connect_lazy());
    let e = ClusterApi::status(&dead).await.expect_err("dead peer");
    assert_eq!(e.code, ErrorCode::Unavailable, "{e:?}");
}
