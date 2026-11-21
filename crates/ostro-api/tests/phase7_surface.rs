//! Phase 7 API surface (07-11) against `FakeControlPlane`: flow CAS and validation,
//! queue/workflow/run pause, demand, worker admin, join tokens and join, the default
//! (Unavailable) bodies, the REST routes and status codes, `RateLimited` on both
//! transports, and the OpenAPI semantic-superset check (W9).

use std::collections::BTreeMap;
use std::sync::Arc;

use ostro_api::domain::{RunStatus, StartRunRequest, UploadModule};
use ostro_api::fake::{FAKE_CLUSTER_ID, FakeControlPlane, fake_module, sha256_hex};
use ostro_api::phase7::{
    CreateWorkerTokenRequest, ExhaustAction, JoinRequest, P7_PENDING, Placement, QueueFlow,
    RateSpecDto, SpillPolicyDto, WorkerInfo,
};
use ostro_api::{ApiError, ControlPlane, ErrorCode};

const CSR: &str =
    "-----BEGIN CERTIFICATE REQUEST-----\nZmFrZQ==\n-----END CERTIFICATE REQUEST-----\n";

async fn fake_with_workflow() -> FakeControlPlane {
    let cp = FakeControlPlane::new();
    let wasm = fake_module(&[("greet", 1)]);
    cp.upload_module(UploadModule {
        sha256: sha256_hex(&wasm),
        wasm,
        language: None,
        tenant: None,
        name: None,
    })
    .await
    .expect("upload");
    cp
}

fn start(queue: &str) -> StartRunRequest {
    StartRunRequest {
        workflow: "greet".into(),
        input_json: b"{}".to_vec(),
        queue: Some(queue.into()),
        ..Default::default()
    }
}

fn code<T: std::fmt::Debug>(r: Result<T, ApiError>) -> ErrorCode {
    r.expect_err("expected an error").code
}

fn worker(id: &str, free: u32, labels: &[(&str, &str)]) -> WorkerInfo {
    WorkerInfo {
        worker_id: id.into(),
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        slots: 4,
        free_slots: free,
        leases: 4 - free,
        connected_at_ms: 1,
        node_id: 1,
        draining: false,
    }
}

/// A control plane that implements none of the Phase 7 methods.
struct Bare;

#[async_trait::async_trait]
impl ControlPlane for Bare {
    async fn upload_module(
        &self,
        _: UploadModule,
    ) -> Result<ostro_api::domain::UploadModuleResult, ApiError> {
        unreachable!()
    }
    async fn get_module(&self, _: String) -> Result<ostro_api::domain::ModuleInfo, ApiError> {
        unreachable!()
    }
    async fn list_workflows(&self) -> Result<Vec<ostro_api::domain::WorkflowInfo>, ApiError> {
        unreachable!()
    }
    async fn activate_workflow(
        &self,
        _: ostro_api::domain::ActivateWorkflow,
    ) -> Result<ostro_api::domain::WorkflowInfo, ApiError> {
        unreachable!()
    }
    async fn start_run(
        &self,
        _: StartRunRequest,
    ) -> Result<ostro_api::domain::StartRunResult, ApiError> {
        unreachable!()
    }
    async fn get_run(&self, _: String) -> Result<ostro_api::domain::RunInfo, ApiError> {
        unreachable!()
    }
    async fn run_history(
        &self,
        _: ostro_api::domain::HistoryRequest,
    ) -> Result<ostro_api::domain::HistoryPage, ApiError> {
        unreachable!()
    }
    async fn list_runs(
        &self,
        _: ostro_api::domain::ListRunsRequest,
    ) -> Result<ostro_api::domain::RunPage, ApiError> {
        unreachable!()
    }
    fn ready(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn phase7_defaults_answer_unavailable() {
    let cp = Bare;
    let errs = vec![
        cp.get_queue_flow("q".into()).await.map(|_| ()),
        cp.put_queue_flow("q".into(), QueueFlow::default(), None)
            .await
            .map(|_| ()),
        cp.pause_queue("q".into()).await.map(|_| ()),
        cp.resume_queue("q".into()).await.map(|_| ()),
        cp.pause_workflow("w".into()).await.map(|_| ()),
        cp.resume_workflow("w".into()).await.map(|_| ()),
        cp.pause_run("r".into(), None).await.map(|_| ()),
        cp.resume_run("r".into()).await.map(|_| ()),
        cp.queue_demand("q".into()).await.map(|_| ()),
        cp.list_workers().await.map(|_| ()),
        cp.drain_worker("w".into()).await.map(|_| ()),
        cp.create_worker_token(CreateWorkerTokenRequest::default())
            .await
            .map(|_| ()),
        cp.list_worker_tokens().await.map(|_| ()),
        cp.revoke_worker_token("t".into()).await,
        cp.join_worker(JoinRequest {
            token: "t".into(),
            csr_pem: CSR.into(),
            worker_id: "w".into(),
            labels: BTreeMap::new(),
        })
        .await
        .map(|_| ()),
    ];
    assert_eq!(errs.len(), 15);
    for e in errs {
        let e = e.expect_err("default body");
        assert_eq!(e.code, ErrorCode::Unavailable);
        assert_eq!(e.message, P7_PENDING);
    }
}

#[tokio::test]
async fn phase7_flow_cas_and_validation() {
    let cp = FakeControlPlane::new();
    let info = cp.get_queue_flow("q".into()).await.expect("get");
    assert_eq!((info.version, info.flow), (0, QueueFlow::default()));

    let flow = QueueFlow {
        placement: Some(Placement::Workers),
        selector: vec!["gpu=true".into(), "zone in (eu-1,eu-2)".into()],
        spill: Some(SpillPolicyDto {
            cpu_pct: 70,
            slots_pct: 90,
            mem_pct: 85,
            lag_ms: 5_000,
            sustain_ms: 10_000,
            cooldown_ms: 30_000,
            hysteresis_pct: 85,
        }),
        concurrency: Some(8),
        weight: Some(3),
        key_concurrency: Some(1),
        throttle: Some(RateSpecDto {
            limit: 10,
            period_ms: 1_000,
            burst: 5,
        }),
        on_retries_exhausted: Some(ExhaustAction::Pause),
        ..Default::default()
    };
    let v1 = cp
        .put_queue_flow("q".into(), flow.clone(), Some(0))
        .await
        .expect("put");
    assert_eq!((v1.version, &v1.flow), (1, &flow));
    assert_eq!(cp.get_queue_flow("q".into()).await.expect("get"), v1);
    // Stale version.
    assert_eq!(
        code(cp.put_queue_flow("q".into(), flow.clone(), Some(0)).await),
        ErrorCode::PreconditionFailed
    );
    // Unconditional put.
    let v2 = cp
        .put_queue_flow("q".into(), QueueFlow::default(), None)
        .await
        .expect("put");
    assert_eq!(v2.version, 2);

    // Invalid flows: placement/exhaust names are serde-checked (see the lib tests);
    // period 0, bad selector, too many entries, spill > 100.
    for bad in [
        QueueFlow {
            rate_limit: Some(RateSpecDto {
                limit: 1,
                period_ms: 0,
                burst: 0,
            }),
            ..Default::default()
        },
        QueueFlow {
            selector: vec!["zone in eu-1".into()],
            ..Default::default()
        },
        QueueFlow {
            selector: vec!["a".into(); 33],
            ..Default::default()
        },
        QueueFlow {
            spill: Some(SpillPolicyDto {
                mem_pct: 101,
                ..Default::default()
            }),
            ..Default::default()
        },
    ] {
        assert_eq!(
            code(cp.put_queue_flow("q".into(), bad, None).await),
            ErrorCode::InvalidArgument
        );
    }
    assert_eq!(
        code(cp.get_queue_flow(String::new()).await),
        ErrorCode::InvalidArgument
    );
    assert_eq!(cp.get_queue_flow("q".into()).await.expect("get").version, 2);
}

#[tokio::test]
async fn phase7_pause_resume_queue_workflow_run() {
    let cp = fake_with_workflow().await;
    let p = cp.pause_queue("q".into()).await.expect("pause");
    assert!(p.paused && p.changed);
    assert!(!cp.pause_queue("q".into()).await.expect("again").changed);
    let info = cp.get_queue_flow("q".into()).await.expect("flow");
    assert!(info.flow.paused);
    assert_eq!(info.version, 1);
    let r = cp.resume_queue("q".into()).await.expect("resume");
    assert!(!r.paused && r.changed);
    assert!(!cp.resume_queue("q".into()).await.expect("again").changed);

    let w = cp.pause_workflow("greet".into()).await.expect("pause wf");
    assert!(w.paused && w.changed);
    assert!(
        !cp.pause_workflow("greet".into())
            .await
            .expect("again")
            .changed
    );
    assert!(cp.resume_workflow("greet".into()).await.expect("r").changed);
    assert_eq!(
        code(cp.pause_workflow("nope".into()).await),
        ErrorCode::NotFound
    );

    let run = cp.start_run(start("q")).await.expect("start").run_id;
    let s = cp
        .pause_run(run.clone(), Some("maintenance".into()))
        .await
        .expect("pause run");
    assert!(s.paused && s.changed);
    assert_eq!(cp.run_pause(&run), Some(Some("maintenance".into())));
    assert!(
        !cp.pause_run(run.clone(), None)
            .await
            .expect("again")
            .changed
    );
    let s = cp.resume_run(run.clone()).await.expect("resume run");
    assert!(!s.paused && s.changed);
    assert!(!cp.resume_run(run.clone()).await.expect("again").changed);
    assert_eq!(
        code(cp.pause_run("run-999999".into(), None).await),
        ErrorCode::NotFound
    );
    assert_eq!(
        code(cp.pause_run(run.clone(), Some("x".repeat(1025))).await),
        ErrorCode::InvalidArgument
    );
    cp.set_run_outcome(&run, RunStatus::Completed, None, None, None)
        .expect("outcome");
    let e = cp.pause_run(run.clone(), None).await.expect_err("terminal");
    assert_eq!(
        (e.code, e.reason()),
        (ErrorCode::Conflict, Some("run_terminal"))
    );
    assert_eq!(code(cp.resume_run(run).await), ErrorCode::Conflict);
}

#[tokio::test]
async fn phase7_demand_and_workers() {
    let cp = fake_with_workflow().await;
    for _ in 0..3 {
        cp.start_run(start("gpu")).await.expect("start");
    }
    let d = cp.queue_demand("gpu".into()).await.expect("demand");
    assert_eq!(
        (d.placement, d.backlog, d.demand, d.workers),
        (Placement::Embedded, 3, 0, 0)
    );
    cp.put_queue_flow(
        "gpu".into(),
        QueueFlow {
            placement: Some(Placement::Workers),
            ..Default::default()
        },
        None,
    )
    .await
    .expect("flow");
    cp.add_worker(worker("w-b", 2, &[("gpu", "true")]));
    cp.add_worker(worker("w-a", 1, &[]));
    let d = cp.queue_demand("gpu".into()).await.expect("demand");
    assert_eq!(
        (
            d.placement,
            d.backlog,
            d.demand,
            d.workers,
            d.worker_slots_free
        ),
        (Placement::Workers, 3, 3, 2, 3)
    );
    let ws = cp.list_workers().await.expect("list");
    assert_eq!(
        ws.iter().map(|w| w.worker_id.as_str()).collect::<Vec<_>>(),
        ["w-a", "w-b"]
    );
    let r = cp.drain_worker("w-b".into()).await.expect("drain");
    assert!(!r.already_draining);
    assert!(
        cp.drain_worker("w-b".into())
            .await
            .expect("again")
            .already_draining
    );
    assert_eq!(
        code(cp.drain_worker("w-x".into()).await),
        ErrorCode::NotFound
    );
    let d = cp.queue_demand("gpu".into()).await.expect("demand");
    assert_eq!(
        d.worker_slots_free, 1,
        "a draining worker's slots are not free"
    );
    cp.pause_queue("gpu".into()).await.expect("pause");
    assert_eq!(cp.queue_demand("gpu".into()).await.expect("d").demand, 0);
}

#[tokio::test]
async fn phase7_tokens_and_join() {
    let cp = FakeControlPlane::new();
    let c = cp
        .create_worker_token(CreateWorkerTokenRequest {
            ttl_ms: Some(60_000),
        })
        .await
        .expect("create");
    assert!(c.token_id.starts_with("wkr_"));
    assert!(c.token.starts_with(&format!("{}.", c.token_id)));
    assert!(c.join_command.contains(&c.token));
    assert!(!format!("{c:?}").contains(&c.token));
    assert_eq!(
        code(
            cp.create_worker_token(CreateWorkerTokenRequest { ttl_ms: Some(0) })
                .await
        ),
        ErrorCode::InvalidArgument
    );
    // Lists never carry the secret.
    let list = cp.list_worker_tokens().await.expect("list");
    assert_eq!(list.len(), 1);
    let secret = cp.worker_token_secret(&c.token_id).expect("secret");
    assert!(!serde_json::to_string(&list).unwrap().contains(&secret));

    let join = |token: &str| JoinRequest {
        token: token.into(),
        csr_pem: CSR.into(),
        worker_id: "w-1".into(),
        labels: [("gpu".to_owned(), "true".to_owned())].into(),
    };
    let r = cp.join_worker(join(&c.token)).await.expect("join");
    assert_eq!(r.cluster_id, FAKE_CLUSTER_ID);
    assert!(r.client_cert_pem.contains("BEGIN CERTIFICATE"));
    assert!(!r.endpoints.is_empty());
    for bad in ["wkr_000001.wrong", "nonsense", "wkr_999999.x"] {
        let e = cp.join_worker(join(bad)).await.expect_err(bad);
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::InvalidArgument, Some("join_token_invalid"))
        );
        assert!(!e.message.contains(bad));
    }
    let mut no_csr = join(&c.token);
    no_csr.csr_pem = "x".into();
    assert_eq!(
        code(cp.join_worker(no_csr).await),
        ErrorCode::InvalidArgument
    );
    let mut bad_id = join(&c.token);
    bad_id.worker_id = "w 1".into();
    assert_eq!(
        code(cp.join_worker(bad_id).await),
        ErrorCode::InvalidArgument
    );
    let mut bad_label = join(&c.token);
    bad_label.labels.insert("bad key".into(), "v".into());
    assert_eq!(
        code(cp.join_worker(bad_label).await),
        ErrorCode::InvalidArgument
    );

    // Expiry on the fake clock: 60 s = 60 ticks of mutating calls.
    for i in 0..61 {
        cp.pause_queue(format!("tick-{i}")).await.expect("tick");
    }
    assert_eq!(
        code(cp.join_worker(join(&c.token)).await),
        ErrorCode::InvalidArgument
    );

    cp.revoke_worker_token(c.token_id.clone())
        .await
        .expect("revoke");
    assert_eq!(
        code(cp.revoke_worker_token(c.token_id.clone()).await),
        ErrorCode::NotFound
    );
    assert!(cp.list_worker_tokens().await.expect("list").is_empty());
    let c2 = cp
        .create_worker_token(CreateWorkerTokenRequest::default())
        .await
        .expect("create");
    cp.join_worker(join(&c2.token)).await.expect("join");
    cp.revoke_worker_token(c2.token_id).await.expect("revoke");
    assert_eq!(
        code(cp.join_worker(join(&c2.token)).await),
        ErrorCode::InvalidArgument
    );
}

#[tokio::test]
async fn phase7_fake_rate_limit() {
    let cp = Arc::new(fake_with_workflow().await);
    cp.put_queue_flow(
        "rl".into(),
        QueueFlow {
            rate_limit: Some(RateSpecDto {
                limit: 1,
                period_ms: 3_600_000,
                burst: 1,
            }),
            ..Default::default()
        },
        None,
    )
    .await
    .expect("flow");
    cp.start_run(start("rl")).await.expect("1");
    cp.start_run(start("rl")).await.expect("2 (burst)");
    let e = cp.start_run(start("rl")).await.expect_err("limited");
    assert_eq!(e.code, ErrorCode::RateLimited);
    assert!(e.retry_after_ms().is_some_and(|ms| ms > 0));
    // Other queues are not limited.
    cp.start_run(start("other")).await.expect("other");
}
