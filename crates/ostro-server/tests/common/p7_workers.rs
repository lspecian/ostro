//! Phase 7 worker harness (plan 07-09; reused by 07-12/07-13/07-14/07-16): a cluster
//! listener over a real meta group, cluster CA and worker registry ([`Rig`]), an optional
//! partition node whose dispatcher hands its jobs to the test ([`CapturedJob`]) so a real
//! dispatcher lease can be attached to a worker session, a raw protocol client
//! ([`TestWorker`]) for precise protocol tests, an in-process `ostro_worker` joined over a
//! loopback HTTP join endpoint ([`WorkerProc`]) and the per-message payload recorder
//! ([`WireStats`], `Rig::wire`).
//!
//! Include with `#[path = "common/p7_workers.rs"] mod p7_workers;`.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use ostro_api::phase7::{JoinRequest, WorkerTokenCreated};
use ostro_meta::MetaHandle;
use ostro_partition::{
    ApplyOutcome, DispatcherConfig, ExecOutcome, Executor, Job, ManualClock, Node, NodeConfig,
    PartitionHandle,
};
use ostro_server::config::ServerConfig;
use ostro_server::tls::{self, TlsProfile, TlsState};
use ostro_server::workers::join::JoinService;
use ostro_server::workers::registry::{AttachSpec, RemoteJobHandle, WorkerRegistry};
use ostro_server::workers::service::{
    self, ClusterDeps, OSTRO_VERSION, WASMTIME_VERSION, WireStats,
};
use ostro_types::command::{ConflictPolicy, StartRun};
use ostro_types::{Command, PartitionCount, PartitionId, RunId, partition_for_new_run};
use ostro_worker_proto::{PROTO_VERSION, v1};
use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

/// The test cluster id (a valid CA name).
pub const CLUSTER: &str = "0190f5c2-7d3e-7000-8000-0000000000c9";
/// Harness timeout.
pub const T: Duration = Duration::from_secs(20);
/// `Node::open` bound (fresh stores cost ~300 fsyncs; see tests/common `OPEN_T`).
pub const OPEN_T: Duration = Duration::from_secs(120);

pub type Client = v1::execute_client::ExecuteClient<Channel>;

/// Rig knobs.
#[derive(Clone, Debug)]
pub struct RigOpts {
    /// Dispatcher lease TTL (and `Welcome.lease_ttl_ms`).
    pub lease_ttl: Duration,
    /// `[workers] heartbeat`.
    pub heartbeat: Duration,
    pub require_same_version: bool,
    /// `[cluster.tls] client_ca` PEM (written to a file).
    pub client_ca_pem: Option<String>,
    /// Open a 1-partition node with a capturing dispatcher.
    pub node: bool,
    /// `[workers] max_frame_bytes`.
    pub max_frame_bytes: u64,
}

impl Default for RigOpts {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::from_secs(30),
            heartbeat: Duration::from_secs(10),
            require_same_version: true,
            client_ca_pem: None,
            node: false,
            max_frame_bytes: 1024 * 1024,
        }
    }
}

/// A job the capturing dispatcher handed out; `finish` ends the dispatcher's execute.
pub struct CapturedJob {
    pub job: Job,
    done: Option<oneshot::Sender<ExecOutcome>>,
}

impl CapturedJob {
    pub fn finish(&mut self, o: ExecOutcome) {
        if let Some(tx) = self.done.take() {
            let _ = tx.send(o);
        }
    }

    pub fn spec(&self) -> AttachSpec {
        AttachSpec {
            run: self.job.run,
            token: self.job.token,
            lease: self.job.lease.clone(),
            handle: self.job.handle.clone(),
            module_sha: self.job.record.module_sha,
        }
    }

    pub fn wire_run(&self) -> v1::RunRef {
        ostro_server::workers::registry::wire_run(&self.job.run)
    }

    pub fn wire_token(&self) -> v1::Token {
        ostro_server::workers::registry::wire_token(self.job.token)
    }

    pub fn revoked(&self) -> bool {
        *self.job.revoked.borrow()
    }
}

struct Capture {
    tx: mpsc::UnboundedSender<CapturedJob>,
}

impl Executor for Capture {
    fn execute(&self, job: Job) -> Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>> {
        let (done, rx) = oneshot::channel();
        let _ = self.tx.send(CapturedJob {
            job,
            done: Some(done),
        });
        Box::pin(async move { rx.await.unwrap_or(ExecOutcome::Released) })
    }
}

/// A cluster listener + join service + registry over a real meta group and cluster CA.
pub struct Rig {
    pub dir: TempDir,
    pub cfg: Arc<ServerConfig>,
    pub meta: Arc<MetaHandle>,
    pub tls: Arc<TlsState>,
    pub registry: Arc<WorkerRegistry>,
    pub join: Arc<JoinService>,
    pub clock: Arc<ManualClock>,
    pub addr: SocketAddr,
    pub shutdown: CancellationToken,
    pub wire: Arc<WireStats>,
    pub node: Option<Node>,
    jobs: tokio::sync::Mutex<mpsc::UnboundedReceiver<CapturedJob>>,
    task: Option<JoinHandle<()>>,
    join_http: Mutex<Option<String>>,
}

/// Wall-clock ms (only to seed the manual clock near real time; certificates use real
/// validity windows).
pub fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

impl Rig {
    pub async fn start(opts: RigOpts) -> Rig {
        let dir = tempfile::Builder::new()
            .prefix("ostro-p7w-")
            .tempdir()
            .expect("tempdir");
        let mut cfg = ServerConfig::dev_defaults();
        cfg.node.data_dir = dir.path().join("data");
        cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
        cfg.cluster.listen = "127.0.0.1:0".parse().expect("addr");
        cfg.workers.heartbeat = opts.heartbeat.into();
        cfg.workers.require_same_version = opts.require_same_version;
        cfg.workers.max_frame_bytes = opts.max_frame_bytes;
        cfg.executor.lease_ttl = opts.lease_ttl.into();
        if let Some(pem) = &opts.client_ca_pem {
            let p = dir.path().join("external-ca.pem");
            std::fs::write(&p, pem).expect("write client_ca");
            cfg.cluster.tls.client_ca = Some(p);
        }
        std::fs::create_dir_all(&cfg.node.data_dir).expect("data dir");
        let meta = Arc::new(
            timeout(T, MetaHandle::start(&dir.path().join("meta"), 1))
                .await
                .expect("meta start timed out")
                .expect("meta start"),
        );
        let clock = Arc::new(ManualClock::new(wall_ms()));
        meta.init_cluster(CLUSTER, 1, 1, wall_ms(), [7; 32])
            .await
            .expect("init cluster");
        let tls = tls::init_with_profile(&cfg, &cfg.node.data_dir, CLUSTER, TlsProfile::Dev)
            .expect("tls init");
        let registry = Arc::new(WorkerRegistry::new(&cfg.workers));
        let join = Arc::new(JoinService::new(
            meta.view().clone(),
            Arc::clone(&tls),
            &cfg.workers,
        ));
        join.attach(Arc::downgrade(&meta), clock.clone());
        let (jtx, jrx) = mpsc::unbounded_channel();
        let node = if opts.node {
            let mut ncfg =
                NodeConfig::new(dir.path().join("node"), PartitionCount::new(1).expect("p"));
            let mut d = DispatcherConfig::new(Arc::new(Capture { tx: jtx }));
            d.lease_ttl = opts.lease_ttl;
            ncfg.leader.dispatcher = Some(d);
            Some(
                timeout(OPEN_T, Node::open(ncfg))
                    .await
                    .expect("node open timed out")
                    .expect("node open"),
            )
        } else {
            drop(jtx);
            None
        };
        let cfg = Arc::new(cfg);
        let shutdown = CancellationToken::new();
        let wire = Arc::new(WireStats::default());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let (addr, task) = service::serve(
            listener,
            ClusterDeps {
                config: Arc::clone(&cfg),
                tls: Arc::clone(&tls),
                registry: Arc::clone(&registry),
                join: Arc::clone(&join),
                meta: meta.view().clone(),
                clock: clock.clone(),
                node_id: 1,
                lease_ttl: opts.lease_ttl,
                shutdown: shutdown.clone(),
                wire: Some(Arc::clone(&wire)),
            },
        )
        .expect("cluster listener");
        Rig {
            dir,
            cfg,
            meta,
            tls,
            registry,
            join,
            clock,
            addr,
            shutdown,
            wire,
            node,
            jobs: tokio::sync::Mutex::new(jrx),
            task: Some(task),
            join_http: Mutex::new(None),
        }
    }

    /// Stop the listener (sessions get `GoAway`) and the node.
    pub async fn stop(mut self) {
        self.shutdown.cancel();
        if let Some(t) = self.task.take() {
            let _ = timeout(T, t).await;
        }
        if let Some(n) = self.node.take() {
            let _ = timeout(T, n.shutdown()).await;
        }
    }

    /// The injected clock's now (ms).
    pub fn clock_now(&self) -> u64 {
        ostro_partition::Clock::now_ms(&*self.clock)
    }

    pub fn ca_pem(&self) -> String {
        self.tls.ca_pem().expect("ca")
    }

    pub fn ca_hash(&self) -> String {
        self.tls.ca_hash().expect("ca hash")
    }

    /// A join token valid for `ttl`.
    pub async fn token(&self, ttl: Duration) -> WorkerTokenCreated {
        self.join
            .create_token(Some(ttl), "http://127.0.0.1:1")
            .await
            .expect("create token")
    }

    /// Join `worker_id` with `token` directly through the JoinService.
    pub async fn identity(&self, token: &str, worker_id: &str) -> TestIdentity {
        let (key_pem, csr_pem) = new_key_and_csr(worker_id);
        let r = self
            .join
            .join(JoinRequest {
                token: token.to_owned(),
                csr_pem,
                worker_id: worker_id.to_owned(),
                labels: BTreeMap::new(),
            })
            .await
            .expect("join");
        TestIdentity {
            cert_pem: r.client_cert_pem,
            key_pem,
            ca_pem: r.ca_cert_pem,
        }
    }

    /// A fresh token + identity + open session (`Credit{slots}` sent).
    pub async fn worker(&self, worker_id: &str, slots: u32) -> TestWorker {
        let tok = self.token(Duration::from_secs(3600)).await;
        let id = self.identity(&tok.token, worker_id).await;
        let mut w = TestWorker::open(self.addr, &id, hello(worker_id, slots))
            .await
            .expect("session");
        w.token_id = Some(tok.token_id);
        w.send(v1::worker_msg::Msg::Credit(v1::Credit {
            free_slots: slots,
        }));
        self.wait(T, "credit registered", || {
            self.registry
                .sessions()
                .iter()
                .any(|s| s.session_id == w.session_id() && s.free > 0)
        })
        .await;
        w
    }

    /// Poll `f` every 10 ms until true or `limit` (panics).
    pub async fn wait(&self, limit: Duration, what: &str, f: impl Fn() -> bool) {
        wait_for(limit, what, f).await;
    }

    // --------------------------------------------------------------------- node + jobs

    pub fn handle(&self) -> PartitionHandle<ostro_log::PartitionLog> {
        self.node
            .as_ref()
            .expect("RigOpts.node")
            .handle(PartitionId(0))
            .expect("partition")
            .clone()
    }

    /// Store `bytes` as a blob (e.g. a fake module).
    pub fn put_blob(&self, bytes: &[u8]) -> [u8; 32] {
        self.handle().blobs().put(bytes).expect("blob put").sha256
    }

    /// Start a run pinned to `module_sha` with `input` (RunStarted at seq 0; an input over
    /// 256 KiB makes it a blob row).
    pub async fn start_run(&self, module_sha: &[u8; 32], input: &[u8]) -> RunId {
        let uuid = RunId::new_v7(PartitionId(0)).uuid();
        let p = partition_for_new_run(&uuid, PartitionCount::new(1).expect("p"));
        let run = RunId::new(p, uuid);
        let now = wall_ms();
        let mut start = StartRun {
            run_id: run.as_bytes().to_vec(),
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "default".into(),
            input: None,
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            ready_at_ms: now,
            requested_at_ms: now,
            rng_seed: uuid.as_bytes().repeat(2),
            module_sha: module_sha.to_vec(),
            ..Default::default()
        };
        let (kind, bytes) =
            ostro_server::run_started::encode_for_start(&start, input).expect("encode");
        let h = self.handle();
        start.input = Some(
            h.prepare_payload(Bytes::copy_from_slice(input))
                .await
                .expect("prepare input"),
        );
        let payload = h
            .prepare_payload(Bytes::from(bytes))
            .await
            .expect("prepare payload");
        start.run_started = Some(ostro_types::command::JournalEvent {
            seq: 0,
            kind,
            payload: Some(payload),
            ..Default::default()
        });
        let o = timeout(T, h.submit_one(Command::start_run(start)))
            .await
            .expect("start timed out")
            .expect("start");
        assert_eq!(o, ApplyOutcome::Accepted);
        run
    }

    /// The next job the dispatcher granted.
    pub async fn next_job(&self) -> CapturedJob {
        timeout(T, self.jobs.lock().await.recv())
            .await
            .expect("no job granted in time")
            .expect("dispatcher gone")
    }

    /// Attach `job` to `worker_id`'s session (picks its credit).
    pub fn attach(&self, job: &CapturedJob, worker_id: &str) -> RemoteJobHandle {
        let credit = self
            .registry
            .pick(|v| v.worker_id == worker_id)
            .expect("worker credit");
        self.registry
            .attach_job(credit, job.spec())
            .expect("attach")
    }

    // ------------------------------------------------------------- in-process ostro_worker

    /// A loopback HTTP join endpoint (`POST /v1/workers/join` -> JoinService::join), the
    /// 07-07 worker's join contract; started once.
    pub async fn join_url(&self) -> String {
        if let Some(u) = self.join_http.lock().expect("lock").clone() {
            return u;
        }
        use axum::Json;
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        let join = Arc::clone(&self.join);
        let app = axum::Router::new().route(
            "/v1/workers/join",
            axum::routing::post(move |Json(req): Json<JoinRequest>| {
                let join = Arc::clone(&join);
                async move {
                    match join.join(req).await {
                        Ok(r) => (StatusCode::OK, Json(serde_json::to_value(r).expect("json")))
                            .into_response(),
                        Err(e) => (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({ "error": { "message": e.message } })),
                        )
                            .into_response(),
                    }
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind join");
        let url = format!("http://{}", l.local_addr().expect("addr"));
        let sd = self.shutdown.clone();
        tokio::spawn(async move {
            let _ = axum::serve(l, app)
                .with_graceful_shutdown(sd.cancelled_owned())
                .await;
        });
        *self.join_http.lock().expect("lock") = Some(url.clone());
        url
    }

    /// Start an in-process `ostro_worker` joined with `token`.
    pub async fn spawn_worker(
        &self,
        token: &str,
        worker_id: &str,
        slots: u32,
        labels: &[(&str, &str)],
    ) -> WorkerProc {
        let cache = tempfile::tempdir().expect("cache dir");
        let cfg = ostro_worker::WorkerConfig {
            join: Some(self.join_url().await),
            token: Some(ostro_worker::JoinToken::new(token)),
            ca_hash: Some(self.ca_hash()),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            slots,
            cache_dir: cache.path().to_path_buf(),
            worker_id: Some(worker_id.to_owned()),
            reconnect_max_ms: 200,
            rejoin_after_failures: 2,
            ..ostro_worker::WorkerConfig::default()
        };
        let worker = Arc::new(ostro_worker::Worker::new(cfg).expect("worker config"));
        let events = worker.subscribe();
        let shutdown = CancellationToken::new();
        let w = Arc::clone(&worker);
        let sd = shutdown.clone();
        let task = tokio::spawn(async move { w.run(sd).await });
        WorkerProc {
            worker,
            events,
            shutdown,
            task: Some(task),
            _cache: cache,
        }
    }
}

/// An in-process worker.
pub struct WorkerProc {
    pub worker: Arc<ostro_worker::Worker>,
    pub events: tokio::sync::broadcast::Receiver<ostro_worker::WorkerEvent>,
    pub shutdown: CancellationToken,
    pub task: Option<JoinHandle<Result<(), ostro_worker::WorkerError>>>,
    _cache: TempDir,
}

impl WorkerProc {
    /// Kill without draining: abort the task and drop the worker (its channel and
    /// session streams go away with it, as when the process dies).
    pub fn kill(mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }

    /// The next event matching `f` within `limit`.
    pub async fn wait_event(
        &mut self,
        limit: Duration,
        f: impl Fn(&ostro_worker::WorkerEvent) -> bool,
    ) -> ostro_worker::WorkerEvent {
        timeout(limit, async {
            loop {
                match self.events.recv().await {
                    Ok(e) if f(&e) => return e,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(e) => panic!("worker events closed: {e}"),
                }
            }
        })
        .await
        .expect("worker event in time")
    }
}

impl Drop for WorkerProc {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
        self.shutdown.cancel();
    }
}

// ------------------------------------------------------------------------- raw protocol

/// A client identity (PEM).
#[derive(Clone)]
pub struct TestIdentity {
    pub cert_pem: String,
    pub key_pem: String,
    pub ca_pem: String,
}

/// An in-memory ECDSA P-256 key and a PKCS#10 CSR (PEM).
pub fn new_key_and_csr(worker_id: &str) -> (String, String) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
    let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
    p.distinguished_name
        .push(rcgen::DnType::CommonName, worker_id);
    let csr = p.serialize_request(&key).expect("csr").pem().expect("pem");
    (key.serialize_pem(), csr)
}

/// The `Hello` of this build.
pub fn hello(worker_id: &str, slots: u32) -> v1::Hello {
    v1::Hello {
        worker_id: worker_id.to_owned(),
        proto_version: PROTO_VERSION,
        ostro_version: OSTRO_VERSION.to_owned(),
        wasmtime_version: WASMTIME_VERSION.to_owned(),
        event_codec_ver: u32::from(ostro_types::journal::EVENT_CODEC_VER),
        wit_packages: vec!["ostro:runtime@0.1.0".into()],
        labels: [("zone".to_owned(), "a".to_owned())].into_iter().collect(),
        slots,
        memory_bytes: 1 << 30,
    }
}

/// A tonic client to the cluster listener (`ca` = trusted root; `id` = client identity).
pub async fn connect(
    addr: SocketAddr,
    ca_pem: &str,
    id: Option<&TestIdentity>,
) -> Result<Client, String> {
    let mut tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_pem));
    if let Some(id) = id {
        tls = tls.identity(Identity::from_pem(&id.cert_pem, &id.key_pem));
    }
    let ch = Endpoint::from_shared(format!("https://{addr}"))
        .map_err(|e| e.to_string())?
        .tls_config(tls)
        .map_err(|e| e.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .map_err(|e| format!("connect: {e:?}"))?;
    Ok(v1::execute_client::ExecuteClient::new(ch)
        .max_decoding_message_size(service::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(service::MAX_MESSAGE_BYTES))
}

/// A raw protocol client with an open session.
pub struct TestWorker {
    pub client: Client,
    pub out: mpsc::UnboundedSender<v1::WorkerMsg>,
    pub inbound: tonic::Streaming<v1::ServerMsg>,
    pub welcome: v1::Welcome,
    pub identity: TestIdentity,
    pub token_id: Option<String>,
}

/// What opening a raw session produced.
#[derive(Debug)]
pub enum Opened {
    Welcome,
    GoAway(String),
    Refused(tonic::Status),
}

impl TestWorker {
    /// Connect, send `hello`, expect `Welcome`.
    pub async fn open(
        addr: SocketAddr,
        id: &TestIdentity,
        hello: v1::Hello,
    ) -> Result<TestWorker, String> {
        let (w, opened) = Self::try_open(addr, id, hello).await?;
        match (w, opened) {
            (Some(w), Opened::Welcome) => Ok(w),
            (_, o) => Err(format!("{o:?}")),
        }
    }

    /// Connect and send `hello`; report what came back.
    pub async fn try_open(
        addr: SocketAddr,
        id: &TestIdentity,
        hello: v1::Hello,
    ) -> Result<(Option<TestWorker>, Opened), String> {
        let mut client = connect(addr, &id.ca_pem, Some(id)).await?;
        let (out, rx) = mpsc::unbounded_channel();
        out.send(v1::WorkerMsg {
            msg: Some(v1::worker_msg::Msg::Hello(hello)),
        })
        .expect("send hello");
        let mut inbound = match client.session(UnboundedReceiverStream::new(rx)).await {
            Ok(r) => r.into_inner(),
            Err(s) => return Ok((None, Opened::Refused(s))),
        };
        let first = timeout(T, inbound.message())
            .await
            .map_err(|_| "no reply".to_owned())?;
        match first {
            Ok(Some(v1::ServerMsg {
                msg: Some(v1::server_msg::Msg::Welcome(welcome)),
            })) => Ok((
                Some(TestWorker {
                    client,
                    out,
                    inbound,
                    welcome,
                    identity: id.clone(),
                    token_id: None,
                }),
                Opened::Welcome,
            )),
            Ok(Some(v1::ServerMsg {
                msg: Some(v1::server_msg::Msg::GoAway(g)),
            })) => Ok((None, Opened::GoAway(g.reason))),
            Ok(other) => Err(format!("unexpected first message {other:?}")),
            Err(s) => Ok((None, Opened::Refused(s))),
        }
    }

    pub fn session_id(&self) -> Vec<u8> {
        self.welcome.session_id.clone()
    }

    pub fn send(&self, msg: v1::worker_msg::Msg) {
        let _ = self.out.send(v1::WorkerMsg { msg: Some(msg) });
    }

    pub fn heartbeat(&self, leases: Vec<v1::Lease>, free_slots: u32) {
        self.send(v1::worker_msg::Msg::Heartbeat(v1::Heartbeat {
            leases,
            free_slots,
            memory_used: 0,
        }));
    }

    /// The next server message within `limit` (`None` = stream ended or timed out).
    pub async fn next(
        &mut self,
        limit: Duration,
    ) -> Option<Result<v1::server_msg::Msg, tonic::Status>> {
        match timeout(limit, self.inbound.message()).await {
            Ok(Ok(Some(v1::ServerMsg { msg: Some(m) }))) => Some(Ok(m)),
            Ok(Ok(_)) => None,
            Ok(Err(s)) => Some(Err(s)),
            Err(_) => None,
        }
    }
}

/// Poll `f` every 10 ms until true or `limit` (panics).
pub async fn wait_for(limit: Duration, what: &str, f: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if f() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {limit:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A throwaway CA (rcgen) for external-CA and wrong-CA tests.
pub struct OtherCa {
    pub pem: String,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl OtherCa {
    pub fn new(cn: &str) -> Self {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        p.distinguished_name.push(rcgen::DnType::CommonName, cn);
        let cert = p.self_signed(&key).expect("self sign");
        OtherCa {
            pem: cert.pem(),
            issuer: rcgen::Issuer::new(p, key),
        }
    }

    /// A client cert with one SAN URI (clientAuth unless `no_eku`), trusted root `ca_pem`
    /// for the server.
    pub fn client(&self, san_uri: &str, server_ca_pem: &str, client_auth: bool) -> TestIdentity {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("key");
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        p.subject_alt_names = vec![rcgen::SanType::URI(
            rcgen::string::Ia5String::try_from(san_uri).expect("ia5"),
        )];
        if client_auth {
            p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        }
        p.distinguished_name
            .push(rcgen::DnType::CommonName, "external worker");
        let cert = p.signed_by(&key, &self.issuer).expect("sign");
        TestIdentity {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            ca_pem: server_ca_pem.to_owned(),
        }
    }
}

/// The external CA PEM path a rig was started with (for reference).
pub fn external_ca_path(rig: &Rig) -> Option<PathBuf> {
    rig.cfg.cluster.tls.client_ca.clone()
}

// ----------------------------------------------------------------- full servers (07-12)

/// A loopback `POST /v1/workers/join` over `join` (the 07-07 worker's join contract).
pub async fn join_endpoint(join: Arc<JoinService>, shutdown: CancellationToken) -> String {
    use axum::Json;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let app = axum::Router::new().route(
        "/v1/workers/join",
        axum::routing::post(move |Json(req): Json<JoinRequest>| {
            let join = Arc::clone(&join);
            async move {
                match join.join(req).await {
                    Ok(r) => (StatusCode::OK, Json(serde_json::to_value(r).expect("json")))
                        .into_response(),
                    Err(e) => (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "error": { "message": e.message } })),
                    )
                        .into_response(),
                }
            }
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind join");
    let url = format!("http://{}", l.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(l, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await;
    });
    url
}

/// Workers of a full in-process server (`ServerHandle::p7()`): in-process `ostro_worker`s
/// joined through a loopback join endpoint over the server's JoinService, and raw protocol
/// clients on its cluster listener.
pub struct ServerWorkers {
    pub p7: Arc<ostro_server::p7::P7Shared>,
    pub join_url: String,
    pub shutdown: CancellationToken,
}

impl ServerWorkers {
    pub async fn new(p7: Arc<ostro_server::p7::P7Shared>) -> Self {
        let shutdown = CancellationToken::new();
        let join_url = join_endpoint(Arc::clone(&p7.join), shutdown.clone()).await;
        Self {
            p7,
            join_url,
            shutdown,
        }
    }

    /// The cluster endpoint workers connect to (the last `set_endpoints`).
    pub fn cluster_addr(&self) -> SocketAddr {
        self.p7.join.endpoints()[0]
            .parse()
            .expect("cluster endpoint")
    }

    async fn token(&self) -> WorkerTokenCreated {
        self.p7
            .join
            .create_token(Some(Duration::from_secs(3600)), "http://127.0.0.1:1")
            .await
            .expect("create token")
    }

    /// Start an in-process worker with a fresh token.
    pub async fn spawn(&self, worker_id: &str, slots: u32, labels: &[(&str, &str)]) -> WorkerProc {
        let tok = self.token().await;
        let cache = tempfile::tempdir().expect("cache dir");
        let cfg = ostro_worker::WorkerConfig {
            join: Some(self.join_url.clone()),
            token: Some(ostro_worker::JoinToken::new(&tok.token)),
            ca_hash: Some(self.p7.tls.ca_hash().expect("ca hash")),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            slots,
            cache_dir: cache.path().to_path_buf(),
            worker_id: Some(worker_id.to_owned()),
            reconnect_max_ms: 200,
            rejoin_after_failures: 2,
            ..ostro_worker::WorkerConfig::default()
        };
        let worker = Arc::new(ostro_worker::Worker::new(cfg).expect("worker config"));
        let events = worker.subscribe();
        let shutdown = CancellationToken::new();
        let w = Arc::clone(&worker);
        let sd = shutdown.clone();
        let task = tokio::spawn(async move { w.run(sd).await });
        let p = WorkerProc {
            worker,
            events,
            shutdown,
            task: Some(task),
            _cache: cache,
        };
        let reg = Arc::clone(&self.p7.registry);
        let id = worker_id.to_owned();
        wait_for(T, "worker credit registered", move || {
            reg.sessions()
                .iter()
                .any(|s| s.worker_id == id && s.free > 0)
        })
        .await;
        p
    }

    /// Like [`spawn`](Self::spawn), but on a dedicated runtime: [`IsolatedWorker::kill`]
    /// drops every task of the worker at once (session, connections and activations), as
    /// a process kill does.
    pub async fn spawn_isolated(
        &self,
        worker_id: &str,
        slots: u32,
        labels: &[(&str, &str)],
    ) -> IsolatedWorker {
        let tok = self.token().await;
        let cache = tempfile::tempdir().expect("cache dir");
        let cfg = ostro_worker::WorkerConfig {
            join: Some(self.join_url.clone()),
            token: Some(ostro_worker::JoinToken::new(&tok.token)),
            ca_hash: Some(self.p7.tls.ca_hash().expect("ca hash")),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            slots,
            cache_dir: cache.path().to_path_buf(),
            worker_id: Some(worker_id.to_owned()),
            reconnect_max_ms: 200,
            rejoin_after_failures: 2,
            ..ostro_worker::WorkerConfig::default()
        };
        let worker = Arc::new(ostro_worker::Worker::new(cfg).expect("worker config"));
        let events = worker.subscribe();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("isolated-worker")
            .build()
            .expect("worker runtime");
        let w = Arc::clone(&worker);
        rt.spawn(async move { w.run(CancellationToken::new()).await });
        let reg = Arc::clone(&self.p7.registry);
        let id = worker_id.to_owned();
        wait_for(T, "worker credit registered", move || {
            reg.sessions()
                .iter()
                .any(|s| s.worker_id == id && s.free > 0)
        })
        .await;
        IsolatedWorker {
            worker,
            events,
            rt: Some(rt),
            _cache: cache,
        }
    }

    /// A raw protocol client joined as `worker_id` with `Credit{slots}` sent.
    pub async fn raw(&self, worker_id: &str, slots: u32) -> TestWorker {
        let tok = self.token().await;
        let (key_pem, csr_pem) = new_key_and_csr(worker_id);
        let r = self
            .p7
            .join
            .join(JoinRequest {
                token: tok.token.clone(),
                csr_pem,
                worker_id: worker_id.to_owned(),
                labels: BTreeMap::new(),
            })
            .await
            .expect("join");
        let id = TestIdentity {
            cert_pem: r.client_cert_pem,
            key_pem,
            ca_pem: r.ca_cert_pem,
        };
        let mut w = TestWorker::open(self.cluster_addr(), &id, hello(worker_id, slots))
            .await
            .expect("session");
        w.token_id = Some(tok.token_id);
        w.send(v1::worker_msg::Msg::Credit(v1::Credit {
            free_slots: slots,
        }));
        let reg = Arc::clone(&self.p7.registry);
        let sid = w.session_id();
        wait_for(T, "raw credit registered", move || {
            reg.sessions()
                .iter()
                .any(|s| s.session_id == sid && s.free > 0)
        })
        .await;
        w
    }
}

impl Drop for ServerWorkers {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// A second cluster listener over a full server's registry and join service, recording
/// the payload size of every bulk message ([`WireStats`]). Joins made afterwards
/// advertise this listener (`JoinService::set_endpoints`).
pub fn wire_listener(
    cfg: Arc<ServerConfig>,
    p7: &Arc<ostro_server::p7::P7Shared>,
    meta: ostro_meta::MetaView,
    clock: Arc<dyn ostro_partition::Clock>,
    lease_ttl: Duration,
    shutdown: CancellationToken,
) -> (SocketAddr, Arc<WireStats>) {
    let wire = Arc::new(WireStats::default());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let (addr, _task) = service::serve(
        listener,
        ClusterDeps {
            config: cfg,
            tls: Arc::clone(&p7.tls),
            registry: Arc::clone(&p7.registry),
            join: Arc::clone(&p7.join),
            meta,
            clock,
            node_id: 1,
            lease_ttl,
            shutdown,
            wire: Some(Arc::clone(&wire)),
        },
    )
    .expect("wire listener");
    (addr, wire)
}

/// Journal equality across executions (embedded vs remote): per event its kind, the
/// command hash of command events, and the recorded outcome of steps and the run. Times,
/// run ids, seeds and fuel are execution-specific and left out.
pub fn journal_shape(journal: &[ostro_types::journal::Event]) -> Vec<String> {
    use ostro_types::journal::EventBody;
    journal
        .iter()
        .map(|e| {
            let kind = e.body.kind();
            match &e.body {
                EventBody::StepCompleted(s) => {
                    format!("{} {kind:?} {:?} {} {:?}", e.seq, s.meta, s.name, s.outcome)
                }
                EventBody::RunCompleted(r) => format!("{} {kind:?} {r:?}", e.seq),
                EventBody::RunFailed(r) => format!("{} {kind:?} {r:?}", e.seq),
                b => format!("{} {kind:?} {:?}", e.seq, b.command_meta()),
            }
        })
        .collect()
}

/// An in-process worker on its own runtime ([`ServerWorkers::spawn_isolated`]).
pub struct IsolatedWorker {
    pub worker: Arc<ostro_worker::Worker>,
    pub events: tokio::sync::broadcast::Receiver<ostro_worker::WorkerEvent>,
    rt: Option<tokio::runtime::Runtime>,
    _cache: TempDir,
}

impl IsolatedWorker {
    /// Drop every task of the worker now (a process kill: its TCP connections close).
    pub fn kill(mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }

    /// The next event matching `f` within `limit`.
    pub async fn wait_event(
        &mut self,
        limit: Duration,
        f: impl Fn(&ostro_worker::WorkerEvent) -> bool,
    ) -> ostro_worker::WorkerEvent {
        timeout(limit, async {
            loop {
                match self.events.recv().await {
                    Ok(e) if f(&e) => return e,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(e) => panic!("worker events closed: {e}"),
                }
            }
        })
        .await
        .expect("worker event in time")
    }
}

impl Drop for IsolatedWorker {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}
