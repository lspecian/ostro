//! Phase 13 surface (13-03) over a real listener, REST and gRPC: every D-P13-23 route and
//! method against `FakeCluster` / `FakeModuleLifecycle`, 404 / `UNIMPLEMENTED` while a slot
//! is absent, 501 from default trait bodies, the join route without a client identity, and
//! the request-metadata headers in (`ostro-*`, deadline) and out (`ostro-consistency`).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_api::fake::{FakeCluster, FakeControlPlane, FakeMigrateOutcome, FakeModuleLifecycle};
use ostro_api::phase13::*;
use ostro_api::request_meta::{self, RequestMeta, TrustedNode};
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::cluster_client::ClusterClient;
use ostro_proto::api::v1::module_lifecycle_client::ModuleLifecycleClient;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tonic::transport::Channel;

const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct Server {
    addr: SocketAddr,
    http: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(ext: ApiExtensions, trusted: Option<u64>) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
        let mut app = ostro_api::router_with(cp, ApiConfig::default(), ext);
        if let Some(node) = trusted {
            // What the cluster listener (13-12) does after its mTLS check.
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
        Server {
            addr,
            http: reqwest::Client::builder()
                .http1_only()
                .build()
                .expect("client"),
            task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> (StatusCode, Value) {
        let r = req.send().await.expect("request");
        let st = r.status();
        let text = r.text().await.expect("body");
        (
            st,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.http.get(self.url(path))).await
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(self.http.post(self.url(path)).json(&body)).await
    }

    async fn post_empty(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.http.post(self.url(path))).await
    }

    async fn delete(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.http.delete(self.url(path))).await
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://{}", self.addr))
            .expect("uri")
            .connect()
            .await
            .expect("h2c connect")
    }
}

struct Fakes {
    cluster: Arc<FakeCluster>,
    modules: Arc<FakeModuleLifecycle>,
}

fn fakes() -> (Fakes, ApiExtensions) {
    let cluster = FakeCluster::new();
    let modules = FakeModuleLifecycle::new();
    let mut ext = ApiExtensions::default();
    cluster.install(&mut ext);
    modules.install(&mut ext);
    modules.add_module(SHA_A, &[("order", 1)], false);
    modules.add_module(SHA_B, &[("order", 2)], true);
    modules.add_run("r1", "order", FakeMigrateOutcome::Pass);
    modules.add_run(
        "r2",
        "order",
        FakeMigrateOutcome::Fail {
            seq: 7,
            detail: "diverged".into(),
        },
    );
    modules.add_run("r3", "order", FakeMigrateOutcome::Busy);
    (Fakes { cluster, modules }, ext)
}

fn code(v: &Value) -> &str {
    v["error"]["code"].as_str().unwrap_or("<none>")
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("<none>")
}

fn join_body(token: &str) -> Value {
    json!({
        "token": token,
        "csr_pem": "-----BEGIN CERTIFICATE REQUEST-----",
        "advertise": {"api": "10.0.0.2:7070", "cluster": "10.0.0.2:7071"},
        "node_version": {"ostro_version": "0.1.0", "level_min": 1, "level_max": 2,
            "wasmtime": "50.0.2", "event_codec_ver": 1, "wit_lines": ["0.1"]}
    })
}

/// Every REST route (method, path) of D-P13-23.
const ROUTES: &[(&str, &str)] = &[
    ("post", "/v1/cluster/init"),
    ("post", "/v1/cluster/join"),
    ("get", "/v1/cluster/status"),
    ("post", "/v1/cluster/tokens"),
    ("post", "/v1/cluster/members/2/remove"),
    ("post", "/v1/cluster/members/2/drain"),
    ("post", "/v1/cluster/reassign"),
    ("post", "/v1/cluster/rebalance"),
    ("get", "/v1/cluster/upgrade"),
    ("post", "/v1/cluster/upgrade/finalize"),
    ("post", "/v1/cluster/upgrade/hold"),
    ("post", "/v1/cluster/upgrade/release"),
    (
        "delete",
        "/v1/modules/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ),
    ("post", "/v1/modules/gc"),
    ("post", "/v1/runs/migrate"),
];

fn route_body(path: &str) -> Value {
    match path {
        "/v1/cluster/join" => join_body("njt_1.x"),
        "/v1/cluster/reassign" => json!({"partition": 1, "replicas": [1]}),
        "/v1/runs/migrate" => json!({"workflow": "order", "from": {"version": 1}, "to_version": 2}),
        _ => json!({}),
    }
}

async fn call(srv: &Server, method: &str, path: &str) -> (StatusCode, Value) {
    match method {
        "get" => srv.get(path).await,
        "delete" => srv.delete(path).await,
        _ => srv.post(path, route_body(path)).await,
    }
}

#[tokio::test]
async fn rest_routes_answer_404_while_the_slots_are_absent() {
    let srv = Server::start(ApiExtensions::default(), None).await;
    for (m, p) in ROUTES {
        let (st, v) = call(&srv, m, p).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "{m} {p}: {v}");
        assert_eq!(code(&v), "not_found", "{m} {p}");
    }
}

/// A slot whose implementation still has the 13-01 default bodies (the 13-02 stubs).
struct Stubs;
impl ClusterApi for Stubs {}
impl ModuleLifecycleApi for Stubs {}

#[tokio::test]
async fn default_trait_bodies_answer_501_on_rest_and_unimplemented_on_grpc() {
    let ext = ApiExtensions {
        cluster: Some(Arc::new(Stubs)),
        module_lifecycle: Some(Arc::new(Stubs)),
        ..ApiExtensions::default()
    };
    let srv = Server::start(ext, None).await;
    for (m, p) in ROUTES {
        let (st, v) = call(&srv, m, p).await;
        assert_eq!(st, StatusCode::NOT_IMPLEMENTED, "{m} {p}: {v}");
        assert_eq!(code(&v), "unimplemented", "{m} {p}");
    }
    let mut c = ClusterClient::new(srv.channel().await);
    let s = c
        .status(pb::ClusterStatusRequest {})
        .await
        .expect_err("stub");
    assert_eq!(s.code(), tonic::Code::Unimplemented);
    assert!(s.message().contains("13-09"), "{}", s.message());
}

#[tokio::test]
async fn rest_cluster_routes_end_to_end() {
    let (f, ext) = fakes();
    let srv = Server::start(ext, None).await;

    // Before init: status works, writes are 412.
    let (st, v) = srv.get("/v1/cluster/status").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["voters"], json!([]));
    let (st, v) = srv.post_empty("/v1/cluster/tokens").await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED, "{v}");

    // Init with an empty body takes the defaults; a second init is 409.
    let (st, v) = srv.post_empty("/v1/cluster/init").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["voters"], json!([1]));
    assert_eq!(
        v["policy"],
        json!({"rf": 3, "min_insync": 2, "max_voters": 5})
    );
    let (st, v) = srv
        .post("/v1/cluster/init", json!({"rf": 5, "min_insync": 3}))
        .await;
    assert_eq!((st, reason(&v)), (StatusCode::CONFLICT, "already_cluster"));

    // Token: 201, no-store.
    let r = srv
        .http
        .post(srv.url("/v1/cluster/tokens"))
        .json(&json!({"ttl_secs": 60}))
        .send()
        .await
        .expect("token");
    assert_eq!(r.status(), StatusCode::CREATED);
    assert_eq!(r.headers()["cache-control"], "no-store");
    let tok: NodeTokenResponse = r.json().await.expect("json");
    assert_eq!(tok.id, "njt_1");
    assert!(tok.ca_hash.starts_with("sha256:"));

    // Join: reachable with no client identity at all, token in the body, no-store.
    let r = reqwest::Client::new()
        .post(srv.url("/v1/cluster/join"))
        .json(&join_body(&tok.token))
        .send()
        .await
        .expect("join");
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers()["cache-control"], "no-store");
    let joined: JoinResponse = r.json().await.expect("json");
    assert_eq!(joined.node_id, 2);
    assert_eq!(joined.seeds.len(), 1);
    let (st, v) = srv.post("/v1/cluster/join", join_body(&tok.token)).await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::BAD_REQUEST, "join_token_invalid")
    );
    let (st, v) = srv.post("/v1/cluster/join", json!({"token": "x"})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "missing fields: {v}");

    let (st, v) = srv.get("/v1/cluster/status").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["learners"], json!([2]));
    assert_eq!(v["members"][1]["state"], "joining");
    f.cluster.promote(2);

    // Reassign: validation, start, in-progress, cancel.
    let (st, v) = srv
        .post("/v1/cluster/reassign", json!({"partition": 3}))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = srv
        .post(
            "/v1/cluster/reassign",
            json!({"partition": 3, "replicas": [1, 1]}),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = srv
        .post(
            "/v1/cluster/reassign",
            json!({"partition": 3, "replicas": [1, 2]}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["reassignments"][0]["target"], json!([1, 2]));
    let (st, v) = srv
        .post(
            "/v1/cluster/reassign",
            json!({"partition": 3, "replicas": [2]}),
        )
        .await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::CONFLICT, "reassignment_in_progress")
    );
    let (st, v) = srv
        .post(
            "/v1/cluster/reassign",
            json!({"partition": 3, "cancel": true}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["reassignments"], json!([]));
    let (st, _) = srv.post_empty("/v1/cluster/rebalance").await;
    assert_eq!(st, StatusCode::OK);

    // Drain / remove: bad ids, unknown members, quorum, force.
    let (st, v) = srv.post_empty("/v1/cluster/members/abc/drain").await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = srv.post_empty("/v1/cluster/members/0/remove").await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, _) = srv.post_empty("/v1/cluster/members/9/drain").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, v) = srv.post_empty("/v1/cluster/members/2/drain").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["members"][1]["state"], "draining");
    f.cluster.set_live(2, false);
    let (st, _) = srv.post_empty("/v1/cluster/members/2/remove").await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    let (st, v) = srv
        .post("/v1/cluster/members/2/remove", json!({"force": true}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["members"][1]["state"], "removed");
    assert_eq!(v["voters"], json!([1]));
    let (st, v) = srv.post_empty("/v1/cluster/members/1/remove").await;
    assert_eq!((st, reason(&v)), (StatusCode::CONFLICT, "quorum"));

    // Upgrades.
    let (st, v) = srv.get("/v1/cluster/upgrade").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        (v["finalized"].clone(), v["common_level_max"].clone()),
        (json!(1), json!(2))
    );
    let (st, v) = srv.post_empty("/v1/cluster/upgrade/hold").await;
    assert_eq!((st, v["hold"].clone()), (StatusCode::OK, json!(1)));
    let (st, v) = srv.post_empty("/v1/cluster/upgrade/finalize").await;
    assert_eq!((st, reason(&v)), (StatusCode::CONFLICT, "held"));
    let (st, v) = srv.post_empty("/v1/cluster/upgrade/release").await;
    assert_eq!((st, v["hold"].clone()), (StatusCode::OK, Value::Null));
    let (st, v) = srv.post_empty("/v1/cluster/upgrade/finalize").await;
    assert_eq!((st, v["finalized"].clone()), (StatusCode::OK, json!(2)));
}

#[tokio::test]
async fn rest_module_lifecycle_routes_end_to_end() {
    let (f, ext) = fakes();
    let srv = Server::start(ext, None).await;

    let (st, v) = srv.delete("/v1/modules/not-a-sha").await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = srv.delete(&format!("/v1/modules/{SHA_B}")).await;
    assert_eq!(st, StatusCode::CONFLICT, "active: {v}");
    let (st, v) = srv.delete(&format!("/v1/modules/{SHA_A}")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["versions"], json!([{"name": "order", "version": 1}]));
    let (st, v) = srv.delete(&format!("/v1/modules/{SHA_A}")).await;
    assert_eq!((st, reason(&v)), (StatusCode::CONFLICT, "module_retired"));
    // GET of a module is untouched by the new DELETE on the same path.
    let (st, _) = srv.get(&format!("/v1/modules/{SHA_A}")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    f.modules.set_referenced(SHA_A, true);
    let (st, v) = srv.post_empty("/v1/modules/gc").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["referenced"], json!([SHA_A]));
    f.modules.set_referenced(SHA_A, false);
    let (_, v) = srv.post_empty("/v1/modules/gc").await;
    assert_eq!(v["deleted"], json!([SHA_A]));

    let (st, v) = srv
        .post(
            "/v1/runs/migrate",
            json!({"workflow": "order", "from": {"version": 1}, "to_version": 2, "dry_run": true}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["passed"], json!(["r1"]));
    assert_eq!(v["repinned"], json!([]));
    assert_eq!(v["failed"][0]["seq"], 7);
    assert_eq!(v["busy"], json!(["r3"]));
    let (st, v) = srv
        .post(
            "/v1/runs/migrate",
            json!({"workflow": "order", "from": {"sha": SHA_A}, "to_version": 2, "limit": 1}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["repinned"], json!(["r1"]));
    for bad in [
        json!({"workflow": "", "from": {"version": 1}, "to_version": 2}),
        json!({"workflow": "order", "from": {"sha": "xyz"}, "to_version": 2}),
        json!({"workflow": "order", "from": {"version": 1}, "to_version": 2, "limit": 0}),
        json!({"workflow": "order", "to_version": 2}),
    ] {
        let (st, v) = srv.post("/v1/runs/migrate", bad.clone()).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
    assert_eq!(f.modules.migrations().len(), 2);
}

#[tokio::test]
async fn grpc_services_are_unimplemented_while_the_slots_are_absent() {
    let srv = Server::start(ApiExtensions::default(), None).await;
    let ch = srv.channel().await;
    let s = ClusterClient::new(ch.clone())
        .status(pb::ClusterStatusRequest {})
        .await
        .expect_err("absent");
    assert_eq!(s.code(), tonic::Code::Unimplemented);
    let s = ClusterClient::new(ch.clone())
        .join(pb::JoinClusterRequest::default())
        .await
        .expect_err("absent");
    assert_eq!(s.code(), tonic::Code::Unimplemented);
    let s = ModuleLifecycleClient::new(ch)
        .run_module_gc(pb::RunModuleGcRequest {})
        .await
        .expect_err("absent");
    assert_eq!(s.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn grpc_methods_end_to_end() {
    let (f, ext) = fakes();
    let srv = Server::start(ext, None).await;
    let ch = srv.channel().await;
    let mut c = ClusterClient::new(ch.clone());
    let mut m = ModuleLifecycleClient::new(ch);

    let st = c
        .init(pb::InitClusterRequest {
            rf: Some(3),
            ..Default::default()
        })
        .await
        .expect("init")
        .into_inner();
    assert_eq!(st.voters, vec![1]);
    let e = c
        .init(pb::InitClusterRequest {
            rf: Some(300),
            ..Default::default()
        })
        .await
        .expect_err("rf out of range");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let tok = c
        .create_node_token(pb::CreateNodeTokenRequest { ttl_secs: None })
        .await
        .expect("token")
        .into_inner();
    let joined = c
        .join(pb::JoinClusterRequest {
            token: tok.token.clone(),
            csr_pem: "csr".into(),
            advertise: Some(pb::NodeAdvertise {
                api: "a:1".into(),
                cluster: "a:2".into(),
                repl: None,
            }),
            node_version: Some(pb::NodeVersion {
                level_min: 1,
                level_max: 2,
                ..Default::default()
            }),
            resume_node_id: None,
        })
        .await
        .expect("join")
        .into_inner();
    assert_eq!(joined.node_id, 2);
    let e = c
        .join(pb::JoinClusterRequest {
            token: tok.token,
            ..Default::default()
        })
        .await
        .expect_err("missing advertise");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    f.cluster.promote(2);
    let st = c
        .status(pb::ClusterStatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert_eq!(st.voters, vec![1, 2]);
    let st = c
        .reassign(pb::ReassignPartitionRequest {
            partition: 4,
            replicas: vec![1, 2],
            cancel: false,
        })
        .await
        .expect("reassign")
        .into_inner();
    assert_eq!(st.reassignments[0].partition, 4);
    let e = c
        .reassign(pb::ReassignPartitionRequest {
            partition: 4,
            replicas: vec![],
            cancel: false,
        })
        .await
        .expect_err("empty replicas");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    c.rebalance(pb::RebalanceRequest {})
        .await
        .expect("rebalance");
    c.drain(pb::DrainMemberRequest { node_id: 2 })
        .await
        .expect("drain");
    let e = c
        .drain(pb::DrainMemberRequest { node_id: 0 })
        .await
        .expect_err("id 0");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let st = c
        .remove(pb::RemoveMemberRequest {
            node_id: 2,
            force: false,
        })
        .await
        .expect("remove")
        .into_inner();
    assert_eq!(st.voters, vec![1]);
    let e = c
        .remove(pb::RemoveMemberRequest {
            node_id: 1,
            force: true,
        })
        .await
        .expect_err("quorum");
    assert_eq!(e.code(), tonic::Code::AlreadyExists);
    assert_eq!(
        ostro_api::grpc::status_details(&e).expect("details")["reason"],
        "quorum"
    );
    let u = c
        .upgrade_hold(pb::UpgradeHoldRequest {})
        .await
        .expect("hold")
        .into_inner();
    assert_eq!(u.hold, Some(1));
    c.upgrade_release(pb::UpgradeReleaseRequest {})
        .await
        .expect("release");
    let u = c
        .upgrade_finalize(pb::UpgradeFinalizeRequest {})
        .await
        .expect("finalize")
        .into_inner();
    assert_eq!(u.finalized, 2);
    let u = c
        .upgrade_status(pb::UpgradeStatusRequest {})
        .await
        .expect("status")
        .into_inner();
    assert_eq!(u.common_level_max, 2);

    let r = m
        .retire_module(pb::RetireModuleRequest {
            sha256: SHA_A.into(),
        })
        .await
        .expect("retire")
        .into_inner();
    assert_eq!(r.versions[0].name, "order");
    let e = m
        .retire_module(pb::RetireModuleRequest { sha256: "x".into() })
        .await
        .expect_err("bad sha");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let g = m
        .run_module_gc(pb::RunModuleGcRequest {})
        .await
        .expect("gc")
        .into_inner();
    assert_eq!(g.deleted, vec![SHA_A.to_owned()]);
    let rep = m
        .migrate_runs(pb::MigrateRunsRequest {
            workflow: "order".into(),
            from: Some(pb::migrate_runs_request::From::FromVersion(1)),
            to_version: 2,
            ..Default::default()
        })
        .await
        .expect("migrate")
        .into_inner();
    assert_eq!(rep.repinned, vec!["r1".to_owned()]);
    let e = m
        .migrate_runs(pb::MigrateRunsRequest {
            workflow: "order".into(),
            ..Default::default()
        })
        .await
        .expect_err("no from");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
}

/// Records the request metadata each call sees and publishes a consistency token.
#[derive(Default)]
struct Probe {
    seen: Mutex<Vec<(RequestMeta, Instant)>>,
}

impl Probe {
    fn record(&self) {
        self.seen
            .lock()
            .expect("lock")
            .push((RequestMeta::current().expect("in scope"), Instant::now()));
    }

    fn take(&self) -> Vec<(RequestMeta, Instant)> {
        std::mem::take(&mut *self.seen.lock().expect("lock"))
    }
}

#[async_trait::async_trait]
impl ClusterApi for Probe {
    async fn status(&self) -> Result<ClusterStatus, ApiError> {
        self.record();
        request_meta::set_consistency("m:42".into());
        Ok(ClusterStatus::default())
    }

    async fn upgrade_hold(&self) -> Result<UpgradeStatus, ApiError> {
        self.record();
        request_meta::set_consistency("m:43".into());
        Err(ApiError::conflict("held elsewhere"))
    }
}

fn probe_ext(p: &Arc<Probe>) -> ApiExtensions {
    ApiExtensions {
        cluster: Some(Arc::clone(p) as Arc<dyn ClusterApi>),
        ..ApiExtensions::default()
    }
}

const META_HEADERS: &[(&str, &str)] = &[
    ("ostro-hop", "1"),
    ("ostro-partition", "5"),
    ("ostro-partitions", "1,2,3"),
    ("ostro-principal", "user:alice"),
    ("ostro-read-after", "p5:2:99"),
];

#[tokio::test]
async fn rest_headers_round_trip() {
    for trusted in [None, Some(7)] {
        let p = Arc::new(Probe::default());
        let srv = Server::start(probe_ext(&p), trusted).await;
        let mut req = srv.http.get(srv.url("/v1/cluster/status"));
        for (k, v) in META_HEADERS {
            req = req.header(*k, *v);
        }
        let sent = Instant::now();
        let r = req
            .header("ostro-deadline-ms", "5000")
            .send()
            .await
            .expect("status");
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()["ostro-consistency"], "m:42");
        let (meta, at) = p.take().pop().expect("recorded");
        assert_eq!(meta.hop, 1);
        assert_eq!(meta.read_after.as_deref(), Some("p5:2:99"));
        let left = meta.deadline.expect("deadline").duration_since(at);
        assert!(
            left <= Duration::from_secs(5) && left > Duration::from_secs(4),
            "{left:?}"
        );
        assert!(meta.deadline.expect("deadline") >= sent);
        match trusted {
            None => assert_eq!(
                (
                    meta.partition_pin,
                    meta.partitions,
                    meta.principal,
                    meta.from_node
                ),
                (None, None, None, None),
                "client headers must not pin"
            ),
            Some(n) => {
                assert_eq!(meta.partition_pin, Some(5));
                assert_eq!(meta.partitions, Some(vec![1, 2, 3]));
                assert_eq!(meta.principal.as_deref(), Some("user:alice"));
                assert_eq!(meta.from_node, Some(n));
            }
        }
        // `from_node` never comes from a header, even a made-up one.
        let r = srv
            .http
            .get(srv.url("/v1/cluster/status"))
            .header("ostro-from-node", "9")
            .send()
            .await
            .expect("status");
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(p.take()[0].0.from_node, trusted);

        // Error: no consistency header even though the handler published one.
        let r = srv
            .http
            .post(srv.url("/v1/cluster/upgrade/hold"))
            .send()
            .await
            .expect("hold");
        assert_eq!(r.status(), StatusCode::CONFLICT);
        assert!(r.headers().get("ostro-consistency").is_none());
        assert_eq!(p.take().len(), 1, "the failing handler ran once");

        // Malformed header: 400 before the handler runs.
        let r = srv
            .http
            .get(srv.url("/v1/cluster/status"))
            .header("ostro-partition", "70000")
            .send()
            .await
            .expect("status");
        assert_eq!(r.status(), StatusCode::BAD_REQUEST);
        let v: Value = r.json().await.expect("json");
        assert_eq!(v["error"]["details"]["header"], "ostro-partition");
        assert!(p.take().is_empty(), "handler must not run");
    }
}

#[tokio::test]
async fn grpc_headers_round_trip() {
    for trusted in [None, Some(4)] {
        let p = Arc::new(Probe::default());
        let srv = Server::start(probe_ext(&p), trusted).await;
        let mut c = ClusterClient::new(srv.channel().await);
        let mut req = tonic::Request::new(pb::ClusterStatusRequest {});
        for (k, v) in META_HEADERS {
            req.metadata_mut().insert(*k, v.parse().expect("ascii"));
        }
        req.set_timeout(Duration::from_secs(3));
        let resp = c.status(req).await.expect("status");
        assert_eq!(
            resp.metadata()
                .get("ostro-consistency")
                .and_then(|v| v.to_str().ok()),
            Some("m:42")
        );
        let (meta, at) = p.take().pop().expect("recorded");
        assert_eq!(meta.hop, 1);
        assert_eq!(meta.read_after.as_deref(), Some("p5:2:99"));
        let left = meta.deadline.expect("deadline").duration_since(at);
        assert!(
            left <= Duration::from_secs(3) && left > Duration::from_secs(2),
            "{left:?}"
        );
        assert_eq!(meta.from_node, trusted);
        assert_eq!(meta.partition_pin, trusted.map(|_| 5));

        let e = c
            .upgrade_hold(pb::UpgradeHoldRequest {})
            .await
            .expect_err("conflict");
        assert_eq!(e.code(), tonic::Code::AlreadyExists);
        assert!(e.metadata().get("ostro-consistency").is_none());
        assert_eq!(p.take().len(), 1, "the failing handler ran once");

        let mut req = tonic::Request::new(pb::ClusterStatusRequest {});
        req.metadata_mut()
            .insert("ostro-hop", "many".parse().expect("ascii"));
        let e = c.status(req).await.expect_err("malformed");
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            ostro_api::grpc::status_details(&e).expect("details")["header"],
            "ostro-hop"
        );
        assert!(p.take().is_empty(), "handler must not run");
    }
}

#[test]
fn every_phase13_route_is_documented() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    let paths = doc["paths"].as_object().expect("paths");
    for (m, p) in ROUTES {
        let p = p
            .replace("/members/2/", "/members/{id}/")
            .replace(SHA_A, "{sha256}");
        assert!(
            paths.get(&p).and_then(|i| i.get(*m)).is_some(),
            "{m} {p} missing from OpenAPI"
        );
    }
    // Phase 7's worker join keeps its schema names; the cluster join has its own.
    let schemas = doc["components"]["schemas"].as_object().expect("schemas");
    assert!(schemas.contains_key("ClusterJoinRequest"));
    assert!(
        schemas["JoinRequest"]["properties"]
            .get("worker_id")
            .is_some()
    );
}
