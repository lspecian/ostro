//! Phase 14 (14-02) bearer authentication over a real listener, REST and gRPC: 401 /
//! `UNAUTHENTICATED` before any handler on a missing, garbage or expired bearer; the
//! handler observes the authenticated principal; the unauthenticated routes never call the
//! authenticator (`/hooks/*` and the join routes run as `System`, health / UI as
//! `Anonymous`); `AllowDev` runs everything as `dev`; forwarded node calls are not this
//! layer's; spawned long-poll producers keep the caller's principal (`spawn_scoped`); and
//! every documented REST route and every gRPC method is either on the unauthenticated list
//! or authenticated (W12).

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::Method;
use ostro_api::auth::{
    self, AllowDev, AuthError, Authenticator, Principal, PrincipalKind, ScopeSet,
};
use ostro_api::authn_layer::{self, RouteAuth, UNAUTHENTICATED, WWW_AUTHENTICATE_VALUE};
use ostro_api::domain::*;
use ostro_api::phase7::{JoinRequest as WorkerJoin, JoinResponse as WorkerJoinResponse};
use ostro_api::phase8::{HookIngress, HookRequest, HookResponse};
use ostro_api::phase13::{ClusterApi, JoinRequest as NodeJoin, JoinResponse as NodeJoinResponse};
use ostro_api::request_meta::TrustedNode;
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane};
use ostro_proto::api::v1 as pb;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tonic::transport::Channel;

const GOOD: &str = concat!(
    "ostro_otk_aaaaaaaaaaaaaaaa_",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
);
const EXPIRED: &str = concat!(
    "ostro_otk_eeeeeeeeeeeeeeee_",
    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
);

fn reader() -> Principal {
    Principal::token(
        "otk_aaaaaaaaaaaaaaaa",
        None,
        ScopeSet::of(&[auth::Scope::Read]),
    )
}

/// Accepts [`GOOD`] as a `read` principal, reports [`EXPIRED`] as expired, rejects the rest.
#[derive(Default)]
struct FakeAuthn {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl Authenticator for FakeAuthn {
    async fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match bearer {
            None => Err(AuthError::Missing),
            Some(GOOD) => Ok(reader()),
            Some(EXPIRED) => Err(AuthError::Expired),
            Some(_) => Err(AuthError::Invalid),
        }
    }
}

/// Records every call's principal; every call counts.
#[derive(Default)]
struct Rec {
    calls: AtomicUsize,
    seen: Mutex<Vec<(String, Principal)>>,
}

impl Rec {
    fn note(&self, what: &str) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push((what.to_owned(), auth::current()));
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn last(&self, what: &str) -> Principal {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(w, _)| w == what)
            .map(|(_, p)| p.clone())
            .unwrap_or_else(|| panic!("{what} was never called"))
    }

    fn nf(&self, what: &str) -> ApiError {
        self.note(what);
        ApiError::not_found(format!("{what}: recorded"))
    }
}

/// A [`ControlPlane`] that records the principal of every call.
#[derive(Default)]
struct RecCp(Arc<Rec>);

#[async_trait::async_trait]
impl ControlPlane for RecCp {
    async fn upload_module(&self, _r: UploadModule) -> Result<UploadModuleResult, ApiError> {
        Err(self.0.nf("upload_module"))
    }
    async fn get_module(&self, _s: String) -> Result<ModuleInfo, ApiError> {
        Err(self.0.nf("get_module"))
    }
    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        self.0.note("list_workflows");
        Ok(vec![])
    }
    async fn activate_workflow(&self, _r: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        Err(self.0.nf("activate_workflow"))
    }
    async fn start_run(&self, _r: StartRunRequest) -> Result<StartRunResult, ApiError> {
        Err(self.0.nf("start_run"))
    }
    async fn get_run(&self, _r: String) -> Result<RunInfo, ApiError> {
        Err(self.0.nf("get_run"))
    }
    async fn run_history(&self, _r: HistoryRequest) -> Result<HistoryPage, ApiError> {
        Err(self.0.nf("run_history"))
    }
    async fn list_runs(&self, _r: ListRunsRequest) -> Result<RunPage, ApiError> {
        self.0.note("list_runs");
        Ok(RunPage {
            runs: vec![],
            next_page_token: None,
        })
    }
    fn ready(&self) -> bool {
        self.0.note("ready");
        true
    }
    async fn await_result(&self, _r: AwaitResultRequest) -> Result<AwaitResultResponse, ApiError> {
        // A long-poll producer on its own task: it must still run as the caller.
        let rec = Arc::clone(&self.0);
        authn_layer::spawn_scoped(async move {
            tokio::task::yield_now().await;
            rec.note("await_result.producer");
        })
        .await
        .expect("producer");
        Err(self.0.nf("await_result"))
    }
    async fn join_worker(&self, _r: WorkerJoin) -> Result<WorkerJoinResponse, ApiError> {
        Err(self.0.nf("join_worker"))
    }
    async fn presigned_object(
        &self,
        _r: PresignedObjectRequest,
    ) -> Result<PresignedObjectResponse, ApiError> {
        Err(self.0.nf("presigned_object"))
    }
}

struct RecExt(Arc<Rec>);

#[async_trait::async_trait]
impl HookIngress for RecExt {
    async fn deliver(&self, req: HookRequest) -> Result<HookResponse, ApiError> {
        self.0.note("deliver");
        Ok(HookResponse::json(200, &json!({ "name": req.name })))
    }
}

#[async_trait::async_trait]
impl ClusterApi for RecExt {
    async fn join(&self, _r: NodeJoin) -> Result<NodeJoinResponse, ApiError> {
        Err(self.0.nf("cluster_join"))
    }
}

struct Server {
    addr: SocketAddr,
    http: reqwest::Client,
    authn: Arc<FakeAuthn>,
    rec: Arc<Rec>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Mode {
    Token,
    Dev,
    TrustedNode,
}

impl Server {
    async fn start(mode: Mode) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let rec = Arc::new(Rec::default());
        let authn = Arc::new(FakeAuthn::default());
        let cp: Arc<dyn ControlPlane> = Arc::new(RecCp(Arc::clone(&rec)));
        let cfg = ApiConfig {
            authenticator: match mode {
                Mode::Dev => Arc::new(AllowDev),
                _ => Arc::clone(&authn) as Arc<dyn Authenticator>,
            },
            ..ApiConfig::default()
        };
        let ext = ApiExtensions {
            hooks: Some(Arc::new(RecExt(Arc::clone(&rec)))),
            cluster: Some(Arc::new(RecExt(Arc::clone(&rec)))),
            ..ApiExtensions::default()
        };
        let mut app = ostro_api::router_with(cp, cfg, ext);
        if let Mode::TrustedNode = mode {
            app = app.layer(axum::middleware::from_fn(
                |mut req: axum::extract::Request, next: axum::middleware::Next| {
                    req.extensions_mut().insert(TrustedNode(2));
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
            authn,
            rec,
            task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn authn_calls(&self) -> usize {
        self.authn.calls.load(Ordering::SeqCst)
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> (StatusCode, Option<String>, Value) {
        let r = req.send().await.expect("request");
        let st = r.status();
        let www = r
            .headers()
            .get("www-authenticate")
            .map(|v| v.to_str().unwrap().to_owned());
        let text = r.text().await.expect("body");
        (
            st,
            www,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://{}", self.addr))
            .expect("uri")
            .connect()
            .await
            .expect("h2c connect")
    }
}

fn with_bearer<T>(msg: T, bearer: Option<&str>) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    if let Some(b) = bearer {
        r.metadata_mut()
            .insert("authorization", format!("Bearer {b}").parse().unwrap());
    }
    r
}

#[tokio::test]
async fn rest_rejects_missing_garbage_and_expired_bearers_before_any_handler() {
    let s = Server::start(Mode::Token).await;
    let cases: [(Option<String>, &str); 6] = [
        (None, "missing"),
        (Some("Bearer garbage".into()), "garbage"),
        (Some(format!("Bearer {EXPIRED}")), "expired"),
        (Some(format!("Basic {GOOD}")), "wrong scheme"),
        (Some("Bearer".into()), "empty"),
        (Some(format!("Bearer {GOOD}x")), "altered"),
    ];
    for (header, what) in cases {
        for (method, path) in [
            (reqwest::Method::GET, "/v1/runs"),
            (reqwest::Method::GET, "/v1/workflows"),
            (reqwest::Method::GET, "/v1/queues/q/demand"),
            (reqwest::Method::GET, "/v1/admin/backup"),
            (reqwest::Method::POST, "/v1/runs/r1/cancel"),
            (reqwest::Method::GET, "/openapi.json"),
        ] {
            let mut req = s.http.request(method.clone(), s.url(path));
            if let Some(h) = &header {
                req = req.header("authorization", h);
            }
            let (st, www, body) = s.send(req).await;
            assert_eq!(
                st,
                StatusCode::UNAUTHORIZED,
                "{what}: {method} {path}: {body}"
            );
            assert_eq!(www.as_deref(), Some(WWW_AUTHENTICATE_VALUE), "{what}");
            assert_eq!(body["error"]["code"], "unauthenticated", "{body}");
            assert_eq!(
                body["error"]["details"]["reason"], "unauthenticated",
                "{body}"
            );
            assert!(
                !body.to_string().contains("aaaaaaaaaaaaaaaa_"),
                "the bearer is never echoed: {body}"
            );
        }
    }
    assert_eq!(s.rec.calls(), 0, "no handler ran on a 401");
    // Wrong scheme / empty header are rejected before the authenticator.
    assert!(s.authn_calls() >= 4 * 6);
}

#[tokio::test]
async fn rest_runs_the_handler_inside_the_authenticated_principal() {
    let s = Server::start(Mode::Token).await;
    for scheme in ["Bearer", "bearer", "BEARER"] {
        let (st, _, body) = s
            .send(
                s.http
                    .get(s.url("/v1/runs"))
                    .header("authorization", format!("{scheme} {GOOD}")),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert_eq!(s.rec.last("list_runs"), reader());
    }
    // An error from the handler is the handler's, not the layer's.
    let (st, www, _) = s
        .send(
            s.http
                .get(s.url("/v1/runs/r1"))
                .header("authorization", format!("Bearer {GOOD}")),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(www, None);
    assert_eq!(s.rec.last("get_run"), reader());
}

#[tokio::test]
async fn grpc_rejects_with_unauthenticated_and_runs_as_the_principal() {
    let s = Server::start(Mode::Token).await;
    let mut runs = pb::runs_client::RunsClient::new(s.channel().await);
    for bearer in [None, Some("garbage"), Some(EXPIRED)] {
        let e = runs
            .list(with_bearer(pb::ListRunsRequest::default(), bearer))
            .await
            .expect_err("unauthenticated");
        assert_eq!(e.code(), tonic::Code::Unauthenticated, "{bearer:?}: {e:?}");
    }
    let mut cluster = pb::cluster_client::ClusterClient::new(s.channel().await);
    let e = cluster
        .status(with_bearer(pb::ClusterStatusRequest {}, None))
        .await
        .expect_err("unauthenticated");
    assert_eq!(e.code(), tonic::Code::Unauthenticated);
    assert_eq!(s.rec.calls(), 0, "no handler ran");

    runs.list(with_bearer(pb::ListRunsRequest::default(), Some(GOOD)))
        .await
        .expect("authenticated list");
    assert_eq!(s.rec.last("list_runs"), reader());
    // An unreachable token store is 503 / UNAVAILABLE, not 401 (covered by AuthError's
    // mapping); a handler error passes through.
    let e = runs
        .get(with_bearer(
            pb::GetRunRequest {
                run_id: "r1".into(),
            },
            Some(GOOD),
        ))
        .await
        .expect_err("not found");
    assert_eq!(e.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn unauthenticated_routes_never_call_the_authenticator() {
    let s = Server::start(Mode::Token).await;
    // Health: no scope (Anonymous).
    let (st, _, _) = s.send(s.http.get(s.url("/healthz"))).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = s.send(s.http.get(s.url("/readyz"))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s.rec.last("ready").kind, PrincipalKind::Anonymous);
    let (st, _, _) = s.send(s.http.get(s.url("/"))).await;
    assert_eq!(st, StatusCode::OK);
    // Webhooks run as System (both forms of the path).
    let (st, _, body) = s.send(s.http.post(s.url("/hooks/gh")).body("{}")).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(s.rec.last("deliver").kind, PrincipalKind::System);
    // Join routes run as System.
    let (st, _, _) = s
        .send(s.http.post(s.url("/v1/workers/join")).json(&json!({
            "token": "wkr_x.y", "csr_pem": "pem", "worker_id": "w1"
        })))
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(s.rec.last("join_worker").kind, PrincipalKind::System);
    let node_version = json!({
        "ostro_version": "0.1.0", "level_min": 1, "level_max": 1,
        "wasmtime": "x", "event_codec_ver": 1, "wit_lines": ["0.1"]
    });
    let (st, _, body) = s
        .send(s.http.post(s.url("/v1/cluster/join")).json(&json!({
            "token": "njt_x", "csr_pem": "pem",
            "advertise": {"api": "a:1", "cluster": "a:2"},
            "node_version": node_version,
        })))
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(s.rec.last("cluster_join").kind, PrincipalKind::System);
    // Presigned objects: the signature is the credential (System).
    let (st, _, _) = s
        .send(s.http.get(s.url("/v1/objects/conn/key?sig=x")))
        .await;
    assert_ne!(st, StatusCode::UNAUTHORIZED);
    // The UI tree is not authenticated here (UiAuth's, 14-21); not served by ostro-api.
    let (st, _, _) = s.send(s.http.get(s.url("/ui/runs"))).await;
    assert_ne!(st, StatusCode::UNAUTHORIZED);
    // gRPC: health and the two joins.
    let ch = s.channel().await;
    ostro_proto::health::v1::health_client::HealthClient::new(ch.clone())
        .check(ostro_proto::health::v1::CheckRequest::default())
        .await
        .expect("health");
    let e = pb::cluster_client::ClusterClient::new(ch.clone())
        .join(pb::JoinClusterRequest {
            token: "njt_x".into(),
            csr_pem: "pem".into(),
            advertise: Some(pb::NodeAdvertise {
                api: "a:1".into(),
                cluster: "a:2".into(),
                repl: None,
            }),
            node_version: Some(pb::NodeVersion::default()),
            resume_node_id: None,
        })
        .await
        .expect_err("recorded");
    assert_eq!(e.code(), tonic::Code::NotFound);
    let e = pb::workers_client::WorkersClient::new(ch)
        .join_worker(pb::JoinWorkerRequest {
            token: "wkr_x.y".into(),
            ..Default::default()
        })
        .await
        .expect_err("recorded");
    assert_eq!(e.code(), tonic::Code::NotFound);
    assert_eq!(s.rec.last("join_worker").kind, PrincipalKind::System);
    assert_eq!(s.rec.last("cluster_join").kind, PrincipalKind::System);

    assert_eq!(s.authn_calls(), 0, "the authenticator was never called");
}

#[tokio::test]
async fn allow_dev_runs_everything_as_dev() {
    let s = Server::start(Mode::Dev).await;
    let (st, _, _) = s.send(s.http.get(s.url("/v1/runs"))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s.rec.last("list_runs"), Principal::dev());
    let (st, _, _) = s
        .send(
            s.http
                .get(s.url("/v1/workflows"))
                .header("authorization", "Bearer whatever"),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s.rec.last("list_workflows"), Principal::dev());
    let mut runs = pb::runs_client::RunsClient::new(s.channel().await);
    runs.list(pb::ListRunsRequest::default())
        .await
        .expect("list");
    assert_eq!(s.rec.last("list_runs"), Principal::dev());
    // Hooks stay System even in dev.
    s.send(s.http.post(s.url("/hooks/gh")).body("{}")).await;
    assert_eq!(s.rec.last("deliver").kind, PrincipalKind::System);
    assert_eq!(s.authn_calls(), 0, "AllowDev, not the fake, answered");
}

#[tokio::test]
async fn forwarded_node_calls_are_not_authenticated_here() {
    let s = Server::start(Mode::TrustedNode).await;
    let (st, _, _) = s.send(s.http.get(s.url("/v1/runs"))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s.authn_calls(), 0);
    assert!(
        s.rec.last("list_runs").is_anonymous(),
        "14-15 ingests the principal"
    );
}

#[tokio::test]
async fn long_poll_producers_keep_the_callers_principal() {
    let s = Server::start(Mode::Token).await;
    let (st, _, _) = s
        .send(
            s.http
                .get(s.url("/v1/runs/r1/result?wait=1s"))
                .header("authorization", format!("Bearer {GOOD}")),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(s.rec.last("await_result.producer"), reader());
    // The gRPC twin (spawned producer inside the service future).
    let mut runs = pb::runs_client::RunsClient::new(s.channel().await);
    let e = runs
        .await_result(with_bearer(
            pb::AwaitResultRequest {
                run_id: "r2".into(),
                wait_ms: 10,
                follow_continuations: false,
            },
            Some(GOOD),
        ))
        .await
        .expect_err("recorded");
    assert_eq!(e.code(), tonic::Code::NotFound);
    assert_eq!(s.rec.calls(), 4, "two producers + two calls");
    assert_eq!(s.rec.last("await_result.producer"), reader());
    assert_eq!(s.rec.last("await_result"), reader());
}

#[tokio::test]
async fn the_layer_scopes_ui_and_health_handlers_as_anonymous() {
    // `authn_layer::apply` on a router with UI-like and health routes: no scope is set.
    let seen = Arc::new(Mutex::new(Vec::<(String, Principal)>::new()));
    let record = |name: &'static str, seen: Arc<Mutex<Vec<(String, Principal)>>>| {
        move || {
            let seen = Arc::clone(&seen);
            async move {
                seen.lock().unwrap().push((name.into(), auth::current()));
                "ok"
            }
        }
    };
    let app = axum::Router::new()
        .route(
            "/ui/{*rest}",
            axum::routing::get(record("ui", Arc::clone(&seen))),
        )
        .route(
            "/healthz",
            axum::routing::get(record("healthz", Arc::clone(&seen))),
        )
        .route(
            "/hooks/{*rest}",
            axum::routing::post(record("hooks", Arc::clone(&seen))),
        )
        .route(
            "/v1/runs",
            axum::routing::get(record("runs", Arc::clone(&seen))),
        );
    let authn = Arc::new(FakeAuthn::default());
    let app = authn_layer::apply(app, Arc::clone(&authn) as Arc<dyn Authenticator>);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let http = reqwest::Client::new();
    for (m, p) in [
        (reqwest::Method::GET, "/ui/runs/r1"),
        (reqwest::Method::GET, "/healthz"),
        (reqwest::Method::POST, "/hooks/acme/gh"),
    ] {
        let r = http
            .request(m, format!("http://{addr}{p}"))
            .send()
            .await
            .expect("send");
        assert_eq!(r.status(), StatusCode::OK, "{p}");
    }
    let r = http
        .get(format!("http://{addr}/v1/runs"))
        .bearer_auth(GOOD)
        .send()
        .await
        .expect("send");
    assert_eq!(r.status(), StatusCode::OK);
    task.abort();
    let seen = seen.lock().unwrap().clone();
    let kind = |n: &str| seen.iter().find(|(w, _)| w == n).expect(n).1.kind;
    assert_eq!(kind("ui"), PrincipalKind::Anonymous);
    assert_eq!(kind("healthz"), PrincipalKind::Anonymous);
    assert_eq!(kind("hooks"), PrincipalKind::System);
    assert_eq!(kind("runs"), PrincipalKind::Token);
    assert_eq!(authn.calls.load(Ordering::SeqCst), 1);
}

/// A concrete path for an OpenAPI path template.
fn concrete(template: &str) -> String {
    template
        .split('/')
        .map(|seg| {
            if seg.starts_with('{') {
                "x".to_owned()
            } else {
                seg.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The documented REST operations that are not bearer-authenticated (exact set, W12).
const EXPECTED_UNAUTHENTICATED_REST: &[(&str, &str)] = &[
    ("get", "/"),
    ("get", "/healthz"),
    ("get", "/readyz"),
    ("post", "/hooks/{name}"),
    ("post", "/v1/workers/join"),
    ("post", "/v1/cluster/join"),
    ("get", "/v1/objects/{connection}/{key}"),
    ("put", "/v1/objects/{connection}/{key}"),
];

/// The gRPC methods that are not bearer-authenticated (exact set, W12).
const EXPECTED_UNAUTHENTICATED_GRPC: &[&str] = &[
    "/ostro.api.v1.Cluster/Join",
    "/ostro.api.v1.Workers/JoinWorker",
    "/ostro.health.v1.Health/Check",
];

#[tokio::test]
async fn every_route_and_grpc_method_is_listed_or_authenticated() {
    let s = Server::start(Mode::Token).await;
    let doc = serde_json::to_value(ostro_api::openapi()).expect("doc");
    let mut unauth_rest = BTreeSet::new();
    let mut checked = 0;
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for method in item.as_object().expect("item").keys() {
            if !["get", "put", "post", "delete", "patch"].contains(&method.as_str()) {
                continue;
            }
            let m = Method::from_bytes(method.to_uppercase().as_bytes()).expect("method");
            let url = concrete(path);
            let class = authn_layer::classify(&m, &url);
            let before = s.authn_calls();
            let req = s
                .http
                .request(
                    reqwest::Method::from_bytes(m.as_str().as_bytes()).unwrap(),
                    s.url(&url),
                )
                .header("content-type", "application/json")
                .body("{}");
            let (st, www, body) = s.send(req).await;
            let called = s.authn_calls() - before;
            checked += 1;
            if class == RouteAuth::Authenticate {
                assert_eq!(st, StatusCode::UNAUTHORIZED, "{method} {path}: {body}");
                assert_eq!(www.as_deref(), Some(WWW_AUTHENTICATE_VALUE));
                assert_eq!(called, 1, "{method} {path}");
            } else {
                assert_ne!(st, StatusCode::UNAUTHORIZED, "{method} {path}: {body}");
                assert_eq!(called, 0, "{method} {path}");
                unauth_rest.insert((method.clone(), path.clone()));
            }
        }
    }
    assert!(checked > 100, "{checked} operations");
    let expected: BTreeSet<(String, String)> = EXPECTED_UNAUTHENTICATED_REST
        .iter()
        .map(|(m, p)| ((*m).to_owned(), (*p).to_owned()))
        .collect();
    assert_eq!(
        unauth_rest, expected,
        "documented unauthenticated REST routes"
    );
    // Non-OpenAPI routes on the client listener.
    for (m, p, want) in [
        (Method::GET, "/metrics", RouteAuth::Anonymous),
        (Method::GET, "/ui", RouteAuth::Anonymous),
        (Method::GET, "/ui/runs/r1", RouteAuth::Anonymous),
        (Method::POST, "/ui/login", RouteAuth::Anonymous),
        (Method::POST, "/ui/logout", RouteAuth::Anonymous),
        (
            Method::POST,
            "/grpc.health.v1.Health/Check",
            RouteAuth::Anonymous,
        ),
    ] {
        assert_eq!(authn_layer::classify(&m, p), want, "{m} {p}");
    }

    // Every gRPC method of the descriptor set.
    let mut unauth_grpc = BTreeSet::new();
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .expect("h2 client");
    for method in ostro_proto::GRPC_METHODS {
        let class = authn_layer::classify(&Method::POST, method);
        let before = s.authn_calls();
        let r = client
            .post(s.url(method))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            // An empty message: 0 = uncompressed, length 0.
            .body(vec![0u8, 0, 0, 0, 0])
            .send()
            .await
            .expect("grpc post");
        let status = r
            .headers()
            .get("grpc-status")
            .map(|v| v.to_str().unwrap().to_owned());
        let called = s.authn_calls() - before;
        if class == RouteAuth::Authenticate {
            assert_eq!(status.as_deref(), Some("16"), "{method}: UNAUTHENTICATED");
            assert_eq!(called, 1, "{method}");
        } else {
            assert_ne!(status.as_deref(), Some("16"), "{method}");
            assert_eq!(called, 0, "{method}");
            unauth_grpc.insert((*method).to_owned());
        }
    }
    let expected: BTreeSet<String> = EXPECTED_UNAUTHENTICATED_GRPC
        .iter()
        .map(|m| (*m).to_owned())
        .collect();
    assert_eq!(unauth_grpc, expected, "unauthenticated gRPC methods");
    // Every list entry is used by a route or method above (no stale entries) or is a
    // documented non-OpenAPI route.
    for e in UNAUTHENTICATED {
        assert!(
            ["/metrics", "/ui", "/ui/*", "/grpc.health.v1.Health/*"].contains(&e.pattern)
                || unauth_rest.iter().any(|(_, p)| authn_layer::classify(
                    &Method::POST,
                    &concrete(p)
                ) == e.auth
                    || authn_layer::classify(&Method::GET, &concrete(p)) == e.auth)
                || unauth_grpc
                    .iter()
                    .any(|m| m.starts_with(e.pattern.trim_end_matches('*'))),
            "stale entry {e:?}"
        );
    }
}
