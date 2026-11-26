//! Phase 14 surface (14-02) over a real listener: every D-P14-29 REST route against the
//! `FakeP14` fakes, 404 (REST) / `UNIMPLEMENTED` (gRPC) while a slot is absent, `no-store`
//! on bearer answers, the `Ostro-Module-Signature` carriage and its limits (REST header
//! and gRPC field, rejected before the body / control plane), `GET /v1/runs?tenant=`, and
//! `POST /hooks/{tenant}/{name}` with D23 validation.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use base64::Engine;
use ostro_api::fake::{FakeControlPlane, FakeP14};
use ostro_api::phase8::{HookIngress, HookRequest, HookResponse};
use ostro_api::phase14::AuditRecordView;
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane, ErrorCode};
use ostro_proto::api::v1 as pb;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::Digest;
use tonic::transport::Channel;

const SHA: &str = "abababababababababababababababababababababababababababababababab";

fn b64(b: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(b)
}

#[derive(Default)]
struct Hooks(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl HookIngress for Hooks {
    async fn deliver(&self, req: HookRequest) -> Result<HookResponse, ApiError> {
        self.0.lock().unwrap().push(req.name.clone());
        Ok(HookResponse::json(200, &json!({ "name": req.name })))
    }
}

struct Server {
    addr: SocketAddr,
    http: reqwest::Client,
    cp: Arc<FakeControlPlane>,
    hooks: Arc<Hooks>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(p14: Option<&FakeP14>) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let cp = Arc::new(FakeControlPlane::new());
        let hooks = Arc::new(Hooks::default());
        let mut ext = ApiExtensions {
            hooks: Some(Arc::clone(&hooks) as Arc<dyn HookIngress>),
            ..ApiExtensions::default()
        };
        if let Some(f) = p14 {
            f.install(&mut ext);
        }
        let dyn_cp: Arc<dyn ControlPlane> = cp.clone();
        let app = ostro_api::router_with(dyn_cp, ApiConfig::default(), ext);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        Server {
            addr,
            http: reqwest::Client::builder()
                .http1_only()
                .build()
                .expect("client"),
            cp,
            hooks,
            task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn send(
        &self,
        req: reqwest::RequestBuilder,
    ) -> (StatusCode, reqwest::header::HeaderMap, Value) {
        let r = req.send().await.expect("request");
        let st = r.status();
        let h = r.headers().clone();
        let text = r.text().await.expect("body");
        (
            st,
            h,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn call(&self, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        let m = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        let mut req = self.http.request(m, self.url(path));
        if let Some(b) = body {
            req = req.json(&b);
        }
        let (st, _, v) = self.send(req).await;
        (st, v)
    }

    async fn channel(&self) -> Channel {
        Channel::from_shared(format!("http://{}", self.addr))
            .expect("uri")
            .connect()
            .await
            .expect("h2c connect")
    }
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or_default()
}

#[tokio::test]
async fn absent_slots_answer_404_and_unimplemented() {
    let s = Server::start(None).await;
    for (m, p, body) in [
        ("GET", "/v1/tenants", None),
        ("POST", "/v1/tenants", Some(json!({"id": "acme"}))),
        ("GET", "/v1/tenants/acme", None),
        ("PATCH", "/v1/tenants/acme", Some(json!({}))),
        ("DELETE", "/v1/tenants/acme", None),
        ("POST", "/v1/tenants/acme/suspend", None),
        ("POST", "/v1/tenants/acme/resume", None),
        ("GET", "/v1/auth/tokens", None),
        (
            "POST",
            "/v1/auth/tokens",
            Some(json!({"name": "a", "scopes": ["read"]})),
        ),
        ("DELETE", "/v1/auth/tokens/otk_x", None),
        ("POST", "/v1/auth/tokens/otk_x/rotate", None),
        ("GET", "/v1/auth/whoami", None),
        ("GET", "/v1/audit", None),
        ("GET", "/v1/security/modules/policy", None),
        (
            "PUT",
            "/v1/security/modules/policy",
            Some(json!({"require_signed_modules": true})),
        ),
        (
            "POST",
            "/v1/security/modules/keys",
            Some(json!({"alg": "minisign", "key_id": "k", "public_key": "p"})),
        ),
        ("DELETE", "/v1/security/modules/keys/k", None),
        (
            "POST",
            &format!("/v1/modules/{SHA}/signatures"),
            Some(json!({"signatures": [b64(b"s")]})),
        ),
    ] {
        let (st, v) = s.call(m, p, body).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "{m} {p}: {v}");
        assert_eq!(v["error"]["code"], "not_found", "{m} {p}");
    }
    let ch = s.channel().await;
    let e = pb::tenants_client::TenantsClient::new(ch.clone())
        .list(pb::ListTenantsRequest {})
        .await
        .expect_err("unmounted");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    let e = pb::api_tokens_client::ApiTokensClient::new(ch.clone())
        .who_am_i(pb::WhoAmIRequest {})
        .await
        .expect_err("unmounted");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    let e = pb::audit_client::AuditClient::new(ch.clone())
        .query(pb::AuditQueryRequest::default())
        .await
        .expect_err("unmounted");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    let e = pb::security_client::SecurityClient::new(ch)
        .get_module_policy(pb::GetModulePolicyRequest {})
        .await
        .expect_err("unmounted");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn tenant_routes() {
    let f = FakeP14::new();
    let s = Server::start(Some(&f)).await;
    let (st, v) = s
        .call(
            "POST",
            "/v1/tenants",
            Some(json!({"id": "acme", "weight": 2})),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["id"], "acme");
    assert_eq!(v["weight"], 2);
    assert_eq!(v["suspended"], false);
    let (st, v) = s
        .call("POST", "/v1/tenants", Some(json!({"id": "acme"})))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    for bad in ["Acme", "_", "a/b", "-x"] {
        let (st, _) = s
            .call("POST", "/v1/tenants", Some(json!({"id": bad})))
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}");
    }
    let (st, _) = s.call("GET", "/v1/tenants/BAD", None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, _) = s.call("GET", "/v1/tenants/nope", None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, v) = s
        .call(
            "PATCH",
            "/v1/tenants/acme",
            Some(json!({"max_concurrency": 5})),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["max_concurrency"], 5);
    assert_eq!(v["weight"], 2);
    let (st, v) = s.call("GET", "/v1/tenants", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["tenants"].as_array().unwrap().len(), 1);
    let (st, v) = s.call("DELETE", "/v1/tenants/acme", None).await;
    assert_eq!(st, StatusCode::CONFLICT, "not suspended: {v}");
    assert_eq!(reason(&v), "tenant_not_empty");
    let (st, v) = s.call("POST", "/v1/tenants/acme/suspend", None).await;
    assert_eq!((st, v["suspended"].clone()), (StatusCode::OK, json!(true)));
    let (st, v) = s.call("POST", "/v1/tenants/acme/resume", None).await;
    assert_eq!((st, v["suspended"].clone()), (StatusCode::OK, json!(false)));
    s.call("POST", "/v1/tenants/acme/suspend", None).await;
    let (st, v) = s.call("DELETE", "/v1/tenants/acme", None).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({"id": "acme", "deleted": true}));
    let (_, v) = s.call("GET", "/v1/tenants", None).await;
    assert_eq!(v["tenants"], json!([]));
}

#[tokio::test]
async fn token_routes_show_the_bearer_once_with_no_store() {
    let f = FakeP14::new();
    let s = Server::start(Some(&f)).await;
    let (st, h, v) = s
        .send(s.http.post(s.url("/v1/auth/tokens")).json(&json!({
            "name": "ci", "scopes": ["deploy", "read"], "tenant": "acme"
        })))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(h["cache-control"], "no-store");
    let id = v["info"]["id"].as_str().unwrap().to_owned();
    assert!(id.starts_with("otk_"), "{id}");
    assert!(v["bearer"].as_str().unwrap().starts_with("ostro_otk_"));
    assert_eq!(v["info"]["created_by"], "dev", "AllowDev principal");
    // Listing never carries a bearer or a hash.
    let (st, v) = s.call("GET", "/v1/auth/tokens", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["tokens"].as_array().unwrap().len(), 1);
    let text = v.to_string();
    assert!(
        !text.contains("bearer") && !text.contains("ostro_otk_") && !text.contains("hash"),
        "{text}"
    );
    let (_, v) = s.call("GET", "/v1/auth/tokens?tenant=other", None).await;
    assert_eq!(v["tokens"], json!([]));
    let (st, _) = s.call("GET", "/v1/auth/tokens?tenant=BAD", None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    // Rotate (empty body = defaults), no-store again.
    let (st, h, v) = s
        .send(s.http.post(s.url(&format!("/v1/auth/tokens/{id}/rotate"))))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(h["cache-control"], "no-store");
    assert_eq!(v["info"]["rotated_from"], id.as_str());
    let (st, v) = s
        .call(
            "POST",
            &format!("/v1/auth/tokens/{id}/rotate"),
            Some(json!({"revoke_old": true})),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let (st, v) = s
        .call("DELETE", &format!("/v1/auth/tokens/{id}"), None)
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v, json!({"id": id, "revoked": true}));
    let (st, _) = s.call("DELETE", "/v1/auth/tokens/otk_unknown", None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, v) = s.call("GET", "/v1/auth/whoami", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["kind"], "dev");
    assert_eq!(v["scopes"], json!(["admin"]));
}

#[tokio::test]
async fn audit_and_security_routes() {
    let f = FakeP14::new();
    for (i, (tenant, action)) in [
        (None, "deploy"),
        (Some("acme"), "token.create"),
        (Some("acme"), "token.revoke"),
    ]
    .into_iter()
    .enumerate()
    {
        f.audit.push(AuditRecordView {
            ts_ms: 1000 + i as u64,
            actor: "dev".into(),
            tenant: tenant.map(str::to_owned),
            action: action.into(),
            outcome: "ok".into(),
            ..AuditRecordView::default()
        });
    }
    let s = Server::start(Some(&f)).await;
    let (st, v) = s
        .call("GET", "/v1/audit?tenant=acme&action=token.*", None)
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["records"].as_array().unwrap().len(), 2);
    let (_, v) = s.call("GET", "/v1/audit?since=1001&until=1002", None).await;
    assert_eq!(v["records"].as_array().unwrap().len(), 1);
    let (_, v) = s
        .call("GET", "/v1/audit?since=1970-01-01T00:00:01.002Z", None)
        .await;
    assert_eq!(v["records"].as_array().unwrap().len(), 1, "{v}");
    let (_, v) = s.call("GET", "/v1/audit?limit=1", None).await;
    assert_eq!(v["cursor"], "1");
    let (_, v) = s.call("GET", "/v1/audit?limit=1&cursor=1", None).await;
    assert_eq!(v["records"][0]["action"], "token.create");
    let (st, _) = s.call("GET", "/v1/audit?since=yesterday", None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    f.audit.fail(
        "query",
        Some(
            ApiError::unavailable("journal down")
                .with_details(json!({"reason": "audit_unavailable"})),
        ),
    );
    let (st, v) = s.call("GET", "/v1/audit", None).await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(reason(&v), "audit_unavailable");

    let (st, v) = s.call("GET", "/v1/security/modules/policy", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["require_signed_modules"], false);
    let (st, v) = s
        .call(
            "PUT",
            "/v1/security/modules/policy",
            Some(json!({"require_signed_modules": true})),
        )
        .await;
    assert_eq!(
        (st, v["require_signed_modules"].clone()),
        (StatusCode::OK, json!(true))
    );
    let key = json!({"alg": "minisign", "key_id": "k1", "public_key": "RWQ"});
    let (st, v) = s
        .call("POST", "/v1/security/modules/keys", Some(key.clone()))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["trusted_keys"][0]["key_id"], "k1");
    let (st, _) = s.call("POST", "/v1/security/modules/keys", Some(key)).await;
    assert_eq!(st, StatusCode::CONFLICT);
    let (st, _) = s
        .call(
            "POST",
            "/v1/security/modules/keys",
            Some(json!({"alg": "minisign", "key_id": "k2", "public_key": "p", "tenant": "Bad"})),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, v) = s.call("DELETE", "/v1/security/modules/keys/k1", None).await;
    assert_eq!((st, v["trusted_keys"].clone()), (StatusCode::OK, json!([])));
    let (st, _) = s.call("DELETE", "/v1/security/modules/keys/k1", None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Attach signatures: sha syntax, unknown module, decoding and limits.
    f.security.add_module(SHA);
    let path = format!("/v1/modules/{SHA}/signatures");
    let (st, v) = s
        .call(
            "POST",
            &path,
            Some(json!({"signatures": [b64(b"sig-a"), b64(b"sig-bb")]})),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["key_ids"], json!(["fake-5", "fake-6"]));
    let (st, v) = s
        .call(
            "POST",
            "/v1/modules/XYZ/signatures",
            Some(json!({"signatures": [b64(b"s")]})),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let other = "cd".repeat(32);
    let (st, _) = s
        .call(
            "POST",
            &format!("/v1/modules/{other}/signatures"),
            Some(json!({"signatures": [b64(b"s")]})),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    for bad in [
        json!({"signatures": []}),
        json!({"signatures": ["!!not base64!!"]}),
        json!({"signatures": [b64(&[1; 8193])]}),
        json!({"signatures": [b64(b"1"), b64(b"2"), b64(b"3"), b64(b"4"), b64(b"5")]}),
    ] {
        let (st, v) = s.call("POST", &path, Some(bad.clone())).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
        assert_eq!(reason(&v), "signature_invalid", "{bad}");
    }
    let (st, _) = s
        .call(
            "POST",
            &path,
            Some(json!({"signatures": [b64(&[1; 8192])]})),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "8 KiB is the limit, inclusive");
}

fn module() -> (Vec<u8>, String) {
    let wasm = b"\0asm\x01\0\0\0p14-sig".to_vec();
    let sha = hex(&sha2::Sha256::digest(&wasm));
    (wasm, sha)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[tokio::test]
async fn upload_signature_headers_reach_the_control_plane_within_limits() {
    let s = Server::start(None).await;
    let (wasm, sha) = module();
    let upload = || {
        s.http
            .put(s.url(&format!("/v1/modules/{sha}")))
            .header("content-type", "application/wasm")
            .body(wasm.clone())
    };
    let (st, _, v) = s
        .send(
            upload()
                .header("Ostro-Module-Signature", b64(b"minisig-one"))
                .header("ostro-module-signature", b64(b"cosign-two")),
        )
        .await;
    assert!(st.is_success(), "{st}: {v}");
    assert_eq!(
        s.cp.last_upload_signatures(),
        vec![b"minisig-one".to_vec(), b"cosign-two".to_vec()]
    );
    // Unsigned upload: empty.
    let (st, _, _) = s.send(upload()).await;
    assert!(st.is_success());
    assert!(s.cp.last_upload_signatures().is_empty());
    // Limits: answered before the body reaches the control plane.
    let before = s.cp.upload_naming(&sha).len();
    let mut five = upload();
    for i in 0..5 {
        five = five.header("Ostro-Module-Signature", b64(format!("s{i}").as_bytes()));
    }
    for (what, req) in [
        ("five headers", five),
        (
            "too large",
            upload().header("Ostro-Module-Signature", b64(&[7; 8193])),
        ),
        (
            "not base64",
            upload().header("Ostro-Module-Signature", "%%%"),
        ),
        ("empty", upload().header("Ostro-Module-Signature", "")),
    ] {
        let (st, _, v) = s.send(req).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{what}: {v}");
        assert_eq!(reason(&v), "signature_invalid", "{what}");
    }
    assert_eq!(
        s.cp.upload_naming(&sha).len(),
        before,
        "no upload reached the fake"
    );
    // Exactly at the limits: accepted.
    let mut four = upload();
    for _ in 0..4 {
        four = four.header("Ostro-Module-Signature", b64(&[9; 8192]));
    }
    let (st, _, v) = s.send(four).await;
    assert!(st.is_success(), "{v}");
    assert_eq!(s.cp.last_upload_signatures().len(), 4);

    // gRPC: the same carriage and limits.
    let mut modules = pb::modules_client::ModulesClient::new(s.channel().await);
    let req = |sigs: Vec<Vec<u8>>| pb::UploadModuleRequest {
        sha256: sha.clone(),
        wasm: wasm.clone(),
        language: None,
        tenant: None,
        name: None,
        signatures: sigs,
    };
    modules
        .upload(req(vec![b"g1".to_vec()]))
        .await
        .expect("grpc upload");
    assert_eq!(s.cp.last_upload_signatures(), vec![b"g1".to_vec()]);
    for sigs in [vec![vec![1u8]; 5], vec![vec![1u8; 8193]], vec![vec![]]] {
        let e = modules.upload(req(sigs)).await.expect_err("limit");
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        let d = ostro_api::grpc::status_details(&e).expect("details");
        assert_eq!(d["reason"], "signature_invalid");
    }
}

#[tokio::test]
async fn run_list_tenant_filter() {
    let s = Server::start(None).await;
    for (q, want) in [
        ("/v1/runs?tenant=acme", Some("acme")),
        ("/v1/runs?tenant=_", Some("_")),
        ("/v1/runs?tenant=", None),
        ("/v1/runs", None),
    ] {
        let (st, v) = s.call("GET", q, None).await;
        assert_eq!(st, StatusCode::OK, "{q}: {v}");
        assert_eq!(
            s.cp.last_list_runs().expect("listed").tenant.as_deref(),
            want,
            "{q}"
        );
    }
    let (st, _) = s.call("GET", "/v1/runs?tenant=Bad", None).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let mut runs = pb::runs_client::RunsClient::new(s.channel().await);
    runs.list(pb::ListRunsRequest {
        tenant: Some("acme".into()),
        ..Default::default()
    })
    .await
    .expect("list");
    assert_eq!(
        s.cp.last_list_runs().unwrap().tenant.as_deref(),
        Some("acme")
    );
    let e = runs
        .list(pb::ListRunsRequest {
            tenant: Some("A/B".into()),
            ..Default::default()
        })
        .await
        .expect_err("invalid");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn tenant_hooks_deliver_to_the_qualified_name() {
    let s = Server::start(None).await;
    let (st, _, v) = s
        .send(s.http.post(s.url("/hooks/acme/gh-push")).body("{}"))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["name"], "acme/gh-push");
    let (st, _, _) = s.send(s.http.post(s.url("/hooks/plain")).body("{}")).await;
    assert_eq!(st, StatusCode::OK);
    for bad in [
        "/hooks/Acme/gh",
        "/hooks/_/gh",
        "/hooks/acme/__x",
        "/hooks/acme/.x",
        "/hooks/acme%2Fx/gh",
        "/hooks/acme/a%2Fb",
        "/hooks/a%2Fb",
    ] {
        let (st, _, v) = s.send(s.http.post(s.url(bad)).body("{}")).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
    }
    assert_eq!(
        *s.hooks.0.lock().unwrap(),
        vec!["acme/gh-push".to_owned(), "plain".to_owned()]
    );
    // Same body limit as /hooks/{name}.
    let big = vec![b'x'; ostro_api::phase8::DEFAULT_HOOK_BODY_LIMIT + 1];
    let (st, _, _) = s.send(s.http.post(s.url("/hooks/acme/gh")).body(big)).await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn grpc_services_mirror_rest_validation() {
    let f = FakeP14::new();
    let s = Server::start(Some(&f)).await;
    let ch = s.channel().await;
    let mut tenants = pb::tenants_client::TenantsClient::new(ch.clone());
    let t = tenants
        .create(pb::CreateTenantRequest {
            spec: Some(pb::TenantSpec {
                id: "acme".into(),
                ..Default::default()
            }),
        })
        .await
        .expect("create")
        .into_inner();
    assert_eq!(t.spec.unwrap().id, "acme");
    let e = tenants
        .create(pb::CreateTenantRequest { spec: None })
        .await
        .expect_err("spec required");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let e = tenants
        .get(pb::GetTenantRequest { id: "Bad".into() })
        .await
        .expect_err("bad id");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    let e = tenants
        .delete(pb::DeleteTenantRequest { id: "acme".into() })
        .await
        .expect_err("not suspended");
    assert_eq!(e.code(), tonic::Code::AlreadyExists);
    let mut tokens = pb::api_tokens_client::ApiTokensClient::new(ch.clone());
    let c = tokens
        .create(pb::CreateTokenRequest {
            name: "x".into(),
            scopes: vec!["read".into()],
            ..Default::default()
        })
        .await
        .expect("create")
        .into_inner();
    assert!(c.bearer.starts_with("ostro_otk_"));
    assert!(
        !format!("{c:?}").contains(&c.bearer),
        "bearer redacted in Debug"
    );
    let me = tokens
        .who_am_i(pb::WhoAmIRequest {})
        .await
        .expect("whoami")
        .into_inner();
    assert_eq!(me.kind, "dev");
    let mut sec = pb::security_client::SecurityClient::new(ch);
    let e = sec
        .attach_signatures(pb::AttachSignaturesRequest {
            sha256: SHA.into(),
            signatures: vec![vec![1; 8193]],
        })
        .await
        .expect_err("too large");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    f.security.fail(
        "get_module_policy",
        Some(ApiError::permission_denied("missing_scope", "needs admin")),
    );
    let e = sec
        .get_module_policy(pb::GetModulePolicyRequest {})
        .await
        .expect_err("denied");
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        ErrorCode::from_grpc_code(e.code()),
        ErrorCode::PermissionDenied
    );
}
