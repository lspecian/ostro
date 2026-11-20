//! Phase 6 bridge admin surface (06-08) against `FakeControlPlane` over a real
//! listener: secrets (write-only), grants, connections, egress policies, the
//! unauthenticated presign route, the gRPC `Secrets`/`Connectors` services, and the
//! `ControlPlane` defaults (501 / UNIMPLEMENTED).
//!
//! ostro-api has no API authentication layer yet, so the "admin routes answer 401
//! without a token" half of the presign check cannot run here. The presign route is
//! mounted outside the REST middleware stack (`router_with`), so a future auth layer on
//! that stack cannot reach it; this file proves it answers without any credentials.

use std::sync::Arc;

use ostro_api::domain::{self, PresignMethod};
use ostro_api::fake::{FAKE_OBJECT_BODY, FAKE_SECRET_KEY_ID, FakeControlPlane};
use ostro_api::{ApiConfig, ApiError, ControlPlane, ErrorCode};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{connectors_client::ConnectorsClient, secrets_client::SecretsClient};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tonic::transport::Channel;

struct Server {
    addr: std::net::SocketAddr,
    cp: Arc<FakeControlPlane>,
    http: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_cp(
    cp: Arc<dyn ControlPlane>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = ostro_api::router(cp, ApiConfig::default());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, task)
}

impl Server {
    async fn start() -> Server {
        let cp = Arc::new(FakeControlPlane::new());
        let (addr, task) = serve_cp(cp.clone()).await;
        Server {
            addr,
            cp,
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

    async fn send(&self, req: reqwest::RequestBuilder) -> (StatusCode, String) {
        let resp = req.send().await.expect("request");
        let status = resp.status();
        (status, resp.text().await.expect("body"))
    }

    async fn json(&self, req: reqwest::RequestBuilder) -> (StatusCode, Value) {
        let (status, text) = self.send(req).await;
        let v = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        (status, v)
    }

    async fn grpc(&self) -> (SecretsClient<Channel>, ConnectorsClient<Channel>) {
        let ch = Channel::from_shared(format!("http://{}", self.addr))
            .expect("uri")
            .connect()
            .await
            .expect("h2c connect");
        (SecretsClient::new(ch.clone()), ConnectorsClient::new(ch))
    }
}

fn reason(v: &Value) -> &Value {
    &v["error"]["details"]["reason"]
}

#[tokio::test]
async fn secrets_rest_round_trip_and_validation() {
    let s = Server::start().await;
    let (st, v) = s
        .json(
            s.http
                .put(s.url("/v1/secrets/stripe/live-key"))
                .json(&json!({ "value": "sk_live_0123456789" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(
        v,
        json!({ "name": "stripe/live-key", "key_id": FAKE_SECRET_KEY_ID, "tenant": null })
    );
    assert_eq!(
        s.cp.secret_value("stripe/live-key", None).as_deref(),
        Some(&b"sk_live_0123456789"[..])
    );

    // value_b64 of the bytes 00 01 .. 09 (not UTF-8 text), tenant-scoped.
    let (st, v) = s
        .json(
            s.http
                .put(s.url("/v1/secrets/db/pass?tenant=acme"))
                .json(&json!({ "value_b64": "AAECAwQFBgcICQ==" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["tenant"], "acme");
    assert_eq!(
        s.cp.secret_value("db/pass", Some("acme")),
        Some((0u8..10).collect::<Vec<_>>())
    );
    // Unpadded base64 is accepted too.
    let (st, _) = s
        .json(
            s.http
                .put(s.url("/v1/secrets/unpadded"))
                .json(&json!({ "value_b64": "AAECAwQFBgcICQ" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    for (name, body, want_reason) in [
        ("k", json!({ "value": "1234567" }), Some("secret_too_short")),
        (
            "k",
            json!({ "value_b64": "MTIzNDU2Nw==" }),
            Some("secret_too_short"),
        ),
        (
            "__ostro/token-key",
            json!({ "value": "0123456789" }),
            Some("secret_reserved"),
        ),
        ("Upper", json!({ "value": "0123456789" }), None),
        // Percent-encoded so the client does not normalise the dot segment away.
        ("a%2F..%2Fb", json!({ "value": "0123456789" }), None),
        ("k", json!({}), None),
        (
            "k",
            json!({ "value": "0123456789", "value_b64": "AAAA" }),
            None,
        ),
        ("k", json!({ "value_b64": "not base64!" }), None),
        ("k", json!({ "value_b64": "AAE" }), None),
        ("k", json!({ "value": "0123456789", "extra": 1 }), None),
    ] {
        let (st, v) = s
            .json(
                s.http
                    .put(s.url(&format!("/v1/secrets/{name}")))
                    .json(&body),
            )
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{name} {body}: {v}");
        assert_eq!(v["error"]["code"], "invalid_argument");
        if let Some(r) = want_reason {
            assert_eq!(reason(&v), r, "{v}");
        }
        assert!(
            !v.to_string().contains("1234567"),
            "error echoes the value: {v}"
        );
    }

    let (st, v) = s.json(s.http.get(s.url("/v1/secrets"))).await;
    assert_eq!(st, StatusCode::OK);
    let names: Vec<&str> = v["secrets"]
        .as_array()
        .expect("secrets")
        .iter()
        .map(|x| x["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, ["stripe/live-key", "unpadded"]);
    let (_, v) = s.json(s.http.get(s.url("/v1/secrets?tenant=acme"))).await;
    assert_eq!(v["secrets"][0]["name"], "db/pass");

    let (st, text) = s
        .send(s.http.delete(s.url("/v1/secrets/stripe/live-key")))
        .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    assert!(text.is_empty());
    let (st, v) = s
        .json(s.http.delete(s.url("/v1/secrets/stripe/live-key")))
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    // Tenant scoping: the global name does not delete the tenant secret.
    let (st, _) = s.json(s.http.delete(s.url("/v1/secrets/db/pass"))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = s
        .json(s.http.delete(s.url("/v1/secrets/db/pass?tenant=acme")))
        .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn grants_connections_egress_rest() {
    let s = Server::start().await;
    // Grants: id computed server-side.
    let (st, g) = s
        .json(s.http.put(s.url("/v1/grants")).json(&json!({
            "module": "acme/billing",
            "secret": "stripe/live",
            "origins": ["https://api.stripe.com", "https://*.example.com:8443"],
            "body": true,
        })))
        .await;
    assert_eq!(st, StatusCode::OK, "{g}");
    assert_eq!(g["id"], "secret:acme/billing:stripe/live");
    let (st, v) = s
        .json(s.http.put(s.url("/v1/grants")).json(&json!({
            "module": "acme/other", "secret": "k", "origins": ["*"],
        })))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = s
        .json(s.http.put(s.url("/v1/grants")).json(&json!({
            "module": "acme/other", "secret": "k", "origins": ["https://x.com"],
        })))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["body"], false);
    let (_, v) = s
        .json(s.http.get(s.url("/v1/grants?module=acme/billing")))
        .await;
    assert_eq!(v["grants"].as_array().map(Vec::len), Some(1));
    assert_eq!(v["grants"][0], g);
    let (_, v) = s.json(s.http.get(s.url("/v1/grants"))).await;
    assert_eq!(v["grants"].as_array().map(Vec::len), Some(2));
    let (st, _) = s
        .send(
            s.http
                .delete(s.url("/v1/grants/secret:acme/billing:stripe/live")),
        )
        .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (st, _) = s
        .send(
            s.http
                .delete(s.url("/v1/grants/secret:acme/billing:stripe/live")),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Connections: the name is percent-decoded (http-target names are origins).
    let (st, c) = s
        .json(
            s.http
                .put(s.url("/v1/connections/https%3A%2F%2Fapi.example.com"))
                .json(&json!({ "kind": "http-target", "config": { "v": 1, "timeout_ms": 500 } })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{c}");
    assert_eq!(c["name"], "https://api.example.com");
    let (st, got) = s
        .json(
            s.http
                .get(s.url("/v1/connections/https%3A%2F%2Fapi.example.com")),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, c);
    let (st, _) = s
        .json(
            s.http
                .put(s.url("/v1/connections/db"))
                .json(&json!({ "kind": "sql-postgres", "config": { "v": 1 } })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    for body in [
        json!({ "kind": "redis", "config": { "v": 1 } }),
        json!({ "kind": "object", "config": { "v": 2 } }),
        json!({ "kind": "object", "config": "nope" }),
        json!({ "kind": "object" }),
    ] {
        let (st, v) = s
            .json(s.http.put(s.url("/v1/connections/bad")).json(&body))
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{body}: {v}");
    }
    let (_, v) = s.json(s.http.get(s.url("/v1/connections"))).await;
    let names: Vec<&str> = v["connections"]
        .as_array()
        .expect("connections")
        .iter()
        .map(|c| c["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, ["db", "https://api.example.com"]);
    let (st, _) = s.send(s.http.delete(s.url("/v1/connections/db"))).await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (st, _) = s.json(s.http.get(s.url("/v1/connections/db"))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Egress: the module name contains `/`.
    let (st, e) = s
        .json(
            s.http
                .put(s.url("/v1/egress/acme/billing"))
                .json(&json!({ "allow": ["https://api.stripe.com", "connection:db"] })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{e}");
    assert_eq!(e["module"], "acme/billing");
    let (st, got) = s.json(s.http.get(s.url("/v1/egress/acme/billing"))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, e);
    let (st, v) = s
        .json(
            s.http
                .put(s.url("/v1/egress/acme/billing"))
                .json(&json!({ "allow": ["*"] })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (_, v) = s.json(s.http.get(s.url("/v1/egress"))).await;
    assert_eq!(v["policies"], json!([e]));
    let (st, _) = s
        .send(s.http.delete(s.url("/v1/egress/acme/billing")))
        .await;
    assert_eq!(st, StatusCode::NO_CONTENT);
    let (st, _) = s.json(s.http.get(s.url("/v1/egress/acme/billing"))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn presign_route_needs_no_credentials() {
    let s = Server::start().await;
    // No Authorization header anywhere in this test.
    let resp = s
        .http
        .get(s.url("/v1/objects/files/a/b/c.txt?exp=1700000000&sig=valid"))
        .send()
        .await
        .expect("get");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()["content-type"].to_str().expect("ascii"),
        "application/octet-stream"
    );
    assert_eq!(&resp.bytes().await.expect("body")[..], FAKE_OBJECT_BODY);
    let last =
        s.cp.last_presigned_object()
            .expect("reached the control plane");
    assert_eq!(
        (
            last.connection.as_str(),
            last.key.as_str(),
            last.method,
            last.exp
        ),
        ("files", "a/b/c.txt", PresignMethod::Get, 1_700_000_000)
    );
    assert_eq!(last.body, None);

    let (st, _) = s
        .send(s.http.get(s.url("/v1/objects/files/a?exp=1&sig=deadbeef")))
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    for q in [
        "",
        "?exp=1",
        "?sig=valid",
        "?exp=1&sig=",
        "?exp=soon&sig=valid",
    ] {
        let (st, v) = s
            .json(s.http.get(s.url(&format!("/v1/objects/files/a{q}"))))
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{q}: {v}");
        assert_eq!(v["error"]["code"], "invalid_argument");
    }

    let (st, _) = s
        .send(
            s.http
                .put(s.url("/v1/objects/files/up/load.bin?exp=9&sig=valid"))
                .header("content-type", "image/png")
                .body(b"\x89PNG-bytes".to_vec()),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let last = s.cp.last_presigned_object().expect("put reached");
    assert_eq!(last.method, PresignMethod::Put);
    assert_eq!(last.key, "up/load.bin");
    assert_eq!(last.body.as_deref(), Some(&b"\x89PNG-bytes"[..]));
    assert_eq!(last.content_type.as_deref(), Some("image/png"));
    let (st, _) = s
        .send(
            s.http
                .put(s.url("/v1/objects/files/x?exp=9&sig=bad"))
                .body("x"),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn grpc_secrets_and_connectors() {
    let s = Server::start().await;
    let (mut sec, mut con) = s.grpc().await;
    let info = sec
        .put_secret(pb::PutSecretRequest {
            name: "k/one".into(),
            value: b"0123456789".to_vec(),
            tenant: None,
        })
        .await
        .expect("put")
        .into_inner();
    assert_eq!(info.key_id, FAKE_SECRET_KEY_ID);
    let e = sec
        .put_secret(pb::PutSecretRequest {
            name: "k".into(),
            value: b"short".to_vec(),
            tenant: None,
        })
        .await
        .expect_err("short");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        ostro_api::grpc::status_details(&e).expect("details")["reason"],
        "secret_too_short"
    );
    let g = sec
        .put_grant(pb::PutGrantRequest {
            grant: Some(pb::GrantInfo {
                module: "t/m".into(),
                secret: "k/one".into(),
                origins: vec!["https://x.com".into()],
                ..Default::default()
            }),
        })
        .await
        .expect("grant")
        .into_inner();
    assert_eq!(g.id, "secret:t/m:k/one");
    let e = sec
        .put_grant(pb::PutGrantRequest { grant: None })
        .await
        .expect_err("missing");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    sec.delete_grant(pb::DeleteGrantRequest { id: g.id })
        .await
        .expect("delete grant");

    let c = con
        .put_connection(pb::PutConnectionRequest {
            connection: Some(pb::ConnectionInfo {
                name: "files".into(),
                kind: "object".into(),
                config_json: br#"{"v":1,"bucket":"b"}"#.to_vec(),
            }),
        })
        .await
        .expect("conn")
        .into_inner();
    assert_eq!(
        serde_json::from_slice::<Value>(&c.config_json).expect("json"),
        json!({ "v": 1, "bucket": "b" })
    );
    for bad in [&b"{nope"[..], b"", b"{\"v\":3}"] {
        let e = con
            .put_connection(pb::PutConnectionRequest {
                connection: Some(pb::ConnectionInfo {
                    name: "x".into(),
                    kind: "object".into(),
                    config_json: bad.to_vec(),
                }),
            })
            .await
            .expect_err("bad config");
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
    }
    let e = con
        .get_connection(pb::GetConnectionRequest {
            name: "nope".into(),
        })
        .await
        .expect_err("unknown");
    assert_eq!(e.code(), tonic::Code::NotFound);
    con.put_egress(pb::PutEgressRequest {
        egress: Some(pb::EgressInfo {
            module: "t/m".into(),
            allow: vec!["connection:files".into()],
        }),
    })
    .await
    .expect("egress");
    let list = con
        .list_egress(pb::ListEgressRequest {})
        .await
        .expect("list")
        .into_inner();
    assert_eq!(list.policies.len(), 1);
    con.delete_egress(pb::DeleteEgressRequest {
        module: "t/m".into(),
    })
    .await
    .expect("delete");
    sec.delete_secret(pb::DeleteSecretRequest {
        name: "k/one".into(),
        tenant: None,
    })
    .await
    .expect("delete secret");
}

/// SC3: a secret value written through either transport never comes back out of any
/// response (REST bodies, gRPC messages) or the OpenAPI document.
#[tokio::test]
async fn secret_values_are_write_only() {
    const CANARY: &str = "CANARY-7f3a9c1e-never-readable";
    const CANARY_GRPC: &str = "CANARY-grpc-5b2d-never-readable";
    let s = Server::start().await;
    let (mut sec, mut con) = s.grpc().await;

    let mut bodies: Vec<String> = Vec::new();
    // Write: REST UTF-8, REST base64 (of the canary), gRPC bytes.
    let (st, t) = s
        .send(
            s.http
                .put(s.url("/v1/secrets/canary/plain"))
                .json(&json!({ "value": CANARY })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    bodies.push(t);
    // "CANARY-7f3a9c1e-never-readable" in standard base64.
    let b64 = "Q0FOQVJZLTdmM2E5YzFlLW5ldmVyLXJlYWRhYmxl";
    let (st, t) = s
        .send(
            s.http
                .put(s.url("/v1/secrets/canary/b64?tenant=acme"))
                .json(&json!({ "value_b64": b64 })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{t}");
    bodies.push(t);
    assert_eq!(
        s.cp.secret_value("canary/b64", Some("acme")).as_deref(),
        Some(CANARY.as_bytes())
    );
    let put = sec
        .put_secret(pb::PutSecretRequest {
            name: "canary/grpc".into(),
            value: CANARY_GRPC.as_bytes().to_vec(),
            tenant: None,
        })
        .await
        .expect("grpc put")
        .into_inner();
    // gRPC responses rendered as text: `Debug` shows every string field verbatim, and
    // the only bytes field in any response (`ConnectionInfo.config_json`) is added as
    // UTF-8 below.
    let mut debug: Vec<String> = vec![format!("{put:?}")];

    // Read everything back over REST.
    for path in [
        "/v1/secrets",
        "/v1/secrets?tenant=acme",
        "/v1/grants",
        "/v1/connections",
        "/v1/egress",
        "/openapi.json",
    ] {
        let (st, t) = s.send(s.http.get(s.url(path))).await;
        assert_eq!(st, StatusCode::OK, "{path}");
        bodies.push(t);
    }
    // ... and over gRPC.
    for tenant in [None, Some("acme".to_owned())] {
        let l = sec
            .list_secrets(pb::ListSecretsRequest { tenant })
            .await
            .expect("list")
            .into_inner();
        debug.push(format!("{l:?}"));
    }
    let g = sec
        .list_grants(pb::ListGrantsRequest { module: None })
        .await
        .expect("grants")
        .into_inner();
    debug.push(format!("{g:?}"));
    let c = con
        .list_connections(pb::ListConnectionsRequest {})
        .await
        .expect("connections")
        .into_inner();
    debug.extend(
        c.connections
            .iter()
            .map(|c| format!("{c:?} {}", String::from_utf8_lossy(&c.config_json))),
    );
    let e = con
        .list_egress(pb::ListEgressRequest {})
        .await
        .expect("egress")
        .into_inner();
    debug.push(format!("{e:?}"));

    let listed: Value = serde_json::from_str(&bodies[2]).expect("list json");
    assert_eq!(listed["secrets"].as_array().map(Vec::len), Some(2));
    for canary in [CANARY, CANARY_GRPC, b64] {
        for b in &bodies {
            assert!(
                !b.contains(canary),
                "REST response leaks a secret value: {b}"
            );
        }
        for d in &debug {
            assert!(
                !d.contains(canary),
                "gRPC message leaks a secret value: {d}"
            );
        }
    }

    // No response schema has a field that could carry a value.
    let doc = serde_json::to_value(ostro_api::openapi()).expect("openapi");
    let schemas = doc["components"]["schemas"].as_object().expect("schemas");
    for name in ["SecretInfoDto", "SecretListResponse"] {
        let props = schemas[name]["properties"].as_object().expect("props");
        assert!(
            !props.keys().any(|k| k.contains("value")),
            "{name} exposes a value field"
        );
    }
    // The request DTO's value fields are documented write-only.
    let put_props = &schemas["PutSecretBody"]["properties"];
    assert_eq!(put_props["value"]["writeOnly"], true, "{put_props}");
    assert_eq!(put_props["value_b64"]["writeOnly"], true, "{put_props}");
}

#[test]
fn openapi_documents_bridge_paths() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    let paths = doc["paths"].as_object().expect("paths");
    for (path, method) in [
        ("/v1/secrets", "get"),
        ("/v1/secrets/{name}", "put"),
        ("/v1/secrets/{name}", "delete"),
        ("/v1/grants", "put"),
        ("/v1/grants", "get"),
        ("/v1/grants/{id}", "delete"),
        ("/v1/connections", "get"),
        ("/v1/connections/{name}", "put"),
        ("/v1/connections/{name}", "get"),
        ("/v1/connections/{name}", "delete"),
        ("/v1/egress", "get"),
        ("/v1/egress/{module}", "put"),
        ("/v1/egress/{module}", "get"),
        ("/v1/egress/{module}", "delete"),
        ("/v1/objects/{connection}/{key}", "get"),
        ("/v1/objects/{connection}/{key}", "put"),
    ] {
        assert!(
            paths.get(path).and_then(|p| p.get(method)).is_some(),
            "{method} {path} missing from OpenAPI"
        );
    }
    // OpenAPI paths never show axum's wildcard syntax.
    assert!(paths.keys().all(|p| !p.contains('*')));
}

/// A control plane with only the required methods: every bridge method answers the
/// default Unimplemented (REST 501, gRPC UNIMPLEMENTED), as ostro-server does until
/// 06-16.
struct Bare;

#[async_trait::async_trait]
impl ControlPlane for Bare {
    async fn upload_module(
        &self,
        _: domain::UploadModule,
    ) -> Result<domain::UploadModuleResult, ApiError> {
        Err(ApiError::internal("bare"))
    }
    async fn get_module(&self, _: String) -> Result<domain::ModuleInfo, ApiError> {
        Err(ApiError::internal("bare"))
    }
    async fn list_workflows(&self) -> Result<Vec<domain::WorkflowInfo>, ApiError> {
        Err(ApiError::internal("bare"))
    }
    async fn activate_workflow(
        &self,
        _: domain::ActivateWorkflow,
    ) -> Result<domain::WorkflowInfo, ApiError> {
        Err(ApiError::internal("bare"))
    }
    async fn start_run(
        &self,
        _: domain::StartRunRequest,
    ) -> Result<domain::StartRunResult, ApiError> {
        Err(ApiError::internal("bare"))
    }
    async fn get_run(&self, _: String) -> Result<domain::RunInfo, ApiError> {
        Err(ApiError::internal("bare"))
    }
    async fn run_history(
        &self,
        _: domain::HistoryRequest,
    ) -> Result<domain::HistoryPage, ApiError> {
        Err(ApiError::internal("bare"))
    }
    async fn list_runs(&self, _: domain::ListRunsRequest) -> Result<domain::RunPage, ApiError> {
        Err(ApiError::internal("bare"))
    }
    fn ready(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn defaults_answer_unimplemented() {
    let (addr, task) = serve_cp(Arc::new(Bare)).await;
    let http = reqwest::Client::new();
    let url = |p: &str| format!("http://{addr}{p}");
    for req in [
        http.get(url("/v1/secrets")),
        http.put(url("/v1/secrets/k"))
            .json(&json!({ "value": "0123456789" })),
        http.get(url("/v1/grants")),
        http.get(url("/v1/connections/x")),
        http.get(url("/v1/egress")),
        http.get(url("/v1/objects/c/k?exp=1&sig=valid")),
    ] {
        let resp = req.send().await.expect("send");
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
        let v: Value = resp.json().await.expect("json");
        assert_eq!(v["error"]["code"], ErrorCode::Unimplemented.as_str());
    }
    let ch = Channel::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("connect");
    let e = SecretsClient::new(ch.clone())
        .list_secrets(pb::ListSecretsRequest::default())
        .await
        .expect_err("unimplemented");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    let e = ConnectorsClient::new(ch)
        .list_connections(pb::ListConnectionsRequest {})
        .await
        .expect_err("unimplemented");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    task.abort();
}
