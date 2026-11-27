//! 14-17 Task 1: `TenantsApi` + `ApiTokensApi` (D-P14-8, D-P14-4).
//!
//! A server in `[auth] mode = "token"` (the bootstrap admin bearer from `admin.token`) is
//! driven over REST for the wire-visible rules (201/409/400, `no-store`, 401 after revoke and
//! rotation, no `hash` in listings). Rules that depend on the calling principal call the trait
//! impls on the server's own context inside `auth::scope(principal, …)` and never rely on
//! guard denials (scope denials are 14-15's tests). The run fan-out of `delete_tenant` is
//! exercised on a bare context bound to a fake routed control plane.

mod common;

use std::sync::Arc;
use std::time::Duration;

use ostro_api::auth::{self, Principal, Scope, ScopeSet};
use ostro_api::domain::{ActivateWorkflow, RunStatus, StartRunRequest, UploadModule};
use ostro_api::error::reason;
use ostro_api::fake::{FakeControlPlane, fake_module, sha256_hex};
use ostro_api::phase14::{
    ApiTokensApi, CreateTokenRequest, RotateTokenRequest, TenantSpec, TenantsApi,
};
use ostro_api::{ApiError, ControlPlane, ErrorCode};
use ostro_meta::ext::{ClusterVersion, put_cluster_version};
use ostro_meta::{
    ActiveVersion, ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult,
    SealedSecret, WorkflowVersion,
};
use ostro_partition::Clock;
use ostro_partition::clock::ManualClock;
use ostro_server::config::ServerConfig;
use ostro_server::p14::bootstrap::ADMIN_TOKEN_FILE;
use ostro_server::p14::tenants_api::{self, TenantsService};
use ostro_server::p14::tokens_api::TokensService;
use ostro_server::p14::{self, P14Init};
use serde_json::{Value, json};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(60);
const HOUR_MS: u64 = 3_600_000;

// ---------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------

struct Srv {
    h: ostro_server::ServerHandle,
    base: String,
    admin: String,
    http: reqwest::Client,
    _dir: tempfile::TempDir,
}

impl Srv {
    async fn start() -> Srv {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = "127.0.0.1:0".parse().unwrap();
        cfg.cluster.listen = "127.0.0.1:0".parse().unwrap();
        cfg.node.data_dir = dir.path().to_path_buf();
        cfg.node.partitions = 2;
        let h = timeout(T, ostro_server::server::start(cfg))
            .await
            .expect("server start timed out")
            .expect("server start");
        let admin = std::fs::read_to_string(dir.path().join(ADMIN_TOKEN_FILE))
            .expect("admin.token")
            .trim()
            .to_owned();
        set_finalized(h.meta(), 2).await;
        let base = format!("http://{}", h.local_addr());
        Srv {
            h,
            base,
            admin,
            http: reqwest::Client::new(),
            _dir: dir,
        }
    }

    fn ctx(&self) -> Arc<p14::P14Ctx> {
        p14::ctx_of(self.h.control())
            .expect("ctx")
            .expect("attached")
    }

    fn tenants(&self) -> TenantsService {
        TenantsService::new(&self.ctx())
    }

    fn tokens(&self) -> TokensService {
        TokensService::new(&self.ctx())
    }

    async fn req(
        &self,
        method: reqwest::Method,
        path: &str,
        bearer: Option<&str>,
        body: Option<Value>,
    ) -> (u16, reqwest::header::HeaderMap, Value) {
        let mut r = self.http.request(method, format!("{}{path}", self.base));
        if let Some(b) = bearer {
            r = r.bearer_auth(b);
        }
        if let Some(b) = body {
            r = r.json(&b);
        }
        let resp = timeout(T, r.send())
            .await
            .expect("request timed out")
            .expect("request");
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let text = resp.text().await.expect("body");
        let v = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        (status, headers, v)
    }

    async fn admin(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let (s, _, v) = self.req(method, path, Some(&self.admin), body).await;
        (s, v)
    }

    async fn shutdown(self) {
        self.h.shutdown().await.expect("shutdown");
    }
}

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
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: Some(cur.version),
            record: MetaRecord::Cluster(info),
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

async fn put(meta: &MetaHandle, record: MetaRecord) {
    let r = meta
        .propose(MetaCommand::Put {
            key: record.key(),
            expected_version: None,
            record,
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

async fn delete(meta: &MetaHandle, key: MetaKey) {
    let version = meta
        .view()
        .read(|s| s.get(&key).map(|v| v.version))
        .expect("present");
    let r = meta
        .propose(MetaCommand::Delete {
            key,
            expected_version: version,
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

fn has_token(meta: &MetaHandle, id: &str) -> bool {
    meta.view().read(|s| s.token(id).is_some())
}

fn tenant_admin(t: &str) -> Principal {
    Principal::token("otk_tenantadmin0000", Some(t.into()), ScopeSet::admin())
}

fn tenant_p(t: &str, scopes: &[Scope]) -> Principal {
    Principal::token("otk_tenantuser00000", Some(t.into()), ScopeSet::of(scopes))
}

fn req(name: &str, scopes: &[&str], tenant: Option<&str>) -> CreateTokenRequest {
    CreateTokenRequest {
        name: name.into(),
        scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
        tenant: tenant.map(str::to_owned),
        ..CreateTokenRequest::default()
    }
}

fn reason_of(e: &ApiError) -> Option<&str> {
    e.reason()
}

/// Every object key anywhere in `v`.
fn keys(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                out.push(k.clone());
                keys(x, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|x| keys(x, out)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------------
// Tenants over REST
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_lifecycle_over_rest() {
    use reqwest::Method;
    let s = Srv::start().await;

    // create (201) with quotas; duplicate 409; `_` and bad grammar 400; bad quota 400, no record
    let (st, v) = s
        .admin(
            Method::POST,
            "/v1/tenants",
            Some(json!({
                "id": "acme", "weight": 2, "max_concurrency": 4, "fuel_per_s": 1000,
                "memory_bytes": 1048576, "event_rate": {"limit": 10, "period_ms": 1000}
            })),
        )
        .await;
    assert_eq!(st, 201, "{v}");
    assert_eq!(v["id"], "acme");
    assert_eq!(v["weight"], 2);
    assert_eq!(v["suspended"], false);
    let now = s.h.clock().now_ms();
    let created = v["created_at_ms"].as_u64().unwrap();
    assert!(
        created <= now && now - created < 60_000,
        "{created} vs {now}"
    );
    let (st, v) = s
        .admin(Method::POST, "/v1/tenants", Some(json!({"id": "acme"})))
        .await;
    assert_eq!(st, 409, "{v}");
    for bad in ["_", "Acme", "-x", ""] {
        let (st, v) = s
            .admin(Method::POST, "/v1/tenants", Some(json!({ "id": bad })))
            .await;
        assert_eq!(st, 400, "{bad:?}: {v}");
    }
    // `_` straight into the impl (the adapter's grammar check is not the only guard).
    let e = auth::scope(
        Principal::system(),
        s.tenants().create_tenant(TenantSpec {
            id: "_".into(),
            ..TenantSpec::default()
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidArgument);
    assert!(e.message.contains("default tenant"), "{e}");
    for (field, value) in [
        ("max_concurrency", json!(0)),
        ("weight", json!(0)),
        ("fuel_burst", json!(0)),
        ("memory_bytes", json!(0)),
        ("event_rate", json!({"limit": 1, "period_ms": 0})),
        ("event_rate", json!({"limit": 0, "period_ms": 1000})),
    ] {
        let (st, v) = s
            .admin(
                Method::POST,
                "/v1/tenants",
                Some(json!({ "id": "zero", field: value })),
            )
            .await;
        assert_eq!(st, 400, "{field}: {v}");
    }
    assert!(s.h.meta().view().read(|m| m.tenant("zero").is_none()));

    // list (sorted) and get
    let (st, _) = s
        .admin(Method::POST, "/v1/tenants", Some(json!({"id": "beta"})))
        .await;
    assert_eq!(st, 201);
    let (st, v) = s.admin(Method::GET, "/v1/tenants", None).await;
    assert_eq!(st, 200);
    let ids: Vec<&str> = v["tenants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["acme", "beta"]);
    let (st, v) = s.admin(Method::GET, "/v1/tenants/acme", None).await;
    assert_eq!((st, v["max_concurrency"].as_u64()), (200, Some(4)));
    let (st, _) = s.admin(Method::GET, "/v1/tenants/nope", None).await;
    assert_eq!(st, 404);

    // patch: set weight, clear max_concurrency with null, keep the rest; 0 refused
    let (st, v) = s
        .admin(
            Method::PATCH,
            "/v1/tenants/acme",
            Some(json!({"weight": 5, "max_concurrency": null})),
        )
        .await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["weight"], 5);
    assert!(v["max_concurrency"].is_null(), "{v}");
    assert_eq!(v["fuel_per_s"], 1000);
    let (st, _) = s
        .admin(
            Method::PATCH,
            "/v1/tenants/acme",
            Some(json!({"fuel_per_s": 0})),
        )
        .await;
    assert_eq!(st, 400);
    let (st, _) = s
        .admin(
            Method::PATCH,
            "/v1/tenants/nope",
            Some(json!({"weight": 1})),
        )
        .await;
    assert_eq!(st, 404);

    // the 14-06 quota view sees the change within one meta-watch tick
    let ctx = s.ctx();
    let seen = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(q) = ctx.quotas.quotas("acme")
                && q.weight == Some(5)
                && q.max_concurrency.is_none()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(seen.is_ok(), "quota view never saw the patch");

    // suspend / resume are idempotent; the quota view follows
    for _ in 0..2 {
        let (st, v) = s
            .admin(Method::POST, "/v1/tenants/acme/suspend", None)
            .await;
        assert_eq!((st, &v["suspended"]), (200, &json!(true)));
    }
    let seen = timeout(Duration::from_secs(5), async {
        while !ctx.quotas.quotas("acme").is_some_and(|q| q.suspended) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(seen.is_ok(), "quota view never saw the suspension");
    for _ in 0..2 {
        let (st, v) = s.admin(Method::POST, "/v1/tenants/beta/resume", None).await;
        assert_eq!((st, &v["suspended"]), (200, &json!(false)));
    }

    // delete: not suspended -> 409; a token or a secret of the tenant -> 409; empty -> 200
    let (st, v) = s.admin(Method::DELETE, "/v1/tenants/beta", None).await;
    assert_eq!(st, 409);
    assert_eq!(
        v["error"]["details"]["reason"],
        reason::TENANT_NOT_EMPTY,
        "{v}"
    );
    let (st, tok) = s
        .admin(
            Method::POST,
            "/v1/auth/tokens",
            Some(json!({"name": "ci", "scopes": ["deploy"], "tenant": "acme"})),
        )
        .await;
    assert_eq!(st, 201, "{tok}");
    let (st, v) = s.admin(Method::DELETE, "/v1/tenants/acme", None).await;
    assert_eq!(st, 409);
    assert_eq!(v["error"]["details"]["reason"], reason::TENANT_NOT_EMPTY);
    assert!(
        v["error"]["message"].as_str().unwrap().contains("token"),
        "{v}"
    );
    let id = tok["info"]["id"].as_str().unwrap();
    let (st, _) = s
        .admin(Method::DELETE, &format!("/v1/auth/tokens/{id}"), None)
        .await;
    assert_eq!(st, 200);
    put(
        s.h.meta(),
        MetaRecord::Secret(SealedSecret {
            name: "acme/db".into(),
            ..SealedSecret::default()
        }),
    )
    .await;
    let (st, v) = s.admin(Method::DELETE, "/v1/tenants/acme", None).await;
    assert_eq!(st, 409);
    assert!(
        v["error"]["message"].as_str().unwrap().contains("secret"),
        "{v}"
    );
    delete(s.h.meta(), MetaKey::Secret("acme/db".into())).await;
    let (st, v) = s.admin(Method::DELETE, "/v1/tenants/acme", None).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v, json!({"id": "acme", "deleted": true}));
    let (st, _) = s.admin(Method::GET, "/v1/tenants/acme", None).await;
    assert_eq!(st, 404);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// delete_tenant: active versions and non-terminal runs (bare context, fake routed plane)
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_counts_active_versions_and_non_terminal_runs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = Arc::new(
        MetaHandle::start(&dir.path().join("meta"), 1)
            .await
            .expect("meta"),
    );
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(7_000));
    let ctx = p14::build_ctx(P14Init {
        cfg: Arc::new(ServerConfig::server_defaults()),
        meta: Arc::clone(&meta),
        clock,
        node_id: 1,
        data_dir: dir.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("ctx");
    let svc = TenantsService::new(&ctx);
    auth::scope(Principal::system(), async {
        let info = svc
            .create_tenant(TenantSpec {
                id: "acme".into(),
                ..TenantSpec::default()
            })
            .await
            .expect("create");
        assert_eq!(info.created_at_ms, 7_000, "injected clock");
        svc.suspend_tenant("acme".into()).await.expect("suspend");

        // No routed plane bound yet: fail closed.
        let e = svc.delete_tenant("acme".into()).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Unavailable, "{e}");

        let fake = Arc::new(FakeControlPlane::new());
        let routed: Arc<dyn ControlPlane> = Arc::clone(&fake) as Arc<dyn ControlPlane>;
        tenants_api::bind_routed(&ctx, &routed);

        // An active version of acme/orders.
        put(
            &meta,
            MetaRecord::Workflow(WorkflowVersion {
                name: "acme/orders".into(),
                version: 1,
                ..WorkflowVersion::default()
            }),
        )
        .await;
        put(
            &meta,
            MetaRecord::Active(ActiveVersion {
                name: "acme/orders".into(),
                version: 1,
                ext: Vec::new(),
            }),
        )
        .await;
        let e = svc.delete_tenant("acme".into()).await.unwrap_err();
        assert_eq!(reason_of(&e), Some(reason::TENANT_NOT_EMPTY));
        assert!(e.message.contains("active version"), "{e}");
        delete(&meta, MetaKey::Active("acme/orders".into())).await;

        // A non-terminal run of acme (fan-out through the routed plane).
        let wasm = fake_module(&[("orders", 1)]);
        fake.upload_module(UploadModule {
            sha256: sha256_hex(&wasm),
            wasm,
            language: None,
            tenant: None,
            name: None,
            signatures: Vec::new(),
        })
        .await
        .expect("upload");
        fake.activate_workflow(ActivateWorkflow {
            name: "orders".into(),
            version: 1,
            expected_version: None,
        })
        .await
        .expect("activate");
        let run = fake
            .start_run(StartRunRequest {
                workflow: "orders".into(),
                input_json: b"{}".to_vec(),
                tenant: Some("acme".into()),
                ..StartRunRequest::default()
            })
            .await
            .expect("start")
            .run_id;
        // A run of another tenant never blocks acme.
        fake.start_run(StartRunRequest {
            workflow: "orders".into(),
            input_json: b"{}".to_vec(),
            tenant: Some("other".into()),
            ..StartRunRequest::default()
        })
        .await
        .expect("start other");
        let e = svc.delete_tenant("acme".into()).await.unwrap_err();
        assert_eq!(reason_of(&e), Some(reason::TENANT_NOT_EMPTY));
        assert!(e.message.contains(&run), "{e}");
        fake.set_run_outcome(&run, RunStatus::Completed, Some(b"1".to_vec()), None, None)
            .expect("complete");
        svc.delete_tenant("acme".into()).await.expect("empty now");
        assert!(meta.view().read(|s| s.tenant("acme").is_none()));
        let e = svc.delete_tenant("acme".into()).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound);
    })
    .await;
}

// ---------------------------------------------------------------------------------
// Token creation rules (principal-dependent: trait impls inside auth::scope)
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_creation_rules() {
    let s = Srv::start().await;
    let te = s.tenants();
    let tk = s.tokens();
    auth::scope(Principal::system(), async {
        for t in ["acme", "globex"] {
            te.create_tenant(TenantSpec {
                id: t.into(),
                ..TenantSpec::default()
            })
            .await
            .expect("tenant");
        }
    })
    .await;
    let now = || s.h.clock().now_ms();

    // Tenant admin: own tenant only (unset = own), any scope it holds.
    auth::scope(tenant_admin("acme"), async {
        let c = tk
            .create_token(req("ci", &["deploy", "read"], Some("acme")))
            .await
            .expect("own tenant");
        assert_eq!(c.info.tenant.as_deref(), Some("acme"));
        assert_eq!(c.info.scopes, ["deploy", "read"]);
        assert_eq!(c.info.created_by, "otk_tenantadmin0000");
        let c = tk
            .create_token(req("implicit", &["trigger"], None))
            .await
            .expect("unset tenant = own");
        assert_eq!(c.info.tenant.as_deref(), Some("acme"));
        let e = tk
            .create_token(req("x", &["deploy"], Some("globex")))
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::PermissionDenied);
        assert_eq!(reason_of(&e), Some(reason::FORBIDDEN_TENANT));
        // no expiry is a system-admin privilege
        let e = tk
            .create_token(CreateTokenRequest {
                no_expiry: true,
                ..req("forever", &["read"], None)
            })
            .await
            .unwrap_err();
        assert_eq!(reason_of(&e), Some(reason::MISSING_SCOPE), "{e}");
    })
    .await;

    // A tenant principal without admin: scopes it lacks are refused.
    auth::scope(tenant_p("acme", &[Scope::Deploy, Scope::Read]), async {
        for wanted in [&["operate"][..], &["admin"], &["deploy", "trigger"]] {
            let e = tk.create_token(req("x", wanted, None)).await.unwrap_err();
            assert_eq!(e.code, ErrorCode::PermissionDenied, "{wanted:?}");
            assert_eq!(reason_of(&e), Some(reason::MISSING_SCOPE), "{wanted:?}");
        }
        tk.create_token(req("sub", &["deploy"], None))
            .await
            .expect("a subset is fine");
    })
    .await;

    // System admin: any tenant; system tokens only admin / read; tenant must exist.
    auth::scope(Principal::system(), async {
        let before = now();
        let c = tk
            .create_token(req("g", &["deploy"], Some("globex")))
            .await
            .expect("any tenant");
        let exp = c.info.expires_at_ms.expect("default expiry");
        assert!(
            exp >= before + 2160 * HOUR_MS && exp <= now() + 2160 * HOUR_MS,
            "default_token_ttl 2160h: {exp}"
        );
        let c = tk
            .create_token(CreateTokenRequest {
                ttl_ms: Some(HOUR_MS),
                ..req("short", &["read"], None)
            })
            .await
            .expect("system read");
        let exp = c.info.expires_at_ms.unwrap();
        assert!(exp >= before + HOUR_MS && exp <= now() + HOUR_MS);
        assert_eq!(c.info.tenant, None);
        let c = tk
            .create_token(CreateTokenRequest {
                no_expiry: true,
                ..req("forever", &["admin"], None)
            })
            .await
            .expect("no expiry by a system admin");
        assert_eq!(c.info.expires_at_ms, None);
        tk.create_token(req("default", &["deploy"], Some("_")))
            .await
            .expect("the default tenant always exists");
        let e = tk
            .create_token(req("sys", &["deploy"], None))
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument, "{e}");
        let e = tk
            .create_token(req("ghost", &["deploy"], Some("nope")))
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::NotFound, "{e}");
        for bad in [
            req("", &["read"], None),
            req("x", &[], None),
            req("x", &["worker:join"], None),
            CreateTokenRequest {
                ttl_ms: Some(0),
                ..req("x", &["read"], None)
            },
            CreateTokenRequest {
                ttl_ms: Some(1),
                no_expiry: true,
                ..req("x", &["read"], None)
            },
        ] {
            let e = tk.create_token(bad.clone()).await.unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{bad:?}: {e}");
        }
    })
    .await;

    // Listing: a tenant admin sees its own tenant only; system sees all, filterable.
    let acme: Vec<_> = auth::scope(tenant_admin("acme"), tk.list_tokens(None))
        .await
        .expect("list");
    assert!(!acme.is_empty());
    assert!(acme.iter().all(|t| t.tenant.as_deref() == Some("acme")));
    let e = auth::scope(tenant_admin("acme"), tk.list_tokens(Some("globex".into())))
        .await
        .unwrap_err();
    assert_eq!(reason_of(&e), Some(reason::FORBIDDEN_TENANT));
    let all = auth::scope(Principal::system(), tk.list_tokens(None))
        .await
        .unwrap();
    assert!(all.iter().any(|t| t.tenant.as_deref() == Some("globex")));
    assert!(all.iter().any(|t| t.tenant.is_none()), "bootstrap admin");
    let globex = auth::scope(Principal::system(), tk.list_tokens(Some("globex".into())))
        .await
        .unwrap();
    assert_eq!(globex.len(), 1);

    // Revoke / rotate of another tenant's token: 404 to a tenant admin, record survives.
    let gid = globex[0].id.clone();
    let e = auth::scope(tenant_admin("acme"), tk.revoke_token(gid.clone()))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    let e = auth::scope(
        tenant_admin("acme"),
        tk.rotate_token(gid.clone(), RotateTokenRequest::default()),
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    assert!(has_token(s.h.meta(), &gid));
    // ... and the same token is revocable by its own tenant's admin.
    auth::scope(tenant_admin("globex"), tk.revoke_token(gid.clone()))
        .await
        .expect("own tenant");
    assert!(!has_token(s.h.meta(), &gid));
    let e = auth::scope(tenant_admin("globex"), tk.revoke_token(gid.clone()))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound, "unknown = 404");

    // whoami answers the caller
    let w = auth::scope(tenant_p("acme", &[Scope::Read]), tk.whoami())
        .await
        .unwrap();
    assert_eq!(
        (w.token_id.as_str(), w.tenant.as_deref(), w.kind.as_str()),
        ("otk_tenantuser00000", Some("acme"), "token")
    );
    assert_eq!(w.scopes, ["read"]);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Bearers: create once, revoke -> 401, rotate, wkr_/njt_ untouchable, no hash in lists
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bearers_revocation_rotation_and_listing() {
    use reqwest::Method;
    let s = Srv::start().await;
    let (st, _) = s
        .admin(Method::POST, "/v1/tenants", Some(json!({"id": "acme"})))
        .await;
    assert_eq!(st, 201);

    // create: 201, Cache-Control no-store, the bearer once, ext 112 fields
    let (st, hdr, c) = s
        .req(
            Method::POST,
            "/v1/auth/tokens",
            Some(&s.admin),
            Some(json!({
                "name": "deployer", "description": "CI", "scopes": ["deploy", "read"],
                "tenant": "acme", "ttl_ms": 3_600_000u64
            })),
        )
        .await;
    assert_eq!(st, 201, "{c}");
    assert_eq!(
        hdr.get("cache-control").and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
    let bearer = c["bearer"].as_str().unwrap().to_owned();
    let id = c["info"]["id"].as_str().unwrap().to_owned();
    assert!(bearer.starts_with(&format!("ostro_{id}_")), "{bearer}");
    assert_eq!(c["info"]["name"], "deployer");
    assert_eq!(c["info"]["description"], "CI");
    assert!(
        c["info"]["created_by"]
            .as_str()
            .unwrap()
            .starts_with("otk_")
    );

    // the bearer authenticates as the tenant principal
    let (st, _, w) = s
        .req(Method::GET, "/v1/auth/whoami", Some(&bearer), None)
        .await;
    assert_eq!(st, 200, "{w}");
    assert_eq!(
        w,
        json!({"token_id": id, "tenant": "acme", "scopes": ["deploy", "read"], "kind": "token"})
    );

    // stored: hash + ApiTokenMeta only; no listing carries a hash or a secret anywhere
    let rec =
        s.h.meta()
            .view()
            .read(|m| m.token(&id).map(|t| t.value.clone()));
    let rec = rec.expect("record");
    assert_ne!(rec.hash, [0u8; 32]);
    let secret = bearer.rsplit_once('_').unwrap().1.to_owned();
    let (st, list) = s.admin(Method::GET, "/v1/auth/tokens", None).await;
    assert_eq!(st, 200);
    let mut ks = Vec::new();
    keys(&list, &mut ks);
    assert!(!ks.iter().any(|k| k == "hash" || k == "bearer"), "{ks:?}");
    let text = list.to_string();
    assert!(!text.contains(&secret), "secret leaked into the listing");
    assert!(!text.contains("\"hash\""));
    let (st, list) = s
        .admin(Method::GET, "/v1/auth/tokens?tenant=acme", None)
        .await;
    assert_eq!(st, 200);
    assert_eq!(list["tokens"].as_array().unwrap().len(), 1, "{list}");

    // rotate keeping the old one: both bearers work; rotated_from set; same name/scopes
    let (st, hdr, r1) = s
        .req(
            Method::POST,
            &format!("/v1/auth/tokens/{id}/rotate"),
            Some(&s.admin),
            Some(json!({"revoke_old": false})),
        )
        .await;
    assert_eq!(st, 201, "{r1}");
    assert_eq!(
        hdr.get("cache-control").and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
    assert_eq!(r1["info"]["rotated_from"], json!(id));
    assert_eq!(r1["info"]["name"], "deployer");
    assert_eq!(r1["info"]["scopes"], json!(["deploy", "read"]));
    assert_eq!(r1["info"]["tenant"], "acme");
    let lifetime = r1["info"]["expires_at_ms"].as_u64().unwrap()
        - r1["info"]["created_at_ms"].as_u64().unwrap();
    assert_eq!(lifetime, 3_600_000, "the old token's lifetime");
    let b1 = r1["bearer"].as_str().unwrap().to_owned();
    for b in [&bearer, &b1] {
        let (st, _, _) = s.req(Method::GET, "/v1/auth/whoami", Some(b), None).await;
        assert_eq!(st, 200);
    }

    // rotate revoking the old one: the old bearer is rejected on the next request
    let id1 = r1["info"]["id"].as_str().unwrap().to_owned();
    let (st, _, r2) = s
        .req(
            Method::POST,
            &format!("/v1/auth/tokens/{id1}/rotate"),
            Some(&s.admin),
            Some(json!({"revoke_old": true})),
        )
        .await;
    assert_eq!(st, 201, "{r2}");
    let b2 = r2["bearer"].as_str().unwrap().to_owned();
    let (st, _, _) = s.req(Method::GET, "/v1/auth/whoami", Some(&b1), None).await;
    assert_eq!(st, 401, "rotated (revoked) bearer");
    let (st, _, _) = s.req(Method::GET, "/v1/auth/whoami", Some(&b2), None).await;
    assert_eq!(st, 200);

    // revoke: deleted, the bearer gets 401 on the next request, a second revoke is 404
    let (st, v) = s
        .admin(Method::DELETE, &format!("/v1/auth/tokens/{id}"), None)
        .await;
    assert_eq!(st, 200, "{v}");
    assert!(!has_token(s.h.meta(), &id));
    let (st, _, _) = s
        .req(Method::GET, "/v1/auth/whoami", Some(&bearer), None)
        .await;
    assert_eq!(st, 401, "revoked bearer");
    let (st, _) = s
        .admin(Method::DELETE, &format!("/v1/auth/tokens/{id}"), None)
        .await;
    assert_eq!(st, 404);

    // wkr_ / njt_ records: never listed, revoke / rotate 404, the record survives
    for (wid, scope) in [
        ("wkr_abcdefghijklmnop", "worker:join"),
        ("njt_abcdefghijklmnop", "node:join"),
    ] {
        put(
            s.h.meta(),
            MetaRecord::Token(ApiToken {
                id: wid.into(),
                hash: [7; 32],
                scopes: vec![scope.into()],
                tenant: None,
                created_at_ms: 1,
                expires_at_ms: None,
                ext: Vec::new(),
            }),
        )
        .await;
        let (st, _) = s
            .admin(Method::DELETE, &format!("/v1/auth/tokens/{wid}"), None)
            .await;
        assert_eq!(st, 404, "{wid}");
        let (st, _) = s
            .admin(
                Method::POST,
                &format!("/v1/auth/tokens/{wid}/rotate"),
                Some(json!({"revoke_old": true})),
            )
            .await;
        assert_eq!(st, 404, "{wid}");
        assert!(has_token(s.h.meta(), wid), "{wid} survives");
    }
    let (_, list) = s.admin(Method::GET, "/v1/auth/tokens", None).await;
    let ids: Vec<&str> = list["tokens"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert!(ids.iter().all(|i| i.starts_with("otk_")), "{ids:?}");

    // the bootstrap admin's own whoami
    let (st, w) = s.admin(Method::GET, "/v1/auth/whoami", None).await;
    assert_eq!(st, 200);
    assert_eq!(w["tenant"], Value::Null);
    assert_eq!(w["scopes"], json!(["admin"]));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ext_112_needs_finalized_level_2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = Arc::new(
        MetaHandle::start(&dir.path().join("meta"), 1)
            .await
            .expect("meta"),
    );
    let ctx = p14::build_ctx(P14Init {
        cfg: Arc::new(ServerConfig::server_defaults()),
        meta: Arc::clone(&meta),
        clock: Arc::new(ManualClock::new(10_000)),
        node_id: 1,
        data_dir: dir.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("ctx");
    let tk = TokensService::new(&ctx);
    auth::scope(Principal::system(), async {
        // Fresh metadata (no finalized level recorded): no ext, so no name / created_by.
        let c = tk
            .create_token(req("before", &["read"], None))
            .await
            .expect("create");
        assert_eq!(c.info.created_at_ms, 10_000, "injected clock");
        assert_eq!(c.info.expires_at_ms, Some(10_000 + 2160 * HOUR_MS));
        let rec = meta
            .view()
            .read(|s| s.token(&c.info.id).map(|t| t.value.clone()))
            .unwrap();
        assert!(rec.ext.is_empty(), "no ext 112 below level 2");
        assert_eq!(c.info.name, "");
    })
    .await;
}

// ---------------------------------------------------------------------------------
// Default tenant `_` tokens (14-15 follow-up): bare names, run starts, isolation
// ---------------------------------------------------------------------------------

impl Srv {
    async fn upload(&self, bearer: &str, guest: &str) -> (u16, Value) {
        let wasm = common::guest_wasm(guest);
        let sha = sha256_hex(&wasm);
        let r = self
            .http
            .put(format!("{}/v1/modules/{sha}", self.base))
            .bearer_auth(bearer)
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        let st = r.status().as_u16();
        (st, r.json().await.unwrap_or(Value::Null))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_tenant_token_starts_runs_and_stays_in_its_tenant() {
    use reqwest::Method;
    let s = Srv::start().await;
    let tk = s.tokens();
    let (st, _) = s
        .admin(Method::POST, "/v1/tenants", Some(json!({"id": "acme"})))
        .await;
    assert_eq!(st, 201);
    let scopes = &["deploy", "trigger", "read"];
    let (default, acme) = auth::scope(Principal::system(), async {
        let d = tk
            .create_token(req("default-ci", scopes, Some("_")))
            .await
            .expect("_ token");
        let a = tk
            .create_token(req("acme-ci", scopes, Some("acme")))
            .await
            .expect("acme token");
        (d, a)
    })
    .await;
    assert_eq!(default.info.tenant.as_deref(), Some("_"));
    let (d, a) = (default.bearer.as_str(), acme.bearer.as_str());

    // whoami: a tenant principal of `_`, not system scope
    let (st, _, w) = s.req(Method::GET, "/v1/auth/whoami", Some(d), None).await;
    assert_eq!((st, &w["tenant"]), (200, &json!("_")), "{w}");

    // `_` deploys and starts with bare names (this was a 400: tenant "_" forced into the
    // start request)
    let (st, v) = s.upload(d, "signals").await;
    assert!((200..300).contains(&st), "deploy as _: {st} {v}");
    let (st, _, run) = s
        .req(
            Method::POST,
            "/v1/workflows/signals/runs",
            Some(d),
            Some(json!({"input": {"mode": "long_wait"}})),
        )
        .await;
    assert_eq!(st, 201, "start as _: {run}");
    let run_d = run["run_id"].as_str().unwrap().to_owned();
    let (st, _, info) = s
        .req(Method::GET, &format!("/v1/runs/{run_d}"), Some(d), None)
        .await;
    assert_eq!(st, 200, "{info}");
    assert_eq!(info["workflow"], "signals");

    // acme deploys the same guest in its tenant and starts one run
    let (st, v) = s.upload(a, "signals").await;
    assert!((200..300).contains(&st), "deploy as acme: {st} {v}");
    let (st, _, run) = s
        .req(
            Method::POST,
            "/v1/workflows/signals/runs",
            Some(a),
            Some(json!({"input": {"mode": "long_wait"}})),
        )
        .await;
    assert_eq!(st, 201, "start as acme: {run}");
    let run_a = run["run_id"].as_str().unwrap().to_owned();

    // isolation both ways: foreign run ids are 404, listings hold only the own tenant
    let (st, _, _) = s
        .req(Method::GET, &format!("/v1/runs/{run_a}"), Some(d), None)
        .await;
    assert_eq!(st, 404, "_ must not see acme's run");
    let (st, _, _) = s
        .req(Method::GET, &format!("/v1/runs/{run_d}"), Some(a), None)
        .await;
    assert_eq!(st, 404, "acme must not see the default tenant's run");
    let ids = |v: &Value| -> Vec<String> {
        v["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["run_id"].as_str().unwrap().to_owned())
            .collect()
    };
    let (st, _, list) = s
        .req(Method::GET, "/v1/runs?limit=100", Some(d), None)
        .await;
    assert_eq!(st, 200, "{list}");
    assert_eq!(ids(&list), std::slice::from_ref(&run_d), "{list}");
    let (st, _, list) = s
        .req(Method::GET, "/v1/runs?limit=100", Some(a), None)
        .await;
    assert_eq!(st, 200, "{list}");
    assert_eq!(ids(&list), std::slice::from_ref(&run_a), "{list}");
    // naming another tenant (or qualifying a name) is refused for `_`
    let (st, _, _) = s
        .req(Method::GET, "/v1/runs?tenant=acme", Some(d), None)
        .await;
    assert_eq!(st, 403);
    let (st, _, _) = s
        .req(
            Method::POST,
            "/v1/workflows/acme%2Fsignals/runs",
            Some(d),
            Some(json!({"input": {"mode": "long_wait"}})),
        )
        .await;
    assert!(st == 400 || st == 404, "qualified name as _: {st}");
    // the system admin sees both, the acme run under its qualified name
    let (_, list) = s.admin(Method::GET, "/v1/runs?limit=100", None).await;
    let all = ids(&list);
    assert!(all.contains(&run_d) && all.contains(&run_a), "{list}");
    s.shutdown().await;
}
