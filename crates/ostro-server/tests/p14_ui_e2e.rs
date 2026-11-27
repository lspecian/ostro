//! 14-21 Task 2: the embedded UI on a real server behind the Phase 14 authentication
//! (D-P14-31, SEC-02).
//!
//! - token mode, tenants `a` and `b`: a page without a session redirects to `/ui/login`;
//!   sign-in needs the CSRF token; a read-only token of `a` sees `a`'s runs (never `b`'s,
//!   `b`'s run page is 404) and cannot cancel (403, run unchanged); an operate token cancels
//!   through the UI and the audit journal records `run.cancel` with that token id; revoking
//!   the token signs its session out on the next request; sign-out clears the cookie; the
//!   failed sign-in is an `auth.failure` audit record;
//! - a session cookie issued by node 1 of a two-node cluster is accepted by node 2;
//! - `[auth] mode = "none"`: the UI works without signing in (Phase 11 behaviour).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

#[path = "common/p12_cluster.rs"]
mod p12;

use std::path::Path;
use std::time::Duration;

use ostro_meta::ext::{ClusterVersion, put_cluster_version};
use ostro_meta::{
    ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, Tenant,
};
use ostro_partition::store::values::RunStatus;
use ostro_server::ServerHandle;
use ostro_server::config::AuthMode;
use ostro_server::p14::tokens;
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};

const LIMIT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

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

/// Finalize level 2 (the Phase 14 records) and create tenants `a` and `b`.
async fn tenants(meta: &MetaHandle) {
    let cur = meta
        .view()
        .read(|st| st.cluster().cloned())
        .expect("cluster");
    let mut info: ClusterInfo = cur.value;
    info.ext = put_cluster_version(
        &info.ext,
        &ClusterVersion {
            finalized: 2,
            hold: None,
            finalized_at_ms: 1,
        },
    )
    .expect("ext 98");
    propose(meta, MetaKey::Cluster, MetaRecord::Cluster(info)).await;
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
}

/// A token stored through `meta`; returns `(id, bearer)`.
async fn put_token(meta: &MetaHandle, tenant: Option<&str>, scopes: &[&str]) -> (String, String) {
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
    (id.as_str().to_owned(), bearer.expose().to_owned())
}

/// REST with a bearer.
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

    async fn deploy(&self, guest: &str) {
        let wasm = guest_wasm(guest);
        let sha: [u8; 32] = Sha256::digest(&wasm).into();
        let hex: String = sha.iter().map(|x| format!("{x:02x}")).collect();
        let r = self
            .http
            .put(format!("{}/v1/modules/{hex}", self.base))
            .bearer_auth(&self.bearer)
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    }

    async fn start(&self, workflow: &str, body: Value) -> RunId {
        let (st, v) = self
            .call(
                reqwest::Method::POST,
                &format!("/v1/workflows/{workflow}/runs"),
                Some(body),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "start {workflow}: {v}");
        v["run_id"]
            .as_str()
            .expect("run_id")
            .parse()
            .expect("run id")
    }
}

/// `k=v&...`, percent-encoding everything but unreserved characters.
fn form_body(fields: &[(&str, &str)]) -> String {
    let enc = |s: &str| -> String {
        s.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                    char::from(b).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect()
    };
    fields
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// A browser-like UI client: no redirects followed, cookies kept by hand.
struct Browser {
    http: reqwest::Client,
    base: String,
    csrf: String,
    session: Option<String>,
}

fn cookie_value(resp: &reqwest::Response, name: &str) -> Option<(String, String)> {
    resp.headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find(|c| c.starts_with(&format!("{name}=")))
        .map(|c| {
            let v = c
                .trim_start_matches(&format!("{name}="))
                .split(';')
                .next()
                .unwrap_or_default()
                .to_owned();
            (v, c.to_owned())
        })
}

impl Browser {
    /// Open `/ui/login` on `base` (mints the CSRF cookie).
    async fn open(base: &str) -> Self {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("client");
        let r = http
            .get(format!("{base}/ui/login"))
            .send()
            .await
            .expect("GET /ui/login");
        assert_eq!(r.status(), StatusCode::OK);
        let (csrf, _) = cookie_value(&r, "ostro_csrf").expect("csrf cookie");
        let body = r.text().await.expect("body");
        assert!(
            body.contains(&format!(r#"name="csrf" value="{csrf}""#)),
            "{body}"
        );
        Self {
            http,
            base: base.to_owned(),
            csrf,
            session: None,
        }
    }

    /// The same cookies against another node.
    fn on(&self, base: &str) -> Self {
        Self {
            http: self.http.clone(),
            base: base.to_owned(),
            csrf: self.csrf.clone(),
            session: self.session.clone(),
        }
    }

    fn cookies(&self) -> String {
        let mut c = format!("ostro_csrf={}", self.csrf);
        if let Some(s) = &self.session {
            c.push_str(&format!("; ostro_session={s}"));
        }
        c
    }

    async fn get(&self, path: &str) -> (StatusCode, Option<String>, String) {
        let r = self
            .http
            .get(format!("{}{path}", self.base))
            .header("cookie", self.cookies())
            .send()
            .await
            .expect("GET");
        let st = r.status();
        let loc = r
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        (st, loc, r.text().await.expect("body"))
    }

    /// A same-origin form POST (`csrf` appended unless `with_csrf` is false).
    async fn post(
        &self,
        path: &str,
        fields: &[(&str, &str)],
        with_csrf: bool,
    ) -> reqwest::Response {
        let mut form: Vec<(&str, &str)> = fields.to_vec();
        if with_csrf {
            form.push(("csrf", &self.csrf));
        }
        let origin = self.base.clone();
        self.http
            .post(format!("{}{path}", self.base))
            .header("cookie", self.cookies())
            .header("origin", origin)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form_body(&form))
            .send()
            .await
            .expect("POST")
    }

    /// Sign in with `bearer`; asserts the cookie attributes.
    async fn login(&mut self, bearer: &str) {
        let r = self.post("/ui/login", &[("token", bearer)], true).await;
        assert_eq!(r.status(), StatusCode::SEE_OTHER, "login");
        assert_eq!(
            r.headers().get("location").and_then(|v| v.to_str().ok()),
            Some("/ui/")
        );
        let (v, raw) = cookie_value(&r, "ostro_session").expect("session cookie");
        for attr in ["Path=/ui", "HttpOnly", "SameSite=Strict", "Max-Age="] {
            assert!(raw.contains(attr), "{raw} lacks {attr}");
        }
        assert!(!raw.contains("Secure"), "plain http loopback: {raw}");
        self.session = Some(v);
    }
}

/// Every audit record of `<data>/audit` (complete lines only).
fn audit_records(data: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(data.join("audit")) else {
        return out;
    };
    let mut paths: Vec<_> = rd.filter_map(Result::ok).map(|e| e.path()).collect();
    paths.sort();
    for p in paths {
        if let Ok(text) = std::fs::read_to_string(&p) {
            out.extend(
                text.lines()
                    .filter_map(|l| serde_json::from_str::<Value>(l).ok()),
            );
        }
    }
    out
}

struct Fx {
    s: TestServer,
    base: String,
    a_admin: Api,
    b_admin: Api,
}

async fn token_server() -> Fx {
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    cfg.auth.mode = AuthMode::Token;
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(LIMIT).await;
    tenants(s.h().meta()).await;
    let base = s.url("").trim_end_matches('/').to_owned();
    let api = |bearer: String| Api {
        http: reqwest::Client::new(),
        base: base.clone(),
        bearer,
    };
    let meta = s.h().meta();
    Fx {
        a_admin: api(put_token(meta, Some("a"), &["admin"]).await.1),
        b_admin: api(put_token(meta, Some("b"), &["admin"]).await.1),
        base,
        s,
    }
}

// ---------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_mode_ui_enforces_sessions_scopes_tenants_and_audit() {
    let Fx {
        mut s,
        base,
        a_admin,
        b_admin,
    } = token_server().await;
    let meta = s.h().meta();
    a_admin.deploy("signals").await;
    b_admin.deploy("signals").await;
    let a_run = a_admin
        .start("signals", json!({"input": {"mode": "long_wait"}}))
        .await;
    let b_run = b_admin
        .start("signals", json!({"input": {"mode": "long_wait"}}))
        .await;
    s.wait_record(&a_run, LIMIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
    let (_, a_read) = put_token(meta, Some("a"), &["read"]).await;
    let (op_id, a_op) = put_token(meta, Some("a"), &["read", "operate"]).await;

    // No session: pages redirect to the login page; actions are 401.
    let mut reader = Browser::open(&base).await;
    for path in ["/ui/", "/ui/runs", "/ui/dlq", "/ui/workflows"] {
        let (st, loc, _) = reader.get(path).await;
        assert_eq!(st, StatusCode::SEE_OTHER, "{path}");
        assert_eq!(loc.as_deref(), Some("/ui/login"), "{path}");
    }
    let r = reader
        .post(&format!("/ui/runs/{a_run}/cancel"), &[], true)
        .await;
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);

    // Sign-in: CSRF required; a bad bearer is a generic 401.
    let r = reader.post("/ui/login", &[("token", &a_read)], false).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN, "login without CSRF");
    assert!(cookie_value(&r, "ostro_session").is_none());
    let r = reader
        .post(
            "/ui/login",
            &[("token", "ostro_otk_aaaaaaaaaaaaaaaa_nope")],
            true,
        )
        .await;
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    assert!(cookie_value(&r, "ostro_session").is_none());
    assert!(r.text().await.expect("body").contains("Sign-in failed"));
    reader.login(&a_read).await;

    // The reader sees a's run (names unqualified), never b's.
    let (st, _, body) = reader.get("/ui/runs").await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert!(body.contains(&a_run.to_string()), "a's run missing: {body}");
    assert!(!body.contains(&b_run.to_string()), "b's run leaked");
    assert!(!body.contains("a/signals"), "names are shown unqualified");
    assert!(body.contains("· tenant a"), "session line: {body}");
    assert!(
        body.contains(r#" class="readonly">"#),
        "reader page is readonly"
    );
    let (st, _, _) = reader.get(&format!("/ui/runs/{a_run}")).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = reader.get(&format!("/ui/runs/{b_run}")).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "b's run is not a's");

    // The reader cannot cancel: 403, the run is unchanged.
    let r = reader
        .post(&format!("/ui/runs/{a_run}/cancel"), &[], true)
        .await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    sleep(Duration::from_millis(300)).await;
    let rec = s.run_record(&a_run);
    assert_eq!(rec.status, RunStatus::Scheduled, "{rec:?}");
    assert!(rec.cancel_reason.is_empty(), "{rec:?}");

    // An operate token cancels through the UI; the audit journal names that token.
    let mut operator = Browser::open(&base).await;
    operator.login(&a_op).await;
    let (_, _, body) = operator.get(&format!("/ui/runs/{a_run}")).await;
    assert!(!body.contains(r#" class="readonly">"#));
    let r = operator
        .post(&format!("/ui/runs/{a_run}/cancel"), &[], true)
        .await;
    assert!(
        r.status() == StatusCode::SEE_OTHER || r.status() == StatusCode::OK,
        "cancel: {}",
        r.status()
    );
    s.wait_record(&a_run, LIMIT, |r| r.status == RunStatus::Cancelled)
        .await;
    let data = s.h().data_dir().to_path_buf();
    let deadline = Instant::now() + LIMIT;
    loop {
        let recs = audit_records(&data);
        let hit = recs.iter().any(|r| {
            r["action"] == "run.cancel"
                && r["actor"] == op_id.as_str()
                && r["actor_kind"] == "token"
                && r["target"]
                    .as_str()
                    .is_some_and(|t| t.contains(&a_run.to_string()))
        });
        if hit {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no run.cancel audit record by {op_id}: {:?}",
            recs.iter()
                .filter(|r| r["action"] == "run.cancel")
                .collect::<Vec<_>>()
        );
        sleep(Duration::from_millis(100)).await;
    }
    // b's run is untouched by a's operator.
    let r = operator
        .post(&format!("/ui/runs/{b_run}/cancel"), &[], true)
        .await;
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert!(s.run_record(&b_run).cancel_reason.is_empty());

    // Revoking the token signs its session out on the next request.
    let (st, v) = a_admin
        .call(
            reqwest::Method::DELETE,
            &format!("/v1/auth/tokens/{op_id}"),
            None,
        )
        .await;
    assert!(st.is_success(), "revoke: {st} {v}");
    let (st, loc, _) = operator.get("/ui/runs").await;
    assert_eq!(st, StatusCode::SEE_OTHER);
    assert_eq!(loc.as_deref(), Some("/ui/login"));

    // Sign-out clears the cookie (CSRF required).
    let r = reader.post("/ui/logout", &[], false).await;
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let r = reader.post("/ui/logout", &[], true).await;
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
    let (v, raw) = cookie_value(&r, "ostro_session").expect("clearing cookie");
    assert!(v.is_empty() && raw.contains("Max-Age=0"), "{raw}");

    // The failed sign-in went through the audited authenticator (14-18): its coalesced
    // auth.failure record is in the journal once the server flushed it at shutdown.
    s.shutdown().await;
    let recs = audit_records(&data);
    assert!(
        recs.iter()
            .any(|r| r["action"] == "auth.failure" && r["outcome"]["result"] == "denied"),
        "no auth.failure record: {:?}",
        recs.iter()
            .filter_map(|r| r["action"].as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_from_node_1_is_accepted_by_node_2() {
    let dir = p12::tmp();
    let mut cfgs = p12::static_cluster(dir.path(), 2, 4, "ui-e2e");
    for c in &mut cfgs {
        c.auth.mode = AuthMode::Token;
    }
    let hs: Vec<ServerHandle> = p12::start_all(&cfgs).await;
    let base = |h: &ServerHandle| format!("http://{}", h.local_addr());
    let ready = p12::eventually(p12::START_T, || {
        hs.iter()
            .all(|h| h.meta().view().read(|s| s.cluster().is_some()))
    })
    .await;
    assert!(ready, "cluster never initialized");
    let (id, bearer) = put_token(hs[0].meta(), None, &["read"]).await;
    let everywhere = p12::eventually(LIMIT, || {
        hs.iter()
            .all(|h| h.meta().view().read(|s| s.token(&id).is_some()))
    })
    .await;
    assert!(everywhere, "token never replicated");

    let mut b1 = Browser::open(&base(&hs[0])).await;
    b1.login(&bearer).await;
    let (st, _, _) = b1.get("/ui/runs").await;
    assert_eq!(st, StatusCode::OK, "node 1 accepts its own cookie");
    // Node 2: same cookie (the key is shared through meta), no sign-in there.
    let b2 = b1.on(&base(&hs[1]));
    let deadline = Instant::now() + LIMIT;
    loop {
        let (st, loc, body) = b2.get("/ui/runs").await;
        if st == StatusCode::OK {
            assert!(body.contains("· system"), "session line on node 2: {body}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 2 never accepted node 1's cookie: {st} {loc:?}"
        );
        sleep(Duration::from_millis(200)).await;
    }
    // A forged cookie is refused on both.
    let mut forged = b1.on(&base(&hs[1]));
    forged.session = Some(format!("{}x", b1.session.clone().expect("session")));
    let (st, loc, _) = forged.get("/ui/runs").await;
    assert_eq!(st, StatusCode::SEE_OTHER);
    assert_eq!(loc.as_deref(), Some("/ui/login"));
    for h in hs {
        h.shutdown().await.expect("shutdown");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mode_none_keeps_the_open_phase_11_ui() {
    let s = TestServer::start_with(ServerOpts::default(), |c| c.auth.mode = AuthMode::None).await;
    s.wait_ready(LIMIT).await;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client");
    let r = http.get(s.url("/ui/runs")).send().await.expect("GET");
    assert_eq!(r.status(), StatusCode::OK);
    let body = r.text().await.expect("body");
    assert!(!body.contains("/ui/logout"), "no session line");
    assert!(!body.contains("readonly"));
    let r = http.get(s.url("/ui/login")).send().await.expect("GET");
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        r.headers().get("location").and_then(|v| v.to_str().ok()),
        Some("/ui/")
    );
    // No session key is created without token mode.
    assert!(s.h().meta().view().read(|st| {
        st.get(&MetaKey::Secret(
            ostro_server::p14::reseal::UI_SESSION_KEY_SECRET.to_owned(),
        ))
        .is_none()
    }));
}
