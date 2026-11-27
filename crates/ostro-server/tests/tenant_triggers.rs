//! 14-16 Task 2: tenant-correct triggers and the tenant run index (SEC-02, D-P14-7, F11).
//!
//! - a schedule created by tenant `a` fires (cron driver and trigger-now) runs with
//!   `RunRecord.tenant == "a"` and a workflow qualified in `a`, visible to `a` and not `b`;
//! - webhook `a/orders` delivered via `POST /hooks/a/orders` starts a run in `a`; the same
//!   webhook name in `b` is independent;
//! - `ListRunsRequest{tenant: a}` over 2,000 runs (half per tenant) returns exactly `a`'s
//!   runs, newest first, through continuation tokens of the tenant index (`i1.n.`: the
//!   `x_run_index` prefix `0x04`); the oldest-first listing (budgeted scan with the tenant
//!   predicate) returns the same set.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use ostro_meta::ext::{ClusterVersion, put_cluster_version};
use ostro_meta::{
    ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, Tenant,
};
use ostro_server::config::AuthMode;
use ostro_server::p14::tokens;
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};

const LIMIT: Duration = Duration::from_secs(60);

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

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.call(reqwest::Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }

    async fn deploy(&self, guest: &str) -> [u8; 32] {
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
        sha
    }

    /// Every run of `GET /v1/runs?{filter}` (all pages) and every page token seen.
    async fn pages(&self, filter: &str, limit: u32) -> (Vec<Value>, Vec<String>) {
        let mut runs = Vec::new();
        let mut tokens = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = format!("/v1/runs?limit={limit}&{filter}");
            if let Some(t) = &token {
                q.push_str(&format!("&page_token={t}"));
            }
            let (st, v) = self.get(&q).await;
            assert_eq!(st, StatusCode::OK, "{q}: {v}");
            runs.extend(v["runs"].as_array().expect("runs").iter().cloned());
            match v["next_page_token"].as_str().filter(|t| !t.is_empty()) {
                Some(t) => {
                    tokens.push(t.to_owned());
                    token = Some(t.to_owned());
                }
                None => return (runs, tokens),
            }
        }
    }

    async fn run_ids(&self, filter: &str) -> Vec<String> {
        let (runs, _) = self.pages(filter, 1000).await;
        runs.iter()
            .map(|r| r["run_id"].as_str().expect("id").to_owned())
            .collect()
    }
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

async fn put_token(meta: &MetaHandle, tenant: Option<&str>, scopes: &[&str]) -> String {
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
    bearer.expose().to_owned()
}

struct Fx {
    s: TestServer,
    a: Api,
    b: Api,
    sys: Api,
}

async fn fixture() -> Fx {
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    cfg.auth.mode = AuthMode::Token;
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(LIMIT).await;
    let meta = s.h().meta();
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
    let base = s.url("").trim_end_matches('/').to_owned();
    let api = |bearer: String| Api {
        http: reqwest::Client::new(),
        base: base.clone(),
        bearer,
    };
    Fx {
        a: api(put_token(meta, Some("a"), &["admin"]).await),
        b: api(put_token(meta, Some("b"), &["admin"]).await),
        sys: api(put_token(meta, None, &["read"]).await),
        s,
    }
}

fn in_tenant(s: &TestServer, id: &str, t: &str, wf: &str) {
    let rec = s.run_record(&id.parse::<RunId>().expect("run id"));
    assert_eq!(rec.tenant.as_deref(), Some(t), "{id}: {rec:?}");
    assert_eq!(rec.workflow, format!("{t}/{wf}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_schedules_fire_in_their_tenant() {
    let Fx { s, a, b, sys } = fixture().await;
    a.deploy("p11-hook").await;
    b.deploy("p11-hook").await;
    let (st, v) = a
        .post(
            "/v1/schedules",
            json!({"name": "tick", "workflow": "w", "cron": "*/2 * * * * *", "input": {}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    // The cron driver fires: runs in tenant a.
    let deadline = Instant::now() + LIMIT;
    let fired = loop {
        let ids = a.run_ids("workflow=w").await;
        if !ids.is_empty() {
            break ids;
        }
        assert!(Instant::now() < deadline, "the schedule never fired");
        sleep(Duration::from_millis(200)).await;
    };
    for id in &fired {
        in_tenant(&s, id, "a", "w");
    }
    // Trigger-now: also tenant a.
    let (st, v) = a.post("/v1/schedules/tick/trigger", json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let manual = v["run_id"].as_str().expect("run_id").to_owned();
    in_tenant(&s, &manual, "a", "w");
    let (st, v) = a.post("/v1/schedules/tick/pause", json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["name"], "tick");
    // Visible to a, never to b; the system token sees the qualified schedule.
    assert!(a.run_ids("").await.contains(&manual));
    let theirs = b.run_ids("").await;
    assert!(
        fired.iter().chain([&manual]).all(|r| !theirs.contains(r)),
        "{theirs:?}"
    );
    let (_, v) = sys.get("/v1/schedules").await;
    assert_eq!(v["schedules"][0]["name"], "a/tick", "{v}");
    let (st, _) = b.get("/v1/schedules/tick").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_webhooks_deliver_to_their_tenant() {
    let Fx { s, a, b, .. } = fixture().await;
    a.deploy("p11-hook").await;
    b.deploy("p11-hook").await;
    for api in [&a, &b] {
        let (st, v) = api
            .post(
                "/v1/webhooks",
                json!({"name": "orders", "action": {"kind": "start", "workflow": "w", "input": "body"}}),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
    }
    let http = reqwest::Client::new();
    let mut got = Vec::new();
    for t in ["a", "b"] {
        let r = http
            .post(s.url(&format!("/hooks/{t}/orders")))
            .header("content-type", "application/json")
            .body(b"{}".to_vec())
            .send()
            .await
            .expect("POST hook");
        let st = r.status();
        let v: Value = r.json().await.expect("json");
        assert!(st.is_success(), "/hooks/{t}/orders: {st} {v}");
        let run = v["run_id"].as_str().expect("run_id").to_owned();
        in_tenant(&s, &run, t, "w");
        got.push(run);
    }
    assert_ne!(got[0], got[1], "independent webhooks");
    assert!(a.run_ids("").await.contains(&got[0]));
    assert!(!a.run_ids("").await.contains(&got[1]));
    assert!(b.run_ids("").await.contains(&got[1]));
    // The bare name is the default tenant's webhook (none here).
    let r = http
        .post(s.url("/hooks/orders"))
        .header("content-type", "application/json")
        .body(b"{}".to_vec())
        .send()
        .await
        .expect("POST hook");
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_run_list_uses_the_tenant_index() {
    let Fx { s, a, b, sys } = fixture().await;
    let sha = a.deploy("p11-hook").await;
    b.deploy("p11-hook").await;
    // 1,000 runs per tenant, delayed by an hour so they stay scheduled (no execution).
    let mut mine = BTreeSet::new();
    let mut theirs = BTreeSet::new();
    for i in 0..2_000u32 {
        let t = if i % 2 == 0 { "a" } else { "b" };
        let run = s
            .start_raw(&format!("{t}/w"), 1, b"{}", &sha, |st| {
                st.tenant = t.to_owned();
                st.queue = format!("{t}/default");
                st.ready_at_ms += 3_600_000;
            })
            .await;
        if t == "a" {
            mine.insert(run.to_string());
        } else {
            theirs.insert(run.to_string());
        }
    }
    // Newest first through the tenant index, in pages of 100.
    let (runs, tokens) = a.pages("order=newest", 100).await;
    assert_eq!(runs.len(), 1_000);
    assert!(
        tokens.iter().all(|t| t.starts_with("i1.n.")),
        "tenant index tokens: {:?}",
        &tokens[..tokens.len().min(3)]
    );
    assert_eq!(tokens.len(), 9, "ten pages");
    let ids: Vec<String> = runs
        .iter()
        .map(|r| r["run_id"].as_str().expect("id").to_owned())
        .collect();
    assert_eq!(ids.iter().cloned().collect::<BTreeSet<_>>(), mine);
    let created: Vec<u64> = runs
        .iter()
        .map(|r| r["created_at_ms"].as_u64().expect("created_at_ms"))
        .collect();
    assert!(created.windows(2).all(|w| w[0] >= w[1]), "newest first");
    assert!(runs.iter().all(|r| r["workflow"] == "w"), "unqualified");
    // The system token with ?tenant=a gets the same page sequence.
    let (sys_runs, sys_tokens) = sys.pages("order=newest&tenant=a", 100).await;
    let sys_ids: Vec<String> = sys_runs
        .iter()
        .map(|r| r["run_id"].as_str().expect("id").to_owned())
        .collect();
    assert_eq!(sys_ids, ids);
    assert!(sys_tokens.iter().all(|t| t.starts_with("i1.n.")));
    assert!(sys_runs.iter().all(|r| r["workflow"] == "a/w"), "qualified");
    // Oldest first: the budgeted scan with the tenant predicate, same set.
    let oldest: BTreeSet<String> = a.run_ids("").await.into_iter().collect();
    assert_eq!(oldest, mine);
    let other: BTreeSet<String> = b.run_ids("order=newest").await.into_iter().collect();
    assert_eq!(other, theirs);
}
