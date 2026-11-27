//! 14-18 Task 3: the cluster-wide audit query (tier B: three static nodes over loopback
//! mTLS, `[auth] mode = "token"`).
//!
//! - Audited actions issued through different nodes are each recorded on their ingress
//!   node (the record's `node`), once.
//! - `GET /v1/audit` from any node answers all of them, merged by `(ts_ms, node, seq)`, and
//!   cursor paging (`limit = 2`) walks exactly the same list.
//! - A tenant admin sees only its tenant's records (the guard forces the tenant filter).
//! - With one node stopped, the query still answers and lists it in `partial_nodes`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_api::auth::{Scope, ScopeSet};
use ostro_meta::ext::{ClusterVersion, put_cluster_version};
use ostro_meta::{
    ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, Tenant,
};
use ostro_server::ServerHandle;
use ostro_server::config::AuthMode;
use ostro_server::p14::tokens;
use p12::{START_T, assignments, eventually, guest_wasm, hex, start_all, static_cluster, tmp};
use reqwest::StatusCode;
use serde_json::{Value, json};

const PARTITIONS: u16 = 4;

fn all_led(h: &ServerHandle) -> bool {
    let rows = assignments(h);
    rows.len() == usize::from(PARTITIONS) && rows.iter().all(|(_, _, leader, _, _)| *leader != 0)
}

async fn propose(meta: &MetaHandle, key: MetaKey, record: MetaRecord, version: Option<u64>) {
    let res = meta
        .propose(MetaCommand::Put {
            key,
            expected_version: version,
            record,
        })
        .await
        .expect("propose");
    assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
}

/// Tenant `acme` on a cluster finalized at feature level 2 (the Phase 14 records).
async fn tenant_acme(hs: &[ServerHandle]) {
    let meta = hs[0].meta();
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
    propose(
        meta,
        MetaKey::Cluster,
        MetaRecord::Cluster(info),
        Some(cur.version),
    )
    .await;
    propose(
        meta,
        MetaKey::Tenant("acme".into()),
        MetaRecord::Tenant(Tenant {
            id: "acme".into(),
            created_at_ms: 1,
            ..Tenant::default()
        }),
        None,
    )
    .await;
    let everywhere = eventually(Duration::from_secs(30), || {
        hs.iter()
            .all(|h| h.meta().view().read(|s| s.tenant("acme").is_some()))
    })
    .await;
    assert!(everywhere, "tenant acme never reached every node");
}

/// A token visible on every node: `(id, bearer)`.
async fn put_token(
    hs: &[ServerHandle],
    scopes: &[Scope],
    tenant: Option<&str>,
) -> (String, String) {
    let (id, secret, bearer) = tokens::generate_os().expect("generate");
    let rec = ApiToken {
        id: id.as_str().to_owned(),
        hash: tokens::hash(&secret),
        scopes: ScopeSet::of(scopes).to_strings(),
        tenant: tenant.map(str::to_owned),
        created_at_ms: 1,
        expires_at_ms: None,
        ext: Vec::new(),
    };
    propose(
        hs[0].meta(),
        MetaKey::Token(rec.id.clone()),
        MetaRecord::Token(rec),
        None,
    )
    .await;
    let id = id.as_str().to_owned();
    let everywhere = eventually(Duration::from_secs(30), || {
        hs.iter()
            .all(|h| h.meta().view().read(|s| s.token(&id).is_some()))
    })
    .await;
    assert!(everywhere, "token {id} never reached every node");
    (id, bearer.expose().to_owned())
}

struct Api {
    http: reqwest::Client,
    base: String,
}

impl Api {
    fn new(h: &ServerHandle) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: format!("http://{}", h.local_addr()),
        }
    }

    async fn send(&self, rb: reqwest::RequestBuilder) -> (StatusCode, Value) {
        let r = rb.send().await.expect("request");
        let st = r.status();
        let text = r.text().await.expect("body");
        let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (st, v)
    }

    async fn deploy(&self, bearer: &str, guest: &str) -> String {
        use sha2::{Digest, Sha256};
        let wasm = guest_wasm(guest);
        let sha = hex(&Sha256::digest(&wasm));
        let (st, v) = self
            .send(
                self.http
                    .put(format!("{}/v1/modules/{sha}", self.base))
                    .bearer_auth(bearer)
                    .header("content-type", "application/wasm")
                    .body(wasm),
            )
            .await;
        assert!(st.is_success(), "deploy {guest}: {st} {v}");
        sha
    }

    async fn post(&self, bearer: &str, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(
            self.http
                .post(format!("{}{path}", self.base))
                .bearer_auth(bearer)
                .json(&body),
        )
        .await
    }

    async fn audit(&self, bearer: &str, query: &str) -> Value {
        let (st, v) = self
            .send(
                self.http
                    .get(format!("{}/v1/audit?{query}", self.base))
                    .bearer_auth(bearer),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "GET /v1/audit?{query}: {v}");
        v
    }

    /// Every record (all pages of `limit`).
    async fn audit_all(&self, bearer: &str, filter: &str, limit: u32) -> (Vec<Value>, usize) {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            let mut q = format!("limit={limit}&{filter}");
            if let Some(c) = &cursor {
                q.push_str(&format!("&cursor={c}"));
            }
            let page = self.audit(bearer, &q).await;
            pages += 1;
            let recs = page["records"].as_array().expect("records");
            assert!(recs.len() <= limit as usize);
            out.extend(recs.iter().cloned());
            match page["cursor"].as_str() {
                Some(c) => cursor = Some(c.to_owned()),
                None => return (out, pages),
            }
            assert!(pages < 1_000, "paging does not end");
        }
    }
}

fn key(r: &Value) -> (u64, u64, u64) {
    (
        r["ts_ms"].as_u64().expect("ts"),
        r["node"].as_u64().expect("node"),
        r["seq"].as_u64().expect("seq"),
    )
}

fn ours(recs: &[Value]) -> Vec<Value> {
    recs.iter()
        .filter(|r| {
            matches!(
                r["action"].as_str(),
                Some("module.upload" | "queue.pause" | "run.cancel")
            )
        })
        .cloned()
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_audit_query_merges_every_members_journal() {
    let dir = tmp();
    let mut cfgs = static_cluster(dir.path(), 3, PARTITIONS, "p14-audit-cluster");
    for c in &mut cfgs {
        c.auth.mode = AuthMode::Token;
    }
    let mut hs = start_all(&cfgs).await;
    assert!(
        eventually(START_T, || all_led(&hs[0])).await,
        "partitions without a leader"
    );
    tenant_acme(&hs).await;
    let (sys_id, sys) = put_token(&hs, &[Scope::Admin], None).await;
    let (acme_id, acme) = put_token(&hs, &[Scope::Admin], Some("acme")).await;
    let apis: Vec<Api> = hs.iter().map(Api::new).collect();

    // One audited action per ingress node (node ids are 1, 2, 3 in order).
    let sha = apis[0].deploy(&sys, "child-leaf").await;
    let (st, v) = apis[1]
        .post(&sys, "/v1/queues/q-two/pause", json!({}))
        .await;
    println!("pause via node 2: {st} {v}");
    let ghost = "0190d0e6-0000-7000-8000-000000000000";
    // (A tenant token on an unknown run id gets the tenant hook's 404 before the audit
    // reserve, like any foreign run id; the system token's attempt executes and fails.)
    let (st, v) = apis[2]
        .post(&sys, &format!("/v1/runs/{ghost}/cancel"), json!({}))
        .await;
    assert!(!st.is_success(), "cancel of an unknown run: {st} {v}");
    let (st, v) = apis[2]
        .post(&acme, "/v1/queues/q-acme/pause", json!({}))
        .await;
    println!("pause via node 3 (acme): {st} {v}");

    // Every node answers the same merged list, in (ts_ms, node, seq) order.
    let mut lists = Vec::new();
    for api in &apis {
        let page = api.audit(&sys, "limit=1000").await;
        assert_eq!(page["partial_nodes"], json!([]), "{page}");
        assert_eq!(page["cursor"], Value::Null);
        let recs = page["records"].as_array().expect("records").clone();
        let keys: Vec<_> = recs.iter().map(key).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted, "merged in (ts_ms, node, seq) order");
        lists.push(ours(&recs));
    }
    assert_eq!(lists[0], lists[1]);
    assert_eq!(lists[0], lists[2]);
    let all = &lists[0];
    let summary: Vec<(String, u64, String)> = all
        .iter()
        .map(|r| {
            (
                r["action"].as_str().unwrap_or_default().to_owned(),
                r["node"].as_u64().unwrap_or_default(),
                r["actor"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect();
    let mut sorted_summary = summary.clone();
    sorted_summary.sort();
    assert_eq!(
        sorted_summary,
        [
            ("module.upload".to_owned(), 1, sys_id.clone()),
            ("queue.pause".to_owned(), 2, sys_id.clone()),
            ("queue.pause".to_owned(), 3, acme_id.clone()),
            ("run.cancel".to_owned(), 3, sys_id.clone()),
        ],
        "each action once, on its ingress node: {all:#?}"
    );
    let upload = all
        .iter()
        .find(|r| r["action"] == "module.upload")
        .expect("upload");
    assert_eq!(upload["target"], sha.as_str());
    let cancel = all
        .iter()
        .find(|r| r["action"] == "run.cancel")
        .expect("cancel");
    assert_eq!(cancel["tenant"], Value::Null);
    let acme_pause = all
        .iter()
        .find(|r| r["action"] == "queue.pause" && r["node"] == 3)
        .expect("acme pause");
    assert_eq!(acme_pause["tenant"], "acme");
    assert_eq!(cancel["target"], ghost);
    assert!(
        cancel["outcome"]
            .as_str()
            .unwrap_or_default()
            .starts_with("failed:")
    );

    // Paging two at a time from node 2 walks the same list.
    let (paged, pages) = apis[1].audit_all(&sys, "", 2).await;
    assert!(pages >= 2, "{pages} pages");
    let full = apis[1].audit(&sys, "limit=1000").await;
    assert_eq!(&paged, full["records"].as_array().expect("records"));

    // A tenant admin sees only its tenant's records (through any node).
    for api in &apis {
        let page = api.audit(&acme, "limit=1000").await;
        let recs = page["records"].as_array().expect("records");
        assert!(!recs.is_empty());
        assert!(
            recs.iter().all(|r| r["tenant"] == "acme"),
            "tenant admin saw another tenant's records: {page}"
        );
        let actions: Vec<&str> = recs.iter().filter_map(|r| r["action"].as_str()).collect();
        assert_eq!(actions, ["queue.pause"], "{page}");
    }
    // Filters: by action and by actor.
    let page = apis[0].audit(&sys, "action=queue.*&limit=1000").await;
    let n = page["records"].as_array().expect("records").len();
    assert_eq!(n, 2, "{page}");
    let page = apis[0]
        .audit(&sys, &format!("actor={acme_id}&limit=1000"))
        .await;
    assert!(
        page["records"]
            .as_array()
            .expect("records")
            .iter()
            .all(|r| r["actor"] == acme_id.as_str())
    );

    // One member stopped: the query still answers, and names it.
    let h3 = hs.pop().expect("node 3");
    h3.shutdown().await.expect("shutdown node 3");
    let page = apis[0].audit(&sys, "limit=1000").await;
    assert_eq!(page["partial_nodes"], json!([3]), "{page}");
    let nodes: BTreeMap<u64, usize> = ours(page["records"].as_array().expect("records"))
        .iter()
        .fold(BTreeMap::new(), |mut m, r| {
            *m.entry(r["node"].as_u64().unwrap_or_default()).or_default() += 1;
            m
        });
    assert_eq!(nodes, BTreeMap::from([(1, 1), (2, 1)]), "{page}");
    for h in hs {
        h.shutdown().await.expect("shutdown");
    }
}
