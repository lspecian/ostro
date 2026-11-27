//! 14-15 Task 2: the caller's principal follows Phase 13 forwarding (D-P14-6).
//!
//! Three static nodes over loopback mTLS with `[auth] mode = "token"`. Authorization happens
//! once, at the ingress node (the guard); a forwarded call carries the caller's principal in
//! `ostro-principal`, and the receiving node's node API runs it inside `PRINCIPAL.scope` of
//! that principal (observed through the node API's `ostro::p14::principal_fwd` debug event,
//! which reads the task-local back). A client-supplied `ostro-principal` on the client
//! listener is ignored: it neither widens the caller's scopes nor reaches a forwarded call.
//!
//! 1. A `trigger` token of tenant `acme` starts keyed runs through a node that does not lead
//!    their partition: the start succeeds and is forwarded, and the leader sees the token's
//!    principal (id, kind `token`, tenant `acme`).
//! 2. A `read` token is refused at the ingress node (403 `missing_scope`) before any
//!    forwarding (the ingress node's forwarded counter does not move).
//! 3. A `read` token plus `ostro-principal: p1;otk_x;*;admin` still gets 403 on a
//!    system-admin method; a forwarded start with the spoofed header carries the real
//!    caller, never `otk_x`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ostro_api::auth::{Scope, ScopeSet};
use ostro_meta::ext::{ClusterVersion, put_cluster_version};
use ostro_meta::{
    ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, Tenant,
};
use ostro_server::ServerHandle;
use ostro_server::config::AuthMode;
use ostro_server::p14::{principal_fwd, tokens};
use p12::{START_T, assignments, eventually, guest_wasm, hex, start_all, static_cluster, tmp};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};

const PARTITIONS: u16 = 4;
const SPOOF: &str = "p1;otk_xxxxxxxxxxxxxxxx;*;admin";

// ---------------------------------------------------------------------------------
// The node API's principal events (process-wide capture)
// ---------------------------------------------------------------------------------

/// One node-API call as the receiving node ran it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Seen {
    from_node: u64,
    principal: String,
    kind: String,
    tenant: String,
    forwarded: bool,
}

impl Visit for Seen {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "principal" => self.principal = format!("{value:?}"),
            "kind" => self.kind = format!("{value:?}").trim_matches('"').to_owned(),
            "tenant" => self.tenant = format!("{value:?}").trim_matches('"').to_owned(),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "kind" => self.kind = value.to_owned(),
            "tenant" => self.tenant = value.to_owned(),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "from_node" {
            self.from_node = value;
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "forwarded" {
            self.forwarded = value;
        }
    }
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Seen>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        metadata.target() == principal_fwd::TRACE_TARGET
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != principal_fwd::TRACE_TARGET {
            return;
        }
        let mut s = Seen::default();
        event.record(&mut s);
        self.0.lock().unwrap_or_else(|e| e.into_inner()).push(s);
    }
}

impl Capture {
    fn snapshot(&self) -> Vec<Seen> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

fn capture() -> Capture {
    static CAP: OnceLock<Capture> = OnceLock::new();
    CAP.get_or_init(|| {
        let cap = Capture::default();
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(cap.clone()))
            .expect("global subscriber");
        cap
    })
    .clone()
}

// ---------------------------------------------------------------------------------
// Cluster helpers
// ---------------------------------------------------------------------------------

fn leader_of(h: &ServerHandle) -> std::collections::BTreeMap<u16, u64> {
    assignments(h)
        .into_iter()
        .map(|(p, _, leader, _, _)| (p, leader))
        .collect()
}

fn all_led(h: &ServerHandle) -> bool {
    let l = leader_of(h);
    l.len() == usize::from(PARTITIONS) && l.values().all(|n| *n != 0)
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

/// Tenant `acme` (a meta record, as the tenant API of 14-17 will write it) on a cluster
/// finalized at feature level 2 (the Phase 14 records), as the 14-16 tests set it up.
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

/// A token stored through `meta` (any node; proposals go to the meta leader); returns
/// `(id, bearer)` once every node's view has it.
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
    let meta: &MetaHandle = hs[0].meta();
    let res = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Token(rec.id.clone()),
            expected_version: None,
            record: MetaRecord::Token(rec),
        })
        .await
        .expect("propose");
    assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
    let id = id.as_str().to_owned();
    let everywhere = eventually(Duration::from_secs(30), || {
        hs.iter()
            .all(|h| h.meta().view().read(|s| s.token(&id).is_some()))
    })
    .await;
    assert!(everywhere, "token {id} never reached every node");
    (id, bearer.expose().to_owned())
}

fn forwarded(h: &ServerHandle) -> u64 {
    ostro_server::route::stats(h.p13().expect("p13 context"))
        .expect("route stats")
        .forwarded()
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

    async fn deploy(&self, bearer: &str, guest: &str) {
        use sha2::{Digest, Sha256};
        let wasm = guest_wasm(guest);
        let sha = hex(&Sha256::digest(&wasm));
        let r = self
            .http
            .put(format!("{}/v1/modules/{sha}", self.base))
            .bearer_auth(bearer)
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    }

    /// A keyed start of `leaf_echo`; `(status, body)`.
    async fn start(&self, bearer: &str, key: &str, spoof: bool) -> (StatusCode, Value) {
        let mut r = self
            .http
            .post(format!("{}/v1/workflows/leaf_echo/runs", self.base))
            .bearer_auth(bearer)
            .json(&json!({ "input": { "k": key }, "idempotency_key": key }));
        if spoof {
            r = r.header("ostro-principal", SPOOF);
        }
        let r = r.send().await.expect("POST start");
        let st = r.status();
        (st, r.json().await.unwrap_or(Value::Null))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn the_principal_follows_forwarded_calls_and_is_never_taken_from_clients() {
    let cap = capture();
    let dir = tmp();
    let mut cfgs = static_cluster(dir.path(), 3, PARTITIONS, "p14-auth-forwarding");
    for c in &mut cfgs {
        c.auth.mode = AuthMode::Token;
    }
    let hs = start_all(&cfgs).await;
    assert!(
        eventually(START_T, || all_led(&hs[0])).await,
        "partitions without a leader: {:?}",
        leader_of(&hs[0])
    );

    tenant_acme(&hs).await;
    let (_, admin) = put_token(&hs, &[Scope::Admin], Some("acme")).await;
    let (trigger_id, trigger) = put_token(&hs, &[Scope::Trigger], Some("acme")).await;
    let (read_id, read) = put_token(&hs, &[Scope::Read], Some("acme")).await;

    // The ingress node: one that does not lead every partition (with three nodes at least
    // two qualify), so some keyed starts are forwarded.
    let leaders = leader_of(&hs[0]);
    let ingress = (1..=3u64)
        .find(|n| leaders.values().filter(|l| *l == n).count() < usize::from(PARTITIONS))
        .expect("a node that does not lead every partition");
    let h_in = &hs[usize::try_from(ingress - 1).expect("index")];
    let api = Api::new(h_in);
    api.deploy(&admin, "child-leaf").await;

    // 1. Trigger token: keyed starts through the ingress node until one is forwarded.
    let mut forwarded_start = None;
    for i in 0..64 {
        let key = format!("k-{i}");
        let before = forwarded(h_in);
        let (st, body) = api.start(&trigger, &key, false).await;
        if !st.is_success() {
            // A partition between leaders answers 503 for a moment; retry the next key.
            println!("{key}: {st} {body}");
            assert_ne!(st, StatusCode::FORBIDDEN, "{key}: {body}");
            assert_ne!(st, StatusCode::UNAUTHORIZED, "{key}: {body}");
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        assert!(body["run_id"].is_string(), "{key}: {body}");
        if forwarded(h_in) > before {
            forwarded_start = Some(key);
            break;
        }
        println!("{key}: served locally ({st} {body})");
    }
    let key = forwarded_start.expect("no keyed start was forwarded by the ingress node");
    let seen = |principal: &str| {
        cap.snapshot()
            .into_iter()
            .filter(|s| s.principal == principal)
            .collect::<Vec<_>>()
    };
    let at_leader = seen(&trigger_id);
    assert!(
        at_leader.iter().any(|s| s.from_node == ingress
            && s.forwarded
            && s.kind == "token"
            && s.tenant == "acme"),
        "{key}: the leader did not run the forwarded start as {trigger_id}: {at_leader:?}"
    );

    // 2. Read token: refused at the ingress node, nothing forwarded.
    let before = forwarded(h_in);
    for i in 0..8 {
        let (st, body) = api.start(&read, &format!("r-{i}"), false).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(
            body["error"]["details"]["reason"], "missing_scope",
            "{body}"
        );
    }
    assert_eq!(forwarded(h_in), before, "a denied start was forwarded");
    assert!(seen(&read_id).is_empty(), "a denied call reached a peer");

    // 3. A client-supplied ostro-principal is ignored on the client listener.
    let r = api
        .http
        .get(format!("{}/v1/cluster/status", api.base))
        .bearer_auth(&read)
        .header("ostro-principal", SPOOF)
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let r = api
        .http
        .post(format!("{}/v1/workers/tokens", api.base))
        .bearer_auth(&read)
        .header("ostro-principal", SPOOF)
        .json(&json!({}))
        .send()
        .await
        .expect("POST");
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    // A forwarded start with the spoofed header carries the real caller.
    let before_seen = seen(&trigger_id).len();
    let mut spoofed_forward = false;
    for i in 0..64 {
        let before = forwarded(h_in);
        let (st, body) = api.start(&trigger, &format!("s-{i}"), true).await;
        assert_ne!(st, StatusCode::FORBIDDEN, "{body}");
        if st.is_success() && forwarded(h_in) > before {
            spoofed_forward = true;
            break;
        }
    }
    assert!(spoofed_forward, "no spoofed start was forwarded");
    assert!(seen(&trigger_id).len() > before_seen);
    assert!(
        seen("otk_xxxxxxxxxxxxxxxx").is_empty(),
        "a client-supplied principal reached a peer"
    );

    p12::stop_all(hs).await;
}
