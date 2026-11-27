//! 14-19 Task 1: signed-module enforcement (SEC-03, D-P14-10 B1a-c).
//!
//! Single-node servers (open API, the dev principal = system admin) are driven over REST:
//! uploads carry `Ostro-Module-Signature` headers, the policy and keys go through
//! `/v1/security/modules/*`. Tenant-level tightening (ext 116, no wire field in 1.0) and the
//! tenant-principal rules of key removal call `SecurityService` inside `auth::scope`.
//! The last test runs three static nodes where only node 1 has `[security.modules]`.
//!
//! Signatures come from in-test signers (aws-lc-rs Ed25519 legacy-`Ed` minisign, ECDSA P-256
//! cosign keyed), as in `p14_signing.rs`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::time::Duration;

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, Ed25519KeyPair, KeyPair};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ostro_api::auth::{self, Principal, ScopeSet};
use ostro_api::phase14::SecurityApi;
use ostro_meta::ext::{
    ClusterVersion, get_module_signatures, get_module_uploaders, get_security_policy,
    put_cluster_version,
};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_server::ServerHandle;
use ostro_server::p14::security_api::{SecurityService, SeedStep, seed_once};
use ostro_server::p14::signing::{key_fingerprint, parse_trusted_key};
use ostro_server::p14::{self};
use p12::{eventually, guest_wasm, hex, single, start, start_all, static_cluster, stop_all, tmp};
use reqwest::StatusCode;
use serde_json::{Value, json};

const T: Duration = Duration::from_secs(60);
const V1: &str = "p10-replay-v1";
const V2: &str = "p10-replay-v2";
const V3: &str = "p10-replay-v3";
const WF: &str = "replay_demo";

// ---------------------------------------------------------------------------------
// Signers
// ---------------------------------------------------------------------------------

/// Legacy `Ed` minisign signer.
struct Mini {
    kp: Ed25519KeyPair,
    key_id: [u8; 8],
}

impl Mini {
    fn new(key_id: [u8; 8]) -> Self {
        let doc = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("keygen");
        Self {
            kp: Ed25519KeyPair::from_pkcs8(doc.as_ref()).expect("pkcs8"),
            key_id,
        }
    }

    fn public(&self) -> String {
        let mut bin = b"Ed".to_vec();
        bin.extend_from_slice(&self.key_id);
        bin.extend_from_slice(self.kp.public_key().as_ref());
        B64.encode(bin)
    }

    fn id(&self) -> String {
        self.key_id
            .iter()
            .rev()
            .map(|b| format!("{b:02X}"))
            .collect()
    }

    fn fingerprint(&self) -> [u8; 32] {
        key_fingerprint(&parse_trusted_key(&self.public()).expect("key"))
    }

    fn sign(&self, blob: &[u8]) -> String {
        let sig = self.kp.sign(blob);
        let mut bin = b"Ed".to_vec();
        bin.extend_from_slice(&self.key_id);
        bin.extend_from_slice(sig.as_ref());
        let tc = "timestamp:1700000000\tfile:module.wasm";
        let mut global = sig.as_ref().to_vec();
        global.extend_from_slice(tc.as_bytes());
        format!(
            "untrusted comment: ostro test\n{}\ntrusted comment: {tc}\n{}\n",
            B64.encode(bin),
            B64.encode(self.kp.sign(&global).as_ref())
        )
    }
}

/// cosign keyed signer.
struct Cosign {
    kp: EcdsaKeyPair,
}

impl Cosign {
    fn new() -> Self {
        let doc =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &SystemRandom::new())
                .expect("keygen");
        Self {
            kp: EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, doc.as_ref())
                .expect("pkcs8"),
        }
    }

    fn pem(&self) -> String {
        let der = self.kp.public_key().as_der().expect("der");
        let b = B64.encode(der.as_ref());
        let mut out = String::from("-----BEGIN PUBLIC KEY-----\n");
        for c in b.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(c).expect("ascii"));
            out.push('\n');
        }
        out.push_str("-----END PUBLIC KEY-----\n");
        out
    }

    fn sign(&self, blob: &[u8]) -> String {
        B64.encode(
            self.kp
                .sign(&SystemRandom::new(), blob)
                .expect("sign")
                .as_ref(),
        )
    }
}

// ---------------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------------

fn sha(b: &[u8]) -> [u8; 32] {
    let d = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, b);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

struct Node {
    base: String,
    http: reqwest::Client,
}

impl Node {
    fn of(h: &ServerHandle) -> Self {
        Self {
            base: format!("http://{}", h.local_addr()),
            http: reqwest::Client::new(),
        }
    }

    async fn send(&self, rb: reqwest::RequestBuilder) -> (StatusCode, Value) {
        let r = tokio::time::timeout(T, rb.send())
            .await
            .expect("request timed out")
            .expect("request");
        let st = r.status();
        let text = r.text().await.expect("body");
        let v = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        (st, v)
    }

    async fn upload(
        &self,
        wasm: &[u8],
        tenant: Option<&str>,
        sigs: &[String],
    ) -> (StatusCode, Value) {
        let q = tenant.map(|t| format!("?tenant={t}")).unwrap_or_default();
        let mut rb = self
            .http
            .put(format!("{}/v1/modules/{}{q}", self.base, hex(&sha(wasm))))
            .header("content-type", "application/wasm")
            .body(wasm.to_vec());
        for s in sigs {
            rb = rb.header("ostro-module-signature", B64.encode(s.as_bytes()));
        }
        self.send(rb).await
    }

    async fn json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut rb = self.http.request(method, format!("{}{path}", self.base));
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        self.send(rb).await
    }

    async fn add_key(&self, public: &str, tenant: Option<&str>) -> (StatusCode, Value) {
        self.json(
            reqwest::Method::POST,
            "/v1/security/modules/keys",
            Some(json!({"alg": "", "key_id": "auto", "public_key": public, "tenant": tenant})),
        )
        .await
    }

    async fn require(&self, on: bool) {
        let (st, v) = self
            .json(
                reqwest::Method::PUT,
                "/v1/security/modules/policy",
                Some(json!({"require_signed_modules": on})),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "set policy: {v}");
        assert_eq!(v["require_signed_modules"], on);
    }

    async fn activate(&self, wf: &str, version: u32) -> (StatusCode, Value) {
        self.json(
            reqwest::Method::PUT,
            &format!("/v1/workflows/{}/active", wf.replace('/', "%2F")),
            Some(json!({"version": version})),
        )
        .await
    }

    async fn attach(&self, wasm: &[u8], sig: &str) -> (StatusCode, Value) {
        self.json(
            reqwest::Method::POST,
            &format!("/v1/modules/{}/signatures", hex(&sha(wasm))),
            Some(json!({"signatures": [B64.encode(sig.as_bytes())]})),
        )
        .await
    }

    async fn start_run(&self, wf: &str, body: Value) -> (StatusCode, Value) {
        self.json(
            reqwest::Method::POST,
            &format!("/v1/workflows/{wf}/runs"),
            Some(body),
        )
        .await
    }

    async fn run_status(&self, id: &str) -> String {
        let (_, v) = self
            .json(reqwest::Method::GET, &format!("/v1/runs/{id}"), None)
            .await;
        v["status"].as_str().unwrap_or_default().to_owned()
    }
}

/// Finalize feature level 2 (ext 113/115/116 emitters), tolerating a concurrent finalize.
async fn ensure_level2(meta: &MetaHandle) {
    for _ in 0..50 {
        let cur = meta.view().read(|s| s.cluster().cloned()).expect("cluster");
        let fin = ostro_meta::ext::get_cluster_version(&cur.value.ext)
            .ok()
            .flatten()
            .map_or(0, |c| c.finalized);
        if fin >= 2 {
            return;
        }
        let mut info = cur.value.clone();
        info.ext = put_cluster_version(
            &info.ext,
            &ClusterVersion {
                finalized: 2,
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
        if matches!(r, MetaResult::Ok { .. }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not finalize level 2");
}

fn has_module(h: &ServerHandle, wasm: &[u8]) -> bool {
    let s = sha(wasm);
    h.meta().view().read(|m| m.module(&s).is_some())
}

fn module_sigs(h: &ServerHandle, wasm: &[u8]) -> Vec<ostro_meta::ext::ModuleSig> {
    let s = sha(wasm);
    h.meta()
        .view()
        .read(|m| m.module(&s).map(|r| r.value.ext.clone()))
        .and_then(|e| get_module_signatures(&e).expect("ext 113"))
        .map(|m| m.sigs)
        .unwrap_or_default()
}

fn security(h: &ServerHandle) -> SecurityService {
    SecurityService::new(&p14::ctx_of(h.control()).expect("ctx").expect("attached"))
}

async fn single_node() -> (ServerHandle, tempfile::TempDir) {
    let dir = tmp();
    let h = start(single(dir.path(), 2)).await.expect("start");
    ensure_level2(h.meta()).await;
    (h, dir)
}

async fn create_tenant(n: &Node, id: &str) {
    let (st, v) = n
        .json(
            reqwest::Method::POST,
            "/v1/tenants",
            Some(json!({"id": id})),
        )
        .await;
    assert!(st.is_success(), "create tenant {id}: {st} {v}");
}

// ---------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------

/// Off → unsigned OK; on → unsigned 403 / bad 400 / minisign + cosign OK with evidence; the
/// active unsigned version keeps serving and its in-flight run completes; a pre-enforcement
/// version needs attached signatures before activation, explicit starts, migrate and rerun;
/// removing or replacing a key invalidates evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enforcement_matrix_single_node() {
    let (h, _dir) = single_node().await;
    let n = Node::of(&h);
    let (v1, v2, v3) = (guest_wasm(V1), guest_wasm(V2), guest_wasm(V3));
    let smoke = guest_wasm("smoke");
    let signals = guest_wasm("signals");

    // Policy off: unsigned uploads are accepted (v1 goes active, v2 registered).
    for w in [&v1, &v2] {
        let (st, v) = n.upload(w, None, &[]).await;
        assert!(st.is_success(), "unsigned upload while off: {st} {v}");
    }
    // A run on v1 in flight across the switch.
    let (st, run) = n.start_run(WF, json!({"input": {"sleep_s": 2}})).await;
    assert_eq!(st, StatusCode::CREATED, "{run}");
    let inflight = run["run_id"].as_str().expect("run id").to_owned();

    // Keys and enforcement.
    let m = Mini::new(*b"ostro-m1");
    let c = Cosign::new();
    let (st, v) = n.add_key(&m.public(), None).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (st, v) = n.add_key(&m.public(), None).await;
    assert_eq!(st, StatusCode::CONFLICT, "duplicate key id: {v}");
    let (st, v) = n.add_key(&c.pem(), None).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    n.require(true).await;
    let (_, pol) = n
        .json(reqwest::Method::GET, "/v1/security/modules/policy", None)
        .await;
    assert_eq!(
        pol["trusted_keys"].as_array().map(Vec::len),
        Some(2),
        "{pol}"
    );

    // Unsigned → 403, nothing stored.
    let (st, v) = n.upload(&smoke, None, &[]).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    assert_eq!(reason(&v), "signature_required");
    assert!(
        !has_module(&h, &smoke),
        "rejected upload left a module record"
    );
    // Signature over other bytes → 400, nothing stored.
    let (st, v) = n.upload(&smoke, None, &[m.sign(&signals)]).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(reason(&v), "signature_invalid");
    assert_eq!(v["error"]["details"]["key_ids"][0], m.id());
    assert!(!has_module(&h, &smoke));
    // Valid minisign → stored with evidence.
    let (st, v) = n.upload(&smoke, None, &[m.sign(&smoke)]).await;
    assert!(st.is_success(), "{st} {v}");
    let ev = module_sigs(&h, &smoke);
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].key_fingerprint, m.fingerprint());
    assert_eq!(ev[0].added_by, "dev");
    // Valid cosign keyed → stored with evidence.
    let (st, v) = n.upload(&v3, None, &[c.sign(&v3)]).await;
    assert!(st.is_success(), "{st} {v}");
    assert_eq!(module_sigs(&h, &v3).len(), 1);

    // The active unsigned v1 keeps serving; the in-flight run completes.
    let (st, v) = n.start_run(WF, json!({"input": {"sleep_s": 0}})).await;
    assert_eq!(st, StatusCode::CREATED, "start on the active version: {v}");
    let mut done = false;
    for _ in 0..600 {
        if n.run_status(&inflight).await == "completed" {
            done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(done, "the in-flight run did not complete after enforcement");

    // v2 (unsigned, before enforcement): activation, explicit start and migrate refused.
    let (st, v) = n.activate(WF, 2).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    assert_eq!(reason(&v), "signature_required");
    let (_, before) = n
        .json(reqwest::Method::GET, "/v1/runs?workflow=replay_demo", None)
        .await;
    let (st, v) = n
        .start_run(WF, json!({"input": {"sleep_s": 0}, "version": 2}))
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "explicit version start: {v}");
    let (_, after) = n
        .json(reqwest::Method::GET, "/v1/runs?workflow=replay_demo", None)
        .await;
    assert_eq!(
        before["runs"], after["runs"],
        "a refused start created a run"
    );
    let (st, v) = n
        .json(
            reqwest::Method::POST,
            "/v1/runs/migrate",
            Some(
                json!({"workflow": WF, "from": {"version": 1}, "to_version": 2, "dry_run": false}),
            ),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "migrate to unsigned: {v}");
    assert_eq!(reason(&v), "signature_required");
    // Rerun of the completed v1 run with an explicit unsigned version.
    let (st, v) = n
        .json(
            reqwest::Method::POST,
            &format!("/v1/runs/{inflight}/rerun"),
            Some(json!({"from": {"seq": 1}, "version": {"version": 2}})),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN, "rerun on unsigned: {v}");

    // Attach a signature to v2, then it activates.
    let (st, v) = n.attach(&v2, &m.sign(&v1)).await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "signature over other bytes: {v}"
    );
    let (st, v) = n.attach(&v2, &m.sign(&v2)).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["key_ids"], json!([m.id()]));
    let (st, v) = n.activate(WF, 2).await;
    assert_eq!(st, StatusCode::OK, "activation after attach: {v}");
    // v3 (cosign) activates too; back to v2 works while m is trusted.
    let (st, v) = n.activate(WF, 3).await;
    assert_eq!(st, StatusCode::OK, "{v}");

    // Removing m: v2 (signed only by m) can no longer be activated.
    let (st, v) = n
        .json(
            reqwest::Method::DELETE,
            &format!("/v1/security/modules/keys/{}", m.id()),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (st, v) = n.activate(WF, 2).await;
    assert_eq!(
        st,
        StatusCode::FORBIDDEN,
        "removed key still satisfies: {v}"
    );
    // A different key with the same minisign key id does not revive the evidence.
    let m2 = Mini::new(*b"ostro-m1");
    assert_eq!(m2.id(), m.id());
    let (st, v) = n.add_key(&m2.public(), None).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (st, v) = n.activate(WF, 2).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "same key id, other key: {v}");
    // An explicit start on the active (cosign-signed) v3 works.
    let (st, v) = n
        .start_run(WF, json!({"input": {"sleep_s": 0}, "version": 3}))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");

    // Policy off again: unsigned uploads work, signatures still verified when given.
    n.require(false).await;
    let (st, v) = n.upload(&signals, None, &[m.sign(&signals)]).await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "invalid supplied signature while off: {v}"
    );
    let (st, v) = n.upload(&signals, None, &[]).await;
    assert!(st.is_success(), "{st} {v}");
    h.shutdown().await.expect("shutdown");
}

/// Tenant tightening (ext 116), cross-tenant re-upload (B1b) and tenant-only keys (B1c).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_tightening_and_cross_tenant() {
    let (h, _dir) = single_node().await;
    let n = Node::of(&h);
    let svc = security(&h);
    let (v1, v2, v3) = (guest_wasm(V1), guest_wasm(V2), guest_wasm(V3));
    let smoke = guest_wasm("smoke");
    let signals = guest_wasm("signals");
    create_tenant(&n, "a").await;
    create_tenant(&n, "b").await;

    // Before any enforcement: a registers v1 (active), v2, v3; b registers v1 (active), v2.
    for (w, t) in [(&v1, "a"), (&v2, "a"), (&v3, "a"), (&v1, "b"), (&v2, "b")] {
        let (st, v) = n.upload(w, Some(t), &[]).await;
        assert!(st.is_success(), "{t}: {st} {v}");
    }
    // A tenant-only key of a signs v2.
    let ka = Mini::new(*b"tenant-a");
    let (st, v) = n.add_key(&ka.public(), Some("a")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert!(
        v["trusted_keys"]
            .as_array()
            .expect("keys")
            .iter()
            .any(|k| k["tenant"] == "a" && k["key_id"] == ka.id()),
        "{v}"
    );
    let (st, v) = n.attach(&v2, &ka.sign(&v2)).await;
    assert_eq!(st, StatusCode::OK, "{v}");

    // Tighten a and b (the cluster stays off).
    auth::scope(Principal::system(), async {
        svc.set_tenant_require("a", Some(true)).await.expect("a");
        svc.set_tenant_require("b", Some(true)).await.expect("b");
    })
    .await;

    // a activates v2 with its own key's evidence.
    let (st, v) = n.activate("a/replay_demo", 2).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    // b cannot: the evidence was made by a's tenant-only key.
    let (st, v) = n.activate("b/replay_demo", 2).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    assert_eq!(reason(&v), "signature_required");

    // B1b: b re-uploads a's v3 unsigned → 403 and no b records, uploaders unchanged.
    let uploaders = |h: &ServerHandle| {
        let s = sha(&v3);
        h.meta().view().read(|m| {
            m.module(&s)
                .map(|r| get_module_uploaders(&r.value.ext).ok().flatten())
        })
    };
    let before = uploaders(&h);
    let (st, v) = n.upload(&v3, Some("b"), &[]).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    let (wv, act) = h.meta().view().read(|m| {
        (
            m.workflow("b/replay_demo", 3).is_some(),
            m.active("b/replay_demo").map(|a| a.value.version),
        )
    });
    assert!(!wv, "a refused upload registered b/replay_demo@3");
    assert_eq!(act, Some(1), "b's active row moved");
    assert_eq!(uploaders(&h), before, "ModuleUploaders changed");
    // b with a's tenant-only key signature → 400 (not a key of b).
    let (st, v) = n.upload(&v3, Some("b"), &[ka.sign(&v3)]).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");

    // Tightening only applies to the tenant: relax a (system), a uploads unsigned, b cannot.
    auth::scope(Principal::system(), async {
        svc.set_tenant_require("a", None).await.expect("relax a");
    })
    .await;
    let (st, v) = n.upload(&smoke, Some("a"), &[]).await;
    assert!(
        st.is_success(),
        "a unsigned while only b is tightened: {st} {v}"
    );
    let (st, v) = n.upload(&signals, Some("b"), &[]).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");
    assert!(!has_module(&h, &signals));

    // Key removal by tenant principals: another tenant's key answers 404, own key works.
    let admin_b = Principal::token("otk_tenantadminb000", Some("b".into()), ScopeSet::admin());
    let admin_a = Principal::token("otk_tenantadmina000", Some("a".into()), ScopeSet::admin());
    let r = auth::scope(admin_b.clone(), svc.remove_trusted_key(ka.id())).await;
    assert_eq!(r.expect_err("404").code.http_status().as_u16(), 404);
    let r = auth::scope(admin_b, svc.remove_trusted_key(format!("a:{}", ka.id()))).await;
    assert_eq!(r.expect_err("404").code.http_status().as_u16(), 404);
    // A tenant principal may only tighten.
    let r = auth::scope(admin_a.clone(), svc.set_tenant_require("a", Some(false))).await;
    assert_eq!(r.expect_err("403").code.http_status().as_u16(), 403);
    let r = auth::scope(admin_a.clone(), svc.set_tenant_require("b", Some(true))).await;
    assert_eq!(r.expect_err("404").code.http_status().as_u16(), 404);
    let p = auth::scope(admin_a, svc.remove_trusted_key(ka.id()))
        .await
        .expect("own key");
    assert!(p.trusted_keys.iter().all(|k| k.key_id != ka.id()));
    h.shutdown().await.expect("shutdown");
}

/// Three static nodes; only node 1 has `[security.modules]`: the policy reaches meta and
/// node 3 (no section) enforces it; drift is detected once meta changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_wide_policy_from_meta() {
    let root = tmp();
    let m = Mini::new(*b"cluster1");
    let mut cfgs = static_cluster(root.path(), 3, 2, "p14-signed");
    cfgs[0].security.modules.require_signed = true;
    cfgs[0].security.modules.minisign_keys = vec![m.public()];
    let hs = start_all(&cfgs).await;
    ensure_level2(hs[0].meta()).await;
    // The seed lands in meta and reaches node 3.
    let seeded = eventually(T, || {
        hs[2].meta().view().read(|s| {
            s.cluster()
                .and_then(|c| get_security_policy(&c.value.ext).ok().flatten())
                .is_some_and(|p| p.require_signed_modules && p.trusted_keys.len() == 1)
        })
    })
    .await;
    assert!(
        seeded,
        "the [security.modules] seed never reached node 3's meta"
    );

    let n3 = Node::of(&hs[2]);
    let smoke = guest_wasm("smoke");
    let (st, v) = n3.upload(&smoke, None, &[]).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "unsigned through node 3: {v}");
    assert_eq!(reason(&v), "signature_required");
    for h in &hs {
        assert!(!has_module(h, &smoke), "rejected upload left a record");
    }
    let (st, v) = n3.upload(&smoke, None, &[m.sign(&smoke)]).await;
    assert!(st.is_success(), "signed through node 3: {st} {v}");

    // Node 1's section now differs from meta: drift (meta wins).
    let ctx1 = p14::ctx_of(hs[0].control())
        .expect("ctx")
        .expect("attached");
    assert_eq!(seed_once(&ctx1).await, SeedStep::InSync);
    let ctx3 = p14::ctx_of(hs[2].control())
        .expect("ctx")
        .expect("attached");
    assert_eq!(seed_once(&ctx3).await, SeedStep::NoSection);
    Node::of(&hs[1]).require(false).await;
    assert!(
        eventually(T, || hs[0].meta().view().read(|s| {
            s.cluster()
                .and_then(|c| get_security_policy(&c.value.ext).ok().flatten())
                .is_some_and(|p| !p.require_signed_modules)
        }))
        .await
    );
    assert_eq!(seed_once(&ctx1).await, SeedStep::Drift);
    // Meta wins on node 1 too.
    let (st, v) = Node::of(&hs[0])
        .upload(&guest_wasm("signals"), None, &[])
        .await;
    assert!(
        st.is_success(),
        "meta policy (off) applies on node 1: {st} {v}"
    );
    let svc = SecurityService::new(&ctx1);
    let p = auth::scope(Principal::system(), svc.get_module_policy())
        .await
        .expect("policy");
    assert!(!p.require_signed_modules);
    stop_all(hs).await;
}
