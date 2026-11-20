//! Object connector (06-12) over a LocalFs tempdir: every op, unknown op, the get/put caps,
//! connection access (`connection:<name>`), local HMAC presign (sign/verify, tamper, expiry)
//! and the `serve_local` round-trip.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use ostro_bridge::config::{ConnectorsConfig, EgressConfig};
use ostro_bridge::connector::{Connector, EffectCtx, GateDecision, SendGate};
use ostro_bridge::egress::Egress;
use ostro_bridge::kinds;
use ostro_bridge::meta_view::StaticMetaView;
use ostro_bridge::object::presign_local::{self, VerifyError};
use ostro_bridge::object::{
    INVALID_REQUEST, MemoryPresignKeys, NOT_FOUND, OBJECT_KIND, ObjectConnector, ServeError,
};
use ostro_bridge::secrets::{ProviderChain, SecretResolver};
use ostro_meta::records::{Connection, EgressPolicy, SealedSecret};
use ostro_objstore::PresignMethod;
use ostro_types::journal::{EffectRequest, Failure, ObjectRequest, OpOutput};
use ostro_types::live::{EffectKind, LiveEffect};

const MODULE: &str = "t/m";
const BASE: &str = "http://127.0.0.1:7070";

struct Gate(GateDecision);

#[async_trait]
impl SendGate for Gate {
    async fn check(&self, _: u16, _: &str, _: u64) -> GateDecision {
        self.0
    }
}

fn ctx() -> EffectCtx {
    EffectCtx {
        cluster_id: "c".into(),
        partition: 0,
        run_id: "run-1".into(),
        attempt: 1,
        module: MODULE.into(),
        tenant: None,
        cancel: tokio_util_cancel(),
    }
}

fn tokio_util_cancel() -> tokio_util::sync::CancellationToken {
    tokio_util::sync::CancellationToken::new()
}

fn effect(conn: &str, op: &str, key: &str, body: Option<&[u8]>) -> LiveEffect {
    LiveEffect {
        op: 7,
        request: EffectRequest::Object(ObjectRequest {
            connection: conn.into(),
            op: op.into(),
            key: key.into(),
            body: body.map(<[u8]>::to_vec),
        }),
        http_retry: None,
        scheduled_ts_nanos: 0,
        redrive: false,
    }
}

fn object_conn(name: &str, cfg: serde_json::Value) -> Connection {
    Connection {
        name: name.into(),
        kind: OBJECT_KIND.into(),
        config: cfg.to_string().into_bytes(),
        ext: vec![],
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: std::path::PathBuf,
    conn: ObjectConnector,
    keys: Arc<MemoryPresignKeys>,
}

fn fixture_with(
    meta: StaticMetaView,
    cfg: ConnectorsConfig,
) -> (ObjectConnector, Arc<MemoryPresignKeys>) {
    let meta = Arc::new(meta);
    let resolver = SecretResolver::new(meta.clone(), ProviderChain::default());
    let client = Egress::operator_client(&EgressConfig::default()).unwrap();
    let keys = Arc::new(MemoryPresignKeys::new());
    let conn =
        ObjectConnector::new(meta, resolver, client, cfg).with_presign_key_store(keys.clone());
    (conn, keys)
}

fn base_cfg() -> ConnectorsConfig {
    ConnectorsConfig {
        public_base_url: Some(BASE.into()),
        ..ConnectorsConfig::default()
    }
}

fn fixture(extra: serde_json::Value) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let mut cfg = serde_json::json!({"v": 1, "backend": "local", "root": root, "prefix": "pre"});
    for (k, v) in extra.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    let meta = StaticMetaView::new()
        .insert_connection(object_conn("files", cfg))
        .insert_connection(Connection {
            name: "db".into(),
            kind: "sql-postgres".into(),
            config: b"{\"v\":1}".to_vec(),
            ext: vec![],
        })
        .insert_egress_policy(EgressPolicy {
            name: MODULE.into(),
            allow: vec![
                "connection:files".into(),
                "connection:db".into(),
                "connection:ghost".into(),
            ],
            ext: vec![],
        });
    let (conn, keys) = fixture_with(meta, base_cfg());
    Fixture {
        _dir: dir,
        root,
        conn,
        keys,
    }
}

async fn run(f: &Fixture, e: LiveEffect) -> Result<Vec<u8>, Failure> {
    run_conn(&f.conn, e).await
}

async fn run_conn(c: &ObjectConnector, e: LiveEffect) -> Result<Vec<u8>, Failure> {
    match c.execute(&ctx(), &e, &Gate(GateDecision::Proceed)).await? {
        OpOutput::Object(b) => Ok(b),
        o => panic!("unexpected output {o:?}"),
    }
}

fn kind_of(f: &Failure) -> (bool, &str) {
    match f {
        Failure::NonRetryable(i) => (false, i.kind.as_str()),
        Failure::Retryable(i) => (true, i.kind.as_str()),
        other => panic!("{other:?}"),
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn every_op_round_trips() {
    let f = fixture(serde_json::json!({"content_type": "text/plain"}));
    assert_eq!(f.conn.kind(), EffectKind::Object);

    let put = run(&f, effect("files", "put", "a/b.txt", Some(b"hello")))
        .await
        .unwrap();
    let put: serde_json::Value = serde_json::from_slice(&put).unwrap();
    let etag = put["e_tag"].as_str().unwrap().to_owned();
    assert_eq!(etag.len(), 64);
    // The prefix is applied on disk.
    assert_eq!(std::fs::read(f.root.join("pre/a/b.txt")).unwrap(), b"hello");

    let got = run(&f, effect("files", "get", "a/b.txt", None))
        .await
        .unwrap();
    assert_eq!(got, b"hello");

    let head = run(&f, effect("files", "head", "a/b.txt", None))
        .await
        .unwrap();
    let head: serde_json::Value = serde_json::from_slice(&head).unwrap();
    assert_eq!(head["size"], 5);
    assert_eq!(head["e_tag"], etag.as_str());
    assert!(head["version"].is_null());
    let lm: jiff::Timestamp = head["last_modified"].as_str().unwrap().parse().unwrap();
    assert!(lm > jiff::Timestamp::UNIX_EPOCH);

    let del = run(&f, effect("files", "delete", "a/b.txt", None))
        .await
        .unwrap();
    assert!(del.is_empty());
    // delete is idempotent; get/head of an absent key are not_found.
    run(&f, effect("files", "delete", "a/b.txt", None))
        .await
        .unwrap();
    for op in ["get", "head"] {
        let e = run(&f, effect("files", op, "a/b.txt", None))
            .await
            .unwrap_err();
        assert_eq!(kind_of(&e), (false, NOT_FOUND), "{op}");
    }
}

#[tokio::test]
async fn unknown_op_bad_key_and_missing_body() {
    let f = fixture(serde_json::json!({}));
    let e = run(&f, effect("files", "list", "a", None))
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (false, kinds::UNSUPPORTED_OP));
    let e = run(&f, effect("files", "put", "a", None))
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (false, INVALID_REQUEST));
    for bad in ["../escape", "/abs", "a//b"] {
        let e = run(&f, effect("files", "put", bad, Some(b"x")))
            .await
            .unwrap_err();
        assert_eq!(kind_of(&e), (false, INVALID_REQUEST), "{bad}");
    }
}

#[tokio::test]
async fn caps() {
    let f = fixture(serde_json::json!({"max_get_bytes": 4, "max_put_bytes": 8}));
    let e = run(&f, effect("files", "put", "big", Some(b"123456789")))
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (false, kinds::RESPONSE_TOO_LARGE));
    run(&f, effect("files", "put", "big", Some(b"12345678")))
        .await
        .unwrap();
    let e = run(&f, effect("files", "get", "big", None))
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (false, kinds::RESPONSE_TOO_LARGE));
    run(&f, effect("files", "put", "small", Some(b"1234")))
        .await
        .unwrap();
    assert_eq!(
        run(&f, effect("files", "get", "small", None))
            .await
            .unwrap(),
        b"1234"
    );
}

#[tokio::test]
async fn connection_access_and_unknown_connections() {
    let f = fixture(serde_json::json!({}));
    // Not in the module's policy -> egress_denied (even if it does not exist).
    for name in ["other", "files2"] {
        let e = run(&f, effect(name, "get", "k", None)).await.unwrap_err();
        assert_eq!(kind_of(&e), (false, kinds::EGRESS_DENIED), "{name}");
    }
    // A module without any policy.
    let mut c = ctx();
    c.module = "t/nopolicy".into();
    let e = f
        .conn
        .execute(
            &c,
            &effect("files", "get", "k", None),
            &Gate(GateDecision::Proceed),
        )
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (false, kinds::EGRESS_DENIED));
    // Allowed but absent, or of another kind -> connection_unknown.
    for name in ["ghost", "db"] {
        let e = run(&f, effect(name, "get", "k", None)).await.unwrap_err();
        assert_eq!(kind_of(&e), (false, kinds::CONNECTION_UNKNOWN), "{name}");
    }
    // The rate-limit target is the connection name.
    let t = f
        .conn
        .target(&ctx(), &effect("files", "get", "k", None))
        .unwrap();
    assert_eq!(t.key, "files");
    assert_eq!(t.quota, None);
}

#[tokio::test]
async fn gate_abandon_cancels() {
    let f = fixture(serde_json::json!({}));
    let c = ctx();
    let e = f
        .conn
        .execute(
            &c,
            &effect("files", "put", "k", Some(b"x")),
            &Gate(GateDecision::Abandon),
        )
        .await
        .unwrap_err();
    assert_eq!(e, Failure::Cancelled);
    assert!(c.cancel.is_cancelled());
    assert!(!f.root.join("pre/k").exists());
}

fn parse_presigned(url: &str) -> (String, String, u64, String) {
    let u = url::Url::parse(url).unwrap();
    let segs: Vec<String> = u.path_segments().unwrap().map(percent_decode).collect();
    assert_eq!(&segs[..2], ["v1", "objects"], "{url}");
    let conn = segs[2].clone();
    let key = segs[3..].join("/");
    let q: std::collections::HashMap<String, String> = u.query_pairs().into_owned().collect();
    (conn, key, q["exp"].parse().unwrap(), q["sig"].clone())
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 3 <= b.len() {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

#[tokio::test]
async fn local_presign_sign_verify_tamper_expiry_and_serve() {
    let f = fixture(serde_json::json!({"presign_expiry": "10m", "content_type": "text/plain"}));
    run(
        &f,
        effect("files", "put", "dir/hello world.txt", Some(b"hi there")),
    )
    .await
    .unwrap();

    let url = run(
        &f,
        effect("files", "presign-get", "dir/hello world.txt", None),
    )
    .await
    .unwrap();
    let url = String::from_utf8(url).unwrap();
    assert!(
        url.starts_with(&format!(
            "{BASE}/v1/objects/files/dir/hello%20world.txt?exp="
        )),
        "{url}"
    );
    assert_eq!(f.keys.len(), 1, "the presign key was created on first use");
    let (conn, key, exp, sig) = parse_presigned(&url);
    assert_eq!(
        (conn.as_str(), key.as_str()),
        ("files", "dir/hello world.txt")
    );
    let n = now();
    assert!(exp > n + 590 && exp <= n + 601, "exp {exp} vs now {n}");

    // serve_local round-trip (GET): content type from put_with.
    let (status, body, ct) = f
        .conn
        .serve_local("files", &key, "GET", exp, &sig, None)
        .await
        .unwrap();
    assert_eq!(
        (status, body.as_slice(), ct.as_deref()),
        (200, &b"hi there"[..], Some("text/plain"))
    );

    // Tampering: other key, other method, other exp, flipped signature.
    let mut flipped = sig.clone().into_bytes();
    flipped[0] = if flipped[0] == b'0' { b'1' } else { b'0' };
    let flipped = String::from_utf8(flipped).unwrap();
    for (k, m, e, s) in [
        ("dir/other.txt", "GET", exp, sig.as_str()),
        (key.as_str(), "PUT", exp, sig.as_str()),
        (key.as_str(), "GET", exp + 1, sig.as_str()),
        (key.as_str(), "GET", exp, flipped.as_str()),
    ] {
        let err = f
            .conn
            .serve_local("files", k, m, e, s, Some(Bytes::from_static(b"x")))
            .await
            .unwrap_err();
        assert!(
            matches!(err, ServeError::Forbidden(VerifyError::BadSignature)),
            "{k} {m} {e}: {err:?}"
        );
        assert_eq!(err.status(), 403);
    }
    let err = f
        .conn
        .serve_local("files", &key, "DELETE", exp, &sig, None)
        .await
        .unwrap_err();
    assert_eq!(err.status(), 400);

    // presign-put + serve_local PUT stores the body (prefix applied, content type stored).
    let url = run(&f, effect("files", "presign-put", "up/new.bin", None))
        .await
        .unwrap();
    let (_, key, exp, sig) = parse_presigned(&String::from_utf8(url).unwrap());
    let (status, body, _) = f
        .conn
        .serve_local(
            "files",
            &key,
            "PUT",
            exp,
            &sig,
            Some(Bytes::from_static(b"uploaded")),
        )
        .await
        .unwrap();
    assert_eq!(status, 200);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["e_tag"].is_string());
    assert_eq!(
        std::fs::read(f.root.join("pre/up/new.bin")).unwrap(),
        b"uploaded"
    );
    assert_eq!(
        run(&f, effect("files", "get", "up/new.bin", None))
            .await
            .unwrap(),
        b"uploaded"
    );
    assert_eq!(f.keys.len(), 1, "one key per connection, reused");

    // Expired: sign with the connector's key and an exp in the past -> Expired (after MAC).
    let hmac = f.keys_value("files").await;
    let past = now() - 5;
    let s = presign_local::sign(&hmac, PresignMethod::Get, "files", &key, past).unwrap();
    let err = f
        .conn
        .serve_local("files", &key, "GET", past, &s, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ServeError::Forbidden(VerifyError::Expired)),
        "{err:?}"
    );
    // ...and the same URL a second in the future works.
    let fut = now() + 60;
    let s = presign_local::sign(&hmac, PresignMethod::Get, "files", &key, fut).unwrap();
    assert_eq!(
        f.conn
            .serve_local("files", &key, "GET", fut, &s, None)
            .await
            .unwrap()
            .0,
        200
    );

    // Absent object -> 404; absent connection -> 403 (no key for it).
    let s = presign_local::sign(&hmac, PresignMethod::Get, "files", "missing", fut).unwrap();
    assert_eq!(
        f.conn
            .serve_local("files", "missing", "GET", fut, &s, None)
            .await
            .unwrap_err()
            .status(),
        404
    );
    let err = f
        .conn
        .serve_local("ghost", "k", "GET", fut, &s, None)
        .await
        .unwrap_err();
    assert_eq!(err.status(), 403);
}

impl Fixture {
    /// The stored presign key of `connection` (test-only peek through the memory store).
    async fn keys_value(&self, connection: &str) -> Vec<u8> {
        use ostro_bridge::object::PresignKeyStore as _;
        let dummy = ostro_bridge::secrets::SecretValue::new(vec![0; 32]);
        self.keys
            .create(&presign_local::key_name(connection), dummy)
            .await
            .unwrap()
            .expose()
            .to_vec()
    }
}

#[tokio::test]
async fn sealed_presign_key_is_used_and_other_nodes_verify() {
    // The key exists as a (plaintext-v0) sealed reserved record: no new key is created, and a
    // second connector (another node) verifies URLs the first one signed.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let key = b"k-0123456789abcdef0123456789abcd".to_vec();
    let meta = StaticMetaView::new()
        .insert_connection(object_conn(
            "files",
            serde_json::json!({"v": 1, "backend": "local", "root": root}),
        ))
        .insert_egress_policy(EgressPolicy {
            name: MODULE.into(),
            allow: vec!["connection:files".into()],
            ext: vec![],
        })
        .insert_sealed_secret(SealedSecret {
            name: "__ostro/presign/files".into(),
            ciphertext: key.clone(),
            key_id: "plaintext-v0".into(),
            ext: vec![],
        });
    let (a, keys_a) = fixture_with(meta.clone(), base_cfg());
    let (b, keys_b) = fixture_with(meta, base_cfg());
    run_conn(&a, effect("files", "put", "x", Some(b"data")))
        .await
        .unwrap();
    let url = run_conn(&a, effect("files", "presign-get", "x", None))
        .await
        .unwrap();
    let (_, k, exp, sig) = parse_presigned(&String::from_utf8(url).unwrap());
    assert!(
        keys_a.is_empty() && keys_b.is_empty(),
        "the sealed key was reused"
    );
    presign_local::verify(&key, PresignMethod::Get, "files", &k, exp, &sig, now()).unwrap();
    let (status, body, _) = b
        .serve_local("files", &k, "GET", exp, &sig, None)
        .await
        .unwrap();
    assert_eq!((status, body.as_slice()), (200, &b"data"[..]));
}

#[tokio::test]
async fn local_presign_needs_public_base_url_and_key_store() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let meta = StaticMetaView::new()
        .insert_connection(object_conn(
            "files",
            serde_json::json!({"v": 1, "backend": "local", "root": root}),
        ))
        .insert_egress_policy(EgressPolicy {
            name: MODULE.into(),
            allow: vec!["connection:files".into()],
            ext: vec![],
        });
    let (no_base, _) = fixture_with(meta.clone(), ConnectorsConfig::default());
    let e = run_conn(&no_base, effect("files", "presign-get", "x", None))
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (false, kinds::UNSUPPORTED_OP));
    let meta = Arc::new(meta);
    let no_store = ObjectConnector::new(
        meta.clone(),
        SecretResolver::new(meta, ProviderChain::default()),
        Egress::operator_client(&EgressConfig::default()).unwrap(),
        base_cfg(),
    );
    let e = run_conn(&no_store, effect("files", "presign-put", "x", None))
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (false, kinds::UNSUPPORTED_OP));
    // Debug output names no secrets and no paths of keys.
    assert!(!format!("{no_store:?}").contains("presign/files"));
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn before_put_failpoint_fails_retryable_and_writes_nothing() {
    let scenario = fail::FailScenario::setup();
    let f = fixture(serde_json::json!({}));
    fail::cfg("bridge::object::before_put", "return(error)").unwrap();
    let e = run(&f, effect("files", "put", "fp", Some(b"x")))
        .await
        .unwrap_err();
    assert_eq!(kind_of(&e), (true, ostro_bridge::object::OBJECT_ERROR));
    assert!(!f.root.join("pre/fp").exists());
    fail::remove("bridge::object::before_put");
    scenario.teardown();
}
