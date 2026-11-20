//! Object connector (06-12) over the S3 backend.
//!
//! - Always: S3 presign is pure signing (no I/O), so `presign-get`/`presign-put` are checked
//!   against an unreachable endpoint; credentials resolve from `{{secret:..}}` templates and
//!   never appear in failures or `Debug` output.
//! - Env-gated (CI SeaweedFS, D-FX5; `OSTRO_TEST_S3_URL` / `_ACCESS_KEY` / `_SECRET_KEY` /
//!   `_BUCKET`): every op plus a presigned GET fetched and a presigned PUT uploaded by a plain
//!   reqwest client. Unset: a loud SKIP and a pass.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use ostro_bridge::config::{ConnectorsConfig, EgressConfig};
use ostro_bridge::connector::{Connector, EffectCtx, GateDecision, SendGate};
use ostro_bridge::egress::Egress;
use ostro_bridge::kinds;
use ostro_bridge::meta_view::StaticMetaView;
use ostro_bridge::object::{NOT_FOUND, OBJECT_ERROR, OBJECT_KIND, ObjectConnector};
use ostro_bridge::secrets::{ProviderChain, ProviderError, SecretProvider, SecretResolver};
use ostro_meta::records::{Connection, EgressPolicy};
use ostro_types::journal::{EffectRequest, Failure, ObjectRequest, OpOutput};
use ostro_types::live::LiveEffect;
use secrecy::SecretBox;

const MODULE: &str = "t/m";
const CANARY_SK: &str = "CANARY-sk-5e2b8f1c9a7d";

struct MapProvider(BTreeMap<String, Vec<u8>>);

#[async_trait]
impl SecretProvider for MapProvider {
    fn name(&self) -> &'static str {
        "map"
    }
    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        Ok(self
            .0
            .get(name)
            .map(|v| SecretBox::new(v.clone().into_boxed_slice())))
    }
}

struct Gate;

#[async_trait]
impl SendGate for Gate {
    async fn check(&self, _: u16, _: &str, _: u64) -> GateDecision {
        GateDecision::Proceed
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
        cancel: tokio_util::sync::CancellationToken::new(),
    }
}

fn effect(op: &str, key: &str, body: Option<&[u8]>) -> LiveEffect {
    LiveEffect {
        op: 1,
        request: EffectRequest::Object(ObjectRequest {
            connection: "s3files".into(),
            op: op.into(),
            key: key.into(),
            body: body.map(<[u8]>::to_vec),
        }),
        http_retry: None,
        scheduled_ts_nanos: 0,
        redrive: false,
    }
}

fn connector(cfg: serde_json::Value, secret: &str) -> ObjectConnector {
    let meta = Arc::new(
        StaticMetaView::new()
            .insert_connection(Connection {
                name: "s3files".into(),
                kind: OBJECT_KIND.into(),
                config: cfg.to_string().into_bytes(),
                ext: vec![],
            })
            .insert_egress_policy(EgressPolicy {
                name: MODULE.into(),
                allow: vec!["connection:s3files".into()],
                ext: vec![],
            }),
    );
    let provider = MapProvider(BTreeMap::from([(
        "s3-secret".to_owned(),
        secret.as_bytes().to_vec(),
    )]));
    let resolver = SecretResolver::new(meta.clone(), ProviderChain::new(vec![Arc::new(provider)]));
    let client = Egress::operator_client(&EgressConfig::default()).unwrap();
    ObjectConnector::new(meta, resolver, client, ConnectorsConfig::default())
}

async fn run(c: &ObjectConnector, e: LiveEffect) -> Result<Vec<u8>, Failure> {
    match c.execute(&ctx(), &e, &Gate).await? {
        OpOutput::Object(b) => Ok(b),
        o => panic!("unexpected output {o:?}"),
    }
}

fn info(f: &Failure) -> (bool, String, String) {
    match f {
        Failure::NonRetryable(i) => (false, i.kind.clone(), format!("{i:?}")),
        Failure::Retryable(i) => (true, i.kind.clone(), format!("{i:?}")),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn s3_presign_is_offline_and_credentials_stay_secret() {
    let c = connector(
        serde_json::json!({
            "v": 1, "backend": "s3", "endpoint": "http://127.0.0.1:1", "bucket": "b",
            "allow_http": true, "prefix": "p", "presign_expiry": "30m",
            "content_type": "text/csv",
            "credentials": {"access_key_id": "AKIATESTKEY", "secret_access_key": "{{secret:s3-secret}}"}
        }),
        CANARY_SK,
    );
    let url = String::from_utf8(
        run(&c, effect("presign-get", "a/b.csv", None))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(url.starts_with("http://127.0.0.1:1/b/p/a/b.csv?"), "{url}");
    assert!(url.contains("X-Amz-Expires=1800"), "{url}");
    assert!(url.contains("X-Amz-Credential=AKIATESTKEY"), "{url}");
    assert!(!url.contains(CANARY_SK), "{url}");
    let url = String::from_utf8(
        run(&c, effect("presign-put", "a/b.csv", None))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(
        url.contains("X-Amz-SignedHeaders=content-type%3Bhost"),
        "{url}"
    );

    // A real request to the unreachable endpoint fails retryably, without the secret.
    let e = run(&c, effect("head", "a/b.csv", None)).await.unwrap_err();
    let (retry, kind, text) = info(&e);
    assert!(retry, "{text}");
    assert_eq!(kind, OBJECT_ERROR);
    assert!(!text.contains(CANARY_SK), "{text}");
    assert!(!format!("{c:?}").contains(CANARY_SK));
}

#[tokio::test]
async fn missing_credential_secret_fails_before_any_request() {
    let c = connector(
        serde_json::json!({
            "v": 1, "backend": "s3", "endpoint": "http://127.0.0.1:1", "bucket": "b",
            "allow_http": true,
            "credentials": {"access_key_id": "AKIATESTKEY", "secret_access_key": "{{secret:absent}}"}
        }),
        CANARY_SK,
    );
    let e = run(&c, effect("get", "k", None)).await.unwrap_err();
    assert_eq!(info(&e).1, kinds::SECRET_NOT_FOUND);
    // plain http without allow_http is a config error.
    let c = connector(
        serde_json::json!({"v": 1, "backend": "s3", "endpoint": "http://127.0.0.1:1", "bucket": "b"}),
        CANARY_SK,
    );
    let e = run(&c, effect("get", "k", None)).await.unwrap_err();
    assert_eq!(info(&e).1, kinds::CONNECTION_UNKNOWN);
}

// ---------------------------------------------------------------------------------------------
// SeaweedFS leg (CI; human_needed locally)
// ---------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_every_op_and_presign_against_external_store() {
    let Ok(endpoint) = std::env::var("OSTRO_TEST_S3_URL") else {
        println!("SKIP: OSTRO_TEST_S3_URL unset (human_needed / CI SeaweedFS)");
        return;
    };
    let var = |n: &str| {
        std::env::var(n).unwrap_or_else(|_| panic!("{n} must be set with OSTRO_TEST_S3_URL"))
    };
    let nanos = std::time::SystemTime::now() // clock-exempt: test namespace uniqueness only
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let c = connector(
        serde_json::json!({
            "v": 1, "backend": "s3", "endpoint": endpoint, "bucket": var("OSTRO_TEST_S3_BUCKET"),
            "region": "us-east-1", "url_style": "path",
            "allow_http": endpoint.starts_with("http://"),
            "prefix": format!("bridge-object/{}-{nanos}", std::process::id()),
            "content_type": "text/plain",
            "credentials": {"access_key_id": var("OSTRO_TEST_S3_ACCESS_KEY"),
                            "secret_access_key": "{{secret:s3-secret}}"}
        }),
        &var("OSTRO_TEST_S3_SECRET_KEY"),
    );

    let put = run(&c, effect("put", "a/b.txt", Some(b"hello s3")))
        .await
        .unwrap();
    let put: serde_json::Value = serde_json::from_slice(&put).unwrap();
    assert!(put["e_tag"].is_string(), "{put}");
    assert_eq!(
        run(&c, effect("get", "a/b.txt", None)).await.unwrap(),
        b"hello s3"
    );
    let head: serde_json::Value =
        serde_json::from_slice(&run(&c, effect("head", "a/b.txt", None)).await.unwrap()).unwrap();
    assert_eq!(head["size"], 8);
    assert!(
        head["last_modified"]
            .as_str()
            .unwrap()
            .parse::<jiff::Timestamp>()
            .is_ok()
    );

    // A plain client with no Ostro credentials (the operator client adds nothing to requests).
    let plain = Egress::operator_client(&EgressConfig::default()).unwrap();
    let url = String::from_utf8(
        run(&c, effect("presign-get", "a/b.txt", None))
            .await
            .unwrap(),
    )
    .unwrap();
    let r = plain.get(&url).send().await.unwrap();
    assert_eq!(r.status(), 200, "presigned GET");
    assert_eq!(&r.bytes().await.unwrap()[..], b"hello s3");

    let url = String::from_utf8(
        run(&c, effect("presign-put", "up.txt", None))
            .await
            .unwrap(),
    )
    .unwrap();
    let r = plain
        .put(&url)
        .header("content-type", "text/plain")
        .body("via presign")
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "presigned PUT: {}", r.status());
    assert_eq!(
        run(&c, effect("get", "up.txt", None)).await.unwrap(),
        b"via presign"
    );

    for k in ["a/b.txt", "up.txt"] {
        assert!(run(&c, effect("delete", k, None)).await.unwrap().is_empty());
    }
    let e = run(&c, effect("get", "a/b.txt", None)).await.unwrap_err();
    assert_eq!(info(&e).1, NOT_FOUND);
}
