//! Integration tests for `ostro_bridge::secrets` (06-05): the public API as later plans use
//! it. Run with `--test-threads=1` (some tests mutate process environment variables; they
//! also hold `ENV`).

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use ostro_bridge::meta_view::{MetaView, StaticMetaView};
use ostro_bridge::origin::{Origin, OriginPattern};
use ostro_bridge::secrets::{
    EnvProvider, FileProvider, GrantSpec, ProviderChain, ProviderError, ResolveCtx, Scrubber,
    SealedProvider, SealedStore, SealingKeyRing, SecretProvider, SecretResolver, SecretValue,
    SecretsConfig, TemplateError, check_grant, template,
};
use ostro_types::journal::Failure;
use secrecy::{ExposeSecret, SecretBox};
use std::sync::atomic::{AtomicUsize, Ordering};
use url::Url;

/// Serialises environment mutation (tests also run with `--test-threads=1`).
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A canary value: must never show up in any `Debug` output, error or message.
const CANARY: &[u8] = b"CANARY-0b5e7d1c-4f2a-value";

fn origin(s: &str) -> Origin {
    Origin::from_url(&Url::parse(s).unwrap()).unwrap()
}

fn assert_no_canary(s: &str) {
    assert!(
        !s.contains("CANARY-0b5e7d1c"),
        "canary leaked into output: {s}"
    );
}

fn nr_kind(f: Failure) -> String {
    match f {
        Failure::NonRetryable(i) => {
            assert_no_canary(&i.message);
            i.kind
        }
        other => panic!("expected NonRetryable, got {other:?}"),
    }
}

#[test]
fn template_header_value_expands_after_parse() {
    let header = "Bearer {{secret:stripe/live-key}}";
    let ph = template::parse(header).unwrap();
    assert_eq!(ph.len(), 1);
    assert_eq!(ph[0].name, "stripe/live-key");
    let mut values = BTreeMap::new();
    values.insert(ph[0].name.clone(), SecretValue::new(CANARY.to_vec()));
    let out = template::expand(header, &values);
    let mut expect = b"Bearer ".to_vec();
    expect.extend_from_slice(CANARY);
    assert_eq!(&out[..], &expect[..]);
    assert_no_canary(&format!("{values:?}"));
}

#[test]
fn url_placeholders_are_detectable() {
    assert!(template::contains("https://api.test/{{secret:k}}/x"));
    assert!(template::contains("{{secret:"));
    assert!(matches!(
        template::parse("{{secret:__ostro/token-key}}"),
        Err(TemplateError::Reserved { .. })
    ));
}

#[test]
fn grant_record_round_trip_and_check() {
    let spec = GrantSpec {
        module: "t/m".into(),
        secret: "api-key".into(),
        origins: vec![OriginPattern::parse("https://api.example.com").unwrap()],
        body: false,
    };
    let rec = spec.to_record();
    assert_eq!(rec.id, GrantSpec::id("t/m", "api-key"));
    assert_eq!(GrantSpec::from_record(&rec).unwrap(), spec);
    let grants = [rec];
    check_grant(
        &grants,
        "t/m",
        "api-key",
        &origin("https://api.example.com/p"),
        false,
    )
    .unwrap();
    let k = nr_kind(
        check_grant(
            &grants,
            "t/m",
            "api-key",
            &origin("https://other.example.com"),
            false,
        )
        .unwrap_err(),
    );
    assert_eq!(k, ostro_bridge::kinds::SECRET_NOT_GRANTED);
    let k = nr_kind(
        check_grant(
            &grants,
            "t/m",
            "api-key",
            &origin("https://api.example.com"),
            true,
        )
        .unwrap_err(),
    );
    assert_eq!(k, ostro_bridge::kinds::SECRET_NOT_GRANTED);
}

#[test]
fn secret_value_debug_never_leaks() {
    let v = SecretValue::new(CANARY.to_vec());
    assert_no_canary(&format!("{v:?}"));
    assert_no_canary(&format!("{:#?}", vec![v.clone()]));
    let short = SecretValue::new(b"short".to_vec());
    let k = nr_kind(short.for_templating().unwrap_err());
    assert_eq!(k, ostro_bridge::kinds::SECRET_NOT_GRANTED);
}

#[test]
fn scrubber_removes_canary_from_echoed_body() {
    let v = SecretValue::new(CANARY.to_vec());
    let s = Scrubber::new(&[&v]);
    let mut body = format!(
        r#"{{"headers":{{"authorization":"Bearer {}","x-b64":"{}"}}}}"#,
        std::str::from_utf8(CANARY).unwrap(),
        STANDARD.encode(CANARY)
    )
    .into_bytes();
    assert_eq!(s.scrub_bytes(&mut body), 2);
    assert_no_canary(&String::from_utf8_lossy(&body));
    assert_no_canary(&format!("{s:?}"));
}

#[test]
fn sealing_key_from_env_round_trips() {
    let _g = ENV.blocking_lock();
    let dir = tempfile::tempdir().unwrap();
    let key = STANDARD.encode([0x5au8; 32]);
    // SAFETY: env mutation is serialised by `ENV` and the suite runs single-threaded.
    unsafe { std::env::set_var("OSTRO_SEALING_KEY", &key) };
    let ring = SealingKeyRing::load(&SecretsConfig::default(), dir.path(), false);
    // SAFETY: as above.
    unsafe { std::env::remove_var("OSTRO_SEALING_KEY") };
    let ring = ring.unwrap().expect("key from env");
    assert_no_canary(&format!("{ring:?}"));
    let store = SealedStore::new(Arc::new(ring));
    let rec = store.seal("_/api-key", CANARY).unwrap();
    assert!(rec.key_id.starts_with("aes256gcm:"));
    assert_eq!(store.open(&rec).unwrap().expose(), CANARY);
    // Without the env var and outside dev mode there is no key.
    assert!(
        SealingKeyRing::load(&SecretsConfig::default(), dir.path(), false)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn env_provider_reads_mapped_variable() {
    let _g = ENV.lock().await;
    let p = EnvProvider::default();
    let var = p.var_name("stripe/live-key.v2");
    assert_eq!(var, "OSTRO_SECRET_STRIPE_LIVE_KEY_V2");
    // SAFETY: env mutation is serialised by `ENV` and the suite runs single-threaded.
    unsafe { std::env::set_var(&var, std::str::from_utf8(CANARY).unwrap()) };
    let got = p.get("stripe/live-key.v2").await;
    // SAFETY: as above.
    unsafe { std::env::remove_var(&var) };
    assert_eq!(got.unwrap().unwrap().expose_secret(), CANARY);
    assert!(p.get("stripe/live-key.v2").await.unwrap().is_none());
    assert!(matches!(
        p.get("__ostro/token-key").await,
        Err(ProviderError::Invalid(_))
    ));
}

#[tokio::test]
async fn file_provider_reads_per_call() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("db-pass"), b"first-value-123\n").unwrap();
    let p = FileProvider::new(dir.path());
    assert_eq!(
        p.get("db-pass").await.unwrap().unwrap().expose_secret(),
        b"first-value-123"
    );
    std::fs::write(dir.path().join("db-pass"), b"second-value-456").unwrap();
    assert_eq!(
        p.get("db-pass").await.unwrap().unwrap().expose_secret(),
        b"second-value-456"
    );
    assert!(p.get("../db-pass").await.is_err());
}

struct Counting(AtomicUsize);

#[async_trait::async_trait]
impl SecretProvider for Counting {
    fn name(&self) -> &'static str {
        "counting"
    }
    async fn get(&self, _: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(SecretBox::new(CANARY.into())))
    }
}

#[tokio::test]
async fn resolver_end_to_end_over_sealed_env_file_chain() {
    let _g = ENV.lock().await;
    let store = SealedStore::new(Arc::new(SealingKeyRing::from_bytes([0x33; 32])));
    let grant = GrantSpec {
        module: "acme/billing".into(),
        secret: "stripe/key".into(),
        origins: vec![OriginPattern::parse("https://api.stripe.com").unwrap()],
        body: false,
    };
    let file_grant = GrantSpec {
        secret: "from-file".into(),
        ..grant.clone()
    };
    let meta = StaticMetaView::new()
        .insert_grant(grant.to_record())
        .insert_grant(file_grant.to_record())
        .insert_sealed_secret(store.seal("acme/stripe/key", CANARY).unwrap());
    let meta: Arc<dyn MetaView> = Arc::new(meta);
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("from-file"), b"file-provided-value").unwrap();
    let chain = ProviderChain::new(vec![
        Arc::new(SealedProvider::new(
            meta.clone(),
            store.clone(),
            Some("acme".into()),
        )) as Arc<dyn SecretProvider>,
        Arc::new(EnvProvider::default()),
        Arc::new(FileProvider::new(dir.path())),
    ]);
    let r = SecretResolver::new(meta.clone(), chain).with_sealed_store(store);
    let ctx = ResolveCtx {
        module: "acme/billing",
        tenant: Some("acme"),
        run_id: "r1",
        attempt: 1,
    };
    let o = origin("https://api.stripe.com/v1/charges");
    assert_eq!(
        r.resolve(&ctx, "stripe/key", &o, false)
            .await
            .unwrap()
            .expose(),
        CANARY
    );
    assert_eq!(
        r.resolve(&ctx, "from-file", &o, false)
            .await
            .unwrap()
            .expose(),
        b"file-provided-value"
    );
    let k = nr_kind(
        r.resolve(&ctx, "stripe/key", &origin("https://evil.test"), false)
            .await
            .unwrap_err(),
    );
    assert_eq!(k, ostro_bridge::kinds::SECRET_NOT_GRANTED);
    let k = nr_kind(r.resolve(&ctx, "stripe/key", &o, true).await.unwrap_err());
    assert_eq!(k, ostro_bridge::kinds::SECRET_NOT_GRANTED);
    assert_no_canary(&format!("{r:?}"));
}

#[tokio::test]
async fn missing_grant_never_calls_provider() {
    let counting = Arc::new(Counting(AtomicUsize::new(0)));
    let chain = ProviderChain::new(vec![counting.clone() as Arc<dyn SecretProvider>]);
    let r = SecretResolver::new(Arc::new(StaticMetaView::new()), chain);
    let ctx = ResolveCtx {
        module: "acme/billing",
        tenant: None,
        run_id: "r1",
        attempt: 2,
    };
    let k = nr_kind(
        r.resolve(&ctx, "stripe/key", &origin("https://api.stripe.com"), false)
            .await
            .unwrap_err(),
    );
    assert_eq!(k, ostro_bridge::kinds::SECRET_NOT_GRANTED);
    assert_eq!(counting.0.load(Ordering::SeqCst), 0);
    // Operator resolution needs no grant and does call the chain.
    assert_eq!(
        r.resolve_operator("stripe/key").await.unwrap().expose(),
        CANARY
    );
    assert_eq!(counting.0.load(Ordering::SeqCst), 1);
    // Reserved names never go through the chain.
    assert!(
        r.resolve_reserved("__ostro/presign/objs")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(counting.0.load(Ordering::SeqCst), 1);
}
