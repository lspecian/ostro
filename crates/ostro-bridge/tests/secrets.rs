//! Integration tests for `ostro_bridge::secrets` (06-05): the public API as later plans use
//! it. Run with `--test-threads=1` (some tests mutate process environment variables; they
//! also hold `ENV`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use ostro_bridge::origin::{Origin, OriginPattern};
use ostro_bridge::secrets::{
    GrantSpec, Scrubber, SealedStore, SealingKeyRing, SecretValue, SecretsConfig, TemplateError,
    check_grant, template,
};
use ostro_types::journal::Failure;
use url::Url;

/// Serialises environment mutation (tests also run with `--test-threads=1`).
static ENV: Mutex<()> = Mutex::new(());

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
    let _g = ENV.lock().unwrap_or_else(|e| e.into_inner());
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
