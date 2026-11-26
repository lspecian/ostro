//! 14-31: TLS and SASL against the Apache Kafka 4.1 test broker (`scripts/test-services.sh
//! start kafka`, then `eval "$(bash scripts/test-services.sh env)"`).
//!
//! - PLAINTEXT (`OSTRO_TEST_KAFKA_BOOTSTRAP`): connect, negotiate (the version table is
//!   printed), Metadata answers.
//! - SSL (`OSTRO_TEST_KAFKA_TLS_BOOTSTRAP`, CA PEM `OSTRO_TEST_KAFKA_TLS_CA`): success with the
//!   CA; native roots only and a wrong server name fail with `Tls`.
//! - SASL_PLAINTEXT (`OSTRO_TEST_KAFKA_SASL_BOOTSTRAP`, `_SASL_USER`, `_SASL_PASSWORD`): PLAIN,
//!   SCRAM-SHA-256 and SCRAM-SHA-512 authenticate and Metadata answers; a wrong password fails
//!   with `Sasl` for each mechanism and the password never appears in the error.
//!
//! Without the variables every test prints a line starting with `SKIP:` and passes; the plan
//! verify starts the broker and fails on `SKIP:`.

use std::path::PathBuf;
use std::time::Duration;

use kafka_protocol::messages::MetadataRequest;
use ostro_kafka::{Client, ClientConfig, KafkaError, SaslConfig, SaslMechanism, TlsConfig};

fn env(names: &[&str]) -> Option<Vec<String>> {
    let mut out = Vec::new();
    for n in names {
        match std::env::var(n) {
            Ok(v) if !v.is_empty() => out.push(v),
            _ => {
                println!("SKIP: {n} is unset (start the broker with scripts/test-services.sh)");
                return None;
            }
        }
    }
    Some(out)
}

fn config(bootstrap: &str) -> ClientConfig {
    ClientConfig {
        bootstrap: vec![bootstrap.to_owned()],
        client_id: "ostro-broker-auth".into(),
        request_timeout: Duration::from_secs(20),
        ..ClientConfig::default()
    }
}

async fn metadata_answers(c: &Client) {
    let md = c.request(&MetadataRequest::default()).await.unwrap();
    assert!(!md.brokers.is_empty(), "{md:?}");
    println!(
        "metadata: cluster {:?}, {} broker(s), first {}:{}",
        md.cluster_id,
        md.brokers.len(),
        md.brokers[0].host,
        md.brokers[0].port
    );
}

#[tokio::test]
async fn plaintext_connects_and_negotiates() {
    let Some(v) = env(&["OSTRO_TEST_KAFKA_BOOTSTRAP"]) else {
        return;
    };
    let c = Client::connect(config(&v[0])).await.unwrap();
    let table = c.api_versions().await.unwrap();
    println!("negotiated against {}:", v[0]);
    for row in &table {
        println!("  {row}");
    }
    assert!(
        table.iter().all(|r| r.version.is_some()),
        "every client API negotiates against Kafka 4.1"
    );
    let get = |key: i16| table.iter().find(|r| r.api.key == key).unwrap().version;
    assert_eq!(get(1), Some(12), "Fetch capped at v12");
    assert_eq!(get(8), Some(8), "OffsetCommit capped at v8");
    metadata_answers(&c).await;
}

fn tls_config(bootstrap: &str, ca: Option<PathBuf>, name: Option<&str>) -> ClientConfig {
    ClientConfig {
        tls: Some(TlsConfig {
            ca_file: ca,
            server_name: name.map(str::to_owned),
        }),
        ..config(bootstrap)
    }
}

#[tokio::test]
async fn tls_with_the_test_ca_connects() {
    let Some(v) = env(&["OSTRO_TEST_KAFKA_TLS_BOOTSTRAP", "OSTRO_TEST_KAFKA_TLS_CA"]) else {
        return;
    };
    let c = Client::connect(tls_config(&v[0], Some(PathBuf::from(&v[1])), None))
        .await
        .unwrap();
    metadata_answers(&c).await;
    // the certificate also names localhost
    let c = Client::connect(tls_config(
        &v[0],
        Some(PathBuf::from(&v[1])),
        Some("localhost"),
    ))
    .await
    .unwrap();
    metadata_answers(&c).await;
}

#[tokio::test]
async fn tls_with_native_roots_only_fails() {
    let Some(v) = env(&["OSTRO_TEST_KAFKA_TLS_BOOTSTRAP"]) else {
        return;
    };
    match Client::connect(tls_config(&v[0], None, None)).await {
        Err(KafkaError::Tls(m)) => println!("native roots only: {m}"),
        other => panic!("expected a TLS error, got {other:?}"),
    }
}

#[tokio::test]
async fn tls_with_a_wrong_server_name_fails() {
    let Some(v) = env(&["OSTRO_TEST_KAFKA_TLS_BOOTSTRAP", "OSTRO_TEST_KAFKA_TLS_CA"]) else {
        return;
    };
    match Client::connect(tls_config(
        &v[0],
        Some(PathBuf::from(&v[1])),
        Some("not-the-broker.example"),
    ))
    .await
    {
        Err(KafkaError::Tls(m)) => println!("wrong server name: {m}"),
        other => panic!("expected a TLS error, got {other:?}"),
    }
}

const SASL_VARS: [&str; 3] = [
    "OSTRO_TEST_KAFKA_SASL_BOOTSTRAP",
    "OSTRO_TEST_KAFKA_SASL_USER",
    "OSTRO_TEST_KAFKA_SASL_PASSWORD",
];

fn sasl_config(v: &[String], mechanism: SaslMechanism, password: &str) -> ClientConfig {
    ClientConfig {
        sasl: Some(SaslConfig {
            mechanism,
            username: v[1].clone(),
            password: zeroize::Zeroizing::new(password.to_owned()),
        }),
        ..config(&v[0])
    }
}

async fn sasl_succeeds(mechanism: SaslMechanism) {
    let Some(v) = env(&SASL_VARS) else {
        return;
    };
    let c = Client::connect(sasl_config(&v, mechanism, &v[2]))
        .await
        .unwrap_or_else(|e| panic!("{mechanism:?}: {e}"));
    println!("{} authenticated as {:?}", mechanism.as_str(), v[1]);
    metadata_answers(&c).await;
}

async fn sasl_wrong_password_fails(mechanism: SaslMechanism) {
    let Some(v) = env(&SASL_VARS) else {
        return;
    };
    let wrong = format!("{}-wrong", v[2]);
    match Client::connect(sasl_config(&v, mechanism, &wrong)).await {
        Err(e @ KafkaError::Sasl(_)) => {
            let msg = e.to_string();
            println!("{} wrong password: {msg}", mechanism.as_str());
            assert!(!msg.contains(&v[2]), "password leaked into the error");
        }
        other => panic!("{mechanism:?}: expected a SASL error, got {other:?}"),
    }
}

#[tokio::test]
async fn sasl_plain_authenticates() {
    sasl_succeeds(SaslMechanism::Plain).await;
}

#[tokio::test]
async fn sasl_scram_sha256_authenticates() {
    sasl_succeeds(SaslMechanism::ScramSha256).await;
}

#[tokio::test]
async fn sasl_scram_sha512_authenticates() {
    sasl_succeeds(SaslMechanism::ScramSha512).await;
}

#[tokio::test]
async fn sasl_plain_wrong_password_fails() {
    sasl_wrong_password_fails(SaslMechanism::Plain).await;
}

#[tokio::test]
async fn sasl_scram_sha256_wrong_password_fails() {
    sasl_wrong_password_fails(SaslMechanism::ScramSha256).await;
}

#[tokio::test]
async fn sasl_scram_sha512_wrong_password_fails() {
    sasl_wrong_password_fails(SaslMechanism::ScramSha512).await;
}
