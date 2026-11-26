//! 14-31: the Kafka transport against the in-process fake broker — ApiVersions negotiation
//! (incl. the v0 fallback), SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 (success, wrong
//! password, unsupported mechanism, forged server signature), TLS with a CA generated at test
//! time (and failure with an unknown CA or a wrong name), malformed frames (oversize,
//! negative, truncated, garbage, huge array, wrong correlation id) as errors, request
//! timeouts, and reconnection after the broker drops the socket.

mod support;

use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::MetadataRequest;
use ostro_kafka::conn::{BACKOFF_BASE, BACKOFF_MAX};
use ostro_kafka::versions::{FETCH, METADATA, OFFSET_COMMIT};
use ostro_kafka::{
    Backoff, Broker, Client, ClientConfig, ConnectOptions, KafkaError, SaslConfig, SaslMechanism,
    TlsConfig,
};
use support::{FakeBroker, Misbehave, Opts, PASSWORD, USER};

const METADATA_KEY: i16 = 3;

fn cfg(b: &FakeBroker) -> ClientConfig {
    ClientConfig {
        bootstrap: vec![b.bootstrap()],
        client_id: "ostro-test".into(),
        request_timeout: Duration::from_secs(5),
        ..ClientConfig::default()
    }
}

fn sasl(mechanism: SaslMechanism, password: &str) -> Option<SaslConfig> {
    Some(SaslConfig {
        mechanism,
        username: USER.into(),
        password: zeroize::Zeroizing::new(password.into()),
    })
}

async fn metadata_ok(c: &Client, port: u16) {
    let md = c.request(&MetadataRequest::default()).await.unwrap();
    assert_eq!(md.brokers.len(), 1);
    assert_eq!(md.brokers[0].port, i32::from(port));
}

#[tokio::test]
async fn negotiates_the_highest_common_versions() {
    let b = FakeBroker::start(Opts::default()).await;
    let c = Client::connect(cfg(&b)).await.unwrap();
    let table = c.api_versions().await.unwrap();
    let v = |key: i16| table.iter().find(|r| r.api.key == key).unwrap().version;
    assert_eq!(v(METADATA.key), Some(12));
    assert_eq!(
        v(FETCH.key),
        Some(12),
        "Fetch capped at v12 (broker offers v17)"
    );
    assert_eq!(v(OFFSET_COMMIT.key), Some(8), "pre-KIP-848 OffsetCommit");
    assert_eq!(b.versions_of(18), vec![3], "ApiVersions v3 first");
    metadata_ok(&c, b.addr.port()).await;
    assert_eq!(b.versions_of(METADATA_KEY), vec![12]);
}

#[tokio::test]
async fn api_versions_falls_back_for_an_old_broker() {
    let b = FakeBroker::start(Opts {
        old_api_versions: true,
        api_versions: vec![(18, 0, 2), (3, 0, 9), (1, 0, 11)],
        ..Opts::default()
    })
    .await;
    let c = Client::connect(cfg(&b)).await.unwrap();
    assert_eq!(b.versions_of(18), vec![3, 2], "v3 refused, retried at v2");
    let table = c.api_versions().await.unwrap();
    let row = |key: i16| *table.iter().find(|r| r.api.key == key).unwrap();
    assert_eq!(row(METADATA.key).version, Some(9));
    assert_eq!(row(FETCH.key).version, Some(11));
    assert_eq!(row(OFFSET_COMMIT.key).version, None);
    metadata_ok(&c, b.addr.port()).await;
}

#[tokio::test]
async fn a_broker_below_the_minimum_is_unsupported_version() {
    // Metadata only up to v0: the client needs v1+.
    let b = FakeBroker::start(Opts {
        api_versions: vec![(18, 0, 3), (3, 0, 0)],
        ..Opts::default()
    })
    .await;
    let c = Client::connect(cfg(&b)).await.unwrap();
    let e = c.request(&MetadataRequest::default()).await.unwrap_err();
    let msg = e.to_string();
    assert!(msg.starts_with("unsupported_version"), "{msg}");
    assert!(msg.contains("Metadata"), "{msg}");
    assert_eq!(
        b.count(METADATA_KEY),
        0,
        "nothing sent at an unsupported version"
    );
}

async fn sasl_ok(mech: SaslMechanism) {
    let b = FakeBroker::start(Opts {
        sasl: Some(vec!["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]),
        ..Opts::default()
    })
    .await;
    let mut cfg = cfg(&b);
    cfg.sasl = sasl(mech, PASSWORD);
    let c = Client::connect(cfg).await.unwrap();
    metadata_ok(&c, b.addr.port()).await;
}

async fn sasl_wrong_password(mech: SaslMechanism) {
    let b = FakeBroker::start(Opts {
        sasl: Some(vec!["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]),
        ..Opts::default()
    })
    .await;
    let mut cfg = cfg(&b);
    let wrong = "wrong-pw-c0ffee";
    cfg.sasl = sasl(mech, wrong);
    let dbg = format!("{cfg:?}");
    assert!(!dbg.contains(wrong), "{dbg}");
    match Client::connect(cfg).await {
        Err(e @ KafkaError::Sasl(_)) => {
            let msg = e.to_string();
            assert!(msg.contains(mech.as_str()), "{msg}");
            assert!(!msg.contains(wrong) && !msg.contains(PASSWORD), "{msg}");
            assert!(!format!("{e:?}").contains(wrong));
        }
        other => panic!("{mech:?}: expected a SASL error, got {other:?}"),
    }
}

#[tokio::test]
async fn sasl_plain_authenticates() {
    sasl_ok(SaslMechanism::Plain).await;
}

#[tokio::test]
async fn sasl_scram_sha256_authenticates() {
    sasl_ok(SaslMechanism::ScramSha256).await;
}

#[tokio::test]
async fn sasl_scram_sha512_authenticates() {
    sasl_ok(SaslMechanism::ScramSha512).await;
}

#[tokio::test]
async fn sasl_plain_wrong_password_fails() {
    sasl_wrong_password(SaslMechanism::Plain).await;
}

#[tokio::test]
async fn sasl_scram_sha256_wrong_password_fails() {
    sasl_wrong_password(SaslMechanism::ScramSha256).await;
}

#[tokio::test]
async fn sasl_scram_sha512_wrong_password_fails() {
    sasl_wrong_password(SaslMechanism::ScramSha512).await;
}

#[tokio::test]
async fn sasl_unsupported_mechanism_and_forged_server_signature_fail() {
    let b = FakeBroker::start(Opts {
        sasl: Some(vec!["PLAIN"]),
        ..Opts::default()
    })
    .await;
    let mut c = cfg(&b);
    c.sasl = sasl(SaslMechanism::ScramSha512, PASSWORD);
    match Client::connect(c).await {
        Err(KafkaError::Sasl(m)) => {
            assert!(m.contains("not enabled") && m.contains("PLAIN"), "{m}")
        }
        other => panic!("{other:?}"),
    }

    let b = FakeBroker::start(Opts {
        sasl: Some(vec!["SCRAM-SHA-256"]),
        bad_server_signature: true,
        ..Opts::default()
    })
    .await;
    let mut c = cfg(&b);
    c.sasl = sasl(SaslMechanism::ScramSha256, PASSWORD);
    match Client::connect(c).await {
        Err(KafkaError::Sasl(m)) => assert!(m.contains("server signature"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn tls_with_the_generated_ca_connects() {
    let b = FakeBroker::start(Opts {
        tls: true,
        ..Opts::default()
    })
    .await;
    let mut c = cfg(&b);
    c.tls = Some(TlsConfig {
        ca_file: b.ca_pem.clone(),
        server_name: None,
    });
    let client = Client::connect(c.clone()).await.unwrap();
    metadata_ok(&client, b.addr.port()).await;
    // SNI override to a name in the certificate
    c.tls = Some(TlsConfig {
        ca_file: b.ca_pem.clone(),
        server_name: Some("localhost".into()),
    });
    Client::connect(c).await.unwrap();
}

#[tokio::test]
async fn tls_with_sasl_scram_connects() {
    let b = FakeBroker::start(Opts {
        tls: true,
        sasl: Some(vec!["SCRAM-SHA-512"]),
        ..Opts::default()
    })
    .await;
    let mut c = cfg(&b);
    c.tls = Some(TlsConfig {
        ca_file: b.ca_pem.clone(),
        server_name: None,
    });
    c.sasl = sasl(SaslMechanism::ScramSha512, PASSWORD);
    let client = Client::connect(c).await.unwrap();
    metadata_ok(&client, b.addr.port()).await;
}

#[tokio::test]
async fn tls_with_an_unknown_ca_or_wrong_name_fails() {
    let b = FakeBroker::start(Opts {
        tls: true,
        ..Opts::default()
    })
    .await;
    for tls in [
        TlsConfig {
            ca_file: b.other_ca_pem.clone(),
            server_name: None,
        },
        TlsConfig {
            ca_file: None,
            server_name: None,
        },
        TlsConfig {
            ca_file: b.ca_pem.clone(),
            server_name: Some("not-the-broker.example".into()),
        },
    ] {
        let mut c = cfg(&b);
        c.tls = Some(tls.clone());
        match Client::connect(c).await {
            Err(KafkaError::Tls(m)) => assert!(m.contains("handshake"), "{m}"),
            other => panic!("{tls:?}: expected a TLS error, got {other:?}"),
        }
    }
    // a ca_file that is missing or holds no certificate
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "not a certificate\n").unwrap();
    for path in [empty, dir.path().join("missing.pem")] {
        let mut c = cfg(&b);
        c.tls = Some(TlsConfig {
            ca_file: Some(path),
            server_name: None,
        });
        assert!(matches!(Client::connect(c).await, Err(KafkaError::Tls(_))));
    }
}

async fn misbehaving(how: Misbehave) -> KafkaError {
    let b = FakeBroker::start(Opts {
        misbehave: Some((METADATA_KEY, how, usize::MAX)),
        ..Opts::default()
    })
    .await;
    let mut c = cfg(&b);
    c.request_timeout = Duration::from_millis(500);
    let client = Client::connect(c).await.unwrap();
    let e = client
        .request(&MetadataRequest::default())
        .await
        .unwrap_err();
    // the connection is gone; a later request reconnects (and fails the same way)
    assert!(client.bootstrap().conn().await.is_ok());
    e
}

#[tokio::test]
async fn malformed_frames_are_errors_not_panics() {
    for how in [
        Misbehave::Oversize,
        Misbehave::NegativeSize,
        Misbehave::Garbage,
        Misbehave::HugeArray,
        Misbehave::WrongCorrelation,
    ] {
        let e = misbehaving(how).await;
        assert!(matches!(e, KafkaError::Protocol(_)), "{how:?}: {e:?}");
    }
    let e = misbehaving(Misbehave::Truncated).await;
    assert!(matches!(e, KafkaError::Io(_)), "truncated: {e:?}");
    assert!(e.to_string().contains("mid-frame"), "{e}");
    let e = misbehaving(Misbehave::Close).await;
    assert!(matches!(e, KafkaError::Io(_)), "close: {e:?}");
}

#[tokio::test]
async fn a_silent_broker_times_out_and_the_next_request_reconnects() {
    let b = FakeBroker::start(Opts {
        misbehave: Some((METADATA_KEY, Misbehave::Silent, 1)),
        ..Opts::default()
    })
    .await;
    let mut c = cfg(&b);
    c.request_timeout = Duration::from_millis(300);
    let client = Client::connect(c).await.unwrap();
    let e = client
        .request(&MetadataRequest::default())
        .await
        .unwrap_err();
    assert!(matches!(e, KafkaError::Timeout), "{e:?}");
    metadata_ok(&client, b.addr.port()).await;
    assert_eq!(
        b.seen.connections.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
}

#[tokio::test]
async fn reconnects_after_the_broker_drops_the_socket() {
    let b = FakeBroker::start(Opts {
        misbehave: Some((METADATA_KEY, Misbehave::Close, 1)),
        ..Opts::default()
    })
    .await;
    let client = Client::connect(cfg(&b)).await.unwrap();
    let e = client
        .request(&MetadataRequest::default())
        .await
        .unwrap_err();
    assert!(matches!(e, KafkaError::Io(_)), "{e:?}");
    metadata_ok(&client, b.addr.port()).await;
    metadata_ok(&client, b.addr.port()).await;
    assert_eq!(
        b.seen.connections.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert_eq!(
        b.versions_of(18),
        vec![3, 3],
        "ApiVersions on every new connection"
    );
}

#[tokio::test]
async fn failed_connects_are_spaced_by_the_backoff() {
    // A port nothing listens on.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = ClientConfig {
        bootstrap: vec![format!("127.0.0.1:{port}")],
        ..ClientConfig::default()
    };
    let opts = Arc::new(ConnectOptions::new(&config).unwrap());
    let broker = Broker::with_backoff(
        format!("127.0.0.1:{port}"),
        opts,
        Backoff::with_rng(BACKOFF_BASE, BACKOFF_MAX, || 0),
    );
    let t0 = tokio::time::Instant::now();
    assert!(matches!(broker.conn().await, Err(KafkaError::Io(_))));
    let t1 = tokio::time::Instant::now();
    assert!(broker.conn().await.is_err());
    let t2 = tokio::time::Instant::now();
    assert!(broker.conn().await.is_err());
    let t3 = tokio::time::Instant::now();
    assert!(t2 - t1 >= Duration::from_millis(50), "{:?}", t2 - t1);
    assert!(t3 - t2 >= Duration::from_millis(100), "{:?}", t3 - t2);
    assert!(
        t1 - t0 < Duration::from_millis(50),
        "the first attempt does not wait"
    );
}

#[tokio::test]
async fn connect_tries_every_bootstrap_and_prefers_definitive_errors() {
    let b = FakeBroker::start(Opts::default()).await;
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut c = cfg(&b);
    c.bootstrap = vec![format!("127.0.0.1:{port}"), b.bootstrap()];
    Client::connect(c).await.unwrap();

    let s = FakeBroker::start(Opts {
        sasl: Some(vec!["PLAIN"]),
        ..Opts::default()
    })
    .await;
    let mut c = cfg(&s);
    c.bootstrap = vec![s.bootstrap(), format!("127.0.0.1:{port}")];
    c.sasl = sasl(SaslMechanism::Plain, "nope");
    assert!(matches!(Client::connect(c).await, Err(KafkaError::Sasl(_))));

    let c = ClientConfig::default();
    assert!(matches!(
        Client::connect(c).await,
        Err(KafkaError::Protocol(_))
    ));
}
