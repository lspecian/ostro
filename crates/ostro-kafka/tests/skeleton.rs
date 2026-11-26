//! 14-01: the crate links and its contract types are usable (the transport landed in 14-31;
//! the read path lands in 14-34).

use ostro_kafka::{Client, ClientConfig, KafkaError, Record, TopicPartition};

#[tokio::test]
async fn crate_links_and_connect_reports_an_unreachable_broker() {
    // A port that was just free: nothing listens there.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let cfg = ClientConfig {
        bootstrap: vec![format!("127.0.0.1:{port}")],
        ..ClientConfig::default()
    };
    match Client::connect(cfg).await {
        Err(KafkaError::Io(_) | KafkaError::Timeout) => {}
        other => panic!("expected a connection error, got {other:?}"),
    }
    let tp = TopicPartition {
        topic: "orders".into(),
        partition: 0,
    };
    assert_eq!(tp.partition, 0);
    assert_eq!(Record::default().offset, 0);
}
