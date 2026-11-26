//! 14-01: the crate links and its contract types are usable (the client itself lands in
//! 14-31 / 14-34).

use ostro_kafka::{Client, ClientConfig, KafkaError, Record, TopicPartition};

#[tokio::test]
async fn crate_links_and_client_is_a_stub_until_14_31() {
    let cfg = ClientConfig {
        bootstrap: vec!["127.0.0.1:9092".into()],
        ..ClientConfig::default()
    };
    match Client::connect(cfg).await {
        Err(KafkaError::Unimplemented(what)) => assert!(what.contains("14-31"), "{what}"),
        other => panic!("expected the 14-01 stub, got {other:?}"),
    }
    let tp = TopicPartition {
        topic: "orders".into(),
        partition: 0,
    };
    assert_eq!(tp.partition, 0);
    assert_eq!(Record::default().offset, 0);
}
