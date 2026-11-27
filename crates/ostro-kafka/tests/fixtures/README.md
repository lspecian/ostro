# Broker-captured record batches (14-34)

`broker-<codec>.bin` is one v2 record batch exactly as Apache Kafka returned it in a Fetch
response: 200 records (`fixture-key-<i>`, `fixture-value-<i>-<"abc" x (i % 17)>`, header
`i=<i>`, offsets 0..199), produced uncompressed by `ostro_kafka::testkit::produce` into a
topic created with `compression.type=<codec>`, so the batch was compressed by the Java
broker (kafka-clients 4.1.2: `snappy-java` 1.1.10.7 in the xerial stream framing,
`lz4-java` 1.10.1 frame, `zstd-jni` 1.5.6-10, JDK gzip). `tests/codec.rs`
`broker_captured_fixtures_decode` decodes them with the pure-Rust codecs.

- Kafka: 4.1.2 (KRaft combined mode, `scripts/test-services.sh start kafka`), Temurin JRE
  21.0.13.1+2, captured locally on 2026-11-27.
- Command (from the repository root):

  ```
  bash scripts/fetch-tools.sh kafka jre
  bash scripts/test-services.sh start kafka
  eval "$(bash scripts/test-services.sh env)"
  OSTRO_KAFKA_CAPTURE_FIXTURES=1 cargo test -p ostro-kafka --features testkit --test broker capture_fixtures -- --nocapture
  bash scripts/test-services.sh stop
  ```

Recapturing rewrites the files (timestamps and producer fields differ; the records do not).
