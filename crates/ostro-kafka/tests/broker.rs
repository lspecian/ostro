//! 14-34: the read path against the Apache Kafka 4.1 test broker (`scripts/test-services.sh
//! start kafka`, then `eval "$(bash scripts/test-services.sh env)"`; run with
//! `--features testkit --test broker -- --test-threads=1`).
//!
//! - every codec (none, gzip, snappy, lz4, zstd): 1,000 records per codec produced by the
//!   testkit and fetched back in order with exact offsets, keys, values, headers and
//!   timestamps, through small fetches that cut batches;
//! - topics with `compression.type=<codec>`: the Java broker recompresses, the client
//!   decodes (these are the fixtures `capture_fixtures` writes);
//! - ListOffsets earliest/latest; OffsetFetch on a fresh group (all `None`); OffsetCommit in
//!   the standalone form then OffsetFetch; two clients committing disjoint partitions of one
//!   group (`kafka-consumer-groups.sh --describe` is printed when the Kafka tools exist);
//! - transactions: aborted records are skipped (read_committed), committed ones returned,
//!   `next_offset` passes the markers;
//! - a fetch beyond the high watermark answers `OFFSET_OUT_OF_RANGE`;
//! - a test-controlled TCP proxy (rewriting the broker addresses in Metadata and
//!   FindCoordinator answers) drops every socket, also during a long-poll fetch: the client
//!   reconnects and continues;
//! - fetch over the SASL (SCRAM-SHA-512) and SSL listeners.
//!
//! Without `OSTRO_TEST_KAFKA_BOOTSTRAP` every test prints a line starting with `SKIP:` and
//! passes; the plan verify starts the broker and fails on `SKIP:`.

#![cfg(feature = "testkit")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{
    ApiKey, FetchRequest, FindCoordinatorResponse, MetadataResponse, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use ostro_kafka::codec::peek_batch;
use ostro_kafka::testkit::{self, Compression};
use ostro_kafka::{
    Client, ClientConfig, KafkaError, OffsetSpec, Record, SaslConfig, SaslMechanism, TlsConfig,
    TopicPartition,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

const CODECS: [(Compression, &str); 5] = [
    (Compression::None, "none"),
    (Compression::Gzip, "gzip"),
    (Compression::Snappy, "snappy"),
    (Compression::Lz4, "lz4"),
    (Compression::Zstd, "zstd"),
];

fn bootstrap() -> Option<String> {
    match std::env::var("OSTRO_TEST_KAFKA_BOOTSTRAP") {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            println!(
                "SKIP: OSTRO_TEST_KAFKA_BOOTSTRAP is unset (start the broker with scripts/test-services.sh)"
            );
            None
        }
    }
}

fn config(bootstrap: &str) -> ClientConfig {
    ClientConfig {
        bootstrap: vec![bootstrap.to_owned()],
        client_id: "ostro-broker-read".into(),
        request_timeout: Duration::from_secs(20),
        ..ClientConfig::default()
    }
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("ostro-1434-{prefix}-{}-{nanos}", std::process::id())
}

fn tp(topic: &str, partition: i32) -> TopicPartition {
    TopicPartition {
        topic: topic.to_owned(),
        partition,
    }
}

fn records(tag: &str, n: usize, ts0: i64) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            offset: 0,
            timestamp_ms: ts0 + i as i64,
            key: (i % 13 != 7).then(|| Bytes::from(format!("{tag}-key-{i}"))),
            value: (i % 17 != 3)
                .then(|| Bytes::from(format!("{tag}-value-{i}-{}", "payload".repeat(1 + i % 9)))),
            headers: match i % 4 {
                0 => vec![],
                1 => vec![("codec".into(), Some(Bytes::from(tag.to_owned())))],
                2 => vec![
                    ("i".into(), Some(Bytes::from(i.to_string()))),
                    ("null".into(), None),
                ],
                _ => vec![("empty".into(), Some(Bytes::new()))],
            },
        })
        .collect()
}

/// Read `tp` from `from` until `end` with small fetches; returns the records and the final
/// position.
async fn read_all(
    c: &Client,
    tp: &TopicPartition,
    from: i64,
    end: i64,
    max_bytes: i32,
) -> (Vec<Record>, i64) {
    let mut out = Vec::new();
    let mut at = from;
    let mut rounds = 0;
    while at < end {
        rounds += 1;
        assert!(rounds < 10_000, "no progress at {at}");
        let f = c
            .fetch_at(tp, at, max_bytes, Duration::from_millis(200))
            .await
            .unwrap();
        if let Some(first) = f.batch.records.first() {
            assert!(first.offset >= at, "{} < {at}", first.offset);
        }
        out.extend(f.batch.records);
        at = f.next_offset;
    }
    (out, at)
}

#[tokio::test]
async fn every_codec_round_trips_in_order() {
    let Some(b) = bootstrap() else { return };
    let c = Client::connect(config(&b)).await.unwrap();
    let topic = unique("codecs");
    testkit::create_topic(&c, &topic, 3).await.unwrap();
    assert_eq!(c.partitions(&topic).await.unwrap(), vec![0, 1, 2]);
    let mut want: HashMap<i32, Vec<Record>> = HashMap::new();
    for (i, (codec, name)) in CODECS.iter().enumerate() {
        let p = (i % 3) as i32;
        let rs = records(name, 1000, 1_700_000_000_000 + 10_000 * i as i64);
        for chunk in rs.chunks(250) {
            let base = testkit::produce(&c, &topic, p, chunk, *codec)
                .await
                .unwrap();
            let log = want.entry(p).or_default();
            assert_eq!(base, log.len() as i64, "{name}: base offset");
            for (j, r) in chunk.iter().enumerate() {
                log.push(Record {
                    offset: base + j as i64,
                    ..r.clone()
                });
            }
        }
    }
    for p in 0..3 {
        let w = &want[&p];
        let end = w.len() as i64;
        // small fetches cut compressed batches in the middle
        let (got, at) = read_all(&c, &tp(&topic, p), 0, end, 16 << 10).await;
        assert_eq!(at, end);
        assert_eq!(got.len(), w.len(), "partition {p}");
        for (g, e) in got.iter().zip(w) {
            assert_eq!(g, e, "partition {p}");
        }
        // one big fetch from the middle of a batch
        let f = c
            .fetch(&tp(&topic, p), 1100.min(end - 1), 8 << 20)
            .await
            .unwrap();
        assert_eq!(f.records.first().map(|r| r.offset), Some(1100.min(end - 1)));
        assert_eq!(f.high_watermark, end);
        println!(
            "partition {p}: {} records over {} codecs read back in order",
            got.len(),
            w.len() / 1000
        );
    }
}

#[tokio::test]
async fn broker_recompressed_topics_decode() {
    let Some(b) = bootstrap() else { return };
    let c = Client::connect(config(&b)).await.unwrap();
    for (_, name) in CODECS.iter().skip(1) {
        let topic = unique(&format!("java-{name}"));
        testkit::create_topic_with(&c, &topic, 1, &[("compression.type", name)])
            .await
            .unwrap();
        let rs = records(name, 500, 1_000);
        testkit::produce(&c, &topic, 0, &rs, Compression::None)
            .await
            .unwrap();
        let raw = raw_fetch(&c, &topic).await;
        let h = peek_batch(&raw).unwrap().unwrap();
        assert_ne!(
            h.compression,
            Compression::None,
            "{name}: broker recompressed"
        );
        let (got, _) = read_all(&c, &tp(&topic, 0), 0, 500, 1 << 20).await;
        let want: Vec<Record> = rs
            .into_iter()
            .enumerate()
            .map(|(i, r)| Record {
                offset: i as i64,
                ..r
            })
            .collect();
        assert_eq!(got, want, "{name}");
        println!(
            "compression.type={name}: Java-compressed batch decoded ({} bytes)",
            raw.len()
        );
    }
}

/// The raw `records` bytes of partition 0 from offset 0 (one Fetch, read_committed).
async fn raw_fetch(c: &Client, topic: &str) -> Bytes {
    let req = FetchRequest::default()
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_max_bytes(8 << 20)
        .with_isolation_level(1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(TopicName(StrBytes::from_string(topic.to_owned())))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(8 << 20),
                ]),
        ]);
    let r = c.request(&req).await.unwrap();
    let p = &r.responses[0].partitions[0];
    assert_eq!(p.error_code, 0);
    p.records.clone().unwrap()
}

/// Same definition as `tests/codec.rs` `fixture_records`.
fn fixture_records() -> Vec<Record> {
    (0..200)
        .map(|i| Record {
            offset: 0,
            timestamp_ms: 1_764_000_000_000 + i as i64,
            key: Some(Bytes::from(format!("fixture-key-{i}"))),
            value: Some(Bytes::from(format!(
                "fixture-value-{i}-{}",
                "abc".repeat(i % 17)
            ))),
            headers: vec![("i".to_owned(), Some(Bytes::from(i.to_string())))],
        })
        .collect()
}

/// Writes `tests/fixtures/broker-<codec>.bin` (one batch compressed by the Java broker,
/// topic `compression.type=<codec>`) when `OSTRO_KAFKA_CAPTURE_FIXTURES=1`.
#[tokio::test]
async fn capture_fixtures() {
    if std::env::var("OSTRO_KAFKA_CAPTURE_FIXTURES").as_deref() != Ok("1") {
        println!("capture_fixtures: not capturing (OSTRO_KAFKA_CAPTURE_FIXTURES != 1)");
        return;
    }
    let Some(b) = bootstrap() else { return };
    let c = Client::connect(config(&b)).await.unwrap();
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    std::fs::create_dir_all(&dir).unwrap();
    for name in ["gzip", "snappy", "lz4", "zstd"] {
        let topic = unique(&format!("fixture-{name}"));
        testkit::create_topic_with(&c, &topic, 1, &[("compression.type", name)])
            .await
            .unwrap();
        testkit::produce(&c, &topic, 0, &fixture_records(), Compression::None)
            .await
            .unwrap();
        let raw = raw_fetch(&c, &topic).await;
        let h = peek_batch(&raw).unwrap().unwrap();
        assert_eq!(h.record_count, 200);
        let path = dir.join(format!("broker-{name}.bin"));
        std::fs::write(&path, &raw[..h.size]).unwrap();
        println!(
            "captured {} ({} bytes, {:?})",
            path.display(),
            h.size,
            h.compression
        );
    }
}

#[tokio::test]
async fn list_offsets_earliest_and_latest() {
    let Some(b) = bootstrap() else { return };
    let c = Client::connect(config(&b)).await.unwrap();
    let topic = unique("offsets");
    testkit::create_topic(&c, &topic, 3).await.unwrap();
    testkit::produce(&c, &topic, 1, &records("lo", 42, 0), Compression::Gzip)
        .await
        .unwrap();
    let tps = [tp(&topic, 0), tp(&topic, 1), tp(&topic, 2)];
    let earliest = c.list_offsets(&tps, OffsetSpec::Earliest).await.unwrap();
    let latest = c.list_offsets(&tps, OffsetSpec::Latest).await.unwrap();
    assert_eq!(earliest.iter().map(|x| x.1).collect::<Vec<_>>(), [0, 0, 0]);
    assert_eq!(latest.iter().map(|x| x.1).collect::<Vec<_>>(), [0, 42, 0]);
    match c
        .list_offsets(&[tp("ostro-1434-no-such-topic", 0)], OffsetSpec::Latest)
        .await
    {
        Err(KafkaError::Broker { code: 3, .. }) => {}
        other => panic!("{other:?}"),
    }
}

fn kafka_tools() -> Option<(PathBuf, PathBuf)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/tools");
    let bin = root.join("kafka-4.1.2/bin/kafka-consumer-groups.sh");
    let jre = root.join("jre-21.0.13.1_2");
    (bin.exists() && jre.exists()).then_some((bin, jre))
}

fn describe_group(bootstrap: &str, group: &str) {
    let Some((bin, jre)) = kafka_tools() else {
        println!("kafka-consumer-groups.sh not available; describe skipped");
        return;
    };
    match std::process::Command::new("bash")
        .arg(&bin)
        .args([
            "--bootstrap-server",
            bootstrap,
            "--describe",
            "--group",
            group,
        ])
        .env("JAVA_HOME", &jre)
        .env("PATH", format!("{}/bin:/usr/bin:/bin", jre.display()))
        .output()
    {
        Ok(o) => println!(
            "kafka-consumer-groups.sh --describe --group {group}:\n{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
                .lines()
                .filter(|l| !l.contains("WARN"))
                .collect::<Vec<_>>()
                .join("\n")
        ),
        Err(e) => println!("kafka-consumer-groups.sh failed to start: {e}"),
    }
}

#[tokio::test]
async fn standalone_offsets_commit_and_fetch() {
    let Some(b) = bootstrap() else { return };
    let c = Client::connect(config(&b)).await.unwrap();
    let topic = unique("commits");
    testkit::create_topic(&c, &topic, 3).await.unwrap();
    for p in 0..3 {
        testkit::produce(&c, &topic, p, &records("c", 20, 0), Compression::None)
            .await
            .unwrap();
    }
    let group = unique("group");
    let tps = [tp(&topic, 0), tp(&topic, 1), tp(&topic, 2)];
    let fresh = c.committed(&group, &tps).await.unwrap();
    assert!(fresh.iter().all(|(_, o)| o.is_none()), "{fresh:?}");
    c.commit(&group, &[(tp(&topic, 0), 5), (tp(&topic, 2), 20)])
        .await
        .unwrap();
    let got = c.committed(&group, &tps).await.unwrap();
    assert_eq!(
        got,
        vec![
            (tp(&topic, 0), Some(5)),
            (tp(&topic, 1), None),
            (tp(&topic, 2), Some(20))
        ]
    );
    // re-commit moves it
    c.commit(&group, &[(tp(&topic, 0), 11)]).await.unwrap();
    assert_eq!(c.committed(&group, &tps[..1]).await.unwrap()[0].1, Some(11));
    describe_group(&b, &group);
}

#[tokio::test]
async fn two_clients_commit_disjoint_partitions_of_one_group() {
    let Some(b) = bootstrap() else { return };
    let setup = Client::connect(config(&b)).await.unwrap();
    let topic = unique("disjoint");
    testkit::create_topic(&setup, &topic, 4).await.unwrap();
    for p in 0..4 {
        testkit::produce(&setup, &topic, p, &records("d", 10, 0), Compression::None)
            .await
            .unwrap();
    }
    let group = unique("shared-group");
    let a = Client::connect(config(&b)).await.unwrap();
    let z = Client::connect(config(&b)).await.unwrap();
    let (ta, tz) = (topic.clone(), topic.clone());
    let (ga, gz) = (group.clone(), group.clone());
    let ja = tokio::spawn(async move {
        for round in 1..=5i64 {
            a.commit(&ga, &[(tp(&ta, 0), round), (tp(&ta, 1), round + 1)])
                .await
                .unwrap();
        }
        a
    });
    let jz = tokio::spawn(async move {
        for round in 1..=5i64 {
            z.commit(&gz, &[(tp(&tz, 2), 2 * round), (tp(&tz, 3), round)])
                .await
                .unwrap();
        }
        z
    });
    let (a, z) = (ja.await.unwrap(), jz.await.unwrap());
    let all: Vec<TopicPartition> = (0..4).map(|p| tp(&topic, p)).collect();
    let want = vec![
        (tp(&topic, 0), Some(5)),
        (tp(&topic, 1), Some(6)),
        (tp(&topic, 2), Some(10)),
        (tp(&topic, 3), Some(5)),
    ];
    assert_eq!(a.committed(&group, &all).await.unwrap(), want);
    assert_eq!(z.committed(&group, &all).await.unwrap(), want);
    describe_group(&b, &group);
}

#[tokio::test]
async fn aborted_transactions_are_skipped() {
    let Some(b) = bootstrap() else { return };
    let c = Client::connect(config(&b)).await.unwrap();
    let topic = unique("txn");
    testkit::create_topic(&c, &topic, 1).await.unwrap();
    let plain = records("plain", 3, 0);
    testkit::produce(&c, &topic, 0, &plain, Compression::None)
        .await
        .unwrap();
    let txn_id = unique("txn-id");
    let aborted = records("aborted", 5, 100);
    let ab = testkit::produce_transaction(&c, &txn_id, &topic, 0, &aborted, false)
        .await
        .unwrap();
    let committed = records("committed", 4, 200);
    let cb = testkit::produce_transaction(&c, &txn_id, &topic, 0, &committed, true)
        .await
        .unwrap();
    let tail = records("tail", 2, 300);
    let tb = testkit::produce(&c, &topic, 0, &tail, Compression::Zstd)
        .await
        .unwrap();
    println!("offsets: plain 0..3, aborted {ab}.., committed {cb}.., tail {tb}..");
    // Wait for the last stable offset to pass the tail (the commit marker may land after
    // it: EndTxn returns before the markers are written).
    let tail_end = tb + tail.len() as i64;
    let mut end = 0;
    for _ in 0..100 {
        end = c
            .list_offsets(&[tp(&topic, 0)], OffsetSpec::Latest)
            .await
            .unwrap()[0]
            .1;
        if end >= tail_end {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(end >= tail_end, "last stable offset {end} < {tail_end}");
    println!("last stable offset {end}");
    let (got, at) = read_all(&c, &tp(&topic, 0), 0, end, 1 << 20).await;
    let kinds: Vec<String> = got
        .iter()
        .map(|r| {
            let k = r
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).into_owned());
            k.unwrap_or_default()
                .split('-')
                .next()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    assert!(!kinds.iter().any(|k| k == "aborted"), "{kinds:?}");
    assert_eq!(got.len(), 3 + 4 + 2, "{kinds:?}");
    assert_eq!(got[3].offset, cb, "committed records follow");
    assert_eq!(at, end);
    // from the aborted range: the broker may answer one batch at a time (it did with
    // Kafka 4.1: [aborted batch], then [marker, committed, ...]); positions still advance
    // past the aborted batch and the markers.
    let f = c
        .fetch_at(&tp(&topic, 0), ab, 1 << 20, Duration::from_millis(100))
        .await
        .unwrap();
    assert!(f.next_offset > ab, "progress past the aborted batch");
    let (from_aborted, _) = read_all(&c, &tp(&topic, 0), ab, end, 1 << 20).await;
    assert_eq!(from_aborted.first().map(|r| r.offset), Some(cb));
    assert_eq!(from_aborted.len(), 4 + 2);
}

#[tokio::test]
async fn fetch_beyond_the_high_watermark_is_offset_out_of_range() {
    let Some(b) = bootstrap() else { return };
    let c = Client::connect(config(&b)).await.unwrap();
    let topic = unique("oor");
    testkit::create_topic(&c, &topic, 1).await.unwrap();
    testkit::produce(&c, &topic, 0, &records("x", 10, 0), Compression::None)
        .await
        .unwrap();
    match c.fetch(&tp(&topic, 0), 1000, 1 << 20).await {
        Err(KafkaError::Broker { api, code: 1 }) => assert_eq!(api, "Fetch"),
        other => panic!("{other:?}"),
    }
    // at the high watermark: an empty answer, not an error
    let f = c
        .fetch_at(&tp(&topic, 0), 10, 1 << 20, Duration::from_millis(50))
        .await
        .unwrap();
    assert!(f.batch.records.is_empty());
    assert_eq!((f.batch.high_watermark, f.next_offset), (10, 10));
}

// ------------------------------------------------------------------------------ proxy

/// A TCP proxy in front of one broker that rewrites the broker addresses in Metadata and
/// FindCoordinator answers to itself, and can drop every connection.
struct Proxy {
    addr: std::net::SocketAddr,
    kill: watch::Sender<u64>,
    connections: Arc<Mutex<usize>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Proxy {
    async fn start(upstream: String) -> Proxy {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (kill, _) = watch::channel(0u64);
        let connections = Arc::new(Mutex::new(0));
        let (k, n) = (kill.clone(), Arc::clone(&connections));
        let task = tokio::spawn(async move {
            while let Ok((sock, _)) = l.accept().await {
                *n.lock().unwrap() += 1;
                let up = upstream.clone();
                let rx = k.subscribe();
                tokio::spawn(async move {
                    if let Ok(up) = TcpStream::connect(&up).await {
                        pipe(sock, up, addr.port(), rx).await;
                    }
                });
            }
        });
        Proxy {
            addr,
            kill,
            connections,
            task,
        }
    }

    fn drop_all(&self) {
        self.kill.send_modify(|g| *g += 1);
    }

    fn connections(&self) -> usize {
        *self.connections.lock().unwrap()
    }
}

async fn read_frame(s: &mut (impl AsyncReadExt + Unpin)) -> Option<Bytes> {
    let n = s.read_i32().await.ok()?;
    let mut b = vec![0u8; n.max(0) as usize];
    s.read_exact(&mut b).await.ok()?;
    Some(Bytes::from(b))
}

fn reframe<R: Encodable + HeaderVersion>(corr: i32, v: i16, r: &R) -> Bytes {
    let mut b = BytesMut::new();
    b.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(corr)
        .encode(&mut b, R::header_version(v))
        .unwrap();
    r.encode(&mut b, v).unwrap();
    let len = (b.len() - 4) as i32;
    b[..4].copy_from_slice(&len.to_be_bytes());
    b.freeze()
}

fn rewrite(frame: Bytes, api: i16, v: i16, port: u16) -> Bytes {
    let mut body = frame.clone();
    let corr = body.get_i32();
    let raw = |f: &Bytes| {
        let mut b = BytesMut::new();
        b.put_i32(f.len() as i32);
        b.put_slice(f);
        b.freeze()
    };
    if api == ApiKey::Metadata as i16 {
        let mut buf = frame.clone();
        ResponseHeader::decode(&mut buf, MetadataResponse::header_version(v)).unwrap();
        let mut r = MetadataResponse::decode(&mut buf, v).unwrap();
        for b in &mut r.brokers {
            b.host = StrBytes::from_static_str("127.0.0.1");
            b.port = i32::from(port);
        }
        return reframe(corr, v, &r);
    }
    if api == ApiKey::FindCoordinator as i16 {
        let mut buf = frame.clone();
        ResponseHeader::decode(&mut buf, FindCoordinatorResponse::header_version(v)).unwrap();
        let mut r = FindCoordinatorResponse::decode(&mut buf, v).unwrap();
        if r.error_code == 0 {
            r.host = StrBytes::from_static_str("127.0.0.1");
            r.port = i32::from(port);
        }
        return reframe(corr, v, &r);
    }
    raw(&frame)
}

async fn pipe(client: TcpStream, up: TcpStream, port: u16, mut kill: watch::Receiver<u64>) {
    let (mut cr, mut cw) = client.into_split();
    let (mut ur, mut uw) = up.into_split();
    let inflight: Arc<Mutex<HashMap<i32, (i16, i16)>>> = Arc::default();
    let seen = Arc::clone(&inflight);
    let to_up = async move {
        while let Some(f) = read_frame(&mut cr).await {
            if f.len() >= 8 {
                let mut h = f.clone();
                let (api, v, corr) = (h.get_i16(), h.get_i16(), h.get_i32());
                seen.lock().unwrap().insert(corr, (api, v));
            }
            let mut b = BytesMut::new();
            b.put_i32(f.len() as i32);
            b.put_slice(&f);
            if uw.write_all(&b).await.is_err() {
                return;
            }
        }
    };
    let to_client = async move {
        while let Some(f) = read_frame(&mut ur).await {
            let corr = i32::from_be_bytes([f[0], f[1], f[2], f[3]]);
            let (api, v) = inflight.lock().unwrap().remove(&corr).unwrap_or((-1, 0));
            let out = rewrite(f, api, v, port);
            if cw.write_all(&out).await.is_err() {
                return;
            }
        }
    };
    tokio::select! {
        _ = to_up => {}
        _ = to_client => {}
        _ = kill.changed() => {}
    }
}

#[tokio::test]
async fn socket_drops_reconnect_and_continue() {
    let Some(b) = bootstrap() else { return };
    let proxy = Proxy::start(b.clone()).await;
    let c = Client::connect(config(&proxy.addr.to_string()))
        .await
        .unwrap();
    let topic = unique("proxy");
    testkit::create_topic(&c, &topic, 1).await.unwrap();
    let first = records("before", 50, 0);
    testkit::produce(&c, &topic, 0, &first, Compression::Lz4)
        .await
        .unwrap();
    let (got, at) = read_all(&c, &tp(&topic, 0), 0, 50, 1 << 20).await;
    assert_eq!((got.len(), at), (50, 50));
    let before = proxy.connections();
    // 1. drop every socket between requests
    proxy.drop_all();
    tokio::time::sleep(Duration::from_millis(50)).await;
    testkit::produce(&c, &topic, 0, &records("after", 10, 0), Compression::Snappy)
        .await
        .unwrap();
    let f = c.fetch(&tp(&topic, 0), 50, 1 << 20).await.unwrap();
    assert_eq!(f.records.len(), 10);
    assert_eq!(f.records[0].offset, 50);
    // 2. drop them during a long-poll fetch at the end of the log
    let c = Arc::new(c);
    let poll = {
        let (c, t) = (Arc::clone(&c), topic.clone());
        tokio::spawn(async move {
            c.fetch_at(&tp(&t, 0), 60, 1 << 20, Duration::from_secs(3))
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    proxy.drop_all();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let producer = Client::connect(config(&b)).await.unwrap();
    testkit::produce(
        &producer,
        &topic,
        0,
        &records("late", 5, 0),
        Compression::Gzip,
    )
    .await
    .unwrap();
    let f = poll.await.unwrap().unwrap();
    assert_eq!(f.batch.records.first().map(|r| r.offset), Some(60));
    assert_eq!(f.next_offset, 65);
    assert!(
        proxy.connections() > before,
        "reconnected ({before} -> {})",
        proxy.connections()
    );
    println!(
        "proxy connections: {} (before the drops: {before})",
        proxy.connections()
    );
}

// ------------------------------------------------------------------------------ SASL / TLS

#[tokio::test]
async fn fetch_over_sasl_and_tls_listeners() {
    let Some(b) = bootstrap() else { return };
    let vars = [
        "OSTRO_TEST_KAFKA_SASL_BOOTSTRAP",
        "OSTRO_TEST_KAFKA_SASL_USER",
        "OSTRO_TEST_KAFKA_SASL_PASSWORD",
        "OSTRO_TEST_KAFKA_TLS_BOOTSTRAP",
        "OSTRO_TEST_KAFKA_TLS_CA",
    ];
    let mut v = Vec::new();
    for n in vars {
        match std::env::var(n) {
            Ok(x) if !x.is_empty() => v.push(x),
            _ => {
                println!("SKIP: {n} is unset (start the broker with scripts/test-services.sh)");
                return;
            }
        }
    }
    let plain = Client::connect(config(&b)).await.unwrap();
    let topic = unique("secure");
    testkit::create_topic(&plain, &topic, 1).await.unwrap();
    let rs = records("secure", 30, 0);
    testkit::produce(&plain, &topic, 0, &rs, Compression::Zstd)
        .await
        .unwrap();
    let sasl = Client::connect(ClientConfig {
        sasl: Some(SaslConfig {
            mechanism: SaslMechanism::ScramSha512,
            username: v[1].clone(),
            password: zeroize::Zeroizing::new(v[2].clone()),
        }),
        ..config(&v[0])
    })
    .await
    .unwrap();
    let tls = Client::connect(ClientConfig {
        tls: Some(TlsConfig {
            ca_file: Some(PathBuf::from(&v[4])),
            server_name: None,
        }),
        ..config(&v[3])
    })
    .await
    .unwrap();
    for (name, c) in [("SASL_PLAINTEXT/SCRAM-SHA-512", &sasl), ("SSL", &tls)] {
        let (got, _) = read_all(c, &tp(&topic, 0), 0, 30, 1 << 20).await;
        assert_eq!(got.len(), 30, "{name}");
        let group = unique("secure-group");
        c.commit(&group, &[(tp(&topic, 0), 30)]).await.unwrap();
        assert_eq!(
            c.committed(&group, &[tp(&topic, 0)]).await.unwrap()[0].1,
            Some(30),
            "{name}"
        );
        println!("{name}: fetched 30 records, committed and read back the offset");
    }
}
