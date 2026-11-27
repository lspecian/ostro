//! 14-34: the read path against an in-process fake cluster (no broker needed).
//!
//! Two fake brokers (node 1 and node 2) share one cluster state and answer ApiVersions,
//! Metadata, FindCoordinator, ListOffsets, Fetch, OffsetFetch and OffsetCommit with
//! kafka-protocol's broker-side encodings. Knobs move partition leaders, answer
//! `COORDINATOR_NOT_AVAILABLE` / `NOT_COORDINATOR`, drop the socket on a Fetch, and serve
//! transactional batches with aborted-transaction lists.
//!
//! Covered: partitions and unknown topics; NOT_LEADER_OR_FOLLOWER on Fetch and ListOffsets
//! refreshes metadata and retries on the new leader; FindCoordinator retry while the
//! coordinator is not available and re-find after NOT_COORDINATOR; OffsetCommit is the
//! standalone form (generation -1, empty member id, no instance id); committed() on a fresh
//! group is all None; OFFSET_OUT_OF_RANGE is surfaced; a socket drop mid-fetch reconnects
//! and continues; read_committed filtering and next_offset; max_wait clamped below the
//! request timeout; per-partition commit errors surfaced.

#![cfg(feature = "testkit")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::fetch_response::{
    AbortedTransaction, FetchableTopicResponse, PartitionData,
};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, ProducerId, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{
    Decodable, Encodable, HeaderVersion, StrBytes, decode_request_header_from_buffer,
};
use ostro_kafka::testkit::{BatchOpts, Compression, encode_batch_with};
use ostro_kafka::{Client, ClientConfig, KafkaError, OffsetSpec, Record, TopicPartition};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOPIC: &str = "orders";

type Tp = (String, i32);

#[derive(Default)]
struct State {
    leaders: HashMap<Tp, i32>,
    partitions: HashMap<String, i32>,
    /// Encoded batches per partition, with their base and last offsets.
    logs: HashMap<Tp, Vec<(i64, i64, Bytes)>>,
    aborted: HashMap<Tp, Vec<(i64, i64)>>,
    committed: HashMap<(String, String, i32), i64>,
    /// `(node, api key)` of every request.
    seen: Vec<(i32, i16)>,
    /// `(generation, member id, instance id)` of every OffsetCommit.
    commits: Vec<(i32, String, Option<String>)>,
    fetch_waits: Vec<i32>,
    coordinator: i32,
    commit_errors: HashMap<i32, i16>,
}

#[derive(Default)]
struct Knobs {
    coord_unavailable: AtomicUsize,
    commit_not_coordinator: AtomicUsize,
    drop_fetch: AtomicUsize,
}

struct Cluster {
    addrs: Vec<SocketAddr>,
    state: Mutex<State>,
    knobs: Knobs,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for t in self.tasks.lock().unwrap().iter() {
            t.abort();
        }
    }
}

impl Cluster {
    async fn start(nodes: usize) -> Arc<Cluster> {
        let mut listeners = Vec::new();
        let mut addrs = Vec::new();
        for _ in 0..nodes {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(l.local_addr().unwrap());
            listeners.push(l);
        }
        let mut st = State {
            coordinator: 1,
            ..State::default()
        };
        st.partitions.insert(TOPIC.into(), 3);
        for p in 0..3 {
            st.leaders.insert((TOPIC.into(), p), 1);
        }
        let c = Arc::new(Cluster {
            addrs,
            state: Mutex::new(st),
            knobs: Knobs::default(),
            tasks: Mutex::new(Vec::new()),
        });
        for (i, l) in listeners.into_iter().enumerate() {
            let node = i as i32 + 1;
            let weak = Arc::downgrade(&c);
            let t = tokio::spawn(async move {
                while let Ok((sock, _)) = l.accept().await {
                    let Some(c) = weak.upgrade() else { return };
                    tokio::spawn(serve(sock, node, c));
                }
            });
            c.tasks.lock().unwrap().push(t);
        }
        c
    }

    fn bootstrap(&self) -> String {
        self.addrs[0].to_string()
    }

    fn config(&self) -> ClientConfig {
        ClientConfig {
            bootstrap: vec![self.bootstrap()],
            client_id: "support-read".into(),
            request_timeout: Duration::from_secs(5),
            ..ClientConfig::default()
        }
    }

    fn set_leader(&self, p: i32, node: i32) {
        self.state
            .lock()
            .unwrap()
            .leaders
            .insert((TOPIC.into(), p), node);
    }

    fn append(&self, p: i32, batch: Bytes, base: i64, last: i64) {
        self.state
            .lock()
            .unwrap()
            .logs
            .entry((TOPIC.into(), p))
            .or_default()
            .push((base, last, batch));
    }

    fn append_records(&self, p: i32, n: usize, c: Compression) -> Vec<Record> {
        let base = self.hw(p);
        let rs = recs(base, n);
        let b = encode_batch_with(
            &rs,
            &BatchOpts {
                base_offset: base,
                ..BatchOpts::new(c)
            },
        )
        .unwrap();
        self.append(p, b, base, base + n as i64 - 1);
        rs
    }

    fn hw(&self, p: i32) -> i64 {
        self.state
            .lock()
            .unwrap()
            .logs
            .get(&(TOPIC.into(), p))
            .and_then(|l| l.last().map(|b| b.1 + 1))
            .unwrap_or(0)
    }

    fn count(&self, node: i32, key: ApiKey) -> usize {
        let k = key as i16;
        self.state
            .lock()
            .unwrap()
            .seen
            .iter()
            .filter(|(n, a)| *n == node && *a == k)
            .count()
    }

    fn count_any(&self, key: ApiKey) -> usize {
        let k = key as i16;
        self.state
            .lock()
            .unwrap()
            .seen
            .iter()
            .filter(|(_, a)| *a == k)
            .count()
    }
}

fn recs(base: i64, n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| {
            let o = base + i as i64;
            Record {
                offset: o,
                timestamp_ms: 1_000 + o,
                key: Some(Bytes::from(format!("k{o}"))),
                value: Some(Bytes::from(format!("v{o}"))),
                headers: vec![("o".into(), Some(Bytes::from(o.to_string())))],
            }
        })
        .collect()
}

fn frame<R: Encodable + HeaderVersion>(corr: i32, version: i16, resp: &R) -> BytesMut {
    let mut b = BytesMut::new();
    b.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(corr)
        .encode(&mut b, R::header_version(version))
        .unwrap();
    resp.encode(&mut b, version).unwrap();
    let len = (b.len() - 4) as i32;
    b[..4].copy_from_slice(&len.to_be_bytes());
    b
}

fn sb(s: &str) -> StrBytes {
    StrBytes::from_string(s.to_owned())
}

fn kafka_41_ranges() -> Vec<(i16, i16, i16)> {
    vec![
        (18, 0, 4),
        (3, 0, 13),
        (10, 0, 6),
        (2, 1, 10),
        (1, 4, 17),
        (9, 1, 9),
        (8, 2, 9),
    ]
}

async fn serve(mut s: TcpStream, node: i32, c: Arc<Cluster>) {
    loop {
        let Ok(n) = s.read_i32().await else { return };
        let mut b = vec![0u8; n.max(0) as usize];
        if s.read_exact(&mut b).await.is_err() {
            return;
        }
        let mut buf = Bytes::from(b);
        let Ok(h) = decode_request_header_from_buffer(&mut buf) else {
            return;
        };
        let (key, v, corr) = (h.request_api_key, h.request_api_version, h.correlation_id);
        c.state.lock().unwrap().seen.push((node, key));
        let out = match ApiKey::try_from(key) {
            Ok(ApiKey::ApiVersions) => {
                let _ = ApiVersionsRequest::decode(&mut buf, v);
                let r = ApiVersionsResponse::default().with_api_keys(
                    kafka_41_ranges()
                        .into_iter()
                        .map(|(k, lo, hi)| {
                            ApiVersion::default()
                                .with_api_key(k)
                                .with_min_version(lo)
                                .with_max_version(hi)
                        })
                        .collect(),
                );
                frame(corr, v, &r)
            }
            Ok(ApiKey::Metadata) => {
                let req = MetadataRequest::decode(&mut buf, v).unwrap();
                assert!(!req.allow_auto_topic_creation, "auto-creation requested");
                frame(corr, v, &metadata(&c, &req, v))
            }
            Ok(ApiKey::FindCoordinator) => {
                let _ = FindCoordinatorRequest::decode(&mut buf, v).unwrap();
                let r = if take(&c.knobs.coord_unavailable) {
                    FindCoordinatorResponse::default()
                        .with_error_code(15)
                        .with_node_id(BrokerId(-1))
                        .with_host(sb(""))
                        .with_port(-1)
                } else {
                    let n = c.state.lock().unwrap().coordinator;
                    let a = c.addrs[(n - 1) as usize];
                    FindCoordinatorResponse::default()
                        .with_node_id(BrokerId(n))
                        .with_host(sb(&a.ip().to_string()))
                        .with_port(i32::from(a.port()))
                };
                frame(corr, v, &r)
            }
            Ok(ApiKey::ListOffsets) => {
                let req = ListOffsetsRequest::decode(&mut buf, v).unwrap();
                assert_eq!(req.replica_id.0, -1);
                assert_eq!(req.isolation_level, 1);
                let st = c.state.lock().unwrap();
                let topics = req
                    .topics
                    .iter()
                    .map(|t| {
                        let parts = t
                            .partitions
                            .iter()
                            .map(|p| {
                                let tp = (t.name.0.to_string(), p.partition_index);
                                let leader = st.leaders.get(&tp).copied();
                                let mut r = ListOffsetsPartitionResponse::default()
                                    .with_partition_index(p.partition_index)
                                    .with_timestamp(-1);
                                if leader != Some(node) {
                                    r = r.with_error_code(if leader.is_none() { 3 } else { 6 });
                                } else {
                                    let log = st.logs.get(&tp);
                                    let start =
                                        log.and_then(|l| l.first().map(|b| b.0)).unwrap_or(0);
                                    let end =
                                        log.and_then(|l| l.last().map(|b| b.1 + 1)).unwrap_or(0);
                                    r = r.with_offset(if p.timestamp == -2 { start } else { end });
                                }
                                r
                            })
                            .collect();
                        ListOffsetsTopicResponse::default()
                            .with_name(t.name.clone())
                            .with_partitions(parts)
                    })
                    .collect();
                drop(st);
                frame(corr, v, &ListOffsetsResponse::default().with_topics(topics))
            }
            Ok(ApiKey::Fetch) => {
                let req = FetchRequest::decode(&mut buf, v).unwrap();
                if take(&c.knobs.drop_fetch) {
                    return; // drop the socket instead of answering
                }
                frame(corr, v, &fetch(&c, &req, node))
            }
            Ok(ApiKey::OffsetFetch) => {
                let req = OffsetFetchRequest::decode(&mut buf, v).unwrap();
                let st = c.state.lock().unwrap();
                let group = req.group_id.0.to_string();
                let r = if node != st.coordinator {
                    OffsetFetchResponse::default().with_error_code(16)
                } else {
                    let topics = req
                        .topics
                        .iter()
                        .flatten()
                        .map(|t| {
                            OffsetFetchResponseTopic::default()
                                .with_name(t.name.clone())
                                .with_partitions(
                                    t.partition_indexes
                                        .iter()
                                        .map(|p| {
                                            let off = st
                                                .committed
                                                .get(&(group.clone(), t.name.0.to_string(), *p))
                                                .copied()
                                                .unwrap_or(-1);
                                            OffsetFetchResponsePartition::default()
                                                .with_partition_index(*p)
                                                .with_committed_offset(off)
                                                .with_metadata(Some(sb("")))
                                        })
                                        .collect(),
                                )
                        })
                        .collect();
                    OffsetFetchResponse::default().with_topics(topics)
                };
                drop(st);
                frame(corr, v, &r)
            }
            Ok(ApiKey::OffsetCommit) => {
                let req = OffsetCommitRequest::decode(&mut buf, v).unwrap();
                let not_coord = take(&c.knobs.commit_not_coordinator);
                let mut st = c.state.lock().unwrap();
                st.commits.push((
                    req.generation_id_or_member_epoch,
                    req.member_id.to_string(),
                    req.group_instance_id.as_ref().map(|s| s.to_string()),
                ));
                let group = req.group_id.0.to_string();
                let is_coord = node == st.coordinator && !not_coord;
                let mut topics = Vec::new();
                for t in &req.topics {
                    let mut parts = Vec::new();
                    for p in &t.partitions {
                        let code = if !is_coord {
                            16
                        } else {
                            st.commit_errors
                                .get(&p.partition_index)
                                .copied()
                                .unwrap_or(0)
                        };
                        if code == 0 {
                            st.committed.insert(
                                (group.clone(), t.name.0.to_string(), p.partition_index),
                                p.committed_offset,
                            );
                        }
                        parts.push(
                            OffsetCommitResponsePartition::default()
                                .with_partition_index(p.partition_index)
                                .with_error_code(code),
                        );
                    }
                    topics.push(
                        OffsetCommitResponseTopic::default()
                            .with_name(t.name.clone())
                            .with_partitions(parts),
                    );
                }
                drop(st);
                frame(
                    corr,
                    v,
                    &OffsetCommitResponse::default().with_topics(topics),
                )
            }
            _ => return,
        };
        if s.write_all(&out).await.is_err() {
            return;
        }
    }
}

fn take(a: &AtomicUsize) -> bool {
    let mut n = a.load(Ordering::SeqCst);
    while n > 0 {
        match a.compare_exchange(n, n - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(now) => n = now,
        }
    }
    false
}

fn metadata(c: &Cluster, req: &MetadataRequest, v: i16) -> MetadataResponse {
    let st = c.state.lock().unwrap();
    let brokers = c
        .addrs
        .iter()
        .enumerate()
        .map(|(i, a)| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(i as i32 + 1))
                .with_host(sb(&a.ip().to_string()))
                .with_port(i32::from(a.port()))
        })
        .collect();
    let topics = req
        .topics
        .iter()
        .flatten()
        .map(|t| {
            let name = t.name.as_ref().unwrap().0.to_string();
            match st.partitions.get(&name) {
                None => MetadataResponseTopic::default()
                    .with_name(Some(TopicName(sb(&name))))
                    .with_error_code(3),
                Some(&n) => MetadataResponseTopic::default()
                    .with_name(Some(TopicName(sb(&name))))
                    .with_partitions(
                        (0..n)
                            .map(|p| {
                                let leader = st.leaders[&(name.clone(), p)];
                                let mut mp = MetadataResponsePartition::default()
                                    .with_partition_index(p)
                                    .with_leader_id(BrokerId(leader))
                                    .with_replica_nodes(vec![BrokerId(leader)])
                                    .with_isr_nodes(vec![BrokerId(leader)]);
                                if v >= 7 {
                                    mp = mp.with_leader_epoch(1);
                                }
                                mp
                            })
                            .collect(),
                    ),
            }
        })
        .collect();
    MetadataResponse::default()
        .with_brokers(brokers)
        .with_cluster_id(Some(sb("fake")))
        .with_controller_id(BrokerId(1))
        .with_topics(topics)
}

fn fetch(c: &Cluster, req: &FetchRequest, node: i32) -> FetchResponse {
    let mut st = c.state.lock().unwrap();
    assert_eq!(req.isolation_level, 1);
    assert_eq!(req.min_bytes, 1);
    st.fetch_waits.push(req.max_wait_ms);
    let mut topics = Vec::new();
    for t in &req.topics {
        let mut parts = Vec::new();
        for p in &t.partitions {
            let tp = (t.topic.0.to_string(), p.partition);
            let leader = st.leaders.get(&tp).copied();
            let empty = Vec::new();
            let log = st.logs.get(&tp).unwrap_or(&empty);
            let hw = log.last().map(|b| b.1 + 1).unwrap_or(0);
            let mut pd = PartitionData::default()
                .with_partition_index(p.partition)
                .with_high_watermark(hw)
                .with_last_stable_offset(hw)
                .with_log_start_offset(0)
                .with_preferred_read_replica(BrokerId(-1));
            if leader != Some(node) {
                pd = pd.with_error_code(if leader.is_none() { 3 } else { 6 });
            } else if p.fetch_offset > hw || p.fetch_offset < 0 {
                pd = pd.with_error_code(1);
            } else {
                let mut out = BytesMut::new();
                for (_, last, b) in log {
                    if *last >= p.fetch_offset {
                        out.extend_from_slice(b);
                    }
                }
                let aborted = st
                    .aborted
                    .get(&tp)
                    .map(|a| {
                        a.iter()
                            .map(|(pid, first)| {
                                AbortedTransaction::default()
                                    .with_producer_id(ProducerId(*pid))
                                    .with_first_offset(*first)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                pd = pd
                    .with_aborted_transactions(Some(aborted))
                    .with_records(Some(out.freeze()));
            }
            parts.push(pd);
        }
        topics.push(
            FetchableTopicResponse::default()
                .with_topic(t.topic.clone())
                .with_partitions(parts),
        );
    }
    FetchResponse::default().with_responses(topics)
}

fn tp(p: i32) -> TopicPartition {
    TopicPartition {
        topic: TOPIC.into(),
        partition: p,
    }
}

#[tokio::test]
async fn partitions_and_unknown_topics() {
    let c = Cluster::start(2).await;
    let client = Client::connect(c.config()).await.unwrap();
    assert_eq!(client.partitions(TOPIC).await.unwrap(), vec![0, 1, 2]);
    match client.partitions("nope").await {
        Err(KafkaError::Broker { api, code: 3 }) => assert_eq!(api, "Metadata"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn fetch_follows_a_leader_change() {
    let c = Cluster::start(2).await;
    let want = c.append_records(0, 30, Compression::Lz4);
    let client = Client::connect(c.config()).await.unwrap();
    let f = client.fetch(&tp(0), 0, 1 << 20).await.unwrap();
    assert_eq!(f.records, want);
    assert_eq!(f.high_watermark, 30);
    assert_eq!(c.count(1, ApiKey::Fetch), 1);
    let md_before = c.count_any(ApiKey::Metadata);
    // the leader moves to node 2; node 1 answers NOT_LEADER_OR_FOLLOWER
    c.set_leader(0, 2);
    let more = c.append_records(0, 5, Compression::Zstd);
    let f = client.fetch(&tp(0), 30, 1 << 20).await.unwrap();
    assert_eq!(f.records, more);
    assert_eq!(
        c.count(1, ApiKey::Fetch),
        2,
        "one refused fetch on the old leader"
    );
    assert_eq!(c.count(2, ApiKey::Fetch), 1);
    assert!(
        c.count_any(ApiKey::Metadata) > md_before,
        "metadata refreshed"
    );
}

#[tokio::test]
async fn list_offsets_follow_a_leader_change() {
    let c = Cluster::start(2).await;
    c.append_records(1, 12, Compression::None);
    let client = Client::connect(c.config()).await.unwrap();
    let tps = [tp(0), tp(1), tp(2)];
    let latest = client.list_offsets(&tps, OffsetSpec::Latest).await.unwrap();
    assert_eq!(latest, vec![(tp(0), 0), (tp(1), 12), (tp(2), 0)]);
    c.set_leader(1, 2);
    c.set_leader(2, 2);
    let earliest = client
        .list_offsets(&tps, OffsetSpec::Earliest)
        .await
        .unwrap();
    assert_eq!(earliest, vec![(tp(0), 0), (tp(1), 0), (tp(2), 0)]);
    assert!(c.count(2, ApiKey::ListOffsets) >= 1);
    let latest = client.list_offsets(&tps, OffsetSpec::Latest).await.unwrap();
    assert_eq!(latest[1], (tp(1), 12));
}

#[tokio::test]
async fn coordinator_retry_and_standalone_commits() {
    let c = Cluster::start(2).await;
    c.knobs.coord_unavailable.store(2, Ordering::SeqCst);
    let client = Client::connect(c.config()).await.unwrap();
    let tps = [tp(0), tp(1)];
    let fresh = client.committed("g1", &tps).await.unwrap();
    assert_eq!(fresh, vec![(tp(0), None), (tp(1), None)]);
    assert_eq!(
        c.count_any(ApiKey::FindCoordinator),
        3,
        "two refusals, then found"
    );
    client
        .commit("g1", &[(tp(0), 7), (tp(1), 9)])
        .await
        .unwrap();
    assert_eq!(
        client.committed("g1", &tps).await.unwrap(),
        vec![(tp(0), Some(7)), (tp(1), Some(9))]
    );
    // the coordinator moves: NOT_COORDINATOR -> FindCoordinator again -> node 2
    c.state.lock().unwrap().coordinator = 2;
    client.commit("g1", &[(tp(0), 8)]).await.unwrap();
    assert_eq!(c.count_any(ApiKey::FindCoordinator), 4);
    assert_eq!(c.count(2, ApiKey::OffsetCommit), 1);
    // a one-off NOT_COORDINATOR answer on the right coordinator
    c.knobs.commit_not_coordinator.store(1, Ordering::SeqCst);
    client.commit("g1", &[(tp(1), 10)]).await.unwrap();
    assert_eq!(
        client.committed("g1", &tps).await.unwrap(),
        vec![(tp(0), Some(8)), (tp(1), Some(10))]
    );
    let commits = c.state.lock().unwrap().commits.clone();
    assert!(!commits.is_empty());
    for (generation, member, instance) in commits {
        assert_eq!((generation, member.as_str(), instance), (-1, "", None));
    }
}

#[tokio::test]
async fn commit_partition_errors_are_surfaced() {
    let c = Cluster::start(1).await;
    c.state.lock().unwrap().commit_errors.insert(1, 25); // UNKNOWN_MEMBER_ID
    let client = Client::connect(c.config()).await.unwrap();
    match client.commit("g", &[(tp(0), 1), (tp(1), 2)]).await {
        Err(KafkaError::Broker { api, code: 25 }) => assert_eq!(api, "OffsetCommit"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn offset_out_of_range_is_surfaced() {
    let c = Cluster::start(1).await;
    c.append_records(0, 3, Compression::Gzip);
    let client = Client::connect(c.config()).await.unwrap();
    match client.fetch(&tp(0), 50, 1 << 20).await {
        Err(KafkaError::Broker { api, code: 1 }) => assert_eq!(api, "Fetch"),
        other => panic!("{other:?}"),
    }
    assert_eq!(c.count_any(ApiKey::Fetch), 1, "not retried");
}

#[tokio::test]
async fn socket_drop_mid_fetch_reconnects_and_continues() {
    let c = Cluster::start(1).await;
    let want = c.append_records(2, 40, Compression::Snappy);
    let client = Client::connect(c.config()).await.unwrap();
    let f = client.fetch(&tp(2), 0, 1 << 20).await.unwrap();
    assert_eq!(f.records[..], want[..]);
    let more = c.append_records(2, 10, Compression::Gzip);
    c.knobs.drop_fetch.store(1, Ordering::SeqCst);
    let f = client.fetch(&tp(2), 40, 1 << 20).await.unwrap();
    assert_eq!(f.records, more);
    assert_eq!(c.count_any(ApiKey::Fetch), 3);
    assert!(
        c.count_any(ApiKey::ApiVersions) >= 3,
        "a new connection negotiated"
    );
}

#[tokio::test]
async fn read_committed_filtering_and_next_offset() {
    let c = Cluster::start(1).await;
    let plain = c.append_records(0, 5, Compression::None);
    let txn = |pid: i64, base: i64, n: usize| {
        encode_batch_with(
            &recs(base, n),
            &BatchOpts {
                base_offset: base,
                producer_id: pid,
                producer_epoch: 0,
                base_sequence: 0,
                transactional: true,
                ..BatchOpts::new(Compression::Gzip)
            },
        )
        .unwrap()
    };
    let marker = |pid: i64, base: i64, kind: i16| {
        let mut key = vec![0, 0];
        key.extend_from_slice(&kind.to_be_bytes());
        encode_batch_with(
            &[Record {
                key: Some(Bytes::from(key)),
                value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
                ..Record::default()
            }],
            &BatchOpts {
                base_offset: base,
                producer_id: pid,
                producer_epoch: 0,
                transactional: true,
                control: true,
                ..BatchOpts::new(Compression::None)
            },
        )
        .unwrap()
    };
    c.append(0, txn(9, 5, 4), 5, 8); // aborted
    c.append(0, marker(9, 9, 0), 9, 9);
    c.state
        .lock()
        .unwrap()
        .aborted
        .insert((TOPIC.into(), 0), vec![(9, 5)]);
    let client = Client::connect(c.config()).await.unwrap();
    let f = client
        .fetch_at(&tp(0), 0, 1 << 20, Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(f.batch.records, plain);
    assert_eq!(f.next_offset, 10, "past the aborted batch and its marker");
    assert_eq!(f.last_stable_offset, 10);
    // only skipped batches after the position: no records, progress
    let f = client
        .fetch_at(&tp(0), 5, 1 << 20, Duration::from_millis(10))
        .await
        .unwrap();
    assert!(f.batch.records.is_empty());
    assert_eq!(f.next_offset, 10);
    // `fetch` continues past them on its own
    let after = c.append_records(0, 3, Compression::Lz4);
    let b = client.fetch(&tp(0), 5, 1 << 20).await.unwrap();
    assert_eq!(b.records, after);
}

#[tokio::test]
async fn max_wait_stays_below_the_request_timeout() {
    let c = Cluster::start(1).await;
    c.append_records(0, 1, Compression::None);
    let cfg = ClientConfig {
        request_timeout: Duration::from_secs(2),
        ..c.config()
    };
    let client = Client::connect(cfg).await.unwrap();
    client
        .fetch_at(&tp(0), 0, 1 << 20, Duration::from_secs(60))
        .await
        .unwrap();
    client.fetch(&tp(0), 0, 1 << 20).await.unwrap();
    let waits = c.state.lock().unwrap().fetch_waits.clone();
    assert_eq!(waits, vec![1000, 500]);
}
