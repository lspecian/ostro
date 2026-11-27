//! Test-only producer helpers (feature `testkit`, 14-34): CreateTopics, Produce (acks = -1)
//! with every codec, transactional produce (commit or abort) and record-batch encoding.
//!
//! Producing is not part of the consumer client (D-P14-42); these helpers exist so tests
//! (this crate's broker suite, the Kafka trigger's tests in 14-32) can create topics and
//! write records. Compressors mirror the decoders in [`crate::codec`]: gzip `flate2`, snappy
//! `snap` in the xerial framing the Java client writes (raw blocks on request), lz4
//! `lz4_flex` frame (64 KiB independent blocks), zstd `ruzstd`.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, CreateTopicsRequest,
    CreateTopicsResponse, EndTxnRequest, EndTxnResponse, FindCoordinatorRequest,
    InitProducerIdRequest, InitProducerIdResponse, ProduceRequest, ProduceResponse, ProducerId,
    TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{self as kr, RecordBatchEncoder, RecordEncodeOptions, TimestampType};

pub use kafka_protocol::records::Compression;

use crate::codec::XERIAL_MAGIC;
use crate::conn::{Broker, WireCheck};
use crate::metadata::{self, ATTEMPTS, codes, retry_delay};
use crate::versions::{ApiRange, ApiRequest};
use crate::{Client, KafkaError, Record, TopicPartition};

/// Produce v3–8 (non-flexible; v3 is Kafka 4's minimum).
pub const PRODUCE: ApiRange = ApiRange {
    key: 0,
    name: "Produce",
    min: 3,
    max: 8,
};
/// CreateTopics v2–4.
pub const CREATE_TOPICS: ApiRange = ApiRange {
    key: 19,
    name: "CreateTopics",
    min: 2,
    max: 4,
};
/// InitProducerId v0–1.
pub const INIT_PRODUCER_ID: ApiRange = ApiRange {
    key: 22,
    name: "InitProducerId",
    min: 0,
    max: 1,
};
/// AddPartitionsToTxn v0–2.
pub const ADD_PARTITIONS_TO_TXN: ApiRange = ApiRange {
    key: 24,
    name: "AddPartitionsToTxn",
    min: 0,
    max: 2,
};
/// EndTxn v0–2.
pub const END_TXN: ApiRange = ApiRange {
    key: 26,
    name: "EndTxn",
    min: 0,
    max: 2,
};

const SMALL: usize = 1 << 20;

/// KAFKA error CONCURRENT_TRANSACTIONS.
const CONCURRENT_TRANSACTIONS: i16 = 51;

fn broker_err(api: &str, code: i16) -> KafkaError {
    KafkaError::Broker {
        api: api.into(),
        code,
    }
}

fn kerr(e: impl std::fmt::Display) -> KafkaError {
    KafkaError::Protocol(format!("testkit: {e}"))
}

// ------------------------------------------------------------------------------ compression

/// Compress `data` with `c` (`xerial`: snappy in the snappy-java stream framing).
pub fn compress(c: Compression, data: &[u8], xerial: bool) -> Result<Vec<u8>, KafkaError> {
    match c {
        Compression::None => Ok(data.to_vec()),
        Compression::Gzip => {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            e.write_all(data).map_err(kerr)?;
            e.finish().map_err(kerr)
        }
        Compression::Snappy => {
            let mut enc = snap::raw::Encoder::new();
            if !xerial {
                return enc.compress_vec(data).map_err(kerr);
            }
            let mut out = XERIAL_MAGIC.to_vec();
            out.extend_from_slice(&1i32.to_be_bytes());
            out.extend_from_slice(&1i32.to_be_bytes());
            for chunk in data.chunks(32 * 1024) {
                let block = enc.compress_vec(chunk).map_err(kerr)?;
                out.extend_from_slice(&(block.len() as i32).to_be_bytes());
                out.extend_from_slice(&block);
            }
            Ok(out)
        }
        Compression::Lz4 => {
            let info = lz4_flex::frame::FrameInfo::new()
                .block_size(lz4_flex::frame::BlockSize::Max64KB)
                .block_mode(lz4_flex::frame::BlockMode::Independent);
            let mut e = lz4_flex::frame::FrameEncoder::with_frame_info(info, Vec::new());
            e.write_all(data).map_err(kerr)?;
            e.finish().map_err(kerr)
        }
        Compression::Zstd => Ok(ruzstd::encoding::compress_to_vec(
            data,
            ruzstd::encoding::CompressionLevel::Fastest,
        )),
    }
}

// ------------------------------------------------------------------------------ batches

/// Batch-level options for [`encode_batch_with`].
#[derive(Clone, Copy, Debug)]
pub struct BatchOpts {
    /// Codec.
    pub compression: Compression,
    /// Snappy framing (true = xerial, what the Java client writes).
    pub xerial: bool,
    /// Offset of the first record (brokers reassign it on produce).
    pub base_offset: i64,
    /// Producer id (-1 = none).
    pub producer_id: i64,
    /// Producer epoch (-1 = none).
    pub producer_epoch: i16,
    /// First sequence number (-1 = none).
    pub base_sequence: i32,
    /// Transactional batch.
    pub transactional: bool,
    /// Control batch.
    pub control: bool,
}

impl BatchOpts {
    /// A plain batch with `compression`.
    pub fn new(compression: Compression) -> Self {
        BatchOpts {
            compression,
            xerial: true,
            base_offset: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            transactional: false,
            control: false,
        }
    }
}

/// Encode `records` as one v2 batch (offsets are `base_offset + index`; `Record::offset`
/// is ignored).
pub fn encode_batch(records: &[Record], compression: Compression) -> Result<Bytes, KafkaError> {
    encode_batch_with(records, &BatchOpts::new(compression))
}

/// Encode `records` as one v2 batch with explicit batch options.
pub fn encode_batch_with(records: &[Record], o: &BatchOpts) -> Result<Bytes, KafkaError> {
    let recs: Vec<kr::Record> = records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut headers: kafka_protocol::indexmap::IndexMap<StrBytes, Option<Bytes>> =
                Default::default();
            for (k, v) in &r.headers {
                headers.insert(StrBytes::from_string(k.clone()), v.clone());
            }
            kr::Record {
                transactional: o.transactional,
                control: o.control,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: o.producer_id,
                producer_epoch: o.producer_epoch,
                timestamp_type: TimestampType::Creation,
                offset: o.base_offset + i as i64,
                // kafka-protocol starts a new batch when `offset - sequence` changes, so the
                // sequence always advances with the offset; without a producer the encoded
                // base sequence stays -1.
                sequence: o.base_sequence.wrapping_add(i as i32),
                timestamp: r.timestamp_ms,
                key: r.key.clone(),
                value: r.value.clone(),
                headers,
            }
        })
        .collect();
    let xerial = o.xerial;
    let compressor = move |src: &mut BytesMut, dst: &mut BytesMut, c: Compression| {
        let out = compress(c, src, xerial).map_err(|e| anyhow::anyhow!("{e}"))?;
        dst.put_slice(&out);
        Ok(())
    };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        &recs,
        &RecordEncodeOptions {
            version: 2,
            compression: o.compression,
        },
        Some(compressor),
    )
    .map_err(kerr)?;
    Ok(buf.freeze())
}

// ------------------------------------------------------------------------------ admin + produce

/// Create `name` with `partitions` partitions (replication factor 1).
pub async fn create_topic(client: &Client, name: &str, partitions: i32) -> Result<(), KafkaError> {
    create_topic_with(client, name, partitions, &[]).await
}

/// Create `name` with topic configs (e.g. `compression.type`).
pub async fn create_topic_with(
    client: &Client,
    name: &str,
    partitions: i32,
    configs: &[(&str, &str)],
) -> Result<(), KafkaError> {
    let req = CreateTopicsRequest::default()
        .with_timeout_ms(30_000)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_string(name.to_owned())))
                .with_num_partitions(partitions)
                .with_replication_factor(1)
                .with_configs(
                    configs
                        .iter()
                        .map(|(k, v)| {
                            CreatableTopicConfig::default()
                                .with_name(StrBytes::from_string((*k).to_owned()))
                                .with_value(Some(StrBytes::from_string((*v).to_owned())))
                        })
                        .collect(),
                ),
        ]);
    let r = client.bootstrap().request(&req).await?;
    for t in &r.topics {
        if t.error_code != 0 {
            return Err(broker_err("CreateTopics", t.error_code));
        }
    }
    // Wait until the topic has leaders.
    let mut attempt = 0;
    loop {
        let ok = match client.partitions(name).await {
            Ok(ps) if ps.len() == partitions as usize => {
                let mut all = true;
                for p in ps {
                    let tp = TopicPartition {
                        topic: name.to_owned(),
                        partition: p,
                    };
                    all &= client.leader(&tp).await.is_ok();
                }
                all
            }
            _ => false,
        };
        if ok {
            return Ok(());
        }
        if attempt >= ATTEMPTS {
            return Err(broker_err("Metadata", codes::LEADER_NOT_AVAILABLE));
        }
        client.invalidate_topic(name);
        tokio::time::sleep(retry_delay(attempt)).await;
        attempt += 1;
    }
}

async fn produce_batch(
    client: &Client,
    tp: &TopicPartition,
    batch: Bytes,
    transactional_id: Option<&str>,
) -> Result<i64, KafkaError> {
    let req = ProduceRequest::default()
        .with_transactional_id(
            transactional_id.map(|t| TransactionalId(StrBytes::from_string(t.into()))),
        )
        .with_acks(-1)
        .with_timeout_ms(30_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(tp.topic.clone())))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(tp.partition)
                        .with_records(Some(batch)),
                ]),
        ]);
    let mut attempt = 0;
    loop {
        let res = async {
            let (_, b) = client.leader(tp).await?;
            let r = b.request(&req).await?;
            let p = r
                .responses
                .iter()
                .flat_map(|t| t.partition_responses.iter())
                .find(|p| p.index == tp.partition)
                .ok_or_else(|| kerr("Produce response without the partition"))?;
            if p.error_code != 0 {
                return Err(broker_err("Produce", p.error_code));
            }
            Ok(p.base_offset)
        }
        .await;
        match res {
            Err(e)
                if attempt + 1 < ATTEMPTS
                    && (metadata::retriable(&e)
                        || matches!(
                            e,
                            KafkaError::Broker {
                                code: codes::NOT_ENOUGH_REPLICAS,
                                ..
                            }
                        )) =>
            {
                client.invalidate_topic(&tp.topic);
                tokio::time::sleep(retry_delay(attempt)).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}

/// Produce `records` to `topic`/`partition` as one batch with `compression` (acks = -1).
/// Returns the base offset the broker assigned.
pub async fn produce(
    client: &Client,
    topic: &str,
    partition: i32,
    records: &[Record],
    compression: Compression,
) -> Result<i64, KafkaError> {
    let tp = TopicPartition {
        topic: topic.to_owned(),
        partition,
    };
    produce_batch(client, &tp, encode_batch(records, compression)?, None).await
}

// ------------------------------------------------------------------------------ transactions

async fn txn_coordinator(client: &Client, txn: &str) -> Result<Arc<Broker>, KafkaError> {
    let mut attempt = 0;
    loop {
        let conn = client.bootstrap().conn().await?;
        let v = conn.version(&crate::versions::FIND_COORDINATOR)?;
        if v < 1 {
            return Err(kerr(
                "FindCoordinator v0 cannot find transaction coordinators",
            ));
        }
        let req = FindCoordinatorRequest::default()
            .with_key(StrBytes::from_string(txn.to_owned()))
            .with_key_type(1);
        let r = conn.request_at(&req, v).await?;
        if r.error_code == 0 {
            return Ok(client
                .meta
                .broker_at(r.node_id.0, &format!("{}:{}", r.host, r.port)));
        }
        if attempt + 1 >= ATTEMPTS * 2 {
            return Err(broker_err("FindCoordinator", r.error_code));
        }
        tokio::time::sleep(retry_delay(attempt)).await;
        attempt += 1;
    }
}

/// Retry `f` on coordinator-loading and concurrent-transaction answers.
async fn txn_retry<T, F, Fut>(f: F) -> Result<T, KafkaError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, KafkaError>>,
{
    let mut attempt = 0;
    loop {
        match f().await {
            Err(KafkaError::Broker { code, .. })
                if attempt + 1 < ATTEMPTS * 2
                    && matches!(
                        code,
                        codes::COORDINATOR_LOAD_IN_PROGRESS
                            | codes::COORDINATOR_NOT_AVAILABLE
                            | CONCURRENT_TRANSACTIONS
                    ) =>
            {
                tokio::time::sleep(retry_delay(attempt).max(Duration::from_millis(100))).await;
                attempt += 1;
            }
            other => return other,
        }
    }
}

/// Write `records` to `topic`/`partition` in one transaction (transactional id `txn`) and
/// commit (`commit = true`) or abort it. Returns the base offset of the records.
pub async fn produce_transaction(
    client: &Client,
    txn: &str,
    topic: &str,
    partition: i32,
    records: &[Record],
    commit: bool,
) -> Result<i64, KafkaError> {
    let coord = txn_coordinator(client, txn).await?;
    let tid = TransactionalId(StrBytes::from_string(txn.to_owned()));
    let init = txn_retry(|| {
        let req = InitProducerIdRequest::default()
            .with_transactional_id(Some(tid.clone()))
            .with_transaction_timeout_ms(60_000);
        let coord = Arc::clone(&coord);
        async move {
            let r = coord.request(&req).await?;
            if r.error_code != 0 {
                return Err(broker_err("InitProducerId", r.error_code));
            }
            Ok((r.producer_id.0, r.producer_epoch))
        }
    })
    .await?;
    let (pid, epoch) = init;
    txn_retry(|| {
        let req = AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(tid.clone())
            .with_v3_and_below_producer_id(ProducerId(pid))
            .with_v3_and_below_producer_epoch(epoch)
            .with_v3_and_below_topics(vec![
                AddPartitionsToTxnTopic::default()
                    .with_name(TopicName(StrBytes::from_string(topic.to_owned())))
                    .with_partitions(vec![partition]),
            ]);
        let coord = Arc::clone(&coord);
        async move {
            let r = coord.request(&req).await?;
            for t in &r.results_by_topic_v3_and_below {
                for p in &t.results_by_partition {
                    if p.partition_error_code != 0 {
                        return Err(broker_err("AddPartitionsToTxn", p.partition_error_code));
                    }
                }
            }
            Ok(())
        }
    })
    .await?;
    let tp = TopicPartition {
        topic: topic.to_owned(),
        partition,
    };
    let opts = BatchOpts {
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: 0,
        transactional: true,
        ..BatchOpts::new(Compression::None)
    };
    let base = produce_batch(client, &tp, encode_batch_with(records, &opts)?, Some(txn)).await?;
    txn_retry(|| {
        let req = EndTxnRequest::default()
            .with_transactional_id(tid.clone())
            .with_producer_id(ProducerId(pid))
            .with_producer_epoch(epoch)
            .with_committed(commit);
        let coord = Arc::clone(&coord);
        async move {
            let r = coord.request(&req).await?;
            if r.error_code != 0 {
                return Err(broker_err("EndTxn", r.error_code));
            }
            Ok(())
        }
    })
    .await?;
    Ok(base)
}

// ------------------------------------------------------------------------------ ApiRequest

impl ApiRequest for ProduceRequest {
    const API: ApiRange = PRODUCE;
    const MAX_RESPONSE: usize = SMALL;
    type Response = ProduceResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        let mut w = WireCheck::new("Produce", body);
        for _ in 0..w.array_len(false, 6)? {
            w.string(false)?;
            let p_min =
                4 + 2 + 8 + 8 + if version >= 5 { 8 } else { 0 } + if version >= 8 { 6 } else { 0 };
            for _ in 0..w.array_len(false, p_min)? {
                w.i32()?;
                w.i16()?;
                w.i64()?;
                w.i64()?;
                if version >= 5 {
                    w.i64()?;
                }
                if version >= 8 {
                    for _ in 0..w.array_len(false, 6)? {
                        w.i32()?;
                        w.string(false)?;
                    }
                    w.string(false)?;
                }
            }
        }
        w.i32()?;
        Ok(())
    }
}

impl ApiRequest for CreateTopicsRequest {
    const API: ApiRange = CREATE_TOPICS;
    const MAX_RESPONSE: usize = SMALL;
    type Response = CreateTopicsResponse;
    fn check_response(body: &[u8], _version: i16) -> Result<(), KafkaError> {
        let mut w = WireCheck::new("CreateTopics", body);
        w.i32()?;
        for _ in 0..w.array_len(false, 6)? {
            w.string(false)?;
            w.i16()?;
            w.string(false)?;
        }
        Ok(())
    }
}

impl ApiRequest for InitProducerIdRequest {
    const API: ApiRange = INIT_PRODUCER_ID;
    const MAX_RESPONSE: usize = SMALL;
    type Response = InitProducerIdResponse;
    fn check_response(body: &[u8], _version: i16) -> Result<(), KafkaError> {
        let mut w = WireCheck::new("InitProducerId", body);
        w.skip(4 + 2 + 8 + 2)
    }
}

impl ApiRequest for AddPartitionsToTxnRequest {
    const API: ApiRange = ADD_PARTITIONS_TO_TXN;
    const MAX_RESPONSE: usize = SMALL;
    type Response = AddPartitionsToTxnResponse;
    fn check_response(body: &[u8], _version: i16) -> Result<(), KafkaError> {
        let mut w = WireCheck::new("AddPartitionsToTxn", body);
        w.i32()?;
        for _ in 0..w.array_len(false, 6)? {
            w.string(false)?;
            for _ in 0..w.array_len(false, 6)? {
                w.skip(6)?;
            }
        }
        Ok(())
    }
}

impl ApiRequest for EndTxnRequest {
    const API: ApiRange = END_TXN;
    const MAX_RESPONSE: usize = SMALL;
    type Response = EndTxnResponse;
    fn check_response(body: &[u8], _version: i16) -> Result<(), KafkaError> {
        let mut w = WireCheck::new("EndTxn", body);
        w.skip(6)
    }
}
