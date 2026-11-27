//! Fetch (v4–12, read_committed) from the partition leader (14-34, D-P14-42).
//!
//! One partition per request: `isolation_level = 1`, `min_bytes = 1`, `max_wait` (kept at
//! most half of the request timeout: a request that outlives `request_timeout` closes its
//! connection, 14-31), sessionless (`session_id = 0`, `session_epoch = -1`). The partition's
//! `records` are decoded by [`crate::codec::decode_committed`] with the response's
//! `aborted_transactions`; records before the requested offset (a compressed batch starts
//! earlier) are dropped.
//!
//! Errors: `OFFSET_OUT_OF_RANGE` is returned to the caller as
//! `Broker { api: "Fetch", code: 1 }`; leader errors (`NOT_LEADER_OR_FOLLOWER`,
//! `UNKNOWN_TOPIC_OR_PARTITION`, ...) and connection failures drop the cached leader and
//! retry (bounded, with backoff).

use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::codec::{self, AbortedTxn};
use crate::conn::{MAX_FRAME, WireCheck};
use crate::metadata::{self, ATTEMPTS, retry_delay};
use crate::versions::{self, ApiRange, ApiRequest};
use crate::{Client, FetchedBatch, KafkaError, TopicPartition};

/// Long-poll wait used by [`Client::fetch`].
pub const DEFAULT_MAX_WAIT: Duration = Duration::from_millis(500);

/// Largest `max_bytes` sent (the response must fit in one frame).
pub const MAX_FETCH_BYTES: i32 = (MAX_FRAME / 2) as i32;

/// One fetch result with the offsets a consumer needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fetched {
    /// Records (offset >= the requested offset) and the high watermark.
    pub batch: FetchedBatch,
    /// Where the next fetch should start: after the last complete batch in the answer,
    /// including skipped control and aborted batches (= the requested offset when the answer
    /// held no complete batch).
    pub next_offset: i64,
    /// The last stable offset (read_committed end of the log).
    pub last_stable_offset: i64,
    /// The log start offset (-1 when the broker did not send it).
    pub log_start_offset: i64,
}

impl Client {
    /// Fetch from `tp` at `offset`: at most `max_bytes` (clamped to [`MAX_FETCH_BYTES`]),
    /// waiting up to `max_wait` (clamped to half the request timeout) for data.
    pub async fn fetch_at(
        &self,
        tp: &TopicPartition,
        offset: i64,
        max_bytes: i32,
        max_wait: Duration,
    ) -> Result<Fetched, KafkaError> {
        let max_bytes = max_bytes.clamp(1, MAX_FETCH_BYTES);
        let wait = max_wait.min(self.config.request_timeout / 2);
        let mut attempt = 0;
        loop {
            let res = self.fetch_once(tp, offset, max_bytes, wait).await;
            match res {
                Err(e) if metadata::retriable(&e) && attempt + 1 < ATTEMPTS => {
                    tracing::debug!(topic = %tp.topic, partition = tp.partition, attempt, error = %e, "kafka: fetch retry");
                    self.invalidate_topic(&tp.topic);
                    tokio::time::sleep(retry_delay(attempt)).await;
                    attempt += 1;
                }
                other => return other,
            }
        }
    }

    async fn fetch_once(
        &self,
        tp: &TopicPartition,
        offset: i64,
        max_bytes: i32,
        wait: Duration,
    ) -> Result<Fetched, KafkaError> {
        let (_, broker) = self.leader(tp).await?;
        let conn = broker.conn().await?;
        let v = conn.version(&versions::FETCH)?;
        let req = FetchRequest::default()
            .with_max_wait_ms(i32::try_from(wait.as_millis()).unwrap_or(i32::MAX))
            .with_min_bytes(1)
            .with_max_bytes(max_bytes)
            .with_isolation_level(1)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from_string(tp.topic.clone())))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(tp.partition)
                            .with_fetch_offset(offset)
                            .with_partition_max_bytes(max_bytes),
                    ]),
            ]);
        let r = conn.request_at(&req, v).await?;
        if r.error_code != 0 {
            return Err(KafkaError::Broker {
                api: "Fetch".into(),
                code: r.error_code,
            });
        }
        let part = r
            .responses
            .iter()
            .filter(|t| t.topic.0.as_str() == tp.topic)
            .flat_map(|t| t.partitions.iter())
            .find(|p| p.partition_index == tp.partition)
            .ok_or_else(|| {
                KafkaError::Protocol(format!(
                    "Fetch response does not answer {}/{}",
                    tp.topic, tp.partition
                ))
            })?;
        if part.error_code != 0 {
            return Err(KafkaError::Broker {
                api: "Fetch".into(),
                code: part.error_code,
            });
        }
        let aborted: Vec<AbortedTxn> = part
            .aborted_transactions
            .iter()
            .flatten()
            .map(|a| AbortedTxn {
                first_offset: a.first_offset,
                producer_id: a.producer_id.0,
            })
            .collect();
        let records = part.records.clone().unwrap_or_else(Bytes::new);
        let decoded = codec::decode_committed(&records, &aborted, offset)?;
        Ok(Fetched {
            batch: FetchedBatch {
                tp: Some(tp.clone()),
                records: decoded.records,
                high_watermark: part.high_watermark,
            },
            next_offset: decoded.next_offset.unwrap_or(offset).max(offset),
            last_stable_offset: part.last_stable_offset,
            log_start_offset: part.log_start_offset,
        })
    }

    /// Fetch from `tp` at `offset`, at most `max_bytes` (Fetch <= v12, read_committed),
    /// long-polling up to [`DEFAULT_MAX_WAIT`]. When an answer holds only control or aborted
    /// batches, the fetch continues after them (a few rounds), so the returned records can
    /// start after `offset`. Consumers that track their position should prefer
    /// [`Client::fetch_at`] and its `next_offset`.
    pub async fn fetch(
        &self,
        tp: &TopicPartition,
        offset: i64,
        max_bytes: i32,
    ) -> Result<FetchedBatch, KafkaError> {
        let mut at = offset;
        let mut wait = DEFAULT_MAX_WAIT;
        for _ in 0..4 {
            let f = self.fetch_at(tp, at, max_bytes, wait).await?;
            if !f.batch.records.is_empty() || f.next_offset <= at {
                return Ok(f.batch);
            }
            at = f.next_offset;
            wait = Duration::ZERO;
            if at >= f.batch.high_watermark {
                return Ok(f.batch);
            }
        }
        Ok(FetchedBatch {
            tp: Some(tp.clone()),
            ..FetchedBatch::default()
        })
    }
}

impl ApiRequest for FetchRequest {
    const API: ApiRange = versions::FETCH;
    type Response = FetchResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        let flex = version >= 12;
        let (s, a, t) = if flex { (1, 1, 1) } else { (2, 4, 0) };
        let mut w = WireCheck::new("Fetch", body);
        w.i32()?;
        if version >= 7 {
            w.i16()?;
            w.i32()?;
        }
        for _ in 0..w.array_len(flex, s + a + t)? {
            w.string(flex)?;
            let p_min = 4
                + 2
                + 8
                + 8
                + if version >= 5 { 8 } else { 0 }
                + a
                + if version >= 11 { 4 } else { 0 }
                + a
                + t;
            for _ in 0..w.array_len(flex, p_min)? {
                w.i32()?;
                w.i16()?;
                w.i64()?;
                w.i64()?;
                if version >= 5 {
                    w.i64()?;
                }
                for _ in 0..w.array_len(flex, 16 + t)? {
                    w.i64()?;
                    w.i64()?;
                    if flex {
                        w.tagged_fields()?;
                    }
                }
                if version >= 11 {
                    w.i32()?;
                }
                w.bytes(flex)?;
                if flex {
                    w.tagged_fields()?;
                }
            }
            if flex {
                w.tagged_fields()?;
            }
        }
        if flex {
            w.tagged_fields()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Response walkers of the read path: every valid encoding passes and decodes, every
    //! truncation and seeded garbage body is rejected or decodes without panicking, and
    //! array counts that cannot fit are rejected before kafka-protocol allocates.

    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::fetch_response::{
        AbortedTransaction, FetchableTopicResponse, PartitionData,
    };
    use kafka_protocol::messages::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };
    use kafka_protocol::messages::offset_commit_response::{
        OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    };
    use kafka_protocol::messages::offset_fetch_response::{
        OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    };
    use kafka_protocol::messages::{
        BrokerId, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
        ListOffsetsRequest, ListOffsetsResponse, OffsetCommitRequest, OffsetCommitResponse,
        OffsetFetchRequest, OffsetFetchResponse, ProducerId, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

    use crate::versions::ApiRequest;

    fn splitmix(s: &mut u64) -> u64 {
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn exercise<R: ApiRequest>(resp: &R::Response, v: i16)
    where
        R::Response: Encodable,
    {
        let mut b = BytesMut::new();
        resp.encode(&mut b, v).unwrap();
        let body = b.freeze();
        R::check_response(&body, v).unwrap_or_else(|e| panic!("{} v{v}: {e}", R::API.name));
        R::Response::decode(&mut body.clone(), v).unwrap();
        for cut in 0..body.len() {
            if R::check_response(&body[..cut], v).is_ok() {
                let _ = R::Response::decode(&mut body.slice(..cut), v);
            }
        }
        let mut seed = 0x5eed ^ u64::from(v as u16);
        for _ in 0..300 {
            let n = (splitmix(&mut seed) % 96) as usize;
            let g: Vec<u8> = (0..n).map(|_| splitmix(&mut seed) as u8).collect();
            if R::check_response(&g, v).is_ok() {
                let _ = R::Response::decode(&mut Bytes::from(g), v);
            }
            // flip one byte of the valid body
            if !body.is_empty() {
                let mut m = body.to_vec();
                let i = (splitmix(&mut seed) as usize) % m.len();
                m[i] = splitmix(&mut seed) as u8;
                if R::check_response(&m, v).is_ok() {
                    let _ = R::Response::decode(&mut Bytes::from(m), v);
                }
            }
        }
    }

    fn name(s: &'static str) -> TopicName {
        TopicName(StrBytes::from_static_str(s))
    }

    #[test]
    fn fetch_walker() {
        for v in 4..=12 {
            let mut p = PartitionData::default()
                .with_partition_index(2)
                .with_high_watermark(100)
                .with_last_stable_offset(90)
                .with_aborted_transactions(Some(vec![
                    AbortedTransaction::default()
                        .with_producer_id(ProducerId(7))
                        .with_first_offset(5),
                ]))
                .with_records(Some(Bytes::from_static(&[1, 2, 3, 4, 5])));
            if v >= 5 {
                p = p.with_log_start_offset(3);
            }
            if v >= 11 {
                p = p.with_preferred_read_replica(BrokerId(-1));
            }
            let mut r = FetchResponse::default().with_responses(vec![
                FetchableTopicResponse::default()
                    .with_topic(name("orders"))
                    .with_partitions(vec![p.clone(), p.with_partition_index(3)]),
            ]);
            if v >= 7 {
                r = r.with_error_code(0).with_session_id(0);
            }
            exercise::<FetchRequest>(&r, v);
        }
        // a partition array claiming 2^28 entries
        let mut body = vec![0u8; 4 + 2 + 4];
        body.extend_from_slice(&[2, 2, b'o']); // 1 topic, name "o" (flex)
        body.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x01]);
        let e = FetchRequest::check_response(&body, 12).unwrap_err();
        assert!(e.to_string().contains("cannot fit"), "{e}");
    }

    #[test]
    fn list_offsets_walker() {
        for v in 1..=7 {
            let mut p = ListOffsetsPartitionResponse::default()
                .with_partition_index(1)
                .with_offset(42)
                .with_timestamp(-1);
            if v >= 4 {
                p = p.with_leader_epoch(3);
            }
            let r = ListOffsetsResponse::default().with_topics(vec![
                ListOffsetsTopicResponse::default()
                    .with_name(name("t"))
                    .with_partitions(vec![p.clone(), p.with_partition_index(0)]),
            ]);
            exercise::<ListOffsetsRequest>(&r, v);
        }
    }

    #[test]
    fn find_coordinator_walker() {
        for v in 0..=3 {
            let mut r = FindCoordinatorResponse::default()
                .with_node_id(BrokerId(1))
                .with_host(StrBytes::from_static_str("127.0.0.1"))
                .with_port(9092);
            if v >= 1 {
                r = r.with_error_message(Some(StrBytes::from_static_str("x")));
            }
            exercise::<FindCoordinatorRequest>(&r, v);
        }
    }

    #[test]
    fn offset_fetch_walker() {
        for v in 1..=7 {
            let mut p = OffsetFetchResponsePartition::default()
                .with_partition_index(0)
                .with_committed_offset(17)
                .with_metadata(Some(StrBytes::from_static_str("")));
            if v >= 5 {
                p = p.with_committed_leader_epoch(2);
            }
            let r = OffsetFetchResponse::default().with_topics(vec![
                OffsetFetchResponseTopic::default()
                    .with_name(name("t"))
                    .with_partitions(vec![p.clone(), p.with_partition_index(1)]),
            ]);
            exercise::<OffsetFetchRequest>(&r, v);
        }
    }

    #[test]
    fn offset_commit_walker() {
        for v in 2..=8 {
            let p = OffsetCommitResponsePartition::default().with_partition_index(0);
            let r = OffsetCommitResponse::default().with_topics(vec![
                OffsetCommitResponseTopic::default()
                    .with_name(name("t"))
                    .with_partitions(vec![
                        p.clone(),
                        p.with_partition_index(1).with_error_code(25),
                    ]),
            ]);
            exercise::<OffsetCommitRequest>(&r, v);
        }
    }
}
