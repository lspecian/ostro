//! Metadata: named topics, partition leaders and the broker pool (14-34, D-P14-42).
//!
//! Metadata requests name their topics and never auto-create them. The answer fills a cache
//! of broker addresses (node id -> `host:port`) and partition leaders; per-broker
//! connections ([`Broker`], reconnecting on their own) are created lazily and kept per node
//! id. A cached topic is dropped ([`Client::invalidate_topic`]) when a request to its leader
//! fails with a leader error (`NOT_LEADER_OR_FOLLOWER`, `UNKNOWN_TOPIC_OR_PARTITION`,
//! `LEADER_NOT_AVAILABLE`, ...) or the connection fails, so the next lookup asks again.
//!
//! Metadata goes to the bootstrap broker first and, when that connection fails, to the other
//! known brokers.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::conn::{Broker, ConnectOptions};
use crate::{Client, KafkaError, TopicPartition};

/// Attempts of a request that fails with a retriable error (metadata refreshed in between).
pub const ATTEMPTS: u32 = 8;

/// Delay before retry `attempt` (0-based): 50 ms doubling, capped at 1 s.
pub fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(50u64.saturating_mul(1 << attempt.min(10))).min(Duration::from_secs(1))
}

/// Kafka error codes used by the read path.
pub mod codes {
    /// OFFSET_OUT_OF_RANGE.
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    /// UNKNOWN_TOPIC_OR_PARTITION.
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    /// LEADER_NOT_AVAILABLE.
    pub const LEADER_NOT_AVAILABLE: i16 = 5;
    /// NOT_LEADER_OR_FOLLOWER.
    pub const NOT_LEADER_OR_FOLLOWER: i16 = 6;
    /// REQUEST_TIMED_OUT.
    pub const REQUEST_TIMED_OUT: i16 = 7;
    /// REPLICA_NOT_AVAILABLE.
    pub const REPLICA_NOT_AVAILABLE: i16 = 9;
    /// COORDINATOR_LOAD_IN_PROGRESS.
    pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
    /// COORDINATOR_NOT_AVAILABLE.
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    /// NOT_COORDINATOR.
    pub const NOT_COORDINATOR: i16 = 16;
    /// NOT_ENOUGH_REPLICAS.
    pub const NOT_ENOUGH_REPLICAS: i16 = 19;
    /// TOPIC_ALREADY_EXISTS.
    pub const TOPIC_ALREADY_EXISTS: i16 = 36;
    /// KAFKA_STORAGE_ERROR.
    pub const KAFKA_STORAGE_ERROR: i16 = 56;
    /// FENCED_LEADER_EPOCH.
    pub const FENCED_LEADER_EPOCH: i16 = 74;
    /// UNKNOWN_LEADER_EPOCH.
    pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
    /// OFFSET_NOT_AVAILABLE.
    pub const OFFSET_NOT_AVAILABLE: i16 = 78;
}

/// Partition errors that mean "the leader moved or is not known yet": refresh and retry.
pub fn is_leader_error(code: i16) -> bool {
    use codes::*;
    matches!(
        code,
        UNKNOWN_TOPIC_OR_PARTITION
            | LEADER_NOT_AVAILABLE
            | NOT_LEADER_OR_FOLLOWER
            | REPLICA_NOT_AVAILABLE
            | KAFKA_STORAGE_ERROR
            | FENCED_LEADER_EPOCH
            | UNKNOWN_LEADER_EPOCH
            | OFFSET_NOT_AVAILABLE
            | REQUEST_TIMED_OUT
    )
}

/// Connection-level failures (the broker handle reconnects on the next request).
pub fn is_conn_error(e: &KafkaError) -> bool {
    matches!(e, KafkaError::Io(_) | KafkaError::Timeout)
}

/// One cached partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionInfo {
    /// Leader node id (-1 = none).
    pub leader: i32,
    /// Leader epoch (-1 = unknown).
    pub leader_epoch: i32,
    /// Partition error code from the last Metadata answer.
    pub error: i16,
}

/// One cached topic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopicInfo {
    /// Partitions by index.
    pub partitions: BTreeMap<i32, PartitionInfo>,
}

#[derive(Debug, Default)]
struct State {
    addrs: HashMap<i32, String>,
    pool: HashMap<i32, Arc<Broker>>,
    topics: HashMap<String, TopicInfo>,
}

/// The metadata cache and broker pool of one [`Client`].
#[derive(Debug)]
pub struct Metadata {
    opts: Arc<ConnectOptions>,
    state: Mutex<State>,
}

impl Metadata {
    /// An empty cache.
    pub fn new(opts: Arc<ConnectOptions>) -> Self {
        Metadata {
            opts,
            state: Mutex::new(State::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The pooled connection to `node` at `addr` (replaced when the address changed).
    pub fn broker_at(&self, node: i32, addr: &str) -> Arc<Broker> {
        let mut st = self.lock();
        if let Some(b) = st.pool.get(&node)
            && b.addr() == addr
        {
            return Arc::clone(b);
        }
        let b = Arc::new(Broker::new(addr, Arc::clone(&self.opts)));
        st.addrs.insert(node, addr.to_owned());
        st.pool.insert(node, Arc::clone(&b));
        b
    }

    /// The pooled connection to a known node.
    pub fn broker(&self, node: i32) -> Option<Arc<Broker>> {
        let addr = self.lock().addrs.get(&node).cloned()?;
        Some(self.broker_at(node, &addr))
    }

    /// Every known broker connection (for metadata fallback).
    fn known(&self) -> Vec<Arc<Broker>> {
        let nodes: Vec<(i32, String)> = self
            .lock()
            .addrs
            .iter()
            .map(|(n, a)| (*n, a.clone()))
            .collect();
        nodes
            .into_iter()
            .map(|(n, a)| self.broker_at(n, &a))
            .collect()
    }

    /// The cached topic.
    pub fn topic(&self, topic: &str) -> Option<TopicInfo> {
        self.lock().topics.get(topic).cloned()
    }

    /// Forget a topic.
    pub fn invalidate(&self, topic: &str) {
        self.lock().topics.remove(topic);
    }

    /// Apply a Metadata answer; returns the per-topic error codes.
    fn apply(&self, r: &MetadataResponse) -> HashMap<String, i16> {
        let mut st = self.lock();
        for b in &r.brokers {
            let addr = format!("{}:{}", b.host, b.port);
            let node = b.node_id.0;
            if st.addrs.get(&node) != Some(&addr) {
                st.pool.remove(&node);
                st.addrs.insert(node, addr);
            }
        }
        let mut errors = HashMap::new();
        for t in &r.topics {
            let Some(name) = t.name.as_ref() else {
                continue;
            };
            let name = name.0.to_string();
            errors.insert(name.clone(), t.error_code);
            if t.error_code != 0 {
                st.topics.remove(&name);
                continue;
            }
            let partitions = t
                .partitions
                .iter()
                .map(|p| {
                    (
                        p.partition_index,
                        PartitionInfo {
                            leader: p.leader_id.0,
                            leader_epoch: p.leader_epoch,
                            error: p.error_code,
                        },
                    )
                })
                .collect();
            st.topics.insert(name, TopicInfo { partitions });
        }
        errors
    }
}

impl Client {
    /// Send Metadata for `topics` (named, no auto-creation) to the bootstrap broker, falling
    /// back to the other known brokers on connection errors, and update the cache. Returns
    /// each topic's error code.
    pub async fn refresh_metadata(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, i16>, KafkaError> {
        let req = MetadataRequest::default()
            .with_topics(Some(
                topics
                    .iter()
                    .map(|t| {
                        MetadataRequestTopic::default()
                            .with_name(Some(TopicName(StrBytes::from_string((*t).to_owned()))))
                    })
                    .collect(),
            ))
            .with_allow_auto_topic_creation(false);
        let resp = match self.bootstrap.request(&req).await {
            Ok(r) => r,
            Err(e) if is_conn_error(&e) => {
                let mut last = e;
                let mut got = None;
                for b in self.meta.known() {
                    match b.request(&req).await {
                        Ok(r) => {
                            got = Some(r);
                            break;
                        }
                        Err(e) => last = e,
                    }
                }
                got.ok_or(last)?
            }
            Err(e) => return Err(e),
        };
        Ok(self.meta.apply(&resp))
    }

    /// Forget the cached leaders of `topic` (the next lookup sends Metadata).
    pub fn invalidate_topic(&self, topic: &str) {
        self.meta.invalidate(topic);
    }

    /// The topic's cached metadata, refreshing once when it is not cached. A topic error
    /// is `Broker { api: "Metadata", code }`.
    pub async fn topic_metadata(&self, topic: &str) -> Result<TopicInfo, KafkaError> {
        if let Some(t) = self.meta.topic(topic) {
            return Ok(t);
        }
        let errors = self.refresh_metadata(&[topic]).await?;
        match errors.get(topic).copied() {
            Some(0) => self
                .meta
                .topic(topic)
                .ok_or_else(|| KafkaError::Protocol(format!("metadata for {topic:?} vanished"))),
            Some(code) => Err(KafkaError::Broker {
                api: "Metadata".into(),
                code,
            }),
            None => Err(KafkaError::Protocol(format!(
                "metadata answer does not name topic {topic:?}"
            ))),
        }
    }

    /// The leader connection of `tp` (Metadata when not cached; a partition without a
    /// leader or unknown to the broker is a retriable `Broker` error and drops the cache).
    pub async fn leader(&self, tp: &TopicPartition) -> Result<(i32, Arc<Broker>), KafkaError> {
        let t = self.topic_metadata(&tp.topic).await?;
        let err = |code| {
            self.meta.invalidate(&tp.topic);
            KafkaError::Broker {
                api: "Metadata".into(),
                code,
            }
        };
        let Some(p) = t.partitions.get(&tp.partition) else {
            return Err(err(codes::UNKNOWN_TOPIC_OR_PARTITION));
        };
        if p.leader < 0 {
            return Err(err(if p.error != 0 {
                p.error
            } else {
                codes::LEADER_NOT_AVAILABLE
            }));
        }
        match self.meta.broker(p.leader) {
            Some(b) => Ok((p.leader, b)),
            None => Err(err(codes::LEADER_NOT_AVAILABLE)),
        }
    }

    /// Partition indexes of `topic`, sorted (Metadata; retried while the topic is unknown or
    /// leaderless, e.g. right after creation; then `Broker { api: "Metadata", code }`).
    pub async fn partitions(&self, topic: &str) -> Result<Vec<i32>, KafkaError> {
        let mut attempt = 0;
        loop {
            let res = async {
                let errors = self.refresh_metadata(&[topic]).await?;
                match errors.get(topic).copied() {
                    Some(0) => Ok(self
                        .meta
                        .topic(topic)
                        .map(|t| t.partitions.keys().copied().collect::<Vec<_>>())
                        .unwrap_or_default()),
                    Some(code) => Err(KafkaError::Broker {
                        api: "Metadata".into(),
                        code,
                    }),
                    None => Err(KafkaError::Protocol(format!(
                        "metadata answer does not name topic {topic:?}"
                    ))),
                }
            }
            .await;
            match res {
                Err(e) if attempt + 1 < ATTEMPTS && retriable(&e) => {
                    tokio::time::sleep(retry_delay(attempt)).await;
                    attempt += 1;
                }
                other => return other,
            }
        }
    }
}

/// Retriable for leader-addressed requests: connection errors and leader errors.
pub fn retriable(e: &KafkaError) -> bool {
    match e {
        KafkaError::Broker { code, .. } => is_leader_error(*code),
        e => is_conn_error(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delays_double_to_a_cap() {
        let d: Vec<u128> = (0..8).map(|a| retry_delay(a).as_millis()).collect();
        assert_eq!(d, [50, 100, 200, 400, 800, 1000, 1000, 1000]);
        assert!(is_leader_error(codes::NOT_LEADER_OR_FOLLOWER));
        assert!(!is_leader_error(codes::OFFSET_OUT_OF_RANGE));
        assert!(retriable(&KafkaError::Timeout));
        assert!(!retriable(&KafkaError::Broker {
            api: "Fetch".into(),
            code: 1
        }));
    }
}
