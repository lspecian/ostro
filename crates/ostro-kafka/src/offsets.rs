//! Group coordinator and offsets for a standalone consumer (14-34, D-P14-42/43).
//!
//! - FindCoordinator (v0–3, `key_type = 0`): the group's coordinator, cached per group until
//!   a request answers `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE` or its connection
//!   fails. `COORDINATOR_LOAD_IN_PROGRESS` is retried on the same coordinator.
//! - OffsetFetch (v1–7, `require_stable = false`): committed offsets; `-1` = none (`None`).
//! - OffsetCommit (v2–8) in the standalone form: `generation_id = -1`, `member_id = ""`, no
//!   group instance id (no JoinGroup/SyncGroup; D-P14-43). Per-partition error codes are
//!   surfaced as `Broker { api: "OffsetCommit", code }`.
//! - ListOffsets (v1–7, `isolation_level = 1` read_committed, `replica_id = -1`): earliest
//!   (`-2`) / latest (`-1`, the last stable offset under read_committed), sent to each
//!   partition's leader with leader errors refreshing metadata and retrying.
//!
//! Every request goes over the 14-31 transport (`conn::Broker` handles, negotiated
//! versions) and every response is walked by `conn::WireCheck` before it is decoded.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
use kafka_protocol::messages::{
    BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, GroupId, ListOffsetsRequest,
    ListOffsetsResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use crate::conn::{Broker, WireCheck};
use crate::metadata::{self, ATTEMPTS, codes, retry_delay};
use crate::versions::{self, ApiRange, ApiRequest};
use crate::{Client, KafkaError, OffsetSpec, TopicPartition};

/// Control responses are small.
const CONTROL_MAX: usize = 1 << 20;
/// Offset responses scale with the partition count; still far below a data frame.
const OFFSETS_MAX: usize = 8 << 20;

/// Cached group coordinators.
#[derive(Debug, Default)]
pub struct Coordinators {
    by_group: Mutex<HashMap<String, Arc<Broker>>>,
}

impl Coordinators {
    fn get(&self, group: &str) -> Option<Arc<Broker>> {
        self.by_group
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(group)
            .cloned()
    }

    fn put(&self, group: &str, b: Arc<Broker>) {
        self.by_group
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(group.to_owned(), b);
    }

    /// Forget the coordinator of `group`.
    pub fn invalidate(&self, group: &str) {
        self.by_group
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(group);
    }
}

fn is_coordinator_error(code: i16) -> bool {
    matches!(
        code,
        codes::COORDINATOR_LOAD_IN_PROGRESS
            | codes::COORDINATOR_NOT_AVAILABLE
            | codes::NOT_COORDINATOR
    )
}

fn broker_err(api: &str, code: i16) -> KafkaError {
    KafkaError::Broker {
        api: api.into(),
        code,
    }
}

fn topic_name(s: &str) -> TopicName {
    TopicName(StrBytes::from_string(s.to_owned()))
}

/// Group `tps` by topic, keeping first-seen order.
fn by_topic(tps: &[TopicPartition]) -> Vec<(String, Vec<i32>)> {
    let mut out: Vec<(String, Vec<i32>)> = Vec::new();
    for tp in tps {
        match out.iter_mut().find(|(t, _)| *t == tp.topic) {
            Some((_, ps)) => ps.push(tp.partition),
            None => out.push((tp.topic.clone(), vec![tp.partition])),
        }
    }
    out
}

impl Client {
    /// The coordinator of `group` (FindCoordinator via the bootstrap broker, cached).
    pub async fn coordinator(&self, group: &str) -> Result<Arc<Broker>, KafkaError> {
        if let Some(b) = self.coordinators.get(group) {
            return Ok(b);
        }
        let conn = self.bootstrap.conn().await?;
        let v = conn.version(&versions::FIND_COORDINATOR)?;
        let mut req =
            FindCoordinatorRequest::default().with_key(StrBytes::from_string(group.into()));
        if v >= 1 {
            req = req.with_key_type(0);
        }
        let r = conn.request_at(&req, v).await?;
        if r.error_code != 0 {
            return Err(broker_err("FindCoordinator", r.error_code));
        }
        let b = self
            .meta
            .broker_at(r.node_id.0, &format!("{}:{}", r.host, r.port));
        self.coordinators.put(group, Arc::clone(&b));
        Ok(b)
    }

    /// Run `f` against the group coordinator, re-finding it on coordinator and connection
    /// errors (bounded attempts with backoff).
    async fn with_coordinator<T, F, Fut>(&self, group: &str, f: F) -> Result<T, KafkaError>
    where
        F: Fn(Arc<Broker>) -> Fut,
        Fut: std::future::Future<Output = Result<T, KafkaError>>,
    {
        let mut attempt = 0;
        loop {
            let res = match self.coordinator(group).await {
                Ok(b) => f(b).await,
                Err(e) => Err(e),
            };
            let retry = match &res {
                Err(KafkaError::Broker { code, .. }) if is_coordinator_error(*code) => {
                    if *code != codes::COORDINATOR_LOAD_IN_PROGRESS {
                        self.coordinators.invalidate(group);
                    }
                    true
                }
                Err(e) if metadata::is_conn_error(e) => {
                    self.coordinators.invalidate(group);
                    true
                }
                _ => false,
            };
            if !retry || attempt + 1 >= ATTEMPTS {
                return res;
            }
            tracing::debug!(group, attempt, error = ?res.err(), "kafka: coordinator retry");
            tokio::time::sleep(retry_delay(attempt)).await;
            attempt += 1;
        }
    }

    /// Committed offsets of `group` for `tps` (OffsetFetch; `None` = nothing committed).
    pub async fn committed(
        &self,
        group: &str,
        tps: &[TopicPartition],
    ) -> Result<Vec<(TopicPartition, Option<i64>)>, KafkaError> {
        let topics: Vec<OffsetFetchRequestTopic> = by_topic(tps)
            .into_iter()
            .map(|(t, ps)| {
                OffsetFetchRequestTopic::default()
                    .with_name(topic_name(&t))
                    .with_partition_indexes(ps)
            })
            .collect();
        let req = OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.into())))
            .with_topics(Some(topics));
        let resp = self
            .with_coordinator(group, |b| {
                let req = req.clone();
                async move {
                    let r = b.request(&req).await?;
                    if r.error_code != 0 {
                        return Err(broker_err("OffsetFetch", r.error_code));
                    }
                    for t in &r.topics {
                        for p in &t.partitions {
                            if is_coordinator_error(p.error_code) {
                                return Err(broker_err("OffsetFetch", p.error_code));
                            }
                        }
                    }
                    Ok(r)
                }
            })
            .await?;
        let mut got: HashMap<(String, i32), (i64, i16)> = HashMap::new();
        for t in &resp.topics {
            for p in &t.partitions {
                got.insert(
                    (t.name.0.to_string(), p.partition_index),
                    (p.committed_offset, p.error_code),
                );
            }
        }
        tps.iter()
            .map(|tp| match got.get(&(tp.topic.clone(), tp.partition)) {
                Some(&(_, code)) if code != 0 && code != codes::UNKNOWN_TOPIC_OR_PARTITION => {
                    Err(broker_err("OffsetFetch", code))
                }
                Some(&(off, 0)) if off >= 0 => Ok((tp.clone(), Some(off))),
                _ => Ok((tp.clone(), None)),
            })
            .collect()
    }

    /// Commit `offsets` for `group` (OffsetCommit, standalone: generation_id -1, empty
    /// member id). The offset is the next record to consume.
    pub async fn commit(
        &self,
        group: &str,
        offsets: &[(TopicPartition, i64)],
    ) -> Result<(), KafkaError> {
        let mut topics: Vec<OffsetCommitRequestTopic> = Vec::new();
        for (tp, off) in offsets {
            let p = OffsetCommitRequestPartition::default()
                .with_partition_index(tp.partition)
                .with_committed_offset(*off);
            match topics.iter_mut().find(|t| t.name.0.as_str() == tp.topic) {
                Some(t) => t.partitions.push(p),
                None => topics.push(
                    OffsetCommitRequestTopic::default()
                        .with_name(topic_name(&tp.topic))
                        .with_partitions(vec![p]),
                ),
            }
        }
        // Standalone consumer form (pre-KIP-848): no generation, no member, no instance id.
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.into())))
            .with_generation_id_or_member_epoch(-1)
            .with_member_id(StrBytes::default())
            .with_group_instance_id(None)
            .with_topics(topics);
        self.with_coordinator(group, |b| {
            let req = req.clone();
            async move {
                let r = b.request(&req).await?;
                let mut first: Option<i16> = None;
                for t in &r.topics {
                    for p in &t.partitions {
                        if p.error_code != 0 {
                            if is_coordinator_error(p.error_code) {
                                return Err(broker_err("OffsetCommit", p.error_code));
                            }
                            first.get_or_insert(p.error_code);
                        }
                    }
                }
                match first {
                    Some(code) => Err(broker_err("OffsetCommit", code)),
                    None => Ok(()),
                }
            }
        })
        .await
    }

    /// Start or end offsets of `tps` (ListOffsets to each leader, read_committed: the end is
    /// the last stable offset). Leader errors refresh metadata and retry.
    pub async fn list_offsets(
        &self,
        tps: &[TopicPartition],
        spec: OffsetSpec,
    ) -> Result<Vec<(TopicPartition, i64)>, KafkaError> {
        let ts = match spec {
            OffsetSpec::Earliest => -2,
            OffsetSpec::Latest => -1,
        };
        let mut found: HashMap<TopicPartition, i64> = HashMap::new();
        let mut todo: Vec<TopicPartition> = tps.to_vec();
        todo.sort();
        todo.dedup();
        let mut attempt = 0;
        loop {
            let mut failed: Vec<TopicPartition> = Vec::new();
            let mut last_err: Option<KafkaError> = None;
            // Group by leader.
            let mut groups: HashMap<i32, (Arc<Broker>, Vec<TopicPartition>)> = HashMap::new();
            for tp in &todo {
                match self.leader(tp).await {
                    Ok((node, b)) => groups
                        .entry(node)
                        .or_insert((b, Vec::new()))
                        .1
                        .push(tp.clone()),
                    Err(e) if metadata::retriable(&e) => {
                        failed.push(tp.clone());
                        last_err = Some(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            for (_, (broker, group)) in groups {
                match list_offsets_on(&broker, &group, ts).await {
                    Ok(answers) => {
                        for (tp, res) in answers {
                            match res {
                                Ok(off) => {
                                    found.insert(tp, off);
                                }
                                Err(code) if metadata::is_leader_error(code) => {
                                    self.invalidate_topic(&tp.topic);
                                    failed.push(tp);
                                    last_err = Some(broker_err("ListOffsets", code));
                                }
                                Err(code) => return Err(broker_err("ListOffsets", code)),
                            }
                        }
                    }
                    Err(e) if metadata::is_conn_error(&e) => {
                        for tp in &group {
                            self.invalidate_topic(&tp.topic);
                        }
                        failed.extend(group);
                        last_err = Some(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            if failed.is_empty() {
                break;
            }
            if attempt + 1 >= ATTEMPTS {
                return Err(last_err.unwrap_or_else(|| broker_err("ListOffsets", -1)));
            }
            tokio::time::sleep(retry_delay(attempt)).await;
            attempt += 1;
            todo = failed;
        }
        tps.iter()
            .map(|tp| {
                found.get(tp).map(|o| (tp.clone(), *o)).ok_or_else(|| {
                    KafkaError::Protocol(format!("ListOffsets did not answer {tp:?}"))
                })
            })
            .collect()
    }
}

/// One ListOffsets round to one leader: per-partition offset or error code.
async fn list_offsets_on(
    broker: &Broker,
    tps: &[TopicPartition],
    ts: i64,
) -> Result<Vec<(TopicPartition, Result<i64, i16>)>, KafkaError> {
    let c = broker.conn().await?;
    let v = c.version(&versions::LIST_OFFSETS)?;
    let topics = by_topic(tps)
        .into_iter()
        .map(|(t, ps)| {
            ListOffsetsTopic::default()
                .with_name(topic_name(&t))
                .with_partitions(
                    ps.into_iter()
                        .map(|p| {
                            ListOffsetsPartition::default()
                                .with_partition_index(p)
                                .with_timestamp(ts)
                        })
                        .collect(),
                )
        })
        .collect();
    let mut req = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_topics(topics);
    if v >= 2 {
        req = req.with_isolation_level(1);
    }
    let r = c.request_at(&req, v).await?;
    let mut by: HashMap<(String, i32), Result<i64, i16>> = HashMap::new();
    for t in &r.topics {
        for p in &t.partitions {
            let res = if p.error_code == 0 {
                Ok(p.offset)
            } else {
                Err(p.error_code)
            };
            by.insert((t.name.0.to_string(), p.partition_index), res);
        }
    }
    Ok(tps
        .iter()
        .map(|tp| {
            let res = by
                .remove(&(tp.topic.clone(), tp.partition))
                .unwrap_or(Err(codes::UNKNOWN_TOPIC_OR_PARTITION));
            (tp.clone(), res)
        })
        .collect())
}

// ------------------------------------------------------------------------------ ApiRequest

impl ApiRequest for FindCoordinatorRequest {
    const API: ApiRange = versions::FIND_COORDINATOR;
    const MAX_RESPONSE: usize = CONTROL_MAX;
    type Response = FindCoordinatorResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        let flex = version >= 3;
        let mut w = WireCheck::new("FindCoordinator", body);
        if version >= 1 {
            w.i32()?;
        }
        w.i16()?;
        if version >= 1 {
            w.string(flex)?;
        }
        w.i32()?;
        w.string(flex)?;
        w.i32()?;
        if flex {
            w.tagged_fields()?;
        }
        Ok(())
    }
}

impl ApiRequest for OffsetFetchRequest {
    const API: ApiRange = versions::OFFSET_FETCH;
    const MAX_RESPONSE: usize = OFFSETS_MAX;
    type Response = OffsetFetchResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        let flex = version >= 6;
        let (s, a, t) = if flex { (1, 1, 1) } else { (2, 4, 0) };
        let mut w = WireCheck::new("OffsetFetch", body);
        if version >= 3 {
            w.i32()?;
        }
        for _ in 0..w.array_len(flex, s + a + t)? {
            w.string(flex)?;
            let p_min = 4 + 8 + if version >= 5 { 4 } else { 0 } + s + 2 + t;
            for _ in 0..w.array_len(flex, p_min)? {
                w.i32()?;
                w.i64()?;
                if version >= 5 {
                    w.i32()?;
                }
                w.string(flex)?;
                w.i16()?;
                if flex {
                    w.tagged_fields()?;
                }
            }
            if flex {
                w.tagged_fields()?;
            }
        }
        if version >= 2 {
            w.i16()?;
        }
        if flex {
            w.tagged_fields()?;
        }
        Ok(())
    }
}

impl ApiRequest for OffsetCommitRequest {
    const API: ApiRange = versions::OFFSET_COMMIT;
    const MAX_RESPONSE: usize = OFFSETS_MAX;
    type Response = OffsetCommitResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        let flex = version >= 8;
        let (s, a, t) = if flex { (1, 1, 1) } else { (2, 4, 0) };
        let mut w = WireCheck::new("OffsetCommit", body);
        if version >= 3 {
            w.i32()?;
        }
        for _ in 0..w.array_len(flex, s + a + t)? {
            w.string(flex)?;
            for _ in 0..w.array_len(flex, 6 + t)? {
                w.i32()?;
                w.i16()?;
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

impl ApiRequest for ListOffsetsRequest {
    const API: ApiRange = versions::LIST_OFFSETS;
    const MAX_RESPONSE: usize = OFFSETS_MAX;
    type Response = ListOffsetsResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        let flex = version >= 6;
        let (s, a, t) = if flex { (1, 1, 1) } else { (2, 4, 0) };
        let mut w = WireCheck::new("ListOffsets", body);
        if version >= 2 {
            w.i32()?;
        }
        for _ in 0..w.array_len(flex, s + a + t)? {
            w.string(flex)?;
            let p_min = 4 + 2 + 8 + 8 + if version >= 4 { 4 } else { 0 } + t;
            for _ in 0..w.array_len(flex, p_min)? {
                w.i32()?;
                w.i16()?;
                w.i64()?;
                w.i64()?;
                if version >= 4 {
                    w.i32()?;
                }
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
