//! Metadata commands, keys and results, plus their wire encoding.
//!
//! Encoding: `[META_FORMAT] ‖ postcard(value)`. Decoders reject any other format
//! byte, so a future format can be introduced without misreading old entries.
//!
//! Format history (RESERVATIONS §10):
//! - `1`: Phase 4 dev format. Rejected with [`DecodeError::PreFreezeFormat`]
//!   ("delete the dev data dir"); no decoder is kept. This pre-freeze bump is
//!   the only one without a decoder (D20).
//! - `2`: Phase 5 (05-07): run policies, Webhook, the Phase 8 Schedule fields,
//!   the Phase 12/14 fields and a trailing `ext` on every record. Pinned by
//!   `tests/golden/meta_v2.txt` and frozen at 05-20.

use serde::{Deserialize, Serialize};

use crate::ext::ElectionInfo;
use crate::state::MetaState;

use crate::records::{
    ActiveVersion, ApiToken, ClusterInfo, Connection, EgressPolicy, Grant, Member, ModuleRecord,
    PartitionAssignment, QueueConfig, QueuePolicy, RateSpec, Schedule, SealedSecret, SpillPolicy,
    Tenant, Webhook, WorkflowPolicy, WorkflowVersion,
};

/// Format byte prefixed to every encoded command, result and snapshot.
pub const META_FORMAT: u8 = 2;

/// The Phase 4 (pre-Phase-5 dev) format byte, rejected with
/// [`DecodeError::PreFreezeFormat`].
pub const PRE_FREEZE_META_FORMAT: u8 = 1;

/// Every record kind. `MetaKind::ALL` lists them in a fixed order.
///
/// Variant order is positional postcard and fixed by `.planning/RESERVATIONS.md`
/// §10: new kinds are appended last, never inserted (D20).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MetaKind {
    Cluster,
    Member,
    Partition,
    Module,
    Workflow,
    Active,
    Queue,
    Schedule,
    Token,
    Tenant,
    Secret,
    Grant,
    Connection,
    Egress,
    WorkflowPolicy,
    QueuePolicy,
    Webhook,
}

impl MetaKind {
    pub const ALL: [MetaKind; 17] = [
        MetaKind::Cluster,
        MetaKind::Member,
        MetaKind::Partition,
        MetaKind::Module,
        MetaKind::Workflow,
        MetaKind::Active,
        MetaKind::Queue,
        MetaKind::Schedule,
        MetaKind::Token,
        MetaKind::Tenant,
        MetaKind::Secret,
        MetaKind::Grant,
        MetaKind::Connection,
        MetaKind::Egress,
        MetaKind::WorkflowPolicy,
        MetaKind::QueuePolicy,
        MetaKind::Webhook,
    ];
}

/// The key of one metadata record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MetaKey {
    Cluster,
    Member(u64),
    Partition(u16),
    Module([u8; 32]),
    Workflow(String, u32),
    Active(String),
    Queue(String),
    Schedule(String),
    Token(String),
    Tenant(String),
    Secret(String),
    Grant(String),
    Connection(String),
    Egress(String),
    WorkflowPolicy(String),
    QueuePolicy(String),
    Webhook(String),
}

impl MetaKey {
    /// One key constructor per kind (`MetaKind::ALL` order). Handy for
    /// table-driven tests that must cover every kind.
    pub const ALL_KINDS: [MetaKind; 17] = MetaKind::ALL;

    pub fn kind(&self) -> MetaKind {
        match self {
            MetaKey::Cluster => MetaKind::Cluster,
            MetaKey::Member(_) => MetaKind::Member,
            MetaKey::Partition(_) => MetaKind::Partition,
            MetaKey::Module(_) => MetaKind::Module,
            MetaKey::Workflow(..) => MetaKind::Workflow,
            MetaKey::Active(_) => MetaKind::Active,
            MetaKey::Queue(_) => MetaKind::Queue,
            MetaKey::Schedule(_) => MetaKind::Schedule,
            MetaKey::Token(_) => MetaKind::Token,
            MetaKey::Tenant(_) => MetaKind::Tenant,
            MetaKey::Secret(_) => MetaKind::Secret,
            MetaKey::Grant(_) => MetaKind::Grant,
            MetaKey::Connection(_) => MetaKind::Connection,
            MetaKey::Egress(_) => MetaKind::Egress,
            MetaKey::WorkflowPolicy(_) => MetaKind::WorkflowPolicy,
            MetaKey::QueuePolicy(_) => MetaKind::QueuePolicy,
            MetaKey::Webhook(_) => MetaKind::Webhook,
        }
    }
}

/// One record of any kind. Variant order = [`MetaKind`] order (positional).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaRecord {
    Cluster(ClusterInfo),
    Member(Member),
    Partition(PartitionAssignment),
    Module(ModuleRecord),
    Workflow(WorkflowVersion),
    Active(ActiveVersion),
    Queue(QueueConfig),
    Schedule(Schedule),
    Token(ApiToken),
    Tenant(Tenant),
    Secret(SealedSecret),
    Grant(Grant),
    Connection(Connection),
    Egress(EgressPolicy),
    WorkflowPolicy(WorkflowPolicy),
    QueuePolicy(QueuePolicy),
    Webhook(Webhook),
}

impl MetaRecord {
    pub fn kind(&self) -> MetaKind {
        self.key().kind()
    }

    /// The key this record must be stored under (derived from its identity
    /// fields). A `Put` whose key differs is rejected as `Invalid`.
    pub fn key(&self) -> MetaKey {
        match self {
            MetaRecord::Cluster(_) => MetaKey::Cluster,
            MetaRecord::Member(m) => MetaKey::Member(m.node_id),
            MetaRecord::Partition(p) => MetaKey::Partition(p.partition),
            MetaRecord::Module(m) => MetaKey::Module(m.sha256),
            MetaRecord::Workflow(w) => MetaKey::Workflow(w.name.clone(), w.version),
            MetaRecord::Active(a) => MetaKey::Active(a.name.clone()),
            MetaRecord::Queue(q) => MetaKey::Queue(q.name.clone()),
            MetaRecord::Schedule(s) => MetaKey::Schedule(s.name.clone()),
            MetaRecord::Token(t) => MetaKey::Token(t.id.clone()),
            MetaRecord::Tenant(t) => MetaKey::Tenant(t.id.clone()),
            MetaRecord::Secret(s) => MetaKey::Secret(s.name.clone()),
            MetaRecord::Grant(g) => MetaKey::Grant(g.id.clone()),
            MetaRecord::Connection(c) => MetaKey::Connection(c.name.clone()),
            MetaRecord::Egress(e) => MetaKey::Egress(e.name.clone()),
            MetaRecord::WorkflowPolicy(p) => MetaKey::WorkflowPolicy(p.workflow.clone()),
            MetaRecord::QueuePolicy(p) => MetaKey::QueuePolicy(p.queue.clone()),
            MetaRecord::Webhook(w) => MetaKey::Webhook(w.name.clone()),
        }
    }

    /// The record's trailing TLV `ext` bytes ([`crate::ext`]).
    pub fn ext(&self) -> &[u8] {
        match self {
            MetaRecord::Cluster(r) => &r.ext,
            MetaRecord::Member(r) => &r.ext,
            MetaRecord::Partition(r) => &r.ext,
            MetaRecord::Module(r) => &r.ext,
            MetaRecord::Workflow(r) => &r.ext,
            MetaRecord::Active(r) => &r.ext,
            MetaRecord::Queue(r) => &r.ext,
            MetaRecord::Schedule(r) => &r.ext,
            MetaRecord::Token(r) => &r.ext,
            MetaRecord::Tenant(r) => &r.ext,
            MetaRecord::Secret(r) => &r.ext,
            MetaRecord::Grant(r) => &r.ext,
            MetaRecord::Connection(r) => &r.ext,
            MetaRecord::Egress(r) => &r.ext,
            MetaRecord::WorkflowPolicy(r) => &r.ext,
            MetaRecord::QueuePolicy(r) => &r.ext,
            MetaRecord::Webhook(r) => &r.ext,
        }
    }

    /// Mutable access to the record's `ext` bytes.
    pub fn ext_mut(&mut self) -> &mut Vec<u8> {
        match self {
            MetaRecord::Cluster(r) => &mut r.ext,
            MetaRecord::Member(r) => &mut r.ext,
            MetaRecord::Partition(r) => &mut r.ext,
            MetaRecord::Module(r) => &mut r.ext,
            MetaRecord::Workflow(r) => &mut r.ext,
            MetaRecord::Active(r) => &mut r.ext,
            MetaRecord::Queue(r) => &mut r.ext,
            MetaRecord::Schedule(r) => &mut r.ext,
            MetaRecord::Token(r) => &mut r.ext,
            MetaRecord::Tenant(r) => &mut r.ext,
            MetaRecord::Secret(r) => &mut r.ext,
            MetaRecord::Grant(r) => &mut r.ext,
            MetaRecord::Connection(r) => &mut r.ext,
            MetaRecord::Egress(r) => &mut r.ext,
            MetaRecord::WorkflowPolicy(r) => &mut r.ext,
            MetaRecord::QueuePolicy(r) => &mut r.ext,
            MetaRecord::Webhook(r) => &mut r.ext,
        }
    }

    /// Test constructor: a record of `kind` named/identified by `id`, with
    /// content derived from `generation`. Partition records use
    /// `epoch = generation`, so a CAS chain of generations 1, 2, 3 yields
    /// versions 1, 2, 3 for every kind. Every sample carries a non-empty ext
    /// under the test-only tag [`crate::ext::EXT_TAG_TEST`].
    #[doc(hidden)]
    pub fn sample(kind: MetaKind, id: u8, generation: u32) -> MetaRecord {
        let mut rec = Self::sample_fields(kind, id, generation);
        *rec.ext_mut() =
            crate::ext::ext_put(&[], crate::ext::EXT_TAG_TEST, &[id, generation as u8])
                .expect("tag 15 is valid");
        rec
    }

    fn sample_fields(kind: MetaKind, id: u8, generation: u32) -> MetaRecord {
        let g = generation;
        let name = format!("{kind:?}-{id}").to_lowercase();
        let ms = u64::from(g) * 1000;
        match kind {
            MetaKind::Cluster => MetaRecord::Cluster(ClusterInfo {
                cluster_id: format!("cluster-{id}-{g}"),
                partitions: 16,
                format: crate::records::CLUSTER_FORMAT,
                created_at_ms: ms,
                ext: Vec::new(),
            }),
            MetaKind::Member => MetaRecord::Member(Member {
                node_id: u64::from(id),
                addr: format!("10.0.0.{id}:{g}"),
                joined_at_ms: ms,
                cluster_addr: Some(format!("10.0.0.{id}:7001")),
                repl_addr: None,
                ext: Vec::new(),
            }),
            MetaKind::Partition => MetaRecord::Partition(PartitionAssignment {
                partition: u16::from(id),
                leader: u64::from(g),
                epoch: g,
                isr: vec![u64::from(g)],
                replicas: vec![u64::from(g), u64::from(g) + 1],
                isr_version: u64::from(g),
                min_insync: 1,
                ext: Vec::new(),
            }),
            MetaKind::Module => MetaRecord::Module(ModuleRecord {
                sha256: [id; 32],
                size: u64::from(g),
                language: Some("rust".into()),
                workflows: vec![(name, g)],
                uploaded_at_ms: ms,
                ext: Vec::new(),
            }),
            MetaKind::Workflow => MetaRecord::Workflow(WorkflowVersion {
                name,
                version: 1,
                module_sha256: [id; 32],
                default_queue: Some(format!("q{g}")),
                run_timeout_ms: Some(u64::from(g)),
                created_at_ms: ms,
                module: format!("acme/mod-{g}"),
                ext: Vec::new(),
            }),
            MetaKind::Active => MetaRecord::Active(ActiveVersion {
                name,
                version: g,
                ext: Vec::new(),
            }),
            MetaKind::Queue => MetaRecord::Queue(QueueConfig {
                name,
                max_concurrency: Some(g),
                created_at_ms: ms,
                ext: Vec::new(),
            }),
            MetaKind::Schedule => MetaRecord::Schedule(Schedule {
                name,
                workflow: "wf".into(),
                version: Some(g),
                cron: "*/5 * * * *".into(),
                input: vec![id, g as u8],
                paused: g.is_multiple_of(2),
                timezone: "Europe/Lisbon".into(),
                overlap: (g % 5) as u8,
                catchup: (g % 3) as u8,
                catchup_window_ms: ms,
                start_at_ms: Some(ms),
                end_at_ms: None,
                created_at_ms: ms,
                ext: Vec::new(),
            }),
            MetaKind::Token => MetaRecord::Token(ApiToken {
                id: name,
                hash: [g as u8; 32],
                scopes: vec!["runs:write".into()],
                tenant: None,
                created_at_ms: ms,
                expires_at_ms: Some(ms + 3_600_000),
                ext: Vec::new(),
            }),
            MetaKind::Tenant => MetaRecord::Tenant(Tenant {
                id: name,
                created_at_ms: ms,
                weight: Some(g),
                max_concurrency: Some(10 * g),
                fuel_per_s: Some(1_000_000),
                fuel_burst: None,
                memory_bytes: Some(64 << 20),
                event_rate: Some(RateSpec {
                    limit: 100,
                    period_ms: 1000,
                    burst: 10,
                }),
                suspended: g.is_multiple_of(2),
                ext: Vec::new(),
            }),
            MetaKind::Secret => MetaRecord::Secret(SealedSecret {
                name,
                ciphertext: vec![g as u8; 8],
                key_id: "k1".into(),
                ext: Vec::new(),
            }),
            MetaKind::Grant => MetaRecord::Grant(Grant {
                id: name,
                subject: format!("user-{g}"),
                scopes: vec!["admin".into()],
                ext: Vec::new(),
            }),
            MetaKind::Connection => MetaRecord::Connection(Connection {
                name,
                kind: "postgres".into(),
                config: vec![g as u8],
                ext: Vec::new(),
            }),
            MetaKind::Egress => MetaRecord::Egress(EgressPolicy {
                name,
                allow: vec![format!("example{g}.com")],
                ext: Vec::new(),
            }),
            MetaKind::WorkflowPolicy => MetaRecord::WorkflowPolicy(WorkflowPolicy {
                workflow: name,
                retention_ms: Some(ms),
                schedule_to_start_ms: Some(u64::from(g)),
                paused: g.is_multiple_of(2),
                on_retries_exhausted: Some((g % 2) as u8),
                ext: Vec::new(),
            }),
            MetaKind::QueuePolicy => {
                let rate = RateSpec {
                    limit: g,
                    period_ms: 1000,
                    burst: 1,
                };
                MetaRecord::QueuePolicy(QueuePolicy {
                    queue: name,
                    retention_ms: Some(ms),
                    schedule_to_start_ms: None,
                    placement: Some((g % 3) as u8),
                    selector: vec![format!("zone=z{g}")],
                    spill: Some(SpillPolicy {
                        cpu_pct: 80,
                        slots_pct: 90,
                        mem_pct: 70,
                        lag_ms: ms,
                        sustain_ms: 5000,
                        cooldown_ms: 30_000,
                        hysteresis_pct: 10,
                    }),
                    concurrency: Some(g),
                    weight: Some(1),
                    key_concurrency: None,
                    throttle: Some(rate.clone()),
                    key_throttle: None,
                    rate_limit: Some(rate),
                    key_rate_limit: None,
                    on_retries_exhausted: Some(0),
                    paused: false,
                    ext: Vec::new(),
                })
            }
            MetaKind::Webhook => MetaRecord::Webhook(Webhook {
                name,
                action: (g % 3) as u8,
                workflow: "wf".into(),
                version: Some(g),
                signal_name: "paid".into(),
                run_ref: "/data/run".into(),
                input_ref: String::new(),
                verifier: (g % 6) as u8,
                verifier_config: vec![g as u8],
                secret_ref: "stripe".into(),
                idempotency: "header:Idempotency-Key".into(),
                tolerance_s: 300,
                ext: Vec::new(),
            }),
        }
    }
}

/// A metadata state-machine command. Validation happens at apply, so every
/// node reaches the same verdict.
// `Put` carries a whole MetaRecord (QueuePolicy is the largest). Commands are
// short-lived values proposed one at a time, so boxing buys nothing and would
// change the public construction syntax.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaCommand {
    /// Create the cluster record, the founding member, one partition
    /// assignment per partition (epoch 0, led by `node_id`) and the token key.
    /// Repeating it with the same partition count is an idempotent `Ok`; a
    /// different count is `Invalid`.
    InitCluster {
        cluster_id: String,
        node_id: u64,
        partitions: u16,
        created_at_ms: u64,
        token_key: [u8; 32],
    },
    /// Create (`expected_version: None`, key must be absent) or compare-and-set
    /// (`Some(v)`, current version must equal `v`) a record.
    ///
    /// Partition records: the version is the epoch, so the new version is
    /// `record.epoch`, which must exceed the current epoch.
    Put {
        key: MetaKey,
        expected_version: Option<u64>,
        record: MetaRecord,
    },
    /// Delete a record whose current version equals `expected_version`.
    Delete { key: MetaKey, expected_version: u64 },
    /// Fence a partition to a new leader/epoch. `epoch >= current` is accepted
    /// (an equal epoch with identical leader/isr is an idempotent re-assert);
    /// a lower epoch is `StaleVersion`.
    SetPartitionLeader {
        partition: u16,
        epoch: u32,
        leader: u64,
        isr: Vec<u64>,
    },
    /// Phase 12 (D-P12-7; shape 12-02, apply 12-05): elect `leader` at `epoch`
    /// with the (shrunk-only) `isr`, CAS on the partition's `isr_version`.
    /// `leader = 0` marks the partition offline. Writes ext tag 80 from `info`.
    /// Apply requires `epoch > current`, the new ISR non-empty and a subset of
    /// the published ISR, and a non-zero `leader` in both (see `MetaState`).
    ElectLeader {
        partition: u16,
        epoch: u32,
        leader: u64,
        isr: Vec<u64>,
        expected_isr_version: u64,
        info: ElectionInfo,
    },
    /// Phase 12 (D-P12-7; shape 12-02, apply 12-05): the leader at `epoch`
    /// publishes a new ISR, CAS on `isr_version`.
    AlterIsr {
        partition: u16,
        epoch: u32,
        expected_isr_version: u64,
        isr: Vec<u64>,
    },
    /// Phase 12 (D-P12-7; shape 12-02, apply 12-05): `node`, the leader at
    /// `epoch`, steps down (`epoch + 1`, `leader = 0`).
    ResignLeader {
        partition: u16,
        epoch: u32,
        node: u64,
        info: ElectionInfo,
    },
}

/// The verdict of applying a [`MetaCommand`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaResult {
    /// Applied (or idempotently re-applied); `version` is the record's version.
    Ok {
        version: u64,
    },
    /// `expected_version` (or epoch) did not match the current version.
    StaleVersion {
        current: u64,
    },
    /// A create hit an existing key, or an equal-epoch re-assert disagreed.
    Conflict {
        current: u64,
    },
    NotFound,
    Invalid(String),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("empty metadata payload")]
    Empty,
    #[error("unknown metadata format byte {0} (expected {META_FORMAT})")]
    UnknownFormat(u8),
    /// The payload was written by a pre-freeze dev build (Phase 4). Its
    /// records have a different positional shape; there is no decoder.
    #[error(
        "pre-Phase-5 dev meta format; delete the dev data dir (pre-freeze meta format byte {found}, this build reads {META_FORMAT})"
    )]
    PreFreezeFormat { found: u8 },
    #[error("malformed metadata payload: {0}")]
    Malformed(String),
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let mut out = vec![META_FORMAT];
    // postcard serialization of these plain data types cannot fail.
    out.extend(postcard::to_stdvec(value).expect("postcard encodes metadata types"));
    out
}

pub(crate) fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, DecodeError> {
    let (&format, body) = bytes.split_first().ok_or(DecodeError::Empty)?;
    if format == PRE_FREEZE_META_FORMAT {
        return Err(DecodeError::PreFreezeFormat { found: format });
    }
    if format != META_FORMAT {
        return Err(DecodeError::UnknownFormat(format));
    }
    let (value, rest) =
        postcard::take_from_bytes::<T>(body).map_err(|e| DecodeError::Malformed(e.to_string()))?;
    if !rest.is_empty() {
        return Err(DecodeError::Malformed(format!(
            "{} trailing bytes",
            rest.len()
        )));
    }
    Ok(value)
}

pub fn encode_command(cmd: &MetaCommand) -> Vec<u8> {
    encode(cmd)
}

pub fn decode_command(bytes: &[u8]) -> Result<MetaCommand, DecodeError> {
    decode(bytes)
}

/// `[META_FORMAT] ‖ postcard(MetaState)`: the Raft snapshot encoding.
pub fn encode_snapshot(state: &MetaState) -> Vec<u8> {
    encode(state)
}

/// Decode a Raft snapshot; a pre-freeze snapshot is
/// [`DecodeError::PreFreezeFormat`].
pub fn decode_snapshot(bytes: &[u8]) -> Result<MetaState, DecodeError> {
    decode(bytes)
}

pub fn encode_result(result: &MetaResult) -> Vec<u8> {
    encode(result)
}

pub fn decode_result(bytes: &[u8]) -> Result<MetaResult, DecodeError> {
    decode(bytes)
}

#[cfg(test)]
mod phase12_tests {
    use super::*;

    fn variant_byte(cmd: &MetaCommand) -> u8 {
        let bytes = encode_command(cmd);
        assert_eq!(bytes[0], META_FORMAT);
        bytes[1]
    }

    #[test]
    fn phase12_commands_take_ordinals_4_to_6_and_roundtrip() {
        let info = ElectionInfo {
            reason: crate::ext::ELECTION_REASON_FAILOVER,
            elected_at_ms: 5,
            prev_leader: 1,
        };
        let cmds = [
            (
                4u8,
                MetaCommand::ElectLeader {
                    partition: 3,
                    epoch: 7,
                    leader: 2,
                    isr: vec![2, 3],
                    expected_isr_version: 9,
                    info,
                },
            ),
            (
                5,
                MetaCommand::AlterIsr {
                    partition: 3,
                    epoch: 7,
                    expected_isr_version: 10,
                    isr: vec![1, 2, 3],
                },
            ),
            (
                6,
                MetaCommand::ResignLeader {
                    partition: 3,
                    epoch: 7,
                    node: 2,
                    info,
                },
            ),
        ];
        for (ordinal, cmd) in cmds {
            assert_eq!(variant_byte(&cmd), ordinal, "{cmd:?}");
            assert_eq!(decode_command(&encode_command(&cmd)).unwrap(), cmd);
        }
    }

    #[test]
    fn existing_command_encodings_are_unchanged() {
        // Byte-exact pins of ordinals 0-3 (the golden file covers them too).
        let spl = MetaCommand::SetPartitionLeader {
            partition: 1,
            epoch: 2,
            leader: 3,
            isr: vec![3],
        };
        assert_eq!(encode_command(&spl), [META_FORMAT, 3, 1, 2, 3, 1, 3]);
        let del = MetaCommand::Delete {
            key: MetaKey::Member(5),
            expected_version: 4,
        };
        assert_eq!(encode_command(&del), [META_FORMAT, 2, 1, 5, 4]);
        let init = MetaCommand::InitCluster {
            cluster_id: "c".into(),
            node_id: 1,
            partitions: 4,
            created_at_ms: 0,
            token_key: [0; 32],
        };
        let bytes = encode_command(&init);
        assert_eq!(&bytes[..6], [META_FORMAT, 0, 1, b'c', 1, 4]);
        let put = MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: None,
            record: MetaRecord::sample(MetaKind::Cluster, 1, 1),
        };
        assert_eq!(&encode_command(&put)[..4], [META_FORMAT, 1, 0, 0]);
    }

    #[test]
    fn phase12_apply_is_validated_since_12_05() {
        // The 12-02 stub (`Invalid("... 12-05")`) is gone: an unknown partition is NotFound.
        let mut st = MetaState::default();
        let r = st.apply(
            1,
            MetaCommand::AlterIsr {
                partition: 0,
                epoch: 1,
                expected_isr_version: 0,
                isr: vec![1],
            },
        );
        assert_eq!(r, MetaResult::NotFound);
    }
}
