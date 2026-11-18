//! Metadata commands, keys and results, plus their wire encoding.
//!
//! Encoding: `[META_FORMAT] ‖ postcard(value)`. Decoders reject any other format
//! byte, so a future format can be introduced without misreading old entries.

use serde::{Deserialize, Serialize};

use crate::records::{
    ActiveVersion, ApiToken, ClusterInfo, Connection, EgressPolicy, Grant, Member, ModuleRecord,
    PartitionAssignment, QueueConfig, Schedule, SealedSecret, Tenant, WorkflowVersion,
};

/// Format byte prefixed to every encoded command, result and snapshot.
pub const META_FORMAT: u8 = 1;

/// Every record kind. `MetaKind::ALL` lists them in a fixed order.
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
}

impl MetaKind {
    pub const ALL: [MetaKind; 14] = [
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
}

impl MetaKey {
    /// One key constructor per kind (`MetaKind::ALL` order). Handy for
    /// table-driven tests that must cover every kind.
    pub const ALL_KINDS: [MetaKind; 14] = MetaKind::ALL;

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
        }
    }
}

/// One record of any kind.
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
        }
    }

    /// Test constructor: a record of `kind` named/identified by `id`, with
    /// content derived from `generation`. Partition records use
    /// `epoch = generation`, so a CAS chain of generations 1, 2, 3 yields
    /// versions 1, 2, 3 for every kind.
    #[doc(hidden)]
    pub fn sample(kind: MetaKind, id: u8, generation: u32) -> MetaRecord {
        let g = generation;
        let name = format!("{kind:?}-{id}").to_lowercase();
        let ms = u64::from(g) * 1000;
        match kind {
            MetaKind::Cluster => MetaRecord::Cluster(ClusterInfo {
                cluster_id: format!("cluster-{id}-{g}"),
                partitions: 16,
                format: crate::records::CLUSTER_FORMAT,
                created_at_ms: ms,
            }),
            MetaKind::Member => MetaRecord::Member(Member {
                node_id: u64::from(id),
                addr: format!("10.0.0.{id}:{g}"),
                joined_at_ms: ms,
            }),
            MetaKind::Partition => MetaRecord::Partition(PartitionAssignment {
                partition: u16::from(id),
                leader: u64::from(g),
                epoch: g,
                isr: vec![u64::from(g)],
            }),
            MetaKind::Module => MetaRecord::Module(ModuleRecord {
                sha256: [id; 32],
                size: u64::from(g),
                language: Some("rust".into()),
                workflows: vec![(name, g)],
                uploaded_at_ms: ms,
            }),
            MetaKind::Workflow => MetaRecord::Workflow(WorkflowVersion {
                name,
                version: 1,
                module_sha256: [id; 32],
                default_queue: Some(format!("q{g}")),
                run_timeout_ms: Some(u64::from(g)),
                created_at_ms: ms,
            }),
            MetaKind::Active => MetaRecord::Active(ActiveVersion { name, version: g }),
            MetaKind::Queue => MetaRecord::Queue(QueueConfig {
                name,
                max_concurrency: Some(g),
                created_at_ms: ms,
            }),
            MetaKind::Schedule => MetaRecord::Schedule(Schedule {
                name,
                workflow: "wf".into(),
                version: Some(g),
                cron: "*/5 * * * *".into(),
                input: vec![id, g as u8],
                paused: g.is_multiple_of(2),
            }),
            MetaKind::Token => MetaRecord::Token(ApiToken {
                id: name,
                hash: [g as u8; 32],
                scopes: vec!["runs:write".into()],
                tenant: None,
                created_at_ms: ms,
            }),
            MetaKind::Tenant => MetaRecord::Tenant(Tenant {
                id: name,
                created_at_ms: ms,
            }),
            MetaKind::Secret => MetaRecord::Secret(SealedSecret {
                name,
                ciphertext: vec![g as u8; 8],
                key_id: "k1".into(),
            }),
            MetaKind::Grant => MetaRecord::Grant(Grant {
                id: name,
                subject: format!("user-{g}"),
                scopes: vec!["admin".into()],
            }),
            MetaKind::Connection => MetaRecord::Connection(Connection {
                name,
                kind: "postgres".into(),
                config: vec![g as u8],
            }),
            MetaKind::Egress => MetaRecord::Egress(EgressPolicy {
                name,
                allow: vec![format!("example{g}.com")],
            }),
        }
    }
}

/// A metadata state-machine command. Validation happens at apply, so every
/// node reaches the same verdict.
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

pub fn encode_result(result: &MetaResult) -> Vec<u8> {
    encode(result)
}

pub fn decode_result(bytes: &[u8]) -> Result<MetaResult, DecodeError> {
    decode(bytes)
}
