//! Metadata record types (META-02).
//!
//! Every record is plain serde data: `BTreeMap`/`Vec` only (no `HashMap`) and no
//! clocks. Every timestamp is supplied by the proposer as `*_at_ms`, so applying
//! the same command on any node yields byte-identical state.

use serde::{Deserialize, Serialize};

/// Format of the cluster record written by `InitCluster`.
pub const CLUSTER_FORMAT: u32 = 1;

/// Name of the [`SealedSecret`] that holds the await-token HMAC key.
///
/// Phase 4: `ciphertext` is the raw 32-byte key and `key_id` is
/// [`TOKEN_KEY_PLAINTEXT_ID`]. Real sealing (envelope encryption under a node or
/// operator key) arrives in Phase 14; until then the metadata directory must be
/// protected like the key itself.
pub const TOKEN_KEY_SECRET: &str = "__ostro/token-key";

/// `key_id` marking a "sealed" secret whose ciphertext is actually plaintext
/// (Phase 4 only; see [`TOKEN_KEY_SECRET`]).
pub const TOKEN_KEY_PLAINTEXT_ID: &str = "plaintext-v0";

/// A record plus its CAS version and the Raft index that last wrote it.
///
/// For [`PartitionAssignment`] records the version IS the partition epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Versioned<T> {
    pub version: u64,
    pub updated_index: u64,
    pub value: T,
}

impl<T> Versioned<T> {
    /// Map the value, keeping version and index.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Versioned<U> {
        Versioned {
            version: self.version,
            updated_index: self.updated_index,
            value: f(self.value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub cluster_id: String,
    /// Fixed partition count; immutable after `InitCluster`.
    pub partitions: u16,
    pub format: u32,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub node_id: u64,
    pub addr: String,
    pub joined_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub partition: u16,
    pub leader: u64,
    pub epoch: u32,
    pub isr: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleRecord {
    pub sha256: [u8; 32],
    pub size: u64,
    pub language: Option<String>,
    /// `(workflow name, version)` pairs registered from this module.
    pub workflows: Vec<(String, u32)>,
    pub uploaded_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVersion {
    pub name: String,
    pub version: u32,
    pub module_sha256: [u8; 32],
    pub default_queue: Option<String>,
    pub run_timeout_ms: Option<u64>,
    pub created_at_ms: u64,
}

/// Which version of a workflow new runs start on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveVersion {
    pub name: String,
    pub version: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    pub max_concurrency: Option<u32>,
    pub created_at_ms: u64,
}

/// Cron schedule (defined and persisted now; driven from Phase 8).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schedule {
    pub name: String,
    pub workflow: String,
    pub version: Option<u32>,
    pub cron: String,
    pub input: Vec<u8>,
    pub paused: bool,
}

/// API token (stored now; enforced from Phase 14). `hash` is a digest of the
/// secret, never the secret itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: String,
    pub hash: [u8; 32],
    pub scopes: Vec<String>,
    pub tenant: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tenant {
    pub id: String,
    pub created_at_ms: u64,
}

/// A secret stored sealed. See [`TOKEN_KEY_SECRET`] for the Phase 4 caveat.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedSecret {
    pub name: String,
    pub ciphertext: Vec<u8>,
    pub key_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub id: String,
    pub subject: String,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Connection {
    pub name: String,
    pub kind: String,
    pub config: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressPolicy {
    pub name: String,
    pub allow: Vec<String>,
}
