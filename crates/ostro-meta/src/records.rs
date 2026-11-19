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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub cluster_id: String,
    /// Fixed partition count; immutable after `InitCluster`.
    pub partitions: u16,
    pub format: u32,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub node_id: u64,
    pub addr: String,
    pub joined_at_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub partition: u16,
    pub leader: u64,
    pub epoch: u32,
    pub isr: Vec<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleRecord {
    pub sha256: [u8; 32],
    pub size: u64,
    pub language: Option<String>,
    /// `(workflow name, version)` pairs registered from this module.
    pub workflows: Vec<(String, u32)>,
    pub uploaded_at_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVersion {
    pub name: String,
    pub version: u32,
    pub module_sha256: [u8; 32],
    pub default_queue: Option<String>,
    pub run_timeout_ms: Option<u64>,
    pub created_at_ms: u64,
}

/// Which version of a workflow new runs start on.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveVersion {
    pub name: String,
    pub version: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    pub max_concurrency: Option<u32>,
    pub created_at_ms: u64,
}

/// Cron schedule (defined and persisted now; driven from Phase 8).
///
/// The fields after `paused` are the Phase 8 F2(a) shape, landed pre-freeze
/// (D20) so the frozen encoding already carries them. Stored only until
/// Phase 8; meta apply checks only the enum ranges and the timezone length.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schedule {
    pub name: String,
    pub workflow: String,
    pub version: Option<u32>,
    pub cron: String,
    pub input: Vec<u8>,
    pub paused: bool,
    /// IANA zone name; `""` = UTC. At most [`SCHEDULE_TIMEZONE_MAX`] bytes;
    /// the name itself is validated by Phase 8.
    pub timezone: String,
    /// [`OVERLAP_SKIP`] (0), [`OVERLAP_BUFFER_ONE`] (1), [`OVERLAP_ALLOW_ALL`]
    /// (2), [`OVERLAP_CANCEL_OTHER`] (3), [`OVERLAP_TERMINATE_OTHER`] (4).
    pub overlap: u8,
    /// [`CATCHUP_SKIP`] (0), [`CATCHUP_ONE`] (1), [`CATCHUP_ALL`] (2).
    pub catchup: u8,
    /// How far back missed fires are considered; 0 = Phase 8 default.
    pub catchup_window_ms: u64,
    /// No fire before this time (unix ms).
    pub start_at_ms: Option<u64>,
    /// No fire after this time (unix ms).
    pub end_at_ms: Option<u64>,
    /// Proposer-supplied creation time (unix ms).
    pub created_at_ms: u64,
    /// TLV extensions (RESERVATIONS §10b).
    pub ext: Vec<u8>,
}

/// Longest accepted [`Schedule::timezone`], in bytes.
pub const SCHEDULE_TIMEZONE_MAX: usize = 64;

pub const OVERLAP_SKIP: u8 = 0;
pub const OVERLAP_BUFFER_ONE: u8 = 1;
pub const OVERLAP_ALLOW_ALL: u8 = 2;
pub const OVERLAP_CANCEL_OTHER: u8 = 3;
pub const OVERLAP_TERMINATE_OTHER: u8 = 4;

pub const CATCHUP_SKIP: u8 = 0;
pub const CATCHUP_ONE: u8 = 1;
pub const CATCHUP_ALL: u8 = 2;

/// API token (stored now; enforced from Phase 14). `hash` is a digest of the
/// secret, never the secret itself.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: String,
    pub hash: [u8; 32],
    pub scopes: Vec<String>,
    pub tenant: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tenant {
    pub id: String,
    pub created_at_ms: u64,
}

/// A secret stored sealed. See [`TOKEN_KEY_SECRET`] for the Phase 4 caveat.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedSecret {
    pub name: String,
    pub ciphertext: Vec<u8>,
    pub key_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub id: String,
    pub subject: String,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Connection {
    pub name: String,
    pub kind: String,
    pub config: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EgressPolicy {
    pub name: String,
    pub allow: Vec<String>,
}

/// `on_retries_exhausted`: fail the run (the default).
pub const ON_EXHAUSTED_FAIL: u8 = 0;
/// `on_retries_exhausted`: pause the run (Phase 7 auto-pause).
pub const ON_EXHAUSTED_PAUSE: u8 = 1;

/// Per-workflow run policy (D9). Phase 5 reads `retention_ms` and
/// `schedule_to_start_ms` (05-11 resolver: request > WorkflowPolicy >
/// QueuePolicy > config). `paused` and `on_retries_exhausted` are the Phase 7
/// F2 fields: stored only, enforced by Phase 7.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowPolicy {
    /// Workflow name (tenant-qualified per D23); the record key.
    pub workflow: String,
    pub retention_ms: Option<u64>,
    pub schedule_to_start_ms: Option<u64>,
    pub paused: bool,
    /// [`ON_EXHAUSTED_FAIL`] (0) or [`ON_EXHAUSTED_PAUSE`] (1); `None` = default.
    pub on_retries_exhausted: Option<u8>,
    /// TLV extensions (RESERVATIONS §10b).
    pub ext: Vec<u8>,
}

pub const PLACEMENT_EMBEDDED: u8 = 0;
pub const PLACEMENT_WORKERS: u8 = 1;
pub const PLACEMENT_AUTO: u8 = 2;

/// At most this many [`QueuePolicy::selector`] entries.
pub const SELECTOR_MAX_ENTRIES: usize = 32;
/// Longest accepted [`QueuePolicy::selector`] entry, in bytes.
pub const SELECTOR_ENTRY_MAX: usize = 128;

/// Per-queue run policy (D9). Phase 5 reads `retention_ms` and
/// `schedule_to_start_ms`; every later field is Phase 7 F2 (flow control,
/// placement, pause, auto-pause): stored only, enforced by Phase 7.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuePolicy {
    /// Queue name (tenant-qualified per D23); the record key.
    pub queue: String,
    pub retention_ms: Option<u64>,
    pub schedule_to_start_ms: Option<u64>,
    /// [`PLACEMENT_EMBEDDED`] (0), [`PLACEMENT_WORKERS`] (1),
    /// [`PLACEMENT_AUTO`] (2); `None` = default.
    pub placement: Option<u8>,
    /// Worker label selector; at most [`SELECTOR_MAX_ENTRIES`] entries of at
    /// most [`SELECTOR_ENTRY_MAX`] bytes.
    pub selector: Vec<String>,
    /// When `placement = auto` spills from embedded execution to workers.
    pub spill: Option<SpillPolicy>,
    pub concurrency: Option<u32>,
    pub weight: Option<u32>,
    pub key_concurrency: Option<u32>,
    pub throttle: Option<RateSpec>,
    pub key_throttle: Option<RateSpec>,
    pub rate_limit: Option<RateSpec>,
    pub key_rate_limit: Option<RateSpec>,
    /// [`ON_EXHAUSTED_FAIL`] (0) or [`ON_EXHAUSTED_PAUSE`] (1); `None` = default.
    pub on_retries_exhausted: Option<u8>,
    pub paused: bool,
    /// TLV extensions (RESERVATIONS §10b).
    pub ext: Vec<u8>,
}

/// Auto-placement spill thresholds (Phase 7 F2). Percentages are 0..=100.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpillPolicy {
    pub cpu_pct: u8,
    pub slots_pct: u8,
    pub mem_pct: u8,
    pub lag_ms: u64,
    pub sustain_ms: u64,
    pub cooldown_ms: u64,
    pub hysteresis_pct: u8,
}

/// `limit` events per `period_ms` (must be > 0), with `burst` extra tokens.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateSpec {
    pub limit: u32,
    pub period_ms: u64,
    pub burst: u32,
}

pub const WEBHOOK_ACTION_START: u8 = 0;
pub const WEBHOOK_ACTION_SIGNAL: u8 = 1;
pub const WEBHOOK_ACTION_COMPLETE_TOKEN: u8 = 2;

pub const VERIFIER_NONE: u8 = 0;
pub const VERIFIER_STRIPE: u8 = 1;
pub const VERIFIER_GITHUB: u8 = 2;
pub const VERIFIER_SLACK: u8 = 3;
pub const VERIFIER_GENERIC_HMAC: u8 = 4;
pub const VERIFIER_STANDARD_WEBHOOKS: u8 = 5;

/// Inbound webhook (Phase 8 F3), landed pre-freeze so Phase 8 needs no meta
/// variant. Stored only until Phase 8; meta apply checks the enum ranges.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Webhook {
    /// Webhook name (tenant-qualified per D23); the record key.
    pub name: String,
    /// [`WEBHOOK_ACTION_START`] (0), [`WEBHOOK_ACTION_SIGNAL`] (1),
    /// [`WEBHOOK_ACTION_COMPLETE_TOKEN`] (2).
    pub action: u8,
    pub workflow: String,
    pub version: Option<u32>,
    /// Signal name (action = signal).
    pub signal_name: String,
    /// JSON pointer to the run id (signal) or the token (complete_token).
    pub run_ref: String,
    /// `""` = whole body, else a JSON pointer (start).
    pub input_ref: String,
    /// [`VERIFIER_NONE`] (0), [`VERIFIER_STRIPE`] (1), [`VERIFIER_GITHUB`] (2),
    /// [`VERIFIER_SLACK`] (3), [`VERIFIER_GENERIC_HMAC`] (4),
    /// [`VERIFIER_STANDARD_WEBHOOKS`] (5).
    pub verifier: u8,
    /// Verifier options blob for generic_hmac (header, prefix, ts header,
    /// template, algorithm, encoding); format owned by Phase 8.
    pub verifier_config: Vec<u8>,
    /// Sealed secret name (resolved by the Phase 6 resolver).
    pub secret_ref: String,
    /// `"header:<name>"`, `"json:<pointer>"` or `"none"`.
    pub idempotency: String,
    /// Replay window in seconds; 0 = verifier default.
    pub tolerance_s: u32,
    /// TLV extensions (RESERVATIONS §10b).
    pub ext: Vec<u8>,
}
