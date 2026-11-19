//! postcard value types. Every encoded value starts with a format byte (1).

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::StoreError;

/// Format byte prefixed to every stored value.
pub const VALUE_FORMAT: u8 = 1;

/// Leader/fencing cursor persisted in meta alongside `applied_offset`.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CursorMeta {
    pub epoch: u32,
    /// `(epoch, first offset of that epoch)`.
    pub epoch_history: Vec<(u32, u64)>,
    /// Next outbox sequence number per target partition.
    pub outbox_next_seq: BTreeMap<u16, u64>,
    /// Highest truncated outbox seq per target: rows up to and including this seq have
    /// been deleted (absent = 0, nothing truncated; seqs start at 1).
    pub outbox_truncated: BTreeMap<u16, u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Scheduled,
    Ready,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadySlot {
    pub priority: i32,
    pub ready_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    pub workflow: String,
    pub workflow_version: u32,
    pub queue: String,
    pub status: RunStatus,
    /// `(epoch, n)` of the current attempt token.
    pub current_attempt: Option<(u32, u32)>,
    pub next_seq: u64,
    pub ready: Option<ReadySlot>,
    /// `(parent run id bytes, op)`.
    pub parent: Option<([u8; 18], u64)>,
    pub cancel_requested: bool,
    pub concurrency_key: String,
    pub priority: i32,
    pub created_at_ms: u64,
    pub rng_seed: Vec<u8>,
    pub inbox_next: u64,
    pub children: u32,
    // ---- Phase 4 additions (plan 04-12). Appended at the end: postcard is positional, so
    // this changes the encoding of every stored RunRecord. Pre-freeze format change, no
    // migration (dev data dirs created before Phase 4 must be deleted). Phase 5 freezes it.
    /// sha256 of the module the run is pinned to (`StartRun.module_sha`; empty = none).
    pub module_sha: Option<[u8; 32]>,
    /// `StartRun.tags`, in submission order.
    pub tags: Vec<String>,
    /// `StartRun.tenant` (empty = none).
    pub tenant: Option<String>,
    /// `StartRun.traceparent` (empty = none).
    pub traceparent: Option<String>,
    /// `StartRun.run_timeout_ms` (0 = none).
    pub run_timeout_ms: Option<u64>,
    /// `CompleteRun.completed_at_ms` of the terminal transition (0 = none).
    pub completed_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyValue {
    pub concurrency_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InboxMsg {
    Signal {
        name: String,
        /// prost-encoded `PayloadRef`.
        payload: Vec<u8>,
    },
    ChildCompleted {
        child: [u8; 18],
        op: u64,
        status: RunStatus,
        result: Vec<u8>,
    },
    CancelRequested {
        reason: String,
    },
    /// No longer written since plan 04-13 (timer inputs are journal events).
    /// Kept so the postcard variant indices stay stable.
    TimerFired {
        op: u64,
        fired_at_ms: u64,
    },
}

/// `VALUE_FORMAT ‖ postcard(v)`.
pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    let mut out = vec![VALUE_FORMAT];
    // postcard serialisation into a Vec only fails for types serde cannot
    // represent (none of ours); treat it as a programming error.
    let body = postcard::to_allocvec(v).expect("postcard encode of a store value");
    out.extend_from_slice(&body);
    out
}

/// Checks the format byte and decodes the postcard body.
pub fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, StoreError> {
    match b.split_first() {
        Some((&VALUE_FORMAT, body)) => {
            postcard::from_bytes(body).map_err(|e| StoreError::Codec(e.to_string()))
        }
        Some((&f, _)) => Err(StoreError::Codec(format!("unknown value format byte {f}"))),
        None => Err(StoreError::Codec("empty value".into())),
    }
}
