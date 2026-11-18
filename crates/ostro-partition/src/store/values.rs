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
    /// Outbox rows below this seq (per target) have been truncated.
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
