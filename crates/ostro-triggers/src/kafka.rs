//! Kafka topic-consumer trigger: the pure part (Phase 14, TRIG-04, D-P14-43).
//!
//! No I/O and no clock reads. The driver (`ostro-server` `p14::kafka`, feature `kafka`)
//! fetches records, starts one run per record through the routed control plane and commits
//! consumer-group offsets; this module decides:
//!
//! - the **idempotency key** of a record ([`idempotency_key`]): `__kafka/<name>/<topic>/
//!   <partition>/<offset>` (offset mode) or `__kafka/<name>/k/<sha256 hex of the key>`
//!   (message-key mode; a null key falls back to the offset form);
//! - the **run input** ([`input`]): the record value as JSON (`value_json`) or an envelope
//!   with base64 key, value and headers (`envelope`);
//! - the **commit watermark** ([`CommitTracker`]): offset `o + 1` is committed only once
//!   every started record at or below `o` is done;
//! - the **owner partition** ([`kafka_owner`]) whose leader consumes a
//!   `(trigger, topic, kafka partition)`.

use std::collections::BTreeSet;

use base64::Engine as _;
use sha2::{Digest, Sha256};

/// Idempotency-key prefix of Kafka starts (RESERVATIONS §11, D-P14-51). The control plane
/// classifies a start with this prefix as `TriggerKind::Kafka`.
pub const KEY_PREFIX: &str = "__kafka/";

/// Longest key [`idempotency_key`] produces for valid inputs (trigger name ≤ 128 bytes,
/// topic ≤ 249 bytes, any partition and offset): 8 + 128 + 1 + 249 + 1 + 11 + 1 + 20 = 419.
/// Pinned at or below this bound by the tests.
pub const MAX_KEY_LEN: usize = 512;

/// Longest Kafka topic name (Kafka's own limit).
pub const MAX_TOPIC_LEN: usize = 249;

/// How a record maps to its run's idempotency key (`idempotency` field).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum IdempotencyMode {
    /// One run per `(topic, partition, offset)` (default).
    #[default]
    Offset,
    /// One run per distinct record key (null keys: per offset).
    MessageKey,
}

impl IdempotencyMode {
    /// Config spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            IdempotencyMode::Offset => "offset",
            IdempotencyMode::MessageKey => "message_key",
        }
    }

    /// Parse the config spelling.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "offset" => Some(IdempotencyMode::Offset),
            "message_key" => Some(IdempotencyMode::MessageKey),
            _ => None,
        }
    }
}

/// How a record maps to its run's input (`input` field).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum InputMode {
    /// The record value parsed as JSON (default; a null value is `null`).
    #[default]
    ValueJson,
    /// `{topic, partition, offset, timestamp_ms, key_b64, headers, value_b64}`.
    Envelope,
}

impl InputMode {
    /// Config spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            InputMode::ValueJson => "value_json",
            InputMode::Envelope => "envelope",
        }
    }

    /// Parse the config spelling.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "value_json" => Some(InputMode::ValueJson),
            "envelope" => Some(InputMode::Envelope),
            _ => None,
        }
    }
}

/// The idempotency key of a record.
///
/// Offset mode: `__kafka/<name>/<topic>/<partition>/<offset>`. Message-key mode:
/// `__kafka/<name>/k/<64 hex chars of sha256(key)>`; a record with a null key uses the offset
/// form. The topic cannot collide with the `k` marker of the key form because the offset form
/// has two more path segments.
pub fn idempotency_key(
    name: &str,
    mode: IdempotencyMode,
    topic: &str,
    partition: i32,
    offset: i64,
    key: Option<&[u8]>,
) -> String {
    match (mode, key) {
        (IdempotencyMode::MessageKey, Some(k)) => {
            format!("{KEY_PREFIX}{name}/k/{}", hex(&Sha256::digest(k)))
        }
        _ => format!("{KEY_PREFIX}{name}/{topic}/{partition}/{offset}"),
    }
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from(D[usize::from(b >> 4)]));
        s.push(char::from(D[usize::from(b & 0x0f)]));
    }
    s
}

/// A record as the input mapping sees it (borrowed from the client's record).
#[derive(Clone, Copy, Debug)]
pub struct RecordView<'a> {
    /// Topic.
    pub topic: &'a str,
    /// Kafka partition.
    pub partition: i32,
    /// Offset.
    pub offset: i64,
    /// Record timestamp (Unix ms).
    pub timestamp_ms: i64,
    /// Key (`None` = null key).
    pub key: Option<&'a [u8]>,
    /// Value (`None` = tombstone).
    pub value: Option<&'a [u8]>,
    /// Headers in order (`None` value = null header).
    pub headers: &'a [(String, Option<&'a [u8]>)],
}

/// A record that cannot become a run input.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InputError {
    /// `value_json` and the value is not JSON.
    #[error("record value is not JSON: {0}")]
    NotJson(String),
}

impl InputError {
    /// The `reason` label of `ostro_kafka_skipped_total`.
    pub const fn reason(&self) -> &'static str {
        match self {
            InputError::NotJson(_) => "not_json",
        }
    }
}

/// The run input of a record.
pub fn input(mode: InputMode, r: &RecordView<'_>) -> Result<serde_json::Value, InputError> {
    let b64 = |b: Option<&[u8]>| match b {
        Some(b) => serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
        None => serde_json::Value::Null,
    };
    match mode {
        InputMode::ValueJson => match r.value {
            None => Ok(serde_json::Value::Null),
            Some(v) => serde_json::from_slice(v).map_err(|e| InputError::NotJson(e.to_string())),
        },
        InputMode::Envelope => {
            let headers: Vec<serde_json::Value> = r
                .headers
                .iter()
                .map(|(name, value)| serde_json::json!({ "name": name, "value_b64": b64(*value) }))
                .collect();
            Ok(serde_json::json!({
                "topic": r.topic,
                "partition": r.partition,
                "offset": r.offset,
                "timestamp_ms": r.timestamp_ms,
                "key_b64": b64(r.key),
                "headers": headers,
                "value_b64": b64(r.value),
            }))
        }
    }
}

/// A [`CommitTracker`] call out of order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TrackerError {
    /// `start`/`skip_to` below the current position.
    #[error("offset {offset} is below the position {position}")]
    Behind {
        /// The offset given.
        offset: i64,
        /// The position (next offset expected).
        position: i64,
    },
    /// `done` for an offset that is not in flight.
    #[error("offset {0} is not in flight")]
    NotInFlight(i64),
}

/// The commit watermark of one Kafka partition (D-P14-43 commit rule).
///
/// Every offset below the *position* is accounted for: it was either [`start`]ed (and is in
/// flight until [`done`]) or skipped as record-less (control and aborted batches,
/// [`skip_to`]). The watermark — the next offset to commit — is the smallest in-flight
/// offset, or the position when nothing is in flight. It never passes a started record that
/// is not done, and it is monotone.
///
/// [`start`]: CommitTracker::start
/// [`done`]: CommitTracker::done
/// [`skip_to`]: CommitTracker::skip_to
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitTracker {
    position: Option<i64>,
    in_flight: BTreeSet<i64>,
}

impl CommitTracker {
    /// A tracker whose position is not known yet: the first [`start`](Self::start) or
    /// [`skip_to`](Self::skip_to) sets it.
    pub fn new() -> Self {
        Self::default()
    }

    /// A tracker positioned at `position` (the committed offset or the `start_from` reset).
    pub fn at(position: i64) -> Self {
        Self {
            position: Some(position),
            in_flight: BTreeSet::new(),
        }
    }

    /// The next offset expected from the log (`None` before the first call).
    pub fn position(&self) -> Option<i64> {
        self.position
    }

    /// A record at `offset` was handed to the start path. Offsets are strictly increasing;
    /// offsets skipped in between are record-less.
    pub fn start(&mut self, offset: i64) -> Result<(), TrackerError> {
        if let Some(p) = self.position
            && offset < p
        {
            return Err(TrackerError::Behind {
                offset,
                position: p,
            });
        }
        self.in_flight.insert(offset);
        self.position = Some(offset.saturating_add(1));
        Ok(())
    }

    /// The log has no further records below `next` (a fetch's `next_offset`).
    pub fn skip_to(&mut self, next: i64) -> Result<(), TrackerError> {
        match self.position {
            Some(p) if next < p => Err(TrackerError::Behind {
                offset: next,
                position: p,
            }),
            _ => {
                self.position = Some(next);
                Ok(())
            }
        }
    }

    /// The record at `offset` has a durable run start (or an existing run, or was skipped by
    /// `on_invalid = "skip"`).
    pub fn done(&mut self, offset: i64) -> Result<(), TrackerError> {
        if self.in_flight.remove(&offset) {
            Ok(())
        } else {
            Err(TrackerError::NotInFlight(offset))
        }
    }

    /// The next offset to commit: the smallest offset in flight, else the position.
    pub fn watermark(&self) -> Option<i64> {
        match self.in_flight.first() {
            Some(&o) => Some(o),
            None => self.position,
        }
    }

    /// Records started and not done.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }
}

/// The Ostro partition whose leader consumes `(name, topic, kpartition)`: the first 8 bytes
/// of `sha256("__kafka/" ‖ name ‖ 0x00 ‖ topic ‖ 0x00 ‖ kpartition as i32 BE)` as a big-endian
/// u64, modulo `p_count` (the cron-owner pattern, `schedule_owner`; sha256 because this crate
/// is the pure trigger crate and already depends on it). Pinned by test vectors: owners must
/// not move across versions. `p_count` 0 is treated as 1.
pub fn kafka_owner(name: &str, topic: &str, kpartition: i32, p_count: u16) -> u16 {
    let mut h = Sha256::new();
    h.update(KEY_PREFIX.as_bytes());
    h.update(name.as_bytes());
    h.update([0u8]);
    h.update(topic.as_bytes());
    h.update([0u8]);
    h.update(kpartition.to_be_bytes());
    let d = h.finalize();
    let mut b = [0u8; 8];
    b.copy_from_slice(&d[..8]);
    let p = u64::from(p_count.max(1));
    // Result < p <= u16::MAX: lossless.
    (u64::from_be_bytes(b) % p) as u16
}

/// Whether `t` is a legal Kafka topic name (`[A-Za-z0-9._-]{1,249}`, not `.`/`..`).
pub fn is_topic_name(t: &str) -> bool {
    !t.is_empty()
        && t.len() <= MAX_TOPIC_LEN
        && t != "."
        && t != ".."
        && t.bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_is_lowercase() {
        assert_eq!(hex(&[0x00, 0xab, 0xff]), "00abff");
    }

    #[test]
    fn tracker_rejects_out_of_order() {
        let mut t = CommitTracker::at(5);
        assert_eq!(
            t.start(4),
            Err(TrackerError::Behind {
                offset: 4,
                position: 5
            })
        );
        assert_eq!(t.done(7), Err(TrackerError::NotInFlight(7)));
    }
}
