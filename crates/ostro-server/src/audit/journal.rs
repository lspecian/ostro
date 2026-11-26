//! Audit record format and canonical encoding (D-P14-11, 14-04).
//!
//! One record is one line of a segment file (`audit-<first seq, 20 digits>.jsonl`):
//!
//! ```text
//! {"v":1,"seq":…,"ts_ms":…,"node":…,"incarnation":…,"actor":…,"actor_kind":…,
//!  "tenant":…|null,"action":…,"target":…,"outcome":{"result":…[,"reason":…]},
//!  "request_id":…|null,"details":{…sorted…}[,"count":…,"first_ts_ms":…,"last_ts_ms":…],
//!  "prev_hash":"<64 hex>","hash":"<64 hex>"}
//! ```
//!
//! **Canonical JSON** = exactly this key order, no whitespace, UTF-8 (serde_json's
//! escaping), `details` sorted by key, the optional coalescing fields omitted when absent,
//! and `hash` omitted. The stored line is the canonical JSON with `,"hash":"…"` inserted
//! before the closing brace, so a line is valid only when it is byte-identical to the
//! re-encoding of its parsed fields ([`Record::parse_line`]).
//!
//! Control records (`actor = "system"`, `actor_kind = "system"`, empty `target`) use the
//! same shape with their fields in `details`: `segment_open{prev_segment_last_hash}`,
//! `audit_recovered{truncated_bytes, last_good_seq}`,
//! `retention_trim{first_kept_seq, first_kept_hash}`.

use std::collections::BTreeMap;

use ostro_types::audit::{ActorKind, AuditEvent, AuditOutcome};
use serde::{Deserialize, Serialize};

use super::chain::{self, Hash};

/// The record format version.
pub const VERSION: u32 = 1;

/// Control action: the first record of every segment.
pub const SEGMENT_OPEN: &str = "segment_open";
/// Control action: open truncated a torn tail.
pub const AUDIT_RECOVERED: &str = "audit_recovered";
/// Control action: retention deleted whole segments.
pub const RETENTION_TRIM: &str = "retention_trim";

/// `segment_open` detail: the hash of the previous segment's last record (= `prev_hash`).
pub const D_PREV_SEGMENT_LAST_HASH: &str = "prev_segment_last_hash";
/// `audit_recovered` detail: bytes removed from the torn tail.
pub const D_TRUNCATED_BYTES: &str = "truncated_bytes";
/// `audit_recovered` detail: the last record kept.
pub const D_LAST_GOOD_SEQ: &str = "last_good_seq";
/// `retention_trim` detail: the first record still on disk.
pub const D_FIRST_KEPT_SEQ: &str = "first_kept_seq";
/// `retention_trim` detail: its hash (the verification anchor).
pub const D_FIRST_KEPT_HASH: &str = "first_kept_hash";

/// One journal record (a parsed line).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub v: u32,
    pub seq: u64,
    pub ts_ms: u64,
    pub node: u64,
    pub incarnation: u64,
    pub actor: String,
    pub actor_kind: ActorKind,
    pub tenant: Option<String>,
    pub action: String,
    pub target: String,
    pub outcome: AuditOutcome,
    pub request_id: Option<String>,
    pub details: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_ts_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ts_ms: Option<u64>,
    pub prev_hash: String,
    pub hash: String,
}

/// The canonical body: every field but `hash`, in the fixed order.
#[derive(Serialize)]
struct Body<'a> {
    v: u32,
    seq: u64,
    ts_ms: u64,
    node: u64,
    incarnation: u64,
    actor: &'a str,
    actor_kind: ActorKind,
    tenant: &'a Option<String>,
    action: &'a str,
    target: &'a str,
    outcome: &'a AuditOutcome,
    request_id: &'a Option<String>,
    details: &'a BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_ts_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_ts_ms: Option<u64>,
    prev_hash: &'a str,
}

/// Why a line is not a valid record.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LineError {
    #[error("not a record: {0}")]
    Parse(String),
    #[error("non-canonical encoding")]
    NonCanonical,
    #[error("unsupported record version {0}")]
    Version(u32),
    #[error("malformed hash field")]
    BadHashField,
    #[error("hash mismatch")]
    HashMismatch,
}

/// Coalescing fields of a summarised record (D-P14-12).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Coalesced {
    pub count: u64,
    pub first_ts_ms: u64,
    pub last_ts_ms: u64,
}

/// What the writer knows when it seals a record.
#[derive(Clone, Copy, Debug)]
pub struct Stamp {
    pub seq: u64,
    pub ts_ms: u64,
    pub node: u64,
    pub incarnation: u64,
}

impl Record {
    /// Seal `ev` at `stamp` after `prev`: fills `prev_hash` and `hash`.
    pub fn seal(ev: AuditEvent, stamp: Stamp, coalesced: Option<Coalesced>, prev: &Hash) -> Self {
        let mut r = Record {
            v: VERSION,
            seq: stamp.seq,
            ts_ms: stamp.ts_ms,
            node: stamp.node,
            incarnation: stamp.incarnation,
            actor: ev.actor,
            actor_kind: ev.actor_kind,
            tenant: ev.tenant,
            action: ev.action,
            target: ev.target,
            outcome: ev.outcome,
            request_id: ev.request_id,
            details: ev.details,
            count: coalesced.map(|c| c.count),
            first_ts_ms: coalesced.map(|c| c.first_ts_ms),
            last_ts_ms: coalesced.map(|c| c.last_ts_ms),
            prev_hash: chain::to_hex(prev),
            hash: String::new(),
        };
        r.hash = chain::to_hex(&chain::link(prev, &r.canonical()));
        r
    }

    /// A control event (system actor, empty target).
    pub fn control(action: &str, details: BTreeMap<String, String>) -> AuditEvent {
        AuditEvent {
            action: action.to_owned(),
            actor: "system".to_owned(),
            actor_kind: ActorKind::System,
            tenant: None,
            target: String::new(),
            outcome: AuditOutcome::Ok,
            request_id: None,
            details,
        }
    }

    /// The canonical JSON (no `hash`).
    pub fn canonical(&self) -> Vec<u8> {
        let body = Body {
            v: self.v,
            seq: self.seq,
            ts_ms: self.ts_ms,
            node: self.node,
            incarnation: self.incarnation,
            actor: &self.actor,
            actor_kind: self.actor_kind,
            tenant: &self.tenant,
            action: &self.action,
            target: &self.target,
            outcome: &self.outcome,
            request_id: &self.request_id,
            details: &self.details,
            count: self.count,
            first_ts_ms: self.first_ts_ms,
            last_ts_ms: self.last_ts_ms,
            prev_hash: &self.prev_hash,
        };
        // Serializing plain strings, integers and string maps cannot fail.
        serde_json::to_vec(&body).unwrap_or_default()
    }

    /// The stored line, newline-terminated.
    pub fn to_line(&self) -> Vec<u8> {
        let mut out = self.canonical();
        out.pop(); // the closing brace
        out.extend_from_slice(b",\"hash\":\"");
        out.extend_from_slice(self.hash.as_bytes());
        out.extend_from_slice(b"\"}\n");
        out
    }

    /// The raw `hash`.
    pub fn hash_bytes(&self) -> Option<Hash> {
        chain::from_hex(&self.hash)
    }

    /// The raw `prev_hash`.
    pub fn prev_hash_bytes(&self) -> Option<Hash> {
        chain::from_hex(&self.prev_hash)
    }

    /// Parse one line (without its newline) and check that it is canonical and that its
    /// `hash` matches its own content (`sha256(prev_hash ‖ canonical)`). Chain linkage to
    /// the previous record is the caller's check.
    pub fn parse_line(line: &[u8]) -> Result<Record, LineError> {
        let r: Record =
            serde_json::from_slice(line).map_err(|e| LineError::Parse(e.to_string()))?;
        if r.v != VERSION {
            return Err(LineError::Version(r.v));
        }
        let (Some(prev), Some(hash)) = (r.prev_hash_bytes(), r.hash_bytes()) else {
            return Err(LineError::BadHashField);
        };
        let mut expect = r.to_line();
        expect.pop();
        if expect != line {
            return Err(LineError::NonCanonical);
        }
        if chain::link(&prev, &r.canonical()) != hash {
            return Err(LineError::HashMismatch);
        }
        Ok(r)
    }

    /// A detail value.
    pub fn detail(&self, k: &str) -> Option<&str> {
        self.details.get(k).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec() -> Record {
        let mut ev = AuditEvent::new("token.create", "otk_a", ActorKind::Token, "otk_b");
        ev.tenant = Some("acme".into());
        ev.outcome = AuditOutcome::Denied("missing_scope".into());
        ev.details.insert("z".into(), "1".into());
        ev.details.insert("a\"b".into(), "é\n".into());
        Record::seal(
            ev,
            Stamp {
                seq: 7,
                ts_ms: 1_000,
                node: 1,
                incarnation: u64::MAX,
            },
            None,
            &chain::genesis(1, 2),
        )
    }

    #[test]
    fn canonical_order_and_round_trip() {
        let r = rec();
        let line = r.to_line();
        let s = std::str::from_utf8(&line).unwrap();
        assert!(
            s.starts_with(
                r#"{"v":1,"seq":7,"ts_ms":1000,"node":1,"incarnation":18446744073709551615,"actor":"otk_a","actor_kind":"token","tenant":"acme","action":"token.create","target":"otk_b","outcome":{"result":"denied","reason":"missing_scope"},"request_id":null,"details":{"a\"b":"é\n","z":"1"},"prev_hash":""#
            ),
            "{s}"
        );
        assert!(s.ends_with("\"}\n"));
        assert_eq!(s.matches('\n').count(), 1, "escaped newlines only");
        let back = Record::parse_line(&line[..line.len() - 1]).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn coalesced_fields_appear_only_when_set() {
        let ev = AuditEvent::new("secret.access", "system", ActorKind::System, "s");
        let c = Coalesced {
            count: 3,
            first_ts_ms: 1,
            last_ts_ms: 2,
        };
        let r = Record::seal(
            ev,
            Stamp {
                seq: 1,
                ts_ms: 2,
                node: 1,
                incarnation: 1,
            },
            Some(c),
            &[0; 32],
        );
        let line = r.to_line();
        let s = std::str::from_utf8(&line).unwrap();
        assert!(
            s.contains(r#""details":{},"count":3,"first_ts_ms":1,"last_ts_ms":2,"prev_hash""#),
            "{s}"
        );
        assert_eq!(Record::parse_line(&line[..line.len() - 1]).unwrap(), r);
    }

    #[test]
    fn whitespace_or_reordering_is_rejected() {
        let r = rec();
        let line = r.to_line();
        let s = std::str::from_utf8(&line[..line.len() - 1]).unwrap();
        let spaced = s.replacen(",\"seq\"", ", \"seq\"", 1);
        assert_eq!(
            Record::parse_line(spaced.as_bytes()),
            Err(LineError::NonCanonical)
        );
        let mut forged = r.clone();
        forged.target = "otk_c".into();
        let fl = forged.to_line();
        assert_eq!(
            Record::parse_line(&fl[..fl.len() - 1]),
            Err(LineError::HashMismatch)
        );
    }
}
