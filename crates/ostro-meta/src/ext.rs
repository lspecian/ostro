//! The meta `ext` TLV codec (Phase 14 F2, D20 as amended).
//!
//! Every meta record ends with `ext: Vec<u8>`. Its bytes are a sequence of
//! entries:
//!
//! ```text
//! entry = varint(tag: u32) ‖ varint(len: u32) ‖ value[len]
//! ```
//!
//! - `varint` is unsigned LEB128 (the postcard varint), at most 5 bytes for a
//!   `u32`, and must be **canonical** (no redundant trailing `0x80`/`0x00`
//!   continuation bytes), so equal extension sets are byte-identical.
//! - Tags are **strictly increasing**; tag `0` is invalid.
//! - An empty `Vec` means "no extensions".
//! - `value` is the postcard encoding of a small struct owned by the tag. This
//!   codec never interprets values.
//! - Tags form one global space across all record kinds and are allocated only
//!   from `.planning/RESERVATIONS.md` §10b. Registering a tag needs no new meta
//!   variant and no `META_FORMAT` bump.
//! - Readers ignore tags they do not know; [`ext_put`] / [`ext_remove`] keep
//!   every other entry byte for byte, so unknown tags survive a
//!   read-modify-write by an older node.
//!
//! Meta apply rejects a `Put` whose record carries an ext that fails
//! [`ext_validate`] (as `MetaResult::Invalid`), so a stored ext is always
//! well formed.

use serde::{Deserialize, Serialize};

/// Golden/test-only tag (RESERVATIONS §10b): used by the meta golden fixtures
/// and codec tests. Never read by production code, never assigned to a feature.
pub const EXT_TAG_TEST: u32 = 15;

/// Why an ext byte string is malformed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ExtError {
    #[error("ext tag 0 at byte {offset} is invalid")]
    ZeroTag { offset: usize },
    #[error("ext tag {tag} at byte {offset} does not increase (previous tag {prev})")]
    NotIncreasing { prev: u32, tag: u32, offset: usize },
    #[error("ext truncated at byte {offset}")]
    Truncated { offset: usize },
    #[error("ext varint at byte {offset} overflows u32 or is not canonical")]
    BadVarint { offset: usize },
    #[error("ext value for tag {tag} is {len} bytes, longer than u32::MAX")]
    ValueTooLong { tag: u32, len: usize },
    #[error("ext value for tag {tag} does not decode: {reason}")]
    BadValue { tag: u32, reason: String },
}

/// One decoded entry, borrowing its value from the ext bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtEntry<'a> {
    pub tag: u32,
    pub value: &'a [u8],
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> Result<u32, ExtError> {
    let start = *pos;
    let mut value: u64 = 0;
    for i in 0..5 {
        let Some(&b) = bytes.get(*pos) else {
            return Err(ExtError::Truncated { offset: *pos });
        };
        *pos += 1;
        value |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            // Canonical: a multi-byte varint never ends in a zero group.
            if i > 0 && b == 0 {
                return Err(ExtError::BadVarint { offset: start });
            }
            return u32::try_from(value).map_err(|_| ExtError::BadVarint { offset: start });
        }
    }
    Err(ExtError::BadVarint { offset: start })
}

fn write_varint(out: &mut Vec<u8>, mut v: u32) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// Every entry of `ext` in order, validating as it goes.
pub fn ext_entries(ext: &[u8]) -> Result<Vec<ExtEntry<'_>>, ExtError> {
    let mut out = Vec::new();
    let mut pos = 0;
    let mut prev: Option<u32> = None;
    while pos < ext.len() {
        let offset = pos;
        let tag = read_varint(ext, &mut pos)?;
        if tag == 0 {
            return Err(ExtError::ZeroTag { offset });
        }
        if let Some(prev) = prev
            && tag <= prev
        {
            return Err(ExtError::NotIncreasing { prev, tag, offset });
        }
        let len = read_varint(ext, &mut pos)? as usize;
        let end = pos
            .checked_add(len)
            .filter(|&e| e <= ext.len())
            .ok_or(ExtError::Truncated { offset: ext.len() })?;
        out.push(ExtEntry {
            tag,
            value: &ext[pos..end],
        });
        pos = end;
        prev = Some(tag);
    }
    Ok(out)
}

/// Check that `ext` is well formed (empty is valid).
pub fn ext_validate(ext: &[u8]) -> Result<(), ExtError> {
    ext_entries(ext).map(|_| ())
}

/// The raw value stored under `tag`, if any. A malformed `ext` (never stored,
/// because apply validates) yields `None`.
pub fn ext_get(ext: &[u8], tag: u32) -> Option<&[u8]> {
    ext_entries(ext)
        .ok()?
        .into_iter()
        .find(|e| e.tag == tag)
        .map(|e| e.value)
}

fn encode_entries(entries: &[ExtEntry<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in entries {
        write_varint(&mut out, e.tag);
        // Callers checked that every length fits a u32.
        write_varint(&mut out, e.value.len() as u32);
        out.extend_from_slice(e.value);
    }
    out
}

/// `ext` with `tag` set to `val` (inserted in tag order, or replacing the
/// existing value). Every other entry is kept byte for byte, including tags
/// this build does not know.
pub fn ext_put(ext: &[u8], tag: u32, val: &[u8]) -> Result<Vec<u8>, ExtError> {
    if tag == 0 {
        return Err(ExtError::ZeroTag { offset: 0 });
    }
    if u32::try_from(val.len()).is_err() {
        return Err(ExtError::ValueTooLong {
            tag,
            len: val.len(),
        });
    }
    let mut entries = ext_entries(ext)?;
    match entries.binary_search_by_key(&tag, |e| e.tag) {
        Ok(i) => entries[i].value = val,
        Err(i) => entries.insert(i, ExtEntry { tag, value: val }),
    }
    Ok(encode_entries(&entries))
}

/// `ext` without `tag` (unchanged bytes if absent).
pub fn ext_remove(ext: &[u8], tag: u32) -> Result<Vec<u8>, ExtError> {
    let mut entries = ext_entries(ext)?;
    entries.retain(|e| e.tag != tag);
    Ok(encode_entries(&entries))
}

/// Typed read: postcard-decode the value under `tag`. `Ok(None)` if absent.
pub fn ext_get_value<T: for<'de> Deserialize<'de>>(
    ext: &[u8],
    tag: u32,
) -> Result<Option<T>, ExtError> {
    let Some(entry) = ext_entries(ext)?.into_iter().find(|e| e.tag == tag) else {
        return Ok(None);
    };
    postcard::from_bytes(entry.value)
        .map(Some)
        .map_err(|e| ExtError::BadValue {
            tag,
            reason: e.to_string(),
        })
}

/// Typed write: postcard-encode `value` and [`ext_put`] it under `tag`.
pub fn ext_put_value<T: Serialize>(ext: &[u8], tag: u32, value: &T) -> Result<Vec<u8>, ExtError> {
    let bytes = postcard::to_stdvec(value).map_err(|e| ExtError::BadValue {
        tag,
        reason: e.to_string(),
    })?;
    ext_put(ext, tag, &bytes)
}

/// Phase 12 (12-02, RESERVATIONS §10b): `PartitionAssignment` election info,
/// written by meta apply of `ElectLeader` / `ResignLeader` (12-05). Value:
/// postcard [`ElectionInfo`].
pub const EXT_ELECTION_INFO: u32 = 80;

/// [`ElectionInfo::reason`]: the bootstrap placement (epoch 0 → 1).
pub const ELECTION_REASON_BOOTSTRAP: u8 = 0;
/// [`ElectionInfo::reason`]: a fresh term on node startup.
pub const ELECTION_REASON_STARTUP: u8 = 1;
/// [`ElectionInfo::reason`]: the controller replaced a dead leader.
pub const ELECTION_REASON_FAILOVER: u8 = 2;
/// [`ElectionInfo::reason`]: the leader resigned (`ResignLeader`).
pub const ELECTION_REASON_RESIGN: u8 = 3;
/// [`ElectionInfo::reason`]: no live ISR member; the partition is offline (`leader = 0`).
pub const ELECTION_REASON_OFFLINE: u8 = 4;

/// Why and when a partition got its current leader (ext tag 80).
///
/// Field order is positional postcard and frozen once shipped; a new field
/// would need a new tag.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElectionInfo {
    /// One of the `ELECTION_REASON_*` constants (unknown values are kept as-is).
    pub reason: u8,
    /// Proposer's clock when the election was proposed (Unix ms).
    pub elected_at_ms: u64,
    /// The leader before this election (0 = none).
    pub prev_leader: u64,
}

/// The [`ElectionInfo`] stored in a record's `ext`, if any.
pub fn get_election_info(ext: &[u8]) -> Result<Option<ElectionInfo>, ExtError> {
    ext_get_value(ext, EXT_ELECTION_INFO)
}

/// `ext` with tag 80 set to `info`; every other entry is kept byte for byte.
pub fn put_election_info(ext: &[u8], info: &ElectionInfo) -> Result<Vec<u8>, ExtError> {
    ext_put_value(ext, EXT_ELECTION_INFO, info)
}

#[cfg(test)]
mod election_info_tests {
    use super::*;

    #[test]
    fn election_info_roundtrips_and_keeps_other_tags() {
        let info = ElectionInfo {
            reason: ELECTION_REASON_FAILOVER,
            elected_at_ms: 1_700_000_000_123,
            prev_leader: 2,
        };
        let base = ext_put(&[], EXT_TAG_TEST, &[7]).unwrap();
        let ext = put_election_info(&base, &info).unwrap();
        assert_eq!(get_election_info(&ext).unwrap(), Some(info));
        assert_eq!(ext_get(&ext, EXT_TAG_TEST), Some(&[7u8][..]));
        // Tag 80 sorts after the test tag; the encoding is canonical.
        let entries = ext_entries(&ext).unwrap();
        assert_eq!(entries.iter().map(|e| e.tag).collect::<Vec<_>>(), [15, 80]);
        assert_eq!(get_election_info(&base).unwrap(), None);
        // Replacing keeps a single entry.
        let again = put_election_info(
            &ext,
            &ElectionInfo {
                reason: ELECTION_REASON_RESIGN,
                ..info
            },
        )
        .unwrap();
        assert_eq!(ext_entries(&again).unwrap().len(), 2);
        assert_eq!(
            get_election_info(&again).unwrap().map(|i| i.reason),
            Some(ELECTION_REASON_RESIGN)
        );
        assert_eq!(
            [
                ELECTION_REASON_BOOTSTRAP,
                ELECTION_REASON_STARTUP,
                ELECTION_REASON_FAILOVER,
                ELECTION_REASON_RESIGN,
                ELECTION_REASON_OFFLINE
            ],
            [0, 1, 2, 3, 4]
        );
    }
}

// ---- Phase 13 (13-01, RESERVATIONS §10b tags 96–102; D-P13-3/12/17/20) ----------------------
//
// Value structs are positional postcard and frozen once shipped (a new field needs a new tag).
// Readers ignore unknown tags and the typed `put_*` helpers keep every other entry byte for
// byte, so an older node's read-modify-write never drops them. Apply never decodes these
// values (05-20 rule 5): they are written verbatim from commands / records and checked only at
// the proposer. Tags 96–99 are the compatibility machinery and are written at every feature
// level (D-P13-3); 100–102 are gated at level 2 (`ostro_types::compat::Feature`).

/// `Member` NodeVersion (tag 96): what the node's binary supports.
pub const EXT_NODE_VERSION: u32 = 96;
/// `Member` MemberState (tag 97): Joining / Active / Draining / Removed (tombstone).
pub const EXT_MEMBER_STATE: u32 = 97;
/// `ClusterInfo` ClusterVersion (tag 98): the finalized feature level (absent = `LEVEL_MIN`).
pub const EXT_CLUSTER_VERSION: u32 = 98;
/// `ClusterInfo` ReplicationPolicy (tag 99).
pub const EXT_REPLICATION_POLICY: u32 = 99;
/// `PartitionAssignment` Reassignment (tag 100, KIP-455 shape).
pub const EXT_REASSIGNMENT: u32 = 100;
/// `ModuleRecord` WitRequirement (tag 101).
pub const EXT_WIT_REQUIREMENT: u32 = 101;
/// `ModuleRecord` / `WorkflowVersion` Retired (tag 102).
pub const EXT_RETIRED: u32 = 102;

/// Every Phase 13 tag, ascending.
pub const P13_EXT_TAGS: [u32; 7] = [
    EXT_NODE_VERSION,
    EXT_MEMBER_STATE,
    EXT_CLUSTER_VERSION,
    EXT_REPLICATION_POLICY,
    EXT_REASSIGNMENT,
    EXT_WIT_REQUIREMENT,
    EXT_RETIRED,
];

/// [`MemberState::state`]: allocated, joining (learner or not yet in the conf).
pub const MEMBER_JOINING: u8 = 0;
/// [`MemberState::state`]: a full member.
pub const MEMBER_ACTIVE: u8 = 1;
/// [`MemberState::state`]: handing off its partitions before removal.
pub const MEMBER_DRAINING: u8 = 2;
/// [`MemberState::state`]: removed (tombstone kept forever; ids never reused).
pub const MEMBER_REMOVED: u8 = 3;

/// What a node's binary supports (tag 96), reported in its own `Member` record.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeVersion {
    /// `CARGO_PKG_VERSION` of the binary.
    pub ostro_version: String,
    /// Lowest feature level the binary runs (`ostro_types::compat::LEVEL_MIN`).
    pub level_min: u32,
    /// Highest feature level the binary emits (`ostro_types::compat::level_max()`).
    pub level_max: u32,
    /// Wasmtime version string.
    pub wasmtime: String,
    /// Event codec version.
    pub event_codec_ver: u32,
    /// WIT lines the engine links (e.g. `"0.1"`).
    pub wit_lines: Vec<String>,
    /// Reporter's clock (Unix ms).
    pub reported_at_ms: u64,
}

/// Membership state of a node (tag 97).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberState {
    /// One of the `MEMBER_*` constants (unknown values are kept as-is).
    pub state: u8,
    /// Proposer's clock when the state changed (Unix ms).
    pub changed_at_ms: u64,
}

/// The cluster's finalized feature level (tag 98). Absent = `LEVEL_MIN`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterVersion {
    /// Finalized feature level.
    pub finalized: u32,
    /// Operator hold: auto-finalization stays at or below this level while set.
    pub hold: Option<u32>,
    /// When `finalized` was last raised (Unix ms).
    pub finalized_at_ms: u64,
}

/// Cluster replication policy (tag 99). Effective RF = min(rf, members).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationPolicy {
    /// Target replication factor.
    pub rf: u8,
    /// Target minimum ISR size.
    pub min_insync: u8,
    /// Maximum metadata voters.
    pub max_voters: u8,
}

/// An in-flight partition reassignment (tag 100, KIP-455 shape).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reassignment {
    /// The target replica set.
    pub target: Vec<u64>,
    /// Replicas being added (`target \ old`).
    pub adding: Vec<u64>,
    /// Replicas being removed (`old \ target`).
    pub removing: Vec<u64>,
    /// When the reassignment started (Unix ms).
    pub started_at_ms: u64,
}

/// A module's WIT line and minimum feature level (tag 101).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WitRequirement {
    /// WIT major line, e.g. `"0.1"`.
    pub line: String,
    /// Max over the imported functions' `introduced_in` levels.
    pub min_level: u32,
}

/// A retired module or workflow version (tag 102).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retired {
    /// When it was retired (Unix ms).
    pub retired_at_ms: u64,
}

macro_rules! typed_ext {
    ($get:ident, $put:ident, $tag:ident, $ty:ty) => {
        #[doc = concat!("The [`", stringify!($ty), "`] stored in a record's `ext` (tag [`", stringify!($tag), "`]), if any.")]
        pub fn $get(ext: &[u8]) -> Result<Option<$ty>, ExtError> {
            ext_get_value(ext, $tag)
        }

        #[doc = concat!("`ext` with tag [`", stringify!($tag), "`] set; every other entry is kept byte for byte.")]
        pub fn $put(ext: &[u8], value: &$ty) -> Result<Vec<u8>, ExtError> {
            ext_put_value(ext, $tag, value)
        }
    };
}

typed_ext!(
    get_node_version,
    put_node_version,
    EXT_NODE_VERSION,
    NodeVersion
);
typed_ext!(
    get_member_state,
    put_member_state,
    EXT_MEMBER_STATE,
    MemberState
);
typed_ext!(
    get_cluster_version,
    put_cluster_version,
    EXT_CLUSTER_VERSION,
    ClusterVersion
);
typed_ext!(
    get_replication_policy,
    put_replication_policy,
    EXT_REPLICATION_POLICY,
    ReplicationPolicy
);
typed_ext!(
    get_reassignment,
    put_reassignment,
    EXT_REASSIGNMENT,
    Reassignment
);
typed_ext!(
    get_wit_requirement,
    put_wit_requirement,
    EXT_WIT_REQUIREMENT,
    WitRequirement
);
typed_ext!(get_retired, put_retired, EXT_RETIRED, Retired);

#[cfg(test)]
mod phase13_tests {
    use super::*;

    fn samples() -> (
        NodeVersion,
        MemberState,
        ClusterVersion,
        ReplicationPolicy,
        Reassignment,
        WitRequirement,
        Retired,
    ) {
        (
            NodeVersion {
                ostro_version: "0.1.0".into(),
                level_min: 1,
                level_max: 2,
                wasmtime: "50.0.2".into(),
                event_codec_ver: 1,
                wit_lines: vec!["0.1".into()],
                reported_at_ms: 1_700_000_000_000,
            },
            MemberState {
                state: MEMBER_DRAINING,
                changed_at_ms: 7,
            },
            ClusterVersion {
                finalized: 2,
                hold: Some(1),
                finalized_at_ms: 9,
            },
            ReplicationPolicy {
                rf: 3,
                min_insync: 2,
                max_voters: 5,
            },
            Reassignment {
                target: vec![2, 3, 4],
                adding: vec![4],
                removing: vec![1],
                started_at_ms: 11,
            },
            WitRequirement {
                line: "0.1".into(),
                min_level: 1,
            },
            Retired { retired_at_ms: 13 },
        )
    }

    #[test]
    fn p13_tags_are_the_registered_block() {
        assert_eq!(P13_EXT_TAGS, [96, 97, 98, 99, 100, 101, 102]);
        assert_eq!(
            [
                MEMBER_JOINING,
                MEMBER_ACTIVE,
                MEMBER_DRAINING,
                MEMBER_REMOVED
            ],
            [0, 1, 2, 3]
        );
    }

    #[test]
    fn every_p13_value_roundtrips_and_keeps_unknown_tags() {
        let (nv, ms, cv, rp, ra, wr, rt) = samples();
        // An "unknown" tag from a newer binary (111, the top of the P13 spare range) and
        // the test tag must survive every typed write.
        let mut ext = ext_put(&[], EXT_TAG_TEST, &[1, 2]).unwrap();
        ext = ext_put(&ext, 111, &[0xde, 0xad]).unwrap();
        ext = put_election_info(&ext, &ElectionInfo::default()).unwrap();
        ext = put_node_version(&ext, &nv).unwrap();
        ext = put_member_state(&ext, &ms).unwrap();
        ext = put_cluster_version(&ext, &cv).unwrap();
        ext = put_replication_policy(&ext, &rp).unwrap();
        ext = put_reassignment(&ext, &ra).unwrap();
        ext = put_wit_requirement(&ext, &wr).unwrap();
        ext = put_retired(&ext, &rt).unwrap();
        ext_validate(&ext).unwrap();
        assert_eq!(get_node_version(&ext).unwrap(), Some(nv));
        assert_eq!(get_member_state(&ext).unwrap(), Some(ms));
        assert_eq!(get_cluster_version(&ext).unwrap(), Some(cv));
        assert_eq!(get_replication_policy(&ext).unwrap(), Some(rp));
        assert_eq!(get_reassignment(&ext).unwrap(), Some(ra));
        assert_eq!(get_wit_requirement(&ext).unwrap(), Some(wr));
        assert_eq!(get_retired(&ext).unwrap(), Some(rt));
        assert_eq!(ext_get(&ext, 111), Some(&[0xde, 0xad][..]));
        assert_eq!(ext_get(&ext, EXT_TAG_TEST), Some(&[1, 2][..]));
        let tags: Vec<u32> = ext_entries(&ext).unwrap().iter().map(|e| e.tag).collect();
        assert_eq!(tags, [15, 80, 96, 97, 98, 99, 100, 101, 102, 111]);
        // Removing one tag keeps the others byte for byte.
        let without = ext_remove(&ext, EXT_REASSIGNMENT).unwrap();
        assert_eq!(get_reassignment(&without).unwrap(), None);
        assert_eq!(ext_get(&without, 111), Some(&[0xde, 0xad][..]));
        // Absent = None (ClusterInfo without tag 98 means finalized = LEVEL_MIN).
        assert_eq!(get_cluster_version(&[]).unwrap(), None);
    }

    /// Rule 5 (05-20): apply never validates ext values, so record validation must keep
    /// treating ext as opaque TLV (shape only). A source scan keeps typed decoding out of
    /// `validate.rs`.
    #[test]
    fn validate_never_decodes_ext_values() {
        let src = include_str!("validate.rs");
        for forbidden in ["ext_get_value", "ext_get(", "postcard::from_bytes", "get_"] {
            assert!(
                !src.contains(forbidden),
                "validate.rs must not decode ext values ({forbidden})"
            );
        }
        assert!(src.contains("ext_validate"), "shape check stays");
    }
}
