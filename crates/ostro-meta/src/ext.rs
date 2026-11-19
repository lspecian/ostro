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
