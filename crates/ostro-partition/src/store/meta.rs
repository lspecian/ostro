//! Meta keyspace layout.
//!
//! | key                   | value                                     |
//! |-----------------------|-------------------------------------------|
//! | `format_ver`          | u32 BE (= [`FORMAT_VERSION`])             |
//! | `partition`           | u16 BE                                    |
//! | `partition_count`     | u16 BE                                    |
//! | `applied_offset`      | u64 BE                                    |
//! | `state_hash`          | 32 B                                      |
//! | `checkpointed_offset` | u64 BE (EXCLUDED from the logical digest) |
//! | `cursor`              | postcard `CursorMeta` (format byte 1)     |
//!
//! `inbound_seen` rows live in their own keyspace (`inbound_key(src)` ->
//! u64 BE) and are folded into [`StoreMeta`] on read.

use std::collections::BTreeMap;

use fjall::{Keyspace, OwnedWriteBatch};
use ostro_types::{Offset, PartitionCount, PartitionId};

use super::values::{self, CursorMeta};
use super::{StoreError, StoreMeta};

pub const FORMAT_VERSION: u32 = 1;

pub const K_FORMAT_VER: &[u8] = b"format_ver";
pub const K_PARTITION: &[u8] = b"partition";
pub const K_PARTITION_COUNT: &[u8] = b"partition_count";
pub const K_APPLIED: &[u8] = b"applied_offset";
pub const K_STATE_HASH: &[u8] = b"state_hash";
pub const K_CHECKPOINTED: &[u8] = b"checkpointed_offset";
pub const K_CURSOR: &[u8] = b"cursor";

fn fixed<const N: usize>(ks: &Keyspace, key: &[u8]) -> Result<[u8; N], StoreError> {
    let v = ks.get(key)?.ok_or_else(|| {
        StoreError::Codec(format!("meta key {} missing", String::from_utf8_lossy(key)))
    })?;
    <[u8; N]>::try_from(&*v).map_err(|_| {
        StoreError::Codec(format!(
            "meta key {} has length {}, want {N}",
            String::from_utf8_lossy(key),
            v.len()
        ))
    })
}

/// `None` if the store was never initialised (no `format_ver`).
pub fn read_format_version(meta: &Keyspace) -> Result<Option<u32>, StoreError> {
    match meta.get(K_FORMAT_VER)? {
        None => Ok(None),
        Some(_) => Ok(Some(u32::from_be_bytes(fixed(meta, K_FORMAT_VER)?))),
    }
}

/// Writes every meta key for a freshly created store.
pub fn write_initial(b: &mut OwnedWriteBatch, meta: &Keyspace, p: PartitionId, n: PartitionCount) {
    b.insert(meta, K_FORMAT_VER, FORMAT_VERSION.to_be_bytes());
    b.insert(meta, K_PARTITION, p.0.to_be_bytes());
    b.insert(meta, K_PARTITION_COUNT, n.get().to_be_bytes());
    b.insert(meta, K_APPLIED, 0u64.to_be_bytes());
    b.insert(meta, K_STATE_HASH, [0u8; 32]);
    b.insert(meta, K_CHECKPOINTED, 0u64.to_be_bytes());
    b.insert(meta, K_CURSOR, values::encode(&CursorMeta::default()));
}

pub fn read(meta: &Keyspace, inbound: &Keyspace) -> Result<StoreMeta, StoreError> {
    let partition = PartitionId(u16::from_be_bytes(fixed(meta, K_PARTITION)?));
    let count = PartitionCount::new(u16::from_be_bytes(fixed(meta, K_PARTITION_COUNT)?))
        .ok_or_else(|| StoreError::Codec("stored partition_count is 0".into()))?;
    let applied = Offset(u64::from_be_bytes(fixed(meta, K_APPLIED)?));
    let checkpointed = Offset(u64::from_be_bytes(fixed(meta, K_CHECKPOINTED)?));
    let state_hash = fixed::<32>(meta, K_STATE_HASH)?;
    let cursor_raw = meta
        .get(K_CURSOR)?
        .ok_or_else(|| StoreError::Codec("meta key cursor missing".into()))?;
    let cursor = values::decode(&cursor_raw)?;
    let mut inbound_seen = BTreeMap::new();
    for g in inbound.iter() {
        let (k, v) = g.into_inner()?;
        let src = <[u8; 2]>::try_from(&*k)
            .map_err(|_| StoreError::Codec("bad inbound_seen key".into()))?;
        let seq = <[u8; 8]>::try_from(&*v)
            .map_err(|_| StoreError::Codec("bad inbound_seen value".into()))?;
        inbound_seen.insert(u16::from_be_bytes(src), u64::from_be_bytes(seq));
    }
    Ok(StoreMeta {
        partition,
        count,
        applied,
        checkpointed,
        state_hash,
        cursor,
        inbound_seen,
    })
}
