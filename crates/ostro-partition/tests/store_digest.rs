//! Logical state digest (plan 02-05, STOR-06 at the store layer).

use std::collections::BTreeMap;
use std::path::Path;

use ostro_partition::store::batch::MetaUpdate;
use ostro_partition::store::config::state_dir;
use ostro_partition::store::values::CursorMeta;
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::{Offset, PartitionCount, PartitionId};

const P: PartitionId = PartitionId(5);

fn open(dir: &Path) -> PartitionStore {
    let n = PartitionCount::new(16).expect("non-zero");
    PartitionStore::open(dir, P, n, &StoreConfig::default())
        .expect("open")
        .0
}

/// Deterministic xorshift64* rows: `(table, key, value)`.
fn rows(n: usize) -> Vec<(Table, Vec<u8>, Vec<u8>)> {
    const DATA: [Table; 6] = [
        Table::Runs,
        Table::Events,
        Table::Ready,
        Table::Timers,
        Table::Outbox,
        Table::Effects,
    ];
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        s.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };
    (0..n)
        .map(|_| {
            let t = DATA[(next() % DATA.len() as u64) as usize];
            let key = next().to_be_bytes()[..(1 + next() % 8) as usize].to_vec();
            let len = (next() % 64) as usize;
            let value = (0..len).map(|_| next() as u8).collect();
            (t, key, value)
        })
        .collect()
}

fn final_meta() -> MetaUpdate {
    MetaUpdate {
        applied: Offset(2_000),
        state_hash: [0xAB; 32],
        cursor: CursorMeta {
            epoch: 4,
            epoch_history: vec![(4, 0)],
            outbox_next_seq: BTreeMap::from([(1, 10)]),
            outbox_truncated: BTreeMap::new(),
        },
        inbound_seen_changed: vec![(3, 99)],
    }
}

/// Writes `rows` in `chunks` batches; the last batch carries `final_meta`.
/// Optionally checkpoints after every batch.
fn write(store: &PartitionStore, rows: &[(Table, Vec<u8>, Vec<u8>)], chunks: usize, ckpt: bool) {
    let per = rows.len().div_ceil(chunks);
    let parts: Vec<_> = rows.chunks(per).collect();
    for (i, part) in parts.iter().enumerate() {
        let mut b = store.batch();
        for (t, k, v) in part.iter() {
            b.insert(*t, k.clone(), v.clone());
        }
        let meta = if i + 1 == parts.len() {
            final_meta()
        } else {
            MetaUpdate {
                applied: Offset(i as u64 + 1),
                state_hash: [i as u8; 32],
                cursor: CursorMeta::default(),
                inbound_seen_changed: vec![(3, i as u64)],
            }
        };
        let applied = meta.applied;
        b.commit(&meta).expect("commit");
        if ckpt {
            store.checkpoint(applied).expect("checkpoint");
        }
    }
}

fn digest_of(chunks: usize, ckpt: bool) -> [u8; 32] {
    let dir = ostro_testkit::fs::tempdir();
    let store = open(dir.path());
    write(&store, &rows(2_000), chunks, ckpt);
    store.digest().expect("digest")
}

#[test]
fn digest_is_chunking_and_checkpoint_invariant() {
    let one = digest_of(1, false);
    assert_eq!(one, digest_of(37, false), "37 batches");
    assert_eq!(one, digest_of(37, true), "37 batches + checkpoints");
}

#[test]
fn digest_ignores_checkpointed_offset_only() {
    let dir = ostro_testkit::fs::tempdir();
    let store = open(dir.path());
    write(&store, &rows(100), 1, false);
    let d0 = store.digest().expect("digest");
    store.checkpoint(Offset(12_345)).expect("checkpoint");
    assert_eq!(store.digest().expect("digest"), d0, "checkpoint alone");

    // applied_offset, state_hash and cursor are all covered.
    let base = final_meta();
    let variants = [
        MetaUpdate {
            applied: Offset(2_001),
            ..base.clone()
        },
        MetaUpdate {
            state_hash: [0xAC; 32],
            ..base.clone()
        },
        MetaUpdate {
            cursor: CursorMeta {
                epoch: 5,
                ..base.cursor.clone()
            },
            ..base.clone()
        },
        MetaUpdate {
            inbound_seen_changed: vec![(3, 100)],
            ..base.clone()
        },
    ];
    for m in variants {
        store.batch().commit(&m).expect("commit");
        assert_ne!(store.digest().expect("digest"), d0, "{m:?}");
        store.batch().commit(&base).expect("restore");
        assert_eq!(store.digest().expect("digest"), d0, "restored");
    }
}

#[test]
fn digest_detects_logical_changes() {
    let dir = ostro_testkit::fs::tempdir();
    let store = open(dir.path());
    let rs = rows(200);
    write(&store, &rs, 3, false);
    let d0 = store.digest().expect("digest");
    let (t, k, v) = rs.iter().find(|(_, _, v)| !v.is_empty()).expect("row");

    // Flip one value byte.
    let mut changed = v.clone();
    changed[0] ^= 1;
    let mut b = store.batch();
    b.insert(*t, k.clone(), changed);
    b.commit(&final_meta()).expect("commit");
    assert_ne!(store.digest().expect("digest"), d0, "value byte");

    // Restore, then add a row.
    let mut b = store.batch();
    b.insert(*t, k.clone(), v.clone());
    b.commit(&final_meta()).expect("commit");
    assert_eq!(store.digest().expect("digest"), d0);
    let mut b = store.batch();
    b.insert(Table::Dlq, b"new".to_vec(), Vec::new());
    b.commit(&final_meta()).expect("commit");
    assert_ne!(store.digest().expect("digest"), d0, "added row");

    // Remove it again, then remove an original row.
    let mut b = store.batch();
    b.remove(Table::Dlq, b"new".to_vec());
    b.commit(&final_meta()).expect("commit");
    assert_eq!(store.digest().expect("digest"), d0);
    let mut b = store.batch();
    b.remove(*t, k.clone());
    b.commit(&final_meta()).expect("commit");
    assert_ne!(store.digest().expect("digest"), d0, "removed row");

    // Moving a row to another table changes the digest too.
    let mut b = store.batch();
    b.insert(Table::Children, k.clone(), v.clone());
    b.commit(&final_meta()).expect("commit");
    assert_ne!(store.digest().expect("digest"), d0, "row in wrong table");
}

#[test]
fn rebuild_after_delete_gives_same_digest() {
    let dir = ostro_testkit::fs::tempdir();
    let rs = rows(500);
    let d0 = {
        let store = open(dir.path());
        write(&store, &rs, 7, true);
        store.digest().expect("digest")
    };
    std::fs::remove_dir_all(state_dir(dir.path(), P)).expect("rm state");
    let store = open(dir.path());
    write(&store, &rs, 2, false);
    assert_eq!(store.digest().expect("digest"), d0);
    drop(store);
    // And it survives reopen.
    assert_eq!(open(dir.path()).digest().expect("digest"), d0);
}
