//! Partition state store: open/validate, ApplyBatch overlay, atomic commit,
//! checkpoint, key ordering and value codecs (plan 02-05, STOR-05/STOR-09).

use std::collections::BTreeMap;
use std::path::Path;

use fjall::{KeyspaceCreateOptions, PersistMode};
use ostro_partition::store::batch::MetaUpdate;
use ostro_partition::store::config::state_dir;
use ostro_partition::store::keys::{
    children_key, children_prefix, decode_outbox_key, decode_ready_key, decode_timer_key,
    dedupe_key, event_key, inbound_key, inbox_key, outbox_key, outbox_prefix, payload_key,
    ready_key, run_key, timer_key,
};
use ostro_partition::store::values::{
    self, CursorMeta, InboxMsg, ReadySlot, ReadyValue, RunRecord, RunStatus,
};
use ostro_partition::{PartitionStore, StoreConfig, StoreError, StoreMeta, TABLES, Table};
use ostro_types::{Offset, PartitionCount, PartitionId, RunId};
use proptest::prelude::*;

fn count(n: u16) -> PartitionCount {
    PartitionCount::new(n).expect("non-zero")
}

fn open(dir: &Path, p: u16, n: u16) -> Result<(PartitionStore, StoreMeta), StoreError> {
    PartitionStore::open(dir, PartitionId(p), count(n), &StoreConfig::default())
}

fn run(p: u16, n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[17] = n;
    b[2] = n.wrapping_mul(7);
    RunId::from_bytes(&b).expect("18 bytes")
}

fn meta_update(applied: u64) -> MetaUpdate {
    MetaUpdate {
        applied: Offset(applied),
        state_hash: [applied as u8; 32],
        cursor: CursorMeta {
            epoch: 3,
            epoch_history: vec![(1, 0), (3, applied)],
            outbox_next_seq: BTreeMap::from([(2, 9)]),
            outbox_truncated: BTreeMap::new(),
        },
        inbound_seen_changed: vec![(5, applied * 2)],
    }
}

#[test]
fn defaults_and_tables() {
    let c = StoreConfig::default();
    assert_eq!(c.worker_threads, 1);
    assert_eq!(c.max_journaling_size, 64 << 20);
    assert_eq!(c.max_memtable_size, 8 << 20);
    assert_eq!(c.cache_size, 16 << 20);
    assert_eq!(
        state_dir(Path::new("/data"), PartitionId(7)),
        Path::new("/data/state/p0007")
    );
    let names: Vec<&str> = TABLES.iter().map(|t| t.name()).collect();
    assert_eq!(
        names,
        [
            "meta",
            "runs",
            "events",
            "payloads",
            "ready",
            "timers",
            "inbox",
            "dedupe",
            "outbox",
            "inbound_seen",
            "children",
            "dlq",
            "effects"
        ]
    );
    let mut sorted = TABLES;
    sorted.sort();
    assert_eq!(sorted, TABLES, "TABLES is declaration order");
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PartitionStore>();
}

#[test]
fn open_creates_meta_and_reopen_matches() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    assert!(!PartitionStore::exists(dir.path(), PartitionId(3)));
    let (store, meta) = open(dir.path(), 3, 16)?;
    assert!(state_dir(dir.path(), PartitionId(3)).is_dir());
    assert!(PartitionStore::exists(dir.path(), PartitionId(3)));
    let expected = StoreMeta {
        partition: PartitionId(3),
        count: count(16),
        applied: Offset(0),
        checkpointed: Offset(0),
        state_hash: [0; 32],
        cursor: CursorMeta::default(),
        inbound_seen: BTreeMap::new(),
    };
    assert_eq!(meta, expected);
    assert_eq!(store.read_meta()?, expected);
    drop(store);
    let (_store, meta) = open(dir.path(), 3, 16)?;
    assert_eq!(meta, expected);
    Ok(())
}

#[test]
fn partition_count_mismatch_is_fatal() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    drop(open(dir.path(), 3, 16)?);
    match open(dir.path(), 3, 64) {
        Err(StoreError::PartitionCountMismatch {
            expected: 64,
            found: 16,
        }) => {}
        other => panic!("expected PartitionCountMismatch, got {other:?}"),
    }
    Ok(())
}

#[test]
fn partition_id_mismatch_is_fatal() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    drop(open(dir.path(), 3, 16)?);
    std::fs::rename(
        state_dir(dir.path(), PartitionId(3)),
        state_dir(dir.path(), PartitionId(4)),
    )?;
    match open(dir.path(), 4, 16) {
        Err(StoreError::PartitionMismatch {
            expected: 4,
            found: 3,
        }) => {}
        other => panic!("expected PartitionMismatch, got {other:?}"),
    }
    Ok(())
}

#[test]
fn unknown_format_version_is_fatal() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    drop(open(dir.path(), 3, 16)?);
    {
        let db = fjall::Database::builder(state_dir(dir.path(), PartitionId(3)))
            .worker_threads(1)
            .open()?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        meta.insert("format_ver", 2u32.to_be_bytes())?;
        db.persist(PersistMode::SyncAll)?;
    }
    match open(dir.path(), 3, 16) {
        Err(StoreError::FormatVersion { found: 2 }) => {}
        other => panic!("expected FormatVersion, got {other:?}"),
    }
    Ok(())
}

#[test]
fn overlay_read_your_writes_and_read_counter() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    let (store, _) = open(dir.path(), 0, 16)?;
    let mut b = store.batch();
    b.insert(Table::Runs, b"k1".to_vec(), b"v1".to_vec());
    b.commit(&meta_update(1))?;

    let mut b = store.batch();
    b.insert(Table::Runs, b"k2".to_vec(), b"v2".to_vec());
    assert_eq!(b.get(Table::Runs, b"k2")?, Some(b"v2".to_vec()));
    assert_eq!(b.reads(), 0, "overlay hit is free");
    b.remove(Table::Runs, b"k2".to_vec());
    assert_eq!(b.get(Table::Runs, b"k2")?, None);
    assert_eq!(b.reads(), 0, "overlay tombstone is free");
    assert_eq!(b.get(Table::Runs, b"k1")?, Some(b"v1".to_vec()));
    assert_eq!(b.reads(), 1, "keyspace point read counted");
    // Same key in another table is a different row.
    assert_eq!(b.get(Table::Events, b"k1")?, None);
    assert_eq!(b.reads(), 2);
    // Overlay remove of a committed row hides it.
    b.remove(Table::Runs, b"k1".to_vec());
    assert_eq!(b.get(Table::Runs, b"k1")?, None);
    assert_eq!(b.reads(), 2);
    Ok(())
}

#[test]
fn range_prefix_merges_overlay_in_key_order() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    let (store, _) = open(dir.path(), 0, 16)?;
    let mut b = store.batch();
    for k in [b"a1", b"a3", b"a5", b"b1"] {
        b.insert(Table::Outbox, k.to_vec(), k.to_vec());
    }
    b.commit(&meta_update(1))?;

    let mut b = store.batch();
    b.insert(Table::Outbox, b"a2".to_vec(), b"new".to_vec());
    b.insert(Table::Outbox, b"a5".to_vec(), b"upd".to_vec());
    b.remove(Table::Outbox, b"a3".to_vec());
    b.insert(Table::Outbox, b"b0".to_vec(), b"x".to_vec());
    b.insert(Table::Timers, b"a4".to_vec(), b"other-table".to_vec());
    let rows = b.range_prefix(Table::Outbox, b"a")?;
    assert_eq!(
        rows,
        vec![
            (b"a1".to_vec(), b"a1".to_vec()),
            (b"a2".to_vec(), b"new".to_vec()),
            (b"a5".to_vec(), b"upd".to_vec()),
        ]
    );
    assert_eq!(b.reads(), 1, "a prefix scan counts one read");
    Ok(())
}

#[test]
fn commit_is_atomic_with_meta() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    let (store, _) = open(dir.path(), 2, 16)?;
    let mut b = store.batch();
    b.insert(Table::Runs, b"r".to_vec(), b"rec".to_vec());
    b.insert(Table::Events, b"e".to_vec(), b"ev".to_vec());
    let mu = meta_update(17);
    b.commit(&mu)?;

    let meta = store.read_meta()?;
    assert_eq!(meta.applied, Offset(17));
    assert_eq!(meta.state_hash, mu.state_hash);
    assert_eq!(meta.cursor, mu.cursor);
    assert_eq!(meta.inbound_seen, BTreeMap::from([(5u16, 34u64)]));
    assert_eq!(meta.checkpointed, Offset(0));
    let r = store.reader();
    assert_eq!(r.get(Table::Runs, b"r")?, Some(b"rec".to_vec()));
    assert_eq!(r.get(Table::Events, b"e")?, Some(b"ev".to_vec()));

    // A batch dropped without commit leaves nothing.
    {
        let mut b = store.batch();
        b.insert(Table::Runs, b"lost".to_vec(), b"x".to_vec());
        b.remove(Table::Runs, b"r".to_vec());
    }
    assert_eq!(r.get(Table::Runs, b"lost")?, None);
    assert_eq!(r.get(Table::Runs, b"r")?, Some(b"rec".to_vec()));
    assert_eq!(store.read_meta()?.applied, Offset(17));
    Ok(())
}

#[test]
fn checkpoint_survives_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    {
        let (store, _) = open(dir.path(), 1, 16)?;
        let mut b = store.batch();
        b.insert(Table::Runs, b"r".to_vec(), b"v".to_vec());
        b.commit(&meta_update(42))?;
        store.checkpoint(Offset(42))?;
        assert_eq!(store.read_meta()?.checkpointed, Offset(42));
    }
    let (store, meta) = open(dir.path(), 1, 16)?;
    assert_eq!(meta.checkpointed, Offset(42));
    assert_eq!(meta.applied, Offset(42));
    assert_eq!(store.reader().get(Table::Runs, b"r")?, Some(b"v".to_vec()));
    Ok(())
}

#[test]
fn reader_prefix_and_range() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    let (store, _) = open(dir.path(), 0, 16)?;
    let mut b = store.batch();
    for seq in 0..10u64 {
        b.insert(Table::Outbox, outbox_key(1, seq), vec![seq as u8]);
        b.insert(Table::Outbox, outbox_key(2, seq), vec![seq as u8]);
    }
    b.commit(&meta_update(1))?;
    let r = store.reader().clone();
    let rows = r.prefix(Table::Outbox, &outbox_prefix(2), 3)?;
    assert_eq!(
        rows.iter()
            .map(|(k, _)| decode_outbox_key(k))
            .collect::<Vec<_>>(),
        vec![Some((2, 0)), Some((2, 1)), Some((2, 2))]
    );
    let rows = r.range(Table::Outbox, &outbox_key(1, 8), &outbox_key(2, 1), 100)?;
    assert_eq!(
        rows.iter()
            .map(|(k, _)| decode_outbox_key(k))
            .collect::<Vec<_>>(),
        vec![Some((1, 8)), Some((1, 9)), Some((2, 0))],
        "range is [from, to)"
    );
    Ok(())
}

#[test]
fn key_layouts() {
    let r = run(3, 1);
    let c = run(3, 2);
    assert_eq!(run_key(&r), r.as_bytes().to_vec());
    assert_eq!(event_key(&r, 5).len(), 26);
    assert_eq!(&event_key(&r, 5)[18..], &5u64.to_be_bytes());
    assert_eq!(payload_key(&r, 1)[18], 1);
    assert_eq!(inbox_key(&r, 9).len(), 26);
    assert_eq!(inbound_key(7), 7u16.to_be_bytes().to_vec());
    assert_eq!(&dedupe_key("wf", b"idem")[..4], &[0, 2, b'w', b'f']);
    assert_eq!(&dedupe_key("wf", b"idem")[4..], b"idem");
    assert!(children_key(&r, &c).starts_with(&children_prefix(&r)));
    assert_eq!(children_key(&r, &c).len(), 36);
    assert!(outbox_key(4, 1).starts_with(&outbox_prefix(4)));
    assert_eq!(decode_timer_key(&timer_key(10, &r, 3)), Some((10, r, 3)));
    assert_eq!(
        decode_ready_key(&ready_key("q", -5, 99, &r)),
        Some(("q".to_string(), -5, 99, r))
    );
    assert_eq!(decode_timer_key(b"short"), None);
}

#[test]
fn ready_key_sorts_higher_priority_first_then_earlier() {
    let r = run(0, 1);
    let mut keys = [
        ready_key("q", 0, 5, &r),
        ready_key("q", 10, 9, &r),
        ready_key("q", 10, 1, &r),
        ready_key("q", -3, 0, &r),
        ready_key("q", i32::MAX, 100, &r),
        ready_key("q", i32::MIN, 0, &r),
    ];
    keys.sort();
    let decoded: Vec<(i32, u64)> = keys
        .iter()
        .map(|k| {
            let (_, p, at, _) = decode_ready_key(k).expect("decodes");
            (p, at)
        })
        .collect();
    assert_eq!(
        decoded,
        vec![
            (i32::MAX, 100),
            (10, 1),
            (10, 9),
            (0, 5),
            (-3, 0),
            (i32::MIN, 0)
        ]
    );
}

proptest! {
    #[test]
    fn timer_key_order_matches_tuple_order(
        a in (any::<u64>(), any::<u8>(), any::<u64>()),
        b in (any::<u64>(), any::<u8>(), any::<u64>()),
    ) {
        let ka = timer_key(a.0, &run(1, a.1), a.2);
        let kb = timer_key(b.0, &run(1, b.1), b.2);
        let ta = (a.0, *run(1, a.1).as_bytes(), a.2);
        let tb = (b.0, *run(1, b.1).as_bytes(), b.2);
        prop_assert_eq!(ka.cmp(&kb), ta.cmp(&tb));
    }

    #[test]
    fn outbox_key_order_matches_tuple_order(
        a in (any::<u16>(), any::<u64>()),
        b in (any::<u16>(), any::<u64>()),
    ) {
        prop_assert_eq!(outbox_key(a.0, a.1).cmp(&outbox_key(b.0, b.1)), a.cmp(&b));
        prop_assert_eq!(decode_outbox_key(&outbox_key(a.0, a.1)), Some(a));
    }

    #[test]
    fn ready_key_order_within_queue(
        a in (any::<i32>(), any::<u64>()),
        b in (any::<i32>(), any::<u64>()),
    ) {
        let r = run(0, 0);
        let ka = ready_key("queue", a.0, a.1, &r);
        let kb = ready_key("queue", b.0, b.1, &r);
        // Higher priority first, then earlier ready_at.
        let ta = (std::cmp::Reverse(a.0), a.1);
        let tb = (std::cmp::Reverse(b.0), b.1);
        prop_assert_eq!(ka.cmp(&kb), ta.cmp(&tb));
    }
}

fn round_trip<T>(v: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = values::encode(v);
    assert_eq!(bytes[0], 1, "format byte");
    let back: T = values::decode(&bytes).expect("decodes");
    assert_eq!(&back, v);
}

#[test]
fn value_round_trips() {
    round_trip(&CursorMeta {
        epoch: 2,
        epoch_history: vec![(1, 0), (2, 77)],
        outbox_next_seq: BTreeMap::from([(1, 2), (3, 4)]),
        outbox_truncated: BTreeMap::from([(1, 1)]),
    });
    round_trip(&RunRecord {
        workflow: "wf".into(),
        workflow_version: 3,
        queue: "default".into(),
        status: RunStatus::Running,
        current_attempt: Some((4, 1)),
        next_seq: 12,
        ready: Some(ReadySlot {
            priority: -1,
            ready_at_ms: 1000,
        }),
        parent: Some(([9; 18], 7)),
        cancel_requested: false,
        concurrency_key: "k".into(),
        priority: -1,
        created_at_ms: 5,
        rng_seed: vec![1, 2, 3],
        inbox_next: 0,
        children: 2,
    });
    round_trip(&ReadyValue {
        concurrency_key: "ck".into(),
    });
    for s in [
        RunStatus::Scheduled,
        RunStatus::Ready,
        RunStatus::Running,
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
    ] {
        round_trip(&s);
    }
    assert!(RunStatus::Completed.is_terminal());
    assert!(RunStatus::Failed.is_terminal());
    assert!(RunStatus::Cancelled.is_terminal());
    assert!(!RunStatus::Running.is_terminal());
    round_trip(&InboxMsg::Signal {
        name: "s".into(),
        payload: vec![1],
    });
    round_trip(&InboxMsg::ChildCompleted {
        child: [1; 18],
        op: 3,
        status: RunStatus::Completed,
        result: vec![4, 5],
    });
    round_trip(&InboxMsg::CancelRequested { reason: "r".into() });
    round_trip(&InboxMsg::TimerFired {
        op: 1,
        fired_at_ms: 2,
    });
}

#[test]
fn decode_rejects_unknown_format_byte() {
    let mut bytes = values::encode(&ReadyValue {
        concurrency_key: "ck".into(),
    });
    bytes[0] = 2;
    assert!(matches!(
        values::decode::<ReadyValue>(&bytes),
        Err(StoreError::Codec(_))
    ));
    assert!(matches!(
        values::decode::<ReadyValue>(&[]),
        Err(StoreError::Codec(_))
    ));
}
