//! Partition state store: open/validate, ApplyBatch overlay, atomic commit,
//! checkpoint, key ordering and value codecs (plan 02-05, STOR-05/STOR-09).

use std::collections::BTreeMap;
use std::path::Path;

use bytes::Bytes;
use fjall::{KeyspaceCreateOptions, PersistMode};
use ostro_partition::store::batch::MetaUpdate;
use ostro_partition::store::config::state_dir;
use ostro_partition::store::keys::{
    INBOX_LEGACY_TAG, WAIT_NAME, WAIT_ROW, children_key, children_prefix, decode_outbox_key,
    decode_ready_key, decode_signal_inbox_key, decode_timer_key, decode_wait_key,
    decode_wait_name_key, dedupe_key, event_key, inbound_key, inbox_key, outbox_key, outbox_prefix,
    payload_key, ready_key, run_key, run_prefix, signal_inbox_key, signal_inbox_prefix,
    signal_seen_key, timer_key, wait_key, wait_name_key, wait_name_prefix, wait_prefixes,
};
use ostro_partition::store::keys::{
    SCHEDULE_RUN, SCHEDULE_STATE, decode_schedule_run_key, decode_schedule_state_key,
    schedule_run_key, schedule_state_key,
};
use ostro_partition::store::values::{
    self, CursorMeta, InboxMsg, PauseMark, ReadySlot, ReadyValue, RunRecord, RunStatus, SignalSlot,
    WaitKind, WaitRow,
};
use ostro_partition::store::values::{BufferedFire, ScheduleRunRow, ScheduleStateRow};
use ostro_partition::{PartitionStore, StoreConfig, StoreError, StoreMeta, TABLES, Table};
use ostro_types::{Offset, PartitionCount, PartitionId, PayloadRef, RunId};
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
            "effects",
            "running",
            "waits",
            "signal_seen",
            "schedule_state"
        ]
    );
    // RESERVATIONS.md §6: ordinal = declaration index.
    assert_eq!(Table::Waits as u8, 14);
    assert_eq!(Table::SignalSeen as u8, 15);
    // Phase 8 (08-01): ordinal 16 `schedule_state`.
    assert_eq!(Table::ScheduleState as u8, 16);
    assert_eq!(TABLES[16].name(), "schedule_state");
    assert_eq!(TABLES.len(), 17);
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

/// Regression: a `StoreReader` must never see half of a committed batch. fjall's batch
/// commit inserts items into the memtables one by one (table order: `runs` before
/// `events` before `payloads`) and publishes the batch seqno only after the last one; a
/// plain `Keyspace::get` reads at `SeqNo::MAX` and sees the unpublished prefix. That is how
/// a long poll answered `completed: true, result: null`: it read the terminal run record
/// of a `CompleteRun` batch still being applied, then found no result payload row.
/// Thousands of `events` rows between the two widen the window from microseconds to
/// milliseconds, so a spinning reader lands inside it on every batch.
#[test]
fn reader_never_sees_a_partial_batch() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const BATCHES: u8 = 6;
    let dir = ostro_testkit::fs::tempdir();
    let (store, _) = open(dir.path(), 0, 16)?;
    let reader = store.reader();
    let done = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let probes = Arc::new(AtomicU64::new(0));
    let spin = {
        let (done, torn, probes) = (Arc::clone(&done), Arc::clone(&torn), Arc::clone(&probes));
        std::thread::spawn(move || -> Result<(), StoreError> {
            while !done.load(Ordering::Acquire) {
                for n in 0..BATCHES {
                    let r = run(0, n);
                    if reader.get(Table::Runs, &run_key(&r))?.is_some() {
                        probes.fetch_add(1, Ordering::Relaxed);
                        if reader.get(Table::Payloads, &payload_key(&r, 1))?.is_none() {
                            torn.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            Ok(())
        })
    };
    for n in 0..BATCHES {
        let r = run(0, n);
        let mut b = store.batch();
        b.insert(Table::Runs, run_key(&r), b"terminal".to_vec());
        for seq in 0..20_000u64 {
            b.insert(Table::Events, event_key(&r, seq), vec![0u8; 16]);
        }
        b.insert(Table::Payloads, payload_key(&r, 1), b"result".to_vec());
        b.commit(&meta_update(u64::from(n) + 1))?;
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    done.store(true, Ordering::Release);
    spin.join().expect("reader thread")?;
    assert!(
        probes.load(Ordering::Relaxed) > 0,
        "the reader saw the runs"
    );
    assert_eq!(
        torn.load(Ordering::Relaxed),
        0,
        "a reader saw a run row of a batch whose payload row was not visible yet"
    );
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
    assert_eq!(inbox_key(&r, 9).len(), 28);
    assert_eq!(&inbox_key(&r, 9)[18..20], &INBOX_LEGACY_TAG.to_be_bytes());
    assert_eq!(&inbox_key(&r, 9)[20..], &9u64.to_be_bytes());
    assert!(inbox_key(&r, 9).starts_with(&run_prefix(&r)));
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
fn phase5_key_layouts() {
    let r = run(3, 1);
    // waits: 0x01 ‖ run ‖ op and 0x02 ‖ run ‖ name_len ‖ name ‖ op.
    let wk = wait_key(&r, 9);
    assert_eq!(wk.len(), 1 + 18 + 8);
    assert_eq!(wk[0], WAIT_ROW);
    assert_eq!(&wk[1..19], r.as_bytes());
    assert_eq!(decode_wait_key(&wk), Some((r, 9)));
    let nk = wait_name_key(&r, "approve", 9);
    assert_eq!(nk[0], WAIT_NAME);
    assert_eq!(&nk[19..21], &7u16.to_be_bytes());
    assert_eq!(&nk[21..28], b"approve");
    assert_eq!(&nk[28..], &9u64.to_be_bytes());
    assert_eq!(
        decode_wait_name_key(&nk),
        Some((r, "approve".to_string(), 9))
    );
    assert!(nk.starts_with(&wait_name_prefix(&r, "approve")));
    assert!(!nk.starts_with(&wait_name_prefix(&r, "approv")));
    let [p1, p2] = wait_prefixes(&r);
    assert!(wk.starts_with(&p1) && !wk.starts_with(&p2));
    assert!(nk.starts_with(&p2) && !nk.starts_with(&p1));
    assert_eq!(decode_wait_key(&nk), None);
    assert_eq!(decode_wait_name_key(&wk), None);

    // signal inbox: run ‖ name_len ‖ name ‖ n; legacy rows never match a signal prefix.
    let sk = signal_inbox_key(&r, "go", 4);
    assert_eq!(sk.len(), 18 + 2 + 2 + 8);
    assert!(sk.starts_with(&signal_inbox_prefix(&r, "go")));
    assert!(sk.starts_with(&run_prefix(&r)));
    assert_eq!(decode_signal_inbox_key(&sk), Some((r, "go".to_string(), 4)));
    assert_eq!(decode_signal_inbox_key(&inbox_key(&r, 4)), None);
    assert!(!inbox_key(&r, 0).starts_with(&signal_inbox_prefix(&r, "")));
    assert!(signal_inbox_key(&r, "", 0).starts_with(&signal_inbox_prefix(&r, "")));

    // signal_seen: run ‖ request_id.
    assert_eq!(&signal_seen_key(&r, b"req-1")[..18], r.as_bytes());
    assert_eq!(&signal_seen_key(&r, b"req-1")[18..], b"req-1");
}

#[test]
fn waits_and_signal_inbox_scan_in_fifo_order() -> Result<(), Box<dyn std::error::Error>> {
    let dir = ostro_testkit::fs::tempdir();
    let (store, _) = open(dir.path(), 3, 16)?;
    let r = run(3, 1);
    let other = run(3, 2);
    let mut b = store.batch();
    // Inserted out of order, across names and runs.
    for op in [300u64, 7, 1 << 40, 42, 256] {
        b.insert(Table::Waits, wait_name_key(&r, "a", op), Vec::new());
        b.insert(
            Table::Waits,
            wait_key(&r, op),
            values::encode(&WaitRow {
                kind: WaitKind::Signal,
                name: "a".into(),
                timeout_due_ms: 0,
            }),
        );
    }
    b.insert(Table::Waits, wait_name_key(&r, "ab", 1), Vec::new());
    b.insert(Table::Waits, wait_name_key(&r, "", 2), Vec::new());
    b.insert(Table::Waits, wait_name_key(&other, "a", 3), Vec::new());
    for n in [9u64, 2, 300, 0] {
        b.insert(
            Table::Inbox,
            signal_inbox_key(&r, "a", n),
            values::encode(&SignalSlot::Journaled {
                payload_seq: n,
                sent_at_ms: 1,
            }),
        );
    }
    b.insert(Table::Inbox, signal_inbox_key(&r, "b", 1), Vec::new());
    b.insert(Table::Inbox, inbox_key(&r, 5), Vec::new());
    b.commit(&meta_update(1))?;

    let rd = store.reader();
    let ops: Vec<u64> = rd
        .prefix(Table::Waits, &wait_name_prefix(&r, "a"), usize::MAX)?
        .iter()
        .map(|(k, _)| decode_wait_name_key(k).expect("index key").2)
        .collect();
    assert_eq!(ops, vec![7, 42, 256, 300, 1 << 40], "oldest op first");
    let [p1, p2] = wait_prefixes(&r);
    assert_eq!(rd.prefix(Table::Waits, &p1, usize::MAX)?.len(), 5);
    assert_eq!(rd.prefix(Table::Waits, &p2, usize::MAX)?.len(), 7);
    let ns: Vec<u64> = rd
        .prefix(Table::Inbox, &signal_inbox_prefix(&r, "a"), usize::MAX)?
        .iter()
        .map(|(k, _)| decode_signal_inbox_key(k).expect("signal key").2)
        .collect();
    assert_eq!(ns, vec![0, 2, 9, 300], "arrival order");
    assert_eq!(
        rd.prefix(Table::Inbox, &run_prefix(&r), usize::MAX)?.len(),
        6
    );
    Ok(())
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
        module_sha: Some([0xab; 32]),
        tags: vec!["a".into(), "b".into()],
        tenant: Some("acme".into()),
        traceparent: None,
        run_timeout_ms: Some(30_000),
        completed_at_ms: Some(99),
        schedule_to_start_ms: 5_000,
        retention_ms: 86_400_000,
        idempotency_key: b"idem".to_vec(),
        terminal_reason: "run_timeout".into(),
        resumable: true,
        retry_of: Some(run(3, 4)),
        continued_from: Some(run(3, 5)),
        cancel_reason: "stop".into(),
        resumed_count: 2,
        open_waits: 3,
        s2s_due_ms: 6_000,
        attempt_failures: 4,
        status_seq: Some(11),
        continued_to: Some(run(3, 6)),
        pause: Some(PauseMark {
            reason: 2,
            at_ms: 77,
            detail: "operator".into(),
        }),
    });
    // Defaults: every Phase 5 field empty/zero/None.
    round_trip(&RunRecord::default());
    round_trip(&RunRecord {
        workflow: "wf".into(),
        pause: None,
        retry_of: None,
        ..Default::default()
    });
    round_trip(&WaitRow {
        kind: WaitKind::Signal,
        name: "approve".into(),
        timeout_due_ms: 1234,
    });
    round_trip(&WaitRow {
        kind: WaitKind::Token,
        name: String::new(),
        timeout_due_ms: 0,
    });
    round_trip(&SignalSlot::Journaled {
        payload_seq: 7,
        sent_at_ms: 8,
    });
    round_trip(&SignalSlot::Pending {
        event: PayloadRef::inline(Bytes::from_static(b"\x01encoded-signal-payload")),
        sent_at_ms: 9,
    });
    // A blob-backed pending event is a PayloadRef field of a documented postcard value
    // (BlobRef invariant), never opaque bytes.
    round_trip(&SignalSlot::Pending {
        event: PayloadRef::blob([0xcd; 32], 1 << 20),
        sent_at_ms: 10,
    });
    round_trip(&ReadyValue::default());
    round_trip(&ReadyValue {
        concurrency_key: "ck".into(),
        ..Default::default()
    });
    for s in [
        RunStatus::Scheduled,
        RunStatus::Ready,
        RunStatus::Running,
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
        RunStatus::Terminated,
        RunStatus::TimedOut,
        RunStatus::ContinuedAsNew,
    ] {
        round_trip(&s);
    }
    assert_eq!(RunStatus::default(), RunStatus::Scheduled);
    assert!(RunStatus::Completed.is_terminal());
    assert!(RunStatus::Failed.is_terminal());
    assert!(RunStatus::Cancelled.is_terminal());
    assert!(!RunStatus::Running.is_terminal());
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
        ..Default::default()
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

// ---- Plan 08-01: Phase 8 keyspace `schedule_state` (ordinal 16) ----------------------------

#[test]
fn phase8_schedule_state_keys() {
    assert_eq!(SCHEDULE_STATE, 0x01);
    assert_eq!(SCHEDULE_RUN, 0x02);
    let k = schedule_state_key("nightly");
    assert_eq!(k, b"\x01nightly");
    assert_eq!(decode_schedule_state_key(&k), Some("nightly"));
    let r = run(3, 9);
    let k = schedule_run_key(&r);
    assert_eq!(k.len(), 19);
    assert_eq!(k[0], 0x02);
    assert_eq!(&k[1..], r.as_bytes());
    assert_eq!(decode_schedule_run_key(&k), Some(r));
    // The shapes never decode as each other, and every watermark row sorts before the index.
    assert_eq!(decode_schedule_run_key(&schedule_state_key("x")), None);
    assert_eq!(decode_schedule_state_key(&schedule_run_key(&r)), None);
    assert!(schedule_state_key("\u{10FFFF}") < schedule_run_key(&r));
}

#[test]
fn phase8_schedule_rows_round_trip() {
    round_trip(&ScheduleStateRow::default());
    round_trip(&ScheduleStateRow {
        schedule_created_at_ms: 1_767_000_000_000,
        watermark_ms: 1_767_225_600_000,
        last_run: Some(run(3, 9)),
        buffered: Some(BufferedFire {
            fire_time_ms: 1_767_229_200_000,
            start: vec![0x0a, 0x12, 0x00],
        }),
        fired: 7,
        skipped: 2,
        duplicates: 1,
    });
    round_trip(&ScheduleRunRow {
        schedule: "nightly".into(),
        schedule_created_at_ms: 1_767_000_000_000,
    });
    // last_run uses the RunId's 18-byte encoding (as RunRecord's run-id fields do).
    let with = values::encode(&ScheduleStateRow {
        last_run: Some(run(3, 9)),
        ..ScheduleStateRow::default()
    });
    let without = values::encode(&ScheduleStateRow::default());
    assert_eq!(with.len(), without.len() + 18);
}

#[test]
fn phase8_new_keyspace_opens_empty_on_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (store, _) = open(dir.path(), 0, 1)?;
    assert!(
        store
            .reader()
            .prefix(Table::ScheduleState, &[], 10)?
            .is_empty()
    );
    let d1 = store.digest()?;
    drop(store);
    let (store, _) = open(dir.path(), 0, 1)?;
    assert_eq!(
        store.digest()?,
        d1,
        "empty schedule_state is stable across reopen"
    );
    Ok(())
}
