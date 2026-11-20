//! OSNP v1 snapshots (plan 08-02, BACK-04): export from one fjall snapshot, streaming reader,
//! corruption detection, concurrent (batch-atomic) export.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use ostro_partition::store::batch::MetaUpdate;
use ostro_partition::store::keys;
use ostro_partition::store::snapshot::{
    self, ExportOptions, Frame, HEADER_LEN, OsnpReader, SnapshotError, SnapshotInfo,
    TRAILER_FRAME_LEN,
};
use ostro_partition::store::values::{
    self, BufferedFire, CursorMeta, RunRecord, ScheduleStateRow, SignalSlot,
};
use ostro_partition::{PartitionStore, StoreConfig, TABLES, Table};
use ostro_types::command::{JournalEvent, OutboxMsg, Signal, StartRun, outbox_msg};
use ostro_types::{Offset, PartitionCount, PartitionId, PayloadRef, RunId};
use proptest::prelude::*;
use prost::Message;

const P: PartitionId = PartitionId(3);

fn count() -> PartitionCount {
    PartitionCount::new(8).expect("non-zero")
}

fn open(dir: &Path) -> PartitionStore {
    PartitionStore::open(dir, P, count(), &StoreConfig::default())
        .expect("open")
        .0
}

fn sha(i: u64) -> [u8; 32] {
    *blake3::hash(&i.to_le_bytes()).as_bytes()
}

fn run(i: u64) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.0.to_be_bytes());
    b[2..10].copy_from_slice(&i.to_be_bytes());
    b[8] = 0x70 | (b[8] & 0x0f); // uuid v7 version nibble
    b[10] = 0x80 | (b[10] & 0x3f); // variant
    RunId::from_bytes(&b).expect("run id")
}

fn payload(i: u64) -> PayloadRef {
    if i.is_multiple_of(5) {
        PayloadRef::blob(sha(i), 300 * 1024)
    } else {
        PayloadRef::inline(Bytes::from(format!("payload-{i}").repeat(3)))
    }
}

/// One deterministic "apply chunk": a mix of every row shape that carries blob refs, plus
/// deletes of older rows. Chunk `i` leaves `applied_offset = i + 1`.
fn chunk_ops(i: u64) -> Vec<(Table, Vec<u8>, Option<Vec<u8>>)> {
    let r = run(i);
    let mut ops = vec![
        (
            Table::Runs,
            keys::run_key(&r),
            Some(values::encode(&RunRecord {
                workflow: format!("wf{}", i % 7),
                queue: "q".into(),
                next_seq: 1,
                created_at_ms: i,
                rng_seed: vec![7; 32],
                ..Default::default()
            })),
        ),
        (
            Table::Events,
            keys::event_key(&r, 0),
            Some(
                JournalEvent {
                    seq: 0,
                    kind: 16,
                    payload: Some(payload(i)),
                }
                .encode_to_vec(),
            ),
        ),
        (
            Table::Payloads,
            keys::payload_key(&r, 0),
            Some(payload(i + 1).encode_to_vec()),
        ),
        (Table::Timers, keys::timer_key(i * 10, &r, 1), Some(vec![])),
    ];
    if i.is_multiple_of(7) {
        ops.push((
            Table::Outbox,
            keys::outbox_key(1, i + 1),
            Some(
                OutboxMsg {
                    kind: Some(outbox_msg::Kind::Signal(Signal {
                        target_run: r.as_bytes().to_vec(),
                        name: "s".into(),
                        payload: Some(PayloadRef::blob(sha(i + 1000), 9)),
                    })),
                }
                .encode_to_vec(),
            ),
        ));
    }
    if i.is_multiple_of(11) {
        ops.push((
            Table::Inbox,
            keys::signal_inbox_key(&r, "sig", 0),
            Some(values::encode(&SignalSlot::Pending {
                event: PayloadRef::blob(sha(i + 2000), 9),
                sent_at_ms: i,
            })),
        ));
    }
    if i.is_multiple_of(13) {
        let start = StartRun {
            input: Some(PayloadRef::blob(sha(i + 3000), 9)),
            ..Default::default()
        };
        ops.push((
            Table::ScheduleState,
            keys::schedule_state_key(&format!("sched{}", i % 3)),
            Some(values::encode(&ScheduleStateRow {
                watermark_ms: i,
                buffered: Some(BufferedFire {
                    fire_time_ms: i,
                    start: start.encode_to_vec(),
                }),
                ..Default::default()
            })),
        ));
    }
    if i % 3 == 2 && i >= 2 {
        let old = run(i - 2);
        ops.push((Table::Runs, keys::run_key(&old), None));
        ops.push((Table::Events, keys::event_key(&old, 0), None));
        ops.push((Table::Timers, keys::timer_key((i - 2) * 10, &old, 1), None));
    }
    ops
}

fn chunk_meta(i: u64) -> MetaUpdate {
    MetaUpdate {
        applied: Offset(i + 1),
        state_hash: *blake3::hash(format!("h{i}").as_bytes()).as_bytes(),
        cursor: CursorMeta {
            epoch: 1 + (i / 50) as u32,
            epoch_history: (0..=(i / 50)).map(|e| (1 + e as u32, e * 50)).collect(),
            outbox_next_seq: BTreeMap::from([(1, i + 2)]),
            outbox_truncated: BTreeMap::new(),
        },
        inbound_seen_changed: vec![((i % 4) as u16, i)],
    }
}

fn apply(store: &PartitionStore, i: u64) {
    let mut b = store.batch();
    for (t, k, v) in chunk_ops(i) {
        match v {
            Some(v) => b.insert(t, k, v),
            None => b.remove(t, k),
        }
    }
    b.commit(&chunk_meta(i)).expect("commit");
}

fn build(dir: &Path, chunks: u64) -> PartitionStore {
    let store = open(dir);
    for i in 0..chunks {
        apply(&store, i);
    }
    store.checkpoint(Offset(chunks)).expect("checkpoint");
    store
}

fn export(store: &PartitionStore, lz4: bool) -> (Vec<u8>, SnapshotInfo) {
    let mut buf = Vec::new();
    let info = store
        .export_snapshot_with(&mut buf, 1_700_000_000_000, ExportOptions { lz4 })
        .expect("export");
    (buf, info)
}

#[test]
fn export_round_trips_through_the_reader() {
    let dir = ostro_testkit::fs::tempdir();
    let store = build(dir.path(), 300);
    let (buf, info) = export(&store, false);
    let meta = store.read_meta().expect("meta");

    assert_eq!(info.bytes, buf.len() as u64);
    assert_eq!(info.logical_digest, store.digest().expect("digest"));
    assert_eq!(info.state_hash, meta.state_hash);
    assert_eq!(info.applied_offset, meta.applied.0);
    assert_eq!(info.latest_epoch, meta.cursor.epoch);
    assert_eq!((info.partition, info.partition_count), (P.0, 8));
    let refs: Vec<_> = store
        .blob_refs_snapshot()
        .expect("refs")
        .into_iter()
        .collect();
    assert!(!refs.is_empty());
    assert_eq!(info.blob_refs, refs);

    // The verifier recomputes the same info from the bytes alone.
    assert_eq!(
        snapshot::verify_snapshot(buf.as_slice()).expect("verify"),
        info
    );

    // Frames decode in order: one TableBegin..TableEnd per table in TABLES order, then
    // BlobRefs, then the Trailer.
    let mut rd = OsnpReader::new(buf.as_slice()).expect("header");
    assert_eq!(rd.header().applied_offset, meta.applied.0);
    assert!(!rd.header().lz4());
    let mut names = Vec::new();
    let mut rows_in_table = 0u64;
    let mut total = 0u64;
    let mut saw_refs = false;
    let mut trailer = None;
    while let Some(f) = rd.next_frame().expect("frame") {
        match f {
            Frame::TableBegin(n) => {
                names.push(n);
                rows_in_table = 0;
            }
            Frame::Rows(r) => {
                // keys ascending
                assert!(r.windows(2).all(|w| w[0].0 < w[1].0));
                rows_in_table += r.len() as u64;
            }
            Frame::TableEnd(n) => {
                assert_eq!(n, rows_in_table, "{:?}", names.last());
                total += n;
            }
            Frame::BlobRefs(r) => {
                assert_eq!(r, refs);
                saw_refs = true;
            }
            Frame::Trailer(t) => trailer = Some(t),
            Frame::Skipped { .. } => panic!("no extension frames written"),
        }
    }
    let trailer = trailer.expect("trailer");
    assert!(saw_refs);
    let want: Vec<_> = TABLES.iter().map(|t| t.name().to_string()).collect();
    assert_eq!(names, want);
    assert_eq!(total, info.total_rows);
    assert_eq!(trailer.total_rows, info.total_rows);
    assert_eq!(trailer.frames + 1, rd.frames());
    assert_eq!(trailer.logical_digest, info.logical_digest);

    // checkpointed_offset is not exported: a checkpoint changes neither bytes nor digest.
    store.checkpoint(Offset(12_345)).expect("checkpoint");
    let (buf2, _) = export(&store, false);
    assert_eq!(buf, buf2);
}

#[test]
fn rows_frames_split_at_one_mib() {
    let dir = ostro_testkit::fs::tempdir();
    let store = open(dir.path());
    let mut b = store.batch();
    for i in 0..40u64 {
        // 100 KiB values: about 10 per 1 MiB frame
        b.insert(
            Table::Runs,
            i.to_be_bytes().to_vec(),
            vec![i as u8; 100 * 1024],
        );
    }
    // one row bigger than a frame gets its own frame
    b.insert(Table::Dlq, b"big".to_vec(), vec![1; 3 << 20]);
    b.commit(&chunk_meta(0)).expect("commit");
    let (buf, info) = export(&store, false);
    let mut rd = OsnpReader::new(buf.as_slice()).expect("header");
    let mut frames = Vec::new();
    let mut table = String::new();
    while let Some(f) = rd.next_frame().expect("frame") {
        match f {
            Frame::TableBegin(n) => table = n,
            Frame::Rows(r) => frames.push((table.clone(), r.len())),
            _ => {}
        }
    }
    let runs: Vec<_> = frames.iter().filter(|(t, _)| t == "runs").collect();
    assert_eq!(runs.iter().map(|(_, n)| n).sum::<usize>(), 40);
    assert!(runs.len() >= 4, "{runs:?}");
    assert!(runs.iter().all(|(_, n)| *n <= 10), "{runs:?}");
    assert!(frames.contains(&("dlq".to_string(), 1)));
    assert_eq!(info.logical_digest, store.digest().expect("digest"));
}

#[test]
fn lz4_flag_round_trips() {
    let dir = ostro_testkit::fs::tempdir();
    let store = build(dir.path(), 500);
    let (plain, a) = export(&store, false);
    let (lz, b) = export(&store, true);
    assert!(
        OsnpReader::new(lz.as_slice())
            .expect("header")
            .header()
            .lz4()
    );
    let vb = snapshot::verify_snapshot(lz.as_slice()).expect("verify lz4");
    assert_eq!(vb, b);
    assert_eq!(
        (a.logical_digest, a.state_hash, a.total_rows, &a.blob_refs),
        (b.logical_digest, b.state_hash, b.total_rows, &b.blob_refs)
    );
    eprintln!(
        "lz4 size ratio (synthetic 500-chunk store): {} -> {} bytes ({:.2})",
        plain.len(),
        lz.len(),
        lz.len() as f64 / plain.len() as f64
    );
}

// ------------------------------------------------------------------------- corruption

fn small_snapshot(lz4: bool) -> Vec<u8> {
    let dir = ostro_testkit::fs::tempdir();
    let store = build(dir.path(), 25);
    export(&store, lz4).0
}

fn assert_detected(buf: &[u8], pos: usize, mask: u8) {
    let mut bad = buf.to_vec();
    bad[pos] ^= mask;
    match snapshot::verify_snapshot(bad.as_slice()) {
        Ok(i) => panic!("flip at {pos} (mask {mask:#04x}) verified: {i:?}"),
        Err(
            SnapshotError::HeaderCrc
            | SnapshotError::FrameCrc { .. }
            | SnapshotError::Truncated
            | SnapshotError::UnknownFrame(_)
            | SnapshotError::DigestMismatch { .. }
            | SnapshotError::Malformed(_),
        ) => {}
        Err(e) => panic!("flip at {pos} (mask {mask:#04x}): unexpected error kind {e:?}"),
    }
}

/// Every byte position, two masks, plain and lz4.
#[test]
fn every_single_byte_flip_is_detected() {
    for lz4 in [false, true] {
        let buf = small_snapshot(lz4);
        snapshot::verify_snapshot(buf.as_slice()).expect("clean snapshot verifies");
        for pos in 0..buf.len() {
            assert_detected(&buf, pos, 0x01);
            assert_detected(&buf, pos, 0x80);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: ostro_testkit::proptest_cases(512).max(256),
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    fn random_single_byte_corruption_is_detected(
        lz4 in any::<bool>(),
        frac in 0.0f64..1.0,
        mask in 1u8..=255,
    ) {
        let buf = if lz4 { SMALL_LZ4.with(|b| b.clone()) } else { SMALL.with(|b| b.clone()) };
        let pos = ((buf.len() as f64) * frac) as usize % buf.len();
        assert_detected(&buf, pos, mask);
    }
}

thread_local! {
    static SMALL: Vec<u8> = small_snapshot(false);
    static SMALL_LZ4: Vec<u8> = small_snapshot(true);
}

#[test]
fn truncation_anywhere_is_detected() {
    let buf = small_snapshot(false);
    for n in 0..buf.len() {
        assert!(
            matches!(
                snapshot::verify_snapshot(&buf[..n]),
                Err(SnapshotError::Truncated)
            ),
            "truncated at {n}"
        );
    }
    let mut longer = buf.clone();
    longer.push(0);
    assert!(matches!(
        snapshot::verify_snapshot(longer.as_slice()),
        Err(SnapshotError::Malformed(_))
    ));
}

/// `u32 LE len | kind | payload | u32 LE crc32c(kind ‖ payload)`.
fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut b = (payload.len() as u32).to_le_bytes().to_vec();
    b.push(kind);
    b.extend_from_slice(payload);
    let crc = crc32c::crc32c_append(crc32c::crc32c(&[kind]), payload);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

/// Inserts `f` right after the header and bumps the trailer's frame count (re-CRC'd).
fn splice_after_header(buf: &[u8], f: &[u8]) -> Vec<u8> {
    let tpos = buf.len() - TRAILER_FRAME_LEN;
    let mut trailer = buf[tpos + 5..buf.len() - 4].to_vec();
    let frames = u64::from_le_bytes(trailer[72..80].try_into().unwrap()) + 1;
    trailer[72..80].copy_from_slice(&frames.to_le_bytes());
    let mut out = buf[..HEADER_LEN].to_vec();
    out.extend_from_slice(f);
    out.extend_from_slice(&buf[HEADER_LEN..tpos]);
    out.extend_from_slice(&frame(5, &trailer));
    out
}

#[test]
fn skippable_frames_are_skipped_and_unknown_low_kinds_rejected() {
    let buf = small_snapshot(false);
    let clean = snapshot::verify_snapshot(buf.as_slice()).expect("clean");

    let ext = splice_after_header(&buf, &frame(200, b"future extension"));
    let got = snapshot::verify_snapshot(ext.as_slice()).expect("kind 200 is skipped");
    assert_eq!(got.logical_digest, clean.logical_digest);
    assert_eq!(got.bytes, ext.len() as u64);
    let mut rd = OsnpReader::new(ext.as_slice()).expect("header");
    assert_eq!(
        rd.next_frame().expect("frame"),
        Some(Frame::Skipped { kind: 200, len: 16 })
    );

    let bad = splice_after_header(&buf, &frame(9, b"x"));
    assert!(matches!(
        snapshot::verify_snapshot(bad.as_slice()),
        Err(SnapshotError::UnknownFrame(9))
    ));
}

#[test]
fn newer_state_format_is_refused() {
    let mut buf = small_snapshot(false);
    buf[12..16].copy_from_slice(&(snapshot::STATE_FORMAT + 1).to_le_bytes());
    let crc = crc32c::crc32c(&buf[..60]);
    buf[60..64].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(
        snapshot::verify_snapshot(buf.as_slice()),
        Err(SnapshotError::FutureFormat { .. })
    ));
}

#[test]
fn migrate_row_is_identity_for_the_current_format() {
    let (k, v) = snapshot::migrate_row(
        Table::Runs,
        snapshot::STATE_FORMAT,
        b"k".to_vec(),
        b"v".to_vec(),
    )
    .expect("identity");
    assert_eq!((k.as_slice(), v.as_slice()), (&b"k"[..], &b"v"[..]));
    assert!(snapshot::migrate_row(Table::Runs, 0, vec![], vec![]).is_err());
}

// ------------------------------------------------------------------------- concurrency

/// The apply thread commits chunks continuously while another thread exports 20 times. Each
/// export's rows give exactly the digest of a store replayed to that export's applied_offset.
#[test]
fn concurrent_export_is_batch_atomic() {
    const CHUNKS: u64 = 3_000;
    let dir = ostro_testkit::fs::tempdir();
    let store = Arc::new(open(dir.path()));
    let done = Arc::new(AtomicBool::new(false));
    let applier = {
        let (store, done) = (Arc::clone(&store), Arc::clone(&done));
        std::thread::spawn(move || {
            for i in 0..CHUNKS {
                apply(&store, i);
            }
            done.store(true, Ordering::SeqCst);
        })
    };
    let mut snaps = Vec::new();
    let mut last = 0;
    while snaps.len() < 20 {
        let applied = store.read_meta().expect("meta").applied.0;
        // spread the exports over the run (until the applier finishes)
        if applied < last + 40 && !done.load(Ordering::SeqCst) {
            std::thread::yield_now();
            continue;
        }
        last = applied;
        let mut buf = Vec::new();
        let info = store.export_snapshot(&mut buf, 0).expect("export");
        snaps.push((info, buf));
    }
    applier.join().expect("applier");

    // Rows <-> digest: the verifier recomputes the digest from the frames.
    for (info, buf) in &snaps {
        assert_eq!(
            &snapshot::verify_snapshot(buf.as_slice()).expect("verify"),
            info
        );
    }
    let mut ks: Vec<u64> = snaps.iter().map(|(i, _)| i.applied_offset).collect();
    ks.sort_unstable();
    ks.dedup();
    assert!(ks.len() >= 2, "exports never overlapped apply: {ks:?}");

    // Replay once, checking the digest at every exported offset.
    let rdir = ostro_testkit::fs::tempdir();
    let replay = open(rdir.path());
    let mut by_k: BTreeMap<u64, Vec<&SnapshotInfo>> = BTreeMap::new();
    for (info, _) in &snaps {
        by_k.entry(info.applied_offset).or_default().push(info);
    }
    let mut next = 0u64;
    for (k, infos) in by_k {
        while next < k {
            apply(&replay, next);
            next += 1;
        }
        let d = replay.digest().expect("digest");
        for info in infos {
            assert_eq!(info.logical_digest, d, "export at applied {k}");
            assert_eq!(info.state_hash, chunk_meta(k - 1).state_hash);
        }
    }
}
