//! Exhaustive blob-ref visitor (plan 08-02, F5 invariant): every documented `PayloadRef`
//! location is found, inline payloads and opaque bytes are never mistaken for refs.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeSet;

use bytes::Bytes;
use common::{cfg, new_run, open_single, start_run};
use ostro_partition::store::batch::MetaUpdate;
use ostro_partition::store::blobrefs::visit_row;
use ostro_partition::store::keys;
use ostro_partition::store::values::{
    self, BufferedFire, CursorMeta, InboxMsg, RunRecord, RunStatus, ScheduleRunRow,
    ScheduleStateRow, SignalSlot,
};
use ostro_partition::{ApplyOutcome, PartitionStore, StoreConfig, TABLES, Table};
use ostro_types::command::{
    CancelRun, ChildCompleted, CronStart, JournalEvent, OutboxMsg, Signal, StartChild, StartRun,
    outbox_msg,
};
use ostro_types::{Command, Offset, PartitionCount, PartitionId, PayloadRef};
use prost::Message;

fn sha(i: u8) -> [u8; 32] {
    [i; 32]
}

fn blob(i: u8) -> Option<PayloadRef> {
    Some(PayloadRef::blob(sha(i), 1 << 20))
}

fn inline() -> Option<PayloadRef> {
    // opaque bytes that happen to look like a prost BlobRef: never a ref (F5)
    let fake = PayloadRef::blob(sha(0xEE), 1).encode_to_vec();
    Some(PayloadRef::inline(Bytes::from(fake)))
}

fn refs(t: Table, k: &[u8], v: &[u8]) -> Vec<[u8; 32]> {
    let mut out = Vec::new();
    visit_row(t, k, v, &mut out).expect("visit");
    out
}

fn start(input: Option<PayloadRef>, started: Option<PayloadRef>) -> StartRun {
    StartRun {
        input,
        run_started: started.map(|p| JournalEvent {
            seq: 0,
            kind: 1,
            payload: Some(p),
        }),
        ..Default::default()
    }
}

fn outbox(k: outbox_msg::Kind) -> Vec<u8> {
    OutboxMsg { kind: Some(k) }.encode_to_vec()
}

#[test]
fn every_payload_ref_location_is_visited() {
    let run = new_run();
    // events / payloads
    let ev = |p| {
        JournalEvent {
            seq: 3,
            kind: 16,
            payload: p,
        }
        .encode_to_vec()
    };
    assert_eq!(
        refs(Table::Events, &keys::event_key(&run, 3), &ev(blob(1))),
        [sha(1)]
    );
    assert!(refs(Table::Events, &keys::event_key(&run, 3), &ev(inline())).is_empty());
    assert!(refs(Table::Events, &keys::event_key(&run, 3), &ev(None)).is_empty());
    let pk = keys::payload_key(&run, 1);
    assert_eq!(
        refs(Table::Payloads, &pk, &blob(2).unwrap().encode_to_vec()),
        [sha(2)]
    );
    assert!(refs(Table::Payloads, &pk, &inline().unwrap().encode_to_vec()).is_empty());

    // outbox: every kind
    let ok = keys::outbox_key(1, 1);
    let cases: Vec<(outbox_msg::Kind, Vec<[u8; 32]>)> = vec![
        (
            outbox_msg::Kind::StartChild(StartChild {
                start: Some(start(blob(3), blob(4))),
            }),
            vec![sha(3), sha(4)],
        ),
        (
            outbox_msg::Kind::ChildCompleted(ChildCompleted {
                result: blob(5),
                event: blob(6),
                ..Default::default()
            }),
            vec![sha(5), sha(6)],
        ),
        (
            outbox_msg::Kind::Signal(Signal {
                payload: blob(7),
                ..Default::default()
            }),
            vec![sha(7)],
        ),
        (outbox_msg::Kind::CancelRun(CancelRun::default()), vec![]),
        (
            outbox_msg::Kind::CronStart(CronStart {
                start: Some(start(blob(8), inline())),
                ..Default::default()
            }),
            vec![sha(8)],
        ),
        (
            outbox_msg::Kind::StartRun(start(inline(), blob(9))),
            vec![sha(9)],
        ),
    ];
    for (k, want) in cases {
        assert_eq!(refs(Table::Outbox, &ok, &outbox(k.clone())), want, "{k:?}");
    }

    // inbox: signal Pending / Journaled, legacy ChildCompleted / CancelRequested
    let sk = keys::signal_inbox_key(&run, "sig", 0);
    let pending = values::encode(&SignalSlot::Pending {
        event: blob(10).unwrap(),
        sent_at_ms: 1,
    });
    assert_eq!(refs(Table::Inbox, &sk, &pending), [sha(10)]);
    let journaled = values::encode(&SignalSlot::Journaled {
        payload_seq: 4,
        sent_at_ms: 1,
    });
    assert!(refs(Table::Inbox, &sk, &journaled).is_empty());
    let lk = keys::inbox_key(&run, 1);
    let child = |result: Vec<u8>| {
        values::encode(&InboxMsg::ChildCompleted {
            child: [1; 18],
            op: 1,
            status: RunStatus::Completed,
            result,
        })
    };
    assert_eq!(
        refs(Table::Inbox, &lk, &child(blob(11).unwrap().encode_to_vec())),
        [sha(11)]
    );
    assert!(refs(Table::Inbox, &lk, &child(vec![])).is_empty());
    assert!(refs(Table::Inbox, &lk, &child(inline().unwrap().encode_to_vec())).is_empty());
    let cancel = values::encode(&InboxMsg::CancelRequested { reason: "r".into() });
    assert!(refs(Table::Inbox, &lk, &cancel).is_empty());

    // schedule_state: buffered StartRun; reverse index has none
    let row = ScheduleStateRow {
        buffered: Some(BufferedFire {
            fire_time_ms: 1,
            start: start(blob(12), blob(13)).encode_to_vec(),
        }),
        ..Default::default()
    };
    assert_eq!(
        refs(
            Table::ScheduleState,
            &keys::schedule_state_key("s"),
            &values::encode(&row)
        ),
        [sha(12), sha(13)]
    );
    let empty = values::encode(&ScheduleStateRow::default());
    assert!(refs(Table::ScheduleState, &keys::schedule_state_key("s"), &empty).is_empty());
    let rev = values::encode(&ScheduleRunRow::default());
    assert!(refs(Table::ScheduleState, &keys::schedule_run_key(&run), &rev).is_empty());
}

#[test]
fn tables_without_refs_ignore_their_bytes() {
    // A run record pinned to a module: module shas are not blob refs of run state.
    let rec = values::encode(&RunRecord {
        module_sha: Some(sha(0x5a)),
        ..Default::default()
    });
    assert!(refs(Table::Runs, &[0; 18], &rec).is_empty());
    let opaque = blob(1).unwrap().encode_to_vec();
    for t in TABLES {
        if matches!(
            t,
            Table::Events | Table::Payloads | Table::Inbox | Table::Outbox | Table::ScheduleState
        ) {
            continue;
        }
        assert!(refs(t, b"k", &opaque).is_empty(), "{t:?}");
    }
}

#[test]
fn undecodable_ref_rows_are_errors_not_skips() {
    let mut out = Vec::new();
    assert!(visit_row(Table::Payloads, b"k", &[0xff, 0xff, 0xff], &mut out).is_err());
    assert!(visit_row(Table::ScheduleState, &[0x09], &[], &mut out).is_err());
    let bad_sha = PayloadRef {
        kind: Some(ostro_types::payload::payload_ref::Kind::Blob(
            ostro_types::BlobRef {
                sha256: vec![1; 31],
                len: 1,
            },
        )),
    };
    assert!(visit_row(Table::Payloads, b"k", &bad_sha.encode_to_vec(), &mut out).is_err());
}

#[test]
fn blob_refs_snapshot_collects_a_deduplicated_set() {
    let dir = ostro_testkit::fs::tempdir();
    let (store, _) = PartitionStore::open(
        dir.path(),
        PartitionId(0),
        PartitionCount::new(1).unwrap(),
        &StoreConfig::default(),
    )
    .expect("open");
    let run = new_run();
    let mut b = store.batch();
    b.insert(
        Table::Payloads,
        keys::payload_key(&run, 0),
        blob(1).unwrap().encode_to_vec(),
    );
    b.insert(
        Table::Payloads,
        keys::payload_key(&run, 1),
        blob(2).unwrap().encode_to_vec(),
    );
    b.insert(
        Table::Events,
        keys::event_key(&run, 0),
        JournalEvent {
            seq: 0,
            kind: 1,
            payload: blob(1),
        }
        .encode_to_vec(),
    );
    b.commit(&MetaUpdate {
        applied: Offset(1),
        state_hash: [0; 32],
        cursor: CursorMeta::default(),
        inbound_seen_changed: vec![],
    })
    .expect("commit");
    assert_eq!(
        store.blob_refs_snapshot().expect("refs"),
        BTreeSet::from([sha(1), sha(2)])
    );
}

/// End to end: a real > 256 KiB run input goes to the blob store, and the visitor finds it in
/// the applied state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn applied_blob_input_is_found() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let bytes = Bytes::from(vec![3u8; 300 * 1024]);
    let p = tp.handle.prepare_payload(bytes).await.unwrap();
    let Some(ostro_types::payload::payload_ref::Kind::Blob(b)) = &p.kind else {
        panic!("expected a blob ref");
    };
    let want: [u8; 32] = b.sha256.as_slice().try_into().unwrap();
    let mut s = start_run(&new_run());
    s.input = Some(p);
    assert_eq!(
        tp.handle.submit_one(Command::start_run(s)).await.unwrap(),
        ApplyOutcome::Accepted
    );
    let set = tp.handle.store().blob_refs_snapshot().expect("refs");
    assert_eq!(set, BTreeSet::from([want]));
    tp.close().await.expect("close");
}
