//! STOR-14: payloads over 256 KiB go to the blob store durably before the referencing command
//! is appended, and are read back sha-verified.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use bytes::Bytes;
use common::{cfg, new_run, open_single, start_run};
use ostro_blob::BlobRef;
use ostro_partition::store::keys::payload_key;
use ostro_partition::{ApplyOutcome, RejectReason, Table};
use ostro_types::payload::payload_ref::Kind;
use ostro_types::{Command, INLINE_PAYLOAD_LIMIT, PayloadRef};
use prost::Message;

fn pattern(n: usize) -> Bytes {
    Bytes::from((0..n).map(|i| (i % 251) as u8).collect::<Vec<_>>())
}

fn blob_ref(p: &PayloadRef) -> BlobRef {
    match &p.kind {
        Some(Kind::Blob(b)) => BlobRef {
            sha256: b.sha256.as_slice().try_into().expect("32-byte sha"),
            len: b.len,
        },
        other => panic!("expected a blob ref, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_payload_is_blob_before_append_and_round_trips() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let bytes = pattern(300 * 1024);
    let p = tp.handle.prepare_payload(bytes.clone()).await.unwrap();
    let r = blob_ref(&p);
    assert_eq!(r.len, bytes.len() as u64);
    // Durable in the blob store before the StartRun is even built.
    assert!(tp.handle.blobs().contains(&r));

    let run = new_run();
    let mut s = start_run(&run);
    s.input = Some(p);
    assert_eq!(
        tp.handle.submit_one(Command::start_run(s)).await.unwrap(),
        ApplyOutcome::Accepted
    );
    let stored = tp
        .handle
        .reader()
        .get(Table::Payloads, &payload_key(&run, 0))
        .unwrap()
        .expect("input payload row");
    let stored = PayloadRef::decode(stored.as_slice()).unwrap();
    assert_eq!(tp.handle.fetch_payload(&stored).await.unwrap(), bytes);
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_payload_is_inline() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let bytes = pattern(100 * 1024);
    let p = tp.handle.prepare_payload(bytes.clone()).await.unwrap();
    assert!(matches!(&p.kind, Some(Kind::Inline(b)) if *b == bytes));
    // Exactly at the limit stays inline.
    let p = tp
        .handle
        .prepare_payload(pattern(INLINE_PAYLOAD_LIMIT))
        .await
        .unwrap();
    assert_eq!(p.inline_len(), Some(INLINE_PAYLOAD_LIMIT));
    assert_eq!(
        tp.handle.fetch_payload(&p).await.unwrap().len(),
        INLINE_PAYLOAD_LIMIT
    );
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversize_inline_bypassing_prepare_is_rejected() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let run = new_run();
    let mut s = start_run(&run);
    s.input = Some(PayloadRef::inline(pattern(300 * 1024)));
    assert_eq!(
        tp.handle.submit_one(Command::start_run(s)).await.unwrap(),
        ApplyOutcome::Rejected(RejectReason::PayloadTooLarge)
    );
    assert!(tp.handle.read_run(&run).unwrap().is_none());
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupted_blob_is_an_error_never_wrong_bytes() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let p = tp
        .handle
        .prepare_payload(pattern(300 * 1024))
        .await
        .unwrap();
    let path = tp.handle.blobs().path_of(&blob_ref(&p));
    let mut raw = std::fs::read(&path).unwrap();
    raw[1234] ^= 0x01;
    // Blob files are written read-only-ish by some stores; write via a fresh file.
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, &raw).unwrap();
    assert!(tp.handle.fetch_payload(&p).await.is_err());
    tp.close().await.unwrap();
}
