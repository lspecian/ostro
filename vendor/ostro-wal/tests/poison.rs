// Ostro-authored (see VENDORED.md): patch P1 (poison on write/fsync error) regression tests.

#![allow(clippy::all)] // each integration test is its own crate; vendored crates allow clippy

//! A write or fsync error in the group-commit worker must poison the
//! partition: the failing append is never reported as success, the commit
//! callback never fires for it, every queued and later append on that
//! partition fails with `WalError::Poisoned`, and other partitions are
//! unaffected. Requires `--features failpoints`; run with `--test-threads=1`
//! (the failpoint registry is process-global).

use std::path::Path;
use std::sync::{Arc, Mutex};

use ostro_wal::group_commit::CommitCallback;
use ostro_wal::{GroupCommitConfig, GroupCommitWal, WalError, WalRecord};

const TOPIC: &str = "p";

fn record(partition: i32, offset: i64) -> WalRecord {
    WalRecord::new_v2(
        TOPIC.to_string(),
        partition,
        format!("payload-{offset}-0123456789abcdef").into_bytes(),
        offset,
        offset,
        1,
    )
}

fn config() -> GroupCommitConfig {
    let mut c = GroupCommitConfig::low_resource();
    c.enable_rotation = false;
    c
}

type Calls = Arc<Mutex<Vec<(i32, i64, i64)>>>;

fn wal_with_callback(dir: &Path) -> (GroupCommitWal, Calls) {
    let calls: Calls = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&calls);
    let cb: CommitCallback = Arc::new(move |_t: &str, p: i32, lo: i64, hi: i64| {
        sink.lock().unwrap().push((p, lo, hi));
    });
    (GroupCommitWal::with_callback(dir.to_path_buf(), config(), cb), calls)
}

fn segment_len(dir: &Path, partition: i32) -> u64 {
    std::fs::metadata(dir.join(TOPIC).join(partition.to_string()).join(format!("wal_{partition}_0.log")))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn callback_saw(calls: &Calls, partition: i32, offset: i64) -> bool {
    calls
        .lock()
        .unwrap()
        .iter()
        .any(|(p, lo, hi)| *p == partition && *lo <= offset && offset <= *hi)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_failure_is_never_reported_as_success() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (wal, calls) = wal_with_callback(dir.path());

    // One good append first, so the partition has durable bytes.
    wal.append(TOPIC.into(), 0, record(0, 0), 1).await.expect("healthy append");
    assert!(callback_saw(&calls, 0, 0));
    assert!(!wal.is_poisoned(TOPIC, 0));

    fail::cfg("wal::fsync", "return").unwrap();
    let r = wal.append(TOPIC.into(), 0, record(0, 1), 1).await;
    assert!(
        matches!(r, Err(WalError::Poisoned(_)) | Err(WalError::CommitFailed(_))),
        "fsync failure must not be Ok: {r:?}"
    );
    fail::remove("wal::fsync");

    assert!(wal.is_poisoned(TOPIC, 0));
    assert!(!callback_saw(&calls, 0, 1), "callback must never fire for a failed batch");

    // Later appends fail immediately, even with the fault gone.
    let r = wal.append(TOPIC.into(), 0, record(0, 2), 1).await;
    assert!(matches!(r, Err(WalError::Poisoned(_))), "later append: {r:?}");
    let r = wal.append(TOPIC.into(), 0, record(0, 3), 0).await;
    assert!(matches!(r, Err(WalError::Poisoned(_))), "later acks=0 append: {r:?}");
    assert!(!callback_saw(&calls, 0, 2));

    // Another partition on the same WAL is unaffected.
    wal.append(TOPIC.into(), 1, record(1, 0), 1).await.expect("other partition still healthy");
    assert!(callback_saw(&calls, 1, 0));
    assert!(!wal.is_poisoned(TOPIC, 1));

    // The durable end did not advance past the healthy record.
    assert_eq!(wal.durable_end_offset(TOPIC, 0), Some(1));

    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enospc_partial_write_poisons_and_is_cut_back() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (wal, calls) = wal_with_callback(dir.path());

    wal.append(TOPIC.into(), 0, record(0, 0), 1).await.expect("healthy append");
    let durable = segment_len(dir.path(), 0);
    assert!(durable > 0);

    fail::cfg("wal::write", "return(enospc)").unwrap();
    let r = wal.append(TOPIC.into(), 0, record(0, 1), 1).await;
    fail::remove("wal::write");
    assert!(r.is_err(), "ENOSPC write must not be Ok: {r:?}");
    assert!(wal.is_poisoned(TOPIC, 0));
    assert!(!callback_saw(&calls, 0, 1));

    // The partial prefix was cut back to the last durable length (best effort).
    assert_eq!(segment_len(dir.path(), 0), durable, "partial write must be truncated away");

    let r = wal.append(TOPIC.into(), 0, record(0, 2), 1).await;
    assert!(matches!(r, Err(WalError::Poisoned(_))), "later append: {r:?}");

    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eio_write_poisons() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (wal, _calls) = wal_with_callback(dir.path());

    fail::cfg("wal::write", "return(eio)").unwrap();
    let r = wal.append(TOPIC.into(), 0, record(0, 0), 1).await;
    fail::remove("wal::write");
    assert!(r.is_err(), "EIO write must not be Ok: {r:?}");
    assert!(wal.is_poisoned(TOPIC, 0));
    assert_eq!(segment_len(dir.path(), 0), 0);

    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_writes_behind_a_failed_batch_all_fail() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let (wal, calls) = wal_with_callback(dir.path());
    let wal = Arc::new(wal);

    // Create the queue up front so all 50 race on the same worker.
    wal.append(TOPIC.into(), 0, record(0, 0), 1).await.expect("healthy append");

    // Slow the failing fsync down so later appends pile up behind it.
    fail::cfg("wal::fsync", "sleep(50)->return").unwrap();
    let mut handles = Vec::new();
    for i in 1..=50i64 {
        let wal = Arc::clone(&wal);
        handles.push(tokio::spawn(async move {
            wal.append(TOPIC.into(), 0, record(0, i), 1).await
        }));
    }
    let mut oks = 0;
    for h in handles {
        let r = tokio::time::timeout(std::time::Duration::from_secs(10), h)
            .await
            .expect("no waiter may hang after poisoning")
            .unwrap();
        if r.is_ok() {
            oks += 1;
        }
    }
    fail::remove("wal::fsync");

    assert_eq!(oks, 0, "no append may succeed once the partition failed");
    assert!(wal.is_poisoned(TOPIC, 0));
    for i in 1..=50 {
        assert!(!callback_saw(&calls, 0, i), "callback fired for failed offset {i}");
    }

    scenario.teardown();
}
