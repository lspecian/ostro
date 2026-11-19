//! Plan 05-18 (QA-01): `meta::log_append` and `meta::fsync` failpoints in the vendored raft
//! log store (Ostro P2). Run with `--features failpoints -- --test-threads=1`: the fail
//! registry is process-global.
//!
//! For each site and each error (EIO by default, ENOSPC with `return(enospc)`):
//! - the failing proposal returns `MetaError::Propose` naming the storage error, and
//!   `MetaHandle::failure()` reports it (the driver stops, as on a real I/O error);
//! - a restart in the same directory recovers exactly the state acknowledged before the
//!   fault (the failed entry was cut back, never half-applied), and accepts new writes
//!   that survive a further restart.

use std::path::Path;

use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaRecord, MetaResult, QueueConfig};

// POSIX errno values, identical on Linux and macOS.
const EIO: i32 = 5;
const ENOSPC: i32 = 28;

fn queue(max: u32, at: u64) -> MetaRecord {
    MetaRecord::Queue(QueueConfig {
        name: "default".into(),
        max_concurrency: Some(max),
        created_at_ms: at,
        ..Default::default()
    })
}

fn put(record: MetaRecord, expected_version: Option<u64>) -> MetaCommand {
    MetaCommand::Put {
        key: record.key(),
        expected_version,
        record,
    }
}

async fn start(dir: &Path) -> MetaHandle {
    let h = MetaHandle::start(dir, 1).await.expect("meta starts");
    assert!(h.is_leader());
    assert_eq!(h.failure(), None);
    h
}

/// Configure `site` with `action`, check the next proposal fails naming `os error {errno}`,
/// then restart and check the recovered state.
async fn fault_then_restart(site: &str, action: &str, errno: i32) {
    let _scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let h = start(dir.path()).await;
    h.init_cluster("c1", 1, 4, 5, [7; 32]).await.expect("init");
    assert_eq!(
        h.propose(put(queue(8, 10), None)).await.unwrap(),
        MetaResult::Ok { version: 1 }
    );
    let before = h.view().read(|s| s.clone());

    fail::cfg(site, action).unwrap();
    let res = h.propose(put(queue(16, 11), Some(1))).await;
    fail::remove(site);
    let want = format!("os error {errno}");
    match &res {
        Err(MetaError::Propose(msg)) => {
            assert!(msg.contains(&want), "{site}={action}: {msg}");
            assert!(msg.contains("raft storage"), "{site}={action}: {msg}");
        }
        other => panic!("{site}={action}: expected a propose error, got {other:?}"),
    }
    let failure = h.failure().expect("driver reports the storage failure");
    assert!(failure.contains(&want), "{failure}");
    // The view never saw the failed command.
    assert_eq!(h.view().read(|s| s.clone()), before);
    h.stop().await;

    // Restart: the acknowledged state, exactly; the failed write is gone.
    let h = start(dir.path()).await;
    assert_eq!(
        h.view().read(|s| s.clone()),
        before,
        "{site}={action}: restart recovers the acknowledged state"
    );
    assert_eq!(h.view().read(|s| s.queue("default").unwrap().version), 1);
    // New writes work and are durable.
    assert_eq!(
        h.propose(put(queue(32, 12), Some(1))).await.unwrap(),
        MetaResult::Ok { version: 2 }
    );
    let after = h.view().read(|s| s.clone());
    h.stop().await;

    let h = start(dir.path()).await;
    assert_eq!(h.view().read(|s| s.clone()), after);
    assert_eq!(
        h.view().read(|s| s
            .queue("default")
            .map(|q| (q.version, q.value.max_concurrency))),
        Some((2, Some(32)))
    );
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn log_append_eio_stops_meta_and_restart_recovers() {
    fault_then_restart("meta::log_append", "return", EIO).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn log_append_enospc_torn_write_is_cut_back() {
    fault_then_restart("meta::log_append", "return(enospc)", ENOSPC).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fsync_eio_stops_meta_and_restart_recovers() {
    fault_then_restart("meta::fsync", "return", EIO).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fsync_enospc_stops_meta_and_restart_recovers() {
    fault_then_restart("meta::fsync", "return(enospc)", ENOSPC).await;
}

/// A failpoint that is configured but off (`off`) changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn off_failpoints_are_inert() {
    let _scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    fail::cfg("meta::log_append", "off").unwrap();
    fail::cfg("meta::fsync", "off").unwrap();
    let h = start(dir.path()).await;
    h.init_cluster("c1", 1, 4, 5, [7; 32]).await.expect("init");
    assert_eq!(
        h.propose(put(queue(8, 10), None)).await.unwrap(),
        MetaResult::Ok { version: 1 }
    );
    assert_eq!(h.failure(), None);
    h.stop().await;
}
