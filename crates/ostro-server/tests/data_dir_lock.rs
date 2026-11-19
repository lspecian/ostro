//! Exclusive data-dir lock (plan 05-26): `Server::start` takes `flock(LOCK_EX|LOCK_NB)`
//! on `<data>/LOCK` before any other disk work, so a second server on the same directory
//! fails fast with [`ServerError::DataDirInUse`]; the lock is released on shutdown and a
//! leftover `LOCK` file never blocks a start. The SIGKILL case (the OS drops the lock
//! with the dead process) runs against the spawned binary in `crates/ostro/tests`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::fs::{File, TryLockError};
use std::time::Duration;

use common::{ServerOpts, TestServer, server_config, tmpfs_dir};
use ostro_server::{LOCK_FILE, Server, ServerError};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_start_on_same_dir_fails_then_works_after_shutdown() {
    let dir = tmpfs_dir();
    let cfg = server_config(dir.path(), &ServerOpts::default());
    let mut first = TestServer::start_cfg(cfg.clone(), None).await;
    first.wait_ready(Duration::from_secs(5)).await;
    let lock = dir.path().join(LOCK_FILE);
    assert!(lock.is_file(), "{} created", lock.display());

    let err = Server::start(cfg.clone())
        .await
        .expect_err("a second server on a locked data dir must fail");
    let msg = err.to_string();
    assert!(
        matches!(&err, ServerError::DataDirInUse { path, lock: l } if path == dir.path() && *l == lock),
        "{err:?}"
    );
    assert_eq!(
        msg,
        format!(
            "data dir {} is in use by another ostro process (lock {})",
            dir.path().display(),
            lock.display()
        )
    );
    assert!(!msg.contains('\n'), "one line: {msg}");

    // The running server is unaffected by the failed attempt.
    first.wait_ready(Duration::from_secs(1)).await;
    let r = first
        .http
        .get(first.url("/healthz"))
        .send()
        .await
        .expect("healthz");
    assert_eq!(r.status(), reqwest::StatusCode::OK);

    // Graceful shutdown releases the lock: the same directory starts again.
    first.shutdown().await;
    assert!(
        lock.is_file(),
        "the LOCK file stays; only the flock is released"
    );
    let mut again = TestServer::start_cfg(cfg, None).await;
    again.wait_ready(Duration::from_secs(5)).await;
    again.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_lock_holder_blocks_start_and_stale_lock_file_does_not() {
    let dir = tmpfs_dir();
    let cfg = server_config(dir.path(), &ServerOpts::default());
    let lock = dir.path().join(LOCK_FILE);

    // A stale LOCK file (left by a crashed process, nobody holds the flock) is no obstacle.
    std::fs::write(&lock, b"stale").expect("write stale LOCK");
    let mut s = TestServer::start_cfg(cfg.clone(), None).await;
    s.wait_ready(Duration::from_secs(5)).await;
    // While the server runs, any other descriptor fails to take the lock.
    let probe = File::open(&lock).expect("open LOCK");
    assert!(
        matches!(probe.try_lock(), Err(TryLockError::WouldBlock)),
        "the server holds the exclusive lock"
    );
    s.shutdown().await;
    probe.try_lock().expect("released after shutdown");

    // Someone else holding the lock: start fails before creating anything.
    let err = Server::start(cfg.clone())
        .await
        .expect_err("locked by another holder");
    assert!(matches!(err, ServerError::DataDirInUse { .. }), "{err:?}");
    drop(probe);
    let mut s = TestServer::start_cfg(cfg, None).await;
    s.wait_ready(Duration::from_secs(5)).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_dir_lock_is_taken_before_any_other_disk_work() {
    let dir = tmpfs_dir();
    let cfg = server_config(dir.path(), &ServerOpts::default());
    let lock = dir.path().join(LOCK_FILE);
    let holder = File::create(&lock).expect("create LOCK");
    holder.try_lock().expect("take the lock first");

    let err = Server::start(cfg).await.expect_err("locked");
    assert!(matches!(err, ServerError::DataDirInUse { .. }), "{err:?}");
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .expect("ls")
        .map(|e| e.expect("entry").file_name())
        .collect();
    assert_eq!(
        entries,
        vec![std::ffi::OsString::from(LOCK_FILE)],
        "no meta/wal/state/cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ephemeral_server_locks_its_temp_dir() {
    let unused = tmpfs_dir();
    let opts = ServerOpts {
        ephemeral: true,
        ..ServerOpts::default()
    };
    let mut s = TestServer::start_cfg(server_config(unused.path(), &opts), None).await;
    s.wait_ready(Duration::from_secs(5)).await;
    let probe = File::open(s.h().data_dir().join(LOCK_FILE)).expect("open LOCK");
    assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
    s.shutdown().await;
}
