//! The apply thread must not panic when the runtime it was spawned with shuts down underneath
//! it (debug session `apply-thread-panic`).
//!
//! A dirty machine (applied past its last checkpoint) idles in `Machine::wait` with the
//! checkpoint deadline armed. That deadline used to be a tokio timer on the server runtime,
//! so `Runtime::shutdown_background` (the test harness's `crash()`, or any runtime drop while
//! the thread is alive) fired it and `Sleep::poll` panicked with "A Tokio 1.x context was
//! found, but it is being shutdown": the machine went `Failed("apply thread panicked")`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::{T, cfg, new_run, open_single, start_cmd};
use ostro_partition::{MachineConfig, PartitionHealth};

#[test]
fn dirty_machine_survives_runtime_shutdown_background() {
    let tmp = ostro_testkit::fs::tempdir();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    // Long interval, no batch trigger: after one applied command the machine is dirty and
    // waits on the checkpoint deadline.
    let c = MachineConfig {
        checkpoint_batches: u32::MAX,
        checkpoint_every: Duration::from_secs(3600),
        ..cfg()
    };
    let tp = rt.block_on(async {
        let tp = open_single(tmp.path(), c).await;
        tp.handle.submit_one(start_cmd(&new_run())).await.unwrap();
        tp
    });
    assert!(tp.applied() > tp.durable(), "machine must be dirty");
    let mut health = tp.handle.machine().health();
    assert_eq!(*health.borrow(), PartitionHealth::Healthy);

    // Kill the runtime under the live apply thread (what `TestServer::crash()` does).
    rt.shutdown_background();
    // The time driver is shut down by the runtime's worker threads in the background; give
    // the apply thread ample time to observe it.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        *health.borrow_and_update(),
        PartitionHealth::Healthy,
        "runtime shutdown failed the apply machine"
    );

    // Dropping every handle stops the thread gracefully (drain, final checkpoint), without a
    // panic. Drop on a fresh runtime, as `crash()` does.
    let drop_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("drop runtime");
    drop_rt.block_on(async move { drop(tp) });
    // The health sender is dropped when the apply thread exits.
    let t0 = Instant::now();
    while health.has_changed().is_ok() {
        assert!(t0.elapsed() < T, "apply thread did not exit");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !matches!(*health.borrow(), PartitionHealth::Failed { .. }),
        "apply thread failed on stop: {:?}",
        *health.borrow()
    );
}
