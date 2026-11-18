//! Crash loops (plan 02-15). All `#[ignore]`: they spawn processes and are slow. Run:
//!
//! ```text
//! OSTRO_CRASH_ITERS=20 cargo test --release -p ostro-crashtest --test crash_loop -- --ignored --nocapture
//! OSTRO_CRASH_ITERS=20 cargo test --release -p ostro-crashtest --features failpoints \
//!     --test crash_loop -- --ignored --nocapture crash_loop_io_errors
//! ```
//!
//! Every iteration runs `crash-target` against one data directory, kills it with SIGKILL
//! at a seeded random point, then checks the directory offline.

use std::path::Path;
use std::time::{Duration, Instant};

use ostro_crashtest::{
    Rng, TargetRun, crash_iters, open_node, rebuild_and_compare, run_and_kill, verify_acks,
};
use ostro_log::ReplicatedLog;
use ostro_partition::{Node, StartMode};
use ostro_types::PartitionId;

const P: u16 = 4;

fn bin() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_crash-target"))
}

fn leos(node: &Node) -> Vec<String> {
    (0..P)
        .map(|p| match node.handle(PartitionId(p)) {
            Some(h) => h.log().log_end().0.to_string(),
            None => "F".into(),
        })
        .collect()
}

/// Inspect-mode open of `dir` (timed), ACK + run verification, shutdown, then
/// rebuild-and-compare. Returns (acks verified, runs verified, reopen time).
async fn check_dir(dir: &Path, run: &TargetRun, i: u64) -> (usize, usize, Duration) {
    let t = Instant::now();
    let node = open_node(dir, P, StartMode::Inspect)
        .await
        .unwrap_or_else(|e| panic!("iter {i}: {e}"));
    let reopen = t.elapsed();
    let c = verify_acks(&node, &run.acks, &[])
        .await
        .unwrap_or_else(|e| panic!("iter {i}: {e}"));
    let leo = leos(&node);
    node.shutdown().await.expect("shutdown");
    rebuild_and_compare(dir, P)
        .await
        .unwrap_or_else(|e| panic!("iter {i}: rebuild mismatch: {e}"));
    println!(
        "iter {i}: acks {} runs {} failed {:?} leo [{}] startup {} ms reopen {} ms",
        c.acks,
        c.runs,
        run.failed,
        leo.join(","),
        run.startup.as_millis(),
        reopen.as_millis()
    );
    (c.acks, c.runs, reopen)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns crash-target processes; run with --ignored"]
async fn crash_loop_kill9() {
    let iters = crash_iters(20);
    let d = ostro_testkit::fs::tempdir();
    let dir = d.path().join("data");
    let mut rng = Rng::new(0x6b69_6c6c);
    let (mut acks, mut runs, mut max_reopen) = (0usize, 0usize, Duration::ZERO);
    let mut last = None;
    for i in 0..iters {
        let run_for = Duration::from_millis(rng.range(5, 300));
        let run = run_and_kill(bin(), &dir, i, P, &[], run_for)
            .unwrap_or_else(|e| panic!("iter {i}: {e}"));
        assert!(
            run.failed.is_empty(),
            "iter {i}: partitions failed: {}",
            run.stdout
        );
        let (a, r, reopen) = check_dir(&dir, &run, i).await;
        acks += a;
        runs += r;
        max_reopen = max_reopen.max(reopen);
        last = Some(run);
    }
    // negative control: the checker does catch a wrong hash and an offset past the log end
    if let Some(ack) = last.and_then(|r| r.acks.into_iter().next()) {
        let node = open_node(&dir, P, StartMode::Inspect).await.expect("open");
        let mut bad_hash = ack.clone();
        bad_hash.hash[0] ^= 1;
        assert!(verify_acks(&node, &[bad_hash], &[]).await.is_err());
        let mut lost = ack;
        lost.offset = u64::MAX / 2;
        let e = verify_acks(&node, &[lost], &[]).await.unwrap_err();
        assert!(e.contains("lost acked command"), "{e}");
        node.shutdown().await.expect("shutdown");
    }
    println!(
        "crash_loop_kill9: {iters} iterations, {acks} acks verified, {runs} runs verified, max reopen {} ms",
        max_reopen.as_millis()
    );
    assert!(acks > 0, "no command was ever acknowledged");
}
