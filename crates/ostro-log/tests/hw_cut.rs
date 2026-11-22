//! BACK-01 / W7: `LogNode::cut()` is causally closed.
//!
//! **Assumption under test:** every HW goes through `HwVector::publish_hw`, which raises the
//! vector entry *before* publishing on the HW watch (update-before-publish). Then a task that
//! observes `HW_A >= x` through the watch and afterwards appends to B (reaching `HW_B >= y`)
//! can never produce a cut with `cut[B] >= y` and `cut[A] < x`.
//!
//! - `causal_closure`: 256 proptest cases on a real 4-partition `LogNode` while a thread takes
//!   cuts as fast as it can, with background appends on the other partitions.
//! - `negative_control_publish_first_breaks_closure`: the test-only publish-first order (with
//!   a delay on A's second step) must violate the property within 10,000 iterations.
//! - `hw_watch_written_only_in_publish_point`: a source grep that the HW watch is written only
//!   inside `publish_hw` (and `retract_hw`, the truncation path) in `cut.rs`, and that only
//!   the partition log's replica core calls `publish_hw` (12-08).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ostro_log::{HwVector, LogNode, LogNodeConfig, ReplicaSet, ReplicatedLog, Rf1Log};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

const P: u16 = 4;

async fn open(dir: &Path) -> (LogNode, Vec<Arc<Rf1Log>>) {
    let cfg = LogNodeConfig::new(dir.join("wal"), PartitionCount::new(P).unwrap(), 1);
    let floors = vec![Offset::ZERO; usize::from(P)];
    let (node, logs) = LogNode::open(cfg, &floors).await.unwrap();
    let logs: Vec<Arc<Rf1Log>> = logs.into_iter().map(Result::unwrap).collect();
    logs.iter().for_each(|l| lead(l));
    (node, logs)
}

/// Lead the way `Node` does at RF=1: epoch above the logged history, ISR {self}.
fn lead(log: &Rf1Log) {
    let e = log.latest_epoch().map_or(1, |e| e.0 + 1);
    log.become_leader(Epoch(e), ReplicaSet::solo(1)).unwrap();
}

fn cmds(tag: &str, n: usize) -> Vec<Bytes> {
    (0..n).map(|i| Bytes::from(format!("{tag}-{i}"))).collect()
}

/// Cuts taken continuously by a dedicated OS thread until `stop`.
fn spawn_cutter(
    v: Arc<HwVector>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<Vec<Vec<Offset>>> {
    std::thread::spawn(move || {
        let mut cuts = Vec::new();
        while !stop.load(Ordering::Relaxed) {
            cuts.push(v.cut());
            std::hint::spin_loop();
        }
        cuts.push(v.cut());
        cuts
    })
}

/// Append to A, wait until A's HW watch shows `x`, then append to B and wait for `y`.
async fn dependent_pair(
    logs: &[Arc<Rf1Log>],
    a: usize,
    b: usize,
    na: usize,
    nb: usize,
) -> (Offset, Offset) {
    let ra = logs[a].append(Epoch(1), cmds("a", na)).await.unwrap();
    let x = ra.last.next();
    let mut hw_a = logs[a].high_watermark();
    hw_a.wait_for(|h| *h >= x).await.unwrap();
    // Observed HW_A >= x: only now append to B.
    let rb = logs[b].append(Epoch(1), cmds("b", nb)).await.unwrap();
    let y = rb.last.next();
    logs[b].wait_committed(rb.last).await.unwrap();
    (x, y)
}

fn violations(cuts: &[Vec<Offset>], a: usize, b: usize, x: Offset, y: Offset) -> usize {
    cuts.iter().filter(|c| c[b] >= y && c[a] < x).count()
}

#[test]
fn causal_closure() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let tmp = ostro_testkit::fs::tempdir();
    let (node, logs) = rt.block_on(open(tmp.path()));
    let v = Arc::clone(node.hw_vector());

    let mut runner = TestRunner::new(Config {
        cases: 256,
        failure_persistence: None,
        ..Config::default()
    });
    let strategy = (
        0..P as usize,
        1..P as usize,
        1usize..4,
        1usize..4,
        0usize..3,
    );
    let total_cuts = std::cell::Cell::new(0usize);
    runner
        .run(&strategy, |(a, shift, na, nb, noise)| {
            let b = (a + shift) % P as usize;
            let stop = Arc::new(AtomicBool::new(false));
            let cutter = spawn_cutter(Arc::clone(&v), Arc::clone(&stop));
            let (x, y) = rt.block_on(async {
                // Background appends on every partition (including A and B) race the pair.
                let mut bg = Vec::new();
                for plog in &logs {
                    for k in 0..noise {
                        let log = Arc::clone(plog);
                        bg.push(tokio::spawn(async move {
                            let r = log
                                .append(Epoch(1), cmds(&format!("n{k}"), 1))
                                .await
                                .unwrap();
                            log.wait_committed(r.last).await.unwrap();
                        }));
                    }
                }
                let pair = dependent_pair(&logs, a, b, na, nb).await;
                for h in bg {
                    h.await.unwrap();
                }
                pair
            });
            stop.store(true, Ordering::Relaxed);
            let cuts = cutter.join().unwrap();
            total_cuts.set(total_cuts.get() + cuts.len());
            // The final cut always contains both.
            let last = cuts.last().unwrap();
            prop_assert!(
                last[a] >= x && last[b] >= y,
                "final cut {last:?} misses x={x} y={y}"
            );
            let bad = violations(&cuts, a, b, x, y);
            if bad > 0 {
                return Err(TestCaseError::fail(format!(
                    "{bad} of {} cuts had cut[{b}] >= {y} but cut[{a}] < {x}",
                    cuts.len()
                )));
            }
            Ok(())
        })
        .unwrap();
    eprintln!(
        "causal_closure: 256 cases, {} cuts checked",
        total_cuts.get()
    );
    rt.block_on(node.shutdown());
}

#[test]
fn negative_control_publish_first_breaks_closure() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let tmp = ostro_testkit::fs::tempdir();
    let (node, logs) = rt.block_on(open(tmp.path()));
    let v = Arc::clone(node.hw_vector());
    let (a, b) = (0usize, 1usize);
    // Publish first everywhere; A sleeps 50 ms between its watch publication and its vector
    // update, long enough for B's append + fsync to complete inside the window.
    v.set_publish_first_for_test(Some((PartitionId(0), Duration::from_millis(50))));

    let mut found = None;
    for iter in 0..10_000u32 {
        let stop = Arc::new(AtomicBool::new(false));
        let cutter = spawn_cutter(Arc::clone(&v), Arc::clone(&stop));
        let (x, y) = rt.block_on(dependent_pair(&logs, a, b, 1, 1));
        stop.store(true, Ordering::Relaxed);
        let cuts = cutter.join().unwrap();
        let bad = violations(&cuts, a, b, x, y);
        if bad > 0 {
            found = Some((iter + 1, bad, cuts.len()));
            break;
        }
    }
    v.set_publish_first_for_test(None);
    let (iters, bad, cuts) = found.expect("publish-first order never violated causal closure");
    eprintln!(
        "negative control: violated after {iters} iteration(s): {bad} of {cuts} cuts had \
         cut[B] >= y with cut[A] < x"
    );
    rt.block_on(node.shutdown());
}

#[test]
fn hw_watch_written_only_in_publish_point() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    // Every source file, including module directories (Phase 12 `replica/`).
    let mut files = Vec::new();
    let mut dirs = vec![src.clone()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                files.push(path);
            }
        }
    }
    for path in files {
        let name = path
            .strip_prefix(&src)
            .unwrap()
            .to_string_lossy()
            .to_string();
        let text = std::fs::read_to_string(&path).unwrap();
        if name == "cut.rs" {
            continue;
        }
        assert!(
            !text.contains("send_if_modified"),
            "{name} writes a watch with send_if_modified; HW publication belongs in cut.rs"
        );
    }
    let cut = std::fs::read_to_string(src.join("cut.rs")).unwrap();
    let code = cut.split("#[cfg(test)]").next().unwrap();
    // Split the non-test code at every fn and check which fns write the watch.
    let mut writers = Vec::new();
    for chunk in code.split("fn ").skip(1) {
        if chunk.contains("send_if_modified") {
            let fname: String = chunk
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            writers.push(fname);
        }
    }
    assert_eq!(
        writers,
        vec!["publish_hw", "retract_hw"],
        "HW watch writers: {writers:?}"
    );
    // 12-08: the commit callback reports the durable end to the partition's replica core,
    // and the core's `publish` is the only caller of `publish_hw` (plus the `rf1_bypass`
    // mutant site, which exists to prove that a shortcut is caught).
    let node = std::fs::read_to_string(src.join("node.rs")).unwrap();
    assert!(
        node.contains("core.on_durable(") && !node.contains("publish_hw("),
        "the commit callback must report to the replica core, never publish directly"
    );
    let log = std::fs::read_to_string(src.join("partition_log.rs")).unwrap();
    let code = log.split("#[cfg(test)]").next().unwrap();
    let mut callers = Vec::new();
    for chunk in code.split("fn ").skip(1) {
        if chunk.contains("hw_vector.publish_hw(") {
            let fname: String = chunk
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            callers.push(fname);
        }
    }
    assert_eq!(
        callers,
        vec!["on_durable", "publish"],
        "publish_hw callers: {callers:?}"
    );
    assert_eq!(code.matches("hw_vector.publish_hw(").count(), 2);
    assert!(code.contains("mutants::RF1_BYPASS"));
}

/// Reports the per-publication overhead the cut adds (one uncontended lock + max): not an
/// assertion, a measurement for the summary.
#[test]
fn publish_hw_cost_report() {
    let ids: Vec<Option<Offset>> = (0..P).map(|_| Some(Offset::ZERO)).collect();
    let v = HwVector::new(&ids);
    const N: u64 = 1_000_000;
    let t = Instant::now();
    for i in 1..=N {
        v.publish_hw(PartitionId((i % u64::from(P)) as u16), Offset(i));
    }
    let per_publish = t.elapsed().as_nanos() as f64 / N as f64;
    let t = Instant::now();
    for _ in 0..N {
        std::hint::black_box(v.cut());
    }
    let per_cut = t.elapsed().as_nanos() as f64 / N as f64;
    // Baseline: the bare watch publication the commit callback did before the cut existed.
    let watches: Vec<_> = (0..P)
        .map(|_| tokio::sync::watch::channel(Offset::ZERO).0)
        .collect();
    let t = Instant::now();
    for i in 1..=N {
        let hw = Offset(i);
        watches[(i % u64::from(P)) as usize].send_if_modified(|cur| {
            if hw > *cur {
                *cur = hw;
                true
            } else {
                false
            }
        });
    }
    let per_bare = t.elapsed().as_nanos() as f64 / N as f64;
    eprintln!(
        "publish_hw: {per_publish:.1} ns/publish (vector lock + watch) vs bare watch \
         {per_bare:.1} ns: overhead {:.1} ns per group commit; cut(): {per_cut:.1} ns",
        per_publish - per_bare
    );
}
