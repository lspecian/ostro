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
    Rng, TargetRun, crash_iters, non_empty_segments, open_node, rebuild_and_compare, record_spans,
    run_and_kill, scan_log, verify_acks,
};
use ostro_log::ReplicatedLog;
use ostro_log::ostro_wal::truncate::RECORD_HEADER_LEN;
use ostro_partition::{ApplyOutcome, Node, PartitionSlot, StartMode, StartupEvent};
use ostro_testkit::corrupt;
use ostro_types::command::{ConflictPolicy, StartRun};
use ostro_types::{Command, PartitionId, RunId};

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

/// The bytes of an un-acknowledged, partially written batch at the tail of `seg`:
/// - zeros (an unflushed extent reads back as zeros), 1..400 bytes;
/// - a genuine record header followed by random bytes, cut short of the declared length
///   (a partial record);
/// - random bytes shorter than a record header (a partial header).
///
/// Arbitrary random bytes that form a complete-looking but invalid record are *not* a torn
/// write: recovery rightly treats them as corruption, so they are not generated here.
fn torn_bytes(rng: &mut Rng, seg: &Path) -> (&'static str, Vec<u8>) {
    match rng.below(3) {
        0 => ("zeros", vec![0u8; rng.range(1, 401) as usize]),
        1 => {
            let buf = std::fs::read(seg).expect("read segment");
            let spans = record_spans(seg).expect("spans");
            let (start, len) = spans[rng.below(spans.len() as u64) as usize];
            let max = (len - 1).min(400);
            let k = rng.range(1, max + 1) as usize;
            let mut b = buf[start as usize..start as usize + k.min(RECORD_HEADER_LEN)].to_vec();
            if k > RECORD_HEADER_LEN {
                b.extend(rng.bytes(k - RECORD_HEADER_LEN));
            }
            ("partial record", b)
        }
        _ => {
            let k = rng.range(1, RECORD_HEADER_LEN as u64) as usize;
            ("partial header", rng.bytes(k))
        }
    }
}

fn start_cmd(p: u16) -> Command {
    let id = RunId::new_v7(PartitionId(p));
    Command::start_run(StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: None,
        idempotency_key: vec![],
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: String::new(),
        priority: 0,
        ready_at_ms: 0,
        requested_at_ms: 0,
        parent: None,
        rng_seed: vec![7; 32],
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns crash-target processes; run with --ignored"]
async fn crash_loop_torn_tail() {
    let iters = crash_iters(20);
    let d = ostro_testkit::fs::tempdir();
    let dir = d.path().join("data");
    let mut rng = Rng::new(0x746f_726e);
    let mut acks = 0usize;
    for i in 0..iters {
        let run_for = Duration::from_millis(rng.range(5, 300));
        let run = run_and_kill(bin(), &dir, 1000 + i, P, &[], run_for)
            .unwrap_or_else(|e| panic!("iter {i}: {e}"));
        assert!(run.failed.is_empty(), "iter {i}: {}", run.stdout);

        // a partial batch at the tail of a random partition's last non-empty segment
        let p = rng.below(u64::from(P)) as u16;
        let segs = non_empty_segments(&dir, p).expect("segments");
        let last = segs.last().expect("non-empty segment").clone();
        let before = std::fs::metadata(&last).expect("stat").len();
        let (kind, bytes) = torn_bytes(&mut rng, &last);
        corrupt::append_garbage(&last, &bytes).expect("append");

        let node = open_node(&dir, P, StartMode::Leader)
            .await
            .unwrap_or_else(|e| panic!("iter {i}: {e}"));
        let ev = node.startup_events();
        let leo = ev
            .iter()
            .find_map(|e| match e {
                StartupEvent::LogRecovered { p: q, leo, torn } if *q == p => {
                    assert!(
                        *torn,
                        "iter {i}: {kind} tail on p{p} not reported torn: {ev:?}"
                    );
                    Some(*leo)
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("iter {i}: no LogRecovered for p{p}: {ev:?}"));
        assert!(
            matches!(node.slots()[usize::from(p)], PartitionSlot::Ready(_)),
            "iter {i}: {}",
            ostro_crashtest::failed_reason(&node, p)
        );
        assert_eq!(
            std::fs::metadata(&last).expect("stat").len(),
            before,
            "iter {i}: cut must restore the pre-garbage length"
        );
        let c = verify_acks(&node, &run.acks, &[])
            .await
            .unwrap_or_else(|e| panic!("iter {i}: {e}"));
        acks += c.acks;
        // the new term starts right at the cut, and new submits append contiguously
        assert!(
            ev.iter().any(|e| matches!(
                e,
                StartupEvent::TermStarted { p: q, offset, .. } if *q == p && *offset == leo
            )),
            "iter {i}: term on p{p} does not start at the recovered end {leo}: {ev:?}"
        );
        let h = node.handle(PartitionId(p)).expect("ready");
        let end = h.log().log_end();
        let out = h
            .submit_with_offsets(vec![start_cmd(p), start_cmd(p)])
            .await
            .expect("submit after cut");
        assert!(out.iter().all(|(_, o)| *o == ApplyOutcome::Accepted));
        assert!(out[0].0 >= end && out[1].0.0 == out[0].0.0 + 1);
        scan_log(h.log().as_ref())
            .await
            .unwrap_or_else(|e| panic!("iter {i}: {e}"));
        node.shutdown().await.expect("shutdown");
        rebuild_and_compare(&dir, P)
            .await
            .unwrap_or_else(|e| panic!("iter {i}: rebuild mismatch: {e}"));
        println!(
            "iter {i}: {kind} ({} bytes) on p{p}, cut at leo {leo}, acks {}",
            bytes.len(),
            c.acks
        );
    }
    println!("crash_loop_torn_tail: {iters} iterations, {acks} acks verified");
    assert!(acks > 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns crash-target processes; run with --ignored"]
async fn crash_loop_corruption_detected() {
    let iters = crash_iters(20);
    let d = ostro_testkit::fs::tempdir();
    let dir = d.path().join("data");
    let mut rng = Rng::new(0x666c_6970);
    let mut checked = 0u64;
    for i in 0..iters {
        let run_for = Duration::from_millis(rng.range(5, 300));
        let run = run_and_kill(bin(), &dir, 2000 + i, P, &[], run_for)
            .unwrap_or_else(|e| panic!("iter {i}: {e}"));
        assert!(run.failed.is_empty(), "iter {i}: {}", run.stdout);

        // a bit flip inside a random record of a SEALED segment, in a copy
        let copy = ostro_testkit::fs::tempdir();
        let cdir = copy.path().join("data");
        ostro_crashtest::copy_dir(&dir, &cdir).expect("copy");
        let p = rng.below(u64::from(P)) as u16;
        let segs = non_empty_segments(&cdir, p).expect("segments");
        if segs.len() >= 2 {
            let seg = segs[rng.below(segs.len() as u64 - 1) as usize].clone();
            let spans = record_spans(&seg).expect("spans");
            let (start, len) = spans[rng.below(spans.len() as u64) as usize];
            let byte = start + rng.below(len);
            let bit = rng.below(8) as u8;
            corrupt::flip_bit(&seg, byte, bit).expect("flip");

            let node = open_node(&cdir, P, StartMode::Inspect)
                .await
                .unwrap_or_else(|e| panic!("iter {i}: {e}"));
            let name = seg.file_name().expect("name").to_string_lossy().to_string();
            for (q, slot) in node.slots().iter().enumerate() {
                match slot {
                    PartitionSlot::Failed { reason, .. } if q == usize::from(p) => {
                        assert!(
                            reason.contains(&name) && reason.contains(&format!("at byte {start}")),
                            "iter {i}: reason {reason:?} should name {name} at byte {start}"
                        );
                    }
                    PartitionSlot::Ready(_) if q != usize::from(p) => {}
                    other => panic!(
                        "iter {i}: flip p{p} {name} byte {byte} (record at {start}, len {len}) \
                         bit {bit}: slot {q} is {other:?}"
                    ),
                }
            }
            node.shutdown().await.expect("shutdown");
            checked += 1;
            println!(
                "iter {i}: flipped bit {bit} of byte {byte} (record at {start}, len {len}) in {name} (p{p}): p{p} failed loudly, others ready"
            );
        } else {
            println!("iter {i}: p{p} has no sealed segment yet; corruption leg skipped");
        }
        drop(copy);
        // the original is untouched and still opens cleanly
        check_dir(&dir, &run, i).await;
    }
    println!("crash_loop_corruption_detected: {iters} iterations, {checked} corruptions detected");
    assert!(
        checked > 0 || iters < 2,
        "no sealed segment was ever corrupted"
    );
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns crash-target processes; run with --ignored --features failpoints"]
async fn crash_loop_io_errors() {
    let iters = crash_iters(20);
    let d = ostro_testkit::fs::tempdir();
    let dir = d.path().join("data");
    let mut rng = Rng::new(0x6965_7272);
    let env = [("FAILPOINTS", "wal::fsync=1%return;state::commit=1%return")];
    let (mut acks, mut failed) = (0usize, 0usize);
    for i in 0..iters {
        let run_for = Duration::from_millis(rng.range(5, 300));
        let run = run_and_kill(bin(), &dir, 3000 + i, P, &env, run_for)
            .unwrap_or_else(|e| panic!("iter {i}: {e}"));
        // reopen without failpoints: every ACK survived, live == rebuilt
        let (a, _, _) = check_dir(&dir, &run, i).await;
        acks += a;
        failed += run.failed.len();
    }
    println!(
        "crash_loop_io_errors: {iters} iterations, {acks} acks verified, {failed} partition failures injected"
    );
    assert!(failed > 0, "no injected I/O error ever failed a partition");
    assert!(acks > 0);
}
