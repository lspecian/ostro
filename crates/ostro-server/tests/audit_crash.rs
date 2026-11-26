//! Audit journal crash test (D-P14-11, 14-04): a child process (this test binary re-executed)
//! commits records through the writer from 8 concurrent committers and prints `ACK <iter>
//! <n>` once each commit resolved; the parent `kill -9`s it at a random moment, reopens the
//! journal (torn-tail recovery), verifies the chain and checks that every acknowledged record
//! is present. `OSTRO_AUDIT_CRASH_ITERS` iterations (default 50).
//!
//! ```text
//! OSTRO_AUDIT_CRASH_ITERS=50 cargo test --release -p ostro-server --features failpoints \
//!     --locked --test audit_crash -- --ignored --test-threads=1 --nocapture
//! ```

use std::collections::{BTreeSet, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use ostro_partition::Clock;
use ostro_partition::clock::SystemClock;
use ostro_server::audit::journal::{self, Record};
use ostro_server::audit::segment;
use ostro_server::audit::verify::verify_dir;
use ostro_server::audit::writer::{AuditWriter, WriterConfig};
use ostro_types::audit::{ActorKind, AuditEvent, AuditSink};

const TEST: &str = "kill9_mid_commit_loses_no_acknowledged_record";
const CHILD_DIR: &str = "OSTRO_AUDIT_CRASH_CHILD_DIR";
const CHILD_ITER: &str = "OSTRO_AUDIT_CRASH_CHILD_ITER";

fn writer_cfg(dir: &Path, incarnation: u64) -> WriterConfig {
    let mut c = WriterConfig::new(dir, 1, incarnation);
    c.group_commit = Duration::from_millis(2);
    // Small segments so kills also land around rotations.
    c.segment_bytes = 64 * 1024;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    c.clock = Some(clock);
    c
}

/// The child: commit forever, acknowledging each durable record on stdout.
fn child(dir: &Path, iter: u64) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async move {
        let w = AuditWriter::open(writer_cfg(dir, 1_000 + iter)).expect("child open");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        for c in 0..8u64 {
            let w = Arc::clone(&w);
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut n = c;
                loop {
                    let target = format!("it{iter}-{n}");
                    let permit = w.reserve().expect("reserve");
                    let ev = AuditEvent::new(
                        "run.cancel",
                        "otk_crash",
                        ActorKind::Token,
                        target.clone(),
                    );
                    permit.commit(ev).await.expect("commit");
                    if tx.send(target).is_err() {
                        return;
                    }
                    n += 8;
                }
            });
        }
        drop(tx);
        let mut out = std::io::stdout().lock();
        while let Some(t) = rx.recv().await {
            writeln!(out, "ACK {t}").expect("stdout");
            out.flush().expect("flush");
        }
    });
}

/// A tiny deterministic generator (no rand dependency).
fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state >> 33
}

fn targets(dir: &Path) -> HashSet<String> {
    let mut out = HashSet::new();
    for (_, p) in segment::list_segments(dir).expect("list") {
        let bytes = std::fs::read(&p).expect("read");
        for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let r = Record::parse_line(line).expect("valid line after recovery");
            out.insert(r.target);
        }
    }
    out
}

#[test]
#[ignore = "crash test: release build, spawns and kills child processes"]
fn kill9_mid_commit_loses_no_acknowledged_record() {
    if let Ok(dir) = std::env::var(CHILD_DIR) {
        let iter: u64 = std::env::var(CHILD_ITER)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        child(Path::new(&dir), iter);
        return;
    }
    let iters: u64 = std::env::var("OSTRO_AUDIT_CRASH_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let dir = tempfile::Builder::new()
        .prefix("ostro-audit-crash-")
        .tempdir()
        .expect("tempdir");
    let exe = std::env::current_exe().expect("current_exe");
    let mut seed = 0x0057_7240_u64;
    let mut total_acked = 0u64;
    let mut recovered = 0u64;
    let mut truncated_bytes = 0u64;
    for iter in 0..iters {
        let mut child = Command::new(&exe)
            .args([
                "--exact",
                TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_DIR, dir.path())
            .env(CHILD_ITER, iter.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");
        let stdout = child.stdout.take().expect("stdout");
        let reader = std::thread::spawn(move || {
            let mut acked = BTreeSet::new();
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(t) = line.strip_prefix("ACK ") {
                    acked.insert(t.to_owned());
                }
            }
            acked
        });
        let wait_ms = 50 + lcg(&mut seed) % 400;
        std::thread::sleep(Duration::from_millis(wait_ms));
        child.kill().expect("kill -9"); // SIGKILL on Unix
        let _ = child.wait();
        let acked = reader.join().expect("reader");
        total_acked += acked.len() as u64;

        // Restart: recovery truncates a torn tail; the chain verifies; nothing acked is lost.
        let w = AuditWriter::open(writer_cfg(dir.path(), 500_000 + iter)).expect("reopen");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(w.flush());
        w.close();
        drop(w);
        let report = verify_dir(dir.path())
            .unwrap_or_else(|e| panic!("iteration {iter}: verify failed: {e}"));
        let present = targets(dir.path());
        let lost: Vec<&String> = acked.iter().filter(|t| !present.contains(*t)).collect();
        assert!(
            lost.is_empty(),
            "iteration {iter}: {} acknowledged records lost, e.g. {:?}",
            lost.len(),
            &lost[..lost.len().min(5)]
        );
        println!(
            "iter {iter:>3}: kill after {wait_ms:>3} ms, {:>5} acked, journal {} records / {} segments",
            acked.len(),
            report.records,
            report.segments
        );
    }
    for (_, p) in segment::list_segments(dir.path()).expect("list") {
        let bytes = std::fs::read(&p).expect("read");
        for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let r = Record::parse_line(line).expect("valid");
            if r.action == journal::AUDIT_RECOVERED {
                recovered += 1;
                truncated_bytes += r
                    .detail(journal::D_TRUNCATED_BYTES)
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
            }
        }
    }
    let report = verify_dir(dir.path()).expect("final verify");
    println!(
        "audit crash test: {iters} iterations, {total_acked} acknowledged records, all present; \
         {recovered} torn-tail recoveries ({truncated_bytes} bytes truncated); final journal \
         {} records in {} segments, seq {}..{}",
        report.records, report.segments, report.first_seq, report.last_seq
    );
}
