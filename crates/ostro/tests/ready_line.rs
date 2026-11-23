//! 10-17: the ready line is a frozen contract (D-P10-9, amended).
//!
//! - `ostro dev --ephemeral` prints exactly one stdout line matching
//!   [`READY_LINE_PATTERN`], with and without `--time-skipping` and `--watch <project>`.
//!   Logs go to stderr: the ready line is the first stdout line
//!   (`dev_stdout_has_no_log_lines_before_the_ready_line`); kits still scan every line.
//! - The four test kits carry the same pattern literal: this test reads them from
//!   `crates/ostro-client/src/testing.rs`, `clients/python/ostro_client/testing.py`,
//!   `clients/ts/src/testing.js` and `clients/go/ostrotest/ostrotest.go`.
//!
//! The `--watch` case needs the system Rust toolchain with wasm32-wasip2 (as
//! `dev_watch.rs`). Run with `--test-threads=1`.

#[path = "common/p10.rs"]
mod p10;

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ostro_client::testing::{READY_LINE_PATTERN, ReadyLine};

/// The pattern as frozen by this plan. Changing it breaks every released kit.
const FROZEN: &str =
    r"^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$";

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn every_kit_carries_the_frozen_pattern() {
    assert_eq!(READY_LINE_PATTERN, FROZEN);
    for rel in [
        "crates/ostro-client/src/testing.rs",
        "clients/python/ostro_client/testing.py",
        "clients/ts/src/testing.js",
        "clients/go/ostrotest/ostrotest.go",
    ] {
        let path = repo().join(rel);
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("kit file {} is missing: {e}", path.display()));
        assert!(
            src.contains(FROZEN),
            "{rel} does not contain the frozen ready-line pattern {FROZEN}"
        );
    }
}

/// Spawn `ostro dev --ephemeral --listen 127.0.0.1:0 <args>` and return every stdout line
/// up to and including the ready line, the parsed ready line, and the child.
fn spawn(
    args: &[&str],
    env: &[(&str, &str)],
) -> (
    Vec<String>,
    ReadyLine,
    std::process::Child,
    mpsc::Receiver<String>,
    tempfile::TempDir,
) {
    let cwd = tempfile::tempdir().unwrap();
    let mut child = p10::ostro_cmd(env)
        .current_dir(cwd.path())
        .args(["dev", "--ephemeral", "--listen", "127.0.0.1:0"])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ostro dev");
    let (tx, rx) = mpsc::channel();
    let out = child.stdout.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let (etx, erx) = mpsc::channel();
    let err = child.stderr.take().unwrap();
    std::thread::spawn(move || {
        for line in BufReader::new(err).lines() {
            let Ok(line) = line else { break };
            if etx.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + p10::READY_TIMEOUT;
    let mut seen = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left.max(Duration::from_millis(1))) {
            Ok(line) => {
                let parsed = ReadyLine::parse(&line);
                seen.push(line);
                if let Some(r) = parsed {
                    return (seen, r, child, erx, cwd);
                }
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                let stderr: Vec<String> = erx.try_iter().collect();
                panic!("no ready line ({e}); stderr:\n{}", stderr.join("\n"));
            }
        }
    }
}

/// The ready line checked against the frozen format, field by field and through the
/// independent `common/p10.rs` parser.
fn check(lines: &[String], r: &ReadyLine) {
    let line = lines.last().unwrap();
    for earlier in &lines[..lines.len() - 1] {
        assert!(
            !earlier.contains(" listening on http://"),
            "a listening line before the ready line: {earlier}"
        );
    }
    assert_eq!(r.name, "ostro dev");
    assert!(r.ephemeral, "group 4 is `, ephemeral`: {line}");
    assert!(r.addr.starts_with("127.0.0.1:"), "{line}");
    assert!(r.addr.parse::<std::net::SocketAddr>().is_ok(), "{line}");
    assert!(r.data_dir.is_dir(), "{line}");
    let rebuilt = format!(
        "{} listening on http://{} (data: {}, ephemeral) ready in {} ms",
        r.name,
        r.addr,
        r.data_dir.display(),
        r.ms
    );
    assert_eq!(
        &rebuilt, line,
        "the line has nothing outside the five groups"
    );
    let p = p10::parse_ready_line(line).expect("common/p10.rs parser agrees");
    assert_eq!(
        (p.name.as_str(), p.addr.as_str(), p.ephemeral, p.ms),
        (r.name.as_str(), r.addr.as_str(), r.ephemeral, r.ms)
    );
    assert_eq!(Path::new(&p.data), r.data_dir.as_path());
}

fn kill(mut child: std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn dev_ephemeral_prints_the_frozen_line() {
    let (lines, r, child, _err, _cwd) = spawn(&[], &[]);
    check(&lines, &r);
    kill(child);
}

#[test]
fn dev_ephemeral_time_skipping_prints_the_frozen_line() {
    let (lines, r, child, _err, _cwd) = spawn(&["--time-skipping"], &[]);
    check(&lines, &r);
    let (st, body) = p10::http_get(r.addr.parse().unwrap(), "/v1/dev/time");
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("\"mode\":\"auto\""), "{body}");
    kill(child);
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_name() == "target" {
            continue;
        }
        let (src, dst) = (e.path(), to.join(e.file_name()));
        if e.file_type().unwrap().is_dir() {
            copy_tree(&src, &dst);
        } else {
            std::fs::copy(&src, &dst).unwrap();
        }
    }
}

#[test]
fn dev_ephemeral_watch_prints_the_frozen_line() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("p");
    copy_tree(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../ostro-build/tests/fixtures/rust-offline"),
        &p,
    );
    let (lines, r, child, err, _cwd) = spawn(
        &["--watch", p.to_str().unwrap()],
        &[("CARGO_NET_OFFLINE", "true")],
    );
    check(&lines, &r);
    // Let the initial build finish so no cargo outlives the test.
    let deadline = Instant::now() + Duration::from_secs(300);
    let mut log = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match err.recv_timeout(left.max(Duration::from_millis(1))) {
            Ok(l) => {
                let done = l.starts_with("ostro dev: deployed") || l.contains("failed");
                log.push(l);
                if done {
                    break;
                }
            }
            Err(e) => panic!(
                "watch did not finish its first build ({e}):\n{}",
                log.join("\n")
            ),
        }
    }
    assert!(
        log.iter().any(|l| l.starts_with("ostro dev: deployed")),
        "{}",
        log.join("\n")
    );
    kill(child);
}

/// Logs go to stderr: at `RUST_LOG=info` (≈1600 startup lines) the ready line is still
/// the first and only stdout line before readiness, and the log lines are on stderr.
#[test]
fn dev_stdout_has_no_log_lines_before_the_ready_line() {
    let (lines, r, child, err, _cwd) = spawn(&[], &[("RUST_LOG", "info"), ("NO_COLOR", "1")]);
    assert_eq!(
        lines.len(),
        1,
        "stdout before the ready line must be the ready line alone: {:#?}",
        &lines[..lines.len().min(20)]
    );
    check(&lines, &r);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stderr = Vec::new();
    while Instant::now() < deadline && !stderr.iter().any(|l: &String| l.contains("INFO")) {
        if let Ok(l) = err.recv_timeout(Duration::from_millis(200)) {
            stderr.push(l);
        }
    }
    assert!(
        stderr.iter().any(|l| l.contains("INFO")),
        "the INFO log lines must be on stderr: {stderr:#?}"
    );
    kill(child);
}
