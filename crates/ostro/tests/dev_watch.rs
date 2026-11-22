//! 10-16 (CLI-08): `ostro dev --watch [DIR]` through the spawned binary, offline (W4).
//!
//! A copy of `crates/ostro-build/tests/fixtures/rust-offline` (workflow `hello`) is
//! watched by a spawned `ostro dev --ephemeral --watch`: the initial deploy, a redeploy on
//! save, ignored paths, failure isolation and coalescing are read from the `ostro dev:`
//! lines on stderr. Needs the system Rust toolchain (rustup + wasm32-wasip2), as
//! `build_cli.rs`. Run with `--test-threads=1`.

#[path = "common/p10.rs"]
mod p10;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;

/// A cold build of the fixture is ~20 s; this is the hang guard under load.
const BUILD_TIMEOUT: Duration = Duration::from_secs(300);

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../ostro-build/tests/fixtures/rust-offline")
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let name = e.file_name();
        if name == "target" {
            continue;
        }
        let (src, dst) = (e.path(), to.join(&name));
        if e.file_type().unwrap().is_dir() {
            copy_tree(&src, &dst);
        } else {
            std::fs::copy(&src, &dst).unwrap();
        }
    }
}

/// Change the greeting and set `hello`'s version (versions are declared in the source).
fn edit(p: &Path, greeting: &str, version: u32) {
    let lib = p.join("src/lib.rs");
    let src = std::fs::read_to_string(&lib).unwrap();
    let mut out = String::new();
    for line in src.lines() {
        if line.contains("Ok(format!(\"") {
            out.push_str(&format!("    Ok(format!(\"{greeting} {{name}}\"))"));
        } else if line.starts_with("#[workflow(name = \"hello\"") {
            out.push_str(&format!(
                "#[workflow(name = \"hello\", version = {version})]"
            ));
        } else if line.starts_with("fn broken(") {
            continue;
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    std::fs::write(&lib, out).unwrap();
}

/// The `ostro dev:` lines of a spawned dev server, accumulated.
struct Log<'a> {
    dev: &'a p10::DevProc,
    /// Every stderr line so far.
    all: Vec<String>,
}

impl<'a> Log<'a> {
    fn new(dev: &'a p10::DevProc) -> Self {
        Log {
            dev,
            all: Vec::new(),
        }
    }

    /// Wait until a line containing `needle` arrives (searching from line `from`); returns
    /// its index. Panics with the whole log on timeout.
    fn wait_for(&mut self, from: usize, needle: &str, timeout: Duration) -> usize {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(i) = self.all[from.min(self.all.len())..]
                .iter()
                .position(|l| l.contains(needle))
            {
                return from + i;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(
                !left.is_zero(),
                "no {needle:?} within {timeout:?}; stderr:\n{}",
                self.all.join("\n")
            );
            if let Ok(l) = self
                .dev
                .stderr_rx
                .recv_timeout(left.min(Duration::from_millis(200)))
            {
                self.all.push(l);
            }
        }
    }

    /// Collect lines for `d`.
    fn settle(&mut self, d: Duration) {
        let deadline = Instant::now() + d;
        while let Some(left) = deadline.checked_duration_since(Instant::now()) {
            if let Ok(l) = self.dev.stderr_rx.recv_timeout(left) {
                self.all.push(l);
            }
        }
    }

    /// The `ostro dev:` lines from `from` on.
    fn watch_lines(&self, from: usize) -> Vec<&String> {
        self.all[from..]
            .iter()
            .filter(|l| l.starts_with("ostro dev:"))
            .collect()
    }

    fn count(&self, from: usize, needle: &str) -> usize {
        self.watch_lines(from)
            .iter()
            .filter(|l| l.contains(needle))
            .count()
    }
}

/// `hello`'s active version from `GET /v1/workflows`.
fn active(dev: &p10::DevProc) -> Option<u64> {
    let (st, body) = p10::http_get(dev.addr, "/v1/workflows");
    assert_eq!(st, 200, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    v["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == "hello")
        .and_then(|w| w["active_version"].as_u64())
}

/// Start a `hello` run (not awaited) and return its version.
fn run_version(dev: &p10::DevProc) -> u64 {
    let (code, start, err) = dev.cli_json(&["run", "hello", "--input", r#"{"name":"ada"}"#]);
    assert_eq!(code, 0, "{start}\n{err}");
    let id = start["run_id"].as_str().unwrap().to_owned();
    let (code, run, err) = dev.cli_json(&["inspect", &id]);
    assert_eq!(code, 0, "{run}\n{err}");
    run["version"].as_u64().unwrap_or_else(|| panic!("{run}"))
}

#[test]
fn watch_redeploys_on_save_ignores_noise_and_survives_broken_edits() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("p");
    copy_tree(&fixture(), &p);
    let ps = p.to_str().unwrap();
    let t_start = Instant::now();
    let dev = p10::spawn_dev(&["--watch", ps], &[("CARGO_NET_OFFLINE", "true")]);
    // the frozen ready line is unchanged by --watch
    assert_eq!(dev.ready.name, "ostro dev");
    assert!(dev.ready.ephemeral);
    let mut log = Log::new(&dev);

    // 1. the initial build + deploy
    let i = log.wait_for(0, "deployed hello v1", BUILD_TIMEOUT);
    eprintln!("[10-16] initial deploy after {:?}", t_start.elapsed());
    assert!(log.all[i].contains("new runs use v1"), "{}", log.all[i]);
    assert_eq!(active(&dev), Some(1));
    assert_eq!(log.count(0, "building"), 1);

    // 2. a save: rebuilt and redeployed, new runs use v2
    let mark = log.all.len();
    let t = Instant::now();
    edit(&p, "hi", 2);
    log.wait_for(mark, "new runs use v2", BUILD_TIMEOUT);
    eprintln!("[10-16] edit -> `new runs use v2`: {:?}", t.elapsed());
    assert_eq!(active(&dev), Some(2));
    assert_eq!(run_version(&dev), 2);
    // FSEvents (macOS) does not bound its delivery by the debounce window: the save can
    // arrive as two batches, the second one during the build, which gives exactly one
    // follow-up build of the same sources. Let it finish before measuring the noise; it
    // must not deploy anything new.
    log.settle(Duration::from_secs(5));
    assert_eq!(
        log.count(mark, "deployed hello"),
        1,
        "one deploy for one save: {:?}",
        log.watch_lines(mark)
    );
    assert_eq!(
        log.count(mark, "build failed"),
        0,
        "{:?}",
        log.watch_lines(mark)
    );
    assert!(
        log.count(mark, "building") <= 2,
        "{:?}",
        log.watch_lines(mark)
    );
    assert_eq!(active(&dev), Some(2));

    // 3. noise: target/, .git/, swap files and vim's 4913 never trigger a build
    let mark = log.all.len();
    std::fs::write(p.join("target/noise.txt"), "x").unwrap();
    std::fs::create_dir_all(p.join(".git")).unwrap();
    std::fs::write(p.join(".git/index"), "x").unwrap();
    std::fs::write(p.join("src/.lib.rs.swp"), "x").unwrap();
    std::fs::write(p.join("src/4913"), "x").unwrap();
    std::fs::remove_file(p.join("src/4913")).unwrap();
    std::fs::write(p.join("src/lib.rs~"), "x").unwrap();
    log.settle(Duration::from_secs(2));
    assert!(
        log.watch_lines(mark).is_empty(),
        "ignored paths triggered: {:?}",
        log.watch_lines(mark)
    );

    // 4. a broken edit: diagnostics, the server and v2 keep serving
    let mark = log.all.len();
    let lib = p.join("src/lib.rs");
    let src = std::fs::read_to_string(&lib).unwrap();
    std::fs::write(&lib, format!("{src}fn broken( {{\n")).unwrap();
    let i = log.wait_for(mark, "build failed; keeping v2", BUILD_TIMEOUT);
    let transcript = log.all[mark..=i.min(log.all.len() - 1)].join("\n");
    assert!(transcript.contains("error"), "{transcript}");
    log.settle(Duration::from_millis(300));
    eprintln!(
        "[10-16] failure transcript (tail):\n{}",
        log.all[mark..]
            .iter()
            .rev()
            .take(12)
            .rev()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(active(&dev), Some(2));
    assert_eq!(run_version(&dev), 2);

    // 5. the fix: v3
    let mark = log.all.len();
    edit(&p, "hey", 3);
    log.wait_for(mark, "new runs use v3", BUILD_TIMEOUT);
    assert_eq!(active(&dev), Some(3));

    // 6. coalescing: five rapid edits while a build runs give exactly one follow-up
    let mark = log.all.len();
    edit(&p, "yo", 4);
    log.wait_for(mark, "building", BUILD_TIMEOUT);
    for (n, g) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        edit(&p, g, 5);
        std::thread::sleep(Duration::from_millis(40 + 10 * n as u64));
    }
    log.wait_for(mark, "new runs use v5", BUILD_TIMEOUT);
    log.settle(Duration::from_secs(2));
    let builds = log.count(mark, "building");
    eprintln!(
        "[10-16] coalescing: {builds} builds for 1 + 5 edits: {:?}",
        log.watch_lines(mark)
    );
    assert!(
        (1..=2).contains(&builds),
        "expected the edit's build plus at most one follow-up: {:?}",
        log.watch_lines(mark)
    );
    assert_eq!(active(&dev), Some(5));
    assert_eq!(run_version(&dev), 5);
}

#[test]
fn watch_without_a_project_exits_2() {
    let d = tempfile::tempdir().unwrap();
    let empty = d.path().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let t = Instant::now();
    let (code, out, err) = p10::run_cli_in(
        d.path(),
        &[
            "dev",
            "--ephemeral",
            "--listen",
            "127.0.0.1:0",
            "--watch",
            empty.to_str().unwrap(),
        ],
        &[],
    );
    assert_eq!(code, 2, "{out}\n{err}");
    assert!(err.contains("no Ostro project"), "{err}");
    assert!(out.is_empty(), "no server started: {out}");
    assert!(t.elapsed() < Duration::from_secs(30));
    // the default DIR is the cwd
    let (code, _, err) = p10::run_cli_in(
        &empty,
        &["dev", "--ephemeral", "--listen", "127.0.0.1:0", "--watch"],
        &[],
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("no Ostro project"), "{err}");
}

/// 10-13: `--watch` and `--time-skipping` together. The watcher's deploys are API writes
/// (the idle detector counts them); between them the node reports auto mode, idle and an
/// untouched clock, and a save still redeploys.
#[test]
fn watch_with_time_skipping_deploys_and_stays_idle() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("p");
    copy_tree(&fixture(), &p);
    let ps = p.to_str().unwrap();
    let dev = p10::spawn_dev(
        &["--watch", ps, "--time-skipping"],
        &[("CARGO_NET_OFFLINE", "true")],
    );
    assert_eq!(dev.ready.name, "ostro dev");
    let mut log = Log::new(&dev);
    log.wait_for(0, "deployed hello v1", BUILD_TIMEOUT);
    assert_eq!(active(&dev), Some(1));
    let (st, body) = p10::http_get(dev.addr, "/v1/dev/time");
    assert_eq!(st, 200, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["mode"], "auto", "{v}");
    assert_eq!(v["idle"], true, "{v}");
    assert_eq!(v["offset_ms"], 0, "{v}");
    let mark = log.all.len();
    edit(&p, "hi", 2);
    log.wait_for(mark, "new runs use v2", BUILD_TIMEOUT);
    assert_eq!(active(&dev), Some(2));
    assert_eq!(run_version(&dev), 2);
}
