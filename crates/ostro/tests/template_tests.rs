//! 10-17 (SC4): the test that `ostro init` generates passes in its own language, against a
//! throwaway `ostro dev --ephemeral --time-skipping` started by the test kit:
//!
//! - rust: `cargo test` (host target; the kit is the bundled `ostro-client` `testing`);
//! - python: `uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest
//!   <dir>/tests -q` (fixtures from the bundled `ostro_client.pytest_plugin`);
//! - ts: `node --test '<dir>/test/**/*.test.js'` (the bundled `ostro-client/testing`).
//!
//! Each generated test builds and deploys the project (`ostro deploy`), starts `hello`,
//! sends `approve` and waits for the result: the server skips the 3-day sleep. Wall times
//! (build included) are printed as `TIMING`.
//!
//! Polyglot only: Python/TS use the hermetic toolchains from the shared
//! `target/ostro-toolchains-test` cache (`OSTRO_TOOLCHAIN_DIR` overrides it), Rust the
//! system rustup with wasm32-wasip2 and crates.io; `uv` and `node` must be on PATH. Run
//! with `--test-threads=1`.
#![cfg(feature = "polyglot")]

#[path = "common/p10.rs"]
mod p10;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Hang guard per generated test run (cold toolchains and crates included).
const RUN_TIMEOUT: Duration = Duration::from_secs(900);

fn toolchain_dir() -> String {
    std::env::var("OSTRO_TOOLCHAIN_DIR").unwrap_or_else(|_| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/ostro-toolchains-test")
            .display()
            .to_string()
    })
}

/// `ostro init <name> --lang <lang>` in `parent`; returns the project dir.
fn init(parent: &Path, lang: &str) -> PathBuf {
    let name = format!("tmpl-{lang}");
    let (code, out, err) = p10::run_cli_in(parent, &["init", &name, "--lang", lang], &[]);
    assert_eq!(code, 0, "ostro init --lang {lang}: {out}{err}");
    parent.join(name)
}

/// `cmd` with the environment of a user's shell: the cargo test harness variables that
/// would leak into a nested cargo or build are removed, OSTRO_BIN and the toolchain cache
/// set.
fn user_cmd(program: &str, cwd: &Path) -> Command {
    let mut cmd = Command::new(program);
    cmd.current_dir(cwd).stdin(Stdio::null());
    for (k, _) in std::env::vars_os() {
        let k = k.to_string_lossy();
        let keep = [
            "CARGO_HOME",
            "CARGO_NET_",
            "CARGO_HTTP_",
            "CARGO_REGISTRIES_",
        ]
        .iter()
        .any(|p| k.starts_with(p));
        if k.starts_with("OSTRO_")
            || (k.starts_with("CARGO_") && !keep)
            || matches!(
                k.as_ref(),
                "RUSTFLAGS" | "RUSTC_WRAPPER" | "RUST_LOG" | "RUSTC"
            )
        {
            cmd.env_remove(k.as_ref());
        }
    }
    cmd.env("OSTRO_BIN", p10::ostro_bin())
        .env("OSTRO_TOOLCHAIN_DIR", toolchain_dir());
    // Keep the parallelism cap of the outer build, if any.
    if let Ok(j) = std::env::var("CARGO_BUILD_JOBS") {
        cmd.env("CARGO_BUILD_JOBS", j);
    }
    cmd
}

/// Run `cmd` to completion (with the hang guard); returns (success, combined output,
/// wall time).
fn run(mut cmd: Command) -> (bool, String, Duration) {
    let t = Instant::now();
    let child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {cmd:?}: {e}"));
    let pid = child.id();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    let out = match rx.recv_timeout(RUN_TIMEOUT) {
        Ok(out) => out.expect("wait"),
        Err(_) => {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
            panic!("{cmd:?} did not finish within {RUN_TIMEOUT:?}");
        }
    };
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text, t.elapsed())
}

#[test]
fn rust_template_test_passes() {
    let d = tempfile::tempdir().unwrap();
    let p = init(d.path(), "rust");
    let mut cmd = user_cmd("cargo", &p);
    cmd.args(["test", "--manifest-path"])
        .arg(p.join("Cargo.toml"));
    let (ok, out, wall) = run(cmd);
    assert!(ok, "cargo test in the rust template failed:\n{out}");
    assert!(
        out.contains("test hello_completes_after_three_days_and_approval ... ok"),
        "{out}"
    );
    eprintln!("TIMING rust template `cargo test` (build included): {wall:?}");
}

#[test]
fn python_template_test_passes() {
    let d = tempfile::tempdir().unwrap();
    let p = init(d.path(), "python");
    let mut cmd = user_cmd("uv", d.path());
    cmd.args([
        "run",
        "--no-project",
        "--python",
        "3.12",
        "--with",
        "pytest==8.4.2",
        "python",
        "-m",
        "pytest",
    ])
    .arg(p.join("tests"))
    .arg("-q");
    let (ok, out, wall) = run(cmd);
    assert!(ok, "pytest in the python template failed:\n{out}");
    assert!(out.contains("1 passed"), "{out}");
    assert!(!out.contains("failed") && !out.contains(" error"), "{out}");
    eprintln!("TIMING python template `pytest` (build included): {wall:?}");
}

#[test]
fn ts_template_test_passes() {
    let d = tempfile::tempdir().unwrap();
    let p = init(d.path(), "ts");
    let mut cmd = user_cmd("node", d.path());
    cmd.args(["--test", "--test-reporter=tap"])
        .arg(format!("{}/test/**/*.test.js", p.display()));
    let (ok, out, wall) = run(cmd);
    assert!(ok, "node --test in the ts template failed:\n{out}");
    assert!(
        out.contains("# pass 1") && out.contains("# fail 0"),
        "{out}"
    );
    eprintln!("TIMING ts template `node --test` (build included): {wall:?}");
}
