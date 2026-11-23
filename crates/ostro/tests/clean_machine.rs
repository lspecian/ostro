//! 10-19 (SC1): drives `scripts/clean-machine.sh`, the clean-machine e2e
//! (`ostro init -> ostro dev -> ostro deploy -> ostro run` with no pre-installed language
//! toolchain, under 600 s per language).
//!
//! - `print_plan` (default suite): the container plan mentions ca-certificates and the
//!   read-only ostro mount, without running Docker.
//! - `local_all` (`#[ignore]`, network): `--local all --no-cc` with this test's `ostro`
//!   (`OSTRO_BIN` overrides it): no cargo/rustup/node/npm/uv/go on PATH, cc/gcc/clang
//!   masked, fresh HOME and toolchain cache. About 2-4 minutes cold for all three.
//! - `container_ubuntu_2404`, `container_debian_12` (`#[ignore]`, network + Docker): a
//!   fresh container with only ca-certificates. They need the static musl ostro
//!   (`OSTRO_MUSL_BIN`, else `<target>/x86_64-unknown-linux-musl/release/ostro`) and
//!   `docker` (or `OSTRO_DOCKER`) and FAIL (not skip) without them.
//!
//! Run: `cargo test -p ostro --test clean_machine -- --ignored --test-threads=1 local_all`.

use std::path::{Path, PathBuf};
use std::process::Command;

const HUMAN_NEEDED: &str = "docker not available (human_needed: run in CI or on a Docker host)";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn script() -> PathBuf {
    root().join("scripts/clean-machine.sh")
}

fn ostro_bin() -> PathBuf {
    std::env::var_os("OSTRO_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_ostro")))
}

/// Runs the script with `args`, streaming its output; returns (success, combined output).
fn run_script(args: &[&str]) -> (bool, String) {
    let out = Command::new("bash")
        .arg(script())
        .args(args)
        .output()
        .expect("spawn bash scripts/clean-machine.sh");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    println!("{text}");
    (out.status.success(), text)
}

/// The distinct `CLEAN <lang> <env> OK ...` lines (the script repeats them in its summary).
fn clean_lines(out: &str) -> std::collections::BTreeSet<&str> {
    out.lines()
        .filter(|l| l.starts_with("CLEAN ") && l.contains(" OK total="))
        .collect()
}

fn docker_available() -> bool {
    let docker = std::env::var("OSTRO_DOCKER").unwrap_or_else(|_| "docker".into());
    Command::new(docker)
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn musl_bin() -> PathBuf {
    if let Some(p) = std::env::var_os("OSTRO_MUSL_BIN") {
        return PathBuf::from(p);
    }
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root().join("target"));
    target.join("x86_64-unknown-linux-musl/release/ostro")
}

fn container(image: &str) {
    assert!(docker_available(), "{HUMAN_NEEDED}");
    let bin = musl_bin();
    assert!(
        Path::new(&bin).is_file(),
        "static musl ostro not found at {} (build it with `cargo zigbuild --release -p ostro \
         --target x86_64-unknown-linux-musl` or set OSTRO_MUSL_BIN)",
        bin.display()
    );
    let bin = bin.display().to_string();
    let (ok, out) = run_script(&["all", "--image", image, "--ostro", &bin]);
    assert!(ok, "clean-machine.sh all --image {image} failed");
    assert_eq!(clean_lines(&out).len(), 3, "expected 3 CLEAN OK lines");
}

#[test]
fn print_plan() {
    let (ok, out) = run_script(&["all", "--image", "debian:12", "--print-plan"]);
    assert!(ok, "--print-plan failed");
    assert!(
        out.contains("ca-certificates"),
        "plan lacks ca-certificates"
    );
    assert!(
        out.contains(":/usr/local/bin/ostro:ro"),
        "plan lacks the read-only ostro mount"
    );
    for lang in ["rust", "python", "ts"] {
        assert!(out.contains(&format!("# {lang} (image debian:12)")));
        assert!(out.contains(&format!("--lang {lang}")));
    }
    assert!(out.contains("docker run --rm"));
    assert!(
        clean_lines(&out).is_empty(),
        "--print-plan must not run anything"
    );

    // The local and --docker plans parse too.
    let (ok, out) = run_script(&["ts", "--local", "--no-cc", "--print-plan"]);
    assert!(
        ok && out.contains("env -i") && out.contains("OSTRO_NO_CC=1"),
        "{out}"
    );
    let (ok, out) = run_script(&[
        "python",
        "--image",
        "ubuntu:24.04",
        "--docker",
        "--print-plan",
    ]);
    assert!(
        ok && out.contains("docker.sock") && out.contains("--docker"),
        "{out}"
    );

    // Usage errors exit 2.
    let s = Command::new("bash")
        .arg(script())
        .args(["all"])
        .output()
        .expect("spawn");
    assert_eq!(s.status.code(), Some(2));
}

#[test]
#[ignore = "network: downloads the Rust, Python and TS toolchains into a fresh cache"]
fn local_all() {
    let bin = ostro_bin().display().to_string();
    let (ok, out) = run_script(&["all", "--local", "--no-cc", "--ostro", &bin]);
    assert!(ok, "clean-machine.sh all --local --no-cc failed");
    assert_eq!(clean_lines(&out).len(), 3, "expected 3 CLEAN OK lines");
    assert!(
        out.contains("zig cc fallback exercised"),
        "--no-cc must exercise the zig cc fallback"
    );
}

#[test]
#[ignore = "network + Docker: fresh ubuntu:24.04 container"]
fn container_ubuntu_2404() {
    container("ubuntu:24.04");
}

#[test]
#[ignore = "network + Docker: fresh debian:12 container"]
fn container_debian_12() {
    container("debian:12");
}
