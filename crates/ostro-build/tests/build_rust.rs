//! 10-12: the Rust builder with the system toolchain (rustup 1.100.0 + wasm32-wasip2),
//! offline: the `tests/fixtures/rust-offline` project (bundled SDK only, committed
//! Cargo.lock) is built, cached, rebuilt, broken, restored, and reproduces its sha.
//!
//! Fails loudly (no skip) when `rustup` or the wasm32-wasip2 target is missing. Run with
//! `CARGO_NET_OFFLINE=true` (passed through to cargo). The template build (with its
//! `ostro-client` dev-dependency, network) is `template_build` under `--features polyglot`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use ostro_build::pipeline::{BuildError, BuildOptions, build_with, step_for};

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rust-offline")
}

/// Copy `from` into `to` (recursively, without `target/`).
pub fn copy_tree(from: &Path, to: &Path) {
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

fn host(k: &str) -> Option<String> {
    std::env::var(k).ok()
}

/// Panics with a fix-it message unless the system toolchain the fixture selects can build
/// wasm32-wasip2.
fn require_system_rust(project: &Path) {
    let rustup = Command::new("rustup").arg("--version").output();
    assert!(
        rustup.is_ok_and(|o| o.status.success()),
        "rustup is not on PATH: install it (https://rustup.rs); this test needs the system toolchain"
    );
    let out = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .current_dir(project)
        .env_remove("RUSTUP_TOOLCHAIN")
        .output()
        .expect("rustup target list");
    let list = String::from_utf8_lossy(&out.stdout);
    assert!(
        list.lines().any(|l| l.trim() == "wasm32-wasip2"),
        "the wasm32-wasip2 target is not installed for the fixture's toolchain \
         (rustup target add wasm32-wasip2 --toolchain 1.100.0): {list}"
    );
}

fn opts(dir: &Path) -> BuildOptions {
    BuildOptions {
        dir: dir.to_path_buf(),
        ..BuildOptions::default()
    }
}

#[test]
fn offline_fixture_builds_caches_rebuilds_and_reproduces() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("hello-offline");
    copy_tree(&fixture(), &root);
    require_system_rust(&root);

    let t = Instant::now();
    let first = build_with(&opts(&root), &host, &step_for).unwrap_or_else(|e| panic!("{e}"));
    eprintln!(
        "cold build: {:?}, {} bytes, sha256={}",
        t.elapsed(),
        first.bytes,
        first.sha256
    );
    assert!(!first.up_to_date);
    assert!(
        first.workflows.contains(&("hello".to_owned(), 1)),
        "{:?}",
        first.workflows
    );
    let m = ostro_build::manifest::read(&first.manifest_path).unwrap();
    assert_eq!(m.toolchains.tools[0], "system-rustup", "{:?}", m.toolchains);
    assert!(
        root.join("target/ostro/cargo/wasm32-wasip2/release/hello_offline.wasm")
            .is_file()
    );

    let t = Instant::now();
    let again = build_with(&opts(&root), &host, &step_for).unwrap();
    let warm = t.elapsed();
    eprintln!("up-to-date check: {warm:?}");
    assert!(again.up_to_date);
    assert_eq!(again.sha256, first.sha256);
    assert!(warm.as_secs_f64() < 2.0, "{warm:?}");

    let lib = root.join("src/lib.rs");
    let original = std::fs::read_to_string(&lib).unwrap();
    std::fs::write(&lib, original.replace("hello {name}", "hi {name}")).unwrap();
    let t = Instant::now();
    let edited = build_with(&opts(&root), &host, &step_for).unwrap();
    eprintln!("warm rebuild after an edit: {:?}", t.elapsed());
    assert!(!edited.up_to_date);
    assert_ne!(
        edited.sha256, first.sha256,
        "an edit must change the module"
    );

    std::fs::write(&lib, format!("{original}\nfn broken( {{\n")).unwrap();
    let err = build_with(&opts(&root), &host, &step_for).unwrap_err();
    let BuildError::Step {
        step, stderr_tail, ..
    } = &err
    else {
        panic!("expected a step error, got {err:?}")
    };
    assert_eq!(step, "cargo build");
    assert!(stderr_tail.contains("error"), "{stderr_tail}");
    assert!(!err.is_usage());
    // The edited build's wasm is still there; the failure is not up to date.
    assert!(edited.wasm_path.is_file());
    assert!(!root.join("target/ostro/inputs.sha256").exists());

    std::fs::write(&lib, &original).unwrap();
    let restored = build_with(&opts(&root), &host, &step_for).unwrap();
    assert!(!restored.up_to_date);
    assert_eq!(
        restored.sha256, first.sha256,
        "Rust builds must be reproducible: restored sources, different module"
    );
}

#[test]
fn missing_lockfile_builds_with_a_warning_offline() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("hello-offline");
    copy_tree(&fixture(), &root);
    require_system_rust(&root);
    std::fs::remove_file(root.join("Cargo.lock")).unwrap();
    // The cargo cache holds every locked crate, so offline resolution still succeeds.
    let out = build_with(&opts(&root), &host, &step_for).unwrap_or_else(|e| panic!("{e}"));
    assert!(out.workflows.contains(&("hello".to_owned(), 1)));
    assert!(
        root.join("Cargo.lock").is_file(),
        "cargo generated the lockfile"
    );
    // The generated lockfile is part of the recorded inputs: the next run is up to date.
    assert!(
        build_with(&opts(&root), &host, &step_for)
            .unwrap()
            .up_to_date
    );
}

#[cfg(feature = "polyglot")]
#[test]
fn template_build() {
    use ostro_build::project::Lang;
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("tmpl");
    ostro_build::templates::render(Lang::Rust, "tmpl", &root, false).unwrap();
    // The template's dev-dependencies (ostro-client, tokio) resolve from the network.
    let net = |k: &str| {
        if k == "CARGO_NET_OFFLINE" {
            None
        } else {
            std::env::var(k).ok()
        }
    };
    let t = Instant::now();
    let out = build_with(&opts(&root), &net, &step_for).unwrap_or_else(|e| panic!("{e}"));
    eprintln!(
        "template cold build: {:?}, {} bytes, sha256={}",
        t.elapsed(),
        out.bytes,
        out.sha256
    );
    assert!(
        out.workflows.contains(&("hello".to_owned(), 1)),
        "{:?}",
        out.workflows
    );
    let t = Instant::now();
    assert!(
        build_with(&opts(&root), &net, &step_for)
            .unwrap()
            .up_to_date
    );
    eprintln!("template up-to-date: {:?}", t.elapsed());
}
