//! 10-15 (D-P10-12): every `ostro init` template builds and describes its `hello`
//! workflow, so the templates cannot rot. Rust uses `auto` (the system rustup when present,
//! else the hermetic toolchain; the template's dev-dependencies resolve from the network),
//! Python and TypeScript the hermetic toolchains from the shared
//! `target/ostro-toolchains-test` cache. Polyglot only (downloads); prints timings.

#![cfg(feature = "polyglot")]

use std::path::PathBuf;
use std::time::Instant;

use ostro_build::pipeline::{BuildOptions, build_with, step_for};
use ostro_build::project::{Lang, ToolchainMode};

fn build_template(lang: Lang, mode: ToolchainMode) {
    let d = tempfile::tempdir().unwrap();
    let name = format!("tmpl-{}", lang.as_str());
    let root = d.path().join(&name);
    ostro_build::templates::render(lang, &name, &root, false).unwrap();
    let home = d.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let cache = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/ostro-toolchains-test")
        .display()
        .to_string();
    let home_s = home.display().to_string();
    let host = move |k: &str| match k {
        "OSTRO_TOOLCHAIN_DIR" => Some(cache.clone()),
        // Rust keeps the real HOME (the system rustup lives there).
        "HOME" if lang != Lang::Rust => Some(home_s.clone()),
        "CARGO_NET_OFFLINE" | "OSTRO_OFFLINE" => None,
        _ => std::env::var(k).ok(),
    };
    let opts = BuildOptions {
        dir: root.clone(),
        toolchain: Some(mode),
        ..BuildOptions::default()
    };
    let t = Instant::now();
    let out = build_with(&opts, &host, &step_for).unwrap_or_else(|e| panic!("{lang}: {e}"));
    let m = ostro_build::manifest::read(&out.manifest_path).unwrap();
    eprintln!(
        "TIMING {lang} template ({}): {:?}, {} bytes, workflows {:?}",
        m.toolchains.tools.join(", "),
        t.elapsed(),
        out.bytes,
        out.workflows
    );
    assert!(
        out.workflows.iter().any(|(n, _)| n == "hello"),
        "{lang}: {:?}",
        out.workflows
    );
    assert_eq!(m.name, name);
}

#[test]
fn rust_template_builds() {
    build_template(Lang::Rust, ToolchainMode::Auto);
}

#[test]
fn python_template_builds_hermetic() {
    build_template(Lang::Python, ToolchainMode::Hermetic);
}

#[test]
fn ts_template_builds_hermetic() {
    build_template(Lang::Ts, ToolchainMode::Hermetic);
}
