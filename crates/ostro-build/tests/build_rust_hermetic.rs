//! 10-12 clean-machine Rust build (polyglot only: downloads rustup-init, Rust 1.100.0 +
//! wasm32-wasip2 and zig 0.17.1 into the shared, reused `target/ostro-toolchains-test`
//! cache). The host `PATH` is a constructed dir of basic utilities with `cc`, `gcc` and
//! `clang` masked by failing stubs and `OSTRO_NO_CC=1` (`--no-cc`), so the build must go
//! through the hermetic rustup and the zig `cc` shim. Prints the timing and the shim argv
//! log. Fails loudly without network.

#![cfg(feature = "polyglot")]

use std::path::{Path, PathBuf};
use std::time::Instant;

use ostro_build::pipeline::{BuildOptions, build_with, step_for};
use ostro_build::project::ToolchainMode;

fn repo_target() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")
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

/// A PATH dir with symlinks to basic utilities only and failing `cc`/`gcc`/`clang` stubs.
fn no_cc_path(dir: &Path) -> PathBuf {
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    for tool in [
        "sh", "env", "cat", "ls", "mkdir", "rm", "cp", "mv", "ln", "chmod", "uname", "tr", "sed",
        "grep", "head", "tail", "find", "sort", "wc", "dirname", "basename", "printf", "git",
    ] {
        for d in ["/usr/bin", "/bin"] {
            let p = Path::new(d).join(tool);
            if p.exists() {
                symlink(&p, bin.join(tool)).unwrap();
                break;
            }
        }
    }
    for cc in ["cc", "gcc", "clang"] {
        let p = bin.join(cc);
        std::fs::write(
            &p,
            "#!/bin/sh\necho \"masked $0 (no host C compiler)\" >&2\nexit 1\n",
        )
        .unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    bin
}

#[test]
fn hermetic_build_without_cc_uses_rustup_init_and_zig() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("hello-offline");
    copy_tree(
        &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rust-offline"),
        &root,
    );
    let path = no_cc_path(d.path()).display().to_string();
    let home = d.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let cache = repo_target().join("ostro-toolchains-test");
    let log = d.path().join("shim.log");
    let vars: Vec<(String, String)> = vec![
        ("PATH".into(), path),
        ("HOME".into(), home.display().to_string()),
        ("OSTRO_TOOLCHAIN_DIR".into(), cache.display().to_string()),
        ("OSTRO_NO_CC".into(), "1".into()),
        ("OSTRO_ZIG_SHIM_LOG".into(), log.display().to_string()),
    ];
    let host = move |k: &str| {
        vars.iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
            .or_else(|| match k {
                // proxies of the CI machine, if any
                "HTTPS_PROXY" | "https_proxy" | "HTTP_PROXY" | "http_proxy" | "NO_PROXY"
                | "no_proxy" => std::env::var(k).ok(),
                _ => None,
            })
    };
    let opts = BuildOptions {
        dir: root.clone(),
        toolchain: Some(ToolchainMode::Hermetic),
        ..BuildOptions::default()
    };
    let t = Instant::now();
    let out = build_with(&opts, &host, &step_for).unwrap_or_else(|e| panic!("{e}"));
    eprintln!(
        "hermetic build (cache {}): {:?}, {} bytes, sha256={}",
        cache.display(),
        t.elapsed(),
        out.bytes,
        out.sha256
    );
    assert!(
        out.workflows.contains(&("hello".to_owned(), 1)),
        "{:?}",
        out.workflows
    );
    let m = ostro_build::manifest::read(&out.manifest_path).unwrap();
    let tools = m.toolchains.tools.join(" ");
    assert!(
        tools.contains("rustup-init@") && tools.contains("zig@0.17.1"),
        "{tools}"
    );

    let shim_log = std::fs::read_to_string(&log).unwrap_or_default();
    eprintln!(
        "zig shim invocations: {} (first 5):\n{}",
        shim_log.lines().count(),
        shim_log.lines().take(5).collect::<Vec<_>>().join("\n")
    );
    assert!(
        shim_log.lines().any(|l| l.starts_with("cc ")),
        "the host linker never went through the zig shim"
    );
}
