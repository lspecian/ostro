//! 10-12: `ostro build` through the spawned binary, offline (W4): the
//! `crates/ostro-build/tests/fixtures/rust-offline` project builds to an
//! `ostro.build.result`, the second run is up to date, no project exits 2, a compile
//! error and a forbidden import exit 1.
//!
//! Needs the system Rust toolchain (rustup 1.100.0 + wasm32-wasip2). The template build is
//! covered by `ostro-build`'s `template_build` under `--features polyglot`.

#[path = "common/p10.rs"]
mod p10;

use std::path::{Path, PathBuf};

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

/// The spawned binary's env: `HOME`/`PATH` are inherited (system rustup), cargo offline.
const ENV: &[(&str, &str)] = &[("CARGO_NET_OFFLINE", "true")];

#[test]
fn build_json_then_up_to_date() {
    let d = tempfile::tempdir().unwrap();
    let t = d.path().join("t");
    copy_tree(&fixture(), &t);
    let t_s = t.display().to_string();

    let (code, v, err) = p10::cli_json_in(d.path(), &["build", &t_s], ENV);
    assert_eq!(code, 0, "{err}");
    assert_eq!(v["kind"], "ostro.build.result", "{v}");
    assert_eq!(v["version"], 1);
    assert_eq!(v["name"], "hello-offline");
    assert_eq!(v["lang"], "rust");
    assert_eq!(v["up_to_date"], false);
    assert_eq!(
        v["workflows"],
        serde_json::json!([{"name": "hello", "version": 1}])
    );
    let sha = v["sha256"].as_str().unwrap().to_owned();
    assert_eq!(sha.len(), 64);
    let wasm = PathBuf::from(v["wasm"].as_str().unwrap());
    assert_eq!(wasm, t.join("target/ostro/hello-offline.wasm"));
    assert_eq!(
        std::fs::metadata(&wasm).unwrap().len(),
        v["bytes"].as_u64().unwrap()
    );
    assert_eq!(
        PathBuf::from(v["manifest"].as_str().unwrap()),
        t.join("target/ostro/manifest.json")
    );
    // The cargo output went to stderr, stdout is the JSON alone (parsed above).
    assert!(err.contains("Finished"), "{err}");

    let t0 = std::time::Instant::now();
    let (code, v, err) = p10::cli_json_in(d.path(), &["build", &t_s], ENV);
    assert_eq!(code, 0, "{err}");
    assert_eq!(v["up_to_date"], true, "{v}");
    assert_eq!(v["sha256"], sha.as_str());
    assert!(t0.elapsed().as_secs_f64() < 2.0, "{:?}", t0.elapsed());

    // Human output, from inside the project dir (DIR defaults to .).
    let (code, out, err) = p10::run_cli_in(&t, &["build"], ENV);
    assert_eq!(code, 0, "{err}");
    assert!(
        out.starts_with("up to date: hello-offline sha256=") && out.contains("workflows=[hello@1]"),
        "{out}"
    );
    assert!(out.contains("(toolchain: system-rustup"), "{out}");
}

#[test]
fn no_project_exits_2() {
    let d = tempfile::tempdir().unwrap();
    let (code, out, err) = p10::run_cli_in(d.path(), &["build", "/nonexistent"], ENV);
    assert_eq!(code, 2, "{out}{err}");
    assert!(err.contains("no Ostro project"), "{err}");
    assert!(out.is_empty(), "{out}");
    // A broken ostro.toml is a usage error too.
    std::fs::write(
        d.path().join("ostro.toml"),
        "[project]\nname = \"x\"\nbogus = 1\n",
    )
    .unwrap();
    let (code, _, err) = p10::run_cli_in(d.path(), &["build"], ENV);
    assert_eq!(code, 2, "{err}");
}

#[test]
fn compile_error_and_forbidden_import_exit_1() {
    let d = tempfile::tempdir().unwrap();
    let t = d.path().join("t");
    copy_tree(&fixture(), &t);
    let lib = t.join("src/lib.rs");
    let original = std::fs::read_to_string(&lib).unwrap();

    std::fs::write(&lib, format!("{original}\nfn broken( {{\n")).unwrap();
    let (code, out, err) = p10::run_cli_in(&t, &["build"], ENV);
    assert_eq!(code, 1, "{out}{err}");
    assert!(
        err.contains("unclosed delimiter"),
        "compiler diagnostics on stderr: {err}"
    );
    assert!(err.contains("ostro: error: cargo build failed"), "{err}");
    assert!(out.is_empty(), "{out}");

    // A workflow calling a WASI API outside the allow-list (std::fs → wasi:filesystem):
    // the engine's validator rejects the component, nothing is written.
    let forbidden = original.replace(
        "Ok(format!(\"hello {name}\"))",
        "let host = std::fs::read_to_string(\"/etc/hostname\").unwrap_or_default();\n    \
         Ok(format!(\"hello {name} from {host}\"))",
    );
    assert_ne!(forbidden, original);
    std::fs::write(&lib, forbidden).unwrap();
    // A relative DIR (from the parent dir): the step resolves it before running cargo.
    let (code, out, err) = p10::run_cli_in(d.path(), &["build", "t", "--json"], ENV);
    assert_eq!(code, 1, "{out}{err}");
    assert!(
        err.contains("failed validation") && err.contains("wasi:filesystem"),
        "{err}"
    );
    assert!(out.is_empty(), "{out}");
    assert!(!t.join("target/ostro/manifest.json").exists());
    assert!(!t.join("target/ostro/inputs.sha256").exists());
}
