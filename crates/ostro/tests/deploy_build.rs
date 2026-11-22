//! 10-16 (CLI-05): `ostro deploy [DIR]` through the spawned binary, offline (W4).
//!
//! A copy of `crates/ostro-build/tests/fixtures/rust-offline` (workflow `hello`) is built
//! if needed, uploaded and activated idempotently against a spawned
//! `ostro dev --ephemeral`. Needs the system Rust toolchain (rustup + wasm32-wasip2), as
//! `build_cli.rs`. The `.wasm` form (05-14) stays covered by `cli_verbs`.

#[path = "common/p10.rs"]
mod p10;

use std::path::{Path, PathBuf};

use serde_json::Value;

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

/// A fresh copy of the offline fixture under `root/p`.
fn project(root: &Path) -> PathBuf {
    let p = root.join("p");
    copy_tree(&fixture(), &p);
    p
}

/// `ostro <args>` in `cwd` against `dev`, cargo offline.
fn cli(dev: &p10::DevProc, cwd: &Path, args: &[&str]) -> (i32, String, String) {
    let server = dev.url();
    p10::run_cli_in(
        cwd,
        args,
        &[("OSTRO_SERVER", &server), ("CARGO_NET_OFFLINE", "true")],
    )
}

/// [`cli`] with `--json`, stdout parsed.
fn cli_json(dev: &p10::DevProc, cwd: &Path, args: &[&str]) -> (i32, Value, String) {
    let server = dev.url();
    p10::cli_json_in(
        cwd,
        args,
        &[("OSTRO_SERVER", &server), ("CARGO_NET_OFFLINE", "true")],
    )
}

/// `hello`'s `(active_version, [versions])` from `GET /v1/workflows`.
fn hello(dev: &p10::DevProc) -> (Option<u64>, Vec<u64>) {
    let (st, body) = p10::http_get(dev.addr, "/v1/workflows");
    assert_eq!(st, 200, "{body}");
    let v: Value = serde_json::from_str(&body).unwrap();
    let Some(w) = v["workflows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["name"] == "hello")
    else {
        return (None, Vec::new());
    };
    (
        w["active_version"].as_u64(),
        w["versions"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|x| x["version"].as_u64())
            .collect(),
    )
}

/// Start a `hello` run (it sleeps 3 days; not awaited) and return its version.
fn run_version(dev: &p10::DevProc, cwd: &Path) -> u64 {
    let (code, start, err) = cli_json(dev, cwd, &["run", "hello", "--input", r#"{"name":"ada"}"#]);
    assert_eq!(code, 0, "{start}\n{err}");
    let id = start["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no run_id: {start}"))
        .to_owned();
    let (code, run, err) = cli_json(dev, cwd, &["inspect", &id]);
    assert_eq!(code, 0, "{run}\n{err}");
    run["version"]
        .as_u64()
        .or_else(|| run["run"]["version"].as_u64())
        .unwrap_or_else(|| panic!("no version in {run}"))
}

/// Change the greeting and bump `hello`'s version to `version` (workflow versions are
/// declared in the source; the server refuses a second module for the same version).
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
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    assert_ne!(src, out, "edit changed nothing");
    std::fs::write(&lib, out).unwrap();
}

#[test]
fn wasm_form_keeps_the_05_14_shape() {
    // Build the fixture once, then deploy its .wasm the 05-14 way.
    let d = tempfile::tempdir().unwrap();
    let p = project(d.path());
    let dev = p10::spawn_dev(&[], &[]);
    let (code, out, err) = p10::run_cli_in(
        d.path(),
        &["build", p.to_str().unwrap()],
        &[("CARGO_NET_OFFLINE", "true")],
    );
    assert_eq!(code, 0, "{out}\n{err}");
    let wasm = p.join("target/ostro/hello-offline.wasm");
    let (code, v, err) = cli_json(
        &dev,
        d.path(),
        &["deploy", wasm.to_str().unwrap(), "--activate"],
    );
    assert_eq!(code, 0, "{err}");
    let keys: Vec<&String> = v.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["activated", "upload"], "{v}");
    assert_eq!(v["upload"]["created"], true);
    assert_eq!(v["activated"][0]["name"], "hello");
    // without --activate: the upload body alone
    let (code, v, err) = cli_json(&dev, d.path(), &["deploy", wasm.to_str().unwrap()]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(v["created"], false, "{v}");
    assert!(v.get("kind").is_none());
}

#[test]
fn project_deploy_is_idempotent() {
    let d = tempfile::tempdir().unwrap();
    let p = project(d.path());
    let ps = p.to_str().unwrap();
    let dev = p10::spawn_dev(&[], &[]);

    // 1. build + upload + activate (ostro.toml: activate defaults to true)
    let (code, v, err) = cli_json(&dev, d.path(), &["deploy", ps]);
    assert_eq!(code, 0, "{v}\n{err}");
    eprintln!(
        "first deploy:\n{}",
        serde_json::to_string_pretty(&v).unwrap()
    );
    assert_eq!(v["kind"], "ostro.deploy.result");
    assert_eq!(v["version"], 1);
    assert_eq!(v["build"]["up_to_date"], false, "{v}");
    assert_eq!(v["build"]["name"], "hello-offline");
    assert_eq!(v["upload"]["created"], true);
    let sha1 = v["upload"]["module"]["sha256"].as_str().unwrap().to_owned();
    assert_eq!(v["build"]["sha256"], sha1.as_str());
    assert_eq!(v["upload"]["module"]["workflows"][0]["name"], "hello");
    assert_eq!(v["upload"]["module"]["workflows"][0]["version"], 1);
    let act = v["activated"].as_array().unwrap();
    assert_eq!(act.len(), 1, "{v}");
    assert_eq!(act[0]["name"], "hello");
    assert_eq!(act[0]["active_version"], 1);
    assert_eq!(hello(&dev), (Some(1), vec![1]));
    assert_eq!(run_version(&dev, d.path()), 1);

    // 2. unchanged: up to date, same sha, no new version, nothing activated
    let (code, v, err) = cli_json(&dev, d.path(), &["deploy", ps]);
    assert_eq!(code, 0, "{v}\n{err}");
    eprintln!(
        "second deploy:\n{}",
        serde_json::to_string_pretty(&v).unwrap()
    );
    assert_eq!(v["build"]["up_to_date"], true, "{v}");
    assert_eq!(v["upload"]["created"], false);
    assert_eq!(v["upload"]["module"]["sha256"], sha1.as_str());
    assert_eq!(v["activated"], Value::Array(vec![]));
    assert_eq!(hello(&dev), (Some(1), vec![1]));
    // human form of the same
    let (code, out, err) = cli(&dev, d.path(), &["deploy", ps]);
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("up to date: hello-offline"), "{out}");
    assert!(out.contains("unchanged"), "{out}");
    assert!(out.contains("workflows: hello@1"), "{out}");
    assert!(out.contains("already active"), "{out}");

    // 3. a source change (and its version bump): new sha, v2 activated, new runs on v2
    edit(&p, "hi", 2);
    let (code, v, err) = cli_json(&dev, d.path(), &["deploy", ps]);
    assert_eq!(code, 0, "{v}\n{err}");
    assert_eq!(v["build"]["up_to_date"], false);
    assert_eq!(v["upload"]["created"], true);
    assert_ne!(v["upload"]["module"]["sha256"], sha1.as_str());
    assert_eq!(v["activated"][0]["active_version"], 2, "{v}");
    assert_eq!(hello(&dev), (Some(2), vec![1, 2]));
    assert_eq!(run_version(&dev, d.path()), 2);

    // 4. a source change without a version bump: 409 with the hint, v2 stays active
    edit(&p, "hey", 2);
    let (code, out, err) = cli(&dev, d.path(), &["deploy", ps]);
    assert_eq!(code, 1, "{out}\n{err}");
    assert!(err.contains("bump the workflow's version"), "{err}");
    assert_eq!(hello(&dev), (Some(2), vec![1, 2]));

    // 5. --activate=false: uploaded, not activated
    edit(&p, "yo", 3);
    let (code, v, err) = cli_json(&dev, d.path(), &["deploy", ps, "--activate=false"]);
    assert_eq!(code, 0, "{v}\n{err}");
    assert_eq!(v["upload"]["created"], true);
    assert_eq!(v["activated"], Value::Array(vec![]));
    assert_eq!(hello(&dev), (Some(2), vec![1, 2, 3]));
}

#[test]
fn no_build_and_build_failures() {
    let d = tempfile::tempdir().unwrap();
    let p = project(d.path());
    let ps = p.to_str().unwrap();
    let dev = p10::spawn_dev(&[], &[]);

    // --no-build before any build: exit 2
    let (code, out, err) = cli(&dev, d.path(), &["deploy", ps, "--no-build"]);
    assert_eq!(code, 2, "{out}\n{err}");
    assert!(err.contains("run `ostro build` first"), "{err}");
    assert_eq!(hello(&dev), (None, vec![]));

    // build, then --no-build deploys the current artefact (relative DIR, from the parent)
    let (code, out, err) =
        p10::run_cli_in(d.path(), &["build", "p"], &[("CARGO_NET_OFFLINE", "true")]);
    assert_eq!(code, 0, "{out}\n{err}");
    let (code, v, err) = cli_json(&dev, d.path(), &["deploy", "p", "--no-build"]);
    assert_eq!(code, 0, "{v}\n{err}");
    assert_eq!(v["build"]["up_to_date"], true);
    assert_eq!(hello(&dev), (Some(1), vec![1]));

    // a stale artefact (source edited) or a deleted one: exit 2
    edit(&p, "hi", 2);
    let (code, _, err) = cli(&dev, d.path(), &["deploy", ps, "--no-build"]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("run `ostro build` first"), "{err}");
    for e in std::fs::read_dir(p.join("target/ostro")).unwrap() {
        let path = e.unwrap().path();
        if path.extension().is_some_and(|x| x == "wasm") {
            std::fs::remove_file(path).unwrap();
        }
    }
    let (code, _, err) = cli(&dev, d.path(), &["deploy", ps, "--no-build"]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("run `ostro build` first"), "{err}");

    // a syntax error: exit 1 with the compiler's diagnostics, nothing uploaded
    let (_, before) = p10::http_get(dev.addr, "/v1/workflows");
    let lib = p.join("src/lib.rs");
    let src = std::fs::read_to_string(&lib).unwrap();
    std::fs::write(&lib, format!("{src}\nfn broken( {{\n")).unwrap();
    let (code, out, err) = cli(&dev, d.path(), &["deploy", ps]);
    assert_eq!(code, 1, "{out}\n{err}");
    assert!(err.contains("error"), "{err}");
    assert!(out.is_empty(), "{out}");
    let (_, after) = p10::http_get(dev.addr, "/v1/workflows");
    assert_eq!(before, after);

    // no project: exit 2
    let empty = tempfile::tempdir().unwrap();
    let (code, _, err) = cli(&dev, d.path(), &["deploy", empty.path().to_str().unwrap()]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("no Ostro project"), "{err}");
}

/// The `ostro init --lang python` template deploys and a run starts (hermetic Python
/// toolchain: network on first use).
#[cfg(feature = "polyglot")]
#[test]
fn python_project_deploy() {
    let d = tempfile::tempdir().unwrap();
    let (code, out, err) = p10::run_cli_in(d.path(), &["init", "py", "--lang", "python"], &[]);
    assert_eq!(code, 0, "{out}\n{err}");
    let dev = p10::spawn_dev(&[], &[]);
    let server = dev.url();
    let (code, v, err) =
        p10::cli_json_in(d.path(), &["deploy", "py"], &[("OSTRO_SERVER", &server)]);
    assert_eq!(code, 0, "{v}\n{err}");
    assert_eq!(v["kind"], "ostro.deploy.result");
    let wf = v["upload"]["module"]["workflows"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(v["activated"][0]["name"], wf.as_str(), "{v}");
    let (code, start, err) = p10::cli_json_in(
        d.path(),
        &["run", &wf, "--input", r#"{"name":"ada"}"#],
        &[("OSTRO_SERVER", &server)],
    );
    assert_eq!(code, 0, "{start}\n{err}");
    assert!(start["run_id"].is_string(), "{start}");
}

/// The `ostro init --lang rust` template (crates from the network) deploys.
#[cfg(feature = "polyglot")]
#[test]
fn rust_template_deploy() {
    let d = tempfile::tempdir().unwrap();
    let (code, out, err) = p10::run_cli_in(d.path(), &["init", "rs", "--lang", "rust"], &[]);
    assert_eq!(code, 0, "{out}\n{err}");
    let dev = p10::spawn_dev(&[], &[]);
    let server = dev.url();
    let (code, v, err) =
        p10::cli_json_in(d.path(), &["deploy", "rs"], &[("OSTRO_SERVER", &server)]);
    assert_eq!(code, 0, "{v}\n{err}");
    assert_eq!(v["activated"][0]["name"], "hello", "{v}");
}
