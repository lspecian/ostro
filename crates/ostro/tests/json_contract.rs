//! 10-18 (D-P10-10): the `--json` contract of the local verbs on the real binary.
//!
//! - Every local verb (`init`, `build`, `deploy DIR`, `replay`, `toolchain list`,
//!   `doctor`) prints an object whose first two keys are `"kind": "ostro.<verb>.result"`
//!   and `"version": 1`. That order is checked on the raw stdout text.
//! - The body is snapshot-tested after redaction ([`redact`]): absolute paths → `[PATH]`,
//!   shas → `[SHA]`, timings → `[MS]`, timestamps → `[TIMESTAMP]`, tool and libc
//!   versions → `[VER]`, the toolchain set id, the SDK bundle id and toolchain cache
//!   entries → `[ID]`, run ids → `[RUN_ID]`, sizes → `[SIZE]`, the platform →
//!   `[PLATFORM]`, plus the host-dependent `doctor` probes (`[BOOL]`, `[WARNINGS]`).
//! - `ostro deploy x.wasm --json` keeps the 05-14 REST-proxy shape (no `kind`).
//! - The exit-code table of docs/cli-json.md equals `ostro_cli::output::EXIT_CODES`.
//!
//! `build` and `deploy DIR` use a copy of `crates/ostro-build/tests/fixtures/rust-offline`
//! with the system Rust toolchain and `CARGO_NET_OFFLINE=true` (W4), as `build_cli.rs`.

mod common;
#[path = "common/p10.rs"]
mod p10;

use std::path::{Path, PathBuf};

use serde_json::Value;

// ---------------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------------

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

fn arg(p: &Path) -> &str {
    p.to_str().expect("utf-8 path")
}

/// Run `ostro <args> --json` in `cwd` and check the envelope on the raw text: the first
/// two keys are `kind` = `ostro.<kind>.result` and `version` = 1. Returns the exit code
/// and the parsed body.
fn local(cwd: &Path, args: &[&str], env: &[(&str, &str)], kind: &str) -> (i32, Value) {
    let mut v = args.to_vec();
    v.push("--json");
    let (code, out, err) = p10::run_cli_in(cwd, &v, env);
    let prefix = format!("{{\n  \"kind\": \"ostro.{kind}.result\",\n  \"version\": 1");
    assert!(
        out.starts_with(&prefix),
        "{v:?}: stdout does not start with the envelope (exit {code}):\n{out}\n{err}"
    );
    let body: Value = serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("{v:?}: stdout is not JSON ({e}): {out}\n{err}"));
    (code, body)
}

/// A 64-character lowercase hex string (a sha256).
fn is_sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

/// An absolute path on any host (`/x`, `C:\x`, `C:/x`, `\\server\x`).
fn is_abs_path(s: &str) -> bool {
    let b = s.as_bytes();
    s.starts_with('/')
        || s.starts_with("\\\\")
        || (b.len() > 2
            && b[0].is_ascii_alphabetic()
            && b[1] == b':'
            && matches!(b[2], b'\\' | b'/'))
}

/// The redaction rules (see the module docs). `tmp` (the test's temp root) is also
/// replaced by `[PATH]` inside longer strings (messages).
fn redact(v: &Value, tmp: &str) -> Value {
    fn by_key(k: &str, x: &Value) -> Option<&'static str> {
        if x.is_null() {
            return None;
        }
        Some(match k {
            "sha256" | "module_sha256" | "actual_sha256" | "inputs_sha256" => "[SHA]",
            "toolchain_set" | "sdk_bundle" | "entry" | "bundle_id" => "[ID]",
            "run_id" | "retry_of" | "existing_run_id" => "[RUN_ID]",
            "bytes" | "size_bytes" => "[SIZE]",
            "record_version" => "[N]",
            "platform" => "[PLATFORM]",
            "glibc" => "[VER]",
            // tool versions are strings; workflow versions (numbers) stay
            "version" if x.is_string() => "[VER]",
            k if k == "ms" || k.ends_with("_ms") => "[MS]",
            k if k.ends_with("_at") => "[TIMESTAMP]",
            _ => return None,
        })
    }
    fn walk(v: &Value, tmp: &str) -> Value {
        match v {
            Value::Object(m) => Value::Object(
                m.iter()
                    .map(|(k, x)| {
                        let nv = match by_key(k, x) {
                            Some(p) => Value::String(p.to_owned()),
                            None => walk(x, tmp),
                        };
                        (k.clone(), nv)
                    })
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(|x| walk(x, tmp)).collect()),
            Value::String(s) if is_sha(s) => Value::String("[SHA]".into()),
            Value::String(s) if is_abs_path(s) => Value::String("[PATH]".into()),
            Value::String(s) if !tmp.is_empty() && s.contains(tmp) => {
                Value::String(s.replace(tmp, "[PATH]"))
            }
            other => other.clone(),
        }
    }
    walk(v, tmp)
}

/// `doctor` reports host probes that differ between machines (a C compiler, Docker,
/// musl, warnings about them): their values are replaced, their keys stay.
fn redact_doctor_host(v: &mut Value) {
    for probe in ["cc", "docker"] {
        if let Some(o) = v.get_mut(probe).and_then(Value::as_object_mut) {
            for (k, x) in o.iter_mut() {
                *x = Value::String(match k.as_str() {
                    "present" => "[BOOL]".into(),
                    "path" => "[PATH]".into(),
                    _ => continue,
                });
            }
        }
    }
    if let Some(x) = v.get_mut("musl") {
        *x = Value::String("[BOOL]".into());
    }
    if let Some(x) = v.get_mut("glibc") {
        *x = Value::String("[VER]".into());
    }
    if let Some(x) = v.get_mut("warnings") {
        assert!(x.is_array(), "doctor warnings: {x}");
        *x = Value::String("[WARNINGS]".into());
    }
}

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).expect("json")
}

// ---------------------------------------------------------------------------------
// local verbs
// ---------------------------------------------------------------------------------

#[test]
fn offline_local_verbs() {
    let root = tempfile::tempdir().expect("tempdir");
    let tmp = root.path().display().to_string();
    let tc = root.path().join("toolchains");
    let tc_s = tc.display().to_string();
    let env: &[(&str, &str)] = &[("OSTRO_TOOLCHAIN_DIR", &tc_s)];

    // init (rust)
    let (code, v) = local(
        root.path(),
        &["init", "proj", "--lang", "rust"],
        env,
        "init",
    );
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["name"], "proj");
    insta::assert_snapshot!("init", pretty(&redact(&v, &tmp)));

    // toolchain list (an empty toolchain dir: nothing installed)
    let (code, v) = local(root.path(), &["toolchain", "list"], env, "toolchain");
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["action"], "list");
    insta::assert_snapshot!("toolchain_list", pretty(&redact(&v, &tmp)));

    // doctor (exit 0 even with warnings)
    let (code, v) = local(root.path(), &["doctor"], env, "doctor");
    assert_eq!(code, 0, "{v}");
    let mut v = redact(&v, &tmp);
    redact_doctor_host(&mut v);
    insta::assert_snapshot!("doctor", pretty(&v));
}

#[test]
fn build_offline_fixture() {
    let root = tempfile::tempdir().expect("tempdir");
    let tmp = root.path().display().to_string();
    let t = root.path().join("t");
    copy_tree(&fixture(), &t);
    let env: &[(&str, &str)] = &[("CARGO_NET_OFFLINE", "true")];
    let (code, v) = local(root.path(), &["build", arg(&t)], env, "build");
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["up_to_date"], false);
    insta::assert_snapshot!("build", pretty(&redact(&v, &tmp)));
}

/// `deploy DIR` and `replay --module` against one spawned dev server; the `.wasm` form
/// of deploy keeps the REST-proxy JSON (no envelope).
#[test]
fn deploy_project_and_replay_against_dev() {
    let dev = p10::spawn_dev(&[], &[]);
    let server = dev.url();
    let root = tempfile::tempdir().expect("tempdir");
    let tmp = root.path().display().to_string();
    let env: &[(&str, &str)] = &[("OSTRO_SERVER", &server), ("CARGO_NET_OFFLINE", "true")];

    // deploy DIR: build + upload + activate.
    let p = root.path().join("p");
    copy_tree(&fixture(), &p);
    let (code, v) = local(root.path(), &["deploy", arg(&p)], env, "deploy");
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["build"]["name"], "hello-offline", "{v}");
    insta::assert_snapshot!("deploy_project", pretty(&redact(&v, &tmp)));

    // deploy x.wasm: the 05-14 `{upload, activated}` body, no `kind`.
    let v1 = root.path().join("replay-v1.wasm");
    std::fs::write(&v1, common::guest_wasm("p10-replay-v1")).expect("write wasm");
    let (code, v, err) = p10::cli_json_in(root.path(), &["deploy", arg(&v1), "--activate"], env);
    assert_eq!(code, 0, "{v}\n{err}");
    assert!(v.get("kind").is_none(), "REST-proxy deploy has a kind: {v}");
    assert!(v.get("version").is_none(), "{v}");
    assert!(v["upload"].is_object() && v["activated"].is_array(), "{v}");

    // One replay_demo run, then replay it against the module that recorded it.
    let (code, v, err) = p10::cli_json_in(root.path(), &["run", "replay_demo", "--wait"], env);
    assert_eq!(code, 0, "{v}\n{err}");
    let (code, v) = local(
        root.path(),
        &["replay", "--module", arg(&v1), "--sample", "1"],
        env,
        "replay",
    );
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["summary"]["ok"], 1, "{v}");
    insta::assert_snapshot!("replay", pretty(&redact(&v, &tmp)));
}

// ---------------------------------------------------------------------------------
// exit codes vs docs/cli-json.md
// ---------------------------------------------------------------------------------

/// The rows `| <code> | <meaning> |` of the `## Exit codes` section of docs/cli-json.md.
fn doc_exit_codes() -> Vec<(i32, String)> {
    let path = common::repo_root().join("docs/cli-json.md");
    let doc = std::fs::read_to_string(&path).expect("docs/cli-json.md");
    let mut rows = Vec::new();
    let mut in_section = false;
    for line in doc.lines() {
        if line.starts_with("## ") {
            in_section = line.trim() == "## Exit codes";
            continue;
        }
        if !in_section {
            continue;
        }
        let cells: Vec<&str> = line
            .trim()
            .strip_prefix('|')
            .and_then(|l| l.strip_suffix('|'))
            .map(|l| l.split('|').map(str::trim).collect())
            .unwrap_or_default();
        if let [code, meaning] = cells[..]
            && let Ok(code) = code.parse::<i32>()
        {
            rows.push((code, meaning.replace('`', "")));
        }
    }
    rows
}

#[test]
fn docs_exit_code_table_matches() {
    let want: Vec<(i32, String)> = ostro_cli::output::EXIT_CODES
        .iter()
        .map(|(c, m)| (*c, (*m).to_owned()))
        .collect();
    assert_eq!(
        doc_exit_codes(),
        want,
        "docs/cli-json.md `## Exit codes` table"
    );
}
