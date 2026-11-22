//! SC3 (plan 10-11, CLI-07): `ostro replay` as a CI gate on the real binary, per language.
//!
//! - `rust_replay_gate`: `ostro dev --ephemeral`, `deploy v1 --activate`, three
//!   `run replay_demo --wait`; `replay --module v2 --sample 5 --json` exits 4 with the
//!   `ostro.replay.result` envelope, ≥ 3 diverged verdicts, each with a seq;
//!   `--module v3` (the swap behind `patched("reorder")`) exits 0; `--run <id> --module
//!   v2` exits 4; positional ids merge with `--run`; a module without the workflow is
//!   `skipped` = exit 4, 0 with `--allow-missing`.
//! - `corpus_offline`: `replay --corpus tests/replay-corpus` exits 0 (every entry `ok`);
//!   the `patch-v2` entry replayed with `--module patch-v1` (v1 sleeps where the journal
//!   has step C) exits 4.
//! - `python_replay_gate` / `ts_replay_gate` (`--features polyglot`): the same gate on
//!   SDK fixtures built with `ostro_conformance::polyglot::build` (`build-guest.sh`).

mod common;
#[path = "common/p10.rs"]
mod p10;

use std::path::{Path, PathBuf};

use serde_json::Value;

use p10::*;

fn write(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, bytes).expect("write wasm");
    p
}

fn arg(p: &Path) -> &str {
    p.to_str().expect("utf-8 path")
}

fn verdicts(v: &Value) -> &Vec<Value> {
    v["verdicts"]
        .as_array()
        .unwrap_or_else(|| panic!("verdicts: {v}"))
}

/// The verdict table of a JSON replay result, for the test log (`--nocapture`).
fn show(label: &str, v: &Value) {
    println!("--- {label}: summary {}", v["summary"]);
    for r in verdicts(v) {
        println!(
            "  {}  {}  {}  seq={}  {}",
            r["run_id"].as_str().unwrap_or("-"),
            r["workflow"].as_str().unwrap_or("-"),
            r["verdict"].as_str().unwrap_or("-"),
            r["seq"],
            r["detail"].as_str().unwrap_or("-"),
        );
    }
}

/// The SC3 gate for one language: `wf` is the workflow, `v1/v2/v3` the module files.
fn gate(wf: &str, v1: &Path, v2: &Path, v3: &Path) {
    let d = spawn_dev(&[], &[]);
    let (code, out, err) = d.cli(&["deploy", arg(v1), "--activate"]);
    assert_eq!(code, 0, "deploy v1: {out}{err}");
    let mut ids = Vec::new();
    for _ in 0..3 {
        let (code, v, err) = d.cli_json(&["run", wf, "--wait"]);
        assert_eq!(code, 0, "run {wf}: {v}{err}");
        ids.push(
            v["start"]["run_id"]
                .as_str()
                .unwrap_or_else(|| panic!("run_id: {v}"))
                .to_owned(),
        );
    }

    // v2: steps swapped, no patch → exit 4 with the divergence seq.
    let (code, v, err) = d.cli_json(&["replay", "--module", arg(v2), "--sample", "5"]);
    show(&format!("{wf} v2"), &v);
    assert_eq!(code, 4, "replay v2: {v}{err}");
    assert_eq!(v["kind"], "ostro.replay.result", "{v}");
    assert_eq!(v["version"], 1, "{v}");
    assert!(
        v["module_sha256"].as_str().is_some_and(|s| s.len() == 64),
        "{v}"
    );
    assert!(v["summary"]["diverged"].as_u64() >= Some(3), "{v}");
    for r in verdicts(&v) {
        assert_eq!(r["verdict"], "diverged", "{r}");
        assert!(r["seq"].as_u64().is_some(), "diverged without a seq: {r}");
    }
    assert!(err.contains("diverged"), "{err}");

    // v3: the swap behind patched("reorder") → exit 0.
    let (code, v, err) = d.cli_json(&["replay", "--module", arg(v3), "--sample", "5"]);
    show(&format!("{wf} v3"), &v);
    assert_eq!(code, 0, "replay v3: {v}{err}");
    assert_eq!(v["summary"]["ok"], 3, "{v}");
    assert_eq!(v["summary"]["diverged"], 0, "{v}");

    // One explicit run against v2: exit 4, human table.
    let (code, out, err) = d.cli(&["replay", "--run", &ids[0], "--module", arg(v2)]);
    assert_eq!(code, 4, "{out}{err}");
    assert!(out.contains("VERDICT") && out.contains("diverged"), "{out}");
    assert!(out.contains(&ids[0]), "{out}");
    assert!(out.contains("1 diverged"), "{out}");

    // Positional ids merge with --run.
    let (code, v, err) = d.cli_json(&["replay", &ids[1], "--run", &ids[2], "--module", arg(v3)]);
    assert_eq!(code, 0, "{v}{err}");
    let got: Vec<&str> = verdicts(&v)
        .iter()
        .map(|r| r["run_id"].as_str().expect("run_id"))
        .collect();
    assert_eq!(got, [ids[1].as_str(), ids[2].as_str()]);

    // A malformed run id is a server error (400): exit 1.
    let (code, _, err) = d.cli(&["replay", "--run", "nope", "--module", arg(v3)]);
    assert_eq!(code, 1, "{err}");
}

#[test]
fn rust_replay_gate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let v1 = write(
        dir.path(),
        "rs-v1.wasm",
        &common::guest_wasm("p10-replay-v1"),
    );
    let v2 = write(
        dir.path(),
        "rs-v2.wasm",
        &common::guest_wasm("p10-replay-v2"),
    );
    let v3 = write(
        dir.path(),
        "rs-v3.wasm",
        &common::guest_wasm("p10-replay-v3"),
    );
    gate("replay_demo", &v1, &v2, &v3);

    // A module without replay_demo: skipped → exit 4, 0 with --allow-missing.
    let other = write(dir.path(), "other.wasm", &common::guest_wasm("sdk-basic"));
    let d = spawn_dev(&[], &[]);
    let (code, out, err) = d.cli(&["deploy", arg(&v1), "--activate"]);
    assert_eq!(code, 0, "{out}{err}");
    let (code, v, err) = d.cli_json(&["run", "replay_demo", "--wait"]);
    assert_eq!(code, 0, "{v}{err}");
    let id = v["start"]["run_id"].as_str().expect("run_id").to_owned();
    let (code, v, err) = d.cli_json(&["replay", &id, "--module", arg(&other)]);
    assert_eq!(code, 4, "{v}{err}");
    assert_eq!(v["summary"]["skipped"], 1, "{v}");
    assert!(err.contains("--allow-missing"), "{err}");
    let (code, v, err) = d.cli_json(&["replay", &id, "--module", arg(&other), "--allow-missing"]);
    assert_eq!(code, 0, "{v}{err}");
}

#[test]
fn corpus_offline() {
    let root = common::repo_root().join("tests/replay-corpus");
    let cwd = tempfile::tempdir().expect("tempdir");
    let start = std::time::Instant::now();
    let (code, v, err) = cli_json_in(cwd.path(), &["replay", "--corpus", arg(&root)], &[]);
    println!(
        "corpus: {} entries replayed offline in {:?}: {}",
        verdicts(&v).len(),
        start.elapsed(),
        v["summary"]
    );
    assert_eq!(code, 0, "{v}{err}");
    assert_eq!(v["kind"], "ostro.replay.result");
    assert!(verdicts(&v).len() >= 24, "{v}");
    assert_eq!(
        v["summary"]["ok"].as_u64(),
        Some(verdicts(&v).len() as u64),
        "{v}"
    );
    assert!(v["module_sha256"].is_null(), "{v}");
    // Human output names the entries.
    let (code, out, err) = run_cli_in(cwd.path(), &["replay", "--corpus", arg(&root)], &[]);
    assert_eq!(code, 0, "{err}");
    assert!(
        out.contains("det-basic") && out.contains(" 0 diverged"),
        "{out}"
    );

    // patch-v2's journal (A, C, sleep, B under patched("add-c")) replayed by patch-v1
    // (A, sleep, B): v1 sleeps where the journal has step C → exit 4.
    let tmp = tempfile::tempdir().expect("tempdir");
    let entry = tmp.path().join("patch-v2");
    std::fs::create_dir_all(&entry).expect("mkdir");
    for f in ["entry.toml", "journal.bin", "module.wasm"] {
        std::fs::copy(root.join("patch-v2").join(f), entry.join(f)).expect("copy entry");
    }
    let v1 = write(cwd.path(), "patch-v1.wasm", &common::guest_wasm("patch-v1"));
    let (code, v, err) = cli_json_in(
        cwd.path(),
        &["replay", "--corpus", arg(tmp.path()), "--module", arg(&v1)],
        &[],
    );
    show("corpus patch-v2 with patch-v1", &v);
    assert_eq!(code, 4, "{v}{err}");
    let r = &verdicts(&v)[0];
    assert_eq!(r["entry"], "patch-v2", "{r}");
    assert_eq!(r["verdict"], "diverged", "{r}");
    assert!(r["seq"].as_u64().is_some(), "{r}");
    // ... and with its own module it is ok.
    let (code, v, err) = cli_json_in(cwd.path(), &["replay", "--corpus", arg(tmp.path())], &[]);
    assert_eq!(code, 0, "{v}{err}");
}

#[cfg(feature = "polyglot")]
fn fixture(mode: ostro_conformance::polyglot::Mode, dir: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/replay")
        .join(dir);
    ostro_conformance::polyglot::build(mode, &d)
        .unwrap_or_else(|e| panic!("polyglot build of {dir} failed (Node 24 + uv 0.7.15): {e}"))
}

#[cfg(feature = "polyglot")]
#[test]
fn python_replay_gate() {
    use ostro_conformance::polyglot::Mode;
    let v1 = fixture(Mode::Py, "py-v1");
    let v2 = fixture(Mode::Py, "py-v2");
    let v3 = fixture(Mode::Py, "py-v3");
    gate("py_replay_demo", &v1, &v2, &v3);
}

#[cfg(feature = "polyglot")]
#[test]
fn ts_replay_gate() {
    use ostro_conformance::polyglot::Mode;
    let v1 = fixture(Mode::Ts, "ts-v1");
    let v2 = fixture(Mode::Ts, "ts-v2");
    let v3 = fixture(Mode::Ts, "ts-v3");
    gate("ts_replay_demo", &v1, &v2, &v3);
}
