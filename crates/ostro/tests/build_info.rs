//! `ostro build-info` (plan 10-10, B3): `--json` prints exactly
//! `{"version", "target", "allocator"}` (the test kits read `version`); the human line is
//! unchanged: `ostro <version> target=<triple> allocator=mimalloc[ failpoints=on]`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p10.rs"]
mod p10;

use p10::run_cli_in;
use serde_json::Value;

#[test]
fn build_info_json_has_version_target_allocator() {
    let cwd = tempfile::tempdir().expect("tempdir");
    let (code, out, err) = run_cli_in(cwd.path(), &["build-info", "--json"], &[]);
    assert_eq!(code, 0, "{err}");
    let v: Value = serde_json::from_str(out.trim()).expect("json");
    let mut keys: Vec<&str> = v
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["allocator", "target", "version"], "{v}");
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"), "{v}");
    assert_eq!(v["allocator"], "mimalloc", "{v}");
    assert!(v["target"].as_str().is_some_and(|t| !t.is_empty()), "{v}");
}

#[test]
fn build_info_human_line_is_unchanged() {
    let cwd = tempfile::tempdir().expect("tempdir");
    let (code, out, err) = run_cli_in(cwd.path(), &["build-info"], &[]);
    assert_eq!(code, 0, "{err}");
    let (_, json, _) = run_cli_in(cwd.path(), &["build-info", "--json"], &[]);
    let v: Value = serde_json::from_str(json.trim()).expect("json");
    let target = v["target"].as_str().expect("target");
    let line = out.trim_end();
    let want = format!(
        "ostro {} target={target} allocator=mimalloc",
        env!("CARGO_PKG_VERSION")
    );
    assert!(
        line == want || line == format!("{want} failpoints=on"),
        "{line:?} vs {want:?}"
    );
    assert_eq!(out.lines().count(), 1, "{out}");
}
