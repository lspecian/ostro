//! Polyglot smoke (09-02, `--features polyglot`): the raw-binding fixtures build in all three
//! modes and have the expected import/export shape. Needs uv 0.7.15 and Node 24; a missing
//! toolchain fails loudly.
#![cfg(feature = "polyglot")]

use std::collections::BTreeSet;
use std::path::PathBuf;

use ostro_conformance::inspect::{ComponentInfo, inspect};
use ostro_conformance::polyglot::{Mode, build, script_runs};
use ostro_conformance::repo_root;
use sha2::{Digest, Sha256};

const OSTRO_IMPORTS: &[&str] = &[
    "ostro:runtime/types@0.1.0",
    "ostro:runtime/context@0.1.0",
    "ostro:runtime/journal@0.1.0",
    "ostro:runtime/effects@0.1.0",
    "ostro:runtime/log@0.1.0",
];
const WORKFLOWS: &str = "ostro:runtime/workflows@0.1.0";

fn fixture(name: &str) -> PathBuf {
    repo_root().join("conformance/fixtures").join(name)
}

fn build_and_inspect(mode: Mode, name: &str) -> (PathBuf, ComponentInfo) {
    let path = build(mode, &fixture(name)).unwrap_or_else(|e| panic!("{e}"));
    let bytes = std::fs::read(&path).expect("read component");
    let info = inspect(&bytes).unwrap_or_else(|e| panic!("inspect {}: {e}", path.display()));
    let sha: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    println!(
        "{name} [{}]: bytes={} sha256={sha} core_instances={} core_modules={} imports={:?} exports={:?}",
        mode.arg(),
        bytes.len(),
        info.core_instances,
        info.core_modules,
        info.imports,
        info.exports
    );
    (path, info)
}

fn assert_ostro_only(info: &ComponentInfo, what: &str) {
    for imp in &info.imports {
        assert!(
            OSTRO_IMPORTS.contains(&imp.as_str()),
            "{what}: unexpected import {imp} (all: {:?})",
            info.imports
        );
    }
    assert!(
        !info.imports.iter().any(|i| i.starts_with("wasi:")),
        "{what}: WASI imports {:?}",
        info.imports
    );
}

#[test]
fn python_raw_fixture_shape() {
    let (_, info) = build_and_inspect(Mode::Py, "py-min");
    assert_ostro_only(&info, "py-min");
    let exports: BTreeSet<&str> = info.exports.iter().map(String::as_str).collect();
    assert_eq!(exports, BTreeSet::from([WORKFLOWS, "exports"]));
    assert!(info.core_instances >= 1);
}

#[test]
fn ts_quickjs_raw_fixture_shape() {
    let (_, info) = build_and_inspect(Mode::Ts, "ts-min");
    // SDK-03: the QuickJS build imports only ostro:runtime/* (zero wasi: imports).
    assert_ostro_only(&info, "ts-min (quickjs)");
    assert_eq!(info.exports, vec![WORKFLOWS.to_owned()]);
    assert!(info.core_instances >= 1);
}

#[test]
fn ts_starlingmonkey_raw_fixture_shape() {
    let (_, info) = build_and_inspect(Mode::TsSm, "ts-min");
    assert_ostro_only(&info, "ts-min (starlingmonkey)");
    assert!(
        info.exports.iter().any(|e| e == WORKFLOWS),
        "{:?}",
        info.exports
    );
    assert!(info.core_instances >= 1);
}

#[test]
fn unchanged_fixture_is_a_cache_hit() {
    let first = build(Mode::Ts, &fixture("ts-min")).unwrap_or_else(|e| panic!("{e}"));
    let runs = script_runs();
    let second = build(Mode::Ts, &fixture("ts-min")).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(first, second);
    assert_eq!(
        script_runs(),
        runs,
        "second build of ts-min ran build-guest.sh"
    );
}
