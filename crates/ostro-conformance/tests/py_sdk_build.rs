//! Python SDK guest build (09-09 Task 3, `--features polyglot`): the SDK example
//! `sdks/python/examples/hello` builds with `build-guest.sh py`, imports only
//! `ostro:runtime/*@0.1.0` and exports `workflows` plus componentize-py's `exports` hook.
//! Needs uv 0.7.15; a missing toolchain fails loudly. The first real-engine run of
//! SDK-built Python guests is 09-12 (after ENG-1 in 09-07).
#![cfg(feature = "polyglot")]

use std::collections::BTreeSet;

use ostro_conformance::inspect::inspect;
use ostro_conformance::polyglot::{Mode, build};
use ostro_conformance::repo_root;

/// Allowed imports. `types` is a type-only interface that componentize-py imports
/// alongside the four function interfaces (it carries `payload`, `failure`, ...).
const ALLOWED_IMPORTS: &[&str] = &[
    "ostro:runtime/types@0.1.0",
    "ostro:runtime/context@0.1.0",
    "ostro:runtime/journal@0.1.0",
    "ostro:runtime/effects@0.1.0",
    "ostro:runtime/log@0.1.0",
];
const WORKFLOWS: &str = "ostro:runtime/workflows@0.1.0";

/// S9 `py-asyncio` (raw bindings + the SDK's stdlib import set), for comparison.
const S9_PY_ASYNCIO_BYTES: u64 = 21_820_473;
/// 09-02's inspector count for the raw `py-min` fixture (Instantiate + FromExports).
const PY_MIN_CORE_INSTANCES: u32 = 111;

#[test]
fn python_sdk_hello_builds_with_ostro_only_imports() {
    let dir = repo_root().join("sdks/python/examples/hello");
    let path = build(Mode::Py, &dir).unwrap_or_else(|e| panic!("{e}"));
    let bytes = std::fs::read(&path).expect("read component");
    let info = inspect(&bytes).unwrap_or_else(|e| panic!("inspect {}: {e}", path.display()));
    let size = bytes.len() as u64;
    let growth = (size as f64 / S9_PY_ASYNCIO_BYTES as f64 - 1.0) * 100.0;
    println!(
        "python sdk hello: bytes={size} ({growth:+.1}% vs S9 py-asyncio {S9_PY_ASYNCIO_BYTES}) \
         core_instances={} (py-min {PY_MIN_CORE_INSTANCES}) core_modules={} imports={:?} exports={:?}",
        info.core_instances, info.core_modules, info.imports, info.exports
    );

    for imp in &info.imports {
        assert!(
            ALLOWED_IMPORTS.contains(&imp.as_str()),
            "unexpected import {imp} (all: {:?})",
            info.imports
        );
    }
    for needed in [
        "ostro:runtime/context@0.1.0",
        "ostro:runtime/journal@0.1.0",
        "ostro:runtime/effects@0.1.0",
        "ostro:runtime/log@0.1.0",
    ] {
        assert!(
            info.imports.iter().any(|i| i == needed),
            "missing import {needed} (all: {:?})",
            info.imports
        );
    }
    let exports: BTreeSet<&str> = info.exports.iter().map(String::as_str).collect();
    assert_eq!(exports, BTreeSet::from([WORKFLOWS, "exports"]));
    assert!(info.core_instances >= 1);
}
