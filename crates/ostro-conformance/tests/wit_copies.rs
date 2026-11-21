//! The SDK WIT copies are byte-identical to the frozen `wit/ostro-runtime-0.1.0/runtime.wit`
//! (09-02; D-P9-29), and the source still matches `wit/FROZEN.toml`.

use ostro_conformance::repo_root;
use sha2::{Digest, Sha256};

const SOURCE: &str = "wit/ostro-runtime-0.1.0/runtime.wit";

fn read(rel: &str) -> Vec<u8> {
    let p = repo_root().join(rel);
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn python_copy_is_byte_identical() {
    assert!(
        read("sdks/python/ostro/wit/runtime.wit") == read(SOURCE),
        "sdks/python/ostro/wit/runtime.wit differs from {SOURCE}"
    );
}

#[test]
fn ts_copy_is_byte_identical() {
    assert!(
        read("sdks/ts/wit/runtime.wit") == read(SOURCE),
        "sdks/ts/wit/runtime.wit differs from {SOURCE}"
    );
}

#[test]
fn rust_sdk_copy_is_byte_identical_when_present() {
    let rel = "sdks/rust/ostro-sdk/wit/runtime.wit";
    let path = repo_root().join(rel);
    if !path.exists() {
        // 09-06 (same wave) vendors this copy; the 09-20 gate requires it present.
        eprintln!(
            "NOTE: {rel} is absent (created by plan 09-06; required present at the 09-20 gate)"
        );
        return;
    }
    assert!(read(rel) == read(SOURCE), "{rel} differs from {SOURCE}");
}

#[test]
fn source_matches_frozen_toml() {
    let toml = String::from_utf8(read("wit/FROZEN.toml")).expect("utf-8");
    let mut in_section = false;
    let mut pinned = None;
    for line in toml.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_section = line == "[ostro-runtime-0_1_0]";
        } else if in_section && let Some(v) = line.strip_prefix("sha256") {
            pinned = Some(
                v.trim()
                    .trim_start_matches('=')
                    .trim()
                    .trim_matches('"')
                    .to_owned(),
            );
        }
    }
    let pinned = pinned.expect("wit/FROZEN.toml: [ostro-runtime-0_1_0] sha256");
    assert_eq!(hex(&Sha256::digest(read(SOURCE))), pinned);
}
