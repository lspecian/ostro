//! WIT 0.1 freeze pins (plan 05-05, WIT-07).
//!
//! - `frozen_0_1_0_is_byte_identical`: `wit/ostro-runtime-0.1.0/runtime.wit` must hash to the
//!   sha256 recorded in `wit/FROZEN.toml`. **Enforced** since plan 05-20 (2026-11-19) created
//!   `wit/FROZEN.toml`. Comments count: the whole file is pinned, so even a doc-comment edit
//!   fails this test. (Before the freeze, a missing `FROZEN.toml` only printed the hash.)
//! - `runtime_0_1_chain_is_additive`: every `wit/ostro-runtime-0.1.N` package is additive over
//!   `0.1.(N-1)` ([`ostro_witcheck::check_chain`]). Today only 0.1.0 exists, so it checks
//!   nothing; `chain_check_on_fixture_trees` proves the mechanism on fixture trees.
//!
//! 05-20 created `wit/FROZEN.toml` (format below) and the local tag `wit/ostro-runtime-v0.1.0`.
//! The 0.1.0 pin never changes; new surface goes into a new `0.1.N` directory, which the
//! chain test then checks.
//!
//! ```toml
//! [ostro-runtime-0_1_0]
//! sha256 = "<64 hex>"
//! ```

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

const WIT_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../wit");
const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wit-fixtures");

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Minimal reader for the one-key-per-section `FROZEN.toml` (no toml dependency).
fn frozen_sha(frozen_toml: &str, section: &str) -> Option<String> {
    let header = format!("[{section}]");
    let mut in_section = false;
    for line in frozen_toml.lines().map(str::trim) {
        if line.starts_with('[') {
            in_section = line == header;
        } else if in_section && let Some(v) = line.strip_prefix("sha256") {
            let v = v.trim_start().strip_prefix('=')?.trim();
            return Some(v.trim_matches('"').to_string());
        }
    }
    None
}

#[test]
fn frozen_0_1_0_is_byte_identical() {
    let wit = Path::new(WIT_ROOT).join("ostro-runtime-0.1.0/runtime.wit");
    let bytes = std::fs::read(&wit).expect("read runtime.wit");
    let got = hex(&Sha256::digest(&bytes));
    eprintln!("wit/ostro-runtime-0.1.0/runtime.wit sha256 = {got}");
    let frozen = Path::new(WIT_ROOT).join("FROZEN.toml");
    match std::fs::read_to_string(&frozen) {
        Ok(toml) => {
            let want = frozen_sha(&toml, "ostro-runtime-0_1_0")
                .expect("wit/FROZEN.toml: [ostro-runtime-0_1_0] sha256 missing");
            assert_eq!(
                got, want,
                "wit/ostro-runtime-0.1.0/runtime.wit changed after the freeze; \
                 add a new ostro-runtime-0.1.N package instead"
            );
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("pre-freeze: wit/FROZEN.toml absent, byte identity not enforced (05-20)");
        }
        Err(e) => panic!("read wit/FROZEN.toml: {e}"),
    }
}

#[test]
fn frozen_toml_reader() {
    let t = "# pins\n[other]\nsha256 = \"aa\"\n\n[ostro-runtime-0_1_0]\nsha256 = \"bb\"\n";
    assert_eq!(frozen_sha(t, "ostro-runtime-0_1_0").as_deref(), Some("bb"));
    assert_eq!(frozen_sha(t, "other").as_deref(), Some("aa"));
    assert_eq!(frozen_sha(t, "missing"), None);
}

#[test]
fn runtime_0_1_chain_is_additive() {
    let checked = ostro_witcheck::check_chain(Path::new(WIT_ROOT))
        .unwrap_or_else(|e| panic!("non-additive WIT package:\n{e:#?}"));
    let pkgs = ostro_witcheck::runtime_0_1_packages(Path::new(WIT_ROOT)).expect("list wit/");
    assert_eq!(
        pkgs.first().map(|p| p.0),
        Some(0),
        "wit/ostro-runtime-0.1.0 must exist"
    );
    assert_eq!(checked.len(), pkgs.len() - 1);
    eprintln!("checked {} WIT package transition(s)", checked.len());
}

/// Build a `wit/`-like tree from fixture packages: `(dir name, fixture name)`.
fn fixture_tree(name: &str, layout: &[(&str, &str)]) -> PathBuf {
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("wit-chain-{name}"));
    let _ = std::fs::remove_dir_all(&root);
    for (dir, fixture) in layout {
        let d = root.join(dir);
        std::fs::create_dir_all(&d).expect("mkdir");
        std::fs::copy(
            Path::new(FIXTURES).join(fixture).join("runtime.wit"),
            d.join("runtime.wit"),
        )
        .expect("copy fixture");
    }
    root
}

#[test]
fn chain_check_on_fixture_trees() {
    // Only 0.1.0: nothing to check.
    let one = fixture_tree("one", &[("ostro-runtime-0.1.0", "base")]);
    assert_eq!(ostro_witcheck::check_chain(&one).expect("ok").len(), 0);

    // 0.1.0 -> 0.1.1 additive (unrelated dirs are ignored).
    let good = fixture_tree(
        "good",
        &[
            ("ostro-runtime-0.1.0", "base"),
            ("ostro-runtime-0.1.1", "add-func-context"),
            ("not-a-runtime-package", "base"),
        ],
    );
    assert_eq!(
        ostro_witcheck::check_chain(&good).expect("additive").len(),
        1
    );

    // 0.1.0 -> 0.1.1 breaking.
    let bad = fixture_tree(
        "bad",
        &[
            ("ostro-runtime-0.1.0", "base"),
            ("ostro-runtime-0.1.1", "add-record-field"),
        ],
    );
    let errs = ostro_witcheck::check_chain(&bad).expect_err("breaking");
    assert!(
        errs.iter().any(|e| e.message.contains("types.run-info")),
        "{errs:#?}"
    );
}
