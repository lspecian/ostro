//! The vendored htmx file is byte-exact with the sha256 recorded in VENDORED.md.

use sha2::{Digest, Sha256};

const VENDOR_PATH: &str = "crates/ostro-ui/assets/vendor/htmx-2.0.11.min.js";

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

/// The sha256 in the VENDORED.md row for `VENDOR_PATH`.
fn recorded_sha256() -> String {
    let md = std::fs::read_to_string(workspace_root().join("VENDORED.md")).expect("VENDORED.md");
    let row = md
        .lines()
        .find(|l| l.starts_with(&format!("| {VENDOR_PATH} |")))
        .unwrap_or_else(|| panic!("no VENDORED.md row for {VENDOR_PATH}"));
    let hex: Vec<&str> = row
        .split('|')
        .map(str::trim)
        .filter(|c| c.len() == 64 && c.bytes().all(|b| b.is_ascii_hexdigit()))
        .collect();
    assert_eq!(hex.len(), 1, "exactly one sha256 cell in: {row}");
    hex[0].to_ascii_lowercase()
}

#[test]
fn htmx_sha256_matches_vendored_md() {
    let bytes = std::fs::read(workspace_root().join(VENDOR_PATH)).expect("vendored htmx");
    let got: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        got,
        recorded_sha256(),
        "{VENDOR_PATH} differs from VENDORED.md"
    );
}

#[test]
fn htmx_looks_like_htmx_and_is_small() {
    let bytes = std::fs::read(workspace_root().join(VENDOR_PATH)).expect("vendored htmx");
    assert!(
        bytes.starts_with(b"var htmx") || bytes.starts_with(b"/*"),
        "unexpected start of {VENDOR_PATH}"
    );
    assert!(bytes.len() < 100 * 1024, "{} bytes", bytes.len());
    assert!(
        std::str::from_utf8(&bytes)
            .expect("utf8")
            .contains("version:\"2.0.11\""),
        "not htmx 2.0.11"
    );
}
