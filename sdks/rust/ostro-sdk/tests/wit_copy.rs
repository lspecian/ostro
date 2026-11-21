//! The SDK's vendored WIT (`wit/runtime.wit`, D-P9-29) must stay byte-identical to the frozen
//! `wit/ostro-runtime-0.1.0/runtime.wit` at the repository root: the bindings are generated
//! from the copy so the crate is self-contained, and the freeze is checked on the original.

use std::path::PathBuf;

#[test]
fn vendored_wit_is_byte_identical_to_the_frozen_source() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let copy = crate_dir.join("wit/runtime.wit");
    let source = crate_dir.join("../../../wit/ostro-runtime-0.1.0/runtime.wit");
    let a = std::fs::read(&copy).unwrap_or_else(|e| panic!("read {}: {e}", copy.display()));
    let b = std::fs::read(&source).unwrap_or_else(|e| panic!("read {}: {e}", source.display()));
    assert!(!a.is_empty(), "vendored WIT is empty");
    assert!(
        a == b,
        "sdks/rust/ostro-sdk/wit/runtime.wit differs from wit/ostro-runtime-0.1.0/runtime.wit; \
         copy the frozen file byte for byte"
    );
}

#[test]
fn bindings_generate_from_the_vendored_copy() {
    let src =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/bindings.rs"))
            .expect("read bindings.rs");
    assert!(
        src.contains(r#"path: "wit","#),
        "bindings.rs must generate from \"wit\""
    );
    assert!(
        !src.contains("../../../wit"),
        "bindings.rs must not reach outside the crate"
    );
}
