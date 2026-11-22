//! Stub (10-02): writes an empty tar archive (1024 zero bytes, two end-of-archive blocks)
//! to `$OUT_DIR/sdk_bundle.tar`. 10-08 replaces it with the real SDK bundler (sha2 for the
//! bundle hash, toml for the bundled manifests).

use sha2 as _;
use toml as _;

fn main() {
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR is set"));
    let mut builder = tar::Builder::new(Vec::new());
    builder.finish().expect("finish empty tar");
    let bytes = builder.into_inner().expect("empty tar bytes");
    assert_eq!(
        bytes.len(),
        1024,
        "an empty tar is two 512-byte zero blocks"
    );
    std::fs::write(out.join("sdk_bundle.tar"), bytes).expect("write sdk_bundle.tar");
    println!("cargo:rerun-if-changed=build.rs");
}
