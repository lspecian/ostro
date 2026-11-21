//! Shared integration-test helpers.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

/// Repository root (`crates/ostro-engine/../..`).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Build the `guests/` workspace once per process and return the named guest's `.wasm`.
///
/// Runs `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml
/// --target-dir target/guests`, isolated from the outer cargo's target-specific env.
pub fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
            ])
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("failed to spawn cargo for guests build");
        if !out.status.success() {
            panic!(
                "guests build failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    });
    let file = format!("{}.wasm", name.replace('-', "_"));
    let path = target_dir.join("wasm32-wasip2/release").join(&file);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The named `guests/` member built path-neutrally (`scripts/corpus-guest-build.sh`, own
/// target dir `target/guests-corpus`, shared with `record_corpus` and the crash campaign).
///
/// A plain build embeds the absolute paths of the cargo registry, the toolchain's rust-src
/// and the checkout in panic-location strings, so its bytes (and the `module_hash` journaled
/// in `RunStarted`) depend on the build host's home directory. Anything that pins module
/// bytes across machines (the golden journal snapshot) must use this build.
pub fn neutral_guest_wasm(name: &str) -> Vec<u8> {
    use std::collections::HashMap;
    use std::sync::Mutex;
    static BUILT: OnceLock<Mutex<HashMap<String, Vec<u8>>>> = OnceLock::new();
    let mut built = BUILT
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(w) = built.get(name) {
        return w.clone();
    }
    let root = repo_root();
    let target = root.join("target/guests-corpus");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let out = Command::new("bash")
        .arg(root.join("scripts/corpus-guest-build.sh"))
        .arg(root.join("guests/Cargo.toml"))
        .arg(&target)
        .args(["-p", name])
        .env("CARGO", cargo)
        .output()
        .expect("spawn scripts/corpus-guest-build.sh (guests)");
    assert!(
        out.status.success(),
        "path-neutral guests build failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let path = target
        .join("wasm32-wasip2/release")
        .join(format!("{}.wasm", name.replace('-', "_")));
    let w = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    built.insert(name.to_owned(), w.clone());
    w
}

/// Engine built from the production config.
pub fn test_engine() -> wasmtime::Engine {
    wasmtime::Engine::new(&ostro_engine::config::engine_config()).expect("engine")
}
