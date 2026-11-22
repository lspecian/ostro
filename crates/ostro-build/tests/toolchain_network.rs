//! 10-07 real-network smoke (polyglot only, run by the ci-local "ostro-build polyglot"
//! step): installs the pinned uv and componentize-py wheel for this platform from their
//! official URLs into the shared, reused `target/ostro-toolchains-test` cache, then runs the
//! verified uv. Fails loudly without network.

#![cfg(feature = "polyglot")]

use std::path::PathBuf;
use std::time::Instant;

use ostro_build::cache::Cache;
use ostro_build::fetch::FetchOptions;
use ostro_build::toolchain::{Platform, Tool, ensure, toolchain_set};

fn cache_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/ostro-toolchains-test")
}

#[tokio::test]
async fn installs_pinned_uv_and_componentize_py_wheel() {
    let platform = Platform::detect().expect("supported platform");
    let set = toolchain_set(&platform).expect("pinned set");
    let cache = Cache::with_root(cache_root());
    // the official URLs (no mirror), never offline: this test exists to hit the network
    let opts = FetchOptions::new().expect("fetch options");

    let uv = set.pin(Tool::Uv).expect("uv pin");
    let start = Instant::now();
    let cached = cache.is_installed(uv);
    let dir = ensure(&cache, uv, &opts).await.expect("install uv");
    eprintln!(
        "uv {} ({}) in {:?}",
        uv.version,
        if cached { "cached" } else { "downloaded" },
        start.elapsed()
    );
    let out = std::process::Command::new(dir.join("uv"))
        .arg("--version")
        .env("UV_NO_CONFIG", "1")
        .output()
        .expect("run uv");
    assert!(out.status.success(), "uv --version failed: {out:?}");
    let version = String::from_utf8_lossy(&out.stdout);
    assert!(
        version.starts_with("uv 0.7.15"),
        "unexpected uv version: {version}"
    );

    let whl = set.pin(Tool::ComponentizePy).expect("componentize-py pin");
    let start = Instant::now();
    let cached = cache.is_installed(whl);
    let dir = ensure(&cache, whl, &opts)
        .await
        .expect("install componentize-py wheel");
    eprintln!(
        "componentize-py {} ({}) in {:?}",
        whl.version,
        if cached { "cached" } else { "downloaded" },
        start.elapsed()
    );
    let file = dir.join(whl.file_name());
    assert!(file.is_file(), "{} missing", file.display());
    assert!(file.to_string_lossy().ends_with(".whl"));

    // a second ensure is a cache hit
    let start = Instant::now();
    ensure(&cache, uv, &opts).await.expect("cache hit");
    eprintln!("cache hit in {:?}", start.elapsed());
}
