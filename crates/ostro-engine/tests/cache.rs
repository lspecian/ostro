//! ENG-07: AOT artefact cache (HMAC-authenticated L1 disk cache) and bounded compile pool.

mod common;

use std::path::Path;

use ostro_engine::cache::{DiskCache, cache_key, compat_digest, load_or_create_key, module_sha};
use ostro_engine::host::bindings::Workflow;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store, WasmBacktraceDetails};

/// Ostro config with backtrace details pinned to `Disable` so the compat digest does not
/// depend on `WASMTIME_BACKTRACE_DETAILS` (plan 03-03's `engine_config` should do the same).
fn pinned_config() -> wasmtime::Config {
    let mut c = ostro_engine::config::engine_config();
    c.wasm_backtrace_details(WasmBacktraceDetails::Disable);
    c
}

fn pinned_engine() -> Engine {
    Engine::new(&pinned_config()).expect("engine")
}

/// An engine whose configuration differs (different compat hash).
fn other_engine() -> Engine {
    let mut c = pinned_config();
    c.cranelift_opt_level(wasmtime::OptLevel::None);
    Engine::new(&c).expect("engine")
}

/// Instantiate the smoke guest and call `describe`.
fn assert_smoke_runs(engine: &Engine, component: &Component) {
    let mut linker: Linker<()> = Linker::new(engine);
    linker
        .define_unknown_imports_as_traps(component)
        .expect("linker");
    let run = async {
        let mut store = Store::new(engine, ());
        store.set_fuel(u64::MAX).unwrap();
        store.set_epoch_deadline(u64::MAX);
        let wf = Workflow::instantiate_async(&mut store, component, &linker)
            .await
            .expect("instantiate");
        let metas = wf
            .ostro_runtime_workflows()
            .call_describe(&mut store)
            .await
            .expect("describe");
        assert_eq!(metas[0].name, "smoke");
    };
    match tokio::runtime::Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(run);
        }),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run),
    }
}

fn flip_byte(path: &Path, idx: usize) {
    let mut bytes = std::fs::read(path).unwrap();
    bytes[idx] ^= 0x01;
    std::fs::write(path, bytes).unwrap();
}

// ---------------------------------------------------------------- Task 1

#[test]
fn cache_key_stable() {
    let wasm = common::guest_wasm("smoke");
    let sha = module_sha(&wasm);
    let a = pinned_engine();
    let b = pinned_engine();
    let k1 = cache_key(&a, &sha);
    assert_eq!(k1, cache_key(&a, &sha));
    assert_eq!(k1, cache_key(&b, &sha));
    let (hex_sha, digest) = k1.split_once('-').expect("dash");
    assert_eq!(hex_sha.len(), 64);
    assert_eq!(digest.len(), 16);
    assert!(
        k1.chars()
            .all(|c| c == '-' || c.is_ascii_digit() || ('a'..='f').contains(&c))
    );
    assert_ne!(compat_digest(&a), compat_digest(&other_engine()));
    assert_ne!(k1, cache_key(&other_engine(), &sha));
}

#[test]
fn compat_digest_env_independent() {
    let before = compat_digest(&pinned_engine());
    // SAFETY: test-only; no other test in this binary reads this variable concurrently
    // in a way that affects correctness (all engines here pin backtrace details).
    unsafe { std::env::set_var("WASMTIME_BACKTRACE_DETAILS", "1") };
    let after = compat_digest(&pinned_engine());
    unsafe { std::env::remove_var("WASMTIME_BACKTRACE_DETAILS") };
    assert_eq!(before, after);
}

#[test]
fn hmac_key_created_0600() {
    let dir = tempfile::tempdir().unwrap();
    let k1 = load_or_create_key(dir.path()).unwrap();
    let path = dir.path().join("aot.key");
    assert_eq!(std::fs::read(&path).unwrap(), k1.to_vec());
    assert_ne!(k1, [0u8; 32]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    let k2 = load_or_create_key(dir.path()).unwrap();
    assert_eq!(k1, k2);
}

fn stored_artefact(dir: &Path) -> (Engine, DiskCache, String, std::path::PathBuf) {
    let wasm = common::guest_wasm("smoke");
    let engine = pinned_engine();
    let key = load_or_create_key(dir).unwrap();
    let disk = DiskCache::new(dir, key).unwrap();
    let ck = cache_key(&engine, &module_sha(&wasm));
    let cwasm = engine.precompile_component(&wasm).unwrap();
    disk.put(&ck, &cwasm).unwrap();
    let path = disk.artefact_path(&ck);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len() as usize,
        cwasm.len() + 32
    );
    (engine, disk, ck, path)
}

#[test]
fn cache_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, disk, ck, path) = stored_artefact(dir.path());
    assert_eq!(path, dir.path().join("aot").join(format!("{ck}.cwasm")));
    let component = disk.get(&engine, &ck).expect("hit");
    assert_smoke_runs(&engine, &component);
    assert!(disk.get(&engine, "missing-0000000000000000").is_none());
}

#[test]
fn cache_tamper_detected_body() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, disk, ck, path) = stored_artefact(dir.path());
    let len = std::fs::metadata(&path).unwrap().len() as usize;
    flip_byte(&path, len / 2);
    assert!(disk.get(&engine, &ck).is_none());
    assert!(!path.exists(), "tampered artefact must be deleted");
    assert_eq!(disk.stats().tamper_total, 1);
}

#[test]
fn cache_tamper_detected_trailer() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, disk, ck, path) = stored_artefact(dir.path());
    let len = std::fs::metadata(&path).unwrap().len() as usize;
    flip_byte(&path, len - 5);
    assert!(disk.get(&engine, &ck).is_none());
    assert!(!path.exists());
    assert_eq!(disk.stats().tamper_total, 1);
}

#[test]
fn cache_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, disk, ck, path) = stored_artefact(dir.path());
    std::fs::write(&path, [7u8; 31]).unwrap();
    assert!(disk.get(&engine, &ck).is_none());
    assert!(!path.exists());
    assert_eq!(disk.stats().tamper_total, 1);
}
