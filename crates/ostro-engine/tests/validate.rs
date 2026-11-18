//! ENG-02: deploy-time validation of component imports/exports + instantiate_pre, and
//! the wasi-probe import snapshot (Research Pitfall 5).

mod common;

use ostro_engine::env::{GuestEnv, RandomStream, StdStream};
use ostro_engine::error::EngineError;
use ostro_engine::validate::{ValidatedComponent, validate_component};
use wasmtime::Engine;
use wasmtime::component::{Component, Linker, ResourceTable};

/// Minimal env: validation never runs guest code, it only needs a `T: GuestEnv`.
struct NullEnv(ResourceTable);

impl GuestEnv for NullEnv {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.0
    }
    fn wall_now(&self) -> (u64, u32) {
        (0, 0)
    }
    fn monotonic_now_ns(&self) -> u64 {
        0
    }
    fn fill_random(&mut self, _stream: RandomStream, buf: &mut [u8]) {
        buf.fill(0);
    }
    fn insecure_seed(&self) -> (u64, u64) {
        (0, 0)
    }
    fn write_stdio(&mut self, _stream: StdStream, _bytes: &[u8]) {}
    fn stdio_budget(&self, _stream: StdStream) -> u64 {
        0
    }
}

/// The "full" linker until 03-07 lands the real ostro imports: the WASI shim plus
/// traps for any remaining (ostro:runtime) imports of `wasm`.
fn full_linker(engine: &Engine, wasm: &[u8]) -> Linker<NullEnv> {
    let mut linker = Linker::new(engine);
    ostro_engine::host::wasi::add_to_linker(&mut linker).expect("shim");
    if let Ok(component) = Component::from_binary(engine, wasm) {
        let ostro_only = component.component_type().imports(engine).all(|(n, _)| {
            let base = n.split('@').next().unwrap_or(n);
            n.starts_with("ostro:runtime/") || ostro_engine::validate::ALLOWED_WASI.contains(&base)
        });
        // Only fill gaps for guests whose non-shim imports are ostro:runtime; never
        // paper over a forbidden WASI import with traps.
        if ostro_only {
            linker
                .define_unknown_imports_as_traps(&component)
                .expect("traps");
        }
    }
    linker
}

fn validate(wasm: &[u8]) -> Result<ValidatedComponent, EngineError> {
    let engine = common::test_engine();
    let linker = full_linker(&engine, wasm);
    validate_component(&engine, &linker, wasm)
}

fn violations(res: Result<ValidatedComponent, EngineError>) -> Vec<String> {
    match res {
        Err(EngineError::Validation(v)) => v,
        Err(other) => panic!("expected Validation error, got {other:?}"),
        Ok(v) => panic!("expected rejection, got {v:?}"),
    }
}

/// `cli-command` is a `[[bin]]`, so its artefact keeps the hyphen (`cli-command.wasm`)
/// whereas `guest_wasm` maps names to cdylib file names (`cli_command.wasm`).
fn cli_command_wasm() -> Vec<u8> {
    let _ = common::guest_wasm("smoke"); // builds the whole guests/ workspace once
    let dir = common::repo_root().join("target/guests/wasm32-wasip2/release");
    for file in ["cli-command.wasm", "cli_command.wasm"] {
        if let Ok(bytes) = std::fs::read(dir.join(file)) {
            return bytes;
        }
    }
    panic!("cli-command artefact not found in {}", dir.display());
}

#[test]
fn validate_accepts_smoke_and_probe() {
    for guest in ["smoke", "wasi-probe"] {
        let v = validate(&common::guest_wasm(guest))
            .unwrap_or_else(|e| panic!("{guest} must validate: {e}"));
        assert_eq!(v.exports.len(), 1, "{guest}: {:?}", v.exports);
        assert!(v.exports[0].starts_with("ostro:runtime/workflows@0.1."));
        assert!(!v.imports.is_empty());
    }
}

#[test]
fn validate_rejects_core_module() {
    let v = violations(validate(b"\0asm\x01\0\0\0"));
    assert_eq!(v.len(), 1);
    assert!(v[0].contains("not a component"), "{v:?}");
}

#[test]
fn validate_rejects_filesystem() {
    let v = violations(validate(&common::guest_wasm("wasi-forbidden")));
    assert!(
        v.iter().any(
            |m| m.contains("wasi:filesystem/") && m.contains("Ostro guests have no filesystem")
        ),
        "{v:?}"
    );
}

#[test]
fn validate_rejects_cli_run_export() {
    let v = violations(validate(&cli_command_wasm()));
    assert!(
        v.iter()
            .any(|m| m.starts_with("export wasi:cli/run@0.2.") && m.contains("not allowed")),
        "{v:?}"
    );
    assert!(
        v.iter()
            .any(|m| m.contains("missing required export ostro:runtime/workflows")),
        "{v:?}"
    );
}

#[test]
fn validate_reports_all_violations() {
    let wasm = common::guest_wasm("wasi-forbidden");
    let engine = common::test_engine();
    let component = Component::from_binary(&engine, &wasm).expect("component");
    let disallowed: Vec<String> = component
        .component_type()
        .imports(&engine)
        .map(|(n, _)| n.to_string())
        .filter(|n| {
            let base = n.split('@').next().unwrap_or(n);
            !ostro_engine::validate::ALLOWED_WASI.contains(&base)
                && !ostro_engine::validate::ALLOWED_OSTRO.contains(&base)
        })
        .collect();
    assert!(
        disallowed.len() >= 2,
        "wasi-forbidden should import several filesystem interfaces: {disallowed:?}"
    );
    let v = violations(validate(&wasm));
    for name in &disallowed {
        assert!(
            v.iter()
                .any(|m| m.starts_with(&format!("import {name} is not allowed"))),
            "{name} not reported in {v:?}"
        );
    }
    // instantiate_pre is skipped when the allow-list already failed.
    assert!(!v.iter().any(|m| m.contains("instantiate_pre")), "{v:?}");
}

#[test]
fn validate_instantiate_pre_failure() {
    let wasm = common::guest_wasm("smoke");
    let engine = common::test_engine();
    let empty: Linker<NullEnv> = Linker::new(&engine);
    let v = violations(validate_component(&engine, &empty, &wasm));
    assert!(v.iter().any(|m| m.contains("instantiate_pre")), "{v:?}");
}

#[test]
fn validate_wasi_probe_imports() {
    let wasm = common::guest_wasm("wasi-probe");
    let engine = common::test_engine();
    let component = Component::from_binary(&engine, &wasm).expect("component");
    let mut names: Vec<String> = component
        .component_type()
        .imports(&engine)
        .map(|(n, _)| n.to_string())
        .collect();
    names.sort();
    insta::assert_snapshot!("wasi_probe_imports", names.join("\n"));
}
