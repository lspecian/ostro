//! ENG-01: the deterministic engine configuration is pinned and observable.

mod common;

use ostro_engine::config::{EngineConfig, build_wasmtime_config, describe_wasmtime_config};
use ostro_engine::host::bindings::Workflow;
use ostro_engine::host::bindings::exports::ostro::runtime::workflows::RunResult;
use ostro_engine::host::bindings::ostro::runtime::types::Payload;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Module, Store};

const CANONICAL_NAN: u64 = 0x7ff8_0000_0000_0000;

/// Core module with one shared memory (limits flag 0x03, min 1, max 1).
const SHARED_MEMORY_MODULE: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, // \0asm
    0x01, 0x00, 0x00, 0x00, // version 1
    0x05, 0x04, // memory section, 4 bytes
    0x01, // one memory
    0x03, 0x01, 0x01, // shared, min 1, max 1
];

/// Core module declaring one GC struct type with no fields (`(type (struct))`).
const GC_STRUCT_MODULE: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, // \0asm
    0x01, 0x00, 0x00, 0x00, // version 1
    0x01, 0x03, // type section, 3 bytes
    0x01, // one type
    0x5f, 0x00, // struct, zero fields
];

#[test]
fn engine_config_snapshot() {
    // wasmtime's Config Debug lacks stack sizes and the allocator; the describe
    // helper appends them. No host-dependent values appear, so no redaction filters.
    let debug = describe_wasmtime_config(&EngineConfig::default());
    for needle in [
        "\"enable_nan_canonicalization\": (\n                    \"true\"",
        "relaxed_simd_deterministic: true",
        "consume_fuel: true",
        "epoch_interruption: true",
        "wasm_threads: false",
        "wasm_gc_types: false",
        "wasm_stack_switching: false",
        "wasm_cm_async: false",
        "max_wasm_stack: 524288",
        "async_stack_size: 4194304",
        "wasm_backtrace_details: Disable",
    ] {
        assert!(debug.contains(needle), "missing `{needle}` in:\n{debug}");
    }
    insta::assert_snapshot!("wasmtime_config_debug", debug);
}

#[test]
fn engine_config_threads_gc_off() {
    let engine = Engine::new(&build_wasmtime_config(&EngineConfig::default())).expect("engine");
    let err = Module::validate(&engine, SHARED_MEMORY_MODULE)
        .expect_err("shared memory must be rejected (threads compiled out)");
    eprintln!("shared memory rejected: {err:#}");
    let err = Module::validate(&engine, GC_STRUCT_MODULE)
        .expect_err("GC struct type must be rejected (gc compiled out)");
    eprintln!("gc struct rejected: {err:#}");
}

#[test]
fn engine_config_nan_canonical() -> wasmtime::Result<()> {
    let wasm = common::guest_wasm("nan-probe");
    let engine = common::test_engine();
    let component = Component::from_binary(&engine, &wasm)?;
    let mut linker: Linker<()> = Linker::new(&engine);
    linker.define_unknown_imports_as_traps(&component)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let mut store = Store::new(&engine, ());
        store.set_fuel(u64::MAX)?;
        store.set_epoch_deadline(u64::MAX);
        let wf = Workflow::instantiate_async(&mut store, &component, &linker).await?;
        let api = wf.ostro_runtime_workflows();
        let input = Payload {
            content_type: "application/octet-stream".into(),
            data: 0x7ff8_0000_0000_0001u64.to_le_bytes().to_vec(),
        };
        let out = match api.call_run(&mut store, "nan", &input).await? {
            Ok(RunResult::Completed(out)) => out,
            other => panic!("unexpected run result: {other:?}"),
        };
        assert_eq!(out.data.len(), 16);
        let sum = u64::from_le_bytes(out.data[..8].try_into()?);
        let prod = u64::from_le_bytes(out.data[8..].try_into()?);
        assert_eq!(sum, CANONICAL_NAN, "x + 1.0 bits {sum:#018x}");
        assert_eq!(prod, CANONICAL_NAN, "x * 2.0 bits {prod:#018x}");
        Ok(())
    })
}
