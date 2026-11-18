//! WIT-01: the WIT 0.1 contract compiles on host (bindgen!) and guest (wit-bindgen).

mod common;

use ostro_engine::host::bindings::Workflow;
use ostro_engine::host::bindings::exports::ostro::runtime::workflows::RunResult;
use ostro_engine::host::bindings::ostro::runtime::types::Payload;
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

#[test]
fn smoke_guest_compiles_and_runs() -> wasmtime::Result<()> {
    let wasm = common::guest_wasm("smoke");
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

        let metas = api.call_describe(&mut store).await?;
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].name, "smoke");
        assert_eq!(metas[0].version, 1);
        assert_eq!(metas[0].run_timeout_ms, Some(60_000));

        let input = Payload {
            content_type: "application/octet-stream".into(),
            data: vec![1, 2, 3, 0xff],
        };
        let res = api.call_run(&mut store, "smoke", &input).await?;
        match res {
            Ok(RunResult::Completed(out)) => {
                assert_eq!(out.content_type, input.content_type);
                assert_eq!(out.data, input.data);
            }
            other => panic!("unexpected run result: {other:?}"),
        }

        let res = api.call_run(&mut store, "nope", &input).await?;
        assert!(res.is_err(), "unknown workflow must fail");
        Ok(())
    })
}
