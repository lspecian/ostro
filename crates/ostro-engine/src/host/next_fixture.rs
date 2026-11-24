//! TEST-ONLY second WIT major line `ostro:runtime@0.2.0` (13-07, ENG-10, D-P13-17; feature
//! `wit-next-fixture`, never in release builds, never under `wit/`).
//!
//! `fixtures/wit-next/runtime.wit` is the 0.1 surface with one changed signature
//! (`run-info` gains `region`, so `context.info` changes type), one removed function
//! (`context.continue-as-new-suggested`) and no `effects` interface. Its own `bindgen!`
//! generates a separate set of types and host traits; the impls below delegate to the 0.1
//! host, converting where the types differ. The line's linker is built by the same
//! [`build_line_linker`] as 0.1, so the fuel-aware journal overrides shadow
//! `journal@0.2.0` (their signatures are unchanged on this line).

use wasmtime::component::{HasSelf, Linker, Resource};

use crate::host::bindings::StepRep;
use crate::host::bindings::ostro::runtime as v01;
use crate::host::linker_set::ostro_iface;
use crate::host::state::{HostState, build_line_linker, violation};

/// The fixture line's package version.
pub const FIXTURE_0_2_VERSION: &str = "0.2.0";

/// The `region` the fixture host reports in `run-info` (the changed signature's new field).
pub const FIXTURE_REGION: &str = "fixture-0.2";

/// Every imported function of the fixture line, by interface (all at
/// [`LEVEL_0_2_FIXTURE`](crate::host::linker_set::LEVEL_0_2_FIXTURE)).
pub const FUNCS: &[(&str, &[&str])] = &[
    (
        "context",
        &[
            "info",
            "now",
            "random-u64",
            "random-bytes",
            "uuid-v7",
            "is-replaying",
            "patched",
            "set-status",
        ],
    ),
    (
        "journal",
        &["[method]step.seq", "[method]step.complete", "begin-step"],
    ),
    ("log", &["emit"]),
];

/// Bindings for the fixture world `ostro:runtime@0.2.0/workflow`.
pub mod bindings {
    wasmtime::component::bindgen!({
        world: "workflow",
        path: "fixtures/wit-next",
        imports: { default: async | trappable },
        exports: { default: async },
        with: {
            "ostro:runtime/journal.step": crate::host::bindings::StepRep,
        },
    });
}

use bindings::ostro::runtime::types as fx_types;
use bindings::ostro::runtime::{context as fx_context, journal as fx_journal, log as fx_log};

/// The `0.2` fixture line's linker.
pub fn build_linker(engine: &wasmtime::Engine) -> wasmtime::Result<Linker<HostState>> {
    build_line_linker(
        engine,
        &ostro_iface("journal", FIXTURE_0_2_VERSION),
        |linker| bindings::Workflow::add_to_linker::<_, HasSelf<_>>(linker, |h| h),
    )
}

fn datetime(d: v01::types::Datetime) -> fx_types::Datetime {
    fx_types::Datetime {
        seconds: d.seconds,
        nanoseconds: d.nanoseconds,
    }
}

fn shadowed(name: &str) -> wasmtime::Error {
    violation(format!(
        "fixture 0.2: the bindgen {name} was called; the fuel-aware override must shadow it"
    ))
}

impl fx_types::Host for HostState {}

impl fx_context::Host for HostState {
    async fn info(&mut self) -> wasmtime::Result<fx_types::RunInfo> {
        let i = v01::context::Host::info(self).await?;
        Ok(fx_types::RunInfo {
            run_id: i.run_id,
            workflow: i.workflow,
            workflow_version: i.workflow_version,
            attempt: i.attempt,
            module_hash: i.module_hash,
            tenant: i.tenant,
            parent_run_id: i.parent_run_id,
            deadline: i.deadline.map(datetime),
            continued_from: i.continued_from,
            region: Some(FIXTURE_REGION.to_string()),
        })
    }
    async fn now(&mut self) -> wasmtime::Result<fx_types::Datetime> {
        v01::context::Host::now(self).await.map(datetime)
    }
    async fn random_u64(&mut self) -> wasmtime::Result<u64> {
        v01::context::Host::random_u64(self).await
    }
    async fn random_bytes(&mut self, len: u32) -> wasmtime::Result<Vec<u8>> {
        v01::context::Host::random_bytes(self, len).await
    }
    async fn uuid_v7(&mut self) -> wasmtime::Result<String> {
        v01::context::Host::uuid_v7(self).await
    }
    async fn is_replaying(&mut self) -> wasmtime::Result<bool> {
        v01::context::Host::is_replaying(self).await
    }
    async fn patched(&mut self, change_id: String) -> wasmtime::Result<bool> {
        v01::context::Host::patched(self, change_id).await
    }
    async fn set_status(&mut self, json: String) -> wasmtime::Result<()> {
        v01::context::Host::set_status(self, json).await
    }
}

impl fx_journal::Host for HostState {
    async fn begin_step(
        &mut self,
        _name: String,
        _opts: fx_types::StepOptions,
    ) -> wasmtime::Result<fx_journal::StepStart> {
        Err(shadowed("begin-step"))
    }
}

impl fx_journal::HostStep for HostState {
    async fn seq(&mut self, step: Resource<StepRep>) -> wasmtime::Result<u64> {
        v01::journal::HostStep::seq(self, step).await
    }

    async fn complete(
        &mut self,
        _step: Resource<StepRep>,
        _outcome: Result<fx_types::Payload, fx_types::Failure>,
    ) -> wasmtime::Result<fx_journal::CompletionAck> {
        Err(shadowed("[method]step.complete"))
    }

    async fn drop(&mut self, step: Resource<StepRep>) -> wasmtime::Result<()> {
        v01::journal::HostStep::drop(self, step).await
    }
}

impl fx_log::Host for HostState {
    async fn emit(
        &mut self,
        level: fx_log::Level,
        message: String,
        fields: Vec<(String, String)>,
    ) -> wasmtime::Result<()> {
        use v01::log::Level as L;
        let level = match level {
            fx_log::Level::Trace => L::Trace,
            fx_log::Level::Debug => L::Debug,
            fx_log::Level::Info => L::Info,
            fx_log::Level::Warn => L::Warn,
            fx_log::Level::Error => L::Error,
        };
        v01::log::Host::emit(self, level, message, fields).await
    }
}
