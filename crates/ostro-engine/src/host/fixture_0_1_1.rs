//! TEST-ONLY host built at `ostro:runtime@0.1.1` (13-07, ENG-10; feature
//! `wit-next-fixture`, never in release builds).
//!
//! No real `0.1.N > 0` exists yet, so this module proves the semver path on a real host:
//! its own `bindgen!` over `fixtures/wit-0.1.1` (the frozen 0.1.0 package with the version
//! bumped and one added function, `journal.note-hint`, level 2 in the test-only
//! `introduced_in` entry), linked exactly as the production line would be once
//! `WIT_0_1_LATEST` moves: only the newest version is defined and the fuel-aware journal
//! overrides shadow `journal@0.1.1`, the name derived from [`FIXTURE_0_1_LATEST`]. A 0.1.0
//! guest then resolves every `@0.1.0` import (and its `workflows@0.1.0` export) through
//! Wasmtime's semver alternate key.
//!
//! `context` and `journal` are generated fresh at 0.1.1 (journal is the interface the
//! overrides shadow; context delegates to the 0.1.0 host one-to-one). `types`, `effects`
//! and `log` are remapped with `with` to the 0.1.0 bindings, so they stay defined at
//! `@0.1.0` and resolve for 0.1.1 guests through the same semver key in the other
//! direction; this keeps the fixture free of type conversions.
//!
//! The bindgen `begin-step` and `[method]step.complete` here fail loudly instead of
//! delegating: they are shadowed, so an activation that completes a step proves the
//! overrides were reached.

use wasmtime::component::{HasSelf, Linker, Resource};

use crate::host::bindings::StepRep;
use crate::host::bindings::ostro::runtime::context::Host as Context010;
use crate::host::bindings::ostro::runtime::journal::HostStep as Step010;
use crate::host::bindings::ostro::runtime::types::{
    Datetime, Failure, Payload, RunInfo, StepOptions,
};
use crate::host::linker_set::ostro_iface;
use crate::host::state::{HostState, build_line_linker, violation};

/// The newest `0.1.N` of the fixture host (what `WIT_0_1_LATEST` would be).
pub const FIXTURE_0_1_LATEST: &str = "0.1.1";

/// Bindings for the fixture world `ostro:runtime@0.1.1/workflow`.
pub mod bindings {
    wasmtime::component::bindgen!({
        world: "workflow",
        path: "fixtures/wit-0.1.1",
        imports: { default: async | trappable },
        exports: { default: async },
        with: {
            "ostro:runtime/types": crate::host::bindings::ostro::runtime::types,
            "ostro:runtime/effects": crate::host::bindings::ostro::runtime::effects,
            "ostro:runtime/log": crate::host::bindings::ostro::runtime::log,
            "ostro:runtime/journal.step": crate::host::bindings::StepRep,
        },
    });
}

use bindings::ostro::runtime::context as fx_context;
use bindings::ostro::runtime::journal as fx_journal;

/// The fixture's 0.1 line linker: overrides at `journal@`[`FIXTURE_0_1_LATEST`].
pub fn build_linker(engine: &wasmtime::Engine) -> wasmtime::Result<Linker<HostState>> {
    build_linker_with_journal_iface(engine, &ostro_iface("journal", FIXTURE_0_1_LATEST))
}

/// The fixture linker with the overrides placed at `journal_iface`. With the stale
/// `ostro_iface("journal", "0.1.0")` this reproduces the hard-coded-`JOURNAL_IFACE` trap
/// (a separate exact-name `@0.1.0` journal holding only the two overrides).
pub fn build_linker_with_journal_iface(
    engine: &wasmtime::Engine,
    journal_iface: &str,
) -> wasmtime::Result<Linker<HostState>> {
    build_line_linker(engine, journal_iface, |linker| {
        bindings::Workflow::add_to_linker::<_, HasSelf<_>>(linker, |h| h)
    })
}

fn shadowed(name: &str) -> wasmtime::Error {
    violation(format!(
        "fixture 0.1.1: the bindgen {name} was called; the fuel-aware override must shadow it"
    ))
}

impl fx_context::Host for HostState {
    async fn info(&mut self) -> wasmtime::Result<RunInfo> {
        Context010::info(self).await
    }
    async fn now(&mut self) -> wasmtime::Result<Datetime> {
        Context010::now(self).await
    }
    async fn random_u64(&mut self) -> wasmtime::Result<u64> {
        Context010::random_u64(self).await
    }
    async fn random_bytes(&mut self, len: u32) -> wasmtime::Result<Vec<u8>> {
        Context010::random_bytes(self, len).await
    }
    async fn uuid_v7(&mut self) -> wasmtime::Result<String> {
        Context010::uuid_v7(self).await
    }
    async fn is_replaying(&mut self) -> wasmtime::Result<bool> {
        Context010::is_replaying(self).await
    }
    async fn patched(&mut self, change_id: String) -> wasmtime::Result<bool> {
        Context010::patched(self, change_id).await
    }
    async fn set_status(&mut self, json: String) -> wasmtime::Result<()> {
        Context010::set_status(self, json).await
    }
    async fn continue_as_new_suggested(&mut self) -> wasmtime::Result<bool> {
        Context010::continue_as_new_suggested(self).await
    }
}

impl fx_journal::Host for HostState {
    async fn begin_step(
        &mut self,
        _name: String,
        _opts: StepOptions,
    ) -> wasmtime::Result<fx_journal::StepStart> {
        Err(shadowed("begin-step"))
    }

    /// The function 0.1.1 adds: a hint the host ignores (no journal effect).
    async fn note_hint(&mut self, _hint: String) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl fx_journal::HostStep for HostState {
    async fn seq(&mut self, step: Resource<StepRep>) -> wasmtime::Result<u64> {
        Step010::seq(self, step).await
    }

    async fn complete(
        &mut self,
        _step: Resource<StepRep>,
        _outcome: Result<Payload, Failure>,
    ) -> wasmtime::Result<fx_journal::CompletionAck> {
        Err(shadowed("[method]step.complete"))
    }

    async fn drop(&mut self, step: Resource<StepRep>) -> wasmtime::Result<()> {
        Step010::drop(self, step).await
    }
}
