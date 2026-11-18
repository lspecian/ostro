//! Host bindings for the `ostro:runtime@0.1.0` world `workflow`.
//!
//! Imports are async + trappable (host calls may await the sink; errors trap the
//! guest). The `step` resource is backed by [`StepRep`] in the store's table.

wasmtime::component::bindgen!({
    world: "workflow",
    path: "../../wit/ostro-runtime-0.1.0",
    imports: { default: async | trappable },
    exports: { default: async },
    with: {
        "ostro:runtime/journal.step": StepRep,
    },
});

/// Host representation of a `journal.step` resource handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepRep {
    /// Journal sequence number assigned at `begin-step`.
    pub seq: u64,
    /// Step name as passed to `begin-step`.
    pub name: String,
    /// Remaining fuel when the step began (for `fuel_used` accounting).
    pub fuel_before: Option<u64>,
}
