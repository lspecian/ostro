//! Host side of the WIT 0.1 world: bindings, deterministic WASI shim and import impls.

pub mod bindings;
pub mod context;
pub mod effects;
pub mod journal;
// Phase 13 (13-01 stub, 13-07 implementation): one linker per WIT major line (ENG-10).
pub mod linker_set;
pub mod log;
pub mod state;
pub mod wasi;
