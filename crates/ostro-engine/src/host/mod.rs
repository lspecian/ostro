//! Host side of the WIT 0.1 world: bindings, deterministic WASI shim and import impls.

pub mod bindings;
pub mod context;
pub mod effects;
pub mod journal;
// Phase 13 (13-07, ENG-10): test-only host built at ostro:runtime@0.1.1 (semver proof).
#[cfg(feature = "wit-next-fixture")]
pub mod fixture_0_1_1;
// Phase 13 (13-01 stub, 13-07 implementation): one linker per WIT major line (ENG-10).
pub mod linker_set;
pub mod log;
// Phase 13 (13-07, ENG-10): test-only second WIT major line ostro:runtime@0.2.0.
#[cfg(feature = "wit-next-fixture")]
pub mod next_fixture;
pub mod state;
pub mod wasi;
