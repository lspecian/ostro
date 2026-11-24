//! ENG-10 fixture components (13-07): one per WIT shape the LinkerSet must handle.
//!
//! Built as real guests from the `guests/` workspace (`wit_bindgen::generate!` over the
//! frozen `wit/` package or over the test-only fixture packages under
//! `crates/ostro-engine/fixtures/`), not with `wit_component::dummy_module`: ostro-engine has
//! no `wit-component` dependency and Phase 13's Cargo files are 13-01-only. Real guests
//! also run activations, which a dummy module (unreachable bodies) cannot.
//!
//! Included with `#[path = "common/wit_fixtures.rs"]` next to `mod common;`.

use super::common::guest_wasm;

/// A real `ostro:runtime@0.1.0` SDK guest (workflow `trivial-steps`, input `{"n": u32}`):
/// imports `journal@0.1.0` (begin-step / step.complete) and exports `workflows@0.1.0`.
pub fn guest_0_1_0() -> Vec<u8> {
    guest_wasm("trivial-steps")
}

/// Built against the test-only `ostro:runtime@0.1.1`; calls the added `journal.note-hint`
/// (level 2). Workflow `w011`, output `w011:<input>`.
pub fn guest_0_1_1() -> Vec<u8> {
    guest_wasm("p13-wit011")
}

/// Built against `ostro:runtime@0.1.1` but calls only 0.1.0 functions (level 1). Workflow
/// `w011-lite`, output `w011-lite:<input>`.
pub fn guest_0_1_1_lite() -> Vec<u8> {
    guest_wasm("p13-wit011-lite")
}

/// Built against the test-only `ostro:runtime@0.2.0` line. Workflow `w020`, output
/// `<region>:<input>`.
pub fn guest_0_2_0() -> Vec<u8> {
    guest_wasm("p13-wit020")
}

/// Imports `ostro:runtime/log` at both 0.1.0 and 0.2.0 (mixed lines; never instantiated).
pub fn guest_mixed() -> Vec<u8> {
    guest_wasm("p13-wit-mixed")
}
