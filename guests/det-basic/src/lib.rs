//! Determinism guest `det` (basic variant): every journaled command type it uses
//! matches across record and replay. 03-09's `det-divergent` includes the same
//! `workflow.rs` with `DIVERGE = true`.

const DIVERGE: bool = false;

include!("workflow.rs");
