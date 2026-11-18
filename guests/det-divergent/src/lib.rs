//! Determinism guest `det` (divergent variant, plan 03-09): the exact workflow source
//! of det-basic with `DIVERGE = true`, so the input payload selects one divergence
//! (`changed-args`, `extra`, `missing`, `reordered`, `early-return`). Replaying a
//! det-basic journal with this module must fail as nondeterminism at the exact seq.

const DIVERGE: bool = true;

include!("../../det-basic/src/workflow.rs");
