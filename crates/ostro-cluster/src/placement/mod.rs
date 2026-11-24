//! Phase 13 replica placement and reassignment (CLUS-05, D-P13-12). Declared by 13-01; final.
//!
//! | module | owner | contents |
//! |--------|-------|----------|
//! | [`plan`] | 13-06 | pure target replica sets |
//! | [`reassign`] | 13-06 | pure reassignment state machine (Expand → CatchUp → LeaderMove → Shrink) |
//! | [`leaders`] | 13-06 | pure preferred-leader balancing |
//! | [`controller`] | 13-10 | the meta leader's placement controller (AlterReplicas proposals) |

pub mod controller;
pub mod leaders;
pub mod plan;
pub mod reassign;
