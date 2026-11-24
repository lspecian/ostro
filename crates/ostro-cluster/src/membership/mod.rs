//! Phase 13 dynamic membership (CLUS-05). Declared by 13-01; final.
//!
//! | module | owner | contents |
//! |--------|-------|----------|
//! | [`reconcile`] | 13-06 | pure learner-first reconciler (`next` → [`reconcile::Step`]) |
//! | [`service`] | 13-09 | the meta leader's membership loop (conf changes, tombstones) |
//! | [`remove`] | 13-09 | quorum-safe removal checks (`--force` semantics) |

pub mod reconcile;
pub mod remove;
pub mod service;
