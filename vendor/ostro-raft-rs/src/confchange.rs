// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
// Vendored from chronik-stream@08715f4 crates/raft/src/confchange.rs; modified by Ostro (see VENDORED.md).
mod changer;
mod restore;

pub use self::changer::{Changer, MapChange, MapChangeType};
pub use self::restore::restore;

use crate::tracker::Configuration;

#[inline]
pub(crate) fn joint(cfg: &Configuration) -> bool {
    !cfg.voters().outgoing.is_empty()
}
