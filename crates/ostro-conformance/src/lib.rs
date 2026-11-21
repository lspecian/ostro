//! ostro-conformance: the Phase 9 polyglot conformance harness.
//!
//! - [`spec`]: the language-neutral scenario model (`conformance/spec/<leg>/<ID>.json`) and
//!   the catalogue (`conformance/spec/catalogue.json`, written once by plan 09-02).
//! - [`inspect`]: component import/export/core-instance inspection (wasmparser).
//! - [`polyglot`]: Python/TypeScript guest builds via `sdks/scripts/build-guest.sh`, cached
//!   by source hash under `target/polyglot-guests`.
//!
//! The Python/TypeScript legs live behind the `polyglot` feature (D-P9-12); everything
//! else runs without guest toolchains.

pub mod inspect;
pub mod polyglot;
pub mod spec;

pub use inspect::{ComponentInfo, inspect};
pub use polyglot::{Mode, build};

use std::path::PathBuf;

/// Repository root (two levels above this crate's manifest).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

// Dependencies declared once by plan 09-02 for later Phase 9 plans (09-11 engine leg,
// 09-13 server leg with the Phase 6 MockProvider, 09-18 crash leg). Anchored here so
// cargo's unused-dependency lint stays quiet until the owning modules consume them;
// harmless afterwards.
mod dep_anchors {
    use ostro_api as _;
    use ostro_engine as _;
    use ostro_meta as _;
    use ostro_server as _;
    use ostro_testkit as _;
    use ostro_types as _;
    use reqwest as _;
    use tempfile as _;
    use thiserror as _;
    use tokio as _;
}
