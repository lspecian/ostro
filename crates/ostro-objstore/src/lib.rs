//! ostro-objstore: the shared object-store abstraction (D19, P8-D5).
//!
//! Phase 8: trait + backends in 08-03 (Phase 6 adds presign/PutOpts).
//!
//! Created empty by plan 08-01 (Phase 8 ran first); 08-03 owns this file from here on.

// Dependencies declared by plan 08-01 (P8-D5), anchored so cargo's unused-dependency lint
// stays quiet until 08-03 consumes them.
mod dep_anchors {
    use async_trait as _;
    use bytes as _;
    use futures_core as _;
    use futures_util as _;
    use jiff as _;
    use reqwest as _;
    use rusty_s3 as _;
    use sha2 as _;
    use thiserror as _;
    use tokio as _;
    use tracing as _;
    use url as _;
}
