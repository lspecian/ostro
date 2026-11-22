//! The SDK sources embedded at build time (`build.rs` writes `$OUT_DIR/sdk_bundle.tar`),
//! their bundle id and materialisation into `target/ostro/sdk/`.
//!
//! Owner: 10-08 (stub from 10-02; the 10-02 build.rs writes an empty tar).

/// The embedded SDK bundle (a tar archive written by `build.rs`).
pub const SDK_BUNDLE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/sdk_bundle.tar"));
