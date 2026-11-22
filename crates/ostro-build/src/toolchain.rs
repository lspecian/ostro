//! Pinned toolchains (`toolchains.toml`, D-P10-1..D-P10-4): the tool list, the platform
//! detection, the pinned set per platform and `ensure` (download, verify, extract into the
//! cache).
//!
//! Owner: 10-07 (stub from 10-02: the contract types only).

/// A tool Ostro can download into its cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tool {
    /// uv (Python package manager).
    Uv,
    /// CPython (managed by uv).
    Python,
    /// componentize-py (released wheel, D-P9-30).
    ComponentizePy,
    /// Node.js 24 (official tarball).
    Node,
    /// The pinned npm toolchain (jco, rolldown) from `sdks/scripts/toolchain`.
    JcoToolchain,
    /// rustup-init (hermetic Rust).
    RustupInit,
    /// zig (`cc` shim when the host has no C compiler).
    Zig,
}

/// The pinned toolchain set for one platform. 10-07 adds the pins and fills in `id`
/// (sha256 of the `toolchains.toml` bytes, first 16 hex).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolchainSet {
    /// Identifier of the pinned set.
    pub id: String,
}
