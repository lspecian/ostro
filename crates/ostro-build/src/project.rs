//! `ostro.toml` (D-P10-5: `[project] name/lang/entry`, `[build] toolchain`,
//! `[deploy] activate/tenant`; unknown keys rejected), project detection and the output
//! directory (`target/ostro/`).
//!
//! Owner: 10-08 (stub from 10-02: the contract types only).

/// A project language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lang {
    /// Rust (`cargo`, wasm32-wasip2).
    Rust,
    /// Python (componentize-py).
    Python,
    /// TypeScript (jco).
    Ts,
}

/// How toolchains are found (`--toolchain`, `[build] toolchain`; D-P10-4 / D-P10-15).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ToolchainMode {
    /// Rust: system rustup if present, else hermetic; Python/TS: hermetic.
    #[default]
    Auto,
    /// Pinned, sha256-verified downloads into the cache only.
    Hermetic,
    /// Tools from `PATH` only.
    System,
    /// Build inside the `ostro-build` Docker image.
    Docker,
}
