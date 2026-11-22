//! `target/ostro/manifest.json` (kind `ostro.manifest`, version 1): what was built, from
//! which inputs and with which toolchains. Written by the build pipeline after the
//! component passed local validation; read by `ostro deploy` / `ostro dev --watch`
//! (10-16).
//!
//! ```json
//! {
//!   "kind": "ostro.manifest",
//!   "version": 1,
//!   "name": "hello",
//!   "lang": "rust",
//!   "module": { "sha256": "<64 hex>", "bytes": 123456 },
//!   "workflows": [ { "name": "hello", "version": 1 } ],
//!   "toolchains": { "set_id": "<16 hex>", "tools": ["system-rustup"] },
//!   "sdk_bundle": "<16 hex>",
//!   "source_date_epoch": 0,
//!   "input_hash": "<64 hex>",
//!   "embedded_runtimes": []
//! }
//! ```
//!
//! Owner: 10-12.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::pipeline::{BuildError, write_atomic};
use crate::project::Lang;

/// `kind` of `manifest.json`.
pub const KIND: &str = "ostro.manifest";
/// `version` of `manifest.json`.
pub const VERSION: u32 = 1;

/// `manifest.json` (fields serialised in declaration order).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Always [`KIND`].
    pub kind: String,
    /// Always [`VERSION`].
    pub version: u32,
    /// The project (module) name.
    pub name: String,
    /// The project language.
    pub lang: Lang,
    /// The component.
    pub module: Module,
    /// The workflows `describe()` returned, sorted by name and version.
    pub workflows: Vec<Workflow>,
    /// The toolchains used.
    pub toolchains: Toolchains,
    /// The SDK bundle id ([`crate::sdk_bundle::bundle_id`]).
    pub sdk_bundle: String,
    /// The `SOURCE_DATE_EPOCH` the build ran with.
    pub source_date_epoch: u64,
    /// The input hash ([`crate::pipeline::input_hash`]).
    pub input_hash: String,
    /// Third-party runtimes linked into the component, with their licences
    /// ([`embedded_runtimes`]).
    pub embedded_runtimes: Vec<String>,
}

/// `module`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Module {
    /// Lower-case hex sha256 of the component.
    pub sha256: String,
    /// Its size.
    pub bytes: u64,
}

/// One `workflows` entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workflow {
    /// The workflow name.
    pub name: String,
    /// Its version.
    pub version: u32,
}

/// `toolchains`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Toolchains {
    /// The pinned toolchain-set id ([`crate::toolchain::set_id`]).
    pub set_id: String,
    /// The tools the step used ([`crate::pipeline::LangStep::tool_ids`]).
    pub tools: Vec<String>,
}

/// The runtimes a `lang` component embeds: CPython and friends for Python (componentize-py),
/// QuickJS-NG for TypeScript (jco); none for Rust (its dependencies are in `Cargo.lock`).
pub fn embedded_runtimes(lang: Lang) -> Vec<String> {
    let list: &[&str] = match lang {
        Lang::Python => &[
            "CPython (PSF-2.0)",
            "libc++/libc++abi (Apache-2.0 WITH LLVM-exception)",
            "wasi-libc",
            "componentize-py runtime (Apache-2.0 WITH LLVM-exception)",
        ],
        Lang::Ts => &["QuickJS-NG (MIT)", "rquickjs (MIT)"],
        Lang::Rust => &[],
    };
    list.iter().map(|s| (*s).to_owned()).collect()
}

/// Read and check `manifest.json` (kind and version must match).
pub fn read(path: &Path) -> Result<Manifest, BuildError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| BuildError::io(format!("reading {}", path.display()), e))?;
    let m: Manifest = serde_json::from_str(&text).map_err(|e| {
        BuildError::io(
            format!("parsing {}", path.display()),
            std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        )
    })?;
    if m.kind != KIND || m.version != VERSION {
        return Err(BuildError::io(
            format!("parsing {}", path.display()),
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "expected kind {KIND:?} version {VERSION}, found {:?} version {}",
                    m.kind, m.version
                ),
            ),
        ));
    }
    Ok(m)
}

/// Write `manifest.json` atomically (pretty JSON plus a trailing newline).
pub fn write(path: &Path, m: &Manifest) -> Result<(), BuildError> {
    let mut text = serde_json::to_string_pretty(m).map_err(|e| {
        BuildError::io(
            "serialising manifest.json",
            std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        )
    })?;
    text.push('\n');
    write_atomic(path, text.as_bytes())
}
