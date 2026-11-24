//! Phase 13: implemented in 13-07 (LinkerSet, ENG-10; D-P13-17).
//!
//! One `Linker<HostState>` per WIT **major line**. Within a line only the newest `0.1.N` is
//! defined and Wasmtime's semver name map resolves older `@0.1.x` imports/exports to it.
//! Interface names will derive from [`WIT_0_1_LATEST`] (13-07 replaces the hard-coded
//! `JOURNAL_IFACE` in `host/state.rs`). [`select`] reads a component's imports, picks its line
//! and computes its minimum feature level (max over imported functions' `introduced_in`).
//!
//! Fixed signatures; the stub selects line 0.1 at level 1 for every component, which is what
//! the Phase 12 engine links today.

use wasmtime::Engine;
use wasmtime::component::Component;

use crate::error::EngineError;

/// The newest `0.1.N` WIT package present under `wit/` (`wit/ostro-runtime-0.1.0`).
pub const WIT_0_1_LATEST: &str = "0.1.0";

/// A WIT major line (`0.1`, `0.2`, ...): `major.minor` of a pre-1.0 package.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WitLine {
    /// Major version.
    pub major: u32,
    /// Minor version (the line for `0.x` packages).
    pub minor: u32,
}

impl WitLine {
    /// The `0.1` line (frozen at 05-20).
    pub const V0_1: WitLine = WitLine { major: 0, minor: 1 };
}

impl std::fmt::Display for WitLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// The line a component links against and its minimum feature level (stored as
/// `ModuleRecord` ext 101 WitRequirement by 13-14).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WitSelection {
    /// The WIT line.
    pub line: WitLine,
    /// Max over the imported functions' `introduced_in` levels.
    pub min_level: u32,
}

/// One linker per WIT line (13-07). Stub: no linkers; the engine keeps its single 0.1 linker.
#[derive(Debug, Default)]
pub struct LinkerSet {
    _private: (),
}

impl LinkerSet {
    /// The lines this build links.
    pub fn lines(&self) -> Vec<WitLine> {
        vec![WitLine::V0_1]
    }
}

/// Select the WIT line of `component` (mixing lines is an upload error). Stub: line 0.1,
/// level 1.
pub fn select(component: &Component, engine: &Engine) -> Result<WitSelection, EngineError> {
    let _ = (component, engine);
    Ok(WitSelection {
        line: WitLine::V0_1,
        min_level: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_matches_the_wit_dir() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../wit");
        let newest = std::fs::read_dir(dir)
            .expect("wit/ exists")
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter_map(|n| n.strip_prefix("ostro-runtime-").map(str::to_owned))
            .filter(|v| v.starts_with("0.1."))
            .max_by_key(|v| v.rsplit('.').next().and_then(|p| p.parse::<u32>().ok()))
            .expect("a 0.1.N package");
        assert_eq!(newest, WIT_0_1_LATEST);
        assert_eq!(WitLine::V0_1.to_string(), "0.1");
        assert_eq!(LinkerSet::default().lines(), [WitLine::V0_1]);
    }
}
