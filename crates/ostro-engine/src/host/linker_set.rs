//! Side-by-side WIT linking by major line (13-07, ENG-10; D-P13-17).
//!
//! One `Linker<HostState>` per WIT **major line** (`0.1`, `0.2`, ...). Within a line only
//! the newest `0.1.N` is defined: Wasmtime's semver name map keeps an alternate key
//! `ostro:runtime/journal@0.1` pointing at the newest definition, so a component importing
//! (or exporting) any older `@0.1.x` resolves to it. Defining an older exact name next to
//! it would win the lookup and break those components, so every `ostro:runtime` interface
//! name the host defines by hand derives from [`WIT_0_1_LATEST`] through [`ostro_iface`]
//! (this replaced the hard-coded `JOURNAL_IFACE = "…journal@0.1.0"` in `host/state.rs`).
//!
//! [`select`] reads a component's `ostro:runtime` imports (and its `workflows` export),
//! rejects components that mix major lines, and computes the module's minimum feature
//! level as the max over the imported **functions** of [`introduced_in`] (not the package
//! version: a guest built against `0.1.1` that calls only `0.1.0` functions needs level 1).
//! [`line_from_bytes`] is the pre-compile variant (the AOT cache key needs the line before
//! the component is compiled). The only line a production build links is `0.1`; the
//! test-only `0.2` fixture line (`fixtures/wit-next`, feature `wit-next-fixture`) proves the
//! side-by-side path without shipping a second line.
//!
//! Versions are parsed by hand (`x.y.z`, no `semver` dependency).

use std::collections::{BTreeMap, BTreeSet};

use wasmtime::Engine;
use wasmtime::component::types::ComponentItem;
use wasmtime::component::{Component, Linker};

use crate::error::EngineError;
use crate::host::state::HostState;
use crate::validate::ValidateError;

/// The newest `0.1.N` WIT package present under `wit/` (`wit/ostro-runtime-0.1.0`). Every
/// `ostro:runtime@0.1` interface name the host defines derives from it.
pub const WIT_0_1_LATEST: &str = "0.1.0";

/// The WIT package every Ostro guest imports from.
pub const OSTRO_PACKAGE: &str = "ostro:runtime";

/// Feature level of every `ostro:runtime@0.1.0` function (`compat::LEVEL_MIN`).
pub const LEVEL_0_1_0: u32 = 1;

/// The full name of `iface` in the `ostro:runtime` package at `version`
/// (`ostro:runtime/journal@0.1.0`).
pub fn ostro_iface(iface: &str, version: &str) -> String {
    format!("{OSTRO_PACKAGE}/{iface}@{version}")
}

/// Parse a plain `major.minor.patch` version (decimal, no pre-release/build suffix).
pub fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
    let mut parts = v.split('.');
    let mut next = || -> Option<u32> {
        let p = parts.next()?;
        if p.is_empty()
            || !p.bytes().all(|b| b.is_ascii_digit())
            || (p.len() > 1 && p.starts_with('0'))
        {
            return None;
        }
        p.parse().ok()
    };
    let out = (next()?, next()?, next()?);
    parts.next().is_none().then_some(out)
}

/// A WIT major line (`0.1`, `0.2`, ...): `major.minor` of a pre-1.0 package, `major.0` from
/// 1.0 on (semver compatibility).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WitLine {
    /// Major version.
    pub major: u32,
    /// Minor version (the line for `0.x` packages; 0 from 1.0 on).
    pub minor: u32,
}

impl WitLine {
    /// The `0.1` line (frozen at 05-20).
    pub const V0_1: WitLine = WitLine { major: 0, minor: 1 };
    /// The `0.2` line: only the test-only fixture (`wit-next-fixture`) links it.
    pub const V0_2: WitLine = WitLine { major: 0, minor: 2 };

    /// The line of version `major.minor.*`.
    pub fn of(major: u32, minor: u32) -> WitLine {
        if major == 0 {
            WitLine { major, minor }
        } else {
            WitLine { major, minor: 0 }
        }
    }

    /// The line of a `major.minor.patch` version string.
    pub fn of_version(v: &str) -> Option<WitLine> {
        parse_version(v).map(|(major, minor, _)| WitLine::of(major, minor))
    }

    /// Parse the [`Display`](std::fmt::Display) form (`0.1`).
    pub fn parse(s: &str) -> Option<WitLine> {
        let (a, b) = s.split_once('.')?;
        let line = WitLine::of_version(&format!("{a}.{b}.0"))?;
        (line.to_string() == s).then_some(line)
    }
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
    /// Max over the imported functions' `introduced_in` levels (1 when it imports none).
    pub min_level: u32,
}

/// Every function of `ostro:runtime@0.1.0` by interface (level [`LEVEL_0_1_0`]). Resource
/// methods use the component-model export name (`[method]step.seq`). `types` has none.
/// `introduced_in_table_matches_the_frozen_wit` pins this against `wit/`.
pub const FUNCS_0_1_0: &[(&str, &[&str])] = &[
    (
        "context",
        &[
            "info",
            "now",
            "random-u64",
            "random-bytes",
            "uuid-v7",
            "is-replaying",
            "patched",
            "set-status",
            "continue-as-new-suggested",
        ],
    ),
    (
        "journal",
        &["[method]step.seq", "[method]step.complete", "begin-step"],
    ),
    (
        "effects",
        &[
            "http-fetch",
            "sql-query",
            "object-store",
            "sleep",
            "sleep-until",
            "wait-signal",
            "start-child",
            "create-await-token",
            "cancellation",
            "cancel-op",
            "wait",
        ],
    ),
    ("log", &["emit"]),
];

/// Functions added by later `0.1.N` packages: `(interface, function, level)`. Empty in
/// production (no `0.1.N > 0` exists). With `wit-next-fixture` it holds the test-only
/// `0.1.1` fixture's `journal.note-hint` at level 2.
#[cfg(not(feature = "wit-next-fixture"))]
pub const FUNCS_0_1_ADDED: &[(&str, &str, u32)] = &[];
/// Functions added by later `0.1.N` packages: `(interface, function, level)`. Empty in
/// production (no `0.1.N > 0` exists). With `wit-next-fixture` it holds the test-only
/// `0.1.1` fixture's `journal.note-hint` at level 2.
#[cfg(feature = "wit-next-fixture")]
pub const FUNCS_0_1_ADDED: &[(&str, &str, u32)] = &[("journal", "note-hint", 2)];

/// Feature level of every function of the test-only `0.2` fixture line.
#[cfg(feature = "wit-next-fixture")]
pub const LEVEL_0_2_FIXTURE: u32 = 2;

/// The feature level at which `func` of `iface` was introduced on `line`, or `None` if
/// this build does not know the function.
pub fn introduced_in(line: WitLine, iface: &str, func: &str) -> Option<u32> {
    if line == WitLine::V0_1 {
        let base = FUNCS_0_1_0
            .iter()
            .any(|(i, fs)| *i == iface && fs.contains(&func));
        if base {
            return Some(LEVEL_0_1_0);
        }
        return FUNCS_0_1_ADDED
            .iter()
            .find(|(i, f, _)| *i == iface && *f == func)
            .map(|(_, _, l)| *l);
    }
    #[cfg(feature = "wit-next-fixture")]
    if line == WitLine::V0_2 {
        return crate::host::next_fixture::FUNCS
            .iter()
            .any(|(i, fs)| *i == iface && fs.contains(&func))
            .then_some(LEVEL_0_2_FIXTURE);
    }
    None
}

/// The WIT lines this build links (`0.1`; plus `0.2` with `wit-next-fixture`).
pub fn supported_lines() -> Vec<WitLine> {
    let mut v = vec![WitLine::V0_1];
    if cfg!(feature = "wit-next-fixture") {
        v.push(WitLine::V0_2);
    }
    v
}

/// `ostro:runtime/<iface>@<version>` → (`iface`, `version`); `None` for other packages.
/// An `ostro:runtime` name without a version yields an empty version.
pub fn split_ostro(name: &str) -> Option<(&str, &str)> {
    let rest = name.strip_prefix(OSTRO_PACKAGE)?.strip_prefix('/')?;
    Some(rest.split_once('@').unwrap_or((rest, "")))
}

/// The single line of a set of `ostro:runtime` names, checked against `supported`.
fn line_of<'a>(
    names: impl IntoIterator<Item = &'a str>,
    supported: &[WitLine],
) -> Result<WitLine, ValidateError> {
    let mut lines = BTreeSet::new();
    for name in names {
        let Some((_, ver)) = split_ostro(name) else {
            continue;
        };
        let line =
            WitLine::of_version(ver).ok_or_else(|| ValidateError::BadWitVersion(name.into()))?;
        lines.insert(line);
    }
    let mut it = lines.iter();
    let line = match (it.next(), it.next()) {
        (None, _) => return Err(ValidateError::NoWitLine),
        (Some(l), None) => *l,
        (Some(_), Some(_)) => {
            return Err(ValidateError::MixedWitLines(
                lines.iter().map(ToString::to_string).collect(),
            ));
        }
    };
    if !supported.contains(&line) {
        return Err(ValidateError::UnsupportedWitLine(line.to_string()));
    }
    Ok(line)
}

/// Pure selection over a component's surface: `imports` are `(name, function names)` (non
/// `ostro:runtime` imports such as the WASI shim are ignored), `exports` the export names.
/// The line comes from every `ostro:runtime` name (imports and the `workflows` export);
/// the level is the max `introduced_in` over the imported functions.
pub fn select_from_surface(
    imports: &[(String, Vec<String>)],
    exports: &[String],
    supported: &[WitLine],
) -> Result<WitSelection, ValidateError> {
    let names = imports
        .iter()
        .map(|(n, _)| n.as_str())
        .chain(exports.iter().map(String::as_str));
    let line = line_of(names, supported)?;
    let mut min_level = LEVEL_0_1_0;
    for (name, funcs) in imports {
        let Some((iface, _)) = split_ostro(name) else {
            continue;
        };
        for func in funcs {
            let level = introduced_in(line, iface, func).ok_or_else(|| {
                ValidateError::UnknownWitFunction {
                    line: line.to_string(),
                    import: name.clone(),
                    func: func.clone(),
                }
            })?;
            min_level = min_level.max(level);
        }
    }
    Ok(WitSelection { line, min_level })
}

/// [`select`] with the typed error.
pub fn select_lines(component: &Component, engine: &Engine) -> Result<WitSelection, ValidateError> {
    let ty = component.component_type();
    let imports: Vec<(String, Vec<String>)> = ty
        .imports(engine)
        .map(|(name, item)| {
            let funcs = match (&item.ty, split_ostro(name)) {
                (ComponentItem::ComponentInstance(inst), Some(_)) => inst
                    .exports(engine)
                    .filter(|(_, ext)| matches!(ext.ty, ComponentItem::ComponentFunc(_)))
                    .map(|(f, _)| f.to_string())
                    .collect(),
                _ => Vec::new(),
            };
            (name.to_string(), funcs)
        })
        .collect();
    let exports: Vec<String> = ty.exports(engine).map(|(n, _)| n.to_string()).collect();
    select_from_surface(&imports, &exports, &supported_lines())
}

/// Select the WIT line of `component` and its minimum level. Mixing lines, an unsupported
/// line, an unparsable `ostro:runtime` version or an unknown function is an upload error
/// ([`EngineError::Validation`] carrying the [`ValidateError`] message).
pub fn select(component: &Component, engine: &Engine) -> Result<WitSelection, EngineError> {
    select_lines(component, engine).map_err(EngineError::from)
}

/// The WIT line of a component binary, read from its top-level import/export names before
/// any compile (the AOT cache key carries the line). Same rules as [`select`] minus the
/// per-function level.
pub fn line_from_bytes(wasm: &[u8]) -> Result<WitLine, ValidateError> {
    use wasmparser::{Parser, Payload};
    let mut depth = 0usize;
    let mut names: Vec<String> = Vec::new();
    for payload in Parser::new(0).parse_all(wasm) {
        let payload = payload.map_err(|e| ValidateError::Unparsable(e.to_string()))?;
        match payload {
            Payload::Version { .. } => depth += 1,
            Payload::End(_) => depth = depth.saturating_sub(1),
            Payload::ComponentImportSection(reader) if depth == 1 => {
                for imp in reader {
                    let imp = imp.map_err(|e| ValidateError::Unparsable(e.to_string()))?;
                    names.push(imp.name.full_name().into_owned());
                }
            }
            Payload::ComponentExportSection(reader) if depth == 1 => {
                for exp in reader {
                    let exp = exp.map_err(|e| ValidateError::Unparsable(e.to_string()))?;
                    names.push(exp.name.full_name().into_owned());
                }
            }
            _ => {}
        }
    }
    line_of(names.iter().map(String::as_str), &supported_lines())
}

/// One linker per WIT line (13-07).
///
/// [`LinkerSet::new`] builds a linker for every line this build links. The `Default` value
/// holds no linkers and only describes the build ([`LinkerSet::lines`], used for the
/// `NodeVersion.wit_lines` advertisement).
#[derive(Default)]
pub struct LinkerSet {
    linkers: BTreeMap<WitLine, Linker<HostState>>,
}

impl std::fmt::Debug for LinkerSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkerSet")
            .field("built", &self.linkers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl LinkerSet {
    /// Build the production linker of every line this build links: `0.1`
    /// ([`build_linker`](crate::host::state::build_linker), newest `0.1.N` only) and, with
    /// `wit-next-fixture`, the test-only `0.2` line.
    pub fn new(engine: &Engine) -> wasmtime::Result<Self> {
        let mut linkers = BTreeMap::new();
        linkers.insert(WitLine::V0_1, crate::host::state::build_linker(engine)?);
        #[cfg(feature = "wit-next-fixture")]
        linkers.insert(
            WitLine::V0_2,
            crate::host::next_fixture::build_linker(engine)?,
        );
        Ok(Self { linkers })
    }

    /// Test constructor (`wit-next-fixture`): the `0.1` line is the test-only host built at
    /// `ostro:runtime@0.1.1` ([`fixture_0_1_1`](crate::host::fixture_0_1_1)), as if
    /// `WIT_0_1_LATEST` were `0.1.1`; the `0.2` fixture line as in [`LinkerSet::new`].
    #[cfg(feature = "wit-next-fixture")]
    pub fn with_fixture_0_1_1(engine: &Engine) -> wasmtime::Result<Self> {
        let mut set = Self::new(engine)?;
        set.linkers.insert(
            WitLine::V0_1,
            crate::host::fixture_0_1_1::build_linker(engine)?,
        );
        Ok(set)
    }

    /// The lines this build links (independent of which linkers were built).
    pub fn lines(&self) -> Vec<WitLine> {
        supported_lines()
    }

    /// The linker of `line`, if built.
    pub fn linker(&self, line: WitLine) -> Option<&Linker<HostState>> {
        self.linkers.get(&line)
    }

    /// Select `component`'s line ([`select`]) and return it with that line's linker.
    pub fn for_component(
        &self,
        component: &Component,
        engine: &Engine,
    ) -> Result<(WitSelection, &Linker<HostState>), EngineError> {
        let sel = select(component, engine)?;
        let linker = self.linker(sel.line).ok_or_else(|| {
            EngineError::from(ValidateError::UnsupportedWitLine(sel.line.to_string()))
        })?;
        Ok((sel, linker))
    }
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
        assert_eq!(LinkerSet::default().lines(), supported_lines());
        assert_eq!(supported_lines()[0], WitLine::V0_1);
    }

    #[test]
    fn versions_parse_by_hand() {
        assert_eq!(parse_version("0.1.0"), Some((0, 1, 0)));
        assert_eq!(parse_version("0.1.17"), Some((0, 1, 17)));
        assert_eq!(parse_version("2.0.3"), Some((2, 0, 3)));
        for bad in [
            "",
            "0.1",
            "0.1.0.0",
            "0.1.x",
            "a.b.c",
            "0.01.0",
            "0.1.0-rc1",
            "0.1.0+b",
            " 0.1.0",
            "-1.0.0",
            "0..0",
        ] {
            assert_eq!(parse_version(bad), None, "{bad:?}");
        }
        assert_eq!(WitLine::of_version("0.1.9"), Some(WitLine::V0_1));
        assert_eq!(WitLine::of_version("0.2.0"), Some(WitLine::V0_2));
        assert_eq!(
            WitLine::of_version("1.4.2"),
            Some(WitLine { major: 1, minor: 0 })
        );
        assert_eq!(WitLine::parse("0.1"), Some(WitLine::V0_1));
        assert_eq!(WitLine::parse("0.2"), Some(WitLine::V0_2));
        assert_eq!(WitLine::parse("1.3"), None);
        assert_eq!(WitLine::parse("0.1.0"), None);
        assert_eq!(
            ostro_iface("journal", WIT_0_1_LATEST),
            format!("ostro:runtime/journal@{WIT_0_1_LATEST}")
        );
        assert_eq!(
            split_ostro("ostro:runtime/journal@0.1.0"),
            Some(("journal", "0.1.0"))
        );
        assert_eq!(split_ostro("ostro:runtime/journal"), Some(("journal", "")));
        assert_eq!(split_ostro("wasi:io/poll@0.2.0"), None);
        assert_eq!(split_ostro("ostro:runtimex/a@0.1.0"), None);
    }

    fn imp(name: &str, funcs: &[&str]) -> (String, Vec<String>) {
        (
            name.to_string(),
            funcs.iter().map(|f| f.to_string()).collect(),
        )
    }

    #[test]
    fn surface_selection_rules() {
        let exports = vec!["ostro:runtime/workflows@0.1.0".to_string()];
        let v01 = [WitLine::V0_1];
        // WASI imports are ignored; 0.1.0 functions are level 1.
        let sel = select_from_surface(
            &[
                imp("wasi:io/poll@0.2.6", &["poll"]),
                imp("ostro:runtime/journal@0.1.0", &["begin-step"]),
                imp("ostro:runtime/types@0.1.0", &[]),
            ],
            &exports,
            &v01,
        )
        .unwrap();
        assert_eq!(
            sel,
            WitSelection {
                line: WitLine::V0_1,
                min_level: 1
            }
        );
        // A package version 0.1.7 with only 0.1.0 functions is still level 1.
        let sel = select_from_surface(
            &[imp("ostro:runtime/context@0.1.7", &["info", "now"])],
            &["ostro:runtime/workflows@0.1.7".to_string()],
            &v01,
        )
        .unwrap();
        assert_eq!(sel.min_level, 1);
        // No ostro import at all: the export decides the line.
        assert_eq!(
            select_from_surface(&[], &exports, &v01).unwrap().line,
            WitLine::V0_1
        );
        assert_eq!(
            select_from_surface(&[], &[], &v01),
            Err(ValidateError::NoWitLine)
        );
        assert_eq!(
            select_from_surface(
                &[
                    imp("ostro:runtime/log@0.1.0", &["emit"]),
                    imp("ostro:runtime/log@0.2.0", &["emit"])
                ],
                &exports,
                &[WitLine::V0_1, WitLine::V0_2],
            ),
            Err(ValidateError::MixedWitLines(vec![
                "0.1".to_string(),
                "0.2".to_string()
            ]))
        );
        assert_eq!(
            select_from_surface(
                &[imp("ostro:runtime/log@0.2.0", &["emit"])],
                &["ostro:runtime/workflows@0.2.0".to_string()],
                &v01,
            ),
            Err(ValidateError::UnsupportedWitLine("0.2".to_string()))
        );
        assert_eq!(
            select_from_surface(&[imp("ostro:runtime/log", &["emit"])], &exports, &v01),
            Err(ValidateError::BadWitVersion(
                "ostro:runtime/log".to_string()
            ))
        );
        assert!(matches!(
            select_from_surface(
                &[imp("ostro:runtime/log@0.1.0", &["emit", "shout"])],
                &exports,
                &v01
            ),
            Err(ValidateError::UnknownWitFunction { .. })
        ));
    }

    #[test]
    fn introduced_in_table_matches_the_frozen_wit() {
        let wit = include_str!("../../../../wit/ostro-runtime-0.1.0/runtime.wit");
        let mut found: Vec<(String, Vec<String>)> = Vec::new();
        let mut current: Option<(String, Vec<String>)> = None;
        // Brace depth inside the current interface, and the open resource (at depth 2).
        let mut depth = 0usize;
        let mut resource: Option<String> = None;
        for line in wit.lines() {
            let t = line.trim();
            if t.starts_with("//") {
                continue;
            }
            if depth == 0 {
                if let Some(name) = t.strip_prefix("interface ") {
                    current = Some((name.trim_end_matches(" {").to_string(), Vec::new()));
                    depth = 1;
                }
                continue;
            }
            if depth == 1
                && let Some(name) = t.strip_prefix("resource ")
            {
                resource = Some(name.trim_end_matches(" {").to_string());
            }
            if let (Some(c), Some((name, rest))) = (current.as_mut(), t.split_once(':'))
                && rest.trim_start().starts_with("func(")
                && !name.contains(' ')
            {
                c.1.push(match (&resource, depth) {
                    (Some(r), 2) => format!("[method]{r}.{name}"),
                    _ => name.to_string(),
                });
            }
            depth += t.matches('{').count();
            depth -= t.matches('}').count();
            if depth == 1 {
                resource = None;
            }
            if depth == 0
                && let Some(c) = current.take()
            {
                found.push(c);
            }
        }
        let imported: Vec<(String, Vec<String>)> = found
            .into_iter()
            .filter(|(i, fs)| i != "workflows" && !fs.is_empty())
            .collect();
        let table: Vec<(String, Vec<String>)> = FUNCS_0_1_0
            .iter()
            .map(|(i, fs)| (i.to_string(), fs.iter().map(|f| f.to_string()).collect()))
            .collect();
        assert_eq!(imported, table);
    }
}
