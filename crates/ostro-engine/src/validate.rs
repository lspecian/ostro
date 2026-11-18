//! Component import/export validation at deploy time (ENG-02).
//!
//! A deployed module must be a Wasm component whose imports are limited to
//! `ostro:runtime/*@0.1.*` plus the deterministic WASI shim interfaces
//! ([`ALLOWED_WASI`], any `@0.2.*`), whose only export is
//! `ostro:runtime/workflows@0.1.*`, and which the host linker can pre-instantiate.
//! Every violation is collected (not first-fail) and returned in
//! [`EngineError::Validation`], each message naming the offending interface.

use wasmtime::Engine;
use wasmtime::component::{Component, Linker};

use crate::error::EngineError;

/// `ostro:runtime` interfaces a guest may import (any `@0.1.*`).
pub const ALLOWED_OSTRO: &[&str] = &[
    "ostro:runtime/types",
    "ostro:runtime/context",
    "ostro:runtime/journal",
    "ostro:runtime/effects",
    "ostro:runtime/log",
];

/// WASI interfaces implemented by the deterministic shim (`wit-wasi/shim.wit`), any
/// `@0.2.*`.
pub const ALLOWED_WASI: &[&str] = &[
    "wasi:io/error",
    "wasi:io/poll",
    "wasi:io/streams",
    "wasi:clocks/wall-clock",
    "wasi:clocks/monotonic-clock",
    "wasi:random/random",
    "wasi:random/insecure",
    "wasi:random/insecure-seed",
    "wasi:cli/environment",
    "wasi:cli/exit",
    "wasi:cli/stdin",
    "wasi:cli/stdout",
    "wasi:cli/stderr",
    "wasi:cli/terminal-input",
    "wasi:cli/terminal-output",
    "wasi:cli/terminal-stdin",
    "wasi:cli/terminal-stdout",
    "wasi:cli/terminal-stderr",
];

/// The one export every workflow component must have (any `@0.1.*`).
///
/// TODO(Phase 9): componentize-py adds an extra `exports` interface (S2). Decide then
/// whether to allow it; it is rejected for now.
pub const REQUIRED_EXPORT: &str = "ostro:runtime/workflows";

/// A component that passed deploy-time validation.
pub struct ValidatedComponent {
    pub component: Component,
    /// Full import names (`ns:pkg/iface@ver`), in component order.
    pub imports: Vec<String>,
    /// Full export names, in component order.
    pub exports: Vec<String>,
}

impl std::fmt::Debug for ValidatedComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidatedComponent")
            .field("imports", &self.imports)
            .field("exports", &self.exports)
            .finish_non_exhaustive()
    }
}

const WASM_MAGIC: &[u8; 4] = b"\0asm";
const CORE_VERSION: [u8; 4] = [0x01, 0x00, 0x00, 0x00];

/// Split `ns:pkg/iface@ver` into (`ns:pkg/iface`, `Some(ver)`).
fn split_version(name: &str) -> (&str, Option<&str>) {
    match name.split_once('@') {
        Some((base, ver)) => (base, Some(ver)),
        None => (name, None),
    }
}

/// `ver` is `major.minor.*` for the given `major.minor` prefix (e.g. "0.1").
fn version_matches(ver: Option<&str>, major_minor: &str) -> bool {
    ver.and_then(|v| v.strip_prefix(major_minor))
        .is_some_and(|rest| rest.starts_with('.'))
}

fn import_allowed(name: &str) -> bool {
    let (base, ver) = split_version(name);
    (ALLOWED_OSTRO.contains(&base) && version_matches(ver, "0.1"))
        || (ALLOWED_WASI.contains(&base) && version_matches(ver, "0.2"))
}

fn import_reason(name: &str) -> &'static str {
    if name.starts_with("wasi:filesystem/") {
        "Ostro guests have no filesystem"
    } else if name.starts_with("wasi:sockets/") || name.starts_with("wasi:http/") {
        "network access goes through ostro effects (http-fetch)"
    } else {
        "not part of the Ostro WIT/WASI allow-list"
    }
}

fn is_required_export(name: &str) -> bool {
    let (base, ver) = split_version(name);
    base == REQUIRED_EXPORT && version_matches(ver, "0.1")
}

/// Validate a deployed module against the Ostro WIT/WASI allow-list and `linker`.
///
/// Steps: (1) must be a component, not a core module; (2) every import allowed;
/// (3) exactly `ostro:runtime/workflows@0.1.*` exported; (4) only if (2) and (3) are
/// clean, `linker.instantiate_pre` must succeed (catches missing functions inside
/// allowed interfaces, e.g. a WASI function newer than the vendored WIT).
pub fn validate_component<T: 'static>(
    engine: &Engine,
    linker: &Linker<T>,
    bytes: &[u8],
) -> Result<ValidatedComponent, EngineError> {
    if bytes.len() >= 8 && &bytes[..4] == WASM_MAGIC && bytes[4..8] == CORE_VERSION {
        return Err(EngineError::Validation(vec![
            "not a component: Ostro accepts Wasm components (wasm32-wasip2 / componentize-*), \
             not core modules"
                .to_string(),
        ]));
    }
    let component = Component::from_binary(engine, bytes)
        .map_err(|e| EngineError::Validation(vec![format!("not a valid Wasm component: {e:#}")]))?;

    let ty = component.component_type();
    let imports: Vec<String> = ty.imports(engine).map(|(n, _)| n.to_string()).collect();
    let exports: Vec<String> = ty.exports(engine).map(|(n, _)| n.to_string()).collect();

    let mut violations = Vec::new();
    for name in &imports {
        if !import_allowed(name) {
            violations.push(format!(
                "import {name} is not allowed: {}",
                import_reason(name)
            ));
        }
    }
    let mut has_required = false;
    for name in &exports {
        if is_required_export(name) {
            has_required = true;
        } else {
            violations.push(format!(
                "export {name} is not allowed: Ostro workflows export {REQUIRED_EXPORT}, \
                 not a command/HTTP handler"
            ));
        }
    }
    if !has_required {
        violations.push(format!("missing required export {REQUIRED_EXPORT}@0.1.*"));
    }

    if violations.is_empty()
        && let Err(e) = linker.instantiate_pre(&component)
    {
        violations.push(format!("instantiate_pre failed: {e:#}"));
    }

    if violations.is_empty() {
        Ok(ValidatedComponent {
            component,
            imports,
            exports,
        })
    } else {
        Err(EngineError::Validation(violations))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_matching() {
        assert!(import_allowed("ostro:runtime/journal@0.1.0"));
        assert!(import_allowed("ostro:runtime/journal@0.1.7"));
        assert!(!import_allowed("ostro:runtime/journal@0.2.0"));
        assert!(!import_allowed("ostro:runtime/journal@0.10.0"));
        assert!(!import_allowed("ostro:runtime/journal"));
        assert!(!import_allowed("ostro:runtime/workflows@0.1.0"));
        assert!(import_allowed("wasi:io/poll@0.2.12"));
        assert!(import_allowed("wasi:io/poll@0.2.0"));
        assert!(!import_allowed("wasi:io/poll@0.3.0"));
        assert!(!import_allowed("wasi:filesystem/types@0.2.12"));
        assert!(is_required_export("ostro:runtime/workflows@0.1.0"));
        assert!(!is_required_export("ostro:runtime/workflows@1.0.0"));
        assert!(!is_required_export("wasi:cli/run@0.2.12"));
    }

    #[test]
    fn reasons_by_family() {
        assert_eq!(
            import_reason("wasi:filesystem/preopens@0.2.12"),
            "Ostro guests have no filesystem"
        );
        assert!(import_reason("wasi:sockets/tcp@0.2.12").contains("http-fetch"));
        assert!(import_reason("wasi:http/outgoing-handler@0.2.12").contains("http-fetch"));
        assert!(import_reason("acme:evil/x@1.0.0").contains("allow-list"));
    }
}
