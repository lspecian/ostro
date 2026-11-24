//! Component import/export validation at deploy time (ENG-02).
//!
//! A deployed module must be a Wasm component whose imports are limited to
//! `ostro:runtime/*` of **one** WIT line this build links (`@0.1.*` in production; 13-07,
//! [`linker_set::select`]) plus the deterministic WASI shim interfaces
//! ([`ALLOWED_WASI`], any `@0.2.*`), whose only export is
//! `ostro:runtime/workflows` of that line (plus componentize-py's `exports` export with its
//! exact `init` shape, see [`COMPONENTIZE_PY_EXPORT`]), and which the host linker can
//! pre-instantiate. The line and the module's minimum feature level are returned in
//! [`ValidatedComponent::selection`].
//! Every violation is collected (not first-fail) and returned in
//! [`EngineError::Validation`], each message naming the offending interface.

use wasmparser::{
    ComponentAlias, ComponentExternalKind, ComponentInstance, ComponentOuterAliasKind,
    ComponentTypeRef, Encoding, Instance, Parser, Payload,
};
use wasmtime::Engine;
use wasmtime::component::types::{ComponentItem, Type};
use wasmtime::component::{Component, Linker};

use crate::error::EngineError;
use crate::host::linker_set::{self, WitLine, WitSelection};

/// WIT line selection errors (13-07, ENG-10). Every one is an upload error: it reaches
/// callers as an [`EngineError::Validation`] message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidateError {
    /// The component imports `ostro:runtime` interfaces from more than one major line.
    #[error(
        "component mixes WIT lines {}: every ostro:runtime import and export must belong to \
         one line",
        .0.join(", ")
    )]
    MixedWitLines(Vec<String>),
    /// The component's line is not linked by this build.
    #[error(
        "WIT line {0} is not supported by this host (supported: {supported})",
        supported = linker_set::supported_lines()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )]
    UnsupportedWitLine(String),
    /// An `ostro:runtime` name without a parsable `@major.minor.patch` version.
    #[error("{0} has no valid @major.minor.patch version")]
    BadWitVersion(String),
    /// The component neither imports nor exports anything from `ostro:runtime`.
    #[error("component has no ostro:runtime import or export (no WIT line)")]
    NoWitLine,
    /// An imported function this build does not know on the selected line.
    #[error("import {import} uses function {func}, unknown to WIT line {line} on this host")]
    UnknownWitFunction {
        line: String,
        import: String,
        func: String,
    },
    /// The binary could not be parsed while reading its import names.
    #[error("not a valid Wasm component: {0}")]
    Unparsable(String),
}

impl From<ValidateError> for EngineError {
    fn from(e: ValidateError) -> Self {
        EngineError::Validation(vec![e.to_string()])
    }
}

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
pub const REQUIRED_EXPORT: &str = "ostro:runtime/workflows";

/// The extra bare export every componentize-py component carries (S2, 09-RESEARCH ENG-1).
///
/// Rule (plan 09-07): a component may have exactly one extra export named `exports`, only
/// next to [`REQUIRED_EXPORT`], and only if its type is structurally identical to
/// componentize-py 0.25.1's `init(app-name, symbols, stub-wasi) -> result<_, string>`
/// interface (see [`componentize_py_init_signature`]). The same name with any other type,
/// or any other extra export, is a violation. The host never calls `init`: componentize-py
/// runs it at build time (pre-initialisation), it is only left in the component's surface.
pub const COMPONENTIZE_PY_EXPORT: &str = "exports";

/// The function inside [`COMPONENTIZE_PY_EXPORT`].
const COMPONENTIZE_PY_FUNC: &str = "init";

/// Canonical rendering (see [`render_type`]) of componentize-py 0.25.1's `exports.init`,
/// transcribed from `jco wit` of a real build (`conformance/fixtures/py-min`).
pub fn componentize_py_init_signature() -> String {
    let function = "record{protocol: string, name: string}";
    let constructor = "record{module: string, protocol: string}";
    let static_ = "record{module: string, protocol: string, name: string}";
    let kind = format!(
        "variant{{freestanding({function}), constructor({constructor}), method(string), \
         static({static_})}}"
    );
    let return_style = "variant{none, normal, result}";
    let function_export = format!("record{{kind: {kind}, return-style: {return_style}}}");
    let resource = "record{package: string, name: string}";
    let record = "record{package: string, name: string, fields: list<string>}";
    let flags = "record{package: string, name: string, u32-count: u32}";
    let tuple = "record{count: u32}";
    let case = "record{name: string, has-payload: bool}";
    let variant = format!("record{{package: string, name: string, cases: list<{case}>}}");
    let enum_ = "record{package: string, name: string, count: u32}";
    let option_kind = "enum{non-nesting, nesting}";
    let result_record = "record{has-ok: bool, has-err: bool}";
    let symbols = format!(
        "record{{exports: list<{function_export}>, resources: list<{resource}>, \
         records: list<{record}>, flags: list<{flags}>, tuples: list<{tuple}>, \
         variants: list<{variant}>, enums: list<{enum_}>, options: list<{option_kind}>, \
         results: list<{result_record}>}}"
    );
    format!(
        "{COMPONENTIZE_PY_FUNC}(app-name: string, symbols: {symbols}, stub-wasi: bool) -> \
         result<_, string>"
    )
}

/// Structural rendering of an interface type (type names are not part of a component
/// type, field/case names are). Owned/borrowed handles, async types and maps render as
/// opaque markers: none appear in the shapes compared here.
pub fn render_type(t: &Type) -> String {
    fn join(it: impl Iterator<Item = String>) -> String {
        it.collect::<Vec<_>>().join(", ")
    }
    match t {
        Type::Bool => "bool".into(),
        Type::S8 => "s8".into(),
        Type::U8 => "u8".into(),
        Type::S16 => "s16".into(),
        Type::U16 => "u16".into(),
        Type::S32 => "s32".into(),
        Type::U32 => "u32".into(),
        Type::S64 => "s64".into(),
        Type::U64 => "u64".into(),
        Type::Float32 => "f32".into(),
        Type::Float64 => "f64".into(),
        Type::Char => "char".into(),
        Type::String => "string".into(),
        Type::List(l) => format!("list<{}>", render_type(&l.ty())),
        Type::FixedLengthList(l) => format!("list<{}, {}>", render_type(&l.ty()), l.len()),
        Type::Record(r) => format!(
            "record{{{}}}",
            join(
                r.fields()
                    .map(|f| format!("{}: {}", f.name, render_type(&f.ty)))
            )
        ),
        Type::Tuple(t) => format!("tuple<{}>", join(t.types().map(|t| render_type(&t)))),
        Type::Variant(v) => format!(
            "variant{{{}}}",
            join(v.cases().map(|c| match c.ty {
                Some(t) => format!("{}({})", c.name, render_type(&t)),
                None => c.name.to_string(),
            }))
        ),
        Type::Enum(e) => format!("enum{{{}}}", join(e.names().map(str::to_string))),
        Type::Flags(f) => format!("flags{{{}}}", join(f.names().map(str::to_string))),
        Type::Option(o) => format!("option<{}>", render_type(&o.ty())),
        Type::Result(r) => {
            let side = |t: Option<Type>| t.map_or_else(|| "_".to_string(), |t| render_type(&t));
            match (r.ok(), r.err()) {
                (None, None) => "result".into(),
                (ok, err) => format!("result<{}, {}>", side(ok), side(err)),
            }
        }
        Type::Own(_) => "own<resource>".into(),
        Type::Borrow(_) => "borrow<resource>".into(),
        Type::Map(_) => "map".into(),
        Type::Future(_) => "future".into(),
        Type::Stream(_) => "stream".into(),
        Type::ErrorContext => "error-context".into(),
    }
}

/// Render the functions exported by `item` (an instance), or describe why it has no
/// function-only shape. Type exports are skipped (they carry no behaviour).
fn render_instance_funcs(engine: &Engine, item: &ComponentItem) -> Result<Vec<String>, String> {
    let ComponentItem::ComponentInstance(inst) = item else {
        return Err("not an instance".into());
    };
    let mut funcs = Vec::new();
    for (name, ext) in inst.exports(engine) {
        match &ext.ty {
            ComponentItem::Type(_) => {}
            ComponentItem::ComponentFunc(f) => {
                let params = f
                    .params()
                    .map(|(n, t)| format!("{n}: {}", render_type(&t)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let results: Vec<String> = f.results().map(|t| render_type(&t)).collect();
                let mut sig = format!("{name}({params})");
                if !results.is_empty() {
                    sig.push_str(" -> ");
                    sig.push_str(&results.join(", "));
                }
                funcs.push(sig);
            }
            _ => return Err(format!("item {name} is not a function")),
        }
    }
    Ok(funcs)
}

/// `None` if `item` is exactly componentize-py's `exports` interface, else the reason.
fn componentize_py_mismatch(engine: &Engine, item: &ComponentItem) -> Option<String> {
    match render_instance_funcs(engine, item) {
        Ok(funcs) if funcs == [componentize_py_init_signature()] => None,
        Ok(funcs) => Some(format!("functions [{}]", funcs.join("; "))),
        Err(e) => Some(e),
    }
}

/// A component that passed deploy-time validation.
pub struct ValidatedComponent {
    pub component: Component,
    /// Full import names (`ns:pkg/iface@ver`), in component order.
    pub imports: Vec<String>,
    /// Full export names, in component order.
    pub exports: Vec<String>,
    /// Core instances one instantiation creates ([`count_core_instances`]): the weight of
    /// the module in the core-instance admission budget (ENG-2).
    pub core_instances: u32,
    /// The WIT line and minimum feature level ([`linker_set::select`], 13-07).
    pub selection: WitSelection,
}

impl std::fmt::Debug for ValidatedComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidatedComponent")
            .field("imports", &self.imports)
            .field("exports", &self.exports)
            .field("core_instances", &self.core_instances)
            .field("selection", &self.selection)
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

/// `ver` is a version on WIT line `line`.
fn on_line(ver: Option<&str>, line: WitLine) -> bool {
    ver.and_then(WitLine::of_version) == Some(line)
}

/// `line` is the component's selected WIT line; `None` when selection failed (that error is
/// reported once and `ostro:runtime` imports are then only checked by interface name).
fn import_allowed(name: &str, line: Option<WitLine>) -> bool {
    let (base, ver) = split_version(name);
    (ALLOWED_OSTRO.contains(&base) && line.is_none_or(|l| on_line(ver, l)))
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

fn is_required_export(name: &str, line: Option<WitLine>) -> bool {
    let (base, ver) = split_version(name);
    base == REQUIRED_EXPORT && on_line(ver, line.unwrap_or(WitLine::V0_1))
}

/// Core instances one instantiation of the component creates (plan 09-07, ENG-2).
///
/// Counts core `instantiate` entries only (a `from-exports` bag allocates nothing, S9 Q5),
/// with nested components flattened the way Wasmtime inlines them: a component
/// instantiation costs its component's own count, every time it is instantiated. The
/// component index space follows the binary: nested definitions, imported components
/// (unknown cost 0: Ostro components import none), outer aliases (the outer entry's cost)
/// and component re-exports. Wasmtime-generated adapter modules are not visible here;
/// `tests/core_budget.rs` pins the result against the pool limit for every guest class.
pub fn count_core_instances(wasm: &[u8]) -> Result<u32, String> {
    #[derive(Default)]
    struct Scope {
        total: u64,
        comps: Vec<u64>,
    }
    // `None` marks an open core module.
    let mut stack: Vec<Option<Scope>> = Vec::new();
    let mut root: Option<u64> = None;
    for payload in Parser::new(0).parse_all(wasm) {
        let payload = payload.map_err(|e| format!("parse: {e}"))?;
        match payload {
            Payload::Version { encoding, .. } => match encoding {
                Encoding::Component => stack.push(Some(Scope::default())),
                Encoding::Module => stack.push(None),
            },
            Payload::End(_) => match stack.pop() {
                Some(Some(scope)) => match stack.last_mut() {
                    Some(Some(parent)) => parent.comps.push(scope.total),
                    Some(None) => return Err("component nested in a core module".into()),
                    None => root = Some(scope.total),
                },
                Some(None) => {}
                None => return Err("unbalanced end".into()),
            },
            Payload::InstanceSection(reader) => {
                let Some(Some(scope)) = stack.last_mut() else {
                    continue;
                };
                for inst in reader {
                    if let Instance::Instantiate { .. } = inst.map_err(|e| format!("{e}"))? {
                        scope.total += 1;
                    }
                }
            }
            Payload::ComponentInstanceSection(reader) => {
                let Some(Some(scope)) = stack.last_mut() else {
                    continue;
                };
                for inst in reader {
                    if let ComponentInstance::Instantiate {
                        component_index, ..
                    } = inst.map_err(|e| format!("{e}"))?
                    {
                        let cost = scope.comps.get(component_index as usize).copied();
                        scope.total += cost.unwrap_or(0);
                    }
                }
            }
            Payload::ComponentImportSection(reader) => {
                let Some(Some(scope)) = stack.last_mut() else {
                    continue;
                };
                for imp in reader {
                    if let ComponentTypeRef::Component(_) = imp.map_err(|e| format!("{e}"))?.ty {
                        scope.comps.push(0);
                    }
                }
            }
            Payload::ComponentExportSection(reader) => {
                let Some(Some(scope)) = stack.last_mut() else {
                    continue;
                };
                for exp in reader {
                    let exp = exp.map_err(|e| format!("{e}"))?;
                    if exp.kind == ComponentExternalKind::Component {
                        let cost = scope.comps.get(exp.index as usize).copied();
                        scope.comps.push(cost.unwrap_or(0));
                    }
                }
            }
            Payload::ComponentAliasSection(reader) => {
                for alias in reader {
                    let alias = alias.map_err(|e| format!("{e}"))?;
                    let cost = match alias {
                        ComponentAlias::Outer {
                            kind: ComponentOuterAliasKind::Component,
                            count,
                            index,
                        } => {
                            let depth = stack.len().checked_sub(1 + count as usize);
                            Some(
                                depth
                                    .and_then(|d| stack[d].as_ref())
                                    .and_then(|s| s.comps.get(index as usize).copied())
                                    .unwrap_or(0),
                            )
                        }
                        ComponentAlias::InstanceExport {
                            kind: ComponentExternalKind::Component,
                            ..
                        } => Some(0),
                        _ => None,
                    };
                    if let (Some(cost), Some(Some(scope))) = (cost, stack.last_mut()) {
                        scope.comps.push(cost);
                    }
                }
            }
            _ => {}
        }
    }
    let total = root.ok_or_else(|| "not a component".to_string())?;
    u32::try_from(total).map_err(|_| format!("{total} core instances"))
}

/// Validate a deployed module against the Ostro WIT/WASI allow-list and `linker`.
///
/// Steps: (1) must be a component, not a core module; (2) one supported WIT line
/// ([`linker_set::select`]: mixed lines, an unsupported line or an unknown function are
/// violations) and every import allowed; (3) exactly `ostro:runtime/workflows` of that line
/// exported; (4) only if (2) and (3) are clean, `linker.instantiate_pre` must succeed
/// (catches missing functions inside allowed interfaces, e.g. a WASI function newer than
/// the vendored WIT). `linker` must be the linker of the component's line
/// ([`linker_set::LinkerSet::for_component`]).
pub fn validate_component<T: 'static>(
    engine: &Engine,
    linker: &Linker<T>,
    bytes: &[u8],
) -> Result<ValidatedComponent, EngineError> {
    reject_core_module(bytes)?;
    let component = Component::from_binary(engine, bytes)
        .map_err(|e| EngineError::Validation(vec![format!("not a valid Wasm component: {e:#}")]))?;
    validate_loaded(engine, linker, &component, bytes, None)
}

/// Step (1) of [`validate_component`]: reject a core module (by its header) before any
/// compile or cache lookup.
pub fn reject_core_module(bytes: &[u8]) -> Result<(), EngineError> {
    if bytes.len() >= 8 && &bytes[..4] == WASM_MAGIC && bytes[4..8] == CORE_VERSION {
        return Err(EngineError::Validation(vec![
            "not a component: Ostro accepts Wasm components (wasm32-wasip2 / componentize-*), \
             not core modules"
                .to_string(),
        ]));
    }
    Ok(())
}

/// Steps (2)-(4) of [`validate_component`] on an already loaded component (e.g. one
/// deserialized from the AOT cache): validation runs on every load, never skipped on
/// cache hits.
///
/// `bytes` are the component's binary (the core-instance count is read from it,
/// [`count_core_instances`]); `max_core_instances` is the node's core-instance capacity
/// (`EngineConfig::effective_total_core_instances` with the pool enabled): a component
/// needing more could never be instantiated and is a violation.
pub fn validate_loaded<T: 'static>(
    engine: &Engine,
    linker: &Linker<T>,
    component: &Component,
    bytes: &[u8],
    max_core_instances: Option<u32>,
) -> Result<ValidatedComponent, EngineError> {
    let component = component.clone();
    let ty = component.component_type();
    let imports: Vec<String> = ty.imports(engine).map(|(n, _)| n.to_string()).collect();
    let exports: Vec<String> = ty.exports(engine).map(|(n, _)| n.to_string()).collect();

    let mut violations = Vec::new();
    let (selection, selection_err) = match linker_set::select_lines(&component, engine) {
        Ok(sel) => (Some(sel), None),
        Err(e) => (None, Some(e)),
    };
    let line = selection.as_ref().map(|s| s.line);
    for name in &imports {
        if !import_allowed(name, line) {
            violations.push(format!(
                "import {name} is not allowed: {}",
                import_reason(name)
            ));
        }
    }
    let has_required = exports.iter().any(|n| is_required_export(n, line));
    for (name, ext) in ty.exports(engine) {
        if is_required_export(name, line) {
            continue;
        }
        if name == COMPONENTIZE_PY_EXPORT && has_required {
            match componentize_py_mismatch(engine, &ext.ty) {
                None => continue,
                Some(why) => {
                    violations.push(format!(
                        "export {name} has an unexpected type: only componentize-py 0.25.1's \
                         init(app-name, symbols, stub-wasi) interface is allowed under this \
                         name (found {why})"
                    ));
                    continue;
                }
            }
        }
        violations.push(format!(
            "export {name} is not allowed: Ostro workflows export {REQUIRED_EXPORT}, \
             not a command/HTTP handler"
        ));
    }
    if !has_required {
        violations.push(format!(
            "missing required export {REQUIRED_EXPORT}@{}.*",
            line.unwrap_or(WitLine::V0_1)
        ));
    }
    // Reported after the allow-list; "no WIT line" is implied by a missing export.
    match selection_err {
        Some(ValidateError::NoWitLine) if !has_required => {}
        Some(e) => violations.push(e.to_string()),
        None => {}
    }
    let core_instances = match count_core_instances(bytes) {
        Ok(n) => n,
        Err(e) => {
            violations.push(format!("cannot count core instances: {e}"));
            0
        }
    };
    if let Some(max) = max_core_instances
        && core_instances > max
    {
        violations.push(format!(
            "component needs {core_instances} core instances per instance, more than the \
             node's pool holds ({max}, engine.pool_total_core_instances): it could never \
             be instantiated"
        ));
    }

    if violations.is_empty()
        && let Err(e) = linker.instantiate_pre(&component)
    {
        violations.push(format!("instantiate_pre failed: {e:#}"));
    }

    match (violations.is_empty(), selection) {
        (true, Some(selection)) => Ok(ValidatedComponent {
            component,
            imports,
            exports,
            core_instances,
            selection,
        }),
        _ => Err(EngineError::Validation(violations)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_matching() {
        let l = Some(WitLine::V0_1);
        assert!(import_allowed("ostro:runtime/journal@0.1.0", l));
        assert!(import_allowed("ostro:runtime/journal@0.1.7", l));
        assert!(!import_allowed("ostro:runtime/journal@0.2.0", l));
        assert!(!import_allowed("ostro:runtime/journal@0.10.0", l));
        assert!(!import_allowed("ostro:runtime/journal", l));
        assert!(!import_allowed("ostro:runtime/workflows@0.1.0", l));
        assert!(import_allowed("wasi:io/poll@0.2.12", l));
        assert!(import_allowed("wasi:io/poll@0.2.0", l));
        assert!(!import_allowed("wasi:io/poll@0.3.0", l));
        assert!(!import_allowed("wasi:filesystem/types@0.2.12", l));
        assert!(import_allowed(
            "ostro:runtime/journal@0.2.0",
            Some(WitLine::V0_2)
        ));
        assert!(import_allowed("ostro:runtime/journal@0.2.0", None));
        assert!(is_required_export("ostro:runtime/workflows@0.1.0", l));
        assert!(!is_required_export("ostro:runtime/workflows@1.0.0", l));
        assert!(!is_required_export("wasi:cli/run@0.2.12", l));
        assert!(is_required_export(
            "ostro:runtime/workflows@0.2.0",
            Some(WitLine::V0_2)
        ));
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
