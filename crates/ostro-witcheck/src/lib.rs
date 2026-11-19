//! ostro-witcheck: WIT-07 freeze tooling (plan 05-05).
//!
//! After the 05-20 freeze, `wit/ostro-runtime-0.1.0/runtime.wit` never changes again. New
//! runtime surface ships as a new `ostro:runtime@0.1.N` package directory next to it, and
//! that package must be **additive** over its predecessor: new functions, new interfaces
//! and new types only. Records, variants and enums never gain fields or cases, no function
//! is removed or retyped, no interface is renamed, and the exported `workflows` interface
//! gains nothing (an old guest would not provide a new export).
//!
//! [`check_additive`] proves that predicate with two independent checkers:
//! 1. [`wit_component::semver_check`]: synthesises a component of the old world and
//!    type-checks it against the new world through wasmparser. It runs on one `Resolve`
//!    holding both packages (it ignores versions and requires equal package names).
//! 2. A structural diff over two separate `Resolve`s ([`structural_diff`]): every
//!    interface, named type and function of the old package must exist in the new one with
//!    an identical resolved shape, every old world import/export must still be there, and
//!    exported interfaces must not gain functions. It also checks the package name and that
//!    the version is a semver-compatible bump. Its messages are human-readable, which
//!    `semver_check`'s wasmparser errors are not.
//!
//! A candidate is accepted only if **both** checkers accept it. If `semver_check` cannot
//! load the two packages into one `Resolve`, that is reported in [`Report::semver`] and the
//! structural diff decides alone. The fixture suite (`tests/semver_fixtures.rs`) records
//! which checker catches which class of change.

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};

use wit_parser::{
    Function, FunctionKind, Handle, InterfaceId, PackageId, Resolve, Type, TypeDefKind, TypeId,
    TypeOwner, WorldItem, WorldKey,
};

/// The world every Ostro workflow component targets.
pub const WORLD: &str = "workflow";

/// Which checker reported an incompatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checker {
    /// `wit_component::semver_check`.
    SemverCheck,
    /// The wit-parser structural diff.
    Structural,
    /// A package failed to parse or resolve on its own.
    Load,
}

impl fmt::Display for Checker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Checker::SemverCheck => "semver_check",
            Checker::Structural => "structural",
            Checker::Load => "load",
        })
    }
}

/// One reason a candidate package is not additive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incompat {
    pub checker: Checker,
    pub message: String,
}

impl fmt::Display for Incompat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.checker, self.message)
    }
}

/// Outcome of the `semver_check` leg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemverOutcome {
    /// `semver_check` accepted the candidate.
    Compatible,
    /// `semver_check` rejected the candidate (error chain, flattened).
    Incompatible(String),
    /// Both packages could not be loaded into one `Resolve`; the structural diff decides.
    LoadFailed(String),
}

/// Full result of comparing two packages; see [`check_additive_report`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub semver: SemverOutcome,
    /// Structural incompatibilities (empty = structurally additive).
    pub structural: Vec<String>,
    /// Set when a package fails to load on its own (the candidate is then rejected).
    pub load_error: Option<String>,
}

impl Report {
    /// Every incompatibility found, tagged with the checker that found it.
    pub fn incompatibilities(&self) -> Vec<Incompat> {
        let mut out = Vec::new();
        if let Some(e) = &self.load_error {
            out.push(Incompat {
                checker: Checker::Load,
                message: e.clone(),
            });
        }
        if let SemverOutcome::Incompatible(e) = &self.semver {
            out.push(Incompat {
                checker: Checker::SemverCheck,
                message: e.clone(),
            });
        }
        out.extend(self.structural.iter().map(|m| Incompat {
            checker: Checker::Structural,
            message: m.clone(),
        }));
        out
    }

    pub fn is_additive(&self) -> bool {
        self.incompatibilities().is_empty()
    }
}

/// Prove `new_dir` is an additive successor of `prev_dir` (both WIT package directories).
///
/// Returns every incompatibility found by either checker.
pub fn check_additive(
    prev_dir: impl AsRef<Path>,
    new_dir: impl AsRef<Path>,
) -> Result<(), Vec<Incompat>> {
    let report = check_additive_report(prev_dir, new_dir);
    let errs = report.incompatibilities();
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}

/// Run both checkers and return their individual results.
pub fn check_additive_report(prev_dir: impl AsRef<Path>, new_dir: impl AsRef<Path>) -> Report {
    let (prev_dir, new_dir) = (prev_dir.as_ref(), new_dir.as_ref());
    let (structural, load_error, same_name) = match (load_alone(prev_dir), load_alone(new_dir)) {
        (Ok(prev), Ok(new)) => {
            let same =
                prev.resolve.packages[prev.package].name == new.resolve.packages[new.package].name;
            (structural_diff(&prev, &new), None, same)
        }
        (Err(e), _) => (
            Vec::new(),
            Some(format!("{}: {e}", prev_dir.display())),
            false,
        ),
        (_, Err(e)) => (
            Vec::new(),
            Some(format!("{}: {e}", new_dir.display())),
            false,
        ),
    };
    // wit-parser panics when one `Resolve` receives the same package name and version
    // twice, so the semver leg needs two distinct versions (the structural leg already
    // rejects a non-increasing version).
    let semver = if same_name {
        SemverOutcome::LoadFailed(
            "both directories declare the same package name and version".into(),
        )
    } else {
        run_semver_check(prev_dir, new_dir)
    };
    Report {
        semver,
        structural,
        load_error,
    }
}

/// `semver_check` over one `Resolve` holding both packages.
fn run_semver_check(prev_dir: &Path, new_dir: &Path) -> SemverOutcome {
    let mut resolve = Resolve::default();
    let loaded = resolve
        .push_dir(prev_dir)
        .and_then(|(prev_pkg, _)| Ok((prev_pkg, resolve.push_dir(new_dir)?.0)))
        .and_then(|(prev_pkg, new_pkg)| {
            Ok((
                resolve.select_world(&[prev_pkg], Some(WORLD))?,
                resolve.select_world(&[new_pkg], Some(WORLD))?,
            ))
        })
        .map_err(|e| flat(&e));
    match loaded {
        Err(e) => SemverOutcome::LoadFailed(e),
        Ok((prev, new)) => match wit_component::semver_check(resolve, prev, new) {
            Ok(()) => SemverOutcome::Compatible,
            Err(e) => SemverOutcome::Incompatible(flat(&e)),
        },
    }
}

/// Flatten an error chain (`anyhow` alternate form) without a direct `anyhow` dependency.
fn flat(e: &impl fmt::Display) -> String {
    format!("{e:#}")
}

/// A WIT package loaded on its own.
pub struct Loaded {
    pub resolve: Resolve,
    pub package: PackageId,
}

/// Parse and resolve one package directory into its own `Resolve`.
pub fn load_alone(dir: &Path) -> Result<Loaded, String> {
    let mut resolve = Resolve::default();
    let (package, _) = resolve.push_dir(dir).map_err(|e| format!("{e:#}"))?;
    Ok(Loaded { resolve, package })
}

/// Structural additivity diff between two separately loaded packages.
///
/// Returns one human-readable line per incompatibility (empty = additive).
pub fn structural_diff(prev: &Loaded, new: &Loaded) -> Vec<String> {
    let mut errs = Vec::new();
    let (pr, nr) = (&prev.resolve, &new.resolve);
    let (pp, np) = (&pr.packages[prev.package], &nr.packages[new.package]);

    if (&pp.name.namespace, &pp.name.name) != (&np.name.namespace, &np.name.name) {
        errs.push(format!(
            "package renamed: {}:{} -> {}:{}",
            pp.name.namespace, pp.name.name, np.name.namespace, np.name.name
        ));
    }
    match (&pp.name.version, &np.name.version) {
        (Some(pv), Some(nv)) => {
            let same_line = if pv.major == 0 {
                nv.major == 0 && nv.minor == pv.minor
            } else {
                nv.major == pv.major
            };
            if !same_line || nv <= pv {
                errs.push(format!(
                    "version {nv} is not a semver-compatible successor of {pv}"
                ));
            }
        }
        (pv, nv) => errs.push(format!(
            "both packages need a version (prev {pv:?}, new {nv:?})"
        )),
    }

    // Interfaces: every old interface, type and function survives unchanged.
    for (iname, &piid) in &pp.interfaces {
        let Some(&niid) = np.interfaces.get(iname) else {
            errs.push(format!("interface `{iname}` removed or renamed"));
            continue;
        };
        diff_interface(pr, piid, nr, niid, iname, &mut errs);
    }

    // Worlds: every old world survives; imports may grow, exports may not.
    for (wname, &pwid) in &pp.worlds {
        let Some(&nwid) = np.worlds.get(wname) else {
            errs.push(format!("world `{wname}` removed or renamed"));
            continue;
        };
        let (pw, nw) = (&pr.worlds[pwid], &nr.worlds[nwid]);
        let pimports = world_items(pr, &pw.imports);
        let nimports = world_items(nr, &nw.imports);
        for (key, shape) in &pimports {
            match nimports.iter().find(|(k, _)| k == key) {
                None => errs.push(format!(
                    "world `{wname}`: import `{key}` removed or renamed"
                )),
                Some((_, nshape)) if nshape != shape => errs.push(format!(
                    "world `{wname}`: import `{key}` changed: `{shape}` -> `{nshape}`"
                )),
                Some(_) => {}
            }
        }
        let pexports = world_items(pr, &pw.exports);
        let nexports = world_items(nr, &nw.exports);
        let pkeys: BTreeSet<_> = pexports.iter().map(|(k, _)| k.clone()).collect();
        let nkeys: BTreeSet<_> = nexports.iter().map(|(k, _)| k.clone()).collect();
        for k in pkeys.difference(&nkeys) {
            errs.push(format!("world `{wname}`: export `{k}` removed or renamed"));
        }
        for k in nkeys.difference(&pkeys) {
            errs.push(format!(
                "world `{wname}`: new export `{k}` (old guests do not provide it)"
            ));
        }
        for (key, shape) in &pexports {
            if let Some((_, nshape)) = nexports.iter().find(|(k, _)| k == key)
                && nshape != shape
            {
                errs.push(format!(
                    "world `{wname}`: export `{key}` changed: `{shape}` -> `{nshape}`"
                ));
            }
        }
        // Exported interfaces must not gain functions: an old guest would not provide them.
        for (key, item) in &pw.exports {
            let WorldItem::Interface { id: pid, .. } = item else {
                continue;
            };
            let k = key_name(pr, key);
            let Some(nid) = nw.exports.iter().find_map(|(nk, ni)| match ni {
                WorldItem::Interface { id, .. } if key_name(nr, nk) == k => Some(*id),
                _ => None,
            }) else {
                continue;
            };
            let pfuncs: BTreeSet<_> = pr.interfaces[*pid].functions.keys().collect();
            for f in nr.interfaces[nid].functions.keys() {
                if !pfuncs.contains(f) {
                    errs.push(format!(
                        "world `{wname}`: exported interface `{k}` gained function `{f}` \
                         (old guests do not provide it)"
                    ));
                }
            }
        }
    }
    errs
}

fn diff_interface(
    pr: &Resolve,
    piid: InterfaceId,
    nr: &Resolve,
    niid: InterfaceId,
    iname: &str,
    errs: &mut Vec<String>,
) {
    let (pi, ni) = (&pr.interfaces[piid], &nr.interfaces[niid]);
    for (tname, &ptid) in &pi.types {
        let Some(&ntid) = ni.types.get(tname) else {
            errs.push(format!("type `{iname}.{tname}` removed or renamed"));
            continue;
        };
        let (ps, ns) = (typedef_shape(pr, ptid), typedef_shape(nr, ntid));
        if ps != ns {
            errs.push(format!(
                "type `{iname}.{tname}` changed shape: `{ps}` -> `{ns}`"
            ));
        }
    }
    for (fname, pf) in &pi.functions {
        let Some(nf) = ni.functions.get(fname) else {
            errs.push(format!("function `{iname}.{fname}` removed or renamed"));
            continue;
        };
        let (ps, ns) = (func_shape(pr, pf), func_shape(nr, nf));
        if ps != ns {
            errs.push(format!(
                "function `{iname}.{fname}` changed signature: `{ps}` -> `{ns}`"
            ));
        }
    }
}

/// Render world items as `(key, shape)` in declaration order.
fn world_items(
    r: &Resolve,
    items: &wit_parser::IndexMap<WorldKey, WorldItem>,
) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(k, item)| {
            let shape = match item {
                WorldItem::Interface { id, .. } => format!("interface {}", iface_name(r, *id)),
                WorldItem::Function(f) => func_shape(r, f),
                WorldItem::Type { id, .. } => typedef_shape(r, *id),
            };
            (key_name(r, k), shape)
        })
        .collect()
}

fn key_name(r: &Resolve, k: &WorldKey) -> String {
    match k {
        WorldKey::Name(n) => n.clone(),
        WorldKey::Interface(id) => iface_name(r, *id),
    }
}

/// Interface name without the package version (versions are compared separately).
fn iface_name(r: &Resolve, id: InterfaceId) -> String {
    let i = &r.interfaces[id];
    let name = i.name.clone().unwrap_or_else(|| "<inline>".into());
    match i.package {
        Some(p) => {
            let pn = &r.packages[p].name;
            format!("{}:{}/{name}", pn.namespace, pn.name)
        }
        None => name,
    }
}

/// A reference to a type: named types render as `owner.name`, anonymous ones inline.
fn type_ref(r: &Resolve, ty: &Type) -> String {
    match ty {
        Type::Bool => "bool".into(),
        Type::U8 => "u8".into(),
        Type::U16 => "u16".into(),
        Type::U32 => "u32".into(),
        Type::U64 => "u64".into(),
        Type::S8 => "s8".into(),
        Type::S16 => "s16".into(),
        Type::S32 => "s32".into(),
        Type::S64 => "s64".into(),
        Type::F32 => "f32".into(),
        Type::F64 => "f64".into(),
        Type::Char => "char".into(),
        Type::String => "string".into(),
        Type::ErrorContext => "error-context".into(),
        Type::Id(id) => {
            let td = &r.types[*id];
            match &td.name {
                Some(n) => format!("{}.{n}", owner_name(r, td.owner)),
                None => kind_shape(r, &td.kind),
            }
        }
    }
}

fn owner_name(r: &Resolve, owner: TypeOwner) -> String {
    match owner {
        TypeOwner::Interface(i) => iface_name(r, i),
        TypeOwner::World(w) => format!("world {}", r.worlds[w].name),
        TypeOwner::None => "<none>".into(),
    }
}

/// The full definition of a (named) type.
fn typedef_shape(r: &Resolve, id: TypeId) -> String {
    kind_shape(r, &r.types[id].kind)
}

fn opt_ref(r: &Resolve, t: &Option<Type>) -> String {
    t.as_ref().map_or_else(|| "_".into(), |t| type_ref(r, t))
}

fn kind_shape(r: &Resolve, kind: &TypeDefKind) -> String {
    let list = |v: Vec<String>| v.join(", ");
    match kind {
        TypeDefKind::Record(rec) => format!(
            "record {{ {} }}",
            list(
                rec.fields
                    .iter()
                    .map(|f| format!("{}: {}", f.name, type_ref(r, &f.ty)))
                    .collect()
            )
        ),
        TypeDefKind::Resource => "resource".into(),
        TypeDefKind::Handle(Handle::Own(t)) => format!("own<{}>", type_ref(r, &Type::Id(*t))),
        TypeDefKind::Handle(Handle::Borrow(t)) => {
            format!("borrow<{}>", type_ref(r, &Type::Id(*t)))
        }
        TypeDefKind::Flags(fl) => format!(
            "flags {{ {} }}",
            list(fl.flags.iter().map(|f| f.name.clone()).collect())
        ),
        TypeDefKind::Tuple(t) => format!(
            "tuple<{}>",
            list(t.types.iter().map(|t| type_ref(r, t)).collect())
        ),
        TypeDefKind::Variant(v) => format!(
            "variant {{ {} }}",
            list(
                v.cases
                    .iter()
                    .map(|c| match &c.ty {
                        Some(t) => format!("{}({})", c.name, type_ref(r, t)),
                        None => c.name.clone(),
                    })
                    .collect()
            )
        ),
        TypeDefKind::Enum(e) => format!(
            "enum {{ {} }}",
            list(e.cases.iter().map(|c| c.name.clone()).collect())
        ),
        TypeDefKind::Option(t) => format!("option<{}>", type_ref(r, t)),
        TypeDefKind::Result(res) => {
            format!("result<{}, {}>", opt_ref(r, &res.ok), opt_ref(r, &res.err))
        }
        TypeDefKind::List(t) => format!("list<{}>", type_ref(r, t)),
        TypeDefKind::Map(k, v) => format!("map<{}, {}>", type_ref(r, k), type_ref(r, v)),
        TypeDefKind::FixedLengthList(t, n) => format!("list<{}, {n}>", type_ref(r, t)),
        TypeDefKind::Future(t) => format!("future<{}>", opt_ref(r, t)),
        TypeDefKind::Stream(t) => format!("stream<{}>", opt_ref(r, t)),
        TypeDefKind::Type(t) => format!("= {}", type_ref(r, t)),
        TypeDefKind::Unknown => "<unknown>".into(),
    }
}

fn func_shape(r: &Resolve, f: &Function) -> String {
    let res = |id: &TypeId| type_ref(r, &Type::Id(*id));
    let kind = match &f.kind {
        FunctionKind::Freestanding => "func".to_string(),
        FunctionKind::AsyncFreestanding => "async func".to_string(),
        FunctionKind::Getter => "getter".to_string(),
        FunctionKind::Setter => "setter".to_string(),
        FunctionKind::Method(t) => format!("method[{}]", res(t)),
        FunctionKind::AsyncMethod(t) => format!("async method[{}]", res(t)),
        FunctionKind::MethodGetter(t) => format!("method-getter[{}]", res(t)),
        FunctionKind::MethodSetter(t) => format!("method-setter[{}]", res(t)),
        FunctionKind::Static(t) => format!("static[{}]", res(t)),
        FunctionKind::AsyncStatic(t) => format!("async static[{}]", res(t)),
        FunctionKind::StaticGetter(t) => format!("static-getter[{}]", res(t)),
        FunctionKind::StaticSetter(t) => format!("static-setter[{}]", res(t)),
        FunctionKind::Constructor(t) => format!("constructor[{}]", res(t)),
    };
    let params = f
        .params
        .iter()
        .map(|p| format!("{}: {}", p.name, type_ref(r, &p.ty)))
        .collect::<Vec<_>>()
        .join(", ");
    let result = f
        .result
        .as_ref()
        .map_or_else(String::new, |t| format!(" -> {}", type_ref(r, t)));
    format!("{kind}({params}){result}")
}

/// The `ostro:runtime@0.1.*` package directories under `wit_root`, ordered by patch number.
pub fn runtime_0_1_packages(wit_root: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(wit_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(patch) = name
            .to_str()
            .and_then(|n| n.strip_prefix("ostro-runtime-0.1."))
            .and_then(|p| p.parse::<u64>().ok())
        else {
            continue;
        };
        if entry.file_type()?.is_dir() {
            out.push((patch, entry.path()));
        }
    }
    out.sort();
    Ok(out)
}

/// Check every consecutive pair of `ostro-runtime-0.1.*` packages under `wit_root`
/// (0.1.0 → 0.1.1 → …). Returns the pairs checked.
pub fn check_chain(wit_root: &Path) -> Result<Vec<(PathBuf, PathBuf)>, Vec<Incompat>> {
    let pkgs = runtime_0_1_packages(wit_root).map_err(|e| {
        vec![Incompat {
            checker: Checker::Load,
            message: format!("{}: {e}", wit_root.display()),
        }]
    })?;
    let mut checked = Vec::new();
    let mut errs = Vec::new();
    for pair in pkgs.windows(2) {
        let (prev, new) = (&pair[0].1, &pair[1].1);
        if let Err(e) = check_additive(prev, new) {
            errs.extend(e.into_iter().map(|i| Incompat {
                checker: i.checker,
                message: format!("{} -> {}: {}", prev.display(), new.display(), i.message),
            }));
        }
        checked.push((prev.clone(), new.clone()));
    }
    if errs.is_empty() {
        Ok(checked)
    } else {
        Err(errs)
    }
}
