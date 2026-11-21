//! Conformance languages: selection (`OSTRO_CONF_LANGS`), workflow naming (D-P9-23) and
//! module builds.
//!
//! [`Lang`] is the one enum defined in [`crate::spec`] (re-exported here).

use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

use ostro_engine::config::LanguageClass;

pub use crate::spec::Lang;
use crate::{polyglot, repo_root};

/// Env var with the comma list of languages to run (`rust,py,ts,ts-sm`; default `rust`).
pub const ENV: &str = "OSTRO_CONF_LANGS";

/// Spec name of a language (`rust`, `py`, `ts`, `ts-sm`).
pub fn name(lang: Lang) -> &'static str {
    match lang {
        Lang::Rust => "rust",
        Lang::Py => "py",
        Lang::Ts => "ts",
        Lang::TsSm => "ts-sm",
    }
}

/// Parses one language name.
pub fn parse(s: &str) -> Option<Lang> {
    match s.trim() {
        "rust" => Some(Lang::Rust),
        "py" => Some(Lang::Py),
        "ts" => Some(Lang::Ts),
        "ts-sm" => Some(Lang::TsSm),
        _ => None,
    }
}

/// Parses an `OSTRO_CONF_LANGS` value (`None`/empty = `rust`). Python and TS require the
/// `polyglot` feature (D-P9-12): without it the error says how to enable them.
pub fn parse_list(value: Option<&str>, polyglot: bool) -> Result<Vec<Lang>, String> {
    let raw = value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or("rust");
    let mut out = Vec::new();
    for part in raw.split(',').filter(|p| !p.trim().is_empty()) {
        let lang = parse(part).ok_or_else(|| {
            format!("{ENV}: unknown language {part:?} (expected rust, py, ts or ts-sm)")
        })?;
        if lang != Lang::Rust && !polyglot {
            return Err(format!(
                "{ENV} selects {:?}, which needs the Python/TS toolchains: run with \
                 `cargo test -p ostro-conformance --features polyglot` (D-P9-12)",
                name(lang)
            ));
        }
        if !out.contains(&lang) {
            out.push(lang);
        }
    }
    if out.is_empty() {
        out.push(Lang::Rust);
    }
    Ok(out)
}

/// The languages selected by `OSTRO_CONF_LANGS` (default `rust`).
///
/// # Panics
///
/// On an unknown name, or on `py`/`ts`/`ts-sm` without `--features polyglot`.
pub fn selected() -> Vec<Lang> {
    let v = std::env::var(ENV).ok();
    parse_list(v.as_deref(), cfg!(feature = "polyglot")).unwrap_or_else(|e| panic!("{e}"))
}

/// Workflow-name prefix (D-P9-23); StarlingMonkey builds reuse `ts_`.
pub fn prefix(lang: Lang) -> &'static str {
    match lang {
        Lang::Rust => "rs_",
        Lang::Py => "py_",
        Lang::Ts | Lang::TsSm => "ts_",
    }
}

/// `rs_<base>`, `py_<base>`, `ts_<base>`.
pub fn workflow_name(lang: Lang, base: &str) -> String {
    format!("{}{base}", prefix(lang))
}

/// Engine language class used to size the pool for a language's modules.
pub fn class(lang: Lang) -> LanguageClass {
    match lang {
        Lang::Rust => LanguageClass::Rust,
        Lang::Py => LanguageClass::Python,
        Lang::Ts | Lang::TsSm => LanguageClass::TypeScript,
    }
}

/// Plan that owns a language's conformance guests.
fn owner(lang: Lang) -> &'static str {
    match lang {
        Lang::Rust => "09-11",
        Lang::Py => "09-14",
        Lang::Ts | Lang::TsSm => "09-15",
    }
}

/// Source directory of a language's conformance guest (`variant` `None` = base, `v2`).
pub fn guest_dir(lang: Lang, variant: Option<&str>) -> PathBuf {
    let root = repo_root();
    let v = variant.map(|v| format!("-{v}")).unwrap_or_default();
    match lang {
        Lang::Rust => root.join(format!("guests/conf-rust{v}")),
        Lang::Py => root.join(format!("conformance/guests/python{v}")),
        Lang::Ts | Lang::TsSm => root.join(format!("conformance/guests/ts{v}")),
    }
}

/// Component bytes of a language's conformance module (`variant` `None` = base, `v2`).
///
/// Rust builds `guests/conf-rust[-v2]` for wasm32-wasip2 into `target/guests` (as
/// `ostro-engine`'s `guest_wasm` does). Python/TS use [`polyglot::build`] on
/// `conformance/guests/{python,python-v2,ts,ts-v2}`; a missing directory is a clear
/// error naming the plan that adds it (09-14 Python, 09-15 TypeScript).
pub fn module_for(lang: Lang, variant: Option<&str>) -> Result<Vec<u8>, String> {
    let dir = guest_dir(lang, variant);
    if !dir.is_dir() {
        return Err(format!(
            "{} conformance guest {} does not exist yet (added by plan {})",
            name(lang),
            dir.display(),
            owner(lang)
        ));
    }
    match lang {
        Lang::Rust => rust_module(variant),
        Lang::Py => read(&polyglot::build(polyglot::Mode::Py, &dir)?),
        Lang::Ts => read(&polyglot::build(polyglot::Mode::Ts, &dir)?),
        Lang::TsSm => read(&polyglot::build(polyglot::Mode::TsSm, &dir)?),
    }
}

fn read(path: &std::path::Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))
}

/// Builds both Rust conformance guests once per process and reads the requested one.
fn rust_module(variant: Option<&str>) -> Result<Vec<u8>, String> {
    static BUILT: Mutex<bool> = Mutex::new(false);
    let root = repo_root();
    let target_dir = root.join("target/guests");
    {
        let mut built = BUILT.lock().unwrap_or_else(|p| p.into_inner());
        if !*built {
            let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
            let out = Command::new(cargo)
                .args(["build", "--release", "--target", "wasm32-wasip2"])
                .arg("--manifest-path")
                .arg(root.join("guests/Cargo.toml"))
                .arg("--target-dir")
                .arg(&target_dir)
                .args(["-p", "conf-rust", "-p", "conf-rust-v2"])
                .env_remove("RUSTFLAGS")
                .env_remove("CARGO_ENCODED_RUSTFLAGS")
                .env_remove("CARGO_BUILD_TARGET")
                .env_remove("CARGO_TARGET_DIR")
                .env_remove("RUSTC_WRAPPER")
                .output()
                .map_err(|e| format!("spawn cargo for the conf-rust build: {e}"))?;
            if !out.status.success() {
                return Err(format!(
                    "conf-rust guests build failed:\n{}",
                    String::from_utf8_lossy(&out.stderr)
                ));
            }
            *built = true;
        }
    }
    let file = match variant {
        None => "conf_rust.wasm".to_string(),
        Some(v) => format!("conf_rust_{v}.wasm"),
    };
    read(&target_dir.join("wasm32-wasip2/release").join(file))
}
