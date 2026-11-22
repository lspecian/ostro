//! `ostro init` project templates (Rust / Python / TypeScript), embedded from
//! `crates/ostro-build/templates/{rust,python,ts}/` (D-P10-6).
//!
//! Owner: 10-08.
//!
//! Each template has one workflow `hello` (a step, a durable 3-day sleep, a wait on the
//! signal `approve`), a test that uses the language's test kit, a README and a
//! `.gitignore`. Placeholders: `{{name}}` (the project name) and `{{name_snake}}` (the
//! name with `-` replaced by `_`).

use std::fs;
use std::path::{Path, PathBuf};

use crate::project::{Lang, ProjectError, validate_name};

macro_rules! template {
    ($lang:literal, $($path:literal),+ $(,)?) => {
        &[$(($path, include_str!(concat!("../templates/", $lang, "/", $path)))),+]
    };
}

const RUST: &[(&str, &str)] = template!(
    "rust",
    "Cargo.toml",
    "README.md",
    "gitignore",
    "ostro.toml",
    "rust-toolchain.toml",
    "src/lib.rs",
    "tests/workflow.rs",
);

const PYTHON: &[(&str, &str)] = template!(
    "python",
    "README.md",
    "app.py",
    "gitignore",
    "ostro.toml",
    "pyproject.toml",
    "tests/conftest.py",
    "tests/test_workflow.py",
);

const TS: &[(&str, &str)] = template!(
    "ts",
    "README.md",
    "gitignore",
    "ostro.toml",
    "package.json",
    "src/index.ts",
    "test/workflow.test.js",
    "tsconfig.json",
);

/// The template files of `lang`: (relative path as stored, contents). The stored path
/// `gitignore` is written as `.gitignore` (see [`output_path`]).
pub fn files(lang: Lang) -> &'static [(&'static str, &'static str)] {
    match lang {
        Lang::Rust => RUST,
        Lang::Python => PYTHON,
        Lang::Ts => TS,
    }
}

/// The path a stored template file is written to (`gitignore` → `.gitignore`).
pub fn output_path(stored: &str) -> &str {
    if stored == "gitignore" {
        ".gitignore"
    } else {
        stored
    }
}

/// Substitute the placeholders of one template file.
pub fn substitute(text: &str, name: &str) -> String {
    text.replace("{{name_snake}}", &name.replace('-', "_"))
        .replace("{{name}}", name)
}

/// Write the `lang` template for project `name` into `dest` (created if missing).
///
/// Refuses a non-empty `dest` unless `force` (then template files are overwritten and
/// other files are left alone). Returns the written paths in template order.
pub fn render(
    lang: Lang,
    name: &str,
    dest: &Path,
    force: bool,
) -> Result<Vec<PathBuf>, ProjectError> {
    validate_name(name)?;
    if lang == Lang::Rust && name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(ProjectError::InvalidName {
            name: name.to_owned(),
            reason: "a Rust package name cannot start with a digit",
        });
    }
    if dest.exists() {
        if !dest.is_dir() {
            return Err(ProjectError::NotEmpty(dest.to_path_buf()));
        }
        let mut entries = fs::read_dir(dest).map_err(|e| ProjectError::io(dest, e))?;
        if entries.next().is_some() && !force {
            return Err(ProjectError::NotEmpty(dest.to_path_buf()));
        }
    }
    fs::create_dir_all(dest).map_err(|e| ProjectError::io(dest, e))?;
    let mut written = Vec::new();
    for (stored, text) in files(lang) {
        let path = dest.join(output_path(stored));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| ProjectError::io(parent, e))?;
        }
        fs::write(&path, substitute(text, name)).map_err(|e| ProjectError::io(&path, e))?;
        written.push(path);
    }
    Ok(written)
}
