//! `ostro.toml` (D-P10-5: `[project] name/lang/entry`, `[build] toolchain`,
//! `[deploy] activate/tenant`; unknown keys rejected), project detection and the output
//! directory (`target/ostro/`).
//!
//! Owner: 10-08.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The project manifest file name.
pub const MANIFEST_FILE: &str = "ostro.toml";

/// A project language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lang {
    /// Rust (`cargo`, wasm32-wasip2).
    Rust,
    /// Python (componentize-py).
    Python,
    /// TypeScript (jco).
    Ts,
}

impl Lang {
    /// Every language, in display order.
    pub const ALL: [Lang; 3] = [Lang::Rust, Lang::Python, Lang::Ts];

    /// The `ostro.toml` / `--lang` spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Lang::Rust => "rust",
            Lang::Python => "python",
            Lang::Ts => "ts",
        }
    }

    /// The default `[project] entry`: the crate manifest (Rust), the entry module (Python),
    /// the entry file (TypeScript).
    pub fn default_entry(self) -> &'static str {
        match self {
            Lang::Rust => "Cargo.toml",
            Lang::Python => "app",
            Lang::Ts => "src/index.ts",
        }
    }
}

impl fmt::Display for Lang {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Lang {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "rust" => Ok(Lang::Rust),
            "python" => Ok(Lang::Python),
            "ts" => Ok(Lang::Ts),
            other => Err(format!(
                "unknown language `{other}` (expected rust, python or ts)"
            )),
        }
    }
}

/// How toolchains are found (`--toolchain`, `[build] toolchain`; D-P10-4 / D-P10-15).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

/// `ostro.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// `[project]`.
    pub project: ProjectSection,
    /// `[build]`.
    #[serde(default)]
    pub build: BuildSection,
    /// `[deploy]`.
    #[serde(default)]
    pub deploy: DeploySection,
}

/// `[project]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSection {
    /// The module name (validated with [`validate_name`]).
    pub name: String,
    /// The language.
    pub lang: Lang,
    /// The entry point; [`load`] fills in [`Lang::default_entry`] when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<String>,
}

impl ProjectSection {
    /// The entry point, defaulted by language.
    pub fn entry(&self) -> &str {
        self.entry
            .as_deref()
            .unwrap_or_else(|| self.lang.default_entry())
    }
}

/// `[build]`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildSection {
    /// `toolchain = "auto" | "hermetic" | "system" | "docker"` (default `auto`).
    #[serde(default)]
    pub toolchain: ToolchainMode,
}

/// `[deploy]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploySection {
    /// Activate the deployed module (default `true`).
    #[serde(default = "default_true")]
    pub activate: bool,
    /// The tenant to deploy to (default: the server's default tenant).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

impl Default for DeploySection {
    fn default() -> Self {
        DeploySection {
            activate: true,
            tenant: None,
        }
    }
}

fn default_true() -> bool {
    true
}

impl Manifest {
    /// Parse `ostro.toml` text (strict: unknown keys are errors). The entry is defaulted
    /// and the name validated.
    pub fn parse(text: &str) -> Result<Manifest, ProjectError> {
        let mut m: Manifest = toml::from_str(text).map_err(|e| ProjectError::Parse {
            path: PathBuf::from(MANIFEST_FILE),
            message: e.message().to_owned(),
        })?;
        validate_name(&m.project.name)?;
        if m.project.entry.is_none() {
            m.project.entry = Some(m.project.lang.default_entry().to_owned());
        }
        Ok(m)
    }
}

/// A loaded (or detected) project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    /// The project directory.
    pub root: PathBuf,
    /// The manifest (synthesised with defaults when detected).
    pub manifest: Manifest,
    /// `true` when there was no `ostro.toml` and the language was detected.
    pub detected: bool,
}

/// Project errors. `NoProject`, `InvalidName` and `NotEmpty` are usage errors (exit 2).
#[derive(Debug, thiserror::Error)]
pub enum ProjectError {
    /// No `ostro.toml`, `Cargo.toml`, `pyproject.toml` or `package.json` in the directory.
    #[error(
        "no Ostro project in {0}: no ostro.toml, Cargo.toml, pyproject.toml or package.json (run `ostro init`)"
    )]
    NoProject(PathBuf),
    /// A manifest could not be parsed (or violates the schema).
    #[error("{}: {message}", path.display())]
    Parse {
        /// The file.
        path: PathBuf,
        /// The parser's message (names the offending key).
        message: String,
    },
    /// A language manifest lacks the project name.
    #[error("{}: {message}", path.display())]
    Detect {
        /// The file.
        path: PathBuf,
        /// What is missing.
        message: String,
    },
    /// The project name violates the module-name grammar.
    #[error("invalid project name `{name}`: {reason}")]
    InvalidName {
        /// The rejected name.
        name: String,
        /// Why.
        reason: &'static str,
    },
    /// `ostro init` into a non-empty directory without `--force`.
    #[error("{} is not empty (use --force to write into it)", .0.display())]
    NotEmpty(PathBuf),
    /// I/O.
    #[error("{}: {source}", path.display())]
    Io {
        /// The file or directory.
        path: PathBuf,
        /// The error.
        source: std::io::Error,
    },
}

impl ProjectError {
    /// `true` for errors caused by the invocation (exit code 2).
    pub fn is_usage(&self) -> bool {
        matches!(
            self,
            ProjectError::NoProject(_)
                | ProjectError::InvalidName { .. }
                | ProjectError::NotEmpty(_)
                | ProjectError::Parse { .. }
                | ProjectError::Detect { .. }
        )
    }

    pub(crate) fn io(path: &Path, source: std::io::Error) -> ProjectError {
        ProjectError::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

/// Validate a project (module) name: the D23 grammar (no `/`, no `:`, no leading `__`)
/// plus `^[a-z0-9][a-z0-9_-]{0,63}$`.
pub fn validate_name(n: &str) -> Result<(), ProjectError> {
    let bad = |reason| {
        Err(ProjectError::InvalidName {
            name: n.to_owned(),
            reason,
        })
    };
    if n.is_empty() {
        return bad("the name is empty");
    }
    if n.len() > 64 {
        return bad("the name is longer than 64 characters");
    }
    if n.contains('/') {
        return bad("the name contains `/`");
    }
    if n.contains(':') {
        return bad("the name contains `:`");
    }
    if n.starts_with("__") {
        return bad("names starting with `__` are reserved");
    }
    if !n
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    {
        return bad("only lowercase letters, digits, `_` and `-` are allowed");
    }
    if !n
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return bad("the name must start with a lowercase letter or a digit");
    }
    Ok(())
}

/// Load the project in `dir`: `dir/ostro.toml` if present, else detect the language from
/// `Cargo.toml` (`[package] name`), `pyproject.toml` (`[project] name`) or
/// `package.json` (`name`), in that order. Only `dir` itself is examined.
pub fn load(dir: &Path) -> Result<Project, ProjectError> {
    let manifest_path = dir.join(MANIFEST_FILE);
    if manifest_path.is_file() {
        let text = read(&manifest_path)?;
        let manifest = Manifest::parse(&text).map_err(|e| match e {
            ProjectError::Parse { message, .. } => ProjectError::Parse {
                path: manifest_path.clone(),
                message,
            },
            other => other,
        })?;
        return Ok(Project {
            root: dir.to_path_buf(),
            manifest,
            detected: false,
        });
    }
    let (lang, name) = detect(dir)?;
    validate_name(&name)?;
    Ok(Project {
        root: dir.to_path_buf(),
        manifest: Manifest {
            project: ProjectSection {
                name,
                lang,
                entry: Some(lang.default_entry().to_owned()),
            },
            build: BuildSection::default(),
            deploy: DeploySection::default(),
        },
        detected: true,
    })
}

/// `<root>/target/ostro`: every build output (`sdk/`, `stage/`, `<name>.wasm`,
/// `manifest.json`, `inputs.sha256`; D-P10-5).
pub fn output_dir(p: &Project) -> PathBuf {
    p.root.join("target").join("ostro")
}

fn read(path: &Path) -> Result<String, ProjectError> {
    std::fs::read_to_string(path).map_err(|e| ProjectError::io(path, e))
}

fn detect(dir: &Path) -> Result<(Lang, String), ProjectError> {
    let cargo = dir.join("Cargo.toml");
    if cargo.is_file() {
        let name = toml_name(&cargo, "package")?;
        return Ok((Lang::Rust, name));
    }
    let py = dir.join("pyproject.toml");
    if py.is_file() {
        let name = toml_name(&py, "project")?;
        return Ok((Lang::Python, name));
    }
    let pkg = dir.join("package.json");
    if pkg.is_file() {
        let text = read(&pkg)?;
        let v: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| ProjectError::Parse {
                path: pkg.clone(),
                message: e.to_string(),
            })?;
        let name = v
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| ProjectError::Detect {
                path: pkg.clone(),
                message: "no \"name\" (add one, or an ostro.toml)".to_owned(),
            })?;
        return Ok((Lang::Ts, name.to_owned()));
    }
    Err(ProjectError::NoProject(dir.to_path_buf()))
}

fn toml_name(path: &Path, table: &str) -> Result<String, ProjectError> {
    let text = read(path)?;
    let v: toml::Table = toml::from_str(&text).map_err(|e| ProjectError::Parse {
        path: path.to_path_buf(),
        message: e.message().to_owned(),
    })?;
    v.get(table)
        .and_then(|t| t.get("name"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| ProjectError::Detect {
            path: path.to_path_buf(),
            message: format!("no [{table}] name (add one, or an ostro.toml)"),
        })
}
