//! Python build step (D-P10-2, D-P9-30): the released componentize-py 0.25.1 wheel in a
//! uv-managed CPython venv, run with exactly the flags of `sdks/scripts/build-guest.sh py`.
//!
//! 1. `ensure` uv 0.7.15 and the sha-verified componentize-py wheel (never the GitHub
//!    release binary: it needs GLIBC_2.39).
//! 2. Once per (wheel sha, cache root), under the cache lock: `uv venv --python 3.12` with a
//!    uv-managed CPython, then `uv pip install --no-index --no-deps <cached wheel>` (offline
//!    by construction), then `.ok`. All uv state lives in
//!    `state/cpy-venv-0.25.1-<sha8>/` (`UV_NO_CONFIG=1`, `UV_CACHE_DIR`,
//!    `UV_PYTHON_INSTALL_DIR`, `UV_TOOL_DIR`, `UV_PYTHON_PREFERENCE=only-managed`;
//!    `UV_PYTHON_INSTALL_MIRROR=<mirror>/python-build-standalone` and
//!    `UV_DEFAULT_INDEX=<mirror>/pypi/simple` with `OSTRO_TOOLCHAIN_MIRROR`; `UV_OFFLINE=1`
//!    when offline). The venv dir carries a hash of the cache root, because a venv is bound
//!    to its absolute path (the same cache is mounted at `/cache` by `--docker`).
//! 3. Dependencies: `uv.lock` → `uv export --frozen --no-dev --no-emit-project` (hashes are
//!    emitted by default) and `uv pip install --require-hashes -r`; else the pyproject
//!    `[project] dependencies`. Both install into `<stage>/.deps` with
//!    `--only-binary :all: --python-version 3.14` (the CPython inside componentize-py), and
//!    every installed wheel must be tagged `py3-none-any` / `py2.py3-none-any`.
//! 4. The SDK (`<sdk>/python/ostro`) is staged as `<stage>/ostro` (build-guest.sh's
//!    staging), then `componentize-py -d <sdk>/python/ostro/wit -w
//!    ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p <stage> [-p <stage>/.deps]
//!    <entry> -o <out>` runs in the stage with the scrubbed environment.
//!
//! `OSTRO_COMPONENTIZE_PY` (Phase 9's D-P9-3 fork, CI opt-in) replaces the wheel with a
//! warning. Musl hosts and glibc < 2.28 get an error pointing at `--docker`.
//!
//! Owner: 10-15.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use crate::pipeline::{BuildError, HostEnv, LangStep, StepCtx, block_on, run_streamed};
use crate::toolchain::{self, Pin, Tool};

/// The pinned uv.
pub const UV_VERSION: &str = "0.7.15";
/// The released componentize-py.
pub const COMPONENTIZE_PY_VERSION: &str = "0.25.1";
/// The CPython that runs componentize-py (uv-managed).
pub const HOST_PYTHON: &str = "3.12";
/// The CPython embedded in componentize-py 0.25.1 (dependency wheels are resolved for it).
pub const GUEST_PYTHON: &str = "3.14";
/// The world every workflow module targets.
pub const WORLD: &str = "ostro:runtime/workflow@0.1.0";

/// The componentize-py argument template, token for token as in build-guest.sh's
/// `build_py`. Placeholders: `<wit>`, `<stage>`, `<entry>`, `<out>`; `-p <stage>/.deps`
/// is inserted after `-p <stage>` when the project has dependencies.
pub const COMMAND_LINE: &[&str] = &[
    "-d",
    "<wit>",
    "-w",
    WORLD,
    "componentize",
    "--stub-wasi",
    "-p",
    "<stage>",
    "<entry>",
    "-o",
    "<out>",
];

/// Host variables passed through to uv (network configuration only).
pub(crate) const PASS_THROUGH: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "all_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "UV_NATIVE_TLS",
];

/// The Python step.
#[derive(Debug, Clone, Copy, Default)]
pub struct PythonStep;

impl PythonStep {
    /// The componentize-py argument template ([`COMMAND_LINE`]); the parity test compares
    /// it with build-guest.sh.
    pub fn command_line() -> Vec<String> {
        COMMAND_LINE.iter().map(|s| (*s).to_owned()).collect()
    }
}

/// [`COMMAND_LINE`] with the placeholders substituted.
pub fn componentize_args(
    wit: &Path,
    stage: &Path,
    deps: Option<&Path>,
    entry: &str,
    out: &Path,
) -> Vec<OsString> {
    let mut v: Vec<OsString> = Vec::new();
    for t in COMMAND_LINE {
        match *t {
            "<wit>" => v.push(wit.into()),
            "<stage>" => {
                v.push(stage.into());
                if let Some(d) = deps {
                    v.push("-p".into());
                    v.push(d.into());
                }
            }
            "<entry>" => v.push(entry.into()),
            "<out>" => v.push(out.into()),
            other => v.push(other.into()),
        }
    }
    v
}

pub(crate) fn step_err(step: &str, message: impl Into<String>) -> BuildError {
    BuildError::Step {
        step: step.to_owned(),
        message: message.into(),
        stderr_tail: String::new(),
    }
}

/// The `OSTRO_COMPONENTIZE_PY` override, if set.
fn override_bin(host: HostEnv) -> Option<PathBuf> {
    host("OSTRO_COMPONENTIZE_PY")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// The Python entry module (`app`, also accepted as `app.py`).
pub fn entry_module(entry: &str) -> &str {
    entry.strip_suffix(".py").unwrap_or(entry)
}

/// Linux hosts where the manylinux wheel and the official builds cannot run: musl, or
/// glibc older than 2.28. Returns the error message.
pub(crate) fn unsupported_libc(what: &str) -> Option<String> {
    if std::env::consts::OS != "linux" {
        return None;
    }
    if toolchain::musl_host() {
        return Some(format!("{what} builds need glibc >= 2.28; use --docker"));
    }
    match toolchain::glibc_version() {
        Some(v) if !toolchain::glibc_at_least_2_28(&v) => Some(format!(
            "{what} builds need glibc >= 2.28 (found {v}); use --docker"
        )),
        _ => None,
    }
}

fn pin(ctx: &StepCtx, tool: Tool) -> Result<Pin, BuildError> {
    ctx.set
        .pin(tool)
        .cloned()
        .ok_or_else(|| step_err(tool.name(), "no pin for this platform"))
}

/// First 8 hex of a sha.
fn sha8(s: &str) -> &str {
    &s[..8.min(s.len())]
}

/// First 8 hex of sha256(the canonical cache root path).
pub(crate) fn root_hash8(root: &Path) -> String {
    let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    crate::pipeline::sha256_hex(root.as_os_str().as_encoded_bytes())[..8].to_owned()
}

/// The uv environment of one componentize-py state dir.
struct Uv {
    bin: PathBuf,
    env: Vec<(String, String)>,
}

impl Uv {
    fn command(&self, ctx: &StepCtx) -> Command {
        let mut cmd = Command::new(&self.bin);
        cmd.env_clear().envs(ctx.env).envs(self.env.iter().cloned());
        for k in PASS_THROUGH {
            if let Some(v) = (ctx.host)(k) {
                cmd.env(k, v);
            }
        }
        cmd
    }
}

fn uv_env(ctx: &StepCtx, state: &Path) -> Vec<(String, String)> {
    let p = |d: &str| state.join(d).display().to_string();
    let mut env = vec![
        ("UV_NO_CONFIG".to_owned(), "1".to_owned()),
        ("UV_CACHE_DIR".to_owned(), p("uv-cache")),
        ("UV_PYTHON_INSTALL_DIR".to_owned(), p("python")),
        ("UV_TOOL_DIR".to_owned(), p("uv-tools")),
        ("UV_PYTHON_PREFERENCE".to_owned(), "only-managed".to_owned()),
        ("UV_NO_PROGRESS".to_owned(), "1".to_owned()),
    ];
    if let Some(m) = &ctx.fetch.mirror {
        let m = m.trim_end_matches('/');
        env.push((
            "UV_PYTHON_INSTALL_MIRROR".to_owned(),
            format!("{m}/python-build-standalone"),
        ));
        env.push(("UV_DEFAULT_INDEX".to_owned(), format!("{m}/pypi/simple")));
    }
    if ctx.fetch.offline {
        env.push(("UV_OFFLINE".to_owned(), "1".to_owned()));
    }
    env
}

impl LangStep for PythonStep {
    fn tool_ids(&self, ctx: &StepCtx) -> Vec<String> {
        let mut v = vec![
            format!("uv@{UV_VERSION}"),
            format!("cpython@{HOST_PYTHON} (uv-managed)"),
        ];
        if let Some(bin) = override_bin(ctx.host) {
            v.push(format!(
                "componentize-py (OSTRO_COMPONENTIZE_PY={})",
                bin.display()
            ));
        } else {
            let sha = ctx
                .set
                .pin(Tool::ComponentizePy)
                .map_or("unpinned", |p| sha8(&p.sha256));
            v.push(format!(
                "componentize-py@{COMPONENTIZE_PY_VERSION} (wheel {sha})"
            ));
        }
        v
    }

    fn run(&self, ctx: &StepCtx) -> Result<(), BuildError> {
        if let Some(msg) = unsupported_libc("Python") {
            return Err(step_err("python toolchain", msg));
        }
        let uv_pin = pin(ctx, Tool::Uv)?;
        let uv_entry = block_on(|| toolchain::ensure(ctx.cache, &uv_pin, ctx.fetch))??;
        let override_cpy = override_bin(ctx.host);
        let (state, cpy_bin) = match &override_cpy {
            Some(bin) => {
                eprintln!(
                    "warning: using OSTRO_COMPONENTIZE_PY={} (Phase 9 fork, CI opt-in)",
                    bin.display()
                );
                let state = ctx
                    .cache
                    .state_dir(&format!("cpy-venv-{COMPONENTIZE_PY_VERSION}-override"));
                (state, bin.clone())
            }
            None => {
                let wheel_pin = pin(ctx, Tool::ComponentizePy)?;
                let entry = block_on(|| toolchain::ensure(ctx.cache, &wheel_pin, ctx.fetch))??;
                let wheel = entry.join(wheel_pin.file_name());
                let state = ctx.cache.state_dir(&format!(
                    "cpy-venv-{COMPONENTIZE_PY_VERSION}-{}",
                    sha8(&wheel_pin.sha256)
                ));
                let uv = Uv {
                    bin: uv_entry.join("uv"),
                    env: uv_env(ctx, &state),
                };
                let venv = ensure_venv(ctx, &uv, &state, &wheel)?;
                (state, venv.join("bin").join("componentize-py"))
            }
        };
        let uv = Uv {
            bin: uv_entry.join("uv"),
            env: uv_env(ctx, &state),
        };

        // The SDK next to the entry module (build-guest.sh stages sdks/python/ostro).
        let sdk_pkg = ctx.sdk.join("python").join("ostro");
        let sde = ctx
            .env
            .get("SOURCE_DATE_EPOCH")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        copy_tree(&sdk_pkg, &ctx.stage.join("ostro"), sde)?;

        let deps = install_deps(ctx, &uv)?;

        let wit = sdk_pkg.join("wit");
        let entry = entry_module(ctx.project.manifest.project.entry()).to_owned();
        let mut cmd = Command::new(&cpy_bin);
        let mut path = String::new();
        if override_cpy.is_none()
            && let Some(d) = cpy_bin.parent()
        {
            path = format!("{}:", d.display());
        }
        path.push_str(&ctx.env["PATH"]);
        cmd.current_dir(ctx.stage)
            .env_clear()
            .envs(ctx.env)
            .env("PATH", path)
            .args(componentize_args(
                &wit,
                ctx.stage,
                deps.as_deref(),
                &entry,
                ctx.out_wasm,
            ));
        run_streamed(&mut cmd, "componentize-py")
    }
}

/// Create (once) the venv holding the componentize-py wheel; returns the venv dir.
fn ensure_venv(ctx: &StepCtx, uv: &Uv, state: &Path, wheel: &Path) -> Result<PathBuf, BuildError> {
    let venv = state.join(format!("venv-{}", root_hash8(&ctx.cache.root)));
    let ok = venv.join(".ok");
    let ready = || ok.is_file() && venv.join("bin").join("componentize-py").is_file();
    if ready() {
        return Ok(venv);
    }
    let lock = block_on(|| ctx.cache.lock())??;
    if ready() {
        drop(lock);
        return Ok(venv);
    }
    std::fs::create_dir_all(state)
        .map_err(|e| BuildError::io(format!("creating {}", state.display()), e))?;
    if venv.exists() {
        std::fs::remove_dir_all(&venv)
            .map_err(|e| BuildError::io(format!("removing {}", venv.display()), e))?;
    }
    eprintln!(
        "ostro: creating the componentize-py {COMPONENTIZE_PY_VERSION} venv (uv-managed CPython {HOST_PYTHON}) in {}",
        venv.display()
    );
    let mut cmd = uv.command(ctx);
    cmd.arg("venv").arg("--python").arg(HOST_PYTHON).arg(&venv);
    run_streamed(&mut cmd, "uv venv")?;
    let mut cmd = uv.command(ctx);
    cmd.args(["pip", "install", "--python"])
        .arg(&venv)
        .args(["--no-index", "--no-deps"])
        .arg(wheel);
    run_streamed(&mut cmd, "uv pip install componentize-py")?;
    std::fs::write(&ok, format!("{}\n", wheel.display()))
        .map_err(|e| BuildError::io(format!("writing {}", ok.display()), e))?;
    drop(lock);
    Ok(venv)
}

/// `[project] dependencies` of the staged pyproject.toml.
pub fn pyproject_dependencies(stage: &Path) -> Result<Vec<String>, BuildError> {
    let path = stage.join("pyproject.toml");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(Vec::new());
    };
    let v: toml::Table = toml::from_str(&text).map_err(|e| {
        BuildError::io(
            format!("parsing {}", path.display()),
            std::io::Error::new(std::io::ErrorKind::InvalidData, e.message().to_owned()),
        )
    })?;
    Ok(v.get("project")
        .and_then(|p| p.get("dependencies"))
        .and_then(toml::Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(toml::Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default())
}

/// Install the project's dependencies into `<stage>/.deps`; `None` when it has none.
fn install_deps(ctx: &StepCtx, uv: &Uv) -> Result<Option<PathBuf>, BuildError> {
    let stage = ctx.stage;
    let target = stage.join(".deps");
    let common = |cmd: &mut Command| {
        cmd.current_dir(stage)
            .args(["pip", "install", "--python", HOST_PYTHON, "--target"])
            .arg(&target)
            .args(["--only-binary", ":all:", "--python-version", GUEST_PYTHON]);
    };
    if stage.join("uv.lock").is_file() {
        let req = stage.join("requirements.txt");
        let mut cmd = uv.command(ctx);
        cmd.current_dir(stage)
            .args([
                "export",
                "--frozen",
                "--no-dev",
                "--no-emit-project",
                "--format",
                "requirements.txt",
                "-o",
            ])
            .arg(&req);
        run_streamed(&mut cmd, "uv export")?;
        let text = std::fs::read_to_string(&req).unwrap_or_default();
        if !text
            .lines()
            .any(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
        {
            return Ok(None);
        }
        let mut cmd = uv.command(ctx);
        common(&mut cmd);
        cmd.arg("--require-hashes").arg("-r").arg(&req);
        run_deps(&mut cmd)?;
    } else {
        let deps = pyproject_dependencies(stage)?;
        if deps.is_empty() {
            return Ok(None);
        }
        let mut cmd = uv.command(ctx);
        common(&mut cmd);
        cmd.args(&deps);
        run_deps(&mut cmd)?;
    }
    check_pure(&target)?;
    Ok(Some(target))
}

fn run_deps(cmd: &mut Command) -> Result<(), BuildError> {
    run_streamed(cmd, "uv pip install (dependencies)").map_err(|e| match e {
        BuildError::Step {
            step,
            message,
            stderr_tail,
        } => BuildError::Step {
            step,
            message: format!(
                "{message}; only pure-Python wheels (py3-none-any) for CPython {GUEST_PYTHON} \
                 can be used (native extensions are not supported, Phase 10)"
            ),
            stderr_tail,
        },
        other => other,
    })
}

/// Every `*.dist-info/WHEEL` under `deps` has only `py3-none-any` / `py2.py3-none-any`
/// tags.
pub fn check_pure(deps: &Path) -> Result<(), BuildError> {
    let rd = match std::fs::read_dir(deps) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(BuildError::io(format!("reading {}", deps.display()), e)),
    };
    let mut names: Vec<PathBuf> = rd
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "dist-info"))
        .collect();
    names.sort();
    for d in names {
        let wheel = d.join("WHEEL");
        let text = std::fs::read_to_string(&wheel)
            .map_err(|e| BuildError::io(format!("reading {}", wheel.display()), e))?;
        let tags: BTreeSet<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("Tag:"))
            .map(str::trim)
            .collect();
        let pure = !tags.is_empty()
            && tags
                .iter()
                .all(|t| matches!(*t, "py3-none-any" | "py2.py3-none-any"));
        if !pure {
            let stem = d
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            let pkg = stem.rsplit_once('-').map_or(stem.as_str(), |(n, _)| n);
            return Err(step_err(
                "python dependencies",
                format!(
                    "{pkg} is not a pure-Python wheel (tag {}); native extensions are not \
                     supported (Phase 10)",
                    tags.into_iter().collect::<Vec<_>>().join(", ")
                ),
            ));
        }
    }
    Ok(())
}

/// Copy the tree `from` into `to` (merging), skipping caches; every copied file and
/// directory gets mtime = `sde`.
pub(crate) fn copy_tree(from: &Path, to: &Path, sde: u64) -> Result<(), BuildError> {
    let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(sde);
    copy_rec(from, to, mtime)
        .map_err(|e| BuildError::io(format!("copying {} to {}", from.display(), to.display()), e))
}

fn copy_rec(from: &Path, to: &Path, mtime: SystemTime) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    let mut entries: Vec<_> = std::fs::read_dir(from)?.flatten().collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let name = e.file_name();
        let n = name.to_string_lossy();
        let ft = e.file_type()?;
        let dst = to.join(&name);
        if ft.is_dir() {
            if matches!(
                n.as_ref(),
                "node_modules" | "__pycache__" | ".venv" | "target"
            ) {
                continue;
            }
            copy_rec(&e.path(), &dst, mtime)?;
        } else if ft.is_file() {
            if n.ends_with(".pyc") {
                continue;
            }
            std::fs::copy(e.path(), &dst)?;
            std::fs::OpenOptions::new()
                .write(true)
                .open(&dst)?
                .set_modified(mtime)?;
        }
    }
    std::fs::File::open(to)?.set_modified(mtime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_follow_the_template() {
        let a = componentize_args(
            Path::new("/w"),
            Path::new("/s"),
            Some(Path::new("/s/.deps")),
            "app",
            Path::new("/o.wasm"),
        );
        let a: Vec<String> = a.iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(
            a,
            [
                "-d",
                "/w",
                "-w",
                WORLD,
                "componentize",
                "--stub-wasi",
                "-p",
                "/s",
                "-p",
                "/s/.deps",
                "app",
                "-o",
                "/o.wasm"
            ]
        );
        assert_eq!(entry_module("app.py"), "app");
    }

    #[test]
    fn pure_wheel_scan() {
        let d = tempfile::tempdir().unwrap();
        let w = |name: &str, tag: &str| {
            let p = d.path().join(format!("{name}.dist-info"));
            std::fs::create_dir_all(&p).unwrap();
            std::fs::write(p.join("WHEEL"), format!("Wheel-Version: 1.0\nTag: {tag}\n")).unwrap();
        };
        w("attrs-25.3.0", "py3-none-any");
        assert!(check_pure(d.path()).is_ok());
        w("pydantic_core-2.41.5", "cp314-cp314-manylinux_2_17_x86_64");
        let e = check_pure(d.path()).unwrap_err().to_string();
        assert!(
            e.contains("pydantic_core is not a pure-Python wheel"),
            "{e}"
        );
    }
}
