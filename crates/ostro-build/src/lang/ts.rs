//! TypeScript build step (D-P10-3, D-P9-1): Node 24 from the cache, the SDK bundle's pinned
//! npm toolchain (jco 1.42.0 + componentize-js 0.23.0, `toolchain/package-lock.json`) and
//! `jco componentize` with exactly the flags of `sdks/scripts/build-guest.sh ts` (QuickJS).
//!
//! 1. `ensure` Node (official tarball, top dir stripped).
//! 2. Once per lock hash, under the cache lock: copy `package.json`, `package-lock.json` and
//!    `rolldown.ostro.mjs` into `state/jco-<lockhash16>/` and run
//!    `npm ci --ignore-scripts --no-audit --no-fund` there with `PATH=<node>/bin:/usr/bin:/bin`,
//!    a private npmrc (`state/npm/npmrc`: `registry=https://registry.npmjs.org/`, or
//!    `<OSTRO_TOOLCHAIN_MIRROR>/npm/`, an npm-registry-compatible mirror), the npm cache in
//!    `state/npm/npm-cache` and `npm_config_offline=true` when offline; then `.ok`. No
//!    package of the pinned lock has an install script, so `--ignore-scripts` loses nothing
//!    (10-15 finding).
//! 3. Stage: the project sources (pipeline), `npm ci --ignore-scripts` when the project's
//!    `package-lock.json` has dependencies, then `node_modules/ostro` = a copy of `<sdk>/ts`.
//! 4. `<toolchain>/node_modules/.bin/jco componentize <stage>/<entry> --wit <sdk>/ts/wit
//!    --world-name workflow --backend quickjs --backend-qjs-disable-async
//!    --backend-qjs-stub-wasi --bundle-config <toolchain>/rolldown.ostro.mjs -o <out>` in
//!    the stage, with the scrubbed environment plus the Node `PATH`.
//!
//! Musl hosts and glibc < 2.28 (no official Node build) get an error pointing at `--docker`.
//!
//! Owner: 10-15.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::lang::python::{copy_tree, step_err, unsupported_libc};
use crate::pipeline::{BuildError, LangStep, StepCtx, block_on, run_streamed, sha256_hex};
use crate::toolchain::{self, Tool};

/// The jco release of the pinned toolchain lock.
pub const JCO_VERSION: &str = "1.42.0";

/// The jco argument template, token for token as in build-guest.sh's `build_ts`.
/// Placeholders: `<entry>`, `<wit>`, `<bundle-config>`, `<out>`.
pub const COMMAND_LINE: &[&str] = &[
    "componentize",
    "<entry>",
    "--wit",
    "<wit>",
    "--world-name",
    "workflow",
    "--backend",
    "quickjs",
    "--backend-qjs-disable-async",
    "--backend-qjs-stub-wasi",
    "--bundle-config",
    "<bundle-config>",
    "-o",
    "<out>",
];

/// The npm registry used without a mirror.
pub const NPM_REGISTRY: &str = "https://registry.npmjs.org/";

/// Host variables passed through to npm (network configuration only).
const PASS_THROUGH: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "NODE_EXTRA_CA_CERTS",
];

/// The TypeScript step.
#[derive(Debug, Clone, Copy, Default)]
pub struct TsStep;

impl TsStep {
    /// The jco argument template ([`COMMAND_LINE`]); the parity test compares it with
    /// build-guest.sh.
    pub fn command_line() -> Vec<String> {
        COMMAND_LINE.iter().map(|s| (*s).to_owned()).collect()
    }
}

/// [`COMMAND_LINE`] with the placeholders substituted.
pub fn jco_args(entry: &Path, wit: &Path, bundle_config: &Path, out: &Path) -> Vec<OsString> {
    COMMAND_LINE
        .iter()
        .map(|t| match *t {
            "<entry>" => entry.into(),
            "<wit>" => wit.into(),
            "<bundle-config>" => bundle_config.into(),
            "<out>" => out.into(),
            other => other.into(),
        })
        .collect()
}

/// The first 16 hex of sha256(`<sdk>/toolchain/package-lock.json`).
fn lock_hash(sdk: &Path) -> Result<String, BuildError> {
    let lock = sdk.join("toolchain").join("package-lock.json");
    let bytes = std::fs::read(&lock)
        .map_err(|e| BuildError::io(format!("reading {}", lock.display()), e))?;
    Ok(sha256_hex(&bytes)[..16].to_owned())
}

/// The npm registry URL for `mirror`.
pub fn npm_registry(mirror: Option<&str>) -> String {
    match mirror {
        Some(m) => format!("{}/npm/", m.trim_end_matches('/')),
        None => NPM_REGISTRY.to_owned(),
    }
}

impl LangStep for TsStep {
    fn tool_ids(&self, ctx: &StepCtx) -> Vec<String> {
        let node = ctx
            .set
            .pin(Tool::Node)
            .map_or("unpinned", |p| p.version.as_str());
        let lock = lock_hash(ctx.sdk)
            .map(|h| h[..8].to_owned())
            .unwrap_or_else(|_| "unknown".to_owned());
        vec![
            format!("node@{node}"),
            format!("jco@{JCO_VERSION} (lock {lock})"),
        ]
    }

    fn run(&self, ctx: &StepCtx) -> Result<(), BuildError> {
        if let Some(msg) = unsupported_libc("TypeScript") {
            return Err(step_err("node", msg));
        }
        let pin = ctx
            .set
            .pin(Tool::Node)
            .cloned()
            .ok_or_else(|| step_err("node", "no pin for this platform"))?;
        let node = block_on(|| toolchain::ensure(ctx.cache, &pin, ctx.fetch))??;
        let node_bin = node.join("bin");
        let path = format!("{}:{}", node_bin.display(), ctx.env["PATH"]);
        let npm = Npm {
            bin: node_bin.join("npm"),
            path: path.clone(),
            dir: ctx.cache.state_dir("npm"),
        };
        npm.write_rc(ctx)?;
        let toolchain = ensure_toolchain(ctx, &npm)?;

        // Project dependencies first: `npm ci` replaces node_modules.
        if has_lock_dependencies(&ctx.stage.join("package-lock.json")) {
            let mut cmd = npm.command(ctx);
            cmd.current_dir(ctx.stage)
                .args(["ci", "--ignore-scripts", "--no-audit", "--no-fund"]);
            run_streamed(&mut cmd, "npm ci (project dependencies)")?;
        }
        let sde = ctx
            .env
            .get("SOURCE_DATE_EPOCH")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        copy_tree(
            &ctx.sdk.join("ts"),
            &ctx.stage.join("node_modules").join("ostro"),
            sde,
        )?;

        let entry = ctx.stage.join(ctx.project.manifest.project.entry());
        let mut cmd = Command::new(toolchain.join("node_modules").join(".bin").join("jco"));
        cmd.current_dir(ctx.stage)
            .env_clear()
            .envs(ctx.env)
            .env("PATH", &path)
            .args(jco_args(
                &entry,
                &ctx.sdk.join("ts").join("wit"),
                &toolchain.join("rolldown.ostro.mjs"),
                ctx.out_wasm,
            ));
        run_streamed(&mut cmd, "jco componentize")
    }
}

struct Npm {
    bin: PathBuf,
    path: String,
    dir: PathBuf,
}

impl Npm {
    fn rc(&self) -> PathBuf {
        self.dir.join("npmrc")
    }

    /// The private npmrc (rewritten when the registry changes).
    fn write_rc(&self, ctx: &StepCtx) -> Result<(), BuildError> {
        std::fs::create_dir_all(&self.dir)
            .map_err(|e| BuildError::io(format!("creating {}", self.dir.display()), e))?;
        let text = format!("registry={}\n", npm_registry(ctx.fetch.mirror.as_deref()));
        if std::fs::read_to_string(self.rc()).is_ok_and(|t| t == text) {
            return Ok(());
        }
        crate::pipeline::write_atomic(&self.rc(), text.as_bytes())
    }

    fn command(&self, ctx: &StepCtx) -> Command {
        let mut cmd = Command::new(&self.bin);
        cmd.env_clear()
            .envs(ctx.env)
            .env("PATH", &self.path)
            .env("npm_config_userconfig", self.rc())
            .env("npm_config_cache", self.dir.join("npm-cache"))
            .env("npm_config_update_notifier", "false");
        if ctx.fetch.offline {
            cmd.env("npm_config_offline", "true");
        }
        for k in PASS_THROUGH {
            if let Some(v) = (ctx.host)(k) {
                cmd.env(k, v);
            }
        }
        cmd
    }
}

/// Install (once per lock hash) the pinned jco toolchain; returns its dir.
fn ensure_toolchain(ctx: &StepCtx, npm: &Npm) -> Result<PathBuf, BuildError> {
    let dir = ctx.cache.state_dir(&format!("jco-{}", lock_hash(ctx.sdk)?));
    let ok = dir.join(".ok");
    let jco = dir.join("node_modules").join(".bin").join("jco");
    let ready = || ok.is_file() && jco.exists();
    if ready() {
        return Ok(dir);
    }
    let lock = block_on(|| ctx.cache.lock())??;
    if ready() {
        drop(lock);
        return Ok(dir);
    }
    std::fs::create_dir_all(&dir)
        .map_err(|e| BuildError::io(format!("creating {}", dir.display()), e))?;
    for f in ["package.json", "package-lock.json", "rolldown.ostro.mjs"] {
        let from = ctx.sdk.join("toolchain").join(f);
        std::fs::copy(&from, dir.join(f))
            .map_err(|e| BuildError::io(format!("copying {}", from.display()), e))?;
    }
    eprintln!(
        "ostro: installing the jco {JCO_VERSION} toolchain (npm ci) into {}",
        dir.display()
    );
    let mut cmd = npm.command(ctx);
    cmd.current_dir(&dir)
        .args(["ci", "--ignore-scripts", "--no-audit", "--no-fund"]);
    run_streamed(&mut cmd, "npm ci (jco toolchain)")?;
    if !jco.exists() {
        return Err(step_err(
            "npm ci (jco toolchain)",
            format!("jco missing after npm ci in {}", dir.display()),
        ));
    }
    crate::pipeline::write_atomic(&ok, b"ok\n")?;
    drop(lock);
    Ok(dir)
}

/// The project lock lists at least one package besides the root.
fn has_lock_dependencies(lock: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(lock) else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
        return false;
    };
    v.get("packages")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|p| p.keys().any(|k| !k.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_follow_the_template() {
        let a = jco_args(
            Path::new("/s/src/index.ts"),
            Path::new("/w"),
            Path::new("/t/rolldown.ostro.mjs"),
            Path::new("/o.wasm"),
        );
        let a: Vec<String> = a.iter().map(|s| s.to_string_lossy().into_owned()).collect();
        assert_eq!(a[1], "/s/src/index.ts");
        assert!(a.contains(&"--backend-qjs-stub-wasi".to_owned()));
        assert_eq!(a.last().unwrap(), "/o.wasm");
        assert_eq!(npm_registry(None), NPM_REGISTRY);
        assert_eq!(
            npm_registry(Some("https://m.example/")),
            "https://m.example/npm/"
        );
    }

    #[test]
    fn lock_dependencies() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("package-lock.json");
        assert!(!has_lock_dependencies(&p));
        std::fs::write(&p, r#"{"packages":{"":{"name":"x"}}}"#).unwrap();
        assert!(!has_lock_dependencies(&p));
        std::fs::write(&p, r#"{"packages":{"":{},"node_modules/a":{}}}"#).unwrap();
        assert!(has_lock_dependencies(&p));
    }
}
