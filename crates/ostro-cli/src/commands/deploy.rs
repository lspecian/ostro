//! `ostro deploy`:
//!
//! - `ostro deploy <file.wasm> [--activate] [--name NAME] [--tenant T]` (05-14, D12):
//!   upload only, no build. Its human output and its `{upload, activated}` JSON are frozen.
//! - `ostro deploy [DIR] [--activate[=BOOL]] [--no-build] [--toolchain T] [--docker]`
//!   (10-16, CLI-05): build the project when its input hash changed (`ostro_build::build`),
//!   upload `target/ostro/<name>.wasm` (`--name` > `[project] name`, `--tenant` >
//!   `[deploy] tenant`) and activate the described versions that were not active before
//!   the upload (`--activate` > `[deploy] activate`, default true). `--json` prints the
//!   `ostro.deploy.result` envelope `{build, upload, activated}` (D-P10-10).
//!
//! [`deploy_project`] is also the deploy step of `ostro dev --watch` ([`crate::watch`]).

use std::path::{Path, PathBuf};

use clap::Args;
use ostro_build::pipeline::{LangStep, StepCtx};
use ostro_build::{BuildError, BuildOptions, BuildOutput};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::Conn;
use super::build::ToolchainMode;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, local_json, s};

/// `ostro deploy`.
#[derive(Debug, Clone, Args)]
pub struct DeployArgs {
    /// The Wasm component to upload (`*.wasm`: upload only), or a project directory to
    /// build if needed and deploy [default: .].
    #[arg(value_name = "FILE|DIR")]
    pub file: Option<PathBuf>,
    /// Activate every workflow version the module describes (a project defaults to
    /// `[deploy] activate`, true; `--activate=false` uploads only).
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub activate: Option<bool>,
    /// Deployed module name (default: the file stem, or `[project] name`).
    #[arg(long)]
    pub name: Option<String>,
    /// Uploading tenant (default: `[deploy] tenant`, else the default tenant).
    #[arg(long)]
    pub tenant: Option<String>,
    /// Project only: never build; fail (exit 2) unless the built component is current.
    #[arg(long)]
    pub no_build: bool,
    /// Project only: toolchain selection [default: `[build] toolchain`, else auto].
    #[arg(long, value_enum)]
    pub toolchain: Option<ToolchainMode>,
    /// Project only: shorthand for `--toolchain docker`.
    #[arg(long, conflicts_with = "toolchain")]
    pub docker: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl DeployArgs {
    /// `--name`, else the file stem (`build/orders.wasm` -> `orders`).
    pub fn module_name(&self) -> Option<String> {
        self.name
            .clone()
            .or_else(|| self.file.as_deref().and_then(stem))
            .filter(|n| !n.is_empty())
    }

    /// The `.wasm` file of the 05-14 form, `None` for a project directory: a path with
    /// the `wasm` extension that is not a directory (a missing `x.wasm` keeps the 05-14
    /// "cannot read" error).
    pub fn wasm_file(&self) -> Option<&Path> {
        self.file
            .as_deref()
            .filter(|p| p.extension().is_some_and(|e| e == "wasm") && !p.is_dir())
    }

    /// The project deploy options of the directory form.
    pub fn project_options(&self) -> DeployOptions {
        let mode = match (self.docker, self.toolchain) {
            (true, _) => Some(ostro_build::ToolchainMode::Docker),
            (false, m) => m.map(|m| match m {
                ToolchainMode::Auto => ostro_build::ToolchainMode::Auto,
                ToolchainMode::Hermetic => ostro_build::ToolchainMode::Hermetic,
                ToolchainMode::System => ostro_build::ToolchainMode::System,
                ToolchainMode::Docker => ostro_build::ToolchainMode::Docker,
            }),
        };
        DeployOptions {
            dir: self.file.clone().unwrap_or_else(|| PathBuf::from(".")),
            name: self.name.clone(),
            tenant: self.tenant.clone(),
            activate: self.activate,
            no_build: self.no_build,
            toolchain: mode,
            docker: self.docker,
        }
    }
}

fn stem(p: &Path) -> Option<String> {
    p.file_stem().map(|s| s.to_string_lossy().into_owned())
}

/// Lowercase hex sha256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The activation name of a described workflow: tenant-qualified when `--tenant` is set
/// (sent as one `%2F`-encoded path segment).
pub fn activation_name(tenant: Option<&str>, workflow: &str) -> String {
    match tenant.filter(|t| !t.is_empty()) {
        Some(t) => format!("{t}/{workflow}"),
        None => workflow.to_owned(),
    }
}

/// The `(name, version)` pairs an upload response describes, in the server's order.
fn described_workflows(upload: &Value) -> Vec<(String, u64)> {
    upload["module"]["workflows"]
        .as_array()
        .map(|ws| {
            ws.iter()
                .filter_map(|w| Some((w["name"].as_str()?.to_owned(), w["version"].as_u64()?)))
                .collect()
        })
        .unwrap_or_default()
}

/// `ostro deploy`: the 05-14 `.wasm` form, or a project.
pub async fn deploy(c: &Client, out: Out, a: &DeployArgs) -> Result<Outcome, CallError> {
    match a.wasm_file() {
        Some(file) => deploy_wasm(c, out, a, file).await,
        None => deploy_project_verb(c, out, a).await,
    }
}

/// The 05-14 form (D12), unchanged: upload `file`, optionally activate.
async fn deploy_wasm(
    c: &Client,
    out: Out,
    a: &DeployArgs,
    file: &Path,
) -> Result<Outcome, CallError> {
    let wasm = std::fs::read(file)
        .map_err(|e| CallError::Usage(format!("cannot read {}: {e}", file.display())))?;
    let sha = sha256_hex(&wasm);
    let size = wasm.len();
    let name = a.module_name();
    let up = c
        .upload_module(&sha, wasm, name.as_deref(), a.tenant.as_deref())
        .await?;
    let described = described_workflows(&up.body);

    if !out.json {
        let state = if up.status == 201 {
            "created"
        } else {
            "unchanged"
        };
        println!(
            "uploaded {} ({state}, {size} bytes){}",
            &sha[..12],
            name.as_deref()
                .map(|n| format!(" as module {n}"))
                .unwrap_or_default()
        );
        let list: Vec<String> = described.iter().map(|(n, v)| format!("{n}@{v}")).collect();
        println!(
            "workflows: {}",
            if list.is_empty() {
                "-".to_owned()
            } else {
                list.join(", ")
            }
        );
    }
    if !a.activate.unwrap_or(false) {
        if out.json {
            out.json(&up.body);
        }
        return Ok(Outcome::Ok);
    }

    let mut activated = Vec::new();
    for (wf, version) in &described {
        let qn = activation_name(a.tenant.as_deref(), wf);
        let r = c.activate(&qn, *version).await?;
        if !out.json {
            println!(
                "activated {qn}@{version} (active version {})",
                s(&r.body["active_version"])
            );
        }
        activated.push(r.body);
    }
    if out.json {
        out.json(&json!({ "upload": up.body, "activated": Value::Array(activated) }));
    }
    Ok(Outcome::Ok)
}

// ---------------------------------------------------------------------------------
// Project deploy (10-16)
// ---------------------------------------------------------------------------------

/// Options of one project deploy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeployOptions {
    /// The project directory.
    pub dir: PathBuf,
    /// `--name` (default `[project] name`).
    pub name: Option<String>,
    /// `--tenant` (default `[deploy] tenant`).
    pub tenant: Option<String>,
    /// `--activate[=BOOL]` (default `[deploy] activate`).
    pub activate: Option<bool>,
    /// `--no-build`: require a current `target/ostro/<name>.wasm`.
    pub no_build: bool,
    /// `--toolchain` (`None`: `[build] toolchain`).
    pub toolchain: Option<ostro_build::ToolchainMode>,
    /// `--docker`.
    pub docker: bool,
}

/// What a project deploy did.
#[derive(Debug, Clone)]
pub struct DeployReport {
    /// The project name (`[project] name`).
    pub project: String,
    /// The build (or the up-to-date check).
    pub build: BuildOutput,
    /// The upload's REST body.
    pub upload: Value,
    /// `true` when the server stored a new module (201), `false` when the sha existed.
    pub created: bool,
    /// The described `(workflow, version)` pairs.
    pub workflows: Vec<(String, u64)>,
    /// Activation was requested.
    pub activate: bool,
    /// The activation REST bodies of the versions that were not active before the upload.
    pub activated: Vec<Value>,
    /// The `(qualified name, version)` pairs of [`DeployReport::activated`].
    pub activated_refs: Vec<(String, u64)>,
}

/// Why a project deploy failed.
#[derive(Debug)]
pub enum DeployError {
    /// The build (or the project load) failed; compiler diagnostics were streamed to
    /// stderr.
    Build(BuildError),
    /// `--no-build` without a current artefact (exit 2).
    NotBuilt(String),
    /// A REST call failed.
    Call(CallError),
}

impl DeployError {
    /// The exit outcome: usage errors (no project, bad `ostro.toml`, `--no-build` without
    /// a current component) exit 2; build, validation and API errors exit 1.
    pub fn outcome(&self) -> Outcome {
        match self {
            DeployError::Build(e) if e.is_usage() => Outcome::Usage,
            DeployError::NotBuilt(_) | DeployError::Call(CallError::Usage(_)) => Outcome::Usage,
            DeployError::Build(_) | DeployError::Call(_) => Outcome::Error,
        }
    }

    /// One line, plus a hint for a `(workflow, version)` already registered by another
    /// module (the source changed but its workflow version did not).
    pub fn message(&self) -> String {
        match self {
            DeployError::Build(e) => e.to_string(),
            DeployError::NotBuilt(m) => m.clone(),
            DeployError::Call(e) => match version_conflict(e) {
                Some(wf) => format!(
                    "{} (the module changed but {wf} did not: bump the workflow's version in \
                     the source to deploy it)",
                    e.message()
                ),
                None => e.message(),
            },
        }
    }
}

/// `name@version` of a 409 "already registered by module" upload conflict.
fn version_conflict(e: &CallError) -> Option<String> {
    let CallError::Api { status: 409, body } = e else {
        return None;
    };
    let d = &body["error"]["details"];
    d.get("existing_module_sha256")?;
    Some(format!(
        "{}@{}",
        d["workflow"].as_str()?,
        d["version"].as_u64()?
    ))
}

impl From<CallError> for DeployError {
    fn from(e: CallError) -> Self {
        DeployError::Call(e)
    }
}

/// The step name `--no-build` reports when the pipeline would have to run the language
/// step (the artefact is missing or stale).
const NO_BUILD_STEP: &str = "ostro deploy --no-build";

/// A language step that never builds: the pipeline calls it only when the input hash
/// does not match the recorded build.
struct NoBuildStep;

impl LangStep for NoBuildStep {
    fn tool_ids(&self, _ctx: &StepCtx) -> Vec<String> {
        Vec::new()
    }

    fn run(&self, _ctx: &StepCtx) -> Result<(), BuildError> {
        Err(BuildError::Step {
            step: NO_BUILD_STEP.to_owned(),
            message: "stale".to_owned(),
            stderr_tail: String::new(),
        })
    }
}

/// The build half: `ostro_build::build`, or with `--no-build` the pipeline's own
/// up-to-date check (the same input hash, mode resolution and manifest checks) with a
/// step that refuses to build.
fn build_step(opts: &DeployOptions) -> Result<BuildOutput, DeployError> {
    let bo = BuildOptions {
        dir: opts.dir.clone(),
        toolchain: opts.toolchain,
        docker: opts.docker,
        offline: false,
        json: false,
    };
    if !opts.no_build {
        return ostro_build::build(bo).map_err(DeployError::Build);
    }
    let host = |k: &str| std::env::var(k).ok();
    let never = |_, _| -> Box<dyn LangStep> { Box::new(NoBuildStep) };
    match ostro_build::pipeline::build_with(&bo, &host, &never) {
        Ok(out) => Ok(out),
        Err(BuildError::Step { step, .. }) if step == NO_BUILD_STEP => {
            let name = ostro_build::project::load(&opts.dir)
                .map(|p| p.manifest.project.name)
                .unwrap_or_else(|_| "<name>".to_owned());
            Err(DeployError::NotBuilt(format!(
                "--no-build: target/ostro/{name}.wasm is missing or stale; run `ostro build` first"
            )))
        }
        Err(e) => Err(DeployError::Build(e)),
    }
}

/// Run the (blocking) build on its own thread, so neither the caller's runtime nor a
/// runtime shutdown waits on it.
async fn build_off_runtime(opts: &DeployOptions) -> Result<BuildOutput, DeployError> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let o = opts.clone();
    std::thread::Builder::new()
        .name("ostro-build".to_owned())
        .spawn(move || {
            let _ = tx.send(build_step(&o));
        })
        .map_err(|e| DeployError::Build(BuildError::io("starting the build thread", e)))?;
    rx.await.unwrap_or_else(|_| {
        Err(DeployError::Build(BuildError::io(
            "the build thread",
            std::io::Error::other("panicked"),
        )))
    })
}

/// The active version of every workflow, keyed by its (qualified) name, from
/// `GET /v1/workflows`.
async fn active_versions(c: &Client) -> Result<Vec<(String, Option<u64>)>, CallError> {
    let r = c.list_workflows().await?;
    let list = r
        .body
        .get("workflows")
        .and_then(Value::as_array)
        .or_else(|| r.body.as_array())
        .cloned()
        .unwrap_or_default();
    Ok(list
        .iter()
        .filter_map(|w| Some((w["name"].as_str()?.to_owned(), w["active_version"].as_u64())))
        .collect())
}

/// Build if needed, upload, and activate the described versions that were not active
/// before the upload (a version the upload itself auto-activated counts as activated
/// here; an already-active version is left alone and not reported).
pub async fn deploy_project(c: &Client, opts: &DeployOptions) -> Result<DeployReport, DeployError> {
    let dir = std::path::absolute(&opts.dir).map_err(|e| {
        DeployError::Build(BuildError::io(
            format!("resolving {}", opts.dir.display()),
            e,
        ))
    })?;
    let project = ostro_build::project::load(&dir).map_err(|e| DeployError::Build(e.into()))?;
    let opts = DeployOptions {
        dir,
        ..opts.clone()
    };
    let build = build_off_runtime(&opts).await?;

    let name = opts
        .name
        .clone()
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| project.manifest.project.name.clone());
    let tenant = opts
        .tenant
        .clone()
        .or_else(|| project.manifest.deploy.tenant.clone())
        .filter(|t| !t.is_empty());
    let activate = opts.activate.unwrap_or(project.manifest.deploy.activate);

    let wasm = std::fs::read(&build.wasm_path).map_err(|e| {
        DeployError::Build(BuildError::io(
            format!("reading {}", build.wasm_path.display()),
            e,
        ))
    })?;
    let sha = sha256_hex(&wasm);
    let before = if activate {
        active_versions(c).await?
    } else {
        Vec::new()
    };
    let up = c
        .upload_module(&sha, wasm, Some(&name), tenant.as_deref())
        .await?;
    let workflows = described_workflows(&up.body);

    let mut activated = Vec::new();
    let mut activated_refs = Vec::new();
    if activate {
        for (wf, version) in &workflows {
            let qn = activation_name(tenant.as_deref(), wf);
            let was = before.iter().find(|(n, _)| *n == qn).and_then(|(_, v)| *v);
            if was == Some(*version) {
                continue;
            }
            let r = c.activate(&qn, *version).await?;
            activated.push(r.body);
            activated_refs.push((qn, *version));
        }
    }
    Ok(DeployReport {
        project: project.manifest.project.name,
        build,
        created: up.status == 201,
        upload: up.body,
        workflows,
        activate,
        activated,
        activated_refs,
    })
}

/// The `build` object of the `ostro.deploy.result` envelope (the `ostro.build.result`
/// fields).
pub fn build_json(project: &str, b: &BuildOutput) -> Value {
    let manifest = ostro_build::manifest::read(&b.manifest_path).ok();
    let workflows: Vec<_> = b
        .workflows
        .iter()
        .map(|(n, v)| json!({"name": n, "version": v}))
        .collect();
    json!({
        "name": project,
        "lang": manifest.as_ref().map(|m| m.lang.as_str()).unwrap_or_default(),
        "wasm": b.wasm_path.display().to_string(),
        "sha256": b.sha256,
        "bytes": b.bytes,
        "workflows": workflows,
        "up_to_date": b.up_to_date,
        "manifest": b.manifest_path.display().to_string(),
    })
}

/// The `--json` output of a project deploy.
pub fn render_json(r: &DeployReport) -> String {
    local_json(
        "deploy",
        &json!({
            "build": build_json(&r.project, &r.build),
            "upload": r.upload,
            "activated": Value::Array(r.activated.clone()),
        }),
    )
}

/// The human output of a project deploy (build line, upload line, activation lines).
pub fn render_human(r: &DeployReport) -> String {
    let bo = BuildOptions::default();
    let mut lines = vec![super::build::render(&bo, &r.build)];
    let sha12 = r.build.sha256.get(..12).unwrap_or(&r.build.sha256);
    let state = if r.created { "created" } else { "unchanged" };
    lines.push(format!(
        "uploaded {sha12} ({state}, {} bytes) as module {}",
        r.build.bytes, r.project
    ));
    let list: Vec<String> = r
        .workflows
        .iter()
        .map(|(n, v)| format!("{n}@{v}"))
        .collect();
    lines.push(format!(
        "workflows: {}",
        if list.is_empty() {
            "-".to_owned()
        } else {
            list.join(", ")
        }
    ));
    if r.activate {
        if r.activated_refs.is_empty() {
            lines.push("activated: - (already active)".to_owned());
        }
        for (qn, v) in &r.activated_refs {
            lines.push(format!("activated {qn}@{v}"));
        }
    } else {
        lines.push("not activated (--activate=false)".to_owned());
    }
    lines.join("\n")
}

/// `ostro deploy [DIR]`.
async fn deploy_project_verb(c: &Client, out: Out, a: &DeployArgs) -> Result<Outcome, CallError> {
    match deploy_project(c, &a.project_options()).await {
        Ok(r) => {
            if out.json {
                println!("{}", render_json(&r));
            } else {
                println!("{}", render_human(&r));
            }
            Ok(Outcome::Ok)
        }
        // REST errors keep the CLI-06 reporting (the server's body on stdout with --json),
        // except a version conflict, which gets its hint.
        Err(DeployError::Call(e)) if version_conflict(&e).is_none() => Err(e),
        Err(e) => {
            if let DeployError::Call(CallError::Api { body, .. }) = &e
                && out.json
            {
                out.json(body);
            }
            eprintln!("ostro: error: {}", e.message());
            Ok(e.outcome())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    #[test]
    fn name_defaults_to_the_file_stem() {
        let ClientCommand::Deploy(a) = parse(&["deploy", "build/orders.wasm"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.module_name().as_deref(), Some("orders"));
        assert_eq!(a.activate, None);
        assert_eq!(a.tenant, None);
        assert_eq!(a.wasm_file(), Some(Path::new("build/orders.wasm")));
        let ClientCommand::Deploy(a) = parse(&[
            "deploy",
            "x.wasm",
            "--activate",
            "--name",
            "billing",
            "--tenant",
            "acme",
        ])
        .unwrap() else {
            panic!()
        };
        assert_eq!(a.module_name().as_deref(), Some("billing"));
        assert_eq!(a.activate, Some(true));
        assert_eq!(a.tenant.as_deref(), Some("acme"));
        // `--activate` before the path does not take it as its value.
        let ClientCommand::Deploy(a) = parse(&["deploy", "--activate", "x.wasm"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.activate, Some(true));
        assert_eq!(a.wasm_file(), Some(Path::new("x.wasm")));
    }

    #[test]
    fn project_form_flags() {
        // No path: the project in `.`.
        let ClientCommand::Deploy(a) = parse(&["deploy"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.wasm_file(), None);
        let o = a.project_options();
        assert_eq!(o.dir, PathBuf::from("."));
        assert_eq!(o.activate, None);
        assert!(!o.no_build);
        assert_eq!(o.toolchain, None);
        let ClientCommand::Deploy(a) = parse(&[
            "deploy",
            "proj",
            "--activate=false",
            "--no-build",
            "--toolchain",
            "system",
        ])
        .unwrap() else {
            panic!()
        };
        let o = a.project_options();
        assert_eq!(o.dir, PathBuf::from("proj"));
        assert_eq!(o.activate, Some(false));
        assert!(o.no_build);
        assert_eq!(o.toolchain, Some(ostro_build::ToolchainMode::System));
        let ClientCommand::Deploy(a) = parse(&["deploy", "--docker"]).unwrap() else {
            panic!()
        };
        assert_eq!(
            a.project_options().toolchain,
            Some(ostro_build::ToolchainMode::Docker)
        );
        assert_eq!(
            parse(&["deploy", "--docker", "--toolchain", "system"])
                .unwrap_err()
                .exit_code(),
            2
        );
    }

    #[test]
    fn conflict_hint() {
        let e = CallError::Api {
            status: 409,
            body: json!({"error": {"code": "conflict", "message": "workflow hello@1 is already registered by module ab",
                "details": {"workflow": "hello", "version": 1, "existing_module_sha256": "ab"}}}),
        };
        assert_eq!(version_conflict(&e).as_deref(), Some("hello@1"));
        let m = DeployError::Call(e).message();
        assert!(m.contains("bump the workflow's version"), "{m}");
        let other = CallError::Api {
            status: 409,
            body: json!({"error": {"code": "conflict", "message": "x"}}),
        };
        assert_eq!(version_conflict(&other), None);
        assert_eq!(DeployError::NotBuilt("x".into()).outcome(), Outcome::Usage);
    }

    #[test]
    fn sha_and_activation_names() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(activation_name(None, "w"), "w");
        assert_eq!(activation_name(Some(""), "w"), "w");
        assert_eq!(activation_name(Some("acme"), "w"), "acme/w");
    }
}
