//! `ostro build` (CLI-04): build a workflow project into a validated Wasm component plus
//! `target/ostro/manifest.json`, on `ostro_build::build` (plan 10-12).
//!
//! Plan 10-03 fixed the flag set. The toolchain is `--toolchain` > `[build] toolchain` >
//! `auto`; `--docker` is Docker mode; the hidden `--in-container` (10-15's docker mode)
//! forces hermetic. The step's output streams to stderr; stdout carries the summary line or
//! the `ostro.build.result` envelope. Exit codes: 0 ok, 1 build or validation failure,
//! 2 no project / bad `ostro.toml` / bad arguments.

use std::path::PathBuf;

use clap::{Args, ValueEnum};
use ostro_build::{BuildError, BuildOptions, BuildOutput};
use serde_json::json;

use crate::output::{Outcome, local_json};

/// Where the build toolchain comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ToolchainMode {
    /// Rust: the system rustup when present, else hermetic; Python/TS: hermetic.
    Auto,
    /// Only the pinned toolchain under the ostro toolchain dir.
    Hermetic,
    /// Only tools found on PATH.
    System,
    /// Inside the `ostro-build` container image.
    Docker,
}

/// `ostro build [DIR] [--toolchain <auto|hermetic|system|docker>] [--docker] [--offline]
/// [--json]`.
#[derive(Debug, Clone, Args)]
pub struct BuildArgs {
    /// Project directory [default: .].
    #[arg(value_name = "DIR")]
    pub dir: Option<PathBuf>,
    /// Toolchain selection [default: `[build] toolchain` in ostro.toml, else auto].
    #[arg(long, value_enum)]
    pub toolchain: Option<ToolchainMode>,
    /// Shorthand for `--toolchain docker`.
    #[arg(long, conflicts_with = "toolchain")]
    pub docker: bool,
    /// Never download anything (fail if a toolchain is missing).
    #[arg(long)]
    pub offline: bool,
    /// Print an `ostro.build.result` JSON object.
    #[arg(long)]
    pub json: bool,
    /// Internal: running inside the build container.
    #[arg(long, hide = true)]
    pub in_container: bool,
}

impl BuildArgs {
    /// The effective toolchain mode from the flags alone (`--docker` wins; `auto` when
    /// neither flag is given).
    pub fn mode(&self) -> ToolchainMode {
        self.explicit_mode().unwrap_or(ToolchainMode::Auto)
    }

    /// The mode the flags select, `None` when the project's `[build] toolchain` decides.
    pub fn explicit_mode(&self) -> Option<ToolchainMode> {
        if self.docker {
            Some(ToolchainMode::Docker)
        } else if self.in_container {
            Some(ToolchainMode::Hermetic)
        } else {
            self.toolchain
        }
    }

    /// The pipeline options (`OSTRO_OFFLINE` is read by the pipeline itself).
    pub fn options(&self) -> BuildOptions {
        let mode = self.explicit_mode().map(|m| match m {
            ToolchainMode::Auto => ostro_build::ToolchainMode::Auto,
            ToolchainMode::Hermetic => ostro_build::ToolchainMode::Hermetic,
            ToolchainMode::System => ostro_build::ToolchainMode::System,
            ToolchainMode::Docker => ostro_build::ToolchainMode::Docker,
        });
        BuildOptions {
            dir: self.dir.clone().unwrap_or_else(|| PathBuf::from(".")),
            toolchain: mode,
            docker: self.docker,
            offline: self.offline,
            json: self.json,
        }
    }
}

/// Run `ostro build`: print the result (stdout) or one error line (stderr).
pub fn run(a: &BuildArgs) -> Outcome {
    let opts = a.options();
    match ostro_build::build(opts.clone()) {
        Ok(out) => {
            println!("{}", render(&opts, &out));
            Outcome::Ok
        }
        Err(e) => {
            eprintln!("ostro: error: {e}");
            outcome_of(&e)
        }
    }
}

/// The exit outcome of a failed build: usage errors (no project, bad `ostro.toml`) exit 2,
/// everything else (compiler errors, validation, toolchains) exits 1.
pub fn outcome_of(e: &BuildError) -> Outcome {
    if e.is_usage() {
        Outcome::Usage
    } else {
        Outcome::Error
    }
}

/// The stdout text of a successful build: the `ostro.build.result` envelope (`--json`) or
/// one summary line.
pub fn render(opts: &BuildOptions, out: &BuildOutput) -> String {
    let manifest = ostro_build::manifest::read(&out.manifest_path).ok();
    let (name, lang, tools, version) = match &manifest {
        Some(m) => (
            m.name.clone(),
            m.lang.as_str().to_owned(),
            m.toolchains.tools.join(", "),
            m.workflows
                .iter()
                .map(|w| format!("{}@{}", w.name, w.version))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        None => Default::default(),
    };
    if opts.json {
        let workflows: Vec<_> = out
            .workflows
            .iter()
            .map(|(n, v)| json!({"name": n, "version": v}))
            .collect();
        return local_json(
            "build",
            &json!({
                "name": name,
                "lang": lang,
                "wasm": out.wasm_path.display().to_string(),
                "sha256": out.sha256,
                "bytes": out.bytes,
                "workflows": workflows,
                "up_to_date": out.up_to_date,
                "manifest": out.manifest_path.display().to_string(),
            }),
        );
    }
    let sha12 = out.sha256.get(..12).unwrap_or(&out.sha256);
    let line = format!(
        "{name} sha256={sha12} bytes={} workflows=[{version}] (toolchain: {tools})",
        out.bytes
    );
    if out.up_to_date {
        format!("up to date: {line}")
    } else {
        format!("built {line}\n  {}", out.wasm_path.display())
    }
}
