//! The build pipeline (`ostro build`, CLI-04): one flow for every language.
//!
//! 1. load the project ([`crate::project::load`]);
//! 2. compute the input hash ([`input_hash`]: the language's sources and lockfiles,
//!    `ostro.toml`, the toolchain-set id, the SDK bundle id and the resolved toolchain mode);
//! 3. stop with `up_to_date` when `target/ostro/inputs.sha256` holds that hash, the wasm
//!    exists and still matches `manifest.json`;
//! 4. materialise the SDK (`target/ostro/sdk`) and stage the sources into a fresh
//!    `target/ostro/stage` with every mtime = `SOURCE_DATE_EPOCH` ([`crate::stage`]);
//! 5. run the language step ([`LangStep`]) with the scrubbed D-P9-3 environment
//!    ([`scrubbed_env`]);
//! 6. validate the component locally with `ostro_engine` (the server's import rules,
//!    ENG-1 included) and describe its workflows ([`crate::validate`]);
//! 7. write `target/ostro/<name>.wasm` (temp file + rename), `manifest.json`
//!    ([`crate::manifest`]) and, last, `inputs.sha256`.
//!
//! A failed build never looks up to date: `inputs.sha256` is removed before the step runs
//! and only written after the wasm and the manifest. A failing step or validation leaves
//! the previous good wasm and manifest untouched.
//!
//! Owner: 10-12. Python, TypeScript and Docker plug in at [`step_for`] (10-15).

use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use sha2::{Digest as _, Sha256};

use crate::cache::Cache;
use crate::fetch::FetchOptions;
use crate::manifest;
use crate::project::{self, Lang, Project, ProjectError, ToolchainMode};
use crate::toolchain::{self, Platform, ToolchainError, ToolchainSet};
use crate::{sdk_bundle, stage, validate};

/// Paths never staged, hashed or watched (shared with `ostro dev --watch`, D-P10-14 / W7).
/// Entries ending in `/` are directory names at any depth; the rest are file-name patterns
/// (`*` prefix = suffix match, `*` suffix = prefix match).
pub const IGNORE: &[&str] = &[
    "target/",
    "node_modules/",
    "__pycache__/",
    ".ostro/",
    ".git/",
    ".venv/",
    "venv/",
    ".tox/",
    ".mypy_cache/",
    "*~",
    ".#*",
    "*.swp",
    "4913",
];

/// `target/ostro/inputs.sha256`: the input hash of the last successful build.
pub const INPUTS_FILE: &str = "inputs.sha256";
/// `target/ostro/manifest.json`.
pub const MANIFEST_FILE: &str = "manifest.json";

/// `true` when a directory called `name` is in [`IGNORE`].
pub fn ignored_dir(name: &str) -> bool {
    IGNORE
        .iter()
        .filter_map(|p| p.strip_suffix('/'))
        .any(|d| d == name)
}

/// `true` when a file called `name` matches a file pattern of [`IGNORE`].
pub fn ignored_file(name: &str) -> bool {
    IGNORE.iter().filter(|p| !p.ends_with('/')).any(|p| {
        if let Some(suffix) = p.strip_prefix('*') {
            name.ends_with(suffix)
        } else if let Some(prefix) = p.strip_suffix('*') {
            name.starts_with(prefix)
        } else {
            name == *p
        }
    })
}

/// `true` when the relative path `rel` is ignored: one of its directory components is an
/// ignored directory, or its file name matches an ignored file pattern.
pub fn is_ignored(rel: &Path) -> bool {
    let comps: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let Some((file, dirs)) = comps.split_last() else {
        return false;
    };
    dirs.iter().any(|d| ignored_dir(d)) || ignored_file(file)
}

/// Options of one `ostro build`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuildOptions {
    /// Project directory (holds `ostro.toml` or a detectable project).
    pub dir: PathBuf,
    /// `--toolchain`; `None` = `[build] toolchain`, else [`ToolchainMode::Auto`].
    pub toolchain: Option<ToolchainMode>,
    /// `--docker` (Docker mode).
    pub docker: bool,
    /// `--offline` / `OSTRO_OFFLINE`: never download.
    pub offline: bool,
    /// `--json` output.
    pub json: bool,
}

/// Result of a successful build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOutput {
    /// `target/ostro/<name>.wasm`.
    pub wasm_path: PathBuf,
    /// Lower-case hex sha256 of the component.
    pub sha256: String,
    /// Component size in bytes.
    pub bytes: u64,
    /// Exported workflows and their versions (from `Runtime::describe`).
    pub workflows: Vec<(String, u32)>,
    /// The input hash was unchanged and the language step did not run.
    pub up_to_date: bool,
    /// `target/ostro/manifest.json`.
    pub manifest_path: PathBuf,
}

/// Why a build failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// The named plan has not landed yet.
    #[error("not yet implemented ({0})")]
    NotImplemented(&'static str),
    /// No project, a bad `ostro.toml`, an invalid name.
    #[error(transparent)]
    Project(#[from] ProjectError),
    /// A toolchain could not be resolved, downloaded or verified.
    #[error(transparent)]
    Toolchain(#[from] ToolchainError),
    /// A filesystem error.
    #[error("{context}: {source}")]
    Io {
        /// What was being done.
        context: String,
        /// The error.
        #[source]
        source: std::io::Error,
    },
    /// The language step failed (its diagnostics were streamed to stderr).
    #[error("{step} failed: {message}")]
    Step {
        /// The step or tool (`cargo build`, `rustup-init`, ...).
        step: String,
        /// Exit status or spawn error.
        message: String,
        /// The last lines of the step's output.
        stderr_tail: String,
    },
    /// The component violates the engine's validation rules (or `describe()` failed).
    #[error("the component failed validation: {0}")]
    Validation(String),
}

impl BuildError {
    /// `true` for errors caused by the invocation (exit code 2): no project, a bad
    /// manifest or name.
    pub fn is_usage(&self) -> bool {
        matches!(self, BuildError::Project(e) if e.is_usage())
    }

    /// An I/O error with context.
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        BuildError::Io {
            context: context.into(),
            source,
        }
    }
}

/// A host environment lookup (the process environment in production; tests inject a
/// map so they never mutate the process env).
pub type HostEnv<'a> = &'a (dyn Fn(&str) -> Option<String> + Sync);

/// What a language step sees.
pub struct StepCtx<'a> {
    /// The project.
    pub project: &'a Project,
    /// The fresh staging dir (`target/ostro/stage`).
    pub stage: &'a Path,
    /// The materialised SDK (`target/ostro/sdk`).
    pub sdk: &'a Path,
    /// Where the step writes the component (a temp file next to the final wasm).
    pub out_wasm: &'a Path,
    /// The scrubbed environment ([`scrubbed_env`]); the step adds its own tool dirs to
    /// `PATH` and its tool-specific variables, nothing from the host otherwise.
    pub env: &'a BTreeMap<String, String>,
    /// The toolchain cache.
    pub cache: &'a Cache,
    /// Download options (`offline`, mirror).
    pub fetch: &'a FetchOptions,
    /// The resolved toolchain mode (never `Auto`).
    pub mode: ToolchainMode,
    /// The pinned toolchain set of this platform.
    pub set: &'a ToolchainSet,
    /// The host environment (tool lookup on the host `PATH`, pass-through variables).
    pub host: HostEnv<'a>,
}

/// A language build step (Rust here; Python, TypeScript, Docker in 10-15).
pub trait LangStep {
    /// The tool ids recorded in `manifest.json` (`system-rustup`, `rustup-init@1.29.3`,
    /// `zig@0.17.1`, ...).
    fn tool_ids(&self, ctx: &StepCtx) -> Vec<String>;
    /// Build the component into `ctx.out_wasm`.
    fn run(&self, ctx: &StepCtx) -> Result<(), BuildError>;
}

/// The step of `lang` in the resolved `mode`. Python, TypeScript and Docker mode return
/// [`BuildError::NotImplemented`] until 10-15 plugs `lang::python`, `lang::ts` and
/// `lang::docker` in here.
pub fn step_for(lang: Lang, mode: ToolchainMode) -> Box<dyn LangStep> {
    match (lang, mode) {
        (_, ToolchainMode::Docker) | (Lang::Python, _) | (Lang::Ts, _) => Box::new(NotYet),
        (Lang::Rust, m) => Box::new(crate::lang::rust::RustStep { mode: m }),
    }
}

/// The placeholder step of the languages 10-15 implements.
struct NotYet;

impl LangStep for NotYet {
    fn tool_ids(&self, _ctx: &StepCtx) -> Vec<String> {
        Vec::new()
    }

    fn run(&self, _ctx: &StepCtx) -> Result<(), BuildError> {
        Err(BuildError::NotImplemented("10-15"))
    }
}

/// Resolve `Auto`: Rust uses the system rustup when `rustup` is on the host `PATH`, else
/// the hermetic toolchain; Python and TypeScript are hermetic. Other modes are returned
/// unchanged.
pub fn resolve_mode(lang: Lang, mode: ToolchainMode, host: HostEnv) -> ToolchainMode {
    match (lang, mode) {
        (Lang::Rust, ToolchainMode::Auto) if which_in(host, "rustup").is_some() => {
            ToolchainMode::System
        }
        (_, ToolchainMode::Auto) => ToolchainMode::Hermetic,
        (_, m) => m,
    }
}

/// The spelling of a mode in the input hash and messages.
pub fn mode_str(mode: ToolchainMode) -> &'static str {
    match mode {
        ToolchainMode::Auto => "auto",
        ToolchainMode::Hermetic => "hermetic",
        ToolchainMode::System => "system",
        ToolchainMode::Docker => "docker",
    }
}

/// The first executable called `name` on the host `PATH`.
pub fn which_in(host: HostEnv, name: &str) -> Option<PathBuf> {
    let path = host("PATH")?;
    std::env::split_paths(&path)
        .filter(|d| !d.as_os_str().is_empty())
        .map(|d| d.join(name))
        .find(|p| is_executable(p))
}

/// A C compiler (`cc`, `gcc` or `clang`) is on the host `PATH`.
pub fn cc_on_path(host: HostEnv) -> bool {
    ["cc", "gcc", "clang"]
        .iter()
        .any(|c| which_in(host, c).is_some())
}

fn is_executable(p: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        p.is_file()
    }
}

/// The base `PATH` of every step (steps prepend their tool dirs).
pub const BASE_PATH: &str = "/usr/bin:/bin";

/// The scrubbed build environment (exactly build-guest.sh's, D-P9-3 step 1): `TZ=UTC`,
/// `LC_ALL=C`, `PYTHONHASHSEED=0`, `PYTHONDONTWRITEBYTECODE=1`, `SOURCE_DATE_EPOCH`,
/// `PATH` ([`BASE_PATH`]) and `HOME` (the host's, else `fallback_home`). Every other host
/// variable is cleared (`env_clear`).
pub fn scrubbed_env(
    host: HostEnv,
    source_date_epoch: u64,
    fallback_home: &Path,
) -> BTreeMap<String, String> {
    let home = host("HOME")
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| fallback_home.to_string_lossy().into_owned());
    BTreeMap::from([
        ("TZ".to_owned(), "UTC".to_owned()),
        ("LC_ALL".to_owned(), "C".to_owned()),
        ("PYTHONHASHSEED".to_owned(), "0".to_owned()),
        ("PYTHONDONTWRITEBYTECODE".to_owned(), "1".to_owned()),
        (
            "SOURCE_DATE_EPOCH".to_owned(),
            source_date_epoch.to_string(),
        ),
        ("PATH".to_owned(), BASE_PATH.to_owned()),
        ("HOME".to_owned(), home),
    ])
}

/// The input hash: sha256 over a version tag, the language, the resolved mode, the
/// toolchain-set id, the SDK bundle id, then every source file ([`stage::source_files`],
/// sorted) as length-prefixed relative path and bytes.
pub fn input_hash(
    project: &Project,
    mode: ToolchainMode,
    set_id: &str,
    bundle_id: &str,
) -> Result<String, BuildError> {
    let files = stage::source_files(project)
        .map_err(|e| BuildError::io(format!("listing {}", project.root.display()), e))?;
    let mut h = Sha256::new();
    let mut field = |b: &[u8]| {
        h.update((b.len() as u64).to_le_bytes());
        h.update(b);
    };
    field(b"ostro.inputs/v1");
    field(project.manifest.project.lang.as_str().as_bytes());
    field(project.manifest.project.name.as_bytes());
    field(mode_str(mode).as_bytes());
    field(set_id.as_bytes());
    field(bundle_id.as_bytes());
    for rel in &files {
        let path = project.root.join(rel);
        let bytes =
            std::fs::read(&path).map_err(|e| BuildError::io(format!("reading {rel}"), e))?;
        field(rel.as_bytes());
        field(&bytes);
    }
    Ok(hex(&h.finalize()))
}

/// Lower-case hex.
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// sha256 hex of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Builds the project in `opts.dir` with the process environment and the real steps.
pub fn build(opts: BuildOptions) -> Result<BuildOutput, BuildError> {
    let host = |k: &str| std::env::var(k).ok();
    build_with(&opts, &host, &step_for)
}

/// A step factory ([`step_for`] in production).
pub type StepFactory<'a> = &'a dyn Fn(Lang, ToolchainMode) -> Box<dyn LangStep>;

/// [`build`] with an injected host environment and step factory.
pub fn build_with(
    opts: &BuildOptions,
    host: HostEnv,
    steps: StepFactory,
) -> Result<BuildOutput, BuildError> {
    // Absolute: the steps run tools with the project as their cwd (a relative
    // CARGO_TARGET_DIR would resolve against it).
    let dir = std::path::absolute(&opts.dir)
        .map_err(|e| BuildError::io(format!("resolving {}", opts.dir.display()), e))?;
    let project = project::load(&dir)?;
    let lang = project.manifest.project.lang;
    let requested = if opts.docker {
        ToolchainMode::Docker
    } else {
        opts.toolchain.unwrap_or(project.manifest.build.toolchain)
    };
    let mode = resolve_mode(lang, requested, host);

    let set = match Platform::detect().and_then(|p| toolchain::toolchain_set(&p)) {
        Ok(s) => s,
        // Unsupported platform: system builds still work, hermetic ones fail in `ensure`.
        Err(_) => ToolchainSet {
            id: toolchain::set_id(),
            pins: Vec::new(),
        },
    };
    let bundle_id = sdk_bundle::bundle_id();
    let input = input_hash(&project, mode, &set.id, &bundle_id)?;

    let out = project::output_dir(&project);
    let name = project.manifest.project.name.clone();
    let wasm_path = out.join(format!("{name}.wasm"));
    let manifest_path = out.join(MANIFEST_FILE);
    let inputs_path = out.join(INPUTS_FILE);

    if let Some(done) = up_to_date(&inputs_path, &wasm_path, &manifest_path, &input) {
        return Ok(done);
    }

    std::fs::create_dir_all(&out)
        .map_err(|e| BuildError::io(format!("creating {}", out.display()), e))?;
    // A failed build must never look up to date.
    match std::fs::remove_file(&inputs_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(BuildError::io(
                format!("removing {}", inputs_path.display()),
                e,
            ));
        }
    }

    let sdk = out.join("sdk");
    sdk_bundle::materialize(&sdk)
        .map_err(|e| BuildError::io(format!("materialising the SDK into {}", sdk.display()), e))?;
    let sde = stage::source_date_epoch(&project, host);
    let stage_dir = out.join("stage");
    stage::stage(&project, &stage_dir, sde)?;
    let env = scrubbed_env(host, sde, &out);

    let mut fetch = FetchOptions::from_env_with(host)?;
    fetch.offline |= opts.offline;
    let cache = match Cache::resolve_with(host, std::env::consts::OS) {
        Ok(c) => c,
        Err(e) if mode == ToolchainMode::Hermetic => return Err(e.into()),
        // No HOME and no OSTRO_TOOLCHAIN_DIR: only hermetic builds need the cache.
        Err(_) => Cache::with_root(out.join("toolchains")),
    };

    let tmp_wasm = out.join(format!(".{name}.wasm.tmp-{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp_wasm);
    let ctx = StepCtx {
        project: &project,
        stage: &stage_dir,
        sdk: &sdk,
        out_wasm: &tmp_wasm,
        env: &env,
        cache: &cache,
        fetch: &fetch,
        mode,
        set: &set,
        host,
    };
    let step = steps(lang, mode);
    let tools = step.tool_ids(&ctx);
    let result = step.run(&ctx).and_then(|()| {
        let bytes = std::fs::read(&tmp_wasm).map_err(|e| {
            BuildError::io(
                format!("the build step wrote no component ({})", tmp_wasm.display()),
                e,
            )
        })?;
        let workflows = validate::validate_and_describe(&bytes)?;
        Ok((bytes, workflows))
    });
    let (bytes, workflows) = match result {
        Ok(v) => v,
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_wasm);
            return Err(e);
        }
    };

    let sha256 = sha256_hex(&bytes);
    // The step may have created a lockfile (cargo without Cargo.lock): record the hash of
    // the sources as they are after the build, so the next run is up to date.
    let input = input_hash(&project, mode, &set.id, &bundle_id).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp_wasm);
    })?;
    std::fs::rename(&tmp_wasm, &wasm_path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp_wasm);
        BuildError::io(format!("writing {}", wasm_path.display()), e)
    })?;
    let m = manifest::Manifest {
        kind: manifest::KIND.to_owned(),
        version: manifest::VERSION,
        name: name.clone(),
        lang,
        module: manifest::Module {
            sha256: sha256.clone(),
            bytes: bytes.len() as u64,
        },
        workflows: workflows
            .iter()
            .map(|(name, version)| manifest::Workflow {
                name: name.clone(),
                version: *version,
            })
            .collect(),
        toolchains: manifest::Toolchains {
            set_id: set.id.clone(),
            tools,
        },
        sdk_bundle: bundle_id,
        source_date_epoch: sde,
        input_hash: input.clone(),
        embedded_runtimes: manifest::embedded_runtimes(lang),
    };
    manifest::write(&manifest_path, &m)?;
    write_atomic(&inputs_path, format!("{input}\n").as_bytes())?;

    Ok(BuildOutput {
        wasm_path,
        sha256,
        bytes: bytes.len() as u64,
        workflows,
        up_to_date: false,
        manifest_path,
    })
}

/// The cached result when `inputs.sha256` holds `input`, the manifest records the same
/// input hash and the wasm still has the manifest's sha.
fn up_to_date(
    inputs_path: &Path,
    wasm_path: &Path,
    manifest_path: &Path,
    input: &str,
) -> Option<BuildOutput> {
    let stored = std::fs::read_to_string(inputs_path).ok()?;
    if stored.trim() != input {
        return None;
    }
    let m = manifest::read(manifest_path).ok()?;
    if m.input_hash != input {
        return None;
    }
    let bytes = std::fs::read(wasm_path).ok()?;
    if sha256_hex(&bytes) != m.module.sha256 {
        return None;
    }
    Some(BuildOutput {
        wasm_path: wasm_path.to_path_buf(),
        sha256: m.module.sha256.clone(),
        bytes: bytes.len() as u64,
        workflows: m
            .workflows
            .iter()
            .map(|w| (w.name.clone(), w.version))
            .collect(),
        up_to_date: true,
        manifest_path: manifest_path.to_path_buf(),
    })
}

/// Write `bytes` to a temp file next to `path`, then rename it into place.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), BuildError> {
    let file = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = path.with_file_name(format!(".{file}.tmp-{}", std::process::id()));
    std::fs::write(&tmp, bytes)
        .and_then(|()| std::fs::rename(&tmp, path))
        .map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            BuildError::io(format!("writing {}", path.display()), e)
        })
}

/// Lines of step output kept for [`BuildError::Step`].
const TAIL_LINES: usize = 60;

/// Run `cmd` (named `step` in errors) with stdout and stderr streamed line by line to
/// this process's stderr (stdout stays clean for `--json`); the last lines are kept for
/// the error.
pub fn run_streamed(cmd: &mut Command, step: &str) -> Result<(), BuildError> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| BuildError::Step {
        step: step.to_owned(),
        message: format!("cannot run {:?}: {e}", cmd.get_program()),
        stderr_tail: String::new(),
    })?;
    let tail = Arc::new(Mutex::new(std::collections::VecDeque::<String>::new()));
    let pump = |r: Box<dyn Read + Send>| {
        let tail = Arc::clone(&tail);
        std::thread::spawn(move || {
            for line in BufReader::new(r).lines() {
                let Ok(line) = line else { break };
                eprintln!("{line}");
                let mut t = tail.lock().unwrap_or_else(|p| p.into_inner());
                if t.len() == TAIL_LINES {
                    t.pop_front();
                }
                t.push_back(line);
            }
        })
    };
    let mut pumps = Vec::new();
    if let Some(o) = child.stdout.take() {
        pumps.push(pump(Box::new(o)));
    }
    if let Some(e) = child.stderr.take() {
        pumps.push(pump(Box::new(e)));
    }
    let status = child.wait().map_err(|e| BuildError::Step {
        step: step.to_owned(),
        message: format!("waiting for the process: {e}"),
        stderr_tail: String::new(),
    })?;
    for p in pumps {
        let _ = p.join();
    }
    if status.success() {
        return Ok(());
    }
    let tail = tail
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    Err(BuildError::Step {
        step: step.to_owned(),
        message: format!("exited with {status}"),
        stderr_tail: tail,
    })
}

/// Drive `f` to completion on a fresh current-thread runtime in a scoped thread, so it
/// works whether or not the caller is inside a tokio runtime.
pub fn block_on<F, T>(f: impl FnOnce() -> F + Send) -> Result<T, BuildError>
where
    F: std::future::Future<Output = T>,
    T: Send,
{
    std::thread::scope(|s| {
        s.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| BuildError::io("starting the async runtime", e))?;
            Ok(rt.block_on(f()))
        })
        .join()
        .unwrap_or_else(|p| std::panic::resume_unwind(p))
    })
}
