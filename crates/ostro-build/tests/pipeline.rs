//! 10-12: the build pipeline with fake language steps: input hash, up-to-date cache,
//! staging, the scrubbed environment, local validation + describe, manifest.json, and
//! failed builds that never look up to date.
//!
//! The fake steps write prebuilt guests (`guests/smoke`, `guests/wasi-forbidden`, built
//! once into `target/guests` with the system toolchain, offline).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ostro_build::manifest;
use ostro_build::pipeline::{
    self, BuildError, BuildOptions, LangStep, StepCtx, build_with, input_hash, sha256_hex,
};
use ostro_build::project::{self, Lang, ToolchainMode};
use ostro_build::sdk_bundle;
use ostro_build::toolchain;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Build the two guests once per process (`target/guests`, as the engine tests do).
fn guest(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
            .args(["build", "--release", "--target", "wasm32-wasip2"])
            .args(["-p", "smoke", "-p", "wasi-forbidden", "--manifest-path"])
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("spawn cargo for the guests");
        assert!(
            out.status.success(),
            "guests build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    });
    let path = target_dir
        .join("wasm32-wasip2/release")
        .join(format!("{}.wasm", name.replace('-', "_")));
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// What the fake step does.
#[derive(Clone)]
enum Act {
    Write(Vec<u8>),
    Fail,
}

/// A fake step: counts calls, records the env and the staged files' mtimes.
#[derive(Clone)]
struct Fake {
    act: Act,
    calls: Arc<AtomicUsize>,
    env: Arc<Mutex<Option<BTreeMap<String, String>>>>,
    staged: Arc<Mutex<Vec<(String, u64)>>>,
}

impl Fake {
    fn new(act: Act) -> Fake {
        Fake {
            act,
            calls: Arc::new(AtomicUsize::new(0)),
            env: Arc::new(Mutex::new(None)),
            staged: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl LangStep for Fake {
    fn tool_ids(&self, _ctx: &StepCtx) -> Vec<String> {
        vec!["fake@1".to_owned()]
    }
    fn run(&self, ctx: &StepCtx) -> Result<(), BuildError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.env.lock().unwrap() = Some(ctx.env.clone());
        let mut staged = Vec::new();
        collect(ctx.stage, ctx.stage, &mut staged);
        *self.staged.lock().unwrap() = staged;
        assert!(ctx.sdk.join(sdk_bundle::BUNDLE_ID_FILE).is_file());
        assert_ne!(ctx.mode, ToolchainMode::Auto);
        match &self.act {
            Act::Write(b) => {
                std::fs::write(ctx.out_wasm, b).unwrap();
                Ok(())
            }
            Act::Fail => Err(BuildError::Step {
                step: "fake".into(),
                message: "exited with exit status: 101".into(),
                stderr_tail: "error[E0425]: cannot find value `x`".into(),
            }),
        }
    }
}

fn collect(root: &Path, dir: &Path, out: &mut Vec<(String, u64)>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        let p = e.path();
        let mtime = std::fs::metadata(&p)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
        out.push((rel, mtime));
        if p.is_dir() {
            collect(root, &p, out);
        }
    }
}

/// A host env with no PATH (Rust `auto` resolves to hermetic) and a temp HOME.
fn host_env(home: &Path) -> impl Fn(&str) -> Option<String> + Sync + use<> {
    let home = home.display().to_string();
    move |k: &str| match k {
        "HOME" => Some(home.clone()),
        "OSTRO_TOOLCHAIN_DIR" => Some(format!("{home}/toolchains")),
        _ => None,
    }
}

fn write(dir: &Path, rel: &str, text: &str) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, text).unwrap();
}

fn rust_project() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "ostro.toml",
        "[project]\nname = \"fake\"\nlang = \"rust\"\n",
    );
    write(
        d.path(),
        "Cargo.toml",
        "[package]\nname = \"fake\"\nversion = \"0.1.0\"\n",
    );
    write(d.path(), "Cargo.lock", "version = 4\n");
    write(d.path(), "src/lib.rs", "pub fn a() {}\n");
    d
}

fn opts(dir: &Path) -> BuildOptions {
    BuildOptions {
        dir: dir.to_path_buf(),
        ..BuildOptions::default()
    }
}

fn hash(dir: &Path, mode: ToolchainMode) -> String {
    let p = project::load(dir).unwrap();
    input_hash(&p, mode, "set", "bundle").unwrap()
}

#[test]
fn input_hash_tracks_sources_not_noise() {
    let d = rust_project();
    let root = d.path();
    let h0 = hash(root, ToolchainMode::System);
    assert_eq!(h0, hash(root, ToolchainMode::System), "stable");
    assert_eq!(h0.len(), 64);

    // Noise: ignored dirs anywhere, editor temp files, files outside the source set.
    for rel in [
        "target/ostro/x.wasm",
        "src/node_modules/m.rs",
        "node_modules/m/index.js",
        "src/__pycache__/x.pyc",
        ".ostro/state",
        ".git/HEAD",
        "src/lib.rs~",
        "src/.#lib.rs",
        "src/.lib.rs.swp",
        "src/4913",
        "README.md",
        "tests/workflow.rs",
    ] {
        write(root, rel, "noise");
        assert_eq!(
            hash(root, ToolchainMode::System),
            h0,
            "{rel} changed the hash"
        );
    }
    // Different set / bundle ids change it.
    let p = project::load(root).unwrap();
    assert_ne!(
        input_hash(&p, ToolchainMode::System, "set2", "bundle").unwrap(),
        h0
    );
    assert_ne!(
        input_hash(&p, ToolchainMode::System, "set", "bundle2").unwrap(),
        h0
    );

    // Mode.
    assert_ne!(hash(root, ToolchainMode::Hermetic), h0, "mode");
    // One source byte.
    write(root, "src/lib.rs", "pub fn b() {}\n");
    let h1 = hash(root, ToolchainMode::System);
    assert_ne!(h1, h0, "source byte");
    // A new source file.
    write(root, "src/m/mod.rs", "\n");
    let h2 = hash(root, ToolchainMode::System);
    assert_ne!(h2, h1, "new source file");
    // Lockfile.
    write(root, "Cargo.lock", "version = 4\n# x\n");
    let h3 = hash(root, ToolchainMode::System);
    assert_ne!(h3, h2, "lockfile");
    // ostro.toml.
    write(
        root,
        "ostro.toml",
        "[project]\nname = \"fake\"\nlang = \"rust\"\n[build]\ntoolchain = \"system\"\n",
    );
    assert_ne!(hash(root, ToolchainMode::System), h3, "ostro.toml");
}

#[test]
fn python_and_ts_source_sets() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path();
    write(
        root,
        "ostro.toml",
        "[project]\nname = \"py\"\nlang = \"python\"\n",
    );
    for rel in [
        "app.py",
        "pkg/util.py",
        "pyproject.toml",
        "uv.lock",
        "requirements-dev.txt",
        "tests/test_app.py",
        ".venv/lib/x.py",
        "venv/x.py",
        "notes.txt",
    ] {
        write(root, rel, "x");
    }
    let p = project::load(root).unwrap();
    assert_eq!(
        ostro_build::stage::source_files(&p).unwrap(),
        [
            "app.py",
            "ostro.toml",
            "pkg/util.py",
            "pyproject.toml",
            "requirements-dev.txt",
            "uv.lock"
        ]
    );

    let d = tempfile::tempdir().unwrap();
    let root = d.path();
    write(
        root,
        "ostro.toml",
        "[project]\nname = \"t\"\nlang = \"ts\"\n",
    );
    for rel in [
        "src/index.ts",
        "src/lib/a.ts",
        "package.json",
        "package-lock.json",
        "tsconfig.json",
        "test/x.test.js",
        "node_modules/ostro/index.js",
    ] {
        write(root, rel, "x");
    }
    let p = project::load(root).unwrap();
    assert_eq!(
        ostro_build::stage::source_files(&p).unwrap(),
        [
            "ostro.toml",
            "package-lock.json",
            "package.json",
            "src/index.ts",
            "src/lib/a.ts",
            "tsconfig.json"
        ]
    );
}

#[test]
fn ignore_rules() {
    use pipeline::is_ignored;
    for p in [
        "target/x",
        "a/node_modules/b",
        "__pycache__/m.pyc",
        ".git/config",
        ".venv/x",
        "x~",
        ".#x",
        "a.swp",
        "4913",
    ] {
        assert!(is_ignored(Path::new(p)), "{p}");
    }
    for p in ["src/lib.rs", "targets.rs", "src/4914", "my_target/x"] {
        assert!(!is_ignored(Path::new(p)), "{p}");
    }
}

#[test]
fn good_build_writes_manifest_then_is_up_to_date() {
    let wasm = guest("smoke");
    let d = rust_project();
    let home = tempfile::tempdir().unwrap();
    let host = host_env(home.path());
    // A stale file in the stage must disappear.
    write(d.path(), "target/ostro/stage/stale.txt", "old");
    let fake = Fake::new(Act::Write(wasm.clone()));
    let f2 = fake.clone();
    let steps = move |lang: Lang, mode: ToolchainMode| -> Box<dyn LangStep> {
        assert_eq!((lang, mode), (Lang::Rust, ToolchainMode::Hermetic));
        Box::new(f2.clone())
    };

    let out = build_with(&opts(d.path()), &host, &steps).unwrap();
    assert_eq!(fake.calls(), 1);
    assert!(!out.up_to_date);
    assert_eq!(out.sha256, sha256_hex(&wasm));
    assert_eq!(out.bytes, wasm.len() as u64);
    assert_eq!(out.workflows, vec![("smoke".to_owned(), 1)]);
    let outdir = d.path().join("target/ostro");
    assert_eq!(out.wasm_path, outdir.join("fake.wasm"));
    assert_eq!(std::fs::read(&out.wasm_path).unwrap(), wasm);
    assert_eq!(out.manifest_path, outdir.join("manifest.json"));

    // manifest.json: every field, in order.
    let text = std::fs::read_to_string(&out.manifest_path).unwrap();
    let keys = [
        "kind",
        "version",
        "name",
        "lang",
        "module",
        "workflows",
        "toolchains",
        "sdk_bundle",
        "source_date_epoch",
        "input_hash",
        "embedded_runtimes",
    ];
    let pos: Vec<usize> = keys
        .iter()
        .map(|k| {
            text.find(&format!("\n  \"{k}\":"))
                .unwrap_or_else(|| panic!("{k}: {text}"))
        })
        .collect();
    assert!(pos.windows(2).all(|w| w[0] < w[1]), "field order: {text}");
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["kind"], "ostro.manifest");
    assert_eq!(v["version"], 1);
    assert_eq!(v["name"], "fake");
    assert_eq!(v["lang"], "rust");
    assert_eq!(v["module"]["sha256"], out.sha256.as_str());
    assert_eq!(v["module"]["bytes"], wasm.len() as u64);
    assert_eq!(
        v["workflows"],
        serde_json::json!([{"name": "smoke", "version": 1}])
    );
    assert_eq!(v["toolchains"]["set_id"], toolchain::set_id().as_str());
    assert_eq!(v["toolchains"]["tools"], serde_json::json!(["fake@1"]));
    assert_eq!(v["sdk_bundle"], sdk_bundle::bundle_id().as_str());
    assert_eq!(v["source_date_epoch"], 0);
    assert_eq!(v["embedded_runtimes"], serde_json::json!([]));
    let m = manifest::read(&out.manifest_path).unwrap();
    let inputs = std::fs::read_to_string(outdir.join("inputs.sha256")).unwrap();
    assert_eq!(inputs.trim(), m.input_hash);

    // The scrubbed env, exactly.
    let env = fake.env.lock().unwrap().clone().unwrap();
    assert_eq!(
        env.keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "HOME",
            "LC_ALL",
            "PATH",
            "PYTHONDONTWRITEBYTECODE",
            "PYTHONHASHSEED",
            "SOURCE_DATE_EPOCH",
            "TZ"
        ]
    );
    assert_eq!(env["TZ"], "UTC");
    assert_eq!(env["LC_ALL"], "C");
    assert_eq!(env["PYTHONHASHSEED"], "0");
    assert_eq!(env["PYTHONDONTWRITEBYTECODE"], "1");
    assert_eq!(env["SOURCE_DATE_EPOCH"], "0", "outside git");
    assert_eq!(env["PATH"], "/usr/bin:/bin");
    assert_eq!(env["HOME"], home.path().display().to_string());

    // The fresh stage: exactly the sources, every mtime = SOURCE_DATE_EPOCH (0).
    let staged = fake.staged.lock().unwrap().clone();
    let mut names: Vec<&str> = staged.iter().map(|(n, _)| n.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "Cargo.lock",
            "Cargo.toml",
            "ostro.toml",
            "src",
            "src/lib.rs"
        ]
    );
    assert!(staged.iter().all(|(_, t)| *t == 0), "{staged:?}");
    assert!(!outdir.join("stage/stale.txt").exists());

    // Second build: up to date, the step does not run.
    let t = std::time::Instant::now();
    let again = build_with(&opts(d.path()), &host, &steps).unwrap();
    assert!(again.up_to_date);
    assert_eq!(fake.calls(), 1, "step ran on an up-to-date build");
    assert_eq!(again.sha256, out.sha256);
    assert_eq!(again.workflows, out.workflows);
    assert!(
        t.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        t.elapsed()
    );

    // A changed source rebuilds; a tampered wasm also rebuilds.
    write(d.path(), "src/lib.rs", "pub fn c() {}\n");
    assert!(
        !build_with(&opts(d.path()), &host, &steps)
            .unwrap()
            .up_to_date
    );
    assert_eq!(fake.calls(), 2);
    std::fs::write(&out.wasm_path, b"tampered").unwrap();
    assert!(
        !build_with(&opts(d.path()), &host, &steps)
            .unwrap()
            .up_to_date
    );
    assert_eq!(fake.calls(), 3);
    assert_eq!(std::fs::read(&out.wasm_path).unwrap(), wasm);
}

#[test]
fn forbidden_import_fails_validation_and_writes_nothing() {
    let wasm = guest("wasi-forbidden");
    let d = rust_project();
    let home = tempfile::tempdir().unwrap();
    let host = host_env(home.path());
    let fake = Fake::new(Act::Write(wasm));
    let steps = move |_: Lang, _: ToolchainMode| -> Box<dyn LangStep> { Box::new(fake.clone()) };
    let err = build_with(&opts(d.path()), &host, &steps).unwrap_err();
    let BuildError::Validation(msg) = &err else {
        panic!("expected Validation, got {err:?}")
    };
    assert!(
        msg.contains("wasi:filesystem") && msg.contains("not allowed"),
        "{msg}"
    );
    let outdir = d.path().join("target/ostro");
    assert!(!outdir.join("manifest.json").exists());
    assert!(!outdir.join("inputs.sha256").exists());
    assert!(!outdir.join("fake.wasm").exists());
    // No temp file left behind either.
    let leftovers: Vec<_> = std::fs::read_dir(&outdir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".tmp-"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    assert!(!err.is_usage());
}

#[test]
fn failing_step_keeps_the_previous_good_build() {
    let wasm = guest("smoke");
    let d = rust_project();
    let home = tempfile::tempdir().unwrap();
    let host = host_env(home.path());
    let good = Fake::new(Act::Write(wasm));
    let steps = move |_: Lang, _: ToolchainMode| -> Box<dyn LangStep> { Box::new(good.clone()) };
    let out = build_with(&opts(d.path()), &host, &steps).unwrap();
    let wasm_before = std::fs::read(&out.wasm_path).unwrap();
    let manifest_before = std::fs::read(&out.manifest_path).unwrap();

    write(d.path(), "src/lib.rs", "pub fn broken( {}\n");
    let bad = Fake::new(Act::Fail);
    let b2 = bad.clone();
    let steps = move |_: Lang, _: ToolchainMode| -> Box<dyn LangStep> { Box::new(b2.clone()) };
    let err = build_with(&opts(d.path()), &host, &steps).unwrap_err();
    let BuildError::Step { stderr_tail, .. } = &err else {
        panic!("expected Step, got {err:?}")
    };
    assert!(stderr_tail.contains("E0425"), "{stderr_tail}");
    assert_eq!(std::fs::read(&out.wasm_path).unwrap(), wasm_before);
    assert_eq!(std::fs::read(&out.manifest_path).unwrap(), manifest_before);
    // ... but it is not up to date any more: the next build runs the step again.
    let err2 = build_with(&opts(d.path()), &host, &steps).unwrap_err();
    assert!(matches!(err2, BuildError::Step { .. }));
    assert_eq!(bad.calls(), 2);
}

#[test]
fn no_project_is_a_usage_error() {
    let d = tempfile::tempdir().unwrap();
    let host = host_env(d.path());
    let err = build_with(&opts(&d.path().join("nope")), &host, &pipeline::step_for).unwrap_err();
    assert!(err.is_usage(), "{err:?}");
}

#[test]
fn source_date_epoch_from_env_reaches_the_stage() {
    let wasm = guest("smoke");
    let d = rust_project();
    let home = tempfile::tempdir().unwrap();
    let home_s = home.path().display().to_string();
    let host = move |k: &str| match k {
        "HOME" => Some(home_s.clone()),
        "SOURCE_DATE_EPOCH" => Some("1700000000".to_owned()),
        _ => None,
    };
    let fake = Fake::new(Act::Write(wasm));
    let f2 = fake.clone();
    let steps = move |_: Lang, _: ToolchainMode| -> Box<dyn LangStep> { Box::new(f2.clone()) };
    let out = build_with(&opts(d.path()), &host, &steps).unwrap();
    let env = fake.env.lock().unwrap().clone().unwrap();
    assert_eq!(env["SOURCE_DATE_EPOCH"], "1700000000");
    let staged = fake.staged.lock().unwrap().clone();
    assert!(
        staged.iter().all(|(_, t)| *t == 1_700_000_000),
        "{staged:?}"
    );
    assert_eq!(
        manifest::read(&out.manifest_path)
            .unwrap()
            .source_date_epoch,
        1_700_000_000
    );
}

#[test]
fn python_and_ts_steps_are_not_implemented_yet() {
    let d = tempfile::tempdir().unwrap();
    write(
        d.path(),
        "ostro.toml",
        "[project]\nname = \"p\"\nlang = \"python\"\n",
    );
    write(d.path(), "app.py", "x = 1\n");
    let home = tempfile::tempdir().unwrap();
    let host = host_env(home.path());
    let err = build_with(&opts(d.path()), &host, &pipeline::step_for).unwrap_err();
    assert!(
        matches!(err, BuildError::NotImplemented("10-15")),
        "{err:?}"
    );
}
