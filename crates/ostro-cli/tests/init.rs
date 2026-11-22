//! 10-08: `ostro init` (CLI-03). Each template's generated tree is snapshot-tested; dir
//! and name validation, the `ostro.init.result` envelope and SDK materialisation are
//! checked. The verb is parsed with clap exactly as the `ostro` binary embeds it.

use std::path::{Path, PathBuf};
use std::process::Command;

use clap::Parser;
use ostro_build::sdk_bundle;
use ostro_cli::ClientCommand;
use ostro_cli::commands::init;
use ostro_cli::output::Outcome;
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(name = "ostro")]
struct Cli {
    #[command(subcommand)]
    cmd: ClientCommand,
}

fn args(argv: &[&str]) -> init::InitArgs {
    let mut full = vec!["ostro", "init"];
    full.extend_from_slice(argv);
    match Cli::try_parse_from(full).expect("parse").cmd {
        ClientCommand::Init(a) => a,
        other => panic!("not init: {other:?}"),
    }
}

fn init_in(dir: &Path, extra: &[&str]) -> (Outcome, Result<String, String>) {
    let d = dir.to_str().unwrap();
    let mut argv = vec![d];
    argv.extend_from_slice(extra);
    init::execute(&args(&argv))
}

/// Sorted relative paths under `root`; `target/ostro/sdk/**` is collapsed to its
/// `.bundle-id` marker.
fn listing(root: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            let rel = p
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if rel == "target/ostro/sdk" {
                assert!(p.join(".bundle-id").is_file(), "no .bundle-id in {rel}");
                out.push("target/ostro/sdk/.bundle-id".to_owned());
            } else if p.is_dir() {
                walk(root, &p, out);
            } else {
                out.push(rel);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

fn tree_snapshot(lang: &str, manifest: &str) -> (tempfile::TempDir, PathBuf, String) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("hello-wf");
    let (o, r) = init_in(&dir, &["--lang", lang]);
    assert_eq!(o, Outcome::Ok, "{r:?}");
    let mut s = String::from("# files\n");
    for f in listing(&dir) {
        s.push_str(&f);
        s.push('\n');
    }
    for f in ["ostro.toml", manifest] {
        s.push_str(&format!("\n# {f}\n"));
        s.push_str(&std::fs::read_to_string(dir.join(f)).unwrap());
    }
    (tmp, dir, s)
}

#[test]
fn tree_rust() {
    let (_t, dir, s) = tree_snapshot("rust", "Cargo.toml");
    insta::assert_snapshot!("tree_rust", s);
    let lib = std::fs::read_to_string(dir.join("src/lib.rs")).unwrap();
    for needle in [
        "#[workflow(name = \"hello\"",
        "#[step]",
        "3 * 24 * 60 * 60",
        "\"approve\"",
    ] {
        assert!(lib.contains(needle), "lib.rs lacks {needle}");
    }
    assert!(
        dir.join("target/ostro/sdk/rust/ostro-sdk/Cargo.toml")
            .is_file()
    );
    assert!(
        dir.join("target/ostro/sdk/rust/ostro-client/Cargo.toml")
            .is_file()
    );
}

#[test]
fn tree_python() {
    let (_t, dir, s) = tree_snapshot("python", "pyproject.toml");
    insta::assert_snapshot!("tree_python", s);
    let app = std::fs::read_to_string(dir.join("app.py")).unwrap();
    for needle in [
        "@ostro.workflow(name=\"hello\")",
        "@ostro.step",
        "3 * 24 * 60 * 60",
        "\"approve\"",
        "ostro.export()",
    ] {
        assert!(app.contains(needle), "app.py lacks {needle}");
    }
    assert!(
        dir.join("target/ostro/sdk/python/ostro/__init__.py")
            .is_file()
    );
}

#[test]
fn tree_ts() {
    let (_t, dir, s) = tree_snapshot("ts", "package.json");
    insta::assert_snapshot!("tree_ts", s);
    let idx = std::fs::read_to_string(dir.join("src/index.ts")).unwrap();
    for needle in [
        "workflow({ name: \"hello\" }",
        "step(\"greet\"",
        "waitSignal",
        "\"approve\"",
        "export { workflows }",
    ] {
        assert!(idx.contains(needle), "index.ts lacks {needle}");
    }
    let pkg: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("package.json")).unwrap()).unwrap();
    assert_eq!(pkg["name"], "hello-wf");
    assert!(pkg.get("devDependencies").is_none());
    let tsconfig: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("tsconfig.json")).unwrap()).unwrap();
    assert_eq!(
        tsconfig["compilerOptions"]["paths"]["ostro"][0],
        "target/ostro/sdk/ts/src/index.ts"
    );
    assert!(dir.join("target/ostro/sdk/ts/src/index.ts").is_file());
}

/// Every generated ostro.toml loads through the strict project model.
#[test]
fn generated_projects_load() {
    for (lang, want) in [
        ("rust", ostro_build::Lang::Rust),
        ("python", ostro_build::Lang::Python),
        ("ts", ostro_build::Lang::Ts),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let (o, r) = init_in(tmp.path(), &["--lang", lang, "--name", "my_wf-2"]);
        assert_eq!(o, Outcome::Ok, "{r:?}");
        let p = ostro_build::project::load(tmp.path()).unwrap();
        assert!(!p.detected);
        assert_eq!(p.manifest.project.lang, want);
        assert_eq!(p.manifest.project.name, "my_wf-2");
        assert_eq!(p.manifest.build.toolchain, ostro_build::ToolchainMode::Auto);
        assert!(p.manifest.deploy.activate);
        let gi = std::fs::read_to_string(tmp.path().join(".gitignore")).unwrap();
        for l in ["target/", "node_modules/", "__pycache__/", ".ostro/"] {
            assert!(gi.lines().any(|x| x == l), "{lang} .gitignore lacks {l}");
        }
        for (_, text) in ostro_build::templates::files(want) {
            assert!(!ostro_build::templates::substitute(text, "x").contains("{{"));
        }
    }
}

#[test]
fn non_empty_dir_needs_force() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("notes.txt"), "keep").unwrap();
    std::fs::write(tmp.path().join("ostro.toml"), "old").unwrap();
    let (o, r) = init_in(tmp.path(), &["--lang", "python", "--name", "p"]);
    assert_eq!(o, Outcome::Usage);
    let e = r.unwrap_err();
    assert!(e.contains("not empty") && e.contains("--force"), "{e}");
    assert!(!e.contains('\n'), "one line: {e}");
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("ostro.toml")).unwrap(),
        "old"
    );

    let (o, r) = init_in(tmp.path(), &["--lang", "python", "--name", "p", "--force"]);
    assert_eq!(o, Outcome::Ok, "{r:?}");
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("notes.txt")).unwrap(),
        "keep"
    );
    assert!(
        std::fs::read_to_string(tmp.path().join("ostro.toml"))
            .unwrap()
            .contains("name = \"p\"")
    );

    // a file in place of the directory
    let f = tmp.path().join("notes.txt");
    let (o, _) = init_in(&f, &["--lang", "ts", "--name", "p"]);
    assert_eq!(o, Outcome::Usage);
}

#[test]
fn bad_names_exit_2() {
    let tmp = tempfile::tempdir().unwrap();
    for bad in ["a/b", "a:b", "__x", "UPPER", "", &"a".repeat(65), "sp ace"] {
        let (o, r) = init_in(&tmp.path().join("p"), &["--lang", "rust", "--name", bad]);
        assert_eq!(o, Outcome::Usage, "{bad}");
        let e = r.unwrap_err();
        assert!(e.contains("invalid project name"), "{bad}: {e}");
    }
    // the directory name is the default name
    let (o, r) = init_in(&tmp.path().join("Bad_Dir"), &["--lang", "ts"]);
    assert_eq!(o, Outcome::Usage);
    assert!(r.unwrap_err().contains("--name"));
    // Rust package names cannot start with a digit
    let (o, _) = init_in(&tmp.path().join("p"), &["--lang", "rust", "--name", "1wf"]);
    assert_eq!(o, Outcome::Usage);
    let (o, r) = init_in(
        &tmp.path().join("p2"),
        &["--lang", "python", "--name", "1wf"],
    );
    assert_eq!(o, Outcome::Ok, "{r:?}");
    // nothing was written for the rejected invocations
    assert!(!tmp.path().join("p").exists());
    assert!(!tmp.path().join("Bad_Dir").exists());
    // --lang is required; unknown languages are rejected by clap
    let (o, _) = init::execute(&args(&[tmp.path().join("q").to_str().unwrap()]));
    assert_eq!(o, Outcome::Usage);
    assert!(Cli::try_parse_from(["ostro", "init", "--lang", "go"]).is_err());
}

#[test]
fn json_envelope() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wf");
    let (o, r) = init_in(&dir, &["--lang", "ts", "--json"]);
    assert_eq!(o, Outcome::Ok);
    let text = r.unwrap();
    assert!(
        text.starts_with("{\n  \"kind\": \"ostro.init.result\",\n  \"version\": 1,\n"),
        "{text}"
    );
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["kind"], "ostro.init.result");
    assert_eq!(v["version"], 1);
    assert_eq!(v["lang"], "ts");
    assert_eq!(v["name"], "wf");
    assert_eq!(v["sdk_bundle"], sdk_bundle::bundle_id());
    assert_eq!(
        Path::new(v["dir"].as_str().unwrap()),
        dir.canonicalize().unwrap()
    );
    let files: Vec<&str> = v["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f.as_str().unwrap())
        .collect();
    assert!(
        files.contains(&".gitignore") && files.contains(&"src/index.ts"),
        "{files:?}"
    );
    assert_eq!(
        files.len(),
        ostro_build::templates::files(ostro_build::Lang::Ts).len()
    );
    assert_eq!(v.as_object().unwrap().len(), 7, "{v}");
}

#[test]
fn human_output_has_next_steps() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wf");
    let (o, r) = init_in(&dir, &["--lang", "python"]);
    assert_eq!(o, Outcome::Ok);
    let text = r.unwrap();
    for needle in [
        "Created python project `wf`",
        "app.py",
        "Next steps:",
        "cd ",
        "ostro dev --watch",
        "ostro deploy",
        "ostro run hello --input '{\"name\":\"ada\"}' --wait",
        "pytest",
        &sdk_bundle::bundle_id(),
    ] {
        assert!(text.contains(needle), "lacks {needle}: {text}");
    }
}

#[test]
fn rematerialise_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let (o, _) = init_in(tmp.path(), &["--lang", "rust", "--name", "r"]);
    assert_eq!(o, Outcome::Ok);
    let sdk = tmp.path().join("target/ostro/sdk");
    let lib = sdk.join("rust/ostro-sdk/src/lib.rs");
    let before = std::fs::metadata(&lib).unwrap().modified().unwrap();
    let (o, _) = init_in(tmp.path(), &["--lang", "rust", "--name", "r", "--force"]);
    assert_eq!(o, Outcome::Ok);
    assert_eq!(std::fs::metadata(&lib).unwrap().modified().unwrap(), before);
    sdk_bundle::materialize(&sdk).unwrap();
    assert_eq!(std::fs::metadata(&lib).unwrap().modified().unwrap(), before);
    assert_eq!(
        std::fs::read_to_string(sdk.join(".bundle-id"))
            .unwrap()
            .trim(),
        sdk_bundle::bundle_id()
    );
}

/// The generated Rust template type-checks for the host against the bundled SDK, offline
/// (the pinned dependencies are in the local cargo cache wherever this workspace builds).
/// Building for wasm32-wasip2 and running the template tests is 10-15 / 10-17.
#[test]
fn rust_template_checks_offline() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("hello");
    let (o, r) = init_in(&dir, &["--lang", "rust"]);
    assert_eq!(o, Outcome::Ok, "{r:?}");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let out = Command::new(cargo)
        .args(["check", "--offline", "--quiet", "--manifest-path"])
        .arg(dir.join("Cargo.toml"))
        .env_remove("CARGO_TARGET_DIR")
        .env_remove("CARGO_BUILD_TARGET_DIR")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .current_dir(&dir)
        .output()
        .expect("run cargo");
    assert!(
        out.status.success(),
        "cargo check failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // the SDK copies are path dependencies, not members (they live under target/)
    assert!(dir.join("Cargo.lock").is_file());
    let lock = std::fs::read_to_string(dir.join("Cargo.lock")).unwrap();
    assert!(lock.contains("name = \"ostro-sdk\""), "{lock}");
}
