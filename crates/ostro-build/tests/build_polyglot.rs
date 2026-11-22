//! 10-15: hermetic Python and TypeScript builds (polyglot only: uv 0.7.15, a uv-managed
//! CPython 3.12, the componentize-py 0.25.1 wheel, Node 24.13.0 and the jco toolchain are
//! downloaded into the shared, reused `target/ostro-toolchains-test` cache; PyPI is reached
//! for the dependency tests). The host environment is injected (a temp `HOME`, the system
//! `PATH`, the CI proxies), so no host Python, uv or Node is used. Each test prints its
//! cold/warm timings. Fails loudly without network.
//!
//! Run with `--test-threads=1` (each build stages hundreds of MB of tooling).

#![cfg(feature = "polyglot")]

use std::path::{Path, PathBuf};
use std::time::Instant;

use ostro_build::pipeline::{BuildError, BuildOptions, BuildOutput, build_with, step_for};
use ostro_build::project::{Lang, ToolchainMode};

fn cache_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/ostro-toolchains-test")
}

/// The injected host environment: `vars` first, then `PATH` and the proxies of the
/// machine.
fn host_with(home: &Path, vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + Sync {
    let mut v: Vec<(String, String)> = vars
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    v.push(("HOME".into(), home.display().to_string()));
    v.push((
        "OSTRO_TOOLCHAIN_DIR".into(),
        cache_dir().display().to_string(),
    ));
    move |k: &str| {
        v.iter()
            .find(|(n, _)| n == k)
            .map(|(_, x)| x.clone())
            .or_else(|| match k {
                "PATH" | "HTTPS_PROXY" | "https_proxy" | "HTTP_PROXY" | "http_proxy"
                | "NO_PROXY" | "no_proxy" | "SSL_CERT_FILE" => std::env::var(k).ok(),
                _ => None,
            })
    }
}

fn opts(dir: &Path) -> BuildOptions {
    BuildOptions {
        dir: dir.to_path_buf(),
        toolchain: Some(ToolchainMode::Hermetic),
        ..BuildOptions::default()
    }
}

fn build(dir: &Path, home: &Path, vars: &[(&str, &str)]) -> Result<BuildOutput, BuildError> {
    let host = host_with(home, vars);
    build_with(&opts(dir), &host, &step_for)
}

fn timed(what: &str, dir: &Path, home: &Path, vars: &[(&str, &str)]) -> BuildOutput {
    let t = Instant::now();
    let out = build(dir, home, vars).unwrap_or_else(|e| panic!("{what}: {e}"));
    eprintln!(
        "TIMING {what}: {:?} ({} bytes, sha256={}, up_to_date={})",
        t.elapsed(),
        out.bytes,
        out.sha256,
        out.up_to_date
    );
    out
}

fn template(lang: Lang, name: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join(name);
    ostro_build::templates::render(lang, name, &root, false).unwrap();
    let home = d.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    (d, root, home)
}

fn has(out: &BuildOutput, wf: &str) -> bool {
    out.workflows.iter().any(|(n, _)| n == wf)
}

fn tools(out: &BuildOutput) -> String {
    ostro_build::manifest::read(&out.manifest_path)
        .unwrap()
        .toolchains
        .tools
        .join(", ")
}

#[test]
fn python_template_builds() {
    let (_d, root, home) = template(Lang::Python, "py-tmpl");
    let out = timed("python template cold", &root, &home, &[]);
    assert!(has(&out, "hello"), "{:?}", out.workflows);
    let t = tools(&out);
    assert!(
        t.contains("uv@0.7.15")
            && t.contains("cpython@3.12 (uv-managed)")
            && t.contains("componentize-py@0.25.1 (wheel "),
        "{t}"
    );
    let warm = timed("python template up-to-date", &root, &home, &[]);
    assert!(warm.up_to_date);
    // A source edit rebuilds with the warm cache.
    let app = root.join("app.py");
    let text = std::fs::read_to_string(&app).unwrap();
    std::fs::write(&app, text.replace("hello {name}", "hi {name}")).unwrap();
    let rebuilt = timed("python template warm rebuild", &root, &home, &[]);
    assert!(!rebuilt.up_to_date && has(&rebuilt, "hello"));
}

#[test]
fn python_pure_dep() {
    let (_d, root, home) = template(Lang::Python, "py-dep");
    let py = root.join("pyproject.toml");
    let text = std::fs::read_to_string(&py).unwrap();
    std::fs::write(
        &py,
        text.replace("dependencies = []", "dependencies = [\"attrs==25.3.0\"]"),
    )
    .unwrap();
    // `import attrs` runs at componentize-py pre-init; the workflow name proves the
    // installed version is the one imported.
    let app = root.join("app.py");
    let text = std::fs::read_to_string(&app).unwrap();
    let text = text
        .replace("import ostro\n", "import attrs\n\nimport ostro\n")
        .replace(
            "ostro.export()",
            "@ostro.workflow(name=\"attrs-\" + attrs.__version__)\n\
         async def attrs_version(ctx: ostro.Ctx, inp: dict) -> str:\n    \
         return attrs.__version__\n\n\nostro.export()",
        );
    std::fs::write(&app, text).unwrap();
    let out = timed("python with attrs", &root, &home, &[]);
    assert!(
        has(&out, "hello") && has(&out, "attrs-25.3.0"),
        "{:?}",
        out.workflows
    );
    assert!(
        root.join("target/ostro/stage/.deps/attrs-25.3.0.dist-info/WHEEL")
            .is_file()
    );
}

#[test]
fn python_native_dep_rejected() {
    let (_d, root, home) = template(Lang::Python, "py-native");
    let py = root.join("pyproject.toml");
    let text = std::fs::read_to_string(&py).unwrap();
    std::fs::write(
        &py,
        text.replace(
            "dependencies = []",
            "dependencies = [\"pydantic-core==2.41.5\"]",
        ),
    )
    .unwrap();
    let err = build(&root, &home, &[]).unwrap_err().to_string();
    eprintln!("native dependency error: {err}");
    assert!(
        err.contains("pydantic_core is not a pure-Python wheel")
            && err.contains("native extensions are not supported"),
        "{err}"
    );
    assert!(!root.join("target/ostro/inputs.sha256").exists());
}

#[test]
fn python_offline_after_warm() {
    let (_d, root, home) = template(Lang::Python, "py-offline");
    timed("python offline: warm-up", &root, &home, &[]);
    let app = root.join("app.py");
    let text = std::fs::read_to_string(&app).unwrap();
    std::fs::write(&app, format!("{text}\n# edited\n")).unwrap();
    let out = timed(
        "python offline rebuild",
        &root,
        &home,
        &[("OSTRO_OFFLINE", "1")],
    );
    assert!(!out.up_to_date && has(&out, "hello"));
}
