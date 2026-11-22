//! 10-15: `ostro build --docker` against a recording docker shim (`OSTRO_DOCKER`), so the
//! default suite needs no Docker. The shim logs every argv, fails `image inspect` until
//! `build` has run (which saves the Dockerfile read from stdin), and for `run` writes a
//! prebuilt component (`guests/smoke`, built once into `target/guests`) plus the JSON
//! result into the directory mounted at `/src`.
//!
//! `real_docker_python_build` (`--features polyglot`, and only with `OSTRO_TEST_DOCKER=1`
//! plus `OSTRO_LINUX_BIN=<a Linux ostro binary>`) runs the real image: it builds the
//! Python template in the container with the shared toolchain cache. Without them it
//! prints why it is skipped and passes (10-19's CI job runs real Docker).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use ostro_build::lang::docker::{DOCKERFILE, image_tag};
use ostro_build::pipeline::{BuildOptions, build_with, step_for};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `guests/smoke` built once per process (`target/guests`, as tests/pipeline.rs does).
fn smoke_wasm() -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
            .args(["build", "--release", "--target", "wasm32-wasip2"])
            .args(["-p", "smoke", "--manifest-path"])
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("spawn cargo for the smoke guest");
        assert!(
            out.status.success(),
            "smoke guest build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    });
    target_dir.join("wasm32-wasip2/release/smoke.wasm")
}

const SHIM: &str = r#"#!/bin/sh
# recording docker shim (10-15 docker_shim.rs)
STATE='@STATE@'
printf '%s\n' "$*" >> "$STATE/argv.log"
case "$1" in
    image)
        [ -f "$STATE/built" ] && exit 0
        exit 1 ;;
    build)
        cat > "$STATE/Dockerfile.stdin"
        touch "$STATE/built"
        echo "shim: built image" >&2
        exit 0 ;;
    run)
        src=''
        prev=''
        for a in "$@"; do
            if [ "$prev" = "-v" ]; then
                case "$a" in
                    *:/src) src="${a%:/src}" ;;
                esac
            fi
            prev="$a"
        done
        [ -n "$src" ] || { echo "shim: no /src mount" >&2; exit 3; }
        mkdir -p "$src/target/ostro"
        cp '@WASM@' "$src/target/ostro/dock.wasm"
        echo "shim: ostro build --in-container" >&2
        printf '{\n  "kind": "ostro.build.result",\n  "version": 1,\n  "wasm": "/src/target/ostro/dock.wasm"\n}\n'
        exit 0 ;;
esac
echo "shim: unexpected $*" >&2
exit 2
"#;

struct Env {
    _d: tempfile::TempDir,
    project: PathBuf,
    state: PathBuf,
    shim: PathBuf,
    bin: PathBuf,
    cache: PathBuf,
    home: PathBuf,
}

fn setup() -> Env {
    use std::os::unix::fs::PermissionsExt as _;
    let d = tempfile::tempdir().unwrap();
    let project = d.path().join("dock");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(
        project.join("ostro.toml"),
        "[project]\nname = \"dock\"\nlang = \"python\"\n",
    )
    .unwrap();
    std::fs::write(project.join("app.py"), "x = 1\n").unwrap();
    let state = d.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let shim = d.path().join("docker-shim");
    std::fs::write(
        &shim,
        SHIM.replace("@STATE@", &state.display().to_string())
            .replace("@WASM@", &smoke_wasm().display().to_string()),
    )
    .unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    // A stand-in for the static musl ostro (not ELF: no PT_INTERP warning).
    let bin = d.path().join("ostro-linux");
    std::fs::write(&bin, "static ostro stand-in\n").unwrap();
    let cache = d.path().join("cache");
    let home = d.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    Env {
        _d: d,
        project,
        state,
        shim,
        bin,
        cache,
        home,
    }
}

fn host<'a>(e: &'a Env, docker: &'a Path) -> impl Fn(&str) -> Option<String> + Sync + 'a {
    move |k: &str| match k {
        "OSTRO_DOCKER" => Some(docker.display().to_string()),
        "OSTRO_LINUX_BIN" => Some(e.bin.display().to_string()),
        "OSTRO_TOOLCHAIN_DIR" => Some(e.cache.display().to_string()),
        "HOME" => Some(e.home.display().to_string()),
        "PATH" => std::env::var(k).ok(),
        _ => None,
    }
}

fn docker_opts(dir: &Path) -> BuildOptions {
    BuildOptions {
        dir: dir.to_path_buf(),
        docker: true,
        ..BuildOptions::default()
    }
}

#[test]
fn docker_mode_builds_the_image_once_and_runs_the_container() {
    let e = setup();
    let h = host(&e, &e.shim);
    let out = build_with(&docker_opts(&e.project), &h, &step_for).unwrap_or_else(|x| panic!("{x}"));
    assert!(!out.up_to_date);
    let tag = image_tag();
    let log = std::fs::read_to_string(e.state.join("argv.log")).unwrap();
    let lines: Vec<&str> = log.lines().collect();
    eprintln!("shim argv:\n{log}");
    assert_eq!(lines[0], format!("image inspect {tag}"));
    assert_eq!(lines[1], format!("build -t {tag} -"));
    let run = lines[2];
    assert!(run.starts_with("run --rm --user "), "{run}");
    let user = run.split_whitespace().nth(3).unwrap();
    assert!(
        user.split(':').count() == 2 && user.split(':').all(|p| p.parse::<u32>().is_ok()),
        "{user}"
    );
    let cache = std::path::absolute(&e.cache).unwrap();
    for want in [
        format!("-v {}:/usr/local/bin/ostro:ro", e.bin.display()),
        format!("-v {}:/src ", e.project.display()),
        format!("-v {}:/cache ", cache.display()),
        "-e HOME=/tmp".to_owned(),
        format!("{tag} ostro build /src --in-container --json"),
    ] {
        assert!(run.contains(&want), "missing {want:?} in {run}");
    }
    let dockerfile = std::fs::read_to_string(e.state.join("Dockerfile.stdin")).unwrap();
    assert_eq!(dockerfile, DOCKERFILE);
    assert!(dockerfile.contains("FROM debian:12-slim@sha256:"));
    let m = ostro_build::manifest::read(&out.manifest_path).unwrap();
    assert_eq!(m.toolchains.tools, vec![format!("docker ({tag})")]);
    assert_eq!(
        std::fs::read(&out.wasm_path).unwrap(),
        std::fs::read(smoke_wasm()).unwrap()
    );

    // A source edit rebuilds through the container again; the image is not rebuilt.
    std::fs::write(e.project.join("app.py"), "x = 2\n").unwrap();
    let again = build_with(&docker_opts(&e.project), &h, &step_for).unwrap();
    assert!(!again.up_to_date);
    let log = std::fs::read_to_string(e.state.join("argv.log")).unwrap();
    assert_eq!(log.lines().filter(|l| l.starts_with("build ")).count(), 1);
    assert_eq!(log.lines().filter(|l| l.starts_with("run ")).count(), 2);
    assert_eq!(
        log.lines()
            .filter(|l| l.starts_with("image inspect"))
            .count(),
        2
    );
}

#[test]
fn offline_and_mirror_reach_the_container() {
    let e = setup();
    let base = host(&e, &e.shim);
    let h = |k: &str| match k {
        "OSTRO_OFFLINE" => Some("1".to_owned()),
        "OSTRO_TOOLCHAIN_MIRROR" => Some("https://mirror.example".to_owned()),
        _ => base(k),
    };
    build_with(&docker_opts(&e.project), &h, &step_for).unwrap_or_else(|x| panic!("{x}"));
    let log = std::fs::read_to_string(e.state.join("argv.log")).unwrap();
    let run = log.lines().find(|l| l.starts_with("run ")).unwrap();
    assert!(run.contains("-e OSTRO_OFFLINE=1"), "{run}");
    assert!(
        run.contains("-e OSTRO_TOOLCHAIN_MIRROR=https://mirror.example"),
        "{run}"
    );
    assert!(run.ends_with("--in-container --json --offline"), "{run}");
}

#[test]
fn missing_docker_is_a_clear_error() {
    let e = setup();
    let missing = e.state.join("no-such-docker");
    let h = host(&e, &missing);
    let err = build_with(&docker_opts(&e.project), &h, &step_for)
        .unwrap_err()
        .to_string();
    assert!(err.contains("--docker needs docker"), "{err}");
    assert!(err.contains("no-such-docker"), "{err}");
    assert!(!e.project.join("target/ostro/inputs.sha256").exists());
}

#[cfg(feature = "polyglot")]
#[test]
fn real_docker_python_build() {
    let enabled = std::env::var("OSTRO_TEST_DOCKER").is_ok_and(|v| v == "1");
    let bin = std::env::var("OSTRO_LINUX_BIN")
        .ok()
        .filter(|v| !v.is_empty());
    let docker = ostro_build::pipeline::which_in(&|k: &str| std::env::var(k).ok(), "docker");
    let (Some(bin), Some(docker), true) = (bin, docker, enabled) else {
        eprintln!(
            "SKIP real_docker_python_build: needs OSTRO_TEST_DOCKER=1, OSTRO_LINUX_BIN and docker \
             on PATH (10-19's CI job runs real Docker)"
        );
        return;
    };
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join("dock-py");
    ostro_build::templates::render(ostro_build::Lang::Python, "dock-py", &root, false).unwrap();
    let cache = repo_root().join("target/ostro-toolchains-test");
    let (cache_s, bin_s, docker_s) = (
        cache.display().to_string(),
        bin.clone(),
        docker.display().to_string(),
    );
    let h = move |k: &str| match k {
        "OSTRO_DOCKER" => Some(docker_s.clone()),
        "OSTRO_LINUX_BIN" => Some(bin_s.clone()),
        "OSTRO_TOOLCHAIN_DIR" => Some(cache_s.clone()),
        _ => std::env::var(k).ok(),
    };
    let t = std::time::Instant::now();
    let out = build_with(&docker_opts(&root), &h, &step_for).unwrap_or_else(|x| panic!("{x}"));
    eprintln!(
        "TIMING docker python template: {:?} ({} bytes, sha256={})",
        t.elapsed(),
        out.bytes,
        out.sha256
    );
    assert!(
        out.workflows.iter().any(|(n, _)| n == "hello"),
        "{:?}",
        out.workflows
    );
}
