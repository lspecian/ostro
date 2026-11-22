//! 10-15 parity (D-P10-16): `ostro build` (hermetic) against Phase 9's reference recipe
//! `sdks/scripts/build-guest.sh` (through `ostro_conformance::polyglot::build`).
//!
//! - `build_guest_flags_match`: the componentize-py and jco argument templates of the
//!   Python and TS steps are token-for-token build-guest.sh's (placeholders normalised),
//!   and build-guest.sh's deterministic environment is a subset of the pipeline's
//!   scrubbed environment.
//! - `py_min_parity` / `ts_min_parity`: the raw fixtures `conformance/fixtures/py-min` and
//!   `ts-min`, copied into a temp project with a generated `ostro.toml`, built both ways:
//!   identical import and export sets. TS shas are equal, or differ within the residual
//!   recorded in docs/sdk-builds.md (`RESIDUAL-ACCEPTED: ts` + the recorded length spread);
//!   Python shas are compared and printed (docs/sdk-builds.md records the Python wrapper
//!   as not reproducible, `RESIDUAL-ACCEPTED: py`).
//!
//! `--features polyglot`; build-guest.sh needs uv 0.7.15 and Node 24 on `PATH` (as the
//! Phase 9 polyglot job), `ostro build` uses the shared `target/ostro-toolchains-test`
//! cache.
#![cfg(feature = "polyglot")]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use ostro_build::lang::{python::PythonStep, ts::TsStep};
use ostro_build::pipeline::{self, BuildOptions, build_with, step_for};
use ostro_build::project::ToolchainMode;
use ostro_conformance::polyglot::{self, Mode};
use ostro_conformance::{inspect, repo_root};

fn read(rel: &str) -> String {
    let p = repo_root().join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The tokens of the command in build-guest.sh's function `func` that starts at the token
/// `first` and ends with `-o "$OUT")`.
fn guest_tokens(script: &str, func: &str, first: &str) -> Vec<String> {
    let start = script
        .find(&format!("{func}() {{"))
        .unwrap_or_else(|| panic!("no {func} in build-guest.sh"));
    let body = &script[start..];
    let body = &body[..body.find("\n}").expect("function end")];
    let joined = body.replace("\\\n", " ");
    let line = joined
        .lines()
        .find(|l| l.contains(first) && l.contains("-o \"$OUT\""))
        .unwrap_or_else(|| panic!("no {first} command in {func}"));
    let toks: Vec<&str> = line.split_whitespace().collect();
    let i = toks.iter().position(|t| *t == first).expect("first token");
    toks[i..]
        .iter()
        .map(|t| t.trim_end_matches(')').to_owned())
        .collect()
}

/// Placeholders (`<…>`, shell variables, build-guest.sh's fixed entry `app`) become `*`.
fn normalise(toks: &[String]) -> Vec<String> {
    toks.iter()
        .map(|t| {
            if t.contains('$') || (t.starts_with('<') && t.ends_with('>')) || t == "app" {
                "*".to_owned()
            } else {
                t.clone()
            }
        })
        .collect()
}

fn flags(toks: &[String]) -> BTreeSet<String> {
    toks.iter()
        .filter(|t| t.starts_with('-'))
        .cloned()
        .collect()
}

#[test]
fn build_guest_flags_match() {
    let script = read("sdks/scripts/build-guest.sh");
    // py: `"${cpy[@]}" -d "$WIT" -w … componentize --stub-wasi -p "$STAGE/src" app -o "$OUT"`
    let py_ref = guest_tokens(&script, "build_py", "-d");
    let py_ours = PythonStep::command_line();
    eprintln!("build-guest.sh py: {py_ref:?}\nostro build py:    {py_ours:?}");
    assert_eq!(normalise(&py_ref), normalise(&py_ours));
    assert_eq!(flags(&py_ref), flags(&py_ours));
    // ts: `jco componentize "$STAGE/src/index.ts" --wit "$WIT" … -o "$OUT"`
    let ts_ref = guest_tokens(&script, "build_ts", "componentize");
    let ts_ours = TsStep::command_line();
    eprintln!("build-guest.sh ts: {ts_ref:?}\nostro build ts:    {ts_ours:?}");
    assert_eq!(normalise(&ts_ref), normalise(&ts_ours));
    assert_eq!(flags(&ts_ref), flags(&ts_ours));

    // `export PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH`
    let export = script
        .lines()
        .find(|l| l.starts_with("export ") && l.contains("PYTHONHASHSEED"))
        .expect("the deterministic export line");
    let host = |k: &str| (k == "HOME").then(|| "/home/x".to_owned());
    let env = pipeline::scrubbed_env(&host, 1_700_000_000, Path::new("/fallback"));
    for item in export.split_whitespace().skip(1) {
        match item.split_once('=') {
            Some((k, v)) => assert_eq!(env.get(k).map(String::as_str), Some(v), "{k}"),
            None => assert!(env.contains_key(item), "{item}"),
        }
    }
    eprintln!("build-guest.sh env: {export}\nostro build env:    {env:?}");
}

fn toolchain_dir() -> PathBuf {
    repo_root().join("target/ostro-toolchains-test")
}

/// Copy `fixture` into a temp project with a generated ostro.toml.
fn project(fixture: &str, lang: &str, entry: &str) -> (tempfile::TempDir, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let root = d.path().join(fixture);
    std::fs::create_dir_all(&root).unwrap();
    let from = repo_root().join("conformance/fixtures").join(fixture);
    for e in std::fs::read_dir(&from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() {
            std::fs::copy(e.path(), root.join(e.file_name())).unwrap();
        }
    }
    std::fs::write(
        root.join("ostro.toml"),
        format!("[project]\nname = \"{fixture}\"\nlang = \"{lang}\"\nentry = \"{entry}\"\n"),
    )
    .unwrap();
    (d, root)
}

struct Built {
    sha: String,
    bytes: usize,
    imports: BTreeSet<String>,
    exports: BTreeSet<String>,
}

fn built(bytes: &[u8]) -> Built {
    let info = inspect(bytes).expect("a component");
    Built {
        sha: pipeline::sha256_hex(bytes),
        bytes: bytes.len(),
        imports: info.imports.into_iter().collect(),
        exports: info.exports.into_iter().collect(),
    }
}

fn both(fixture: &str, lang: &str, entry: &str, mode: Mode) -> (Built, Built) {
    let (d, root) = project(fixture, lang, entry);
    let home = d.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let tc = toolchain_dir().display().to_string();
    let home_s = home.display().to_string();
    let host = move |k: &str| match k {
        "OSTRO_TOOLCHAIN_DIR" => Some(tc.clone()),
        "HOME" => Some(home_s.clone()),
        "PATH" | "HTTPS_PROXY" | "https_proxy" | "HTTP_PROXY" | "http_proxy" | "NO_PROXY"
        | "no_proxy" | "SSL_CERT_FILE" => std::env::var(k).ok(),
        _ => None,
    };
    let opts = BuildOptions {
        dir: root.clone(),
        toolchain: Some(ToolchainMode::Hermetic),
        ..BuildOptions::default()
    };
    let ours = build_with(&opts, &host, &step_for).unwrap_or_else(|e| panic!("ostro build: {e}"));
    let ours = built(&std::fs::read(&ours.wasm_path).unwrap());
    let reference_dir = repo_root().join("conformance/fixtures").join(fixture);
    let reference = polyglot::build(mode, &reference_dir).expect("build-guest.sh");
    let reference = built(&std::fs::read(&reference).unwrap());
    eprintln!(
        "PARITY {fixture}: ostro build sha256={} bytes={} | build-guest.sh sha256={} bytes={}",
        ours.sha, ours.bytes, reference.sha, reference.bytes
    );
    assert_eq!(ours.imports, reference.imports, "{fixture}: imports");
    assert_eq!(ours.exports, reference.exports, "{fixture}: exports");
    (ours, reference)
}

/// The TS (QuickJS) residual recorded in docs/sdk-builds.md: the length spread of the
/// `lengths A–B B` cell of the TypeScript (QuickJS) row, when `RESIDUAL-ACCEPTED: ts` is
/// present.
fn ts_residual_bytes() -> Option<usize> {
    let doc = read("docs/sdk-builds.md");
    if !doc.lines().any(|l| l.trim() == "RESIDUAL-ACCEPTED: ts") {
        return None;
    }
    let row = doc
        .lines()
        .find(|l| l.starts_with("| TypeScript (QuickJS)"))?;
    let cell = &row[row.find("lengths ")? + "lengths ".len()..];
    let range = cell.split(" B").next()?;
    let (a, b) = range.split_once('–')?;
    let n = |s: &str| s.replace(',', "").trim().parse::<usize>().ok();
    Some(n(b)?.abs_diff(n(a)?))
}

#[test]
fn ts_min_parity() {
    let (ours, reference) = both("ts-min", "ts", "index.ts", Mode::Ts);
    if ours.sha != reference.sha {
        let residual = ts_residual_bytes()
            .expect("TS shas differ and docs/sdk-builds.md records no accepted TS residual");
        let delta = ours.bytes.abs_diff(reference.bytes);
        eprintln!("ts-min: shas differ, length delta {delta} B (recorded residual {residual} B)");
        assert!(
            delta <= residual,
            "ts-min: length delta {delta} B exceeds the recorded residual {residual} B"
        );
    }
}

#[test]
fn py_min_parity() {
    let (ours, reference) = both("py-min", "python", "app", Mode::Py);
    let doc = read("docs/sdk-builds.md");
    let py_residual = doc.lines().any(|l| l.trim() == "RESIDUAL-ACCEPTED: py");
    eprintln!(
        "py-min: shas {} (docs/sdk-builds.md RESIDUAL-ACCEPTED: py = {py_residual}), \
         length delta {} B",
        if ours.sha == reference.sha {
            "equal"
        } else {
            "differ"
        },
        ours.bytes.abs_diff(reference.bytes)
    );
    if !py_residual {
        assert_eq!(
            ours.sha, reference.sha,
            "py-min: the Python wrapper is reproducible"
        );
    }
}
