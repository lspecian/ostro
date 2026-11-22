//! C-REBUILD-01 (plan 09-17, SDK-04 / D-P9-3): journals recorded on one build replay with 0
//! appended events and no NonDeterminism on an independently rebuilt module of the same
//! source. Needs `--features polyglot` (uv 0.7.15, Node 24); run with `--test-threads=1`.
//!
//! - **Build A** is what the conformance legs use: `langs::module_for` (the polyglot cache
//!   for Python/TS, `target/guests` for Rust). **Build B** is a fresh build of the same
//!   source: `build-guest.sh` into a new temp dir (Python, TS), cargo into the separate
//!   target dir `target/rebuild-rust` (Rust; must be byte-identical to A, D-P9-3).
//! - Every engine-leg scenario allowed for the language runs on build A
//!   (`engine_leg::run_spec`, with its own strict checks); each of its journals is then
//!   replayed on build B (base or `v2` module, whichever the run ended on): 0 appended
//!   events, no AttemptFailed. A `non_determinism` scenario (C-NDET-01) must stay
//!   NonDeterminism on build B.
//! - `py-setorder` (`conformance/fixtures/py-setorder`) iterates a `set` of 20 str into
//!   steps. Same step order on A and B ⇔ the str hash secret is pinned at pre-init (the
//!   D-P9-3 fork, `OSTRO_COMPONENTIZE_PY`; then equality is asserted). With the released
//!   componentize-py 0.25.1 the orders differ and the replay of A's journal on B must end
//!   in NonDeterminism, i.e. the hazard is detected, never silently accepted.
#![cfg(feature = "polyglot")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ostro_conformance::engine_leg::{self, DEFAULT_RUN_ID, T0};
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::polyglot::{self, Mode};
use ostro_conformance::repo_root;
use ostro_conformance::spec::{self, Leg, ScriptStep, Spec};
use ostro_engine::config::{EngineConfig, LanguageClass};
use ostro_engine::error::AttemptFailure;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{FakeClock, RunSpec, Script, drive_to_completion, replay_journal};
use ostro_types::journal::{Event, EventBody};
use sha2::{Digest, Sha256};

/// Partition the engine leg runs on.
const PARTITION: u16 = 1;

fn sha_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Replay runtime per language class: frozen clock at the engine leg's T0 and the engine
/// leg's token key, so replays see exactly what the recording saw.
fn runtime(class: LanguageClass) -> &'static Runtime {
    static RT: OnceLock<std::sync::Mutex<HashMap<String, &'static Runtime>>> = OnceLock::new();
    let map = RT.get_or_init(Default::default);
    let mut map = map.lock().unwrap_or_else(|p| p.into_inner());
    map.entry(format!("{class:?}")).or_insert_with(|| {
        Box::leak(Box::new(
            Runtime::new(
                EngineConfig {
                    class,
                    hot_wait: Duration::ZERO,
                    ..EngineConfig::default()
                },
                RuntimeOptions {
                    clock: Arc::new(FakeClock::with_step(T0, 0)),
                    token_key: [0x0c; 32],
                    ..RuntimeOptions::default()
                },
            )
            .unwrap_or_else(|e| panic!("rebuild runtime ({class:?}): {e}")),
        ))
    })
}

fn tokio_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Fresh polyglot build of `dir` through `build-guest.sh` into a new temp dir (build B).
fn fresh_polyglot(mode: Mode, dir: &Path) -> Vec<u8> {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = tmp.path().join("b.wasm");
    let script = repo_root().join("sdks/scripts/build-guest.sh");
    let res = Command::new("bash")
        .arg(&script)
        .arg(mode.arg())
        .arg(dir)
        .arg(&out)
        .output()
        .expect("spawn build-guest.sh");
    assert!(
        res.status.success(),
        "build-guest.sh {} {} failed:\n{}\n{}",
        mode.arg(),
        dir.display(),
        String::from_utf8_lossy(&res.stdout),
        String::from_utf8_lossy(&res.stderr)
    );
    std::fs::read(&out).expect("read build B")
}

/// Rust build B: conf-rust and conf-rust-v2 into `target/rebuild-rust`.
fn fresh_rust(variant: Option<&str>) -> Vec<u8> {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    let dir = BUILT.get_or_init(|| {
        let root = repo_root();
        let target_dir = root.join("target/rebuild-rust");
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
            .args([
                "build",
                "--release",
                "--locked",
                "--target",
                "wasm32-wasip2",
            ])
            .arg("--manifest-path")
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .args(["-p", "conf-rust", "-p", "conf-rust-v2"])
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("spawn cargo");
        assert!(
            out.status.success(),
            "conf-rust rebuild failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        target_dir.join("wasm32-wasip2/release")
    });
    let file = match variant {
        None => "conf_rust.wasm".to_string(),
        Some(v) => format!("conf_rust_{v}.wasm"),
    };
    std::fs::read(dir.join(file)).expect("read Rust build B")
}

fn build_b(lang: Lang, variant: Option<&str>) -> Vec<u8> {
    match lang {
        Lang::Rust => fresh_rust(variant),
        Lang::Py => fresh_polyglot(Mode::Py, &langs::guest_dir(lang, variant)),
        Lang::Ts => fresh_polyglot(Mode::Ts, &langs::guest_dir(lang, variant)),
        Lang::TsSm => fresh_polyglot(Mode::TsSm, &langs::guest_dir(lang, variant)),
    }
}

fn prepare(rt: &Runtime, wasm: &[u8]) -> Arc<Prepared> {
    tokio_rt()
        .block_on(rt.prepare_cached(wasm))
        .unwrap_or_else(|e| panic!("prepare: {e}"))
}

fn run_ref(spec: &Spec, generation: usize) -> RunRef {
    let base = spec.run_id.clone().unwrap_or_else(|| DEFAULT_RUN_ID.into());
    RunRef {
        partition: PARTITION,
        run_id: if generation == 0 {
            base
        } else {
            format!("{base}.{generation}")
        },
    }
}

fn allowed(id: &str, spec: &Spec) -> Vec<Lang> {
    spec.languages.clone().unwrap_or_else(|| {
        spec::catalogue()
            .get(id)
            .expect("catalogue id")
            .languages
            .clone()
    })
}

/// Module variant each journal of the chain ended on: the start variant, except that the
/// last run ends on the last `swap_module` variant. A swap inside a multi-run chain would
/// make that ambiguous, so it is rejected.
fn variants(spec: &Spec, runs: usize) -> Result<Vec<Option<String>>, String> {
    let swaps: Vec<&String> = spec
        .script
        .iter()
        .filter_map(|s| match s {
            ScriptStep::SwapModule { variant } => Some(variant),
            _ => None,
        })
        .collect();
    if !swaps.is_empty() && runs > 1 {
        return Err(format!(
            "{}: swap_module in a {runs}-run chain is not supported by the rebuild runner",
            spec.id
        ));
    }
    let start = spec.variant.clone();
    let end = swaps
        .last()
        .map(|v| Some((*v).clone()))
        .unwrap_or(start.clone());
    Ok((0..runs)
        .map(|i| {
            if i + 1 == runs {
                end.clone()
            } else {
                start.clone()
            }
        })
        .collect())
}

/// Records every engine scenario for `lang` on build A and replays it on build B.
fn rebuild_leg(lang: Lang) {
    let rt = runtime(langs::class(lang));
    let mut b_modules: HashMap<Option<String>, Arc<Prepared>> = HashMap::new();
    let mut shas = Vec::new();
    for variant in [None, Some("v2")] {
        let a = langs::module_for(lang, variant).unwrap_or_else(|e| panic!("build A: {e}"));
        let b = build_b(lang, variant);
        let (sa, sb) = (sha_hex(&a), sha_hex(&b));
        eprintln!(
            "{} {variant:?}: build A sha256={sa} ({} B), build B sha256={sb} ({} B), {}",
            langs::name(lang),
            a.len(),
            b.len(),
            if sa == sb { "identical" } else { "differ" }
        );
        if lang == Lang::Rust {
            assert_eq!(sa, sb, "Rust guests must be bit-reproducible (D-P9-3)");
        }
        shas.push((sa, sb));
        b_modules.insert(variant.map(str::to_owned), prepare(rt, &b));
    }

    let tokio = tokio_rt();
    let mut failures = Vec::new();
    let mut replayed = 0usize;
    let mut scenarios = 0usize;
    for entry in spec::scenarios(Leg::Engine) {
        if entry.is_pending() {
            failures.push(format!("{}: spec file missing", entry.id));
            continue;
        }
        let spec = spec::load_spec(&entry.id).unwrap_or_else(|e| panic!("{e}"));
        if !allowed(&entry.id, &spec).contains(&lang) {
            continue;
        }
        let report = match engine_leg::run_spec(&spec, lang) {
            Ok(r) => r,
            Err(f) => {
                failures.push(format!("{f}"));
                continue;
            }
        };
        let vs = match variants(&spec, report.journals.len()) {
            Ok(v) => v,
            Err(e) => {
                failures.push(e);
                continue;
            }
        };
        scenarios += 1;
        let last = report.journals.len() - 1;
        for (i, (journal, variant)) in report.journals.iter().zip(&vs).enumerate() {
            let module = b_modules.get(variant).expect("variant built");
            let run = run_ref(&spec, i);
            let (outcome, appended) =
                tokio.block_on(replay_journal(rt, module, run.clone(), journal.clone()));
            let want_nd = i == last && report.status == "non_determinism";
            let nd = matches!(
                outcome,
                ActivationOutcome::AttemptFailed(AttemptFailure::NonDeterminism(_))
            );
            if want_nd {
                if !nd {
                    failures.push(format!(
                        "{} {}: build A ended non_determinism, build B replay did not: {outcome:?}",
                        spec.id, run.run_id
                    ));
                }
            } else if appended != 0 || matches!(outcome, ActivationOutcome::AttemptFailed(_)) {
                failures.push(format!(
                    "{} {}: replay on build B appended {appended} events, outcome {outcome:?}",
                    spec.id, run.run_id
                ));
            }
            replayed += 1;
        }
    }
    assert!(
        failures.is_empty(),
        "C-REBUILD-01 {}: {} failure(s):\n{}",
        langs::name(lang),
        failures.len(),
        failures.join("\n")
    );
    assert!(
        scenarios > 0,
        "no engine scenario for {}",
        langs::name(lang)
    );
    eprintln!(
        "C-REBUILD-01 {}: {scenarios} scenarios, {replayed} journals from build A replay on build B \
         (0 appended, no non-determinism)",
        langs::name(lang)
    );
}

#[test]
fn rebuild_rust() {
    rebuild_leg(Lang::Rust);
}

#[test]
fn rebuild_python() {
    rebuild_leg(Lang::Py);
}

#[test]
fn rebuild_ts() {
    rebuild_leg(Lang::Ts);
}

fn steps(journal: &[Event]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.clone()),
            _ => None,
        })
        .collect()
}

fn drive_setorder(rt: &Runtime, p: &Prepared, run: &RunRef) -> Vec<Event> {
    let d = tokio_rt().block_on(drive_to_completion(
        rt,
        p,
        RunSpec::new(run.clone(), "setorder", b"null"),
        Script::new(),
    ));
    assert!(
        matches!(d.outcome, ActivationOutcome::Completed { .. }),
        "setorder: expected Completed, got {:?}",
        d.outcome
    );
    d.journal
}

#[test]
fn rebuild_python_setorder() {
    let dir = repo_root().join("conformance/fixtures/py-setorder");
    let a_path = polyglot::build(Mode::Py, &dir).unwrap_or_else(|e| panic!("build A: {e}"));
    let a = std::fs::read(&a_path).expect("read build A");
    let b = fresh_polyglot(Mode::Py, &dir);
    let rt = runtime(LanguageClass::Python);
    let (pa, pb) = (prepare(rt, &a), prepare(rt, &b));
    let run = RunRef {
        partition: PARTITION,
        run_id: "rebuild-setorder".into(),
    };
    let ja = drive_setorder(rt, &pa, &run);
    let jb = drive_setorder(rt, &pb, &run);
    let (oa, ob) = (steps(&ja), steps(&jb));
    assert_eq!(oa.len(), 20, "setorder issues 20 steps: {oa:?}");
    let pinned = std::env::var_os("OSTRO_COMPONENTIZE_PY").is_some_and(|v| !v.is_empty());
    eprintln!(
        "setorder: build A sha256={} order {oa:?}\nsetorder: build B sha256={} order {ob:?}\n\
         toolchain: {}",
        sha_hex(&a),
        sha_hex(&b),
        if pinned {
            "D-P9-3 fork (OSTRO_COMPONENTIZE_PY)"
        } else {
            "released componentize-py 0.25.1"
        }
    );
    let (outcome, appended) = tokio_rt().block_on(replay_journal(rt, &pb, run.clone(), ja));
    if oa == ob {
        assert!(
            appended == 0 && !matches!(outcome, ActivationOutcome::AttemptFailed(_)),
            "setorder: same order, yet replay on build B appended {appended}: {outcome:?}"
        );
        eprintln!("setorder: same step order on both builds; build A's journal replays on B");
    } else {
        assert!(
            !pinned,
            "setorder: the pinned toolchain must give the same set order on every build"
        );
        assert!(
            matches!(
                outcome,
                ActivationOutcome::AttemptFailed(AttemptFailure::NonDeterminism(_))
            ),
            "setorder: orders differ, so replaying build A's journal on build B must be \
             NonDeterminism, got {outcome:?} (appended {appended})"
        );
        eprintln!(
            "setorder: step order differs across builds (str hash secret not pinned by the \
             released toolchain); replay on B is detected as NonDeterminism"
        );
    }
}
