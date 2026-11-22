//! Python conformance (plan 09-14, SDK-05 / WF-07): the language-neutral suite for the
//! Python guests `conformance/guests/python[-v2]`, cross-compared with the Rust reference
//! (`guests/conf-rust[-v2]`). Needs `--features polyglot` (uv 0.7.15 + componentize-py).
//!
//! - `engine_leg_python`: every engine-leg scenario (`spec::scenarios(Leg::Engine)`) runs
//!   for `Lang::Rust` and `Lang::Py` with the runner's strict per-language checks (two
//!   executions byte-identical, every journal replays with 0 appended events, spec
//!   expectations). Python's status, canonical outcome and projection (facts, values,
//!   inputs) must equal Rust's (`engine_leg::cross_compare`); C-NATIVE-01 compares facts and
//!   inputs only, its values are per language (replay identity + seed sensitivity, WF-07).
//! - `server_leg_python_*`: every 09-13 server scenario on a real in-process server, all
//!   roles in Python, compared with Rust on status and root facts (results with run ids
//!   differ per run), then exactly one child run per (parent, op).
//!
//! This file is Python-only so the TypeScript plan (09-15) never shares it. Run with
//! `--test-threads=2` at most (guest builds are serialised by `polyglot::build`).
#![cfg(feature = "polyglot")]

use ostro_conformance::engine_leg::{self, Failure, RunReport};
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::repo_root;
use ostro_conformance::server_leg::ServerHarness;
use ostro_conformance::spec::{self, Leg, Spec};

const LANGS: [Lang; 2] = [Lang::Rust, Lang::Py];

fn load(id: &str, failures: &mut Vec<String>) -> Option<(Spec, Vec<Lang>)> {
    let entry = spec::catalogue().get(id).expect("catalogue id");
    if entry.is_pending() {
        failures.push(format!(
            "{id}: spec file {} is missing",
            entry.spec_path().display()
        ));
        return None;
    }
    match spec::load_spec(id) {
        Ok(s) => {
            let allowed = s
                .languages
                .clone()
                .unwrap_or_else(|| entry.languages.clone());
            Some((s, allowed))
        }
        Err(e) => {
            failures.push(format!("{id}: {e}"));
            None
        }
    }
}

fn finish(what: &str, failures: &[String], passed: usize) {
    assert!(
        failures.is_empty(),
        "{what}: {} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert!(passed > 0, "{what}: no Python scenario ran");
    eprintln!("{what}: {passed} Python scenario(s) pass and match Rust");
}

#[test]
fn engine_leg_python() {
    let ids: Vec<String> = spec::scenarios(Leg::Engine).map(|e| e.id.clone()).collect();
    assert_eq!(ids.len(), 27, "engine-leg scenario count");
    let mut failures = Vec::new();
    let mut passed = 0;
    for id in &ids {
        let Some((spec, allowed)) = load(id, &mut failures) else {
            continue;
        };
        assert!(allowed.contains(&Lang::Py), "{id} excludes Python");
        let mut reference: Option<RunReport> = None;
        for lang in LANGS {
            match engine_leg::run_spec(&spec, lang) {
                Ok(r) => {
                    eprintln!(
                        "PASS {} [{}] {} activations={} {:?}",
                        r.id,
                        langs::name(lang),
                        r.status,
                        r.activations,
                        r.elapsed
                    );
                    if let Some(rf) = &reference {
                        match engine_leg::cross_compare(rf, &r) {
                            Ok(()) => passed += 1,
                            Err(e) => failures.push(format!("cross-language: {e}")),
                        }
                    } else {
                        reference = Some(r);
                    }
                }
                Err(e) => {
                    failures.push(e.to_string());
                    if lang == Lang::Rust {
                        break;
                    }
                }
            }
        }
    }
    finish("engine leg", &failures, passed);
    assert_eq!(passed, ids.len(), "every engine scenario passes for Python");
}

/// 09-13 server families (same split as `tests/server_leg.rs`, one server each).
const FAMILIES: &[(&str, &[&str])] = &[
    (
        "children_basic",
        &[
            "C-CHILD-01",
            "C-CHILD-02",
            "C-CHILD-03",
            "C-CHILD-04",
            "C-CHILD-05",
        ],
    ),
    (
        "children_lifecycle",
        &["C-CHILD-06", "C-CHILD-07", "C-CHILD-09"],
    ),
    ("children_resume_shielding", &["C-CHILD-10", "C-CHILD-11"]),
    (
        "api_signals_tokens_can",
        &["C-SIG-API-01", "C-TOKEN-API-01", "C-CAN-02"],
    ),
    ("http_effect", &["C-FX-01"]),
];

/// Status and root facts against the Rust reference (as `tests/server_leg.rs`).
fn server_cross_compare(reference: &RunReport, other: &RunReport) -> Result<(), Failure> {
    let fail = |message: String| Failure {
        id: other.id.clone(),
        lang: other.lang,
        message,
    };
    if reference.status != other.status {
        return Err(fail(format!(
            "status {} vs reference {}",
            other.status, reference.status
        )));
    }
    ostro_conformance::compare::facts(&reference.projection.facts, &other.projection.facts)
        .map_err(|d| fail(d.to_string()))
}

async fn server_family(name: &str) {
    let ids = FAMILIES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, ids)| *ids)
        .expect("family");
    let h = ServerHarness::start().await.expect("server harness");
    let mut failures = Vec::new();
    let mut passed = 0;
    for id in ids {
        let Some((spec, allowed)) = load(id, &mut failures) else {
            continue;
        };
        assert!(allowed.contains(&Lang::Py), "{id} excludes Python");
        let mut reference: Option<RunReport> = None;
        for lang in LANGS {
            match h.run_spec(&spec, &[lang]).await {
                Ok(r) => {
                    eprintln!(
                        "PASS {} [{}] {} {:?}",
                        r.id,
                        langs::name(lang),
                        r.status,
                        r.elapsed
                    );
                    if let Some(rf) = &reference {
                        match server_cross_compare(rf, &r) {
                            Ok(()) => passed += 1,
                            Err(e) => failures.push(format!("cross-language: {e}")),
                        }
                    } else {
                        reference = Some(r);
                    }
                }
                Err(e) => {
                    failures.push(e.to_string());
                    if lang == Lang::Rust {
                        break;
                    }
                }
            }
        }
    }
    match h.check_children_unique().await {
        Ok(n) => eprintln!("{name}: exactly one child run per (parent, op) for {n} child starts"),
        Err(e) => failures.push(format!("one child per (parent, op): {e}")),
    }
    h.shutdown().await.expect("shutdown");
    finish(name, &failures, passed);
    assert_eq!(
        passed,
        ids.len(),
        "{name}: every scenario passes for Python"
    );
}

#[test]
fn server_leg_python_covers_every_09_13_scenario() {
    let owned: Vec<String> = spec::scenarios(Leg::Server)
        .filter(|e| e.owner == "09-13")
        .map(|e| e.id.clone())
        .collect();
    assert_eq!(owned.len(), 14, "09-13 server scenario count");
    for id in &owned {
        let n = FAMILIES
            .iter()
            .filter(|(_, ids)| ids.contains(&id.as_str()))
            .count();
        assert_eq!(n, 1, "{id} is in {n} families");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_python_children_basic() {
    server_family("children_basic").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_python_children_lifecycle() {
    server_family("children_lifecycle").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_python_children_resume_shielding() {
    server_family("children_resume_shielding").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_python_api_signals_tokens_can() {
    server_family("api_signals_tokens_can").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_python_http_effect() {
    server_family("http_effect").await;
}

/// `python-v2/app.py` is `python/app.py` with only the marked v1/v2 section (conf_patch,
/// conf_ndet) and the docstring's first line changed.
#[test]
fn engine_leg_python_v2_differs_only_in_patch_and_ndet() {
    let root = repo_root().join("conformance/guests");
    let read =
        |p: &str| std::fs::read_to_string(root.join(p)).unwrap_or_else(|e| panic!("read {p}: {e}"));
    let (v1, v2) = (read("python/app.py"), read("python-v2/app.py"));
    let split = |s: &str| -> (String, String) {
        let start = s
            .find("\n# --- v1/v2 bodies (")
            .expect("v1/v2 start marker");
        let end = s
            .find("\n# --- end of v1/v2 bodies")
            .expect("v1/v2 end marker");
        let head = s[..start].split_once('\n').map_or("", |(_, rest)| rest);
        (head.to_string(), s[end..].to_string())
    };
    assert_eq!(
        split(&v1),
        split(&v2),
        "python-v2/app.py drifted from python/app.py"
    );
    for w in ["py_conf_patch", "py_conf_ndet"] {
        assert!(v2.contains(&format!("name=\"{w}\"")), "{w} missing in v2");
    }
}
