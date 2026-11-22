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
//!
//! This file is Python-only so the TypeScript plan (09-15) never shares it. Run with
//! `--test-threads=2` at most (guest builds are serialised by `polyglot::build`).
#![cfg(feature = "polyglot")]

use ostro_conformance::engine_leg::{self, RunReport};
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::repo_root;
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
