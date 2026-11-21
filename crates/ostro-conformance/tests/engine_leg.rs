//! Engine leg (09-11, SDK-05): every engine-leg scenario of the catalogue, run for each
//! language selected by `OSTRO_CONF_LANGS` (default `rust`) on ostro-engine with the
//! harness as scheduler (`ostro_conformance::engine_leg`).
//!
//! One `#[test]` per scenario family keeps failures readable. Each scenario is checked
//! strictly per language (two executions byte-identical, replay appends 0 events) and
//! against its spec; with several languages the others are also compared with the Rust
//! reference (canonical outcome + normalised projection). Run with `--test-threads=2` at
//! most: scenarios serialise on the shared clock anyway.

use ostro_conformance::engine_leg::{self, RunReport};
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::spec::{self, CatalogueEntry, Leg};

/// Scenario families (id prefixes); every engine scenario must belong to exactly one.
const FAMILIES: &[&str] = &[
    "C-STEP-",
    "C-NATIVE-",
    "C-RETRY-",
    "C-TIMER-",
    "C-SIG-",
    "C-RACE-",
    "C-CANCEL-",
    "C-PATCH-",
    "C-NDET-",
    "C-RAND-",
    "C-VAL-",
    "C-CAN-",
    "C-TOKEN-",
    "C-STATUS-",
    "C-SAGA-",
];

fn entries(prefix: &str) -> Vec<&'static CatalogueEntry> {
    spec::scenarios(Leg::Engine)
        .filter(|e| e.id.starts_with(prefix))
        .collect()
}

/// Runs a family for the selected languages; panics with every failure listed.
fn family(prefix: &str) -> Vec<RunReport> {
    let selected = langs::selected();
    let list = entries(prefix);
    assert!(!list.is_empty(), "no engine scenarios with prefix {prefix}");
    let mut failures = Vec::new();
    let mut reports = Vec::new();
    for entry in list {
        if entry.is_pending() {
            failures.push(format!(
                "{}: spec file {} is missing",
                entry.id,
                entry.spec_path().display()
            ));
            continue;
        }
        let spec = match spec::load_spec(&entry.id) {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("{}: {e}", entry.id));
                continue;
            }
        };
        let allowed: Vec<Lang> = spec
            .languages
            .clone()
            .unwrap_or_else(|| entry.languages.clone());
        let mut reference: Option<RunReport> = None;
        for &lang in selected.iter().filter(|l| allowed.contains(l)) {
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
                    if lang == Lang::Rust {
                        reference = Some(r.clone());
                    } else if let Some(rf) = &reference
                        && let Err(e) = engine_leg::cross_compare(rf, &r)
                    {
                        failures.push(format!("cross-language: {e}"));
                    }
                    reports.push(r);
                }
                Err(e) => failures.push(e.to_string()),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    reports
}

fn report<'a>(reports: &'a [RunReport], id: &str) -> Option<&'a RunReport> {
    reports.iter().find(|r| r.id == id && r.lang == Lang::Rust)
}

#[test]
fn every_engine_scenario_has_one_family_and_owner_09_11() {
    let all: Vec<&CatalogueEntry> = spec::scenarios(Leg::Engine).collect();
    assert_eq!(all.len(), 27, "engine-leg scenario count");
    for e in all {
        let n = FAMILIES.iter().filter(|f| e.id.starts_with(*f)).count();
        assert_eq!(n, 1, "{} matches {n} families", e.id);
        assert_eq!(e.owner, "09-11", "{}", e.id);
        assert!(!e.is_pending(), "{} has no spec file", e.id);
    }
}

#[test]
fn steps() {
    family("C-STEP-");
}

#[test]
fn native() {
    family("C-NATIVE-");
}

#[test]
fn retries() {
    let r = family("C-RETRY-");
    if let Some(r) = report(&r, "C-RETRY-01") {
        // Two requeued backoffs: attempt 1 + 2 failed, attempt 3 succeeded.
        assert!(r.projection.facts.contains(&"retry:flaky#2".to_string()));
    }
}

#[test]
fn timers() {
    family("C-TIMER-");
}

#[test]
fn signals() {
    family("C-SIG-");
}

#[test]
fn races() {
    family("C-RACE-");
}

#[test]
fn cancellation() {
    family("C-CANCEL-");
}

#[test]
fn patching() {
    let r = family("C-PATCH-");
    if let (Some(old), Some(new)) = (report(&r, "C-PATCH-01"), report(&r, "C-PATCH-02")) {
        assert!(!old.projection.facts.iter().any(|f| f.starts_with("patch:")));
        assert!(new.projection.facts.contains(&"patch:x".to_string()));
    }
}

#[test]
fn nondeterminism() {
    let r = family("C-NDET-");
    if let Some(r) = report(&r, "C-NDET-01") {
        assert_eq!(r.status, "non_determinism");
    }
}

#[test]
fn randomness() {
    let r = family("C-RAND-");
    // C-RAND-*: a second seed changes the result (asserted by run_spec through
    // `differs_across_seeds`, required here) and replay reproduces it (strict check).
    for id in ["C-RAND-01", "C-RAND-02"] {
        let s = spec::load_spec(id).expect("spec");
        assert_eq!(
            s.expect.extra.get("differs_across_seeds"),
            Some(&serde_json::Value::Bool(true)),
            "{id}"
        );
        if let Some(rep) = report(&r, id) {
            assert!(rep.result.is_some(), "{id}");
        }
    }
}

#[test]
fn values() {
    let r = family("C-VAL-");
    if let Some(rep) = report(&r, "C-VAL-01") {
        let key = rep
            .result
            .as_ref()
            .and_then(|v| v["idempotency_key"].as_str())
            .unwrap_or("");
        assert_eq!(key.len(), 38, "{key}");
        assert!(key.starts_with("ostro-"));
    }
}

#[test]
fn continue_as_new() {
    let r = family("C-CAN-");
    if let Some(rep) = report(&r, "C-CAN-01") {
        assert!(rep.journals.len() > 1, "a chain of runs");
    }
}

#[test]
fn tokens() {
    family("C-TOKEN-");
}

#[test]
fn status() {
    family("C-STATUS-");
}

#[test]
fn sagas() {
    family("C-SAGA-");
}
