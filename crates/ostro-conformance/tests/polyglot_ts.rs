//! TypeScript conformance (plan 09-15, SDK-05 / WF-07; `--features polyglot`, needs
//! Node 24): the language-neutral suite run on `conformance/guests/ts[-v2]` (jco QuickJS,
//! plus the StarlingMonkey smoke subset), cross-compared with the Rust reference.
//!
//! - `engine_leg_ts`: every engine-leg scenario (`spec::scenarios(Leg::Engine)`) for
//!   `Lang::Ts` with the runner's strict per-language checks (two executions byte-identical,
//!   every journal replays with 0 appended events, `differs_across_seeds` where the spec
//!   asks) and the spec expectations; then the canonical outcome and the normalised
//!   projection are compared with `Lang::Rust` (`engine_leg::cross_compare`; C-NATIVE-01
//!   compares status and facts only, its values are per language).
//! - `engine_leg_ts_sm_smoke`: C-STEP-01, C-RACE-01, C-SIG-01, C-RAND-01, C-CANCEL-01 on the
//!   StarlingMonkey build (`Lang::TsSm`, D-P9-1), also compared with Rust.
//! - `server_leg_ts_*`: every 09-13 server scenario with all roles in TypeScript on a real
//!   in-process `ostro-server` (`ServerHarness::run_spec`), compared with the Rust run on the
//!   same server (status + root facts; results carry run ids), ending with the
//!   one-child-run-per-(parent, op) check.
//!
//! The file is TypeScript-only so the Python plan (09-14) never shares it. Run with
//! `--test-threads=2` at most: engine scenarios serialise on the harness clock and guest
//! builds serialise in `polyglot::build`.
#![cfg(feature = "polyglot")]

use std::time::{Duration, Instant};

use ostro_conformance::engine_leg::{self, Failure, RunReport};
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::server_leg::ServerHarness;
use ostro_conformance::spec::{self, Leg, Spec};

/// D-P9-1 smoke subset for the StarlingMonkey fallback backend.
const SM_SMOKE: &[&str] = &[
    "C-STEP-01",
    "C-RACE-01",
    "C-SIG-01",
    "C-RAND-01",
    "C-CANCEL-01",
];

fn load(id: &str) -> Result<Spec, String> {
    let entry = spec::catalogue()
        .get(id)
        .ok_or_else(|| format!("{id}: not in the catalogue"))?;
    if entry.is_pending() {
        return Err(format!(
            "{id}: spec file {} is missing",
            entry.spec_path().display()
        ));
    }
    spec::load_spec(id).map_err(|e| format!("{id}: {e}"))
}

fn allowed(spec: &Spec) -> Vec<Lang> {
    spec.languages.clone().unwrap_or_else(|| {
        spec::catalogue()
            .get(&spec.id)
            .map(|e| e.languages.clone())
            .unwrap_or_default()
    })
}

fn line(r: &RunReport) -> String {
    format!(
        "PASS {} [{}] {} activations={} {:.1?}",
        r.id,
        langs::name(r.lang),
        r.status,
        r.activations,
        r.elapsed
    )
}

/// Runs `ids` on the engine leg for Rust and `lang`, cross-comparing `lang` with Rust.
fn engine_leg(ids: &[String], lang: Lang) -> Vec<RunReport> {
    let started = Instant::now();
    let mut failures = Vec::new();
    let mut reports = Vec::new();
    for id in ids {
        let spec = match load(id) {
            Ok(s) => s,
            Err(e) => {
                failures.push(e);
                continue;
            }
        };
        let langs_ok = allowed(&spec);
        if !langs_ok.contains(&lang) {
            failures.push(format!(
                "{id}: the catalogue does not list {} for this scenario",
                langs::name(lang)
            ));
            continue;
        }
        let reference = match engine_leg::run_spec(&spec, Lang::Rust) {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("rust reference: {e}"));
                continue;
            }
        };
        match engine_leg::run_spec(&spec, lang) {
            Ok(r) => {
                eprintln!("{}", line(&r));
                if let Err(e) = engine_leg::cross_compare(&reference, &r) {
                    failures.push(format!("cross-language vs rust: {e}"));
                }
                reports.push(r);
            }
            Err(e) => failures.push(e.to_string()),
        }
    }
    eprintln!(
        "engine leg [{}]: {} of {} scenario(s) passed in {:.1?}",
        langs::name(lang),
        reports.len(),
        ids.len(),
        started.elapsed()
    );
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    reports
}

#[test]
fn engine_leg_ts() {
    let ids: Vec<String> = spec::scenarios(Leg::Engine).map(|e| e.id.clone()).collect();
    assert_eq!(ids.len(), 27, "engine-leg scenario count");
    let reports = engine_leg(&ids, Lang::Ts);
    assert_eq!(reports.len(), 27);

    // C-STEP-04 (step exclusivity, B1): the sibling's timer lands after the step and its
    // ctx draw is the first draw of the ctx stream, equal to Rust's (checked by
    // cross_compare; spelled out here because it is the point of the scenario).
    let s4 = reports
        .iter()
        .find(|r| r.id == "C-STEP-04")
        .expect("C-STEP-04");
    assert_eq!(
        s4.projection.facts,
        ["step:s", "timer#1", "terminal:completed"]
    );
    assert_eq!(
        s4.result.as_ref().and_then(|v| v["sibling_rand"].as_str()),
        Some("16357943517170225811")
    );

    // C-NATIVE-01 (WF-07): native Date.now / Math.random / Map iteration observed outside
    // and inside a step; replay identity is the runner's strict check and
    // `differs_across_seeds` re-ran with another seed.
    let native = reports
        .iter()
        .find(|r| r.id == "C-NATIVE-01")
        .expect("C-NATIVE-01");
    let v = native.result.as_ref().expect("C-NATIVE-01 result");
    for side in ["outside", "inside"] {
        let o = &v[side];
        assert!(o["time_ms"].is_u64(), "{side}: {o}");
        assert!(o["random"].is_f64(), "{side}: {o}");
        assert_eq!(o["map_order"].as_array().map(Vec::len), Some(16), "{side}");
    }
    // Inside the step Math.random draws from the step-local stream.
    assert_ne!(v["outside"]["random"], v["inside"]["random"]);
    let s = spec::load_spec("C-NATIVE-01").expect("spec");
    assert_eq!(
        s.expect.extra.get("differs_across_seeds"),
        Some(&serde_json::Value::Bool(true))
    );
}

#[test]
fn engine_leg_ts_sm_smoke() {
    let ids: Vec<String> = SM_SMOKE.iter().map(|s| (*s).to_string()).collect();
    let reports = engine_leg(&ids, Lang::TsSm);
    assert_eq!(reports.len(), SM_SMOKE.len());
}

// ---------------------------------------------------------------------------------
// Server leg
// ---------------------------------------------------------------------------------

/// Families (explicit id lists, as in tests/server_leg.rs); together exactly the 09-13
/// server scenarios.
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
        "api_signals_tokens_can_fx",
        &["C-SIG-API-01", "C-TOKEN-API-01", "C-CAN-02", "C-FX-01"],
    ),
];

/// Status + root facts against the Rust run (results with run ids differ per run).
fn server_cross_compare(reference: &RunReport, other: &RunReport) -> Result<(), Failure> {
    let fail = |message: String| Failure {
        id: other.id.clone(),
        lang: other.lang,
        message,
    };
    if reference.status != other.status {
        return Err(fail(format!(
            "status {} vs rust {}",
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
    let started = Instant::now();
    let h = ServerHarness::start().await.expect("server harness");
    let mut failures = Vec::new();
    let mut passed = 0usize;
    for id in ids {
        let spec = match load(id) {
            Ok(s) => s,
            Err(e) => {
                failures.push(e);
                continue;
            }
        };
        if !allowed(&spec).contains(&Lang::Ts) {
            failures.push(format!("{id}: the catalogue does not list ts"));
            continue;
        }
        let reference = match h.run_spec(&spec, &[Lang::Rust]).await {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("rust reference: {e}"));
                continue;
            }
        };
        // All roles (parent, child, grandchild) in TypeScript.
        match h.run_spec(&spec, &[Lang::Ts, Lang::Ts, Lang::Ts]).await {
            Ok(r) => {
                eprintln!("PASS {} [ts] {} {:.2?}", r.id, r.status, r.elapsed);
                match server_cross_compare(&reference, &r) {
                    Ok(()) => passed += 1,
                    Err(e) => failures.push(format!("cross-language vs rust: {e}")),
                }
            }
            Err(e) => failures.push(e.to_string()),
        }
    }
    match h.check_children_unique().await {
        Ok(n) => eprintln!("{name}: exactly one child run per (parent, op) for {n} child starts"),
        Err(e) => failures.push(format!("one child per (parent, op): {e}")),
    }
    let shutdown = Instant::now();
    h.shutdown().await.expect("shutdown");
    assert!(shutdown.elapsed() < Duration::from_secs(60));
    eprintln!(
        "server leg {name} [ts]: {passed} of {} scenario(s) passed in {:.1?}",
        ids.len(),
        started.elapsed()
    );
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

#[test]
fn server_leg_ts_covers_every_09_13_scenario() {
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
    let listed: usize = FAMILIES.iter().map(|(_, ids)| ids.len()).sum();
    assert_eq!(listed, owned.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_ts_children_basic() {
    server_family("children_basic").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_ts_children_lifecycle() {
    server_family("children_lifecycle").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_ts_children_resume_shielding() {
    server_family("children_resume_shielding").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_leg_ts_api_signals_tokens_can_fx() {
    server_family("api_signals_tokens_can_fx").await;
}
