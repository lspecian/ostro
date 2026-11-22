//! Server leg (09-13, WF-06 / SDK-05): the server-leg scenarios owned by 09-13, run for
//! each language selected by `OSTRO_CONF_LANGS` (default `rust`) on a real in-process
//! `ostro-server` (`ostro_conformance::server_leg::ServerHarness`): children end to end,
//! signals and tokens through the REST API, a continue-as-new chain followed by the
//! result long-poll, and an HTTP effect against the Phase 6 `MockProvider`.
//!
//! One `#[tokio::test]` per scenario family, each with its own server. Every family ends
//! by listing runs and asserting exactly one child run per (parent, op). Run with
//! `--test-threads=2` at most.

use std::time::{Duration, Instant};

use ostro_conformance::engine_leg::RunReport;
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::server_leg::{ServerHarness, idempotency_key_for};
use ostro_conformance::spec::{self, CatalogueEntry, Leg};
use serde_json::json;

/// Families (explicit id lists); every 09-13 server scenario belongs to exactly one.
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

fn owned() -> Vec<&'static CatalogueEntry> {
    spec::scenarios(Leg::Server)
        .filter(|e| e.owner == "09-13")
        .collect()
}

/// Runs a family on a fresh server for the selected languages; panics with every failure.
async fn family(name: &str) -> Vec<RunReport> {
    let ids = FAMILIES
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, ids)| *ids)
        .expect("family");
    let selected = langs::selected();
    let h = ServerHarness::start().await.expect("server harness");
    let mut failures = Vec::new();
    let mut reports = Vec::new();
    for id in ids {
        let entry = spec::catalogue().get(id).expect("catalogue id");
        if entry.is_pending() {
            failures.push(format!(
                "{id}: spec file {} is missing",
                entry.spec_path().display()
            ));
            continue;
        }
        let spec = match spec::load_spec(id) {
            Ok(s) => s,
            Err(e) => {
                failures.push(format!("{id}: {e}"));
                continue;
            }
        };
        let allowed: Vec<Lang> = spec
            .languages
            .clone()
            .unwrap_or_else(|| entry.languages.clone());
        let mut reference: Option<RunReport> = None;
        for &lang in selected.iter().filter(|l| allowed.contains(l)) {
            match h.run_spec(&spec, &[lang]).await {
                Ok(r) => {
                    eprintln!(
                        "PASS {} [{}] {} {:?}",
                        r.id,
                        langs::name(lang),
                        r.status,
                        r.elapsed
                    );
                    if lang == Lang::Rust {
                        reference = Some(r.clone());
                    } else if let Some(rf) = &reference
                        && let Err(e) = cross_compare(rf, &r)
                    {
                        failures.push(format!("cross-language: {e}"));
                    }
                    reports.push(r);
                }
                Err(e) => failures.push(e.to_string()),
            }
        }
    }
    match h.check_children_unique().await {
        Ok(n) => eprintln!("{name}: exactly one child run per (parent, op) for {n} child starts"),
        Err(e) => failures.push(format!("one child per (parent, op): {e}")),
    }
    h.shutdown().await.expect("shutdown");
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    reports
}

/// Cross-language check against the Rust reference: status, result and root facts (run
/// ids in results differ per run, so outcomes with `@` references compare facts only).
fn cross_compare(
    reference: &RunReport,
    other: &RunReport,
) -> Result<(), ostro_conformance::engine_leg::Failure> {
    let fail = |message: String| ostro_conformance::engine_leg::Failure {
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

#[test]
fn every_09_13_server_scenario_has_a_spec_and_one_family() {
    let all = owned();
    assert_eq!(all.len(), 14, "09-13 server scenario count");
    for e in all {
        let n = FAMILIES
            .iter()
            .filter(|(_, ids)| ids.contains(&e.id.as_str()))
            .count();
        assert_eq!(n, 1, "{} is in {n} families", e.id);
        assert!(!e.is_pending(), "{} has no spec file", e.id);
        spec::load_spec(&e.id).unwrap_or_else(|err| panic!("{}: {err}", e.id));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn children_basic() {
    family("children_basic").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn children_lifecycle() {
    family("children_lifecycle").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn children_resume_shielding() {
    family("children_resume_shielding").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_signals_tokens_can() {
    family("api_signals_tokens_can").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_effect() {
    family("http_effect").await;
}

/// `conf_saga_http` (spec C-SAGA-03 belongs to 09-16): the Rust workflow books `a` and `b`
/// against the mock, fails `book_c`, and the compensations refund `b` then `a`, one charge
/// per key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saga_http_workflow_compensates_with_one_charge_per_key() {
    let h = ServerHarness::start().await.expect("server harness");
    h.deploy_lang(Lang::Rust).await.expect("deploy");
    let run = h
        .start_run(
            &langs::workflow_name(Lang::Rust, "conf_saga_http"),
            &json!({ "url": h.mock().base_url() }),
        )
        .await
        .expect("start");
    let out = h
        .result(&run, Duration::from_secs(30))
        .await
        .expect("result");
    assert_eq!(out.status, "failed", "{out:?}");
    let id = run.to_string();
    let keys: Vec<String> = ["a", "b", "refund-b", "refund-a"]
        .iter()
        .map(|n| idempotency_key_for(&id, n))
        .collect();
    for k in &keys {
        assert_eq!(h.mock().charges(k), 1, "charges under {k}");
        assert_eq!(h.mock().requests(k), 1, "requests under {k}");
    }
    assert_eq!(h.mock().requests_to("/pay"), 4);
    // Compensations ran in reverse order: refund-b's effect precedes refund-a's.
    let j = h.history(&run).await.expect("journal");
    let order: Vec<String> = j
        .iter()
        .filter_map(|e| match &e.body {
            ostro_types::journal::EventBody::EffectScheduled(s) => match &s.request {
                ostro_types::journal::EffectRequest::Http(r) => r.idempotency_key.clone(),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(order, keys, "effect order");
    let failure_kind = j.iter().rev().find_map(|e| match &e.body {
        ostro_types::journal::EventBody::RunFailed(f) => {
            Some(ostro_conformance::projection::failure_kind(&f.failure))
        }
        _ => None,
    });
    assert_eq!(failure_kind.as_deref(), Some("book_c_failed"));
    let started = Instant::now();
    h.shutdown().await.expect("shutdown");
    assert!(started.elapsed() < Duration::from_secs(60));
}
