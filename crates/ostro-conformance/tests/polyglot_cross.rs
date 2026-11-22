//! Cross-language conformance (plan 09-16, WF-06 / SDK-05; `--features polyglot`, needs
//! Python 3.12 + componentize-py and Node 24): the scenarios that only mean something when
//! several SDKs meet.
//!
//! - `cross_chain` (C-CHILD-08): a TypeScript root starts a Python child which starts a Rust
//!   grandchild (`ServerHarness::run_spec(spec, &[Ts, Py, Rust])`, roles by `wf:`/`wf2:`).
//!   The nested result reaches the root; the cancel variant cancels the TS root once the
//!   Rust grandchild sleeps and all three levels end Cancelled with the grandchild's
//!   cleanup run. Each level is checked to run in its language's module, and exactly one
//!   child run exists per (parent, op).
//! - `saga_http` (C-SAGA-03): `conf_saga_http` in Rust, Python and TypeScript against the
//!   MockProvider. Besides the spec (status, failure kind, projection), the test checks what
//!   the spec schema cannot express because the keys depend on the run id: the four HTTP
//!   effects carry `idempotency_key_for(root, a|b|refund-b|refund-a)` in that order (the
//!   compensations run in reverse), and the mock saw exactly one request and one charge
//!   per key.
//! - `values_cross_language` (C-VAL-01): `random_u64` (decimal), `random_f64`, `uuid_v7` and
//!   `idempotency_key("x")` are byte-equal (canonical JSON) across Rust, Python, TS on
//!   QuickJS and TS on StarlingMonkey, for the spec's pinned seed/run id and two more.
//! - `full_table`: every engine and server catalogue scenario in every catalogue language
//!   (all roles in that language; ts-sm where listed; C-CHILD-08 also TS -> Python -> Rust),
//!   each compared with Rust, printed as the scenario x language table (SC1 evidence).
//!
//! Run with `--test-threads=2` at most (guest builds serialise in `polyglot::build`).
#![cfg(feature = "polyglot")]

use std::time::{Duration, Instant};

use ostro_conformance::engine_leg::{Failure, RunReport};
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::server_leg::{ServerHarness, idempotency_key_for};
use ostro_conformance::spec::{self, Spec};
use ostro_types::journal::{EffectRequest, EventBody};

/// The C-CHILD-08 role assignment: root, child, grandchild.
const CHAIN: [Lang; 3] = [Lang::Ts, Lang::Py, Lang::Rust];

/// The saga's HTTP effects in journal order: two bookings, then the compensations in
/// reverse.
const SAGA_KEYS: [&str; 4] = ["a", "b", "refund-b", "refund-a"];

fn load(id: &str) -> Spec {
    let entry = spec::catalogue()
        .get(id)
        .unwrap_or_else(|| panic!("{id}: not in the catalogue"));
    assert!(
        !entry.is_pending(),
        "{id}: spec file {} is missing",
        entry.spec_path().display()
    );
    spec::load_spec(id).unwrap_or_else(|e| panic!("{id}: {e}"))
}

fn roles_name(roles: &[Lang]) -> String {
    roles
        .iter()
        .map(|l| langs::name(*l))
        .collect::<Vec<_>>()
        .join("->")
}

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
    if reference.failure_kind != other.failure_kind {
        return Err(fail(format!(
            "failure_kind {:?} vs rust {:?}",
            other.failure_kind, reference.failure_kind
        )));
    }
    ostro_conformance::compare::facts(&reference.projection.facts, &other.projection.facts)
        .map_err(|d| fail(d.to_string()))
}

/// Every child edge recorded on `h` runs the workflow of its level's language: level 1 =
/// `roles[1]`, level 2 = `roles[2]`.
fn check_chain_languages(h: &ServerHarness, roles: &[Lang]) -> Result<usize, String> {
    let edges = h.edges();
    let root_wf = langs::workflow_name(roles[0], "conf_parent_chain");
    let mid_prefix = langs::prefix(roles[1]);
    let leaf_prefix = langs::prefix(roles[2]);
    let mut checked = 0;
    for e in &edges {
        // Level-1 edges: the parent is not itself a child.
        let parent_is_child = edges.iter().any(|p| p.child == e.parent);
        if parent_is_child {
            if !e.workflow.starts_with(leaf_prefix) {
                return Err(format!(
                    "grandchild {} runs {} (expected a {} workflow)",
                    e.child,
                    e.workflow,
                    langs::name(roles[2])
                ));
            }
        } else if e.workflow != langs::workflow_name(roles[1], "conf_parent_chain") {
            return Err(format!(
                "child {} of a {root_wf} root runs {} (expected {}conf_parent_chain)",
                e.child, e.workflow, mid_prefix
            ));
        }
        checked += 1;
    }
    Ok(checked)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_chain() {
    let started = Instant::now();
    let spec = load("C-CHILD-08");
    let h = ServerHarness::start().await.expect("server harness");
    let reference = h
        .run_spec(&spec, &[Lang::Rust, Lang::Rust, Lang::Rust])
        .await
        .unwrap_or_else(|e| panic!("rust reference: {e}"));
    let edges_before = h.edges().len();
    // A fresh harness for the cross-language run, so the per-level language check sees
    // only TS -> Python -> Rust edges.
    h.check_children_unique()
        .await
        .unwrap_or_else(|e| panic!("rust: one child per (parent, op): {e}"));
    h.shutdown().await.expect("shutdown");

    let h = ServerHarness::start().await.expect("server harness");
    let r = h
        .run_spec(&spec, &CHAIN)
        .await
        .unwrap_or_else(|e| panic!("{} : {e}", roles_name(&CHAIN)));
    eprintln!(
        "PASS {} [{}] {} {:.2?} result {}",
        r.id,
        roles_name(&CHAIN),
        r.status,
        r.elapsed,
        r.result
            .as_ref()
            .map_or_else(|| "-".into(), |v| v.to_string())
    );
    server_cross_compare(&reference, &r).unwrap_or_else(|e| panic!("vs rust: {e}"));
    let n = check_chain_languages(&h, &CHAIN).unwrap_or_else(|e| panic!("{e}"));
    // Two root runs (completed + cancel variant), two levels each.
    assert_eq!(n, 4, "child edges recorded: {:?}", h.edges());
    assert_eq!(edges_before, 4, "rust reference child edges");
    let unique = h
        .check_children_unique()
        .await
        .unwrap_or_else(|e| panic!("one child per (parent, op): {e}"));
    eprintln!("C-CHILD-08: exactly one child run per (parent, op) for {unique} child starts");
    let t = Instant::now();
    h.shutdown().await.expect("shutdown");
    assert!(t.elapsed() < Duration::from_secs(60));
    eprintln!("cross_chain: {:.1?}", started.elapsed());
}

/// The saga's per-key checks for the run `report` of `lang` on `h` (the only
/// `conf_saga_http` root of that language on this harness).
async fn saga_checks(h: &ServerHarness, lang: Lang, report: &RunReport) -> Result<(), String> {
    let wf = langs::workflow_name(lang, "conf_saga_http");
    let roots = h.list_runs(&wf).await?;
    let [root] = roots.as_slice() else {
        return Err(format!("{wf}: expected one run, listed {roots:?}"));
    };
    let id = root.to_string();
    let keys: Vec<String> = SAGA_KEYS
        .iter()
        .map(|n| idempotency_key_for(&id, n))
        .collect();
    let order: Vec<String> = report
        .journals
        .iter()
        .flatten()
        .filter_map(|e| match &e.body {
            EventBody::EffectScheduled(s) => match &s.request {
                EffectRequest::Http(r) => Some(r.idempotency_key.clone().unwrap_or_default()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    if order != keys {
        return Err(format!(
            "effect keys in journal order {order:?}, expected {keys:?} (a, b, refund-b, refund-a)"
        ));
    }
    for (name, k) in SAGA_KEYS.iter().zip(&keys) {
        let (req, charges) = (h.mock().requests(k), h.mock().charges(k));
        if (req, charges) != (1, 1) {
            return Err(format!(
                "key {name} ({k}): {req} requests, {charges} charges (expected 1 / 1)"
            ));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn saga_http() {
    let started = Instant::now();
    let spec = load("C-SAGA-03");
    let h = ServerHarness::start().await.expect("server harness");
    let mut failures = Vec::new();
    let mut reference: Option<RunReport> = None;
    for lang in [Lang::Rust, Lang::Py, Lang::Ts] {
        match h.run_spec(&spec, &[lang]).await {
            Ok(r) => {
                eprintln!(
                    "PASS {} [{}] {} {:?} {:.2?}",
                    r.id,
                    langs::name(lang),
                    r.status,
                    r.failure_kind,
                    r.elapsed
                );
                if let Err(e) = saga_checks(&h, lang, &r).await {
                    failures.push(format!("{} [{}]: {e}", r.id, langs::name(lang)));
                }
                match &reference {
                    None => reference = Some(r),
                    Some(rf) => {
                        if let Err(e) = server_cross_compare(rf, &r) {
                            failures.push(format!("cross-language vs rust: {e}"));
                        }
                    }
                }
            }
            Err(e) => failures.push(e.to_string()),
        }
    }
    assert_eq!(h.mock().requests_to("/pay"), 12, "4 POST /pay per language");
    let t = Instant::now();
    h.shutdown().await.expect("shutdown");
    assert!(t.elapsed() < Duration::from_secs(60));
    eprintln!("saga_http: {:.1?}", started.elapsed());
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

// ---------------------------------------------------------------------------------
// C-VAL-01 across SDKs and JS backends
// ---------------------------------------------------------------------------------

/// The value-probe backends: Rust, Python, TS on QuickJS (jco) and TS on StarlingMonkey.
const VALUE_LANGS: [Lang; 4] = [Lang::Rust, Lang::Py, Lang::Ts, Lang::TsSm];

/// Extra (seed, run id) pairs beyond the spec's pinned seed 42, so the equality is not an
/// artefact of one input.
const EXTRA_VALUE_INPUTS: [(&str, &str); 2] = [
    ("7", "01890000-0000-7000-8000-0000000000aa"),
    (
        "18446744073709551557",
        "0193a1b2-c3d4-7e5f-9a0b-1c2d3e4f5a6b",
    ),
];

#[test]
fn values_cross_language() {
    let pinned = load("C-VAL-01");
    let mut inputs = vec![pinned.clone()];
    for (seed, run_id) in EXTRA_VALUE_INPUTS {
        let mut s = pinned.clone();
        s.seed = Some(seed.to_string());
        s.run_id = Some(run_id.to_string());
        // Only the pinned seed has spec values; the extra inputs are compared across
        // languages (byte-equal to Rust) rather than against constants.
        s.expect.result = None;
        inputs.push(s);
    }
    let mut failures = Vec::new();
    let mut per_input = Vec::new();
    for s in &inputs {
        let seed = s.seed.clone().unwrap_or_default();
        let run_id = s.run_id.clone().unwrap_or_default();
        let mut reference: Option<String> = None;
        for lang in VALUE_LANGS {
            let r = match ostro_conformance::engine_leg::run_spec(s, lang) {
                Ok(r) => r,
                Err(e) => {
                    failures.push(e.to_string());
                    continue;
                }
            };
            let v = r.result.as_ref().expect("C-VAL-01 result");
            for k in ["random_u64", "random_f64", "uuid_v7", "idempotency_key"] {
                assert!(
                    v.get(k).is_some(),
                    "[{}] missing {k}: {v}",
                    langs::name(lang)
                );
            }
            let text = ostro_conformance::canon::canonical(v);
            eprintln!(
                "C-VAL-01 seed={seed} run_id={run_id} [{}] {text}",
                langs::name(lang)
            );
            match &reference {
                None => reference = Some(text),
                Some(want) if *want == text => {}
                Some(want) => failures.push(format!(
                    "C-VAL-01 seed={seed} run_id={run_id} [{}]: {text}\n  rust: {want}",
                    langs::name(lang)
                )),
            }
        }
        per_input.push(reference.unwrap_or_default());
    }
    // The extra inputs really produce values other than the pinned ones.
    per_input.sort();
    per_input.dedup();
    assert_eq!(per_input.len(), inputs.len(), "distinct values per input");
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

// ---------------------------------------------------------------------------------
// Scenario x language table
// ---------------------------------------------------------------------------------

/// Table columns: one per language with all roles in it, then the cross-language chain.
const COLUMNS: [&str; 5] = ["rust", "py", "ts", "ts-sm", "ts->py->rust"];

#[derive(Default)]
struct Row {
    leg: &'static str,
    status: String,
    cells: [&'static str; 5],
    secs: f64,
}

fn col(lang: Lang) -> usize {
    match lang {
        Lang::Rust => 0,
        Lang::Py => 1,
        Lang::Ts => 2,
        Lang::TsSm => 3,
    }
}

fn new_row(leg: &'static str) -> Row {
    Row {
        leg,
        cells: ["-"; 5],
        ..Row::default()
    }
}

#[test]
fn full_table() {
    let started = Instant::now();
    let mut rows: Vec<(String, Row)> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    // Engine leg: every scenario x every catalogue language, each compared with Rust.
    for entry in spec::scenarios(spec::Leg::Engine) {
        let t = Instant::now();
        let mut row = new_row("engine");
        let s = load(&entry.id);
        let reference = match ostro_conformance::engine_leg::run_spec(&s, Lang::Rust) {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("rust: {e}"));
                row.cells[0] = "FAIL";
                rows.push((entry.id.clone(), row));
                continue;
            }
        };
        row.status.clone_from(&reference.status);
        row.cells[0] = "pass";
        for lang in entry.languages.iter().copied().filter(|l| *l != Lang::Rust) {
            let ok = match ostro_conformance::engine_leg::run_spec(&s, lang) {
                Ok(r) => match ostro_conformance::engine_leg::cross_compare(&reference, &r) {
                    Ok(()) => true,
                    Err(e) => {
                        failures.push(format!("cross-language vs rust: {e}"));
                        false
                    }
                },
                Err(e) => {
                    failures.push(e.to_string());
                    false
                }
            };
            row.cells[col(lang)] = if ok { "pass" } else { "FAIL" };
        }
        row.secs = t.elapsed().as_secs_f64();
        rows.push((entry.id.clone(), row));
    }

    // Server leg: every scenario with all roles in each catalogue language, compared with
    // the Rust run on the same server; C-CHILD-08 also with its TS -> Python -> Rust roles.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let h = ServerHarness::start().await.expect("server harness");
        for entry in spec::scenarios(spec::Leg::Server) {
            let t = Instant::now();
            let mut row = new_row("server");
            let s = load(&entry.id);
            let mut reference: Option<RunReport> = None;
            let mut runs: Vec<(usize, Vec<Lang>)> = entry
                .languages
                .iter()
                .map(|l| (col(*l), vec![*l; 3]))
                .collect();
            if entry.id == "C-CHILD-08" {
                runs.push((4, CHAIN.to_vec()));
            }
            for (c, roles) in runs {
                let lang = roles[0];
                let ok = match h.run_spec(&s, &roles).await {
                    Ok(r) => {
                        let mut ok = true;
                        if entry.id == "C-SAGA-03"
                            && let Err(e) = saga_checks(&h, lang, &r).await
                        {
                            failures.push(format!("C-SAGA-03 [{}]: {e}", langs::name(lang)));
                            ok = false;
                        }
                        match &reference {
                            None => {
                                row.status.clone_from(&r.status);
                                reference = Some(r);
                            }
                            Some(rf) => {
                                if let Err(e) = server_cross_compare(rf, &r) {
                                    failures.push(format!(
                                        "[{}] cross-language vs rust: {e}",
                                        roles_name(&roles)
                                    ));
                                    ok = false;
                                }
                            }
                        }
                        ok
                    }
                    Err(e) => {
                        failures.push(format!("[{}] {e}", roles_name(&roles)));
                        false
                    }
                };
                row.cells[c] = if ok { "pass" } else { "FAIL" };
            }
            row.secs = t.elapsed().as_secs_f64();
            rows.push((entry.id.clone(), row));
        }
        match h.check_children_unique().await {
            Ok(n) => {
                eprintln!("server leg: exactly one child run per (parent, op) for {n} child starts")
            }
            Err(e) => failures.push(format!("one child per (parent, op): {e}")),
        }
        h.shutdown().await.expect("shutdown");
    });

    let mut table = String::from("| Scenario | Leg | Status |");
    for c in COLUMNS {
        table.push_str(&format!(" {c} |"));
    }
    table.push_str(" Time |\n|---|---|---|");
    table.push_str(&"---|".repeat(COLUMNS.len()));
    table.push_str("---|\n");
    for (id, r) in &rows {
        table.push_str(&format!("| {id} | {} | {} |", r.leg, r.status));
        for c in r.cells {
            table.push_str(&format!(" {c} |"));
        }
        table.push_str(&format!(" {:.1} s |\n", r.secs));
    }
    let cells: usize = rows
        .iter()
        .map(|(_, r)| r.cells.iter().filter(|c| **c != "-").count())
        .sum();
    let passed: usize = rows
        .iter()
        .map(|(_, r)| r.cells.iter().filter(|c| **c == "pass").count())
        .sum();
    eprintln!(
        "scenario x language table ({} scenarios, {passed}/{cells} cells pass, {:.1?}):\n{table}",
        rows.len(),
        started.elapsed()
    );
    // Every catalogue scenario of both legs has a row.
    let expected =
        spec::scenarios(spec::Leg::Engine).count() + spec::scenarios(spec::Leg::Server).count();
    assert_eq!(rows.len(), expected);
    assert!(
        failures.is_empty(),
        "{} failure(s):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert_eq!(passed, cells);
}
