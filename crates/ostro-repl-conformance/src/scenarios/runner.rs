//! The scenario runner (12-03, D-P12-19 / W11).
//!
//! [`run_scenario`] starts a fresh [`InProcCluster`] from the scenario's spec, runs the
//! measured `cluster_formed` precondition first (its failure is the scenario's
//! `ScenarioFailure { oracle: "cluster_formed" }`), runs the body under the scenario's hard
//! deadline and always stops the cluster. Only a [`ScenarioFailure`] is a valid red result:
//! a deadline, a panic or a cluster that does not start is a [`HarnessError`], never red
//! evidence and never proof that a mutant bites.
//!
//! [`run_group_test`] is what each group's thin test file calls: pending scenarios
//! ([`crate::pending::is_pending`]) print `PENDING <name>` and are skipped unless
//! `OSTRO_REPL_RED_RUN` is set, in which case they must fail with a `ScenarioFailure`.
//! [`red_run_all`] (`tests/red_run.rs`) runs every tier-B scenario: pending ⇒ must fail,
//! non-pending ⇒ must pass, and prints `scenario | pending | result | oracle: detail`.

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt as _;

use super::{Scenario, ScenarioFailure, Tier};
use crate::harness::inproc::InProcCluster;
use crate::harness::{ClusterHarness, HarnessError};
use crate::oracle::cluster_formed;
use crate::pending;

/// How one scenario ended.
#[derive(Debug)]
pub enum Outcome {
    /// Every oracle held.
    Passed,
    /// A measured oracle failed: the only valid red result.
    Failed(ScenarioFailure),
    /// Deadline, panic or start failure: never red evidence.
    Harness(HarnessError),
}

impl Outcome {
    /// Short form for the result table.
    pub fn label(&self) -> String {
        match self {
            Outcome::Passed => "pass".into(),
            Outcome::Failed(f) => format!("FAIL {f}"),
            Outcome::Harness(e) => format!("HARNESS {e}"),
        }
    }
}

/// One row of a run.
#[derive(Debug)]
pub struct Row {
    /// Scenario name.
    pub scenario: &'static str,
    /// Whether it is on a pending list.
    pub pending: bool,
    /// What happened (`None` = not run: pending outside a red run, or tier C).
    pub outcome: Option<Outcome>,
}

/// Whether `OSTRO_REPL_RED_RUN` asks for a red run (set and not `0`).
pub fn red_run_env() -> bool {
    std::env::var("OSTRO_REPL_RED_RUN").is_ok_and(|v| !v.is_empty() && v != "0")
}

fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into())
}

/// [`run_scenario_with`] with the default formation deadline.
pub async fn run_scenario(s: &Scenario) -> Outcome {
    run_scenario_with(s, cluster_formed::FORMATION_DEADLINE).await
}

/// Run one tier-B scenario on a fresh cluster (module docs).
pub async fn run_scenario_with(s: &Scenario, formation_deadline: Duration) -> Outcome {
    if s.tier != Tier::B {
        return Outcome::Harness(HarnessError::Other(format!(
            "{}: tier C scenarios run on the process harness (12-09)",
            s.name
        )));
    }
    let spec = (s.spec)();
    let mut cluster = match AssertUnwindSafe(InProcCluster::start(spec.clone()))
        .catch_unwind()
        .await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return Outcome::Harness(e),
        Err(p) => return Outcome::Harness(HarnessError::Panic(panic_message(&*p))),
    };
    let outcome = run_on_cluster(&mut cluster, s, formation_deadline).await;
    cluster.stop().await;
    outcome
}

/// Run scenario `s` on a cluster the caller started from `(s.spec)()` (either tier; tier C
/// starts its processes with per-node failpoints, 12-17): the measured `cluster_formed`
/// precondition, then the body under the scenario's deadline. The caller stops the cluster.
pub async fn run_on_cluster<C: ClusterHarness>(
    cluster: &mut C,
    s: &Scenario<C>,
    formation_deadline: Duration,
) -> Outcome {
    let spec = &(s.spec)();
    let formed = AssertUnwindSafe(cluster_formed::check_within(
        cluster,
        spec,
        formation_deadline,
    ))
    .catch_unwind()
    .await;
    match formed {
        Err(p) => return Outcome::Harness(HarnessError::Panic(panic_message(&*p))),
        Ok(report) => {
            if let Err(f) = report.into_result() {
                return Outcome::Failed(f);
            }
        }
    }
    let body = AssertUnwindSafe((s.run)(cluster)).catch_unwind();
    // clock-exempt: test deadline
    match tokio::time::timeout(s.deadline, body).await {
        Err(_) => Outcome::Harness(HarnessError::Deadline(s.deadline)),
        Ok(Err(p)) => Outcome::Harness(HarnessError::Panic(panic_message(&*p))),
        Ok(Ok(Ok(()))) => Outcome::Passed,
        Ok(Ok(Err(f))) => Outcome::Failed(f),
    }
}

/// The pending list (`pending/<list>.rs`) naming `name`.
pub fn pending_list_of(name: &str) -> &'static str {
    let names = ["core", "rest", "process"];
    pending::all_lists()
        .iter()
        .position(|l| l.contains(&name))
        .map_or("<list>", |i| names[i])
}

/// What is wrong with a row in a red run (`None` = as expected).
pub fn red_problem(row: &Row) -> Option<String> {
    let name = row.scenario;
    match (&row.outcome, row.pending) {
        (None, _) => None,
        (Some(Outcome::Harness(e)), _) => Some(format!(
            "{name}: harness error (never valid red evidence): {e}"
        )),
        (Some(Outcome::Passed), true) => Some(format!(
            "{name} passes while pending: remove it from pending/{}.rs",
            pending_list_of(name)
        )),
        (Some(Outcome::Failed(_)), true) | (Some(Outcome::Passed), false) => None,
        (Some(Outcome::Failed(f)), false) => Some(format!("{name} fails: {f}")),
    }
}

/// `scenario | pending | result | oracle: detail` rows.
pub fn format_rows(rows: &[Row]) -> String {
    let mut out = String::from("scenario | pending | result | oracle: detail\n");
    for r in rows {
        let (result, detail) = match &r.outcome {
            None => ("not run".to_owned(), String::new()),
            Some(Outcome::Passed) => ("pass".to_owned(), String::new()),
            Some(Outcome::Failed(f)) => ("FAIL".to_owned(), f.to_string()),
            Some(Outcome::Harness(e)) => ("HARNESS".to_owned(), e.to_string()),
        };
        out.push_str(&format!(
            "{} | {} | {result} | {detail}\n",
            r.scenario,
            if r.pending { "yes" } else { "no" }
        ));
    }
    out
}

/// Run `scenarios` in red-run mode with an explicit pending predicate; returns the rows and
/// the problems ([`red_problem`]).
pub async fn red_run_with(
    scenarios: &[Scenario],
    is_pending: impl Fn(&str) -> bool,
    formation_deadline: Duration,
) -> (Vec<Row>, Vec<String>) {
    let mut rows = Vec::new();
    for s in scenarios {
        let pending = is_pending(s.name);
        let outcome = if s.tier == Tier::B {
            Some(run_scenario_with(s, formation_deadline).await)
        } else {
            println!("SKIP {} (tier C)", s.name);
            None
        };
        rows.push(Row {
            scenario: s.name,
            pending,
            outcome,
        });
    }
    let problems = rows.iter().filter_map(red_problem).collect();
    (rows, problems)
}

/// Run every scenario in red-run mode (`OSTRO_REPL_RED_RUN=1`): pending ⇒ must fail with a
/// `ScenarioFailure`, non-pending ⇒ must pass. Called by `tests/red_run.rs`.
pub async fn red_run_all() {
    let all = super::all();
    let (rows, problems) = red_run_with(
        &all,
        pending::is_pending,
        cluster_formed::FORMATION_DEADLINE,
    )
    .await;
    println!("{}", format_rows(&rows));
    assert!(
        problems.is_empty(),
        "red run problems:\n{}",
        problems.join("\n")
    );
}

/// Run `scenarios` the way a group test does; returns the rows and the problems. `red`
/// selects red-run semantics for pending scenarios (else they are skipped with `PENDING`).
pub async fn run_group_with(
    scenarios: &[Scenario],
    is_pending: impl Fn(&str) -> bool,
    red: bool,
    formation_deadline: Duration,
) -> (Vec<Row>, Vec<String>) {
    let mut rows = Vec::new();
    for s in scenarios {
        let pending = is_pending(s.name);
        let outcome = if pending && !red {
            println!("PENDING {}", s.name);
            None
        } else if s.tier != Tier::B {
            println!("SKIP {} (tier C)", s.name);
            None
        } else {
            Some(run_scenario_with(s, formation_deadline).await)
        };
        rows.push(Row {
            scenario: s.name,
            pending,
            outcome,
        });
    }
    let problems = rows.iter().filter_map(red_problem).collect();
    (rows, problems)
}

/// The body of each group's test file: run the group's scenarios, honouring pending lists
/// and `OSTRO_REPL_RED_RUN`; panics with every problem.
pub async fn run_group_test(group: &str) {
    let scenarios: Vec<Scenario> = super::all()
        .into_iter()
        .filter(|s| s.group == group)
        .collect();
    let (rows, problems) = run_group_with(
        &scenarios,
        pending::is_pending,
        red_run_env(),
        cluster_formed::FORMATION_DEADLINE,
    )
    .await;
    println!("{}", format_rows(&rows));
    assert!(
        problems.is_empty(),
        "group {group}:\n{}",
        problems.join("\n")
    );
}
