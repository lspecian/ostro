//! 12-03 Task 3: the runner classifies outcomes correctly (W11). Synthetic scenarios: one
//! passing, one failing an oracle, one sleeping past a 1 s deadline, one panicking, and one
//! whose cluster never forms. Only an oracle failure is red evidence.

use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_repl_conformance::harness::inproc::InProcCluster;
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec, HarnessError};
use ostro_repl_conformance::scenarios::runner::{
    Outcome, Row, format_rows, red_problem, red_run_with, run_group_with, run_scenario_with,
};
use ostro_repl_conformance::scenarios::{Scenario, ScenarioFailure, ScenarioResult, Tier};

const FORMED: Duration = Duration::from_secs(20);

fn passing(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        assert!(c.client(1).ready().await);
        Ok(())
    })
}

fn failing(_c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        Err(ScenarioFailure {
            oracle: "synthetic",
            detail: "deliberate".into(),
        })
    })
}

fn sleeping(_c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    })
}

fn panicking(_c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move { panic!("synthetic panic") })
}

fn scenario(
    name: &'static str,
    run: for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>,
) -> Scenario {
    Scenario {
        name,
        group: "selftest",
        tier: Tier::B,
        deadline: Duration::from_secs(1),
        spec: ClusterSpec::single,
        run,
    }
}

fn two_nodes_rf3() -> ClusterSpec {
    ClusterSpec {
        rf: 3,
        ..two_nodes()
    }
}

fn two_nodes() -> ClusterSpec {
    ClusterSpec {
        nodes: 2,
        rf: 2,
        min_insync: 1,
        partitions: 2,
        ..ClusterSpec::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcomes_are_classified() {
    let o = run_scenario_with(&scenario("pass", passing), FORMED).await;
    assert!(matches!(o, Outcome::Passed), "{o:?}");

    let o = run_scenario_with(&scenario("fail", failing), FORMED).await;
    assert!(
        matches!(&o, Outcome::Failed(f) if f.oracle == "synthetic" && f.detail == "deliberate"),
        "{o:?}"
    );

    let o = run_scenario_with(&scenario("slow", sleeping), FORMED).await;
    assert!(
        matches!(o, Outcome::Harness(HarnessError::Deadline(d)) if d == Duration::from_secs(1)),
        "{o:?}"
    );

    let o = run_scenario_with(&scenario("boom", panicking), FORMED).await;
    assert!(
        matches!(&o, Outcome::Harness(HarnessError::Panic(m)) if m.contains("synthetic panic")),
        "{o:?}"
    );

    // An unformed cluster is the scenario's measured red reason, before its body runs
    // (since 12-14 two nodes do form a cluster, but never one with rf 3).
    let mut unformed = scenario("unformed", panicking);
    unformed.spec = two_nodes_rf3;
    let o = run_scenario_with(&unformed, Duration::from_secs(1)).await;
    assert!(
        matches!(&o, Outcome::Failed(f) if f.oracle == "cluster_formed" && f.detail.contains("expected rf 3")),
        "{o:?}"
    );

    // Tier C never runs here.
    let mut c = scenario("tier-c", passing);
    c.tier = Tier::C;
    assert!(matches!(
        run_scenario_with(&c, FORMED).await,
        Outcome::Harness(_)
    ));
}

fn row(scenario: &'static str, pending: bool, outcome: Outcome) -> Row {
    Row {
        scenario,
        pending,
        outcome: Some(outcome),
    }
}

fn failure() -> ScenarioFailure {
    ScenarioFailure {
        oracle: "disk",
        detail: "x".into(),
    }
}

#[test]
fn red_run_rules() {
    // pending ⇒ must fail with a ScenarioFailure; non-pending ⇒ must pass.
    assert!(red_problem(&row("a", true, Outcome::Failed(failure()))).is_none());
    assert!(red_problem(&row("a", false, Outcome::Passed)).is_none());
    let p = red_problem(&row("a", true, Outcome::Passed)).expect("pending pass");
    assert!(
        p.contains("passes while pending: remove it from pending/"),
        "{p}"
    );
    let p = red_problem(&row("a", false, Outcome::Failed(failure()))).expect("fail");
    assert!(p.contains("a fails: disk: x"), "{p}");
    // A harness error is never red evidence, pending or not.
    for pending in [true, false] {
        let p = red_problem(&row(
            "a",
            pending,
            Outcome::Harness(HarnessError::Deadline(Duration::from_secs(1))),
        ))
        .expect("harness");
        assert!(p.contains("harness error"), "{p}");
        let p = red_problem(&row(
            "a",
            pending,
            Outcome::Harness(HarnessError::Panic("p".into())),
        ))
        .expect("panic");
        assert!(p.contains("never valid red evidence"), "{p}");
    }
    let table = format_rows(&[
        row("a", true, Outcome::Failed(failure())),
        row("b", false, Outcome::Passed),
    ]);
    assert!(
        table.starts_with("scenario | pending | result | oracle: detail\n"),
        "{table}"
    );
    assert!(table.contains("a | yes | FAIL | disk: x"), "{table}");
    assert!(table.contains("b | no | pass | "), "{table}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn red_run_and_group_runs() {
    let set = [scenario("pass", passing), scenario("fail", failing)];
    // "fail" pending: the red run is clean.
    let (rows, problems) = red_run_with(&set, |n| n == "fail", FORMED).await;
    assert!(problems.is_empty(), "{problems:?}\n{}", format_rows(&rows));
    // "pass" pending too: flagged.
    let (_, problems) = red_run_with(&set[..1], |_| true, FORMED).await;
    assert_eq!(problems.len(), 1, "{problems:?}");

    // Outside a red run a pending scenario is not run at all (it would panic).
    let set = [scenario("boom", panicking), scenario("pass", passing)];
    let (rows, problems) = run_group_with(&set, |n| n == "boom", false, FORMED).await;
    assert!(problems.is_empty(), "{problems:?}");
    assert!(rows[0].outcome.is_none());
    assert!(matches!(rows[1].outcome, Some(Outcome::Passed)));
    // In a red run it is, and its panic is a harness problem.
    let (_, problems) = run_group_with(&set[..1], |_| true, true, FORMED).await;
    assert!(problems[0].contains("harness error"), "{problems:?}");
}
