//! The permanent mutant matrix (12-07, D-P12-19): `cargo test -p ostro-repl-conformance
//! --features failpoints --test mutants -- --test-threads=1`.
//!
//! For every tier-B mapping in [`ostro_repl_conformance::mutants::MAP`] whose scenario is
//! not pending: the scenario runs without the mutant (must pass), then with
//! `fail::cfg(<mutant>, "return")` (must fail with a measured `ScenarioFailure`; a harness
//! error or a deadline never counts as the mutant biting). Pending scenarios print
//! `SKIP (pending)`, a failure when `OSTRO_REPL_REQUIRE_ALL=1`; tier-C-only mutants print
//! `TIER-C (process_mutants)`. Failpoints are process-global, so the matrix runs as one test,
//! one whole cluster at a time.

#[cfg(not(feature = "failpoints"))]
#[test]
fn mutant_matrix() {
    println!("failpoints feature off: the mutant matrix runs with --features failpoints");
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutant_matrix() {
    use ostro_repl_conformance::mutants::MAP;
    use ostro_repl_conformance::pending;
    use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

    let require_all =
        std::env::var("OSTRO_REPL_REQUIRE_ALL").is_ok_and(|v| !v.is_empty() && v != "0");
    let all = ostro_repl_conformance::scenarios::all();
    let mut problems = Vec::new();
    let mut bitten = 0usize;
    for m in MAP {
        if m.tier_b.is_empty() {
            println!(
                "MUTANT {}: TIER-C (process_mutants) {}",
                m.mutant,
                m.tier_c.unwrap_or("<none>")
            );
            continue;
        }
        for name in m.tier_b {
            let Some(s) = all.iter().find(|s| s.name == *name) else {
                problems.push(format!("{}: {name} is not a registered scenario", m.mutant));
                continue;
            };
            if pending::is_pending(name) {
                println!("MUTANT {}: {name} SKIP (pending)", m.mutant);
                if require_all {
                    problems.push(format!(
                        "{}: {name} is pending (OSTRO_REPL_REQUIRE_ALL=1)",
                        m.mutant
                    ));
                }
                continue;
            }
            match run_scenario(s).await {
                Outcome::Passed => {}
                other => {
                    problems.push(format!(
                        "{}: {name} baseline (no mutant) must pass: {}",
                        m.mutant,
                        other.label()
                    ));
                    continue;
                }
            }
            if let Err(e) = fail::cfg(m.mutant, "return") {
                problems.push(format!("{}: fail::cfg: {e}", m.mutant));
                continue;
            }
            let out = run_scenario(s).await;
            fail::remove(m.mutant);
            match out {
                Outcome::Failed(f) => {
                    bitten += 1;
                    println!("MUTANT {}: {name} FAILED as expected: {f}", m.mutant);
                }
                Outcome::Passed => problems.push(format!(
                    "{}: {name} passes with the mutant enabled: the scenario does not catch it",
                    m.mutant
                )),
                Outcome::Harness(e) => problems.push(format!(
                    "{}: {name} ended in a harness error with the mutant (never counts as biting): {e}",
                    m.mutant
                )),
            }
        }
    }
    println!("mutant matrix: {bitten} (mutant, scenario) pair(s) bitten");
    assert!(
        problems.is_empty(),
        "mutant matrix:\n{}",
        problems.join("\n")
    );
}
