//! The Phase 13 mutant matrix (13-18, D-P13-25): `cargo test -p ostro-repl-conformance
//! --features failpoints --test mutants_p13 -- --test-threads=1 --nocapture`.
//!
//! Per mutant of [`ostro_repl_conformance::mutants_p13::MAP`]: every mapped scenario runs
//! without the mutant (must pass), then with `fail::cfg(<mutant>, "return")` until one fails
//! with a measured `ScenarioFailure` (`FAILED as expected`; a harness error or a deadline
//! never counts as biting). A mutant without a runnable scenario prints `SKIP`; with
//! `OSTRO_P13_REQUIRE_ALL=1` a skipped or non-biting mutant fails the matrix. Failpoints are
//! process-global: one test, one whole cluster at a time.
//!
//! The matrix runs about 25 whole-cluster scenarios, so it runs only when asked: with
//! `OSTRO_P13_REQUIRE_ALL` set (CI's dedicated step, 13-20) or `OSTRO_P13_MUTANT=<substring>`
//! (local iteration). The whole-crate failpoint suite (`cargo test -p
//! ostro-repl-conformance --features failpoints`) prints how to run it and passes.

#[cfg(not(feature = "failpoints"))]
#[test]
fn p13_mutant_matrix() {
    println!("failpoints feature off: the Phase 13 mutant matrix runs with --features failpoints");
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p13_mutant_matrix() {
    use ostro_repl_conformance::mutants_p13::{MAP, find};
    use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};

    let require_all =
        std::env::var("OSTRO_P13_REQUIRE_ALL").is_ok_and(|v| !v.is_empty() && v != "0");
    // `OSTRO_P13_MUTANT=<substring>` runs only the matching mutants (local iteration).
    let only = std::env::var("OSTRO_P13_MUTANT")
        .ok()
        .filter(|v| !v.is_empty());
    if std::env::var_os("OSTRO_P13_REQUIRE_ALL").is_none() && only.is_none() {
        println!(
            "p13 mutant matrix not requested: run with OSTRO_P13_REQUIRE_ALL=1 (or OSTRO_P13_MUTANT=<substring>)"
        );
        return;
    }
    let mut problems = Vec::new();
    let mut bitten = 0usize;
    for m in MAP {
        if only.as_deref().is_some_and(|o| !m.mutant.contains(o)) {
            println!(
                "MUTANT {}: SKIP (OSTRO_P13_MUTANT={})",
                m.mutant,
                only.as_deref().unwrap_or("")
            );
            if require_all {
                problems.push(format!("{}: skipped by OSTRO_P13_MUTANT", m.mutant));
            }
            continue;
        }
        let runnable: Vec<_> = m.scenarios.iter().filter_map(|n| find(n)).collect();
        if runnable.is_empty() {
            println!("MUTANT {}: SKIP (no runnable scenario)", m.mutant);
            if require_all {
                problems.push(format!("{}: no runnable scenario", m.mutant));
            }
            continue;
        }
        let mut baseline_ok = true;
        for s in &runnable {
            let t0 = std::time::Instant::now(); // clock-exempt: report timing
            match run_scenario(s).await {
                Outcome::Passed => println!(
                    "MUTANT {}: {} baseline passed in {:?}",
                    m.mutant,
                    s.name,
                    t0.elapsed()
                ),
                other => {
                    baseline_ok = false;
                    problems.push(format!(
                        "{}: {} baseline (no mutant) must pass: {}",
                        m.mutant,
                        s.name,
                        other.label()
                    ));
                }
            }
        }
        if !baseline_ok {
            continue;
        }
        let mut bit = false;
        for s in &runnable {
            if let Err(e) = fail::cfg(m.mutant, "return") {
                problems.push(format!("{}: fail::cfg: {e}", m.mutant));
                break;
            }
            let t0 = std::time::Instant::now(); // clock-exempt: report timing
            let out = run_scenario(s).await;
            fail::remove(m.mutant);
            match out {
                Outcome::Failed(f) => {
                    println!(
                        "MUTANT {}: {} FAILED as expected in {:?}: {f}",
                        m.mutant,
                        s.name,
                        t0.elapsed()
                    );
                    bit = true;
                    break;
                }
                Outcome::Passed => println!(
                    "MUTANT {}: {} passed with the mutant (does not catch it)",
                    m.mutant, s.name
                ),
                Outcome::Harness(e) => println!(
                    "MUTANT {}: {} harness error with the mutant (never counts): {e}",
                    m.mutant, s.name
                ),
            }
        }
        if bit {
            bitten += 1;
        } else {
            println!("MUTANT {}: NOT BITTEN by {:?}", m.mutant, m.scenarios);
            problems.push(format!(
                "{}: no scenario of {:?} fails with the mutant",
                m.mutant, m.scenarios
            ));
        }
    }
    println!(
        "p13 mutant matrix: {bitten} of {} mutants bitten",
        MAP.len()
    );
    if !require_all {
        // Without REQUIRE_ALL a non-biting mutant is reported, a broken baseline still fails.
        problems.retain(|p| p.contains("baseline") || p.contains("fail::cfg"));
    }
    assert!(
        problems.is_empty(),
        "p13 mutant matrix:\n{}",
        problems.join("\n")
    );
}
