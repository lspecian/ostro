//! Run named tier-B scenarios on demand (debugging aid, 12-16):
//! `OSTRO_REPL_SCENARIO=a,b [OSTRO_REPL_REPEAT=3] cargo test -p ostro-repl-conformance
//! --test scenario -- --ignored --nocapture` (`RUST_LOG` set: server logs on stdout).
//! Pending lists are ignored: every named
//! scenario must pass every repetition.

use ostro_repl_conformance::scenarios::{self, runner};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "on demand (OSTRO_REPL_SCENARIO=<name>[,<name>...])"]
async fn named_scenarios() {
    if std::env::var_os("RUST_LOG").is_some() {
        ostro_server::logging::init_tracing(&ostro_server::config::LogSection::default());
    }
    let names = std::env::var("OSTRO_REPL_SCENARIO").unwrap_or_default();
    let repeat: usize = std::env::var("OSTRO_REPL_REPEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let all = scenarios::all();
    let mut failed = Vec::new();
    let mut ran = 0usize;
    for name in names.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let s = all
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no scenario named {name}"));
        for i in 1..=repeat {
            // clock-exempt: progress output
            let t = std::time::Instant::now();
            let outcome = runner::run_scenario(s).await;
            ran += 1;
            println!(
                "SCENARIO {name} #{i}: {} ({:.1}s)",
                outcome.label(),
                t.elapsed().as_secs_f64()
            );
            if !matches!(outcome, runner::Outcome::Passed) {
                failed.push(format!("{name} #{i}: {}", outcome.label()));
            }
        }
    }
    assert!(ran > 0, "set OSTRO_REPL_SCENARIO");
    assert!(failed.is_empty(), "{failed:#?}");
}
