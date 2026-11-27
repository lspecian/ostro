//! ostro-jepsen: Jepsen-style fault testing, benchmarks and soak for Ostro (Phase 14,
//! QA-04/QA-05; D-P14-19..22).
//!
//! Built on the Phase 12/13 tier-C harness (`ostro_repl_conformance::harness::process`:
//! `ProcCluster` + the Rust `ChaosProxy` on `127.r.r.n`). Runs are seeded and reproducible;
//! reports go to `target/jepsen/<seed>/`.
//!
//! Module owners (this list is final after 14-01): [`workload`], [`history`], [`run`],
//! [`report`], [`checker`] (`lost`, `duplicate`, `fencing`) and the `partition` / `kill`
//! nemeses are 14-09; the `clock` / `disk` nemeses are 14-10; [`bench`] and [`soak`] (and the
//! `ostro-soak` binary) are 14-23.

pub mod bench;
pub mod checker;
pub mod history;
pub mod nemesis;
pub mod report;
pub mod run;
pub mod soak;
pub mod workload;

// Dependencies declared by plan 14-01 (D-P14-37), anchored so cargo's unused-dependency lint
// stays quiet until 14-09 / 14-10 / 14-23 consume them (owners delete the lines they use).
mod dep_anchors {
    use clap as _;
    use ostro_client as _;
    use ostro_log as _;
}
