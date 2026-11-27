//! `ostro-soak`: the soak driver (14-23, D-P14-22). A fraction (`--load`) of the measured
//! rated load (`--rated <steps/s>` or `auto`) against the 1.0 configuration for
//! `--duration`, sampled every `--interval`, judged against the criteria pre-registered in
//! `docs/soak.md`. Writes `<out>/samples.jsonl` and `<out>/verdict.json`.
//!
//! ```text
//! ostro-soak --nodes 3 --duration 168h --load 0.7 --rated <steps/s> --interval 1h --out <dir>
//! ```
//!
//! Exit code: 0 PASS, 1 FAIL, 2 the soak could not start.

use std::process::ExitCode;

use clap::Parser;
use ostro_jepsen::soak::{self, SoakArgs};

fn main() -> ExitCode {
    let args = SoakArgs::parse();
    let spec = match args.spec() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("ostro-soak: {e}");
            return ExitCode::from(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ostro-soak: tokio runtime: {e}");
            return ExitCode::from(2);
        }
    };
    match rt.block_on(soak::run(&spec)) {
        Ok(v) => {
            println!("{}", v.summary());
            println!("verdict: {}", spec.out.join("verdict.json").display());
            if v.passed() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(e) => {
            eprintln!("ostro-soak: {e}");
            ExitCode::from(2)
        }
    }
}
