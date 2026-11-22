//! Real Python and TypeScript components on the real engine (plan 09-07, ENG-1/ENG-2;
//! `--features polyglot`, needs uv 0.7.15 and Node 24).
//!
//! The raw-binding fixtures `py-min` (componentize-py 0.25.1, with its extra `exports`
//! export) and `ts-min` (jco QuickJS) pass production deploy validation
//! (`Runtime::prepare`), describe `hello` v1, run `hello` to `Completed` on the testkit
//! sink with the journal `[RunStarted, StepCompleted("greet"), RunCompleted]`, and replay
//! that journal identically.
#![cfg(feature = "polyglot")]

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use ostro_conformance::polyglot::{Mode, build};
use ostro_conformance::repo_root;
use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, FakeClock, RunSpec, Script, drive_to_completion, replay_journal,
};
use ostro_types::journal::{EventBody, EventKind};

fn runtime(cfg: EngineConfig) -> Runtime {
    Runtime::new(
        cfg,
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
            token_key: [7; 32],
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime")
}

/// Default engine config (class rust, default pool).
fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| runtime(EngineConfig::default()))
}

fn fixture_wasm(mode: Mode, name: &str) -> Vec<u8> {
    let dir: PathBuf = repo_root().join("conformance/fixtures").join(name);
    let path = build(mode, &dir).unwrap_or_else(|e| panic!("build {name}: {e}"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn run_ref(id: &str) -> RunRef {
    RunRef {
        partition: 2,
        run_id: id.to_string(),
    }
}

async fn hello_runs(rt: &Runtime, p: &Prepared, what: &str) {
    let metas = rt.describe(p).await.expect("describe");
    let got: Vec<(&str, u32)> = metas.iter().map(|m| (m.name.as_str(), m.version)).collect();
    assert_eq!(got, [("hello", 1)], "{what} describe()");

    let run = run_ref(&format!("run-{what}"));
    let d = drive_to_completion(
        rt,
        p,
        RunSpec::new(run.clone(), "hello", br#""world""#),
        Script::new(),
    )
    .await;
    let ActivationOutcome::Completed { output } = &d.outcome else {
        panic!(
            "{what}: expected Completed, got {:?} ({:?})",
            d.outcome, d.journal
        );
    };
    assert_eq!(output.data, br#"{"hello":"world"}"#, "{what} output");
    let kinds: Vec<EventKind> = d.journal.iter().map(|e| e.body.kind()).collect();
    assert_eq!(
        kinds,
        [
            EventKind::RunStarted,
            EventKind::StepCompleted,
            EventKind::RunCompleted
        ],
        "{what} journal"
    );
    let EventBody::StepCompleted(sc) = &d.journal[1].body else {
        unreachable!()
    };
    assert_eq!(sc.name, "greet", "{what} step name");

    let (outcome, appended) = replay_journal(rt, p, run, d.journal.clone()).await;
    assert_eq!(
        outcome, d.outcome,
        "{what}: replay outcome must be identical"
    );
    assert_eq!(appended, 0, "{what}: replay must append nothing");
}

#[tokio::test]
async fn py_min_validates_and_runs() {
    let wasm = fixture_wasm(Mode::Py, "py-min");
    let p = rt()
        .prepare(&wasm)
        .expect("py-min must pass deploy validation");
    hello_runs(rt(), &p, "py-min").await;
}

#[tokio::test]
async fn ts_min_validates_and_runs() {
    let wasm = fixture_wasm(Mode::Ts, "ts-min");
    let p = rt()
        .prepare(&wasm)
        .expect("ts-min must pass deploy validation");
    hello_runs(rt(), &p, "ts-min").await;
}
