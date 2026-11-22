//! Real Python and TypeScript components on the real engine (plan 09-07, ENG-1/ENG-2;
//! `--features polyglot`, needs uv 0.7.15 and Node 24).
//!
//! The raw-binding fixtures `py-min` (componentize-py 0.25.1, with its extra `exports`
//! export) and `ts-min` (jco QuickJS) pass production deploy validation
//! (`Runtime::prepare`), describe `hello` v1, run `hello` to `Completed` on the testkit
//! sink with the journal `[RunStarted, StepCompleted("greet"), RunCompleted]`, and replay
//! that journal identically.
//!
//! ENG-2: the calibrated admission weight equals S9's pool minimum (py-min 38, QuickJS
//! 13). A raw Python guest that sleeps (`py-sleep`, written below) parks hot: with
//! `class = "python"` 100 instances are live at once; with the default class (3,000 core
//! instances, room for 78 Python instances) 100 concurrent runs still all complete because
//! the 79th..100th wait for core-instance permits instead of failing to instantiate.
//!
//! Plan eng2-mem: admission also charges memories and tables. With `class = mixed` and
//! `total_instances = 60` the core budget admits 175 QuickJS instances but the pool's 60
//! memories hold 20 (3 each): 30 parked SDK `ts_conf_timer` runs now give 20 parked and
//! 10 waiting, 0 `AttemptFailed` (09-19 measured 19-20 parked and the rest failing with
//! "instance pool exhausted ... memory stripe 0"). The real SDK guests' memories, tables
//! and declared memory floors pin `LanguageClass::min_memory_bytes`.
#![cfg(feature = "polyglot")]

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ostro_conformance::langs::{Lang, module_for, workflow_name};
use ostro_conformance::polyglot::{Mode, build};
use ostro_conformance::repo_root;
use ostro_engine::config::{EngineConfig, LanguageClass};
use ostro_engine::core_budget::PoolWeights;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec, Script, drive_to_completion, replay_journal,
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

#[tokio::test]
async fn calibrated_weights_match_pool_minimum() {
    for (mode, name, want) in [(Mode::Py, "py-min", 38), (Mode::Ts, "ts-min", 13)] {
        let p = rt().prepare(&fixture_wasm(mode, name)).expect("prepare");
        let w = rt().core_weight(&p).await;
        println!(
            "{name}: static core instances {}, admission weight {w}",
            p.core_instances
        );
        assert_eq!(w, want, "{name}");
    }
}

/// Raw-binding Python guest: workflow `nap` v1 sleeps 1 s (logical) and completes with
/// its input. With a long hot wait it parks hot inside the activation.
const PY_SLEEP: &str = r#"
from wit_world import exports
from wit_world.exports import workflows
from wit_world.imports import effects, types


class Workflows(exports.Workflows):
    def describe(self) -> list[workflows.WorkflowMeta]:
        return [workflows.WorkflowMeta(name="nap", version=1, default_queue=None, run_timeout_ms=None)]

    def run(self, name: str, input: types.Payload) -> workflows.RunResult:
        op = effects.sleep(1000)
        effects.wait([op], effects.WaitMode.ANY)
        return workflows.RunResult_Completed(input)
"#;

fn py_sleep_wasm() -> Vec<u8> {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("app.py"), PY_SLEEP).expect("write app.py");
    let path = build(Mode::Py, dir.path()).unwrap_or_else(|e| panic!("build py-sleep: {e}"));
    std::fs::read(&path).expect("read py-sleep")
}

fn parking_runtime(class: LanguageClass) -> Arc<Runtime> {
    Arc::new(runtime(EngineConfig {
        class,
        hot_wait: Duration::from_secs(60),
        // Soft accounting only; keep parked instances from being LRU-evicted.
        node_memory_budget: 64 << 30,
        ..EngineConfig::default()
    }))
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn python_class_holds_100_live_instances() {
    let rt = parking_runtime(LanguageClass::Python);
    let p = Arc::new(rt.prepare(&py_sleep_wasm()).expect("prepare py-sleep"));
    let sink = Arc::new(FakeSink::new());
    let mut running = Vec::new();
    for i in 0..100 {
        let run = run_ref(&format!("py-live-{i}"));
        let spec = RunSpec::new(run.clone(), "nap", b"1");
        sink.seed(
            run.clone(),
            vec![spec.run_started_event(&p.module_hash_hex)],
            1,
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let act = Activate {
            run: run.clone(),
            attempt: 1,
            module_sha: p.sha,
            journal: sink.journal(&run),
            inputs: Some(rx),
            cancel: Default::default(),
        };
        let cancel = act.cancel.clone();
        let (rt2, p2, sink2) = (rt.clone(), p.clone(), sink.clone());
        let h = tokio::spawn(async move { rt2.activate(&p2, act, sink2).await });
        running.push((h, cancel, tx));
    }
    wait_until(
        "100 parked Python instances",
        Duration::from_secs(120),
        || rt.parked().parked_count() == 100,
    )
    .await;
    assert_eq!(rt.core_budget().in_use(), 100 * 38);
    assert!(sink.outcomes().is_empty(), "{:?}", sink.outcomes());
    for (h, cancel, _tx) in running {
        cancel.cancel();
        let out = h.await.expect("join");
        assert!(matches!(out, ActivationOutcome::Evicted { .. }), "{out:?}");
    }
    assert_eq!(rt.core_budget().in_use(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn default_class_admits_100_python_runs_by_waiting() {
    let rt = parking_runtime(LanguageClass::Rust);
    assert_eq!(rt.core_budget().capacity(), 3_000);
    let p = Arc::new(rt.prepare(&py_sleep_wasm()).expect("prepare py-sleep"));
    let mut handles = Vec::new();
    for i in 0..100 {
        let (rt2, p2) = (rt.clone(), p.clone());
        handles.push(tokio::spawn(async move {
            drive_to_completion(
                &rt2,
                &p2,
                RunSpec::new(run_ref(&format!("py-wait-{i}")), "nap", b"1"),
                // Timers fire 300 ms after they start, so parked runs hold their permits
                // long enough for the 79th..100th to queue.
                Script::new().live(Duration::from_millis(300)),
            )
            .await
        }));
    }
    let mut peak = 0;
    while handles.iter().any(|h| !h.is_finished()) {
        peak = peak.max(rt.core_budget().in_use());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for h in handles {
        let d = h.await.expect("join");
        assert!(
            matches!(d.outcome, ActivationOutcome::Completed { .. }),
            "{:?} (outcomes {:?})",
            d.outcome,
            d.outcomes
        );
        assert!(
            !d.outcomes
                .iter()
                .any(|o| matches!(o, ActivationOutcome::AttemptFailed(_))),
            "{:?}",
            d.outcomes
        );
    }
    println!("peak core-instance slots in use: {peak} / 3000");
    assert!(peak <= 3_000);
    assert!(peak >= 78 * 38, "budget never filled (peak {peak})");
    assert_eq!(rt.core_budget().in_use(), 0);
}

fn sdk_module(lang: Lang) -> Vec<u8> {
    module_for(lang, None).unwrap_or_else(|e| panic!("conformance module: {e}"))
}

#[tokio::test]
async fn sdk_guest_shapes_pin_class_memory_floors() {
    let mut floors = Vec::new();
    for (lang, shape) in [
        (Lang::Rust, (1, 2)),
        (Lang::Py, (1, 2)),
        (Lang::Ts, (3, 3)),
        (Lang::TsSm, (1, 2)),
    ] {
        let p = rt().prepare(&sdk_module(lang)).expect("prepare");
        println!(
            "{lang:?}: memories {} tables {} floor {} B, weights {:?}",
            p.memories,
            p.tables,
            p.min_memory_bytes,
            rt().admission_weights(&p).await
        );
        assert_eq!((p.memories, p.tables), shape, "{lang:?}");
        floors.push((lang, p.min_memory_bytes));
    }
    let floor = |l: Lang| floors.iter().find(|(x, _)| *x == l).expect("lang").1;
    assert!(floor(Lang::Rust) <= LanguageClass::Rust.min_memory_bytes());
    // The class floors are exactly what the SDK guests declare (09-19 open issue 2).
    assert_eq!(floor(Lang::Py), LanguageClass::Python.min_memory_bytes());
    assert_eq!(
        floor(Lang::Ts).max(floor(Lang::TsSm)),
        LanguageClass::TypeScript.min_memory_bytes()
    );
    let all = floors.iter().map(|(_, f)| *f).max().expect("floors");
    assert_eq!(all, LanguageClass::Mixed.min_memory_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn quickjs_memories_wait_for_admission() {
    let rt = Arc::new(runtime(EngineConfig {
        class: LanguageClass::Mixed,
        // Longer than ts_conf_timer's 1 h sleep: it parks hot (the FakeClock is frozen).
        hot_wait: Duration::from_secs(7_200),
        node_memory_budget: 64 << 30,
        pool: ostro_engine::config::PoolConfig {
            total_instances: 60,
            ..Default::default()
        },
        ..EngineConfig::default()
    }));
    let p = Arc::new(rt.prepare(&sdk_module(Lang::Ts)).expect("prepare"));
    let w = rt.admission_weights(&p).await;
    assert_eq!(
        w,
        PoolWeights {
            instances: 1,
            core_instances: 13,
            memories: 3,
            tables: 3
        }
    );
    assert_eq!(
        rt.core_budget().capacity() / 13,
        175,
        "core alone admits 175"
    );
    let wf = workflow_name(Lang::Ts, "conf_timer");
    let sink = Arc::new(FakeSink::new());
    let mut running = Vec::new();
    for i in 0..30 {
        let run = run_ref(&format!("qjs-mem-{i}"));
        let mut spec = RunSpec::new(run.clone(), &wf, b"null");
        spec.input.content_type = "application/json".into();
        sink.seed(
            run.clone(),
            vec![spec.run_started_event(&p.module_hash_hex)],
            1,
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let act = Activate {
            run: run.clone(),
            attempt: 1,
            module_sha: p.sha,
            journal: sink.journal(&run),
            inputs: Some(rx),
            cancel: Default::default(),
        };
        let cancel = act.cancel.clone();
        let (rt2, p2, sink2) = (rt.clone(), p.clone(), sink.clone());
        let h = tokio::spawn(async move { rt2.activate(&p2, act, sink2).await });
        running.push((h, cancel, tx));
    }
    wait_until(
        "20 parked QuickJS instances",
        Duration::from_secs(120),
        || rt.parked().parked_count() == 20 || !sink.outcomes().is_empty(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(sink.outcomes().is_empty(), "{:?}", sink.outcomes());
    assert_eq!(rt.parked().parked_count(), 20);
    assert_eq!(rt.pool_budget().in_use().memories, 60);
    assert!(running.iter().all(|(h, _, _)| !h.is_finished()), "10 wait");

    // Cancel the first 10 spawned. Whether each was parked or still waiting, every
    // parked one cancelled frees 3 memories for one remaining waiter, so 20 park again.
    for (_, cancel, _tx) in &running[..10] {
        cancel.cancel();
    }
    wait_until(
        "10 cancelled, 20 parked again",
        Duration::from_secs(120),
        || sink.outcomes().len() == 10 && rt.parked().parked_count() == 20,
    )
    .await;
    for (h, cancel, _tx) in running {
        cancel.cancel();
        let out = h.await.expect("join");
        assert!(matches!(out, ActivationOutcome::Evicted { .. }), "{out:?}");
    }
    let outcomes = sink.outcomes();
    assert_eq!(outcomes.len(), 30);
    assert!(
        !outcomes
            .iter()
            .any(|(_, _, o)| matches!(o, ActivationOutcome::AttemptFailed(_))),
        "{outcomes:?}"
    );
    assert_eq!(rt.pool_budget().in_use(), PoolWeights::default());
}
