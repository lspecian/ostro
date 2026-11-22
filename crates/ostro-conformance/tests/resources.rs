//! SC5 resource measurements with the real SDK-built conformance guests (plan 09-19;
//! `--features polyglot`, needs uv 0.7.15 and Node 24).
//!
//! Every test is `#[ignore]`: run them on purpose, one at a time, in a release build:
//!
//! ```text
//! cargo test --release -p ostro-conformance --features polyglot --test resources \
//!     -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! - `warm_instantiate`: per language/backend (Rust, Python, TS QuickJS, TS
//!   StarlingMonkey), the conformance module is prepared once (`Runtime::prepare_cached`),
//!   then the cached `InstancePre` is instantiated 500 times exactly as an activation
//!   builds its store (`Runtime::instantiate_probe`). Also measured: the first export
//!   call on a fresh instance (`describe`, which includes its own instantiation) and
//!   start-to-first-step (`Runtime::activate` of `<prefix>conf_steps` until the first
//!   `StepCompleted` reaches the sink; Pitfall 11, NFR-2.4).
//! - `idle_rss`: 50 live `<prefix>conf_timer` activations parked hot on their 1 h sleep;
//!   resident memory delta (`/proc/self/statm`) divided by 50.
//! - `pool_mixed`: `class = mixed` holds 100 parked Python runs (plus Rust and both TS
//!   backends) with no core-instance error; the default class (3,000 core instances)
//!   admits 100 Python runs by making them wait for permits; and the QuickJS memory
//!   ceiling (3 memories per instance against `total_memories = total_instances`) makes
//!   runs wait for admission instead of failing (plan eng2-mem).
//!
//! Each measurement appends one JSON line to `docs/data/sdk-resources.jsonl` with the
//! load average, build profile and host. SC5 thresholds (warm instantiate p50: Python
//! < 10 ms, TS < 5 ms) are asserted only with `OSTRO_SC5_ENFORCE=1` (D15: the native
//! perf machine enforces, WSL2 numbers are advisory).
#![cfg(feature = "polyglot")]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_conformance::langs::{self, Lang, module_for, workflow_name};
use ostro_conformance::repo_root;
use ostro_engine::config::{EngineConfig, LanguageClass};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, ActivationSink, AppendError, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec, Script, drive_to_completion,
};
use ostro_types::journal::{Event, EventBody, Payload};
use serde_json::{Value, json};

const ALL: [Lang; 4] = [Lang::Rust, Lang::Py, Lang::Ts, Lang::TsSm];
const INSTANTIATIONS: usize = 500;
const FIRST_CALLS: usize = 200;
const FIRST_STEPS: usize = 100;
const IDLE_N: usize = 50;

// ---------------------------------------------------------------------------
// Environment and output
// ---------------------------------------------------------------------------

fn backend(lang: Lang) -> &'static str {
    match lang {
        Lang::Rust => "native",
        Lang::Py => "componentize-py",
        Lang::Ts => "quickjs",
        Lang::TsSm => "starlingmonkey",
    }
}

fn loadavg() -> Vec<f64> {
    std::fs::read_to_string("/proc/loadavg")
        .unwrap_or_default()
        .split_whitespace()
        .take(3)
        .filter_map(|x| x.parse().ok())
        .collect()
}

fn host() -> Value {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find_map(|l| {
            l.strip_prefix("model name")
                .map(|r| r.trim_start_matches([' ', '\t', ':']).to_string())
        })
        .unwrap_or_default();
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .unwrap_or_default()
        .trim()
        .to_string();
    let wsl = kernel.to_ascii_lowercase().contains("microsoft");
    json!({
        "cpu": cpu,
        "vcpus": std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
        "kernel": kernel,
        "wsl2": wsl,
    })
}

fn profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn rss_kb() -> u64 {
    let pages: u64 = std::fs::read_to_string("/proc/self/statm")
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0);
    pages * 4
}

/// `/proc/self/smaps_rollup` fields in KiB (`Rss`, `Pss`, `Pss_Anon`, `Pss_File`,
/// `Pss_Shmem`, `Anonymous`). A page mapped into several pool slots (the copy-on-write
/// memory image) counts once per mapping in `Rss` but is shared out in `Pss`.
fn smaps() -> std::collections::BTreeMap<String, u64> {
    const KEYS: [&str; 6] = [
        "Rss",
        "Pss",
        "Pss_Anon",
        "Pss_File",
        "Pss_Shmem",
        "Anonymous",
    ];
    std::fs::read_to_string("/proc/self/smaps_rollup")
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let (k, rest) = l.split_once(':')?;
            if !KEYS.contains(&k) {
                return None;
            }
            let v = rest.split_whitespace().next()?.parse().ok()?;
            Some((k.to_string(), v))
        })
        .collect()
}

/// Per-instance delta of every [`smaps`] field.
fn smaps_delta(
    before: &std::collections::BTreeMap<String, u64>,
    after: &std::collections::BTreeMap<String, u64>,
    n: u64,
) -> Value {
    let m: serde_json::Map<String, Value> = after
        .iter()
        .map(|(k, a)| {
            let b = before.get(k).copied().unwrap_or(0);
            (
                format!("{}_kb", k.to_ascii_lowercase()),
                json!(a.saturating_sub(b) / n),
            )
        })
        .collect();
    Value::Object(m)
}

/// Appends one record to `docs/data/sdk-resources.jsonl` (and prints it).
fn record(test: &str, lang: Option<Lang>, mut fields: serde_json::Map<String, Value>) {
    use std::io::Write;
    let mut line = serde_json::Map::new();
    line.insert("test".into(), json!(test));
    if let Some(lang) = lang {
        line.insert("lang".into(), json!(langs::name(lang)));
        line.insert("backend".into(), json!(backend(lang)));
    }
    line.append(&mut fields);
    line.insert("loadavg".into(), json!(loadavg()));
    line.insert("profile".into(), json!(profile()));
    line.insert("host".into(), host());
    line.insert(
        "ts".into(),
        json!(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs())
        ),
    );
    let text = Value::Object(line).to_string();
    println!("{text}");
    let path = repo_root().join("docs/data/sdk-resources.jsonl");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir docs/data");
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    writeln!(f, "{text}").expect("append jsonl");
}

fn fields(v: Value) -> serde_json::Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => unreachable!(),
    }
}

fn enforce() -> bool {
    std::env::var("OSTRO_SC5_ENFORCE").is_ok_and(|v| v == "1")
}

struct Stats {
    p50: u64,
    p90: u64,
    max: u64,
}

fn stats(mut us: Vec<u64>) -> Stats {
    us.sort_unstable();
    let at = |q: f64| us[((us.len() - 1) as f64 * q).round() as usize];
    Stats {
        p50: at(0.5),
        p90: at(0.9),
        max: *us.last().expect("samples"),
    }
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Engine plumbing
// ---------------------------------------------------------------------------

fn runtime(cfg: EngineConfig) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            cfg,
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [0x19; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

/// A runtime whose waits park hot for a long time and whose parked instances are never
/// LRU-evicted by the soft node budget. The hot-wait window (2 h) exceeds the
/// conformance guests' 1 h sleep, so `conf_timer` parks hot instead of evicting with
/// `WaitUnsatisfied`; the frozen `FakeClock` never fires it.
fn parking_runtime(class: LanguageClass, total_instances: Option<u32>) -> Arc<Runtime> {
    let mut cfg = EngineConfig {
        class,
        hot_wait: Duration::from_secs(7_200),
        node_memory_budget: 64 << 30,
        ..EngineConfig::default()
    };
    if let Some(n) = total_instances {
        cfg.pool.total_instances = n;
    }
    runtime(cfg)
}

fn module(lang: Lang) -> Vec<u8> {
    module_for(lang, None).unwrap_or_else(|e| panic!("{} module: {e}", langs::name(lang)))
}

fn run_ref(id: &str) -> RunRef {
    RunRef {
        partition: 3,
        run_id: id.to_string(),
    }
}

/// `RunSpec` with a JSON `null` input (what the conformance engine leg sends).
fn spec(id: &str, workflow: &str) -> RunSpec {
    let mut s = RunSpec::new(run_ref(id), workflow, b"null");
    s.input = Payload {
        content_type: "application/json".into(),
        data: b"null".to_vec(),
    };
    s
}

/// Static resource shape of a component: memories and tables defined by its core modules
/// and the largest initial memory (the declared memory floor).
fn shape(wasm: &[u8]) -> Value {
    use wasmparser::{Parser, Payload as P};
    let (mut memories, mut tables, mut floor) = (0u32, 0u32, 0u64);
    for payload in Parser::new(0).parse_all(wasm) {
        match payload.expect("parse") {
            P::MemorySection(r) => {
                for m in r {
                    let m = m.expect("memory");
                    memories += 1;
                    floor = floor.max(m.initial * 65_536);
                }
            }
            // Imported memories/tables alias another module's and are not counted.
            P::TableSection(r) => tables += r.count(),
            _ => {}
        }
    }
    json!({ "memories_defined": memories, "tables_defined": tables, "memory_floor_bytes": floor })
}

/// Sink that records when the first `StepCompleted` is appended (start-to-first-step).
struct FirstStepSink {
    inner: Arc<FakeSink>,
    t0: Instant,
    first_step: Mutex<Option<Duration>>,
}

type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl ActivationSink for FirstStepSink {
    fn append<'a, 'b, 'c>(
        &'a self,
        run: &'b RunRef,
        attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> BoxFut<'c, Result<u64, AppendError>>
    where
        'a: 'c,
        'b: 'c,
        Self: 'c,
    {
        Box::pin(async move {
            if events
                .iter()
                .any(|e| matches!(e.body, EventBody::StepCompleted(_)))
            {
                let mut first = self.first_step.lock().expect("lock");
                if first.is_none() {
                    *first = Some(self.t0.elapsed());
                }
            }
            self.inner.append(run, attempt, from_seq, events).await
        })
    }

    fn fetch_blob<'a, 'b, 'c>(
        &'a self,
        sha: &'b [u8; 32],
    ) -> BoxFut<'c, Result<Vec<u8>, AppendError>>
    where
        'a: 'c,
        'b: 'c,
        Self: 'c,
    {
        self.inner.fetch_blob(sha)
    }

    fn done<'a, 'b, 'c>(
        &'a self,
        run: &'b RunRef,
        attempt: u32,
        outcome: ActivationOutcome,
    ) -> BoxFut<'c, ()>
    where
        'a: 'c,
        'b: 'c,
        Self: 'c,
    {
        self.inner.done(run, attempt, outcome)
    }
}

/// A live activation parked (or about to park) hot on its waits.
struct Live {
    handle: tokio::task::JoinHandle<ActivationOutcome>,
    cancel: Box<dyn FnOnce() + Send>,
    _inputs: tokio::sync::mpsc::UnboundedSender<Event>,
}

/// Starts `workflow` as a live activation with an input channel that never delivers, so
/// its waits park hot until cancelled.
fn spawn_live(
    rt: &Arc<Runtime>,
    p: &Arc<Prepared>,
    sink: &Arc<FakeSink>,
    workflow: &str,
    id: &str,
) -> Live {
    let s = spec(id, workflow);
    sink.seed(
        s.run.clone(),
        vec![s.run_started_event(&p.module_hash_hex)],
        1,
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let act = Activate {
        run: s.run.clone(),
        attempt: 1,
        module_sha: p.sha,
        journal: sink.journal(&s.run),
        inputs: Some(rx),
        cancel: Default::default(),
    };
    let cancel = act.cancel.clone();
    let (rt2, p2, sink2) = (rt.clone(), p.clone(), sink.clone());
    Live {
        handle: tokio::spawn(async move { rt2.activate(&p2, act, sink2).await }),
        cancel: Box::new(move || cancel.cancel()),
        _inputs: tx,
    }
}

async fn release(lives: Vec<Live>) -> Vec<ActivationOutcome> {
    let mut out = Vec::new();
    for l in lives {
        (l.cancel)();
        out.push(l.handle.await.expect("join"));
    }
    out
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// warm_instantiate (+ first_call, start_to_first_step)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "SC5 measurement: run with --ignored --test-threads=1 --nocapture (release)"]
async fn warm_instantiate() {
    let mut misses = Vec::new();
    for lang in ALL {
        let wasm = module(lang);
        let rt = runtime(EngineConfig {
            class: langs::class(lang),
            ..EngineConfig::default()
        });
        let t = Instant::now();
        let cwasm = rt
            .engine()
            .engine()
            .precompile_component(&wasm)
            .expect("precompile")
            .len();
        let compile_ms = t.elapsed().as_millis();
        let p = rt.prepare_cached(&wasm).await.expect("prepare");
        let weight = rt.core_weight(&p).await;

        // Warm instantiation of the cached InstancePre.
        let probe_run = run_ref(&format!("probe-{}", langs::name(lang)));
        let journal = vec![
            spec("probe", &workflow_name(lang, "conf_steps")).run_started_event(&p.module_hash_hex),
        ];
        for _ in 0..20 {
            rt.instantiate_probe(&p, probe_run.clone(), journal.clone())
                .await
                .expect("warm-up instantiate");
        }
        let mut inst = Vec::with_capacity(INSTANTIATIONS);
        for _ in 0..INSTANTIATIONS {
            let t = Instant::now();
            rt.instantiate_probe(&p, probe_run.clone(), journal.clone())
                .await
                .expect("instantiate");
            inst.push(micros(t.elapsed()));
        }
        let inst = stats(inst);
        record(
            "warm_instantiate",
            Some(lang),
            fields(json!({
                "metric": "instantiate",
                "p50_us": inst.p50, "p90_us": inst.p90, "max_us": inst.max,
                "n": INSTANTIATIONS,
                "wasm_bytes": wasm.len(), "cwasm_bytes": cwasm, "compile_ms": compile_ms,
                "core_instances_static": p.core_instances, "core_weight": weight,
                "shape": shape(&wasm),
            })),
        );

        // First export call on a fresh instance: `describe` (instantiation included).
        let mut calls = Vec::with_capacity(FIRST_CALLS);
        for _ in 0..FIRST_CALLS {
            let t = Instant::now();
            let metas = rt.describe(&p).await.expect("describe");
            calls.push(micros(t.elapsed()));
            assert!(!metas.is_empty());
        }
        let call = stats(calls);
        record(
            "warm_instantiate",
            Some(lang),
            fields(json!({
                "metric": "first_call",
                "call": "describe",
                "includes_instantiate": true,
                "p50_us": call.p50, "p90_us": call.p90, "max_us": call.max,
                "call_only_p50_us": call.p50.saturating_sub(inst.p50),
                "n": FIRST_CALLS,
            })),
        );

        // Start-to-first-step: activate conf_steps, time to the first StepCompleted append.
        let wf = workflow_name(lang, "conf_steps");
        let mut firsts = Vec::with_capacity(FIRST_STEPS);
        let mut totals = Vec::with_capacity(FIRST_STEPS);
        for i in 0..FIRST_STEPS {
            let s = spec(&format!("steps-{}-{i}", langs::name(lang)), &wf);
            let inner = Arc::new(FakeSink::new());
            inner.seed(
                s.run.clone(),
                vec![s.run_started_event(&p.module_hash_hex)],
                1,
            );
            let journal = inner.journal(&s.run);
            let t0 = Instant::now();
            let sink = Arc::new(FirstStepSink {
                inner: inner.clone(),
                t0,
                first_step: Mutex::new(None),
            });
            let out = rt
                .activate(
                    &p,
                    Activate {
                        run: s.run.clone(),
                        attempt: 1,
                        module_sha: p.sha,
                        journal,
                        inputs: None,
                        cancel: Default::default(),
                    },
                    sink.clone(),
                )
                .await;
            totals.push(micros(t0.elapsed()));
            assert!(
                matches!(out, ActivationOutcome::Completed { .. }),
                "{wf}: {out:?} {:?}",
                inner.journal(&s.run)
            );
            let first = sink
                .first_step
                .lock()
                .expect("lock")
                .expect("a StepCompleted");
            firsts.push(micros(first));
        }
        let first = stats(firsts);
        let total = stats(totals);
        record(
            "warm_instantiate",
            Some(lang),
            fields(json!({
                "metric": "start_to_first_step",
                "workflow": wf,
                "p50_us": first.p50, "p90_us": first.p90, "max_us": first.max,
                "run_to_completion_p50_us": total.p50, "run_to_completion_p90_us": total.p90,
                "n": FIRST_STEPS,
            })),
        );

        let target_us = match lang {
            Lang::Py => Some(10_000),
            Lang::Ts | Lang::TsSm => Some(5_000),
            Lang::Rust => None,
        };
        if let Some(target) = target_us
            && inst.p50 >= target
        {
            misses.push(format!(
                "{} warm instantiate p50 {} us >= {target} us",
                langs::name(lang),
                inst.p50
            ));
        }
    }
    if enforce() {
        assert!(misses.is_empty(), "SC5 targets missed: {misses:?}");
    } else if !misses.is_empty() {
        println!("SC5 advisory misses (not enforced): {misses:?}");
    }
}

// ---------------------------------------------------------------------------
// idle_rss
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "SC5 measurement: run with --ignored --test-threads=1 --nocapture (release)"]
async fn idle_rss() {
    for lang in ALL {
        let wasm = module(lang);
        let rt = parking_runtime(langs::class(lang), None);
        let p = rt.prepare_cached(&wasm).await.expect("prepare");
        let weight = rt.core_weight(&p).await;
        let wf = workflow_name(lang, "conf_timer");
        let sink = Arc::new(FakeSink::new());

        // Warm-up: one park/release so code pages and shared host state are resident.
        let warm = spawn_live(&rt, &p, &sink, &wf, &format!("warm-{}", langs::name(lang)));
        wait_until("warm-up park", Duration::from_secs(60), || {
            rt.parked().parked_count() == 1
        })
        .await;
        let out = release(vec![warm]).await;
        assert!(
            matches!(out[0], ActivationOutcome::Evicted { .. }),
            "{out:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;

        let before = rss_kb();
        let smaps_before = smaps();
        let charged_before = rt.engine().budget().used();
        let lives: Vec<Live> = (0..IDLE_N)
            .map(|i| {
                spawn_live(
                    &rt,
                    &p,
                    &sink,
                    &wf,
                    &format!("idle-{}-{i}", langs::name(lang)),
                )
            })
            .collect();
        wait_until("50 parked instances", Duration::from_secs(180), || {
            rt.parked().parked_count() == IDLE_N
        })
        .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let after = rss_kb();
        let smaps_after = smaps();
        let charged = rt.engine().budget().used().saturating_sub(charged_before);
        assert!(
            sink.outcomes().len() == 1,
            "only the warm-up finished: {:?}",
            sink.outcomes()
        );
        let in_use = rt.core_budget().in_use();
        assert_eq!(in_use, weight * IDLE_N as u32);
        let per = after.saturating_sub(before) / IDLE_N as u64;
        record(
            "idle_rss",
            Some(lang),
            fields(json!({
                "workflow": wf,
                "rss_kb": per,
                "rss_before_kb": before, "rss_after_kb": after,
                "per_instance_smaps": smaps_delta(&smaps_before, &smaps_after, IDLE_N as u64),
                // Linear memory charged to the soft `engine.node_memory_budget` (default
                // 1 GiB; parked instances are LRU-evicted above it).
                "node_budget_bytes_per_instance": charged / IDLE_N as u64,
                "n": IDLE_N,
                "core_slots_in_use": in_use,
            })),
        );
        let outs = release(lives).await;
        assert!(
            outs.iter()
                .all(|o| matches!(o, ActivationOutcome::Evicted { .. })),
            "{outs:?}"
        );
        assert_eq!(rt.core_budget().in_use(), 0);
    }
}

// ---------------------------------------------------------------------------
// pool_mixed
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "SC5 measurement: run with --ignored --test-threads=1 --nocapture (release)"]
async fn pool_mixed() {
    // (1) class = mixed: 100 parked Python runs plus 50 each of Rust, QuickJS and
    // StarlingMonkey, all live at once.
    let rt = parking_runtime(LanguageClass::Mixed, None);
    let mut prepared = Vec::new();
    for lang in ALL {
        let p = rt.prepare_cached(&module(lang)).await.expect("prepare");
        let w = rt.core_weight(&p).await;
        prepared.push((lang, p, w));
    }
    let sink = Arc::new(FakeSink::new());
    let mut lives = Vec::new();
    let mut want_slots = 0;
    let t = Instant::now();
    for (lang, p, w) in &prepared {
        let n = if *lang == Lang::Py { 100 } else { 50 };
        want_slots += w * n;
        let wf = workflow_name(*lang, "conf_timer");
        for i in 0..n {
            lives.push(spawn_live(
                &rt,
                p,
                &sink,
                &wf,
                &format!("mixed-{}-{i}", langs::name(*lang)),
            ));
        }
    }
    let total = lives.len();
    wait_until("250 parked mixed runs", Duration::from_secs(300), || {
        rt.parked().parked_count() == total || !sink.outcomes().is_empty()
    })
    .await;
    assert!(
        sink.outcomes().is_empty(),
        "no run may fail: {:?}",
        sink.outcomes()
    );
    let park_ms = t.elapsed().as_millis();
    let in_use = rt.core_budget().in_use();
    assert_eq!(in_use, want_slots);
    record(
        "pool_mixed",
        Some(Lang::Py),
        fields(json!({
            "case": "class_mixed_parked",
            "n": 100,
            "others": {"rust": 50, "ts": 50, "ts-sm": 50},
            "weights": prepared.iter().map(|(l, _, w)| (langs::name(*l).to_string(), json!(w))).collect::<serde_json::Map<_, _>>(),
            "core_slots_in_use": in_use,
            "capacity": rt.core_budget().capacity(),
            "park_all_ms": park_ms,
            "failed": 0,
        })),
    );
    let outs = release(lives).await;
    assert!(
        outs.iter()
            .all(|o| matches!(o, ActivationOutcome::Evicted { .. })),
        "{outs:?}"
    );
    assert_eq!(rt.core_budget().in_use(), 0);
    drop(prepared);
    drop(rt);

    // (2) default class (rust: 3,000 core instances = 78 Python instances): 100 concurrent
    // Python conf_timer runs whose timers fire 300 ms after they start. The 79th..100th
    // wait for permits; none fails.
    let rt = parking_runtime(LanguageClass::Rust, None);
    assert_eq!(rt.core_budget().capacity(), 3_000);
    let p = rt.prepare_cached(&module(Lang::Py)).await.expect("prepare");
    let w = rt.core_weight(&p).await;
    let wf = workflow_name(Lang::Py, "conf_timer");
    let t = Instant::now();
    let mut handles = Vec::new();
    for i in 0..100 {
        let (rt2, p2, wf2) = (rt.clone(), p.clone(), wf.clone());
        handles.push(tokio::spawn(async move {
            drive_to_completion(
                &rt2,
                &p2,
                spec(&format!("wait-py-{i}"), &wf2),
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
    let mut reactivations = 0;
    for h in handles {
        let d = h.await.expect("join");
        assert!(
            matches!(d.outcome, ActivationOutcome::Completed { .. }),
            "{:?} ({:?})",
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
        reactivations += d.activations - 1;
    }
    let all_ms = t.elapsed().as_millis();
    assert!(peak <= 3_000);
    assert!(peak >= (3_000 / w) * w, "budget never filled (peak {peak})");
    assert_eq!(rt.core_budget().in_use(), 0);
    record(
        "pool_mixed",
        Some(Lang::Py),
        fields(json!({
            "case": "class_default_waits",
            "n": 100,
            "weight": w,
            "capacity": 3_000,
            "peak_core_slots": peak,
            "fit_at_once": 3_000 / w,
            "reactivations": reactivations,
            "all_complete_ms": all_ms,
            "failed": 0,
        })),
    );
    drop(p);
    drop(rt);

    // (3) QuickJS memory ceiling: class = mixed sizes core instances for Python (38 per
    // component instance) but the pool keeps total_memories = total_instances while a
    // QuickJS instance needs 3 memories. With total_instances = 60 the core budget alone
    // admits 2,280 / 13 = 175 QuickJS instances, the pool's memories hold 20. Since plan
    // eng2-mem admission charges memories too: 20 park, the other 10 wait, none fails
    // (09-19 measured 19-20 parked and the rest AttemptFailed before the fix).
    let rt = parking_runtime(LanguageClass::Mixed, Some(60));
    let p = rt.prepare_cached(&module(Lang::Ts)).await.expect("prepare");
    let w = rt.core_weight(&p).await;
    let sink = Arc::new(FakeSink::new());
    let wf = workflow_name(Lang::Ts, "conf_timer");
    let tries = 30;
    let lives: Vec<Live> = (0..tries)
        .map(|i| spawn_live(&rt, &p, &sink, &wf, &format!("qjs-ceiling-{i}")))
        .collect();
    wait_until(
        "QuickJS ceiling probe settles",
        Duration::from_secs(120),
        || rt.parked().parked_count() == 20 || !sink.outcomes().is_empty(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let waiting = lives.iter().filter(|l| !l.handle.is_finished()).count() - 20;
    let parked = rt.parked().parked_count();
    let failures: Vec<String> = sink
        .outcomes()
        .iter()
        .map(|(_, _, o)| format!("{o:?}").chars().take(240).collect())
        .collect();
    record(
        "pool_mixed",
        Some(Lang::Ts),
        fields(json!({
            "case": "qjs_memory_ceiling",
            "class": "mixed",
            "total_instances": 60,
            "total_memories": 60,
            "weight": w,
            "capacity": rt.core_budget().capacity(),
            "budget_admits": rt.core_budget().capacity() / w,
            "memories_admit": rt.pool_budget().capacity().memories / 3,
            "n": tries,
            "parked": parked,
            "waiting": waiting,
            "failed": failures.len(),
            "first_failure": failures.first(),
        })),
    );
    println!(
        "QuickJS under class=mixed, total_instances=60: {parked} parked, {waiting} waiting, {} \
         failed",
        failures.len()
    );
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!((parked, waiting), (20, 10));
    let outs = release(lives).await;
    assert!(
        !outs
            .iter()
            .any(|o| matches!(o, ActivationOutcome::AttemptFailed(_))),
        "{outs:?}"
    );
}
