//! Spike S9 measurement harness (plan 09-04; Wasmtime 50.0.2). Throwaway.
//!
//! Every subcommand appends JSON lines to `--out` (default
//! `docs/spikes/data/S9/measurements.jsonl`) with `cmd`, `guest`, `backend`, the values,
//! the load average at the time of the measurement and `wasmtime: "50.0.2"`. WSL2 timings
//! are advisory (D15): read them next to `loadavg`.
//!
//! - `inspect`: imports, exports, core instances (nested included), core modules,
//!   declared memories, `.wasm` size (wasmparser =0.261.0 + wasmtime component types).
//! - `pool`: the pooling allocator sized from the module (minimum core instances per
//!   component and minimum `max_memory_size` found by bisection), N live instances, RSS
//!   delta per instance from `/proc/self/statm`.
//! - `instantiate`: p50/p90 of instantiate and of the first `describe` call on fresh
//!   instances (pooling allocator, AOT artefact loaded with `deserialize_file`).
//! - `aot`: `Engine::precompile_component` time and `.cwasm` size.
//! - `fuel`: fuel used by `describe` and by one `run(<workflow>)` on a fake host,
//!   against `engine.activation_fuel` (10,000,000,000).
//! - `engine`: runs workflows on the REAL `ostro_engine::runtime::Runtime` with the
//!   testkit fake sink and a frozen FakeClock (production linker validation included).
//! - `note`: appends a build-time observation (`cmd: "probe"`) recorded by run.sh.
//!
//! The plain-wasmtime subcommands (all but `engine`) use a fake host for the real
//! `ostro:runtime@0.1.0` world: steps execute, effect calls hand out op ids, and `wait`
//! completes every listed op at once (timers -> `timer`, signals -> `signal({})`).
//! Python cannot run on the real engine until 09-07 (ENG-1, the `exports` export).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use wasmtime::{Result, bail, format_err as anyhow};
use clap::{Args, Parser, Subcommand};
use serde_json::{Map, Value, json};
use wasmtime::component::{Component, Linker, Resource, ResourceTable};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../../wit/ostro-runtime-0.1.0",
        world: "ostro:runtime/workflow",
    });
}

use bindings::Workflow;
use bindings::WorkflowPre;
use bindings::exports::ostro::runtime::workflows::RunResult;
use bindings::ostro::runtime::{context, effects, journal, log, types};

const WASMTIME: &str = "50.0.2";
const ACTIVATION_FUEL: u64 = 10_000_000_000;
const PAGE: u64 = 65_536;

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

#[derive(Args, Clone)]
struct Common {
    /// Guest label (py-asyncio, py-base, ts-qjs, ts-sm, ...).
    #[arg(long)]
    guest: String,
    /// Backend label (componentize-py, quickjs, starlingmonkey).
    #[arg(long)]
    backend: String,
    /// JSON-lines file to append to.
    #[arg(long, default_value = "docs/spikes/data/S9/measurements.jsonl")]
    out: PathBuf,
    /// Tag the line(s) with `"probe": <name>` (behaviour probes, Task 2).
    #[arg(long)]
    probe: Option<String>,
}

fn loadavg() -> Value {
    let s = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    let v: Vec<f64> = s
        .split_whitespace()
        .take(3)
        .filter_map(|x| x.parse().ok())
        .collect();
    json!(v)
}

fn emit(c: &Common, cmd: &str, mut fields: Map<String, Value>) -> Result<()> {
    use std::io::Write;
    let mut line = Map::new();
    line.insert("cmd".into(), json!(cmd));
    line.insert("guest".into(), json!(c.guest));
    line.insert("backend".into(), json!(c.backend));
    if let Some(p) = &c.probe {
        line.insert("probe".into(), json!(p));
    }
    line.append(&mut fields);
    line.insert("loadavg".into(), loadavg());
    line.insert("wasmtime".into(), json!(WASMTIME));
    let text = Value::Object(line).to_string();
    println!("{text}");
    if let Some(dir) = c.out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&c.out)
        .map_err(|e| anyhow!("open {}: {e}", c.out.display()))?;
    writeln!(f, "{text}")?;
    Ok(())
}

fn obj(v: Value) -> Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => Map::new(),
    }
}

fn us(d: Duration) -> f64 {
    (d.as_secs_f64() * 1e7).round() / 10.0
}

fn pct(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    us(sorted[idx.min(sorted.len() - 1)])
}

fn stats(mut v: Vec<Duration>) -> Value {
    v.sort();
    json!({ "n": v.len(), "p50_us": pct(&v, 0.5), "p90_us": pct(&v, 0.9), "max_us": v.last().map(|d| us(*d)) })
}

fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: u64 = s
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .unwrap_or(0);
    pages * 4096
}

// ---------------------------------------------------------------------------
// Static inspection
// ---------------------------------------------------------------------------

#[derive(Default, Debug)]
struct Counts {
    core_instances_instantiate: u64,
    core_instances_from_exports: u64,
    core_modules: u64,
    nested_components: u64,
    memories: Vec<(u64, Option<u64>)>,
    tables: u64,
}

fn count_wasm(bytes: &[u8]) -> Result<Counts> {
    use wasmparser::{Instance, Parser, Payload};
    let mut c = Counts::default();
    for payload in Parser::new(0).parse_all(bytes) {
        match payload? {
            Payload::InstanceSection(r) => {
                for i in r {
                    match i? {
                        Instance::Instantiate { .. } => c.core_instances_instantiate += 1,
                        Instance::FromExports(_) => c.core_instances_from_exports += 1,
                    }
                }
            }
            Payload::ModuleSection { .. } => c.core_modules += 1,
            Payload::ComponentSection { .. } => c.nested_components += 1,
            Payload::MemorySection(r) => {
                for m in r {
                    let m = m?;
                    c.memories.push((m.initial, m.maximum));
                }
            }
            Payload::TableSection(r) => c.tables += u64::from(r.count()),
            _ => {}
        }
    }
    Ok(c)
}

fn cmd_inspect(c: &Common, wasm: &Path) -> Result<()> {
    let bytes = std::fs::read(wasm)?;
    let counts = count_wasm(&bytes)?;
    let engine = Engine::new(&base_config())?;
    let component = Component::new(&engine, &bytes)?;
    let ty = component.component_type();
    let imports: Vec<String> = ty.imports(&engine).map(|(n, _)| n.to_string()).collect();
    let exports: Vec<String> = ty.exports(&engine).map(|(n, _)| n.to_string()).collect();
    let wasi: Vec<&String> = imports.iter().filter(|n| n.starts_with("wasi:")).collect();
    let declared_min: u64 = counts.memories.iter().map(|(i, _)| i * PAGE).max().unwrap_or(0);
    emit(
        c,
        "inspect",
        obj(json!({
            "wasm_bytes": bytes.len(),
            "imports": imports,
            "wasi_imports": wasi,
            "exports": exports,
            "core_instances": counts.core_instances_instantiate,
            "core_instance_entries_from_exports": counts.core_instances_from_exports,
            "core_modules": counts.core_modules,
            "nested_components": counts.nested_components,
            "memories": counts.memories.iter().map(|(i, m)| json!({"min_pages": i, "max_pages": m})).collect::<Vec<_>>(),
            "declared_memory_floor_bytes": declared_min,
            "tables": counts.tables,
        })),
    )
}

// ---------------------------------------------------------------------------
// Fake host for the real world
// ---------------------------------------------------------------------------

pub struct StepRep {
    seq: u64,
}

#[derive(Clone, Copy)]
enum OpKind {
    Timer,
    Signal,
    Other,
}

#[derive(Default)]
struct Host {
    table: ResourceTable,
    seq: u64,
    ops: HashMap<u64, OpKind>,
    steps_executed: u64,
    waits: u64,
    logs: u64,
    rng: u64,
}

impl Host {
    fn op(&mut self, k: OpKind) -> u64 {
        self.seq += 1;
        self.ops.insert(self.seq, k);
        self.seq
    }
}

fn info() -> types::RunInfo {
    types::RunInfo {
        run_id: "run-s9".into(),
        workflow: "hello".into(),
        workflow_version: 1,
        attempt: 1,
        module_hash: "00".into(),
        tenant: None,
        parent_run_id: None,
        deadline: None,
        continued_from: None,
    }
}

impl types::Host for Host {}

impl context::Host for Host {
    fn info(&mut self) -> types::RunInfo {
        info()
    }
    fn now(&mut self) -> types::Datetime {
        types::Datetime {
            seconds: 1_700_000_000,
            nanoseconds: 0,
        }
    }
    fn random_u64(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn random_bytes(&mut self, len: u32) -> Vec<u8> {
        (0..len).map(|_| self.random_u64() as u8).collect()
    }
    fn uuid_v7(&mut self) -> String {
        "018bcfe5-6800-7000-8000-000000000000".into()
    }
    fn is_replaying(&mut self) -> bool {
        false
    }
    fn patched(&mut self, _change_id: String) -> bool {
        true
    }
    fn set_status(&mut self, _json: String) {}
    fn continue_as_new_suggested(&mut self) -> bool {
        false
    }
}

impl journal::HostStep for Host {
    fn seq(&mut self, s: Resource<journal::Step>) -> u64 {
        let r: Resource<StepRep> = Resource::new_borrow(s.rep());
        self.table.get(&r).map(|s| s.seq).unwrap_or(u64::MAX)
    }
    fn complete(
        &mut self,
        _s: Resource<journal::Step>,
        _outcome: Result<types::Payload, types::Failure>,
    ) -> journal::CompletionAck {
        journal::CompletionAck::Recorded
    }
    fn drop(&mut self, s: Resource<journal::Step>) -> wasmtime::Result<()> {
        let r: Resource<StepRep> = Resource::new_own(s.rep());
        self.table.delete(r)?;
        Ok(())
    }
}

impl journal::Host for Host {
    fn begin_step(&mut self, _name: String, _opts: types::StepOptions) -> journal::StepStart {
        self.seq += 1;
        self.steps_executed += 1;
        let r = self
            .table
            .push(StepRep { seq: self.seq })
            .expect("resource table");
        journal::StepStart::Execute(Resource::new_own(r.rep()))
    }
}

fn unsupported() -> types::Failure {
    types::Failure::NonRetryable(types::ErrorInfo {
        kind: "unsupported".into(),
        message: "s9 fake host".into(),
        details: None,
    })
}

impl effects::Host for Host {
    fn http_fetch(&mut self, _req: effects::HttpRequest) -> u64 {
        self.op(OpKind::Other)
    }
    fn sql_query(&mut self, _req: effects::SqlRequest) -> u64 {
        self.op(OpKind::Other)
    }
    fn object_store(&mut self, _req: effects::ObjectRequest) -> u64 {
        self.op(OpKind::Other)
    }
    fn sleep(&mut self, _ms: u64) -> u64 {
        self.op(OpKind::Timer)
    }
    fn sleep_until(&mut self, _at: types::Datetime) -> u64 {
        self.op(OpKind::Timer)
    }
    fn wait_signal(&mut self, _name: String, _timeout: Option<u64>) -> u64 {
        self.op(OpKind::Signal)
    }
    fn start_child(
        &mut self,
        _workflow: String,
        _input: types::Payload,
        _opts: effects::ChildOptions,
    ) -> u64 {
        self.op(OpKind::Other)
    }
    fn create_await_token(&mut self, _timeout: Option<u64>) -> effects::AwaitToken {
        let op = self.op(OpKind::Other);
        effects::AwaitToken {
            op,
            token: format!("tok-{op}"),
        }
    }
    fn cancellation(&mut self) -> u64 {
        u64::MAX
    }
    fn cancel_op(&mut self, _op: u64) {}
    fn wait(&mut self, ops: Vec<u64>, _mode: effects::WaitMode) -> Vec<effects::Completion> {
        self.waits += 1;
        self.seq += 1;
        ops.into_iter()
            .filter(|op| *op != u64::MAX)
            .map(|op| effects::Completion {
                op,
                outcome: match self.ops.get(&op).copied().unwrap_or(OpKind::Other) {
                    OpKind::Timer => Ok(effects::OpOutput::Timer),
                    OpKind::Signal => Ok(effects::OpOutput::Signal(types::Payload {
                        content_type: "application/json".into(),
                        data: b"{}".to_vec(),
                    })),
                    OpKind::Other => Err(unsupported()),
                },
            })
            .collect()
    }
}

impl log::Host for Host {
    fn emit(&mut self, _level: log::Level, _message: String, _fields: Vec<(String, String)>) {
        self.logs += 1;
    }
}

struct D;
impl wasmtime::component::HasData for D {
    type Data<'a> = &'a mut Host;
}

// ---------------------------------------------------------------------------
// Engines
// ---------------------------------------------------------------------------

/// Same determinism/limit flags as `ostro_engine::config::base_config` (fuel and epoch
/// interruption included, so fuel numbers are comparable), without async.
fn base_config() -> Config {
    let mut cfg = Config::new();
    cfg.wasm_component_model(true)
        .cranelift_nan_canonicalization(true)
        .relaxed_simd_deterministic(true)
        .consume_fuel(true)
        .epoch_interruption(true)
        .max_wasm_stack(512 * 1024)
        .parallel_compilation(true);
    cfg
}

#[derive(Clone, Copy, Debug)]
struct PoolShape {
    instances: u32,
    core_per: u32,
    memories_per: u32,
    tables_per: u32,
    max_memory: usize,
}

fn pooled_engine(s: PoolShape) -> Result<Engine> {
    let mut p = PoolingAllocationConfig::default();
    p.total_component_instances(s.instances)
        .total_core_instances(s.instances * s.core_per)
        .total_memories(s.instances * s.memories_per.max(1))
        .total_tables(s.instances * s.tables_per.max(1))
        .total_stacks(s.instances)
        .max_core_instances_per_component(s.core_per)
        .max_memories_per_component(s.memories_per.max(1))
        .max_tables_per_component(s.tables_per.max(1))
        .max_memory_size(s.max_memory)
        .table_elements(20_000);
    let mut cfg = base_config();
    cfg.allocation_strategy(InstanceAllocationStrategy::Pooling(p));
    Engine::new(&cfg)
}

fn on_demand_engine() -> Result<Engine> {
    let mut cfg = base_config();
    cfg.allocation_strategy(InstanceAllocationStrategy::OnDemand);
    Engine::new(&cfg)
}

/// AOT-compiles once per engine config into `<wasm>.cwasm` next to the input and loads
/// it with `deserialize_file` (mmap + CoW memory images, like the production AOT cache).
fn load(engine: &Engine, wasm: &Path) -> Result<Component> {
    let bytes = std::fs::read(wasm)?;
    let cwasm = wasm.with_extension("cwasm");
    let art = engine.precompile_component(&bytes)?;
    std::fs::write(&cwasm, &art)?;
    // SAFETY: the artefact was produced just above by this engine's own config and
    // nothing else writes it while it is mapped.
    Ok(unsafe { Component::deserialize_file(engine, &cwasm)? })
}

fn pre(engine: &Engine, component: &Component) -> Result<WorkflowPre<Host>> {
    let mut linker: Linker<Host> = Linker::new(engine);
    Workflow::add_to_linker::<Host, D>(&mut linker, |h| h)?;
    linker.define_unknown_imports_as_traps(component)?;
    WorkflowPre::new(linker.instantiate_pre(component)?)
}

fn store(engine: &Engine, fuel: u64) -> Result<Store<Host>> {
    let mut s = Store::new(engine, Host::default());
    s.set_fuel(fuel)?;
    s.set_epoch_deadline(u64::MAX / 2);
    Ok(s)
}

fn try_instance(engine: &Engine, component: &Component) -> Result<()> {
    let p = pre(engine, component)?;
    let mut s = store(engine, ACTIVATION_FUEL)?;
    let w = p.instantiate(&mut s)?;
    w.ostro_runtime_workflows().call_describe(&mut s)?;
    Ok(())
}

/// Smallest value in `lo..=hi` for which `ok` holds (assumes monotonic).
fn bisect(mut lo: u64, mut hi: u64, mut ok: impl FnMut(u64) -> bool) -> Option<u64> {
    if !ok(hi) {
        return None;
    }
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if ok(mid) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    Some(hi)
}

// ---------------------------------------------------------------------------
// pool / instantiate / aot / fuel
// ---------------------------------------------------------------------------

fn cmd_pool(c: &Common, wasm: &Path, n: u32) -> Result<()> {
    let bytes = std::fs::read(wasm)?;
    let counts = count_wasm(&bytes)?;
    let mems = counts.memories.len() as u32;
    let tables = counts.tables as u32;
    let big = 256 * 1024 * 1024usize;
    // Compile once; every probe engine (fresh pooled engine per pool shape) loads the
    // same artefact with `Component::deserialize` (the allocator is not part of the
    // compatibility check), falling back to a compile if it is rejected.
    let art = on_demand_engine()?.precompile_component(&bytes)?;
    let probe = |core_per: u32, max_memory: usize| -> bool {
        let shape = PoolShape {
            instances: 1,
            core_per,
            memories_per: mems,
            tables_per: tables,
            max_memory,
        };
        pooled_engine(shape)
            .and_then(|e| {
                // SAFETY: `art` was produced above by an engine with the same flags.
                let comp = match unsafe { Component::deserialize(&e, &art) } {
                    Ok(c) => c,
                    Err(_) => Component::new(&e, &bytes)?,
                };
                try_instance(&e, &comp)
            })
            .is_ok()
    };
    let min_core = bisect(1, 400, |k| probe(k as u32, big)).map(|v| v as u32);
    let Some(core_per) = min_core else {
        bail!("no core-instance limit up to 400 instantiates");
    };
    let min_mem = bisect(1, (big as u64) / PAGE, |pages| {
        probe(core_per, (pages * PAGE) as usize)
    })
    .map(|p| p * PAGE);
    let max_memory = min_mem.map_or(big, |m| m as usize);

    // N live instances on a pool sized exactly from the measured shape.
    let shape = PoolShape {
        instances: n,
        core_per,
        memories_per: mems,
        tables_per: tables,
        max_memory,
    };
    let engine = pooled_engine(shape)?;
    let component = load(&engine, wasm)?;
    let p = pre(&engine, &component)?;
    let rss_before = rss_bytes();
    let mut live = Vec::with_capacity(n as usize);
    let mut errors = Vec::new();
    for _ in 0..n {
        let mut s = store(&engine, ACTIVATION_FUEL)?;
        match p.instantiate(&mut s) {
            Ok(w) => {
                if let Err(e) = w.ostro_runtime_workflows().call_describe(&mut s) {
                    errors.push(format!("describe: {e}"));
                }
                live.push((s, w));
            }
            Err(e) => {
                errors.push(format!("instantiate: {e}"));
                break;
            }
        }
    }
    let rss_live = rss_bytes();
    let got = live.len();
    // One more must fail: proves the pool is exactly N wide.
    let extra = {
        let mut s = store(&engine, ACTIVATION_FUEL)?;
        p.instantiate(&mut s).err().map(|e| format!("{e}"))
    };
    drop(live);
    let rss_after = rss_bytes();
    let delta = rss_live.saturating_sub(rss_before);
    emit(
        c,
        "pool",
        obj(json!({
            "n": n,
            "live": got,
            "min_core_instances_per_component": core_per,
            "static_core_instances": counts.core_instances_instantiate,
            "total_core_instances": n * core_per,
            "memories_per_component": mems,
            "tables_per_component": tables,
            "min_max_memory_size_bytes": min_mem,
            "rss_before_bytes": rss_before,
            "rss_live_bytes": rss_live,
            "rss_after_drop_bytes": rss_after,
            "rss_delta_per_instance_bytes": delta / u64::from(n.max(1)),
            "errors": errors,
            "instance_n_plus_1": extra.unwrap_or_else(|| "UNEXPECTEDLY SUCCEEDED".into()),
        })),
    )
}

fn cmd_instantiate(c: &Common, wasm: &Path, iters: usize) -> Result<()> {
    let bytes = std::fs::read(wasm)?;
    let counts = count_wasm(&bytes)?;
    // Generous pool (production shape: pooling allocator, AOT artefact via mmap).
    let shape = PoolShape {
        instances: 16,
        core_per: (counts.core_instances_instantiate as u32).max(1) + 8,
        memories_per: counts.memories.len() as u32,
        tables_per: counts.tables as u32,
        max_memory: 256 * 1024 * 1024,
    };
    let engine = pooled_engine(shape)?;
    let t = Instant::now();
    let component = load(&engine, wasm)?;
    let compile_and_load = t.elapsed();
    let p = pre(&engine, &component)?;

    let mut s = store(&engine, ACTIVATION_FUEL)?;
    let t = Instant::now();
    let w = p.instantiate(&mut s)?;
    let first_inst = t.elapsed();
    let t = Instant::now();
    let metas = w.ostro_runtime_workflows().call_describe(&mut s)?;
    let first_call = t.elapsed();
    drop(s);

    let mut inst = Vec::with_capacity(iters);
    let mut call = Vec::with_capacity(iters);
    for _ in 0..iters {
        let mut s = store(&engine, ACTIVATION_FUEL)?;
        let t = Instant::now();
        let w = p.instantiate(&mut s)?;
        inst.push(t.elapsed());
        let t = Instant::now();
        w.ostro_runtime_workflows().call_describe(&mut s)?;
        call.push(t.elapsed());
    }
    emit(
        c,
        "instantiate",
        obj(json!({
            "iters": iters,
            "allocator": "pooling",
            "aot_compile_and_load_ms": compile_and_load.as_secs_f64() * 1e3,
            "first_instantiate_us": us(first_inst),
            "first_describe_us": us(first_call),
            "describe_names": metas.iter().map(|m| m.name.clone()).collect::<Vec<_>>(),
            "instantiate": stats(inst),
            "first_call_describe": stats(call),
        })),
    )
}

fn cmd_aot(c: &Common, wasm: &Path) -> Result<()> {
    let bytes = std::fs::read(wasm)?;
    let engine = on_demand_engine()?;
    let t = Instant::now();
    let art = engine.precompile_component(&bytes)?;
    let compile = t.elapsed();
    emit(
        c,
        "aot",
        obj(json!({
            "wasm_bytes": bytes.len(),
            "cwasm_bytes": art.len(),
            "compile_ms": compile.as_secs_f64() * 1e3,
            "parallel_compilation": true,
        })),
    )
}

fn run_once(engine: &Engine, p: &WorkflowPre<Host>, workflow: &str) -> Result<Value> {
    let mut s = store(engine, ACTIVATION_FUEL)?;
    let w = p.instantiate(&mut s)?;
    let input = types::Payload {
        content_type: "application/json".into(),
        data: b"{}".to_vec(),
    };
    let r = w
        .ostro_runtime_workflows()
        .call_run(&mut s, workflow, &input);
    let used = ACTIVATION_FUEL - s.get_fuel()?;
    let outcome = match r {
        Ok(Ok(RunResult::Completed(p))) => json!({"completed": String::from_utf8_lossy(&p.data)}),
        Ok(Ok(RunResult::ContinueAsNew(_))) => json!("continue-as-new"),
        Ok(Err(f)) => json!({"err": format!("{f:?}")}),
        Err(e) => json!({"trap": format!("{}", e.root_cause())}),
    };
    Ok(json!({
        "workflow": workflow,
        "fuel_used": used,
        "fraction_of_activation_fuel": used as f64 / ACTIVATION_FUEL as f64,
        "outcome": outcome,
        "steps_executed": s.data().steps_executed,
        "waits": s.data().waits,
    }))
}

fn cmd_fuel(c: &Common, wasm: &Path, workflows: &[String]) -> Result<()> {
    let engine = on_demand_engine()?;
    let component = load(&engine, wasm)?;
    let p = pre(&engine, &component)?;
    let mut s = store(&engine, ACTIVATION_FUEL)?;
    let w = p.instantiate(&mut s)?;
    w.ostro_runtime_workflows().call_describe(&mut s)?;
    let describe_fuel = ACTIVATION_FUEL - s.get_fuel()?;
    let mut runs = Vec::new();
    for wf in workflows {
        // twice: fresh instance each time, numbers must match (determinism of fuel)
        let a = run_once(&engine, &p, wf)?;
        let b = run_once(&engine, &p, wf)?;
        let same = a["fuel_used"] == b["fuel_used"];
        runs.push(json!({"first": a, "fuel_identical_on_fresh_instance": same}));
    }
    emit(
        c,
        "fuel",
        obj(json!({
            "activation_fuel_default": ACTIVATION_FUEL,
            "describe_fuel": describe_fuel,
            "runs": runs,
        })),
    )
}

// ---------------------------------------------------------------------------
// Real engine (TS prototype)
// ---------------------------------------------------------------------------

fn run_engine(c: &Common, wasm: &Path, workflows: &[String]) -> Result<()> {
    use ostro_engine::config::EngineConfig;
    use ostro_engine::runtime::{Runtime, RuntimeOptions};
    use ostro_engine::sink::{ActivationOutcome, RunRef};
    use ostro_engine::testkit::{
        DEFAULT_ACCEPTED_AT, FakeClock, Rule, RunSpec, Script, drive_to_completion, replay_journal,
    };
    use ostro_types::journal::Payload;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let runtime = Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [9; 32],
                ..RuntimeOptions::default()
            },
        )
        .map_err(|e| anyhow!("runtime: {e}"))?;
        let bytes = std::fs::read(wasm)?;
        let prepared = match runtime.prepare_cached(&bytes).await {
            Ok(p) => p,
            Err(e) => {
                let f = obj(json!({"prepare_error": format!("{e}"), "validated": false}));
                emit(c, "engine", f)?;
                return Ok(());
            }
        };
        let metas = runtime
            .describe(&prepared)
            .await
            .map_err(|e| anyhow!("describe: {e}"))?;
        let names: Vec<String> = metas.iter().map(|m| m.name.clone()).collect();
        for wf in workflows {
            let run = RunRef {
                partition: 1,
                run_id: format!("run-s9-{wf}"),
            };
            let script = if wf == "s9-probe" {
                // Signal-vs-1h-timer race: timers never fire, the signal arrives as an
                // input after the first eviction.
                Script::new().fire_timers(false).rule(Rule::Signal {
                    name: "go".into(),
                    payload: Payload {
                        content_type: "application/json".into(),
                        data: br#"{"go":true}"#.to_vec(),
                    },
                })
            } else {
                Script::new()
            };
            let d = drive_to_completion(
                &runtime,
                &prepared,
                RunSpec::new(run.clone(), wf.as_str(), br#"{"x":2}"#),
                script,
            )
            .await;
            let (replayed, appended) =
                replay_journal(&runtime, &prepared, run, d.journal.clone()).await;
            let summary = summarize(&format!("{:?}", d.outcome));
            let replay_summary = summarize(&format!("{replayed:?}"));
            let output = match &d.outcome {
                ActivationOutcome::Completed { output } => {
                    Some(String::from_utf8_lossy(&output.data).to_string())
                }
                _ => None,
            };
            let kinds: Vec<String> = d
                .journal
                .iter()
                .map(|e| format!("{:?}", e.body.kind()))
                .collect();
            let f = obj(json!({
                "validated": true,
                "describe": names,
                "workflow": wf,
                "outcome": summary,
                "output": output,
                "activations": d.activations,
                "activation_outcomes": d.outcomes.iter().map(|o| summarize(&format!("{o:?}"))).collect::<Vec<_>>(),
                "replay_outcome": replay_summary,
                "journal_kinds": kinds,
                "replay_same_outcome": replayed == d.outcome,
                "replay_appended": appended,
            }));
            emit(c, "engine", f)?;
        }
        Ok::<(), wasmtime::Error>(())
    })
}

/// Debug text of an activation outcome without payload byte arrays and backtraces:
/// `Completed { .. }` keeps only its kind, traps keep the trap reason and the export frame.
fn summarize(dbg: &str) -> String {
    if dbg.starts_with("Completed") {
        return "Completed".into();
    }
    if dbg.contains("Trap(") {
        let reason = dbg
            .rsplit_once("wasm trap:")
            .map(|(_, r)| r.trim_end_matches(['"', ')']).trim().to_string())
            .or_else(|| dbg.lines().last().map(str::to_string))
            .unwrap_or_default();
        let export = dbg
            .split("!")
            .find(|f| f.starts_with("ostro:runtime/workflows"))
            .map(|f| f.split(':').take(2).collect::<Vec<_>>().join(":"));
        let head = dbg.split('(').next().unwrap_or("");
        return format!("{head}(Trap: {reason}{})", export.map(|e| format!(" in {e}")).unwrap_or_default());
    }
    dbg.chars().take(400).collect()
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(about = "Ostro spike S9 harness (wasmtime 50.0.2)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Inspect {
        wasm: PathBuf,
        #[command(flatten)]
        c: Common,
    },
    Pool {
        wasm: PathBuf,
        #[arg(long, default_value_t = 50)]
        n: u32,
        #[command(flatten)]
        c: Common,
    },
    Instantiate {
        wasm: PathBuf,
        #[arg(long, default_value_t = 200)]
        iters: usize,
        #[command(flatten)]
        c: Common,
    },
    Aot {
        wasm: PathBuf,
        #[command(flatten)]
        c: Common,
    },
    Fuel {
        wasm: PathBuf,
        #[arg(long = "workflow", required = true)]
        workflows: Vec<String>,
        #[command(flatten)]
        c: Common,
    },
    Engine {
        wasm: PathBuf,
        #[arg(long = "workflow", required = true)]
        workflows: Vec<String>,
        #[command(flatten)]
        c: Common,
    },
    /// Append a build-time observation (`--probe` required): `--field k=v` (string) and
    /// `--json k=<json>`.
    Note {
        #[arg(long = "field")]
        fields: Vec<String>,
        #[arg(long = "json")]
        jsons: Vec<String>,
        #[command(flatten)]
        c: Common,
    },
}

fn cmd_note(c: &Common, fields: &[String], jsons: &[String]) -> Result<()> {
    if c.probe.is_none() {
        bail!("note needs --probe");
    }
    let mut m = Map::new();
    for f in fields {
        let (k, v) = f.split_once('=').ok_or_else(|| anyhow!("--field {f}: k=v"))?;
        m.insert(k.into(), json!(v));
    }
    for f in jsons {
        let (k, v) = f.split_once('=').ok_or_else(|| anyhow!("--json {f}: k=json"))?;
        let v: Value = serde_json::from_str(v).map_err(|e| anyhow!("--json {k}: {e}"))?;
        m.insert(k.into(), v);
    }
    emit(c, "probe", m)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.cmd {
        Cmd::Inspect { wasm, c } => cmd_inspect(c, wasm),
        Cmd::Pool { wasm, n, c } => cmd_pool(c, wasm, *n),
        Cmd::Instantiate { wasm, iters, c } => cmd_instantiate(c, wasm, *iters),
        Cmd::Aot { wasm, c } => cmd_aot(c, wasm),
        Cmd::Fuel { wasm, workflows, c } => cmd_fuel(c, wasm, workflows),
        Cmd::Engine { wasm, workflows, c } => run_engine(c, wasm, workflows),
        Cmd::Note { fields, jsons, c } => cmd_note(c, fields, jsons),
    }
}
