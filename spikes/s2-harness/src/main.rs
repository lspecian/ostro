//! Spike S2 measurement harness (Wasmtime 50.0.2). Throwaway.
//!
//! One subcommand per measurement, each intended to run in its own process so
//! that Cranelift compile memory never pollutes instance-memory numbers
//! (research Pitfall 4). Every subcommand prints exactly ONE JSON line to
//! stdout so plan 01-05 can collect results mechanically.
//!
//! Unknown imports (the Rust wasm32-wasip2 guest imports `wasi:*`) are
//! defined as traps via `Linker::define_unknown_imports_as_traps`; no
//! wasmtime-wasi. A probe that touches WASI therefore traps, and the trap is
//! reported as data.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};
use wasmtime::component::{Component, Linker, Resource, ResourceTable, bindgen};
use wasmtime::{
    Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store, StoreLimits,
    StoreLimitsBuilder,
};

bindgen!({ path: "../wit", world: "spike" });

use ostro::spike::journal::{self, StepStart};

// ---------------------------------------------------------------------------
// Host state
// ---------------------------------------------------------------------------

pub struct StepRep {
    seq: u64,
}

/// Records the largest linear-memory size any memory in the store asked for
/// (initial allocation included). Always allows growth.
struct Lim {
    max_memory_desired: usize,
    inner: StoreLimits,
}

impl wasmtime::ResourceLimiter for Lim {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.max_memory_desired = self.max_memory_desired.max(desired);
        self.inner.memory_growing(current, desired, maximum)
    }
    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.inner.table_growing(current, desired, maximum)
    }
}

struct Host {
    table: ResourceTable,
    lim: Lim,
    next: u64,
    rng: u64,
    step_drops: u64,
    step_completes: u64,
}

impl Host {
    fn new(seed: u64) -> Self {
        Host {
            table: ResourceTable::new(),
            lim: Lim {
                max_memory_desired: 0,
                inner: StoreLimitsBuilder::new().build(),
            },
            next: 0,
            rng: seed,
            step_drops: 0,
            step_completes: 0,
        }
    }
}

impl journal::HostStep for Host {
    fn seq(&mut self, s: Resource<journal::Step>) -> u64 {
        let r: Resource<StepRep> = Resource::new_borrow(s.rep());
        self.table.get(&r).map(|s| s.seq).unwrap_or(u64::MAX)
    }
    fn complete(&mut self, _s: Resource<journal::Step>, _o: String) -> bool {
        self.step_completes += 1;
        true
    }
    fn drop(&mut self, s: Resource<journal::Step>) -> wasmtime::Result<()> {
        let r: Resource<StepRep> = Resource::new_own(s.rep());
        self.table.delete(r)?;
        self.step_drops += 1;
        Ok(())
    }
}

impl journal::Host for Host {
    fn begin_step(&mut self, _n: String) -> StepStart {
        self.next += 1;
        let r = self
            .table
            .push(StepRep { seq: self.next })
            .expect("resource table full");
        StepStart::Execute(Resource::new_own(r.rep()))
    }
    fn begin_step_u64(&mut self, _n: String) -> u64 {
        self.next += 1;
        self.next
    }
    fn complete_u64(&mut self, _h: u64, _o: String) -> bool {
        self.step_completes += 1;
        true
    }
}

impl ostro::spike::context::Host for Host {
    fn now_ms(&mut self) -> u64 {
        1_700_000_000_000
    }
    /// splitmix64 seeded from the harness `--seed`.
    fn random_u64(&mut self) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

struct D;
impl wasmtime::component::HasData for D {
    type Data<'a> = &'a mut Host;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(about = "Ostro spike S2 measurement harness (wasmtime 50.0.2)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct EngineOpts {
    /// Use the pooling instance allocator instead of on-demand.
    #[arg(long)]
    pooling: bool,
    /// Pooling: total component instances (default: wasmtime default).
    #[arg(long)]
    pool_total_component_instances: Option<u32>,
    /// Pooling: total core instances.
    #[arg(long)]
    pool_total_core_instances: Option<u32>,
    /// Pooling: total linear memories.
    #[arg(long)]
    pool_total_memories: Option<u32>,
    /// Pooling: total tables.
    #[arg(long)]
    pool_total_tables: Option<u32>,
    /// Pooling: max linear memory size in bytes.
    #[arg(long)]
    pool_max_memory_size: Option<usize>,
    /// Pooling: max core instances per component.
    #[arg(long)]
    pool_max_core_instances_per_component: Option<u32>,
    /// Pooling: max memories per component.
    #[arg(long)]
    pool_max_memories_per_component: Option<u32>,
    /// Pooling: max tables per component.
    #[arg(long)]
    pool_max_tables_per_component: Option<u32>,
    /// Pooling: max core instance (vmctx) size in bytes.
    #[arg(long)]
    pool_max_core_instance_size: Option<usize>,
    /// Pooling: max component instance size in bytes.
    #[arg(long)]
    pool_max_component_instance_size: Option<usize>,
}

#[derive(Subcommand)]
enum Cmd {
    /// AOT-compile a component and report sizes/time.
    Compile { wasm: PathBuf, out: PathBuf },
    /// Instantiation latency from an mmap'd AOT artefact.
    Instantiate {
        cwasm: PathBuf,
        #[command(flatten)]
        eng: EngineOpts,
        #[arg(long, default_value_t = 1000)]
        n: usize,
    },
    /// Declared linear memory and RSS per live instance.
    Memory {
        cwasm: PathBuf,
        #[command(flatten)]
        eng: EngineOpts,
        #[arg(long, default_value_t = 50)]
        n: usize,
    },
    /// Ambient-nondeterminism probes, reseed and dropped-step observation.
    Probes {
        cwasm: PathBuf,
        #[command(flatten)]
        eng: EngineOpts,
    },
    /// List component imports and exports.
    Imports { wasm: PathBuf },
    /// One cold start in this process: engine -> deserialize -> instantiate -> run.
    Cold {
        cwasm: PathBuf,
        #[command(flatten)]
        eng: EngineOpts,
    },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn base_config() -> Config {
    let mut cfg = Config::new();
    cfg.cranelift_nan_canonicalization(true)
        .relaxed_simd_deterministic(true);
    cfg
}

/// Builds the engine; returns it plus a JSON description of the allocator
/// settings actually used (pooling values are printed so 01-05 can report them).
fn build_engine(o: &EngineOpts) -> wasmtime::Result<(Engine, Value)> {
    let mut cfg = base_config();
    let desc = if o.pooling {
        let mut p = PoolingAllocationConfig::default();
        let mut set = serde_json::Map::new();
        macro_rules! opt {
            ($field:ident, $method:ident) => {
                if let Some(v) = o.$field {
                    p.$method(v);
                    set.insert(stringify!($method).into(), json!(v));
                }
            };
        }
        opt!(pool_total_component_instances, total_component_instances);
        opt!(pool_total_core_instances, total_core_instances);
        opt!(pool_total_memories, total_memories);
        opt!(pool_total_tables, total_tables);
        opt!(pool_max_memory_size, max_memory_size);
        opt!(pool_max_core_instances_per_component, max_core_instances_per_component);
        opt!(pool_max_memories_per_component, max_memories_per_component);
        opt!(pool_max_tables_per_component, max_tables_per_component);
        opt!(pool_max_core_instance_size, max_core_instance_size);
        opt!(pool_max_component_instance_size, max_component_instance_size);
        cfg.allocation_strategy(InstanceAllocationStrategy::Pooling(p));
        json!({ "strategy": "pooling", "overrides": set })
    } else {
        cfg.allocation_strategy(InstanceAllocationStrategy::OnDemand);
        json!({ "strategy": "on-demand" })
    };
    Ok((Engine::new(&cfg)?, desc))
}

/// Loads an AOT artefact via `deserialize_file` (mmap; enables CoW memory init).
///
/// SAFETY: `deserialize_file` trusts the file to be a valid artefact produced
/// by a compatible Wasmtime engine and requires that it is not modified while
/// mapped. In this spike the file is produced moments earlier by our own
/// `compile` subcommand with the same wasmtime version and config, and nothing
/// else writes to it, so both preconditions hold.
fn load(engine: &Engine, path: &Path) -> wasmtime::Result<Component> {
    unsafe { Component::deserialize_file(engine, path) }
}

fn linker_for(engine: &Engine, component: &Component) -> wasmtime::Result<Linker<Host>> {
    let mut linker: Linker<Host> = Linker::new(engine);
    Spike::add_to_linker::<Host, D>(&mut linker, |h| h)?;
    linker.define_unknown_imports_as_traps(component)?;
    Ok(linker)
}

fn new_store(engine: &Engine, seed: u64) -> Store<Host> {
    let mut store = Store::new(engine, Host::new(seed));
    store.limiter(|h| &mut h.lim);
    store
}

fn proc_status_kb(key: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|l| l.starts_with(key))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}
fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
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
    json!({
        "n": v.len(),
        "p50_us": pct(&v, 0.50),
        "p99_us": pct(&v, 0.99),
        "max_us": v.last().map(|d| us(*d)).unwrap_or(f64::NAN),
    })
}

/// Turns a call result into JSON; traps become `{"trap": "..."}`.
fn outcome(r: wasmtime::Result<String>) -> Value {
    match r {
        Ok(s) => match serde_json::from_str::<Value>(&s) {
            Ok(v) => json!({ "ok": v }),
            Err(_) => json!({ "ok": s }),
        },
        Err(e) => {
            // Root cause = the actual trap reason (e.g. `unreachable`, or the
            // unknown-import trap message); the outer context is just
            // "error while executing at wasm backtrace".
            let root = e.root_cause().to_string();
            let trap_code = e.downcast_ref::<wasmtime::Trap>().map(|t| t.to_string());
            // First few backtrace frames help tell an `unreachable` stub from
            // an unknown-import trap.
            let frames: Vec<String> = format!("{e:?}")
                .lines()
                .map(str::trim)
                .filter(|l| l.chars().next().is_some_and(|c| c.is_ascii_digit()))
                .take(4)
                .map(str::to_string)
                .collect();
            json!({ "trap": root.lines().next().unwrap_or(""), "trap_code": trap_code, "frames": frames })
        }
    }
}

fn emit(v: Value) {
    println!("{v}");
}

// ---------------------------------------------------------------------------
// Subcommands
// ---------------------------------------------------------------------------

fn cmd_compile(wasm: &Path, out: &Path) -> wasmtime::Result<Value> {
    use std::io::Write;
    let engine = Engine::new(&base_config())?;
    let bytes = std::fs::read(wasm)?;
    let t = Instant::now();
    let cwasm = engine.precompile_component(&bytes)?;
    let compile = t.elapsed();
    std::fs::write(out, &cwasm)?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(&bytes)?;
    let gz_bytes = gz.finish()?.len();
    Ok(json!({
        "cmd": "compile",
        "wasm": wasm.display().to_string(),
        "wasm_bytes": bytes.len(),
        "gzip_bytes": gz_bytes,
        "cwasm_bytes": cwasm.len(),
        "compile_ms": ms(compile),
        // Process peak RSS including Cranelift; caller may also wrap with
        // `/usr/bin/time -f %M`.
        "vm_hwm_kb": proc_status_kb("VmHWM:"),
    }))
}

fn cmd_instantiate(cwasm: &Path, eng: &EngineOpts, n: usize) -> wasmtime::Result<Value> {
    let (engine, alloc) = build_engine(eng)?;
    let t = Instant::now();
    let component = load(&engine, cwasm)?;
    let deser = t.elapsed();
    let linker = linker_for(&engine, &component)?;
    let pre = SpikePre::new(linker.instantiate_pre(&component)?)?;

    // First instantiate (includes lazy init) and first call, reported separately.
    let mut store = new_store(&engine, 1);
    let t = Instant::now();
    let inst = pre.instantiate(&mut store)?;
    let first_inst = t.elapsed();
    let t = Instant::now();
    let first_out = inst.call_run(&mut store, "x");
    let first_call = t.elapsed();
    let first_out = outcome(first_out);

    // Steady-state calls of `run` on one warm instance.
    let mut steady_calls = Vec::with_capacity(100);
    if first_out.get("ok").is_some() {
        for _ in 0..100 {
            let t = Instant::now();
            inst.call_run(&mut store, "x")?;
            steady_calls.push(t.elapsed());
        }
    }
    drop(store);

    // n further instantiations, fresh Store each; also first `run` per instance.
    let mut insts = Vec::with_capacity(n);
    let mut per_inst_first_call = Vec::with_capacity(n);
    for _ in 0..n {
        let mut store = new_store(&engine, 1);
        let t = Instant::now();
        let inst = pre.instantiate(&mut store)?;
        insts.push(t.elapsed());
        let t = Instant::now();
        if inst.call_run(&mut store, "x").is_ok() {
            per_inst_first_call.push(t.elapsed());
        }
    }

    Ok(json!({
        "cmd": "instantiate",
        "cwasm": cwasm.display().to_string(),
        "allocator": alloc,
        "deserialize_file_ms": ms(deser),
        "first_instantiate_us": us(first_inst),
        "first_call_run_us": us(first_call),
        "first_run_output": first_out,
        "instantiate": stats(insts),
        "run_first_call_per_instance": stats(per_inst_first_call),
        "run_steady_same_instance": stats(steady_calls),
    }))
}

fn cmd_memory(cwasm: &Path, eng: &EngineOpts, n: usize) -> wasmtime::Result<Value> {
    let (engine, alloc) = build_engine(eng)?;
    let component = load(&engine, cwasm)?;
    let linker = linker_for(&engine, &component)?;
    let pre = SpikePre::new(linker.instantiate_pre(&component)?)?;

    let rss_before = proc_status_kb("VmRSS:").unwrap_or(0);
    let mut live = Vec::with_capacity(n);
    let mut declared_max = 0usize;
    let mut run_errors = 0usize;
    for _ in 0..n {
        let mut store = new_store(&engine, 1);
        let inst = pre.instantiate(&mut store)?;
        if inst.call_run(&mut store, "x").is_err() {
            run_errors += 1;
        }
        declared_max = declared_max.max(store.data().lim.max_memory_desired);
        live.push((store, inst));
    }
    let rss_live = proc_status_kb("VmRSS:").unwrap_or(0);
    drop(live);
    let rss_after_drop = proc_status_kb("VmRSS:").unwrap_or(0);

    let delta = rss_live.saturating_sub(rss_before);
    Ok(json!({
        "cmd": "memory",
        "cwasm": cwasm.display().to_string(),
        "allocator": alloc,
        "n": n,
        "run_errors": run_errors,
        "declared_linear_memory_max_bytes": declared_max,
        "rss_before_kb": rss_before,
        "rss_live_kb": rss_live,
        "rss_after_drop_kb": rss_after_drop,
        "rss_delta_per_instance_kb": delta as f64 / n.max(1) as f64,
        "rss_retained_after_drop_kb": rss_after_drop.saturating_sub(rss_before),
    }))
}

fn cmd_probes(cwasm: &Path, eng: &EngineOpts) -> wasmtime::Result<Value> {
    let (engine, alloc) = build_engine(eng)?;
    let component = load(&engine, cwasm)?;
    let linker = linker_for(&engine, &component)?;
    let pre = SpikePre::new(linker.instantiate_pre(&component)?)?;

    // Every call gets a fresh instance: a trap poisons the instance.
    let fresh = |seed: u64| -> wasmtime::Result<(Store<Host>, Spike)> {
        let mut store = new_store(&engine, seed);
        let inst = pre.instantiate(&mut store)?;
        Ok((store, inst))
    };

    let mut probes = serde_json::Map::new();
    for name in ["probe-random", "probe-time", "probe-hash", "probe-uuid"] {
        let mut rounds = Vec::new();
        for _ in 0..2 {
            let (mut s, i) = fresh(1)?;
            let r = match name {
                "probe-random" => i.call_probe_random(&mut s),
                "probe-time" => i.call_probe_time(&mut s),
                "probe-hash" => i.call_probe_hash(&mut s),
                _ => i.call_probe_uuid(&mut s),
            };
            rounds.push(outcome(r));
        }
        let identical = rounds[0] == rounds[1];
        probes.insert(name.into(), json!({ "instances": rounds, "identical_across_instances": identical }));
    }

    const SEED_A: u64 = 0xA11CE;
    const SEED_B: u64 = 0xB0B;
    let mut reseeded = serde_json::Map::new();
    for (label, seed, from_host) in [
        ("no_reseed_1", SEED_A, false),
        ("no_reseed_2", SEED_A, false),
        ("seed_a_1", SEED_A, true),
        ("seed_b", SEED_B, true),
        ("seed_a_2", SEED_A, true),
    ] {
        let (mut s, i) = fresh(seed)?;
        reseeded.insert(label.into(), outcome(i.call_run_reseeded(&mut s, from_host)));
    }
    let same_seed_same = reseeded["seed_a_1"] == reseeded["seed_a_2"];
    let diff_seed_diff = reseeded["seed_a_1"] != reseeded["seed_b"];

    let (mut s, i) = fresh(1)?;
    let dropped_out = outcome(i.call_run_dropped_step(&mut s));
    let dropped = json!({
        "output": dropped_out,
        "step_drops": s.data().step_drops,
        "step_completes": s.data().step_completes,
        "live_resources_in_table": s.data().next.saturating_sub(s.data().step_drops),
    });

    // run once too, for the resource round trip and drop accounting.
    let (mut s, i) = fresh(1)?;
    let run_out = outcome(i.call_run(&mut s, "x"));
    let run = json!({
        "output": run_out,
        "step_drops": s.data().step_drops,
        "step_completes": s.data().step_completes,
    });

    Ok(json!({
        "cmd": "probes",
        "cwasm": cwasm.display().to_string(),
        "allocator": alloc,
        "probes": probes,
        "run_reseeded": {
            "results": reseeded,
            "same_host_seed_same_draws": same_seed_same,
            "different_host_seed_different_draws": diff_seed_diff,
        },
        "run_dropped_step": dropped,
        "run": run,
    }))
}

fn cmd_imports(wasm: &Path) -> wasmtime::Result<Value> {
    let engine = Engine::new(&base_config())?;
    let component = Component::from_file(&engine, wasm)?;
    let ty = component.component_type();
    let imports: Vec<String> = ty.imports(&engine).map(|(n, _)| n.to_string()).collect();
    let exports: Vec<String> = ty.exports(&engine).map(|(n, _)| n.to_string()).collect();
    let non_spike: Vec<&String> = imports
        .iter()
        .filter(|n| !n.starts_with("ostro:spike/"))
        .collect();
    Ok(json!({
        "cmd": "imports",
        "wasm": wasm.display().to_string(),
        "imports": imports,
        "non_spike_imports": non_spike,
        "exports": exports,
    }))
}

fn cmd_cold(start: Instant, cwasm: &Path, eng: &EngineOpts) -> wasmtime::Result<Value> {
    let t0 = start.elapsed();
    let (engine, alloc) = build_engine(eng)?;
    let t_engine = start.elapsed();
    let component = load(&engine, cwasm)?;
    let t_deser = start.elapsed();
    let linker = linker_for(&engine, &component)?;
    let pre = SpikePre::new(linker.instantiate_pre(&component)?)?;
    let t_pre = start.elapsed();
    let mut store = new_store(&engine, 1);
    let inst = pre.instantiate(&mut store)?;
    let t_inst = start.elapsed();
    let out = outcome(inst.call_run(&mut store, "x"));
    let t_run = start.elapsed();
    Ok(json!({
        "cmd": "cold",
        "cwasm": cwasm.display().to_string(),
        "allocator": alloc,
        "note": "times are from the first statement of main (exec/ld.so not included)",
        "main_entry_us": us(t0),
        "engine_ms": ms(t_engine - t0),
        "deserialize_file_ms": ms(t_deser - t_engine),
        "linker_instance_pre_ms": ms(t_pre - t_deser),
        "instantiate_ms": ms(t_inst - t_pre),
        "first_run_ms": ms(t_run - t_inst),
        "total_ms": ms(t_run),
        "run_output": out,
    }))
}

fn main() -> anyhow::Result<()> {
    let start = Instant::now();
    let cli = Cli::parse();
    let v = match &cli.cmd {
        Cmd::Compile { wasm, out } => cmd_compile(wasm, out),
        Cmd::Instantiate { cwasm, eng, n } => cmd_instantiate(cwasm, eng, *n),
        Cmd::Memory { cwasm, eng, n } => cmd_memory(cwasm, eng, *n),
        Cmd::Probes { cwasm, eng } => cmd_probes(cwasm, eng),
        Cmd::Imports { wasm } => cmd_imports(wasm),
        Cmd::Cold { cwasm, eng } => cmd_cold(start, cwasm, eng),
    }
    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    emit(v);
    Ok(())
}
