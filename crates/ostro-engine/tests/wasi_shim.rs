//! ENG-02: Rust wasm32-wasip2 std guests run on Ostro's own deterministic WASI shim.

mod common;

use std::cell::Cell;
use std::time::{Duration, Instant};

use ostro_engine::env::{GuestEnv, RandomStream, StdStream};
use ostro_engine::error::GuestExit;
use ostro_engine::host::bindings::Workflow;
use ostro_engine::host::bindings::exports::ostro::runtime::workflows::RunResult;
use ostro_engine::host::bindings::ostro::runtime::types::Payload;
use wasmtime::Store;
use wasmtime::component::{Component, Linker, ResourceTable};

const WALL_SECS: u64 = 1_700_000_000;
const MONO_STEP_NS: u64 = 1_000;
const STDIO_BUDGET: u64 = 64 * 1024;

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministic test environment: fixed wall time, counting monotonic clock, one
/// seeded PRNG per stream, and a stdio log with a 64 KiB per-stream budget.
struct TestEnv {
    table: ResourceTable,
    seed: u64,
    mono: Cell<u64>,
    rng: [u64; 4],
    streams_seen: Vec<RandomStream>,
    log: Vec<(StdStream, Vec<u8>)>,
}

impl TestEnv {
    fn new(seed: u64) -> Self {
        let mut rng = [0u64; 4];
        for (i, s) in rng.iter_mut().enumerate() {
            *s = seed ^ (0x1000 * (i as u64 + 1));
        }
        Self {
            table: ResourceTable::new(),
            seed,
            mono: Cell::new(0),
            rng,
            streams_seen: Vec::new(),
            log: Vec::new(),
        }
    }

    fn written(&self, stream: StdStream) -> Vec<u8> {
        self.log
            .iter()
            .filter(|(s, _)| *s == stream)
            .flat_map(|(_, b)| b.iter().copied())
            .collect()
    }
}

impl GuestEnv for TestEnv {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn wall_now(&self) -> (u64, u32) {
        (WALL_SECS, 0)
    }

    fn monotonic_now_ns(&self) -> u64 {
        let v = self.mono.get() + MONO_STEP_NS;
        self.mono.set(v);
        v
    }

    fn fill_random(&mut self, stream: RandomStream, buf: &mut [u8]) {
        self.streams_seen.push(stream);
        let state = &mut self.rng[stream as usize - 1];
        for chunk in buf.chunks_mut(8) {
            let v = splitmix64(state).to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }

    fn insecure_seed(&self) -> (u64, u64) {
        let mut s = self.seed ^ 0xDEAD_BEEF;
        (splitmix64(&mut s), splitmix64(&mut s))
    }

    fn write_stdio(&mut self, stream: StdStream, bytes: &[u8]) {
        let n = (self.stdio_budget(stream) as usize).min(bytes.len());
        self.log.push((stream, bytes[..n].to_vec()));
    }

    fn stdio_budget(&self, stream: StdStream) -> u64 {
        STDIO_BUDGET.saturating_sub(self.written(stream).len() as u64)
    }
}

struct Outcome {
    result: wasmtime::Result<String>,
    env: TestEnv,
    /// Wall time spent inside `call_run` only (excludes compilation/instantiation).
    call_elapsed: Duration,
}

/// Instantiate wasi-probe on the shim and call `run(probe)`.
fn run_probe(probe: &str, seed: u64) -> Outcome {
    let wasm = common::guest_wasm("wasi-probe");
    let engine = common::test_engine();
    let component = Component::from_binary(&engine, &wasm).expect("component");
    let mut linker: Linker<TestEnv> = Linker::new(&engine);
    ostro_engine::host::wasi::add_to_linker(&mut linker).expect("shim");
    linker
        .define_unknown_imports_as_traps(&component)
        .expect("traps");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(async {
        let mut store = Store::new(&engine, TestEnv::new(seed));
        store.set_fuel(u64::MAX).expect("fuel");
        store.set_epoch_deadline(u64::MAX);
        let mut call_elapsed = Duration::ZERO;
        let result = async {
            let wf = Workflow::instantiate_async(&mut store, &component, &linker).await?;
            let input = Payload {
                content_type: "text/plain".into(),
                data: Vec::new(),
            };
            let start = Instant::now();
            let res = wf
                .ostro_runtime_workflows()
                .call_run(&mut store, probe, &input)
                .await;
            call_elapsed = start.elapsed();
            let res = res?;
            match res {
                Ok(RunResult::Completed(out)) => Ok(String::from_utf8(out.data)?),
                other => Err(wasmtime::Error::msg(format!("unexpected: {other:?}"))),
            }
        }
        .await;
        Outcome {
            result,
            env: store.into_data(),
            call_elapsed,
        }
    })
}

fn ok(probe: &str, seed: u64) -> (String, TestEnv) {
    let o = run_probe(probe, seed);
    match o.result {
        Ok(s) => (s, o.env),
        Err(e) => panic!("probe {probe} failed: {e:?}"),
    }
}

#[test]
fn wasi_shim_time() {
    let (out, env) = ok("time", 1);
    assert_eq!(
        out,
        format!("{{\"secs\":{WALL_SECS},\"elapsed_ns\":{MONO_STEP_NS}}}")
    );
    assert!(
        env.mono.get() >= 2 * MONO_STEP_NS,
        "Instant::now uses the env"
    );
}

#[test]
fn wasi_shim_hashmap_seeded() {
    let (a1, _) = ok("hashmap", 1);
    let (a2, _) = ok("hashmap", 1);
    let (b, _) = ok("hashmap", 2);
    assert_eq!(a1.len(), 26);
    assert_eq!(a1, a2, "same seed -> same HashMap order");
    assert_ne!(a1, b, "different seed -> different HashMap order");
}

#[test]
fn wasi_shim_rand() {
    let (a1, env) = ok("rand", 1);
    let (a2, _) = ok("rand", 1);
    let (b, _) = ok("rand", 2);
    assert_eq!(a1.split(',').count(), 4);
    assert_eq!(a1, a2, "same seed -> same draws");
    assert_ne!(a1, b, "different seed -> different draws");
    assert!(
        env.streams_seen.contains(&RandomStream::WasiRandom),
        "rand draws from WasiRandom, saw {:?}",
        env.streams_seen
    );
}

#[test]
fn wasi_shim_stdio() {
    let (out, env) = ok("print", 1);
    assert_eq!(out, "printed");
    assert_eq!(env.written(StdStream::Stdout), b"hello\n");
    assert_eq!(env.written(StdStream::Stderr), b"oops\n");

    let (out, env) = ok("print-big", 1);
    assert_eq!(out, "printed-big", "guest completes past its stdio budget");
    let stdout = env.written(StdStream::Stdout);
    assert_eq!(stdout.len() as u64, STDIO_BUDGET, "excess output dropped");
    assert!(stdout.iter().all(|&b| b == b'x' || b == b'\n'));
}

#[test]
fn wasi_shim_env_empty() {
    let (out, _) = ok("env", 1);
    // The shim's `initial-cwd` is None; wasi-libc then falls back to "/".
    assert_eq!(out, "{\"vars\":0,\"args\":0,\"cwd\":\"/\"}");
}

#[test]
fn wasi_shim_sleep_no_block() {
    // The probe calls std::thread::sleep(Duration::from_secs(3600)).
    let o = run_probe("sleep", 1);
    assert_eq!(o.result.expect("sleep probe"), "slept");
    assert!(
        o.call_elapsed < Duration::from_secs(1),
        "sleep must not block in real time (took {:?})",
        o.call_elapsed
    );
}

#[test]
fn wasi_shim_exit() {
    let o = run_probe("exit", 1);
    let err = o.result.expect_err("exit must trap");
    let exit = err
        .downcast_ref::<GuestExit>()
        .unwrap_or_else(|| panic!("expected GuestExit, got {err:?}"));
    assert_eq!(exit.0, 3);
}

#[test]
fn wasi_shim_stdin_eof() {
    let (out, _) = ok("stdin", 1);
    assert_eq!(out, "{\"stdin_len\":0}");
}
