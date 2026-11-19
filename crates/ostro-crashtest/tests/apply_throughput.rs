//! Apply throughput under the read-modify-write (RMW) workload of spike S3 (plan 02-16).
//! `#[ignore]`: slow and machine-dependent. The tests print markdown tables and assert only
//! that the workload was really applied (outcomes Accepted), never a rate. Run on a quiet
//! machine:
//!
//! ```text
//! cargo test --release -p ostro-crashtest --test apply_throughput -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Workload (S3 `--rmw`, adapted to the real command set): 1,000,000 preloaded runs
//! (`StartRun`, then `AttemptGranted` for each), then 200,000 measured commands: 25% start a
//! new run; 75% update an existing run, picked 90% from the 64Ki most recent runs and 10%
//! uniformly. An update is `AttemptGranted` for a run that has no attempt yet, otherwise
//! `EventsAppended` with the run's next sequence number. Every command reads its `runs` row
//! once and rewrites it.
//!
//! - `apply_throughput_machine`: P=1 `Node` (Leader, default configs), the commands go through
//!   `PartitionHandle::submit` (WAL append + group commit fsync + apply thread). Runs are split
//!   across 32 concurrent submitters by `run index % 32` so per-run order is preserved.
//!   Batch latency comes from the machine's `ostro_apply_batch_seconds` histogram (captured
//!   by a test recorder), checkpoint latency from `ostro_state_checkpoint_seconds`.
//! - `apply_throughput_store`: the same commands applied directly with `apply_command` +
//!   `ApplyBatch::commit` (no log), chunked like the machine, with the machine's checkpoint
//!   cadence (`persist(SyncAll)` every 100 ms or 256 chunks). This is the number comparable
//!   to the S3 >= 90k cmds/s state-store rule. `OSTRO_STORE_CACHE_MIB`,
//!   `OSTRO_STORE_MEMTABLE_MIB` and `OSTRO_STORE_WORKERS` override the `StoreConfig` for
//!   tuning experiments.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use ostro_crashtest::{Rng, loadavg};
use ostro_partition::{
    ApplyOutcome, Cursor, Node, NodeConfig, PartitionStore, StoreConfig, apply_command,
};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, ConflictPolicy, EventsAppended, JournalEvent, StartRun,
    encode_command,
};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

const PRELOAD: usize = 1_000_000;
const MEASURED: usize = 200_000;
const RECENT: u64 = 64 * 1024;
const SUBMITTERS: usize = 32;

// --------------------------------------------------------------------- test recorder

/// Captures every histogram sample by metric name (counters/gauges are no-ops).
#[derive(Default)]
struct Samples(Mutex<BTreeMap<String, Vec<f64>>>);

struct Hist {
    name: String,
    samples: Arc<Samples>,
}

impl HistogramFn for Hist {
    fn record(&self, value: f64) {
        if let Ok(mut m) = self.samples.0.lock() {
            m.entry(self.name.clone()).or_default().push(value);
        }
    }
}

struct TestRecorder(Arc<Samples>);

impl Recorder for TestRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }
    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(Hist {
            name: key.name().to_string(),
            samples: Arc::clone(&self.0),
        }))
    }
}

fn samples() -> &'static Arc<Samples> {
    static S: OnceLock<Arc<Samples>> = OnceLock::new();
    S.get_or_init(|| {
        let s = Arc::new(Samples::default());
        metrics::set_global_recorder(TestRecorder(Arc::clone(&s)))
            .unwrap_or_else(|_| panic!("global recorder already set"));
        s
    })
}

fn take(name: &str) -> Vec<f64> {
    samples()
        .0
        .lock()
        .map(|mut m| m.remove(name).unwrap_or_default())
        .unwrap_or_default()
}

// ------------------------------------------------------------------------ workload

#[derive(Clone)]
struct RunSt {
    id: RunId,
    granted: bool,
    seq: u64,
}

/// Generates the workload sequentially, tracking per-run state as if every command is
/// Accepted (the tests assert that it is).
struct Gen {
    rng: Rng,
    runs: Vec<RunSt>,
    epoch: u32,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

impl Gen {
    fn new(seed: u64, epoch: u32) -> Self {
        Self {
            rng: Rng::new(seed),
            runs: Vec::with_capacity(PRELOAD + MEASURED),
            epoch,
        }
    }

    fn start(&mut self) -> (usize, Command) {
        let id = RunId::new_v7(PartitionId(0));
        self.runs.push(RunSt {
            id,
            granted: false,
            seq: 0,
        });
        let cmd = Command::start_run(StartRun {
            run_id: id.as_bytes().to_vec(),
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "q".into(),
            input: Some(PayloadRef::inline(Bytes::from_static(b"input"))),
            idempotency_key: Vec::new(),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            concurrency_key: String::new(),
            priority: 0,
            ready_at_ms: 0,
            requested_at_ms: now_ms(),
            parent: None,
            rng_seed: vec![7; 32],
            ..Default::default()
        });
        (self.runs.len() - 1, cmd)
    }

    fn update(&mut self, i: usize) -> (usize, Command) {
        let token = Some(AttemptTokenMsg {
            epoch: self.epoch,
            n: 1,
        });
        let r = &mut self.runs[i];
        let cmd = if r.granted {
            let seq = r.seq;
            r.seq += 1;
            Command::events_appended(EventsAppended {
                run_id: r.id.as_bytes().to_vec(),
                token,
                from_seq: seq,
                events: vec![JournalEvent {
                    seq,
                    kind: 16,
                    payload: Some(PayloadRef::inline(Bytes::from_static(b"event"))),
                }],
                derived: Vec::new(),
            })
        } else {
            r.granted = true;
            Command::attempt_granted(AttemptGranted {
                run_id: r.id.as_bytes().to_vec(),
                token,
                after_failure: false,
            })
        };
        (i, cmd)
    }

    /// One measured RMW command.
    fn rmw(&mut self) -> (usize, Command) {
        if self.rng.chance(25) {
            return self.start();
        }
        let n = self.runs.len() as u64;
        let i = if self.rng.chance(90) {
            n - 1 - self.rng.below(RECENT.min(n))
        } else {
            self.rng.below(n)
        };
        self.update(i as usize)
    }
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    let i = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[i]
}

fn load() -> String {
    loadavg().map_or_else(|| "?".into(), |l| format!("{l:.2}"))
}

fn ms(s: f64) -> String {
    format!("{:.2}", s * 1000.0)
}

// ------------------------------------------------------------------- machine level

type Plan = Vec<Vec<Command>>;

/// Split `(run index, command)` pairs over the submitters (by run index) and into
/// envelopes of at most `per_env` commands, preserving per-run order.
fn plan(cmds: Vec<(usize, Command)>, per_env: usize) -> Vec<Plan> {
    let mut lanes: Vec<Vec<Command>> = vec![Vec::new(); SUBMITTERS];
    for (i, c) in cmds {
        lanes[i % SUBMITTERS].push(c);
    }
    lanes
        .into_iter()
        .map(|l| l.chunks(per_env).map(<[Command]>::to_vec).collect())
        .collect()
}

/// Submits every lane concurrently; returns (accepted, other outcomes, wall time).
async fn run_plan(
    h: &ostro_partition::PartitionHandle<ostro_log::Rf1Log>,
    lanes: Vec<Plan>,
) -> (usize, usize, Duration) {
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for lane in lanes {
        let h = h.clone();
        tasks.push(tokio::spawn(async move {
            let (mut ok, mut other) = (0usize, 0usize);
            for env in lane {
                for o in h.submit(env).await.expect("submit") {
                    if o == ApplyOutcome::Accepted {
                        ok += 1;
                    } else {
                        other += 1;
                    }
                }
            }
            (ok, other)
        }));
    }
    let (mut ok, mut other) = (0, 0);
    for t in tasks {
        let (a, b) = t.await.expect("submitter");
        ok += a;
        other += b;
    }
    (ok, other, t0.elapsed())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement: preloads 1M runs through the WAL; run with --ignored"]
async fn apply_throughput_machine() {
    samples();
    let t = ostro_testkit::fs::tempdir();
    let count = PartitionCount::new(1).expect("1");
    let node = Node::open(NodeConfig::new(t.path().join("data"), count))
        .await
        .expect("node opens");
    let h = node.handle(PartitionId(0)).expect("ready").clone();
    let mut g = Gen::new(0x0216, h.epoch().0);

    // preload: 1M starts, then 1M grants (not measured)
    let t0 = Instant::now();
    let starts: Vec<_> = (0..PRELOAD).map(|_| g.start()).collect();
    let (ok, other, _) = run_plan(&h, plan(starts, 1000)).await;
    assert_eq!((ok, other), (PRELOAD, 0), "preload starts");
    let grants: Vec<_> = (0..PRELOAD).map(|i| g.update(i)).collect();
    let (ok, other, _) = run_plan(&h, plan(grants, 1000)).await;
    assert_eq!((ok, other), (PRELOAD, 0), "preload grants");
    h.machine().force_checkpoint().await.expect("checkpoint");
    let preload = t0.elapsed();
    take("ostro_apply_batch_seconds");
    take("ostro_state_checkpoint_seconds");

    let mut rows = Vec::new();
    for per_env in [64usize, 1] {
        let before = load();
        let cmds: Vec<_> = (0..MEASURED).map(|_| g.rmw()).collect();
        let (ok, other, wall) = run_plan(&h, plan(cmds, per_env)).await;
        assert!(
            other * 1000 <= MEASURED,
            "too many non-Accepted outcomes: {other}"
        );
        let mut batch = take("ostro_apply_batch_seconds");
        let mut ckpt = take("ostro_state_checkpoint_seconds");
        let stalls = batch.iter().filter(|&&s| s >= 0.1).count();
        let rate = MEASURED as f64 / wall.as_secs_f64();
        rows.push(format!(
            "| {per_env} | {SUBMITTERS} | {ok} / {other} | {rate:.0} | {} | {} / {} / {} | {} | {} | {} / {} | {before} -> {} |",
            batch.len(),
            ms(pct(&mut batch, 0.5)),
            ms(pct(&mut batch, 0.99)),
            ms(pct(&mut batch, 1.0)),
            stalls,
            ckpt.len(),
            ms(pct(&mut ckpt, 0.5)),
            ms(pct(&mut ckpt, 1.0)),
            load(),
        ));
    }
    node.shutdown().await.expect("shutdown");
    println!();
    println!(
        "machine level: P=1 Node (default configs), {PRELOAD} runs preloaded + granted in {:.1} s, {MEASURED} RMW commands per row",
        preload.as_secs_f64()
    );
    println!(
        "| cmds/envelope | submitters | accepted / other | end-to-end cmds/s | apply batches | batch ms p50 / p99 / max | batches >= 100 ms | checkpoints | checkpoint ms p50 / max | loadavg |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for r in rows {
        println!("{r}");
    }
}

// --------------------------------------------------------------------- store level

struct Direct {
    store: PartitionStore,
    cur: Cursor,
    since_ckpt: u32,
    last_ckpt: Instant,
}

#[derive(Default)]
struct Stats {
    chunk: Vec<f64>,
    ckpt: Vec<f64>,
    reads: u64,
    accepted: usize,
    other: usize,
}

impl Direct {
    fn apply(&mut self, cmds: &[Bytes], chunk: usize, st: &mut Stats) {
        for c in cmds.chunks(chunk) {
            let t0 = Instant::now();
            let mut b = self.store.batch();
            let mut actions = Vec::new();
            for bytes in c {
                let off = self.cur.next;
                match apply_command(&mut b, &mut self.cur, off, Epoch(1), bytes, &mut actions)
                    .expect("apply")
                {
                    ApplyOutcome::Accepted => st.accepted += 1,
                    _ => st.other += 1,
                }
            }
            st.reads += b.reads();
            let mu = self.cur.meta_update();
            b.commit(&mu).expect("commit");
            st.chunk.push(t0.elapsed().as_secs_f64());
            self.since_ckpt += 1;
            // the machine's cadence: 256 chunks or 100 ms
            if self.since_ckpt >= 256 || self.last_ckpt.elapsed() >= Duration::from_millis(100) {
                let t0 = Instant::now();
                self.store.checkpoint(self.cur.next).expect("checkpoint");
                st.ckpt.push(t0.elapsed().as_secs_f64());
                self.since_ckpt = 0;
                self.last_ckpt = Instant::now();
            }
        }
    }
}

fn encode(cmds: Vec<(usize, Command)>) -> Vec<Bytes> {
    cmds.iter().map(|(_, c)| encode_command(c)).collect()
}

#[test]
#[ignore = "measurement: preloads 1M runs into a fjall store; run with --ignored"]
fn apply_throughput_store() {
    let t = ostro_testkit::fs::tempdir();
    // Optional overrides for tuning experiments (MiB / threads); defaults = StoreConfig::default().
    let mut cfg = StoreConfig::default();
    let env = |k: &str| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
    };
    if let Some(v) = env("OSTRO_STORE_CACHE_MIB") {
        cfg.cache_size = v << 20;
    }
    if let Some(v) = env("OSTRO_STORE_MEMTABLE_MIB") {
        cfg.max_memtable_size = v << 20;
    }
    if let Some(v) = env("OSTRO_STORE_WORKERS") {
        cfg.worker_threads = usize::try_from(v.max(1)).unwrap_or(1);
    }
    let (store, meta) = PartitionStore::open(
        t.path(),
        PartitionId(0),
        PartitionCount::new(1).expect("1"),
        &cfg,
    )
    .expect("open");
    let mut d = Direct {
        cur: Cursor::from_meta(&meta),
        store,
        since_ckpt: 0,
        last_ckpt: Instant::now(),
    };
    let mut st = Stats::default();
    d.apply(
        &[encode_command(&Command::leader_announced(1, 1, 0))],
        1,
        &mut st,
    );
    let mut g = Gen::new(0x0216, 1);

    let t0 = Instant::now();
    let starts = encode((0..PRELOAD).map(|_| g.start()).collect());
    d.apply(&starts, 1024, &mut st);
    let grants = encode((0..PRELOAD).map(|i| g.update(i)).collect());
    d.apply(&grants, 1024, &mut st);
    d.store.checkpoint(d.cur.next).expect("checkpoint");
    assert_eq!(st.accepted, 2 * PRELOAD + 1, "preload accepted");
    let preload = t0.elapsed();

    let mut rows = Vec::new();
    for chunk in [64usize, 1024] {
        let before = load();
        let cmds = encode((0..MEASURED).map(|_| g.rmw()).collect());
        let mut s = Stats::default();
        d.since_ckpt = 0;
        d.last_ckpt = Instant::now();
        let t0 = Instant::now();
        d.apply(&cmds, chunk, &mut s);
        let wall = t0.elapsed();
        assert!(s.other * 1000 <= MEASURED, "non-Accepted: {}", s.other);
        let stalls = s.chunk.iter().filter(|&&x| x >= 0.1).count();
        rows.push(format!(
            "| {chunk} | {} / {} | {:.0} | {:.2} | {} | {} / {} / {} | {} | {} | {} / {} | {before} -> {} |",
            s.accepted,
            s.other,
            MEASURED as f64 / wall.as_secs_f64(),
            s.reads as f64 / MEASURED as f64,
            s.chunk.len(),
            ms(pct(&mut s.chunk, 0.5)),
            ms(pct(&mut s.chunk, 0.99)),
            ms(pct(&mut s.chunk, 1.0)),
            stalls,
            s.ckpt.len(),
            ms(pct(&mut s.ckpt, 0.5)),
            ms(pct(&mut s.ckpt, 1.0)),
            load(),
        ));
    }
    println!();
    println!(
        "store level: apply_command + ApplyBatch::commit, StoreConfig {:?}, {PRELOAD} runs preloaded + granted in {:.1} s, {MEASURED} RMW commands per row",
        cfg,
        preload.as_secs_f64()
    );
    println!(
        "| chunk cmds | accepted / other | cmds/s (incl. checkpoints) | keyspace reads/cmd | chunks | chunk ms p50 / p99 / max | chunks >= 100 ms | checkpoints | checkpoint ms p50 / max | loadavg |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for r in rows {
        println!("{r}");
    }
}
