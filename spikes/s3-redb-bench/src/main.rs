//! Spike S3: redb 4.6 durability tiers under an `apply_batch`-like workload.
//!
//! Workload per command (emulates one partition state-machine command):
//!   - insert 1 `runs` row        (random or sequential 16-byte run id -> ~200 B value)
//!   - insert 1 `runs_by_status`  (17-byte key: status byte + run id -> ())
//!   - insert 1 `outbox` row      (u64 seq -> ~64 B value)
//!   - delete 1 older `outbox` row (ring of OUTBOX_RING prior keys)
//!
//! Each batch of B commands is one write transaction that also writes
//! `meta["applied_offset"] = offset_after_batch` (offset counted in commands).
//!
//! Tiers:
//!   t1      every batch Durability::Immediate
//!   t2      every batch Durability::None + Immediate checkpoint txn every
//!           checkpoint-ms OR every checkpoint-batches batches (planned design)
//!   t3      every batch None, one Immediate at the end (upper bound)
//!   t2-2pc  t2 with WriteTransaction::set_two_phase_commit(true)
//!   t2-qr   t2 with WriteTransaction::set_quick_repair(true)
//!
//! Read-modify-write mode (`--rmw`, plan 01-08): every command first reads the run
//! row (as the real state machine does before applying a command), then writes the
//! updated row, moves the run in `runs_by_status` (remove old status key, insert new)
//! and does the same outbox insert/delete. A fraction `--new-ratio` of commands start
//! a new run (the read misses); the rest update an existing run, picked 90% from the
//! most recent 64Ki runs and 10% uniformly over all runs. `--preload N` inserts N
//! runs before warm-up (not measured) so reads run against a realistically sized db.
//!
//! Note: in redb 4.6 two-phase commit and quick-repair are per-WriteTransaction
//! settings (not Builder settings); they are applied to every transaction here.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::json;

const RUNS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("runs");
const RUNS_BY_STATUS: TableDefinition<&[u8; 17], ()> = TableDefinition::new("runs_by_status");
const OUTBOX: TableDefinition<u64, &[u8]> = TableDefinition::new("outbox");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const RUN_VALUE_BYTES: usize = 200;
const OUTBOX_VALUE_BYTES: usize = 64;
const OUTBOX_RING: u64 = 1024;
const COMMANDS_PER_STEP: f64 = 3.0;
const RECENT_RUNS: usize = 65_536;
const PRELOAD_BATCH: u64 = 10_000;

#[derive(Parser)]
#[command(about = "Spike S3: redb durability tier benchmark")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Tier {
    T1,
    T2,
    T3,
    #[value(name = "t2-2pc")]
    T2TwoPc,
    #[value(name = "t2-qr")]
    T2Qr,
}

impl Tier {
    fn name(self) -> &'static str {
        match self {
            Tier::T1 => "t1",
            Tier::T2 => "t2",
            Tier::T3 => "t3",
            Tier::T2TwoPc => "t2-2pc",
            Tier::T2Qr => "t2-qr",
        }
    }
    fn checkpoints(self) -> bool {
        matches!(self, Tier::T2 | Tier::T2TwoPc | Tier::T2Qr)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Keys {
    Random,
    Sequential,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Engine {
    Redb,
    /// Requires `--features fjall`. Only t1/t2/t3 analogues are defined.
    Fjall,
}

#[derive(clap::Args, Clone)]
struct BenchOpts {
    #[arg(long, value_enum, default_value = "redb")]
    engine: Engine,
    #[arg(long, value_enum, default_value = "t2")]
    tier: Tier,
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value_t = 200_000)]
    commands: u64,
    #[arg(long, default_value_t = 20_000)]
    warmup: u64,
    #[arg(long, default_value_t = 64)]
    batch: u64,
    #[arg(long, default_value_t = 100)]
    checkpoint_ms: u64,
    #[arg(long, default_value_t = 256)]
    checkpoint_batches: u64,
    #[arg(long, value_enum, default_value = "random")]
    keys: Keys,
    #[arg(long, default_value_t = 0x05_7a0)]
    seed: u64,
    /// Read-modify-write workload (read the run row before writing it).
    #[arg(long)]
    rmw: bool,
    /// Fraction of commands that start a new run in --rmw mode.
    #[arg(long, default_value_t = 0.25)]
    new_ratio: f64,
    /// Runs inserted before warm-up (not measured).
    #[arg(long, default_value_t = 0)]
    preload: u64,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run one tier and print one JSON result line.
    Bench(BenchOpts),
    /// Run t2 (or another checkpointing tier) forever, printing `CKPT <offset>` after each Immediate commit.
    CrashChild {
        #[arg(long, value_enum, default_value = "redb")]
        engine: Engine,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, value_enum, default_value = "t2")]
        tier: Tier,
        #[arg(long, default_value_t = 64)]
        batch: u64,
    },
    /// Spawn crash-child, SIGKILL it after `after-ms`, reopen and check invariants.
    CrashTest {
        #[arg(long, value_enum, default_value = "redb")]
        engine: Engine,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 3000)]
        after_ms: u64,
        #[arg(long, value_enum, default_value = "t2")]
        tier: Tier,
        #[arg(long, default_value_t = 64)]
        batch: u64,
        #[arg(long, default_value_t = 0)]
        run: u64,
    },
}

type AnyResult<T> = Result<T, Box<dyn std::error::Error>>;

fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unavailable".into())
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn summarize(mut v: Vec<u64>) -> serde_json::Value {
    v.sort_unstable();
    json!({
        "n": v.len(),
        "p50_us": pct(&v, 50.0),
        "p99_us": pct(&v, 99.0),
        "max_us": v.last().copied().unwrap_or(0),
    })
}

fn db_path(dir: &Path) -> PathBuf {
    dir.join("s3-bench.redb")
}

fn guard_dir(dir: &Path) -> AnyResult<()> {
    let s = dir.to_string_lossy();
    if s.starts_with("/mnt/") {
        return Err(format!("refusing to run under /mnt (WSL2 9p): {s}").into());
    }
    std::fs::create_dir_all(dir)?;
    Ok(())
}

/// Chooses the run each --rmw command touches (identical sequence for both engines).
struct Picker {
    ids: Vec<[u8; 16]>,
    new_ratio: f64,
}

impl Picker {
    fn new(new_ratio: f64) -> Self {
        Self { ids: Vec::new(), new_ratio }
    }
    fn fresh(&mut self, rng: &mut StdRng) -> [u8; 16] {
        let mut id = [0u8; 16];
        rng.fill(&mut id);
        self.ids.push(id);
        id
    }
    /// Returns (run id, is_new).
    fn pick(&mut self, rng: &mut StdRng) -> ([u8; 16], bool) {
        if self.ids.is_empty() || rng.random::<f64>() < self.new_ratio {
            return (self.fresh(rng), true);
        }
        let n = self.ids.len();
        let i = if rng.random::<f64>() < 0.9 {
            n - 1 - rng.random_range(0..n.min(RECENT_RUNS))
        } else {
            rng.random_range(0..n)
        };
        (self.ids[i], false)
    }
}

fn status_key(status: u8, id: &[u8; 16]) -> [u8; 17] {
    let mut k = [0u8; 17];
    k[0] = status;
    k[1..].copy_from_slice(id);
    k
}

/// Given the previously stored row (if any), produce (old_status, new_status, new_row).
fn next_row(old: Option<Vec<u8>>, base: &[u8], id: &[u8; 16]) -> (Option<u8>, u8, Vec<u8>) {
    match old {
        Some(mut v) => {
            let old_s = v[1];
            let new_s = (old_s + 1) % 4;
            v[1] = new_s;
            let c = u64::from_le_bytes(v[2..10].try_into().unwrap()) + 1;
            v[2..10].copy_from_slice(&c.to_le_bytes());
            (Some(old_s), new_s, v)
        }
        None => {
            let mut v = base.to_vec();
            v[0] = id[0];
            v[1] = 0;
            v[2..10].copy_from_slice(&0u64.to_le_bytes());
            (None, 0, v)
        }
    }
}

/// Workload state shared by bench and crash-child.
struct Workload {
    rng: StdRng,
    keys: Keys,
    seq_run: u64,
    outbox_seq: u64,
    offset: u64,
    run_value: Vec<u8>,
    outbox_value: Vec<u8>,
    tier: Tier,
    rmw: bool,
    picker: Picker,
    /// --rmw: reads whose hit/miss disagreed with the picker (must stay 0).
    read_mismatch: u64,
    reads: u64,
}

impl Workload {
    fn new(tier: Tier, keys: Keys, seed: u64) -> Self {
        Self::with_rmw(tier, keys, seed, false, 0.25)
    }
    fn with_rmw(tier: Tier, keys: Keys, seed: u64, rmw: bool, new_ratio: f64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut run_value = vec![0u8; RUN_VALUE_BYTES];
        rng.fill(&mut run_value[..]);
        let mut outbox_value = vec![0u8; OUTBOX_VALUE_BYTES];
        rng.fill(&mut outbox_value[..]);
        Self {
            rng,
            keys,
            seq_run: 0,
            outbox_seq: 0,
            offset: 0,
            run_value,
            outbox_value,
            tier,
            rmw,
            picker: Picker::new(new_ratio),
            read_mismatch: 0,
            reads: 0,
        }
    }

    /// Insert `n` runs in large None batches plus one Immediate commit (not measured).
    fn preload(&mut self, db: &Database, n: u64) -> AnyResult<()> {
        let mut left = n;
        while left > 0 {
            let k = left.min(PRELOAD_BATCH);
            let mut tx = db.begin_write()?;
            tx.set_durability(Durability::None)?;
            {
                let mut runs = tx.open_table(RUNS)?;
                let mut idx = tx.open_table(RUNS_BY_STATUS)?;
                for _ in 0..k {
                    let id = self.picker.fresh(&mut self.rng);
                    let (_, s, v) = next_row(None, &self.run_value, &id);
                    runs.insert(&id, &v[..])?;
                    idx.insert(&status_key(s, &id), ())?;
                }
            }
            tx.commit()?;
            left -= k;
        }
        let tx = db.begin_write()?;
        tx.commit()?; // Immediate (default)
        Ok(())
    }

    fn apply_batch_rmw(&mut self, db: &Database, batch: u64, durability: Durability) -> AnyResult<(u64, u64)> {
        let t0 = Instant::now();
        let tx = self.begin(db, durability)?;
        {
            let mut runs = tx.open_table(RUNS)?;
            let mut idx = tx.open_table(RUNS_BY_STATUS)?;
            let mut outbox = tx.open_table(OUTBOX)?;
            let mut meta = tx.open_table(META)?;
            for _ in 0..batch {
                let (id, is_new) = self.picker.pick(&mut self.rng);
                let old = runs.get(&id)?.map(|g| g.value().to_vec());
                self.reads += 1;
                if is_new != old.is_none() {
                    self.read_mismatch += 1;
                }
                let (old_s, new_s, v) = next_row(old, &self.run_value, &id);
                if let Some(s) = old_s {
                    idx.remove(&status_key(s, &id))?;
                }
                idx.insert(&status_key(new_s, &id), ())?;
                runs.insert(&id, &v[..])?;
                outbox.insert(self.outbox_seq, &self.outbox_value[..])?;
                if self.outbox_seq >= OUTBOX_RING {
                    outbox.remove(self.outbox_seq - OUTBOX_RING)?;
                }
                self.outbox_seq += 1;
                self.offset += 1;
            }
            meta.insert("applied_offset", self.offset)?;
        }
        let tc = Instant::now();
        tx.commit()?;
        let end = Instant::now();
        Ok(((end - tc).as_micros() as u64, (end - t0).as_micros() as u64))
    }

    fn begin(&self, db: &Database, durability: Durability) -> AnyResult<redb::WriteTransaction> {
        let mut tx = db.begin_write()?;
        tx.set_durability(durability)?;
        match self.tier {
            Tier::T2TwoPc => tx.set_two_phase_commit(true),
            Tier::T2Qr => tx.set_quick_repair(true),
            _ => {}
        }
        Ok(tx)
    }

    /// One batch = one write txn. Returns (commit_us, txn_us).
    fn apply_batch(&mut self, db: &Database, batch: u64, durability: Durability) -> AnyResult<(u64, u64)> {
        if self.rmw {
            return self.apply_batch_rmw(db, batch, durability);
        }
        let t0 = Instant::now();
        let tx = self.begin(db, durability)?;
        {
            let mut runs = tx.open_table(RUNS)?;
            let mut idx = tx.open_table(RUNS_BY_STATUS)?;
            let mut outbox = tx.open_table(OUTBOX)?;
            let mut meta = tx.open_table(META)?;
            for _ in 0..batch {
                let mut run_id = [0u8; 16];
                match self.keys {
                    Keys::Random => self.rng.fill(&mut run_id),
                    Keys::Sequential => {
                        run_id[..8].copy_from_slice(&self.seq_run.to_be_bytes());
                        self.seq_run += 1;
                    }
                }
                // vary value a little so pages are not trivially identical
                self.run_value[0] = run_id[0];
                runs.insert(&run_id, &self.run_value[..])?;
                let mut ik = [0u8; 17];
                ik[0] = (self.offset % 4) as u8; // status byte
                ik[1..].copy_from_slice(&run_id);
                idx.insert(&ik, ())?;
                outbox.insert(self.outbox_seq, &self.outbox_value[..])?;
                if self.outbox_seq >= OUTBOX_RING {
                    outbox.remove(self.outbox_seq - OUTBOX_RING)?;
                }
                self.outbox_seq += 1;
                self.offset += 1;
            }
            meta.insert("applied_offset", self.offset)?;
        }
        let tc = Instant::now();
        tx.commit()?;
        let end = Instant::now();
        Ok(((end - tc).as_micros() as u64, (end - t0).as_micros() as u64))
    }

    /// Immediate checkpoint txn. Returns commit-inclusive txn latency in us.
    fn checkpoint(&self, db: &Database) -> AnyResult<u64> {
        let t0 = Instant::now();
        let tx = self.begin(db, Durability::Immediate)?;
        {
            let mut meta = tx.open_table(META)?;
            meta.insert("checkpointed_offset", self.offset)?;
        }
        tx.commit()?;
        Ok(t0.elapsed().as_micros() as u64)
    }
}

fn batch_durability(tier: Tier) -> Durability {
    match tier {
        Tier::T1 => Durability::Immediate,
        _ => Durability::None,
    }
}

fn bench(o: BenchOpts) -> AnyResult<()> {
    if o.engine == Engine::Fjall {
        #[cfg(feature = "fjall")]
        return fjall_bench::bench(o);
        #[cfg(not(feature = "fjall"))]
        return Err("--engine fjall requires building with --features fjall".into());
    }
    guard_dir(&o.dir)?;
    let path = db_path(&o.dir);
    let _ = std::fs::remove_file(&path);
    let load_before = loadavg();
    let db = Database::create(&path)?;
    let mut w = Workload::with_rmw(o.tier, o.keys, o.seed, o.rmw, o.new_ratio);
    let dur = batch_durability(o.tier);
    let t_pre = Instant::now();
    if o.preload > 0 {
        w.preload(&db, o.preload)?;
    }
    let preload_s = t_pre.elapsed().as_secs_f64();

    let mut last_ckpt = Instant::now();
    let mut batches_since_ckpt = 0u64;

    // warm-up (same db, same checkpoint cadence, not measured)
    let warm_batches = o.warmup / o.batch;
    for _ in 0..warm_batches {
        w.apply_batch(&db, o.batch, dur)?;
        if o.tier.checkpoints() {
            batches_since_ckpt += 1;
            if last_ckpt.elapsed() >= Duration::from_millis(o.checkpoint_ms)
                || batches_since_ckpt >= o.checkpoint_batches
            {
                w.checkpoint(&db)?;
                last_ckpt = Instant::now();
                batches_since_ckpt = 0;
            }
        }
    }
    if o.tier.checkpoints() || o.tier == Tier::T3 {
        // start measurement from a durable point
        w.checkpoint(&db)?;
        last_ckpt = Instant::now();
        batches_since_ckpt = 0;
    }

    let n_batches = o.commands / o.batch;
    let mut commit_us = Vec::with_capacity(n_batches as usize);
    let mut txn_us = Vec::with_capacity(n_batches as usize);
    let mut ckpt_us = Vec::new();
    let start = Instant::now();
    for _ in 0..n_batches {
        let (c, t) = w.apply_batch(&db, o.batch, dur)?;
        commit_us.push(c);
        txn_us.push(t);
        if o.tier.checkpoints() {
            batches_since_ckpt += 1;
            if last_ckpt.elapsed() >= Duration::from_millis(o.checkpoint_ms)
                || batches_since_ckpt >= o.checkpoint_batches
            {
                ckpt_us.push(w.checkpoint(&db)?);
                last_ckpt = Instant::now();
                batches_since_ckpt = 0;
            }
        }
    }
    // T2 variants and T3 end with a final Immediate so all measured work is durable.
    if o.tier != Tier::T1 {
        let c = w.checkpoint(&db)?;
        if o.tier.checkpoints() {
            ckpt_us.push(c);
        }
    }
    let wall = start.elapsed().as_secs_f64();
    let commands = n_batches * o.batch;
    let cmds_per_s = commands as f64 / wall;
    drop(db);
    let db_file_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let load_after = loadavg();
    let checkpoints = ckpt_us.len();
    let out = json!({
        "engine": "redb",
        "tier": o.tier.name(),
        "keys": format!("{:?}", o.keys).to_lowercase(),
        "batch": o.batch,
        "commands": commands,
        "batches": n_batches,
        "warmup_commands": warm_batches * o.batch,
        "wall_s": wall,
        "cmds_per_s": cmds_per_s,
        "batches_per_s": n_batches as f64 / wall,
        "commit": summarize(commit_us),
        "txn": summarize(txn_us),
        "checkpoint": if o.tier.checkpoints() { summarize(ckpt_us) } else { serde_json::Value::Null },
        "checkpoints": checkpoints,
        "checkpoint_ms": o.checkpoint_ms,
        "checkpoint_batches": o.checkpoint_batches,
        "db_file_bytes": db_file_bytes,
        "equivalent_steps_per_s": cmds_per_s / COMMANDS_PER_STEP,
        "steps_assumption": "3 commands per workflow step",
        "rmw": o.rmw,
        "new_ratio": if o.rmw { json!(o.new_ratio) } else { serde_json::Value::Null },
        "preload_runs": o.preload,
        "preload_s": preload_s,
        "reads": w.reads,
        "read_mismatch": w.read_mismatch,
        "loadavg_before": load_before,
        "loadavg_after": load_after,
    });
    println!("{out}");
    let _ = std::fs::remove_file(&path);
    Ok(())
}

fn crash_child(engine: Engine, dir: PathBuf, tier: Tier, batch: u64) -> AnyResult<()> {
    if engine == Engine::Fjall {
        #[cfg(feature = "fjall")]
        return fjall_bench::crash_child(dir, batch);
        #[cfg(not(feature = "fjall"))]
        return Err("--engine fjall requires building with --features fjall".into());
    }
    guard_dir(&dir)?;
    let path = db_path(&dir);
    let _ = std::fs::remove_file(&path);
    let db = Database::create(&path)?;
    let mut w = Workload::new(tier, Keys::Random, 0xc4a5);
    let dur = batch_durability(tier);
    let stdout = std::io::stdout();
    let mut last = Instant::now();
    let mut since = 0u64;
    loop {
        w.apply_batch(&db, batch, dur)?;
        since += 1;
        if last.elapsed() >= Duration::from_millis(100) || since >= 256 {
            w.checkpoint(&db)?;
            // printed only after the Immediate commit returned
            let mut lock = stdout.lock();
            writeln!(lock, "CKPT {}", w.offset)?;
            lock.flush()?;
            last = Instant::now();
            since = 0;
        }
    }
}

/// State observed after reopening a crashed database.
struct Reopened {
    open_ok: bool,
    open_err: String,
    repair_callbacks: u64,
    applied: u64,
    checkpointed: u64,
    /// Number of rows in `runs` (each crash-child command inserts exactly one fresh run).
    runs_rows: u64,
    /// Highest outbox key (the last command's outbox seq), if any.
    outbox_last: Option<u64>,
}

fn redb_reopen(path: &Path) -> AnyResult<Reopened> {
    let repairs = Arc::new(AtomicU64::new(0));
    let rc = repairs.clone();
    let open = Database::builder()
        .set_repair_callback(move |_s| {
            rc.fetch_add(1, Ordering::SeqCst);
        })
        .open(path);
    let mut r = Reopened {
        open_ok: false,
        open_err: String::new(),
        repair_callbacks: 0,
        applied: 0,
        checkpointed: 0,
        runs_rows: 0,
        outbox_last: None,
    };
    match open {
        Ok(db) => {
            use redb::ReadableTableMetadata;
            let rtx = db.begin_read()?;
            let meta = rtx.open_table(META)?;
            r.applied = meta.get("applied_offset")?.map(|g| g.value()).unwrap_or(0);
            r.checkpointed = meta.get("checkpointed_offset")?.map(|g| g.value()).unwrap_or(0);
            r.runs_rows = rtx.open_table(RUNS)?.len()?;
            r.outbox_last = rtx.open_table(OUTBOX)?.last()?.map(|(k, _)| k.value());
            r.open_ok = true;
        }
        Err(e) => r.open_err = e.to_string(),
    }
    r.repair_callbacks = repairs.load(Ordering::SeqCst);
    Ok(r)
}

fn du(p: &Path) -> u64 {
    match std::fs::metadata(p) {
        Ok(m) if m.is_dir() => std::fs::read_dir(p)
            .map(|rd| rd.flatten().map(|e| du(&e.path())).sum())
            .unwrap_or(0),
        Ok(m) => m.len(),
        Err(_) => 0,
    }
}

fn crash_test(engine: Engine, dir: PathBuf, after_ms: u64, tier: Tier, batch: u64, run: u64) -> AnyResult<()> {
    guard_dir(&dir)?;
    let (path, engine_name) = match engine {
        Engine::Redb => (db_path(&dir), "redb"),
        Engine::Fjall => (dir.join("s3-bench.fjall"), "fjall"),
    };
    let exe = std::env::current_exe()?;
    let load_before = loadavg();
    let mut child = Command::new(exe)
        .arg("crash-child")
        .arg("--engine")
        .arg(engine_name)
        .arg("--dir")
        .arg(&dir)
        .arg("--tier")
        .arg(tier.name())
        .arg("--batch")
        .arg(batch.to_string())
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().ok_or("no child stdout")?;
    let last_ckpt = Arc::new(AtomicU64::new(0));
    let seen = Arc::new(AtomicU64::new(0));
    let (lc, sn) = (last_ckpt.clone(), seen.clone());
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if let Some(v) = line.strip_prefix("CKPT ") {
                if let Ok(n) = v.trim().parse::<u64>() {
                    lc.store(n, Ordering::SeqCst);
                    sn.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    });
    std::thread::sleep(Duration::from_millis(after_ms));
    child.kill()?; // SIGKILL on Unix
    let status = child.wait()?;
    // drain: any line already in the pipe was printed after a durable commit
    reader.join().map_err(|_| "reader panicked")?;
    let last_ckpt = last_ckpt.load(Ordering::SeqCst);
    let ckpts_seen = seen.load(Ordering::SeqCst);
    let file_bytes = du(&path);

    let t_open = Instant::now();
    let r = match engine {
        Engine::Redb => redb_reopen(&path)?,
        Engine::Fjall => {
            #[cfg(feature = "fjall")]
            {
                fjall_bench::reopen(&path)?
            }
            #[cfg(not(feature = "fjall"))]
            {
                return Err("--engine fjall requires building with --features fjall".into());
            }
        }
    };
    let open_us = t_open.elapsed().as_micros() as u64;

    let ok_ckpt = r.checkpointed >= last_ckpt && r.applied >= last_ckpt;
    // Torn-batch checks: applied offset is on a batch boundary, and the data rows match
    // it exactly (one fresh run and one outbox seq per command, all-or-nothing per batch).
    let ok_batch = r.applied % batch == 0;
    let ok_rows = r.runs_rows == r.applied && r.outbox_last == r.applied.checked_sub(1);
    let pass = r.open_ok && ok_ckpt && ok_batch && ok_rows;
    let out = json!({
        "engine": engine_name,
        "crash_run": run,
        "tier": tier.name(),
        "after_ms": after_ms,
        "child_status": format!("{status}"),
        "ckpts_seen": ckpts_seen,
        "last_ckpt": last_ckpt,
        "open_ok": r.open_ok,
        "open_err": r.open_err,
        "open_us": open_us,
        "repair_callbacks": r.repair_callbacks,
        "db_file_bytes_at_kill": file_bytes,
        "applied_offset": r.applied,
        "checkpointed_offset": r.checkpointed,
        "runs_rows": r.runs_rows,
        "outbox_last": r.outbox_last,
        "gap_applied_minus_last_ckpt": r.applied as i64 - last_ckpt as i64,
        "assert_durable_ckpt": ok_ckpt,
        "assert_no_torn_batch": ok_batch,
        "assert_rows_match_applied": ok_rows,
        "pass": pass,
        "loadavg_before": load_before,
        "loadavg_after": loadavg(),
    });
    println!("{out}");
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&path);
    if !pass {
        return Err("crash-test invariant violated".into());
    }
    Ok(())
}

fn main() -> AnyResult<()> {
    match Cli::parse().cmd {
        Cmd::Bench(o) => bench(o),
        Cmd::CrashChild { engine, dir, tier, batch } => crash_child(engine, dir, tier, batch),
        Cmd::CrashTest {
            engine,
            dir,
            after_ms,
            tier,
            batch,
            run,
        } => crash_test(engine, dir, after_ms, tier, batch, run),
    }
}

/// fjall 3.1.13 variant of the same workload (S3 Task 2 fallback; built only with `--features fjall`).
///
/// Mapping: one keyspace per table; one `WriteBatch` per B-command batch (atomic across keyspaces;
/// the default journal mode flushes the batch to OS buffers on commit, i.e. it survives a process
/// crash but not power loss = analogue of redb Durability::None). Tiers:
///   t1  every batch committed with `durability(Some(PersistMode::SyncAll))`
///   t2  `db.persist(PersistMode::SyncAll)` every checkpoint-ms or checkpoint-batches batches
///   t3  one `persist(SyncAll)` at the end
///
/// --rmw: reads go to the keyspace (`Keyspace::get`) with a per-batch overlay map for rows already
/// written in the current batch (a WriteBatch has no read-your-writes; the real single-writer
/// partition state machine would keep the same overlay).
#[cfg(feature = "fjall")]
mod fjall_bench {
    use super::*;
    use fjall::{Database as FDb, Keyspace, KeyspaceCreateOptions, PersistMode};
    use std::collections::HashMap;

    struct Ks {
        runs: Keyspace,
        idx: Keyspace,
        outbox: Keyspace,
        meta: Keyspace,
    }

    fn open(path: &Path) -> AnyResult<(FDb, Ks)> {
        let db = FDb::builder(path).open()?;
        let ks = Ks {
            runs: db.keyspace("runs", KeyspaceCreateOptions::default)?,
            idx: db.keyspace("runs_by_status", KeyspaceCreateOptions::default)?,
            outbox: db.keyspace("outbox", KeyspaceCreateOptions::default)?,
            meta: db.keyspace("meta", KeyspaceCreateOptions::default)?,
        };
        Ok((db, ks))
    }

    struct FWork {
        rng: StdRng,
        keys: Keys,
        seq_run: u64,
        outbox_seq: u64,
        offset: u64,
        run_value: Vec<u8>,
        outbox_value: Vec<u8>,
        rmw: bool,
        picker: Picker,
        overlay: HashMap<[u8; 16], Vec<u8>>,
        reads: u64,
        read_mismatch: u64,
    }

    impl FWork {
        fn new(keys: Keys, seed: u64, rmw: bool, new_ratio: f64) -> Self {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut run_value = vec![0u8; RUN_VALUE_BYTES];
            rng.fill(&mut run_value[..]);
            let mut outbox_value = vec![0u8; OUTBOX_VALUE_BYTES];
            rng.fill(&mut outbox_value[..]);
            Self {
                rng,
                keys,
                seq_run: 0,
                outbox_seq: 0,
                offset: 0,
                run_value,
                outbox_value,
                rmw,
                picker: Picker::new(new_ratio),
                overlay: HashMap::new(),
                reads: 0,
                read_mismatch: 0,
            }
        }

        fn preload(&mut self, db: &FDb, ks: &Ks, n: u64) -> AnyResult<()> {
            let mut left = n;
            while left > 0 {
                let k = left.min(PRELOAD_BATCH);
                let mut b = db.batch();
                for _ in 0..k {
                    let id = self.picker.fresh(&mut self.rng);
                    let (_, st, v) = next_row(None, &self.run_value, &id);
                    b.insert(&ks.runs, id, v);
                    b.insert(&ks.idx, status_key(st, &id), []);
                }
                b.commit()?;
                left -= k;
            }
            db.persist(PersistMode::SyncAll)?;
            Ok(())
        }

        fn apply(&mut self, db: &FDb, ks: &Ks, batch: u64, sync: bool) -> AnyResult<(u64, u64)> {
            let t0 = Instant::now();
            let mut b = db.batch();
            if sync {
                b = b.durability(Some(PersistMode::SyncAll));
            }
            self.overlay.clear();
            for _ in 0..batch {
                if self.rmw {
                    let (id, is_new) = self.picker.pick(&mut self.rng);
                    let old = match self.overlay.get(&id) {
                        Some(v) => Some(v.clone()),
                        None => ks.runs.get(id)?.map(|v| v.to_vec()),
                    };
                    self.reads += 1;
                    if is_new != old.is_none() {
                        self.read_mismatch += 1;
                    }
                    let (old_s, new_s, v) = next_row(old, &self.run_value, &id);
                    if let Some(st) = old_s {
                        b.remove(&ks.idx, status_key(st, &id));
                    }
                    b.insert(&ks.idx, status_key(new_s, &id), []);
                    b.insert(&ks.runs, id, v.as_slice());
                    self.overlay.insert(id, v);
                } else {
                    let mut run_id = [0u8; 16];
                    match self.keys {
                        Keys::Random => self.rng.fill(&mut run_id),
                        Keys::Sequential => {
                            run_id[..8].copy_from_slice(&self.seq_run.to_be_bytes());
                            self.seq_run += 1;
                        }
                    }
                    self.run_value[0] = run_id[0];
                    b.insert(&ks.runs, run_id, self.run_value.as_slice());
                    b.insert(&ks.idx, status_key((self.offset % 4) as u8, &run_id), []);
                }
                b.insert(&ks.outbox, self.outbox_seq.to_be_bytes(), self.outbox_value.as_slice());
                if self.outbox_seq >= OUTBOX_RING {
                    b.remove(&ks.outbox, (self.outbox_seq - OUTBOX_RING).to_be_bytes());
                }
                self.outbox_seq += 1;
                self.offset += 1;
            }
            b.insert(&ks.meta, "applied_offset", self.offset.to_le_bytes());
            let tc = Instant::now();
            b.commit()?;
            let end = Instant::now();
            Ok(((end - tc).as_micros() as u64, (end - t0).as_micros() as u64))
        }

        /// Durable checkpoint: record checkpointed_offset, then fsync the journal.
        fn checkpoint(&self, db: &FDb, ks: &Ks) -> AnyResult<u64> {
            let t0 = Instant::now();
            let mut b = db.batch();
            b.insert(&ks.meta, "checkpointed_offset", self.offset.to_le_bytes());
            b.commit()?;
            db.persist(PersistMode::SyncAll)?;
            Ok(t0.elapsed().as_micros() as u64)
        }
    }

    pub fn bench(o: BenchOpts) -> AnyResult<()> {
        if !matches!(o.tier, Tier::T1 | Tier::T2 | Tier::T3) {
            return Err("fjall engine supports only t1/t2/t3".into());
        }
        guard_dir(&o.dir)?;
        let path = o.dir.join("s3-bench.fjall");
        let _ = std::fs::remove_dir_all(&path);
        let load_before = loadavg();
        let (db, ks) = open(&path)?;
        let mut w = FWork::new(o.keys, o.seed, o.rmw, o.new_ratio);
        let t_pre = Instant::now();
        if o.preload > 0 {
            w.preload(&db, &ks, o.preload)?;
        }
        let preload_s = t_pre.elapsed().as_secs_f64();
        let sync = o.tier == Tier::T1;

        let mut last = Instant::now();
        let mut since = 0u64;
        let warm_batches = o.warmup / o.batch;
        for _ in 0..warm_batches {
            w.apply(&db, &ks, o.batch, sync)?;
            if o.tier == Tier::T2 {
                since += 1;
                if last.elapsed() >= Duration::from_millis(o.checkpoint_ms) || since >= o.checkpoint_batches {
                    w.checkpoint(&db, &ks)?;
                    last = Instant::now();
                    since = 0;
                }
            }
        }
        if o.tier != Tier::T1 {
            w.checkpoint(&db, &ks)?;
            last = Instant::now();
            since = 0;
        }
        let n_batches = o.commands / o.batch;
        let mut commit_us = Vec::with_capacity(n_batches as usize);
        let mut txn_us = Vec::with_capacity(n_batches as usize);
        let mut ckpt_us = Vec::new();
        let start = Instant::now();
        for _ in 0..n_batches {
            let (c, t) = w.apply(&db, &ks, o.batch, sync)?;
            commit_us.push(c);
            txn_us.push(t);
            if o.tier == Tier::T2 {
                since += 1;
                if last.elapsed() >= Duration::from_millis(o.checkpoint_ms) || since >= o.checkpoint_batches {
                    ckpt_us.push(w.checkpoint(&db, &ks)?);
                    last = Instant::now();
                    since = 0;
                }
            }
        }
        if o.tier != Tier::T1 {
            let c = w.checkpoint(&db, &ks)?;
            if o.tier == Tier::T2 {
                ckpt_us.push(c);
            }
        }
        let wall = start.elapsed().as_secs_f64();
        let commands = n_batches * o.batch;
        let cmds_per_s = commands as f64 / wall;
        let disk = db.disk_space().unwrap_or(0);
        drop(ks);
        drop(db);
        let checkpoints = ckpt_us.len();
        let out = json!({
            "engine": "fjall",
            "tier": o.tier.name(),
            "keys": format!("{:?}", o.keys).to_lowercase(),
            "batch": o.batch,
            "commands": commands,
            "batches": n_batches,
            "warmup_commands": warm_batches * o.batch,
            "wall_s": wall,
            "cmds_per_s": cmds_per_s,
            "batches_per_s": n_batches as f64 / wall,
            "commit": summarize(commit_us),
            "txn": summarize(txn_us),
            "checkpoint": if o.tier == Tier::T2 { summarize(ckpt_us) } else { serde_json::Value::Null },
            "checkpoints": checkpoints,
            "checkpoint_ms": o.checkpoint_ms,
            "checkpoint_batches": o.checkpoint_batches,
            "db_file_bytes": disk,
            "equivalent_steps_per_s": cmds_per_s / COMMANDS_PER_STEP,
            "steps_assumption": "3 commands per workflow step",
            "rmw": o.rmw,
            "new_ratio": if o.rmw { json!(o.new_ratio) } else { serde_json::Value::Null },
            "preload_runs": o.preload,
            "preload_s": preload_s,
            "reads": w.reads,
            "read_mismatch": w.read_mismatch,
            "loadavg_before": load_before,
            "loadavg_after": loadavg(),
        });
        println!("{out}");
        let _ = std::fs::remove_dir_all(&path);
        Ok(())
    }

    /// t2 forever; prints `CKPT <offset>` only after persist(SyncAll) returned.
    pub fn crash_child(dir: PathBuf, batch: u64) -> AnyResult<()> {
        guard_dir(&dir)?;
        let path = dir.join("s3-bench.fjall");
        let _ = std::fs::remove_dir_all(&path);
        let (db, ks) = open(&path)?;
        let mut w = FWork::new(Keys::Random, 0xc4a5, false, 0.25);
        let stdout = std::io::stdout();
        let mut last = Instant::now();
        let mut since = 0u64;
        loop {
            w.apply(&db, &ks, batch, false)?;
            since += 1;
            if last.elapsed() >= Duration::from_millis(100) || since >= 256 {
                w.checkpoint(&db, &ks)?;
                let mut lock = stdout.lock();
                writeln!(lock, "CKPT {}", w.offset)?;
                lock.flush()?;
                last = Instant::now();
                since = 0;
            }
        }
    }

    fn le_u64(v: Option<fjall::UserValue>) -> u64 {
        v.and_then(|v| <[u8; 8]>::try_from(&v[..]).ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    }

    pub fn reopen(path: &Path) -> AnyResult<Reopened> {
        let mut r = Reopened {
            open_ok: false,
            open_err: String::new(),
            repair_callbacks: 0,
            applied: 0,
            checkpointed: 0,
            runs_rows: 0,
            outbox_last: None,
        };
        match open(path) {
            Ok((_db, ks)) => {
                r.applied = le_u64(ks.meta.get("applied_offset")?);
                r.checkpointed = le_u64(ks.meta.get("checkpointed_offset")?);
                r.runs_rows = ks.runs.len()? as u64;
                r.outbox_last = match ks.outbox.last_key_value() {
                    Some(g) => {
                        let (k, _) = g.into_inner()?;
                        Some(u64::from_be_bytes(<[u8; 8]>::try_from(&k[..])?))
                    }
                    None => None,
                };
                r.open_ok = true;
            }
            Err(e) => r.open_err = e.to_string(),
        }
        Ok(r)
    }
}
