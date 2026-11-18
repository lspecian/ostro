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
use redb::{Database, Durability, ReadableDatabase, TableDefinition};
use serde_json::json;

const RUNS: TableDefinition<&[u8; 16], &[u8]> = TableDefinition::new("runs");
const RUNS_BY_STATUS: TableDefinition<&[u8; 17], ()> = TableDefinition::new("runs_by_status");
const OUTBOX: TableDefinition<u64, &[u8]> = TableDefinition::new("outbox");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const RUN_VALUE_BYTES: usize = 200;
const OUTBOX_VALUE_BYTES: usize = 64;
const OUTBOX_RING: u64 = 1024;
const COMMANDS_PER_STEP: f64 = 3.0;

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
}

#[derive(Subcommand)]
enum Cmd {
    /// Run one tier and print one JSON result line.
    Bench(BenchOpts),
    /// Run t2 (or another checkpointing tier) forever, printing `CKPT <offset>` after each Immediate commit.
    CrashChild {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, value_enum, default_value = "t2")]
        tier: Tier,
        #[arg(long, default_value_t = 64)]
        batch: u64,
    },
    /// Spawn crash-child, SIGKILL it after `after-ms`, reopen and check invariants.
    CrashTest {
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
}

impl Workload {
    fn new(tier: Tier, keys: Keys, seed: u64) -> Self {
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
        }
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
    let mut w = Workload::new(o.tier, o.keys, o.seed);
    let dur = batch_durability(o.tier);

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
        "loadavg_before": load_before,
        "loadavg_after": load_after,
    });
    println!("{out}");
    let _ = std::fs::remove_file(&path);
    Ok(())
}

fn crash_child(dir: PathBuf, tier: Tier, batch: u64) -> AnyResult<()> {
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

fn crash_test(dir: PathBuf, after_ms: u64, tier: Tier, batch: u64, run: u64) -> AnyResult<()> {
    guard_dir(&dir)?;
    let path = db_path(&dir);
    let exe = std::env::current_exe()?;
    let load_before = loadavg();
    let mut child = Command::new(exe)
        .arg("crash-child")
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
    let file_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    let repairs = Arc::new(AtomicU64::new(0));
    let rc = repairs.clone();
    let t_open = Instant::now();
    let open = Database::builder()
        .set_repair_callback(move |_s| {
            rc.fetch_add(1, Ordering::SeqCst);
        })
        .open(&path);
    let open_us = t_open.elapsed().as_micros() as u64;

    let (open_ok, open_err, applied, checkpointed) = match open {
        Ok(db) => {
            let rtx = db.begin_read()?;
            let meta = rtx.open_table(META)?;
            let a = meta.get("applied_offset")?.map(|g| g.value()).unwrap_or(0);
            let c = meta.get("checkpointed_offset")?.map(|g| g.value()).unwrap_or(0);
            (true, String::new(), a, c)
        }
        Err(e) => (false, e.to_string(), 0, 0),
    };
    let ok_ckpt = checkpointed >= last_ckpt && applied >= last_ckpt;
    let ok_batch = applied % batch == 0;
    let pass = open_ok && ok_ckpt && ok_batch;
    let out = json!({
        "crash_run": run,
        "tier": tier.name(),
        "after_ms": after_ms,
        "child_status": format!("{status}"),
        "ckpts_seen": ckpts_seen,
        "last_ckpt": last_ckpt,
        "open_ok": open_ok,
        "open_err": open_err,
        "open_us": open_us,
        "repair_callbacks": repairs.load(Ordering::SeqCst),
        "db_file_bytes_at_kill": file_bytes,
        "applied_offset": applied,
        "checkpointed_offset": checkpointed,
        "gap_applied_minus_last_ckpt": applied as i64 - last_ckpt as i64,
        "assert_durable_ckpt": ok_ckpt,
        "assert_no_torn_batch": ok_batch,
        "pass": pass,
        "loadavg_before": load_before,
        "loadavg_after": loadavg(),
    });
    println!("{out}");
    let _ = std::fs::remove_file(&path);
    if !pass {
        return Err("crash-test invariant violated".into());
    }
    Ok(())
}

fn main() -> AnyResult<()> {
    match Cli::parse().cmd {
        Cmd::Bench(o) => bench(o),
        Cmd::CrashChild { dir, tier, batch } => crash_child(dir, tier, batch),
        Cmd::CrashTest {
            dir,
            after_ms,
            tier,
            batch,
            run,
        } => crash_test(dir, after_ms, tier, batch, run),
    }
}

/// fjall 3.1.13 variant of the same workload (S3 Task 2 fallback; built only with `--features fjall`).
///
/// Mapping: one keyspace per table; one `WriteBatch` per B-command batch (atomic across keyspaces,
/// default journal mode flushes to OS buffers on commit, i.e. survives a process crash but not
/// power loss = analogue of redb Durability::None). Tiers:
///   t1  every batch committed with `durability(Some(PersistMode::SyncAll))`
///   t2  `db.persist(PersistMode::SyncAll)` every checkpoint-ms or checkpoint-batches batches
///   t3  one `persist(SyncAll)` at the end
#[cfg(feature = "fjall")]
mod fjall_bench {
    use super::*;
    use fjall::{Database as FDb, KeyspaceCreateOptions, PersistMode};

    pub fn bench(o: BenchOpts) -> AnyResult<()> {
        if !matches!(o.tier, Tier::T1 | Tier::T2 | Tier::T3) {
            return Err("fjall engine supports only t1/t2/t3".into());
        }
        guard_dir(&o.dir)?;
        let path = o.dir.join("s3-bench.fjall");
        let _ = std::fs::remove_dir_all(&path);
        let load_before = loadavg();
        let db = FDb::builder(&path).open()?;
        let runs = db.keyspace("runs", KeyspaceCreateOptions::default)?;
        let idx = db.keyspace("runs_by_status", KeyspaceCreateOptions::default)?;
        let outbox = db.keyspace("outbox", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;

        let mut rng = StdRng::seed_from_u64(o.seed);
        let mut run_value = vec![0u8; RUN_VALUE_BYTES];
        rng.fill(&mut run_value[..]);
        let mut outbox_value = vec![0u8; OUTBOX_VALUE_BYTES];
        rng.fill(&mut outbox_value[..]);
        let mut seq_run = 0u64;
        let mut outbox_seq = 0u64;
        let mut offset = 0u64;

        let mut apply = || -> AnyResult<(u64, u64)> {
            let t0 = Instant::now();
            let mut b = db.batch();
            if o.tier == Tier::T1 {
                b = b.durability(Some(PersistMode::SyncAll));
            }
            for _ in 0..o.batch {
                let mut run_id = [0u8; 16];
                match o.keys {
                    Keys::Random => rng.fill(&mut run_id),
                    Keys::Sequential => {
                        run_id[..8].copy_from_slice(&seq_run.to_be_bytes());
                        seq_run += 1;
                    }
                }
                run_value[0] = run_id[0];
                b.insert(&runs, run_id, run_value.as_slice());
                let mut ik = [0u8; 17];
                ik[0] = (offset % 4) as u8;
                ik[1..].copy_from_slice(&run_id);
                b.insert(&idx, ik, []);
                b.insert(&outbox, outbox_seq.to_be_bytes(), outbox_value.as_slice());
                if outbox_seq >= OUTBOX_RING {
                    b.remove(&outbox, (outbox_seq - OUTBOX_RING).to_be_bytes());
                }
                outbox_seq += 1;
                offset += 1;
            }
            b.insert(&meta, "applied_offset", offset.to_le_bytes());
            let tc = Instant::now();
            b.commit()?;
            let end = Instant::now();
            Ok(((end - tc).as_micros() as u64, (end - t0).as_micros() as u64))
        };
        let ckpt = |db: &FDb| -> AnyResult<u64> {
            let t0 = Instant::now();
            db.persist(PersistMode::SyncAll)?;
            Ok(t0.elapsed().as_micros() as u64)
        };

        let mut last = Instant::now();
        let mut since = 0u64;
        let warm_batches = o.warmup / o.batch;
        for _ in 0..warm_batches {
            apply()?;
            if o.tier == Tier::T2 {
                since += 1;
                if last.elapsed() >= Duration::from_millis(o.checkpoint_ms) || since >= o.checkpoint_batches {
                    ckpt(&db)?;
                    last = Instant::now();
                    since = 0;
                }
            }
        }
        if o.tier != Tier::T1 {
            ckpt(&db)?;
            last = Instant::now();
            since = 0;
        }
        let n_batches = o.commands / o.batch;
        let mut commit_us = Vec::with_capacity(n_batches as usize);
        let mut txn_us = Vec::with_capacity(n_batches as usize);
        let mut ckpt_us = Vec::new();
        let start = Instant::now();
        for _ in 0..n_batches {
            let (c, t) = apply()?;
            commit_us.push(c);
            txn_us.push(t);
            if o.tier == Tier::T2 {
                since += 1;
                if last.elapsed() >= Duration::from_millis(o.checkpoint_ms) || since >= o.checkpoint_batches {
                    ckpt_us.push(ckpt(&db)?);
                    last = Instant::now();
                    since = 0;
                }
            }
        }
        if o.tier != Tier::T1 {
            let c = ckpt(&db)?;
            if o.tier == Tier::T2 {
                ckpt_us.push(c);
            }
        }
        let wall = start.elapsed().as_secs_f64();
        let commands = n_batches * o.batch;
        let cmds_per_s = commands as f64 / wall;
        let disk = db.disk_space().unwrap_or(0);
        drop((runs, idx, outbox, meta));
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
            "loadavg_before": load_before,
            "loadavg_after": loadavg(),
        });
        println!("{out}");
        let _ = std::fs::remove_dir_all(&path);
        Ok(())
    }
}
