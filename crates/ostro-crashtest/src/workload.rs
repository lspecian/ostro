//! Crash-saga load generator over HTTP with an fsynced ack log (plan 05-19).
//!
//! Every run is planned up front from a seeded RNG and the plan travels inside the input
//! (`input.plan`, ignored by the guest), so a resumed harness re-derives every follow-up
//! request from the ack log alone:
//!
//! - start with `idempotency_key = id` (10% are followed by a deliberate duplicate start
//!   with the same key and input, which must return the same run id);
//! - signals `go` then `extra`, each with a fixed `request_id` that is also embedded in the
//!   payload (W9). 10% of runs get `go` right after the start ack, before their first
//!   activation (the D24/B1 pre-start shape). 10% get no signal and a short wait timeout
//!   (deterministic `null` go/extra, exercises wait timeouts);
//! - 5% are cancelled and 2% terminated at a random point;
//! - 2% carry a 300 KiB pad, so RunStarted's input goes through the blob store.
//!
//! [`AckLog`] writes an intent line before a request is first sent and an ack line only
//! after a 2xx (or a rejection line after a 409 `run_terminal`), each `sync_data`'d.
//! Failed requests (connection refused while the server is down, timeouts, 5xx) are retried
//! with the same key / request_id until they get an answer.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::Rng;
use crate::check::{Ledger, Record, Stop};

/// Workflow name of the workload.
pub const WORKFLOW: &str = "crash-saga";

// --------------------------------------------------------------------------- ack log

/// Append-only, fsync-per-record log of intents and acks, mirrored in a [`Ledger`].
pub struct AckLog {
    file: Mutex<File>,
    ledger: Mutex<Ledger>,
    records: AtomicU64,
}

/// Every complete record of an ack log file (a torn last line is ignored).
pub fn read_records(path: &Path) -> std::io::Result<Vec<Record>> {
    let f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        match serde_json::from_str::<Record>(&line) {
            Ok(r) => out.push(r),
            Err(_) => break,
        }
    }
    Ok(out)
}

impl AckLog {
    /// Open (create) `path`, loading the records already in it.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let recs = read_records(path)?;
        // Rewrite a torn tail away: keep exactly the parsed records.
        let mut buf = Vec::new();
        for r in &recs {
            serde_json::to_writer(&mut buf, r).map_err(std::io::Error::other)?;
            buf.push(b'\n');
        }
        let tmp = path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
            ledger: Mutex::new(Ledger::from_records(&recs)),
            records: AtomicU64::new(recs.len() as u64),
        })
    }

    /// Append one record (fsynced) and apply it to the ledger.
    pub fn append(&self, r: Record) {
        let mut line = serde_json::to_vec(&r).expect("encode record");
        line.push(b'\n');
        {
            let mut f = self.file.lock().unwrap_or_else(|e| e.into_inner());
            f.write_all(&line).expect("ack log write");
            f.sync_data().expect("ack log fsync");
        }
        self.records.fetch_add(1, Ordering::Relaxed);
        self.ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .apply(&r);
    }

    pub fn ledger(&self) -> Ledger {
        self.ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn records(&self) -> u64 {
        self.records.load(Ordering::Relaxed)
    }
}

// ------------------------------------------------------------------------------ plan

/// The follow-up plan of one run (stored in `input.plan`).
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    pub signals: bool,
    pub b1: bool,
    pub stop: Option<Stop>,
    pub dup: bool,
}

impl Plan {
    pub fn of(input: &Value) -> Plan {
        let p = input.get("plan").unwrap_or(&Value::Null);
        Plan {
            signals: p.get("signals").and_then(Value::as_bool).unwrap_or(false),
            b1: p.get("b1").and_then(Value::as_bool).unwrap_or(false),
            stop: match p.get("stop").and_then(Value::as_str) {
                Some("cancel") => Some(Stop::Cancel),
                Some("terminate") => Some(Stop::Terminate),
                _ => None,
            },
            dup: p.get("dup").and_then(Value::as_bool).unwrap_or(false),
        }
    }
}

fn key_hash(key: &str) -> u64 {
    let h = blake3::hash(key.as_bytes());
    u64::from_le_bytes(h.as_bytes()[..8].try_into().expect("8 bytes"))
}

/// The input of run `key` (deterministic in `seed` and `key`).
pub fn plan_input(seed: u64, key: &str) -> Value {
    let mut r = Rng::new(seed ^ key_hash(key));
    let steps = r.range(3, 9);
    let mut pattern: Vec<char> = (0..steps)
        .map(|_| match r.below(100) {
            0..70 => '0',
            70..85 => '1',
            85..95 => '2',
            _ => '3',
        })
        .collect();
    if r.chance(8) {
        let k = r.below(steps) as usize;
        pattern[k] = 'x';
    }
    let sleep_ms = if r.chance(20) {
        r.range(2100, 3000)
    } else {
        r.range(10, 400)
    };
    let signals = !r.chance(10);
    let signal_timeout_ms = if signals { 600_000 } else { r.range(100, 600) };
    let b1 = signals && r.chance(10);
    let stop = match r.below(100) {
        0..5 => json!("cancel"),
        5..7 => json!("terminate"),
        _ => Value::Null,
    };
    let dup = r.chance(10);
    let mut input = json!({
        "id": key,
        "steps": steps,
        "fail_pattern": pattern.into_iter().collect::<String>(),
        "sleep_ms": sleep_ms,
        "signal_timeout_ms": signal_timeout_ms,
        "plan": {"signals": signals, "b1": b1, "stop": stop, "dup": dup},
    });
    if r.chance(2) {
        input["pad"] = json!("p".repeat(300 * 1024));
    }
    input
}

/// `request_id` of signal `name` of run `key`.
pub fn request_id(key: &str, name: &str) -> String {
    format!("{key}-{name}")
}

/// Payload of signal `name` of run `key`: embeds its request_id (W9).
pub fn signal_payload(key: &str, name: &str) -> Value {
    json!({"request_id": request_id(key, name), "signal": name, "key": key})
}

// -------------------------------------------------------------------------- workload

#[derive(Clone, Debug)]
enum Action {
    Start {
        key: String,
        input: Value,
        dup: bool,
    },
    Signal {
        key: String,
        run_id: String,
        name: String,
    },
    Stop {
        run_id: String,
        stop: Stop,
    },
}

/// Due-time heap of item ids + the items (action, attempt).
type Queue = (
    BinaryHeap<Reverse<(Instant, u64)>>,
    HashMap<u64, (Action, u32)>,
);

/// Workload counters.
#[derive(Debug, Default)]
pub struct Counters {
    pub requests: AtomicU64,
    pub ok: AtomicU64,
    pub errors: AtomicU64,
    pub server_errors: AtomicU64,
    pub rejected: AtomicU64,
}

/// Load generator against whatever server [`Workload::set_target`] names.
pub struct Workload {
    seed: u64,
    prefix: String,
    log: Arc<AckLog>,
    http: reqwest::Client,
    target: RwLock<Option<String>>,
    queue: Mutex<Queue>,
    next_item: AtomicU64,
    next_run: AtomicU64,
    inflight: AtomicU64,
    generating: AtomicBool,
    stopped: AtomicBool,
    followups: Mutex<HashSet<String>>,
    rng: Mutex<Rng>,
    /// Unexpected answers (404 for an acked run, 4xx other than 409), with detail.
    pub anomalies: Mutex<Vec<String>>,
    pub counters: Counters,
}

impl Workload {
    /// A workload over `log`; runs are named `<prefix>-<n>`. Pending follow-ups of the
    /// records already in the log are queued again (resume).
    pub fn new(seed: u64, prefix: String, log: Arc<AckLog>, http: reqwest::Client) -> Arc<Self> {
        let ledger = log.ledger();
        let w = Arc::new(Workload {
            seed,
            prefix,
            log,
            http,
            target: RwLock::new(None),
            queue: Mutex::new((BinaryHeap::new(), HashMap::new())),
            next_item: AtomicU64::new(0),
            next_run: AtomicU64::new(
                ledger
                    .runs
                    .keys()
                    .filter_map(|k| k.rsplit('-').next()?.parse::<u64>().ok())
                    .max()
                    .map_or(0, |n| n + 1),
            ),
            inflight: AtomicU64::new(0),
            generating: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            followups: Mutex::new(HashSet::new()),
            rng: Mutex::new(Rng::new(seed ^ 0x776f_726b)),
            anomalies: Mutex::new(Vec::new()),
            counters: Counters::default(),
        });
        let now = Instant::now();
        for r in ledger.runs.values() {
            match &r.run_id {
                None => w.push(
                    now,
                    Action::Start {
                        key: r.key.clone(),
                        input: r.input.clone(),
                        dup: false,
                    },
                ),
                Some(run_id) => {
                    w.lock_followups().insert(r.key.clone());
                    let plan = Plan::of(&r.input);
                    if plan.signals {
                        for name in ["go", "extra"] {
                            if !r.signals_acked.contains_key(name)
                                && !r.signals_rejected.contains(name)
                            {
                                w.push(
                                    now,
                                    Action::Signal {
                                        key: r.key.clone(),
                                        run_id: run_id.clone(),
                                        name: name.into(),
                                    },
                                );
                            }
                        }
                    }
                    if let Some(stop) = plan.stop
                        && r.stop_acked.is_none()
                        && !r.stop_rejected
                    {
                        w.push(
                            now,
                            Action::Stop {
                                run_id: run_id.clone(),
                                stop,
                            },
                        );
                    }
                }
            }
        }
        w
    }

    fn lock_followups(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.followups.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn push(&self, due: Instant, a: Action) {
        self.push_attempt(due, a, 0);
    }

    fn push_attempt(&self, due: Instant, a: Action, attempt: u32) {
        let id = self.next_item.fetch_add(1, Ordering::Relaxed);
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        q.0.push(Reverse((due, id)));
        q.1.insert(id, (a, attempt));
    }

    fn pop_due(&self) -> Option<(Action, u32)> {
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        let Reverse((due, id)) = *q.0.peek()?;
        if due > Instant::now() {
            return None;
        }
        q.0.pop();
        self.inflight.fetch_add(1, Ordering::SeqCst);
        q.1.remove(&id)
    }

    fn rand(&self, lo: u64, hi: u64) -> u64 {
        self.rng
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range(lo, hi)
    }

    /// Point the workload at `base` (`http://addr`) or at nothing (server down).
    pub fn set_target(&self, base: Option<String>) {
        *self.target.write().unwrap_or_else(|e| e.into_inner()) = base;
    }

    fn base(&self) -> Option<String> {
        self.target
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Start or stop creating new runs (follow-ups and retries continue either way).
    pub fn set_generating(&self, on: bool) {
        self.generating.store(on, Ordering::SeqCst);
    }

    /// Queued plus in-flight actions.
    pub fn pending(&self) -> usize {
        let q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        q.1.len() + self.inflight.load(Ordering::SeqCst) as usize
    }

    /// Spawn the generator (`rate` new runs per second) and `workers` request workers.
    pub fn spawn(self: &Arc<Self>, rate: f64, workers: usize) -> Vec<tokio::task::JoinHandle<()>> {
        let mut hs = Vec::new();
        let w = Arc::clone(self);
        hs.push(tokio::spawn(async move {
            let period = Duration::from_secs_f64(1.0 / rate.max(0.01));
            while !w.stopped.load(Ordering::SeqCst) {
                if w.generating.load(Ordering::SeqCst) && w.base().is_some() {
                    let n = w.next_run.fetch_add(1, Ordering::SeqCst);
                    let key = format!("{}-{n}", w.prefix);
                    let input = plan_input(w.seed, &key);
                    w.push(
                        Instant::now(),
                        Action::Start {
                            key,
                            input,
                            dup: false,
                        },
                    );
                }
                tokio::time::sleep(period).await;
            }
        }));
        for _ in 0..workers {
            let w = Arc::clone(self);
            hs.push(tokio::spawn(async move {
                while !w.stopped.load(Ordering::SeqCst) {
                    match w.pop_due() {
                        Some((a, attempt)) => {
                            w.execute(a, attempt).await;
                            w.inflight.fetch_sub(1, Ordering::SeqCst);
                        }
                        None => tokio::time::sleep(Duration::from_millis(5)).await,
                    }
                }
            }));
        }
        hs
    }

    /// Stop the generator and workers (they exit after their current request).
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    fn retry(&self, a: Action, attempt: u32) {
        let backoff = self.rand(50, 300) * u64::from(attempt.min(4) + 1);
        self.push_attempt(
            Instant::now() + Duration::from_millis(backoff),
            a,
            attempt + 1,
        );
    }

    fn anomaly(&self, s: String) {
        self.anomalies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(s);
    }

    async fn post(&self, path: &str, body: &Value) -> Result<(u16, Value), ()> {
        let Some(base) = self.base() else {
            return Err(());
        };
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        let r = self
            .http
            .post(format!("{base}{path}"))
            .json(body)
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        match r {
            Ok(r) => {
                let st = r.status().as_u16();
                let v = r.json::<Value>().await.unwrap_or(Value::Null);
                if st >= 500 {
                    self.counters.server_errors.fetch_add(1, Ordering::Relaxed);
                }
                Ok((st, v))
            }
            Err(_) => {
                self.counters.errors.fetch_add(1, Ordering::Relaxed);
                Err(())
            }
        }
    }

    fn schedule_followups(&self, key: &str, run_id: &str, input: &Value) {
        if !self.lock_followups().insert(key.to_owned()) {
            return;
        }
        let plan = Plan::of(input);
        let now = Instant::now();
        if plan.signals {
            let go = if plan.b1 {
                now
            } else {
                now + Duration::from_millis(self.rand(0, 800))
            };
            self.push(
                go,
                Action::Signal {
                    key: key.into(),
                    run_id: run_id.into(),
                    name: "go".into(),
                },
            );
            self.push(
                go + Duration::from_millis(self.rand(0, 800)),
                Action::Signal {
                    key: key.into(),
                    run_id: run_id.into(),
                    name: "extra".into(),
                },
            );
        }
        if let Some(stop) = plan.stop {
            self.push(
                now + Duration::from_millis(self.rand(0, 1500)),
                Action::Stop {
                    run_id: run_id.into(),
                    stop,
                },
            );
        }
        if plan.dup {
            self.push(
                now + Duration::from_millis(self.rand(0, 1000)),
                Action::Start {
                    key: key.into(),
                    input: input.clone(),
                    dup: true,
                },
            );
        }
    }

    async fn execute(&self, a: Action, attempt: u32) {
        match &a {
            Action::Start { key, input, dup } => {
                if attempt == 0 && !dup {
                    self.log.append(Record::StartIntent {
                        key: key.clone(),
                        input: input.clone(),
                    });
                }
                let body = json!({"input": input, "idempotency_key": key});
                match self
                    .post(&format!("/v1/workflows/{WORKFLOW}/runs"), &body)
                    .await
                {
                    Ok((200 | 201, v)) => {
                        let Some(run_id) = v.get("run_id").and_then(Value::as_str) else {
                            self.anomaly(format!("start {key}: 2xx without run_id: {v}"));
                            return self.retry(a, attempt);
                        };
                        self.counters.ok.fetch_add(1, Ordering::Relaxed);
                        self.log.append(Record::StartAck {
                            key: key.clone(),
                            run_id: run_id.to_owned(),
                            created: v.get("created").and_then(Value::as_bool).unwrap_or(false),
                        });
                        if !dup {
                            self.schedule_followups(key, run_id, input);
                        }
                    }
                    Ok((st, v)) if st < 500 && st != 404 && st != 412 => {
                        self.anomaly(format!("start {key}: {st} {v}"));
                        self.retry(a, attempt);
                    }
                    _ => self.retry(a, attempt),
                }
            }
            Action::Signal { key, run_id, name } => {
                let rid = request_id(key, name);
                if attempt == 0 {
                    self.log.append(Record::SignalIntent {
                        run_id: run_id.clone(),
                        name: name.clone(),
                        request_id: rid.clone(),
                        payload: signal_payload(key, name),
                    });
                }
                let body = json!({"payload": signal_payload(key, name), "request_id": rid});
                match self
                    .post(&format!("/v1/runs/{run_id}/signals/{name}"), &body)
                    .await
                {
                    Ok((200, v)) => {
                        self.counters.ok.fetch_add(1, Ordering::Relaxed);
                        self.log.append(Record::SignalAck {
                            run_id: run_id.clone(),
                            name: name.clone(),
                            request_id: rid,
                            duplicate: v.get("duplicate").and_then(Value::as_bool).unwrap_or(false),
                        });
                    }
                    Ok((409, _)) => {
                        self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                        self.log.append(Record::SignalRejected {
                            run_id: run_id.clone(),
                            name: name.clone(),
                            request_id: rid,
                        });
                    }
                    Ok((st, v)) if st < 500 => {
                        self.anomaly(format!("signal {name} {run_id}: {st} {v}"));
                        self.retry(a, attempt);
                    }
                    _ => self.retry(a, attempt),
                }
            }
            Action::Stop { run_id, stop } => {
                if attempt == 0 {
                    self.log.append(Record::StopIntent {
                        run_id: run_id.clone(),
                        stop: *stop,
                    });
                }
                let verb = match stop {
                    Stop::Cancel => "cancel",
                    Stop::Terminate => "terminate",
                };
                match self
                    .post(&format!("/v1/runs/{run_id}/{verb}"), &json!({}))
                    .await
                {
                    Ok((200, _)) => {
                        self.counters.ok.fetch_add(1, Ordering::Relaxed);
                        self.log.append(Record::StopAck {
                            run_id: run_id.clone(),
                            stop: *stop,
                        });
                    }
                    Ok((409, _)) => {
                        self.counters.rejected.fetch_add(1, Ordering::Relaxed);
                        self.log.append(Record::StopRejected {
                            run_id: run_id.clone(),
                            stop: *stop,
                        });
                    }
                    Ok((st, v)) if st < 500 => {
                        self.anomaly(format!("{verb} {run_id}: {st} {v}"));
                        self.retry(a, attempt);
                    }
                    _ => self.retry(a, attempt),
                }
            }
        }
    }
}

// ------------------------------------------------------------------ Phase 8 config

/// Phase 8 campaign settings (plan 08-20): tiny thresholds, so the snapshot service, the
/// bottomless uploader, retention and blob GC all run inside a campaign iteration's kill
/// window (20 ms .. a few seconds after ready).
///
/// - `[snapshots]`: `interval = 1s`, `min_interval = 500ms`, `bytes = 32 KiB`, `keep = 2`;
/// - `[backup]`: `url = file://<shard>/bucket`, `seal_interval = 500ms`,
///   `remote_snapshots_keep = 2` (retention moves `chain_start` and deletes old chunks);
/// - `[blob_gc]`: `interval = 2s`, `grace = 30s` (well above any put-to-apply latency, so
///   the only unlink candidates are the orphans the harness plants, see
///   [`plant_orphan_blobs`]).
///
/// WAL retention runs on its fixed 60 s leader interval (no server knob), so the
/// `trim::after_delete` scenario waits up to [`TRIM_WAIT`] for its first pass.
pub fn phase8_env(shard_dir: &Path) -> Vec<(String, String)> {
    let bucket = shard_dir.join("bucket");
    [
        ("OSTRO_SNAPSHOTS__ENABLED", "true".to_owned()),
        ("OSTRO_SNAPSHOTS__INTERVAL", "1s".to_owned()),
        ("OSTRO_SNAPSHOTS__MIN_INTERVAL", "500ms".to_owned()),
        ("OSTRO_SNAPSHOTS__BYTES", "32768".to_owned()),
        ("OSTRO_SNAPSHOTS__KEEP", "2".to_owned()),
        ("OSTRO_BACKUP__URL", format!("file://{}", bucket.display())),
        ("OSTRO_BACKUP__SEAL_INTERVAL", "500ms".to_owned()),
        ("OSTRO_BACKUP__REMOTE_SNAPSHOTS_KEEP", "2".to_owned()),
        ("OSTRO_BLOB_GC__ENABLED", "true".to_owned()),
        ("OSTRO_BLOB_GC__INTERVAL", "2s".to_owned()),
        ("OSTRO_BLOB_GC__GRACE", "30s".to_owned()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

/// How long the `trim::after_delete` scenario waits for the first retention pass (the
/// leader's fixed 60 s interval plus slack).
pub const TRIM_WAIT: Duration = Duration::from_secs(75);

/// Plant `n` unreferenced blobs (random bytes, stored under their sha256 in the
/// `<data>/blobs/<hex[0:2]>/<hex>` layout) with an mtime 2 h in the past, while the server
/// is down. Blob GC (grace 30 s) deletes them on its first pass, which is what the
/// `blobgc::before_unlink` scenario kills into. Returns the planted paths.
pub fn plant_orphan_blobs(data_dir: &Path, n: usize, seed: u64) -> std::io::Result<Vec<PathBuf>> {
    use sha2::{Digest, Sha256};
    let mut rng = Rng::new(seed ^ 0x000b_10b5);
    let old = std::time::SystemTime::now() - Duration::from_secs(2 * 3600);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let len = rng.range(64, 4096) as usize;
        let bytes = rng.bytes(len);
        let sha: [u8; 32] = Sha256::digest(&bytes).into();
        let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
        let dir = data_dir.join("blobs").join(&hex[..2]);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(&hex);
        let mut f = File::create(&path)?;
        f.write_all(&bytes)?;
        f.set_modified(old)?;
        f.sync_all()?;
        out.push(path);
    }
    Ok(out)
}

/// Delete every `state/pNNNN` directory of a stopped data dir, so the next start rebuilds
/// each partition from its newest local snapshot plus the log tail (or a full replay when
/// it has none). The `snapshot::install_*` scenario uses it to force installs at startup.
/// Returns the number of partitions removed.
pub fn drop_partition_states(data_dir: &Path) -> std::io::Result<usize> {
    let state = data_dir.join("state");
    let mut n = 0;
    let rd = match std::fs::read_dir(&state) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    for e in rd {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if name.len() == 5 && name.starts_with('p') && e.file_type()?.is_dir() {
            std::fs::remove_dir_all(e.path())?;
            n += 1;
        }
    }
    Ok(n)
}

/// Whether partition `p` of a stopped data dir holds at least one local snapshot.
pub fn has_local_snapshot(data_dir: &Path, p: u16) -> bool {
    std::fs::read_dir(data_dir.join("snapshots").join(format!("p{p:04}")))
        .map(|rd| {
            rd.flatten().any(|e| {
                let n = e.file_name().to_string_lossy().into_owned();
                n.ends_with(".osnap") && !n.starts_with(".tmp-")
            })
        })
        .unwrap_or(false)
}
