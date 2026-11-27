//! The operation history of a Jepsen run (14-09, D-P14-19): every client operation with its
//! invocation and completion time, its outcome and, for acknowledged partition writes, the
//! Phase 13 consistency token (`ostro-consistency: p<P>:<epoch>:<offset>`), plus the nemesis
//! events. Written as append-only JSONL (`history.jsonl`, `nemesis.jsonl`).
//!
//! Outcomes follow Jepsen: `ok` = acknowledged (2xx), `fail` = definitely not applied (the
//! request never reached a server, or a 4xx refusal), `info` = indeterminate (timeout,
//! connection reset after sending, 5xx). `info` operations are never counted as lost: they
//! may or may not have taken effect, but an idempotency key still never yields two runs.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// What a client operation did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpKind {
    /// `POST /v1/workflows/{wf}/runs` with an idempotency key.
    Start,
    /// `POST /v1/runs/{id}/signals/{name}` with a request id.
    Signal,
    /// `GET /v1/runs/{id}` (a result poll; reads carry no token).
    Result,
    /// `POST /v1/runs/{id}/cancel`.
    Cancel,
}

/// How a client operation ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Acknowledged.
    Ok,
    /// Definitely not applied.
    Fail,
    /// Indeterminate (unknown).
    Info,
}

/// One client operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOp {
    /// Unique, in invocation order.
    pub op_id: u64,
    /// The client task that issued it.
    pub client: u32,
    /// What it did.
    pub kind: OpKind,
    /// The node it was sent to.
    pub node: u64,
    /// Invocation time (ms since the run's t0).
    pub invoke_ms: u64,
    /// Completion time (ms since the run's t0).
    pub complete_ms: u64,
    /// How it ended.
    pub outcome: Outcome,
    /// The run (`None` for a start that got no run id).
    pub run_id: Option<String>,
    /// The run's idempotency key.
    pub idem_key: String,
    /// The `ostro-consistency` header of the answer (acknowledged writes).
    pub consistency_token: Option<String>,
    /// Signal name (signals), observed status (result polls), HTTP status / error (fail,
    /// info).
    pub detail: Option<String>,
}

/// One nemesis event (an injected fault and its heal).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NemesisEvent {
    /// The schedule slot it executed (`None` for the final heal).
    pub slot: Option<usize>,
    /// Fault kind (`partition:majority_minority`, `kill:leader`, ...).
    pub kind: String,
    /// The resolved target (nodes, links).
    pub target: String,
    /// Injection time (ms since t0).
    pub start_ms: u64,
    /// Heal / restart completion time (ms since t0).
    pub end_ms: u64,
    /// Remarks (restart time, errors).
    pub detail: Option<String>,
}

/// A parsed partition consistency token `p<P>:<epoch>:<offset>`. The offset is the
/// partition's applied cursor on the answering leader right after the write returned (13-13):
/// an upper bound of the write's own offset, and everything below it is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionToken {
    /// Partition.
    pub p: u16,
    /// The leader epoch of the answering node.
    pub epoch: u32,
    /// Its applied cursor (exclusive).
    pub offset: u64,
}

impl PartitionToken {
    /// Parse `p<P>:<epoch>:<offset>` (`None` for a meta token `m:<index>` or garbage).
    pub fn parse(s: &str) -> Option<Self> {
        let rest = s.trim().strip_prefix('p')?;
        let mut it = rest.split(':');
        let p = it.next()?.parse().ok()?;
        let epoch = it.next()?.parse().ok()?;
        let offset = it.next()?.parse().ok()?;
        if it.next().is_some() {
            return None;
        }
        Some(Self { p, epoch, offset })
    }
}

impl std::fmt::Display for PartitionToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "p{}:{}:{}", self.p, self.epoch, self.offset)
    }
}

/// The recorder: thread-safe, append-only, optionally streaming JSONL to files.
pub struct History {
    t0: Instant,
    next: AtomicU64,
    ops: Mutex<Vec<ClientOp>>,
    nemesis: Mutex<Vec<NemesisEvent>>,
    op_sink: Mutex<Option<BufWriter<File>>>,
    nemesis_sink: Mutex<Option<BufWriter<File>>>,
}

impl std::fmt::Debug for History {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("History")
            .field("ops", &self.ops.lock().map(|o| o.len()).unwrap_or(0))
            .finish_non_exhaustive()
    }
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn append_line<T: Serialize>(sink: &Mutex<Option<BufWriter<File>>>, v: &T) {
    if let Some(w) = lock(sink).as_mut() {
        // A failed history write is reported by `flush` (the file is evidence, not state).
        let _ = serde_json::to_writer(&mut *w, v);
        let _ = w.write_all(b"\n");
    }
}

impl History {
    /// An in-memory history whose clock starts now.
    pub fn new() -> Self {
        Self {
            t0: Instant::now(), // clock-exempt: harness timing
            next: AtomicU64::new(0),
            ops: Mutex::new(Vec::new()),
            nemesis: Mutex::new(Vec::new()),
            op_sink: Mutex::new(None),
            nemesis_sink: Mutex::new(None),
        }
    }

    /// Also stream every op to `<dir>/history.jsonl` and every nemesis event to
    /// `<dir>/nemesis.jsonl` (truncated first) as they are recorded.
    pub fn stream_to(&self, dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        *lock(&self.op_sink) = Some(BufWriter::new(File::create(dir.join("history.jsonl"))?));
        *lock(&self.nemesis_sink) = Some(BufWriter::new(File::create(dir.join("nemesis.jsonl"))?));
        Ok(())
    }

    /// Milliseconds since t0.
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.t0.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// A fresh op id (taken at invocation, so ids follow invocation order).
    pub fn next_op_id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    /// Record a completed op.
    pub fn record(&self, op: ClientOp) {
        append_line(&self.op_sink, &op);
        lock(&self.ops).push(op);
    }

    /// Record a nemesis event.
    pub fn record_nemesis(&self, ev: NemesisEvent) {
        append_line(&self.nemesis_sink, &ev);
        lock(&self.nemesis).push(ev);
    }

    /// Every op recorded so far, in completion order.
    pub fn ops(&self) -> Vec<ClientOp> {
        lock(&self.ops).clone()
    }

    /// Every nemesis event so far.
    pub fn nemesis(&self) -> Vec<NemesisEvent> {
        lock(&self.nemesis).clone()
    }

    /// Flush the streaming sinks.
    pub fn flush(&self) -> std::io::Result<()> {
        if let Some(w) = lock(&self.op_sink).as_mut() {
            w.flush()?;
        }
        if let Some(w) = lock(&self.nemesis_sink).as_mut() {
            w.flush()?;
        }
        Ok(())
    }
}

/// Write `items` as JSONL to `path`.
pub fn write_jsonl<T: Serialize>(path: &Path, items: &[T]) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    for it in items {
        serde_json::to_writer(&mut w, it).map_err(std::io::Error::other)?;
        w.write_all(b"\n")?;
    }
    w.flush()
}

/// Read a JSONL file (a torn last line is an error: these files are written whole).
pub fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &Path) -> std::io::Result<Vec<T>> {
    let f = File::open(path)?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str(&line).map_err(std::io::Error::other)?);
    }
    Ok(out)
}
