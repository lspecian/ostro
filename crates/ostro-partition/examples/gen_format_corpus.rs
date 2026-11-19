//! On-disk format corpus generator (plan 05-12, D18 freeze).
//!
//! ```text
//! cargo run -p ostro-partition --example gen_format_corpus -- --out DIR
//! ```
//!
//! Creates a fresh P=2 data dir under `DIR/data` with a small WAL segment rotation
//! ([`WAL_ROTATION_BYTES`], so the corpus spans several segments; segments are not
//! preallocated, they grow with writes). It then drives about 50 runs through **every
//! command kind**:
//! - `LeaderAnnounced` (two terms: the node restarts mid-way);
//! - `StartRun` with every field (tags, tenant Some/None, traceparent, module sha, run
//!   timeout, schedule-to-start, retention, retry_of, run_started (inline and blob, D24),
//!   idempotency key,
//!   concurrency key, priority, delayed start);
//! - `AttemptGranted`, with and without `after_failure` (`attempt_failures` > 0);
//! - `EventsAppended` with every `Derived` intent: ScheduleTimer, CancelTimer,
//!   SendMessage, EnqueueReady, CompleteRun (every executor status, resumable,
//!   `parent_event` inline and blob), Suspend, RegisterWait (signal / token, with and
//!   without timeout), CancelWait. Events include `StatusPublished`, so `status_seq` is set;
//! - `TimerFired` from the timer service (guest timers, wait timeouts, and reserved ops:
//!   start delay, run timeout, schedule-to-start, expiry) plus explicit duplicate fires;
//! - `InboundBatch` with every outbox kind (StartChild, ChildCompleted inline + blob event,
//!   Signal, CancelRun, CronStart, StartRun from continue-as-new), and `OutboxTruncated`,
//!   both appended by the shuffler;
//! - `SignalRun` (inline and blob event, a duplicate request id, B1 pending slots),
//!   `RequestCancel`, `TerminateRun`, `RetryRun`, `CompleteToken`.
//!
//! The run stops cleanly (outboxes drained, logs idle). It then writes `DIR/format.toml`
//! with, per partition, `{digest, state_hash, leo, applied}` and a per-table digest map
//! (`[partition.tables]`, `name -> blake3`, see [`table_digest`]) for every table that
//! exists now. It also records the total size, the rotation setting and the ostro version.
//!
//! `tests/format_corpus.rs` verifies such a directory: the committed `tests/format-corpus/v0.1`
//! once 05-20 freezes it, otherwise one generated on the fly. Journal events are opaque
//! payloads (apply never decodes them), as in the partition suites.

#![allow(dead_code, clippy::print_stdout, clippy::print_stderr)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use ostro_log::ReplicatedLog;
use ostro_partition::store::meta::K_CHECKPOINTED;
use ostro_partition::{
    ApplyOutcome, Node, NodeConfig, PartitionSlot, RejectReason, StartMode, TABLES, Table,
};
use ostro_types::command::decode_command;
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CancelRun, CancelTimer, CompleteRun, CompleteToken,
    ConflictPolicy, CronStart, Derived, EXPIRE_OP, EnqueueReady, EventsAppended, JournalEvent,
    OutboxMsg, ParentRef, RUN_TIMEOUT_OP, RequestCancel, RetryRun, ScheduleTimer, SendMessage,
    Signal, SignalRun, StartChild, StartRun, TerminalStatus, TerminateRun, TimerFired, WaitKind,
    derived, outbox_msg,
};
use ostro_types::journal::EventKind;
use ostro_types::{
    AttemptToken, Cmd, Command, INLINE_PAYLOAD_LIMIT, PartitionCount, PartitionId, PayloadRef,
    RunId,
};
use tokio::time::timeout;

/// Partition count of the corpus.
pub const P: u16 = 2;
/// WAL segment rotation used for the corpus (default is 250 MB).
pub const WAL_ROTATION_BYTES: u64 = 4 * 1024;
/// A deadline that never comes due.
const NEVER: u64 = u64::MAX / 8;
const T: Duration = Duration::from_secs(30);

/// The node config the corpus is generated (and opened) with.
pub fn node_config(dir: &Path, mode: StartMode) -> NodeConfig {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(P).expect("P > 0"));
    cfg.mode = mode;
    cfg.wal.enable_rotation = true;
    cfg.wal.rotation_size_bytes = WAL_ROTATION_BYTES;
    cfg.leader.shuffler.idle_poll = Duration::from_millis(20);
    cfg
}

pub async fn open(dir: &Path, mode: StartMode) -> Result<Node, String> {
    timeout(Duration::from_secs(60), Node::open(node_config(dir, mode)))
        .await
        .map_err(|_| "node open timed out".to_string())?
        .map_err(|e| format!("node open: {e}"))
}

// ------------------------------------------------------------------- digests / format

/// blake3 over one table, in the layout of `PartitionStore::digest` restricted to that
/// table: `u16 LE name_len ‖ name`, every row in key order as
/// `u32 LE key_len ‖ key ‖ u32 LE value_len ‖ value`, then `u64 LE row_count`. The meta row
/// `checkpointed_offset` (wall-clock checkpoint timing) is skipped, as in the full digest.
pub fn table_digest(node: &Node, p: u16, t: Table) -> Result<([u8; 32], u64), String> {
    let h = node
        .handle(PartitionId(p))
        .ok_or_else(|| format!("p{p} not ready"))?;
    let rows = h
        .reader()
        .prefix(t, &[], usize::MAX)
        .map_err(|e| format!("p{p} {}: {e}", t.name()))?;
    let mut hs = blake3::Hasher::new();
    let name = t.name().as_bytes();
    hs.update(&(name.len() as u16).to_le_bytes());
    hs.update(name);
    let mut n = 0u64;
    for (k, v) in rows {
        if t == Table::Meta && k == K_CHECKPOINTED {
            continue;
        }
        hs.update(&(k.len() as u32).to_le_bytes());
        hs.update(&k);
        hs.update(&(v.len() as u32).to_le_bytes());
        hs.update(&v);
        n += 1;
    }
    hs.update(&n.to_le_bytes());
    Ok((*hs.finalize().as_bytes(), n))
}

/// One partition's recorded state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartFormat {
    pub id: u16,
    /// [`log_census`] of the partition (informational in format.toml).
    pub commands: BTreeMap<String, u64>,
    pub digest: [u8; 32],
    pub state_hash: [u8; 32],
    pub leo: u64,
    pub applied: u64,
    /// `table name -> (digest, rows)` for every table.
    pub tables: BTreeMap<String, ([u8; 32], u64)>,
}

pub async fn part_format(node: &Node, p: u16) -> Result<PartFormat, String> {
    let commands = log_census(node, p).await?;
    let h = match &node.slots()[usize::from(p)] {
        PartitionSlot::Ready(h) => h,
        PartitionSlot::Failed { reason, .. } => return Err(format!("p{p} failed: {reason}")),
    };
    let meta = h
        .store()
        .read_meta()
        .map_err(|e| format!("p{p} meta: {e}"))?;
    let digest = h
        .store()
        .digest()
        .map_err(|e| format!("p{p} digest: {e}"))?;
    let mut tables = BTreeMap::new();
    for t in TABLES {
        tables.insert(t.name().to_string(), table_digest(node, p, t)?);
    }
    Ok(PartFormat {
        id: p,
        commands,
        digest,
        state_hash: meta.state_hash,
        leo: h.log().log_end().0,
        applied: meta.applied.0,
        tables,
    })
}

/// Every command kind, outbox kind (as `InboundBatch.<kind>`) and `Derived` intent (as
/// `EventsAppended.<intent>`) a complete corpus must contain.
pub const CENSUS: [&str; 26] = [
    "LeaderAnnounced",
    "StartRun",
    "AttemptGranted",
    "EventsAppended",
    "TimerFired",
    "InboundBatch",
    "OutboxTruncated",
    "SignalRun",
    "RequestCancel",
    "TerminateRun",
    "RetryRun",
    "CompleteToken",
    "InboundBatch.StartChild",
    "InboundBatch.ChildCompleted",
    "InboundBatch.Signal",
    "InboundBatch.CancelRun",
    "InboundBatch.CronStart",
    "InboundBatch.StartRun",
    "EventsAppended.ScheduleTimer",
    "EventsAppended.CancelTimer",
    "EventsAppended.SendMessage",
    "EventsAppended.EnqueueReady",
    "EventsAppended.CompleteRun",
    "EventsAppended.Suspend",
    "EventsAppended.RegisterWait",
    "EventsAppended.CancelWait",
];

fn outbox_kind_name(k: &outbox_msg::Kind) -> &'static str {
    match k {
        outbox_msg::Kind::StartChild(_) => "StartChild",
        outbox_msg::Kind::ChildCompleted(_) => "ChildCompleted",
        outbox_msg::Kind::Signal(_) => "Signal",
        outbox_msg::Kind::CancelRun(_) => "CancelRun",
        outbox_msg::Kind::CronStart(_) => "CronStart",
        outbox_msg::Kind::StartRun(_) => "StartRun",
    }
}

fn intent_name(i: &derived::Intent) -> &'static str {
    match i {
        derived::Intent::ScheduleTimer(_) => "ScheduleTimer",
        derived::Intent::CancelTimer(_) => "CancelTimer",
        derived::Intent::SendMessage(_) => "SendMessage",
        derived::Intent::EnqueueReady(_) => "EnqueueReady",
        derived::Intent::CompleteRun(_) => "CompleteRun",
        derived::Intent::Suspend(_) => "Suspend",
        derived::Intent::RegisterWait(_) => "RegisterWait",
        derived::Intent::CancelWait(_) => "CancelWait",
    }
}

/// Count every committed command of partition `p` by kind (plus outbox kinds inside
/// `InboundBatch` and intents inside `EventsAppended`), reading the log `[start, HW)`.
pub async fn log_census(node: &Node, p: u16) -> Result<BTreeMap<String, u64>, String> {
    let h = node
        .handle(PartitionId(p))
        .ok_or_else(|| format!("p{p} not ready"))?;
    let log = h.log();
    let hw = *log.high_watermark().borrow();
    let mut next = log.log_start();
    let mut m: BTreeMap<String, u64> = BTreeMap::new();
    while next < hw {
        let batches = log
            .read(next, hw, 1 << 20)
            .await
            .map_err(|e| format!("p{p} read {next}: {e}"))?;
        let last = batches.last().ok_or("short read below HW")?.last();
        for vb in &batches {
            for bytes in &vb.commands {
                let Ok(c) = decode_command(bytes) else {
                    *m.entry("undecodable".into()).or_default() += 1;
                    continue;
                };
                *m.entry(c.kind_name().into()).or_default() += 1;
                match &c.cmd {
                    Some(Cmd::InboundBatch(b)) => {
                        for k in b.msgs.iter().filter_map(|x| x.msg.as_ref()?.kind.as_ref()) {
                            *m.entry(format!("InboundBatch.{}", outbox_kind_name(k)))
                                .or_default() += 1;
                        }
                    }
                    Some(Cmd::EventsAppended(e)) => {
                        for i in e.derived.iter().filter_map(|d| d.intent.as_ref()) {
                            *m.entry(format!("EventsAppended.{}", intent_name(i)))
                                .or_default() += 1;
                        }
                    }
                    _ => {}
                }
            }
        }
        next = last + 1;
    }
    Ok(m)
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Total bytes of every regular file under `path`.
pub fn dir_bytes(path: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(path) else {
        return 0;
    };
    rd.filter_map(Result::ok)
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&e.path()),
            Ok(_) => e.metadata().map(|m| m.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}

/// `format.toml` text.
pub fn format_toml(parts: &[PartFormat], total_bytes: u64, runs: usize) -> String {
    let mut s = String::from(
        "# On-disk format corpus (generated by crates/ostro-partition/examples/gen_format_corpus.rs).\n\
         # tests/format_corpus.rs compares state_hash, leo, applied and every per-table digest;\n\
         # the full digest is informational (placeholder keyspaces may be added, RESERVATIONS §6).\n",
    );
    s.push_str("format = \"v0.1\"\n");
    s.push_str(&format!("ostro = \"{}\"\n", env!("CARGO_PKG_VERSION")));
    s.push_str(&format!("partitions = {P}\n"));
    s.push_str(&format!("wal_rotation_bytes = {WAL_ROTATION_BYTES}\n"));
    s.push_str(&format!("total_bytes = {total_bytes}\n"));
    s.push_str(&format!("runs = {runs}\n"));
    for p in parts {
        s.push_str("\n[[partition]]\n");
        s.push_str(&format!("id = {}\n", p.id));
        s.push_str(&format!("digest = \"{}\"\n", hex(&p.digest)));
        s.push_str(&format!("state_hash = \"{}\"\n", hex(&p.state_hash)));
        s.push_str(&format!("leo = {}\n", p.leo));
        s.push_str(&format!("applied = {}\n", p.applied));
        s.push_str("\n[partition.tables]\n");
        for (name, (d, _)) in &p.tables {
            s.push_str(&format!("{name} = \"{}\"\n", hex(d)));
        }
        s.push_str("\n[partition.rows]\n");
        for (name, (_, n)) in &p.tables {
            s.push_str(&format!("{name} = {n}\n"));
        }
        s.push_str("\n[partition.commands]\n");
        for (name, n) in &p.commands {
            s.push_str(&format!("\"{name}\" = {n}\n"));
        }
    }
    s
}

// ---------------------------------------------------------------------------- driver

/// Driver view of one run.
#[derive(Clone, Debug)]
struct R {
    id: RunId,
    token: Option<AttemptToken>,
}

impl R {
    fn p(&self) -> u16 {
        self.id.partition().0
    }
}

struct Gen {
    node: Node,
    runs: usize,
    n: u64,
    blob: Option<PayloadRef>,
    /// `command kind -> [accepted, rejected, duplicate]` of submitted commands.
    stats: Stats,
}

/// `command kind -> [accepted, rejected, duplicate]`.
pub type Stats = BTreeMap<&'static str, [u32; 3]>;

fn inline(b: &[u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::copy_from_slice(b)))
}

fn intent(i: derived::Intent) -> Derived {
    Derived::new(i)
}

fn send(kind: outbox_msg::Kind) -> Derived {
    intent(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg { kind: Some(kind) }),
    }))
}

fn complete(status: TerminalStatus, resumable: bool, parent_event: Option<PayloadRef>) -> Derived {
    intent(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: inline(b"{\"result\":1}"),
        completed_at_ms: 900,
        resumable,
        parent_event,
    }))
}

impl Gen {
    fn h(&self, p: u16) -> Result<&ostro_partition::PartitionHandle<ostro_log::Rf1Log>, String> {
        self.node
            .handle(PartitionId(p))
            .ok_or_else(|| format!("p{p} not ready"))
    }

    async fn submit(&mut self, p: u16, c: Command) -> Result<ApplyOutcome, String> {
        let kind = c.kind_name();
        let o = timeout(T, self.h(p)?.submit_one(c))
            .await
            .map_err(|_| "submit timed out".to_string())?
            .map_err(|e| format!("submit: {e}"))?;
        let slot = match o {
            ApplyOutcome::Accepted => 0,
            ApplyOutcome::Rejected(_) => 1,
            ApplyOutcome::Duplicate { .. } => 2,
        };
        self.stats.entry(kind).or_default()[slot] += 1;
        Ok(o)
    }

    fn next_seq(&self, r: &R) -> Result<u64, String> {
        Ok(self
            .h(r.p())?
            .read_run(&r.id)
            .map_err(|e| format!("read run: {e}"))?
            .map_or(0, |rec| rec.next_seq))
    }

    /// Seq the first event of the run's next `append` gets (after RunStarted for a run
    /// with an empty journal).
    fn op_base(&self, r: &R) -> Result<u64, String> {
        Ok(self.next_seq(r)?.max(1))
    }

    fn base_start(&mut self, p: u16) -> StartRun {
        self.n += 1;
        let id = RunId::new_v7(PartitionId(p));
        StartRun {
            run_id: id.as_bytes().to_vec(),
            workflow: format!("wf-{}", self.n % 3),
            workflow_version: 1,
            queue: "default".into(),
            input: inline(format!("{{\"n\":{}}}", self.n).as_bytes()),
            idempotency_key: vec![],
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            concurrency_key: String::new(),
            priority: 0,
            ready_at_ms: 100,
            requested_at_ms: 100,
            parent: None,
            rng_seed: vec![7; 32],
            ..Default::default()
        }
    }

    /// A start with every optional field set (`tenant`/`run_started` vary with `rich`).
    fn rich_start(&mut self, p: u16, tenant: bool, run_started: bool) -> StartRun {
        let n = self.n + 1;
        let mut s = self.base_start(p);
        s.idempotency_key = format!("key-{n}").into_bytes();
        s.conflict_policy = ConflictPolicy::Reject as i32;
        s.concurrency_key = format!("ck-{}", n % 2);
        s.priority = (n % 5) as i32 - 2;
        s.module_sha = vec![0x5a; 32];
        s.tags = vec![format!("n:{n}"), "env:corpus".into()];
        s.tenant = if tenant { "acme".into() } else { String::new() };
        s.traceparent = format!("00-{n:032x}-{n:016x}-01");
        s.run_timeout_ms = NEVER;
        s.schedule_to_start_ms = 0;
        s.retention_ms = 0;
        if run_started {
            s.run_started = Some(JournalEvent {
                seq: 0,
                kind: EventKind::RunStarted.code(),
                payload: inline(b"\x01run-started"),
            });
        }
        s
    }

    async fn start(&mut self, s: StartRun) -> Result<Option<R>, String> {
        let id = RunId::from_bytes(&s.run_id).map_err(|e| format!("run id: {e}"))?;
        let o = self.submit(id.partition().0, Command::start_run(s)).await?;
        if o == ApplyOutcome::Accepted {
            self.runs += 1;
            Ok(Some(R { id, token: None }))
        } else {
            Ok(None)
        }
    }

    async fn grant(&self, r: &mut R) -> Result<(), String> {
        let t = timeout(T, self.h(r.p())?.grant_attempt(r.id))
            .await
            .map_err(|_| "grant timed out".to_string())?
            .map_err(|e| format!("grant {}: {e}", r.id))?;
        r.token = Some(t);
        Ok(())
    }

    /// Re-grant after a (simulated) retryable attempt failure.
    async fn grant_after_failure(&mut self, r: &mut R) -> Result<(), String> {
        let t = r.token.ok_or("grant first")?;
        let next = AttemptToken {
            epoch: self.h(r.p())?.epoch(),
            n: t.n + 1,
        };
        let o = self
            .submit(
                r.p(),
                Command::attempt_granted(AttemptGranted {
                    run_id: r.id.as_bytes().to_vec(),
                    token: Some(next.into()),
                    after_failure: true,
                }),
            )
            .await?;
        if o == ApplyOutcome::Accepted {
            r.token = Some(next);
        }
        Ok(())
    }

    /// `EventsAppended` of `kinds` (opaque payloads) plus `derived`; a run's first batch
    /// starts with `RunStarted` at seq 0. Returns the seq of the first of `kinds`.
    async fn append(
        &mut self,
        r: &R,
        kinds: &[EventKind],
        derived: Vec<Derived>,
    ) -> Result<(u64, ApplyOutcome), String> {
        let t = r.token.ok_or("append needs a token")?;
        // An input journaled by apply (a wait timeout fired by the timer service) can move
        // next_seq between the read and the submit: re-read and resubmit, as the executor does.
        for _ in 0..5 {
            let from = self.next_seq(r)?;
            let mut all = Vec::with_capacity(kinds.len() + 1);
            if from == 0 {
                all.push(EventKind::RunStarted);
            }
            all.extend_from_slice(kinds);
            let first = from + (all.len() - kinds.len()) as u64;
            let events = all
                .iter()
                .enumerate()
                .map(|(i, k)| JournalEvent {
                    seq: from + i as u64,
                    kind: k.code(),
                    payload: inline(format!("\x01{k:?}").as_bytes()),
                })
                .collect();
            let o = self
                .submit(
                    r.p(),
                    Command::events_appended(EventsAppended {
                        run_id: r.id.as_bytes().to_vec(),
                        token: Some(AttemptTokenMsg::from(t)),
                        from_seq: from,
                        events,
                        derived: derived.clone(),
                    }),
                )
                .await?;
            if !matches!(o, ApplyOutcome::Rejected(RejectReason::SeqMismatch { .. })) {
                return Ok((first, o));
            }
        }
        Err(format!("run {}: append kept hitting SeqMismatch", r.id))
    }

    async fn blob(&mut self) -> Result<PayloadRef, String> {
        if let Some(b) = &self.blob {
            return Ok(b.clone());
        }
        // Just over the inline limit: one real blob in the corpus blob store.
        let mut bytes = vec![0x01u8];
        bytes.extend((0..INLINE_PAYLOAD_LIMIT).map(|i| (i % 251) as u8));
        let r = timeout(T, self.h(0)?.prepare_payload(Bytes::from(bytes)))
            .await
            .map_err(|_| "blob timed out".to_string())?
            .map_err(|e| format!("blob: {e}"))?;
        self.blob = Some(r.clone());
        Ok(r)
    }

    async fn wait_run(&self, id: &RunId) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + T;
        loop {
            if self
                .h(id.partition().0)?
                .read_run(id)
                .map_err(|e| format!("read run: {e}"))?
                .is_some()
            {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                return Err(format!("run {id} never arrived"));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn outbox_rows(&self) -> usize {
        (0..P)
            .filter_map(|p| self.node.handle(PartitionId(p)))
            .map(|h| {
                h.reader()
                    .prefix(Table::Outbox, &[], usize::MAX)
                    .map_or(0, |r| r.len())
            })
            .sum()
    }

    /// Outboxes empty and no log growth for 200 ms (shuffler and timers idle).
    async fn settle(&self) -> Result<(), String> {
        let ends = || -> Vec<u64> {
            (0..P)
                .filter_map(|p| self.node.handle(PartitionId(p)))
                .map(|h| h.log().log_end().0)
                .collect()
        };
        let deadline = tokio::time::Instant::now() + T;
        let mut last = ends();
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let now = ends();
            if now == last && self.outbox_rows() == 0 {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                return Err("node never settled".into());
            }
            last = now;
        }
    }
}

/// Drive the workload on a live node under `data`; returns the number of runs created and
/// the per-command outcome counts.
async fn drive(data: &Path) -> Result<(usize, Stats), String> {
    let node = open(data, StartMode::Leader).await?;
    let mut g = Gen {
        node,
        runs: 0,
        n: 0,
        blob: None,
        stats: Stats::new(),
    };

    // 1. Starts with every field: tenant Some/None, run_started on some, delayed, keyed.
    let mut fresh = Vec::new();
    for i in 0..24u16 {
        let p = i % P;
        let s = g.rich_start(p, i % 2 == 0, i % 3 == 0);
        if let Some(r) = g.start(s).await? {
            fresh.push(r);
        }
    }
    // Duplicate keyed start (ReturnExisting vs Reject): rejected or deduped.
    let mut dup = g.base_start(0);
    dup.idempotency_key = b"key-1".to_vec();
    g.start(dup).await?;
    // Delayed start (START_DELAY timer, never due) and a past-due delay (fires now).
    let mut delayed = g.base_start(1);
    delayed.ready_at_ms = NEVER;
    g.start(delayed).await?;
    let mut soon = g.base_start(0);
    soon.ready_at_ms = 150;
    soon.requested_at_ms = 120;
    g.start(soon).await?;
    // D24 blob variant (05-20): `StartRun.run_started` (tag 23) whose RunStarted bytes sit in
    // the blob store, as the API produces for an input over the inline limit.
    let blob = g.blob().await?;
    let mut big = g.rich_start(1, true, true);
    if let Some(ev) = big.run_started.as_mut() {
        ev.payload = Some(blob);
    }
    if g.start(big).await?.is_none() {
        return Err("run_started blob start was not accepted".into());
    }

    // 2. Grants (first = false ready rows after EnqueueReady), after-failure re-grants.
    let mut running: Vec<R> = Vec::new();
    for mut r in fresh.into_iter().take(12) {
        g.grant(&mut r).await?;
        running.push(r);
    }
    for r in running.iter_mut().take(3) {
        g.grant_after_failure(r).await?;
        g.grant_after_failure(r).await?;
    }

    // 3. EventsAppended with every intent.
    // a. events incl. StatusPublished (status_seq), timers (past due fires, never due stays,
    //    one cancelled), EnqueueReady.
    {
        let r = &running[0];
        let (from, _) = g
            .append(
                r,
                &[
                    EventKind::StepCompleted,
                    EventKind::StatusPublished,
                    EventKind::TimerStarted,
                    EventKind::TimerStarted,
                    EventKind::TimerStarted,
                ],
                vec![intent(derived::Intent::EnqueueReady(EnqueueReady {
                    ready_at_ms: 200,
                }))],
            )
            .await?;
        // Re-grant (first = false), then timers via their real ops.
        let mut r = r.clone();
        g.grant(&mut r).await?;
        g.append(
            &r,
            &[EventKind::StatusPublished],
            vec![
                intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                    op: from + 2,
                    due_at_ms: 5,
                })),
                intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                    op: from + 3,
                    due_at_ms: NEVER,
                })),
                intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                    op: from + 4,
                    due_at_ms: NEVER - 1,
                })),
                intent(derived::Intent::CancelTimer(CancelTimer {
                    op: from + 4,
                    due_at_ms: NEVER - 1,
                })),
            ],
        )
        .await?;
        running[0] = r;
    }
    // b. Suspend (with a past-due timer that wakes the run again).
    {
        let r = &running[1];
        let from = g.op_base(r)?;
        g.append(
            r,
            &[EventKind::TimerStarted],
            vec![
                intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                    op: from,
                    due_at_ms: 7,
                })),
                Derived::suspend(),
            ],
        )
        .await?;
    }
    // c. Waits: signal wait with a past-due timeout (WaitTimedOut via the timer service), a
    //    signal wait without timeout, a token wait, and a cancelled wait.
    let (signal_run, token_run) = (running[2].clone(), running[3].clone());
    {
        let r = &signal_run;
        let from = g.op_base(r)?;
        g.append(
            r,
            &[
                EventKind::SignalWaitStarted,
                EventKind::SignalWaitStarted,
                EventKind::SignalWaitStarted,
            ],
            vec![
                Derived::register_wait(from, WaitKind::Signal, "go", 9, 9),
                Derived::register_wait(from + 1, WaitKind::Signal, "go", 0, 9),
                Derived::register_wait(from + 2, WaitKind::Signal, "other", NEVER, 9),
            ],
        )
        .await?;
        g.append(
            r,
            &[EventKind::OpCancelled],
            vec![Derived::cancel_wait(from + 2)],
        )
        .await?;
    }
    let token_op = {
        let r = &token_run;
        let from = g.op_base(r)?;
        g.append(
            r,
            &[EventKind::AwaitTokenCreated, EventKind::AwaitTokenCreated],
            vec![
                Derived::register_wait(from, WaitKind::Token, "", NEVER, 10),
                Derived::register_wait(from + 1, WaitKind::Token, "", 0, 10),
            ],
        )
        .await?;
        from
    };
    // d. Completions: completed, failed (resumable, then RetryRun), cancelled; retention
    //    expiry; status_seq on the completing batch.
    {
        let r = &running[4];
        g.append(
            r,
            &[EventKind::StatusPublished, EventKind::RunCompleted],
            vec![complete(TerminalStatus::Completed, false, None)],
        )
        .await?;
        let r = &running[5];
        g.append(
            r,
            &[EventKind::RunFailed],
            vec![complete(TerminalStatus::Failed, true, None)],
        )
        .await?;
        g.submit(
            r.p(),
            Command::retry_run(RetryRun {
                run_id: r.id.as_bytes().to_vec(),
                requested_at_ms: 950,
            }),
        )
        .await?;
        let r = &running[6];
        g.append(
            r,
            &[EventKind::RunFailed],
            vec![complete(TerminalStatus::Cancelled, false, None)],
        )
        .await?;
    }
    // Retention: a run with retention 1 ms completes (EXPIRE fires, the run is deleted), and
    // one with a never-due retention stays.
    for retention in [1u64, NEVER] {
        let mut s = g.base_start(1);
        s.retention_ms = retention;
        s.idempotency_key = format!("ret-{retention}").into_bytes();
        if let Some(mut r) = g.start(s).await? {
            g.grant(&mut r).await?;
            g.append(
                &r,
                &[EventKind::RunCompleted],
                vec![complete(TerminalStatus::Completed, false, None)],
            )
            .await?;
        }
    }
    // Run timeout (fires: TimedOut) and schedule-to-start (fires before any grant).
    let mut s = g.base_start(0);
    s.run_timeout_ms = 1;
    g.start(s).await?;
    let mut s = g.base_start(1);
    s.schedule_to_start_ms = 1;
    g.start(s).await?;

    // e. Cross-partition messages: signal, cancel, cron start (each to the other partition).
    {
        let r = &running[7];
        let other = running
            .iter()
            .find(|o| o.p() != r.p())
            .ok_or("no run on the other partition")?
            .clone();
        let cron = g.base_start(1 - r.p());
        let cancel_target = running[8].clone();
        g.append(
            r,
            &[EventKind::StepCompleted],
            vec![
                send(outbox_msg::Kind::Signal(Signal {
                    target_run: other.id.as_bytes().to_vec(),
                    name: "go".into(),
                    payload: inline(b"\"x\""),
                })),
                send(outbox_msg::Kind::CancelRun(CancelRun {
                    target_run: cancel_target.id.as_bytes().to_vec(),
                    reason: "parent says stop".into(),
                })),
                send(outbox_msg::Kind::CronStart(CronStart {
                    schedule_id: "nightly".into(),
                    fire_time_ms: 1_000,
                    start: Some(cron),
                })),
            ],
        )
        .await?;
    }
    // f. Children: StartChild to both partitions, then ChildCompleted with an inline and a
    //    blob parent_event; a third child stays running (children row kept).
    {
        let parent = running[9].clone();
        let from = g.op_base(&parent)?;
        let mut kids = Vec::new();
        let mut ds = Vec::new();
        for (i, p) in [0u16, 1, 0].into_iter().enumerate() {
            let mut s = g.base_start(p);
            s.parent = Some(ParentRef {
                run_id: parent.id.as_bytes().to_vec(),
                op: from + i as u64,
            });
            kids.push(RunId::from_bytes(&s.run_id).map_err(|e| format!("{e}"))?);
            ds.push(send(outbox_msg::Kind::StartChild(StartChild {
                start: Some(s),
            })));
        }
        g.append(
            &parent,
            &[
                EventKind::ChildStarted,
                EventKind::ChildStarted,
                EventKind::ChildStarted,
            ],
            ds,
        )
        .await?;
        let blob = g.blob().await?;
        for (i, kid) in kids.into_iter().enumerate() {
            g.wait_run(&kid).await?;
            g.runs += 1;
            let mut k = R {
                id: kid,
                token: None,
            };
            g.grant(&mut k).await?;
            let event = match i {
                0 => inline(b"\x01child-completed"),
                1 => Some(blob.clone()),
                _ => continue,
            };
            g.append(
                &k,
                &[EventKind::RunCompleted],
                vec![complete(TerminalStatus::Completed, false, event)],
            )
            .await?;
        }
    }
    // g. Continue-as-new: successor start through the outbox (InboundBatch{StartRun}).
    {
        let r = &running[10];
        let succ = g.base_start(1 - r.p());
        let succ_id = RunId::from_bytes(&succ.run_id).map_err(|e| format!("{e}"))?;
        g.append(
            r,
            &[EventKind::RunContinuedAsNew],
            vec![
                send(outbox_msg::Kind::StartRun(succ)),
                complete(TerminalStatus::ContinuedAsNew, false, None),
            ],
        )
        .await?;
        g.wait_run(&succ_id).await?;
        g.runs += 1;
    }

    // 4. Direct commands: SignalRun (inline, blob, duplicate request id, B1 pending),
    //    CompleteToken (+ duplicate), RequestCancel, TerminateRun, explicit TimerFired.
    let blob = g.blob().await?;
    for (i, event) in [
        inline(b"\x01sig-a"),
        Some(blob.clone()),
        inline(b"\x01sig-b"),
    ]
    .into_iter()
    .enumerate()
    {
        g.submit(
            signal_run.p(),
            Command::signal_run(SignalRun {
                run_id: signal_run.id.as_bytes().to_vec(),
                name: "go".into(),
                event,
                request_id: if i == 2 {
                    b"req-1".to_vec()
                } else {
                    format!("req-{i}").into_bytes()
                },
                sent_at_ms: 1_000 + i as u64,
            }),
        )
        .await?;
    }
    // B1: signals to a never-granted run (pending slots before seq 0).
    let pending = g.rich_start(0, false, false);
    if let Some(r) = g.start(pending).await? {
        for name in ["early", "early", "late"] {
            g.submit(
                r.p(),
                Command::signal_run(SignalRun {
                    run_id: r.id.as_bytes().to_vec(),
                    name: name.into(),
                    event: inline(b"\x01pending"),
                    request_id: vec![],
                    sent_at_ms: 1_100,
                }),
            )
            .await?;
        }
        // Cancel before the first event: flag only.
        g.submit(
            r.p(),
            Command::request_cancel(RequestCancel {
                run_id: r.id.as_bytes().to_vec(),
                reason: "before start".into(),
                requested_at_ms: 1_200,
            }),
        )
        .await?;
    }
    for _ in 0..2 {
        g.submit(
            token_run.p(),
            Command::complete_token(CompleteToken {
                run_id: token_run.id.as_bytes().to_vec(),
                op: token_op,
                event: inline(b"\x01token"),
                completed_at_ms: 1_300,
            }),
        )
        .await?;
    }
    g.submit(
        token_run.p(),
        Command::complete_token(CompleteToken {
            run_id: token_run.id.as_bytes().to_vec(),
            op: token_op + 1,
            event: Some(blob.clone()),
            completed_at_ms: 1_301,
        }),
    )
    .await?;
    let r = &running[11];
    g.submit(
        r.p(),
        Command::request_cancel(RequestCancel {
            run_id: r.id.as_bytes().to_vec(),
            reason: "graceful".into(),
            requested_at_ms: 1_400,
        }),
    )
    .await?;
    g.submit(
        r.p(),
        Command::terminate_run(TerminateRun {
            run_id: r.id.as_bytes().to_vec(),
            reason: "operator".into(),
            terminated_at_ms: 1_500,
        }),
    )
    .await?;
    // Resume the terminated run in place (resumable hard stop), and retry a completed one
    // (rejected: not resumable).
    g.submit(
        r.p(),
        Command::retry_run(RetryRun {
            run_id: r.id.as_bytes().to_vec(),
            requested_at_ms: 1_600,
        }),
    )
    .await?;
    let done = &running[4];
    g.submit(
        done.p(),
        Command::retry_run(RetryRun {
            run_id: done.id.as_bytes().to_vec(),
            requested_at_ms: 1_600,
        }),
    )
    .await?;
    for (op, due) in [(RUN_TIMEOUT_OP, 101u64), (EXPIRE_OP, 901), (0, 0)] {
        g.submit(
            done.p(),
            Command::timer_fired(TimerFired {
                run_id: done.id.as_bytes().to_vec(),
                op,
                due_at_ms: due,
                fired_at_ms: due + 1,
            }),
        )
        .await?;
    }

    // 5. Restart: a second term (LeaderAnnounced), then a little more traffic.
    g.settle().await?;
    let Gen {
        node,
        runs,
        n,
        blob: _,
        stats,
    } = g;
    node.shutdown()
        .await
        .map_err(|e| format!("shutdown: {e}"))?;
    let mut g = Gen {
        node: open(data, StartMode::Leader).await?,
        runs,
        n,
        blob: None,
        stats,
    };
    for i in 0..12u16 {
        let p = i % P;
        let s = g.rich_start(p, i % 2 == 0, i % 4 == 0);
        if let Some(mut r) = g.start(s).await? {
            if i % 3 == 2 {
                continue; // stays ready (first = true)
            }
            g.grant(&mut r).await?;
            let derived = match i % 3 {
                0 => vec![complete(TerminalStatus::Completed, false, None)],
                _ => vec![],
            };
            g.append(
                &r,
                &[EventKind::StepCompleted, EventKind::StatusPublished],
                derived,
            )
            .await?;
        }
    }
    g.settle().await?;
    let (runs, stats) = (g.runs, g.stats.clone());
    g.node
        .shutdown()
        .await
        .map_err(|e| format!("shutdown: {e}"))?;
    Ok((runs, stats))
}

/// What [`generate`] produced.
#[derive(Debug, Clone)]
pub struct Generated {
    pub parts: Vec<PartFormat>,
    pub total_bytes: u64,
    pub runs: usize,
    pub stats: Stats,
}

/// Generate a corpus into `out` (`out/data` + `out/format.toml`).
pub async fn generate(out: &Path) -> Result<Generated, String> {
    let data = out.join("data");
    if data.exists() {
        return Err(format!("{} exists", data.display()));
    }
    std::fs::create_dir_all(&data).map_err(|e| format!("{}: {e}", data.display()))?;
    let (runs, stats) = drive(&data).await?;
    let node = open(&data, StartMode::Inspect).await?;
    let mut parts = Ok(Vec::new());
    for p in 0..P {
        match (part_format(&node, p).await, &mut parts) {
            (Ok(f), Ok(v)) => v.push(f),
            (Err(e), _) => {
                parts = Err(e);
                break;
            }
            _ => {}
        }
    }
    node.shutdown()
        .await
        .map_err(|e| format!("shutdown: {e}"))?;
    let parts = parts?;
    let total = dir_bytes(&data);
    std::fs::write(out.join("format.toml"), format_toml(&parts, total, runs))
        .map_err(|e| format!("format.toml: {e}"))?;
    Ok(Generated {
        parts,
        total_bytes: total,
        runs,
        stats,
    })
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let out = match args.as_slice() {
        [flag, dir] if flag == "--out" => PathBuf::from(dir),
        _ => {
            eprintln!("usage: gen_format_corpus --out DIR");
            return std::process::ExitCode::from(2);
        }
    };
    match generate(&out).await {
        Ok(Generated {
            parts,
            total_bytes: total,
            runs,
            stats,
        }) => {
            for (kind, [a, r, d]) in &stats {
                println!("{kind:<16} accepted {a:>3} rejected {r:>3} duplicate {d:>3}");
            }
            for p in &parts {
                let nonempty: Vec<String> = p
                    .tables
                    .iter()
                    .filter(|(_, (_, n))| *n > 0)
                    .map(|(t, (_, n))| format!("{t}={n}"))
                    .collect();
                println!(
                    "p{}: leo {} applied {} rows [{}]",
                    p.id,
                    p.leo,
                    p.applied,
                    nonempty.join(" ")
                );
            }
            println!(
                "format corpus: {} ({runs} runs, {total} bytes, WAL rotation {WAL_ROTATION_BYTES})",
                out.display()
            );
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("gen_format_corpus: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
