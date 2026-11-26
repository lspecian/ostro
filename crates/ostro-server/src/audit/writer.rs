//! The group-committing audit writer with fail-closed permits (D-P14-11/12, 14-04).
//!
//! [`AuditWriter`] implements [`AuditSink`]:
//!
//! - **Permits** ([`AuditSink::reserve`]): at most `[audit] queue` records may be reserved
//!   and not yet durable. A reserve waits up to `[audit] reserve_timeout` for a slot and
//!   fails with [`AuditUnavailable`] when none frees up (the writer is stalled or the queue
//!   is full), when the writer is poisoned, or once it is closed; nothing is written then.
//! - **Group commit**: one dedicated thread owns the [`Journal`]. It takes the first record,
//!   waits `[audit] group_commit` for more, appends the batch and fsyncs once; every
//!   committer's future resolves only after that fsync. A write or fsync error **poisons**
//!   the writer: the batch's commits fail ([`AuditCommitError`], 500 `audit_commit_failed`)
//!   and every later reserve fails until the process restarts (fail closed).
//! - **Coalescing** ([`AuditSink::record_coalesced`]): never blocks; events are summarised
//!   per [`CoalesceKey`] into one record with `count`, `first_ts_ms`, `last_ts_ms`, written
//!   when the key's window (`secret_coalesce` / `auth_failure_coalesce`, injected clock) ends
//!   and on [`AuditWriter::flush`].
//! - **Ticks** ([`AuditWriter::tick`], every [`TICK`] from the seam's task): due coalesced
//!   records, retention ([`Journal::retention_plan`]) and the chain-head export every
//!   `head_export_interval` ([`export::emit_head`]).
//! - Committed records are offered to the OTLP export ([`export::Exporter`]), which never
//!   blocks the writer.
//!
//! Every timestamp comes from the injected clock. The seam's `open` has no clock (it runs in
//! `build_ctx`), so the writer thread holds incoming work until [`AuditWriter::set_clock`]
//! (called by [`start`] and [`flush`] with `P14Ctx.clock`) and only then opens the
//! incarnation's segment.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::thread::JoinHandle as ThreadHandle;
use std::time::Duration;

use ostro_partition::clock::Clock;
use ostro_types::audit::{
    AuditCommitError, AuditEvent, AuditPermit, AuditSink, AuditUnavailable, CoalesceKey,
    CommitFuture, NoopAuditSink, PermitSlot,
};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::chain;
use super::export::{self, AuditExport, ChainHead, ExportItem, Exporter, OtlpExport};
use super::journal::{Coalesced, Record};
use super::segment::Journal;
use super::verify::journal_dir;
use crate::config::{AuditSection, ServerConfig};
use crate::p14::{P14Ctx, P14Error};

/// Cadence of the seam's tick task (coalescing windows, retention, head export).
pub const TICK: Duration = Duration::from_secs(1);
/// Metric: records made durable.
pub const M_RECORDS: &str = "ostro_audit_records_total";
/// Metric: group-commit fsyncs.
pub const M_FSYNCS: &str = "ostro_audit_fsyncs_total";
/// Metric: reserves refused (fail closed).
pub const M_UNAVAILABLE: &str = "ostro_audit_unavailable_total";
/// Metric: coalesced events dropped because the writer was saturated.
pub const M_COALESCED_DROPPED: &str = "ostro_audit_coalesced_dropped_total";

/// Writer settings (from `[audit]` via [`WriterConfig::from_section`]).
#[derive(Clone)]
pub struct WriterConfig {
    pub dir: PathBuf,
    pub node: u64,
    pub incarnation: u64,
    pub segment_bytes: u64,
    /// Permits (records reserved and not yet durable).
    pub queue: usize,
    pub group_commit: Duration,
    pub reserve_timeout: Duration,
    pub retention_ms: u64,
    pub max_bytes: u64,
    pub secret_coalesce_ms: u64,
    pub auth_failure_coalesce_ms: u64,
    pub head_export_interval_ms: u64,
    /// The injected clock; `None` = set later ([`AuditWriter::set_clock`]).
    pub clock: Option<Arc<dyn Clock>>,
    /// Record / head export (OTLP when telemetry logs are on).
    pub export: Option<Arc<dyn AuditExport>>,
}

impl std::fmt::Debug for WriterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterConfig")
            .field("dir", &self.dir)
            .field("node", &self.node)
            .field("segment_bytes", &self.segment_bytes)
            .field("queue", &self.queue)
            .field("export", &self.export.is_some())
            .finish_non_exhaustive()
    }
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl WriterConfig {
    /// `[audit]` defaults for `dir`.
    pub fn new(dir: impl Into<PathBuf>, node: u64, incarnation: u64) -> Self {
        Self::from_section(&AuditSection::default(), dir.into(), node, incarnation)
    }

    /// From an `[audit]` section.
    pub fn from_section(a: &AuditSection, dir: PathBuf, node: u64, incarnation: u64) -> Self {
        Self {
            dir,
            node,
            incarnation,
            segment_bytes: a.segment_bytes,
            queue: usize::try_from(a.queue).unwrap_or(usize::MAX).max(1),
            group_commit: a.group_commit.get(),
            reserve_timeout: a.reserve_timeout.get(),
            retention_ms: ms(a.retention.get()),
            max_bytes: a.max_bytes,
            secret_coalesce_ms: ms(a.secret_coalesce.get()),
            auth_failure_coalesce_ms: ms(a.auth_failure_coalesce.get()),
            head_export_interval_ms: ms(a.head_export_interval.get()),
            clock: None,
            export: None,
        }
    }
}

/// Counters (tests and the soak).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriterStats {
    /// Records made durable (control records included).
    pub records: u64,
    /// Successful segment fsyncs.
    pub fsyncs: u64,
    /// Group commits of API records.
    pub batches: u64,
    /// Export copies dropped on a full export channel.
    pub export_dropped: u64,
    /// Coalesced events dropped on a saturated writer.
    pub coalesced_dropped: u64,
    /// Chain heads published.
    pub heads: u64,
}

#[derive(Default)]
struct PermitState {
    reserved: usize,
    poisoned: Option<String>,
    stalled: bool,
    closed: bool,
}

struct Shared {
    st: Mutex<PermitState>,
    cv: Condvar,
    queue: usize,
    records: AtomicU64,
    fsyncs: AtomicU64,
    batches: AtomicU64,
    heads: AtomicU64,
    export_dropped: AtomicU64,
    coalesced_pending: AtomicUsize,
    coalesced_dropped: AtomicU64,
    head: Mutex<(u64, String)>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, PermitState> {
        self.st.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn release(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut st = self.lock();
        st.reserved = st.reserved.saturating_sub(n);
        drop(st);
        self.cv.notify_all();
    }

    fn poison(&self, why: String) {
        let mut st = self.lock();
        if st.poisoned.is_none() {
            error!(error = %why, "audit journal write failed: the writer is poisoned (fail closed until restart)");
            st.poisoned = Some(why);
        }
        drop(st);
        self.cv.notify_all();
    }

    fn poisoned(&self) -> Option<String> {
        self.lock().poisoned.clone()
    }

    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
    fn set_stalled(&self, v: bool) {
        let mut st = self.lock();
        if st.stalled != v {
            st.stalled = v;
            drop(st);
            self.cv.notify_all();
        }
    }
}

type Done = oneshot::Sender<Result<(), AuditCommitError>>;

enum Msg {
    SetClock(Arc<dyn Clock>),
    Record {
        ev: AuditEvent,
        done: Done,
    },
    Coalesced {
        ev: AuditEvent,
        key: CoalesceKey,
        ts_ms: Option<u64>,
    },
    Tick {
        done: oneshot::Sender<()>,
    },
    Flush {
        done: oneshot::Sender<()>,
    },
    Close,
}

/// The audit journal writer.
pub struct AuditWriter {
    shared: Arc<Shared>,
    tx: Sender<Msg>,
    thread: Mutex<Option<ThreadHandle<()>>>,
    reserve_timeout: Duration,
    clock: OnceLock<Arc<dyn Clock>>,
    dir: PathBuf,
}

impl std::fmt::Debug for AuditWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditWriter")
            .field("dir", &self.dir)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl AuditWriter {
    /// Open the journal in `cfg.dir` (recovering a torn tail) and start the writer thread.
    pub fn open(cfg: WriterConfig) -> io::Result<Arc<Self>> {
        let journal = Journal::open(&cfg.dir, cfg.node, cfg.incarnation, cfg.segment_bytes)?;
        let rec = journal.recovery().clone();
        if rec.truncated_bytes > 0 {
            warn!(
                dir = %cfg.dir.display(),
                truncated_bytes = rec.truncated_bytes,
                last_good_seq = rec.last_good_seq,
                "audit journal: truncated a torn tail"
            );
        }
        let (seq, hash) = journal.head();
        let shared = Arc::new(Shared {
            st: Mutex::new(PermitState::default()),
            cv: Condvar::new(),
            queue: cfg.queue.max(1),
            records: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            heads: AtomicU64::new(0),
            export_dropped: AtomicU64::new(0),
            coalesced_pending: AtomicUsize::new(0),
            coalesced_dropped: AtomicU64::new(0),
            head: Mutex::new((
                seq,
                if seq == 0 {
                    String::new()
                } else {
                    chain::to_hex(&hash)
                },
            )),
        });
        let exporter = match &cfg.export {
            Some(t) => Some(Exporter::spawn(Arc::clone(t), export::EXPORT_QUEUE)?),
            None => None,
        };
        let (tx, rx) = mpsc::channel();
        let clock = cfg.clock.clone();
        let worker = Worker {
            journal,
            shared: Arc::clone(&shared),
            exporter,
            clock: None,
            coalescer: BTreeMap::new(),
            last_head_ms: 0,
            cfg: cfg.clone(),
        };
        let thread = std::thread::Builder::new()
            .name("ostro-audit-writer".into())
            .spawn(move || worker.run(rx))?;
        let w = Arc::new(Self {
            shared,
            tx,
            thread: Mutex::new(Some(thread)),
            reserve_timeout: cfg.reserve_timeout,
            clock: OnceLock::new(),
            dir: cfg.dir,
        });
        if let Some(c) = clock {
            w.set_clock(c);
        }
        Ok(w)
    }

    /// Hand the writer its clock (first call wins). Work submitted earlier waits for it.
    pub fn set_clock(&self, clock: Arc<dyn Clock>) {
        if self.clock.set(Arc::clone(&clock)).is_ok() {
            let _ = self.tx.send(Msg::SetClock(clock));
        }
    }

    /// The journal directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Counters.
    pub fn stats(&self) -> WriterStats {
        let s = &self.shared;
        WriterStats {
            records: s.records.load(Ordering::Relaxed),
            fsyncs: s.fsyncs.load(Ordering::Relaxed),
            batches: s.batches.load(Ordering::Relaxed),
            export_dropped: s.export_dropped.load(Ordering::Relaxed),
            coalesced_dropped: s.coalesced_dropped.load(Ordering::Relaxed),
            heads: s.heads.load(Ordering::Relaxed),
        }
    }

    /// `(seq, hash hex)` of the last durable record (seq 0 = empty journal).
    pub fn head(&self) -> (u64, String) {
        self.shared
            .head
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether a write failure poisoned the writer.
    pub fn is_poisoned(&self) -> bool {
        self.shared.poisoned().is_some()
    }

    /// Run one tick now (due coalesced records, retention, head export); resolves once done.
    pub async fn tick(&self) {
        let (done, rx) = oneshot::channel();
        if self.tx.send(Msg::Tick { done }).is_ok() {
            let _ = rx.await;
        }
    }

    /// Write every pending coalesced record, sync, and publish the chain head; resolves once
    /// done (shutdown, tests).
    pub async fn flush(&self) {
        let (done, rx) = oneshot::channel();
        if self.tx.send(Msg::Flush { done }).is_ok() {
            let _ = rx.await;
        }
    }

    /// Flush and stop the writer thread (blocking). Later reserves fail; later commits fail.
    pub fn close(&self) {
        {
            let mut st = self.shared.lock();
            st.closed = true;
        }
        self.shared.cv.notify_all();
        let _ = self.tx.send(Msg::Close);
        let t = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(t) = t
            && t.join().is_err()
        {
            error!("the audit writer thread panicked");
        }
    }

    fn try_take(&self, st: &mut PermitState) -> Option<Result<(), AuditUnavailable>> {
        if let Some(p) = &st.poisoned {
            return Some(Err(AuditUnavailable(format!("audit writer failed: {p}"))));
        }
        if st.closed {
            return Some(Err(AuditUnavailable("audit writer is closed".into())));
        }
        if !st.stalled && st.reserved < self.shared.queue {
            st.reserved += 1;
            return Some(Ok(()));
        }
        None
    }

    fn reserve_slow(&self) -> Result<(), AuditUnavailable> {
        let st = self.shared.lock();
        let (mut st, _) = self
            .shared
            .cv
            .wait_timeout_while(st, self.reserve_timeout, |s| {
                s.poisoned.is_none() && !s.closed && (s.stalled || s.reserved >= self.shared.queue)
            })
            .unwrap_or_else(PoisonError::into_inner);
        match self.try_take(&mut st) {
            Some(r) => r,
            None => Err(AuditUnavailable(format!(
                "no audit permit within {:?} (writer stalled or queue full)",
                self.reserve_timeout
            ))),
        }
    }
}

impl Drop for AuditWriter {
    fn drop(&mut self) {
        self.close();
    }
}

struct Slot {
    shared: Arc<Shared>,
    tx: Sender<Msg>,
    armed: bool,
}

impl PermitSlot for Slot {
    fn commit(mut self: Box<Self>, ev: AuditEvent) -> CommitFuture {
        self.armed = false;
        let (done, rx) = oneshot::channel();
        if self.tx.send(Msg::Record { ev, done }).is_err() {
            self.shared.release(1);
            return Box::pin(async { Err(AuditCommitError("audit writer is closed".into())) });
        }
        Box::pin(async move {
            match rx.await {
                Ok(r) => r,
                Err(_) => Err(AuditCommitError(
                    "audit writer stopped before the record was durable".into(),
                )),
            }
        })
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if self.armed {
            self.shared.release(1);
        }
    }
}

impl AuditSink for AuditWriter {
    fn reserve(&self) -> Result<AuditPermit, AuditUnavailable> {
        let fast = {
            let mut st = self.shared.lock();
            self.try_take(&mut st)
        };
        let r = match fast {
            Some(r) => r,
            None => match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
                Ok(tokio::runtime::RuntimeFlavor::MultiThread) => {
                    tokio::task::block_in_place(|| self.reserve_slow())
                }
                _ => self.reserve_slow(),
            },
        };
        match r {
            Ok(()) => Ok(AuditPermit::new(Box::new(Slot {
                shared: Arc::clone(&self.shared),
                tx: self.tx.clone(),
                armed: true,
            }))),
            Err(e) => {
                metrics::counter!(M_UNAVAILABLE).increment(1);
                Err(e)
            }
        }
    }

    fn record_coalesced(&self, ev: AuditEvent, key: CoalesceKey) {
        let pending = self.shared.coalesced_pending.fetch_add(1, Ordering::AcqRel);
        // The event time is the call time (the writer may get to it later).
        let ts_ms = self.clock.get().map(|c| c.now_ms());
        if pending >= self.shared.queue || self.tx.send(Msg::Coalesced { ev, key, ts_ms }).is_err()
        {
            self.shared.coalesced_pending.fetch_sub(1, Ordering::AcqRel);
            self.shared
                .coalesced_dropped
                .fetch_add(1, Ordering::Relaxed);
            metrics::counter!(M_COALESCED_DROPPED).increment(1);
        }
    }
}

struct Pending {
    ev: AuditEvent,
    count: u64,
    first_ts_ms: u64,
    last_ts_ms: u64,
}

struct Worker {
    journal: Journal,
    shared: Arc<Shared>,
    exporter: Option<Exporter>,
    clock: Option<Arc<dyn Clock>>,
    coalescer: BTreeMap<CoalesceKey, Pending>,
    last_head_ms: u64,
    cfg: WriterConfig,
}

impl Worker {
    fn now(&self) -> u64 {
        self.clock.as_ref().map_or(0, |c| c.now_ms())
    }

    fn run(mut self, rx: Receiver<Msg>) {
        // Hold everything until the clock arrives (the seam's `open` has none).
        let mut backlog: VecDeque<Msg> = VecDeque::new();
        loop {
            match rx.recv() {
                Ok(Msg::SetClock(c)) => {
                    self.clock = Some(c);
                    break;
                }
                Ok(Msg::Close) | Err(_) => {
                    self.fail_backlog(backlog);
                    return;
                }
                Ok(m) => backlog.push_back(m),
            }
        }
        let now = self.now();
        self.last_head_ms = now;
        let started = self
            .journal
            .start_incarnation(now)
            .and_then(|recs| self.journal.sync().map(|()| recs));
        match started {
            Ok(recs) => self.durable(recs),
            Err(e) => self
                .shared
                .poison(format!("opening the audit segment: {e}")),
        }
        loop {
            let msg = match backlog.pop_front() {
                Some(m) => m,
                None => match self.next(&rx) {
                    Some(m) => m,
                    None => break,
                },
            };
            self.stall_point();
            match msg {
                Msg::SetClock(_) => {}
                Msg::Record { ev, done } => {
                    let mut batch = vec![(ev, done)];
                    if !self.cfg.group_commit.is_zero() {
                        std::thread::sleep(self.cfg.group_commit);
                    }
                    while let Ok(m) = rx.try_recv() {
                        match m {
                            Msg::Record { ev, done } => batch.push((ev, done)),
                            other => backlog.push_back(other),
                        }
                    }
                    self.commit_batch(batch);
                }
                Msg::Coalesced { ev, key, ts_ms } => {
                    self.shared.coalesced_pending.fetch_sub(1, Ordering::AcqRel);
                    let at = ts_ms.unwrap_or_else(|| self.now());
                    self.coalesce(ev, key, at);
                }
                Msg::Tick { done } => {
                    self.tick(false);
                    let _ = done.send(());
                }
                Msg::Flush { done } => {
                    self.tick(true);
                    let _ = done.send(());
                }
                Msg::Close => {
                    self.tick(true);
                    break;
                }
            }
        }
        // Commits still queued behind Close fail (the permits are released).
        self.fail_backlog(backlog);
        while let Ok(m) = rx.try_recv() {
            self.fail_msg(m);
        }
        let mut st = self.shared.lock();
        st.closed = true;
        drop(st);
        self.shared.cv.notify_all();
    }

    fn fail_backlog(&self, backlog: VecDeque<Msg>) {
        for m in backlog {
            self.fail_msg(m);
        }
    }

    fn fail_msg(&self, m: Msg) {
        match m {
            Msg::Record { done, .. } => {
                let _ = done.send(Err(AuditCommitError("audit writer is closed".into())));
                self.shared.release(1);
            }
            Msg::Coalesced { .. } => {
                self.shared.coalesced_pending.fetch_sub(1, Ordering::AcqRel);
            }
            Msg::Tick { done } | Msg::Flush { done } => {
                let _ = done.send(());
            }
            Msg::SetClock(_) | Msg::Close => {}
        }
    }

    #[cfg(not(feature = "failpoints"))]
    fn next(&mut self, rx: &Receiver<Msg>) -> Option<Msg> {
        rx.recv().ok()
    }

    /// Failpoints builds poll so an idle writer notices `audit::writer_stall` at once.
    #[cfg(feature = "failpoints")]
    fn next(&mut self, rx: &Receiver<Msg>) -> Option<Msg> {
        loop {
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(m) => return Some(m),
                Err(mpsc::RecvTimeoutError::Timeout) => self.stall_point(),
                Err(mpsc::RecvTimeoutError::Disconnected) => return None,
            }
        }
    }

    /// `audit::writer_stall` (failpoints builds): `return` marks the writer stalled (reserves
    /// fail after `reserve_timeout`) until the failpoint is removed; `pause`/`sleep` block the
    /// thread itself (the queue fills).
    #[cfg(feature = "failpoints")]
    fn stall_point(&self) {
        fn stalled() -> bool {
            fail::fail_point!("audit::writer_stall", |_| true);
            false
        }
        while stalled() {
            self.shared.set_stalled(true);
            std::thread::sleep(Duration::from_millis(10));
        }
        self.shared.set_stalled(false);
    }

    #[cfg(not(feature = "failpoints"))]
    fn stall_point(&self) {}

    fn commit_batch(&mut self, batch: Vec<(AuditEvent, Done)>) {
        let n = batch.len();
        if let Some(p) = self.shared.poisoned() {
            for (_, done) in batch {
                let _ = done.send(Err(AuditCommitError(format!("audit writer failed: {p}"))));
            }
            self.shared.release(n);
            return;
        }
        let now = self.now();
        let mut written = Vec::with_capacity(n + 1);
        let mut dones = Vec::with_capacity(n);
        let mut res: io::Result<()> = Ok(());
        for (ev, done) in batch {
            dones.push(done);
            if res.is_ok() {
                match self.journal.append(ev, None, now) {
                    Ok(recs) => written.extend(recs),
                    Err(e) => res = Err(e),
                }
            }
        }
        let res = res.and_then(|()| self.journal.sync());
        match res {
            Ok(()) => {
                self.shared.batches.fetch_add(1, Ordering::Relaxed);
                self.durable(written);
                for d in dones {
                    let _ = d.send(Ok(()));
                }
            }
            Err(e) => {
                let why = format!("writing the audit journal: {e}");
                self.shared.poison(why.clone());
                for d in dones {
                    let _ = d.send(Err(AuditCommitError(why.clone())));
                }
            }
        }
        self.shared.release(n);
    }

    /// Records are on disk: counters, head, export.
    fn durable(&mut self, recs: Vec<Record>) {
        let s = &self.shared;
        s.fsyncs.store(self.journal.syncs(), Ordering::Relaxed);
        if recs.is_empty() {
            return;
        }
        s.records.fetch_add(recs.len() as u64, Ordering::Relaxed);
        metrics::counter!(M_RECORDS).increment(recs.len() as u64);
        metrics::counter!(M_FSYNCS).absolute(self.journal.syncs());
        let (seq, hash) = self.journal.head();
        *s.head.lock().unwrap_or_else(PoisonError::into_inner) = (seq, chain::to_hex(&hash));
        if let Some(x) = &self.exporter {
            for r in recs {
                if !x.offer(ExportItem::Record(Box::new(r))) {
                    s.export_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn window(&self, key: &CoalesceKey) -> u64 {
        match key {
            CoalesceKey::SecretAccess { .. } => self.cfg.secret_coalesce_ms,
            CoalesceKey::AuthFailure { .. } => self.cfg.auth_failure_coalesce_ms,
        }
    }

    fn coalesce(&mut self, ev: AuditEvent, key: CoalesceKey, at: u64) {
        match self.coalescer.get_mut(&key) {
            Some(p) => {
                p.count += 1;
                p.first_ts_ms = p.first_ts_ms.min(at);
                p.last_ts_ms = p.last_ts_ms.max(at);
            }
            None => {
                if self.coalescer.len() >= self.shared.queue {
                    // Bounded memory: summarise everything pending now.
                    let now = self.now();
                    self.flush_coalesced(true, now);
                }
                self.coalescer.insert(
                    key,
                    Pending {
                        ev,
                        count: 1,
                        first_ts_ms: at,
                        last_ts_ms: at,
                    },
                );
            }
        }
    }

    fn flush_coalesced(&mut self, force: bool, now: u64) {
        if self.coalescer.is_empty() {
            return;
        }
        let due: Vec<CoalesceKey> = self
            .coalescer
            .iter()
            .filter(|(k, p)| force || now >= p.first_ts_ms.saturating_add(self.window(k)))
            .map(|(k, _)| k.clone())
            .collect();
        if due.is_empty() {
            return;
        }
        if self.shared.poisoned().is_some() {
            for k in due {
                self.coalescer.remove(&k);
            }
            return;
        }
        let mut written = Vec::new();
        let mut res: io::Result<()> = Ok(());
        for k in due {
            let Some(p) = self.coalescer.remove(&k) else {
                continue;
            };
            if res.is_err() {
                continue;
            }
            let c = Coalesced {
                count: p.count,
                first_ts_ms: p.first_ts_ms,
                last_ts_ms: p.last_ts_ms,
            };
            match self.journal.append(p.ev, Some(c), now) {
                Ok(recs) => written.extend(recs),
                Err(e) => res = Err(e),
            }
        }
        match res.and_then(|()| self.journal.sync()) {
            Ok(()) => self.durable(written),
            Err(e) => self
                .shared
                .poison(format!("writing coalesced audit records: {e}")),
        }
    }

    fn tick(&mut self, force: bool) {
        let now = self.now();
        self.flush_coalesced(force, now);
        if self.shared.poisoned().is_none() {
            let n = self
                .journal
                .retention_plan(now, self.cfg.retention_ms, self.cfg.max_bytes);
            if n > 0 {
                match self.journal.trim(n, now) {
                    Ok(recs) => {
                        info!(segments = n, dir = %self.journal.dir().display(), "audit retention trimmed segments");
                        self.durable(recs);
                    }
                    Err(e) => self.shared.poison(format!("audit retention: {e}")),
                }
            }
        }
        if force
            || now
                >= self
                    .last_head_ms
                    .saturating_add(self.cfg.head_export_interval_ms)
        {
            self.last_head_ms = now;
            let (seq, hash) = self.journal.head();
            let head = ChainHead {
                node: self.cfg.node,
                incarnation: self.cfg.incarnation,
                seq,
                hash: if seq == 0 {
                    String::new()
                } else {
                    chain::to_hex(&hash)
                },
                ts_ms: now,
            };
            let before = self.exporter.as_ref().map_or(0, Exporter::dropped);
            export::emit_head(&head, self.exporter.as_ref());
            let after = self.exporter.as_ref().map_or(0, Exporter::dropped);
            self.shared
                .export_dropped
                .fetch_add(after - before, Ordering::Relaxed);
            self.shared.heads.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------------
// The seam (p14/mod.rs: build_ctx → open, start → start, before_shutdown → flush)
// ---------------------------------------------------------------------------------

/// Writers opened through [`open`], keyed by the sink's data pointer, so [`start`] and
/// [`flush`] find the concrete writer behind `P14Ctx.audit`.
static REGISTRY: Mutex<Vec<(usize, Weak<AuditWriter>)>> = Mutex::new(Vec::new());

fn sink_key(sink: &Arc<dyn AuditSink>) -> usize {
    Arc::as_ptr(sink).cast::<()>() as usize
}

fn lookup(sink: &Arc<dyn AuditSink>) -> Option<Arc<AuditWriter>> {
    let key = sink_key(sink);
    let mut reg = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
    reg.retain(|(_, w)| w.strong_count() > 0);
    reg.iter()
        .find(|(k, _)| *k == key)
        .and_then(|(_, w)| w.upgrade())
}

/// Whether OTLP logs are on for this server (`[telemetry] otlp_endpoint` + `logs`) and the
/// audit export is wanted (`[audit] otlp`).
fn otlp_enabled(cfg: &ServerConfig) -> bool {
    cfg.audit.otlp && cfg.telemetry.otlp_endpoint.is_some() && cfg.telemetry.logs
}

/// Open the journal (`[audit] dir`, default `<data>/audit`) and return its sink;
/// [`NoopAuditSink`] with `[audit] enabled = false`. A journal that cannot be opened (or a
/// modified one, see [`Journal::open`]) fails startup.
pub fn open(
    cfg: &ServerConfig,
    data_dir: &Path,
    node_id: u64,
    incarnation: u64,
) -> Result<Arc<dyn AuditSink>, P14Error> {
    if !cfg.audit.enabled {
        return Ok(Arc::new(NoopAuditSink));
    }
    let dir = journal_dir(cfg, data_dir);
    let mut wc = WriterConfig::from_section(&cfg.audit, dir.clone(), node_id, incarnation);
    if otlp_enabled(cfg) {
        wc.export = Some(Arc::new(OtlpExport::pipeline()));
    }
    let w = AuditWriter::open(wc)
        .map_err(|e| P14Error::new(format!("audit journal {}: {e}", dir.display())))?;
    let sink: Arc<dyn AuditSink> = w.clone();
    REGISTRY
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((sink_key(&sink), Arc::downgrade(&w)));
    Ok(sink)
}

/// The writer's background task: hands the writer `P14Ctx.clock`, ticks every [`TICK`]
/// (coalescing windows, retention, head export) and, once `stop` fires, flushes and closes
/// the writer. `None` when audit is off.
pub fn start(ctx: &P14Ctx, stop: CancellationToken) -> Option<JoinHandle<()>> {
    let w = lookup(&ctx.audit)?;
    w.set_clock(Arc::clone(&ctx.clock));
    let weak = Arc::downgrade(&w);
    drop(w);
    Some(tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(TICK) => {}
            }
            let Some(w) = weak.upgrade() else { return };
            w.tick().await;
        }
        if let Some(w) = weak.upgrade() {
            w.flush().await;
            let _ = tokio::task::spawn_blocking(move || w.close()).await;
        }
    }))
}

/// Flush pending coalesced records and publish the chain head (shutdown).
pub async fn flush(ctx: &P14Ctx) {
    if let Some(w) = lookup(&ctx.audit) {
        w.set_clock(Arc::clone(&ctx.clock));
        w.flush().await;
    }
}
