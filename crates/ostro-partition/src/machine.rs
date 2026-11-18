//! The per-partition apply machine (STOR-05, STOR-07, STOR-11).
//!
//! One dedicated `std::thread` ("ostro-apply-p{N}") per partition:
//! 1. waits until `HW > cursor.next` (or a control message, a log health change, or the
//!    checkpoint deadline);
//! 2. reads verified batches `[cursor.next, HW)` (capped at `max_chunk_bytes` /
//!    `max_chunk_cmds`) and checks that no batch ends above HW;
//! 3. applies the chunk with [`apply_chunk`] into ONE `ApplyBatch` and commits it together
//!    with `applied_offset`, the rolling hash and the cursor (fjall buffered journal flush);
//! 4. publishes `applied`, resolves acks (only now, after the commit) and fans out actions;
//! 5. checkpoints (`checkpointed_offset` + `persist(SyncAll)`) every `checkpoint_every` or
//!    `checkpoint_batches` chunks, on demand and on graceful shutdown.
//!
//! Any read, apply, commit or persist error, a log health failure, or a panic makes the
//! machine `Failed` exactly once: `acks.fail_all`, then the thread exits. There is no retry;
//! reopening replays the log from the state's own `applied_offset`.
//!
//! Tokio is used only through `rt.block_on` for log I/O and waits.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use metrics::{Counter, Histogram};
use ostro_log::{LogHealth, ReplicatedLog};
use ostro_types::Offset;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tracing::{error, info};

use crate::acks::PendingAcks;
use crate::apply::{Action, Cursor, apply_chunk};
use crate::handle::PartitionError;
use crate::store::PartitionStore;

/// Apply machine tuning.
#[derive(Clone, Debug)]
pub struct MachineConfig {
    /// Most commands applied in one chunk (one fjall `WriteBatch`). Default 1024.
    pub max_chunk_cmds: usize,
    /// Read budget per chunk. Default 1 MiB.
    pub max_chunk_bytes: usize,
    /// Checkpoint interval when there is unpersisted state. Default 100 ms.
    pub checkpoint_every: Duration,
    /// Checkpoint after this many committed chunks. Default 256.
    pub checkpoint_batches: u32,
}

impl Default for MachineConfig {
    fn default() -> Self {
        Self {
            max_chunk_cmds: 1024,
            max_chunk_bytes: 1 << 20,
            checkpoint_every: Duration::from_millis(100),
            checkpoint_batches: 256,
        }
    }
}

/// Health of the partition's apply machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionHealth {
    /// Catching up to the HW found at spawn.
    Starting,
    /// Caught up at least once and applying.
    Healthy,
    /// Fail-stop (terminal until the partition is reopened).
    Failed { reason: String },
}

type Reply = oneshot::Sender<Result<Offset, PartitionError>>;

enum Ctl {
    Checkpoint(Reply),
    Shutdown(Reply),
}

struct Shared {
    ctl: mpsc::UnboundedSender<Ctl>,
    applied: watch::Receiver<Offset>,
    durable: watch::Receiver<Offset>,
    health: watch::Receiver<PartitionHealth>,
    actions: broadcast::Sender<Arc<Vec<Action>>>,
    emit: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
}

/// Handle to a partition's apply thread. Cheap to clone. When every clone is dropped the
/// thread drains to the current HW, checkpoints and exits.
#[derive(Clone)]
pub struct MachineHandle {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for MachineHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MachineHandle")
            .field("applied", &*self.shared.applied.borrow())
            .field("health", &*self.shared.health.borrow())
            .finish_non_exhaustive()
    }
}

/// Action fan-out buffer (lagging receivers lose the oldest batches).
const ACTIONS_CAPACITY: usize = 1024;

impl MachineHandle {
    /// Spawn the apply thread for `cursor.partition`, starting at `cursor.next` (the state's
    /// own `applied_offset`).
    pub fn spawn<L: ReplicatedLog>(
        store: Arc<PartitionStore>,
        log: Arc<L>,
        cursor: Cursor,
        cfg: MachineConfig,
        rt: tokio::runtime::Handle,
        acks: Arc<PendingAcks>,
    ) -> MachineHandle {
        let partition = cursor.partition;
        let start = cursor.next;
        let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
        let (applied_tx, applied_rx) = watch::channel(start);
        let (durable_tx, durable_rx) = watch::channel(start);
        let (health_tx, health_rx) = watch::channel(PartitionHealth::Starting);
        let health_tx = Arc::new(health_tx);
        let (actions_tx, _) = broadcast::channel(ACTIONS_CAPACITY);
        let emit = Arc::new(AtomicBool::new(true));
        let label = partition.0.to_string();

        let hw_rx = log.high_watermark();
        let log_health = log.health();
        // Thread start counts as a checkpoint instant: the store was persisted at open or
        // reflects the last checkpoint plus buffered commits.
        #[allow(clippy::disallowed_methods)]
        let now = Instant::now();
        let machine = Machine {
            store,
            log,
            cur: cursor,
            cfg,
            rt,
            acks: Arc::clone(&acks),
            applied_tx,
            durable_tx,
            health_tx: Arc::clone(&health_tx),
            actions_tx: actions_tx.clone(),
            emit: Arc::clone(&emit),
            ctl_rx,
            hw_rx,
            log_health,
            batches_since: 0,
            last_ckpt: now,
            durable: start,
            shutdown: None,
            m_batches: metrics::counter!("ostro_apply_batches_total", "partition" => label.clone()),
            m_cmds: metrics::counter!("ostro_apply_commands_total", "partition" => label.clone()),
            m_batch_secs: metrics::histogram!("ostro_apply_batch_seconds", "partition" => label.clone()),
            m_ckpt_secs: metrics::histogram!("ostro_state_checkpoint_seconds", "partition" => label),
        };

        let join = std::thread::Builder::new()
            .name(format!("ostro-apply-p{}", partition.0))
            .spawn(move || {
                let r = std::panic::catch_unwind(AssertUnwindSafe(move || machine.run()));
                if r.is_err() {
                    let reason = "apply thread panicked".to_string();
                    error!(partition = partition.0, "{reason}");
                    set_failed(&health_tx, &acks, &reason);
                }
            })
            .expect("spawn apply thread");

        MachineHandle {
            shared: Arc::new(Shared {
                ctl: ctl_tx,
                applied: applied_rx,
                durable: durable_rx,
                health: health_rx,
                actions: actions_tx,
                emit,
                join: Mutex::new(Some(join)),
            }),
        }
    }

    /// `cursor.next` after each commit (everything below is applied and visible).
    pub fn applied(&self) -> watch::Receiver<Offset> {
        self.shared.applied.clone()
    }

    /// `checkpointed_offset` after each successful `persist(SyncAll)`.
    pub fn durable_checkpoint(&self) -> watch::Receiver<Offset> {
        self.shared.durable.clone()
    }

    pub fn health(&self) -> watch::Receiver<PartitionHealth> {
        self.shared.health.clone()
    }

    /// Actions of each committed chunk (only while emitting is on).
    pub fn subscribe_actions(&self) -> broadcast::Receiver<Arc<Vec<Action>>> {
        self.shared.actions.subscribe()
    }

    /// `false` during catch-up: actions are discarded.
    pub fn set_emit_actions(&self, on: bool) {
        self.shared.emit.store(on, Ordering::Release);
    }

    /// `Err(Failed)` if the machine has failed or stopped.
    pub fn check_healthy(&self) -> Result<(), PartitionError> {
        match &*self.shared.health.borrow() {
            PartitionHealth::Failed { reason } => Err(PartitionError::Failed(reason.clone())),
            _ => Ok(()),
        }
    }

    fn stopped_err(&self) -> PartitionError {
        match &*self.shared.health.borrow() {
            PartitionHealth::Failed { reason } => PartitionError::Failed(reason.clone()),
            _ => PartitionError::Failed("apply machine stopped".into()),
        }
    }

    async fn request(&self, f: fn(Reply) -> Ctl) -> Result<Offset, PartitionError> {
        let (tx, rx) = oneshot::channel();
        if self.shared.ctl.send(f(tx)).is_err() {
            return Err(self.stopped_err());
        }
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(self.stopped_err()),
        }
    }

    /// Checkpoint now; returns the checkpointed offset.
    pub async fn force_checkpoint(&self) -> Result<Offset, PartitionError> {
        self.request(Ctl::Checkpoint).await
    }

    /// Resolves once `applied > off`; `Err` if the machine fails or stops first.
    pub async fn wait_applied(&self, off: Offset) -> Result<(), PartitionError> {
        let mut applied = self.applied();
        let mut health = self.health();
        tokio::select! {
            biased;
            r = async { applied.wait_for(|a| *a > off).await.map(|_| ()) } => {
                r.map_err(|_| self.stopped_err())
            }
            reason = wait_failed(&mut health) => Err(PartitionError::Failed(reason)),
        }
    }

    /// Graceful shutdown: drain to the current HW, final checkpoint, join the thread.
    /// Returns the final checkpointed offset.
    pub async fn shutdown(self) -> Result<Offset, PartitionError> {
        let r = self.request(Ctl::Shutdown).await;
        let join = self
            .shared
            .join
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(j) = join {
            let joined = tokio::task::spawn_blocking(move || j.join()).await;
            if !matches!(joined, Ok(Ok(()))) {
                return Err(PartitionError::Failed("apply thread panicked".into()));
            }
        }
        r
    }
}

/// Resolves with the failure reason once `health` is `Failed` (or its sender is gone).
pub(crate) async fn wait_failed(health: &mut watch::Receiver<PartitionHealth>) -> String {
    loop {
        if let PartitionHealth::Failed { reason } = &*health.borrow_and_update() {
            return reason.clone();
        }
        if health.changed().await.is_err() {
            return match &*health.borrow() {
                PartitionHealth::Failed { reason } => reason.clone(),
                _ => "apply machine stopped".into(),
            };
        }
    }
}

/// Set `Failed` exactly once and fail every waiter.
fn set_failed(health: &watch::Sender<PartitionHealth>, acks: &PendingAcks, reason: &str) {
    let first = health.send_if_modified(|h| {
        if matches!(h, PartitionHealth::Failed { .. }) {
            false
        } else {
            *h = PartitionHealth::Failed {
                reason: reason.to_string(),
            };
            true
        }
    });
    if first {
        error!(reason, "partition apply machine failed");
    }
    acks.fail_all(reason);
}

enum Wake {
    Hw,
    HwClosed,
    Ctl(Option<Ctl>),
    LogHealth,
    Timer,
}

struct Machine<L: ReplicatedLog> {
    store: Arc<PartitionStore>,
    log: Arc<L>,
    cur: Cursor,
    cfg: MachineConfig,
    rt: tokio::runtime::Handle,
    acks: Arc<PendingAcks>,
    applied_tx: watch::Sender<Offset>,
    durable_tx: watch::Sender<Offset>,
    health_tx: Arc<watch::Sender<PartitionHealth>>,
    actions_tx: broadcast::Sender<Arc<Vec<Action>>>,
    emit: Arc<AtomicBool>,
    ctl_rx: mpsc::UnboundedReceiver<Ctl>,
    hw_rx: watch::Receiver<Offset>,
    log_health: watch::Receiver<LogHealth>,
    batches_since: u32,
    last_ckpt: Instant,
    durable: Offset,
    /// `(drain target HW, reply)` once a shutdown was requested.
    shutdown: Option<(Offset, Option<Reply>)>,
    m_batches: Counter,
    m_cmds: Counter,
    m_batch_secs: Histogram,
    m_ckpt_secs: Histogram,
}

impl<L: ReplicatedLog> Machine<L> {
    fn run(mut self) {
        let pid = self.cur.partition.0;
        match self.run_inner() {
            Ok(()) => info!(partition = pid, applied = %self.cur.next, "apply machine stopped"),
            Err(reason) => self.fail(&reason),
        }
    }

    fn fail(&mut self, reason: &str) {
        set_failed(&self.health_tx, &self.acks, reason);
        let err = || Err(PartitionError::Failed(reason.to_string()));
        if let Some((_, Some(tx))) = self.shutdown.take() {
            let _ = tx.send(err());
        }
        self.ctl_rx.close();
        while let Ok(c) = self.ctl_rx.try_recv() {
            match c {
                Ctl::Checkpoint(tx) | Ctl::Shutdown(tx) => {
                    let _ = tx.send(err());
                }
            }
        }
    }

    fn run_inner(&mut self) -> Result<(), String> {
        loop {
            while let Ok(c) = self.ctl_rx.try_recv() {
                self.on_ctl(Some(c))?;
            }
            self.check_log_health()?;

            if let Some((target, _)) = &self.shutdown
                && self.cur.next >= *target
            {
                return self.finish();
            }

            let hw = *self.hw_rx.borrow_and_update();
            if hw > self.cur.next {
                self.apply_once(hw)?;
                self.maybe_checkpoint()?;
                continue;
            }

            self.health_tx.send_if_modified(|h| {
                if *h == PartitionHealth::Starting {
                    *h = PartitionHealth::Healthy;
                    true
                } else {
                    false
                }
            });
            self.maybe_checkpoint()?;

            match self.wait() {
                Wake::Hw | Wake::LogHealth | Wake::Timer => {}
                Wake::HwClosed => return Err("log high-watermark channel closed".into()),
                Wake::Ctl(c) => self.on_ctl(c)?,
            }
        }
    }

    fn on_ctl(&mut self, c: Option<Ctl>) -> Result<(), String> {
        match c {
            Some(Ctl::Checkpoint(tx)) => match self.checkpoint() {
                Ok(o) => {
                    let _ = tx.send(Ok(o));
                }
                Err(reason) => {
                    let _ = tx.send(Err(PartitionError::Failed(reason.clone())));
                    return Err(reason);
                }
            },
            Some(Ctl::Shutdown(tx)) => {
                if self.shutdown.is_none() {
                    self.shutdown = Some((*self.hw_rx.borrow(), Some(tx)));
                } else {
                    let _ = tx.send(Err(PartitionError::Failed("already shutting down".into())));
                }
            }
            // Every handle dropped: graceful stop without a reply.
            None => {
                if self.shutdown.is_none() {
                    self.shutdown = Some((*self.hw_rx.borrow(), None));
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), String> {
        let reply = self.shutdown.take().and_then(|(_, tx)| tx);
        match self.checkpoint() {
            Ok(o) => {
                self.acks.fail_all("partition shut down");
                if let Some(tx) = reply {
                    let _ = tx.send(Ok(o));
                }
                Ok(())
            }
            Err(reason) => {
                if let Some(tx) = reply {
                    let _ = tx.send(Err(PartitionError::Failed(reason.clone())));
                }
                Err(reason)
            }
        }
    }

    fn check_log_health(&mut self) -> Result<(), String> {
        match &*self.log_health.borrow_and_update() {
            LogHealth::Healthy => Ok(()),
            LogHealth::Failed { reason } => Err(format!("partition log failed: {reason}")),
        }
    }

    /// Block until something may have changed.
    fn wait(&mut self) -> Wake {
        let dirty = self.cur.next > self.durable;
        let deadline = tokio::time::Instant::from_std(if dirty {
            self.last_ckpt + self.cfg.checkpoint_every
        } else {
            // Disabled branch; the value is never awaited.
            self.last_ckpt + Duration::from_secs(86_400)
        });
        let hw_rx = &mut self.hw_rx;
        let ctl_rx = &mut self.ctl_rx;
        let log_health = &mut self.log_health;
        self.rt.block_on(async move {
            tokio::select! {
                r = hw_rx.changed() => if r.is_ok() { Wake::Hw } else { Wake::HwClosed },
                c = ctl_rx.recv() => Wake::Ctl(c),
                _ = log_health.changed() => Wake::LogHealth,
                _ = tokio::time::sleep_until(deadline), if dirty => Wake::Timer,
            }
        })
    }

    fn apply_once(&mut self, hw: Offset) -> Result<(), String> {
        let mut batches = self
            .rt
            .block_on(self.log.read(self.cur.next, hw, self.cfg.max_chunk_bytes))
            .map_err(|e| format!("log read [{}, {hw}) failed: {e}", self.cur.next))?;
        if batches.is_empty() {
            return Err(format!("empty log read below HW at {}", self.cur.next));
        }
        // Cap the chunk at max_chunk_cmds (always at least one batch).
        let mut cmds = 0usize;
        let mut take = 0usize;
        for b in &batches {
            if take > 0 && cmds + b.commands.len() > self.cfg.max_chunk_cmds {
                break;
            }
            cmds += b.commands.len();
            take += 1;
        }
        batches.truncate(take);
        if batches[0].base != self.cur.next {
            return Err(format!(
                "log read returned base {}, expected {}",
                batches[0].base, self.cur.next
            ));
        }
        if let Some(b) = batches.iter().find(|b| b.end() > hw) {
            return Err(format!(
                "apply passed HW: batch [{}, {}) above HW {hw}",
                b.base,
                b.end()
            ));
        }

        #[allow(clippy::disallowed_methods)] // metrics only
        let t0 = Instant::now();
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let outcomes = apply_chunk(&mut b, &mut self.cur, &batches, &mut actions)
            .map_err(|e| format!("apply failed: {e}"))?;
        let mu = self.cur.meta_update();
        b.commit(&mu)
            .map_err(|e| format!("state commit failed: {e}"))?;

        // Only after the commit: publish, ack, act.
        self.applied_tx.send_replace(self.cur.next);
        self.acks.resolve(&outcomes, self.cur.next);
        if !actions.is_empty() && self.emit.load(Ordering::Acquire) {
            let _ = self.actions_tx.send(Arc::new(actions));
        }
        self.batches_since = self.batches_since.saturating_add(1);
        self.m_batches.increment(1);
        self.m_cmds.increment(outcomes.len() as u64);
        self.m_batch_secs.record(t0.elapsed().as_secs_f64());
        Ok(())
    }

    fn maybe_checkpoint(&mut self) -> Result<(), String> {
        if self.cur.next <= self.durable {
            return Ok(());
        }
        // The checkpoint decision is wall-clock driven but never changes logical state:
        // the digest excludes checkpointed_offset.
        #[allow(clippy::disallowed_methods)]
        let elapsed = self.last_ckpt.elapsed();
        if self.batches_since >= self.cfg.checkpoint_batches || elapsed >= self.cfg.checkpoint_every
        {
            self.checkpoint()?;
        }
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<Offset, String> {
        #[allow(clippy::disallowed_methods)] // metrics + cadence only (see maybe_checkpoint)
        let t0 = Instant::now();
        self.store
            .checkpoint(self.cur.next)
            .map_err(|e| format!("state checkpoint failed: {e}"))?;
        self.m_ckpt_secs.record(t0.elapsed().as_secs_f64());
        self.durable = self.cur.next;
        self.durable_tx.send_replace(self.durable);
        self.batches_since = 0;
        self.last_ckpt = t0;
        Ok(self.durable)
    }
}
