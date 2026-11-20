//! The server half of the effect seam (06-14): the engine's [`EffectDriver`] adapter over
//! the bridge, completion submission through `Cmd::CompleteEffect` and the pre-send gate.
//!
//! # Flow
//!
//! 1. The engine commits `EffectScheduled(op)` (the sink derives `RegisterEffect`, so the
//!    partition holds an `effects` row) and calls [`ServerEffectDriver::start`] (live, or
//!    `redrive = true` from a replaying activation).
//! 2. `start` reads the run record, resolves the **deployed module name** from the run's
//!    `WorkflowVersion.module` (05-07, D23; cached per `(workflow, version)`; never from the
//!    sha-keyed, nameless `ModuleRecord`), applies [`EffectHooks::decorate_live_request`] to
//!    the live copy and hands the effect to [`Bridge::start`] (non-blocking, idempotent per
//!    `(partition, run, op)`).
//! 3. Before every send the bridge asks [`PartitionSendGate`]: proceed only while this node
//!    leads the partition, the run exists and is not terminal, and the `effects` row is
//!    still there (Pitfall 7).
//! 4. The outcome goes to [`PartitionCompletionSink`]: the encoded `EffectCompleted` event
//!    bytes (`complete_effect_payload`, B2), prepared like any payload (inline up to
//!    256 KiB, blob ref above), submitted as `Cmd::CompleteEffect` (P-10). First wins;
//!    the activation's attempt token is never used.
//!
//! An effect is never re-sent from the journal by this module (F1): a re-drive comes only
//! from the engine's replay of a re-issued call, and the journal row stays untouched.
//!
//! # Partition handles
//!
//! The bridge is built before `Node::open` (the executor needs the driver), so the gate and
//! the sink look partitions up in a late-bound [`PartitionTable`]. Every sink that carries
//! the driver registers its partition handle there ([`crate::sink::PartitionSink::with_bridge`]),
//! and the server registers every started partition after `Node::open`.
//!
//! # Leadership
//!
//! "This node leads partition p" = the handle's apply machine is healthy **and** meta's
//! partition assignment names this node with an epoch not above the handle's epoch. Before
//! `SetPartitionLeader` is recorded at startup, meta still holds the previous (lower) epoch,
//! so a re-drive during `Node::open` is not abandoned; once another leader records a higher
//! epoch, every send is.
//!
//! # Phase 8 hooks
//!
//! [`EffectHooks`] (default [`NoopEffectHooks`]) is the seam 08-21 fills in for effect
//! tracing. The signatures are stable; the default changes neither the live request nor the
//! journal.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use ostro_bridge::connector::{CompletionError, CompletionSink, GateDecision, SendGate};
use ostro_bridge::driver::{Bridge, EffectBase};
use ostro_engine::EffectDriver;
use ostro_engine::LiveEffect;
use ostro_engine::sink::RunRef;
use ostro_log::Rf1Log;
use ostro_partition::store::values::RunRecord;
use ostro_partition::{ApplyOutcome, Clock, PartitionHandle, RejectReason, query};
use ostro_types::command::{CompleteEffect, complete_effect_payload};
use ostro_types::journal::{EffectRequest, HttpRequest, RecordedResult};
use ostro_types::live::EffectKind;
use ostro_types::{Command, RunId};

/// The partition handle type of the server.
pub type Handle = PartitionHandle<Rf1Log>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ------------------------------------------------------------------- partition table

/// Late-bound partition handles by partition number (see the module docs).
#[derive(Default)]
pub struct PartitionTable {
    slots: RwLock<BTreeMap<u16, Handle>>,
    closed: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for PartitionTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<u16> = self
            .slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect();
        f.debug_struct("PartitionTable")
            .field("partitions", &parts)
            .finish()
    }
}

impl PartitionTable {
    /// An empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) the handle of `h.partition()`. No-op once [`close`d](Self::close).
    pub fn insert(&self, h: Handle) {
        let p = h.partition().0;
        let mut slots = self.slots.write().unwrap_or_else(PoisonError::into_inner);
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let fresh = slots.get(&p).is_none_or(|cur| cur.epoch() != h.epoch());
        if fresh {
            slots.insert(p, h);
        }
    }

    /// The handle of partition `p`, if registered.
    pub fn get(&self, p: u16) -> Option<Handle> {
        self.slots
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&p)
            .cloned()
    }

    /// Forget every handle and refuse new ones (shutdown: the node is about to close its
    /// partitions, which must not stay referenced from here).
    pub fn close(&self) {
        let mut slots = self.slots.write().unwrap_or_else(PoisonError::into_inner);
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        slots.clear();
    }
}

// ------------------------------------------------------------------- hooks

/// Effect tracing hooks (Phase 8, 08-21). Every method has a no-op default.
pub trait EffectHooks: Send + Sync {
    /// Called in [`ServerEffectDriver::start`] on the **live** copy of an HTTP request (the
    /// intent is already journaled), right before `Bridge::start`, so a header added here
    /// (e.g. `traceparent`) never reaches the journal. Runs on the activation task: must
    /// not block. `run` names the run (08-21: the record carries no run id).
    fn decorate_live_request(
        &self,
        _run: &RunRef,
        _rec: &RunRecord,
        _op: u64,
        _req: &mut HttpRequest,
    ) {
    }

    /// Called by [`PartitionCompletionSink`] after a completion was submitted (accepted or a
    /// duplicate). `started` is when [`ServerEffectDriver::start`] first saw `(run, op)` on
    /// this node.
    fn on_effect_complete(
        &self,
        _run: &RunRef,
        _op: u64,
        _kind: EffectKind,
        _started: SystemTime,
        _ended: SystemTime,
        _outcome: &RecordedResult,
    ) {
    }
}

/// The default [`EffectHooks`]: does nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopEffectHooks;

impl EffectHooks for NoopEffectHooks {}

// ------------------------------------------------------------------- shared state

/// Key of an in-flight effect on this node.
type FxKey = (u16, String, u64);

/// State shared by the driver, the gate and the completion sink.
pub struct EffectSeam {
    table: Arc<PartitionTable>,
    meta: ostro_meta::MetaView,
    node_id: u64,
    cluster_id: String,
    clock: Arc<dyn Clock>,
    hooks: RwLock<Arc<dyn EffectHooks>>,
    /// `(partition, run, op)` -> (kind, first start on this node), for `on_effect_complete`.
    started: Mutex<HashMap<FxKey, (EffectKind, SystemTime)>>,
    /// `(workflow, version)` -> deployed module name (immutable once registered).
    modules: Mutex<HashMap<(String, u32), String>>,
}

impl std::fmt::Debug for EffectSeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectSeam")
            .field("node_id", &self.node_id)
            .field("cluster_id", &self.cluster_id)
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl EffectSeam {
    /// Shared state over `table` and the meta view (leadership and workflow versions).
    pub fn new(
        table: Arc<PartitionTable>,
        meta: ostro_meta::MetaView,
        node_id: u64,
        cluster_id: impl Into<String>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            table,
            meta,
            node_id,
            cluster_id: cluster_id.into(),
            clock,
            hooks: RwLock::new(Arc::new(NoopEffectHooks)),
            started: Mutex::new(HashMap::new()),
            modules: Mutex::new(HashMap::new()),
        }
    }

    /// The partition table.
    pub fn table(&self) -> &Arc<PartitionTable> {
        &self.table
    }

    /// The cluster id (part of every `EffectBase`).
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    fn hooks(&self) -> Arc<dyn EffectHooks> {
        Arc::clone(&self.hooks.read().unwrap_or_else(PoisonError::into_inner))
    }

    fn set_hooks(&self, hooks: Arc<dyn EffectHooks>) {
        *self.hooks.write().unwrap_or_else(PoisonError::into_inner) = hooks;
    }

    /// The deployed module name of `rec` (`WorkflowVersion.module`); `""` when the version
    /// is unknown or unnamed (no egress policy or grant can then match).
    fn module_of(&self, rec: &RunRecord) -> String {
        let key = (rec.workflow.clone(), rec.workflow_version);
        if let Some(m) = lock(&self.modules).get(&key) {
            return m.clone();
        }
        let found = self.meta.read(|s| {
            s.workflow(&rec.workflow, rec.workflow_version)
                .map(|v| v.value.module.clone())
        });
        match found {
            Some(m) if !m.is_empty() => {
                lock(&self.modules).insert(key, m.clone());
                m
            }
            _ => String::new(),
        }
    }

    /// Whether this node currently leads `h`'s partition (module docs).
    fn leads(&self, h: &Handle) -> bool {
        if h.machine().check_healthy().is_err() {
            return false;
        }
        let p = h.partition().0;
        let epoch = h.epoch().0;
        self.meta
            .read(|s| s.partition(p).map(|a| (a.value.leader, a.value.epoch)))
            .is_some_and(|(leader, meta_epoch)| leader == self.node_id && meta_epoch <= epoch)
    }
}

// ------------------------------------------------------------------- driver

/// The engine's [`EffectDriver`] over the bridge (P-1; see the module docs).
pub struct ServerEffectDriver {
    seam: Arc<EffectSeam>,
    bridge: Arc<Bridge>,
}

impl std::fmt::Debug for ServerEffectDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerEffectDriver")
            .field("seam", &self.seam)
            .finish_non_exhaustive()
    }
}

impl ServerEffectDriver {
    /// A driver handing effects to `bridge`.
    pub fn new(seam: Arc<EffectSeam>, bridge: Arc<Bridge>) -> Self {
        Self { seam, bridge }
    }

    /// Install effect hooks (Phase 8, 08-21); shared with the completion sink.
    pub fn set_hooks(&self, hooks: Arc<dyn EffectHooks>) {
        self.seam.set_hooks(hooks);
    }

    /// The bridge behind this driver.
    pub fn bridge(&self) -> &Arc<Bridge> {
        &self.bridge
    }

    /// The shared seam state.
    pub fn seam(&self) -> &Arc<EffectSeam> {
        &self.seam
    }
}

impl EffectDriver for ServerEffectDriver {
    fn start(&self, run: &RunRef, attempt: u32, mut effect: LiveEffect) {
        let Some(h) = self.seam.table.get(run.partition) else {
            tracing::warn!(partition = run.partition, run = %run.run_id, op = effect.op,
                "effect start: partition not registered; it is re-driven by the next activation");
            return;
        };
        let Ok(id) = run.run_id.parse::<RunId>() else {
            tracing::warn!(run = %run.run_id, "effect start: malformed run id");
            return;
        };
        let rec = match h.read_run(&id) {
            Ok(Some(r)) => r,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(run = %run.run_id, op = effect.op, error = %e,
                    "effect start: run record unreadable; it is re-driven by the next activation");
                return;
            }
        };
        let module = self.seam.module_of(&rec);
        if let EffectRequest::Http(req) = &mut effect.request {
            self.seam
                .hooks()
                .decorate_live_request(run, &rec, effect.op, req);
        }
        // clock-exempt: span timing for the tracing hooks only, never used for logic.
        let now = SystemTime::now();
        lock(&self.seam.started)
            .entry((run.partition, run.run_id.clone(), effect.op))
            .or_insert((effect.kind(), now));
        self.bridge.start(
            EffectBase {
                cluster_id: self.seam.cluster_id.clone(),
                partition: run.partition,
                run_id: run.run_id.clone(),
                attempt,
                module,
                tenant: rec.tenant.clone(),
            },
            effect,
        );
    }

    fn cancel(&self, run: &RunRef, op: u64) {
        self.bridge.cancel(run.partition, &run.run_id, op);
        lock(&self.seam.started).remove(&(run.partition, run.run_id.clone(), op));
    }
}

// ------------------------------------------------------------------- gate

/// The pre-send gate (Pitfall 7): `Abandon` unless this node leads the partition, the run
/// exists and is not terminal, and its `effects` row for `op` is still present.
#[derive(Debug)]
pub struct PartitionSendGate {
    seam: Arc<EffectSeam>,
}

impl PartitionSendGate {
    /// A gate over the shared seam state.
    pub fn new(seam: Arc<EffectSeam>) -> Self {
        Self { seam }
    }

    fn decide(&self, partition: u16, run_id: &str, op: u64) -> GateDecision {
        let Some(h) = self.seam.table.get(partition) else {
            return GateDecision::Abandon;
        };
        if !self.seam.leads(&h) {
            return GateDecision::Abandon;
        }
        let Ok(run) = run_id.parse::<RunId>() else {
            return GateDecision::Abandon;
        };
        match h.read_run(&run) {
            Ok(Some(rec)) if !rec.status.is_terminal() => {}
            _ => return GateDecision::Abandon,
        }
        match query::effect_row(&h.reader(), &run, op) {
            Ok(Some(_)) => GateDecision::Proceed,
            _ => GateDecision::Abandon,
        }
    }
}

#[async_trait]
impl SendGate for PartitionSendGate {
    async fn check(&self, partition: u16, run_id: &str, op: u64) -> GateDecision {
        self.decide(partition, run_id, op)
    }
}

// ------------------------------------------------------------------- completions

/// Submits bridge outcomes as `Cmd::CompleteEffect` through the partition handle (P-10).
///
/// | partition outcome | result |
/// |---|---|
/// | `Accepted`, `Duplicate` | `Ok` |
/// | `Rejected(RunTerminal \| RunNotFound)` | `Dropped` |
/// | any other rejection (malformed completion) | `Dropped` (logged) |
/// | not registered, payload write failed, not leader, partition failed, ack lost | `Unavailable` (the driver retries) |
#[derive(Debug)]
pub struct PartitionCompletionSink {
    seam: Arc<EffectSeam>,
}

impl PartitionCompletionSink {
    /// A sink over the shared seam state.
    pub fn new(seam: Arc<EffectSeam>) -> Self {
        Self { seam }
    }
}

#[async_trait]
impl CompletionSink for PartitionCompletionSink {
    async fn complete(
        &self,
        partition: u16,
        run_id: &str,
        op: u64,
        outcome: RecordedResult,
    ) -> Result<(), CompletionError> {
        let h = self.seam.table.get(partition).ok_or_else(|| {
            CompletionError::Unavailable(format!("partition {partition} not registered"))
        })?;
        let run: RunId = run_id
            .parse()
            .map_err(|_| CompletionError::Dropped(format!("malformed run id {run_id:?}")))?;
        let key = (partition, run_id.to_owned(), op);
        let completed_at_ms = self.seam.clock.now_ms();
        let bytes =
            complete_effect_payload(op, &outcome, completed_at_ms.saturating_mul(1_000_000));
        let payload = h
            .prepare_payload(Bytes::from(bytes))
            .await
            .map_err(|e| CompletionError::Unavailable(e.to_string()))?;
        let cmd = Command::complete_effect(CompleteEffect {
            run_id: run.as_bytes().to_vec(),
            op,
            outcome: Some(payload),
            completed_at_ms,
        });
        let res = match h.submit_one(cmd).await {
            Ok(ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. }) => Ok(()),
            Ok(ApplyOutcome::Rejected(RejectReason::RunTerminal | RejectReason::RunNotFound)) => {
                Err(CompletionError::Dropped("run terminal or gone".into()))
            }
            Ok(ApplyOutcome::Rejected(r)) => {
                tracing::warn!(partition, run = run_id, op, reason = ?r, "effect completion rejected");
                Err(CompletionError::Dropped(format!("rejected: {r:?}")))
            }
            Err(e) => Err(CompletionError::Unavailable(e.to_string())),
        };
        if !matches!(res, Err(CompletionError::Unavailable(_)))
            && let Some((kind, started)) = lock(&self.seam.started).remove(&key)
            && res.is_ok()
        {
            // clock-exempt: span timing for the tracing hooks only.
            let ended = SystemTime::now();
            let rref = RunRef {
                partition,
                run_id: run_id.to_owned(),
            };
            self.seam
                .hooks()
                .on_effect_complete(&rref, op, kind, started, ended, &outcome);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::journal::Header;

    /// The default hooks leave the live request byte-identical (headers, url, body).
    #[test]
    fn noop_hooks_leave_live_request_unchanged() {
        let req = HttpRequest {
            method: "POST".into(),
            url: "https://api.example.com/pay?x=1".into(),
            headers: vec![
                Header {
                    name: "Authorization".into(),
                    value: "Bearer {{secret:tok}}".into(),
                },
                Header {
                    name: "X-Trace".into(),
                    value: "a".into(),
                },
            ],
            body: Some(b"{\"amount\":1}".to_vec()),
            timeout_ms: Some(1000),
            idempotency_key: None,
            redact: vec!["Authorization".into()],
        };
        let mut live = req.clone();
        let rec = RunRecord::default();
        let rref = RunRef {
            partition: 0,
            run_id: "r".into(),
        };
        NoopEffectHooks.decorate_live_request(&rref, &rec, 3, &mut live);
        assert_eq!(live, req);
        let hooks: Arc<dyn EffectHooks> = Arc::new(NoopEffectHooks);
        hooks.on_effect_complete(
            &RunRef {
                partition: 0,
                run_id: "r".into(),
            },
            3,
            EffectKind::Http,
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            &RecordedResult {
                codec_ver: 1,
                bytes: vec![],
            },
        );
    }
}
