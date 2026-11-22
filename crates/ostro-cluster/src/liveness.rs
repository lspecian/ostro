//! Node liveness for the partition controller (12-10, D-P12-9): an accumulator over the
//! metadata leader's `RaftHandle::sample_live` (voters ∪ learners since 12-10) plus suspect
//! reports from followers whose fetch failed.
//!
//! Rules (all time through the injected [`Clock`]):
//!
//! - A node is **live** while it was present in some sample within `session_timeout`, and
//!   **dead** once it was absent from every sample for `session_timeout`. One missed sample
//!   never kills a node.
//! - A **suspect report** marks the node suspect and asks the controller loop for a
//!   confirming sample. The confirming sample is taken once `confirm_window` has passed
//!   since the report, with `within = confirm_window`, so it answers "was the node heard
//!   from after the report?": absent → dead at once (the fast path for a `kill -9` on
//!   loopback), present → the suspicion is cleared. A report alone never kills a node.
//! - A newly elected controller ([`Liveness::on_became_controller`]) forgets everything and
//!   gives every node a fresh grace period: a node absent from all samples of its first
//!   `seed_window` becomes suspect and goes through the same confirmation. The first sample
//!   of a new controller therefore never declares a node dead (its `last_heard` table still
//!   holds its follower-era contacts and has never heard from the other followers).
//!
//! Regular samples use `sample_window` (default `session_timeout / 3`), not the whole
//! `session_timeout`: a node present in a sample was heard within that window, so a dead
//! node stops appearing after at most `sample_window` and is declared dead `session_timeout`
//! later (detection ≤ `session_timeout + sample_window + tick` instead of twice the timeout).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ostro_meta::RaftHandle;
use ostro_partition::Clock;
use tokio::sync::Notify;

/// Receives `MetaPeer.ReportSuspect` reports (and local fetch failures). The controller
/// confirms a suspect with its next liveness sample and never acts on a report alone.
pub trait SuspectSink: Send + Sync {
    /// `reporter` saw `suspect` fail (connection refused/reset), for `partition` if known.
    fn report(&self, reporter: u64, suspect: u64, partition: Option<u16>);
}

/// Liveness timing, all derived from `session_timeout` by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LivenessConfig {
    /// A node unseen for this long is dead (`[replication] session_timeout`, 3 s).
    pub session_timeout: Duration,
    /// The controller's liveness tick (`session_timeout / 6`).
    pub tick: Duration,
    /// `within` of a regular sample (`session_timeout / 3`).
    pub sample_window: Duration,
    /// Delay before, and `within` of, a confirming sample (`session_timeout / 4`; several
    /// metadata heartbeats at the 200 ms default).
    pub confirm_window: Duration,
    /// A new controller suspects nodes absent from this much of its first samples
    /// (`session_timeout / 3`).
    pub seed_window: Duration,
}

impl LivenessConfig {
    /// The defaults for `session_timeout`.
    pub fn new(session_timeout: Duration) -> LivenessConfig {
        let t = session_timeout.max(Duration::from_millis(6));
        LivenessConfig {
            session_timeout: t,
            tick: t / 6,
            sample_window: t / 3,
            confirm_window: t / 4,
            seed_window: t / 3,
        }
    }
}

/// The liveness of one node as the controller sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeState {
    /// Seen within `session_timeout`, or still inside its grace period.
    Live,
    /// Reported (or seeded) and awaiting a confirming sample. Still counts as live.
    Suspect,
    /// Unseen for `session_timeout`, or absent from a confirming sample.
    Dead,
}

#[derive(Clone, Copy, Debug)]
struct Node {
    /// When tracking started (or the controller took over): the grace period's start.
    since_ms: u64,
    /// Last sample this node was present in.
    last_seen_ms: Option<u64>,
    /// Pending suspicion: when it was reported (or seeded).
    suspect_at_ms: Option<u64>,
    /// Absent from a confirming sample; cleared when it is seen again.
    confirmed_dead: bool,
}

/// The liveness accumulator. Not thread-safe on its own; [`LivenessHandle`] shares it.
pub struct Liveness {
    clock: Arc<dyn Clock>,
    cfg: LivenessConfig,
    nodes: BTreeMap<u64, Node>,
}

impl std::fmt::Debug for Liveness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Liveness")
            .field("now_ms", &self.clock.now_ms())
            .field("cfg", &self.cfg)
            .field("nodes", &self.nodes)
            .finish()
    }
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl Liveness {
    /// A detector with the default timing for `session_timeout`.
    pub fn new(clock: Arc<dyn Clock>, session_timeout: Duration) -> Liveness {
        Liveness::with_config(clock, LivenessConfig::new(session_timeout))
    }

    /// A detector with explicit timing.
    pub fn with_config(clock: Arc<dyn Clock>, cfg: LivenessConfig) -> Liveness {
        Liveness {
            clock,
            cfg,
            nodes: BTreeMap::new(),
        }
    }

    /// The timing in use.
    pub fn config(&self) -> LivenessConfig {
        self.cfg
    }

    fn fresh(now: u64) -> Node {
        Node {
            since_ms: now,
            last_seen_ms: None,
            suspect_at_ms: None,
            confirmed_dead: false,
        }
    }

    /// Start tracking `nodes` (no-op for known ones). A new node gets a full grace period.
    pub fn track(&mut self, nodes: impl IntoIterator<Item = u64>) {
        let now = self.clock.now_ms();
        for n in nodes {
            if n != 0 {
                self.nodes.entry(n).or_insert_with(|| Self::fresh(now));
            }
        }
    }

    /// This node just became the controller: forget every observation and restart every
    /// tracked node's grace period (the seeding window starts now).
    pub fn on_became_controller(&mut self) {
        let now = self.clock.now_ms();
        for n in self.nodes.values_mut() {
            *n = Self::fresh(now);
        }
    }

    /// A regular sample (taken with `within = sample_window`).
    pub fn observe(&mut self, sample: &[u64]) {
        let now = self.clock.now_ms();
        self.track(sample.iter().copied());
        for id in sample {
            if let Some(n) = self.nodes.get_mut(id) {
                n.last_seen_ms = Some(now);
                n.suspect_at_ms = None;
                n.confirmed_dead = false;
            }
        }
        // Seeding: absent from every sample since the grace period started.
        let seed = ms(self.cfg.seed_window);
        for n in self.nodes.values_mut() {
            if n.last_seen_ms.is_none()
                && !n.confirmed_dead
                && n.suspect_at_ms.is_none()
                && now.saturating_sub(n.since_ms) >= seed
            {
                n.suspect_at_ms = Some(now);
            }
        }
    }

    /// `reporter` saw `suspect` fail. Marks it suspect (keeping an earlier pending report).
    pub fn report_suspect(&mut self, reporter: u64, suspect: u64) {
        let now = self.clock.now_ms();
        self.track([suspect]);
        if let Some(n) = self.nodes.get_mut(&suspect)
            && !n.confirmed_dead
            && n.suspect_at_ms.is_none()
        {
            tracing::debug!(reporter, suspect, "liveness: suspect reported");
            n.suspect_at_ms = Some(now);
        }
    }

    /// `Some(confirm_window)` when a confirming sample is due now (some suspicion is at
    /// least `confirm_window` old).
    pub fn confirm_due(&self) -> Option<Duration> {
        self.next_confirm_ms()
            .filter(|&at| at <= self.clock.now_ms())
            .map(|_| self.cfg.confirm_window)
    }

    /// When (clock ms) the earliest pending confirmation becomes due.
    pub fn next_confirm_ms(&self) -> Option<u64> {
        let w = ms(self.cfg.confirm_window);
        self.nodes
            .values()
            .filter_map(|n| n.suspect_at_ms)
            .map(|at| at.saturating_add(w))
            .min()
    }

    /// A confirming sample (taken with `within = confirm_window`): every suspicion at least
    /// `confirm_window` old is settled, dead when absent, cleared when present.
    pub fn confirm(&mut self, sample: &[u64]) {
        let now = self.clock.now_ms();
        let w = ms(self.cfg.confirm_window);
        for (id, n) in self.nodes.iter_mut() {
            let Some(at) = n.suspect_at_ms else { continue };
            if at.saturating_add(w) > now {
                continue;
            }
            n.suspect_at_ms = None;
            if sample.contains(id) {
                n.last_seen_ms = Some(now);
            } else {
                tracing::info!(node = id, "liveness: suspect confirmed dead");
                n.confirmed_dead = true;
            }
        }
    }

    /// The state of `node` now (untracked nodes are `Live`: never declared dead unseen).
    pub fn state(&self, node: u64) -> NodeState {
        let Some(n) = self.nodes.get(&node) else {
            return NodeState::Live;
        };
        let now = self.clock.now_ms();
        let seen = n.last_seen_ms.unwrap_or(n.since_ms);
        if n.confirmed_dead || now.saturating_sub(seen) > ms(self.cfg.session_timeout) {
            NodeState::Dead
        } else if n.suspect_at_ms.is_some() {
            NodeState::Suspect
        } else {
            NodeState::Live
        }
    }

    /// The non-dead members of `universe`.
    pub fn live_set(&self, universe: &BTreeSet<u64>) -> BTreeSet<u64> {
        universe
            .iter()
            .copied()
            .filter(|&n| self.state(n) != NodeState::Dead)
            .collect()
    }
}

/// A shared [`Liveness`] plus the wake-up the controller loop waits on (a suspect report
/// asks for an early sample). Cheap to clone.
#[derive(Clone, Debug)]
pub struct LivenessHandle {
    inner: Arc<Mutex<Liveness>>,
    wake: Arc<Notify>,
}

/// The future returned by [`LiveSource`] methods.
pub type LiveFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the controller needs from liveness (the real [`LivenessHandle`], or a scripted
/// fake in tests).
pub trait LiveSource: Send + Sync + 'static {
    /// This node just became the metadata leader.
    fn on_became_controller(&self);
    /// One controller tick: sample through `raft` when due, then the non-dead members of
    /// `universe`.
    fn poll<'a>(
        &'a self,
        raft: &'a RaftHandle,
        universe: &'a BTreeSet<u64>,
    ) -> LiveFuture<'a, BTreeSet<u64>>;
    /// Resolves when liveness wants a sample before the next tick (a suspect report).
    fn wake(&self) -> LiveFuture<'_, ()>;
}

impl LivenessHandle {
    /// Share `liveness`.
    pub fn new(liveness: Liveness) -> LivenessHandle {
        LivenessHandle {
            inner: Arc::new(Mutex::new(liveness)),
            wake: Arc::new(Notify::new()),
        }
    }

    /// The accumulator (never held across an await).
    pub fn lock(&self) -> MutexGuard<'_, Liveness> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record a suspect report and wake the controller loop.
    pub fn report_suspect(&self, reporter: u64, suspect: u64) {
        self.lock().report_suspect(reporter, suspect);
        self.wake.notify_one();
    }
}

impl SuspectSink for LivenessHandle {
    fn report(&self, reporter: u64, suspect: u64, partition: Option<u16>) {
        tracing::debug!(reporter, suspect, ?partition, "suspect report");
        self.report_suspect(reporter, suspect);
    }
}

impl LiveSource for LivenessHandle {
    fn on_became_controller(&self) {
        self.lock().on_became_controller();
    }

    fn poll<'a>(
        &'a self,
        raft: &'a RaftHandle,
        universe: &'a BTreeSet<u64>,
    ) -> LiveFuture<'a, BTreeSet<u64>> {
        Box::pin(async move {
            let (sample_window, confirm) = {
                let mut l = self.lock();
                l.track(universe.iter().copied());
                (l.config().sample_window, l.confirm_due())
            };
            if let Some(within) = confirm
                && let Some(sample) = raft.sample_live(within).await
            {
                self.lock().confirm(&sample);
            }
            if let Some(sample) = raft.sample_live(sample_window).await {
                self.lock().observe(&sample);
            }
            self.lock().live_set(universe)
        })
    }

    fn wake(&self) -> LiveFuture<'_, ()> {
        Box::pin(async move {
            let due_in = {
                let l = self.lock();
                let now = l.clock.now_ms();
                l.next_confirm_ms().map(|at| at.saturating_sub(now))
            };
            match due_in {
                // A pending confirmation: wake when it is due (or on a newer report).
                Some(ms) => {
                    // Floor: a due confirmation the caller cannot take (not leader) must
                    // not spin.
                    let ms = ms.max(5);
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_millis(ms)) => {}
                        () = self.wake.notified() => {}
                    }
                }
                None => self.wake.notified().await,
            }
        })
    }
}
