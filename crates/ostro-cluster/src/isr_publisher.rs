//! Leader-side ISR maintenance (12-11, D-P12-5, KIP-497): `AlterIsr` proposals from measured
//! follower progress.
//!
//! [`IsrPublisher::run`] runs on the leader of one partition for one epoch. Every
//! `replica_lag_max / 4` (and whenever the HW, the durable end or the assignment moves) it
//! asks the 12-04 rules ([`isr::next_change`]) for the next change over the log's tracker. At
//! most one change is in flight:
//!
//! 1. it is marked pending in the log ([`PartitionLog::propose_isr_change`]): the HW now
//!    counts the **maximal** ISR (a pending add counts, a pending remove still counts);
//! 2. `AlterIsr{p, e, expected_isr_version, isr'}` is proposed (failpoint `isr::before_alter`);
//! 3. the outcome is applied **only from meta**: once the assignment watch shows a new
//!    `isr_version` at this epoch, the log adopts it ([`ReplicatedLog::set_published_isr`])
//!    and the pending change is dropped;
//! 4. a rejection (`StaleVersion`, `Invalid`, …) drops the pending change at once; the next
//!    round re-reads the published state;
//! 5. an **unknown outcome** (the proposal errored: it may or may not have applied, re-check
//!    W4) keeps the pending change counted. The publisher waits for meta to show the result
//!    and re-proposes the *same* command (its CAS on `isr_version` makes that idempotent)
//!    until the published version moves; only then does it propose anything new. A pending
//!    shrink is never dropped on an unknown outcome alone.
//!
//! A replica that a published ISR dropped (the controller's shrink of a node liveness
//! declared dead, or this publisher's own shrink) is re-added only after it fetched again
//! since that ISR was adopted: its tracker row may still look caught up for up to
//! `replica_lag_max`, and re-adding a dead node at once undoes the controller's shrink,
//! stalls the HW on it and ping-pongs `AlterIsr` with the controller (12-16).
//!
//! The loop ends when the log no longer leads the epoch or meta shows a newer epoch (the
//! role lifecycle, 12-13, starts a fresh publisher per term). At RF=1 there is nothing to
//! change and the publisher stays idle.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_log::replica::isr;
use ostro_log::{ReplicaSet, ReplicatedLog};
use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaResult};
use ostro_partition::Clock;
use ostro_partition::roles::{PartitionAssignmentView, PartitionLogHandle};
use ostro_types::{Epoch, PartitionId};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Proposes `AlterIsr` to the metadata group (implemented for [`MetaHandle`]).
pub trait IsrMeta: Send + Sync + 'static {
    /// Propose `cmd` (always a `MetaCommand::AlterIsr`). `Err` means the outcome is unknown.
    fn alter_isr(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, MetaError>>;
}

impl IsrMeta for MetaHandle {
    fn alter_isr(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, MetaError>> {
        Box::pin(self.propose(cmd))
    }
}

impl<T: IsrMeta> IsrMeta for Arc<T> {
    fn alter_isr(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, MetaError>> {
        (**self).alter_isr(cmd)
    }
}

/// ISR publisher tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IsrPublisherConfig {
    /// `replica_lag_max` (D-P12-10 default 10 s); the decision period is a quarter of it.
    pub replica_lag_max: Duration,
    /// How long to wait for an unknown outcome to become visible before re-proposing.
    pub resolve_retry: Duration,
    /// Bound on one `AlterIsr` proposal. A proposal still unanswered after it is an unknown
    /// outcome (step 5): the same CAS is re-proposed, so a request lost on a broken link
    /// (the forwarder's own call deadline is 10 s) never holds an ISR change back for
    /// longer than this plus `resolve_retry`.
    pub propose_timeout: Duration,
}

impl Default for IsrPublisherConfig {
    fn default() -> Self {
        Self {
            replica_lag_max: Duration::from_secs(10),
            resolve_retry: Duration::from_millis(500),
            propose_timeout: Duration::from_secs(3),
        }
    }
}

impl IsrPublisherConfig {
    fn period(&self) -> Duration {
        (self.replica_lag_max / 4).max(Duration::from_millis(10))
    }

    fn lag_max_ms(&self) -> u64 {
        u64::try_from(self.replica_lag_max.as_millis()).unwrap_or(u64::MAX)
    }
}

/// Where an in-flight `AlterIsr` stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FlightState {
    /// Applied by meta (or rejected after an earlier unknown outcome, so it may have
    /// applied): wait for the assignment watch to show the new `isr_version`.
    AwaitVisible,
    /// The last proposal's outcome is unknown: re-propose the same CAS until meta shows a
    /// result.
    Unknown,
}

/// One in-flight `AlterIsr`.
#[derive(Clone, Debug)]
struct InFlight {
    expected_version: u64,
    proposed: Vec<u64>,
    state: FlightState,
}

/// Why [`IsrPublisher::run`] returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IsrPublisherExit {
    /// The shutdown token fired.
    Shutdown,
    /// The log no longer leads the epoch, or meta moved to a newer epoch / another leader.
    LostLeadership,
}

/// The leader-side ISR publisher of one partition.
pub struct IsrPublisher {
    p: PartitionId,
    log: Arc<PartitionLogHandle>,
    meta: Arc<dyn IsrMeta>,
    assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
    clock: Arc<dyn Clock>,
    cfg: IsrPublisherConfig,
    epoch: Epoch,
    in_flight: Option<InFlight>,
    /// Replica → clock time when an adopted published ISR dropped it (module docs).
    removed_at: std::collections::BTreeMap<u64, u64>,
}

impl fmt::Debug for IsrPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IsrPublisher")
            .field("partition", &self.p)
            .field("epoch", &self.epoch)
            .field("in_flight", &self.in_flight)
            .finish_non_exhaustive()
    }
}

impl IsrPublisher {
    /// Run the publisher of `p` while `log` leads it (see the module docs). `assignments` is
    /// the meta-published assignment watch; `clock` is the node clock that also drives the
    /// log's replica tracker.
    pub async fn run(
        p: PartitionId,
        log: Arc<PartitionLogHandle>,
        meta: Arc<dyn IsrMeta>,
        assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
        clock: Arc<dyn Clock>,
        cfg: IsrPublisherConfig,
        shutdown: CancellationToken,
    ) -> IsrPublisherExit {
        let Some(view) = log.isr_view() else {
            return IsrPublisherExit::LostLeadership;
        };
        let mut me = IsrPublisher {
            p,
            log,
            meta,
            assignments,
            clock,
            cfg,
            epoch: view.epoch,
            in_flight: None,
            removed_at: std::collections::BTreeMap::new(),
        };
        info!(partition = p.0, epoch = me.epoch.0, "ISR publisher started");
        let exit = me.main(shutdown).await;
        if me.in_flight.is_some() {
            // Leaving with a change in flight: the next term starts from meta's ISR anyway.
            let _ = me.log.clear_isr_change(me.epoch);
        }
        info!(
            partition = p.0,
            epoch = me.epoch.0,
            ?exit,
            "ISR publisher stopped"
        );
        exit
    }

    fn assignment(&self) -> Option<PartitionAssignmentView> {
        self.assignments
            .borrow()
            .iter()
            .find(|a| a.partition == self.p)
            .cloned()
    }

    async fn main(&mut self, shutdown: CancellationToken) -> IsrPublisherExit {
        let mut ticker = tokio::time::interval(self.cfg.period());
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut hw = self.log.high_watermark();
        let mut durable = self.log.durable_end_watch();
        loop {
            if let Some(exit) = self.step().await {
                return exit;
            }
            let retry = self
                .in_flight
                .as_ref()
                .is_some_and(|f| f.state == FlightState::Unknown)
                .then_some(self.cfg.resolve_retry);
            tokio::select! {
                () = shutdown.cancelled() => return IsrPublisherExit::Shutdown,
                _ = ticker.tick() => {}
                r = self.assignments.changed() => {
                    if r.is_err() {
                        return IsrPublisherExit::Shutdown;
                    }
                }
                _ = hw.changed() => {}
                _ = durable.changed() => {}
                () = async {
                    match retry {
                        Some(d) => tokio::time::sleep(d).await,
                        None => std::future::pending().await,
                    }
                } => {}
            }
        }
    }

    /// One decision round. `Some(exit)` ends the loop.
    async fn step(&mut self) -> Option<IsrPublisherExit> {
        let Some(view) = self.log.isr_view() else {
            return Some(IsrPublisherExit::LostLeadership);
        };
        if view.epoch != self.epoch {
            return Some(IsrPublisherExit::LostLeadership);
        }
        let a = self.assignment()?;
        let me = view.tracker.self_id();
        if a.epoch > self.epoch || (a.epoch == self.epoch && a.leader != me) {
            return Some(IsrPublisherExit::LostLeadership);
        }
        if a.epoch < self.epoch {
            // Meta has not caught up with this term yet (this node is ahead): wait.
            return None;
        }

        // 3. Adopt a newer published ISR (the only way the log's ISR changes).
        if a.isr_version > view.isr.version {
            let now = self.clock.now_ms();
            for r in view.isr.published.iter().filter(|r| !a.isr.contains(r)) {
                self.removed_at.insert(*r, now);
            }
            let set = ReplicaSet {
                replicas: a.replicas.clone(),
                isr: a.isr.clone(),
                isr_version: a.isr_version,
                min_insync: a.min_insync,
            };
            if let Err(e) = self.log.set_published_isr(self.epoch, set) {
                warn!(partition = self.p.0, error = %e, "cannot adopt the published ISR");
                return Some(IsrPublisherExit::LostLeadership);
            }
            if let Some(f) = self.in_flight.take() {
                info!(
                    partition = self.p.0,
                    epoch = self.epoch.0,
                    isr = ?a.isr,
                    isr_version = a.isr_version,
                    applied = (a.isr == f.proposed),
                    "published ISR adopted; in-flight AlterIsr resolved"
                );
                let _ = self.log.clear_isr_change(self.epoch);
            } else {
                info!(
                    partition = self.p.0,
                    epoch = self.epoch.0,
                    isr = ?a.isr,
                    isr_version = a.isr_version,
                    "published ISR adopted"
                );
            }
            return None;
        }

        // Someone else (the role lifecycle) already adopted a newer publication.
        if let Some(f) = self.in_flight.as_ref()
            && view.isr.version != f.expected_version
        {
            self.in_flight = None;
            let _ = self.log.clear_isr_change(self.epoch);
            return None;
        }

        // 5. An unknown outcome: re-propose the same CAS until meta shows the result.
        if let Some(f) = self.in_flight.clone() {
            if f.state == FlightState::Unknown {
                self.propose(f.expected_version, f.proposed).await;
            }
            return None;
        }

        // 1-2. Decide and propose the next change.
        let now = self.clock.now_ms();
        let change = isr::next_change(
            &view.tracker,
            &view.isr,
            &a.replicas,
            view.hw,
            view.epoch_start,
            now,
            self.cfg.lag_max_ms(),
        )?;
        // The re-add gate is part of the in-sync decision, so the `isr_from_assignment`
        // mutant (membership from the assignment alone) bypasses it too (12-16: with the
        // gate honoured, a never-fetching replica stayed out after its first shrink and the
        // conformance scenario caught the mutant only when the first expansion raced the HW).
        // 14-33: the `readd_stale_progress` mutant (re-add from progress measured before
        // the removal) skips this fetched-again gate as well as the tracker reset.
        if let isr::PendingChange::Add(r) = change
            && !crate::fetch::mutant(ostro_log::mutants::ISR_FROM_ASSIGNMENT)
            && !crate::fetch::mutant(ostro_log::mutants::READD_STALE_PROGRESS)
            && let Some(&at) = self.removed_at.get(&r)
        {
            if view.tracker.last_fetch_ms(r).is_none_or(|f| f <= at) {
                return None;
            }
            self.removed_at.remove(&r);
        }
        let proposed = view.isr.proposed(change);
        match self.log.propose_isr_change(self.epoch, change) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(e) => {
                debug!(partition = self.p.0, error = %e, "cannot mark the ISR change pending");
                return Some(IsrPublisherExit::LostLeadership);
            }
        }
        info!(
            partition = self.p.0,
            epoch = self.epoch.0,
            ?change,
            from = ?view.isr.published,
            to = ?proposed,
            isr_version = view.isr.version,
            "proposing AlterIsr"
        );
        self.in_flight = Some(InFlight {
            expected_version: view.isr.version,
            proposed: proposed.clone(),
            state: FlightState::AwaitVisible,
        });
        self.propose(view.isr.version, proposed).await;
        None
    }

    async fn propose(&mut self, expected_version: u64, proposed: Vec<u64>) {
        #[cfg(feature = "failpoints")]
        fail::fail_point!("isr::before_alter");
        let cmd = MetaCommand::AlterIsr {
            partition: self.p.0,
            epoch: self.epoch.0,
            expected_isr_version: expected_version,
            isr: proposed,
        };
        let outcome =
            match tokio::time::timeout(self.cfg.propose_timeout, self.meta.alter_isr(cmd)).await {
                Ok(r) => r,
                Err(_) => Err(MetaError::Timeout),
            };
        match outcome {
            Ok(MetaResult::Ok { .. }) => {
                // Applied: the assignment watch delivers the new ISR; it stays pending (and
                // counted in the maximal ISR) until then.
                if let Some(f) = self.in_flight.as_mut() {
                    f.state = FlightState::AwaitVisible;
                }
            }
            Ok(MetaResult::StaleVersion { current })
                if self
                    .in_flight
                    .as_ref()
                    .is_some_and(|f| f.state == FlightState::Unknown) =>
            {
                // A re-proposal after an unknown outcome lost its CAS: the first proposal may
                // have applied. Keep the change pending until meta's view shows the version.
                debug!(
                    partition = self.p.0,
                    current, "re-proposed AlterIsr is stale; waiting for meta"
                );
                if let Some(f) = self.in_flight.as_mut() {
                    f.state = FlightState::AwaitVisible;
                }
            }
            Ok(other) => {
                info!(partition = self.p.0, epoch = self.epoch.0, result = ?other, "AlterIsr rejected; dropping the pending change");
                self.in_flight = None;
                let _ = self.log.clear_isr_change(self.epoch);
            }
            Err(e) => {
                warn!(partition = self.p.0, epoch = self.epoch.0, error = %e, "AlterIsr outcome unknown; keeping the pending change");
                if let Some(f) = self.in_flight.as_mut() {
                    f.state = FlightState::Unknown;
                }
            }
        }
    }
}
