//! The scenario registry (12-02). Scenarios are library functions run by one runner
//! ([`runner`], 12-03) with a hard deadline and a measured `cluster_formed` precondition;
//! a scenario fails only with a [`ScenarioFailure`] (timeouts and panics are
//! `HarnessError`s, D-P12-19). Each group module exports `pub fn scenarios() -> Vec<Scenario>`.
//!
//! A [`Scenario`] is generic over the harness it runs on (12-17): tier-B groups are
//! `Scenario<InProcCluster>` (the default) and are listed by [`all`]; the tier-C `process`
//! group is `Scenario<ProcCluster>`, listed by [`all_process`] and run by
//! [`runner::run_on_cluster`] on a cluster the caller started (per-process failpoints).

pub mod blobs;
pub mod bootstrap;
pub mod divergence;
pub mod failover;
pub mod fetch_path;
pub mod hw;
pub mod isr_honesty;
pub mod min_insync;
pub mod object_storage;
pub mod one_path;
pub mod placement;
pub mod process;
pub mod runner;
pub mod stale_epoch;
pub mod standby;
pub mod zombie;

use std::time::Duration;

use futures_util::future::BoxFuture;

use crate::harness::ClusterSpec;
use crate::harness::inproc::InProcCluster;
use crate::harness::process::ProcCluster;

/// Default per-scenario deadline.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(120);

/// Which harness tier a scenario needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// In-process nodes over the in-memory transport (12-03).
    B,
    /// Multi-process nodes behind the TCP chaos proxy (12-09).
    C,
}

/// A measured oracle failure: the only valid red result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScenarioFailure {
    /// The oracle that failed (`"cluster_formed"`, `"disk"`, ...).
    pub oracle: &'static str,
    /// What it found.
    pub detail: String,
}

impl std::fmt::Display for ScenarioFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.oracle, self.detail)
    }
}

/// What a scenario body returns.
pub type ScenarioResult = Result<(), ScenarioFailure>;

/// One scenario on harness `C` (tier B: [`InProcCluster`], tier C: [`ProcCluster`]).
pub struct Scenario<C = InProcCluster> {
    /// Unique name (pending lists and the mutant matrix refer to it).
    pub name: &'static str,
    /// Its group module's name.
    pub group: &'static str,
    /// Harness tier.
    pub tier: Tier,
    /// Hard deadline (default [`DEFAULT_DEADLINE`]).
    pub deadline: Duration,
    /// The cluster it needs.
    pub spec: fn() -> ClusterSpec,
    /// The body.
    pub run: for<'a> fn(&'a mut C) -> BoxFuture<'a, ScenarioResult>,
}

impl<C> Clone for Scenario<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C> Copy for Scenario<C> {}

impl<C> std::fmt::Debug for Scenario<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scenario")
            .field("name", &self.name)
            .field("group", &self.group)
            .field("tier", &self.tier)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Every tier-B scenario of every group, in group order.
pub fn all() -> Vec<Scenario> {
    [
        placement::scenarios(),
        isr_honesty::scenarios(),
        hw::scenarios(),
        stale_epoch::scenarios(),
        divergence::scenarios(),
        failover::scenarios(),
        min_insync::scenarios(),
        standby::scenarios(),
        bootstrap::scenarios(),
        fetch_path::scenarios(),
        blobs::scenarios(),
        one_path::scenarios(),
        zombie::scenarios(),
        object_storage::scenarios(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Every tier-C scenario (the `process` group, 12-17).
pub fn all_process() -> Vec<Scenario<ProcCluster>> {
    process::scenarios()
}
