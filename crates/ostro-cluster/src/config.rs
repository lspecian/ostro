//! Cluster runtime configuration (12-14): the `[replication]` / `[cluster]` values the
//! server hands to the cluster components, and the derived per-component configs.

use std::time::Duration;

use ostro_meta::handle::TickConfig;

use crate::controller::ControllerConfig;
use crate::fetch::FetchConfig;
use crate::fetcher::FetcherConfig;
use crate::isr_publisher::IsrPublisherConfig;
use crate::liveness::LivenessConfig;

/// `[replication]` as the cluster layer needs it (the server converts its config section).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationTuning {
    /// Leader self-demotion after this long without meta contact; the liveness window.
    pub session_timeout: Duration,
    /// A follower not caught up within this long leaves the ISR.
    pub replica_lag_max: Duration,
    /// Metadata Raft tick.
    pub meta_tick_interval: Duration,
    /// Metadata election timeout (ticks).
    pub meta_election_tick: usize,
    /// Metadata heartbeat (ticks).
    pub meta_heartbeat_tick: usize,
    /// Fetch long-poll idle wait.
    pub fetch_max_wait: Duration,
    /// Bound of one fetch read.
    pub fetch_read_timeout: Duration,
    /// Fetch response size cap.
    pub fetch_max_bytes: usize,
    /// Largest envelope of the cluster.
    pub max_envelope_bytes: usize,
    /// Bootstrap replication factor seed (`None` = `min(3, voters)`).
    pub rf: Option<u8>,
    /// Bootstrap minimum ISR seed (`None` = 2 when rf >= 3, else 1).
    pub min_insync: Option<u8>,
}

impl Default for ReplicationTuning {
    fn default() -> Self {
        Self {
            session_timeout: Duration::from_secs(3),
            replica_lag_max: Duration::from_secs(10),
            meta_tick_interval: Duration::from_millis(100),
            meta_election_tick: 10,
            meta_heartbeat_tick: 2,
            fetch_max_wait: crate::fetch::DEFAULT_FETCH_MAX_WAIT,
            fetch_read_timeout: crate::fetch::DEFAULT_FETCH_READ_TIMEOUT,
            fetch_max_bytes: crate::fetch::DEFAULT_FETCH_MAX_BYTES,
            max_envelope_bytes: ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES,
            rf: None,
            min_insync: None,
        }
    }
}

impl ReplicationTuning {
    /// The bootstrap replication factor for `voters` voters: the seed clamped to
    /// `1..=voters`, default `min(3, voters)` (D-P12-14).
    pub fn effective_rf(&self, voters: usize) -> usize {
        let voters = voters.max(1);
        self.rf
            .map_or(voters.min(3), |r| usize::from(r).clamp(1, voters))
    }

    /// The bootstrap `min_insync` for `rf`: the seed clamped to `1..=rf`, default 2 when
    /// `rf >= 3`, else 1.
    pub fn effective_min_insync(&self, rf: usize) -> u8 {
        let rf = u8::try_from(rf.max(1)).unwrap_or(u8::MAX);
        self.min_insync
            .map_or(if rf >= 3 { 2 } else { 1 }, |m| m.clamp(1, rf))
    }

    /// Metadata Raft ticks.
    pub fn tick(&self) -> TickConfig {
        TickConfig {
            interval: self.meta_tick_interval,
            election_tick: self.meta_election_tick,
            heartbeat_tick: self.meta_heartbeat_tick,
        }
    }

    /// The controller (bootstrap seeds for `voters` voters).
    pub fn controller(&self, voters: usize) -> ControllerConfig {
        let rf = self.effective_rf(voters);
        ControllerConfig::new(rf, self.effective_min_insync(rf))
    }

    /// Liveness windows.
    pub fn liveness(&self) -> LivenessConfig {
        LivenessConfig::new(self.session_timeout)
    }

    /// Leader-side fetch handling.
    pub fn fetch(&self) -> FetchConfig {
        FetchConfig {
            max_wait: self.fetch_max_wait,
            read_timeout: self.fetch_read_timeout,
            max_bytes: self.fetch_max_bytes,
        }
    }

    /// The follower fetch loop.
    pub fn fetcher(&self) -> FetcherConfig {
        FetcherConfig {
            max_wait: self.fetch_max_wait,
            max_bytes: self.fetch_max_bytes,
            read_timeout: self.fetch_read_timeout,
            max_envelope_bytes: self.max_envelope_bytes,
            ..FetcherConfig::default()
        }
    }

    /// The ISR publisher.
    pub fn isr_publisher(&self) -> IsrPublisherConfig {
        IsrPublisherConfig {
            replica_lag_max: self.replica_lag_max,
            ..IsrPublisherConfig::default()
        }
    }

    /// The `Replication` server's encode/decode limit.
    pub fn replication_message_limit(&self) -> usize {
        crate::service::replication_message_limit(self.max_envelope_bytes, self.fetch_max_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_seeds() {
        let t = ReplicationTuning::default();
        assert_eq!(t.effective_rf(1), 1);
        assert_eq!(t.effective_rf(3), 3);
        assert_eq!(t.effective_rf(5), 3);
        assert_eq!(t.effective_min_insync(1), 1);
        assert_eq!(t.effective_min_insync(3), 2);
        let t = ReplicationTuning {
            rf: Some(5),
            min_insync: Some(9),
            ..t
        };
        assert_eq!(t.effective_rf(3), 3);
        assert_eq!(t.effective_min_insync(3), 3);
        assert_eq!(t.controller(1).rf, 1);
        assert_eq!(t.controller(1).min_insync, 1);
    }
}
