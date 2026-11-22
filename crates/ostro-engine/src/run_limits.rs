//! Per-run limits (Phase 11, OPS-07, D-P11-13): journal length, children per run and payload
//! size. Contract: 11-02 (struct and defaults only); enforced from 11-06.
//!
//! Distinct from [`crate::limits`] (ENG-05 store limits), which is untouched.

use serde::{Deserialize, Serialize};

/// Per-run limits (`[limits]` config section). No enforcement yet (11-06).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RunLimits {
    /// Maximum journal events per run (default 50,000).
    pub max_history_events: u64,
    /// Maximum child runs started by one run (default 10,000).
    pub max_children_per_run: u64,
    /// Maximum size of one payload (input, output, signal, step result) in bytes
    /// (default 4 MiB).
    pub max_payload_bytes: u64,
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            max_history_events: 50_000,
            max_children_per_run: 10_000,
            max_payload_bytes: 4 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_d_p11_13() {
        let l = RunLimits::default();
        assert_eq!(l.max_history_events, 50_000);
        assert_eq!(l.max_children_per_run, 10_000);
        assert_eq!(l.max_payload_bytes, 4 * 1024 * 1024);
        assert_eq!(crate::config::EngineConfig::default().run_limits, l);
    }
}
