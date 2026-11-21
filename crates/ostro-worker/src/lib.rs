//! ostro-worker: the remote worker runtime behind `ostro worker` (Phase 7, WORK-01/02).
//!
//! A worker joins a cluster (kubeadm-style token + CSR, D-W4), opens one mTLS
//! `Execute.Session` per server node (D-W2) and runs activations on its local engine. The
//! leader stays authoritative: fencing, derive, effects and secrets never leave it (D-W3).
//!
//! Skeleton from plan 07-02 (modules and the [`config::WorkerConfig`] contract); 07-07
//! implements the modules and [`run`].

pub mod blob_fetch;
pub mod config;
pub mod drain;
pub mod effect_proxy;
pub mod join;
pub mod journal_fetch;
pub mod remote_sink;
pub mod runner;
pub mod session;

pub use config::{JoinToken, WorkerConfig};
use tokio_util::sync::CancellationToken;

/// Worker failures.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The worker runtime is not implemented yet (plan 07-07).
    #[error("ostro worker is not implemented yet (Phase 7 plan 07-07)")]
    NotImplemented,
    /// Invalid configuration (the message names the key; never a secret value).
    #[error("invalid worker configuration: {0}")]
    Config(String),
}

/// Run the worker until `shutdown` fires.
///
/// Phase 7 stub: returns [`WorkerError::NotImplemented`] until 07-07.
pub async fn run(cfg: WorkerConfig, shutdown: CancellationToken) -> Result<(), WorkerError> {
    let _ = (cfg, shutdown);
    Err(WorkerError::NotImplemented)
}

// Dependencies declared once by plan 07-02 (D-W17) for 07-07; anchored so cargo's
// unused-dependency lint stays quiet until the modules consume them.
mod dep_anchors {
    #[cfg(feature = "failpoints")]
    use fail as _;
    use ostro_worker_proto as _;
    use rcgen as _;
    use reqwest as _;
}
