//! Phase 7: implemented in 07-09.
//!
//! Worker join (D-W4): `POST /v1/workers/join {token, csr, labels, worker_id}` on the client
//! listener verifies a `wkr_` token with the `worker:join` scope (and `expires_at_ms`, F8),
//! signs the CSR with the cluster CA (SAN URI `ostro://<cluster>/worker/<token_id>/<worker_id>`,
//! clientAuth, `[workers] client_cert_ttl`) and returns the CA certificate, the cluster
//! endpoints and `renew_after`. Secrets never appear in `Debug` or errors.

use std::collections::BTreeMap;
use std::sync::Arc;

use ostro_api::ApiError;
use ostro_meta::MetaView;

use crate::config::WorkersSection;
use crate::tls::TlsState;

/// A join request (the token is redacted in `Debug`).
#[derive(Clone)]
pub struct JoinRequest {
    /// `wkr_<id>.<secret>`.
    pub token: String,
    /// PEM PKCS#10 CSR of an in-memory key.
    pub csr: String,
    pub labels: BTreeMap<String, String>,
    pub worker_id: String,
}

impl std::fmt::Debug for JoinRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinRequest")
            .field("token", &"<redacted>")
            .field("labels", &self.labels)
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

/// A successful join.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinResponse {
    /// PEM client certificate.
    pub client_cert: String,
    /// PEM cluster CA certificate.
    pub ca_cert: String,
    /// Cluster listener endpoints (`host:port`).
    pub endpoints: Vec<String>,
    /// Renew after this many ms (2/3 of the lifetime).
    pub renew_after_ms: u64,
}

/// Token verification and CSR signing.
pub struct JoinService {
    meta: MetaView,
    tls: Arc<TlsState>,
    client_cert_ttl_ms: u64,
}

impl JoinService {
    pub fn new(meta: MetaView, tls: Arc<TlsState>, cfg: &WorkersSection) -> Self {
        Self {
            meta,
            tls,
            client_cert_ttl_ms: u64::try_from(cfg.client_cert_ttl.get().as_millis())
                .unwrap_or(u64::MAX),
        }
    }

    /// The meta view tokens are verified against.
    pub fn meta(&self) -> &MetaView {
        &self.meta
    }

    /// The node TLS state (cluster CA).
    pub fn tls(&self) -> &Arc<TlsState> {
        &self.tls
    }

    /// `[workers] client_cert_ttl` in ms.
    pub fn client_cert_ttl_ms(&self) -> u64 {
        self.client_cert_ttl_ms
    }

    /// Join a worker.
    ///
    /// Phase 7 stub (07-02): `Unavailable`.
    pub async fn join(&self, req: JoinRequest) -> Result<JoinResponse, ApiError> {
        let _ = req;
        Err(ApiError::unavailable(
            "worker join is not implemented yet (07-09)",
        ))
    }

    /// Renew a worker certificate for the authenticated `token_id` / `worker_id`.
    ///
    /// Phase 7 stub (07-02): `Unavailable`.
    pub async fn renew(
        &self,
        token_id: &str,
        worker_id: &str,
        csr: &str,
    ) -> Result<JoinResponse, ApiError> {
        let _ = (token_id, worker_id, csr);
        Err(ApiError::unavailable(
            "worker certificate renewal is not implemented yet (07-09)",
        ))
    }
}

impl std::fmt::Debug for JoinService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinService").finish_non_exhaustive()
    }
}
