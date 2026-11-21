//! Worker join tokens, join and certificate renewal (plan 07-09, D-W4, SEC-01).
//!
//! - **Tokens.** `create_token` stores the existing meta `ApiToken` record
//!   `{id: "wkr_<base32 10>", hash: sha256(secret), scopes: ["worker:join"],
//!   created_at_ms, expires_at_ms: Some(now + ttl)}` with a create-only CAS (Phase 5 F8 field;
//!   no field is added). The secret is shown once (`wkr_<id>.<secret>`), stored only as its
//!   sha256 and never logged. `list_tokens` returns ids and times only; `revoke_token`
//!   deletes the record by CAS. Live sessions whose certificate carries a revoked token id
//!   are closed by the cluster service on the meta change ([`super::service`]).
//! - **Join.** `join` checks the token (constant-time hash compare, `worker:join` scope,
//!   expiry against the injected clock, D-W26), signs the CSR with the cluster CA
//!   (`ostro://<cluster>/worker/<token_id>/<worker_id>`, clientAuth,
//!   `[workers] client_cert_ttl`) and returns the CA, the cluster listener endpoints and
//!   `renew_after = 2/3` of the lifetime. Unknown, expired and revoked tokens are one
//!   indistinguishable `InvalidArgument` (`details.reason = join_token_invalid`), as in the
//!   07-11 contract; a bad CSR is `InvalidArgument` naming the CSR.
//! - **Renew.** `renew` runs the same token checks against the token id of the caller's
//!   certificate: an expired or revoked token refuses renewal (the live session stays
//!   until its certificate expires or the token is revoked).
//!
//! The service is created by `p7::init` with `JoinService::new(meta, tls, cfg)` (fixed
//! signature) and late-bound by `workers::service::start` with the meta handle, the clock
//! ([`JoinService::attach`]) and the cluster listener endpoints
//! ([`JoinService::set_endpoints`]).

use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::Duration;

use ostro_api::ApiError;
pub use ostro_api::phase7::{JoinRequest, JoinResponse, WorkerTokenCreated, WorkerTokenInfo};
use ostro_api::phase7::{REASON_JOIN_TOKEN_INVALID, WORKER_TOKEN_PREFIX};
use ostro_meta::{ApiToken, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, MetaView};
use ostro_partition::Clock;
use sha2::{Digest, Sha256};

use crate::config::WorkersSection;
use crate::tls::TlsState;
use crate::tls::ca::{MAX_ID_LEN, pem_encode, valid_id};

/// The scope of worker join tokens (RESERVATIONS §11).
pub const WORKER_JOIN_SCOPE: &str = "worker:join";

/// Random characters of a token id after `wkr_`.
const TOKEN_ID_CHARS: usize = 10;
/// Random bytes of a token secret (base32: 32 characters).
const SECRET_BYTES: usize = 20;
/// Crockford base32 alphabet (lowercase), as run ids and await tokens.
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// Late-bound dependencies (set by `workers::service::start`).
struct Bound {
    meta: Weak<MetaHandle>,
    clock: Arc<dyn Clock>,
}

/// Token verification, CSR signing and the token lifecycle.
pub struct JoinService {
    meta: MetaView,
    tls: Arc<TlsState>,
    client_cert_ttl_ms: u64,
    join_token_ttl: Duration,
    bound: OnceLock<Bound>,
    endpoints: RwLock<Vec<String>>,
}

/// A verified join token (the caller's `ApiToken` record).
#[derive(Debug)]
struct Verified {
    id: String,
}

impl JoinService {
    pub fn new(meta: MetaView, tls: Arc<TlsState>, cfg: &WorkersSection) -> Self {
        Self {
            meta,
            tls,
            client_cert_ttl_ms: u64::try_from(cfg.client_cert_ttl.get().as_millis())
                .unwrap_or(u64::MAX),
            join_token_ttl: cfg.join_token_ttl.get(),
            bound: OnceLock::new(),
            endpoints: RwLock::new(Vec::new()),
        }
    }

    /// Bind the meta handle (proposals) and the injected clock (D-W26). The first call
    /// wins; later calls are ignored.
    pub fn attach(&self, meta: Weak<MetaHandle>, clock: Arc<dyn Clock>) {
        let _ = self.bound.set(Bound { meta, clock });
    }

    /// Set the cluster listener endpoints handed out by `join` (`host:port`).
    pub fn set_endpoints(&self, endpoints: Vec<String>) {
        *self
            .endpoints
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = endpoints;
    }

    /// The cluster listener endpoints (empty until the listener is bound).
    pub fn endpoints(&self) -> Vec<String> {
        self.endpoints
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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

    /// `renew_after_ms` of issued certificates (2/3 of the lifetime).
    pub fn renew_after_ms(&self) -> u64 {
        self.client_cert_ttl_ms / 3 * 2
    }

    fn bound(&self) -> Result<&Bound, ApiError> {
        self.bound
            .get()
            .ok_or_else(|| ApiError::unavailable("the worker join service is not started yet"))
    }

    fn now_ms(&self) -> Result<u64, ApiError> {
        Ok(self.bound()?.clock.now_ms())
    }

    fn meta_handle(&self) -> Result<Arc<MetaHandle>, ApiError> {
        self.bound()?
            .meta
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("metadata is shutting down"))
    }

    // ------------------------------------------------------------------------- tokens

    /// Create a `worker:join` token valid for `ttl` (`None` = `[workers] join_token_ttl`).
    /// `join_url` is the client listener URL put into the returned join command.
    pub async fn create_token(
        &self,
        ttl: Option<Duration>,
        join_url: &str,
    ) -> Result<WorkerTokenCreated, ApiError> {
        let ttl = ttl.unwrap_or(self.join_token_ttl);
        if ttl.is_zero() {
            return Err(ApiError::invalid_argument("token ttl must be > 0"));
        }
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        let meta = self.meta_handle()?;
        let ca_hash = self
            .tls
            .ca_hash()
            .ok_or_else(|| ApiError::unavailable("no cluster CA"))?;
        // A fresh id collides with probability ~2^-50; retry a few times anyway.
        for _ in 0..4 {
            let id = format!("{WORKER_TOKEN_PREFIX}{}", random_base32(TOKEN_ID_CHARS)?);
            let secret = random_base32(base32_len(SECRET_BYTES))?;
            let now = self.now_ms()?;
            let expires_at_ms = now.saturating_add(ttl_ms);
            let record = ApiToken {
                id: id.clone(),
                hash: sha256(secret.as_bytes()),
                scopes: vec![WORKER_JOIN_SCOPE.to_owned()],
                tenant: None,
                created_at_ms: now,
                expires_at_ms: Some(expires_at_ms),
                ext: Vec::new(),
            };
            let res = meta
                .propose(MetaCommand::Put {
                    key: MetaKey::Token(id.clone()),
                    expected_version: None,
                    record: MetaRecord::Token(record),
                })
                .await
                .map_err(|e| ApiError::unavailable(format!("storing the join token: {e}")))?;
            match res {
                MetaResult::Ok { .. } => {
                    let token = format!("{id}.{secret}");
                    let join_command = format!(
                        "ostro worker --join {join_url} --token {token} --ca-hash {ca_hash}"
                    );
                    return Ok(WorkerTokenCreated {
                        token_id: id,
                        token,
                        join_command,
                        expires_at_ms,
                    });
                }
                MetaResult::Conflict { .. } => continue,
                other => {
                    return Err(ApiError::internal(format!(
                        "storing the join token: {other:?}"
                    )));
                }
            }
        }
        Err(ApiError::internal("could not allocate a unique token id"))
    }

    /// Every worker join token (ids and times; never a secret or hash), sorted by id.
    pub fn list_tokens(&self) -> Vec<WorkerTokenInfo> {
        self.meta.read(|s| {
            s.tokens()
                .filter(|t| is_join_token(&t.value))
                .map(|t| WorkerTokenInfo {
                    token_id: t.value.id.clone(),
                    created_at_ms: t.value.created_at_ms,
                    expires_at_ms: t.value.expires_at_ms,
                })
                .collect()
        })
    }

    /// Revoke (delete) a join token. Unknown = NotFound.
    pub async fn revoke_token(&self, token_id: &str) -> Result<(), ApiError> {
        let meta = self.meta_handle()?;
        for _ in 0..4 {
            let version = self.meta.read(|s| {
                s.token(token_id)
                    .filter(|t| is_join_token(&t.value))
                    .map(|t| t.version)
            });
            let Some(version) = version else {
                return Err(ApiError::not_found(format!(
                    "worker token {token_id} not found"
                )));
            };
            let res = meta
                .propose(MetaCommand::Delete {
                    key: MetaKey::Token(token_id.to_owned()),
                    expected_version: version,
                })
                .await
                .map_err(|e| ApiError::unavailable(format!("revoking the join token: {e}")))?;
            match res {
                MetaResult::Ok { .. } => {
                    tracing::info!(token_id, "worker join token revoked");
                    return Ok(());
                }
                MetaResult::NotFound => {
                    return Err(ApiError::not_found(format!(
                        "worker token {token_id} not found"
                    )));
                }
                MetaResult::StaleVersion { .. } => continue,
                other => {
                    return Err(ApiError::internal(format!(
                        "revoking the join token: {other:?}"
                    )));
                }
            }
        }
        Err(ApiError::unavailable(
            "the join token changed concurrently; retry",
        ))
    }

    /// Whether `token_id` names a live (present) join token. Expiry is not checked: an
    /// expired token refuses join and renewal, but live sessions stay (D-W4).
    pub fn token_exists(&self, token_id: &str) -> bool {
        self.meta
            .read(|s| s.token(token_id).is_some_and(|t| is_join_token(&t.value)))
    }

    fn invalid_token() -> ApiError {
        ApiError::invalid_argument("join token is invalid, revoked or expired")
            .with_details(serde_json::json!({ "reason": REASON_JOIN_TOKEN_INVALID }))
    }

    /// Check `wkr_<id>.<secret>` (constant-time hash compare, scope, expiry).
    fn verify_secret(&self, token: &str) -> Result<Verified, ApiError> {
        let (id, secret) = token.split_once('.').ok_or_else(Self::invalid_token)?;
        if !id.starts_with(WORKER_TOKEN_PREFIX) || secret.is_empty() {
            return Err(Self::invalid_token());
        }
        let now = self.now_ms()?;
        let hash = sha256(secret.as_bytes());
        let ok = self.meta.read(|s| {
            s.token(id).is_some_and(|t| {
                // Evaluate every check (no early exit on the hash).
                let hash_ok = ct_eq(&t.value.hash, &hash);
                hash_ok && is_join_token(&t.value) && !expired(&t.value, now)
            })
        });
        if ok {
            Ok(Verified { id: id.to_owned() })
        } else {
            Err(Self::invalid_token())
        }
    }

    /// Check a token id from an authenticated certificate (scope, expiry).
    fn verify_id(&self, token_id: &str) -> Result<Verified, ApiError> {
        let now = self.now_ms()?;
        let ok = self.meta.read(|s| {
            s.token(token_id)
                .is_some_and(|t| is_join_token(&t.value) && !expired(&t.value, now))
        });
        if ok {
            Ok(Verified {
                id: token_id.to_owned(),
            })
        } else {
            Err(Self::invalid_token())
        }
    }

    fn sign(&self, csr_der: &[u8], token_id: &str, worker_id: &str) -> Result<String, ApiError> {
        let der = self
            .tls
            .sign_worker_csr(
                csr_der,
                token_id,
                worker_id,
                Duration::from_millis(self.client_cert_ttl_ms),
            )
            .map_err(|e| ApiError::invalid_argument(format!("csr: {e}")))?;
        Ok(pem_encode("CERTIFICATE", &der))
    }

    fn response(&self, client_cert_pem: String) -> Result<JoinResponse, ApiError> {
        let endpoints = self.endpoints();
        if endpoints.is_empty() {
            return Err(ApiError::unavailable(
                "the cluster listener is not running ([workers] enabled = false?)",
            ));
        }
        Ok(JoinResponse {
            client_cert_pem,
            ca_cert_pem: self
                .tls
                .ca_pem()
                .ok_or_else(|| ApiError::unavailable("no cluster CA"))?,
            endpoints,
            renew_after_ms: self.renew_after_ms(),
            cluster_id: self.tls.cluster_id().to_owned(),
        })
    }

    // --------------------------------------------------------------------- join / renew

    /// Join a worker (`POST /v1/workers/join`).
    pub async fn join(&self, req: JoinRequest) -> Result<JoinResponse, ApiError> {
        req.validate()?;
        if !valid_id(&req.worker_id, MAX_ID_LEN) {
            return Err(ApiError::invalid_argument(format!(
                "worker_id must be 1-{MAX_ID_LEN} characters of [A-Za-z0-9._-] to be certified"
            )));
        }
        let v = self.verify_secret(&req.token)?;
        let csr_der = csr_der(req.csr_pem.as_bytes())?;
        let cert = self.sign(&csr_der, &v.id, &req.worker_id)?;
        tracing::info!(token_id = %v.id, worker_id = %req.worker_id, "worker joined");
        self.response(cert)
    }

    /// Renew the certificate of the authenticated `token_id` / `worker_id`; `csr` is a PEM
    /// or DER PKCS#10 request.
    pub async fn renew(
        &self,
        token_id: &str,
        worker_id: &str,
        csr: &[u8],
    ) -> Result<JoinResponse, ApiError> {
        let v = self.verify_id(token_id)?;
        let csr_der = csr_der(csr)?;
        let cert = self.sign(&csr_der, &v.id, worker_id)?;
        tracing::info!(token_id, worker_id, "worker certificate renewed");
        self.response(cert)
    }
}

impl std::fmt::Debug for JoinService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinService").finish_non_exhaustive()
    }
}

fn is_join_token(t: &ApiToken) -> bool {
    t.id.starts_with(WORKER_TOKEN_PREFIX) && t.scopes.iter().any(|s| s == WORKER_JOIN_SCOPE)
}

fn expired(t: &ApiToken, now_ms: u64) -> bool {
    t.expires_at_ms.is_some_and(|e| now_ms >= e)
}

fn sha256(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

/// Constant-time equality of two digests.
fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut d = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        d |= x ^ y;
    }
    std::hint::black_box(d) == 0
}

/// A CSR as DER: PEM (`CERTIFICATE REQUEST` / `NEW CERTIFICATE REQUEST`) or raw DER.
fn csr_der(csr: &[u8]) -> Result<Vec<u8>, ApiError> {
    if csr.first() == Some(&0x30) {
        return Ok(csr.to_vec());
    }
    let (_, pem) = x509_parser::pem::parse_x509_pem(csr)
        .map_err(|_| ApiError::invalid_argument("csr: not a PEM certificate request"))?;
    if !pem.label.contains("CERTIFICATE REQUEST") {
        return Err(ApiError::invalid_argument(format!(
            "csr: PEM label {:?} is not a certificate request",
            pem.label
        )));
    }
    Ok(pem.contents)
}

fn base32_len(bytes: usize) -> usize {
    (bytes * 8).div_ceil(5)
}

/// `n` random Crockford base32 characters.
fn random_base32(n: usize) -> Result<String, ApiError> {
    let mut raw = vec![0u8; n];
    getrandom::fill(&mut raw).map_err(|e| ApiError::unavailable(format!("rng: {e}")))?;
    Ok(raw
        .iter()
        .map(|b| char::from(ALPHABET[usize::from(b & 31)]))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ct_eq_and_ids() {
        let a = sha256(b"x");
        assert!(ct_eq(&a, &a));
        assert!(!ct_eq(&a, &sha256(b"y")));
        let id = random_base32(TOKEN_ID_CHARS).unwrap();
        assert_eq!(id.len(), 10);
        assert!(id.bytes().all(|b| ALPHABET.contains(&b)));
        assert_eq!(base32_len(SECRET_BYTES), 32);
    }

    #[test]
    fn csr_forms() {
        assert!(csr_der(b"garbage").is_err());
        assert_eq!(csr_der(&[0x30, 1, 2]).unwrap(), vec![0x30, 1, 2]);
    }
}
