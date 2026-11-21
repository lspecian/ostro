//! Worker administration, join tokens, join and the demand endpoint (plan 07-14; WORK-01,
//! WORK-05).
//!
//! Every method delegates to the Phase 7 state the control plane holds (`cp.p7`, B3):
//!
//! | Method | Implementation |
//! |---|---|
//! | `list_workers` | `WorkerRegistry::sessions()`, one entry per worker id (sessions of one worker are merged: the newest session's labels/slots/connect time, free slots and leases summed, draining only when every session drains), `node_id` = this node |
//! | `drain_worker` | `WorkerRegistry::drain` (stops assignment to every session of the worker; 404 when none) |
//! | `create_worker_token` | `JoinService::create_token(ttl, join_url)`; the secret appears only in this answer |
//! | `list_worker_tokens` | `JoinService::list_tokens` (ids and times, never a secret or hash) |
//! | `revoke_worker_token` | `JoinService::revoke_token` (404 when absent; live sessions of the token are closed by the 07-09 revocation watcher) |
//! | `join_worker` | `JoinService::join` |
//! | `queue_demand` | `workers::demand::snapshot` (zeros until 07-16) |
//!
//! # Join errors
//!
//! A refused join (unknown, revoked or expired token, or a wrong secret) answers
//! `InvalidArgument` (HTTP 400 / gRPC `INVALID_ARGUMENT`) with
//! `details.reason = "join_token_invalid"`: the 07-11 contract, kept here because the
//! `ErrorCode` set has no "unauthenticated" and a non-specific answer does not tell a caller
//! which tokens exist. Malformed requests (CSR, worker id, labels) are plain
//! `InvalidArgument`; a server without a cluster CA or without cluster endpoints answers
//! `Unavailable`.
//!
//! # Join URL
//!
//! The join command printed with a new token names the client listener: the scheme follows
//! the effective TLS mode, the host is the first ACME domain (acme mode), else
//! `[connectors] public_base_url`'s authority, else `[api] listen` (an unspecified address
//! is shown as loopback). The control plane does not know the bound port of an ephemeral
//! listener (`:0`); operators of such servers pass `--join` themselves.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::phase7::{
    CreateWorkerTokenRequest, DemandSnapshot, DrainResult, JoinRequest, JoinResponse, Placement,
    WorkerInfo, WorkerTokenCreated, WorkerTokenInfo, require_name,
};

use super::ServerControlPlane;
use crate::tls::TlsMode;
use crate::workers::{demand, registry};

/// Sessions -> one [`WorkerInfo`] per worker id. `sessions` is sorted by
/// `(worker_id, connected_at_ms)` (the registry's order), so the last one wins.
pub(crate) fn merge_sessions(node_id: u64, sessions: Vec<registry::WorkerInfo>) -> Vec<WorkerInfo> {
    let mut out: BTreeMap<String, WorkerInfo> = BTreeMap::new();
    for s in sessions {
        match out.get_mut(&s.worker_id) {
            Some(w) => {
                w.labels = s.labels;
                w.slots = s.slots;
                w.connected_at_ms = s.connected_at_ms;
                w.free_slots = w.free_slots.saturating_add(s.free);
                w.leases = w.leases.saturating_add(s.leases);
                w.draining &= s.draining;
            }
            None => {
                out.insert(
                    s.worker_id.clone(),
                    WorkerInfo {
                        worker_id: s.worker_id,
                        labels: s.labels,
                        slots: s.slots,
                        free_slots: s.free,
                        leases: s.leases,
                        connected_at_ms: s.connected_at_ms,
                        node_id,
                        draining: s.draining,
                    },
                );
            }
        }
    }
    out.into_values().collect()
}

pub(crate) async fn list_workers(cp: &ServerControlPlane) -> Result<Vec<WorkerInfo>, ApiError> {
    Ok(merge_sessions(
        cp.cfg.node.node_id,
        cp.p7.registry.sessions(),
    ))
}

pub(crate) async fn drain_worker(
    cp: &ServerControlPlane,
    worker_id: String,
) -> Result<DrainResult, ApiError> {
    require_name("worker id", &worker_id)?;
    match cp.p7.registry.drain(&worker_id) {
        Some(already_draining) => Ok(DrainResult {
            worker_id,
            already_draining,
        }),
        None => Err(ApiError::not_found(format!(
            "worker {worker_id} is not connected"
        ))),
    }
}

fn loopback_if_unspecified(mut addr: SocketAddr) -> SocketAddr {
    if addr.ip().is_unspecified() {
        addr.set_ip(match addr {
            SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        });
    }
    addr
}

/// The client listener URL put into a join command (module docs).
pub(crate) fn join_url(cp: &ServerControlPlane) -> String {
    let mode = cp.p7.tls.mode();
    let scheme = match mode {
        TlsMode::Off => "http",
        TlsMode::SelfSigned | TlsMode::Files | TlsMode::Acme => "https",
    };
    let listen = loopback_if_unspecified(cp.cfg.api.listen);
    if mode == TlsMode::Acme
        && let Some(domain) = cp.cfg.api.tls.acme.domains.first()
    {
        return format!("{scheme}://{domain}:{}", listen.port());
    }
    if let Some(base) = &cp.cfg.connectors.public_base_url {
        let rest = base.split_once("://").map_or(base.as_str(), |(_, r)| r);
        let authority = rest.split('/').next().unwrap_or(rest);
        if !authority.is_empty() {
            return format!("{scheme}://{authority}");
        }
    }
    format!("{scheme}://{listen}")
}

pub(crate) async fn create_worker_token(
    cp: &ServerControlPlane,
    req: CreateWorkerTokenRequest,
) -> Result<WorkerTokenCreated, ApiError> {
    req.validate()?;
    let ttl = req.ttl_ms.map(Duration::from_millis);
    cp.p7.join.create_token(ttl, &join_url(cp)).await
}

pub(crate) async fn list_worker_tokens(
    cp: &ServerControlPlane,
) -> Result<Vec<WorkerTokenInfo>, ApiError> {
    Ok(cp.p7.join.list_tokens())
}

pub(crate) async fn revoke_worker_token(
    cp: &ServerControlPlane,
    token_id: String,
) -> Result<(), ApiError> {
    require_name("token id", &token_id)?;
    cp.p7.join.revoke_token(&token_id).await
}

pub(crate) async fn join_worker(
    cp: &ServerControlPlane,
    req: JoinRequest,
) -> Result<JoinResponse, ApiError> {
    cp.p7.join.join(req).await
}

/// The server's demand snapshot in the API shape.
pub(crate) fn to_api(s: demand::DemandSnapshot) -> DemandSnapshot {
    DemandSnapshot {
        placement: Placement::parse(&s.placement).unwrap_or(Placement::Embedded),
        queue: s.queue,
        demand: s.demand,
        backlog: s.backlog,
        running_on_workers: s.running_on_workers,
        spilling: s.spilling,
        workers: s.workers,
        worker_slots_free: s.worker_slots_free,
    }
}

pub(crate) async fn queue_demand(
    cp: &ServerControlPlane,
    queue: String,
) -> Result<DemandSnapshot, ApiError> {
    require_name("queue name", &queue)?;
    Ok(to_api(demand::snapshot(&cp.p7, &queue)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(
        worker: &str,
        at: u64,
        free: u32,
        leases: u32,
        draining: bool,
    ) -> registry::WorkerInfo {
        registry::WorkerInfo {
            session_id: Default::default(),
            worker_id: worker.into(),
            token_id: None,
            labels: BTreeMap::from([("at".to_owned(), at.to_string())]),
            slots: 4,
            free,
            leases,
            memory_bytes: 0,
            connected_at_ms: at,
            draining,
        }
    }

    #[test]
    fn sessions_merge_per_worker() {
        let v = merge_sessions(
            7,
            vec![
                session("a", 1, 1, 2, true),
                session("a", 5, 3, 1, false),
                session("b", 2, 4, 0, true),
            ],
        );
        assert_eq!(v.len(), 2);
        let a = &v[0];
        assert_eq!(a.worker_id, "a");
        assert_eq!(a.connected_at_ms, 5);
        assert_eq!(a.labels["at"], "5");
        assert_eq!((a.free_slots, a.leases), (4, 3));
        assert!(!a.draining);
        assert_eq!(a.node_id, 7);
        assert!(v[1].draining);
    }

    #[test]
    fn demand_maps_placement() {
        let d = to_api(demand::DemandSnapshot {
            queue: "q".into(),
            placement: "workers".into(),
            demand: 3,
            ..Default::default()
        });
        assert_eq!(d.placement, Placement::Workers);
        assert_eq!(d.demand, 3);
        let d = to_api(demand::DemandSnapshot::default());
        assert_eq!(d.placement, Placement::Embedded);
    }
}
