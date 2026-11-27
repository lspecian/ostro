//! `PeerControlPlane` (13-12): the forwarding client to a peer's cluster listener, one
//! `ostro_api::grpc_client::GrpcControlPlane` per node over `ClusterTransport::channel(node)`
//! (mTLS node identity; the in-memory transport in tier B). The router forwards inside a
//! `RequestMeta` scope with `hop = 1`; the client copies it into the `ostro-*` metadata and
//! sends the remaining deadline. Transport failures come back as `unavailable` (the 13-03
//! client's `status_to_error`), so the router's retry rule applies; a node whose channel
//! cannot be built at all answers `unavailable` too.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ostro_api::grpc_client::{DEFAULT_MAX_MESSAGE_BYTES, GrpcControlPlane};
use ostro_api::request_meta::RequestMeta;
use ostro_api::{ApiError, ApiExtensions, ControlPlane};
use ostro_cluster::transport::ClusterTransport;

use crate::p14::principal_fwd;

/// Phase 14 (14-15, D-P14-6): set `ostro-principal` of a forwarded call's metadata to the
/// current principal (the client copies it into the outgoing request, `to_metadata`). Any
/// inherited value is replaced, so a forwarded call never relays a header; an internal
/// caller without a principal (`Anonymous`) sends none and the peer treats the call as
/// internal (`principal_fwd::ingest`).
pub fn stamp_principal(out: &mut RequestMeta) {
    let p = ostro_api::auth::current();
    out.principal = (!p.is_anonymous()).then(|| principal_fwd::encode(&p));
}

/// One peer's API as the router calls it: the control plane and every extension slot (all
/// backed by the same gRPC client in production; tests plug fakes in).
#[derive(Clone)]
pub struct PeerApi {
    /// The peer's control plane.
    pub cp: Arc<dyn ControlPlane>,
    /// The peer's extension traits (`None` slot: the call answers `unavailable`).
    pub ext: ApiExtensions,
}

impl std::fmt::Debug for PeerApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerApi").finish_non_exhaustive()
    }
}

impl PeerApi {
    /// Every slot backed by one gRPC client.
    pub fn grpc(client: Arc<GrpcControlPlane>) -> Self {
        let ext = ApiExtensions {
            schedules: Some(client.clone()),
            webhooks: Some(client.clone()),
            hooks: Some(client.clone()),
            run_logs: Some(client.clone()),
            admin: Some(client.clone()),
            ops: Some(client.clone()),
            inventory: Some(client.clone()),
            cluster: Some(client.clone()),
            module_lifecycle: Some(client.clone()),
            ..ApiExtensions::default()
        };
        PeerApi { cp: client, ext }
    }
}

/// The per-node client cache over the cluster transport.
pub struct PeerControlPlane {
    transport: Arc<dyn ClusterTransport>,
    max_message_bytes: usize,
    clients: Mutex<HashMap<u64, Arc<GrpcControlPlane>>>,
}

impl std::fmt::Debug for PeerControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerControlPlane")
            .field("max_message_bytes", &self.max_message_bytes)
            .finish_non_exhaustive()
    }
}

impl PeerControlPlane {
    /// Clients over `transport`; messages up to `max_message_bytes` (at least the client
    /// default).
    pub fn new(transport: Arc<dyn ClusterTransport>, max_message_bytes: usize) -> Self {
        Self {
            transport,
            max_message_bytes: max_message_bytes.max(DEFAULT_MAX_MESSAGE_BYTES),
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// The client of `node` (cached; built from `ClusterTransport::channel(node)`).
    pub fn client(&self, node: u64) -> Result<Arc<GrpcControlPlane>, ApiError> {
        let mut clients = self.clients.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = clients.get(&node) {
            return Ok(Arc::clone(c));
        }
        let ch = self.transport.channel(node).map_err(|e| {
            ApiError::unavailable(format!("node {node} is unreachable: {e}"))
                .with_details(serde_json::json!({ "reason": "peer_unreachable", "node": node }))
        })?;
        let c = Arc::new(GrpcControlPlane::new(ch).with_max_message_bytes(self.max_message_bytes));
        clients.insert(node, Arc::clone(&c));
        Ok(c)
    }

    /// Drop `node`'s cached client (after a transport failure: the address may have moved,
    /// and the transport re-dials a changed address).
    pub fn forget(&self, node: u64) {
        self.clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&node);
    }

    /// `node`'s API.
    pub fn api(&self, node: u64) -> Result<PeerApi, ApiError> {
        self.client(node).map(PeerApi::grpc)
    }
}
