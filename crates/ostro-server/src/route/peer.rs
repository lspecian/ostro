//! Phase 13: implemented in 13-12 (`PeerControlPlane`: the forwarding client to a peer's
//! cluster listener over `ostro_api::grpc_client::GrpcControlPlane` and the cluster
//! transport, one hop at most, `ostro-hop: 1`). No fixed signature: only [`super`] uses it.
