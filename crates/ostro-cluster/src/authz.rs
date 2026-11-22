//! Phase 12: implemented in 12-05 (peer node identity checks for cluster services).

use tonic::{Request, Status};

/// The calling node's id, from the [`PeerNode`](crate::transport::PeerNode) the transport
/// put into the request extensions; `Status::unauthenticated` / `permission_denied` for
/// anything else (worker certificates, other clusters).
pub fn require_node<T>(req: &Request<T>) -> Result<u64, Status> {
    let _ = req;
    Err(Status::unimplemented("Phase 12: implemented in 12-05"))
}
