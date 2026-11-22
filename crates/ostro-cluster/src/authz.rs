//! Peer node identity checks for cluster services (12-05).
//!
//! The cluster listener is shared with the Phase 7 worker service, so the transport never
//! rejects a connection by identity: it annotates every request with either
//! [`PeerNode`] (a node certificate of this cluster) or [`PeerRejected`] (any other client
//! certificate: a worker, another cluster's node, a malformed identity). Cluster services
//! (MetaPeer, Replication) then call [`require_node`].
//!
//! | caller | result |
//! |--------|--------|
//! | node certificate `ostro://<this cluster>/node/<id>` | `Ok(id)` |
//! | worker certificate, another cluster's node, malformed identity | `PERMISSION_DENIED` |
//! | no identity at all (no client certificate seen) | `UNAUTHENTICATED` |

use tonic::{Request, Status};
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

use crate::transport::PeerNode;

/// URI scheme of node and worker identity SANs (ostro-server `tls::ca::IDENTITY_SCHEME`).
pub const IDENTITY_SCHEME: &str = "ostro://";

/// Set by the transport when the client presented a certificate that is not a node
/// certificate of this cluster; the reason goes into the `PERMISSION_DENIED` message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerRejected(pub String);

/// The node id of a node certificate of `cluster_id` (`ostro://<cluster_id>/node/<id>`,
/// exactly one `ostro://` URI SAN, id non-zero). Worker certificates, other clusters'
/// node certificates and anything malformed are an error naming why.
pub fn node_identity_from_der(cert_der: &[u8], cluster_id: &str) -> Result<u64, String> {
    let (_, cert) =
        X509Certificate::from_der(cert_der).map_err(|e| format!("unparseable certificate: {e}"))?;
    let san = cert
        .subject_alternative_name()
        .map_err(|e| format!("bad subjectAltName: {e}"))?;
    let uris: Vec<&str> = san
        .map(|ext| {
            ext.value
                .general_names
                .iter()
                .filter_map(|n| match n {
                    GeneralName::URI(u) if u.starts_with(IDENTITY_SCHEME) => Some(*u),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    let uri = match uris.as_slice() {
        [one] => *one,
        [] => return Err("certificate has no ostro:// identity SAN".into()),
        _ => return Err("certificate has more than one ostro:// identity SAN".into()),
    };
    let parts: Vec<&str> = uri[IDENTITY_SCHEME.len()..].split('/').collect();
    match parts.as_slice() {
        [cluster, "node", id] => {
            if *cluster != cluster_id {
                return Err(format!(
                    "node certificate of cluster {cluster:?}, this is cluster {cluster_id:?}"
                ));
            }
            match id.parse::<u64>() {
                Ok(0) | Err(_) => Err(format!("node identity {uri:?} has an invalid node id")),
                Ok(n) => Ok(n),
            }
        }
        [_, "worker", ..] => Err("worker certificates may not call cluster services".into()),
        _ => Err(format!("not a node identity: {uri:?}")),
    }
}

/// The calling node's id, from the [`PeerNode`] the transport put into the request
/// extensions; `Status::permission_denied` when the transport marked the caller
/// [`PeerRejected`] (worker certificates, other clusters), `Status::unauthenticated` when
/// there is no identity at all.
pub fn require_node<T>(req: &Request<T>) -> Result<u64, Status> {
    if let Some(PeerNode(id)) = req.extensions().get::<PeerNode>()
        && *id != 0
    {
        return Ok(*id);
    }
    match req.extensions().get::<PeerRejected>() {
        Some(PeerRejected(why)) => Err(Status::permission_denied(format!(
            "cluster services require a node certificate of this cluster: {why}"
        ))),
        None => Err(Status::unauthenticated(
            "cluster services require a node certificate",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_node_matrix() {
        let mut req = Request::new(());
        assert_eq!(
            require_node(&req).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        req.extensions_mut()
            .insert(PeerRejected("worker certificates may not".into()));
        let s = require_node(&req).unwrap_err();
        assert_eq!(s.code(), tonic::Code::PermissionDenied);
        assert!(s.message().contains("worker"), "{}", s.message());
        req.extensions_mut().insert(PeerNode(0));
        assert_eq!(
            require_node(&req).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        req.extensions_mut().insert(PeerNode(3));
        assert_eq!(require_node(&req).unwrap(), 3);
    }

    #[test]
    fn garbage_der_is_rejected() {
        assert!(node_identity_from_der(b"not a cert", "c").is_err());
    }
}
