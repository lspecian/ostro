//! The production cluster transport: mTLS over TCP (12-05).
//!
//! - **Client side.** One lazily connecting tonic [`Channel`] per peer to
//!   `https://<cluster_addr>` (from the [`AddressBook`]), with this node's certificate as
//!   the client identity and the cluster CA as the only root (rustls via tonic
//!   `tls-aws-lc`), HTTP/2 keepalive and TCP_NODELAY. A channel is re-dialled when the
//!   peer's address changes.
//! - **Server side.** [`TcpTransport::server_tls`] is the listener's TLS config (node
//!   certificate, client certificate required and verified against the cluster CA), and
//!   [`PeerIdentity`] is the interceptor that turns the verified peer certificate into
//!   [`PeerNode`] (or [`PeerRejected`]) for `authz::require_node`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig,
};
use tonic::{Request, Status};

use super::{ClusterIncoming, ClusterTransport, PeerNode, TransportError};
use crate::address_book::AddressBook;
use crate::authz::{PeerRejected, node_identity_from_der};

/// This node's TLS material, PEM. `Debug` never shows the key.
#[derive(Clone)]
pub struct NodeTls {
    /// The node certificate chain (leaf first).
    pub cert_chain_pem: String,
    /// The node private key.
    pub key_pem: String,
    /// The cluster CA certificate (the only trust root).
    pub ca_pem: String,
}

impl std::fmt::Debug for NodeTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeTls")
            .field("cert_chain_pem", &self.cert_chain_pem.len())
            .field("key_pem", &"[redacted]")
            .field("ca_pem", &self.ca_pem.len())
            .finish()
    }
}

/// Client channel tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpTransportConfig {
    /// TCP + TLS connect timeout.
    pub connect_timeout: Duration,
    /// HTTP/2 PING interval (also while idle).
    pub keepalive_interval: Duration,
    /// How long a PING may stay unanswered before the connection is dropped.
    pub keepalive_timeout: Duration,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        TcpTransportConfig {
            connect_timeout: Duration::from_secs(3),
            keepalive_interval: Duration::from_secs(5),
            keepalive_timeout: Duration::from_secs(10),
        }
    }
}

/// mTLS TCP [`ClusterTransport`].
pub struct TcpTransport {
    cluster_id: String,
    node_id: u64,
    book: Arc<AddressBook>,
    tls: NodeTls,
    client_tls: ClientTlsConfig,
    cfg: TcpTransportConfig,
    /// node → (address the channel dials, channel).
    channels: Mutex<HashMap<u64, (String, Channel)>>,
}

impl std::fmt::Debug for TcpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpTransport")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("peers", &self.book.ids())
            .finish_non_exhaustive()
    }
}

impl TcpTransport {
    /// A transport for node `node_id` of `cluster_id` with the default tuning.
    pub fn new(
        cluster_id: &str,
        node_id: u64,
        tls: NodeTls,
        book: Arc<AddressBook>,
    ) -> Result<Self, TransportError> {
        Self::with_config(
            cluster_id,
            node_id,
            tls,
            book,
            TcpTransportConfig::default(),
        )
    }

    /// [`TcpTransport::new`] with explicit tuning.
    pub fn with_config(
        cluster_id: &str,
        node_id: u64,
        tls: NodeTls,
        book: Arc<AddressBook>,
        cfg: TcpTransportConfig,
    ) -> Result<Self, TransportError> {
        let bad = |reason: &str| TransportError::Connect {
            node: node_id,
            reason: reason.to_owned(),
        };
        if !tls.cert_chain_pem.contains("-----BEGIN CERTIFICATE-----") {
            return Err(bad("node certificate chain has no PEM certificate"));
        }
        if !tls.ca_pem.contains("-----BEGIN CERTIFICATE-----") {
            return Err(bad("cluster CA has no PEM certificate"));
        }
        if !tls.key_pem.contains("PRIVATE KEY-----") {
            return Err(bad("node key is not a PEM private key"));
        }
        let client_tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&tls.ca_pem))
            .identity(Identity::from_pem(&tls.cert_chain_pem, &tls.key_pem));
        Ok(TcpTransport {
            cluster_id: cluster_id.to_owned(),
            node_id,
            book,
            tls,
            client_tls,
            cfg,
            channels: Mutex::new(HashMap::new()),
        })
    }

    /// The cluster this node belongs to.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// This node's id.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// The address book the channels dial from.
    pub fn address_book(&self) -> &Arc<AddressBook> {
        &self.book
    }

    /// The cluster listener's TLS config: the node certificate, and a client certificate
    /// required and verified against the cluster CA.
    pub fn server_tls(&self) -> ServerTlsConfig {
        ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &self.tls.cert_chain_pem,
                &self.tls.key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&self.tls.ca_pem))
    }

    /// The interceptor that maps the peer certificate to [`PeerNode`] for this cluster.
    pub fn identity_interceptor(&self) -> PeerIdentity {
        PeerIdentity::new(&self.cluster_id)
    }

    fn dial(&self, node: u64, addr: &str) -> Result<Channel, TransportError> {
        let err = |reason: String| TransportError::Connect { node, reason };
        let ep = Endpoint::from_shared(format!("https://{addr}"))
            .map_err(|e| err(format!("endpoint {addr}: {e}")))?
            .tls_config(self.client_tls.clone())
            .map_err(|e| err(format!("TLS config: {e}")))?
            .connect_timeout(self.cfg.connect_timeout)
            .http2_keep_alive_interval(self.cfg.keepalive_interval)
            .keep_alive_timeout(self.cfg.keepalive_timeout)
            .keep_alive_while_idle(true)
            .tcp_nodelay(true);
        Ok(ep.connect_lazy())
    }
}

impl ClusterTransport for TcpTransport {
    /// A lazily connecting channel to `node` (cached per address). Must be called inside
    /// a tokio runtime (the channel's worker task is spawned there).
    fn channel(&self, node: u64) -> Result<Channel, TransportError> {
        let addr = self
            .book
            .cluster_addr(node)
            .ok_or(TransportError::UnknownNode(node))?;
        let mut channels = self.channels.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((a, ch)) = channels.get(&node)
            && *a == addr
        {
            return Ok(ch.clone());
        }
        let ch = self.dial(node, &addr)?;
        channels.insert(node, (addr, ch.clone()));
        Ok(ch)
    }

    fn incoming(&self) -> ClusterIncoming {
        ClusterIncoming::Tcp
    }
}

/// Server-side interceptor: the verified peer certificate (tonic `peer_certs`, so the
/// listener must use [`TcpTransport::server_tls`]) becomes [`PeerNode`] when it is a node
/// certificate of this cluster, else [`PeerRejected`] with the reason. A request that
/// already carries a [`PeerNode`] (an in-memory transport) is left alone; a request
/// without any certificate gets neither (`require_node` answers `UNAUTHENTICATED`).
/// Never rejects by itself: the cluster listener also serves workers.
#[derive(Clone, Debug)]
pub struct PeerIdentity {
    cluster_id: Arc<str>,
}

impl PeerIdentity {
    /// The interceptor for `cluster_id`.
    pub fn new(cluster_id: &str) -> Self {
        PeerIdentity {
            cluster_id: Arc::from(cluster_id),
        }
    }

    /// Annotate `req` (the interceptor body, usable from a custom layer too).
    pub fn identify<T>(&self, req: &mut Request<T>) {
        if req.extensions().get::<PeerNode>().is_some() {
            return;
        }
        let Some(certs) = req.peer_certs() else {
            return;
        };
        let Some(leaf) = certs.first() else {
            return;
        };
        match node_identity_from_der(leaf.as_ref(), &self.cluster_id) {
            Ok(id) => {
                req.extensions_mut().insert(PeerNode(id));
            }
            Err(why) => {
                req.extensions_mut().insert(PeerRejected(why));
            }
        }
    }
}

impl tonic::service::Interceptor for PeerIdentity {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        self.identify(&mut req);
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_pem_material() {
        let tls = NodeTls {
            cert_chain_pem: "x".into(),
            key_pem: "y".into(),
            ca_pem: "z".into(),
        };
        assert!(format!("{tls:?}").contains("redacted"));
        let r = TcpTransport::new("c", 1, tls, Arc::new(AddressBook::new()));
        assert!(matches!(r, Err(TransportError::Connect { node: 1, .. })));
    }

    #[test]
    fn identify_without_certificates_sets_nothing() {
        let id = PeerIdentity::new("c");
        let mut req = Request::new(());
        id.identify(&mut req);
        assert!(req.extensions().get::<PeerNode>().is_none());
        assert!(req.extensions().get::<PeerRejected>().is_none());
        // An in-memory transport's PeerNode is kept.
        req.extensions_mut().insert(PeerNode(4));
        id.identify(&mut req);
        assert_eq!(req.extensions().get::<PeerNode>(), Some(&PeerNode(4)));
    }
}
