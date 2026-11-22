//! The cluster transport contract (12-02): how nodes reach each other's cluster listener.
//!
//! Production uses [`tcp`] (mTLS over TCP, 12-05); the conformance harness injects an
//! in-memory transport through the server hook `ServerOptions::cluster_transport`. Either
//! way the transport inserts the authenticated caller as [`PeerNode`] into every request's
//! extensions, which `authz::require_node` reads.

pub mod tcp;

use tokio::io::DuplexStream;
use tokio::sync::mpsc;

/// The authenticated calling node, inserted into every request's extensions by the
/// transport (from the mTLS peer certificate, or by the in-memory transport).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PeerNode(pub u64);

/// One accepted in-memory connection: the server half of a duplex pipe plus the identity
/// of the connecting node.
#[derive(Debug)]
pub struct MemConn {
    /// Server side of the byte pipe (HTTP/2 runs over it).
    pub io: DuplexStream,
    /// Who connected.
    pub peer: PeerNode,
}

/// Where the cluster listener's connections come from.
#[derive(Debug)]
pub enum ClusterIncoming {
    /// Bind the configured TCP listener(s) (mTLS; production).
    Tcp,
    /// Accept in-memory connections (conformance harness).
    Memory(mpsc::Receiver<MemConn>),
}

/// Transport failure.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// No address is known for the node.
    #[error("unknown node {0}")]
    UnknownNode(u64),
    /// Building or connecting the channel failed.
    #[error("connect to node {node}: {reason}")]
    Connect {
        /// Target node.
        node: u64,
        /// What failed.
        reason: String,
    },
}

/// Node-to-node channels and the incoming side of the cluster listener.
pub trait ClusterTransport: Send + Sync + 'static {
    /// A (lazily connecting) channel to `node`'s cluster listener.
    fn channel(&self, node: u64) -> Result<tonic::transport::Channel, TransportError>;

    /// The incoming connections. Called once, by the server that owns the listener.
    fn incoming(&self) -> ClusterIncoming;
}
