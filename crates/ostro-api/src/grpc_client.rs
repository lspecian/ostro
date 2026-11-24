//! Phase 13: implemented in 13-03 (the production `ostro.api.v1` gRPC client:
//! `PeerControlPlane` forwarding over the mTLS cluster listener, D-P13-14).
//!
//! Fixed constructor; 13-03 adds the `ControlPlane` and extension-trait impls.

use tonic::transport::Channel;

/// A `ControlPlane` over an `ostro.api.v1` gRPC channel (13-03).
#[derive(Clone, Debug)]
pub struct GrpcControlPlane {
    channel: Channel,
}

impl GrpcControlPlane {
    /// A client over `channel`.
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }

    /// The underlying channel.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }
}
