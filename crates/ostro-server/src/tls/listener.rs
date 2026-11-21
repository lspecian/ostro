//! Phase 7: implemented in 07-06.
//!
//! The client listener: plaintext (`axum::serve`) when TLS is off, else TLS through
//! axum-server with the node's `ResolvesServerCert` and ALPN `["h2", "http/1.1"]`.

use std::future::Future;
use std::io;
use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::TlsState;

/// Serve `app` on `listener` until `shutdown` fires (graceful: in-flight requests finish).
///
/// Phase 7 stub (07-02): today's plaintext `axum::serve` with graceful shutdown.
pub fn serve(
    state: Arc<TlsState>,
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> impl Future<Output = io::Result<()>> + Send + 'static {
    let _ = state;
    async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    }
}
