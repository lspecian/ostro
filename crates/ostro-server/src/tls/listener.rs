//! The client listener: plaintext (`axum::serve`) when TLS is off, else TLS through
//! axum-server with the node's `ResolvesServerCert` and ALPN `["h2", "http/1.1"]`.
//!
//! Mode dispatch ([`TlsMode`] is resolved by [`super::init`], D-W8):
//! - `off` (and `auto` on loopback): today's plaintext path (HTTP/1.1 + h2c);
//! - `self-signed`: the CA-issued dev cert;
//! - `files`: the PEM files, reloaded on SIGHUP and on an mtime poll every
//!   `[api.tls] reload_poll` (a failed reload keeps the served cert);
//! - `acme`: [`super::acme::start`] (07-10), then TLS with the certificate it installs.
//!
//! Every TLS mode serves through the same resolver, so a swap applies from the next
//! handshake with no restart. Graceful shutdown: the `CancellationToken` drives an
//! axum-server `Handle::graceful_shutdown(None)` (in-flight requests finish), matching the
//! plaintext path.

use std::future::Future;
use std::io;
use std::sync::Arc;

use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{TlsMode, TlsState, acme};

fn io_other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// Serve `app` on `listener` until `shutdown` fires (graceful: in-flight requests finish).
// The signature is fixed by 07-02 (an explicit `Send + 'static` future for p7.rs).
#[allow(clippy::manual_async_fn)]
pub fn serve(
    state: Arc<TlsState>,
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> impl Future<Output = io::Result<()>> + Send + 'static {
    async move {
        if state.mode() == TlsMode::Off {
            return axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await;
        }
        let mut aux: Vec<JoinHandle<()>> = Vec::new();
        match state.mode() {
            TlsMode::Files => aux.push(spawn_files_reloader(Arc::clone(&state), shutdown.clone())),
            TlsMode::Acme => {
                aux.push(acme::start(Arc::clone(&state), shutdown.clone()).map_err(io_other)?)
            }
            TlsMode::Off | TlsMode::SelfSigned => {}
        }
        let res = serve_tls(&state, listener, app, shutdown).await;
        for t in aux {
            t.abort();
        }
        res
    }
}

async fn serve_tls(
    state: &Arc<TlsState>,
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let config = state.client_server_config().map_err(io_other)?;
    let addr = listener.local_addr()?;
    let std_listener = listener.into_std()?;
    let handle = axum_server::Handle::<std::net::SocketAddr>::new();
    let stopper = {
        let handle = handle.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            handle.graceful_shutdown(None);
        })
    };
    info!(
        %addr,
        mode = ?state.mode(),
        fingerprint = ?state.fingerprint(),
        "client listener serving TLS"
    );
    let res = match axum_server::from_tcp_rustls(std_listener, RustlsConfig::from_config(config)) {
        Ok(server) => server.handle(handle).serve(app.into_make_service()).await,
        Err(e) => Err(e),
    };
    stopper.abort();
    res
}

/// `files` mode: reload on SIGHUP and when an mtime changes (poll every `reload_poll`).
fn spawn_files_reloader(state: Arc<TlsState>, shutdown: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        #[cfg(unix)]
        let mut hup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(error = %e, "[api.tls] cannot watch SIGHUP; mtime poll only");
                None
            }
        };
        let poll = state.reload_poll();
        let mut tick = (!poll.is_zero()).then(|| {
            // The first tick fires at once; mtimes are unchanged then, so nothing reloads.
            let mut t = tokio::time::interval(poll);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t
        });
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = async {
                    match tick.as_mut() {
                        Some(t) => { t.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    state.poll_files();
                }
                _ = async {
                    #[cfg(unix)]
                    match hup.as_mut() {
                        Some(h) => { h.recv().await; }
                        None => std::future::pending::<()>().await,
                    }
                    #[cfg(not(unix))]
                    std::future::pending::<()>().await
                } => {
                    info!("[api.tls] SIGHUP: reloading the certificate files");
                    let _ = state.reload_files();
                }
            }
        }
    })
}
