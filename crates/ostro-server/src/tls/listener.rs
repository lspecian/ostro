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
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::conn_limit::{CappedAddr, CappedListener, ConnLimit};
use super::{TlsMode, TlsState, acme};

fn io_other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// Serve `app` on `listener` until `shutdown` fires (graceful: in-flight requests finish).
/// No connection cap (tests); the server uses [`serve_capped`].
// The signature is fixed by 07-02 (an explicit `Send + 'static` future for p7.rs).
#[allow(clippy::manual_async_fn)]
pub fn serve(
    state: Arc<TlsState>,
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
) -> impl Future<Output = io::Result<()>> + Send + 'static {
    serve_capped(state, listener, app, shutdown, None)
}

/// [`serve`] with at most `limit` concurrent connections (p14-fixups-4,
/// `[api] max_connections`; `None` = uncapped). At the cap no further connection is
/// accepted until one closes (see [`conn_limit`]).
#[allow(clippy::manual_async_fn)]
pub fn serve_capped(
    state: Arc<TlsState>,
    listener: TcpListener,
    app: Router,
    shutdown: CancellationToken,
    limit: Option<ConnLimit>,
) -> impl Future<Output = io::Result<()>> + Send + 'static {
    async move {
        if state.mode() == TlsMode::Off {
            return match limit {
                None => {
                    axum::serve(listener, app)
                        .with_graceful_shutdown(shutdown.cancelled_owned())
                        .await
                }
                Some(limit) => {
                    axum::serve(CappedListener::new(listener, limit), app)
                        .with_graceful_shutdown(shutdown.cancelled_owned())
                        .await
                }
            };
        }
        let mut aux: Vec<JoinHandle<()>> = Vec::new();
        match state.mode() {
            TlsMode::Files => aux.push(spawn_files_reloader(Arc::clone(&state), shutdown.clone())),
            TlsMode::Acme => {
                aux.push(acme::start(Arc::clone(&state), shutdown.clone()).map_err(io_other)?)
            }
            TlsMode::Off | TlsMode::SelfSigned => {}
        }
        let res = serve_tls(&state, listener, app, shutdown, limit).await;
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
    limit: Option<ConnLimit>,
) -> io::Result<()> {
    let config = state.client_server_config().map_err(io_other)?;
    let addr = listener.local_addr()?;
    info!(
        %addr,
        mode = ?state.mode(),
        fingerprint = ?state.fingerprint(),
        max_connections = limit.as_ref().map(ConnLimit::max),
        "client listener serving TLS"
    );
    let acceptor = RustlsAcceptor::new(RustlsConfig::from_config(config));
    match limit {
        None => {
            let std_listener = listener.into_std()?;
            let handle = axum_server::Handle::<std::net::SocketAddr>::new();
            let stopper = graceful_stopper(handle.clone(), shutdown);
            let res = match axum_server::from_tcp(std_listener) {
                Ok(server) => {
                    server
                        .acceptor(acceptor)
                        .handle(handle)
                        .serve(app.into_make_service())
                        .await
                }
                Err(e) => Err(e),
            };
            stopper.abort();
            res
        }
        Some(limit) => {
            let handle = axum_server::Handle::<CappedAddr>::new();
            let stopper = graceful_stopper(handle.clone(), shutdown);
            let res = axum_server::Server::<CappedAddr>::from_listener(CappedListener::new(
                listener, limit,
            ))
            .acceptor(acceptor)
            .handle(handle)
            .serve(app.into_make_service())
            .await;
            stopper.abort();
            res
        }
    }
}

/// `shutdown` → `handle.graceful_shutdown(None)` (in-flight requests finish).
fn graceful_stopper<A: axum_server::Address + Send + Sync + 'static>(
    handle: axum_server::Handle<A>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        shutdown.cancelled().await;
        handle.graceful_shutdown(None);
    })
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
