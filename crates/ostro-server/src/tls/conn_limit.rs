//! p14-fixups-4: the cap on concurrent client-listener connections (`[api] max_connections`).
//!
//! Every accepted connection holds one permit until it closes. At the cap the listener stops
//! calling `accept(2)`: new connections wait in the kernel's accept backlog (no descriptor
//! is used for them), so a connection flood cannot exhaust the process's open-file limit and
//! starve the WAL, the state stores and the metadata Raft log of descriptors (the 14-23 soak
//! smoke: EMFILE stopped the metadata Raft driver). Both listener paths use it: plaintext
//! (`axum::serve` over [`CappedListener`]) and TLS (axum-server over [`CappedAddr`]).
//!
//! HTTP/2 streams need no descriptor; hyper bounds them per connection (200 by default).
//!
//! `max_connections = 0` (the default) resolves to half of the process's soft
//! `RLIMIT_NOFILE`, clamped to [[`AUTO_MIN`], [`AUTO_MAX`]] ([`resolve`]); the binary raises
//! the soft limit to the hard limit at startup ([`raise_nofile_soft_limit`]) before that.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

/// The automatic cap never goes below this (a tiny fd limit still serves a few clients).
pub const AUTO_MIN: usize = 64;
/// The automatic cap never goes above this.
pub const AUTO_MAX: usize = 16_384;

/// The process's soft `RLIMIT_NOFILE`, `None` when unlimited or unknown.
pub fn soft_nofile() -> Option<u64> {
    nofile().and_then(|(soft, _)| soft)
}

/// (soft, hard) `RLIMIT_NOFILE`; `None` in a field = unlimited.
#[cfg(not(unix))]
fn nofile() -> Option<(Option<u64>, Option<u64>)> {
    None
}

/// (soft, hard) `RLIMIT_NOFILE`; `None` in a field = unlimited.
#[cfg(unix)]
fn nofile() -> Option<(Option<u64>, Option<u64>)> {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes into the struct we own; no other memory is touched.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
    if rc != 0 {
        return None;
    }
    // `rlim_t` is not `u64` on every unix target.
    #[allow(clippy::useless_conversion)]
    let f = |v: libc::rlim_t| {
        (v != libc::RLIM_INFINITY)
            .then(|| u64::try_from(v).ok())
            .flatten()
    };
    Some((f(rl.rlim_cur), f(rl.rlim_max)))
}

/// Raise the soft `RLIMIT_NOFILE` to the hard limit (what Go's runtime does at startup):
/// systemd and many shells start services with a soft limit of 1,024 and a much higher hard
/// limit. Returns `(old soft, new soft)` when it changed anything (`None` also when the
/// kernel refuses, e.g. macOS above `OPEN_MAX`: the limit then stays as it was).
#[cfg(not(unix))]
pub fn raise_nofile_soft_limit() -> Option<(u64, u64)> {
    None
}

/// See the unix variant.
#[cfg(unix)]
pub fn raise_nofile_soft_limit() -> Option<(u64, u64)> {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: as above.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) } != 0 {
        return None;
    }
    if rl.rlim_cur == rl.rlim_max || rl.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    let target = rl.rlim_max;
    let old = rl.rlim_cur;
    rl.rlim_cur = target;
    // SAFETY: setrlimit reads the struct we own.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rl) } != 0 {
        return None;
    }
    // `rlim_t` is not `u64` on every unix target.
    #[allow(clippy::useless_conversion)]
    Some((
        u64::try_from(old).unwrap_or(u64::MAX),
        u64::try_from(target).unwrap_or(u64::MAX),
    ))
}

/// The effective cap for `[api] max_connections = configured` (0 = automatic).
pub fn resolve(configured: u64) -> usize {
    if configured > 0 {
        return usize::try_from(configured).unwrap_or(usize::MAX);
    }
    auto_for(soft_nofile())
}

/// The automatic cap for a soft fd limit (`None` = unlimited): half of it, clamped.
pub fn auto_for(soft: Option<u64>) -> usize {
    match soft {
        Some(n) => usize::try_from(n / 2)
            .unwrap_or(AUTO_MAX)
            .clamp(AUTO_MIN, AUTO_MAX),
        None => AUTO_MAX,
    }
}

/// The shared permit pool of one listener.
#[derive(Clone, Debug)]
pub struct ConnLimit {
    sem: Arc<Semaphore>,
    max: usize,
    /// Set while the cap is reached (one warning per episode).
    saturated: Arc<AtomicBool>,
}

impl ConnLimit {
    /// A pool of `max` connections (at least 1).
    pub fn new(max: usize) -> Self {
        let max = max.clamp(1, Semaphore::MAX_PERMITS);
        Self {
            sem: Arc::new(Semaphore::new(max)),
            max,
            saturated: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The cap.
    pub fn max(&self) -> usize {
        self.max
    }

    /// Connections open now.
    pub fn in_use(&self) -> usize {
        self.max - self.sem.available_permits()
    }

    /// One permit; waits while the cap is reached.
    async fn acquire(&self) -> OwnedSemaphorePermit {
        if let Ok(p) = Arc::clone(&self.sem).try_acquire_owned() {
            self.saturated.store(false, Ordering::Relaxed);
            return p;
        }
        if !self.saturated.swap(true, Ordering::Relaxed) {
            warn!(
                max_connections = self.max,
                "client listener reached [api] max_connections; new connections wait until \
                 one closes"
            );
        }
        // The semaphore is never closed.
        match Arc::clone(&self.sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => std::future::pending().await,
        }
    }
}

/// A connection holding its permit (released when the connection is dropped).
#[derive(Debug)]
pub struct Capped<S> {
    inner: S,
    _permit: OwnedSemaphorePermit,
}

impl<S: AsyncRead + Unpin> AsyncRead for Capped<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Capped<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// A TCP listener that accepts only while a permit is free.
#[derive(Debug)]
pub struct CappedListener {
    inner: TcpListener,
    limit: ConnLimit,
}

impl CappedListener {
    pub fn new(inner: TcpListener, limit: ConnLimit) -> Self {
        Self { inner, limit }
    }

    /// A permit first, then `accept(2)`.
    async fn accept_capped(&self) -> io::Result<(Capped<TcpStream>, SocketAddr)> {
        let permit = self.limit.acquire().await;
        let (s, a) = self.inner.accept().await?;
        Ok((
            Capped {
                inner: s,
                _permit: permit,
            },
            a,
        ))
    }
}

/// Per-connection accept errors (the peer gave up) are retried at once; anything else
/// (EMFILE, ENFILE, ENOBUFS) is logged and retried after a pause, like `axum::serve`.
async fn accept_error(e: io::Error) {
    if matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    ) {
        return;
    }
    warn!(error = %e, "client listener accept failed; retrying in 1 s");
    tokio::time::sleep(Duration::from_secs(1)).await;
}

impl axum::serve::Listener for CappedListener {
    type Io = Capped<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.accept_capped().await {
                Ok(v) => return v,
                Err(e) => accept_error(e).await,
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// The address type of the capped TLS server (axum-server keys its listener on it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CappedAddr(pub SocketAddr);

impl axum_server::Address for CappedAddr {
    type Stream = Capped<TcpStream>;
    type Listener = CappedListener;
}

impl axum_server::AddrListener<Capped<TcpStream>, CappedAddr> for CappedListener {
    fn bind_to(_addr: CappedAddr) -> impl Future<Output = io::Result<Self>> + Send {
        // The server is always built from a bound listener (`Server::from_listener`).
        std::future::ready(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "a capped listener is built from a bound listener",
        )))
    }

    async fn accept_stream(&self) -> io::Result<(Capped<TcpStream>, CappedAddr)> {
        let (s, a) = self.accept_capped().await?;
        Ok((s, CappedAddr(a)))
    }

    fn get_local_addr(&self) -> io::Result<CappedAddr> {
        self.inner.local_addr().map(CappedAddr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_cap_is_half_the_soft_limit_clamped() {
        assert_eq!(auto_for(Some(1024)), 512);
        assert_eq!(auto_for(Some(65_536)), AUTO_MAX);
        assert_eq!(auto_for(Some(64)), AUTO_MIN);
        assert_eq!(auto_for(None), AUTO_MAX);
        assert_eq!(resolve(10), 10);
    }

    #[tokio::test]
    async fn connections_beyond_the_cap_wait_until_one_closes() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let limit = ConnLimit::new(2);
        let listener = Arc::new(CappedListener::new(l, limit.clone()));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let acc = Arc::clone(&listener);
        let task = tokio::spawn(async move {
            loop {
                let (s, _) = acc.accept_capped().await.unwrap();
                if tx.send(s).is_err() {
                    return;
                }
            }
        });
        let _c: Vec<TcpStream> = futures_join(addr, 3).await;
        let a = rx.recv().await.unwrap();
        let _b = rx.recv().await.unwrap();
        assert_eq!(limit.in_use(), 2);
        // The third connection is in the backlog, not accepted.
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err()
        );
        drop(a);
        let _c3 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("accepted once a permit is free")
            .unwrap();
        assert_eq!(limit.in_use(), 2);
        task.abort();
    }

    async fn futures_join(addr: SocketAddr, n: usize) -> Vec<TcpStream> {
        let mut v = Vec::new();
        for _ in 0..n {
            v.push(TcpStream::connect(addr).await.unwrap());
        }
        v
    }
}
