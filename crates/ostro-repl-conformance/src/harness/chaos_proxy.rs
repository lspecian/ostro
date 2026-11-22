//! Tier C (12-09, D-P12-18): a Rust TCP chaos proxy between the nodes of a multi-process
//! cluster. No root, no iptables, no Docker.
//!
//! Every **directed link** `from → to` (the connections node `from` opens to node `to`) has
//! its own listener, so node A's view of B is a port of its own. The proxy copies bytes
//! both ways without looking at them (mTLS passes through unchanged) and applies the
//! link's current rule to every chunk:
//!
//! | rule | effect |
//! |------|--------|
//! | pass | bytes forwarded as read |
//! | `delay(ms)` | each chunk is held `ms` before it is written |
//! | `blackhole` | the proxy stops reading (and holds a chunk already read): the peer's TCP window fills and nothing arrives, no FIN, no RST. New connections are accepted and stall the same way. `heal` resumes delivery of everything held, like a healed network path. |
//! | `cut` | new connections are accepted and immediately reset; live connections are reset (RST on both sides, `SO_LINGER 0`) |
//!
//! **RST passthrough (W2).** When either side resets its connection (a read or write fails
//! with an error), the proxy resets the other side with an RST, not a FIN. A clean EOF is
//! passed on as a FIN (write half shut down), and an upstream that refuses the connection
//! (a `kill -9`'d node's port) makes the proxy reset the accepted client. So a killed peer
//! looks to its clients exactly as it would without the proxy.
//!
//! Byte counters per link (`from → to` direction and back) let tests prove traffic flowed.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

/// Bound on the proxy's upstream connect.
pub const UPSTREAM_CONNECT_T: Duration = Duration::from_secs(5);
/// Copy buffer per direction.
const CHUNK: usize = 64 * 1024;

/// One directed link: connections `from` opens to `to` arrive at `listen` and are forwarded
/// to `upstream` (`to`'s real listener). `listen` may use port 0; see [`ChaosProxy::addr`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    /// The dialing node.
    pub from: u64,
    /// The dialed node.
    pub to: u64,
    /// The proxy listener for this link.
    pub listen: SocketAddr,
    /// The real listener of `to`.
    pub upstream: SocketAddr,
}

/// The current rule of one link.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkRule {
    /// Refuse (reset) new connections and reset live ones.
    pub cut: bool,
    /// Stop forwarding (no FIN, no RST).
    pub blackhole: bool,
    /// Hold each chunk this long before writing it.
    pub delay: Duration,
}

/// Byte and connection counters of one link.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LinkStats {
    /// Bytes forwarded `from → to`.
    pub to_upstream: u64,
    /// Bytes forwarded `to → from`.
    pub to_client: u64,
    /// Connections accepted so far.
    pub accepted: u64,
    /// Connections currently open.
    pub open: usize,
    /// Connections the proxy ended with an RST (cut, or a reset passed through).
    pub resets: u64,
}

#[derive(Default)]
struct Counters {
    up: AtomicU64,
    down: AtomicU64,
    accepted: AtomicU64,
    open: AtomicUsize,
    resets: AtomicU64,
}

struct LinkState {
    link: Link,
    bound: SocketAddr,
    rule: watch::Sender<LinkRule>,
    counters: Arc<Counters>,
}

/// The chaos proxy. Dropping it stops every listener and resets every proxied connection.
pub struct ChaosProxy {
    links: BTreeMap<(u64, u64), LinkState>,
    tasks: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for ChaosProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for ((a, b), s) in &self.links {
            m.entry(
                &format!("{a}->{b}"),
                &(s.bound, s.link.upstream, *s.rule.borrow()),
            );
        }
        m.finish()
    }
}

impl Drop for ChaosProxy {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// End a connection with an RST instead of a FIN.
fn reset(s: TcpStream) {
    let _ = s.set_zero_linger();
    drop(s);
}

impl ChaosProxy {
    /// Bind one listener per link and start forwarding (every link starts healed). Must be
    /// called inside a tokio runtime.
    pub async fn start(links: Vec<Link>) -> io::Result<Self> {
        let mut out = ChaosProxy {
            links: BTreeMap::new(),
            tasks: Vec::new(),
        };
        for link in links {
            if out.links.contains_key(&(link.from, link.to)) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate link {} -> {}", link.from, link.to),
                ));
            }
            let listener = TcpListener::bind(link.listen).await.map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("bind {} ({} -> {}): {e}", link.listen, link.from, link.to),
                )
            })?;
            let bound = listener.local_addr()?;
            let (rule, rx) = watch::channel(LinkRule::default());
            let counters = Arc::new(Counters::default());
            out.tasks.push(tokio::spawn(accept_loop(
                listener,
                link.upstream,
                rx,
                Arc::clone(&counters),
            )));
            out.links.insert(
                (link.from, link.to),
                LinkState {
                    link,
                    bound,
                    rule,
                    counters,
                },
            );
        }
        Ok(out)
    }

    /// Every link, ascending by `(from, to)`.
    pub fn links(&self) -> Vec<Link> {
        self.links.values().map(|s| s.link).collect()
    }

    /// The bound listener of `from → to` (what `from` must dial to reach `to`).
    pub fn addr(&self, from: u64, to: u64) -> SocketAddr {
        self.state(from, to).bound
    }

    /// The current rule of `from → to`.
    pub fn rule(&self, from: u64, to: u64) -> LinkRule {
        *self.state(from, to).rule.borrow()
    }

    /// Counters of `from → to`.
    pub fn stats(&self, from: u64, to: u64) -> LinkStats {
        let c = &self.state(from, to).counters;
        LinkStats {
            to_upstream: c.up.load(Ordering::Relaxed),
            to_client: c.down.load(Ordering::Relaxed),
            accepted: c.accepted.load(Ordering::Relaxed),
            open: c.open.load(Ordering::Relaxed),
            resets: c.resets.load(Ordering::Relaxed),
        }
    }

    fn state(&self, from: u64, to: u64) -> &LinkState {
        self.links
            .get(&(from, to))
            .unwrap_or_else(|| panic!("no proxied link {from} -> {to}"))
    }

    fn modify(&self, from: u64, to: u64, f: impl FnOnce(&mut LinkRule)) {
        self.state(from, to).rule.send_modify(f);
    }

    fn touching(&self, node: u64) -> Vec<(u64, u64)> {
        self.links
            .keys()
            .filter(|(a, b)| *a == node || *b == node)
            .copied()
            .collect()
    }

    /// Refuse new connections on `from → to` and reset the live ones.
    pub fn cut(&self, from: u64, to: u64) {
        self.modify(from, to, |r| r.cut = true);
    }

    /// Stop forwarding on `from → to` (no RST, no FIN; connections stall).
    pub fn blackhole(&self, from: u64, to: u64) {
        self.modify(from, to, |r| r.blackhole = true);
    }

    /// Hold every chunk on `from → to` (both directions) for `ms` before writing it.
    pub fn delay(&self, from: u64, to: u64, ms: u64) {
        self.modify(from, to, |r| r.delay = Duration::from_millis(ms));
    }

    /// Clear every rule of `from → to`.
    pub fn heal(&self, from: u64, to: u64) {
        self.modify(from, to, |r| *r = LinkRule::default());
    }

    /// Cut every link to and from `node`.
    pub fn isolate(&self, node: u64) {
        for (a, b) in self.touching(node) {
            self.cut(a, b);
        }
    }

    /// Clear every rule of every link to and from `node`.
    pub fn heal_node(&self, node: u64) {
        for (a, b) in self.touching(node) {
            self.heal(a, b);
        }
    }

    /// Cut `a → b` and `b → a` (whichever exist).
    pub fn partition(&self, a: u64, b: u64) {
        for (x, y) in [(a, b), (b, a)] {
            if self.links.contains_key(&(x, y)) {
                self.cut(x, y);
            }
        }
    }

    /// Clear every rule of every link.
    pub fn heal_all(&self) {
        for (a, b) in self.links.keys().copied().collect::<Vec<_>>() {
            self.heal(a, b);
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    upstream: SocketAddr,
    rule: watch::Receiver<LinkRule>,
    counters: Arc<Counters>,
) {
    // Owned here: aborting the accept task drops the set, which aborts every connection.
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            acc = listener.accept() => {
                let Ok((client, _)) = acc else {
                    // EMFILE and friends: back off instead of spinning.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                counters.accepted.fetch_add(1, Ordering::Relaxed);
                if rule.borrow().cut {
                    counters.resets.fetch_add(1, Ordering::Relaxed);
                    reset(client);
                    continue;
                }
                conns.spawn(connection(client, upstream, rule.clone(), Arc::clone(&counters)));
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

/// How one direction ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End {
    /// Clean EOF, passed on as a FIN.
    Eof,
    /// A reset (or I/O error, or a cut): reset both sides.
    Reset,
}

struct OpenGuard(Arc<Counters>);

impl Drop for OpenGuard {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn connection(
    mut client: TcpStream,
    upstream: SocketAddr,
    mut rule: watch::Receiver<LinkRule>,
    counters: Arc<Counters>,
) {
    counters.open.fetch_add(1, Ordering::Relaxed);
    let _guard = OpenGuard(Arc::clone(&counters));
    let _ = client.set_nodelay(true);
    // Connect upstream; a cut during the connect, a refused or timed-out connect all reset
    // the client (what it would see dialing a dead node directly).
    let connect = tokio::time::timeout(UPSTREAM_CONNECT_T, TcpStream::connect(upstream));
    let up = tokio::select! {
        r = connect => match r {
            Ok(Ok(s)) => Some(s),
            _ => None,
        },
        _ = wait_cut(&mut rule) => None,
    };
    let Some(mut up) = up else {
        counters.resets.fetch_add(1, Ordering::Relaxed);
        reset(client);
        return;
    };
    let _ = up.set_nodelay(true);
    let end = {
        let (cr, cw) = client.split();
        let (ur, uw) = up.split();
        let a = pump(cr, uw, rule.clone(), &counters.up);
        let b = pump(ur, cw, rule.clone(), &counters.down);
        tokio::pin!(a, b);
        let (mut a_done, mut b_done) = (false, false);
        loop {
            tokio::select! {
                e = &mut a, if !a_done => {
                    if e == End::Reset { break End::Reset; }
                    a_done = true;
                }
                e = &mut b, if !b_done => {
                    if e == End::Reset { break End::Reset; }
                    b_done = true;
                }
            }
            if a_done && b_done {
                break End::Eof;
            }
        }
    };
    if end == End::Reset {
        counters.resets.fetch_add(1, Ordering::Relaxed);
        reset(client);
        reset(up);
    }
}

/// Resolves once the link is cut.
async fn wait_cut(rule: &mut watch::Receiver<LinkRule>) {
    let gone = rule.wait_for(|r| r.cut).await.is_err();
    if gone {
        // The proxy is gone: never resolve (the task is being aborted).
        std::future::pending::<()>().await;
    }
}

/// Wait while blackholed. `Err(())` = the link was cut meanwhile.
async fn pass_gate(rule: &mut watch::Receiver<LinkRule>) -> Result<Duration, ()> {
    let r = rule.wait_for(|r| r.cut || !r.blackhole).await.map(|r| *r);
    match r {
        Ok(r) if r.cut => Err(()),
        Ok(r) => Ok(r.delay),
        Err(_) => {
            std::future::pending::<()>().await;
            Err(())
        }
    }
}

/// Copy `src → dst` under the link rule.
async fn pump(
    mut src: tokio::net::tcp::ReadHalf<'_>,
    mut dst: tokio::net::tcp::WriteHalf<'_>,
    mut rule: watch::Receiver<LinkRule>,
    bytes: &AtomicU64,
) -> End {
    let mut buf = vec![0u8; CHUNK];
    loop {
        // A blackholed link does not read: the sender's window fills, nothing is lost.
        if pass_gate(&mut rule).await.is_err() {
            return End::Reset;
        }
        let n = tokio::select! {
            r = src.read(&mut buf) => match r {
                Ok(0) => {
                    // FIN passthrough.
                    let _ = dst.shutdown().await;
                    return End::Eof;
                }
                Ok(n) => n,
                Err(_) => return End::Reset,
            },
            // A rule change (cut, blackhole) re-evaluates the gate before the next read
            // (TcpStream reads are cancel-safe: nothing is lost).
            ch = rule.changed() => {
                if ch.is_err() {
                    std::future::pending::<()>().await;
                }
                continue;
            }
        };
        // A chunk read just before a blackhole is held until the link heals.
        let delay = match pass_gate(&mut rule).await {
            Ok(d) => d,
            Err(()) => return End::Reset,
        };
        if !delay.is_zero() {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = wait_cut(&mut rule) => return End::Reset,
            }
        }
        tokio::select! {
            r = dst.write_all(&buf[..n]) => if r.is_err() { return End::Reset; },
            _ = wait_cut(&mut rule) => return End::Reset,
        }
        bytes.fetch_add(n as u64, Ordering::Relaxed);
    }
}
