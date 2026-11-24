//! 12-09 Task 1: the tier-C TCP chaos proxy. Pass-through is byte-identical (5 MiB burst,
//! mTLS handshake + HTTP/2), `cut` resets live connections within 100 ms and refuses new
//! ones, an upstream RST reaches the client as a reset (not a clean close) while a clean
//! close stays a FIN, `delay` adds latency, `blackhole` stalls without losing bytes, and
//! `heal` / `isolate` / `partition` / `heal_all` act on the right directed links.

use std::convert::Infallible;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ostro_repl_conformance::harness::certs::TestPki;
use ostro_repl_conformance::harness::chaos_proxy::{ChaosProxy, Link, LinkRule};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::body::Body;
use tonic::codegen::http;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

const LOCAL: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);

/// An echo server; returns its address.
async fn echo_server() -> SocketAddr {
    let l = TcpListener::bind(LOCAL).await.expect("bind echo");
    let addr = l.local_addr().expect("addr");
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = s.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
                let _ = w.shutdown().await;
            });
        }
    });
    addr
}

async fn proxy_to(upstream: SocketAddr) -> ChaosProxy {
    ChaosProxy::start(vec![Link {
        from: 1,
        to: 2,
        listen: LOCAL,
        upstream,
    }])
    .await
    .expect("proxy")
}

async fn roundtrip(s: &mut TcpStream, msg: &[u8]) -> std::io::Result<Vec<u8>> {
    s.write_all(msg).await?;
    let mut got = vec![0u8; msg.len()];
    s.read_exact(&mut got).await?;
    Ok(got)
}

fn is_reset(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted | ErrorKind::BrokenPipe
    )
}

/// Read until an error or EOF (bounded): `Err(kind)` or `Ok(())` for a clean EOF.
async fn read_end(s: &mut TcpStream, limit: Duration) -> Result<(), ErrorKind> {
    let mut buf = [0u8; 4096];
    let r = tokio::time::timeout(limit, async {
        loop {
            match s.read(&mut buf).await {
                Ok(0) => return Ok(()),
                Ok(_) => continue,
                Err(e) => return Err(e.kind()),
            }
        }
    })
    .await;
    r.unwrap_or(Err(ErrorKind::TimedOut))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passthrough_is_byte_identical_including_a_5mib_burst() {
    let p = proxy_to(echo_server().await).await;
    let mut s = TcpStream::connect(p.addr(1, 2)).await.expect("connect");
    assert_eq!(roundtrip(&mut s, b"hello").await.expect("rt"), b"hello");

    let burst: Vec<u8> = (0..5 * 1024 * 1024u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let (mut r, mut w) = s.into_split();
    let send = burst.clone();
    let writer = tokio::spawn(async move {
        w.write_all(&send).await.expect("write burst");
        w.shutdown().await.expect("fin");
    });
    let mut got = Vec::with_capacity(burst.len());
    r.read_to_end(&mut got).await.expect("read burst");
    writer.await.expect("writer");
    assert_eq!(got.len(), burst.len());
    assert!(got == burst, "the 5 MiB burst came back altered");
    // The client's FIN went through and the echo's FIN came back (read_to_end ended).
    let st = p.stats(1, 2);
    assert_eq!(st.to_upstream, 5 + burst.len() as u64, "{st:?}");
    assert_eq!(st.to_client, 5 + burst.len() as u64, "{st:?}");
    assert_eq!(st.accepted, 1);
    assert_eq!(st.resets, 0, "a clean close is not a reset");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cut_resets_an_open_connection_quickly_and_refuses_new_ones() {
    let p = proxy_to(echo_server().await).await;
    let mut s = TcpStream::connect(p.addr(1, 2)).await.expect("connect");
    assert_eq!(roundtrip(&mut s, b"ping").await.expect("rt"), b"ping");

    let t0 = Instant::now();
    p.cut(1, 2);
    let end = read_end(&mut s, Duration::from_secs(2)).await;
    let took = t0.elapsed();
    assert_eq!(end, Err(ErrorKind::ConnectionReset), "an RST, not a FIN");
    assert!(took < Duration::from_millis(100), "reset took {took:?}");
    assert!(p.rule(1, 2).cut);

    // New connections are reset right after the accept.
    for _ in 0..3 {
        match TcpStream::connect(p.addr(1, 2)).await {
            Ok(mut s2) => {
                let r = roundtrip(&mut s2, b"x").await;
                assert!(r.is_err(), "a cut link must not carry bytes: {r:?}");
            }
            Err(e) => assert!(
                is_reset(&e) || e.kind() == ErrorKind::ConnectionRefused,
                "{e}"
            ),
        }
    }

    p.heal(1, 2);
    assert_eq!(p.rule(1, 2), LinkRule::default());
    let mut s3 = TcpStream::connect(p.addr(1, 2))
        .await
        .expect("connect after heal");
    assert_eq!(roundtrip(&mut s3, b"back").await.expect("rt"), b"back");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upstream_rst_reaches_the_client_as_a_reset_and_a_fin_as_a_fin() {
    // Upstream: the first connection is reset (SO_LINGER 0), the second closed cleanly.
    let l = TcpListener::bind(LOCAL).await.expect("bind");
    let up = l.local_addr().expect("addr");
    tokio::spawn(async move {
        let mut n = 0;
        while let Ok((mut s, _)) = l.accept().await {
            let mut b = [0u8; 1];
            let _ = s.read_exact(&mut b).await;
            if n == 0 {
                s.set_zero_linger().expect("linger");
            }
            drop(s);
            n += 1;
        }
    });
    let p = proxy_to(up).await;

    let mut s = TcpStream::connect(p.addr(1, 2)).await.expect("connect");
    s.write_all(b"a").await.expect("write");
    assert_eq!(
        read_end(&mut s, Duration::from_secs(5)).await,
        Err(ErrorKind::ConnectionReset),
        "an upstream reset must not look like a clean close"
    );

    let mut s = TcpStream::connect(p.addr(1, 2)).await.expect("connect");
    s.write_all(b"b").await.expect("write");
    assert_eq!(
        read_end(&mut s, Duration::from_secs(5)).await,
        Ok(()),
        "an upstream FIN stays a clean close"
    );
    assert!(p.stats(1, 2).resets >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_upstream_resets_the_client() {
    // What a kill -9'd node's port looks like: nothing listens, the connect is refused.
    // The port stays bound (no `listen`) for the whole test: a bind-then-drop port can be
    // handed to a listener of a concurrently running test, whose clean close then reached
    // the client as `Ok(())` (seen on linux-aarch64-musl CI).
    let dead_sock = tokio::net::TcpSocket::new_v4().expect("socket");
    dead_sock.bind(LOCAL).expect("bind");
    let dead = dead_sock.local_addr().expect("addr");
    let p = proxy_to(dead).await;
    // The refused upstream connect is immediate, so the proxy's RST can reach the client
    // before (or during) its connect/write. Linux reports a reset exactly once: to whichever
    // call sees it first, and the socket then reads as a clean EOF. So the reset must surface
    // in the first call that observes it (connect, write or read), and an error swallowed at
    // the write is not allowed to turn into `Ok(())` at the read (seen on linux-*-musl CI).
    let end = async {
        let mut s = TcpStream::connect(p.addr(1, 2))
            .await
            .map_err(|e| e.kind())?;
        s.write_all(b"z").await.map_err(|e| e.kind())?;
        read_end(&mut s, Duration::from_secs(10)).await
    }
    .await;
    assert_eq!(end, Err(ErrorKind::ConnectionReset));
    drop(dead_sock);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delay_adds_at_least_the_configured_latency() {
    let p = proxy_to(echo_server().await).await;
    let mut s = TcpStream::connect(p.addr(1, 2)).await.expect("connect");
    let t0 = Instant::now();
    roundtrip(&mut s, b"warm").await.expect("rt");
    let base = t0.elapsed();

    p.delay(1, 2, 150);
    let t0 = Instant::now();
    assert_eq!(roundtrip(&mut s, b"slow").await.expect("rt"), b"slow");
    let slow = t0.elapsed();
    // Each direction holds the chunk 150 ms.
    assert!(
        slow >= Duration::from_millis(300),
        "delayed rtt {slow:?} (base {base:?})"
    );

    p.heal(1, 2);
    let t0 = Instant::now();
    roundtrip(&mut s, b"fast").await.expect("rt");
    assert!(
        t0.elapsed() < Duration::from_millis(150),
        "{:?}",
        t0.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blackhole_stalls_without_rst_and_heal_delivers_the_held_bytes() {
    let p = proxy_to(echo_server().await).await;
    let mut s = TcpStream::connect(p.addr(1, 2)).await.expect("connect");
    assert_eq!(roundtrip(&mut s, b"one").await.expect("rt"), b"one");

    p.blackhole(1, 2);
    s.write_all(b"two").await.expect("write into the blackhole");
    let mut got = [0u8; 3];
    let r = tokio::time::timeout(Duration::from_millis(400), s.read_exact(&mut got)).await;
    assert!(
        r.is_err(),
        "the client must stall until its own timeout: {r:?}"
    );

    // A new connection is accepted and stalls the same way (no reset).
    let mut s2 = TcpStream::connect(p.addr(1, 2))
        .await
        .expect("connect while blackholed");
    s2.write_all(b"new").await.expect("write");
    let mut g2 = [0u8; 3];
    let r2 = tokio::time::timeout(Duration::from_millis(300), s2.read_exact(&mut g2)).await;
    assert!(r2.is_err(), "{r2:?}");

    p.heal(1, 2);
    tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut got))
        .await
        .expect("delivered after heal")
        .expect("read");
    assert_eq!(&got, b"two", "nothing is lost in a blackhole");
    tokio::time::timeout(Duration::from_secs(5), s2.read_exact(&mut g2))
        .await
        .expect("delivered after heal")
        .expect("read");
    assert_eq!(&g2, b"new");
    assert_eq!(p.stats(1, 2).resets, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_level_rules_hit_exactly_the_links_of_that_node() {
    let echo = echo_server().await;
    let mut links = Vec::new();
    for from in 1..=3u64 {
        for to in 1..=3u64 {
            if from != to {
                links.push(Link {
                    from,
                    to,
                    listen: LOCAL,
                    upstream: echo,
                });
            }
        }
    }
    let p = ChaosProxy::start(links).await.expect("proxy");
    assert_eq!(p.links().len(), 6);

    p.isolate(3);
    for (a, b) in [(1, 3), (3, 1), (2, 3), (3, 2)] {
        assert!(p.rule(a, b).cut, "{a}->{b}");
    }
    for (a, b) in [(1, 2), (2, 1)] {
        assert!(!p.rule(a, b).cut, "{a}->{b}");
        let mut s = TcpStream::connect(p.addr(a, b)).await.expect("connect");
        assert_eq!(roundtrip(&mut s, b"ok").await.expect("rt"), b"ok");
    }
    p.heal_node(3);
    assert!((1..=3).all(|a| (1..=3).filter(|b| *b != a).all(|b| !p.rule(a, b).cut)));

    p.partition(1, 2);
    assert!(p.rule(1, 2).cut && p.rule(2, 1).cut);
    assert!(!p.rule(1, 3).cut && !p.rule(3, 2).cut);
    p.delay(2, 3, 10);
    p.heal_all();
    for l in p.links() {
        assert_eq!(p.rule(l.from, l.to), LinkRule::default(), "{l:?}");
    }
    // Every listener is distinct: A's view of B is its own port.
    let mut addrs: Vec<_> = p.links().iter().map(|l| p.addr(l.from, l.to)).collect();
    addrs.sort();
    addrs.dedup();
    assert_eq!(addrs.len(), 6);
}

/// A minimal HTTP/2 service: every request gets 200 with `grpc-status: 0` and an
/// `x-echo-path` header.
#[derive(Clone)]
struct PathEcho;

impl tower::Service<http::Request<Body>> for PathEcho {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let mut resp = http::Response::new(Body::empty());
        resp.headers_mut().insert(
            "x-echo-path",
            http::HeaderValue::from_str(req.uri().path()).expect("header"),
        );
        resp.headers_mut().insert(
            "content-type",
            http::HeaderValue::from_static("application/grpc"),
        );
        resp.headers_mut()
            .insert("grpc-status", http::HeaderValue::from_static("0"));
        std::future::ready(Ok(resp))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_mtls_handshake_and_http2_pass_through_unchanged() {
    let pki = TestPki::new("chaos-tls").expect("pki");
    let server = pki.node_cert(2, &[]).expect("server cert");
    let client = pki.node_cert(1, &[]).expect("client cert");

    let l = TcpListener::bind(LOCAL).await.expect("bind");
    let up = l.local_addr().expect("addr");
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(&server.cert_pem, &server.key_pem))
        .client_ca_root(Certificate::from_pem(pki.ca_cert_pem()));
    let srv = Server::builder().tls_config(tls).expect("server tls");
    tokio::spawn(async move {
        let _ = srv
            .serve_with_incoming(PathEcho, tokio_stream::wrappers::TcpListenerStream::new(l))
            .await;
    });

    let p = proxy_to(up).await;
    let ep = Endpoint::from_shared(format!("https://{}", p.addr(1, 2)))
        .expect("endpoint")
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(pki.ca_cert_pem()))
                .identity(Identity::from_pem(&client.cert_pem, &client.key_pem)),
        )
        .expect("client tls")
        .connect_timeout(Duration::from_secs(5));
    let mut ch = ep.connect().await.expect("TLS connect through the proxy");
    for path in ["/ostro.test.Echo/One", "/ostro.test.Echo/Two"] {
        let req = http::Request::builder()
            .method("POST")
            .uri(format!("https://{}{path}", p.addr(1, 2)))
            .header("content-type", "application/grpc")
            .body(Body::empty())
            .expect("request");
        use tower::{Service as _, ServiceExt as _};
        let resp = ch
            .ready()
            .await
            .expect("ready")
            .call(req)
            .await
            .expect("call");
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.headers()["x-echo-path"], path);
    }
    let st = p.stats(1, 2);
    assert!(st.to_upstream > 500 && st.to_client > 500, "{st:?}");
    assert_eq!(st.accepted, 1, "one TLS connection carried both calls");

    // A cut kills the TLS connection; the channel fails instead of hanging.
    p.cut(1, 2);
    let req = http::Request::builder()
        .method("POST")
        .uri(format!("https://{}/ostro.test.Echo/Three", p.addr(1, 2)))
        .body(Body::empty())
        .expect("request");
    use tower::{Service as _, ServiceExt as _};
    let r = tokio::time::timeout(Duration::from_secs(10), async {
        match ch.ready().await {
            Ok(svc) => svc.call(req).await.map(|_| ()),
            Err(e) => Err(e),
        }
    })
    .await
    .expect("a cut link fails fast, it does not hang");
    assert!(r.is_err(), "a call over a cut link must fail");
}
