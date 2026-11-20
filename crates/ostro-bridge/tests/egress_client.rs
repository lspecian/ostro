//! The egress client (FX-08 layer 3): env proxies ignored, redirects not followed,
//! connections pinned to vetted addresses, bodies capped. Plain tokio HTTP/1.1 listeners on
//! loopback (the tests set `allow_private = [127.0.0.0/8]` explicitly); no internet.
//!
//! Run single-threaded or not: env mutation is serialised by `ENV`.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ostro_bridge::egress::{Denied, Egress, EgressConfig, ReadError, StaticLookup, read_capped};
use ostro_meta::records::EgressPolicy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Serialises process-environment mutation (proxy variables).
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const MIB: usize = 1024 * 1024;

/// A loopback HTTP/1.1 server; counts accepted connections.
struct Server {
    addr: SocketAddr,
    accepts: Arc<AtomicUsize>,
}

async fn serve() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let count = accepts.clone();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(handle(sock));
        }
    });
    Server { addr, accepts }
}

async fn read_head(sock: &mut TcpStream) -> Option<String> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = sock.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    Some(String::from_utf8_lossy(&buf).into_owned())
}

async fn handle(mut sock: TcpStream) {
    let Some(head) = read_head(&mut sock).await else {
        return;
    };
    let path = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_owned();
    // Echo whether the request carried a Referer or Accept-Encoding header.
    let lower = head.to_ascii_lowercase();
    let flags = format!(
        "referer={} accept-encoding={}",
        lower.contains("\r\nreferer:"),
        lower.contains("\r\naccept-encoding:")
    );
    let _ = match path.as_str() {
        "/ok" => {
            let body = format!("ok {flags}");
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await
        }
        "/redir" => {
            sock.write_all(
                b"HTTP/1.1 302 Found\r\nLocation: /ok\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
        }
        "/small" => {
            let body = vec![b'x'; 1000];
            let mut resp =
                b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n".to_vec();
            resp.extend_from_slice(&body);
            sock.write_all(&resp).await
        }
        "/big-chunked" => big_chunked(&mut sock, 5 * MIB).await,
        "/big-length" => {
            // Announces 5 MiB, sends a little, then waits: the reader must fail on the header.
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\nxxxx",
                5 * MIB
            );
            let r = sock.write_all(resp.as_bytes()).await;
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            r
        }
        _ => {
            sock.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
        }
    };
}

async fn big_chunked(sock: &mut TcpStream, total: usize) -> std::io::Result<()> {
    sock.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        .await?;
    let chunk = vec![b'y'; 64 * 1024];
    let mut sent = 0;
    while sent < total {
        sock.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await?;
        sock.write_all(&chunk).await?;
        sock.write_all(b"\r\n").await?;
        sent += chunk.len();
    }
    sock.write_all(b"0\r\n\r\n").await
}

fn loopback_cfg() -> EgressConfig {
    EgressConfig {
        allow_private: vec!["127.0.0.0/8".parse().unwrap()],
        ..EgressConfig::default()
    }
}

fn policy(port: u16) -> EgressPolicy {
    EgressPolicy {
        name: "acme/client".into(),
        allow: vec![
            format!("http://mock.test:{port}"),
            format!("http://mixed.test:{port}"),
            format!("http://127.0.0.1:{port}"),
        ],
        ext: Vec::new(),
    }
}

fn egress(cfg: EgressConfig) -> Egress {
    let lookup = StaticLookup::new()
        .with("mock.test", &["127.0.0.1"])
        .with("mixed.test", &["127.0.0.1", "10.0.0.1"]);
    Egress::new(cfg, Arc::new(lookup)).unwrap()
}

/// Gate, then send through the guarded client.
async fn get(e: &Egress, port: u16, url: &str) -> reqwest::Result<reqwest::Response> {
    let checked = e.check(Some(&policy(port)), url).expect("gate");
    e.client().get(checked.url).send().await
}

fn chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut cur = err.source();
    while let Some(e) = cur {
        out.push_str(": ");
        out.push_str(&e.to_string());
        cur = e.source();
    }
    out
}

const PROXY_VARS: &[&str] = &[
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "ALL_PROXY",
    "https_proxy",
    "http_proxy",
    "all_proxy",
];

/// Removes the proxy variables on drop (also when the test panics).
struct ProxyEnv;

impl Drop for ProxyEnv {
    fn drop(&mut self) {
        for v in PROXY_VARS {
            // SAFETY: env mutation is serialised by `ENV`, still held while this drops.
            unsafe { std::env::remove_var(v) };
        }
    }
}

#[tokio::test]
async fn env_proxies_are_ignored() {
    let _env = ENV.lock().await;
    let proxy = serve().await;
    let target = serve().await;
    let proxy_url = format!("http://{}", proxy.addr);
    let _restore = ProxyEnv;
    for v in PROXY_VARS {
        // SAFETY: env mutation is serialised by `ENV`; no other thread reads these here.
        unsafe { std::env::set_var(v, &proxy_url) };
    }
    // SAFETY: as above.
    unsafe { std::env::remove_var("NO_PROXY") };
    // SAFETY: as above.
    unsafe { std::env::remove_var("no_proxy") };

    // Both clients are built while the proxy variables are set.
    let e = egress(loopback_cfg());
    let operator = Egress::operator_client(&loopback_cfg()).unwrap();

    let port = target.addr.port();
    let resp = get(&e, port, &format!("http://mock.test:{port}/ok"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.starts_with("ok "), "{body}");
    // No referer, no compression negotiation.
    assert_eq!(body, "ok referer=false accept-encoding=false");

    let resp = operator
        .get(format!("http://127.0.0.1:{port}/ok"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    assert_eq!(
        proxy.accepts.load(Ordering::SeqCst),
        0,
        "the proxy saw a connection"
    );
    assert_eq!(target.accepts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let _env = ENV.lock().await;
    let s = serve().await;
    let port = s.addr.port();
    let e = egress(loopback_cfg());
    let resp = get(&e, port, &format!("http://mock.test:{port}/redir"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 302);
    assert_eq!(resp.headers()["location"], "/ok");
    let operator = Egress::operator_client(&loopback_cfg()).unwrap();
    let resp = operator
        .get(format!("http://127.0.0.1:{port}/redir"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 302);
    assert_eq!(s.accepts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn connections_pin_to_vetted_addresses() {
    let _env = ENV.lock().await;
    let s = serve().await;
    let port = s.addr.port();
    let e = egress(loopback_cfg());

    let resp = get(&e, port, &format!("http://mock.test:{port}/ok"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(s.accepts.load(Ordering::SeqCst), 1);

    // One blocked answer fails the whole name before any connection is made.
    let err = get(&e, port, &format!("http://mixed.test:{port}/ok"))
        .await
        .unwrap_err();
    let msg = chain(&err);
    assert!(
        msg.contains("egress_denied") && msg.contains("reason=blocked"),
        "{msg}"
    );
    assert_eq!(
        Denied::from_error(&err),
        Some(Denied::Blocked("10.0.0.1".parse().unwrap()))
    );
    assert_eq!(
        s.accepts.load(Ordering::SeqCst),
        1,
        "no connection for a denied name"
    );

    // The default config blocks loopback at the resolver too.
    let closed = egress(EgressConfig::default());
    let err = closed
        .client()
        .get(format!("http://mock.test:{port}/ok"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        Denied::from_error(&err).map(|d| d.reason()),
        Some("blocked")
    );
    assert_eq!(s.accepts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn bodies_are_capped() {
    let _env = ENV.lock().await;
    let s = serve().await;
    let port = s.addr.port();
    let e = egress(loopback_cfg());
    let cap = 4 * MIB as u64;

    let resp = get(&e, port, &format!("http://mock.test:{port}/small"))
        .await
        .unwrap();
    let body = read_capped(resp, cap).await.unwrap();
    assert_eq!(body.len(), 1000);
    assert!(body.iter().all(|b| *b == b'x'));

    let resp = get(&e, port, &format!("http://mock.test:{port}/big-chunked"))
        .await
        .unwrap();
    match read_capped(resp, cap).await {
        Err(ReadError::TooLarge(t)) => {
            assert_eq!(t.cap, cap);
            assert!(t.seen > cap, "{t:?}");
            // At most one chunk past the cap is read.
            assert!(t.seen <= cap + MIB as u64, "{t:?}");
        }
        other => panic!("expected TooLarge, got {other:?}"),
    }

    let resp = get(&e, port, &format!("http://mock.test:{port}/big-length"))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    match read_capped(resp, cap).await {
        Err(ReadError::TooLarge(t)) => assert_eq!(t.seen, 5 * MIB as u64),
        other => panic!("expected TooLarge, got {other:?}"),
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "read before failing"
    );

    // A body exactly at the cap passes.
    let resp = get(&e, port, &format!("http://mock.test:{port}/small"))
        .await
        .unwrap();
    assert_eq!(read_capped(resp, 1000).await.unwrap().len(), 1000);
    let resp = get(&e, port, &format!("http://mock.test:{port}/small"))
        .await
        .unwrap();
    assert!(matches!(
        read_capped(resp, 999).await,
        Err(ReadError::TooLarge(_))
    ));
}

/// `Client::builder` appears only in `egress/client.rs`; `Client::new` nowhere (P-5,
/// PITFALLS #16).
#[test]
fn client_builder_only_in_egress_client() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    walk(&src, &mut files);
    assert!(files.len() > 10, "walked {}", src.display());
    let mut builder_sites = Vec::new();
    for f in &files {
        let text = std::fs::read_to_string(f).unwrap();
        let rel = f
            .strip_prefix(&src)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        assert!(!text.contains("Client::new"), "{rel} calls Client::new");
        if text.contains("Client::builder") || text.contains("ClientBuilder::new") {
            builder_sites.push(rel);
        }
    }
    assert_eq!(builder_sites, vec!["egress/client.rs".to_owned()]);
}

fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let p = entry.unwrap().path();
        if p.is_dir() {
            walk(&p, out);
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
}
