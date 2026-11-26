//! Broker connections (14-31): TCP or TLS sockets, length-prefixed framing, correlation ids,
//! ApiVersions negotiation and SASL on every new connection, and reconnection with jittered
//! backoff.
//!
//! - [`Conn`]: one connected, negotiated (and authenticated) socket. A single task per
//!   connection owns the socket: it writes request frames and reads response frames,
//!   matching them to waiters by correlation id (requests may be pipelined). Frames are
//!   `i32` size + payload; a size above [`MAX_FRAME`] (or the per-API cap of
//!   [`ApiRequest::MAX_RESPONSE`]), a negative size or an unknown correlation id closes the
//!   connection with [`KafkaError::Protocol`] before anything is allocated for the frame.
//!   Writes and each request's wait are bounded by `request_timeout`; a request that times out
//!   closes the connection (the next request reconnects).
//! - [`Broker`]: a reconnecting handle to one `host:port`. A closed connection is replaced on
//!   the next request; failed connection attempts are spaced by [`Backoff`] (100 ms doubling to
//!   10 s, jittered to `[d/2, d]`, injectable RNG).
//! - [`WireCheck`]: a bounds-checked walker over response bytes. `kafka-protocol` pre-allocates
//!   arrays from the wire count, so every response is walked first ([`ApiRequest::check_response`])
//!   and counts that cannot fit in the remaining bytes are rejected as `Protocol` errors: a
//!   malformed frame is an error, never a panic or an unbounded allocation.
//! - TLS: tokio-rustls on aws-lc-rs; roots = `rustls-native-certs` plus the optional
//!   `ca_file` PEM bundle; SNI / certificate name = `server_name` or the bootstrap host.
//!
//! Credentials never appear in `Debug` output, logs or errors (see [`crate::sasl`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiVersionsRequest, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::AbortHandle;

use crate::versions::{self, ApiRange, ApiRequest, ApiVersionsOutcome, BrokerVersions};
use crate::{ClientConfig, KafkaError, SaslConfig, TlsConfig};

/// Largest frame the client accepts (64 MiB, after the `i32` size prefix).
pub const MAX_FRAME: usize = 64 << 20;

/// First reconnect delay.
pub const BACKOFF_BASE: Duration = Duration::from_millis(100);
/// Reconnect delay cap.
pub const BACKOFF_MAX: Duration = Duration::from_secs(10);

/// The software name sent in ApiVersions v3.
const SOFTWARE_NAME: &str = "ostro-kafka";

// ------------------------------------------------------------------------------ WireCheck

/// A bounds-checked walker over Kafka wire bytes (legacy and flexible encodings).
///
/// Every read checks the remaining length; [`WireCheck::array_len`] additionally checks that
/// `count * min_elem` fits in what is left, which bounds what `kafka-protocol` will
/// pre-allocate when it decodes the same bytes.
#[derive(Debug)]
pub struct WireCheck<'a> {
    what: &'static str,
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireCheck<'a> {
    /// A walker over `buf`; `what` names the message in errors.
    pub fn new(what: &'static str, buf: &'a [u8]) -> Self {
        WireCheck { what, buf, pos: 0 }
    }

    fn err(&self, msg: impl std::fmt::Display) -> KafkaError {
        KafkaError::Protocol(format!(
            "malformed {} response at byte {}: {msg}",
            self.what, self.pos
        ))
    }

    /// Bytes left.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], KafkaError> {
        if n > self.remaining() {
            return Err(self.err(format_args!("needs {n} bytes, {} left", self.remaining())));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Skip `n` bytes.
    pub fn skip(&mut self, n: usize) -> Result<(), KafkaError> {
        self.take(n).map(|_| ())
    }

    /// An `int8`.
    pub fn i8(&mut self) -> Result<i8, KafkaError> {
        Ok(self.take(1)?[0] as i8)
    }

    /// An `int16`.
    pub fn i16(&mut self) -> Result<i16, KafkaError> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    /// An `int32`.
    pub fn i32(&mut self) -> Result<i32, KafkaError> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// An `int64`.
    pub fn i64(&mut self) -> Result<i64, KafkaError> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(i64::from_be_bytes(a))
    }

    /// An unsigned varint (at most 5 bytes).
    pub fn uvarint(&mut self) -> Result<u32, KafkaError> {
        let mut v: u32 = 0;
        for i in 0..5 {
            let b = self.take(1)?[0];
            v |= u32::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(self.err("varint longer than 5 bytes"))
    }

    /// A (nullable) string: `int16` length (legacy) or unsigned varint length + 1 (`flex`).
    pub fn string(&mut self, flex: bool) -> Result<(), KafkaError> {
        let n = if flex {
            (self.uvarint()? as usize).saturating_sub(1)
        } else {
            self.i16()?.max(0) as usize
        };
        self.skip(n)
    }

    /// (Nullable) bytes: `int32` length (legacy) or unsigned varint length + 1 (`flex`).
    pub fn bytes(&mut self, flex: bool) -> Result<(), KafkaError> {
        let n = if flex {
            (self.uvarint()? as usize).saturating_sub(1)
        } else {
            self.i32()?.max(0) as usize
        };
        self.skip(n)
    }

    /// An array count (`int32` legacy, unsigned varint + 1 `flex`; null = 0), checked against
    /// the remaining bytes with `min_elem` = the smallest possible encoding of one element.
    pub fn array_len(&mut self, flex: bool, min_elem: usize) -> Result<usize, KafkaError> {
        let n: usize = if flex {
            (self.uvarint()? as usize).saturating_sub(1)
        } else {
            let n = self.i32()?;
            if n < -1 {
                return Err(self.err(format_args!("negative array length {n}")));
            }
            n.max(0) as usize
        };
        if n.saturating_mul(min_elem.max(1)) > self.remaining() {
            return Err(self.err(format_args!(
                "array of {n} elements cannot fit in {} bytes",
                self.remaining()
            )));
        }
        Ok(n)
    }

    /// Skip a tagged-field section (flexible versions).
    pub fn tagged_fields(&mut self) -> Result<(), KafkaError> {
        self.tagged_fields_with(|_, _| Ok(()))
    }

    /// Walk a tagged-field section, handing each field's bytes to `f` as its own walker.
    pub fn tagged_fields_with(
        &mut self,
        mut f: impl FnMut(u32, &mut WireCheck<'a>) -> Result<(), KafkaError>,
    ) -> Result<(), KafkaError> {
        let n = self.uvarint()? as usize;
        if n.saturating_mul(2) > self.remaining() {
            return Err(self.err(format_args!("{n} tagged fields cannot fit")));
        }
        for _ in 0..n {
            let tag = self.uvarint()?;
            let size = self.uvarint()? as usize;
            let field = self.take(size)?;
            let mut sub = WireCheck::new(self.what, field);
            f(tag, &mut sub)?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------------------ backoff

/// Jittered exponential reconnect backoff: the n-th delay is uniform in `[d/2, d]` with
/// `d = min(max, base * 2^n)`.
pub struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
    rng: Box<dyn FnMut() -> u64 + Send>,
}

impl std::fmt::Debug for Backoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backoff")
            .field("base", &self.base)
            .field("max", &self.max)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

impl Default for Backoff {
    fn default() -> Self {
        let mut seed = [0u8; 8];
        // A failed system RNG only weakens the jitter; fall back to the address of a local.
        if aws_lc_rs::rand::fill(&mut seed).is_err() {
            seed = (&seed as *const _ as usize as u64).to_le_bytes();
        }
        let mut state = u64::from_le_bytes(seed);
        Backoff::with_rng(BACKOFF_BASE, BACKOFF_MAX, move || splitmix64(&mut state))
    }
}

impl Backoff {
    /// A backoff with an injected RNG (tests use a fixed sequence).
    pub fn with_rng(
        base: Duration,
        max: Duration,
        rng: impl FnMut() -> u64 + Send + 'static,
    ) -> Self {
        Backoff {
            base,
            max,
            attempt: 0,
            rng: Box::new(rng),
        }
    }

    /// The next delay (and advance).
    pub fn next_delay(&mut self) -> Duration {
        let cap = self
            .base
            .saturating_mul(1u32 << self.attempt.min(20))
            .min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        let cap_ns = cap.as_nanos() as u64;
        let half = cap_ns / 2;
        let span = cap_ns - half;
        Duration::from_nanos(half + (self.rng)() % (span + 1))
    }

    /// Back to the first delay (after a successful connection).
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

fn splitmix64(s: &mut u64) -> u64 {
    *s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *s;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// ------------------------------------------------------------------------------ options / TLS

/// Everything needed to open a connection, built once per client ([`ConnectOptions::new`]
/// loads the TLS roots).
#[derive(Clone)]
pub struct ConnectOptions {
    client_id: String,
    tls: Option<(tokio_rustls::TlsConnector, Option<String>)>,
    sasl: Option<SaslConfig>,
    request_timeout: Duration,
    max_frame: usize,
}

impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("client_id", &self.client_id)
            .field("tls", &self.tls.as_ref().map(|(_, name)| name))
            .field("sasl", &self.sasl)
            .field("request_timeout", &self.request_timeout)
            .field("max_frame", &self.max_frame)
            .finish()
    }
}

impl ConnectOptions {
    /// From a client configuration (reads the native roots and `ca_file` when TLS is on).
    pub fn new(config: &ClientConfig) -> Result<Self, KafkaError> {
        let tls = match &config.tls {
            Some(t) => Some((
                tokio_rustls::TlsConnector::from(tls_client_config(t)?),
                t.server_name.clone(),
            )),
            None => None,
        };
        Ok(ConnectOptions {
            client_id: config.client_id.clone(),
            tls,
            sasl: config.sasl.clone(),
            request_timeout: config.request_timeout,
            max_frame: MAX_FRAME,
        })
    }

    /// Lower the frame limit (tests).
    pub fn with_max_frame(mut self, max_frame: usize) -> Self {
        self.max_frame = max_frame.min(MAX_FRAME);
        self
    }

    /// The per-request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

/// The rustls client configuration: native roots + the optional PEM `ca_file`, aws-lc-rs.
pub fn tls_client_config(cfg: &TlsConfig) -> Result<Arc<rustls::ClientConfig>, KafkaError> {
    let mut roots = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    if !native.errors.is_empty() {
        tracing::debug!(
            errors = native.errors.len(),
            "kafka: some native roots failed to load"
        );
    }
    let (_added, _ignored) = roots.add_parsable_certificates(native.certs);
    if let Some(path) = &cfg.ca_file {
        let pem = std::fs::read(path)
            .map_err(|e| KafkaError::Tls(format!("reading ca_file {}: {e}", path.display())))?;
        let mut n = 0usize;
        for cert in CertificateDer::pem_slice_iter(&pem) {
            let cert = cert
                .map_err(|e| KafkaError::Tls(format!("parsing ca_file {}: {e}", path.display())))?;
            roots.add(cert).map_err(|e| {
                KafkaError::Tls(format!("ca_file {} certificate: {e}", path.display()))
            })?;
            n += 1;
        }
        if n == 0 {
            return Err(KafkaError::Tls(format!(
                "ca_file {} holds no PEM certificate",
                path.display()
            )));
        }
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| KafkaError::Tls(format!("TLS configuration: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// `host:port` (IPv6 hosts in brackets) → `(host, port)`.
pub fn split_host_port(addr: &str) -> Result<(String, u16), KafkaError> {
    let bad = || KafkaError::Protocol(format!("broker address {addr:?} is not host:port"));
    let (host, port) = addr.rsplit_once(':').ok_or_else(bad)?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(bad());
    }
    let port: u16 = port.parse().map_err(|_| bad())?;
    Ok((host.to_owned(), port))
}

// ------------------------------------------------------------------------------ connection

/// Why a connection failed; turned into a fresh [`KafkaError`] for every waiter.
#[derive(Clone, Debug)]
enum Failure {
    Io(std::io::ErrorKind, String),
    Protocol(String),
    Timeout,
}

impl Failure {
    fn error(&self) -> KafkaError {
        match self {
            Failure::Io(kind, msg) => KafkaError::Io(std::io::Error::new(*kind, msg.clone())),
            Failure::Protocol(msg) => KafkaError::Protocol(msg.clone()),
            Failure::Timeout => KafkaError::Timeout,
        }
    }
}

struct Pending {
    max: usize,
    reply: oneshot::Sender<Result<Bytes, KafkaError>>,
}

struct Cmd {
    corr: i32,
    frame: Bytes,
    max: usize,
    reply: oneshot::Sender<Result<Bytes, KafkaError>>,
}

/// One connected, negotiated (and, with SASL, authenticated) broker connection.
pub struct Conn {
    addr: String,
    tx: mpsc::Sender<Cmd>,
    task: AbortHandle,
    closed: AtomicBool,
    corr: AtomicI32,
    client_id: StrBytes,
    timeout: Duration,
    versions: BrokerVersions,
}

impl std::fmt::Debug for Conn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Conn")
            .field("addr", &self.addr)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Conn {
    /// Connect to `addr` (`host:port`): TCP, TLS when configured, ApiVersions, SASL when
    /// configured. Bounded by the request timeout per step.
    pub async fn open(addr: &str, opts: &ConnectOptions) -> Result<Conn, KafkaError> {
        let (host, port) = split_host_port(addr)?;
        let tcp = tokio::time::timeout(
            opts.request_timeout,
            tokio::net::TcpStream::connect((host.as_str(), port)),
        )
        .await
        .map_err(|_| KafkaError::Timeout)??;
        tcp.set_nodelay(true)?;
        let mut conn = match &opts.tls {
            None => Conn::spawn(addr, tcp, opts),
            Some((connector, name)) => {
                let name = name.clone().unwrap_or_else(|| host.clone());
                let server_name = ServerName::try_from(name.clone())
                    .map_err(|e| KafkaError::Tls(format!("server name {name:?}: {e}")))?;
                let tls =
                    tokio::time::timeout(opts.request_timeout, connector.connect(server_name, tcp))
                        .await
                        .map_err(|_| KafkaError::Timeout)?
                        .map_err(|e| {
                            KafkaError::Tls(format!("handshake with {addr} as {name:?}: {e}"))
                        })?;
                Conn::spawn(addr, tls, opts)
            }
        };
        conn.versions = conn.negotiate().await?;
        if let Some(sasl) = &opts.sasl {
            crate::sasl::authenticate(&conn, sasl).await?;
        }
        tracing::debug!(
            addr,
            tls = opts.tls.is_some(),
            sasl = opts.sasl.is_some(),
            "kafka: connected"
        );
        Ok(conn)
    }

    fn spawn<S>(addr: &str, stream: S, opts: &ConnectOptions) -> Conn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(64);
        let handle = tokio::spawn(run(stream, rx, opts.max_frame, opts.request_timeout));
        Conn {
            addr: addr.to_owned(),
            tx,
            task: handle.abort_handle(),
            closed: AtomicBool::new(false),
            corr: AtomicI32::new(1),
            client_id: StrBytes::from_string(opts.client_id.clone()),
            timeout: opts.request_timeout,
            versions: BrokerVersions::default(),
        }
    }

    /// ApiVersions at v3, then the fallback version the broker names.
    async fn negotiate(&self) -> Result<BrokerVersions, KafkaError> {
        let mut v = versions::API_VERSIONS.max;
        for _ in 0..3 {
            let req = ApiVersionsRequest::default()
                .with_client_software_name(StrBytes::from_static_str(SOFTWARE_NAME))
                .with_client_software_version(StrBytes::from_static_str(env!("CARGO_PKG_VERSION")));
            let body = self.call(&req, v).await?;
            match versions::decode_api_versions(body, v)? {
                ApiVersionsOutcome::Ok(r) => return Ok(BrokerVersions::from_response(&r)),
                ApiVersionsOutcome::Retry(next) => {
                    tracing::debug!(addr = %self.addr, from = v, to = next, "kafka: ApiVersions fallback");
                    v = next;
                }
            }
        }
        Err(KafkaError::Protocol(
            "ApiVersions negotiation did not converge".into(),
        ))
    }

    /// The broker address.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The broker's advertised API ranges.
    pub fn versions(&self) -> &BrokerVersions {
        &self.versions
    }

    /// The negotiated version of `api` on this connection.
    pub fn version(&self, api: &ApiRange) -> Result<i16, KafkaError> {
        self.versions.version(api)
    }

    /// The connection's task has ended (socket closed, protocol error or timeout).
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.tx.is_closed()
    }

    /// Close the connection.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.task.abort();
    }

    /// Send `req` at the negotiated version and decode the response.
    pub async fn request<R: ApiRequest>(&self, req: &R) -> Result<R::Response, KafkaError> {
        let v = self.version(&R::API)?;
        self.request_at(req, v).await
    }

    /// Send `req` at `version` and decode the response.
    pub async fn request_at<R: ApiRequest>(
        &self,
        req: &R,
        version: i16,
    ) -> Result<R::Response, KafkaError> {
        let body = self.call(req, version).await?;
        R::check_response(&body, version)?;
        R::Response::decode(&mut body.clone(), version)
            .map_err(|e| KafkaError::Protocol(format!("{} v{version} response: {e}", R::API.name)))
    }

    /// Frame, send and await one response; returns the body after the response header.
    async fn call<R: ApiRequest>(&self, req: &R, version: i16) -> Result<Bytes, KafkaError> {
        let api = R::API;
        let corr = self.corr.fetch_add(1, Ordering::Relaxed) & i32::MAX;
        let header = RequestHeader::default()
            .with_request_api_key(api.key)
            .with_request_api_version(version)
            .with_correlation_id(corr)
            .with_client_id(Some(self.client_id.clone()));
        let mut buf = BytesMut::with_capacity(64);
        buf.put_i32(0);
        header
            .encode(&mut buf, R::header_version(version))
            .map_err(|e| KafkaError::Protocol(format!("encoding {} header: {e}", api.name)))?;
        req.encode(&mut buf, version)
            .map_err(|e| KafkaError::Protocol(format!("encoding {} v{version}: {e}", api.name)))?;
        let len = i32::try_from(buf.len() - 4)
            .map_err(|_| KafkaError::Protocol(format!("{} request too large", api.name)))?;
        buf[..4].copy_from_slice(&len.to_be_bytes());
        let (reply, rx) = oneshot::channel();
        let cmd = Cmd {
            corr,
            frame: buf.freeze(),
            max: R::MAX_RESPONSE,
            reply,
        };
        if self.is_closed() || self.tx.send(cmd).await.is_err() {
            return Err(closed());
        }
        let mut frame = match tokio::time::timeout(self.timeout, rx).await {
            Err(_) => {
                tracing::debug!(addr = %self.addr, api = api.name, "kafka: request timed out; closing");
                self.close();
                return Err(KafkaError::Timeout);
            }
            Ok(Err(_)) => return Err(closed()),
            Ok(Ok(r)) => r?,
        };
        // Response header: correlation id (already matched), then tagged fields when flexible.
        let header_len = {
            let mut w = WireCheck::new(api.name, &frame);
            w.i32()?;
            if R::Response::header_version(version) >= 1 {
                w.tagged_fields()?;
            }
            frame.len() - w.remaining()
        };
        frame.advance(header_len);
        Ok(frame)
    }
}

fn closed() -> KafkaError {
    KafkaError::Io(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        "kafka connection closed",
    ))
}

/// The connection task: owns the socket until EOF, an error, or every [`Conn`] handle drops.
async fn run<S>(stream: S, mut rx: mpsc::Receiver<Cmd>, max_frame: usize, timeout: Duration)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut pending: HashMap<i32, Pending> = HashMap::new();
    let mut buf = BytesMut::with_capacity(8 * 1024);
    let failure = loop {
        tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { return };
                let write = async {
                    wr.write_all(&cmd.frame).await?;
                    wr.flush().await
                };
                match tokio::time::timeout(timeout, write).await {
                    Err(_) => {
                        let _ = cmd.reply.send(Err(KafkaError::Timeout));
                        break Failure::Timeout;
                    }
                    Ok(Err(e)) => {
                        let f = Failure::Io(e.kind(), format!("kafka write: {e}"));
                        let _ = cmd.reply.send(Err(f.error()));
                        break f;
                    }
                    Ok(Ok(())) => {
                        pending.insert(cmd.corr, Pending { max: cmd.max, reply: cmd.reply });
                    }
                }
            }
            n = rd.read_buf(&mut buf) => {
                match n {
                    Err(e) => break Failure::Io(e.kind(), format!("kafka read: {e}")),
                    Ok(0) => {
                        let msg = if buf.is_empty() {
                            "kafka connection closed by the broker".to_owned()
                        } else {
                            format!("kafka connection closed mid-frame ({} bytes buffered)", buf.len())
                        };
                        break Failure::Io(std::io::ErrorKind::UnexpectedEof, msg);
                    }
                    Ok(_) => {
                        if let Err(f) = drain_frames(&mut buf, &mut pending, max_frame) {
                            break f;
                        }
                    }
                }
            }
        }
    };
    tracing::debug!(?failure, "kafka: connection closed");
    for (_, p) in pending.drain() {
        let _ = p.reply.send(Err(failure.error()));
    }
    rx.close();
    while let Ok(cmd) = rx.try_recv() {
        let _ = cmd.reply.send(Err(failure.error()));
    }
}

/// Hand every complete frame in `buf` to its waiter; reserve room for the next one.
fn drain_frames(
    buf: &mut BytesMut,
    pending: &mut HashMap<i32, Pending>,
    max_frame: usize,
) -> Result<(), Failure> {
    loop {
        if buf.len() < 4 {
            buf.reserve(8);
            return Ok(());
        }
        let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if size < 4 {
            return Err(Failure::Protocol(format!(
                "invalid kafka frame size {size}"
            )));
        }
        let size = size as usize;
        if size > max_frame {
            return Err(Failure::Protocol(format!(
                "kafka frame of {size} bytes exceeds the {max_frame}-byte limit"
            )));
        }
        if buf.len() < 8 {
            buf.reserve(8 - buf.len());
            return Ok(());
        }
        let corr = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let Some(p) = pending.get(&corr) else {
            return Err(Failure::Protocol(format!(
                "kafka response with unknown correlation id {corr}"
            )));
        };
        if size > p.max {
            return Err(Failure::Protocol(format!(
                "kafka response of {size} bytes exceeds the {}-byte limit for its request",
                p.max
            )));
        }
        if buf.len() < 4 + size {
            // Grow in bounded steps: memory follows bytes actually received.
            let want = (4 + size - buf.len()).min(1 << 20);
            buf.reserve(want);
            return Ok(());
        }
        buf.advance(4);
        let frame = buf.split_to(size).freeze();
        if let Some(p) = pending.remove(&corr) {
            let _ = p.reply.send(Ok(frame));
        }
    }
}

// ------------------------------------------------------------------------------ broker

struct BrokerState {
    conn: Option<Arc<Conn>>,
    backoff: Backoff,
    next_attempt: Option<tokio::time::Instant>,
}

/// A reconnecting handle to one broker.
pub struct Broker {
    addr: String,
    opts: Arc<ConnectOptions>,
    state: Mutex<BrokerState>,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl Broker {
    /// A broker handle (connects lazily) with the default backoff.
    pub fn new(addr: impl Into<String>, opts: Arc<ConnectOptions>) -> Self {
        Self::with_backoff(addr, opts, Backoff::default())
    }

    /// A broker handle with an injected backoff.
    pub fn with_backoff(
        addr: impl Into<String>,
        opts: Arc<ConnectOptions>,
        backoff: Backoff,
    ) -> Self {
        Broker {
            addr: addr.into(),
            opts,
            state: Mutex::new(BrokerState {
                conn: None,
                backoff,
                next_attempt: None,
            }),
        }
    }

    /// A broker handle around an already open connection.
    pub fn from_conn(conn: Conn, opts: Arc<ConnectOptions>) -> Self {
        Broker {
            addr: conn.addr().to_owned(),
            opts,
            state: Mutex::new(BrokerState {
                conn: Some(Arc::new(conn)),
                backoff: Backoff::default(),
                next_attempt: None,
            }),
        }
    }

    /// The broker address.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The live connection, reconnecting if the last one closed. After a failed attempt the
    /// next one waits for the backoff delay first.
    pub async fn conn(&self) -> Result<Arc<Conn>, KafkaError> {
        let mut st = self.state.lock().await;
        if let Some(c) = &st.conn {
            if !c.is_closed() {
                return Ok(Arc::clone(c));
            }
            st.conn = None;
        }
        if let Some(at) = st.next_attempt.take() {
            tokio::time::sleep_until(at).await;
        }
        match Conn::open(&self.addr, &self.opts).await {
            Ok(c) => {
                st.backoff.reset();
                let c = Arc::new(c);
                st.conn = Some(Arc::clone(&c));
                Ok(c)
            }
            Err(e) => {
                let delay = st.backoff.next_delay();
                st.next_attempt = Some(tokio::time::Instant::now() + delay);
                tracing::debug!(addr = %self.addr, error = %e, ?delay, "kafka: connect failed");
                Err(e)
            }
        }
    }

    /// Send `req` on the live connection (reconnecting first if needed).
    pub async fn request<R: ApiRequest>(&self, req: &R) -> Result<R::Response, KafkaError> {
        self.conn().await?.request(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_the_cap_with_jitter_in_range() {
        let mut lo = Backoff::with_rng(BACKOFF_BASE, BACKOFF_MAX, || 0);
        let got: Vec<_> = (0..10).map(|_| lo.next_delay().as_millis()).collect();
        assert_eq!(got, [50, 100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000]);
        let mut hi = Backoff::with_rng(BACKOFF_BASE, BACKOFF_MAX, || u64::MAX);
        let mut seen = Vec::new();
        for _ in 0..40 {
            let d = hi.next_delay();
            assert!(d <= BACKOFF_MAX, "{d:?}");
            seen.push(d);
        }
        assert!(seen[0] >= Duration::from_millis(50) && seen[0] <= Duration::from_millis(100));
        lo.reset();
        assert_eq!(lo.next_delay(), Duration::from_millis(50));
        let mut s = 1u64;
        let mut rnd = Backoff::with_rng(BACKOFF_BASE, BACKOFF_MAX, move || splitmix64(&mut s));
        for n in 0..30u32 {
            let cap = BACKOFF_BASE.saturating_mul(1 << n.min(20)).min(BACKOFF_MAX);
            let d = rnd.next_delay();
            assert!(d >= cap / 2 && d <= cap, "{n}: {d:?} vs {cap:?}");
        }
        assert!(format!("{rnd:?}").contains("attempt"));
    }

    #[test]
    fn host_port_parsing() {
        assert_eq!(
            split_host_port("127.0.0.1:9092").unwrap(),
            ("127.0.0.1".into(), 9092)
        );
        assert_eq!(
            split_host_port("[::1]:19092").unwrap(),
            ("::1".into(), 19092)
        );
        assert_eq!(
            split_host_port("kafka.example:9093").unwrap().0,
            "kafka.example"
        );
        for bad in ["", "host", ":9092", "host:x", "host:70000"] {
            assert!(split_host_port(bad).is_err(), "{bad}");
        }
    }

    fn pending_with(
        corr: i32,
        max: usize,
    ) -> (
        HashMap<i32, Pending>,
        oneshot::Receiver<Result<Bytes, KafkaError>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let mut m = HashMap::new();
        m.insert(corr, Pending { max, reply: tx });
        (m, rx)
    }

    #[test]
    fn frames_are_split_and_matched() {
        let (mut p, mut rx) = pending_with(7, 1024);
        let mut buf = BytesMut::new();
        buf.put_i32(6);
        buf.put_i32(7);
        buf.put_slice(&[1, 2]);
        // partial first: only the size
        let mut part = buf.split_to(3);
        drain_frames(&mut part, &mut p, MAX_FRAME).unwrap();
        assert!(rx.try_recv().is_err());
        part.unsplit(buf);
        drain_frames(&mut part, &mut p, MAX_FRAME).unwrap();
        let frame = rx.try_recv().unwrap().unwrap();
        assert_eq!(&frame[..], &[0, 0, 0, 7, 1, 2]);
        assert!(part.is_empty() && p.is_empty());
    }

    #[test]
    fn oversize_negative_and_unknown_frames_fail_without_allocating() {
        for (size, corr, max) in [
            (i32::MAX, 1, MAX_FRAME),
            ((MAX_FRAME + 1) as i32, 1, MAX_FRAME),
            (-5, 1, MAX_FRAME),
            (2, 1, MAX_FRAME),
            (100, 99, MAX_FRAME),
            (2000, 1, 1024),
        ] {
            let (mut p, _rx) = pending_with(1, max);
            let mut buf = BytesMut::new();
            buf.put_i32(size);
            buf.put_i32(corr);
            let cap_before = buf.capacity();
            let r = drain_frames(&mut buf, &mut p, MAX_FRAME);
            assert!(
                matches!(r, Err(Failure::Protocol(_))),
                "{size} {corr}: {r:?}"
            );
            assert!(
                buf.capacity() <= cap_before.max(64),
                "no allocation for a rejected frame"
            );
        }
    }

    #[test]
    fn wire_check_bounds() {
        let mut w = WireCheck::new("T", &[0, 0, 0, 2, 0, 1, b'a', 0, 5]);
        assert_eq!(w.array_len(false, 2).unwrap(), 2);
        w.string(false).unwrap();
        w.string(false).unwrap_err();
        let mut w = WireCheck::new("T", &[0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
        assert!(w.uvarint().is_err(), "6-byte varint");
        let mut w = WireCheck::new("T", &[0x80, 0x80, 0x80, 0x80, 0x08]);
        assert!(w.array_len(true, 1).is_err());
        // tagged field larger than the buffer
        let mut w = WireCheck::new("T", &[1, 0, 50, 0]);
        assert!(w.tagged_fields().is_err());
        let mut w = WireCheck::new("T", &[1, 0, 1, 9]);
        w.tagged_fields_with(|tag, f| {
            assert_eq!((tag, f.i8()?), (0, 9));
            Ok(())
        })
        .unwrap();
        assert_eq!(w.remaining(), 0);
        let mut w = WireCheck::new("T", &[0xff, 0xff, 0xff, 0xfe]);
        assert!(w.array_len(false, 1).is_err(), "negative count");
        let mut w = WireCheck::new("T", &[0xff, 0xff, 0xff, 0xff]);
        assert_eq!(w.array_len(false, 1).unwrap(), 0, "null array");
        let mut w = WireCheck::new("T", &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert_eq!(
            (w.i64().unwrap(), w.i32().unwrap(), w.i16().unwrap()),
            (1, 0, 0)
        );
        assert!(w.bytes(false).is_err());
    }
}
