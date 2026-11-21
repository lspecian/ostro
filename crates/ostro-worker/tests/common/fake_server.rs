//! In-process fake Execute server (tonic, mTLS with an rcgen test CA) and a fake join
//! endpoint (HTTPS or plain HTTP on a std thread) for the worker protocol tests.
//!
//! The fake records every `WorkerMsg` (per session), `Hello`, append frame (chunk sizes),
//! reassembled event, `StartEffect`, journal page size and blob fetch, and serves journals
//! and blobs from memory (rows over 256 KiB are served as `BlobRef`s, like the server).
#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener as StdListener};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_types::journal::{Event, decode_event, encode_event};
use ostro_worker_proto::v1;
use ostro_worker_proto::v1::execute_server::{Execute, ExecuteServer};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::{self, Stream, wrappers::UnboundedReceiverStream};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

/// Rows above this are served as blob refs (the server's INLINE_PAYLOAD_LIMIT).
pub const INLINE_LIMIT: usize = 256 * 1024;

// ------------------------------------------------------------------------------ test CA

/// A test cluster CA with a server leaf for 127.0.0.1 / localhost.
pub struct TestCa {
    pub ca_pem: String,
    pub ca_der: Vec<u8>,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    pub server_cert_pem: String,
    pub server_cert_der: Vec<u8>,
    pub server_key_pem: String,
    pub server_key_der: Vec<u8>,
}

fn now_secs() -> i64 {
    // clock-exempt: X.509 validity of test certificates.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

impl TestCa {
    pub fn new() -> Self {
        let ca_key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
            rcgen::KeyUsagePurpose::DigitalSignature,
        ];
        p.distinguished_name
            .push(rcgen::DnType::CommonName, "ostro test cluster CA");
        let ca = p.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(p, ca_key);

        let skey = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let mut sp =
            rcgen::CertificateParams::new(vec!["127.0.0.1".to_string(), "localhost".to_string()])
                .unwrap();
        sp.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        sp.distinguished_name
            .push(rcgen::DnType::CommonName, "ostro test node");
        let scert = sp.signed_by(&skey, &issuer).unwrap();
        Self {
            ca_pem: ca.pem(),
            ca_der: ca.der().to_vec(),
            issuer,
            server_cert_pem: scert.pem(),
            server_cert_der: scert.der().to_vec(),
            server_key_pem: skey.serialize_pem(),
            server_key_der: skey.serialize_der(),
        }
    }

    /// `sha256:<hex>` of the CA certificate.
    pub fn hash(&self) -> String {
        ostro_worker::join::ca_hash_of(&self.ca_der)
    }

    /// Sign a worker CSR (clientAuth). `lifetime` = validity from now (default: rcgen's).
    pub fn sign_csr(&self, csr_pem: &str, lifetime: Option<Duration>) -> String {
        let mut csr = rcgen::CertificateSigningRequestParams::from_pem(csr_pem).unwrap();
        csr.params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        if let Some(l) = lifetime {
            let now = now_secs();
            csr.params.not_before = x509_parser::time::ASN1Time::from_timestamp(now - 60)
                .unwrap()
                .to_datetime();
            csr.params.not_after =
                x509_parser::time::ASN1Time::from_timestamp(now + l.as_secs() as i64)
                    .unwrap()
                    .to_datetime();
        }
        csr.signed_by(&self.issuer).unwrap().pem()
    }

    /// A rustls server config presenting `[leaf, ca]` (for the join endpoint).
    pub fn rustls_server(&self) -> Arc<rustls::ServerConfig> {
        let chain = vec![
            CertificateDer::from(self.server_cert_der.clone()),
            CertificateDer::from(self.ca_der.clone()),
        ];
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.server_key_der.clone()));
        let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
        Arc::new(cfg)
    }
}

// ------------------------------------------------------------------------------ fake Execute

/// Scripted behaviour of the fake.
#[derive(Clone)]
pub struct FakeConfig {
    pub welcome: v1::Welcome,
    /// Reply `GoAway{reason}` to `Hello` instead of `Welcome`.
    pub go_away: Option<String>,
    /// Flip one byte of every blob's first chunk.
    pub corrupt_blobs: bool,
    /// FetchBlob chunk size.
    pub blob_chunk: usize,
    /// Answer every append `fenced`.
    pub fence_appends: bool,
    /// Renew: certificate lifetime and the `renew_after_ms` returned.
    pub renew_lifetime: Option<Duration>,
    pub renew_after_ms: u64,
}

impl Default for FakeConfig {
    fn default() -> Self {
        Self {
            welcome: v1::Welcome {
                session_id: Vec::new(),
                node_id: 1,
                cluster_id: "c1".into(),
                lease_ttl_ms: 30_000,
                heartbeat_ms: 200,
                max_frame_bytes: 1024 * 1024,
                limits: Some(v1::EngineLimits {
                    hot_wait_ms: 120_000,
                    ..Default::default()
                }),
                members: Vec::new(),
            },
            go_away: None,
            corrupt_blobs: false,
            blob_chunk: 1024 * 1024,
            fence_appends: false,
            renew_lifetime: None,
            renew_after_ms: 3_600_000,
        }
    }
}

struct SessionRec {
    id: Vec<u8>,
    tx: mpsc::UnboundedSender<Result<v1::ServerMsg, Status>>,
    kill: CancellationToken,
}

/// Journal rows of a run: `(kind, encoded bytes)` by seq.
pub type Rows = Vec<(u32, Vec<u8>)>;
/// One reassembled append: `(run_id, seq, kind, bytes)`.
pub type Appended = (Vec<u8>, u64, u32, Vec<u8>);

/// One recorded append frame.
#[derive(Debug, Clone)]
pub struct FrameRec {
    pub run_id: Vec<u8>,
    pub event_index: u32,
    pub chunk_len: usize,
    pub kind: u32,
}

/// Everything the fake records and serves.
pub struct FakeState {
    pub cfg: Mutex<FakeConfig>,
    pub ca: Arc<TestCa>,
    sessions: Mutex<Vec<SessionRec>>,
    next_session: AtomicU64,
    /// (session_id, message) in arrival order.
    pub msgs: Mutex<Vec<(Vec<u8>, v1::WorkerMsg)>>,
    pub hellos: Mutex<Vec<v1::Hello>>,
    pub journals: Mutex<HashMap<Vec<u8>, Rows>>,
    pub blobs: Mutex<HashMap<[u8; 32], Vec<u8>>>,
    pub frames: Mutex<Vec<FrameRec>>,
    pub appended: Mutex<Vec<Appended>>,
    pub effects: Mutex<Vec<v1::StartEffectRequest>>,
    pub page_bytes: Mutex<Vec<usize>>,
    pub blob_fetches: AtomicUsize,
    pub renewals: AtomicUsize,
}

impl FakeState {
    pub fn new(ca: Arc<TestCa>, cfg: FakeConfig) -> Arc<Self> {
        Arc::new(Self {
            cfg: Mutex::new(cfg),
            ca,
            sessions: Mutex::new(Vec::new()),
            next_session: AtomicU64::new(1),
            msgs: Mutex::new(Vec::new()),
            hellos: Mutex::new(Vec::new()),
            journals: Mutex::new(HashMap::new()),
            blobs: Mutex::new(HashMap::new()),
            frames: Mutex::new(Vec::new()),
            appended: Mutex::new(Vec::new()),
            effects: Mutex::new(Vec::new()),
            page_bytes: Mutex::new(Vec::new()),
            blob_fetches: AtomicUsize::new(0),
            renewals: AtomicUsize::new(0),
        })
    }

    /// Store a blob; returns its sha.
    pub fn put_blob(&self, bytes: Vec<u8>) -> [u8; 32] {
        let sha: [u8; 32] = Sha256::digest(&bytes).into();
        self.blobs.lock().unwrap().insert(sha, bytes);
        sha
    }

    /// Append events to a run's journal (seq must be contiguous).
    pub fn push_events(&self, run_id: &[u8], events: &[Event]) {
        let mut j = self.journals.lock().unwrap();
        let rows = j.entry(run_id.to_vec()).or_default();
        for ev in events {
            assert_eq!(ev.seq, rows.len() as u64, "contiguous seq");
            rows.push(encode_event(ev));
        }
    }

    pub fn journal_len(&self, run_id: &[u8]) -> u64 {
        self.journals
            .lock()
            .unwrap()
            .get(run_id)
            .map_or(0, |r| r.len() as u64)
    }

    /// Decoded journal of a run.
    pub fn journal(&self, run_id: &[u8]) -> Vec<Event> {
        self.journals
            .lock()
            .unwrap()
            .get(run_id)
            .map(|rows| {
                rows.iter()
                    .enumerate()
                    .map(|(i, (k, b))| decode_event(i as u64, *k, b).unwrap())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Send to the newest session.
    pub fn send(&self, msg: v1::server_msg::Msg) -> bool {
        let g = self.sessions.lock().unwrap();
        match g.last() {
            Some(s) => s.tx.send(Ok(v1::ServerMsg { msg: Some(msg) })).is_ok(),
            None => false,
        }
    }

    /// Kill the newest session's stream (server-side stream loss).
    pub fn kill_current(&self) {
        if let Some(s) = self.sessions.lock().unwrap().last() {
            s.kill.cancel();
        }
    }

    pub fn session_count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    pub fn current_session_id(&self) -> Option<Vec<u8>> {
        self.sessions.lock().unwrap().last().map(|s| s.id.clone())
    }

    /// Messages matching `f`.
    pub fn find<T>(&self, f: impl Fn(&[u8], &v1::worker_msg::Msg) -> Option<T>) -> Vec<T> {
        self.msgs
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(sid, m)| m.msg.as_ref().and_then(|m| f(sid, m)))
            .collect()
    }

    /// `Done`s for a run.
    pub fn dones(&self, run_id: &[u8]) -> Vec<v1::Done> {
        self.find(|_, m| match m {
            v1::worker_msg::Msg::Done(d) if d.run.as_ref().is_some_and(|r| r.run_id == run_id) => {
                Some(d.clone())
            }
            _ => None,
        })
    }

    /// Poll `f` every 10 ms until true or `timeout`.
    pub async fn wait_for(&self, timeout: Duration, what: &str, f: impl Fn(&Self) -> bool) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if f(self) {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out after {timeout:?} waiting for {what}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The tonic service.
#[derive(Clone)]
pub struct FakeExecute {
    st: Arc<FakeState>,
    kill_all: CancellationToken,
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl Execute for FakeExecute {
    type SessionStream = BoxStream<v1::ServerMsg>;
    type FetchJournalStream = BoxStream<v1::JournalPage>;
    type FetchBlobStream = BoxStream<v1::BlobChunk>;

    async fn session(
        &self,
        req: Request<Streaming<v1::WorkerMsg>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let mut inbound = req.into_inner();
        let (tx, rx) = mpsc::unbounded_channel();
        let kill = self.kill_all.child_token();
        let n = self.st.next_session.fetch_add(1, Ordering::Relaxed);
        let id = format!("s{n}").into_bytes();
        self.st.sessions.lock().unwrap().push(SessionRec {
            id: id.clone(),
            tx: tx.clone(),
            kill: kill.clone(),
        });
        let st = Arc::clone(&self.st);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = kill.cancelled() => {
                        let _ = tx.send(Err(Status::unavailable("killed by test")));
                        break;
                    }
                    m = inbound.message() => match m {
                        Ok(Some(msg)) => {
                            st.msgs.lock().unwrap().push((id.clone(), msg.clone()));
                            if let Some(v1::worker_msg::Msg::Hello(h)) = msg.msg {
                                st.hellos.lock().unwrap().push(h);
                                let cfg = st.cfg.lock().unwrap().clone();
                                if let Some(reason) = cfg.go_away {
                                    let _ = tx.send(Ok(v1::ServerMsg {
                                        msg: Some(v1::server_msg::Msg::GoAway(v1::GoAway { reason })),
                                    }));
                                    break;
                                }
                                let mut w = cfg.welcome;
                                w.session_id = id.clone();
                                let _ = tx.send(Ok(v1::ServerMsg {
                                    msg: Some(v1::server_msg::Msg::Welcome(w)),
                                }));
                            }
                        }
                        Ok(None) | Err(_) => break,
                    },
                }
            }
            st.sessions.lock().unwrap().retain(|s| s.id != id);
            drop(tx);
        });
        Ok(Response::new(Box::pin(UnboundedReceiverStream::new(rx))))
    }

    async fn fetch_journal(
        &self,
        req: Request<v1::FetchJournalRequest>,
    ) -> Result<Response<Self::FetchJournalStream>, Status> {
        let r = req.into_inner();
        let run = r.run.ok_or_else(|| Status::invalid_argument("run"))?;
        let max = self.st.cfg.lock().unwrap().welcome.max_frame_bytes as usize;
        let rows: Vec<(u32, Vec<u8>)> = self
            .st
            .journals
            .lock()
            .unwrap()
            .get(&run.run_id)
            .cloned()
            .unwrap_or_default();
        if r.to_seq > rows.len() as u64 || r.from_seq > r.to_seq {
            return Err(Status::out_of_range("journal range"));
        }
        let mut pages = Vec::new();
        let mut cur = Vec::new();
        let mut cur_bytes = 0usize;
        for seq in r.from_seq..r.to_seq {
            let (kind, bytes) = &rows[seq as usize];
            let payload = if bytes.len() > INLINE_LIMIT {
                let sha = self.st.put_blob(bytes.clone());
                v1::journal_row::Payload::Blob(v1::BlobRef {
                    sha: sha.to_vec(),
                    len: bytes.len() as u64,
                })
            } else {
                v1::journal_row::Payload::Inline(bytes.clone())
            };
            let size = match &payload {
                v1::journal_row::Payload::Inline(b) => b.len() + 16,
                v1::journal_row::Payload::Blob(_) => 64,
            };
            if !cur.is_empty() && cur_bytes + size > max {
                pages.push(std::mem::take(&mut cur));
                self.st.page_bytes.lock().unwrap().push(cur_bytes);
                cur_bytes = 0;
            }
            cur.push(v1::JournalRow {
                seq,
                kind: *kind,
                payload: Some(payload),
            });
            cur_bytes += size;
        }
        self.st.page_bytes.lock().unwrap().push(cur_bytes);
        pages.push(cur);
        let n = pages.len();
        let out: Vec<Result<v1::JournalPage, Status>> = pages
            .into_iter()
            .enumerate()
            .map(|(i, rows)| {
                Ok(v1::JournalPage {
                    rows,
                    last: i + 1 == n,
                })
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(out))))
    }

    async fn fetch_blob(
        &self,
        req: Request<v1::FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        let r = req.into_inner();
        self.st.blob_fetches.fetch_add(1, Ordering::Relaxed);
        let sha: [u8; 32] = r
            .sha
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("sha"))?;
        let bytes = self
            .st
            .blobs
            .lock()
            .unwrap()
            .get(&sha)
            .cloned()
            .ok_or_else(|| Status::not_found("blob"))?;
        let cfg = self.st.cfg.lock().unwrap().clone();
        let total = bytes.len() as u64;
        let chunks: Vec<Vec<u8>> = bytes
            .chunks(cfg.blob_chunk.max(1))
            .map(<[u8]>::to_vec)
            .collect();
        let n = chunks.len();
        let out: Vec<Result<v1::BlobChunk, Status>> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, mut data)| {
                if i == 0 && cfg.corrupt_blobs && !data.is_empty() {
                    let mid = data.len() / 2;
                    data[mid] ^= 0xff;
                }
                Ok(v1::BlobChunk {
                    data,
                    last: i + 1 == n,
                    total_len: total,
                })
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(out))))
    }

    async fn append(
        &self,
        req: Request<Streaming<v1::AppendFrame>>,
    ) -> Result<Response<v1::AppendResult>, Status> {
        use v1::append_result::Result as R;
        let mut s = req.into_inner();
        let mut run_id: Option<Vec<u8>> = None;
        let mut from_seq = 0u64;
        let mut events: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut cur: Vec<u8> = Vec::new();
        let mut expect_index = 0u32;
        while let Some(f) = s.message().await? {
            let rid = f.run.as_ref().map(|r| r.run_id.clone()).unwrap_or_default();
            self.st.frames.lock().unwrap().push(FrameRec {
                run_id: rid.clone(),
                event_index: f.event_index,
                chunk_len: f.chunk.len(),
                kind: f.kind,
            });
            run_id.get_or_insert(rid);
            from_seq = f.from_seq;
            if f.event_index != expect_index {
                return Ok(Response::new(v1::AppendResult {
                    result: Some(R::Rejected("event_index out of order".into())),
                }));
            }
            cur.extend_from_slice(&f.chunk);
            if f.event_end {
                events.push((f.kind, std::mem::take(&mut cur)));
                expect_index += 1;
            }
            if f.last {
                break;
            }
        }
        let run_id = run_id.unwrap_or_default();
        if self.st.cfg.lock().unwrap().fence_appends {
            return Ok(Response::new(v1::AppendResult {
                result: Some(R::Fenced(true)),
            }));
        }
        let mut j = self.st.journals.lock().unwrap();
        let rows = j.entry(run_id.clone()).or_default();
        if from_seq != rows.len() as u64 {
            return Ok(Response::new(v1::AppendResult {
                result: Some(R::ConflictExpected(rows.len() as u64)),
            }));
        }
        for (i, (kind, bytes)) in events.into_iter().enumerate() {
            let seq = from_seq + i as u64;
            let ev = match decode_event(seq, kind, &bytes) {
                Ok(ev) => ev,
                Err(e) => {
                    return Ok(Response::new(v1::AppendResult {
                        result: Some(R::Rejected(format!("seq {seq}: {e}"))),
                    }));
                }
            };
            assert_eq!(encode_event(&ev), (kind, bytes.clone()), "byte-identical");
            self.st
                .appended
                .lock()
                .unwrap()
                .push((run_id.clone(), seq, kind, bytes.clone()));
            rows.push((kind, bytes));
        }
        Ok(Response::new(v1::AppendResult {
            result: Some(R::Next(rows.len() as u64)),
        }))
    }

    async fn start_effect(
        &self,
        req: Request<v1::StartEffectRequest>,
    ) -> Result<Response<v1::StartEffectResponse>, Status> {
        self.st.effects.lock().unwrap().push(req.into_inner());
        Ok(Response::new(v1::StartEffectResponse {
            result: Some(v1::start_effect_response::Result::Started(true)),
        }))
    }

    async fn renew(
        &self,
        req: Request<v1::RenewRequest>,
    ) -> Result<Response<v1::RenewResponse>, Status> {
        let csr =
            String::from_utf8(req.into_inner().csr).map_err(|_| Status::invalid_argument("csr"))?;
        let cfg = self.st.cfg.lock().unwrap().clone();
        let cert = self.st.ca.sign_csr(&csr, cfg.renew_lifetime);
        self.st.renewals.fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(v1::RenewResponse {
            client_cert: cert.into_bytes(),
            renew_after_ms: cfg.renew_after_ms,
        }))
    }
}

/// A running fake Execute server.
pub struct FakeServer {
    pub st: Arc<FakeState>,
    pub addr: SocketAddr,
    shutdown: CancellationToken,
    kill_all: CancellationToken,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl FakeServer {
    /// Start on an ephemeral loopback port (mTLS: client certificate required).
    pub async fn start(st: Arc<FakeState>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let kill_all = CancellationToken::new();
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &st.ca.server_cert_pem,
                &st.ca.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&st.ca.ca_pem));
        let svc = ExecuteServer::new(FakeExecute {
            st: Arc::clone(&st),
            kill_all: kill_all.clone(),
        })
        .max_decoding_message_size(ostro_worker::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(ostro_worker::MAX_MESSAGE_BYTES);
        let sd = shutdown.clone();
        let handle = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    sd.cancelled_owned(),
                )
                .await
                .unwrap();
        });
        Self {
            st,
            addr,
            shutdown,
            kill_all,
            handle: Some(handle),
        }
    }

    /// `host:port` as advertised to workers.
    pub fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    /// Kill every session and stop listening.
    pub async fn stop(mut self) {
        self.kill_all.cancel();
        self.shutdown.cancel();
        if let Some(h) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
        }
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.kill_all.cancel();
        self.shutdown.cancel();
    }
}

// ------------------------------------------------------------------------------ fake join

/// A fake `POST /v1/workers/join` endpoint signing CSRs with the test CA.
pub struct FakeJoin {
    pub addr: SocketAddr,
    pub tls: bool,
    /// Raw request bodies received (one per join).
    pub requests: Arc<Mutex<Vec<String>>>,
    pub endpoints: Arc<Mutex<Vec<String>>>,
    pub renew_after_ms: Arc<AtomicU64>,
    /// Client certificate lifetime in seconds (0 = rcgen default).
    pub cert_lifetime_s: Arc<AtomicU64>,
    /// Replace the CA certificate in the response (CA-hash mismatch test).
    pub ca_override: Arc<Mutex<Option<String>>>,
    /// Refuse every join with 400 `join_token_invalid` (revoked token, 07-22).
    pub refuse: Arc<AtomicBool>,
}

impl FakeJoin {
    pub fn start(ca: Arc<TestCa>, tls: bool, endpoints: Vec<String>) -> Self {
        let listener = StdListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let me = Self {
            addr,
            tls,
            requests: Arc::new(Mutex::new(Vec::new())),
            endpoints: Arc::new(Mutex::new(endpoints)),
            renew_after_ms: Arc::new(AtomicU64::new(0)),
            cert_lifetime_s: Arc::new(AtomicU64::new(0)),
            ca_override: Arc::new(Mutex::new(None)),
            refuse: Arc::new(AtomicBool::new(false)),
        };
        let server_cfg = ca.rustls_server();
        let (reqs, eps, renew, life, ovr, refuse) = (
            Arc::clone(&me.requests),
            Arc::clone(&me.endpoints),
            Arc::clone(&me.renew_after_ms),
            Arc::clone(&me.cert_lifetime_s),
            Arc::clone(&me.ca_override),
            Arc::clone(&me.refuse),
        );
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(conn) = conn else { continue };
                let (ca, cfg, reqs, eps, renew, life, ovr, refuse) = (
                    Arc::clone(&ca),
                    Arc::clone(&server_cfg),
                    Arc::clone(&reqs),
                    Arc::clone(&eps),
                    Arc::clone(&renew),
                    Arc::clone(&life),
                    Arc::clone(&ovr),
                    Arc::clone(&refuse),
                );
                std::thread::spawn(move || {
                    let handler = |stream: &mut dyn ReadWrite| {
                        let Some(body) = read_request(stream) else {
                            return;
                        };
                        reqs.lock().unwrap().push(body.clone());
                        if refuse.load(Ordering::Relaxed) {
                            let resp = serde_json::json!({"error": {
                                "code": "invalid_argument",
                                "message": "join token revoked",
                                "reason": "join_token_invalid",
                            }})
                            .to_string();
                            let _ = write!(
                                stream,
                                "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{resp}",
                                resp.len()
                            );
                            let _ = stream.flush();
                            return;
                        }
                        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                        let csr = v["csr_pem"].as_str().unwrap();
                        let l = life.load(Ordering::Relaxed);
                        let cert = ca.sign_csr(csr, (l > 0).then(|| Duration::from_secs(l)));
                        let ca_pem = ovr
                            .lock()
                            .unwrap()
                            .clone()
                            .unwrap_or_else(|| ca.ca_pem.clone());
                        let resp = serde_json::json!({
                            "client_cert_pem": cert,
                            "ca_cert_pem": ca_pem,
                            "endpoints": eps.lock().unwrap().clone(),
                            "renew_after_ms": renew.load(Ordering::Relaxed),
                            "cluster_id": "c1",
                        })
                        .to_string();
                        let _ = write!(
                            stream,
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{resp}",
                            resp.len()
                        );
                        let _ = stream.flush();
                    };
                    if tls {
                        let Ok(sc) = rustls::ServerConnection::new(cfg) else {
                            return;
                        };
                        let mut s = rustls::StreamOwned::new(sc, conn);
                        handler(&mut s);
                        s.conn.send_close_notify();
                        let _ = s.flush();
                    } else {
                        let mut c = conn;
                        handler(&mut c);
                    }
                });
            }
        });
        me
    }

    pub fn url(&self) -> String {
        format!(
            "{}://127.0.0.1:{}",
            if self.tls { "https" } else { "http" },
            self.addr.port()
        )
    }
}

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

/// Read one HTTP/1.1 request; returns its body (None on handshake / parse failure).
fn read_request(s: &mut dyn ReadWrite) -> Option<String> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        let n = s.read(&mut tmp).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let len: usize = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())?
        })
        .unwrap_or(0);
    while buf.len() < header_end + len {
        let n = s.read(&mut tmp).ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    if !head.starts_with("POST /v1/workers/join ") {
        return None;
    }
    Some(String::from_utf8_lossy(&buf[header_end..header_end + len]).to_string())
}
