//! The mTLS session client (D-W2): channel, `Session` stream, credits, heartbeats,
//! reconnect / re-join, self-fence and renewal.
//!
//! - **Channel:** tonic `Channel` to `https://<endpoint>` with `ClientTlsConfig` (cluster CA
//!   as the only root, the in-memory client identity; aws-lc-rs), HTTP/2 keepalive 5 s,
//!   encode/decode limits [`MAX_MESSAGE_BYTES`](crate::MAX_MESSAGE_BYTES).
//! - **Handshake:** the first message is `Hello{worker_id, proto_version, ostro_version,
//!   wasmtime_version, event_codec_ver, wit_packages, labels, slots, memory_bytes}`. On
//!   `Welcome` the worker adopts `lease_ttl_ms`, `heartbeat_ms`, `max_frame_bytes` and
//!   `EngineLimits` (the engine runtime is built from them). A `GoAway` before `Welcome`
//!   whose reason mentions a version is [`WorkerError::VersionMismatch`]: fatal, no retry.
//! - **Credits:** `Credit{free_slots}` (absolute) whenever the free slot count changes;
//!   `Heartbeat{leases, free_slots, memory_used}` every `heartbeat_ms`.
//! - **Stream loss** (error, EOF, `GoAway` after `Welcome`): every in-flight activation's
//!   token is cancelled immediately (self-fence) and the worker reconnects to the next
//!   endpoint with jittered exponential backoff capped at `reconnect_max_ms`.
//! - **Re-join (B5):** when every known endpoint has failed `rejoin_after_failures` times
//!   in a row and the identity came from a join, the worker joins again (new key,
//!   certificate and endpoints). External (cert-manager) identities keep retrying.
//! - **Renewal (Pitfall 10):** at `renew_after`, `Renew{csr}` over the current channel, a
//!   new channel + session with the new identity; in-flight activations move to the new
//!   session (their leases are listed in its first heartbeat), then `Drain` is sent on the
//!   old session and it is closed.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ostro_worker_proto::{PROTO_VERSION, v1};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tracing::{info, warn};

use crate::join::{self, Identity};
use crate::{Link, MAX_MESSAGE_BYTES, Shared, WorkerError, WorkerEvent, runner};

/// How long to wait for `Welcome` after `Hello`.
const WELCOME_TIMEOUT: Duration = Duration::from_secs(10);
/// First reconnect delay.
const BACKOFF_BASE_MS: u64 = 100;
/// Renewal retry delay after a failed renewal.
const RENEW_RETRY: Duration = Duration::from_secs(5);
/// Default heartbeat when the server sends 0.
const DEFAULT_HEARTBEAT_MS: u64 = 5_000;
/// Default max frame when the server sends 0 (D-W2: 1 MiB).
const DEFAULT_MAX_FRAME: usize = 1024 * 1024;

pub(crate) type Client = v1::execute_client::ExecuteClient<Channel>;

/// An open session (after `Welcome`).
pub(crate) struct Session {
    pub(crate) endpoint: String,
    pub(crate) client: Client,
    pub(crate) out: mpsc::UnboundedSender<v1::WorkerMsg>,
    pub(crate) inbound: tonic::Streaming<v1::ServerMsg>,
    pub(crate) welcome: v1::Welcome,
}

impl Session {
    pub(crate) fn send(&self, msg: v1::worker_msg::Msg) {
        let _ = self.out.send(v1::WorkerMsg { msg: Some(msg) });
    }

    fn link(&self) -> Link {
        let mf = self.welcome.max_frame_bytes as usize;
        Link {
            client: self.client.clone(),
            out: self.out.clone(),
            session_id: self.welcome.session_id.clone(),
            max_frame: if mf == 0 { DEFAULT_MAX_FRAME } else { mf },
        }
    }
}

/// Why opening a session failed.
pub(crate) enum OpenError {
    Transient(String),
    Fatal(WorkerError),
}

/// Why driving a session stopped.
enum DriveEnd {
    Shutdown,
    Lost(String),
    Renew,
}

/// The `Hello` this worker sends.
pub fn hello(shared_cfg: &crate::WorkerConfig, worker_id: &str) -> v1::Hello {
    v1::Hello {
        worker_id: worker_id.to_owned(),
        proto_version: PROTO_VERSION,
        ostro_version: crate::OSTRO_VERSION.to_owned(),
        wasmtime_version: crate::WASMTIME_VERSION.to_owned(),
        event_codec_ver: u32::from(ostro_types::journal::EVENT_CODEC_VER),
        wit_packages: crate::WIT_PACKAGES
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
        labels: shared_cfg
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        slots: shared_cfg.slots,
        memory_bytes: shared_cfg.memory_bytes,
    }
}

/// A tonic channel to `endpoint` with the cluster CA and the client identity.
pub(crate) async fn connect(endpoint: &str, id: &Identity) -> Result<Client, String> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&id.ca_pem))
        .identity(id.tonic_identity());
    let ep = Endpoint::from_shared(format!("https://{endpoint}"))
        .map_err(|e| format!("endpoint {endpoint}: {e}"))?
        .tls_config(tls)
        .map_err(|e| format!("TLS config: {e}"))?
        .connect_timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(5))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    let ch = ep.connect().await.map_err(|e| {
        use std::error::Error as _;
        let mut msg = format!("connect {endpoint}: {e}");
        let mut src = e.source();
        while let Some(s) = src {
            msg.push_str(": ");
            msg.push_str(&s.to_string());
            src = s.source();
        }
        msg
    })?;
    Ok(v1::execute_client::ExecuteClient::new(ch)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES))
}

/// Connect, send `Hello`, wait for `Welcome`.
pub(crate) async fn open(
    shared: &Shared,
    endpoint: &str,
    id: &Identity,
) -> Result<Session, OpenError> {
    let mut client = connect(endpoint, id).await.map_err(OpenError::Transient)?;
    let (out, rx) = mpsc::unbounded_channel();
    let _ = out.send(v1::WorkerMsg {
        msg: Some(v1::worker_msg::Msg::Hello(hello(
            &shared.cfg,
            &shared.worker_id,
        ))),
    });
    let mut inbound = client
        .session(UnboundedReceiverStream::new(rx))
        .await
        .map_err(|s| OpenError::Transient(format!("session: {}: {}", s.code(), s.message())))?
        .into_inner();
    let first = tokio::time::timeout(WELCOME_TIMEOUT, inbound.message())
        .await
        .map_err(|_| OpenError::Transient("no Welcome within 10 s".into()))?
        .map_err(|s| OpenError::Transient(format!("session: {}: {}", s.code(), s.message())))?;
    match first.and_then(|m| m.msg) {
        Some(v1::server_msg::Msg::Welcome(welcome)) => Ok(Session {
            endpoint: endpoint.to_owned(),
            client,
            out,
            inbound,
            welcome,
        }),
        Some(v1::server_msg::Msg::GoAway(g)) => {
            if g.reason.to_ascii_lowercase().contains("version") {
                Err(OpenError::Fatal(WorkerError::VersionMismatch(g.reason)))
            } else {
                Err(OpenError::Transient(format!("go away: {}", g.reason)))
            }
        }
        Some(other) => Err(OpenError::Transient(format!(
            "expected Welcome, got {other:?}"
        ))),
        None => Err(OpenError::Transient("session closed before Welcome".into())),
    }
}

fn heartbeat(shared: &Shared) -> v1::worker_msg::Msg {
    let leases = shared
        .active()
        .values()
        .map(|a| v1::Lease {
            run: Some(a.run.clone()),
            token: Some(a.token),
        })
        .collect();
    let memory_used = shared
        .runtime()
        .map(|r| r.parked().budget().used())
        .unwrap_or(0);
    v1::worker_msg::Msg::Heartbeat(v1::Heartbeat {
        leases,
        free_slots: shared.free_slots(),
        memory_used,
    })
}

fn credit(shared: &Shared) -> v1::worker_msg::Msg {
    let free = if shared.draining.load(Ordering::Acquire) {
        0
    } else {
        shared.free_slots()
    };
    v1::worker_msg::Msg::Credit(v1::Credit { free_slots: free })
}

/// Make `s` the current session: adopt `Welcome` (engine limits), publish the link, send
/// the current credit and a heartbeat with every lease.
fn install(shared: &Shared, s: &Session) -> Result<(), WorkerError> {
    let limits = s.welcome.limits.unwrap_or_default();
    shared.runtime_for(&limits)?;
    shared.set_link(Some(s.link()));
    s.send(credit(shared));
    s.send(heartbeat(shared));
    shared.emit(WorkerEvent::Connected {
        endpoint: s.endpoint.clone(),
        session_id: s.welcome.session_id.clone(),
    });
    info!(endpoint = %s.endpoint, node = s.welcome.node_id, "session established");
    Ok(())
}

fn token_eq(a: &v1::Token, b: Option<&v1::Token>) -> bool {
    b.is_none_or(|b| a == b)
}

fn handle(shared: &Arc<Shared>, msg: v1::server_msg::Msg) -> Option<DriveEnd> {
    use v1::server_msg::Msg;
    match msg {
        Msg::Activate(a) => {
            if shared.draining.load(Ordering::Acquire) {
                if let (Some(run), Some(token)) = (a.run.as_ref(), a.token.as_ref()) {
                    crate::remote_sink::send_done(
                        shared,
                        run,
                        token,
                        &ostro_engine::sink::ActivationOutcome::Evicted {
                            reason: ostro_engine::error::EvictReason::External,
                        },
                        a.journal_len,
                    );
                }
            } else {
                runner::spawn(shared, a);
            }
        }
        Msg::Input(i) => {
            if let Some(run) = &i.run {
                let g = shared.active();
                if let Some(a) = g.get(&run.run_id)
                    && token_eq(&a.token, i.token.as_ref())
                {
                    let _ = a.inputs.send(i.up_to_seq);
                }
            }
        }
        Msg::Cancel(v1::Cancel { run, token, .. })
        | Msg::LeaseLost(v1::LeaseLost { run, token }) => {
            if let Some(run) = &run {
                let g = shared.active();
                if let Some(a) = g.get(&run.run_id)
                    && token_eq(&a.token, token.as_ref())
                {
                    a.cancel.cancel();
                }
            }
        }
        Msg::GoAway(g) => return Some(DriveEnd::Lost(format!("go away: {}", g.reason))),
        Msg::Welcome(_) => warn!("unexpected second Welcome: ignored"),
    }
    None
}

async fn drive(
    shared: &Arc<Shared>,
    s: &mut Session,
    renew_at: Option<tokio::time::Instant>,
    shutdown: &CancellationToken,
) -> DriveEnd {
    let hb_ms = match s.welcome.heartbeat_ms {
        0 => DEFAULT_HEARTBEAT_MS,
        v => v,
    };
    let mut hb = tokio::time::interval(Duration::from_millis(hb_ms));
    hb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    hb.reset();
    loop {
        let renew_sleep = async {
            match renew_at {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = shutdown.cancelled() => return DriveEnd::Shutdown,
            _ = hb.tick() => s.send(heartbeat(shared)),
            _ = shared.slots_changed.notified() => s.send(credit(shared)),
            _ = renew_sleep => return DriveEnd::Renew,
            m = s.inbound.message() => match m {
                Ok(Some(v1::ServerMsg { msg: Some(msg) })) => {
                    if let Some(end) = handle(shared, msg) {
                        return end;
                    }
                }
                Ok(Some(v1::ServerMsg { msg: None })) => {}
                Ok(None) => return DriveEnd::Lost("session stream ended".into()),
                Err(st) => return DriveEnd::Lost(format!("session stream: {}: {}", st.code(), st.message())),
            },
        }
    }
}

/// Self-fence: cancel every in-flight activation and drop the link.
fn self_fence(shared: &Shared) {
    shared.set_link(None);
    let n = shared.cancel_all();
    if n > 0 {
        warn!(
            cancelled = n,
            "session lost: in-flight activations cancelled (self-fence)"
        );
    }
    shared.emit(WorkerEvent::SelfFenced { cancelled: n });
}

/// Jittered exponential backoff: uniform in [d/2, d], d = min(cap, base * 2^k).
fn backoff(k: u32, cap_ms: u64) -> Duration {
    let d = BACKOFF_BASE_MS
        .saturating_mul(1u64 << k.min(20))
        .min(cap_ms.max(1));
    let half = d / 2;
    let jitter = if half == 0 {
        0
    } else {
        crate::random_u64() % (half + 1)
    };
    Duration::from_millis(half + jitter)
}

async fn obtain_identity(shared: &Shared) -> Result<Identity, WorkerError> {
    let id = if shared.cfg.uses_external_cert() {
        join::load_external(&shared.cfg)?
    } else {
        join::join(&shared.cfg, &shared.worker_id).await?
    };
    shared.emit(WorkerEvent::Joined {
        endpoints: id.endpoints.clone(),
        cluster_id: id.cluster_id.clone(),
    });
    Ok(id)
}

fn renew_deadline(id: &Identity) -> Option<tokio::time::Instant> {
    // clock-exempt: X.509 renewal deadline of the worker's own certificate (no server logic).
    id.renew_after.map(|d| tokio::time::Instant::now() + d)
}

/// The worker's main loop (see the module docs).
pub(crate) async fn run_loop(
    shared: Arc<Shared>,
    shutdown: CancellationToken,
) -> Result<(), WorkerError> {
    let mut id = tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        r = obtain_identity(&shared) => r?,
    };
    if id.endpoints.is_empty() {
        return Err(WorkerError::Config("no cluster endpoints".into()));
    }
    let mut renew_at = renew_deadline(&id);
    let mut failures: HashMap<String, u32> = HashMap::new();
    let mut idx = 0usize;
    let mut attempt = 0u32;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let ep = id.endpoints[idx % id.endpoints.len()].clone();
        let opened = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            r = open(&shared, &ep, &id) => r,
        };
        match opened {
            Err(OpenError::Fatal(e)) => return Err(e),
            Err(OpenError::Transient(msg)) => {
                warn!(endpoint = %ep, error = %msg, "session failed");
                shared.emit(WorkerEvent::Disconnected {
                    endpoint: ep.clone(),
                    error: msg,
                });
                *failures.entry(ep.clone()).or_default() += 1;
            }
            Ok(mut s) => {
                failures.clear();
                attempt = 0;
                install(&shared, &s)?;
                loop {
                    match drive(&shared, &mut s, renew_at, &shutdown).await {
                        DriveEnd::Shutdown => {
                            crate::drain::drain(&shared, s).await;
                            return Ok(());
                        }
                        DriveEnd::Lost(msg) => {
                            warn!(endpoint = %s.endpoint, error = %msg, "session lost");
                            self_fence(&shared);
                            shared.emit(WorkerEvent::Disconnected {
                                endpoint: s.endpoint.clone(),
                                error: msg,
                            });
                            break;
                        }
                        DriveEnd::Renew => {
                            match renew_and_reopen(&shared, &mut s, &id).await {
                                Ok(new_id) => {
                                    id = new_id;
                                    renew_at = renew_deadline(&id);
                                }
                                Err(e) => {
                                    warn!(error = %e, "certificate renewal failed; retrying later");
                                    // clock-exempt: X.509 renewal retry on the worker.
                                    renew_at = Some(tokio::time::Instant::now() + RENEW_RETRY);
                                }
                            }
                        }
                    }
                }
            }
        }
        // Re-join (B5): every endpoint failed rejoin_after_failures times in a row.
        let limit = shared.cfg.rejoin_after_failures.max(1);
        if id.joined
            && id
                .endpoints
                .iter()
                .all(|e| failures.get(e).copied().unwrap_or(0) >= limit)
        {
            info!("every cluster endpoint failed {limit} times: re-joining");
            match tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                r = join::join(&shared.cfg, &shared.worker_id) => r,
            } {
                Ok(new_id) if !new_id.endpoints.is_empty() => {
                    shared.emit(WorkerEvent::Joined {
                        endpoints: new_id.endpoints.clone(),
                        cluster_id: new_id.cluster_id.clone(),
                    });
                    shared.emit(WorkerEvent::Rejoined);
                    id = new_id;
                    renew_at = renew_deadline(&id);
                    failures.clear();
                    idx = 0;
                    attempt = 0;
                    continue;
                }
                Ok(_) => warn!("re-join returned no endpoints"),
                Err(e) => {
                    warn!(error = %e, "re-join failed");
                    failures.clear();
                }
            }
        }
        idx = idx.wrapping_add(1);
        let delay = backoff(attempt, shared.cfg.reconnect_max_ms);
        attempt = attempt.saturating_add(1);
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// Renew over the current channel, open a new session with the new identity, move the
/// activations to it, then drain and close the old session.
async fn renew_and_reopen(
    shared: &Arc<Shared>,
    s: &mut Session,
    id: &Identity,
) -> Result<Identity, String> {
    let new_id = join::renew(&mut s.client, id, &shared.worker_id)
        .await
        .map_err(|e| e.to_string())?;
    let new_s = match open(shared, &s.endpoint, &new_id).await {
        Ok(n) => n,
        Err(OpenError::Transient(m)) => return Err(m),
        Err(OpenError::Fatal(e)) => return Err(e.to_string()),
    };
    let old = std::mem::replace(s, new_s);
    install(shared, s).map_err(|e| e.to_string())?;
    old.send(v1::worker_msg::Msg::Drain(v1::Drain {}));
    drop(old);
    shared.emit(WorkerEvent::Renewed);
    info!("client certificate renewed; session moved to a new channel");
    Ok(new_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_bounds() {
        for k in 0..30 {
            let d = backoff(k, 1_000).as_millis() as u64;
            let full = (BACKOFF_BASE_MS << k.min(20)).min(1_000);
            assert!(d >= full / 2 && d <= full, "k={k} d={d}");
        }
    }
}
