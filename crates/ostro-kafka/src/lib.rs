//! ostro-kafka: a thin, consumer-side Kafka-protocol client (Phase 14, TRIG-04; D-P14-42).
//!
//! Built on `kafka-protocol` (client messages only), tokio TCP + tokio-rustls/aws-lc-rs TLS
//! and SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512. No librdkafka, no openssl, no C
//! compression: record batches are decoded with pure-Rust codecs (gzip `flate2`, snappy
//! `snap`, lz4 `lz4_flex` frame, zstd `ruzstd`).
//!
//! Module owners: [`conn`], [`sasl`], [`versions`] are 14-31 (transport, TLS, SASL, ApiVersions
//! negotiation); [`metadata`], [`fetch`], [`offsets`], [`codec`] and the `testkit` feature are
//! 14-34 (Metadata/FindCoordinator, ListOffsets/Fetch, OffsetFetch/OffsetCommit, record-batch
//! decoding). This module list and the contract types below are final after 14-01.
//!
//! Consumer-only: producing exists only behind the `testkit` feature (tests create topics and
//! produce records); consumer-group membership (JoinGroup/SyncGroup/KIP-848) is out of scope
//! (standalone offsets, D-P14-43).

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;

pub mod codec;
pub mod conn;
pub mod fetch;
pub mod metadata;
pub mod offsets;
pub mod sasl;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod versions;

pub use conn::{Backoff, Broker, Conn, ConnectOptions, MAX_FRAME, WireCheck};
pub use fetch::Fetched;
pub use versions::{ApiRange, ApiRequest, BrokerVersions, NegotiatedApi};

// Dependencies declared by plan 14-01 (D-P14-37) that no module uses: 14-31 does SCRAM with
// aws-lc-rs (HMAC, digest, PBKDF2, RNG), so `hmac` / `sha2` stay anchored here (removing them
// is a Cargo.toml change outside 14-34's scope). The codecs are used by `codec` (14-34).
mod dep_anchors {
    use hmac as _;
    use sha2 as _;
}

#[cfg(test)]
mod dev_dep_anchors {
    use rcgen as _;
    use tempfile as _;
}

/// TLS settings of a connection (roots: `rustls-native-certs` plus the optional `ca_file`,
/// the D-FX2 rule).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    /// Extra PEM CA bundle trusted in addition to the native roots.
    pub ca_file: Option<PathBuf>,
    /// SNI / certificate name override (default: the bootstrap host).
    pub server_name: Option<String>,
}

/// SASL mechanism (D-P14-42).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaslMechanism {
    /// `PLAIN` (RFC 4616).
    Plain,
    /// `SCRAM-SHA-256` (RFC 5802 / 7677).
    ScramSha256,
    /// `SCRAM-SHA-512`.
    ScramSha512,
}

impl SaslMechanism {
    /// The mechanism name sent in SaslHandshake.
    pub const fn as_str(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// SASL credentials. The password is wiped on drop and never printed.
#[derive(Clone)]
pub struct SaslConfig {
    /// Mechanism.
    pub mechanism: SaslMechanism,
    /// User name.
    pub username: String,
    /// Password (resolved from a sealed meta secret by the trigger, 14-32).
    pub password: zeroize::Zeroizing<String>,
}

impl std::fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Client configuration.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// `host:port` bootstrap brokers.
    pub bootstrap: Vec<String>,
    /// Kafka `client.id`.
    pub client_id: String,
    /// TLS, when the listener is SSL / SASL_SSL.
    pub tls: Option<TlsConfig>,
    /// SASL, when the listener is SASL_PLAINTEXT / SASL_SSL.
    pub sasl: Option<SaslConfig>,
    /// Per-request timeout.
    pub request_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            bootstrap: Vec::new(),
            client_id: "ostro".to_owned(),
            tls: None,
            sasl: None,
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// One Kafka topic partition.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
}

/// One consumed record.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Record {
    /// Offset in its partition.
    pub offset: i64,
    /// Record timestamp (Unix ms).
    pub timestamp_ms: i64,
    /// Key (`None` = null key).
    pub key: Option<Bytes>,
    /// Value (`None` = tombstone).
    pub value: Option<Bytes>,
    /// Headers in order (`None` value = null header).
    pub headers: Vec<(String, Option<Bytes>)>,
}

/// Where to start when the group has no committed offset (D-P14-43 `start_from`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OffsetSpec {
    /// The log end (`committed_or_latest`).
    #[default]
    Latest,
    /// The log start (`committed_or_earliest`).
    Earliest,
}

/// Records fetched from one partition.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FetchedBatch {
    /// The partition.
    pub tp: Option<TopicPartition>,
    /// Records with `offset >= ` the requested offset (read_committed: aborted ones skipped).
    pub records: Vec<Record>,
    /// The partition's high watermark (lag = `high_watermark - next offset`).
    pub high_watermark: i64,
}

/// Client errors.
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    /// Not implemented yet (names the owner plan).
    #[error("ostro-kafka: {0} is not implemented yet")]
    Unimplemented(&'static str),
    /// The broker's version range for `api` does not overlap the client's (D-P14-42).
    #[error(
        "unsupported_version: broker supports {api} v{broker_min}..=v{broker_max}, client needs v{client_min}..=v{client_max}"
    )]
    UnsupportedVersion {
        /// Kafka API name, e.g. `Fetch`.
        api: String,
        /// Broker minimum.
        broker_min: i16,
        /// Broker maximum.
        broker_max: i16,
        /// Client minimum.
        client_min: i16,
        /// Client maximum.
        client_max: i16,
    },
    /// Connection / socket failure.
    #[error("kafka I/O: {0}")]
    Io(#[from] std::io::Error),
    /// TLS setup or handshake failure.
    #[error("kafka TLS: {0}")]
    Tls(String),
    /// SASL authentication failure.
    #[error("kafka SASL: {0}")]
    Sasl(String),
    /// Malformed or unexpected protocol data.
    #[error("kafka protocol: {0}")]
    Protocol(String),
    /// A broker error code for a request.
    #[error("kafka broker error {code} in {api}")]
    Broker {
        /// Kafka API name.
        api: String,
        /// Kafka error code.
        code: i16,
    },
    /// The request timed out.
    #[error("kafka request timed out")]
    Timeout,
}

impl KafkaError {
    /// The `unsupported_version` error for `api` (the stable error name, RESERVATIONS §11).
    pub fn unsupported_version(
        api: impl Into<String>,
        broker: (i16, i16),
        client: (i16, i16),
    ) -> Self {
        KafkaError::UnsupportedVersion {
            api: api.into(),
            broker_min: broker.0,
            broker_max: broker.1,
            client_min: client.0,
            client_max: client.1,
        }
    }
}

/// A connected client (bootstrap connection + per-broker connections, 14-31/14-34).
#[derive(Debug)]
pub struct Client {
    config: ClientConfig,
    opts: std::sync::Arc<ConnectOptions>,
    bootstrap: Broker,
    meta: metadata::Metadata,
    coordinators: offsets::Coordinators,
}

impl Client {
    /// Connect to the first reachable bootstrap broker: TCP, TLS when configured, ApiVersions
    /// negotiation, SASL when configured (14-31). Bootstrap addresses are tried in order once;
    /// when all fail, a definitive error (TLS, SASL, protocol, unsupported version) is
    /// preferred over socket errors and timeouts.
    pub async fn connect(config: ClientConfig) -> Result<Client, KafkaError> {
        if config.bootstrap.is_empty() {
            return Err(KafkaError::Protocol(
                "no bootstrap brokers configured".into(),
            ));
        }
        let opts = std::sync::Arc::new(ConnectOptions::new(&config)?);
        let mut definitive: Option<KafkaError> = None;
        let mut last: Option<KafkaError> = None;
        for addr in &config.bootstrap {
            match Conn::open(addr, &opts).await {
                Ok(conn) => {
                    let bootstrap = Broker::from_conn(conn, std::sync::Arc::clone(&opts));
                    return Ok(Client {
                        config,
                        meta: metadata::Metadata::new(std::sync::Arc::clone(&opts)),
                        coordinators: offsets::Coordinators::default(),
                        opts,
                        bootstrap,
                    });
                }
                Err(e) => {
                    tracing::debug!(addr = %addr, error = %e, "kafka: bootstrap broker failed");
                    if matches!(e, KafkaError::Io(_) | KafkaError::Timeout) {
                        last = Some(e);
                    } else if definitive.is_none() {
                        definitive = Some(e);
                    }
                }
            }
        }
        Err(definitive
            .or(last)
            .unwrap_or_else(|| KafkaError::Protocol("no bootstrap broker reachable".into())))
    }

    /// The configuration this client was built with.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// The connection options shared by every broker connection of this client.
    pub fn connect_options(&self) -> &std::sync::Arc<ConnectOptions> {
        &self.opts
    }

    /// The (reconnecting) bootstrap broker.
    pub fn bootstrap(&self) -> &Broker {
        &self.bootstrap
    }

    /// The ApiVersions negotiation table of the bootstrap broker (reconnects if needed).
    pub async fn api_versions(&self) -> Result<Vec<NegotiatedApi>, KafkaError> {
        Ok(self.bootstrap.conn().await?.versions().table())
    }

    /// Send one request to the bootstrap broker at the negotiated version.
    pub async fn request<R: ApiRequest>(&self, req: &R) -> Result<R::Response, KafkaError> {
        self.bootstrap.request(req).await
    }

    // The read path (14-34) lives next to its requests: `partitions` / `leader` /
    // `refresh_metadata` in [`metadata`], `list_offsets` / `committed` / `commit` /
    // `coordinator` in [`offsets`], `fetch` / `fetch_at` in [`fetch`].
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sasl_debug_redacts_the_password() {
        let s = SaslConfig {
            mechanism: SaslMechanism::ScramSha512,
            username: "u".into(),
            password: zeroize::Zeroizing::new("hunter2-secret".into()),
        };
        let dbg = format!("{s:?}");
        assert!(!dbg.contains("hunter2"), "{dbg}");
        assert_eq!(SaslMechanism::ScramSha256.as_str(), "SCRAM-SHA-256");
    }

    #[test]
    fn unsupported_version_names_the_api() {
        let e = KafkaError::unsupported_version("Fetch", (13, 17), (4, 12));
        let msg = e.to_string();
        assert!(msg.starts_with("unsupported_version"), "{msg}");
        assert!(msg.contains("Fetch"), "{msg}");
    }
}
