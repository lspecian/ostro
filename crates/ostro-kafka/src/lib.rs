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
//! decoding). This module list and the contract types below are final after 14-01; the
//! [`Client`] methods answer [`KafkaError::Unimplemented`] until their owner plan lands.
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

// Dependencies declared by plan 14-01 (D-P14-37), anchored so cargo's unused-dependency lint
// stays quiet until 14-31 / 14-34 consume them (owners delete the lines they use).
mod dep_anchors {
    use anyhow as _;
    use aws_lc_rs as _;
    use base64 as _;
    use flate2 as _;
    use hmac as _;
    use kafka_protocol as _;
    use lz4_flex as _;
    use rustls as _;
    use rustls_native_certs as _;
    use rustls_pki_types as _;
    use ruzstd as _;
    use sha2 as _;
    use snap as _;
    use tokio as _;
    use tokio_rustls as _;
    use tracing as _;
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
}

impl Client {
    /// Connect to the first reachable bootstrap broker and negotiate ApiVersions (14-31).
    pub async fn connect(config: ClientConfig) -> Result<Client, KafkaError> {
        drop(config);
        Err(KafkaError::Unimplemented("Client::connect (14-31)"))
    }

    /// The configuration this client was built with.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Partition indexes of `topic` (Metadata, 14-34).
    pub async fn partitions(&self, topic: &str) -> Result<Vec<i32>, KafkaError> {
        let _ = topic;
        Err(KafkaError::Unimplemented("Client::partitions (14-34)"))
    }

    /// Start or end offsets of `tps` (ListOffsets, 14-34).
    pub async fn list_offsets(
        &self,
        tps: &[TopicPartition],
        spec: OffsetSpec,
    ) -> Result<Vec<(TopicPartition, i64)>, KafkaError> {
        let _ = (tps, spec);
        Err(KafkaError::Unimplemented("Client::list_offsets (14-34)"))
    }

    /// Fetch from `tp` at `offset`, at most `max_bytes` (Fetch ≤ v12, read_committed, 14-34).
    pub async fn fetch(
        &self,
        tp: &TopicPartition,
        offset: i64,
        max_bytes: i32,
    ) -> Result<FetchedBatch, KafkaError> {
        let _ = (tp, offset, max_bytes);
        Err(KafkaError::Unimplemented("Client::fetch (14-34)"))
    }

    /// Committed offsets of `group` for `tps` (FindCoordinator + OffsetFetch, 14-34).
    pub async fn committed(
        &self,
        group: &str,
        tps: &[TopicPartition],
    ) -> Result<Vec<(TopicPartition, Option<i64>)>, KafkaError> {
        let _ = (group, tps);
        Err(KafkaError::Unimplemented("Client::committed (14-34)"))
    }

    /// Commit `offsets` for `group` (OffsetCommit, generation -1, empty member id; 14-34).
    pub async fn commit(
        &self,
        group: &str,
        offsets: &[(TopicPartition, i64)],
    ) -> Result<(), KafkaError> {
        let _ = (group, offsets);
        Err(KafkaError::Unimplemented("Client::commit (14-34)"))
    }
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
