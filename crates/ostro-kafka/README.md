# ostro-kafka

A thin, consumer-side Kafka-protocol client for Ostro's Kafka topic-consumer trigger
(TRIG-04, D-P14-42). Pure Rust: built on `kafka-protocol` (client messages), tokio TCP and
tokio-rustls on aws-lc-rs. **No librdkafka, no OpenSSL, no C compression libraries.**

Status: the transport (connections, framing, version negotiation, TLS, SASL) landed in 14-31.
Metadata, fetch, offsets and record-batch codecs land in 14-34.

## Supported brokers

- **Apache Kafka 3.x and 4.x.** The CI broker is Apache Kafka 4.1 (KRaft, single node).
- **Any broker that speaks the Kafka protocol** with the APIs and versions below, e.g. Redpanda
  or Chronik. Such brokers are supported **only through the Kafka protocol**: nothing in Ostro
  depends on, links or references their code.

On every new connection the client sends ApiVersions first (v3; a broker that refuses v3 gets
the version it names, down to v0) and then uses, per API, the highest version both sides
support. A broker whose range for an API does not overlap the client's fails that API with
`unsupported_version: broker supports <API> vX..=vY, client needs vA..=vB`. Other APIs keep
working.

| API              | Key | Client versions | Notes                                              |
|------------------|-----|-----------------|----------------------------------------------------|
| ApiVersions      | 18  | v0–3            | sent first on every connection                     |
| Metadata         | 3   | v1–12           |                                                    |
| FindCoordinator  | 10  | v0–3            | single-key form                                    |
| ListOffsets      | 2   | v1–7            |                                                    |
| Fetch            | 1   | v4–12           | capped below topic-id-only Fetch (v13+)            |
| OffsetFetch      | 9   | v1–7            | single-group form                                  |
| OffsetCommit     | 8   | v2–8            | standalone commits (generation -1, no KIP-848)     |
| SaslHandshake    | 17  | v1              | SASL tokens are carried in SaslAuthenticate        |
| SaslAuthenticate | 36  | v0–2            |                                                    |

Against Apache Kafka 4.1 the client uses the top of every range: ApiVersions v3, Metadata v12,
FindCoordinator v3, ListOffsets v7, Fetch v12, OffsetFetch v7, OffsetCommit v8,
SaslHandshake v1 and SaslAuthenticate v2.

## Security options

| Listener          | `ClientConfig`                                                   |
|-------------------|------------------------------------------------------------------|
| `PLAINTEXT`       | no `tls`, no `sasl`                                              |
| `SSL`             | `tls: Some(TlsConfig { ca_file, server_name })`                  |
| `SASL_PLAINTEXT`  | `sasl: Some(SaslConfig { mechanism, username, password })`       |
| `SASL_SSL`        | both                                                             |

- **TLS:** TLS 1.2/1.3 via rustls with the aws-lc-rs provider. The client trusts the system
  roots (`rustls-native-certs`) plus the optional `ca_file` PEM bundle. The certificate name
  (and SNI) is `server_name` if set, otherwise the host part of the broker address. IP
  addresses work when the certificate has a matching IP SAN. Client certificates (mTLS) are
  not supported in 1.0.
- **SASL:** `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512` (RFC 5802 / RFC 7677). SCRAM
  verifies the server's signature. A server that asks for fewer than 4096 iterations is
  rejected. OAUTHBEARER and Kerberos (GSSAPI) are not supported in 1.0.
- **Credentials** never show up in `Debug` output, logs or error messages. Password-derived
  buffers are wiped on drop. In the Ostro trigger the password comes from a sealed meta secret.

## Robustness

- Frames carry an `i32` size prefix. The client rejects any frame over 64 MiB (ApiVersions
  and SASL responses: 1 MiB), a negative size, or an unknown correlation id. Each of these
  closes the connection with a protocol error before any memory is allocated for the frame.
- Before decoding, every response is checked by a bounds-checked walker (`WireCheck`), so a
  malformed or truncated response is an error, never a panic or an unbounded allocation.
- Writes and requests time out after `request_timeout`. A timed-out connection is closed.
- A closed connection is replaced on the next request. Failed connection attempts are spaced
  by jittered exponential backoff, starting at 100 ms and capped at 10 s.

## Scope

Consumer only. Producing exists only behind the `testkit` feature, for tests. Consumer-group
membership (JoinGroup/SyncGroup, KIP-848), regex subscriptions and transactions on the
producer side are out of scope. Isolation is `read_committed`.
