//! Client API version ranges and ApiVersions negotiation (14-31, D-P14-42).
//!
//! Every new connection sends ApiVersions first (v3, falling back when the broker answers
//! `UNSUPPORTED_VERSION`), records the broker's ranges ([`BrokerVersions`]) and then uses, per
//! API, the highest version in both ranges ([`negotiate`]). A broker whose range does not
//! overlap the client's fails with [`KafkaError::UnsupportedVersion`]
//! (`unsupported_version: ...` naming the API) when that API is first used, so a broker
//! without, say, OffsetCommit still serves Metadata and Fetch.
//!
//! Client ranges (all inside what `kafka-protocol` 0.18 implements; nothing had to be
//! tightened):
//!
//! | API              | key | client | why the cap                                         |
//! |------------------|-----|--------|-----------------------------------------------------|
//! | ApiVersions      | 18  | v0–3   | v4 adds nothing a consumer needs                     |
//! | Metadata         | 3   | v1–12  | v13 only adds a top-level error code                 |
//! | FindCoordinator  | 10  | v0–3   | v4+ is the batched (coordinator keys) form           |
//! | ListOffsets      | 2   | v1–7   | v8+ adds tiered-storage timestamps                   |
//! | Fetch            | 1   | v4–12  | v13+ addresses topics by id only                     |
//! | OffsetFetch      | 9   | v1–7   | v8+ is the multi-group form                          |
//! | OffsetCommit     | 8   | v2–8   | v9+ is KIP-848 member-epoch commits                  |
//! | SaslHandshake    | 17  | v1     | v1 = tokens wrapped in SaslAuthenticate              |
//! | SaslAuthenticate | 36  | v0–2   | full range                                           |
//!
//! [`ApiRequest`] ties a `kafka-protocol` request type to its range, response type and a
//! structural pre-check of the response bytes (see [`crate::conn::WireCheck`]: kafka-protocol
//! pre-allocates arrays from the wire count, so every response is walked before decoding).
//! This module implements it for ApiVersions, SaslHandshake, SaslAuthenticate and Metadata;
//! the read-path plan (14-34) implements it for its own requests next to their code.

use std::collections::BTreeMap;

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};

use crate::KafkaError;
use crate::conn::{MAX_FRAME, WireCheck};

/// One API's client version range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiRange {
    /// Kafka API key.
    pub key: i16,
    /// Kafka API name (used in errors and logs).
    pub name: &'static str,
    /// Lowest version the client speaks.
    pub min: i16,
    /// Highest version the client speaks.
    pub max: i16,
}

/// ApiVersions v0–3.
pub const API_VERSIONS: ApiRange = ApiRange {
    key: 18,
    name: "ApiVersions",
    min: 0,
    max: 3,
};
/// Metadata v1–12.
pub const METADATA: ApiRange = ApiRange {
    key: 3,
    name: "Metadata",
    min: 1,
    max: 12,
};
/// FindCoordinator v0–3 (single-key form).
pub const FIND_COORDINATOR: ApiRange = ApiRange {
    key: 10,
    name: "FindCoordinator",
    min: 0,
    max: 3,
};
/// ListOffsets v1–7.
pub const LIST_OFFSETS: ApiRange = ApiRange {
    key: 2,
    name: "ListOffsets",
    min: 1,
    max: 7,
};
/// Fetch v4–12 (capped below topic-id-only Fetch).
pub const FETCH: ApiRange = ApiRange {
    key: 1,
    name: "Fetch",
    min: 4,
    max: 12,
};
/// OffsetFetch v1–7 (single-group form).
pub const OFFSET_FETCH: ApiRange = ApiRange {
    key: 9,
    name: "OffsetFetch",
    min: 1,
    max: 7,
};
/// OffsetCommit v2–8 (pre-KIP-848: generation -1, empty member id).
pub const OFFSET_COMMIT: ApiRange = ApiRange {
    key: 8,
    name: "OffsetCommit",
    min: 2,
    max: 8,
};
/// SaslHandshake v1.
pub const SASL_HANDSHAKE: ApiRange = ApiRange {
    key: 17,
    name: "SaslHandshake",
    min: 1,
    max: 1,
};
/// SaslAuthenticate v0–2.
pub const SASL_AUTHENTICATE: ApiRange = ApiRange {
    key: 36,
    name: "SaslAuthenticate",
    min: 0,
    max: 2,
};

/// Every API the client uses, in table order.
pub const CLIENT_RANGES: [ApiRange; 9] = [
    API_VERSIONS,
    METADATA,
    FIND_COORDINATOR,
    LIST_OFFSETS,
    FETCH,
    OFFSET_FETCH,
    OFFSET_COMMIT,
    SASL_HANDSHAKE,
    SASL_AUTHENTICATE,
];

/// Kafka error code `UNSUPPORTED_VERSION`.
pub const UNSUPPORTED_VERSION_CODE: i16 = 35;

/// The highest version in both ranges, or `unsupported_version` naming the API.
/// `broker = None` means the broker does not offer the API at all (reported as `v-1..=v-1`).
pub fn negotiate(client: &ApiRange, broker: Option<(i16, i16)>) -> Result<i16, KafkaError> {
    let (bmin, bmax) = broker.unwrap_or((-1, -1));
    let v = client.max.min(bmax);
    if broker.is_some() && bmin <= bmax && v >= client.min.max(bmin) {
        Ok(v)
    } else {
        Err(KafkaError::unsupported_version(
            client.name,
            (bmin, bmax),
            (client.min, client.max),
        ))
    }
}

/// The ranges a broker advertised in its ApiVersions response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrokerVersions {
    ranges: BTreeMap<i16, (i16, i16)>,
}

impl BrokerVersions {
    /// From `(api key, min, max)` triples.
    pub fn from_ranges(ranges: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        BrokerVersions {
            ranges: ranges
                .into_iter()
                .map(|(k, lo, hi)| (k, (lo, hi)))
                .collect(),
        }
    }

    /// From a decoded ApiVersions response.
    pub fn from_response(r: &ApiVersionsResponse) -> Self {
        Self::from_ranges(
            r.api_keys
                .iter()
                .map(|a| (a.api_key, a.min_version, a.max_version)),
        )
    }

    /// The broker's range for `key`.
    pub fn range(&self, key: i16) -> Option<(i16, i16)> {
        self.ranges.get(&key).copied()
    }

    /// The version to use for `api` on this broker.
    pub fn version(&self, api: &ApiRange) -> Result<i16, KafkaError> {
        negotiate(api, self.range(api.key))
    }

    /// The negotiation table for every client API.
    pub fn table(&self) -> Vec<NegotiatedApi> {
        CLIENT_RANGES
            .iter()
            .map(|api| NegotiatedApi {
                api: *api,
                broker: self.range(api.key),
                version: self.version(api).ok(),
            })
            .collect()
    }
}

/// One row of [`BrokerVersions::table`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NegotiatedApi {
    /// The client range.
    pub api: ApiRange,
    /// The broker range (`None` = not offered).
    pub broker: Option<(i16, i16)>,
    /// The version in use (`None` = unsupported).
    pub version: Option<i16>,
}

impl std::fmt::Display for NegotiatedApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let broker = match self.broker {
            Some((lo, hi)) => format!("v{lo}..=v{hi}"),
            None => "-".to_owned(),
        };
        let used = match self.version {
            Some(v) => format!("v{v}"),
            None => "unsupported".to_owned(),
        };
        write!(
            f,
            "{:<16} key {:>2}  client v{}..=v{}  broker {:<11} -> {used}",
            self.api.name, self.api.key, self.api.min, self.api.max, broker
        )
    }
}

/// A `kafka-protocol` request the client can send.
pub trait ApiRequest: Encodable + HeaderVersion + Send + Sync + 'static {
    /// The client's range for this API.
    const API: ApiRange;
    /// Largest response frame accepted for this API (bytes, after the size prefix).
    const MAX_RESPONSE: usize = MAX_FRAME;
    /// The response type.
    type Response: Decodable + HeaderVersion + Send + 'static;
    /// Walk `body` (the response after its header) and reject counts or lengths that do not
    /// fit, before `kafka-protocol` decodes (and pre-allocates) anything.
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError>;
}

/// Control-plane responses are small; cap them well below the data frame limit.
const CONTROL_MAX: usize = 1 << 20;

impl ApiRequest for ApiVersionsRequest {
    const API: ApiRange = API_VERSIONS;
    const MAX_RESPONSE: usize = CONTROL_MAX;
    type Response = ApiVersionsResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        check_api_versions(body, version)
    }
}

impl ApiRequest for SaslHandshakeRequest {
    const API: ApiRange = SASL_HANDSHAKE;
    const MAX_RESPONSE: usize = CONTROL_MAX;
    type Response = SaslHandshakeResponse;
    fn check_response(body: &[u8], _version: i16) -> Result<(), KafkaError> {
        let mut w = WireCheck::new("SaslHandshake", body);
        w.i16()?;
        for _ in 0..w.array_len(false, 2)? {
            w.string(false)?;
        }
        Ok(())
    }
}

impl ApiRequest for SaslAuthenticateRequest {
    const API: ApiRange = SASL_AUTHENTICATE;
    const MAX_RESPONSE: usize = CONTROL_MAX;
    type Response = SaslAuthenticateResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        let flex = version >= 2;
        let mut w = WireCheck::new("SaslAuthenticate", body);
        w.i16()?;
        w.string(flex)?;
        w.bytes(flex)?;
        if version >= 1 {
            w.i64()?;
        }
        if flex {
            w.tagged_fields()?;
        }
        Ok(())
    }
}

impl ApiRequest for MetadataRequest {
    const API: ApiRange = METADATA;
    type Response = MetadataResponse;
    fn check_response(body: &[u8], version: i16) -> Result<(), KafkaError> {
        check_metadata(body, version)
    }
}

fn check_api_versions(body: &[u8], version: i16) -> Result<(), KafkaError> {
    let flex = version >= 3;
    let mut w = WireCheck::new("ApiVersions", body);
    w.i16()?;
    for _ in 0..w.array_len(flex, if flex { 7 } else { 6 })? {
        w.skip(6)?;
        if flex {
            w.tagged_fields()?;
        }
    }
    if version >= 1 {
        w.i32()?;
    }
    if flex {
        w.tagged_fields_with(|tag, f| {
            match tag {
                // SupportedFeatures / FinalizedFeatures: [name, i16, i16, tags]
                0 | 2 => {
                    for _ in 0..f.array_len(true, 6)? {
                        f.string(true)?;
                        f.skip(4)?;
                        f.tagged_fields()?;
                    }
                }
                1 => {
                    f.i64()?;
                }
                3 => {
                    f.skip(1)?;
                }
                _ => {}
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn check_metadata(body: &[u8], version: i16) -> Result<(), KafkaError> {
    let flex = version >= 9;
    let mut w = WireCheck::new("Metadata", body);
    if version >= 3 {
        w.i32()?;
    }
    for _ in 0..w.array_len(flex, 11)? {
        w.i32()?;
        w.string(flex)?;
        w.i32()?;
        if version >= 1 {
            w.string(flex)?;
        }
        if flex {
            w.tagged_fields()?;
        }
    }
    if version >= 2 {
        w.string(flex)?;
    }
    if version >= 1 {
        w.i32()?;
    }
    for _ in 0..w.array_len(flex, 6)? {
        w.i16()?;
        w.string(flex)?;
        if version >= 10 {
            w.skip(16)?;
        }
        if version >= 1 {
            w.skip(1)?;
        }
        for _ in 0..w.array_len(flex, if flex { 13 } else { 18 })? {
            w.i16()?;
            w.i32()?;
            w.i32()?;
            if version >= 7 {
                w.i32()?;
            }
            for _ in 0..w.array_len(flex, 4)? {
                w.i32()?;
            }
            for _ in 0..w.array_len(flex, 4)? {
                w.i32()?;
            }
            if version >= 5 {
                for _ in 0..w.array_len(flex, 4)? {
                    w.i32()?;
                }
            }
            if flex {
                w.tagged_fields()?;
            }
        }
        if version >= 8 {
            w.i32()?;
        }
        if flex {
            w.tagged_fields()?;
        }
    }
    if (8..=10).contains(&version) {
        w.i32()?;
    }
    if flex {
        w.tagged_fields()?;
    }
    Ok(())
}

/// Result of decoding an ApiVersions response to a request sent at some version.
#[derive(Debug)]
pub(crate) enum ApiVersionsOutcome {
    /// The broker answered at the requested version.
    Ok(ApiVersionsResponse),
    /// The broker refused the version (`UNSUPPORTED_VERSION`, answered in v0 form); retry at
    /// this version.
    Retry(i16),
}

fn broker_error(code: i16) -> KafkaError {
    KafkaError::Broker {
        api: API_VERSIONS.name.into(),
        code,
    }
}

/// Decode an ApiVersions response body. A broker that does not support `requested` answers
/// with a v0-form body carrying `UNSUPPORTED_VERSION` and its own ApiVersions range (KIP-511),
/// so a failed or refused decode is retried as v0.
pub(crate) fn decode_api_versions(
    body: Bytes,
    requested: i16,
) -> Result<ApiVersionsOutcome, KafkaError> {
    let at = |v: i16| -> Result<ApiVersionsResponse, KafkaError> {
        check_api_versions(&body, v)?;
        ApiVersionsResponse::decode(&mut body.clone(), v)
            .map_err(|e| KafkaError::Protocol(format!("ApiVersions v{v} response: {e}")))
    };
    let first = at(requested);
    match &first {
        Ok(r) if r.error_code == 0 => {
            return first.map(ApiVersionsOutcome::Ok);
        }
        Ok(r) if r.error_code != UNSUPPORTED_VERSION_CODE => {
            return Err(broker_error(r.error_code));
        }
        _ if requested == 0 => {
            return match first {
                Ok(r) => Err(broker_error(r.error_code)),
                Err(e) => Err(e),
            };
        }
        _ => {}
    }
    match at(0) {
        Ok(r) if r.error_code == UNSUPPORTED_VERSION_CODE => {
            let theirs = BrokerVersions::from_response(&r).range(API_VERSIONS.key);
            let v = match theirs {
                Some((lo, hi)) if hi >= lo && hi >= 0 => hi,
                _ => 0,
            };
            Ok(ApiVersionsOutcome::Retry(v.min(requested - 1)))
        }
        _ => match first {
            Err(e) => Err(e),
            Ok(r) => Err(broker_error(r.error_code)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Apache Kafka 4.1 ranges for the APIs the client uses.
    fn kafka_41() -> BrokerVersions {
        BrokerVersions::from_ranges([
            (18, 0, 4),
            (3, 0, 13),
            (10, 0, 6),
            (2, 1, 10),
            (1, 4, 17),
            (9, 1, 9),
            (8, 2, 9),
            (17, 0, 1),
            (36, 0, 2),
        ])
    }

    /// A Kafka 2.x-era broker.
    fn kafka_2x() -> BrokerVersions {
        BrokerVersions::from_ranges([
            (18, 0, 3),
            (3, 0, 9),
            (10, 0, 3),
            (2, 0, 5),
            (1, 0, 11),
            (9, 0, 7),
            (8, 0, 8),
            (17, 0, 1),
            (36, 0, 2),
        ])
    }

    #[test]
    fn kafka_41_uses_the_client_caps() {
        let b = kafka_41();
        assert_eq!(b.version(&API_VERSIONS).unwrap(), 3);
        assert_eq!(b.version(&METADATA).unwrap(), 12);
        assert_eq!(b.version(&FIND_COORDINATOR).unwrap(), 3);
        assert_eq!(b.version(&LIST_OFFSETS).unwrap(), 7);
        assert_eq!(b.version(&FETCH).unwrap(), 12);
        assert_eq!(b.version(&OFFSET_FETCH).unwrap(), 7);
        assert_eq!(b.version(&OFFSET_COMMIT).unwrap(), 8);
        assert_eq!(b.version(&SASL_HANDSHAKE).unwrap(), 1);
        assert_eq!(b.version(&SASL_AUTHENTICATE).unwrap(), 2);
        assert!(b.table().iter().all(|r| r.version.is_some()));
    }

    #[test]
    fn kafka_2x_uses_the_broker_caps() {
        let b = kafka_2x();
        assert_eq!(b.version(&METADATA).unwrap(), 9);
        assert_eq!(b.version(&LIST_OFFSETS).unwrap(), 5);
        assert_eq!(b.version(&FETCH).unwrap(), 11);
        assert_eq!(b.version(&OFFSET_FETCH).unwrap(), 7);
        assert_eq!(b.version(&OFFSET_COMMIT).unwrap(), 8);
    }

    #[test]
    fn missing_offset_commit_is_unsupported_version_naming_the_api() {
        let ranges = kafka_41()
            .ranges
            .into_iter()
            .filter(|(k, _)| *k != OFFSET_COMMIT.key)
            .map(|(k, (lo, hi))| (k, lo, hi));
        let b = BrokerVersions::from_ranges(ranges);
        assert_eq!(b.version(&FETCH).unwrap(), 12, "other APIs still negotiate");
        let msg = b.version(&OFFSET_COMMIT).unwrap_err().to_string();
        assert!(msg.starts_with("unsupported_version"), "{msg}");
        assert!(msg.contains("OffsetCommit"), "{msg}");
        let row = b
            .table()
            .into_iter()
            .find(|r| r.api.key == OFFSET_COMMIT.key)
            .unwrap();
        assert_eq!((row.broker, row.version), (None, None));
        assert!(row.to_string().contains("unsupported"));
    }

    #[test]
    fn broker_above_the_client_range_is_unsupported() {
        // A broker that dropped every Fetch version up to v12 (topic ids only).
        let b = BrokerVersions::from_ranges([(1, 13, 17)]);
        match b.version(&FETCH).unwrap_err() {
            KafkaError::UnsupportedVersion {
                api,
                broker_min,
                broker_max,
                client_min,
                client_max,
            } => {
                assert_eq!(api, "Fetch");
                assert_eq!(
                    (broker_min, broker_max, client_min, client_max),
                    (13, 17, 4, 12)
                );
            }
            other => panic!("{other:?}"),
        }
        // and one below the client minimum
        let b = BrokerVersions::from_ranges([(8, 0, 1)]);
        assert!(
            b.version(&OFFSET_COMMIT)
                .unwrap_err()
                .to_string()
                .contains("OffsetCommit")
        );
    }

    #[test]
    fn negotiate_edges() {
        let r = ApiRange {
            key: 99,
            name: "X",
            min: 2,
            max: 5,
        };
        assert_eq!(negotiate(&r, Some((5, 9))).unwrap(), 5);
        assert_eq!(negotiate(&r, Some((0, 2))).unwrap(), 2);
        assert!(negotiate(&r, Some((6, 9))).is_err());
        assert!(negotiate(&r, Some((0, 1))).is_err());
        assert!(negotiate(&r, Some((4, 3))).is_err(), "empty broker range");
        assert!(negotiate(&r, None).is_err());
    }

    fn v0_body(code: i16, keys: &[(i16, i16, i16)]) -> Bytes {
        let mut b = Vec::new();
        b.extend_from_slice(&code.to_be_bytes());
        b.extend_from_slice(&(keys.len() as i32).to_be_bytes());
        for (k, lo, hi) in keys {
            b.extend_from_slice(&k.to_be_bytes());
            b.extend_from_slice(&lo.to_be_bytes());
            b.extend_from_slice(&hi.to_be_bytes());
        }
        Bytes::from(b)
    }

    #[test]
    fn api_versions_fallback_on_unsupported_version() {
        // An old broker answers a v3 request in v0 form with UNSUPPORTED_VERSION and its range.
        match decode_api_versions(v0_body(35, &[(18, 0, 2)]), 3).unwrap() {
            ApiVersionsOutcome::Retry(v) => assert_eq!(v, 2),
            other => panic!("{other:?}"),
        }
        match decode_api_versions(v0_body(35, &[]), 3).unwrap() {
            ApiVersionsOutcome::Retry(v) => assert_eq!(v, 0),
            other => panic!("{other:?}"),
        }
        // A v0 answer at v0 decodes.
        match decode_api_versions(v0_body(0, &[(18, 0, 2), (3, 0, 9)]), 0).unwrap() {
            ApiVersionsOutcome::Ok(r) => {
                assert_eq!(BrokerVersions::from_response(&r).range(3), Some((0, 9)))
            }
            other => panic!("{other:?}"),
        }
        // Another error code is a broker error.
        assert!(matches!(
            decode_api_versions(v0_body(1, &[]), 0),
            Err(KafkaError::Broker { code: 1, .. })
        ));
    }

    #[test]
    fn api_versions_check_rejects_oversized_counts() {
        let mut b = Vec::new();
        b.extend_from_slice(&0i16.to_be_bytes());
        b.extend_from_slice(&i32::MAX.to_be_bytes());
        let e = decode_api_versions(Bytes::from(b), 0).unwrap_err();
        assert!(matches!(e, KafkaError::Protocol(_)), "{e:?}");
        // compact form: a huge varint count
        let b = [0u8, 0, 0xff, 0xff, 0xff, 0xff, 0x07];
        assert!(matches!(
            check_api_versions(&b, 3),
            Err(KafkaError::Protocol(_))
        ));
    }

    /// SplitMix64, for the garbage loop.
    fn mix(s: &mut u64) -> u64 {
        *s = s.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    #[test]
    fn garbage_and_truncated_bodies_never_panic() {
        let mut s = 7u64;
        for i in 0..20_000u32 {
            let len = (mix(&mut s) % 96) as usize;
            let body: Vec<u8> = (0..len).map(|_| mix(&mut s) as u8).collect();
            let v = (i % 4) as i16;
            let _ = decode_api_versions(Bytes::from(body.clone()), v);
            let mv = (i % 12 + 1) as i16;
            if check_metadata(&body, mv).is_ok() {
                let _ = MetadataResponse::decode(&mut Bytes::from(body.clone()), mv);
            }
            if SaslHandshakeRequest::check_response(&body, 1).is_ok() {
                let _ = SaslHandshakeResponse::decode(&mut Bytes::from(body.clone()), 1);
            }
            if SaslAuthenticateRequest::check_response(&body, v % 3).is_ok() {
                let _ = SaslAuthenticateResponse::decode(&mut Bytes::from(body), v % 3);
            }
        }
        // every truncation of a valid v0 body
        let full = v0_body(0, &[(18, 0, 3), (3, 1, 12), (1, 4, 12)]);
        for n in 0..full.len() {
            assert!(
                decode_api_versions(full.slice(..n), 0).is_err(),
                "truncated at {n}"
            );
        }
    }
}
