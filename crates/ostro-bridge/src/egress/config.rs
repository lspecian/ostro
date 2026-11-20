//! The `[egress]` node config section (06-DECISIONS, RESERVATIONS §11).
//!
//! Durations are friendly strings (`"5s"`, `"24h"`, `"1m 30s"`) parsed with the same jiff
//! parser `ServerConfig` uses. Byte sizes are integers or strings with a binary/decimal unit
//! (`"4MiB"`, `"512KiB"`, `"1MB"`). 06-07 and 06-10 read their defaults from here (W1).

use std::fmt;
use std::time::Duration;

use ipnet::IpNet;
use jiff::fmt::friendly::SpanParser;
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

static PARSER: SpanParser = SpanParser::new();

/// 1 MiB.
const MIB: u64 = 1024 * 1024;

/// The whole `[egress]` section.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EgressConfig {
    /// CIDRs exempt from the private/special-purpose block (default empty, P-6). Tests that
    /// talk to loopback mocks set `["127.0.0.0/8"]` explicitly (Pitfall 8).
    #[serde(deserialize_with = "de_nets")]
    pub allow_private: Vec<IpNet>,
    /// TCP connect timeout of the egress clients (default 5 s).
    #[serde(deserialize_with = "de_duration")]
    pub connect_timeout: Duration,
    /// Per-attempt request timeout when the effect sets none (default 30 s).
    #[serde(deserialize_with = "de_duration")]
    pub default_timeout: Duration,
    /// Response body cap when the effect or target sets none (default 4 MiB).
    #[serde(deserialize_with = "de_bytes")]
    pub max_response_bytes: u64,
    /// Upper bound for any per-effect or per-target body cap (default 64 MiB).
    #[serde(deserialize_with = "de_bytes")]
    pub hard_max_response_bytes: u64,
    /// Redirect hops the HTTP connector follows for untemplated requests (default 5, P-5).
    pub max_redirects: u8,
    /// Longest wait for a rate-limit permit before `rate_limited` (default 30 s).
    #[serde(deserialize_with = "de_duration")]
    pub rate_max_wait: Duration,
    /// Default provider idempotency window (default 24 h).
    #[serde(deserialize_with = "de_duration")]
    pub idempotency_window: Duration,
    /// Margin subtracted from the idempotency window before a re-send is refused (default 5 m).
    #[serde(deserialize_with = "de_duration")]
    pub safety_margin: Duration,
}

impl Default for EgressConfig {
    fn default() -> Self {
        Self {
            allow_private: Vec::new(),
            connect_timeout: Duration::from_secs(5),
            default_timeout: Duration::from_secs(30),
            max_response_bytes: 4 * MIB,
            hard_max_response_bytes: 64 * MIB,
            max_redirects: 5,
            rate_max_wait: Duration::from_secs(30),
            idempotency_window: Duration::from_secs(24 * 3600),
            safety_margin: Duration::from_secs(5 * 60),
        }
    }
}

impl EgressConfig {
    /// Checks cross-field invariants (`max_response_bytes <= hard_max_response_bytes`,
    /// non-zero timeouts and caps).
    pub fn validate(&self) -> Result<(), String> {
        if self.max_response_bytes == 0 || self.hard_max_response_bytes == 0 {
            return Err("egress: response byte caps must be non-zero".into());
        }
        if self.max_response_bytes > self.hard_max_response_bytes {
            return Err(format!(
                "egress: max_response_bytes ({}) exceeds hard_max_response_bytes ({})",
                self.max_response_bytes, self.hard_max_response_bytes
            ));
        }
        if self.connect_timeout.is_zero() || self.default_timeout.is_zero() {
            return Err("egress: connect_timeout and default_timeout must be non-zero".into());
        }
        Ok(())
    }

    /// The effective body cap for a request that asks for `requested` bytes (or none):
    /// the requested value, else `max_response_bytes`, never above `hard_max_response_bytes`.
    pub fn response_cap(&self, requested: Option<u64>) -> u64 {
        requested
            .unwrap_or(self.max_response_bytes)
            .min(self.hard_max_response_bytes)
    }
}

/// Parses a friendly duration (`"5s"`, `"24h"`).
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    PARSER.parse_unsigned_duration(s.trim()).map_err(|e| {
        format!("invalid duration {s:?} (expected e.g. \"30s\", \"5m\", \"24h\"): {e}")
    })
}

/// Parses a byte size: digits with an optional unit `B`, `KiB`, `MiB`, `GiB`, `KB`, `MB`,
/// `GB` (case-insensitive, optional space).
pub fn parse_bytes(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid byte size {s:?} (expected e.g. \"4MiB\")"))?;
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" => 1024,
        "mib" => MIB,
        "gib" => 1024 * MIB,
        "kb" => 1000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        _ => return Err(format!("invalid byte size unit in {s:?}")),
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("byte size {s:?} is too large"))
}

fn de_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let s = String::deserialize(d)?;
    parse_duration(&s).map_err(de::Error::custom)
}

fn de_nets<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<IpNet>, D::Error> {
    let v = Vec::<String>::deserialize(d)?;
    v.iter()
        .map(|s| {
            let s = s.trim();
            s.parse::<IpNet>()
                .or_else(|_| s.parse::<std::net::IpAddr>().map(IpNet::from))
                .map_err(|_| de::Error::custom(format!("allow_private: invalid CIDR {s:?}")))
        })
        .collect()
}

fn de_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    struct V;
    impl Visitor<'_> for V {
        type Value = u64;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a byte count or a size string such as \"4MiB\"")
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("byte size must not be negative"))
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<u64, E> {
            parse_bytes(v).map_err(E::custom)
        }
    }
    d.deserialize_any(V)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_decisions() {
        let c = EgressConfig::default();
        assert!(
            c.allow_private.is_empty(),
            "default must block loopback (Pitfall 8)"
        );
        assert_eq!(c.connect_timeout, Duration::from_secs(5));
        assert_eq!(c.default_timeout, Duration::from_secs(30));
        assert_eq!(c.max_response_bytes, 4 * MIB);
        assert_eq!(c.hard_max_response_bytes, 64 * MIB);
        assert_eq!(c.max_redirects, 5);
        assert_eq!(c.rate_max_wait, Duration::from_secs(30));
        assert_eq!(c.idempotency_window, Duration::from_secs(86_400));
        assert_eq!(c.safety_margin, Duration::from_secs(300));
        c.validate().unwrap();
    }

    #[test]
    fn every_key_parses() {
        // The `[egress]` table as a serde map (06-07 feeds it from the TOML server config;
        // the derive is format-independent).
        let v = serde_json::json!({
            "allow_private": ["127.0.0.0/8", "10.20.0.0/16", "::1"],
            "connect_timeout": "2s",
            "default_timeout": "1m 30s",
            "max_response_bytes": "8MiB",
            "hard_max_response_bytes": 33554432u64,
            "max_redirects": 3,
            "rate_max_wait": "250ms",
            "idempotency_window": "48h",
            "safety_margin": "10m",
        });
        let c: EgressConfig = serde_json::from_value(v).unwrap();
        assert_eq!(
            c.allow_private,
            vec![
                "127.0.0.0/8".parse::<IpNet>().unwrap(),
                "10.20.0.0/16".parse().unwrap(),
                "::1/128".parse().unwrap()
            ]
        );
        assert_eq!(c.connect_timeout, Duration::from_secs(2));
        assert_eq!(c.default_timeout, Duration::from_secs(90));
        assert_eq!(c.max_response_bytes, 8 * MIB);
        assert_eq!(c.hard_max_response_bytes, 32 * MIB);
        assert_eq!(c.max_redirects, 3);
        assert_eq!(c.rate_max_wait, Duration::from_millis(250));
        assert_eq!(c.idempotency_window, Duration::from_secs(48 * 3600));
        assert_eq!(c.safety_margin, Duration::from_secs(600));
        c.validate().unwrap();
    }

    #[test]
    fn empty_table_is_default_and_unknown_keys_fail() {
        let c: EgressConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(c, EgressConfig::default());
        assert!(
            serde_json::from_value::<EgressConfig>(serde_json::json!({"allow_privat": []}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<EgressConfig>(
                serde_json::json!({"allow_private": ["10.0.0.0/33"]})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<EgressConfig>(serde_json::json!({"connect_timeout": "soon"}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<EgressConfig>(
                serde_json::json!({"max_response_bytes": "4 parsecs"})
            )
            .is_err()
        );
    }

    #[test]
    fn validate_and_caps() {
        let c = EgressConfig {
            max_response_bytes: 65 * MIB,
            ..EgressConfig::default()
        };
        assert!(c.validate().is_err());
        let d = EgressConfig::default();
        assert_eq!(d.response_cap(None), 4 * MIB);
        assert_eq!(d.response_cap(Some(1)), 1);
        assert_eq!(d.response_cap(Some(u64::MAX)), 64 * MIB);
        assert_eq!(parse_bytes("512 KiB").unwrap(), 512 * 1024);
        assert_eq!(parse_bytes("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_bytes("42").unwrap(), 42);
    }
}
