//! Bridge node config (RESERVATIONS §11). This module owns the `[connectors]` section
//! ([`ConnectorsConfig`], 06-07). `[egress]` is [`EgressConfig`](crate::egress::EgressConfig)
//! (06-04) and `[secrets]` is [`SecretsConfig`](crate::secrets::SecretsConfig) (06-05); both are
//! re-exported here so the server can embed all three from one place.
//!
//! `rate_max_wait` belongs to `[egress]` and is passed to [`Bridge::new`](crate::driver::Bridge)
//! from `EgressConfig`; it is not duplicated here (W1).
//!
//! Durations and byte sizes use the 06-04 parsers ([`parse_duration`], [`parse_bytes`]), the same
//! jiff friendly parser `ServerConfig` uses. Write hours, not days (`"168h"`, D9).

use std::time::Duration;

use serde::Deserialize;
use serde::de::{self, Deserializer};

pub use crate::egress::EgressConfig;
pub use crate::egress::config::{parse_bytes, parse_duration};
pub use crate::secrets::SecretsConfig;

use crate::ratelimit::QuotaSpec;

const MIB: u64 = 1024 * 1024;

/// The `[connectors]` section (06-DECISIONS).
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConnectorsConfig {
    /// Row cap of one SQL result (default 10000).
    pub sql_max_rows: u64,
    /// Byte cap of one SQL result (default 4 MiB).
    #[serde(deserialize_with = "de_bytes")]
    pub sql_max_bytes: u64,
    /// Retention of `ostro_effects` token rows in user databases (default 168 h).
    #[serde(deserialize_with = "de_duration")]
    pub sql_effects_retention: Duration,
    /// Byte cap of one object `get` (default 16 MiB).
    #[serde(deserialize_with = "de_bytes")]
    pub object_max_get_bytes: u64,
    /// Default expiry of presigned URLs (default 15 m).
    #[serde(deserialize_with = "de_duration")]
    pub presign_default_expiry: Duration,
    /// Idle TTL of cached connection backends (pools, object clients; default 5 m).
    #[serde(deserialize_with = "de_duration")]
    pub backend_ttl: Duration,
    /// Rate applied to targets without their own quota (`"100/s"`, `"6000/m"`); unset =
    /// unlimited.
    #[serde(deserialize_with = "de_quota")]
    pub default_rate: Option<QuotaSpec>,
    /// Base URL in local presigned URLs; unset = the API listen address.
    #[serde(deserialize_with = "de_url")]
    pub public_base_url: Option<String>,
}

impl Default for ConnectorsConfig {
    fn default() -> Self {
        Self {
            sql_max_rows: 10_000,
            sql_max_bytes: 4 * MIB,
            sql_effects_retention: Duration::from_secs(168 * 3600),
            object_max_get_bytes: 16 * MIB,
            presign_default_expiry: Duration::from_secs(15 * 60),
            backend_ttl: Duration::from_secs(5 * 60),
            default_rate: None,
            public_base_url: None,
        }
    }
}

impl ConnectorsConfig {
    /// Checks the caps and durations are non-zero.
    pub fn validate(&self) -> Result<(), String> {
        if self.sql_max_rows == 0 || self.sql_max_bytes == 0 || self.object_max_get_bytes == 0 {
            return Err("connectors: row and byte caps must be non-zero".into());
        }
        if self.presign_default_expiry.is_zero() || self.backend_ttl.is_zero() {
            return Err(
                "connectors: presign_default_expiry and backend_ttl must be non-zero".into(),
            );
        }
        Ok(())
    }
}

fn de_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let s = String::deserialize(d)?;
    parse_duration(&s).map_err(de::Error::custom)
}

fn de_bytes<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        N(u64),
        S(String),
    }
    match Raw::deserialize(d)? {
        Raw::N(n) => Ok(n),
        Raw::S(s) => parse_bytes(&s).map_err(de::Error::custom),
    }
}

fn de_quota<'de, D: Deserializer<'de>>(d: D) -> Result<Option<QuotaSpec>, D::Error> {
    Option::<String>::deserialize(d)?
        .map(|s| QuotaSpec::parse(&s).map_err(de::Error::custom))
        .transpose()
}

fn de_url<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    let Some(s) = Option::<String>::deserialize(d)? else {
        return Ok(None);
    };
    let u = url::Url::parse(s.trim())
        .map_err(|e| de::Error::custom(format!("public_base_url: invalid URL {s:?}: {e}")))?;
    if !matches!(u.scheme(), "http" | "https") {
        return Err(de::Error::custom(
            "public_base_url: scheme must be http or https",
        ));
    }
    Ok(Some(u.as_str().trim_end_matches('/').to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_match_decisions() {
        let c = ConnectorsConfig::default();
        assert_eq!(c.sql_max_rows, 10_000);
        assert_eq!(c.sql_max_bytes, 4 * MIB);
        assert_eq!(c.sql_effects_retention, Duration::from_secs(604_800));
        assert_eq!(c.object_max_get_bytes, 16 * MIB);
        assert_eq!(c.presign_default_expiry, Duration::from_secs(900));
        assert_eq!(c.backend_ttl, Duration::from_secs(300));
        assert_eq!(c.default_rate, None);
        assert_eq!(c.public_base_url, None);
        c.validate().unwrap();
        let empty: ConnectorsConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, c);
    }

    #[test]
    fn config_every_key_parses() {
        let c: ConnectorsConfig = serde_json::from_value(serde_json::json!({
            "sql_max_rows": 50,
            "sql_max_bytes": "1MiB",
            "sql_effects_retention": "48h",
            "object_max_get_bytes": 1024,
            "presign_default_expiry": "1m 30s",
            "backend_ttl": "10m",
            "default_rate": "100/s",
            "public_base_url": "https://ostro.example.com/",
        }))
        .unwrap();
        assert_eq!(c.sql_max_rows, 50);
        assert_eq!(c.sql_max_bytes, MIB);
        assert_eq!(c.sql_effects_retention, Duration::from_secs(48 * 3600));
        assert_eq!(c.object_max_get_bytes, 1024);
        assert_eq!(c.presign_default_expiry, Duration::from_secs(90));
        assert_eq!(c.backend_ttl, Duration::from_secs(600));
        assert_eq!(
            c.default_rate,
            Some(QuotaSpec {
                per_second: 100,
                burst: 100
            })
        );
        assert_eq!(
            c.public_base_url.as_deref(),
            Some("https://ostro.example.com")
        );
    }

    #[test]
    fn config_rejects_bad_values() {
        for bad in [
            serde_json::json!({"unknown_key": 1}),
            serde_json::json!({"sql_effects_retention": "7d"}),
            serde_json::json!({"sql_max_bytes": "4XB"}),
            serde_json::json!({"default_rate": "10/h"}),
            serde_json::json!({"public_base_url": "ftp://x"}),
            serde_json::json!({"public_base_url": "not a url"}),
        ] {
            assert!(
                serde_json::from_value::<ConnectorsConfig>(bad.clone()).is_err(),
                "{bad}"
            );
        }
        let zero: ConnectorsConfig =
            serde_json::from_value(serde_json::json!({"sql_max_rows": 0})).unwrap();
        assert!(zero.validate().is_err());
    }
}
