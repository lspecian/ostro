//! Per-destination HTTP config: the `http-target` connection kind (RESERVATIONS §11).
//!
//! `Connection{kind: "http-target", name: <normalised origin>, config: JSON}` with
//!
//! ```json
//! {"v":1, "idempotency_window":"24h", "idempotency_header":"Idempotency-Key",
//!  "resend":"window", "force_window":false, "safety_margin":"5m",
//!  "rate":"100/s", "burst":100, "max_response_bytes":4194304, "timeout":"30s"}
//! ```
//!
//! Every field is optional. Defaults come from `[egress]` ([`EgressConfig`]):
//! `timeout` ← `default_timeout`, `idempotency_window` ← `idempotency_window`,
//! `safety_margin` ← `safety_margin`, `max_response_bytes` ← `max_response_bytes` (always
//! clamped to `hard_max_response_bytes`). `idempotency_header` defaults to
//! `Idempotency-Key`; `null` or `""` suppresses the header. `resend` is `"window"` (default)
//! or `"never"`. No `rate` means no per-target quota (the node default applies, if any).
//!
//! An unknown `"v"`, an unknown key or an invalid value is an error, surfaced to the guest as
//! `NonRetryable{connection_unknown}`: a misconfigured destination must not silently fall
//! back to defaults (the window decides whether a payment is re-sent).

use std::time::Duration;

use ostro_meta::records::Connection;
use ostro_types::journal::Failure;
use serde_json::Value;

use crate::egress::EgressConfig;
use crate::egress::config::{parse_bytes, parse_duration};
use crate::kinds::CONNECTION_UNKNOWN;
use crate::outcome::non_retryable;
use crate::ratelimit::QuotaSpec;

use super::key::DEFAULT_HEADER;

/// The connection kind of a per-destination HTTP config.
pub const HTTP_TARGET_KIND: &str = "http-target";

/// Whether an unfinished non-idempotent request may be re-sent at re-drive time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResendMode {
    /// Re-send within the idempotency window (default).
    Window,
    /// Never re-send: always `effect_outcome_unknown`.
    Never,
}

/// The effective config of one destination.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpTarget {
    /// How long the provider remembers a key.
    pub idempotency_window: Duration,
    /// The key header name; `None` = suppressed.
    pub idempotency_header: Option<String>,
    /// Re-send mode for non-idempotent methods.
    pub resend: ResendMode,
    /// Apply the window to idempotent methods too.
    pub force_window: bool,
    /// Subtracted from the window (clock skew, provider pruning slack).
    pub safety_margin: Duration,
    /// Per-target quota.
    pub quota: Option<QuotaSpec>,
    /// Response body cap, bytes (already clamped to the hard max).
    pub max_response_bytes: u64,
    /// Per-attempt timeout.
    pub timeout: Duration,
}

/// An `http-target` config could not be used.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("http-target {name:?} is invalid: {reason}")]
pub struct TargetError {
    /// The connection name.
    pub name: String,
    /// Why.
    pub reason: String,
}

impl TargetError {
    /// `NonRetryable{connection_unknown}`.
    pub fn to_failure(&self) -> Failure {
        non_retryable(
            CONNECTION_UNKNOWN,
            self.to_string(),
            Some(serde_json::json!({ "connection": self.name })),
        )
    }
}

const KEYS: &[&str] = &[
    "v",
    "idempotency_window",
    "idempotency_header",
    "resend",
    "force_window",
    "safety_margin",
    "rate",
    "burst",
    "max_response_bytes",
    "timeout",
];

impl HttpTarget {
    /// The config of a destination without an `http-target` connection.
    pub fn defaults(cfg: &EgressConfig) -> Self {
        Self {
            idempotency_window: cfg.idempotency_window,
            idempotency_header: Some(DEFAULT_HEADER.to_owned()),
            resend: ResendMode::Window,
            force_window: false,
            safety_margin: cfg.safety_margin,
            quota: None,
            max_response_bytes: cfg.response_cap(None),
            timeout: cfg.default_timeout,
        }
    }

    /// Parses `conn` (an `http-target` connection), or the defaults when `None`.
    pub fn from_connection(
        conn: Option<&Connection>,
        cfg: &EgressConfig,
    ) -> Result<Self, TargetError> {
        let mut t = Self::defaults(cfg);
        let Some(conn) = conn else {
            return Ok(t);
        };
        let err = |reason: String| TargetError {
            name: conn.name.clone(),
            reason,
        };
        if conn.kind != HTTP_TARGET_KIND {
            return Err(err(format!(
                "connection kind is {:?}, expected {HTTP_TARGET_KIND:?}",
                conn.kind
            )));
        }
        let v: Value = serde_json::from_slice(&conn.config)
            .map_err(|e| err(format!("config is not JSON: {e}")))?;
        let obj = v
            .as_object()
            .ok_or_else(|| err("config is not a JSON object".into()))?;
        if let Some(k) = obj.keys().find(|k| !KEYS.contains(&k.as_str())) {
            return Err(err(format!("unknown key {k:?}")));
        }
        match obj.get("v") {
            Some(Value::Number(n)) if n.as_u64() == Some(1) => {}
            Some(other) => return Err(err(format!("unsupported config version {other}"))),
            None => return Err(err("missing config version \"v\"".into())),
        }
        let dur = |k: &str| -> Result<Option<Duration>, TargetError> {
            match obj.get(k) {
                None => Ok(None),
                Some(Value::String(s)) => parse_duration(s).map(Some).map_err(err),
                Some(_) => Err(err(format!("{k} must be a duration string"))),
            }
        };
        if let Some(d) = dur("idempotency_window")? {
            t.idempotency_window = d;
        }
        if let Some(d) = dur("safety_margin")? {
            t.safety_margin = d;
        }
        if let Some(d) = dur("timeout")? {
            if d.is_zero() {
                return Err(err("timeout must be positive".into()));
            }
            t.timeout = d;
        }
        match obj.get("idempotency_header") {
            None => {}
            Some(Value::Null) => t.idempotency_header = None,
            Some(Value::String(s)) if s.is_empty() => t.idempotency_header = None,
            Some(Value::String(s)) => {
                reqwest::header::HeaderName::from_bytes(s.as_bytes())
                    .map_err(|_| err(format!("invalid idempotency_header {s:?}")))?;
                t.idempotency_header = Some(s.clone());
            }
            Some(_) => return Err(err("idempotency_header must be a string or null".into())),
        }
        match obj.get("resend") {
            None => {}
            Some(Value::String(s)) if s == "window" => t.resend = ResendMode::Window,
            Some(Value::String(s)) if s == "never" => t.resend = ResendMode::Never,
            Some(other) => {
                return Err(err(format!(
                    "resend must be \"window\" or \"never\", got {other}"
                )));
            }
        }
        match obj.get("force_window") {
            None => {}
            Some(Value::Bool(b)) => t.force_window = *b,
            Some(_) => return Err(err("force_window must be a boolean".into())),
        }
        let burst = match obj.get("burst") {
            None => None,
            Some(Value::Number(n)) => Some(
                n.as_u64()
                    .and_then(|b| u32::try_from(b).ok())
                    .ok_or_else(|| err("burst must be a positive integer".into()))?,
            ),
            Some(_) => return Err(err("burst must be a positive integer".into())),
        };
        match obj.get("rate") {
            None if burst.is_some() => return Err(err("burst without rate".into())),
            None => {}
            Some(Value::String(s)) => {
                t.quota =
                    Some(QuotaSpec::parse_with_burst(s, burst).map_err(|e| err(e.to_string()))?);
            }
            Some(_) => return Err(err("rate must be a string like \"100/s\"".into())),
        }
        match obj.get("max_response_bytes") {
            None => {}
            Some(Value::Number(n)) => {
                let n = n
                    .as_u64()
                    .ok_or_else(|| err("max_response_bytes must be a positive integer".into()))?;
                t.max_response_bytes = cfg.response_cap(Some(n));
            }
            Some(Value::String(s)) => {
                let n = parse_bytes(s).map_err(err)?;
                t.max_response_bytes = cfg.response_cap(Some(n));
            }
            Some(_) => return Err(err("max_response_bytes must be an integer or size".into())),
        }
        Ok(t)
    }

    /// The window as nanoseconds (saturating).
    pub fn window_nanos(&self) -> u64 {
        u64::try_from(self.idempotency_window.as_nanos()).unwrap_or(u64::MAX)
    }

    /// The safety margin as nanoseconds (saturating).
    pub fn margin_nanos(&self) -> u64 {
        u64::try_from(self.safety_margin.as_nanos()).unwrap_or(u64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(cfg: &str) -> Connection {
        Connection {
            name: "https://api.stripe.com".into(),
            kind: HTTP_TARGET_KIND.into(),
            config: cfg.as_bytes().to_vec(),
            ext: vec![],
        }
    }

    fn egress() -> EgressConfig {
        EgressConfig {
            default_timeout: Duration::from_secs(7),
            idempotency_window: Duration::from_secs(3600),
            safety_margin: Duration::from_secs(60),
            max_response_bytes: 1000,
            hard_max_response_bytes: 5000,
            ..EgressConfig::default()
        }
    }

    #[test]
    fn defaults_come_from_egress_config() {
        let t = HttpTarget::from_connection(None, &egress()).unwrap();
        assert_eq!(t.timeout, Duration::from_secs(7));
        assert_eq!(t.idempotency_window, Duration::from_secs(3600));
        assert_eq!(t.safety_margin, Duration::from_secs(60));
        assert_eq!(t.max_response_bytes, 1000);
        assert_eq!(t.idempotency_header.as_deref(), Some("Idempotency-Key"));
        assert_eq!(t.resend, ResendMode::Window);
        assert!(!t.force_window);
        assert_eq!(t.quota, None);
        // A minimal config is the defaults.
        let m = HttpTarget::from_connection(Some(&conn(r#"{"v":1}"#)), &egress()).unwrap();
        assert_eq!(m, t);
    }

    #[test]
    fn every_field_parses() {
        let t = HttpTarget::from_connection(
            Some(&conn(
                r#"{"v":1,"idempotency_window":"48h","idempotency_header":"PayPal-Request-Id",
                "resend":"never","force_window":true,"safety_margin":"10m","rate":"100/s",
                "burst":20,"max_response_bytes":4096,"timeout":"3s"}"#,
            )),
            &egress(),
        )
        .unwrap();
        assert_eq!(t.idempotency_window, Duration::from_secs(48 * 3600));
        assert_eq!(t.idempotency_header.as_deref(), Some("PayPal-Request-Id"));
        assert_eq!(t.resend, ResendMode::Never);
        assert!(t.force_window);
        assert_eq!(t.safety_margin, Duration::from_secs(600));
        assert_eq!(
            t.quota,
            Some(QuotaSpec {
                per_second: 100,
                burst: 20
            })
        );
        assert_eq!(t.max_response_bytes, 4096);
        assert_eq!(t.timeout, Duration::from_secs(3));
    }

    #[test]
    fn header_can_be_suppressed_and_cap_is_clamped() {
        for cfg in [
            r#"{"v":1,"idempotency_header":null}"#,
            r#"{"v":1,"idempotency_header":""}"#,
        ] {
            let t = HttpTarget::from_connection(Some(&conn(cfg)), &egress()).unwrap();
            assert_eq!(t.idempotency_header, None, "{cfg}");
        }
        let t = HttpTarget::from_connection(
            Some(&conn(r#"{"v":1,"max_response_bytes":"1GiB"}"#)),
            &egress(),
        )
        .unwrap();
        assert_eq!(t.max_response_bytes, 5000);
    }

    #[test]
    fn invalid_configs_are_connection_unknown() {
        for cfg in [
            r#"{"v":2}"#,
            r#"{}"#,
            r#"[1]"#,
            r#"not json"#,
            r#"{"v":1,"windw":"1h"}"#,
            r#"{"v":1,"idempotency_window":"forever"}"#,
            r#"{"v":1,"resend":"sometimes"}"#,
            r#"{"v":1,"rate":"fast"}"#,
            r#"{"v":1,"burst":5}"#,
            r#"{"v":1,"idempotency_header":"bad header"}"#,
            r#"{"v":1,"timeout":"0s"}"#,
        ] {
            let e = HttpTarget::from_connection(Some(&conn(cfg)), &egress()).unwrap_err();
            match e.to_failure() {
                Failure::NonRetryable(i) => assert_eq!(i.kind, CONNECTION_UNKNOWN, "{cfg}"),
                other => panic!("{cfg}: {other:?}"),
            }
        }
        let mut wrong_kind = conn(r#"{"v":1}"#);
        wrong_kind.kind = "sql-postgres".into();
        assert!(HttpTarget::from_connection(Some(&wrong_kind), &egress()).is_err());
    }
}
