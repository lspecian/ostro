//! SSRF-hardened egress (06-04, FX-08).
//!
//! The guarantees here come from *where* checks happen, not from string matching:
//!
//! 1. **URL gate** ([`url_gate`], via [`Egress::check`]): WHATWG parse, userinfo / scheme /
//!    port / placeholder rejection, the per-module allow-list ([`policy`]) on the normalised
//!    host, and classification of IP-literal hosts (hyper-util never resolves literals, so
//!    this is the only place they can be stopped).
//! 2. **Resolver** ([`resolver::EgressResolver`]): every A/AAAA answer is classified; any
//!    blocked answer fails the whole name; the vetted addresses are the only ones hyper
//!    connects to (pinned connect).
//! 3. **Classifier** ([`classify`]): the IANA special-purpose tables with IPv4-mapped,
//!    IPv4-compatible, NAT64 and 6to4 unwrapping, and the node-level `allow_private`
//!    exemptions ([`EgressConfig::allow_private`], P-6).

pub mod classify;
pub mod config;
pub mod policy;
pub mod resolver;
pub mod url_gate;

use std::error::Error as StdError;
use std::net::IpAddr;
use std::sync::Arc;

use ipnet::IpNet;
use ostro_meta::records::EgressPolicy;
use ostro_types::journal::{ErrorInfo, Failure, Payload};

pub use classify::{effective_v4, is_blocked};
pub use config::EgressConfig;
pub use policy::{AllowRule, RuleError, allows_connection, parse_rule};
pub use resolver::{EgressResolver, HickoryLookup, Lookup, ResolveDenied, StaticLookup};

use crate::origin::Origin;

/// Why a destination was refused. Messages name the rule, never a secret or a header.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Denied {
    /// The URL does not parse (or has an invalid host).
    #[error("the URL is invalid: {0}")]
    Parse(String),
    /// The scheme is not https (or http where the matching rule says `http://`).
    #[error("the URL scheme is not allowed by the module's egress policy")]
    Scheme,
    /// The URL carries userinfo (`user:pw@`).
    #[error("URLs with userinfo are not allowed")]
    Userinfo,
    /// The port is not the one the matching rule allows.
    #[error("the URL port is not allowed by the module's egress policy")]
    Port,
    /// A `{{secret:..}}` placeholder appears in the URL (D-FX4).
    #[error("secret placeholders are not allowed in the URL")]
    Placeholder,
    /// No rule of the module's egress policy matches (or the module has no policy).
    #[error("the destination is not allowed by the module's egress policy")]
    NotAllowed,
    /// An IP-literal host that no IP/CIDR rule lists.
    #[error("IP-literal destination {0} is not listed in the module's egress policy")]
    IpLiteral(IpAddr),
    /// The destination is (or resolves to) a blocked address.
    #[error("the destination resolves to blocked address {0}")]
    Blocked(IpAddr),
    /// DNS resolution failed or returned no address.
    #[error("DNS resolution failed: {0}")]
    Dns(String),
}

impl Denied {
    /// The snake_case reason carried in `details.reason`.
    pub fn reason(&self) -> &'static str {
        match self {
            Denied::Parse(_) => "parse",
            Denied::Scheme => "scheme",
            Denied::Userinfo => "userinfo",
            Denied::Port => "port",
            Denied::Placeholder => "placeholder",
            Denied::NotAllowed => "not_allowed",
            Denied::IpLiteral(_) => "ip_literal",
            Denied::Blocked(_) => "blocked",
            Denied::Dns(_) => "dns",
        }
    }

    /// `NonRetryable{kind: egress_denied}` with JSON details `{"reason": .., "ip"?: ..}`.
    pub fn to_failure(&self) -> Failure {
        let mut details = serde_json::json!({ "reason": self.reason() });
        if let Denied::IpLiteral(ip) | Denied::Blocked(ip) = self {
            details["ip"] = serde_json::Value::String(ip.to_string());
        }
        Failure::NonRetryable(ErrorInfo {
            kind: crate::kinds::EGRESS_DENIED.to_owned(),
            message: format!("egress denied: {self}"),
            details: Some(Payload {
                content_type: "application/json".to_owned(),
                data: serde_json::to_vec(&details).expect("JSON object"),
            }),
        })
    }

    /// The [`Denied`] a request failed with, if its error chain holds a
    /// [`ResolveDenied`] (a name resolved to a blocked address, or DNS failed inside the
    /// egress resolver). Connectors map such transport errors to `egress_denied` (W2).
    pub fn from_error(err: &(dyn StdError + 'static)) -> Option<Denied> {
        let mut cur: Option<&(dyn StdError + 'static)> = Some(err);
        while let Some(e) = cur {
            if let Some(d) = e.downcast_ref::<ResolveDenied>() {
                return Some(d.denied.clone());
            }
            cur = e.source();
        }
        None
    }
}

/// A URL that passed the gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedUrl {
    /// The normalised URL to send (trailing host dot removed).
    pub url: url::Url,
    /// Its origin.
    pub origin: Origin,
    /// The allow-list rule that matched.
    pub rule: AllowRule,
}

/// Building the egress layer failed.
#[derive(Debug, thiserror::Error)]
pub enum EgressError {
    /// The `[egress]` section is inconsistent.
    #[error("{0}")]
    Config(String),
    /// reqwest refused the client configuration.
    #[error("building the egress HTTP client failed: {0}")]
    Client(#[from] reqwest::Error),
}

/// The egress layer: one URL gate, one resolver, one guarded client.
pub struct Egress {
    cfg: EgressConfig,
    allow_private: Arc<[IpNet]>,
    lookup: Arc<dyn Lookup>,
}

impl Egress {
    /// Validates `cfg` and builds the egress layer over `lookup` ([`HickoryLookup`] in
    /// production, [`StaticLookup`] in tests).
    pub fn new(cfg: EgressConfig, lookup: Arc<dyn Lookup>) -> Result<Self, EgressError> {
        cfg.validate().map_err(EgressError::Config)?;
        let allow_private: Arc<[IpNet]> = cfg.allow_private.clone().into();
        Ok(Self {
            cfg,
            allow_private,
            lookup,
        })
    }

    /// The `[egress]` section this layer was built with.
    pub fn config(&self) -> &EgressConfig {
        &self.cfg
    }

    /// The URL gate: placeholder, parse, userinfo, scheme, allow-list (deny by default),
    /// port, and IP-literal classification. Names are vetted later by the resolver.
    pub fn check(&self, policy: Option<&EgressPolicy>, url: &str) -> Result<CheckedUrl, Denied> {
        url_gate::check(policy, url, &self.allow_private)
    }

    /// Resolves and vets `host` the way the guarded client does (for connectors that open
    /// their own sockets, and for tests).
    pub async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, Denied> {
        resolver::vet(self.lookup.as_ref(), &self.allow_private, host).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_shape() {
        let ip: IpAddr = "169.254.169.254".parse().unwrap();
        let Failure::NonRetryable(info) = Denied::Blocked(ip).to_failure() else {
            panic!("not NonRetryable")
        };
        assert_eq!(info.kind, "egress_denied");
        let d = info.details.unwrap();
        assert_eq!(d.content_type, "application/json");
        let v: serde_json::Value = serde_json::from_slice(&d.data).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"reason": "blocked", "ip": "169.254.169.254"})
        );
        let Failure::NonRetryable(info) = Denied::Placeholder.to_failure() else {
            panic!()
        };
        assert_eq!(
            info.message,
            "egress denied: secret placeholders are not allowed in the URL"
        );
        let v: serde_json::Value = serde_json::from_slice(&info.details.unwrap().data).unwrap();
        assert_eq!(v, serde_json::json!({"reason": "placeholder"}));
    }

    #[test]
    fn reasons_are_distinct() {
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let all = [
            Denied::Parse(String::new()),
            Denied::Scheme,
            Denied::Userinfo,
            Denied::Port,
            Denied::Placeholder,
            Denied::NotAllowed,
            Denied::IpLiteral(ip),
            Denied::Blocked(ip),
            Denied::Dns(String::new()),
        ];
        let set: std::collections::BTreeSet<_> = all.iter().map(Denied::reason).collect();
        assert_eq!(set.len(), all.len());
    }
}
