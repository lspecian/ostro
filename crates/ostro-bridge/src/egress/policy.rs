//! Per-module egress allow-list rules (`EgressPolicy.allow`, 06-DECISIONS).
//!
//! A rule is one of:
//! - `scheme://host[:port]`: exact host (normalised by `url`: lowercase, IDNA, trailing dot
//!   removed);
//! - `scheme://*.domain[:port]`: exactly one extra leading label;
//! - `scheme://<ip>[:port]` or `scheme://<ip>/<len>[:port]` (IPv6 in brackets,
//!   `https://[fd00::/8]:443`): IP literals and CIDRs. Only these rules admit IP-literal
//!   URLs, and only for addresses the classifier does not block;
//! - `connection:<name>`: lets the module use the named connection (P-7). Ignored by
//!   [`super::Egress::check`]; see [`allows_connection`].
//!
//! The scheme is `http` or `https`; the port defaults to 80/443.

use std::fmt;
use std::net::IpAddr;

use ipnet::IpNet;
use ostro_meta::records::EgressPolicy;
use url::{Host, Url};

use crate::origin::{Origin, OriginPattern};

/// A parsed allow-list entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AllowRule {
    /// A host name, exact or `*.`-wildcard.
    Host {
        /// `http` or `https`.
        scheme: String,
        /// Effective port.
        port: u16,
        /// The host pattern (scheme and port included).
        pattern: OriginPattern,
    },
    /// An IP literal or CIDR.
    Ip {
        /// `http` or `https`.
        scheme: String,
        /// Effective port.
        port: u16,
        /// Addresses admitted (a single address is a /32 or /128).
        net: IpNet,
    },
    /// `connection:<name>`.
    Connection(String),
}

impl AllowRule {
    /// `(scheme, port)` for URL rules; `None` for connection entries.
    pub fn scheme_port(&self) -> Option<(&str, u16)> {
        match self {
            AllowRule::Host { scheme, port, .. } | AllowRule::Ip { scheme, port, .. } => {
                Some((scheme.as_str(), *port))
            }
            AllowRule::Connection(_) => None,
        }
    }

    /// Whether the rule's host part admits `host` (scheme and port not compared).
    pub(crate) fn admits_host(&self, host: &Host<String>) -> bool {
        match (self, host) {
            (
                AllowRule::Host {
                    pattern,
                    scheme,
                    port,
                },
                Host::Domain(d),
            ) => pattern.matches(&Origin {
                scheme: scheme.clone(),
                host: d.clone(),
                port: *port,
            }),
            (AllowRule::Ip { net, .. }, Host::Ipv4(v4)) => net.contains(&IpAddr::V4(*v4)),
            (AllowRule::Ip { net, .. }, Host::Ipv6(v6)) => net.contains(&IpAddr::V6(*v6)),
            _ => false,
        }
    }
}

impl fmt::Display for AllowRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AllowRule::Host { pattern, .. } => write!(f, "{pattern}"),
            AllowRule::Ip { scheme, port, net } => {
                let host = match (net, net.prefix_len() == net.max_prefix_len()) {
                    (IpNet::V4(n), true) => n.addr().to_string(),
                    (IpNet::V4(n), false) => n.to_string(),
                    (IpNet::V6(n), true) => format!("[{}]", n.addr()),
                    (IpNet::V6(n), false) => format!("[{n}]"),
                };
                write!(f, "{scheme}://{host}")?;
                if default_port(scheme) != *port {
                    write!(f, ":{port}")?;
                }
                Ok(())
            }
            AllowRule::Connection(name) => write!(f, "connection:{name}"),
        }
    }
}

/// An allow-list entry could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid egress rule {rule:?}: {reason}")]
pub struct RuleError {
    /// The rejected input.
    pub rule: String,
    /// Why it was rejected.
    pub reason: String,
}

fn default_port(scheme: &str) -> u16 {
    if scheme == "http" { 80 } else { 443 }
}

/// Parses one allow-list entry (see the module docs for the grammar).
pub fn parse_rule(s: &str) -> Result<AllowRule, RuleError> {
    let err = |reason: &str| RuleError {
        rule: s.to_owned(),
        reason: reason.to_owned(),
    };
    let t = s.trim();
    if let Some(name) = t.strip_prefix("connection:") {
        if name.is_empty() || name.chars().any(char::is_whitespace) {
            return Err(err("connection name must be non-empty without whitespace"));
        }
        return Ok(AllowRule::Connection(name.to_owned()));
    }
    let (scheme, rest) = t.split_once("://").ok_or_else(|| err("missing scheme"))?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(err("scheme must be http or https"));
    }
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    if rest.contains(['?', '#', '@']) {
        return Err(err("only scheme://host[:port] is allowed"));
    }
    let normalised = strip_trailing_dot(rest);
    let rest = normalised.as_str();
    if rest.contains('/') {
        return parse_cidr_rule(&scheme, rest).ok_or_else(|| err("invalid CIDR rule"));
    }
    if !rest.starts_with("*.") {
        // Normalise through `url`: an IP host (in any spelling) becomes an `Ip` rule.
        let url =
            Url::parse(&format!("{scheme}://{rest}/")).map_err(|_| err("not a valid origin"))?;
        let port = url.port_or_known_default().ok_or_else(|| err("no port"))?;
        let ip = match url.host() {
            Some(Host::Ipv4(v4)) => Some(IpAddr::V4(v4)),
            Some(Host::Ipv6(v6)) => Some(IpAddr::V6(v6)),
            _ => None,
        };
        if let Some(ip) = ip {
            return Ok(AllowRule::Ip {
                scheme,
                port,
                net: IpNet::from(ip),
            });
        }
    }
    let pattern = OriginPattern::parse(&format!("{scheme}://{rest}")).map_err(|e| err(e.reason))?;
    // Recover the effective port (the pattern keeps it private).
    let probe = rest.replacen("*.", "wildcard-label.", 1);
    let url = Url::parse(&format!("{scheme}://{probe}/")).map_err(|_| err("not a valid origin"))?;
    let port = url.port_or_known_default().ok_or_else(|| err("no port"))?;
    Ok(AllowRule::Host {
        scheme,
        port,
        pattern,
    })
}

/// Removes one trailing dot from the host part of `host[:port]` (`localhost.:80` →
/// `localhost:80`), the same normalisation the URL gate applies to request hosts.
fn strip_trailing_dot(rest: &str) -> String {
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') && !h.contains(':') => (h, Some(p)),
        Some((h, p)) if h.ends_with(']') => (h, Some(p)),
        _ => (rest, None),
    };
    let host = host.strip_suffix('.').unwrap_or(host);
    match port {
        Some(p) => format!("{host}:{p}"),
        None => host.to_owned(),
    }
}

/// `a.b.c.d/len[:port]` or `[v6/len][:port]`.
fn parse_cidr_rule(scheme: &str, rest: &str) -> Option<AllowRule> {
    let (net, port) = if let Some(inner) = rest.strip_prefix('[') {
        let (net, tail) = inner.split_once(']')?;
        let port = match tail {
            "" => None,
            p => Some(p.strip_prefix(':')?),
        };
        (net, port)
    } else {
        match rest.split_once(':') {
            Some((n, p)) => (n, Some(p)),
            None => (rest, None),
        }
    };
    let net: IpNet = net.parse().ok()?;
    let port = match port {
        Some(p) => p.parse().ok()?,
        None => default_port(scheme),
    };
    Some(AllowRule::Ip {
        scheme: scheme.to_owned(),
        port,
        net: net.trunc(),
    })
}

/// The parsable URL rules of `policy`. Unparsable entries are skipped (the admin API
/// validates entries with [`parse_rule`] on write; skipping can only deny more).
pub fn rules(policy: &EgressPolicy) -> Vec<AllowRule> {
    policy
        .allow
        .iter()
        .filter_map(|s| match parse_rule(s) {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::warn!(module = %policy.name, error = %e, "ignoring invalid egress rule");
                None
            }
        })
        .collect()
}

/// Whether `policy` lists `connection:<name>` (P-7). No policy = no connections.
pub fn allows_connection(policy: Option<&EgressPolicy>, name: &str) -> bool {
    policy.is_some_and(|p| {
        p.allow
            .iter()
            .any(|s| s.trim().strip_prefix("connection:") == Some(name))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(allow: &[&str]) -> EgressPolicy {
        EgressPolicy {
            name: "t/m".into(),
            allow: allow.iter().map(|s| s.to_string()).collect(),
            ext: Vec::new(),
        }
    }

    #[test]
    fn parses_rule_kinds() {
        let r = parse_rule("https://api.stripe.com").unwrap();
        assert_eq!(r.scheme_port(), Some(("https", 443)));
        assert_eq!(r.to_string(), "https://api.stripe.com");
        let r = parse_rule("http://*.example.com:8443/").unwrap();
        assert_eq!(r.scheme_port(), Some(("http", 8443)));
        assert_eq!(r.to_string(), "http://*.example.com:8443");
        let r = parse_rule("https://203.0.113.7").unwrap();
        assert_eq!(
            r,
            AllowRule::Ip {
                scheme: "https".into(),
                port: 443,
                net: "203.0.113.7/32".parse().unwrap()
            }
        );
        // Any IPv4 spelling normalises to the same rule.
        assert_eq!(parse_rule("https://0xcb.0.113.7").unwrap(), r);
        let r = parse_rule("http://[::1]:8080").unwrap();
        assert_eq!(r.to_string(), "http://[::1]:8080");
        let r = parse_rule("https://10.20.0.0/16:8443").unwrap();
        assert_eq!(r.to_string(), "https://10.20.0.0/16:8443");
        let r = parse_rule("https://[fd00::/8]").unwrap();
        assert_eq!(r.to_string(), "https://[fd00::/8]");
        assert_eq!(
            parse_rule("connection:orders-db").unwrap(),
            AllowRule::Connection("orders-db".into())
        );
        assert_eq!(
            parse_rule("https://LOCALHOST.").unwrap().to_string(),
            "https://localhost"
        );
        assert_eq!(
            parse_rule("http://localhost.:9000").unwrap().to_string(),
            "http://localhost:9000"
        );
    }

    #[test]
    fn rejects_bad_rules() {
        for bad in [
            "",
            "connection:",
            "api.example.com",
            "ftp://api.example.com",
            "wss://api.example.com",
            "https://user@api.example.com",
            "https://api.example.com/path",
            "https://*",
            "https://*.com",
            "https://a.*.example.com",
            "https://10.0.0.0/33",
            "https://10.0.0.0/8:http",
            "https://api.example.com?x",
        ] {
            assert!(parse_rule(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn wildcard_admits_one_label() {
        let r = parse_rule("https://*.example.com").unwrap();
        let d = |s: &str| Host::Domain(s.to_owned());
        assert!(r.admits_host(&d("a.example.com")));
        assert!(!r.admits_host(&d("example.com")));
        assert!(!r.admits_host(&d("a.b.example.com")));
        assert!(!r.admits_host(&Host::Ipv4("10.0.0.1".parse().unwrap())));
    }

    #[test]
    fn connections() {
        let p = policy(&["https://api.stripe.com", "connection:orders-db", "garbage"]);
        assert!(allows_connection(Some(&p), "orders-db"));
        assert!(!allows_connection(Some(&p), "orders"));
        assert!(!allows_connection(None, "orders-db"));
        assert_eq!(rules(&p).len(), 2);
    }
}
