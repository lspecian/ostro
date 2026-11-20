//! Origins and origin patterns (06-02, P-3), shared by egress allow-lists (06-04), grants
//! (06-05) and rate-limit targets (06-07).
//!
//! An [`Origin`] is `scheme://host[:port]` with the host lowercased (and IDNA-normalised, as
//! the `url` crate gives it) and the effective port. An [`OriginPattern`] is an origin whose
//! host may start with a single-label wildcard: `https://*.example.com:8443` matches
//! `https://api.example.com:8443` but neither `https://example.com:8443` nor
//! `https://a.b.example.com:8443`. A bare `*` host is rejected.

use std::fmt;

use url::{Host, Url};

/// The default port of `scheme`, if it has one.
fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "http" | "ws" => Some(80),
        "https" | "wss" => Some(443),
        _ => None,
    }
}

/// `scheme://host[:port]`, normalised.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Origin {
    /// Scheme, lowercase.
    pub scheme: String,
    /// Host, lowercase; IPv6 literals in brackets (`[::1]`).
    pub host: String,
    /// Effective port (the scheme default when the URL has none).
    pub port: u16,
}

impl Origin {
    /// The origin of `url`; `None` when the URL has no host or no effective port.
    pub fn from_url(url: &Url) -> Option<Self> {
        let host = url.host_str()?.to_ascii_lowercase();
        let port = url.port_or_known_default()?;
        Some(Self {
            scheme: url.scheme().to_ascii_lowercase(),
            host,
            port,
        })
    }
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.host)?;
        if default_port(&self.scheme) != Some(self.port) {
            write!(f, ":{}", self.port)?;
        }
        Ok(())
    }
}

/// An origin pattern could not be parsed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid origin pattern {pattern:?}: {reason}")]
pub struct OriginPatternError {
    /// The rejected input.
    pub pattern: String,
    /// Why it was rejected.
    pub reason: &'static str,
}

/// An origin whose host may start with a single-label wildcard (`*.`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OriginPattern {
    scheme: String,
    /// Exact host, or the suffix after `*.` when `wildcard`.
    host: String,
    wildcard: bool,
    port: u16,
}

impl OriginPattern {
    /// Parses `scheme://host[:port]` or `scheme://*.domain[:port]` (an optional trailing `/`
    /// is accepted; paths, queries, fragments and userinfo are not).
    pub fn parse(s: &str) -> Result<Self, OriginPatternError> {
        let err = |reason| OriginPatternError {
            pattern: s.to_owned(),
            reason,
        };
        let (scheme, rest) = s.split_once("://").ok_or(err("missing scheme"))?;
        let rest = rest.strip_suffix('/').unwrap_or(rest);
        if rest.contains(['/', '?', '#', '@']) {
            return Err(err("only scheme://host[:port] is allowed"));
        }
        let (wildcard, probe) = if rest == "*" || rest.starts_with("*:") {
            return Err(err("a bare `*` host is not allowed"));
        } else if let Some(tail) = rest.strip_prefix("*.") {
            // Normalise the suffix through `url` with a placeholder first label.
            (true, format!("{scheme}://wildcard-label.{tail}/"))
        } else {
            (false, format!("{scheme}://{rest}/"))
        };
        if probe.contains('*') {
            return Err(err("`*` is allowed only as the whole first label"));
        }
        let url = Url::parse(&probe).map_err(|_| err("not a valid origin"))?;
        let origin = Origin::from_url(&url).ok_or(err("no host or no known default port"))?;
        let host = if wildcard {
            if !matches!(url.host(), Some(Host::Domain(_))) {
                return Err(err("a wildcard needs a domain name"));
            }
            origin
                .host
                .strip_prefix("wildcard-label.")
                .ok_or(err("not a valid origin"))?
                .to_owned()
        } else {
            origin.host
        };
        if wildcard && !host.contains('.') {
            return Err(err("a wildcard needs at least two labels after `*.`"));
        }
        Ok(Self {
            scheme: origin.scheme,
            host,
            wildcard,
            port: origin.port,
        })
    }

    /// Whether `o` matches: same scheme and effective port, and the same host or (wildcard)
    /// exactly one extra leading label.
    pub fn matches(&self, o: &Origin) -> bool {
        if o.scheme != self.scheme || o.port != self.port {
            return false;
        }
        if !self.wildcard {
            return o.host == self.host;
        }
        match o
            .host
            .strip_suffix(self.host.as_str())
            .and_then(|p| p.strip_suffix('.'))
        {
            Some(label) => !label.is_empty() && !label.contains('.'),
            None => false,
        }
    }
}

impl fmt::Display for OriginPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let star = if self.wildcard { "*." } else { "" };
        write!(f, "{}://{star}{}", self.scheme, self.host)?;
        if default_port(&self.scheme) != Some(self.port) {
            write!(f, ":{}", self.port)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin(s: &str) -> Origin {
        Origin::from_url(&Url::parse(s).unwrap()).unwrap()
    }

    #[test]
    fn origin_normalises_case_and_port() {
        let o = origin("HTTPS://API.Example.COM/v1/charge?x=1");
        assert_eq!(o.scheme, "https");
        assert_eq!(o.host, "api.example.com");
        assert_eq!(o.port, 443);
        assert_eq!(o.to_string(), "https://api.example.com");
        assert_eq!(
            origin("https://api.example.com:443/").to_string(),
            "https://api.example.com"
        );
        assert_eq!(
            origin("https://api.example.com:8443/").to_string(),
            "https://api.example.com:8443"
        );
        assert_eq!(
            origin("http://127.0.0.1:8080").to_string(),
            "http://127.0.0.1:8080"
        );
        assert_eq!(origin("http://[::1]/").to_string(), "http://[::1]");
        assert!(Origin::from_url(&Url::parse("data:text/plain,x").unwrap()).is_none());
    }

    #[test]
    fn exact_pattern() {
        let p = OriginPattern::parse("https://api.stripe.com").unwrap();
        assert!(p.matches(&origin("https://api.stripe.com/v1")));
        assert!(p.matches(&origin("https://API.STRIPE.COM:443")));
        assert!(!p.matches(&origin("http://api.stripe.com")));
        assert!(!p.matches(&origin("https://api.stripe.com:8443")));
        assert!(!p.matches(&origin("https://x.api.stripe.com")));
        assert!(!p.matches(&origin("https://stripe.com")));
        assert_eq!(p.to_string(), "https://api.stripe.com");
        // Trailing slash and upper case are normalised.
        assert_eq!(OriginPattern::parse("HTTPS://API.Stripe.com/").unwrap(), p);
    }

    #[test]
    fn wildcard_matches_one_label_only() {
        let p = OriginPattern::parse("https://*.example.com:8443").unwrap();
        assert!(p.matches(&origin("https://api.example.com:8443")));
        assert!(p.matches(&origin("https://API.example.com:8443/x")));
        assert!(!p.matches(&origin("https://example.com:8443")));
        assert!(!p.matches(&origin("https://a.b.example.com:8443")));
        assert!(!p.matches(&origin("https://api.example.com")));
        assert!(!p.matches(&origin("https://apiexample.com:8443")));
        assert!(!p.matches(&origin("https://api.example.com.evil:8443")));
        assert_eq!(p.to_string(), "https://*.example.com:8443");
    }

    #[test]
    fn port_default_and_explicit() {
        let d = OriginPattern::parse("https://*.example.com").unwrap();
        assert!(d.matches(&origin("https://a.example.com")));
        assert!(d.matches(&origin("https://a.example.com:443")));
        assert!(!d.matches(&origin("https://a.example.com:444")));
        let e = OriginPattern::parse("http://localhost:9000").unwrap();
        assert!(e.matches(&origin("http://localhost:9000/bucket")));
        assert!(!e.matches(&origin("http://localhost")));
    }

    #[test]
    fn rejects_bad_patterns() {
        for bad in [
            "*",
            "https://*",
            "https://*:443",
            "https://a.*.com",
            "https://*a.example.com",
            "https://**.example.com",
            "https://*.com",
            "https://*.1.2.3",
            "api.example.com",
            "https://api.example.com/path",
            "https://user@api.example.com",
            "https://api.example.com?x",
            "foo://api.example.com",
        ] {
            assert!(OriginPattern::parse(bad).is_err(), "{bad}");
        }
    }
}
