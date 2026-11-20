//! Layer 1: the URL gate (FX-08).
//!
//! Runs before anything is sent, and again on every redirect hop (06-10, P-5):
//! 1. a `{{secret:` placeholder anywhere in the URL → [`Denied::Placeholder`] (D-FX4);
//! 2. WHATWG parse with `url` (every inet_aton spelling — decimal, octal, hex, short,
//!    percent-encoded, ideographic dots — normalises to `Host::Ipv4`; invalid hosts fail);
//! 3. userinfo → [`Denied::Userinfo`] (`http://user:pw@evil.com@127.0.0.1/` is host
//!    127.0.0.1);
//! 4. no policy → [`Denied::NotAllowed`] (deny by default);
//! 5. IP literals are classified **here**, because hyper-util's `HttpConnector` never calls
//!    the resolver for them; a literal also needs an IP/CIDR rule that lists it;
//! 6. the normalised host (trailing dot removed), scheme and port must match one rule.
//!
//! No regex ever looks at the URL string.

use std::net::IpAddr;

use ipnet::IpNet;
use ostro_meta::records::EgressPolicy;
use url::{Host, Url};

use super::classify::is_blocked;
use super::policy::{AllowRule, rules};
use super::{CheckedUrl, Denied};
use crate::origin::Origin;

/// The marker that opens a secret template (06-05).
const PLACEHOLDER: &str = "{{secret:";

/// Checks `raw` against `policy` and the classifier. See the module docs.
pub fn check(
    policy: Option<&EgressPolicy>,
    raw: &str,
    allow_private: &[IpNet],
) -> Result<CheckedUrl, Denied> {
    if raw.contains(PLACEHOLDER) {
        return Err(Denied::Placeholder);
    }
    let mut url = Url::parse(raw).map_err(|e| Denied::Parse(e.to_string()))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Denied::Userinfo);
    }
    // Decoded forms too: a percent-encoded placeholder in the path or query would not be
    // expanded, but such a URL is never legitimate.
    if url.as_str().to_ascii_lowercase().contains("%7b%7bsecret:") {
        return Err(Denied::Placeholder);
    }
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Denied::Scheme);
    }
    let policy = policy.ok_or(Denied::NotAllowed)?;

    let host: Host<String> = match url.host() {
        None => return Err(Denied::Parse("URL has no host".into())),
        Some(Host::Domain(d)) => {
            let d = d.strip_suffix('.').unwrap_or(d).to_owned();
            if d.is_empty() || d.ends_with('.') {
                return Err(Denied::Parse("empty host label".into()));
            }
            Host::Domain(d)
        }
        Some(Host::Ipv4(v4)) => Host::Ipv4(v4),
        Some(Host::Ipv6(v6)) => Host::Ipv6(v6),
    };
    if let Host::Domain(d) = &host
        && url.host_str() != Some(d.as_str())
    {
        url.set_host(Some(d))
            .map_err(|e| Denied::Parse(e.to_string()))?;
    }

    let literal: Option<IpAddr> = match host {
        Host::Ipv4(v4) => Some(v4.into()),
        Host::Ipv6(v6) => Some(v6.into()),
        Host::Domain(_) => None,
    };
    if let Some(ip) = literal
        && is_blocked(ip, allow_private)
    {
        return Err(Denied::Blocked(ip));
    }

    let scheme = url.scheme().to_owned();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| Denied::Parse("no port".into()))?;
    let candidates: Vec<AllowRule> = rules(policy)
        .into_iter()
        .filter(|r| r.admits_host(&host))
        .collect();
    if candidates.is_empty() {
        return Err(match literal {
            Some(ip) => Denied::IpLiteral(ip),
            None => Denied::NotAllowed,
        });
    }
    let same_scheme: Vec<&AllowRule> = candidates
        .iter()
        .filter(|r| r.scheme_port().is_some_and(|(s, _)| s == scheme))
        .collect();
    if same_scheme.is_empty() {
        return Err(Denied::Scheme);
    }
    let rule = same_scheme
        .into_iter()
        .find(|r| r.scheme_port().is_some_and(|(_, p)| p == port))
        .ok_or(Denied::Port)?
        .clone();
    let origin = Origin::from_url(&url).ok_or_else(|| Denied::Parse("no origin".into()))?;
    Ok(CheckedUrl { url, origin, rule })
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

    fn reason(p: Option<&EgressPolicy>, url: &str) -> &'static str {
        match check(p, url, &[]) {
            Ok(_) => "ok",
            Err(d) => d.reason(),
        }
    }

    #[test]
    fn deny_by_default() {
        assert_eq!(reason(None, "https://api.stripe.com/"), "not_allowed");
        let p = policy(&[]);
        assert_eq!(reason(Some(&p), "https://api.stripe.com/"), "not_allowed");
    }

    #[test]
    fn exact_wildcard_scheme_port() {
        let p = policy(&[
            "https://api.stripe.com",
            "https://*.example.com:8443",
            "http://plain.test",
        ]);
        let p = Some(&p);
        assert_eq!(reason(p, "https://api.stripe.com/v1/charges"), "ok");
        assert_eq!(reason(p, "https://API.STRIPE.COM./v1"), "ok");
        assert_eq!(reason(p, "http://api.stripe.com/"), "scheme");
        assert_eq!(reason(p, "https://api.stripe.com:444/"), "port");
        assert_eq!(reason(p, "https://a.example.com:8443/"), "ok");
        assert_eq!(reason(p, "https://example.com:8443/"), "not_allowed");
        assert_eq!(reason(p, "https://a.b.example.com:8443/"), "not_allowed");
        assert_eq!(reason(p, "https://a.example.com/"), "port");
        assert_eq!(reason(p, "http://plain.test/"), "ok");
        assert_eq!(reason(p, "https://plain.test/"), "scheme");
        assert_eq!(reason(p, "ftp://api.stripe.com/"), "scheme");
        assert_eq!(reason(p, "https://user:pw@api.stripe.com/"), "userinfo");
        assert_eq!(
            reason(p, "https://api.stripe.com/{{secret:k}}"),
            "placeholder"
        );
        assert_eq!(
            reason(p, "https://api.stripe.com/?q=%7B%7Bsecret:k}}"),
            "placeholder"
        );
        assert_eq!(reason(p, "https://api.stripe.com../"), "parse");
        assert_eq!(
            reason(p, "https://api.stripe.com/?q=%7b%7bsecret:k"),
            "placeholder"
        );
    }

    #[test]
    fn trailing_dot_is_normalised() {
        let p = policy(&["https://api.stripe.com"]);
        let c = check(Some(&p), "https://api.stripe.com./x", &[]).unwrap();
        assert_eq!(c.url.as_str(), "https://api.stripe.com/x");
        assert_eq!(c.origin.host, "api.stripe.com");
    }

    #[test]
    fn literals_need_a_rule_and_must_not_be_blocked() {
        let p = policy(&["https://8.8.8.8", "https://10.0.0.0/8"]);
        let p = Some(&p);
        assert_eq!(reason(p, "https://8.8.8.8/"), "ok");
        assert_eq!(reason(p, "https://134744072/"), "ok"); // decimal 8.8.8.8
        assert_eq!(reason(p, "https://1.1.1.1/"), "ip_literal");
        assert_eq!(reason(p, "https://10.1.2.3/"), "blocked");
        let lo: Vec<IpNet> = vec!["10.0.0.0/8".parse().unwrap()];
        assert!(check(p, "https://10.1.2.3/", &lo).is_ok());
        assert_eq!(
            check(p, "https://10.1.2.3:8443/", &lo)
                .unwrap_err()
                .reason(),
            "port"
        );
    }
}
