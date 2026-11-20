//! Layer 2: the egress resolver (FX-08).
//!
//! [`EgressResolver`] implements `reqwest::dns::Resolve` over an injectable [`Lookup`]. It
//! collects **every** address of a name; if **any** is blocked the whole name fails
//! ("rebinding with mixed answers"), otherwise it returns exactly the vetted addresses.
//! hyper-util connects only to the addresses the resolver returns, so the connection is
//! pinned (no second resolution) and pooled connections stay on vetted IPs.
//!
//! IP-literal URLs never reach a resolver (hyper-util parses them first); the URL gate
//! classifies them.

use std::collections::BTreeMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use hickory_resolver::TokioResolver;
use hickory_resolver::config::{GOOGLE, ResolverConfig};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use ipnet::IpNet;

use super::Denied;
use super::classify::is_blocked;

/// A DNS lookup returning every A/AAAA address of `host`.
#[async_trait]
pub trait Lookup: Send + Sync {
    /// Every address of `host` (no port).
    async fn lookup(&self, host: &str) -> io::Result<Vec<IpAddr>>;
}

/// hickory-resolver lookup (async, no getaddrinfo thread pool; `/etc/hosts` honoured).
#[derive(Clone)]
pub struct HickoryLookup {
    inner: TokioResolver,
}

impl HickoryLookup {
    /// A resolver from the system configuration (`/etc/resolv.conf`). If that cannot be
    /// read, falls back to hickory's Google public-DNS config and logs a warning.
    pub fn new() -> io::Result<Self> {
        let builder = match TokioResolver::builder_tokio() {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "egress: system DNS config unreadable; using public DNS fallback");
                TokioResolver::builder_with_config(
                    ResolverConfig::udp_and_tcp(&GOOGLE),
                    TokioRuntimeProvider::default(),
                )
            }
        };
        let inner = builder.build().map_err(io::Error::other)?;
        Ok(Self { inner })
    }
}

impl std::fmt::Debug for HickoryLookup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HickoryLookup")
    }
}

#[async_trait]
impl Lookup for HickoryLookup {
    async fn lookup(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        let r = self
            .inner
            .lookup_ip(host)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::NotFound, e.to_string()))?;
        Ok(r.iter().collect())
    }
}

/// A fixed name → addresses table for tests (no network). Names are matched lowercase,
/// without a trailing dot; unknown names fail with `NotFound`.
#[derive(Clone, Debug, Default)]
pub struct StaticLookup(pub BTreeMap<String, Vec<IpAddr>>);

impl StaticLookup {
    /// An empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds (or replaces) `host` → `ips`.
    pub fn with(mut self, host: &str, ips: &[&str]) -> Self {
        let ips = ips
            .iter()
            .map(|s| s.parse().expect("StaticLookup: invalid IP"))
            .collect();
        self.0.insert(normalise(host), ips);
        self
    }
}

fn normalise(host: &str) -> String {
    host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase()
}

#[async_trait]
impl Lookup for StaticLookup {
    async fn lookup(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        self.0
            .get(&normalise(host))
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("no such host {host}")))
    }
}

/// Resolves `host` and vets every address: any blocked address fails the name with
/// [`Denied::Blocked`]; a failed or empty lookup is [`Denied::Dns`].
pub async fn vet(
    lookup: &dyn Lookup,
    allow_private: &[IpNet],
    host: &str,
) -> Result<Vec<IpAddr>, Denied> {
    let ips = lookup
        .lookup(host)
        .await
        .map_err(|e| Denied::Dns(e.to_string()))?;
    if ips.is_empty() {
        return Err(Denied::Dns(format!("{host}: no addresses")));
    }
    if let Some(bad) = ips.iter().find(|ip| is_blocked(**ip, allow_private)) {
        return Err(Denied::Blocked(*bad));
    }
    Ok(ips)
}

/// The error a denied resolution surfaces through reqwest's error chain. Its `Display`
/// starts with `egress_denied: reason=<reason>`; connectors recover the [`Denied`] with
/// [`Denied::from_error`] (W2).
#[derive(Debug, thiserror::Error)]
#[error("{kind}: reason={reason} {host}: {denied}", kind = crate::kinds::EGRESS_DENIED, reason = .denied.reason())]
pub struct ResolveDenied {
    /// The name being resolved.
    pub host: String,
    /// Why.
    pub denied: Denied,
}

/// `reqwest::dns::Resolve` that vets every answer (see the module docs).
#[derive(Clone)]
pub struct EgressResolver {
    lookup: Arc<dyn Lookup>,
    allow_private: Arc<[IpNet]>,
}

impl EgressResolver {
    /// A resolver over `lookup` with the node-level `allow_private` exemptions.
    pub fn new(lookup: Arc<dyn Lookup>, allow_private: Arc<[IpNet]>) -> Self {
        Self {
            lookup,
            allow_private,
        }
    }
}

impl reqwest::dns::Resolve for EgressResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let this = self.clone();
        Box::pin(async move {
            let host = name.as_str().to_owned();
            match vet(this.lookup.as_ref(), &this.allow_private, &host).await {
                Ok(ips) => {
                    // Port 0: hyper-util substitutes the URL's port.
                    let addrs: reqwest::dns::Addrs =
                        Box::new(ips.into_iter().map(|ip| SocketAddr::new(ip, 0)));
                    Ok(addrs)
                }
                Err(denied) => Err(Box::new(ResolveDenied { host, denied }) as _),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::dns::Resolve;

    fn resolver(l: StaticLookup, allow: &[&str]) -> EgressResolver {
        let nets: Vec<IpNet> = allow.iter().map(|s| s.parse().unwrap()).collect();
        EgressResolver::new(Arc::new(l), nets.into())
    }

    #[tokio::test]
    async fn any_blocked_answer_fails_the_name() {
        let l = StaticLookup::new()
            .with("mixed.test", &["93.184.216.34", "10.0.0.1"])
            .with("ok.test", &["93.184.216.34", "2606:4700::1111"])
            .with("empty.test", &[]);
        let r = resolver(l, &[]);
        let err = r
            .resolve("mixed.test".parse().unwrap())
            .await
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("egress_denied") && msg.contains("reason=blocked"),
            "{msg}"
        );
        assert_eq!(
            err.downcast_ref::<ResolveDenied>().unwrap().denied,
            Denied::Blocked("10.0.0.1".parse().unwrap())
        );
        let addrs: Vec<SocketAddr> = r
            .resolve("ok.test".parse().unwrap())
            .await
            .unwrap()
            .collect();
        assert_eq!(addrs.len(), 2);
        assert!(addrs.iter().all(|a| a.port() == 0));
        let err = r
            .resolve("empty.test".parse().unwrap())
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("reason=dns"), "{err}");
        let err = r.resolve("nope.test".parse().unwrap()).await.err().unwrap();
        assert!(err.to_string().contains("reason=dns"), "{err}");
    }

    #[tokio::test]
    async fn allow_private_admits_loopback() {
        let l = StaticLookup::new().with("mock.test", &["127.0.0.1"]);
        assert!(
            resolver(l.clone(), &[])
                .resolve("mock.test".parse().unwrap())
                .await
                .is_err()
        );
        assert!(
            resolver(l, &["127.0.0.0/8"])
                .resolve("mock.test".parse().unwrap())
                .await
                .is_ok()
        );
    }
}
