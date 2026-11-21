//! One `ResolvesServerCert` over an `ArcSwap<CertifiedKey>`: every TLS mode
//! (`self-signed`, `files`, `acme`) feeds it, so reloads and renewals swap the served
//! certificate without a restart (D-W7).
//!
//! ACME hook (07-10): a ClientHello whose ALPN offers only `acme-tls/1` is answered with
//! the challenge certificate registered for its SNI through
//! [`ReloadableResolver::set_challenge`] (TLS-ALPN-01, RFC 8737), never with the served
//! certificate.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwapOption;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

use super::ca::sha256_fingerprint;

/// The TLS-ALPN-01 protocol id (RFC 8737).
pub const ACME_TLS_ALPN: &[u8] = b"acme-tls/1";

/// The hot-swappable certificate resolver of the client listener.
#[derive(Default)]
pub struct ReloadableResolver {
    current: ArcSwapOption<CertifiedKey>,
    /// TLS-ALPN-01 challenge certificates by (lowercase) domain.
    challenges: RwLock<HashMap<String, Arc<CertifiedKey>>>,
}

impl fmt::Debug for ReloadableResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let challenges = self.challenges.read().map(|c| c.len()).unwrap_or_default();
        f.debug_struct("ReloadableResolver")
            .field("fingerprint", &self.fingerprint())
            .field("challenges", &challenges)
            .finish()
    }
}

impl ReloadableResolver {
    /// A resolver serving `initial` (`None`: every handshake fails until [`Self::swap`]).
    pub fn new(initial: Option<Arc<CertifiedKey>>) -> Self {
        Self {
            current: ArcSwapOption::new(initial),
            challenges: RwLock::default(),
        }
    }

    /// Serve `new` from the next handshake on (in-flight connections keep theirs).
    pub fn swap(&self, new: impl Into<Arc<CertifiedKey>>) {
        self.current.store(Some(new.into()));
    }

    /// The served certificate.
    pub fn current(&self) -> Option<Arc<CertifiedKey>> {
        self.current.load_full()
    }

    /// `sha256:<hex>` of the served leaf.
    pub fn fingerprint(&self) -> Option<String> {
        self.current
            .load()
            .as_ref()
            .and_then(|k| k.cert.first().map(|c| sha256_fingerprint(c)))
    }

    /// Register (`Some`) or clear (`None`) the TLS-ALPN-01 challenge certificate of
    /// `domain` (07-10).
    pub fn set_challenge(&self, domain: &str, cert: Option<CertifiedKey>) {
        let domain = domain.trim_end_matches('.').to_ascii_lowercase();
        let mut map = match self.challenges.write() {
            Ok(m) => m,
            Err(poisoned) => poisoned.into_inner(),
        };
        match cert {
            Some(c) => {
                map.insert(domain, Arc::new(c));
            }
            None => {
                map.remove(&domain);
            }
        }
    }

    fn challenge_for(&self, sni: Option<&str>) -> Option<Arc<CertifiedKey>> {
        let sni = sni?.trim_end_matches('.').to_ascii_lowercase();
        let map = match self.challenges.read() {
            Ok(m) => m,
            Err(poisoned) => poisoned.into_inner(),
        };
        map.get(&sni).cloned()
    }
}

/// True when the ClientHello offers ALPN and every offered protocol is `acme-tls/1`.
fn acme_only(hello: &ClientHello<'_>) -> bool {
    match hello.alpn() {
        Some(mut protos) => {
            let mut any = false;
            let all = protos.all(|p| {
                any = true;
                p == ACME_TLS_ALPN
            });
            any && all
        }
        None => false,
    }
}

impl ResolvesServerCert for ReloadableResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        if acme_only(&hello) {
            // A validator handshake: only a registered challenge cert, never the real one.
            return self.challenge_for(hello.server_name());
        }
        self.current.load_full()
    }
}
