//! The Kubernetes `Secret` provider (feature `k8s`), owned by 06-13.
//!
//! # Names
//!
//! `<secret>/<key>` or `<secret>#<key>` reads `data[<key>]` of the `Secret` `<secret>` in the
//! provider's namespace. `<secret>` is a DNS-1123 subdomain and `<key>` matches
//! `[-._a-zA-Z0-9]{1,253}`. A name of another shape cannot exist in Kubernetes and is
//! `Ok(None)` (the chain moves on); a reserved `__ostro/` name is `Invalid`.
//!
//! Note: the `{{secret:NAME}}` grammar (06-05) is lower-case and has no `#`, so templates
//! reach keys of the `<secret>/<key>` form with lower-case keys.
//!
//! # Values
//!
//! The raw bytes of `data[<key>]` (base64-decoded by k8s-openapi's `ByteString`). A missing
//! secret or key is `Ok(None)`. A 403 is `Unavailable("permission denied")`; any other API
//! or connection error is `Unavailable` with the status only. After the key is taken, the
//! other `data`/`stringData` values and the annotations (which can hold
//! `kubectl.kubernetes.io/last-applied-configuration`, a copy of the data) are zeroized.
//! The response body buffers inside kube/hyper are outside this module's control.
//!
//! # Client and namespace
//!
//! With `kubeconfig` set, that file's current context is used
//! (`Kubeconfig::read_from` + `Config::from_custom_kubeconfig`). Otherwise
//! `kube::Config::infer()` (`KUBECONFIG` / `~/.kube/config`, then the in-cluster service
//! account). The namespace is `namespace` from the config, else the client config's default
//! namespace (the in-cluster namespace file, the context's namespace, or `default`).
//!
//! Config-loading errors are reported without their underlying text, because a kubeconfig
//! parse error could quote the file (and its token).
//!
//! # Caching
//!
//! Found values are cached per name for `cache_ttl` in a [`TtlCache`].
//!
//! # kube 5.1.0 / k8s-openapi 0.29 API used (checked in the registry source)
//!
//! `kube::config::{Kubeconfig::read_from, KubeConfigOptions}`,
//! `kube::Config::{from_custom_kubeconfig, infer, default_namespace}`,
//! `kube::Client::try_from(Config)`, `kube::Api::<Secret>::namespaced(client, ns).get(name)`,
//! `kube::Error::Api(Box<Status { code, .. }>)`,
//! `k8s_openapi::api::core::v1::Secret { data: Option<BTreeMap<String, ByteString>>, .. }`.
//! kube's connector is `https_or_http`, so a plain-http API server (the test mock) works.

use std::fmt;

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Client, Config};
use secrecy::SecretBox;
use zeroize::{Zeroize, Zeroizing};

use super::cache::TtlCache;
use super::{ProviderError, SecretProvider};
use crate::secrets::config::K8sConfig;
use crate::secrets::template::RESERVED_PREFIX;

/// Reads secrets from Kubernetes `Secret` objects.
pub struct K8sProvider {
    namespace: String,
    api: Api<Secret>,
    cache: TtlCache<String>,
}

impl fmt::Debug for K8sProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("K8sProvider")
            .field("namespace", &self.namespace)
            .field("cache_ttl", &self.cache.ttl())
            .finish_non_exhaustive()
    }
}

impl K8sProvider {
    /// A provider for `[secrets.k8s]`: loads the client configuration (no API call yet).
    pub async fn from_config(cfg: &K8sConfig) -> Result<Self, ProviderError> {
        let config = match &cfg.kubeconfig {
            Some(p) => {
                let kc = Kubeconfig::read_from(p).map_err(|_| {
                    ProviderError::Unavailable(format!(
                        "k8s: kubeconfig {} is unreadable or invalid",
                        p.display()
                    ))
                })?;
                Config::from_custom_kubeconfig(kc, &KubeConfigOptions::default())
                    .await
                    .map_err(|_| {
                        ProviderError::Unavailable(format!(
                            "k8s: kubeconfig {} has no usable current context",
                            p.display()
                        ))
                    })?
            }
            None => Config::infer().await.map_err(|_| {
                ProviderError::Unavailable(
                    "k8s: neither a kubeconfig (KUBECONFIG, ~/.kube/config) nor an in-cluster \
                     service account is usable"
                        .into(),
                )
            })?,
        };
        let namespace = cfg
            .namespace
            .clone()
            .unwrap_or_else(|| config.default_namespace.clone());
        if !is_dns_label(&namespace) {
            return Err(ProviderError::Invalid(format!(
                "secrets.k8s: invalid namespace {namespace:?}"
            )));
        }
        let client = Client::try_from(config).map_err(|_| {
            ProviderError::Unavailable("k8s: the client could not be built from its config".into())
        })?;
        Ok(Self {
            api: Api::namespaced(client, &namespace),
            namespace,
            cache: TtlCache::new(cfg.cache_ttl),
        })
    }

    /// The namespace secrets are read from.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The value cache.
    pub fn cache(&self) -> &TtlCache<String> {
        &self.cache
    }
}

fn is_dns_label(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && b.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        && b[0] != b'-'
        && b[b.len() - 1] != b'-'
}

fn is_dns_subdomain(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 253
        && b.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.')
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
}

fn is_key(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 253
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-._".contains(&c))
}

/// `(secret, key)`, or `None` when `name` cannot name a Kubernetes secret key.
fn parse_name(name: &str) -> Option<(&str, &str)> {
    let (secret, key) = if name.contains('#') {
        name.split_once('#')?
    } else {
        name.split_once('/')?
    };
    (is_dns_subdomain(secret) && is_key(key)).then_some((secret, key))
}

fn unavailable(e: &kube::Error) -> ProviderError {
    match e {
        kube::Error::Api(s) if s.code == 403 => {
            ProviderError::Unavailable("permission denied".into())
        }
        kube::Error::Api(s) if s.code == 401 => {
            ProviderError::Unavailable("k8s: unauthorized".into())
        }
        kube::Error::Api(s) => ProviderError::Unavailable(format!("k8s: status {}", s.code)),
        kube::Error::HyperError(_) | kube::Error::Service(_) => {
            ProviderError::Unavailable(format!("k8s request failed: {e}"))
        }
        // Decoding errors can quote the response (secret data): never echo them.
        _ => ProviderError::Unavailable("k8s: the API response could not be processed".into()),
    }
}

/// Takes `data[key]` out of `s` and zeroizes every other value-bearing field.
fn take_key(mut s: Secret, key: &str) -> Option<Zeroizing<Vec<u8>>> {
    let taken = s.data.as_mut().and_then(|d| d.remove(key));
    if let Some(d) = s.data.as_mut() {
        d.values_mut().for_each(|v| v.0.zeroize());
    }
    if let Some(d) = s.string_data.as_mut() {
        d.values_mut().for_each(Zeroize::zeroize);
    }
    if let Some(a) = s.metadata.annotations.as_mut() {
        a.values_mut().for_each(Zeroize::zeroize);
    }
    taken.map(|ByteString(b)| Zeroizing::new(b))
}

#[async_trait::async_trait]
impl SecretProvider for K8sProvider {
    fn name(&self) -> &'static str {
        "k8s"
    }

    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        if name.starts_with(RESERVED_PREFIX) {
            return Err(ProviderError::Invalid(format!(
                "secret name {name:?} uses the reserved prefix"
            )));
        }
        let Some((secret, key)) = parse_name(name) else {
            return Ok(None);
        };
        let cache_key = name.to_owned();
        if let Some(v) = self.cache.get(&cache_key) {
            return Ok(Some(v));
        }
        let s = match self.api.get(secret).await {
            Ok(s) => s,
            Err(kube::Error::Api(st)) if st.code == 404 => return Ok(None),
            Err(e) => {
                tracing::debug!(provider = "k8s", namespace = %self.namespace, secret, "k8s secret read failed");
                return Err(unavailable(&e));
            }
        };
        let Some(bytes) = take_key(s, key) else {
            return Ok(None);
        };
        let out = SecretBox::new(bytes.as_slice().into());
        self.cache.insert(cache_key, bytes);
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn names() {
        assert_eq!(parse_name("stripe/live-key"), Some(("stripe", "live-key")));
        assert_eq!(
            parse_name("db.creds#PASSWORD"),
            Some(("db.creds", "PASSWORD"))
        );
        assert_eq!(parse_name("db#a/b"), None, "keys have no '/'");
        for none in [
            "plain",
            "a/b/c",
            "Upper/key",
            "-x/key",
            "x/",
            "/k",
            "x#",
            "x/k k",
        ] {
            assert_eq!(parse_name(none), None, "{none}");
        }
        assert!(is_dns_label("ostro") && !is_dns_label("Ostro") && !is_dns_label("a.b"));
    }

    #[test]
    fn take_key_scrubs_the_rest() {
        let mut s = Secret {
            data: Some(BTreeMap::from([
                ("a".to_owned(), ByteString(b"value-a-123".to_vec())),
                ("b".to_owned(), ByteString(b"value-b-456".to_vec())),
            ])),
            ..Secret::default()
        };
        s.metadata.annotations = Some(BTreeMap::from([(
            "kubectl.kubernetes.io/last-applied-configuration".to_owned(),
            "{\"data\":{\"a\":\"dmFsdWUtYS0xMjM=\"}}".to_owned(),
        )]));
        assert_eq!(take_key(s.clone(), "a").unwrap().as_slice(), b"value-a-123");
        assert!(take_key(s, "missing").is_none());
        assert!(take_key(Secret::default(), "a").is_none());
    }

    #[test]
    fn errors_carry_status_only() {
        let st = |code: u16| {
            kube::Error::Api(
                kube::core::Status::failure("secret-ish message", "Forbidden")
                    .with_code(code)
                    .boxed(),
            )
        };
        assert!(
            matches!(unavailable(&st(403)), ProviderError::Unavailable(m) if m == "permission denied")
        );
        let m = unavailable(&st(500)).to_string();
        assert!(m.contains("status 500") && !m.contains("secret-ish"), "{m}");
        let m = unavailable(&st(401)).to_string();
        assert!(!m.contains("secret-ish"), "{m}");
    }
}
