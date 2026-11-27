//! The `[secrets]` node config section (06-DECISIONS, RESERVATIONS §11).
//!
//! ```toml
//! [secrets]
//! providers = ["sealed", "env", "file"]   # ordered; the first provider that has the secret wins
//! sealing_key_file = "/etc/ostro/sealing.key"
//! old_sealing_key_files = ["/etc/ostro/sealing-old.key"]   # read-only, for rotation
//! env_prefix = "OSTRO_SECRET_"
//! file_dir = "/run/secrets"
//!
//! [secrets.vault]          # interpreted by 06-13
//! addr = "https://vault:8200"
//! mount = "secret"
//! auth = "approle"         # token | approle | kubernetes
//! role_id = "..."
//! secret_id_file = "/run/vault/secret-id"
//! cache_ttl = "30s"
//!
//! [secrets.k8s]            # interpreted by 06-13
//! namespace = "ostro"
//! cache_ttl = "30s"
//! ```
//!
//! The sealing key itself is never configured inline: it comes from `OSTRO_SEALING_KEY`, the
//! `sealing_key_file`, or (dev only) `<data_dir>/sealing.key`; older keys kept for opening
//! only come from `OSTRO_SEALING_KEY_OLD` and `old_sealing_key_files` (see
//! [`super::sealed::SealingKeyRing::load`]).

use std::path::PathBuf;
use std::time::Duration;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

use crate::egress::config::parse_duration;

/// Default `env_prefix`.
pub const DEFAULT_ENV_PREFIX: &str = "OSTRO_SECRET_";
/// Default cache TTL of the remote providers.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);

/// One secret provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    /// AES-256-GCM sealed secrets in the meta store.
    Sealed,
    /// `OSTRO_SECRET_<NAME>` environment variables.
    Env,
    /// One file per secret under `file_dir`.
    File,
    /// HashiCorp Vault KV v2 (06-13, feature `vault`).
    Vault,
    /// Kubernetes `Secret`s (06-13, feature `k8s`).
    K8s,
}

impl ProviderKind {
    /// The provider's name (`SecretProvider::name`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sealed => "sealed",
            Self::Env => "env",
            Self::File => "file",
            Self::Vault => "vault",
            Self::K8s => "k8s",
        }
    }
}

/// The whole `[secrets]` section.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecretsConfig {
    /// Provider order (default `["sealed", "env", "file"]`).
    pub providers: Vec<ProviderKind>,
    /// File holding the 32-byte sealing key (base64 text or 32 raw bytes).
    pub sealing_key_file: Option<PathBuf>,
    /// Older sealing keys (same file format), used only to open records sealed before a key
    /// rotation; never used to seal (p14-key-rotation).
    pub old_sealing_key_files: Vec<PathBuf>,
    /// Environment variable prefix of the env provider (default `OSTRO_SECRET_`).
    pub env_prefix: String,
    /// Directory of the file provider; the file provider is skipped while unset.
    pub file_dir: Option<PathBuf>,
    /// `[secrets.vault]`.
    pub vault: Option<VaultConfig>,
    /// `[secrets.k8s]`.
    pub k8s: Option<K8sConfig>,
}

impl Default for SecretsConfig {
    fn default() -> Self {
        Self {
            providers: vec![ProviderKind::Sealed, ProviderKind::Env, ProviderKind::File],
            sealing_key_file: None,
            old_sealing_key_files: Vec::new(),
            env_prefix: DEFAULT_ENV_PREFIX.to_owned(),
            file_dir: None,
            vault: None,
            k8s: None,
        }
    }
}

impl SecretsConfig {
    /// Cross-field checks: no duplicate providers; `vault`/`k8s` listed only with their
    /// table; a non-empty `env_prefix` of `[A-Z0-9_]`. A listed `file` provider without
    /// `file_dir` is allowed (the default list includes it) and is simply not built.
    pub fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::BTreeSet::new();
        for p in &self.providers {
            if !seen.insert(*p) {
                return Err(format!("secrets: provider {:?} listed twice", p.as_str()));
            }
        }
        if seen.contains(&ProviderKind::Vault) && self.vault.is_none() {
            return Err("secrets: provider \"vault\" needs a [secrets.vault] table".into());
        }
        if seen.contains(&ProviderKind::K8s) && self.k8s.is_none() {
            return Err("secrets: provider \"k8s\" needs a [secrets.k8s] table".into());
        }
        if self.env_prefix.is_empty()
            || !self
                .env_prefix
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            return Err(format!(
                "secrets: env_prefix {:?} must be non-empty [A-Z0-9_]",
                self.env_prefix
            ));
        }
        Ok(())
    }
}

/// Vault authentication method.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VaultAuth {
    /// A token from `token_file` (or `VAULT_TOKEN`, 06-13).
    #[default]
    Token,
    /// AppRole (`role_id` + `secret_id_file`).
    Approle,
    /// Kubernetes service-account auth (`k8s_role`).
    Kubernetes,
}

/// `[secrets.vault]` (interpreted by 06-13).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VaultConfig {
    /// Vault address (`https://vault:8200`).
    pub addr: String,
    /// KV v2 mount (default `secret`).
    #[serde(default = "default_mount")]
    pub mount: String,
    /// Auth method (default `token`).
    #[serde(default)]
    pub auth: VaultAuth,
    /// Token file (`auth = "token"`).
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    /// AppRole role id (`auth = "approle"`).
    #[serde(default)]
    pub role_id: Option<String>,
    /// AppRole secret-id file (`auth = "approle"`).
    #[serde(default)]
    pub secret_id_file: Option<PathBuf>,
    /// Vault Kubernetes-auth role (`auth = "kubernetes"`).
    #[serde(default)]
    pub k8s_role: Option<String>,
    /// Cache TTL (default 30 s).
    #[serde(default = "default_ttl", deserialize_with = "de_duration")]
    pub cache_ttl: Duration,
}

/// `[secrets.k8s]` (interpreted by 06-13).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct K8sConfig {
    /// Namespace (default: the in-cluster namespace).
    #[serde(default)]
    pub namespace: Option<String>,
    /// Kubeconfig path (default: in-cluster config, then `KUBECONFIG`).
    #[serde(default)]
    pub kubeconfig: Option<PathBuf>,
    /// Cache TTL (default 30 s).
    #[serde(default = "default_ttl", deserialize_with = "de_duration")]
    pub cache_ttl: Duration,
}

impl Default for K8sConfig {
    fn default() -> Self {
        Self {
            namespace: None,
            kubeconfig: None,
            cache_ttl: DEFAULT_CACHE_TTL,
        }
    }
}

fn default_mount() -> String {
    "secret".to_owned()
}

fn default_ttl() -> Duration {
    DEFAULT_CACHE_TTL
}

fn de_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let s = String::deserialize(d)?;
    parse_duration(&s).map_err(de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_decisions() {
        let c = SecretsConfig::default();
        assert_eq!(
            c.providers,
            [ProviderKind::Sealed, ProviderKind::Env, ProviderKind::File]
        );
        assert_eq!(c.env_prefix, "OSTRO_SECRET_");
        assert!(c.sealing_key_file.is_none() && c.file_dir.is_none());
        assert!(c.vault.is_none() && c.k8s.is_none());
        c.validate().unwrap();
        let empty: SecretsConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(empty, c);
    }

    #[test]
    fn every_key_parses() {
        let c: SecretsConfig = serde_json::from_value(serde_json::json!({
            "providers": ["env", "vault", "k8s", "sealed", "file"],
            "sealing_key_file": "/etc/ostro/sealing.key",
            "old_sealing_key_files": ["/etc/ostro/old.key"],
            "env_prefix": "MY_SECRET_",
            "file_dir": "/run/secrets",
            "vault": {
                "addr": "https://vault:8200",
                "mount": "kv",
                "auth": "approle",
                "token_file": "/t",
                "role_id": "r",
                "secret_id_file": "/s",
                "k8s_role": "k",
                "cache_ttl": "1m"
            },
            "k8s": { "namespace": "ostro", "kubeconfig": "/kc", "cache_ttl": "5s" }
        }))
        .unwrap();
        c.validate().unwrap();
        assert_eq!(
            c.old_sealing_key_files,
            [PathBuf::from("/etc/ostro/old.key")]
        );
        assert_eq!(c.providers[1], ProviderKind::Vault);
        let v = c.vault.unwrap();
        assert_eq!(v.auth, VaultAuth::Approle);
        assert_eq!(v.mount, "kv");
        assert_eq!(v.cache_ttl, Duration::from_secs(60));
        let k = c.k8s.unwrap();
        assert_eq!(k.cache_ttl, Duration::from_secs(5));
        assert_eq!(k.namespace.as_deref(), Some("ostro"));

        let v: VaultConfig =
            serde_json::from_value(serde_json::json!({"addr": "http://v:8200"})).unwrap();
        assert_eq!(v.mount, "secret");
        assert_eq!(v.auth, VaultAuth::Token);
        assert_eq!(v.cache_ttl, DEFAULT_CACHE_TTL);
        assert_eq!(K8sConfig::default().cache_ttl, DEFAULT_CACHE_TTL);
    }

    #[test]
    fn rejects_bad_config() {
        let parse = |v: serde_json::Value| serde_json::from_value::<SecretsConfig>(v);
        assert!(parse(serde_json::json!({"providers": ["ldap"]})).is_err());
        assert!(parse(serde_json::json!({"unknown": 1})).is_err());
        assert!(parse(serde_json::json!({"vault": {"mount": "x"}})).is_err());
        assert!(parse(serde_json::json!({"k8s": {"cache_ttl": "7d"}})).is_err());
        let dup = parse(serde_json::json!({"providers": ["env", "env"]})).unwrap();
        assert!(dup.validate().unwrap_err().contains("twice"));
        let vault = parse(serde_json::json!({"providers": ["vault"]})).unwrap();
        assert!(vault.validate().is_err());
        let k8s = parse(serde_json::json!({"providers": ["k8s"]})).unwrap();
        assert!(k8s.validate().is_err());
        let pre = parse(serde_json::json!({"env_prefix": "lower_"})).unwrap();
        assert!(pre.validate().is_err());
    }
}
