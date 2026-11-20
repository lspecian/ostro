//! Secret providers (06-02 contract). 06-05 fills `sealed`, `env`, `file` and `chain`; 06-13
//! fills `cache`, `vault` (feature `vault`) and `k8s` (feature `k8s`).

pub mod cache;
pub mod chain;
pub mod env;
pub mod file;
#[cfg(feature = "k8s")]
pub mod k8s;
pub mod sealed;
#[cfg(feature = "vault")]
pub mod vault;

/// One source of secret values.
#[async_trait::async_trait]
pub trait SecretProvider: Send + Sync {
    /// Provider name (`"sealed"`, `"env"`, `"file"`, `"vault"`, `"k8s"`).
    fn name(&self) -> &'static str;
    /// None = not found here (the chain tries the next provider). Never log the value.
    async fn get(&self, name: &str) -> Result<Option<secrecy::SecretBox<[u8]>>, ProviderError>;
}

/// A provider failed (never carries a secret value).
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// The provider cannot be reached or is not configured.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// The stored value or the request is invalid.
    #[error("invalid: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    struct Fixed;

    #[async_trait::async_trait]
    impl SecretProvider for Fixed {
        fn name(&self) -> &'static str {
            "fixed"
        }
        async fn get(&self, name: &str) -> Result<Option<secrecy::SecretBox<[u8]>>, ProviderError> {
            Ok((name == "k").then(|| secrecy::SecretBox::new(b"canary-value-123".to_vec().into())))
        }
    }

    #[tokio::test]
    async fn provider_is_object_safe_and_debug_redacts() {
        let p: Box<dyn SecretProvider> = Box::new(Fixed);
        assert_eq!(p.name(), "fixed");
        let v = p.get("k").await.unwrap().unwrap();
        assert_eq!(v.expose_secret(), b"canary-value-123");
        assert!(!format!("{v:?}").contains("canary"));
        assert!(p.get("other").await.unwrap().is_none());
    }
}
