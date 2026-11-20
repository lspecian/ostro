//! The provider chain: the ordered `[secrets] providers` list; the first provider that has
//! the secret wins.
//!
//! - `Ok(None)` from a provider: try the next one.
//! - `Unavailable` from a provider: logged (provider and secret name only) and the chain
//!   continues. If no later provider has the secret, the chain returns `Unavailable`, not
//!   `None`, so the caller reports a retryable failure instead of `secret_not_found` while a
//!   provider is down.
//! - `Invalid` (tampered sealed record, a file outside `file_dir`, a bad name): the chain
//!   stops and returns it (fail closed).

use std::fmt;
use std::sync::Arc;

use super::{ProviderError, SecretProvider};
use crate::secrets::value::SecretValue;

/// Ordered secret providers.
#[derive(Clone, Default)]
pub struct ProviderChain(Vec<Arc<dyn SecretProvider>>);

impl fmt::Debug for ProviderChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProviderChain").field(&self.names()).finish()
    }
}

impl ProviderChain {
    /// A chain over `providers`, tried in order.
    pub fn new(providers: Vec<Arc<dyn SecretProvider>>) -> Self {
        Self(providers)
    }

    /// Appends a provider.
    pub fn push(&mut self, p: Arc<dyn SecretProvider>) {
        self.0.push(p);
    }

    /// Provider names, in order.
    pub fn names(&self) -> Vec<&'static str> {
        self.0.iter().map(|p| p.name()).collect()
    }

    /// Whether the chain has no providers.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The first provider's value for `name`; see the module docs for error handling.
    pub async fn get(&self, name: &str) -> Result<Option<SecretValue>, ProviderError> {
        let mut down: Vec<&'static str> = Vec::new();
        for p in &self.0 {
            match p.get(name).await {
                Ok(Some(v)) => return Ok(Some(SecretValue::from_box(v))),
                Ok(None) => {}
                Err(ProviderError::Unavailable(e)) => {
                    tracing::warn!(provider = p.name(), secret = name, error = %e, "secret provider unavailable");
                    down.push(p.name());
                }
                Err(e @ ProviderError::Invalid(_)) => return Err(e),
            }
        }
        if down.is_empty() {
            Ok(None)
        } else {
            Err(ProviderError::Unavailable(format!(
                "secret {name:?} not found; unavailable providers: {}",
                down.join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretBox;

    type Answer = Result<Option<&'static [u8]>, fn(String) -> ProviderError>;

    struct P(&'static str, Answer);

    #[async_trait::async_trait]
    impl SecretProvider for P {
        fn name(&self) -> &'static str {
            self.0
        }
        async fn get(&self, _: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
            match &self.1 {
                Ok(v) => Ok(v.map(|b| SecretBox::new(b.into()))),
                Err(f) => Err(f(format!("{} down", self.0))),
            }
        }
    }

    fn chain(ps: Vec<P>) -> ProviderChain {
        ProviderChain::new(
            ps.into_iter()
                .map(|p| Arc::new(p) as Arc<dyn SecretProvider>)
                .collect(),
        )
    }

    #[tokio::test]
    async fn first_some_wins() {
        let c = chain(vec![
            P("a", Ok(None)),
            P("b", Ok(Some(b"from-b-value"))),
            P("c", Ok(Some(b"from-c-value"))),
        ]);
        assert_eq!(c.get("k").await.unwrap().unwrap().expose(), b"from-b-value");
        assert_eq!(c.names(), ["a", "b", "c"]);
        assert_eq!(format!("{c:?}"), r#"ProviderChain(["a", "b", "c"])"#);
    }

    #[tokio::test]
    async fn unavailable_continues_invalid_stops() {
        let c = chain(vec![
            P("a", Err(ProviderError::Unavailable)),
            P("b", Ok(Some(b"from-b-value"))),
        ]);
        assert_eq!(c.get("k").await.unwrap().unwrap().expose(), b"from-b-value");

        let c = chain(vec![
            P("a", Err(ProviderError::Unavailable)),
            P("b", Ok(None)),
        ]);
        let e = c.get("k").await.unwrap_err();
        assert!(matches!(&e, ProviderError::Unavailable(m) if m.contains("a")));

        let c = chain(vec![
            P("a", Err(ProviderError::Invalid)),
            P("b", Ok(Some(b"from-b-value"))),
        ]);
        assert!(matches!(c.get("k").await, Err(ProviderError::Invalid(_))));

        assert!(
            chain(vec![P("a", Ok(None))])
                .get("k")
                .await
                .unwrap()
                .is_none()
        );
        assert!(ProviderChain::default().get("k").await.unwrap().is_none());
        assert!(ProviderChain::default().is_empty());
    }
}
