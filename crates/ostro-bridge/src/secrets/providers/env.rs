//! The environment provider: secret `stripe/live-key.v2` is read from
//! `OSTRO_SECRET_STRIPE_LIVE_KEY_V2` (the configured prefix plus the name upper-cased, with
//! `-`, `.` and `/` mapped to `_`).
//!
//! Distinct names can map to the same variable (`a-b` and `a_b`); operators should avoid such
//! pairs. The variable is read on every call, so a restart is not needed after a change in
//! the process environment (rare in practice).

use std::ffi::OsString;

use secrecy::SecretBox;

use super::{ProviderError, SecretProvider};
use crate::secrets::config::DEFAULT_ENV_PREFIX;
use crate::secrets::template;
use crate::secrets::value::SecretValue;

/// Reads secrets from environment variables.
#[derive(Clone, Debug)]
pub struct EnvProvider {
    prefix: String,
}

impl Default for EnvProvider {
    fn default() -> Self {
        Self::new(DEFAULT_ENV_PREFIX)
    }
}

impl EnvProvider {
    /// A provider with `prefix` (e.g. `OSTRO_SECRET_`).
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// The variable name for secret `name`.
    pub fn var_name(&self, name: &str) -> String {
        let mut v = self.prefix.clone();
        v.extend(name.chars().map(|c| match c {
            '-' | '.' | '/' => '_',
            c => c.to_ascii_uppercase(),
        }));
        v
    }

    fn value_of(
        &self,
        name: &str,
        var: Option<OsString>,
    ) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        template::validate_name(name).map_err(|e| ProviderError::Invalid(e.to_string()))?;
        Ok(var.map(|v| SecretValue::new(v.into_encoded_bytes()).into_box()))
    }
}

#[async_trait::async_trait]
impl SecretProvider for EnvProvider {
    fn name(&self) -> &'static str {
        "env"
    }

    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        let var = std::env::var_os(self.var_name(name));
        self.value_of(name, var)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn var_name_mapping() {
        let p = EnvProvider::default();
        assert_eq!(
            p.var_name("stripe/live-key.v2"),
            "OSTRO_SECRET_STRIPE_LIVE_KEY_V2"
        );
        assert_eq!(p.var_name("a_b"), "OSTRO_SECRET_A_B");
        assert_eq!(EnvProvider::new("X_").var_name("db/pass"), "X_DB_PASS");
    }

    #[test]
    fn value_of_validates_names() {
        let p = EnvProvider::default();
        let v = p
            .value_of("k", Some(OsString::from("canary-env-value")))
            .unwrap()
            .unwrap();
        assert_eq!(v.expose_secret(), b"canary-env-value");
        assert!(p.value_of("k", None).unwrap().is_none());
        assert!(matches!(
            p.value_of("__ostro/token-key", Some(OsString::from("x"))),
            Err(ProviderError::Invalid(_))
        ));
        assert!(p.value_of("a/../b", None).is_err());
    }
}
