//! [`SecretValue`]: a resolved secret held in a zeroize-on-drop [`SecretBox`].
//!
//! The bytes are reachable only through [`SecretValue::expose`]; `Debug` prints the length
//! and never the value, and there is no `Display`.

use std::fmt;

use ostro_types::journal::Failure;
use secrecy::{ExposeSecret, SecretBox};
use zeroize::Zeroize;

use crate::kinds::SECRET_NOT_GRANTED;

/// Values shorter than this cannot be templated (06-DECISIONS: too guessable, and the
/// scrubber would produce false positives).
pub const MIN_TEMPLATE_LEN: usize = 8;

/// A resolved secret value. Zeroized on drop; redacted in `Debug`.
pub struct SecretValue(SecretBox<[u8]>);

impl SecretValue {
    /// Takes ownership of `bytes`. If the vector has spare capacity it is copied into an
    /// exact-size box and the original buffer is zeroized, so no unzeroized copy is left
    /// behind by the shrink.
    pub fn new(mut bytes: Vec<u8>) -> Self {
        let boxed: Box<[u8]> = if bytes.capacity() == bytes.len() {
            bytes.into_boxed_slice()
        } else {
            let exact: Box<[u8]> = bytes.as_slice().into();
            bytes.zeroize();
            exact
        };
        Self(SecretBox::new(boxed))
    }

    /// Wraps a provider's [`SecretBox`].
    pub fn from_box(b: SecretBox<[u8]>) -> Self {
        Self(b)
    }

    /// Copies `bytes` into a new value (the caller still owns and must zeroize `bytes`).
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self(SecretBox::new(bytes.into()))
    }

    /// The secret bytes. Never log, format or persist them.
    pub fn expose(&self) -> &[u8] {
        self.0.expose_secret()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.expose().len()
    }

    /// Whether the value is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Converts into a provider-style [`SecretBox`].
    pub fn into_box(self) -> SecretBox<[u8]> {
        self.0
    }

    /// `self` if it is long enough to be templated (`>= MIN_TEMPLATE_LEN` bytes); otherwise
    /// `NonRetryable{kind: secret_not_granted}`. The message carries no part of the value.
    pub fn for_templating(self) -> Result<Self, Failure> {
        if self.len() < MIN_TEMPLATE_LEN {
            return Err(super::non_retryable(
                SECRET_NOT_GRANTED,
                "secret shorter than 8 bytes cannot be templated".to_owned(),
            ));
        }
        Ok(self)
    }
}

impl Clone for SecretValue {
    fn clone(&self) -> Self {
        Self::from_slice(self.expose())
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretValue([REDACTED; {} bytes])", self.len())
    }
}

impl From<SecretBox<[u8]>> for SecretValue {
    fn from(b: SecretBox<[u8]>) -> Self {
        Self::from_box(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANARY: &[u8] = b"canary-7f3a9e-value";

    #[test]
    fn debug_is_redacted() {
        let v = SecretValue::new(CANARY.to_vec());
        let d = format!("{v:?}");
        assert_eq!(
            d,
            format!("SecretValue([REDACTED; {} bytes])", CANARY.len())
        );
        assert!(!d.contains("canary"));
        assert!(!format!("{:?}", v.clone()).contains("canary"));
        assert!(!format!("{:#?}", Some(&v)).contains("canary"));
    }

    #[test]
    fn expose_len_and_spare_capacity() {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(CANARY);
        let v = SecretValue::new(bytes);
        assert_eq!(v.expose(), CANARY);
        assert_eq!(v.len(), CANARY.len());
        assert!(!v.is_empty());
        let b = v.clone().into_box();
        assert_eq!(SecretValue::from(b).expose(), CANARY);
        assert!(SecretValue::new(Vec::new()).is_empty());
    }

    #[test]
    fn short_values_cannot_be_templated() {
        assert!(
            SecretValue::new(b"12345678".to_vec())
                .for_templating()
                .is_ok()
        );
        let err = SecretValue::new(b"1234567".to_vec())
            .for_templating()
            .unwrap_err();
        let Failure::NonRetryable(info) = err else {
            panic!("expected NonRetryable")
        };
        assert_eq!(info.kind, SECRET_NOT_GRANTED);
        assert_eq!(
            info.message,
            "secret shorter than 8 bytes cannot be templated"
        );
        assert!(!info.message.contains("1234567"));
    }
}
