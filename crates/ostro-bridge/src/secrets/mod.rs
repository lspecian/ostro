//! Secrets (06-05; FX-06, FX-07, part of FX-10): secret values, `{{secret:NAME}}` templates,
//! origin-bound grants, scrubbing, the sealed store, the providers and the resolver.
//!
//! A resolved value lives in a [`SecretValue`] (zeroized on drop, redacted in `Debug`) and
//! never reaches a log line, an error message, a panic message, the journal or a snapshot.
//! "Zeroized" covers Ostro-owned buffers only: hyper/rustls internals and `HeaderValue`
//! bytes are outside our control.

pub mod config;
pub mod grants;
pub mod providers;
pub mod scrub;
pub mod sealed;
pub mod template;
pub mod value;

pub use config::SecretsConfig;
pub use grants::{GrantError, GrantSpec, check_grant};
pub use scrub::Scrubber;
pub use sealed::{SealError, SealedStore, SealingKeyRing};
pub use template::{Placeholder, TemplateError};
pub use value::{MIN_TEMPLATE_LEN, SecretValue};

use ostro_types::journal::{ErrorInfo, Failure};

/// `Failure::NonRetryable` with `kind` and `message` (never a secret value) and no details.
pub(crate) fn non_retryable(kind: &str, message: String) -> Failure {
    Failure::NonRetryable(ErrorInfo {
        kind: kind.to_owned(),
        message,
        details: None,
    })
}
