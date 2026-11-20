//! Origin-bound secret grants (FX-07) over the existing `Grant` meta record, by convention
//! (06-DECISIONS, no new record variant):
//!
//! ```text
//! Grant{ id: "secret:<module>:<secret>", subject: "module:<module>",
//!        scopes: ["origin:https://api.stripe.com", "origin:https://*.example.com:8443", "body"?] }
//! ```
//!
//! `<module>` is the tenant-qualified deployed module name (`<tenant>/<name>`, D23), so
//! redeploys keep their grants. A request may carry `{{secret:S}}` only if module `M` holds
//! the grant `secret:M:S`, the request origin matches one of its origin patterns, and (for a
//! body placeholder) the grant has the `body` scope. Anything else is
//! `NonRetryable{kind: secret_not_granted}`, decided before anything is sent.

use ostro_meta::records::Grant;
use ostro_types::journal::Failure;

use super::template::{self, TemplateError};
use crate::kinds::SECRET_NOT_GRANTED;
use crate::origin::{Origin, OriginPattern, OriginPatternError};

/// Grant id prefix (RESERVATIONS §11).
pub const GRANT_ID_PREFIX: &str = "secret:";
/// Grant subject prefix (06-02 `MetaView::grants_for`).
pub const SUBJECT_PREFIX: &str = "module:";
/// Scope prefix of an origin pattern.
pub const ORIGIN_SCOPE_PREFIX: &str = "origin:";
/// Scope allowing placeholders in the request body.
pub const BODY_SCOPE: &str = "body";

/// A parsed secret grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantSpec {
    /// Tenant-qualified deployed module name.
    pub module: String,
    /// Secret name (template grammar).
    pub secret: String,
    /// Origins the secret may be sent to (at least one).
    pub origins: Vec<OriginPattern>,
    /// Whether body placeholders are allowed.
    pub body: bool,
}

/// A `Grant` record is not a valid secret grant.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GrantError {
    /// The id does not start with `secret:`: not a secret grant.
    #[error("grant id {0:?} is not a secret grant (expected `secret:<module>:<secret>`)")]
    NotSecretGrant(String),
    /// The subject is not `module:<module>`.
    #[error("grant subject {0:?} is not `module:<module>`")]
    BadSubject(String),
    /// The module name is empty or contains `:`.
    #[error("invalid module name {0:?}")]
    BadModule(String),
    /// The id does not match `secret:<module>:<secret>` for the subject's module.
    #[error("grant id {id:?} does not match subject module {module:?}")]
    IdMismatch {
        /// The record id.
        id: String,
        /// The module from the subject.
        module: String,
    },
    /// The secret name is invalid.
    #[error("invalid secret name: {0}")]
    BadSecret(#[from] TemplateError),
    /// An origin scope does not parse (a bare `*` is rejected here).
    #[error(transparent)]
    BadOrigin(#[from] OriginPatternError),
    /// An unknown scope.
    #[error("unknown grant scope {0:?}")]
    UnknownScope(String),
    /// No origin scope.
    #[error("a secret grant needs at least one `origin:` scope")]
    NoOrigins,
}

fn check_module(module: &str) -> Result<(), GrantError> {
    if module.is_empty() || module.contains(':') {
        return Err(GrantError::BadModule(module.to_owned()));
    }
    Ok(())
}

impl GrantSpec {
    /// `secret:<module>:<secret>`.
    pub fn id(module: &str, secret: &str) -> String {
        format!("{GRANT_ID_PREFIX}{module}:{secret}")
    }

    /// Checks the module, the secret name and that there is at least one origin.
    pub fn validate(&self) -> Result<(), GrantError> {
        check_module(&self.module)?;
        template::validate_name(&self.secret)?;
        if self.origins.is_empty() {
            return Err(GrantError::NoOrigins);
        }
        Ok(())
    }

    /// The meta record (`ext` empty). Origin scopes use the normalised pattern form.
    pub fn to_record(&self) -> Grant {
        let mut scopes: Vec<String> = self
            .origins
            .iter()
            .map(|o| format!("{ORIGIN_SCOPE_PREFIX}{o}"))
            .collect();
        if self.body {
            scopes.push(BODY_SCOPE.to_owned());
        }
        Grant {
            id: Self::id(&self.module, &self.secret),
            subject: format!("{SUBJECT_PREFIX}{}", self.module),
            scopes,
            ext: Vec::new(),
        }
    }

    /// Parses a `Grant` record. Unknown scopes, an id that does not match the subject, an
    /// invalid secret name and grants without origins are rejected.
    pub fn from_record(g: &Grant) -> Result<Self, GrantError> {
        let id_rest =
            g.id.strip_prefix(GRANT_ID_PREFIX)
                .ok_or_else(|| GrantError::NotSecretGrant(g.id.clone()))?;
        let module = g
            .subject
            .strip_prefix(SUBJECT_PREFIX)
            .ok_or_else(|| GrantError::BadSubject(g.subject.clone()))?;
        check_module(module)?;
        let secret = id_rest
            .strip_prefix(module)
            .and_then(|r| r.strip_prefix(':'))
            .ok_or_else(|| GrantError::IdMismatch {
                id: g.id.clone(),
                module: module.to_owned(),
            })?;
        template::validate_name(secret)?;
        let mut origins = Vec::new();
        let mut body = false;
        for scope in &g.scopes {
            if scope == BODY_SCOPE {
                body = true;
            } else if let Some(p) = scope.strip_prefix(ORIGIN_SCOPE_PREFIX) {
                origins.push(OriginPattern::parse(p)?);
            } else {
                return Err(GrantError::UnknownScope(scope.clone()));
            }
        }
        let spec = Self {
            module: module.to_owned(),
            secret: secret.to_owned(),
            origins,
            body,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Whether `origin` matches one of the grant's origin patterns.
    pub fn allows_origin(&self, origin: &Origin) -> bool {
        self.origins.iter().any(|p| p.matches(origin))
    }
}

fn not_granted(message: String) -> Failure {
    super::non_retryable(SECRET_NOT_GRANTED, message)
}

/// Checks that `module` may send secret `secret` to `origin` (in the body if `in_body`).
///
/// `grants` is typically `MetaView::grants_for(module)`; records with other ids or subjects
/// are ignored. Failures are `NonRetryable{kind: secret_not_granted}`; the message names the
/// secret, the module and the origin, never a value.
pub fn check_grant(
    grants: &[Grant],
    module: &str,
    secret: &str,
    origin: &Origin,
    in_body: bool,
) -> Result<(), Failure> {
    let id = GrantSpec::id(module, secret);
    let subject = format!("{SUBJECT_PREFIX}{module}");
    let Some(record) = grants.iter().find(|g| g.id == id && g.subject == subject) else {
        return Err(not_granted(format!(
            "secret {secret:?} is not granted to module {module:?}"
        )));
    };
    let spec = GrantSpec::from_record(record).map_err(|e| {
        not_granted(format!(
            "grant for secret {secret:?} of module {module:?} is invalid: {e}"
        ))
    })?;
    if !spec.allows_origin(origin) {
        return Err(not_granted(format!(
            "secret {secret:?} is not granted to module {module:?} for origin {origin}"
        )));
    }
    if in_body && !spec.body {
        return Err(not_granted(format!(
            "secret {secret:?} is not granted to module {module:?} for use in a request body"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    fn origin(s: &str) -> Origin {
        Origin::from_url(&Url::parse(s).unwrap()).unwrap()
    }

    fn spec(body: bool) -> GrantSpec {
        GrantSpec {
            module: "acme/billing".into(),
            secret: "stripe/live-key".into(),
            origins: vec![
                OriginPattern::parse("https://api.stripe.com").unwrap(),
                OriginPattern::parse("https://*.example.com:8443").unwrap(),
            ],
            body,
        }
    }

    fn kind_and_message(r: Result<(), Failure>) -> (String, String) {
        match r.unwrap_err() {
            Failure::NonRetryable(i) => (i.kind, i.message),
            other => panic!("expected NonRetryable, got {other:?}"),
        }
    }

    #[test]
    fn record_round_trip() {
        let s = spec(true);
        let r = s.to_record();
        assert_eq!(r.id, "secret:acme/billing:stripe/live-key");
        assert_eq!(r.subject, "module:acme/billing");
        assert_eq!(
            r.scopes,
            [
                "origin:https://api.stripe.com",
                "origin:https://*.example.com:8443",
                "body"
            ]
        );
        assert_eq!(GrantSpec::from_record(&r).unwrap(), s);
        let r2 = spec(false).to_record();
        assert!(!r2.scopes.contains(&"body".to_owned()));
        assert_eq!(GrantSpec::from_record(&r2).unwrap(), spec(false));
    }

    #[test]
    fn from_record_rejects_bad_records() {
        let base = spec(false).to_record();
        let with = |f: &dyn Fn(&mut Grant)| {
            let mut g = base.clone();
            f(&mut g);
            GrantSpec::from_record(&g)
        };
        assert!(matches!(
            with(&|g| g.scopes.push("write".into())),
            Err(GrantError::UnknownScope(_))
        ));
        assert!(matches!(
            with(&|g| g.scopes.push("origin:*".into())),
            Err(GrantError::BadOrigin(_))
        ));
        assert!(matches!(
            with(&|g| g.scopes = vec!["origin:https://*".into()]),
            Err(GrantError::BadOrigin(_))
        ));
        assert!(matches!(
            with(&|g| g.scopes = vec!["body".into()]),
            Err(GrantError::NoOrigins)
        ));
        assert!(matches!(
            with(&|g| g.id = "conn:acme/billing:x".into()),
            Err(GrantError::NotSecretGrant(_))
        ));
        assert!(matches!(
            with(&|g| g.subject = "user:x".into()),
            Err(GrantError::BadSubject(_))
        ));
        assert!(matches!(
            with(&|g| g.subject = "module:other/mod".into()),
            Err(GrantError::IdMismatch { .. })
        ));
        assert!(matches!(
            with(&|g| g.id = "secret:acme/billing:__ostro/token-key".into()),
            Err(GrantError::BadSecret(TemplateError::Reserved { .. }))
        ));
        assert!(matches!(
            with(&|g| g.id = "secret:acme/billing:a/../b".into()),
            Err(GrantError::BadSecret(TemplateError::BadPath { .. }))
        ));
    }

    #[test]
    fn check_grant_allows_matching_origin() {
        let grants = [spec(true).to_record()];
        let m = "acme/billing";
        let s = "stripe/live-key";
        check_grant(&grants, m, s, &origin("https://api.stripe.com/v1/x"), false).unwrap();
        check_grant(&grants, m, s, &origin("https://api.stripe.com"), true).unwrap();
        check_grant(&grants, m, s, &origin("https://a.example.com:8443"), false).unwrap();
    }

    #[test]
    fn check_grant_refusals() {
        let m = "acme/billing";
        let s = "stripe/live-key";
        let no_body = [spec(false).to_record()];

        let (k, msg) = kind_and_message(check_grant(
            &[],
            m,
            s,
            &origin("https://api.stripe.com"),
            false,
        ));
        assert_eq!(k, SECRET_NOT_GRANTED);
        assert!(msg.contains(s) && msg.contains(m), "{msg}");

        let (k, msg) = kind_and_message(check_grant(
            &no_body,
            m,
            s,
            &origin("https://evil.test"),
            false,
        ));
        assert_eq!(k, SECRET_NOT_GRANTED);
        assert!(
            msg.contains("https://evil.test") && msg.contains(s),
            "{msg}"
        );

        // Same host, other scheme / port: refused.
        for o in [
            "http://api.stripe.com",
            "https://api.stripe.com:444",
            "https://example.com:8443",
        ] {
            let (k, _) = kind_and_message(check_grant(&no_body, m, s, &origin(o), false));
            assert_eq!(k, SECRET_NOT_GRANTED, "{o}");
        }

        let (k, msg) = kind_and_message(check_grant(
            &no_body,
            m,
            s,
            &origin("https://api.stripe.com"),
            true,
        ));
        assert_eq!(k, SECRET_NOT_GRANTED);
        assert!(msg.contains("body"), "{msg}");

        // A grant for another module or another secret does not count.
        let (k, _) = kind_and_message(check_grant(
            &no_body,
            "acme/other",
            s,
            &origin("https://api.stripe.com"),
            false,
        ));
        assert_eq!(k, SECRET_NOT_GRANTED);
        let (k, _) = kind_and_message(check_grant(
            &no_body,
            m,
            "stripe/other",
            &origin("https://api.stripe.com"),
            false,
        ));
        assert_eq!(k, SECRET_NOT_GRANTED);

        // A malformed record is a refusal, not a panic.
        let mut bad = spec(false).to_record();
        bad.scopes.push("admin".into());
        let (k, msg) = kind_and_message(check_grant(
            &[bad],
            m,
            s,
            &origin("https://api.stripe.com"),
            false,
        ));
        assert_eq!(k, SECRET_NOT_GRANTED);
        assert!(msg.contains("invalid"), "{msg}");
    }
}
