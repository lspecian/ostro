//! The Phase 14 seam (D-P11-16): who is asking, and may they do this.
//!
//! Pages need the `read` scope; actions, bulk, rerun and DLQ need `operate`. Phase 11
//! enforces nothing: [`AllowAll`] grants every scope. Phase 14 supplies a real
//! [`UiAuth`] (bearer -> session cookie via `POST /ui/login`).

/// Every scope the UI checks.
pub const SCOPES: &[&str] = &["read", "operate"];

/// Resolves the principal behind a `/ui` request.
pub trait UiAuth: Send + Sync {
    /// The principal for this request, or the rejection to send.
    fn principal(&self, parts: &http::request::Parts) -> Result<UiPrincipal, UiReject>;
}

/// Phase 11's default: every request is a principal with every scope.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl UiAuth for AllowAll {
    fn principal(&self, _parts: &http::request::Parts) -> Result<UiPrincipal, UiReject> {
        Ok(UiPrincipal {
            scopes: SCOPES.iter().map(|s| (*s).to_owned()).collect(),
        })
    }
}

/// An authenticated caller and the scopes it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiPrincipal {
    /// Granted scopes (`read`, `operate`).
    pub scopes: Vec<String>,
}

impl UiPrincipal {
    /// Ok when the principal holds `scope`; otherwise a 403 rejection.
    pub fn require(&self, scope: &str) -> Result<(), UiReject> {
        if self.scopes.iter().any(|s| s == scope) {
            Ok(())
        } else {
            Err(UiReject {
                status: http::StatusCode::FORBIDDEN,
                message: format!("missing scope `{scope}`"),
            })
        }
    }
}

/// A refused `/ui` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UiReject {
    /// Response status (401/403).
    pub status: http::StatusCode,
    /// Human-readable reason (rendered escaped).
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_rejects_missing_scope() {
        let p = UiPrincipal {
            scopes: vec!["read".into()],
        };
        assert!(p.require("read").is_ok());
        let e = p.require("operate").expect_err("no operate");
        assert_eq!(e.status, http::StatusCode::FORBIDDEN);
    }
}
