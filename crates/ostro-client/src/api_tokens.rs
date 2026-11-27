//! Phase 14 client surface for API tokens (`ApiTokensApi`; 14-17): create, list, revoke,
//! rotate, whoami.
//!
//! Raw methods return the response body ([`Response`]); non-2xx answers are
//! [`CallError::Api`] with the server's error body ([`crate::ClientError::from`] for the
//! typed error). `create_token` / `rotate_token` answers carry the bearer, the only time it
//! is ever shown; listings never carry a secret or a hash.
//!
//! | method | path | client |
//! |--------|------|--------|
//! | GET | `/v1/auth/tokens[?tenant=]` | [`Client::list_api_tokens`] |
//! | POST | `/v1/auth/tokens` | [`Client::create_api_token`] |
//! | DELETE | `/v1/auth/tokens/{id}` | [`Client::revoke_api_token`] |
//! | POST | `/v1/auth/tokens/{id}/rotate` | [`Client::rotate_api_token`] |
//! | GET | `/v1/auth/whoami` | [`Client::whoami`] |

use reqwest::Method;
use serde_json::Value;

use crate::client::{CallError, Client, Response};

impl Client {
    /// `POST /v1/auth/tokens` `CreateTokenRequest` -> 201 `{info, bearer}`.
    pub async fn create_api_token(&self, body: &Value) -> Result<Response, CallError> {
        self.call(Method::POST, &["v1", "auth", "tokens"], &[], Some(body))
            .await
    }

    /// `GET /v1/auth/tokens[?tenant=t]` -> `{tokens: [TokenInfo]}`.
    pub async fn list_api_tokens(&self, tenant: Option<&str>) -> Result<Response, CallError> {
        let q: Vec<(&str, String)> = tenant
            .map(|t| ("tenant", t.to_owned()))
            .into_iter()
            .collect();
        self.call(Method::GET, &["v1", "auth", "tokens"], &q, None)
            .await
    }

    /// `DELETE /v1/auth/tokens/{id}` -> `{id, revoked}`.
    pub async fn revoke_api_token(&self, id: &str) -> Result<Response, CallError> {
        self.call(Method::DELETE, &["v1", "auth", "tokens", id], &[], None)
            .await
    }

    /// `POST /v1/auth/tokens/{id}/rotate` `{revoke_old, ttl_ms?}` -> 201 `{info, bearer}`.
    pub async fn rotate_api_token(&self, id: &str, body: &Value) -> Result<Response, CallError> {
        self.call(
            Method::POST,
            &["v1", "auth", "tokens", id, "rotate"],
            &[],
            Some(body),
        )
        .await
    }

    /// `GET /v1/auth/whoami` -> `{token_id, tenant, scopes, kind}`.
    pub async fn whoami(&self) -> Result<Response, CallError> {
        self.call(Method::GET, &["v1", "auth", "whoami"], &[], None)
            .await
    }
}
