//! Phase 14 client surface for tenants (`TenantsApi`, D-P14-29 REST paths; 14-17).
//!
//! Raw methods return the response body ([`Response`]) so `ostro tenant … --json` prints it
//! verbatim; non-2xx answers are [`CallError::Api`] with the server's error body
//! ([`crate::ClientError::from`] gives the typed error, `reason()` e.g. `tenant_not_empty`).
//! The bearer is the client's (`OSTRO_TOKEN`, [`crate::bearer_from_env`]).
//!
//! | method | path | client |
//! |--------|------|--------|
//! | GET | `/v1/tenants` | [`Client::list_tenants`] |
//! | POST | `/v1/tenants` | [`Client::create_tenant`] |
//! | GET | `/v1/tenants/{id}` | [`Client::get_tenant`] |
//! | PATCH | `/v1/tenants/{id}` | [`Client::update_tenant`] (`null` clears a quota) |
//! | DELETE | `/v1/tenants/{id}` | [`Client::delete_tenant`] |
//! | POST | `/v1/tenants/{id}/suspend` | [`Client::suspend_tenant`] |
//! | POST | `/v1/tenants/{id}/resume` | [`Client::resume_tenant`] |

use reqwest::Method;
use serde_json::{Value, json};

use crate::client::{CallError, Client, Response};

impl Client {
    /// `GET /v1/tenants` -> `{tenants: [TenantInfo]}`.
    pub async fn list_tenants(&self) -> Result<Response, CallError> {
        self.call(Method::GET, &["v1", "tenants"], &[], None).await
    }

    /// `POST /v1/tenants` `TenantSpec` -> 201 `TenantInfo`.
    pub async fn create_tenant(&self, spec: &Value) -> Result<Response, CallError> {
        self.call(Method::POST, &["v1", "tenants"], &[], Some(spec))
            .await
    }

    /// `GET /v1/tenants/{id}` -> `TenantInfo`.
    pub async fn get_tenant(&self, id: &str) -> Result<Response, CallError> {
        self.call(Method::GET, &["v1", "tenants", id], &[], None)
            .await
    }

    /// `PATCH /v1/tenants/{id}` `TenantPatch` (present keys change, `null` clears).
    pub async fn update_tenant(&self, id: &str, patch: &Value) -> Result<Response, CallError> {
        self.call(Method::PATCH, &["v1", "tenants", id], &[], Some(patch))
            .await
    }

    /// `DELETE /v1/tenants/{id}` -> `{id, deleted}` (409 `tenant_not_empty` unless
    /// suspended and empty).
    pub async fn delete_tenant(&self, id: &str) -> Result<Response, CallError> {
        self.call(Method::DELETE, &["v1", "tenants", id], &[], None)
            .await
    }

    /// `POST /v1/tenants/{id}/suspend` -> `TenantInfo` (idempotent).
    pub async fn suspend_tenant(&self, id: &str) -> Result<Response, CallError> {
        self.call(
            Method::POST,
            &["v1", "tenants", id, "suspend"],
            &[],
            Some(&json!({})),
        )
        .await
    }

    /// `POST /v1/tenants/{id}/resume` -> `TenantInfo` (idempotent).
    pub async fn resume_tenant(&self, id: &str) -> Result<Response, CallError> {
        self.call(
            Method::POST,
            &["v1", "tenants", id, "resume"],
            &[],
            Some(&json!({})),
        )
        .await
    }
}
