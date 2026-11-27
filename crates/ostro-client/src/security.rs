//! Phase 14 client surface for module security (`SecurityApi`, D-P14-29 REST paths; 14-19).
//!
//! Raw methods return the response body ([`Response`]) so `ostro security … --json` prints
//! it verbatim; non-2xx answers are [`CallError::Api`] with the server's error body
//! (`reason()` e.g. `signature_required`, `signature_invalid`).
//!
//! | method | path | client |
//! |--------|------|--------|
//! | GET | `/v1/security/modules/policy` | [`Client::get_module_policy`] |
//! | PUT | `/v1/security/modules/policy` | [`Client::set_module_policy`] |
//! | POST | `/v1/security/modules/keys` | [`Client::add_trusted_key`] |
//! | DELETE | `/v1/security/modules/keys/{key_id}` | [`Client::remove_trusted_key`] (`<tenant>:<key id>` names a tenant key) |
//! | POST | `/v1/modules/{sha}/signatures` | [`Client::attach_signatures`] |
//! | PUT | `/v1/modules/{sha}` + `Ostro-Module-Signature` | [`Client::upload_module_signed`] |

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use reqwest::Method;
use serde_json::{Value, json};

use crate::client::{CallError, Client, Response};

/// The upload signature header (one per signature file, base64 of the file).
pub const SIGNATURE_HEADER: &str = "ostro-module-signature";
/// Largest signature file the server accepts (decoded bytes).
pub const MAX_SIGNATURE_BYTES: usize = 8 * 1024;
/// Most signatures per upload.
pub const MAX_SIGNATURES: usize = 4;
/// `key_id` asking the server to derive the id from the key.
pub const AUTO_KEY_ID: &str = "auto";

impl Client {
    /// `GET /v1/security/modules/policy` -> `ModulePolicy`.
    pub async fn get_module_policy(&self) -> Result<Response, CallError> {
        self.call(
            Method::GET,
            &["v1", "security", "modules", "policy"],
            &[],
            None,
        )
        .await
    }

    /// `PUT /v1/security/modules/policy` `{require_signed_modules}` (an empty
    /// `trusted_keys` keeps the cluster keys).
    pub async fn set_module_policy(&self, require_signed: bool) -> Result<Response, CallError> {
        self.call(
            Method::PUT,
            &["v1", "security", "modules", "policy"],
            &[],
            Some(&json!({ "require_signed_modules": require_signed })),
        )
        .await
    }

    /// `POST /v1/security/modules/keys` `TrustedKeySpec`: `alg` `minisign` / `cosign-p256`,
    /// `key_id` the key's id or [`AUTO_KEY_ID`] (the server derives it).
    pub async fn add_trusted_key(
        &self,
        alg: &str,
        key_id: &str,
        public_key: &str,
        comment: Option<&str>,
        tenant: Option<&str>,
    ) -> Result<Response, CallError> {
        let body = json!({
            "alg": alg,
            "key_id": key_id,
            "public_key": public_key,
            "comment": comment.unwrap_or_default(),
            "tenant": tenant,
        });
        self.call(
            Method::POST,
            &["v1", "security", "modules", "keys"],
            &[],
            Some(&body),
        )
        .await
    }

    /// `DELETE /v1/security/modules/keys/{key_id}` (`<tenant>:<key id>` for a tenant key).
    pub async fn remove_trusted_key(&self, key_id: &str) -> Result<Response, CallError> {
        self.call(
            Method::DELETE,
            &["v1", "security", "modules", "keys", key_id],
            &[],
            None,
        )
        .await
    }

    /// `POST /v1/modules/{sha}/signatures` with each signature file base64-encoded.
    pub async fn attach_signatures(
        &self,
        sha256: &str,
        signatures: &[Vec<u8>],
    ) -> Result<Response, CallError> {
        let sigs: Vec<Value> = signatures.iter().map(|s| json!(B64.encode(s))).collect();
        self.call(
            Method::POST,
            &["v1", "modules", sha256, "signatures"],
            &[],
            Some(&json!({ "signatures": sigs })),
        )
        .await
    }

    /// [`Client::upload_module`] with one `Ostro-Module-Signature` header per signature file.
    pub async fn upload_module_signed(
        &self,
        sha256: &str,
        wasm: Vec<u8>,
        name: Option<&str>,
        tenant: Option<&str>,
        signatures: &[Vec<u8>],
    ) -> Result<Response, CallError> {
        let mut q = Vec::new();
        if let Some(n) = name {
            q.push(("name", n.to_owned()));
        }
        if let Some(t) = tenant {
            q.push(("tenant", t.to_owned()));
        }
        let mut rb = self
            .request(Method::PUT, self.url(&["v1", "modules", sha256], &q))
            .header(reqwest::header::CONTENT_TYPE, "application/wasm");
        for s in signatures {
            rb = rb.header(SIGNATURE_HEADER, B64.encode(s));
        }
        self.send(rb.body(wasm)).await
    }
}
