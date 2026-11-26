//! Phase 14 client surface for the audit query (`AuditApi`): implemented in **14-18**. Stubs
//! answer `unimplemented` until then.

use serde_json::Value;

use crate::client::Client;
use crate::error::ClientError;

impl Client {
    /// `GET /v1/audit`. Stub.
    pub async fn audit_query(&self) -> Result<Value, ClientError> {
        Err(crate::lands_in("audit_query", "14-18"))
    }
}
