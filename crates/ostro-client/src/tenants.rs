//! Phase 14 client surface for tenants (`TenantsApi`, D-P14-29 REST paths): implemented in
//! **14-17**. Stubs answer `unimplemented` until then.

use serde_json::Value;

use crate::client::Client;
use crate::error::ClientError;

impl Client {
    /// `GET /v1/tenants`. Stub.
    pub async fn list_tenants(&self) -> Result<Value, ClientError> {
        Err(crate::lands_in("list_tenants", "14-17"))
    }
}
