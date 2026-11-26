//! Phase 14 client surface for module security (`SecurityApi`: policy, trusted keys,
//! signatures): implemented in **14-19** (with 14-17's shared pieces). Stubs answer
//! `unimplemented` until then.

use serde_json::Value;

use crate::client::Client;
use crate::error::ClientError;

impl Client {
    /// `GET /v1/security/modules/policy`. Stub.
    pub async fn get_module_policy(&self) -> Result<Value, ClientError> {
        Err(crate::lands_in("get_module_policy", "14-19"))
    }
}
