//! Phase 14 client surface for API tokens (`ApiTokensApi`: create, list, revoke, rotate,
//! whoami): implemented in **14-17**. Stubs answer `unimplemented` until then.

use serde_json::Value;

use crate::client::Client;
use crate::error::ClientError;

impl Client {
    /// `GET /v1/auth/whoami`. Stub.
    pub async fn whoami(&self) -> Result<Value, ClientError> {
        Err(crate::lands_in("whoami", "14-17"))
    }
}
