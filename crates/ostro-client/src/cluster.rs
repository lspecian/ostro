//! Phase 13 client methods (D-P13-23 REST paths): cluster membership, upgrades, run
//! migration and module lifecycle. Signatures fixed by 13-02; every body answers
//! [`unimplemented`] (HTTP 501 `unimplemented`, [`crate::ClientError::code`] =
//! `"unimplemented"`) until 13-16 fills them.

use serde_json::{Value, json};

use crate::client::{CallError, Client, Response};

/// The answer of a method that is not implemented yet (13-16): a 501 `unimplemented` error
/// naming the route.
pub fn unimplemented(route: &str) -> CallError {
    CallError::Api {
        status: 501,
        body: json!({
            "error": {
                "code": "unimplemented",
                "message": format!("{route}: the client method lands in 13-16"),
            }
        }),
    }
}

impl Client {
    /// `POST /v1/cluster/init` `{advertise, rf, min_insync}`.
    pub async fn cluster_init(&self, body: &Value) -> Result<Response, CallError> {
        let _ = body;
        Err(unimplemented("POST /v1/cluster/init"))
    }

    /// `POST /v1/cluster/join` `{token, csr_pem, advertise, node_version, resume_node_id}`.
    pub async fn cluster_join(&self, body: &Value) -> Result<Response, CallError> {
        let _ = body;
        Err(unimplemented("POST /v1/cluster/join"))
    }

    /// `GET /v1/cluster/status`.
    pub async fn cluster_status(&self) -> Result<Response, CallError> {
        Err(unimplemented("GET /v1/cluster/status"))
    }

    /// `POST /v1/cluster/tokens` `{ttl_secs}` (a `njt_` node join token).
    pub async fn create_node_token(&self, body: &Value) -> Result<Response, CallError> {
        let _ = body;
        Err(unimplemented("POST /v1/cluster/tokens"))
    }

    /// `POST /v1/cluster/members/{id}/remove` `{force}`.
    pub async fn cluster_remove(&self, id: u64, body: &Value) -> Result<Response, CallError> {
        let _ = (id, body);
        Err(unimplemented("POST /v1/cluster/members/{id}/remove"))
    }

    /// `POST /v1/cluster/members/{id}/drain`.
    pub async fn cluster_drain(&self, id: u64) -> Result<Response, CallError> {
        let _ = id;
        Err(unimplemented("POST /v1/cluster/members/{id}/drain"))
    }

    /// `POST /v1/cluster/reassign` `{partition, replicas, cancel}`.
    pub async fn cluster_reassign(&self, body: &Value) -> Result<Response, CallError> {
        let _ = body;
        Err(unimplemented("POST /v1/cluster/reassign"))
    }

    /// `POST /v1/cluster/rebalance`.
    pub async fn cluster_rebalance(&self) -> Result<Response, CallError> {
        Err(unimplemented("POST /v1/cluster/rebalance"))
    }

    /// `GET /v1/cluster/upgrade`.
    pub async fn upgrade_status(&self) -> Result<Response, CallError> {
        Err(unimplemented("GET /v1/cluster/upgrade"))
    }

    /// `POST /v1/cluster/upgrade/finalize`.
    pub async fn upgrade_finalize(&self) -> Result<Response, CallError> {
        Err(unimplemented("POST /v1/cluster/upgrade/finalize"))
    }

    /// `POST /v1/cluster/upgrade/hold`.
    pub async fn upgrade_hold(&self) -> Result<Response, CallError> {
        Err(unimplemented("POST /v1/cluster/upgrade/hold"))
    }

    /// `POST /v1/cluster/upgrade/release`.
    pub async fn upgrade_release(&self) -> Result<Response, CallError> {
        Err(unimplemented("POST /v1/cluster/upgrade/release"))
    }

    /// `POST /v1/runs/migrate` `{workflow, from, to_version, filter, limit, dry_run}`.
    pub async fn migrate_runs(&self, body: &Value) -> Result<Response, CallError> {
        let _ = body;
        Err(unimplemented("POST /v1/runs/migrate"))
    }

    /// `DELETE /v1/modules/{sha}` (retire a module).
    pub async fn retire_module(&self, sha: &str) -> Result<Response, CallError> {
        let _ = sha;
        Err(unimplemented("DELETE /v1/modules/{sha}"))
    }

    /// `POST /v1/modules/gc` (one module GC round now).
    pub async fn run_module_gc(&self) -> Result<Response, CallError> {
        Err(unimplemented("POST /v1/modules/gc"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientError;
    use std::time::Duration;

    #[tokio::test]
    async fn every_stub_is_unimplemented() {
        let c = Client::new("http://127.0.0.1:1", Duration::from_secs(1)).expect("client");
        let v = json!({});
        let all = [
            c.cluster_init(&v).await,
            c.cluster_join(&v).await,
            c.cluster_status().await,
            c.create_node_token(&v).await,
            c.cluster_remove(4, &v).await,
            c.cluster_drain(4).await,
            c.cluster_reassign(&v).await,
            c.cluster_rebalance().await,
            c.upgrade_status().await,
            c.upgrade_finalize().await,
            c.upgrade_hold().await,
            c.upgrade_release().await,
            c.migrate_runs(&v).await,
            c.retire_module("ab").await,
            c.run_module_gc().await,
        ];
        for r in all {
            let e = ClientError::from(r.expect_err("stub"));
            assert_eq!((e.status, e.code.as_str()), (Some(501), "unimplemented"));
        }
    }
}
