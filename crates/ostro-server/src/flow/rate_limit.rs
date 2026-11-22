//! StartRun admission (plan 07-08, D-W21).
//!
//! The same hand-rolled GCRA as the throttles ([`super::limits`], nanoseconds from the
//! injected clock) per queue (`QueuePolicy.rate_limit`) and per (queue, key)
//! (`QueuePolicy.key_rate_limit`, non-empty concurrency keys only). One limiter mutex is
//! held across checking both TATs and committing them, and both are committed only if both
//! conform: no partial charge and no interleaving between concurrent starts.
//!
//! - Excess → [`ApiError::rate_limited`]: `code = "rate_limited"`, HTTP 429 with
//!   `Retry-After` (ceil seconds), gRPC `RESOURCE_EXHAUSTED`, `details.retry_after_ms`.
//!   Nothing is proposed and no payload is written.
//! - Not charged: a start carrying an idempotency key whose dedupe row already exists
//!   (it returns the existing run, or 409 under `reject`). The row is read with one point
//!   read on the partition the key routes to (the same read the dry run does), and only
//!   when the queue has a rate limit at all: unlimited queues cost nothing (read budget).
//! - A dry run is never charged (the call sits after the dry-run return).
//! - Webhook and cron starts (Phase 8) go through the same `ControlPlane` start, so they
//!   are limited too.
//!
//! The race between the dedupe read and the charge is benign: two concurrent first starts
//! of one new key may both be charged although only one creates a run.

use ostro_api::ApiError;
use ostro_log::PartitionLog;
use ostro_partition::PartitionHandle;
use ostro_partition::store::Table;
use ostro_partition::store::keys::dedupe_key;

use super::NodeFlow;
use crate::control::ServerControlPlane;

/// Charge one start into `(queue, key)` at `now_ms`; `already_exists` is asked only when
/// an idempotency key is present and the queue is rate limited, and a `true` answer makes
/// the start free.
pub fn admit_start(
    flow: &NodeFlow,
    queue: &str,
    key: &str,
    now_ms: u64,
    idempotency: Option<&[u8]>,
    already_exists: impl FnOnce() -> bool,
) -> Result<(), ApiError> {
    if !flow.rate_limited(queue, key) {
        return Ok(());
    }
    if idempotency.is_some_and(|k| !k.is_empty()) && already_exists() {
        return Ok(());
    }
    flow.charge_start(queue, key, now_ms)
        .map_err(|retry_after_ms| {
            ApiError::rate_limited(format!("queue {queue} rate limit exceeded"), retry_after_ms)
        })
}

/// The start path's admission call (`control/runs.rs`): [`admit_start`] with the dedupe
/// row of `(workflow, idempotency)` read from `handle` (the key's partition).
pub(crate) async fn admit(
    cp: &ServerControlPlane,
    handle: &PartitionHandle<PartitionLog>,
    workflow: &str,
    queue: &str,
    key: &str,
    idempotency: Option<&str>,
) -> Result<(), ApiError> {
    let flow = &cp.p7.flow;
    if !flow.rate_limited(queue, key) {
        return Ok(());
    }
    let exists = match idempotency.filter(|k| !k.is_empty()) {
        Some(k) => {
            let reader = handle.reader();
            let dk = dedupe_key(workflow, k.as_bytes());
            tokio::task::spawn_blocking(move || reader.get(Table::Dedupe, &dk))
                .await
                .map_err(|e| ApiError::internal(format!("dedupe read task: {e}")))?
                .map_err(|e| ApiError::internal(format!("dedupe read: {e}")))?
                .is_some()
        }
        None => false,
    };
    admit_start(
        flow,
        queue,
        key,
        cp.now_ms(),
        idempotency.map(str::as_bytes),
        || exists,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ostro_api::ErrorCode;
    use ostro_meta::{MetaCommand, MetaHandle, MetaRecord, QueuePolicy, RateSpec};
    use ostro_partition::QueueGauges;

    use super::*;

    #[tokio::test]
    async fn rate_limit_charges_and_skips_existing_idempotent_starts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
        let record = MetaRecord::QueuePolicy(QueuePolicy {
            queue: "q".into(),
            rate_limit: Some(RateSpec {
                limit: 3,
                period_ms: 1_000,
                burst: 3,
            }),
            ..QueuePolicy::default()
        });
        meta.propose(MetaCommand::Put {
            key: record.key(),
            expected_version: None,
            record,
        })
        .await
        .expect("propose");
        let flow = NodeFlow::new(meta.view().clone(), Arc::new(QueueGauges::new()));
        let mut asked = false;
        // Unlimited queue: never asks for the dedupe row.
        admit_start(&flow, "free", "", 0, Some(b"k"), || {
            asked = true;
            true
        })
        .expect("unlimited");
        assert!(!asked);
        for _ in 0..3 {
            admit_start(&flow, "q", "", 10_000, None, || false).expect("within burst");
        }
        let e = admit_start(&flow, "q", "", 10_000, None, || false).expect_err("4th");
        assert_eq!(e.code, ErrorCode::RateLimited);
        // T = 333.33 ms: the 4th may go one emission interval later.
        assert_eq!(e.retry_after_ms(), Some(334));
        // An existing idempotent start is free even when the limit is exhausted.
        admit_start(&flow, "q", "", 10_000, Some(b"k"), || true).expect("existing");
        // A new idempotency key is charged.
        assert!(admit_start(&flow, "q", "", 10_000, Some(b"k2"), || false).is_err());
        admit_start(&flow, "q", "", 10_334, None, || false).expect("refilled");
        meta.stop().await;
    }
}
