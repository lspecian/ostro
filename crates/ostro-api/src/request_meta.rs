//! Phase 13 request metadata (13-01 contract; D-P13-14/15/24). The REST and gRPC adapters
//! (13-03) fill a [`RequestMeta`] from the `ostro-*` headers and run the call inside
//! [`scope`]; the routed control plane (13-12) and the consistency hooks (13-13) read it with
//! [`RequestMeta::current`] and publish a write's consistency token with
//! [`set_consistency`], which the adapter emits as `ostro-consistency` after
//! [`take_consistency`].
//!
//! Headers (RESERVATIONS §11 P13): `ostro-hop`, `ostro-partition`, `ostro-partitions`,
//! `ostro-principal`, `ostro-consistency`, `ostro-read-after`. `partition_pin`,
//! `partitions`, `principal` and `from_node` are honoured only from node identities
//! (Pitfall 7); the adapters enforce that.

use std::cell::RefCell;
use std::future::Future;
use std::time::Instant;

/// `ostro-hop`: 1 on a forwarded call (never forwarded again).
pub const HEADER_HOP: &str = "ostro-hop";
/// `ostro-partition`: the partition an unkeyed start was placed on.
pub const HEADER_PARTITION: &str = "ostro-partition";
/// `ostro-partitions`: the partitions a fan-out leg serves.
pub const HEADER_PARTITIONS: &str = "ostro-partitions";
/// `ostro-principal`: the original caller (Phase 14 hook).
pub const HEADER_PRINCIPAL: &str = "ostro-principal";
/// `ostro-consistency`: a write's token (`p<P>:<epoch>:<offset>` or `m:<index>`).
pub const HEADER_CONSISTENCY: &str = "ostro-consistency";
/// `ostro-read-after`: a token a read must observe.
pub const HEADER_READ_AFTER: &str = "ostro-read-after";

/// Metadata of the current API call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestMeta {
    /// Forwarding hops so far (0 = from a client).
    pub hop: u8,
    /// Partition pin of a forwarded unkeyed start.
    pub partition_pin: Option<u16>,
    /// Partitions of a fan-out leg.
    pub partitions: Option<Vec<u16>>,
    /// Original principal (trusted only from node SANs).
    pub principal: Option<String>,
    /// Consistency token the read must observe.
    pub read_after: Option<String>,
    /// The forwarding node, when the caller is a node identity.
    pub from_node: Option<u64>,
    /// Remaining deadline (`grpc-timeout`).
    pub deadline: Option<Instant>,
}

struct Slot {
    meta: RequestMeta,
    consistency: RefCell<Option<String>>,
}

tokio::task_local! {
    static REQUEST_META: Slot;
}

impl RequestMeta {
    /// The metadata of the current call; `None` outside [`scope`] (in-process consumers such
    /// as triggers, cron and bulk jobs).
    pub fn current() -> Option<RequestMeta> {
        REQUEST_META.try_with(|s| s.meta.clone()).ok()
    }

    /// Whether this call was already forwarded once.
    pub fn is_forwarded(&self) -> bool {
        self.hop > 0
    }
}

/// Run `fut` with `meta` as the current request metadata.
pub async fn scope<F: Future>(meta: RequestMeta, fut: F) -> F::Output {
    REQUEST_META
        .scope(
            Slot {
                meta,
                consistency: RefCell::new(None),
            },
            fut,
        )
        .await
}

/// Run `fut` with `meta` and return its output plus the consistency token it published.
pub async fn scope_with_consistency<F: Future>(
    meta: RequestMeta,
    fut: F,
) -> (F::Output, Option<String>) {
    scope(meta, async move {
        let out = fut.await;
        (out, take_consistency())
    })
    .await
}

/// Publish the consistency token of a write (no-op outside [`scope`]).
pub fn set_consistency(token: String) {
    let _ = REQUEST_META.try_with(|s| *s.consistency.borrow_mut() = Some(token));
}

/// Take the published consistency token (inside [`scope`]).
pub fn take_consistency() -> Option<String> {
    REQUEST_META
        .try_with(|s| s.consistency.borrow_mut().take())
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scope_carries_meta_and_the_consistency_out_slot() {
        assert_eq!(RequestMeta::current(), None);
        set_consistency("ignored".into());
        assert_eq!(take_consistency(), None);
        let meta = RequestMeta {
            hop: 1,
            partition_pin: Some(3),
            from_node: Some(2),
            ..RequestMeta::default()
        };
        let (seen, token) = scope_with_consistency(meta.clone(), async {
            let m = RequestMeta::current().expect("in scope");
            tokio::task::yield_now().await;
            set_consistency("p3:7:42".into());
            m
        })
        .await;
        assert_eq!(seen, meta);
        assert!(seen.is_forwarded());
        assert_eq!(token.as_deref(), Some("p3:7:42"));
        assert_eq!(RequestMeta::current(), None);
    }
}
