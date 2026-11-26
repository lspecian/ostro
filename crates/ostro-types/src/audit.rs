//! Phase 14 audit contract (14-01; D-P14-11/12/13).
//!
//! Producers (the API guard, the bridge's audited secret resolver, the worker and cluster
//! join paths) describe what happened as an [`AuditEvent`] and hand it to an [`AuditSink`].
//! The journal behind the sink (node-local, hash-chained, `<data>/audit/`) is 14-04's; this
//! module only fixes the shapes so every producer and the journal compile against one
//! contract.
//!
//! **Fail closed (D-P14-11):** an audited request first calls [`AuditSink::reserve`] and does
//! not execute when no permit is available ([`AuditUnavailable`] → 503
//! `audit_unavailable`). After executing it commits the permit with the event and answers
//! only once the record is durable ([`AuditPermit::commit`]).
//!
//! **Coalescing (D-P14-12):** high-volume events (secret access, authentication failures)
//! go through [`AuditSink::record_coalesced`] instead and are summarised per
//! [`CoalesceKey`] and window by the journal; they never block the caller.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

/// Who performed an audited action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    /// An API token (`otk_…`).
    Token,
    /// The server itself (cron, webhooks, triggers, bulk, hooks, reseal, bootstrap).
    System,
    /// A worker (Phase 7 `wkr_` token / worker certificate).
    Worker,
    /// A cluster node (node certificate / `njt_` join token).
    Node,
    /// The synthetic dev principal (`[auth] mode = "none"`).
    Dev,
    /// No credentials (authentication failures).
    Anonymous,
}

impl ActorKind {
    /// Stable snake_case name (journal `actor_kind`).
    pub const fn as_str(self) -> &'static str {
        match self {
            ActorKind::Token => "token",
            ActorKind::System => "system",
            ActorKind::Worker => "worker",
            ActorKind::Node => "node",
            ActorKind::Dev => "dev",
            ActorKind::Anonymous => "anonymous",
        }
    }
}

/// How an audited action ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "result", content = "reason")]
pub enum AuditOutcome {
    /// The action succeeded.
    Ok,
    /// Refused before executing (authorization, signature policy, quota); the reason is a
    /// stable `details.reason` value such as `missing_scope`.
    Denied(String),
    /// Executed and failed (the API error code / message).
    Failed(String),
}

/// One audited action (D-P14-11 record fields minus the journal's own `v`, `seq`, `ts_ms`,
/// `node`, `incarnation`, `prev_hash` and `hash`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Dotted action name, e.g. `module.upload`, `token.create`, `run.terminate`.
    pub action: String,
    /// Actor id: token id, worker id, node id, `system` or `anonymous`.
    pub actor: String,
    /// Kind of actor.
    pub actor_kind: ActorKind,
    /// Tenant the action applies to (`None` = system scope / default tenant unknown).
    pub tenant: Option<String>,
    /// Target of the action (run id, qualified name, sha, token id, …).
    pub target: String,
    /// Result.
    pub outcome: AuditOutcome,
    /// Request id of the API call, when there is one.
    pub request_id: Option<String>,
    /// Extra fields (sorted, so the canonical JSON is stable).
    pub details: BTreeMap<String, String>,
}

impl AuditEvent {
    /// An `Ok` event with no tenant, request id or details.
    pub fn new(
        action: impl Into<String>,
        actor: impl Into<String>,
        actor_kind: ActorKind,
        target: impl Into<String>,
    ) -> Self {
        AuditEvent {
            action: action.into(),
            actor: actor.into(),
            actor_kind,
            tenant: None,
            target: target.into(),
            outcome: AuditOutcome::Ok,
            request_id: None,
            details: BTreeMap::new(),
        }
    }
}

/// The coalescing key of a high-volume event (D-P14-12).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CoalesceKey {
    /// A secret resolved for a run's effect: one record per (run, module, secret, origin)
    /// and window (`[audit] secret_coalesce`, 60 s).
    SecretAccess {
        run: String,
        module: String,
        secret: String,
        origin: String,
    },
    /// An authentication failure: one record per (token id or `malformed`, client IP) and
    /// window.
    AuthFailure { token_id: String, client_ip: String },
}

/// No audit permit could be reserved in time (`[audit] reserve_timeout`): the request must
/// not execute (503 `audit_unavailable`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("audit journal unavailable: {0}")]
pub struct AuditUnavailable(pub String);

/// Committing a reserved record failed **after** the action executed (500
/// `audit_commit_failed`, `details.executed = true`; the writer is poisoned, D-P14-11 W8).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("audit commit failed: {0}")]
pub struct AuditCommitError(pub String);

/// The boxed future of [`AuditPermit::commit`].
pub type CommitFuture = Pin<Box<dyn Future<Output = Result<(), AuditCommitError>> + Send>>;

/// The journal side of a reserved permit (implemented by 14-04's writer).
pub trait PermitSlot: Send {
    /// Write `ev` into the reserved slot; resolves once the record is durable.
    fn commit(self: Box<Self>, ev: AuditEvent) -> CommitFuture;
}

/// A reserved place in the audit journal. Dropping it without committing releases the
/// reservation (the action did not run).
pub struct AuditPermit {
    slot: Option<Box<dyn PermitSlot>>,
}

impl std::fmt::Debug for AuditPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditPermit")
            .field("noop", &self.slot.is_none())
            .finish()
    }
}

impl AuditPermit {
    /// A permit backed by a journal slot.
    pub fn new(slot: Box<dyn PermitSlot>) -> Self {
        AuditPermit { slot: Some(slot) }
    }

    /// A permit that records nothing ([`NoopAuditSink`]).
    pub fn noop() -> Self {
        AuditPermit { slot: None }
    }

    /// Commit `ev` into the reserved slot; resolves once the record is durable.
    pub fn commit(self, ev: AuditEvent) -> CommitFuture {
        match self.slot {
            Some(slot) => slot.commit(ev),
            None => {
                drop(ev);
                Box::pin(async { Ok(()) })
            }
        }
    }
}

/// Where audited actions go (the 14-04 journal; [`NoopAuditSink`] until then and with audit
/// off).
pub trait AuditSink: Send + Sync {
    /// Reserve a journal slot before executing an audited action (fail closed).
    fn reserve(&self) -> Result<AuditPermit, AuditUnavailable>;

    /// Record a high-volume event under its coalescing key; never blocks.
    fn record_coalesced(&self, ev: AuditEvent, key: CoalesceKey);
}

/// An [`AuditSink`] that accepts everything and records nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAuditSink;

impl AuditSink for NoopAuditSink {
    fn reserve(&self) -> Result<AuditPermit, AuditUnavailable> {
        Ok(AuditPermit::noop())
    }

    fn record_coalesced(&self, ev: AuditEvent, key: CoalesceKey) {
        drop((ev, key));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    fn block_on<F: Future>(f: F) -> F::Output {
        // The futures here never pend; poll once with a no-op waker.
        let mut f = std::pin::pin!(f);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match f.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => panic!("test future pended"),
        }
    }

    fn assert_send_sync<T: Send + Sync + ?Sized>() {}

    #[test]
    fn noop_sink_reserves_and_commits() {
        let sink: Arc<dyn AuditSink> = Arc::new(NoopAuditSink);
        let permit = sink.reserve().expect("noop always reserves");
        let ev = AuditEvent::new("module.upload", "otk_x", ActorKind::Token, "abc");
        block_on(permit.commit(ev.clone())).expect("noop commit");
        sink.record_coalesced(
            ev,
            CoalesceKey::AuthFailure {
                token_id: "malformed".into(),
                client_ip: "127.0.0.1".into(),
            },
        );
    }

    #[test]
    fn sink_and_event_are_send_sync() {
        assert_send_sync::<dyn AuditSink>();
        assert_send_sync::<NoopAuditSink>();
        assert_send_sync::<AuditEvent>();
        fn assert_send<T: Send>() {}
        assert_send::<AuditPermit>();
        assert_send::<CommitFuture>();
    }

    struct Rec(Arc<Mutex<Vec<AuditEvent>>>);

    impl PermitSlot for Rec {
        fn commit(self: Box<Self>, ev: AuditEvent) -> CommitFuture {
            self.0.lock().unwrap().push(ev);
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn a_slot_permit_hands_the_event_to_the_journal() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let permit = AuditPermit::new(Box::new(Rec(Arc::clone(&log))));
        let mut ev = AuditEvent::new("token.create", "otk_a", ActorKind::Token, "otk_b");
        ev.outcome = AuditOutcome::Denied("missing_scope".into());
        ev.details.insert("scope".into(), "admin".into());
        block_on(permit.commit(ev.clone())).unwrap();
        assert_eq!(log.lock().unwrap().as_slice(), &[ev]);
    }

    #[test]
    fn wire_names_are_stable() {
        let all = [
            ActorKind::Token,
            ActorKind::System,
            ActorKind::Worker,
            ActorKind::Node,
            ActorKind::Dev,
            ActorKind::Anonymous,
        ];
        let names: Vec<_> = all.iter().map(|k| k.as_str()).collect();
        assert_eq!(names, ["token", "system", "worker", "node", "dev", "anonymous"]);
        for k in all {
            let v = postcard::to_stdvec(&k).unwrap();
            assert_eq!(postcard::from_bytes::<ActorKind>(&v).unwrap(), k);
        }
    }
}
