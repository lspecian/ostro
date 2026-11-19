//! Run policy resolution: retention and schedule-to-start (D9, plan 05-11).
//!
//! Order, first match wins: **start request > `WorkflowPolicy` > `QueuePolicy` > `[runs]`
//! config default** (`168h` for the server profile, `24h` for dev). The policies are the
//! 05-07 meta records, keyed by the **qualified** (D23) workflow / queue name, the same
//! strings as `StartRun.workflow` / `StartRun.queue`.
//!
//! - **Retention** is always > 0: a `0` at any level is "not set" and falls through to the
//!   next one, ending at the config default. `[runs] max_retention`, when set, caps the
//!   result.
//! - **Schedule-to-start** `0` means none. A level that sets it (even to 0) decides; the
//!   config has no schedule-to-start default.
//!
//! The values are resolved once, when a run is started (the API in 05-13, a continue-as-new
//! successor in [`crate::derive::successor_start`]), and stored in `StartRun.retention_ms` /
//! `schedule_to_start_ms`. **Policy changes therefore affect future runs only**: a running
//! or terminal run keeps the values it was started with.

use ostro_meta::{MetaView, QueuePolicy, WorkflowPolicy};

use crate::config::RunsSection;
use crate::derive::PolicyResolver;

/// Per-request overrides (`None` = not given by the client).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunPolicy {
    pub retention_ms: Option<u64>,
    pub schedule_to_start_ms: Option<u64>,
}

fn ms(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Pure resolution over already-read policies; returns `(retention_ms, s2s_ms)`.
pub fn resolve(
    workflow: Option<&WorkflowPolicy>,
    queue: Option<&QueuePolicy>,
    request: RunPolicy,
    defaults: &RunsSection,
) -> (u64, u64) {
    let positive = |v: Option<u64>| v.filter(|v| *v > 0);
    let retention = positive(request.retention_ms)
        .or_else(|| positive(workflow.and_then(|w| w.retention_ms)))
        .or_else(|| positive(queue.and_then(|q| q.retention_ms)))
        .unwrap_or_else(|| ms(defaults.retention.get()).max(1));
    let retention = match defaults.max_retention {
        Some(cap) => retention.min(ms(cap.get()).max(1)),
        None => retention,
    };
    let s2s = request
        .schedule_to_start_ms
        .or_else(|| workflow.and_then(|w| w.schedule_to_start_ms))
        .or_else(|| queue.and_then(|q| q.schedule_to_start_ms))
        .unwrap_or(0);
    (retention, s2s)
}

/// `(retention_ms, schedule_to_start_ms)` of a new run of `workflow` on `queue` (both
/// qualified), reading the policies from `meta` (see the module docs for the order).
pub fn resolve_run_policy(
    meta: &MetaView,
    defaults: &RunsSection,
    workflow: &str,
    queue: &str,
    request: RunPolicy,
) -> (u64, u64) {
    meta.read(|s| {
        resolve(
            s.workflow_policy(workflow).map(|v| &v.value),
            s.queue_policy(queue).map(|v| &v.value),
            request,
            defaults,
        )
    })
}

/// [`PolicyResolver`] over the metadata state machine and the `[runs]` defaults (the
/// continue-as-new successor's policy; no request overrides).
#[derive(Clone)]
pub struct MetaPolicies {
    pub meta: MetaView,
    pub defaults: RunsSection,
}

impl std::fmt::Debug for MetaPolicies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaPolicies")
            .field("defaults", &self.defaults)
            .finish_non_exhaustive()
    }
}

impl PolicyResolver for MetaPolicies {
    fn resolve(&self, workflow: &str, queue: &str) -> (u64, u64) {
        resolve_run_policy(
            &self.meta,
            &self.defaults,
            workflow,
            queue,
            RunPolicy::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FriendlyDuration, ServerConfig};

    const H: u64 = 3_600_000;

    fn wp(retention: Option<u64>, s2s: Option<u64>) -> WorkflowPolicy {
        WorkflowPolicy {
            workflow: "acme/w".into(),
            retention_ms: retention,
            schedule_to_start_ms: s2s,
            ..Default::default()
        }
    }

    fn qp(retention: Option<u64>, s2s: Option<u64>) -> QueuePolicy {
        QueuePolicy {
            queue: "acme/q".into(),
            retention_ms: retention,
            schedule_to_start_ms: s2s,
            ..Default::default()
        }
    }

    fn req(retention: Option<u64>, s2s: Option<u64>) -> RunPolicy {
        RunPolicy {
            retention_ms: retention,
            schedule_to_start_ms: s2s,
        }
    }

    #[test]
    fn retention_defaults_per_profile() {
        let server = ServerConfig::server_defaults().runs;
        let dev = ServerConfig::dev_defaults().runs;
        assert_eq!(
            resolve(None, None, RunPolicy::default(), &server),
            (168 * H, 0)
        );
        assert_eq!(resolve(None, None, RunPolicy::default(), &dev), (24 * H, 0));
    }

    #[test]
    fn request_beats_workflow_beats_queue_beats_config() {
        let d = RunsSection::default();
        let (w, q) = (wp(Some(2), Some(20)), qp(Some(3), Some(30)));
        // Every level set: the request wins.
        assert_eq!(
            resolve(Some(&w), Some(&q), req(Some(1), Some(10)), &d),
            (1, 10)
        );
        // No request: the workflow policy.
        assert_eq!(resolve(Some(&w), Some(&q), req(None, None), &d), (2, 20));
        // No workflow policy: the queue policy.
        assert_eq!(resolve(None, Some(&q), req(None, None), &d), (3, 30));
        // A workflow policy without the field falls through to the queue's.
        let empty = wp(None, None);
        assert_eq!(
            resolve(Some(&empty), Some(&q), req(None, None), &d),
            (3, 30)
        );
        // Nothing: the config default, no schedule-to-start.
        assert_eq!(resolve(None, None, req(None, None), &d), (168 * H, 0));
        // Fields resolve independently.
        assert_eq!(resolve(Some(&w), Some(&q), req(Some(1), None), &d), (1, 20));
    }

    #[test]
    fn zero_retention_falls_through_zero_s2s_decides() {
        let d = RunsSection::default();
        let (w, q) = (wp(Some(0), Some(0)), qp(Some(3), Some(30)));
        // Retention is always > 0: zeros are "not set".
        assert_eq!(resolve(Some(&w), Some(&q), req(Some(0), None), &d).0, 3);
        // Schedule-to-start 0 = none, and a level that sets it decides.
        assert_eq!(resolve(Some(&w), Some(&q), req(None, None), &d).1, 0);
        assert_eq!(resolve(None, Some(&q), req(None, Some(0)), &d).1, 0);
        // A zero config default still yields a positive retention.
        let zero = RunsSection {
            retention: FriendlyDuration::from_millis(0),
            max_retention: None,
        };
        assert_eq!(resolve(None, None, req(None, None), &zero).0, 1);
    }

    #[test]
    fn max_retention_caps_every_level() {
        let d = RunsSection {
            retention: FriendlyDuration::from_secs(48 * 3600),
            max_retention: Some(FriendlyDuration::from_secs(24 * 3600)),
        };
        assert_eq!(resolve(None, None, req(None, None), &d).0, 24 * H);
        assert_eq!(resolve(None, None, req(Some(100 * H), None), &d).0, 24 * H);
        assert_eq!(resolve(None, None, req(Some(H), None), &d).0, H);
        let w = wp(Some(1_000 * H), None);
        assert_eq!(resolve(Some(&w), None, req(None, None), &d).0, 24 * H);
    }

    #[tokio::test]
    async fn meta_policies_resolve_by_qualified_name() {
        use ostro_meta::{MetaCommand, MetaHandle, MetaRecord};
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
        let put = |record: MetaRecord| MetaCommand::Put {
            key: record.key(),
            expected_version: None,
            record,
        };
        meta.propose(put(MetaRecord::WorkflowPolicy(wp(Some(5 * H), None))))
            .await
            .expect("workflow policy");
        meta.propose(put(MetaRecord::QueuePolicy(qp(Some(9 * H), Some(700)))))
            .await
            .expect("queue policy");
        let d = RunsSection::default();
        let view = meta.view();
        let r = |w: &str, q: &str, rq: RunPolicy| resolve_run_policy(view, &d, w, q, rq);
        assert_eq!(r("acme/w", "acme/q", RunPolicy::default()), (5 * H, 700));
        assert_eq!(r("acme/w", "acme/q", req(Some(H), Some(1))), (H, 1));
        // Keys are qualified: the default tenant's `w` / `q` have no policy.
        assert_eq!(r("w", "q", RunPolicy::default()), (168 * H, 0));
        assert_eq!(r("other/w", "acme/q", RunPolicy::default()), (9 * H, 700));
        let p = MetaPolicies {
            meta: view.clone(),
            defaults: d.clone(),
        };
        assert_eq!(
            PolicyResolver::resolve(&p, "acme/w", "acme/q"),
            (5 * H, 700)
        );
        meta.stop().await;
    }
}
