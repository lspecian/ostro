//! 13-13 Task 1: the read-your-writes rules of `ostro_server::route::consistency`.
//!
//! - Over a fake node (fake monotonic time, roles, anchor, barrier): the lease boundary,
//!   `LeaderAnnounced`-not-applied and handoff refusals, strict reads (barrier + epoch
//!   check), tokens (format, parsing, malformed → 400, covered → no barrier), the meta
//!   barrier and its timeout, and the `serve_without_lease` mutant (failpoints build).
//! - Through the routed instance (plan check B4): `routed_hooks_effective`.
//! - Over a real one-voter metadata group (plan check NB2): `single_node_reads_served`
//!   (anchor = now, no barrier, micro-benchmark). The 3-voter lease tests
//!   (`lease_on_meta_leader_node`, `isolated_meta_leader_stops_serving_reads`) live in
//!   `ostro-cluster/tests/controller_lease_wait.rs`: an in-process 3-voter group needs the raft
//!   message type (`ostro-raft-driver`), which is not a dependency of this crate, and
//!   manifests are 13-01-only.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_api::domain::*;
use ostro_api::request_meta::{self, RequestMeta};
use ostro_api::{ApiError, ApiExtensions, ControlPlane, ErrorCode};
use ostro_meta::MetaHandle;
use ostro_meta::handle::{MetaStart, TickConfig};
use ostro_server::route::consistency::{
    self, ReadEnv, Token, check_meta_read, check_partition_read, check_partition_write,
    lease_holds, lease_limit, meta_token, parse_token, partition_token,
};
use ostro_server::route::peer::PeerApi;
use ostro_server::route::table::Target;
use ostro_server::route::{BoxFuture, RouteCore, RouteEnv, RoutedControlPlane};
use ostro_types::{Epoch, PartitionCount, PartitionId};

const ST: Duration = Duration::from_millis(1000);
const MARGIN: f64 = 0.2;
const START_US: u64 = 1_000_000_000;

fn p(n: u16) -> PartitionId {
    PartitionId(n)
}

fn with_token(t: &str) -> RequestMeta {
    RequestMeta {
        read_after: Some(t.into()),
        ..RequestMeta::default()
    }
}

// ---------------------------------------------------------------------------------
// A fake node
// ---------------------------------------------------------------------------------

/// Fake time (base + offset), role, anchor, barrier and meta view.
struct Fake {
    me: u64,
    base: Instant,
    offset_us: AtomicU64,
    strict: AtomicBool,
    leading: Mutex<HashMap<u16, Epoch>>,
    handoff: Mutex<BTreeSet<u16>>,
    /// Anchor as an offset (µs) from `base`; `None` = no anchor.
    anchor_us: Mutex<Option<u64>>,
    applied: Mutex<HashMap<u16, (Epoch, u64)>>,
    meta_applied: AtomicU64,
    assignment: Mutex<HashMap<u16, (u64, Epoch)>>,
    hint: Mutex<HashMap<u16, u64>>,
    barrier_ok: AtomicBool,
    barriers: AtomicUsize,
    /// The barrier publishes this assignment (a move seen only after the barrier).
    after_barrier: Mutex<Option<(u16, (u64, Epoch))>>,
}

impl Fake {
    fn new(me: u64) -> Arc<Self> {
        Arc::new(Self {
            me,
            base: Instant::now(),
            // Fake time starts 1000 s after `base`, so anchors can lie in the past.
            offset_us: AtomicU64::new(START_US),
            strict: AtomicBool::new(false),
            leading: Mutex::default(),
            handoff: Mutex::default(),
            anchor_us: Mutex::new(Some(START_US)),
            applied: Mutex::default(),
            meta_applied: AtomicU64::new(0),
            assignment: Mutex::default(),
            hint: Mutex::default(),
            barrier_ok: AtomicBool::new(true),
            barriers: AtomicUsize::new(0),
            after_barrier: Mutex::default(),
        })
    }

    /// Leads `p` at `e` (role leader, LeaderAnnounced applied; meta agrees).
    fn lead(&self, part: u16, e: u32) {
        self.leading.lock().unwrap().insert(part, Epoch(e));
        self.assignment
            .lock()
            .unwrap()
            .insert(part, (self.me, Epoch(e)));
        self.applied.lock().unwrap().insert(part, (Epoch(e), 100));
    }

    fn advance(&self, d: Duration) {
        self.offset_us
            .fetch_add(d.as_micros() as u64, Ordering::SeqCst);
    }

    /// The anchor is `age` old now.
    fn anchor_age(&self, age: Duration) {
        let now = self.offset_us.load(Ordering::SeqCst);
        *self.anchor_us.lock().unwrap() = Some(now.saturating_sub(age.as_micros() as u64));
    }

    fn barriers(&self) -> usize {
        self.barriers.load(Ordering::SeqCst)
    }
}

impl ReadEnv for Fake {
    fn node_id(&self) -> u64 {
        self.me
    }
    fn now(&self) -> Instant {
        self.base + Duration::from_micros(self.offset_us.load(Ordering::SeqCst))
    }
    fn session_timeout(&self) -> Duration {
        ST
    }
    fn lease_margin(&self) -> f64 {
        MARGIN
    }
    fn strict_reads(&self) -> bool {
        self.strict.load(Ordering::SeqCst)
    }
    fn leading(&self, p: PartitionId) -> Option<Epoch> {
        self.leading.lock().unwrap().get(&p.0).copied()
    }
    fn in_handoff(&self, p: PartitionId) -> bool {
        self.handoff.lock().unwrap().contains(&p.0)
    }
    fn meta_lease_anchor(&self) -> Option<Instant> {
        self.anchor_us
            .lock()
            .unwrap()
            .map(|us| self.base + Duration::from_micros(us))
    }
    fn leader_hint(&self, p: PartitionId) -> Option<u64> {
        self.hint.lock().unwrap().get(&p.0).copied()
    }
    fn applied(&self, p: PartitionId) -> Option<(Epoch, u64)> {
        self.applied.lock().unwrap().get(&p.0).copied()
    }
    fn meta_applied(&self) -> u64 {
        self.meta_applied.load(Ordering::SeqCst)
    }
    fn read_index_barrier(&self, deadline: Instant) -> BoxFuture<'_, Result<u64, String>> {
        self.barriers.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.barrier_ok.load(Ordering::SeqCst) {
                if let Some((part, a)) = self.after_barrier.lock().unwrap().take() {
                    self.assignment.lock().unwrap().insert(part, a);
                }
                Ok(self.meta_applied())
            } else {
                // Never completes: the deadline answers.
                let now = self.now();
                if deadline > now {
                    self.advance(deadline - now);
                }
                Err("timed out".into())
            }
        })
    }
    fn assignment(&self, p: PartitionId) -> Option<(u64, Epoch)> {
        self.assignment.lock().unwrap().get(&p.0).copied()
    }
    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()> {
        self.advance(d);
        Box::pin(async {})
    }
}

fn reason(e: &ApiError) -> Option<&str> {
    e.reason()
}

// ---------------------------------------------------------------------------------
// Fake-node rules
// ---------------------------------------------------------------------------------

#[test]
fn lease_limit_and_boundary() {
    assert_eq!(lease_limit(ST, MARGIN), Duration::from_millis(800));
    let t0 = Instant::now();
    let limit = lease_limit(ST, MARGIN);
    assert!(lease_holds(
        Some(t0),
        t0 + Duration::from_millis(799),
        limit
    ));
    assert!(!lease_holds(
        Some(t0),
        t0 + Duration::from_millis(800),
        limit
    ));
    assert!(!lease_holds(None, t0, limit));
    // An anchor "in the future" (clock granularity) counts as age 0.
    assert!(lease_holds(Some(t0 + Duration::from_millis(5)), t0, limit));
}

#[tokio::test]
async fn lease_boundary_just_below_and_above_the_margin() {
    let f = Fake::new(1);
    f.lead(0, 3);
    f.hint.lock().unwrap().insert(0, 2);
    // Age just below session_timeout × (1 − margin) = 800 ms: served, no barrier.
    f.anchor_age(Duration::from_millis(799));
    check_partition_read(&*f, p(0), &RequestMeta::default())
        .await
        .expect("fresh lease");
    assert_eq!(f.barriers(), 0, "the lease path takes no barrier");
    // Just above: refused (after the short wait for a fresh anchor), not_leader + hint.
    f.anchor_age(Duration::from_millis(801));
    let t = f.now();
    let e = check_partition_read(&*f, p(0), &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert_eq!(reason(&e), Some("not_leader"));
    let d = e.details.clone().unwrap();
    assert_eq!(d["partition"], 0);
    assert_eq!(d["leader"], 2);
    let waited = f.now() - t;
    assert!(
        waited <= Duration::from_millis(205),
        "waited {waited:?} for a fresh anchor"
    );
    // No anchor at all: refused.
    *f.anchor_us.lock().unwrap() = None;
    assert!(
        check_partition_read(&*f, p(0), &RequestMeta::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_late_anchor_within_the_wait_is_accepted() {
    // A meta follower's first ask starts its lease rounds: the anchor appears shortly.
    struct Late(Arc<Fake>, AtomicUsize);
    let f = Fake::new(1);
    f.lead(0, 1);
    *f.anchor_us.lock().unwrap() = None;
    let late = Late(Arc::clone(&f), AtomicUsize::new(0));
    impl ReadEnv for Late {
        fn node_id(&self) -> u64 {
            self.0.node_id()
        }
        fn now(&self) -> Instant {
            self.0.now()
        }
        fn session_timeout(&self) -> Duration {
            ST
        }
        fn lease_margin(&self) -> f64 {
            MARGIN
        }
        fn strict_reads(&self) -> bool {
            false
        }
        fn leading(&self, p: PartitionId) -> Option<Epoch> {
            self.0.leading(p)
        }
        fn in_handoff(&self, p: PartitionId) -> bool {
            self.0.in_handoff(p)
        }
        fn meta_lease_anchor(&self) -> Option<Instant> {
            // The third ask finds a fresh round.
            if self.1.fetch_add(1, Ordering::SeqCst) >= 2 {
                Some(self.0.now())
            } else {
                None
            }
        }
        fn leader_hint(&self, p: PartitionId) -> Option<u64> {
            self.0.leader_hint(p)
        }
        fn applied(&self, p: PartitionId) -> Option<(Epoch, u64)> {
            self.0.applied(p)
        }
        fn meta_applied(&self) -> u64 {
            0
        }
        fn read_index_barrier(&self, d: Instant) -> BoxFuture<'_, Result<u64, String>> {
            self.0.read_index_barrier(d)
        }
        fn assignment(&self, p: PartitionId) -> Option<(u64, Epoch)> {
            self.0.assignment(p)
        }
        fn sleep(&self, d: Duration) -> BoxFuture<'_, ()> {
            self.0.sleep(d)
        }
    }
    check_partition_read(&late, p(0), &RequestMeta::default())
        .await
        .expect("served once the anchor appears");
}

#[tokio::test]
async fn not_leading_or_announce_not_applied_is_refused() {
    let f = Fake::new(1);
    // Never led (role follower, or Announcing: LeaderAnnounced not applied yet).
    let e = check_partition_read(&*f, p(1), &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(reason(&e), Some("not_leader"));
    // Leading: served; then in handoff: refused even with a fresh lease.
    f.lead(1, 2);
    check_partition_read(&*f, p(1), &RequestMeta::default())
        .await
        .unwrap();
    f.handoff.lock().unwrap().insert(1);
    let e = check_partition_read(&*f, p(1), &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(reason(&e), Some("not_leader"));
    assert!(e.message.contains("handoff"), "{}", e.message);
}

#[tokio::test]
async fn partition_writes_need_leading_and_the_lease_only() {
    // p13-fixes: a write's inner call reads the run record before it proposes, so a
    // leader without the lease (a zombie that never learnt of its successor) refuses with
    // the same retryable not_leader before that read.
    let f = Fake::new(1);
    f.hint.lock().unwrap().insert(0, 2);
    let e = check_partition_write(&*f, p(0), &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert_eq!(reason(&e), Some("not_leader"));
    assert!(e.message.contains("writes"), "{}", e.message);
    f.lead(0, 3);
    check_partition_write(&*f, p(0), &RequestMeta::default())
        .await
        .expect("leader with a fresh lease");
    // Lease gone: refused, with the leader hint.
    f.anchor_age(Duration::from_millis(801));
    let e = check_partition_write(&*f, p(0), &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(reason(&e), Some("not_leader"));
    assert_eq!(e.details.clone().unwrap()["leader"], 2);
    *f.anchor_us.lock().unwrap() = None;
    assert!(
        check_partition_write(&*f, p(0), &RequestMeta::default())
            .await
            .is_err()
    );
    // Barrier, handoff freeze and tokens stay read-only: strict mode, a handoff in
    // progress and an uncovered token neither take a barrier nor refuse a write (the
    // proposal is fenced by the partition log).
    f.anchor_age(Duration::ZERO);
    f.strict.store(true, Ordering::SeqCst);
    f.handoff.lock().unwrap().insert(0);
    let t = f.now();
    check_partition_write(&*f, p(0), &with_token("p0:9:100000"))
        .await
        .expect("write rules: leading + lease only");
    assert_eq!(f.barriers(), 0, "a write takes no barrier");
    assert_eq!(f.now(), t, "a write does not wait for a token");
    // A malformed read-after is not a write's business either.
    check_partition_write(&*f, p(0), &with_token("garbage"))
        .await
        .expect("tokens are ignored on writes");
}

#[tokio::test]
async fn published_leader_waits_for_its_takeover_bounded() {
    // The metadata names this node (taking over) but its LeaderAnnounced is not applied:
    // the read waits (bounded by session_timeout) instead of refusing at once, and is
    // refused when the takeover never completes.
    let f = Fake::new(1);
    f.hint.lock().unwrap().insert(0, 1);
    let t = f.now();
    let e = check_partition_read(&*f, p(0), &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(reason(&e), Some("not_leader"));
    let waited = f.now() - t;
    assert!(
        waited >= ST && waited < ST + Duration::from_millis(50),
        "{waited:?}"
    );
    // Another node published: refused at once.
    f.hint.lock().unwrap().insert(0, 2);
    let t = f.now();
    assert!(
        check_partition_read(&*f, p(0), &RequestMeta::default())
            .await
            .is_err()
    );
    assert_eq!(f.now(), t);
}

#[tokio::test]
async fn strict_reads_take_the_barrier_and_check_the_epoch() {
    let f = Fake::new(1);
    f.lead(0, 5);
    f.strict.store(true, Ordering::SeqCst);
    check_partition_read(&*f, p(0), &RequestMeta::default())
        .await
        .expect("strict read on the current leader");
    assert_eq!(f.barriers(), 1);
    // The barrier reveals a newer assignment (another node at epoch 6): refused.
    *f.after_barrier.lock().unwrap() = Some((0, (2, Epoch(6))));
    let e = check_partition_read(&*f, p(0), &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(reason(&e), Some("not_leader"));
    assert_eq!(f.barriers(), 2);
    // A failing barrier refuses too (and the lease is still required first).
    f.assignment.lock().unwrap().insert(0, (1, Epoch(5)));
    f.barrier_ok.store(false, Ordering::SeqCst);
    assert!(
        check_partition_read(&*f, p(0), &RequestMeta::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn covered_partition_token_skips_the_strict_confirmation_not_the_lease() {
    let f = Fake::new(1);
    f.lead(0, 5); // applied cursor 100
    f.strict.store(true, Ordering::SeqCst);
    check_partition_read(&*f, p(0), &with_token("p0:5:100"))
        .await
        .unwrap();
    check_partition_read(&*f, p(0), &with_token("p0:4:90"))
        .await
        .unwrap();
    assert_eq!(f.barriers(), 0, "covered tokens take no barrier");
    // A token of another partition does not count.
    check_partition_read(&*f, p(0), &with_token("p3:5:1"))
        .await
        .unwrap();
    assert_eq!(f.barriers(), 1);
    // Not covered (ahead of the applied cursor): barrier, then wait for the apply.
    let f2 = Arc::clone(&f);
    let bump = async move {
        f2.applied.lock().unwrap().insert(0, (Epoch(5), 120));
    };
    bump.await;
    check_partition_read(&*f, p(0), &with_token("p0:5:110"))
        .await
        .unwrap();
    // A token far ahead: barrier, then refused at the deadline (never served stale).
    let e = check_partition_read(&*f, p(0), &with_token("p0:5:999999"))
        .await
        .unwrap_err();
    assert_eq!(reason(&e), Some("not_leader"));
    // The lease is still required with a covered token.
    f.anchor_age(Duration::from_secs(5));
    let e = check_partition_read(&*f, p(0), &with_token("p0:5:100"))
        .await
        .unwrap_err();
    assert_eq!(reason(&e), Some("not_leader"));
    // Without strict reads a non-covered token also takes the barrier (correctness).
    let g = Fake::new(1);
    g.lead(0, 5);
    check_partition_read(&*g, p(0), &with_token("p0:5:100"))
        .await
        .unwrap();
    assert_eq!(g.barriers(), 0);
    g.applied.lock().unwrap().insert(0, (Epoch(5), 300));
    check_partition_read(&*g, p(0), &with_token("p0:5:200"))
        .await
        .unwrap();
    assert_eq!(g.barriers(), 0);
}

#[tokio::test]
async fn meta_reads_take_the_barrier_unless_the_token_is_covered() {
    let f = Fake::new(1);
    f.meta_applied.store(40, Ordering::SeqCst);
    check_meta_read(&*f, &RequestMeta::default()).await.unwrap();
    assert_eq!(f.barriers(), 1);
    check_meta_read(&*f, &with_token("m:40")).await.unwrap();
    assert_eq!(f.barriers(), 1, "covered token: no barrier");
    check_meta_read(&*f, &with_token("m:41")).await.unwrap();
    assert_eq!(f.barriers(), 2, "token ahead: barrier");
    // A partition token on a meta read is not a meta token: barrier.
    check_meta_read(&*f, &with_token("p0:1:1")).await.unwrap();
    assert_eq!(f.barriers(), 3);
}

#[tokio::test]
async fn meta_barrier_timeout_is_unavailable() {
    let f = Fake::new(1);
    f.barrier_ok.store(false, Ordering::SeqCst);
    let t = f.now();
    let meta = RequestMeta {
        deadline: Some(t + Duration::from_millis(300)),
        ..RequestMeta::default()
    };
    let e = check_meta_read(&*f, &meta).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert_eq!(reason(&e), Some("meta_unavailable"));
    assert_eq!(
        f.now() - t,
        Duration::from_millis(300),
        "bounded by the request deadline"
    );
    // Without a deadline: bounded by 2 × session_timeout.
    let t = f.now();
    check_meta_read(&*f, &RequestMeta::default())
        .await
        .unwrap_err();
    assert_eq!(f.now() - t, ST * 2);
}

#[test]
fn token_format_and_parsing() {
    assert_eq!(
        parse_token("p7:3:42").unwrap(),
        Token::Partition {
            p: p(7),
            epoch: Epoch(3),
            offset: 42
        }
    );
    assert_eq!(parse_token("m:17").unwrap(), Token::Meta { index: 17 });
    for bad in [
        "",
        "x",
        "m:",
        "m:-1",
        "p7",
        "p7:3",
        "p7:3:",
        "p:3:4",
        "p7:3:4:5",
        "p70000:1:1",
        "q7:3:42",
        "m:1:2",
    ] {
        let e = parse_token(bad).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument, "{bad:?}");
        assert_eq!(e.http_status().as_u16(), 400, "{bad:?}");
    }
    let f = Fake::new(1);
    f.lead(2, 9);
    f.applied.lock().unwrap().insert(2, (Epoch(9), 4711));
    f.meta_applied.store(88, Ordering::SeqCst);
    assert_eq!(partition_token(&*f, p(2)).as_deref(), Some("p2:9:4711"));
    assert_eq!(partition_token(&*f, p(3)), None);
    assert_eq!(meta_token(&*f).as_deref(), Some("m:88"));
    assert_eq!(
        parse_token(&partition_token(&*f, p(2)).unwrap())
            .unwrap()
            .to_string(),
        "p2:9:4711"
    );
}

#[tokio::test]
async fn malformed_read_after_is_400_on_both_paths() {
    let f = Fake::new(1);
    f.lead(0, 1);
    let e = check_partition_read(&*f, p(0), &with_token("garbage"))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidArgument);
    let e = check_meta_read(&*f, &with_token("m:x")).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn lease_refusals_are_counted() {
    let before = consistency::stats();
    let f = Fake::new(1);
    let _ = check_partition_read(&*f, p(0), &RequestMeta::default()).await;
    assert!(consistency::stats().lease_refusals > before.lease_refusals);
}

#[cfg(feature = "failpoints")]
#[tokio::test]
async fn mutant_serve_without_lease_bites() {
    let f = Fake::new(1);
    f.lead(0, 1);
    f.anchor_age(Duration::from_secs(60));
    assert!(
        check_partition_read(&*f, p(0), &RequestMeta::default())
            .await
            .is_err()
    );
    fail::cfg(consistency::MUTANT_SERVE_WITHOUT_LEASE, "return").unwrap();
    let served = check_partition_read(&*f, p(0), &RequestMeta::default()).await;
    fail::remove(consistency::MUTANT_SERVE_WITHOUT_LEASE);
    served.expect("the mutant serves an expired lease");
    // Not leading still refuses under the mutant (only the lease is skipped).
    assert!(
        check_partition_read(&*f, p(1), &RequestMeta::default())
            .await
            .is_err()
    );
}

// ---------------------------------------------------------------------------------
// Through the routed instance (B4)
// ---------------------------------------------------------------------------------

/// An inner control plane that records calls.
struct Inner {
    calls: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl ControlPlane for Inner {
    async fn upload_module(&self, _r: UploadModule) -> Result<UploadModuleResult, ApiError> {
        Err(ApiError::not_found("fake"))
    }
    async fn get_module(&self, _s: String) -> Result<ModuleInfo, ApiError> {
        Err(ApiError::not_found("fake"))
    }
    async fn activate_workflow(&self, _r: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        Err(ApiError::not_found("fake"))
    }
    async fn start_run(&self, _r: StartRunRequest) -> Result<StartRunResult, ApiError> {
        Err(ApiError::not_found("fake"))
    }
    async fn run_history(&self, _r: HistoryRequest) -> Result<HistoryPage, ApiError> {
        Err(ApiError::not_found("fake"))
    }
    async fn list_runs(&self, _r: ListRunsRequest) -> Result<RunPage, ApiError> {
        Err(ApiError::not_found("fake"))
    }
    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        self.calls.lock().unwrap().push(format!("get_run {run_id}"));
        Err(ApiError::not_found("fake run"))
    }
    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        self.calls.lock().unwrap().push("list_workflows".into());
        Ok(vec![])
    }
    async fn cancel_run(&self, r: CancelRequest) -> Result<CancelResponse, ApiError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("cancel_run {}", r.run_id));
        Ok(CancelResponse {
            already_requested: false,
            dry_run: false,
        })
    }
    async fn delete_secret(&self, _n: String, _t: Option<String>) -> Result<(), ApiError> {
        self.calls.lock().unwrap().push("delete_secret".into());
        Ok(())
    }
    fn ready(&self) -> bool {
        true
    }
}

/// The router's node: every partition is local; the four hooks run the real rules over
/// the fake node.
struct Env {
    read: Arc<Fake>,
}

impl RouteEnv for Env {
    fn local_node(&self) -> u64 {
        self.read.me
    }
    fn partition_count(&self) -> PartitionCount {
        PartitionCount::new(4).unwrap()
    }
    fn owner(&self, _p: PartitionId) -> (Target, Epoch) {
        (Target::Local, Epoch(4))
    }
    fn led(&self) -> Vec<PartitionId> {
        vec![p(0), p(1), p(2), p(3)]
    }
    fn members(&self) -> Vec<u64> {
        vec![]
    }
    fn meta_leader(&self) -> Option<u64> {
        Some(self.read.me)
    }
    fn unkeyed_local(&self) -> bool {
        true
    }
    fn uniform(&self, _n: usize) -> usize {
        0
    }
    fn retry_wait(&self) -> Duration {
        Duration::from_millis(10)
    }
    fn wait_change(&self, _p: PartitionId, _s: (Target, Epoch), _l: Duration) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn peer(&self, node: u64) -> Result<PeerApi, ApiError> {
        Err(ApiError::unavailable(format!("no peer {node}")))
    }
    fn forget_peer(&self, _node: u64) {}
    fn before_partition_read(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>> {
        let meta = RequestMeta::current().unwrap_or_default();
        Box::pin(async move { check_partition_read(&*self.read, p, &meta).await })
    }
    fn before_partition_write(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>> {
        let meta = RequestMeta::current().unwrap_or_default();
        Box::pin(async move { check_partition_write(&*self.read, p, &meta).await })
    }
    fn before_meta_read(&self) -> BoxFuture<'_, Result<(), ApiError>> {
        let meta = RequestMeta::current().unwrap_or_default();
        Box::pin(async move { check_meta_read(&*self.read, &meta).await })
    }
    fn token_after_partition_write(&self, p: PartitionId) -> Option<String> {
        partition_token(&*self.read, p)
    }
    fn token_after_meta_write(&self) -> Option<String> {
        meta_token(&*self.read)
    }
}

fn run_of(part: u16) -> String {
    ostro_types::RunId::new(PartitionId(part), uuid::Uuid::now_v7()).to_string()
}

#[tokio::test]
async fn routed_hooks_effective() {
    for forwarded in [false, true] {
        let label = if forwarded { "forwarded-in" } else { "direct" };
        let meta = RequestMeta {
            hop: u8::from(forwarded),
            from_node: forwarded.then_some(2),
            ..RequestMeta::default()
        };
        let read = Fake::new(1);
        read.lead(1, 4);
        read.applied.lock().unwrap().insert(1, (Epoch(4), 17));
        read.meta_applied.store(9, Ordering::SeqCst);
        let inner = Arc::new(Inner {
            calls: Mutex::new(vec![]),
        });
        let core = Arc::new(RouteCore::new(Arc::new(Env {
            read: Arc::clone(&read),
        })));
        let cp = RoutedControlPlane::new(core, Arc::clone(&inner) as Arc<dyn ControlPlane>);

        // Fresh lease: the read reaches the inner control plane.
        let run = run_of(1);
        let (r, _) =
            request_meta::scope_with_consistency(meta.clone(), cp.get_run(run.clone())).await;
        assert_eq!(r.unwrap_err().code, ErrorCode::NotFound, "{label}");
        assert_eq!(inner.calls.lock().unwrap().len(), 1, "{label}");

        // Expired lease: refused before the inner call (a direct call re-routes once and
        // then answers 503; a forwarded-in call answers at once).
        read.anchor_age(Duration::from_secs(5));
        let (r, _) =
            request_meta::scope_with_consistency(meta.clone(), cp.get_run(run.clone())).await;
        let e = r.unwrap_err();
        assert_eq!(e.code, ErrorCode::Unavailable, "{label}: {e:?}");
        assert_eq!(
            inner.calls.lock().unwrap().len(),
            1,
            "{label}: inner not called"
        );
        // The same zombie refuses a write before the inner call (p13-fixes: its run
        // record may be stale, so a pre-read would answer not_found).
        let (r, _) = request_meta::scope_with_consistency(
            meta.clone(),
            cp.cancel_run(CancelRequest {
                run_id: run.clone(),
                reason: None,
                dry_run: false,
            }),
        )
        .await;
        let e = r.unwrap_err();
        assert_eq!(e.code, ErrorCode::Unavailable, "{label}: {e:?}");
        assert_eq!(
            inner.calls.lock().unwrap().len(),
            1,
            "{label}: inner not called for the write"
        );
        read.anchor_age(Duration::ZERO);

        // A meta read passes the barrier first.
        let b = read.barriers();
        let (r, _) = request_meta::scope_with_consistency(meta.clone(), cp.list_workflows()).await;
        r.unwrap();
        assert_eq!(read.barriers(), b + 1, "{label}: meta read barrier");

        // A write publishes ostro-consistency into the out-slot.
        let (r, token) = request_meta::scope_with_consistency(
            meta.clone(),
            cp.cancel_run(CancelRequest {
                run_id: run.clone(),
                reason: None,
                dry_run: false,
            }),
        )
        .await;
        r.unwrap();
        assert_eq!(token.as_deref(), Some("p1:4:17"), "{label}");
        let (r, token) =
            request_meta::scope_with_consistency(meta.clone(), cp.delete_secret("s".into(), None))
                .await;
        r.unwrap();
        assert_eq!(token.as_deref(), Some("m:9"), "{label}");

        // The token makes the next meta read skip the barrier.
        let b = read.barriers();
        let with = RequestMeta {
            read_after: Some("m:9".into()),
            ..meta.clone()
        };
        let (r, _) = request_meta::scope_with_consistency(with, cp.list_workflows()).await;
        r.unwrap();
        assert_eq!(
            read.barriers(),
            b,
            "{label}: covered token skips the barrier"
        );
        let _ = ApiExtensions::default();
    }
}

// ---------------------------------------------------------------------------------
// Real metadata groups (NB2)
// ---------------------------------------------------------------------------------

/// A partition leader role (fake) over a real metadata handle (anchor, barrier, view).
struct OnMeta {
    me: u64,
    meta: Arc<MetaHandle>,
    st: Duration,
    leading: AtomicBool,
}

impl ReadEnv for OnMeta {
    fn node_id(&self) -> u64 {
        self.me
    }
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn session_timeout(&self) -> Duration {
        self.st
    }
    fn lease_margin(&self) -> f64 {
        MARGIN
    }
    fn strict_reads(&self) -> bool {
        false
    }
    fn leading(&self, _p: PartitionId) -> Option<Epoch> {
        self.leading.load(Ordering::SeqCst).then_some(Epoch(1))
    }
    fn in_handoff(&self, _p: PartitionId) -> bool {
        false
    }
    fn meta_lease_anchor(&self) -> Option<Instant> {
        self.meta.lease_anchor()
    }
    fn leader_hint(&self, _p: PartitionId) -> Option<u64> {
        None
    }
    fn applied(&self, _p: PartitionId) -> Option<(Epoch, u64)> {
        Some((Epoch(1), 1))
    }
    fn meta_applied(&self) -> u64 {
        self.meta.raft_status().applied_index
    }
    fn read_index_barrier(&self, deadline: Instant) -> BoxFuture<'_, Result<u64, String>> {
        Box::pin(async move {
            self.meta
                .read_index_barrier_until(tokio::time::Instant::from_std(deadline))
                .await
                .map_err(|e| e.to_string())
        })
    }
    fn assignment(&self, _p: PartitionId) -> Option<(u64, Epoch)> {
        Some((self.me, Epoch(1)))
    }
    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(d))
    }
}

fn ticks() -> TickConfig {
    TickConfig {
        interval: Duration::from_millis(50),
        election_tick: 10,
        heartbeat_tick: 2,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_reads_served() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaHandle::start_with(
        dir.path(),
        MetaStart {
            node_id: 1,
            voters: vec![1],
            outbox: Box::new(ostro_meta::NullOutbox),
            forwarder: None,
            tick: ticks(),
        },
    )
    .await
    .expect("start");
    meta.wait_ready().await.unwrap();
    let env = OnMeta {
        me: 1,
        meta: Arc::new(meta),
        st: Duration::from_secs(3),
        leading: AtomicBool::new(true),
    };
    // One voter: the anchor is now, the read is served without any barrier / RPC.
    let a = env.meta_lease_anchor().expect("one-voter anchor");
    assert!(a.elapsed() < Duration::from_millis(50));
    let before = consistency::stats();
    check_partition_read(&env, p(0), &RequestMeta::default())
        .await
        .unwrap();
    assert_eq!(consistency::stats().barriers, before.barriers, "no barrier");

    // Micro-benchmark: the hook against the pre-13-13 stub (`Ok(())`).
    const N: u32 = 20_000;
    let t = Instant::now();
    for _ in 0..N {
        std::hint::black_box(Ok::<(), ApiError>(()))
            .as_ref()
            .unwrap();
    }
    let stub = t.elapsed() / N;
    let t = Instant::now();
    for _ in 0..N {
        check_partition_read(&env, p(0), &RequestMeta::default())
            .await
            .unwrap();
    }
    let hook = t.elapsed() / N;
    eprintln!("single-node partition read hook: {hook:?}/read (stub {stub:?}/read)");
    // 5% of the fastest single-node read path measured in 13-12 (GET /v1/runs/{id} p50
    // 0.78 ms in process) is ~39 µs; the hook must stay far below it.
    assert!(
        hook < Duration::from_micros(39),
        "hook {hook:?} per read exceeds 5% of a single-node read"
    );
    Arc::try_unwrap(env.meta).ok().unwrap().stop().await;
}
