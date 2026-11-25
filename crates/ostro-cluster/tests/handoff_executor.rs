//! 13-11: the leader-side handoff executor (`LocalHandoff`), its `NodeOps` client's local
//! short-circuit and the controlled-shutdown order, over fakes for the meta, the log and
//! the partition's `HandoffControl`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_cluster::contracts::{HandoffClient, HandoffError, HandoffExecutor, HandoffOutcome};
use ostro_cluster::handoff::{
    HandoffConfig, HandoffLog, HandoffLogLookup, HandoffMeta, LocalHandoff, NodeOpsHandoffClient,
};
use ostro_cluster::shutdown::{
    LedPartition, ShutdownMeta, ShutdownReport, controlled_shutdown_with,
};
use ostro_cluster::transport::{ClusterIncoming, ClusterTransport, TransportError};
use ostro_meta::{MetaCommand, MetaResult, PartitionAssignment};
use ostro_partition::{
    HandoffControl, HandoffGuard, HandoffRelease, HandoffReport, PartitionError,
};
use ostro_types::{Epoch, Offset, PartitionId};

const P: PartitionId = PartitionId(3);
const ME: u64 = 1;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

// ------------------------------------------------------------------ fakes

/// Partition-side control: records what happened to the guard.
#[derive(Default)]
struct Control {
    events: Mutex<Vec<String>>,
    fail_prepare: Mutex<Option<PartitionError>>,
}

impl Control {
    fn events(&self) -> Vec<String> {
        lock(&self.events).clone()
    }
}

struct ControlRef(Arc<Control>);

impl HandoffControl for ControlRef {
    fn prepare_handoff(
        &self,
        p: PartitionId,
        grace: Duration,
    ) -> BoxFuture<'_, Result<HandoffGuard, PartitionError>> {
        let c = Arc::clone(&self.0);
        Box::pin(async move {
            if let Some(e) = lock(&c.fail_prepare).take() {
                return Err(e);
            }
            lock(&c.events).push(format!("prepare p{} grace {grace:?}", p.0));
            let rel = Arc::clone(&c);
            let frz = Arc::clone(&c);
            Ok(HandoffGuard::new(p, move |how| {
                lock(&rel.events).push(match how {
                    HandoffRelease::Aborted => "abort".into(),
                    HandoffRelease::Completed => "complete".into(),
                });
            })
            .with_report(HandoffReport {
                epoch: Epoch(5),
                leased_left: 0,
                evicted: 0,
                started_at_ms: 1_700_000_000_000,
            })
            .with_freeze(move || lock(&frz.events).push("freeze".into())))
        })
    }

    fn in_handoff(&self, _p: PartitionId) -> bool {
        false
    }
}

/// Meta: one assignment; `propose` applies an ElectLeader (CAS checks like meta apply) or
/// answers a scripted result.
struct Meta {
    a: Mutex<PartitionAssignment>,
    proposals: Mutex<Vec<MetaCommand>>,
    answer: Mutex<Option<MetaResult>>,
}

impl Meta {
    fn new(leader: u64, epoch: u32, isr: &[u64]) -> Arc<Self> {
        Arc::new(Self {
            a: Mutex::new(PartitionAssignment {
                partition: P.0,
                leader,
                epoch,
                isr: isr.to_vec(),
                replicas: vec![1, 2, 3],
                isr_version: 11,
                min_insync: 1,
                ext: Vec::new(),
            }),
            proposals: Mutex::new(Vec::new()),
            answer: Mutex::new(None),
        })
    }

    fn set_isr(&self, isr: &[u64]) {
        let mut a = lock(&self.a);
        a.isr = isr.to_vec();
        a.isr_version += 1;
    }
}

impl HandoffMeta for Meta {
    fn node_id(&self) -> u64 {
        ME
    }

    fn assignment(&self, partition: u16) -> Option<PartitionAssignment> {
        let a = lock(&self.a).clone();
        (a.partition == partition).then_some(a)
    }

    fn propose(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, String>> {
        lock(&self.proposals).push(cmd.clone());
        let scripted = lock(&self.answer).take();
        Box::pin(async move {
            if let Some(r) = scripted {
                return Ok(r);
            }
            let MetaCommand::ElectLeader {
                epoch,
                leader,
                isr,
                expected_isr_version,
                ..
            } = cmd
            else {
                return Ok(MetaResult::Invalid("unexpected".into()));
            };
            let mut a = lock(&self.a);
            if epoch <= a.epoch || expected_isr_version != a.isr_version {
                return Ok(MetaResult::StaleVersion {
                    current: u64::from(a.epoch),
                });
            }
            if !a.isr.contains(&leader) || isr.iter().any(|n| !a.isr.contains(n)) {
                return Ok(MetaResult::Invalid("outside the published ISR".into()));
            }
            a.leader = leader;
            a.epoch = epoch;
            a.isr = isr;
            a.isr_version += 1;
            Ok(MetaResult::Ok {
                version: u64::from(epoch),
            })
        })
    }
}

/// Leader log: end, durable end and per-follower durable LEOs.
struct Log {
    end: Mutex<u64>,
    durable: Mutex<u64>,
    followers: Mutex<BTreeMap<u64, u64>>,
}

impl Log {
    fn new(end: u64, followers: &[(u64, u64)]) -> Arc<Self> {
        Arc::new(Self {
            end: Mutex::new(end),
            durable: Mutex::new(end),
            followers: Mutex::new(followers.iter().copied().collect()),
        })
    }

    fn set_follower(&self, node: u64, leo: u64) {
        lock(&self.followers).insert(node, leo);
    }
}

impl HandoffLog for Log {
    fn log_end(&self) -> Offset {
        Offset(*lock(&self.end))
    }

    fn durable_end(&self) -> Offset {
        Offset(*lock(&self.durable))
    }

    fn follower_durable(&self, node: u64) -> Option<Offset> {
        lock(&self.followers).get(&node).copied().map(Offset)
    }
}

fn cfg(catchup_ms: u64) -> HandoffConfig {
    HandoffConfig {
        grace: Duration::from_millis(50),
        catchup_timeout: Duration::from_millis(catchup_ms),
    }
}

fn executor(
    control: &Arc<Control>,
    meta: &Arc<Meta>,
    log: &Arc<Log>,
    catchup_ms: u64,
) -> LocalHandoff {
    let l = Arc::clone(log);
    let logs: HandoffLogLookup =
        Arc::new(move |p| (p == P).then(|| Arc::clone(&l) as Arc<dyn HandoffLog>));
    LocalHandoff::with_parts(
        Arc::new(ControlRef(Arc::clone(control))),
        Arc::clone(meta) as Arc<dyn HandoffMeta>,
        logs,
        cfg(catchup_ms),
    )
}

fn aborted(o: &HandoffOutcome) -> &str {
    match o {
        HandoffOutcome::Aborted { reason } => reason,
        HandoffOutcome::Moved { .. } => panic!("expected an abort, got {o:?}"),
    }
}

// ------------------------------------------------------------------ executor

#[tokio::test]
async fn success_path_quiesces_freezes_elects_and_completes() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    let log = Log::new(40, &[(2, 40), (3, 12)]);
    let ex = executor(&control, &meta, &log, 2_000);
    let out = ex.execute(P, 2, Epoch(5)).await.expect("handoff");
    assert_eq!(
        out,
        HandoffOutcome::Moved {
            new_epoch: Epoch(6)
        }
    );
    assert_eq!(
        control.events(),
        ["prepare p3 grace 50ms", "freeze", "complete"],
        "quiesce, fence, then complete"
    );
    let props = lock(&meta.proposals).clone();
    assert_eq!(props.len(), 1);
    match &props[0] {
        MetaCommand::ElectLeader {
            partition,
            epoch,
            leader,
            isr,
            expected_isr_version,
            info,
        } => {
            assert_eq!((*partition, *epoch, *leader), (3, 6, 2));
            assert_eq!(isr, &vec![1, 2, 3], "the published ISR, unchanged");
            assert_eq!(*expected_isr_version, 11);
            assert_eq!(info.prev_leader, ME);
            assert_eq!(info.elected_at_ms, 1_700_000_000_000, "the partition clock");
        }
        other => panic!("unexpected proposal {other:?}"),
    }
}

#[tokio::test]
async fn catch_up_waits_for_the_target() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    let log = Log::new(40, &[(2, 30)]);
    let ex = executor(&control, &meta, &log, 5_000);
    let l = Arc::clone(&log);
    let bump = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(lock(&l.followers).get(&2) == Some(&30));
        l.set_follower(2, 40);
    });
    let out = ex.execute(P, 2, Epoch(5)).await.expect("handoff");
    bump.await.expect("join");
    assert_eq!(
        out,
        HandoffOutcome::Moved {
            new_epoch: Epoch(6)
        }
    );
}

#[tokio::test]
async fn target_not_in_isr_is_refused_before_quiescing() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 3]);
    let log = Log::new(40, &[(2, 40)]);
    let ex = executor(&control, &meta, &log, 2_000);
    let err = ex.execute(P, 2, Epoch(5)).await.expect_err("refused");
    assert_eq!(
        err,
        HandoffError::TargetNotInIsr {
            partition: 3,
            target: 2
        }
    );
    assert!(control.events().is_empty(), "nothing quiesced");
    assert!(lock(&meta.proposals).is_empty());
}

#[tokio::test]
async fn not_leader_or_stale_epoch_is_refused() {
    let control = Arc::new(Control::default());
    let log = Log::new(40, &[(2, 40)]);
    let meta = Meta::new(2, 5, &[1, 2, 3]);
    let ex = executor(&control, &meta, &log, 2_000);
    assert_eq!(
        ex.execute(P, 3, Epoch(5)).await,
        Err(HandoffError::NotLeader {
            partition: 3,
            epoch: 5
        })
    );
    let meta = Meta::new(ME, 6, &[1, 2, 3]);
    let ex = executor(&control, &meta, &log, 2_000);
    assert_eq!(
        ex.execute(P, 2, Epoch(5)).await,
        Err(HandoffError::NotLeader {
            partition: 3,
            epoch: 5
        }),
        "the caller saw an older epoch"
    );
    // The partition itself refuses (demoted meanwhile).
    *lock(&control.fail_prepare) = Some(PartitionError::NotLeader);
    assert_eq!(
        ex.execute(P, 2, Epoch(6)).await,
        Err(HandoffError::NotLeader {
            partition: 3,
            epoch: 6
        })
    );
    assert!(control.events().is_empty());
}

#[tokio::test]
async fn catch_up_timeout_aborts_and_restores_admission() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    let log = Log::new(40, &[(2, 12)]);
    let ex = executor(&control, &meta, &log, 200);
    let out = ex.execute(P, 2, Epoch(5)).await.expect("answered");
    let reason = aborted(&out);
    assert!(
        reason.contains("catch-up timeout") || reason.contains("not catching up"),
        "{out:?}"
    );
    assert_eq!(
        control.events(),
        ["prepare p3 grace 50ms", "freeze", "abort"]
    );
    assert!(lock(&meta.proposals).is_empty(), "no election");
    assert_eq!(lock(&meta.a).leader, ME, "this node keeps leading");
}

#[tokio::test]
async fn a_target_that_stopped_fetching_aborts_before_the_timeout() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    let log = Log::new(40, &[(2, 12)]);
    let ex = executor(&control, &meta, &log, 60_000);
    let out = tokio::time::timeout(Duration::from_secs(20), ex.execute(P, 2, Epoch(5)))
        .await
        .expect("aborted on the stall, not at the 60 s catch-up bound")
        .expect("answered");
    assert!(aborted(&out).contains("not catching up"), "{out:?}");
    assert_eq!(control.events().last().map(String::as_str), Some("abort"));
    assert!(lock(&meta.proposals).is_empty());
}

#[tokio::test]
async fn target_leaving_the_isr_aborts() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    let log = Log::new(40, &[(2, 12)]);
    let ex = executor(&control, &meta, &log, 5_000);
    let m = Arc::clone(&meta);
    let shrink = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        m.set_isr(&[1, 3]);
    });
    let out = ex.execute(P, 2, Epoch(5)).await.expect("answered");
    shrink.await.expect("join");
    assert!(aborted(&out).contains("left the ISR"), "{out:?}");
    assert_eq!(control.events().last().map(String::as_str), Some("abort"));
    assert!(lock(&meta.proposals).is_empty());
}

#[tokio::test]
async fn stale_version_aborts() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    *lock(&meta.answer) = Some(MetaResult::StaleVersion { current: 12 });
    let log = Log::new(40, &[(2, 40)]);
    let ex = executor(&control, &meta, &log, 2_000);
    let out = ex.execute(P, 2, Epoch(5)).await.expect("answered");
    assert!(aborted(&out).contains("stale"), "{out:?}");
    assert_eq!(control.events().last().map(String::as_str), Some("abort"));
}

#[tokio::test]
async fn invalid_election_aborts() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    *lock(&meta.answer) = Some(MetaResult::Invalid("leader not in ISR".into()));
    let log = Log::new(40, &[(2, 40)]);
    let ex = executor(&control, &meta, &log, 2_000);
    let out = ex.execute(P, 2, Epoch(5)).await.expect("answered");
    assert!(aborted(&out).contains("not applied"), "{out:?}");
    assert_eq!(control.events().last().map(String::as_str), Some("abort"));
}

/// A transport that must not be dialed (the leader is this node).
struct NoTransport;

impl ClusterTransport for NoTransport {
    fn channel(&self, node: u64) -> Result<tonic::transport::Channel, TransportError> {
        Err(TransportError::UnknownNode(node))
    }

    fn incoming(&self) -> ClusterIncoming {
        ClusterIncoming::Tcp
    }
}

#[tokio::test]
async fn node_ops_client_serves_a_local_leader_without_the_network() {
    let control = Arc::new(Control::default());
    let meta = Meta::new(ME, 5, &[1, 2, 3]);
    let log = Log::new(40, &[(2, 40)]);
    let local = Arc::new(executor(&control, &meta, &log, 2_000));
    let client = NodeOpsHandoffClient::new(Arc::new(NoTransport), local);
    let out = client.hand_off(ME, P, 2, Epoch(5)).await.expect("local");
    assert_eq!(
        out,
        HandoffOutcome::Moved {
            new_epoch: Epoch(6)
        }
    );
    // A remote leader goes through the transport (here: unknown node).
    let err = client
        .hand_off(7, P, 2, Epoch(6))
        .await
        .expect_err("remote");
    assert!(matches!(err, HandoffError::Transport(_)), "{err:?}");
}

// ------------------------------------------------------------------ controlled shutdown

/// Shutdown meta + handoff client sharing one event log.
struct Cluster {
    events: Mutex<Vec<String>>,
    members: usize,
    led: Vec<LedPartition>,
    meta_target: Option<u64>,
    /// The metadata quorum probe succeeds.
    quorum: bool,
    /// `(partition, target)` pairs that abort.
    refuse: Vec<(u16, u64)>,
    /// Handoffs take this long.
    delay: Duration,
}

impl Cluster {
    fn new(led: Vec<LedPartition>) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            members: 3,
            led,
            meta_target: Some(2),
            quorum: true,
            refuse: Vec::new(),
            delay: Duration::from_millis(5),
        })
    }

    fn events(&self) -> Vec<String> {
        lock(&self.events).clone()
    }
}

struct MetaView(Arc<Cluster>);

impl ShutdownMeta for MetaView {
    fn members(&self) -> usize {
        self.0.members
    }

    fn led(&self, node: u64) -> Vec<LedPartition> {
        assert_eq!(node, ME);
        self.0.led.clone()
    }

    fn meta_transfer_target(&self) -> Option<u64> {
        self.0.meta_target
    }

    fn transfer_leader(&self, to: u64) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            lock(&self.0.events).push(format!("meta -> {to}"));
            Ok(())
        })
    }

    fn quorum_contact(&self) -> BoxFuture<'_, bool> {
        Box::pin(async move { self.0.quorum })
    }
}

struct Client(Arc<Cluster>);

impl HandoffClient for Client {
    fn hand_off(
        &self,
        leader: u64,
        p: PartitionId,
        to: u64,
        epoch: Epoch,
    ) -> BoxFuture<'_, Result<HandoffOutcome, HandoffError>> {
        let c = Arc::clone(&self.0);
        Box::pin(async move {
            assert_eq!(leader, ME);
            tokio::time::sleep(c.delay).await;
            lock(&c.events).push(format!("p{} -> {to}", p.0));
            if c.refuse.contains(&(p.0, to)) {
                return Ok(HandoffOutcome::Aborted {
                    reason: "target lags".into(),
                });
            }
            Ok(HandoffOutcome::Moved {
                new_epoch: Epoch(epoch.0 + 1),
            })
        })
    }
}

fn led(partition: u16, targets: &[u64]) -> LedPartition {
    LedPartition {
        partition,
        epoch: 4,
        targets: targets.to_vec(),
    }
}

async fn shutdown(c: &Arc<Cluster>, timeout: Duration) -> ShutdownReport {
    controlled_shutdown_with(
        ME,
        &MetaView(Arc::clone(c)),
        Arc::new(Client(Arc::clone(c))),
        timeout,
    )
    .await
}

#[tokio::test]
async fn shutdown_moves_partitions_before_meta_then_returns() {
    let c = Cluster::new(vec![
        led(0, &[2, 3]),
        led(1, &[3, 2]),
        led(2, &[2]),
        led(5, &[]),
        led(6, &[3]),
        led(7, &[2, 3]),
    ]);
    let r = shutdown(&c, Duration::from_secs(10)).await;
    assert_eq!(r.moved, vec![0, 1, 2, 6, 7]);
    assert_eq!(r.kept, vec![5], "no other ISR member");
    assert!(r.meta_transferred);
    let ev = c.events();
    assert_eq!(ev.last().map(String::as_str), Some("meta -> 2"), "{ev:?}");
    assert_eq!(ev.iter().filter(|e| e.starts_with("meta")).count(), 1);
    assert!(
        ev.contains(&"p1 -> 3".to_string()),
        "preferred target first"
    );
}

#[tokio::test]
async fn shutdown_tries_the_next_target_after_an_abort() {
    let mut c = Cluster::new(vec![led(0, &[2, 3]), led(1, &[2])]);
    Arc::get_mut(&mut c).expect("unique").refuse = vec![(0, 2), (1, 2)];
    let r = shutdown(&c, Duration::from_secs(10)).await;
    assert_eq!(r.moved, vec![0]);
    assert_eq!(r.kept, vec![1]);
    let ev = c.events();
    let first = ev.iter().position(|e| e == "p0 -> 2").expect("tried 2");
    let second = ev.iter().position(|e| e == "p0 -> 3").expect("then 3");
    assert!(first < second);
}

#[tokio::test]
async fn shutdown_is_bounded_and_skips_meta_on_timeout() {
    let mut c = Cluster::new(vec![led(0, &[2]), led(1, &[2])]);
    Arc::get_mut(&mut c).expect("unique").delay = Duration::from_secs(30);
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        shutdown(&c, Duration::from_millis(200)),
    )
    .await
    .expect("bounded by shutdown_handoff_timeout");
    assert!(r.moved.is_empty());
    assert_eq!(r.kept, vec![0, 1], "left to Phase 12 failover");
    assert!(!r.meta_transferred);
    assert!(c.events().is_empty());
}

#[tokio::test]
async fn single_member_cluster_skips_the_handoff() {
    let mut c = Cluster::new(vec![led(0, &[])]);
    Arc::get_mut(&mut c).expect("unique").members = 1;
    let r = shutdown(&c, Duration::from_secs(1)).await;
    assert_eq!(r, ShutdownReport::default());
    assert!(c.events().is_empty());
}

#[tokio::test]
async fn no_metadata_quorum_skips_the_handoffs_at_once() {
    let mut c = Cluster::new(vec![led(0, &[2]), led(4, &[3])]);
    Arc::get_mut(&mut c).expect("unique").quorum = false;
    let r = shutdown(&c, Duration::from_secs(30)).await;
    assert!(r.moved.is_empty());
    assert_eq!(r.kept, vec![0, 4]);
    assert!(!r.meta_transferred);
    assert!(c.events().is_empty(), "nothing attempted");
}
