//! Leader ISR publisher (12-11 Task 3): `AlterIsr` from measured durable progress, applied
//! only from meta. A 3-voter metadata group (in-process routing, `support_controller`),
//! three partition logs, the leader's Fetch server over loopback mTLS and real fetchers.

mod support_controller;
mod support_fetch;
mod support_meta;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_cluster::bootstrap::{bootstrap_command, bootstrap_record};
use ostro_cluster::fetch::{FetchConfig, FetchCtx, fetch_decode_limit};
use ostro_cluster::fetcher::{FetcherConfig, FetcherDriver, FetcherHooks};
use ostro_cluster::install::InstallCtx;
use ostro_cluster::isr_publisher::{IsrMeta, IsrPublisher, IsrPublisherConfig, IsrPublisherExit};
use ostro_log::replica::isr::PendingChange;
use ostro_log::{ReplicaSet, ReplicatedLog};
use ostro_meta::ext::{ELECTION_REASON_FAILOVER, ElectionInfo};
use ostro_meta::{MetaCommand, MetaError, MetaResult};
use ostro_partition::SystemClock;
use ostro_partition::roles::{FollowerDriver, FollowerTask, PartitionAssignmentView};
use ostro_types::{Epoch, Offset};
use support_controller::Group;
use support_fetch::*;
use support_meta::TestCa;
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

const LAG_MAX: Duration = Duration::from_millis(800);
/// Above `TestMeta::hold` (600 ms), so the injected slow re-proposals still complete.
const PROPOSE_TIMEOUT: Duration = Duration::from_millis(1500);

/// What the test meta adapter does to `AlterIsr` proposals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Inject {
    /// Pass every proposal through.
    None,
    /// First Remove: report an unknown outcome without proposing; later calls wait
    /// `hold` and pass through.
    UnknownNotApplied,
    /// First Remove: propose (it applies), then report an unknown outcome.
    UnknownApplied,
    /// First Remove: a concurrent `ElectLeader` (epoch + 1) applies first.
    ElectFirst,
    /// First Remove: the call never returns (a request lost on a broken link); later
    /// calls pass through.
    HangFirst,
}

struct TestMeta {
    g: Arc<Group>,
    inject: Inject,
    hold: Duration,
    calls: AtomicUsize,
    injected: AtomicBool,
    results: Mutex<Vec<Result<MetaResult, String>>>,
}

impl TestMeta {
    fn new(g: Arc<Group>, inject: Inject) -> Arc<TestMeta> {
        Arc::new(TestMeta {
            g,
            inject,
            hold: Duration::from_millis(600),
            calls: AtomicUsize::new(0),
            injected: AtomicBool::new(false),
            results: Mutex::new(Vec::new()),
        })
    }
}

/// A shrink that removes replica 3 (from an ISR that holds it).
fn removes_3(cmd: &MetaCommand, current: &[u64]) -> bool {
    matches!(cmd, MetaCommand::AlterIsr { isr, .. } if !isr.contains(&3) && current.contains(&3))
}

impl IsrMeta for TestMeta {
    fn alter_isr(&self, cmd: MetaCommand) -> BoxFuture<'_, Result<MetaResult, MetaError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let current = self.g.assignment(0).await.isr;
            let first_remove = removes_3(&cmd, &current) && !self.injected.load(Ordering::SeqCst);
            let unknown = || Err(MetaError::Propose("outcome unknown (injected)".into()));
            let r = match self.inject {
                Inject::UnknownNotApplied if first_remove => {
                    self.injected.store(true, Ordering::SeqCst);
                    unknown()
                }
                Inject::UnknownNotApplied if self.injected.load(Ordering::SeqCst) => {
                    tokio::time::sleep(self.hold).await;
                    Ok(self.g.propose(cmd).await)
                }
                Inject::UnknownApplied if first_remove => {
                    self.injected.store(true, Ordering::SeqCst);
                    let applied = self.g.propose(cmd).await;
                    assert!(matches!(applied, MetaResult::Ok { .. }), "{applied:?}");
                    unknown()
                }
                Inject::HangFirst if first_remove => {
                    self.injected.store(true, Ordering::SeqCst);
                    std::future::pending::<()>().await;
                    unreachable!()
                }
                Inject::ElectFirst if first_remove => {
                    self.injected.store(true, Ordering::SeqCst);
                    let a = self.g.assignment(0).await;
                    let elect = self
                        .g
                        .propose(MetaCommand::ElectLeader {
                            partition: 0,
                            epoch: a.epoch + 1,
                            leader: 1,
                            isr: a.isr.clone(),
                            expected_isr_version: a.isr_version,
                            info: ElectionInfo {
                                reason: ELECTION_REASON_FAILOVER,
                                elected_at_ms: 1,
                                prev_leader: 1,
                            },
                        })
                        .await;
                    assert!(matches!(elect, MetaResult::Ok { .. }), "{elect:?}");
                    Ok(self.g.propose(cmd).await)
                }
                _ => Ok(self.g.propose(cmd).await),
            };
            self.results
                .lock()
                .unwrap()
                .push(r.clone().map_err(|e: MetaError| e.to_string()));
            r
        })
    }
}

struct Cluster {
    g: Arc<Group>,
    ca: TestCa,
    logs: Vec<LogFx>, // nodes 1, 2, 3
    srv: Option<ReplServer>,
    rx: watch::Receiver<std::sync::Arc<Vec<PartitionAssignmentView>>>,
    token: CancellationToken,
    meta: Arc<TestMeta>,
    publisher: Option<tokio::task::JoinHandle<IsrPublisherExit>>,
    fetchers: Vec<(u64, Box<dyn FollowerTask>)>,
}

impl Cluster {
    async fn start(inject: Inject) -> Cluster {
        let g = Arc::new(Group::new(3).await);
        g.init(1).await;
        let cur = g.assignment(0).await;
        let rec = bootstrap_record(&cur, vec![1, 2, 3], 1, 1).unwrap();
        let r = g.propose(bootstrap_command(rec)).await;
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
        let a = g.assignment(0).await;
        assert_eq!((a.leader, a.epoch, a.isr.clone()), (1, 1, vec![1]));

        let token = CancellationToken::new();
        let rx = meta_assignments(g.meta(1), token.clone());
        let mut logs = Vec::new();
        for id in 1..=3 {
            logs.push(LogFx::open(id).await);
        }
        logs[0]
            .log
            .become_leader(
                Epoch(1),
                ReplicaSet {
                    replicas: vec![1, 2, 3],
                    isr: vec![1],
                    isr_version: a.isr_version,
                    min_insync: 1,
                },
            )
            .unwrap();
        for f in &logs[1..] {
            f.log.become_follower(Epoch(1), 1).unwrap();
        }
        let ca = test_ca();
        let ctx = Arc::new(FetchCtx::new(
            Arc::clone(&logs[0].log),
            rx.clone(),
            Arc::new(SystemClock),
            FetchConfig {
                max_wait: Duration::from_millis(100),
                ..FetchConfig::default()
            },
        ));
        let limit = fetch_decode_limit(
            ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES,
            FetchConfig::default().max_bytes,
        );
        let srv = ReplServer::start(&ca, 1, Some(ctx), Arc::clone(&logs[0].blobs), limit).await;
        let meta = TestMeta::new(Arc::clone(&g), inject);
        Cluster {
            g,
            ca,
            logs,
            srv: Some(srv),
            rx,
            token,
            meta,
            publisher: None,
            fetchers: Vec::new(),
        }
    }

    fn leader(&self) -> &Arc<ostro_log::PartitionLog> {
        &self.logs[0].log
    }

    fn log(&self, id: u64) -> &Arc<ostro_log::PartitionLog> {
        &self.logs[usize::try_from(id - 1).unwrap()].log
    }

    fn start_publisher(&mut self) {
        let meta: Arc<dyn IsrMeta> = self.meta.clone();
        self.publisher = Some(tokio::spawn(IsrPublisher::run(
            P,
            Arc::clone(self.leader()),
            meta,
            self.rx.clone(),
            Arc::new(SystemClock),
            IsrPublisherConfig {
                replica_lag_max: LAG_MAX,
                resolve_retry: Duration::from_millis(200),
                propose_timeout: PROPOSE_TIMEOUT,
            },
            self.token.clone(),
        )));
    }

    fn start_fetcher(&mut self, id: u64, hooks: FetcherHooks) {
        let d = FetcherDriver::new(
            id,
            node_transport(&self.ca, id, 1, self.srv.as_ref().unwrap().addr),
            Arc::clone(&self.logs[usize::try_from(id - 1).unwrap()].blobs),
            Arc::new(Suspects::default()),
            Arc::new(InstallCtx::default()),
            FetcherConfig {
                max_wait: Duration::from_millis(100),
                ..FetcherConfig::default()
            },
        )
        .with_hooks(hooks);
        let t = d.start(P, Epoch(1), 1, Arc::clone(self.log(id)));
        self.fetchers.push((id, t));
    }

    async fn stop_fetcher(&mut self, id: u64) {
        let i = self.fetchers.iter().position(|(f, _)| *f == id).unwrap();
        let (_, t) = self.fetchers.remove(i);
        t.stop().await;
    }

    fn meta_isr(&self) -> Vec<u64> {
        self.rx
            .borrow()
            .iter()
            .find(|a| a.partition == P)
            .map(|a| a.isr.clone())
            .unwrap_or_default()
    }

    async fn wait_isr(&self, want: &[u64], within: Duration) {
        let mut want = want.to_vec();
        want.sort_unstable();
        let rx = self.rx.clone();
        let leader = Arc::clone(self.leader());
        wait_until(&format!("ISR {want:?}"), within, || {
            let mut m = rx
                .borrow()
                .iter()
                .find(|a| a.partition == P)
                .map(|a| a.isr.clone())
                .unwrap_or_default();
            m.sort_unstable();
            let mut l = leader.replica_status().published_isr;
            l.sort_unstable();
            m == want && l == want
        })
        .await;
    }

    /// Append a batch every 20 ms until the token fires.
    fn appender(&self) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let stop = CancellationToken::new();
        let s = stop.clone();
        let log = Arc::clone(self.leader());
        let h = tokio::spawn(async move {
            let mut i = 10_000u64;
            while !s.is_cancelled() {
                if let Ok(Ok(r)) =
                    tokio::time::timeout(Duration::from_secs(1), log.append(Epoch(1), vec![cmd(i)]))
                        .await
                {
                    let _ = r;
                }
                i += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        (stop, h)
    }

    /// Asserts, until stopped, that while the leader's published ISR still contains 3 its
    /// HW never passes replica 3's durable end. Returns the number of checked samples.
    fn hw_guard(&self) -> (CancellationToken, tokio::task::JoinHandle<usize>) {
        let stop = CancellationToken::new();
        let s = stop.clone();
        let (l, f3) = (Arc::clone(self.leader()), Arc::clone(self.log(3)));
        let h = tokio::spawn(async move {
            let mut n = 0;
            while !s.is_cancelled() {
                let st = l.replica_status();
                if st.published_isr.contains(&3) {
                    let d3 = f3.durable_end();
                    assert!(
                        st.hw <= d3,
                        "HW {} passed replica 3's durable end {d3}",
                        st.hw
                    );
                    n += 1;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            n
        });
        (stop, h)
    }

    async fn shutdown(mut self) {
        self.token.cancel();
        if let Some(p) = self.publisher.take() {
            let _ = p.await;
        }
        for (_, t) in self.fetchers.drain(..) {
            t.stop().await;
        }
        if let Some(s) = self.srv.take() {
            s.stop().await;
        }
        for l in self.logs.drain(..) {
            l.shutdown().await;
        }
    }
}

/// All three replicas in the ISR, published by the publisher itself.
async fn full_isr(inject: Inject) -> Cluster {
    let mut c = Cluster::start(inject).await;
    for i in 0..5u64 {
        append(c.leader(), Epoch(1), vec![cmd(i)]).await;
    }
    c.start_publisher();
    c.start_fetcher(2, FetcherHooks::default());
    c.start_fetcher(3, FetcherHooks::default());
    c.wait_isr(&[1, 2, 3], Duration::from_secs(10)).await;
    c
}

#[tokio::test]
async fn empty_follower_is_never_added_and_a_caught_up_one_is() {
    let mut c = Cluster::start(Inject::None).await;
    // The first command references a blob, so replica 3 (gated) stays empty while fetching.
    let blob = c.logs[0].blobs.put(b"gate").unwrap();
    append(
        c.leader(),
        Epoch(1),
        vec![blob_cmd(0, blob.sha256, blob.len)],
    )
    .await;
    for i in 1..20u64 {
        append(c.leader(), Epoch(1), vec![cmd(i)]).await;
    }
    c.start_publisher();
    // Nobody fetches: nobody is added.
    tokio::time::sleep(LAG_MAX * 2).await;
    assert_eq!(c.meta_isr(), vec![1]);

    let gate = Arc::new(Notify::new());
    let g2 = Arc::clone(&gate);
    c.start_fetcher(
        3,
        FetcherHooks {
            before_blob_fetch: Some(Arc::new(move |_| {
                let g = Arc::clone(&g2);
                Box::pin(async move { g.notified().await })
            })),
            ..FetcherHooks::default()
        },
    );
    c.start_fetcher(2, FetcherHooks::default());
    c.wait_isr(&[1, 2], Duration::from_secs(10)).await;
    assert!(c.log(2).durable_end() >= *c.leader().high_watermark().borrow());
    // Replica 3 fetched (it is measured) but holds nothing: never added, not even pending.
    tokio::time::sleep(LAG_MAX * 2).await;
    assert_eq!(c.log(3).durable_end(), Offset(0));
    let row3 = c
        .leader()
        .replica_status()
        .followers
        .into_iter()
        .find(|f| f.replica == 3)
        .unwrap();
    assert_eq!(row3.durable_leo, Offset(0));
    let mut isr = c.meta_isr();
    isr.sort_unstable();
    assert_eq!(isr, vec![1, 2]);
    assert!(c.leader().isr_view().unwrap().isr.pending.is_none());
    // Released, it catches up and joins.
    gate.notify_one();
    c.wait_isr(&[1, 2, 3], Duration::from_secs(10)).await;
    assert!(c.log(3).durable_end() >= *c.leader().high_watermark().borrow());
    c.shutdown().await;
}

#[tokio::test]
async fn stalled_follower_is_removed_and_hw_waits_until_meta_commits() {
    let mut c = full_isr(Inject::None).await;
    let (gstop, guard) = c.hw_guard();
    c.stop_fetcher(3).await;
    let d3 = c.log(3).durable_end();
    let (astop, appender) = c.appender();
    c.wait_isr(&[1, 2], LAG_MAX * 6).await;
    // Once the shrink is published the HW moves past replica 3.
    let l = Arc::clone(c.leader());
    wait_until("HW past replica 3", Duration::from_secs(5), || {
        *l.high_watermark().borrow() > d3
    })
    .await;
    astop.cancel();
    appender.await.unwrap();
    gstop.cancel();
    assert!(guard.await.unwrap() > 10);
    c.shutdown().await;
}

async fn outcome_unknown(inject: Inject) {
    let mut c = full_isr(inject).await;
    let (gstop, guard) = c.hw_guard();
    c.stop_fetcher(3).await;
    let d3 = c.log(3).durable_end();
    let (astop, appender) = c.appender();
    let m = Arc::clone(&c.meta);
    wait_until("injected", LAG_MAX * 6, || {
        m.injected.load(Ordering::SeqCst)
    })
    .await;
    if inject == Inject::UnknownNotApplied {
        // Unresolved: the pending shrink still counts, meta still has 3, HW waits.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let v = c.leader().isr_view().unwrap();
        assert_eq!(v.isr.pending, Some(PendingChange::Remove(3)));
        assert!(v.isr.published.contains(&3));
        assert!(c.meta_isr().contains(&3));
        assert!(*c.leader().high_watermark().borrow() <= d3);
    }
    c.wait_isr(&[1, 2], Duration::from_secs(10)).await;
    let l = Arc::clone(c.leader());
    wait_until("HW past replica 3", Duration::from_secs(5), || {
        *l.high_watermark().borrow() > d3
    })
    .await;
    assert!(c.leader().isr_view().unwrap().isr.pending.is_none());
    astop.cancel();
    appender.await.unwrap();
    gstop.cancel();
    assert!(guard.await.unwrap() > 10);
    let results = c.meta.results.lock().unwrap().clone();
    assert!(results.iter().any(|r| r.is_err()), "{results:?}");
    c.shutdown().await;
}

#[tokio::test]
async fn outcome_unknown_not_applied_keeps_the_shrink_pending_w4() {
    outcome_unknown(Inject::UnknownNotApplied).await;
}

#[tokio::test]
async fn outcome_unknown_applied_is_adopted_from_meta_w4() {
    outcome_unknown(Inject::UnknownApplied).await;
}

/// 12-16 `isr_slow_follower_shrinks`: an `AlterIsr` whose request is lost (never answered)
/// is an unknown outcome after `propose_timeout`; the same CAS is re-proposed and the shrink
/// lands instead of waiting for the forwarder's 10 s call deadline.
#[tokio::test]
async fn hung_alter_isr_is_retried_after_the_propose_timeout() {
    let mut c = full_isr(Inject::HangFirst).await;
    c.stop_fetcher(3).await;
    let (astop, appender) = c.appender();
    let m = Arc::clone(&c.meta);
    wait_until("injected", LAG_MAX * 6, || {
        m.injected.load(Ordering::SeqCst)
    })
    .await;
    let v = c.leader().isr_view().unwrap();
    assert_eq!(v.isr.pending, Some(PendingChange::Remove(3)));
    c.wait_isr(&[1, 2], PROPOSE_TIMEOUT + Duration::from_secs(2))
        .await;
    assert!(m.calls.load(Ordering::SeqCst) >= 2);
    astop.cancel();
    appender.await.unwrap();
    c.shutdown().await;
}

/// 12-16: a replica the controller shrank out (liveness said dead) is not re-added while
/// its tracker row merely still looks caught up: it must fetch again first. Before, the
/// leader re-added it at once and the two ping-ponged `AlterIsr` for `replica_lag_max`,
/// stalling the HW on the dead node each time.
#[tokio::test]
async fn controller_shrink_is_not_undone_without_a_new_fetch() {
    let mut c = full_isr(Inject::None).await;
    c.stop_fetcher(3).await;
    // The controller's shrink (meta proposal from outside the publisher).
    let a = c.g.assignment(0).await;
    let r =
        c.g.propose(MetaCommand::AlterIsr {
            partition: 0,
            epoch: a.epoch,
            expected_isr_version: a.isr_version,
            isr: vec![1, 2],
        })
        .await;
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    c.wait_isr(&[1, 2], Duration::from_secs(5)).await;
    let calls = c.meta.calls.load(Ordering::SeqCst);
    // Within replica_lag_max replica 3 still looks caught up; it must stay out.
    tokio::time::sleep(LAG_MAX / 2).await;
    assert!(!c.meta_isr().contains(&3), "{:?}", c.meta_isr());
    assert_eq!(
        c.meta.calls.load(Ordering::SeqCst),
        calls,
        "no AlterIsr re-adding 3"
    );
    // Once it fetches again it is re-added as usual.
    c.start_fetcher(3, FetcherHooks::default());
    c.wait_isr(&[1, 2, 3], Duration::from_secs(10)).await;
    c.shutdown().await;
}

#[tokio::test]
async fn alter_isr_rejected_by_a_concurrent_election_kip497() {
    let mut c = full_isr(Inject::ElectFirst).await;
    let (gstop, guard) = c.hw_guard();
    c.stop_fetcher(3).await;
    let d3 = c.log(3).durable_end();
    let (astop, appender) = c.appender();
    let publisher = c.publisher.take().unwrap();
    let exit = tokio::time::timeout(LAG_MAX * 8, publisher)
        .await
        .expect("publisher exits")
        .unwrap();
    assert_eq!(exit, IsrPublisherExit::LostLeadership);
    let results = c.meta.results.lock().unwrap().clone();
    assert!(
        matches!(results.last(), Some(Ok(MetaResult::StaleVersion { .. }))),
        "{results:?}"
    );
    // Meta moved to epoch 2 with the full ISR; the old leader's HW never passed replica 3.
    let a = c.g.assignment(0).await;
    assert_eq!(a.epoch, 2);
    assert!(a.isr.contains(&3));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(*c.leader().high_watermark().borrow() <= d3);
    assert!(c.leader().isr_view().unwrap().isr.pending.is_none());
    astop.cancel();
    appender.await.unwrap();
    gstop.cancel();
    assert!(guard.await.unwrap() > 10);
    c.shutdown().await;
}
