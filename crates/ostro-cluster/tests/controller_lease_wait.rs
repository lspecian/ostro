//! 13-13 Task 1 (plan check W2): the controller's lease wait on a real 3-voter metadata
//! group with a fake clock and a scripted liveness / contact source.
//!
//! - `failover_waits_for_session_timeout_since_last_heard`: a dead partition leader is
//!   replaced only once the controller has not heard from it for `lease_wait`.
//! - `new_meta_leader_waits_a_full_session_timeout`: a controller that just became meta
//!   leader proposes no failover for `lease_wait`, even for a leader it never heard from.
//! - `resignation_does_not_wait`: a resigned leader gave the partition up itself.
//! - `lease_on_meta_leader_node` / `isolated_meta_leader_stops_serving_reads` (plan check
//!   NB2): the read lease of `ostro_server::route::consistency` (anchor younger than
//!   `session_timeout × (1 − lease_margin)`, rule mirrored in [`lease_holds`]) over the real
//!   13-05 anchor of a 3-voter group: on the meta leader it follows quorum-acked heartbeat
//!   rounds; cut off, the meta leader's lease ends within the lease bound, before any
//!   successor exists. (Here, not in ostro-server's unit tests: an in-process 3-voter group
//!   needs `ostro-raft-driver`, a dependency of this crate only.)

mod support_controller;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_cluster::controller::{self, Controller, ControllerConfig, ProposalRecord};
use ostro_cluster::liveness::{LiveFuture, LiveSource};
use ostro_meta::ext::{ELECTION_REASON_FAILOVER, get_election_info};
use ostro_meta::{MetaCommand, MetaHandle, MetaResult, RaftHandle};
use ostro_partition::Clock;
use support_controller::{Group, partition_of};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const LONG: Duration = Duration::from_secs(20);
const WAIT: Duration = Duration::from_millis(3000);

/// A clock the test moves by hand.
#[derive(Default)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Scripted liveness plus the controller's own contacts, both on the manual clock.
struct Script {
    clock: Arc<ManualClock>,
    dead: Mutex<BTreeSet<u64>>,
    /// Node -> clock ms of the last contact; nodes absent are heard "now".
    last_heard: Mutex<BTreeMap<u64, u64>>,
    wake: Notify,
}

impl Script {
    fn new(clock: &Arc<ManualClock>) -> Arc<Self> {
        Arc::new(Self {
            clock: Arc::clone(clock),
            dead: Mutex::default(),
            last_heard: Mutex::default(),
            wake: Notify::new(),
        })
    }

    /// `node` stops answering now (liveness says dead, contact frozen at now).
    fn kill(&self, node: u64) {
        self.last_heard
            .lock()
            .unwrap()
            .insert(node, self.clock.now_ms());
        self.dead.lock().unwrap().insert(node);
        self.wake.notify_waiters();
    }
}

impl LiveSource for Script {
    fn on_became_controller(&self) {}
    fn poll<'a>(
        &'a self,
        _raft: &'a RaftHandle,
        universe: &'a BTreeSet<u64>,
    ) -> LiveFuture<'a, BTreeSet<u64>> {
        let dead = self.dead.lock().unwrap().clone();
        Box::pin(async move { universe.difference(&dead).copied().collect() })
    }
    fn wake(&self) -> LiveFuture<'_, ()> {
        Box::pin(self.wake.notified())
    }
    fn heard_within<'a>(
        &'a self,
        _raft: &'a RaftHandle,
        within: Duration,
    ) -> LiveFuture<'a, Option<BTreeSet<u64>>> {
        let now = self.clock.now_ms();
        let within = within.as_millis() as u64;
        let heard = self.last_heard.lock().unwrap().clone();
        Box::pin(async move {
            Some(
                (1..=9u64)
                    .filter(|n| {
                        heard
                            .get(n)
                            .is_none_or(|t| now.saturating_sub(*t) <= within)
                    })
                    .collect(),
            )
        })
    }
}

fn cfg() -> ControllerConfig {
    ControllerConfig {
        lease_wait: WAIT,
        ..ControllerConfig::new(3, 2)
    }
}

struct Ctl {
    token: CancellationToken,
    join: tokio::task::JoinHandle<()>,
    records: Arc<Mutex<Vec<ProposalRecord>>>,
}

impl Ctl {
    fn start(meta: Arc<MetaHandle>, live: Arc<Script>, clock: Arc<ManualClock>) -> Ctl {
        let c = Controller::new(meta, live, cfg(), clock);
        let mut events = c.subscribe();
        let records = Arc::new(Mutex::new(Vec::new()));
        let r = Arc::clone(&records);
        tokio::spawn(async move {
            while let Ok(rec) = events.recv().await {
                r.lock().unwrap().push(rec);
            }
        });
        let token = CancellationToken::new();
        let join = tokio::spawn(c.run(token.clone()));
        Ctl {
            token,
            join,
            records,
        }
    }

    fn elects(&self, p: u16) -> Vec<ProposalRecord> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                matches!(r.cmd, MetaCommand::ElectLeader { leader, .. } if leader != 0)
                    && partition_of(&r.cmd) == p
                    && matches!(r.result, Ok(MetaResult::Ok { .. }))
            })
            .cloned()
            .collect()
    }

    async fn stop(self) {
        self.token.cancel();
        let _ = self.join.await;
    }
}

/// Place 2 partitions (controller on the meta leader) and give each the full ISR.
async fn placed(g: &Group, clock: &Arc<ManualClock>, live: &Arc<Script>) -> (u64, Ctl) {
    g.init(2).await;
    let l = g.leader().await;
    let ctl = Ctl::start(g.meta_arc(l), Arc::clone(live), Arc::clone(clock));
    g.wait_for("placement", LONG, |a| a.iter().all(|a| a.epoch == 1))
        .await;
    for a in g.assignments().await {
        g.expand_isr(a.partition, a.replicas.clone()).await;
    }
    (l, ctl)
}

/// Let the controller run a few ticks at the current clock value.
async fn ticks() {
    tokio::time::sleep(Duration::from_millis(400)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_waits_for_session_timeout_since_last_heard() {
    let g = Group::new(3).await;
    let clock = Arc::new(ManualClock(AtomicU64::new(1_000_000)));
    let live = Script::new(&clock);
    let (meta_leader, ctl) = placed(&g, &clock, &live).await;
    // The controller has led for a long time (its own grace is over).
    clock.advance(WAIT.as_millis() as u64 * 2);
    // A partition led by another node.
    let a = g
        .assignments()
        .await
        .into_iter()
        .find(|a| a.leader != meta_leader)
        .expect("a partition led elsewhere");
    let victim = a.leader;
    live.kill(victim);
    // Liveness says dead at once, but the controller heard the victim "just now".
    ticks().await;
    assert!(ctl.elects(a.partition).is_empty(), "elected while heard");
    assert_eq!(g.assignment(a.partition).await.leader, victim);
    // Just below the wait: still nothing.
    clock.advance(WAIT.as_millis() as u64 - 10);
    ticks().await;
    assert!(
        ctl.elects(a.partition).is_empty(),
        "elected {} ms after the last contact",
        WAIT.as_millis() - 10
    );
    // Past the wait: the failover happens.
    clock.advance(20);
    let after = g
        .wait_for("failover", LONG, |v| {
            v.iter()
                .any(|b| b.partition == a.partition && b.leader != victim && b.leader != 0)
        })
        .await;
    let b = after.iter().find(|b| b.partition == a.partition).unwrap();
    assert_eq!(b.epoch, a.epoch + 1);
    let info = get_election_info(&b.ext).unwrap().unwrap();
    assert_eq!(
        (info.reason, info.prev_leader),
        (ELECTION_REASON_FAILOVER, victim)
    );
    ctl.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_meta_leader_waits_a_full_session_timeout() {
    let g = Group::new(3).await;
    let clock = Arc::new(ManualClock(AtomicU64::new(1_000_000)));
    let live = Script::new(&clock);
    let (meta_leader, ctl) = placed(&g, &clock, &live).await;
    let a = g
        .assignments()
        .await
        .into_iter()
        .find(|a| a.leader != meta_leader)
        .expect("a partition led elsewhere");
    let victim = a.leader;
    // Contact with the victim was lost long ago (the controller never heard it lately)...
    live.last_heard.lock().unwrap().insert(victim, 0);
    live.dead.lock().unwrap().insert(victim);
    live.wake.notify_waiters();
    // ...but this controller became meta leader at the start of the clock: no failover
    // until it has led for the whole wait.
    ticks().await;
    assert!(
        ctl.elects(a.partition).is_empty(),
        "a fresh controller elected before its grace ended"
    );
    clock.advance(WAIT.as_millis() as u64 - 10);
    ticks().await;
    assert!(
        ctl.elects(a.partition).is_empty(),
        "elected inside the grace"
    );
    clock.advance(20);
    g.wait_for("failover after the grace", LONG, |v| {
        v.iter()
            .any(|b| b.partition == a.partition && b.leader != victim && b.leader != 0)
    })
    .await;
    ctl.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resignation_does_not_wait() {
    let g = Group::new(3).await;
    let clock = Arc::new(ManualClock(AtomicU64::new(1_000_000)));
    let live = Script::new(&clock);
    let (meta_leader, ctl) = placed(&g, &clock, &live).await;
    let a = g
        .assignments()
        .await
        .into_iter()
        .find(|a| a.leader != meta_leader)
        .expect("a partition led elsewhere");
    let resigner = a.leader;
    // The leader resigns (heard just now, controller fresh): the next ISR member is
    // elected without any wait.
    controller::resign(
        g.meta(resigner),
        resigner,
        a.partition,
        a.epoch,
        clock.as_ref(),
    )
    .await
    .expect("resign");
    g.wait_for("election after resign", LONG, |v| {
        v.iter()
            .any(|b| b.partition == a.partition && b.leader != resigner && b.leader != 0)
    })
    .await;
    ctl.stop().await;
}

/// `ostro_server::route::consistency::lease_limit` / `lease_holds` (default margin 0.2).
fn lease_holds(anchor: Option<std::time::Instant>, st: Duration) -> bool {
    anchor.is_some_and(|a| a.elapsed() < st.mul_f64(0.8))
}

/// Wait until `meta` holds a lease (a fresh leader's first rounds resolve within a
/// heartbeat or two).
async fn until_lease(meta: &MetaHandle, st: Duration) {
    let t = std::time::Instant::now();
    while !lease_holds(meta.lease_anchor(), st) {
        assert!(t.elapsed() < Duration::from_secs(5), "no lease");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_on_meta_leader_node() {
    let mut g = Group::new(3).await;
    let l = g.leader().await;
    let st = Duration::from_millis(600);
    let meta = g.meta_arc(l);
    until_lease(&meta, st).await;
    let t0 = std::time::Instant::now();
    let (mut first, mut last, mut checks) = (None, None, 0u32);
    while t0.elapsed() < Duration::from_secs(2) {
        let a = meta.lease_anchor();
        assert!(lease_holds(a, st), "meta leader lost its lease: {a:?}");
        first.get_or_insert(a.unwrap());
        last = a;
        checks += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        last.unwrap() > first.unwrap() + Duration::from_millis(500),
        "the anchor follows quorum-acked heartbeat rounds"
    );
    // A follower's anchor (ReadIndex round trips through the leader; the first ask starts
    // the rounds and may answer None).
    let f = *g.running().iter().find(|n| **n != l).unwrap();
    let fm = g.meta_arc(f);
    let t1 = std::time::Instant::now();
    while !lease_holds(fm.lease_anchor(), st) {
        assert!(
            t1.elapsed() < Duration::from_secs(2),
            "follower never got a lease"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    eprintln!(
        "meta leader held its lease over {checks} checks in 2 s; follower lease after {:?}",
        t1.elapsed()
    );
    drop((meta, fm));
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_meta_leader_stops_serving_reads() {
    let mut g = Group::new(3).await;
    let l = g.leader().await;
    // session_timeout 400 ms -> lease 320 ms; the meta election timeout is 500-1000 ms.
    let st = Duration::from_millis(400);
    let meta = g.meta_arc(l);
    until_lease(&meta, st).await;
    g.cut(l);
    let cut = std::time::Instant::now();
    let refused = loop {
        if !lease_holds(meta.lease_anchor(), st) {
            break cut.elapsed();
        }
        let others = g
            .running()
            .into_iter()
            .filter(|n| *n != l)
            .any(|n| g.meta(n).is_leader());
        assert!(
            !others,
            "a successor leads while the cut-off leader holds its lease"
        );
        assert!(cut.elapsed() < Duration::from_secs(5), "never refused");
        tokio::time::sleep(Duration::from_millis(2)).await;
    };
    let successor = g
        .running()
        .into_iter()
        .filter(|n| *n != l)
        .find(|n| g.meta(*n).is_leader());
    eprintln!(
        "cut-off meta leader lost its lease {refused:?} after the cut (bound {:?}); successor then: {successor:?}",
        st.mul_f64(0.8)
    );
    assert!(
        refused <= st.mul_f64(0.8) + Duration::from_millis(20),
        "{refused:?}"
    );
    assert!(successor.is_none());
    g.heal(l);
    drop(meta);
    g.stop_all().await;
}
