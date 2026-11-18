//! Lazy windowed timer service (plan 02-14, STOR-13): timers fire once via `TimerFired`, the
//! in-memory window stays bounded, overdue timers fire after a restart, and a duplicate
//! `TimerFired` has no effect.
#![allow(clippy::disallowed_methods)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use common::{T, run_on, start_run};
use ostro_partition::leader::timers::{TimerConfig, TimerStats};
use ostro_partition::store::keys::{ready_prefix, run_key};
use ostro_partition::store::values::{self, InboxMsg, RunRecord, RunStatus};
use ostro_partition::{ApplyOutcome, ManualClock, Node, NodeConfig, PartitionHandle, Table};
use ostro_types::command::{
    AttemptTokenMsg, CancelTimer, Derived, EventsAppended, JournalEvent, ScheduleTimer, TimerFired,
    derived,
};
use ostro_types::{Command, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::time::{sleep, timeout};

const START: u64 = 1_000_000;
const P0: PartitionId = PartitionId(0);

type Handle = PartitionHandle<ostro_log::Rf1Log>;

async fn open(dir: &Path, clock: &ManualClock, timers: TimerConfig) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    cfg.clock = Arc::new(clock.clone());
    cfg.leader.timers = timers;
    timeout(T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn h(node: &Node) -> &Handle {
    node.handle(P0).expect("partition ready")
}

async fn submit(h: &Handle, c: Command) -> ApplyOutcome {
    timeout(T, h.submit_one(c))
        .await
        .expect("submit timed out")
        .expect("submit")
}

/// A started, granted run on partition 0; returns the run and its token.
async fn running(h: &Handle) -> (RunId, AttemptTokenMsg) {
    let run = run_on(0);
    assert_eq!(
        submit(h, Command::start_run(start_run(&run))).await,
        ApplyOutcome::Accepted
    );
    let t = h.grant_attempt(run).await.expect("grant");
    (
        run,
        AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        },
    )
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn sched(op: u64, due: u64) -> Derived {
    intent(derived::Intent::ScheduleTimer(ScheduleTimer {
        op,
        due_at_ms: due,
    }))
}

fn cancel(op: u64, due: u64) -> Derived {
    intent(derived::Intent::CancelTimer(CancelTimer {
        op,
        due_at_ms: due,
    }))
}

fn events(run: &RunId, t: AttemptTokenMsg, seq: u64, derived: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(t),
        from_seq: seq,
        events: vec![JournalEvent {
            seq,
            kind: 16,
            payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
        }],
        derived,
    })
}

/// `(op, fired_at_ms)` of every `TimerFired` in the run's inbox.
fn inbox_fired(h: &Handle, run: &RunId) -> Vec<(u64, u64)> {
    h.reader()
        .prefix(Table::Inbox, &run_key(run), usize::MAX)
        .expect("inbox")
        .iter()
        .filter_map(
            |(_, v)| match values::decode::<InboxMsg>(v).expect("inbox msg") {
                InboxMsg::TimerFired { op, fired_at_ms } => Some((op, fired_at_ms)),
                _ => None,
            },
        )
        .collect()
}

fn timer_rows(h: &Handle) -> usize {
    h.reader()
        .prefix(Table::Timers, &[], usize::MAX)
        .expect("timers")
        .len()
}

/// Poll `f` until true (real time), panicking after `limit`.
async fn eventually(limit: Duration, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        sleep(Duration::from_millis(10)).await;
    }
}

fn stats_cfg() -> (TimerConfig, Arc<TimerStats>) {
    let stats = Arc::new(TimerStats::default());
    (
        TimerConfig {
            stats: Some(Arc::clone(&stats)),
            ..TimerConfig::default()
        },
        stats,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timer_fires_once_and_row_is_removed() {
    let tmp = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(START);
    let (cfg, stats) = stats_cfg();
    let node = open(tmp.path(), &clock, cfg).await;
    let h = h(&node);
    let (run, t) = running(h).await;
    assert_eq!(
        submit(h, events(&run, t, 0, vec![sched(1, START + 1_000)])).await,
        ApplyOutcome::Accepted
    );
    sleep(Duration::from_millis(200)).await;
    assert!(inbox_fired(h, &run).is_empty(), "fired before due");
    clock.advance(1_000);
    eventually(Duration::from_secs(2), "timer fired", || {
        !inbox_fired(h, &run).is_empty()
    })
    .await;
    sleep(Duration::from_millis(200)).await;
    assert_eq!(inbox_fired(h, &run), vec![(1, START + 1_000)]);
    assert_eq!(timer_rows(h), 0);
    assert_eq!(stats.fired.load(Ordering::Relaxed), 1);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_timer_never_fires() {
    let tmp = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(START);
    let (cfg, stats) = stats_cfg();
    let node = open(tmp.path(), &clock, cfg).await;
    let h = h(&node);
    let (run, t) = running(h).await;
    let due = START + 1_000;
    assert_eq!(
        submit(h, events(&run, t, 0, vec![sched(1, due)])).await,
        ApplyOutcome::Accepted
    );
    assert_eq!(
        submit(h, events(&run, t, 1, vec![cancel(1, due)])).await,
        ApplyOutcome::Accepted
    );
    clock.advance(5_000);
    sleep(Duration::from_secs(1)).await;
    assert!(inbox_fired(h, &run).is_empty());
    assert_eq!(timer_rows(h), 0);
    assert_eq!(stats.fired.load(Ordering::Relaxed), 0);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_start_becomes_ready() {
    let tmp = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(START);
    let node = open(tmp.path(), &clock, TimerConfig::default()).await;
    let h = h(&node);
    let run = run_on(0);
    let mut s = start_run(&run);
    s.requested_at_ms = START;
    s.ready_at_ms = START + 5_000;
    assert_eq!(
        submit(h, Command::start_run(s)).await,
        ApplyOutcome::Accepted
    );
    let status = |h: &Handle| -> RunStatus {
        let rec: RunRecord = h.read_run(&run).expect("read").expect("run");
        rec.status
    };
    assert_eq!(status(h), RunStatus::Scheduled);
    sleep(Duration::from_millis(200)).await;
    assert_eq!(status(h), RunStatus::Scheduled);
    clock.advance(5_000);
    eventually(Duration::from_secs(2), "run ready", || {
        status(h) == RunStatus::Ready
    })
    .await;
    let ready = h
        .reader()
        .prefix(Table::Ready, &ready_prefix("q"), usize::MAX)
        .expect("ready");
    assert_eq!(ready.len(), 1);
    assert_eq!(timer_rows(h), 0);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overdue_timer_fires_once_after_restart() {
    let tmp = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(START);
    let node = open(tmp.path(), &clock, TimerConfig::default()).await;
    let (run, t) = running(h(&node)).await;
    assert_eq!(
        submit(h(&node), events(&run, t, 0, vec![sched(7, START + 500)])).await,
        ApplyOutcome::Accepted
    );
    node.shutdown().await.expect("shutdown");

    clock.advance(10_000);
    let (cfg, stats) = stats_cfg();
    let node = open(tmp.path(), &clock, cfg).await;
    let h = h(&node);
    eventually(Duration::from_secs(2), "overdue timer fired", || {
        !inbox_fired(h, &run).is_empty()
    })
    .await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(inbox_fired(h, &run), vec![(7, START + 10_000)]);
    assert_eq!(stats.fired.load(Ordering::Relaxed), 1);
    assert_eq!(timer_rows(h), 0);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_timer_fired_has_no_effect() {
    let tmp = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(START);
    let node = open(tmp.path(), &clock, TimerConfig::default()).await;
    let h = h(&node);
    let (run, t) = running(h).await;
    let due = START + 100;
    assert_eq!(
        submit(h, events(&run, t, 0, vec![sched(3, due)])).await,
        ApplyOutcome::Accepted
    );
    clock.advance(100);
    eventually(Duration::from_secs(2), "timer fired", || {
        !inbox_fired(h, &run).is_empty()
    })
    .await;
    let dup = Command::timer_fired(TimerFired {
        run_id: run.as_bytes().to_vec(),
        op: 3,
        due_at_ms: due,
        fired_at_ms: START + 999,
    });
    assert!(matches!(
        submit(h, dup).await,
        ApplyOutcome::Duplicate { .. }
    ));
    assert_eq!(inbox_fired(h, &run), vec![(3, START + 100)]);
    // Only the rolling hash / applied offset move; the tables are unchanged.
    let rec: RunRecord = h.read_run(&run).expect("read").expect("run");
    assert_eq!(rec.inbox_next, 1);
    node.shutdown().await.expect("shutdown");
}

/// Many timers share the due at the window cut-off: none is lost at the horizon.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timers_sharing_the_cutoff_due_all_fire() {
    let tmp = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(START);
    let stats = Arc::new(TimerStats::default());
    let cfg = TimerConfig {
        window_max: 10,
        window_span_ms: 60_000,
        fire_batch: 4,
        stats: Some(Arc::clone(&stats)),
    };
    let node = open(tmp.path(), &clock, cfg).await;
    let h = h(&node);
    let (run, t) = running(h).await;
    // 5 early timers, then 25 sharing one due: every scan of 10 is cut inside that due.
    let shared = START + 2_000;
    let mut d: Vec<Derived> = (0..5).map(|i| sched(i, START + 1_000 + i)).collect();
    d.extend((100..125).map(|op| sched(op, shared)));
    assert_eq!(
        submit(h, events(&run, t, 0, d)).await,
        ApplyOutcome::Accepted
    );
    clock.advance(1_000);
    eventually(Duration::from_secs(3), "early timer fired", || {
        !inbox_fired(h, &run).is_empty()
    })
    .await;
    clock.advance(1_000);
    eventually(Duration::from_secs(5), "all 30 fired", || {
        stats.fired.load(Ordering::Relaxed) == 30
    })
    .await;
    sleep(Duration::from_millis(200)).await;
    let mut ops: Vec<u64> = inbox_fired(h, &run).into_iter().map(|(op, _)| op).collect();
    ops.sort_unstable();
    let want: Vec<u64> = (0..5).chain(100..125).collect();
    assert_eq!(ops, want);
    assert_eq!(timer_rows(h), 0);
    assert!(stats.max_window_len.load(Ordering::Relaxed) <= 10);
    node.shutdown().await.expect("shutdown");
}

/// 100 000 timers over two hours with window_max = 1 000: the window never exceeds its bound
/// and every timer fires exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hundred_thousand_timers_bounded_window_large() {
    const N: u64 = 100_000;
    const PER_CMD: u64 = 1_000;
    const SPREAD_MS: u64 = 2 * 60 * 60 * 1_000;
    let tmp = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(START);
    let stats = Arc::new(TimerStats::default());
    let cfg = TimerConfig {
        window_max: 1_000,
        window_span_ms: 60_000,
        fire_batch: 256,
        stats: Some(Arc::clone(&stats)),
    };
    let node = open(tmp.path(), &clock, cfg).await;
    let h = h(&node);
    let (run, t) = running(h).await;
    let step = SPREAD_MS / N; // 72 ms
    let due_of = |i: u64| START + 1 + i * step;
    for c in 0..N / PER_CMD {
        let d = (c * PER_CMD..(c + 1) * PER_CMD)
            .map(|i| sched(i, due_of(i)))
            .collect();
        assert_eq!(
            submit(h, events(&run, t, c, d)).await,
            ApplyOutcome::Accepted
        );
    }
    assert_eq!(timer_rows(h), N as usize);
    assert!(stats.max_window_len.load(Ordering::Relaxed) <= 1_000);

    let mut now = START;
    while now < START + SPREAD_MS + 60_000 {
        now += 60_000;
        clock.set(now);
        let expect = (0..N).filter(|&i| due_of(i) <= now).count() as u64;
        eventually(Duration::from_secs(20), "step fired", || {
            stats.fired.load(Ordering::Relaxed) >= expect
        })
        .await;
        assert!(stats.max_window_len.load(Ordering::Relaxed) <= 1_000);
    }
    eventually(Duration::from_secs(20), "all fired", || {
        stats.fired.load(Ordering::Relaxed) == N
    })
    .await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(stats.fired.load(Ordering::Relaxed), N);
    assert!(stats.max_window_len.load(Ordering::Relaxed) <= 1_000);
    assert_eq!(timer_rows(h), 0);
    let fired = inbox_fired(h, &run);
    assert_eq!(fired.len() as u64, N);
    let mut ops: Vec<u64> = fired.into_iter().map(|(op, _)| op).collect();
    ops.sort_unstable();
    ops.dedup();
    assert_eq!(ops.len() as u64, N, "a timer fired twice");
    node.shutdown().await.expect("shutdown");
}
