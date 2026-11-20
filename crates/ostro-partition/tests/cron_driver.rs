//! Cron driver (plan 08-14, TRIG-02/TRIG-03): the owner partition's leader service turns
//! schedules into `ScheduleFire` commands on a `ManualClock`, catches up per policy after
//! downtime without enumerating the missed range, stays exactly-once across crash restarts
//! (WAL-only crash images, state rebuilt by replay), never backfills a re-created schedule
//! (W6), and only the owner partition ever submits.
#![allow(clippy::disallowed_methods)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{OPEN_T, T, copy_dir, wait_until};
use ostro_partition::leader::cron::{ScheduleSource, ScheduleSpec, schedule_state};
use ostro_partition::query::runs_page;
use ostro_partition::{ManualClock, Node, NodeConfig};
use ostro_types::routing::schedule_owner;
use ostro_types::{PartitionCount, PartitionId, RunId};
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

/// Partition count of every test.
const P: u16 = 4;
/// A 10 s aligned start instant (2027-01-15T08:00:00Z).
const T0: u64 = 1_800_000_000_000;
const OVERLAP_SKIP: u8 = 0;
const OVERLAP_ALLOW_ALL: u8 = 2;
const CATCHUP_SKIP: u8 = 0;
const CATCHUP_ONE: u8 = 1;
const CATCHUP_ALL: u8 = 2;

fn pc() -> PartitionCount {
    PartitionCount::new(P).expect("non-zero")
}

/// A schedule source the test edits directly.
struct TestSource {
    tx: watch::Sender<Arc<Vec<ScheduleSpec>>>,
}

impl TestSource {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            tx: watch::channel(Arc::new(Vec::new())).0,
        })
    }

    fn set(&self, specs: Vec<ScheduleSpec>) {
        self.tx.send_replace(Arc::new(specs));
    }
}

impl ScheduleSource for TestSource {
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<ScheduleSpec>>> {
        self.tx.subscribe()
    }
}

/// The first name `<prefix>-<i>` owned by partition `p`.
fn owned(prefix: &str, p: u16) -> String {
    (0..)
        .map(|i| format!("{prefix}-{i}"))
        .find(|n| schedule_owner(n, pc()) == PartitionId(p))
        .expect("some name hashes to p")
}

fn spec(name: &str, cron: &str, overlap: u8, catchup: u8, created_at_ms: u64) -> ScheduleSpec {
    ScheduleSpec {
        name: name.to_owned(),
        workflow: "cron-wf".to_owned(),
        version: Some(1),
        cron: cron.to_owned(),
        timezone: String::new(),
        input: b"\"tick\"".to_vec(),
        paused: false,
        overlap,
        catchup,
        catchup_window_ms: 0,
        start_at_ms: None,
        end_at_ms: None,
        created_at_ms,
        meta_version: 1,
    }
}

async fn open(dir: &Path, clock: &ManualClock, src: &Arc<TestSource>, cap: u32) -> Node {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), pc());
    cfg.clock = Arc::new(clock.clone());
    cfg.schedule_source = Some(Arc::clone(src) as Arc<dyn ScheduleSource>);
    cfg.cron.catchup_cap = cap;
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

/// Every cron (non-manual) run of `name` on partition `p`: `(fire_time, run)`, by fire time.
fn cron_runs(node: &Node, p: u16, name: &str) -> Vec<(u64, RunId)> {
    let h = node.handle(PartitionId(p)).expect("partition ready");
    let prefix = format!("__cron/{name}/");
    let mut out: Vec<(u64, RunId)> = runs_page(&h.reader(), None, usize::MAX)
        .expect("runs")
        .into_iter()
        .filter_map(|(id, rec)| {
            let key = String::from_utf8(rec.idempotency_key).ok()?;
            let ft = key.strip_prefix(&prefix)?.parse::<u64>().ok()?;
            Some((ft, id))
        })
        .collect();
    out.sort();
    out
}

fn all_runs(node: &Node, p: u16) -> usize {
    let h = node.handle(PartitionId(p)).expect("partition ready");
    runs_page(&h.reader(), None, usize::MAX)
        .expect("runs")
        .len()
}

fn watermark(node: &Node, p: u16, name: &str) -> Option<u64> {
    let h = node.handle(PartitionId(p)).expect("partition ready");
    schedule_state(&h.reader(), name)
        .expect("schedule_state")
        .map(|r| r.watermark_ms)
}

fn fire_times(node: &Node, p: u16, name: &str) -> Vec<u64> {
    cron_runs(node, p, name)
        .into_iter()
        .map(|(t, _)| t)
        .collect()
}

async fn wait_runs(node: &Node, p: u16, name: &str, n: usize) {
    wait_until(T, &format!("{n} cron runs of {name}"), || {
        cron_runs(node, p, name).len() >= n
    })
    .await;
}

/// A crash image: node.toml + the WAL only (fjall state is rebuilt by replay), taken while
/// the node runs.
fn crash_image(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    std::fs::copy(src.join("node.toml"), dst.join("node.toml")).expect("node.toml");
    copy_dir(&src.join("wal"), &dst.join("wal"));
}

/// Settle: the driver polls every 50 ms of real time; give it a few rounds.
async fn settle() {
    sleep(Duration::from_millis(400)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fires_on_each_boundary_with_one_run_each() {
    let d = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let name = owned("every10", 1);
    src.set(vec![spec(
        &name,
        "*/10 * * * * *",
        OVERLAP_ALLOW_ALL,
        CATCHUP_ONE,
        T0 + 1,
    )]);
    let node = open(d.path(), &clock, &src, 100).await;
    settle().await;
    assert!(cron_runs(&node, 1, &name).is_empty(), "nothing due yet");
    for i in 1..=5u64 {
        clock.set(T0 + 10_000 * i);
        wait_runs(&node, 1, &name, i as usize).await;
    }
    settle().await;
    assert_eq!(
        fire_times(&node, 1, &name),
        (1..=5).map(|i| T0 + 10_000 * i).collect::<Vec<_>>(),
        "one run per 10 s boundary"
    );
    assert_eq!(watermark(&node, 1, &name), Some(T0 + 50_000));
    // Between boundaries nothing fires.
    clock.set(T0 + 59_999);
    settle().await;
    assert_eq!(cron_runs(&node, 1, &name).len(), 5);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_restarts_stay_exactly_once_and_catch_up_one() {
    let root = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let name = owned("restart", 2);
    src.set(vec![spec(
        &name,
        "*/10 * * * * *",
        OVERLAP_ALLOW_ALL,
        CATCHUP_ONE,
        T0 + 1,
    )]);
    let mut dir = root.path().join("gen0");
    let mut node = open(&dir, &clock, &src, 100).await;
    settle().await;
    let mut expected = Vec::new();
    for round in 1..=4u64 {
        clock.set(T0 + 10_000 * round);
        expected.push(T0 + 10_000 * round);
        wait_runs(&node, 2, &name, expected.len()).await;
        // Crash image while running, then restart from it at the same instant.
        let next = root.path().join(format!("round{round}"));
        crash_image(&dir, &next);
        node.shutdown().await.expect("shutdown");
        dir = next;
        node = open(&dir, &clock, &src, 100).await;
        settle().await;
        assert_eq!(
            fire_times(&node, 2, &name),
            expected,
            "round {round}: every fire time has exactly one run"
        );
    }
    // 5 minutes (+3 s) of downtime, policy One: exactly one run, at the latest fire <= now.
    let next = root.path().join("down");
    crash_image(&dir, &next);
    node.shutdown().await.expect("shutdown");
    let now = T0 + 40_000 + 303_000;
    clock.set(now);
    let node = open(&next, &clock, &src, 100).await;
    let latest = now - now % 10_000;
    expected.push(latest);
    wait_runs(&node, 2, &name, expected.len()).await;
    settle().await;
    assert_eq!(fire_times(&node, 2, &name), expected);
    assert_eq!(watermark(&node, 2, &name), Some(latest));
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catch_up_skip_advances_the_watermark_without_runs() {
    let d = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let name = owned("skip", 0);
    src.set(vec![spec(
        &name,
        "*/10 * * * * *",
        OVERLAP_ALLOW_ALL,
        CATCHUP_SKIP,
        T0 + 1,
    )]);
    let node = open(d.path(), &clock, &src, 100).await;
    settle().await;
    clock.set(T0 + 10_000);
    wait_runs(&node, 0, &name, 1).await;
    node.shutdown().await.expect("shutdown");
    let now = T0 + 10_000 + 300_000 + 4_000;
    clock.set(now);
    let node = open(d.path(), &clock, &src, 100).await;
    let latest = now - now % 10_000;
    wait_until(T, "watermark advanced", || {
        watermark(&node, 0, &name) == Some(latest)
    })
    .await;
    settle().await;
    assert_eq!(
        fire_times(&node, 0, &name),
        vec![T0 + 10_000],
        "no catch-up run"
    );
    let h = node.handle(PartitionId(0)).expect("p0");
    let row = schedule_state(&h.reader(), &name)
        .expect("read")
        .expect("row");
    assert_eq!(row.fired, 1);
    assert!(
        row.skipped >= 30,
        "skipped counts the missed fires: {row:?}"
    );
    // The steady state resumes at the next boundary.
    clock.set(latest + 10_000);
    wait_runs(&node, 0, &name, 2).await;
    assert_eq!(
        fire_times(&node, 0, &name),
        vec![T0 + 10_000, latest + 10_000]
    );
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catch_up_all_keeps_the_newest_fires_within_window_and_cap() {
    let d = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let name = owned("all", 3);
    let mut s = spec(
        &name,
        "*/10 * * * * *",
        OVERLAP_ALLOW_ALL,
        CATCHUP_ALL,
        T0 + 1,
    );
    s.catchup_window_ms = 60_000;
    src.set(vec![s]);
    let node = open(d.path(), &clock, &src, 3).await;
    settle().await;
    node.shutdown().await.expect("shutdown");
    // 5 minutes of downtime: 30 missed fires, 6 inside the window, the newest 3 run.
    let now = T0 + 300_000 + 5_000;
    clock.set(now);
    let node = open(d.path(), &clock, &src, 3).await;
    let latest = now - now % 10_000;
    wait_runs(&node, 3, &name, 3).await;
    settle().await;
    assert_eq!(
        fire_times(&node, 3, &name),
        vec![latest - 20_000, latest - 10_000, latest],
        "the 3 newest fire times <= now"
    );
    assert_eq!(watermark(&node, 3, &name), Some(latest));
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_day_downtime_policy_one_is_bounded_work() {
    let d = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let name = owned("everysec", 1);
    src.set(vec![spec(
        &name,
        "* * * * * *",
        OVERLAP_ALLOW_ALL,
        CATCHUP_ONE,
        T0 + 1,
    )]);
    let node = open(d.path(), &clock, &src, 100).await;
    settle().await;
    clock.set(T0 + 1_000);
    wait_runs(&node, 1, &name, 1).await;
    node.shutdown().await.expect("shutdown");
    // 2 days = 172,800 missed fires.
    let now = T0 + 1_000 + 2 * 86_400_000 + 500;
    clock.set(now);
    let t = Instant::now(); // clock-exempt: measures the catch-up cost (B5)
    let node = open(d.path(), &clock, &src, 100).await;
    wait_runs(&node, 1, &name, 2).await;
    let took = t.elapsed();
    settle().await;
    let latest = now - now % 1_000;
    assert_eq!(fire_times(&node, 1, &name), vec![T0 + 1_000, latest]);
    assert_eq!(watermark(&node, 1, &name), Some(latest));
    // Enumerating 172,800 fires at ~4 us each per croner call would take seconds; the
    // bounded path (latest_at_or_before + a 256-fire skip estimate) does not.
    assert!(took < Duration::from_secs(60), "catch-up took {took:?}");
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recreated_schedule_never_backfills_from_an_orphaned_watermark() {
    let d = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let name = owned("recreate", 2);
    src.set(vec![spec(
        &name,
        "*/10 * * * * *",
        OVERLAP_ALLOW_ALL,
        CATCHUP_ALL,
        T0 + 1,
    )]);
    let node = open(d.path(), &clock, &src, 100).await;
    settle().await;
    clock.set(T0 + 10_000);
    wait_runs(&node, 2, &name, 1).await;
    // Delete; the schedule_state row stays (orphaned).
    src.set(vec![]);
    settle().await;
    clock.set(T0 + 10_000 + 300_000 + 2_000);
    settle().await;
    assert_eq!(cron_runs(&node, 2, &name).len(), 1, "deleted: no fires");
    // Re-create with the same name: a new incarnation created now.
    let created = clock_now(&clock);
    src.set(vec![spec(
        &name,
        "*/10 * * * * *",
        OVERLAP_ALLOW_ALL,
        CATCHUP_ALL,
        created,
    )]);
    settle().await;
    assert_eq!(
        fire_times(&node, 2, &name),
        vec![T0 + 10_000],
        "no backfill of the 30 fires since the old watermark"
    );
    // It fires normally from its creation on, and the row belongs to the new incarnation.
    let next = created - created % 10_000 + 10_000;
    clock.set(next);
    wait_runs(&node, 2, &name, 2).await;
    assert_eq!(fire_times(&node, 2, &name), vec![T0 + 10_000, next]);
    let h = node.handle(PartitionId(2)).expect("p2");
    let row = schedule_state(&h.reader(), &name)
        .expect("read")
        .expect("row");
    assert_eq!(row.schedule_created_at_ms, created);
    assert_eq!(row.watermark_ms, next);
    node.shutdown().await.expect("shutdown");
}

fn clock_now(c: &ManualClock) -> u64 {
    use ostro_partition::Clock;
    c.now_ms()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_the_owner_partition_submits() {
    let d = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let names: Vec<String> = (0..P).map(|p| owned("own", p)).collect();
    src.set(
        names
            .iter()
            .map(|n| spec(n, "*/10 * * * * *", OVERLAP_ALLOW_ALL, CATCHUP_ONE, T0 + 1))
            .collect(),
    );
    let node = open(d.path(), &clock, &src, 100).await;
    settle().await;
    clock.set(T0 + 10_000);
    for (p, n) in names.iter().enumerate() {
        wait_runs(&node, p as u16, n, 1).await;
    }
    settle().await;
    for (p, n) in names.iter().enumerate() {
        let p = p as u16;
        assert_eq!(
            all_runs(&node, p),
            1,
            "partition {p} holds only its own run"
        );
        assert_eq!(fire_times(&node, p, n), vec![T0 + 10_000]);
        for (q, other) in names.iter().enumerate() {
            if q as u16 != p {
                assert_eq!(
                    watermark(&node, p, other),
                    None,
                    "partition {p} never applied a fire of {other}"
                );
            }
        }
    }
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_keeps_the_watermark_and_resume_catches_up() {
    let d = ostro_testkit::fs::tempdir();
    let clock = ManualClock::new(T0 + 1);
    let src = TestSource::new();
    let name = owned("pause", 0);
    let mut s = spec(&name, "*/10 * * * * *", OVERLAP_SKIP, CATCHUP_ONE, T0 + 1);
    src.set(vec![s.clone()]);
    let node = open(d.path(), &clock, &src, 100).await;
    settle().await;
    clock.set(T0 + 10_000);
    wait_runs(&node, 0, &name, 1).await;
    s.paused = true;
    src.set(vec![s.clone()]);
    settle().await;
    clock.set(T0 + 60_000);
    settle().await;
    assert_eq!(fire_times(&node, 0, &name), vec![T0 + 10_000], "paused");
    assert_eq!(watermark(&node, 0, &name), Some(T0 + 10_000));
    clock.set(T0 + 65_000);
    s.paused = false;
    src.set(vec![s]);
    // Resume: catch-up One fires the latest missed fire (the overlap is Skip and the first
    // run never ends here, so the fire is accepted as an overlap skip: watermark moves, no run).
    wait_until(T, "resume catch-up", || {
        watermark(&node, 0, &name) == Some(T0 + 60_000)
    })
    .await;
    assert_eq!(fire_times(&node, 0, &name), vec![T0 + 10_000]);
    node.shutdown().await.expect("shutdown");
}
