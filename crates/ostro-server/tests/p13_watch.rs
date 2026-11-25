//! 13-13 Task 2: per-term `RunWatchers` (in process, fake led set and apply machines).
//!
//! - `waiter_woken_on_demotion`: losing the partition wakes its waiters with
//!   `LeaderChanged` (and leaves no registration behind).
//! - `resubscribe_after_new_term_delivers_completions`: a new term on a new machine is
//!   followed; its completions wake waiters.
//! - `new_epoch_here_wakes_with_retry`: a new epoch on the same node re-reads, no re-route.
//! - `no_waiter_sleeps_past_its_deadline`: a waiter nobody wakes returns at its deadline.
//! - `constant_leadership_is_unchanged` (RF=1 single node): completions, `Lagged` and
//!   explicit wakes behave as before; no `LeaderChanged` ever.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_partition::Action;
use ostro_partition::store::values;
use ostro_server::control::watch::{ActionSource, RunWatchers, Subscription, WaitOutcome};
use ostro_types::{Epoch, PartitionId, RunId};
use tokio::sync::{broadcast, watch};

type Batch = Arc<Vec<Action>>;
type Led = Arc<Vec<(PartitionId, Epoch)>>;

const P: PartitionId = PartitionId(1);
const T: Duration = Duration::from_secs(5);

fn run() -> RunId {
    RunId::new(P, uuid::Uuid::now_v7())
}

fn completed(r: RunId) -> Batch {
    Arc::new(vec![Action::RunCompleted {
        run: r,
        status: values::RunStatus::Completed,
    }])
}

/// The partition's current apply machine (replaceable: a new machine per restart).
struct Machines(Mutex<Option<broadcast::Sender<Batch>>>);

impl Machines {
    fn new() -> (Arc<Self>, broadcast::Sender<Batch>) {
        let tx = broadcast::channel(16).0;
        (Arc::new(Self(Mutex::new(Some(tx.clone())))), tx)
    }

    /// Replace the machine; returns the new sender.
    fn replace(&self) -> broadcast::Sender<Batch> {
        let tx = broadcast::channel(16).0;
        *self.0.lock().unwrap() = Some(tx.clone());
        tx
    }
}

impl ActionSource for Machines {
    fn subscribe(&self, _p: PartitionId) -> Subscription {
        match &*self.0.lock().unwrap() {
            Some(tx) => Subscription::Rx(tx.subscribe()),
            None => Subscription::NotRunning,
        }
    }
}

fn led(epoch: Option<u32>) -> Led {
    Arc::new(epoch.map(|e| vec![(P, Epoch(e))]).unwrap_or_default())
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn waiter_woken_on_demotion() {
    let w = RunWatchers::new(2);
    let (led_tx, led_rx) = watch::channel(led(Some(3)));
    let (src, _tx) = Machines::new();
    w.follow(P, Some(led_rx), src);
    let r = run();
    let mut a = w.register(r);
    let mut b = w.register(run());
    settle().await;
    led_tx.send_replace(led(None));
    assert_eq!(
        tokio::time::timeout(T, a.notified()).await.expect("woken"),
        WaitOutcome::LeaderChanged
    );
    assert_eq!(
        tokio::time::timeout(T, b.notified()).await.expect("woken"),
        WaitOutcome::LeaderChanged
    );
    drop((a, b));
    assert_eq!(w.waiters(), 0, "nothing left behind");
    // Another partition's waiters are untouched.
    let mut other = w.register(RunId::new(PartitionId(0), uuid::Uuid::now_v7()));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), other.notified())
            .await
            .is_err()
    );
    w.stop();
}

#[tokio::test]
async fn resubscribe_after_new_term_delivers_completions() {
    let w = RunWatchers::new(2);
    let (led_tx, led_rx) = watch::channel(led(Some(1)));
    let (src, old) = Machines::new();
    w.follow(P, Some(led_rx), Arc::clone(&src) as Arc<dyn ActionSource>);
    settle().await;
    // Demoted, then the partition restarts on a new machine and leads again at epoch 4.
    led_tx.send_replace(led(None));
    settle().await;
    let new = src.replace();
    drop(old);
    led_tx.send_replace(led(Some(4)));
    settle().await;
    let r = run();
    let mut a = w.register(r);
    new.send(completed(r))
        .expect("subscribed to the new machine");
    assert_eq!(
        tokio::time::timeout(T, a.notified()).await.expect("woken"),
        WaitOutcome::Completed
    );
    w.stop();
}

#[tokio::test]
async fn completions_queued_before_the_switch_are_delivered() {
    // A completion sent on the old receiver just before the term change is still handed
    // to its waiter (the old receiver is drained before it is dropped).
    let w = RunWatchers::new(2);
    let (led_tx, led_rx) = watch::channel(led(Some(1)));
    let (src, tx) = Machines::new();
    w.follow(P, Some(led_rx), src);
    settle().await;
    let r = run();
    let mut a = w.register(r);
    tx.send(completed(r)).unwrap();
    led_tx.send_replace(led(Some(2)));
    let got = tokio::time::timeout(T, a.notified()).await.expect("woken");
    assert!(
        matches!(got, WaitOutcome::Completed | WaitOutcome::Retry),
        "{got:?}"
    );
    w.stop();
}

#[tokio::test]
async fn new_epoch_here_wakes_with_retry() {
    let w = RunWatchers::new(2);
    let (led_tx, led_rx) = watch::channel(led(Some(1)));
    let (src, tx) = Machines::new();
    w.follow(P, Some(led_rx), src);
    settle().await;
    let mut a = w.register(run());
    led_tx.send_replace(led(Some(2)));
    assert_eq!(
        tokio::time::timeout(T, a.notified()).await.expect("woken"),
        WaitOutcome::Retry
    );
    // Completions keep flowing in the new term.
    let r = run();
    let mut b = w.register(r);
    tx.send(completed(r)).unwrap();
    assert_eq!(
        tokio::time::timeout(T, b.notified()).await.expect("woken"),
        WaitOutcome::Completed
    );
    w.stop();
}

#[tokio::test]
async fn no_waiter_sleeps_past_its_deadline() {
    let w = RunWatchers::new(2);
    let (_led_tx, led_rx) = watch::channel(led(Some(1)));
    let (src, _tx) = Machines::new();
    w.follow(P, Some(led_rx), src);
    let mut a = w.register(run());
    let t0 = tokio::time::Instant::now();
    let deadline = t0 + Duration::from_millis(200);
    let r = tokio::time::timeout_at(deadline, a.notified()).await;
    assert!(r.is_err());
    let late = tokio::time::Instant::now() - deadline;
    assert!(
        late < Duration::from_millis(100),
        "{late:?} past the deadline"
    );
    drop(a);
    assert_eq!(w.waiters(), 0);
    w.stop();
}

#[tokio::test]
async fn constant_leadership_is_unchanged() {
    // RF=1 single node: always led at one epoch.
    let w = RunWatchers::new(2);
    let (_led_tx, led_rx) = watch::channel(led(Some(1)));
    let (src, tx) = Machines::new();
    w.follow(P, Some(led_rx), src);
    let r = run();
    let mut a = w.register(r);
    tx.send(completed(r)).unwrap();
    assert_eq!(
        tokio::time::timeout(T, a.notified()).await.unwrap(),
        WaitOutcome::Completed
    );
    // Lagged wakes the whole partition with Retry.
    let mut b = w.register(run());
    for _ in 0..40 {
        tx.send(Arc::new(Vec::new())).unwrap();
    }
    assert_eq!(
        tokio::time::timeout(T, b.notified()).await.unwrap(),
        WaitOutcome::Retry
    );
    // An explicit wake (dev time skipping, 10-13) is a Retry, never a re-route.
    let mut c = w.register(run());
    w.wake_partition(P);
    assert_eq!(
        tokio::time::timeout(T, c.notified()).await.unwrap(),
        WaitOutcome::Retry
    );
    drop(tx);
    w.stop();
}
