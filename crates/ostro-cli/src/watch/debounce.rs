//! The debouncer of `ostro dev --watch` (D-P14-47): raw watcher events in, batches out.
//!
//! A batch is emitted `quiet` after the last event of a burst; a stream that never goes
//! quiet still flushes every `max_latency` (measured from the first event of the batch).
//! Paths are de-duplicated per batch and an empty batch is never emitted.
//!
//! The state machine ([`Debouncer`]) takes the current instant as an argument, so its
//! tests drive it with synthetic instants; [`spawn`] runs it on tokio time.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;

/// The default maximum latency of a batch under a continuous stream of events.
pub const MAX_LATENCY_MS: u64 = 2_000;

/// One raw event of a watcher backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// This path changed (created, written, removed, renamed or its metadata changed).
    Changed(PathBuf),
    /// Events may have been lost (inotify queue overflow): treat everything as changed.
    Rescan,
}

/// A debounced batch of changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
    /// The changed paths, de-duplicated.
    pub paths: BTreeSet<PathBuf>,
    /// `true` when the batch includes a [`Event::Rescan`]: anything may have changed.
    pub rescan: bool,
}

impl Batch {
    /// `true` when there is nothing in the batch.
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty() && !self.rescan
    }
}

/// The debouncing state machine: a quiet window plus a maximum latency.
#[derive(Debug)]
pub struct Debouncer {
    /// A batch is emitted this long after its last event.
    pub quiet: Duration,
    /// ... and at the latest this long after its first event.
    pub max_latency: Duration,
    pending: Batch,
    first: Option<Instant>,
    last: Option<Instant>,
}

impl Debouncer {
    /// A debouncer with the given windows (`max_latency` is raised to `quiet` if smaller).
    pub fn new(quiet: Duration, max_latency: Duration) -> Self {
        Debouncer {
            quiet,
            max_latency: max_latency.max(quiet),
            pending: Batch::default(),
            first: None,
            last: None,
        }
    }

    /// Record `ev`, observed at `now`.
    pub fn push(&mut self, ev: Event, now: Instant) {
        match ev {
            Event::Changed(p) => {
                self.pending.paths.insert(p);
            }
            Event::Rescan => self.pending.rescan = true,
        }
        self.first.get_or_insert(now);
        self.last = Some(now);
    }

    /// When the pending batch is due (`None` when nothing is pending).
    pub fn deadline(&self) -> Option<Instant> {
        let (first, last) = (self.first?, self.last?);
        Some((last + self.quiet).min(first + self.max_latency))
    }

    /// The pending batch if it is due at `now`.
    pub fn take_due(&mut self, now: Instant) -> Option<Batch> {
        if self.deadline()? > now {
            return None;
        }
        self.first = None;
        self.last = None;
        let batch = std::mem::take(&mut self.pending);
        (!batch.is_empty()).then_some(batch)
    }

    /// The pending batch, due or not (used when the event source ends).
    pub fn flush(&mut self) -> Option<Batch> {
        self.first = None;
        self.last = None;
        let batch = std::mem::take(&mut self.pending);
        (!batch.is_empty()).then_some(batch)
    }
}

/// Run `deb` on tokio time between `events` and `batches` until either side closes.
pub async fn run(
    mut deb: Debouncer,
    mut events: UnboundedReceiver<Event>,
    batches: UnboundedSender<Batch>,
) {
    loop {
        let ev = match deb.deadline() {
            Some(at) => tokio::select! {
                ev = events.recv() => ev,
                () = tokio::time::sleep_until(at) => {
                    if let Some(b) = deb.take_due(Instant::now())
                        && batches.send(b).is_err()
                    {
                        return;
                    }
                    continue;
                }
            },
            None => events.recv().await,
        };
        match ev {
            Some(ev) => deb.push(ev, Instant::now()),
            None => {
                if let Some(b) = deb.flush() {
                    let _ = batches.send(b);
                }
                return;
            }
        }
    }
}

/// Spawn [`run`] with the given windows; returns the event sender and the batch receiver.
pub fn spawn(
    quiet: Duration,
    max_latency: Duration,
) -> (UnboundedSender<Event>, UnboundedReceiver<Batch>) {
    let (etx, erx) = tokio::sync::mpsc::unbounded_channel();
    let (btx, brx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run(Debouncer::new(quiet, max_latency), erx, btx));
    (etx, brx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn changed(p: &str) -> Event {
        Event::Changed(PathBuf::from(p))
    }

    #[test]
    fn a_burst_gives_one_batch_quiet_after_the_last_event() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(ms(250), ms(2_000));
        assert_eq!(d.deadline(), None);
        assert_eq!(d.take_due(t0), None);
        for i in 0..5 {
            d.push(changed("/p/a.rs"), t0 + ms(40 * i));
        }
        d.push(changed("/p/b.rs"), t0 + ms(200));
        assert_eq!(d.deadline(), Some(t0 + ms(450)));
        assert_eq!(d.take_due(t0 + ms(449)), None);
        let b = d.take_due(t0 + ms(450)).unwrap();
        assert_eq!(b.paths.len(), 2, "de-duplicated: {b:?}");
        assert!(!b.rescan);
        assert_eq!(d.deadline(), None);
        assert_eq!(d.take_due(t0 + ms(10_000)), None, "never an empty batch");
    }

    #[test]
    fn a_continuous_stream_still_flushes_every_max_latency() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(ms(250), ms(2_000));
        let mut batches = Vec::new();
        // an event every 100 ms for 5 s: never quiet for 250 ms
        for i in 0..50u64 {
            let now = t0 + ms(100 * i);
            if let Some(b) = d.take_due(now) {
                batches.push((now, b));
            }
            d.push(changed(&format!("/p/f{}.rs", i % 3)), now);
        }
        assert_eq!(batches.len(), 2, "{batches:?}");
        assert_eq!(batches[0].0, t0 + ms(2_000));
        assert_eq!(batches[1].0, t0 + ms(4_000));
        assert!(batches.iter().all(|(_, b)| b.paths.len() == 3));
    }

    #[test]
    fn rescan_is_carried_and_max_latency_is_at_least_quiet() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(ms(250), ms(10));
        assert_eq!(d.max_latency, ms(250));
        d.push(Event::Rescan, t0);
        let b = d.take_due(t0 + ms(250)).unwrap();
        assert!(b.rescan && b.paths.is_empty());
        assert!(!b.is_empty());
        assert_eq!(d.flush(), None);
    }

    #[tokio::test]
    async fn the_driver_emits_batches_on_tokio_time_and_flushes_on_close() {
        let (tx, mut rx) = spawn(ms(50), ms(2_000));
        for _ in 0..20 {
            tx.send(changed("/p/a.rs")).unwrap();
        }
        let b = tokio::time::timeout(ms(5_000), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b.paths.len(), 1);
        tx.send(changed("/p/b.rs")).unwrap();
        drop(tx);
        let b = tokio::time::timeout(ms(5_000), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(b.paths.contains(&PathBuf::from("/p/b.rs")));
        assert_eq!(rx.recv().await, None);
    }
}
