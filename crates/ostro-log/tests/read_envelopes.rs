//! BACK-01: `ReplicatedLog::read_envelopes` returns the stored OSTR envelope bytes of whole
//! batches, HW-capped, grouped per epoch with every batch boundary preserved, and
//! byte-identical to `envelope::encode` of the commands `read` decodes.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use ostro_log::{
    EpochHistory, Isr, LogError, LogHealth, LogNode, LogNodeConfig, RawBatch, ReplicatedLog,
    Rf1Log, Role, TrimPermit, VerifiedBatch,
};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId, envelope};
use tokio::sync::watch;

async fn open(dir: &Path) -> (LogNode, Arc<Rf1Log>) {
    let cfg = LogNodeConfig::new(dir.join("wal"), PartitionCount::new(1).unwrap(), 1);
    let (node, mut logs) = LogNode::open(cfg, &[Offset::ZERO]).await.unwrap();
    (node, logs.remove(0).unwrap())
}

fn cmds(tag: &str, n: usize) -> Vec<Bytes> {
    (0..n)
        .map(|i| Bytes::from(format!("{tag}-{i}-{}", "z".repeat(i * 7))))
        .collect()
}

/// `(epoch, n_cmds)` per batch; returns the last offset.
async fn append_all(log: &Rf1Log, batches: &[(u32, usize)]) -> Offset {
    let mut last = Offset::ZERO;
    for (i, &(e, n)) in batches.iter().enumerate() {
        last = log
            .append(Epoch(e), cmds(&format!("b{i}"), n))
            .await
            .unwrap()
            .last;
    }
    log.wait_committed(last).await.unwrap();
    last
}

/// Every envelope of `raw` in order, checked for contiguity and grouping invariants.
fn flatten(raw: &[RawBatch]) -> Vec<Bytes> {
    let mut out = Vec::new();
    for (i, rb) in raw.iter().enumerate() {
        assert!(!rb.envelopes.is_empty());
        assert!(rb.base < rb.end);
        if i > 0 {
            assert_eq!(raw[i - 1].end, rb.base, "raw batches are contiguous");
            assert_ne!(
                raw[i - 1].epoch,
                rb.epoch,
                "adjacent raw batches differ in epoch"
            );
        }
        let mut at = rb.base;
        for env in &rb.envelopes {
            let v = envelope::verify(env).unwrap();
            assert_eq!(
                v.header.base, at,
                "envelopes are contiguous inside a raw batch"
            );
            assert_eq!(
                v.header.epoch,
                Epoch(rb.epoch),
                "envelope epoch == raw batch epoch"
            );
            at = v.last().next();
            out.push(env.clone());
        }
        assert_eq!(at, rb.end, "raw batch end == last envelope end");
    }
    out
}

fn reencode(p: PartitionId, batches: &[VerifiedBatch]) -> Vec<Bytes> {
    batches
        .iter()
        .map(|b| Bytes::from(envelope::encode(p, b.epoch, b.base, &b.commands)))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn byte_identical_to_encode_and_grouped_by_epoch() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path()).await;
    let layout = [(1, 3), (1, 1), (1, 4), (2, 2), (2, 5), (4, 1), (4, 2)];
    let last = append_all(&log, &layout).await;
    let hw = *log.high_watermark().borrow();
    assert_eq!(hw, last.next());

    let verified = log.read(Offset::ZERO, hw, usize::MAX).await.unwrap();
    assert_eq!(verified.len(), layout.len());
    let raw = log.read_envelopes(Offset::ZERO, usize::MAX).await.unwrap();

    // Byte identity with Envelope::encode of the decoded commands.
    assert_eq!(flatten(&raw), reencode(log.partition(), &verified));
    // One raw batch per epoch run, batch boundaries preserved across the epoch changes.
    let shape: Vec<(u32, u64, u64, usize)> = raw
        .iter()
        .map(|r| (r.epoch, r.base.0, r.end.0, r.envelopes.len()))
        .collect();
    assert_eq!(shape, vec![(1, 0, 8, 3), (2, 8, 15, 2), (4, 15, 18, 2)]);

    // Starting mid-log at a batch boundary inside an epoch run.
    let raw = log.read_envelopes(Offset(3), usize::MAX).await.unwrap();
    assert_eq!(flatten(&raw), reencode(log.partition(), &verified[1..]));
    assert_eq!((raw[0].base, raw[0].envelopes.len()), (Offset(3), 2));

    // `from` inside a batch is a caller error; `from` at HW reads nothing.
    assert!(matches!(
        log.read_envelopes(Offset(1), usize::MAX).await,
        Err(LogError::OutOfRange(Offset(1)))
    ));
    assert!(log.read_envelopes(hw, usize::MAX).await.unwrap().is_empty());

    // max_bytes: at least one whole batch, never a partial one.
    let one = log.read_envelopes(Offset::ZERO, 1).await.unwrap();
    assert_eq!(flatten(&one), reencode(log.partition(), &verified[..1]));

    // The default (re-encoding) implementation agrees byte for byte.
    let capped = CappedLog {
        inner: Arc::clone(&log),
        cap: Offset(u64::MAX),
    };
    assert_eq!(
        capped
            .read_envelopes(Offset::ZERO, usize::MAX)
            .await
            .unwrap(),
        log.read_envelopes(Offset::ZERO, usize::MAX).await.unwrap()
    );
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn respects_the_high_watermark() {
    let tmp = ostro_testkit::fs::tempdir();
    let (node, log) = open(tmp.path()).await;
    append_all(&log, &[(1, 2), (1, 3), (2, 1)]).await;

    // An HW below the LEO (here: a wrapper whose HW stops after the second batch) caps the
    // read at a batch boundary.
    let capped = CappedLog {
        inner: Arc::clone(&log),
        cap: Offset(5),
    };
    let raw = capped
        .read_envelopes(Offset::ZERO, usize::MAX)
        .await
        .unwrap();
    assert_eq!(raw.len(), 1);
    assert_eq!(
        (raw[0].base, raw[0].end, raw[0].envelopes.len()),
        (Offset(0), Offset(5), 2)
    );

    // Racing appends: nothing returned is ever beyond the HW observed after the read.
    let writer = {
        let log = Arc::clone(&log);
        tokio::spawn(async move {
            for i in 0..200 {
                log.append(Epoch(2 + i / 50), cmds("w", 1 + (i % 3) as usize))
                    .await
                    .unwrap();
            }
        })
    };
    let mut reads = 0;
    while !writer.is_finished() {
        let raw = log.read_envelopes(Offset::ZERO, usize::MAX).await.unwrap();
        let hw = *log.high_watermark().borrow();
        if let Some(r) = raw.last() {
            assert!(r.end <= hw, "read end {} beyond HW {hw}", r.end);
        }
        flatten(&raw);
        reads += 1;
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();
    assert!(reads > 0);
    node.shutdown().await;
}

/// Delegates to an `Rf1Log` but uses the trait's **default** `read_envelopes` and caps the
/// high watermark at `cap`.
struct CappedLog {
    inner: Arc<Rf1Log>,
    cap: Offset,
}

impl ReplicatedLog for CappedLog {
    fn partition(&self) -> PartitionId {
        self.inner.partition()
    }
    async fn append(
        &self,
        epoch: Epoch,
        cmds: Vec<Bytes>,
    ) -> Result<ostro_log::AppendReceipt, LogError> {
        self.inner.append(epoch, cmds).await
    }
    async fn wait_committed(&self, last: Offset) -> Result<(), LogError> {
        self.inner.wait_committed(last).await
    }
    fn high_watermark(&self) -> watch::Receiver<Offset> {
        self.inner.high_watermark()
    }
    fn log_end(&self) -> Offset {
        self.inner.log_end()
    }
    fn log_start(&self) -> Offset {
        self.inner.log_start()
    }
    async fn read(
        &self,
        from: Offset,
        upto: Offset,
        max_bytes: usize,
    ) -> Result<Vec<VerifiedBatch>, LogError> {
        self.inner.read(from, upto.min(self.cap), max_bytes).await
    }
    fn epochs(&self) -> EpochHistory {
        self.inner.epochs()
    }
    fn isr(&self) -> Isr {
        self.inner.isr()
    }
    fn role(&self) -> watch::Receiver<Role> {
        self.inner.role()
    }
    fn health(&self) -> watch::Receiver<LogHealth> {
        self.inner.health()
    }
    async fn truncate_to(&self, end: Offset) -> Result<Offset, LogError> {
        self.inner.truncate_to(end).await
    }
    async fn trim_prefix(&self, permit: TrimPermit) -> Result<Offset, LogError> {
        self.inner.trim_prefix(permit).await
    }
}
