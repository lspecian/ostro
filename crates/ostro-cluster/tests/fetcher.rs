//! Follower fetch loop (12-11 Task 2): a follower `PartitionLog` driven by `FetcherDriver`
//! against a leader `PartitionLog` served over loopback mTLS. Convergence, HW bounds,
//! divergence truncation (KIP-279), blobs before durability (F11), reconnects, verification.

mod support_fetch;
mod support_meta;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ostro_blob::BlobStore;
use ostro_cluster::blob::{self, has_blob};
use ostro_cluster::fetch::{FetchConfig, FetchCtx, fetch_decode_limit};
use ostro_cluster::fetcher::{FetcherConfig, FetcherDriver, FetcherHooks, FetcherStats};
use ostro_cluster::install::InstallCtx;
use ostro_cluster::transport::ClusterTransport;
use ostro_log::{ReplicaSet, ReplicatedLog};
use ostro_partition::SystemClock;
use ostro_partition::roles::{FollowerDriver, FollowerTask};
use ostro_types::{Epoch, Offset};
use support_fetch::*;
use support_meta::TestCa;

fn set(replicas: &[u64], isr: &[u64], min_insync: u8) -> ReplicaSet {
    ReplicaSet {
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        isr_version: 0,
        min_insync,
    }
}

fn limit() -> usize {
    fetch_decode_limit(
        ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES,
        FetcherConfig::default().max_bytes,
    )
}

fn fetcher_cfg() -> FetcherConfig {
    FetcherConfig {
        max_wait: Duration::from_millis(200),
        ..FetcherConfig::default()
    }
}

/// A served leader (node 1) and an unstarted follower (node 2).
struct Pair {
    ca: TestCa,
    leader: LogFx,
    follower: LogFx,
    srv: Option<ReplServer>,
    _a: Assignments,
    suspects: Arc<Suspects>,
}

impl Pair {
    async fn new(leader: LogFx, follower: LogFx, epoch: u32) -> Pair {
        let ca = test_ca();
        let a = assignments(view(1, epoch, &[1, 2], &[1]));
        let ctx = Arc::new(FetchCtx::new(
            Arc::clone(&leader.log),
            a.subscribe(),
            Arc::new(SystemClock),
            FetchConfig {
                max_wait: Duration::from_millis(200),
                ..FetchConfig::default()
            },
        ));
        let srv = ReplServer::start(&ca, 1, Some(ctx), Arc::clone(&leader.blobs), limit()).await;
        Pair {
            ca,
            leader,
            follower,
            srv: Some(srv),
            _a: a,
            suspects: Arc::new(Suspects::default()),
        }
    }

    fn addr(&self) -> std::net::SocketAddr {
        self.srv.as_ref().unwrap().addr
    }

    fn driver(&self, hooks: FetcherHooks) -> FetcherDriver {
        FetcherDriver::new(
            2,
            node_transport(&self.ca, 2, 1, self.addr()),
            Arc::clone(&self.follower.blobs),
            self.suspects.clone(),
            Arc::new(InstallCtx::default()),
            fetcher_cfg(),
        )
        .with_hooks(hooks)
    }

    fn start(&self, driver: &FetcherDriver, epoch: u32) -> Box<dyn FollowerTask> {
        driver.start(P, Epoch(epoch), 1, Arc::clone(&self.follower.log))
    }

    async fn converged(&self, within: Duration) {
        let (l, f) = (&self.leader.log, &self.follower.log);
        wait_until("follower caught up", within, || {
            f.log_end() == l.log_end() && f.durable_end() == l.log_end()
        })
        .await;
        assert_eq!(
            envelopes(f, f.log_start()).await,
            envelopes(l, l.log_start()).await,
            "byte-identical logs"
        );
    }

    async fn shutdown(mut self) {
        if let Some(s) = self.srv.take() {
            s.stop().await;
        }
        self.leader.shutdown().await;
        self.follower.shutdown().await;
    }
}

/// Samples HW bounds until stopped: follower HW <= leader HW and <= follower durable end.
/// Takes at least [`MIN_HW_SAMPLES`] samples even when stopped earlier: on the
/// current-thread test runtime the watcher only runs when the test task yields, and a fast
/// runner can converge in fewer polls than that (CI linux-x86_64-musl saw <= 10).
const MIN_HW_SAMPLES: usize = 11;

fn hw_watcher(p: &Pair) -> (Arc<AtomicBool>, tokio::task::JoinHandle<usize>) {
    let stop = Arc::new(AtomicBool::new(false));
    let (l, f) = (Arc::clone(&p.leader.log), Arc::clone(&p.follower.log));
    let s = Arc::clone(&stop);
    let h = tokio::spawn(async move {
        let mut samples = 0;
        while !s.load(Ordering::SeqCst) || samples < MIN_HW_SAMPLES {
            let fhw = *f.high_watermark().borrow();
            let fd = f.durable_end();
            let lhw = *l.high_watermark().borrow();
            assert!(fhw <= lhw, "follower HW {fhw} > leader HW {lhw}");
            assert!(fhw <= fd, "follower HW {fhw} > own durable end {fd}");
            samples += 1;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        samples
    });
    (stop, h)
}

#[tokio::test]
async fn follower_converges_byte_identical_with_bounded_hw() {
    let leader = LogFx::open(1).await;
    leader
        .log
        .become_leader(Epoch(1), set(&[1, 2], &[1], 1))
        .unwrap();
    for i in 0..10u64 {
        let cmds = (0..=i % 3).map(|j| cmd(i * 10 + j)).collect();
        append(&leader.log, Epoch(1), cmds).await;
    }
    let follower = LogFx::open(2).await;
    follower.log.become_follower(Epoch(1), 1).unwrap();
    let p = Pair::new(leader, follower, 1).await;
    let (stop, watcher) = hw_watcher(&p);
    let driver = p.driver(FetcherHooks::default());
    let task = p.start(&driver, 1);
    for i in 10..30u64 {
        append(&p.leader.log, Epoch(1), vec![cmd(i * 10)]).await;
    }
    p.converged(Duration::from_secs(20)).await;
    let lhw = *p.leader.log.high_watermark().borrow();
    let f = Arc::clone(&p.follower.log);
    wait_until(
        "follower adopts the leader HW",
        Duration::from_secs(5),
        || *f.high_watermark().borrow() == lhw,
    )
    .await;
    stop.store(true, Ordering::SeqCst);
    assert!(watcher.await.unwrap() > 10);
    // The leader measured the follower's durable progress.
    let row = p.leader.log.replica_status().followers;
    assert_eq!(row[0].replica, 2);
    assert_eq!(row[0].durable_leo, p.leader.log.log_end());
    task.stop().await;
    p.shutdown().await;
}

#[tokio::test]
async fn catch_up_rate() {
    // Measured for the SUMMARY: an empty follower catching up 2,000 single-command batches.
    let leader = LogFx::open(1).await;
    leader
        .log
        .become_leader(Epoch(1), set(&[1, 2], &[1], 1))
        .unwrap();
    let n = 2000u64;
    let mut last = Offset(0);
    for i in 0..n {
        last = leader
            .log
            .append(Epoch(1), vec![cmd(i)])
            .await
            .unwrap()
            .last;
    }
    wait_durable(&leader.log, last.next()).await;
    let follower = LogFx::open(2).await;
    follower.log.become_follower(Epoch(1), 1).unwrap();
    let p = Pair::new(leader, follower, 1).await;
    let driver = p.driver(FetcherHooks::default());
    let started = Instant::now();
    let task = p.start(&driver, 1);
    p.converged(Duration::from_secs(60)).await;
    let took = started.elapsed();
    let stats = driver.stats();
    eprintln!(
        "catch-up: {n} batches in {took:?} ({:.0} batches/s, {} fetches)",
        n as f64 / took.as_secs_f64(),
        stats.fetches.load(Ordering::Relaxed)
    );
    assert!(stats.fetches.load(Ordering::Relaxed) < n, "batched fetches");
    task.stop().await;
    p.shutdown().await;
}

/// Follower node 2 holding `own` epochs (each `(epoch, cmds-per-batch ids)`) that never
/// committed (ISR [2, 9], min_insync 2, so its HW stays 0).
async fn divergent_follower(own: &[(u32, &[u64])]) -> LogFx {
    let f = LogFx::open(2).await;
    for (e, ids) in own {
        f.log
            .become_leader(Epoch(*e), set(&[2, 9], &[2, 9], 2))
            .unwrap();
        for i in *ids {
            append(&f.log, Epoch(*e), vec![cmd(*i)]).await;
        }
    }
    assert_eq!(*f.log.high_watermark().borrow(), Offset(0));
    f
}

async fn leader_with(epochs: &[(u32, &[u64])]) -> LogFx {
    let l = LogFx::open(1).await;
    for (e, ids) in epochs {
        l.log
            .become_leader(Epoch(*e), set(&[1, 2], &[1], 1))
            .unwrap();
        for i in *ids {
            append(&l.log, Epoch(*e), vec![cmd(*i)]).await;
        }
    }
    l
}

#[tokio::test]
async fn divergent_tail_is_truncated_then_identical() {
    // Common prefix a, b at epoch 1; the follower also holds x, y at epoch 1, the leader
    // wrote c, d at epoch 2.
    let follower = divergent_follower(&[(1, &[0, 1, 100, 101])]).await;
    let leader = leader_with(&[(1, &[0, 1]), (2, &[2, 3])]).await;
    follower.log.become_follower(Epoch(2), 1).unwrap();
    let p = Pair::new(leader, follower, 2).await;
    let driver = p.driver(FetcherHooks::default());
    let task = p.start(&driver, 2);
    let st = driver.stats();
    wait_until("truncated", Duration::from_secs(10), || {
        st.truncations.load(Ordering::Relaxed) >= 1
    })
    .await;
    p.converged(Duration::from_secs(20)).await;
    assert_eq!(driver.stats().truncations.load(Ordering::Relaxed), 1);
    task.stop().await;
    p.shutdown().await;
}

#[tokio::test]
async fn two_divergent_epochs_converge_kip279() {
    // Follower: epoch 1 a, b, x; epoch 3 y, z. Leader: epoch 1 a, b; epoch 2 c, d, e;
    // epoch 4 f. First answer Diverging{2, 5} → truncate to 3 (own end of epoch <= 2);
    // second Diverging{1, 2} → truncate to 2; then Ok.
    let follower = divergent_follower(&[(1, &[0, 1, 100]), (3, &[101, 102])]).await;
    let leader = leader_with(&[(1, &[0, 1]), (2, &[2, 3, 4]), (4, &[5])]).await;
    follower.log.become_follower(Epoch(4), 1).unwrap();
    let p = Pair::new(leader, follower, 4).await;
    let driver = p.driver(FetcherHooks::default());
    let task = p.start(&driver, 4);
    let st = driver.stats();
    wait_until("truncated twice", Duration::from_secs(10), || {
        st.truncations.load(Ordering::Relaxed) >= 2
    })
    .await;
    p.converged(Duration::from_secs(20)).await;
    assert_eq!(driver.stats().truncations.load(Ordering::Relaxed), 2);
    assert_eq!(p.follower.log.epochs(), p.leader.log.epochs());
    task.stop().await;
    p.shutdown().await;
}

#[tokio::test]
async fn blob_is_local_before_the_command_is_durable_f11() {
    let leader = LogFx::open(1).await;
    leader
        .log
        .become_leader(Epoch(1), set(&[1, 2], &[1], 1))
        .unwrap();
    let blob_bytes: Vec<u8> = (0..(1u32 << 20)).map(|i| (i % 251) as u8).collect();
    let r = leader.blobs.put(&blob_bytes).unwrap();
    append(&leader.log, Epoch(1), vec![cmd(0)]).await; // offset 0
    append(&leader.log, Epoch(1), vec![blob_cmd(1, r.sha256, r.len)]).await; // offset 1
    append(&leader.log, Epoch(1), vec![cmd(2)]).await; // offset 2
    let follower = LogFx::open(2).await;
    follower.log.become_follower(Epoch(1), 1).unwrap();
    let p = Pair::new(leader, follower, 1).await;

    // Record every durable end the follower reaches together with "blob present".
    let samples: Arc<Mutex<Vec<(Offset, bool)>>> = Arc::default();
    let (fstore, flog, s) = (
        Arc::clone(&p.follower.blobs),
        Arc::clone(&p.follower.log),
        Arc::clone(&samples),
    );
    let sha = r.sha256;
    let watcher = tokio::spawn(async move {
        let mut rx = flog.durable_end_watch();
        loop {
            let d = *rx.borrow_and_update();
            s.lock().unwrap().push((d, has_blob(&fstore, &sha)));
            if d >= Offset(3) || rx.changed().await.is_err() {
                return;
            }
        }
    });
    let delayed = Arc::new(AtomicUsize::new(0));
    let dc = Arc::clone(&delayed);
    let hooks = FetcherHooks {
        before_blob_fetch: Some(Arc::new(move |_sha| {
            dc.fetch_add(1, Ordering::SeqCst);
            Box::pin(tokio::time::sleep(Duration::from_millis(400)))
        })),
        ..FetcherHooks::default()
    };
    let driver = p.driver(hooks);
    let task = p.start(&driver, 1);
    p.converged(Duration::from_secs(20)).await;
    watcher.await.unwrap();
    assert_eq!(delayed.load(Ordering::SeqCst), 1);
    assert_eq!(driver.stats().blobs_fetched.load(Ordering::Relaxed), 1);
    let samples = samples.lock().unwrap().clone();
    assert!(samples.iter().any(|(d, _)| *d >= Offset(3)), "{samples:?}");
    for (d, present) in &samples {
        assert!(
            *d <= Offset(1) || *present,
            "durable end {d} passed the blob command before the blob was local: {samples:?}"
        );
    }
    assert_eq!(p.follower.blobs.get(&r).unwrap(), blob_bytes);
    task.stop().await;
    p.shutdown().await;
}

#[tokio::test]
async fn push_and_fetch_blob_rpcs() {
    let leader = LogFx::open(1).await;
    let follower = LogFx::open(2).await;
    let p = Pair::new(leader, follower, 1).await;
    // Push a 2.5 MiB blob (3 chunks) from node 2's store to node 1.
    let bytes: Vec<u8> = (0..(5u32 << 19)).map(|i| (i % 253) as u8).collect();
    let r = p.follower.blobs.put(&bytes).unwrap();
    let t = node_transport(&p.ca, 2, 1, p.addr());
    let out = blob::push_to(&*t, &[1], &p.follower.blobs, r.sha256).await;
    assert!(matches!(out[0], (1, Ok(true))), "{:?}", out[0].1);
    let out = blob::push_to(&*t, &[1], &p.follower.blobs, r.sha256).await;
    assert!(matches!(out[0], (1, Ok(false))), "already held");
    assert_eq!(p.leader.blobs.get(&r).unwrap(), bytes);
    // Fetch it back into a fresh store; an unknown sha is NOT_FOUND.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BlobStore::open(dir.path()).unwrap());
    blob::fetch_blob_into(t.channel(1).unwrap(), &store, r.sha256)
        .await
        .unwrap();
    assert_eq!(store.get(&r).unwrap(), bytes);
    let e = blob::fetch_blob_into(t.channel(1).unwrap(), &store, [9; 32])
        .await
        .unwrap_err();
    assert!(matches!(e, blob::BlobTransferError::NotFound(_)), "{e}");
    p.shutdown().await;
}

#[tokio::test]
async fn fetcher_reconnects_after_a_leader_restart() {
    let leader = LogFx::open(1).await;
    leader
        .log
        .become_leader(Epoch(1), set(&[1, 2], &[1], 1))
        .unwrap();
    append(&leader.log, Epoch(1), vec![cmd(0)]).await;
    let follower = LogFx::open(2).await;
    follower.log.become_follower(Epoch(1), 1).unwrap();
    let mut p = Pair::new(leader, follower, 1).await;
    let driver = p.driver(FetcherHooks::default());
    let task = p.start(&driver, 1);
    p.converged(Duration::from_secs(10)).await;

    let (addr, svc) = p.srv.take().unwrap().stop().await;
    for i in 1..5u64 {
        append(&p.leader.log, Epoch(1), vec![cmd(i)]).await;
    }
    let s = Arc::clone(&p.suspects);
    wait_until("suspect reported", Duration::from_secs(10), || {
        !s.0.lock().unwrap().is_empty()
    })
    .await;
    assert_eq!(s.0.lock().unwrap()[0], (2, 1, Some(0)));
    p.srv = Some(ReplServer::restart(&p.ca, 1, addr, svc, limit()).await);
    p.converged(Duration::from_secs(20)).await;
    assert!(driver.stats().transport_errors.load(Ordering::Relaxed) > 0);
    task.stop().await;
    p.shutdown().await;
}

#[tokio::test]
async fn corrupted_envelope_is_rejected_and_retried() {
    let leader = LogFx::open(1).await;
    leader
        .log
        .become_leader(Epoch(1), set(&[1, 2], &[1], 1))
        .unwrap();
    for i in 0..5u64 {
        append(&leader.log, Epoch(1), vec![cmd(i)]).await;
    }
    let follower = LogFx::open(2).await;
    follower.log.become_follower(Epoch(1), 1).unwrap();
    let p = Pair::new(leader, follower, 1).await;
    let corrupted = Arc::new(AtomicBool::new(false));
    let leo_after: Arc<Mutex<Option<Offset>>> = Arc::default();
    let (c, la, flog) = (
        Arc::clone(&corrupted),
        Arc::clone(&leo_after),
        Arc::clone(&p.follower.log),
    );
    let hooks = FetcherHooks {
        on_response: Some(Arc::new(move |resp| {
            if resp.envelopes.is_empty() {
                return;
            }
            if !c.swap(true, Ordering::SeqCst) {
                // Flip one byte of the first command body: the CRC no longer matches.
                let env = &mut resp.envelopes[0];
                let i = env.len() - 1;
                env[i] ^= 0xff;
            } else {
                la.lock().unwrap().get_or_insert(flog.log_end());
            }
        })),
        ..FetcherHooks::default()
    };
    let driver = p.driver(hooks);
    let task = p.start(&driver, 1);
    p.converged(Duration::from_secs(20)).await;
    assert!(corrupted.load(Ordering::SeqCst));
    assert_eq!(
        *leo_after.lock().unwrap(),
        Some(Offset(0)),
        "nothing of the rejected response was appended"
    );
    let stats: Arc<FetcherStats> = driver.stats();
    assert_eq!(stats.rejected.load(Ordering::Relaxed), 1);
    task.stop().await;
    p.shutdown().await;
}

#[test]
fn blob_refs_walk_the_command_tree() {
    let sha = [3u8; 32];
    let refs = blob::blob_refs_of([blob_cmd(1, sha, 10).as_ref(), cmd(2).as_ref()]);
    assert_eq!(refs, vec![sha]);
    let junk = Bytes::from_static(b"\xff\xff\xff");
    assert!(blob::blob_refs_of([junk.as_ref()]).is_empty());
}
