//! 12-08 Task 2: follower and install primitives on the one log type. A follower mirrors a
//! leader byte for byte through `read_replica` → `append_replica`, truncates a divergent
//! tail (12-04 divergence rules), and is reset/anchored for a snapshot install (`reset_to`).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_log::replica::divergence::{FetchCheck, check, follower_truncation_point};
use ostro_log::{
    EpochHistory, FollowerProgress, LogError, LogNode, LogNodeConfig, PartitionLog, RawBatch,
    ReplicaSet, ReplicatedLog,
};
use ostro_types::envelope::HEADER_LEN;
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId, envelope};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);
const LEADER: u64 = 1;
const FOLLOWER: u64 = 2;
const MAX_ENV: usize = 256 << 10;

async fn open(dir: &Path, node_id: u64, applied: Offset) -> (LogNode, Arc<PartitionLog>) {
    let mut cfg = LogNodeConfig::new(dir.join("wal"), PartitionCount::new(1).unwrap(), node_id);
    cfg.max_envelope_bytes = MAX_ENV;
    let (node, mut logs) = LogNode::open(cfg, &[applied]).await.unwrap();
    (node, logs.remove(0).unwrap())
}

fn pair_set(isr: &[u64]) -> ReplicaSet {
    ReplicaSet {
        replicas: vec![LEADER, FOLLOWER],
        isr: isr.to_vec(),
        isr_version: 1,
        min_insync: 1,
    }
}

fn cmds(tag: &str, n: usize) -> Vec<Bytes> {
    (0..n)
        .map(|i| Bytes::from(format!("{tag}-{i}-{}", "p".repeat(i * 11))))
        .collect()
}

fn hw(log: &PartitionLog) -> Offset {
    *log.high_watermark().borrow()
}

async fn wait_durable(log: &PartitionLog, end: Offset) {
    let mut rx = log.durable_end_watch();
    timeout(T, rx.wait_for(|d| *d >= end))
        .await
        .expect("durable end never reached")
        .unwrap();
}

async fn append_batches(log: &PartitionLog, epoch: u32, tag: &str, n: usize) -> Offset {
    let mut last = log.log_end();
    for i in 0..n {
        let r = log
            .append(Epoch(epoch), cmds(&format!("{tag}{i}"), 1 + i % 4))
            .await
            .unwrap();
        last = r.last.next();
    }
    wait_durable(log, last).await;
    last
}

/// One follower fetch loop (what 12-11's fetcher does): divergence check, progress recorded
/// only after an `Ok` check with the durable end sampled before the read (W3), verbatim
/// append, follower HW = min(leader HW, own durable end). Runs until caught up.
async fn pull(leader: &PartitionLog, follower: &PartitionLog, max_bytes: usize) -> usize {
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds < 10_000, "pull did not converge");
        let fetch = follower.log_end();
        match check(
            &leader.epochs(),
            leader.log_start(),
            leader.log_end(),
            fetch,
            follower.latest_epoch(),
        ) {
            FetchCheck::Ok => {}
            FetchCheck::Diverging { epoch, end } => {
                let at =
                    follower_truncation_point(&follower.epochs(), follower.log_end(), epoch, end);
                follower.truncate_to(at).await.unwrap();
                continue;
            }
            FetchCheck::SnapshotRequired => panic!("snapshot required at {fetch}"),
        }
        let leader_end = leader.durable_end();
        let batches = leader.read_replica(fetch, max_bytes).await.unwrap();
        leader
            .record_progress(FollowerProgress {
                replica: FOLLOWER,
                durable_leo: follower.durable_end(),
                fetch_offset: fetch,
                now_ms: rounds as u64,
                leader_durable_end: leader_end,
            })
            .unwrap();
        follower.set_follower_hw(hw(leader));
        assert!(hw(follower) <= hw(leader), "follower HW above the leader's");
        assert!(hw(follower) <= follower.durable_end());
        if batches.is_empty() {
            if fetch >= leader.durable_end() && follower.durable_end() >= fetch {
                // One more round reports the final durable end to the leader.
                leader
                    .record_progress(FollowerProgress {
                        replica: FOLLOWER,
                        durable_leo: follower.durable_end(),
                        fetch_offset: fetch,
                        now_ms: rounds as u64,
                        leader_durable_end: leader_end,
                    })
                    .unwrap();
                follower.set_follower_hw(hw(leader));
                return rounds;
            }
            continue;
        }
        assert!(batches.iter().all(|b| b.end <= leader_end));
        let leo = follower.append_replica(batches).await.unwrap();
        assert_eq!(leo, follower.log_end());
        wait_durable(follower, leo).await;
    }
}

async fn all_envelopes(log: &PartitionLog, from: Offset) -> Vec<(u32, Bytes)> {
    let raw = log.read_replica(from, usize::MAX).await.unwrap();
    raw.into_iter()
        .flat_map(|b| b.envelopes.into_iter().map(move |e| (b.epoch, e)))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_mirrors_the_leader_byte_for_byte() {
    let (ld, fd) = (ostro_testkit::fs::tempdir(), ostro_testkit::fs::tempdir());
    let (ln, leader) = open(ld.path(), LEADER, Offset::ZERO).await;
    let (fnode, follower) = open(fd.path(), FOLLOWER, Offset::ZERO).await;
    leader
        .become_leader(Epoch(1), pair_set(&[LEADER, FOLLOWER]))
        .unwrap();
    follower.become_follower(Epoch(1), LEADER).unwrap();

    let end = append_batches(&leader, 1, "m", 50).await;
    // Not committed (the follower holds nothing), yet read_replica serves up to the durable
    // end: it is never HW-capped.
    assert_eq!(hw(&leader), Offset::ZERO);
    let first = leader.read_replica(Offset::ZERO, usize::MAX).await.unwrap();
    assert_eq!(first.last().unwrap().end, end);
    assert!(
        leader
            .read_envelopes(Offset::ZERO, usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
    // At least one whole batch even with a 1-byte budget.
    let one = leader.read_replica(Offset::ZERO, 1).await.unwrap();
    assert_eq!(
        (one.len(), one[0].envelopes.len(), one[0].base),
        (1, 1, Offset::ZERO)
    );

    let rounds = pull(&leader, &follower, 300).await;
    assert!(
        rounds > 3,
        "small budget pulls in several rounds ({rounds})"
    );
    assert_eq!(follower.log_end(), end);
    assert_eq!(hw(&leader), end);
    assert_eq!(hw(&follower), end);
    assert_eq!(follower.epochs(), leader.epochs());
    // Verbatim: every stored envelope is identical.
    assert_eq!(
        all_envelopes(&follower, Offset::ZERO).await,
        all_envelopes(&leader, Offset::ZERO).await
    );
    let st = leader.replica_status();
    assert_eq!(st.followers.len(), 1);
    assert_eq!(st.followers[0].durable_leo, end);
    // A follower refuses to drop committed data.
    assert!(matches!(
        follower.truncate_to(Offset(3)).await,
        Err(LogError::Failed(m)) if m.contains("truncate below HW")
    ));

    // And it survives a reopen: the same bytes come back.
    fnode.shutdown().await;
    drop(follower);
    drop(fnode);
    let (fnode, follower) = open(fd.path(), FOLLOWER, end).await;
    assert_eq!(hw(&follower), end, "HW at open = applied (F4)");
    assert_eq!(
        all_envelopes(&follower, Offset::ZERO).await,
        all_envelopes(&leader, Offset::ZERO).await
    );
    fnode.shutdown().await;
    ln.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_divergent_follower_truncates_and_refetches_to_identity() {
    let (ld, fd) = (ostro_testkit::fs::tempdir(), ostro_testkit::fs::tempdir());
    let (ln, leader) = open(ld.path(), LEADER, Offset::ZERO).await;
    let (fnode, follower) = open(fd.path(), FOLLOWER, Offset::ZERO).await;
    leader
        .become_leader(Epoch(1), pair_set(&[LEADER, FOLLOWER]))
        .unwrap();
    follower.become_follower(Epoch(1), LEADER).unwrap();
    let common = append_batches(&leader, 1, "c", 10).await;
    pull(&leader, &follower, 1 << 20).await;
    assert_eq!(hw(&follower), common);

    // The follower briefly leads epoch 2 (its peer never acknowledges) and writes a tail
    // nobody else has; the HW stays at the common prefix.
    follower
        .become_leader(
            Epoch(2),
            ReplicaSet {
                replicas: vec![FOLLOWER, LEADER],
                isr: vec![FOLLOWER, LEADER],
                isr_version: 2,
                min_insync: 1,
            },
        )
        .unwrap();
    let divergent = append_batches(&follower, 2, "d", 3).await;
    assert_eq!(hw(&follower), common);
    // Meanwhile the old leader is elected at epoch 3 and writes its own tail.
    leader
        .become_leader(Epoch(3), pair_set(&[LEADER, FOLLOWER]))
        .unwrap();
    let end = append_batches(&leader, 3, "n", 5).await;
    follower.become_follower(Epoch(3), LEADER).unwrap();
    assert!(divergent > common);

    pull(&leader, &follower, 1 << 20).await;
    assert_eq!(follower.log_end(), end);
    assert_eq!(follower.epochs(), leader.epochs());
    assert_eq!(
        all_envelopes(&follower, Offset::ZERO).await,
        all_envelopes(&leader, Offset::ZERO).await
    );
    assert_eq!(hw(&follower), end);
    fnode.shutdown().await;
    ln.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reset_to_anchors_for_install_and_survives_a_reopen() {
    let (ld, fd) = (ostro_testkit::fs::tempdir(), ostro_testkit::fs::tempdir());
    let history = EpochHistory(vec![(Epoch(1), Offset(0)), (Epoch(2), Offset(300))]);
    // A leader whose log starts at 500 (its own install / trim), history below seeded.
    let (ln, leader) = open(ld.path(), LEADER, Offset(500)).await;
    leader.seed_epochs(&history.0).await.unwrap();
    leader
        .become_leader(Epoch(3), pair_set(&[LEADER, FOLLOWER]))
        .unwrap();
    let end = append_batches(&leader, 3, "i", 8).await;

    // A follower with an old, unrelated log of its own.
    let (fnode, follower) = open(fd.path(), FOLLOWER, Offset::ZERO).await;
    follower
        .become_leader(Epoch(1), ReplicaSet::solo(FOLLOWER))
        .unwrap();
    append_batches(&follower, 1, "old", 6).await;
    // reset_to is refused while leading.
    assert!(matches!(
        follower.reset_to(Offset(500), history.clone()).await,
        Err(LogError::Failed(_))
    ));
    follower.become_follower(Epoch(3), LEADER).unwrap();
    follower
        .reset_to(Offset(500), history.clone())
        .await
        .unwrap();
    assert_eq!(
        (
            follower.log_start(),
            follower.log_end(),
            hw(&follower),
            follower.durable_end()
        ),
        (Offset(500), Offset(500), Offset(500), Offset(500))
    );
    assert_eq!(follower.epochs(), history);
    assert_eq!(follower.latest_epoch(), Some(Epoch(2)));
    let seg_dir = fd.path().join("wal/p/0");
    for entry in std::fs::read_dir(&seg_dir).unwrap() {
        let len = entry.unwrap().metadata().unwrap().len();
        assert_eq!(len, 0, "every segment with records is gone");
    }
    assert!(matches!(
        follower.read(Offset(0), Offset(600), 1 << 20).await,
        Err(LogError::OutOfRange(_))
    ));

    // The follower catches up from the anchor (W5: fetch at log_start is Ok).
    pull(&leader, &follower, 1 << 20).await;
    assert_eq!(follower.log_end(), end);
    assert_eq!(
        all_envelopes(&follower, Offset(500)).await,
        all_envelopes(&leader, Offset(500)).await
    );
    fnode.shutdown().await;
    drop(follower);
    drop(fnode);

    // Reopen (the installed state's applied offset is 500): same anchored log.
    let (fnode, follower) = open(fd.path(), FOLLOWER, Offset(500)).await;
    assert_eq!(
        (follower.log_start(), follower.log_end()),
        (Offset(500), end)
    );
    assert_eq!(follower.durable_end(), end);
    assert_eq!(hw(&follower), Offset(500));
    assert_eq!(
        all_envelopes(&follower, Offset(500)).await,
        all_envelopes(&leader, Offset(500)).await
    );
    fnode.shutdown().await;
    ln.shutdown().await;

    // A reset with nothing appended after it reopens anchored, history re-seeded by the
    // caller from its cursor (05-21).
    let d = ostro_testkit::fs::tempdir();
    let (node, log) = open(d.path(), FOLLOWER, Offset::ZERO).await;
    log.become_leader(Epoch(1), ReplicaSet::solo(FOLLOWER))
        .unwrap();
    append_batches(&log, 1, "x", 4).await;
    log.become_follower(Epoch(3), LEADER).unwrap();
    log.reset_to(Offset(500), history.clone()).await.unwrap();
    node.shutdown().await;
    drop(log);
    drop(node);
    let (node, log) = open(d.path(), FOLLOWER, Offset(500)).await;
    assert_eq!((log.log_start(), log.log_end()), (Offset(500), Offset(500)));
    log.seed_epochs(&history.0).await.unwrap();
    assert_eq!(log.epochs(), history);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_max_size_envelope_round_trips() {
    let (ld, fd) = (ostro_testkit::fs::tempdir(), ostro_testkit::fs::tempdir());
    let (ln, leader) = open(ld.path(), LEADER, Offset::ZERO).await;
    let (fnode, follower) = open(fd.path(), FOLLOWER, Offset::ZERO).await;
    leader.become_leader(Epoch(1), pair_set(&[LEADER])).unwrap();
    follower.become_follower(Epoch(1), LEADER).unwrap();
    let big = Bytes::from(vec![0xa5u8; MAX_ENV - HEADER_LEN - 4]);
    let r = leader.append(Epoch(1), vec![big]).await.unwrap();
    let small = leader.append(Epoch(1), cmds("s", 2)).await.unwrap();
    leader.wait_committed(small.last).await.unwrap();
    let raw = leader.read_replica(Offset::ZERO, 1).await.unwrap();
    assert_eq!(raw[0].envelopes.len(), 1);
    assert_eq!(raw[0].envelopes[0].len(), MAX_ENV);
    assert_eq!(raw[0].end, r.last.next());
    pull(&leader, &follower, 1).await;
    assert_eq!(
        all_envelopes(&follower, Offset::ZERO).await,
        all_envelopes(&leader, Offset::ZERO).await
    );
    fnode.shutdown().await;
    ln.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_replica_verifies_every_envelope() {
    let (fd, _) = (ostro_testkit::fs::tempdir(), ());
    let (fnode, follower) = open(fd.path(), FOLLOWER, Offset::ZERO).await;
    let p = PartitionId(0);
    let env = |epoch: u32, base: u64, part: PartitionId| {
        Bytes::from(envelope::encode(
            part,
            Epoch(epoch),
            Offset(base),
            &cmds("v", 2),
        ))
    };
    let batch = |epoch: u32, base: u64, e: Bytes| RawBatch {
        base: Offset(base),
        end: Offset(base + 2),
        epoch,
        envelopes: vec![e],
    };
    // Not a follower yet.
    assert!(
        follower
            .append_replica(vec![batch(1, 0, env(1, 0, p))])
            .await
            .is_err()
    );
    follower.become_follower(Epoch(2), LEADER).unwrap();
    let rejected = |r: Result<Offset, LogError>| matches!(r, Err(LogError::Wal(_)));
    // base != LEO
    assert!(rejected(
        follower
            .append_replica(vec![batch(1, 5, env(1, 5, p))])
            .await
    ));
    // corrupted CRC
    let mut bad = env(1, 0, p).to_vec();
    let last = bad.len() - 1;
    bad[last] ^= 0xff;
    assert!(rejected(
        follower
            .append_replica(vec![batch(1, 0, Bytes::from(bad))])
            .await
    ));
    // another partition's envelope
    assert!(rejected(
        follower
            .append_replica(vec![batch(1, 0, env(1, 0, PartitionId(3)))])
            .await
    ));
    // epoch above the followed epoch
    assert!(rejected(
        follower
            .append_replica(vec![batch(3, 0, env(3, 0, p))])
            .await
    ));
    assert_eq!(follower.log_end(), Offset::ZERO, "nothing written");
    // Valid: accepted, and the epoch cache grows; an older epoch afterwards is refused.
    assert_eq!(
        follower
            .append_replica(vec![batch(2, 0, env(2, 0, p))])
            .await
            .unwrap(),
        Offset(2)
    );
    assert!(rejected(
        follower
            .append_replica(vec![batch(1, 2, env(1, 2, p))])
            .await
    ));
    assert_eq!(follower.epochs().0, vec![(Epoch(2), Offset(0))]);
    wait_durable(&follower, Offset(2)).await;
    assert_eq!(hw(&follower), Offset::ZERO, "no leader HW received yet");
    follower.set_follower_hw(Offset(100));
    assert_eq!(hw(&follower), Offset(2), "capped at the local durable end");
    fnode.shutdown().await;
}
