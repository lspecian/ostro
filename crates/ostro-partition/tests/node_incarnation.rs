//! Node bottomless primitives (08-08):
//! - `node.toml` `incarnation`: written on first open, preserved on reopen, added to an
//!   old file without it (existing lines kept, atomic rewrite);
//! - `Node::log_cut` / `Node::read_envelopes` pass through to the log node;
//! - `Node::applied_state` returns an atomic `(applied_offset, state_hash)` pair: under
//!   concurrent applies every observed pair equals a replay of the log to that offset.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::{T, open_node, run_on, start_cmd};
use ostro_log::ReplicatedLog;
use ostro_partition::apply::{Cursor, apply_command};
use ostro_partition::node::hex16;
use ostro_partition::{ApplyOutcome, PartitionStore, StartMode, StoreConfig};
use ostro_types::{Offset, PartitionCount, PartitionId};
use tokio::time::timeout;

fn incarnation_in_file(dir: &std::path::Path) -> Option<String> {
    let s = std::fs::read_to_string(dir.join("node.toml")).expect("node.toml");
    s.lines()
        .filter_map(|l| l.split_once('='))
        .find(|(k, _)| k.trim() == "incarnation")
        .map(|(_, v)| v.trim().trim_matches('"').to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn incarnation_written_on_first_open_and_preserved() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    let inc = node.incarnation();
    assert_ne!(inc, [0u8; 16]);
    assert_eq!(incarnation_in_file(d.path()), Some(hex16(&inc)));
    node.shutdown().await.expect("shutdown");

    let body_before = std::fs::read(d.path().join("node.toml")).unwrap();
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    assert_eq!(
        node.incarnation(),
        inc,
        "incarnation is stable across reopens"
    );
    node.shutdown().await.expect("shutdown");
    assert_eq!(
        std::fs::read(d.path().join("node.toml")).unwrap(),
        body_before,
        "a reopen does not rewrite node.toml"
    );

    // A different data dir gets a different incarnation.
    let d2 = ostro_testkit::fs::tempdir();
    let other = open_node(d2.path(), 2, StartMode::Inspect).await;
    assert_ne!(other.incarnation(), inc);
    other.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn old_node_file_without_incarnation_opens_and_gains_it() {
    let d = ostro_testkit::fs::tempdir();
    // A pre-08-08 node: open once, then strip the key to get the old file format.
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    node.shutdown().await.expect("shutdown");
    let old = "# written before 08-08\nformat_version = 1\npartitions = 2\nfuture_key = 7";
    std::fs::write(d.path().join("node.toml"), old).unwrap();

    let node = open_node(d.path(), 2, StartMode::Leader).await;
    let inc = node.incarnation();
    node.shutdown().await.expect("shutdown");
    let body = std::fs::read_to_string(d.path().join("node.toml")).unwrap();
    assert!(body.starts_with(old), "existing lines are kept: {body:?}");
    assert_eq!(
        &body[old.len()..],
        format!("\nincarnation = \"{}\"\n", hex16(&inc))
    );
    assert!(
        !d.path().join("node.toml.tmp").exists(),
        "the tmp file was renamed into place"
    );

    let node = open_node(d.path(), 2, StartMode::Inspect).await;
    assert_eq!(node.incarnation(), inc);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_incarnation_is_rejected() {
    let d = ostro_testkit::fs::tempdir();
    std::fs::write(
        d.path().join("node.toml"),
        "format_version = 1\npartitions = 1\nincarnation = \"xyz\"\n",
    )
    .unwrap();
    let cfg =
        ostro_partition::NodeConfig::new(d.path().to_path_buf(), PartitionCount::new(1).unwrap());
    let err = ostro_partition::Node::open(cfg)
        .await
        .expect_err("must fail");
    assert!(
        matches!(err, ostro_partition::NodeError::NodeFile(_)),
        "{err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn log_cut_and_read_envelopes_pass_through() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    for p in 0..2u16 {
        let h = node.handle(PartitionId(p)).expect("ready");
        for _ in 0..3 {
            let o = timeout(T, h.submit_one(start_cmd(&run_on(p))))
                .await
                .expect("submit timed out")
                .expect("submit");
            assert_eq!(o, ApplyOutcome::Accepted);
        }
    }
    let cut = node.log_cut();
    assert_eq!(cut.len(), 2);
    for p in 0..2u16 {
        let h = node.handle(PartitionId(p)).expect("ready");
        let hw = *h.log().high_watermark().borrow();
        assert!(cut[usize::from(p)] >= Offset(4), "cut {cut:?}");
        assert!(cut[usize::from(p)] <= hw);

        let raw = node
            .read_envelopes(PartitionId(p), Offset(0), usize::MAX)
            .await
            .expect("read_envelopes");
        let direct = h
            .log()
            .read_envelopes(Offset(0), usize::MAX)
            .await
            .expect("direct");
        assert_eq!(raw, direct);
        assert_eq!(raw.first().expect("batches").base, Offset(0));
        assert_eq!(raw.last().expect("batches").end, hw);
    }
    let err = node
        .read_envelopes(PartitionId(9), Offset(0), 1024)
        .await
        .expect_err("no such partition");
    assert!(matches!(err, ostro_log::LogError::Failed(_)), "{err:?}");
    assert_eq!(node.applied_state(PartitionId(9)), None);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_state_is_atomic_under_concurrent_applies() {
    const P: u16 = 1;
    const WRITERS: usize = 4;
    const PER_WRITER: usize = 60;
    let d = ostro_testkit::fs::tempdir();
    let node = Arc::new(open_node(d.path(), 2, StartMode::Leader).await);
    let p = PartitionId(P);

    let stop = Arc::new(AtomicBool::new(false));
    let poller = {
        let node = Arc::clone(&node);
        let stop = Arc::clone(&stop);
        tokio::task::spawn_blocking(move || {
            let mut seen = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                seen.push(node.applied_state(p).expect("applied_state"));
            }
            seen.push(node.applied_state(p).expect("applied_state"));
            seen
        })
    };
    let mut writers = Vec::new();
    for _ in 0..WRITERS {
        let node = Arc::clone(&node);
        writers.push(tokio::spawn(async move {
            let h = node.handle(p).expect("ready");
            for i in 0..PER_WRITER {
                // Mix single submits and small batches (multi-command apply chunks).
                let n = 1 + i % 3;
                let cmds = (0..n).map(|_| start_cmd(&run_on(P))).collect();
                let outs = timeout(T, h.submit(cmds))
                    .await
                    .expect("submit timed out")
                    .expect("submit");
                assert!(outs.iter().all(|o| *o == ApplyOutcome::Accepted));
            }
        }));
    }
    for w in writers {
        w.await.expect("writer");
    }
    stop.store(true, Ordering::Relaxed);
    let seen = poller.await.expect("poller");

    // Replay the whole log into a fresh store: the (next offset, hash) pair after every
    // command.
    let h = node.handle(p).expect("ready");
    let hw = *h.log().high_watermark().borrow();
    let batches = h
        .log()
        .read(Offset(0), hw, usize::MAX)
        .await
        .expect("read log");
    let rd = ostro_testkit::fs::tempdir();
    let (store, meta) = PartitionStore::open(
        rd.path(),
        p,
        PartitionCount::new(2).unwrap(),
        &StoreConfig::default(),
    )
    .expect("replay store");
    let mut cur = Cursor::from_meta(&meta);
    let mut truth: BTreeMap<Offset, [u8; 32]> = BTreeMap::new();
    truth.insert(cur.next, cur.hash);
    let mut b = store.batch();
    let mut actions = Vec::new();
    for vb in &batches {
        for (off, bytes) in vb.commands_with_offsets() {
            apply_command(&mut b, &mut cur, off, vb.epoch, bytes, &mut actions).expect("apply");
            truth.insert(cur.next, cur.hash);
        }
    }
    assert_eq!(cur.next, hw, "replayed up to HW");

    let mut distinct = std::collections::BTreeSet::new();
    for (off, hash) in &seen {
        let want = truth
            .get(off)
            .unwrap_or_else(|| panic!("applied offset {off:?} beyond the replayed log"));
        assert_eq!(hash, want, "pair at {off:?} mixes two apply chunks");
        distinct.insert(*off);
    }
    assert!(
        distinct.len() > 1,
        "the poller observed only {distinct:?}: no concurrency exercised"
    );
    // Once HW is applied, the pair is the fully applied state (leader services may have
    // appended past `hw` meanwhile; then only the offset bound is checked).
    timeout(T, h.machine().wait_applied(Offset(hw.0 - 1)))
        .await
        .expect("wait_applied timed out")
        .expect("wait_applied");
    let last = node.applied_state(p).expect("applied_state");
    assert!(last.0 >= hw, "{last:?} below HW {hw:?}");
    if last.0 == hw {
        assert_eq!(last.1, truth[&hw]);
    }
    let node = Arc::try_unwrap(node).expect("sole owner");
    node.shutdown().await.expect("shutdown");
}
