//! 13-05 (D-P13-6, plan check B6): metadata apply is fail-stop on newer input.
//!
//! A committed entry in a newer `META_FORMAT`, with an unknown `MetaCommand` variant, or
//! (level emulation) needing a higher feature level halts the voter through the driver's
//! `FatalApply`: `applied` does not advance, the voter stays halted across restarts, and it
//! is never answered with `MetaResult::Invalid`. One- and three-voter groups.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_meta::handle::{ForwardFuture, MetaOptions, MetaStart, ProposeForwarder, TickConfig};
use ostro_meta::{
    META_FORMAT, MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult, ProposeError,
    QueueConfig, RaftHandle, decode_command, encode_command,
};
use ostro_raft_driver::driver::ChannelOutbox;
use tempfile::TempDir;
use tokio::sync::mpsc;

const LONG: Duration = Duration::from_secs(30);

fn queue(name: &str) -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Queue(name.into()),
        expected_version: None,
        record: MetaRecord::Queue(QueueConfig {
            name: name.into(),
            max_concurrency: Some(1),
            created_at_ms: 1,
            ext: Vec::new(),
        }),
    }
}

fn alter_replicas() -> MetaCommand {
    MetaCommand::AlterReplicas {
        partition: 0,
        epoch: 1,
        expected_isr_version: 0,
        replicas: vec![1, 2, 3],
        isr: vec![1, 2, 3],
        min_insync: 2,
        reassignment: None,
    }
}

/// `[META_FORMAT] ‖ postcard varint 200`: a variant index no build knows yet.
fn unknown_variant() -> Vec<u8> {
    vec![META_FORMAT, 0xC8, 0x01]
}

fn newer_format() -> Vec<u8> {
    let mut b = encode_command(&queue("from-the-future"));
    b[0] = META_FORMAT + 1;
    b
}

async fn halted(raft: &RaftHandle) -> String {
    let mut rx = raft.subscribe();
    tokio::time::timeout(LONG, async {
        loop {
            if let Some(f) = rx.borrow_and_update().fatal.clone() {
                return f;
            }
            if rx.changed().await.is_err() {
                return rx
                    .borrow()
                    .fatal
                    .clone()
                    .expect("driver ended without a fatal");
            }
        }
    })
    .await
    .expect("the voter never halted")
}

#[test]
fn known_variants_decode_identically() {
    for cmd in [queue("q"), alter_replicas()] {
        let bytes = encode_command(&cmd);
        assert_eq!(decode_command(&bytes).unwrap(), cmd);
        assert_eq!(encode_command(&decode_command(&bytes).unwrap()), bytes);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn one_voter_newer_format_halts_and_stays_halted() {
    let dir = TempDir::new().unwrap();
    let h = MetaHandle::start(dir.path(), 1).await.unwrap();
    assert_eq!(
        h.propose(queue("before")).await.unwrap(),
        MetaResult::Ok { version: 1 }
    );
    let applied = h.view().applied();
    let err = h.raft().propose(newer_format()).await.unwrap_err();
    assert!(
        matches!(&err, ProposeError::Fatal(m) if m.contains("written by a newer Ostro")),
        "{err:?}"
    );
    let f = halted(h.raft()).await;
    assert!(f.contains("finalized by a newer Ostro"), "{f}");
    assert_eq!(h.view().newer_format(), Some(META_FORMAT + 1));
    assert_eq!(
        h.view().applied(),
        applied,
        "applied must not pass the refused entry"
    );
    assert!(h.failure().is_some());
    assert_eq!(
        h.propose(queue("after")).await,
        Err(MetaError::NewerFormat {
            found: META_FORMAT + 1,
            supported: META_FORMAT
        })
    );
    h.stop().await;

    // The restart replays the entry and refuses to serve.
    let e = MetaHandle::start(dir.path(), 1).await.unwrap_err();
    assert_eq!(
        e,
        MetaError::NewerFormat {
            found: META_FORMAT + 1,
            supported: META_FORMAT
        }
    );
    assert!(e.to_string().contains("finalized by a newer Ostro"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_voter_unknown_variant_halts_not_invalid() {
    let dir = TempDir::new().unwrap();
    let h = MetaHandle::start(dir.path(), 1).await.unwrap();
    let err = h.raft().propose(unknown_variant()).await.unwrap_err();
    assert!(
        matches!(&err, ProposeError::Fatal(m) if m.contains("metadata entry written by a newer Ostro; upgrade this binary")),
        "{err:?}"
    );
    halted(h.raft()).await;
    assert_eq!(h.view().newer_format(), None);
    h.stop().await;
    let e = MetaHandle::start(dir.path(), 1).await.unwrap_err();
    assert!(e.to_string().contains("written by a newer Ostro"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_voter_level_one_halts_on_alter_replicas() {
    let dir = TempDir::new().unwrap();
    let opts = MetaOptions {
        level_max: Some(1),
        ..MetaOptions::default()
    };
    let start = |d: &std::path::Path| {
        let d = d.to_path_buf();
        async move {
            MetaHandle::start_with_options(
                &d,
                MetaStart {
                    node_id: 1,
                    voters: vec![1],
                    outbox: Box::new(ostro_meta::NullOutbox),
                    forwarder: None,
                    tick: TickConfig::default(),
                },
                opts,
            )
            .await
        }
    };
    let h = start(dir.path()).await.unwrap();
    assert_eq!(
        h.propose(queue("ok")).await.unwrap(),
        MetaResult::Ok { version: 1 }
    );
    let e = h.propose(alter_replicas()).await.unwrap_err();
    assert!(e.to_string().contains("needs feature level 2"), "{e}");
    halted(h.raft()).await;
    h.stop().await;
    let e = start(dir.path()).await.unwrap_err();
    assert!(e.to_string().contains("needs feature level 2"), "{e}");
    // The same data at level 2 (no emulation) applies it.
    let h = MetaHandle::start(dir.path(), 1).await.unwrap();
    assert!(h.view().read(|s| s.queue("ok").is_some()));
    h.stop().await;
}

// ---- three voters ------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Net(Arc<Mutex<HashMap<u64, RaftHandle>>>);

impl Net {
    fn get(&self, id: u64) -> Option<RaftHandle> {
        self.0.lock().unwrap().get(&id).cloned()
    }
}

struct LocalForwarder(Net);

impl ProposeForwarder for LocalForwarder {
    fn forward(&self, leader: u64, cmd: Vec<u8>) -> ForwardFuture<'_> {
        Box::pin(async move {
            // As `GrpcForwarder`: a stopped or halted leader is an unknown outcome (retried).
            match self.0.get(leader) {
                Some(h) => match h.propose(cmd).await {
                    Err(ProposeError::Stopped | ProposeError::Fatal(_)) => {
                        Err(ProposeError::OutcomeUnknown)
                    }
                    other => other,
                },
                None => Err(ProposeError::NotLeader { leader: None }),
            }
        })
    }
}

struct Group {
    net: Net,
    dirs: HashMap<u64, TempDir>,
    nodes: HashMap<u64, MetaHandle>,
}

impl Group {
    async fn new(levels: &[(u64, Option<u32>)]) -> Group {
        let mut g = Group {
            net: Net::default(),
            dirs: levels
                .iter()
                .map(|&(i, _)| (i, TempDir::new().unwrap()))
                .collect(),
            nodes: HashMap::new(),
        };
        for &(id, level) in levels {
            g.start(id, level).await.unwrap();
        }
        for h in g.nodes.values() {
            h.wait_ready().await.unwrap();
        }
        g
    }

    async fn start(&mut self, id: u64, level_max: Option<u32>) -> Result<(), MetaError> {
        let (tx, mut rx) =
            mpsc::unbounded_channel::<Vec<ostro_raft_driver::raft::eraftpb::Message>>();
        let h = MetaHandle::start_with_options(
            self.dirs[&id].path(),
            MetaStart {
                node_id: id,
                voters: vec![1, 2, 3],
                outbox: Box::new(ChannelOutbox(tx)),
                forwarder: Some(Arc::new(LocalForwarder(self.net.clone()))),
                tick: TickConfig {
                    interval: Duration::from_millis(50),
                    election_tick: 10,
                    heartbeat_tick: 2,
                },
            },
            MetaOptions {
                sync: false,
                level_max,
            },
        )
        .await?;
        self.net.0.lock().unwrap().insert(id, h.raft().clone());
        let net = self.net.clone();
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                for m in batch {
                    if let Some(to) = net.get(m.to) {
                        to.step(m);
                    }
                }
            }
        });
        self.nodes.insert(id, h);
        Ok(())
    }

    async fn stop(&mut self, id: u64) {
        self.net.0.lock().unwrap().remove(&id);
        if let Some(h) = self.nodes.remove(&id) {
            h.stop().await;
        }
    }

    async fn stop_all(&mut self) {
        for id in [1, 2, 3] {
            self.stop(id).await;
        }
    }
}

/// Mixed levels: node 1 runs at level 1 (an old binary), 2 and 3 at level 2. AlterReplicas
/// halts node 1 only; the others keep committing (2 of 3) and node 1 stays halted after a
/// restart.
#[tokio::test(flavor = "multi_thread")]
async fn three_voters_level_one_voter_halts_others_continue() {
    let mut g = Group::new(&[(1, Some(1)), (2, None), (3, None)]).await;
    let via = 2;
    assert_eq!(
        g.nodes[&via].propose(queue("q0")).await.unwrap(),
        MetaResult::Ok { version: 1 }
    );
    // Proposed through a level-2 node (13-04's gate refuses it earlier in production).
    let r = g.nodes[&via].propose(alter_replicas()).await;
    assert!(
        r.is_ok() || matches!(r, Err(MetaError::Propose(_))),
        "{r:?}"
    );
    let f = halted(g.nodes[&1].raft()).await;
    assert!(f.contains("needs feature level 2"), "{f}");
    let halted_at = g.nodes[&1].view().applied();
    // The other two continue.
    for (i, id) in [2u64, 3].into_iter().enumerate() {
        let r = g.nodes[&id]
            .propose(queue(&format!("after{i}")))
            .await
            .unwrap();
        assert_eq!(r, MetaResult::Ok { version: 1 });
        assert!(g.nodes[&id].raft().status().fatal.is_none());
    }
    assert_eq!(g.nodes[&1].view().applied(), halted_at);
    assert!(g.nodes[&1].view().read(|s| s.queue("after0").is_none()));

    // Restarted at level 1: halts again during the replay.
    g.stop(1).await;
    let e = g.start(1, Some(1)).await;
    match e {
        Err(e) => assert!(e.to_string().contains("needs feature level 2"), "{e}"),
        Ok(()) => {
            let f = halted(g.nodes[&1].raft()).await;
            assert!(f.contains("needs feature level 2"), "{f}");
        }
    }
    g.stop_all().await;
}

/// Same binary everywhere: an unknown variant halts every voter that learns it committed.
/// A quorum halts (the leader that applied it, then the next leader whose first commit
/// covers it); a voter that never learns the commit index cannot apply it either, and the
/// group makes no progress past the entry. Every halted voter halts again on restart.
#[tokio::test(flavor = "multi_thread")]
async fn three_voters_unknown_variant_halts_a_quorum_and_nobody_applies_it() {
    let mut g = Group::new(&[(1, None), (2, None), (3, None)]).await;
    assert_eq!(
        g.nodes[&1].propose(queue("q0")).await.unwrap(),
        MetaResult::Ok { version: 1 }
    );
    let leader = loop {
        if let Some(l) = g.nodes[&1].raft().status().leader_id {
            break l;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let before = g.nodes[&leader].view().applied();
    let _ = g.nodes[&leader].raft().propose(unknown_variant()).await;
    let f = halted(g.nodes[&leader].raft()).await;
    assert!(f.contains("written by a newer Ostro"), "{f}");
    let halted_at = g.nodes[&leader].view().applied();
    assert!(halted_at >= before);
    // A second voter halts once a new leader commits past the entry.
    let deadline = std::time::Instant::now() + LONG;
    let second = loop {
        if let Some(id) = [1u64, 2, 3]
            .into_iter()
            .find(|&id| id != leader && g.nodes[&id].raft().status().fatal.is_some())
        {
            break id;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no second voter halted"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(g.nodes[&second].view().applied(), halted_at, "same index");
    for id in [1u64, 2, 3] {
        assert!(
            g.nodes[&id].view().applied() <= halted_at,
            "node {id} applied past the entry"
        );
    }
    g.stop_all().await;
    // Restarted together, a voter whose persisted commit covers the entry halts during
    // its replay (start fails); the others wait for a leader whose first commit covers it
    // and halt then (a halt does not persist its cycle's commit index). A voter left alone
    // without a quorum cannot learn the commit and stays unhalted, but never applies it.
    // So: a quorum halts again, and nobody applies past the entry.
    let mut failed_starts = Vec::new();
    for id in [1u64, 2, 3] {
        if let Err(e) = g.start(id, None).await {
            assert!(e.to_string().contains("written by a newer Ostro"), "{e}");
            failed_starts.push(id);
        }
    }
    let deadline = std::time::Instant::now() + LONG;
    loop {
        let mut halted_now = failed_starts.len();
        for (id, h) in &g.nodes {
            assert!(
                h.view().applied() <= halted_at,
                "node {id} applied past the entry"
            );
            if let Some(f) = h.raft().status().fatal {
                assert!(f.contains("written by a newer Ostro"), "{f}");
                halted_now += 1;
            }
        }
        if halted_now >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "only {halted_now} voters halted after the restart"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    g.stop_all().await;
}
