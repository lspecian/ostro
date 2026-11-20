//! Shared helpers for the ostro-backup integration tests (plan 08-13): a Node + LocalFs store
//! in a tempdir, workload commands, and a "restore at this manifest" simulation.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_backup::chunk;
use ostro_backup::manifest::{self, Manifest};
use ostro_backup::uploader::{Uploader, UploaderConfig};
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_partition::apply::{Cursor, apply_command};
use ostro_partition::store::snapshot::install_snapshot;
use ostro_partition::{ApplyOutcome, Node, NodeConfig, PartitionStore, StoreConfig, SystemClock};
use ostro_types::command::{
    AttemptTokenMsg, ConflictPolicy, Derived, EventsAppended, OutboxMsg, SendMessage, Signal,
    StartRun, derived, outbox_msg,
};
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::time::timeout;

/// Generous timeout for things that should happen soon.
pub const T: Duration = Duration::from_secs(30);
/// Node open timeout (fresh fjall stores fsync a lot; see ostro-partition's harness).
pub const OPEN_T: Duration = Duration::from_secs(120);

/// Node config: backup enabled, no cadence snapshots (tests take them explicitly), fast
/// shuffler, retention that only runs when asked.
pub fn node_cfg(dir: &Path, p: u16) -> NodeConfig {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(p).expect("non-zero"));
    cfg.backup_enabled = true;
    cfg.snapshots.interval = Duration::from_secs(3600);
    cfg.snapshots.min_interval = Duration::ZERO;
    cfg.snapshots.bytes_threshold = u64::MAX;
    cfg.snapshots.keep = 2;
    cfg.leader.shuffler.idle_poll = Duration::from_millis(20);
    cfg.leader.retention.interval = Duration::from_secs(3600);
    cfg.wal.enable_rotation = true;
    cfg.wal.rotation_size_bytes = 2 * 1024;
    cfg
}

pub async fn open(cfg: NodeConfig) -> Arc<Node> {
    Arc::new(
        timeout(OPEN_T, Node::open(cfg))
            .await
            .expect("node open timed out")
            .expect("node open"),
    )
}

pub async fn shutdown(node: Arc<Node>) {
    let node = Arc::try_unwrap(node).expect("node still shared");
    node.shutdown().await.expect("shutdown");
}

/// A LocalFs store rooted at `dir`.
pub fn local_store(dir: &Path, conditional: bool) -> Arc<dyn ObjectStore> {
    Arc::new(
        LocalFs::new(dir)
            .expect("localfs")
            .with_conditional_put(conditional),
    )
}

pub fn up_cfg() -> UploaderConfig {
    UploaderConfig {
        seal_interval: Duration::from_millis(200),
        remote_snapshots_keep: 3,
        cluster_id: "test-cluster".into(),
        node_id: 1,
        max_chunk_bytes: ostro_backup::chunk::MAX_CHUNK_BYTES,
        publish_floor: true,
        retention: true,
    }
}

pub fn uploader(node: &Arc<Node>, cfg: UploaderConfig) -> Arc<Uploader> {
    Arc::new(Uploader::new(
        Arc::clone(node),
        None,
        Arc::new(SystemClock),
        cfg,
    ))
}

pub fn run_on(p: u16) -> RunId {
    RunId::new_v7(PartitionId(p))
}

pub fn start_run(run: &RunId) -> StartRun {
    StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"in"))),
        idempotency_key: vec![],
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: String::new(),
        priority: 0,
        ready_at_ms: 100,
        requested_at_ms: 100,
        parent: None,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

/// Submit and require `Accepted`.
pub async fn submit_ok(node: &Node, p: PartitionId, cmds: Vec<Command>) {
    let h = node.handle(p).expect("ready");
    let out = timeout(T, h.submit(cmds))
        .await
        .expect("submit timed out")
        .expect("submit");
    for o in out {
        assert_eq!(o, ApplyOutcome::Accepted);
    }
}

/// Start `n` runs on `p`.
pub async fn start_runs(node: &Node, p: u16, n: usize) -> Vec<RunId> {
    let runs: Vec<RunId> = (0..n).map(|_| run_on(p)).collect();
    let cmds = runs
        .iter()
        .map(|r| Command::start_run(start_run(r)))
        .collect();
    submit_ok(node, PartitionId(p), cmds).await;
    runs
}

/// Start `run` and grant it an attempt.
pub async fn start_and_grant(node: &Node, run: &RunId) -> AttemptToken {
    submit_ok(
        node,
        run.partition(),
        vec![Command::start_run(start_run(run))],
    )
    .await;
    let h = node.handle(run.partition()).expect("ready");
    timeout(T, h.grant_attempt(*run))
        .await
        .expect("grant timed out")
        .expect("grant")
}

/// `EventsAppended` from `run` sending one signal to each target.
pub fn signals_cmd(run: &RunId, t: AttemptToken, targets: &[(RunId, String)]) -> Command {
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        }),
        from_seq: 0,
        events: vec![],
        derived: targets
            .iter()
            .map(|(target, name)| Derived {
                intent: Some(derived::Intent::SendMessage(SendMessage {
                    msg: Some(OutboxMsg {
                        kind: Some(outbox_msg::Kind::Signal(Signal {
                            target_run: target.as_bytes().to_vec(),
                            name: name.clone(),
                            payload: Some(PayloadRef::inline(Bytes::from_static(b"p"))),
                        })),
                    }),
                })),
            })
            .collect(),
    })
}

/// Per partition after a simulated restore: `(outbox_next_seq, outbox_truncated,
/// inbound_seen)` plus the digest.
#[derive(Clone, Debug)]
pub struct Restored {
    pub p: u16,
    pub next: std::collections::BTreeMap<u16, u64>,
    pub truncated: std::collections::BTreeMap<u16, u64>,
    pub seen: std::collections::BTreeMap<u16, u64>,
    pub digest: [u8; 32],
    pub applied: u64,
}

/// Simulate "restore at this manifest" for every partition: install the referenced snapshot
/// (or start empty), walk the chain back from the head, replay `[start, cut)` and check the
/// state hash at `verify_offset`.
pub async fn simulate_restore(
    store: &Arc<dyn ObjectStore>,
    m: &Manifest,
    count: u16,
) -> Vec<Restored> {
    m.validate().expect("manifest validates");
    let pc = PartitionCount::new(count).expect("non-zero");
    let mut out = Vec::new();
    for e in &m.cut {
        let dir = tempfile::tempdir().expect("tempdir");
        let pid = PartitionId(e.p);
        if let (Some(k), Some(sha)) = (&e.snapshot_key, e.snapshot_sha256) {
            let (bytes, _) = store.get(k).await.expect("get").expect("snapshot present");
            assert_eq!(chunk::chunk_sha(&bytes), sha, "snapshot sha");
            let b = bytes.to_vec();
            let d = dir.path().to_path_buf();
            tokio::task::spawn_blocking(move || {
                install_snapshot(
                    &d,
                    pid,
                    pc,
                    &StoreConfig::default(),
                    std::io::Cursor::new(b),
                )
            })
            .await
            .expect("join")
            .expect("install");
        }
        let (store_p, meta) =
            PartitionStore::open(dir.path(), pid, pc, &StoreConfig::default()).expect("open");
        let mut cur = Cursor::from_meta(&meta);
        let start = e.snapshot_offset.unwrap_or(0);
        assert_eq!(cur.next.0, start, "restore starts at the snapshot");
        let mut verified = false;
        let check = |store_p: &PartitionStore, at: u64, verified: &mut bool| {
            if at == e.verify_offset {
                let meta = store_p.read_meta().expect("meta");
                assert_eq!(
                    meta.state_hash, e.verify_state_hash,
                    "partition {} state hash at verify_offset {at}",
                    e.p
                );
                *verified = true;
            }
        };
        check(&store_p, cur.next.0, &mut verified);
        if let (Some(head), Some(sha)) = (&e.last_chunk_key, e.last_chunk_sha256) {
            let stop = e.snapshot_offset.unwrap_or(e.chain_start);
            let chunks = chunk::walk_back(store, head, sha, stop)
                .await
                .expect("walk back");
            assert!(chunks.first().is_some_and(|c| c.chunk.header.base <= start));
            assert_eq!(
                chunks.last().map(|c| c.chunk.header.end),
                Some(e.cut_offset)
            );
            for c in &chunks {
                for env in &c.chunk.envelopes {
                    let view = ostro_types::envelope::verify(env).expect("envelope");
                    if view.last().0 < cur.next.0 {
                        continue;
                    }
                    let mut b = store_p.batch();
                    let mut actions = Vec::new();
                    for (off, cmd) in view.commands_with_offsets() {
                        apply_command(&mut b, &mut cur, off, view.header.epoch, cmd, &mut actions)
                            .expect("apply");
                    }
                    let mu = cur.meta_update();
                    b.commit(&mu).expect("commit");
                    check(&store_p, cur.next.0, &mut verified);
                }
            }
        }
        assert_eq!(
            cur.next.0, e.cut_offset,
            "partition {} replayed to the cut",
            e.p
        );
        assert!(
            verified,
            "partition {}: verify_offset {} never reached",
            e.p, e.verify_offset
        );
        let meta = store_p.read_meta().expect("meta");
        out.push(Restored {
            p: e.p,
            next: meta.cursor.outbox_next_seq.clone(),
            truncated: meta.cursor.outbox_truncated.clone(),
            seen: meta.inbound_seen.clone(),
            digest: store_p.digest().expect("digest"),
            applied: meta.applied.0,
        });
    }
    out
}

/// Cross-partition consistency (Pitfall 1): for every pair A != B,
/// `outbox_truncated[A->B] <= inbound_seen_B[A] <= outbox_next_seq[A->B] - 1`.
pub fn assert_consistent(r: &[Restored]) {
    for a in r {
        for b in r {
            if a.p == b.p {
                continue;
            }
            let next = a.next.get(&b.p).copied().unwrap_or(1);
            let truncated = a.truncated.get(&b.p).copied().unwrap_or(0);
            let seen = b.seen.get(&a.p).copied().unwrap_or(0);
            assert!(
                seen < next,
                "partition {} saw seq {seen} from {} but the sender's next seq is {next}",
                b.p,
                a.p
            );
            assert!(
                truncated <= seen,
                "partition {} truncated its outbox to {} up to {truncated} but {} only saw {seen}",
                a.p,
                b.p,
                b.p
            );
        }
    }
}

/// The current manifest.
pub async fn current_manifest(store: &Arc<dyn ObjectStore>) -> Option<Manifest> {
    let (b, _) = store.get(manifest::CURRENT_KEY).await.expect("get")?;
    Some(manifest::decode(&b).expect("decode"))
}

/// Every key under `prefix`.
pub async fn keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    store
        .list(prefix, None)
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}
