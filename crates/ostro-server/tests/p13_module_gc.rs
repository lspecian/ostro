//! Module GC rounds (13-15, CLUS-07 / D-P13-20) over a real single-voter meta and a fake
//! `NodeRefs` per partition leader (nodes 1, 2, 3 lead partitions 0..4):
//!
//! - two clean scans after the grace delete the WorkflowVersions, then the Module; other
//!   modules and the active version are untouched;
//! - a reference appearing between the scans (or present in scan 1) keeps the module;
//! - an unreachable leader, an `uncovered` answer or a leaderless partition postpones the
//!   whole round (nothing deleted, `postponed` + reason);
//! - a module retired less than `gc_grace` ago is not a candidate;
//! - a retired module whose version became active (retire/activate race) is kept;
//! - failpoints build: the `gc::mutant::single_scan` mutant deletes in the
//!   "reference between scans" case (the mutant bites), `gc::before_delete` /
//!   `gc::between_scans` (`return`) postpone.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_cluster::contracts::RefsAnswer;
use ostro_meta::ext::{Retired, put_retired};
use ostro_meta::records::{ActiveVersion, ModuleRecord, PartitionAssignment, WorkflowVersion};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_partition::{Clock, ManualClock};
use ostro_server::p13::module_gc::{GcOptions, ModuleGc, NodeRefs};
use ostro_types::{Epoch, PartitionId};
use tempfile::TempDir;

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

const A: [u8; 32] = [0xaa; 32];
const B: [u8; 32] = [0xbb; 32];
const T0: u64 = 1_000_000;
const GRACE: Duration = Duration::from_millis(200);
const LEADERS: [u64; 4] = [1, 2, 3, 1];

fn hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

/// What the fake nodes answer.
#[derive(Default)]
struct Script {
    /// `(from_call, node, sha)`: `node` reports `sha` from global call number `from_call` on.
    refs: Vec<(usize, u64, [u8; 32])>,
    /// Nodes that fail every call.
    unreachable: BTreeSet<u64>,
    /// Nodes that report every asked partition uncovered.
    uncovering: BTreeSet<u64>,
}

/// `(node, partitions)` of one fake call.
type Call = (u64, Vec<(PartitionId, Epoch)>);

#[derive(Default)]
struct FakeNodes {
    calls: AtomicUsize,
    script: Mutex<Script>,
    /// `(node, partitions)` of every call, in order.
    log: Mutex<Vec<Call>>,
}

impl NodeRefs for FakeNodes {
    fn module_refs(
        &self,
        node: u64,
        shas: Vec<[u8; 32]>,
        partitions: Vec<(PartitionId, Epoch)>,
        _protect_resumable: bool,
    ) -> BoxFuture<'_, Result<RefsAnswer, String>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.log.lock().unwrap().push((node, partitions.clone()));
        let s = self.script.lock().unwrap();
        let out = if s.unreachable.contains(&node) {
            Err(format!("node {node}: connection refused"))
        } else if s.uncovering.contains(&node) {
            Ok(RefsAnswer {
                referenced: vec![],
                uncovered: partitions,
            })
        } else {
            let hit: BTreeSet<[u8; 32]> = s
                .refs
                .iter()
                .filter(|(from, n, _)| *n == node && call >= *from)
                .map(|(_, _, sha)| *sha)
                .collect();
            Ok(RefsAnswer {
                referenced: shas.into_iter().filter(|x| hit.contains(x)).collect(),
                uncovered: vec![],
            })
        };
        Box::pin(async move { out })
    }
}

struct Fixture {
    _dir: TempDir,
    meta: Arc<MetaHandle>,
    clock: ManualClock,
    nodes: Arc<FakeNodes>,
}

async fn ok(meta: &MetaHandle, cmd: MetaCommand) {
    let r = meta.propose(cmd).await.expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

fn module(sha: [u8; 32], workflows: Vec<(String, u32)>, retired_at: Option<u64>) -> MetaRecord {
    let ext = match retired_at {
        Some(t) => put_retired(&[], &Retired { retired_at_ms: t }).expect("ext"),
        None => Vec::new(),
    };
    MetaRecord::Module(ModuleRecord {
        sha256: sha,
        size: 1,
        language: Some("rust".into()),
        workflows,
        uploaded_at_ms: 1,
        ext,
    })
}

fn version(name: &str, v: u32, sha: [u8; 32], retired_at: Option<u64>) -> MetaRecord {
    let ext = match retired_at {
        Some(t) => put_retired(&[], &Retired { retired_at_ms: t }).expect("ext"),
        None => Vec::new(),
    };
    MetaRecord::Workflow(WorkflowVersion {
        name: name.into(),
        version: v,
        module_sha256: sha,
        created_at_ms: 1,
        ext,
        ..WorkflowVersion::default()
    })
}

/// Meta with 4 partitions led by [`LEADERS`] at epoch 1; module A (`pipeline` v1 and
/// `other` v1) retired at T0; module B (`pipeline` v2, active) live.
async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = Arc::new(
        MetaHandle::start(&dir.path().join("meta"), 1)
            .await
            .expect("meta"),
    );
    meta.init_cluster("gc-test", 1, 4, 1, [7; 32])
        .await
        .expect("init");
    for (p, leader) in LEADERS.iter().enumerate() {
        let p = p as u16;
        ok(
            &meta,
            MetaCommand::Put {
                key: MetaKey::Partition(p),
                expected_version: Some(0),
                record: MetaRecord::Partition(PartitionAssignment {
                    partition: p,
                    leader: *leader,
                    epoch: 1,
                    isr: vec![*leader],
                    replicas: vec![*leader],
                    isr_version: 0,
                    min_insync: 1,
                    ext: Vec::new(),
                }),
            },
        )
        .await;
    }
    let wf_a = vec![("pipeline".to_owned(), 1), ("other".to_owned(), 1)];
    for rec in [
        module(A, wf_a, Some(T0)),
        version("pipeline", 1, A, Some(T0)),
        version("other", 1, A, Some(T0)),
        module(B, vec![("pipeline".to_owned(), 2)], None),
        version("pipeline", 2, B, None),
        MetaRecord::Active(ActiveVersion {
            name: "pipeline".into(),
            version: 2,
            ext: Vec::new(),
        }),
    ] {
        ok(
            &meta,
            MetaCommand::Put {
                key: rec.key(),
                expected_version: None,
                record: rec,
            },
        )
        .await;
    }
    Fixture {
        _dir: dir,
        meta,
        clock: ManualClock::new(T0 + GRACE.as_millis() as u64),
        nodes: Arc::new(FakeNodes::default()),
    }
}

impl Fixture {
    fn gc(&self) -> ModuleGc {
        ModuleGc::new(
            Arc::clone(&self.meta),
            Arc::clone(&self.nodes) as Arc<dyn NodeRefs>,
            Arc::new(self.clock.clone()) as Arc<dyn Clock>,
        )
    }

    async fn round(&self) -> ostro_api::phase13::GcRunReport {
        self.gc()
            .round(GcOptions {
                grace: GRACE,
                protect_resumable: true,
            })
            .await
            .expect("round")
    }

    /// `(module A, pipeline@1, other@1)` still present.
    fn a_records(&self) -> (bool, bool, bool) {
        self.meta.view().read(|s| {
            (
                s.module(&A).is_some(),
                s.workflow("pipeline", 1).is_some(),
                s.workflow("other", 1).is_some(),
            )
        })
    }

    fn b_intact(&self) -> bool {
        self.meta.view().read(|s| {
            s.module(&B).is_some()
                && s.workflow("pipeline", 2).is_some()
                && s.active("pipeline").map(|a| a.value.version) == Some(2)
        })
    }

    fn calls(&self) -> usize {
        self.nodes.calls.load(Ordering::SeqCst)
    }
}

#[tokio::test]
async fn deletes_after_two_clean_scans() {
    let f = fixture().await;
    let rep = f.round().await;
    assert_eq!(rep.candidates, vec![hex(&A)]);
    assert_eq!(rep.deleted, vec![hex(&A)], "{rep:?}");
    assert!(rep.referenced.is_empty() && !rep.postponed, "{rep:?}");
    assert_eq!(f.a_records(), (false, false, false));
    assert!(f.b_intact());
    // Two scans, each asking every leader once for exactly its partitions.
    assert_eq!(f.calls(), 6);
    let log = f.nodes.log.lock().unwrap().clone();
    let mut per_scan: BTreeMap<u64, Vec<(PartitionId, Epoch)>> = BTreeMap::new();
    for (node, parts) in &log[..3] {
        per_scan.insert(*node, parts.clone());
    }
    assert_eq!(
        per_scan[&1],
        vec![(PartitionId(0), Epoch(1)), (PartitionId(3), Epoch(1))]
    );
    assert_eq!(per_scan[&2], vec![(PartitionId(1), Epoch(1))]);
    assert_eq!(per_scan[&3], vec![(PartitionId(2), Epoch(1))]);
    assert_eq!(log[..3], log[3..], "scan 2 asks the same pairs");
    // Nothing left to do.
    let rep = f.round().await;
    assert!(rep.candidates.is_empty(), "{rep:?}");
}

#[tokio::test]
async fn reference_between_scans_keeps_the_module() {
    let f = fixture().await;
    // Node 2 reports A from the first call of scan 2 on.
    f.nodes.script.lock().unwrap().refs.push((3, 2, A));
    let rep = f.round().await;
    assert_eq!(rep.referenced, vec![hex(&A)], "{rep:?}");
    assert!(rep.deleted.is_empty(), "{rep:?}");
    assert_eq!(f.a_records(), (true, true, true));
    assert_eq!(f.calls(), 6);
}

#[tokio::test]
async fn reference_in_scan_one_keeps_the_module() {
    let f = fixture().await;
    f.nodes.script.lock().unwrap().refs.push((0, 3, A));
    let rep = f.round().await;
    assert_eq!(rep.referenced, vec![hex(&A)]);
    assert!(rep.deleted.is_empty());
    assert_eq!(f.a_records(), (true, true, true));
}

#[tokio::test]
async fn unreachable_or_uncovered_leader_postpones_the_round() {
    let f = fixture().await;
    f.nodes.script.lock().unwrap().unreachable.insert(3);
    let rep = f.round().await;
    assert!(rep.postponed, "{rep:?}");
    assert!(
        rep.reason.as_deref().unwrap_or("").contains("node 3"),
        "{rep:?}"
    );
    assert!(rep.deleted.is_empty());
    assert_eq!(f.a_records(), (true, true, true));

    let f = fixture().await;
    f.nodes.script.lock().unwrap().uncovering.insert(2);
    let rep = f.round().await;
    assert!(rep.postponed, "{rep:?}");
    assert!(
        rep.reason.as_deref().unwrap_or("").contains("partition 1"),
        "{rep:?}"
    );
    assert_eq!(f.a_records(), (true, true, true));

    // A leaderless partition: nobody can vouch for it.
    let f = fixture().await;
    let cur = f.meta.view().read(|s| s.partition(2).cloned()).expect("p2");
    let mut rec = cur.value.clone();
    rec.leader = 0;
    rec.epoch = 2;
    ok(
        &f.meta,
        MetaCommand::Put {
            key: MetaKey::Partition(2),
            expected_version: Some(cur.version),
            record: MetaRecord::Partition(rec),
        },
    )
    .await;
    let rep = f.round().await;
    assert!(rep.postponed, "{rep:?}");
    assert!(
        rep.reason.as_deref().unwrap_or("").contains("partition 2"),
        "{rep:?}"
    );
    assert_eq!(f.calls(), 0, "no node asked");
    assert_eq!(f.a_records(), (true, true, true));
}

#[tokio::test]
async fn within_grace_is_not_a_candidate() {
    let f = fixture().await;
    f.clock.set(T0 + GRACE.as_millis() as u64 - 1);
    let rep = f.round().await;
    assert!(rep.candidates.is_empty(), "{rep:?}");
    assert_eq!(f.calls(), 0);
    assert_eq!(f.a_records(), (true, true, true));
    f.clock.advance(1);
    let rep = f.round().await;
    assert_eq!(rep.deleted, vec![hex(&A)]);
}

#[tokio::test]
async fn retired_module_with_an_active_version_is_kept() {
    let f = fixture().await;
    // The retire/activate race: `other` v1 (module A) became active after the retirement.
    ok(
        &f.meta,
        MetaCommand::Put {
            key: MetaKey::Active("other".into()),
            expected_version: None,
            record: MetaRecord::Active(ActiveVersion {
                name: "other".into(),
                version: 1,
                ext: Vec::new(),
            }),
        },
    )
    .await;
    let rep = f.round().await;
    assert_eq!(rep.candidates, vec![hex(&A)]);
    assert!(rep.deleted.is_empty(), "{rep:?}");
    assert_eq!(f.a_records(), (true, true, true));
}

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;
    use ostro_server::p13::module_gc::{FP_BEFORE_DELETE, FP_BETWEEN_SCANS, MUTANT_SINGLE_SCAN};

    /// Removes a failpoint configuration even when the test panics.
    struct Cfg(&'static str);
    impl Cfg {
        fn set(name: &'static str) -> Self {
            fail::cfg(name, "return").expect("fail cfg");
            Self(name)
        }
    }
    impl Drop for Cfg {
        fn drop(&mut self) {
            fail::remove(self.0);
        }
    }

    #[tokio::test]
    async fn single_scan_mutant_bites() {
        let f = fixture().await;
        f.nodes.script.lock().unwrap().refs.push((3, 2, A));
        let rep = {
            let _m = Cfg::set(MUTANT_SINGLE_SCAN);
            f.round().await
        };
        // Under the mutant scan 2 never runs: the module of a run that appeared between
        // the scans is deleted (the scenario the double scan exists for).
        assert_eq!(f.calls(), 3, "one scan only");
        assert_eq!(rep.deleted, vec![hex(&A)], "mutant bites: {rep:?}");
        assert_eq!(f.a_records(), (false, false, false));

        // Without it the same script keeps the module.
        let f = fixture().await;
        f.nodes.script.lock().unwrap().refs.push((3, 2, A));
        let rep = f.round().await;
        assert!(rep.deleted.is_empty(), "{rep:?}");
    }

    #[tokio::test]
    async fn failpoints_postpone_the_round() {
        for fp in [FP_BETWEEN_SCANS, FP_BEFORE_DELETE] {
            let f = fixture().await;
            let rep = {
                let _c = Cfg::set(fp);
                f.round().await
            };
            assert!(rep.postponed, "{fp}: {rep:?}");
            assert!(rep.reason.as_deref().unwrap_or("").contains(fp));
            assert_eq!(f.a_records(), (true, true, true), "{fp}");
        }
    }
}
