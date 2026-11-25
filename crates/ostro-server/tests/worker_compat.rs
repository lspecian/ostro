//! Worker version compatibility (13-14; CLUS-06, D-P13-18) over the Phase 7 worker harness
//! (`common/p7_workers.rs`: a real meta group, cluster CA, cluster listener and registry,
//! raw protocol sessions).
//!
//! - `incompatible_worker_never_receives`: the placement predicate over real sessions and
//!   real `Module` / `Workflow` records (ext 101) offers 200 activations of a level-2
//!   module only to the compatible worker; a worker without the module's WIT line and one
//!   with too low a level get none.
//! - `old_level_worker_refused_at_admission`: `version_policy = "compatible"` refuses a
//!   level below `finalized − 1` (and a Hello without any level), admits newer ones, and
//!   closes an admitted session that finalization left behind.
//! - `exact_policy_unchanged`: `"exact"` keeps the Phase 7 equality checks and ignores the
//!   feature level.
//! - `goaway_on_node_drain`: a `Draining` member record sends every session
//!   `GoAway{node_draining}` and refuses new ones; `Welcome.members` lists the `Active`
//!   members only.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7_workers.rs"]
mod p7_workers;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ostro_meta::ext::{
    ClusterVersion, MEMBER_ACTIVE, MEMBER_DRAINING, MemberState, WitRequirement,
    put_cluster_version, put_member_state, put_wit_requirement,
};
use ostro_meta::{
    ClusterInfo, Member, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, ModuleRecord,
    WorkflowVersion,
};
use ostro_partition::Placement;
use ostro_partition::leader::placement::{EntryView, ReservationKind, Reserve};
use ostro_server::config::{PlacementSetting, VersionPolicy};
use ostro_server::workers::placement::{
    MetaCompat, NodePlacement, SpillSource, WorkerSlot, compatible, module_requirement,
};
use ostro_server::workers::registry::{WorkerVersion, WorkerView, level_of_ostro_version};
use ostro_server::workers::selector::PlacementPolicies;
use ostro_server::workers::service::REASON_NODE_DRAINING;
use ostro_worker_proto::v1;
use p7_workers::{Opened, Rig, RigOpts, T, TestWorker, hello, wait_for};
use tokio::sync::Semaphore;

const LINE_0_1: &str = "ostro:runtime@0.1.0";
const LINE_0_2: &str = "ostro:runtime@0.2.0";

// ------------------------------------------------------------------------- helpers

/// Put `record`, creating it or replacing the current version.
async fn put(meta: &MetaHandle, record: MetaRecord) {
    let key = record.key();
    let current = meta.view().read(|s| match &key {
        MetaKey::Cluster => s.cluster().map(|v| v.version),
        MetaKey::Member(id) => s.member(*id).map(|v| v.version),
        MetaKey::Module(sha) => s.module(sha).map(|v| v.version),
        MetaKey::Workflow(name, version) => s.workflow(name, *version).map(|v| v.version),
        _ => None,
    });
    let r = meta
        .propose(MetaCommand::Put {
            key: key.clone(),
            expected_version: current,
            record,
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "put {key:?}: {r:?}");
}

/// Set the cluster's finalized level (ext 98 on `ClusterInfo`).
async fn set_finalized(meta: &MetaHandle, finalized: u32) {
    let mut info: ClusterInfo = meta
        .view()
        .read(|s| s.cluster().map(|c| c.value.clone()))
        .expect("cluster info");
    info.ext = put_cluster_version(
        &info.ext,
        &ClusterVersion {
            finalized,
            hold: None,
            finalized_at_ms: 1,
        },
    )
    .expect("ext 98");
    put(meta, MetaRecord::Cluster(info)).await;
}

/// Register module `sha` (with ext 101 when `req` is set) and workflow `name@1` on it.
async fn register(meta: &MetaHandle, sha: [u8; 32], name: &str, req: Option<(&str, u32)>) {
    let ext = match req {
        Some((line, min_level)) => put_wit_requirement(
            &[],
            &WitRequirement {
                line: line.to_owned(),
                min_level,
            },
        )
        .expect("ext 101"),
        None => Vec::new(),
    };
    put(
        meta,
        MetaRecord::Workflow(WorkflowVersion {
            name: name.to_owned(),
            version: 1,
            module_sha256: sha,
            default_queue: None,
            run_timeout_ms: None,
            created_at_ms: 1,
            module: String::new(),
            ext: Vec::new(),
        }),
    )
    .await;
    put(
        meta,
        MetaRecord::Module(ModuleRecord {
            sha256: sha,
            size: 1,
            language: None,
            workflows: vec![(name.to_owned(), 1)],
            uploaded_at_ms: 1,
            ext,
        }),
    )
    .await;
}

/// A Hello at `level` (0 = absent) advertising `packages`.
fn hello_at(worker_id: &str, slots: u32, level: u32, packages: &[&str]) -> v1::Hello {
    v1::Hello {
        feature_level: level,
        wit_packages: packages.iter().map(|p| (*p).to_owned()).collect(),
        ..hello(worker_id, slots)
    }
}

/// Open a session with `h` (fresh token + identity) and grant `slots` credit.
async fn open(rig: &Rig, h: v1::Hello) -> TestWorker {
    let tok = rig.token(Duration::from_secs(3600)).await;
    let id = rig.identity(&tok.token, &h.worker_id).await;
    let slots = h.slots;
    let w = TestWorker::open(rig.addr, &id, h).await.expect("session");
    w.send(v1::worker_msg::Msg::Credit(v1::Credit {
        free_slots: slots,
    }));
    let sid = w.session_id();
    rig.wait(T, "credit registered", || {
        rig.registry
            .sessions()
            .iter()
            .any(|s| s.session_id == sid && s.free > 0)
    })
    .await;
    w
}

/// Try to open a session with `h`; what came back.
async fn try_open(rig: &Rig, h: v1::Hello) -> Opened {
    let tok = rig.token(Duration::from_secs(3600)).await;
    let id = rig.identity(&tok.token, &h.worker_id).await;
    TestWorker::try_open(rig.addr, &id, h)
        .await
        .expect("connect")
        .1
}

#[derive(Default)]
struct NoSpill(AtomicBool);

impl SpillSource for NoSpill {
    fn spilling(&self, _queue: &str) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    fn note_overflow(&self, _queue: &str, _now_ms: u64) {}
}

fn entry<'a>(queue: &'a str, workflow: &'a str) -> EntryView<'a> {
    EntryView {
        queue,
        workflow,
        tenant: None,
        concurrency_key: "",
        priority: 0,
        first_activation: true,
    }
}

/// The worker a reservation landed on (`None` = blocked; `Some("embedded")`).
fn landed(r: &Reserve) -> Option<String> {
    match r {
        Reserve::Blocked => None,
        Reserve::Granted(r) => match r.kind() {
            ReservationKind::Embedded => Some("embedded".into()),
            ReservationKind::Worker => Some(
                r.downcast_ref::<WorkerSlot>()
                    .expect("worker slot")
                    .worker_id()
                    .to_owned(),
            ),
        },
    }
}

// ------------------------------------------------------------------------- tests

#[test]
fn predicate_and_version_parsing() {
    // Level from ostro_version for workers that predate Hello.feature_level.
    assert_eq!(level_of_ostro_version("0.1.0"), Some(1));
    assert_eq!(level_of_ostro_version("0.1.7-dev+abc"), Some(1));
    assert_eq!(level_of_ostro_version("0.0.0-other"), None);
    assert_eq!(level_of_ostro_version("garbage"), None);
    assert_eq!(level_of_ostro_version(""), None);

    let v = WorkerVersion::from_hello(&hello_at("w", 1, 0, &[LINE_0_1, "wasi:io@0.2.0"]))
        .expect("derived level");
    assert_eq!(v.level, 1);
    assert_eq!(v.wit_lines.len(), 1);
    let mut h = hello_at("w", 1, 0, &[LINE_0_1]);
    h.ostro_version = "nope".into();
    let e = WorkerVersion::from_hello(&h).expect_err("no level");
    assert!(e.contains("version"), "{e}");

    // ext 101 absent = {0.1, 1}; present = its value; undecodable = nobody.
    let mut rec = ModuleRecord::default();
    let req = module_requirement(&rec).expect("default");
    assert_eq!((req.line.to_string(), req.min_level), ("0.1".into(), 1));
    rec.ext = put_wit_requirement(
        &[],
        &WitRequirement {
            line: "0.1".into(),
            min_level: 2,
        },
    )
    .expect("ext");
    let req2 = module_requirement(&rec).expect("ext 101");
    assert_eq!(req2.min_level, 2);
    rec.ext = put_wit_requirement(
        &[],
        &WitRequirement {
            line: "x.y".into(),
            min_level: 2,
        },
    )
    .expect("ext");
    assert!(module_requirement(&rec).is_none());

    let labels = BTreeMap::new();
    let view = |v: &WorkerVersion| -> bool {
        let w = WorkerView {
            worker_id: "w",
            labels: &labels,
            free: 1,
            version: v,
        };
        compatible(&w, &req2)
    };
    let lv = |level, pkgs: &[&str]| WorkerVersion::lenient(&hello_at("w", 1, level, pkgs));
    assert!(view(&lv(2, &[LINE_0_1])));
    assert!(view(&lv(3, &[LINE_0_1, LINE_0_2])));
    assert!(!view(&lv(1, &[LINE_0_1])), "level too low");
    assert!(!view(&lv(2, &[LINE_0_2])), "line missing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incompatible_worker_never_receives() {
    let rig = Rig::start(RigOpts::default()).await;
    let level2 = [0x22; 32];
    let level1 = [0x11; 32];
    register(&rig.meta, level2, "needs2", Some(("0.1", 2))).await;
    register(&rig.meta, level1, "plain", None).await;

    let _good = open(&rig, hello_at("good", 4, 2, &[LINE_0_1])).await;
    let _noline = open(&rig, hello_at("noline", 4, 2, &[LINE_0_2])).await;
    let _old = open(&rig, hello_at("old", 4, 1, &[LINE_0_1])).await;
    assert_eq!(rig.registry.connected(), 3);

    let view = rig.meta.view().clone();
    let placement = NodePlacement::new(
        Arc::new(Semaphore::new(0)),
        Arc::clone(&rig.registry),
        Arc::new(NoSpill::default()),
        Arc::new(PlacementPolicies::new(
            view.clone(),
            PlacementSetting::Workers,
        )),
        rig.clock.clone(),
    )
    .with_compat(Arc::new(MetaCompat::new(view)));

    // 200 activations of the level-2 module: all on the compatible worker.
    let mut got: BTreeMap<String, u32> = BTreeMap::new();
    for _ in 0..200 {
        let r = placement.reserve(&entry("q", "needs2"));
        let who = landed(&r).expect("a compatible worker has credit");
        *got.entry(who).or_default() += 1;
        drop(r);
    }
    assert_eq!(got.get("good"), Some(&200), "{got:?}");
    assert_eq!(got.len(), 1, "{got:?}");

    // A level-1 module runs on any worker with the 0.1 line, never on `noline`.
    let mut got: BTreeMap<String, u32> = BTreeMap::new();
    for _ in 0..200 {
        let r = placement.reserve(&entry("q", "plain"));
        *got.entry(landed(&r).expect("credit")).or_default() += 1;
    }
    assert!(!got.contains_key("noline"), "{got:?}");
    assert!(
        got.contains_key("good") && got.contains_key("old"),
        "{got:?}"
    );

    // Holding the compatible worker's whole credit: the level-2 activation waits (a
    // `workers` queue blocks), it is never offered to the others.
    let held: Vec<Reserve> = (0..4)
        .map(|_| placement.reserve(&entry("q", "needs2")))
        .collect();
    assert!(held.iter().all(|r| landed(r).as_deref() == Some("good")));
    assert!(landed(&placement.reserve(&entry("q", "needs2"))).is_none());
    drop(held);

    // Finalization moves the bar: at finalized 3 the level-1 worker is no longer current
    // and the meta watch sends it away.
    set_finalized(&rig.meta, 3).await;
    wait_for(T, "old session closed", || {
        rig.registry.sessions().iter().all(|s| s.worker_id != "old")
    })
    .await;
    for _ in 0..50 {
        let r = placement.reserve(&entry("q", "plain"));
        assert_ne!(landed(&r).as_deref(), Some("old"));
    }
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_takes_what_no_worker_can_run() {
    let rig = Rig::start(RigOpts::default()).await;
    register(&rig.meta, [0x33; 32], "needs2", Some(("0.1", 2))).await;
    let _old = open(&rig, hello_at("old", 4, 1, &[LINE_0_1])).await;
    let view = rig.meta.view().clone();
    let spill = Arc::new(NoSpill::default());
    spill.0.store(true, Ordering::SeqCst);
    let placement = NodePlacement::new(
        Arc::new(Semaphore::new(1)),
        Arc::clone(&rig.registry),
        spill as Arc<dyn SpillSource>,
        Arc::new(PlacementPolicies::new(view.clone(), PlacementSetting::Auto)),
        rig.clock.clone(),
    )
    .with_compat(Arc::new(MetaCompat::new(view)));
    // `auto` spilling: embedded first, then overflow only to a compatible worker (none).
    let r1 = placement.reserve(&entry("q", "needs2"));
    assert_eq!(landed(&r1).as_deref(), Some("embedded"));
    assert!(landed(&placement.reserve(&entry("q", "needs2"))).is_none());
    drop(r1);
    assert_eq!(
        landed(&placement.reserve(&entry("q", "needs2"))).as_deref(),
        Some("embedded")
    );
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_level_worker_refused_at_admission() {
    let rig = Rig::start(RigOpts::default()).await;
    // finalized 1 (absent tag 98): level 1 and 2 are admitted, ostro / wasmtime may differ.
    let mut h = hello_at("w1", 1, 2, &[LINE_0_1]);
    h.ostro_version = "0.1.9".into();
    h.wasmtime_version = "0.0.1".into();
    assert!(matches!(try_open(&rig, h).await, Opened::Welcome));
    assert!(matches!(
        try_open(&rig, hello_at("w2", 1, 0, &[LINE_0_1])).await,
        Opened::Welcome
    ));
    // No feature level and no parsable ostro_version: refused.
    let mut h = hello_at("w3", 1, 0, &[LINE_0_1]);
    h.ostro_version = "0.0.0-other".into();
    match try_open(&rig, h).await {
        Opened::GoAway(reason) => assert!(reason.contains("version"), "{reason}"),
        o => panic!("expected GoAway, got {o:?}"),
    }
    // The event codec still has to match.
    let mut h = hello_at("w4", 1, 2, &[LINE_0_1]);
    h.event_codec_ver += 1;
    match try_open(&rig, h).await {
        Opened::GoAway(reason) => assert!(reason.contains("event_codec_ver"), "{reason}"),
        o => panic!("expected GoAway, got {o:?}"),
    }

    // finalized 3: level 2 is the floor (finalized − 1).
    let mut admitted = open(&rig, hello_at("w5", 1, 1, &[LINE_0_1])).await;
    set_finalized(&rig.meta, 3).await;
    match admitted.next(T).await {
        Some(Ok(v1::server_msg::Msg::GoAway(g))) => {
            assert!(g.reason.contains("version too old"), "{}", g.reason)
        }
        other => panic!("expected GoAway after finalization, got {other:?}"),
    }
    match try_open(&rig, hello_at("w6", 1, 1, &[LINE_0_1])).await {
        Opened::GoAway(reason) => {
            assert!(reason.contains("version too old"), "{reason}");
            assert!(reason.contains("finalized at level 3"), "{reason}");
        }
        o => panic!("expected GoAway, got {o:?}"),
    }
    assert!(matches!(
        try_open(&rig, hello_at("w7", 1, 2, &[LINE_0_1])).await,
        Opened::Welcome
    ));
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_policy_unchanged() {
    let rig = Rig::start(RigOpts {
        version_policy: VersionPolicy::Exact,
        ..RigOpts::default()
    })
    .await;
    // Phase 7: same ostro + wasmtime version (require_same_version), whatever the level.
    let mut h = hello_at("w1", 1, 2, &[LINE_0_1]);
    h.wasmtime_version = "0.0.1".into();
    match try_open(&rig, h).await {
        Opened::GoAway(reason) => assert!(reason.contains("wasmtime_version"), "{reason}"),
        o => panic!("expected GoAway, got {o:?}"),
    }
    let mut h = hello_at("w2", 1, 2, &[LINE_0_1]);
    h.ostro_version = "0.1.9".into();
    match try_open(&rig, h).await {
        Opened::GoAway(reason) => assert!(reason.contains("ostro_version"), "{reason}"),
        o => panic!("expected GoAway, got {o:?}"),
    }
    // The level plays no part in admission under `exact`.
    set_finalized(&rig.meta, 3).await;
    assert!(matches!(
        try_open(&rig, hello_at("w3", 1, 1, &[LINE_0_1])).await,
        Opened::Welcome
    ));
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn goaway_on_node_drain() {
    let rig = Rig::start(RigOpts::default()).await;
    let member = |id: u64, state: u8, addr: &str| Member {
        node_id: id,
        addr: format!("127.0.0.{id}:7700"),
        joined_at_ms: 1,
        cluster_addr: Some(addr.to_owned()),
        repl_addr: None,
        ext: put_member_state(
            &[],
            &MemberState {
                state,
                changed_at_ms: 1,
            },
        )
        .expect("ext 97"),
    };
    put(
        &rig.meta,
        MetaRecord::Member(member(1, MEMBER_ACTIVE, "self:1")),
    )
    .await;
    put(
        &rig.meta,
        MetaRecord::Member(member(2, MEMBER_ACTIVE, "10.0.0.2:17701")),
    )
    .await;
    put(
        &rig.meta,
        MetaRecord::Member(member(3, MEMBER_DRAINING, "10.0.0.3:17701")),
    )
    .await;

    let mut w = open(&rig, hello_at("w1", 1, 2, &[LINE_0_1])).await;
    // Welcome: this node's own endpoint + the other Active member (not the draining one).
    let members: Vec<(u64, String)> = w
        .welcome
        .members
        .iter()
        .map(|m| (m.node_id, m.endpoint.clone()))
        .collect();
    assert_eq!(
        members,
        vec![(1, rig.addr.to_string()), (2, "10.0.0.2:17701".to_owned())]
    );

    // This node starts draining: the session is sent away, new ones are refused.
    put(
        &rig.meta,
        MetaRecord::Member(member(1, MEMBER_DRAINING, "self:1")),
    )
    .await;
    match w.next(T).await {
        Some(Ok(v1::server_msg::Msg::GoAway(g))) => assert_eq!(g.reason, REASON_NODE_DRAINING),
        other => panic!("expected GoAway, got {other:?}"),
    }
    wait_for(T, "session closed", || rig.registry.connected() == 0).await;
    match try_open(&rig, hello_at("w2", 1, 2, &[LINE_0_1])).await {
        Opened::GoAway(reason) => assert_eq!(reason, REASON_NODE_DRAINING),
        o => panic!("expected GoAway, got {o:?}"),
    }
    rig.stop().await;
}
