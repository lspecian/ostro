//! 13-04 Task 1 (CLUS-06, D-P13-3/4): the feature gate, the pure finalization rule, the
//! auto-finalizer and the operator calls.
//!
//! - `finalize_decision` table: Removed members ignored, a missing advertisement blocks,
//!   a hold blocks, a dead member blocks, auto-finalize off waits, never decreases, single
//!   node finalizes;
//! - on a real single-voter meta group: `require` refuses until finalized, the finalizer
//!   finalizes once the node advertised level 2, hold / release / manual finalize;
//! - on a 3-voter group over mTLS: manual finalize refuses while a member is down and
//!   succeeds once it is back; every voter sees the new level.

mod support_meta;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_cluster::compat::{self, AdvertiseOutcome, FeatureNotEnabled};
use ostro_cluster::contracts::{ClusterStatusView, MemberView};
use ostro_cluster::upgrade::{
    self, FinalizeDecision, FinalizerConfig, UpgradeError, finalize_decision,
};
use ostro_meta::MetaHandle;
use ostro_meta::ext::{
    EXT_CLUSTER_VERSION, EXT_TAG_TEST, MEMBER_ACTIVE, MEMBER_JOINING, MEMBER_REMOVED, NodeVersion,
    ext_get, ext_put,
};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_partition::ManualClock;
use ostro_types::compat::{Feature, LEVEL, LEVEL_MIN};
use support_meta::MetaCluster;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------- pure rule

fn nv(level_max: u32) -> NodeVersion {
    NodeVersion {
        ostro_version: "0.1.0".into(),
        level_min: LEVEL_MIN,
        level_max,
        wasmtime: "w".into(),
        event_codec_ver: 1,
        wit_lines: vec!["0.1".into()],
        reported_at_ms: 1,
    }
}

fn member(id: u64, state: u8, level: Option<u32>, live: bool) -> MemberView {
    MemberView {
        node_id: id,
        addr: format!("n{id}"),
        cluster_addr: None,
        state,
        version: level.map(nv),
        live,
    }
}

fn status(finalized: u32, hold: Option<u32>, members: Vec<MemberView>) -> ClusterStatusView {
    ClusterStatusView {
        members,
        finalized,
        hold,
        ..ClusterStatusView::default()
    }
}

#[test]
fn decision_table() {
    use FinalizeDecision::*;
    let a = MEMBER_ACTIVE;
    let cases: Vec<(&str, ClusterStatusView, bool, FinalizeDecision)> = vec![
        (
            "single node finalizes",
            status(1, None, vec![member(1, a, Some(2), true)]),
            true,
            Finalize { to: 2 },
        ),
        (
            "all three at 2",
            status(
                1,
                None,
                vec![
                    member(1, a, Some(2), true),
                    member(2, a, Some(2), true),
                    member(3, MEMBER_JOINING, Some(2), true),
                ],
            ),
            true,
            Finalize { to: 2 },
        ),
        (
            "one member still at 1",
            status(
                1,
                None,
                vec![member(1, a, Some(2), true), member(2, a, Some(1), true)],
            ),
            true,
            Stay,
        ),
        (
            "Removed members ignored (even dead, old and unreported)",
            status(
                1,
                None,
                vec![
                    member(1, a, Some(2), true),
                    member(2, MEMBER_REMOVED, Some(1), false),
                    member(3, MEMBER_REMOVED, None, false),
                ],
            ),
            true,
            Finalize { to: 2 },
        ),
        (
            "a missing advertisement blocks",
            status(
                1,
                None,
                vec![member(1, a, Some(2), true), member(2, a, None, true)],
            ),
            true,
            Waiting {
                reason: "member 2 has not reported its version".into(),
            },
        ),
        (
            "a hold blocks",
            status(
                1,
                Some(1),
                vec![member(1, a, Some(2), true), member(2, a, Some(2), true)],
            ),
            true,
            Held { at: 1 },
        ),
        (
            "a dead member blocks",
            status(
                1,
                None,
                vec![member(1, a, Some(2), true), member(2, a, Some(2), false)],
            ),
            true,
            Waiting {
                reason: "member 2 is not live".into(),
            },
        ),
        (
            "never decreases",
            status(2, None, vec![member(1, a, Some(1), true)]),
            true,
            Stay,
        ),
        (
            "already final",
            status(2, None, vec![member(1, a, Some(2), true)]),
            true,
            Stay,
        ),
        (
            "no members",
            status(1, None, vec![member(9, MEMBER_REMOVED, Some(2), true)]),
            true,
            Waiting {
                reason: "no active members".into(),
            },
        ),
    ];
    for (name, st, auto, want) in cases {
        assert_eq!(finalize_decision(&st, auto), want, "{name}");
    }
    // Auto-finalize off: ready, but waits for the operator.
    let st = status(1, None, vec![member(1, a, Some(2), true)]);
    assert!(
        matches!(finalize_decision(&st, false), Waiting { reason } if reason.contains("auto-finalize is off"))
    );
}

#[test]
fn upgrade_view_reads_states_versions_and_liveness() {
    // Built from a real MetaState in the meta-backed tests below; here: the defaults.
    let st = ostro_meta::MetaState::default();
    let v = upgrade::upgrade_view(&st, &BTreeSet::new());
    assert!(v.members.is_empty());
    assert_eq!((v.finalized, v.hold), (LEVEL_MIN, None));
}

// ---------------------------------------------------------------- single voter

async fn single() -> (tempfile::TempDir, Arc<MetaHandle>) {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta start");
    meta.init_cluster("c-13-04", 1, 4, 1_000, [7; 32])
        .await
        .expect("init cluster");
    (dir, Arc::new(meta))
}

async fn until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn finalized(meta: &MetaHandle) -> u32 {
    meta.view().read(compat::finalized_level)
}

#[tokio::test(flavor = "multi_thread")]
async fn gate_refuses_until_the_single_node_finalizes() {
    let (_dir, meta) = single().await;
    // A Phase 12 cluster record carries no tag 98: finalized = LEVEL_MIN.
    assert_eq!(finalized(&meta), LEVEL_MIN);
    for f in Feature::ALL {
        let e = compat::require(meta.view(), f, LEVEL).unwrap_err();
        assert_eq!(
            e,
            FeatureNotEnabled {
                feature: f,
                needed: 2,
                finalized: 1
            }
        );
    }
    // Keep an unknown cluster ext tag around: finalization must preserve it.
    let cur = meta.view().read(|s| s.cluster().cloned()).unwrap();
    let mut rec = cur.value.clone();
    rec.ext = ext_put(&rec.ext, EXT_TAG_TEST, b"keep").unwrap();
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: Some(cur.version),
            record: MetaRecord::Cluster(rec),
        })
        .await
        .unwrap();
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");

    // Unreported: the finalizer has nothing to do.
    let v = meta.view().read(|s| upgrade::upgrade_view(s, &[1].into()));
    assert_eq!(finalize_decision(&v, true), FinalizeDecision::Stay);

    let clock = Arc::new(ManualClock::new(5_000));
    let stop = CancellationToken::new();
    let fin = upgrade::start_finalizer_with(
        Arc::clone(&meta),
        FinalizerConfig {
            auto_finalize: true,
            interval: Duration::from_millis(50),
            live_window: Duration::from_millis(500),
        },
        clock.clone(),
        stop.clone(),
    );
    // Advertise level 2; the finalizer then finalizes.
    let o = compat::advertise_once(
        &meta,
        1,
        &nv(2),
        ostro_partition::Clock::now_ms(&*clock),
        600_000,
    )
    .await
    .unwrap();
    assert_eq!(o, AdvertiseOutcome::Written);
    until("finalized 2", Duration::from_secs(20), || {
        finalized(&meta) == 2
    })
    .await;
    let cv = meta.view().read(compat::cluster_version);
    assert_eq!(
        (cv.finalized, cv.hold, cv.finalized_at_ms),
        (2, None, 5_000)
    );
    for f in Feature::ALL {
        compat::require(meta.view(), f, LEVEL).expect("finalized: allowed");
        assert!(
            compat::require(meta.view(), f, 1).is_err(),
            "a level-1 process never emits, even after finalization"
        );
    }
    let ext = meta.view().read(|s| s.cluster().unwrap().value.ext.clone());
    assert_eq!(ext_get(&ext, EXT_TAG_TEST), Some(&b"keep"[..]));
    assert!(ext_get(&ext, EXT_CLUSTER_VERSION).is_some());

    stop.cancel();
    fin.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn hold_release_and_manual_finalize() {
    let (_dir, meta) = single().await;
    // Hold first (at level 1): manual finalize refuses, auto would hold.
    let cv = upgrade::hold(&meta, 10).await.unwrap();
    assert_eq!((cv.finalized, cv.hold), (1, Some(1)));
    assert_eq!(
        upgrade::hold(&meta, 11).await.unwrap().hold,
        Some(1),
        "idempotent"
    );
    compat::advertise_once(&meta, 1, &nv(2), 12, 600_000)
        .await
        .unwrap();
    let e = upgrade::finalize(&meta, 13).await.unwrap_err();
    assert!(
        matches!(&e, UpgradeError::NotReady(m) if m.contains("hold")),
        "{e}"
    );
    let v = meta.view().read(|s| upgrade::upgrade_view(s, &[1].into()));
    assert_eq!(
        finalize_decision(&v, true),
        FinalizeDecision::Held { at: 1 }
    );
    assert_eq!(finalized(&meta), 1);

    // Release, then a manual finalize (auto-finalize does not matter for the manual call).
    assert_eq!(upgrade::release(&meta, 14).await.unwrap().hold, None);
    assert_eq!(
        upgrade::release(&meta, 15).await.unwrap().hold,
        None,
        "idempotent"
    );
    let cv = upgrade::finalize(&meta, 16).await.unwrap();
    assert_eq!((cv.finalized, cv.hold, cv.finalized_at_ms), (2, None, 16));
    // Again: already at the common level, unchanged.
    let cv2 = upgrade::finalize(&meta, 17).await.unwrap();
    assert_eq!(cv2, cv);
    // A hold after finalization pins the new level.
    assert_eq!(upgrade::hold(&meta, 18).await.unwrap().hold, Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_finalize_off_never_finalizes() {
    let (_dir, meta) = single().await;
    compat::advertise_once(&meta, 1, &nv(2), 1, 600_000)
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let fin = upgrade::start_finalizer_with(
        Arc::clone(&meta),
        FinalizerConfig {
            auto_finalize: false,
            interval: Duration::from_millis(20),
            live_window: Duration::from_millis(200),
        },
        Arc::new(ManualClock::new(1)),
        stop.clone(),
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(finalized(&meta), 1);
    stop.cancel();
    fin.await.unwrap();
}

// ---------------------------------------------------------------- three voters

#[tokio::test(flavor = "multi_thread")]
async fn three_voters_finalize_only_with_every_member_live() {
    let mut c = MetaCluster::start(&[1, 2, 3]).await;
    let leader = c.leader(Duration::from_secs(30)).await;
    c.handle(leader)
        .init_cluster("c-13-04", leader, 4, 1_000, [7; 32])
        .await
        .expect("init");
    for id in [1, 2, 3] {
        let me = ostro_meta::Member {
            node_id: id,
            addr: format!("127.0.0.1:{}", 9000 + id),
            joined_at_ms: 1,
            cluster_addr: None,
            repl_addr: None,
            ext: Vec::new(),
        };
        ostro_cluster::members::upsert_self(c.handle(id), me)
            .await
            .expect("upsert self");
        let o = compat::advertise_once(c.handle(id), id, &nv(2), 2, 600_000)
            .await
            .expect("advertise");
        assert_eq!(o, AdvertiseOutcome::Written, "node {id}");
    }
    c.settle().await;

    // Stop a follower and wait until the leader no longer hears from it.
    let down = c.follower(leader);
    c.stop(down).await;
    tokio::time::sleep(upgrade::LIVE_WINDOW + Duration::from_millis(500)).await;
    let e = upgrade::finalize(c.handle(leader), 3).await.unwrap_err();
    assert_eq!(
        e,
        UpgradeError::NotReady(format!("member {down} is not live")),
        "a dead member blocks finalization"
    );
    assert_eq!(finalized(c.handle(leader)), 1);

    // Only the leader can judge liveness.
    let other = *c
        .voters
        .iter()
        .find(|&&i| i != leader && i != down)
        .unwrap();
    assert!(matches!(
        upgrade::finalize(c.handle(other), 3).await,
        Err(UpgradeError::NotReady(_))
    ));

    c.restart(down).await;
    let leader = c.leader(Duration::from_secs(30)).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let cv = loop {
        match upgrade::finalize(c.handle(leader), 4).await {
            Ok(cv) if cv.finalized == 2 => break cv,
            r => {
                assert!(Instant::now() < deadline, "finalize never succeeded: {r:?}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };
    assert_eq!(cv.hold, None);
    c.settle().await;
    for id in [1, 2, 3] {
        assert_eq!(finalized(c.handle(id)), 2, "node {id}");
        compat::require(c.handle(id).view(), Feature::AlterReplicas, LEVEL).expect("allowed");
    }
    c.stop_all().await;
}
