//! 13-04 Task 3 (CLUS-06, D-P13-5/7): NodeVersion advertisement, the boot fence (before
//! `Node::open`), the running fence and the level source, on in-process 1- and 3-voter
//! metadata groups.
//!
//! - boot fence: a data dir finalized at 2 refuses a level-1 binary with "data finalized by
//!   a newer Ostro", and no file under `state/` or `wal/` changes (mtime/size snapshot);
//!   an unfinalized (Phase 12) cluster lets a level-1 binary start (downgrade window);
//! - 3 voters: every voter's fence sees the finalized level;
//! - running fence: a level-1 node fail-stops once the cluster finalizes at 2, exactly once;
//! - advertiser: RMW keeps unknown member ext tags, writes only on change or refresh, and
//!   restores a dropped tag 96;
//! - `MetaLevels`: the override wins, else `ostro_types::compat::level_max()`.

mod support_meta;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use ostro_cluster::compat::{self, AdvertiseOutcome, AdvertiserConfig, CompatError, MetaLevels};
use ostro_cluster::contracts::LevelSource;
use ostro_cluster::upgrade;
use ostro_meta::ext::{EXT_TAG_TEST, NodeVersion, ext_get, ext_put, get_node_version};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, QueueConfig};
use ostro_partition::{Clock, ManualClock};
use ostro_types::compat::{LEVEL, LEVEL_MIN};
use support_meta::MetaCluster;
use tokio_util::sync::CancellationToken;

fn nv(level_max: u32) -> NodeVersion {
    NodeVersion {
        ostro_version: "0.1.0".into(),
        level_min: LEVEL_MIN,
        level_max,
        wasmtime: "w".into(),
        event_codec_ver: 1,
        wit_lines: vec!["0.1".into()],
        reported_at_ms: 0,
    }
}

async fn until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn init_single(meta_dir: &Path) -> MetaHandle {
    let meta = MetaHandle::start(meta_dir, 1).await.expect("meta start");
    meta.init_cluster("c-13-04", 1, 4, 1_000, [7; 32])
        .await
        .expect("init");
    meta
}

/// Finalize a single-node cluster at level 2 (advertise, then the manual call).
async fn finalize_single(meta: &MetaHandle) {
    compat::advertise_once(meta, 1, &nv(2), 10, 600_000)
        .await
        .expect("advertise");
    let cv = upgrade::finalize(meta, 11).await.expect("finalize");
    assert_eq!(cv.finalized, 2);
}

/// `(mtime, len)` of every file under `dirs`, keyed by path.
fn snapshot(dirs: &[PathBuf]) -> BTreeMap<PathBuf, (SystemTime, u64)> {
    fn walk(d: &Path, out: &mut BTreeMap<PathBuf, (SystemTime, u64)>) {
        for e in std::fs::read_dir(d).expect("read_dir") {
            let e = e.expect("entry");
            let md = e.metadata().expect("metadata");
            if md.is_dir() {
                walk(&e.path(), out);
            } else {
                out.insert(e.path(), (md.modified().expect("mtime"), md.len()));
            }
        }
    }
    let mut out = BTreeMap::new();
    for d in dirs {
        walk(d, &mut out);
    }
    out
}

// ---------------------------------------------------------------- boot fence

#[tokio::test(flavor = "multi_thread")]
async fn boot_fence_refuses_a_downgrade_after_finalization_before_partition_files() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let meta_dir = data.join("meta");
    {
        let meta = init_single(&meta_dir).await;
        finalize_single(&meta).await;
        meta.stop().await;
    }
    // Partition data left by the (newer) binary.
    let state = data.join("state");
    let wal = data.join("wal");
    for (dir, files) in [
        (&state, ["p0/MANIFEST", "p1/MANIFEST"]),
        (&wal, ["p0/00000001.seg", "p1/00000001.seg"]),
    ] {
        for f in files {
            let p = dir.join(f);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, b"partition bytes").unwrap();
        }
    }
    let before = snapshot(&[state.clone(), wal.clone()]);
    // Give a write a chance to show up as a different mtime.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The old binary (level_max 1) restarts: meta opens, the fence refuses.
    let meta = MetaHandle::start(&meta_dir, 1).await.expect("meta restart");
    let e = compat::boot_fence(&meta, 1).await.unwrap_err();
    assert_eq!(
        e,
        CompatError::FinalizedAbove {
            finalized: 2,
            level_max: 1
        }
    );
    assert!(
        e.to_string().contains("data finalized by a newer Ostro"),
        "{e}"
    );
    // The new binary passes.
    compat::boot_fence(&meta, LEVEL)
        .await
        .expect("level 2 passes");
    meta.stop().await;

    assert_eq!(
        snapshot(&[state, wal]),
        before,
        "the boot fence must not touch partition files"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unfinalized_cluster_allows_a_level_1_binary() {
    let tmp = tempfile::tempdir().unwrap();
    let meta = init_single(&tmp.path().join("meta")).await;
    // Phase 12 cluster record (no tag 98) = LEVEL_MIN: the downgrade window is open.
    compat::boot_fence(&meta, 1).await.expect("level 1 passes");
    compat::boot_fence(&meta, LEVEL)
        .await
        .expect("level 2 passes");
    // Before init there is no cluster record at all: also LEVEL_MIN.
    let tmp2 = tempfile::tempdir().unwrap();
    let blank = MetaHandle::start(&tmp2.path().join("meta"), 1)
        .await
        .unwrap();
    compat::boot_fence(&blank, 1).await.expect("blank passes");
    blank.stop().await;
    meta.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn three_voters_fence_on_the_finalized_level() {
    let mut c = MetaCluster::start(&[1, 2, 3]).await;
    let leader = c.leader(Duration::from_secs(30)).await;
    c.handle(leader)
        .init_cluster("c-13-04", leader, 4, 1_000, [7; 32])
        .await
        .expect("init");
    for id in [1, 2, 3] {
        let me = ostro_meta::Member {
            node_id: id,
            addr: format!("127.0.0.1:{}", 9100 + id),
            joined_at_ms: 1,
            cluster_addr: None,
            repl_addr: None,
            ext: Vec::new(),
        };
        ostro_cluster::members::upsert_self(c.handle(id), me)
            .await
            .expect("upsert");
        compat::advertise_once(c.handle(id), id, &nv(2), 2, 600_000)
            .await
            .expect("advertise");
    }
    c.settle().await;
    for id in [1, 2, 3] {
        compat::boot_fence(c.handle(id), 1)
            .await
            .expect("unfinalized: level 1 passes");
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match upgrade::finalize(c.handle(leader), 3).await {
            Ok(cv) if cv.finalized == 2 => break,
            r => {
                assert!(Instant::now() < deadline, "finalize: {r:?}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    c.settle().await;
    for id in [1, 2, 3] {
        let e = compat::boot_fence(c.handle(id), 1).await.unwrap_err();
        assert!(
            matches!(
                e,
                CompatError::FinalizedAbove {
                    finalized: 2,
                    level_max: 1
                }
            ),
            "node {id}: {e}"
        );
        compat::boot_fence(c.handle(id), 2).await.expect("level 2");
    }
    c.stop_all().await;
}

// ---------------------------------------------------------------- running fence

#[tokio::test(flavor = "multi_thread")]
async fn running_fence_stops_an_unupgraded_node_once() {
    let tmp = tempfile::tempdir().unwrap();
    let meta = Arc::new(init_single(&tmp.path().join("meta")).await);
    let fatal: Arc<Mutex<Vec<String>>> = Arc::default();
    let hook = {
        let fatal = Arc::clone(&fatal);
        Arc::new(move |m: String| fatal.lock().unwrap().push(m))
            as Arc<dyn Fn(String) + Send + Sync>
    };
    let ok_hook = Arc::new(|m: String| -> () { panic!("level-2 fence fired: {m}") })
        as Arc<dyn Fn(String) + Send + Sync>;
    let stop = CancellationToken::new();
    let old = compat::start_running_fence(Arc::clone(&meta), 1, hook, stop.clone());
    let new = compat::start_running_fence(Arc::clone(&meta), 2, ok_hook, stop.clone());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        fatal.lock().unwrap().is_empty(),
        "unfinalized: no fail-stop"
    );

    finalize_single(&meta).await;
    until("running fence fired", Duration::from_secs(10), || {
        !fatal.lock().unwrap().is_empty()
    })
    .await;
    // The task ends after firing; later meta changes do not fire again.
    tokio::time::timeout(Duration::from_secs(5), old)
        .await
        .expect("fence task ended")
        .unwrap();
    put_queue(&meta, "after").await;
    let msgs = fatal.lock().unwrap().clone();
    assert_eq!(msgs.len(), 1, "{msgs:?}");
    assert!(
        msgs[0].contains("data finalized by a newer Ostro"),
        "{}",
        msgs[0]
    );
    stop.cancel();
    new.await.unwrap();
}

async fn put_queue(meta: &MetaHandle, name: &str) {
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Queue(name.into()),
            expected_version: None,
            record: MetaRecord::Queue(QueueConfig {
                name: name.into(),
                ..QueueConfig::default()
            }),
        })
        .await
        .unwrap();
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

// ---------------------------------------------------------------- advertiser

fn stored(meta: &MetaHandle) -> (u64, Option<NodeVersion>, Vec<u8>) {
    meta.view().read(|s| {
        let m = s.member(1).expect("member 1");
        (
            m.version,
            get_node_version(&m.value.ext).unwrap(),
            m.value.ext.clone(),
        )
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn advertiser_upserts_only_on_change_and_keeps_unknown_tags() {
    let tmp = tempfile::tempdir().unwrap();
    let meta = Arc::new(init_single(&tmp.path().join("meta")).await);
    // An unknown member ext tag written by someone else.
    let (ver, _, ext) = stored(&meta);
    let mut rec = meta.view().read(|s| s.member(1).unwrap().value.clone());
    rec.ext = ext_put(&ext, EXT_TAG_TEST, b"other").unwrap();
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Member(1),
            expected_version: Some(ver),
            record: MetaRecord::Member(rec.clone()),
        })
        .await
        .unwrap();
    assert!(matches!(r, MetaResult::Ok { .. }));

    let clock = Arc::new(ManualClock::new(50_000));
    let stop = CancellationToken::new();
    let task = compat::start_advertiser_with(
        Arc::clone(&meta),
        1,
        nv(2),
        clock.clone(),
        AdvertiserConfig {
            refresh: Duration::from_secs(600),
            retry: Duration::from_millis(50),
        },
        stop.clone(),
    );
    until("advertised", Duration::from_secs(10), || {
        stored(&meta).1.is_some()
    })
    .await;
    let (v1, got, ext) = stored(&meta);
    assert_eq!(
        got,
        Some(NodeVersion {
            reported_at_ms: 50_000,
            ..nv(2)
        })
    );
    assert_eq!(
        ext_get(&ext, EXT_TAG_TEST),
        Some(&b"other"[..]),
        "RMW kept tag 15"
    );

    // Unrelated meta changes and a small clock step: nothing is rewritten.
    clock.advance(1_000);
    put_queue(&meta, "q1").await;
    put_queue(&meta, "q2").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        stored(&meta).0,
        v1,
        "unchanged report must not be rewritten"
    );

    // Past the refresh period, the next check refreshes reported_at_ms.
    clock.advance(600_000);
    put_queue(&meta, "q3").await;
    until("refreshed", Duration::from_secs(10), || {
        stored(&meta).0 > v1
    })
    .await;
    let (v2, got, _) = stored(&meta);
    assert_eq!(got.unwrap().reported_at_ms, clock.now_ms());

    // Someone rewrites the member without tag 96: the advertiser restores it.
    let mut rec = meta.view().read(|s| s.member(1).unwrap().value.clone());
    rec.ext = ext_put(&[], EXT_TAG_TEST, b"other").unwrap();
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Member(1),
            expected_version: Some(v2),
            record: MetaRecord::Member(rec),
        })
        .await
        .unwrap();
    assert!(matches!(r, MetaResult::Ok { .. }));
    until("restored", Duration::from_secs(10), || {
        stored(&meta).1.is_some()
    })
    .await;
    stop.cancel();
    task.await.unwrap();

    // A changed version (an emulated level-1 restart) is written at once.
    let now = clock.now_ms();
    let o = compat::advertise_once(&meta, 1, &nv(1), now, 600_000)
        .await
        .unwrap();
    assert_eq!(o, AdvertiseOutcome::Written);
    assert_eq!(stored(&meta).1.unwrap().level_max, 1);
    assert_eq!(
        compat::advertise_once(&meta, 1, &nv(1), now + 1, 600_000)
            .await
            .unwrap(),
        AdvertiseOutcome::Unchanged
    );
    // No member record for another node: nothing to do.
    assert_eq!(
        compat::advertise_once(&meta, 9, &nv(2), now, 600_000)
            .await
            .unwrap(),
        AdvertiseOutcome::NoMember
    );
}

// ---------------------------------------------------------------- level source

#[tokio::test(flavor = "multi_thread")]
async fn meta_levels_prefer_the_override() {
    let tmp = tempfile::tempdir().unwrap();
    let meta = init_single(&tmp.path().join("meta")).await;
    let l = MetaLevels::new(meta.view().clone(), Some(1));
    assert_eq!((l.level_max(), l.finalized()), (1, LEVEL_MIN));
    let d = MetaLevels::new(meta.view().clone(), None);
    assert_eq!(d.level_max(), ostro_types::compat::level_max());
    assert_eq!(
        compat::effective_level_max(None),
        ostro_types::compat::level_max()
    );
    finalize_single(&meta).await;
    assert_eq!(d.finalized(), 2);
    let src: Arc<dyn LevelSource> = Arc::new(d);
    assert_eq!(src.finalized(), 2);
    meta.stop().await;
}
