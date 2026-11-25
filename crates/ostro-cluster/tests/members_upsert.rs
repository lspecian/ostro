//! 13-09 Task 1 (plan check W10): the Member self-upsert is a read-modify-write that keeps
//! every ext entry (known and unknown tags) byte for byte, writes nothing when nothing
//! changed, and never overwrites a Removed tombstone.

use ostro_cluster::members::upsert_self;
use ostro_meta::ext::{
    MEMBER_REMOVED, MemberState, NodeVersion, ext_get, ext_put, put_member_state, put_node_version,
};
use ostro_meta::{Member, MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult};

const UNKNOWN_TAG: u32 = 200;

fn me(addr: &str) -> Member {
    Member {
        node_id: 1,
        addr: addr.to_owned(),
        joined_at_ms: 99,
        cluster_addr: Some("c:1".into()),
        repl_addr: None,
        ext: Vec::new(),
    }
}

async fn put_raw(meta: &MetaHandle, m: Member) {
    let cur = meta.view().read(|s| s.member(m.node_id).map(|x| x.version));
    let res = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Member(m.node_id),
            expected_version: cur,
            record: MetaRecord::Member(m),
        })
        .await
        .unwrap();
    assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
}

fn current(meta: &MetaHandle) -> (u64, Member) {
    meta.view()
        .read(|s| s.member(1).map(|m| (m.version, m.value.clone())))
        .unwrap()
}

#[tokio::test]
async fn unknown_ext_tags_survive_and_unchanged_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaHandle::start(dir.path(), 1).await.unwrap();
    meta.init_cluster("c", 1, 1, 1, [0; 32]).await.unwrap();
    let ext = ext_put(&[], UNKNOWN_TAG, b"from-a-newer-ostro").unwrap();
    let ext = put_node_version(
        &ext,
        &NodeVersion {
            ostro_version: "x".into(),
            level_max: 2,
            ..NodeVersion::default()
        },
    )
    .unwrap();
    put_raw(
        &meta,
        Member {
            ext: ext.clone(),
            ..me("old:1")
        },
    )
    .await;
    assert!(
        upsert_self(&meta, me("new:1")).await.unwrap(),
        "address changed"
    );
    let (v1, m) = current(&meta);
    assert_eq!(m.addr, "new:1");
    assert_eq!(m.joined_at_ms, 99, "kept");
    assert_eq!(m.ext, ext, "every ext entry kept byte for byte");
    assert_eq!(
        ext_get(&m.ext, UNKNOWN_TAG),
        Some(&b"from-a-newer-ostro"[..])
    );
    assert!(
        !upsert_self(&meta, me("new:1")).await.unwrap(),
        "nothing changed"
    );
    assert_eq!(current(&meta).0, v1, "no write");
}

#[tokio::test]
async fn removed_is_never_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaHandle::start(dir.path(), 1).await.unwrap();
    meta.init_cluster("c", 1, 1, 1, [0; 32]).await.unwrap();
    let ext = put_member_state(
        &[],
        &MemberState {
            state: MEMBER_REMOVED,
            changed_at_ms: 5,
        },
    )
    .unwrap();
    put_raw(&meta, Member { ext, ..me("old:1") }).await;
    let before = current(&meta);
    let e = upsert_self(&meta, me("new:1")).await.unwrap_err();
    assert!(
        matches!(&e, MetaError::Rejected(m) if m.contains("removed")),
        "{e}"
    );
    assert_eq!(current(&meta), before, "the tombstone is untouched");
}
