//! `node.toml` identity binding (12-13, D-P12-16): `bind_node_id` (before meta) and
//! `bind_cluster_id` (before partitions) write their key once, durably and append-only;
//! the same value again is a no-op; a different stored value is fatal; node id 0 is refused;
//! Phase 8 keys and unknown keys survive byte for byte; `Node::open` refuses a data dir bound
//! to another node id.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::Path;

use ostro_partition::{Node, NodeConfig, NodeError, bind_cluster_id, bind_node_id};
use ostro_types::PartitionCount;
use tokio::time::timeout;

const OPEN_T: std::time::Duration = std::time::Duration::from_secs(120);

fn node_toml(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("node.toml")).expect("node.toml")
}

fn cfg(dir: &Path, node_id: u64) -> NodeConfig {
    let mut c = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(2).unwrap());
    c.node_id = node_id;
    c
}

async fn open(c: NodeConfig) -> Result<Node, NodeError> {
    timeout(OPEN_T, Node::open(c))
        .await
        .expect("open timed out")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_dir_binds_both_then_opens() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path().join("data");
    bind_node_id(&dir, 7).expect("bind node_id");
    bind_cluster_id(&dir, "c-test-1").expect("bind cluster_id");
    assert_eq!(node_toml(&dir), "node_id = 7\ncluster_id = \"c-test-1\"\n");
    assert!(
        !dir.join("node.toml.tmp").exists(),
        "no temp file left behind"
    );

    // The first open adds the Phase 2/8 keys after the identity lines.
    let node = open(cfg(&dir, 7)).await.expect("open");
    let inc = node.incarnation();
    node.shutdown().await.expect("shutdown");
    let body = node_toml(&dir);
    assert!(
        body.starts_with("node_id = 7\ncluster_id = \"c-test-1\"\n"),
        "{body}"
    );
    assert!(
        body.contains("format_version = 1\npartitions = 2\n"),
        "{body}"
    );
    assert!(body.contains("incarnation = \""), "{body}");

    // Restart with the same values: no-ops, same file, same incarnation.
    bind_node_id(&dir, 7).expect("same node_id");
    bind_cluster_id(&dir, "c-test-1").expect("same cluster_id");
    assert_eq!(node_toml(&dir), body);
    let node = open(cfg(&dir, 7)).await.expect("reopen");
    assert_eq!(node.incarnation(), inc);
    node.shutdown().await.expect("shutdown");
    assert_eq!(node_toml(&dir), body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mismatches_are_fatal_and_change_nothing() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    bind_node_id(dir, 3).unwrap();
    bind_cluster_id(dir, "alpha").unwrap();
    let before = node_toml(dir);

    let e = bind_node_id(dir, 4).expect_err("other node id");
    assert!(
        matches!(&e, NodeError::IdentityMismatch { key: "node_id", stored, configured }
            if stored == "3" && configured == "4"),
        "{e:?}"
    );
    let msg = e.to_string();
    assert!(
        msg.contains("node_id") && msg.contains('3') && msg.contains('4'),
        "{msg}"
    );

    let e = bind_cluster_id(dir, "beta").expect_err("other cluster id");
    assert!(
        matches!(&e, NodeError::IdentityMismatch { key: "cluster_id", stored, configured }
            if stored == "alpha" && configured == "beta"),
        "{e:?}"
    );
    assert!(e.to_string().contains("alpha") && e.to_string().contains("beta"));
    assert_eq!(node_toml(dir), before, "nothing written on a mismatch");

    // Node::open refuses a data dir bound to another node id, before touching partitions.
    let e = open(cfg(dir, 9)).await.expect_err("open as another node");
    assert!(
        matches!(e, NodeError::IdentityMismatch { key: "node_id", .. }),
        "{e:?}"
    );
    assert!(!dir.join("state").exists(), "no partition opened");
    assert_eq!(node_toml(dir), before);
}

#[test]
fn node_id_zero_and_bad_cluster_ids_are_refused() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    assert!(bind_node_id(dir, 0).is_err());
    for bad in ["", "has space", "quo\"te", "back\\slash", "new\nline"] {
        assert!(bind_cluster_id(dir, bad).is_err(), "{bad:?}");
    }
    assert!(!dir.join("node.toml").exists(), "nothing written");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn phase8_keys_are_preserved_byte_for_byte() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    // A Phase 8 data dir (format_version, partitions, incarnation), opened as node 1.
    let node = open(cfg(dir, 1)).await.expect("open");
    let inc = node.incarnation();
    node.shutdown().await.expect("shutdown");
    let p8 = node_toml(dir);

    bind_node_id(dir, 1).unwrap();
    bind_cluster_id(dir, "c-9").unwrap();
    let body = node_toml(dir);
    assert_eq!(body, format!("{p8}node_id = 1\ncluster_id = \"c-9\"\n"));
    let node = open(cfg(dir, 1)).await.expect("reopen");
    assert_eq!(node.incarnation(), inc, "incarnation unchanged");
    node.shutdown().await.expect("shutdown");
    assert_eq!(node_toml(dir), body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn old_node_toml_gains_the_keys_and_keeps_unknown_ones() {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    // Pre-08-08 shape, a comment, an unknown key and no trailing newline.
    let old = "# old\nformat_version = 1\npartitions = 2\nfuture_key = \"x\"";
    std::fs::write(dir.join("node.toml"), old).unwrap();
    bind_node_id(dir, 5).unwrap();
    bind_cluster_id(dir, "gamma").unwrap();
    assert_eq!(
        node_toml(dir),
        format!("{old}\nnode_id = 5\ncluster_id = \"gamma\"\n")
    );
    let node = open(cfg(dir, 5)).await.expect("open");
    node.shutdown().await.expect("shutdown");
    let body = node_toml(dir);
    assert!(body.starts_with(&format!("{old}\nnode_id = 5\ncluster_id = \"gamma\"\n")));
    assert!(
        body.contains("incarnation = \""),
        "incarnation added: {body}"
    );
}
