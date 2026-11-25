//! 13-09 Task 2: the marker-first joiner, the boot rule and both voter checks (D-P13-9,
//! D-P13-29) over real loopback mTLS.
//!
//! Crash points: with `--features failpoints` the real failpoints (`member::join_before_marker`,
//! `member::join_after_marker`, `member::join_after_node_toml`) stop `ostro cluster join`
//! there; without the feature the test performs the same handshake by hand and writes the
//! same durable steps up to the crash point. In every test the persisted meta logs hold one
//! InitCluster (the bootstrapping node's) and no joiner ever proposes one.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;
#[path = "common/p13_join.rs"]
mod p13;

use std::path::Path;
use std::time::Duration;

use ostro_cluster::membership::service::set_member_state;
use ostro_meta::ext::MEMBER_REMOVED;
use ostro_server::ServerHandle;
use ostro_server::config::{FriendlyDuration, ServerConfig};
use ostro_server::p13::joiner::{self, write_identity};
use ostro_server::p13::{BootMode, DataDirProbe};
use p12::{START_T, node_toml, start, start_all, static_cluster, stop_all, tmp};
use p13::*;

/// Where `ostro cluster join` "crashes".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Crash {
    /// Before the marker (the seed allocated an id; nothing on disk).
    BeforeMarker,
    /// After the marker, before `tls/joined/`.
    AfterMarker,
    /// After node.toml (everything prepared, meta never started).
    AfterNodeToml,
}

impl Crash {
    #[cfg(feature = "failpoints")]
    fn failpoint(self) -> &'static str {
        match self {
            Crash::BeforeMarker => "member::join_before_marker",
            Crash::AfterMarker => "member::join_after_marker",
            Crash::AfterNodeToml => "member::join_after_node_toml",
        }
    }
}

/// `ostro cluster join` through `seed`, stopped at `crash`. Returns the allocated id.
#[cfg(feature = "failpoints")]
async fn crash_join(seed: &ServerHandle, j: &Joiner, crash: Crash) -> u64 {
    let before: Vec<u64> = seed
        .meta()
        .view()
        .read(|s| s.members().map(|m| m.value.node_id).collect());
    fail::cfg(crash.failpoint(), "return").unwrap();
    let r = join(seed, j).await;
    fail::remove(crash.failpoint());
    let e = r.expect_err("the failpoint stops the join");
    assert!(e.contains("failpoint"), "{e}");
    seed.meta()
        .view()
        .read(|s| {
            s.members()
                .map(|m| m.value.node_id)
                .find(|id| !before.contains(id))
        })
        .or_else(|| joiner::read_marker(&j.data_dir).unwrap().map(|m| m.node_id))
        .expect("an allocated id")
}

/// The same handshake by hand, writing the durable steps up to `crash`.
#[cfg(not(feature = "failpoints"))]
async fn crash_join(seed: &ServerHandle, j: &Joiner, crash: Crash) -> u64 {
    use ostro_api::phase13::{Advertise, JoinRequest, JoinResponse, NodeVersionDto};
    use ostro_server::p13::joiner::{write_joined_tls, write_marker};
    use ostro_server::p13::{JoinMarker, JoinedTls};
    let t = token(seed, None).await;
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let csr = rcgen::CertificateParams::new(Vec::<String>::new())
        .unwrap()
        .serialize_request(&key)
        .unwrap()
        .pem()
        .unwrap();
    let req = JoinRequest {
        token: t.token,
        csr_pem: csr,
        advertise: Advertise {
            api: j.api.to_string(),
            cluster: j.cluster.to_string(),
            repl: None,
        },
        node_version: NodeVersionDto {
            ostro_version: "test".into(),
            level_min: ostro_types::compat::LEVEL_MIN,
            level_max: ostro_types::compat::level_max(),
            wasmtime: String::new(),
            event_codec_ver: 1,
            wit_lines: vec!["0.1".into()],
        },
        resume_node_id: joiner::read_marker(&j.data_dir).unwrap().map(|m| m.node_id),
    };
    let (st, v) = post(
        seed,
        "/v1/cluster/join",
        serde_json::to_value(&req).unwrap(),
    )
    .await;
    assert!((200..300).contains(&st), "{st} {v}");
    let r: JoinResponse = serde_json::from_value(v).unwrap();
    if crash == Crash::BeforeMarker {
        return r.node_id;
    }
    write_marker(
        &j.data_dir,
        &JoinMarker {
            node_id: r.node_id,
            cluster_id: r.cluster_id.clone(),
            seeds: r
                .seeds
                .iter()
                .map(|s| ostro_cluster::address_book::Seed {
                    id: s.id,
                    addr: s.addr.clone(),
                    repl_addr: s.repl_addr.clone(),
                })
                .collect(),
        },
    )
    .unwrap();
    if crash == Crash::AfterMarker {
        return r.node_id;
    }
    write_joined_tls(
        &j.data_dir,
        &JoinedTls {
            ca_pem: r.ca_cert_pem,
            cert_chain_pem: r.node_cert_pem,
            key_pem: key.serialize_pem(),
        },
    )
    .unwrap();
    write_identity(&j.data_dir, r.node_id, &r.cluster_id).unwrap();
    r.node_id
}

/// The boot rule of `dir` as `cluster.rs` probes it.
fn mode_of(cfg: &ServerConfig) -> Result<BootMode, String> {
    let data_dir = cfg.node.data_dir.clone();
    let meta_dir = data_dir.join("meta");
    let marker = ostro_server::p13::join_identity(&data_dir).map_err(|e| e.to_string())?;
    let probe = ostro_cluster::identity::probe_meta(&meta_dir).map_err(|e| e.to_string())?;
    ostro_server::p13::boot_mode(
        cfg,
        &DataDirProbe {
            data_dir: data_dir.clone(),
            meta_dir,
            meta_blank: probe.blank,
            has_partition_data: false,
            join_marker: marker,
            tls_complete: ostro_server::p13::tls_complete(&data_dir),
        },
    )
    .map_err(|e| e.to_string())
}

async fn start_joined(j: &Joiner, id: u64) -> ServerHandle {
    tokio::time::timeout(START_T, ostro_server::Server::start(joined_cfg(j, id)))
        .await
        .expect("joined start timed out")
        .expect("joined node starts")
}

async fn until_active(seed: &ServerHandle, id: u64) {
    status_until(seed, Duration::from_secs(60), |st| {
        member(st, id).is_some_and(|m| m["state"] == "active")
    })
    .await;
}

/// Exactly one InitCluster over every persisted meta log in `dirs`, by node 1.
fn one_init_cluster(dirs: &[&Path]) {
    let mut seen: Vec<(u64, u64, String)> = Vec::new();
    for d in dirs {
        for e in init_clusters(d) {
            assert_eq!(
                e.1,
                1,
                "{}: InitCluster proposed by node {}",
                d.display(),
                e.1
            );
            if !seen.iter().any(|s| s.0 == e.0) {
                seen.push(e);
            }
        }
    }
    assert!(seen.len() <= 1, "more than one InitCluster: {seen:?}");
}

/// A seed with short join / orphan timeouts (orphan ≥ token TTL still holds).
fn short_seed(root: &Path) -> ServerConfig {
    let mut cfg = seed_cfg(root);
    cfg.workers.join_token_ttl = FriendlyDuration::from_secs(2);
    cfg.cluster.join_orphan_timeout = FriendlyDuration::from_secs(3);
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_fresh_with_bound_cluster_id_bootstraps() {
    let root = tmp();
    let cfg = seed_cfg(root.path());
    std::fs::create_dir_all(&cfg.node.data_dir).unwrap();
    ostro_partition::bind_cluster_id(&cfg.node.data_dir, CLUSTER).unwrap();
    assert_eq!(
        mode_of(&cfg),
        Ok(BootMode::Fresh),
        "no marker: never a joiner"
    );
    let h = start(cfg.clone()).await.expect("bootstrap");
    assert_eq!(h.cluster_id(), CLUSTER);
    h.shutdown().await.unwrap();
    let inits = init_clusters(&cfg.node.data_dir);
    assert_eq!(inits.len(), 1, "{inits:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_bind_cluster_id_restart_bootstraps() {
    let root = tmp();
    let mut cfgs = static_cluster(root.path(), 3, PARTS, CLUSTER);
    for c in &mut cfgs {
        timings(c);
        // The crash: node.toml bound (node id + cluster id), meta never written.
        std::fs::create_dir_all(&c.node.data_dir).unwrap();
        write_identity(&c.node.data_dir, c.node.node_id, CLUSTER).unwrap();
        assert_eq!(
            mode_of(c),
            Ok(BootMode::Fresh),
            "configured peers alone: no joiner"
        );
    }
    let hs = start_all(&cfgs).await;
    stop_all(hs).await;
    let dirs: Vec<&Path> = cfgs.iter().map(|c| c.node.data_dir.as_path()).collect();
    for d in &dirs {
        for (_, node, cid) in init_clusters(d) {
            assert_eq!(cid, CLUSTER);
            assert!((1..=3).contains(&node), "bootstrap voters only");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_crash_before_marker() {
    let root = tmp();
    let seed = start(short_seed(root.path())).await.expect("seed");
    let j = joiner(root.path(), 2);
    let id = crash_join(&seed, &j, Crash::BeforeMarker).await;
    assert_eq!(id, 2);
    // Restart: not a joiner, data dir unchanged (nothing written).
    assert!(!j.data_dir.join("cluster-seeds.toml").exists());
    assert!(node_toml(&j.data_dir).is_empty());
    assert_eq!(mode_of(&joined_cfg(&j, 2)), Ok(BootMode::Fresh));
    // The orphan Joining member is tombstoned after join_orphan_timeout.
    status_until(&seed, Duration::from_secs(60), |st| {
        member(st, 2).is_some_and(|m| m["state"] == "removed" && m["raft_role"] == "none")
    })
    .await;
    // Re-running join allocates a new id and that node joins.
    let prepared = join(&seed, &j).await.expect("re-run join");
    assert_eq!(prepared.node_id, 3, "ids are never reused");
    let joined = start_joined(&j, 3).await;
    until_active(&seed, 3).await;
    joined.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
    one_init_cluster(&[&root.path().join("node1"), &j.data_dir]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_crash_after_marker_before_node_toml() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let j = joiner(root.path(), 2);
    let id = crash_join(&seed, &j, Crash::AfterMarker).await;
    assert_eq!(
        joiner::read_marker(&j.data_dir).unwrap().map(|m| m.node_id),
        Some(id)
    );
    // Restart with tls/ incomplete: refused before any TLS load, nothing bootstrapped.
    let e = tokio::time::timeout(START_T, ostro_server::Server::start(joined_cfg(&j, id)))
        .await
        .expect("start returns")
        .expect_err("incomplete join refused");
    assert!(e.to_string().contains("re-run `ostro cluster join`"), "{e}");
    assert!(
        ostro_cluster::identity::probe_meta(&j.data_dir.join("meta"))
            .unwrap()
            .blank
    );
    // Re-running join resumes the same id.
    let prepared = join(&seed, &j).await.expect("resume");
    assert_eq!(prepared.node_id, id, "resume, no new id");
    // A crash between tls/ and node.toml: the boot completes node.toml from the marker.
    std::fs::remove_file(j.data_dir.join("node.toml")).unwrap();
    assert_eq!(mode_of(&joined_cfg(&j, id)), Ok(BootMode::Joiner));
    assert!(node_toml(&j.data_dir).contains(&format!("node_id = {id}")));
    let joined = start_joined(&j, id).await;
    until_active(&seed, id).await;
    let ids: Vec<u64> = seed
        .meta()
        .view()
        .read(|s| s.members().map(|m| m.value.node_id).collect());
    assert_eq!(ids, vec![1, id], "no second allocation");
    joined.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
    one_init_cluster(&[&root.path().join("node1"), &j.data_dir]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_crash_after_node_toml_before_meta() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let j = joiner(root.path(), 2);
    let id = crash_join(&seed, &j, Crash::AfterNodeToml).await;
    assert_eq!(mode_of(&joined_cfg(&j, id)), Ok(BootMode::Joiner));
    let joined = start_joined(&j, id).await;
    assert_eq!(
        joined.p13().map(|c| c.cluster.boot_mode()),
        Some(BootMode::Joiner)
    );
    assert_eq!(joined.p13().map(|c| c.node_id()), Some(id), "same id");
    until_active(&seed, id).await;
    // A later restart is an existing member (no marker effect, no InitCluster).
    joined.shutdown().await.unwrap();
    let again = start_joined(&j, id).await;
    assert_eq!(
        again.p13().map(|c| c.cluster.boot_mode()),
        Some(BootMode::Existing)
    );
    again.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
    one_init_cluster(&[&root.path().join("node1"), &j.data_dir]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_after_orphan_tombstone() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let j = joiner(root.path(), 2);
    let id = crash_join(&seed, &j, Crash::AfterMarker).await;
    // The orphan GC (or an operator) tombstoned the id meanwhile.
    set_member_state(seed.meta(), id, MEMBER_REMOVED, 1)
        .await
        .unwrap();
    // Re-running join: the resume is refused (member_removed); meta is blank and there is no
    // partition data, so the join discards the marker / tls / identity and gets a new id.
    let prepared = join(&seed, &j).await.expect("rejoin");
    assert_eq!(prepared.node_id, id + 1);
    assert_eq!(
        joiner::read_marker(&j.data_dir).unwrap().map(|m| m.node_id),
        Some(id + 1)
    );
    assert!(node_toml(&j.data_dir).contains(&format!("node_id = {}", id + 1)));
    let joined = start_joined(&j, id + 1).await;
    until_active(&seed, id + 1).await;
    joined.shutdown().await.unwrap();
    seed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joiner_with_unreachable_seed_exits_after_join_timeout() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let j = joiner(root.path(), 2);
    let prepared = join(&seed, &j).await.expect("join");
    seed.shutdown().await.unwrap();
    let mut cfg = joined_cfg(&j, prepared.node_id);
    cfg.cluster.join_timeout = FriendlyDuration::from_secs(3);
    let started = std::time::Instant::now(); // clock-exempt: test timing
    let e = tokio::time::timeout(START_T, ostro_server::Server::start(cfg))
        .await
        .expect("start returns")
        .expect_err("no leader: the joiner fails");
    assert!(e.to_string().contains("join_timeout"), "{e}");
    assert!(started.elapsed() < Duration::from_secs(60));
    // It never served: no partition data, still a blank meta.
    assert!(
        !j.data_dir.join("wal").exists()
            || std::fs::read_dir(j.data_dir.join("wal"))
                .unwrap()
                .next()
                .is_none()
    );
    assert!(init_clusters(&j.data_dir).is_empty(), "no InitCluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_node_refuses_to_start() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let j = joiner(root.path(), 2);
    let prepared = join(&seed, &j).await.expect("join");
    let joined = start_joined(&j, prepared.node_id).await;
    until_active(&seed, 2).await;
    let (st, v) = post(&seed, "/v1/cluster/members/2/remove", serde_json::json!({})).await;
    assert!((200..300).contains(&st), "{st} {v}");
    status_until(&seed, Duration::from_secs(60), |st| {
        member(st, 2).is_some_and(|m| m["state"] == "removed" && m["raft_role"] == "none")
    })
    .await;
    // The removed node stops by itself (fail-stop).
    tokio::time::timeout(Duration::from_secs(30), joined.wait())
        .await
        .expect("the removed node stops");
    assert!(
        joined.fatal().is_some_and(|f| f.contains("removed")),
        "{:?}",
        joined.fatal()
    );
    let _ = joined.shutdown().await;
    let e = tokio::time::timeout(START_T, ostro_server::Server::start(joined_cfg(&j, 2)))
        .await
        .expect("start returns")
        .expect_err("a removed node refuses to start");
    assert!(e.to_string().contains("removed"), "{e}");
    seed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_three_grown_to_five_restarts_without_the_grow_fail_stop() {
    let root = tmp();
    let mut cfgs = static_cluster(root.path(), 3, PARTS, CLUSTER);
    for c in &mut cfgs {
        timings(c);
    }
    let mut hs = start_all(&cfgs).await;
    let j4 = joiner(root.path(), 4);
    let j5 = joiner(root.path(), 5);
    let p4 = join(&hs[0], &j4).await.expect("join 4");
    let n4 = start_joined(&j4, p4.node_id).await;
    let p5 = join(&hs[0], &j5).await.expect("join 5");
    let n5 = start_joined(&j5, p5.node_id).await;
    // 5 members: the reconciler grows the voters 3 -> 5 in one joint change.
    status_until(&hs[0], Duration::from_secs(90), |st| {
        st["voters"].as_array().is_some_and(|v| v.len() == 5)
            && st["voters_outgoing"]
                .as_array()
                .is_some_and(|v| v.is_empty())
    })
    .await;
    // Restart every node: neither the PreStart (persisted voters {1..5} vs peers {1,2,3})
    // nor the PostReady check fail-stops.
    n5.shutdown().await.unwrap();
    n4.shutdown().await.unwrap();
    for h in hs.drain(..) {
        h.shutdown().await.unwrap();
    }
    let hs = start_all(&cfgs).await;
    let n4 = start_joined(&j4, p4.node_id).await;
    let n5 = start_joined(&j5, p5.node_id).await;
    status_until(&hs[1], Duration::from_secs(90), |st| {
        st["voters"].as_array().is_some_and(|v| v.len() == 5) && st["meta_leader"] != 0
    })
    .await;
    n5.shutdown().await.unwrap();
    n4.shutdown().await.unwrap();
    stop_all(hs).await;
    let dirs: Vec<&Path> = cfgs.iter().map(|c| c.node.data_dir.as_path()).collect();
    for d in dirs
        .iter()
        .copied()
        .chain([j4.data_dir.as_path(), j5.data_dir.as_path()])
    {
        for (_, node, _) in init_clusters(d) {
            assert!((1..=3).contains(&node), "joiners never InitCluster");
        }
    }
}
