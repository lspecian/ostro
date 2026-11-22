//! 12-03 Task 2: the tier-B in-process cluster. RF=1 workloads are acknowledged and survive
//! a restart; a 3-node spec starts (as independent nodes until 12-14), answers, is killed /
//! restarted and stops without leaking fjall locks.

use std::time::Duration;

use ostro_meta::MetaCommand;
use ostro_partition::store::{PartitionStore, StoreConfig};
use ostro_repl_conformance::harness::inproc::{InProcCluster, TEST_WORKFLOW, read_meta_log};
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec};
use ostro_types::{PartitionCount, PartitionId};
use serde_json::json;

const RUN_T: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_workload_is_acked_and_survives_restart() {
    let mut c = InProcCluster::start(ClusterSpec::single())
        .await
        .expect("start");
    assert_eq!(c.node_ids(), vec![1]);
    let api = c.client(1);
    assert!(api.ready().await, "node 1 ready");
    api.deploy_test_module().await.expect("deploy");

    let mut acks = Vec::new();
    for i in 0..20 {
        acks.push(
            api.start_run(TEST_WORKFLOW, json!({ "i": i }), None)
                .await
                .expect("start run"),
        );
    }
    for a in &acks {
        assert_eq!(
            api.wait_run(&a.run_id, RUN_T).await.expect("wait"),
            "completed"
        );
    }
    // Keyed routing lands every workload run on the requested partition.
    let routed = api.append_workload(2, 3).await.expect("workload");
    assert_eq!(routed.len(), 3);
    assert!(routed.iter().all(|a| a.partition == 2), "{routed:?}");
    acks.extend(routed);

    c.restart(1).await.expect("restart");
    let api = c.client(1);
    for a in &acks {
        assert_eq!(
            api.wait_run(&a.run_id, RUN_T).await.expect("after restart"),
            "completed",
            "run {} survives the restart",
            a.run_id
        );
    }
    // The measured meta log holds the bootstrap and the partition leadership records.
    let log = c.meta_log(1).await;
    assert!(
        log.iter()
            .any(|e| matches!(e.command, MetaCommand::InitCluster { .. })),
        "{log:?}"
    );
    let a = c.assignment(0).await;
    assert_eq!((a.leader, a.replicas.clone()), (1, vec![1]), "{a:?}");
    assert!(a.epoch.0 >= 1, "{a:?}");
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_start_kill_restart_and_release_locks() {
    let spec = ClusterSpec {
        seed: 7,
        ..ClusterSpec::default()
    };
    let mut c = InProcCluster::start(spec.clone()).await.expect("start");
    assert_eq!(c.node_ids(), vec![1, 2, 3]);
    for n in c.node_ids() {
        assert!(c.client(n).ready().await, "node {n} ready");
        // Until 12-14 each node is its own single-voter meta group.
        let dump = read_meta_log(&c.node_disk(n).root.join("meta")).expect("meta log");
        assert_eq!(dump.voters, vec![n], "node {n}");
        assert!(dump.commit >= 1);
        // `/v1/admin/replication` does not exist before 12-14.
        assert_eq!(c.admin_status(n).await, serde_json::Value::Null);
        assert_eq!(c.config(n).cluster.peers.len(), 2);
        assert_eq!(c.config(n).replication.rf, Some(3));
    }
    assert_eq!(c.cluster_id(), "conf-7");

    c.kill(2).await;
    assert!(c.handle(2).is_none());
    assert!(!c.client(2).ready().await, "a killed node does not answer");
    assert!(c.net().faults().link_cut(1, 2), "kill isolates first");
    c.restart(2).await.expect("restart");
    assert!(c.client(2).ready().await, "restarted node answers");
    assert!(!c.net().faults().link_cut(1, 2), "restart heals");

    let root = c.stop_keeping_dirs().await;
    // No thread of a stopped node still holds a fjall lock: the stores reopen here.
    let count = PartitionCount::new(spec.partitions).expect("count");
    for n in 1..=3u64 {
        let dir = root.path().join(format!("node{n}"));
        let (store, meta) =
            PartitionStore::open(&dir, PartitionId(0), count, &StoreConfig::default())
                .unwrap_or_else(|e| panic!("node {n}: reopen after stop: {e}"));
        assert_eq!(meta.partition, PartitionId(0));
        drop(store);
    }
}
