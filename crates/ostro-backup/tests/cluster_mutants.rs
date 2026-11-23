//! Mutant bite (plan 12-15, D-P12-19): with `repl::mutant::upload_above_hw` the uploader reads
//! up to the durable log end instead of the HW, and an uncommitted envelope reaches the bucket.
//! The "remote ⊆ committed" property fails then. Without the mutant it holds on the same log.
#![cfg(feature = "failpoints")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{local_store, node_cfg, open, run_on, shutdown, start_run, start_runs, up_cfg};
use ostro_backup::chunk;
use ostro_backup::cluster;
use ostro_backup::uploader::{Uploader, UploaderClusterDeps};
use ostro_log::{ReplicaSet, ReplicatedLog as _};
use ostro_objstore::ObjectStore;
use ostro_partition::roles::PartitionRoles;
use ostro_partition::{Node, SystemClock};
use ostro_types::{Command, Epoch, PartitionId};
use tokio::sync::watch;

const P0: PartitionId = PartitionId(0);

type Led = Arc<Vec<(PartitionId, Epoch)>>;

struct FakeRoles(watch::Sender<Led>);

impl PartitionRoles for FakeRoles {
    fn led(&self) -> watch::Receiver<Led> {
        self.0.subscribe()
    }
}

fn cluster_uploader(node: &Arc<Node>) -> Arc<Uploader> {
    let (tx, _) = watch::channel(Arc::new(vec![(P0, Epoch(1))]));
    let (_, members) = watch::channel(2usize);
    Arc::new(
        Uploader::new(Arc::clone(node), None, Arc::new(SystemClock), up_cfg()).with_cluster(
            UploaderClusterDeps {
                roles: Arc::new(FakeRoles(tx)) as Arc<dyn PartitionRoles>,
                members,
            },
        ),
    )
}

/// The highest offset end reachable from p0's head (0 = nothing uploaded).
async fn remote_end(store: &Arc<dyn ObjectStore>) -> u64 {
    let Some((h, _)) = cluster::read_head(store, 0).await.expect("head") else {
        return 0;
    };
    let Some(k) = &h.last_chunk_key else {
        return h.chain_start;
    };
    let chain = chunk::walk_back(store, k, h.last_chunk_sha256.expect("sha"), h.chain_start)
        .await
        .expect("walks");
    chain
        .iter()
        .flat_map(|c| c.chunk.envelopes.iter())
        .map(|e| chunk::envelope_span(e).expect("span").1)
        .max()
        .unwrap_or(h.chain_start)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_above_hw_puts_uncommitted_envelopes_in_the_bucket() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 1)).await;
    start_runs(&node, 0, 8).await;

    // Publish an ISR {1, 2} with min_insync 2: node 2 never fetches, so later appends become
    // durable locally but never committed (HW stays put).
    let h = node.handle(P0).expect("ready");
    let committed = h.log().high_watermark().borrow().0;
    h.log()
        .set_published_isr(
            h.epoch(),
            ReplicaSet {
                replicas: vec![1, 2],
                isr: vec![1, 2],
                isr_version: 1,
                min_insync: 2,
            },
        )
        .expect("publish ISR");
    let pending = {
        let h = h.clone();
        tokio::spawn(async move {
            let cmds: Vec<Command> = (0..3)
                .map(|_| Command::start_run(start_run(&run_on(0))))
                .collect();
            let _ = h.submit(cmds).await;
        })
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while h.log().durable_end().0 <= committed {
        assert!(
            tokio::time::Instant::now() < deadline,
            "append never became durable"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let durable = h.log().durable_end().0;
    assert_eq!(
        h.log().high_watermark().borrow().0,
        committed,
        "the tail is not committed"
    );

    // Without the mutant: the bucket holds the committed prefix only.
    let up = cluster_uploader(&node);
    up.round(&store).await.expect("round");
    assert_eq!(remote_end(&store).await, committed, "remote ⊆ committed");

    // With the mutant: the uncommitted tail is uploaded (the property is violated).
    fail::cfg(ostro_log::mutants::UPLOAD_ABOVE_HW, "return").expect("cfg");
    let r = up.round(&store).await;
    fail::remove(ostro_log::mutants::UPLOAD_ABOVE_HW);
    r.expect("mutant round");
    let end = remote_end(&store).await;
    assert_eq!(end, durable, "the mutant uploads up to the durable end");
    assert!(
        end > committed,
        "remote chain end {end} above HW {committed}: remote ⊄ committed under the mutant"
    );

    pending.abort();
    let _ = pending.await;
    drop((up, h));
    shutdown(node).await;
    scenario.teardown();
}
