//! p13-bugs2 bug 3 (tier B): child runs and parent-completion notifications cross nodes.
//!
//! A child run's `StartChild` and its parent's `ChildCompleted` / `ChildStarted` travel
//! through the source partition's outbox and the shuffler's `PartitionRouter`. Until
//! p13-bugs2 the only router was the in-process one: a target partition led by another node
//! answered `NotLeader` forever, so the child never started (or the parent never heard
//! back). Three nodes, RF 3, four partitions with leaders on more than one node; parents
//! started through every node; every parent and child must complete, and at least one
//! parent → child pair must cross nodes (else the test proves nothing and fails).

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_repl_conformance::harness::inproc::{InProcCluster, guest_wasm};
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec};
use ostro_types::RunId;
use serde_json::{Value, json};

const RUN_T: Duration = Duration::from_secs(60);
const PARENTS: usize = 12;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

async fn deploy(c: &InProcCluster, guest: &str) {
    use sha2::{Digest, Sha256};
    let wasm = guest_wasm(guest).expect("guest build");
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let cl = c.client(1);
    let r = cl
        .http
        .put(format!("{}/v1/modules/{}", cl.base_url, hex(&sha)))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
}

/// Every partition's leader, once each has one.
async fn leaders(c: &InProcCluster) -> BTreeMap<u16, u64> {
    let end = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut m = BTreeMap::new();
        for p in 0..c.spec().partitions {
            let a = c.assignment(p).await;
            if a.leader != 0 {
                m.insert(p, a.leader);
            }
        }
        if m.len() == usize::from(c.spec().partitions) {
            return m;
        }
        assert!(
            tokio::time::Instant::now() < end,
            "partitions without a leader: {m:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn output(c: &InProcCluster, node: u64, run: &str) -> Value {
    let (st, v) = c
        .client(node)
        .get_json(&format!("/v1/runs/{run}"))
        .await
        .expect("GET run");
    assert!(st.is_success(), "GET run {run}: {st} {v}");
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn child_runs_and_notifications_cross_nodes() {
    let c = InProcCluster::start(ClusterSpec {
        seed: 1913,
        ..ClusterSpec::default()
    })
    .await
    .expect("cluster");
    deploy(&c, "child-parent").await;
    deploy(&c, "child-leaf").await;
    let lead = leaders(&c).await;
    let distinct: std::collections::BTreeSet<u64> = lead.values().copied().collect();
    assert!(
        distinct.len() > 1,
        "the scenario needs leaders on more than one node: {lead:?}"
    );

    let nodes = c.node_ids();
    let mut crossing = 0;
    let mut detached = Vec::new();
    let mut attached = Vec::new();
    for i in 0..PARENTS {
        let node = nodes[i % nodes.len()];
        let api = c.client(node);
        let input = json!({ "workflow": "leaf_echo", "input": { "i": i } });
        let wf = if i % 2 == 0 {
            "parent_detached"
        } else {
            "parent_attached"
        };
        let ack = api.start_run(wf, input, None).await.expect("start parent");
        if i % 2 == 0 {
            detached.push((node, ack));
        } else {
            attached.push((node, ack));
        }
    }
    // Detached parents name their child: the child must start (StartChild delivered), the
    // parent must learn it (ChildStarted delivered back) and the child must complete.
    for (node, ack) in &detached {
        let st = c.client(*node).wait_run(&ack.run_id, RUN_T).await;
        assert_eq!(
            st.as_deref().ok(),
            Some("completed"),
            "detached parent {} (p{}, leader n{}): {st:?}",
            ack.run_id,
            ack.partition,
            lead[&ack.partition]
        );
        let v = output(&c, *node, &ack.run_id).await;
        let child = v["result"]["detached"]
            .as_str()
            .unwrap_or_else(|| panic!("detached parent output: {v}"))
            .to_owned();
        let cp = child.parse::<RunId>().expect("child run id").partition().0;
        if lead[&cp] != lead[&ack.partition] {
            crossing += 1;
        }
        let st = c.client(*node).wait_run(&child, RUN_T).await;
        assert_eq!(
            st.as_deref().ok(),
            Some("completed"),
            "child {child} (p{cp}, leader n{}) of p{} (leader n{}): {st:?}",
            lead[&cp],
            ack.partition,
            lead[&ack.partition]
        );
    }
    // Attached parents wait for their child's completion notification.
    for (node, ack) in &attached {
        let st = c.client(*node).wait_run(&ack.run_id, RUN_T).await;
        assert_eq!(
            st.as_deref().ok(),
            Some("completed"),
            "attached parent {} (p{}, leader n{}): {st:?}",
            ack.run_id,
            ack.partition,
            lead[&ack.partition]
        );
        let v = output(&c, *node, &ack.run_id).await;
        assert!(
            v["result"]["child"]["echo"].is_object(),
            "attached parent output: {v}"
        );
    }
    println!(
        "leaders {lead:?}; {crossing} of {} detached parent/child pairs crossed nodes",
        detached.len()
    );
    assert!(
        crossing > 0,
        "no parent/child pair crossed nodes (leaders {lead:?})"
    );
    c.stop().await;
}
