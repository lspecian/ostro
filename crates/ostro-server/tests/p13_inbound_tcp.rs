//! p13-children (13-19 finding 1): cross-node outbox delivery over real loopback mTLS.
//!
//! The Phase 13 cluster-listener services (Membership, `NodeOps`) check the caller with
//! `authz::require_node`, which reads the `PeerNode` annotation. Only the in-memory
//! transport set it; over mTLS TCP nothing annotated the Phase 13 routes, so
//! `NodeOps.DeliverInbound` answered `UNAUTHENTICATED` and an outbox batch (a child's
//! `StartChild`, its `ChildCompleted`) whose target partition another node led was resent
//! forever: in tier C, `parent_attached` runs hung once restarts spread leadership over
//! the nodes. Tier B (in-memory transport) could not see it.
//!
//! Three static nodes over TCP with a shared CA, leaders on at least two nodes:
//!
//! 1. every node calls `NodeOps.DeliverInbound` (an empty batch, answered `Duplicate`) on
//!    every other node that leads a partition, through its own cluster transport: no call
//!    may be refused for its identity;
//! 2. attached parents whose child partition another node leads complete with the
//!    child's output.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_api::proto::cluster::v1::DeliverInboundRequest;
use ostro_api::proto::cluster::v1::node_ops_client::NodeOpsClient;
use ostro_cluster::contracts::HandoffOutcome;
use ostro_server::ServerHandle;
use ostro_types::command::{InboundBatch, encode_command};
use ostro_types::{Command, Epoch, PartitionId, RunId};
use p12::{START_T, assignments, eventually, guest_wasm, hex, start_all, static_cluster, tmp};
use serde_json::{Value, json};

const PARTITIONS: u16 = 4;
const RUN_T: Duration = Duration::from_secs(60);

fn leaders(h: &ServerHandle) -> BTreeMap<u16, (u64, u32, Vec<u64>)> {
    assignments(h)
        .into_iter()
        .map(|(p, epoch, leader, _, isr)| (p, (leader, epoch, isr)))
        .collect()
}

fn all_led(h: &ServerHandle) -> bool {
    let l = leaders(h);
    l.len() == usize::from(PARTITIONS) && l.values().all(|(n, _, _)| *n != 0)
}

fn distinct_leaders(h: &ServerHandle) -> usize {
    leaders(h)
        .values()
        .map(|(n, _, _)| *n)
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

/// Spread leadership: while every partition has the same leader, hand the highest
/// partition to another ISR member (served locally by its leader).
async fn spread(hs: &[ServerHandle]) {
    for _ in 0..10 {
        if distinct_leaders(&hs[0]) > 1 {
            return;
        }
        let l = leaders(&hs[0]);
        let (&p, (leader, epoch, isr)) = l.iter().next_back().expect("partitions");
        let to = *isr.iter().find(|n| *n != leader).expect("an ISR peer");
        let ctx = hs[usize::try_from(*leader - 1).unwrap()]
            .p13()
            .expect("p13 context");
        match ctx
            .handoff_client
            .hand_off(*leader, PartitionId(p), to, Epoch(*epoch))
            .await
        {
            Ok(HandoffOutcome::Moved { .. }) => {
                let moved = eventually(Duration::from_secs(20), || {
                    leaders(&hs[0]).get(&p).map(|x| x.0) == Some(to)
                })
                .await;
                assert!(moved, "p{p}: meta never showed leader {to}");
            }
            other => println!("handoff p{p} {leader} -> {to}: {other:?}; retrying"),
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        distinct_leaders(&hs[0]) > 1,
        "leaders: {:?}",
        leaders(&hs[0])
    );
}

/// `NodeOps.DeliverInbound` of an empty batch for `p` from `from` to `to` (`to` leads `p`).
async fn deliver_empty(
    hs: &[ServerHandle],
    from: u64,
    to: u64,
    p: u16,
) -> Result<u32, tonic::Status> {
    let ctx = hs[usize::try_from(from - 1).unwrap()]
        .p13()
        .expect("p13 context");
    let ch = ctx.cluster.transport().channel(to).expect("channel");
    let batch = InboundBatch {
        src: 0,
        msgs: Vec::new(),
    };
    NodeOpsClient::new(ch)
        .deliver_inbound(DeliverInboundRequest {
            partition: u32::from(p),
            batch: encode_command(&Command::inbound_batch(batch)).to_vec(),
        })
        .await
        .map(|r| r.into_inner().outcome)
}

struct Api {
    http: reqwest::Client,
    base: String,
}

impl Api {
    fn new(h: &ServerHandle) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: format!("http://{}", h.local_addr()),
        }
    }

    async fn deploy(&self, guest: &str) {
        use sha2::{Digest, Sha256};
        let wasm = guest_wasm(guest);
        let sha = hex(&Sha256::digest(&wasm));
        let r = self
            .http
            .put(format!("{}/v1/modules/{sha}", self.base))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    }

    async fn start(&self, i: usize) -> String {
        let body = json!({ "input": { "workflow": "leaf_echo", "input": { "i": i } } });
        for _ in 0..200 {
            if let Ok(r) = self
                .http
                .post(format!("{}/v1/workflows/parent_attached/runs", self.base))
                .json(&body)
                .send()
                .await
                && r.status().is_success()
                && let Ok(v) = r.json::<Value>().await
                && let Some(id) = v["run_id"].as_str()
            {
                return id.to_owned();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("start parent_attached {i}: no acknowledgement");
    }

    async fn run(&self, id: &str) -> Value {
        match self
            .http
            .get(format!("{}/v1/runs/{id}", self.base))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r.json().await.unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn outbox_batches_cross_mtls_nodes() {
    let dir = tmp();
    let cfgs = static_cluster(dir.path(), 3, PARTITIONS, "p13-inbound-tcp");
    let hs = start_all(&cfgs).await;
    assert!(
        eventually(START_T, || all_led(&hs[0])).await,
        "partitions without a leader: {:?}",
        leaders(&hs[0])
    );
    spread(&hs).await;

    // 1. Node identity on NodeOps over mTLS: every (caller, leader) pair.
    let ids: Vec<u64> = (1..=3).collect();
    let mut checked = 0;
    for &from in &ids {
        for &to in &ids {
            if from == to {
                continue;
            }
            let Some((&p, _)) = leaders(&hs[0]).iter().find(|(_, (l, _, _))| *l == to) else {
                continue;
            };
            // A leader that just moved answers FAILED_PRECONDITION / UNAVAILABLE for a
            // moment; an identity refusal is the bug and fails at once.
            let mut last = None;
            for _ in 0..100 {
                match deliver_empty(&hs, from, to, p).await {
                    Ok(code) => {
                        assert_eq!(code, 1, "empty batch from n{from} on n{to} p{p}");
                        last = None;
                        break;
                    }
                    Err(s) => {
                        assert!(
                            !matches!(
                                s.code(),
                                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
                            ),
                            "NodeOps.DeliverInbound from node {from} on node {to} (p{p}) \
                             refused the caller's node certificate: {s:?}"
                        );
                        last = Some(s);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            assert!(last.is_none(), "n{from} -> n{to} p{p}: {last:?}");
            checked += 1;
        }
    }
    assert!(checked >= 2, "only {checked} caller/leader pairs checked");

    // 2. Attached parents whose child partition another node leads.
    let api = Api::new(&hs[0]);
    api.deploy("child-parent").await;
    api.deploy("child-leaf").await;
    let mut crossing = Vec::new();
    for i in 0..64 {
        let id = api.start(i).await;
        let run: RunId = id.parse().expect("run id");
        let child = ostro_server::children::child_run_id(&run, 1, PARTITIONS);
        let l = leaders(&hs[0]);
        let (pl, cl) = (l[&run.partition().0].0, l[&child.partition().0].0);
        if pl != cl {
            crossing.push((id, run.partition().0, pl, child.partition().0, cl));
            if crossing.len() == 4 {
                break;
            }
        }
    }
    assert!(!crossing.is_empty(), "no parent/child pair crossed nodes");
    for (id, pp, pl, cp, cl) in &crossing {
        let end = std::time::Instant::now() + RUN_T; // clock-exempt: test deadline
        let v = loop {
            let v = api.run(id).await;
            if v["status"] == "completed" || std::time::Instant::now() >= end {
                break v;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(
            v["status"], "completed",
            "parent {id} (p{pp}, leader n{pl}) with its child on p{cp} (leader n{cl}): {v}"
        );
        assert!(
            v["result"]["child"]["echo"].is_object(),
            "parent {id} output: {v}"
        );
    }
    println!(
        "{checked} NodeOps caller/leader pairs; {} crossing parents completed",
        crossing.len()
    );
    p12::stop_all(hs).await;
}
