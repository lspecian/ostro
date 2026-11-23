//! `GET /v1/admin/replication` (12-14): this node's replication view, per partition, built
//! only from the partition log's measured [`ReplicaStatus`] (the published ISR the leader
//! follows, the tracker's follower progress) and meta. Nothing in it is assumed: a follower
//! that never fetched has no progress row and is never in the published ISR.
//!
//! Shape:
//!
//! ```json
//! {"node_id": 1,
//!  "meta": {"leader": 1, "term": 2, "voters": [1, 2, 3], "learners": []},
//!  "partitions": [{"partition": 0, "role": "leader", "epoch": 1, "leader": 1,
//!                  "hw": 12, "durable_end": 12, "published_isr": [1, 2], "isr": [1, 2],
//!                  "isr_version": 3, "min_insync": 2, "replicas": [1, 2, 3],
//!                  "followers": [{"replica": 2, "durable_leo": 12, "fetch_offset": 12,
//!                                 "lag_ms": 40, "in_sync": true}],
//!                  "hw_publishes": 9, "max_envelopes_per_fetch": 0}]}
//! ```
//!
//! `isr` repeats `published_isr` (the 12-03 status oracle reads `isr`). The route is
//! server-local, like `/metrics`: not part of the ControlPlane / OpenAPI parity.

use ostro_log::{ReplicaStatus, Role};
use serde_json::{Value, json};

/// The metadata Raft view in the document.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaSummary {
    /// Current meta leader (0 = unknown).
    pub leader: u64,
    /// Raft term.
    pub term: u64,
    /// Voters.
    pub voters: Vec<u64>,
    /// Learners.
    pub learners: Vec<u64>,
}

/// One partition's inputs.
#[derive(Clone, Debug)]
pub struct PartitionInput {
    /// The partition.
    pub partition: u16,
    /// The local log's measured status (`None`: the partition is not running here).
    pub status: Option<ReplicaStatus>,
    /// Meta's leader for the partition (0 = none).
    pub leader: u64,
    /// Meta's replica set.
    pub replicas: Vec<u64>,
}

fn role_name(r: &Role) -> &'static str {
    match r {
        Role::Leader { .. } => "leader",
        Role::Follower { .. } => "follower",
        Role::Offline => "offline",
    }
}

/// The status document (`now_ms`: the node clock, for follower lag).
pub fn document(node_id: u64, meta: &MetaSummary, parts: &[PartitionInput], now_ms: u64) -> Value {
    let partitions: Vec<Value> = parts
        .iter()
        .map(|p| match &p.status {
            None => json!({
                "partition": p.partition,
                "role": "failed",
                "leader": p.leader,
                "replicas": p.replicas,
            }),
            Some(s) => {
                let followers: Vec<Value> = s
                    .followers
                    .iter()
                    .map(|f| {
                        json!({
                            "replica": f.replica,
                            "durable_leo": f.durable_leo.0,
                            "fetch_offset": f.fetch_offset.0,
                            "lag_ms": now_ms.saturating_sub(f.last_caught_up_ms),
                            "in_sync": f.in_sync,
                        })
                    })
                    .collect();
                json!({
                    "partition": p.partition,
                    "role": role_name(&s.role),
                    "epoch": s.epoch.0,
                    "leader": p.leader,
                    "hw": s.hw.0,
                    "durable_end": s.durable_end.0,
                    "published_isr": s.published_isr,
                    "isr": s.published_isr,
                    "isr_version": s.isr_version,
                    "min_insync": s.min_insync,
                    "replicas": p.replicas,
                    "followers": followers,
                    "hw_publishes": s.hw_publishes,
                    "max_envelopes_per_fetch": s.max_envelopes_per_fetch,
                })
            }
        })
        .collect();
    json!({
        "node_id": node_id,
        "meta": {
            "leader": meta.leader,
            "term": meta.term,
            "voters": meta.voters,
            "learners": meta.learners,
        },
        "partitions": partitions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_log::FollowerStatus;
    use ostro_types::{Epoch, Offset};

    #[test]
    fn document_shape() {
        let s = ReplicaStatus {
            role: Role::Leader { epoch: Epoch(2) },
            epoch: Epoch(2),
            hw: Offset(7),
            durable_end: Offset(9),
            published_isr: vec![1, 2],
            isr_version: 4,
            min_insync: 2,
            followers: vec![FollowerStatus {
                replica: 2,
                durable_leo: Offset(7),
                fetch_offset: Offset(9),
                last_caught_up_ms: 100,
                in_sync: true,
            }],
            hw_publishes: 3,
            max_envelopes_per_fetch: 5,
        };
        let doc = document(
            1,
            &MetaSummary {
                leader: 1,
                term: 3,
                voters: vec![1, 2, 3],
                learners: vec![],
            },
            &[
                PartitionInput {
                    partition: 0,
                    status: Some(s),
                    leader: 1,
                    replicas: vec![1, 2, 3],
                },
                PartitionInput {
                    partition: 1,
                    status: None,
                    leader: 2,
                    replicas: vec![2],
                },
            ],
            150,
        );
        let p = &doc["partitions"][0];
        assert_eq!(p["role"], "leader");
        assert_eq!(p["hw"], 7);
        assert_eq!(p["isr"], json!([1, 2]));
        assert_eq!(p["published_isr"], json!([1, 2]));
        assert_eq!(p["followers"][0]["lag_ms"], 50);
        assert_eq!(p["hw_publishes"], 3);
        assert_eq!(doc["partitions"][1]["role"], "failed");
        assert_eq!(doc["meta"]["voters"], json!([1, 2, 3]));
    }
}
