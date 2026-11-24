//! Phase 13 (13-03) wire contracts: every message of `ostro/api/v1/cluster.proto` and
//! `ostro/cluster/v1/membership.proto` round-trips, the services exist under the reserved
//! names (D-P13-23), and secrets stay out of `Debug`.

use ostro_proto::api::v1 as api;
use ostro_proto::cluster::v1 as cl;
use prost::Message;

fn rt<M: Message + Default + PartialEq + std::fmt::Debug>(m: M) {
    let bytes = m.encode_to_vec();
    let back = M::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, m);
}

fn version() -> api::NodeVersion {
    api::NodeVersion {
        ostro_version: "0.2.0".into(),
        level_min: 1,
        level_max: 2,
        wasmtime: "50.0.2".into(),
        event_codec_ver: 1,
        wit_lines: vec!["0.1".into(), "0.2".into()],
    }
}

fn status() -> api::ClusterStatus {
    api::ClusterStatus {
        cluster_id: "c-1".into(),
        members: vec![
            api::ClusterMember {
                node_id: 1,
                addr: "10.0.0.1:7070".into(),
                cluster_addr: Some("10.0.0.1:7071".into()),
                state: "active".into(),
                version: Some(version()),
                live: true,
                raft_role: "voter".into(),
            },
            api::ClusterMember {
                node_id: 4,
                addr: "10.0.0.4:7070".into(),
                cluster_addr: None,
                state: "removed".into(),
                version: None,
                live: false,
                raft_role: "none".into(),
            },
        ],
        voters: vec![1, 2, 3],
        learners: vec![5],
        voters_outgoing: vec![],
        meta_leader: 2,
        finalized: 1,
        hold: Some(1),
        level_min: 1,
        level_max: 2,
        policy: Some(api::ClusterReplicationPolicy {
            rf: 3,
            min_insync: 2,
            max_voters: 5,
        }),
        reassignments: vec![api::PartitionReassignment {
            partition: 7,
            target: vec![1, 2, 5],
            adding: vec![5],
            removing: vec![3],
            started_at_ms: 1_700_000_000_000,
        }],
    }
}

fn upgrade() -> api::UpgradeStatus {
    api::UpgradeStatus {
        finalized: 1,
        hold: None,
        common_level_max: 2,
        auto_finalize: true,
        members: vec![api::MemberLevel {
            node_id: 1,
            level_min: 1,
            level_max: 2,
            live: true,
        }],
    }
}

#[test]
fn cluster_messages_round_trip() {
    rt(api::InitClusterRequest {
        advertise: Some("10.0.0.1:7071".into()),
        rf: Some(3),
        min_insync: Some(2),
    });
    rt(api::InitClusterRequest::default());
    rt(api::JoinClusterRequest {
        token: "njt_abc.secret".into(),
        csr_pem: "-----BEGIN CERTIFICATE REQUEST-----".into(),
        advertise: Some(api::NodeAdvertise {
            api: "10.0.0.5:7070".into(),
            cluster: "10.0.0.5:7071".into(),
            repl: Some("10.0.0.5:7072".into()),
        }),
        node_version: Some(version()),
        resume_node_id: Some(5),
    });
    rt(api::JoinClusterResponse {
        node_id: 5,
        cluster_id: "c-1".into(),
        partitions: 64,
        ca_cert_pem: "ca".into(),
        node_cert_pem: "cert".into(),
        seeds: vec![api::SeedNode {
            id: 1,
            addr: "10.0.0.1:7071".into(),
            repl_addr: None,
        }],
        finalized_level: 1,
    });
    rt(api::ClusterStatusRequest {});
    rt(api::CreateNodeTokenRequest {
        ttl_secs: Some(3600),
    });
    rt(api::NodeTokenCreated {
        id: "njt_abc".into(),
        token: "njt_abc.secret".into(),
        expires_at_ms: 42,
        ca_hash: "sha256:00".into(),
    });
    rt(status());
    rt(api::RemoveMemberRequest {
        node_id: 3,
        force: true,
    });
    rt(api::DrainMemberRequest { node_id: 3 });
    rt(api::ReassignPartitionRequest {
        partition: 7,
        replicas: vec![1, 2, 5],
        cancel: false,
    });
    rt(api::RebalanceRequest {});
    rt(upgrade());
    rt(api::UpgradeStatusRequest {});
    rt(api::UpgradeFinalizeRequest {});
    rt(api::UpgradeHoldRequest {});
    rt(api::UpgradeReleaseRequest {});
}

#[test]
fn module_lifecycle_messages_round_trip() {
    rt(api::RetireModuleRequest {
        sha256: "ab".repeat(32),
    });
    rt(api::RetireModuleResponse {
        sha256: "ab".repeat(32),
        retired_at_ms: 9,
        versions: vec![api::WorkflowVersionRef {
            name: "order".into(),
            version: 2,
        }],
    });
    rt(api::RunModuleGcRequest {});
    rt(api::GcRunReport {
        candidates: vec!["a".into(), "b".into()],
        deleted: vec!["a".into()],
        referenced: vec!["b".into()],
        postponed: true,
        reason: Some("uncovered partitions".into()),
    });
    for from in [
        api::migrate_runs_request::From::FromVersion(1),
        api::migrate_runs_request::From::FromSha("cd".repeat(32)),
    ] {
        rt(api::MigrateRunsRequest {
            workflow: "order".into(),
            from: Some(from),
            to_version: 3,
            filter: Some("tag:x".into()),
            limit: Some(10),
            dry_run: true,
        });
    }
    rt(api::MigrateRunsReport {
        passed: vec!["r1".into()],
        repinned: vec!["r1".into()],
        failed: vec![api::MigrateFailure {
            run_id: "r2".into(),
            seq: 12,
            detail: "diverged".into(),
        }],
        busy: vec!["r3".into()],
        seq_mismatch: vec!["r4".into()],
    });
}

#[test]
fn membership_and_node_ops_messages_round_trip() {
    rt(cl::RemovedNotice {
        node_id: 4,
        cluster_id: "c-1".into(),
        reason: "removed by operator".into(),
    });
    rt(cl::RemovedAck {});
    rt(cl::HandOffRequest {
        partition: 3,
        to: 2,
        epoch: 7,
    });
    rt(cl::HandOffResponse {
        moved: true,
        new_epoch: 8,
        abort_reason: String::new(),
    });
    rt(cl::HandOffResponse {
        moved: false,
        new_epoch: 0,
        abort_reason: "target lagging".into(),
    });
    rt(cl::ModuleRefsRequest {
        shas: vec![vec![7; 32], vec![8; 32]],
        protect_resumable: true,
        partitions: vec![cl::PartitionEpoch {
            partition: 1,
            epoch: 2,
        }],
    });
    rt(cl::ModuleRefsResponse {
        referenced: vec![vec![7; 32]],
        uncovered: vec![cl::PartitionEpoch {
            partition: 1,
            epoch: 2,
        }],
    });
}

#[test]
fn optional_fields_keep_presence() {
    // `hold: None` and `hold: Some(0)` are different statuses.
    let mut s = status();
    s.hold = Some(0);
    let back = api::ClusterStatus::decode(s.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(back.hold, Some(0));
    s.hold = None;
    let back = api::ClusterStatus::decode(s.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(back.hold, None);
    // An absent `from` is distinguishable (the server answers INVALID_ARGUMENT).
    let m = api::MigrateRunsRequest::default();
    let back = api::MigrateRunsRequest::decode(m.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(back.from, None);
}

#[test]
fn services_use_the_reserved_names() {
    use ostro_proto::api::v1::{cluster_server, module_lifecycle_server};
    use ostro_proto::cluster::v1::{membership_server, node_ops_server};
    assert_eq!(cluster_server::SERVICE_NAME, "ostro.api.v1.Cluster");
    assert_eq!(
        module_lifecycle_server::SERVICE_NAME,
        "ostro.api.v1.ModuleLifecycle"
    );
    assert_eq!(
        membership_server::SERVICE_NAME,
        "ostro.cluster.v1.Membership"
    );
    assert_eq!(node_ops_server::SERVICE_NAME, "ostro.cluster.v1.NodeOps");
}

#[test]
fn join_and_token_secrets_are_redacted_in_debug() {
    let j = api::JoinClusterRequest {
        token: "njt_abc.supersecret".into(),
        ..api::JoinClusterRequest::default()
    };
    let t = api::NodeTokenCreated {
        token: "njt_abc.supersecret".into(),
        ..api::NodeTokenCreated::default()
    };
    for s in [format!("{j:?}"), format!("{t:?}")] {
        assert!(!s.contains("supersecret"), "{s}");
        assert!(s.contains("<redacted>"), "{s}");
    }
}
