//! 13-08 Task 2: every Phase 13 oracle passes a clean input and catches a damaged one
//! (stale read, returning removed id, stuck joint state, even voter set, gated command or
//! ext tag before finalization, RepinRun without finalization, reject-and-continue,
//! `module_gone` for a live run).

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use ostro_api::phase13::{ClusterMember, ClusterStatus};
use ostro_meta::ext::{
    ClusterVersion, MEMBER_ACTIVE, MEMBER_REMOVED, MemberState, Reassignment, Retired,
    put_cluster_version, put_member_state, put_reassignment, put_retired,
};
use ostro_meta::{
    ClusterInfo, Member, MetaCommand, MetaKey, MetaRecord, MetaResult, MetaState, ModuleRecord,
    PartitionAssignment,
};
use ostro_partition::apply::{ApplyOutcome, InvalidCode, RejectReason};
use ostro_repl_conformance::harness::MetaLogEntry;
use ostro_repl_conformance::harness::inproc::MetaLogDump;
use ostro_repl_conformance::oracle::disk::{LogScan, ScannedRecord};
use ostro_repl_conformance::oracle::p13_gc::{RetireEvent, RunResult, no_module_gone_for_live};
use ostro_repl_conformance::oracle::p13_membership::{
    StatusSample, joint_left_within, removed_never_return, voters_odd,
};
use ostro_repl_conformance::oracle::p13_ryw::{Effect, Observation, RywChecker};
use ostro_repl_conformance::oracle::p13_upgrade::{
    OutcomeRecord, finalization_index, no_gated_before_finalize, no_gated_partition_commands,
    no_reject_continue,
};
use ostro_types::command::{Command, RepinRun, StartRun, encode_command};
use ostro_types::{Cmd, Epoch, Offset, PartitionId};
use serde_json::json;

// ---- RYW ------------------------------------------------------------------------------

fn run_obs(run_id: &str, found: bool, status: Option<&str>, signals: &[&str]) -> Observation {
    Observation::Run {
        run_id: run_id.to_owned(),
        found,
        status: status.map(str::to_owned),
        signals: signals.iter().map(|s| (*s).to_owned()).collect(),
    }
}

#[test]
fn ryw_checker_passes_fresh_reads() {
    let c = RywChecker::new();
    let a = c.session("a");
    // A read before the write may miss it.
    a.observe(run_obs("r1", false, None, &[]));
    a.ack(Effect::RunExists {
        run_id: "r1".into(),
    });
    a.observe(run_obs("r1", true, Some("running"), &[]));
    a.ack(Effect::SignalVisible {
        run_id: "r1".into(),
        request_id: "s1".into(),
    });
    a.observe(run_obs("r1", true, Some("running"), &["s1"]));
    a.ack(Effect::RunTerminal {
        run_id: "r1".into(),
    });
    a.observe(run_obs("r1", true, Some("cancelled"), &["s1"]));
    a.ack(Effect::ActiveVersion {
        workflow: "w".into(),
        version: 2,
    });
    a.observe(Observation::ActiveVersion {
        workflow: "w".into(),
        version: Some(2),
    });
    a.ack(Effect::Policy {
        key: "workflow/w".into(),
        value: json!({"max": 3}),
    });
    a.observe(Observation::Policy {
        key: "workflow/w".into(),
        value: Some(json!({"max": 3})),
    });
    // A read issued before the ack (answer recorded later) may be stale.
    let b = c.session("b");
    let t = b.begin_read();
    b.ack(Effect::RunExists {
        run_id: "r2".into(),
    });
    b.observe_issued(t, run_obs("r2", false, None, &[]));
    // Another session's writes are not this session's.
    c.session("x").observe(run_obs("r2", false, None, &[]));
    let r = c.check();
    assert!(r.is_ok(), "{r}");
}

#[test]
fn ryw_checker_catches_stale_read() {
    let c = RywChecker::new();
    let s = c.session("s");
    s.ack(Effect::RunExists {
        run_id: "r1".into(),
    });
    s.observe(run_obs("r1", false, None, &[]));
    let r = c.check();
    assert!(!r.is_ok());
    assert!(r.mentions("run r1 not found"), "{r}");

    for (effect, obs, needle) in [
        (
            Effect::SignalVisible {
                run_id: "r".into(),
                request_id: "sig-9".into(),
            },
            run_obs("r", true, Some("running"), &[]),
            "signal sig-9",
        ),
        (
            Effect::RunTerminal { run_id: "r".into() },
            run_obs("r", true, Some("running"), &[]),
            "cancel acked",
        ),
        (
            Effect::ActiveVersion {
                workflow: "w".into(),
                version: 3,
            },
            Observation::ActiveVersion {
                workflow: "w".into(),
                version: Some(2),
            },
            "version 3 activated",
        ),
        (
            Effect::Policy {
                key: "k".into(),
                value: json!(1),
            },
            Observation::Policy {
                key: "k".into(),
                value: None,
            },
            "policy k",
        ),
    ] {
        let c = RywChecker::new();
        let s = c.session("s");
        s.ack(effect);
        s.observe(obs);
        let r = c.check();
        assert!(r.mentions(needle), "{needle}: {r}");
    }
}

// ---- membership -----------------------------------------------------------------------

fn entry(index: u64, command: MetaCommand) -> MetaLogEntry {
    MetaLogEntry {
        index,
        command,
        applied_result: MetaResult::Ok { version: index },
    }
}

fn member_put(id: u64, state: u8) -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Member(id),
        expected_version: None,
        record: MetaRecord::Member(Member {
            node_id: id,
            addr: format!("127.0.0.1:{id}"),
            ext: put_member_state(
                &[],
                &MemberState {
                    state,
                    changed_at_ms: 1,
                },
            )
            .expect("ext"),
            ..Member::default()
        }),
    }
}

fn dump(entries: Vec<MetaLogEntry>, voters: Vec<u64>) -> MetaLogDump {
    let mut state = MetaState::default();
    for e in &entries {
        state.apply(e.index, e.command.clone());
    }
    MetaLogDump {
        voters,
        learners: Vec::new(),
        commit: entries.last().map_or(0, |e| e.index),
        snapshot_index: 0,
        entries,
        undecodable: Vec::new(),
        snapshot_state: MetaState::default(),
        state,
    }
}

fn status(voters: &[u64], outgoing: &[u64], members: &[(u64, &str)]) -> ClusterStatus {
    ClusterStatus {
        cluster_id: "c".into(),
        members: members
            .iter()
            .map(|(id, st)| ClusterMember {
                node_id: *id,
                state: (*st).to_owned(),
                ..ClusterMember::default()
            })
            .collect(),
        voters: voters.to_vec(),
        voters_outgoing: outgoing.to_vec(),
        meta_leader: voters.first().copied().unwrap_or(0),
        finalized: 2,
        level_min: 1,
        level_max: 2,
        ..ClusterStatus::default()
    }
}

fn sample(ms: u64, st: ClusterStatus) -> StatusSample {
    StatusSample {
        at: Duration::from_millis(ms),
        node: 1,
        status: st,
    }
}

fn clean_membership() -> (BTreeMap<u64, MetaLogDump>, Vec<StatusSample>) {
    let log = vec![
        entry(1, member_put(1, MEMBER_ACTIVE)),
        entry(2, member_put(2, MEMBER_ACTIVE)),
        entry(3, member_put(3, MEMBER_ACTIVE)),
        entry(
            4,
            MetaCommand::AlterIsr {
                partition: 0,
                epoch: 1,
                expected_isr_version: 0,
                isr: vec![1, 2, 3],
            },
        ),
        entry(
            5,
            MetaCommand::AlterIsr {
                partition: 0,
                epoch: 1,
                expected_isr_version: 1,
                isr: vec![1, 2],
            },
        ),
        entry(6, member_put(3, MEMBER_REMOVED)),
    ];
    let logs = BTreeMap::from([(1, dump(log, vec![1, 2, 4]))]);
    let statuses = vec![
        sample(
            0,
            status(
                &[1, 2, 3],
                &[],
                &[(1, "active"), (2, "active"), (3, "active")],
            ),
        ),
        sample(100, status(&[1, 2, 4], &[1, 2, 3], &[(3, "draining")])),
        sample(
            200,
            status(&[1, 2, 4], &[], &[(3, "removed"), (4, "active")]),
        ),
        sample(300, status(&[1, 2, 4], &[], &[(3, "removed")])),
    ];
    (logs, statuses)
}

#[test]
fn membership_oracles_pass_a_clean_change() {
    let (logs, statuses) = clean_membership();
    let r = removed_never_return(&logs, &statuses);
    assert!(r.is_ok(), "{r}");
    let r = joint_left_within(&statuses, Duration::from_secs(5));
    assert!(r.is_ok(), "{r}");
    let r = voters_odd(&statuses);
    assert!(r.is_ok(), "{r}");
}

#[test]
fn removed_id_returning_detected() {
    // In the meta log: an ISR write naming the tombstoned node after its tombstone.
    let (mut logs, statuses) = clean_membership();
    let d = logs.get_mut(&1).expect("node 1");
    let mut entries = d.entries.clone();
    entries.push(entry(
        7,
        MetaCommand::AlterIsr {
            partition: 0,
            epoch: 1,
            expected_isr_version: 2,
            isr: vec![1, 3],
        },
    ));
    *d = dump(entries, vec![1, 2, 4]);
    let r = removed_never_return(&logs, &statuses);
    assert!(r.mentions("removed node 3 (tombstone at 6) back"), "{r}");

    // In the persisted ConfState.
    let (mut logs, statuses) = clean_membership();
    let d = logs.get_mut(&1).expect("node 1");
    d.learners = vec![3];
    let r = removed_never_return(&logs, &statuses);
    assert!(r.mentions("in the persisted ConfState"), "{r}");

    // In a later status sample.
    let (logs, mut statuses) = clean_membership();
    statuses.push(sample(400, status(&[1, 2, 3], &[], &[(3, "active")])));
    let r = removed_never_return(&logs, &statuses);
    assert!(r.mentions("node 3 (removed by 200ms) is back"), "{r}");

    // Re-written as a member.
    let (mut logs, statuses) = clean_membership();
    let d = logs.get_mut(&1).expect("node 1");
    let mut entries = d.entries.clone();
    entries.push(entry(7, member_put(3, MEMBER_ACTIVE)));
    *d = dump(entries, vec![1, 2, 4]);
    let r = removed_never_return(&logs, &statuses);
    assert!(r.mentions("re-written as not removed"), "{r}");
}

#[test]
fn stuck_joint_state_detected() {
    let statuses = vec![
        sample(0, status(&[1, 2, 4], &[1, 2, 3], &[])),
        sample(3_000, status(&[1, 2, 4], &[1, 2, 3], &[])),
        sample(5_500, status(&[1, 2, 4], &[1, 2, 3], &[])),
        sample(9_000, status(&[1, 2, 4], &[1, 2, 3], &[])),
    ];
    let r = joint_left_within(&statuses, Duration::from_secs(5));
    assert_eq!(r.violations.len(), 1, "reported once per streak: {r}");
    assert!(r.mentions("still joint at 5.5s"), "{r}");
}

#[test]
fn even_voter_set_detected() {
    let statuses = vec![
        sample(0, status(&[1, 2, 3], &[], &[])),
        sample(10, status(&[1, 2, 3, 4], &[1, 2, 3], &[])), // joint: not judged
        sample(20, status(&[1, 2, 3, 4], &[], &[])),
    ];
    let r = voters_odd(&statuses);
    assert_eq!(r.violations.len(), 1, "{r}");
    assert!(r.mentions("even voter set [1, 2, 3, 4]"), "{r}");
}

// ---- upgrade --------------------------------------------------------------------------

fn finalize(index: u64, level: u32) -> MetaLogEntry {
    entry(
        index,
        MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: Some(1),
            record: MetaRecord::Cluster(ClusterInfo {
                cluster_id: "c".into(),
                partitions: 4,
                format: 1,
                created_at_ms: 1,
                ext: put_cluster_version(
                    &[],
                    &ClusterVersion {
                        finalized: level,
                        hold: None,
                        finalized_at_ms: 1,
                    },
                )
                .expect("ext"),
            }),
        },
    )
}

fn alter_replicas(index: u64) -> MetaLogEntry {
    entry(
        index,
        MetaCommand::AlterReplicas {
            partition: 0,
            epoch: 1,
            expected_isr_version: 0,
            replicas: vec![1, 2],
            isr: vec![1, 2],
            min_insync: 1,
            reassignment: Some(Reassignment {
                target: vec![1, 2],
                adding: vec![2],
                removing: vec![],
                started_at_ms: 1,
            }),
        },
    )
}

fn partition_put_with_reassignment(index: u64) -> MetaLogEntry {
    entry(
        index,
        MetaCommand::Put {
            key: MetaKey::Partition(1),
            expected_version: None,
            record: MetaRecord::Partition(PartitionAssignment {
                partition: 1,
                leader: 1,
                epoch: 2,
                isr: vec![1],
                replicas: vec![1],
                isr_version: 0,
                min_insync: 1,
                ext: put_reassignment(&[], &Reassignment::default()).expect("ext"),
            }),
        },
    )
}

fn retired_module(index: u64) -> MetaLogEntry {
    entry(
        index,
        MetaCommand::Put {
            key: MetaKey::Module([7; 32]),
            expected_version: Some(1),
            record: MetaRecord::Module(ModuleRecord {
                sha256: [7; 32],
                ext: put_retired(&[], &Retired { retired_at_ms: 5 }).expect("ext"),
                ..ModuleRecord::default()
            }),
        },
    )
}

fn scan_with(cmds: Vec<Command>) -> LogScan {
    let commands: Vec<Bytes> = cmds.iter().map(encode_command).collect();
    LogScan {
        node: 2,
        partition: PartitionId(3),
        records: vec![ScannedRecord {
            base: Offset(10),
            end: Offset(10 + commands.len() as u64),
            epoch: Epoch(1),
            envelope: Bytes::new(),
            commands,
        }],
        stopped: None,
    }
}

fn repin() -> Command {
    Command {
        cmd: Some(Cmd::RepinRun(RepinRun::default())),
    }
}

fn start_run() -> Command {
    Command {
        cmd: Some(Cmd::StartRun(StartRun::default())),
    }
}

#[test]
fn upgrade_oracle_passes_gated_writes_after_finalization() {
    let log = vec![
        entry(1, member_put(1, MEMBER_ACTIVE)),
        finalize(2, 1),
        finalize(3, 2),
        alter_replicas(4),
        partition_put_with_reassignment(5),
        retired_module(6),
    ];
    assert_eq!(finalization_index(1, &log, 2), Some(3));
    let r = no_gated_before_finalize(&log, &[scan_with(vec![start_run(), repin()])]);
    assert!(r.is_ok(), "{r}");
    // A finalization to 1 only does not open level-2 features; plain commands are fine.
    let r = no_gated_before_finalize(&log[..2], &[scan_with(vec![start_run()])]);
    assert!(r.is_ok(), "{r}");
}

#[test]
fn gated_command_before_finalize_detected() {
    // An injected early AlterReplicas (index 2, finalization at 3).
    let log = vec![
        entry(1, member_put(1, MEMBER_ACTIVE)),
        alter_replicas(2),
        finalize(3, 2),
    ];
    let r = no_gated_before_finalize(&log, &[]);
    assert!(r.mentions("meta index 2: "), "{r}");
    assert!(r.mentions("finalized at index 3"), "{r}");
    // Never finalized: gated ext tags and RepinRun are violations too.
    let log = vec![
        entry(1, member_put(1, MEMBER_ACTIVE)),
        partition_put_with_reassignment(2),
        retired_module(3),
    ];
    let r = no_gated_before_finalize(&log, &[scan_with(vec![start_run(), repin()])]);
    assert!(r.mentions("meta index 2"), "ext 100: {r}");
    assert!(r.mentions("meta index 3"), "ext 102: {r}");
    assert!(r.mentions("finalized at never"), "{r}");
    assert!(
        r.mentions("node 2 partition 3 offset 11: RepinRun"),
        "RepinRun without finalization: {r}"
    );
    // Partition scans captured before finalization.
    let r = no_gated_partition_commands(&[scan_with(vec![repin()])]);
    assert!(r.mentions("offset 10: RepinRun"), "{r}");
}

#[test]
fn reject_and_continue_detected() {
    let ok = OutcomeRecord {
        node: 1,
        partition: Some(0),
        offset: 4,
        command: encode_command(&start_run()),
        outcome: ApplyOutcome::Accepted,
    };
    // An empty Command is a legitimate Invalid(Empty).
    let empty = OutcomeRecord {
        offset: 5,
        command: Bytes::new(),
        outcome: ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Empty)),
        ..ok.clone()
    };
    let r = no_reject_continue(&[ok.clone(), empty.clone()]);
    assert!(r.is_ok(), "{r}");
    // An injected outcome: non-empty bytes (a newer command) rejected as Empty.
    let skipped = OutcomeRecord {
        offset: 6,
        command: encode_command(&repin()),
        outcome: ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Empty)),
        ..ok.clone()
    };
    let r = no_reject_continue(&[ok, empty, skipped]);
    assert_eq!(r.violations.len(), 1, "{r}");
    assert!(
        r.mentions("partition 0 offset 6: Rejected(Invalid(Empty))"),
        "{r}"
    );
}

// ---- GC -------------------------------------------------------------------------------

#[test]
fn gc_oracle_passes_terminal_or_other_failures() {
    let events = vec![RetireEvent {
        module_sha: "ab".into(),
        non_terminal_runs: vec!["r1".into(), "r2".into()],
    }];
    let results = vec![
        RunResult::from_json(&json!({"run_id": "r1", "status": "completed"})),
        RunResult::from_json(
            &json!({"run_id": "r2", "status": "failed", "error": {"code": "user_error"}}),
        ),
        // A run that was already terminal (not listed) may see module_gone on a re-run.
        RunResult::from_json(
            &json!({"run_id": "r3", "status": "failed", "error": {"code": "module_gone"}}),
        ),
    ];
    let r = no_module_gone_for_live(&events, &results);
    assert!(r.is_ok(), "{r}");
}

#[test]
fn module_gone_for_live_run_detected() {
    let events = vec![RetireEvent {
        module_sha: "ab".into(),
        non_terminal_runs: vec!["r1".into()],
    }];
    let gone = RunResult::from_json(
        &json!({"run_id": "r1", "status": "failed", "error": {"code": "module_gone", "message": "module ab was collected"}}),
    );
    assert!(gone.is_module_gone());
    let r = no_module_gone_for_live(&events, &[gone]);
    assert!(
        r.mentions("run r1 was live when module ab was retired"),
        "{r}"
    );
}
