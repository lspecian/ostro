//! 13-18 (held for approval): the LA-rule and election audits on a **compacted** meta log
//! (the snapshot a learner join makes the leader take, 13-05 / 13-09 note).
//!
//! Each test shows (1) the false result of the old reading on a correct compacted cluster
//! history, (2) that the snapshot-aware reading accepts it, and (3) that the new reading
//! still flags every damaged variant.

use bytes::Bytes;
use ostro_meta::ext::ElectionInfo;
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaState, PartitionAssignment};
use ostro_repl_conformance::harness::MetaLogEntry;
use ostro_repl_conformance::oracle::disk::{LogScan, ScannedRecord};
use ostro_repl_conformance::oracle::{election, la_rule};
use ostro_types::command::encode_command;
use ostro_types::{Command, Epoch, Offset, PartitionId};

fn init() -> MetaCommand {
    MetaCommand::InitCluster {
        cluster_id: "conf-0".into(),
        node_id: 1,
        partitions: 1,
        created_at_ms: 0,
        token_key: [3; 32],
    }
}

fn placement() -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Partition(0),
        expected_version: Some(0),
        record: MetaRecord::Partition(PartitionAssignment {
            partition: 0,
            leader: 1,
            epoch: 1,
            isr: vec![1, 2, 3],
            replicas: vec![1, 2, 3],
            isr_version: 1,
            min_insync: 2,
            ext: Vec::new(),
        }),
    }
}

fn elect(epoch: u32, leader: u64, isr: Vec<u64>, expected_isr_version: u64) -> MetaCommand {
    MetaCommand::ElectLeader {
        partition: 0,
        epoch,
        leader,
        isr,
        expected_isr_version,
        info: ElectionInfo::default(),
    }
}

/// A full history split at `at`: `(snapshot state after entries 1..=at, entries after)`.
fn compacted(cmds: Vec<MetaCommand>, at: usize) -> (MetaState, Vec<MetaLogEntry>) {
    let mut state = MetaState::default();
    let mut snapshot = None;
    let mut entries = Vec::new();
    for (i, command) in cmds.into_iter().enumerate() {
        let index = i as u64 + 1;
        let applied_result = state.apply(index, command.clone());
        if i + 1 == at {
            snapshot = Some(state.clone());
        } else if i + 1 > at {
            entries.push(MetaLogEntry {
                index,
                command,
                applied_result,
            });
        }
    }
    (
        snapshot.expect("snapshot point inside the history"),
        entries,
    )
}

/// The isr_version the partition record has after `state`.
fn isr_version(state: &MetaState) -> u64 {
    state.partition(0).expect("placed").value.isr_version
}

fn la(epoch: u32, node: u64) -> Bytes {
    encode_command(&Command::leader_announced(epoch, node, 0))
}

/// One record per epoch, each starting with `LeaderAnnounced{epoch, node}`.
fn scan(epochs: &[(u32, u64)]) -> LogScan {
    LogScan {
        node: 1,
        partition: PartitionId(0),
        records: epochs
            .iter()
            .enumerate()
            .map(|(i, (e, n))| ScannedRecord {
                base: Offset(i as u64),
                end: Offset(i as u64 + 1),
                epoch: Epoch(*e),
                envelope: Bytes::new(),
                commands: vec![la(*e, *n)],
            })
            .collect(),
        stopped: None,
    }
}

/// History: placement (epoch 1, leader 1), elections epoch 2 → node 2, epoch 3 → node 3;
/// the snapshot is taken after epoch 2's election.
fn history() -> (MetaState, Vec<MetaLogEntry>) {
    let (s1, _) = compacted(vec![init(), placement()], 2);
    let v1 = isr_version(&s1);
    let (s2, _) = compacted(vec![init(), placement(), elect(2, 2, vec![1, 2, 3], v1)], 3);
    let v2 = isr_version(&s2);
    compacted(
        vec![
            init(),
            placement(),
            elect(2, 2, vec![1, 2, 3], v1),
            elect(3, 3, vec![1, 2, 3], v2),
        ],
        3,
    )
}

#[test]
fn la_rule_on_a_compacted_meta_log() {
    let (snapshot, entries) = history();
    assert!(
        entries
            .iter()
            .all(|e| !matches!(e.command, MetaCommand::ElectLeader { epoch: 2, .. })),
        "epoch 2's election is compacted into the snapshot"
    );
    let good = scan(&[(1, 1), (2, 2), (3, 3)]);

    // (1) The old reading (log entries only) reports a correct log.
    let old = la_rule::check_scan(&good, &entries);
    assert!(
        old.mentions("no leader for epoch 1") && old.mentions("no leader for epoch 2"),
        "old reading: {old}"
    );
    // (2) Snapshot-aware: accepted.
    let new = la_rule::check_scan_from(&good, &snapshot, &entries);
    assert!(new.is_ok(), "{new}");

    // (3) Still strict.
    // The snapshot's epoch announced by the wrong node.
    let bad = la_rule::check_scan_from(&scan(&[(1, 1), (2, 3), (3, 3)]), &snapshot, &entries);
    assert!(
        bad.mentions("LeaderAnnounced by node 3 but meta's leader for epoch 2 is node 2"),
        "{bad}"
    );
    // An epoch after the snapshot announced by the wrong node.
    let bad = la_rule::check_scan_from(&scan(&[(1, 1), (2, 2), (3, 1)]), &snapshot, &entries);
    assert!(
        bad.mentions("LeaderAnnounced by node 1 but meta's leader for epoch 3 is node 3"),
        "{bad}"
    );
    // An epoch meta never committed (after the snapshot).
    let bad = la_rule::check_scan_from(
        &scan(&[(1, 1), (2, 2), (3, 3), (4, 2)]),
        &snapshot,
        &entries,
    );
    assert!(bad.mentions("no leader for epoch 4"), "{bad}");
    // An epoch without an announcement.
    let mut no_la = scan(&[(1, 1), (2, 2), (3, 3)]);
    no_la.records[2].commands = vec![encode_command(&Command::leader_announced(2, 3, 0))];
    let bad = la_rule::check_scan_from(&no_la, &snapshot, &entries);
    assert!(bad.mentions("LeaderAnnounced carries epoch 2"), "{bad}");
    // Without a snapshot the reading is the old one exactly.
    let full = compacted(
        vec![
            init(),
            placement(),
            elect(2, 2, vec![1, 2, 3], 1),
            elect(3, 3, vec![1, 2, 3], 2),
        ],
        1,
    );
    assert_eq!(
        la_rule::check_scan_from(&good, &MetaState::default(), &full.1).violations,
        la_rule::check_scan(&good, &full.1).violations
    );
}

#[test]
fn election_audit_on_a_compacted_meta_log() {
    let (snapshot, entries) = history();
    // (2) A correct compacted history passes.
    let ok = election::isr_only_from(snapshot.clone(), &entries);
    assert!(ok.is_ok(), "{ok}");

    // (1)+(3) A SetPartitionLeader right after a snapshot that compacted the replicated
    // bootstrap and every election. The old replay started "not bootstrapped" from any
    // snapshot and let it through; the snapshot-aware replay flags it as the full log does.
    let (s1, _) = compacted(vec![init(), placement()], 2);
    let v1 = isr_version(&s1);
    let mut all = vec![init(), placement(), elect(2, 2, vec![1, 2, 3], v1)];
    let (mut state, _) = compacted(all.clone(), 3);
    let late = MetaCommand::SetPartitionLeader {
        partition: 0,
        epoch: 9,
        leader: 2,
        isr: vec![2],
    };
    all.push(late.clone());
    let (snapshot_all, _) = compacted(all.clone(), 3);
    let applied_result = state.apply(4, late.clone());
    let only_late = vec![MetaLogEntry {
        index: 4,
        command: late,
        applied_result,
    }];
    let bad = election::isr_only_from(snapshot_all, &only_late);
    assert!(
        bad.mentions("SetPartitionLeader partition 0 epoch 9 after cluster bootstrap"),
        "{bad}"
    );
    // The full log flags it too (the reference reading).
    let (_, full) = compacted(
        vec![
            init(),
            placement(),
            elect(2, 2, vec![1, 2, 3], v1),
            all[3].clone(),
        ],
        1,
    );
    let mut full_log = vec![MetaLogEntry {
        index: 1,
        command: init(),
        applied_result: ostro_meta::MetaResult::Ok { version: 1 },
    }];
    full_log.extend(full);
    let reference = election::isr_only(&full_log);
    assert!(
        reference.mentions("SetPartitionLeader partition 0 epoch 9 after cluster bootstrap"),
        "{reference}"
    );

    // (3) A second placement after the snapshot is still flagged, and an election outside
    // the published ISR too.
    let (snapshot, mut entries) = history();
    entries.push(MetaLogEntry {
        index: 99,
        command: placement(),
        applied_result: ostro_meta::MetaResult::Ok { version: 0 },
    });
    let bad = election::isr_only_from(snapshot.clone(), &entries);
    assert!(bad.mentions("is not the bootstrap placement"), "{bad}");
    let (snapshot, mut entries) = history();
    entries.push(MetaLogEntry {
        index: 99,
        command: elect(4, 7, vec![7], 9),
        applied_result: ostro_meta::MetaResult::Ok { version: 0 },
    });
    let bad = election::isr_only_from(snapshot, &entries);
    assert!(
        bad.mentions("elects node 7 outside the published ISR"),
        "{bad}"
    );

    // An RF=1 SetPartitionLeader history compacted into a snapshot stays unflagged.
    let rf1 = |e: u32| MetaCommand::SetPartitionLeader {
        partition: 0,
        epoch: e,
        leader: 1,
        isr: vec![1],
    };
    let (snapshot, entries) = compacted(vec![init(), rf1(1), rf1(2)], 2);
    let ok = election::isr_only_from(snapshot, &entries);
    assert!(ok.is_ok(), "RF=1 SetPartitionLeader log: {ok}");
}
