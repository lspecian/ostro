//! 14-09 Task 1: the checkers bite. Each checker passes on a clean synthetic run and reports
//! exactly the anomaly injected into it: a lost acknowledged run, a duplicated journal seq, a
//! divergent replica entry, two leaders in one epoch, an acknowledged offset overwritten
//! after a truncation, and a stale attempt token accepted by apply. Pure: no cluster.

use std::collections::BTreeMap;

use ostro_jepsen::checker::duplicate::check_duplicates;
use ostro_jepsen::checker::fencing::check_fencing;
use ostro_jepsen::checker::lost::{JournalEvent, LostInput, RunObservation, check_lost};
use ostro_jepsen::checker::{
    Applied, Entry, LogExport, Op, PartitionLogs, Record, ReplicaLog, Verdict, fnv64,
};
use ostro_jepsen::history::{ClientOp, OpKind, Outcome, PartitionToken};
use ostro_types::{PartitionId, RunId};
use serde_json::{Value, json};

const WF: &str = "crash-saga";
const KEY: &str = "k-1";
const RID: &str = "k-1-go";

/// One synthetic scenario: the client history, the observations and the logs.
struct World {
    run: String,
    ops: Vec<ClientOp>,
    runs: BTreeMap<String, RunObservation>,
    expected: BTreeMap<String, Value>,
    logs: LogExport,
}

impl World {
    fn lost(&self) -> Verdict {
        check_lost(LostInput {
            ops: &self.ops,
            runs: &self.runs,
            expected: &self.expected,
            logs: Some(&self.logs),
        })
    }

    fn duplicate(&self) -> Verdict {
        check_duplicates(&self.logs, &self.runs)
    }

    fn fencing(&self) -> Verdict {
        check_fencing(&self.logs, &self.ops)
    }

    fn replicas(&mut self) -> &mut Vec<ReplicaLog> {
        &mut self.logs.partitions[0].replicas
    }
}

fn entry(offset: u64, epoch: u32, op: Op) -> Entry {
    Entry {
        offset,
        epoch,
        digest: fnv64(format!("{op:?}").as_bytes()),
        op,
    }
}

/// A replica holding `entries` (one record per command), every command accepted except the
/// offsets in `rejected`.
fn replica(node: u64, entries: &[Entry], rejected: &[u64]) -> ReplicaLog {
    ReplicaLog {
        node,
        in_isr: true,
        records: entries
            .iter()
            .map(|e| Record {
                base: e.offset,
                end: e.offset + 1,
                epoch: e.epoch,
                len: 16,
                digest: e.digest,
            })
            .collect(),
        entries: entries.to_vec(),
        stopped: None,
        reapply: Some(
            entries
                .iter()
                .map(|e| {
                    let ok = !rejected.contains(&e.offset);
                    Applied {
                        offset: e.offset,
                        accepted: ok,
                        outcome: if ok {
                            "Accepted"
                        } else {
                            "Rejected(StaleEpoch)"
                        }
                        .into(),
                    }
                })
                .collect(),
        ),
    }
}

fn op(id: u64, kind: OpKind, run: &str, token: &str, detail: Option<&str>) -> ClientOp {
    ClientOp {
        op_id: id,
        client: 0,
        kind,
        node: 1,
        invoke_ms: id * 10,
        complete_ms: id * 10 + 5,
        outcome: Outcome::Ok,
        run_id: Some(run.to_owned()),
        idem_key: KEY.into(),
        consistency_token: Some(token.to_owned()),
        detail: detail.map(str::to_owned),
    }
}

fn expected_result() -> Value {
    json!({"id": KEY, "status": "completed", "results": ["k-1-0"], "go": {"request_id": RID}, "extra": null})
}

/// Leader 1 (epoch 1) starts, grants, journals seqs 1..2 and takes a signal; leader 2
/// (epoch 2) takes over: the old attempt's late append is rejected (stale), a new grant and
/// the append of seq 3 are accepted. HW 9, three identical replicas.
fn clean() -> World {
    let run = RunId::new_v7(PartitionId(0)).to_string();
    let r = run.clone();
    let entries = vec![
        entry(0, 1, Op::LeaderAnnounced { epoch: 1, node: 1 }),
        entry(
            1,
            1,
            Op::StartRun {
                run_id: r.clone(),
                workflow: WF.into(),
                idem_key: KEY.into(),
            },
        ),
        entry(
            2,
            1,
            Op::Grant {
                run_id: r.clone(),
                token: Some((1, 1)),
            },
        ),
        entry(
            3,
            1,
            Op::Events {
                run_id: r.clone(),
                token: Some((1, 1)),
                seqs: vec![1, 2],
            },
        ),
        entry(
            4,
            1,
            Op::Signal {
                run_id: r.clone(),
                name: "go".into(),
                request_id: RID.into(),
            },
        ),
        entry(5, 2, Op::LeaderAnnounced { epoch: 2, node: 2 }),
        entry(
            6,
            2,
            Op::Events {
                run_id: r.clone(),
                token: Some((1, 1)),
                seqs: vec![3],
            },
        ),
        entry(
            7,
            2,
            Op::Grant {
                run_id: r.clone(),
                token: Some((2, 1)),
            },
        ),
        entry(
            8,
            2,
            Op::Events {
                run_id: r.clone(),
                token: Some((2, 1)),
                seqs: vec![3],
            },
        ),
    ];
    let replicas = (1..=3).map(|n| replica(n, &entries, &[6])).collect();
    let journal = vec![
        JournalEvent::new(0, "run_started", json!({})),
        JournalEvent::new(1, "step_completed", json!({"name": "step0"})),
        JournalEvent::new(2, "signal_payload", json!({"payload": {"request_id": RID}})),
        JournalEvent::new(3, "run_completed", json!({})),
    ];
    let mut runs = BTreeMap::new();
    runs.insert(
        run.clone(),
        RunObservation::terminal("completed", Some(expected_result()), journal),
    );
    let mut expected = BTreeMap::new();
    expected.insert(KEY.to_owned(), expected_result());
    World {
        ops: vec![
            op(0, OpKind::Start, &run, "p0:1:2", None),
            op(
                1,
                OpKind::Signal,
                &run,
                "p0:1:5",
                Some(&format!("go/{RID}")),
            ),
        ],
        run,
        runs,
        expected,
        logs: LogExport {
            partitions: vec![PartitionLogs {
                partition: 0,
                hw: 9,
                replicas,
            }],
        },
    }
}

fn assert_ok(v: &Verdict) {
    assert!(v.ok(), "{} should pass: {:?}", v.checker, v.violations);
}

#[test]
fn clean_run_passes_every_checker_non_vacuously() {
    let w = clean();
    let (lost, dup, fen) = (w.lost(), w.duplicate(), w.fencing());
    assert_ok(&lost);
    assert_ok(&dup);
    assert_ok(&fen);
    assert_eq!(lost.count("runs_checked"), 1);
    assert_eq!(lost.count("completed"), 1);
    assert_eq!(lost.count("acked_signals"), 1);
    assert_eq!(dup.count("journal_entries"), 3 * 3);
    assert!(dup.count("records_compared") >= 27);
    assert_eq!(fen.count("tokens_checked"), 2);
    assert_eq!(
        fen.count("stale_rejected"),
        3,
        "the late append of attempt (1,1), per replica"
    );
    assert_eq!(fen.count("commands_reapplied"), 27);
    assert_eq!(fen.count("epochs"), 2);
}

#[test]
fn lost_ack_is_reported() {
    let mut w = clean();
    let run = w.run.clone();
    w.runs.remove(&run);
    let lost = w.lost();
    assert!(
        lost.mentions("does not exist after quiescence (lost)"),
        "{lost:?}"
    );
    assert_eq!(lost.violations.len(), 1, "{:?}", lost.violations);
    assert_ok(&w.fencing());

    // A run that never finished is lost too; an `info` start of another key is not.
    let mut w = clean();
    let run = w.run.clone();
    w.runs.get_mut(&run).expect("run").status = Some("running".into());
    let mut info = op(9, OpKind::Start, "", "", None);
    info.outcome = Outcome::Info;
    info.run_id = None;
    info.idem_key = "k-2".into();
    info.consistency_token = None;
    w.ops.push(info);
    let lost = w.lost();
    assert!(lost.mentions("not terminal"), "{lost:?}");
    assert_eq!(lost.violations.len(), 1, "{:?}", lost.violations);
    assert_eq!(lost.count("info_starts"), 1);
}

#[test]
fn wrong_result_and_two_runs_per_key_are_reported() {
    let mut w = clean();
    let run = w.run.clone();
    w.runs.get_mut(&run).expect("run").result = Some(json!({"id": KEY, "status": "compensated"}));
    assert!(w.lost().mentions("instead of"), "{:?}", w.lost());

    // The same key started a second accepted run in the log (an `info` retry that
    // created a new run instead of returning the existing one).
    let mut w = clean();
    let other = RunId::new_v7(PartitionId(0)).to_string();
    for r in w.replicas() {
        r.entries.push(entry(
            9,
            2,
            Op::StartRun {
                run_id: other.clone(),
                workflow: WF.into(),
                idem_key: KEY.into(),
            },
        ));
        if let Some(a) = r.reapply.as_mut() {
            a.push(Applied {
                offset: 9,
                accepted: true,
                outcome: "Accepted".into(),
            });
        }
    }
    let lost = w.lost();
    assert!(lost.mentions("started 2 runs in the logs"), "{lost:?}");
    assert_eq!(lost.violations.len(), 1, "{:?}", lost.violations);
}

#[test]
fn duplicate_seq_is_reported() {
    let mut w = clean();
    // Attempt (2,1) journals seq 3 a second time and apply (wrongly) accepts it.
    let r = w.run.clone();
    for rep in w.replicas() {
        let e = entry(
            9,
            2,
            Op::Events {
                run_id: r.clone(),
                token: Some((2, 1)),
                seqs: vec![3],
            },
        );
        rep.records.push(Record {
            base: 9,
            end: 10,
            epoch: 2,
            len: 16,
            digest: e.digest,
        });
        rep.entries.push(e);
        if let Some(a) = rep.reapply.as_mut() {
            a.push(Applied {
                offset: 9,
                accepted: true,
                outcome: "Accepted".into(),
            });
        }
    }
    let dup = w.duplicate();
    assert!(
        dup.mentions(&format!("run {r} seq 3 journaled twice (offsets 8 and 9)")),
        "{dup:?}"
    );
    assert_eq!(
        dup.violations.len(),
        3,
        "one per replica: {:?}",
        dup.violations
    );
    assert_ok(&w.fencing());
    assert_ok(&w.lost());

    // The same in the API journal: a second terminal event and a repeated seq.
    let mut w = clean();
    let r = w.run.clone();
    let obs = w.runs.get_mut(&r).expect("run");
    if let Some(j) = obs.journal.as_mut() {
        j.push(JournalEvent::new(3, "run_completed", json!({})));
    }
    let dup = w.duplicate();
    assert!(dup.mentions("seq 3 journaled twice"), "{dup:?}");
    assert!(dup.mentions("duplicate completion"), "{dup:?}");
}

#[test]
fn divergent_replica_entry_is_reported() {
    let mut w = clean();
    // Node 3 holds a different command at offset 4, below HW 9.
    let r = w.run.clone();
    let rep = &mut w.replicas()[2];
    let e = entry(
        4,
        1,
        Op::Signal {
            run_id: r,
            name: "go".into(),
            request_id: "other".into(),
        },
    );
    rep.records[4].digest = e.digest;
    rep.entries[4] = e;
    let dup = w.duplicate();
    assert!(
        dup.mentions("offset 4: record differs between node 1 and node 3"),
        "{dup:?}"
    );
    assert_eq!(dup.violations.len(), 1, "{:?}", dup.violations);
    let fen = w.fencing();
    assert!(fen.mentions("divergent replica entry"), "{fen:?}");
}

#[test]
fn two_leaders_in_one_epoch_are_reported() {
    let mut w = clean();
    // Node 3's log says node 3 announced epoch 2 (node 1 and 2 say node 2 did).
    let rep = &mut w.replicas()[2];
    let e = entry(5, 2, Op::LeaderAnnounced { epoch: 2, node: 3 });
    rep.records[5].digest = e.digest;
    rep.entries[5] = e;
    let fen = w.fencing();
    assert!(
        fen.mentions("epoch 2 announced by nodes {2, 3} (two leaders in one epoch)"),
        "{fen:?}"
    );
    // ... which also makes the replicas diverge at offset 5.
    assert!(fen.mentions("offset 5 (below HW 9)"), "{fen:?}");
    assert_eq!(fen.violations.len(), 2, "{:?}", fen.violations);

    // Epochs that do not increase along one log.
    let mut w = clean();
    for rep in w.replicas() {
        rep.entries[5].op = Op::LeaderAnnounced { epoch: 1, node: 2 };
    }
    let fen = w.fencing();
    assert!(fen.mentions("epochs not increasing"), "{fen:?}");
}

#[test]
fn acked_offset_overwritten_after_truncation_is_reported() {
    let mut w = clean();
    // Leader 2 truncated everything from offset 3 on and rewrote it in epoch 2; the signal
    // acknowledged with p0:1:5 (applied cursor 5 in epoch 1) is gone.
    let r = w.run.clone();
    let entries = vec![
        entry(0, 1, Op::LeaderAnnounced { epoch: 1, node: 1 }),
        entry(
            1,
            1,
            Op::StartRun {
                run_id: r.clone(),
                workflow: WF.into(),
                idem_key: KEY.into(),
            },
        ),
        entry(
            2,
            1,
            Op::Grant {
                run_id: r.clone(),
                token: Some((1, 1)),
            },
        ),
        entry(3, 2, Op::LeaderAnnounced { epoch: 2, node: 2 }),
        entry(
            4,
            2,
            Op::Grant {
                run_id: r.clone(),
                token: Some((2, 1)),
            },
        ),
        entry(
            5,
            2,
            Op::Events {
                run_id: r.clone(),
                token: Some((2, 1)),
                seqs: vec![1, 2, 3],
            },
        ),
    ];
    w.logs.partitions[0].hw = 6;
    w.logs.partitions[0].replicas = (1..=3).map(|n| replica(n, &entries, &[])).collect();
    let fen = w.fencing();
    assert!(
        fen.mentions(
            "node 1 holds no such command below offset 5; a record of epoch 2 > 1 sits at offset 3 (acked offset overwritten)"
        ),
        "{fen:?}"
    );
    assert_eq!(
        fen.violations.len(),
        3,
        "one per replica: {:?}",
        fen.violations
    );

    // A token beyond every replica's log is a lost acknowledged write.
    let mut w = clean();
    w.ops[1].consistency_token = Some("p0:1:40".into());
    let fen = w.fencing();
    assert!(
        fen.mentions("no replica's log reaches offset 40"),
        "{fen:?}"
    );
}

#[test]
fn stale_token_accepted_is_reported() {
    let mut w = clean();
    // The re-apply accepts the old attempt's append at offset 6 (token (1,1) in epoch 2).
    for rep in w.replicas() {
        if let Some(a) = rep.reapply.as_mut() {
            a[6].accepted = true;
            a[6].outcome = "Accepted".into();
        }
    }
    let fen = w.fencing();
    assert!(
        fen.mentions("offset 6: stale attempt token Some((1, 1))"),
        "{fen:?}"
    );
    assert_eq!(
        fen.violations.len(),
        3,
        "one per replica: {:?}",
        fen.violations
    );
    assert_eq!(fen.count("stale_rejected"), 0);

    // A command written under an old envelope epoch and accepted.
    let mut w = clean();
    let r = w.run.clone();
    for rep in w.replicas() {
        rep.entries[7] = entry(
            7,
            1,
            Op::Grant {
                run_id: r.clone(),
                token: Some((2, 1)),
            },
        );
    }
    let fen = w.fencing();
    assert!(
        fen.mentions("command of envelope epoch 1 under current epoch 2"),
        "{fen:?}"
    );
}

#[test]
fn tokens_parse() {
    assert_eq!(
        PartitionToken::parse("p3:7:120"),
        Some(PartitionToken {
            p: 3,
            epoch: 7,
            offset: 120
        })
    );
    assert_eq!(PartitionToken::parse("m:42"), None);
    assert_eq!(PartitionToken::parse("p3:7"), None);
    assert_eq!(PartitionToken::parse("p3:7:1:2"), None);
    assert_eq!(
        PartitionToken::parse("p3:7:120").map(|t| t.to_string()),
        Some("p3:7:120".into())
    );
}

/// The fencing model over the production apply: a real log (encoded commands) re-applied on
/// a fresh store by `reapply_scan`. A grant carrying epoch 1's token after epoch 2 began is
/// rejected `StaleEpoch` and counted; the rest is accepted; the checker passes.
#[test]
fn reapply_runs_the_production_apply() {
    use ostro_jepsen::checker::fencing::reapply_scan;
    use ostro_jepsen::checker::from_scan;
    use ostro_repl_conformance::oracle::disk::{LogScan, ScannedRecord};
    use ostro_types::command::{AttemptGranted, AttemptTokenMsg, StartRun, encode_command};
    use ostro_types::{Command, Epoch, Offset, PartitionCount, PayloadRef};

    let p = PartitionId(1);
    let run = RunId::new_v7(p);
    let grant = |epoch: u32, n: u32| {
        Command::attempt_granted(AttemptGranted {
            run_id: run.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch, n }),
            after_failure: false,
        })
    };
    let cmds: Vec<(u32, Command)> = vec![
        (1, Command::leader_announced(1, 1, 0)),
        (
            1,
            Command::start_run(StartRun {
                run_id: run.as_bytes().to_vec(),
                workflow: "wf".into(),
                workflow_version: 1,
                queue: "q".into(),
                input: Some(PayloadRef::inline(b"in".to_vec().into())),
                ready_at_ms: 100,
                requested_at_ms: 100,
                ..StartRun::default()
            }),
        ),
        (1, grant(1, 1)),
        (2, Command::leader_announced(2, 2, 0)),
        (2, grant(1, 2)),
        (2, grant(2, 1)),
    ];
    let records = cmds
        .iter()
        .enumerate()
        .map(|(i, (epoch, c))| {
            let bytes = encode_command(c);
            ScannedRecord {
                base: Offset(i as u64),
                end: Offset(i as u64 + 1),
                epoch: Epoch(*epoch),
                envelope: bytes.clone(),
                commands: vec![bytes],
            }
        })
        .collect();
    let scan = LogScan {
        node: 1,
        partition: p,
        records,
        stopped: None,
    };
    let applied = reapply_scan(&scan, PartitionCount::new(4).expect("count")).expect("re-apply");
    let outcomes: Vec<&str> = applied.iter().map(|a| a.outcome.as_str()).collect();
    assert_eq!(
        outcomes,
        [
            "Accepted",
            "Accepted",
            "Accepted",
            "Accepted",
            "Rejected(StaleEpoch)",
            "Accepted"
        ]
    );
    let mut rep = from_scan(&scan, true);
    rep.reapply = Some(applied);
    let logs = LogExport {
        partitions: vec![PartitionLogs {
            partition: 1,
            hw: 6,
            replicas: vec![rep],
        }],
    };
    let fen = check_fencing(&logs, &[]);
    assert_ok(&fen);
    assert_eq!(fen.count("stale_rejected"), 1);
    assert_eq!(fen.count("commands_reapplied"), 6);

    // A trimmed log cannot be re-applied on a fresh store.
    let mut trimmed = scan.clone();
    trimmed.records.remove(0);
    assert!(reapply_scan(&trimmed, PartitionCount::new(4).expect("count")).is_err());
}
