//! Checker `fencing` (14-09, D-P14-19): leader epochs fence every write.
//!
//! Per partition, over every replica's log read offline:
//!
//! 1. **Epoch order.** The `LeaderAnnounced` commands of each log carry strictly increasing
//!    epochs.
//! 2. **One leader per epoch.** Across all replicas, each epoch is announced by one node.
//! 3. **No divergence below HW.** No two replicas hold different commands at one offset
//!    below the settled HW.
//! 4. **Acknowledged writes survive.** For every acknowledged partition write (start,
//!    signal, cancel) with a consistency token `p<P>:<epoch>:<offset>`: some replica's log
//!    reaches `offset`; on every replica that does, no record below `offset` was written in
//!    an epoch above the token's (nothing below an acknowledged applied cursor was truncated
//!    and overwritten by a later leader), and the acknowledged command (the run's `StartRun`,
//!    the signal with its request id, the cancel) is there below `offset`. The token's offset
//!    is the answering leader's applied cursor after the write (13-13), an upper bound of the
//!    write's own offset, so the command is matched by run id / request id below it rather
//!    than at one exact offset.
//! 5. **Stale commands rejected.** Each log is re-applied offline on a fresh store
//!    ([`reapply_scan`], the production `apply_command`); an independent model of the fencing
//!    rule replays the accepted commands and reports every accepted command it would refuse:
//!    a `LeaderAnnounced` that does not raise the epoch (or whose envelope epoch differs), a
//!    command whose envelope epoch is not the current one, an `AttemptGranted` whose token is
//!    not of the current epoch or not above the run's current attempt, an `EventsAppended` or
//!    fenced `PauseRun` whose token is not the run's current attempt. Rejected stale commands
//!    are counted (`stale_rejected`).

use std::collections::{BTreeMap, BTreeSet};

use ostro_partition::store::PartitionStore;
use ostro_partition::{ApplyOutcome, Cursor, StoreConfig, apply_command};
use ostro_repl_conformance::oracle::disk::LogScan;
use ostro_types::{Offset, PartitionCount, RunId};

use super::lost::signal_request_id;
use super::{Applied, AttemptTok, LogExport, Op, PartitionLogs, ReplicaLog, Verdict};
use crate::history::{ClientOp, OpKind, Outcome, PartitionToken};

/// The `fencing` verdict over the final logs and the acknowledged writes in `ops`.
pub fn check_fencing(logs: &LogExport, ops: &[ClientOp]) -> Verdict {
    let mut v = Verdict::new("fencing");
    for p in &logs.partitions {
        let mut leaders: BTreeMap<u32, BTreeSet<u64>> = BTreeMap::new();
        for r in &p.replicas {
            epoch_order(p.partition, r, &mut leaders, &mut v);
            match &r.reapply {
                Some(_) => fencing_model(p.partition, r, &mut v),
                None => v.note(format!(
                    "partition {} node {}: log not re-applied (trimmed or re-apply error)",
                    p.partition, r.node
                )),
            }
        }
        for (epoch, nodes) in &leaders {
            v.add("epochs", 1);
            if nodes.len() > 1 {
                v.violation(format!(
                    "partition {}: epoch {epoch} announced by nodes {nodes:?} (two leaders in one epoch)",
                    p.partition
                ));
            }
        }
        same_offset(p, &mut v);
    }
    acked_tokens(logs, ops, &mut v);
    v
}

fn epoch_order(
    p: u16,
    r: &ReplicaLog,
    leaders: &mut BTreeMap<u32, BTreeSet<u64>>,
    v: &mut Verdict,
) {
    let mut last: Option<(u32, u64)> = None;
    for e in &r.entries {
        let Op::LeaderAnnounced { epoch, node } = e.op else {
            continue;
        };
        v.add("leader_announcements", 1);
        leaders.entry(epoch).or_default().insert(node);
        if let Some((le, lo)) = last
            && epoch <= le
        {
            v.violation(format!(
                "partition {p} node {}: LeaderAnnounced epoch {epoch} at offset {} after epoch {le} at offset {lo} (epochs not increasing)",
                r.node, e.offset
            ));
        }
        last = Some((epoch, e.offset));
    }
}

/// Item 5 of the module docs over one re-applied replica log.
fn fencing_model(p: u16, r: &ReplicaLog, v: &mut Verdict) {
    let mut epoch: u32 = 0;
    let mut current: BTreeMap<&str, AttemptTok> = BTreeMap::new();
    for e in &r.entries {
        let Some(a) = r.applied_at(e.offset) else {
            v.violation(format!(
                "partition {p} node {}: the re-apply has no outcome for offset {}",
                r.node, e.offset
            ));
            continue;
        };
        v.add("commands_reapplied", 1);
        let at = |what: String| {
            format!(
                "partition {p} node {} offset {}: {what} accepted ({})",
                r.node, e.offset, a.outcome
            )
        };
        if let Op::LeaderAnnounced { epoch: le, .. } = e.op {
            let legal = le > epoch && e.epoch == le;
            if a.accepted {
                if !legal {
                    v.violation(at(format!(
                        "stale LeaderAnnounced epoch {le} (envelope {}, current {epoch})",
                        e.epoch
                    )));
                }
                epoch = epoch.max(le);
            } else if !legal {
                v.add("stale_rejected", 1);
            }
            continue;
        }
        let env_ok = e.epoch == epoch;
        let tok_ok = match &e.op {
            Op::Grant { run_id, token } => token.is_some_and(|t| {
                t.0 == epoch && current.get(run_id.as_str()).is_none_or(|c| t > *c)
            }),
            Op::Events { run_id, token, .. } => {
                token.is_some_and(|t| t.0 == epoch && current.get(run_id.as_str()) == Some(&t))
            }
            Op::Pause {
                run_id,
                token: Some(t),
            } => t.0 == epoch && current.get(run_id.as_str()) == Some(t),
            _ => true,
        };
        if a.accepted {
            if !env_ok {
                v.violation(at(format!(
                    "command of envelope epoch {} under current epoch {epoch}",
                    e.epoch
                )));
            } else if !tok_ok {
                let (run, tok) = match &e.op {
                    Op::Grant { run_id, token }
                    | Op::Events { run_id, token, .. }
                    | Op::Pause { run_id, token } => (run_id.as_str(), *token),
                    _ => ("", None),
                };
                v.violation(at(format!(
                    "stale attempt token {tok:?} of run {run} (epoch {epoch}, current attempt {:?})",
                    current.get(run)
                )));
            }
            if let Op::Grant {
                run_id,
                token: Some(t),
            } = &e.op
            {
                current.insert(run_id.as_str(), *t);
            }
        } else if !env_ok || !tok_ok {
            v.add("stale_rejected", 1);
        }
    }
}

/// Item 3: commands below HW agree across replicas.
fn same_offset(p: &PartitionLogs, v: &mut Verdict) {
    let mut at: BTreeMap<u64, (u64, u32, u64)> = BTreeMap::new();
    for r in &p.replicas {
        for e in r.entries.iter().filter(|e| e.offset < p.hw) {
            match at.get(&e.offset) {
                None => {
                    at.insert(e.offset, (r.node, e.epoch, e.digest));
                }
                Some(&(n0, ep0, d0)) => {
                    v.add("entries_compared", 1);
                    if ep0 != e.epoch || d0 != e.digest {
                        v.violation(format!(
                            "partition {} offset {} (below HW {}): node {n0} holds epoch {ep0} digest {d0:016x}, node {} epoch {} digest {:016x} (divergent replica entry)",
                            p.partition, e.offset, p.hw, r.node, e.epoch, e.digest
                        ));
                    }
                }
            }
        }
    }
}

/// Whether `e` is the command acknowledged by `op`.
fn carries(op: &ClientOp, e: &Op) -> bool {
    let Some(run) = op.run_id.as_deref() else {
        return false;
    };
    match (op.kind, e) {
        (OpKind::Start, Op::StartRun { run_id, .. }) => run_id == run,
        (
            OpKind::Signal,
            Op::Signal {
                run_id, request_id, ..
            },
        ) => run_id == run && signal_request_id(op).as_deref() == Some(request_id.as_str()),
        (OpKind::Cancel, Op::Cancel { run_id }) => run_id == run,
        _ => false,
    }
}

/// Item 4 of the module docs.
fn acked_tokens(logs: &LogExport, ops: &[ClientOp], v: &mut Verdict) {
    for op in ops {
        if op.outcome != Outcome::Ok || op.kind == OpKind::Result {
            continue;
        }
        let Some(tok) = op
            .consistency_token
            .as_deref()
            .and_then(PartitionToken::parse)
        else {
            v.add("acked_writes_without_token", 1);
            continue;
        };
        v.add("tokens_checked", 1);
        let what = format!(
            "acknowledged {:?} op {} of run {} (token {tok})",
            op.kind,
            op.op_id,
            op.run_id.as_deref().unwrap_or("?")
        );
        if let Some(run) = op.run_id.as_deref()
            && let Ok(id) = run.parse::<RunId>()
            && id.partition().0 != tok.p
        {
            v.violation(format!(
                "{what}: the token names partition {}, the run lives on {}",
                tok.p,
                id.partition().0
            ));
            continue;
        }
        let Some(p) = logs.partition(tok.p) else {
            v.violation(format!(
                "{what}: no log of partition {} was exported",
                tok.p
            ));
            continue;
        };
        let covering: Vec<&ReplicaLog> = p
            .replicas
            .iter()
            .filter(|r| r.end() >= tok.offset)
            .collect();
        if covering.is_empty() {
            v.violation(format!(
                "{what}: no replica's log reaches offset {} (ends {:?}): acknowledged write lost",
                tok.offset,
                p.replicas
                    .iter()
                    .map(|r| (r.node, r.end()))
                    .collect::<Vec<_>>()
            ));
            continue;
        }
        for r in covering {
            if let Some(rec) = r
                .records
                .iter()
                .find(|x| x.base < tok.offset && x.epoch > tok.epoch)
            {
                v.violation(format!(
                    "{what}: node {} holds a record at offset {} of epoch {} > {} below the acknowledged offset (acked offset overwritten)",
                    r.node, rec.base, rec.epoch, tok.epoch
                ));
                continue;
            }
            let found = r.entries.iter().any(|e| {
                e.offset < tok.offset
                    && carries(op, &e.op)
                    && (op.kind != OpKind::Start
                        || r.reapply.is_none()
                        || r.applied_at(e.offset).is_some_and(|a| a.accepted))
            });
            if !found {
                if r.start().unwrap_or(0) > 0 {
                    v.note(format!(
                        "{what}: node {} log starts at {:?} (trimmed); command not checked",
                        r.node,
                        r.start()
                    ));
                } else {
                    v.violation(format!(
                        "{what}: node {} holds no such command below offset {}",
                        r.node, tok.offset
                    ));
                }
            }
        }
    }
}

/// `Accepted` / `Duplicate` / `Rejected(<reason>)` (the reason's variant name).
fn describe(o: &ApplyOutcome) -> String {
    match o {
        ApplyOutcome::Accepted => "Accepted".into(),
        ApplyOutcome::Duplicate { .. } => "Duplicate".into(),
        ApplyOutcome::Rejected(r) => {
            let d = format!("{r:?}");
            let name: String = d
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            format!("Rejected({name})")
        }
    }
}

/// Re-apply `scan` (a log starting at offset 0) on a fresh store with the production apply
/// core, one store batch per record, and return every command's outcome. `Err` for a log
/// that does not start at 0 (trimmed: a fresh store cannot replay it) or a store failure.
pub fn reapply_scan(scan: &LogScan, count: PartitionCount) -> Result<Vec<Applied>, String> {
    if let Some(start) = scan.log_start()
        && start.0 != 0
    {
        return Err(format!("log starts at {} (trimmed)", start.0));
    }
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let (store, meta) =
        PartitionStore::open(dir.path(), scan.partition, count, &StoreConfig::default())
            .map_err(|e| format!("open fresh store: {e}"))?;
    let mut cur = Cursor::from_meta(&meta);
    if cur.next != Offset(0) {
        return Err(format!("fresh store cursor at {}", cur.next.0));
    }
    let mut out = Vec::new();
    for rec in &scan.records {
        let mut b = store.batch();
        let mut actions = Vec::new();
        for (i, c) in rec.commands.iter().enumerate() {
            let off = Offset(rec.base.0 + i as u64);
            let o = apply_command(&mut b, &mut cur, off, rec.epoch, c, &mut actions)
                .map_err(|e| format!("apply at offset {}: {e}", off.0))?;
            out.push(Applied {
                offset: off.0,
                accepted: o == ApplyOutcome::Accepted,
                outcome: describe(&o),
            });
        }
        let mu = cur.meta_update();
        b.commit(&mu)
            .map_err(|e| format!("commit at offset {}: {e}", rec.end.0))?;
    }
    Ok(out)
}
