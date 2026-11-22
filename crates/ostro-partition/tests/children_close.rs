//! Parent-close cascade and detached children rows (plan 09-08, PF-F3, D-P9-10, D-P9-11).
//!
//! On every terminal transition of a parent with attached children, apply sends a graceful
//! `CancelRun` to each child still in the `children` keyspace and removes those rows
//! (`children = 0`), so a resumed parent's journal-load repair re-sends the StartChild
//! (D-P9-24). Executor and continue-as-new terminals use the reason `parent_closed`; the
//! hard stops (Terminated, TimedOut) keep 05-09's cascade, which carries the hard-stop
//! reason. A detached child gets no row at all.

mod children_common;

use children_common::*;
use ostro_partition::Table;
use ostro_partition::apply::{ApplyOutcome, InvalidCode, RejectReason};
use ostro_partition::store::keys::children_key;
use ostro_partition::store::values::RunStatus;
use ostro_types::RunId;
use ostro_types::command::{Derived, RUN_TIMEOUT_OP, TerminalStatus, outbox_msg};

#[derive(Clone, Copy, Debug)]
enum Close {
    Completed,
    Failed,
    Cancelled,
    ContinuedAsNew,
    Terminated,
    TimedOut,
}

const ALL: [Close; 6] = [
    Close::Completed,
    Close::Failed,
    Close::Cancelled,
    Close::ContinuedAsNew,
    Close::Terminated,
    Close::TimedOut,
];

/// Closes the running parent `p` (journal `0..3`) and returns (outbox delta, reads).
fn close(h: &mut H, p: &RunId, how: Close) -> (Vec<outbox_msg::Kind>, u64) {
    let before = h.outbox();
    let cmd = match how {
        Close::Completed => append(p, 3, 1, vec![complete(TerminalStatus::Completed)]),
        Close::Failed => append(p, 3, 1, vec![complete(TerminalStatus::Failed)]),
        Close::Cancelled => append(p, 3, 1, vec![complete(TerminalStatus::Cancelled)]),
        Close::ContinuedAsNew => append(p, 3, 1, can_tail(&rid(90))),
        Close::Terminated => terminate(p, "stop", 500),
        Close::TimedOut => fire(p, RUN_TIMEOUT_OP, 100 + 5_000),
    };
    let (out, _, reads) = h.apply_full(&cmd);
    assert_eq!(out, ApplyOutcome::Accepted, "{how:?}");
    (h.outbox_since(&before), reads)
}

fn expected_reason(how: Close) -> &'static str {
    match how {
        Close::Terminated => "stop",
        Close::TimedOut => "run_timeout",
        _ => "parent_closed",
    }
}

fn expected_status(how: Close) -> RunStatus {
    match how {
        Close::Completed => RunStatus::Completed,
        Close::Failed => RunStatus::Failed,
        Close::Cancelled => RunStatus::Cancelled,
        Close::ContinuedAsNew => RunStatus::ContinuedAsNew,
        Close::Terminated => RunStatus::Terminated,
        Close::TimedOut => RunStatus::TimedOut,
    }
}

fn parent_start() -> ostro_types::command::StartRun {
    let mut s = start(&rid(1));
    s.run_timeout_ms = 5_000;
    s
}

#[test]
fn every_parent_terminal_cancels_and_removes_attached_children() {
    for how in ALL {
        let mut h = H::new();
        let kids = parent_with_children(&mut h, parent_start(), &[1, 2]);
        let p = rid(1);
        let (out, _) = close(&mut h, &p, how);
        let mut got = cancels(&out);
        got.sort();
        let mut want: Vec<_> = kids
            .iter()
            .map(|k| (*k, expected_reason(how).to_string()))
            .collect();
        want.sort();
        assert_eq!(got, want, "{how:?}: {out:?}");
        let rec = h.run(&p);
        assert_eq!(rec.status, expected_status(how), "{how:?}");
        assert_eq!(rec.children, 0, "{how:?}");
        for k in &kids {
            assert!(
                h.get(Table::Children, &children_key(&p, k)).is_none(),
                "{how:?}: row removed"
            );
        }
    }
}

#[test]
fn a_parent_without_children_scans_nothing() {
    for how in ALL {
        let mut h = H::new();
        let mut with = H::new();
        parent_with_children(&mut h, parent_start(), &[]);
        parent_with_children(&mut with, parent_start(), &[1]);
        let p = rid(1);
        let (out, plain) = close(&mut h, &p, how);
        assert!(cancels(&out).is_empty(), "{how:?}");
        let (_, scanned) = close(&mut with, &p, how);
        assert_eq!(
            scanned,
            plain + 1,
            "{how:?}: one children scan only with children"
        );
    }
}

#[test]
fn a_late_parent_closed_cancel_is_dropped_by_an_ended_child() {
    // A child that already ended drops the parent-close CancelRun (per-message drop).
    let mut h = H::new();
    let kids = parent_with_children(&mut h, parent_start(), &[1]);
    let c = kids[0];
    // The child exists on this partition and has completed.
    let p = rid(1);
    running_with(&mut h, child_start(&p, 1, &c, false));
    h.ok(&append(&c, 0, 1, vec![complete(TerminalStatus::Completed)]));
    let (out, _) = close(&mut h, &p, Close::Completed);
    let before = h.outbox();
    for k in out {
        let (o, _, _) = h.deliver(k);
        assert_eq!(o, ApplyOutcome::Accepted);
    }
    assert!(h.outbox_since(&before).is_empty());
    let rec = h.run(&c);
    assert_eq!(rec.status, RunStatus::Completed);
    assert!(!rec.cancel_requested);
}

#[test]
fn detached_start_writes_no_row_and_attached_does() {
    let mut h = H::new();
    let p = running_with(&mut h, start(&rid(1)));
    let det = child_id(&p, 1);
    let att = child_id(&p, 2);
    h.ok(&append(
        &p,
        0,
        3,
        vec![
            send(start_child_kind(child_start(&p, 1, &det, true))),
            send(start_child_kind(child_start(&p, 2, &att, false))),
            Derived::child_journal(&att),
        ],
    ));
    let rec = h.run(&p);
    assert_eq!(rec.children, 1);
    assert!(h.get(Table::Children, &children_key(&p, &det)).is_none());
    assert!(h.get(Table::Children, &children_key(&p, &att)).is_some());
    // The parent's close leaves the detached child alone.
    let (out, _) = close(&mut h, &p, Close::Completed);
    assert_eq!(cancels(&out), vec![(att, "parent_closed".to_string())]);
}

#[test]
fn detached_start_in_a_continue_as_new_batch_writes_no_row() {
    let mut h = H::new();
    let p = running_with(&mut h, start(&rid(1)));
    let det = child_id(&p, 0);
    let mut d = vec![send(start_child_kind(child_start(&p, 0, &det, true)))];
    d.extend(can_tail(&rid(90)));
    let before = h.outbox();
    h.ok(&append(&p, 0, 1, d));
    assert_eq!(h.run(&p).children, 0);
    assert!(h.get(Table::Children, &children_key(&p, &det)).is_none());
    assert!(cancels(&h.outbox_since(&before)).is_empty());
}

#[test]
fn child_journal_marker_after_a_detached_start_is_rejected() {
    let mut h = H::new();
    let p = running_with(&mut h, start(&rid(1)));
    let det = child_id(&p, 0);
    let out = h.apply(&append(
        &p,
        0,
        1,
        vec![
            send(start_child_kind(child_start(&p, 0, &det, true))),
            Derived::child_journal(&det),
        ],
    ));
    assert_eq!(
        out,
        ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadIntent))
    );
}

#[test]
fn hard_stop_still_reports_to_its_own_parent_before_the_cascade() {
    // 05-09 order kept: ChildCompleted to the grandparent first, then the CancelRuns.
    let mut h = H::new();
    let gp = rid(9);
    let mut s = parent_start();
    s.parent = Some(ostro_types::command::ParentRef {
        run_id: gp.as_bytes().to_vec(),
        op: 4,
    });
    let kids = parent_with_children(&mut h, s, &[1]);
    let (out, _) = close(&mut h, &rid(1), Close::Terminated);
    assert_eq!(out.len(), 2, "{out:?}");
    assert!(matches!(out[0], outbox_msg::Kind::ChildCompleted(_)));
    assert_eq!(cancels(&out), vec![(kids[0], "stop".to_string())]);
}
