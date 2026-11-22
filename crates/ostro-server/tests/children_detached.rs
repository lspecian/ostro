//! Cancel-op propagation, detached completion and the journal-load repair table (plan 09-08,
//! WF-06; D-P9-10, D-P9-11, D-P9-24 W-b, D-P9-25 W1). Synthetic journals, a fake
//! `ModuleResolver` and a one-partition node.

mod children_fx;

use children_fx::*;
use ostro_engine::sink::ActivationSink;
use ostro_server::children::{CHILD_TERMINATED_KIND, UNKNOWN_WORKFLOW_KIND, child_run_id};
use ostro_server::derive::{self, ChildCtx, DeriveCtx};
use ostro_types::command::{CHILD_ROW_JOURNAL, Derived, StartRun, derived::Intent, outbox_msg};
use ostro_types::journal::{ChildStarted, EventBody};
use ostro_types::{PartitionId, RunId};

// ------------------------------------------------------------------ derive: cancel-op

fn cancel_runs(d: &[Derived]) -> Vec<(Vec<u8>, String)> {
    d.iter()
        .filter_map(|x| match &x.intent {
            Some(Intent::SendMessage(m)) => match m.msg.as_ref().and_then(|m| m.kind.as_ref()) {
                Some(outbox_msg::Kind::CancelRun(c)) => {
                    Some((c.target_run.clone(), c.reason.clone()))
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn derive_children(
    events: &[ostro_types::journal::Event],
    prior: &dyn Fn(u64) -> Option<EventBody>,
    starts: &dyn Fn(u64) -> Option<StartRun>,
    child_run: &dyn Fn(u64) -> Option<RunId>,
) -> Vec<Derived> {
    derive::derive_with_children(
        events,
        DeriveCtx {
            prior,
            result: None,
            successor: None,
        },
        ChildCtx {
            starts,
            parent_event: None,
            child_run,
        },
    )
    .expect("derive")
}

#[test]
fn op_cancelled_of_an_attached_child_derives_a_graceful_cancel_run() {
    let parent = RunId::new_v7(PartitionId(0));
    let child = child_run_id(&parent, 1, 4);
    let ids = |op: u64| Some(child_run_id(&parent, op, 4));
    let mut s = base_start(&child, None);
    s.parent = Some(ostro_types::command::ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op: 1,
    });
    let start = s.clone();
    // Same batch: StartChild + marker, then the CancelRun.
    let d = derive_children(
        &[child_started(1, "leaf", attached()), op_cancelled(2, 1)],
        &|_| None,
        &|op| (op == 1).then(|| start.clone()),
        &ids,
    );
    assert_eq!(d.len(), 3, "{d:?}");
    assert!(matches!(d[1].intent, Some(Intent::ChildJournal(_))));
    assert_eq!(
        cancel_runs(&d),
        vec![(child.as_bytes().to_vec(), "parent_cancel_op".to_string())]
    );
    assert_eq!(cancel_runs(&d[2..]).len(), 1, "after the StartChild");
    // Started in an earlier batch (prior lookup).
    let prior_child = |op: u64| {
        (op == 1).then(|| {
            EventBody::ChildStarted(ChildStarted {
                meta: meta(),
                workflow: "leaf".into(),
                input: payload(b"{}"),
                options: attached(),
            })
        })
    };
    let d = derive_children(&[op_cancelled(5, 1)], &prior_child, &|_| None, &ids);
    assert_eq!(
        cancel_runs(&d),
        vec![(child.as_bytes().to_vec(), "parent_cancel_op".to_string())]
    );
    // Detached: nothing.
    let prior_det = |op: u64| {
        (op == 1).then(|| {
            EventBody::ChildStarted(ChildStarted {
                meta: meta(),
                workflow: "leaf".into(),
                input: payload(b"{}"),
                options: detached(),
            })
        })
    };
    let d = derive_children(&[op_cancelled(5, 1)], &prior_det, &|_| None, &ids);
    assert!(d.is_empty(), "{d:?}");
}

#[tokio::test]
async fn sink_cancel_op_reaches_the_child() {
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    let sink = fx.sink(run, token, Fake::leaf());
    assert_eq!(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", attached())]
        )
        .await,
        Ok(2)
    );
    let child = child_run_id(&run, 1, 1);
    fx.wait_record(&child).await;
    assert_eq!(
        sink.append(&rref(&run), token.n, 2, vec![op_cancelled(2, 1)])
            .await,
        Ok(3)
    );
    fx.wait("child cancel flag", |f| {
        f.record(&child).is_some_and(|r| r.cancel_requested)
    })
    .await;
    assert_eq!(
        fx.record(&child).expect("child").cancel_reason,
        "parent_cancel_op"
    );
}

// ------------------------------------------------------------------ detached

#[tokio::test]
async fn detached_flush_completes_with_the_run_id_on_the_hot_wait_channel() {
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = fx.sink_with(run, token, Fake::leaf(), Some(tx));
    assert_eq!(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", detached())]
        )
        .await,
        Ok(2)
    );
    let child = child_run_id(&run, 1, 1);
    let j = fx.journal(&run).await;
    assert_eq!(j.len(), 3, "the {{run_id}} completion follows the flush");
    assert_eq!(completion_of(&j, 1), Some(Ok(run_id_payload(&child))));
    let hot = rx.try_recv().expect("hot-wait input");
    assert_eq!(hot.seq, 2);
    assert!(matches!(&hot.body, EventBody::ChildCompleted(c) if c.op == 1));
    assert!(fx.row(&run, &child).is_none(), "no children row");
    assert_eq!(fx.record(&run).expect("parent").children, 0);
    let rec = fx.wait_record(&child).await;
    assert_eq!(rec.parent, None);
    // Repair finds the completion: nothing more.
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    assert_eq!(fx.journal(&run).await.len(), 3);
}

// ------------------------------------------------------------------ repair table (W1)

#[tokio::test]
async fn repair_attached_with_row_does_nothing() {
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    let sink = fx.sink(run, token, Fake::leaf());
    sink.append(
        &rref(&run),
        token.n,
        1,
        vec![child_started(1, "leaf", attached())],
    )
    .await
    .expect("append");
    let child = child_run_id(&run, 1, 1);
    assert_eq!(fx.row(&run, &child).as_deref(), Some(CHILD_ROW_JOURNAL));
    // Even with the target gone from meta: a delivered start is never re-decided.
    let sink2 = fx.sink(run, token, Fake::default());
    sink2
        .repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    assert_eq!(fx.journal(&run).await.len(), 2);
    assert_eq!(fx.record(&run).expect("parent").next_seq, 2);
}

#[tokio::test]
async fn repair_attached_without_row_resends_or_journals_unknown_once() {
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    fx.raw(
        &run,
        token,
        1,
        &[
            child_started(1, "leaf", attached()),
            child_started(2, "nope", attached()),
        ],
    )
    .await;
    let sink = fx.sink(run, token, Fake::leaf());
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    let c1 = child_run_id(&run, 1, 1);
    assert_eq!(
        fx.row(&run, &c1).as_deref(),
        Some(CHILD_ROW_JOURNAL),
        "re-sent + marked"
    );
    fx.wait_record(&c1).await;
    let j = fx.journal(&run).await;
    assert_eq!(j.len(), 4);
    assert_kind(completion_of(&j, 2), UNKNOWN_WORKFLOW_KIND);
    assert_eq!(completion_of(&j, 1), None);
    sink.repair_unsupported(&j).await.expect("repair");
    assert_eq!(fx.journal(&run).await.len(), 4, "once");
    assert_eq!(fx.runs(), 2);
}

#[tokio::test]
async fn repair_detached_resends_and_journals_the_run_id_or_unknown() {
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    fx.raw(
        &run,
        token,
        1,
        &[
            child_started(1, "leaf", detached()),
            child_started(2, "nope", detached()),
        ],
    )
    .await;
    let sink = fx.sink(run, token, Fake::leaf());
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    let c1 = child_run_id(&run, 1, 1);
    let rec = fx.wait_record(&c1).await;
    assert_eq!(rec.parent, None);
    assert!(fx.row(&run, &c1).is_none());
    let j = fx.journal(&run).await;
    assert_eq!(j.len(), 5);
    assert_eq!(completion_of(&j, 1), Some(Ok(run_id_payload(&c1))));
    assert_kind(completion_of(&j, 2), UNKNOWN_WORKFLOW_KIND);
    sink.repair_unsupported(&j).await.expect("repair");
    assert_eq!(fx.journal(&run).await.len(), 5, "once");
    assert_eq!(fx.runs(), 2, "parent + one detached child");
}

#[tokio::test]
async fn repair_skips_a_cancelled_child_op() {
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    fx.raw(
        &run,
        token,
        1,
        &[child_started(1, "leaf", attached()), op_cancelled(2, 1)],
    )
    .await;
    let sink = fx.sink(run, token, Fake::leaf());
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    assert_eq!(fx.runs(), 1, "no re-send for a cancelled op");
    assert_eq!(fx.journal(&run).await.len(), 3);
}

/// A resumed parent (retention `retention_ms`) with an attached child op 1 whose row is gone,
/// as a synthetic journal: `[RunStarted, ChildStarted@1, RunTerminated@closed_ms]` (the
/// journal of a hard-stopped, RetryRun-resumed run; the record stays live, so no expiry timer
/// interferes). Returns (run, token).
async fn resumed_parent(
    fx: &Fx,
    retention_ms: u64,
    closed_ms: u64,
) -> (RunId, ostro_types::AttemptToken) {
    let mut s = base_start(&RunId::new_v7(PartitionId(0)), None);
    s.retention_ms = retention_ms;
    let (run, token) = fx.start(s).await;
    let terminated = ostro_types::journal::Event {
        seq: 2,
        ts_nanos: closed_ms * 1_000_000,
        body: EventBody::RunTerminated(ostro_types::journal::RunTerminated {
            reason: "ops".into(),
        }),
    };
    fx.raw(
        &run,
        token,
        1,
        &[child_started(1, "leaf", attached()), terminated],
    )
    .await;
    (run, token)
}

#[tokio::test]
async fn repair_after_the_retention_window_completes_child_terminated() {
    let fx = Fx::new(1).await;
    // Closed in 2001 with a 1 s retention: long expired.
    let (run, t2) = resumed_parent(&fx, 1_000, 978_307_200_000).await;
    let sink = fx.sink(run, t2, Fake::leaf());
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    let j = fx.journal(&run).await;
    assert_kind(completion_of(&j, 1), CHILD_TERMINATED_KIND);
    assert_eq!(fx.runs(), 1, "no StartChild re-sent");
    sink.repair_unsupported(&j).await.expect("repair");
    assert_eq!(completions(&fx.journal(&run).await, 1), 1);
}

#[tokio::test]
async fn repair_within_the_retention_window_resends() {
    let fx = Fx::new(1).await;
    let (run, t2) = resumed_parent(&fx, FAR_MS, 978_307_200_000).await;
    let sink = fx.sink(run, t2, Fake::leaf());
    sink.repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    let c1 = child_run_id(&run, 1, 1);
    fx.wait_record(&c1).await;
    assert_eq!(fx.row(&run, &c1).as_deref(), Some(CHILD_ROW_JOURNAL));
    assert_eq!(completion_of(&fx.journal(&run).await, 1), None);
}
