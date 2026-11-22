//! C-CHILD-10 at the server level (plan 09-08; D-P9-24 B3, D-P9-11 PF-F3): resume after
//! close never hangs a parent.
//!
//! A parent (partition 0) with an in-flight attached child (partition 1) is terminated: the
//! child gets `CancelRun{parent_closed}` and the parent's children rows are removed. After a
//! RetryRun resume, the first activation's journal-load repair re-sends the StartChild:
//! (i) child already Cancelled → event-less re-completion → parent journals `Err(Cancelled)`;
//! (ii) child Completed while the parent was terminal → `Err(non-retryable child_terminated)`;
//! (iii) child still running → the duplicate is a drop, the row is back and the child's own
//! completion is accepted. In every case the parent completes and no op completes twice.

mod children_fx;

use children_fx::*;
use ostro_engine::sink::ActivationSink;
use ostro_partition::ApplyOutcome;
use ostro_server::children::{CHILD_TERMINATED_KIND, child_run_id};
use ostro_types::command::{CHILD_ROW_JOURNAL, RetryRun, TerminateRun};
use ostro_types::journal::{EventBody, Failure, OpOutput, RunCompleted, RunFailed};
use ostro_types::{AttemptToken, Command, PartitionId, RunId};

#[derive(Clone, Copy, Debug)]
enum Case {
    AlreadyCancelled,
    CompletedWhileParentTerminal,
    StillRunning,
}

/// The first op >= 1 whose child lives on partition 1 (of 2).
fn remote_op(run: &RunId) -> u64 {
    (1..64)
        .find(|op| child_run_id(run, *op, 2).partition() == PartitionId(1))
        .expect("an op routed to partition 1")
}

/// The child's terminal event at its journal end (after the journaled CancelRequested).
async fn child_ends(fx: &Fx, child: RunId, ct: AttemptToken, body: EventBody) {
    let end = fx.record(&child).expect("child").next_seq;
    let sink = fx.sink(child, ct, Fake::leaf());
    assert_eq!(
        sink.append(&rref(&child), ct.n, end, vec![ev(end, body)])
            .await,
        Ok(end + 1)
    );
    assert!(sink.is_terminal());
}

async fn scenario(case: Case) {
    let fx = Fx::new(2).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    let op = remote_op(&run);
    // Fill the journal up to the child op, then the flush that starts the child.
    if op > 1 {
        let fill: Vec<_> = (1..op).map(|s| ev(s, step_body())).collect();
        fx.raw(&run, token, 1, &fill).await;
    }
    let sink = fx.sink(run, token, Fake::leaf());
    assert_eq!(
        sink.append(
            &rref(&run),
            token.n,
            op,
            vec![child_started(op, "leaf", attached())]
        )
        .await,
        Ok(op + 1)
    );
    let child = child_run_id(&run, op, 2);
    assert_eq!(child.partition(), PartitionId(1));
    fx.wait_record(&child).await;
    let ct = fx.grant(&child).await;
    fx.raw(&child, ct, 0, &[ev(0, step_body())]).await;

    // The parent is terminated: graceful parent_closed cancel, rows removed.
    let o = fx
        .submit(
            0,
            Command::terminate_run(TerminateRun {
                run_id: run.as_bytes().to_vec(),
                reason: "ops".into(),
                terminated_at_ms: FAR_MS,
            }),
        )
        .await;
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(fx.row(&run, &child).is_none(), "row removed at close");
    assert_eq!(fx.record(&run).expect("parent").children, 0);
    fx.wait("child cancel flag", |f| {
        f.record(&child).is_some_and(|r| r.cancel_requested)
    })
    .await;
    // The hard-stop cascade carries the hard-stop reason (05-09).
    assert_eq!(fx.record(&child).expect("child").cancel_reason, "ops");

    // The child acts while the parent is terminal.
    match case {
        Case::AlreadyCancelled => {
            child_ends(
                &fx,
                child,
                ct,
                EventBody::RunFailed(RunFailed {
                    meta: meta(),
                    failure: Failure::Cancelled,
                }),
            )
            .await;
        }
        Case::CompletedWhileParentTerminal => {
            child_ends(
                &fx,
                child,
                ct,
                EventBody::RunCompleted(RunCompleted {
                    meta: meta(),
                    output: payload(b"{\"late\":1}"),
                }),
            )
            .await;
        }
        Case::StillRunning => {}
    }
    fx.drain().await;
    let before = fx.journal(&run).await;
    assert_eq!(
        completions(&before, op),
        0,
        "{case:?}: dropped at a terminal parent"
    );

    // RetryRun resume; the first activation's repair re-sends the StartChild.
    let o = fx
        .submit(
            0,
            Command::retry_run(RetryRun {
                run_id: run.as_bytes().to_vec(),
                requested_at_ms: FAR_MS + 1,
            }),
        )
        .await;
    assert_eq!(o, ApplyOutcome::Accepted);
    let t2 = fx.grant(&run).await;
    let sink2 = fx.sink(run, t2, Fake::leaf());
    sink2
        .repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    assert_eq!(fx.runs(), 2, "{case:?}: still exactly one child");

    let j = match case {
        Case::AlreadyCancelled => {
            let j = fx.wait_completion(&run, op).await;
            assert_eq!(completion_of(&j, op), Some(Err(Failure::Cancelled)));
            j
        }
        Case::CompletedWhileParentTerminal => {
            let j = fx.wait_completion(&run, op).await;
            assert_kind(completion_of(&j, op), CHILD_TERMINATED_KIND);
            j
        }
        Case::StillRunning => {
            fx.drain().await;
            assert_eq!(
                fx.row(&run, &child).as_deref(),
                Some(CHILD_ROW_JOURNAL),
                "row re-inserted"
            );
            assert_eq!(
                completions(&fx.journal(&run).await, op),
                0,
                "duplicate dropped"
            );
            child_ends(
                &fx,
                child,
                ct,
                EventBody::RunCompleted(RunCompleted {
                    meta: meta(),
                    output: payload(b"{\"ok\":1}"),
                }),
            )
            .await;
            let j = fx.wait_completion(&run, op).await;
            assert_eq!(
                completion_of(&j, op),
                Some(Ok(OpOutput::Child(payload(b"{\"ok\":1}"))))
            );
            j
        }
    };
    // A second repair (another activation) does nothing more, and nothing completes twice.
    let sink3 = fx.sink(run, t2, Fake::leaf());
    sink3.repair_unsupported(&j).await.expect("repair");
    fx.drain().await;
    let j = fx.journal(&run).await;
    assert_eq!(completions(&j, op), 1, "{case:?}: exactly one completion");
    assert!(fx.row(&run, &child).is_none(), "{case:?}: row consumed");

    // The parent completes.
    let end = j.len() as u64;
    let sink4 = fx.sink(run, t2, Fake::leaf());
    assert_eq!(
        sink4
            .append(
                &rref(&run),
                t2.n,
                end,
                vec![ev(
                    end,
                    EventBody::RunCompleted(RunCompleted {
                        meta: meta(),
                        output: payload(b"{}"),
                    })
                )]
            )
            .await,
        Ok(end + 1)
    );
    assert!(fx.record(&run).expect("parent").status.is_terminal());
}

#[tokio::test]
async fn c_child_10_child_already_cancelled() {
    scenario(Case::AlreadyCancelled).await;
}

#[tokio::test]
async fn c_child_10_child_completed_while_parent_terminal() {
    scenario(Case::CompletedWhileParentTerminal).await;
}

#[tokio::test]
async fn c_child_10_child_still_running() {
    scenario(Case::StillRunning).await;
}
