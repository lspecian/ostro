//! `child::` failpoints converge (plan 09-08; D-P9-25 W1). Each failpoint makes the sink's
//! `append` fail with `Unavailable` at its site; the test then does what the executor does
//! (a fresh sink, journal-load repair, the engine re-appending the same flush) and checks
//! that exactly one child run and one parent completion exist.
//!
//! - `child::start_child_after_flush`: after the accepted flush of a batch with a
//!   `ChildStarted`, before any post-flush completion;
//! - `child::detached_after_flush`: before a detached child's `{run_id}` completion;
//! - `child::parent_event_before_flush`: after a child's `parent_event` was prepared, before
//!   its terminal append.
//!
//! Run single-threaded (failpoints are process-global): `--test-threads=1`.
#![cfg(feature = "failpoints")]

mod children_fx;

use children_fx::*;
use ostro_engine::sink::{ActivationSink, AppendError};
use ostro_server::children::{UNKNOWN_WORKFLOW_KIND, child_run_id, parent_event_bytes};
use ostro_types::command::CHILD_ROW_JOURNAL;
use ostro_types::journal::{EventBody, OpOutput, RunCompleted};
use ostro_types::{PartitionId, RunId};

fn injected(r: Result<u64, AppendError>, name: &str) {
    match r {
        Err(AppendError::Unavailable(m)) => assert!(m.contains(name), "{m}"),
        other => panic!("expected the injected {name} failure, got {other:?}"),
    }
}

#[tokio::test]
async fn start_child_after_flush_converges_to_one_child() {
    let scenario = fail::FailScenario::setup();
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    fail::cfg("child::start_child_after_flush", "1*return").expect("cfg");
    let sink = fx.sink(run, token, Fake::leaf());
    injected(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", attached())],
        )
        .await,
        "child::start_child_after_flush",
    );
    // The executor retries: a fresh sink repairs (row present: nothing) and the engine's
    // re-append of the same flush conflicts (it is already journaled).
    let child = child_run_id(&run, 1, 1);
    assert_eq!(fx.row(&run, &child).as_deref(), Some(CHILD_ROW_JOURNAL));
    let sink2 = fx.sink(run, token, Fake::leaf());
    sink2
        .repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    assert_eq!(
        sink2
            .append(
                &rref(&run),
                token.n,
                1,
                vec![child_started(1, "leaf", attached())]
            )
            .await,
        Err(AppendError::Conflict { expected: 2 })
    );
    fx.wait_record(&child).await;
    fx.drain().await;
    assert_eq!(fx.runs(), 2, "parent + exactly one child");
    assert_eq!(fx.journal(&run).await.len(), 2);
    scenario.teardown();
}

#[tokio::test]
async fn detached_after_flush_repair_journals_the_run_id_once() {
    let scenario = fail::FailScenario::setup();
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    fail::cfg("child::detached_after_flush", "1*return").expect("cfg");
    let sink = fx.sink(run, token, Fake::leaf());
    injected(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "leaf", detached())],
        )
        .await,
        "child::detached_after_flush",
    );
    let child = child_run_id(&run, 1, 1);
    assert_eq!(completion_of(&fx.journal(&run).await, 1), None);
    // Journal-load repair: the start is re-sent (duplicate) and the completion journaled.
    let sink2 = fx.sink(run, token, Fake::leaf());
    sink2
        .repair_unsupported(&fx.journal(&run).await)
        .await
        .expect("repair");
    let j = fx.journal(&run).await;
    assert_eq!(completion_of(&j, 1), Some(Ok(run_id_payload(&child))));
    let sink3 = fx.sink(run, token, Fake::leaf());
    sink3.repair_unsupported(&j).await.expect("repair");
    fx.drain().await;
    let j = fx.journal(&run).await;
    assert_eq!(completions(&j, 1), 1, "once");
    assert_eq!(fx.runs(), 2, "parent + exactly one detached child");
    scenario.teardown();
}

#[tokio::test]
async fn parent_event_before_flush_retry_reencodes_identical_bytes() {
    let scenario = fail::FailScenario::setup();
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
    .expect("start child");
    let child = child_run_id(&run, 1, 1);
    fx.wait_record(&child).await;
    let ct = fx.grant(&child).await;
    fx.raw(&child, ct, 0, &[ev(0, step_body())]).await;
    let done = ev(
        1,
        EventBody::RunCompleted(RunCompleted {
            meta: meta(),
            output: payload(b"{\"ok\":1}"),
        }),
    );
    fail::cfg("child::parent_event_before_flush", "1*return").expect("cfg");
    let cs = fx.sink(child, ct, Fake::leaf());
    injected(
        cs.append(&rref(&child), ct.n, 1, vec![done.clone()]).await,
        "child::parent_event_before_flush",
    );
    assert_eq!(fx.journal(&child).await.len(), 1, "nothing appended");
    // Re-activation: the same terminal event re-derives the same parent event.
    let cs2 = fx.sink(child, ct, Fake::leaf());
    assert_eq!(cs2.append(&rref(&child), ct.n, 1, vec![done]).await, Ok(2));
    let j = fx.wait_completion(&run, 1).await;
    fx.drain().await;
    let j2 = fx.journal(&run).await;
    assert_eq!(completions(&j2, 1), 1, "one parent completion");
    assert_eq!(
        completion_of(&j, 1),
        Some(Ok(OpOutput::Child(payload(b"{\"ok\":1}"))))
    );
    let rows =
        ostro_partition::query::journal_rows(&fx.h(&run).reader(), &run, 2, 1).expect("rows");
    let bytes = fx.h(&run).fetch_payload(&rows[0].2).await.expect("bytes");
    assert_eq!(
        bytes.as_ref(),
        parent_event_bytes(
            1,
            Ok(OpOutput::Child(payload(b"{\"ok\":1}"))),
            TS0 + 1_000_000
        )
        .as_slice()
    );
    scenario.teardown();
}

/// W1: the target was unresolvable at the flush, the node crashed (failpoint) before
/// `unknown_workflow` was journaled, then the target was deployed: the repair starts the real
/// child and never journals `unknown_workflow`.
#[tokio::test]
async fn deploy_between_crash_and_repair_starts_the_real_child() {
    let scenario = fail::FailScenario::setup();
    let fx = Fx::new(1).await;
    let (run, token) = fx
        .start(base_start(&RunId::new_v7(PartitionId(0)), None))
        .await;
    fail::cfg("child::start_child_after_flush", "1*return").expect("cfg");
    let sink = fx.sink(run, token, Fake::leaf());
    injected(
        sink.append(
            &rref(&run),
            token.n,
            1,
            vec![child_started(1, "late", attached())],
        )
        .await,
        "child::start_child_after_flush",
    );
    let j = fx.journal(&run).await;
    assert_eq!(j.len(), 2, "no unknown_workflow journaled yet");
    let child = child_run_id(&run, 1, 1);
    assert!(fx.row(&run, &child).is_none());
    // Deploy `late`, restart: the repair re-resolves and starts the child.
    let mut deployed = Fake::leaf();
    deployed.add("acme/late", 1, [0x77; 32]);
    let sink2 = fx.sink(run, token, deployed.clone());
    sink2.repair_unsupported(&j).await.expect("repair");
    let rec = fx.wait_record(&child).await;
    assert_eq!(rec.workflow, "acme/late");
    assert_eq!(rec.module_sha, Some([0x77; 32]));
    assert_eq!(fx.row(&run, &child).as_deref(), Some(CHILD_ROW_JOURNAL));
    let j = fx.journal(&run).await;
    assert!(
        j.iter()
            .all(|e| !matches!(&e.body, EventBody::ChildCompleted(_))),
        "no {UNKNOWN_WORKFLOW_KIND} completion"
    );
    let sink3 = fx.sink(run, token, deployed);
    sink3.repair_unsupported(&j).await.expect("repair");
    fx.drain().await;
    assert_eq!(fx.runs(), 2);
    assert_eq!(fx.journal(&run).await.len(), 2);
    scenario.teardown();
}
