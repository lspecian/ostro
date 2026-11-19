//! Resume re-arm through apply (plan 05-11, B2): the executor submits a **zero-event**
//! `EventsAppended{from_seq: next_seq, events: [], derived: rearm}` before activating a
//! resumed run. Apply already accepts intent-only batches (the executor's `Suspend` /
//! `EnqueueReady` use them), so no apply change is needed; this suite pins the behaviour:
//! accepted with `next_seq` unchanged, idempotent on repeat, CAS-checked like any batch.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use bytes::Bytes;
use common::{P0, T, handle_of, open_node, run_on, start_run};
use ostro_partition::store::values::RunStatus;
use ostro_partition::{ApplyOutcome, PartitionHandle, RejectReason, StartMode};
use ostro_types::command::{
    AttemptTokenMsg, Derived, EventsAppended, JournalEvent, ScheduleTimer, StartRun, WaitKind,
    derived::Intent,
};
use ostro_types::journal::{
    CommandHash, CommandMeta, Event, EventBody, SignalWaitStarted, TimerStarted, encode_event,
};
use ostro_types::{AttemptToken, Command, PayloadRef, RunId};
use tokio::time::timeout;

type Handle = PartitionHandle<ostro_log::Rf1Log>;

/// Far in the future (ms), so neither a run timeout nor a re-armed timer fires.
const FAR_MS: u64 = 4_102_444_800_000;

fn jev(seq: u64, body: EventBody) -> JournalEvent {
    let (kind, bytes) = encode_event(&Event {
        seq,
        ts_nanos: FAR_MS * 1_000_000,
        body,
    });
    JournalEvent {
        seq,
        kind,
        payload: Some(PayloadRef::inline(Bytes::from(bytes))),
    }
}

fn meta() -> CommandMeta {
    CommandMeta {
        hash_ver: 1,
        hash: CommandHash([0; 16]),
    }
}

fn appended(
    run: &RunId,
    t: AttemptToken,
    from: u64,
    events: Vec<JournalEvent>,
    d: Vec<Derived>,
) -> Command {
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        }),
        from_seq: from,
        events,
        derived: d,
    })
}

async fn submit(h: &Handle, c: Command) -> ApplyOutcome {
    timeout(T, h.submit_one(c))
        .await
        .expect("submit timed out")
        .expect("submit")
}

fn rearm() -> Vec<Derived> {
    vec![
        Derived::new(Intent::ScheduleTimer(ScheduleTimer {
            op: 1,
            due_at_ms: FAR_MS + 5,
        })),
        Derived::register_wait(2, WaitKind::Signal, "x", FAR_MS + 9, FAR_MS),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_event_rearm_is_accepted_idempotent_and_cas_checked() {
    let tmp = ostro_testkit::fs::tempdir();
    let node = open_node(tmp.path(), 1, StartMode::Leader).await;
    let h = handle_of(&node, P0);
    let run = run_on(0);
    assert_eq!(
        submit(
            h,
            Command::start_run(StartRun {
                requested_at_ms: FAR_MS,
                ready_at_ms: FAR_MS,
                ..start_run(&run)
            })
        )
        .await,
        ApplyOutcome::Accepted
    );
    let t = h.grant_attempt(run).await.expect("grant");
    // seq 0 placeholder (RunStarted's place) + a sleep + a signal wait, no intents: the
    // state a resumed run is in after its terminal transition removed rows.
    let events = vec![
        jev(
            0,
            EventBody::TimerStarted(TimerStarted {
                meta: meta(),
                fire_at_nanos: 1,
            }),
        ),
        jev(
            1,
            EventBody::TimerStarted(TimerStarted {
                meta: meta(),
                fire_at_nanos: (FAR_MS + 5) * 1_000_000,
            }),
        ),
        jev(
            2,
            EventBody::SignalWaitStarted(SignalWaitStarted {
                meta: meta(),
                name: "x".into(),
                timeout_ms: Some(9),
            }),
        ),
    ];
    assert_eq!(
        submit(h, appended(&run, t, 0, events, vec![])).await,
        ApplyOutcome::Accepted
    );
    let rec = h.read_run(&run).expect("read").expect("run");
    assert_eq!((rec.next_seq, rec.open_waits), (3, 0));

    // The re-arm: zero events, intents only, at the journal end.
    assert_eq!(
        submit(h, appended(&run, t, 3, vec![], rearm())).await,
        ApplyOutcome::Accepted
    );
    let rec = h.read_run(&run).expect("read").expect("run");
    assert_eq!(rec.next_seq, 3, "next_seq unchanged");
    assert_eq!(rec.open_waits, 1);
    assert_eq!(rec.status, RunStatus::Running);

    // A repeat (restart lost the executor's in-memory marker) changes nothing.
    assert_eq!(
        submit(h, appended(&run, t, 3, vec![], rearm())).await,
        ApplyOutcome::Accepted
    );
    let rec = h.read_run(&run).expect("read").expect("run");
    assert_eq!((rec.next_seq, rec.open_waits), (3, 1));

    // A stale journal end is a SeqMismatch like any batch (the executor re-activates).
    assert_eq!(
        submit(h, appended(&run, t, 2, vec![], rearm())).await,
        ApplyOutcome::Rejected(RejectReason::SeqMismatch { expected: 3 })
    );
    node.shutdown().await.expect("shutdown");
}
