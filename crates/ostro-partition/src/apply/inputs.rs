//! Journaling an input at `next_seq` and waking the run (plan 05-02; extracted from the 04-13
//! `TimerFired` path in `timer.rs`).
//!
//! Two helpers, one row format (a prost [`JournalEvent`] in `events`, the same encoding as
//! `EventsAppended` rows):
//!
//! - [`journal_input`] encodes an **apply-built** event with `ts_nanos = ts_ms * 1_000_000`.
//!   Only for events without user-sized data: `TimerFired`, `WaitTimedOut`, `CancelRequested`,
//!   `RunTerminated`, `SignalMatched`.
//! - [`journal_encoded`] writes **submitter-encoded** event bytes verbatim (D21):
//!   `JournalEvent{seq: next_seq, kind, payload: event}`. Apply never decodes, re-encodes or
//!   fetches them, so a blob-backed event (> 256 KiB) costs no blob I/O and its `BlobRef`
//!   stays in a `PayloadRef` field (BlobRef invariant).
//!
//! Both move `next_seq` by one and return the seq they wrote. Waking by status:
//! `Scheduled` becomes `Ready` (ready row + `EnqueueReady`), `Running` gets
//! `Action::InputAppended{run, seq}` (the `running` row is untouched), `Ready` gets nothing
//! more. Because `next_seq` moves, any `EventsAppended` built before the input was seen (a
//! `Suspend` included) is rejected `SeqMismatch`: the lost-wake-up window stays closed.
//!
//! Callers own the run record: they check that the run is live, then `store_run` it after
//! the call. No reads.

use bytes::Bytes;
use ostro_types::command::JournalEvent;
use ostro_types::journal::{Event, EventBody, EventKind, encode_event};
use ostro_types::{PayloadRef, RunId};

use super::inbound::make_ready;
use super::outcome::Action;
use super::run::put_event;
use crate::store::values::{RunRecord, RunStatus};
use crate::store::{ApplyBatch, StoreError};

/// Journals the apply-built `body` at `rec.next_seq` (ts `ts_ms`) and wakes the run. A
/// `Scheduled` run becomes ready at `ts_ms`.
pub(crate) fn journal_input(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    ts_ms: u64,
    body: EventBody,
    actions: &mut Vec<Action>,
) -> Result<u64, StoreError> {
    let seq = rec.next_seq;
    let (kind, bytes) = encode_event(&Event {
        seq,
        ts_nanos: ts_ms.saturating_mul(1_000_000),
        body,
    });
    put_event(
        b,
        run,
        &JournalEvent {
            seq,
            kind,
            payload: Some(PayloadRef::inline(Bytes::from(bytes))),
            ..Default::default()
        },
    );
    advance_and_wake(b, run, rec, seq, ts_ms, actions);
    Ok(seq)
}

/// Journals submitter-encoded event bytes verbatim at `rec.next_seq` (D21).
///
/// Callers: inbound `Signal` (`waits::inbound_signal`); 05-08 adds SignalRun and
/// CompleteToken (`TokenCompleted`). With `wake`, the
/// run is woken; a `Scheduled` run becomes ready at `rec.created_at_ms` (apply cannot read the
/// ts inside `event`, and the earliest ready time keeps it at the front of its priority).
pub(crate) fn journal_encoded(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    kind: EventKind,
    event: &PayloadRef,
    wake: bool,
    actions: &mut Vec<Action>,
) -> Result<u64, StoreError> {
    let seq = rec.next_seq;
    put_event(
        b,
        run,
        &JournalEvent {
            seq,
            kind: kind.code(),
            payload: Some(event.clone()),
            ..Default::default()
        },
    );
    if wake {
        let at = rec.created_at_ms;
        advance_and_wake(b, run, rec, seq, at, actions);
    } else {
        rec.next_seq = seq.saturating_add(1);
    }
    Ok(seq)
}

fn advance_and_wake(
    b: &mut ApplyBatch<'_>,
    run: &RunId,
    rec: &mut RunRecord,
    seq: u64,
    ready_at_ms: u64,
    actions: &mut Vec<Action>,
) {
    // u64 seqs cannot be exhausted in practice; saturating keeps apply total (EventsAppended
    // rejects at the same bound).
    rec.next_seq = seq.saturating_add(1);
    match rec.status {
        RunStatus::Scheduled => make_ready(b, run, rec, ready_at_ms, actions),
        RunStatus::Running => actions.push(Action::InputAppended { run: *run, seq }),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use ostro_types::command::JournalEvent;
    use ostro_types::journal::{EventBody, EventKind, TimerFired};
    use ostro_types::{PartitionCount, PartitionId, PayloadRef, RunId};
    use prost::Message;

    use super::{journal_encoded, journal_input};
    use crate::apply::outcome::Action;
    use crate::store::keys::{event_key, ready_key};
    use crate::store::values::{RunRecord, RunStatus};
    use crate::store::{PartitionStore, StoreConfig, Table};

    fn store() -> (tempfile::TempDir, PartitionStore) {
        let dir = ostro_testkit::fs::tempdir();
        let (s, _) = PartitionStore::open(
            dir.path(),
            PartitionId(1),
            PartitionCount::new(4).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open");
        (dir, s)
    }

    fn run() -> RunId {
        let mut b = [0u8; 18];
        b[1] = 1;
        b[17] = 9;
        RunId::from_bytes(&b).expect("run id")
    }

    fn rec(status: RunStatus, next_seq: u64) -> RunRecord {
        RunRecord {
            workflow: "wf".into(),
            queue: "q".into(),
            status,
            next_seq,
            created_at_ms: 5,
            ..Default::default()
        }
    }

    fn row(s: &PartitionStore, r: &RunId, seq: u64) -> JournalEvent {
        let v = s
            .reader()
            .get(Table::Events, &event_key(r, seq))
            .expect("read")
            .expect("event row");
        JournalEvent::decode(v.as_slice()).expect("journal event")
    }

    #[test]
    fn journal_encoded_writes_blob_and_inline_events_verbatim() {
        let (_d, s) = store();
        let r = run();
        // A 1 MiB blob-backed event: only its PayloadRef is journaled, never fetched.
        let blob = PayloadRef::blob([0xab; 32], 1 << 20);
        let inline = PayloadRef::inline(bytes::Bytes::from_static(b"\x01opaque-not-postcard"));
        let mut rec = rec(RunStatus::Running, 3);
        let mut actions = Vec::new();
        let mut b = s.batch();
        let s1 = journal_encoded(
            &mut b,
            &r,
            &mut rec,
            EventKind::SignalPayload,
            &blob,
            true,
            &mut actions,
        )
        .expect("journal");
        let s2 = journal_encoded(
            &mut b,
            &r,
            &mut rec,
            EventKind::TokenCompleted,
            &inline,
            false,
            &mut actions,
        )
        .expect("journal");
        assert_eq!(b.reads(), 0, "no reads, no blob fetch");
        b.commit(&crate::store::MetaUpdate {
            applied: ostro_types::Offset(1),
            state_hash: [0; 32],
            cursor: Default::default(),
            inbound_seen_changed: vec![],
        })
        .expect("commit");
        assert_eq!((s1, s2, rec.next_seq), (3, 4, 5));
        assert_eq!(
            row(&s, &r, 3),
            JournalEvent {
                seq: 3,
                kind: EventKind::SignalPayload.code(),
                payload: Some(blob),
                ..Default::default()
            }
        );
        assert_eq!(
            row(&s, &r, 4),
            JournalEvent {
                seq: 4,
                kind: EventKind::TokenCompleted.code(),
                payload: Some(inline),
                ..Default::default()
            }
        );
        // Only the waking call woke the Running run.
        assert_eq!(actions, vec![Action::InputAppended { run: r, seq: 3 }]);
    }

    #[test]
    fn wake_depends_on_status() {
        let (_d, s) = store();
        let r = run();
        let ev = PayloadRef::inline(bytes::Bytes::from_static(b"e"));

        // Scheduled -> Ready at created_at_ms (journal_encoded) with EnqueueReady.
        let mut sched = rec(RunStatus::Scheduled, 1);
        let mut actions = Vec::new();
        let mut b = s.batch();
        journal_encoded(
            &mut b,
            &r,
            &mut sched,
            EventKind::SignalPayload,
            &ev,
            true,
            &mut actions,
        )
        .expect("journal");
        assert_eq!(sched.status, RunStatus::Ready);
        assert!(
            b.get(Table::Ready, &ready_key("q", 0, 5, &r))
                .expect("read")
                .is_some()
        );
        assert!(matches!(
            actions.as_slice(),
            [Action::EnqueueReady { ready_at_ms: 5, .. }]
        ));

        // Ready -> nothing more; journal_input wakes Scheduled at ts_ms.
        let mut ready = rec(RunStatus::Ready, 0);
        let mut actions = Vec::new();
        let seq = journal_input(
            &mut b,
            &r,
            &mut ready,
            77,
            EventBody::TimerFired(TimerFired { op: 1 }),
            &mut actions,
        )
        .expect("journal");
        assert_eq!((seq, ready.next_seq), (0, 1));
        assert!(actions.is_empty());
        let mut sched = rec(RunStatus::Scheduled, 0);
        journal_input(
            &mut b,
            &r,
            &mut sched,
            77,
            EventBody::TimerFired(TimerFired { op: 1 }),
            &mut actions,
        )
        .expect("journal");
        assert!(matches!(
            actions.as_slice(),
            [Action::EnqueueReady {
                ready_at_ms: 77,
                ..
            }]
        ));
    }
}
