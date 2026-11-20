//! `rebuild_digest` generator hook for in-flight effects (plan **06-03**; FX-01, FX-03).
//!
//! `tests/rebuild_digest.rs` merges [`strategy`] into its command strategy and, for every
//! generated op, submits [`commands`] against the driver's runs (their `next_seq` is re-read
//! from live state first). The ops are mostly valid, with deliberate repeats and misses:
//!
//! - `Schedule`: an `EventsAppended` with one opaque `EffectScheduled` (kind 18) event and a
//!   `RegisterEffect` for its seq (kind 0..=2, or the invalid 3: `BadIntent`), optionally
//!   repeated in the batch (idempotent), optionally with a `CancelEffect` for an earlier op,
//!   optionally followed by a `Suspend` (so a later `CompleteEffect` or leader start wakes the
//!   run).
//! - `Complete`: `CompleteEffect` for an op `back` seqs below `next_seq` (valid for an open
//!   row, else `Duplicate`; repeats are `Duplicate`), or for `next_seq + ahead`
//!   (`Invalid(BadEffectCompletion)`), inline or as a blob ref of a > 256 KiB outcome (never
//!   fetched: apply has no blob access).
//! - `Cancel`: an `EventsAppended` carrying only `CancelEffect` for a recent op.
//! - `Terminal`: an `EventsAppended` ending in `CompleteRun`, resumable or not (P-8).
//! - `Announce`: a `LeaderAnnounced` for the current epoch, submitted through the leader, so
//!   it is a deterministic `StaleEpoch` rejection. Accepted announcements (and with them the
//!   P-9 wake) come from the base generator's `Restart` op, which reopens the node.
//!
//! Outcomes are not asserted here. The properties (in `rebuild_digest.rs`) are that
//! rebuilding from the log reproduces the live digest and that no `effects` row survives for
//! a non-resumable terminal run.

use bytes::Bytes;
use ostro_types::command::{
    AttemptTokenMsg, CompleteEffect, CompleteRun, Derived, EventsAppended, JournalEvent,
    TerminalStatus, complete_effect_payload, derived,
};
use ostro_types::journal::RecordedResult;
use ostro_types::{Command, PayloadRef};
use proptest::prelude::*;

use super::GenRun;

/// One generated effect operation.
#[derive(Clone, Debug)]
pub enum EffectOp {
    Schedule {
        run: usize,
        /// `RegisterEffect.kind` (3 is invalid).
        kind: u32,
        /// Repeat the `RegisterEffect` in the same batch (must be a no-op).
        repeat: bool,
        /// `CancelEffect` for the op `back` seqs below this one.
        cancel_back: Option<u64>,
        /// Follow with an `EventsAppended{Suspend}`.
        suspend: bool,
    },
    Complete {
        run: usize,
        /// `Ok(back)`: op `next_seq - 1 - back`; `Err(ahead)`: op `next_seq + ahead`.
        op: Result<u64, u64>,
        blob: bool,
        /// Submit the same completion twice.
        twice: bool,
    },
    Cancel {
        run: usize,
        back: u64,
    },
    Terminal {
        run: usize,
        resumable: bool,
        failed: bool,
    },
    Announce {
        run: usize,
    },
}

/// Strategy for one optional [`EffectOp`].
pub fn strategy() -> BoxedStrategy<Option<EffectOp>> {
    let op = prop_oneof![
        4 => (
            any::<usize>(),
            prop_oneof![8 => 0u32..3, 1 => Just(3u32)],
            proptest::bool::weighted(0.2),
            proptest::option::weighted(0.2, 0u64..4),
            proptest::bool::weighted(0.3),
        )
            .prop_map(|(run, kind, repeat, cancel_back, suspend)| EffectOp::Schedule {
                run, kind, repeat, cancel_back, suspend
            }),
        4 => (
            any::<usize>(),
            prop_oneof![6 => (0u64..4).prop_map(Ok), 1 => (0u64..3).prop_map(Err)],
            proptest::bool::weighted(0.15),
            proptest::bool::weighted(0.2),
        )
            .prop_map(|(run, op, blob, twice)| EffectOp::Complete { run, op, blob, twice }),
        1 => (any::<usize>(), 0u64..4).prop_map(|(run, back)| EffectOp::Cancel { run, back }),
        1 => (any::<usize>(), any::<bool>(), any::<bool>())
            .prop_map(|(run, resumable, failed)| EffectOp::Terminal { run, resumable, failed }),
        1 => any::<usize>().prop_map(|run| EffectOp::Announce { run }),
    ];
    proptest::option::weighted(0.8, op).boxed()
}

/// An `EventsAppended` of `r` at `from` with `events` opaque `EffectScheduled` events.
fn append(r: &GenRun, from: u64, events: u64, derived: Vec<Derived>) -> Option<Command> {
    let t = r.token?;
    Some(Command::events_appended(EventsAppended {
        run_id: r.id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        }),
        from_seq: from,
        events: (from..from + events)
            .map(|seq| JournalEvent {
                seq,
                kind: 18,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"fx"))),
            })
            .collect(),
        derived,
    }))
}

fn complete_effect(r: &GenRun, op: u64, blob: bool) -> Command {
    let outcome = if blob {
        let result = RecordedResult {
            codec_ver: 1,
            bytes: vec![0x42; 300 * 1024],
        };
        let len = complete_effect_payload(op, &result, 1_000_000).len() as u64;
        PayloadRef::blob([(op % 251) as u8; 32], len)
    } else {
        let result = RecordedResult {
            codec_ver: 1,
            bytes: op.to_be_bytes().to_vec(),
        };
        PayloadRef::inline(Bytes::from(complete_effect_payload(op, &result, 1_000_000)))
    };
    Command::complete_effect(CompleteEffect {
        run_id: r.id.as_bytes().to_vec(),
        op,
        outcome: Some(outcome),
        completed_at_ms: 1_000,
    })
}

/// `(partition, command)` pairs for `op` over the driver's runs.
pub fn commands(op: &EffectOp, runs: &[GenRun], _n: usize) -> Vec<(u16, Command)> {
    let pick = |i: usize| (!runs.is_empty()).then(|| &runs[i % runs.len()]);
    match op {
        EffectOp::Schedule {
            run,
            kind,
            repeat,
            cancel_back,
            suspend,
        } => pick(*run)
            .map(|r| {
                let op = r.next_seq;
                let mut d = vec![Derived::register_effect(op, *kind, 100 + op)];
                if *repeat {
                    d.push(Derived::register_effect(op, *kind, 100 + op));
                }
                if let Some(back) = cancel_back {
                    d.push(Derived::cancel_effect(op.saturating_sub(1 + back)));
                }
                let mut out: Vec<(u16, Command)> = append(r, op, 1, d)
                    .map(|c| (r.partition, c))
                    .into_iter()
                    .collect();
                if *suspend && let Some(c) = append(r, op + 1, 0, vec![Derived::suspend()]) {
                    out.push((r.partition, c));
                }
                out
            })
            .unwrap_or_default(),
        EffectOp::Complete {
            run,
            op,
            blob,
            twice,
        } => pick(*run)
            .map(|r| {
                let target = match op {
                    Ok(back) => r.next_seq.saturating_sub(1 + back),
                    Err(ahead) => r.next_seq + ahead,
                };
                let c = complete_effect(r, target, *blob);
                let mut out = vec![(r.partition, c.clone())];
                if *twice {
                    out.push((r.partition, c));
                }
                out
            })
            .unwrap_or_default(),
        EffectOp::Cancel { run, back } => pick(*run)
            .and_then(|r| {
                let op = r.next_seq.saturating_sub(1 + back);
                append(r, r.next_seq, 0, vec![Derived::cancel_effect(op)])
                    .map(|c| vec![(r.partition, c)])
            })
            .unwrap_or_default(),
        EffectOp::Terminal {
            run,
            resumable,
            failed,
        } => pick(*run)
            .and_then(|r| {
                let status = if *failed {
                    TerminalStatus::Failed
                } else {
                    TerminalStatus::Completed
                };
                let c = Derived::new(derived::Intent::CompleteRun(CompleteRun {
                    status: status as i32,
                    result: Some(PayloadRef::inline(Bytes::from_static(b"res"))),
                    completed_at_ms: 900,
                    resumable: *resumable,
                    parent_event: None,
                }));
                append(r, r.next_seq, 1, vec![c]).map(|c| vec![(r.partition, c)])
            })
            .unwrap_or_default(),
        EffectOp::Announce { run } => pick(*run)
            .map(|r| {
                let epoch = r.token.map_or(1, |t| t.epoch.0);
                vec![(r.partition, Command::leader_announced(epoch, 1, 0))]
            })
            .unwrap_or_default(),
    }
}
