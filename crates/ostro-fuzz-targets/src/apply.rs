//! Fuzz target `apply_command` (14-07, SEC-05): the deterministic partition apply
//! (`ostro_partition::apply_command`) over arbitrary command sequences.
//!
//! Every input runs against a fresh tempdir [`PartitionStore`] (partition 5 of 16, the
//! partition of the frozen golden commands) seeded with a leader epoch and one run.
//!
//! Input: `selector ‖ bytes`.
//! - `selector & 1 == 0` (raw): `bytes` is a sequence of frames
//!   `env: u8 ‖ len: u16 LE ‖ command bytes` (prost `Command` encodings, at most
//!   [`MAX_CMDS`]).
//! - `selector & 1 == 1` (structured): `bytes` drives [`arbitrary::Unstructured`] to build
//!   well-formed commands (start, grant, append with Derived intents, timers, signals, cancel,
//!   terminate, retry, tokens, effects, pause/resume, leader changes) over a small pool of
//!   run ids, so apply logic past decoding is reached.
//!
//! `env` picks the envelope epoch of a frame: the current epoch, the next one, or the
//! previous one (stale).
//!
//! Invariants:
//! - apply never panics; undecodable bytes are `Rejected(Invalid(Decode))`; the only error
//!   is the designed fail-stop for a command written by a newer Ostro
//!   (`StoreError::NewerFormat`), which ends the sequence as it halts a partition;
//! - every step commits and the state digest is computable (every 8 steps and at the end);
//! - apply is deterministic: the same sequence on a second fresh store ends with the same
//!   logical digest and the same rolling state hash (checked when `selector & 2`).

use arbitrary::Unstructured;
use ostro_partition::apply::InvalidCode;
use ostro_partition::{
    ApplyOutcome, Cursor, PartitionStore, RejectReason, StoreConfig, StoreError,
};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CancelTimer, CancelWait, CompleteEffect, CompleteRun,
    CompleteToken, Derived, EnqueueReady, EventsAppended, JournalEvent, PauseRun, RegisterWait,
    RequestCancel, ResumeRun, RetryRun, ScheduleTimer, SignalRun, StartRun, TerminateRun,
    TimerFired, decode_command, derived, encode_command,
};
use ostro_types::payload::payload_ref;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef};

/// Commands applied per input.
pub const MAX_CMDS: usize = 24;

/// The store's partition (the golden commands' run ids name partition 5).
pub const PARTITION: u16 = 5;

/// The run every input starts with.
pub const SEEDED_RUN: [u8; 18] = run_id(0xaa);

/// Run id `[partition BE ‖ 16 × fill]`.
pub const fn run_id(fill: u8) -> [u8; 18] {
    let mut id = [fill; 18];
    let p = PARTITION.to_be_bytes();
    id[0] = p[0];
    id[1] = p[1];
    id
}

/// One command of a sequence: envelope epoch selector and command bytes.
#[derive(Clone, Debug)]
pub struct Step {
    /// `0..=5` current epoch, `6` next epoch, `7` previous epoch; LeaderAnnounced built in
    /// structured mode carries its own epoch (`8`).
    pub env: u8,
    /// Encoded command.
    pub bytes: Vec<u8>,
}

/// Fuzz entry point.
pub fn apply_command(data: &[u8]) {
    let Some((&sel, bytes)) = data.split_first() else {
        return;
    };
    let steps = if sel & 1 == 0 {
        raw(bytes)
    } else {
        structured(bytes)
    };
    let a = run(&steps);
    // The determinism re-run doubles the cost (two stores): half the inputs (selector bit 1).
    if sel & 2 != 0 {
        let b = run(&steps);
        assert_eq!(a, b, "apply is not deterministic");
    }
}

/// Raw frames (see the module docs).
pub fn raw(mut b: &[u8]) -> Vec<Step> {
    let mut out = Vec::new();
    while out.len() < MAX_CMDS && b.len() >= 3 {
        let env = b[0] % 8;
        let len = usize::from(u16::from_le_bytes([b[1], b[2]])).min(b.len() - 3);
        out.push(Step {
            env,
            bytes: b[3..3 + len].to_vec(),
        });
        b = &b[3 + len..];
    }
    out
}

/// Encode steps as raw frames (inverse of [`raw`] for frames shorter than 64 KiB).
pub fn to_raw(steps: &[Step]) -> Vec<u8> {
    let mut out = vec![0u8];
    for s in steps {
        out.push(s.env);
        out.extend_from_slice(&(s.bytes.len() as u16).to_le_bytes());
        out.extend_from_slice(&s.bytes);
    }
    out
}

/// Final `(logical digest, rolling state hash, steps applied)` of a sequence.
fn run(steps: &[Step]) -> ([u8; 32], [u8; 32], usize) {
    let dir = crate::wal::scratch_dir();
    let (store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(PARTITION),
        PartitionCount::new(16).expect("non-zero"),
        &StoreConfig::default(),
    )
    .expect("open store");
    let mut cur = Cursor::from_meta(&meta);

    let seed = [
        Step {
            env: 8,
            bytes: encode_command(&Command::leader_announced(1, 1, 1_000)).to_vec(),
        },
        Step {
            env: 0,
            bytes: encode_command(&Command::start_run(StartRun {
                run_id: SEEDED_RUN.to_vec(),
                workflow: "order".into(),
                workflow_version: 1,
                queue: "default".into(),
                input: Some(inline(b"{}")),
                requested_at_ms: 1_000,
                module_sha: vec![0xab; 32],
                rng_seed: vec![1; 16],
                ..Default::default()
            }))
            .to_vec(),
        },
    ];
    for s in &seed {
        let out = step(&store, &mut cur, s).expect("seed applies");
        assert_eq!(out, ApplyOutcome::Accepted, "seed rejected");
    }
    let mut applied = 0;
    for s in steps {
        match step(&store, &mut cur, s) {
            Some(_) => applied += 1,
            None => break,
        }
        if applied % 8 == 0 {
            store.digest().expect("digest");
        }
    }
    let digest = store.digest().expect("digest");
    (digest, cur.hash, applied)
}

/// Apply one step in its own batch and commit; `None` = fail-stop (newer format).
fn step(store: &PartitionStore, cur: &mut Cursor, s: &Step) -> Option<ApplyOutcome> {
    let current = cur.epoch().0;
    let env = match s.env {
        0..=5 => current,
        6 => current.saturating_add(1),
        7 => current.saturating_sub(1),
        _ => decode_command(&s.bytes)
            .ok()
            .and_then(|c| match c.cmd {
                Some(ostro_types::Cmd::LeaderAnnounced(la)) => Some(la.epoch),
                _ => None,
            })
            .unwrap_or(current),
    };
    let mut b = store.batch();
    let mut actions = Vec::new();
    let off = cur.next;
    let out = match ostro_partition::apply_command(
        &mut b,
        cur,
        off,
        Epoch(env),
        &s.bytes,
        &mut actions,
    ) {
        Ok(o) => o,
        Err(StoreError::NewerFormat(_)) => return None,
        Err(e) => panic!("apply failed: {e}"),
    };
    if decode_command(&s.bytes).is_err() {
        assert_eq!(
            out,
            ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Decode)),
            "undecodable bytes not rejected as Invalid(Decode)"
        );
    }
    assert_eq!(cur.next, off.next(), "cursor did not advance by one");
    let mu = cur.meta_update();
    b.commit(&mu).expect("commit");
    Some(out)
}

fn inline(b: &[u8]) -> PayloadRef {
    PayloadRef {
        kind: Some(payload_ref::Kind::Inline(b.to_vec().into())),
    }
}

// ------------------------------------------------------------------------------ structured

const RUNS: [[u8; 18]; 3] = [SEEDED_RUN, run_id(0xbb), run_id(0xcc)];
const EVENT_KINDS: [u32; 12] = [1, 16, 17, 18, 19, 21, 23, 26, 27, 64, 66, 71];

/// Well-formed commands built from `bytes` (see the module docs).
pub fn structured(bytes: &[u8]) -> Vec<Step> {
    let mut u = Unstructured::new(bytes);
    let mut out = Vec::new();
    while out.len() < MAX_CMDS && !u.is_empty() {
        match one(&mut u) {
            Ok(s) => out.push(s),
            Err(_) => break,
        }
    }
    out
}

type R<T> = arbitrary::Result<T>;

fn run_of(u: &mut Unstructured<'_>) -> R<Vec<u8>> {
    Ok(RUNS[u.choose_index(RUNS.len())?].to_vec())
}

fn small(u: &mut Unstructured<'_>) -> R<u64> {
    u.int_in_range(0..=8)
}

fn ms(u: &mut Unstructured<'_>) -> R<u64> {
    Ok(1_000 + u64::from(u.int_in_range(0u32..=100_000)?))
}

fn payload(u: &mut Unstructured<'_>) -> R<Option<PayloadRef>> {
    if u.ratio(1, 4)? {
        return Ok(None);
    }
    let n = u.int_in_range(0..=24)?;
    Ok(Some(inline(u.bytes(n)?)))
}

fn token(u: &mut Unstructured<'_>) -> R<Option<AttemptTokenMsg>> {
    if u.ratio(1, 10)? {
        return Ok(None);
    }
    Ok(Some(AttemptTokenMsg {
        epoch: u.int_in_range(0..=3)?,
        n: u.int_in_range(0..=4)?,
    }))
}

fn name(u: &mut Unstructured<'_>) -> R<String> {
    Ok(["s1", "s2", "", "go"][u.choose_index(4)?].to_string())
}

fn intent(u: &mut Unstructured<'_>) -> R<Derived> {
    let i = match u.choose_index(9)? {
        0 => derived::Intent::ScheduleTimer(ScheduleTimer {
            op: small(u)?,
            due_at_ms: ms(u)?,
        }),
        1 => derived::Intent::CancelTimer(CancelTimer {
            op: small(u)?,
            due_at_ms: ms(u)?,
        }),
        2 => derived::Intent::EnqueueReady(EnqueueReady {
            ready_at_ms: ms(u)?,
        }),
        3 => derived::Intent::CompleteRun(CompleteRun {
            status: u.int_in_range(0..=6)?,
            result: payload(u)?,
            completed_at_ms: ms(u)?,
            resumable: u.arbitrary()?,
            parent_event: None,
        }),
        4 => return Ok(Derived::suspend()),
        5 => derived::Intent::RegisterWait(RegisterWait {
            op: small(u)?,
            kind: u.int_in_range(0..=3)?,
            name: name(u)?,
            timeout_due_ms: if u.arbitrary()? { ms(u)? } else { 0 },
            at_ms: ms(u)?,
        }),
        6 => derived::Intent::CancelWait(CancelWait { op: small(u)? }),
        7 => {
            return Ok(Derived::register_effect(
                small(u)?,
                u.int_in_range(0..=3)?,
                ms(u)?,
            ));
        }
        _ => return Ok(Derived::cancel_effect(small(u)?)),
    };
    Ok(Derived::new(i))
}

fn one(u: &mut Unstructured<'_>) -> R<Step> {
    let mut env = u.int_in_range(0..=7)?;
    let cmd = match u.choose_index(14)? {
        0 => {
            env = 8;
            Command::leader_announced(u.int_in_range(0..=3)?, 1, ms(u)?)
        }
        1 => Command::start_run(StartRun {
            run_id: run_of(u)?,
            workflow: ["order", "ship"][u.choose_index(2)?].into(),
            workflow_version: u.int_in_range(0..=2)?,
            queue: ["default", "q2"][u.choose_index(2)?].into(),
            input: payload(u)?,
            idempotency_key: if u.arbitrary()? {
                b"k1".to_vec()
            } else {
                Vec::new()
            },
            conflict_policy: u.int_in_range(0..=3)?,
            concurrency_key: if u.arbitrary()? {
                "c".into()
            } else {
                String::new()
            },
            priority: u.int_in_range(-2..=2)?,
            ready_at_ms: ms(u)?,
            requested_at_ms: ms(u)?,
            module_sha: vec![0xab; 32],
            rng_seed: vec![1; 16],
            tenant: if u.ratio(1, 4)? {
                "t1".into()
            } else {
                String::new()
            },
            run_timeout_ms: if u.ratio(1, 4)? { ms(u)? } else { 0 },
            retention_ms: if u.ratio(1, 4)? { ms(u)? } else { 0 },
            ..Default::default()
        }),
        2 => Command::attempt_granted(AttemptGranted {
            run_id: run_of(u)?,
            token: token(u)?,
            after_failure: u.arbitrary()?,
        }),
        3 => {
            let from_seq = small(u)?;
            let n = u.int_in_range(0..=3)?;
            let mut events = Vec::with_capacity(n);
            for i in 0..n as u64 {
                let gap = u64::from(u.ratio(1, 16)?);
                events.push(JournalEvent {
                    seq: from_seq + i + gap,
                    kind: EVENT_KINDS[u.choose_index(EVENT_KINDS.len())?],
                    payload: payload(u)?,
                    attempt: u.int_in_range(0..=2)?,
                });
            }
            let nd = u.int_in_range(0..=3)?;
            let mut derived = Vec::with_capacity(nd);
            for _ in 0..nd {
                derived.push(intent(u)?);
            }
            Command::events_appended(EventsAppended {
                run_id: run_of(u)?,
                token: token(u)?,
                from_seq,
                events,
                derived,
            })
        }
        4 => Command::timer_fired(TimerFired {
            run_id: run_of(u)?,
            op: small(u)?,
            due_at_ms: ms(u)?,
            fired_at_ms: ms(u)?,
        }),
        5 => Command::signal_run(SignalRun {
            run_id: run_of(u)?,
            name: name(u)?,
            event: payload(u)?,
            request_id: {
                let n = u.int_in_range(0..=4)?;
                u.bytes(n)?.to_vec()
            },
            sent_at_ms: ms(u)?,
        }),
        6 => Command::request_cancel(RequestCancel {
            run_id: run_of(u)?,
            reason: "r".into(),
            requested_at_ms: ms(u)?,
        }),
        7 => Command::terminate_run(TerminateRun {
            run_id: run_of(u)?,
            reason: "t".into(),
            terminated_at_ms: ms(u)?,
        }),
        8 => Command::retry_run(RetryRun {
            run_id: run_of(u)?,
            requested_at_ms: ms(u)?,
        }),
        9 => Command::complete_token(CompleteToken {
            run_id: run_of(u)?,
            op: small(u)?,
            event: payload(u)?,
            completed_at_ms: ms(u)?,
        }),
        10 => Command::complete_effect(CompleteEffect {
            run_id: run_of(u)?,
            op: small(u)?,
            outcome: payload(u)?,
            completed_at_ms: ms(u)?,
        }),
        11 => Command::pause_run(PauseRun {
            run_id: run_of(u)?,
            reason: u.int_in_range(0..=3)?,
            detail: String::new(),
            token: token(u)?,
            requested_at_ms: ms(u)?,
        }),
        12 => Command::resume_run(ResumeRun {
            run_id: run_of(u)?,
            requested_at_ms: ms(u)?,
        }),
        _ => {
            let n = u.int_in_range(0..=48)?;
            return Ok(Step {
                env,
                bytes: u.bytes(n)?.to_vec(),
            });
        }
    };
    Ok(Step {
        env,
        bytes: encode_command(&cmd).to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_inputs() {
        apply_command(&[]);
        apply_command(&[0]);
        apply_command(&[0, 0, 2, 0, 0xff, 0xff]);
        apply_command(&[1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
        apply_command(&[1; 300]);
    }

    #[test]
    fn raw_round_trip() {
        let steps = vec![
            Step {
                env: 3,
                bytes: vec![1, 2, 3],
            },
            Step {
                env: 6,
                bytes: Vec::new(),
            },
        ];
        let back = raw(&to_raw(&steps)[1..]);
        assert_eq!(back.len(), 2);
        assert_eq!(
            (back[0].env, back[0].bytes.as_slice()),
            (3, &[1u8, 2, 3][..])
        );
        assert_eq!((back[1].env, back[1].bytes.len()), (6, 0));
    }
}
