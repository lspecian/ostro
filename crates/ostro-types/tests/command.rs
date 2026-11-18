//! Partition command set (plan 02-06): round trips, golden encodings, malformed input,
//! forward compatibility and outbox routing.
//!
//! `tests/golden/commands.txt` freezes the command encodings. Regenerate ONLY for an additive
//! change made before the Phase 5 freeze, with `OSTRO_BLESS=1 cargo test -p ostro-types --test command`.

use bytes::Bytes;
use ostro_types::command::{
    self, AttemptGranted, AttemptTokenMsg, CancelTimer, ChildCompleted, CompleteRun,
    ConflictPolicy, CronStart, Derived, EnqueueReady, EventsAppended, InboundBatch, InboundMsg,
    JournalEvent, OutboxMsg, OutboxTruncated, ParentRef, ScheduleTimer, SendMessage, Signal,
    StartChild, StartRun, TerminalStatus, TimerFired, decode_command, encode_command,
};
use ostro_types::{AttemptToken, Command, Epoch, IdError, PartitionId, PayloadRef, RunId};
use proptest::prelude::*;

const GOLDEN: &str = include_str!("golden/commands.txt");
const HEADER: &str = "# ostro partition command golden vectors — name<TAB>hex — DO NOT REGENERATE (frozen at Phase 5)";

fn run(p: u16, tail: u8) -> RunId {
    RunId::new(PartitionId(p), uuid::Uuid::from_bytes([tail; 16]))
}

fn token(epoch: u32, n: u32) -> Option<AttemptTokenMsg> {
    Some(
        AttemptToken {
            epoch: Epoch(epoch),
            n,
        }
        .into(),
    )
}

fn sample_start(p: u16) -> StartRun {
    StartRun {
        run_id: run(p, 0x11).as_bytes().to_vec(),
        workflow: "order".into(),
        workflow_version: 3,
        queue: "default".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"{\"id\":1}"))),
        idempotency_key: b"idem-1".to_vec(),
        conflict_policy: ConflictPolicy::Reject as i32,
        concurrency_key: "cust-9".into(),
        priority: -2,
        ready_at_ms: 1_000,
        requested_at_ms: 999,
        parent: Some(ParentRef {
            run_id: run(1, 0x22).as_bytes().to_vec(),
            op: 4,
        }),
        rng_seed: vec![0xAB; 32],
    }
}

fn outbox_all(p: u16) -> Vec<OutboxMsg> {
    vec![
        OutboxMsg::start_child(sample_start(p)),
        OutboxMsg {
            kind: Some(command::outbox_msg::Kind::ChildCompleted(ChildCompleted {
                parent_run: run(p, 0x33).as_bytes().to_vec(),
                child_run: run(9, 0x44).as_bytes().to_vec(),
                op: 7,
                status: TerminalStatus::Failed as i32,
                result: Some(PayloadRef::blob([0x5A; 32], 300_000)),
            })),
        },
        OutboxMsg {
            kind: Some(command::outbox_msg::Kind::Signal(Signal {
                target_run: run(p, 0x55).as_bytes().to_vec(),
                name: "approve".into(),
                payload: Some(PayloadRef::inline(Bytes::from_static(b"yes"))),
            })),
        },
        OutboxMsg {
            kind: Some(command::outbox_msg::Kind::CancelRun(command::CancelRun {
                target_run: run(p, 0x66).as_bytes().to_vec(),
                reason: "user".into(),
            })),
        },
        OutboxMsg {
            kind: Some(command::outbox_msg::Kind::CronStart(CronStart {
                schedule_id: "nightly".into(),
                fire_time_ms: 86_400_000,
                start: Some(sample_start(p)),
            })),
        },
    ]
}

fn derived_all() -> Vec<Derived> {
    use command::derived::Intent;
    vec![
        Derived {
            intent: Some(Intent::ScheduleTimer(ScheduleTimer {
                op: 2,
                due_at_ms: 5_000,
            })),
        },
        Derived {
            intent: Some(Intent::CancelTimer(CancelTimer {
                op: 2,
                due_at_ms: 5_000,
            })),
        },
        Derived {
            intent: Some(Intent::SendMessage(SendMessage {
                msg: Some(outbox_all(3).remove(2)),
            })),
        },
        Derived {
            intent: Some(Intent::EnqueueReady(EnqueueReady { ready_at_ms: 6_000 })),
        },
        Derived {
            intent: Some(Intent::CompleteRun(CompleteRun {
                status: TerminalStatus::Completed as i32,
                result: Some(PayloadRef::inline(Bytes::from_static(b"done"))),
                completed_at_ms: 7_000,
            })),
        },
    ]
}

/// One fixed instance per `Cmd` variant (the golden set).
fn golden_commands() -> Vec<(&'static str, Command)> {
    vec![
        ("LeaderAnnounced", Command::leader_announced(7, 1, 1_000)),
        ("StartRun", Command::start_run(sample_start(5))),
        (
            "AttemptGranted",
            Command::attempt_granted(AttemptGranted {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                token: token(7, 2),
            }),
        ),
        (
            "EventsAppended",
            Command::events_appended(EventsAppended {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                token: token(7, 2),
                from_seq: 10,
                events: (10..13)
                    .map(|seq| JournalEvent {
                        seq,
                        kind: 16 + seq as u32,
                        payload: Some(PayloadRef::inline(Bytes::from(vec![seq as u8; 3]))),
                    })
                    .collect(),
                derived: derived_all(),
            }),
        ),
        (
            "TimerFired",
            Command::timer_fired(TimerFired {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                op: 2,
                due_at_ms: 5_000,
                fired_at_ms: 5_003,
            }),
        ),
        (
            "InboundBatch",
            Command::inbound_batch(InboundBatch {
                src: 2,
                msgs: outbox_all(5)
                    .into_iter()
                    .enumerate()
                    .map(|(i, m)| InboundMsg {
                        seq: i as u64 + 1,
                        msg: Some(m),
                    })
                    .collect(),
            }),
        ),
        (
            "OutboxTruncated",
            Command::outbox_truncated(OutboxTruncated {
                target: 9,
                upto_seq: 42,
            }),
        ),
    ]
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn render_golden() -> String {
    let mut s = String::from(HEADER);
    s.push('\n');
    for (name, c) in golden_commands() {
        s.push_str(&format!("{name}\t{}\n", hex(&encode_command(&c))));
    }
    s
}

#[test]
fn round_trip_every_variant() {
    let cmds = golden_commands();
    assert_eq!(cmds.len(), 7);
    for (name, c) in cmds {
        assert_eq!(c.kind_name(), name);
        let bytes = encode_command(&c);
        let back = decode_command(&bytes).expect("decode");
        assert_eq!(back, c, "{name} round trip");
    }
}

#[test]
fn nested_fields_survive_round_trip() {
    let (_, c) = golden_commands().remove(3);
    let back = decode_command(&encode_command(&c)).unwrap();
    let Some(command::Cmd::EventsAppended(ea)) = back.cmd else {
        panic!("wrong variant")
    };
    assert_eq!(ea.events.len(), 3);
    assert_eq!(ea.derived.len(), 5);
    let (_, c) = golden_commands().remove(5);
    let Some(command::Cmd::InboundBatch(ib)) = decode_command(&encode_command(&c)).unwrap().cmd
    else {
        panic!("wrong variant")
    };
    assert_eq!(ib.msgs.len(), 5);
    let Some(command::outbox_msg::Kind::StartChild(sc)) = &ib.msgs[0].msg.as_ref().unwrap().kind
    else {
        panic!("wrong outbox kind")
    };
    let start = sc.start.as_ref().unwrap();
    assert_eq!(start.parent.as_ref().unwrap().op, 4);
    assert_eq!(start.input.as_ref().unwrap().inline_len(), Some(8));
    assert_eq!(start.conflict_policy(), ConflictPolicy::Reject);
}

#[test]
fn golden_encodings() {
    if std::env::var_os("OSTRO_BLESS").is_some() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/commands.txt");
        std::fs::write(path, render_golden()).expect("write golden");
    }
    let mut lines = GOLDEN.lines();
    assert_eq!(lines.next(), Some(HEADER));
    let expected: Vec<(&str, String)> = golden_commands()
        .into_iter()
        .map(|(n, c)| (n, hex(&encode_command(&c))))
        .collect();
    let got: Vec<(&str, &str)> = lines
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split_once('\t').expect("name<TAB>hex"))
        .collect();
    assert_eq!(got.len(), expected.len(), "one vector per Cmd variant");
    for ((gn, gh), (en, eh)) in got.iter().zip(&expected) {
        assert_eq!(gn, en);
        assert_eq!(*gh, eh.as_str(), "{en} encoding drifted");
    }
}

#[test]
fn leader_announced_bytes_from_first_principles() {
    // Independent of the golden file: tag 1 (LEN) { 08 07 | 10 01 | 18 e8 07 }.
    let b = encode_command(&Command::leader_announced(7, 1, 1_000));
    assert_eq!(hex(&b), "0a0708071001 18e807".replace(' ', ""));
}

#[test]
fn empty_slice_decodes_to_none() {
    let c = decode_command(&[]).expect("empty is a valid message");
    assert_eq!(c.cmd, None);
    assert_eq!(c.kind_name(), "None");
}

#[test]
fn unknown_future_field_is_skipped() {
    for (name, c) in golden_commands() {
        let mut b = encode_command(&c).to_vec();
        // tag 60, wire type 0 (varint): key = 60 << 3 = 480 -> e0 03; value 5.
        b.extend_from_slice(&[0xe0, 0x03, 0x05]);
        assert_eq!(decode_command(&b).expect("forward compatible"), c, "{name}");
    }
}

#[test]
fn malformed_bytes_are_errors() {
    // A length-delimited field whose length overruns the buffer.
    assert!(decode_command(&[0x0a, 0x10, 0x08]).is_err());
    // Truncated varint.
    assert!(decode_command(&[0x08, 0xff]).is_err());
}

#[test]
fn attempt_token_conversion_round_trips() {
    for (e, n) in [(0, 0), (1, 1), (u32::MAX, 7), (3, u32::MAX)] {
        let t = AttemptToken { epoch: Epoch(e), n };
        let m: AttemptTokenMsg = t.into();
        assert_eq!(m.epoch, e);
        assert_eq!(m.n, n);
        assert_eq!(AttemptToken::from(m), t);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]
    #[test]
    fn decode_random_bytes_never_panics(b in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode_command(&b);
    }

    #[test]
    fn decode_mutated_valid_never_panics(idx in 0usize..7, pos in any::<usize>(), byte in any::<u8>()) {
        let (_, c) = golden_commands().remove(idx);
        let mut b = encode_command(&c).to_vec();
        let p = pos % b.len();
        b[p] = byte;
        let _ = decode_command(&b);
        let _ = decode_command(&b[..p]);
    }
}

// ---- Task 2: routing + validation helpers -------------------------------------------------

#[test]
fn routing_target_partition_every_kind() {
    for p in 0..16u16 {
        for m in outbox_all(p) {
            assert_eq!(m.target_partition(), Ok(PartitionId(p)), "{m:?}");
        }
    }
}

#[test]
fn routing_child_completed_targets_parent_not_child() {
    let m = &outbox_all(4)[1];
    assert_eq!(m.target_partition(), Ok(PartitionId(4)));
}

#[test]
fn routing_malformed_run_id_is_error() {
    let bad = vec![0u8; 17];
    let mut msgs = outbox_all(2);
    for m in &mut msgs {
        use command::outbox_msg::Kind;
        match m.kind.as_mut().unwrap() {
            Kind::StartChild(s) => s.start.as_mut().unwrap().run_id = bad.clone(),
            Kind::ChildCompleted(c) => c.parent_run = bad.clone(),
            Kind::Signal(s) => s.target_run = bad.clone(),
            Kind::CancelRun(c) => c.target_run = bad.clone(),
            Kind::CronStart(c) => c.start.as_mut().unwrap().run_id = bad.clone(),
        }
        assert_eq!(m.target_partition(), Err(IdError::BadLength(17)), "{m:?}");
    }
    // Missing nested start / missing kind are errors too, never panics.
    assert!(OutboxMsg { kind: None }.target_partition().is_err());
    let no_start = OutboxMsg {
        kind: Some(command::outbox_msg::Kind::StartChild(StartChild {
            start: None,
        })),
    };
    assert!(no_start.target_partition().is_err());
}

#[test]
fn routing_start_run_id_length_checked() {
    let mut s = sample_start(5);
    assert_eq!(s.run_id(), Ok(run(5, 0x11)));
    for len in [0usize, 1, 17, 19, 36] {
        s.run_id = vec![0; len];
        assert_eq!(s.run_id(), Err(IdError::BadLength(len)));
    }
    let parent = sample_start(5).parent.unwrap();
    assert_eq!(parent.run_id(), Ok(run(1, 0x22)));
}

#[test]
fn routing_events_appended_helpers() {
    let mut ea = EventsAppended {
        run_id: run(3, 0x77).as_bytes().to_vec(),
        token: token(2, 5),
        from_seq: 4,
        events: (4..8)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: None,
            })
            .collect(),
        derived: vec![],
    };
    assert_eq!(ea.run_id(), Ok(run(3, 0x77)));
    assert_eq!(
        ea.token(),
        Ok(AttemptToken {
            epoch: Epoch(2),
            n: 5
        })
    );
    assert!(ea.events_contiguous_from(4));
    assert!(!ea.events_contiguous_from(3));
    assert!(!ea.events_contiguous_from(5));
    ea.events[2].seq = 9;
    assert!(!ea.events_contiguous_from(4));
    ea.events.clear();
    assert!(ea.events_contiguous_from(4));
    ea.events.push(JournalEvent {
        seq: u64::MAX,
        kind: 0,
        payload: None,
    });
    ea.events.push(JournalEvent {
        seq: 0,
        kind: 0,
        payload: None,
    });
    assert!(!ea.events_contiguous_from(u64::MAX), "no wrap-around");
    ea.token = None;
    assert!(ea.token().is_err());
    ea.run_id = vec![1, 2, 3];
    assert_eq!(ea.run_id(), Err(IdError::BadLength(3)));
}

#[test]
fn routing_timer_fired_run_id() {
    let mut t = TimerFired {
        run_id: run(8, 0x12).as_bytes().to_vec(),
        op: 1,
        due_at_ms: 2,
        fired_at_ms: 3,
    };
    assert_eq!(t.run_id(), Ok(run(8, 0x12)));
    t.run_id.pop();
    assert_eq!(t.run_id(), Err(IdError::BadLength(17)));
}
