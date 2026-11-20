//! Partition command set (plan 02-06): round trips, golden encodings, malformed input,
//! forward compatibility and outbox routing.
//!
//! `tests/golden/commands.txt` freezes the command encodings. Regenerate ONLY for an additive
//! change made before the Phase 5 freeze, with `OSTRO_BLESS=1 cargo test -p ostro-types --test command`.

use bytes::Bytes;
use ostro_types::command::{
    self, AttemptGranted, AttemptTokenMsg, CancelTimer, CancelWait, ChildCompleted, CompleteRun,
    CompleteToken, ConflictPolicy, CronStart, Derived, EnqueueReady, EventsAppended, InboundBatch,
    InboundMsg, JournalEvent, OutboxMsg, OutboxTruncated, ParentRef, RegisterWait, RequestCancel,
    RetryRun, ScheduleTimer, SendMessage, Signal, SignalRun, StartChild, StartRun, Suspend,
    TerminalStatus, TerminateRun, TimerFired, WaitKind, decode_command, encode_command,
};
use ostro_types::command::{
    MAX_REQUEST_ID_LEN, OverlapPolicy, ScheduleFire, ScheduleFireKind, cron_idempotency_key,
    cron_manual_idempotency_key,
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
        // Phase 4 fields (14..18) left at their defaults: the original golden bytes must not move.
        ..Default::default()
    }
}

/// `sample_start` plus every Phase 4 field (plan 04-09: StartRun tags 14..18).
fn sample_start_v4(p: u16) -> StartRun {
    StartRun {
        module_sha: vec![0xC3; 32],
        tags: vec!["env:prod".into(), "team:billing".into()],
        tenant: "acme".into(),
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
        run_timeout_ms: 3_600_000,
        ..sample_start(p)
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
                event: None,
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
                resumable: false,
                parent_event: None,
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
                after_failure: false,
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

/// Vectors appended after the original seven (additive changes only; never edit an earlier line).
/// Plan 04-09: StartRun fields 14..18 and the `Suspend` intent (Derived oneof tag 6).
fn golden_appended() -> Vec<(&'static str, Command)> {
    vec![
        ("start_run_v4", Command::start_run(sample_start_v4(5))),
        (
            "events_appended_suspend",
            Command::events_appended(EventsAppended {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                token: token(7, 2),
                from_seq: 13,
                events: vec![JournalEvent {
                    seq: 13,
                    kind: 20,
                    payload: Some(PayloadRef::inline(Bytes::from_static(b"wait"))),
                }],
                derived: vec![Derived::suspend()],
            }),
        ),
    ]
}

/// `sample_start_v4` plus every Phase 5 field (plan 05-01: StartRun fields 19..23).
fn sample_start_v5(p: u16) -> StartRun {
    StartRun {
        schedule_to_start_ms: 60_000,
        retention_ms: 604_800_000,
        retry_of: run(p, 0x77).as_bytes().to_vec(),
        continued_from: run(p, 0x78).as_bytes().to_vec(),
        run_started: Some(JournalEvent {
            seq: 0,
            kind: 1,
            payload: Some(PayloadRef::inline(Bytes::from_static(b"run-started"))),
        }),
        ..sample_start_v4(p)
    }
}

/// An `EventsAppended` (seq 14) whose only intent is `CompleteRun` with `status`.
fn complete_with(status: TerminalStatus, resumable: bool, parent_event: bool) -> Command {
    Command::events_appended(EventsAppended {
        run_id: run(5, 0x11).as_bytes().to_vec(),
        token: token(7, 2),
        from_seq: 14,
        events: vec![],
        derived: vec![Derived::new(command::derived::Intent::CompleteRun(
            CompleteRun {
                status: status as i32,
                result: None,
                completed_at_ms: 8_000,
                resumable,
                parent_event: parent_event
                    .then(|| PayloadRef::inline(Bytes::from_static(b"child-completed-event"))),
            },
        ))],
    })
}

/// Vectors appended by plan 05-01 (Phase 5 contract; D1-D4, D9, D10, D13, D21, F9, PF-F1).
fn golden_appended_v5() -> Vec<(&'static str, Command)> {
    vec![
        (
            "signal_run",
            Command::signal_run(SignalRun {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                name: "approve".into(),
                event: Some(PayloadRef::inline(Bytes::from_static(
                    b"signal-payload-event",
                ))),
                request_id: b"req-1".to_vec(),
                sent_at_ms: 9_000,
            }),
        ),
        (
            "request_cancel",
            Command::request_cancel(RequestCancel {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                reason: "user".into(),
                requested_at_ms: 9_001,
            }),
        ),
        (
            "terminate_run",
            Command::terminate_run(TerminateRun {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                reason: "ops".into(),
                terminated_at_ms: 9_002,
            }),
        ),
        (
            "retry_run",
            Command::retry_run(RetryRun {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                requested_at_ms: 9_003,
            }),
        ),
        (
            "complete_token",
            Command::complete_token(CompleteToken {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                op: 8,
                event: Some(PayloadRef::blob([0x6B; 32], 1_048_576)),
                completed_at_ms: 9_004,
            }),
        ),
        ("start_run_v5", Command::start_run(sample_start_v5(5))),
        (
            "events_appended_waits",
            Command::events_appended(EventsAppended {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                token: token(7, 2),
                from_seq: 14,
                events: vec![JournalEvent {
                    seq: 14,
                    kind: 19,
                    payload: Some(PayloadRef::inline(Bytes::from_static(b"wait-signal"))),
                }],
                derived: vec![
                    Derived::register_wait(6, WaitKind::Signal, "approve", 70_000, 10_000),
                    Derived::register_wait(7, WaitKind::Token, "", 0, 10_000),
                    Derived::cancel_wait(5),
                ],
            }),
        ),
        (
            "complete_run_terminated_resumable",
            complete_with(TerminalStatus::Terminated, true, true),
        ),
        (
            "complete_run_timed_out",
            complete_with(TerminalStatus::TimedOut, true, false),
        ),
        (
            "complete_run_continued_as_new",
            complete_with(TerminalStatus::ContinuedAsNew, false, false),
        ),
        (
            "attempt_granted_after_failure",
            Command::attempt_granted(AttemptGranted {
                run_id: run(5, 0x11).as_bytes().to_vec(),
                token: token(7, 3),
                after_failure: true,
            }),
        ),
        (
            "inbound_batch_v5",
            Command::inbound_batch(InboundBatch {
                src: 2,
                msgs: vec![
                    InboundMsg {
                        seq: 6,
                        msg: Some(OutboxMsg::start_run(sample_start_v5(5))),
                    },
                    InboundMsg {
                        seq: 7,
                        msg: Some(OutboxMsg {
                            kind: Some(command::outbox_msg::Kind::ChildCompleted(ChildCompleted {
                                parent_run: run(5, 0x33).as_bytes().to_vec(),
                                child_run: run(9, 0x44).as_bytes().to_vec(),
                                op: 7,
                                status: TerminalStatus::TimedOut as i32,
                                result: None,
                                event: Some(PayloadRef::inline(Bytes::from_static(
                                    b"child-completed-event",
                                ))),
                            })),
                        }),
                    },
                ],
            }),
        ),
    ]
}

/// A cron-started `StartRun` (Phase 8 vectors): `sample_start_v5` with the cron idempotency key.
fn cron_start(key: &str) -> StartRun {
    StartRun {
        idempotency_key: key.as_bytes().to_vec(),
        tags: vec!["ostro.schedule=nightly".into()],
        ..sample_start_v5(5)
    }
}

/// Vectors appended by plan 08-01 (Phase 8 contract: `Cmd::ScheduleFire`, tag 27, P8-D10).
/// Appended after every Phase 5 line; the older lines are byte-identical.
fn golden_appended_v8() -> Vec<(&'static str, Command)> {
    vec![
        (
            "schedule_fire_fire",
            Command::schedule_fire(ScheduleFire {
                schedule: "nightly".into(),
                fire_time_ms: 1_767_225_600_000,
                kind: ScheduleFireKind::Fire as i32,
                start: Some(cron_start(&cron_idempotency_key(
                    "nightly",
                    1_767_225_600_000,
                ))),
                overlap: OverlapPolicy::BufferOne as u32,
                request_id: vec![],
                skipped: 2,
                schedule_created_at_ms: 1_767_000_000_000,
            }),
        ),
        (
            "schedule_fire_manual",
            Command::schedule_fire(ScheduleFire {
                schedule: "nightly".into(),
                fire_time_ms: 1_767_230_000_000,
                kind: ScheduleFireKind::Manual as i32,
                start: Some(cron_start(&cron_manual_idempotency_key(
                    "nightly", b"req-1",
                ))),
                overlap: OverlapPolicy::AllowAll as u32,
                request_id: b"req-1".to_vec(),
                skipped: 0,
                schedule_created_at_ms: 1_767_000_000_000,
            }),
        ),
        (
            "schedule_fire_advance",
            Command::schedule_fire(ScheduleFire {
                schedule: "nightly".into(),
                fire_time_ms: 1_767_312_000_000,
                kind: ScheduleFireKind::Advance as i32,
                start: None,
                overlap: OverlapPolicy::TerminateOther as u32,
                request_id: vec![],
                skipped: 5,
                schedule_created_at_ms: 1_767_000_000_000,
            }),
        ),
    ]
}

/// Every golden vector in file order: the original seven, then the appended ones.
fn all_golden() -> Vec<(&'static str, Command)> {
    let mut v = golden_commands();
    v.extend(golden_appended());
    v.extend(golden_appended_v5());
    v.extend(golden_appended_v8());
    v
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn golden_line(name: &str) -> Vec<u8> {
    let line = GOLDEN
        .lines()
        .find_map(|l| l.strip_prefix(name).and_then(|r| r.strip_prefix('\t')))
        .unwrap_or_else(|| panic!("golden line {name}"));
    unhex(line)
}

fn render_golden() -> String {
    let mut s = String::from(HEADER);
    s.push('\n');
    for (name, c) in all_golden() {
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
    let expected: Vec<(&str, String)> = all_golden()
        .into_iter()
        .map(|(n, c)| (n, hex(&encode_command(&c))))
        .collect();
    let got: Vec<(&str, &str)> = lines
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split_once('\t').expect("name<TAB>hex"))
        .collect();
    assert_eq!(
        got.len(),
        expected.len(),
        "one vector per Cmd variant + appended vectors"
    );
    for ((gn, gh), (en, eh)) in got.iter().zip(&expected) {
        assert_eq!(gn, en);
        assert_eq!(*gh, eh.as_str(), "{en} encoding drifted");
    }
}

#[test]
fn appended_vectors_round_trip() {
    for (name, c) in golden_appended() {
        let back = decode_command(&encode_command(&c)).expect("decode");
        assert_eq!(back, c, "{name} round trip");
    }
    let Some(command::Cmd::StartRun(s)) = golden_appended().remove(0).1.cmd else {
        panic!("wrong variant")
    };
    assert_eq!(s.module_sha.len(), command::MODULE_SHA_LEN);
    assert_eq!(s.tags, ["env:prod", "team:billing"]);
    assert_eq!(s.tenant, "acme");
    assert_eq!(s.run_timeout_ms, 3_600_000);
}

#[test]
fn old_start_run_bytes_decode_with_defaults() {
    // The StartRun line written by plan 02-06, before fields 14..18 existed.
    let old = golden_line("StartRun");
    let c = decode_command(&old).expect("old bytes decode");
    let Some(command::Cmd::StartRun(s)) = &c.cmd else {
        panic!("wrong variant")
    };
    assert!(s.module_sha.is_empty());
    assert!(s.tags.is_empty());
    assert!(s.tenant.is_empty());
    assert!(s.traceparent.is_empty());
    assert_eq!(s.run_timeout_ms, 0, "0 = no run timeout");
    assert_eq!(*s, sample_start(5));
    assert_eq!(
        encode_command(&c).as_ref(),
        old.as_slice(),
        "re-encode is identical"
    );
}

#[test]
fn old_events_appended_bytes_decode_unchanged() {
    let old = golden_line("EventsAppended");
    let c = decode_command(&old).expect("old bytes decode");
    assert_eq!(encode_command(&c).as_ref(), old.as_slice());
    let Some(command::Cmd::EventsAppended(ea)) = c.cmd else {
        panic!("wrong variant")
    };
    assert!(
        ea.derived
            .iter()
            .all(|d| !matches!(d.intent, Some(command::derived::Intent::Suspend(_))))
    );
}

#[test]
fn start_run_new_fields_bytes_from_first_principles() {
    // Each new field alone, wrapped in Cmd tag 2 (LEN): key = field << 3 | wire type.
    let only = |s: StartRun| hex(&encode_command(&Command::start_run(s)));
    // 14 bytes: key 0x72, len 2.
    assert_eq!(
        only(StartRun {
            module_sha: vec![0xAA, 0xBB],
            ..Default::default()
        }),
        "1204".to_string() + "7202aabb"
    );
    // 15 repeated string: key 0x7a per element.
    assert_eq!(
        only(StartRun {
            tags: vec!["a".into(), "b".into()],
            ..Default::default()
        }),
        "1206".to_string() + "7a0161" + "7a0162"
    );
    // 16 string: key 0x82 0x01.
    assert_eq!(
        only(StartRun {
            tenant: "t".into(),
            ..Default::default()
        }),
        "1204".to_string() + "820101" + "74"
    );
    // 17 string: key 0x8a 0x01.
    assert_eq!(
        only(StartRun {
            traceparent: "x".into(),
            ..Default::default()
        }),
        "1204".to_string() + "8a0101" + "78"
    );
    // 18 varint: key 0x90 0x01.
    assert_eq!(
        only(StartRun {
            run_timeout_ms: 1,
            ..Default::default()
        }),
        "1203".to_string() + "900101"
    );
}

#[test]
fn suspend_intent_bytes_from_first_principles() {
    let d = Derived::suspend();
    assert_eq!(
        d.intent,
        Some(command::derived::Intent::Suspend(Suspend {}))
    );
    // Derived oneof tag 6 (LEN), empty Suspend message: 32 00.
    assert_eq!(hex(&prost::Message::encode_to_vec(&d)), "3200");
    let back: Derived = prost::Message::decode(&[0x32, 0x00][..]).unwrap();
    assert_eq!(back, d);
}

#[test]
fn start_run_field_limits_are_documented_constants() {
    assert_eq!(command::MODULE_SHA_LEN, 32);
    assert_eq!(command::MAX_RUN_TAGS, 64);
    assert_eq!(command::MAX_RUN_TAG_LEN, 128);
    assert_eq!(command::MAX_TENANT_LEN, 128);
    assert_eq!(command::MAX_TRACEPARENT_LEN, 256);
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
    for (name, c) in all_golden() {
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
    fn decode_mutated_valid_never_panics(idx in 0usize..9, pos in any::<usize>(), byte in any::<u8>()) {
        let (_, c) = all_golden().remove(idx);
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
            Kind::StartRun(s) => s.run_id = bad.clone(),
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

// ---- Plan 05-01: Phase 5 contract ---------------------------------------------------------

#[test]
fn phase5_vectors_round_trip() {
    for (name, c) in golden_appended_v5() {
        let back = decode_command(&encode_command(&c)).expect("decode");
        assert_eq!(back, c, "{name} round trip");
    }
    let names: Vec<&str> = golden_appended_v5()
        .iter()
        .map(|(_, c)| c.kind_name())
        .collect();
    for n in [
        "SignalRun",
        "RequestCancel",
        "TerminateRun",
        "RetryRun",
        "CompleteToken",
    ] {
        assert!(names.contains(&n), "{n} has a golden vector");
    }
}

#[test]
fn phase5_cmd_tags_from_first_principles() {
    // Cmd oneof key = tag << 3 | 2 (LEN); an empty message encodes as key 00.
    let empty = |c: Command| hex(&encode_command(&c));
    assert_eq!(empty(Command::signal_run(SignalRun::default())), "4200");
    assert_eq!(
        empty(Command::request_cancel(RequestCancel::default())),
        "4a00"
    );
    assert_eq!(
        empty(Command::terminate_run(TerminateRun::default())),
        "5200"
    );
    assert_eq!(empty(Command::retry_run(RetryRun::default())), "5a00");
    assert_eq!(
        empty(Command::complete_token(CompleteToken::default())),
        "6200"
    );
}

#[test]
fn phase5_start_run_fields_from_first_principles() {
    let only = |s: StartRun| hex(&encode_command(&Command::start_run(s)));
    // 19 varint: key 0x98 0x01.
    assert_eq!(
        only(StartRun {
            schedule_to_start_ms: 1,
            ..Default::default()
        }),
        "1203".to_string() + "980101"
    );
    // 20 varint: key 0xa0 0x01.
    assert_eq!(
        only(StartRun {
            retention_ms: 1,
            ..Default::default()
        }),
        "1203".to_string() + "a00101"
    );
    // 21 bytes: key 0xaa 0x01.
    assert_eq!(
        only(StartRun {
            retry_of: vec![0xAA],
            ..Default::default()
        }),
        "1204".to_string() + "aa0101aa"
    );
    // 22 bytes: key 0xb2 0x01.
    assert_eq!(
        only(StartRun {
            continued_from: vec![0xBB],
            ..Default::default()
        }),
        "1204".to_string() + "b20101bb"
    );
    // 23 message: key 0xba 0x01, empty JournalEvent.
    assert_eq!(
        only(StartRun {
            run_started: Some(JournalEvent::default()),
            ..Default::default()
        }),
        "1203".to_string() + "ba0100"
    );
}

#[test]
fn phase5_intent_and_field_tags_from_first_principles() {
    use prost::Message as _;
    // Derived oneof tag 7 / 8 (LEN).
    assert_eq!(
        hex(&Derived::new(command::derived::Intent::RegisterWait(
            RegisterWait::default()
        ))
        .encode_to_vec()),
        "3a00"
    );
    assert_eq!(hex(&Derived::cancel_wait(0).encode_to_vec()), "4200");
    // RegisterWait.at_ms = field 5 varint (key 0x28); WaitKind::Token = field 2 value 1.
    let rw = RegisterWait {
        kind: WaitKind::Token as i32,
        at_ms: 1,
        ..Default::default()
    };
    assert_eq!(hex(&rw.encode_to_vec()), "10012801");
    // CompleteRun.resumable = field 4 bool (key 0x20); parent_event = field 5 LEN (key 0x2a).
    let cr = CompleteRun {
        resumable: true,
        parent_event: Some(PayloadRef::default()),
        ..Default::default()
    };
    assert_eq!(hex(&cr.encode_to_vec()), "20012a00");
    // ChildCompleted.event = field 6 LEN (key 0x32).
    let cc = ChildCompleted {
        event: Some(PayloadRef::default()),
        ..Default::default()
    };
    assert_eq!(hex(&cc.encode_to_vec()), "3200");
    // AttemptGranted.after_failure = field 3 bool (key 0x18).
    let ag = AttemptGranted {
        after_failure: true,
        ..Default::default()
    };
    assert_eq!(hex(&ag.encode_to_vec()), "1801");
    // Outbox Kind::StartRun = tag 6 (LEN, key 0x32).
    assert_eq!(
        hex(&OutboxMsg::start_run(StartRun::default()).encode_to_vec()),
        "3200"
    );
    // TerminalStatus values.
    assert_eq!(TerminalStatus::Terminated as i32, 3);
    assert_eq!(TerminalStatus::TimedOut as i32, 4);
    assert_eq!(TerminalStatus::ContinuedAsNew as i32, 5);
}

#[test]
fn old_bytes_decode_with_phase5_defaults() {
    // StartRun written by 02-06 and by 04-09: the Phase 5 fields default and re-encode
    // byte-identically.
    for (line, expected) in [
        ("StartRun", sample_start(5)),
        ("start_run_v4", sample_start_v4(5)),
    ] {
        let old = golden_line(line);
        let c = decode_command(&old).expect("old bytes decode");
        let Some(command::Cmd::StartRun(s)) = &c.cmd else {
            panic!("wrong variant")
        };
        assert_eq!(s.schedule_to_start_ms, 0);
        assert_eq!(s.retention_ms, 0);
        assert!(s.retry_of.is_empty());
        assert!(s.continued_from.is_empty());
        assert!(s.run_started.is_none());
        assert_eq!(*s, expected);
        assert_eq!(encode_command(&c).as_ref(), old.as_slice(), "{line}");
    }
    // AttemptGranted / CompleteRun / ChildCompleted written before Phase 5.
    for line in ["AttemptGranted", "EventsAppended", "InboundBatch"] {
        let old = golden_line(line);
        let c = decode_command(&old).expect("old bytes decode");
        assert_eq!(encode_command(&c).as_ref(), old.as_slice(), "{line}");
    }
    let Some(command::Cmd::AttemptGranted(g)) =
        decode_command(&golden_line("AttemptGranted")).unwrap().cmd
    else {
        panic!("wrong variant")
    };
    assert!(!g.after_failure);
}

#[test]
fn reserved_ops_are_pinned() {
    assert_eq!(command::RESERVED_OP_MIN, u64::MAX - 15);
    assert_eq!(command::START_DELAY_OP, u64::MAX);
    assert_eq!(command::RUN_TIMEOUT_OP, u64::MAX - 1);
    assert_eq!(command::SCHEDULE_TO_START_OP, u64::MAX - 2);
    assert_eq!(command::EXPIRE_OP, u64::MAX - 3);
    for op in [
        command::START_DELAY_OP,
        command::RUN_TIMEOUT_OP,
        command::SCHEDULE_TO_START_OP,
        command::EXPIRE_OP,
        u64::MAX - 15,
    ] {
        assert!(command::is_reserved_op(op), "{op}");
    }
    assert!(!command::is_reserved_op(u64::MAX - 16));
    assert!(!command::is_reserved_op(0));
}

#[test]
fn phase5_field_limits() {
    assert_eq!(command::MAX_SIGNAL_NAME_LEN, 256);
    assert_eq!(command::MAX_REASON_LEN, 1024);
    assert_eq!(command::MAX_REQUEST_ID_LEN, 64);
    let mut s = SignalRun {
        name: "n".repeat(256),
        request_id: vec![1; 64],
        ..Default::default()
    };
    assert!(s.check_limits().is_ok());
    s.name.push('x');
    assert_eq!(s.check_limits().unwrap_err().field, "name");
    s.name.pop();
    s.request_id.push(1);
    assert_eq!(s.check_limits().unwrap_err().field, "request_id");
    let mut rc = RequestCancel {
        reason: "r".repeat(1024),
        ..Default::default()
    };
    assert!(rc.check_limits().is_ok());
    rc.reason.push('r');
    assert!(rc.check_limits().is_err());
    let t = TerminateRun {
        reason: "r".repeat(1025),
        ..Default::default()
    };
    assert!(t.check_limits().is_err());
    let rw = RegisterWait {
        name: "n".repeat(257),
        ..Default::default()
    };
    assert!(rw.check_limits().is_err());
}

#[test]
fn phase5_run_id_helpers_and_routing() {
    let id = run(6, 0x99);
    let b = id.as_bytes().to_vec();
    assert_eq!(
        SignalRun {
            run_id: b.clone(),
            ..Default::default()
        }
        .run_id(),
        Ok(id)
    );
    assert_eq!(
        RequestCancel {
            run_id: b.clone(),
            ..Default::default()
        }
        .run_id(),
        Ok(id)
    );
    assert_eq!(
        TerminateRun {
            run_id: b.clone(),
            ..Default::default()
        }
        .run_id(),
        Ok(id)
    );
    assert_eq!(
        RetryRun {
            run_id: b.clone(),
            ..Default::default()
        }
        .run_id(),
        Ok(id)
    );
    assert_eq!(
        CompleteToken {
            run_id: vec![1],
            ..Default::default()
        }
        .run_id(),
        Err(IdError::BadLength(1))
    );
    // Outbox Kind::StartRun routes by the new run's id.
    for p in 0..16u16 {
        let m = OutboxMsg::start_run(sample_start_v5(p));
        assert_eq!(m.target_partition(), Ok(PartitionId(p)));
    }
    let _ = CancelWait::default();
}

// ---- Plan 08-01: Phase 8 contract ---------------------------------------------------------

#[test]
fn phase8_vectors_round_trip() {
    let v = golden_appended_v8();
    assert_eq!(v.len(), 3, "one Fire, one Manual, one Advance");
    for (name, c) in &v {
        let back = decode_command(&encode_command(c)).expect("decode");
        assert_eq!(&back, c, "{name} round trip");
        assert_eq!(c.kind_name(), "ScheduleFire");
    }
    let kinds: Vec<ScheduleFireKind> = v
        .iter()
        .map(|(_, c)| match &c.cmd {
            Some(command::Cmd::ScheduleFire(s)) => s.kind(),
            _ => panic!("not a ScheduleFire"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            ScheduleFireKind::Fire,
            ScheduleFireKind::Manual,
            ScheduleFireKind::Advance
        ]
    );
}

#[test]
fn phase8_schedule_fire_tag_and_fields_from_first_principles() {
    // Cmd tag 27 (RESERVATIONS P8 = 27..33): key = 27 << 3 | 2 = 218 = varint `da 01`.
    assert_eq!(
        hex(&encode_command(&Command::schedule_fire(
            ScheduleFire::default()
        ))),
        "da0100"
    );
    // Field numbers 1..=8: one byte per key (field << 3 | wire type).
    let s = ScheduleFire {
        schedule: "s".into(),                  // 1, LEN: 0a 01 73
        fire_time_ms: 1,                       // 2, varint: 10 01
        kind: ScheduleFireKind::Manual as i32, // 3, varint: 18 01
        start: Some(StartRun::default()),      // 4, LEN: 22 00
        overlap: 4,                            // 5, varint: 28 04
        request_id: b"r".to_vec(),             // 6, LEN: 32 01 72
        skipped: 3,                            // 7, varint: 38 03
        schedule_created_at_ms: 2,             // 8, varint: 40 02
    };
    assert_eq!(
        hex(&encode_command(&Command::schedule_fire(s))),
        "da01120a0173100118012200280432017238034002"
    );
}

#[test]
fn phase8_golden_lines_are_appended_last() {
    // The Phase 5 lines keep their positions; the three Phase 8 lines follow them.
    let names: Vec<&str> = GOLDEN
        .lines()
        .skip(1)
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.split_once('\t').expect("name<TAB>hex").0)
        .collect();
    assert_eq!(names.len(), 24);
    assert_eq!(names[20], "inbound_batch_v5");
    assert_eq!(
        &names[21..],
        [
            "schedule_fire_fire",
            "schedule_fire_manual",
            "schedule_fire_advance"
        ]
    );
}

#[test]
fn phase8_overlap_mapping_matches_meta() {
    // Same numbers as ostro_meta::records::OVERLAP_* (plan 05-07).
    for (v, p) in [
        (0, OverlapPolicy::Skip),
        (1, OverlapPolicy::BufferOne),
        (2, OverlapPolicy::AllowAll),
        (3, OverlapPolicy::CancelOther),
        (4, OverlapPolicy::TerminateOther),
    ] {
        assert_eq!(OverlapPolicy::from_u32(v), Some(p));
        assert_eq!(p as u32, v);
    }
    assert_eq!(OverlapPolicy::from_u32(5), None);
    let s = ScheduleFire {
        overlap: 9,
        ..ScheduleFire::default()
    };
    assert_eq!(s.overlap_policy(), None);
}

#[test]
fn phase8_cron_idempotency_keys_and_limits() {
    assert_eq!(
        cron_idempotency_key("nightly", 1_000),
        "__cron/nightly/1000"
    );
    assert_eq!(
        cron_manual_idempotency_key("nightly", &[0x00, 0xab, 0x7f]),
        "__cron/nightly/manual/00ab7f"
    );
    assert_eq!(cron_manual_idempotency_key("n", b""), "__cron/n/manual/");
    let mut s = ScheduleFire {
        request_id: vec![1; MAX_REQUEST_ID_LEN],
        ..ScheduleFire::default()
    };
    assert!(s.check_limits().is_ok());
    s.request_id.push(1);
    assert!(s.check_limits().is_err());
}
