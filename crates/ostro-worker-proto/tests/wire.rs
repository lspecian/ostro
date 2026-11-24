//! WireOutcome postcard round-trip for every `ActivationOutcome` variant (D-W19), and a
//! smoke check of the generated `ostro.worker.v1` types.

use ostro_engine::error::{AttemptFailure, EvictReason, NonDeterminism};
use ostro_engine::sink::ActivationOutcome;
use ostro_types::journal::{Continuation, ErrorInfo, Failure, Payload};
use ostro_worker_proto::{PROTO_VERSION, WireOutcome, v1};
use prost::Message;

fn payload(data: &[u8]) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: data.to_vec(),
    }
}

fn every_outcome() -> Vec<ActivationOutcome> {
    let mut v = vec![
        ActivationOutcome::Completed {
            output: payload(br#"{"ok":true}"#),
        },
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(ErrorInfo {
                kind: "bad".into(),
                message: "boom".into(),
                details: Some(payload(b"[1]")),
            }),
        },
        ActivationOutcome::Failed {
            failure: Failure::Retryable(ErrorInfo {
                kind: "flaky".into(),
                message: "again".into(),
                details: None,
            }),
        },
        ActivationOutcome::Failed {
            failure: Failure::Timeout,
        },
        ActivationOutcome::Failed {
            failure: Failure::Cancelled,
        },
        ActivationOutcome::ContinuedAsNew {
            continuation: Continuation {
                input: payload(b"{}"),
                workflow: Some("next".into()),
                version: Some(3),
            },
        },
        ActivationOutcome::ContinuedAsNew {
            continuation: Continuation {
                input: payload(b""),
                workflow: None,
                version: None,
            },
        },
        ActivationOutcome::Fenced,
    ];
    for reason in [
        EvictReason::WaitUnsatisfied,
        EvictReason::HotWaitElapsed,
        EvictReason::BudgetPressure,
        EvictReason::External,
        EvictReason::AppendConflict,
        EvictReason::StepBackoff {
            retry_at_nanos: 1_700_000_000_000_000_000,
        },
        EvictReason::StepInterrupted,
    ] {
        v.push(ActivationOutcome::Evicted { reason });
    }
    for f in [
        AttemptFailure::NonDeterminism(NonDeterminism {
            seq: 7,
            expected: "step a".into(),
            actual: "step b".into(),
        }),
        AttemptFailure::FuelExhausted,
        AttemptFailure::Timeout,
        AttemptFailure::MemoryExceeded,
        AttemptFailure::StackOverflow,
        AttemptFailure::GuestExit(3),
        AttemptFailure::ProtocolViolation("twice".into()),
        AttemptFailure::Trap("unreachable".into()),
        AttemptFailure::LimitExceeded {
            limit: "history_events",
            max: 20,
            actual: 21,
        },
        AttemptFailure::LimitExceeded {
            limit: "children_per_run",
            max: 3,
            actual: 4,
        },
        AttemptFailure::LimitExceeded {
            limit: "payload_bytes",
            max: 1_048_576,
            actual: 2_097_152,
        },
    ] {
        v.push(ActivationOutcome::AttemptFailed(f));
    }
    v
}

#[test]
fn every_variant_round_trips_through_postcard() {
    let all = every_outcome();
    assert_eq!(all.len(), 26);
    for o in all {
        let wire = WireOutcome::from(&o);
        let bytes = wire.encode();
        let back = WireOutcome::decode(&bytes).expect("decode");
        assert_eq!(back, wire);
        assert_eq!(back.into_engine(), o);
    }
}

#[test]
fn limit_exceeded_is_appended_last_and_old_encodings_are_unchanged() {
    // WireOutcome::AttemptFailed = 4; Trap = 7 (pre-11-06), LimitExceeded = 8 (appended).
    let trap = WireOutcome::from(&ActivationOutcome::AttemptFailed(AttemptFailure::Trap(
        "x".into(),
    )));
    assert_eq!(trap.encode(), vec![4, 7, 1, b'x']);
    let le = WireOutcome::from(&ActivationOutcome::AttemptFailed(
        AttemptFailure::LimitExceeded {
            limit: "history_events",
            max: 20,
            actual: 21,
        },
    ));
    let mut want = vec![4, 8, 14];
    want.extend_from_slice(b"history_events");
    want.extend_from_slice(&[20, 21]);
    assert_eq!(le.encode(), want);
}

#[test]
fn unknown_limit_name_from_a_worker_is_a_protocol_violation() {
    let w = WireOutcome::AttemptFailed(ostro_worker_proto::WireAttemptFailure::LimitExceeded {
        limit: "galaxies".into(),
        max: 1,
        actual: 2,
    });
    match w.into_engine() {
        ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(m)) => {
            assert!(m.contains("galaxies"), "{m}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn engine_limits_run_limit_fields_are_appended() {
    // Fields 10..12 (11-06); 0 = worker default.
    let l = v1::EngineLimits {
        max_history_events: 1,
        max_children_per_run: 2,
        max_payload_bytes: 3,
        ..Default::default()
    };
    assert_eq!(l.encode_to_vec(), vec![10 << 3, 1, 11 << 3, 2, 12 << 3, 3]);
    assert!(v1::EngineLimits::default().encode_to_vec().is_empty());
}

#[test]
fn garbage_outcome_bytes_are_an_error_not_a_panic() {
    assert!(WireOutcome::decode(&[0xff, 0xff, 0xff]).is_err());
    assert!(WireOutcome::decode(&[]).is_err());
}

#[test]
fn done_message_carries_the_outcome_bytes() {
    let outcome = WireOutcome::from(&ActivationOutcome::Completed {
        output: payload(b"42"),
    });
    let done = v1::Done {
        run: Some(v1::RunRef {
            run_id: b"run-1".to_vec(),
            partition: 3,
        }),
        token: Some(v1::Token { epoch: 2, n: 9 }),
        outcome: outcome.encode(),
        engine_end_seq: 12,
    };
    let msg = v1::WorkerMsg {
        msg: Some(v1::worker_msg::Msg::Done(done.clone())),
    };
    let back = v1::WorkerMsg::decode(msg.encode_to_vec().as_slice()).expect("decode");
    let Some(v1::worker_msg::Msg::Done(d)) = back.msg else {
        panic!("not a Done");
    };
    assert_eq!(d, done);
    assert_eq!(WireOutcome::decode(&d.outcome).expect("outcome"), outcome);
    assert_eq!(PROTO_VERSION, 1);
}

#[test]
fn append_result_variants_round_trip() {
    use v1::append_result::Result as R;
    for r in [
        R::Next(10),
        R::Fenced(true),
        R::ConflictExpected(4),
        R::Paused(true),
        R::Rejected("too large".into()),
    ] {
        let m = v1::AppendResult {
            result: Some(r.clone()),
        };
        let back = v1::AppendResult::decode(m.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back.result, Some(r));
    }
}

#[test]
fn execute_service_client_and_server_exist() {
    // Compile-time check that the full service was generated.
    let _client: Option<v1::execute_client::ExecuteClient<tonic::transport::Channel>> = None;
    let _ = std::mem::size_of::<v1::Hello>();
    let _ = std::mem::size_of::<v1::Welcome>();
    let _ = std::mem::size_of::<v1::EngineLimits>();
    let _ = std::mem::size_of::<v1::FetchBlobRequest>();
    let _ = std::mem::size_of::<v1::BlobChunk>();
    let _ = std::mem::size_of::<v1::RenewRequest>();
    let _ = std::mem::size_of::<v1::RenewResponse>();
    let _ = std::mem::size_of::<v1::StartEffectResponse>();
    let _ = v1::execute_server::SERVICE_NAME;
}

/// Phase 13 (13-03, D-P13-18): `Hello.feature_level` is the additive field 10; bytes from a
/// pre-Phase-13 worker (no field 10) still decode, with `feature_level = 0`.
#[test]
fn hello_feature_level_is_additive() {
    let full = v1::Hello {
        worker_id: "w1".into(),
        proto_version: PROTO_VERSION,
        ostro_version: "0.2.0".into(),
        wasmtime_version: "50.0.2".into(),
        event_codec_ver: 1,
        wit_packages: vec!["ostro:runtime@0.1.0".into()],
        labels: [("zone".to_owned(), "a".to_owned())].into_iter().collect(),
        slots: 4,
        memory_bytes: 1 << 30,
        feature_level: 2,
    };
    let back = v1::Hello::decode(full.encode_to_vec().as_slice()).expect("decode");
    assert_eq!(back, full);
    // Field 10, varint: tag byte (10 << 3) | 0 = 0x50, value 2.
    let bytes = full.encode_to_vec();
    assert!(bytes.ends_with(&[0x50, 0x02]), "{bytes:?}");

    // An old worker's Hello: everything up to field 9.
    let old = v1::Hello {
        feature_level: 0,
        ..full.clone()
    };
    let old_bytes = old.encode_to_vec();
    assert_eq!(&bytes[..bytes.len() - 2], old_bytes.as_slice());
    let decoded = v1::Hello::decode(old_bytes.as_slice()).expect("old hello decodes");
    assert_eq!(decoded.feature_level, 0);
    assert_eq!(decoded.worker_id, "w1");
    assert_eq!(decoded.memory_bytes, 1 << 30);
}
