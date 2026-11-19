//! Golden vectors and property tests for the journal codec and command hash v1 (plan 03-02).
//!
//! `tests/golden/journal_v1.txt` pins every byte layout. Regenerate ONLY when a codec or
//! hash version is bumped: `OSTRO_BLESS=1 cargo test -p ostro-types --test journal_golden`.

use ostro_types::journal::*;
use proptest::prelude::*;

const GOLDEN: &str = include_str!("golden/journal_v1.txt");
const HEADER: &str = "# ostro journal golden vectors v1 — DO NOT REGENERATE without bumping HASH_VER/EVENT_CODEC_VER/RESULT_CODEC_VER (frozen at Phase 5)";

/// A NaN with a non-default payload (sign bit set, payload 0xdead).
const ODD_NAN: u64 = 0xfff8_0000_0000_dead;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn meta(n: u8) -> CommandMeta {
    CommandMeta {
        hash_ver: HASH_VER,
        hash: CommandHash([n; 16]),
    }
}

fn payload(s: &str) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: s.as_bytes().to_vec(),
    }
}

fn err(kind: &str) -> ErrorInfo {
    ErrorInfo {
        kind: kind.into(),
        message: "boom".into(),
        details: Some(payload("{\"x\":1}")),
    }
}

fn all_sql_values() -> Vec<SqlValue> {
    vec![
        SqlValue::Null,
        SqlValue::Boolean(true),
        SqlValue::Int8(-42),
        SqlValue::Float8(1.5),
        SqlValue::Float8(f64::from_bits(ODD_NAN)),
        SqlValue::Numeric("12.50".into()),
        SqlValue::Text("héllo".into()),
        SqlValue::Bytea(vec![0, 1, 0xff]),
        SqlValue::Json("{\"a\":[1,2]}".into()),
        SqlValue::Timestamptz {
            seconds: 1_700_000_000,
            nanoseconds: 123_456_789,
        },
    ]
}

/// Deterministic events; at least one per EventKind (named for the golden file).
fn golden_events() -> Vec<(&'static str, Event)> {
    let ev = |seq: u64, body: EventBody| Event {
        seq,
        ts_nanos: 1_700_000_000_000_000_000 + seq,
        body,
    };
    let step_ok = encode_result(&Ok::<Payload, Failure>(payload("{\"ok\":true}")));
    let eff_ok = encode_result(&Ok::<OpOutput, Failure>(OpOutput::Timer));
    let child_err = encode_result(&Err::<OpOutput, Failure>(Failure::Cancelled));
    vec![
        (
            "run_started",
            ev(
                0,
                EventBody::RunStarted(RunStarted {
                    workflow: "orders".into(),
                    workflow_version: 3,
                    input: payload("{\"id\":7}"),
                    rng_seed: RngSeed([0xab; 32]),
                    accepted_at_nanos: 1_699_999_999_000_000_000,
                    deadline_nanos: Some(1_800_000_000_000_000_000),
                    module_hash: "sha256:00ff".into(),
                    tenant: Some("acme".into()),
                    parent_run_id: None,
                    continued_from: Some("run-0".into()),
                }),
            ),
        ),
        (
            "step_completed",
            ev(
                1,
                EventBody::StepCompleted(StepCompleted {
                    meta: meta(1),
                    name: "charge".into(),
                    outcome: step_ok,
                    fuel_used: 12_345,
                }),
            ),
        ),
        (
            "timer_started",
            ev(
                2,
                EventBody::TimerStarted(TimerStarted {
                    meta: meta(2),
                    fire_at_nanos: 1_700_000_060_000_000_000,
                }),
            ),
        ),
        (
            "effect_scheduled_http",
            ev(
                3,
                EventBody::EffectScheduled(EffectScheduled {
                    meta: meta(3),
                    request: EffectRequest::Http(HttpRequest {
                        method: "POST".into(),
                        url: "https://api.example.com/v1/charge".into(),
                        headers: vec![Header {
                            name: "Authorization".into(),
                            value: "Bearer {{secret:stripe}}".into(),
                        }],
                        body: Some(b"{\"amount\":100}".to_vec()),
                        timeout_ms: Some(5_000),
                        idempotency_key: Some("idem-1".into()),
                        redact: vec!["authorization".into()],
                    }),
                }),
            ),
        ),
        (
            "effect_scheduled_sql",
            ev(
                4,
                EventBody::EffectScheduled(EffectScheduled {
                    meta: meta(4),
                    request: EffectRequest::Sql(SqlRequest {
                        connection: "main".into(),
                        statement: "insert into t values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)".into(),
                        params: all_sql_values(),
                        exactly_once: true,
                    }),
                }),
            ),
        ),
        (
            "effect_scheduled_object",
            ev(
                5,
                EventBody::EffectScheduled(EffectScheduled {
                    meta: meta(5),
                    request: EffectRequest::Object(ObjectRequest {
                        connection: "s3".into(),
                        op: "put".into(),
                        key: "invoices/7.pdf".into(),
                        body: Some(vec![1, 2, 3]),
                    }),
                }),
            ),
        ),
        (
            "signal_wait_started",
            ev(
                6,
                EventBody::SignalWaitStarted(SignalWaitStarted {
                    meta: meta(6),
                    name: "approve".into(),
                    timeout_ms: Some(86_400_000),
                }),
            ),
        ),
        (
            "child_started",
            ev(
                7,
                EventBody::ChildStarted(ChildStarted {
                    meta: meta(7),
                    workflow: "ship".into(),
                    input: payload("{\"order\":7}"),
                    options: ChildOptions {
                        queue: Some("fast".into()),
                        version: Some(2),
                        detached: true,
                        run_timeout_ms: Some(60_000),
                    },
                }),
            ),
        ),
        (
            "await_token_created",
            ev(
                8,
                EventBody::AwaitTokenCreated(AwaitTokenCreated {
                    meta: meta(8),
                    token: "tok-8".into(),
                    timeout_ms: None,
                }),
            ),
        ),
        (
            "op_cancelled",
            ev(
                9,
                EventBody::OpCancelled(OpCancelled {
                    meta: meta(9),
                    op: 4,
                }),
            ),
        ),
        (
            "wait_resolved",
            ev(
                10,
                EventBody::WaitResolved(WaitResolved {
                    meta: meta(10),
                    completed: vec![2, 3, u64::MAX],
                }),
            ),
        ),
        (
            "patch_marker",
            ev(
                11,
                EventBody::PatchMarker(PatchMarker {
                    meta: meta(11),
                    change_id: "use-v2-pricing".into(),
                }),
            ),
        ),
        (
            "status_published",
            ev(
                12,
                EventBody::StatusPublished(StatusPublished {
                    meta: meta(12),
                    json: "{\"stage\":\"shipping\"}".into(),
                }),
            ),
        ),
        (
            "run_completed",
            ev(
                13,
                EventBody::RunCompleted(RunCompleted {
                    meta: meta(13),
                    output: payload("{\"done\":true}"),
                }),
            ),
        ),
        (
            "run_failed",
            ev(
                14,
                EventBody::RunFailed(RunFailed {
                    meta: meta(14),
                    failure: Failure::NonRetryable(err("validation")),
                }),
            ),
        ),
        (
            "run_continued_as_new",
            ev(
                15,
                EventBody::RunContinuedAsNew(RunContinuedAsNew {
                    meta: meta(15),
                    continuation: Continuation {
                        input: payload("{\"cursor\":100}"),
                        workflow: Some("orders".into()),
                        version: Some(4),
                    },
                }),
            ),
        ),
        (
            "timer_fired",
            ev(16, EventBody::TimerFired(TimerFired { op: 2 })),
        ),
        (
            "effect_completed",
            ev(
                17,
                EventBody::EffectCompleted(EffectCompleted {
                    op: 3,
                    outcome: eff_ok,
                }),
            ),
        ),
        (
            "signal_received",
            ev(
                18,
                EventBody::SignalReceived(SignalReceived {
                    op: 6,
                    payload: payload("{\"approved\":true}"),
                }),
            ),
        ),
        (
            "child_completed",
            ev(
                19,
                EventBody::ChildCompleted(ChildCompleted {
                    op: 7,
                    outcome: child_err,
                }),
            ),
        ),
        (
            "token_completed",
            ev(
                20,
                EventBody::TokenCompleted(TokenCompleted {
                    op: 8,
                    payload: payload("{\"t\":1}"),
                }),
            ),
        ),
        (
            "cancel_requested",
            ev(
                21,
                EventBody::CancelRequested(CancelRequested {
                    reason: "user request".into(),
                }),
            ),
        ),
        // Additive kinds (no EVENT_CODEC_VER bump) are appended here, AFTER every
        // Phase 3 vector, so blessing only ever adds golden lines.
        (
            "step_attempt_failed",
            ev(
                22,
                EventBody::StepAttemptFailed(StepAttemptFailed {
                    meta: meta(1),
                    name: "charge".into(),
                    attempt: 2,
                    failure: Failure::Retryable(err("io")),
                    retry_at_nanos: 1_700_000_000_400_000_000,
                    backoff: AttemptBackoff::Requeue,
                    fuel_used: 777,
                }),
            ),
        ),
        // Phase 5 kinds (plan 05-01, D3/D4/D21), appended in EventBody order.
        (
            "wait_timed_out",
            ev(23, EventBody::WaitTimedOut(WaitTimedOut { op: 9 })),
        ),
        (
            "run_terminated",
            ev(
                24,
                EventBody::RunTerminated(RunTerminated {
                    reason: "run_timeout".into(),
                }),
            ),
        ),
        (
            "signal_payload",
            ev(
                25,
                EventBody::SignalPayload(SignalPayload {
                    name: "approve".into(),
                    payload: payload("{\"approved\":true}"),
                }),
            ),
        ),
        (
            "signal_matched",
            ev(
                26,
                EventBody::SignalMatched(SignalMatched {
                    op: 6,
                    payload_seq: 25,
                }),
            ),
        ),
    ]
}

/// Fixed recorded results: step results and every OpOutput case plus every Failure case.
fn golden_results() -> Vec<(&'static str, RecordedResult)> {
    type Step = Result<Payload, Failure>;
    type Op = Result<OpOutput, Failure>;
    vec![
        ("step_ok", encode_result(&Step::Ok(payload("{\"n\":1}")))),
        (
            "step_err_retryable",
            encode_result(&Step::Err(Failure::Retryable(err("io")))),
        ),
        (
            "step_err_non_retryable",
            encode_result(&Step::Err(Failure::NonRetryable(ErrorInfo {
                kind: "bad".into(),
                message: "no".into(),
                details: None,
            }))),
        ),
        (
            "step_err_timeout",
            encode_result(&Step::Err(Failure::Timeout)),
        ),
        (
            "step_err_cancelled",
            encode_result(&Step::Err(Failure::Cancelled)),
        ),
        (
            "op_http",
            encode_result(&Op::Ok(OpOutput::Http(HttpResponse {
                status: 201,
                headers: vec![Header {
                    name: "content-type".into(),
                    value: "application/json".into(),
                }],
                body: b"{\"id\":\"ch_1\"}".to_vec(),
            }))),
        ),
        (
            "op_sql",
            encode_result(&Op::Ok(OpOutput::Sql(SqlResult {
                columns: vec!["a".into(), "b".into()],
                rows: vec![
                    all_sql_values()[..5].to_vec(),
                    all_sql_values()[5..].to_vec(),
                ],
                rows_affected: 2,
            }))),
        ),
        (
            "op_object",
            encode_result(&Op::Ok(OpOutput::Object(vec![9, 8, 7]))),
        ),
        ("op_timer", encode_result(&Op::Ok(OpOutput::Timer))),
        (
            "op_signal",
            encode_result(&Op::Ok(OpOutput::Signal(payload("{\"s\":1}")))),
        ),
        (
            "op_child",
            encode_result(&Op::Ok(OpOutput::Child(payload("{\"c\":1}")))),
        ),
        (
            "op_token",
            encode_result(&Op::Ok(OpOutput::Token(payload("{\"k\":1}")))),
        ),
        (
            "op_cancel_requested",
            encode_result(&Op::Ok(OpOutput::CancelRequested)),
        ),
        ("op_err_timeout", encode_result(&Op::Err(Failure::Timeout))),
    ]
}

/// One CommandArgs per CommandKind.
fn golden_hashes() -> Vec<(&'static str, CommandHash)> {
    let params = all_sql_values();
    let ops = [1u64, 5, 9];
    let args: Vec<(&'static str, CommandArgs<'_>)> = vec![
        ("step", CommandArgs::Step { name: "charge" }),
        (
            "http",
            CommandArgs::Http {
                method: "POST",
                url: "https://api.example.com/v1/charge",
                body: b"{\"amount\":100}",
                header_names: vec!["Authorization", "Content-Type"],
            },
        ),
        (
            "sql",
            CommandArgs::Sql {
                connection: "main",
                statement: "select $1",
                params: &params,
            },
        ),
        (
            "object",
            CommandArgs::Object {
                connection: "s3",
                op: "put",
                key: "invoices/7.pdf",
                body: &[1, 2, 3],
            },
        ),
        (
            "sleep",
            CommandArgs::Sleep {
                duration_ms: 60_000,
            },
        ),
        (
            "sleep_until",
            CommandArgs::SleepUntil {
                seconds: 1_700_000_000,
                nanoseconds: 5,
            },
        ),
        ("wait_signal", CommandArgs::WaitSignal { name: "approve" }),
        (
            "start_child",
            CommandArgs::StartChild {
                workflow: "ship",
                input: b"{\"order\":7}",
            },
        ),
        ("create_await_token", CommandArgs::CreateAwaitToken),
        ("cancel_op", CommandArgs::CancelOp { op: 4 }),
        (
            "wait",
            CommandArgs::Wait {
                ops: &ops,
                mode: WaitMode::Any,
            },
        ),
        (
            "patch",
            CommandArgs::Patch {
                change_id: "use-v2-pricing",
            },
        ),
        ("set_status", CommandArgs::SetStatus),
        ("terminal", CommandArgs::Terminal),
    ];
    args.into_iter()
        .map(|(n, a)| (n, command_hash(HASH_VER, &a).expect("v1")))
        .collect()
}

// ---------------------------------------------------------------------------
// Golden file plumbing
// ---------------------------------------------------------------------------

fn render() -> String {
    let mut out = String::new();
    out.push_str(HEADER);
    out.push('\n');
    for (name, ev) in golden_events() {
        let (kind, bytes) = encode_event(&ev);
        out.push_str(&format!("event {kind} {name} {}\n", hex(&bytes)));
    }
    for (name, r) in golden_results() {
        out.push_str(&format!(
            "result {name} {:02x}{}\n",
            r.codec_ver,
            hex(&r.bytes)
        ));
    }
    for (name, h) in golden_hashes() {
        out.push_str(&format!("hash {name} {}\n", hex(&h.0)));
    }
    out
}

/// With `OSTRO_BLESS=1`, rewrite the golden file (atomically) and return true.
fn bless() -> bool {
    if std::env::var("OSTRO_BLESS").as_deref() != Ok("1") {
        return false;
    }
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden");
    let tmp = format!("{dir}/journal_v1.txt.{}.tmp", std::process::id());
    let content = render();
    std::fs::write(&tmp, &content).expect("write tmp");
    std::fs::rename(&tmp, format!("{dir}/journal_v1.txt")).expect("rename");
    true
}

/// Golden lines starting with `prefix ` (in order).
fn golden_section(prefix: &str) -> Vec<&'static str> {
    let mut lines = GOLDEN.lines();
    assert_eq!(lines.next(), Some(HEADER), "golden header changed");
    lines
        .filter(|l| l.split(' ').next() == Some(prefix))
        .collect()
}

fn rendered_section(prefix: &str) -> Vec<String> {
    render()
        .lines()
        .filter(|l| l.split(' ').next() == Some(prefix))
        .map(str::to_string)
        .collect()
}

fn assert_section(prefix: &str) {
    let want = golden_section(prefix);
    let got = rendered_section(prefix);
    assert!(!want.is_empty(), "no `{prefix}` lines in golden file");
    assert_eq!(
        got, want,
        "`{prefix}` golden vectors drifted; bump the codec/hash version instead of re-blessing"
    );
}

/// Event equality that compares f64 by bit pattern (NaN != NaN under PartialEq).
fn assert_event_eq(a: &Event, b: &Event) {
    if a != b {
        // Fall back to byte comparison: NaN-bearing events are equal iff they re-encode
        // identically, and the bit pattern of every Float8 must survive.
        assert_eq!(
            encode_event(a),
            encode_event(b),
            "events differ: {a:?} vs {b:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Golden tests
// ---------------------------------------------------------------------------

#[test]
fn golden_event_codec() {
    if bless() {
        return;
    }
    assert_section("event");
    let events = golden_events();
    let kinds: std::collections::BTreeSet<u32> =
        events.iter().map(|(_, e)| e.body.kind().code()).collect();
    assert_eq!(kinds.len(), 25, "every EventKind needs a golden event");
    for (line, (name, ev)) in golden_section("event").iter().zip(&events) {
        let mut parts = line.split(' ');
        assert_eq!(parts.next(), Some("event"));
        let kind: u32 = parts.next().expect("kind").parse().expect("u32");
        assert_eq!(parts.next(), Some(*name));
        let bytes = unhex(parts.next().expect("hex"));
        let back = decode_event(ev.seq, kind, &bytes).expect("decode golden");
        assert_eq!(back.seq, ev.seq);
        assert_event_eq(&back, ev);
        if let EventBody::EffectScheduled(EffectScheduled {
            request: EffectRequest::Sql(r),
            ..
        }) = &back.body
        {
            let bits: Vec<u64> = r
                .params
                .iter()
                .filter_map(|p| match p {
                    SqlValue::Float8(f) => Some(f.to_bits()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                bits,
                vec![1.5f64.to_bits(), ODD_NAN],
                "f64 bits must survive"
            );
        }
    }
}

#[test]
fn golden_result_codec() {
    if bless() {
        return;
    }
    assert_section("result");
    for (line, (name, r)) in golden_section("result").iter().zip(golden_results()) {
        assert_eq!(line.split(' ').nth(1), Some(name));
        assert_eq!(r.codec_ver, RESULT_CODEC_VER);
        // Every committed result decodes as its type and re-encodes byte-identically.
        if name.starts_with("step") {
            let v: Result<Payload, Failure> = decode_result(&r).expect("decode step");
            assert_eq!(encode_result(&v), r);
        } else {
            let v: Result<OpOutput, Failure> = decode_result(&r).expect("decode op");
            assert_eq!(encode_result(&v), r);
        }
    }
}

#[test]
fn golden_command_hash() {
    if bless() {
        return;
    }
    assert_section("hash");
    let hashes = golden_hashes();
    assert_eq!(hashes.len(), 14, "one hash per CommandKind");
    let distinct: std::collections::BTreeSet<[u8; 16]> = hashes.iter().map(|(_, h)| h.0).collect();
    assert_eq!(distinct.len(), 14, "kinds must not collide");
}

// ---------------------------------------------------------------------------
// Hash semantics
// ---------------------------------------------------------------------------

fn h(args: &CommandArgs<'_>) -> CommandHash {
    command_hash(HASH_VER, args).expect("v1")
}

fn http_req(headers: &[(&str, &str)], url: &str) -> HttpRequest {
    HttpRequest {
        method: "POST".into(),
        url: url.into(),
        headers: headers
            .iter()
            .map(|(n, v)| Header {
                name: (*n).into(),
                value: (*v).into(),
            })
            .collect(),
        body: Some(b"{}".to_vec()),
        timeout_ms: Some(1),
        idempotency_key: None,
        redact: vec![],
    }
}

#[test]
fn hash_ignores_unhashed_fields() {
    // Header values and header order are not hashed; header-name case is canonicalised.
    // Callers build CommandArgs from the request via `command_args`, which keeps names only.
    let a = http_req(
        &[("Authorization", "Bearer {{secret:a}}"), ("X-Id", "1")],
        "https://x/a",
    );
    let mut b = http_req(
        &[("x-id", "2"), ("authorization", "Bearer other")],
        "https://x/a",
    );
    b.timeout_ms = Some(99_999);
    b.idempotency_key = Some("k".into());
    b.redact = vec!["authorization".into()];
    let ra = EffectRequest::Http(a);
    let rb = EffectRequest::Http(b);
    match command_args(&ra) {
        CommandArgs::Http { header_names, .. } => {
            assert_eq!(header_names, vec!["Authorization", "X-Id"]);
        }
        other => panic!("expected http args, got {other:?}"),
    }
    assert_eq!(h(&command_args(&ra)), h(&command_args(&rb)));

    // A different url changes the hash.
    let rc = EffectRequest::Http(http_req(&[("X-Id", "1")], "https://x/b"));
    let rd = EffectRequest::Http(http_req(&[("X-Id", "1")], "https://x/a"));
    assert_ne!(h(&command_args(&rc)), h(&command_args(&rd)));

    // NaN payloads are canonicalised in SQL params.
    let p1 = [SqlValue::Float8(f64::NAN)];
    let p2 = [SqlValue::Float8(f64::from_bits(ODD_NAN))];
    let p3 = [SqlValue::Float8(0.0)];
    fn sql(p: &[SqlValue]) -> CommandHash {
        h(&CommandArgs::Sql {
            connection: "c",
            statement: "s",
            params: p,
        })
    }
    assert_eq!(sql(&p1), sql(&p2));
    assert_ne!(sql(&p1), sql(&p3));
    // exactly_once is not hashed.
    let s1 = EffectRequest::Sql(SqlRequest {
        connection: "c".into(),
        statement: "s".into(),
        params: vec![SqlValue::Int8(1)],
        exactly_once: true,
    });
    let s2 = EffectRequest::Sql(SqlRequest {
        connection: "c".into(),
        statement: "s".into(),
        params: vec![SqlValue::Int8(1)],
        exactly_once: false,
    });
    assert_eq!(h(&command_args(&s1)), h(&command_args(&s2)));

    // Hashed scalar fields change the hash.
    assert_ne!(
        h(&CommandArgs::Step { name: "a" }),
        h(&CommandArgs::Step { name: "b" })
    );
    assert_ne!(
        h(&CommandArgs::Sleep { duration_ms: 1 }),
        h(&CommandArgs::Sleep { duration_ms: 2 })
    );
    assert_ne!(
        h(&CommandArgs::Wait {
            ops: &[1, 2],
            mode: WaitMode::All
        }),
        h(&CommandArgs::Wait {
            ops: &[1, 2],
            mode: WaitMode::Any
        })
    );
    // Ops are hashed in guest order.
    assert_ne!(
        h(&CommandArgs::Wait {
            ops: &[1, 2],
            mode: WaitMode::All
        }),
        h(&CommandArgs::Wait {
            ops: &[2, 1],
            mode: WaitMode::All
        })
    );
    // Length prefixes prevent field-boundary ambiguity.
    assert_ne!(
        h(&CommandArgs::Object {
            connection: "ab",
            op: "c",
            key: "k",
            body: &[]
        }),
        h(&CommandArgs::Object {
            connection: "a",
            op: "bc",
            key: "k",
            body: &[]
        })
    );
    // Kind-only commands: same kind same hash, different kinds differ.
    assert_eq!(h(&CommandArgs::SetStatus), h(&CommandArgs::SetStatus));
    assert_ne!(h(&CommandArgs::SetStatus), h(&CommandArgs::Terminal));
}

#[test]
fn canonical_args_layout_is_hand_specified() {
    // Step: u32 LE length ‖ UTF-8.
    assert_eq!(
        canonical_args(&CommandArgs::Step { name: "ab" }),
        vec![2, 0, 0, 0, b'a', b'b']
    );
    // Sleep: u64 LE.
    assert_eq!(
        canonical_args(&CommandArgs::Sleep { duration_ms: 258 }),
        vec![2, 1, 0, 0, 0, 0, 0, 0]
    );
    // SleepUntil: u64 LE ‖ u32 LE.
    assert_eq!(
        canonical_args(&CommandArgs::SleepUntil {
            seconds: 1,
            nanoseconds: 2
        }),
        vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0]
    );
    // Wait: u32 LE count ‖ u64 LE ops ‖ mode u8.
    assert_eq!(
        canonical_args(&CommandArgs::Wait {
            ops: &[3],
            mode: WaitMode::Any
        }),
        vec![1, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1]
    );
    // Kind-only commands hash no argument bytes.
    assert!(canonical_args(&CommandArgs::CreateAwaitToken).is_empty());
    assert!(canonical_args(&CommandArgs::SetStatus).is_empty());
    assert!(canonical_args(&CommandArgs::Terminal).is_empty());
    // Http: method, url, sha256(body) (32 bytes), sorted lower-cased header names.
    let c = canonical_args(&CommandArgs::Http {
        method: "GET",
        url: "u",
        body: b"",
        header_names: vec!["B", "a"],
    });
    assert_eq!(c.len(), (4 + 3) + (4 + 1) + 32 + 4 + (4 + 1) + (4 + 1));
    assert_eq!(
        &c[c.len() - 14..],
        &[2, 0, 0, 0, 1, 0, 0, 0, b'a', 1, 0, 0, 0, b'b']
    );
}

// ---------------------------------------------------------------------------
// Decode rejections
// ---------------------------------------------------------------------------

#[test]
fn decode_rejects() {
    let (_, ev) = golden_events().remove(1);
    let (kind, bytes) = encode_event(&ev);
    assert!(decode_event(1, kind, &bytes).is_ok());

    assert!(matches!(
        decode_event(1, 9999, &bytes),
        Err(CodecError::UnknownKind(9999))
    ));
    let mut v2 = bytes.clone();
    v2[0] = 2;
    assert!(matches!(
        decode_event(1, kind, &v2),
        Err(CodecError::UnsupportedVersion(2))
    ));
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        decode_event(1, kind, &trailing),
        Err(CodecError::Malformed(_))
    ));
    let truncated = &bytes[..bytes.len() - 1];
    assert!(matches!(
        decode_event(1, kind, truncated),
        Err(CodecError::Malformed(_))
    ));
    assert!(matches!(
        decode_event(1, kind, &[]),
        Err(CodecError::Malformed(_))
    ));
    // A valid payload under the wrong kind does not silently decode as something else.
    let (tk, tb) = encode_event(&Event {
        seq: 0,
        ts_nanos: 0,
        body: EventBody::TimerFired(TimerFired { op: 1 }),
    });
    assert_eq!(tk, 64);
    assert!(decode_event(0, EventKind::RunStarted.code(), &tb).is_err());

    // Results.
    let r = encode_result(&Ok::<Payload, Failure>(payload("{}")));
    let bad_ver = RecordedResult {
        codec_ver: 2,
        bytes: r.bytes.clone(),
    };
    assert!(matches!(
        decode_result::<Result<Payload, Failure>>(&bad_ver),
        Err(CodecError::UnsupportedVersion(2))
    ));
    let mut tr = r.clone();
    tr.bytes.push(0);
    assert!(matches!(
        decode_result::<Result<Payload, Failure>>(&tr),
        Err(CodecError::Malformed(_))
    ));
    let mut tc = r.clone();
    tc.bytes.pop();
    assert!(matches!(
        decode_result::<Result<Payload, Failure>>(&tc),
        Err(CodecError::Malformed(_))
    ));

    // Hash version.
    assert!(matches!(
        command_hash(2, &CommandArgs::Terminal),
        Err(CodecError::UnsupportedVersion(2))
    ));
    assert!(matches!(
        command_hash(0, &CommandArgs::Terminal),
        Err(CodecError::UnsupportedVersion(0))
    ));

    // Errors render.
    assert!(CodecError::UnknownKind(9999).to_string().contains("9999"));
    let e: Box<dyn std::error::Error> = Box::new(CodecError::KindMismatch { kind: 3 });
    assert!(e.to_string().contains('3'));
}

// ---------------------------------------------------------------------------
// Property round-trips
// ---------------------------------------------------------------------------

fn arb_string() -> impl Strategy<Value = String> {
    "\\PC{0,12}"
}

fn arb_payload() -> impl Strategy<Value = Payload> {
    (arb_string(), proptest::collection::vec(any::<u8>(), 0..32))
        .prop_map(|(content_type, data)| Payload { content_type, data })
}

fn arb_error_info() -> impl Strategy<Value = ErrorInfo> {
    (
        arb_string(),
        arb_string(),
        proptest::option::of(arb_payload()),
    )
        .prop_map(|(kind, message, details)| ErrorInfo {
            kind,
            message,
            details,
        })
}

fn arb_failure() -> impl Strategy<Value = Failure> {
    prop_oneof![
        arb_error_info().prop_map(Failure::Retryable),
        arb_error_info().prop_map(Failure::NonRetryable),
        Just(Failure::Timeout),
        Just(Failure::Cancelled),
    ]
}

fn arb_sql_value() -> impl Strategy<Value = SqlValue> {
    prop_oneof![
        Just(SqlValue::Null),
        any::<bool>().prop_map(SqlValue::Boolean),
        any::<i64>().prop_map(SqlValue::Int8),
        any::<u64>().prop_map(|b| SqlValue::Float8(f64::from_bits(b))),
        arb_string().prop_map(SqlValue::Numeric),
        arb_string().prop_map(SqlValue::Text),
        proptest::collection::vec(any::<u8>(), 0..16).prop_map(SqlValue::Bytea),
        arb_string().prop_map(SqlValue::Json),
        (any::<u64>(), any::<u32>()).prop_map(|(seconds, nanoseconds)| SqlValue::Timestamptz {
            seconds,
            nanoseconds
        }),
    ]
}

fn arb_header() -> impl Strategy<Value = Header> {
    (arb_string(), arb_string()).prop_map(|(name, value)| Header { name, value })
}

fn arb_op_output() -> impl Strategy<Value = OpOutput> {
    prop_oneof![
        (
            any::<u16>(),
            proptest::collection::vec(arb_header(), 0..3),
            proptest::collection::vec(any::<u8>(), 0..32)
        )
            .prop_map(|(status, headers, body)| OpOutput::Http(HttpResponse {
                status,
                headers,
                body
            })),
        (
            proptest::collection::vec(arb_string(), 0..3),
            proptest::collection::vec(proptest::collection::vec(arb_sql_value(), 0..4), 0..3),
            any::<u64>()
        )
            .prop_map(|(columns, rows, rows_affected)| OpOutput::Sql(SqlResult {
                columns,
                rows,
                rows_affected
            })),
        proptest::collection::vec(any::<u8>(), 0..32).prop_map(OpOutput::Object),
        Just(OpOutput::Timer),
        arb_payload().prop_map(OpOutput::Signal),
        arb_payload().prop_map(OpOutput::Child),
        arb_payload().prop_map(OpOutput::Token),
        Just(OpOutput::CancelRequested),
    ]
}

fn arb_meta() -> impl Strategy<Value = CommandMeta> {
    any::<[u8; 16]>().prop_map(|h| CommandMeta {
        hash_ver: HASH_VER,
        hash: CommandHash(h),
    })
}

fn arb_body() -> impl Strategy<Value = EventBody> {
    let rr = proptest::collection::vec(any::<u8>(), 0..32).prop_map(|bytes| RecordedResult {
        codec_ver: RESULT_CODEC_VER,
        bytes,
    });
    prop_oneof![
        (arb_meta(), arb_string(), rr, any::<u64>()).prop_map(
            |(meta, name, outcome, fuel_used)| {
                EventBody::StepCompleted(StepCompleted {
                    meta,
                    name,
                    outcome,
                    fuel_used,
                })
            }
        ),
        (arb_meta(), any::<u64>()).prop_map(|(meta, fire_at_nanos)| {
            EventBody::TimerStarted(TimerStarted {
                meta,
                fire_at_nanos,
            })
        }),
        any::<u64>().prop_map(|op| EventBody::TimerFired(TimerFired { op })),
        (arb_meta(), proptest::collection::vec(any::<u64>(), 0..6)).prop_map(
            |(meta, completed)| EventBody::WaitResolved(WaitResolved { meta, completed })
        ),
        (arb_meta(), arb_string(), proptest::option::of(any::<u64>())).prop_map(
            |(meta, name, timeout_ms)| EventBody::SignalWaitStarted(SignalWaitStarted {
                meta,
                name,
                timeout_ms
            })
        ),
        (any::<u64>(), arb_payload())
            .prop_map(|(op, payload)| EventBody::SignalReceived(SignalReceived { op, payload })),
        any::<u64>().prop_map(|op| EventBody::WaitTimedOut(WaitTimedOut { op })),
        arb_string().prop_map(|reason| EventBody::RunTerminated(RunTerminated { reason })),
        (arb_string(), arb_payload())
            .prop_map(|(name, payload)| EventBody::SignalPayload(SignalPayload { name, payload })),
        (any::<u64>(), any::<u64>()).prop_map(|(op, payload_seq)| EventBody::SignalMatched(
            SignalMatched { op, payload_seq }
        )),
        (
            arb_meta(),
            arb_string(),
            any::<u32>(),
            arb_failure(),
            any::<u64>(),
            any::<bool>(),
            any::<u64>()
        )
            .prop_map(
                |(meta, name, attempt, failure, retry_at_nanos, requeue, fuel_used)| {
                    EventBody::StepAttemptFailed(StepAttemptFailed {
                        meta,
                        name,
                        attempt,
                        failure,
                        retry_at_nanos,
                        backoff: if requeue {
                            AttemptBackoff::Requeue
                        } else {
                            AttemptBackoff::InActivation
                        },
                        fuel_used,
                    })
                }
            ),
    ]
}

proptest! {
    #[test]
    fn prop_step_result_roundtrip(v in prop_oneof![arb_payload().prop_map(Ok), arb_failure().prop_map(Err)]) {
        let r = encode_result(&v);
        let back: Result<Payload, Failure> = decode_result(&r).unwrap();
        prop_assert_eq!(&back, &v);
        prop_assert_eq!(encode_result(&back), r);
    }

    #[test]
    fn prop_op_result_roundtrip(v in prop_oneof![arb_op_output().prop_map(Ok), arb_failure().prop_map(Err)]) {
        let r = encode_result(&v);
        let back: Result<OpOutput, Failure> = decode_result(&r).unwrap();
        // NaN-safe: re-encoding must be byte-identical.
        prop_assert_eq!(encode_result(&back), r);
    }

    #[test]
    fn prop_event_roundtrip(seq in any::<u64>(), ts_nanos in any::<u64>(), body in arb_body()) {
        let ev = Event { seq, ts_nanos, body };
        let (kind, bytes) = encode_event(&ev);
        prop_assert_eq!(kind, ev.body.kind().code());
        prop_assert_eq!(bytes[0], EVENT_CODEC_VER);
        let back = decode_event(seq, kind, &bytes).unwrap();
        prop_assert_eq!(&back, &ev);
        prop_assert_eq!(encode_event(&back), (kind, bytes));
    }

    #[test]
    fn prop_sql_hash_nan_canonical(a in any::<u64>(), b in any::<u64>()) {
        let fa = f64::from_bits(a);
        let fb = f64::from_bits(b);
        let pa = [SqlValue::Float8(fa)];
        let pb = [SqlValue::Float8(fb)];
        let ha = h(&CommandArgs::Sql { connection: "c", statement: "s", params: &pa });
        let hb = h(&CommandArgs::Sql { connection: "c", statement: "s", params: &pb });
        if (fa.is_nan() && fb.is_nan()) || a == b {
            prop_assert_eq!(ha, hb);
        } else {
            prop_assert_ne!(ha, hb);
        }
    }
}
