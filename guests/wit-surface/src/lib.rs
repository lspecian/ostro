//! WIT surface guest: calls every `ostro:runtime@0.1.0` import, with one named workflow
//! per WIT feature (used by the host adapter and activation tests).

wit_bindgen::generate!({
    path: "../../wit/ostro-runtime-0.1.0",
    world: "workflow",
});

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

use exports::ostro::runtime::workflows::{Continuation, Guest, RunResult, WorkflowMeta};
use ostro::runtime::context;
use ostro::runtime::effects::{
    self, ChildOptions, Completion, Header, HttpRequest, ObjectRequest, OpOutput, SqlRequest,
    SqlValue, WaitMode,
};
use ostro::runtime::journal::{self, StepStart};
use ostro::runtime::log::{self, Level};
use ostro::runtime::types::{Datetime, ErrorInfo, Failure, Payload, StepOptions};

const TEXT: &str = "text/plain";
const BYTES: &str = "application/octet-stream";

const WORKFLOWS: &[&str] = &[
    "surface",
    "echo-bytes",
    "cancel-cleanup",
    "can",
    "token",
    "status",
    "non-retryable",
    "step-fail",
    "nested-step",
    "info",
    "panic",
    "exit",
];

fn opts() -> StepOptions {
    StepOptions {
        retry: None,
        timeout_ms: None,
        fuel: None,
    }
}

fn text(s: impl Into<String>) -> Payload {
    Payload {
        content_type: TEXT.to_string(),
        data: s.into().into_bytes(),
    }
}

/// Run (or replay) a step with outcome `f()`; returns the journaled outcome.
fn step(name: &str, f: impl FnOnce() -> Result<Payload, Failure>) -> Result<Payload, Failure> {
    match journal::begin_step(name, &opts()) {
        StepStart::Replayed(r) => r,
        StepStart::Execute(s) => {
            let r = f();
            s.complete(r.as_ref());
            r
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn sha(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

fn failure_kind(f: &Failure) -> String {
    match f {
        Failure::Retryable(e) => format!("retryable:{}", e.kind),
        Failure::NonRetryable(e) => format!("non-retryable:{}", e.kind),
        Failure::Timeout => "timeout".to_string(),
        Failure::Cancelled => "cancelled".to_string(),
    }
}

fn sql_digest(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "null".into(),
        SqlValue::Boolean(b) => format!("bool:{b}"),
        SqlValue::Int8(i) => format!("int8:{i}"),
        SqlValue::Float8(f) => format!("float8:{:016x}", f.to_bits()),
        SqlValue::Numeric(s) => format!("numeric:{s}"),
        SqlValue::Text(s) => format!("text:{s}"),
        SqlValue::Bytea(b) => format!("bytea:{}", sha(b)),
        SqlValue::Json(s) => format!("json:{s}"),
        SqlValue::Timestamptz(d) => format!("ts:{}.{}", d.seconds, d.nanoseconds),
    }
}

fn dump_completion(out: &mut String, c: &Completion) {
    let _ = write!(out, "op {}: ", c.op);
    match &c.outcome {
        Err(f) => {
            let _ = writeln!(out, "failed {}", failure_kind(f));
        }
        Ok(OpOutput::Http(r)) => {
            let headers: Vec<String> = r
                .headers
                .iter()
                .map(|h| format!("{}={}", h.name, h.value))
                .collect();
            let _ = writeln!(
                out,
                "http {} [{}] body-sha256 {}",
                r.status,
                headers.join(","),
                sha(&r.body)
            );
        }
        Ok(OpOutput::Sql(r)) => {
            let mut h = Sha256::new();
            for row in &r.rows {
                for v in row {
                    h.update(sql_digest(v).as_bytes());
                    h.update([0]);
                }
                h.update([1]);
            }
            let _ = writeln!(
                out,
                "sql cols [{}] rows {} affected {} digest {}",
                r.columns.join(","),
                r.rows.len(),
                r.rows_affected,
                hex(&h.finalize())
            );
        }
        Ok(OpOutput::Object(b)) => {
            let _ = writeln!(out, "object {} bytes sha256 {}", b.len(), sha(b));
        }
        Ok(OpOutput::Timer) => {
            let _ = writeln!(out, "timer");
        }
        Ok(OpOutput::Signal(p)) => {
            let _ = writeln!(out, "signal sha256 {}", sha(&p.data));
        }
        Ok(OpOutput::Child(p)) => {
            let _ = writeln!(out, "child sha256 {}", sha(&p.data));
        }
        Ok(OpOutput::Token(p)) => {
            let _ = writeln!(out, "token sha256 {}", sha(&p.data));
        }
        Ok(OpOutput::CancelRequested) => {
            let _ = writeln!(out, "cancel-requested");
        }
    }
}

fn surface() -> Result<RunResult, Failure> {
    let mut out = String::new();
    let info = context::info();
    let _ = writeln!(
        out,
        "info {} {} v{}",
        info.run_id, info.workflow, info.workflow_version
    );
    let now = context::now();
    let _ = writeln!(out, "now {}.{}", now.seconds, now.nanoseconds);
    let _ = writeln!(out, "random-u64 {}", context::random_u64());
    let _ = writeln!(out, "random-bytes {}", hex(&context::random_bytes(8)));
    let _ = writeln!(out, "uuid-v7 {}", context::uuid_v7());
    let _ = writeln!(out, "is-replaying {}", context::is_replaying());
    let _ = writeln!(out, "patched {}", context::patched("p1"));
    context::set_status("{\"surface\":1}");
    log::emit(
        Level::Info,
        "surface log line",
        &[("k".to_string(), "v".to_string())],
    );
    let s = step("s", || Ok(text("step-out")));
    let _ = writeln!(
        out,
        "step {:?}",
        s.map(|p| p.data.len()).map_err(|f| failure_kind(&f))
    );

    let http_op = effects::http_fetch(&HttpRequest {
        method: "GET".to_string(),
        url: "https://example.test/x".to_string(),
        headers: vec![
            Header {
                name: "X-Token".to_string(),
                value: "{{secret:tok}}".to_string(),
            },
            Header {
                name: "Accept".to_string(),
                value: "*/*".to_string(),
            },
        ],
        body: None,
        timeout_ms: Some(5_000),
        idempotency_key: Some("idem-1".to_string()),
        retry: None,
        redact: vec!["X-Token".to_string()],
    });
    let sql_op = effects::sql_query(&SqlRequest {
        connection: "main".to_string(),
        statement: "select $1".to_string(),
        params: vec![SqlValue::Int8(1), SqlValue::Text("a".to_string())],
        exactly_once: false,
    });
    let object_op = effects::object_store(&ObjectRequest {
        connection: "blobs".to_string(),
        op: "get".to_string(),
        key: "k1".to_string(),
        body: None,
    });
    let sleep_op = effects::sleep(1_000);
    let until_op = effects::sleep_until(Datetime {
        seconds: now.seconds + 3_600,
        nanoseconds: 0,
    });
    let signal_op = effects::wait_signal("go", Some(10_000));
    let child_op = effects::start_child(
        "child-wf",
        &text("child-input"),
        &ChildOptions {
            queue: None,
            version: Some(1),
            detached: false,
            run_timeout_ms: None,
        },
    );
    let tok = effects::create_await_token(Some(60_000));
    effects::cancel_op(until_op);
    let _ = writeln!(
        out,
        "ops http={http_op} sql={sql_op} object={object_op} sleep={sleep_op} until={until_op} \
         signal={signal_op} child={child_op} token={}:{}",
        tok.op, tok.token
    );

    for c in effects::wait(&[http_op, sql_op, object_op], WaitMode::All) {
        dump_completion(&mut out, &c);
    }
    for c in effects::wait(&[effects::cancellation(), sleep_op], WaitMode::Any) {
        dump_completion(&mut out, &c);
    }
    Ok(RunResult::Completed(text(out)))
}

fn echo_bytes() -> Result<RunResult, Failure> {
    let r = step("bytes", || {
        Ok(Payload {
            content_type: BYTES.to_string(),
            data: context::random_bytes(256),
        })
    })?;
    Ok(RunResult::Completed(r))
}

fn cancel_cleanup() -> Result<RunResult, Failure> {
    let long = effects::sleep(3_600_000);
    let done = effects::wait(&[effects::cancellation(), long], WaitMode::Any);
    let cancelled = done
        .iter()
        .any(|c| matches!(c.outcome, Ok(OpOutput::CancelRequested)));
    if cancelled {
        let _ = step("cleanup", || Ok(text("cleaned")));
        Err(Failure::Cancelled)
    } else {
        Ok(RunResult::Completed(text("slept")))
    }
}

fn can(input: Payload) -> Result<RunResult, Failure> {
    let mut b = [0u8; 4];
    let n = input.data.len().min(4);
    b[..n].copy_from_slice(&input.data[..n]);
    let counter = u32::from_le_bytes(b);
    let suggested = context::continue_as_new_suggested();
    if counter < 3 {
        Ok(RunResult::ContinueAsNew(Continuation {
            input: Payload {
                content_type: BYTES.to_string(),
                data: (counter + 1).to_le_bytes().to_vec(),
            },
            workflow: None,
            version: None,
        }))
    } else {
        Ok(RunResult::Completed(text(format!(
            "counter={counter} suggested={suggested}"
        ))))
    }
}

fn token() -> Result<RunResult, Failure> {
    let tok = effects::create_await_token(Some(60_000));
    log::emit(
        Level::Info,
        "await token",
        &[("token".to_string(), tok.token.clone())],
    );
    for c in effects::wait(&[tok.op], WaitMode::All) {
        match c.outcome {
            Ok(OpOutput::Token(p)) => return Ok(RunResult::Completed(p)),
            Ok(_) => {}
            Err(f) => return Err(f),
        }
    }
    Err(Failure::NonRetryable(ErrorInfo {
        kind: "no-token".to_string(),
        message: "token wait returned no token completion".to_string(),
        details: None,
    }))
}

fn status(input: Payload) -> Result<RunResult, Failure> {
    let s = String::from_utf8_lossy(&input.data).into_owned();
    context::set_status(&s);
    context::set_status(&format!("{s}!"));
    Ok(RunResult::Completed(text("ok")))
}

fn non_retryable() -> Result<RunResult, Failure> {
    Err(Failure::NonRetryable(ErrorInfo {
        kind: "bad-input".to_string(),
        message: "input rejected".to_string(),
        details: None,
    }))
}

fn step_fail() -> Result<RunResult, Failure> {
    step("flaky", || {
        Err(Failure::Retryable(ErrorInfo {
            kind: "flaky".to_string(),
            message: "step failed".to_string(),
            details: None,
        }))
    })
    .map(RunResult::Completed)
}

fn nested_step() -> Result<RunResult, Failure> {
    match journal::begin_step("outer", &opts()) {
        StepStart::Replayed(r) => r.map(RunResult::Completed),
        StepStart::Execute(s) => {
            // Journaled command inside an open step: the host must trap.
            let _ = effects::sleep(10);
            s.complete(Ok(&text("unreachable")));
            Ok(RunResult::Completed(text("unreachable")))
        }
    }
}

fn info() -> Result<RunResult, Failure> {
    let i = context::info();
    let deadline = i
        .deadline
        .map(|d| format!("{}.{}", d.seconds, d.nanoseconds))
        .unwrap_or_else(|| "none".to_string());
    Ok(RunResult::Completed(text(format!(
        "run-id={} workflow={} version={} attempt={} module-hash={} tenant={:?} parent={:?} \
         deadline={} continued-from={:?}",
        i.run_id,
        i.workflow,
        i.workflow_version,
        i.attempt,
        i.module_hash,
        i.tenant,
        i.parent_run_id,
        deadline,
        i.continued_from
    ))))
}

fn panic_wf() -> Result<RunResult, Failure> {
    let _ = step("before-panic", || Ok(text("ok")));
    panic!("boom");
}

fn exit_wf() -> Result<RunResult, Failure> {
    let _ = step("before-exit", || Ok(text("ok")));
    std::process::exit(7);
}

struct Surface;

impl Guest for Surface {
    fn describe() -> Vec<WorkflowMeta> {
        WORKFLOWS
            .iter()
            .map(|n| WorkflowMeta {
                name: (*n).to_string(),
                version: 1,
                default_queue: None,
                run_timeout_ms: if *n == "surface" { Some(30_000) } else { None },
            })
            .collect()
    }

    fn run(name: String, input: Payload) -> Result<RunResult, Failure> {
        match name.as_str() {
            "surface" => surface(),
            "echo-bytes" => echo_bytes(),
            "cancel-cleanup" => cancel_cleanup(),
            "can" => can(input),
            "token" => token(),
            "status" => status(input),
            "non-retryable" => non_retryable(),
            "step-fail" => step_fail(),
            "nested-step" => nested_step(),
            "info" => info(),
            "panic" => panic_wf(),
            "exit" => exit_wf(),
            other => Err(Failure::NonRetryable(ErrorInfo {
                kind: "unknown-workflow".to_string(),
                message: format!("unknown workflow: {other}"),
                details: None,
            })),
        }
    }
}

export!(Surface);
