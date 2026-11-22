//! `run_history` (API-04): a page of a run's journal, decoded and rendered.
//!
//! Rows come from `query::journal_rows` (`seq >= from_seq`, at most `limit + 1` to learn
//! whether another page follows); blob payloads are fetched (verified) and every row is
//! decoded with `decode_event`. Each event becomes a [`HistoryEvent`]:
//!
//! - `kind`: the snake_case event kind name ([`kind_name`]), e.g. `run_started`,
//!   `step_completed`, `status_published`, `run_completed`
//! - `ts_ms`: `ts_nanos / 1e6` (REST adds the RFC 3339 form)
//! - `summary`: [`EventBody::summary`] (never contains payload bytes)
//! - `details_json`: per-kind JSON ([`details`]). Payloads with a JSON content type are
//!   embedded as JSON; anything else is `{"content_type", "base64"}`. Step outcomes are
//!   `{"ok": <value>}` or `{"error": <failure>}`; failures are
//!   `{"type": "retryable" | "non_retryable" | "timeout" | "cancelled", "kind", "message"}`.
//!
//! - `attempt`: the stored `JournalEvent.attempt` (`epoch << 32 | n`, D-P11-18); `0`
//!   (apply-originated, or written before Phase 11) is `None` ([`attempt_of`]).
//!
//! The page size is `effective_limit(limit)` (default 100, max 1000); `next_from_seq` is
//! the seq of the first event not returned.

use ostro_api::ApiError;
use ostro_api::domain::{HistoryEvent, HistoryPage, HistoryRequest, effective_limit};
use ostro_partition::query;
use ostro_types::journal::{
    AttemptBackoff, EffectRequest, EventBody, EventKind, Failure, Payload, decode_event,
    decode_result,
};
use serde_json::{Value, json};

use super::ServerControlPlane;
use super::runs::{not_found, parse_run_id, partition_error, store_error, task_error};

/// snake_case name of an event kind (stable API strings).
pub fn kind_name(k: EventKind) -> &'static str {
    match k {
        EventKind::RunStarted => "run_started",
        EventKind::StepCompleted => "step_completed",
        EventKind::TimerStarted => "timer_started",
        EventKind::EffectScheduled => "effect_scheduled",
        EventKind::SignalWaitStarted => "signal_wait_started",
        EventKind::ChildStarted => "child_started",
        EventKind::AwaitTokenCreated => "await_token_created",
        EventKind::OpCancelled => "op_cancelled",
        EventKind::WaitResolved => "wait_resolved",
        EventKind::PatchMarker => "patch_marker",
        EventKind::StatusPublished => "status_published",
        EventKind::RunCompleted => "run_completed",
        EventKind::RunFailed => "run_failed",
        EventKind::RunContinuedAsNew => "run_continued_as_new",
        EventKind::StepAttemptFailed => "step_attempt_failed",
        EventKind::TimerFired => "timer_fired",
        EventKind::EffectCompleted => "effect_completed",
        EventKind::SignalReceived => "signal_received",
        EventKind::ChildCompleted => "child_completed",
        EventKind::TokenCompleted => "token_completed",
        EventKind::CancelRequested => "cancel_requested",
        // Phase 5 kinds (plan 05-01).
        EventKind::WaitTimedOut => "wait_timed_out",
        EventKind::RunTerminated => "run_terminated",
        EventKind::SignalPayload => "signal_payload",
        EventKind::SignalMatched => "signal_matched",
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard padded base64 (RFC 4648 section 4).
pub fn base64(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(B64[((n >> (18 - 6 * i)) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn is_json(content_type: &str) -> bool {
    let ct = content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    ct == "application/json" || ct.ends_with("+json")
}

/// A payload as JSON: embedded when its content type is JSON and it parses, else base64.
pub fn payload_json(p: &Payload) -> Value {
    if is_json(&p.content_type)
        && let Ok(v) = serde_json::from_slice::<Value>(&p.data)
    {
        return v;
    }
    json!({ "content_type": p.content_type, "base64": base64(&p.data) })
}

fn failure_json(f: &Failure) -> Value {
    match f {
        Failure::Retryable(i) | Failure::NonRetryable(i) => {
            let ty = if matches!(f, Failure::Retryable(_)) {
                "retryable"
            } else {
                "non_retryable"
            };
            let mut v = json!({ "type": ty, "kind": i.kind, "message": i.message });
            if let Some(d) = &i.details {
                v["details"] = payload_json(d);
            }
            v
        }
        Failure::Timeout => json!({ "type": "timeout", "kind": "timeout", "message": "timeout" }),
        Failure::Cancelled => {
            json!({ "type": "cancelled", "kind": "cancelled", "message": "cancelled" })
        }
    }
}

fn ms(nanos: u64) -> u64 {
    nanos / 1_000_000
}

/// Per-kind details of an event body.
pub fn details(body: &EventBody) -> Value {
    match body {
        EventBody::RunStarted(e) => json!({
            "workflow": e.workflow,
            "version": e.workflow_version,
            "module_sha256": e.module_hash,
            "accepted_at_ms": ms(e.accepted_at_nanos),
            "deadline_ms": e.deadline_nanos.map(ms),
            "tenant": e.tenant,
            "parent_run_id": e.parent_run_id,
            "input": payload_json(&e.input),
        }),
        EventBody::StepCompleted(e) => {
            let outcome = match decode_result::<Result<Payload, Failure>>(&e.outcome) {
                Ok(Ok(p)) => json!({ "ok": payload_json(&p) }),
                Ok(Err(f)) => json!({ "error": failure_json(&f) }),
                Err(err) => json!({ "undecodable": err.to_string() }),
            };
            json!({ "name": e.name, "outcome": outcome, "fuel_used": e.fuel_used })
        }
        EventBody::StepAttemptFailed(e) => json!({
            "name": e.name,
            "attempt": e.attempt,
            "failure": failure_json(&e.failure),
            "retry_at_ms": ms(e.retry_at_nanos),
            "backoff": match e.backoff {
                AttemptBackoff::InActivation => "in_activation",
                AttemptBackoff::Requeue => "requeue",
            },
            "fuel_used": e.fuel_used,
        }),
        EventBody::TimerStarted(e) => json!({ "fire_at_ms": ms(e.fire_at_nanos) }),
        EventBody::EffectScheduled(e) => match &e.request {
            EffectRequest::Http(r) => json!({ "effect": "http", "method": r.method, "url": r.url }),
            EffectRequest::Sql(r) => json!({ "effect": "sql", "connection": r.connection }),
            EffectRequest::Object(r) => json!({
                "effect": "object", "connection": r.connection, "op": r.op, "key": r.key,
            }),
        },
        EventBody::SignalWaitStarted(e) => json!({ "name": e.name, "timeout_ms": e.timeout_ms }),
        EventBody::ChildStarted(e) => json!({
            "workflow": e.workflow,
            "queue": e.options.queue,
            "version": e.options.version,
            "detached": e.options.detached,
            "input": payload_json(&e.input),
        }),
        EventBody::AwaitTokenCreated(e) => json!({ "timeout_ms": e.timeout_ms }),
        EventBody::OpCancelled(e) => json!({ "op": e.op }),
        EventBody::WaitResolved(e) => json!({ "completed": e.completed }),
        EventBody::PatchMarker(e) => json!({ "change_id": e.change_id }),
        EventBody::StatusPublished(e) => json!({
            "status": serde_json::from_str::<Value>(&e.json)
                .unwrap_or_else(|_| Value::String(e.json.clone())),
        }),
        EventBody::RunCompleted(e) => json!({ "output": payload_json(&e.output) }),
        EventBody::RunFailed(e) => json!({ "failure": failure_json(&e.failure) }),
        EventBody::RunContinuedAsNew(e) => json!({
            "workflow": e.continuation.workflow,
            "version": e.continuation.version,
        }),
        EventBody::TimerFired(e) => json!({ "op": e.op }),
        EventBody::EffectCompleted(e) => json!({ "op": e.op }),
        EventBody::SignalReceived(e) => json!({ "op": e.op, "payload": payload_json(&e.payload) }),
        EventBody::ChildCompleted(e) => json!({ "op": e.op }),
        EventBody::TokenCompleted(e) => json!({ "op": e.op, "payload": payload_json(&e.payload) }),
        EventBody::CancelRequested(e) => json!({ "reason": e.reason }),
        // Phase 5 kinds (plan 05-01).
        EventBody::WaitTimedOut(e) => json!({ "op": e.op }),
        EventBody::RunTerminated(e) => json!({ "reason": e.reason }),
        EventBody::SignalPayload(e) => {
            json!({ "name": e.name, "payload": payload_json(&e.payload) })
        }
        EventBody::SignalMatched(e) => json!({ "op": e.op, "payload_seq": e.payload_seq }),
    }
}

/// [`details`] plus, on `RunStarted`, the run's `retry_of` (`StartRun.retry_of` from the
/// run record: the source of a restart or rerun, 11-10) when it has one. The key is
/// omitted otherwise, so histories of ordinary runs are unchanged.
fn run_details(body: &EventBody, retry_of: Option<&ostro_types::RunId>) -> Value {
    let mut d = details(body);
    if let (EventBody::RunStarted(_), Some(src), Value::Object(m)) = (body, retry_of, &mut d) {
        m.insert("retry_of".to_owned(), Value::String(src.to_string()));
    }
    d
}

/// The API view of a stored `JournalEvent.attempt`: 0 (unset) is `None`.
pub fn attempt_of(stored: u64) -> Option<u64> {
    (stored != 0).then_some(stored)
}

/// `seq -> JournalEvent.attempt` of the rows [`query::journal_rows`] returns for the same
/// arguments. `query` exposes only `(seq, kind, payload)`, and this crate has no prost
/// dependency, so the two fields are read from the protobuf encoding directly
/// ([`event_seq_and_attempt`]).
fn journal_attempts(
    reader: &ostro_partition::store::StoreReader,
    run: &ostro_types::RunId,
    from_seq: u64,
    limit: usize,
) -> Result<std::collections::HashMap<u64, u64>, ostro_partition::StoreError> {
    let from = ostro_partition::store::keys::event_key(run, from_seq);
    let mut to = run.as_bytes().to_vec();
    to.extend_from_slice(&[0xff; 9]);
    let rows = reader.range(ostro_partition::store::Table::Events, &from, &to, limit)?;
    Ok(rows
        .iter()
        .filter_map(|(_, v)| event_seq_and_attempt(v))
        .collect())
}

/// `(seq, attempt)` of an encoded `JournalEvent` (field 1 varint, field 4 fixed64; other
/// fields skipped by wire type). `None` when the bytes are not a well-formed message.
pub fn event_seq_and_attempt(mut b: &[u8]) -> Option<(u64, u64)> {
    fn varint(b: &mut &[u8]) -> Option<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let (&x, rest) = b.split_first()?;
            *b = rest;
            v |= u64::from(x & 0x7f) << shift;
            if x & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    }
    fn skip(b: &mut &[u8], n: usize) -> Option<()> {
        *b = b.get(n..)?;
        Some(())
    }
    let mut seq = 0;
    let mut attempt = 0;
    while !b.is_empty() {
        let key = varint(&mut b)?;
        match (key >> 3, key & 7) {
            (1, 0) => seq = varint(&mut b)?,
            (4, 1) => {
                let raw: [u8; 8] = b.get(..8)?.try_into().ok()?;
                attempt = u64::from_le_bytes(raw);
                skip(&mut b, 8)?;
            }
            (_, 0) => {
                varint(&mut b)?;
            }
            (_, 1) => skip(&mut b, 8)?,
            (_, 2) => {
                let n = usize::try_from(varint(&mut b)?).ok()?;
                skip(&mut b, n)?;
            }
            (_, 5) => skip(&mut b, 4)?,
            _ => return None,
        }
    }
    Some((seq, attempt))
}

pub(crate) async fn run_history(
    cp: &ServerControlPlane,
    req: HistoryRequest,
) -> Result<HistoryPage, ApiError> {
    let run = parse_run_id(cp, &req.run_id)?;
    let limit = effective_limit(req.limit) as usize;
    let handle = cp.partition(run.partition())?;
    let reader = handle.reader();
    let from = req.from_seq;
    let rows = tokio::task::spawn_blocking(move || {
        let Some(rec) = query::run_record(&reader, &run)? else {
            return Ok(None);
        };
        let rows = query::journal_rows(&reader, &run, from, limit + 1)?;
        let attempts = journal_attempts(&reader, &run, from, limit + 1)?;
        Ok(Some((rows, attempts, rec.retry_of)))
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?
    .ok_or_else(|| not_found(&req.run_id))?;

    let (mut rows, attempts, retry_of) = rows;
    let next_from_seq = (rows.len() > limit).then(|| rows[limit].0);
    rows.truncate(limit);
    let mut events = Vec::with_capacity(rows.len());
    for (seq, kind, payload) in rows {
        let bytes = handle
            .fetch_payload(&payload)
            .await
            .map_err(partition_error)?;
        let ev = decode_event(seq, kind, &bytes)
            .map_err(|e| ApiError::internal(format!("run {run} seq {seq}: decode: {e}")))?;
        events.push(HistoryEvent {
            seq,
            kind: kind_name(ev.body.kind()).to_owned(),
            ts_ms: ms(ev.ts_nanos),
            summary: ev.body.summary(),
            details_json: run_details(&ev.body, retry_of.as_ref())
                .to_string()
                .into_bytes(),
            attempt: attempts.get(&seq).copied().and_then(attempt_of),
        });
    }
    Ok(HistoryPage {
        events,
        next_from_seq,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt_is_read_from_the_encoded_journal_event() {
        // seq = 300 (varint 0xac 0x02), kind = 7, payload = 3 bytes, attempt = (2 << 32) | 5.
        let attempt: u64 = (2 << 32) | 5;
        let mut b = vec![0x08, 0xac, 0x02, 0x10, 0x07, 0x1a, 0x03, 1, 2, 3, 0x21];
        b.extend_from_slice(&attempt.to_le_bytes());
        assert_eq!(event_seq_and_attempt(&b), Some((300, attempt)));
        // Without field 4 (pre-Phase 11 or apply-originated): 0 -> None.
        assert_eq!(event_seq_and_attempt(&b[..10]), Some((300, 0)));
        assert_eq!(attempt_of(0), None);
        assert_eq!(attempt_of(attempt), Some(attempt));
        // Unknown fields of every wire type are skipped.
        let mut u = vec![
            0x08, 0x01, 0x29, 0, 0, 0, 0, 0, 0, 0, 0, 0x35, 0, 0, 0, 0, 0x21,
        ];
        u.extend_from_slice(&9u64.to_le_bytes());
        assert_eq!(event_seq_and_attempt(&u), Some((1, 9)));
        // Truncated input.
        assert_eq!(event_seq_and_attempt(&b[..b.len() - 1]), None);
    }

    #[test]
    fn base64_matches_rfc4648_vectors() {
        for (i, o) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(i.as_bytes()), o);
        }
    }

    #[test]
    fn payloads_embed_json_else_base64() {
        let j = Payload {
            content_type: "application/json; charset=utf-8".into(),
            data: br#"{"a":1}"#.to_vec(),
        };
        assert_eq!(payload_json(&j), json!({"a": 1}));
        let b = Payload {
            content_type: "application/octet-stream".into(),
            data: b"hi".to_vec(),
        };
        assert_eq!(
            payload_json(&b),
            json!({"content_type": "application/octet-stream", "base64": "aGk="})
        );
        let bad = Payload {
            content_type: "application/json".into(),
            data: b"{nope".to_vec(),
        };
        assert_eq!(payload_json(&bad)["base64"], base64(b"{nope"));
    }
}
