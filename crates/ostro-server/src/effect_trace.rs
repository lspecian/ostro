//! Outbound trace propagation and effect spans (plan 08-21, OBS-01 / P8-D24).
//!
//! [`TraceEffectHooks`] implements 06-14's [`EffectHooks`] seam of the
//! [`ServerEffectDriver`](crate::effects::ServerEffectDriver):
//!
//! - [`decorate_live_request`](EffectHooks::decorate_live_request) appends the W3C
//!   `traceparent` of the effect's deterministic span
//!   ([`effect_span_context`]`(run, record.traceparent, op)`) to the **live** HTTP request
//!   copy. The intent was journaled before the hook runs, so the journal bytes never carry
//!   it, and a re-driven effect (crash before completion, replay after a restart) sends the
//!   same header, because the span id depends only on `(run, op)`. A `traceparent` the guest
//!   set itself wins: it is left untouched (the effect span is still emitted).
//! - [`on_effect_complete`](EffectHooks::on_effect_complete) emits
//!   [`effect_span`] (`effect:<kind>`, parent = the run's `workflow.run` span) with the
//!   driver's start and end times. The run's traceparent is read from its record; a run
//!   whose record is gone falls back to the trace id derived from the run id.
//!
//! The server installs the hooks only when OTLP traces are on (`[telemetry] otlp_endpoint`
//! + `traces`); otherwise the driver keeps the no-op hooks and outbound requests are
//!   byte-identical to the journaled intent.

use std::sync::Arc;
use std::time::SystemTime;

use ostro_engine::sink::RunRef;
use ostro_engine::telemetry::{RunTrace, effect_span, effect_span_context, traceparent_headers};
use ostro_partition::store::values::RunRecord;
use ostro_types::RunId;
use ostro_types::journal::{Failure, Header, HttpRequest, OpOutput, RecordedResult, decode_result};
use ostro_types::live::EffectKind;

use crate::effects::{EffectHooks, PartitionTable};

/// The OBS-01 effect hooks: outbound `traceparent` on the live request copy and one
/// `effect:<kind>` span per completed effect.
#[derive(Debug)]
pub struct TraceEffectHooks {
    partitions: Arc<PartitionTable>,
}

impl TraceEffectHooks {
    /// Hooks reading run records (for the stored traceparent) through `partitions`.
    pub fn new(partitions: Arc<PartitionTable>) -> Self {
        Self { partitions }
    }

    /// The stored traceparent of `run`, when its record is readable.
    fn traceparent_of(&self, run: &RunRef) -> Option<String> {
        let h = self.partitions.get(run.partition)?;
        let id: RunId = run.run_id.parse().ok()?;
        h.read_run(&id).ok().flatten()?.traceparent
    }
}

/// The span name suffix of `kind` (`effect:http`, `effect:sql`, `effect:object`).
pub fn kind_name(kind: EffectKind) -> &'static str {
    match kind {
        EffectKind::Http => "http",
        EffectKind::Sql => "sql",
        EffectKind::Object => "object",
    }
}

/// `Ok(())` for a successful outcome, else the failure kind (bounded: the connector's error
/// kind, `timeout`, `cancelled`, or `undecodable`).
fn outcome_of(r: &RecordedResult) -> Result<(), String> {
    match decode_result::<Result<OpOutput, Failure>>(r) {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(Failure::Retryable(e) | Failure::NonRetryable(e))) => Err(e.kind),
        Ok(Err(Failure::Timeout)) => Err("timeout".into()),
        Ok(Err(Failure::Cancelled)) => Err("cancelled".into()),
        Err(_) => Err("undecodable".into()),
    }
}

/// Append the effect's propagation headers to `req` unless it already carries a
/// `traceparent` (the guest's own header wins).
pub fn add_traceparent(req: &mut HttpRequest, run_id: &str, traceparent: Option<&str>, op: u64) {
    if req
        .headers
        .iter()
        .any(|h| h.name.eq_ignore_ascii_case("traceparent"))
    {
        return;
    }
    let cx = effect_span_context(run_id, traceparent, op);
    req.headers.extend(
        traceparent_headers(&cx)
            .into_iter()
            .map(|(name, value)| Header { name, value }),
    );
}

impl EffectHooks for TraceEffectHooks {
    fn decorate_live_request(&self, run: &RunRef, rec: &RunRecord, op: u64, req: &mut HttpRequest) {
        add_traceparent(req, &run.run_id, rec.traceparent.as_deref(), op);
    }

    fn on_effect_complete(
        &self,
        run: &RunRef,
        op: u64,
        kind: EffectKind,
        started: SystemTime,
        ended: SystemTime,
        outcome: &RecordedResult,
    ) {
        let tp = self.traceparent_of(run);
        let trace = RunTrace::new(&run.run_id, tp.as_deref());
        let outcome = outcome_of(outcome);
        effect_span(
            &trace,
            op,
            kind_name(kind),
            started,
            ended,
            outcome.as_ref().map(|_| ()).map_err(String::as_str),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_engine::telemetry::{KIND_EFFECT, span_id};
    use ostro_types::journal::{ErrorInfo, encode_result};

    fn req(headers: Vec<Header>) -> HttpRequest {
        HttpRequest {
            method: "GET".into(),
            url: "http://127.0.0.1:1/x".into(),
            headers,
            body: None,
            timeout_ms: None,
            idempotency_key: None,
            redact: vec![],
        }
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn traceparent_is_appended_with_the_run_trace_and_effect_span() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let mut r = req(vec![Header {
            name: "accept".into(),
            value: "*/*".into(),
        }]);
        add_traceparent(&mut r, "run-1", Some(tp), 7);
        let got: Vec<_> = r
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case("traceparent"))
            .collect();
        assert_eq!(got.len(), 1, "{:?}", r.headers);
        assert_eq!(
            got[0].value,
            format!(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-{}-01",
                hex(&span_id("run-1", KIND_EFFECT, 7))
            )
        );
        assert_eq!(
            r.headers[0].name, "accept",
            "existing headers kept in order"
        );
        // Deterministic: the same (run, op) gives the same header.
        let mut again = req(vec![]);
        add_traceparent(&mut again, "run-1", Some(tp), 7);
        assert_eq!(again.headers.last(), r.headers.last());
    }

    #[test]
    fn a_guest_traceparent_wins() {
        let mine = Header {
            name: "TraceParent".into(),
            value: "00-11111111111111111111111111111111-2222222222222222-01".into(),
        };
        let mut r = req(vec![mine.clone()]);
        add_traceparent(&mut r, "run-1", None, 1);
        assert_eq!(r.headers, vec![mine]);
    }

    #[test]
    fn outcomes_map_to_bounded_kinds() {
        let ok: Result<OpOutput, Failure> = Ok(OpOutput::Object(vec![]));
        assert_eq!(outcome_of(&encode_result(&ok)), Ok(()));
        let err: Result<OpOutput, Failure> = Err(Failure::NonRetryable(ErrorInfo {
            kind: "egress_denied".into(),
            message: "no".into(),
            details: None,
        }));
        assert_eq!(
            outcome_of(&encode_result(&err)),
            Err("egress_denied".into())
        );
        let t: Result<OpOutput, Failure> = Err(Failure::Timeout);
        assert_eq!(outcome_of(&encode_result(&t)), Err("timeout".into()));
        let junk = RecordedResult {
            codec_ver: 1,
            bytes: vec![0xff, 0xff, 0xff],
        };
        assert_eq!(outcome_of(&junk), Err("undecodable".into()));
    }
}
