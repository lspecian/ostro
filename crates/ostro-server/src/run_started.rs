//! Synthesis of a run's first journal event, `RunStarted` (seq 0), from its `RunRecord`.
//!
//! The executor (04-16) appends it through the sink before the first activation when the
//! run's journal is empty (`RunRecord.next_seq == 0`). Every field comes from the record
//! that `StartRun` wrote, so a re-dispatch after a crash synthesises identical bytes.
//!
//! Phase 5 (05-11):
//! - **D23 journal form.** `RunRecord.workflow` is the tenant-qualified storage key
//!   (`<tenant>/<name>`). `RunStarted.workflow` is the **unqualified** module-local name
//!   (the engine passes it to the guest's `run(name, ...)`), with the tenant in
//!   `RunStarted.tenant`. The prefix is stripped with the API's own rule
//!   ([`crate::control::names::split_qualified`]).
//! - `RunStarted.continued_from` is the predecessor of a continue-as-new successor.
//! - [`first_events`]: a run whose cancel was requested before its first activation
//!   (`RunRecord.cancel_requested`; apply journals nothing while `next_seq == 0`, 05-08)
//!   gets `CancelRequested{cancel_reason}` at seq 1 in the same flush as `RunStarted`, so the
//!   guest's `cancellation()` resolves.
//!
//! Plan 05-16: [`encode_for_start`] pre-encodes the same `RunStarted` from a `StartRun`
//! (through [`record_of_start`], the record apply will store), for `StartRun.run_started`
//! (tag 23): apply then journals it at seq 0 and the executor skips its own append
//! (`next_seq > 0`). The bytes are identical to the executor's synthesis for the same run,
//! so either path yields the same journal.

use ostro_partition::store::values::RunRecord;
use ostro_types::RunId;
use ostro_types::command::StartRun;
use ostro_types::journal::{
    CancelRequested, Event, EventBody, Payload, RngSeed, RunStarted, encode_event,
};

use crate::control::names::split_qualified;

/// Content type of run inputs and outputs (WF-01: typed JSON in/out). The partition stores
/// only payload bytes, so the input's content type is fixed here.
pub const JSON_CONTENT_TYPE: &str = "application/json";

/// Why a record cannot produce a `RunStarted`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RunStartedError {
    #[error("run record rng_seed is {0} bytes, expected 32")]
    BadSeed(usize),
    #[error("run record parent id is malformed: {0}")]
    BadParent(String),
}

/// The run input as the engine sees it: the stored bytes with [`JSON_CONTENT_TYPE`].
pub fn input_payload(bytes: impl Into<Vec<u8>>) -> Payload {
    Payload {
        content_type: JSON_CONTENT_TYPE.to_string(),
        data: bytes.into(),
    }
}

/// Build `RunStarted` (seq 0) for `rec`.
///
/// - `accepted_at_nanos` = `ts_nanos` = `rec.created_at_ms * 1e6` (`StartRun.requested_at_ms`)
/// - `deadline_nanos` = accepted + `run_timeout_ms * 1e6` when the record has a timeout
/// - `module_hash` = `module_hash_hex` (lowercase hex of `rec.module_sha`, passed by the
///   caller, which has already resolved the module)
/// - `workflow` = the module-local part of `rec.workflow` (D23); `tenant` = the record's
///   tenant, else the qualified name's prefix; `parent_run_id` = the parent's run id string;
///   `continued_from` = the predecessor's run id string
pub fn build_run_started(
    rec: &RunRecord,
    input: Payload,
    module_hash_hex: &str,
) -> Result<Event, RunStartedError> {
    let seed: [u8; 32] = rec
        .rng_seed
        .as_slice()
        .try_into()
        .map_err(|_| RunStartedError::BadSeed(rec.rng_seed.len()))?;
    let parent_run_id = rec
        .parent
        .map(|(id, _)| {
            RunId::from_bytes(&id)
                .map(|r| r.to_string())
                .map_err(|e| RunStartedError::BadParent(e.to_string()))
        })
        .transpose()?;
    let accepted = rec.created_at_ms.saturating_mul(1_000_000);
    let deadline = rec
        .run_timeout_ms
        .map(|ms| accepted.saturating_add(ms.saturating_mul(1_000_000)));
    let (prefix, workflow) = split_qualified(&rec.workflow);
    let tenant = rec.tenant.clone().or_else(|| prefix.map(str::to_owned));
    Ok(Event {
        seq: 0,
        ts_nanos: accepted,
        body: EventBody::RunStarted(RunStarted {
            workflow: workflow.to_owned(),
            workflow_version: rec.workflow_version,
            input,
            rng_seed: RngSeed(seed),
            accepted_at_nanos: accepted,
            deadline_nanos: deadline,
            module_hash: module_hash_hex.to_string(),
            tenant,
            parent_run_id,
            continued_from: rec.continued_from.map(|r| r.to_string()),
        }),
    })
}

/// The fields of the [`RunRecord`] apply stores for `s` that [`build_run_started`] reads
/// (workflow, version, created_at, seed, timeout, tenant, parent, continued_from); every
/// other field is default. Malformed id fields are carried as absent (apply rejects them).
pub fn record_of_start(s: &StartRun) -> RunRecord {
    let opt_id = |b: &[u8]| (!b.is_empty()).then(|| RunId::from_bytes(b).ok()).flatten();
    RunRecord {
        workflow: s.workflow.clone(),
        workflow_version: s.workflow_version,
        queue: s.queue.clone(),
        created_at_ms: s.requested_at_ms,
        rng_seed: s.rng_seed.clone(),
        run_timeout_ms: (s.run_timeout_ms != 0).then_some(s.run_timeout_ms),
        tenant: (!s.tenant.is_empty()).then(|| s.tenant.clone()),
        parent: s
            .parent
            .as_ref()
            .and_then(|p| p.run_id().ok().map(|id| (*id.as_bytes(), p.op))),
        continued_from: opt_id(&s.continued_from),
        module_sha: <[u8; 32]>::try_from(s.module_sha.as_slice()).ok(),
        ..Default::default()
    }
}

/// `(kind, bytes)` of the `RunStarted` a fresh run started by `s` journals at seq 0, for
/// `StartRun.run_started` (05-16). `input` is the run input (JSON bytes).
pub fn encode_for_start(s: &StartRun, input: &[u8]) -> Result<(u32, Vec<u8>), RunStartedError> {
    let rec = record_of_start(s);
    let hash = rec
        .module_sha
        .map(|h| ostro_blob::hex(&h))
        .unwrap_or_default();
    let ev = build_run_started(&rec, input_payload(input.to_vec()), &hash)?;
    Ok(encode_event(&ev))
}

/// The first flush of a run: `RunStarted` (seq 0), plus `CancelRequested{cancel_reason}` at
/// seq 1 (same ts) when the cancel was requested before the first activation.
pub fn first_events(
    rec: &RunRecord,
    input: Payload,
    module_hash_hex: &str,
) -> Result<Vec<Event>, RunStartedError> {
    let started = build_run_started(rec, input, module_hash_hex)?;
    let ts = started.ts_nanos;
    let mut out = vec![started];
    if rec.cancel_requested {
        out.push(Event {
            seq: 1,
            ts_nanos: ts,
            body: EventBody::CancelRequested(CancelRequested {
                reason: rec.cancel_reason.clone(),
            }),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use ostro_engine::sink::RunRef;
    use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, RunSpec};
    use ostro_partition::store::values::RunStatus;
    use ostro_types::PartitionId;

    use super::*;

    fn record() -> RunRecord {
        RunRecord {
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "default".into(),
            status: RunStatus::Ready,
            current_attempt: None,
            next_seq: 0,
            ready: None,
            parent: None,
            cancel_requested: false,
            concurrency_key: String::new(),
            priority: 0,
            created_at_ms: DEFAULT_ACCEPTED_AT / 1_000_000,
            rng_seed: vec![0x5a; 32],
            inbox_next: 0,
            children: 0,
            module_sha: Some([0xab; 32]),
            tags: vec![],
            tenant: None,
            traceparent: None,
            run_timeout_ms: None,
            completed_at_ms: None,
            ..Default::default()
        }
    }

    fn spec() -> RunSpec {
        RunSpec::new(
            RunRef {
                partition: 0,
                run_id: "r".into(),
            },
            "wf",
            b"{\"n\":1}",
        )
    }

    #[test]
    fn matches_testkit_run_started() {
        let hash = ostro_blob::hex(&[0xab; 32]);
        let s = spec();
        let got = build_run_started(&record(), s.input.clone(), &hash).unwrap();
        assert_eq!(got, s.run_started_event(&hash));
    }

    #[test]
    fn deadline_from_run_timeout() {
        let mut rec = record();
        rec.run_timeout_ms = Some(5_000);
        let mut s = spec();
        s.deadline_nanos = Some(DEFAULT_ACCEPTED_AT + 5_000_000_000);
        let got = build_run_started(&rec, s.input.clone(), "h").unwrap();
        assert_eq!(got, s.run_started_event("h"));
    }

    #[test]
    fn tenant_parent_version_and_json_input() {
        let mut rec = record();
        rec.tenant = Some("acme".into());
        rec.workflow_version = 3;
        let parent = RunId::new_v7(PartitionId(2));
        rec.parent = Some((*parent.as_bytes(), 7));
        let ev = build_run_started(&rec, input_payload(b"{}".to_vec()), "h").unwrap();
        let EventBody::RunStarted(rs) = ev.body else {
            panic!("not RunStarted")
        };
        assert_eq!(rs.tenant.as_deref(), Some("acme"));
        assert_eq!(rs.workflow_version, 3);
        assert_eq!(rs.parent_run_id, Some(parent.to_string()));
        assert_eq!(rs.input.content_type, "application/json");
        assert_eq!(rs.continued_from, None);
    }

    #[test]
    fn qualified_workflow_is_stripped_to_the_module_local_name() {
        let mut rec = record();
        rec.workflow = "acme/w".into();
        rec.tenant = Some("acme".into());
        let EventBody::RunStarted(rs) = build_run_started(&rec, input_payload(vec![]), "h")
            .unwrap()
            .body
        else {
            panic!("not RunStarted")
        };
        assert_eq!(rs.workflow, "w");
        assert_eq!(rs.tenant.as_deref(), Some("acme"));
        // A record without a tenant takes it from the qualified name.
        rec.tenant = None;
        let EventBody::RunStarted(rs) = build_run_started(&rec, input_payload(vec![]), "h")
            .unwrap()
            .body
        else {
            panic!("not RunStarted")
        };
        assert_eq!(
            (rs.workflow.as_str(), rs.tenant.as_deref()),
            ("w", Some("acme"))
        );
        // Default-tenant names are unchanged; a non-tenant prefix is not stripped.
        for name in ["w", "Bad/w"] {
            rec.workflow = name.into();
            let EventBody::RunStarted(rs) = build_run_started(&rec, input_payload(vec![]), "h")
                .unwrap()
                .body
            else {
                panic!("not RunStarted")
            };
            assert_eq!((rs.workflow.as_str(), rs.tenant.as_deref()), (name, None));
        }
    }

    #[test]
    fn continued_from_and_cancel_before_start() {
        let mut rec = record();
        let pred = RunId::new_v7(PartitionId(1));
        rec.continued_from = Some(pred);
        let evs = first_events(&rec, input_payload(vec![]), "h").unwrap();
        assert_eq!(evs.len(), 1);
        let EventBody::RunStarted(rs) = &evs[0].body else {
            panic!("not RunStarted")
        };
        assert_eq!(rs.continued_from, Some(pred.to_string()));
        rec.cancel_requested = true;
        rec.cancel_reason = "user".into();
        let evs = first_events(&rec, input_payload(vec![]), "h").unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!((evs[1].seq, evs[1].ts_nanos), (1, evs[0].ts_nanos));
        assert_eq!(
            evs[1].body,
            EventBody::CancelRequested(CancelRequested {
                reason: "user".into()
            })
        );
    }

    /// 05-16: the bytes pre-encoded from a StartRun equal the executor's synthesis from the
    /// record apply stores for it (tenant-qualified name, parent, continued_from, timeout).
    #[test]
    fn start_run_pre_encoding_matches_the_executor_synthesis() {
        let parent = RunId::new_v7(PartitionId(2));
        let pred = RunId::new_v7(PartitionId(1));
        let s = StartRun {
            run_id: RunId::new_v7(PartitionId(0)).as_bytes().to_vec(),
            workflow: "acme/w".into(),
            workflow_version: 4,
            queue: "acme/q".into(),
            requested_at_ms: 1_700_000_000_123,
            rng_seed: vec![9; 32],
            module_sha: vec![0xcd; 32],
            tenant: "acme".into(),
            run_timeout_ms: 30_000,
            parent: Some(ostro_types::command::ParentRef {
                run_id: parent.as_bytes().to_vec(),
                op: 7,
            }),
            continued_from: pred.as_bytes().to_vec(),
            ..Default::default()
        };
        let input = b"{\"n\":1}";
        let (kind, bytes) = encode_for_start(&s, input).unwrap();
        let mut rec = record();
        rec.workflow = "acme/w".into();
        rec.workflow_version = 4;
        rec.created_at_ms = s.requested_at_ms;
        rec.rng_seed = vec![9; 32];
        rec.module_sha = Some([0xcd; 32]);
        rec.tenant = Some("acme".into());
        rec.run_timeout_ms = Some(30_000);
        rec.parent = Some((*parent.as_bytes(), 7));
        rec.continued_from = Some(pred);
        let evs = first_events(
            &rec,
            input_payload(input.to_vec()),
            &ostro_blob::hex(&[0xcd; 32]),
        )
        .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(encode_event(&evs[0]), (kind, bytes));
        assert_eq!(kind, ostro_types::journal::EventKind::RunStarted.code());
    }

    #[test]
    fn bad_seed_is_error() {
        let mut rec = record();
        rec.rng_seed = vec![1; 7];
        assert_eq!(
            build_run_started(&rec, input_payload(vec![]), "h"),
            Err(RunStartedError::BadSeed(7))
        );
    }
}
