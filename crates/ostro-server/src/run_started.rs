//! Synthesis of a run's first journal event, `RunStarted` (seq 0), from its `RunRecord`.
//!
//! The executor (04-16) appends it through the sink before the first activation when the
//! run's journal is empty (`RunRecord.next_seq == 0`). Every field comes from the record
//! that `StartRun` wrote, so a re-dispatch after a crash synthesises identical bytes.

use ostro_partition::store::values::RunRecord;
use ostro_types::RunId;
use ostro_types::journal::{Event, EventBody, Payload, RngSeed, RunStarted};

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
/// - `tenant` from the record; `parent_run_id` = the parent's run id string;
///   `continued_from` = None (continue-as-new is Phase 5)
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
    Ok(Event {
        seq: 0,
        ts_nanos: accepted,
        body: EventBody::RunStarted(RunStarted {
            workflow: rec.workflow.clone(),
            workflow_version: rec.workflow_version,
            input,
            rng_seed: RngSeed(seed),
            accepted_at_nanos: accepted,
            deadline_nanos: deadline,
            module_hash: module_hash_hex.to_string(),
            tenant: rec.tenant.clone(),
            parent_run_id,
            continued_from: None,
        }),
    })
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
    fn bad_seed_is_error() {
        let mut rec = record();
        rec.rng_seed = vec![1; 7];
        assert_eq!(
            build_run_started(&rec, input_payload(vec![]), "h"),
            Err(RunStartedError::BadSeed(7))
        );
    }
}
