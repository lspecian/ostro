//! Replay a journal against a module and classify the result (Phase 10 D-P10-7 /
//! Phase 11 D-P11-5). Shared by the 05-12 replay corpus (`tests/replay_corpus.rs`), the
//! rerun control plane's mandatory pre-submit check (11-10) and Phase 10's CLI replay.
//!
//! [`replay_check`] activates `module` once over `journal` (attempt 1, as run `run`) on
//! `rt` with a private recording sink:
//!
//! - the sink performs no effect I/O ([`ActivationSink::effect_driver`] is `None`), stores
//!   no blobs and drops guest log lines;
//! - the **first** append past the journal end is refused with [`AppendError::Fenced`], so
//!   nothing live beyond the frontier ever runs (no second live step, no wait). The size of
//!   that refused batch is the verdict's `appended`;
//! - a journal is **terminal** when its last event is `RunCompleted`, `RunFailed` or
//!   `RunContinuedAsNew`; terminal vs frontier is derived from the journal, never passed in.
//!
//! Verdicts:
//!
//! | journal | activation | verdict |
//! |---|---|---|
//! | any | `NonDeterminism{seq}` | [`ReplayVerdict::Diverged`] at that seq |
//! | terminal | same terminal outcome as journaled, nothing appended | [`ReplayVerdict::Ok`] `{appended: 0}` |
//! | terminal | an append, or a different outcome | `Diverged` at the journal end / last seq |
//! | non-terminal | an append (refused), an eviction (wait), or any live outcome | [`ReplayVerdict::OkFrontier`] |
//! | any | protocol violation, or a malformed journal | [`ReplayVerdict::Error`] |
//!
//! For a non-terminal journal every non-nondeterminism outcome is "live progress past the
//! end": the replay of the prefix itself matched (a mismatch inside the prefix is always a
//! `NonDeterminism`), and what the guest does live afterwards (fail a step, trap, run out of
//! fuel) is the behaviour the real run will show.

use std::sync::Arc;
use std::sync::Mutex;

use ostro_types::journal::{Event, EventBody};
use tokio_util::sync::CancellationToken;

use crate::effects_driver::EffectDriver;
use crate::error::AttemptFailure;
use crate::runtime::{Prepared, Runtime};
use crate::sink::{
    Activate, ActivationOutcome, ActivationSink, AppendError, GuestLogRecord, RunRef,
};

/// The outcome of [`replay_check`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayVerdict {
    /// Terminal journal replayed to the same outcome; `appended` is always 0.
    Ok { appended: usize },
    /// Non-terminal journal: the prefix replayed and the guest made live progress past the
    /// end (`appended` = events in its first live append, refused by the check; 0 when it
    /// stopped at a wait or ended without appending).
    OkFrontier { appended: usize },
    /// The module does not reproduce the journal at `seq`.
    Diverged { seq: u64, detail: String },
    /// The check could not run (bad journal, protocol violation, engine error).
    Error(String),
}

impl ReplayVerdict {
    /// `Ok` or `OkFrontier`.
    pub fn passed(&self) -> bool {
        matches!(self, Self::Ok { .. } | Self::OkFrontier { .. })
    }
}

/// The outcome a terminal journal records (its last event), if it is terminal.
pub fn journaled_outcome(journal: &[Event]) -> Option<ActivationOutcome> {
    match journal.last().map(|e| &e.body) {
        Some(EventBody::RunCompleted(c)) => Some(ActivationOutcome::Completed {
            output: c.output.clone(),
        }),
        Some(EventBody::RunFailed(f)) => Some(ActivationOutcome::Failed {
            failure: f.failure.clone(),
        }),
        Some(EventBody::RunContinuedAsNew(c)) => Some(ActivationOutcome::ContinuedAsNew {
            continuation: c.continuation.clone(),
        }),
        _ => None,
    }
}

/// Recording sink: refuses (fences) the first append past the journal end.
struct CheckSink {
    /// Events in the first refused append (`None` until one happens).
    refused: Mutex<Option<(u64, usize)>>,
}

#[async_trait::async_trait]
impl ActivationSink for CheckSink {
    async fn append(
        &self,
        _run: &RunRef,
        _attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError> {
        let mut r = self.refused.lock().unwrap_or_else(|e| e.into_inner());
        if r.is_none() {
            *r = Some((from_seq, events.len()));
        }
        Err(AppendError::Fenced)
    }

    async fn fetch_blob(&self, _sha: &[u8; 32]) -> Result<Vec<u8>, AppendError> {
        Err(AppendError::Unavailable(
            "replay check stores no blobs".to_owned(),
        ))
    }

    async fn done(&self, _run: &RunRef, _attempt: u32, _outcome: ActivationOutcome) {}

    fn effect_driver(&self) -> Option<Arc<dyn EffectDriver>> {
        None
    }

    fn guest_log(&self, _rec: GuestLogRecord) {}

    fn handles_guest_logs(&self) -> bool {
        true
    }
}

fn validate(journal: &[Event]) -> Result<(), String> {
    match journal.first().map(|e| &e.body) {
        Some(EventBody::RunStarted(_)) => {}
        Some(other) => return Err(format!("seq 0 is {:?}, not RunStarted", other.kind())),
        None => return Err("empty journal".to_owned()),
    }
    if let Some((i, e)) = journal.iter().enumerate().find(|(i, e)| e.seq != *i as u64) {
        return Err(format!("journal[{i}] has seq {} (want {i})", e.seq));
    }
    Ok(())
}

/// Replay `journal` with `module` as run `run` on `rt` and classify it (see the module
/// docs). Never performs effect I/O and never runs past the first live append.
pub async fn replay_check(
    rt: &Runtime,
    module: &Prepared,
    run: RunRef,
    journal: &[Event],
) -> ReplayVerdict {
    if let Err(e) = validate(journal) {
        return ReplayVerdict::Error(e);
    }
    let journaled = journaled_outcome(journal);
    let sink = Arc::new(CheckSink {
        refused: Mutex::new(None),
    });
    let outcome = rt
        .activate(
            module,
            Activate {
                run,
                attempt: 1,
                module_sha: module.sha,
                journal: journal.to_vec(),
                inputs: None,
                cancel: CancellationToken::new(),
            },
            sink.clone(),
        )
        .await;
    let refused = *sink.refused.lock().unwrap_or_else(|e| e.into_inner());
    classify(journal, journaled, outcome, refused)
}

fn classify(
    journal: &[Event],
    journaled: Option<ActivationOutcome>,
    outcome: ActivationOutcome,
    refused: Option<(u64, usize)>,
) -> ReplayVerdict {
    let end = journal.len() as u64;
    match (&outcome, journaled) {
        (ActivationOutcome::AttemptFailed(AttemptFailure::NonDeterminism(nd)), _) => {
            ReplayVerdict::Diverged {
                seq: nd.seq,
                detail: format!("expected {}, actual {}", nd.expected, nd.actual),
            }
        }
        (ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(m)), _) => {
            ReplayVerdict::Error(format!("protocol violation: {m}"))
        }
        // Terminal journal.
        (_, Some(journaled)) => {
            if let Some((from, n)) = refused {
                return ReplayVerdict::Diverged {
                    seq: from.max(end),
                    detail: format!(
                        "replay appended {n} event(s) past the terminal event (outcome {outcome:?})"
                    ),
                };
            }
            if outcome == journaled {
                return ReplayVerdict::Ok { appended: 0 };
            }
            match outcome {
                ActivationOutcome::Completed { .. }
                | ActivationOutcome::Failed { .. }
                | ActivationOutcome::ContinuedAsNew { .. } => ReplayVerdict::Diverged {
                    seq: end.saturating_sub(1),
                    detail: format!(
                        "terminal payload differs from the journal: replayed {outcome:?}, journaled {journaled:?}"
                    ),
                },
                other => ReplayVerdict::Error(format!("replay outcome is not terminal: {other:?}")),
            }
        }
        // Non-terminal journal: anything else is live progress past the end.
        (_, None) => ReplayVerdict::OkFrontier {
            appended: refused.map_or(0, |(_, n)| n),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::NonDeterminism;
    use ostro_types::journal::{CommandHash, CommandMeta, Payload, RunCompleted};

    fn completed(seq: u64) -> Event {
        Event {
            seq,
            ts_nanos: 0,
            body: EventBody::RunCompleted(RunCompleted {
                meta: CommandMeta {
                    hash_ver: 1,
                    hash: CommandHash([0; 16]),
                },
                output: Payload {
                    content_type: String::new(),
                    data: Vec::new(),
                },
            }),
        }
    }

    #[test]
    fn nondeterminism_is_diverged_whatever_the_journal() {
        let nd = ActivationOutcome::AttemptFailed(AttemptFailure::NonDeterminism(NonDeterminism {
            seq: 3,
            expected: "a".into(),
            actual: "b".into(),
        }));
        let v = classify(&[], None, nd, Some((5, 1)));
        assert!(matches!(v, ReplayVerdict::Diverged { seq: 3, .. }), "{v:?}");
    }

    #[test]
    fn frontier_counts_the_refused_batch() {
        let fenced = ActivationOutcome::Fenced;
        assert_eq!(
            classify(&[], None, fenced.clone(), Some((4, 2))),
            ReplayVerdict::OkFrontier { appended: 2 }
        );
        let j = vec![completed(0)];
        let v = classify(&j, journaled_outcome(&j), fenced, Some((1, 1)));
        assert!(matches!(v, ReplayVerdict::Diverged { seq: 1, .. }), "{v:?}");
    }

    #[test]
    fn terminal_match_is_ok_zero() {
        let j = vec![completed(0)];
        let out = journaled_outcome(&j).expect("terminal");
        assert_eq!(
            classify(&j, Some(out.clone()), out, None),
            ReplayVerdict::Ok { appended: 0 }
        );
        assert!(ReplayVerdict::Ok { appended: 0 }.passed());
        assert!(!ReplayVerdict::Error(String::new()).passed());
    }

    #[test]
    fn malformed_journals_are_errors() {
        assert!(validate(&[]).is_err());
        assert!(validate(&[completed(0)]).is_err());
    }
}
