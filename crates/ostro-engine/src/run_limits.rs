//! Per-run limits (Phase 11, OPS-07, D-P11-13): journal length, children per run and payload
//! size. Contract: 11-02 (struct and defaults); enforced from 11-06.
//!
//! Distinct from [`crate::limits`] (ENG-05 store limits), which is untouched.
//!
//! # Enforcement (11-06, D-P11-13)
//!
//! [`check_live_append`] is called by the replay cursor on its single live append path
//! (`ReplayState::append_live`), before the event is built, and never for a replayed
//! event: lowering a limit never breaks replay of a journal that already exceeds it. A
//! breach ends the attempt with `AttemptFailure::LimitExceeded` (non-retryable), which the
//! executor turns into a resumable `Failed` run. Apply never enforces these limits.
//!
//! - **history**: appending at `next_seq` would make the journal `next_seq + 1` events
//!   long; past `max_history_events` it breaches. Terminal and failure records
//!   (`RunCompleted`, `RunFailed`, `RunContinuedAsNew`, `StepAttemptFailed`) are exempt,
//!   so a run at the limit can still complete or fail cleanly (W8).
//! - **children**: a live `ChildStarted` when `children_so_far` (journaled `ChildStarted`
//!   events, replayed ones included) already equals `max_children_per_run`.
//! - **payload**: the user payload carried by the body: a step's `Ok` output (the
//!   `Payload.data` length; an `Err` outcome counts its encoded length), the run result,
//!   a child's input and a continue-as-new input.

use ostro_types::journal::{EventBody, Failure, Payload, decode_result};
use serde::{Deserialize, Serialize};

/// Limit name of [`RunLimits::max_history_events`] in breaches and failure reasons.
pub const LIMIT_HISTORY: &str = "history_events";
/// Limit name of [`RunLimits::max_children_per_run`].
pub const LIMIT_CHILDREN: &str = "children_per_run";
/// Limit name of [`RunLimits::max_payload_bytes`].
pub const LIMIT_PAYLOAD: &str = "payload_bytes";

/// A live append that would exceed a run limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("limit_exceeded: {limit} {actual} > {max}")]
pub struct LimitBreach {
    /// One of [`LIMIT_HISTORY`], [`LIMIT_CHILDREN`], [`LIMIT_PAYLOAD`].
    pub limit: &'static str,
    pub max: u64,
    pub actual: u64,
}

impl From<LimitBreach> for crate::error::AttemptFailure {
    fn from(b: LimitBreach) -> Self {
        Self::LimitExceeded {
            limit: b.limit,
            max: b.max,
            actual: b.actual,
        }
    }
}

/// Per-run limits (`[limits]` config section), enforced on live appends only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RunLimits {
    /// Maximum journal events per run (default 50,000).
    pub max_history_events: u64,
    /// Maximum child runs started by one run (default 10,000).
    pub max_children_per_run: u64,
    /// Maximum size of one payload (input, output, signal, step result) in bytes
    /// (default 4 MiB).
    pub max_payload_bytes: u64,
}

impl Default for RunLimits {
    fn default() -> Self {
        Self {
            max_history_events: 50_000,
            max_children_per_run: 10_000,
            max_payload_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Whether `body` is exempt from the history limit (W8: terminal and failure records).
fn history_exempt(body: &EventBody) -> bool {
    matches!(
        body,
        EventBody::RunCompleted(_)
            | EventBody::RunFailed(_)
            | EventBody::RunContinuedAsNew(_)
            | EventBody::StepAttemptFailed(_)
    )
}

/// Byte length of the user payload carried by `body` (0 for bodies without one).
///
/// A step outcome is the encoded `Result<Payload, Failure>`; the encoding is never shorter
/// than the payload data, so it is decoded only when the encoded length alone exceeds
/// `max` (the common path never decodes).
fn payload_len(body: &EventBody, max: u64) -> u64 {
    let data = |p: &Payload| p.data.len() as u64;
    match body {
        EventBody::StepCompleted(sc) => {
            let encoded = sc.outcome.bytes.len() as u64;
            if encoded <= max {
                return encoded;
            }
            match decode_result::<Result<Payload, Failure>>(&sc.outcome) {
                Ok(Ok(p)) => data(&p),
                _ => encoded,
            }
        }
        EventBody::RunCompleted(rc) => data(&rc.output),
        EventBody::ChildStarted(cs) => data(&cs.input),
        EventBody::RunContinuedAsNew(c) => data(&c.continuation.input),
        _ => 0,
    }
}

/// Check one live append against `l`. `next_seq` is the seq the event gets (the journal
/// is `next_seq` events long before it); `children_so_far` counts the journal's
/// `ChildStarted` events. Called ONLY on the live path, never for replayed events.
pub fn check_live_append(
    l: &RunLimits,
    next_seq: u64,
    children_so_far: u64,
    body: &EventBody,
) -> Result<(), LimitBreach> {
    let len = next_seq.saturating_add(1);
    if len > l.max_history_events && !history_exempt(body) {
        return Err(LimitBreach {
            limit: LIMIT_HISTORY,
            max: l.max_history_events,
            actual: len,
        });
    }
    if matches!(body, EventBody::ChildStarted(_)) {
        let n = children_so_far.saturating_add(1);
        if n > l.max_children_per_run {
            return Err(LimitBreach {
                limit: LIMIT_CHILDREN,
                max: l.max_children_per_run,
                actual: n,
            });
        }
    }
    let bytes = payload_len(body, l.max_payload_bytes);
    if bytes > l.max_payload_bytes {
        return Err(LimitBreach {
            limit: LIMIT_PAYLOAD,
            max: l.max_payload_bytes,
            actual: bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::journal::{
        AttemptBackoff, ChildOptions, ChildStarted, CommandHash, CommandMeta, Continuation,
        HASH_VER, RunCompleted, RunContinuedAsNew, RunFailed, StepAttemptFailed, StepCompleted,
        encode_result,
    };

    fn meta() -> CommandMeta {
        CommandMeta {
            hash_ver: HASH_VER,
            hash: CommandHash([0; 16]),
        }
    }

    fn payload(n: usize) -> Payload {
        Payload {
            content_type: "application/json".into(),
            data: vec![b'x'; n],
        }
    }

    fn step(n: usize) -> EventBody {
        EventBody::StepCompleted(StepCompleted {
            meta: meta(),
            name: "s".into(),
            outcome: encode_result(&Ok::<Payload, Failure>(payload(n))),
            fuel_used: 0,
        })
    }

    fn small() -> RunLimits {
        RunLimits {
            max_history_events: 20,
            max_children_per_run: 3,
            max_payload_bytes: 1024,
        }
    }

    #[test]
    fn history_breach_and_w8_exemptions() {
        let l = small();
        assert!(check_live_append(&l, 19, 0, &step(1)).is_ok());
        assert_eq!(
            check_live_append(&l, 20, 0, &step(1)),
            Err(LimitBreach {
                limit: LIMIT_HISTORY,
                max: 20,
                actual: 21
            })
        );
        let exempt = [
            EventBody::RunCompleted(RunCompleted {
                meta: meta(),
                output: payload(1),
            }),
            EventBody::RunFailed(RunFailed {
                meta: meta(),
                failure: Failure::Cancelled,
            }),
            EventBody::RunContinuedAsNew(RunContinuedAsNew {
                meta: meta(),
                continuation: Continuation {
                    input: payload(1),
                    workflow: None,
                    version: None,
                },
            }),
            EventBody::StepAttemptFailed(StepAttemptFailed {
                meta: meta(),
                name: "s".into(),
                attempt: 1,
                failure: Failure::Cancelled,
                retry_at_nanos: 0,
                backoff: AttemptBackoff::InActivation,
                fuel_used: 0,
            }),
        ];
        for b in &exempt {
            assert!(check_live_append(&l, 20, 0, b).is_ok(), "{}", b.summary());
            assert!(check_live_append(&l, 500, 0, b).is_ok(), "{}", b.summary());
        }
    }

    #[test]
    fn children_and_payload_breaches() {
        let l = small();
        let child = |n: usize| {
            EventBody::ChildStarted(ChildStarted {
                meta: meta(),
                workflow: "c".into(),
                input: payload(n),
                options: ChildOptions {
                    queue: None,
                    version: None,
                    detached: true,
                    run_timeout_ms: None,
                },
            })
        };
        assert!(check_live_append(&l, 5, 2, &child(1)).is_ok());
        assert_eq!(
            check_live_append(&l, 5, 3, &child(1)),
            Err(LimitBreach {
                limit: LIMIT_CHILDREN,
                max: 3,
                actual: 4
            })
        );
        assert_eq!(
            check_live_append(&l, 5, 0, &child(2048)).map_err(|b| b.limit),
            Err(LIMIT_PAYLOAD)
        );
        assert!(check_live_append(&l, 5, 0, &step(1024)).is_ok());
        assert_eq!(
            check_live_append(&l, 5, 0, &step(2048)),
            Err(LimitBreach {
                limit: LIMIT_PAYLOAD,
                max: 1024,
                actual: 2048
            })
        );
        assert_eq!(
            LimitBreach {
                limit: LIMIT_HISTORY,
                max: 20,
                actual: 21
            }
            .to_string(),
            "limit_exceeded: history_events 21 > 20"
        );
    }

    #[test]
    fn defaults_match_d_p11_13() {
        let l = RunLimits::default();
        assert_eq!(l.max_history_events, 50_000);
        assert_eq!(l.max_children_per_run, 10_000);
        assert_eq!(l.max_payload_bytes, 4 * 1024 * 1024);
        assert_eq!(crate::config::EngineConfig::default().run_limits, l);
    }
}
