//! `EffectProxy`: the worker's [`EffectDriver`] (D-W3 bridge-back, Pattern 6).
//!
//! The worker never performs effect I/O and never resolves a secret. When the engine starts
//! an effect (after its `EffectScheduled` committed, or on a re-drive) the proxy sends
//! `StartEffect{session_id, run, token, op, redrive, redacted_values, http_retry}` to the
//! leader, which re-reads the request from the committed journal and runs it through its
//! Phase 6 `ServerEffectDriver`. The only request bytes the leader cannot read from the
//! journal are the values of headers named in `redact` (journaled as `[redacted]`); exactly
//! those are sent, as written by the guest (`{{secret:NAME}}` templates stay templates).
//! The completion comes back as a journaled input (`Input` + `FetchJournal`).
//!
//! Rules: idempotent per op within the activation (a second `start` of the same op is a
//! no-op; the leader also attaches by (partition, run, op)); nothing is sent once the
//! activation is cancelled (self-fence); `cancel` sends `CancelEffect`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use ostro_engine::EffectDriver;
use ostro_engine::sink::RunRef;
use ostro_types::journal::EffectRequest;
use ostro_types::live::LiveEffect;
use ostro_worker_proto::v1;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::Shared;

/// The header values the journal copy shows as `[redacted]` (ASCII case-insensitive name
/// match, exactly as the engine's `redact_http_request`).
pub fn redacted_values(req: &EffectRequest) -> Vec<v1::Header> {
    match req {
        EffectRequest::Http(r) => r
            .headers
            .iter()
            .filter(|h| r.redact.iter().any(|n| n.eq_ignore_ascii_case(&h.name)))
            .map(|h| v1::Header {
                name: h.name.clone(),
                value: h.value.clone(),
            })
            .collect(),
        EffectRequest::Sql(_) | EffectRequest::Object(_) => Vec::new(),
    }
}

/// Bridges effect starts of one activation back to the leader.
pub struct EffectProxy {
    shared: Arc<Shared>,
    run: v1::RunRef,
    token: v1::Token,
    cancel: CancellationToken,
    started: Mutex<HashSet<u64>>,
}

impl EffectProxy {
    pub(crate) fn new(
        shared: Arc<Shared>,
        run: v1::RunRef,
        token: v1::Token,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            shared,
            run,
            token,
            cancel,
            started: Mutex::new(HashSet::new()),
        }
    }
}

impl EffectDriver for EffectProxy {
    fn start(&self, run: &RunRef, _attempt: u32, effect: LiveEffect) {
        if self.cancel.is_cancelled() {
            return;
        }
        if !self
            .started
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(effect.op)
        {
            return;
        }
        let Some(link) = self.shared.link() else {
            warn!(run = %run.run_id, op = effect.op, "StartEffect: no session");
            return;
        };
        let req = v1::StartEffectRequest {
            session_id: link.session_id.clone(),
            run: Some(self.run.clone()),
            token: Some(self.token),
            op: effect.op,
            redrive: effect.redrive,
            redacted_values: redacted_values(&effect.request),
            http_retry: effect.http_retry.as_ref().map(|r| v1::EffectRetry {
                max_attempts: r.max_attempts,
                initial_interval_ms: r.initial_interval_ms,
                max_interval_ms: r.max_interval_ms,
                backoff_coefficient: r.backoff_coefficient,
                non_retryable_kinds: r.non_retryable_kinds.clone(),
            }),
        };
        let cancel = self.cancel.clone();
        let run_id = run.run_id.clone();
        let op = effect.op;
        tokio::spawn(async move {
            if crate::worker_fail!("worker::before_start_effect") {
                warn!(run = %run_id, op, "worker::before_start_effect (injected): StartEffect not sent");
                return;
            }
            if cancel.is_cancelled() {
                return;
            }
            let mut client = link.client.clone();
            match client.start_effect(req).await {
                Ok(r) => match r.into_inner().result {
                    Some(v1::start_effect_response::Result::Started(_)) => {
                        debug!(run = %run_id, op, "effect started on the leader");
                    }
                    Some(v1::start_effect_response::Result::Rejected(why)) => {
                        warn!(run = %run_id, op, reason = %why, "StartEffect rejected");
                    }
                    None => warn!(run = %run_id, op, "StartEffect: empty response"),
                },
                Err(s) => {
                    warn!(run = %run_id, op, code = %s.code(), msg = %s.message(), "StartEffect failed")
                }
            }
            if crate::worker_fail!("worker::after_start_effect") {
                warn!(run = %run_id, op, "worker::after_start_effect (injected)");
            }
        });
    }

    fn cancel(&self, _run: &RunRef, op: u64) {
        if self.cancel.is_cancelled() {
            return;
        }
        if let Some(link) = self.shared.link() {
            let _ = link.out.send(v1::WorkerMsg {
                msg: Some(v1::worker_msg::Msg::CancelEffect(v1::CancelEffect {
                    run: Some(self.run.clone()),
                    token: Some(self.token),
                    op,
                })),
            });
        }
    }
}
