//! `RemoteSink`: the [`ActivationSink`] of a remote activation.
//!
//! - **append:** every event is encoded with `ostro_types::journal::encode_event` and split
//!   into `AppendFrame`s whose `chunk` is at most `max_frame_bytes - FRAME_RESERVE` bytes
//!   (`event_index` = position in the batch, `kind` = EventKind code, `event_end` on the
//!   event's last chunk, `last` on the batch's last frame), client-streamed with
//!   `Execute.Append`. `AppendResult` maps to `Ok(next)` / `Fenced` / `Conflict{expected}`;
//!   `paused` maps to `Fenced` (the run stops here; the server maps a pause to Released);
//!   `rejected` and transport errors to `Unavailable`; `PERMISSION_DENIED` /
//!   `FAILED_PRECONDITION` (lease not held) to `Fenced`. A successful append renews the
//!   lease on the server.
//! - **self-fence:** once the activation is fenced (its lease is not this worker's any
//!   more: `Cancel`, `LeaseLost`, a newer token, session loss), `append` answers `Fenced`
//!   locally and nothing more is sent. A drain only evicts the activation, so its final
//!   flush is still sent.
//! - **fetch_blob:** [`crate::blob_fetch`] (length + sha256 verified).
//! - **done:** `Done{run, token, postcard(WireOutcome), engine_end_seq}` on the session.
//! - **effects:** [`crate::effect_proxy::EffectProxy`].
//! - **guest logs:** buffered and forwarded as `GuestLogs` batches (on each append, at 64
//!   lines and at `done`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ostro_engine::sink::{ActivationOutcome, ActivationSink, AppendError, GuestLogRecord, RunRef};
use ostro_types::journal::{Event, encode_event};
use ostro_worker_proto::{WireOutcome, v1};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::Shared;
use crate::blob_fetch;
use crate::effect_proxy::EffectProxy;

/// Bytes of a frame reserved for the non-chunk fields (session id, run, token, indices).
pub const FRAME_RESERVE: usize = 256;
/// Guest log lines buffered before a forced flush.
const LOG_BATCH: usize = 64;

/// Split encoded events into `AppendFrame`s with chunks of at most `chunk_max` bytes.
pub fn frames(
    session_id: &[u8],
    run: &v1::RunRef,
    token: &v1::Token,
    from_seq: u64,
    events: &[Event],
    chunk_max: usize,
) -> Vec<v1::AppendFrame> {
    let chunk_max = chunk_max.max(1);
    let mut out = Vec::new();
    for (i, ev) in events.iter().enumerate() {
        let (kind, bytes) = encode_event(ev);
        let mut parts: Vec<&[u8]> = bytes.chunks(chunk_max).collect();
        if parts.is_empty() {
            parts.push(&[]);
        }
        let n = parts.len();
        for (j, part) in parts.into_iter().enumerate() {
            out.push(v1::AppendFrame {
                session_id: session_id.to_vec(),
                run: Some(run.clone()),
                token: Some(*token),
                from_seq,
                event_index: u32::try_from(i).unwrap_or(u32::MAX),
                chunk: part.to_vec(),
                event_end: j + 1 == n,
                last: false,
                kind,
            });
        }
    }
    if let Some(f) = out.last_mut() {
        f.last = true;
    }
    out
}

/// The engine sink of one remote activation.
pub struct RemoteSink {
    shared: Arc<Shared>,
    run: v1::RunRef,
    token: v1::Token,
    cancel: CancellationToken,
    engine_end: AtomicU64,
    effects: Arc<EffectProxy>,
    logs: Mutex<Vec<v1::GuestLogLine>>,
}

impl RemoteSink {
    pub(crate) fn new(
        shared: Arc<Shared>,
        run: v1::RunRef,
        token: v1::Token,
        cancel: CancellationToken,
        journal_end: u64,
    ) -> Self {
        let effects = Arc::new(EffectProxy::new(
            Arc::clone(&shared),
            run.clone(),
            token,
            cancel.clone(),
        ));
        Self {
            shared,
            run,
            token,
            cancel,
            engine_end: AtomicU64::new(journal_end),
            effects,
            logs: Mutex::new(Vec::new()),
        }
    }

    /// The journal end as the engine saw it (initially the fetched journal's end).
    pub fn engine_end(&self) -> u64 {
        self.engine_end.load(Ordering::Acquire)
    }

    fn flush_logs(&self) {
        let lines = std::mem::take(&mut *self.logs.lock().unwrap_or_else(|p| p.into_inner()));
        if lines.is_empty() || self.cancel.is_cancelled() {
            return;
        }
        if let Some(link) = self.shared.link() {
            let _ = link.out.send(v1::WorkerMsg {
                msg: Some(v1::worker_msg::Msg::GuestLogs(v1::GuestLogs {
                    run: Some(self.run.clone()),
                    token: Some(self.token),
                    lines,
                })),
            });
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[async_trait::async_trait]
impl ActivationSink for RemoteSink {
    async fn append(
        &self,
        run: &RunRef,
        _attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError> {
        if self.cancel.is_cancelled() {
            return Err(AppendError::Fenced);
        }
        if crate::worker_fail!("worker::before_append") {
            return Err(AppendError::Unavailable(
                "worker::before_append (injected)".into(),
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(AppendError::Fenced);
        }
        self.flush_logs();
        if events.is_empty() {
            return Ok(from_seq);
        }
        let link = self
            .shared
            .link()
            .ok_or_else(|| AppendError::Unavailable("no session".into()))?;
        let chunk_max = link.max_frame.saturating_sub(FRAME_RESERVE).max(1024);
        let fr = frames(
            &link.session_id,
            &self.run,
            &self.token,
            from_seq,
            &events,
            chunk_max,
        );
        let mut client = link.client.clone();
        let res = client
            .append(tonic::Request::new(tonic::codegen::tokio_stream::iter(fr)))
            .await;
        let result = match res {
            Ok(r) => r.into_inner().result,
            Err(s) => {
                return Err(match s.code() {
                    tonic::Code::PermissionDenied | tonic::Code::FailedPrecondition => {
                        AppendError::Fenced
                    }
                    c => AppendError::Unavailable(format!("append: {c}: {}", s.message())),
                });
            }
        };
        match result {
            Some(v1::append_result::Result::Next(next)) => {
                self.engine_end.store(next, Ordering::Release);
                Ok(next)
            }
            Some(v1::append_result::Result::Fenced(_)) => Err(AppendError::Fenced),
            Some(v1::append_result::Result::Paused(_)) => {
                warn!(run = %run.run_id, "append refused: run paused");
                Err(AppendError::Fenced)
            }
            Some(v1::append_result::Result::ConflictExpected(expected)) => {
                Err(AppendError::Conflict { expected })
            }
            Some(v1::append_result::Result::Rejected(why)) => {
                Err(AppendError::Unavailable(format!("append rejected: {why}")))
            }
            None => Err(AppendError::Unavailable("append: empty result".into())),
        }
    }

    async fn fetch_blob(&self, sha: &[u8; 32]) -> Result<Vec<u8>, AppendError> {
        let link = self
            .shared
            .link()
            .ok_or_else(|| AppendError::Unavailable("no session".into()))?;
        blob_fetch::fetch(&link, &self.run, &self.token, sha, None)
            .await
            .map_err(|e| match e {
                blob_fetch::FetchError::Fenced(_) => AppendError::Fenced,
                other => AppendError::Unavailable(format!("blob {}: {other}", hex(sha))),
            })
    }

    async fn done(&self, run: &RunRef, attempt: u32, outcome: ActivationOutcome) {
        self.flush_logs();
        if crate::worker_fail!("worker::before_done") {
            warn!(run = %run.run_id, attempt, "worker::before_done (injected): Done not sent");
            return;
        }
        send_done(
            &self.shared,
            &self.run,
            &self.token,
            &outcome,
            self.engine_end(),
        );
    }

    fn effect_driver(&self) -> Option<Arc<dyn ostro_engine::EffectDriver>> {
        Some(Arc::clone(&self.effects) as Arc<dyn ostro_engine::EffectDriver>)
    }

    fn handles_guest_logs(&self) -> bool {
        true
    }

    fn guest_log(&self, rec: GuestLogRecord) {
        let line = v1::GuestLogLine {
            attempt: rec.attempt,
            level: rec.level.to_owned(),
            message: rec.message,
            fields: rec
                .fields
                .into_iter()
                .map(|(name, value)| v1::Header { name, value })
                .collect(),
            ts_ms: rec.ts_ms,
            trace_id: rec.trace_id.map(|t| hex(&t)).unwrap_or_default(),
            span_id: rec.span_id.map(|s| hex(&s)).unwrap_or_default(),
            workflow: rec.workflow,
        };
        let full = {
            let mut g = self.logs.lock().unwrap_or_else(|p| p.into_inner());
            g.push(line);
            g.len() >= LOG_BATCH
        };
        if full {
            self.flush_logs();
        }
    }
}

/// Send `Done` for `(run, token)` on the current session (if any).
pub(crate) fn send_done(
    shared: &Shared,
    run: &v1::RunRef,
    token: &v1::Token,
    outcome: &ActivationOutcome,
    engine_end_seq: u64,
) {
    let Some(link) = shared.link() else {
        return;
    };
    let _ = link.out.send(v1::WorkerMsg {
        msg: Some(v1::worker_msg::Msg::Done(v1::Done {
            run: Some(run.clone()),
            token: Some(*token),
            outcome: WireOutcome::from(outcome).encode(),
            engine_end_seq,
        })),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::journal::{EventBody, TimerFired};

    #[test]
    fn frames_split_and_mark() {
        let evs: Vec<Event> = (0..3)
            .map(|i| Event {
                seq: 10 + i,
                ts_nanos: 7,
                body: EventBody::TimerFired(TimerFired { op: i }),
            })
            .collect();
        let run = v1::RunRef {
            run_id: vec![1; 18],
            partition: 0,
        };
        let token = v1::Token { epoch: 1, n: 2 };
        let f = frames(b"s", &run, &token, 10, &evs, 4);
        assert!(f.iter().all(|x| x.chunk.len() <= 4));
        assert!(f.last().unwrap().last);
        assert_eq!(f.iter().filter(|x| x.last).count(), 1);
        assert_eq!(f.iter().filter(|x| x.event_end).count(), 3);
        for i in 0..3u32 {
            let bytes: Vec<u8> = f
                .iter()
                .filter(|x| x.event_index == i)
                .flat_map(|x| x.chunk.clone())
                .collect();
            let (kind, enc) = encode_event(&evs[i as usize]);
            assert_eq!(bytes, enc);
            assert!(
                f.iter()
                    .filter(|x| x.event_index == i)
                    .all(|x| x.kind == kind)
            );
        }
    }
}
