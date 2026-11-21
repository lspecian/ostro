//! The remote runner (plan 07-12, WORK-01/02/06, research Pattern 1, D-W3, D-W27).
//!
//! A dispatcher [`Job`] placed on a worker stays a server-side job: the leader keeps every
//! write, effect and secret; the worker only runs the guest.
//!
//! [`PlacementExecutor`] routes by the reservation's kind: `Embedded` to the wrapped
//! [`EngineExecutor`] (unchanged), `Worker` to [`RemoteRunner`].
//!
//! # One job = one remote activation
//!
//! 1. **Credit:** the reservation's [`WorkerSlot`] credit (a re-grant under the same lease,
//!    e.g. after `Retry`, picks a fresh selector-matching credit, preferring the same
//!    worker; none = `Abandoned`).
//! 2. **Sink:** `EngineExecutor::sink_for` (B3), the embedded construction site: the same
//!    resolvers, guest log sink and Phase 6 bridge. Every accepted append renews the lease.
//! 3. **Prelude:** `EngineExecutor::prelude`, shared with the embedded path: `RunStarted`
//!    on the first activation (`next_seq == 0`), the resume re-arm (B2), the journal load
//!    with `repair_unsupported`. A run without a module, or whose module blob is missing,
//!    fails like an embedded run (`no_module` / `module_not_found`).
//! 4. **Activate:** the job is attached to the credit's session
//!    ([`WorkerRegistry::attach_job`]) and `Activate{run, token, module_sha, journal_len,
//!    workflow, workflow_version, traceparent, hot_wait_ms}` is sent; logged at info as
//!    `remote activate run=<id> attempt=<n> worker=<id>` (W3).
//! 5. **Append:** the service reassembles the frames (`[workers] max_append_bytes`); every
//!    event is decoded server-side with the journal codec (a decode error rejects the
//!    batch; raw bytes never reach apply; the wire carries no intents) and appended through
//!    the run's `PartitionSink` (derive, blob offload, fencing by apply). Results map to
//!    `AppendResult` (`next` / `fenced` / `conflict_expected` / `rejected`); a successful
//!    append is logged at debug as `applied events run=<id> attempt=<n> from_seq=<s>
//!    count=<k>` (W3). After a revoke every append answers `fenced` (the lease check; apply
//!    stays the only write fence).
//! 6. **Await tokens (D-W27):** tokens are minted on the leader only. A batch whose
//!    `AwaitTokenCreated` carries a token other than the leader's mint (the worker's key is
//!    process-local) is cut after that event, the token is re-minted with the leader key
//!    (`ostro_engine::replay::token::mint`, deterministic per (partition, run, op)) and
//!    appended, and the reply is `conflict_expected = next`: the worker's engine evicts
//!    (`AppendConflict`) and the run is re-granted and replays the journaled leader token.
//!    Events after the token in that batch are never applied (the replay regenerates them
//!    with the leader token). The key never leaves the leader.
//! 7. **Inputs:** dispatcher `InputAppended` seqs and the sink's own completions are
//!    wake-ups only; each becomes `Input{run, token, up_to_seq}` (monotonic); the worker's
//!    ordered bridge fetches `[next, up_to_seq)`.
//! 8. **StartEffect** (D-W3), in order: (1) the request's token is this job's and the lease
//!    is not revoked, else `FAILED_PRECONDITION` (the service already checked that the
//!    calling session holds the lease); (2) `EffectScheduled(op)` exists in the run's journal
//!    and nothing completes it, else `FAILED_PRECONDITION`; (3) the request's redacted header
//!    names equal, in order, the journal copy's redacted header names (`redact`, ASCII
//!    case-insensitive), else `rejected`; (3b) a redacted value containing `{{` must be made
//!    of well-formed `{{secret:NAME}}` placeholders only, else `rejected: template in
//!    redacted value` (see the 07-12 SUMMARY: W6 adaptation); (4) the `LiveEffect` is rebuilt
//!    from the journal copy (`scheduled_ts_nanos` = the event's ts, `redrive` and
//!    `http_retry` from the request, header values from the request for redacted names only)
//!    and started through the run's `ServerEffectDriver` (Phase 6 unchanged: registry
//!    attach-on-duplicate, SendGate, per-(run, attempt) secrets, CompleteEffect,
//!    EffectHooks). Secrets are resolved inside the bridge; nothing secret is sent back.
//! 9. **Done:** `WireOutcome` → `ActivationOutcome` → `EngineExecutor::map_outcome`, the
//!    embedded outcome mapper (04-16 table + 07-08 exhaust policy), at the journal end as
//!    the leader saw the worker's appends. A `Reactivate` becomes `Abandoned` (re-grant
//!    `n + 1` now, no failure counted): re-sending `Activate` with the same token could be
//!    swallowed by the worker's duplicate-lease check while the previous activation is
//!    still unregistering.
//!
//! Stream loss (`RemoteEvent::Abandoned`) is `ExecOutcome::Abandoned`, never an attempt
//! failure (Pitfall 3). A revoked lease sends `Cancel{run, token}` and waits for `Done` (at
//! most `min(lease_ttl, 10 s)`), answering every append `fenced` meanwhile.
//!
//! Guest logs (`GuestLogs`) go to the 08-17 guest log sink through the run's sink
//! (`ActivationSink::guest_log`), the same path as embedded lines.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ostro_engine::error::AttemptFailure;
use ostro_engine::sink::{ActivationOutcome, ActivationSink, AppendError, GuestLogRecord, RunRef};
use ostro_meta::MetaView;
use ostro_partition::leader::dispatcher::LeaseReservation;
use ostro_partition::leader::placement::ReservationKind;
use ostro_partition::{ExecOutcome, Executor, Job};
use ostro_types::journal::{EffectRequest, Event, EventBody, decode_event};
use ostro_types::live::{EffectRetry, LiveEffect};
use ostro_types::{AttemptToken, Epoch, RunId};
use ostro_worker_proto::{WireOutcome, v1};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{debug, info, warn};

use crate::bridge_services::BridgeServices;
use crate::executor::{EngineExecutor, JobRecord, REASON_MODULE_NOT_FOUND, REASON_NO_MODULE};
use crate::journal_io;
use crate::p7::{P7Ctx, P7Shared};
use crate::sink::{PartitionSink, RenewFn};
use crate::workers::placement::WorkerSlot;
use crate::workers::registry::{AttachSpec, JobRpc, RemoteEvent, wire_run, wire_token};
use crate::workers::service::WireDir;

/// Upper bound of the wait for `Done` after a `Cancel` (revoked lease).
const MAX_CANCEL_WAIT: Duration = Duration::from_secs(10);

// ------------------------------------------------------------------------- test hooks

/// One message the runner sent or received ([`RemoteHooks::tap`]).
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TapRecord {
    pub dir: WireDir,
    /// `activate` | `input` | `cancel` | `append` | `append_result` | `start_effect` |
    /// `start_effect_result` | `done` | `guest_logs` | `cancel_effect`.
    pub what: &'static str,
    /// The message's content: every string and bytes field, concatenated.
    pub bytes: Vec<u8>,
}

/// Remote execution test hooks (part of [`crate::executor::ExecutorHooks`]; not product
/// surface).
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct RemoteHooks {
    /// `(kind, bridge instance address)` of every sink built by `EngineExecutor::sink_for`.
    pub sinks: Mutex<Vec<(ReservationKind, Option<usize>)>>,
    /// Record every message of every remote job in [`wire`](Self::wire).
    pub tap: AtomicBool,
    pub wire: Mutex<Vec<TapRecord>>,
    /// Await tokens re-minted with the leader key (D-W27).
    pub remints: AtomicU64,
    /// Remote activations sent.
    pub activations: AtomicU64,
}

impl RemoteHooks {
    pub(crate) fn note_sink(&self, kind: ReservationKind, bridge: Option<&Arc<BridgeServices>>) {
        let addr = bridge.map(|b| Arc::as_ptr(b) as usize);
        lock(&self.sinks).push((kind, addr));
    }

    fn record(&self, dir: WireDir, what: &'static str, parts: &[&[u8]]) {
        if !self.tap.load(Ordering::Acquire) {
            return;
        }
        let bytes = parts.concat();
        lock(&self.wire).push(TapRecord { dir, what, bytes });
    }

    /// Recorded messages so far.
    pub fn records(&self) -> Vec<TapRecord> {
        lock(&self.wire).clone()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ------------------------------------------------------------------------- executor

/// Runs embedded reservations on the wrapped (engine) executor and worker reservations
/// on the [`RemoteRunner`].
pub struct PlacementExecutor {
    embedded: Arc<dyn Executor>,
    remote: Arc<RemoteRunner>,
}

impl PlacementExecutor {
    pub fn new(embedded: Arc<dyn Executor>, remote: Arc<RemoteRunner>) -> Self {
        Self { embedded, remote }
    }
}

impl Executor for PlacementExecutor {
    fn execute(&self, job: Job) -> Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>> {
        match job.reservation_kind() {
            ReservationKind::Embedded => self.embedded.execute(job),
            ReservationKind::Worker => {
                let remote = Arc::clone(&self.remote);
                Box::pin(async move { remote.run(job).await })
            }
        }
    }
}

/// Proxies one leased job to a worker session (module docs).
pub struct RemoteRunner {
    shared: Arc<P7Shared>,
    meta: MetaView,
    hot_wait_ms: u64,
    cancel_wait: Duration,
}

impl std::fmt::Debug for RemoteRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteRunner")
            .field("hot_wait_ms", &self.hot_wait_ms)
            .finish_non_exhaustive()
    }
}

/// Per-activation state of the event loop.
struct Live {
    /// Journal end as the worker's engine saw it (the CAS `from_seq`).
    engine_end: u64,
    /// Highest `Input.up_to_seq` sent.
    input_up_to: u64,
    revoked: bool,
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn token_of(t: Option<&v1::Token>) -> Option<AttemptToken> {
    t.map(|t| AttemptToken {
        epoch: Epoch(t.epoch),
        n: t.n,
    })
}

fn level(s: &str) -> &'static str {
    match s {
        "trace" => "trace",
        "debug" => "debug",
        "warn" => "warn",
        "error" => "error",
        _ => "info",
    }
}

fn unhex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn result_msg(r: &v1::append_result::Result) -> String {
    format!("{r:?}")
}

/// Whether a header of `r` is redacted in the journal copy (the engine's rule).
fn is_redacted(r: &ostro_types::journal::HttpRequest, name: &str) -> bool {
    r.redact.iter().any(|n| n.eq_ignore_ascii_case(name))
}

/// A redacted value containing `{{` must consist of well-formed `{{secret:NAME}}`
/// placeholders only (module docs, step 3b).
fn check_template(value: &str) -> Result<(), String> {
    use ostro_bridge::secrets::template;
    if !value.contains("{{") {
        return Ok(());
    }
    let bad = || "template in redacted value".to_owned();
    let ph = template::parse(value).map_err(|_| bad())?;
    if ph.is_empty() {
        return Err(bad());
    }
    // Nothing but placeholders may open with `{{`.
    let mut rest = String::with_capacity(value.len());
    let mut at = 0;
    for p in &ph {
        rest.push_str(&value[at..p.range.start]);
        at = p.range.end;
    }
    rest.push_str(&value[at..]);
    if rest.contains("{{") {
        Err(bad())
    } else {
        Ok(())
    }
}

/// The live request of a remote `StartEffect`: the journal copy with the redacted header
/// values taken from the worker (names checked, in order).
pub fn rebuild_request(
    journal: &EffectRequest,
    values: &[v1::Header],
) -> Result<EffectRequest, String> {
    match journal {
        EffectRequest::Http(r) => {
            let want: Vec<&str> = r
                .headers
                .iter()
                .filter(|h| is_redacted(r, &h.name))
                .map(|h| h.name.as_str())
                .collect();
            let got: Vec<&str> = values.iter().map(|h| h.name.as_str()).collect();
            if want != got {
                return Err(format!(
                    "redacted header names {got:?} do not match the journal ({want:?})"
                ));
            }
            for v in values {
                check_template(&v.value)?;
            }
            let mut out = r.clone();
            let mut it = values.iter();
            for h in &mut out.headers {
                if is_redacted(r, &h.name) {
                    let v = it.next().ok_or("redacted value missing")?;
                    h.value.clone_from(&v.value);
                }
            }
            Ok(EffectRequest::Http(out))
        }
        EffectRequest::Sql(_) | EffectRequest::Object(_) if values.is_empty() => {
            Ok(journal.clone())
        }
        EffectRequest::Sql(_) | EffectRequest::Object(_) => {
            Err("redacted header values for a non-HTTP effect".into())
        }
    }
}

impl RemoteRunner {
    pub fn new(ctx: &P7Ctx) -> Self {
        let hot_wait = ctx.config.engine.to_engine_config().hot_wait;
        Self {
            shared: Arc::clone(&ctx.shared),
            meta: ctx.meta.clone(),
            hot_wait_ms: ms(hot_wait),
            cancel_wait: ctx.lease_ttl.min(MAX_CANCEL_WAIT),
        }
    }

    /// Run `job` on its worker (module docs).
    pub async fn run(&self, job: Job) -> ExecOutcome {
        let Some(engine) = self.shared.engine.get().cloned() else {
            warn!(run = %job.run, "remote job without an engine executor; abandoning");
            return ExecOutcome::Abandoned;
        };
        let (run, token) = (job.run, job.token);
        let (activation, outcome, revoked) = self.drive(&engine, job).await;
        engine.hooks().jobs().push(JobRecord {
            run,
            token,
            activation,
            outcome,
            revoked,
            self_fenced: false,
        });
        outcome
    }

    /// The job's worker credit: the reservation's, or (a re-grant under the same lease) a
    /// fresh selector-matching one, preferring the same worker. `None` = lease gone,
    /// `Some(None)` = no credit.
    fn credit(
        &self,
        reservation: &ostro_partition::leader::placement::Reservation,
    ) -> Option<Option<crate::workers::registry::CreditGuard>> {
        let lr = reservation.downcast_ref::<LeaseReservation>()?;
        lr.with::<WorkerSlot, _>(|w| {
            w.take_credit()
                .or_else(|| {
                    self.shared
                        .registry
                        .pick(|v| v.worker_id == w.worker_id() && w.queue().matches(v.labels))
                })
                .or_else(|| self.shared.registry.pick(|v| w.queue().matches(v.labels)))
        })
    }

    async fn drive(
        &self,
        engine: &EngineExecutor,
        job: Job,
    ) -> (Option<ActivationOutcome>, ExecOutcome, bool) {
        if job.record.status.is_terminal() {
            return (None, ExecOutcome::Released, false);
        }
        // 1. Credit.
        let credit = match self.credit(&job.reservation) {
            None => return (None, ExecOutcome::Released, true),
            Some(None) => {
                debug!(run = %job.run, "no worker credit for the re-grant; abandoning");
                return (None, ExecOutcome::Abandoned, false);
            }
            Some(Some(c)) => c,
        };
        // 2. Sink (the embedded construction site, B3).
        let failures = job.record.attempt_failures;
        let renew_lease = job.lease.clone();
        let on_renew: RenewFn = Arc::new(move || {
            renew_lease.renew();
        });
        let (sink_tx, mut sink_rx) = mpsc::unbounded_channel::<Event>();
        let sink = Arc::new(engine.sink_for(&job, Some(sink_tx), Some(on_renew)));
        let Job {
            run,
            token,
            record,
            handle,
            inputs: mut seqs,
            mut revoked,
            lease,
            ..
        } = job;
        let rref = RunRef {
            partition: run.partition().0,
            run_id: run.to_string(),
        };
        let hooks = Arc::clone(engine.hooks());
        // Revoke -> cancel during the prelude.
        let cancel = CancellationToken::new();
        let watcher = {
            let cancel = cancel.clone();
            let mut r = revoked.clone();
            tokio::spawn(async move {
                let _ = r.wait_for(|v| *v).await;
                cancel.cancel();
            })
        };
        let _abort = AbortOnDrop(watcher);
        // 3. Module + prelude.
        let Some(sha) = engine.module_sha(&run, &record) else {
            let out = engine
                .fail_run(
                    &sink,
                    REASON_NO_MODULE,
                    REASON_NO_MODULE,
                    "no module pinned",
                    false,
                )
                .await;
            return (None, out, false);
        };
        let blobs = Arc::clone(handle.blobs());
        let probe = ostro_blob::BlobRef {
            sha256: sha,
            len: 0,
        };
        let present = tokio::task::spawn_blocking(move || blobs.path_of(&probe).is_file())
            .await
            .unwrap_or(false);
        if !present {
            let msg = format!("module {} not found", ostro_blob::hex(&sha));
            let out = engine
                .fail_run(
                    &sink,
                    REASON_MODULE_NOT_FOUND,
                    REASON_MODULE_NOT_FOUND,
                    &msg,
                    false,
                )
                .await;
            return (None, out, false);
        }
        let journal = match engine
            .prelude(&handle, run, token, &record, &sink, &rref, &cancel, &sha)
            .await
        {
            Ok(j) => j,
            Err(out) => return (None, out, *revoked.borrow()),
        };
        let journal_len = journal.len() as u64;
        drop(journal);
        // 4. Attach + Activate.
        let mut rj = match self.shared.registry.attach_job(
            credit,
            AttachSpec {
                run,
                token,
                lease,
                handle: handle.clone(),
                module_sha: Some(sha),
            },
        ) {
            Ok(h) => h,
            Err(e) => {
                debug!(%run, error = %e, "attach failed; abandoning");
                return (None, ExecOutcome::Abandoned, false);
            }
        };
        let mut rpcs = rj.rpcs();
        let act = v1::Activate {
            run: Some(wire_run(&run)),
            token: Some(wire_token(token)),
            module_sha: sha.to_vec(),
            journal_len,
            workflow: record.workflow.clone(),
            workflow_version: record.workflow_version,
            traceparent: record.traceparent.clone().unwrap_or_default(),
            hot_wait_ms: self.hot_wait_ms,
        };
        hooks.remote.record(
            WireDir::Out,
            "activate",
            &[
                &act.module_sha,
                act.workflow.as_bytes(),
                act.traceparent.as_bytes(),
                &wire_run(&run).run_id,
            ],
        );
        if !rj.send(v1::server_msg::Msg::Activate(act)) {
            return (None, ExecOutcome::Abandoned, false);
        }
        hooks.remote.activations.fetch_add(1, Ordering::Relaxed);
        info!(
            "remote activate run={run} attempt={} worker={}",
            token.n,
            rj.worker_id()
        );
        let mut st = Live {
            engine_end: journal_len,
            input_up_to: journal_len,
            revoked: false,
        };
        let (mut seqs_open, mut sink_open) = (true, true);
        let mut cancel_deadline: Option<tokio::time::Instant> = None;
        let done = loop {
            tokio::select! {
                biased;
                () = async { drop(revoked.wait_for(|v| *v).await) }, if !st.revoked => {
                    st.revoked = true;
                    let msg = v1::Cancel { run: Some(wire_run(&run)), token: Some(wire_token(token)), reason: 0 };
                    hooks.remote.record(WireDir::Out, "cancel", &[&wire_run(&run).run_id]);
                    rj.send(v1::server_msg::Msg::Cancel(msg));
                    cancel_deadline = Some(tokio::time::Instant::now() + self.cancel_wait);
                }
                _ = sleep_until_opt(cancel_deadline), if cancel_deadline.is_some() => {
                    debug!(%run, "no Done after Cancel within the bound");
                    break None;
                }
                ev = rj.next_event() => match ev {
                    Some(RemoteEvent::Done(d)) => {
                        hooks.remote.record(WireDir::In, "done", &[&d.outcome]);
                        break Some(d);
                    }
                    Some(RemoteEvent::GuestLogs(g)) => self.guest_logs(&hooks, &sink, &rref, g),
                    Some(RemoteEvent::CancelEffect(c)) => {
                        hooks.remote.record(WireDir::In, "cancel_effect", &[]);
                        if !st.revoked
                            && let Some(d) = sink.effect_driver()
                        {
                            d.cancel(&rref, c.op);
                        }
                    }
                    Some(RemoteEvent::Abandoned { reason }) => {
                        info!(%run, attempt = token.n, %reason, "remote job abandoned (worker stream lost)");
                        let out = if st.revoked { ExecOutcome::Released } else { ExecOutcome::Abandoned };
                        return (None, out, st.revoked);
                    }
                    None => return (None, ExecOutcome::Abandoned, st.revoked),
                },
                rpc = rpcs.recv() => match rpc {
                    Some(JobRpc::Append { batch, reply }) => {
                        let r = self.on_append(&hooks, &mut st, &sink, &rref, token, batch).await;
                        let _ = reply.send(r);
                    }
                    Some(JobRpc::StartEffect { req, reply }) => {
                        let r = self.on_start_effect(&hooks, &st, &sink, &rref, run, token, req).await;
                        let _ = reply.send(r);
                    }
                    None => {}
                },
                s = seqs.recv(), if seqs_open => match s {
                    Some(s) => self.input(&hooks, &mut st, &rj, run, token, s + 1),
                    None => seqs_open = false,
                },
                e = sink_rx.recv(), if sink_open => match e {
                    Some(e) => self.input(&hooks, &mut st, &rj, run, token, e.seq + 1),
                    None => sink_open = false,
                },
            }
        };
        drop(rj);
        let Some(done) = done else {
            return (None, ExecOutcome::Released, true);
        };
        let outcome = match WireOutcome::decode(&done.outcome) {
            Ok(w) => w.into_engine(),
            Err(e) => ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(format!(
                "worker sent an undecodable outcome: {e}"
            ))),
        };
        if st.revoked || *revoked.borrow() || sink.is_terminal() {
            return (Some(outcome), ExecOutcome::Released, st.revoked);
        }
        let exec = engine
            .map_outcome(&sink, &outcome, st.engine_end, failures, &record)
            .await;
        let exec = match exec {
            // Module docs, step 9: never re-send the same token.
            ExecOutcome::Reactivate => ExecOutcome::Abandoned,
            other => other,
        };
        (Some(outcome), exec, false)
    }

    #[allow(clippy::too_many_arguments)]
    fn input(
        &self,
        hooks: &crate::executor::ExecutorHooks,
        st: &mut Live,
        rj: &crate::workers::registry::RemoteJobHandle,
        run: RunId,
        token: AttemptToken,
        up_to: u64,
    ) {
        if st.revoked || up_to <= st.input_up_to {
            return;
        }
        st.input_up_to = up_to;
        hooks
            .remote
            .record(WireDir::Out, "input", &[&wire_run(&run).run_id]);
        rj.send(v1::server_msg::Msg::Input(v1::Input {
            run: Some(wire_run(&run)),
            token: Some(wire_token(token)),
            up_to_seq: up_to,
        }));
    }

    fn guest_logs(
        &self,
        hooks: &crate::executor::ExecutorHooks,
        sink: &PartitionSink,
        rref: &RunRef,
        g: v1::GuestLogs,
    ) {
        for l in g.lines {
            let mut parts: Vec<&[u8]> = vec![l.message.as_bytes(), l.workflow.as_bytes()];
            for f in &l.fields {
                parts.push(f.name.as_bytes());
                parts.push(f.value.as_bytes());
            }
            hooks.remote.record(WireDir::In, "guest_logs", &parts);
            let rec = GuestLogRecord {
                run_id: rref.run_id.clone(),
                workflow: l.workflow,
                attempt: l.attempt,
                level: level(&l.level),
                message: l.message,
                fields: l.fields.into_iter().map(|h| (h.name, h.value)).collect(),
                ts_ms: l.ts_ms,
                trace_id: unhex::<16>(&l.trace_id),
                span_id: unhex::<8>(&l.span_id),
            };
            if sink.handles_guest_logs() {
                sink.guest_log(rec);
            } else {
                info!(
                    target: "ostro::guest",
                    run = %rec.run_id,
                    attempt = rec.attempt,
                    level = rec.level,
                    "{}",
                    rec.message
                );
            }
        }
    }

    async fn on_append(
        &self,
        hooks: &crate::executor::ExecutorHooks,
        st: &mut Live,
        sink: &PartitionSink,
        rref: &RunRef,
        token: AttemptToken,
        batch: crate::workers::registry::AppendBatch,
    ) -> Result<v1::AppendResult, Status> {
        use v1::append_result::Result as R;
        let parts: Vec<&[u8]> = batch.events.iter().map(|(_, b)| b.as_slice()).collect();
        hooks.remote.record(WireDir::In, "append", &parts);
        let reply = |r: R| {
            hooks
                .remote
                .record(WireDir::Out, "append_result", &[result_msg(&r).as_bytes()]);
            Ok(v1::AppendResult { result: Some(r) })
        };
        if st.revoked {
            return reply(R::Fenced(true));
        }
        if batch.events.is_empty() {
            return reply(R::Rejected("empty append".into()));
        }
        let mut events = Vec::with_capacity(batch.events.len());
        for (i, (kind, bytes)) in batch.events.iter().enumerate() {
            let seq = batch.from_seq + i as u64;
            match decode_event(seq, *kind, bytes) {
                Ok(ev) => events.push(ev),
                Err(e) => return reply(R::Rejected(format!("event {seq} does not decode: {e}"))),
            }
        }
        // D-W27: await tokens are minted on the leader only.
        let mut remint = false;
        if let Some(i) = events
            .iter()
            .position(|e| matches!(e.body, EventBody::AwaitTokenCreated(_)))
        {
            let Some(key) = self.meta.read(|s| s.token_key()) else {
                return reply(R::Rejected("the cluster has no token key".into()));
            };
            let seq = events[i].seq;
            if let EventBody::AwaitTokenCreated(t) = &mut events[i].body {
                let leader =
                    ostro_engine::replay::token::mint(rref.partition, &rref.run_id, seq, &key);
                if t.token != leader {
                    t.token = leader;
                    remint = true;
                }
            }
            if remint {
                events.truncate(i + 1);
            }
        }
        let count = events.len();
        let r = match sink.append(rref, token.n, batch.from_seq, events).await {
            Ok(next) => {
                st.engine_end = next;
                debug!(
                    "applied events run={} attempt={} from_seq={} count={count}",
                    rref.run_id, token.n, batch.from_seq
                );
                if remint {
                    hooks.remote.remints.fetch_add(1, Ordering::Relaxed);
                    debug!(run = %rref.run_id, "await token re-minted on the leader; ending the activation");
                    R::ConflictExpected(next)
                } else {
                    R::Next(next)
                }
            }
            Err(AppendError::Fenced) => R::Fenced(true),
            Err(AppendError::Conflict { expected }) => R::ConflictExpected(expected),
            Err(AppendError::Unavailable(e)) => R::Rejected(e),
        };
        reply(r)
    }

    #[allow(clippy::too_many_arguments)]
    async fn on_start_effect(
        &self,
        hooks: &crate::executor::ExecutorHooks,
        st: &Live,
        sink: &PartitionSink,
        rref: &RunRef,
        run: RunId,
        token: AttemptToken,
        req: v1::StartEffectRequest,
    ) -> Result<v1::StartEffectResponse, Status> {
        use v1::start_effect_response::Result as R;
        let mut parts: Vec<&[u8]> = Vec::new();
        for h in &req.redacted_values {
            parts.push(h.name.as_bytes());
            parts.push(h.value.as_bytes());
        }
        hooks.remote.record(WireDir::In, "start_effect", &parts);
        let rejected = |m: String| {
            hooks
                .remote
                .record(WireDir::Out, "start_effect_result", &[m.as_bytes()]);
            Ok(v1::StartEffectResponse {
                result: Some(R::Rejected(m)),
            })
        };
        // (1) The lease of this token.
        if st.revoked || token_of(req.token.as_ref()) != Some(token) {
            return Err(Status::failed_precondition(
                "the lease of this token is no longer held",
            ));
        }
        // (2) EffectScheduled(op), not completed.
        let handle = sink.handle();
        let ev = journal_io::read_event(handle, &run, req.op)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        let Some(ev) = ev else {
            return Err(Status::failed_precondition(format!(
                "no journal event at op {}",
                req.op
            )));
        };
        let EventBody::EffectScheduled(es) = &ev.body else {
            return Err(Status::failed_precondition(format!(
                "op {} is not an effect",
                req.op
            )));
        };
        let later = journal_io::read_journal_from(handle, &run, req.op + 1)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        if later.iter().any(|e| e.body.completes_op() == Some(req.op)) {
            return Err(Status::failed_precondition(format!(
                "effect {} is already completed",
                req.op
            )));
        }
        // (3) + (3b) Redacted names and literal-or-placeholder values.
        let request = match rebuild_request(&es.request, &req.redacted_values) {
            Ok(r) => r,
            Err(e) => {
                warn!(run = %rref.run_id, op = req.op, reason = %e, "StartEffect rejected");
                return rejected(e);
            }
        };
        let Some(driver) = sink.effect_driver() else {
            return rejected("this node has no effects bridge".into());
        };
        // (4) Rebuild and start through the run's ServerEffectDriver (Phase 6 unchanged).
        let live = LiveEffect {
            op: req.op,
            request,
            http_retry: req.http_retry.map(|r| EffectRetry {
                max_attempts: r.max_attempts,
                initial_interval_ms: r.initial_interval_ms,
                max_interval_ms: r.max_interval_ms,
                backoff_coefficient: r.backoff_coefficient,
                non_retryable_kinds: r.non_retryable_kinds,
            }),
            scheduled_ts_nanos: ev.ts_nanos,
            redrive: req.redrive,
        };
        driver.start(rref, token.n, live);
        hooks
            .remote
            .record(WireDir::Out, "start_effect_result", &[b"started"]);
        Ok(v1::StartEffectResponse {
            result: Some(R::Started(true)),
        })
    }
}

async fn sleep_until_opt(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use ostro_types::journal::{Header, HttpRequest};

    use super::*;

    fn http(headers: &[(&str, &str)], redact: &[&str]) -> EffectRequest {
        EffectRequest::Http(HttpRequest {
            method: "POST".into(),
            url: "http://x.test/pay".into(),
            headers: headers
                .iter()
                .map(|(n, v)| Header {
                    name: (*n).into(),
                    value: (*v).into(),
                })
                .collect(),
            body: None,
            timeout_ms: None,
            idempotency_key: None,
            redact: redact.iter().map(|s| (*s).to_owned()).collect(),
        })
    }

    fn hv(n: &str, v: &str) -> v1::Header {
        v1::Header {
            name: n.into(),
            value: v.into(),
        }
    }

    #[test]
    fn rebuild_takes_only_redacted_values_from_the_worker() {
        let j = http(
            &[("Idempotency-Key", "k1"), ("Authorization", "[redacted]")],
            &["authorization"],
        );
        let r = rebuild_request(&j, &[hv("Authorization", "Bearer {{secret:pay_token}}")])
            .expect("rebuilt");
        let EffectRequest::Http(r) = r else {
            panic!("http")
        };
        assert_eq!(r.headers[0].value, "k1");
        assert_eq!(r.headers[1].value, "Bearer {{secret:pay_token}}");
    }

    #[test]
    fn rebuild_rejects_mismatched_names() {
        let j = http(&[("Authorization", "[redacted]")], &["Authorization"]);
        assert!(rebuild_request(&j, &[]).is_err());
        assert!(rebuild_request(&j, &[hv("X-Other", "v")]).is_err());
        assert!(
            rebuild_request(&j, &[hv("Authorization", "a"), hv("Authorization", "b")]).is_err()
        );
        // A worker cannot overwrite a header the journal shows in clear.
        let j = http(&[("X-Clear", "c")], &[]);
        assert!(rebuild_request(&j, &[hv("X-Clear", "evil")]).is_err());
    }

    #[test]
    fn rebuild_rejects_template_injection() {
        let j = http(&[("Authorization", "[redacted]")], &["Authorization"]);
        for v in [
            "{{",
            "Bearer {{ not a template",
            "{{secret:}}",
            "{{secret:__ostro/token-key}}",
            "{{secret:pay_token}} {{other}}",
            "{{secret:Bad Name}}",
        ] {
            assert!(
                rebuild_request(&j, &[hv("Authorization", v)]).is_err(),
                "{v:?} must be rejected"
            );
        }
        assert!(rebuild_request(&j, &[hv("Authorization", "literal")]).is_ok());
    }

    #[test]
    fn hex_round_trip_for_trace_ids() {
        assert_eq!(unhex::<2>("0aff"), Some([0x0a, 0xff]));
        assert_eq!(unhex::<2>("0af"), None);
        assert_eq!(unhex::<2>("zzzz"), None);
    }
}
