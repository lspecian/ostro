//! The activation runner: one `Activate` = one engine activation (mirrors `ostro-server`'s
//! 04-16 `EngineExecutor::drive` without importing it).
//!
//! 1. **Module:** the runtime's L2 cache ([`Runtime::prepared`]) or `FetchBlob(module_sha)`
//!    (length + sha256 verified) and [`Runtime::prepare_cached`], i.e. the authenticated L1
//!    AOT disk cache under `--cache-dir` (node-local HMAC `aot.key`): a second start with the
//!    same cache dir loads the artefact instead of compiling; a tampered artefact fails the
//!    HMAC check and is recompiled.
//! 2. **Journal:** `FetchJournal [0, journal_len)` in pages ([`crate::journal_fetch`]), blob
//!    rows resolved and verified, every row decoded.
//! 3. **Inputs (ordered bridge, 04-16):** `Input{up_to_seq}` is a wake-up only: one task
//!    fetches `[next, up_to_seq)` strictly in seq order and forwards every input event
//!    (completions plus `SignalPayload` / `RunTerminated`) to the engine exactly once.
//! 4. **Engine:** [`Runtime::activate`] with the server's `EngineLimits` (adopted at
//!    `Welcome`, Pitfall 5), inside `ostro_engine::telemetry::with_traceparent` when
//!    `Activate.traceparent` is set (Phase 8 seam). The engine reports the outcome through
//!    [`RemoteSink::done`](crate::remote_sink::RemoteSink) exactly once (`Done`).
//! 5. A module or journal fetch failure sends `Done{Evicted{External}}` (the server
//!    re-activates); an invalid component sends `Done{AttemptFailed(ProtocolViolation)}`
//!    (no retry can fix it; the server fails the run, as embedded does).
//!
//! Logs `activate run=<id> attempt=<n>` at info when an activation starts (W3).

use std::sync::Arc;
use std::time::Duration;

use ostro_engine::config::EngineConfig;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::sink::{Activate, ActivationOutcome, ActivationSink, RunRef};
use ostro_types::RunId;
use ostro_types::journal::{Event, EventBody};
use ostro_worker_proto::v1;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::remote_sink::{RemoteSink, send_done};
use crate::{ActiveRun, Shared, WorkerConfig, WorkerEvent, blob_fetch, journal_fetch};

/// The engine configuration for the server's `EngineLimits` (zero = engine default) and
/// this worker's slots / memory budget.
pub fn engine_config(l: &v1::EngineLimits, cfg: &WorkerConfig) -> EngineConfig {
    let mut c = EngineConfig::default();
    let ms = |v: u64| Duration::from_millis(v);
    if l.hot_wait_ms > 0 {
        c.hot_wait = ms(l.hot_wait_ms);
    }
    if l.activation_timeout_ms > 0 {
        c.activation_timeout = ms(l.activation_timeout_ms);
    }
    if l.activation_fuel > 0 {
        c.activation_fuel = l.activation_fuel;
    }
    if l.continue_as_new_events > 0 {
        c.continue_as_new_events = l.continue_as_new_events;
    }
    if l.max_memory_bytes > 0 {
        c.pool.max_memory_bytes = l.max_memory_bytes;
    }
    if l.max_wasm_stack > 0 {
        c.max_wasm_stack = usize::try_from(l.max_wasm_stack).unwrap_or(c.max_wasm_stack);
    }
    if l.max_table_elements > 0 {
        c.max_table_elements =
            usize::try_from(l.max_table_elements).unwrap_or(c.max_table_elements);
    }
    if l.async_stack_size > 0 {
        c.async_stack_size = usize::try_from(l.async_stack_size).unwrap_or(c.async_stack_size);
    }
    if l.epoch_tick_ms > 0 {
        c.epoch_tick = ms(l.epoch_tick_ms);
    }
    // Live instances on a worker: its slots plus hot-parked activations.
    c.pool.total_instances = cfg.slots.saturating_mul(4).clamp(8, 1000);
    c.node_memory_budget = cfg.memory_bytes;
    c
}

/// Journal events the engine accepts as delivered inputs (the 04-16 `is_input` rule).
pub fn is_input(body: &EventBody) -> bool {
    body.completes_op().is_some()
        || matches!(
            body,
            EventBody::SignalPayload(_) | EventBody::RunTerminated(_)
        )
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Handle an `Activate`: register the activation and spawn its task. Returns `false` when
/// it was refused (malformed or no free slot; a `Done{Evicted}` was sent).
pub(crate) fn spawn(shared: &Arc<Shared>, act: v1::Activate) -> bool {
    let (Some(run), Some(token)) = (act.run.clone(), act.token) else {
        warn!("Activate without run or token: ignored");
        return false;
    };
    let evict = |reason: EvictReason| {
        send_done(
            shared,
            &run,
            &token,
            &ActivationOutcome::Evicted { reason },
            act.journal_len,
        );
    };
    let cancel = CancellationToken::new();
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    let generation = shared.next_generation();
    {
        let mut active = shared.active();
        if let Some(prev) = active.get(&run.run_id) {
            if prev.token == token {
                // Duplicate delivery of the same lease: already running.
                return true;
            }
            // A newer token for the same run: the old activation is fenced.
            prev.cancel.cancel();
            active.remove(&run.run_id);
        }
        if active.len() >= shared.cfg.slots as usize {
            drop(active);
            warn!(run = %hex(&run.run_id), "Activate without a free slot: evicted");
            evict(EvictReason::BudgetPressure);
            return false;
        }
        active.insert(
            run.run_id.clone(),
            ActiveRun {
                run: run.clone(),
                token,
                cancel: cancel.clone(),
                inputs: in_tx,
                generation,
            },
        );
    }
    shared.slots_changed.notify_one();
    let shared = Arc::clone(shared);
    tokio::spawn(async move {
        let key = run.run_id.clone();
        run_activation(&shared, act, run, token, cancel, in_rx).await;
        {
            let mut active = shared.active();
            if active.get(&key).is_some_and(|a| a.generation == generation) {
                active.remove(&key);
            }
        }
        shared.slots_changed.notify_one();
    });
    true
}

async fn run_activation(
    shared: &Arc<Shared>,
    act: v1::Activate,
    run: v1::RunRef,
    token: v1::Token,
    cancel: CancellationToken,
    inputs: mpsc::UnboundedReceiver<u64>,
) {
    let fail_start = |error: String, outcome: ActivationOutcome, run_id: String| {
        warn!(run = %run_id, attempt = token.n, %error, "activation could not start");
        shared.emit(WorkerEvent::ActivationFailed {
            run_id: run_id.clone(),
            error,
        });
        send_done(shared, &run, &token, &outcome, act.journal_len);
        shared.emit(WorkerEvent::ActivationEnded {
            run_id,
            attempt: token.n,
            outcome: format!("{outcome:?}"),
        });
    };
    let external = || ActivationOutcome::Evicted {
        reason: EvictReason::External,
    };
    let run_id = match RunId::from_bytes(&run.run_id) {
        Ok(r) => r.to_string(),
        Err(e) => {
            let msg = format!("bad run id: {e}");
            fail_start(
                msg.clone(),
                ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(msg)),
                hex(&run.run_id),
            );
            return;
        }
    };
    info!("activate run={run_id} attempt={}", token.n);
    shared.emit(WorkerEvent::ActivationStarted {
        run_id: run_id.clone(),
        attempt: token.n,
    });
    let Some(runtime) = shared.runtime() else {
        fail_start("no engine (no Welcome yet)".into(), external(), run_id);
        return;
    };
    let Ok(sha) = <[u8; 32]>::try_from(act.module_sha.as_slice()) else {
        let msg = format!("module_sha is {} bytes, expected 32", act.module_sha.len());
        fail_start(
            msg.clone(),
            ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(msg)),
            run_id,
        );
        return;
    };
    let Some(link) = shared.link() else {
        fail_start("no session".into(), external(), run_id);
        return;
    };
    // 1. Module.
    let prepared = match runtime.prepared(&sha) {
        Some(p) => p,
        None => {
            let bytes = match blob_fetch::fetch(&link, &run, &token, &sha, None).await {
                Ok(b) => b,
                Err(e) => {
                    fail_start(format!("module {}: {e}", hex(&sha)), external(), run_id);
                    return;
                }
            };
            match runtime.prepare_cached(&bytes).await {
                Ok(p) => p,
                Err(e) => {
                    let msg = format!("invalid module {}: {e}", hex(&sha));
                    fail_start(
                        msg.clone(),
                        ActivationOutcome::AttemptFailed(AttemptFailure::ProtocolViolation(msg)),
                        run_id,
                    );
                    return;
                }
            }
        }
    };
    if cancel.is_cancelled() {
        fail_start("cancelled before start".into(), external(), run_id);
        return;
    }
    // 2. Journal.
    let journal = match journal_fetch::fetch(&link, &run, &token, 0, act.journal_len).await {
        Ok(j) => j,
        Err(e) => {
            fail_start(format!("journal: {e}"), external(), run_id);
            return;
        }
    };
    if cancel.is_cancelled() {
        fail_start("cancelled before start".into(), external(), run_id);
        return;
    }
    let end = journal.len() as u64;
    // 3. Ordered input bridge.
    let (eng_tx, eng_rx) = mpsc::unbounded_channel();
    let bridge = tokio::spawn(input_bridge(
        Arc::clone(shared),
        run.clone(),
        token,
        end,
        inputs,
        eng_tx,
        cancel.clone(),
    ));
    // 4. Engine.
    let sink = Arc::new(RemoteSink::new(
        Arc::clone(shared),
        run.clone(),
        token,
        cancel.clone(),
        end,
    ));
    let eng_act = Activate {
        run: RunRef {
            partition: u16::try_from(run.partition).unwrap_or(u16::MAX),
            run_id: run_id.clone(),
        },
        attempt: token.n,
        module_sha: sha,
        journal,
        inputs: Some(eng_rx),
        cancel: cancel.clone(),
    };
    let traceparent = (!act.traceparent.is_empty()).then(|| act.traceparent.clone());
    let outcome = ostro_engine::telemetry::with_traceparent(
        traceparent,
        runtime.activate(
            &prepared,
            eng_act,
            Arc::clone(&sink) as Arc<dyn ActivationSink>,
        ),
    )
    .await;
    bridge.abort();
    shared.emit(WorkerEvent::ActivationEnded {
        run_id,
        attempt: token.n,
        outcome: format!("{outcome:?}"),
    });
}

/// Forward input events to the engine strictly in journal order; `hints` (`Input.up_to_seq`,
/// exclusive) are wake-ups only.
async fn input_bridge(
    shared: Arc<Shared>,
    run: v1::RunRef,
    token: v1::Token,
    mut next: u64,
    mut hints: mpsc::UnboundedReceiver<u64>,
    tx: mpsc::UnboundedSender<Event>,
    cancel: CancellationToken,
) {
    loop {
        let up_to = tokio::select! {
            _ = cancel.cancelled() => return,
            h = hints.recv() => match h {
                Some(h) => h,
                None => return,
            },
        };
        // Coalesce queued hints.
        let mut up_to = up_to;
        while let Ok(h) = hints.try_recv() {
            up_to = up_to.max(h);
        }
        if up_to <= next {
            continue;
        }
        let Some(link) = shared.link() else {
            return;
        };
        match journal_fetch::fetch(&link, &run, &token, next, up_to).await {
            Ok(events) => {
                for ev in events {
                    next = ev.seq + 1;
                    if is_input(&ev.body) && tx.send(ev).is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                // The engine's next append conflicts and the run is re-activated.
                warn!(error = %e, "input bridge: journal fetch failed; inputs stop");
                return;
            }
        }
    }
}
