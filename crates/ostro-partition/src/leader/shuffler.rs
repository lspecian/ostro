//! Outbox shuffler (STOR-12, delivery half): moves this partition's outbox rows to their
//! target partitions.
//!
//! The ordering rule is load-bearing (research Pattern 9, Pitfall 7): per target there is
//! **at most one `InboundBatch` in flight**, and every batch starts at the lowest
//! untruncated seq and is ascending. The target dedupes `(src, seq)` against its
//! `inbound_seen` and rejects gaps, so with this rule a batch can only ever overlap what the
//! target already applied (a harmless re-send after a lost ack or a restart), never skip.
//!
//! Per target, one round is: read up to `max_batch_msgs` rows from `outbox_prefix(t)` ->
//! `router.deliver(t, InboundBatch{src, msgs})` -> once the target answers `Accepted` or
//! `Duplicate` (it applied the batch, or had already) -> append
//! `OutboxTruncated{t, upto_seq = last seq of the batch}` here. Only then is `t` free for the
//! next round. Errors (target down, lost ack, lost request) back off 50 ms doubling to 1 s
//! and re-read from the table, so the same rows are resent.
//!
//! Wake-ups: [`Action::OutboxAppended`] from the apply machine, the completion of a round,
//! a backoff deadline, or the `idle_poll` tick (fallback, also covers a lagged action
//! stream). A restart or leadership change recreates the task, which starts from the table.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ostro_types::command::{InboundBatch, InboundMsg, OutboxMsg, OutboxTruncated};
use ostro_types::{Command, PartitionId};
use prost::Message;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, error, warn};

use super::LeaderCtx;
use crate::apply::{Action, ApplyOutcome, RejectReason};
use crate::handle::PartitionError;
use crate::store::Table;
use crate::store::keys::{decode_outbox_key, outbox_prefix};

/// Shuffler tuning.
#[derive(Clone, Debug)]
pub struct ShufflerConfig {
    /// Most messages per `InboundBatch`. Default 256.
    pub max_batch_msgs: usize,
    /// Poll interval when no outbox action arrived. Default 200 ms.
    pub idle_poll: Duration,
}

impl Default for ShufflerConfig {
    fn default() -> Self {
        Self {
            max_batch_msgs: 256,
            idle_poll: Duration::from_millis(200),
        }
    }
}

/// Encoded message bytes per batch (at least one message is always sent). Keeps the
/// target's `InboundBatch` envelope well below the 4 MiB envelope limit even with 256 KiB
/// inline payloads.
const MAX_BATCH_BYTES: usize = 1 << 20;
const BACKOFF_MIN: Duration = Duration::from_millis(50);
const BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Start the shuffler of `ctx.partition`. Runs until `ctx.shutdown` turns true.
pub fn spawn(ctx: LeaderCtx, cfg: ShufflerConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(Shuffler::new(ctx, cfg).run())
}

/// How one delivery round of a target ended.
enum Round {
    /// Delivered and truncated through `upto`.
    Done { upto: u64 },
    /// The target rejected the batch (`SeqGap` is a shuffler bug; anything else a malformed
    /// row). Retried after a backoff from a fresh read.
    Rejected(RejectReason),
    /// Delivery failed (target down, request or ack lost): resend later.
    DeliverFailed(PartitionError),
    /// Delivered, but the local `OutboxTruncated` append failed.
    TruncateFailed(PartitionError),
}

#[derive(Clone, Copy)]
struct Backoff {
    delay: Duration,
    until: Option<Instant>,
}

struct Shuffler {
    ctx: LeaderCtx,
    cfg: ShufflerConfig,
    src: PartitionId,
    in_flight: BTreeSet<u16>,
    backoff: Vec<Backoff>,
    /// Rows seen by the last read per target (approximate `ostro_outbox_rows`).
    rows: Vec<usize>,
    rounds: JoinSet<(u16, Round)>,
}

impl Shuffler {
    fn new(ctx: LeaderCtx, cfg: ShufflerConfig) -> Self {
        let n = usize::from(ctx.count.get());
        Self {
            src: ctx.partition,
            ctx,
            cfg,
            in_flight: BTreeSet::new(),
            backoff: vec![
                Backoff {
                    delay: BACKOFF_MIN,
                    until: None,
                };
                n
            ],
            rows: vec![0; n],
            rounds: JoinSet::new(),
        }
    }

    #[allow(clippy::disallowed_methods)] // leader services may read the clock (backoff)
    async fn run(mut self) {
        let mut shutdown = self.ctx.shutdown.clone();
        let mut actions = self.ctx.handle.machine().subscribe_actions();
        let mut tick = tokio::time::interval(self.cfg.idle_poll.max(Duration::from_millis(1)));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut scan = true;
        loop {
            if *shutdown.borrow() {
                break;
            }
            if scan {
                scan = false;
                if !self.scan(Instant::now()) {
                    break;
                }
            }
            let next_backoff = self.backoff.iter().filter_map(|b| b.until).min();
            tokio::select! {
                biased;
                _ = shutdown.wait_for(|v| *v) => break,
                Some(r) = self.rounds.join_next(), if !self.rounds.is_empty() => {
                    match r {
                        Ok((t, round)) => {
                            if !self.finish(t, round, Instant::now()) {
                                break;
                            }
                        }
                        Err(e) => {
                            // A panicking round cannot be attributed to its target; stop and
                            // let the next leadership start from the table.
                            error!(partition = self.src.0, error = %e, "shuffler round panicked");
                            break;
                        }
                    }
                    scan = true;
                }
                r = actions.recv() => match r {
                    Ok(a) => {
                        if a.iter().any(|x| matches!(x, Action::OutboxAppended { .. })) {
                            scan = true;
                        }
                    }
                    Err(RecvError::Lagged(_)) => scan = true,
                    Err(RecvError::Closed) => {
                        // The apply machine is gone; nothing more can be truncated.
                        debug!(partition = self.src.0, "shuffler: action stream closed");
                        break;
                    }
                },
                _ = async {
                    match next_backoff {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                } => scan = true,
                _ = tick.tick() => scan = true,
            }
        }
        // In-flight rounds are abandoned: a delivered-but-untruncated batch is resent by the
        // next shuffler and deduplicated by the target.
        self.rounds.abort_all();
        while self.rounds.join_next().await.is_some() {}
    }

    /// Start a round for every idle target with outbox rows. `false` if the store is
    /// unreadable (the partition is failing).
    fn scan(&mut self, now: Instant) -> bool {
        for t in 0..self.ctx.count.get() {
            let ti = usize::from(t);
            if self.in_flight.contains(&t) {
                continue;
            }
            if let Some(until) = self.backoff[ti].until {
                if until > now {
                    continue;
                }
                self.backoff[ti].until = None;
            }
            let rows = match self.ctx.handle.reader().prefix(
                Table::Outbox,
                &outbox_prefix(t),
                self.cfg.max_batch_msgs.max(1),
            ) {
                Ok(r) => r,
                Err(e) => {
                    error!(partition = self.src.0, error = %e, "shuffler: outbox read failed");
                    return false;
                }
            };
            self.rows[ti] = rows.len();
            if rows.is_empty() {
                continue;
            }
            let mut msgs = Vec::with_capacity(rows.len());
            let mut bytes = 0usize;
            for (k, v) in &rows {
                let seq = match decode_outbox_key(k) {
                    Some((kt, seq)) if kt == t => seq,
                    _ => {
                        error!(
                            partition = self.src.0,
                            target = t,
                            "shuffler: malformed outbox key"
                        );
                        break;
                    }
                };
                let msg = match OutboxMsg::decode(&v[..]) {
                    Ok(m) => m,
                    Err(e) => {
                        error!(partition = self.src.0, target = t, seq, error = %e,
                            "shuffler: undecodable outbox row");
                        break;
                    }
                };
                if !msgs.is_empty() && bytes + v.len() > MAX_BATCH_BYTES {
                    break;
                }
                bytes += v.len();
                msgs.push(InboundMsg {
                    seq,
                    msg: Some(msg),
                });
            }
            let Some(upto) = msgs.last().map(|m| m.seq) else {
                // The lowest row is unusable: nothing can be sent without a gap.
                self.back_off(ti, now);
                continue;
            };
            let batch = InboundBatch {
                src: u32::from(self.src.0),
                msgs,
            };
            self.in_flight.insert(t);
            let router = Arc::clone(&self.ctx.router);
            let handle = self.ctx.handle.clone();
            self.rounds.spawn(async move {
                let target = PartitionId(t);
                let round = match router.deliver(target, batch).await {
                    Ok(ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. }) => {
                        let trunc = Command::outbox_truncated(OutboxTruncated {
                            target: u32::from(t),
                            upto_seq: upto,
                        });
                        match handle.submit_one(trunc).await {
                            Ok(ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. }) => {
                                Round::Done { upto }
                            }
                            Ok(ApplyOutcome::Rejected(r)) => {
                                Round::TruncateFailed(PartitionError::Rejected(r))
                            }
                            Err(e) => Round::TruncateFailed(e),
                        }
                    }
                    Ok(ApplyOutcome::Rejected(r)) => Round::Rejected(r),
                    Err(e) => Round::DeliverFailed(e),
                };
                (t, round)
            });
            metrics::counter!("ostro_shuffler_batches_total",
                "src" => self.src.0.to_string(), "target" => t.to_string())
            .increment(1);
        }
        metrics::gauge!("ostro_outbox_rows", "partition" => self.src.0.to_string())
            .set(self.rows.iter().sum::<usize>() as f64);
        true
    }

    /// Book-keeping after a round of target `t`. `false` = stop the shuffler.
    fn finish(&mut self, t: u16, round: Round, now: Instant) -> bool {
        let ti = usize::from(t);
        self.in_flight.remove(&t);
        match round {
            Round::Done { upto } => {
                debug!(partition = self.src.0, target = t, upto, "outbox delivered");
                self.backoff[ti] = Backoff {
                    delay: BACKOFF_MIN,
                    until: None,
                };
            }
            Round::Rejected(r) => {
                error!(partition = self.src.0, target = t, reason = ?r,
                    "shuffler: target rejected an outbox batch");
                self.retry(ti, now);
            }
            Round::DeliverFailed(e) => {
                debug!(partition = self.src.0, target = t, error = %e,
                    "shuffler: delivery failed, will resend");
                self.retry(ti, now);
            }
            Round::TruncateFailed(e) => {
                if self.ctx.handle.machine().check_healthy().is_err() {
                    warn!(partition = self.src.0, error = %e,
                        "shuffler: partition failing, stopping");
                    return false;
                }
                warn!(partition = self.src.0, target = t, error = %e,
                    "shuffler: outbox truncation failed, will resend");
                self.retry(ti, now);
            }
        }
        true
    }

    fn retry(&mut self, ti: usize, now: Instant) {
        metrics::counter!("ostro_shuffler_retries_total").increment(1);
        self.back_off(ti, now);
    }

    fn back_off(&mut self, ti: usize, now: Instant) {
        let b = &mut self.backoff[ti];
        b.until = Some(now + b.delay);
        b.delay = (b.delay * 2).min(BACKOFF_MAX);
    }
}
