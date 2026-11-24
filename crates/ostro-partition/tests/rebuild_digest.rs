//! STOR-06 rebuild property (plan 02-15): for random workloads, apply chunk sizes,
//! checkpoint cadences and restart points, the live state equals the state rebuilt from the
//! log after deleting every `state/pNNNN` (logical digest, rolling hash, applied offset).
//!
//! Chunking must never leak into state (Pitfall 9): the same log is rebuilt twice, with two
//! different chunk sizes, and both rebuilds must equal the live state. (Re-running the
//! workload from scratch would not produce the same *log*: the live shuffler and timer
//! service append on their own schedule, so the comparison is made on one log.)
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use common::gen_effects::EffectOp;
use common::gen_lifecycle::LifecycleOp;
use common::gen_pause::PauseOp;
use common::gen_waits::WaitOp;
use common::{GenRun, OPEN_T, T, copy_dir, run_on, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::store::keys::{decode_effect_key, decode_ready_key};
use ostro_partition::store::values::{self, RunRecord};
use ostro_partition::{
    ApplyOutcome, MachineConfig, Node, NodeConfig, PartitionSlot, StartMode, Table, query,
};
use ostro_types::command::{
    AttemptTokenMsg, CompleteRun, DLQ_DISMISS_REASON_MAX, Derived, DlqUpdate, EventsAppended,
    JournalEvent, MAX_RUN_TAGS, MAX_TENANT_LEN, MAX_TRACEPARENT_LEN, MODULE_SHA_LEN, OutboxMsg,
    RetryRun, ScheduleTimer, SendMessage, Signal, StartRun, TerminalStatus, derived, dlq_update,
    outbox_msg,
};
use ostro_types::{
    AttemptToken, Command, INLINE_PAYLOAD_LIMIT, PartitionCount, PartitionId, PayloadRef, RunId,
};
use proptest::prelude::*;
use tokio::time::timeout;

const P: u16 = 2;

// ------------------------------------------------------------------------------ scenario

#[derive(Clone, Debug)]
struct Cfg {
    max_chunk_cmds: usize,
    checkpoint_batches: u32,
    checkpoint_every: Duration,
}

impl Cfg {
    fn machine(&self) -> MachineConfig {
        MachineConfig {
            max_chunk_cmds: self.max_chunk_cmds,
            checkpoint_batches: self.checkpoint_batches,
            checkpoint_every: self.checkpoint_every,
            ..MachineConfig::default()
        }
    }
}

#[derive(Clone, Debug)]
enum EvMode {
    Valid,
    /// A token that is not the current attempt (Fenced).
    StaleToken,
    /// `from_seq` that does not continue the journal (SeqMismatch).
    OutOfSeq,
}

#[derive(Clone, Debug)]
enum Op {
    Start {
        p: u16,
        key: Option<u8>,
        oversize: bool,
        delayed: bool,
    },
    Grant {
        p: u16,
        i: usize,
    },
    Events {
        p: u16,
        i: usize,
        mode: EvMode,
        timer: Option<u64>,
        signal: Option<usize>,
        complete: bool,
    },
    Raw {
        p: u16,
        bytes: Vec<u8>,
    },
    Restart(Cfg),
    /// Plan 04-12: StartRun with fields 14..18 set; `bad` picks a limit violation
    /// (rejected Invalid) instead.
    StartV4 {
        p: u16,
        bad: Option<u8>,
    },
    /// Plan 04-12: EventsAppended ending in `Suspend`, optionally with one event and a
    /// timer (due in the past, so the timer service wakes the run again).
    Suspend {
        p: u16,
        i: usize,
        mode: EvMode,
        event: bool,
        timer: Option<u64>,
    },
    /// Plan 05-02 hook: wait/signal/token/cancel commands from `common::gen_waits` (05-08).
    Waits(Option<WaitOp>),
    /// Plan 05-02 hook: lifecycle commands from `common::gen_lifecycle` (05-09).
    Lifecycle(Option<LifecycleOp>),
    /// Plan 06-03: effect commands from `common::gen_effects` (RegisterEffect/CancelEffect,
    /// CompleteEffect, terminal cleanup, stale LeaderAnnounced).
    Effects(Option<EffectOp>),
    /// Plan 07-04: pause/resume commands from `common::gen_pause` (with inputs, timeouts,
    /// terminate and RetryRun around them).
    Pause(Option<PauseOp>),
    /// Plan 11-08: `DlqUpdate` (Cmd 45) marks around failing / timed-out runs.
    Dlq(DlqOp),
}

/// Plan 11-08: one dead-letter mark on driver run `i` of partition `p`, optionally after
/// ending the run (`fail`: `Some(false)` Failed, `Some(true)` Completed, a non-DLQ status) and
/// optionally followed by a `RetryRun` (so a later failure opens a new episode).
#[derive(Clone, Debug)]
struct DlqOp {
    p: u16,
    i: usize,
    fail: Option<bool>,
    /// 0 HookStarted, 1 Dismissed, 2 Requeued.
    kind: u8,
    /// Requeue mode 0..=3 (3 is invalid).
    mode: u32,
    /// 0 the run's current episode, 1 a stale one (current - 1), 2 zero.
    episode: u8,
    /// A malformation: 0 17-byte run id, 1 no op, 2 reason too long, 3 bad hook / target id.
    bad: Option<u8>,
    hook: u8,
    twice: bool,
    retry: bool,
}

/// Accepted `DlqUpdate`s across the test binary (coverage check of `fixed_scenario_dlq`).
static DLQ_ACCEPTED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn dlq_strategy() -> impl Strategy<Value = DlqOp> {
    (
        (
            0..P,
            any::<usize>(),
            proptest::option::weighted(0.6, proptest::bool::weighted(0.2)),
            0u8..3,
            0u32..4,
        ),
        (
            prop_oneof![6 => Just(0u8), 1 => Just(1u8), 1 => Just(2u8)],
            proptest::option::weighted(0.15, 0u8..4),
            0u8..3,
            proptest::bool::weighted(0.3),
            proptest::bool::weighted(0.3),
        ),
    )
        .prop_map(
            |((p, i, fail, kind, mode), (episode, bad, hook, twice, retry))| DlqOp {
                p,
                i,
                fail,
                kind,
                mode,
                episode,
                bad,
                hook,
                twice,
                retry,
            },
        )
}

fn cfg_strategy() -> impl Strategy<Value = Cfg> {
    (
        1usize..=64,
        1u32..=300,
        prop_oneof![
            Just(Duration::from_millis(1)),
            Just(Duration::from_millis(50)),
            Just(Duration::from_secs(3600)),
        ],
    )
        .prop_map(
            |(max_chunk_cmds, checkpoint_batches, checkpoint_every)| Cfg {
                max_chunk_cmds,
                checkpoint_batches,
                checkpoint_every,
            },
        )
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let p = 0..P;
    prop_oneof![
        8 => (p.clone(), proptest::option::weighted(0.4, 0u8..6), proptest::bool::weighted(0.05), proptest::bool::weighted(0.15))
            .prop_map(|(p, key, oversize, delayed)| Op::Start { p, key, oversize, delayed }),
        6 => (p.clone(), any::<usize>()).prop_map(|(p, i)| Op::Grant { p, i }),
        8 => (
            p.clone(),
            any::<usize>(),
            prop_oneof![8 => Just(EvMode::Valid), 1 => Just(EvMode::StaleToken), 1 => Just(EvMode::OutOfSeq)],
            proptest::option::weighted(0.4, prop_oneof![0u64..10_000, Just(u64::MAX / 4)]),
            proptest::option::weighted(0.5, any::<usize>()),
            proptest::bool::weighted(0.2),
        )
            .prop_map(|(p, i, mode, timer, signal, complete)| Op::Events { p, i, mode, timer, signal, complete }),
        // Malformed input stays on the reject-and-continue path (empty / undecodable). Since
        // 13-04 (D-P13-6) bytes that decode with unknown fields or an unknown Cmd tag
        // fail-stop the partition instead (tests/apply_newer_format.rs), so such draws get a
        // leading 0x07 (field number 0: never decodable).
        1 => (p.clone(), proptest::collection::vec(any::<u8>(), 0..24))
            .prop_map(|(p, bytes)| Op::Raw { p, bytes: reject_path(bytes) }),
        1 => cfg_strategy().prop_map(Op::Restart),
        // Plan 04-12 additions (the arms above are unchanged).
        3 => (p.clone(), proptest::option::weighted(0.2, 0u8..4))
            .prop_map(|(p, bad)| Op::StartV4 { p, bad }),
        4 => (
            p.clone(),
            any::<usize>(),
            prop_oneof![8 => Just(EvMode::Valid), 1 => Just(EvMode::StaleToken), 1 => Just(EvMode::OutOfSeq)],
            any::<bool>(),
            proptest::option::weighted(0.4, 0u64..10_000),
        )
            .prop_map(|(p, i, mode, event, timer)| Op::Suspend { p, i, mode, event, timer }),
        // Plan 05-02 generator hooks (05-08 / 05-09 fill them; `None` = no command).
        2 => common::gen_waits::strategy().prop_map(Op::Waits),
        2 => common::gen_lifecycle::strategy().prop_map(Op::Lifecycle),
        // Plan 06-03 generator hook.
        2 => common::gen_effects::strategy().prop_map(Op::Effects),
        // Plan 07-04 generator hook.
        2 => common::gen_pause::strategy().prop_map(Op::Pause),
        // Plan 11-08: DLQ marks (weighted low).
        1 => dlq_strategy().prop_map(Op::Dlq),
    ]
}

#[derive(Clone, Debug)]
struct Scenario {
    initial: Cfg,
    ops: Vec<Op>,
    /// Chunk sizes used by the two rebuilds.
    rebuild_chunks: (usize, usize),
}

fn scenario_strategy() -> impl Strategy<Value = Scenario> {
    (
        cfg_strategy(),
        proptest::collection::vec(op_strategy(), 20..200),
        1usize..=64,
        1usize..=1024,
    )
        .prop_map(|(initial, ops, a, b)| Scenario {
            initial,
            ops,
            rebuild_chunks: (a, if a == b { b + 1 } else { b }),
        })
}

// -------------------------------------------------------------------------------- driver

/// Driver-side knowledge of one run (only used to produce mostly-valid commands).
#[derive(Clone, Debug)]
struct RunModel {
    id: RunId,
    token: Option<AttemptToken>,
    next_seq: u64,
}

/// `bytes` unchanged when empty or undecodable, else `0x07 ‖ bytes` (undecodable).
fn reject_path(bytes: Vec<u8>) -> Vec<u8> {
    if bytes.is_empty() || ostro_types::command::decode_command(&bytes).is_err() {
        bytes
    } else {
        let mut b = vec![0x07];
        b.extend(bytes);
        b
    }
}

async fn open(dir: &Path, cfg: &Cfg, mode: StartMode) -> Node {
    let mut nc = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(P).expect("non-zero"));
    nc.machine = cfg.machine();
    nc.mode = mode;
    nc.leader.shuffler.idle_poll = Duration::from_millis(20);
    timeout(OPEN_T, Node::open(nc))
        .await
        .expect("node open timed out")
        .expect("node open")
}

fn inline(b: &'static [u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from_static(b)))
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn signal_to(target: &RunId) -> Derived {
    intent(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg {
            kind: Some(outbox_msg::Kind::Signal(Signal {
                target_run: target.as_bytes().to_vec(),
                name: "s".into(),
                payload: inline(b"p"),
            })),
        }),
    }))
}

async fn submit(node: &Node, p: u16, c: Command) -> ApplyOutcome {
    let h = node.handle(PartitionId(p)).expect("ready");
    timeout(T, h.submit_one(c))
        .await
        .expect("submit timed out")
        .expect("submit")
}

/// The run's live `next_seq` (apply-journaled inputs such as signals move it past the
/// driver's model); `fallback` if the run is unknown.
fn live_next_seq(node: &Node, p: u16, id: &RunId, fallback: u64) -> u64 {
    node.handle(PartitionId(p))
        .expect("ready")
        .read_run(id)
        .ok()
        .flatten()
        .map_or(fallback, |r| r.next_seq)
}

/// Driver view of every run for the generator hooks, `next_seq` re-read from live state.
fn gen_view(node: &Node, runs: &[Vec<RunModel>]) -> Vec<GenRun> {
    runs.iter()
        .enumerate()
        .flat_map(|(p, rs)| rs.iter().map(move |r| (p as u16, r)))
        .map(|(p, r)| GenRun {
            partition: p,
            id: r.id,
            token: r.token,
            next_seq: live_next_seq(node, p, &r.id, r.next_seq),
        })
        .collect()
}

/// Submits generator-hook commands (outcomes are free: rejections are deterministic too).
async fn submit_all(node: &Node, cmds: Vec<(u16, Command)>) {
    for (p, c) in cmds {
        submit(node, p, c).await;
    }
}

/// Run every op against a live node under `dir`; graceful shutdown at the end.
async fn drive(dir: &Path, sc: &Scenario) {
    let mut node = open(dir, &sc.initial, StartMode::Leader).await;
    let mut runs: Vec<Vec<RunModel>> = vec![Vec::new(); usize::from(P)];
    for (n, op) in sc.ops.iter().enumerate() {
        match op {
            Op::Start {
                p,
                key,
                oversize,
                delayed,
            } => {
                let id = run_on(*p);
                let mut s = start_run(&id);
                if let Some(k) = key {
                    s.idempotency_key = format!("k{k}").into_bytes();
                }
                if *oversize {
                    // deliberately bypasses prepare_payload: rejected PayloadTooLarge
                    s.input = Some(PayloadRef::inline(Bytes::from(vec![
                        7u8;
                        INLINE_PAYLOAD_LIMIT
                            + 1
                    ])));
                }
                if *delayed {
                    s.ready_at_ms = u64::MAX / 4; // never due during the test
                }
                let o = submit(&node, *p, Command::start_run(s)).await;
                if o == ApplyOutcome::Accepted {
                    runs[usize::from(*p)].push(RunModel {
                        id,
                        token: None,
                        next_seq: 0,
                    });
                }
                if *oversize {
                    assert!(matches!(o, ApplyOutcome::Rejected(_)), "{o:?}");
                }
            }
            Op::Grant { p, i } => {
                let rs = &mut runs[usize::from(*p)];
                if rs.is_empty() {
                    continue;
                }
                let k = i % rs.len();
                let h = node.handle(PartitionId(*p)).expect("ready");
                // delayed (Scheduled) and terminal runs are refused: fine
                if let Ok(t) = timeout(T, h.grant_attempt(rs[k].id))
                    .await
                    .expect("grant timed out")
                {
                    rs[k].token = Some(t);
                }
            }
            Op::Events {
                p,
                i,
                mode,
                timer,
                signal,
                complete,
            } => {
                let pi = usize::from(*p);
                if runs[pi].is_empty() {
                    continue;
                }
                let k = i % runs[pi].len();
                let other = usize::from((*p + 1) % P);
                let target = match signal {
                    Some(j) if !runs[other].is_empty() => {
                        Some(runs[other][j % runs[other].len()].id)
                    }
                    Some(_) => Some(run_on((*p + 1) % P)), // unknown run: dropped at apply
                    None => None,
                };
                let r = &mut runs[pi][k];
                r.next_seq = live_next_seq(&node, *p, &r.id, r.next_seq);
                let base = r.token.unwrap_or(AttemptToken {
                    epoch: ostro_types::Epoch(1),
                    n: 1,
                });
                let token = match mode {
                    EvMode::StaleToken => AttemptToken {
                        epoch: base.epoch,
                        n: base.n.wrapping_add(5),
                    },
                    _ => base,
                };
                let from = match mode {
                    EvMode::OutOfSeq => r.next_seq + 3,
                    _ => r.next_seq,
                };
                let mut d = Vec::new();
                if let Some(due) = timer {
                    d.push(intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                        op: n as u64 + 1,
                        due_at_ms: *due,
                    })));
                }
                if let Some(t) = target {
                    d.push(signal_to(&t));
                }
                if *complete {
                    d.push(intent(derived::Intent::CompleteRun(CompleteRun {
                        status: TerminalStatus::Completed as i32,
                        result: inline(b"out"),
                        completed_at_ms: 9,
                        resumable: false,
                        parent_event: None,
                    })));
                }
                let c = Command::events_appended(EventsAppended {
                    run_id: r.id.as_bytes().to_vec(),
                    token: Some(AttemptTokenMsg {
                        epoch: token.epoch.0,
                        n: token.n,
                    }),
                    from_seq: from,
                    events: vec![JournalEvent {
                        seq: from,
                        kind: 16,
                        payload: inline(b"e"),
                        ..Default::default()
                    }],
                    derived: d,
                });
                if submit(&node, *p, c).await == ApplyOutcome::Accepted {
                    r.next_seq = from + 1;
                }
            }
            Op::Raw { p, bytes } => {
                let h = node.handle(PartitionId(*p)).expect("ready");
                let rec = timeout(
                    T,
                    h.log().append(h.epoch(), vec![Bytes::from(bytes.clone())]),
                )
                .await
                .expect("raw append timed out")
                .expect("raw append");
                timeout(T, h.machine().wait_applied(rec.last))
                    .await
                    .expect("raw apply timed out")
                    .expect("raw apply");
            }
            Op::Restart(cfg) => {
                settle(&node).await;
                node.shutdown().await.expect("shutdown");
                node = open(dir, cfg, StartMode::Leader).await;
            }
            Op::StartV4 { p, bad } => {
                let id = run_on(*p);
                let mut s = StartRun {
                    module_sha: vec![0x5a; MODULE_SHA_LEN],
                    tags: vec![format!("op:{n}"), "env:test".into()],
                    tenant: "acme".into(),
                    traceparent: format!("00-{n:032x}-{n:016x}-01"),
                    run_timeout_ms: 60_000,
                    ..start_run(&id)
                };
                match bad {
                    Some(0) => s.module_sha = vec![1; MODULE_SHA_LEN - 1],
                    Some(1) => s.tags = vec!["t".into(); MAX_RUN_TAGS + 1],
                    Some(2) => s.tenant = "t".repeat(MAX_TENANT_LEN + 1),
                    Some(_) => s.traceparent = "p".repeat(MAX_TRACEPARENT_LEN + 1),
                    None => {}
                }
                let o = submit(&node, *p, Command::start_run(s)).await;
                if bad.is_some() {
                    assert!(matches!(o, ApplyOutcome::Rejected(_)), "{o:?}");
                } else {
                    assert_eq!(o, ApplyOutcome::Accepted);
                    runs[usize::from(*p)].push(RunModel {
                        id,
                        token: None,
                        next_seq: 0,
                    });
                }
            }
            Op::Suspend {
                p,
                i,
                mode,
                event,
                timer,
            } => {
                let rs = &mut runs[usize::from(*p)];
                if rs.is_empty() {
                    continue;
                }
                let k = i % rs.len();
                let r = &mut rs[k];
                r.next_seq = live_next_seq(&node, *p, &r.id, r.next_seq);
                let base = r.token.unwrap_or(AttemptToken {
                    epoch: ostro_types::Epoch(1),
                    n: 1,
                });
                let token = match mode {
                    EvMode::StaleToken => AttemptToken {
                        epoch: base.epoch,
                        n: base.n.wrapping_add(5),
                    },
                    _ => base,
                };
                let from = match mode {
                    EvMode::OutOfSeq => r.next_seq + 3,
                    _ => r.next_seq,
                };
                let mut d = Vec::new();
                if let Some(due) = timer {
                    d.push(intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                        op: n as u64 + 1,
                        due_at_ms: *due,
                    })));
                }
                d.push(Derived::suspend());
                let events = if *event {
                    vec![JournalEvent {
                        seq: from,
                        kind: 16,
                        payload: inline(b"e"),
                        ..Default::default()
                    }]
                } else {
                    vec![]
                };
                let added = events.len() as u64;
                let c = Command::events_appended(EventsAppended {
                    run_id: r.id.as_bytes().to_vec(),
                    token: Some(AttemptTokenMsg {
                        epoch: token.epoch.0,
                        n: token.n,
                    }),
                    from_seq: from,
                    events,
                    derived: d,
                });
                // Not Running (never granted, already suspended, terminal): rejected, fine.
                if submit(&node, *p, c).await == ApplyOutcome::Accepted {
                    r.next_seq = from + added;
                }
            }
            Op::Waits(w) => {
                if let Some(w) = w {
                    let view = gen_view(&node, &runs);
                    submit_all(&node, common::gen_waits::commands(w, &view, n)).await;
                }
            }
            Op::Lifecycle(l) => {
                if let Some(l) = l {
                    let view = gen_view(&node, &runs);
                    submit_all(&node, common::gen_lifecycle::commands(l, &view, n)).await;
                }
            }
            Op::Effects(e) => {
                if let Some(e) = e {
                    let view = gen_view(&node, &runs);
                    submit_all(&node, common::gen_effects::commands(e, &view, n)).await;
                }
            }
            Op::Pause(o) => {
                if let Some(o) = o {
                    let view = gen_view(&node, &runs);
                    submit_all(&node, common::gen_pause::commands(o, &view, n)).await;
                }
            }
            Op::Dlq(d) => drive_dlq(&node, &mut runs, d, n).await,
        }
    }
    settle(&node).await;
    node.shutdown().await.expect("shutdown");
}

/// Plan 11-08: ends the run (optionally), submits the mark (optionally twice), then
/// (optionally) resumes it. Outcomes are free: rejections are deterministic too.
async fn drive_dlq(node: &Node, runs: &mut [Vec<RunModel>], d: &DlqOp, n: usize) {
    let pi = usize::from(d.p);
    if runs[pi].is_empty() {
        return;
    }
    let k = d.i % runs[pi].len();
    let r = &mut runs[pi][k];
    if let Some(completed) = d.fail {
        r.next_seq = live_next_seq(node, d.p, &r.id, r.next_seq);
        let t = r.token.unwrap_or(AttemptToken {
            epoch: ostro_types::Epoch(1),
            n: 1,
        });
        let status = if completed {
            TerminalStatus::Completed
        } else {
            TerminalStatus::Failed
        };
        let c = Command::events_appended(EventsAppended {
            run_id: r.id.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg {
                epoch: t.epoch.0,
                n: t.n,
            }),
            from_seq: r.next_seq,
            events: vec![JournalEvent {
                seq: r.next_seq,
                kind: 16,
                payload: inline(b"e"),
                ..Default::default()
            }],
            derived: vec![intent(derived::Intent::CompleteRun(CompleteRun {
                status: status as i32,
                result: inline(b"out"),
                // Unique per op: a later failure is a new episode.
                completed_at_ms: 1_000 + n as u64,
                resumable: true,
                parent_event: None,
            }))],
        });
        if submit(node, d.p, c).await == ApplyOutcome::Accepted {
            r.next_seq += 1;
        }
    }
    let current = node
        .handle(PartitionId(d.p))
        .expect("ready")
        .read_run(&r.id)
        .ok()
        .flatten()
        .and_then(|rec| rec.completed_at_ms)
        .unwrap_or(0);
    let episode = match d.episode {
        0 => current,
        1 => current.saturating_sub(1),
        _ => 0,
    };
    let other = run_on(d.p);
    let target = if d.bad == Some(3) {
        vec![1u8; 17]
    } else {
        other.as_bytes().to_vec()
    };
    let op = match d.kind {
        0 => dlq_update::Op::HookStarted(dlq_update::HookStarted {
            hook_run_id: if d.bad == Some(3) {
                target
            } else {
                // A small set of hook ids, so a second, different hook happens.
                let mut b = *r.id.as_bytes();
                b[17] ^= 0x80 | d.hook;
                b.to_vec()
            },
        }),
        1 => dlq_update::Op::Dismissed(dlq_update::Dismissed {
            reason: if d.bad == Some(2) {
                "r".repeat(DLQ_DISMISS_REASON_MAX + 1)
            } else {
                format!("reason {}", d.hook)
            },
            at_ms: 2_000 + n as u64,
        }),
        _ => dlq_update::Op::Requeued(dlq_update::Requeued {
            mode: d.mode,
            new_run_id: if d.mode == dlq_update::REQUEUE_RESUME && d.bad != Some(3) {
                Vec::new()
            } else {
                target
            },
            at_ms: 3_000 + n as u64,
        }),
    };
    let run_id = if d.bad == Some(0) {
        r.id.as_bytes()[..17].to_vec()
    } else {
        r.id.as_bytes().to_vec()
    };
    let c = Command::dlq_update(DlqUpdate {
        run_id,
        op: (d.bad != Some(1)).then_some(op),
        episode_completed_at_ms: episode,
    });
    if submit(node, d.p, c.clone()).await == ApplyOutcome::Accepted {
        DLQ_ACCEPTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    if d.twice {
        submit(node, d.p, c).await;
    }
    if d.retry {
        submit(
            node,
            d.p,
            Command::retry_run(RetryRun {
                run_id: r.id.as_bytes().to_vec(),
                requested_at_ms: 4_000 + n as u64,
            }),
        )
        .await;
    }
}

/// Outboxes empty and no log growth for 150 ms (shuffler and timers idle).
async fn settle(node: &Node) {
    common::wait_drained(node, T).await;
    let ends = || -> Vec<u64> {
        (0..P)
            .map(|p| {
                node.handle(PartitionId(p))
                    .expect("ready")
                    .log()
                    .log_end()
                    .0
            })
            .collect()
    };
    let deadline = tokio::time::Instant::now() + T;
    let mut last = ends();
    loop {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let now = ends();
        if now == last && common::outbox_rows(node) == 0 {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "node never settled");
        last = now;
    }
}

#[derive(Debug, PartialEq, Eq)]
struct PartState {
    digest: [u8; 32],
    hash: [u8; 32],
    applied: u64,
}

fn states(node: &Node) -> Vec<PartState> {
    node.slots()
        .iter()
        .map(|s| match s {
            PartitionSlot::Ready(h) => {
                let meta = h.store().read_meta().expect("meta");
                PartState {
                    digest: h.store().digest().expect("digest"),
                    hash: meta.state_hash,
                    applied: meta.applied.0,
                }
            }
            PartitionSlot::Failed { partition, reason } => {
                panic!("partition {} failed: {reason}", partition.0)
            }
        })
        .collect()
}

/// Copy `src` to `dst`, delete every state dir in the copy.
fn stateless_copy(src: &Path, dst: &Path) {
    copy_dir(src, dst);
    std::fs::remove_dir_all(dst.join("state")).expect("delete state");
}

/// Drive `sc`, then compare live state against two rebuilds with different chunk sizes.
async fn check(sc: Scenario) -> Result<(), String> {
    let root = ostro_testkit::fs::tempdir();
    let a = root.path().join("live");
    drive(&a, &sc).await;
    let b = root.path().join("rebuild-a");
    let c = root.path().join("rebuild-b");
    stateless_copy(&a, &b);
    stateless_copy(&a, &c);

    let live = open(&a, &sc.initial, StartMode::Inspect).await;
    let mut cb = sc.initial.clone();
    cb.max_chunk_cmds = sc.rebuild_chunks.0;
    let mut cc = sc.initial.clone();
    cc.max_chunk_cmds = sc.rebuild_chunks.1;
    let nb = open(&b, &cb, StartMode::Inspect).await;
    let nc = open(&c, &cc, StartMode::Inspect).await;
    let (sa, sb, sc_) = (states(&live), states(&nb), states(&nc));
    for p in 0..usize::from(P) {
        let ends = live
            .handle(PartitionId(p as u16))
            .expect("ready")
            .log()
            .log_end()
            .0;
        if sa[p].applied != ends {
            return Err(format!(
                "p{p}: live applied {} != LEO {ends}",
                sa[p].applied
            ));
        }
        if sa[p] != sb[p] {
            return Err(format!(
                "p{p}: live {:?} != rebuild(chunk {}) {:?}",
                sa[p], sc.rebuild_chunks.0, sb[p]
            ));
        }
        if sa[p] != sc_[p] {
            return Err(format!(
                "p{p}: live {:?} != rebuild(chunk {}) {:?}",
                sa[p], sc.rebuild_chunks.1, sc_[p]
            ));
        }
    }
    effects_invariant(&live)?;
    pause_invariant(&live)?;
    live.shutdown().await.expect("shutdown");
    nb.shutdown().await.expect("shutdown");
    nc.shutdown().await.expect("shutdown");
    Ok(())
}

/// P-8 (06-03): every `effects` row belongs to an existing run that is live or a resumable
/// terminal; no row survives a non-resumable terminal transition.
fn effects_invariant(node: &Node) -> Result<(), String> {
    for p in 0..P {
        let h = node.handle(PartitionId(p)).expect("ready");
        let reader = h.store().reader();
        let rows = reader
            .prefix(Table::Effects, &[], usize::MAX)
            .expect("effects scan");
        for (k, _) in rows {
            let (run, op) = decode_effect_key(&k).ok_or("malformed effects key")?;
            let Some(rec) = query::run_record(&reader, &run).expect("run record") else {
                return Err(format!("p{p}: effects row {run:?}/{op} without a run"));
            };
            if rec.status.is_terminal() && !rec.resumable {
                return Err(format!(
                    "p{p}: effects row {run:?}/{op} survived non-resumable {:?}",
                    rec.status
                ));
            }
        }
    }
    Ok(())
}

/// 07-04: no run is both paused and holding a ready row (the `enqueue_ready` gate), and no
/// terminal run is paused (the terminal hook clears the mark, B7).
fn pause_invariant(node: &Node) -> Result<(), String> {
    for p in 0..P {
        let h = node.handle(PartitionId(p)).expect("ready");
        let reader = h.store().reader();
        for (k, v) in reader
            .prefix(Table::Runs, &[], usize::MAX)
            .expect("runs scan")
        {
            let rec: RunRecord = values::decode(&v).map_err(|e| e.to_string())?;
            if rec.pause.is_none() {
                continue;
            }
            if rec.status.is_terminal() {
                return Err(format!(
                    "p{p}: terminal run {k:?} is paused ({:?})",
                    rec.status
                ));
            }
            if rec.ready.is_some() {
                return Err(format!("p{p}: paused run {k:?} has a ready slot"));
            }
        }
        for (k, _) in reader
            .prefix(Table::Ready, &[], usize::MAX)
            .expect("ready scan")
        {
            let (_, _, _, run) = decode_ready_key(&k).ok_or("malformed ready key")?;
            let rec = query::run_record(&reader, &run)
                .expect("run record")
                .ok_or_else(|| format!("p{p}: ready row {run:?} without a run"))?;
            if rec.pause.is_some() {
                return Err(format!("p{p}: paused run {run:?} has a ready row"));
            }
        }
    }
    Ok(())
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: ostro_testkit::proptest_cases(24),
        max_shrink_iters: 64,
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    fn live_state_equals_rebuilt_state(sc in scenario_strategy()) {
        let r = rt().block_on(check(sc));
        prop_assert!(r.is_ok(), "{}", r.unwrap_err());
    }
}

/// A fixed heavy scenario: many cross-partition signals, timers, restarts with chunk 1 and
/// checkpoint every batch, rebuilt with chunk 1 and 1024.
#[test]
fn fixed_scenario_with_restarts() {
    let tiny = Cfg {
        max_chunk_cmds: 1,
        checkpoint_batches: 1,
        checkpoint_every: Duration::from_millis(1),
    };
    let lazy = Cfg {
        max_chunk_cmds: 64,
        checkpoint_batches: 300,
        checkpoint_every: Duration::from_secs(3600),
    };
    let mut ops = Vec::new();
    for i in 0..60usize {
        let p = (i % 2) as u16;
        ops.push(Op::Start {
            p,
            key: (i % 5 == 0).then_some((i % 3) as u8),
            oversize: i == 17,
            delayed: i % 11 == 0,
        });
        ops.push(Op::Grant { p, i });
        ops.push(Op::Events {
            p,
            i,
            mode: match i % 9 {
                4 => EvMode::StaleToken,
                7 => EvMode::OutOfSeq,
                _ => EvMode::Valid,
            },
            timer: (i % 2 == 0).then_some(i as u64),
            signal: Some(i * 7),
            complete: i % 4 == 3,
        });
        if i % 13 == 0 {
            ops.push(Op::Raw {
                p,
                bytes: vec![0xff, i as u8, 0x01],
            });
        }
        if i == 20 {
            ops.push(Op::Restart(lazy.clone()));
        }
        if i == 40 {
            ops.push(Op::Restart(tiny.clone()));
        }
    }
    let sc = Scenario {
        initial: tiny,
        ops,
        rebuild_chunks: (1, 1024),
    };
    rt().block_on(check(sc)).expect("live == rebuilt");
}

/// Plan 04-12: a fixed scenario over the Phase 4 paths: rich and invalid StartV4 starts,
/// grants (Running rows), suspends with and without timers (timer wake-ups re-ready the
/// runs), re-grants, completions and a restart; rebuilt with chunk 1 and 1024.
#[test]
fn fixed_scenario_phase4() {
    let tiny = Cfg {
        max_chunk_cmds: 1,
        checkpoint_batches: 1,
        checkpoint_every: Duration::from_millis(1),
    };
    let mut ops = Vec::new();
    for i in 0..40usize {
        let p = (i % 2) as u16;
        ops.push(Op::StartV4 {
            p,
            bad: (i % 7 == 3).then_some((i % 4) as u8),
        });
        ops.push(Op::Grant { p, i });
        ops.push(Op::Suspend {
            p,
            i,
            mode: match i % 9 {
                4 => EvMode::StaleToken,
                7 => EvMode::OutOfSeq,
                _ => EvMode::Valid,
            },
            event: i % 3 != 0,
            timer: (i % 2 == 0).then_some(i as u64),
        });
        ops.push(Op::Grant { p, i: i + 1 });
        ops.push(Op::Events {
            p,
            i: i + 1,
            mode: EvMode::Valid,
            timer: None,
            signal: (i % 5 == 0).then_some(i),
            complete: i % 4 == 1,
        });
        if i == 20 {
            ops.push(Op::Restart(tiny.clone()));
        }
    }
    let sc = Scenario {
        initial: tiny,
        ops,
        rebuild_chunks: (1, 1024),
    };
    rt().block_on(check(sc)).expect("live == rebuilt");
}

/// Plan 06-03: a fixed scenario over the effect paths: effects scheduled on granted runs
/// (some suspended), completions (valid, duplicate, ahead, blob), cancels, resumable and
/// non-resumable terminals, stale announcements and a restart (an accepted LeaderAnnounced:
/// the P-9 wake of suspended runs with open effects); rebuilt with chunk 1 and 1024.
#[test]
fn fixed_scenario_effects() {
    let tiny = Cfg {
        max_chunk_cmds: 1,
        checkpoint_batches: 1,
        checkpoint_every: Duration::from_millis(1),
    };
    let mut ops = Vec::new();
    for i in 0..30usize {
        let p = (i % 2) as u16;
        ops.push(Op::Start {
            p,
            key: None,
            oversize: false,
            delayed: false,
        });
        ops.push(Op::Grant { p, i });
        ops.push(Op::Effects(Some(EffectOp::Schedule {
            run: i,
            kind: (i % 4) as u32,
            repeat: i % 5 == 0,
            cancel_back: (i % 7 == 3).then_some(0),
            suspend: i % 3 == 0,
        })));
        ops.push(Op::Effects(Some(EffectOp::Schedule {
            run: i,
            kind: 1,
            repeat: false,
            cancel_back: None,
            suspend: false,
        })));
        if i % 4 != 1 {
            ops.push(Op::Effects(Some(EffectOp::Complete {
                run: i,
                op: if i % 6 == 5 {
                    Err(1)
                } else {
                    Ok((i % 2) as u64)
                },
                blob: i % 8 == 2,
                twice: i % 3 == 1,
            })));
        }
        if i % 5 == 2 {
            ops.push(Op::Effects(Some(EffectOp::Cancel { run: i, back: 0 })));
        }
        if i % 4 == 3 {
            ops.push(Op::Effects(Some(EffectOp::Terminal {
                run: i,
                resumable: i % 8 == 3,
                failed: true,
            })));
        }
        if i % 9 == 4 {
            ops.push(Op::Effects(Some(EffectOp::Announce { run: i })));
        }
        if i == 15 {
            ops.push(Op::Restart(tiny.clone()));
        }
    }
    let sc = Scenario {
        initial: tiny,
        ops,
        rebuild_chunks: (1, 1024),
    };
    rt().block_on(check(sc)).expect("live == rebuilt");
}

/// Plan 07-04: a fixed pause/resume scenario: own runs paused (operator, auto, stale auto)
/// around run timeouts, inputs, rejected executor writes, resume, terminate and RetryRun, plus
/// pauses, resumes and executor writes on driver runs, with a restart; rebuilt with chunk 1
/// and 1024.
#[test]
fn fixed_scenario_pause() {
    use common::gen_pause::PauseKind;
    let tiny = Cfg {
        max_chunk_cmds: 1,
        checkpoint_batches: 1,
        checkpoint_every: Duration::from_millis(1),
    };
    let kinds = [PauseKind::Operator, PauseKind::Auto, PauseKind::StaleAuto];
    let mut ops = Vec::new();
    for i in 0..24usize {
        let p = (i % 2) as u16;
        ops.push(Op::Start {
            p,
            key: None,
            oversize: false,
            delayed: i % 7 == 6,
        });
        ops.push(Op::Grant { p, i });
        ops.push(Op::Pause(Some(PauseOp::Own {
            p,
            run_timeout: [0, 5, u64::MAX / 8][i % 3],
            grant: i % 4 != 0,
            pause: kinds[i % 3],
            fire_timeout: i % 2 == 1,
            inputs: i % 3 != 2,
            write: i % 2 == 0,
            resume: i % 4 == 1,
            terminate: i % 5 == 2,
            retry: i % 3 == 1 || i % 5 == 2,
            pause_after_retry: i % 6 == 4,
        })));
        ops.push(Op::Pause(Some(PauseOp::Pause {
            i,
            kind: kinds[(i + 1) % 3],
        })));
        ops.push(Op::Pause(Some(PauseOp::Events {
            i,
            intent: (i % 3) as u8,
        })));
        ops.push(Op::Pause(Some(PauseOp::Grant { i })));
        ops.push(Op::Pause(Some(PauseOp::Signal { i })));
        if i % 3 == 0 {
            ops.push(Op::Pause(Some(PauseOp::Resume { i })));
        }
        if i % 5 == 4 {
            ops.push(Op::Pause(Some(PauseOp::Terminate { i })));
            ops.push(Op::Pause(Some(PauseOp::Retry { i })));
        }
        if i % 4 == 2 {
            ops.push(Op::Pause(Some(PauseOp::Cancel { i })));
        }
        if i == 12 {
            ops.push(Op::Restart(tiny.clone()));
        }
    }
    let sc = Scenario {
        initial: tiny,
        ops,
        rebuild_chunks: (1, 1024),
    };
    rt().block_on(check(sc)).expect("live == rebuilt");
}

/// Plan 11-08: a fixed DLQ scenario: runs granted, failed (and some completed), all three marks
/// (valid, repeated, stale-episode, malformed), RetryRun resumes and re-failures (new
/// episodes), a restart; rebuilt with chunk 1 and 1024.
#[test]
fn fixed_scenario_dlq() {
    let tiny = Cfg {
        max_chunk_cmds: 1,
        checkpoint_batches: 1,
        checkpoint_every: Duration::from_millis(1),
    };
    let mut ops = Vec::new();
    for i in 0..24usize {
        let p = (i % 2) as u16;
        ops.push(Op::Start {
            p,
            key: None,
            oversize: false,
            delayed: false,
        });
        ops.push(Op::Grant { p, i });
        for (j, kind) in [0u8, 1, 2].into_iter().enumerate() {
            ops.push(Op::Dlq(DlqOp {
                p,
                i,
                fail: (j == 0).then_some(i % 7 == 6),
                kind,
                mode: ((i + j) % 4) as u32,
                episode: if (i + j) % 5 == 4 { 1 } else { 0 },
                bad: ((i + j) % 6 == 5).then_some(((i + j) % 4) as u8),
                hook: (i % 3) as u8,
                twice: (i + j) % 3 == 0,
                retry: j == 2 && i % 2 == 0,
            }));
        }
        // A second failure episode on the resumed runs (re-granted first).
        if i % 2 == 0 {
            ops.push(Op::Grant { p, i });
            ops.push(Op::Dlq(DlqOp {
                p,
                i,
                fail: Some(false),
                kind: 0,
                mode: 0,
                episode: 0,
                bad: None,
                hook: ((i + 1) % 3) as u8,
                twice: false,
                retry: false,
            }));
        }
        if i == 12 {
            ops.push(Op::Restart(tiny.clone()));
        }
    }
    let sc = Scenario {
        initial: tiny,
        ops,
        rebuild_chunks: (1, 1024),
    };
    rt().block_on(check(sc)).expect("live == rebuilt");
    let accepted = DLQ_ACCEPTED.load(std::sync::atomic::Ordering::Relaxed);
    assert!(accepted >= 10, "only {accepted} DlqUpdate accepted");
}

// ---------------------------------------------------------------------- schedule workloads

/// Plan 08-07: `ScheduleFire` workloads from `common::gen_schedule` (fires, manual fires,
/// advances, completions, cancels, terminations, continue-as-new, re-created schedules and
/// stale fires) applied to partition `OWNER` of `PC` with `apply_command`, one committed batch
/// per command. The state equals a one-batch replay of the same log (digest and rolling
/// hash), and the cron fires started at most one run per `(schedule, fire_time)` (exactly one
/// for every `AllowAll` fire above the watermark with a fresh key).
mod schedule_workloads {
    use std::collections::BTreeMap;

    use super::common::gen_schedule::{self, Finish, Model, OWNER, PC, ScheduleOp, Step, pcount};
    use ostro_partition::store::values::{self, RunRecord};
    use ostro_partition::{Cursor, PartitionStore, StoreConfig, Table, apply_command};
    use ostro_types::{Epoch, PartitionId, RunId};
    use proptest::prelude::*;

    fn open() -> (tempfile::TempDir, PartitionStore, Cursor) {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(OWNER),
            pcount(PC),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        (dir, store, cur)
    }

    fn read_run(store: &PartitionStore, run: &RunId) -> Option<RunRecord> {
        ostro_partition::query::run_record(&store.reader(), run).expect("run record")
    }

    pub(super) fn check(ops: &[ScheduleOp]) -> Result<(), String> {
        check_with(ops).map(|_| ())
    }

    /// [`check`], returning the model (for the fixed workload's non-vacuity asserts).
    fn check_with(ops: &[ScheduleOp]) -> Result<Model, String> {
        let (_d, store, mut cur) = open();
        let mut log: Vec<Vec<u8>> = Vec::new();
        let mut model = Model::new();
        let mut cmds = vec![ostro_types::Command::leader_announced(1, 1, 0)];
        let mut i = 0;
        loop {
            for c in cmds.drain(..) {
                let bytes = ostro_types::command::encode_command(&c).to_vec();
                let mut b = store.batch();
                let mut actions = Vec::new();
                let off = cur.next;
                apply_command(&mut b, &mut cur, off, Epoch(1), &bytes, &mut actions)
                    .map_err(|e| format!("apply: {e}"))?;
                let mu = cur.meta_update();
                b.commit(&mu).map_err(|e| format!("commit: {e}"))?;
                log.push(bytes);
            }
            let Some(op) = ops.get(i) else { break };
            i += 1;
            cmds = model.commands(op, 1, &|r| read_run(&store, r).map(|rec| rec.next_seq));
        }

        // One-batch replay.
        let (_d2, replay, mut rc) = open();
        let mut b = replay.batch();
        let mut actions = Vec::new();
        for bytes in &log {
            let off = rc.next;
            apply_command(&mut b, &mut rc, off, Epoch(1), bytes, &mut actions)
                .map_err(|e| format!("replay: {e}"))?;
        }
        let mu = rc.meta_update();
        b.commit(&mu).map_err(|e| format!("replay commit: {e}"))?;
        let (live, rebuilt) = (
            store.digest().expect("digest"),
            replay.digest().expect("digest"),
        );
        if live != rebuilt {
            return Err("live digest != replay digest".into());
        }
        if cur.hash != rc.hash {
            return Err("live rolling hash != replay rolling hash".into());
        }

        // Exactly-once: runs per cron key.
        let mut per_key: BTreeMap<String, usize> = BTreeMap::new();
        for (_, v) in store
            .reader()
            .prefix(Table::Runs, &[], usize::MAX)
            .expect("runs")
        {
            let rec: RunRecord = values::decode(&v).expect("decode run");
            let key = String::from_utf8_lossy(&rec.idempotency_key).into_owned();
            if key.starts_with("__cron/") && !key.contains("/manual/") {
                *per_key.entry(key).or_default() += 1;
            }
        }
        if let Some((k, n)) = per_key.iter().find(|(_, n)| **n > 1) {
            return Err(format!("{n} runs for {k}"));
        }
        for k in &model.must_start {
            if per_key.get(k) != Some(&1) {
                return Err(format!("AllowAll fire {k} did not start exactly one run"));
            }
        }
        Ok(model)
    }

    /// Every op kind and overlap policy on both schedules, in a fixed order.
    #[test]
    fn fixed_schedule_workload() {
        let mut ops = Vec::new();
        for round in 0..6usize {
            for s in 0..2 {
                for overlap in 0..5u32 {
                    ops.push(ScheduleOp::Fire {
                        s,
                        step: Step::Next(1),
                        overlap,
                    });
                    ops.push(ScheduleOp::Fire {
                        s,
                        step: Step::Repeat,
                        overlap,
                    });
                }
                ops.push(ScheduleOp::Manual {
                    s,
                    req: (round % 2) as u8,
                    overlap: 1,
                });
                ops.push(ScheduleOp::Advance {
                    s,
                    ahead: 1,
                    skipped: 2,
                });
                ops.push(ScheduleOp::Fire {
                    s,
                    step: Step::Back,
                    overlap: 2,
                });
                let how = [
                    Finish::Complete,
                    Finish::Fail,
                    Finish::Terminate,
                    Finish::Cancel,
                    Finish::ContinueAsNew,
                    Finish::Complete,
                ][round];
                ops.push(ScheduleOp::Finish { s, back: 0, how });
                ops.push(ScheduleOp::Finish {
                    s,
                    back: 2,
                    how: Finish::Complete,
                });
                if round == 3 {
                    ops.push(ScheduleOp::Recreate { s });
                    ops.push(ScheduleOp::StaleFire { s });
                }
            }
        }
        let model = check_with(&ops).expect("live == replay, exactly once");
        assert!(model.must_start.len() >= 12, "{:?}", model.must_start);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: ostro_testkit::proptest_cases(128),
            max_shrink_iters: 256,
            failure_persistence: None,
            .. ProptestConfig::default()
        })]

        #[test]
        fn schedule_workloads(ops in gen_schedule::strategy()) {
            let r = check(&ops);
            prop_assert!(r.is_ok(), "{}", r.unwrap_err());
        }
    }
}
