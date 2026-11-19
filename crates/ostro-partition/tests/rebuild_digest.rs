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
use common::{T, copy_dir, run_on, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::{ApplyOutcome, MachineConfig, Node, NodeConfig, PartitionSlot, StartMode};
use ostro_types::command::{
    AttemptTokenMsg, CompleteRun, Derived, EventsAppended, JournalEvent, MAX_RUN_TAGS,
    MAX_TENANT_LEN, MAX_TRACEPARENT_LEN, MODULE_SHA_LEN, OutboxMsg, ScheduleTimer, SendMessage,
    Signal, StartRun, TerminalStatus, derived, outbox_msg,
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
        1 => (p.clone(), proptest::collection::vec(any::<u8>(), 0..24)).prop_map(|(p, bytes)| Op::Raw { p, bytes }),
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

async fn open(dir: &Path, cfg: &Cfg, mode: StartMode) -> Node {
    let mut nc = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(P).expect("non-zero"));
    nc.machine = cfg.machine();
    nc.mode = mode;
    nc.leader.shuffler.idle_poll = Duration::from_millis(20);
    timeout(T, Node::open(nc))
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
        }
    }
    settle(&node).await;
    node.shutdown().await.expect("shutdown");
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
    live.shutdown().await.expect("shutdown");
    nb.shutdown().await.expect("shutdown");
    nc.shutdown().await.expect("shutdown");
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
