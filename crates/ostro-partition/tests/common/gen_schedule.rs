//! `ScheduleFire` workloads (plan **08-07**; TRIG-02, TRIG-03): command builders shared by
//! `tests/apply_schedule.rs` and the proptest generator used by `tests/rebuild_digest.rs`
//! (`schedule_workloads`).
//!
//! [`strategy`] produces interleavings of [`ScheduleOp`] for one or two schedules owned by
//! partition [`OWNER`] of [`PC`] partitions: cron fires (increasing, repeated and older fire
//! times, every overlap policy), manual fires (with repeated request ids), catch-up
//! `Advance`s, run completions (completed, failed, terminated, graceful cancel,
//! continue-as-new), schedule re-creation (a new incarnation, W4) and stale fires of an older
//! incarnation. [`Model`] turns them into commands for a direct `apply_command` harness. It
//! also records which `(schedule, fire_time)` keys must have started exactly one run
//! (`AllowAll` fires above the watermark whose key was never used before).

use bytes::Bytes;
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EventsAppended,
    OutboxMsg, RequestCancel, ScheduleFire, ScheduleFireKind, SendMessage, StartRun,
    TerminalStatus, TerminateRun, cron_idempotency_key, cron_manual_idempotency_key, derived,
};
use ostro_types::routing::schedule_owner;
use ostro_types::{Command, PartitionCount, PartitionId, PayloadRef, RunId};
use proptest::prelude::*;

/// Partition count of the schedule workloads.
pub const PC: u16 = 4;
/// The partition that owns every generated schedule.
pub const OWNER: u16 = 0;

pub fn pcount(n: u16) -> PartitionCount {
    PartitionCount::new(n).expect("non-zero")
}

/// The first `n` names `sched-<i>` owned by partition `p` of `count`.
pub fn owned_names(p: u16, count: u16, n: usize) -> Vec<String> {
    (0..)
        .map(|i| format!("sched-{i}"))
        .filter(|s| schedule_owner(s, pcount(count)) == PartitionId(p))
        .take(n)
        .collect()
}

/// A name NOT owned by partition `p` of `count`.
pub fn foreign_name(p: u16, count: u16) -> String {
    (0..)
        .map(|i| format!("other-{i}"))
        .find(|s| schedule_owner(s, pcount(count)) != PartitionId(p))
        .expect("some name hashes elsewhere")
}

/// Run id `n` on partition `p` (deterministic, distinct per `n`).
pub fn run_id(p: u16, n: u64) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[10..].copy_from_slice(&n.to_be_bytes());
    RunId::from_bytes(&b).expect("18 bytes")
}

/// The `StartRun` a schedule fire carries, with idempotency key `key`.
pub fn sched_start(run: &RunId, key: String, at_ms: u64) -> StartRun {
    StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: "cron-wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"tick"))),
        idempotency_key: key.into_bytes(),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: at_ms,
        requested_at_ms: at_ms,
        rng_seed: vec![3; 32],
        ..Default::default()
    }
}

/// A cron `Fire` of `schedule` at `ft` starting `run`.
pub fn fire_cmd(schedule: &str, ft: u64, overlap: u32, created: u64, run: &RunId) -> ScheduleFire {
    ScheduleFire {
        schedule: schedule.into(),
        fire_time_ms: ft,
        kind: ScheduleFireKind::Fire as i32,
        start: Some(sched_start(run, cron_idempotency_key(schedule, ft), ft)),
        overlap,
        request_id: vec![],
        skipped: 0,
        schedule_created_at_ms: created,
    }
}

/// A trigger-now (`Manual`) of `schedule` at `at` starting `run`.
pub fn manual_cmd(
    schedule: &str,
    at: u64,
    req: &[u8],
    overlap: u32,
    created: u64,
    run: &RunId,
) -> ScheduleFire {
    ScheduleFire {
        schedule: schedule.into(),
        fire_time_ms: at,
        kind: ScheduleFireKind::Manual as i32,
        start: Some(sched_start(
            run,
            cron_manual_idempotency_key(schedule, req),
            at,
        )),
        overlap,
        request_id: req.to_vec(),
        skipped: 0,
        schedule_created_at_ms: created,
    }
}

/// A catch-up `Advance` of `schedule` to `ft`.
pub fn advance_cmd(schedule: &str, ft: u64, skipped: u32, created: u64) -> ScheduleFire {
    ScheduleFire {
        schedule: schedule.into(),
        fire_time_ms: ft,
        kind: ScheduleFireKind::Advance as i32,
        start: None,
        overlap: 0,
        request_id: vec![],
        skipped,
        schedule_created_at_ms: created,
    }
}

/// `AttemptGranted{epoch, n}` for `run`.
pub fn grant_cmd(run: &RunId, epoch: u32, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch, n }),
        after_failure: false,
    })
}

/// `EventsAppended` without events, ending in `CompleteRun{status}`; `successor` adds the
/// continue-as-new `StartRun`.
pub fn finish_cmd(
    run: &RunId,
    token: (u32, u32),
    from_seq: u64,
    status: TerminalStatus,
    successor: Option<&RunId>,
) -> Command {
    let mut d = Vec::new();
    if let Some(next) = successor {
        let mut s = sched_start(next, String::new(), 50);
        s.workflow = "cron-wf".into();
        d.push(Derived::new(derived::Intent::SendMessage(SendMessage {
            msg: Some(OutboxMsg::start_run(s)),
        })));
    }
    d.push(Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: None,
        completed_at_ms: 900,
        resumable: false,
        parent_event: None,
    })));
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: token.0,
            n: token.1,
        }),
        from_seq,
        events: vec![],
        derived: d,
    })
}

/// How a generated op ends a run.
#[derive(Clone, Copy, Debug)]
pub enum Finish {
    Complete,
    Fail,
    Terminate,
    Cancel,
    ContinueAsNew,
}

/// Fire-time choice of a generated `Fire`.
#[derive(Clone, Copy, Debug)]
pub enum Step {
    /// `ahead` (1..=3) intervals above the schedule's highest fire time.
    Next(u8),
    /// The highest fire time again (a duplicate).
    Repeat,
    /// One interval below the highest fire time (a late duplicate).
    Back,
}

/// One generated schedule operation (`s` picks the schedule).
#[derive(Clone, Debug)]
pub enum ScheduleOp {
    Fire { s: usize, step: Step, overlap: u32 },
    Manual { s: usize, req: u8, overlap: u32 },
    Advance { s: usize, ahead: u8, skipped: u8 },
    Finish { s: usize, back: usize, how: Finish },
    Recreate { s: usize },
    StaleFire { s: usize },
}

/// Strategy for one [`ScheduleOp`].
pub fn op_strategy() -> BoxedStrategy<ScheduleOp> {
    let s = 0usize..2;
    let step = prop_oneof![
        6 => (1u8..=3).prop_map(Step::Next),
        1 => Just(Step::Repeat),
        1 => Just(Step::Back),
    ];
    let how = prop_oneof![
        3 => Just(Finish::Complete),
        1 => Just(Finish::Fail),
        1 => Just(Finish::Terminate),
        1 => Just(Finish::Cancel),
        1 => Just(Finish::ContinueAsNew),
    ];
    prop_oneof![
        8 => (s.clone(), step, 0u32..5).prop_map(|(s, step, overlap)| ScheduleOp::Fire { s, step, overlap }),
        2 => (s.clone(), 0u8..3, 0u32..5).prop_map(|(s, req, overlap)| ScheduleOp::Manual { s, req, overlap }),
        2 => (s.clone(), 0u8..3, 0u8..4).prop_map(|(s, ahead, skipped)| ScheduleOp::Advance { s, ahead, skipped }),
        6 => (s.clone(), 0usize..3, how).prop_map(|(s, back, how)| ScheduleOp::Finish { s, back, how }),
        1 => s.clone().prop_map(|s| ScheduleOp::Recreate { s }),
        1 => s.prop_map(|s| ScheduleOp::StaleFire { s }),
    ]
    .boxed()
}

/// A workload: 10..80 ops.
pub fn strategy() -> BoxedStrategy<Vec<ScheduleOp>> {
    proptest::collection::vec(op_strategy(), 10..80).boxed()
}

/// Fire-time interval.
pub const STEP_MS: u64 = 1_000;

#[derive(Clone, Debug)]
struct Sched {
    name: String,
    created: u64,
    /// Highest Fire/Advance time of the current incarnation (the expected watermark).
    watermark: u64,
    /// Highest fire time ever generated (fire times keep increasing across incarnations).
    top: u64,
    /// Every run id offered to a start, newest last.
    runs: Vec<RunId>,
}

/// Turns [`ScheduleOp`]s into commands for partition [`OWNER`].
#[derive(Clone, Debug)]
pub struct Model {
    scheds: Vec<Sched>,
    next_run: u64,
    /// Granted attempt counter per offered run.
    grants: std::collections::BTreeMap<RunId, u32>,
    /// Every `__cron/<s>/<ft>` key a fire carried.
    used: std::collections::BTreeSet<String>,
    /// Keys that must have started exactly one run.
    pub must_start: Vec<String>,
    clock: u64,
}

impl Default for Model {
    fn default() -> Self {
        Self::new()
    }
}

impl Model {
    pub fn new() -> Self {
        let scheds = owned_names(OWNER, PC, 2)
            .into_iter()
            .map(|name| Sched {
                name,
                created: 1,
                watermark: 0,
                top: 0,
                runs: Vec::new(),
            })
            .collect();
        Self {
            scheds,
            next_run: 1,
            grants: Default::default(),
            used: Default::default(),
            must_start: Vec::new(),
            clock: 10,
        }
    }

    pub fn names(&self) -> Vec<String> {
        self.scheds.iter().map(|s| s.name.clone()).collect()
    }

    fn fresh_run(&mut self) -> RunId {
        let r = run_id(OWNER, self.next_run);
        self.next_run += 1;
        r
    }

    /// Commands for `op`. `next_seq(run)` reads the run's live `next_seq` (`None` = unknown).
    pub fn commands(
        &mut self,
        op: &ScheduleOp,
        epoch: u32,
        next_seq: &dyn Fn(&RunId) -> Option<u64>,
    ) -> Vec<Command> {
        self.clock += 7;
        match *op {
            ScheduleOp::Fire { s, step, overlap } => {
                let run = self.fresh_run();
                let sc = &mut self.scheds[s];
                let ft = match step {
                    Step::Next(k) => sc.top + u64::from(k) * STEP_MS,
                    Step::Repeat => sc.top.max(STEP_MS),
                    Step::Back => sc.top.saturating_sub(STEP_MS).max(STEP_MS),
                };
                sc.top = sc.top.max(ft);
                sc.runs.push(run);
                let key = cron_idempotency_key(&sc.name, ft);
                let fresh_key = self.used.insert(key.clone());
                if ft > sc.watermark {
                    sc.watermark = ft;
                    if overlap == 2 && fresh_key {
                        self.must_start.push(key);
                    }
                }
                vec![Command::schedule_fire(fire_cmd(
                    &sc.name, ft, overlap, sc.created, &run,
                ))]
            }
            ScheduleOp::Manual { s, req, overlap } => {
                let run = self.fresh_run();
                let at = self.clock;
                let sc = &mut self.scheds[s];
                sc.runs.push(run);
                vec![Command::schedule_fire(manual_cmd(
                    &sc.name,
                    at,
                    &[b'r', req],
                    overlap,
                    sc.created,
                    &run,
                ))]
            }
            ScheduleOp::Advance { s, ahead, skipped } => {
                let sc = &mut self.scheds[s];
                let ft = (sc.top + u64::from(ahead) * STEP_MS).max(STEP_MS);
                sc.top = sc.top.max(ft);
                sc.watermark = sc.watermark.max(ft);
                vec![Command::schedule_fire(advance_cmd(
                    &sc.name,
                    ft,
                    u32::from(skipped),
                    sc.created,
                ))]
            }
            ScheduleOp::Recreate { s } => {
                let sc = &mut self.scheds[s];
                sc.created += 1;
                sc.watermark = 0;
                vec![]
            }
            ScheduleOp::StaleFire { s } => {
                let run = self.fresh_run();
                let sc = &mut self.scheds[s];
                if sc.created < 2 {
                    return vec![];
                }
                sc.top += STEP_MS;
                let ft = sc.top;
                self.used.insert(cron_idempotency_key(&sc.name, ft));
                vec![Command::schedule_fire(fire_cmd(
                    &sc.name,
                    ft,
                    2,
                    sc.created - 1,
                    &run,
                ))]
            }
            ScheduleOp::Finish { s, back, how } => {
                let sc = &self.scheds[s];
                let Some(&run) = sc.runs.iter().rev().nth(back % sc.runs.len().max(1)) else {
                    return vec![];
                };
                let at = self.clock;
                match how {
                    Finish::Terminate => vec![Command::terminate_run(TerminateRun {
                        run_id: run.as_bytes().to_vec(),
                        reason: "gen".into(),
                        terminated_at_ms: at,
                    })],
                    Finish::Cancel => vec![Command::request_cancel(RequestCancel {
                        run_id: run.as_bytes().to_vec(),
                        reason: "gen".into(),
                        requested_at_ms: at,
                    })],
                    Finish::Complete | Finish::Fail | Finish::ContinueAsNew => {
                        let Some(seq) = next_seq(&run) else {
                            return vec![];
                        };
                        let n = self.grants.entry(run).or_insert(0);
                        *n += 1;
                        let n = *n;
                        let (status, succ) = match how {
                            Finish::Complete => (TerminalStatus::Completed, None),
                            Finish::Fail => (TerminalStatus::Failed, None),
                            _ => (TerminalStatus::ContinuedAsNew, Some(self.fresh_run())),
                        };
                        vec![
                            grant_cmd(&run, epoch, n),
                            finish_cmd(&run, (epoch, n), seq, status, succ.as_ref()),
                        ]
                    }
                }
            }
        }
    }
}
