//! Rerun support on the execution side (Phase 11, OPS-02, D-P11-3/4/6/18), implemented by
//! **11-07**. The control plane (cut selection, replay check, submit) is 11-10.
//!
//! - [`analyze_cut`]: the cut-safety analyzer (D-P11-4), a pure function over decoded
//!   events. A cut `c` copies events `0..c` (`RunStarted` + the prefix). It is unsafe when
//!   the prefix holds an **open** `ChildStarted` (bound to the old parent), `EffectScheduled`
//!   (the bridge is bound to the old run) or `AwaitTokenCreated` (the token embeds the old
//!   run id), or a `SignalPayload` (96) without its `SignalMatched` (97). Open
//!   `TimerStarted` and `SignalWaitStarted` are allowed: [`prearm`] re-arms them. Valid cuts
//!   are `1 ..= index of the terminal event` (the prefix never contains a terminal kind
//!   26/27/28/71) and never beyond the journal end.
//! - Pairing: an op is the command event at seq `op`; it is closed by an input whose
//!   `EventBody::completes_op()` is `op` or by an `OpCancelled{op}`: the same pairing as
//!   [`crate::derive::rearm_unresolved`] (cross-checked in the tests). An op started at `s`
//!   and closed at `e` is open for every cut `s < c <= e` (`e = inf` when never closed), so
//!   one pass over the journal yields the safety of every cut ([`safe_cuts`]).
//! - [`step_cuts`] / [`resolve_step`]: the per-op view for "rerun from here" (11-10, 11-16).
//! - [`rerun_from_tag`] and [`prearm`]: the durable re-arm trigger (D-P11-6). On every
//!   activation of a run carrying `__ostro/rerun_from=<cut>` while `next_seq == cut` (no live
//!   event yet), the executor's shared prelude (`EngineExecutor::prelude`, used by the
//!   embedded executor and by Phase 7's `RemoteRunner`) submits
//!   `derive::rearm_unresolved(journal)` as a zero-event `EventsAppended` before the
//!   activation. Idempotent (RegisterWait is idempotent, timers are point keys), so a crash
//!   between grant and re-arm (failpoint `rerun::before_rearm`) just re-arms on the next
//!   activation.
//!
//! Every emission of the Phase 11 encodings goes through
//! [`crate::p11::p11_feature_enabled`].

use std::collections::{BTreeMap, HashMap, HashSet};

use ostro_partition::store::values::RunRecord;
use ostro_types::command::RERUN_FROM_TAG;
use ostro_types::journal::{CommandHash, Event, EventBody};

/// A safe cut: what the rerun will re-arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CutPlan {
    pub cut: u64,
    /// Open `TimerStarted` ops in the prefix (re-armed).
    pub open_timers: usize,
    /// Open `SignalWaitStarted` ops in the prefix (re-armed).
    pub open_waits: usize,
}

/// Why a cut is unsafe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CutUnsafe {
    /// The offending event (the earliest one in the prefix), or the cut itself for
    /// `terminal_or_out_of_range`.
    pub seq: u64,
    /// `open_child` | `open_effect` | `open_await_token` | `unmatched_signal` |
    /// `terminal_or_out_of_range`.
    pub reason: &'static str,
    /// The largest safe cut below the requested one (`None` if there is none).
    pub nearest_safe_seq: Option<u64>,
}

pub const REASON_OPEN_CHILD: &str = "open_child";
pub const REASON_OPEN_EFFECT: &str = "open_effect";
pub const REASON_OPEN_AWAIT_TOKEN: &str = "open_await_token";
pub const REASON_UNMATCHED_SIGNAL: &str = "unmatched_signal";
pub const REASON_OUT_OF_RANGE: &str = "terminal_or_out_of_range";

fn is_terminal(body: &EventBody) -> bool {
    matches!(
        body,
        EventBody::RunCompleted(_)
            | EventBody::RunFailed(_)
            | EventBody::RunContinuedAsNew(_)
            | EventBody::RunTerminated(_)
    )
}

/// Seq of the first event closing each op (`completes_op()` or `OpCancelled`), and of the
/// first `SignalMatched` of each `SignalPayload` seq.
fn closers(events: &[Event]) -> (HashMap<u64, u64>, HashMap<u64, u64>) {
    let mut closed = HashMap::new();
    let mut matched = HashMap::new();
    for ev in events {
        if let Some(op) = ev.body.completes_op() {
            closed.entry(op).or_insert(ev.seq);
        }
        match &ev.body {
            EventBody::OpCancelled(c) => {
                closed.entry(c.op).or_insert(ev.seq);
            }
            EventBody::SignalMatched(m) => {
                matched.entry(m.payload_seq).or_insert(ev.seq);
            }
            _ => {}
        }
    }
    (closed, matched)
}

/// The unsafe reason of the op / signal started by `ev`, if it blocks a cut while open.
fn unsafe_reason(body: &EventBody) -> Option<&'static str> {
    match body {
        EventBody::ChildStarted(_) => Some(REASON_OPEN_CHILD),
        EventBody::EffectScheduled(_) => Some(REASON_OPEN_EFFECT),
        EventBody::AwaitTokenCreated(_) => Some(REASON_OPEN_AWAIT_TOKEN),
        EventBody::SignalPayload(_) => Some(REASON_UNMATCHED_SIGNAL),
        _ => None,
    }
}

/// The first terminal event's seq (the upper bound of valid cuts), or the journal length.
fn max_cut(events: &[Event]) -> u64 {
    events
        .iter()
        .find(|e| is_terminal(&e.body))
        .map_or(events.len() as u64, |e| e.seq)
}

/// `safe[c]` for every cut `c` in `0..=events.len()`: one pass plus a difference array.
pub fn safe_cuts(events: &[Event]) -> Vec<bool> {
    let n = events.len();
    let (closed, matched) = closers(events);
    // cover[c] > 0 <=> some unsafe op is open at cut c (start s < c <= close e).
    let mut diff = vec![0i64; n + 2];
    for ev in events {
        if unsafe_reason(&ev.body).is_none() {
            continue;
        }
        let close = if matches!(ev.body, EventBody::SignalPayload(_)) {
            matched.get(&ev.seq)
        } else {
            closed.get(&ev.seq)
        };
        let lo = usize::try_from(ev.seq)
            .unwrap_or(usize::MAX)
            .saturating_add(1);
        let hi = close.map_or(n, |&e| usize::try_from(e).unwrap_or(n).min(n));
        if lo <= hi && lo <= n {
            diff[lo] += 1;
            diff[hi + 1] -= 1;
        }
    }
    let max = max_cut(events);
    let mut cover = 0i64;
    (0..=n)
        .map(|c| {
            cover += diff[c];
            c >= 1 && (c as u64) <= max && cover == 0
        })
        .collect()
}

fn nearest_below(safe: &[bool], cut: u64) -> Option<u64> {
    let top = usize::try_from(cut).unwrap_or(usize::MAX).min(safe.len());
    (1..top).rev().find(|&c| safe[c]).map(|c| c as u64)
}

/// Classifies cut `cut` of `events` (a source journal, `events[i].seq == i`).
pub fn analyze_cut(events: &[Event], cut: u64) -> Result<CutPlan, CutUnsafe> {
    let safe = safe_cuts(events);
    analyze_with(events, cut, &safe)
}

fn analyze_with(events: &[Event], cut: u64, safe: &[bool]) -> Result<CutPlan, CutUnsafe> {
    let nearest = || nearest_below(safe, cut);
    if cut == 0 || cut > max_cut(events) {
        return Err(CutUnsafe {
            seq: cut,
            reason: REASON_OUT_OF_RANGE,
            nearest_safe_seq: nearest(),
        });
    }
    let (closed, matched) = closers(events);
    let open = |seq: u64, sig: bool| {
        let close = if sig {
            matched.get(&seq)
        } else {
            closed.get(&seq)
        };
        close.is_none_or(|&e| e >= cut)
    };
    let mut plan = CutPlan {
        cut,
        open_timers: 0,
        open_waits: 0,
    };
    for ev in events.iter().take_while(|e| e.seq < cut) {
        let sig = matches!(ev.body, EventBody::SignalPayload(_));
        if !open(ev.seq, sig) {
            continue;
        }
        if let Some(reason) = unsafe_reason(&ev.body) {
            return Err(CutUnsafe {
                seq: ev.seq,
                reason,
                nearest_safe_seq: nearest(),
            });
        }
        match ev.body {
            EventBody::TimerStarted(_) => plan.open_timers += 1,
            EventBody::SignalWaitStarted(_) => plan.open_waits += 1,
            _ => {}
        }
    }
    Ok(plan)
}

/// One "rerun from here" candidate: the first event of an op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepCut {
    /// The op's first event (the cut that re-executes this op and everything after it).
    pub seq: u64,
    /// The step name for step ops (`StepAttemptFailed` / `StepCompleted`), else `None`.
    pub step: Option<String>,
    /// 1-based occurrence of this step name in the journal (0 for non-step ops).
    pub occurrence: u32,
    pub safe: Result<CutPlan, CutUnsafe>,
}

/// Every op start in seq order: step ops (a retry chain of `StepAttemptFailed` followed by
/// its `StepCompleted` is one op, identified by the command hash) and the other scheduling
/// commands (timer, effect, signal wait, child, await token). Terminal events are never
/// listed.
pub fn step_cuts(events: &[Event]) -> Vec<StepCut> {
    let safe = safe_cuts(events);
    let mut chains: HashSet<CommandHash> = HashSet::new();
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    let mut out = Vec::new();
    for ev in events {
        let (start, step) = match &ev.body {
            EventBody::StepAttemptFailed(f) => (chains.insert(f.meta.hash), Some(&f.name)),
            EventBody::StepCompleted(s) => (!chains.remove(&s.meta.hash), Some(&s.name)),
            EventBody::TimerStarted(_)
            | EventBody::EffectScheduled(_)
            | EventBody::SignalWaitStarted(_)
            | EventBody::ChildStarted(_)
            | EventBody::AwaitTokenCreated(_) => (true, None),
            _ => (false, None),
        };
        if !start {
            continue;
        }
        let occurrence = step.map_or(0, |name| {
            let c = counts.entry(name.clone()).or_insert(0);
            *c += 1;
            *c
        });
        out.push(StepCut {
            seq: ev.seq,
            step: step.cloned(),
            occurrence,
            safe: analyze_with(events, ev.seq, &safe),
        });
    }
    out
}

/// The cut (first event seq) of the `occurrence`-th (1-based) op of step `name`.
pub fn resolve_step(events: &[Event], name: &str, occurrence: u32) -> Option<u64> {
    step_cuts(events)
        .into_iter()
        .find(|c| c.step.as_deref() == Some(name) && c.occurrence == occurrence)
        .map(|c| c.seq)
}

/// The cut of a rerun run: the value of its single `__ostro/rerun_from=<n>` tag (canonical
/// decimal, the form apply accepts). `None` without a tag, with two, or when malformed.
pub fn rerun_from_tag(tags: &[String]) -> Option<u64> {
    let mut it = tags.iter().filter_map(|t| t.strip_prefix(RERUN_FROM_TAG));
    let v = it.next()?;
    if it.next().is_some() {
        return None;
    }
    let n: u64 = v.parse().ok()?;
    (n.to_string() == v).then_some(n)
}

/// Whether the rerun re-arm is due for `rec` at journal end `next_seq` (D-P11-6): the run
/// carries the rerun tag and has no live event yet.
pub(crate) fn prearm_due(rec: &RunRecord, next_seq: u64) -> bool {
    next_seq != 0 && rerun_from_tag(&rec.tags) == Some(next_seq)
}

/// Failpoint `rerun::before_rearm` (feature `failpoints`): between the grant and the rerun
/// re-arm. When it fires the job ends `Released` without the re-arm and without touching the
/// run, which stays `Running` exactly as after a crash at this point (the next start
/// recovers it and re-arms). `Ok` otherwise.
pub(crate) fn before_rearm() -> Result<(), ostro_partition::ExecOutcome> {
    #[cfg(feature = "failpoints")]
    if fail::eval("rerun::before_rearm", |_| ()).is_some() {
        tracing::warn!("rerun::before_rearm (injected): skipping the re-arm");
        return Err(ostro_partition::ExecOutcome::Released);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::journal::{
        AttemptBackoff, AwaitTokenCreated, ChildCompleted, ChildOptions, ChildStarted, CommandMeta,
        EffectCompleted, EffectRequest, EffectScheduled, ErrorInfo, Failure, ObjectRequest,
        OpCancelled, Payload, RecordedResult, RngSeed, RunFailed, RunStarted, SignalMatched,
        SignalPayload, SignalWaitStarted, StepAttemptFailed, StepCompleted, TimerFired,
        TimerStarted,
    };

    fn meta(h: u8) -> CommandMeta {
        CommandMeta {
            hash_ver: 1,
            hash: CommandHash([h; 16]),
        }
    }

    fn payload() -> Payload {
        Payload {
            content_type: "application/json".into(),
            data: b"1".to_vec(),
        }
    }

    fn rr() -> RecordedResult {
        RecordedResult {
            codec_ver: 1,
            bytes: vec![],
        }
    }

    fn started() -> EventBody {
        EventBody::RunStarted(RunStarted {
            workflow: "wf".into(),
            workflow_version: 1,
            input: payload(),
            rng_seed: RngSeed([0; 32]),
            accepted_at_nanos: 0,
            deadline_nanos: None,
            module_hash: "m".into(),
            tenant: None,
            parent_run_id: None,
            continued_from: None,
        })
    }

    fn step(h: u8, name: &str) -> EventBody {
        EventBody::StepCompleted(StepCompleted {
            meta: meta(h),
            name: name.into(),
            outcome: rr(),
            fuel_used: 0,
        })
    }

    fn step_failed(h: u8, name: &str, attempt: u32) -> EventBody {
        EventBody::StepAttemptFailed(StepAttemptFailed {
            meta: meta(h),
            name: name.into(),
            attempt,
            failure: Failure::Retryable(ErrorInfo {
                kind: "x".into(),
                message: "y".into(),
                details: None,
            }),
            retry_at_nanos: 0,
            backoff: AttemptBackoff::InActivation,
            fuel_used: 0,
        })
    }

    fn timer(h: u8) -> EventBody {
        EventBody::TimerStarted(TimerStarted {
            meta: meta(h),
            fire_at_nanos: 5_000_000_000,
        })
    }

    fn wait(h: u8) -> EventBody {
        EventBody::SignalWaitStarted(SignalWaitStarted {
            meta: meta(h),
            name: "go".into(),
            timeout_ms: None,
        })
    }

    fn child(h: u8) -> EventBody {
        EventBody::ChildStarted(ChildStarted {
            meta: meta(h),
            workflow: "leaf".into(),
            input: payload(),
            options: ChildOptions {
                queue: None,
                version: None,
                detached: false,
                run_timeout_ms: None,
            },
        })
    }

    fn effect(h: u8) -> EventBody {
        EventBody::EffectScheduled(EffectScheduled {
            meta: meta(h),
            request: EffectRequest::Object(ObjectRequest {
                connection: "c".into(),
                op: "get".into(),
                key: "k".into(),
                body: None,
            }),
        })
    }

    fn token(h: u8) -> EventBody {
        EventBody::AwaitTokenCreated(AwaitTokenCreated {
            meta: meta(h),
            token: "t".into(),
            timeout_ms: None,
        })
    }

    fn failed() -> EventBody {
        EventBody::RunFailed(RunFailed {
            meta: meta(99),
            failure: Failure::NonRetryable(ErrorInfo {
                kind: "x".into(),
                message: "y".into(),
                details: None,
            }),
        })
    }

    fn journal(bodies: Vec<EventBody>) -> Vec<Event> {
        bodies
            .into_iter()
            .enumerate()
            .map(|(i, body)| Event {
                seq: i as u64,
                ts_nanos: 1_000_000 * i as u64,
                body,
            })
            .collect()
    }

    fn err(seq: u64, reason: &'static str, nearest: Option<u64>) -> Result<CutPlan, CutUnsafe> {
        Err(CutUnsafe {
            seq,
            reason,
            nearest_safe_seq: nearest,
        })
    }

    #[test]
    fn rerun_open_child_is_unsafe_and_nearest_is_its_start() {
        // 0 started, 1-3 steps, 4 child (open), 5 step, 6 failed.
        let j = journal(vec![
            started(),
            step(1, "a"),
            step(2, "b"),
            step(3, "c"),
            child(4),
            step(5, "d"),
            failed(),
        ]);
        assert_eq!(analyze_cut(&j, 6), err(4, REASON_OPEN_CHILD, Some(4)));
        assert_eq!(
            analyze_cut(&j, 4),
            Ok(CutPlan {
                cut: 4,
                open_timers: 0,
                open_waits: 0
            })
        );
    }

    #[test]
    fn rerun_completed_child_is_safe() {
        let j = journal(vec![
            started(),
            child(1),
            EventBody::ChildCompleted(ChildCompleted {
                op: 1,
                outcome: rr(),
            }),
            step(3, "a"),
            failed(),
        ]);
        assert!(analyze_cut(&j, 4).is_ok());
        // The cut between start and completion is unsafe.
        assert_eq!(analyze_cut(&j, 2), err(1, REASON_OPEN_CHILD, Some(1)));
    }

    #[test]
    fn rerun_open_effect_token_and_unmatched_signal() {
        let j = journal(vec![started(), step(1, "a"), effect(2), failed()]);
        assert_eq!(analyze_cut(&j, 3), err(2, REASON_OPEN_EFFECT, Some(2)));
        let j = journal(vec![
            started(),
            effect(1),
            EventBody::EffectCompleted(EffectCompleted {
                op: 1,
                outcome: rr(),
            }),
            failed(),
        ]);
        assert!(analyze_cut(&j, 3).is_ok());

        let j = journal(vec![started(), step(1, "a"), token(2), failed()]);
        assert_eq!(analyze_cut(&j, 3), err(2, REASON_OPEN_AWAIT_TOKEN, Some(2)));

        let sig = EventBody::SignalPayload(SignalPayload {
            name: "go".into(),
            payload: payload(),
        });
        let j = journal(vec![
            started(),
            step(1, "a"),
            sig.clone(),
            step(3, "b"),
            failed(),
        ]);
        assert_eq!(analyze_cut(&j, 4), err(2, REASON_UNMATCHED_SIGNAL, Some(2)));
        // Matched later: unsafe only between payload and match.
        let j = journal(vec![
            started(),
            wait(1),
            sig,
            EventBody::SignalMatched(SignalMatched {
                op: 1,
                payload_seq: 2,
            }),
            failed(),
        ]);
        assert_eq!(analyze_cut(&j, 3), err(2, REASON_UNMATCHED_SIGNAL, Some(2)));
        assert_eq!(
            analyze_cut(&j, 4),
            Ok(CutPlan {
                cut: 4,
                open_timers: 0,
                open_waits: 0
            })
        );
        // Cut 2: the wait is open (allowed), the payload is not copied.
        assert_eq!(
            analyze_cut(&j, 2),
            Ok(CutPlan {
                cut: 2,
                open_timers: 0,
                open_waits: 1
            })
        );
    }

    #[test]
    fn rerun_open_timer_and_wait_are_safe_and_match_rearm() {
        let j = journal(vec![
            started(),
            step(1, "a"),
            timer(2),
            wait(3),
            timer(4),
            EventBody::TimerFired(TimerFired { op: 4 }),
            failed(),
        ]);
        let plan = analyze_cut(&j, 6).expect("safe");
        assert_eq!(
            plan,
            CutPlan {
                cut: 6,
                open_timers: 1,
                open_waits: 1
            }
        );
        // Cross-check against the executor's re-arm on the same prefix.
        let rearm = crate::derive::rearm_unresolved(&j[..6]);
        assert_eq!(rearm.len(), plan.open_timers + plan.open_waits);
        // A cancelled timer is closed for both.
        let j = journal(vec![
            started(),
            timer(1),
            EventBody::OpCancelled(OpCancelled {
                meta: meta(9),
                op: 1,
            }),
            failed(),
        ]);
        assert_eq!(analyze_cut(&j, 3).expect("safe").open_timers, 0);
        assert!(crate::derive::rearm_unresolved(&j[..3]).is_empty());
    }

    #[test]
    fn rerun_out_of_range_cuts() {
        let j = journal(vec![started(), step(1, "a"), step(2, "b"), failed()]);
        assert_eq!(analyze_cut(&j, 0), err(0, REASON_OUT_OF_RANGE, None));
        // Cut 3 = the terminal's index: copies 0..3 (valid). 4 would copy the terminal.
        assert!(analyze_cut(&j, 3).is_ok());
        assert_eq!(analyze_cut(&j, 4), err(4, REASON_OUT_OF_RANGE, Some(3)));
        assert_eq!(analyze_cut(&j, 9), err(9, REASON_OUT_OF_RANGE, Some(3)));
        // Without a terminal, the journal end bounds the cut.
        let j = journal(vec![started(), step(1, "a")]);
        assert!(analyze_cut(&j, 2).is_ok());
        assert_eq!(analyze_cut(&j, 3), err(3, REASON_OUT_OF_RANGE, Some(2)));
    }

    #[test]
    fn rerun_safe_cuts_agree_with_analyze_cut() {
        let sig = EventBody::SignalPayload(SignalPayload {
            name: "go".into(),
            payload: payload(),
        });
        let j = journal(vec![
            started(),
            child(1),
            step(2, "a"),
            EventBody::ChildCompleted(ChildCompleted {
                op: 1,
                outcome: rr(),
            }),
            effect(4),
            timer(5),
            sig,
            wait(7),
            EventBody::SignalMatched(SignalMatched {
                op: 7,
                payload_seq: 6,
            }),
            step(9, "b"),
            failed(),
        ]);
        let safe = safe_cuts(&j);
        for c in 0..=12u64 {
            let s = safe.get(c as usize).copied().unwrap_or(false);
            assert_eq!(analyze_cut(&j, c).is_ok(), s, "cut {c}");
        }
    }

    #[test]
    fn rerun_step_cuts_and_resolve_step() {
        let j = journal(vec![
            started(),
            step(1, "charge"),
            timer(2),
            step_failed(3, "charge", 1),
            step_failed(3, "charge", 2),
            step(3, "charge"),
            step(4, "ship"),
            failed(),
        ]);
        let cuts = step_cuts(&j);
        let view: Vec<(u64, Option<&str>, u32)> = cuts
            .iter()
            .map(|c| (c.seq, c.step.as_deref(), c.occurrence))
            .collect();
        assert_eq!(
            view,
            vec![
                (1, Some("charge"), 1),
                (2, None, 0),
                (3, Some("charge"), 2),
                (6, Some("ship"), 1),
            ]
        );
        assert_eq!(cuts[2].safe.expect("safe").open_timers, 1);
        assert_eq!(resolve_step(&j, "charge", 2), Some(3));
        assert_eq!(resolve_step(&j, "charge", 1), Some(1));
        assert_eq!(resolve_step(&j, "charge", 3), None);
        assert_eq!(resolve_step(&j, "nope", 1), None);
    }

    #[test]
    fn rerun_prearm_due_only_for_tagged_runs_without_live_events() {
        let mut rec = RunRecord {
            tags: vec!["user".into()],
            next_seq: 4,
            ..Default::default()
        };
        assert!(!prearm_due(&rec, 4), "a non-rerun never re-arms");
        rec.tags.push(format!("{RERUN_FROM_TAG}4"));
        assert!(prearm_due(&rec, 4));
        assert!(!prearm_due(&rec, 5), "a live event exists");
        assert!(!prearm_due(&rec, 0));
    }

    #[test]
    fn rerun_from_tag_parses_strictly() {
        let t = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(rerun_from_tag(&t(&["x", "__ostro/rerun_from=7"])), Some(7));
        assert_eq!(rerun_from_tag(&t(&["x"])), None);
        assert_eq!(rerun_from_tag(&t(&["__ostro/rerun_from="])), None);
        assert_eq!(rerun_from_tag(&t(&["__ostro/rerun_from=x"])), None);
        assert_eq!(rerun_from_tag(&t(&["__ostro/rerun_from=07"])), None);
        assert_eq!(rerun_from_tag(&t(&["__ostro/rerun_from=+7"])), None);
        assert_eq!(
            rerun_from_tag(&t(&["__ostro/rerun_from=7", "__ostro/rerun_from=7"])),
            None
        );
    }
}
