//! The run timeline with replay markers (plan 11-15, D-P11-18).
//!
//! **Exact** mode (any event carries `HistoryEvent.attempt`): events are grouped by the
//! activation attempt (`epoch << 32 | n`) that wrote them. Apply-originated inputs
//! (attempt `None`) stay in the current group, so a hot wait (the same attempt on both
//! sides of an input) is one group with no marker. The first event of every later group
//! shows "attempt e.n replayed seq 0..k", k being the last seq before the group.
//!
//! **Approximate** mode (no attempts, pre-Phase 11 journals): a marker before each input
//! event that follows a command event ("likely re-activation"), before the event after
//! a requeued step retry ("likely resume"), on StepAttemptFailed (retry) and on
//! RunTerminated. Each re-activation starts a new group.
//!
//! A rerun's copied prefix (seq < the `__ostro/rerun_from=<cut>` tag) is one leading
//! group whose rows read "copied from <source>". Step durations come from consecutive
//! `ts`; the bars are SVG `x`/`width` geometry (CSP: no `style=`).

use std::collections::BTreeMap;

use ostro_api::domain::HistoryEvent;
use ostro_api::phase11::{RerunCut, RerunCutsView};

use crate::pages::{fmt_duration, fmt_ts, href_encode};

/// Width of a timeline bar's SVG, in user units.
pub(crate) const BAR_WIDTH: u64 = 200;

/// What the timeline knows besides the events.
#[derive(Debug, Default, Clone)]
pub(crate) struct TimelineCtx<'a> {
    /// The run (for the rerun links).
    pub run_id: &'a str,
    /// The rerun cut (`__ostro/rerun_from=<cut>`), when the run is a rerun.
    pub rerun_cut: Option<u64>,
    /// The rerun's source run, when known.
    pub source: Option<&'a str>,
    /// The cut analyzer's answer (terminal runs with the ops slot present).
    pub cuts: Option<&'a RerunCutsView>,
}

/// A replay marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Marker {
    /// Exact (from `attempt`) or approximate (heuristic).
    pub exact: bool,
    /// Rendered text.
    pub text: String,
}

impl Marker {
    fn exact(text: String) -> Self {
        Marker { exact: true, text }
    }
    fn approx(text: &str) -> Self {
        Marker {
            exact: false,
            text: text.to_owned(),
        }
    }
    /// CSS class.
    pub fn class(&self) -> &'static str {
        if self.exact { "exact" } else { "approximate" }
    }
}

/// The "Rerun from here" control of one step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RerunControl {
    /// The analyzer says the cut is safe.
    pub enabled: bool,
    /// Target of the (11-16) rerun form.
    pub href: String,
    /// Why not, when disabled.
    pub reason: String,
}

/// One timeline row (a journal event).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Row {
    pub seq: u64,
    pub kind: String,
    pub summary: String,
    pub ts: String,
    /// Time since the previous event.
    pub duration: Option<String>,
    /// `start` | `command` | `input` | `retry` | `terminal` | `terminal-bad`.
    pub class: &'static str,
    pub marker: Option<Marker>,
    /// `copied from <source>` for a rerun's copied prefix.
    pub copied_from: Option<String>,
    /// The attempt as `e.n`, when present.
    pub attempt: Option<String>,
    pub bar_x: u64,
    pub bar_w: u64,
    pub rerun: Option<RerunControl>,
}

/// A run of events written by one activation (or the copied prefix).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Group {
    pub label: String,
    pub copied: bool,
    pub rows: Vec<Row>,
}

/// The whole timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Timeline {
    /// Markers come from `attempt` (true) or the heuristic (false).
    pub exact: bool,
    pub groups: Vec<Group>,
    /// Total span, formatted.
    pub span: String,
}

/// Event classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Start,
    Command,
    Input,
    Retry,
    Terminal,
    TerminalBad,
}

impl Class {
    fn css(self) -> &'static str {
        match self {
            Class::Start => "start",
            Class::Command => "command",
            Class::Input => "input",
            Class::Retry => "retry",
            Class::Terminal => "terminal",
            Class::TerminalBad => "terminal-bad",
        }
    }
}

/// Normalised kind: lowercase, no `_` (the server says `step_completed`, fakes may say
/// `StepCompleted`).
fn norm(kind: &str) -> String {
    kind.chars()
        .filter(|c| *c != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

fn classify(kind: &str) -> Class {
    match norm(kind).as_str() {
        "runstarted" => Class::Start,
        "timerfired" | "effectcompleted" | "signalreceived" | "childcompleted"
        | "tokencompleted" | "cancelrequested" | "waittimedout" | "signalpayload"
        | "signalmatched" => Class::Input,
        "stepattemptfailed" => Class::Retry,
        "runcompleted" | "runcontinuedasnew" => Class::Terminal,
        "runfailed" | "runterminated" => Class::TerminalBad,
        _ => Class::Command,
    }
}

/// `e.n` of a stored attempt.
pub(crate) fn fmt_attempt(a: u64) -> String {
    format!("{}.{}", a >> 32, a & 0xffff_ffff)
}

/// Whether a StepAttemptFailed event's backoff requeued the run (next event = resume).
fn requeued(ev: &HistoryEvent) -> bool {
    serde_json::from_slice::<serde_json::Value>(&ev.details_json)
        .ok()
        .and_then(|v| {
            v.get("backoff")
                .and_then(|b| b.as_str())
                .map(|b| b == "requeue")
        })
        .unwrap_or(false)
}

fn rerun_control(run_id: &str, cut: &RerunCut) -> RerunControl {
    RerunControl {
        enabled: cut.safe,
        href: format!(
            "/ui/runs/{}/rerun?from_seq={}",
            href_encode(run_id),
            cut.seq
        ),
        reason: if cut.safe {
            String::new()
        } else {
            let mut r = cut
                .reason
                .clone()
                .unwrap_or_else(|| "unsafe cut".to_owned());
            if let Some(n) = cut.nearest_safe_seq {
                r.push_str(&format!(" (nearest safe cut: seq {n})"));
            }
            r
        },
    }
}

/// Builds the timeline of `events` (ascending seq).
pub(crate) fn build_timeline(events: &[HistoryEvent], ctx: &TimelineCtx<'_>) -> Timeline {
    let exact = events.iter().any(|e| e.attempt.is_some_and(|a| a != 0));
    let t0 = events.first().map_or(0, |e| e.ts_ms);
    let t_end = events.last().map_or(0, |e| e.ts_ms);
    let span = t_end.saturating_sub(t0).max(1);
    let cuts: BTreeMap<u64, &RerunCut> = ctx
        .cuts
        .map(|v| v.cuts.iter().map(|c| (c.seq, c)).collect())
        .unwrap_or_default();
    let copied_label = match ctx.source {
        Some(s) => format!("copied from {s}"),
        None => "copied from the source run".to_owned(),
    };

    let mut groups: Vec<Group> = Vec::new();
    // Exact: the attempt of the open group. Approximate: unused.
    let mut open_attempt: Option<u64> = None;
    // A previous activation (or a copied prefix) exists, so a new group replays.
    let mut prior = false;
    let mut activations = 0u32;
    let mut prev: Option<&HistoryEvent> = None;

    for ev in events {
        let class = classify(&ev.kind);
        let copied = ctx.rerun_cut.is_some_and(|cut| ev.seq < cut);
        let prev_ts = prev.map_or(ev.ts_ms, |p| p.ts_ms);
        let dur = prev.map(|p| ev.ts_ms.saturating_sub(p.ts_ms));
        let bar_x = prev_ts.saturating_sub(t0).saturating_mul(BAR_WIDTH) / span;
        let bar_w = (dur.unwrap_or(0).saturating_mul(BAR_WIDTH) / span)
            .max(2)
            .min(BAR_WIDTH.saturating_sub(bar_x).max(2));
        let mut row = Row {
            seq: ev.seq,
            kind: ev.kind.clone(),
            summary: ev.summary.clone(),
            ts: fmt_ts(ev.ts_ms),
            duration: dur.map(fmt_duration),
            class: class.css(),
            marker: None,
            copied_from: copied.then(|| copied_label.clone()),
            attempt: ev.attempt.filter(|a| *a != 0).map(fmt_attempt),
            bar_x: bar_x.min(BAR_WIDTH.saturating_sub(2)),
            bar_w,
            rerun: cuts.get(&ev.seq).map(|c| rerun_control(ctx.run_id, c)),
        };

        let mut new_group: Option<(String, bool)> = None;
        if copied {
            if groups.last().is_none_or(|g| !g.copied) {
                let cut = ctx.rerun_cut.unwrap_or(0);
                new_group = Some((
                    format!("{copied_label} (seq 0..{})", cut.saturating_sub(1)),
                    true,
                ));
            }
        } else {
            let after_copy = groups.last().is_some_and(|g| g.copied);
            if after_copy {
                prior = true;
            }
            if exact {
                match ev.attempt.filter(|a| *a != 0) {
                    Some(a) if open_attempt != Some(a) || after_copy => {
                        if prior {
                            let k = ev.seq.saturating_sub(1);
                            row.marker = Some(Marker::exact(format!(
                                "attempt {} replayed seq 0..{k}",
                                fmt_attempt(a)
                            )));
                        }
                        open_attempt = Some(a);
                        prior = true;
                        new_group = Some((format!("attempt {}", fmt_attempt(a)), false));
                    }
                    Some(_) => {}
                    None if groups.is_empty() || after_copy => {
                        new_group = Some(("start".to_owned(), false));
                    }
                    None => {}
                }
            } else {
                let p_class = prev.map(|p| classify(&p.kind));
                let reactivated = (class == Class::Input && p_class == Some(Class::Command))
                    || prev.is_some_and(|p| classify(&p.kind) == Class::Retry && requeued(p));
                if reactivated {
                    row.marker = Some(Marker::approx(if class == Class::Input {
                        "likely re-activation"
                    } else {
                        "likely resume (requeued retry)"
                    }));
                }
                if reactivated || groups.is_empty() || after_copy {
                    activations += 1;
                    new_group = Some((format!("activation {activations} (approximate)"), false));
                }
                if class == Class::Retry {
                    row.marker = Some(Marker::approx("retry: step attempt failed"));
                } else if norm(&ev.kind) == "runterminated" {
                    row.marker = Some(Marker::approx("terminated by an operator"));
                }
            }
        }
        if exact && class == Class::Retry && row.marker.is_none() {
            row.marker = Some(Marker::exact("retry: step attempt failed".to_owned()));
        }
        if let Some((label, copied)) = new_group {
            groups.push(Group {
                label,
                copied,
                rows: Vec::new(),
            });
        }
        if let Some(g) = groups.last_mut() {
            g.rows.push(row);
        }
        prev = Some(ev);
    }

    Timeline {
        exact,
        groups,
        span: fmt_duration(t_end.saturating_sub(t0)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(seq: u64, kind: &str, attempt: Option<(u64, u64)>) -> HistoryEvent {
        HistoryEvent {
            seq,
            kind: kind.to_owned(),
            ts_ms: 1_700_000_000_000 + seq * 250,
            summary: format!("{kind} #{seq}"),
            details_json: b"{}".to_vec(),
            attempt: attempt.map(|(e, n)| (e << 32) | n),
        }
    }

    fn markers(t: &Timeline) -> Vec<(u64, String, bool)> {
        t.groups
            .iter()
            .flat_map(|g| g.rows.iter())
            .filter_map(|r| r.marker.as_ref().map(|m| (r.seq, m.text.clone(), m.exact)))
            .collect()
    }

    fn ctx() -> TimelineCtx<'static> {
        TimelineCtx {
            run_id: "run-1",
            ..TimelineCtx::default()
        }
    }

    #[test]
    fn exact_groups_by_attempt_with_replay_marker() {
        let events = vec![
            ev(0, "run_started", Some((1, 1))),
            ev(1, "step_completed", Some((1, 1))),
            ev(2, "timer_started", Some((1, 1))),
            ev(3, "step_completed", Some((1, 1))),
            ev(4, "step_completed", Some((1, 2))),
            ev(5, "timer_fired", None), // apply-originated input
            ev(6, "run_completed", Some((1, 2))),
        ];
        let t = build_timeline(&events, &ctx());
        assert!(t.exact);
        assert_eq!(t.groups.len(), 2, "{t:#?}");
        assert_eq!(t.groups[0].label, "attempt 1.1");
        assert_eq!(t.groups[1].label, "attempt 1.2");
        assert_eq!(t.groups[1].rows.len(), 3);
        assert_eq!(
            markers(&t),
            vec![(4, "attempt 1.2 replayed seq 0..3".to_owned(), true)]
        );
    }

    #[test]
    fn hot_wait_is_one_group_without_marker() {
        let events = vec![
            ev(0, "run_started", None),
            ev(1, "signal_wait_started", Some((1, 1))),
            ev(2, "signal_received", None),
            ev(3, "step_completed", Some((1, 1))),
            ev(4, "run_completed", Some((1, 1))),
        ];
        let t = build_timeline(&events, &ctx());
        assert!(t.exact);
        assert!(markers(&t).is_empty(), "{t:#?}");
        let labels: Vec<_> = t.groups.iter().map(|g| g.label.as_str()).collect();
        assert_eq!(labels, ["start", "attempt 1.1"]);
    }

    #[test]
    fn approximate_markers_for_pre_phase11_journals() {
        let mut retry = ev(4, "step_attempt_failed", None);
        retry.details_json = br#"{"backoff":"requeue"}"#.to_vec();
        let events = vec![
            ev(0, "run_started", None),
            ev(1, "timer_started", None),
            ev(2, "timer_fired", None),
            ev(3, "step_completed", None),
            retry,
            ev(5, "step_completed", None),
            ev(6, "run_terminated", None),
        ];
        let t = build_timeline(&events, &ctx());
        assert!(!t.exact);
        assert_eq!(
            markers(&t),
            vec![
                (2, "likely re-activation".to_owned(), false),
                (4, "retry: step attempt failed".to_owned(), false),
                (5, "likely resume (requeued retry)".to_owned(), false),
                (6, "terminated by an operator".to_owned(), false),
            ]
        );
        let labels: Vec<_> = t.groups.iter().map(|g| g.label.as_str()).collect();
        assert_eq!(
            labels,
            [
                "activation 1 (approximate)",
                "activation 2 (approximate)",
                "activation 3 (approximate)"
            ]
        );
        let retry_row = &t.groups[1].rows[2];
        assert_eq!(retry_row.class, "retry");
    }

    #[test]
    fn rerun_prefix_is_copied_and_live_group_replays() {
        let events = vec![
            ev(0, "run_started", Some((1, 1))),
            ev(1, "step_completed", Some((1, 1))),
            ev(2, "step_completed", Some((2, 1))),
            ev(3, "step_completed", Some((7, 1))),
            ev(4, "run_completed", Some((7, 1))),
        ];
        let c = TimelineCtx {
            run_id: "run-2",
            rerun_cut: Some(3),
            source: Some("run-1"),
            cuts: None,
        };
        let t = build_timeline(&events, &c);
        assert_eq!(t.groups.len(), 2);
        assert!(t.groups[0].copied);
        assert_eq!(t.groups[0].label, "copied from run-1 (seq 0..2)");
        assert!(
            t.groups[0]
                .rows
                .iter()
                .all(|r| r.copied_from.as_deref() == Some("copied from run-1"))
        );
        assert!(t.groups[1].rows.iter().all(|r| r.copied_from.is_none()));
        assert_eq!(
            markers(&t),
            vec![(3, "attempt 7.1 replayed seq 0..2".to_owned(), true)]
        );
    }

    #[test]
    fn durations_and_svg_geometry() {
        let mut events = vec![
            ev(0, "run_started", None),
            ev(1, "step_completed", None),
            ev(2, "run_completed", None),
        ];
        events[1].ts_ms = events[0].ts_ms + 1_500;
        events[2].ts_ms = events[0].ts_ms + 2_000;
        let t = build_timeline(&events, &ctx());
        let rows: Vec<&Row> = t.groups.iter().flat_map(|g| g.rows.iter()).collect();
        assert_eq!(rows[0].duration, None);
        assert_eq!(rows[1].duration.as_deref(), Some("1.5 s"));
        assert_eq!(rows[2].duration.as_deref(), Some("500 ms"));
        assert_eq!((rows[1].bar_x, rows[1].bar_w), (0, 150));
        assert_eq!((rows[2].bar_x, rows[2].bar_w), (150, 50));
        assert_eq!(t.span, "2.0 s");
    }

    #[test]
    fn rerun_controls_follow_the_analyzer() {
        let events = vec![
            ev(0, "run_started", None),
            ev(1, "step_completed", None),
            ev(2, "step_completed", None),
        ];
        let view = RerunCutsView {
            run_id: "run-1".to_owned(),
            cuts: vec![
                RerunCut {
                    seq: 1,
                    step: Some("charge".to_owned()),
                    occurrence: 1,
                    safe: true,
                    reason: None,
                    nearest_safe_seq: None,
                },
                RerunCut {
                    seq: 2,
                    step: Some("ship".to_owned()),
                    occurrence: 1,
                    safe: false,
                    reason: Some("open effect".to_owned()),
                    nearest_safe_seq: Some(1),
                },
            ],
        };
        let c = TimelineCtx {
            run_id: "run-1",
            cuts: Some(&view),
            ..TimelineCtx::default()
        };
        let t = build_timeline(&events, &c);
        let rows: Vec<&Row> = t.groups.iter().flat_map(|g| g.rows.iter()).collect();
        assert!(rows[0].rerun.is_none());
        let ok = rows[1].rerun.as_ref().expect("control");
        assert!(ok.enabled);
        assert_eq!(ok.href, "/ui/runs/run-1/rerun?from_seq=1");
        let no = rows[2].rerun.as_ref().expect("control");
        assert!(!no.enabled);
        assert_eq!(no.reason, "open effect (nearest safe cut: seq 1)");
    }
}
