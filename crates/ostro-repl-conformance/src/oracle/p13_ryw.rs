//! Read-your-writes session checker (13-08; scenarios in 13-13, fixes in 13-18).
//!
//! A scenario opens one [`Session`] per client session ([`RywChecker::session`]), records
//! every acknowledged write with its observable effect ([`Session::ack`]) and every read
//! with what the client saw ([`Session::observe`], or [`Session::begin_read`] +
//! [`Session::observe_issued`] for a read issued before its answer is recorded). The
//! observations are client measurements (REST answers), never a node's claim about its own
//! consistency.
//!
//! [`RywChecker::check`] fails a read that was issued after an acknowledged write of the
//! same session and does not reflect it:
//!
//! | acked [`Effect`] | a later [`Observation`] of the same object must show |
//! |------------------|-------------------------------------------------------|
//! | `RunExists` (start) | the run is found |
//! | `SignalVisible` (signal with `request_id`) | the run is found and its history holds the request id |
//! | `RunTerminal` (cancel) | the run is found with a terminal status |
//! | `ActiveVersion` (activation) | the session's latest acked active version |
//! | `Policy` (policy write) | the session's latest acked policy value |
//!
//! Version and policy keys are compared with the session's *latest* acknowledged value, so a
//! scenario whose sessions write the same version / policy key concurrently must use one key
//! per session.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::Value;

use super::Report;
use crate::harness::inproc::TERMINAL;

/// The observable effect of an acknowledged write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// A started run exists (`POST /v1/workflows/{w}/runs` answered with this id).
    RunExists {
        /// Run id.
        run_id: String,
    },
    /// A signal is in the run's history (`request_id` of the signal call).
    SignalVisible {
        /// Run id.
        run_id: String,
        /// The signal's request id.
        request_id: String,
    },
    /// A cancelled run is terminal.
    RunTerminal {
        /// Run id.
        run_id: String,
    },
    /// `workflow`'s active version is `version`.
    ActiveVersion {
        /// Workflow name.
        workflow: String,
        /// Activated version.
        version: u32,
    },
    /// The policy `key` (e.g. `workflow/<name>`) has `value`.
    Policy {
        /// Policy key.
        key: String,
        /// Its written value.
        value: Value,
    },
}

/// What one read returned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    /// `GET /v1/runs/{id}` (+ history for signals).
    Run {
        /// Run id.
        run_id: String,
        /// The run was found (2xx).
        found: bool,
        /// Its status (`None` when not found).
        status: Option<String>,
        /// Request ids of the signals visible in its history.
        signals: Vec<String>,
    },
    /// The active version of `workflow` (`None` = no active version).
    ActiveVersion {
        /// Workflow name.
        workflow: String,
        /// Observed active version.
        version: Option<u32>,
    },
    /// The policy `key` (`None` = absent).
    Policy {
        /// Policy key.
        key: String,
        /// Observed value.
        value: Option<Value>,
    },
}

impl Observation {
    /// A run read from a `GET /v1/runs/{id}` answer: `found` = 2xx, `status` = `status`.
    pub fn run_from_json(run_id: &str, found: bool, body: &Value, signals: Vec<String>) -> Self {
        Observation::Run {
            run_id: run_id.to_owned(),
            found,
            status: found
                .then(|| body["status"].as_str().map(str::to_owned))
                .flatten(),
            signals,
        }
    }
}

/// A read issued at a point of the global order (see [`Session::begin_read`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadTicket(u64);

#[derive(Clone, Debug)]
struct Acked {
    seq: u64,
    effect: Effect,
}

#[derive(Clone, Debug)]
struct Read {
    seq: u64,
    obs: Observation,
}

#[derive(Debug, Default)]
struct Inner {
    next: u64,
    acks: BTreeMap<String, Vec<Acked>>,
    reads: BTreeMap<String, Vec<Read>>,
}

impl Inner {
    fn tick(&mut self) -> u64 {
        self.next += 1;
        self.next
    }
}

/// The checker shared by every session of one scenario (cheap to clone).
#[derive(Clone, Debug, Default)]
pub struct RywChecker {
    inner: Arc<Mutex<Inner>>,
}

/// One client session's recorder.
#[derive(Clone, Debug)]
pub struct Session {
    id: String,
    inner: Arc<Mutex<Inner>>,
}

impl RywChecker {
    /// An empty checker.
    pub fn new() -> Self {
        Self::default()
    }

    /// The recorder of session `id` (the same id always names the same session).
    pub fn session(&self, id: impl Into<String>) -> Session {
        Session {
            id: id.into(),
            inner: Arc::clone(&self.inner),
        }
    }

    /// Every stale read: a read issued after an acknowledged write of its session that
    /// does not reflect it.
    pub fn check(&self) -> Report {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let mut report = Report::new("p13_ryw");
        for (session, reads) in &inner.reads {
            let acks = inner.acks.get(session).map(Vec::as_slice).unwrap_or(&[]);
            for read in reads {
                let before: Vec<&Acked> = acks.iter().filter(|a| a.seq < read.seq).collect();
                for v in stale(&before, &read.obs) {
                    report.violation(format!("session {session} read #{}: {v}", read.seq));
                }
            }
        }
        report
    }
}

impl Session {
    /// The session id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Record an acknowledged write (call when the write's success answer arrived).
    pub fn ack(&self, effect: Effect) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let seq = inner.tick();
        inner
            .acks
            .entry(self.id.clone())
            .or_default()
            .push(Acked { seq, effect });
    }

    /// Mark the moment a read is issued (its answer is recorded later with
    /// [`Session::observe_issued`]).
    pub fn begin_read(&self) -> ReadTicket {
        ReadTicket(
            self.inner
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .tick(),
        )
    }

    /// Record the answer of a read issued at `ticket`.
    pub fn observe_issued(&self, ticket: ReadTicket, obs: Observation) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .reads
            .entry(self.id.clone())
            .or_default()
            .push(Read { seq: ticket.0, obs });
    }

    /// Record a read issued now (after every write acknowledged so far).
    pub fn observe(&self, obs: Observation) {
        let ticket = self.begin_read();
        self.observe_issued(ticket, obs);
    }
}

/// Why `obs` misses one of the `before` writes (one line each).
fn stale(before: &[&Acked], obs: &Observation) -> Vec<String> {
    let mut out = Vec::new();
    match obs {
        Observation::Run {
            run_id,
            found,
            status,
            signals,
        } => {
            for a in before {
                match &a.effect {
                    Effect::RunExists { run_id: r } if r == run_id && !found => {
                        out.push(format!(
                            "run {run_id} not found after its start was acked (#{})",
                            a.seq
                        ));
                    }
                    Effect::SignalVisible {
                        run_id: r,
                        request_id,
                    } if r == run_id && (!found || !signals.contains(request_id)) => {
                        out.push(format!(
                            "run {run_id}: signal {request_id} acked (#{}) but not in history",
                            a.seq
                        ));
                    }
                    Effect::RunTerminal { run_id: r }
                        if r == run_id
                            && !(*found
                                && status.as_deref().is_some_and(|s| TERMINAL.contains(&s))) =>
                    {
                        out.push(format!(
                            "run {run_id}: cancel acked (#{}) but status {status:?}",
                            a.seq
                        ));
                    }
                    _ => {}
                }
            }
        }
        Observation::ActiveVersion { workflow, version } => {
            let latest = before.iter().rev().find_map(|a| match &a.effect {
                Effect::ActiveVersion {
                    workflow: w,
                    version: v,
                } if w == workflow => Some((a.seq, *v)),
                _ => None,
            });
            if let Some((seq, want)) = latest
                && *version != Some(want)
            {
                out.push(format!(
                    "workflow {workflow}: version {want} activated (#{seq}) but read {version:?}"
                ));
            }
        }
        Observation::Policy { key, value } => {
            let latest = before.iter().rev().find_map(|a| match &a.effect {
                Effect::Policy { key: k, value: v } if k == key => Some((a.seq, v)),
                _ => None,
            });
            if let Some((seq, want)) = latest
                && value.as_ref() != Some(want)
            {
                out.push(format!(
                    "policy {key}: {want} written (#{seq}) but read {value:?}"
                ));
            }
        }
    }
    out
}
