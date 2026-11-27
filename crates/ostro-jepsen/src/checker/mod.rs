//! History checkers (14-09): lost acknowledged runs, duplicate journal entries, fencing.
//!
//! The checkers are pure functions over two inputs, so the self-tests
//! (`tests/checker_selftest.rs`) feed synthetic data without a cluster:
//!
//! - the client [`History`](crate::history::History) (ops with their outcome and the
//!   `ostro-consistency` token of every acknowledged write), plus per-run observations made
//!   after quiescence ([`lost::RunObservation`]);
//! - the [`LogExport`]: every replica's partition log, read offline after the cluster stopped
//!   (`ostro_repl_conformance::oracle::disk::scan_partition`, the verified Phase 12 scan),
//!   decoded into [`Entry`]s, plus the outcome of an offline re-apply of each log on a fresh
//!   store ([`fencing::reapply_scan`]).
//!
//! [`from_scan`] turns a Phase 12 [`LogScan`] into a [`ReplicaLog`]. Each checker returns a
//! [`Verdict`]: named counts (proof the check was not vacuous) and one line per violation
//! naming the culprit (partition, offset, node, run id).

pub mod duplicate;
pub mod fencing;
pub mod lost;

use std::collections::BTreeMap;

use ostro_repl_conformance::oracle::disk::LogScan;
use ostro_types::RunId;
use ostro_types::command::{Cmd, decode_command};
use serde::{Deserialize, Serialize};

/// What one checker found.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    /// Checker name (`lost`, `duplicate`, `fencing`).
    pub checker: String,
    /// Named counts (what was checked).
    pub counts: BTreeMap<String, u64>,
    /// One line per violation.
    pub violations: Vec<String>,
    /// Remarks that are not violations (skipped inputs, coverage notes).
    pub notes: Vec<String>,
}

impl Verdict {
    /// An empty (passing) verdict.
    pub fn new(checker: &str) -> Self {
        Self {
            checker: checker.to_owned(),
            ..Self::default()
        }
    }

    /// Record a violation.
    pub fn violation(&mut self, v: impl Into<String>) {
        self.violations.push(v.into());
    }

    /// Record a note.
    pub fn note(&mut self, n: impl Into<String>) {
        self.notes.push(n.into());
    }

    /// Add `n` to count `key`.
    pub fn add(&mut self, key: &str, n: u64) {
        *self.counts.entry(key.to_owned()).or_default() += n;
    }

    /// Count `key` (0 when absent).
    pub fn count(&self, key: &str) -> u64 {
        self.counts.get(key).copied().unwrap_or(0)
    }

    /// No violation.
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }

    /// Whether any violation contains `needle` (self-tests).
    pub fn mentions(&self, needle: &str) -> bool {
        self.violations.iter().any(|v| v.contains(needle))
    }

    /// Fold `other`'s counts, violations and notes into this verdict.
    pub fn absorb(&mut self, other: Verdict) {
        for (k, v) in other.counts {
            self.add(&k, v);
        }
        self.violations.extend(other.violations);
        self.notes.extend(other.notes);
    }

    /// One line: `name: PASS (counts)` or `name: FAIL n violation(s) (counts)`.
    pub fn line(&self) -> String {
        let counts: Vec<String> = self
            .counts
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        if self.ok() {
            format!("{}: PASS ({})", self.checker, counts.join(", "))
        } else {
            format!(
                "{}: FAIL {} violation(s) ({})",
                self.checker,
                self.violations.len(),
                counts.join(", ")
            )
        }
    }
}

/// A fencing token `(epoch, n)` (`AttemptTokenMsg`).
pub type AttemptTok = (u32, u32);

/// The decoded meaning of one partition command, as far as the checkers need it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// `LeaderAnnounced { epoch, node_id }`.
    LeaderAnnounced {
        /// The announced epoch.
        epoch: u32,
        /// The announcing node.
        node: u64,
    },
    /// `StartRun`.
    StartRun {
        /// Run id (display form).
        run_id: String,
        /// Workflow name.
        workflow: String,
        /// Idempotency key (lossy UTF-8; empty = none).
        idem_key: String,
    },
    /// `AttemptGranted`.
    Grant {
        /// Run id.
        run_id: String,
        /// The granted token (`None` = missing, apply rejects it).
        token: Option<AttemptTok>,
    },
    /// `EventsAppended`.
    Events {
        /// Run id.
        run_id: String,
        /// The executor's token.
        token: Option<AttemptTok>,
        /// Journal seqs of the events, in order.
        seqs: Vec<u64>,
    },
    /// `PauseRun` (fenced when it carries a token).
    Pause {
        /// Run id.
        run_id: String,
        /// The auto-pausing attempt's token (`None` = operator pause, not fenced).
        token: Option<AttemptTok>,
    },
    /// `SignalRun`.
    Signal {
        /// Run id.
        run_id: String,
        /// Signal name.
        name: String,
        /// Dedupe request id (lossy UTF-8).
        request_id: String,
    },
    /// `RequestCancel`.
    Cancel {
        /// Run id.
        run_id: String,
    },
    /// Anything else (variant name; `undecodable` for bytes that do not decode).
    Other {
        /// Variant name.
        kind: String,
    },
}

impl Op {
    /// The run a command names (`None` for leader announcements and unrecognised variants).
    pub fn run_id(&self) -> Option<&str> {
        match self {
            Op::StartRun { run_id, .. }
            | Op::Grant { run_id, .. }
            | Op::Events { run_id, .. }
            | Op::Pause { run_id, .. }
            | Op::Signal { run_id, .. }
            | Op::Cancel { run_id } => Some(run_id),
            Op::LeaderAnnounced { .. } | Op::Other { .. } => None,
        }
    }
}

/// One command of a replica's log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Its offset.
    pub offset: u64,
    /// The envelope (leader) epoch that wrote it.
    pub epoch: u32,
    /// FNV-1a 64 of the command bytes.
    pub digest: u64,
    /// The decoded command.
    pub op: Op,
}

/// One stored record (envelope) of a replica's log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// First offset.
    pub base: u64,
    /// Offset just past its last command.
    pub end: u64,
    /// Envelope epoch.
    pub epoch: u32,
    /// Envelope length in bytes.
    pub len: u64,
    /// FNV-1a 64 of the stored envelope bytes.
    pub digest: u64,
}

/// Outcome of one command in the offline re-apply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Applied {
    /// The command's offset.
    pub offset: u64,
    /// `Accepted` (true) or `Duplicate` / `Rejected(_)` (false).
    pub accepted: bool,
    /// The outcome (`Accepted`, `Duplicate`, `Rejected(StaleEpoch)`, ...).
    pub outcome: String,
}

/// One replica's verified log of one partition.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaLog {
    /// Node id.
    pub node: u64,
    /// Whether the node was a live ISR member when the partition settled (it must then hold
    /// everything below HW).
    pub in_isr: bool,
    /// Stored records, in offset order.
    pub records: Vec<Record>,
    /// Commands, in offset order.
    pub entries: Vec<Entry>,
    /// Why the verified scan stopped early (`None` = every byte verified).
    pub stopped: Option<String>,
    /// The offline re-apply of this log on a fresh store (`None` = not run: trimmed log or
    /// a re-apply error, see the notes of the fencing verdict).
    pub reapply: Option<Vec<Applied>>,
}

impl ReplicaLog {
    /// First offset held (`None` when empty).
    pub fn start(&self) -> Option<u64> {
        self.records.first().map(|r| r.base)
    }

    /// Offset just past the last record (0 when empty).
    pub fn end(&self) -> u64 {
        self.records.last().map_or(0, |r| r.end)
    }

    /// The outcome of the command at `offset` in the re-apply.
    pub fn applied_at(&self, offset: u64) -> Option<&Applied> {
        let a = self.reapply.as_ref()?;
        let first = a.first()?.offset;
        let i = usize::try_from(offset.checked_sub(first)?).ok()?;
        a.get(i).filter(|x| x.offset == offset)
    }
}

/// Every replica of one partition.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionLogs {
    /// The partition.
    pub partition: u16,
    /// The settled high watermark (everything below is committed).
    pub hw: u64,
    /// Its replicas.
    pub replicas: Vec<ReplicaLog>,
}

/// Every partition's logs after the run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogExport {
    /// One element per partition.
    pub partitions: Vec<PartitionLogs>,
}

impl LogExport {
    /// The logs of partition `p`.
    pub fn partition(&self, p: u16) -> Option<&PartitionLogs> {
        self.partitions.iter().find(|x| x.partition == p)
    }
}

/// FNV-1a, 64 bit (stable digests for the export; not a security hash).
pub fn fnv64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

fn run_str(b: &[u8]) -> String {
    match RunId::from_bytes(b) {
        Ok(r) => r.to_string(),
        Err(_) => format!(
            "raw:{}",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        ),
    }
}

/// Decode one command into an [`Op`].
pub fn decode_op(bytes: &[u8]) -> Op {
    let Ok(cmd) = decode_command(bytes) else {
        return Op::Other {
            kind: "undecodable".into(),
        };
    };
    let tok = |t: Option<ostro_types::command::AttemptTokenMsg>| t.map(|t| (t.epoch, t.n));
    let kind = cmd.kind_name();
    match cmd.cmd {
        Some(Cmd::LeaderAnnounced(la)) => Op::LeaderAnnounced {
            epoch: la.epoch,
            node: la.node_id,
        },
        Some(Cmd::StartRun(s)) => Op::StartRun {
            run_id: run_str(&s.run_id),
            workflow: s.workflow,
            idem_key: String::from_utf8_lossy(&s.idempotency_key).into_owned(),
        },
        Some(Cmd::AttemptGranted(g)) => Op::Grant {
            run_id: run_str(&g.run_id),
            token: tok(g.token),
        },
        Some(Cmd::EventsAppended(e)) => Op::Events {
            run_id: run_str(&e.run_id),
            token: tok(e.token),
            seqs: e.events.iter().map(|ev| ev.seq).collect(),
        },
        Some(Cmd::PauseRun(p)) => Op::Pause {
            run_id: run_str(&p.run_id),
            token: tok(p.token),
        },
        Some(Cmd::SignalRun(s)) => Op::Signal {
            run_id: run_str(&s.run_id),
            name: s.name,
            request_id: String::from_utf8_lossy(&s.request_id).into_owned(),
        },
        Some(Cmd::RequestCancel(r)) => Op::Cancel {
            run_id: run_str(&r.run_id),
        },
        _ => Op::Other { kind: kind.into() },
    }
}

/// A Phase 12 verified scan as a [`ReplicaLog`] (re-apply not run; see
/// [`fencing::reapply_scan`]).
pub fn from_scan(scan: &LogScan, in_isr: bool) -> ReplicaLog {
    let mut out = ReplicaLog {
        node: scan.node,
        in_isr,
        stopped: scan.stopped.clone(),
        ..ReplicaLog::default()
    };
    for r in &scan.records {
        out.records.push(Record {
            base: r.base.0,
            end: r.end.0,
            epoch: r.epoch.0,
            len: r.envelope.len() as u64,
            digest: fnv64(&r.envelope),
        });
        for (i, c) in r.commands.iter().enumerate() {
            out.entries.push(Entry {
                offset: r.base.0 + i as u64,
                epoch: r.epoch.0,
                digest: fnv64(c),
                op: decode_op(c),
            });
        }
    }
    out
}
