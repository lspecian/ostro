//! Shared fixture of the 09-08 children tests (`children_detached.rs`, `children_resume.rs`,
//! `children_failpoints.rs`): a real `Node` (1 or 2 partitions, shuffler running), a fake
//! [`ModuleResolver`], raw appends and journal helpers (same shapes as `children_derive.rs`).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ostro_engine::sink::RunRef;
use ostro_log::Rf1Log;
use ostro_partition::store::keys::children_key;
use ostro_partition::store::values::RunRecord;
use ostro_partition::{ApplyOutcome, Node, NodeConfig, PartitionHandle, Table, query};
use ostro_server::derive::{InheritPolicy, ModuleResolver, ResolvedWorkflow};
use ostro_server::journal_io::read_journal;
use ostro_server::sink::PartitionSink;
use ostro_types::command::{
    AttemptTokenMsg, ConflictPolicy, EventsAppended, JournalEvent, ParentRef, StartRun,
};
use ostro_types::journal::{
    ChildOptions, ChildStarted, CommandHash, CommandMeta, Event, EventBody, Failure, OpOutput,
    Payload, decode_result, encode_event,
};
use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::timeout;

pub const T: Duration = Duration::from_secs(20);
pub const OPEN_T: Duration = Duration::from_secs(120);
pub const PARENT_SHA: [u8; 32] = [0xab; 32];
pub const LEAF_SHA: [u8; 32] = [0xcd; 32];
/// Far in the future, so no timer (run timeout, start delay, expiry) fires in a test.
pub const FAR_MS: u64 = 4_102_444_800_000;
/// Event times far in the future too.
pub const TS0: u64 = FAR_MS * 1_000_000;

// ------------------------------------------------------------------ fake resolver

/// One version: (module sha, default queue, run timeout).
type Version = ([u8; 32], Option<String>, Option<u64>);

/// Workflows by qualified name: `(versions, active version)`.
#[derive(Default, Clone)]
pub struct Fake(BTreeMap<String, (BTreeMap<u32, Version>, u32)>);

impl Fake {
    /// `acme/leaf` v3 (active).
    pub fn leaf() -> Self {
        let mut f = Fake::default();
        f.add("acme/leaf", 3, LEAF_SHA);
        f
    }

    /// Adds `wf` version `v` and makes it active.
    pub fn add(&mut self, wf: &str, v: u32, sha: [u8; 32]) {
        let e = self.0.entry(wf.into()).or_default();
        e.0.insert(v, (sha, None, None));
        e.1 = v;
    }
}

impl ModuleResolver for Fake {
    fn resolve(&self, workflow: &str, version: Option<u32>) -> Option<ResolvedWorkflow> {
        let (vs, active) = self.0.get(workflow)?;
        let v = version.unwrap_or(*active);
        vs.get(&v).map(|(sha, _, t)| ResolvedWorkflow {
            version: v,
            module_sha: *sha,
            run_timeout_ms: *t,
        })
    }

    fn default_queue(&self, workflow: &str, version: u32) -> Option<String> {
        self.0.get(workflow)?.0.get(&version)?.1.clone()
    }
}

// ------------------------------------------------------------------ fixture

pub async fn open(dir: &Path, partitions: u16) -> Node {
    let mut cfg = NodeConfig::new(
        dir.to_path_buf(),
        PartitionCount::new(partitions).expect("non-zero"),
    );
    cfg.leader.shuffler.idle_poll = Duration::from_millis(20);
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

pub fn rref(run: &RunId) -> RunRef {
    RunRef {
        partition: run.partition().0,
        run_id: run.to_string(),
    }
}

pub fn base_start(run: &RunId, parent: Option<ParentRef>) -> StartRun {
    StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: "acme/wf".into(),
        workflow_version: 1,
        queue: "acme/default".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"{}"))),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        requested_at_ms: FAR_MS,
        ready_at_ms: FAR_MS,
        rng_seed: vec![0x5a; 32],
        module_sha: PARENT_SHA.to_vec(),
        tenant: "acme".into(),
        parent,
        ..Default::default()
    }
}

pub struct Fx {
    _dir: tempfile::TempDir,
    pub node: Node,
    pub partitions: u16,
}

impl Fx {
    pub async fn new(partitions: u16) -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let node = open(dir.path(), partitions).await;
        Self {
            _dir: dir,
            node,
            partitions,
        }
    }

    pub fn h(&self, run: &RunId) -> PartitionHandle<Rf1Log> {
        self.node
            .handle(run.partition())
            .expect("partition ready")
            .clone()
    }

    pub async fn submit(&self, p: u16, c: Command) -> ApplyOutcome {
        self.node
            .handle(PartitionId(p))
            .expect("ready")
            .submit_one(c)
            .await
            .expect("submit")
    }

    /// Starts `s`, grants attempt 1 and appends RunStarted (seq 0) raw.
    pub async fn start(&self, s: StartRun) -> (RunId, AttemptToken) {
        let run = RunId::from_bytes(&s.run_id).expect("id");
        assert_eq!(
            self.submit(run.partition().0, Command::start_run(s)).await,
            ApplyOutcome::Accepted
        );
        let token = self.h(&run).grant_attempt(run).await.expect("grant");
        self.raw(&run, token, 0, &[ev(0, step_body())]).await;
        (run, token)
    }

    pub async fn grant(&self, run: &RunId) -> AttemptToken {
        self.h(run).grant_attempt(*run).await.expect("grant")
    }

    pub fn sink(&self, run: RunId, token: AttemptToken, modules: Fake) -> PartitionSink {
        self.sink_with(run, token, modules, None)
    }

    pub fn sink_with(
        &self,
        run: RunId,
        token: AttemptToken,
        modules: Fake,
        inputs: Option<UnboundedSender<Event>>,
    ) -> PartitionSink {
        PartitionSink::new(self.h(&run), run, token, inputs, None)
            .with_resolvers(Arc::new(modules), Arc::new(InheritPolicy))
    }

    /// A raw EventsAppended without intents (a flush whose derive/post-flush step was lost).
    pub async fn raw(&self, run: &RunId, token: AttemptToken, from: u64, events: &[Event]) {
        let jevs = events
            .iter()
            .map(|e| {
                let (kind, bytes) = encode_event(e);
                JournalEvent {
                    seq: e.seq,
                    kind,
                    payload: Some(PayloadRef::inline(bytes.into())),
                }
            })
            .collect();
        let o = self
            .submit(
                run.partition().0,
                Command::events_appended(EventsAppended {
                    run_id: run.as_bytes().to_vec(),
                    token: Some(AttemptTokenMsg::from(token)),
                    from_seq: from,
                    events: jevs,
                    derived: vec![],
                }),
            )
            .await;
        assert_eq!(o, ApplyOutcome::Accepted);
    }

    pub fn record(&self, run: &RunId) -> Option<RunRecord> {
        query::run_record(&self.h(run).reader(), run).expect("read")
    }

    pub async fn journal(&self, run: &RunId) -> Vec<Event> {
        read_journal(&self.h(run), run).await.expect("journal")
    }

    pub fn row(&self, parent: &RunId, child: &RunId) -> Option<Vec<u8>> {
        self.h(parent)
            .reader()
            .get(Table::Children, &children_key(parent, child))
            .expect("read")
    }

    /// Runs on every partition.
    pub fn runs(&self) -> usize {
        (0..self.partitions)
            .map(|p| {
                self.node
                    .handle(PartitionId(p))
                    .expect("ready")
                    .reader()
                    .prefix(Table::Runs, &[], usize::MAX)
                    .expect("runs")
                    .len()
            })
            .sum()
    }

    pub async fn wait(&self, what: &str, mut f: impl FnMut(&Self) -> bool) {
        let t0 = Instant::now();
        while !f(self) {
            assert!(t0.elapsed() < T, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn wait_record(&self, run: &RunId) -> RunRecord {
        self.wait("run record", |f| f.record(run).is_some()).await;
        self.record(run).expect("record")
    }

    /// Waits until the journal of `run` holds a completion of `op`.
    pub async fn wait_completion(&self, run: &RunId, op: u64) -> Vec<Event> {
        let t0 = Instant::now();
        loop {
            let j = self.journal(run).await;
            if completion_of(&j, op).is_some() {
                return j;
            }
            assert!(t0.elapsed() < T, "no completion of op {op} for {run}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until every outbox is empty (all cross-partition messages applied).
    pub async fn drain(&self) {
        let rows = |f: &Self| -> usize {
            (0..f.partitions)
                .map(|p| {
                    f.node
                        .handle(PartitionId(p))
                        .expect("ready")
                        .reader()
                        .prefix(Table::Outbox, &[], usize::MAX)
                        .expect("outbox")
                        .len()
                })
                .sum()
        };
        self.wait("outboxes to drain", |f| rows(f) == 0).await;
    }
}

pub fn meta() -> CommandMeta {
    CommandMeta {
        hash_ver: 1,
        hash: CommandHash([7; 16]),
    }
}

pub fn ev(seq: u64, body: EventBody) -> Event {
    Event {
        seq,
        ts_nanos: TS0 + seq * 1_000_000,
        body,
    }
}

pub fn step_body() -> EventBody {
    EventBody::PatchMarker(ostro_types::journal::PatchMarker {
        meta: meta(),
        change_id: "x".into(),
    })
}

pub fn payload(data: &[u8]) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: data.to_vec(),
    }
}

pub fn child_started(seq: u64, workflow: &str, opts: ChildOptions) -> Event {
    ev(
        seq,
        EventBody::ChildStarted(ChildStarted {
            meta: meta(),
            workflow: workflow.into(),
            input: payload(b"{\"n\":7}"),
            options: opts,
        }),
    )
}

pub fn attached() -> ChildOptions {
    ChildOptions {
        queue: None,
        version: None,
        detached: false,
        run_timeout_ms: None,
    }
}

pub fn detached() -> ChildOptions {
    ChildOptions {
        detached: true,
        ..attached()
    }
}

pub fn op_cancelled(seq: u64, op: u64) -> Event {
    ev(
        seq,
        EventBody::OpCancelled(ostro_types::journal::OpCancelled { meta: meta(), op }),
    )
}

/// The first completion of `op` in `j`.
pub fn completion_of(j: &[Event], op: u64) -> Option<Result<OpOutput, Failure>> {
    j.iter().find_map(|e| match &e.body {
        EventBody::ChildCompleted(c) if c.op == op => {
            Some(decode_result(&c.outcome).expect("outcome"))
        }
        _ => None,
    })
}

/// How many completions of `op` `j` holds.
pub fn completions(j: &[Event], op: u64) -> usize {
    j.iter()
        .filter(|e| matches!(&e.body, EventBody::ChildCompleted(c) if c.op == op))
        .count()
}

/// The `{"run_id"}` payload of a detached child.
pub fn run_id_payload(child: &RunId) -> OpOutput {
    OpOutput::Child(Payload {
        content_type: "application/json".into(),
        data: format!("{{\"run_id\":\"{child}\"}}").into_bytes(),
    })
}

pub fn assert_kind(r: Option<Result<OpOutput, Failure>>, kind: &str) {
    match r {
        Some(Err(Failure::NonRetryable(i))) => assert_eq!(i.kind, kind, "{}", i.message),
        other => panic!("expected non-retryable {kind}, got {other:?}"),
    }
}
