//! Rerun prefix apply (plan 11-07, OPS-02, D-P11-3/6, W14): a StartRun with
//! `rerun_from_seq` (41) != 0 journals `run_started` (23) + `rerun_prefix` (40) verbatim as
//! events `0..cut`, sets `next_seq = cut` and `status_seq`, and activates like any start.
//! Invalid prefixes are `Invalid(BadRerunPrefix)` with nothing written. A StartRun without
//! the fields is unchanged (the existing `apply_run_started` / `rebuild_digest` tests).

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::store::derived::run_index_rows;
use ostro_partition::store::keys::{event_key, run_key};
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, ConflictPolicy, EventsAppended, JournalEvent, RERUN_FROM_TAG,
    StartRun, encode_command,
};
use ostro_types::journal::EventKind;
use ostro_types::payload::INLINE_PAYLOAD_LIMIT;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use prost::Message;

const P: u16 = 3;

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
    log: Vec<Vec<u8>>,
}

fn open_store() -> (tempfile::TempDir, PartitionStore, Cursor) {
    let dir = ostro_testkit::fs::tempdir();
    let (store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(P),
        PartitionCount::new(16).expect("non-zero"),
        &StoreConfig::default(),
    )
    .expect("open store");
    let cur = Cursor::from_meta(&meta);
    (dir, store, cur)
}

impl H {
    fn new() -> Self {
        let (dir, store, cur) = open_store();
        let mut h = H {
            _dir: dir,
            store,
            cur,
            log: Vec::new(),
        };
        assert_eq!(
            h.apply(&Command::leader_announced(1, 1, 0)),
            ApplyOutcome::Accepted
        );
        h
    }

    /// One command in its own committed batch: (outcome, actions, overlay writes).
    fn apply_full(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>, usize) {
        let bytes = encode_command(c);
        self.log.push(bytes.to_vec());
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
            .expect("apply");
        let writes = b.len();
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, writes)
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).0
    }

    fn run(&self, id: &RunId) -> Option<RunRecord> {
        self.store
            .reader()
            .get(Table::Runs, &run_key(id))
            .expect("read")
            .map(|v| values::decode(&v).expect("decode"))
    }

    /// The raw `events` rows of `r` in seq order.
    fn rows(&self, r: &RunId) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for seq in 0.. {
            match self
                .store
                .reader()
                .get(Table::Events, &event_key(r, seq))
                .expect("read")
            {
                Some(v) => out.push(v.to_vec()),
                None => break,
            }
        }
        out
    }

    fn index_rows(&self) -> Vec<Vec<u8>> {
        self.store
            .reader()
            .run_index_rows(&[], None, usize::MAX)
            .expect("index rows")
            .into_iter()
            .map(|(k, _)| k)
            .collect()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }

    /// Every logged command applied again to a fresh store reproduces the live digest.
    fn assert_rebuild_equal(&self) {
        let (_dir, store, mut cur) = open_store();
        let mut b = store.batch();
        let mut actions = Vec::new();
        for bytes in &self.log {
            let off = cur.next;
            apply_command(&mut b, &mut cur, off, Epoch(1), bytes, &mut actions).expect("apply");
        }
        let mu = cur.meta_update();
        b.commit(&mu).expect("commit");
        assert_eq!(store.digest().expect("digest"), self.digest(), "rebuild");
    }
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn inline(b: &[u8]) -> PayloadRef {
    PayloadRef::inline(Bytes::copy_from_slice(b))
}

fn ev(seq: u64, kind: EventKind, body: &[u8]) -> JournalEvent {
    JournalEvent {
        seq,
        kind: kind.code(),
        payload: Some(inline(body)),
        ..Default::default()
    }
}

fn started() -> JournalEvent {
    ev(0, EventKind::RunStarted, b"\x01opaque-run-started")
}

fn base_start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: Some(inline(b"in")),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        run_started: Some(started()),
        tags: vec!["user".into()],
        ..Default::default()
    }
}

fn tag(cut: u64) -> String {
    format!("{RERUN_FROM_TAG}{cut}")
}

/// A rerun StartRun copying `prefix` (events 1..cut) with cut = len + 1 and the tag.
fn rerun_start(id: &RunId, run_started: JournalEvent, prefix: Vec<JournalEvent>) -> StartRun {
    let cut = prefix.len() as u64 + 1;
    let mut s = base_start(id);
    s.run_started = Some(run_started);
    s.rerun_prefix = prefix;
    s.rerun_from_seq = cut;
    s.tags.push(tag(cut));
    s
}

/// The synthetic source journal events 1..=5 (the attempt stamps are kept verbatim).
fn source_events() -> Vec<JournalEvent> {
    let mut v = vec![
        ev(1, EventKind::StepCompleted, b"s1"),
        ev(2, EventKind::StepCompleted, b"s2"),
        ev(3, EventKind::TimerStarted, b"t"),
        ev(4, EventKind::StatusPublished, b"{\"p\":1}"),
        ev(5, EventKind::StepAttemptFailed, b"f"),
    ];
    for e in &mut v {
        e.attempt = (1u64 << 32) | 1;
    }
    v
}

/// Source run `src` with events 0..=5 journaled through the ordinary path.
fn source_run(h: &mut H, src: &RunId) {
    assert_eq!(
        h.apply(&Command::start_run(base_start(src))),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&Command::attempt_granted(AttemptGranted {
            run_id: src.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
            after_failure: false,
        })),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(&Command::events_appended(EventsAppended {
            run_id: src.as_bytes().to_vec(),
            token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
            from_seq: 1,
            events: source_events(),
            derived: vec![],
        })),
        ApplyOutcome::Accepted
    );
}

fn enqueued(actions: &[Action]) -> bool {
    actions.iter().any(|a| {
        matches!(
            a,
            Action::EnqueueReady {
                first_activation: true,
                ..
            }
        )
    })
}

const BAD: ApplyOutcome =
    ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::BadRerunPrefix));

#[test]
fn prefix_is_journaled_verbatim_and_the_run_activates() {
    let mut h = H::new();
    let src = rid(1);
    source_run(&mut h, &src);
    let src_rows = h.rows(&src);
    assert_eq!(src_rows.len(), 6);
    // The control plane copies the stored rows (decoded as JournalEvent) up to cut = 6.
    let copied: Vec<JournalEvent> = src_rows
        .iter()
        .map(|r| JournalEvent::decode(r.as_slice()).expect("row"))
        .collect();
    let new = rid(2);
    let s = rerun_start(&new, copied[0].clone(), copied[1..].to_vec());
    assert_eq!(s.rerun_from_seq, 6);
    let (out, actions, _) = h.apply_full(&Command::start_run(s));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(
        h.rows(&new),
        src_rows,
        "events 0..6 byte-identical to the source"
    );
    let rec = h.run(&new).expect("run");
    assert_eq!(rec.next_seq, 6);
    assert_eq!(rec.status_seq, Some(4));
    assert_eq!(rec.status, RunStatus::Ready);
    assert!(rec.current_attempt.is_none());
    assert!(
        enqueued(&actions),
        "activation enqueued like a normal start"
    );
    // 11-05 index rows exist for the new run.
    let idx = h.index_rows();
    for row in run_index_rows(&new, &rec) {
        assert!(idx.contains(&row), "index row missing");
    }
    // Copied events keep the source's attempt stamps.
    for r in &h.rows(&new)[1..] {
        assert_eq!(
            JournalEvent::decode(r.as_slice()).expect("row").attempt,
            (1 << 32) | 1
        );
    }
    h.assert_rebuild_equal();
}

#[test]
fn cut_one_journals_only_run_started() {
    let mut h = H::new();
    let new = rid(3);
    let s = rerun_start(&new, started(), vec![]);
    assert_eq!(s.rerun_from_seq, 1);
    let (out, actions, _) = h.apply_full(&Command::start_run(s));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(h.rows(&new), vec![started().encode_to_vec()]);
    let rec = h.run(&new).expect("run");
    assert_eq!(rec.next_seq, 1);
    assert_eq!(rec.status_seq, None);
    assert_eq!(rec.status, RunStatus::Ready);
    assert!(enqueued(&actions));
    h.assert_rebuild_equal();
}

#[test]
fn blob_ref_payloads_are_stored_as_refs() {
    let mut h = H::new();
    let new = rid(4);
    let blob = JournalEvent {
        seq: 1,
        kind: EventKind::StepCompleted.code(),
        payload: Some(PayloadRef::blob([9; 32], 10 * 1024 * 1024)),
        ..Default::default()
    };
    let s = rerun_start(&new, started(), vec![blob.clone()]);
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    let rows = h.rows(&new);
    assert_eq!(rows[1], blob.encode_to_vec());
    assert_eq!(h.run(&new).expect("run").next_seq, 2);
}

#[test]
fn rerun_dedupe_returns_existing() {
    let mut h = H::new();
    let new = rid(5);
    let mut s = rerun_start(&new, started(), vec![ev(1, EventKind::StepCompleted, b"x")]);
    s.idempotency_key = b"__rerun/src/k".to_vec();
    assert_eq!(
        h.apply(&Command::start_run(s.clone())),
        ApplyOutcome::Accepted
    );
    // Re-submit under a different run id with the same key.
    s.run_id = rid(6).as_bytes().to_vec();
    assert_eq!(
        h.apply(&Command::start_run(s)),
        ApplyOutcome::Duplicate {
            existing: Some(new)
        }
    );
    assert!(h.run(&rid(6)).is_none());
}

#[test]
fn invalid_prefixes_are_rejected_without_writes() {
    let prefix = || {
        vec![
            ev(1, EventKind::StepCompleted, b"a"),
            ev(2, EventKind::TimerStarted, b"b"),
        ]
    };
    let mut cases: Vec<(&str, StartRun)> = Vec::new();
    let id = rid(7);

    let mut s = base_start(&id);
    s.rerun_prefix = prefix();
    cases.push(("field 40 without field 41", s));

    let mut s = rerun_start(&id, started(), prefix());
    s.run_started = None;
    cases.push(("no run_started", s));

    let mut s = rerun_start(&id, started(), prefix());
    s.rerun_prefix[1].seq = 3;
    cases.push(("seq gap", s));

    let mut s = rerun_start(&id, started(), prefix());
    s.rerun_from_seq = 4;
    s.tags = vec![tag(4)];
    cases.push(("rerun_from_seq off by one", s));

    for k in [
        EventKind::RunCompleted,
        EventKind::RunFailed,
        EventKind::RunContinuedAsNew,
        EventKind::RunTerminated,
    ] {
        let mut p = prefix();
        p.push(ev(3, k, b"t"));
        cases.push(("terminal kind", rerun_start(&id, started(), p)));
    }
    for k in [5u32, 200, 1, 15, 128] {
        let mut p = prefix();
        p[1].kind = k;
        cases.push(("kind out of range", rerun_start(&id, started(), p)));
    }

    let mut p = prefix();
    p[0].payload = Some(inline(&vec![0u8; INLINE_PAYLOAD_LIMIT + 1]));
    cases.push(("oversize inline", rerun_start(&id, started(), p)));

    let mut p = prefix();
    p[0].payload = None;
    cases.push(("missing payload", rerun_start(&id, started(), p)));

    let mut s = rerun_start(&id, started(), prefix());
    s.tags.retain(|t| !t.starts_with(RERUN_FROM_TAG));
    cases.push(("no rerun tag", s));

    let mut s = rerun_start(&id, started(), prefix());
    s.tags.push(tag(3));
    cases.push(("two rerun tags", s));

    let mut s = rerun_start(&id, started(), prefix());
    s.tags = vec![tag(2)];
    cases.push(("tag value != field 41", s));

    let mut s = rerun_start(&id, started(), prefix());
    s.tags = vec![format!("{RERUN_FROM_TAG}03")];
    cases.push(("non-canonical tag value", s));

    let mut s = rerun_start(&id, started(), prefix());
    s.run_started = Some(ev(0, EventKind::StepCompleted, b"x"));
    cases.push(("run_started of the wrong kind", s));

    for (what, s) in cases {
        let mut h = H::new();
        let (out, actions, writes) = h.apply_full(&Command::start_run(s));
        assert_eq!(out, BAD, "{what}");
        assert_eq!(writes, 0, "{what}: nothing written");
        assert!(actions.is_empty(), "{what}");
        assert!(h.run(&id).is_none(), "{what}");
        assert!(h.rows(&id).is_empty(), "{what}");
    }
}

#[test]
fn start_without_rerun_fields_is_unchanged() {
    let mut h = H::new();
    let r = rid(8);
    let (out, _, _) = h.apply_full(&Command::start_run(base_start(&r)));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(h.rows(&r), vec![started().encode_to_vec()]);
    let rec = h.run(&r).expect("run");
    assert_eq!(rec.next_seq, 1);
    // A user tag that merely looks like the rerun tag does nothing without field 41.
    let r2 = rid(9);
    let mut s = base_start(&r2);
    s.tags.push(tag(5));
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r2).expect("run").next_seq, 1);
    h.assert_rebuild_equal();
}
