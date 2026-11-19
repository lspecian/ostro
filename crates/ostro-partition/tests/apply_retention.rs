//! Run retention apply (plan 05-09, D9, OPS-06): `EXPIRE_OP` armed at every terminal
//! transition deletes every run-scoped row in one batch, the dedupe row included, so the
//! idempotency key can be reused; it is a no-op on a resumed run.
// Wall clock only to measure the 50k-event expiry (apply itself never reads a clock).
#![allow(clippy::disallowed_methods)]

use std::time::Instant;

use bytes::Bytes;
use ostro_partition::apply::{Action, ApplyOutcome, Cursor, apply_command};
use ostro_partition::store::keys::{
    decode_timer_key, dedupe_key, run_key, signal_seen_key, timer_key,
};
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, TABLES, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EXPIRE_OP,
    EventsAppended, JournalEvent, OutboxMsg, RetryRun, SendMessage, StartRun, TerminalStatus,
    TerminateRun, TimerFired, derived, encode_command,
};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

impl H {
    fn new() -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(2),
            PartitionCount::new(16).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        let mut h = H {
            _dir: dir,
            store,
            cur,
        };
        h.ok(&Command::leader_announced(1, 1, 0));
        h
    }

    fn apply_full(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>, u64) {
        let bytes = encode_command(c);
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
            .expect("apply");
        let reads = b.reads();
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads)
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).0
    }

    fn ok(&mut self, c: &Command) {
        assert_eq!(self.apply(c), ApplyOutcome::Accepted, "{}", c.kind_name());
    }

    fn put(&mut self, t: Table, k: Vec<u8>, v: Vec<u8>) {
        let mut b = self.store.batch();
        b.insert(t, k, v);
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn try_run(&self, id: &RunId) -> Option<RunRecord> {
        self.get(Table::Runs, &run_key(id))
            .map(|v| values::decode(&v).expect("decode"))
    }

    fn rows(&self, t: Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store
            .reader()
            .prefix(t, &[], usize::MAX)
            .expect("rows")
    }

    /// Rows of every run-scoped table that mention `run` (key contains its bytes, or the
    /// value is exactly its id, as in `dedupe`). `timers` keys are decoded. `meta`,
    /// `inbound_seen` and `outbox` are partition-scoped and skipped.
    fn rows_of(&self, run: &RunId) -> Vec<(Table, Vec<u8>)> {
        let id = run.as_bytes();
        let mut out = Vec::new();
        for t in TABLES {
            if matches!(t, Table::Meta | Table::InboundSeen | Table::Outbox) {
                continue;
            }
            for (k, v) in self.rows(t) {
                let hit = if t == Table::Timers {
                    decode_timer_key(&k).is_some_and(|(_, r, _)| r == *run)
                } else {
                    k.windows(id.len()).any(|w| w == id) || v.as_slice() == id.as_slice()
                };
                if hit {
                    out.push((t, k));
                }
            }
        }
        out
    }
}

fn rid(n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&2u16.to_be_bytes());
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn start(id: &RunId, key: &[u8], retention_ms: u64) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "acme/wf".into(),
        workflow_version: 1,
        queue: "acme/q".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"in"))),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        idempotency_key: key.to_vec(),
        retention_ms,
        run_timeout_ms: 60_000,
        schedule_to_start_ms: 30_000,
        ..Default::default()
    }
}

fn grant(r: &RunId) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        after_failure: false,
    })
}

fn append(r: &RunId, from: u64, count: u64, d: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n: 1 }),
        from_seq: from,
        events: (from..from + count)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
            })
            .collect(),
        derived: d,
    })
}

fn complete(status: TerminalStatus, at: u64, resumable: bool) -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: Some(PayloadRef::inline(Bytes::from_static(b"res"))),
        completed_at_ms: at,
        resumable,
        parent_event: None,
    }))
}

fn fire(r: &RunId, op: u64, due: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: due + 3,
    })
}

fn child_of(n: u8) -> Derived {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&9u16.to_be_bytes());
    b[17] = n;
    let child = RunId::from_bytes(&b).expect("id");
    Derived::new(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg::start_child(start(&child, b"", 0))),
    }))
}

/// A started, granted run with 3 events, a child, the given terminal status at 1000.
fn finished(h: &mut H, n: u8, key: &[u8], status: TerminalStatus, resumable: bool) -> RunId {
    let r = rid(n);
    h.ok(&Command::start_run(start(&r, key, 5_000)));
    h.ok(&grant(&r));
    h.ok(&append(&r, 0, 2, vec![child_of(n)]));
    h.ok(&append(&r, 2, 1, vec![complete(status, 1_000, resumable)]));
    r
}

#[test]
fn expiry_deletes_every_run_scoped_row_and_frees_the_idempotency_key() {
    let mut h = H::new();
    let keep = finished(&mut h, 1, b"other", TerminalStatus::Completed, false);
    let r = finished(&mut h, 2, b"order-42", TerminalStatus::Failed, true);
    // Rows apply would only write through 05-08 / Phase 6 paths: seed them directly.
    let id = r.as_bytes();
    let mut inbox_k = id.to_vec();
    inbox_k.extend_from_slice(&[0, 2, b'g', b'o', 0, 0, 0, 0, 0, 0, 0, 1]);
    h.put(Table::Inbox, inbox_k, b"slot".to_vec());
    h.put(Table::SignalSeen, signal_seen_key(&r, b"req-1"), Vec::new());
    let mut eff = id.to_vec();
    eff.extend_from_slice(&7u64.to_be_bytes());
    h.put(Table::Effects, eff.clone(), b"effect".to_vec());
    h.put(Table::Dlq, id.to_vec(), b"dlq".to_vec());
    let mut wait = vec![0x01];
    wait.extend_from_slice(id);
    wait.extend_from_slice(&3u64.to_be_bytes());
    h.put(Table::Waits, wait, Vec::new());
    assert_eq!(h.rows(Table::Timers).len(), 2, "one expiry row per run");
    let tables: std::collections::BTreeSet<_> = h.rows_of(&r).into_iter().map(|(t, _)| t).collect();
    assert!(
        tables.len() >= 9,
        "seeded every run-scoped table: {tables:?}"
    );
    let keep_rows = h.rows_of(&keep);

    // Before expiry the key is taken.
    let again = rid(3);
    assert_eq!(
        h.apply(&Command::start_run(start(&again, b"order-42", 5_000))),
        ApplyOutcome::Duplicate { existing: Some(r) }
    );
    let (out, actions, reads) = h.apply_full(&fire(&r, EXPIRE_OP, 6_000));
    assert_eq!(out, ApplyOutcome::Accepted);
    assert!(actions.is_empty());
    assert_eq!(reads, 10, "timer + record + 7 prefix scans + dedupe");
    assert!(h.rows_of(&r).is_empty(), "left: {:?}", h.rows_of(&r));
    assert_eq!(h.rows_of(&keep), keep_rows, "other runs untouched");
    // OPS-06: dedupe entries have retention; the same key now creates a NEW run.
    h.ok(&Command::start_run(start(&again, b"order-42", 5_000)));
    assert_eq!(
        h.get(Table::Dedupe, &dedupe_key("acme/wf", b"order-42")),
        Some(again.as_bytes().to_vec())
    );
    assert!(h.try_run(&again).is_some());
    // The expired run is unknown now: a duplicate fire finds no row.
    assert_eq!(
        h.apply(&fire(&r, EXPIRE_OP, 6_000)),
        ApplyOutcome::Duplicate { existing: None }
    );
}

#[test]
fn expiry_keeps_a_dedupe_row_that_points_elsewhere() {
    let mut h = H::new();
    let r = finished(&mut h, 1, b"k", TerminalStatus::Completed, false);
    let dk = dedupe_key("acme/wf", b"k");
    h.put(Table::Dedupe, dk.clone(), rid(77).as_bytes().to_vec());
    h.ok(&fire(&r, EXPIRE_OP, 6_000));
    assert!(h.try_run(&r).is_none());
    assert_eq!(h.get(Table::Dedupe, &dk), Some(rid(77).as_bytes().to_vec()));
}

#[test]
fn expiry_on_a_resumed_run_is_a_no_op() {
    let mut h = H::new();
    let r = finished(&mut h, 1, b"k", TerminalStatus::Failed, true);
    h.ok(&Command::retry_run(RetryRun {
        run_id: r.as_bytes().to_vec(),
        requested_at_ms: 2_000,
    }));
    assert!(
        h.rows(Table::Timers).is_empty(),
        "retry removed the expiry row"
    );
    // A stale fire of the old expiry (row re-seeded) only loses its row.
    h.put(Table::Timers, timer_key(6_000, &r, EXPIRE_OP), Vec::new());
    let before = h.rows_of(&r);
    h.ok(&fire(&r, EXPIRE_OP, 6_000));
    let mut want = before;
    want.retain(|(t, _)| *t != Table::Timers);
    assert_eq!(h.rows_of(&r), want);
    assert_eq!(h.try_run(&r).expect("run").status, RunStatus::Ready);
}

#[test]
fn expiry_ignores_rows_that_do_not_match_the_record() {
    let mut h = H::new();
    let r = finished(&mut h, 1, b"", TerminalStatus::Completed, false);
    // Wrong due time: stale row, the run stays.
    h.put(Table::Timers, timer_key(5_999, &r, EXPIRE_OP), Vec::new());
    h.ok(&fire(&r, EXPIRE_OP, 5_999));
    assert!(h.try_run(&r).is_some());
    // Unknown run: Duplicate after removing the row.
    let ghost = rid(50);
    h.put(Table::Timers, timer_key(10, &ghost, EXPIRE_OP), Vec::new());
    assert_eq!(
        h.apply(&fire(&ghost, EXPIRE_OP, 10)),
        ApplyOutcome::Duplicate { existing: None }
    );
    assert!(
        h.get(Table::Timers, &timer_key(10, &ghost, EXPIRE_OP))
            .is_none()
    );
    // No retention: no expiry row at all.
    let mut s = start(&rid(2), b"", 0);
    s.run_timeout_ms = 0;
    s.schedule_to_start_ms = 0;
    h.ok(&Command::start_run(s));
    h.ok(&Command::terminate_run(TerminateRun {
        run_id: rid(2).as_bytes().to_vec(),
        reason: "x".into(),
        terminated_at_ms: 500,
    }));
    assert!(
        h.rows(Table::Timers)
            .iter()
            .all(|(k, _)| decode_timer_key(k).is_some_and(|(_, run, _)| run != rid(2)))
    );
}

#[test]
fn a_50k_event_run_expires_in_one_apply() {
    const EVENTS: u64 = 50_000;
    const CHUNK: u64 = 5_000;
    let mut h = H::new();
    let r = rid(1);
    h.ok(&Command::start_run(start(&r, b"big", 5_000)));
    h.ok(&grant(&r));
    let mut from = 0;
    while from < EVENTS {
        h.ok(&append(&r, from, CHUNK, vec![]));
        from += CHUNK;
    }
    h.ok(&append(
        &r,
        EVENTS,
        0,
        vec![complete(TerminalStatus::Completed, 1_000, false)],
    ));
    assert_eq!(h.rows(Table::Events).len() as u64, EVENTS);

    let t0 = Instant::now();
    let (out, _, reads) = h.apply_full(&fire(&r, EXPIRE_OP, 6_000));
    let took = t0.elapsed();
    assert_eq!(out, ApplyOutcome::Accepted);
    assert_eq!(reads, 10);
    eprintln!("EXPIRE of a {EVENTS}-event run: apply + commit {took:?}");
    assert!(h.rows(Table::Events).is_empty());
    assert!(h.rows_of(&r).is_empty());
}
