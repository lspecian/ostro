//! The derived run index `x_run_index` (plan 11-05, OPS-05, D-P11-9; PF-11-2): apply hooks
//! keep `index rows == f(scan(runs))` ([`run_index_rows`]) atomically with the state change,
//! outside the digest; a missing or stale marker rebuilds from `runs`; pages are newest first,
//! bounded and budgeted.

// Wall clock only for the Leader-rebuild test timeout (apply never reads a clock).
#![allow(clippy::disallowed_methods)]

#[path = "children_common/mod.rs"]
mod cc;
mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use cc::{P, can_tail, child_start, expire, grant, retry, start, start_child_kind, terminate};
use ostro_partition::apply::{ApplyOutcome, Cursor, apply_command};
use ostro_partition::query::{IndexError, index_page, index_state};
use ostro_partition::store::derived::{
    IndexKind, IndexState, index_prefix, run_index_rows, status_code,
};
use ostro_partition::store::snapshot::install_snapshot;
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StartMode, StoreConfig, StoreMeta, Table};
use ostro_types::command::{
    AttemptTokenMsg, CompleteRun, CronStart, Derived, EventsAppended, InboundBatch, InboundMsg,
    OutboxMsg, StartRun, TerminalStatus, derived, encode_command, outbox_msg,
};
use ostro_types::ids::child_run_uuid;
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, RunId};
use proptest::prelude::*;
use prost::Message;

const SRC: u32 = 5;

fn pc() -> PartitionCount {
    PartitionCount::new(16).expect("non-zero")
}

fn open(dir: &std::path::Path) -> (PartitionStore, StoreMeta) {
    PartitionStore::open(dir, PartitionId(P), pc(), &StoreConfig::default()).expect("open store")
}

struct H {
    dir: tempfile::TempDir,
    store: Option<PartitionStore>,
    cur: Cursor,
    seq: u64,
}

impl H {
    fn new() -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = open(dir.path());
        let mut h = H {
            dir,
            store: Some(store),
            cur: Cursor::from_meta(&meta),
            seq: 0,
        };
        assert_eq!(
            h.apply(&Command::leader_announced(1, 1, 0)),
            ApplyOutcome::Accepted
        );
        h
    }

    fn store(&self) -> &PartitionStore {
        self.store.as_ref().expect("open")
    }

    /// Drop (crash: no checkpoint) and reopen.
    fn reopen(&mut self) {
        self.store = None;
        let (s, m) = open(self.dir.path());
        self.cur = Cursor::from_meta(&m);
        self.store = Some(s);
    }

    /// (outcome, index row writes of the batch).
    fn apply_n(&mut self, c: &Command) -> (ApplyOutcome, usize) {
        let bytes = encode_command(c);
        let store = self.store.as_ref().expect("open");
        let mut b = store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(1), &bytes, &mut actions)
            .expect("apply");
        let n = b.derived_len();
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, n)
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_n(c).0
    }

    fn ok(&mut self, c: &Command) {
        assert_eq!(self.apply(c), ApplyOutcome::Accepted, "{}", c.kind_name());
    }

    fn deliver(&mut self, kind: outbox_msg::Kind) -> ApplyOutcome {
        self.seq += 1;
        let c = Command::inbound_batch(InboundBatch {
            src: SRC,
            msgs: vec![InboundMsg {
                seq: self.seq,
                msg: Some(OutboxMsg { kind: Some(kind) }),
            }],
        });
        let o = self.apply(&c);
        if matches!(o, ApplyOutcome::Rejected(_)) {
            self.seq -= 1;
        }
        o
    }

    fn rec(&self, r: &RunId) -> Option<RunRecord> {
        self.store()
            .reader()
            .get(Table::Runs, r.as_bytes())
            .expect("read")
            .map(|v| values::decode(&v).expect("decode"))
    }

    fn outbox(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.store()
            .reader()
            .prefix(Table::Outbox, &[], usize::MAX)
            .expect("outbox")
    }

    /// Applies `c` and delivers every outbox message it produced back to this partition.
    fn apply_and_deliver(&mut self, c: &Command) -> ApplyOutcome {
        let before = self.outbox();
        let o = self.apply(c);
        for (_, v) in self.outbox() {
            if before.iter().any(|(_, b)| *b == v) {
                continue;
            }
            let kind = OutboxMsg::decode(v.as_slice())
                .expect("outbox")
                .kind
                .expect("kind");
            self.deliver(kind);
        }
        o
    }
}

/// Every index row (marker excluded).
fn actual(s: &PartitionStore) -> BTreeSet<Vec<u8>> {
    s.reader()
        .run_index_rows(&[], None, usize::MAX)
        .expect("index rows")
        .into_iter()
        .map(|(k, v)| {
            assert!(k[0] == 0x00 || v.is_empty(), "index rows carry no value");
            k
        })
        .filter(|k| k[0] != 0x00)
        .collect()
}

/// f(scan(runs)).
fn expected(s: &PartitionStore) -> BTreeSet<Vec<u8>> {
    let mut out = BTreeSet::new();
    for (k, v) in s
        .reader()
        .prefix(Table::Runs, &[], usize::MAX)
        .expect("runs")
    {
        let run = RunId::from_bytes(&k).expect("run id");
        let rec: RunRecord = values::decode(&v).expect("record");
        out.extend(run_index_rows(&run, &rec));
    }
    out
}

fn assert_index_eq(s: &PartitionStore, what: &str) {
    let (a, e) = (actual(s), expected(s));
    assert_eq!(
        a.len(),
        e.len(),
        "{what}: extra {:?} missing {:?}",
        a.difference(&e).collect::<Vec<_>>(),
        e.difference(&a).collect::<Vec<_>>()
    );
    assert_eq!(a, e, "{what}");
}

fn wait_ready(s: &PartitionStore) {
    s.join_run_index_rebuild();
    assert_eq!(s.run_index_state(), IndexState::Ready);
}

fn id(hi: u8, n: u16) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.to_be_bytes());
    b[2] = hi;
    b[16..].copy_from_slice(&n.to_be_bytes());
    RunId::from_bytes(&b).expect("18 bytes")
}

fn uuid(r: &RunId) -> [u8; 16] {
    r.as_bytes()[2..].try_into().expect("16")
}

fn append_n(r: &RunId, n: u32, from: u64, d: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: r.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 1, n }),
        from_seq: from,
        events: vec![cc::ev(from)],
        derived: d,
    })
}

fn complete_with(status: TerminalStatus, resumable: bool) -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: None,
        completed_at_ms: cc::DONE_AT,
        resumable,
        parent_event: None,
    }))
}

/// Grants the next attempt of a live run (any outcome).
fn grant_next(h: &mut H, r: &RunId) -> ApplyOutcome {
    let n = h
        .rec(r)
        .and_then(|rec| rec.current_attempt)
        .map_or(1, |(_, n)| n + 1);
    h.apply(&grant(r, n))
}

/// Appends one event plus `d` under the current attempt (any outcome).
fn append_current(h: &mut H, r: &RunId, d: Vec<Derived>) -> ApplyOutcome {
    let Some(rec) = h.rec(r) else {
        return ApplyOutcome::Duplicate { existing: None };
    };
    let n = rec.current_attempt.map_or(1, |(_, n)| n);
    h.apply_and_deliver(&append_n(r, n, rec.next_seq, d))
}

fn tagged(r: &RunId, tags: &[&str], workflow: &str, tenant: &str, at: u64) -> StartRun {
    let mut s = start(r);
    s.tags = tags.iter().map(|t| (*t).to_string()).collect();
    s.workflow = workflow.into();
    s.tenant = tenant.into();
    s.requested_at_ms = at;
    s.ready_at_ms = at;
    s
}

fn all_page(h: &H) -> Vec<(RunId, u64)> {
    index_page(
        &h.store().reader(),
        &index_prefix(IndexKind::All, &[]),
        None,
        10_000,
        10_000,
    )
    .expect("page")
    .runs
}

// ------------------------------------------------------------------- Task 1: hooks

#[test]
fn start_writes_all_tag_workflow_and_tenant_rows() {
    let mut h = H::new();
    let r = id(1, 1);
    h.ok(&Command::start_run(tagged(&r, &["a", "b"], "w", "t", 500)));
    let u = uuid(&r);
    let want: BTreeSet<_> = [
        ostro_partition::store::derived::index_key(IndexKind::All, &[], 500, &u),
        ostro_partition::store::derived::index_key(IndexKind::Tag, b"a", 500, &u),
        ostro_partition::store::derived::index_key(IndexKind::Tag, b"b", 500, &u),
        ostro_partition::store::derived::index_key(IndexKind::Workflow, b"w", 500, &u),
        ostro_partition::store::derived::index_key(IndexKind::Tenant, b"t", 500, &u),
    ]
    .into_iter()
    .collect();
    assert_eq!(actual(h.store()), want);
    // Default tenant: no tenant row.
    let d = id(1, 2);
    h.ok(&Command::start_run(tagged(&d, &["a"], "w", "", 600)));
    let rows = actual(h.store());
    assert_eq!(rows.len(), 5 + 3);
    let tenant = index_prefix(IndexKind::Tenant, b"");
    assert_eq!(
        rows.iter()
            .filter(|k| k[0] == IndexKind::Tenant as u8)
            .count(),
        1,
        "{tenant:?}"
    );
    assert_index_eq(h.store(), "after starts");
}

#[test]
fn terminal_resume_and_restart_maintain_status_rows() {
    let mut h = H::new();
    let r = id(1, 1);
    h.ok(&Command::start_run(tagged(&r, &["x"], "w", "", 100)));
    h.ok(&grant(&r, 1));
    h.ok(&append_n(
        &r,
        1,
        0,
        vec![complete_with(TerminalStatus::Failed, true)],
    ));
    let failed = index_prefix(
        IndexKind::Status,
        &[status_code(RunStatus::Failed).unwrap()],
    );
    let reader = h.store().reader();
    let page = index_page(&reader, &failed, None, 10, 10).expect("page");
    assert_eq!(page.runs, vec![(r, 100)]);
    assert_index_eq(h.store(), "failed");

    // Resume in place: the Status row goes, tag/workflow rows stay.
    h.ok(&retry(&r, 950));
    assert!(
        index_page(&reader, &failed, None, 10, 10)
            .expect("page")
            .runs
            .is_empty()
    );
    let tag = index_prefix(IndexKind::Tag, b"x");
    assert_eq!(
        index_page(&reader, &tag, None, 10, 10).unwrap().runs,
        vec![(r, 100)]
    );
    assert_index_eq(h.store(), "resumed");

    // Fail again, then a restart-mode retry: a new linked run with its own rows.
    h.ok(&grant(&r, 2));
    let next = h.rec(&r).unwrap().next_seq;
    h.ok(&append_n(
        &r,
        2,
        next,
        vec![complete_with(TerminalStatus::Failed, true)],
    ));
    let n = id(1, 2);
    let mut s = tagged(&n, &["x"], "w", "", 200);
    s.retry_of = r.as_bytes().to_vec();
    h.ok(&Command::start_run(s));
    assert_eq!(
        index_page(&reader, &tag, None, 10, 10).unwrap().runs,
        vec![(n, 200), (r, 100)]
    );
    assert_eq!(
        index_page(&reader, &failed, None, 10, 10).unwrap().runs,
        vec![(r, 100)]
    );
    assert_index_eq(h.store(), "restarted");
}

#[test]
fn expiry_removes_every_row_at_the_real_delete() {
    let mut h = H::new();
    let keep = id(1, 1);
    h.ok(&Command::start_run(tagged(&keep, &["x"], "w", "t", 100)));
    let r = id(1, 2);
    h.ok(&Command::start_run(tagged(&r, &["x", "y"], "w", "t", 200)));
    h.ok(&grant(&r, 1));
    h.ok(&append_n(
        &r,
        1,
        0,
        vec![complete_with(TerminalStatus::Completed, false)],
    ));
    let before = actual(h.store());
    assert_eq!(before.len(), 4 + 6);
    h.ok(&expire(&r, cc::DONE_AT + cc::RETENTION));
    assert!(h.rec(&r).is_none());
    assert_eq!(
        actual(h.store()).len(),
        4,
        "only the kept run's rows remain"
    );
    assert_index_eq(h.store(), "expired");
}

#[test]
fn deferred_chain_link_keeps_rows_until_the_real_delete() {
    let mut h = H::new();
    let p = id(1, 1);
    let c = RunId::new(PartitionId(P), child_run_uuid(&p, 1));
    let succ = id(1, 50);
    h.ok(&Command::start_run(start(&p)));
    assert_eq!(
        h.deliver(start_child_kind(child_start(&p, 1, &c, false))),
        ApplyOutcome::Accepted
    );
    h.ok(&grant(&c, 1));
    assert_eq!(
        append_current(&mut h, &c, can_tail(&succ)),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.rec(&c).unwrap().status, RunStatus::ContinuedAsNew);
    assert!(
        h.rec(&succ).is_some(),
        "successor started through inbound StartRun"
    );
    assert_index_eq(h.store(), "continued");
    // First expiry is deferred: the record stays, and so do its rows.
    h.ok(&expire(&c, cc::DONE_AT + cc::RETENTION));
    assert!(h.rec(&c).is_some());
    let can = index_prefix(
        IndexKind::Status,
        &[status_code(RunStatus::ContinuedAsNew).unwrap()],
    );
    let reader = h.store().reader();
    assert_eq!(
        index_page(&reader, &can, None, 10, 10).unwrap().runs.len(),
        1
    );
    assert_index_eq(h.store(), "deferred");
    // The bound deletes the record and the rows.
    h.ok(&expire(&c, cc::DONE_AT + 2 * cc::RETENTION));
    assert!(h.rec(&c).is_none());
    assert!(
        index_page(&reader, &can, None, 10, 10)
            .unwrap()
            .runs
            .is_empty()
    );
    assert_index_eq(h.store(), "deleted");
}

#[test]
fn ready_and_running_transitions_write_no_index_row() {
    let mut h = H::new();
    let r = id(1, 1);
    let (o, n) = h.apply_n(&Command::start_run(tagged(&r, &["a"], "w", "", 100)));
    assert_eq!((o, n), (ApplyOutcome::Accepted, 3), "All + Tag + Workflow");
    let (o, n) = h.apply_n(&grant(&r, 1));
    assert_eq!((o, n), (ApplyOutcome::Accepted, 0), "AttemptGranted");
    let (o, n) = h.apply_n(&append_n(&r, 1, 0, vec![]));
    assert_eq!(
        (o, n),
        (ApplyOutcome::Accepted, 0),
        "EventsAppended without terminal"
    );
    let (o, n) = h.apply_n(&append_n(
        &r,
        1,
        1,
        vec![complete_with(TerminalStatus::Failed, true)],
    ));
    assert_eq!((o, n), (ApplyOutcome::Accepted, 1), "terminal: Status row");
    let (o, n) = h.apply_n(&retry(&r, 950));
    assert_eq!(
        (o, n),
        (ApplyOutcome::Accepted, 1),
        "resume: Status row removed"
    );
    let (o, n) = h.apply_n(&grant(&r, 2));
    assert_eq!((o, n), (ApplyOutcome::Accepted, 0), "re-grant after resume");
}

/// Same workload twice, with and without the hooks: identical digest and state hash.
#[test]
fn digest_is_unchanged_by_the_index() {
    let run = |hooks: bool| {
        let mut h = H::new();
        h.store().set_run_index_hooks(hooks);
        for i in 0..20u16 {
            let r = id(3, i);
            h.ok(&Command::start_run(tagged(
                &r,
                &["a", "b"],
                "w",
                "t",
                100 + u64::from(i),
            )));
            if i % 2 == 0 {
                h.ok(&grant(&r, 1));
                h.ok(&append_n(
                    &r,
                    1,
                    0,
                    vec![complete_with(TerminalStatus::Failed, true)],
                ));
            }
            if i % 4 == 0 {
                h.ok(&retry(&r, 950));
            }
            if i % 6 == 2 {
                // Resumed runs' expiry rows are disarmed (Duplicate): any outcome.
                h.apply(&expire(&r, cc::DONE_AT + cc::RETENTION));
            }
        }
        let rows = actual(h.store()).len();
        (
            h.store().digest().expect("digest"),
            h.cur.meta_update().state_hash,
            rows,
        )
    };
    let (d1, s1, rows1) = run(true);
    let (d0, s0, rows0) = run(false);
    assert!(rows1 > 0 && rows0 == 0);
    assert_eq!(d1, d0, "digest");
    assert_eq!(s1, s0, "state hash");
}

// ------------------------------------------------- Task 2: rebuild, install, pages

#[test]
fn fresh_store_is_ready_and_marker_loss_rebuilds() {
    let mut h = H::new();
    assert_eq!(h.store().run_index_state(), IndexState::Ready);
    for i in 0..30u16 {
        h.ok(&Command::start_run(tagged(
            &id(1, i),
            &["t"],
            "w",
            "",
            10 + u64::from(i),
        )));
    }
    h.reopen();
    assert_eq!(
        h.store().run_index_state(),
        IndexState::Ready,
        "marker tracks applied"
    );
    h.store().delete_run_index_marker().unwrap();
    h.reopen();
    let reader = h.store().reader();
    assert_eq!(index_state(&reader), IndexState::Rebuilding);
    assert!(matches!(
        index_page(&reader, &index_prefix(IndexKind::All, &[]), None, 10, 10),
        Err(IndexError::Rebuilding)
    ));
    assert!(h.store().start_run_index_rebuild());
    wait_ready(h.store());
    assert_eq!(all_page(&h).len(), 30);
    assert_index_eq(h.store(), "rebuilt");
    assert!(!h.store().start_run_index_rebuild(), "ready: nothing to do");
}

#[test]
fn stale_marker_from_a_binary_without_hooks_rebuilds() {
    let mut h = H::new();
    for i in 0..10u16 {
        h.ok(&Command::start_run(tagged(&id(1, i), &["t"], "w", "", 10)));
    }
    // A downgrade: commands applied without the hooks; the marker offset lags.
    h.store().set_run_index_hooks(false);
    for i in 10..20u16 {
        h.ok(&Command::start_run(tagged(&id(1, i), &["t"], "w", "", 10)));
    }
    // Unknown run: Duplicate, still advances applied.
    assert_eq!(
        h.apply(&expire(&id(1, 99), 1)),
        ApplyOutcome::Duplicate { existing: None }
    );
    assert_ne!(actual(h.store()), expected(h.store()));
    h.reopen();
    assert_eq!(h.store().run_index_state(), IndexState::Rebuilding);
    h.store().start_run_index_rebuild();
    wait_ready(h.store());
    assert_index_eq(h.store(), "after stale-marker rebuild");
    // From now on commits keep the marker current.
    h.ok(&Command::start_run(tagged(&id(1, 30), &["t"], "w", "", 10)));
    h.reopen();
    assert_eq!(h.store().run_index_state(), IndexState::Ready);
    assert_index_eq(h.store(), "after reopen");
}

#[test]
fn installed_snapshot_opens_rebuilding_and_rebuilds() {
    let mut h = H::new();
    for i in 0..25u16 {
        let r = id(1, i);
        h.ok(&Command::start_run(tagged(
            &r,
            &["t"],
            "w",
            "x",
            10 + u64::from(i),
        )));
        if i % 3 == 0 {
            h.ok(&grant(&r, 1));
            h.ok(&append_n(
                &r,
                1,
                0,
                vec![complete_with(TerminalStatus::Completed, false)],
            ));
        }
    }
    let mut buf = Vec::new();
    h.store().export_snapshot(&mut buf, 1).expect("export");
    let other = ostro_testkit::fs::tempdir();
    install_snapshot(
        other.path(),
        PartitionId(P),
        pc(),
        &StoreConfig::default(),
        &buf[..],
    )
    .expect("install");
    let (s, _) = open(other.path());
    assert_eq!(
        s.run_index_state(),
        IndexState::Rebuilding,
        "OSNP carries no x_run_index"
    );
    assert!(actual(&s).is_empty());
    assert_eq!(s.digest().unwrap(), h.store().digest().unwrap());
    s.start_run_index_rebuild();
    wait_ready(&s);
    assert_index_eq(&s, "installed");
    assert_eq!(actual(&s), actual(h.store()));
}

#[test]
fn order_is_created_at_desc_then_uuid_desc_across_v7_and_v8_ids() {
    let mut h = H::new();
    let parent = id(0x10, 1);
    h.ok(&Command::start_run(tagged(&parent, &[], "w", "", 50)));
    let mut want = vec![(parent, 50u64)];
    for i in 0..40u16 {
        let at = 100 + u64::from(i % 7) * 10;
        if i % 2 == 0 {
            let r = id((i * 37 % 251) as u8, i);
            h.ok(&Command::start_run(tagged(&r, &[], "w", "", at)));
            want.push((r, at));
        } else {
            let c = RunId::new(PartitionId(P), child_run_uuid(&parent, u64::from(i)));
            let mut s = child_start(&parent, u64::from(i), &c, true);
            s.requested_at_ms = at;
            s.ready_at_ms = at;
            assert_eq!(h.deliver(start_child_kind(s)), ApplyOutcome::Accepted);
            want.push((c, at));
        }
    }
    want.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| uuid(&b.0).cmp(&uuid(&a.0))));
    assert_eq!(all_page(&h), want);
}

#[test]
fn pages_are_bounded_budgeted_and_gapless() {
    let mut h = H::new();
    for i in 0..120u16 {
        let tags: &[&str] = if i % 4 == 3 { &["other"] } else { &["t"] };
        h.ok(&Command::start_run(tagged(
            &id(2, i),
            tags,
            "w",
            "",
            1_000 + u64::from(i),
        )));
    }
    let reader = h.store().reader();
    let prefix = index_prefix(IndexKind::Tag, b"t");
    let mut seen = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    let mut pages = 0;
    loop {
        let p = index_page(&reader, &prefix, after.as_deref(), 50, 200).expect("page");
        assert!(p.scanned <= 200);
        assert!(p.runs.len() <= 50);
        pages += 1;
        seen.extend(p.runs.iter().map(|(r, at)| (*r, *at)));
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(pages, 2, "90 runs: 50 + 40");
    assert_eq!(seen.len(), 90);
    let uniq: BTreeSet<_> = seen.iter().map(|(r, _)| *r).collect();
    assert_eq!(uniq.len(), 90, "no duplicates");
    assert!(seen.windows(2).all(|w| w[0].1 > w[1].1), "newest first");
    let first = index_page(&reader, &prefix, None, 50, 200).unwrap();
    assert_eq!(first.runs[0].1, 1_000 + 118);
    // Budget below the limit: at most `budget` rows, never a peek.
    let p = index_page(&reader, &prefix, None, 50, 7).unwrap();
    assert_eq!((p.runs.len(), p.scanned), (7, 7));
    assert!(p.next.is_some());
    // Exactly exhausted with budget == limit: one empty follow-up page.
    let mut after = None;
    let mut total = 0;
    for _ in 0..100 {
        let p = index_page(&reader, &prefix, after.as_deref(), 45, 45).unwrap();
        total += p.runs.len();
        match p.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(total, 90);
    // An unknown tag: empty and exhausted.
    let none = index_page(
        &reader,
        &index_prefix(IndexKind::Tag, b"nope"),
        None,
        50,
        200,
    )
    .unwrap();
    assert!(none.runs.is_empty() && none.next.is_none());
}

/// The rebuild races with live apply (starts, terminals, resumes, expiries) and converges.
#[test]
fn rebuild_racing_live_apply_converges() {
    let mut h = H::new();
    let n = 3_000u16;
    for i in 0..n {
        let r = id((i % 200) as u8, i);
        h.ok(&Command::start_run(tagged(
            &r,
            &["t"],
            "w",
            "",
            u64::from(i),
        )));
        if i % 5 == 0 {
            h.ok(&grant(&r, 1));
            h.ok(&append_n(
                &r,
                1,
                0,
                vec![complete_with(TerminalStatus::Failed, true)],
            ));
        }
    }
    h.store().delete_run_index_marker().unwrap();
    h.reopen();
    assert!(h.store().start_run_index_rebuild());
    for i in 0..n {
        let r = id((i % 200) as u8, i);
        match i % 5 {
            0 if i % 10 == 0 => h.ok(&retry(&r, 950)),
            0 => h.ok(&expire(&r, cc::DONE_AT + cc::RETENTION)),
            1 => {
                h.ok(&grant(&r, 1));
                h.ok(&append_n(
                    &r,
                    1,
                    0,
                    vec![complete_with(TerminalStatus::Completed, false)],
                ));
            }
            _ => {}
        }
        if i % 300 == 0 {
            h.ok(&Command::start_run(tagged(
                &id(250, i),
                &["new"],
                "w",
                "",
                5_000,
            )));
        }
    }
    wait_ready(h.store());
    assert_index_eq(h.store(), "after racing rebuild");
}

/// `StartMode::Inspect` never starts the rebuild; a Leader start does.
#[tokio::test(flavor = "multi_thread")]
async fn inspect_mode_never_rebuilds_and_leader_mode_does() {
    let root = ostro_testkit::fs::tempdir();
    let dir = root.path();
    let p0 = PartitionId(0);
    let node = common::open_node(dir, 1, StartMode::Leader).await;
    let h = node.handle(p0).expect("ready");
    for _ in 0..20 {
        let mut s = common::start_run(&common::run_on(0));
        s.tags = vec!["t".into()];
        let o = h.submit_one(Command::start_run(s)).await.expect("submit");
        assert_eq!(o, ApplyOutcome::Accepted);
    }
    drop(h); // an owned handle keeps the store open
    node.shutdown().await.expect("shutdown");
    {
        let (s, _) = PartitionStore::open(
            dir,
            p0,
            PartitionCount::new(1).unwrap(),
            &StoreConfig::default(),
        )
        .expect("open");
        assert_eq!(s.run_index_state(), IndexState::Ready);
        s.delete_run_index_marker().unwrap();
    }
    let node = common::open_node(dir, 1, StartMode::Inspect).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let store = node.handle(p0).expect("ready").store().clone();
    assert_eq!(store.run_index_state(), IndexState::Rebuilding);
    drop(store);
    node.shutdown().await.expect("shutdown");

    let node = common::open_node(dir, 1, StartMode::Leader).await;
    let store = node.handle(p0).expect("ready").store().clone();
    let deadline = Instant::now() + Duration::from_secs(30); // clock-exempt: test timeout
    while store.run_index_state() != IndexState::Ready {
        assert!(Instant::now() < deadline, "leader start never rebuilt"); // clock-exempt: test timeout
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let reader = store.reader();
    let page = index_page(&reader, &index_prefix(IndexKind::Tag, b"t"), None, 100, 100).unwrap();
    assert_eq!(page.runs.len(), 20);
    assert_index_eq(&store, "leader rebuild");
    drop(store);
    node.shutdown().await.expect("shutdown");
}

// ------------------------------------------------------------------- property test

#[derive(Clone, Debug)]
struct Shape {
    hi: u8,
    tags: u8,
    wf: u8,
    tenant: Option<u8>,
    at: u64,
}

#[derive(Clone, Debug)]
enum Op {
    Start(Shape),
    Child(Shape, usize, u64),
    Cron(Shape, u64),
    Grant(usize),
    Complete(usize, u8, bool),
    ContinueAsNew(usize, u8),
    Terminate(usize),
    Resume(usize),
    Restart(usize, Shape),
    Expire(usize),
    ExpireBound(usize),
    Crash,
}

fn shape() -> impl Strategy<Value = Shape> {
    (
        any::<u8>(),
        0u8..16,
        0u8..3,
        proptest::option::of(0u8..2),
        0u64..20,
    )
        .prop_map(|(hi, tags, wf, tenant, at)| Shape {
            hi,
            tags,
            wf,
            tenant,
            at: 100 + at * 10,
        })
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => shape().prop_map(Op::Start),
        2 => (shape(), any::<usize>(), 0u64..8).prop_map(|(s, p, o)| Op::Child(s, p, o)),
        1 => (shape(), 0u64..4).prop_map(|(s, f)| Op::Cron(s, f)),
        3 => any::<usize>().prop_map(Op::Grant),
        3 => (any::<usize>(), 0u8..3, any::<bool>()).prop_map(|(i, s, r)| Op::Complete(i, s, r)),
        1 => (any::<usize>(), any::<u8>()).prop_map(|(i, h)| Op::ContinueAsNew(i, h)),
        1 => any::<usize>().prop_map(Op::Terminate),
        2 => any::<usize>().prop_map(Op::Resume),
        1 => (any::<usize>(), shape()).prop_map(|(i, s)| Op::Restart(i, s)),
        2 => any::<usize>().prop_map(Op::Expire),
        1 => any::<usize>().prop_map(Op::ExpireBound),
        1 => Just(Op::Crash),
    ]
}

const TAGS: [&str; 4] = ["a", "b", "nul\0tag", ""];
const WFS: [&str; 3] = ["wf", "acme/orders", "w"];
const TENANTS: [&str; 2] = ["t1", "t2"];

fn shaped(r: &RunId, s: &Shape) -> StartRun {
    let tags: Vec<&str> = (0..4)
        .filter(|b| s.tags & (1 << b) != 0)
        .map(|b| TAGS[b])
        .collect();
    tagged(
        r,
        &tags,
        WFS[usize::from(s.wf)],
        s.tenant.map_or("", |t| TENANTS[usize::from(t)]),
        s.at,
    )
}

fn run_ops(ops: &[Op]) {
    let mut h = H::new();
    let mut runs: Vec<RunId> = vec![id(0, 0)];
    let mut next: u16 = 1;
    let mut fresh = |hi: u8| {
        next += 1;
        id(hi, next)
    };
    let pick = |runs: &[RunId], i: usize| runs[i % runs.len()];
    for op in ops {
        match op {
            Op::Start(s) => {
                let r = fresh(s.hi);
                h.apply(&Command::start_run(shaped(&r, s)));
                runs.push(r);
            }
            Op::Child(s, p, o) => {
                let parent = pick(&runs, *p);
                let c = RunId::new(PartitionId(P), child_run_uuid(&parent, *o));
                let mut st = shaped(&c, s);
                st.parent = Some(ostro_types::command::ParentRef {
                    run_id: parent.as_bytes().to_vec(),
                    op: *o,
                });
                h.deliver(start_child_kind(st));
                runs.push(c);
            }
            Op::Cron(s, f) => {
                let r = fresh(s.hi);
                h.deliver(outbox_msg::Kind::CronStart(CronStart {
                    schedule_id: "s1".into(),
                    fire_time_ms: *f,
                    start: Some(shaped(&r, s)),
                }));
                runs.push(r);
            }
            Op::Grant(i) => {
                let r = pick(&runs, *i);
                grant_next(&mut h, &r);
            }
            Op::Complete(i, st, resumable) => {
                let r = pick(&runs, *i);
                let st = [
                    TerminalStatus::Completed,
                    TerminalStatus::Failed,
                    TerminalStatus::Cancelled,
                ][usize::from(*st)];
                append_current(&mut h, &r, vec![complete_with(st, *resumable)]);
            }
            Op::ContinueAsNew(i, hi) => {
                let r = pick(&runs, *i);
                let succ = fresh(*hi);
                if append_current(&mut h, &r, can_tail(&succ)) == ApplyOutcome::Accepted {
                    runs.push(succ);
                }
            }
            Op::Terminate(i) => {
                h.apply(&terminate(&pick(&runs, *i), "op", 900));
            }
            Op::Resume(i) => {
                h.apply(&retry(&pick(&runs, *i), 950));
            }
            Op::Restart(i, s) => {
                let old = pick(&runs, *i);
                let r = fresh(s.hi);
                let mut st = shaped(&r, s);
                st.retry_of = old.as_bytes().to_vec();
                h.apply(&Command::start_run(st));
                runs.push(r);
            }
            Op::Expire(i) | Op::ExpireBound(i) => {
                let r = pick(&runs, *i);
                if let Some(rec) = h.rec(&r) {
                    let k = if matches!(op, Op::Expire(_)) { 1 } else { 2 };
                    let due = rec
                        .completed_at_ms
                        .unwrap_or(0)
                        .saturating_add(k * rec.retention_ms);
                    h.apply(&expire(&r, due));
                }
            }
            Op::Crash => h.reopen(),
        }
    }
    // (a) live
    assert_index_eq(h.store(), "live");
    // (b) crash / reopen
    h.reopen();
    assert_eq!(h.store().run_index_state(), IndexState::Ready);
    assert_index_eq(h.store(), "reopened");
    // (c) marker deleted: reopen, rebuild, compare
    h.store().delete_run_index_marker().unwrap();
    h.reopen();
    assert_eq!(h.store().run_index_state(), IndexState::Rebuilding);
    h.store().start_run_index_rebuild();
    wait_ready(h.store());
    assert_index_eq(h.store(), "rebuilt");
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn index_rows_equal_f_of_runs(ops in proptest::collection::vec(op(), 1..80)) {
        run_ops(&ops);
    }
}
