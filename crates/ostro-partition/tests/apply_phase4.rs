//! Phase 4 apply additions (plan 04-12; API-03, DISP-01, DISP-02): StartRun fields 14..18
//! validated and stored in `RunRecord`, `CompleteRun.completed_at_ms` stored, the `Running`
//! keyspace (a run is indexed from `AttemptGranted` until it leaves Running), the `Suspend`
//! intent with its `from_seq` compare-and-set, and `Action::InputAppended`.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::store::keys::{event_key, ready_key, run_key, timer_key};
use ostro_partition::store::values::{self, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EnqueueReady,
    EventsAppended, JournalEvent, MAX_RUN_TAG_LEN, MAX_RUN_TAGS, MAX_TENANT_LEN,
    MAX_TRACEPARENT_LEN, MODULE_SHA_LEN, ScheduleTimer, StartRun, TerminalStatus, TimerFired,
    derived, encode_command,
};
use ostro_types::{AttemptToken, Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};

// ---------------------------------------------------------------- helpers

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

impl H {
    fn new(p: u16) -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(p),
            PartitionCount::new(16).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        H {
            _dir: dir,
            store,
            cur,
        }
    }

    /// Applies one command in its own committed batch: (outcome, actions, reads).
    fn apply_full(&mut self, env: u32, c: &Command) -> (ApplyOutcome, Vec<Action>, u64) {
        let bytes = encode_command(c);
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let before = b.reads();
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(env), &bytes, &mut actions)
            .expect("apply");
        let reads = b.reads() - before;
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads)
    }

    fn apply(&mut self, env: u32, c: &Command) -> ApplyOutcome {
        self.apply_full(env, c).0
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn run(&self, id: &RunId) -> RunRecord {
        values::decode(&self.get(Table::Runs, &run_key(id)).expect("run row")).expect("decode")
    }

    /// The Running row of `id`, decoded as `(epoch, n)`.
    fn running_row(&self, id: &RunId) -> Option<(u32, u32)> {
        self.get(Table::Running, &run_key(id))
            .map(|v| values::decode(&v).expect("decode running row"))
    }

    fn all_running(&self) -> usize {
        self.store
            .reader()
            .prefix(Table::Running, &[], usize::MAX)
            .expect("scan")
            .len()
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }
}

fn rid(p: u16, n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[2] = n.wrapping_mul(7);
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn inline(b: &'static [u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from_static(b)))
}

fn start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: inline(b"in"),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: "ck".into(),
        priority: 5,
        ready_at_ms: 100,
        requested_at_ms: 100,
        rng_seed: vec![7; 32],
        ..Default::default()
    }
}

fn la(e: u32) -> Command {
    Command::leader_announced(e, 1, 0)
}

fn grant(id: &RunId, e: u32, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: e, n }),
        after_failure: false,
    })
}

fn ev(seq: u64) -> JournalEvent {
    JournalEvent {
        seq,
        kind: 16,
        payload: inline(b"e"),
    }
}

fn events(
    id: &RunId,
    tok: (u32, u32),
    from: u64,
    evs: Vec<JournalEvent>,
    derived: Vec<Derived>,
) -> Command {
    Command::events_appended(EventsAppended {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: tok.0,
            n: tok.1,
        }),
        from_seq: from,
        events: evs,
        derived,
    })
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn complete_at(at: u64) -> Derived {
    intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: inline(b"out"),
        completed_at_ms: at,
        resumable: false,
        parent_event: None,
    }))
}

fn enqueue(at: u64) -> Derived {
    intent(derived::Intent::EnqueueReady(EnqueueReady {
        ready_at_ms: at,
    }))
}

fn rej(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

fn bad(c: InvalidCode) -> ApplyOutcome {
    rej(RejectReason::Invalid(c))
}

/// Epoch 1 announced, run `r` started (ready at 100) and granted `{1,1}`.
fn running(p: u16, n: u8) -> (H, RunId) {
    let mut h = H::new(p);
    let r = rid(p, n);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(1, &Command::start_run(start(&r))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(1, &grant(&r, 1, 1)), ApplyOutcome::Accepted);
    (h, r)
}

// ------------------------------------------------------- StartRun fields 14..18

#[test]
fn start_run_new_fields_stored() {
    let mut h = H::new(0);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);

    let r = rid(0, 1);
    let s = StartRun {
        module_sha: vec![0xab; MODULE_SHA_LEN],
        tags: vec![
            "env:prod".into(),
            String::new(),
            "x".repeat(MAX_RUN_TAG_LEN),
        ],
        tenant: "t".repeat(MAX_TENANT_LEN),
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
        run_timeout_ms: 30_000,
        ..start(&r)
    };
    assert_eq!(h.apply(1, &Command::start_run(s)), ApplyOutcome::Accepted);
    let rec = h.run(&r);
    assert_eq!(rec.module_sha, Some([0xab; 32]));
    assert_eq!(
        rec.tags,
        vec![
            "env:prod".to_string(),
            String::new(),
            "x".repeat(MAX_RUN_TAG_LEN)
        ]
    );
    assert_eq!(rec.tenant.as_deref(), Some(&*"t".repeat(MAX_TENANT_LEN)));
    assert_eq!(
        rec.traceparent.as_deref(),
        Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    );
    assert_eq!(rec.run_timeout_ms, Some(30_000));
    assert_eq!(rec.completed_at_ms, None);

    // Empty / zero fields mean "none".
    let r2 = rid(0, 2);
    assert_eq!(
        h.apply(1, &Command::start_run(start(&r2))),
        ApplyOutcome::Accepted
    );
    let rec = h.run(&r2);
    assert_eq!(rec.module_sha, None);
    assert!(rec.tags.is_empty());
    assert_eq!(rec.tenant, None);
    assert_eq!(rec.traceparent, None);
    assert_eq!(rec.run_timeout_ms, None);

    // Exactly the limits are accepted.
    let r3 = rid(0, 3);
    let s = StartRun {
        tags: vec!["t".into(); MAX_RUN_TAGS],
        traceparent: "p".repeat(MAX_TRACEPARENT_LEN),
        ..start(&r3)
    };
    assert_eq!(h.apply(1, &Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r3).tags.len(), MAX_RUN_TAGS);
}

#[test]
fn start_run_new_fields_validated() {
    let mut h = H::new(0);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    let r = rid(0, 1);
    let before = h.digest();

    let cases: Vec<(StartRun, InvalidCode)> = vec![
        (
            StartRun {
                module_sha: vec![1; 31],
                ..start(&r)
            },
            InvalidCode::BadLength,
        ),
        (
            StartRun {
                module_sha: vec![1; 33],
                ..start(&r)
            },
            InvalidCode::BadLength,
        ),
        (
            StartRun {
                tags: vec!["t".into(); MAX_RUN_TAGS + 1],
                ..start(&r)
            },
            InvalidCode::FieldTooLong,
        ),
        (
            StartRun {
                tags: vec!["x".repeat(MAX_RUN_TAG_LEN + 1)],
                ..start(&r)
            },
            InvalidCode::FieldTooLong,
        ),
        (
            StartRun {
                tenant: "t".repeat(MAX_TENANT_LEN + 1),
                ..start(&r)
            },
            InvalidCode::FieldTooLong,
        ),
        (
            StartRun {
                traceparent: "p".repeat(MAX_TRACEPARENT_LEN + 1),
                ..start(&r)
            },
            InvalidCode::FieldTooLong,
        ),
    ];
    for (s, code) in cases {
        let (o, a, reads) = h.apply_full(1, &Command::start_run(s));
        assert_eq!(o, bad(code));
        assert!(a.is_empty());
        assert_eq!(reads, 0, "field validation happens before the point read");
    }
    assert!(h.get(Table::Runs, &run_key(&r)).is_none());
    // Only meta (applied offset, rolling hash) moved.
    assert_ne!(h.digest(), before);
    assert_eq!(h.all_running(), 0);
}

#[test]
fn complete_run_stores_completed_at() {
    let (mut h, r) = running(0, 1);
    assert_eq!(
        h.apply(
            1,
            &events(&r, (1, 1), 0, vec![ev(0)], vec![complete_at(4242)])
        ),
        ApplyOutcome::Accepted
    );
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(rec.completed_at_ms, Some(4242));
}

// ------------------------------------------------------------ Running keyspace

#[test]
fn running_row_lifecycle_grant_and_complete() {
    let mut h = H::new(0);
    let r = rid(0, 1);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(1, &Command::start_run(start(&r))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).status, RunStatus::Ready);
    assert_eq!(h.running_row(&r), None, "Ready runs are not indexed");

    let (o, _, reads) = h.apply_full(1, &grant(&r, 1, 1));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(
        reads, 1,
        "AttemptGranted: still one point read (Running row is blind)"
    );
    assert_eq!(h.running_row(&r), Some((1, 1)));

    // A later grant overwrites the row.
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), ApplyOutcome::Accepted);
    assert_eq!(h.running_row(&r), Some((1, 2)));
    assert_eq!(h.all_running(), 1);

    // CompleteRun removes it.
    let (o, _, reads) = h.apply_full(1, &events(&r, (1, 2), 0, vec![ev(0)], vec![complete_at(7)]));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(reads, 1);
    assert_eq!(h.running_row(&r), None);
    assert_eq!(h.all_running(), 0);
}

#[test]
fn running_row_removed_by_enqueue_ready() {
    let (mut h, r) = running(0, 1);
    assert_eq!(h.running_row(&r), Some((1, 1)));
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![ev(0)], vec![enqueue(500)])),
        ApplyOutcome::Accepted
    );
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert_eq!(h.running_row(&r), None);
    assert!(h.get(Table::Ready, &ready_key("q", 5, 500, &r)).is_some());

    // Re-granted: indexed again.
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), ApplyOutcome::Accepted);
    assert_eq!(h.running_row(&r), Some((1, 2)));
}

#[test]
fn delayed_and_rejected_starts_have_no_running_row() {
    let mut h = H::new(0);
    assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
    let r = rid(0, 1);
    let s = StartRun {
        ready_at_ms: 10_000,
        ..start(&r)
    };
    assert_eq!(h.apply(1, &Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).status, RunStatus::Scheduled);
    assert_eq!(h.running_row(&r), None);
    // Stale grant: rejected, still no row.
    assert_eq!(
        h.apply(1, &grant(&r, 0, 1)),
        rej(RejectReason::StaleEpoch { current: Epoch(1) })
    );
    assert_eq!(h.all_running(), 0);
}

// ---------------------------------------------------------------- Suspend

#[test]
fn suspend_moves_running_to_scheduled() {
    let (mut h, r) = running(0, 1);
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![ev(0), ev(1)], vec![])),
        ApplyOutcome::Accepted
    );

    // No events, matching from_seq.
    let (o, a, reads) = h.apply_full(1, &events(&r, (1, 1), 2, vec![], vec![Derived::suspend()]));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty(), "Suspend emits no action: {a:?}");
    assert_eq!(reads, 1, "Suspend adds no point read");
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Scheduled);
    assert_eq!(rec.ready, None);
    assert_eq!(rec.next_seq, 2);
    assert_eq!(
        rec.current_attempt,
        Some((1, 1)),
        "the token stays for fencing"
    );
    assert_eq!(h.running_row(&r), None);
    assert_eq!(h.all_running(), 0);
    assert!(
        h.store
            .reader()
            .prefix(Table::Ready, &[], usize::MAX)
            .expect("scan")
            .is_empty()
    );

    // A suspended run can be re-granted and runs again.
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).status, RunStatus::Running);
    assert_eq!(h.running_row(&r), Some((1, 2)));
}

#[test]
fn suspend_with_events_and_timer() {
    let (mut h, r) = running(0, 1);
    let sleep = intent(derived::Intent::ScheduleTimer(ScheduleTimer {
        op: 3,
        due_at_ms: 9_000,
    }));
    let (o, a, _) = h.apply_full(
        1,
        &events(&r, (1, 1), 0, vec![ev(0)], vec![sleep, Derived::suspend()]),
    );
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(
        a,
        vec![Action::ScheduleTimer {
            due_at_ms: 9_000,
            run: r,
            op: 3
        }]
    );
    assert!(h.get(Table::Events, &event_key(&r, 0)).is_some());
    assert!(h.get(Table::Timers, &timer_key(9_000, &r, 3)).is_some());
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Scheduled);
    assert_eq!(rec.next_seq, 1);
    assert_eq!(h.running_row(&r), None);

    // The timer fires: the Scheduled run is made Ready again.
    let fired = Command::timer_fired(TimerFired {
        run_id: r.as_bytes().to_vec(),
        op: 3,
        due_at_ms: 9_000,
        fired_at_ms: 9_001,
    });
    let (o, a, _) = h.apply_full(1, &fired);
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(
        a.iter()
            .any(|x| matches!(x, Action::EnqueueReady { run, .. } if *run == r)),
        "{a:?}"
    );
    assert_eq!(h.run(&r).status, RunStatus::Ready);
}

#[test]
fn suspend_seq_mismatch_is_rejected_and_state_unchanged() {
    let (mut h, r) = running(0, 1);
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![ev(0)], vec![])),
        ApplyOutcome::Accepted
    );
    let rows_before = h.store.reader().prefix(Table::Runs, &[], 10).expect("scan");
    let running_before = h.running_row(&r);
    for from in [0u64, 2, 5] {
        assert_eq!(
            h.apply(
                1,
                &events(&r, (1, 1), from, vec![], vec![Derived::suspend()])
            ),
            rej(RejectReason::SeqMismatch { expected: 1 })
        );
    }
    assert_eq!(
        h.store.reader().prefix(Table::Runs, &[], 10).expect("scan"),
        rows_before
    );
    assert_eq!(h.running_row(&r), running_before);
    assert_eq!(h.run(&r).status, RunStatus::Running);
}

#[test]
fn suspend_fenced_by_stale_token() {
    let (mut h, r) = running(0, 1);
    assert_eq!(h.apply(1, &grant(&r, 1, 2)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![], vec![Derived::suspend()])),
        rej(RejectReason::Fenced {
            current: Some(AttemptToken {
                epoch: Epoch(1),
                n: 2
            })
        })
    );
    assert_eq!(h.run(&r).status, RunStatus::Running);
    assert_eq!(h.running_row(&r), Some((1, 2)));
}

#[test]
fn suspend_structure_is_validated() {
    let (mut h, r) = running(0, 1);
    let cases = vec![
        vec![Derived::suspend(), complete_at(1)],
        vec![complete_at(1), Derived::suspend()],
        vec![Derived::suspend(), enqueue(1)],
        vec![enqueue(1), Derived::suspend()],
        vec![Derived::suspend(), Derived::suspend()],
        vec![
            Derived::suspend(),
            intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                op: 1,
                due_at_ms: 1,
            })),
        ],
    ];
    for d in cases {
        assert_eq!(
            h.apply(1, &events(&r, (1, 1), 0, vec![], d.clone())),
            bad(InvalidCode::BadIntent),
            "{d:?}"
        );
    }
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Running);
    assert_eq!(rec.next_seq, 0);
    assert_eq!(h.running_row(&r), Some((1, 1)));
}

#[test]
fn suspend_requires_running_status() {
    let (mut h, r) = running(0, 1);
    // Yield: Ready with the old token still current.
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![], vec![enqueue(200)])),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        h.apply(1, &events(&r, (1, 1), 0, vec![], vec![Derived::suspend()])),
        bad(InvalidCode::NotRunning)
    );
    let rec = h.run(&r);
    assert_eq!(rec.status, RunStatus::Ready);
    assert!(h.get(Table::Ready, &ready_key("q", 5, 200, &r)).is_some());
}

// ---------------------------------------------------------- InputAppended

#[test]
fn input_appended_action_exists() {
    let r = rid(0, 1);
    let a = Action::InputAppended { run: r, seq: 7 };
    match a {
        Action::InputAppended { run, seq } => {
            assert_eq!(run, r);
            assert_eq!(seq, 7);
        }
        other => panic!("unexpected {other:?}"),
    }
}

// --------------------------------------------- determinism of the new paths

#[test]
fn same_commands_same_digest() {
    fn drive() -> [u8; 32] {
        let (mut h, r) = running(0, 1);
        let r2 = rid(0, 2);
        let s = StartRun {
            module_sha: vec![9; MODULE_SHA_LEN],
            tags: vec!["a".into(), "b".into()],
            tenant: "acme".into(),
            traceparent: "tp".into(),
            run_timeout_ms: 5,
            ..start(&r2)
        };
        assert_eq!(h.apply(1, &Command::start_run(s)), ApplyOutcome::Accepted);
        assert_eq!(h.apply(1, &grant(&r2, 1, 1)), ApplyOutcome::Accepted);
        assert_eq!(
            h.apply(
                1,
                &events(&r, (1, 1), 0, vec![ev(0)], vec![Derived::suspend()])
            ),
            ApplyOutcome::Accepted
        );
        assert_eq!(
            h.apply(1, &events(&r2, (1, 1), 0, vec![], vec![complete_at(77)])),
            ApplyOutcome::Accepted
        );
        h.digest()
    }
    assert_eq!(drive(), drive());
}

// ------------------------------------------------- query helpers (Task 2)

mod query_helpers {
    use super::*;
    use ostro_partition::query::{
        PAYLOAD_INPUT, PAYLOAD_RESULT, ReadyEntryRow, journal_rows, ready_entries, run_payload,
        run_record, running_runs, runs_page,
    };

    #[test]
    fn running_runs_lists_in_flight_runs_with_tokens() {
        let mut h = H::new(0);
        assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
        let ids: Vec<RunId> = (1..=4).map(|n| rid(0, n)).collect();
        for r in &ids {
            assert_eq!(
                h.apply(1, &Command::start_run(start(r))),
                ApplyOutcome::Accepted
            );
        }
        assert!(running_runs(&h.store.reader()).expect("scan").is_empty());
        assert_eq!(h.apply(1, &grant(&ids[0], 1, 1)), ApplyOutcome::Accepted);
        assert_eq!(h.apply(1, &grant(&ids[1], 1, 1)), ApplyOutcome::Accepted);
        assert_eq!(h.apply(1, &grant(&ids[1], 1, 4)), ApplyOutcome::Accepted);
        assert_eq!(h.apply(1, &grant(&ids[2], 1, 1)), ApplyOutcome::Accepted);
        // ids[2] suspends; ids[3] is never granted.
        assert_eq!(
            h.apply(
                1,
                &events(&ids[2], (1, 1), 0, vec![], vec![Derived::suspend()])
            ),
            ApplyOutcome::Accepted
        );
        let mut got = running_runs(&h.store.reader()).expect("scan");
        got.sort();
        let tok = |n| AttemptToken { epoch: Epoch(1), n };
        let mut want = vec![(ids[0], tok(1)), (ids[1], tok(4))];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn ready_entries_decodes_all_queues() {
        let mut h = H::new(0);
        assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
        let a = rid(0, 1);
        let b = rid(0, 2);
        let c = rid(0, 3);
        let sa = StartRun {
            queue: "alpha".into(),
            priority: 1,
            concurrency_key: "ka".into(),
            ..start(&a)
        };
        let sb = StartRun {
            queue: "beta".into(),
            priority: -3,
            concurrency_key: String::new(),
            ..start(&b)
        };
        let sc = StartRun {
            queue: "alpha".into(),
            priority: 9,
            concurrency_key: "kc".into(),
            ..start(&c)
        };
        for s in [sa, sb, sc] {
            assert_eq!(h.apply(1, &Command::start_run(s)), ApplyOutcome::Accepted);
        }
        let rows = ready_entries(&h.store.reader(), 100).expect("scan");
        let row = |queue: &str, priority, run, ck: &str| ReadyEntryRow {
            queue: queue.into(),
            priority,
            ready_at_ms: 100,
            run,
            concurrency_key: ck.into(),
        };
        // Key order: queue key (u16 length prefix first, so "beta" < "alpha"), then higher
        // priority first.
        assert_eq!(
            rows,
            vec![
                row("beta", -3, b, ""),
                row("alpha", 9, c, "kc"),
                row("alpha", 1, a, "ka"),
            ]
        );
        assert_eq!(ready_entries(&h.store.reader(), 1).expect("scan").len(), 1);

        // Granting removes the ready row.
        assert_eq!(h.apply(1, &grant(&c, 1, 1)), ApplyOutcome::Accepted);
        assert_eq!(
            ready_entries(&h.store.reader(), 100).expect("scan").len(),
            2
        );
    }

    #[test]
    fn journal_rows_from_seq_and_limit() {
        let (mut h, r) = running(0, 1);
        let other = rid(0, 2);
        assert_eq!(
            h.apply(1, &Command::start_run(start(&other))),
            ApplyOutcome::Accepted
        );
        assert_eq!(h.apply(1, &grant(&other, 1, 1)), ApplyOutcome::Accepted);
        let evs = |from: u64, n: u64| -> Vec<JournalEvent> {
            (from..from + n)
                .map(|seq| JournalEvent {
                    seq,
                    kind: 16 + seq as u32,
                    payload: Some(PayloadRef::inline(Bytes::from(vec![seq as u8]))),
                })
                .collect()
        };
        assert_eq!(
            h.apply(1, &events(&r, (1, 1), 0, evs(0, 5), vec![])),
            ApplyOutcome::Accepted
        );
        assert_eq!(
            h.apply(1, &events(&other, (1, 1), 0, evs(0, 3), vec![])),
            ApplyOutcome::Accepted
        );
        let reader = h.store.reader();
        let all = journal_rows(&reader, &r, 0, 100).expect("journal");
        assert_eq!(all.len(), 5, "only this run's rows");
        for (i, (seq, kind, p)) in all.iter().enumerate() {
            assert_eq!(*seq, i as u64);
            assert_eq!(*kind, 16 + i as u32);
            assert_eq!(*p, PayloadRef::inline(Bytes::from(vec![i as u8])));
        }
        let tail = journal_rows(&reader, &r, 3, 100).expect("journal");
        assert_eq!(tail.iter().map(|x| x.0).collect::<Vec<_>>(), vec![3, 4]);
        let page = journal_rows(&reader, &r, 1, 2).expect("journal");
        assert_eq!(page.iter().map(|x| x.0).collect::<Vec<_>>(), vec![1, 2]);
        assert!(
            journal_rows(&reader, &r, 5, 100)
                .expect("journal")
                .is_empty()
        );
        assert!(
            journal_rows(&reader, &rid(0, 9), 0, 100)
                .expect("journal")
                .is_empty()
        );
    }

    #[test]
    fn run_record_and_payloads() {
        let (mut h, r) = running(0, 1);
        let reader = h.store.reader();
        let rec = run_record(&reader, &r).expect("read").expect("run");
        assert_eq!(rec.status, RunStatus::Running);
        assert!(run_record(&reader, &rid(0, 9)).expect("read").is_none());
        assert_eq!(
            run_payload(&reader, &r, PAYLOAD_INPUT).expect("read"),
            inline(b"in")
        );
        assert_eq!(
            run_payload(&reader, &r, PAYLOAD_RESULT).expect("read"),
            None
        );
        assert_eq!(
            h.apply(1, &events(&r, (1, 1), 0, vec![], vec![complete_at(5)])),
            ApplyOutcome::Accepted
        );
        assert_eq!(
            run_payload(&h.store.reader(), &r, PAYLOAD_RESULT).expect("read"),
            inline(b"out")
        );
    }

    #[test]
    fn runs_page_walks_key_order() {
        let mut h = H::new(3);
        assert_eq!(h.apply(1, &la(1)), ApplyOutcome::Accepted);
        let mut ids: Vec<RunId> = (1..=7).map(|n| rid(3, n)).collect();
        for r in &ids {
            assert_eq!(
                h.apply(1, &Command::start_run(start(r))),
                ApplyOutcome::Accepted
            );
        }
        ids.sort();
        let reader = h.store.reader();
        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let page = runs_page(&reader, after, 3).expect("page");
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 3);
            for (id, rec) in &page {
                assert_eq!(rec.workflow, "wf");
                seen.push(*id);
            }
            after = page.last().map(|(id, _)| *id);
        }
        assert_eq!(seen, ids);
        assert!(
            runs_page(&reader, Some(ids[6]), 10)
                .expect("page")
                .is_empty()
        );
        assert_eq!(
            runs_page(&reader, Some(ids[0]), 1).expect("page")[0].0,
            ids[1]
        );
    }
}
