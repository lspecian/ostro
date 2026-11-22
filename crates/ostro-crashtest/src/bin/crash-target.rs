//! `crash-target <data_dir> <seed> [--partitions N] [--pipeline N] [--blob-pct N]
//! [--journal-mib N] [--memtable-mib N] [--workers N] [--cache-mib N] [--snapshot-ms N]
//! [--snapshot-bytes N] [--retention-ms N] [--segment-bytes N]`: a Leader-mode node
//! under seeded load, killed with SIGKILL by tests/crash_loop.rs (plan 02-15) and
//! tests/reopen_time.rs (plan 02-16).
//!
//! Defaults: 4 partitions, one load task per partition, 2% of starts with a blob input,
//! `StoreConfig::default()`. `--pipeline N` runs N independent load tasks per partition
//! (N submits in flight, so group commit batches them); the store flags override the
//! matching `StoreConfig` field (MiB for sizes). Plan 08-09: `--snapshot-ms` / `--snapshot-bytes`
//! set the snapshot cadence (interval and bytes threshold, `min_interval` 0), `--retention-ms`
//! the retention pass interval and `--segment-bytes` the WAL segment rotation size, so a kill
//! lands during snapshot writes and trims.
//!
//! Prints `READY` once the node is open, then one line per command whose outcome was
//! `Accepted`, only after that outcome arrived (WAL fsync + apply commit):
//! `ACK <p> <offset> <blake3(cmd bytes) hex>` plus ` RUN <run_id>` for a `StartRun`.
//! A partition whose submit fails prints `FAILED <p> <reason>` and stops (fail-stop; with
//! failpoints this is expected). stdout is flushed after every line.
//!
//! Load per partition (one task each, seeded by `seed` and the partition): batches of
//! `StartRun` (about half with an idempotency key from a small space, so `Duplicate`s
//! happen; some with > 256 KiB inputs written to the blob store via `prepare_payload`),
//! `AttemptGranted`, and `EventsAppended` carrying `ScheduleTimer` / cross-partition
//! `SendMessage` signals / `CompleteRun` intents. The leader services (shuffler, timers)
//! run as in production.

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ostro_crashtest::{Rng, ack_line};
use ostro_partition::{
    ApplyOutcome, Node, NodeConfig, PartitionHandle, PartitionSlot, StoreConfig,
};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EventsAppended,
    JournalEvent, OutboxMsg, ScheduleTimer, SendMessage, Signal, StartRun, TerminalStatus, derived,
    encode_command, outbox_msg,
};
use ostro_types::{Command, PartitionCount, PartitionId, PayloadRef, RunId};

/// A run this process started.
#[derive(Clone, Debug)]
struct Live {
    id: RunId,
    /// Granted attempt number under the current epoch.
    n: Option<u32>,
    next_seq: u64,
    done: bool,
}

/// Runs started by this process, per partition (signal targets for other partitions).
type Shared = Arc<Mutex<Vec<Vec<RunId>>>>;

fn emit(line: &str) {
    let mut out = std::io::stdout().lock();
    // A closed pipe means the harness is gone: exit quietly.
    if writeln!(out, "{line}").and_then(|()| out.flush()).is_err() {
        std::process::exit(0);
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: crash-target <data_dir> <seed> [--partitions N] [--pipeline N] [--blob-pct N] \
         [--journal-mib N] [--memtable-mib N] [--workers N] [--cache-mib N] [--snapshot-ms N] \
         [--snapshot-bytes N] [--retention-ms N] [--segment-bytes N]"
    );
    std::process::exit(2);
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn inline(b: &'static [u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from_static(b)))
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn main() {
    #[cfg(feature = "failpoints")]
    let _scenario = fail::FailScenario::setup();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        usage();
    }
    let dir = PathBuf::from(&args[0]);
    let seed: u64 = args[1].parse().unwrap_or_else(|_| usage());
    let mut partitions: u16 = 4;
    let mut pipeline: u64 = 1;
    let mut blob_pct: u64 = 2;
    let mut store = StoreConfig::default();
    let mut snapshot_ms: Option<u64> = None;
    let mut snapshot_bytes: Option<u64> = None;
    let mut retention_ms: Option<u64> = None;
    let mut segment_bytes: Option<u64> = None;
    let mut rest = args[2..].iter();
    while let Some(a) = rest.next() {
        let v: u64 = rest
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| usage());
        match a.as_str() {
            "--partitions" => partitions = u16::try_from(v).unwrap_or_else(|_| usage()),
            "--pipeline" => pipeline = v.max(1),
            "--blob-pct" => blob_pct = v.min(100),
            "--journal-mib" => store.max_journaling_size = v << 20,
            "--memtable-mib" => store.max_memtable_size = v << 20,
            "--workers" => store.worker_threads = usize::try_from(v.max(1)).unwrap_or(1),
            "--cache-mib" => store.cache_size = v << 20,
            "--snapshot-ms" => snapshot_ms = Some(v.max(1)),
            "--snapshot-bytes" => snapshot_bytes = Some(v),
            "--retention-ms" => retention_ms = Some(v.max(1)),
            "--segment-bytes" => segment_bytes = Some(v.max(1024)),
            _ => usage(),
        }
    }
    let count = PartitionCount::new(partitions).unwrap_or_else(|| usage());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        let mut cfg = NodeConfig::new(dir, count);
        cfg.store = store;
        if let Some(ms) = snapshot_ms {
            cfg.snapshots.interval = std::time::Duration::from_millis(ms);
            cfg.snapshots.min_interval = std::time::Duration::ZERO;
        }
        if let Some(b) = snapshot_bytes {
            cfg.snapshots.bytes_threshold = b;
            cfg.snapshots.min_interval = std::time::Duration::ZERO;
        }
        if let Some(ms) = retention_ms {
            cfg.leader.retention.interval = std::time::Duration::from_millis(ms);
        }
        if let Some(b) = segment_bytes {
            cfg.wal.enable_rotation = true;
            cfg.wal.rotation_size_bytes = b;
        }
        let node = match Node::open(cfg).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("crash-target: node open failed: {e}");
                std::process::exit(1);
            }
        };
        for s in node.slots() {
            if let PartitionSlot::Failed { partition, reason } = s {
                emit(&format!("FAILED {} startup: {reason}", partition.0));
            }
        }
        emit("READY");
        let shared: Shared = Arc::new(Mutex::new(vec![Vec::new(); usize::from(partitions)]));
        let mut tasks = Vec::new();
        for p in count.ids() {
            if let Some(h) = node.handle(p) {
                for k in 0..pipeline {
                    let rng = Rng::new(
                        seed.wrapping_mul(1_000_003)
                            .wrapping_add(u64::from(p.0))
                            .wrapping_add(k << 32),
                    );
                    tasks.push(tokio::spawn(load(
                        h.clone(),
                        partitions,
                        rng,
                        Arc::clone(&shared),
                        blob_pct,
                    )));
                }
            }
        }
        for t in tasks {
            let _ = t.await;
        }
        // every partition failed: idle until killed (the harness owns the lifetime)
        std::future::pending::<()>().await;
        drop(node);
    });
}

/// Submit `cmds`; print an ACK for every `Accepted` outcome. `runs[i]` is the run id of
/// `cmds[i]` when it is a `StartRun`. Returns the outcomes, or `None` after a failure.
async fn submit(
    h: &PartitionHandle<ostro_log::Rf1Log>,
    cmds: Vec<Command>,
    runs: &[Option<RunId>],
) -> Option<Vec<ApplyOutcome>> {
    let p = h.partition().0;
    let bytes: Vec<Bytes> = cmds.iter().map(encode_command).collect();
    match h.submit_with_offsets(cmds).await {
        Ok(v) => {
            let mut out = Vec::with_capacity(v.len());
            for (i, (off, o)) in v.into_iter().enumerate() {
                if o == ApplyOutcome::Accepted {
                    let run = runs.get(i).and_then(Option::as_ref);
                    emit(&ack_line(p, off, &bytes[i], run));
                }
                out.push(o);
            }
            Some(out)
        }
        Err(e) => {
            emit(&format!("FAILED {p} {e}"));
            None
        }
    }
}

async fn load(
    h: PartitionHandle<ostro_log::Rf1Log>,
    count: u16,
    mut rng: Rng,
    shared: Shared,
    blob_pct: u64,
) {
    let me = h.partition();
    let mut live: Vec<Live> = Vec::new();
    loop {
        let roll = rng.below(100);
        if live.is_empty() || roll < 35 {
            // a batch of 1..=8 starts
            let k = rng.range(1, 9) as usize;
            let mut cmds = Vec::with_capacity(k);
            let mut ids = Vec::with_capacity(k);
            for _ in 0..k {
                let id = RunId::new_v7(me);
                let input = if rng.chance(blob_pct) {
                    let big = Bytes::from(rng.bytes(300 * 1024));
                    match h.prepare_payload(big).await {
                        Ok(r) => Some(r),
                        Err(e) => {
                            emit(&format!("FAILED {} blob: {e}", me.0));
                            return;
                        }
                    }
                } else {
                    inline(b"in")
                };
                let idempotency_key = if rng.chance(50) {
                    format!("k{}", rng.below(5000)).into_bytes()
                } else {
                    Vec::new()
                };
                cmds.push(Command::start_run(StartRun {
                    run_id: id.as_bytes().to_vec(),
                    workflow: "wf".into(),
                    workflow_version: 1,
                    queue: "q".into(),
                    input,
                    idempotency_key,
                    conflict_policy: ConflictPolicy::ReturnExisting as i32,
                    concurrency_key: String::new(),
                    priority: 0,
                    ready_at_ms: 0,
                    requested_at_ms: now_ms(),
                    parent: None,
                    rng_seed: vec![7; 32],
                    ..Default::default()
                }));
                ids.push(Some(id));
            }
            let Some(out) = submit(&h, cmds, &ids).await else {
                return;
            };
            for (o, id) in out.into_iter().zip(ids) {
                if o == ApplyOutcome::Accepted
                    && let Some(id) = id
                {
                    live.push(Live {
                        id,
                        n: None,
                        next_seq: 0,
                        done: false,
                    });
                    if let Ok(mut s) = shared.lock() {
                        let v = &mut s[usize::from(me.0)];
                        v.push(id);
                        if v.len() > 4096 {
                            v.drain(..2048);
                        }
                    }
                }
            }
            if live.len() > 2048 {
                live.drain(..1024);
            }
            continue;
        }
        let i = rng.below(live.len() as u64) as usize;
        if live[i].done {
            continue;
        }
        let run = live[i].clone();
        match run.n {
            None => {
                // AttemptGranted n = 1 under this process's epoch
                let cmd = Command::attempt_granted(AttemptGranted {
                    run_id: run.id.as_bytes().to_vec(),
                    token: Some(AttemptTokenMsg {
                        epoch: h.epoch().0,
                        n: 1,
                    }),
                    after_failure: false,
                });
                let Some(out) = submit(&h, vec![cmd], &[]).await else {
                    return;
                };
                if out[0] == ApplyOutcome::Accepted {
                    live[i].n = Some(1);
                } else {
                    live[i].done = true;
                }
            }
            Some(n) => {
                let mut d = Vec::new();
                if rng.chance(40) {
                    // due now-ish or in the past: the timer service fires it
                    d.push(intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                        op: run.next_seq + 1,
                        due_at_ms: now_ms().saturating_sub(1000) + rng.below(2000),
                    })));
                }
                if count > 1 && rng.chance(50) {
                    let q = (me.0 + 1 + rng.below(u64::from(count - 1)) as u16) % count;
                    let target = shared.lock().ok().and_then(|s| {
                        let v = &s[usize::from(q)];
                        (!v.is_empty()).then(|| v[rng.below(v.len() as u64) as usize])
                    });
                    let target = target.unwrap_or_else(|| RunId::new_v7(PartitionId(q)));
                    d.push(intent(derived::Intent::SendMessage(SendMessage {
                        msg: Some(OutboxMsg {
                            kind: Some(outbox_msg::Kind::Signal(Signal {
                                target_run: target.as_bytes().to_vec(),
                                name: format!("s{}", rng.below(4)),
                                payload: inline(b"p"),
                            })),
                        }),
                    })));
                }
                let complete = rng.chance(15);
                if complete {
                    d.push(intent(derived::Intent::CompleteRun(CompleteRun {
                        status: TerminalStatus::Completed as i32,
                        result: inline(b"out"),
                        completed_at_ms: now_ms(),
                        resumable: false,
                        parent_event: None,
                    })));
                }
                let cmd = Command::events_appended(EventsAppended {
                    run_id: run.id.as_bytes().to_vec(),
                    token: Some(AttemptTokenMsg {
                        epoch: h.epoch().0,
                        n,
                    }),
                    from_seq: run.next_seq,
                    events: vec![JournalEvent {
                        seq: run.next_seq,
                        kind: 16,
                        payload: inline(b"e"),
                        ..Default::default()
                    }],
                    derived: d,
                });
                let Some(out) = submit(&h, vec![cmd], &[]).await else {
                    return;
                };
                if out[0] == ApplyOutcome::Accepted {
                    live[i].next_seq += 1;
                    live[i].done = complete;
                } else {
                    live[i].done = true;
                }
            }
        }
    }
}
