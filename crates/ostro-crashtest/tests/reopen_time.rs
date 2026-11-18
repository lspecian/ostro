//! Reopen-time measurements (plan 02-16). `#[ignore]`: slow, spawns processes, and the numbers
//! depend on the machine. They print markdown tables; the only assertions are sanity bounds
//! (a reopen under 30 s), so a slow machine never fails CI. Run on a quiet machine:
//!
//! ```text
//! cargo test --release -p ostro-crashtest --test reopen_time -- --ignored --nocapture --test-threads=1
//! ```
//!
//! - `reopen_time_store_variants`: for each `StoreConfig` variant, run `crash-target` (P=4,
//!   `--pipeline 64`, no blobs) long enough for the fjall journals to reach their cap, SIGKILL
//!   it, then time `PartitionStore::open` per partition (fjall journal replay), the WAL
//!   recovery scan per partition, and a full Leader-mode `Node::open` until every slot is
//!   Ready. Env: `OSTRO_REOPEN_RUN_SECS` (default 20), `OSTRO_REOPEN_PIPELINE` (default 64).
//! - `reopen_time_segments`: many short kill -9 cycles on one data directory (every process
//!   start opens a fresh WAL segment per partition), timing the Inspect-mode reopen as
//!   segments accumulate, with a breakdown of reading every segment vs. fsyncing every
//!   segment vs. fsyncing only the last one. Env: `OSTRO_REOPEN_RESTARTS` (default 200).

use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

use ostro_crashtest::{
    Rng, copy_dir, dir_bytes, loadavg, non_empty_segments, open_node, run_and_kill,
    run_and_kill_with,
};
use ostro_log::ReplicatedLog;
use ostro_log::recovery::{list_segments, recover_partition, segment_dir};
use ostro_partition::{Node, NodeConfig, PartitionSlot, PartitionStore, StartMode, StoreConfig};
use ostro_types::{Offset, PartitionCount, PartitionId};

const P: u16 = 4;
const SANITY: Duration = Duration::from_secs(30);

fn bin() -> &'static Path {
    Path::new(env!("CARGO_BIN_EXE_crash-target"))
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1000.0)
}

fn load() -> String {
    loadavg().map_or_else(|| "?".into(), |l| format!("{l:.2}"))
}

fn count() -> PartitionCount {
    PartitionCount::new(P).expect("P > 0")
}

/// `(journal files, allocated bytes)` of one partition's fjall journals (`*.jnl`). fjall
/// preallocates every journal file to 64 MiB (`PRE_ALLOCATED_BYTES`), so the file length says
/// nothing; allocated blocks approximate the bytes actually written.
fn journal_bytes(data: &Path, p: u16) -> (usize, u64) {
    use std::os::unix::fs::MetadataExt as _;
    let dir = data.join("state").join(format!("p{p:04}"));
    let Ok(rd) = std::fs::read_dir(dir) else {
        return (0, 0);
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().ends_with(".jnl"))
        .filter_map(|e| e.metadata().ok())
        .fold((0, 0), |(n, b), m| (n + 1, b + m.blocks() * 512))
}

fn p50_max(mut v: Vec<Duration>) -> (Duration, Duration) {
    v.sort_unstable();
    (v[v.len() / 2], *v.last().expect("non-empty"))
}

struct Variant {
    name: &'static str,
    args: Vec<String>,
    cfg: StoreConfig,
}

fn variants() -> Vec<Variant> {
    let d = StoreConfig::default();
    let mib = |v: u64| v.to_string();
    vec![
        Variant {
            name: "default",
            args: vec![],
            cfg: d.clone(),
        },
        Variant {
            name: "journal 128 MiB",
            args: vec!["--journal-mib".into(), mib(128)],
            cfg: StoreConfig {
                max_journaling_size: 128 << 20,
                ..d.clone()
            },
        },
        Variant {
            name: "memtable 32 MiB",
            args: vec!["--memtable-mib".into(), mib(32)],
            cfg: StoreConfig {
                max_memtable_size: 32 << 20,
                ..d.clone()
            },
        },
        Variant {
            name: "workers 2",
            args: vec!["--workers".into(), mib(2)],
            cfg: StoreConfig {
                worker_threads: 2,
                ..d
            },
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement: spawns crash-target for ~20 s per variant; run with --ignored"]
async fn reopen_time_store_variants() {
    let run_for = Duration::from_secs(env_u64("OSTRO_REOPEN_RUN_SECS", 20));
    let pipeline = env_u64("OSTRO_REOPEN_PIPELINE", 64);
    let mut rows = Vec::new();
    for (i, v) in variants().into_iter().enumerate() {
        let before = load();
        let t = ostro_testkit::fs::tempdir();
        let data = t.path().join("data");
        let mut args = vec![
            "--pipeline".to_string(),
            pipeline.to_string(),
            "--blob-pct".into(),
            "0".into(),
        ];
        args.extend(v.args.iter().cloned());
        let run = run_and_kill_with(bin(), &data, 0x0217 + i as u64, P, &args, &[], run_for)
            .unwrap_or_else(|e| panic!("{}: {e}", v.name));
        assert!(run.failed.is_empty(), "{}: failed {:?}", v.name, run.failed);

        // Two copies taken right after the kill: (a) store opens + WAL scans, (b) Node::open.
        let a = t.path().join("a");
        let b = t.path().join("b");
        copy_dir(&data, &a).expect("copy a");
        copy_dir(&data, &b).expect("copy b");
        let jnl_files: usize = (0..P).map(|p| journal_bytes(&a, p).0).max().unwrap_or(0);
        let jnl: Vec<u64> = (0..P).map(|p| journal_bytes(&a, p).1).collect();
        let state_bytes = dir_bytes(&a.join("state")) / u64::from(P);

        let mut store_open = Vec::new();
        let mut floors = Vec::new();
        for p in count().ids() {
            let t0 = Instant::now();
            let (store, meta) =
                PartitionStore::open(&a, p, count(), &v.cfg).expect("store opens after kill -9");
            store_open.push(t0.elapsed());
            floors.push(meta.applied);
            drop(store);
        }
        let mut wal_scan = Vec::new();
        let applied: u64 = floors.iter().map(|o| o.0).sum();
        for p in count().ids() {
            let t0 = Instant::now();
            recover_partition(&a.join("wal"), p, floors[p.index()]).expect("wal recovers");
            wal_scan.push(t0.elapsed());
        }

        let mut cfg = NodeConfig::new(b.clone(), count());
        cfg.store = v.cfg.clone();
        let t0 = Instant::now();
        let node = tokio::time::timeout(SANITY * 2, Node::open(cfg))
            .await
            .expect("node open within sanity bound")
            .expect("node opens");
        let node_open = t0.elapsed();
        for s in node.slots() {
            assert!(
                matches!(s, PartitionSlot::Ready(_)),
                "{}: slot not ready: {s:?}",
                v.name
            );
        }
        node.shutdown().await.expect("shutdown");

        let (so50, somax) = p50_max(store_open);
        let (ws50, wsmax) = p50_max(wal_scan);
        assert!(
            somax < SANITY && node_open < SANITY,
            "{}: reopen > 30 s",
            v.name
        );
        rows.push(format!(
            "| {} | {} | {} | {} | {:.1} / {:.1} | {:.1} | {} / {} | {} / {} | {} | {} -> {} |",
            v.name,
            run.acks.len(),
            applied,
            jnl_files,
            *jnl.iter().min().unwrap_or(&0) as f64 / f64::from(1u32 << 20),
            *jnl.iter().max().unwrap_or(&0) as f64 / f64::from(1u32 << 20),
            state_bytes as f64 / f64::from(1u32 << 20),
            ms(so50),
            ms(somax),
            ms(ws50),
            ms(wsmax),
            ms(node_open),
            before,
            load(),
        ));
    }
    println!();
    println!(
        "P={P}, crash-target --pipeline {pipeline} --blob-pct 0, SIGKILL after {} s",
        run_for.as_secs()
    );
    println!(
        "| variant | acks | applied (sum) | journal files/partition (max) | journal MiB written/partition (min / max) | state MiB/partition | store open ms (p50 / max) | WAL scan ms (p50 / max) | Node::open ms (Leader, all Ready) | loadavg before -> after |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for r in rows {
        println!("{r}");
    }
}

/// `(read every segment, fsync every non-empty segment, fsync only the last non-empty)`.
fn segment_costs(data: &Path, p: u16) -> (Duration, Duration, Duration) {
    let segs = non_empty_segments(data, p).expect("segments");
    let t0 = Instant::now();
    for s in &segs {
        std::fs::read(s).expect("read segment");
    }
    let read = t0.elapsed();
    let t0 = Instant::now();
    for s in &segs {
        File::open(s).and_then(|f| f.sync_all()).expect("fsync");
    }
    let all = t0.elapsed();
    let t0 = Instant::now();
    if let Some(s) = segs.last() {
        File::open(s).and_then(|f| f.sync_all()).expect("fsync");
    }
    (read, all, t0.elapsed())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement: hundreds of crash-target restarts; run with --ignored"]
async fn reopen_time_segments() {
    let restarts = env_u64("OSTRO_REOPEN_RESTARTS", 200).max(1);
    let t = ostro_testkit::fs::tempdir();
    let data = t.path().join("data");
    let mut rng = Rng::new(0x5e67);
    let marks: Vec<u64> = [
        0,
        restarts / 4,
        restarts / 2,
        3 * restarts / 4,
        restarts - 1,
    ]
    .into_iter()
    .collect();
    let mut rows = Vec::new();
    let mut max_open = Duration::ZERO;
    let start_load = load();
    for i in 0..restarts {
        let run_for = Duration::from_millis(rng.range(5, 300));
        let run = run_and_kill(bin(), &data, i, P, &[], run_for)
            .unwrap_or_else(|e| panic!("restart {i}: {e}"));
        assert!(
            run.failed.is_empty(),
            "restart {i}: failed {:?}",
            run.failed
        );
        if !marks.contains(&i) {
            continue;
        }
        // The realistic number first (dirty tail still in the page cache), as in 02-15.
        let t0 = Instant::now();
        let node = open_node(&data, P, StartMode::Inspect)
            .await
            .unwrap_or_else(|e| panic!("restart {i}: {e}"));
        let open = t0.elapsed();
        max_open = max_open.max(open);
        let mut leo = 0u64;
        for p in 0..P {
            let h = node.handle(PartitionId(p)).expect("ready");
            leo += h.log().log_end().0;
        }
        node.shutdown().await.expect("shutdown");

        let mut recover = Vec::new();
        let (mut read, mut all, mut last) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let (mut files, mut non_empty) = (0usize, 0usize);
        for p in count().ids() {
            let t0 = Instant::now();
            recover_partition(&data.join("wal"), p, Offset(u64::MAX)).expect("recover");
            recover.push(t0.elapsed());
            let (r, a, l) = segment_costs(&data, p.0);
            read += r;
            all += a;
            last += l;
            files += list_segments(&segment_dir(&data.join("wal"), p), p)
                .expect("list")
                .len();
            non_empty += non_empty_segments(&data, p.0).expect("segments").len();
        }
        let (_, rmax) = p50_max(recover);
        rows.push(format!(
            "| {} | {} / {} | {} | {} | {} | {} | {} | {} | {} |",
            i + 1,
            non_empty / usize::from(P),
            files / usize::from(P),
            leo / u64::from(P),
            ms(open),
            ms(run.startup),
            ms(rmax),
            ms(read),
            ms(all),
            ms(last),
        ));
        println!("{}", rows.last().expect("row"));
    }
    assert!(max_open < SANITY, "Inspect reopen took {max_open:?}");
    println!();
    println!(
        "P={P}, {restarts} kill -9 cycles (5-300 ms of load each), loadavg {start_load} -> {}",
        load()
    );
    println!(
        "| restarts | segments/partition (non-empty / files) | offsets/partition | Inspect Node::open ms | target startup ms | recover_partition ms (max p, warm) | read all segs ms (sum P) | fsync all segs ms (sum P) | fsync last seg ms (sum P) |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for r in rows {
        println!("{r}");
    }
}
