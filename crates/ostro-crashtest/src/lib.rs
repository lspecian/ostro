//! ostro-crashtest: Phase 2 crash/corruption harness (plan 02-15).
//!
//! - `crash-target` (src/bin): a Leader-mode node under seeded load that prints one
//!   `ACK <p> <offset> <blake3 hex>[ RUN <run_id>]` line per command whose outcome was
//!   `Accepted`, only after the outcome arrived (i.e. after WAL fsync + apply commit).
//! - this library: the helpers the crash loops use to check a data directory after a
//!   `kill -9`: ACK parsing, a verified log scan (offset -> command hash), ACK/run
//!   verification, and rebuild-and-compare (live state vs. state rebuilt from the log).
//!
//! `kill -9` never produces torn writes (the page cache survives a process kill), so torn
//! tails and bit flips are injected offline between runs (02-RESEARCH Pitfall 2).
//!
//! Plan 05-19 (QA-01) adds the whole-server kill campaign against the real failpoint-enabled
//! `ostro` binary: [`server_target`] (spawn / kill -9 / restart), [`workload`] (crash-saga
//! load over HTTP + fsynced ack log), [`check`] (lost-ack / duplicate-journal / result
//! checker, negative controls in `tests/check_controls.rs`).

pub mod check;
pub mod server_target;
pub mod workload;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command as Proc, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ostro_log::ReplicatedLog;
use ostro_log::ostro_wal::truncate::declared_record_size;
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_partition::{Node, NodeConfig, PartitionSlot, StartMode};
use ostro_types::{Offset, PartitionCount, PartitionId, RunId};

/// Generous limit for node opens and target startup.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(60);

// ------------------------------------------------------------------------------ PRNG

/// Tiny seeded xorshift64* PRNG (no `rand` dependency).
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // splitmix the seed so small seeds (0, 1, 2, ...) diverge immediately
        let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        Self(if z == 0 { 1 } else { z })
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `0..n` (`n > 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// Uniform in `lo..hi` (`lo < hi`).
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo)
    }

    /// True with probability `pct` percent.
    pub fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next_u64() as u8).collect()
    }
}

// ---------------------------------------------------------------------- ACK protocol

/// One acknowledged command, as printed by `crash-target`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    pub partition: u16,
    pub offset: u64,
    pub hash: [u8; 32],
    /// Set for an accepted `StartRun`.
    pub run: Option<RunId>,
}

/// The ACK line for an accepted command.
pub fn ack_line(p: u16, offset: Offset, cmd: &[u8], run: Option<&RunId>) -> String {
    let h = blake3::hash(cmd).to_hex();
    match run {
        Some(r) => format!("ACK {p} {} {h} RUN {r}", offset.0),
        None => format!("ACK {p} {} {h}", offset.0),
    }
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

fn parse_ack(line: &str) -> Option<Ack> {
    let mut it = line.split_ascii_whitespace();
    if it.next()? != "ACK" {
        return None;
    }
    let partition = it.next()?.parse().ok()?;
    let offset = it.next()?.parse().ok()?;
    let hash = parse_hex32(it.next()?)?;
    let run = match it.next() {
        None => None,
        Some("RUN") => Some(it.next()?.parse().ok()?),
        Some(_) => return None,
    };
    if it.next().is_some() {
        return None;
    }
    Some(Ack {
        partition,
        offset,
        hash,
        run,
    })
}

/// Every complete `ACK` line of `stdout`. A last line without a trailing newline is torn
/// (the process was killed mid-write) and ignored; a complete but malformed ACK line panics
/// (that would be a harness bug, not a crash artefact).
pub fn parse_acks(stdout: &str) -> Vec<Ack> {
    let complete = match stdout.rfind('\n') {
        Some(i) => &stdout[..=i],
        None => "",
    };
    complete
        .lines()
        .filter(|l| l.starts_with("ACK "))
        .map(|l| parse_ack(l).unwrap_or_else(|| panic!("malformed ACK line {l:?}")))
        .collect()
}

/// Partitions reported `FAILED <p> ...` by the target (complete lines only).
pub fn parse_failed(stdout: &str) -> Vec<u16> {
    let complete = match stdout.rfind('\n') {
        Some(i) => &stdout[..=i],
        None => "",
    };
    let mut v: Vec<u16> = complete
        .lines()
        .filter_map(|l| l.strip_prefix("FAILED "))
        .filter_map(|r| r.split_ascii_whitespace().next()?.parse().ok())
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

// ------------------------------------------------------------------- target process

/// Result of one `crash-target` run that was killed with SIGKILL.
#[derive(Debug)]
pub struct TargetRun {
    pub stdout: String,
    pub acks: Vec<Ack>,
    pub failed: Vec<u16>,
    /// Time from spawn to `READY`.
    pub startup: Duration,
}

/// Spawn `bin <dir> <seed> --partitions <p>` with `env`, wait for `READY`, let it run for
/// `run_for`, then `kill()` it (SIGKILL on Unix) and collect its stdout.
pub fn run_and_kill(
    bin: &Path,
    dir: &Path,
    seed: u64,
    partitions: u16,
    env: &[(&str, &str)],
    run_for: Duration,
) -> Result<TargetRun, String> {
    run_and_kill_with(bin, dir, seed, partitions, &[], env, run_for)
}

/// [`run_and_kill`] with extra `crash-target` arguments (e.g. `--pipeline 8`,
/// `--journal-mib 128`; see the binary's usage).
pub fn run_and_kill_with(
    bin: &Path,
    dir: &Path,
    seed: u64,
    partitions: u16,
    args: &[String],
    env: &[(&str, &str)],
    run_for: Duration,
) -> Result<TargetRun, String> {
    let mut cmd = Proc::new(bin);
    cmd.arg(dir)
        .arg(seed.to_string())
        .arg("--partitions")
        .arg(partitions.to_string())
        .args(args)
        .env_remove("FAILPOINTS")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    for (k, v) in env {
        cmd.env(k, v);
    }
    let start = Instant::now();
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", bin.display()))?;
    let stdout = child.stdout.take().ok_or("no stdout pipe")?;
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let reader = std::thread::spawn(move || {
        let mut r = BufReader::new(stdout);
        let mut all = String::new();
        let mut line = String::new();
        let mut ready = Some(ready_tx);
        loop {
            line.clear();
            match r.read_line(&mut line) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    if line == "READY\n"
                        && let Some(tx) = ready.take()
                    {
                        let _ = tx.send(());
                    }
                    all.push_str(&line);
                }
            }
        }
        // anything left without a newline (torn last line) is kept and ignored by parsers
        let mut rest = String::new();
        let _ = r.read_to_string(&mut rest);
        all.push_str(&rest);
        all
    });
    let ready = ready_rx.recv_timeout(OPEN_TIMEOUT);
    let startup = start.elapsed();
    if ready.is_ok() {
        std::thread::sleep(run_for);
    }
    let _ = child.kill();
    let status = child.wait().map_err(|e| format!("wait: {e}"))?;
    let stdout = reader.join().map_err(|_| "stdout reader panicked")?;
    if ready.is_err() {
        return Err(format!(
            "crash-target never printed READY (status {status}); stdout: {stdout}"
        ));
    }
    Ok(TargetRun {
        acks: parse_acks(&stdout),
        failed: parse_failed(&stdout),
        stdout,
        startup,
    })
}

// ------------------------------------------------------------------------ inspection

/// Open `dir` with `p` partitions in `mode` (default configs otherwise).
pub async fn open_node(dir: &Path, p: u16, mode: StartMode) -> Result<Node, String> {
    let mut cfg = NodeConfig::new(
        dir.to_path_buf(),
        PartitionCount::new(p).ok_or("zero partitions")?,
    );
    cfg.mode = mode;
    match tokio::time::timeout(OPEN_TIMEOUT, Node::open(cfg)).await {
        Ok(Ok(n)) => Ok(n),
        Ok(Err(e)) => Err(format!("open {}: {e}", dir.display())),
        Err(_) => Err(format!("open {} timed out", dir.display())),
    }
}

/// Command hash used in ACK lines.
pub fn cmd_hash(b: &[u8]) -> [u8; 32] {
    *blake3::hash(b).as_bytes()
}

/// `offset -> blake3(command bytes)` for every offset in `[log_start, HW)`. The verified
/// reader already enforces CRCs and offset continuity; this also checks it never skips.
pub async fn scan_log<L: ReplicatedLog>(log: &L) -> Result<BTreeMap<u64, [u8; 32]>, String> {
    let hw = *log.high_watermark().borrow();
    let mut next = log.log_start();
    let mut out = BTreeMap::new();
    while next < hw {
        let batches = log
            .read(next, hw, 1 << 20)
            .await
            .map_err(|e| format!("read p{} at {}: {e}", log.partition().0, next.0))?;
        if batches.is_empty() {
            return Err(format!(
                "short read p{} at {} below HW {}",
                log.partition().0,
                next.0,
                hw.0
            ));
        }
        for b in &batches {
            if b.base != next {
                return Err(format!(
                    "p{}: batch at {} where {} was expected",
                    log.partition().0,
                    b.base.0,
                    next.0
                ));
            }
            for (off, c) in b.commands_with_offsets() {
                out.insert(off.0, cmd_hash(c));
            }
            next = b.end();
        }
    }
    Ok(out)
}

/// Counters from [`verify_acks`].
#[derive(Debug, Default, Clone, Copy)]
pub struct AckCheck {
    pub acks: usize,
    pub runs: usize,
}

/// Every ACK's offset holds a command with the acknowledged hash; every acknowledged run
/// exists. Partitions listed in `skip` (e.g. a deliberately corrupted copy) are ignored.
pub async fn verify_acks(node: &Node, acks: &[Ack], skip: &[u16]) -> Result<AckCheck, String> {
    let mut scans: BTreeMap<u16, BTreeMap<u64, [u8; 32]>> = BTreeMap::new();
    let mut c = AckCheck::default();
    for a in acks {
        if skip.contains(&a.partition) {
            continue;
        }
        let h = node
            .handle(PartitionId(a.partition))
            .ok_or_else(|| failed_reason(node, a.partition))?;
        if let std::collections::btree_map::Entry::Vacant(e) = scans.entry(a.partition) {
            e.insert(scan_log(h.log().as_ref()).await?);
        }
        let scan = &scans[&a.partition];
        match scan.get(&a.offset) {
            None => {
                return Err(format!(
                    "lost acked command p{} offset {} (log [{}, {}))",
                    a.partition,
                    a.offset,
                    h.log().log_start().0,
                    h.log().log_end().0
                ));
            }
            Some(got) if *got != a.hash => {
                return Err(format!(
                    "acked command p{} offset {} has different bytes",
                    a.partition, a.offset
                ));
            }
            Some(_) => {}
        }
        c.acks += 1;
        if let Some(run) = &a.run {
            match h.read_run(run) {
                Ok(Some(_)) => c.runs += 1,
                Ok(None) => return Err(format!("acked run {run} missing on p{}", a.partition)),
                Err(e) => return Err(format!("read run {run}: {e}")),
            }
        }
    }
    Ok(c)
}

/// `"partition p failed: reason"` / `"partition p not ready"`.
pub fn failed_reason(node: &Node, p: u16) -> String {
    match node.slots().get(usize::from(p)) {
        Some(PartitionSlot::Failed { reason, .. }) => format!("partition {p} failed: {reason}"),
        Some(PartitionSlot::Ready(_)) => format!("partition {p} ready"),
        None => format!("partition {p} out of range"),
    }
}

/// Recursively copy `src` into `dst` (created).
pub fn copy_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let to = dst.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &to)?;
        } else {
            std::fs::copy(e.path(), &to)?;
        }
    }
    Ok(())
}

/// A copy of `src` (in a fresh tempdir, ext4 via `ostro_testkit`) with every state store
/// deleted. The data lives in `<tempdir>/data`.
pub fn stateless_copy(src: &Path) -> Result<tempfile::TempDir, String> {
    let t = ostro_testkit::fs::tempdir();
    let dst = t.path().join("data");
    copy_dir(src, &dst).map_err(|e| format!("copy: {e}"))?;
    std::fs::remove_dir_all(dst.join("state")).map_err(|e| format!("delete state: {e}"))?;
    Ok(t)
}

/// Per-partition `(digest, state_hash, applied)` of every Ready partition.
fn state_of(node: &Node, p: u16) -> Result<([u8; 32], [u8; 32], u64), String> {
    let h = node
        .handle(PartitionId(p))
        .ok_or_else(|| failed_reason(node, p))?;
    let meta = h
        .store()
        .read_meta()
        .map_err(|e| format!("meta p{p}: {e}"))?;
    let d = h
        .store()
        .digest()
        .map_err(|e| format!("digest p{p}: {e}"))?;
    Ok((d, meta.state_hash, meta.applied.0))
}

/// Copy `data_dir`, delete every state dir in the copy, open both in Inspect mode and
/// compare digest, rolling hash and applied offset per partition. `data_dir` must not be
/// open by anyone else.
pub async fn rebuild_and_compare(data_dir: &Path, p: u16) -> Result<(), String> {
    let copy = stateless_copy(data_dir)?;
    let live = open_node(data_dir, p, StartMode::Inspect).await?;
    let rebuilt = match open_node(&copy.path().join("data"), p, StartMode::Inspect).await {
        Ok(n) => n,
        Err(e) => {
            let _ = live.shutdown().await;
            return Err(e);
        }
    };
    let mut r = Ok(());
    for q in 0..p {
        let cmp = state_of(&live, q).and_then(|a| state_of(&rebuilt, q).map(|b| (a, b)));
        match cmp {
            Err(e) => r = Err(e),
            Ok((a, b)) => {
                if a.2 != b.2 {
                    r = Err(format!("p{q}: applied live {} != rebuilt {}", a.2, b.2));
                } else if a.1 != b.1 {
                    r = Err(format!("p{q}: rolling hash differs at applied {}", a.2));
                } else if a.0 != b.0 {
                    r = Err(format!("p{q}: digest differs at applied {}", a.2));
                }
            }
        }
        if r.is_err() {
            break;
        }
    }
    let _ = live.shutdown().await;
    let _ = rebuilt.shutdown().await;
    r
}

// --------------------------------------------------------------------------- segments

/// Non-empty segment files of partition `p` under `data_dir`, in segment order.
pub fn non_empty_segments(data_dir: &Path, p: u16) -> Result<Vec<PathBuf>, String> {
    let pid = PartitionId(p);
    let segs = list_segments(&segment_dir(&data_dir.join("wal"), pid), pid)
        .map_err(|e| format!("list segments p{p}: {e}"))?;
    Ok(segs
        .into_iter()
        .map(|(_, path)| path)
        .filter(|path| {
            std::fs::metadata(path)
                .map(|m| m.len() > 0)
                .unwrap_or(false)
        })
        .collect())
}

/// `(start, len)` of every complete record in a segment file (stops at the first span that
/// does not parse or does not fit).
pub fn record_spans(path: &Path) -> Result<Vec<(u64, u64)>, String> {
    let buf = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut pos = 0usize;
    let mut out = Vec::new();
    while let Some(len) = declared_record_size(&buf[pos..]).filter(|&l| pos + l <= buf.len()) {
        out.push((pos as u64, len as u64));
        pos += len;
    }
    Ok(out)
}

/// Total size in bytes of every regular file under `path` (0 if it does not exist).
pub fn dir_bytes(path: &Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(path) else {
        return 0;
    };
    rd.filter_map(Result::ok)
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&e.path()),
            Ok(_) => e.metadata().map(|m| m.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}

/// The 1-minute load average from `/proc/loadavg` (`None` where unavailable).
pub fn loadavg() -> Option<f64> {
    std::fs::read_to_string("/proc/loadavg")
        .ok()?
        .split_ascii_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Iteration count for the crash loops: `OSTRO_CRASH_ITERS` or `default`.
pub fn crash_iters(default: u64) -> u64 {
    std::env::var("OSTRO_CRASH_ITERS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_roundtrip_and_torn_last_line() {
        let run = RunId::new_v7(PartitionId(3));
        let l1 = ack_line(3, Offset(17), b"abc", Some(&run));
        let l2 = ack_line(1, Offset(2), b"xyz", None);
        let out = format!("READY\n{l1}\n{l2}\nACK 1 9 deadbe");
        let acks = parse_acks(&out);
        assert_eq!(acks.len(), 2);
        assert_eq!(acks[0].partition, 3);
        assert_eq!(acks[0].offset, 17);
        assert_eq!(acks[0].hash, cmd_hash(b"abc"));
        assert_eq!(acks[0].run, Some(run));
        assert_eq!(acks[1].run, None);
        assert!(parse_acks("ACK 1 2 ab").is_empty());
        assert_eq!(
            parse_failed("FAILED 2 x\nFAILED 0 y\nFAILED 2 z\nFAILED 1"),
            [0, 2]
        );
    }

    #[test]
    fn rng_is_deterministic_and_bounded() {
        let mut a = Rng::new(0);
        let mut b = Rng::new(0);
        let mut c = Rng::new(1);
        let va: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let vb: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        let vc: Vec<u64> = (0..8).map(|_| c.next_u64()).collect();
        assert_eq!(va, vb);
        assert_ne!(va, vc);
        for _ in 0..1000 {
            let x = a.range(5, 300);
            assert!((5..300).contains(&x));
        }
    }
}
