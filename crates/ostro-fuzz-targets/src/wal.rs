//! Fuzz target `wal_recovery_scan` (14-07, SEC-05): the partition log recovery scan
//! (`ostro_log::recover_partition`) over arbitrary segment bytes.
//!
//! Input: `selector ‖ bytes`. `selector >> 4` is the recovery floor (0..=15).
//! - `selector & 1 == 0` (raw): `bytes` is written as segment 0 (`selector & 2`: split in two
//!   segments at the middle).
//! - `selector & 1 == 1` (framed): CRC-correct V2 WAL records are built from `bytes` as
//!   `epoch_step: u8 ‖ base_step: u8 ‖ n: u8 ‖ n × (len: u8 ‖ command bytes)`; `selector & 2`
//!   flips one byte of the result at the position named by the last input byte, so the
//!   torn-tail / corruption classification behind valid records is reached.
//!
//! Invariants: never panics; the result is a report (possibly with a torn tail cut) or an
//! error; a successful recovery is idempotent: recovering the same directory again succeeds
//! with the same log bounds, epochs and record count and nothing left to cut.

use std::fs;

use ostro_log::ostro_wal::WalRecord;
use ostro_log::recover_partition;
use ostro_log::recovery::{TOPIC, segment_dir};
use ostro_types::{Epoch, Offset, PartitionId, envelope};

/// Records built per input in framed mode.
pub const MAX_RECORDS: usize = 32;

const PID: PartitionId = PartitionId(0);

/// Fuzz entry point.
pub fn wal_recovery_scan(data: &[u8]) {
    let Some((&sel, bytes)) = data.split_first() else {
        return;
    };
    let floor = Offset(u64::from(sel >> 4));
    let segments: Vec<Vec<u8>> = if sel & 1 == 0 {
        if sel & 2 != 0 {
            let (a, b) = bytes.split_at(bytes.len() / 2);
            vec![a.to_vec(), b.to_vec()]
        } else {
            vec![bytes.to_vec()]
        }
    } else {
        let mut seg = framed(bytes);
        if sel & 2 != 0
            && !seg.is_empty()
            && let Some(&at) = bytes.last()
        {
            let i = usize::from(at) * 7 % seg.len();
            seg[i] ^= 0x5a;
        }
        vec![seg]
    };
    recover(&segments, floor);
}

/// CRC-correct V2 records from `bytes` (see the module docs).
pub fn framed(mut bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut epoch, mut base) = (1u32, 0u64);
    for _ in 0..MAX_RECORDS {
        if bytes.len() < 3 {
            break;
        }
        epoch = epoch.saturating_add(u32::from(bytes[0] & 3));
        // Mostly contiguous; sometimes a gap or an overlap.
        base = match bytes[1] {
            0..=239 => base,
            240..=247 => base + u64::from(bytes[1] - 239),
            _ => base.saturating_sub(u64::from(bytes[1] - 247)),
        };
        let n = usize::from(bytes[2] % 8).max(1);
        bytes = &bytes[3..];
        let mut cmds = Vec::with_capacity(n);
        for _ in 0..n {
            let len = bytes.first().map_or(0, |&l| usize::from(l));
            let take = len.min(bytes.len().saturating_sub(1));
            let cmd = bytes.get(1..1 + take).unwrap_or_default().to_vec();
            bytes = bytes.get(1 + take..).unwrap_or_default();
            cmds.push(cmd.into());
        }
        let env = envelope::encode(PID, Epoch(epoch), Offset(base), cmds.as_slice());
        let last = base + n as u64 - 1;
        let rec = WalRecord::new_v2(
            TOPIC.to_string(),
            PID.as_i32(),
            env,
            base as i64,
            last as i64,
            n as i32,
        );
        if let Ok(b) = rec.to_bytes() {
            out.extend_from_slice(&b);
        }
        base = last + 1;
    }
    out
}

/// A scratch directory, on tmpfs (`/dev/shm`) when available so fsyncs are cheap.
pub fn scratch_dir() -> tempfile::TempDir {
    let shm = std::path::Path::new("/dev/shm");
    if shm.is_dir()
        && let Ok(d) = tempfile::Builder::new()
            .prefix("ostro-fuzz-")
            .tempdir_in(shm)
    {
        return d;
    }
    tempfile::tempdir().expect("tempdir")
}

fn recover(segments: &[Vec<u8>], floor: Offset) {
    let dir = scratch_dir();
    let seg_dir = segment_dir(dir.path(), PID);
    fs::create_dir_all(&seg_dir).expect("segment dir");
    for (i, s) in segments.iter().enumerate() {
        fs::write(seg_dir.join(format!("wal_{}_{i}.log", PID.0)), s).expect("write segment");
    }
    let first = recover_partition(dir.path(), PID, floor);
    let second = recover_partition(dir.path(), PID, floor);
    match (first, second) {
        (Ok(a), Ok(b)) => {
            assert!(a.log_start <= a.leo, "log_start after leo");
            assert_eq!(b.torn, None, "second recovery cut again");
            assert_eq!(
                (b.log_start, b.leo, &b.epochs, b.records),
                (a.log_start, a.leo, &a.epochs, a.records),
                "recovery is not idempotent"
            );
            for w in a.epochs.windows(2) {
                assert!(w[0].0 < w[1].0, "epoch history not increasing");
            }
        }
        (Ok(a), Err(e)) => panic!("recovered {a:?}, then failed on the result: {e}"),
        (Err(_), _) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_inputs() {
        wal_recovery_scan(&[]);
        wal_recovery_scan(&[0]);
        wal_recovery_scan(&[0, 1, 2, 3]);
        wal_recovery_scan(&[2, 0xff, 0xff]);
        wal_recovery_scan(&[1, 0, 0, 1, 3, b'a', b'b', b'c']);
        wal_recovery_scan(&[
            3, 0, 0, 1, 3, b'a', b'b', b'c', 0, 0, 2, 1, b'x', 1, b'y', 9,
        ]);
        wal_recovery_scan(&[0x11, 0, 0, 1, 3, b'a', b'b', b'c']);
    }
}
