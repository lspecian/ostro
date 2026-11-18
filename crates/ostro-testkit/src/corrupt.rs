//! Offline corruption helpers. Each mutator opens the file read+write,
//! applies the change and calls `sync_all()`.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

fn open_rw(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

fn write_zeros(f: &mut File, start: u64, len: u64) -> io::Result<()> {
    f.seek(SeekFrom::Start(start))?;
    let zeros = vec![0u8; usize::try_from(len).map_err(|e| invalid(e.to_string()))?];
    f.write_all(&zeros)?;
    f.sync_all()
}

/// Sector size used by [`zero_sector`].
pub const SECTOR: u64 = 512;

/// Flip bit `bit` (0 = LSB, must be < 8) of the byte at `byte_pos`.
pub fn flip_bit(path: &Path, byte_pos: u64, bit: u8) -> io::Result<()> {
    if bit >= 8 {
        return Err(invalid(format!("bit {bit} out of range 0..8")));
    }
    let mut f = open_rw(path)?;
    let len = f.metadata()?.len();
    if byte_pos >= len {
        return Err(invalid(format!("byte {byte_pos} past EOF ({len})")));
    }
    let mut b = [0u8; 1];
    f.seek(SeekFrom::Start(byte_pos))?;
    f.read_exact(&mut b)?;
    b[0] ^= 1 << bit;
    f.seek(SeekFrom::Start(byte_pos))?;
    f.write_all(&b)?;
    f.sync_all()
}

/// Zero `[start, start + len)`; errors if the range extends past EOF.
pub fn zero_range(path: &Path, start: u64, len: u64) -> io::Result<()> {
    let mut f = open_rw(path)?;
    let file_len = f.metadata()?.len();
    let end = start
        .checked_add(len)
        .ok_or_else(|| invalid("range overflows u64".into()))?;
    if end > file_len {
        return Err(invalid(format!(
            "range {start}..{end} past EOF ({file_len})"
        )));
    }
    write_zeros(&mut f, start, len)
}

/// Zero the 512-byte aligned sector `sector`, clamped at EOF.
pub fn zero_sector(path: &Path, sector: u64) -> io::Result<()> {
    let mut f = open_rw(path)?;
    let file_len = f.metadata()?.len();
    let start = sector
        .checked_mul(SECTOR)
        .ok_or_else(|| invalid("sector offset overflows u64".into()))?;
    if start >= file_len {
        return Err(invalid(format!(
            "sector {sector} starts past EOF ({file_len})"
        )));
    }
    let len = SECTOR.min(file_len - start);
    write_zeros(&mut f, start, len)
}

/// Shrink the file to `len` bytes; errors if `len` exceeds the current length.
pub fn truncate_at(path: &Path, len: u64) -> io::Result<()> {
    let f = open_rw(path)?;
    let cur = f.metadata()?.len();
    if len > cur {
        return Err(invalid(format!("truncate to {len} exceeds length {cur}")));
    }
    f.set_len(len)?;
    f.sync_all()
}

/// Append `bytes` at EOF (simulates an incomplete record at the tail).
pub fn append_garbage(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut f = OpenOptions::new().append(true).open(path)?;
    f.write_all(bytes)?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::tempdir;

    fn fixture(len: usize) -> (tempfile::TempDir, std::path::PathBuf, Vec<u8>) {
        let d = tempdir();
        let p = d.path().join("f.bin");
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8 + 1).collect();
        std::fs::write(&p, &data).unwrap();
        (d, p, data)
    }

    #[test]
    fn flip_bit_flips_one_bit_and_is_an_involution() {
        let (_d, p, orig) = fixture(100);
        flip_bit(&p, 10, 3).unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got.len(), orig.len());
        let diffs: Vec<_> = (0..orig.len()).filter(|&i| got[i] != orig[i]).collect();
        assert_eq!(diffs, [10]);
        assert_eq!(got[10] ^ orig[10], 1 << 3);
        flip_bit(&p, 10, 3).unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), orig);
    }

    #[test]
    fn flip_bit_rejects_bad_args() {
        let (_d, p, _) = fixture(10);
        assert!(flip_bit(&p, 10, 0).is_err());
        assert!(flip_bit(&p, 0, 8).is_err());
    }

    #[test]
    fn zero_range_zeroes_exactly_the_range() {
        let (_d, p, orig) = fixture(100);
        zero_range(&p, 20, 30).unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got.len(), 100);
        assert_eq!(&got[..20], &orig[..20]);
        assert!(got[20..50].iter().all(|&b| b == 0));
        assert_eq!(&got[50..], &orig[50..]);
        assert!(zero_range(&p, 90, 11).is_err());
    }

    #[test]
    fn zero_sector_zeroes_aligned_sector_clamped_at_eof() {
        let (_d, p, orig) = fixture(1200);
        zero_sector(&p, 1).unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(&got[..512], &orig[..512]);
        assert!(got[512..1024].iter().all(|&b| b == 0));
        assert_eq!(&got[1024..], &orig[1024..]);
        // last, partial sector: clamped, length unchanged
        zero_sector(&p, 2).unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(got.len(), 1200);
        assert!(got[1024..].iter().all(|&b| b == 0));
        // entirely past EOF
        assert!(zero_sector(&p, 3).is_err());
    }

    #[test]
    fn truncate_at_shrinks_and_rejects_growth() {
        let (_d, p, orig) = fixture(100);
        truncate_at(&p, 40).unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), &orig[..40]);
        assert!(truncate_at(&p, 41).is_err());
        truncate_at(&p, 40).unwrap();
        assert_eq!(crate::fs::file_len(&p), 40);
    }

    #[test]
    fn append_garbage_extends_tail() {
        let (_d, p, orig) = fixture(10);
        append_garbage(&p, &[0xAB, 0xCD]).unwrap();
        let got = std::fs::read(&p).unwrap();
        assert_eq!(&got[..10], &orig[..]);
        assert_eq!(&got[10..], &[0xAB, 0xCD]);
    }
}
