//! Logical snapshots in the OSNP v1 format (plan 08-02, P8-D1; spec: docs/snapshot-format.md).
//!
//! One format serves local snapshots, bottomless restore (Phase 8) and follower install
//! (Phase 12).
//!
//! - [`PartitionStore::export_snapshot`] streams every keyspace from ONE fjall
//!   `Database::snapshot()`. fjall commits a `WriteBatch` at a single seqno and publishes it
//!   after the last item, and every apply chunk (data + `applied_offset` + `state_hash` +
//!   `cursor`) is one batch. So the snapshot sees each chunk entirely or not at all, and the
//!   `applied_offset` read from it names exactly the rows exported. The apply thread is never
//!   paused.
//! - [`OsnpReader`] streams frames (CRC32C each) so a transport can forward them one by one.
//!   [`verify_snapshot`] checks a whole stream without writing anything.
//! - The trailer's `logical_digest` uses the `store/digest.rs` layout (shared
//!   [`DigestBuilder`]), so `store.digest()` of the source and of the installed store both
//!   equal it.
//!
//! **State format and migrators (D18 amended).** Once WAL trimming starts, snapshot rows are a
//! source of truth. The header carries `state_format` (= meta `format_ver`). Readers accept
//! every `state_format <= STATE_FORMAT` and pass each row through [`migrate_row`]; a newer
//! format is [`SnapshotError::FutureFormat`]. Today there is one format and the migrator is
//! the identity. Every later STATE/VALUE_FORMAT change MUST add its row migrator there (and a
//! format-corpus generation, docs/storage-format.md §14.2).

use std::io::{self, Read, Write};

use fjall::Readable;

use super::blobrefs;
use super::digest::DigestBuilder;
use super::meta::{self, K_APPLIED, K_CHECKPOINTED, K_CURSOR, K_STATE_HASH};
use super::values::{self, CursorMeta};
use super::{PartitionStore, StoreError, TABLES, Table};

/// File magic.
pub const MAGIC: [u8; 4] = *b"OSNP";
/// Snapshot container version.
pub const SNAP_VER: u16 = 1;
/// Header length in bytes.
pub const HEADER_LEN: usize = 64;
/// Header flag bit 0: `Rows` frame payloads are lz4 block-compressed (size-prepended).
pub const FLAG_LZ4: u16 = 1;
/// The state format this binary writes and installs (meta `format_ver`).
pub const STATE_FORMAT: u32 = meta::FORMAT_VERSION;
/// Target uncompressed size of one `Rows` frame. A single larger row gets its own frame.
pub const ROWS_FRAME_TARGET: usize = 1 << 20;
/// Largest frame payload a reader accepts (corruption guard).
pub const MAX_FRAME_LEN: u32 = 64 << 20;
/// Encoded size of the trailer frame.
pub const TRAILER_FRAME_LEN: usize = 4 + 1 + TRAILER_LEN + 4;
const TRAILER_LEN: usize = 32 + 32 + 8 + 8;

/// Frame kinds. Unknown kinds below [`kind::FIRST_SKIPPABLE`] are errors; at or above it
/// they are skipped (forward-compatible extensions).
pub mod kind {
    pub const TABLE_BEGIN: u8 = 1;
    pub const ROWS: u8 = 2;
    pub const TABLE_END: u8 = 3;
    pub const BLOB_REFS: u8 = 4;
    pub const TRAILER: u8 = 5;
    pub const FIRST_SKIPPABLE: u8 = 128;
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot I/O: {0}")]
    Io(io::Error),
    #[error("snapshot store: {0}")]
    Store(#[from] StoreError),
    #[error("snapshot header CRC mismatch")]
    HeaderCrc,
    #[error("not an OSNP snapshot (bad magic)")]
    BadMagic,
    #[error("unsupported OSNP version {0}")]
    UnsupportedVersion(u16),
    #[error("snapshot state_format {found} is newer than this binary's {current}")]
    FutureFormat { found: u32, current: u32 },
    #[error("CRC mismatch in frame {frame}")]
    FrameCrc { frame: u64 },
    #[error("snapshot truncated")]
    Truncated,
    #[error("unknown non-skippable frame kind {0}")]
    UnknownFrame(u8),
    #[error("malformed snapshot: {0}")]
    Malformed(String),
    #[error("logical digest mismatch: trailer {expected:02x?}, computed {actual:02x?}")]
    DigestMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    #[error("snapshot does not match the target: {0}")]
    Mismatch(String),
    #[error("injected failure at {0}")]
    Injected(&'static str),
}

impl From<io::Error> for SnapshotError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            SnapshotError::Truncated
        } else {
            SnapshotError::Io(e)
        }
    }
}

impl From<fjall::Error> for SnapshotError {
    fn from(e: fjall::Error) -> Self {
        SnapshotError::Store(StoreError::Fjall(e))
    }
}

fn malformed(s: impl Into<String>) -> SnapshotError {
    SnapshotError::Malformed(s.into())
}

/// The 64-byte OSNP v1 header (all integers little-endian):
/// `magic[4] | snap_ver u16 | flags u16 | partition u16 | partition_count u16 |
/// state_format u32 | applied_offset u64 | latest_epoch u32 | created_at_ms u64 |
/// reserved[24] = 0 | header_crc32c u32` (CRC over bytes 0..60).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    pub flags: u16,
    pub partition: u16,
    pub partition_count: u16,
    pub state_format: u32,
    /// Exclusive: the next offset to apply on top of this state.
    pub applied_offset: u64,
    pub latest_epoch: u32,
    pub created_at_ms: u64,
}

impl SnapshotHeader {
    pub fn lz4(&self) -> bool {
        self.flags & FLAG_LZ4 != 0
    }

    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0..4].copy_from_slice(&MAGIC);
        b[4..6].copy_from_slice(&SNAP_VER.to_le_bytes());
        b[6..8].copy_from_slice(&self.flags.to_le_bytes());
        b[8..10].copy_from_slice(&self.partition.to_le_bytes());
        b[10..12].copy_from_slice(&self.partition_count.to_le_bytes());
        b[12..16].copy_from_slice(&self.state_format.to_le_bytes());
        b[16..24].copy_from_slice(&self.applied_offset.to_le_bytes());
        b[24..28].copy_from_slice(&self.latest_epoch.to_le_bytes());
        b[28..36].copy_from_slice(&self.created_at_ms.to_le_bytes());
        // 36..60 reserved, zero
        let crc = crc32c::crc32c(&b[..60]);
        b[60..64].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// Checks the CRC first, then magic, version, flags and `state_format`.
    pub fn decode(b: &[u8; HEADER_LEN]) -> Result<Self, SnapshotError> {
        let crc = u32::from_le_bytes(b[60..64].try_into().expect("4 bytes"));
        if crc32c::crc32c(&b[..60]) != crc {
            return Err(SnapshotError::HeaderCrc);
        }
        if b[0..4] != MAGIC {
            return Err(SnapshotError::BadMagic);
        }
        let u16_at = |i: usize| u16::from_le_bytes(b[i..i + 2].try_into().expect("2 bytes"));
        let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().expect("4 bytes"));
        let u64_at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().expect("8 bytes"));
        let ver = u16_at(4);
        if ver != SNAP_VER {
            return Err(SnapshotError::UnsupportedVersion(ver));
        }
        let h = SnapshotHeader {
            flags: u16_at(6),
            partition: u16_at(8),
            partition_count: u16_at(10),
            state_format: u32_at(12),
            applied_offset: u64_at(16),
            latest_epoch: u32_at(24),
            created_at_ms: u64_at(28),
        };
        if h.flags & !FLAG_LZ4 != 0 {
            return Err(malformed(format!("unknown header flags {:#06x}", h.flags)));
        }
        if h.partition_count == 0 || h.partition >= h.partition_count {
            return Err(malformed(format!(
                "partition {} of {}",
                h.partition, h.partition_count
            )));
        }
        if h.state_format == 0 {
            return Err(malformed("state_format 0"));
        }
        if h.state_format > STATE_FORMAT {
            return Err(SnapshotError::FutureFormat {
                found: h.state_format,
                current: STATE_FORMAT,
            });
        }
        Ok(h)
    }
}

/// The trailer frame: totals and digests of the whole snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trailer {
    /// `store/digest.rs` layout over the exported rows (= `digest()` of the source store).
    pub logical_digest: [u8; 32],
    /// The meta `state_hash` of the exported state.
    pub state_hash: [u8; 32],
    /// Rows over all tables.
    pub total_rows: u64,
    /// Frames before the trailer (skippable ones included).
    pub frames: u64,
}

/// What an export wrote or a verification read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub partition: u16,
    pub partition_count: u16,
    pub state_format: u32,
    pub applied_offset: u64,
    pub state_hash: [u8; 32],
    pub latest_epoch: u32,
    pub logical_digest: [u8; 32],
    pub total_rows: u64,
    /// Encoded size of the whole snapshot.
    pub bytes: u64,
    /// Blob refs of the state (sorted, deduplicated; the `BlobRefs` frame).
    pub blob_refs: Vec<[u8; 32]>,
}

/// Export options.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExportOptions {
    /// lz4-compress `Rows` frames (header flag bit 0). Off by default: state values are
    /// mostly small postcard/prost records with little redundancy (see the 08-02 SUMMARY for
    /// the measured ratio), and snapshots are read on the restore/install path where
    /// decompression costs CPU for little gain.
    pub lz4: bool,
}

// ------------------------------------------------------------------------------- writer

struct Counter<W> {
    w: W,
    n: u64,
}

impl<W: Write> Counter<W> {
    fn put(&mut self, b: &[u8]) -> io::Result<()> {
        self.w.write_all(b)?;
        self.n += b.len() as u64;
        Ok(())
    }
}

fn frame_len(n: usize) -> Result<u32, SnapshotError> {
    u32::try_from(n)
        .ok()
        .filter(|&l| l <= MAX_FRAME_LEN)
        .ok_or_else(|| malformed(format!("frame payload of {n} bytes")))
}

fn len_u32(n: usize) -> Result<[u8; 4], SnapshotError> {
    u32::try_from(n)
        .map(u32::to_le_bytes)
        .map_err(|_| malformed("row longer than u32::MAX"))
}

/// Streaming OSNP v1 writer. Tables must be written in [`TABLES`] order, every one of them
/// (empty ones included), rows in strictly ascending key order.
pub struct OsnpWriter<W: Write> {
    out: Counter<W>,
    lz4: bool,
    frames: u64,
    digest: DigestBuilder,
    refs: Vec<[u8; 32]>,
    next_table: usize,
    current: Option<Table>,
    buf: Vec<u8>,
    total_rows: u64,
}

impl<W: Write> OsnpWriter<W> {
    /// Writes the header.
    pub fn new(w: W, header: &SnapshotHeader) -> Result<Self, SnapshotError> {
        let mut out = Counter { w, n: 0 };
        out.put(&header.encode())?;
        Ok(Self {
            out,
            lz4: header.lz4(),
            frames: 0,
            digest: DigestBuilder::new(),
            refs: Vec::new(),
            next_table: 0,
            current: None,
            buf: Vec::new(),
            total_rows: 0,
        })
    }

    fn frame(&mut self, kind: u8, payload: &[u8]) -> Result<(), SnapshotError> {
        let len = frame_len(payload.len())?;
        let crc = crc32c::crc32c_append(crc32c::crc32c(&[kind]), payload);
        self.out.put(&len.to_le_bytes())?;
        self.out.put(&[kind])?;
        self.out.put(payload)?;
        self.out.put(&crc.to_le_bytes())?;
        self.frames += 1;
        Ok(())
    }

    fn flush_rows(&mut self) -> Result<(), SnapshotError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let raw = std::mem::take(&mut self.buf);
        if self.lz4 {
            let c = lz4_flex::compress_prepend_size(&raw);
            self.frame(kind::ROWS, &c)
        } else {
            self.frame(kind::ROWS, &raw)
        }
    }

    pub fn table_begin(&mut self, t: Table) -> Result<(), SnapshotError> {
        if self.current.is_some() || TABLES.get(self.next_table) != Some(&t) {
            return Err(malformed(format!("table {} out of order", t.name())));
        }
        let name = t.name().as_bytes();
        let mut p = Vec::with_capacity(2 + name.len());
        p.extend_from_slice(&(name.len() as u16).to_le_bytes());
        p.extend_from_slice(name);
        self.frame(kind::TABLE_BEGIN, &p)?;
        self.digest.table_begin(t.name());
        self.current = Some(t);
        Ok(())
    }

    /// One row of the current table: buffered into `Rows` frames, fed to the digest and to the
    /// blob-ref visitor.
    pub fn row(&mut self, k: &[u8], v: &[u8]) -> Result<(), SnapshotError> {
        let t = self
            .current
            .ok_or_else(|| malformed("row outside a table"))?;
        blobrefs::visit_row(t, k, v, &mut self.refs)?;
        self.digest.row(k, v);
        let need = 8 + k.len() + v.len();
        if !self.buf.is_empty() && self.buf.len() + need > ROWS_FRAME_TARGET {
            self.flush_rows()?;
        }
        self.buf.extend_from_slice(&len_u32(k.len())?);
        self.buf.extend_from_slice(k);
        self.buf.extend_from_slice(&len_u32(v.len())?);
        self.buf.extend_from_slice(v);
        self.total_rows += 1;
        Ok(())
    }

    pub fn table_end(&mut self) -> Result<(), SnapshotError> {
        if self.current.take().is_none() {
            return Err(malformed("table_end outside a table"));
        }
        self.flush_rows()?;
        let rows = self.digest.table_end();
        self.frame(kind::TABLE_END, &rows.to_le_bytes())?;
        self.next_table += 1;
        Ok(())
    }

    /// A forward-compatible extension frame (`kind >= 128`), skipped by readers that do not
    /// know it. Allowed between tables.
    pub fn extension_frame(&mut self, kind: u8, payload: &[u8]) -> Result<(), SnapshotError> {
        if kind < kind::FIRST_SKIPPABLE || self.current.is_some() {
            return Err(malformed(format!("extension frame kind {kind}")));
        }
        self.frame(kind, payload)
    }

    /// Writes the `BlobRefs` and `Trailer` frames and flushes.
    pub fn finish(mut self, state_hash: [u8; 32]) -> Result<Finished<W>, SnapshotError> {
        if self.current.is_some() || self.next_table != TABLES.len() {
            return Err(malformed("finish before every table was written"));
        }
        let mut refs = std::mem::take(&mut self.refs);
        refs.sort_unstable();
        refs.dedup();
        let n = u32::try_from(refs.len()).map_err(|_| malformed("too many blob refs"))?;
        let mut p = Vec::with_capacity(4 + 32 * refs.len());
        p.extend_from_slice(&n.to_le_bytes());
        for r in &refs {
            p.extend_from_slice(r);
        }
        self.frame(kind::BLOB_REFS, &p)?;
        let digest = std::mem::replace(&mut self.digest, DigestBuilder::new());
        let trailer = Trailer {
            logical_digest: digest.finish(),
            state_hash,
            total_rows: self.total_rows,
            frames: self.frames,
        };
        self.frame(kind::TRAILER, &encode_trailer(&trailer))?;
        self.out.w.flush()?;
        Ok(Finished {
            writer: self.out.w,
            trailer,
            blob_refs: refs,
            bytes: self.out.n,
        })
    }
}

/// Result of [`OsnpWriter::finish`].
pub struct Finished<W> {
    pub writer: W,
    pub trailer: Trailer,
    /// Sorted, deduplicated blob refs (the `BlobRefs` frame).
    pub blob_refs: Vec<[u8; 32]>,
    /// Total bytes written, header included.
    pub bytes: u64,
}

fn encode_trailer(t: &Trailer) -> [u8; TRAILER_LEN] {
    let mut b = [0u8; TRAILER_LEN];
    b[0..32].copy_from_slice(&t.logical_digest);
    b[32..64].copy_from_slice(&t.state_hash);
    b[64..72].copy_from_slice(&t.total_rows.to_le_bytes());
    b[72..80].copy_from_slice(&t.frames.to_le_bytes());
    b
}

// ------------------------------------------------------------------------------- export

fn snap_get(
    snap: &fjall::Snapshot,
    ks: &fjall::Keyspace,
    key: &[u8],
) -> Result<Vec<u8>, SnapshotError> {
    snap.get(ks, key)?.map(|v| v.to_vec()).ok_or_else(|| {
        SnapshotError::Store(StoreError::Codec(format!(
            "meta key {} missing",
            String::from_utf8_lossy(key)
        )))
    })
}

fn fixed<const N: usize>(v: &[u8], what: &str) -> Result<[u8; N], SnapshotError> {
    <[u8; N]>::try_from(v)
        .map_err(|_| malformed(format!("meta {what} has length {}, want {N}", v.len())))
}

impl PartitionStore {
    /// Exports the current state as OSNP v1 (lz4 off). See [`Self::export_snapshot_with`].
    pub fn export_snapshot<W: Write>(
        &self,
        w: W,
        created_at_ms: u64,
    ) -> Result<SnapshotInfo, SnapshotError> {
        self.export_snapshot_with(w, created_at_ms, ExportOptions::default())
    }

    /// Exports every keyspace from ONE fjall snapshot: `applied_offset`, `state_hash` and the
    /// cursor are read from that same snapshot, so the rows correspond exactly to
    /// `applied_offset` (batch-atomic) without pausing the apply thread. The meta row
    /// `checkpointed_offset` is skipped (as in the digest). `created_at_ms` comes from the
    /// caller's injected clock.
    pub fn export_snapshot_with<W: Write>(
        &self,
        w: W,
        created_at_ms: u64,
        opts: ExportOptions,
    ) -> Result<SnapshotInfo, SnapshotError> {
        let snap = self.db.snapshot();
        let mks = self.ks(Table::Meta);
        let format = u32::from_be_bytes(fixed(
            &snap_get(&snap, mks, meta::K_FORMAT_VER)?,
            "format_ver",
        )?);
        if format != STATE_FORMAT {
            return Err(SnapshotError::Store(StoreError::FormatVersion {
                found: format,
            }));
        }
        let partition = u16::from_be_bytes(fixed(
            &snap_get(&snap, mks, meta::K_PARTITION)?,
            "partition",
        )?);
        let count = u16::from_be_bytes(fixed(
            &snap_get(&snap, mks, meta::K_PARTITION_COUNT)?,
            "partition_count",
        )?);
        let applied =
            u64::from_be_bytes(fixed(&snap_get(&snap, mks, K_APPLIED)?, "applied_offset")?);
        let state_hash: [u8; 32] = fixed(&snap_get(&snap, mks, K_STATE_HASH)?, "state_hash")?;
        let cursor: CursorMeta = values::decode(&snap_get(&snap, mks, K_CURSOR)?)?;
        let header = SnapshotHeader {
            flags: if opts.lz4 { FLAG_LZ4 } else { 0 },
            partition,
            partition_count: count,
            state_format: STATE_FORMAT,
            applied_offset: applied,
            latest_epoch: cursor.epoch,
            created_at_ms,
        };
        let mut out = OsnpWriter::new(w, &header)?;
        for t in TABLES {
            out.table_begin(t)?;
            for g in snap.iter(self.ks(t)) {
                let (k, v) = g.into_inner()?;
                if t == Table::Meta && &*k == K_CHECKPOINTED {
                    continue;
                }
                out.row(&k, &v)?;
            }
            out.table_end()?;
        }
        let Finished {
            trailer,
            blob_refs,
            bytes,
            ..
        } = out.finish(state_hash)?;
        Ok(SnapshotInfo {
            partition,
            partition_count: count,
            state_format: STATE_FORMAT,
            applied_offset: applied,
            state_hash,
            latest_epoch: cursor.epoch,
            logical_digest: trailer.logical_digest,
            total_rows: trailer.total_rows,
            bytes,
            blob_refs,
        })
    }
}

// ------------------------------------------------------------------------------- reader

/// A CRC-verified frame as stored (for frame-by-frame forwarding, Phase 12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawFrame {
    pub kind: u8,
    /// The payload as stored (still lz4-compressed for `Rows` when the header flag is set).
    pub payload: Vec<u8>,
}

impl RawFrame {
    /// The frame's wire encoding: `u32 LE len | kind | payload | u32 LE crc32c(kind ‖ payload)`.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(9 + self.payload.len());
        b.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        b.push(self.kind);
        b.extend_from_slice(&self.payload);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&[self.kind]), &self.payload);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }
}

/// A decoded frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    TableBegin(String),
    Rows(Vec<(Vec<u8>, Vec<u8>)>),
    TableEnd(u64),
    BlobRefs(Vec<[u8; 32]>),
    Trailer(Trailer),
    /// An unknown skippable frame (`kind >= 128`).
    Skipped {
        kind: u8,
        len: usize,
    },
}

/// Streaming OSNP v1 reader: header on construction, then one CRC-checked frame at a time.
/// After the trailer it checks that the stream ends.
pub struct OsnpReader<R: Read> {
    r: R,
    header: SnapshotHeader,
    frames: u64,
    bytes: u64,
    done: bool,
}

impl<R: Read> OsnpReader<R> {
    pub fn new(mut r: R) -> Result<Self, SnapshotError> {
        let mut h = [0u8; HEADER_LEN];
        r.read_exact(&mut h)?;
        let header = SnapshotHeader::decode(&h)?;
        Ok(Self {
            r,
            header,
            frames: 0,
            bytes: HEADER_LEN as u64,
            done: false,
        })
    }

    pub fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    /// Bytes consumed so far.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Frames read so far (the trailer included once read).
    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// The next CRC-verified frame as stored; `None` after the trailer.
    pub fn next_raw(&mut self) -> Result<Option<RawFrame>, SnapshotError> {
        if self.done {
            return Ok(None);
        }
        let mut lk = [0u8; 5];
        self.r.read_exact(&mut lk)?;
        let len = u32::from_le_bytes(lk[..4].try_into().expect("4 bytes"));
        let kind = lk[4];
        if len > MAX_FRAME_LEN {
            return Err(malformed(format!("frame length {len}")));
        }
        let mut payload = Vec::new();
        (&mut self.r)
            .take(u64::from(len))
            .read_to_end(&mut payload)?;
        if payload.len() != len as usize {
            return Err(SnapshotError::Truncated);
        }
        let mut crc = [0u8; 4];
        self.r.read_exact(&mut crc)?;
        if crc32c::crc32c_append(crc32c::crc32c(&[kind]), &payload) != u32::from_le_bytes(crc) {
            return Err(SnapshotError::FrameCrc { frame: self.frames });
        }
        self.frames += 1;
        self.bytes += 9 + u64::from(len);
        if kind == kind::TRAILER {
            self.done = true;
            let mut extra = [0u8; 1];
            if self.r.read(&mut extra)? != 0 {
                return Err(malformed("bytes after the trailer"));
            }
        }
        Ok(Some(RawFrame { kind, payload }))
    }

    /// The next frame, decoded; `None` after the trailer.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, SnapshotError> {
        match self.next_raw()? {
            None => Ok(None),
            Some(f) => decode_frame(f, self.header.lz4()).map(Some),
        }
    }
}

fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8], SnapshotError> {
    if b.len() < n {
        return Err(malformed("frame payload too short"));
    }
    let (h, t) = b.split_at(n);
    *b = t;
    Ok(h)
}

fn take_u32(b: &mut &[u8]) -> Result<u32, SnapshotError> {
    Ok(u32::from_le_bytes(take(b, 4)?.try_into().expect("4 bytes")))
}

fn take_u64(b: &mut &[u8]) -> Result<u64, SnapshotError> {
    Ok(u64::from_le_bytes(take(b, 8)?.try_into().expect("8 bytes")))
}

fn take_32(b: &mut &[u8]) -> Result<[u8; 32], SnapshotError> {
    Ok(take(b, 32)?.try_into().expect("32 bytes"))
}

fn end(b: &[u8]) -> Result<(), SnapshotError> {
    if b.is_empty() {
        Ok(())
    } else {
        Err(malformed("trailing bytes in frame payload"))
    }
}

fn decode_frame(f: RawFrame, lz4: bool) -> Result<Frame, SnapshotError> {
    let mut b = f.payload.as_slice();
    let frame = match f.kind {
        kind::TABLE_BEGIN => {
            let n = u16::from_le_bytes(take(&mut b, 2)?.try_into().expect("2 bytes"));
            let name = std::str::from_utf8(take(&mut b, usize::from(n))?)
                .map_err(|_| malformed("table name is not utf-8"))?
                .to_string();
            end(b)?;
            Frame::TableBegin(name)
        }
        kind::ROWS => {
            let raw;
            let mut rb: &[u8] = if lz4 {
                let (size, _) = lz4_flex::block::uncompressed_size(b)
                    .map_err(|e| malformed(format!("lz4: {e}")))?;
                if size > MAX_FRAME_LEN as usize {
                    return Err(malformed(format!("lz4 frame of {size} bytes")));
                }
                raw = lz4_flex::decompress_size_prepended(b)
                    .map_err(|e| malformed(format!("lz4: {e}")))?;
                &raw
            } else {
                b
            };
            let mut rows = Vec::new();
            while !rb.is_empty() {
                let kl = take_u32(&mut rb)? as usize;
                let k = take(&mut rb, kl)?.to_vec();
                let vl = take_u32(&mut rb)? as usize;
                let v = take(&mut rb, vl)?.to_vec();
                rows.push((k, v));
            }
            if rows.is_empty() {
                return Err(malformed("empty Rows frame"));
            }
            b = &[];
            Frame::Rows(rows)
        }
        kind::TABLE_END => {
            let n = take_u64(&mut b)?;
            Frame::TableEnd(n)
        }
        kind::BLOB_REFS => {
            let n = take_u32(&mut b)? as usize;
            if b.len() != n.saturating_mul(32) {
                return Err(malformed("BlobRefs length"));
            }
            let mut refs = Vec::with_capacity(n);
            for _ in 0..n {
                refs.push(take_32(&mut b)?);
            }
            Frame::BlobRefs(refs)
        }
        kind::TRAILER => Frame::Trailer(Trailer {
            logical_digest: take_32(&mut b)?,
            state_hash: take_32(&mut b)?,
            total_rows: take_u64(&mut b)?,
            frames: take_u64(&mut b)?,
        }),
        k if k >= kind::FIRST_SKIPPABLE => {
            return Ok(Frame::Skipped {
                kind: k,
                len: f.payload.len(),
            });
        }
        k => return Err(SnapshotError::UnknownFrame(k)),
    };
    end(b)?;
    Ok(frame)
}

// ---------------------------------------------------------------------------- migration

/// Converts one row written at `from_format` to [`STATE_FORMAT`] (D18 amended).
///
/// Identity for the single format that exists today. A later STATE/VALUE_FORMAT change adds
/// an arm here that rewrites the affected tables' rows (including the meta `format_ver` row);
/// `install_snapshot` then checks the installed store against the digest of the migrated
/// rows, and the trailer against the digest of the rows as read.
pub fn migrate_row(
    table: Table,
    from_format: u32,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(Vec<u8>, Vec<u8>), SnapshotError> {
    match from_format {
        STATE_FORMAT => Ok((key, value)),
        f => Err(malformed(format!(
            "no migrator from state_format {f} for table {}",
            table.name()
        ))),
    }
}

// ----------------------------------------------------------------------- stream checking

/// Structural + digest verification of a whole stream; every row (already migrated) is
/// handed to `sink`. Returns the info plus the digest the installed store must have.
pub(crate) fn read_stream<R: Read>(
    r: R,
    mut sink: impl FnMut(Table, Vec<u8>, Vec<u8>) -> Result<(), SnapshotError>,
) -> Result<(SnapshotHeader, SnapshotInfo, [u8; 32]), SnapshotError> {
    let mut rd = OsnpReader::new(r)?;
    let header = rd.header().clone();
    let migrating = header.state_format != STATE_FORMAT;
    let mut digest = DigestBuilder::new();
    let mut mdigest = migrating.then(DigestBuilder::new);
    let mut next_table = 0usize;
    let mut current: Option<Table> = None;
    let mut last_key: Option<Vec<u8>> = None;
    let mut total_rows = 0u64;
    let mut refs = Vec::new();
    let mut frame_refs: Option<Vec<[u8; 32]>> = None;
    let mut meta_applied: Option<u64> = None;
    let mut meta_hash: Option<[u8; 32]> = None;
    let mut meta_partition: Option<u16> = None;
    let mut meta_count: Option<u16> = None;

    // Tables absent from the stream (an older snapshot written before a keyspace existed)
    // are empty: digest them as such.
    let skip_to =
        |upto: usize, next: &mut usize, d: &mut DigestBuilder, md: &mut Option<DigestBuilder>| {
            while *next < upto {
                let name = TABLES[*next].name();
                d.table_begin(name);
                d.table_end();
                if let Some(m) = md.as_mut() {
                    m.table_begin(name);
                    m.table_end();
                }
                *next += 1;
            }
        };

    loop {
        let frames_before = rd.frames();
        let f = rd.next_frame()?.ok_or(SnapshotError::Truncated)?;
        match f {
            Frame::TableBegin(name) => {
                if current.is_some() || frame_refs.is_some() {
                    return Err(malformed(format!("TableBegin {name} out of place")));
                }
                let idx = TABLES
                    .iter()
                    .position(|t| t.name() == name)
                    .filter(|&i| i >= next_table)
                    .ok_or_else(|| malformed(format!("unknown or out-of-order table {name}")))?;
                skip_to(idx, &mut next_table, &mut digest, &mut mdigest);
                let t = TABLES[idx];
                digest.table_begin(t.name());
                if let Some(m) = mdigest.as_mut() {
                    m.table_begin(t.name());
                }
                current = Some(t);
                last_key = None;
            }
            Frame::Rows(rows) => {
                let t = current.ok_or_else(|| malformed("Rows outside a table"))?;
                for (k, v) in rows {
                    if last_key.as_ref().is_some_and(|l| *l >= k) {
                        return Err(malformed(format!("keys out of order in {}", t.name())));
                    }
                    if t == Table::Meta {
                        match k.as_slice() {
                            K_CHECKPOINTED => {
                                return Err(malformed("checkpointed_offset exported"));
                            }
                            K_APPLIED => {
                                meta_applied =
                                    Some(u64::from_be_bytes(fixed(&v, "applied_offset")?))
                            }
                            K_STATE_HASH => meta_hash = Some(fixed(&v, "state_hash")?),
                            meta::K_PARTITION => {
                                meta_partition = Some(u16::from_be_bytes(fixed(&v, "partition")?))
                            }
                            meta::K_PARTITION_COUNT => {
                                meta_count = Some(u16::from_be_bytes(fixed(&v, "partition_count")?))
                            }
                            _ => {}
                        }
                    }
                    blobrefs::visit_row(t, &k, &v, &mut refs)?;
                    digest.row(&k, &v);
                    last_key = Some(k.clone());
                    total_rows += 1;
                    let (k2, v2) = migrate_row(t, header.state_format, k, v)?;
                    if let Some(m) = mdigest.as_mut() {
                        m.row(&k2, &v2);
                    }
                    sink(t, k2, v2)?;
                }
            }
            Frame::TableEnd(n) => {
                let t = current
                    .take()
                    .ok_or_else(|| malformed("TableEnd outside a table"))?;
                let rows = digest.table_end();
                if let Some(m) = mdigest.as_mut() {
                    m.table_end();
                }
                if rows != n {
                    return Err(malformed(format!(
                        "table {}: TableEnd says {n} rows, read {rows}",
                        t.name()
                    )));
                }
                next_table += 1;
            }
            Frame::BlobRefs(fr) => {
                if current.is_some() || frame_refs.is_some() {
                    return Err(malformed("BlobRefs out of place"));
                }
                skip_to(TABLES.len(), &mut next_table, &mut digest, &mut mdigest);
                refs.sort_unstable();
                refs.dedup();
                if fr != refs {
                    return Err(malformed(format!(
                        "BlobRefs frame lists {} refs, state has {}",
                        fr.len(),
                        refs.len()
                    )));
                }
                frame_refs = Some(fr);
            }
            Frame::Trailer(tr) => {
                if current.is_some() || frame_refs.is_none() {
                    return Err(malformed("Trailer before BlobRefs"));
                }
                let actual = std::mem::replace(&mut digest, DigestBuilder::new()).finish();
                if actual != tr.logical_digest {
                    return Err(SnapshotError::DigestMismatch {
                        expected: tr.logical_digest,
                        actual,
                    });
                }
                if tr.total_rows != total_rows || tr.frames != frames_before {
                    return Err(malformed(format!(
                        "trailer counts rows {} frames {}, read {total_rows} / {frames_before}",
                        tr.total_rows, tr.frames
                    )));
                }
                if meta_hash != Some(tr.state_hash) {
                    return Err(malformed("trailer state_hash differs from the meta row"));
                }
                if meta_applied != Some(header.applied_offset) {
                    return Err(SnapshotError::Mismatch(format!(
                        "meta applied_offset {meta_applied:?} != header {}",
                        header.applied_offset
                    )));
                }
                if meta_partition != Some(header.partition)
                    || meta_count != Some(header.partition_count)
                {
                    return Err(SnapshotError::Mismatch(
                        "meta partition / partition_count differ from the header".into(),
                    ));
                }
                let install_digest = match mdigest.take() {
                    Some(m) => m.finish(),
                    None => actual,
                };
                let info = SnapshotInfo {
                    partition: header.partition,
                    partition_count: header.partition_count,
                    state_format: header.state_format,
                    applied_offset: header.applied_offset,
                    state_hash: tr.state_hash,
                    latest_epoch: header.latest_epoch,
                    logical_digest: tr.logical_digest,
                    total_rows,
                    bytes: rd.bytes(),
                    blob_refs: refs,
                };
                return Ok((header, info, install_digest));
            }
            Frame::Skipped { .. } => {
                if current.is_some() {
                    return Err(malformed("extension frame inside a table"));
                }
            }
        }
    }
}

/// Reads and fully verifies a snapshot (header CRC, frame CRCs, structure, row order, blob
/// refs, trailer digest and counts) without writing anything.
pub fn verify_snapshot<R: Read>(r: R) -> Result<SnapshotInfo, SnapshotError> {
    read_stream(r, |_, _, _| Ok(())).map(|(_, info, _)| info)
}
