//! Record-batch decoding with pure-Rust codecs (14-34, D-P14-42).
//!
//! Kafka `records` bytes (a Fetch partition's payload) are a sequence of v2 record batches,
//! the last one possibly cut short by the broker's byte limit. [`decode_committed`] walks
//! them: a partial trailing batch is ignored, every complete batch goes through
//! `kafka-protocol`'s [`RecordBatchDecoder::decode_with_custom_compression`] (framing, CRC-32C,
//! attributes) with this module's decompressor:
//!
//! | codec  | crate            | notes                                                     |
//! |--------|------------------|-----------------------------------------------------------|
//! | gzip   | `flate2` (rust)  | single member                                             |
//! | snappy | `snap`           | raw blocks, and the xerial/Java framing `\x82SNAPPY\0`    |
//! | lz4    | `lz4_flex` frame | the standard LZ4 frame format Kafka writes for v2 batches |
//! | zstd   | `ruzstd`         | streaming decoder                                         |
//!
//! Every decompressor stops at [`MAX_DECOMPRESSED`] (64 MiB per batch, a zip-bomb guard) and
//! fails with [`KafkaError::Protocol`] beyond it. Before `kafka-protocol` turns the
//! decompressed bytes into records (it pre-allocates from the wire's record and header
//! counts), the records are walked here with every count and length checked against the
//! bytes that are left; the walk also yields this crate's [`Record`]s, which keep duplicate
//! header keys in order (kafka-protocol keeps headers in a map) and use the batch's max
//! timestamp for `LogAppendTime` batches (as the Java client does).
//!
//! read_committed: control batches are never returned; a transactional batch whose producer
//! has an aborted transaction starting at or before the batch's last offset (the Fetch
//! response's `aborted_transactions`) is skipped until that producer's ABORT marker.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::io::Read;

use bytes::Bytes;
use kafka_protocol::records::{Compression, RecordBatchDecoder};

use crate::{KafkaError, Record};

/// Largest decompressed size of one record batch (64 MiB).
pub const MAX_DECOMPRESSED: usize = 64 << 20;

/// Bytes of a v2 batch header (base offset .. record count).
pub const BATCH_HEADER: usize = 61;

/// The xerial (snappy-java) stream magic.
pub const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

/// The smallest encoding of one record: size, attributes, timestamp delta, offset delta,
/// key length, value length, header count (one byte each).
const MIN_RECORD: usize = 7;

fn protocol(msg: impl std::fmt::Display) -> KafkaError {
    KafkaError::Protocol(msg.to_string())
}

// ------------------------------------------------------------------------------ batch header

/// The fixed header of one v2 record batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchHeader {
    /// First offset of the batch.
    pub base_offset: i64,
    /// Last offset of the batch (`base_offset + last_offset_delta`).
    pub last_offset: i64,
    /// Total bytes of the batch, including the 12-byte offset + length prefix.
    pub size: usize,
    /// Codec.
    pub compression: Compression,
    /// Timestamps are the broker's log-append time (attribute bit 3).
    pub log_append_time: bool,
    /// Transactional batch (attribute bit 4).
    pub transactional: bool,
    /// Control batch (attribute bit 5).
    pub control: bool,
    /// First timestamp.
    pub first_timestamp: i64,
    /// Max timestamp.
    pub max_timestamp: i64,
    /// Producer id (-1 when not idempotent).
    pub producer_id: i64,
    /// Number of records.
    pub record_count: i32,
}

fn be_i64(b: &[u8], o: usize) -> i64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[o..o + 8]);
    i64::from_be_bytes(a)
}

fn be_i32(b: &[u8], o: usize) -> i32 {
    i32::from_be_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

/// Parse the batch header at the start of `buf`. `Ok(None)` = `buf` does not hold the whole
/// batch (a partial trailing batch, which the broker may send at its byte limit).
pub fn peek_batch(buf: &[u8]) -> Result<Option<BatchHeader>, KafkaError> {
    if buf.len() < 17 {
        return Ok(None);
    }
    let base_offset = be_i64(buf, 0);
    let length = be_i32(buf, 8);
    let magic = buf[16] as i8;
    if magic != 2 {
        return Err(protocol(format_args!(
            "record batch at offset {base_offset}: message format v{magic} is unsupported (v2 only)"
        )));
    }
    if length < (BATCH_HEADER - 12) as i32 {
        return Err(protocol(format_args!(
            "record batch at offset {base_offset}: invalid length {length}"
        )));
    }
    let size = 12 + length as usize;
    if buf.len() < size {
        return Ok(None);
    }
    let attributes = i16::from_be_bytes([buf[21], buf[22]]);
    let compression = match attributes & 0x7 {
        0 => Compression::None,
        1 => Compression::Gzip,
        2 => Compression::Snappy,
        3 => Compression::Lz4,
        4 => Compression::Zstd,
        c => {
            return Err(protocol(format_args!(
                "record batch at offset {base_offset}: unknown compression {c}"
            )));
        }
    };
    let last_offset_delta = be_i32(buf, 23);
    let record_count = be_i32(buf, 57);
    if last_offset_delta < 0 || record_count < 0 {
        return Err(protocol(format_args!(
            "record batch at offset {base_offset}: negative offset delta or record count"
        )));
    }
    Ok(Some(BatchHeader {
        base_offset,
        last_offset: base_offset.saturating_add(i64::from(last_offset_delta)),
        size,
        compression,
        log_append_time: attributes & (1 << 3) != 0,
        transactional: attributes & (1 << 4) != 0,
        control: attributes & (1 << 5) != 0,
        first_timestamp: be_i64(buf, 27),
        max_timestamp: be_i64(buf, 35),
        producer_id: be_i64(buf, 43),
        record_count,
    }))
}

// ------------------------------------------------------------------------------ decompression

/// Read `r` to the end, failing once more than `limit` bytes come out.
fn read_bounded(r: impl Read, limit: usize, what: &str) -> Result<Vec<u8>, KafkaError> {
    let mut out = Vec::new();
    r.take(limit as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| protocol(format_args!("{what} decompression: {e}")))?;
    if out.len() > limit {
        return Err(protocol(format_args!(
            "{what} batch decompresses to more than {limit} bytes"
        )));
    }
    Ok(out)
}

fn snappy_block(data: &[u8], out: &mut Vec<u8>, limit: usize) -> Result<(), KafkaError> {
    let n = snap::raw::decompress_len(data)
        .map_err(|e| protocol(format_args!("snappy decompression: {e}")))?;
    if n > limit.saturating_sub(out.len()) {
        return Err(protocol(format_args!(
            "snappy batch decompresses to more than {limit} bytes"
        )));
    }
    let start = out.len();
    out.resize(start + n, 0);
    let got = snap::raw::Decoder::new()
        .decompress(data, &mut out[start..])
        .map_err(|e| protocol(format_args!("snappy decompression: {e}")))?;
    out.truncate(start + got);
    Ok(())
}

/// Snappy: the xerial framing (`\x82SNAPPY\0`, version, compatible version, then
/// `[i32 length][raw block]*`) when the magic is present, else one raw block.
fn snappy(data: &[u8], limit: usize) -> Result<Vec<u8>, KafkaError> {
    let mut out = Vec::new();
    if data.len() >= 16 && data[..8] == XERIAL_MAGIC {
        let mut rest = &data[16..];
        while !rest.is_empty() {
            if rest.len() < 4 {
                return Err(protocol("snappy (xerial): truncated chunk length"));
            }
            let n = be_i32(rest, 0);
            rest = &rest[4..];
            if n < 0 || n as usize > rest.len() {
                return Err(protocol(format_args!(
                    "snappy (xerial): chunk of {n} bytes with {} left",
                    rest.len()
                )));
            }
            let (chunk, tail) = rest.split_at(n as usize);
            snappy_block(chunk, &mut out, limit)?;
            rest = tail;
        }
    } else {
        snappy_block(data, &mut out, limit)?;
    }
    Ok(out)
}

/// Decompress one batch's records section, at most `limit` bytes out.
pub fn decompress(c: Compression, data: &[u8], limit: usize) -> Result<Vec<u8>, KafkaError> {
    match c {
        Compression::None => {
            if data.len() > limit {
                return Err(protocol(format_args!(
                    "batch of {} bytes exceeds {limit}",
                    data.len()
                )));
            }
            Ok(data.to_vec())
        }
        Compression::Gzip => read_bounded(flate2::read::GzDecoder::new(data), limit, "gzip"),
        Compression::Snappy => snappy(data, limit),
        Compression::Lz4 => read_bounded(lz4_flex::frame::FrameDecoder::new(data), limit, "lz4"),
        Compression::Zstd => {
            let dec = ruzstd::decoding::StreamingDecoder::new(data)
                .map_err(|e| protocol(format_args!("zstd decompression: {e}")))?;
            read_bounded(dec, limit, "zstd")
        }
    }
}

// ------------------------------------------------------------------------------ record walk

struct Walk<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Walk<'a> {
    fn err(&self, msg: impl std::fmt::Display) -> KafkaError {
        protocol(format_args!("malformed record at byte {}: {msg}", self.pos))
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], KafkaError> {
        if n > self.remaining() {
            return Err(self.err(format_args!("needs {n} bytes, {} left", self.remaining())));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// A zig-zag varlong (at most 10 bytes).
    fn varlong(&mut self) -> Result<i64, KafkaError> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = self.take(1)?[0];
            v |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(((v >> 1) as i64) ^ -((v & 1) as i64));
            }
        }
        Err(self.err("varint longer than 10 bytes"))
    }

    fn varint(&mut self) -> Result<i32, KafkaError> {
        let v = self.varlong()?;
        i32::try_from(v).map_err(|_| self.err(format_args!("varint {v} out of range")))
    }

    /// A length-prefixed byte string; `-1` = null.
    fn nullable(&mut self, what: &str) -> Result<Option<&'a [u8]>, KafkaError> {
        match self.varint()? {
            -1 => Ok(None),
            n if n < 0 => Err(self.err(format_args!("negative {what} length {n}"))),
            n => self.take(n as usize).map(Some),
        }
    }
}

/// Walk the records of a decompressed batch, checking every length and count against the
/// bytes left, and build this crate's records.
fn walk_records(buf: &Bytes, header: &BatchHeader) -> Result<Vec<Record>, KafkaError> {
    let count = header.record_count.max(0) as usize;
    if count.saturating_mul(MIN_RECORD) > buf.len() {
        return Err(protocol(format_args!(
            "batch at offset {} claims {count} records in {} bytes",
            header.base_offset,
            buf.len()
        )));
    }
    let slice = |s: &[u8]| buf.slice_ref(s);
    let mut out = Vec::with_capacity(count);
    let mut w = Walk { buf, pos: 0 };
    for _ in 0..count {
        let size = w.varint()?;
        if size < 0 {
            return Err(w.err(format_args!("negative record size {size}")));
        }
        let body = w.take(size as usize)?;
        let mut r = Walk { buf: body, pos: 0 };
        r.take(1)?; // attributes
        let ts_delta = r.varlong()?;
        let off_delta = r.varint()?;
        let key = r.nullable("key")?.map(slice);
        let value = r.nullable("value")?.map(slice);
        let n = r.varint()?;
        if n < 0 || (n as usize).saturating_mul(2) > r.remaining() {
            return Err(r.err(format_args!("{n} headers cannot fit")));
        }
        let mut headers = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let Some(k) = r.nullable("header key")? else {
                return Err(r.err("null header key"));
            };
            let k = std::str::from_utf8(k)
                .map_err(|_| r.err("header key is not UTF-8"))?
                .to_owned();
            let v = r.nullable("header value")?.map(slice);
            headers.push((k, v));
        }
        out.push(Record {
            offset: header.base_offset.saturating_add(i64::from(off_delta)),
            timestamp_ms: if header.log_append_time {
                header.max_timestamp
            } else {
                header.first_timestamp.saturating_add(ts_delta)
            },
            key,
            value,
            headers,
        });
    }
    if w.remaining() != 0 {
        return Err(protocol(format_args!(
            "batch at offset {}: {} bytes after its {count} records",
            header.base_offset,
            w.remaining()
        )));
    }
    Ok(out)
}

/// Decode one complete batch (`batch` = exactly the batch's bytes, `header` from
/// [`peek_batch`]) into records.
pub fn decode_batch(batch: &Bytes, header: &BatchHeader) -> Result<Vec<Record>, KafkaError> {
    let walked: RefCell<Option<Vec<Record>>> = RefCell::new(None);
    let decompress_fn = |records: &mut Bytes, c: Compression| -> anyhow::Result<Bytes> {
        let out = Bytes::from(decompress(c, records, MAX_DECOMPRESSED)?);
        // Walk (and bound) the records before kafka-protocol pre-allocates from their counts.
        *walked.borrow_mut() = Some(walk_records(&out, header)?);
        Ok(out)
    };
    let set =
        RecordBatchDecoder::decode_with_custom_compression(&mut batch.clone(), Some(decompress_fn))
            .map_err(|e| match e.downcast::<KafkaError>() {
                Ok(k) => k,
                Err(e) => protocol(format_args!(
                    "record batch at offset {}: {e}",
                    header.base_offset
                )),
            })?;
    let records = walked
        .into_inner()
        .ok_or_else(|| protocol("record batch was not decompressed"))?;
    if set.records.len() != records.len()
        || set
            .records
            .iter()
            .zip(&records)
            .any(|(a, b)| a.offset != b.offset)
    {
        return Err(protocol(format_args!(
            "record batch at offset {}: inconsistent record decode",
            header.base_offset
        )));
    }
    Ok(records)
}

// ------------------------------------------------------------------------------ read_committed

/// One entry of a Fetch response's `aborted_transactions`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AbortedTxn {
    /// First offset of the aborted transaction.
    pub first_offset: i64,
    /// Its producer id.
    pub producer_id: i64,
}

/// Result of [`decode_committed`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Decoded {
    /// Records at or after `from`, in offset order.
    pub records: Vec<Record>,
    /// The offset after the last complete batch (`None` = no complete batch). Skipped control
    /// and aborted batches advance it too, so a consumer never re-reads them.
    pub next_offset: Option<i64>,
    /// Complete batches seen.
    pub batches: usize,
}

/// The control record type of a control batch's first record (`0` = ABORT, `1` = COMMIT).
fn control_type(records: &[Record]) -> Option<i16> {
    let k = records.first()?.key.as_ref()?;
    (k.len() >= 4).then(|| i16::from_be_bytes([k[2], k[3]]))
}

/// Decode Fetch `records` bytes with read_committed filtering (see the module docs).
pub fn decode_committed(
    bytes: &Bytes,
    aborted: &[AbortedTxn],
    from: i64,
) -> Result<Decoded, KafkaError> {
    let mut pending: BTreeSet<AbortedTxn> = aborted.iter().copied().collect();
    let mut aborting: HashSet<i64> = HashSet::new();
    let mut out = Decoded::default();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let Some(h) = peek_batch(&bytes[pos..])? else {
            break; // partial trailing batch
        };
        let batch = bytes.slice(pos..pos + h.size);
        pos += h.size;
        out.batches += 1;
        out.next_offset = Some(h.last_offset.saturating_add(1));
        // As the Java consumer: every batch with a producer id (data or control) first
        // activates the aborted transactions that start at or before its last offset, so a
        // fetch that starts at an ABORT marker still clears its producer.
        if h.producer_id >= 0 {
            while let Some(t) = pending.first().copied() {
                if t.first_offset > h.last_offset {
                    break;
                }
                pending.pop_first();
                aborting.insert(t.producer_id);
            }
        }
        if h.control {
            if aborting.contains(&h.producer_id) {
                let recs = decode_batch(&batch, &h)?;
                if control_type(&recs) == Some(0) {
                    aborting.remove(&h.producer_id);
                }
            }
            continue;
        }
        if h.transactional && aborting.contains(&h.producer_id) {
            continue;
        }
        if h.last_offset < from {
            continue;
        }
        out.records.extend(
            decode_batch(&batch, &h)?
                .into_iter()
                .filter(|r| r.offset >= from),
        );
    }
    Ok(out)
}

/// Decode every complete batch in `bytes` (control batches skipped, no aborted-transaction
/// information: transactional records are all returned).
pub fn decode_batches(bytes: &Bytes) -> Result<Vec<Record>, KafkaError> {
    Ok(decode_committed(bytes, &[], i64::MIN)?.records)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(record_count: i32) -> BatchHeader {
        BatchHeader {
            base_offset: 0,
            last_offset: 0,
            size: 61,
            compression: Compression::None,
            log_append_time: false,
            transactional: false,
            control: false,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            record_count,
        }
    }

    #[test]
    fn partial_and_legacy_headers() {
        assert_eq!(peek_batch(&[0u8; 10]).unwrap(), None);
        let mut b = vec![0u8; 61];
        b[11] = 49;
        b[16] = 1;
        assert!(peek_batch(&b).is_err(), "v1 message set");
        b[16] = 2;
        let h = peek_batch(&b).unwrap().unwrap();
        assert_eq!(
            (h.size, h.record_count, h.compression),
            (61, 0, Compression::None)
        );
        b[11] = 60;
        assert_eq!(peek_batch(&b).unwrap(), None, "claims more than present");
        b[11] = 10;
        assert!(peek_batch(&b).is_err(), "length below the header size");
        b[11] = 49;
        b[22] = 6;
        assert!(peek_batch(&b).is_err(), "unknown codec");
    }

    #[test]
    fn varlongs_decode_zigzag() {
        let mut w = Walk {
            buf: &[0x01, 0x02, 0xfe, 0xff, 0xff, 0xff, 0x0f],
            pos: 0,
        };
        assert_eq!(w.varlong().unwrap(), -1);
        assert_eq!(w.varlong().unwrap(), 1);
        assert_eq!(w.varint().unwrap(), i32::MAX);
        let mut w = Walk {
            buf: &[0xff; 11],
            pos: 0,
        };
        assert!(w.varlong().is_err());
    }

    #[test]
    fn snappy_bounds() {
        let raw = snap::raw::Encoder::new()
            .compress_vec(&[7u8; 4096])
            .unwrap();
        assert_eq!(snappy(&raw, 4096).unwrap(), vec![7u8; 4096]);
        assert!(snappy(&raw, 4095).is_err());
        let mut x = XERIAL_MAGIC.to_vec();
        x.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        for _ in 0..2 {
            x.extend_from_slice(&(raw.len() as i32).to_be_bytes());
            x.extend_from_slice(&raw);
        }
        assert_eq!(snappy(&x, 8192).unwrap().len(), 8192);
        assert!(snappy(&x, 8191).is_err());
        x.extend_from_slice(&[0, 0, 0x10, 0]);
        assert!(snappy(&x, 1 << 20).is_err(), "chunk longer than the rest");
    }

    #[test]
    fn walk_rejects_huge_counts() {
        let buf = Bytes::from_static(&[0u8; 64]);
        assert!(walk_records(&buf, &header(i32::MAX)).is_err());
        // one record (size 12) claiming 2^29 headers
        let rec = [
            0x18, 0, 0, 0, 0x01, 0x01, 0x80, 0x80, 0x80, 0x80, 0x04, 0, 0,
        ];
        let buf = Bytes::copy_from_slice(&rec);
        let e = walk_records(&buf, &header(1)).unwrap_err();
        assert!(e.to_string().contains("headers"), "{e}");
    }
}
