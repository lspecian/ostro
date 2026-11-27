//! 14-34: record-batch decoding without a broker (feature `testkit` for the encoder).
//!
//! Every codec round-trips (gzip, snappy raw and xerial-framed, lz4 frame, zstd); multiple
//! batches, a partial trailing batch, `from` filtering and `next_offset`; CRC corruption,
//! corrupt compressed data and decompression bombs (a zstd frame regenerating 1 GiB, gzip
//! and lz4 beyond 64 MiB) fail with `Protocol`; read_committed skips control batches and
//! aborted transactions; LogAppendTime batches use the max timestamp; duplicate header keys
//! stay in order; the broker-captured fixtures (`tests/fixtures/broker-<codec>.bin`, written
//! by `tests/broker.rs capture_fixtures`, compressed by the Java broker) decode exactly.

#![cfg(feature = "testkit")]

use bytes::Bytes;
use ostro_kafka::codec::{
    self, AbortedTxn, MAX_DECOMPRESSED, decode_batches, decode_committed, peek_batch,
};
use ostro_kafka::testkit::{BatchOpts, Compression, compress, encode_batch, encode_batch_with};
use ostro_kafka::{KafkaError, Record};

const CODECS: [(Compression, bool, &str); 6] = [
    (Compression::None, false, "none"),
    (Compression::Gzip, false, "gzip"),
    (Compression::Snappy, true, "snappy-xerial"),
    (Compression::Snappy, false, "snappy-raw"),
    (Compression::Lz4, false, "lz4"),
    (Compression::Zstd, false, "zstd"),
];

/// CRC-32C (Castagnoli), as Kafka uses for v2 batches.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Recompute the CRC of the single batch in `b`.
fn fix_crc(b: &mut [u8]) {
    let crc = crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
}

fn records(n: usize, base_ts: i64) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            offset: 0,
            timestamp_ms: base_ts + i as i64,
            key: (i % 7 != 3).then(|| Bytes::from(format!("key-{i}"))),
            value: (i % 11 != 5).then(|| Bytes::from(format!("value-{i}-{}", "x".repeat(i % 40)))),
            headers: if i % 3 == 0 {
                vec![
                    ("h1".into(), Some(Bytes::from(format!("{i}")))),
                    ("nul".into(), None),
                ]
            } else {
                Vec::new()
            },
        })
        .collect()
}

fn with_offsets(mut rs: Vec<Record>, base: i64) -> Vec<Record> {
    for (i, r) in rs.iter_mut().enumerate() {
        r.offset = base + i as i64;
    }
    rs
}

fn opts(c: Compression, xerial: bool, base: i64) -> BatchOpts {
    BatchOpts {
        xerial,
        base_offset: base,
        ..BatchOpts::new(c)
    }
}

#[test]
fn every_codec_round_trips() {
    for (c, xerial, name) in CODECS {
        let rs = records(300, 1_700_000_000_000);
        let b = encode_batch_with(&rs, &opts(c, xerial, 40)).unwrap();
        let h = peek_batch(&b).unwrap().unwrap();
        assert_eq!(h.compression, c, "{name}");
        assert_eq!(
            (h.base_offset, h.last_offset, h.record_count),
            (40, 339, 300)
        );
        if c == Compression::Snappy {
            let records_section = &b[61..];
            assert_eq!(
                records_section.starts_with(&codec::XERIAL_MAGIC),
                xerial,
                "{name} framing"
            );
        }
        let got = decode_batches(&b).unwrap();
        assert_eq!(got, with_offsets(rs, 40), "{name}");
    }
}

#[test]
fn several_batches_partial_tail_from_and_next_offset() {
    let mut buf = Vec::new();
    let mut want = Vec::new();
    for (i, (c, xerial, _)) in CODECS.iter().enumerate() {
        let rs = records(50, 1000 * i as i64);
        let base = 50 * i as i64;
        buf.extend_from_slice(&encode_batch_with(&rs, &opts(*c, *xerial, base)).unwrap());
        want.extend(with_offsets(rs, base));
    }
    let full = Bytes::from(buf.clone());
    let d = decode_committed(&full, &[], 0).unwrap();
    assert_eq!(d.records, want);
    assert_eq!((d.batches, d.next_offset), (6, Some(300)));
    // from = 75: the second batch is cut in the middle
    let d = decode_committed(&full, &[], 75).unwrap();
    assert_eq!(d.records, want[75..].to_vec());
    // a partial trailing batch (the broker's byte limit) is ignored
    let extra = encode_batch_with(&records(10, 0), &opts(Compression::Gzip, false, 300)).unwrap();
    for cut in [1, 12, 17, 61, extra.len() - 1] {
        let mut b = buf.clone();
        b.extend_from_slice(&extra[..cut]);
        let d = decode_committed(&Bytes::from(b), &[], 0).unwrap();
        assert_eq!(d.records.len(), 300, "cut {cut}");
        assert_eq!(d.next_offset, Some(300));
    }
    // only a partial batch: nothing, no progress
    let d = decode_committed(&extra.slice(..extra.len() - 1), &[], 300).unwrap();
    assert_eq!((d.records.len(), d.next_offset, d.batches), (0, None, 0));
    assert!(decode_batches(&Bytes::new()).unwrap().is_empty());
}

#[test]
fn crc_mismatch_is_a_protocol_error() {
    for (c, xerial, name) in CODECS {
        let b = encode_batch_with(&records(20, 5), &opts(c, xerial, 0)).unwrap();
        let mut bad = b.to_vec();
        let last = bad.len() - 1;
        bad[last] ^= 0x40;
        match decode_batches(&Bytes::from(bad)) {
            Err(KafkaError::Protocol(m)) => assert!(m.contains("Cyclic"), "{name}: {m}"),
            other => panic!("{name}: {other:?}"),
        }
    }
}

#[test]
fn corrupt_compressed_data_with_a_valid_crc_is_a_protocol_error() {
    for (c, xerial, name) in CODECS {
        if c == Compression::None {
            continue;
        }
        let b = encode_batch_with(&records(200, 5), &opts(c, xerial, 0)).unwrap();
        let mut bad = b.to_vec();
        // scramble the middle of the compressed records
        let mid = 61 + (bad.len() - 61) / 2;
        for x in &mut bad[mid..(mid + 8).min(b.len())] {
            *x ^= 0xa5;
        }
        fix_crc(&mut bad);
        // Either the codec rejects it or the record walk does; never a panic, never Ok
        // with different records.
        match decode_batches(&Bytes::from(bad)) {
            Err(KafkaError::Protocol(_)) => {}
            Ok(rs) => assert_ne!(rs, with_offsets(records(200, 5), 0), "{name}: undetected"),
            Err(e) => panic!("{name}: {e:?}"),
        }
    }
    // Uncompressed: a record length pointing past the batch.
    let b = encode_batch(&records(3, 5), Compression::None).unwrap();
    let mut bad = b.to_vec();
    bad[61] = 0x7e; // first record size varint = 63
    fix_crc(&mut bad);
    assert!(matches!(
        decode_batches(&Bytes::from(bad)),
        Err(KafkaError::Protocol(_))
    ));
    // A record count far above what the bytes can hold.
    let mut bad = b.to_vec();
    bad[57..61].copy_from_slice(&i32::MAX.to_be_bytes());
    fix_crc(&mut bad);
    match decode_batches(&Bytes::from(bad)) {
        Err(KafkaError::Protocol(m)) => assert!(m.contains("claims"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// A zstd frame whose header claims 1 GiB of content and whose RLE blocks really
/// regenerate it (8192 blocks of 128 KiB from 4 bytes each).
fn zstd_bomb() -> Vec<u8> {
    let mut f = vec![0x28, 0xb5, 0x2f, 0xfd];
    f.push(0xc0); // FCS 8 bytes, window descriptor present
    f.push(0x38); // window 128 KiB
    f.extend_from_slice(&(1u64 << 30).to_le_bytes());
    let blocks = (1usize << 30) / (128 << 10);
    for i in 0..blocks {
        let last = u32::from(i + 1 == blocks);
        let hdr = last | (1 << 1) | ((128u32 << 10) << 3);
        f.extend_from_slice(&hdr.to_le_bytes()[..3]);
        f.push(b'A');
    }
    f
}

/// Wrap a compressed records section into a batch claiming `count` records.
fn batch_around(c: Compression, section: &[u8], count: i32) -> Bytes {
    let template = encode_batch(&records(1, 0), c).unwrap();
    let mut b = template[..61].to_vec();
    b.extend_from_slice(section);
    let len = (b.len() - 12) as i32;
    b[8..12].copy_from_slice(&len.to_be_bytes());
    b[57..61].copy_from_slice(&count.to_be_bytes());
    fix_crc(&mut b);
    Bytes::from(b)
}

#[test]
fn decompression_bombs_hit_the_bound() {
    let bomb = zstd_bomb();
    assert!(bomb.len() < 64 << 10, "{}", bomb.len());
    match codec::decompress(Compression::Zstd, &bomb, MAX_DECOMPRESSED) {
        Err(KafkaError::Protocol(m)) => assert!(m.contains("more than"), "{m}"),
        other => panic!("{:?}", other.map(|v| v.len())),
    }
    // the frame itself is valid: with a 1 MiB bound it decodes up to the bound
    let e = codec::decompress(Compression::Zstd, &bomb, 1 << 20).unwrap_err();
    assert!(e.to_string().contains("more than 1048576"), "{e}");
    match decode_batches(&batch_around(Compression::Zstd, &bomb, 1)) {
        Err(KafkaError::Protocol(m)) => assert!(m.contains("more than"), "{m}"),
        other => panic!("{other:?}"),
    }
    let zeros = vec![0u8; MAX_DECOMPRESSED + 1];
    for c in [Compression::Gzip, Compression::Lz4, Compression::Snappy] {
        let z = compress(c, &zeros, true).unwrap();
        assert!(z.len() < 8 << 20, "{c:?} {}", z.len());
        match decode_batches(&batch_around(c, &z, 1)) {
            Err(KafkaError::Protocol(m)) => assert!(m.contains("more than"), "{c:?}: {m}"),
            other => panic!("{c:?}: {other:?}"),
        }
        // exactly at the bound decompresses
        assert_eq!(
            codec::decompress(
                c,
                &compress(c, &zeros[1..], true).unwrap(),
                MAX_DECOMPRESSED
            )
            .unwrap()
            .len(),
            MAX_DECOMPRESSED
        );
    }
}

fn control(pid: i64, base: i64, kind: i16) -> Bytes {
    let mut key = vec![0, 0];
    key.extend_from_slice(&kind.to_be_bytes());
    let rec = Record {
        key: Some(Bytes::from(key)),
        value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
        ..Record::default()
    };
    encode_batch_with(
        &[rec],
        &BatchOpts {
            base_offset: base,
            producer_id: pid,
            producer_epoch: 0,
            transactional: true,
            control: true,
            ..BatchOpts::new(Compression::None)
        },
    )
    .unwrap()
}

fn txn(pid: i64, base: i64, n: usize, c: Compression) -> (Bytes, Vec<Record>) {
    let rs = records(n, base * 10);
    let b = encode_batch_with(
        &rs,
        &BatchOpts {
            base_offset: base,
            producer_id: pid,
            producer_epoch: 0,
            base_sequence: 0,
            transactional: true,
            ..BatchOpts::new(c)
        },
    )
    .unwrap();
    (b, with_offsets(rs, base))
}

#[test]
fn read_committed_skips_control_and_aborted_batches() {
    // 0..5 plain, 5..10 pid 7 (aborted), 10..15 pid 8 (committed), 15 ABORT(7),
    // 16 COMMIT(8), 17..20 pid 7 again (committed later), 20 COMMIT(7), 21..23 plain.
    let plain1 = records(5, 0);
    let (a7, _) = txn(7, 5, 5, Compression::Gzip);
    let (c8, c8_recs) = txn(8, 10, 5, Compression::Zstd);
    let (c7, c7_recs) = txn(7, 17, 3, Compression::Lz4);
    let plain2 = records(2, 99);
    let mut buf = Vec::new();
    buf.extend_from_slice(&encode_batch_with(&plain1, &opts(Compression::None, false, 0)).unwrap());
    buf.extend_from_slice(&a7);
    buf.extend_from_slice(&c8);
    buf.extend_from_slice(&control(7, 15, 0));
    buf.extend_from_slice(&control(8, 16, 1));
    buf.extend_from_slice(&c7);
    buf.extend_from_slice(&control(7, 20, 1));
    buf.extend_from_slice(
        &encode_batch_with(&plain2, &opts(Compression::Snappy, true, 21)).unwrap(),
    );
    let buf = Bytes::from(buf);
    let aborted = [AbortedTxn {
        first_offset: 5,
        producer_id: 7,
    }];
    let d = decode_committed(&buf, &aborted, 0).unwrap();
    let mut want = with_offsets(plain1, 0);
    want.extend(c8_recs);
    want.extend(c7_recs);
    want.extend(with_offsets(plain2.clone(), 21));
    assert_eq!(d.records, want);
    assert_eq!(d.next_offset, Some(23));
    // Without the aborted list every data record comes back; control batches never do.
    let all = decode_batches(&buf).unwrap();
    assert_eq!(all.len(), 5 + 5 + 5 + 3 + 2);
    // Only control / aborted batches after `from`: no records, but progress.
    let d = decode_committed(&buf.slice(..), &aborted, 15).unwrap();
    assert_eq!(d.records.first().map(|r| r.offset), Some(17));
    let tail = {
        let mut b = Vec::new();
        b.extend_from_slice(&a7);
        b.extend_from_slice(&control(7, 15, 0));
        Bytes::from(b)
    };
    let d = decode_committed(&tail, &aborted, 5).unwrap();
    assert!(d.records.is_empty());
    assert_eq!(d.next_offset, Some(16));
}

#[test]
fn log_append_time_uses_the_max_timestamp() {
    let rs = records(4, 1000);
    let mut b = encode_batch(&rs, Compression::Gzip).unwrap().to_vec();
    let max_ts = 2_000_000i64;
    b[22] |= 1 << 3;
    b[35..43].copy_from_slice(&max_ts.to_be_bytes());
    fix_crc(&mut b);
    let got = decode_batches(&Bytes::from(b)).unwrap();
    assert!(got.iter().all(|r| r.timestamp_ms == max_ts), "{got:?}");
}

#[test]
fn duplicate_header_keys_stay_in_order() {
    let r = Record {
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::from_static(b"v")),
        headers: vec![
            ("aa".into(), Some(Bytes::from_static(b"1"))),
            ("bb".into(), Some(Bytes::from_static(b"2"))),
        ],
        ..Record::default()
    };
    let mut b = encode_batch(&[r], Compression::None).unwrap().to_vec();
    let pos = b.windows(2).position(|w| w == b"bb").unwrap();
    b[pos..pos + 2].copy_from_slice(b"aa");
    fix_crc(&mut b);
    let got = decode_batches(&Bytes::from(b)).unwrap();
    assert_eq!(
        got[0].headers,
        vec![
            ("aa".to_owned(), Some(Bytes::from_static(b"1"))),
            ("aa".to_owned(), Some(Bytes::from_static(b"2"))),
        ]
    );
}

#[test]
fn legacy_message_sets_are_rejected() {
    let mut b = encode_batch(&records(2, 0), Compression::None)
        .unwrap()
        .to_vec();
    b[16] = 1;
    match decode_batches(&Bytes::from(b)) {
        Err(KafkaError::Protocol(m)) => assert!(m.contains("v1"), "{m}"),
        other => panic!("{other:?}"),
    }
}

type Headers = Vec<(String, Option<Bytes>)>;

/// The records `capture_fixtures` produced (tests/broker.rs keeps the same definition).
fn fixture_records() -> Vec<(Bytes, Bytes, Headers)> {
    (0..200)
        .map(|i| {
            (
                Bytes::from(format!("fixture-key-{i}")),
                Bytes::from(format!("fixture-value-{i}-{}", "abc".repeat(i % 17))),
                vec![("i".to_owned(), Some(Bytes::from(i.to_string())))],
            )
        })
        .collect()
}

#[test]
fn broker_captured_fixtures_decode() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut found = 0;
    for codec in ["gzip", "snappy", "lz4", "zstd"] {
        let path = dir.join(format!("broker-{codec}.bin"));
        let Ok(bytes) = std::fs::read(&path) else {
            println!(
                "fixture missing: {} (capture with tests/broker.rs)",
                path.display()
            );
            continue;
        };
        found += 1;
        let bytes = Bytes::from(bytes);
        let h = peek_batch(&bytes).unwrap().unwrap();
        let want_codec = match codec {
            "gzip" => Compression::Gzip,
            "snappy" => Compression::Snappy,
            "lz4" => Compression::Lz4,
            _ => Compression::Zstd,
        };
        assert_eq!(h.compression, want_codec, "{codec}");
        if codec == "snappy" {
            println!(
                "broker snappy framing: {}",
                if bytes[61..].starts_with(&codec::XERIAL_MAGIC) {
                    "xerial"
                } else {
                    "raw"
                }
            );
        }
        let got = decode_batches(&bytes).unwrap();
        let want = fixture_records();
        assert_eq!(got.len(), want.len(), "{codec}");
        for (i, (r, (k, v, hs))) in got.iter().zip(&want).enumerate() {
            assert_eq!(r.offset, i as i64, "{codec}");
            assert_eq!(r.key.as_ref(), Some(k), "{codec}");
            assert_eq!(r.value.as_ref(), Some(v), "{codec}");
            assert_eq!(&r.headers, hs, "{codec}");
        }
    }
    println!("broker fixtures decoded: {found}/4");
}
