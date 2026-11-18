//! Envelope codec: 32-byte LE header + CRC32C + length-prefixed commands (plan 02-03, STOR-01).
//!
//! The golden hex below was computed independently (Python `struct` + bitwise CRC32C), not by
//! this implementation. It pins the on-disk bytes; never regenerate it from `encode`.

use bytes::Bytes;
use ostro_types::envelope::{
    DEFAULT_MAX_ENVELOPE_BYTES, EnvelopeError, HEADER_LEN, MAGIC, encode, peek_header, verify,
};
use ostro_types::ids::{Epoch, Offset, PartitionId};
use proptest::prelude::*;

const GOLDEN_HEX: &str =
    "4f535452010000000300000002000000640000000000000002000000270efb250100000061020000006263";

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn small() -> Vec<u8> {
    encode(
        PartitionId(3),
        Epoch(2),
        Offset(100),
        &[Bytes::from_static(b"a"), Bytes::from_static(b"bc")],
    )
}

/// Recomputes the CRC after a deliberate header tamper (so the tamper itself is what's detected).
fn reseal(b: &mut [u8]) {
    let crc = crc32c::crc32c_append(crc32c::crc32c(&b[..28]), &b[HEADER_LEN..]);
    b[28..32].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn constants() {
    assert_eq!(HEADER_LEN, 32);
    assert_eq!(MAGIC, u32::from_le_bytes(*b"OSTR"));
    assert_eq!(DEFAULT_MAX_ENVELOPE_BYTES, 4 << 20);
}

#[test]
fn golden_bytes() {
    assert_eq!(hex(&small()), GOLDEN_HEX);
}

#[test]
fn verify_small() {
    let v = verify(&small()).expect("valid");
    assert_eq!(v.header.codec_ver, 1);
    assert_eq!(v.header.partition, PartitionId(3));
    assert_eq!(v.header.epoch, Epoch(2));
    assert_eq!(v.header.base, Offset(100));
    assert_eq!(v.header.cmd_count, 2);
    assert_eq!(
        v.commands,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")]
    );
    assert_eq!(v.last(), Offset(101));
    let with: Vec<_> = v.commands_with_offsets().collect();
    assert_eq!(with[0].0, Offset(100));
    assert_eq!(with[1].0, Offset(101));
    assert_eq!(with[1].1, &Bytes::from_static(b"bc"));
}

#[test]
fn every_single_bit_flip_is_rejected() {
    let good = small();
    for bit in 0..good.len() * 8 {
        let mut b = good.clone();
        b[bit / 8] ^= 1 << (bit % 8);
        assert!(verify(&b).is_err(), "bit {bit} flip accepted");
    }
}

#[test]
fn every_truncation_is_rejected() {
    let good = small();
    for len in 0..good.len() {
        assert!(
            verify(&good[..len]).is_err(),
            "truncation to {len} accepted"
        );
    }
    assert_eq!(verify(&good[..10]), Err(EnvelopeError::TooShort));
}

#[test]
fn trailing_byte_is_rejected() {
    let mut b = small();
    b.push(0);
    // CRC covers the body, so an unsealed trailing byte is a CRC mismatch...
    assert!(matches!(verify(&b), Err(EnvelopeError::CrcMismatch { .. })));
    // ...and a resealed one is TrailingBytes.
    reseal(&mut b);
    assert_eq!(verify(&b), Err(EnvelopeError::TrailingBytes(1)));
}

#[test]
fn header_tampers_with_recomputed_crc() {
    let good = small();

    let mut b = good.clone();
    b[0] = b'X';
    reseal(&mut b);
    assert!(matches!(verify(&b), Err(EnvelopeError::BadMagic(_))));

    let mut b = good.clone();
    b[4..6].copy_from_slice(&2u16.to_le_bytes());
    reseal(&mut b);
    assert_eq!(verify(&b), Err(EnvelopeError::UnknownCodec(2)));

    let mut b = good.clone();
    b[6..8].copy_from_slice(&1u16.to_le_bytes());
    reseal(&mut b);
    assert_eq!(verify(&b), Err(EnvelopeError::NonZeroFlags(1)));

    let mut b = good.clone();
    b[10..12].copy_from_slice(&7u16.to_le_bytes());
    reseal(&mut b);
    assert_eq!(verify(&b), Err(EnvelopeError::NonZeroReserved(7)));

    // Count says 3 but only 2 commands present: parsing runs out of body.
    let mut b = good.clone();
    b[24..28].copy_from_slice(&3u32.to_le_bytes());
    reseal(&mut b);
    assert_eq!(
        verify(&b),
        Err(EnvelopeError::CountMismatch {
            header: 3,
            parsed: 2
        })
    );

    // Count says 1 but 2 commands present: the second shows up as trailing bytes.
    let mut b = good.clone();
    b[24..28].copy_from_slice(&1u32.to_le_bytes());
    reseal(&mut b);
    assert_eq!(verify(&b), Err(EnvelopeError::TrailingBytes(6)));

    // Count 0 is an empty envelope.
    let mut b = good[..HEADER_LEN].to_vec();
    b[24..28].copy_from_slice(&0u32.to_le_bytes());
    reseal(&mut b);
    assert_eq!(verify(&b), Err(EnvelopeError::Empty));
}

#[test]
fn length_prefix_overrun_is_malformed() {
    let mut b = small();
    // First command length 0x01 -> 0xff: overruns the body.
    b[HEADER_LEN] = 0xff;
    reseal(&mut b);
    assert_eq!(verify(&b), Err(EnvelopeError::BodyMalformed));
}

#[test]
fn crc_mismatch_reports_values() {
    let mut b = small();
    let last = b.len() - 1;
    b[last] ^= 0x01;
    match verify(&b) {
        Err(EnvelopeError::CrcMismatch { stored, computed }) => assert_ne!(stored, computed),
        other => panic!("expected CrcMismatch, got {other:?}"),
    }
}

#[test]
fn peek_header_checks_crc_without_parsing_body() {
    let good = small();
    let h = peek_header(&good).expect("ok");
    assert_eq!(h.partition, PartitionId(3));
    assert_eq!(h.epoch, Epoch(2));
    assert_eq!(h.base, Offset(100));
    assert_eq!(h.cmd_count, 2);

    // A body that doesn't parse but has a valid CRC: peek is Ok, verify is Err.
    let mut b = good.clone();
    b[HEADER_LEN] = 0xff;
    reseal(&mut b);
    assert!(peek_header(&b).is_ok());
    assert!(verify(&b).is_err());

    // Any corruption still fails the CRC.
    let mut b = good.clone();
    b[HEADER_LEN + 4] ^= 0x10;
    assert!(matches!(
        peek_header(&b),
        Err(EnvelopeError::CrcMismatch { .. })
    ));
    assert_eq!(peek_header(&good[..5]), Err(EnvelopeError::TooShort));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn round_trip(
        p in any::<u16>(),
        e in any::<u32>(),
        base in 0u64..(1 << 62),
        cmds in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..4096), 1..64),
    ) {
        let cmds: Vec<Bytes> = cmds.into_iter().map(Bytes::from).collect();
        let enc = encode(PartitionId(p), Epoch(e), Offset(base), &cmds);
        prop_assert_eq!(enc.len(), HEADER_LEN + cmds.iter().map(|c| 4 + c.len()).sum::<usize>());
        let v = verify(&enc).unwrap();
        prop_assert_eq!(v.header.partition, PartitionId(p));
        prop_assert_eq!(v.header.epoch, Epoch(e));
        prop_assert_eq!(v.header.base, Offset(base));
        prop_assert_eq!(v.header.cmd_count as usize, cmds.len());
        prop_assert_eq!(v.last(), Offset(base + cmds.len() as u64 - 1));
        prop_assert_eq!(&v.commands, &cmds);
        prop_assert_eq!(peek_header(&enc).unwrap(), v.header);
    }
}
