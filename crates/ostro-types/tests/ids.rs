//! Ids, RunId string form, routing golden vectors and PayloadRef (plan 02-03).
//!
//! `tests/golden/routing.txt` freezes the routing hash. Regenerate ONLY if the routing layout is
//! deliberately changed before the Phase 5 format freeze:
//! `OSTRO_BLESS=1 cargo test -p ostro-types --test ids`.

use std::str::FromStr;

use bytes::Bytes;
use ostro_types::ids::{AttemptToken, Epoch, IdError, Offset, PartitionCount, PartitionId, RunId};
use ostro_types::payload::{INLINE_PAYLOAD_LIMIT, PayloadRef, payload_ref};
use ostro_types::routing::{partition_for_key, partition_for_new_run};
use proptest::prelude::*;
use prost::Message;

const GOLDEN: &str = include_str!("golden/routing.txt");
const HEADER: &str = "# ostro routing golden vectors — workflow<TAB>idem_key_hex<TAB>P<TAB>partition — DO NOT REGENERATE (frozen at Phase 5)";

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn pc(n: u16) -> PartitionCount {
    PartitionCount::new(n).expect("non-zero")
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}

// ---------------------------------------------------------------------------
// ids
// ---------------------------------------------------------------------------

#[test]
fn run_id_embeds_partition_big_endian() {
    let uuid = uuid::Uuid::from_bytes([0xab; 16]);
    let id = RunId::new(PartitionId(7), uuid);
    assert_eq!(id.partition(), PartitionId(7));
    assert_eq!(&id.as_bytes()[0..2], &7u16.to_be_bytes());
    assert_eq!(&id.as_bytes()[2..], uuid.as_bytes());
    assert_eq!(RunId::LEN, 18);

    let big = RunId::new(PartitionId(0x0102), uuid);
    assert_eq!(&big.as_bytes()[0..2], &[0x01, 0x02]);
    assert_eq!(big.partition(), PartitionId(0x0102));
}

#[test]
fn run_id_new_v7_is_v7_and_keeps_partition() {
    let id = RunId::new_v7(PartitionId(3));
    assert_eq!(id.partition(), PartitionId(3));
    let uuid = uuid::Uuid::from_slice(&id.as_bytes()[2..]).expect("uuid");
    assert_eq!(uuid.get_version_num(), 7);
}

#[test]
fn run_id_from_bytes_checks_length() {
    let id = RunId::new_v7(PartitionId(9));
    assert_eq!(RunId::from_bytes(id.as_bytes()).expect("ok"), id);
    assert!(matches!(
        RunId::from_bytes(&[0u8; 17]),
        Err(IdError::BadLength(17))
    ));
    assert!(matches!(
        RunId::from_bytes(&[0u8; 19]),
        Err(IdError::BadLength(19))
    ));
}

#[test]
fn run_id_string_known_value() {
    // All-zero bytes encode to 29 '0' characters.
    let zero = RunId::from_bytes(&[0u8; 18]).expect("ok");
    assert_eq!(zero.to_string(), "0".repeat(29));
    // All-ones: 144 bits = 28 full groups of 5 + a final group of 4 bits left-aligned (11110 = 30 = 'y').
    let ones = RunId::from_bytes(&[0xff; 18]).expect("ok");
    assert_eq!(ones.to_string(), format!("{}y", "z".repeat(28)));
    assert_eq!(RunId::from_str(&ones.to_string()).expect("parse"), ones);
}

#[test]
fn run_id_from_str_accepts_upper_and_aliases() {
    let id = RunId::new_v7(PartitionId(12));
    let s = id.to_string();
    assert_eq!(RunId::from_str(&s.to_uppercase()).expect("upper"), id);

    // Crockford aliases: i/l -> 1, o -> 0.
    let zero = RunId::from_bytes(&[0u8; 18]).expect("ok");
    let aliased = format!("{}{}", "o".repeat(28), "O");
    assert_eq!(RunId::from_str(&aliased).expect("alias"), zero);
    let mut one_bytes = [0u8; 18];
    // '1' at position 0 = 00001 in the top five bits => byte0 = 0b0000_1000.
    one_bytes[0] = 0b0000_1000;
    let one = RunId::from_bytes(&one_bytes).expect("ok");
    assert_eq!(
        RunId::from_str(&format!("i{}", "0".repeat(28))).expect("i"),
        one
    );
    assert_eq!(
        RunId::from_str(&format!("L{}", "0".repeat(28))).expect("L"),
        one
    );
}

#[test]
fn run_id_from_str_rejects_bad_input() {
    let s = RunId::new_v7(PartitionId(1)).to_string();
    assert!(RunId::from_str(&s[..28]).is_err(), "too short");
    assert!(RunId::from_str(&format!("{s}0")).is_err(), "too long");
    assert!(RunId::from_str("").is_err(), "empty");
    let mut bad = s.clone();
    bad.replace_range(5..6, "u");
    assert!(RunId::from_str(&bad).is_err(), "'u' is not in the alphabet");
    let mut bad = s.clone();
    bad.replace_range(5..6, "-");
    assert!(RunId::from_str(&bad).is_err(), "punctuation");
    // Non-zero padding bits in the final group (the last char carries 4 data bits + 1 pad bit).
    let ones = format!("{}z", "z".repeat(28));
    assert!(RunId::from_str(&ones).is_err(), "non-zero pad bit");
    // A 30-char string decodes to more than 18 bytes.
    assert!(RunId::from_str(&"0".repeat(30)).is_err());
    // Multi-byte UTF-8 of the right byte length.
    assert!(RunId::from_str(&format!("é{}", "0".repeat(27))).is_err());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn run_id_string_round_trip(p in any::<u16>(), raw in any::<[u8; 16]>()) {
        let id = RunId::new(PartitionId(p), uuid::Uuid::from_bytes(raw));
        let s = id.to_string();
        prop_assert_eq!(s.len(), 29);
        prop_assert!(s.chars().all(|c| "0123456789abcdefghjkmnpqrstvwxyz".contains(c)));
        prop_assert_eq!(RunId::from_str(&s).unwrap(), id);
        prop_assert_eq!(format!("{id:?}"), format!("RunId({s})"));
    }

    #[test]
    fn run_id_v7_round_trip(p in any::<u16>()) {
        let id = RunId::new_v7(PartitionId(p));
        prop_assert_eq!(RunId::from_str(&id.to_string()).unwrap(), id);
        prop_assert_eq!(id.partition(), PartitionId(p));
    }
}

#[test]
fn attempt_token_ordering_is_epoch_then_n() {
    let a = AttemptToken {
        epoch: Epoch(1),
        n: 9,
    };
    let b = AttemptToken {
        epoch: Epoch(2),
        n: 1,
    };
    let c = AttemptToken {
        epoch: Epoch(2),
        n: 2,
    };
    assert!(a < b);
    assert!(b < c);
    assert!(a < c);
}

#[test]
fn partition_count_bounds() {
    assert!(PartitionCount::new(0).is_none());
    let p = pc(16);
    assert_eq!(p.get(), 16);
    let ids: Vec<_> = p.ids().collect();
    assert_eq!(ids, (0..16).map(PartitionId).collect::<Vec<_>>());
    assert!(p.contains(PartitionId(0)));
    assert!(p.contains(PartitionId(15)));
    assert!(!p.contains(PartitionId(16)));
    assert_eq!(PartitionCount::SINGLE_NODE_DEFAULT, 16);
    assert_eq!(PartitionCount::CLUSTER_DEFAULT, 64);
    assert_eq!(PartitionId(5).as_i32(), 5);
    assert_eq!(PartitionId(5).index(), 5usize);
}

#[test]
fn offset_arithmetic() {
    assert_eq!(Offset::ZERO, Offset(0));
    assert_eq!(Offset(4).next(), Offset(5));
    assert_eq!(Offset(4) + 3, Offset(7));
    assert_eq!(Offset(7) - Offset(4), 3u64);
    assert_eq!(Offset(9).as_i64(), 9);
    assert_eq!(Offset::from_i64(9), Some(Offset(9)));
    assert_eq!(Offset::from_i64(-1), None);
    assert_eq!(Offset::default(), Offset::ZERO);
    assert_eq!(Epoch::default(), Epoch(0));
}

// ---------------------------------------------------------------------------
// routing
// ---------------------------------------------------------------------------

/// The fixed input set for the golden file. Changing this list requires regenerating the file.
fn golden_inputs() -> Vec<(String, Vec<u8>, u16)> {
    let workflows = ["", "orders", "payments.v2", "überweisung-流程-🚀"];
    let keys: [&[u8]; 3] = [b"", b"order-42", &[0x00, 0xff, 0x10, 0x80]];
    let ps = [1u16, 16, 64, 1000];
    let mut out = Vec::new();
    for w in workflows {
        for k in keys {
            for p in ps {
                // Keep the set at a reasonable size but cover every P for every workflow.
                out.push((w.to_string(), k.to_vec(), p));
            }
        }
    }
    out
}

fn render_golden() -> String {
    let mut s = String::from(HEADER);
    s.push('\n');
    for (w, k, p) in golden_inputs() {
        let part = partition_for_key(&w, &k, pc(p));
        s.push_str(&format!("{w}\t{}\t{p}\t{}\n", hex(&k), part.0));
    }
    s
}

#[test]
fn routing_golden_vectors() {
    if std::env::var_os("OSTRO_BLESS").is_some() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/routing.txt");
        std::fs::write(path, render_golden()).expect("write golden");
        return;
    }
    let mut n = 0;
    for line in GOLDEN.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(cols.len(), 4, "malformed golden line: {line:?}");
        let key = unhex(cols[1]);
        let p: u16 = cols[2].parse().expect("P");
        let expected: u16 = cols[3].parse().expect("partition");
        assert_eq!(
            partition_for_key(cols[0], &key, pc(p)),
            PartitionId(expected),
            "routing changed for {line:?}"
        );
        n += 1;
    }
    assert!(n >= 24, "golden file has only {n} vectors");
    assert_eq!(
        GOLDEN,
        render_golden(),
        "golden file out of sync with golden_inputs()"
    );
}

#[test]
fn routing_layout_computed_in_test() {
    // Independent of the golden file: xxh3_64(u32 LE len(workflow) ‖ workflow ‖ key) % P.
    let w = "orders";
    let k = b"order-42";
    let mut buf = Vec::new();
    buf.extend_from_slice(&(w.len() as u32).to_le_bytes());
    buf.extend_from_slice(w.as_bytes());
    buf.extend_from_slice(k);
    for p in [1u16, 16, 64, 1000] {
        let expected = (xxhash_rust::xxh3::xxh3_64(&buf) % u64::from(p)) as u16;
        assert_eq!(partition_for_key(w, k, pc(p)), PartitionId(expected));
    }
    // The length prefix is part of the hashed bytes: ("ab", "c") hashes 2u32 ‖ "abc".
    let ab_c = partition_for_key("ab", b"c", pc(1000));
    let mut x = Vec::new();
    x.extend_from_slice(&2u32.to_le_bytes());
    x.extend_from_slice(b"abc");
    assert_eq!(u64::from(ab_c.0), xxhash_rust::xxh3::xxh3_64(&x) % 1000);
}

#[test]
fn routing_new_run_uses_uuid_bytes() {
    let uuid = uuid::Uuid::from_bytes([7u8; 16]);
    let expected = (xxhash_rust::xxh3::xxh3_64(uuid.as_bytes()) % 64) as u16;
    assert_eq!(partition_for_new_run(&uuid, pc(64)), PartitionId(expected));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn routing_is_in_range(w in ".{0,40}", k in proptest::collection::vec(any::<u8>(), 0..64), p in 1u16..=2000) {
        let part = partition_for_key(&w, &k, pc(p));
        prop_assert!(pc(p).contains(part));
        prop_assert!(pc(p).contains(partition_for_new_run(&uuid::Uuid::now_v7(), pc(p))));
    }
}

// ---------------------------------------------------------------------------
// payload
// ---------------------------------------------------------------------------

#[test]
fn inline_limit_boundary() {
    assert_eq!(INLINE_PAYLOAD_LIMIT, 256 * 1024);
    let at = PayloadRef::inline(Bytes::from(vec![0u8; INLINE_PAYLOAD_LIMIT]));
    assert_eq!(at.inline_len(), Some(INLINE_PAYLOAD_LIMIT));
    assert!(!at.is_oversize_inline());
    let over = PayloadRef::inline(Bytes::from(vec![0u8; INLINE_PAYLOAD_LIMIT + 1]));
    assert!(over.is_oversize_inline());
    let blob = PayloadRef::blob([1u8; 32], 10 << 20);
    assert_eq!(blob.inline_len(), None);
    assert!(!blob.is_oversize_inline());
}

#[test]
fn payload_ref_prost_round_trip() {
    for v in [
        PayloadRef::inline(Bytes::from_static(b"hello")),
        PayloadRef::inline(Bytes::new()),
        PayloadRef::blob([0xcd; 32], 1_000_000),
    ] {
        let enc = v.encode_to_vec();
        let dec = PayloadRef::decode(enc.as_slice()).expect("decode");
        assert_eq!(dec, v);
    }
    let blob = PayloadRef::blob([0xcd; 32], 5);
    match blob.kind {
        Some(payload_ref::Kind::Blob(b)) => {
            assert_eq!(b.sha256, vec![0xcd; 32]);
            assert_eq!(b.len, 5);
        }
        other => panic!("expected blob, got {other:?}"),
    }
}
