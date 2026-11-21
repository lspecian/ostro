//! Cross-SDK vectors (plan 09-06): the Rust reference SDK matches
//! `conformance/vectors/{idempotency,random_f64,json}.json`, which were computed
//! independently (Python stdlib, generator in `conformance/vectors/README.md`). The Python
//! and TypeScript SDKs load the same files.
//!
//! Host test: only pure SDK functions are called (the WIT import stubs are unreachable on
//! native targets).

use std::path::PathBuf;

use ostro_sdk::codec::{decode_json, encode_json};
use ostro_sdk::{f64_from_u64, idempotency_key_for};
use serde_json::Value;

fn load(name: &str) -> Vec<Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../conformance/vectors")
        .join(format!("{name}.json"));
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let doc: Value = serde_json::from_str(&text).expect("vector file is JSON");
    assert_eq!(doc["version"], 1, "{name}: version");
    let cases = doc["cases"].as_array().expect("cases").clone();
    assert!(cases.len() >= 5, "{name}: at least 5 cases");
    cases
}

fn str_of<'a>(v: &'a Value, what: &str) -> &'a str {
    v.as_str()
        .unwrap_or_else(|| panic!("{what} is not a string: {v}"))
}

fn hex_decode(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn idempotency_keys_match_the_vectors() {
    let cases = load("idempotency");
    assert!(cases.len() >= 8);
    let mut saw = (false, false, false); // empty, non-ASCII, >= 1 KiB name
    for c in &cases {
        let run = str_of(&c["input"]["run_id"], "run_id");
        let name = str_of(&c["input"]["name"], "name");
        let want = str_of(&c["expected"], "expected");
        assert_eq!(want.len(), 38, "{want}");
        assert!(want.starts_with("ostro-"), "{want}");
        assert_eq!(
            idempotency_key_for(run, name),
            want,
            "run {run:?} name {name:?}"
        );
        // The 06-09 name is the same function.
        assert_eq!(ostro_sdk::effects::idempotency_key(run, name), want);
        saw.0 |= name.is_empty();
        saw.1 |= !name.is_ascii();
        saw.2 |= name.len() >= 1024;
    }
    assert_eq!(
        saw,
        (true, true, true),
        "coverage: empty, non-ASCII, 1 KiB names"
    );
}

#[test]
fn random_f64_matches_the_vectors_bit_exactly() {
    let cases = load("random_f64");
    assert!(cases.len() >= 13);
    for c in &cases {
        let u: u64 = str_of(&c["input"]["u64"], "u64").parse().expect("u64");
        let u_hex = u64::from_str_radix(str_of(&c["input"]["u64_hex"], "u64_hex"), 16).unwrap();
        assert_eq!(u, u_hex, "u64 and u64_hex disagree");
        let bits = u64::from_str_radix(str_of(&c["expected"]["f64_bits_hex"], "bits"), 16).unwrap();
        let got = f64_from_u64(u);
        assert_eq!(got.to_bits(), bits, "u64 {u}");
        assert!((0.0..1.0).contains(&got), "{got} out of [0, 1)");
        let dec: f64 = str_of(&c["expected"]["f64"], "f64").parse().expect("f64");
        assert_eq!(dec.to_bits(), bits, "decimal form of u64 {u}");
    }
    assert!(f64_from_u64(u64::MAX) < 1.0);
}

#[test]
fn json_codec_matches_the_vectors() {
    let cases = load("json");
    let mut ops = (0, 0, 0);
    for c in &cases {
        let input = &c["input"];
        let note = input["note"].as_str().unwrap_or("");
        match str_of(&input["op"], "op") {
            "encode" => {
                let want = hex_decode(str_of(&c["expected"]["hex"], "hex"));
                let got = encode_json(&input["value"]).expect("encode");
                assert_eq!(hex_encode(&got), hex_encode(&want), "{note}");
                assert!(!got.starts_with(&[0xef, 0xbb, 0xbf]), "BOM: {note}");
                // Round trip.
                let back: Value = decode_json(&got).expect("decode");
                assert_eq!(back, input["value"], "{note}");
                ops.0 += 1;
            }
            "encode_f64" => {
                let bits = u64::from_str_radix(str_of(&input["f64_bits_hex"], "bits"), 16).unwrap();
                let want = hex_decode(str_of(&c["expected"]["hex"], "hex"));
                let got = encode_json(&f64::from_bits(bits)).expect("encode f64");
                assert_eq!(got, want, "{note}");
                ops.1 += 1;
            }
            "decode" => {
                let data = hex_decode(str_of(&input["hex"], "hex"));
                let got: Value = decode_json(&data).expect("decode");
                assert_eq!(got, c["expected"]["value"], "{note}");
                ops.2 += 1;
            }
            other => panic!("unknown op {other}"),
        }
    }
    assert!(ops.0 >= 5 && ops.1 >= 3 && ops.2 >= 3, "coverage {ops:?}");
    // Empty data decodes as null for typed targets too.
    let none: Option<u32> = decode_json(b"").expect("empty");
    assert_eq!(none, None);
}
