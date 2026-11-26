//! 14-03 Task 1: the API token codec (D-P14-4): `ostro_otk_<16 base32>_<52 base32>`.

use std::collections::HashSet;

use aws_lc_rs::rand::SystemRandom;
use ostro_server::p14::tokens::{
    self, BEARER_PREFIX, ID_CHARS, ID_PREFIX, MAX_BEARER_LEN, ParseError, SECRET_CHARS, Secret,
    TokenId,
};
use proptest::prelude::*;

const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

fn fresh() -> (TokenId, Secret, String) {
    let (id, secret, bearer) = tokens::generate(&SystemRandom::new()).expect("generate");
    (id, secret, bearer.expose().to_owned())
}

#[test]
fn generated_bearer_has_the_documented_shape() {
    let (id, secret, bearer) = fresh();
    assert!(id.as_str().starts_with(ID_PREFIX), "{}", id.as_str());
    assert_eq!(id.as_str().len(), ID_PREFIX.len() + ID_CHARS);
    assert_eq!(secret.expose().len(), SECRET_CHARS);
    assert_eq!(
        bearer,
        format!("{BEARER_PREFIX}{}_{}", id.as_str(), secret.expose())
    );
    assert_eq!(bearer.len(), 6 + 4 + 16 + 1 + 52);
    assert!(bearer.len() <= MAX_BEARER_LEN);
    for c in id.as_str()[ID_PREFIX.len()..]
        .bytes()
        .chain(secret.expose().bytes())
    {
        assert!(ALPHABET.contains(&c), "{c}");
    }
    // Debug output never shows the secret.
    let dbg = format!("{secret:?}");
    assert!(!dbg.contains(secret.expose()), "{dbg}");
}

#[test]
fn round_trip_parse_and_verify() {
    for _ in 0..64 {
        let (id, secret, bearer) = fresh();
        let (pid, psecret) = tokens::parse(&bearer).expect("parse");
        assert_eq!(pid, id);
        assert_eq!(psecret.expose(), secret.expose());
        let stored = tokens::hash(&secret);
        assert!(tokens::verify(&stored, &psecret));
        // The stored hash is sha256 of the decoded secret bytes (32 bytes).
        let bytes = tokens::base32_decode(secret.expose()).expect("decode");
        assert_eq!(bytes.len(), 32);
        use sha2::Digest;
        let want: [u8; 32] = sha2::Sha256::digest(&bytes).into();
        assert_eq!(stored, want);
    }
}

#[test]
fn base32_known_vectors() {
    // RFC 4648 §10, lowercase, no padding.
    let cases: &[(&[u8], &str)] = &[
        (b"", ""),
        (b"f", "my"),
        (b"fo", "mzxq"),
        (b"foo", "mzxw6"),
        (b"foob", "mzxw6yq"),
        (b"fooba", "mzxw6ytb"),
        (b"foobar", "mzxw6ytboi"),
    ];
    for (raw, enc) in cases {
        assert_eq!(tokens::base32_encode(raw), *enc);
        assert_eq!(tokens::base32_decode(enc).as_deref(), Some(*raw));
    }
    // Non-canonical trailing bits, bad lengths, uppercase, padding are refused.
    for bad in [
        "mz",
        "MZXW6",
        "mzxw6=",
        "m",
        "mzx",
        "mzxw6yt",
        "mzxw6ytbo1",
        "my======",
    ] {
        assert_eq!(tokens::base32_decode(bad), None, "{bad}");
    }
}

#[test]
fn parse_rejects_malformed_bearers() {
    let (id, secret, bearer) = fresh();
    let s = secret.expose();
    let i = &id.as_str()[ID_PREFIX.len()..];
    let long = format!("{bearer}{}", "a".repeat(MAX_BEARER_LEN));
    let cases: Vec<(String, ParseError)> = vec![
        (String::new(), ParseError::Prefix),
        (format!(" {bearer}"), ParseError::Prefix),
        (format!("{bearer} "), ParseError::Secret),
        (format!("{bearer}\n"), ParseError::Secret),
        (bearer.to_uppercase(), ParseError::Prefix),
        (
            format!("ostro_otk_{}_{}", i.to_uppercase(), s),
            ParseError::Id,
        ),
        (
            format!("ostro_otk_{i}_{}", s.to_uppercase()),
            ParseError::Secret,
        ),
        (format!("ostro_otk_{i}__{s}"), ParseError::Id),
        (format!("ostro_otk_{i}_{s}_"), ParseError::Id),
        (format!("ostro_otk__{i}_{s}"), ParseError::Id),
        (format!("ostro_otk_{i}_"), ParseError::Secret),
        (format!("ostro__{s}"), ParseError::Id),
        (format!("ostro_otk_{i}"), ParseError::Id),
        (format!("ostro_wkr_{i}_{s}"), ParseError::NotApiToken),
        (format!("ostro_njt_{i}_{s}"), ParseError::NotApiToken),
        (format!("ostro_otk_{}_{s}", &i[1..]), ParseError::Id),
        (format!("ostro_otk_{i}a_{s}"), ParseError::Id),
        (format!("ostro_otk_{i}_{}", &s[1..]), ParseError::Secret),
        (format!("ostro_otk_{i}_{s}a"), ParseError::Secret),
        (format!("ostro_otk_{i}_{}1", &s[..51]), ParseError::Secret),
        (format!("Bearer {bearer}"), ParseError::Prefix),
        (format!("{}.{}", id.as_str(), s), ParseError::Prefix),
        (long, ParseError::TooLong),
    ];
    for (input, want) in cases {
        assert_eq!(tokens::parse(&input).err(), Some(want), "{input:?}");
    }
    assert!(tokens::parse(&bearer).is_ok());
}

#[test]
fn ids_are_unique() {
    let rng = SystemRandom::new();
    let mut seen = HashSet::new();
    for _ in 0..10_000 {
        let (id, _, _) = tokens::generate(&rng).expect("generate");
        assert!(
            seen.insert(id.as_str().to_owned()),
            "duplicate {}",
            id.as_str()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Arbitrary input never panics the parser.
    #[test]
    fn parse_never_panics(s in any::<String>()) {
        let _ = tokens::parse(&s);
    }

    /// Bearer-shaped input (right prefix and lengths, arbitrary chars) never panics either.
    #[test]
    fn parse_never_panics_on_near_misses(i in "[a-z2-9_A-Z]{0,20}", s in "[a-z0-9_=]{0,60}") {
        let _ = tokens::parse(&format!("ostro_otk_{i}_{s}"));
    }

    /// Any single-character change of the secret no longer authenticates: either the bearer
    /// stops parsing or the hash differs.
    #[test]
    fn single_char_mutation_fails(pos in 0usize..SECRET_CHARS, c in prop::sample::select(ALPHABET.to_vec())) {
        let (id, secret, _) = fresh();
        let stored = tokens::hash(&secret);
        let mut m = secret.expose().as_bytes().to_vec();
        prop_assume!(m[pos] != c);
        m[pos] = c;
        let mutated = format!("ostro_{}_{}", id.as_str(), String::from_utf8(m).unwrap());
        match tokens::parse(&mutated) {
            Err(_) => {}
            Ok((_, s)) => prop_assert!(!tokens::verify(&stored, &s)),
        }
    }

    /// generate → parse → verify always holds.
    #[test]
    fn generate_parse_verify(_x in 0u8..4) {
        let (_, secret, bearer) = fresh();
        let (_, s) = tokens::parse(&bearer).unwrap();
        prop_assert!(tokens::verify(&tokens::hash(&secret), &s));
    }

    /// The hand-written base32 codec round-trips arbitrary bytes.
    #[test]
    fn base32_round_trips(b in prop::collection::vec(any::<u8>(), 0..64)) {
        let e = tokens::base32_encode(&b);
        prop_assert_eq!(tokens::base32_decode(&e), Some(b));
    }
}
