//! Plan 14-05 (SEC-03, D-P14-10): module signature verification.
//!
//! minisign (legacy `Ed` and prehashed `ED`, via `minisign-verify`) and cosign keyed ECDSA
//! P-256 / SHA-256 (raw base64 DER or a Sigstore bundle v0.3, via aws-lc-rs). Unit cases use
//! fresh keys from aws-lc-rs (an Ed25519 test signer producing legacy `Ed` minisign
//! signatures, and a P-256 cosign signer); the prehashed `ED` path is covered by the committed
//! rsign2 interop vector in `tests/fixtures/signing` (see its README).

use std::path::PathBuf;

use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{ECDSA_P256_SHA256_ASN1_SIGNING, EcdsaKeyPair, Ed25519KeyPair, KeyPair};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ostro_meta::ext::{ModuleSig, SIG_ALG_COSIGN_P256, SIG_ALG_MINISIGN_ED};
use ostro_server::p14::signing::{
    Alg, KeyFamily, MAX_COMMENT_BYTES, MAX_SIG_BYTES, MAX_SIGS, ModuleSigEvidence, SigError,
    TrustedKey, key_fingerprint, key_id_hex, parse_signature, parse_trusted_key, reverify,
    verify_module,
};
use proptest::prelude::*;

// ---------------------------------------------------------------------------------------
// Test signers
// ---------------------------------------------------------------------------------------

/// Legacy `Ed` minisign signer over an aws-lc-rs Ed25519 key.
struct MiniSigner {
    kp: Ed25519KeyPair,
    key_id: [u8; 8],
}

impl MiniSigner {
    fn new(key_id: [u8; 8]) -> Self {
        let doc = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("ed25519 keygen");
        let kp = Ed25519KeyPair::from_pkcs8(doc.as_ref()).expect("ed25519 pkcs8");
        Self { kp, key_id }
    }

    fn pk_b64(&self) -> String {
        let mut bin = b"Ed".to_vec();
        bin.extend_from_slice(&self.key_id);
        bin.extend_from_slice(self.kp.public_key().as_ref());
        B64.encode(bin)
    }

    fn pub_file(&self) -> String {
        format!(
            "untrusted comment: minisign public key {}\n{}\n",
            hex_upper_le(&self.key_id),
            self.pk_b64()
        )
    }

    fn trusted(&self) -> TrustedKey {
        parse_trusted_key(&self.pub_file()).expect("own key parses")
    }

    fn sign_with_comment(&self, blob: &[u8], trusted_comment: &str) -> String {
        let sig = self.kp.sign(blob);
        let mut bin1 = b"Ed".to_vec();
        bin1.extend_from_slice(&self.key_id);
        bin1.extend_from_slice(sig.as_ref());
        let mut global_msg = sig.as_ref().to_vec();
        global_msg.extend_from_slice(trusted_comment.as_bytes());
        let global = self.kp.sign(&global_msg);
        format!(
            "untrusted comment: signature from ostro test signer\n{}\ntrusted comment: {}\n{}\n",
            B64.encode(bin1),
            trusted_comment,
            B64.encode(global.as_ref())
        )
    }

    fn sign(&self, blob: &[u8]) -> String {
        self.sign_with_comment(blob, "timestamp:1700000000\tfile:module.wasm")
    }
}

fn hex_upper_le(id: &[u8; 8]) -> String {
    id.iter().rev().map(|b| format!("{b:02X}")).collect()
}

/// cosign keyed signer (ECDSA P-256 / SHA-256, ASN.1 DER signatures).
struct CosignSigner {
    kp: EcdsaKeyPair,
}

impl CosignSigner {
    fn new() -> Self {
        let rng = SystemRandom::new();
        let doc = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
            .expect("p256 keygen");
        let kp = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, doc.as_ref())
            .expect("p256 pkcs8");
        Self { kp }
    }

    fn spki_der(&self) -> Vec<u8> {
        self.kp
            .public_key()
            .as_der()
            .expect("spki der")
            .as_ref()
            .to_vec()
    }

    fn pem(&self) -> String {
        let b64 = B64.encode(self.spki_der());
        let mut out = String::from("-----BEGIN PUBLIC KEY-----\n");
        for chunk in b64.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(chunk).expect("ascii"));
            out.push('\n');
        }
        out.push_str("-----END PUBLIC KEY-----\n");
        out
    }

    fn trusted(&self) -> TrustedKey {
        parse_trusted_key(&self.pem()).expect("own key parses")
    }

    fn sign_der(&self, blob: &[u8]) -> Vec<u8> {
        self.kp
            .sign(&SystemRandom::new(), blob)
            .expect("p256 sign")
            .as_ref()
            .to_vec()
    }

    fn sign_b64(&self, blob: &[u8]) -> String {
        B64.encode(self.sign_der(blob))
    }

    fn hint(&self) -> String {
        B64.encode(sha256(&self.spki_der()))
    }

    fn bundle(&self, blob: &[u8], digest: Option<[u8; 32]>) -> String {
        let mut msg = serde_json::json!({ "signature": self.sign_b64(blob) });
        if let Some(d) = digest {
            msg["messageDigest"] =
                serde_json::json!({ "algorithm": "SHA2_256", "digest": B64.encode(d) });
        }
        serde_json::json!({
            "mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
            "verificationMaterial": { "publicKey": { "hint": self.hint() } },
            "messageSignature": msg,
        })
        .to_string()
    }
}

fn sha256(b: &[u8]) -> [u8; 32] {
    let d = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, b);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

fn blob() -> Vec<u8> {
    (0..10_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect()
}

fn tampered(b: &[u8]) -> Vec<u8> {
    let mut t = b.to_vec();
    t[17] ^= 0x01;
    t
}

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signing")
}

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures().join(name)).expect("fixture")
}

fn ps(raw: &str) -> ostro_server::p14::signing::ParsedSig {
    parse_signature(raw.as_bytes()).expect("signature parses")
}

// ---------------------------------------------------------------------------------------
// minisign
// ---------------------------------------------------------------------------------------

#[test]
fn minisign_legacy_verifies_with_matching_key() {
    let s = MiniSigner::new([1, 2, 3, 4, 5, 6, 7, 8]);
    let b = blob();
    let sig = ps(&s.sign(&b));
    assert_eq!(sig.alg(), Alg::MinisignEd);
    assert_eq!(sig.key_id(), &[1, 2, 3, 4, 5, 6, 7, 8]);
    let ev = verify_module(&b, &[sig], &[s.trusted()]).expect("verifies");
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].alg, Alg::MinisignEd);
    assert_eq!(ev[0].key_id, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(ev[0].key_fingerprint, key_fingerprint(&s.trusted()));
}

#[test]
fn minisign_wrong_key_id_is_unknown_key() {
    let s = MiniSigner::new([1; 8]);
    let other = MiniSigner::new([2; 8]);
    let b = blob();
    let err = verify_module(&b, &[ps(&s.sign(&b))], &[other.trusted()]).unwrap_err();
    assert_eq!(err, SigError::UnknownKey);
    let err = verify_module(&b, &[ps(&s.sign(&b))], &[]).unwrap_err();
    assert_eq!(err, SigError::UnknownKey);
}

#[test]
fn minisign_tampered_blob_is_bad_signature() {
    let s = MiniSigner::new([3; 8]);
    let b = blob();
    let err = verify_module(&tampered(&b), &[ps(&s.sign(&b))], &[s.trusted()]).unwrap_err();
    assert_eq!(err, SigError::BadSignature);
}

#[test]
fn minisign_tampered_trusted_comment_is_bad_signature() {
    let s = MiniSigner::new([4; 8]);
    let b = blob();
    let text = s.sign_with_comment(&b, "timestamp:1700000000\tfile:a.wasm");
    let forged = text.replace("file:a.wasm", "file:b.wasm");
    assert_ne!(text, forged);
    let err = verify_module(&b, &[ps(&forged)], &[s.trusted()]).unwrap_err();
    assert_eq!(err, SigError::BadSignature);
}

#[test]
fn minisign_same_key_id_different_key_is_bad_signature_not_unknown() {
    // Key ids are matched before verification; a key with the right id but the wrong
    // public key is tried and fails.
    let s = MiniSigner::new([5; 8]);
    let impostor = MiniSigner::new([5; 8]);
    let b = blob();
    let err = verify_module(&b, &[ps(&s.sign(&b))], &[impostor.trusted()]).unwrap_err();
    assert_eq!(err, SigError::BadSignature);
    // With both keys trusted, the genuine one is found.
    let ev = verify_module(&b, &[ps(&s.sign(&b))], &[impostor.trusted(), s.trusted()])
        .expect("genuine key verifies");
    assert_eq!(ev[0].key_fingerprint, key_fingerprint(&s.trusted()));
}

#[test]
fn trusted_key_parses_bare_and_file_forms() {
    let s = MiniSigner::new([0x52, 0xEB, 0x6A, 0xA7, 0x47, 0xBB, 0x7D, 0xEE]);
    let bare = parse_trusted_key(&s.pk_b64()).expect("bare RW… key");
    let file = parse_trusted_key(&s.pub_file()).expect("pub file");
    assert_eq!(bare.alg_family, KeyFamily::Minisign);
    assert_eq!(bare.key_id, file.key_id);
    assert_eq!(bare.public_key, file.public_key);
    assert_eq!(bare.public_key.len(), 32);
    assert_eq!(bare.public_key, s.kp.public_key().as_ref());
    assert!(file.comment.starts_with("minisign public key"));
    assert_eq!(key_id_hex(&bare), "EE7DBB47A76AEB52");
    assert_eq!(key_fingerprint(&bare), key_fingerprint(&file));
}

// ---------------------------------------------------------------------------------------
// cosign keyed
// ---------------------------------------------------------------------------------------

#[test]
fn cosign_raw_base64_der_verifies() {
    let c = CosignSigner::new();
    let b = blob();
    let k = c.trusted();
    assert_eq!(k.alg_family, KeyFamily::Cosign);
    assert_eq!(k.public_key, c.spki_der());
    assert_eq!(k.key_id, sha256(&c.spki_der())[..8].to_vec());
    assert_eq!(key_id_hex(&k).len(), 16);
    let sig = ps(&c.sign_b64(&b));
    assert_eq!(sig.alg(), Alg::CosignP256);
    let ev = verify_module(&b, &[sig], std::slice::from_ref(&k)).expect("verifies");
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].alg, Alg::CosignP256);
    assert_eq!(ev[0].key_id, k.key_id);
    assert_eq!(ev[0].key_fingerprint, key_fingerprint(&k));
    // Trailing newline, as in a .sig file written by cosign.
    let with_nl = format!("{}\n", c.sign_b64(&b));
    verify_module(&b, &[ps(&with_nl)], &[k]).expect("newline-terminated sig verifies");
}

#[test]
fn cosign_tampered_blob_and_wrong_key() {
    let c = CosignSigner::new();
    let other = CosignSigner::new();
    let b = blob();
    let sig = ps(&c.sign_b64(&b));
    assert_eq!(
        verify_module(&tampered(&b), std::slice::from_ref(&sig), &[c.trusted()]).unwrap_err(),
        SigError::BadSignature
    );
    assert_eq!(
        verify_module(&b, std::slice::from_ref(&sig), &[other.trusted()]).unwrap_err(),
        SigError::BadSignature
    );
    assert_eq!(
        verify_module(&b, &[sig], &[MiniSigner::new([9; 8]).trusted()]).unwrap_err(),
        SigError::UnknownKey
    );
}

#[test]
fn cosign_bundle_v03_verifies_with_and_without_digest() {
    let c = CosignSigner::new();
    let b = blob();
    let with = ps(&c.bundle(&b, Some(sha256(&b))));
    assert_eq!(with.alg(), Alg::CosignP256);
    assert_eq!(with.hint(), Some(c.hint().as_str()));
    verify_module(&b, &[with], &[c.trusted()]).expect("bundle with digest verifies");
    let without = ps(&c.bundle(&b, None));
    verify_module(&b, &[without], &[c.trusted()]).expect("bundle without digest verifies");
}

#[test]
fn cosign_bundle_digest_mismatch() {
    let c = CosignSigner::new();
    let b = blob();
    let sig = ps(&c.bundle(&b, Some(sha256(b"something else"))));
    assert_eq!(
        verify_module(&b, &[sig], &[c.trusted()]).unwrap_err(),
        SigError::DigestMismatch
    );
}

#[test]
fn cosign_keyless_bundle_is_unsupported() {
    let keyless = serde_json::json!({
        "mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
        "verificationMaterial": {
            "certificate": { "rawBytes": B64.encode(b"not really a certificate") },
            "tlogEntries": [ { "logIndex": "1" } ]
        },
        "messageSignature": { "signature": B64.encode([0x30, 0x06, 2, 1, 1, 2, 1, 1]) }
    })
    .to_string();
    assert_eq!(
        parse_signature(keyless.as_bytes()).unwrap_err(),
        SigError::KeylessUnsupported
    );
}

#[test]
fn cosign_bundle_malformed_shapes_are_typed_errors() {
    for raw in [
        r#"{}"#,
        r#"{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json"}"#,
        r#"{"mediaType":"text/plain","verificationMaterial":{"publicKey":{}},"messageSignature":{"signature":"MAY="}}"#,
        r#"{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json","verificationMaterial":{"publicKey":{"hint":"x"}},"messageSignature":{"signature":"!!"}}"#,
        r#"{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json","verificationMaterial":{"publicKey":{"hint":"x"}},"messageSignature":{"signature":"MAY=","messageDigest":{"algorithm":"SHA2_512","digest":"AAAA"}}}"#,
        r#"{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json","verificationMaterial":{"publicKey":{"hint":"x"}},"dsseEnvelope":{}}"#,
        r#"[1,2,3"#,
    ] {
        match parse_signature(raw.as_bytes()) {
            Err(SigError::Malformed(_)) => {}
            other => panic!("{raw}: expected Malformed, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Fingerprints and reverify
// ---------------------------------------------------------------------------------------

fn to_stored(ev: &[ModuleSigEvidence]) -> Vec<ModuleSig> {
    ev.iter()
        .map(|e| e.to_module_sig(1_700_000_000_000, "otk_test"))
        .collect()
}

#[test]
fn fingerprints_differ_for_same_key_id() {
    let a = MiniSigner::new([7; 8]);
    let b = MiniSigner::new([7; 8]);
    assert_eq!(a.trusted().key_id, b.trusted().key_id);
    assert_ne!(key_fingerprint(&a.trusted()), key_fingerprint(&b.trusted()));
    // sha256(alg ‖ public_key) with the key family's alg byte.
    let mut pre = vec![SIG_ALG_MINISIGN_ED];
    pre.extend_from_slice(&a.trusted().public_key);
    assert_eq!(key_fingerprint(&a.trusted()), sha256(&pre));
    let c = CosignSigner::new();
    let mut pre = vec![SIG_ALG_COSIGN_P256];
    pre.extend_from_slice(&c.spki_der());
    assert_eq!(key_fingerprint(&c.trusted()), sha256(&pre));
}

#[test]
fn evidence_round_trips_through_module_sig_and_reverifies() {
    let s = MiniSigner::new([8; 8]);
    let c = CosignSigner::new();
    let b = blob();
    let keys = [s.trusted(), c.trusted()];
    let ev = verify_module(
        &b,
        &[ps(&s.sign(&b)), ps(&c.bundle(&b, Some(sha256(&b))))],
        &keys,
    )
    .expect("verifies");
    assert_eq!(ev.len(), 2);
    let stored = to_stored(&ev);
    assert_eq!(stored[0].alg, SIG_ALG_MINISIGN_ED);
    assert_eq!(stored[1].alg, SIG_ALG_COSIGN_P256);
    assert_eq!(stored[0].added_at_ms, 1_700_000_000_000);
    assert_eq!(stored[0].added_by, "otk_test");
    let again = reverify(&b, &stored, &keys).expect("reverifies");
    assert_eq!(again, ev);
    // Meta round trip of the trusted keys.
    for k in &keys {
        assert_eq!(&TrustedKey::from_meta(&k.to_meta()).expect("from meta"), k);
    }
}

#[test]
fn reverify_requires_fingerprint_not_just_key_id() {
    let s = MiniSigner::new([6; 8]);
    let same_id = MiniSigner::new([6; 8]);
    let b = blob();
    let stored = to_stored(&verify_module(&b, &[ps(&s.sign(&b))], &[s.trusted()]).unwrap());
    assert_eq!(
        reverify(&b, &stored, &[same_id.trusted()]).unwrap_err(),
        SigError::UnknownKey
    );
    assert_eq!(
        reverify(&b, &stored, &[]).unwrap_err(),
        SigError::UnknownKey
    );
}

#[test]
fn reverify_over_tampered_bytes_fails() {
    let s = MiniSigner::new([6; 8]);
    let c = CosignSigner::new();
    let b = blob();
    let keys = [s.trusted(), c.trusted()];
    let stored =
        to_stored(&verify_module(&b, &[ps(&s.sign(&b)), ps(&c.sign_b64(&b))], &keys).unwrap());
    assert_eq!(
        reverify(&tampered(&b), &stored, &keys).unwrap_err(),
        SigError::BadSignature
    );
    // A stored signature whose bytes were altered.
    let mut bad = stored.clone();
    bad[1].sig[10] ^= 0x40;
    bad.truncate(2);
    bad.remove(0);
    match reverify(&b, &bad, &keys) {
        Err(SigError::BadSignature) | Err(SigError::Malformed(_)) => {}
        other => panic!("altered stored sig: {other:?}"),
    }
    assert_eq!(reverify(&b, &[], &keys).unwrap_err(), SigError::Unsigned);
}

// ---------------------------------------------------------------------------------------
// Multiple signatures and limits
// ---------------------------------------------------------------------------------------

#[test]
fn multiple_signatures_valid_if_one_verifies_and_evidence_lists_only_valid() {
    let s = MiniSigner::new([1; 8]);
    let untrusted = MiniSigner::new([2; 8]);
    let c = CosignSigner::new();
    let b = blob();
    let sigs = [
        ps(&untrusted.sign(&b)),    // unknown key
        ps(&s.sign(&b)),            // valid
        ps(&s.sign(&tampered(&b))), // bad signature
        ps(&c.sign_b64(&b)),        // valid
    ];
    let ev = verify_module(&b, &sigs, &[s.trusted(), c.trusted()]).expect("two valid");
    assert_eq!(ev.len(), 2);
    assert_eq!(ev[0].alg, Alg::MinisignEd);
    assert_eq!(ev[0].key_fingerprint, key_fingerprint(&s.trusted()));
    assert_eq!(ev[1].alg, Alg::CosignP256);
    assert_eq!(ev[1].key_fingerprint, key_fingerprint(&c.trusted()));
    // None valid: the most specific failure wins (bad signature over unknown key).
    let err = verify_module(&b, &sigs[..1], &[s.trusted()]).unwrap_err();
    assert_eq!(err, SigError::UnknownKey);
    let err = verify_module(&b, &[sigs[0].clone(), sigs[2].clone()], &[s.trusted()]).unwrap_err();
    assert_eq!(err, SigError::BadSignature);
    assert_eq!(
        verify_module(&b, &[], &[s.trusted()]).unwrap_err(),
        SigError::Unsigned
    );
}

#[test]
fn limits_are_too_large() {
    let s = MiniSigner::new([1; 8]);
    let b = blob();
    let big = vec![b'A'; MAX_SIG_BYTES + 1];
    assert!(matches!(parse_signature(&big), Err(SigError::TooLarge(_))));
    let sig = ps(&s.sign(&b));
    let five = vec![sig.clone(); MAX_SIGS + 1];
    assert!(matches!(
        verify_module(&b, &five, &[s.trusted()]),
        Err(SigError::TooLarge(_))
    ));
    let four = vec![sig; MAX_SIGS];
    verify_module(&b, &four, &[s.trusted()]).expect("four signatures are fine");
    let long = "c".repeat(MAX_COMMENT_BYTES + 1);
    let text = s.sign_with_comment(&b, &long);
    assert!(text.len() < MAX_SIG_BYTES);
    assert!(matches!(
        parse_signature(text.as_bytes()),
        Err(SigError::TooLarge(_))
    ));
    let key = format!("untrusted comment: {long}\n{}\n", s.pk_b64());
    assert!(matches!(
        parse_trusted_key(&key),
        Err(SigError::TooLarge(_))
    ));
}

#[test]
fn malformed_inputs_are_typed_errors() {
    for raw in [
        &b""[..],
        b"   ",
        b"\xff\xfe\x00",
        b"untrusted comment: x\n",
        b"untrusted comment: x\nAAAA\ntrusted comment: y\nAAAA\n",
        b"not base64 at all !!",
        b"MAY=",
        b"-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n",
    ] {
        match parse_signature(raw) {
            Err(SigError::Malformed(_)) => {}
            other => panic!(
                "{:?}: expected Malformed, got {other:?}",
                String::from_utf8_lossy(raw)
            ),
        }
    }
    for text in [
        "",
        "RW",
        "RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1C",
        "untrusted comment: only a comment",
        "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n",
        "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGb9ECWmEzf6FQbrBZ9w7lshQhqowtrbLDFw4rXAxZuE=\n-----END PUBLIC KEY-----\n",
        "-----BEGIN PUBLIC KEY-----\nnot-base64\n",
    ] {
        match parse_trusted_key(text) {
            Err(SigError::Malformed(_)) => {}
            other => panic!("{text:?}: expected Malformed, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Interop vectors (rsign2 0.6.7)
// ---------------------------------------------------------------------------------------

#[test]
fn interop_rsign2_prehashed_vector_verifies() {
    let b = fixture("blob.bin");
    assert_eq!(b.len(), 4096);
    let key = parse_trusted_key(&String::from_utf8(fixture("minisign.pub")).unwrap())
        .expect("rsign2 public key");
    assert_eq!(key_id_hex(&key), "EE7DBB47A76AEB52");
    let sig = parse_signature(&fixture("blob.bin.minisig")).expect("rsign2 signature");
    assert_eq!(sig.alg(), Alg::MinisignEdPrehashed);
    let ev = verify_module(&b, std::slice::from_ref(&sig), std::slice::from_ref(&key))
        .expect("rsign2 vector verifies");
    assert_eq!(ev[0].alg, Alg::MinisignEdPrehashed);
    assert_eq!(
        reverify(&b, &to_stored(&ev), std::slice::from_ref(&key)).expect("reverify"),
        ev
    );
    assert_eq!(
        verify_module(&tampered(&b), &[sig], &[key]).unwrap_err(),
        SigError::BadSignature
    );
}

#[test]
fn interop_legacy_vector_verifies() {
    let b = fixture("blob.bin");
    let key = parse_trusted_key(&String::from_utf8(fixture("minisign.pub")).unwrap()).unwrap();
    let sig = parse_signature(&fixture("blob.bin.legacy.minisig")).expect("legacy signature");
    assert_eq!(sig.alg(), Alg::MinisignEd);
    let ev = verify_module(&b, &[sig], std::slice::from_ref(&key)).expect("legacy verifies");
    assert_eq!(ev[0].alg, Alg::MinisignEd);
    assert_eq!(ev[0].key_fingerprint, key_fingerprint(&key));
}

// ---------------------------------------------------------------------------------------
// Never panic on arbitrary input
// ---------------------------------------------------------------------------------------

fn near_valid_sig() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..2048),
        (proptest::collection::vec(any::<u8>(), 0..120), ".{0,80}", ".{0,80}").prop_map(
            |(bin, tc, g)| format!(
                "untrusted comment: x\n{}\ntrusted comment: {tc}\n{g}\n",
                B64.encode(bin)
            )
            .into_bytes()
        ),
        (proptest::collection::vec(any::<u8>(), 0..200), ".{0,40}", ".{0,40}").prop_map(
            |(sig, hint, alg)| format!(
                r#"{{"mediaType":"application/vnd.dev.sigstore.bundle.v0.3+json","verificationMaterial":{{"publicKey":{{"hint":{}}}}},"messageSignature":{{"signature":"{}","messageDigest":{{"algorithm":{},"digest":"{}"}}}}}}"#,
                serde_json::Value::String(hint),
                B64.encode(&sig),
                serde_json::Value::String(alg),
                B64.encode(&sig[..sig.len().min(40)])
            )
            .into_bytes()
        ),
        proptest::collection::vec(any::<u8>(), 0..100).prop_map(|b| B64.encode(b).into_bytes()),
    ]
}

fn near_valid_key() -> impl Strategy<Value = String> {
    prop_oneof![
        ".{0,300}",
        proptest::collection::vec(any::<u8>(), 0..120).prop_map(|b| B64.encode(b)),
        proptest::collection::vec(any::<u8>(), 0..120)
            .prop_map(|b| format!("untrusted comment: k\n{}\n", B64.encode(b))),
        proptest::collection::vec(any::<u8>(), 0..160).prop_map(|b| format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----\n",
            B64.encode(b)
        )),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn parse_signature_never_panics(raw in near_valid_sig()) {
        if let Ok(sig) = parse_signature(&raw) {
            let s = MiniSigner::new([1; 8]);
            let _ = verify_module(b"x", &[sig], &[s.trusted()]);
        }
    }

    #[test]
    fn parse_trusted_key_never_panics(text in near_valid_key()) {
        if let Ok(k) = parse_trusted_key(&text) {
            let _ = key_id_hex(&k);
            let _ = key_fingerprint(&k);
            let _ = TrustedKey::from_meta(&k.to_meta());
        }
    }

    #[test]
    fn reverify_never_panics(
        alg in any::<u8>(),
        key_id in proptest::collection::vec(any::<u8>(), 0..16),
        sig in proptest::collection::vec(any::<u8>(), 0..400),
    ) {
        let s = MiniSigner::new([1; 8]);
        let k = s.trusted();
        let stored = ModuleSig {
            alg,
            key_fingerprint: key_fingerprint(&k),
            key_id,
            sig,
            added_at_ms: 0,
            added_by: String::new(),
        };
        prop_assert!(reverify(b"x", &[stored], &[k]).is_err());
    }
}
