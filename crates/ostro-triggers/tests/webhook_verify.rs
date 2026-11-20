//! TRIG-01 (P8-D12): webhook signature verifiers and idempotency extractors.
//!
//! Vector provenance:
//! - GitHub: GitHub docs ("Validating webhook deliveries"), secret `It's a Secret to Everybody`.
//! - Slack: Slack docs ("Verifying requests from Slack"), signing secret
//!   `8f742231b10e8888abcd99yyyzzz85a5`, timestamp 1531420618.
//! - Standard Webhooks: the spec reference libraries' test vector (`msg_p5jXN8AQM9LWM0D4loKWxJek`).
//! - HMAC-SHA1: RFC 2202 test case 2 (`Jefe`).
//! - Stripe and the generic-HMAC combinations: computed in this file with an independent
//!   HMAC-SHA256 built by hand on `sha2` (not through the `hmac` crate the verifier uses).
//!
//! All recalled vendor vectors were re-checked against Python's `hmac` module while writing
//! this file; none needed correcting.

use ostro_triggers::extract::{
    ExtractError, IdempotencyExtractor, InputSelector, MAX_KEY_LEN, default_for,
};
use ostro_triggers::webhook::{
    DEFAULT_TOLERANCE_S, GenericHmacSpec, HmacAlgorithm, HookError, SigEncoding, SignedTemplate,
    VerifiedDelivery, VerifierKind, VerifierSpec, slack_challenge, verify,
};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------------------------
// Independent helpers (deliberately not shared with the implementation).

fn hmac_sha256_by_hand(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let ipad: Vec<u8> = k.iter().map(|b| b ^ 0x36).collect();
    let opad: Vec<u8> = k.iter().map(|b| b ^ 0x5c).collect();
    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(msg);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(inner);
    outer.finalize().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = match chunk.len() {
            3 => (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]),
            2 => (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8),
            _ => u32::from(chunk[0]) << 16,
        };
        out.push(A[(n >> 18) as usize & 63] as char);
        out.push(A[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(A[(n >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(A[n as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn h(pairs: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.as_bytes().to_vec()))
        .collect()
}

fn flip(body: &[u8]) -> Vec<u8> {
    let mut b = body.to_vec();
    b[0] ^= 0x01;
    b
}

/// A proof token for extractor tests, obtained the only way possible: by verifying.
fn proof() -> VerifiedDelivery {
    verify(&VerifierSpec::None, b"", &[], b"", 0).expect("none verifier always passes")
}

// ---------------------------------------------------------------------------------------------
// GitHub

const GH_SECRET: &[u8] = b"It's a Secret to Everybody";
const GH_BODY: &[u8] = b"Hello, World!";
const GH_SIG: &str = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";

#[test]
fn github_vendor_vector_accepted() {
    let hs = h(&[("X-Hub-Signature-256", GH_SIG)]);
    verify(&VerifierSpec::GitHub, GH_SECRET, &hs, GH_BODY, 0).unwrap();
    // Uppercase hex is accepted too.
    let hs = h(&[(
        "x-hub-signature-256",
        &GH_SIG.to_ascii_uppercase().replace("SHA256=", "sha256="),
    )]);
    verify(&VerifierSpec::GitHub, GH_SECRET, &hs, GH_BODY, 0).unwrap();
}

#[test]
fn github_independent_hmac_matches_vendor_vector() {
    assert_eq!(
        format!("sha256={}", hex(&hmac_sha256_by_hand(GH_SECRET, GH_BODY))),
        GH_SIG
    );
}

#[test]
fn github_rejections() {
    let hs = h(&[("x-hub-signature-256", GH_SIG)]);
    assert_eq!(
        verify(&VerifierSpec::GitHub, GH_SECRET, &hs, &flip(GH_BODY), 0),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&VerifierSpec::GitHub, b"wrong secret", &hs, GH_BODY, 0),
        Err(HookError::BadSignature)
    );
    let no_prefix = h(&[("x-hub-signature-256", &GH_SIG["sha256=".len()..])]);
    assert_eq!(
        verify(&VerifierSpec::GitHub, GH_SECRET, &no_prefix, GH_BODY, 0),
        Err(HookError::Malformed)
    );
    let bad_hex = h(&[("x-hub-signature-256", "sha256=zz")]);
    assert_eq!(
        verify(&VerifierSpec::GitHub, GH_SECRET, &bad_hex, GH_BODY, 0),
        Err(HookError::Malformed)
    );
    assert_eq!(
        verify(&VerifierSpec::GitHub, GH_SECRET, &[], GH_BODY, 0),
        Err(HookError::MissingHeader("x-hub-signature-256"))
    );
    // A truncated signature is rejected (verify_slice checks the length).
    let short = h(&[("x-hub-signature-256", &GH_SIG[..GH_SIG.len() - 2])]);
    assert_eq!(
        verify(&VerifierSpec::GitHub, GH_SECRET, &short, GH_BODY, 0),
        Err(HookError::BadSignature)
    );
}

// ---------------------------------------------------------------------------------------------
// Slack

const SLACK_SECRET: &[u8] = b"8f742231b10e8888abcd99yyyzzz85a5";
const SLACK_TS: i64 = 1_531_420_618;
const SLACK_BODY: &[u8] = b"token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c";
const SLACK_SIG: &str = "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503";

fn slack_headers(sig: &str) -> Vec<(String, Vec<u8>)> {
    h(&[
        ("X-Slack-Request-Timestamp", "1531420618"),
        ("X-Slack-Signature", sig),
    ])
}

fn slack() -> VerifierSpec {
    VerifierSpec::Slack {
        tolerance_s: DEFAULT_TOLERANCE_S,
    }
}

#[test]
fn slack_vendor_vector_accepted() {
    verify(
        &slack(),
        SLACK_SECRET,
        &slack_headers(SLACK_SIG),
        SLACK_BODY,
        SLACK_TS,
    )
    .unwrap();
    // Within tolerance on both sides.
    verify(
        &slack(),
        SLACK_SECRET,
        &slack_headers(SLACK_SIG),
        SLACK_BODY,
        SLACK_TS + 300,
    )
    .unwrap();
    verify(
        &slack(),
        SLACK_SECRET,
        &slack_headers(SLACK_SIG),
        SLACK_BODY,
        SLACK_TS - 300,
    )
    .unwrap();
}

#[test]
fn slack_independent_hmac_matches_vendor_vector() {
    let mut msg = b"v0:1531420618:".to_vec();
    msg.extend_from_slice(SLACK_BODY);
    assert_eq!(
        format!("v0={}", hex(&hmac_sha256_by_hand(SLACK_SECRET, &msg))),
        SLACK_SIG
    );
}

#[test]
fn slack_rejections() {
    let hs = slack_headers(SLACK_SIG);
    assert_eq!(
        verify(&slack(), SLACK_SECRET, &hs, SLACK_BODY, SLACK_TS + 301),
        Err(HookError::Stale)
    );
    assert_eq!(
        verify(&slack(), SLACK_SECRET, &hs, SLACK_BODY, SLACK_TS - 301),
        Err(HookError::Stale)
    );
    assert_eq!(
        verify(&slack(), SLACK_SECRET, &hs, &flip(SLACK_BODY), SLACK_TS),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&slack(), b"nope", &hs, SLACK_BODY, SLACK_TS),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(
            &slack(),
            SLACK_SECRET,
            &slack_headers(&SLACK_SIG[3..]),
            SLACK_BODY,
            SLACK_TS
        ),
        Err(HookError::Malformed)
    );
    let bad_ts = h(&[
        ("x-slack-request-timestamp", "soon"),
        ("x-slack-signature", SLACK_SIG),
    ]);
    assert_eq!(
        verify(&slack(), SLACK_SECRET, &bad_ts, SLACK_BODY, SLACK_TS),
        Err(HookError::Malformed)
    );
    let no_ts = h(&[("x-slack-signature", SLACK_SIG)]);
    assert_eq!(
        verify(&slack(), SLACK_SECRET, &no_ts, SLACK_BODY, SLACK_TS),
        Err(HookError::MissingHeader("x-slack-request-timestamp"))
    );
    let no_sig = h(&[("x-slack-request-timestamp", "1531420618")]);
    assert_eq!(
        verify(&slack(), SLACK_SECRET, &no_sig, SLACK_BODY, SLACK_TS),
        Err(HookError::MissingHeader("x-slack-signature"))
    );
}

#[test]
fn slack_url_verification_challenge() {
    let p = proof();
    assert_eq!(
        slack_challenge(&p, br#"{"type":"url_verification","challenge":"x"}"#),
        Some("x".to_string())
    );
    assert_eq!(
        slack_challenge(&p, br#"{"token":"t","challenge":"3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P","type":"url_verification"}"#),
        Some("3eZbrw1aBm2rZgRNFdxV2595E9CY3gmdALWMmHkvFXO7tYXAYM8P".to_string())
    );
    assert_eq!(
        slack_challenge(&p, br#"{"type":"event_callback","challenge":"x"}"#),
        None
    );
    assert_eq!(slack_challenge(&p, br#"{"type":"url_verification"}"#), None);
    assert_eq!(
        slack_challenge(&p, br#"{"type":"url_verification","challenge":7}"#),
        None
    );
    assert_eq!(slack_challenge(&p, b"token=a&b=c"), None);
    assert_eq!(slack_challenge(&p, b""), None);
}

// ---------------------------------------------------------------------------------------------
// Stripe (computed in-test)

const STRIPE_SECRET: &[u8] = b"whsec_test_5WbX5kEWLlfzsGNjH64I8lOOqUB6e8FH";
const STRIPE_BODY: &[u8] = br#"{"id":"evt_1","object":"event","data":{"object":{"id":"cs_9"}}}"#;
const NOW: i64 = 1_763_600_000;

fn stripe() -> VerifierSpec {
    VerifierSpec::Stripe {
        tolerance_s: DEFAULT_TOLERANCE_S,
    }
}

fn stripe_sig(secret: &[u8], t: i64, body: &[u8]) -> String {
    let mut msg = format!("{t}.").into_bytes();
    msg.extend_from_slice(body);
    // The endpoint secret is used as-is: no base64 decoding of `whsec_…`.
    hex(&hmac_sha256_by_hand(secret, &msg))
}

fn stripe_hdr(v: &str) -> Vec<(String, Vec<u8>)> {
    h(&[("Stripe-Signature", v)])
}

#[test]
fn stripe_single_v1_accepted() {
    let good = stripe_sig(STRIPE_SECRET, NOW, STRIPE_BODY);
    let hs = stripe_hdr(&format!("t={NOW},v1={good}"));
    verify(&stripe(), STRIPE_SECRET, &hs, STRIPE_BODY, NOW).unwrap();
}

#[test]
fn stripe_rotation_any_v1_matches() {
    let good = stripe_sig(STRIPE_SECRET, NOW, STRIPE_BODY);
    let bad = stripe_sig(b"whsec_old_secret", NOW, STRIPE_BODY);
    let hs = stripe_hdr(&format!("t={NOW},v1={bad},v1={good}"));
    verify(&stripe(), STRIPE_SECRET, &hs, STRIPE_BODY, NOW).unwrap();
    let hs = stripe_hdr(&format!("t={NOW},v1={good},v1={bad}"));
    verify(&stripe(), STRIPE_SECRET, &hs, STRIPE_BODY, NOW).unwrap();
    // A garbage (non-hex) v1 entry is skipped, not fatal.
    let hs = stripe_hdr(&format!("t={NOW},v1=not-hex,v1={good}"));
    verify(&stripe(), STRIPE_SECRET, &hs, STRIPE_BODY, NOW).unwrap();
}

#[test]
fn stripe_unknown_entries_ignored_and_never_used() {
    let good = stripe_sig(STRIPE_SECRET, NOW, STRIPE_BODY);
    // v0 and the unsigned debug entries carry a *valid* signature for a different body:
    // using any of them would wrongly accept the tampered body below.
    let decoy = stripe_sig(STRIPE_SECRET, NOW, &flip(STRIPE_BODY));
    let v = format!(
        "t={NOW},v0={decoy},secret={decoy},destination={decoy},payload={decoy},x-future=1,v1={good}"
    );
    verify(&stripe(), STRIPE_SECRET, &stripe_hdr(&v), STRIPE_BODY, NOW).unwrap();
    assert_eq!(
        verify(
            &stripe(),
            STRIPE_SECRET,
            &stripe_hdr(&v),
            &flip(STRIPE_BODY),
            NOW
        ),
        Err(HookError::BadSignature)
    );
}

#[test]
fn stripe_rejections() {
    let good = stripe_sig(STRIPE_SECRET, NOW, STRIPE_BODY);
    let hs = stripe_hdr(&format!("t={NOW},v1={good}"));
    assert_eq!(
        verify(&stripe(), STRIPE_SECRET, &hs, STRIPE_BODY, NOW + 301),
        Err(HookError::Stale)
    );
    assert_eq!(
        verify(&stripe(), STRIPE_SECRET, &hs, STRIPE_BODY, NOW - 301),
        Err(HookError::Stale)
    );
    verify(&stripe(), STRIPE_SECRET, &hs, STRIPE_BODY, NOW + 300).unwrap();
    // A custom tolerance is honoured.
    let tight = VerifierSpec::Stripe { tolerance_s: 10 };
    assert_eq!(
        verify(&tight, STRIPE_SECRET, &hs, STRIPE_BODY, NOW + 11),
        Err(HookError::Stale)
    );
    assert_eq!(
        verify(
            &stripe(),
            STRIPE_SECRET,
            &stripe_hdr(&format!("v1={good}")),
            STRIPE_BODY,
            NOW
        ),
        Err(HookError::Malformed)
    );
    assert_eq!(
        verify(
            &stripe(),
            STRIPE_SECRET,
            &stripe_hdr(&format!("t=abc,v1={good}")),
            STRIPE_BODY,
            NOW
        ),
        Err(HookError::Malformed)
    );
    assert_eq!(
        verify(
            &stripe(),
            STRIPE_SECRET,
            &stripe_hdr(&format!("t={NOW},v0={good}")),
            STRIPE_BODY,
            NOW
        ),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&stripe(), STRIPE_SECRET, &hs, &flip(STRIPE_BODY), NOW),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&stripe(), b"whsec_wrong", &hs, STRIPE_BODY, NOW),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&stripe(), STRIPE_SECRET, &[], STRIPE_BODY, NOW),
        Err(HookError::MissingHeader("stripe-signature"))
    );
    // Extreme timestamps do not overflow.
    assert_eq!(
        verify(
            &stripe(),
            STRIPE_SECRET,
            &stripe_hdr(&format!("t={},v1={good}", i64::MIN)),
            STRIPE_BODY,
            i64::MAX
        ),
        Err(HookError::Stale)
    );
}

#[test]
fn stripe_secret_is_not_base64_decoded() {
    // If the verifier base64-decoded the part after `whsec_`, it would compute with a different
    // key and reject this signature, which is made with the literal secret bytes.
    let secret = b"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    let good = stripe_sig(secret, NOW, STRIPE_BODY);
    let hs = stripe_hdr(&format!("t={NOW},v1={good}"));
    verify(&stripe(), secret, &hs, STRIPE_BODY, NOW).unwrap();
}

// ---------------------------------------------------------------------------------------------
// Standard Webhooks

const SW_SECRET: &[u8] = b"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
const SW_ID: &str = "msg_p5jXN8AQM9LWM0D4loKWxJek";
const SW_TS: i64 = 1_614_265_330;
const SW_BODY: &[u8] = br#"{"test": 2432232314}"#;
const SW_SIG: &str = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";

fn sw() -> VerifierSpec {
    VerifierSpec::StandardWebhooks {
        tolerance_s: DEFAULT_TOLERANCE_S,
    }
}

fn sw_headers(sig: &str) -> Vec<(String, Vec<u8>)> {
    h(&[
        ("webhook-id", SW_ID),
        ("webhook-timestamp", "1614265330"),
        ("webhook-signature", sig),
    ])
}

#[test]
fn standard_webhooks_spec_vector_accepted() {
    verify(&sw(), SW_SECRET, &sw_headers(SW_SIG), SW_BODY, SW_TS).unwrap();
}

#[test]
fn standard_webhooks_signed_content_and_multiple_signatures() {
    // Independently: key = base64-decode("MfKQ…"), content = "{id}.{ts}.{body}".
    let key = [
        0x31, 0xf2, 0x90, 0xf6, 0xbf, 0x06, 0x29, 0x8a, 0xab, 0x4f, 0x08, 0xd4, 0x3c, 0x3f, 0x08,
        0x2c, 0xf6, 0x48, 0xa3, 0x62, 0xda, 0x2d, 0xa4, 0xb0,
    ];
    let mut msg = format!("{SW_ID}.{SW_TS}.").into_bytes();
    msg.extend_from_slice(SW_BODY);
    assert_eq!(
        format!("v1,{}", b64(&hmac_sha256_by_hand(&key, &msg))),
        SW_SIG
    );

    let other = b64(&hmac_sha256_by_hand(b"another key", &msg));
    for sig in [
        format!("v1,{other} {SW_SIG}"),
        format!("{SW_SIG} v1,{other}"),
        format!("v1a,{other} {SW_SIG}"),
        format!("v1,!!notbase64!! {SW_SIG}"),
    ] {
        verify(&sw(), SW_SECRET, &sw_headers(&sig), SW_BODY, SW_TS).unwrap();
    }
    // A v1a (asymmetric) entry carrying the right bytes is not treated as v1.
    let wrong_version = SW_SIG.replacen("v1,", "v1a,", 1);
    assert_eq!(
        verify(
            &sw(),
            SW_SECRET,
            &sw_headers(&wrong_version),
            SW_BODY,
            SW_TS
        ),
        Err(HookError::BadSignature)
    );
}

#[test]
fn standard_webhooks_rejections() {
    assert_eq!(
        verify(&sw(), SW_SECRET, &sw_headers(SW_SIG), SW_BODY, SW_TS + 301),
        Err(HookError::Stale)
    );
    assert_eq!(
        verify(&sw(), SW_SECRET, &sw_headers(SW_SIG), &flip(SW_BODY), SW_TS),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(
            &sw(),
            b"whsec_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            &sw_headers(SW_SIG),
            SW_BODY,
            SW_TS
        ),
        Err(HookError::BadSignature)
    );
    // A tampered id changes the signed content.
    let mut hs = sw_headers(SW_SIG);
    hs[0].1 = b"msg_other".to_vec();
    assert_eq!(
        verify(&sw(), SW_SECRET, &hs, SW_BODY, SW_TS),
        Err(HookError::BadSignature)
    );
    // The secret must be base64 after `whsec_`.
    assert_eq!(
        verify(&sw(), b"whsec_***", &sw_headers(SW_SIG), SW_BODY, SW_TS),
        Err(HookError::Config)
    );
    assert_eq!(
        verify(&sw(), SW_SECRET, &sw_headers("garbage"), SW_BODY, SW_TS),
        Err(HookError::Malformed)
    );
    let missing_id = h(&[
        ("webhook-timestamp", "1614265330"),
        ("webhook-signature", SW_SIG),
    ]);
    assert_eq!(
        verify(&sw(), SW_SECRET, &missing_id, SW_BODY, SW_TS),
        Err(HookError::MissingHeader("webhook-id"))
    );
}

// ---------------------------------------------------------------------------------------------
// Generic HMAC: one test per combination.

fn generic(
    prefix: Option<&str>,
    algorithm: HmacAlgorithm,
    encoding: SigEncoding,
    ts: Option<(&str, SignedTemplate)>,
    tolerance_s: Option<i64>,
) -> VerifierSpec {
    VerifierSpec::GenericHmac(GenericHmacSpec {
        header: "x-signature".to_string(),
        prefix: prefix.map(str::to_string),
        algorithm,
        encoding,
        timestamp_header: ts.as_ref().map(|(n, _)| n.to_string()),
        template: ts.map(|(_, t)| t).unwrap_or(SignedTemplate::Body),
        tolerance_s,
    })
}

const G_SECRET: &[u8] = b"generic-secret";
const G_BODY: &[u8] = br#"{"order":42}"#;

#[test]
fn generic_sha256_hex_no_prefix_body() {
    let spec = generic(None, HmacAlgorithm::Sha256, SigEncoding::Hex, None, None);
    let sig = hex(&hmac_sha256_by_hand(G_SECRET, G_BODY));
    verify(&spec, G_SECRET, &h(&[("x-signature", &sig)]), G_BODY, NOW).unwrap();
    assert_eq!(
        verify(
            &spec,
            G_SECRET,
            &h(&[("x-signature", &sig)]),
            &flip(G_BODY),
            NOW
        ),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&spec, b"other", &h(&[("x-signature", &sig)]), G_BODY, NOW),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&spec, G_SECRET, &h(&[("x-signature", "xyz")]), G_BODY, NOW),
        Err(HookError::Malformed)
    );
    assert_eq!(
        verify(&spec, G_SECRET, &[], G_BODY, NOW),
        Err(HookError::MissingHeader("signature"))
    );
}

#[test]
fn generic_sha256_hex_with_prefix() {
    let spec = generic(
        Some("sha256="),
        HmacAlgorithm::Sha256,
        SigEncoding::Hex,
        None,
        None,
    );
    let sig = hex(&hmac_sha256_by_hand(G_SECRET, G_BODY));
    verify(
        &spec,
        G_SECRET,
        &h(&[("x-signature", &format!("sha256={sig}"))]),
        G_BODY,
        NOW,
    )
    .unwrap();
    assert_eq!(
        verify(&spec, G_SECRET, &h(&[("x-signature", &sig)]), G_BODY, NOW),
        Err(HookError::Malformed)
    );
}

#[test]
fn generic_sha256_base64() {
    let spec = generic(None, HmacAlgorithm::Sha256, SigEncoding::Base64, None, None);
    let sig = b64(&hmac_sha256_by_hand(G_SECRET, G_BODY));
    verify(&spec, G_SECRET, &h(&[("x-signature", &sig)]), G_BODY, NOW).unwrap();
    assert_eq!(
        verify(&spec, G_SECRET, &h(&[("x-signature", "@@@@")]), G_BODY, NOW),
        Err(HookError::Malformed)
    );
    // A hex signature under a base64 spec does not verify.
    let hex_sig = hex(&hmac_sha256_by_hand(G_SECRET, G_BODY));
    assert!(
        verify(
            &spec,
            G_SECRET,
            &h(&[("x-signature", &hex_sig)]),
            G_BODY,
            NOW
        )
        .is_err()
    );
}

#[test]
fn generic_legacy_sha1_hex_rfc2202() {
    let spec = generic(
        Some("sha1="),
        HmacAlgorithm::LegacySha1,
        SigEncoding::Hex,
        None,
        None,
    );
    let body = b"what do ya want for nothing?";
    let hs = h(&[(
        "x-signature",
        "sha1=effcdf6ae5eb2fa2d27416d5f184df9c259a7c79",
    )]);
    verify(&spec, b"Jefe", &hs, body, NOW).unwrap();
    assert_eq!(
        verify(&spec, b"Jefe", &hs, &flip(body), NOW),
        Err(HookError::BadSignature)
    );
    // The SHA-256 MAC of the same input is not accepted by a SHA-1 spec.
    let s256 = format!("sha1={}", hex(&hmac_sha256_by_hand(b"Jefe", body)));
    assert_eq!(
        verify(&spec, b"Jefe", &h(&[("x-signature", &s256)]), body, NOW),
        Err(HookError::BadSignature)
    );
}

#[test]
fn generic_legacy_sha1_base64() {
    let spec = generic(
        None,
        HmacAlgorithm::LegacySha1,
        SigEncoding::Base64,
        None,
        None,
    );
    // base64 of effcdf6ae5eb2fa2d27416d5f184df9c259a7c79 (RFC 2202 case 2).
    let hs = h(&[("x-signature", "7/zfauXrL6LSdBbV8YTfnCWafHk=")]);
    verify(&spec, b"Jefe", &hs, b"what do ya want for nothing?", NOW).unwrap();
}

#[test]
fn generic_timestamp_template_with_tolerance() {
    let spec = generic(
        None,
        HmacAlgorithm::Sha256,
        SigEncoding::Hex,
        Some(("x-timestamp", SignedTemplate::TsDotBody)),
        Some(60),
    );
    let mut msg = format!("{NOW}.").into_bytes();
    msg.extend_from_slice(G_BODY);
    let sig = hex(&hmac_sha256_by_hand(G_SECRET, &msg));
    let ts = NOW.to_string();
    let hs = h(&[("x-signature", &sig), ("x-timestamp", &ts)]);
    verify(&spec, G_SECRET, &hs, G_BODY, NOW + 60).unwrap();
    assert_eq!(
        verify(&spec, G_SECRET, &hs, G_BODY, NOW + 61),
        Err(HookError::Stale)
    );
    // The timestamp is part of the signed content.
    let later = (NOW + 1).to_string();
    let tampered = h(&[("x-signature", &sig), ("x-timestamp", &later)]);
    assert_eq!(
        verify(&spec, G_SECRET, &tampered, G_BODY, NOW),
        Err(HookError::BadSignature)
    );
    assert_eq!(
        verify(&spec, G_SECRET, &h(&[("x-signature", &sig)]), G_BODY, NOW),
        Err(HookError::MissingHeader("timestamp"))
    );
    assert_eq!(
        verify(
            &spec,
            G_SECRET,
            &h(&[("x-signature", &sig), ("x-timestamp", "x")]),
            G_BODY,
            NOW
        ),
        Err(HookError::Malformed)
    );
}

#[test]
fn generic_timestamp_template_without_tolerance() {
    let spec = generic(
        None,
        HmacAlgorithm::Sha256,
        SigEncoding::Hex,
        Some(("x-timestamp", SignedTemplate::TsDotBody)),
        None,
    );
    let mut msg = b"1000.".to_vec();
    msg.extend_from_slice(G_BODY);
    let sig = hex(&hmac_sha256_by_hand(G_SECRET, &msg));
    let hs = h(&[("x-signature", &sig), ("x-timestamp", "1000")]);
    // No replay window: years later still verifies.
    verify(&spec, G_SECRET, &hs, G_BODY, NOW).unwrap();
}

#[test]
fn generic_invalid_combinations_are_config_errors() {
    // {ts}.{body} without a timestamp header.
    let spec = VerifierSpec::GenericHmac(GenericHmacSpec {
        header: "x-signature".into(),
        prefix: None,
        algorithm: HmacAlgorithm::Sha256,
        encoding: SigEncoding::Hex,
        timestamp_header: None,
        template: SignedTemplate::TsDotBody,
        tolerance_s: None,
    });
    assert_eq!(
        verify(&spec, G_SECRET, &h(&[("x-signature", "00")]), G_BODY, NOW),
        Err(HookError::Config)
    );
    // A timestamp header with the {body} template would be an unsigned timestamp.
    let spec = generic(
        None,
        HmacAlgorithm::Sha256,
        SigEncoding::Hex,
        Some(("x-timestamp", SignedTemplate::Body)),
        Some(60),
    );
    assert_eq!(
        verify(&spec, G_SECRET, &h(&[("x-signature", "00")]), G_BODY, NOW),
        Err(HookError::Config)
    );
    // A tolerance without a timestamp header.
    let spec = generic(
        None,
        HmacAlgorithm::Sha256,
        SigEncoding::Hex,
        None,
        Some(60),
    );
    assert_eq!(
        verify(&spec, G_SECRET, &h(&[("x-signature", "00")]), G_BODY, NOW),
        Err(HookError::Config)
    );
    // An empty secret.
    let spec = generic(None, HmacAlgorithm::Sha256, SigEncoding::Hex, None, None);
    assert_eq!(
        verify(&spec, b"", &h(&[("x-signature", "00")]), G_BODY, NOW),
        Err(HookError::Config)
    );
}

// ---------------------------------------------------------------------------------------------
// Meta mapping (crates/ostro-meta/src/records.rs `Webhook`: verifier u8, verifier_config, tolerance_s)

#[test]
fn verifier_kind_ordinals_match_meta_records() {
    let records = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../ostro-meta/src/records.rs"
    ))
    .unwrap();
    for (name, kind) in [
        ("VERIFIER_NONE", VerifierKind::None),
        ("VERIFIER_STRIPE", VerifierKind::Stripe),
        ("VERIFIER_GITHUB", VerifierKind::GitHub),
        ("VERIFIER_SLACK", VerifierKind::Slack),
        ("VERIFIER_GENERIC_HMAC", VerifierKind::GenericHmac),
        ("VERIFIER_STANDARD_WEBHOOKS", VerifierKind::StandardWebhooks),
    ] {
        let line = format!("pub const {name}: u8 = {};", kind.as_u8());
        assert!(records.contains(&line), "records.rs lacks `{line}`");
        assert_eq!(VerifierKind::from_u8(kind.as_u8()), Some(kind));
    }
    assert_eq!(VerifierKind::from_u8(6), None);
}

#[test]
fn from_meta_defaults_and_round_trip() {
    assert_eq!(VerifierSpec::from_meta(0, b"", 0), Ok(VerifierSpec::None));
    assert_eq!(
        VerifierSpec::from_meta(1, b"", 0),
        Ok(VerifierSpec::Stripe { tolerance_s: 300 })
    );
    assert_eq!(
        VerifierSpec::from_meta(1, b"", 30),
        Ok(VerifierSpec::Stripe { tolerance_s: 30 })
    );
    assert_eq!(VerifierSpec::from_meta(2, b"", 0), Ok(VerifierSpec::GitHub));
    assert_eq!(
        VerifierSpec::from_meta(3, b"", 0),
        Ok(VerifierSpec::Slack { tolerance_s: 300 })
    );
    assert_eq!(
        VerifierSpec::from_meta(5, b"", 0),
        Ok(VerifierSpec::StandardWebhooks { tolerance_s: 300 })
    );
    // Unknown kind, stray config, a tolerance on a kind without timestamps.
    assert_eq!(VerifierSpec::from_meta(9, b"", 0), Err(HookError::Config));
    assert_eq!(VerifierSpec::from_meta(2, b"{}", 0), Err(HookError::Config));
    assert_eq!(VerifierSpec::from_meta(2, b"", 5), Err(HookError::Config));
    // Generic needs a config.
    assert_eq!(VerifierSpec::from_meta(4, b"", 0), Err(HookError::Config));

    let cfg = br#"{"header":"X-Sig","prefix":"sha256=","algorithm":"sha256","encoding":"base64","timestamp_header":"X-Ts","template":"{ts}.{body}"}"#;
    let spec = VerifierSpec::from_meta(4, cfg, 120).unwrap();
    assert_eq!(
        spec,
        VerifierSpec::GenericHmac(GenericHmacSpec {
            header: "x-sig".into(),
            prefix: Some("sha256=".into()),
            algorithm: HmacAlgorithm::Sha256,
            encoding: SigEncoding::Base64,
            timestamp_header: Some("x-ts".into()),
            template: SignedTemplate::TsDotBody,
            tolerance_s: Some(120),
        })
    );
    assert_eq!(spec.kind(), VerifierKind::GenericHmac);

    let minimal = VerifierSpec::from_meta(4, br#"{"header":"x-sig"}"#, 0).unwrap();
    assert_eq!(
        minimal,
        VerifierSpec::GenericHmac(GenericHmacSpec {
            header: "x-sig".into(),
            prefix: None,
            algorithm: HmacAlgorithm::Sha256,
            encoding: SigEncoding::Hex,
            timestamp_header: None,
            template: SignedTemplate::Body,
            tolerance_s: None,
        })
    );
    let sha1 =
        VerifierSpec::from_meta(4, br#"{"header":"x-sig","algorithm":"legacy_sha1"}"#, 0).unwrap();
    assert!(matches!(
        sha1,
        VerifierSpec::GenericHmac(GenericHmacSpec {
            algorithm: HmacAlgorithm::LegacySha1,
            ..
        })
    ));
    // Plain "sha1" is not accepted: the opt-in name is explicit.
    assert_eq!(
        VerifierSpec::from_meta(4, br#"{"header":"x-sig","algorithm":"sha1"}"#, 0),
        Err(HookError::Config)
    );
    // Unknown keys, bad JSON and invalid combinations are rejected at create time.
    assert_eq!(
        VerifierSpec::from_meta(4, br#"{"header":"x-sig","colour":"red"}"#, 0),
        Err(HookError::Config)
    );
    assert_eq!(VerifierSpec::from_meta(4, b"{", 0), Err(HookError::Config));
    assert_eq!(
        VerifierSpec::from_meta(4, br#"{"header":"x-sig","template":"{ts}.{body}"}"#, 0),
        Err(HookError::Config)
    );
    assert_eq!(
        VerifierSpec::from_meta(4, br#"{"header":"x-sig"}"#, 60),
        Err(HookError::Config)
    );

    for spec in [
        VerifierSpec::None,
        VerifierSpec::Stripe { tolerance_s: 300 },
        VerifierSpec::GitHub,
        VerifierSpec::Slack { tolerance_s: 17 },
        VerifierSpec::StandardWebhooks { tolerance_s: 300 },
        spec.clone(),
        minimal.clone(),
        sha1.clone(),
    ] {
        let (kind, cfg, tol) = spec.to_meta();
        assert_eq!(VerifierSpec::from_meta(kind, &cfg, tol), Ok(spec.clone()));
    }
}

#[test]
fn none_verifier_passes_without_headers() {
    verify(&VerifierSpec::None, b"", &[], b"anything", 0).unwrap();
}

#[test]
fn errors_never_display_header_values() {
    let all = [
        HookError::Malformed,
        HookError::MissingHeader("stripe-signature"),
        HookError::Stale,
        HookError::BadSignature,
        HookError::Config,
    ];
    for e in all {
        let s = e.to_string();
        assert!(!s.is_empty());
        assert!(!s.contains("v1="));
    }
}

#[test]
fn webhook_rs_has_no_equality_on_signatures() {
    // Constant time: every signature comparison goes through `Mac::verify_slice`. The simplest
    // enforceable rule is that the module's code contains no `==`/`!=` at all.
    let src =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/webhook.rs")).unwrap();
    assert!(src.contains("verify_slice"));
    for (i, line) in src.lines().enumerate() {
        let code = line.split("//").next().unwrap_or("");
        assert!(
            !code.contains("==") && !code.contains("!="),
            "webhook.rs:{}: equality operator in verifier code: {line}",
            i + 1
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Extractors

#[test]
fn extractor_parse() {
    assert_eq!(
        IdempotencyExtractor::parse("header:X-GitHub-Delivery"),
        Ok(IdempotencyExtractor::Header("x-github-delivery".into()))
    );
    assert_eq!(
        IdempotencyExtractor::parse("json:/data/object/id"),
        Ok(IdempotencyExtractor::JsonPointer("/data/object/id".into()))
    );
    assert_eq!(
        IdempotencyExtractor::parse("none"),
        Ok(IdempotencyExtractor::None)
    );
    for bad in [
        "",
        "header:",
        "json:",
        "json:id",
        "header:bad header",
        "cookie:x",
        "NONE",
    ] {
        assert_eq!(
            IdempotencyExtractor::parse(bad),
            Err(ExtractError::Syntax),
            "{bad:?}"
        );
    }
    for s in ["header:x-github-delivery", "json:/data/object/id", "none"] {
        assert_eq!(IdempotencyExtractor::parse(s).unwrap().to_string(), s);
    }
}

#[test]
fn extractor_header_and_json() {
    let p = proof();
    let hs = h(&[("X-GitHub-Delivery", "72d3162e-cc78-11e3-81ab-4c9367dc0958")]);
    let ex = IdempotencyExtractor::parse("header:X-GitHub-Delivery").unwrap();
    assert_eq!(
        ex.extract(&p, &hs, b"not json").unwrap().as_deref(),
        Some("72d3162e-cc78-11e3-81ab-4c9367dc0958")
    );
    assert_eq!(ex.extract(&p, &[], b"").unwrap(), None);

    let ex = IdempotencyExtractor::parse("json:/data/object/id").unwrap();
    assert_eq!(
        ex.extract(&p, &[], STRIPE_BODY).unwrap().as_deref(),
        Some("cs_9")
    );
    assert_eq!(ex.extract(&p, &[], br#"{"data":{}}"#).unwrap(), None);
    assert_eq!(
        IdempotencyExtractor::parse("json:/n")
            .unwrap()
            .extract(&p, &[], br#"{"n":42}"#)
            .unwrap()
            .as_deref(),
        Some("42")
    );
    assert_eq!(ex.extract(&p, &[], b"{oops"), Err(ExtractError::BadJson));
    assert_eq!(
        ex.extract(&p, &[], br#"{"data":{"object":{"id":{"x":1}}}}"#),
        Err(ExtractError::NotScalar)
    );
    assert_eq!(
        IdempotencyExtractor::None
            .extract(&p, &hs, STRIPE_BODY)
            .unwrap(),
        None
    );
}

#[test]
fn extractor_rejects_long_keys() {
    let p = proof();
    let ok = "k".repeat(MAX_KEY_LEN);
    let long = "k".repeat(MAX_KEY_LEN + 1);
    let ex = IdempotencyExtractor::parse("header:x-key").unwrap();
    assert_eq!(
        ex.extract(&p, &h(&[("x-key", &ok)]), b"")
            .unwrap()
            .as_deref(),
        Some(ok.as_str())
    );
    assert_eq!(
        ex.extract(&p, &h(&[("x-key", &long)]), b""),
        Err(ExtractError::TooLong)
    );
    let body = format!(r#"{{"id":"{long}"}}"#);
    let ex = IdempotencyExtractor::parse("json:/id").unwrap();
    assert_eq!(
        ex.extract(&p, &[], body.as_bytes()),
        Err(ExtractError::TooLong)
    );
    let bin = vec![("x-key".to_string(), vec![0xff, 0xfe])];
    assert_eq!(
        IdempotencyExtractor::parse("header:x-key")
            .unwrap()
            .extract(&p, &bin, b""),
        Err(ExtractError::NotUtf8)
    );
}

#[test]
fn vendor_default_extractors() {
    assert_eq!(
        default_for(VerifierKind::Stripe),
        IdempotencyExtractor::JsonPointer("/id".into())
    );
    assert_eq!(
        default_for(VerifierKind::GitHub),
        IdempotencyExtractor::Header("x-github-delivery".into())
    );
    assert_eq!(
        default_for(VerifierKind::Slack),
        IdempotencyExtractor::JsonPointer("/event_id".into())
    );
    assert_eq!(
        default_for(VerifierKind::StandardWebhooks),
        IdempotencyExtractor::Header("webhook-id".into())
    );
    assert_eq!(
        default_for(VerifierKind::GenericHmac),
        IdempotencyExtractor::None
    );
    assert_eq!(default_for(VerifierKind::None), IdempotencyExtractor::None);
}

#[test]
fn input_selector() {
    let p = proof();
    assert_eq!(InputSelector::from_meta(""), Ok(InputSelector::Body));
    assert_eq!(
        InputSelector::from_meta("/data/object"),
        Ok(InputSelector::JsonPointer("/data/object".into()))
    );
    assert_eq!(InputSelector::from_meta("data"), Err(ExtractError::Syntax));
    // Body: the raw bytes, untouched (not even required to be JSON).
    assert_eq!(
        InputSelector::Body.select(&p, b"a=b&c").unwrap(),
        b"a=b&c".to_vec()
    );
    let sel = InputSelector::from_meta("/data/object").unwrap();
    assert_eq!(
        sel.select(&p, STRIPE_BODY).unwrap(),
        br#"{"id":"cs_9"}"#.to_vec()
    );
    assert_eq!(
        sel.select(&p, br#"{"data":1}"#),
        Err(ExtractError::PointerNotFound)
    );
    assert_eq!(sel.select(&p, b"nope"), Err(ExtractError::BadJson));
}
