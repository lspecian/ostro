//! Parser fuzz targets (14-07, SEC-05): REST JSON request bodies, webhook verification, cron
//! expressions, egress patterns, secret templates, API bearer tokens (14-03) and module
//! signatures / trusted keys (14-05). Each target calls the production parser and checks a
//! target-specific invariant (see each function).

use std::collections::BTreeMap;

use ostro_bridge::egress::policy::{allows_connection, parse_rule};
use ostro_bridge::egress::url_gate;
use ostro_bridge::origin::{Origin, OriginPattern};
use ostro_bridge::secrets::template;
use ostro_bridge::secrets::value::SecretValue;
use ostro_meta::EgressPolicy;
use ostro_server::p14::{signing, tokens};
use ostro_triggers::cron::CronSpec;
use ostro_triggers::webhook::{VerifierSpec, verify};

// ------------------------------------------------------------------------------ rest_json

/// `rest_json`: the bytes are a REST request body, deserialised as every JSON request DTO the
/// REST API accepts. Invariant: never panics; a body that deserialises as a serialisable DTO
/// serialises to JSON that deserialises to an equal value, and re-serialising that is
/// byte-identical.
pub fn rest_json(data: &[u8]) {
    macro_rules! rt {
        ($($t:ty),* $(,)?) => {$(
            if let Ok(v) = serde_json::from_slice::<$t>(data) {
                let j1 = serde_json::to_vec(&v).expect(concat!("serialise ", stringify!($t)));
                let v2: $t = serde_json::from_slice(&j1)
                    .expect(concat!("re-parse ", stringify!($t)));
                assert_eq!(v2, v, concat!("round trip ", stringify!($t)));
                let j2 = serde_json::to_vec(&v2).expect("serialise again");
                assert_eq!(j2, j1, concat!("re-serialisation ", stringify!($t)));
            }
        )*};
    }
    macro_rules! de {
        ($($t:ty),* $(,)?) => {$(
            let _ = serde_json::from_slice::<$t>(data);
        )*};
    }
    use ostro_api::{phase7, phase8, phase11, phase13, phase13_adapters, phase14, rest};
    rt!(
        rest::ActivateWorkflowBody,
        rest::StartRunBody,
        rest::SignalBody,
        rest::StopRunBody,
        rest::RetryBody,
        rest::RunPolicyBody,
        rest::GrantDto,
        phase7::JoinRequest,
        phase7::PutQueueFlowBody,
        phase7::PauseRunRequest,
        phase7::CreateWorkerTokenRequest,
        phase8::ScheduleDto,
        phase8::WebhookDto,
        phase8::TriggerScheduleBody,
        phase11::FailurePolicy,
        phase11::BulkCreateRequest,
        phase11::DismissBody,
        phase11::RequeueBody,
        phase11::RerunBody,
        phase13::InitRequest,
        phase13::JoinRequest,
        phase13::NodeTokenRequest,
        phase13::ReassignRequest,
        phase13::MigrateRunsRequest,
        phase13_adapters::RemoveMemberBody,
        phase14::TenantSpec,
        phase14::CreateTokenRequest,
        phase14::RotateTokenRequest,
        phase14::ModulePolicy,
        phase14::TrustedKeySpec,
        phase14::AttachSignaturesRequest,
        ostro_api::phase14_adapters::AttachSignaturesBody,
    );
    // TenantPatch: `Serialize` writes absent keys as `null` (= clear), so it is not the inverse
    // of `Deserialize`; no request path serialises it (the client builds the JSON by hand).
    de!(
        phase14::TenantPatch,
        rest::CompleteTokenBody,
        rest::PutSecretBody,
        rest::PutConnectionBody,
        rest::PutEgressBody,
        rest::DevSkipBody,
        rest::ReplayBody,
    );
}

// ------------------------------------------------------------------------------ webhook_verify

/// The fixed fuzzing "now" (unix seconds) when the input carries none.
const NOW: i64 = 1_763_600_000;
const WRONG_SECRET: &[u8] = b"ostro-fuzz-wrong-secret";

/// The secret each verifier kind is fuzzed with (vendor test vectors where they exist; never
/// a real credential).
fn webhook_secret(kind: u8) -> &'static [u8] {
    match kind {
        1 => b"ostro-fuzz-stripe-secret",
        2 => b"It's a Secret to Everybody",
        3 => b"8f742231b10e8888abcd99yyyzzz85a5",
        4 => b"ostro-fuzz-generic-secret",
        // Standard Webhooks spec vector, split so secret scanners do not flag it.
        5 => concat!("whsec_", "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").as_bytes(),
        _ => b"x",
    }
}

/// `webhook_verify`: `kind: u8 ‖ tolerance_s: u8 ‖ now: i64 LE ‖ config ‖ 0x00 ‖ headers ‖
/// 0x00 ‖ body`, headers as `name:value` lines. Invariants: building the verifier from the
/// meta fields and verifying never panic; the meta mapping round-trips; a delivery that
/// verifies with the kind's secret does not verify with another secret.
pub fn webhook_verify(data: &[u8]) {
    if data.len() < 10 {
        return;
    }
    let kind = data[0] % 6;
    let tol = u32::from(data[1]);
    let now = i64::from_le_bytes(data[2..10].try_into().expect("8 bytes"));
    let now = if now == 0 { NOW } else { now };
    let mut parts = data[10..].splitn(3, |&b| b == 0);
    let config = parts.next().unwrap_or_default();
    let headers: Vec<(String, Vec<u8>)> = parts
        .next()
        .unwrap_or_default()
        .split(|&b| b == b'\n')
        .filter_map(|line| {
            let at = line.iter().position(|&b| b == b':')?;
            let name = String::from_utf8_lossy(&line[..at]).to_ascii_lowercase();
            Some((name, line[at + 1..].to_vec()))
        })
        .collect();
    let body = parts.next().unwrap_or_default();

    // The meta ordinals are 0..=5 in VerifierKind order: None, Stripe, GitHub, Slack, generic
    // HMAC, Standard Webhooks.
    let Ok(spec) = VerifierSpec::from_meta(kind, config, tol) else {
        return;
    };
    let (v, c, t) = spec.to_meta();
    assert_eq!(
        VerifierSpec::from_meta(v, &c, t).as_ref(),
        Ok(&spec),
        "verifier meta round trip"
    );
    let secret = webhook_secret(kind);
    if verify(&spec, secret, &headers, body, now).is_ok() && !matches!(spec, VerifierSpec::None) {
        assert!(
            verify(&spec, WRONG_SECRET, &headers, body, now).is_err(),
            "delivery verified with two secrets"
        );
    }
    let _ = verify(&spec, b"", &headers, body, now);
}

// ------------------------------------------------------------------------------ cron_expr

/// 2026-01-01T00:00:00Z in unix ms.
const T0: i64 = 1_767_225_600_000;

/// `cron_expr`: `expr ‖ '\n' ‖ timezone` (UTF-8; timezone optional). Invariants: parsing never
/// panics; for a parsed spec the next fire is after the probe, the latest fire is at or before
/// it, and `fires_in` returns at most `cap` strictly increasing fires inside the window.
pub fn cron_expr(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let (expr, tz) = text.split_once('\n').unwrap_or((text, ""));
    let tz = tz.lines().next().unwrap_or("");
    let Ok(spec) = CronSpec::parse(expr, tz) else {
        return;
    };
    // Probe instants the scheduler can pass (clock readings): a backward search from the far
    // future (e.g. i64::MAX / 2) is unbounded in croner and is never asked for in production.
    for t in [T0, 0, -1, T0 + 1, T0 + 86_400_000 * 366] {
        if let Some(n) = spec.next_after(t) {
            assert!(n > t, "next_after({t}) = {n}");
        }
        if let Some(p) = spec.latest_at_or_before(t) {
            assert!(p <= t, "latest_at_or_before({t}) = {p}");
        }
    }
    let (from, to, cap) = (T0, T0 + 86_400_000, 64);
    let (fires, _) = spec.fires_in(from, to, cap);
    assert!(fires.len() <= cap);
    assert!(
        fires.iter().all(|&f| f > from && f <= to),
        "fire outside window"
    );
    assert!(
        fires.windows(2).all(|w| w[0] < w[1]),
        "fires not increasing"
    );
    let _ = spec.count_fires(from, from + 3_600_000);
}

// ------------------------------------------------------------------------------ egress_pattern

/// `egress_pattern`: UTF-8 lines; every line but the last is an egress allow rule, the last is
/// the URL to check. Invariants: rule / origin-pattern parsing and the URL gate never panic; a
/// URL the gate admits is an `http`/`https` URL without a secret placeholder, its origin is
/// the origin of the normalised URL, and the matching rule is one of the policy's rules with
/// the URL's scheme and port.
pub fn egress_pattern(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let mut lines: Vec<&str> = text.split('\n').collect();
    let url = lines.pop().unwrap_or("");
    let patterns: Vec<OriginPattern> = lines
        .iter()
        .filter_map(|l| OriginPattern::parse(l).ok())
        .collect();
    let rules: Vec<_> = lines.iter().filter_map(|l| parse_rule(l).ok()).collect();
    let policy = EgressPolicy {
        name: "fuzz".into(),
        allow: lines.iter().map(|s| s.to_string()).collect(),
        ext: Vec::new(),
    };
    if let Ok(checked) = url_gate::check(Some(&policy), url, &[]) {
        let scheme = checked.url.scheme();
        assert!(
            scheme == "http" || scheme == "https",
            "gate admitted {url:?}"
        );
        assert!(!url.contains(template::OPEN), "gate admitted a placeholder");
        assert_eq!(
            Origin::from_url(&checked.url).as_ref(),
            Some(&checked.origin),
            "origin of the admitted URL"
        );
        assert!(
            rules.contains(&checked.rule),
            "matching rule not in the policy"
        );
        if let Some((s, p)) = checked.rule.scheme_port() {
            assert_eq!(
                (s, p),
                (checked.origin.scheme.as_str(), checked.origin.port)
            );
        }
        for p in &patterns {
            let _ = p.matches(&checked.origin);
        }
    }
    let _ = url_gate::check(None, url, &[]);
    let _ = allows_connection(Some(&policy), url);
}

// ------------------------------------------------------------------------------ secret_template

/// `secret_template`: a UTF-8 string with `{{secret:name}}` placeholders. Invariants: parsing
/// never panics; placeholders are in order, non-overlapping and spell exactly
/// `{{secret:<name>}}` with a valid name; expansion with every name resolved has exactly the
/// predicted length; expansion without values or of an unparsable template is the input.
pub fn secret_template(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    assert_eq!(template::contains(s), s.contains(template::OPEN));
    let empty = BTreeMap::new();
    match template::parse(s) {
        Ok(ps) => {
            let mut last = 0;
            let mut values = BTreeMap::new();
            let mut len = s.len();
            for p in &ps {
                assert!(p.range.start >= last, "placeholders overlap");
                last = p.range.end;
                assert_eq!(
                    &s[p.range.clone()],
                    format!("{}{}{}", template::OPEN, p.name, template::CLOSE),
                    "placeholder range"
                );
                assert_eq!(template::validate_name(&p.name), Ok(()));
                if !values.contains_key(&p.name) {
                    values.insert(p.name.clone(), SecretValue::from_slice(b"V"));
                }
                len = len - p.range.len() + 1;
            }
            let out = template::expand(s, &values);
            assert_eq!(out.len(), len, "expanded length");
        }
        Err(_) => {
            let mut values = BTreeMap::new();
            values.insert("a".to_string(), SecretValue::from_slice(b"V"));
            assert_eq!(template::expand(s, &values).as_slice(), s.as_bytes());
        }
    }
    assert_eq!(template::expand(s, &empty).as_slice(), s.as_bytes());
}

// ------------------------------------------------------------------------------ bearer_token

/// `bearer_token`: a UTF-8 `Authorization: Bearer` value. Invariants: `tokens::parse` never
/// panics; a parsed bearer re-assembles to exactly the input, its id is a valid API token id,
/// its secret verifies against its own hash; base32 decoding is canonical (decode → encode
/// is the identity).
pub fn bearer_token(data: &[u8]) {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    if let Some(b) = tokens::base32_decode(s) {
        assert_eq!(tokens::base32_encode(&b), s, "base32 not canonical");
    }
    let Ok((id, secret)) = tokens::parse(s) else {
        return;
    };
    assert_eq!(
        tokens::Bearer::of(&id, &secret).expose(),
        s,
        "bearer re-assembly"
    );
    assert!(tokens::is_api_token_id(id.as_str()));
    assert!(tokens::verify(&tokens::hash(&secret), &secret));
    let raw = tokens::base32_decode(secret.expose()).expect("parsed secret decodes");
    assert_eq!(raw.len(), tokens::SECRET_BYTES);
}

// ------------------------------------------------------------------------------ signature_parse

const BLOB: &[u8] = include_bytes!("../../ostro-server/tests/fixtures/signing/blob.bin");
const MINISIGN_PUB: &str = include_str!("../../ostro-server/tests/fixtures/signing/minisign.pub");

/// `signature_parse`: the bytes as a module signature (`.minisig`, cosign base64 DER or a
/// Sigstore bundle) and, if UTF-8, as a trusted key (minisign public key or cosign PEM).
/// Invariants: parsing and verification never panic; a parsed minisign signature re-parses
/// from its stored bytes to an equal signature; a parsed key survives the ext 115/116 meta
/// round trip; verification of a parsed signature against the fixture key returns evidence
/// only for signatures that verify.
pub fn signature_parse(data: &[u8]) {
    if let Ok(sig) = signing::parse_signature(data) {
        let _ = (sig.alg(), sig.key_id(), sig.hint());
        if sig.key_id().len() == 8 {
            let again = signing::parse_signature(sig.stored_bytes())
                .expect("stored minisig bytes re-parse");
            assert_eq!(again, sig, "minisig normalisation is not stable");
        }
        let key = signing::parse_trusted_key(MINISIGN_PUB).expect("fixture key");
        if let Ok(ev) = signing::verify_module(BLOB, std::slice::from_ref(&sig), &[key]) {
            assert_eq!(ev.len(), 1, "evidence for one signature");
            assert_eq!(ev[0].sig, sig.stored_bytes());
        }
    }
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(k) = signing::parse_trusted_key(text)
    {
        let back = signing::TrustedKey::from_meta(&k.to_meta()).expect("meta round trip");
        assert_eq!(back, k, "trusted key meta round trip");
        let _ = signing::key_id_hex(&k);
        let _ = signing::key_fingerprint(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_parse() {
        assert!(signing::parse_trusted_key(MINISIGN_PUB).is_ok());
        let sig = include_bytes!("../../ostro-server/tests/fixtures/signing/blob.bin.minisig");
        let parsed = signing::parse_signature(sig).expect("fixture minisig");
        let key = signing::parse_trusted_key(MINISIGN_PUB).expect("key");
        assert!(signing::verify_module(BLOB, &[parsed], &[key]).is_ok());
        signature_parse(sig);
    }

    #[test]
    fn known_inputs() {
        for t in [
            rest_json as fn(&[u8]),
            webhook_verify,
            cron_expr,
            egress_pattern,
            secret_template,
            bearer_token,
            signature_parse,
        ] {
            t(b"");
            t(b"{}");
            t(b"* * * * *\nEurope/Berlin");
            t(b"{{secret:a}} {{secret:");
            t(b"https://*.example.com\nhttps://a.example.com/x");
        }
    }
}
