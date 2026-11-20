---
phase: 08-triggers-bottomless-observability
plan: 06
subsystem: triggers
tags: [webhooks, hmac, stripe, github, slack, standard-webhooks, constant-time, tdd]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-01 ostro-triggers skeleton (webhook/extract stubs) with hmac 0.13, sha2 0.11, serde_json, thiserror declared
provides:
  - "ostro_triggers::webhook: VerifierSpec {None, Stripe, GitHub, Slack, StandardWebhooks, GenericHmac(GenericHmacSpec)}, VerifierSpec::from_meta(verifier, verifier_config, tolerance_s) / to_meta / kind, verify(spec, secret, headers, body, now_unix) -> Result<VerifiedDelivery, HookError>, slack_challenge(&VerifiedDelivery, body), header(), VerifierKind (u8 ordinals mirrored from ostro-meta), HmacAlgorithm, SigEncoding, SignedTemplate, DEFAULT_TOLERANCE_S"
  - "ostro_triggers::extract: IdempotencyExtractor::{parse, extract(&VerifiedDelivery, headers, body), Display}, default_for(VerifierKind), InputSelector::{from_meta, to_meta, select(&VerifiedDelivery, body)}, json_pointer_string, ExtractError, MAX_KEY_LEN = 256"
  - "The generic-HMAC verifier_config blob format (JSON object, documented on GenericHmacSpec)"
affects: [08-10 webhook CRUD validation, 08-15 POST /hooks/{name}, 08-21 OTLP canary]

tech-stack:
  added: []
  patterns:
    - "Verify-before-parse is enforced by the types: VerifiedDelivery is a zero-size token only verify() can build; every body reader takes &VerifiedDelivery"
    - "Constant time: all signature checks go through Mac::verify_slice; a grep test forbids ==/!= anywhere in webhook.rs code"
    - "Signed content uses the timestamp header string exactly as received, never a re-formatted integer"

key-files:
  created:
    - crates/ostro-triggers/tests/webhook_verify.rs
  modified:
    - crates/ostro-triggers/src/webhook.rs
    - crates/ostro-triggers/src/extract.rs

key-decisions:
  - "VerifierSpec::from_meta takes the meta Webhook fields (verifier: u8, verifier_config: &[u8], tolerance_s: u32), not &ostro_meta::Webhook: ostro-triggers has no ostro-meta dependency and wave 2 may not touch Cargo.toml/lock. Callers pass (w.verifier, &w.verifier_config, w.tolerance_s). VerifierKind ordinals are mirrored and a test greps records.rs to keep them equal"
  - "tolerance_s 0 = verifier default: 300 s for Stripe/Slack/Standard Webhooks, no replay window for generic HMAC; must be 0 for GitHub and None; verifier_config must be empty for every kind except generic HMAC"
  - "Generic-HMAC verifier_config is a strict JSON object {header, prefix?, algorithm: sha256|legacy_sha1, encoding: hex|base64, timestamp_header?, template: {body}|{ts}.{body}}; unknown keys rejected; plain \"sha1\" rejected (opt-in name is legacy_sha1)"
  - "Generic HMAC rejects a timestamp header with the {body} template (unsigned timestamp) and a tolerance without a timestamp header as HookError::Config"
  - "Legacy SHA-1 is a small local FIPS 180-4 implementation behind the digest 0.11 traits, used via hmac::SimpleHmac (the sha1 crate is not a dependency and wave 2 adds none); FIPS vectors incl. 1M 'a' and RFC 2202 cases 1, 2, 6 pass"
  - "slack_challenge, the extractors and InputSelector::select take &VerifiedDelivery (plan wrote slack_challenge(body)): keeps 'nothing parses the body before verification' type-enforced"
  - "Standard Webhooks: entries of other versions (v1a) are ignored, undecodable v1 entries skipped; a header with no `ver,sig` entry at all is Malformed, otherwise no match is BadSignature. Stripe: duplicate t is Malformed, non-hex v1 entries skipped"
  - "VerifierSpec::None always verifies; whether an unauthenticated hook is allowed is 08-15's policy. An empty secret on any other kind is HookError::Config"
  - "Generic-HMAC MissingHeader reports the static names \"signature\" / \"timestamp\" (HookError carries &'static str)"

requirements-completed: []

duration: ~30 min
completed: 2026-11-20
---

# Phase 8 Plan 06: Webhook verifiers and extractors Summary

**This plan adds pure, constant-time webhook signature verification to `ostro-triggers`. It covers Stripe (any number of v1 entries, unknown entries ignored), GitHub, Slack, Standard Webhooks and a configurable generic HMAC (SHA-256 or opt-in legacy SHA-1, hex or base64, optional signed timestamp). It also adds idempotency-key extractors with vendor defaults and run-input selectors. Verification runs on raw body bytes, and a zero-size `VerifiedDelivery` token makes the type system enforce verify-before-parse. 33 integration tests and 5 unit tests pass, and clippy is clean.**

## Performance

- **Duration:** about 30 minutes
- **Completed:** 2026-11-20
- **Tasks:** 1 TDD feature (RED c8183e9, GREEN fb6632c; no refactor commit needed)
- **Files:** 3

## Vector provenance

| Vector | Source | Status |
|---|---|---|
| GitHub `sha256=757107ea…3e17` (secret `It's a Secret to Everybody`, body `Hello, World!`) | GitHub docs | recalled; re-checked with Python `hmac` and an in-test hand-built HMAC: correct |
| Slack `v0=a2114d57…b503`, ts 1531420618, signing secret `8f742231b10e8888abcd99yyyzzz85a5`, documented slash-command body | Slack docs | recalled; re-checked with Python and in-test HMAC: correct |
| Standard Webhooks `v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=` (`whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw`, `msg_p5jXN8AQM9LWM0D4loKWxJek`, 1614265330, `{"test": 2432232314}`) | spec reference libraries' test vector | recalled; re-checked with Python and in-test HMAC over the decoded key: correct |
| HMAC-SHA1 `effcdf6a…7c79` (`Jefe`), cases 1 and 6 | RFC 2202 | correct |
| SHA-1 `abc`, empty, 448-bit, 1M `a` | FIPS 180 examples | correct |
| Stripe, every generic-HMAC combination | computed in-test with an independent HMAC-SHA256 built by hand on `sha2` (not the `hmac` crate) | n/a |

No recalled vector needed correcting.

## Coverage (tests/webhook_verify.rs)

- Each verifier: valid accepted; tampered body, wrong secret, stale timestamp (both sides of the window, boundary ±300 accepted), malformed header and missing header rejected with the specific `HookError`.
- Stripe: rotation (bad v1 before or after the good one), a non-hex v1 skipped, `v0`/`secret`/`destination`/`payload`/unknown entries carrying a valid MAC for a *different* body are never used, no `t` → Malformed, only `v0` → BadSignature, `whsec_…` used as-is (a test with a base64-looking secret), custom tolerance, i64 extremes do not overflow.
- Standard Webhooks: multiple signatures in any order, `v1a` ignored, tampered id, non-base64 secret → Config.
- Generic HMAC: sha256 hex, hex + prefix, base64, legacy sha1 hex (RFC 2202) and base64, `{ts}.{body}` with and without a tolerance, and the invalid combinations as Config.
- Meta mapping: ordinals checked against `ostro-meta/src/records.rs` text, defaults, rejection cases, and `to_meta`/`from_meta` round trip for every kind.
- Extractors: parse/Display round trip and syntax errors, header and JSON-pointer extraction, number keys, missing → `None`, bad JSON, non-scalar, the 256-byte cap (256 ok, 257 TooLong), non-UTF-8 header, vendor defaults, `InputSelector` body/pointer/not-found/bad JSON.
- Grep gate: `webhook.rs` contains `verify_slice` and no `==`/`!=` in code.

## Verification

- `cargo test -p ostro-triggers --test webhook_verify --locked`: `test result: ok. 33 passed`.
- `cargo test -p ostro-triggers --locked`: lib 5 passed, cron 22 passed, webhook_verify 33 passed.
- `cargo clippy -p ostro-triggers --all-targets --locked -- -D warnings`: clean. `cargo fmt --check`: clean. No warnings.
- `cargo tree -p ostro-triggers -i chrono`: chrono is not in the graph. No clock reads in webhook.rs or extract.rs (time arrives as `now_unix`).

## Deviations from Plan

1. **[Rule 3 - Blocking] `from_meta` signature.** The plan specified `VerifierSpec::from_meta(&Webhook)`, but ostro-triggers does not depend on ostro-meta, and this plan may not edit Cargo.toml or Cargo.lock. It therefore takes the three fields one-to-one. A test keeps the mirrored ordinals in sync with records.rs.
2. **[Rule 2 - Correctness] `slack_challenge` takes `&VerifiedDelivery`.** This enforces the "nothing parses the body before verification" truth. The extractors take the token as the plan required.
3. **[Rule 3 - Blocking] SHA-1 implemented locally.** Generic HMAC needs SHA-1, which no dependency of this crate provides. The `sha1` 0.11 crate is in Cargo.lock, but adding it would edit Cargo.toml and Cargo.lock. Hex and base64 decoders are also local, as the plan allowed. All of these are unit-tested.
4. `VerifiedDelivery` derives `PartialEq`/`Eq` so tests can `assert_eq!` on `Result`s. It carries no data, so this has no security effect.

## Notes for 08-10 / 08-15

- Validate at create with `VerifierSpec::from_meta(w.verifier, &w.verifier_config, w.tolerance_s)`, `IdempotencyExtractor::parse(&w.idempotency)` and `InputSelector::from_meta(&w.input_ref)`. Use `GenericHmacSpec::to_config()` to write the blob.
- Pass `now_unix` from the injected `Clock`, as seconds. `run_ref` (run id or token) can use `extract::json_pointer_string`.
- The plan's TRIG-01 requirement completes with the endpoint (08-15), so it is not marked here.

## Self-Check: PASSED

- FOUND: crates/ostro-triggers/src/webhook.rs, crates/ostro-triggers/src/extract.rs, crates/ostro-triggers/tests/webhook_verify.rs
- FOUND: c8183e9 (RED), fb6632c (GREEN) on main
