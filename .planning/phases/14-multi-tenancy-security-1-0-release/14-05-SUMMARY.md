---
phase: 14-multi-tenancy-security-1-0-release
plan: 05
subsystem: signed modules (pure verification core)
tags: [p14, security, signing, minisign, cosign, sigstore-bundle, tdd, SEC-03]
requires:
  - phase: 14-01
    provides: "minisign-verify =0.3.0 + direct aws-lc-rs in ostro-server; ext 113 ModuleSig (key_fingerprint), ext 115/116 TrustedKey, SIG_ALG_* constants"
  - phase: 14-01b
    provides: "empty p14/signing.rs stub, rsign2 0.6.7 pin in scripts/fetch-tools.sh"
provides:
  - "crates/ostro-server/src/p14/signing.rs: parse_trusted_key, parse_signature, verify_module, reverify, key_id_hex, key_fingerprint, ModuleSigEvidence::to_module_sig, TrustedKey::{to_meta, from_meta}"
  - "rsign2 interop vectors (prehashed ED) + legacy Ed vector in crates/ostro-server/tests/fixtures/signing"
affects: [14-07, 14-19, 14-02]
tech-stack:
  added: []
  patterns:
    - "minisign verification delegated to minisign-verify (format authority); key id, alg and stored bytes read with base64 after minisign-verify accepted the input"
    - "evidence stores re-verifiable bytes: normalised .minisig text (minisign) or DER (cosign), so reverify needs only the blob and the key set"
key-files:
  created:
    - crates/ostro-server/tests/p14_signing.rs
    - crates/ostro-server/tests/fixtures/signing/{README.md,blob.bin,minisign.pub,blob.bin.minisig,blob.bin.legacy.minisig}
  modified:
    - crates/ostro-server/src/p14/signing.rs
    - crates/ostro-server/src/lib.rs
key-decisions:
  - "key_fingerprint = sha256(key-family alg byte ‖ public_key) with alg byte 1 for every minisign key (minisign public keys always carry Ed) and 3 for cosign: one fingerprint per key, independent of whether a signature was Ed or ED"
  - "ModuleSig.sig = normalised four-line .minisig text for algs 1/2 (the global signature is needed to re-verify) and raw DER for alg 3 (a bundle's messageDigest is checked at upload only)"
  - "cosign TrustedKey.key_id = sha256(canonical SPKI DER)[..8]; public_key is the aws-lc-rs re-encoded SPKI; the bundle hint only orders key attempts (every cosign key is tried)"
  - "when no signature verifies, the most specific error wins: DigestMismatch > BadSignature > Malformed/Keyless/TooLarge > UnknownKey; empty list = Unsigned (14-19 maps it to 403 signature_required)"
  - "legacy Ed interop vector came from the test signer with the rsign2 throwaway key (rsign2 0.6.7 has no legacy signing mode)"
requirements-completed: []
duration: ~2h active (+ cluster queue waits)
completed: 2026-11-27
---

# Phase 14 Plan 05: Module signature verifiers Summary

**Pure SEC-03 verification core: minisign `Ed`/`ED` via `minisign-verify` (key ids matched first, trusted-comment global signature checked) and cosign keyed ECDSA P-256 via aws-lc-rs from a raw base64 DER signature or a keyed Sigstore bundle v0.3 (digest checked, keyless refused), with trusted-key parsing, ext 113 evidence, fingerprint-matched re-verification for activation, input limits, a never-panic proptest and a real rsign2 interop vector.**

Requirement ids are not ticked here (14-27's, D-P14-35). The `sigstore` crate is not used.

## API as landed (`crates/ostro-server/src/p14/signing.rs`)

```rust
pub const MAX_SIG_BYTES: usize = 8192;  pub const MAX_KEY_BYTES: usize = 8192;
pub const MAX_COMMENT_BYTES: usize = 1024;  pub const MAX_SIGS: usize = 4;

#[repr(u8)] pub enum Alg { MinisignEd = 1, MinisignEdPrehashed = 2, CosignP256 = 3 }
//   as_u8, from_u8, family
pub enum KeyFamily { Minisign, Cosign }            // alg_byte(): 1 / 3
pub struct TrustedKey { pub alg_family: KeyFamily, pub key_id: Vec<u8>, pub public_key: Vec<u8>, pub comment: String }
//   to_meta() -> ext::TrustedKey;  from_meta(&ext::TrustedKey) -> Result<Self, SigError> (re-validates)
pub enum SigError { TooLarge(&'static str), Malformed(String), UnknownKey, BadSignature,
                    DigestMismatch, KeylessUnsupported, Unsigned }   // thiserror, Clone/PartialEq
pub struct ParsedSig { .. }                        // alg(), key_id(), hint(), stored_bytes()
pub struct ModuleSigEvidence { pub alg: Alg, pub key_id: Vec<u8>, pub key_fingerprint: [u8; 32], pub sig: Vec<u8> }
//   to_module_sig(added_at_ms, added_by) -> ext::ModuleSig

pub fn parse_trusted_key(text: &str) -> Result<TrustedKey, SigError>;   // RW… | .pub file | PEM SPKI P-256
pub fn parse_signature(raw: &[u8]) -> Result<ParsedSig, SigError>;      // .minisig | base64 DER | bundle v0.3 JSON
pub fn verify_module(bytes: &[u8], sigs: &[ParsedSig], keys: &[TrustedKey]) -> Result<Vec<ModuleSigEvidence>, SigError>;
pub fn reverify(bytes: &[u8], stored: &[ext::ModuleSig], keys: &[TrustedKey]) -> Result<Vec<ModuleSigEvidence>, SigError>;
pub fn key_id_hex(k: &TrustedKey) -> String;       // minisign: LE u64 upper hex (as rsign prints); cosign: lower hex
pub fn key_fingerprint(k: &TrustedKey) -> [u8; 32];// sha256(alg byte ‖ public_key)
```

Semantics for 14-19:
- `verify_module` succeeds when at least one signature verifies; the evidence lists every verified signature in input order (duplicates dropped), none of the failed ones. Errors: `Unsigned` (no signatures, map to 403 `signature_required`), anything else to 400 `signature_invalid`. More than 4 signatures → `TooLarge`.
- minisign: only keys with the signature's 8-byte key id are tried (none → `UnknownKey`); several keys may share an id. cosign: every cosign key is tried, the bundle hint (`base64(sha256(SPKI DER))`, cosign's format) only orders attempts. A bundle `messageDigest` must be `SHA2_256` and equal sha256 of the bytes (`DigestMismatch`).
- Bundles: mediaType `application/vnd.dev.sigstore.bundle.v0.3+json` or `…bundle+json;version=0.3`; `verificationMaterial.publicKey` required (certificate / x509CertificateChain / tlogEntries without it → `KeylessUnsupported`); DSSE envelopes → `Malformed`.
- `reverify` (activation / version gate): for each stored `ModuleSig`, only keys with the same family and `key_fingerprint` are used, so a different key with the same key id never re-validates evidence; the blob is re-verified each time.
- Timestamps are passed in (`to_module_sig(added_at_ms, added_by)`); the module reads no clock and does no I/O.

## Interop vectors (`crates/ostro-server/tests/fixtures/signing`)

rsign2 0.6.7 from `bash scripts/fetch-tools.sh rsign2`, run in `~/.local/share/ostro-orchestrator/scratch/14-05/keys` (outside the repo):

```sh
python3 -c "import hashlib; open('blob.bin','wb').write(b''.join(hashlib.sha256(b'ostro-14-05-blob-%d' % i).digest() for i in range(128)))"
rsign generate -W --unencrypted -p minisign.pub -s minisign.key -c "ostro 14-05 throwaway test key (secret discarded)"
rsign sign -W -s minisign.key -p minisign.pub -x blob.bin.minisig -t "ostro 14-05 interop vector" blob.bin
rsign verify -p minisign.pub -x blob.bin.minisig blob.bin   # Signature and comment signature verified
```

- `blob.bin.minisig` (prehashed `ED`) is from the real rsign2 CLI; key id `EE7DBB47A76AEB52`.
- `blob.bin.legacy.minisig` (legacy `Ed`) came from the **test signer**: rsign2 0.6.7 has no legacy signing flag (`-l` does not exist; `rsign verify` refuses legacy signatures). The Ed25519 seed was read from the unencrypted `minisign.key`, checked against `minisign.pub`, and the legacy signature + global signature were produced with Python `cryptography`. Labelled so in the fixture README.
- `minisign.key` was destroyed with `shred -u`; it was never in the repository.

## Tests (`crates/ostro-server/tests/p14_signing.rs`, 24 tests)

minisign legacy verify, wrong key id → `UnknownKey`, tampered blob / tampered trusted comment → `BadSignature`, same key id different key; bare and `.pub` key forms + `key_id_hex`; cosign raw DER (+ trailing newline), tampered/wrong key, bundle with and without digest, digest mismatch, keyless bundle, malformed bundle shapes; fingerprints (same key id, different keys), evidence → `ModuleSig` → `reverify` round trip, `TrustedKey` meta round trip, reverify needs the fingerprint, reverify over tampered bytes / altered stored sig, empty → `Unsigned`; multiple signatures (evidence only for valid ones, error precedence); limits (`> 8 KiB`, `> 4` sigs, comment `> 1 KiB` for signatures and keys); malformed inputs; both interop vectors; proptest 512 cases each for `parse_signature`, `parse_trusted_key` and `reverify` (arbitrary + near-valid inputs, no panics).

## Task Commits

1. **RED** `1cd2a8e` test(14-05): failing signature verification tests (compile failure: API absent)
2. **GREEN** `85af020` feat(14-05): minisign + cosign keyed verification

Both landed on main in one CAS (73a435b → 85af020) so main never carried a failing test. No REFACTOR commit was needed.

## Verification

- Task verify (plan command) on the worktree at 85af020: `test result: ok. 24 passed`; no `.key`/secret fixture; `cargo tree` has no `sigstore`; `minisign_verify::` present. Exit 0.
- `cargo clippy -p ostro-server --lib --tests -D warnings` (default and `--no-default-features`) clean; `cargo fmt -p ostro-server --check` clean; `scripts/p12-grep-gates.sh` clean.
- Plan gate: `ostro-k8s-ci --parallel` on the worktree at 85af020 (run p173358-29664, throwaway 974cc933 = 85af020 + the untracked SUMMARY draft). Three non-PASS results, none caused by this plan (it adds pure code that nothing calls yet):
  - **lint group pod evicted** ("API unreachable for 2 min", pod disappeared, BackoffLimitExceeded) during "Phase 13 tier C". Before that, its log shows these steps finished with no failure: fmt, clippy -D warnings, warning-free build, sdk workspace, guests, order-saga, semantics sleepers, failpoint list sync, Phase 13 mutant matrix (8 bites, none skipped) and engine wit-next. The remaining steps were rerun on slot 3 with `CI_LOCAL_ONLY` (`20261126-220828-s3-61187921-cmd.log`). All passed: Phase 13 tier C (6490 s), phase 7 failpoint legs + manifests, cargo deny check, check-vendored.sh, check-deny-fixtures.sh, no chronik/tokio-uring/prometheus, one log path + P12 grep gates, P13 grep gates, workflow validation.
  - **cargo test (test2) and musl zigbuild test (musl2)**: `backup_cli::bucket_online_and_offline_round_trips` hit "/readyz not 200 within 60s". This test is on the known load-only list. It failed again while tier C loaded the node, then passed alone on slot 3 twice (2/2, `20261127-023458-s3-466d915c-cmd.log`).
  - **ostro-build polyglot**: `template_tests::rust_template_test_passes` timed out: the template's `cargo test` did not finish within 900 s. The step also timed out when rerun alone on slot 3 (4/5, `20261127-042340-s3-966b29d0-cmd.log`), and in 14-06's gate (3422767). It **passed** in four other runs whose commits contain 85af020 (cbae331, 14-31 b3e7208, 14-09 a885319, 14-03 5ea700b), so it is load-sensitive. Proposed for the brief's known-flake list.
  - PASS: polyglot group (conformance, clients python/ts/go, clippy polyglot), failpoint suites, replication mutant matrix, server campaign smoke (20 kills), musl release, musl binary size limit, test1.
  - Slot note: the slot-1 rerun was cancelled while still queued, by the orchestrator's instruction, and resubmitted on slot 3.

## Deviations from Plan

**1. [Rule 3 - Blocking/brief] lib.rs anchor removal (file outside files_modified)**
- Per the orchestrator's p14-notes ("delete the `use <crate> as _;` line once your plan really uses the crate"), the `use minisign_verify as _;` anchor in `crates/ostro-server/src/lib.rs` was deleted and the comment trimmed. 14-03 had removed the `aws_lc_rs` anchor concurrently; the rebase conflict was resolved to keep only the `ostro_kafka` anchor (14-32). Commit 85af020.

**2. Legacy interop vector from the test signer** (anticipated by the plan): rsign2 0.6.7 has no legacy mode.

No crash checker, oracle, manifest or workflow was touched. No ci.yml hunks.

## Notes for later plans

- 14-07 `signature_parse` fuzz target: call `parse_signature` / `parse_trusted_key` / `TrustedKey::from_meta` on arbitrary input; the proptests in `p14_signing.rs` are the seed shapes.
- 14-19: store `ev.to_module_sig(clock_ms, token_id)` in ext 113; at activation build the tenant's effective keys with `TrustedKey::from_meta` over ext 115 ∪ ext 116 and call `reverify(blob, &stored, &keys)`.

## Self-Check: PASSED

All seven files are on main; commits 1cd2a8e and 85af020 are ancestors of main.
