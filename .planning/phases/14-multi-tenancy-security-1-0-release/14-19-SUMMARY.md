---
phase: 14-multi-tenancy-security-1-0-release
plan: 19
subsystem: signed-module enforcement, security API/CLI, system-secret sealing
tags: [p14, sec-03, signed-modules, minisign, cosign, security-api, reseal, cli]
requires:
  - phase: 14-02
    provides: "Ostro-Module-Signature carriage, /v1/security/modules/* + /v1/modules/{sha}/signatures adapters, FakeSecurity"
  - phase: 14-04
    provides: "audit sink (system.reseal records)"
  - phase: 14-05
    provides: "p14/signing.rs: parse_trusted_key, parse_signature, verify_module, reverify, key_fingerprint, key_id_hex"
  - phase: 14-15
    provides: "authz rows (policy get = system read, set = system admin, keys = tenant admin, attach = deploy)"
  - phase: 14-16
    provides: "tenant_scope (TrustedKeySpec.tenant forced, attach_signatures own-module check)"
provides:
  - "p14/module_sign.rs: effective policy (ext 115 or [security.modules], ext 116 tightening), upload_gate, evidence_ext, merge_evidence, version_gate, activation_gate"
  - "p14/security_api.rs: SecurityService (SecurityApi), set_tenant_require, [security.modules] seed/drift (seed_once, seed_task)"
  - "p14/reseal.rs: __ostro/ui-session-key created/re-sealed with aes256gcm, ext 117, system.reseal audit; token-key / cluster-ca deferred"
  - "ostro-client security.rs, ostro security policy|trust verbs, ostro deploy --signature"
affects: [14-18, 14-21, 14-24, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "policy read = meta ext 115, else the node's [security.modules] (fail closed on an undecodable ext)"
    - "evidence re-verified over the blob by key fingerprint at every activation / explicit version selection"
key-files:
  created:
    - crates/ostro-server/tests/signed_modules.rs
    - crates/ostro-server/tests/p14_reseal.rs
    - crates/ostro-cli/tests/p14_security_cli.rs
    - crates/ostro-cli/tests/snapshots/p14_security_cli__{deploy_signature_refused,security_human,security_json,security_usage}.snap
  modified:
    - crates/ostro-server/src/p14/{module_sign,security_api,reseal}.rs
    - crates/ostro-client/src/security.rs
    - crates/ostro-cli/src/commands/{security,deploy}.rs
    - "deviations: crates/ostro-server/tests/p14_skeleton.rs (1 assertion: security slot filled), crates/ostro-cli/src/lib.rs + crates/ostro-client/src/lib.rs (base64 anchors dropped, p14-notes rule)"
key-decisions:
  - "Cluster policy = ClusterInfo ext 115; while meta has none the node's [security.modules] applies locally, and ANY node holding the section seeds ext 115 (CAS, forwarded to the meta leader), not only the leader: a leader-only seed never happens when the configured node is a follower"
  - "Supplied but invalid signatures are 400 signature_invalid even when signing is not required; none supplied + required = 403 signature_required"
  - "Below finalized level 2 signatures are verified but ext 113 is not written (compat gate); policy/key writes answer 409 feature_not_enabled"
  - "PUT policy with an empty trusted_keys keeps the cluster keys (a non-empty list replaces them), so `policy set --require-signed` never wipes keys"
  - "Key ids: wire key_id must equal signing::key_id_hex or be \"auto\"; ids are unique across the cluster set and a tenant's set (409); remove accepts <tenant>:<key id>; a tenant principal only reaches its own tenant's keys (404 otherwise, record tenant checked via auth::current())"
  - "Evidence merge: one signature per key fingerprint (newest wins), at most 4 (oldest dropped) so reverify's MAX_SIGS bound always holds"
  - "Module bytes for re-verification come from the local blob store; a node without the blob answers 503"
  - "Reseal limited to __ostro/ui-session-key: __ostro/token-key and __ostro/cluster-ca readers decode raw bytes (needs orchestrator decision, see below)"
requirements-completed: []
duration: ~3h
completed: 2026-11-27
---

# Phase 14 Plan 19: Signed-module enforcement, security API/CLI, system-secret sealing Summary

**Signed modules can now be enforced per cluster. With `require_signed_modules` on (`ClusterInfo` ext 115, or a tenant's ext 116 tightening it), an upload with no signature is rejected 403 `signature_required` and one whose signatures all fail is rejected 400 `signature_invalid`. Both answers come before the existing-sha lookup, compilation and any meta write. Valid minisign and cosign-keyed signatures are stored as ext 113 evidence carrying key fingerprints. Every activation, explicit-version start, migrate target and rerun target re-verifies that evidence over the blob, using the effective keys of the tenant doing it. Versions that are already active keep serving. The policy is read from meta, so a node without `[security.modules]` still enforces it. Also in this plan: `SecurityApi`, `ostro security …`, `ostro deploy --signature`, and sealing of `__ostro/ui-session-key` with aes256gcm. Sealing `__ostro/token-key` and `__ostro/cluster-ca` is deferred; it needs an orchestrator decision.**

Requirement ids are not ticked here (14-27's, D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| cbd3eb2 | 1 | module_sign gates + evidence, SecurityApi, tests/signed_modules.rs |
| 4e5e4be | 2 | reseal (ui-session-key) + seed task wiring, tests/p14_reseal.rs, p14_skeleton assertion |
| b415d9f | 3 | ostro-client security, `ostro security` verbs, `deploy --signature`, tests/p14_security_cli.rs |

## Enforcement matrix as tested (`tests/signed_modules.rs`, 3/3 PASS locally)

| Situation | Answer |
|---|---|
| policy off, unsigned upload | 201 |
| policy off, signature over other bytes | 400 `signature_invalid` (users notice) |
| policy on, unsigned upload | 403 `signature_required`, no ModuleRecord |
| policy on, minisign signature over other bytes | 400 `signature_invalid` (`details.key_ids` names the key), no ModuleRecord |
| policy on, valid minisign (legacy `Ed`) | 201, ext 113 = 1 sig, fingerprint = sha256(0x01 ‖ pk), `added_by` = caller (`dev`) |
| policy on, valid cosign keyed (raw base64 DER) | 201, ext 113 = 1 sig |
| active unsigned v1 after enforcement | starts keep working (201); a run in flight across the switch completes |
| activate v2 uploaded unsigned before enforcement | 403 → attach (`POST /v1/modules/{sha}/signatures`, bad sig 400) → 200 |
| start with explicit unsigned `version` | 403, run listing unchanged |
| `migrate-runs` 1→2 (`dry_run: false`) to unsigned | 403 `signature_required` (before any RepinRun) |
| rerun with `version: {version: 2}` unsigned | 403 |
| duplicate key id | 409 |
| trusted key removed | activation of a version signed only by it → 403 |
| new key with the same minisign key id, different public key | evidence still refused (fingerprint mismatch) → 403 |
| tenant tightening: a, b `Some(true)`, cluster off | a activates with its tenant key evidence (200); b's activation of the same sha → 403 (evidence from a's tenant-only key) |
| B1b: b re-uploads a's sha unsigned | 403; no `b/replay_demo@3`, b's Active row unchanged, ModuleUploaders unchanged |
| b uploads with a's tenant-only key signature | 400 |
| a relaxed (`None`), b tight | a's unsigned upload 201, b's 403 (no record) |
| tenant principal b removes a's key (`ID` or `a:ID`) | 404; a tenant admin cannot set `Some(false)` (403) or touch another tenant (404); own key removal OK |
| 3 nodes, `[security.modules]` on node 1 only | seed reaches node 3's meta; unsigned upload through node 3 → 403 (no record on any node); signed → 201 |
| meta policy changed afterwards | node 1 `seed_once` = Drift (WARN once + `ostro_security_policy_drift` = 1), meta policy applies on node 1 |

Unit tests: module_sign 6 (tightening never relaxes, key union + bad keys skipped, merge cap, default tenant, key id display, absent section), security_api 3.

## Evidence format (ext 113 `ModuleSignatures.sigs[]`)

```text
minisign: ModuleSig { alg: 1 (Ed) | 2 (ED), key_fingerprint: sha256(0x01 ‖ ed25519_pk[32]),
          key_id: 8 bytes in file order (displayed reversed upper hex, e.g. "316D2D6F7274736F" for b"ostro-m1"),
          sig: normalised 4-line .minisig text, added_at_ms: injected clock, added_by: "otk_…" | "dev" | "system" }
cosign:   ModuleSig { alg: 3, key_fingerprint: sha256(0x03 ‖ SPKI DER), key_id: sha256(SPKI DER)[..8]
          (lower hex), sig: ASN.1 DER ECDSA bytes, added_at_ms, added_by }
```

The wire form of a key (`GET /v1/security/modules/policy`) is `{alg: "minisign"|"cosign-p256", key_id, public_key: "RW…" | PEM, comment, tenant}`.

## Reseal results (`tests/p14_reseal.rs`, 3/3 PASS locally)

- With `[secrets] sealing_key_file`: `__ostro/ui-session-key` is created at startup with key_id `aes256gcm:<kid>`. It opens to 32 bytes with the key, and `system.reseal` is in `<data>/audit`. A second `reseal::run` returns `AlreadySealed`. A restart leaves the record version and bytes unchanged.
- A planted `plaintext-v0` ui-session-key is re-sealed after a restart with a key: same value, ext 117 `rotated_at_ms` set.
- No key: nothing changes. In token mode `ResealReport.warned` is set and a WARN is logged.
- `__ostro/token-key` and `__ostro/cluster-ca` stay `plaintext-v0`, and the await-token key is the same after the restart (pre-reseal tokens keep verifying, joins keep the CA). See the decision below.

## Deviations from Plan

### Auto-fixed / adjusted

1. **[Rule 1] Seed by any configured node, not only the meta leader.** With a leader-only seed, a cluster whose configured node is a follower never gets the policy (found while writing the 3-node test). The CAS on `ClusterInfo` still makes it a single write.
2. **[Rule 3] p14_skeleton asserted `ext.security.is_none()`** (stub behaviour). It now asserts `is_some()`; 1 assertion changed.
3. **[Rule 3] Tenant-level require has no wire field.** `ModulePolicy` (14-02 DTO) carries no tenant, and authz makes policy get/set system-only. `SecurityService::set_tenant_require` implements the ext 116 rule (tenant principal: `Some(true)` on its own tenant only) and is covered by tests. `ostro security policy set --tenant` is refused locally with a clear message. `policy get --tenant t` filters the keys shown client-side.
4. **`trust add` clap shape changed** from a positional key file to `--minisign <KEY|FILE> | --cosign <FILE>` (plan wording). The CLI computes minisign key ids; cosign sends `key_id = "auto"` and the server derives it from the canonical SPKI.
5. **`trust rm --tenant t`** sends `t:<key id>`. The server accepts that form so a system admin can resolve a key id held by several tenants (409 otherwise).
6. **base64 anchors removed** from ostro-cli / ostro-client `lib.rs`, because the crate is now used (p14-notes rule).
7. **Deploy:** `--signature` works on the `.wasm` form only. With a project directory it is a usage error, because the signature must cover the built component.

### Deferred (needs an orchestrator decision)

- **Sealing `__ostro/token-key` and `__ostro/cluster-ca` (D-P14-30) is not done.** Their readers decode raw record bytes without a key ring:
  - `MetaState::token_key()` (ostro-meta), used by `server.rs` (2 sites), `cluster.rs`, `control/lifecycle.rs`, `workers/remote_executor.rs` and `examples/export_run.rs`;
  - `p13::join::stored_ca` and `p13::joiner::import_cluster_ca`.

  Sealing them now would break await tokens and node joins on every node. `server.rs` is 14-01b-only (shared-file rule = STOP). The routine is generic (`reseal::reseal_one`), so adding both names to `reseal::RESEAL_NOW` is the whole change once the readers open aes256gcm records through the node's key ring. The proposed owner is a fixup or 14-26: route those readers through a `SealedStore`-aware helper, and require the same sealing key on every node, as Phase 6 sealed secrets already do.

## Verification

- Task 1 verify (`--test signed_modules`, 3 passed) PASS; Task 2 verify (`--test p14_reseal`, 3 passed, clock grep on the diff empty) PASS; Task 3 verify (`--test p14_security_cli`, 4 passed; `ostro deploy --help` lists `--signature`) PASS.
- Also local: ostro-server `--lib` p14 unit tests, `p14_skeleton`, `p14_guard`, `auth_scopes`, `tenant_isolation` PASS. `cargo test -p ostro-cli -p ostro-client` all suites PASS. clippy `-D warnings` on ostro-server (lib + both new tests), ostro-cli and ostro-client (lib + tests) clean. `cargo fmt -p ostro-server -- --check` clean. `scripts/p12-grep-gates.sh` PASS.

**Plan gate: gate incomplete — cluster withdrawn; consolidated gate pending on dell-4.** Not verified here:
- every `scripts/ci-local.sh` step at b415d9f (lint group incl. workspace-wide clippy/fmt/deny/OpenAPI snapshot, test1/test2 cargo-test shards, failpoint suites, polyglot/clients, ostro-build, musl release + "musl binary size limit" + zigbuild);
- the ostro-server and ostro crates' full test suites (only the suites listed above ran);
- musl binary size after this plan (no measurement; the plan adds no dependency).

## Next / hand-offs

- 14-21: `__ostro/ui-session-key` exists sealed (aes256gcm) when a sealing key resolves. Open it with the node key ring (`reseal::load_ring` + `SealedStore::open`). Without a key it is absent, so create or handle it there.
- 14-26 / fixup: token-key + cluster-ca reseal (above). Optionally add a `tenant` field to the wire `ModulePolicy` for tenant-level require.
- 14-27: SEC-03 tick; no ci.yml change from this plan.

## Self-Check: PASSED

- Files: crates/ostro-server/src/p14/{module_sign,security_api,reseal}.rs, tests/{signed_modules,p14_reseal}.rs, crates/ostro-client/src/security.rs, crates/ostro-cli/src/commands/{security,deploy}.rs, crates/ostro-cli/tests/p14_security_cli.rs + 4 snapshots exist at b415d9f.
- Commits cbd3eb2, 4e5e4be, b415d9f are on main.
