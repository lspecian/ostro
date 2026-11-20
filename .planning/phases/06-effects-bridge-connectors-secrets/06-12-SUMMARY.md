---
phase: 06-effects-bridge-connectors-secrets
plan: 12
subsystem: effects-bridge
tags: [object-store, s3, rusty-s3, presign, hmac, localfs, fx-05]
requires:
  - "08-03 ostro-objstore (frozen trait, LocalFs, S3Store, s3s-fs harness, CI SeaweedFS step)"
  - "06-04 Egress::operator_client, allows_connection"
  - "06-05 SecretResolver::{resolve_operator, resolve_reserved}, template, SecretValue"
  - "06-07 Connector contract, ConnectorsConfig, outcome helpers, bridge_fail!"
provides:
  - "ostro-objstore (additive): PutOpts, PresignMethod, PresignOpts, clamp_presign_expiry, ObjectStore::put_with / presign (default impls), S3 overrides, S3Store::with_client, LocalFs content-type sidecar + LocalFs::content_type"
  - "ostro_bridge::object: ObjectConnector (Connector, kind Object), ObjectConnConfig (object connection config v1), serve_local for the 06-16 route"
  - "ostro_bridge::object::presign_local: sign/verify/url, PresignKeyStore + MemoryPresignKeys, generate_key, key_name"
affects: [06-14 server seam (PresignKeyStore impl, public_base_url), 06-16 /v1/objects route, 06-19 canary object leg, 06-20 docs]
tech-stack:
  added: []
  patterns:
    - "Phase 6 additions to a Phase 8 crate are defaulted trait methods + new inherent constructors; no Phase 8 signature or semantics changed"
    - "Backend cache keyed by connection name, invalidated by config bytes or backend_ttl; credentials resolved only at build time"
key-files:
  created:
    - crates/ostro-objstore/tests/presign.rs
    - crates/ostro-bridge/src/object/config.rs
    - crates/ostro-bridge/src/object/presign_local.rs
    - crates/ostro-bridge/tests/object_local.rs
    - crates/ostro-bridge/tests/object_s3.rs
  modified:
    - crates/ostro-objstore/src/lib.rs
    - crates/ostro-objstore/src/local.rs
    - crates/ostro-objstore/src/s3.rs
    - crates/ostro-objstore/tests/localfs.rs
    - crates/ostro-objstore/tests/s3.rs
    - crates/ostro-bridge/src/object/mod.rs
key-decisions:
  - "Phase 8's retry policy (5 retries, 100 ms base) and 512-byte error-body truncation are kept; the plan's '3 attempts' / '1 KiB' text is superseded by the never-change-Phase-8 rule"
  - "LocalFs sidecar holds only content_type; e_tag stays Phase 8's sha256-at-read (not stored in the sidecar)"
  - "Key segments ending in .ostro-meta.json are reserved on LocalFs; put/put_if_*/put_multipart/delete drop a stale sidecar"
  - "S3Store::with_client lets the bridge use Egress::operator_client; S3Store::new is unchanged (it now delegates to with_client)"
  - "object-request has no content-type field (WIT 0.1.0 frozen): the connection config's content_type is used for put and signed into presign-put"
  - "access_key_id may be literal (SeaweedFS's CI key 'ostro' is under the 8-byte template minimum); secret_access_key and session_token must be {{secret:..}} templates"
  - "Connector failure kinds outside W8: not_found, invalid_request, object_error (retryable for transport/I/O/5xx/429); S3 error bodies never copied into failures"
  - "Local presign keys are cached in the connector after create/resolve; serve_local never creates a key (absent key -> 403)"
metrics:
  duration: "~80 min"
  completed: 2026-11-20
---

# Phase 6 Plan 12: Object Store Connector Summary

**FX-05 object effects run through the shared `ostro-objstore` crate. Phase 6 added `put_with` and `presign` to the 08-03 trait as defaulted methods, with S3 overrides over rusty-s3 `S3Action::sign`. The bridge `ObjectConnector` handles get/put/head/delete/presign-get/presign-put with the W7 encodings, caps, `connection:<name>` access, templated credentials and the trusted operator client. Local roots presign through an Ostro HMAC-SHA256 URL (`/v1/objects/{connection}/{key}?exp=&sig=`) whose key is the sealed `__ostro/presign/<connection>`.**

## ostro-objstore ownership (what Phase 6 added)

The crate was created by Phase 8 (08-01 skeleton, 08-03 trait and backends). At the start of this plan `crates/ostro-objstore` contained the full Phase 8 implementation, so the shared-crate rule applied. Nothing was removed or renamed, and no Phase 8 method's semantics changed:

| Item | Where | Notes |
|------|-------|-------|
| `PutOpts { content_type }` | lib.rs | new type |
| `PresignMethod { Get, Put }` + `as_str` | lib.rs | new type |
| `PresignOpts { content_type, response_content_type }` | lib.rs | new type |
| `MIN_PRESIGN_EXPIRY`, `MAX_PRESIGN_EXPIRY`, `clamp_presign_expiry` | lib.rs | 1 s ..= 7 d |
| `ObjectStore::put_with(key, body, &PutOpts)` | lib.rs | default: `put` |
| `ObjectStore::presign(method, key, expires, &PresignOpts) -> Result<Option<Url>>` | lib.rs | default: `Ok(None)` |
| `S3Store::put_with` | s3.rs | `content-type` inserted via `headers_mut()` before signing, sent on the request; same `send` retry path |
| `S3Store::presign` | s3.rs | `get_object`/`put_object` + `query_mut()` (`response-content-type`) / `headers_mut()` (`content-type`) + `.sign(clamped)` |
| `S3Store::with_client(bucket, prefix, opts, client)` | s3.rs | `new` now builds its client, then delegates; behaviour identical |
| LocalFs `put_with` + sidecar `<key>.ostro-meta.json`, `LocalFs::content_type(key)` | local.rs | sidecar written tmp + fsync + rename under the root lock; hidden in `walk`/`list_blocking`; reserved suffix; stale sidecar dropped by put/put_if_*/put_multipart/delete |
| LocalFs `presign` | — | inherits the default `Ok(None)` |

`ObjectMeta` already had `e_tag`/`version`/`last_modified`, so that step was a no-op. Conditional puts and multipart are Phase 8's, unchanged. The crate-level doc in lib.rs records the ownership.

**rusty-s3 0.12.0 API (checked in `~/.cargo/registry/src/.../rusty-s3-0.12.0`):** `S3Action::{sign(Duration) -> Url, sign_with_time, query_mut() -> &mut Map, headers_mut() -> &mut Map}` (headers lower-case and signed; host always signed), `Map::insert(K, V)`, `Bucket::{get_object, put_object}(Option<&Credentials>, key)`, `Credentials::{new, new_with_token}`. `sign` documents the S3 range 1 s to 604800 s.

## SeaweedFS / S3 status

- **Local:** no Docker in WSL, so every `OSTRO_TEST_S3_*` leg printed `SKIP: OSTRO_TEST_S3_URL unset (human_needed / CI SeaweedFS)` and passed.
- **In-process s3s-fs (Phase 8's dev harness), always run:** presign verification **is supported** there. s3s checks SigV4 query signatures. A presigned GET is fetched by a plain reqwest client (the `response-content-type` override is honoured: `text/plain`). A presigned PUT carrying the signed content type uploads. A PUT with a different content type gets 403, a flipped signature digit gets 403, and a 1 s URL gets 403 after 2.1 s. `put_with` content type round-trips (a presigned GET answers `text/html`).
- **CI:** the existing `Start SeaweedFS` step and its `OSTRO_TEST_S3_*` env are reused unchanged, and no MinIO was introduced. The workspace `cargo test` on linux-x86_64-gnu runs `presign_external_s3`, `external_s3_put_with_content_type` and the bridge's `s3_every_op_and_presign_against_external_store` against SeaweedFS. ci.yml was not edited. The dedicated "ostro-objstore S3 conformance (SeaweedFS)" step still runs only `--test s3`; the new external legs live in `--test s3` and `--test presign` (presign runs in the workspace test step).

## Bridge API (for 06-14 / 06-16)

```rust
// ostro_bridge::object
pub struct ObjectConnector;
impl ObjectConnector {
    pub fn new(meta: Arc<dyn MetaView>, resolver: SecretResolver, operator_client: reqwest::Client,
               cfg: ConnectorsConfig) -> Self;   // uses backend_ttl, public_base_url, presign_default_expiry, object_max_get_bytes
    pub fn with_presign_key_store(self, store: Arc<dyn PresignKeyStore>) -> Self;
    pub async fn serve_local(&self, connection: &str, key: &str, method: &str /* GET|PUT */,
               exp: u64, sig: &str, body: Option<Bytes>) -> Result<LocalResponse, ServeError>;
}
impl Connector for ObjectConnector { /* kind Object; target = {connection name, config quota} */ }
pub type LocalResponse = (u16, Vec<u8>, Option<String>);       // (status, body, content type)
pub enum ServeError { Forbidden(VerifyError) /*403*/, NotFound /*404*/, BadRequest(String) /*400*/,
                      TooLarge /*413*/, Backend(String) /*500*/ }   // .status()
pub enum Op { Get, Put, Delete, Head, PresignGet, PresignPut }  // Op::parse
pub const NOT_FOUND = "not_found", INVALID_REQUEST = "invalid_request", OBJECT_ERROR = "object_error",
          FP_BEFORE_PUT = "bridge::object::before_put";
pub mod config { OBJECT_KIND = "object"; DEFAULT_MAX_PUT_BYTES = 64 MiB;
                 ObjectConnConfig::parse(&Connection, &ConnectorsConfig) -> Result<_, ConfigError>; full_key;
                 BackendConfig::{S3{..}, Local{root}}; CredentialTemplates (Debug redacted); ConfigError::to_failure -> connection_unknown }
pub mod presign_local { DOMAIN = "ostro/presign/v1"; ROUTE_PREFIX = "/v1/objects"; KEY_LEN = 32;
                 key_name(conn) -> "__ostro/presign/<conn>";
                 sign(key, PresignMethod, conn, obj_key, exp) -> Option<String /*hex*/>;
                 verify(key, method, conn, obj_key, exp, sig_hex, now) -> Result<(), VerifyError{Expired, BadSignature, Malformed}>;
                 url(base, conn, obj_key, exp, sig) -> Option<Url>;
                 trait PresignKeyStore { async fn create(&self, name, key: SecretValue) -> Result<SecretValue, Failure>; } // insert-if-absent, returns winner
                 MemoryPresignKeys; generate_key() -> Result<SecretValue, Failure> }
```

- **06-14:** implement `PresignKeyStore` by sealing `__ostro/presign/<connection>` through the meta log, insert-if-absent. Pass `Egress::operator_client(&egress_cfg)` and `ConnectorsConfig` with `public_base_url` defaulted to the API listen address.
- **06-16:** the `/v1/objects/{connection}/{key}` route calls `serve_local` and maps `ServeError::status()`. The key in the URL is the guest-visible key; the connection prefix is applied when the request is served.
- **Signed string pin:** `HMAC-SHA256(b"0123456789abcdef0123456789abcdef", "ostro/presign/v1\nGET\nfiles\na/b.txt\n1000") = fa56f9e794b7bc84d0440d6fd8690d17f8aa57a2bd3789b36d0a04900c567570`, cross-checked with Python `hmac`.

## Task Commits

1. **Task 1, ostro-objstore put_with/presign:** `8fab95d`
2. **Task 2, bridge object connector + local HMAC presign:** `d951b20`

## Verification

- `cargo test -p ostro-objstore --locked -- --test-threads=1`: lib **7 passed** (adds `presign_signs_content_type_and_clamps_expiry`). localfs **11 passed**. presign **3 passed** (LocalFs None, s3s-fs suite, external SKIP). s3 **14 passed** (adds s3s-fs put_with, with_client, external put_with SKIP).
- `cargo test -p ostro-bridge --locked -- --test-threads=1`: lib **137** (9 object), driver 14, egress_client 5, http_connector 24, **object_local 8**, **object_s3 3** (external SKIP), ratelimit 4, secrets 10, ssrf_table 5. All ok.
- `cargo test -p ostro-bridge --features failpoints --test object_local --locked -- --test-threads=1`: **9 passed** (adds the before_put failpoint test). After rebasing onto e905376 (06-11 landed): object_local 8, object_s3 3, lib object 9, all ok.
- `cargo clippy -D warnings`, warning-free:
  - `-p ostro-objstore --all-targets`
  - `-p ostro-bridge --all-targets --all-features`
  - `-p ostro-bridge --all-targets --no-default-features`
- `cargo fmt` is clean for both crates.
- `cargo tree -p ostro-objstore -e normal --locked` has no `chrono` (pipefail-safe check). No Cargo files were touched, and no dependency was added.
- `bash scripts/ci-local.sh` was **not** run. It takes about 40 minutes with four other executors building in parallel. Crate tests and clippy were run instead.

## Deviations from Plan

**1. [Shared-crate rule] Phase 8's retry/error policy was kept.** The plan text says "retry 3 attempts" and "body truncated 1 KiB". The 08-03 S3Store already retries 5 times with a 100 ms base and truncates bodies at 512 bytes. The phase rule says never change Phase 8's retry policy, so both are unchanged. `put_with` reuses the same `send` path.

**2. [Shared-crate rule] The LocalFs e_tag is not stored in the sidecar.** Phase 8 computes the sha256 at `head`/`get` time. The sidecar stores only `content_type`, and the e_tag values are identical.

**3. [Rule 3 - Blocking] `S3Store::with_client` was added.** `S3Store::new` builds its own reqwest client, but the plan requires `Egress::operator_client`. This is an additive constructor. `new` delegates to it, with the same validation and behaviour.

**4. [Rule 2 - Correctness] LocalFs sidecar hygiene.**
- Key segments ending in `.ostro-meta.json` are rejected, so a user key can never collide with a sidecar or be hidden from `list`.
- Every write path and `delete` drop a stale sidecar, so content types never outlive their object and directory pruning still works.

**5. [Plan gap] Content type source.** WIT 0.1.0 `object-request` has no content-type field (D14/D-FX8). The connection config gained an optional `content_type`, used by `put` (`put_with`) and signed into `presign-put`.

**6. [Rule 2 - Security] S3 error bodies are not copied into guest failures.** AWS `SignatureDoesNotMatch` bodies echo the access key id and the canonical request. Failures carry only `op`, key and status.

**7. [Plan wording] Failure kinds outside W8.** The plan names only `not_found`. The connector also uses `invalid_request` (bad key, missing put body) and `object_error` (backend failures). These are documented in `object/mod.rs` as plain error-info kinds, like `not_found`. `kinds::ALL` (W8) is unchanged.

**8. [Process] TDD.** The tests were written together with each module, not as separate RED commits, matching the 08-03 precedent. Each task is one commit.

Not touched: `crates/ostro-objstore/src/error.rs` (listed in files_modified, but no new error variant was needed).

## Self-Check: PASSED

- FOUND: every created and modified file listed above, at d951b20.
- FOUND: commits 8fab95d and d951b20 on main. `git show --stat` showed 6 objstore files (827+/18-) and 5 bridge files (2359+/1-) respectively.
