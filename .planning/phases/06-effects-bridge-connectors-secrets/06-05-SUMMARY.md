---
phase: 06-effects-bridge-connectors-secrets
plan: 05
subsystem: effects-bridge
tags: [secrets, templates, grants, scrubber, sealed-store, aes-gcm, providers]
requires:
  - "06-02 contract: SecretProvider/ProviderError, Origin/OriginPattern, MetaView/StaticMetaView, kinds"
  - "06-04 egress::config::parse_duration (reused for cache_ttl)"
provides:
  - "ostro_bridge::secrets: SecretValue, template::{parse, expand, contains, validate_name}, GrantSpec + check_grant, Scrubber, SealingKeyRing + SealedStore, SecretsConfig, EnvProvider/FileProvider/SealedProvider, ProviderChain, SecretResolver + ResolveCtx"
affects: [06-10, 06-11, 06-12, 06-13, 06-14, 06-16, 08 webhook secret refs]
tech-stack:
  added: []
  patterns:
    - "Every failure is Failure::{NonRetryable,Retryable}(ErrorInfo{kind, message}) with the secret NAME but never a value"
    - "Secret-bearing buffers are sized before secret bytes are written (no realloc copies) and zeroized"
key-files:
  created:
    - crates/ostro-bridge/src/secrets/value.rs
    - crates/ostro-bridge/src/secrets/template.rs
    - crates/ostro-bridge/src/secrets/grants.rs
    - crates/ostro-bridge/src/secrets/scrub.rs
    - crates/ostro-bridge/src/secrets/sealed.rs
    - crates/ostro-bridge/src/secrets/resolver.rs
    - crates/ostro-bridge/src/secrets/config.rs
    - crates/ostro-bridge/tests/secrets.rs
  modified:
    - crates/ostro-bridge/src/secrets/mod.rs
    - crates/ostro-bridge/src/secrets/providers/{sealed,env,file,chain}.rs
key-decisions:
  - "Scrubber uses aho-corasick MatchKind::Standard + overlapping search with merged ranges (not LeftmostLongest), so overlapping secrets are both fully removed"
  - "ProviderChain returns Err(Unavailable) (not None) when nothing was found and a provider was down; resolver maps it to Retryable secret_not_found. Invalid stops the chain (fail closed)"
  - "SealedProvider has a fixed tenant; the server (06-14) builds one chain per tenant"
  - "SecretResolver::with_sealed_store(store) supplies the store for resolve_reserved (the interface's new(meta, chain) has none)"
  - "resolve_operator also applies the 8-byte minimum (06-DECISIONS: refused at template resolution)"
  - "Template names additionally refuse `.` path segments (besides `..` and empty segments)"
metrics:
  duration: "~55 min"
  completed: 2026-11-20
---

# Phase 6 Plan 05: Secrets Core Summary

**This plan adds the secrets core of the bridge. It covers zeroized `SecretValue`s with redacted `Debug`, a strict hand-written `{{secret:NAME}}` parser and exact-capacity expander, and origin-bound grants over the existing `Grant` record. It adds an aho-corasick scrubber that covers raw, hex, percent, JSON and base64 at all three alignments, plus an aws-lc-rs AES-256-GCM sealed store over `SealedSecret` (AAD binds name and key_id, `plaintext-v0` stays readable, rotation keeps old kids readable). Also included: env, file and sealed providers, an ordered chain, and `SecretResolver`, which checks the grant before any fetch.**

## Final public API (for 06-10 / 06-11 / 06-12 / 06-13 / 06-14 / 06-16)

```rust
// ostro_bridge::secrets (re-exports at the module root)
pub struct SecretValue(SecretBox<[u8]>);
impl SecretValue {
    pub fn new(bytes: Vec<u8>) -> Self;                 // zeroizes spare capacity
    pub fn from_box(b: SecretBox<[u8]>) -> Self;
    pub fn from_slice(bytes: &[u8]) -> Self;
    pub fn expose(&self) -> &[u8];
    pub fn len(&self) -> usize;  pub fn is_empty(&self) -> bool;
    pub fn into_box(self) -> SecretBox<[u8]>;
    pub fn for_templating(self) -> Result<Self, Failure>;   // < 8 bytes -> NonRetryable secret_not_granted
}   // Clone; Debug = "SecretValue([REDACTED; n bytes])"; From<SecretBox<[u8]>>
pub const MIN_TEMPLATE_LEN: usize = 8;

pub mod template {
    pub const OPEN: &str = "{{secret:"; pub const CLOSE: &str = "}}";
    pub const RESERVED_PREFIX: &str = "__ostro/"; pub const MAX_NAME_LEN: usize = 128;
    pub struct Placeholder { pub name: String, pub range: Range<usize> }
    pub enum TemplateError { Unterminated{at}, InvalidName{at}, Reserved{name}, BadPath{at} }
    pub fn parse(s: &str) -> Result<Vec<Placeholder>, TemplateError>;
    pub fn contains(s: &str) -> bool;
    pub fn expand(s: &str, values: &BTreeMap<String, SecretValue>) -> Zeroizing<Vec<u8>>;
    pub fn validate_name(n: &str) -> Result<(), TemplateError>;
}

pub mod grants {
    pub struct GrantSpec { pub module: String, pub secret: String, pub origins: Vec<OriginPattern>, pub body: bool }
    impl GrantSpec {
        pub fn id(module: &str, secret: &str) -> String;          // "secret:<m>:<s>"
        pub fn validate(&self) -> Result<(), GrantError>;
        pub fn to_record(&self) -> Grant;
        pub fn from_record(g: &Grant) -> Result<Self, GrantError>;
        pub fn allows_origin(&self, origin: &Origin) -> bool;
    }
    pub enum GrantError { NotSecretGrant, BadSubject, BadModule, IdMismatch{..}, BadSecret(TemplateError), BadOrigin(OriginPatternError), UnknownScope, NoOrigins }
    pub fn check_grant(grants: &[Grant], module: &str, secret: &str, origin: &Origin, in_body: bool) -> Result<(), Failure>;
}

pub struct Scrubber;   // Debug = "Scrubber { patterns: n }"
impl Scrubber {
    pub fn new(values: &[&SecretValue]) -> Self;
    pub fn scrub_bytes(&self, b: &mut Vec<u8>) -> usize;   // # of [REDACTED] inserted; old buffer zeroized
    pub fn scrub_str(&self, s: &str) -> String;
    pub fn is_empty(&self) -> bool;
}
pub const scrub::REDACTED: &[u8] = b"[REDACTED]";

pub struct SealingKeyRing;   // Debug shows kids only
impl SealingKeyRing {
    pub fn load(cfg: &SecretsConfig, data_dir: &Path, dev: bool) -> Result<Option<Self>, SealError>;
    pub fn load_with(cfg, data_dir, dev, env: &dyn Fn(&str) -> Option<OsString>) -> Result<Option<Self>, SealError>;
    pub fn from_bytes(key: [u8; 32]) -> Self;
    pub fn rotate(&mut self, key: [u8; 32]);
    pub fn current_kid(&self) -> &str;  pub fn current_key_id(&self) -> String;   // "aes256gcm:<kid>"
}
#[derive(Clone, Debug)] pub struct SealedStore { ring: Arc<SealingKeyRing> }
impl SealedStore {
    pub fn new(ring: Arc<SealingKeyRing>) -> Self;
    pub fn ring(&self) -> &Arc<SealingKeyRing>;
    pub fn seal(&self, name: &str, plaintext: &[u8]) -> Result<SealedSecret, SealError>;
    pub fn open(&self, s: &SealedSecret) -> Result<SecretValue, SealError>;
}
pub enum SealError { KeyLength{source_name,len}, KeyFormat(String), Io{path,err}, UnknownKeyId(String), Malformed, Decrypt, Crypto }

pub struct SecretsConfig { pub providers: Vec<ProviderKind>, pub sealing_key_file: Option<PathBuf>,
    pub env_prefix: String, pub file_dir: Option<PathBuf>, pub vault: Option<VaultConfig>, pub k8s: Option<K8sConfig> }
impl SecretsConfig { pub fn validate(&self) -> Result<(), String>; }
pub enum ProviderKind { Sealed, Env, File, Vault, K8s }          // serde lowercase; as_str()
pub struct VaultConfig { addr, mount = "secret", auth: VaultAuth{Token|Approle|Kubernetes}, token_file, role_id, secret_id_file, k8s_role, cache_ttl = 30s }
pub struct K8sConfig { namespace, kubeconfig, cache_ttl = 30s }

pub struct EnvProvider;   impl EnvProvider { pub fn new(prefix) -> Self; pub fn var_name(&self, name) -> String; }   // Default = OSTRO_SECRET_
pub struct FileProvider;  impl FileProvider { pub fn new(dir) -> Self; pub fn dir(&self) -> &Path; }
pub struct SealedProvider; impl SealedProvider { pub fn new(meta: Arc<dyn MetaView>, store: SealedStore, tenant: Option<String>) -> Self; pub fn record_name(&self, name) -> String; }
#[derive(Clone, Default)] pub struct ProviderChain(Vec<Arc<dyn SecretProvider>>);
impl ProviderChain { pub fn new(v) -> Self; pub fn push(&mut self, p); pub fn names(&self) -> Vec<&'static str>; pub fn is_empty(&self) -> bool;
    pub async fn get(&self, name: &str) -> Result<Option<SecretValue>, ProviderError>; }

#[derive(Clone, Copy, Debug)]
pub struct ResolveCtx<'a> { pub module: &'a str, pub tenant: Option<&'a str>, pub run_id: &'a str, pub attempt: u32 }
#[derive(Clone)] pub struct SecretResolver { /* meta, chain, sealed: Option<SealedStore> */ }
impl SecretResolver {
    pub fn new(meta: Arc<dyn MetaView>, chain: ProviderChain) -> Self;
    pub fn with_sealed_store(self, store: SealedStore) -> Self;
    pub fn chain(&self) -> &ProviderChain;
    pub async fn resolve(&self, ctx: &ResolveCtx<'_>, name: &str, origin: &Origin, in_body: bool) -> Result<SecretValue, Failure>;
    pub async fn resolve_operator(&self, name: &str) -> Result<SecretValue, Failure>;
    pub async fn resolve_reserved(&self, name: &str) -> Result<Option<SecretValue>, Failure>;
}
pub const resolver::PRESIGN_PREFIX: &str = "__ostro/presign/";
```

### Failure mapping

| Situation | Failure |
|-----------|---------|
| Invalid or reserved name, no grant, origin mismatch, body without `body` scope, value < 8 bytes | `NonRetryable secret_not_granted` |
| Not in any provider | `NonRetryable secret_not_found` |
| Not found, and at least one provider `Unavailable` | `Retryable secret_not_found` |
| Provider `Invalid` (tampered sealed record, file escape) | `NonRetryable secret_not_found` |
| `resolve_reserved` with a non-`__ostro/presign/<x>` name | `NonRetryable secret_not_granted` |

### aws-lc-rs 1.19 API used (checked in the registry source)

- `aead::{RandomizedNonceKey, AES_256_GCM, Aad, Nonce, NONCE_LEN}`
- `RandomizedNonceKey::new(&AES_256_GCM, &key)`
- `seal_in_place_append_tag(Aad, &mut Vec<u8>) -> Result<Nonce, Unspecified>`, where the nonce is generated internally
- `open_in_place(Nonce::assume_unique_for_key([u8; 12]), Aad, &mut [u8]) -> Result<&mut [u8], _>`
- `digest::{digest, SHA256}` for the kid
- `rand::fill` for the dev key

## Notes for consumers

- **06-10 (http):**
  - Call `template::contains` on the URL, method and header names, and fail with `egress_denied`.
  - `template::parse` header values and the body.
  - Call `resolve(ctx, name, origin, in_body)` per name after the gates.
  - `expand` into a `HeaderValue` with `set_sensitive(true)`.
  - Build `Scrubber::new(&[&v...])` and scrub the body and every response header before encoding the outcome.
- **06-11/06-12 (connection configs):** use `resolve_operator`. **06-12 presign keys:** use `resolve_reserved("__ostro/presign/<conn>")`, which needs `with_sealed_store`.
- **06-13:** wrap the vault and k8s providers in `ProviderChain::new`. `SecretsConfig.vault` / `.k8s` are typed structs you can interpret directly.
- **06-14:** call `SealingKeyRing::load(&cfg.secrets, data_dir, dev)`. `Ok(None)` means the server refuses to store secrets. Build one chain per tenant, with `SealedProvider` carrying that tenant, in `providers` order. Skip `file` while `file_dir` is unset.
- **06-16 (admin set):** call `template::validate_name` (it refuses `__ostro/`), enforce the 8-byte minimum, then `SealedStore::seal("<tenant|_>/<name>", value)`.

## Verification

- `cargo test -p ostro-bridge --lib --locked -- secrets::template secrets::grants secrets::value`: 15 passed.
- `cargo test -p ostro-bridge --lib --locked -- secrets::scrub secrets::sealed secrets::config`: 17 passed.
- `cargo test -p ostro-bridge --lib secrets --locked`: 49 passed.
- `cargo test -p ostro-bridge --test secrets --locked -- --test-threads=1`: 10 passed.
- `cargo test -p ostro-bridge --locked -- --test-threads=1`, at a8750e1 on top of the current main: lib 84, egress_client 5, secrets 10, ssrf_table 5. All ok.
- Builds and clippy at a8750e1 are warning-free:
  - `cargo build -p ostro-bridge` (default features and `--no-default-features`);
  - `cargo clippy -p ostro-bridge --all-features --all-targets -D warnings`;
  - `cargo clippy -p ostro-bridge --no-default-features --all-targets -D warnings`.
- A grep of `src` for `format!`/`{:?}`/`tracing` on `expose()` finds only a test-local canary string. The only two `tracing::warn!`s log a path and a provider/secret name, never a value.
- Canary tests cover `Debug` output of `SecretValue`, `Scrubber`, `SealingKeyRing`, `SealedStore`, `SealedProvider`, `ProviderChain` and `SecretResolver`, plus every error message checked in tests.
- `bash scripts/ci-local.sh` was **not** run. The plan names it as the gate, but it takes about 40 minutes with 4 other executors building in parallel. Crate-level tests and clippy were run instead.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 2 - Correctness] Scrubber uses overlapping search plus merged ranges instead of LeftmostLongest**
- **Issue:** LeftmostLongest is non-overlapping. When two secrets overlap in the text (`AAAA..SHARED` + `SHARED..BBBB`), the tail of the second one survives, which breaks the must-have "overlapping secrets are both removed".
- **Fix:** `MatchKind::Standard` + `find_overlapping_iter`, with overlapping or adjacent ranges merged into one `[REDACTED]`.
- **Commit:** a0fd45f.

**2. [Rule 2 - Correctness] ProviderChain reports `Unavailable` instead of `None` when a provider was down**
- **Issue:** returning `None` would give a permanent `secret_not_found` during a transient Vault or K8s outage.
- **Fix:** the chain still continues past an `Unavailable` provider. If no provider has the secret, it returns `Unavailable`, which maps to `Retryable secret_not_found`. `Invalid` stops the chain (fail closed).
- **Commit:** a8750e1.

**3. [Rule 3 - Blocking] `SecretResolver::with_sealed_store`**
- **Issue:** `resolve_reserved` must read the sealed store directly, but the interface's `new(meta, chain)` has no store.
- **Fix:** added a builder. Without it, only `plaintext-v0` reserved records open; anything else fails closed with `secret_not_found`.

**4. Small additions to the interface**
- `SealingKeyRing::load_with`, an injectable env lookup, so lib tests never mutate the process env.
- `SecretValue::from_slice` / `into_box` / `from_box`, `GrantSpec::validate` / `allows_origin`, `ProviderChain::new` / `push` / `names`, and `SealedStore::new` / `ring`.
- Template names also refuse `.` segments.
- `resolve_operator` applies the 8-byte minimum.

### Not done here

- None. No Cargo, `lib.rs` or `providers/mod.rs` edits were needed: every dependency was already declared and anchored by 06-02.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | e7f062c | SecretValue, template parser/expander and grant model |
| 2 | a0fd45f | multi-encoding scrubber, AES-256-GCM sealed store and [secrets] config |
| 3 | a8750e1 | env/file/sealed providers, provider chain and SecretResolver |

## Self-Check: PASSED

- FOUND: every file listed in key-files.
- FOUND: commits e7f062c, a0fd45f and a8750e1 on main.
