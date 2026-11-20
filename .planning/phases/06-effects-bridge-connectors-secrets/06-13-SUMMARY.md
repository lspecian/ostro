---
phase: 06-effects-bridge-connectors-secrets
plan: 13
subsystem: effects-bridge
tags: [secrets, vault, vaultrs, kubernetes, kube, ttl-cache, zeroize, providers]
requires:
  - "06-02 contract: SecretProvider/ProviderError, cfg-gated providers/mod.rs, vaultrs/kube/k8s-openapi deps behind default features vault/k8s"
  - "06-05 secrets core: VaultConfig/K8sConfig typed sub-tables, ProviderChain, template::validate_name"
provides:
  - "ostro_bridge::secrets::providers::cache::TtlCache<K> (Zeroizing values, MAX_ENTRIES cap)"
  - "ostro_bridge::secrets::providers::vault::VaultProvider::from_config(&VaultConfig) (sync, lazy login) + from_config_with_sa_token (feature vault)"
  - "ostro_bridge::secrets::providers::k8s::K8sProvider::from_config(&K8sConfig) (async) (feature k8s)"
affects: [06-14 server wiring of the provider chain, 06-19 canary sweep]
tech-stack:
  added: []
  patterns:
    - "Remote providers cache only found values per full name; misses and errors always hit the backend"
    - "Errors carry operation + HTTP status only; kubeconfig/response decode errors are never echoed"
key-files:
  created:
    - crates/ostro-bridge/tests/providers_vault.rs
    - crates/ostro-bridge/tests/providers_k8s.rs
  modified:
    - crates/ostro-bridge/src/secrets/providers/cache.rs
    - crates/ostro-bridge/src/secrets/providers/vault.rs
    - crates/ostro-bridge/src/secrets/providers/k8s.rs
key-decisions:
  - "VaultProvider::from_config is synchronous and validates only; the first login / token-file read happens on the first get, so an unreachable Vault never blocks server start"
  - "Vault sessions re-login after 3/4 of the login lease; a 403 starts one new session (re-reading token_file / secret_id_file / SA JWT) and retries once, then Unavailable(\"permission denied\")"
  - "Vault 404 (incl. the deleted-version 404 with data but no errors) -> Ok(None); number/bool fields are returned as JSON text, object/array fields are Invalid"
  - "K8s names that cannot name a Secret key (no separator, bad DNS name/key) are Ok(None) so the chain continues; __ostro/ names are Invalid"
  - "K8s: after data[key] is taken, other data/stringData values and all annotations (last-applied-configuration copies the data) are zeroized"
  - "TtlCache uses std Instant with crate-private *_at methods for tests: tokio test-util (paused time) is not enabled for ostro-bridge and Cargo edits are 06-02-only"
metrics:
  duration: "~70 min"
  completed: 2026-11-20
---

# Phase 6 Plan 13: Vault + Kubernetes Secret Providers Summary

**This plan adds HashiCorp Vault KV v2 (vaultrs 0.8.0) and Kubernetes `Secret` (kube 5.1.0) providers. Both implement `SecretProvider` and slot into `ProviderChain`, and both share a TTL cache that zeroizes values on eviction. Vault supports token, AppRole and Kubernetes auth, with lazy sessions, re-login on lease expiry and one retry after a 403. Both are proven against axum mocks, and an optional real-Vault leg runs only when its environment variables are set.**

## Public API (for 06-14)

```rust
// secrets::providers::cache
pub const MAX_ENTRIES: usize = 4096;
pub struct TtlCache<K>;                         // Debug = ttl + entry count only
impl<K: Eq + Hash + Clone> TtlCache<K> {
    pub fn new(ttl: Duration) -> Self;          // ZERO disables caching
    pub fn ttl(&self) -> Duration;
    pub fn get(&self, key: &K) -> Option<SecretBox<[u8]>>;   // copy of a live entry; expired -> removed
    pub fn insert(&self, key: K, value: Zeroizing<Vec<u8>>); // sweeps expired; skipped at the cap
    pub fn remove(&self, key: &K); pub fn clear(&self); pub fn evict_expired(&self) -> usize;
    pub fn len(&self) -> usize; pub fn is_empty(&self) -> bool;
}

// secrets::providers::vault (feature "vault")
pub const SA_TOKEN_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
pub const DEFAULT_FIELD: &str = "value";
pub const APPROLE_MOUNT: &str = "approle"; pub const K8S_AUTH_MOUNT: &str = "kubernetes";
pub const REQUEST_TIMEOUT: Duration = 10 s;
impl VaultProvider {
    pub fn from_config(cfg: &VaultConfig) -> Result<Self, ProviderError>;   // sync; Invalid on bad config
    pub fn from_config_with_sa_token(cfg: &VaultConfig, jwt_file: impl Into<PathBuf>) -> Result<Self, ProviderError>;
    pub fn cache(&self) -> &TtlCache<String>;
}   // SecretProvider::name() == "vault"

// secrets::providers::k8s (feature "k8s")
impl K8sProvider {
    pub async fn from_config(cfg: &K8sConfig) -> Result<Self, ProviderError>; // Unavailable when no config is usable
    pub fn namespace(&self) -> &str;
    pub fn cache(&self) -> &TtlCache<String>;
}   // SecretProvider::name() == "k8s"
```

### Error mapping

| Situation | Vault | K8s |
|-----------|-------|-----|
| Secret or field/key missing; deleted KV version | `Ok(None)` | `Ok(None)` |
| Name not expressible in the backend | `Invalid` (bad path/field) | `Ok(None)` |
| `__ostro/` name | `Invalid` | `Invalid` |
| 403 | one new session + retry, then `Unavailable("permission denied")` | `Unavailable("permission denied")` |
| Other status / connection failure | `Unavailable("vault read: status N")` / `Unavailable(...)` | `Unavailable("k8s: status N")` / `Unavailable("k8s request failed: ...")` |
| Login failure | `Unavailable("vault approle login: status 400")` | n/a |
| Bad config | `Invalid("secrets.vault: ...")` from `from_config` | `Invalid` for a bad namespace; `Unavailable` for an unloadable kubeconfig or in-cluster config, without the file text |

### API names checked in the registry source

- **vaultrs 0.8.0:**
  - `client::{VaultClient, VaultClientSettingsBuilder}`, using `address`, `token`, `timeout` and `build`.
  - `kv2::read::<serde_json::Value>(client, mount, path)` returns the inner data map.
  - `auth::approle::login(client, mount, role_id, secret_id)` and `auth::kubernetes::login(client, mount, role, jwt)` both return `AuthInfo { client_token, lease_duration, .. }`.
  - `error::ClientError::{APIError { code, errors }, RestClientError { source }}`.
  - Responses must carry the full `EndpointResult` envelope (`request_id`, `lease_id`, `lease_duration`, `renewable`), so the mock returns it.
- **kube 5.1.0 / k8s-openapi 0.29:**
  - `config::{Kubeconfig::read_from, KubeConfigOptions}` and `Config::{from_custom_kubeconfig, infer}` with `.default_namespace`.
  - `Client::try_from(Config)` and `Api::<Secret>::namespaced(client, ns).get(name)`.
  - `Error::Api(Box<Status { code, .. }>)`.
  - `Secret.data: Option<BTreeMap<String, ByteString>>`.
  - kube's connector is `https_or_http`, so the plain-http mock kubeconfig works with the rustls feature. No self-signed certificate or new crate was needed.
- **Duplicates:** `cargo tree -d -p ostro-bridge` shows no reqwest, hyper or rustls duplicate. Cargo.lock is unchanged; every duplicate listed predates this plan.
- **k8s-openapi compile time:** no new crates were added. kube and k8s-openapi (`v1_33`) were already in the default build graph since 06-02, so this plan only adds ostro-bridge's own code.

## Verification

- `cargo test -p ostro-bridge --test providers_vault --locked`: 8 passed. The cases are token read, field selection, missing, deleted, custom mount, AppRole login then read, AppRole with a bad secret id, Kubernetes login plus re-login after the 1 s lease, cache hit then refetch, no caching of misses, 403 after one re-login, a rotated token file picked up on retry, connection refused, the chain, and Debug/no-token checks. `real_vault_dev_server` prints `SKIP ... (human_needed)`.
- `cargo test -p ostro-bridge --test providers_k8s --locked`: 5 passed. The cases are read (including binary bytes), `#` form, missing key, missing secret, namespace from the context and the override, 403, 401 without echoing the token, cache hit then refetch, unreachable server, broken kubeconfig without echoing the token, bad namespace, chain, and Debug.
- `cargo test -p ostro-bridge --lib --locked`: 161 passed, including the cache (4), vault (5) and k8s (3) unit tests.
- `cargo test -p ostro-bridge --locked`: every test binary is green: lib 161, driver 14, egress_client 5, http_connector 24, object_local 8, object_s3 3, providers_k8s 5, providers_vault 8, ratelimit 4, secrets 10, sql_mysql 8, sql_pg 10, ssrf_table 5. The DB and S3 legs SKIP internally as before.
- `cargo build -p ostro-bridge --no-default-features --locked` and the default build are both warning-free.
- `cargo clippy -p ostro-bridge --all-targets --all-features -- -D warnings` and the same with `--no-default-features` are both clean.
- `cargo fmt --check` is clean.
- `bash scripts/ci-local.sh` was **not** run. It takes about 40 minutes and 4 other executors were building in parallel. Crate-level tests and clippy were run instead.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] TtlCache tests use an injected `Instant` instead of tokio paused time**
- **Issue:** `tokio::time::pause` needs tokio's `test-util` feature, which ostro-bridge does not enable, and Cargo edits belong to 06-02 only.
- **Fix:** the cache runs on `std::time::Instant`. The unit tests drive the crate-private `get_at`/`insert_at`/`evict_expired_at` methods, and the integration tests use short real TTLs (300 ms) and sleeps.
- **Commit:** e243533.

**2. [Rule 1 - Bug] The textual P-5 guard in `tests/egress_client.rs` flagged `VaultClient::new`**
- **Issue:** the guard checks that no source file outside `egress/client.rs` contains the text `Client::new`, and `VaultClient::new` matched it. Commit e243533 therefore left `egress_client` red until the next commit.
- **Fix:** the constructor is now called as `<VaultClient>::new(settings)`, with a doc comment. Vault is an operator-configured backend from `[secrets.vault]`, not guest egress, so the SSRF-guarded client does not apply. The test file is outside this plan's files, so it was not changed.
- **Commit:** 723b3c1.

**3. [Rule 2 - Correctness] Mock-driven protocol details**
- vaultrs sends login bodies without `Content-Type: application/json`. Real Vault accepts this, so the mock parses raw bytes.
- The KV v2 deleted-version 404 has `data` but no `errors`, so vaultrs reports it as `RestClientError`. `status_of` reads the code from rustify's `ServerResponseError { code, .. }` Debug prefix, because rustify is not a direct dependency. The text stays local and is zeroized.

### Gaps recorded (not fixed here)

- **The `#field` / `#key` name forms are unreachable from templates.** The 06-05 `{{secret:NAME}}` grammar has no `#` and no upper case, and `resolve_operator` validates against the same grammar. Through the resolver, Vault reads only the default `value` field and K8s reads only lower-case keys in the `<secret>/<key>` form. Direct callers of the providers get the full grammar. Widening the grammar would be a 06-05 template change.
- **vaultrs keeps the session token in a plain `String`** inside `VaultClientSettings` and its middleware, because its API takes `&str`. Ostro's own copies are `SecretString`, and the vaultrs settings are never formatted.
- **`Config::infer()` tries kubeconfig first, then in-cluster.** The 06-05 config comment says the opposite order. The behaviour is kube's; set `kubeconfig` explicitly to pin it.
- **No Vault token renewal call (`renew_self`).** Leases are refreshed by a fresh login at 3/4 of their duration. A static token (auth `token`) is re-read only after a 403.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | e243533 | TtlCache and the Vault KV v2 provider, with mock Vault tests |
| 2 | 723b3c1 | Kubernetes Secret provider with mock API server tests, plus the `<VaultClient>::new` P-5 guard fix |

## Self-Check: PASSED

- FOUND: crates/ostro-bridge/src/secrets/providers/{cache,vault,k8s}.rs and tests/providers_{vault,k8s}.rs on main.
- FOUND: commits e243533 and 723b3c1 on main.
