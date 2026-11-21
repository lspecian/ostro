---
phase: 07-workers-placement-flow-control
plan: 06
subsystem: tls (cluster CA, client-listener TLS)
tags: [tls, rcgen, rustls, axum-server, ca, mtls, sec-01, work-01]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-02 p7 seam, tls/ stubs, [api.tls]/[cluster] config, rcgen/x509-parser/axum-server/arc-swap deps"
provides:
  - "ClusterCa (load_or_create, import_pem, issue_leaf, issue_node_cert, load_or_issue_node_cert, sign_worker_csr, ca_hash, root_store)"
  - "NodeIdentity / WorkerIdentity parsers (disjoint ostro:// schemes)"
  - "CA-issued dev cert (self-signed mode, chain [leaf, CA])"
  - "ReloadableResolver (ArcSwapOption<CertifiedKey>, ACME TLS-ALPN-01 challenge hook)"
  - "client listener modes off / auto / self-signed / files (hot reload) on axum-server, ALPN h2 + http/1.1 (+ acme-tls/1)"
  - "acme::start contract (stub until 07-10)"
affects: [07-07, 07-09, 07-10, 07-14, 07-15]
tech-stack:
  added: []
  patterns: ["one ResolvesServerCert for every TLS mode", "key files 0600 via create-new tmp + fsync + rename + dir fsync", "CA bound to cluster_id by CN"]
key-files:
  created:
    - crates/ostro-server/tests/tls_ca.rs
    - crates/ostro-server/tests/tls_listener.rs
  modified:
    - crates/ostro-server/src/tls/mod.rs
    - crates/ostro-server/src/tls/ca.rs
    - crates/ostro-server/src/tls/dev_cert.rs
    - crates/ostro-server/src/tls/resolver.rs
    - crates/ostro-server/src/tls/listener.rs
    - crates/ostro-server/src/tls/acme.rs
decisions:
  - "axum-server 0.9.2 path used (from_tcp_rustls on the pre-bound listener + RustlsConfig::from_config); the D-W7 fallback was not needed"
  - "auto rule profile: ServerConfig has no profile field, so TlsProfile::of(cfg) treats secrets.auto_sealing_key (set only by dev_defaults) as the dev marker; init_with_profile takes the profile explicitly"
  - "acme::start(state: Arc<TlsState>, shutdown: CancellationToken) -> Result<JoinHandle<()>, TlsError> is the 07-10 contract; listener::serve calls it before serving TLS"
  - "The CA and node cert are created on every start, in every mode (the cluster listener always needs them)"
metrics:
  duration: ~55 min
  completed: 2026-11-21
---

# Phase 7 Plan 06: TLS foundation Summary

The cluster CA (ECDSA P-256 via rcgen on aws-lc-rs, bound to cluster_id, importable) issues node, worker and dev certificates. One hot-swappable `ResolvesServerCert` serves the client listener in `self-signed`/`files` modes over axum-server, with REST and gRPC on the same port. `auto` keeps loopback plaintext.

## Commits

| Task | Commit | Message |
|---|---|---|
| 1 | be81151 | feat(07-06): cluster CA, node/worker certificate issuance |
| 2 | cc12cfd | feat(07-06): reloadable TLS resolver and client listener modes |

## rcgen 0.14 API actually used

- `KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)`, `KeyPair::from_pem`, `serialize_pem`/`serialize_der`, `PublicKeyData::subject_public_key_info` (cert/key match check).
- `CertificateParams::default()` with the fields `distinguished_name` (`DistinguishedName::new` + `push(DnType::CommonName, ..)`), `subject_alt_names` (`SanType::{DnsName, IpAddress, URI}` from `Ia5String::try_from`), `is_ca` (`IsCa::Ca(BasicConstraints::Constrained(0))` / `IsCa::ExplicitNoCa`), `key_usages`, `extended_key_usages` (`ExtendedKeyUsagePurpose::{ServerAuth, ClientAuth}`), `use_authority_key_identifier_extension`, `serial_number` (`SerialNumber::from_slice`, 16 random bytes), `not_before`/`not_after`.
- `CertificateParams::self_signed(&key)`, `CertificateParams::signed_by(&public_key, &issuer)`.
- `Issuer::new(params, key)` on creation and `Issuer::from_ca_cert_der(&der, key)` on load. `Issuer::key()` gives back the key for `key_pem()`.
- `CertificateSigningRequestParams::from_der(&CertificateSigningRequestDer)` verifies the CSR signature (`Error::InvalidCertificationRequestSignature`). The key type is checked separately with x509-parser: `id-ecPublicKey` + `prime256v1` or `Ed25519`. rcgen's algorithm mapping cannot tell a P-384 key signed with SHA-256 apart from a P-256 key.
- Validity times are built as `x509_parser::time::ASN1Time::from_timestamp(secs).to_datetime()`, because the `time` crate is not a direct dependency. The time source is `SystemTime::now()`, marked `// clock-exempt: X.509 validity timestamps checked by peers`.
- `CustomExtension::new_acme_identifier` exists in 0.14.10 (for 07-10).

## File layout under `<data_dir>/tls/`

| File | Mode | Content |
|---|---|---|
| `ca.crt` | 0644 | CA cert (CN `ostro cluster <cluster_id>`, CA:true pathlen 0, keyCertSign + cRLSign + digitalSignature, 10 y) |
| `ca.key` | 0600 | CA key, PKCS#8 PEM |
| `node.crt` | 0644 | chain [node leaf, CA]. SANs: exactly one `ostro://<cluster_id>/node/<node_id>`, the `[cluster] advertise` hosts/IPs (ports stripped), `localhost`, `127.0.0.1`, `::1`. EKU serverAuth + clientAuth, 1 y |
| `node.key` | 0600 | node key |
| `dev.crt` / `dev.key` | 0644 / 0600 | `self-signed` mode only: chain [dev leaf, CA]. SANs: hostname, `localhost`, `127.0.0.1`, `::1`, the listen IP. EKU serverAuth, 1 y, no `ostro://` SAN |

- The directory is 0700. Writes go through create-new `.<name>.tmp-<pid>`, then fsync, rename and dir fsync.
- A half-present CA pair, a corrupt file, a cert/key mismatch or a CA of another cluster is an error naming the file. Nothing is ever regenerated silently.
- `node.*` and `dev.*` are reused when the leaf verifies against the current CA, is valid for more than 30 days and has exactly the wanted SAN set. Otherwise they are re-issued, which also covers a CA that `import_pem` replaced.

## Startup cost (D-W16 / CLI-01)

Debug build on WSL2, under load:
- First start with an empty dir: CA + node cert in about 100–190 ms. Of that, about 22 ms is aws-lc-rs first-use init, about 37 ms goes to fsync per persisted pair (key + cert + 2 dir fsyncs), and key generation + signing take under 1 ms.
- Restart (load + reuse): under 1 ms each.
- `dev_startup` (spawned `ostro dev`) is still green.
- The test asserts less than 500 ms.

## Resolver ACME hook (for 07-10)

```rust
pub const ACME_TLS_ALPN: &[u8] = b"acme-tls/1";
impl ReloadableResolver {
    pub fn new(initial: Option<Arc<CertifiedKey>>) -> Self;
    pub fn swap(&self, new: impl Into<Arc<CertifiedKey>>);       // next handshake on
    pub fn current(&self) -> Option<Arc<CertifiedKey>>;
    pub fn fingerprint(&self) -> Option<String>;                  // sha256:<hex> of the leaf
    pub fn set_challenge(&self, domain: &str, cert: Option<CertifiedKey>); // None clears
}
// tls/acme.rs (07-10 replaces the body, keeps the signature):
pub fn start(state: Arc<TlsState>, shutdown: CancellationToken) -> Result<JoinHandle<()>, TlsError>;
```

- A ClientHello whose ALPN offers only `acme-tls/1` gets the challenge cert registered for its SNI (case- and trailing-dot-insensitive). It never gets the real cert, and it gets nothing (handshake fails) when no challenge cert is registered.
- In mode acme with challenge tls-alpn-01, `TlsState::alpn()` is `["h2","http/1.1","acme-tls/1"]`.
- `listener::serve` calls `acme::start` inside the runtime before it serves, then serves TLS while the returned task runs, and aborts the task when serving ends.
- `TlsState` exposes `resolver()`, `acme_config()`, `data_dir()` and `dir()` for 07-10.
- Mode acme starts with no certificate, so `fingerprint()` is None until `swap`.

## TlsState accessors (for 07-09 / 07-14 / 07-15)

- `ca() -> Option<&Arc<ClusterCa>>`, `ca_der() -> Option<Vec<u8>>`, `ca_pem()`, `ca_hash() -> Option<String>` (`sha256:<hex of CA DER>`). These are `None` only for `TlsState::off()`; `tls::init` always creates the CA.
- `node_cert() -> Option<&CertifiedKeyPair>` (`chain()`, `chain_pem()`, `key_pem()` for tonic `Identity::from_pem`, `leaf_der()`, `fingerprint()`, `to_rustls()`).
- `node_server_config() -> Result<Arc<rustls::ServerConfig>>`: node cert, `WebPkiClientVerifier` over the cluster CA (a client cert is always required), ALPN h2.
- `sign_worker_csr(csr_der, token_id, worker_id, ttl) -> Result<Vec<u8>>` (cert DER). Ids are `[A-Za-z0-9._-]{1,64}`. Only the SAN URI `ostro://<cluster>/worker/<token>/<worker>` is set, with EKU clientAuth, not a CA, `not_before = now-60s`.
- `WorkerIdentity::from_der` / `NodeIdentity::from_der` require exactly one `ostro://` URI SAN, and each rejects the other's identity. D-W25's `worker/ext/<id>` parses as token_id `ext`.
- `endpoint(addr)`: `https` + `Some(sha256:<hex>)` for TLS modes (the live resolver leaf, so it follows `files` reloads), `http` + `None` for plaintext. Also `mode()`, `fingerprint()`, `alpn()`, `client_server_config()`, `reload_files()`, `poll_files()`, `dev_cert(listen)`.
- `ClusterCa::import_pem(dir, cluster_id, crt_pem, key_pem)` and `ClusterCa::key_pem()` are what Phase 12 needs to distribute the CA.

## Listener modes

| `[api.tls] mode` | Result |
|---|---|
| `off` | plaintext `axum::serve` (as before) |
| `auto`, loopback listen | plaintext, so Phase 4/5/6/8 tests and the `http://` ready line are unchanged |
| `auto`, non-loopback, dev | self-signed (CA-issued dev cert) |
| `auto`, non-loopback, server | startup error `[api.tls] mode=auto on a non-loopback listener requires files or acme (or allow_self_signed = true)`; with `allow_self_signed` it serves self-signed and logs a warning |
| `self-signed` | dev cert |
| `files` | PEM chain + key (any of PKCS#8 / SEC1 / PKCS#1). Reloaded on SIGHUP and on an mtime poll every `reload_poll` (0 disables the poll). A failed reload, including a key that does not match, keeps the served cert and logs an error |
| `acme` | `acme::start` (stub error "ACME lands in 07-10") |

- TLS is served by axum-server 0.9.2: `from_tcp_rustls(listener.into_std(), RustlsConfig::from_config(cfg))`, with `Handle::graceful_shutdown(None)` driven by the `CancellationToken`.
- rustls `ServerConfig::builder_with_provider(ostro_api::tls_provider())` + `with_cert_resolver`. **The D-W7 fallback was not needed.**

## Verification

- `cargo test -p ostro-server --test tls_ca --locked`: 8 passed. `--test tls_listener`: 6 passed (REST over HTTPS + tonic gRPC health over TLS on one port, ALPN h2/http1.1 negotiation, foreign CA refused, files reload A→B, corrupt and mismatched reloads keep B, mtime poll, auto/off per profile, ACME stub error, resolver challenge hook).
- `cargo test -p ostro-server --locked`: every suite green (exit 0).
- `cargo test -p ostro --test server_config --test dev_startup --test cli --test data_dir_lock --test e2e_dev`: green. Spawned `ostro dev` still prints `http://`.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. Build warning-free. `cargo fmt --check` clean.
- `cargo tree -p ostro-server -e normal --target all` has no `ring`.
- Not run: the full `bash scripts/ci-local.sh` gate (load average about 27 with parallel executors + the Phase 8 campaign). The crates I touched are covered above.

## Deviations from Plan

1. **[Rule 3 - blocking] `tls/acme.rs` (owned by 07-10) gained the `acme::start` stub.** The plan's key link requires `listener.rs` to call `acme::start(..)`, but the 07-02 stub file had no such function. I appended only the function and its contract doc. 07-10 replaces the body and must keep the signature.
2. **[Rule 3] Profile detection.** `ServerConfig` has no profile field, and config.rs is not this plan's file. `TlsProfile::of(cfg)` uses `secrets.auto_sealing_key` (true only in `dev_defaults`). `init_with_profile` takes an explicit profile. **Follow-up for 07-15 / owner of config.rs:** if a user sets `auto_sealing_key = true` under `ostro server`, the server profile is treated as dev for the `auto` rule. 07-15 can call `init_with_profile`, or a future config field can replace the heuristic.
3. **`TlsError::Cert(String)` variant added** (tls/mod.rs, this plan's file). It is used for unusable certificates, keys and CSRs and never carries key material.
4. **`#[allow(clippy::manual_async_fn)]` on `listener::serve`.** The 07-02 signature is fixed (an explicit `Send + 'static` future), and clippy flags the body once it is a plain `async move`.
5. **Resolver uses `ArcSwapOption<CertifiedKey>`** instead of `ArcSwap`, because mode acme starts with no certificate.
6. **Behaviour change (intended by D-W8, USER REVIEW):** `ostro server` with the default `0.0.0.0:7700` listener and `[api.tls] mode = auto` now refuses to start unless `files`/`acme`/`allow_self_signed`/`off` is configured. No existing test starts the server profile on a non-loopback listener.

## Self-Check: PASSED

- All files listed under key-files exist at cc12cfd.
- be81151 and cc12cfd are on main.
