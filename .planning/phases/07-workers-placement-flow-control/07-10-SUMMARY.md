---
phase: 07-workers-placement-flow-control
plan: 10
subsystem: tls (ACME for the client listener)
tags: [tls, acme, instant-acme, rcgen, tls-alpn-01, http-01, pebble, sec-01]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-06 ReloadableResolver (swap, set_challenge), TlsState accessors, acme::start contract; 07-02 [api.tls.acme] config + instant-acme dependency"
provides:
  - "acme::start (07-06 signature kept) + start_with_clock / set_clock (injected clock)"
  - "AcmeIssuer: instant-acme account / order / TLS-ALPN-01 + HTTP-01 / finalize_csr / install / cache"
  - "challenge_cert (critical acmeIdentifier), Http01Responder, Validity, RenewalPolicy, run_renewal, Obtain"
  - "cache layout <cache_dir>/{account-<hash>.json, cert.pem, key.pem}, all 0600"
affects: [07-14, 07-15, 07-22]
tech-stack:
  added: []
  patterns: ["renewal scheduled from the injected clock, tokio sleeps capped at 1 h then the clock is re-read", "challenge material always cleared after an issuance attempt"]
key-files:
  created:
    - crates/ostro-server/tests/acme.rs
  modified:
    - crates/ostro-server/src/tls/acme.rs
    - crates/ostro-server/tests/tls_listener.rs
decisions:
  - "instant-acme is built without its `rcgen` feature, so Order::finalize() is unavailable: the CSR is built with rcgen 0.14 and sent through finalize_csr"
  - "The challenge cert is turned into a rustls CertifiedKey with CertifiedKey::new + key_provider.load_private_key; CertifiedKey::from_der fails because its key/leaf check parses the leaf with webpki, which rejects the critical acmeIdentifier"
  - "acme::start reads the clock through a process-wide OnceLock (acme::set_clock); unset, it falls back to ostro_partition::SystemClock (follow-up below)"
  - "Pebble v2.10.3 builds with GOTOOLCHAIN auto-switch to go1.26.11; both Pebble legs ran green locally (no human_needed for Pebble)"
metrics:
  duration: ~45 min
  completed: 2026-11-21
---

# Phase 7 Plan 10: ACME Summary

`[api.tls] mode = acme` now obtains certificates with instant-acme 0.8.5. TLS-ALPN-01 is answered by the client listener's resolver, and HTTP-01 by a small axum responder. The issued chain is swapped into the reloadable resolver with no restart, cached under `cache_dir` and renewed at 2/3 of its lifetime on the injected clock. Fake-ACME tests always run. The Pebble legs ran locally against Pebble v2.10.3, including a real-validation run for both challenge types.

## Commits

| Task | Commit | Message |
|---|---|---|
| 1 | 3d8fbde | feat(07-10): ACME (instant-acme) with TLS-ALPN-01, HTTP-01, renewal and cache |
| 2 | 5cae838 | test(07-10): env-gated Pebble ACME tests |

## instant-acme 0.8.5 API actually used

Features as declared by 07-02: `aws-lc-rs` and `hyper-rustls`. There is no `rcgen` feature, so `Order::finalize()` does not exist.

- `Account::builder() -> Result<AccountBuilder>` uses the platform verifier. `Account::builder_with_root(pem_path)` is used when `directory_ca_file` is set; it trusts only the first certificate of that PEM.
- `AccountBuilder::create(&NewAccount { contact: &["mailto:<email>"], terms_of_service_agreed: true, only_return_existing: false }, directory_url, None) -> (Account, AccountCredentials)`.
- `AccountBuilder::from_credentials(AccountCredentials)`. The credentials are serde JSON (`id`, PKCS#8 key, directory). `Account::id()` is logged, never the key.
- `Account::new_order(&NewOrder::new(&[Identifier::Dns(..)])) -> Order`.
- `Order::authorizations()` returns a stream-like `Authorizations` with `next().await -> Option<Result<AuthorizationHandle>>`. `AuthorizationHandle` derefs to `AuthorizationState` (`status: AuthorizationStatus::{Pending, Valid, ..}`).
- `AuthorizationHandle::challenge(ChallengeType::{TlsAlpn01, Http01}) -> Option<ChallengeHandle>`. `ChallengeHandle` derefs to `Challenge` (`token`) and has `identifier().identifier: &Identifier`, `key_authorization() -> KeyAuthorization` (`as_str()` for HTTP-01, `digest()` = SHA-256 for TLS-ALPN-01) and `set_ready().await`.
- `Order::poll_ready(&RetryPolicy) -> OrderStatus`. Then `Order::finalize_csr(&csr_der)` and `Order::poll_certificate(&RetryPolicy) -> String`, the PEM chain. `Order::state().error` is read for the failure detail.
- `RetryPolicy::new().initial_delay(500 ms).backoff(1.5).timeout(120 s)`. This policy honours `Retry-After`. It uses `std::time::Instant` internally, inside the library and only for poll timeouts.
- instant-acme's hyper-rustls client calls `rustls::ClientConfig::builder()`. To make that resolve to the node provider, `ensure_provider()` runs `CryptoProvider::install_default(ostro_api::tls_provider())` once, ignoring the error if a provider is already installed. There is no `ring` in the tree.

## acmeIdentifier construction

- rcgen 0.14.10 has `CustomExtension::new_acme_identifier(sha_digest)`: OID `1.3.6.1.5.5.7.1.31` (`id-pe-acmeIdentifier`), `critical: true`, content = DER OCTET STRING of the 32-byte digest. The `from_oid_content` fallback was not needed.
- `challenge_cert(domain, key_auth.digest())`: a fresh ECDSA P-256 key, self-signed, empty DN, single SAN `domain`.
- The test `challenge_cert_carries_critical_acme_identifier` checks: the extension is present and critical, its value is `04 20 || sha256(key authorization)`, the SANs are exactly `[domain]`, and a digest that is not 32 bytes is an error.
- The resolver serves the challenge cert only to ClientHellos whose ALPN offers only `acme-tls/1` (07-06). The listener advertises `acme-tls/1` in tls-alpn-01 mode (W11), and this is asserted end to end in `listener_serves_cached_cert_and_tls_alpn_challenge`.

## Cache layout

`cache_dir` defaults to `<data_dir>/tls/acme`. The directory is mode 0700.

| File | Mode | Content |
|---|---|---|
| `account-<first 16 hex of sha256(directory URL)>.json` | 0600 | instant-acme `AccountCredentials` (one account per directory) |
| `cert.pem` | 0600 | issued chain, leaf first |
| `key.pem` | 0600 | certificate key, PKCS#8 PEM (new P-256 key per certificate) |

- Writes go through `ca::write_atomic`: create-new tmp, fsync, rename, dir fsync.
- At start, the cached pair is used only if the key matches the leaf and the leaf's DNS SANs cover every configured domain. A corrupt or foreign pair is logged and a new certificate is ordered.

## Renewal policy

- `Validity::renew_at_ms = not_after - lifetime/3`, which is 2/3 of the lifetime.
- At `start`, a cached cert that is still valid on the injected clock is installed immediately, even inside its last third, so it keeps serving. If it is more than 1/3 from expiry, the directory is not contacted at all.
- `run_renewal(policy, clock, current, shutdown, issuer, jitter)` waits until `clock.now_ms() >= renew_at`. Each tokio sleep is capped at `max_sleep` (1 h), after which the clock is re-read, so the deadline always comes from the clock. When renewal is due it calls `obtain`.
- On failure: `RenewalPolicy::backoff_delay(failures, jitter) = clamp(60 s * 2^(failures-1) * [0.75, 1.25), 1 min, 1 h)`, with jitter from `getrandom`. The old certificate stays served. The failure is logged at warn with `retry_in_s` and `next_attempt_ms`. Success is logged at info.
- There is no metric, because no TLS metric prefix is reserved for Phase 7.

## Pebble outcome

- Build: `GOBIN=<target>/pebble/bin go install github.com/letsencrypt/pebble/v2/cmd/pebble@v2.10.3`, the latest tag on 2026-11-21. The local Go is 1.23.10. Pebble v2.10.3 requires go >= 1.25.0, so Go's toolchain auto-switch downloaded and used **go1.26.11** (`GOTOOLCHAIN` default `auto`). No system Go configuration was changed. **Built: yes.**
- Config: written to `target/wt-07-10/pebble/pebble-config.json`. Listens on loopback only: `listenAddress 127.0.0.1:34000`, management `127.0.0.1:35000`, `httpPort 35002`, `tlsPort 35001`. The HTTPS cert/key are `test/certs/localhost/{cert,key}.pem` from the Pebble module, the directory CA is `test/certs/pebble.minica.pem`, `retryAfter` is 1 s, ecdsa, 90-day profile. Started with `-strict`, `PEBBLE_VA_NOSLEEP=1` and `PEBBLE_WFE_NONCEREJECT=0`.
- Run 1, `PEBBLE_VA_ALWAYS_VALID=1`: `OSTRO_TEST_ACME_DIRECTORY=https://127.0.0.1:34000/dir OSTRO_TEST_ACME_CA=<pebble.minica.pem> cargo test -p ostro-server --test acme --locked -- pebble_order_tls_alpn` gave `1 passed`. Pebble logged the order, finalize, `Issued certificate serial 0e0160ea04519e89` and the cert download.
- Run 2, real validation (`PEBBLE_VA_ALWAYS_VALID` unset), with `OSTRO_TEST_ACME_HTTP_PORT=35002 OSTRO_TEST_ACME_TLS_PORT=35001`, running `-- pebble --test-threads=1`: `2 passed` in 4.45 s. Pebble logged `Attempting to validate w/ HTTP: http://localhost:35002/.well-known/acme-challenge/<token>` and authz VALID for HTTP-01. For TLS-ALPN-01 it ran a real validation against the client listener on :35001, then authz VALID.
- Each Pebble test checks:
  - the listener in acme mode installs the Pebble cert;
  - a TLS client sees it (fingerprint equals the resolver's, SAN `localhost`, issuer `Pebble Intermediate CA`);
  - the account, cert and key files are 0600;
  - no challenge material is left;
  - a second `acme::start` on the same data dir installs the cached cert at once.
- Without the env vars, both tests print `SKIP ... (human_needed: run Pebble, see 07-10-SUMMARY.md)` and pass.
- To repeat the run:
  ```bash
  GOBIN=$PWD/target/pebble/bin go install github.com/letsencrypt/pebble/v2/cmd/pebble@v2.10.3
  P=$(go env GOMODCACHE)/github.com/letsencrypt/pebble/v2@v2.10.3
  # config as above (listen 127.0.0.1:34000, httpPort 35002, tlsPort 35001, cert/key from $P/test/certs/localhost)
  PEBBLE_VA_NOSLEEP=1 PEBBLE_WFE_NONCEREJECT=0 target/pebble/bin/pebble -config target/pebble/pebble-config.json -strict &
  OSTRO_TEST_ACME_DIRECTORY=https://127.0.0.1:34000/dir OSTRO_TEST_ACME_CA=$P/test/certs/pebble.minica.pem \
  OSTRO_TEST_ACME_HTTP_PORT=35002 OSTRO_TEST_ACME_TLS_PORT=35001 \
    cargo test -p ostro-server --test acme --locked -- pebble --test-threads=1
  ```
- **Remaining human_needed:** real Let's Encrypt staging on a publicly reachable host. No test contacts a public directory, and `pebble_env` asserts that the directory is not `letsencrypt.org`.

## Verification

- `cargo test -p ostro-server --test acme --locked`: 10 passed. The 8 fake-ACME tests ran and both Pebble tests SKIPped. `--test tls_listener`: 6 passed.
- `cargo test -p ostro-server --test acme --locked -- pebble`: `test result: ok. 2 passed`. The SKIP path without env vars; the green runs with env vars are listed above.
- `cargo test -p ostro-server --locked --no-fail-fast`: exit 0, 43 test binaries, 333 tests passed. The first plain run stopped on a load flake in Phase 8's `guest_logs_cap_ring_endpoint_and_metrics` (`dropped_run_cap` 9000 vs 10000 at load average 85). That suite was green when re-run alone (4/4) and in the `--no-fail-fast` run. It is not related to this plan.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. The build is warning-free and `cargo fmt --check` is clean.
- Clock rule: `acme.rs` has no `SystemTime::now`, `Instant::now` or `tokio::time::Instant::now`. Renewal deadlines come from `Clock::now_ms`.

## Deviations from Plan

1. **[Rule 3 - blocking] `tests/tls_listener.rs` edited (owned by 07-06).** Its `acme_mode_delegates_to_acme_start` test configured `domains = ["ostro.example"]` with the default directory (Let's Encrypt production) and expected the stub error. With the real `acme::start`, that test would contact Let's Encrypt production. It now leaves `domains` empty and asserts the config rejection (`domains`). The diff is 8 lines.
2. **[Rule 3] Renewal loop test uses real tokio time, not `start_paused`.** ostro-server's dev-dependencies do not enable tokio `test-util`, and Cargo edits are 07-02 only. I added `RenewalPolicy` (backoff bounds plus the clock re-read cap; the production default is 1 min / 1 h / 1 h). The test drives the loop with a `ManualClock` and a millisecond policy. The schedule and backoff math is tested as pure functions.
3. **Clock seam follow-up (07-14/07-15 or the p7.rs owner).** `acme::start` has the fixed signature `(state, shutdown)` and `TlsState` carries no clock. The clock therefore comes from `acme::set_clock(Arc<dyn Clock>)`, a first-call-wins OnceLock; tests use `start_with_clock`. Until someone calls `tls::acme::set_clock(ctx.clock.clone())` from the seam (p7.rs / server.rs, not this plan's files), production falls back to `ostro_partition::SystemClock`, the existing partition wall clock. 07-22's clock grep should check that this call exists.
4. **Challenge `CertifiedKey` built without the consistency check** (decision above). The fake-ACME test client accepts the challenge cert's handshake signature without webpki, for the same reason. Pebble's real TLS-ALPN-01 validation proves the served challenge cert is correct.

## Self-Check: PASSED

- `crates/ostro-server/src/tls/acme.rs` and `crates/ostro-server/tests/acme.rs` exist at 5cae838.
- 3d8fbde and 5cae838 are on main.
