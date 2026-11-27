---
phase: 14-multi-tenancy-security-1-0-release
plan: 31
subsystem: ostro-kafka transport (connections, framing, ApiVersions, TLS, SASL)
tags: [kafka, trig-04, transport, tls, sasl, scram, pure-rust, fake-broker, tdd]
requires:
  - phase: 14-01
    provides: "ostro-kafka crate, contract types (ClientConfig/TlsConfig/SaslConfig/KafkaError/Client), every dependency (kafka-protocol client + dev broker, tokio-rustls/rustls/aws-lc-rs, rustls-native-certs, base64, zeroize, dev rcgen)"
  - phase: 14-01b
    provides: "scripts/fetch-tools.sh (Kafka 4.1.2 + Temurin 21 JRE), scripts/test-services.sh (PLAINTEXT, SASL_PLAINTEXT PLAIN/SCRAM-256/512, SSL listeners)"
provides:
  - "crates/ostro-kafka/src/conn.rs: Conn (one task per connection, pipelined frames by correlation id), Broker (reconnecting, jittered backoff), Backoff, ConnectOptions, tls_client_config, WireCheck (bounds walker), MAX_FRAME"
  - "crates/ostro-kafka/src/versions.rs: client ranges (API_VERSIONS..SASL_AUTHENTICATE consts, CLIENT_RANGES), negotiate(), BrokerVersions + table(), ApiRequest trait (impls: ApiVersions, SaslHandshake, SaslAuthenticate, Metadata)"
  - "crates/ostro-kafka/src/sasl.rs: PLAIN + SCRAM-SHA-256/512 (ScramClient), authenticate() used by Conn::open"
  - "Client::connect (real), Client::{api_versions, request, bootstrap, connect_options} (additive)"
  - "tests/support/mod.rs fake broker; tests/transport.rs (18); tests/broker_auth.rs (10, Kafka 4.1)"
affects: [14-34, 14-32, 14-27, 14-26]
tech-stack:
  added: []
  patterns:
    - "own ApiRequest trait (kafka-protocol's Request trait exists only with client+broker features): range + response type + check_response walker per API"
    - "every response is walked by WireCheck before kafka-protocol decodes it: kafka-protocol does Vec::with_capacity(wire count), so a garbage count would otherwise allocate unboundedly"
    - "per-request response cap checked on the 8-byte frame prefix (size + correlation id) before any frame allocation"
key-files:
  created:
    - crates/ostro-kafka/README.md
    - crates/ostro-kafka/tests/support/mod.rs
    - crates/ostro-kafka/tests/transport.rs
    - crates/ostro-kafka/tests/broker_auth.rs
  modified:
    - crates/ostro-kafka/src/{lib,conn,versions,sasl}.rs
    - crates/ostro-kafka/tests/skeleton.rs (deviation: asserted the 14-01 stub)
key-decisions:
  - "Wrong credentials fail with the contract's KafkaError::Sasl (14-01 has no Auth variant; never rename)"
  - "SCRAM crypto (HMAC, digest, PBKDF2, nonce RNG) is all aws-lc-rs; the hmac/sha2 crates stay anchored in lib.rs for 14-34 or removal"
  - "Unsupported versions fail lazily per API (a broker without OffsetCommit still serves Metadata/Fetch); a missing API is reported as broker v-1..=v-1"
  - "A request timeout closes the connection; the next request reconnects. Failed connects are spaced by Backoff ([d/2, d], d = 100 ms doubling to 10 s); the first attempt never waits"
  - "Client::connect tries each bootstrap once and prefers a definitive error (TLS/SASL/protocol/unsupported_version) over Io/Timeout"
  - "SCRAM accepts 4096..=1,000,000 iterations; broker error messages are scrubbed of the password before being surfaced"
requirements-completed: []
duration: ~2h
completed: 2026-11-27
---

# Phase 14 Plan 31: Kafka transport (framing, ApiVersions, TLS, SASL) Summary

**Pure-Rust Kafka transport for the 1.0 topic-consumer trigger: i32-framed pipelined connections owned by one task each, ApiVersions v3 negotiation with the KIP-511 v0 fallback and per-API `unsupported_version`, tokio-rustls/aws-lc-rs TLS (native roots + `ca_file`, SNI override), SASL PLAIN and SCRAM-SHA-256/512 with server-signature verification, bounded frames and a bounds-checked pre-decode walker so malformed input is an error and never a panic or an unbounded allocation; proven against an in-process fake broker and the Apache Kafka 4.1.2 PLAINTEXT, SSL and SASL_PLAINTEXT listeners.**

Requirement ids are not ticked here (14-27's, D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| 0e5a17a | 1 | feat: conn/versions/sasl, Client::connect, fake broker, transport suite, README, skeleton test update |
| b3e7208 | 2 | test: broker_auth (Kafka 4.1 TLS + SASL) |
| 8e5a23c | - | refactor: `Conn::version` calls `versions::negotiate` directly (same result; makes the key link explicit); lib/transport/broker_auth rerun green locally |

## API version table as landed

All ranges fit inside kafka-protocol 0.18 (nothing had to be tightened). Negotiated against
Apache Kafka 4.1.2 (local run and the slot-1 cluster run agree, from `plaintext_connects_and_negotiates`):

| API | Key | Client | Kafka 4.1.2 | Used |
|---|---|---|---|---|
| ApiVersions | 18 | v0–3 | v0–4 | v3 |
| Metadata | 3 | v1–12 | v0–13 | v12 |
| FindCoordinator | 10 | v0–3 | v0–6 | v3 |
| ListOffsets | 2 | v1–7 | v1–10 | v7 |
| Fetch | 1 | v4–12 | v4–18 | v12 |
| OffsetFetch | 9 | v1–7 | v1–9 | v7 |
| OffsetCommit | 8 | v2–8 | v2–9 | v8 |
| SaslHandshake | 17 | v1 | v0–1 | v1 |
| SaslAuthenticate | 36 | v0–2 | v0–2 | v2 |

## Coverage

**Fake broker (`tests/transport.rs`, 18 tests):** highest-common negotiation (Fetch capped at
12 vs broker 17, OffsetCommit 8); old broker refusing ApiVersions v3 (retried at v2);
Metadata below the client minimum → `unsupported_version` naming Metadata and nothing sent;
SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 success and wrong password (`Sasl`, password absent
from Display/Debug, config Debug redacted); unsupported mechanism (lists the offered ones);
forged SCRAM server signature; TLS with the rcgen CA (IP name and `localhost` override), TLS +
SCRAM-512; unknown CA, native roots only, wrong server name, empty/missing `ca_file` → `Tls`;
oversize, negative-size, garbage, huge-array and wrong-correlation frames → `Protocol`;
truncated frame and socket close → `Io`; silent broker → `Timeout` then reconnect;
reconnect after a dropped socket (ApiVersions again on the new connection); backoff spacing
between failed connects (50 ms, 100 ms with a zero RNG; first attempt immediate); bootstrap
list fallback and definitive-error preference.

**Unit (`--lib`, 21 tests):** negotiation tables (Kafka-4.1-like, 2.x-like, missing
OffsetCommit, broker above/below the client range, edges); ApiVersions v0-form fallback
decode; oversized array counts (legacy + varint); 20,000 seeded garbage bodies across
ApiVersions/Metadata/SaslHandshake/SaslAuthenticate decode paths and every truncation of a
valid body (no panic); RFC 7677 SCRAM-SHA-256 vector (client-final and server-final exact);
wrong server signature; SCRAM-SHA-512 against independently computed server formulas;
hostile server-first messages (foreign/short nonce, bad salt, 1 or 10^11 iterations, missing
attribute, mandatory extension, `e=`); framing split/match; oversize/negative/unknown frames
rejected without allocation; WireCheck bounds; backoff sequence and jitter range; host:port
parsing.

**Kafka 4.1.2 (`tests/broker_auth.rs`, 10 tests):** see the runs below.

## Broker runs

Local (`scripts/test-services.sh start kafka`, Kafka 4.1.2 + Temurin 21.0.13.1+2), Task 2 verify:
`test result: ok. 10 passed; 0 failed` in 1.11 s, no `SKIP:`. Error texts seen:

- PLAIN wrong password: `kafka SASL: PLAIN: authentication failed for user "ostro" (broker error 58: Authentication failed: Invalid username or password)`
- SCRAM-SHA-256/512 wrong password: `... (broker error 58: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256|512)`
- native roots only: `handshake with 127.0.0.1:<ssl> as "127.0.0.1": invalid peer certificate: UnknownIssuer`
- wrong server name: `... certificate not valid for name "not-the-broker.example"; certificate is only valid for IpAddress(127.0.0.1) or DnsName("localhost")`

Cluster (`ostro-k8s-ci --cmd '<fetch-tools + test-services + broker_auth>' <worktree> 1`): job on dell-2 at b3e7208, `exit=0 elapsed=102s`: Kafka ready in 2 s
(PLAINTEXT/SASL_PLAINTEXT/SSL on loopback), the same negotiation table as locally (Metadata v12,
Fetch v12 of broker v4..=v18, OffsetCommit v8, ...), `test result: ok. 10 passed; 0 failed` in
1.43 s, no `SKIP:`.

## Plan gate

**Gate: incomplete — cluster withdrawn; consolidated gate pending on dell-4.** The user ended
cluster use while the reruns were in progress. Nothing further runs on the cluster, so the
steps below are unverified for this plan and belong to the consolidated gate on dell-4:

- `cargo test` (test2 units): `keda_sim::zero_to_n_to_zero` never got its alone rerun.
- `musl zigbuild test` (musl2 units): `flow_e2e` alone passed 11/12 at b3e7208; `weights` hit
  a server-start timeout at load 134 and was not rerun; the base-commit (51246c0) comparison
  never ran.
- `musl binary size limit`: not rerun under main's new 70 MB limit (b89e1fb). This plan adds
  0 bytes (60,370,032 B both at 5ea700b and here).
- Verified for this plan: the lint-group steps (all PASS on slot 1); test1 units,
  failpoint suites, mutant matrices, server campaign smoke, polyglot/obuild/clients and musl
  release (PASS in the parallel run); ostro-kafka lib/transport/skeleton/clippy/fmt/tree
  checks and broker_auth 10/10 (local Kafka 4.1.2, twice, and the slot-1 cluster run).

Details of the parallel run:

`ostro-k8s-ci --parallel` at b3e7208 (p003721-32319, 251 min wall): **not green; none of the failures
trace to this plan** (the transport is the only change, and ostro-server only anchors `ostro_kafka`):

| Step | Result | Cause |
|---|---|---|
| polyglot / obuild / clients / failpoint suites / replication mutants / server campaign smoke / musl release | PASS | |
| lint group (fmt, clippy, build, sdk, guests, P13 mutants, tier C, deny, grep gates, workflow validation, ...) | NORESULT | pod killed (exit 137, `FailedKillPod` on dell-1). **Rerun on slot 1: every step PASS** (exit 0, 1095 s) |
| musl binary size limit | FAIL | 60,370,032 bytes vs 60 MB. **Pre-existing on main:** the 5ea700b run (14-03, an ancestor of this plan's base) measured exactly 60,370,032 bytes too, so this plan adds 0 bytes (the client is dead code until 14-32 calls it). Since then main raised the shared limit to 70 MB (b89e1fb, D-P14-53), so this step should pass on current main |
| cargo test (test2) | FAIL | `keda_sim::zero_to_n_to_zero`: ostro server did not start (p7.rs:712) on dell-1 under load |
| musl zigbuild test (musl2) | FAIL | `flow_e2e` 8 tests: server did not start, either no ready line within 240 s or exited before it, with log lines 15-80 s apart (dell-1 load) |

Alone reruns of `flow_e2e` (known load-sensitive) on slot 0 (dell-1): the first run failed 11/12 on
server-start timeouts. The second, at b3e7208, passed 11/12 (`weights` timed out at boot while
the node load average went from 31 to 134). `keda_sim::zero_to_n_to_zero` has not been rerun
alone yet (cargo stopped after flow_e2e). The orchestrator paused cluster use before a
base-commit comparison could run.

## Proposed ci-local hunk for 14-27 (Kafka leg)

In `scripts/ci-local.sh`, inside the test-services step 14-27 adds (after
`eval "$(bash scripts/test-services.sh env)"`):

```bash
# 14-31: TLS + SASL against the Kafka 4.1 test broker (fails on SKIP: the broker must be up)
out=$(cargo test -p ostro-kafka --locked --test broker_auth -- --test-threads=1 --nocapture 2>&1)
echo "$out" | tail -n 5
grep -Eq "test result: ok\. [1-9][0-9]* passed" <<<"$out"
if grep -q "SKIP:" <<<"$out"; then echo "broker_auth skipped: Kafka test service not running"; exit 1; fi
```

GitHub `ci.yml` (D-P14-44, the `apache/kafka:4.1.<z>` docker step 14-27 adds): the image's
default listener is PLAINTEXT only, so the SSL/SASL variables stay unset there and the same
command would SKIP. Either run broker_auth only in ci-local / `ostro-k8s-ci` pods (where
`test-services.sh` provides all three listeners), or have 14-27 start the Kafka tgz with
`scripts/test-services.sh` in the GitHub job too. Recommendation: the latter (one broker
setup everywhere). No workflow file was edited by this plan.

## Hand-off to 14-34

- Implement `versions::ApiRequest` for FindCoordinator, ListOffsets, Fetch, OffsetFetch and
  OffsetCommit next to their code (the range consts `versions::{FIND_COORDINATOR,
  LIST_OFFSETS, FETCH, OFFSET_FETCH, OFFSET_COMMIT}` exist). `check_response` is required:
  walk the response with `conn::WireCheck` (`array_len(flex, min_elem)` bounds what
  kafka-protocol pre-allocates). MetadataRequest's impl (with a full walker) is already in
  versions.rs.
- Per-broker connections: `conn::Broker::new(addr, Arc::clone(client.connect_options()))`
  reconnects on its own; `Client::bootstrap()` is the bootstrap broker.
- `Conn::request` uses the negotiated version; `Conn::request_at` forces one. Long-poll
  Fetch blocks only its own response (requests are pipelined on one connection), but a
  request that exceeds `request_timeout` closes the connection, so Fetch `max_wait_ms` must
  stay well below `request_timeout`.
- `hmac` / `sha2` remain in `lib.rs` dep anchors (unused by 14-31).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] tests/skeleton.rs asserted the 14-01 stub**
- **Found during:** Task 1
- **Issue:** `crate_links_and_client_is_a_stub_until_14_31` expected `KafkaError::Unimplemented("…14-31")` from `Client::connect`, which this plan implements.
- **Fix:** the test now connects to a just-freed loopback port and expects `Io`/`Timeout` (contract types still exercised). File not in `files_modified`.
- **Commit:** 0e5a17a

**2. [Rule 2 - Critical] Pre-decode walker against unbounded allocation**
- **Found during:** Task 1 (reading kafka-protocol 0.18 `types.rs`)
- **Issue:** kafka-protocol decodes arrays with `Vec::with_capacity(count)` straight from the wire, so a garbage count (e.g. 2^31) in a response would attempt a huge allocation (abort, not a recoverable error).
- **Fix:** `conn::WireCheck` + mandatory `ApiRequest::check_response` walk every response before decoding; counts that cannot fit in the remaining bytes are `Protocol` errors. Control responses are capped at 1 MiB.
- **Commit:** 0e5a17a

**3. [Contract] `Auth` in the plan text is `KafkaError::Sasl` in the 14-01 contract**
- The plan's must-haves say wrong credentials fail with `Auth`; the landed contract names the variant `Sasl(String)`. Kept the contract (never rename); all tests assert `Sasl`.

### TDD note

Task 1 is `tdd="true"`; tests and implementation were written in the same session and committed together in 0e5a17a (no separate RED commit), as main must not get a non-compiling head between CAS updates.

## Self-Check

## Self-Check: PASSED

- FOUND on main: crates/ostro-kafka/src/{conn,versions,sasl}.rs, tests/support/mod.rs, tests/transport.rs, tests/broker_auth.rs, README.md
- FOUND commits on main: 0e5a17a, b3e7208, 8e5a23c
- Artifact patterns: `unsupported_version` (versions.rs), `ScramSha512` (sasl.rs), `OSTRO_TEST_KAFKA_SASL_BOOTSTRAP` (broker_auth.rs); key links `sasl::` and `versions::negotiate` in conn.rs
- No `.key`/`.pem` tracked under crates/ostro-kafka; `cargo tree -e normal` has no rdkafka-sys/openssl-sys/lz4-sys/zstd-sys/native-tls
