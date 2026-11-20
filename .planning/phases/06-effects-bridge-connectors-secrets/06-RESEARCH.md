# Phase 6: Effects Bridge, Connectors & Secrets - Research

**Researched:** 2026-11-19
**Domain:** Durable outbound I/O. This covers the HTTP outbox with idempotency keys, exactly-once SQL through a transactional token, object storage, a sealed secret store with pluggable providers, origin-bound secret templating, and an SSRF-hardened egress client.
**Confidence:** HIGH for the dependency and licence results, which were checked by resolving and running `cargo deny` against the real workspace. HIGH for the SSRF mechanics, which were checked in reqwest/hyper-util source and with a `url` crate probe. MEDIUM for some exact API shapes (hickory 0.26 builder, reqwest `IntoResolve`). Those are marked inline. Provider idempotency windows: Stripe's is verified, the rest are MEDIUM or LOW.

## Summary

Phase 6 replaces the Phase 4/5 "non-retryable `unsupported`" completion for http/sql/object effects with a real host I/O bridge in the empty `crates/ostro-bridge`. Most of the contract already exists:
- The WIT `effects` interface has `http-fetch`, `sql-query` and `object-store` with `idempotency-key`, `retry`, `redact` and `exactly-once` fields.
- The journal mirrors are `EffectScheduled` (kind 18), which journals the `EffectRequest`, and `EffectCompleted` (kind 65), which carries a `RecordedResult` of `Result<OpOutput, Failure>`.
- The command hash already excludes HTTP header values, so `{{secret:..}}` templates and redacted values never break replay.
- The partition store reserves an `effects` keyspace (#12).
- The meta records `SealedSecret`, `Grant`, `Connection` and `EgressPolicy` exist.

Phase 6 therefore needs **no WIT record changes**. It does need **doc-comment clarifications that must land in 05-05 before the 05-19 freeze**: after 05-19 the file is pinned by sha256, so even comment bytes are frozen. It also needs additive partition contract items: `Cmd::CompleteEffect`, `Intent::RegisterEffect`/`CancelEffect`, and `effects` rows.

The central design rule: **an effect is re-driven only inside a replaying activation, never from the journal alone.** The journal's `HttpRequest` mirror deliberately omits the retry policy, and redacted header values are not journaled. The full request (retry policy, real header values) is available only when the guest re-issues the call during replay. The command hash matches because header values and retry are not hashed. Effect I/O runs in a node-owned task (an `EffectRegistry` keyed by `(run, op)`), so it survives activation eviction. Its result is committed through a new attempt-independent command, `CompleteEffect`, which journals `EffectCompleted` once (first completion wins, like `CompleteToken` in 05-07). After a crash, the leader wakes every run that has open `effects` rows so that replay re-drives them. Suspended runs would otherwise sleep forever, which contradicts the "no sweeper" claim in docs/03 §5.2.

Two dependency findings change the stack in `.planning/research/STACK.md`:
1. **`object_store` 0.14.2 has a non-optional `chrono` dependency.** Chrono is banned in deny.toml (RUSTSEC-2026-0543, published 2026-09-14), and object_store also brings in `humantime` (RUSTSEC-2026-0542). Upstream issue apache/arrow-rs-object-store#909, opened 2026-11-17, notes that removal needs a breaking release because `ObjectMeta::last_modified: DateTime<Utc>` is public API. Verified: `cargo deny` fails with `banned chrono` plus two `unmaintained` errors. Phase 8 research (08-RESEARCH.md, commit e757400) found the same thing independently. **Aligned decision:** Phases 6 and 8 share one in-house crate, **`ostro-objstore`**, with a local-fs backend and an S3-compatible backend on **rusty-s3 0.12.0** (BSD-2-Clause, sans-IO SigV4, jiff 1.0 / hmac 0.13 / sha2 0.11, all already pinned) over the existing reqwest 0.13.5 + rustls. Whichever phase runs first creates the crate. Presign fits rusty-s3 natively (verified in source; see §Object store). With rusty-s3 instead of object_store, the full Phase 6 stack passes the **stock** deny.toml, with no exception and no chrono in the graph (verified).
2. **sqlx `tls-rustls-aws-lc-rs` pulls `webpki-roots`, licensed `CDLA-Permissive-2.0`.** That licence is not in the allow-list, so `cargo deny` fails. `tls-rustls-aws-lc-rs-native-roots` passes. mysql_async's rustls feature always pulls webpki-roots too, which is another reason to choose sqlx.

The full candidate stack passes bans, licences and advisories against the real workspace and the **unmodified** deny.toml, with exactly one version each of hyper, http, tonic, prost, axum, reqwest, rustls and wasmtime. The stack is sqlx 0.9.1, rusty-s3 0.12.0 (via `ostro-objstore`), hickory-resolver 0.26.3, vaultrs 0.8.0, kube 5.1.0, k8s-openapi 0.29.0, zeroize, secrecy, governor, ipnet and aho-corasick.

**Primary recommendation:** Build `ostro-bridge` as three layers:
- an egress core: a `url`-normalised gate, a resolver-level IP classifier with pinned connect, and a reqwest client with `no_proxy`, no auto-redirect, no referer and no gzip;
- connectors: HTTP, SQL (sqlx 0.9.1 native-roots) and object (a thin adapter over the shared `ostro-objstore` crate: rusty-s3 S3-compatible + local fs);
- secrets: a sealed store on aws-lc-rs AEAD (already in the tree), env/file/Vault/K8s providers, origin-bound grants and a response scrubber.

Wire it through an `EffectDriver` hook in the engine and an `EffectRegistry` + `CompleteEffect` command in the server.

<user_constraints>
## User Constraints

There is no CONTEXT.md for Phase 6: discuss-phase was not run, and the project runs in YOLO + auto_advance. The binding constraints below come from ROADMAP.md, STATE.md, PROJECT.md, deny.toml and the Phase 5 standing rules in 05-DECISIONS.md. The planner must treat them as locked.

### Locked Decisions (from project files)
- **Freeze:** WIT 0.1, the on-disk format and the command set freeze at the end of Phase 5 (05-19). After that, changes are additive only:
  - new functions or interfaces go in a new `wit/ostro-runtime-0.1.N/` package that passes `ostro-witcheck`;
  - new `Cmd` tags, oneof variants, enum values, keyspaces and meta variants are appended at the end;
  - existing records, variants and enums never gain fields or cases;
  - `Cmd` tags 1–7 (and 8–12 from Phase 5) are frozen;
  - journal kinds are append-only, with no `EVENT_CODEC_VER` bump.
- **D14 (05-DECISIONS):** no WIT record/variant/enum shape changes. Every candidate takes the "new function in 0.1.N later" path. Phase 5 WIT edits are doc comments only. Listed candidates: object presign expiry and content-type, http redirect/size policy.
- **cargo-deny:** no duplicate tonic, tonic-prost, prost, prost-types, hyper, http, axum, opentelemetry, opentelemetry_sdk or wasmtime. No openssl-sys or native-tls (rustls + aws-lc-rs only). No chrono (RUSTSEC-2026-0543), no bincode, no prometheus, no tokio-uring. The licence allow-list excludes CDLA-Permissive-2.0.
- No wasmtime `cache` feature. No C++ dependencies (single static binary on musl x86_64/aarch64 and macOS).
- Secrets never enter guest memory. Guests have no ambient authority. There is no raw `wasi:http`/`wasi:sockets` for guests.
- `bash scripts/ci-local.sh` is the gate. Builds are warning-free. clippy runs with `-D warnings`. `FAILPOINT_CRATES` stays in sync with ci.yml. Failpoints use the `fail` crate only. The failpoint binary goes in `target/failpoints`.
- Never reference a Chronik clone path. No remotes or pushes. Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Executors never run gsd-tools state/roadmap/phase/requirements mutators.
- All third-party pins live in the root `Cargo.toml` `[workspace.dependencies]` (Phase 4 convention).
- Phase 7 owns workers, mTLS `ResolveSecret` and cluster-wide flow control. Phase 8 owns OTel/OTLP and bottomless. Phase 9 owns the Python/TS SDKs.

### Claude's Discretion
- Crate choices within the bans, connector configuration shape, the allow-list/grant model, test infrastructure and plan breakdown. The recommendations are below.

### Deferred / Out of scope for Phase 6
- Kafka connector (FR-4.6), polyglot connector components (FR-4.7), cluster-wide token buckets owned by a partition (FR-4.5's "metadata-assigned partition" part; Phase 7/13), OTLP export (Phase 8), worker-side bridge and mTLS `ResolveSecret` (Phase 7), KMS-backed sealing key (Phase 14), UI surfacing of the unknown-outcome warning (Phase 11).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| FX-01 | HTTP effect: intent committed before I/O; timeouts, response size cap, redirect policy, retries per policy | §Architecture "Effect lifecycle" (I/O only after the `EffectScheduled` commit, via `flush` then `EffectDriver::start`), §HTTP effect (per-attempt timeout, streaming size cap, redirect rules, retry classification) |
| FX-02 | Stable idempotency key (run, op, name), user-overridable | §Idempotency key: `ostro-` + 32 hex of sha256("ostro/idem/v1"‖run_id‖op). WIT has no `name` field; the op seq is the stable name. The `idempotency-key` field overrides it. |
| FX-03 | Replay re-drives in-flight effects with the same key; past the provider window → `effect_outcome_unknown` | §Re-drive rule + §Unknown-outcome window (window measured from the `EffectScheduled` journal ts; idempotent methods exempt; per-destination `idempotency_window`, default 24h) |
| FX-04 | SQL connector (PG, MySQL), named connections; exactly-once via `ostro_effects` token | §SQL exactly-once protocol, MySQL implicit-commit list, PG no-transaction-block list, statement allow-list, sqlx 0.9.1 |
| FX-05 | Object store (S3/GCS/Azure/local via `object_store`) get/put/presign. **Wording update proposed**, see §Object store | §Object store (shared `ostro-objstore`: rusty-s3 S3-compatible incl. GCS via HMAC interop + local fs; rusty-s3 `S3Action::sign` presign; HMAC Ostro-served presign for local; Azure deferred) |
| FX-06 | `{{secret:name}}` resolved only in the bridge, zeroized, never journaled or logged, never visible to guests | §Secrets: template grammar, resolution point, `Zeroizing`/`SecretString`, `HeaderValue::set_sensitive`, canary harness |
| FX-07 | Grants bound to (module, secret, origins); no redirects on templated requests; responses scrubbed | §Grants, §Redirect rules (reqwest strips only Authorization/Cookie/Proxy-Authorization cross-host, verified in source), §Scrubber |
| FX-08 | Per-module allow-lists; SSRF guard (private, link-local, metadata, mapped IPv6, NAT64, decimal/octal, `fd00:ec2::254`, redirects, env proxies) | §SSRF: bypass table, url-crate normalisation probe, the hyper-util IP-literal resolver bypass (verified), `no_proxy` (reqwest reads env proxies by default, verified) |
| FX-09 | Per-target rate limits | §Rate limits: governor 0.10.4 keyed limiter per target (origin / connection), per node |
| FX-10 | Sealed store, env/file, Vault, Kubernetes providers | §Secret providers: aws-lc-rs AEAD sealed store in `SealedSecret`, env/file, vaultrs 0.8.0, kube 5.1.0 |
</phase_requirements>

## What Exists Today, and What Phases 4 and 5 Will Deliver

| Item | Where | State for Phase 6 |
|------|-------|-------------------|
| WIT `effects` interface | `wit/ostro-runtime-0.1.0/runtime.wit` | `http-request{method,url,headers,body,timeout-ms,idempotency-key,retry,redact}`, `http-response{status,headers,body}`, `sql-value` (9 cases), `sql-request{connection,statement,params,exactly-once}`, `sql-result`, `object-request{connection,op:string,key,body}`, `op-output{http,sql,object(list<u8>),…}`. Frozen at 05-19. |
| Host impl | `crates/ostro-engine/src/host/effects.rs` | `http_fetch`/`sql_query`/`object_store` → `schedule_effect` → replay-core command → `EffectScheduled`. `convert::http_request_from_wit` **drops `retry`** and journals header values verbatim, including names in `redact`. `hot_waitable` treats an in-flight `EffectScheduled` as hot-parkable (≤ `engine.hot_wait`, 2 s). |
| Journal mirror + hash | `crates/ostro-types/src/journal.rs` | Http hash = method, url, sha256(body) and sorted lower-case header names. Header values, timeout, idempotency key, redact and retry are **not hashed**. Sql hash = connection, statement, sha256(params); `exactly_once` is not hashed. Object hash = connection, op, key, sha256(body). `EffectScheduled`=18, `EffectCompleted`=65. Event kinds 70/71 are taken by Phase 5, so 72+ are free. |
| Sink | `crates/ostro-engine/src/sink.rs` | `ActivationSink{append, fetch_blob, done}`, with the comment "Secrets are Phase 6 and deliberately absent". |
| "unsupported" completion | 04-15 `PartitionSink` (ostro-server) | After an `EffectScheduled` append, a second `EventsAppended` with the **same attempt token** journals `EffectCompleted(Err NonRetryable{kind:"unsupported"})`. 05-10 keeps this for effects and children. **Phase 6 replaces it for effects.** |
| Executor lease/self-fence | 04-16 `EngineExecutor` | Self-fence at `lease_ttl - margin`, with a revoke → `CancellationToken`. 04-RESEARCH Pitfall 14 says "a fence on the journal does not stop I/O. That matters from Phase 6." |
| Input journaling helper, terminal hook | 05-02 (`apply/inputs.rs`, `apply/terminal.rs`) | Reuse these for `CompleteEffect` (journal an input event, then wake the run) and for effects-row cleanup on terminal. |
| `CompleteToken` pattern | 05-07 | Wait row = token, first completion wins, second is `Duplicate`. Phase 6 copies this shape for `CompleteEffect`. |
| `effects` keyspace | `docs/storage-format.md` §6 #12 | "reserved (no rows written in Phase 2)". Phase 6 writes rows here. Partition state values are derived (D18), so this is additive. |
| Meta records | `crates/ostro-meta/src/records.rs` | `SealedSecret{name, ciphertext, key_id}`, `Grant{id, subject, scopes}`, `Connection{name, kind, config: Vec<u8>}`, `EgressPolicy{name, allow: Vec<String>}`. The await-token MAC key is stored as `SealedSecret "__ostro/token-key"` with `key_id "plaintext-v0"` (04-01). |
| Failpoints | 05-17 | `failpoints` feature chain on `ostro`, `FAILPOINTS` env, `--target-dir target/failpoints`. Phase 6 adds `ostro-bridge` to the chain. |
| Rust SDK | `sdks/rust` (04-05/04-06/05-06) | http/sql/object op handles exist and complete "unsupported". Phase 6 adds ergonomic builders. |
| Local env | this machine | PostgreSQL 14 is running at 127.0.0.1:5432. The MySQL 8.0 client is installed but **no mysqld**. **Docker is not available inside WSL** (the CLI exists only on the Windows path). |

## Pre-Freeze Findings (WIT and format): Act in Phase 5

**Bottom line: Phase 6 needs no WIT record, variant or enum changes, so D14 holds.** But `wit_freeze::frozen_0_1_0_is_byte_identical` (05-05/05-19) pins the **whole file's sha256, including comments**. Any semantics Phase 6 wants documented in `runtime.wit` must therefore be written **in 05-05 Task 3** (doc-comment-only edits, already planned) **before 05-19**. These items should go back to 05-05 as findings:

| # | Where in runtime.wit | Doc comment to add (frozen semantics Phase 6 implements) | Severity |
|---|---|---|---|
| W1 | `record header` / `http-request` | `{{secret:NAME}}` placeholders are resolved by the host **only in header values** (and in the body only when the secret's grant allows body templating). They are **never resolved in the URL, method or header names**. A request that contains a placeholder **never follows redirects**: a 3xx is returned as the response. Each secret is usable only towards the origins its grant lists. Any resolved secret value (raw, base64, hex, percent-encoded or JSON-escaped) is replaced with `[REDACTED]` in the response before the journal or the guest sees it. | High: without it, guests may expect URL templating (docs/03 §9.1 already forbids it). |
| W2 | `http-request.idempotency-key` | If absent, the host derives `ostro-<32 hex>` from (run id, op id) and sends it as `Idempotency-Key`; the header name can be configured per destination. The key is stable across attempts and resumes. A retry-as-new run or a continue-as-new run gets new keys. | Medium |
| W3 | `http-request.redact` | Values of these request headers are replaced in the journal and logs. Response headers with these names are returned to the guest redacted, so live and replay are identical. | Medium: the current comment implies only journal/log redaction. Response redaction must also apply live, because completions reach the guest only through journaled inputs. |
| W4 | `http-request.timeout-ms` / `retry` | `timeout-ms` applies per attempt. Host retries happen only for retryable conditions (connect failure, 408/425/429/5xx, transport loss) using the same key. A re-drive after a crash, past the destination's idempotency window, completes with `failure::non-retryable{kind: "effect_outcome_unknown"}` and is never re-sent. | High (FX-03) |
| W5 | `http-response` | Any final status is a successful completion; the guest inspects `status`. A body over the host cap fails with `non-retryable{kind:"response_too_large"}`. | Medium |
| W6 | `sql-request.exactly-once` | It runs one statement in a transaction together with an `ostro_effects` token row in the same database. A re-drive returns the stored result. Statements that implicitly commit or control transactions are rejected with `non-retryable{kind:"sql_not_transactional"}`. `exactly-once=false` is at-least-once on re-drive. | High (FX-04) |
| W7 | `object-request.op` | Values `get` \| `put` \| `delete` \| `head` \| `presign-get` \| `presign-put`. Output encodings: `get` returns bytes. `put`, `head` and `delete` return UTF-8 JSON metadata, or empty for `delete`. `presign-*` returns a UTF-8 URL whose expiry comes from the connection config until a 0.1.N `object-presign` function exists. Unknown op → `non-retryable{kind:"unsupported_op"}`. | High: the op is a free string, and the output is untyped bytes. |
| W8 | `types.error-info.kind` | Reserved bridge kinds: `effect_outcome_unknown`, `egress_denied`, `secret_not_granted`, `secret_not_found`, `rate_limited`, `response_too_large`, `connection_unknown`, `sql_not_transactional`, `sql_exactly_once_violated`, `unsupported_op`, `http_transport`. | Medium |

Non-WIT items (format, also frozen at 05-19):
- **F1 (no change needed, architecture lock).** The journal `HttpRequest` mirror omits `retry`, and its struct shape freezes with `EVENT_CODEC_VER` 1. This is acceptable **only because** re-drive happens inside the replaying activation, where the guest re-supplies the full WIT request. Phase 6 must never build a leader-side sweeper that re-sends from the journal.
- **F2 (no change needed).** Phase 6 changes *what string* is journaled for redacted header values (`"[redacted]"`), not the codec. Existing golden and corpus journals stay valid.
- **F3 (coordination, now concrete).** Phases 6, 7 and 8 append to the same ordinal spaces in parallel. Phase 6's exact needs are in §Ordinal Reservations below, for the coordinator's reservation table (Phase 8 finding F4).
- **F4 (no change recommended).** Phase 6 can use the thin meta records through documented conventions; see §Meta record conventions. If the Phase 5 planner prefers typed shapes, the only pre-freeze alternative is to reshape `Grant`/`EgressPolicy` now. After the freeze, Phase 6 can only append new `MetaRecord` variants: postcard structs cannot gain fields.

## Ordinal Reservations Phase 6 Needs (for the cross-phase reservation table)

Phases 6, 7 and 8 run in parallel after the 05-19 freeze, and all append to ordinal-encoded spaces (08-RESEARCH F4). Phase 6's exact needs, assuming Phase 5's numbering (Cmd 8–12 used; Intent 7–8 used; journal kinds 70–71 used; TerminalStatus 3–5 used; outbox Kind 6 used):

| Space | Encoding (why it collides) | Phase 6 needs | Items | Notes |
|---|---|---|---|---|
| `Cmd` oneof tags (`ostro_types::command`) | prost tag | **1** (Phase 8 proposes blocks of 7; Phase 6 uses 1) | `CompleteEffect{run_id, op u64, outcome PayloadRef (inline or blob), completed_at_ms}` | Attempt-independent. Apply: effects row present → journal `EffectCompleted`, delete row, wake; absent with op < next_seq → `Duplicate` (existing code 1); run terminal → existing `RunTerminal` reject. |
| `Derived` intent oneof tags (submitter-derived, inside `EventsAppended`) | prost tag | **2** | `RegisterEffect{op, kind u8 (http/sql/object), scheduled_at_ms}`, `CancelEffect{op}` | **This space is missing from Phase 8's F4 list; add it to the table.** Phase 7 may need intents too. |
| State keyspaces (`TABLES`, digest order) | declaration order = digest order | **0 new** | Uses the **already-reserved `effects` keyspace (#12)**. Key `run(18 bytes) ‖ op BE u64` → `VALUE_FORMAT ‖ postcard EffectRow{kind u8, scheduled_at_ms u64}` | Digest order is unchanged. The first rows appear in Phase 6, which is additive (D18: derived values). No new `RunRecord` field: an `open_effects` counter would be a VALUE_FORMAT change, so the terminal cleanup prefix-scans `effects` by run instead. |
| Journal `EventBody` kinds | u32 kind + enum position (postcard) | **0** | none (`EffectScheduled` 18 / `EffectCompleted` 65 exist) | An optional observability `EffectAttempt` kind is not needed for FX-*, so nothing is reserved. |
| Apply outcome codes / `RejectReason` | 16 + index, folded into the state hash | **0** | reuse `Duplicate`, `RunTerminal` | |
| `InvalidCode` | appended index | **1** | `BadEffectCompletion` (CompleteEffect for an op ≥ next_seq or an op that is not an effect) | Bad `RegisterEffect`/`CancelEffect` intents reuse the existing `BadIntent`. |
| `MetaRecord` variants / `MetaKind` / `MetaKey` | postcard variant index | **0** | Uses the existing `Secret(SealedSecret)`, `Grant`, `Connection` (opaque `config` bytes = versioned JSON) and `Egress(EgressPolicy)` through the conventions below | Deliberately avoids new variants, so `MetaState` gains no map (Phase 8 F3: a new map changes the snapshot struct). |
| `MetaState` fields | positional postcard | **0** | none | |
| Outbox `Kind` / `TerminalStatus` | prost tags / enum values | **0** | none | |
| WIT packages `0.1.N` | directory/package version | **0** | no 0.1.1 in Phase 6 (D-FX8) | |
| Golden files (append-only) | line order | `commands.txt` +1 Cmd and +2 intent lines; `journal_v1.txt` +0 | | |

Non-ordinal names to reserve (they can only collide by name):
- Config sections `[secrets]`, `[egress]`, `[connectors]`.
- Failpoint prefix `bridge::`.
- REST prefixes `/v1/secrets`, `/v1/grants`, `/v1/connections`, `/v1/egress`, `/v1/objects/{connection}/{key}` (local presign).
- Sealed-secret prefix `__ostro/` (exists: `__ostro/token-key`; Phase 6 adds `__ostro/presign/<connection>`).
- Grant id prefix `secret:`.
- The crate `ostro-objstore` (shared with Phase 8; first runner creates it).

**Cross-phase coupling:** Phase 8 webhook verifiers carry a "secret ref". They should resolve it through Phase 6's `SecretProvider` chain and sealed store, not a second mechanism. If Phase 8 lands first, it reads `SealedSecret` using the `key_id` convention below (`plaintext-v0` | `aes256gcm:<kid>`).

### Meta record conventions (no format change)
- `SealedSecret{name: "<tenant|_>/<name>", ciphertext: [ver][nonce 12][ct+tag], key_id: "aes256gcm:<kid>" | "plaintext-v0"}`.
- `Grant{id: "secret:<module>:<secret>", subject: "module:<module>", scopes: ["origin:https://api.stripe.com", …, "body"]}`.
- `Connection{name, kind: "sql-postgres" | "sql-mysql" | "object" | "http-target", config: JSON {"v":1, …}}`.
- `EgressPolicy{name: <module>, allow: ["https://api.stripe.com", "https://*.example.com:8443", …]}`.

## Standard Stack

Versions were verified against crates.io on 2026-11-19. Deny results come from a probe crate that path-depends on ostro-api, ostro-server, ostro-partition, ostro-engine and ostro-meta, seeded with the workspace `Cargo.lock` and checked with the project `deny.toml`.

### Core
| Library | Version | Purpose | Why standard / verification |
|---------|---------|---------|-----------------------------|
| reqwest | 0.13.5 (existing pin; promote to a normal dep of ostro-bridge; features `rustls`, `http2`, `stream`; **not** `gzip`, not `hickory-dns`) | HTTP effect client | It is already in the workspace. It supports `dns_resolver`, `no_proxy()`, `redirect(Policy)`, `referer(false)`, `https_only`, `connect_timeout`, `timeout`, `read_timeout` and `connector_layer` (docs.rs 0.13.5). Note that reqwest 0.13.5 unconditionally depends on tower-http **0.6** for follow-redirect, which duplicates Ostro's tower-http 0.7. That duplicate is a warning only, since tower-http is not in the deny list. |
| hickory-resolver | 0.26.3 (`default-features=false`, `tokio`, `system-config`) | DNS for the egress resolver (classify every address) | Async, no getaddrinfo thread pool, no inet_aton quirks. Deny-clean. Use it directly, not reqwest's `hickory-dns` feature, to avoid version drift. |
| url | 2.5.8 (already in lock) | WHATWG URL parsing and host normalisation | Probe: decimal, octal, hex, short and percent-encoded IPv4 forms normalise to `Host::Ipv4`. See the SSRF section. |
| ipnet | 2.12.2 (already in lock) | CIDR tables | |
| sqlx | **=0.9.1** (`default-features=false`; `runtime-tokio`, `tls-rustls-aws-lc-rs-native-roots`, `postgres`, `mysql`, `json`, `time`, `bigdecimal`, `uuid`) | PG and MySQL connector with pooling | One API for both databases, pure Rust. **Deny-clean only with `-native-roots`.** `tls-rustls-aws-lc-rs` pulls webpki-roots (CDLA-Permissive-2.0, rejected). No `macros`/`migrate`/`any`/`sqlite` (no libsqlite3-sys in the graph; verified). No `chrono`: `time` 0.3.55 is used for DATETIME/TIMESTAMPTZ decoding and `bigdecimal` for NUMERIC/DECIMAL. 0.9.0 shipped 2026-05-21, 0.9.1 on 2026-11-18. |
| rusty-s3 (via the shared **`ostro-objstore`** crate, created by Phase 6 or Phase 8, whichever runs first) | 0.12.0 | S3-compatible object effect (and Phase 8 bottomless) | Sans-IO SigV4 with first-class presign: `bucket.get_object(Some(&creds), key).sign(Duration)` / `put_object(..)` → `Url` (verified in 0.12.0 source: `S3Action::sign`, 1 s–7 d validity, `query_mut` for e.g. `response-content-type`, `headers_mut` for signed headers such as `content-type`, `Credentials::new_with_token` for session tokens). BSD-2-Clause. Deny-clean with the stock deny.toml. Transport = reqwest 0.13.5 (`rustls`, `stream`). |
| aws-lc-rs | 1.19.0 (already in tree via rustls) | Sealed-store AEAD (AES-256-GCM, `RandomizedNonceKey`) | No new crypto crate. FIPS-capable provider already linked. |
| zeroize | 1.9.2 (already in lock; add the `derive` feature) | Wipe Ostro-owned secret buffers | |
| secrecy | 0.10.3 | `SecretString`/`SecretBox` with redacted Debug and explicit `expose_secret()` | Deny-clean. kube already depends on it. |
| governor | 0.10.4 | Per-target GCRA rate limiting (keyed) | Deny-clean. |
| aho-corasick | 1.x (1.1.5 already in lock) | Multi-pattern scrubbing of secret values and encodings | |
| fail | 0.5.1 (existing) | Failpoints `bridge::*` | |

### Secret providers
| Library | Version | Purpose | Notes |
|---------|---------|---------|-------|
| vaultrs | 0.8.0 (`default-features=false`, `rustls`) | Vault KV v2 reads; token/AppRole/Kubernetes auth | Uses reqwest 0.13, the same version as Ostro, so there is no duplicate. rustify pulls thiserror 1, a warn-only duplicate. Deny-clean. |
| kube | 5.1.0 (`default-features=false`, `client`, `rustls-tls`, `aws-lc-rs`) + k8s-openapi 0.29.0 (`v1_33`) | Kubernetes `Secret` reads via the API | MSRV 1.95. Pulls jiff **0.2** (a warn-only duplicate with jiff 1.0.1) and hyper-rustls 0.27 (already in lock). Deny-clean. Put it behind a cargo feature `k8s` (on by default) so binary size can be measured. |

### Test-only
| Library | Version | Purpose | When |
|---------|---------|---------|------|
| axum (existing) | 0.8.9 | Mock provider: payment API with key dedupe, echo, redirect, slow, oversize | ostro-testkit |
| (none) testcontainers | — | **Not recommended** | Docker is unavailable in WSL here. testcontainers-modules 0.15 pins testcontainers ^0.27 (lagging 0.28). Use GitHub Actions service containers plus env-gated tests. |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| sqlx | tokio-postgres 0.7.18 + tokio-postgres-rustls 0.14.0 + mysql_async 0.37.1 | Two APIs and two pools (PG needs deadpool/bb8). mysql_async `rustls-tls` **always** pulls webpki-roots, so it fails the licence check (verified). It adds no capability that matters here. |
| `ostro-objstore` (rusty-s3 + local fs) | object_store 0.14.2 + a scoped deny exception (`wrappers = ["object_store"]` plus ignores for RUSTSEC-2026-0543/-0542) | Verified to pass with the exception, and it brings GCS/Azure/multipart for free. **Rejected** (aligned with Phase 8) while chrono and humantime are required: it relaxes a standing rule, and chrono would sit in public API types. Re-evaluate when arrow-rs-object-store #909/#908 ship. The `ostro-objstore` trait keeps a later swap cheap. |
| aws-lc-rs AEAD | chacha20poly1305 0.11.0 | It is deny-clean but adds a crate for no gain. |
| vaultrs | hand-rolled KV v2 GET with reqwest | Smaller, but re-implements AppRole/K8s login and token renewal. |
| kube | hand-rolled in-cluster GET with the SA token + CA | ~80 lines, but loses kubeconfig/out-of-cluster support and token refresh. The **file provider already covers mounted secrets**. |
| native roots for SQL TLS | allow `CDLA-Permissive-2.0` in deny.toml (webpki-roots) | Bundled roots work in scratch containers. This is a licence-policy decision (D-FX2). Native roots plus a per-connection `ca_file` covers managed databases (RDS/Cloud SQL ship their own CA bundles). |

**Workspace pins (06-01 adds all of them, skipping any Phase 8 already pinned; later plans never touch Cargo.toml):**
```toml
# Phase 6 effects bridge (versions verified 2026-11-19)
sqlx = { version = "=0.9.1", default-features = false, features = ["runtime-tokio", "tls-rustls-aws-lc-rs-native-roots", "postgres", "mysql", "json", "time", "bigdecimal", "uuid"] }
rusty-s3 = "0.12.0"   # shared with Phase 8 (ostro-objstore); add only if Phase 8's 08-01 has not already pinned it
hickory-resolver = { version = "0.26.3", default-features = false, features = ["tokio", "system-config"] }
url = "2.5.8"
ipnet = "2.12.2"
aws-lc-rs = "1.19"   # already in the graph via rustls; use the same feature set rustls enables (check `cargo tree -e features -i aws-lc-rs`) so no second aws-lc-sys build appears
zeroize = { version = "1.9.2", features = ["derive"] }
secrecy = "0.10.3"
governor = "0.10.4"
aho-corasick = "1"
vaultrs = { version = "0.8.0", default-features = false, features = ["rustls"] }
kube = { version = "5.1.0", default-features = false, features = ["client", "rustls-tls", "aws-lc-rs"] }
k8s-openapi = { version = "0.29.0", features = ["v1_33"] }
```
**deny.toml:** no change. The full Phase 6 stack, including rusty-s3, sqlx native-roots, hickory, vaultrs, kube, governor, secrecy, zeroize and aho-corasick, passes `cargo deny check bans licenses advisories` against the stock deny.toml (verified 2026-11-19; only warn-level duplicates such as thiserror 1/2, jiff 0.2/1.0 via kube, tower-http 0.6/0.7 via reqwest).

## Architecture Patterns

### Recommended structure
```
crates/ostro-bridge/src/
├── lib.rs              # Bridge facade: start(effect) -> completion future; config; error kinds (W8)
├── driver.rs           # EffectDriver impl, EffectRegistry {(run,op) -> task}, pre-send gate, outcome encoding
├── egress/
│   ├── url_gate.rs     # parse+normalise, scheme/port/userinfo rules, IP-literal classification, allow-list match
│   ├── classify.rs     # IP blocklist table + unwrap (mapped, compat, NAT64, 6to4); Teredo blocked
│   ├── resolver.rs     # reqwest::dns::Resolve over hickory: resolve once, reject if ANY addr blocked
│   └── client.rs       # reqwest ClientBuilder: no_proxy, redirect none/custom, referer(false), no gzip, timeouts
├── http.rs             # HTTP effect: key, window, retry classification, templating, size cap, scrub
├── sql/{mod,pg,mysql,classify,value}.rs   # pools, exactly-once protocol, statement allow-list, value mapping
├── object.rs           # effect adapter over ostro-objstore: op dispatch, JSON encodings, caps, presign (rusty-s3 sign / local HMAC route)
├── ratelimit.rs        # governor keyed limiter per target
└── secrets/
    ├── value.rs        # SecretValue (SecretBox<[u8]>), Zeroizing
    ├── template.rs     # {{secret:NAME}} parser (never resolves; returns spans)
    ├── grants.rs       # (module, secret) -> origins, body flag
    ├── scrub.rs        # aho-corasick over value + encodings
    ├── sealed.rs       # AES-256-GCM envelope in SealedSecret
    └── providers/{env,file,vault,k8s,chain}.rs
```
Dependency direction: `ostro-bridge` depends on `ostro-types` (journal mirrors and `encode_result`) and `ostro-meta` (records), and on neither `ostro-engine` nor `ostro-partition`. `ostro-engine` defines the `EffectDriver` trait. `ostro-server` wires the bridge to the engine and the partition handle.

### Effect lifecycle
```
guest http_fetch(req)                        (live)
  └─ host: EffectScheduled{request with redacted values → "[redacted]"} queued; op = seq
     host keeps LiveEffect{op, full WIT req (real header values, retry)} in HostState.pending_effects
guest wait([op]) → flush() commits EffectScheduled            ← FX-01: I/O only after this commit
  └─ for each pending effect now committed: driver.start(run, attempt, op, LiveEffect, scheduled_ts)
     registry: (run,op) already running? → attach (no second send) : spawn task
task (node-owned, survives eviction):
  pre-send gate (not cancelled, partition still led, run not terminal) → resolve secrets (grant check)
  → egress (url gate, resolver, pinned connect) → response → scrub → encode RecordedResult
  → submit Cmd::CompleteEffect{run, op, outcome} (no attempt token)
apply CompleteEffect: effects row present? → journal EffectCompleted at next_seq, delete row,
  wake run (InputAppended if Running → hot activation gets the input; else EnqueueReady) : Duplicate
guest's wait resolves from the journaled input (same bytes live and on replay)

replay after crash/evict:
  host replays EffectScheduled(op) → completion_for(op) is None in the loaded journal
  → re-drive: driver.start(run, attempt, op, LiveEffect from the guest's re-issued WIT req, scheduled_ts)
     (HTTP: window check first; SQL: token check makes it exact)
leader start / restart: scan `effects` rows → EnqueueReady(run) for runs not already Ready/Running
  (otherwise a run Suspended on an in-flight effect whose task died never wakes)
```
Derive mappings (pure, unit-tested, added to 05-10's derive table):
- `EffectScheduled(op)` → `Intent::RegisterEffect{op, kind, scheduled_at_ms}`, which creates the effects row.
- `OpCancelled(op)` where op is an effect → `Intent::CancelEffect{op}`. This deletes the row and cancels the registry task (best effort; the side effect may already have happened).
- Terminal → delete all effects rows of the run (05-02 terminal hook).

### Engine seam (additive, default impls keep the existing tests compiling)
```rust
// crates/ostro-engine (new file effects_driver.rs or in sink.rs)
pub struct LiveEffect { pub op: OpId, pub request: LiveRequest /* full WIT-derived request incl. retry + real header values */, pub scheduled_ts_nanos: u64 }
#[async_trait::async_trait]
pub trait EffectDriver: Send + Sync {
    /// Called after the EffectScheduled for `op` is committed (live) or found without completion (replay).
    /// Must be idempotent per (run, op): a second call attaches to the running task.
    fn start(&self, run: &RunRef, attempt: u32, effect: LiveEffect);
}
// Activate gains `effects: Option<Arc<dyn EffectDriver>>`; None => Phase 4/5 behaviour (tests).
```
In Phase 7 the worker supplies its own `EffectDriver` (the bridge runs in the worker) plus `ActivationSink::resolve_secret` and `complete_effect` over gRPC. Shaping the trait now avoids a rewrite.

### Idempotency key (FX-02)
```rust
// key = "ostro-" + hex(sha256(b"ostro/idem/v1" || run_id_utf8 || op.to_le_bytes())[..16])  → 38 chars
// Fits Stripe (≤255), Adyen (64) and Square payments (45, per third-party report).
// Never includes attempt (Pitfall 5). The user override (`idempotency-key`) is sent verbatim.
```
FX-02's "(run, op, name)": WIT `http-request` has no name field. The op seq is the deterministic per-run name, and SDKs can pass a semantic key through `idempotency-key`. Document this; do not add a field (D14).

### Re-drive rule and unknown-outcome window (FX-03)
Decision at **re-drive time only**, meaning replay found `EffectScheduled` without a completion. A live first dispatch always sends.
```
elapsed = wall_now - EffectScheduled.ts      // conservative: ts ≤ first send
if method in {GET, HEAD, OPTIONS, PUT, DELETE} (RFC 9110 idempotent) and !dest.force_window → resend
else if dest.resend == never                                         → unknown
else if elapsed + safety_margin(default 5 min) < dest.idempotency_window (default 24h) → resend same key
else → CompleteEffect(Err(NonRetryable{kind:"effect_outcome_unknown",
         message:"in-flight effect older than idempotency window; not re-sent", details: JSON{op, key, elapsed}}))
```
- In-attempt retries follow the same split. "Request never left" errors (DNS failure, connect refused, TLS handshake failure) are always safe to retry. Errors after bytes were written (timeout waiting for the response, reset mid-response) mean the outcome is unknown, so they are retried only with the key and within the window.
- A 409 for a concurrent same-key request (Stripe: "conflicts with another request that's executing concurrently ... You can retry") is retryable with backoff.
- Provider windows are operator configuration on the destination rule; Ostro cannot discover them. Known values:

| Provider | Window | Source / confidence |
|---|---|---|
| Stripe API v1 | keys may be pruned once "at least 24 hours old"; "We generate a new request if a key is reused after the original is pruned" | docs.stripe.com/api/idempotent_requests: **HIGH** |
| Stripe API v2 | 30 days (third-party glossary) | **LOW**, unverified |
| PayPal (`PayPal-Request-Id` header) | per endpoint; "you can retry calls for as long as the server stores the ID" | developer.paypal.com: MEDIUM (no number) |
| Adyen (`Idempotency-Key`) | 7–14 days (third-party) | **LOW** |
| Square (`idempotency_key` **in the body**) | not officially stated | LOW. Body-located keys need SDK help: the guest puts the derived key in the body. |
| Nuvei 15 min, Yoco 24 h, Lead Bank 48 h | | third-party, LOW. Shows the spread; supports a per-destination config. |

Default: `idempotency_window = "24h"` for any destination without a rule (Stripe v1's floor). Document loudly that exactly-once HTTP requires a provider that honours the key.

### SQL exactly-once protocol (FX-04)
```
key = sha256("ostro/sql-effect/v1" ‖ cluster_id ‖ run_id ‖ op_le)   (32 bytes)
conn = pool.acquire()
BEGIN                                   (PG: READ COMMITTED default; MySQL: START TRANSACTION, InnoDB REPEATABLE READ)
INSERT INTO ostro_effects(effect_key, run_id, op) VALUES ($1,$2,$3)     -- token FIRST: concurrent dupes block on PK
   on unique violation (PG 23505 / MySQL 1062) → ROLLBACK; SELECT result FROM ostro_effects WHERE effect_key=$1
       result NULL → Err(NonRetryable sql_exactly_once_violated)   -- an implicit commit split the tx earlier
       else → return stored RecordedResult bytes verbatim (live == re-drive)
<user statement with params>             (sqlx::query only, never raw_sql → protocol-level single statement)
   collect rows up to cap (default 10k rows / 4 MiB) → over cap: ROLLBACK, Err(NonRetryable response_too_large)
UPDATE ostro_effects SET result=$2 WHERE effect_key=$1     (result = encode_result(Ok(OpOutput::Sql(..))))
COMMIT     ← failpoint bridge::sql::after_commit
   COMMIT error / connection lost → outcome unknown, but re-drive resolves it exactly via the token
submit CompleteEffect(result bytes)
```
- **Errors.** PG `40001` serialization_failure and `40P01` deadlock, and MySQL `1213` deadlock and `1205` lock wait timeout, map to `Failure::Retryable` with the same key. Other user-statement errors map to `NonRetryable{kind:"sql_error", details: {sqlstate, message}}`.
- **Token table.** It is created outside any effect transaction when the connection is validated (`CREATE TABLE IF NOT EXISTS`; on MySQL this is DDL with an implicit commit, so never create it inside the tx). It must be in the **same database/schema** as the user tables (configurable `effects_table`). MySQL needs `ENGINE=InnoDB`. With `manage_schema=false`, Ostro prints the DDL instead.
  - PG: `effect_key bytea PRIMARY KEY, run_id text NOT NULL, op bigint NOT NULL, result bytea, created_at timestamptz NOT NULL DEFAULT now()`.
  - MySQL: `effect_key BINARY(32) PRIMARY KEY, run_id VARCHAR(64) NOT NULL, op BIGINT UNSIGNED NOT NULL, result LONGBLOB, created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)`.
- **GC.** Batched `DELETE ... WHERE created_at < now() - retention`, default 7 days, at or above the server run retention of 168h (D9).
- **`exactly-once=false`.** Autocommit execution, at-least-once on re-drive. Document this.

**Statement gate for exactly-once (allow-list, not deny-list).** Strip leading whitespace and `--`, `#` (MySQL) and `/* */` comments. **Reject** statements starting with `/*!` or `/*+`: MySQL executable comments and hints can hide anything. Then the first keyword must be one of `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `REPLACE` (MySQL), `WITH`, `MERGE` (PG 15+), `VALUES`, `TABLE`.
- Reject `SELECT … INTO OUTFILE|DUMPFILE`, which writes outside the transaction.
- `CALL` is allowed only with a connection-level `allow_call = true`, because procedures can contain implicit commits.
- An allow-list survives new server versions; a deny-list does not.
- MySQL stored functions and triggers cannot commit ("Explicit or implicit commit is not allowed in stored function or trigger"), so DML that fires triggers is safe.
- Non-transactional engines (MyISAM, MEMORY) silently break atomicity and cannot be detected from the statement. Document this as a warning sign.

**MySQL 8.4 statements that cause an implicit commit** (MySQL 8.4 Reference Manual §15.3.3, verified). All of these are rejected by the allow-list, and the list is used in tests and docs:
- *DDL:* ALTER EVENT, ALTER FUNCTION, ALTER PROCEDURE, ALTER SERVER, ALTER TABLE, ALTER TABLESPACE, ALTER VIEW, CREATE DATABASE, CREATE EVENT, CREATE FUNCTION, CREATE INDEX, CREATE PROCEDURE, CREATE ROLE, CREATE SERVER, CREATE SPATIAL REFERENCE SYSTEM, CREATE TABLE, CREATE TABLESPACE, CREATE TRIGGER, CREATE VIEW, DROP DATABASE, DROP EVENT, DROP FUNCTION, DROP INDEX, DROP PROCEDURE, DROP ROLE, DROP SERVER, DROP SPATIAL REFERENCE SYSTEM, DROP TABLE, DROP TABLESPACE, DROP TRIGGER, DROP VIEW, INSTALL PLUGIN, RENAME TABLE, TRUNCATE TABLE, UNINSTALL PLUGIN.
  - `CREATE/DROP TEMPORARY TABLE` does not commit but **cannot be rolled back**, so it is rejected too.
  - `ALTER TABLE`/`CREATE INDEX` on temporary tables does commit.
  - `CREATE TABLE … SELECT` commits before **and** after.
- *Implicitly use/modify `mysql` tables:* ALTER USER, CREATE USER, DROP USER, GRANT, RENAME USER, REVOKE, SET PASSWORD.
- *Transaction control and locking:* BEGIN, LOCK TABLES, `SET autocommit = 1` (if not already 1), START TRANSACTION, UNLOCK TABLES (commits only if tables were locked with LOCK TABLES).
  - `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE SAVEPOINT`, `XA …` and `SET TRANSACTION` also end or alter the transaction and are rejected.
- *Data loading:* LOAD DATA (implicit commit only for NDB tables). Rejected anyway: not DML on the allow-list.
- *Administrative:* ANALYZE TABLE, CACHE INDEX, CHECK TABLE, FLUSH, LOAD INDEX INTO CACHE, OPTIMIZE TABLE, REPAIR TABLE, RESET (but not RESET PERSIST).
- *Replication control:* START REPLICA, STOP REPLICA, RESET REPLICA, CHANGE REPLICATION SOURCE TO. Also the 8.0 synonyms START/STOP SLAVE, CHANGE MASTER TO and RESET MASTER, from training data (MEDIUM).
- `PREPARE`/`EXECUTE`/`DEALLOCATE` (dynamic SQL) are rejected: they can hide DDL.
- MariaDB has its own list. Target MySQL 8.0/8.4; MariaDB is best effort.

**PostgreSQL.** PG has no implicit commits.
- Statements that "cannot run inside a transaction block" **fail loudly** inside BEGIN, so the transaction rolls back and nothing is applied. That is safe, but they are rejected up front for a clear error: CREATE/DROP DATABASE, CREATE/DROP TABLESPACE, ALTER SYSTEM, VACUUM, CREATE/DROP INDEX CONCURRENTLY, REINDEX … CONCURRENTLY / SYSTEM / DATABASE, CLUSTER without a table, CREATE/ALTER/DROP SUBSCRIPTION with slot operations, DISCARD ALL. This comes from training data (MEDIUM); the allow-list rejects them anyway.
- **Dangerous in PG** are the transaction-control statements `COMMIT`/`END`/`ROLLBACK`/`ABORT`/`PREPARE TRANSACTION`/`COMMIT PREPARED`. Executed inside the open transaction, they end it. The allow-list rejects them.
- `CALL` of a procedure that commits errors inside an explicit transaction block ("invalid transaction termination"), which is safe.
- dblink/postgres_fdw writes are not atomic with the token: document.

**Value mapping (sqlx dynamic decode by `type_info().name()`):**
- bool → `boolean`
- int2/int4/int8 and MySQL TINYINT…BIGINT → `int8`. MySQL BIGINT UNSIGNED values above i64::MAX → `numeric`.
- float4/8 → `float8`
- numeric/decimal → `numeric(BigDecimal::to_string)`
- text/varchar/char/name/enum → `text`
- bytea/blob/binary → `bytea`
- json/jsonb → `json`
- timestamptz/MySQL TIMESTAMP → `timestamptz` (via `time::OffsetDateTime`, then `{seconds, nanoseconds}`). timestamp-without-tz/DATETIME → `timestamptz`, interpreted as UTC (document).
- uuid → `text`
- date/time/interval/arrays/other → `text` via a server-side cast is impossible after the fact. Return `NonRetryable{kind:"sql_unsupported_type", details: column/type}` and document "cast in SQL (`::text`)".
- Params map the reverse way. `Timestamptz` binds as `OffsetDateTime`.

### Object store (FX-05): the shared `ostro-objstore` crate
**Alignment with Phase 8.** There is one crate, `crates/ostro-objstore`, and **whichever of Phase 6 / Phase 8 runs first creates it**. Phase 8's 08-03 defines the trait: `put`, `put_if_absent`, `put_if_match`, `get`, `get_range`, `head`, `list(prefix, start_after)`, `delete`, multipart ≥ 64 MiB; LocalFs uses tmp + fsync + rename; S3 uses rusty-s3 + reqwest with retries, path- or virtual-host style, region and endpoint. Phase 6 adds only what FX-05 needs on top:
- `async fn presign(&self, method: PresignMethod /* Get | Put */, key: &str, expires: Duration, opts: &PresignOpts /* content_type, response_content_type */) -> Result<Option<Url>>`.
  - S3: `bucket.get_object(Some(&creds), key)` or `put_object(..)`, then `query_mut()` (`response-content-type`) / `headers_mut()` (`content-type`, which the uploader must then send), then `.sign(expires)`. All verified in rusty-s3 0.12.0 source. S3 accepts 1 s–7 d. URLs signed with session tokens (`Credentials::new_with_token`) expire with the token.
  - LocalFs: returns `None`, and the bridge uses the Ostro-served HMAC route below.
- A `PutOpts { content_type: Option<String> }` on `put` (S3: `headers_mut().insert("content-type", ..)`; LocalFs: stored in a sidecar or ignored, documented).
- `ObjectMeta { size, last_modified: jiff::Timestamp, e_tag: Option<String>, version: Option<String> }`. jiff only, no chrono anywhere.

If Phase 6 creates the crate first, it builds the full Phase 8 trait surface minus multipart and conditional puts (stubs returning `Unsupported`), so 08-03 only fills in methods. If Phase 8 created it first, 06-08 adds `presign` + `PutOpts` additively. Either way, document the ownership in both phases' SUMMARYs.

**Effect adapter (`ostro-bridge/src/object.rs`).**
- `Connection{kind: "object", config}` holds JSON `{"v":1, "backend":"s3"|"local", "endpoint", "bucket", "region", "url_style":"path"|"virtual", "allow_http"?, "root"? (local), "prefix"?, "credentials": {"access_key_id": "{{secret:x}}", "secret_access_key": "{{secret:y}}", "session_token"?}, "presign_expiry": "15m", "max_get_bytes": 16777216}`.
  - Credentials are templated and resolved when the backend is built; rebuilt on a TTL (default 5 min).
  - **GCS** works as `backend = "s3"` with `endpoint = "https://storage.googleapis.com"`, `region = "auto"` and GCS HMAC keys, through the GCS XML API's S3 interoperability. This is training data plus a GCS interop claim: **MEDIUM**, so it needs an integration check before docs promise it.
  - **Azure Blob is deferred.** It has no S3 API, so it needs its own backend in `ostro-objstore` later.
- Ops:
  - `get` → bytes, capped;
  - `put` → returns JSON `{"e_tag":…, "version":…}`;
  - `head` → JSON `{size, last_modified (RFC 3339 via jiff), e_tag}`;
  - `delete` → empty;
  - `presign-get`/`presign-put` → URL bytes.
- **Local backend presign:** Ostro serves `GET|PUT /v1/objects/{connection}/{key}?exp=<unix>&sig=<hmac-sha256>` from ostro-api. It uses `hmac 0.13`/`sha2 0.11` (already pinned), with a per-connection key stored as a sealed secret `__ostro/presign/<connection>`. Constant-time compare. The route is unauthenticated, but the signature check is the authentication.
- Object-store endpoints are **operator-configured** (trusted). They use a separate reqwest client (still `.no_proxy()` unless an operator proxy is configured) and are exempt from guest egress allow-lists and the private-range block, since an S3-compatible store such as SeaweedFS on localhost is legitimate. Module access is governed by connection grants.

**Proposed FX-05 wording (REQUIREMENTS.md):**
> - [ ] **FX-05**: Object store connector through the shared `ostro-objstore` crate: S3-compatible backends (AWS S3, SeaweedFS, R2, Ceph; GCS via S3-interoperability HMAC keys) and the local filesystem, with get/put/delete/head/presign. Azure Blob is deferred until a native backend exists [FR-4.3]

ROADMAP Phase 6 success criterion 5 ("S3-compatible and local backends") already matches and needs no change. PRD FR-4.3 ("S3/GCS/Azure/local") should get a footnote saying Azure is deferred. docs/03 §3/§11's `chronik-object-store` / `object_store` references are superseded, as Phase 8 also notes.

### Secrets (FX-06/07/10)
- **Template grammar:** `\{\{secret:([a-z0-9][a-z0-9_.\-/]{0,127})\}\}`, case-sensitive. `__ostro/` is a reserved prefix that guests can never reference. Escaping: not supported. A literal `{{secret:` in a header value is always treated as a placeholder, and unknown names fail.
  - Header values: always.
  - Body: only if every secret referenced in the body has `body = true` in its grant.
  - URL, method, header names: placeholder present → `NonRetryable egress_denied` ("secret placeholders are not allowed in the URL").
- **Resolution point:** in the bridge task, after the pre-send gate and the URL gate, immediately before building the reqwest request. Values are held as `SecretBox<[u8]>`. The expanded header value is built in a `Zeroizing<Vec<u8>>` and converted with `HeaderValue::from_bytes`, then `set_sensitive(true)`, which makes hyper skip HPACK indexing and redact it in Debug. **Honest limit:** hyper/rustls internal buffers and `HeaderValue`'s `Bytes` are not zeroized. Document "zeroized in Ostro-owned buffers".
- **Grants (origin-bound):** `(module, secret) → {origins: ["https://api.stripe.com"], body: false}`. The origin is the RFC 6454 triple (scheme, host, port), compared against the **normalised** request URL. An explicit `https://*.example.com` wildcard is allowed: a single label level, never a bare `*`. "module" is the deployed module name, not the sha, so redeploys keep grants. No grant, or an origin mismatch → `NonRetryable secret_not_granted`, **without** sending.
- **Redirects:** templated request → `Policy::none()`; the 3xx is returned to the guest. Untemplated → `Policy::custom`, max 5 hops. Every hop re-checks the module allow-list **and** IP-literal targets, because the resolver is bypassed for literals. reqwest itself strips only Authorization/Cookie/Cookie2/Proxy-Authorization on cross-host redirects (verified in `src/redirect.rs`), so a templated `X-Api-Key` **would** be forwarded. That is why there are no redirects at all for templated requests.
- **Scrubbing:** build an aho-corasick automaton per request from every resolved value, with patterns for:
  - raw;
  - standard and URL-safe base64, with and without padding, at all **three byte alignments**, using the stable middle portion of each so that a secret embedded in a larger base64 blob is still caught;
  - lowercase and uppercase hex;
  - percent-encoding;
  - JSON string escaping.

  Replace matches with `[REDACTED]` in the response body and all header values **before** encoding the `RecordedResult`. The guest gets journal bytes even on the live path, so live and replay match. Values shorter than 8 bytes are refused for templating at secret creation: too many false positives, and too guessable.
- **Sealed store:** `SealedSecret{name: "<tenant|_>/<name>", ciphertext: [ver=1][nonce 12][ct+tag], key_id: "aes256gcm:<kid>"}`. The AEAD AAD is `b"ostro/secret/v1" ‖ name ‖ key_id`. The 32-byte sealing key comes from, in order: `OSTRO_SEALING_KEY` (base64) env, then `[secrets] sealing_key_file`, then in **dev only** an auto-generated `<data_dir>/sealing.key` (0600, with a warning). Server mode without a key refuses to store secrets. Rotation: a new kid, old kids kept read-only, plus `ostro secret reseal`. Optional: reseal `__ostro/token-key` (`plaintext-v0`) when a sealing key is present.
- **Providers:** a `SecretProvider` trait (`async fn get(&self, name) -> Result<Option<SecretBox<[u8]>>>`) plus an ordered chain from config:
  - `sealed` (meta);
  - `env` (`OSTRO_SECRET_<NAME>`, upper-cased, with `-./` mapped to `_`);
  - `file` (`<dir>/<name>`, read per resolve; covers Docker and K8s mounted secrets);
  - `vault` (KV v2 `kv2::read(client, mount, path)`; key field selected by `name#field`; auth token|approle|kubernetes; TTL cache default 30 s, zeroized on evict);
  - `k8s` (`Api::<Secret>::namespaced(client, ns).get(name)` → `data[key]` raw bytes; in-cluster config or kubeconfig; TTL cache 30 s).
  - Resolve per (run, attempt, name), as in docs/03 §5.3, so Phase 7's `ResolveSecret` slots in.

### SSRF defence (FX-08)
**Layer 1, URL gate (before anything else):**
- Parse with `url::Url`.
- Scheme `https` by default; `http` only if the allow rule says so.
- **Reject userinfo** (`user:pw@`), because the probe shows `http://user:pw@evil.com@127.0.0.1/` parses to host 127.0.0.1.
- Port: allowed per rule (default 443/80).
- Match the allow-list against the **normalised** `host()`.
- `Host::Ipv4`/`Host::Ipv6` literals must pass the classifier **here**, because hyper-util's `HttpConnector` skips the resolver for IP literals (verified in `connect/http.rs`: `SocketAddrs::try_parse(host, port)` before `resolve`). By default IP literals are denied unless a rule lists that exact IP/CIDR.

**Layer 2, resolver (`reqwest::dns::Resolve`):**
- Resolve with hickory and collect every A/AAAA record.
- **Unwrap and classify each one.** If **any** address is blocked, fail the whole name ("rebinding mixed answers").
- Return only vetted `SocketAddr`s. reqwest/hyper connect **only** to the returned addresses, which pins the connection (no second resolution). Pooled connections stay on the vetted IP.

**Layer 3, client:**
- `.no_proxy()`: reqwest reads `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` by default (`auto_sys_proxy = true`, verified in `async_impl/client.rs` and the lib.rs docs).
- `.redirect(..)` per the rules above, `.referer(false)`, no gzip/brotli (decompression-bomb risk and size-cap accounting).
- `.connect_timeout(5s)`, per-attempt `.timeout`, `.https_only(rule)`.
- Streaming body read with a hard byte cap (default 4 MiB, max 64 MiB).

Optional defence in depth: a `connector_layer` that checks `peer_addr()` against the classifier after connect.

**Bypass table (all must be blocked; CI table test).** Probe results are from `url` 2.5.8 on this machine.

| Payload | What it really is | Blocked by |
|---|---|---|
| `http://127.0.0.1/`, `http://localhost/`, `http://localhost./` | loopback | classifier (127/8; localhost resolves to 127.0.0.1/::1) |
| `http://2130706433/` | decimal → `Ipv4(127.0.0.1)` (probe) | URL gate IP-literal check |
| `http://0177.0.0.1/`, `http://0x7f.1/`, `http://127.1/`, `http://0x7f000001/`, `http://%31%32%37.0.0.1/`, `http://127。0。0。1/` | octal/hex/short/percent/ideographic dot → `Ipv4(127.0.0.1)` (probe) | URL gate |
| `http://0/`, `http://0.0.0.0/` | `Ipv4(0.0.0.0)` (probe), which reaches localhost on Linux | classifier 0.0.0.0/8 |
| `http://169.254.169.254/` (AWS/GCP/Azure/OCI/DO IMDS), `169.254.170.2` (ECS), `169.254.170.23` (EKS pod identity) | link-local / metadata | classifier 169.254/16 |
| `http://metadata.google.internal/` | resolves to 169.254.169.254 | resolver |
| `http://100.100.100.200/` (Alibaba IMDS) | CGNAT | classifier 100.64/10 |
| `http://[fd00:ec2::254]/` (AWS IMDS IPv6), `fd00:ec2::23`, `[fd20:ce::254]` (GCP IPv6 metadata) | ULA | classifier fc00::/7 |
| `http://[::ffff:169.254.169.254]/`, `http://[::ffff:a9fe:a9fe]/`, `http://[0:0:0:0:0:ffff:127.0.0.1]/` | IPv4-mapped (probe normalises to `::ffff:a.b.c.d`) | unwrap `to_ipv4_mapped()`, then classify v4 |
| `http://[::127.0.0.1]/` | IPv4-compatible (deprecated ::/96) | unwrap ::/96 (except `::`, `::1`) |
| `http://[64:ff9b::a9fe:a9fe]/`, `[64:ff9b:1::…]` | NAT64 well-known (RFC 6052) / local-use (RFC 8215) | unwrap the last 32 bits for 64:ff9b::/96. **Block 64:ff9b:1::/48 entirely** (embedding is operator-defined). |
| `http://[2002:a9fe:a9fe::]/` | 6to4 embedding 169.254.169.254 | unwrap 2002::/16 bits 16–48 |
| `http://[2001:0::…]/` | Teredo | block 2001::/32 |
| `http://[::]/`, `http://[::1]/`, `fe80::/10`, `fec0::/10`, `ff00::/8` | unspecified/loopback/link-local/site-local/multicast | classifier |
| `http://169.254.169.254.nip.io/`, attacker DNS → private, rebinding TTL 0 | name → private | resolver: classify every answer, reject if any is blocked; pinned connect |
| redirect `302 Location: http://127.0.0.1/` or `http://[::1]/` | redirect to internal | templated: no redirects. Untemplated: custom policy re-checks the allow-list plus the IP-literal classifier (the resolver is not consulted for literals). |
| `HTTPS_PROXY=http://attacker` in the server env | the proxy resolves for us | `.no_proxy()` |
| `http://user:pw@evil.com@127.0.0.1/` | userinfo confusion (probe: host 127.0.0.1) | URL gate rejects userinfo |
| `http://1.2.3.4.5/`, `http://08.1/` | invalid (probe: parse error) | URL gate |
| Full IPv4 block list | 0/8, 10/8, 100.64/10, 127/8, 169.254/16, 172.16/12, 192.0.0/24 (incl. 192.0.0.192 OCI legacy), 192.0.2/24, 192.88.99/24, 192.168/16, 198.18/15, 198.51.100/24, 203.0.113/24, 224/4, 240/4, 255.255.255.255/32 | IANA special-purpose registry |
| Full IPv6 block list | ::/128, ::1/128, ::ffff:0:0/96 (unwrap), ::/96 (unwrap), 64:ff9b::/96 (unwrap), 64:ff9b:1::/48, 100::/64, 2001::/32, 2001:2::/48, 2001:10::/28, 2001:20::/28, 2001:db8::/32, 2002::/16 (unwrap), 3fff::/20, 5f00::/16, fc00::/7, fe80::/10, fec0::/10, ff00::/8 | IANA IPv6 special-purpose registry |

**Operator overrides:**
- `[egress] allow_private = ["10.20.0.0/16"]` (node-level) or a per-rule `allow_private`.
- Tests need `127.0.0.0/8`, because the mock servers listen on loopback. The guard tests run *without* it.

**Per-module allow-lists:** an `EgressPolicy{name: <module>, allow: [rule…]}`. A rule is a string spec `scheme://host[:port]` or `scheme://*.domain[:port]`. Per-destination settings live in `Connection{kind:"http-target", name: <origin>, config: JSON {"v":1, idempotency_window, idempotency_header, resend, rate, burst, max_response_bytes, allow_http, allow_private}}`. A module with no policy has **no egress** (deny by default).

### Rate limits (FX-09)
- governor `RateLimiter::keyed`, keyed by target: origin for HTTP, connection name for SQL/object.
- Quota from the target config (`rate = "100/s"`, `burst`).
- On limit: `until_key_ready_with_jitter`, bounded by `max_wait` (default 30 s, and never beyond the effect timeout). Then fail `Retryable{kind:"rate_limited"}`.
- Per node only. FR-4.5's partition-owned cluster-wide buckets are Phase 7/13; record this as a decision.
- Metrics (`metrics` facade): `ostro_effect_rate_limited_total{target}`.

### Anti-Patterns to Avoid
- **`reqwest::Client::new()` anywhere in the bridge:** env proxies and 10-hop redirects would bypass every guard (PITFALLS #16).
- **URL-string SSRF checks:** they miss rebinding and literal encodings. Classify at the resolver plus the IP-literal gate.
- **Re-sending from the journal (sweeper):** the journal lacks retry and redacted values (F1). Re-drive only in replay.
- **Completing effects with the activation's attempt token:** evicted or fenced activations lose completions. Use `CompleteEffect` (attempt-independent, first wins).
- **Applying scrub/redact only to the journaled copy:** the guest would see different bytes live and on replay. Completions must reach the guest only from the journal.
- **Creating `ostro_effects` inside the effect transaction on MySQL:** DDL commits the transaction implicitly.
- **`raw_sql` / multi-statement strings:** this defeats the single-statement guarantee of the extended or prepared protocol.
- **Adding `object_store` (or anything that pulls chrono/humantime):** use `ostro-objstore`. The deny ban stays absolute.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| URL host normalisation (octal/hex/decimal/IDNA/percent) | regexes over URL strings | `url` 2.5.8 (WHATWG) | Proven by the probe. Every inet_aton form normalises to `Host::Ipv4`. |
| DNS | getaddrinfo via `tokio::net::lookup_host` | hickory-resolver 0.26.3 | No libc quirks, async, all records visible. |
| CIDR math | bit twiddling | `ipnet` + std `to_ipv4_mapped()` | |
| SigV4 signing and presign for S3-compatible stores | your own canonical-request/signing code | rusty-s3 0.12.0 inside `ostro-objstore` | Edge cases: URI encoding, payload hash modes, session tokens, clock handling. |
| PG/MySQL wire protocol, pooling, TLS | — | sqlx 0.9.1 | |
| AEAD | your own crypto | aws-lc-rs `RandomizedNonceKey` (AES_256_GCM) | Already linked. Nonce misuse is impossible by construction. |
| Vault auth and renewal | raw HTTP | vaultrs 0.8.0 | |
| K8s client config and SA token refresh | raw HTTP | kube 5.1.0 (or the file provider) | |
| Rate limiting | token bucket | governor 0.10.4 | |
| Multi-pattern scrub | nested `memmem` loops | aho-corasick | Linear time across all encodings. |

**Key insight:** In this domain the guarantees come from *where* checks happen: resolver vs string, protocol-level single statement vs parsing, journal-only completions vs live paths. They do not come from how clever the checks are. Libraries supply the mechanics; Ostro owns the placement.

## Common Pitfalls

### Pitfall 1: Suspended run with a dead in-flight effect never wakes
**What goes wrong:** the activation evicts (hot wait elapsed) while an HTTP call runs. The node crashes, and nothing re-dispatches the run.
**Why:** docs/03 §5.2 says "no sweeper", but that only holds for runs that are Running at the crash.
**How to avoid:** `effects` rows plus a leader-start scan → EnqueueReady. Add a test: evict mid-effect, kill -9, restart, and check the run completes.
**Warning signs:** runs stuck `Suspended` with open effects rows after restart.

### Pitfall 2: The IP-literal path skips the resolver
**What goes wrong:** the custom resolver is correct, but `http://[::ffff:169.254.169.254]/` connects anyway.
**Why:** hyper-util parses IP literals and never calls `Resolve` (verified).
**How to avoid:** classify `Host::Ipv4/Ipv6` in the URL gate and on every redirect hop.
**Warning signs:** the bypass table passes for names but fails for literals.

### Pitfall 3: Env proxies
**What goes wrong:** the server runs with `HTTPS_PROXY` set, the proxy resolves internal names, and the guard checks nothing. **How to avoid:** `.no_proxy()`. A test sets `HTTPS_PROXY` to a recording listener and asserts zero connections to it.

### Pitfall 4: Live and replay divergence from scrub/redact
**What goes wrong:** scrubbing is applied to the journal copy while the live guest gets raw bytes. **How to avoid:** the completion is encoded once (after scrub/redact) and delivered only as a journaled input. Test: compare what the guest observed live with a replay of the same run.

### Pitfall 5: MySQL caching_sha2_password without TLS
**What goes wrong:** sqlx without `mysql-rsa` cannot complete MySQL 8 full authentication over plaintext. The first connect fails in CI.
**How to avoid:** use `ssl-mode=REQUIRED` (the MySQL 8.4 server auto-generates certs) or a unix socket. Do **not** enable `mysql-rsa`: it pulls the pre-release `rsa 0.10.0-rc`, and the rsa crate has the Marvin advisory history (RUSTSEC-2023-0071, training data, MEDIUM).
**Warning signs:** "authentication requires secure connection".

### Pitfall 6: Implicit commit splits token and statement (MySQL)
**How to avoid:** the allow-list gate plus NULL-result detection on re-drive (`sql_exactly_once_violated`). Tests: an exactly-once `ALTER TABLE` and `CREATE TEMPORARY TABLE` are rejected *before* any statement runs, and `/*!50000 DROP TABLE x */` is rejected.

### Pitfall 7: Zombie sends after the lease is lost
**What goes wrong:** single-node registry dedupe prevents same-node duplicates, but a self-fenced activation's effect task may still send.
**How to avoid:** the pre-send gate checks the effect's cancellation (run terminal, `cancel-op`, partition leadership lost) **immediately before** resolving secrets and sending. The key is the only protection for the external side; document this as the at-least-once boundary (PITFALLS #14).
**Test:** a mock that counts calls per key asserts **one charge**, not just one journaled completion.

### Pitfall 8: Test mocks on loopback vs the guard
**What goes wrong:** every e2e test needs `allow_private = ["127.0.0.0/8"]`, and someone ships that default.
**How to avoid:** the default is empty. Tests set it explicitly. The bypass-table test asserts the default config blocks loopback.

### Pitfall 9: Two object-store layers
**What goes wrong:** Phase 6 and Phase 8 run in parallel and each builds its own S3 client.
**How to avoid:** there is one `ostro-objstore` crate, and the first phase to run creates it with the agreed trait surface (§Object store). The other phase extends it additively. A STATE.md decision records the owner.

### Pitfall 10: Large request bodies or responses bloat the journal
**What goes wrong:** `EffectScheduled` journals the request body, and responses up to the cap go into `EffectCompleted`.
**How to avoid:** offload payloads above 256 KiB to blobs using the 04-15 mechanism, and verify it also applies to `CompleteEffect` outcomes and `EffectScheduled`. Caps are documented.

### Pitfall 11: Body-located idempotency keys (Square)
**What goes wrong:** the provider ignores headers, so re-drive duplicates. **How to avoid:** the SDK exposes `ctx.idempotency_key(op)`, so the guest can embed the derived key in the body. Destination config sets `idempotency_header = ""` to suppress the header.

## Code Examples

### Egress resolver (reqwest 0.13 + hickory 0.26)
```rust
// Source: reqwest 0.13.5 ClientBuilder::dns_resolver (docs.rs); hyper-util connect/http.rs (IP-literal bypass).
// API shapes for reqwest::dns::{Resolve, Resolving, Name, Addrs} and hickory's builder: MEDIUM, verify at implementation.
pub struct EgressResolver { inner: hickory_resolver::TokioResolver, policy: Arc<ClassifyPolicy> }
impl reqwest::dns::Resolve for EgressResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let (r, p) = (self.inner.clone(), self.policy.clone());
        Box::pin(async move {
            let ips: Vec<IpAddr> = r.lookup_ip(name.as_str()).await?.iter().collect();
            if ips.is_empty() { return Err("no addresses".into()); }
            if let Some(bad) = ips.iter().find(|ip| p.blocked(**ip)) {    // ANY blocked → reject all
                return Err(format!("egress_denied: {} resolves to blocked {bad}", name.as_str()).into());
            }
            let addrs: reqwest::dns::Addrs = Box::new(ips.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addrs)   // hyper-util substitutes the URL port when the resolved port is 0
        })
    }
}
let client = reqwest::Client::builder()
    .dns_resolver(Arc::new(EgressResolver { .. }))
    .no_proxy()                                   // env proxies are on by default
    .redirect(reqwest::redirect::Policy::none())  // templated client; a 2nd client uses Policy::custom
    .referer(false)
    .connect_timeout(Duration::from_secs(5))
    .build()?;
```

### Classifier unwrap
```rust
fn effective_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = ip.to_ipv4_mapped() { return Some(v4); }                 // ::ffff:0:0/96
    let s = ip.segments();
    if s[..6] == [0; 6] && !(s[6] == 0 && s[7] <= 1) { return Some(v4_from(s[6], s[7])); } // ::/96 compat
    if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] { return Some(v4_from(s[6], s[7])); }          // 64:ff9b::/96
    if s[0] == 0x2002 { return Some(v4_from(s[1], s[2])); }                                // 6to4
    None
}
// blocked(ip) = v4 table hit || v6 table hit || effective_v4(..).is_some_and(v4 table hit)
```

### Exactly-once SQL (Postgres flavour; MySQL differs only in the SQL text and error codes)
```rust
// Source: protocol in this document; sqlx 0.9 Transaction API (verify names at implementation).
let mut tx = pool.begin().await?;
match sqlx::query("INSERT INTO ostro_effects(effect_key, run_id, op) VALUES ($1,$2,$3)")
        .bind(&key[..]).bind(run_id).bind(op as i64).execute(&mut *tx).await {
    Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => {
        tx.rollback().await?;
        return stored_result(&pool, &key).await;  // NULL → sql_exactly_once_violated
    }
    r => { r?; }
}
fail::fail_point!("bridge::sql::before_user_stmt");
let out = run_user_statement(&mut tx, &req).await?;           // sqlx::query + bound params, capped
let bytes = encode_result(&Ok::<_, Failure>(OpOutput::Sql(out)))?;
sqlx::query("UPDATE ostro_effects SET result=$2 WHERE effect_key=$1")
    .bind(&key[..]).bind(&bytes.bytes).execute(&mut *tx).await?;
tx.commit().await?;
fail::fail_point!("bridge::sql::after_commit");                // SC2 crash point
complete(op, bytes)
```

### Window decision
```rust
let elapsed = now_nanos.saturating_sub(scheduled_ts_nanos);
let resend = method_is_idempotent(&req.method) && !dest.force_window
    || (dest.resend != Resend::Never
        && elapsed.saturating_add(dest.safety_margin_nanos) < dest.window_nanos);
if !resend { return Outcome::unknown(op, key, elapsed); }   // NonRetryable effect_outcome_unknown
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| chrono for time types | jiff 1.0 / time 0.3 | RUSTSEC-2026-0543 (2026-09-14); humantime RUSTSEC-2026-0542 | object_store still depends on chrono (issue #909, 2026-11-17), so Ostro uses rusty-s3 0.12 (jiff 1.0) in `ostro-objstore`. |
| sqlx 0.8 | sqlx 0.9 (0.9.0 May 2026, 0.9.1 Nov 2026) | 2026 | Feature names: `tls-rustls-aws-lc-rs[-native-roots]`, `runtime-tokio`. No jiff feature, so use `time`. |
| reqwest 0.12 (native-tls default) | reqwest 0.13 (rustls + aws-lc-rs default, `retry` builder) | 2025/26 | Env proxies are still on by default. |
| object_store as the default Rust S3 client | rusty-s3 (sans-IO) + own transport for chrono-free stacks | 2026-11 | Re-evaluate object_store after #909 ships. |

**Deprecated/outdated:** STACK.md's `sqlx` line (feature `tls-rustls-aws-lc-rs` plus `macros`) and its `object_store` line (blocked by the chrono ban). Both are superseded by this document.

## Open Questions / Decisions to Confirm

1. **D-FX1: object store = shared `ostro-objstore` (rusty-s3 0.12.0 + local fs), aligned with Phase 8.** No deny exception. GCS goes through S3 interop (MEDIUM, to verify). Azure is deferred. The FX-05 wording update is proposed in §Object store. **USER REVIEW** only for the FX-05 text change and the Azure deferral.
2. **D-FX2: SQL TLS roots.** Recommendation: native roots plus a per-connection `ca_file`. Alternative: allow `CDLA-Permissive-2.0` (webpki-roots) in deny.toml.
3. **D-FX3: default idempotency window = 24h** for unconfigured destinations, and resend for idempotent methods regardless of window.
4. **D-FX4: body templating** is opt-in per grant (`body = true`). The URL is never templated (docs/03 §9.1).
5. **D-FX5: MySQL locally.** There is no mysqld and no Docker in WSL. MySQL tests are env-gated (`OSTRO_TEST_MYSQL_URL`), run in GitHub Actions service containers, and are **human_needed** locally unless the user installs mysql-server 8.4 or enables Docker Desktop WSL integration. Postgres runs locally (PG 14 at 127.0.0.1:5432).
6. **D-FX6: OTLP canary.** OTLP export lands in Phase 8 (OBS-01). Phase 6's canary harness scans tracing output through a capturing subscriber layer at TRACE, which is everything OTLP would export, and exposes a sink-registration hook. Phase 8 adds the OTLP mock collector to the same harness. SC3's "OTLP" wording is then satisfied at the Phase 8 gate. **USER REVIEW** (scope note).
7. **D-FX7: rate limits are per node** in Phase 6. Cluster-wide buckets come in Phase 7/13.
8. **D-FX8: presign expiry** comes from the connection config (`presign_expiry`, default 15m). No `ostro:runtime@0.1.1` in Phase 6, which keeps the Phase 9 SDKs on 0.1.0. A 0.1.1 `object-presign` function with expiry and content-type can follow when needed.
9. **D-FX9: admin surface.** Phase 6 ships REST, gRPC and CLI for secrets (set/list/delete; values are never returned), grants, connections and egress policies. There is no UI (Phase 11).
10. Exact reqwest 0.13 `dns_resolver(impl IntoResolve)` and hickory 0.26 builder names: verify in the first bridge plan (MEDIUM).

## Proposed Plan Breakdown

| Plan | Wave | Depends on | Scope | Requirements |
|------|------|------------|-------|--------------|
| **06-01** GATE + contracts | 1 | Phase 5 complete (05-19 freeze verified) | Verify the freeze, record D-FX1..9 in STATE.md, add all Phase 6 workspace pins (no deny.toml change), create the `ostro-objstore` skeleton with the agreed trait if Phase 8 has not already, and add the additive contract, using the numbers from the reservation table (§Ordinal Reservations): `Cmd::CompleteEffect`, `Intent::RegisterEffect` / `CancelEffect`, `InvalidCode::BadEffectCompletion`, the `EffectRow` value for keyspace #12, and golden lines appended (never re-blessed). Add `ostro-bridge` to the failpoints feature chain. The only wave-1 plan touching Cargo.toml or Cargo.lock. | infra for all |
| **06-02** Partition apply for effects | 2 | 06-01 | `apply/effects.rs`: RegisterEffect/CancelEffect/CompleteEffect (first wins, Duplicate, RunTerminal), terminal cleanup hook, wake (InputAppended/EnqueueReady), leader-start scan API, `gen_effects.rs` for the rebuild_digest proptest, read-budget notes in storage-format.md | FX-01, FX-03 |
| **06-03** Egress core | 2 | 06-01 | url gate, classifier tables, EgressResolver, client builders, streaming cap, allow-list matcher, bypass-table unit tests (injectable lookup for DNS cases) | FX-08 |
| **06-04** Secrets core | 2 | 06-01 | SecretValue, template parser, grants model plus origin check, scrubber with encodings, sealed store (aws-lc-rs), env/file providers, provider chain, `[secrets]` config | FX-06, FX-07, FX-10 (part) |
| **06-05** Engine seam | 2 | 06-01 | `EffectDriver` trait, `Activate.effects`, pending LiveEffect, start after flush, replay re-drive for uncompleted ops, redacted journal copy, `ActivationSink::{resolve_secret, complete_effect}` default impls; engine tests with a fake driver (live drive after commit only, re-drive on replay, no drive when a completion exists) | FX-01, FX-03, FX-06 |
| **06-06** HTTP connector + rate limits | 3 | 06-03, 06-04, 06-05 | key derivation, window decision, retry classification, templating at egress, redirect rules, scrub/redact, outcome encoding, governor per target, http-target config; testkit mock provider (dedupe by key, echo, redirect, slow, oversize) | FX-01, 02, 03, 07, 09 |
| **06-07** SQL connector | 3 | 06-01, 06-04 (connection secrets) | sqlx pools per connection, value mapping, statement allow-list, exactly-once protocol, error mapping, ostro_effects DDL + GC, failpoints `bridge::sql::*`; env-gated PG/MySQL integration tests | FX-04 |
| **06-08** Object connector | 3 | 06-01, 06-04 (and 08-03 if Phase 8 already owns `ostro-objstore`) | `ostro-objstore`: presign + PutOpts (and the base LocalFs/S3 methods if Phase 6 is first); bridge adapter (ops, JSON encodings, caps), local HMAC presign route in ostro-api; SeaweedFS integration test | FX-05 |
| **06-09** Vault + K8s providers | 3 | 06-04 | vaultrs KV v2 + auth modes, kube Secret reads, TTL caches with zeroize; mock Vault/K8s HTTP servers (axum) for tests | FX-10 |
| **06-10** Server seam | 4 | 06-02, 06-05, 06-06, 06-07, 06-08 | EffectRegistry, bridge as EffectDriver, CompleteEffect submission, pre-send gate (cancel/terminal/leadership), leader-start wake scan, removal of the 04-15 "unsupported" path for effects (regression: children still unsupported), `[egress]`/`[connectors]` config | FX-01..05 |
| **06-11** Admin API + CLI | 4 | 06-04, 06-09, 06-01 | REST/gRPC/OpenAPI for secrets/grants/connections/egress over meta (values write-only), CLI `ostro secret|grant|connection|egress`, FakeControlPlane parity | FX-06, 07, 08, 10 |
| **06-12** Rust SDK + test guests | 4 | 06-05 (WIT unchanged) | `ctx.http()` builder, `ctx.sql(conn).exactly_once()`, `ctx.object(conn)`, `ctx.idempotency_key(op)`; guests `fx-http`, `fx-sql`, `fx-object`, `fx-canary`, `fx-ssrf` | all (test enablers) |
| **06-13** E2E HTTP + SSRF + rate | 5 | 06-10, 06-11, 06-12 | SC1 (intent-before-I/O via `bridge::http::before_send` pause, same key on re-drive, window → unknown, idempotent-method resend), SC4 bypass table through a guest, redirects, env proxy, allow-lists, rate limits | FX-01/02/03/08/09 |
| **06-14** E2E SQL crash (PG + MySQL) + CI services | 5 | 06-10, 06-12 | failpoint binary: kill -9 at `bridge::sql::after_commit` → restart → exactly one row change per run, on both databases; ci.yml `services:` postgres:17 + mysql:8.4 and env wiring; ci-local env gating with loud SKIP | FX-04 |
| **06-15** E2E canary + object + providers | 5 | 06-10, 06-11, 06-12 | SC3 canary harness (disk, journal raw and decoded, logs, tracing capture, object-store dirs, blob store, guest-observed bytes, API responses), grant refusal, no-redirect, echo scrub; SC5 object get/put/presign on SeaweedFS (CI service) and local; providers env/file/sealed/Vault/K8s | FX-05/06/07/10 |
| **06-16** Phase gate | 6 | 06-13, 06-14, 06-15 | docs (`docs/connectors.md`, `docs/security-egress.md`, docs/03 corrections incl. the "no sweeper" fix), new replay-corpus entries for effect runs (never re-bless), STATE updates, full `scripts/ci-local.sh`, human_needed list (remote CI MySQL/SeaweedFS legs) | all |

Parallelism: wave 2 has 4 plans on disjoint crates (partition / bridge-egress / bridge-secrets / engine). Wave 3 has 4 plans in disjoint bridge modules. Wave 4 has 3 plans (server, api+cli, sdk). Wave 5 has 3 e2e plans with disjoint test files. Only 06-01 edits Cargo.toml, Cargo.lock and deny.toml. Only 06-10 and 06-14 edit ci-local.sh/ci.yml (06-10: FAILPOINT_CRATES; 06-14: services). Serialise them via the dependency.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | `cargo test` (libtest) + proptest 1.12 + insta 1 + `fail` 0.5.1 failpoints; cargo-nextest available locally |
| Config file | none beyond the workspace; failpoint tests use `required-features = ["failpoints"]` |
| Quick run command | `cargo test -p ostro-bridge --locked` (unit, < 30 s, no DB) |
| Full suite command | `bash scripts/ci-local.sh` (DB legs run when `OSTRO_TEST_PG_URL` / `OSTRO_TEST_MYSQL_URL` / `OSTRO_TEST_S3_URL` are set; otherwise a loud SKIP) |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FX-01 | No I/O before the `EffectScheduled` commit (driver called only after flush; `before_send` pause shows the journal already holds EffectScheduled) | unit (engine fake driver) + e2e failpoint | `cargo test -p ostro-engine --test effects_driver --locked`; `cargo test -p ostro-server --features failpoints --test fx_http_e2e --locked -- --test-threads=1` | ❌ Wave 0 |
| FX-01 | Timeout per attempt, size cap, redirect policy, retry classification | integration (mock provider) | `cargo test -p ostro-bridge --test http_connector --locked` | ❌ |
| FX-02 | Derived key stable across attempts and resume; user override verbatim; key format/length | unit + e2e | `cargo test -p ostro-bridge http::key --locked`; `fx_http_e2e::same_key_after_crash` | ❌ |
| FX-03 | Re-drive with the same key after kill -9; past the window → `effect_outcome_unknown` with exactly 1 request seen; idempotent methods resend | e2e (failpoint binary, short window config) | `cargo test -p ostro --features failpoints --test fx_redrive --locked -- --test-threads=1` | ❌ |
| FX-03 | Evicted mid-effect + crash → leader scan wakes the run | e2e | `fx_redrive::suspended_inflight_wakes` | ❌ |
| FX-04 | Crash between COMMIT and effect_completed → exactly 1 row change per run (PG and MySQL, N≥50 runs) | e2e failpoint + kill -9 | `OSTRO_TEST_PG_URL=… OSTRO_TEST_MYSQL_URL=… cargo test -p ostro --features failpoints --test fx_sql_crash --locked -- --test-threads=1` | ❌ |
| FX-04 | Statement allow-list; MySQL implicit-commit table rejected; `/*!` rejected; NULL-result detection; serialization/deadlock → retryable | unit + DB integration | `cargo test -p ostro-bridge sql::classify --locked`; `cargo test -p ostro-bridge --test sql_pg --test sql_mysql --locked` | ❌ |
| FX-05 | get/put/head/delete/presign on local + S3-compatible (SeaweedFS); presigned GET/PUT URL usable by a plain HTTP client and rejected after expiry; local HMAC presign route | integration | `cargo test -p ostro-objstore --test presign --locked`; `cargo test -p ostro-bridge --test object_local --locked`; `OSTRO_TEST_S3_URL=… cargo test -p ostro-bridge --test object_s3 --locked`; `cargo test -p ostro-api --test presign --locked` | ❌ |
| FX-06 | Canary never on disk/journal/logs/tracing/object store/guest/API; secret never readable by the guest | e2e harness | `cargo test -p ostro-server --test fx_canary --locked` | ❌ |
| FX-06 | Template parser; URL placeholder rejected; zeroize on drop (Ostro buffers) | unit | `cargo test -p ostro-bridge secrets:: --locked` | ❌ |
| FX-07 | Grant for another origin refused (no request sent); templated request not redirected (3xx returned); echoed value scrubbed (raw/base64 at 3 alignments/hex/pct/JSON) | unit + e2e | `cargo test -p ostro-bridge secrets::scrub --locked`; `fx_canary::grant_origin_refused`, `::no_redirect`, `::echo_scrubbed` | ❌ |
| FX-08 | Full bypass table blocked (literals, mapped, NAT64, 6to4, Teredo, metadata v4/v6, rebinding mixed answers, redirect to literal, env proxy, userinfo); per-module allow-list deny-by-default | unit table + e2e via guest | `cargo test -p ostro-bridge --test ssrf_table --locked`; `cargo test -p ostro-server --test fx_ssrf_e2e --locked` | ❌ |
| FX-09 | Per-target limit enforced (N requests in window ≤ quota + burst; other target unaffected); `rate_limited` after max_wait | integration | `cargo test -p ostro-bridge --test ratelimit --locked` | ❌ |
| FX-10 | Sealed store round trip + wrong-key failure + AAD binding; env/file; Vault KV v2 (mock and dev server in CI); K8s Secret (mock API) | unit + integration | `cargo test -p ostro-bridge secrets::sealed --locked`; `cargo test -p ostro-bridge --test providers --locked` | ❌ |
| deny | Stock deny.toml still passes with the Phase 6 stack; no chrono in the graph | gate | `cargo deny check && ! cargo tree --workspace -e normal --locked \| grep -q 'chrono v'` | ✅ (existing deny step) |

### Sampling Rate
- **Per task commit:** `cargo test -p <crate touched> --locked` plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`.
- **Per wave merge:** `cargo test --workspace --locked` with the PG env var set locally (PG 14 is available).
- **Phase gate:** full `bash scripts/ci-local.sh` green. The MySQL/SeaweedFS legs are green in CI or recorded as human_needed.

### Wave 0 Gaps
- [ ] `crates/ostro-testkit/src/mock_provider.rs`: axum mock (key-dedupe payment endpoint with per-key call counter, echo, redirect-to-target, slow, oversize, 409-in-progress)
- [ ] `crates/ostro-testkit/src/canary.rs`: canary generator plus a scanner over a directory tree, byte buffers, and the captured tracing layer (all encodings)
- [ ] `crates/ostro-testkit/src/db.rs`: env-gated PG/MySQL helpers that create a per-test schema/database (`t_<uuid>`) and print a loud SKIP when unset
- [ ] `crates/ostro-bridge/tests/ssrf_table.rs` with an injectable lookup (fake DNS answers for rebinding/nip.io cases; no internet in CI)
- [ ] `.github/workflows/ci.yml` `services:` for postgres:17, mysql:8.4 (TLS on by default) and SeaweedFS (`docker run`), plus env exports. A hashicorp/vault dev container is optional (the mock covers the default suite).
- [ ] Failpoint sites: `bridge::http::before_send`, `bridge::http::after_response`, `bridge::sql::before_user_stmt`, `bridge::sql::after_commit`, `bridge::effect::before_complete_submit`
- [ ] Guests in `guests/` (fx-http, fx-sql, fx-object, fx-canary, fx-ssrf), built by the existing guest build step

## Sources

### Primary (HIGH confidence)
- Workspace probe (this research): resolution against `Cargo.lock` plus `cargo deny check bans licenses advisories` with the project's deny.toml. Results: sqlx (aws-lc-rs → webpki-roots licence fail; native-roots → ok), direct drivers (licence fail), object_store (chrono banned + 2 unmaintained), object_store with a scoped exception (ok, rejected anyway), and the final full stack with rusty-s3 under the **stock** deny.toml (ok, no chrono, single hyper/http/tonic/prost/axum/reqwest/rustls/wasmtime).
- `url` 2.5.8 parse probe on this machine (bypass literal normalisation).
- crates.io API (versions, features, dependency lists), 2026-11-19: sqlx/sqlx-core/-postgres/-mysql 0.9.1, object_store 0.14.2, hickory-resolver 0.26.3, vaultrs 0.8.0, kube-client/kube-core 5.1.0, k8s-openapi 0.29.0, mysql_async 0.37.1, tokio-postgres 0.7.18, testcontainers 0.28.0 / -modules 0.15.0, bollard 0.21, rusty-s3 0.12.0, governor 0.10.4, secrecy 0.10.3, zeroize 1.9.2, chacha20poly1305 0.11.0
- OSV: [RUSTSEC-2026-0543](https://rustsec.org/advisories/RUSTSEC-2026-0543.html) (chrono unmaintained, published 2026-09-14), RUSTSEC-2026-0542 (humantime)
- [apache/arrow-rs-object-store#909](https://github.com/apache/arrow-rs-object-store/issues/909) (chrono in public API), #908 (humantime → jiff), and the main-branch `Cargo.toml` (chrono non-optional)
- [docs.rs object_store 0.14.2 `Signer`](https://docs.rs/object_store/0.14.2/object_store/signer/trait.Signer.html) (considered, rejected)
- rusty-s3 0.12.0 source (`~/.cargo/registry`): `actions/mod.rs` `S3Action::{sign, query_mut, headers_mut}`, `bucket.rs` `{get,put,head,delete}_object`, `credentials` `new_with_token`, `Cargo.toml` licence BSD-2-Clause
- `.planning/phases/08-triggers-bottomless-observability/08-RESEARCH.md` (commit e757400): `ostro-objstore` design, F2–F4 reservation findings
- [docs.rs reqwest 0.13.5 ClientBuilder](https://docs.rs/reqwest/0.13.5/reqwest/struct.ClientBuilder.html); reqwest v0.13.5 source (`lib.rs` proxy env docs, `async_impl/client.rs` `auto_sys_proxy`, `redirect.rs` `remove_sensitive_headers`, Cargo.toml tower-http 0.6)
- hyper-util master `src/client/legacy/connect/http.rs` (`SocketAddrs::try_parse` skips the resolver for IP literals)
- [MySQL 8.4 Reference Manual §15.3.3 Statements That Cause an Implicit Commit](https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html)
- [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests) (24 h pruning, 255 chars, concurrent conflict retryable)
- Project files: runtime.wit, ostro-types journal.rs, ostro-engine host/effects.rs + sink.rs, ostro-meta records.rs, docs/03 §2.3/§4.3/§5.2/§7.3/§9, docs/02 FR-4, storage-format.md §6, 04-15/04-16 plans, 05-DECISIONS, 05-RESEARCH, 05-05/05-17 plans, .planning/research/PITFALLS.md #5/#6/#14/#15/#16, STACK.md

### Secondary (MEDIUM confidence)
- [PayPal idempotency](https://developer.paypal.com/api/rest/reference/idempotency/) (per-endpoint retention, no number)
- [Adyen API idempotency](https://docs.adyen.com/development-resources/api-idempotency) (POST only; window not found in excerpts)
- PG "cannot run inside a transaction block" list; MySQL 8.0 replication synonyms (training data, consistent with docs)

### Tertiary (LOW confidence)
- Stripe v2 30-day / PayPal 45-day / Adyen 7–14-day windows ([spark.money glossary](https://www.spark.money/glossary/idempotency-key), [oneuptime blog](https://oneuptime.com/blog/post/2026-09-13-idempotency-key-retention-ttl-replay/view)); Square 45-char key limit; Nuvei/Yoco/Lead Bank windows ([Yoco](https://developer.yoco.com/online/api-reference/idempotency), [Lead Bank](https://docs.lead.bank/api-reference/using-the-api/idempotency), [Nuvei](https://docs.nuvei.com/?p=579191))

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH. Every recommended crate was resolved against the real workspace and checked with cargo-deny; the two blocking issues were found empirically.
- Architecture: HIGH for the seams, which come from the code and the Phase 4/5 plans. MEDIUM for exact trait and API names in reqwest 0.13 / hickory 0.26 / sqlx 0.9.
- SSRF: HIGH. Source-verified resolver bypass and proxy defaults, plus the probe-verified URL normalisation.
- SQL: HIGH for the MySQL list (official 8.4 docs) and the protocol. MEDIUM for the PG list.
- Provider windows: Stripe HIGH, others LOW. Hence the per-destination config.

**Research date:** 2026-11-19
**Valid until:** 2026-12-19. Re-check sqlx 0.9.x, rusty-s3 0.12.x, object_store #909 (possible future swap), and the Wasmtime 51 bump (D17) before execution.
