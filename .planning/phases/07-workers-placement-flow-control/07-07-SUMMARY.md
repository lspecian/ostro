---
phase: 07-workers-placement-flow-control
plan: 07
subsystem: worker runtime (ostro-worker), worker protocol, test guests
tags: [worker, mtls, join, ca-pinning, session, self-fence, drain, renewal, remote-sink, paging, framing, effects, aot-cache, guests]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-02 ostro-worker-proto (Execute service, WireOutcome, PROTO_VERSION) and the ostro-worker skeleton (WorkerConfig contract, failpoints feature)"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "EffectDriver seam (06-06), LiveEffect / EffectRetry (06-02), redaction rule ([redacted] header values)"
  - phase: 03-wasm-engine
    provides: "Runtime::prepare_cached over the HMAC-authenticated L1 AOT cache, Runtime::activate, ActivationSink"
  - phase: 08-triggers-bottomless-observability
    provides: "ostro_engine::telemetry::with_traceparent, ActivationSink::guest_log"
provides:
  - "ostro-worker library: join with CA-hash pinning, mTLS session (Hello/Welcome, credits, heartbeats), reconnect + re-join (B5), self-fence, drain, renewal"
  - "Activation runner, RemoteSink (Append framing), verified journal/blob fetch, EffectProxy (D-W3 bridge-back)"
  - "Worker / WorkerEvent observability API for e2e and chaos plans"
  - "worker:: failpoints (before_append, before_done, before_start_effect, after_start_effect) via worker_fail!"
  - "execute.proto: AppendFrame.kind = 9"
  - "Fake Execute server + fake join endpoint (tests/common) reusable by later worker tests"
  - "Guests long-journal, pay-flow, cpu-burn (wasm32-wasip2)"
affects: [07-09, 07-12, 07-14, 07-15, 07-16, 07-17, 07-18, 07-19, 07-20, 07-21, 07-22]
tech-stack:
  added: []
  patterns:
    - "bootstrap rustls ServerCertVerifier pinned to sha256(DER) of a chain certificate, delegating to webpki with that certificate as the only root"
    - "session link cell: activations always talk through the current session, so a renewal moves them to the new channel without cancelling them"
    - "Input = wake-up only; ordered bridge fetches [next, up_to) and forwards only input events (04-16 rule)"
key-files:
  created:
    - crates/ostro-worker/tests/common/mod.rs
    - crates/ostro-worker/tests/common/fake_server.rs
    - crates/ostro-worker/tests/protocol.rs
    - crates/ostro-worker/tests/module_cache.rs
    - guests/long-journal/Cargo.toml
    - guests/long-journal/src/lib.rs
    - guests/pay-flow/Cargo.toml
    - guests/pay-flow/src/lib.rs
    - guests/cpu-burn/Cargo.toml
    - guests/cpu-burn/src/lib.rs
  modified:
    - crates/ostro-worker/src/lib.rs
    - crates/ostro-worker/src/config.rs
    - crates/ostro-worker/src/join.rs
    - crates/ostro-worker/src/session.rs
    - crates/ostro-worker/src/drain.rs
    - crates/ostro-worker/src/runner.rs
    - crates/ostro-worker/src/remote_sink.rs
    - crates/ostro-worker/src/journal_fetch.rs
    - crates/ostro-worker/src/blob_fetch.rs
    - crates/ostro-worker/src/effect_proxy.rs
    - crates/ostro-worker/tests/failpoints.rs
    - crates/ostro-worker-proto/proto/ostro/worker/v1/execute.proto
key-decisions:
  - "AppendFrame gains `uint32 kind = 9` (EventKind code on every chunk); an appended event's seq is from_seq + event_index and its bytes are the concatenated chunks = encode_event output (server decodes with decode_event(seq, kind, bytes))"
  - "WorkerConfig gains `endpoints: Vec<String>` (OSTRO_WORKER_ENDPOINTS, comma list), required with the external-certificate triple (no join response to learn them from)"
  - "A GoAway before Welcome whose reason contains \"version\" is fatal (WorkerError::VersionMismatch, no retry); any other GoAway or stream end is a stream loss (self-fence + reconnect)"
  - "Module/journal fetch or verification failure -> Done{Evicted{External}} (server re-activates); invalid component -> Done{AttemptFailed(ProtocolViolation)} (server fails the run, as embedded does)"
  - "AppendResult.paused and PERMISSION_DENIED/FAILED_PRECONDITION map to AppendError::Fenced; rejected and transport errors to Unavailable"
  - "Renewal keeps in-flight activations: they move to the new session (its first heartbeat lists their leases), then Drain is sent on the old session and it is closed; 07-09 must hand leases of a renewing worker to its new session instead of revoking them on that Drain"
  - "Await tokens minted on a worker use a process-local key (Welcome carries no token key): open item for 07-09/07-12"
requirements-completed: []
duration: ~2h40m
completed: 2026-11-21
---

# Phase 7 Plan 07: Worker runtime library Summary

**The stateless worker now works end to end against an in-process fake Execute server. It joins with a CA-hash-pinned bootstrap verifier and holds an mTLS session with absolute credits and lease heartbeats. It runs real guests from verified, fetched modules and paged journals, and frames large appends within `max_frame_bytes`. Effects are bridged back to the leader without ever resolving a secret. On stream loss it self-fences within 200 ms, re-joins after repeated endpoint failures, renews onto a new channel without dropping a lease, and drains cleanly.**

## Commits

1. `67b4a46` feat(07-07): worker join, mTLS session, credits, heartbeats, self-fence, drain
2. `4802f84` feat(07-07): worker runner, RemoteSink, verified fetch, effect proxy, AOT cache, Phase 7 guests

## Join HTTP contract (as implemented; 07-09/07-14 serve it)

`POST <join>/v1/workers/join`, `content-type: application/json`:

```json
{"token": "wkr_<id>.<secret>", "csr_pem": "-----BEGIN CERTIFICATE REQUEST-----...", "worker_id": "w1", "labels": {"zone": "a"}}
```

Success (2xx):

```json
{"client_cert_pem": "...", "ca_cert_pem": "...", "endpoints": ["host:7701"], "renew_after_ms": 57600000, "cluster_id": "c1"}
```

- `renew_after_ms` is optional: `0` or absent means the worker renews at 2/3 of the issued certificate's validity, which it reads from the certificate with x509-parser and so needs no clock read. `cluster_id` is optional. `endpoints` must not be empty.
- On an error status the worker reports `JoinError::Refused{status, message}`, where `message` is `error.message` from the ostro-api error body (falling back to `message`, then the raw body), truncated to 512 characters. The token is scrubbed from every error.
- The CSR is PKCS#10 PEM for an ECDSA P-256 key generated in memory, with CN = worker_id. The key exists only as PEM in memory, and `Identity`'s `Debug` prints it as `<redacted>`.
- `Renew{csr}` takes the same PEM CSR as bytes. `RenewResponse.client_cert` may be PEM or DER; DER is converted to PEM.

## Bootstrap verifier design (D-W20)

- `PinnedCaVerifier` implements `rustls::client::danger::ServerCertVerifier`. It looks through `[end_entity] + intermediates` for a certificate whose `sha256(DER)` equals `--ca-hash`. If none matches, the handshake fails ("no certificate in the server chain matches --ca-hash"), so no request byte is sent and the token never leaves the worker. The test checks that the fake join endpoint recorded no request.
- If one matches, a `WebPkiServerVerifier` is built with that certificate as the only root (aws-lc-rs provider) and verifies the leaf: chain signatures, validity and server name. This works because the self-signed client listener presents `[leaf, cluster CA]` (D-W20).
- Without `--ca-hash`, reqwest's default rustls backend uses the platform verifier, for `files`/`acme` listeners.
- With `--ca-hash`, `sha256(DER(ca_cert_pem))` from the response must also equal the pin (`JoinError::CaHashMismatch`, tested with a response that carries a different CA).
- `http://` is accepted only for `localhost`, `127.0.0.0/8` and `::1`, and a warning is logged. A non-loopback `http://` URL is refused before any network I/O.

## execute.proto additions

| Message | Field | Number | Meaning |
|---|---|---|---|
| `AppendFrame` | `uint32 kind` | 9 | EventKind code of the event the chunk belongs to (set on every chunk) |

Append framing: `chunk` is at most `max_frame_bytes - 256` bytes (`FRAME_RESERVE` covers the session id, run, token and index fields). `event_index` is the event's position in the batch. `event_end` marks an event's last chunk, and `last` marks the batch's last frame. An event's seq is `from_seq + event_index`.

## Engine APIs used

- `Runtime::new(EngineConfig, RuntimeOptions{clock: SystemClock (same as the server's default), token_key, cache_dir: Some(--cache-dir), instance_pre_capacity: 256})`. It is built at the first `Welcome` from `EngineLimits` and rebuilt only when the limits change.
- `EngineLimits` maps to `EngineConfig`: hot_wait, activation_timeout, activation_fuel, continue_as_new_events, pool.max_memory_bytes, max_wasm_stack, max_table_elements, async_stack_size and epoch_tick. A value of 0 keeps the engine default. Two settings come from the worker instead: `pool.total_instances = clamp(slots × 4, 8, 1000)` and `node_memory_budget = memory_bytes`.
- Module loading: `Runtime::prepared(sha)` first (L2), otherwise `FetchBlob` + `Runtime::prepare_cached(bytes)`. That uses the L1 AOT cache at `<cache_dir>/aot/*.cwasm`, keyed by the node-local `<cache_dir>/aot.key` HMAC key. `Runtime::cache_stats()` is exposed through `Worker::runtime()`.
- `Runtime::activate(&prepared, Activate{run, attempt: token.n, module_sha, journal, inputs, cancel}, sink)`.
- `ostro_engine::telemetry::with_traceparent(Some(Activate.traceparent))` wraps the activation when `traceparent` is non-empty (Phase 8 seam; the API exists, nothing was a no-op).
- Effect seam, as found on HEAD (06-06): `trait EffectDriver: Send + Sync { fn start(&self, run: &RunRef, attempt: u32, effect: LiveEffect); fn cancel(&self, run: &RunRef, op: u64) {} }`. It is supplied through `ActivationSink::effect_driver()`. `EffectProxy` implements it:
  - `start` sends `StartEffect` and is idempotent per op.
  - `cancel` sends `CancelEffect`.
  - `redacted_values` uses the engine's exact rule: headers whose name is in `redact`, compared ASCII case-insensitively.
- Guest logs: `ActivationSink::handles_guest_logs() = true`, and `guest_log` buffers lines that are forwarded as `GuestLogs` (on each append, at 64 lines, and at done).

## Measurements

- **Framing:** a 3 MiB step result with `max_frame_bytes = 64 KiB` (chunk cap 65,280 B) went out as 49 frames. The largest chunk was ≤ 64 KiB. The fake reassembled it byte-identical: `encode_event(decode_event(bytes)) == bytes` is asserted for every appended event.
- **Paging:** a 2,000-step journal replayed from `FetchJournal` pages of ≤ 64 KiB, using several pages. The 3 MiB row was served as a `BlobRef` and resolved with `FetchBlob`, with length and sha256 verified. No step re-executed: there are still exactly 2,000 `StepCompleted` events.
- **Self-fence:** after the fake killed the stream, the hot-parked activation ended in under 200 ms (asserted). The worker reconnected and sent a second `Hello`.
- **AOT cache:** first start `compiles = 1`. A second worker on the same `--cache-dir` had `compiles = 0, hits ≥ 1`. After tampering with the artefact, `tamper_total ≥ 1` and `compiles = 1`, and a fresh artefact was written. A corrupted module chunk produced "blob sha256 mismatch: requested …, received …", with nothing compiled, nothing cached and nothing appended.

## Guests (wasm32-wasip2, Rust SDK; serde =1.0.229, serde_json =1.0.154)

| Guest | Input | Output |
|---|---|---|
| `long-journal` v1 | `{steps, result_bytes?, sleep_ms?, wait_input?}`: `steps` `tick` steps, one `big` step returning `result_bytes` bytes, optionally one durable sleep, optionally one wait for signal `"input"` (any JSON, up to 3 MiB) | `{steps, sum, big_len, input_len}` |
| `pay-flow` v1 | `{url}`: one `POST {url}/pay` with `Idempotency-Key: ctx.idempotency_key("pay")` (also the host idempotency key), `Authorization: Bearer {{secret:PAY_TOKEN}}` (redacted), JSON body `{idempotency_key, amount: 100}` | `{status, idempotency_key}` |
| `cpu-burn` v1 | `{ms, iters_per_ms?}`: one `burn` step looping `ms × iters_per_ms` times (default 250,000/ms) | `{started_ms, ended_ms, iters}`, read from the journaled `ctx.now()` before and after the step (W3) |

`sleep_ms` was added to `long-journal` beyond the plan's input shape. It gives the session tests a hot-parked activation that holds a lease (the hot wait must be longer than the sleep).

## Failpoints (names fixed for 07-18 / 07-21)

`worker::before_append`, `worker::before_done`, `worker::before_start_effect`, `worker::after_start_effect`. They are read through `worker_fail!` (`return(error)` by default, `return(hang)`, `return(wait:<ms>)`) and compiled to `false` without the `failpoints` feature. Every failpoint has a test in `tests/failpoints.rs`, which runs with `--test-threads=1`.

## Worker API for later plans

- `Worker::new(cfg)`, `Worker::run(&self, shutdown)`, `Worker::subscribe()` (a `WorkerEvent` broadcast), `Worker::runtime()`, `Worker::active_activations()`, `Worker::worker_id()`.
- `pub async fn run(cfg, shutdown)` keeps the 07-02 signature.
- `WorkerEvent` variants: `Joined`, `Connected`, `Disconnected`, `SelfFenced{cancelled}`, `Rejoined`, `Renewed`, `ActivationStarted`, `ActivationFailed`, `ActivationEnded`, `Drained`.
- The runner logs `activate run=<id> attempt=<n>` at info level when an activation starts.
- `WorkerError::NotImplemented` (the 07-02 stub) is gone. The error variants are now `Config`, `Join`, `VersionMismatch` and `Engine`.

## Test results

- `cargo test -p ostro-worker --locked`: 14 unit, 13 protocol, 3 module_cache passed. Five back-to-back reruns of protocol + module_cache were all green.
- `cargo test -p ostro-worker --features failpoints --locked -- --test-threads=1`: 13 unit, 4 failpoints, 3 module_cache, 13 protocol passed.
- `cargo test -p ostro-worker-proto --locked`: 5 wire tests passed.
- `cargo clippy -p ostro-worker -p ostro-worker-proto --all-targets --locked -- -D warnings` is clean, with and without `--features failpoints`. `cargo fmt --check` is clean.
- `cargo check -p ostro-server -p ostro --all-targets --features ostro/failpoints --locked` is warning-free with the proto change.
- The guests build with `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests -p long-journal -p pay-flow -p cpu-burn`.
- `grep -rn secret crates/ostro-worker/src` shows only the token-format validation (`wkr_<id>.<secret>`) and the scrub unit test. There is no secret-resolution code path.

## Deviations from Plan

1. **Commit split.** The Task 1 session code spawns activations through the Task 2 runner, RemoteSink and EffectProxy, so an honest compiling split was not possible. Commit 1 holds the whole runtime library, the fake server and the failpoint suite. Commit 2 holds the guests and the two suites that execute them (protocol.rs and module_cache.rs).
2. **[Rule 2] `WorkerConfig.endpoints`.** The cert-manager path had no way to learn the cluster endpoints. The field is required with `tls_cert/tls_key/tls_ca`, and a config unit test was extended to cover it.
3. **[Rule 3] `AppendFrame.kind = 9`.** The frame had no kind, and `decode_event` needs one (additive proto field, allowed by the plan).
4. **Fake join endpoint.** It runs on a std thread over `rustls::StreamOwned` instead of axum. axum and hyper are not ostro-worker dependencies, and only 07-02 may edit Cargo files.
5. **Frame sizes.** They are recorded as chunk lengths, and "frame ≤ max" is asserted on the chunk. `prost::Message::encoded_len` is unavailable because prost is not a direct ostro-worker dependency.
6. **`long-journal.sleep_ms`.** This input was added; see Guests.

## Open items for later plans

- **07-09/07-12: await-token key.** A worker mints await tokens with a process-local key, because `Welcome` carries no token key. A token minted live on a worker therefore will not verify on the leader (replay never needs the key). Fix options: an additive `Welcome.token_key` field, which sends a credential to workers, or leader-side minting. This is a security decision.
- **07-09: renewal `Drain`.** The `Drain` on the old session after a renewal must not revoke that worker's leases. The new session's first heartbeat lists them, sent with the same worker_id and a new certificate.
- **07-09: version checks.** `Hello.wasmtime_version` is the constant `"50.0.2"` (`ostro_worker::WASMTIME_VERSION`), because the engine keeps its copy private in `cache.rs`. The server should compare against the same constant, or the engine should export it.
- **07-09/07-12: one session at a time.** The worker holds one session to the current endpoint and rotates endpoints on failure. Connecting to every `Welcome.members` entry at once is not implemented.
- `scripts/ci-local.sh` was not run in full (hours on this machine). The touched crates' tests, clippy, fmt and a check of the dependent crates are green. 07-22 runs the full gate.

## Self-Check: PASSED

- Files: every file in key-files exists at `4802f84`.
- Commits: `67b4a46` and `4802f84` are on main.
