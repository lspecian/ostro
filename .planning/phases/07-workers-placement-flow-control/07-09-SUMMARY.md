---
phase: 07-workers-placement-flow-control
plan: 09
subsystem: worker service (ostro-server workers/)
tags: [workers, join, tokens, mtls, cluster-listener, registry, leases, revocation, sec-01, work-01]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-02 seam (P7Shared.registry/join, service::start hook, [workers]/[cluster] config); 07-05 LeaseRenewer/Job/ExecOutcome::Abandoned; 07-06 ClusterCa/TlsState/WorkerIdentity/sign_worker_csr; 07-07 worker runtime + join contract; 07-11 ostro_api::phase7 join/token types"
provides:
  - "JoinService: create/list/revoke worker:join tokens (meta ApiToken, F8 expiry), join, renew"
  - "Cluster listener (tonic mTLS, client cert always required, optional external client_ca) + Execute service"
  - "WorkerRegistry: sessions, absolute credit, pick -> CreditGuard, attach_job -> RemoteJobHandle, heartbeat lease renewal, renewal hand-off, abandonment, changed()"
  - "Lease-scoped FetchJournal/FetchBlob, Append/StartEffect routed to the job handler (UNIMPLEMENTED until 07-12), Renew"
  - "Phase 7 worker harness tests/common/p7_workers.rs (Rig, CapturedJob, TestWorker, WorkerProc, OtherCa, WireStats)"
  - "D-W27: await tokens minted on the leader only"
affects: [07-12, 07-13, 07-14, 07-16, 07-17, 07-18, 07-21, 07-22]
tech-stack:
  added: []
  patterns:
    - "late binding of fixed-signature seam objects: JoinService::attach(meta, clock) + set_endpoints from service::start"
    - "issuer classification by signature check against the cluster CA key (x509-parser verify) when an external client CA is configured"
    - "renewal hand-off: a heartbeat lease held by a sibling session of the same worker moves the job; Drain + close hands remaining jobs to a live sibling"
key-files:
  created:
    - crates/ostro-server/tests/worker_session.rs
    - crates/ostro-server/tests/common/p7_workers.rs
  modified:
    - crates/ostro-server/src/workers/join.rs
    - crates/ostro-server/src/workers/registry.rs
    - crates/ostro-server/src/workers/service.rs
    - crates/ostro-meta/src/state.rs
    - crates/ostro-server/tests/common/mod.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/services/blob_gc.rs
    - crates/ostro/tests/common/mod.rs
    - crates/ostro/tests/fx_redrive.rs
    - .planning/phases/07-workers-placement-flow-control/07-DECISIONS.md
key-decisions:
  - "D-W27 (coordinator, user-overridable): await tokens are minted on the leader; no Welcome.token_key; 07-12 re-mints AwaitTokenCreated on Append"
  - "Join/renew refusals reuse the 07-11 contract: InvalidArgument + details.reason = join_token_invalid (gRPC Renew maps it to UNAUTHENTICATED)"
  - "No server->worker Drain message: adding a ServerMsg variant breaks 07-07's exhaustive match, so registry.drain() only stops assignment; execute.proto unchanged"
  - "Server-side WASMTIME_VERSION constant + a test asserting it equals ostro_worker::WASMTIME_VERSION (engine export deferred: ostro-engine is shared with Phase 8)"
  - "Test configs bind the cluster listener on 127.0.0.1:0 (server default 0.0.0.0:7701 would collide across parallel tests)"
metrics:
  duration: "~2h"
  completed: 2026-11-21
---

# Phase 7 Plan 09: Worker service Summary

**The server now accepts authenticated workers. `worker:join` tokens are stored as the existing meta `ApiToken` (sha256 hash, F8 expiry on the injected clock). The mTLS cluster listener (client certificate always required) serves `Execute`: identity from `peer_certs()`, version checks with `GoAway`, `Welcome` with `heartbeat_ms = min(heartbeat, lease_ttl / 3)`, credits, heartbeat lease renewal, lease-scoped journal/blob reads, peer-identity-checked bulk RPCs (W6), the external-CA identity rule (D-W25), revocation on the meta change, and abandonment of every job of a dead stream.**

## Commits

See the final report (`git log --grep '(07-09)'`): `feat(07-09): worker join tokens and registry`, `feat(07-09): mTLS cluster listener and Execute session service`, then the docs commit.

## Registry API (for 07-12 / 07-13 / 07-14 / 07-16)

```rust
// crates/ostro-server/src/workers/registry.rs
impl WorkerRegistry {
    pub fn new(cfg: &WorkersSection) -> Self;                       // 07-02 signature
    pub fn connected(&self) -> usize;                                 // sessions
    pub fn slots_free(&self) -> u64;                                  // assignable credit, all sessions
    pub fn max_frame_bytes(&self) -> u64;
    pub fn changed(&self) -> watch::Receiver<u64>;                    // Placement::changed source
    pub fn sessions(&self) -> Vec<WorkerInfo>;                        // sorted (worker_id, connected_at)
    pub fn drain(&self, worker_id: &str) -> Option<bool>;             // None unknown, Some(already)
    pub fn pick(&self, pred: impl Fn(&WorkerView<'_>) -> bool) -> Option<CreditGuard>;
    pub fn attach_job(&self, credit: CreditGuard, spec: AttachSpec) -> Result<RemoteJobHandle, AttachError>;
    pub fn close_where(&self, reason: &str, pred: impl Fn(&PeerIdentity) -> bool) -> usize;
    pub fn close_token(&self, token_id: &str) -> usize;
    // used by the service: open_session, close_session, credit, heartbeat, worker_drain,
    // job_event, authorize, note_blob, known_blob, rpc_handler, renew_job, session_peer, has_session
}
pub struct WorkerInfo { session_id, worker_id, token_id: Option<String>, labels: BTreeMap, slots, free, leases, memory_bytes, connected_at_ms, draining }
pub struct WorkerView<'a> { worker_id: &'a str, labels: &'a BTreeMap<String, String>, free: u32 }
pub struct AttachSpec { run: RunId, token: AttemptToken, lease: LeaseRenewer, handle: PartitionHandle<Rf1Log>, module_sha: Option<[u8; 32]> }
pub struct CreditGuard;      // session_id(), worker_id(); Drop returns the credit (bumps changed)
pub struct RemoteJobHandle;  // job_id(), run(), token(), worker_id(), session_id() (None once abandoned),
                             // send(ServerMsg::Msg) -> bool, next_event().await, try_event(),
                             // rpcs() -> UnboundedReceiver<JobRpc> (registers the Append/StartEffect handler),
                             // renew() -> bool; Drop detaches (bumps changed)
pub enum RemoteEvent { Done(v1::Done), GuestLogs(v1::GuestLogs), CancelEffect(v1::CancelEffect), Abandoned { reason } }
pub enum JobRpc { Append { batch: AppendBatch, reply: oneshot::Sender<Result<v1::AppendResult, Status>> },
                  StartEffect { req: v1::StartEffectRequest, reply: oneshot::Sender<Result<v1::StartEffectResponse, Status>> } }
pub struct AppendBatch { from_seq: u64, events: Vec<(u32 /* kind */, Vec<u8> /* encode_event bytes */)> }
pub fn wire_run(&RunId) -> v1::RunRef; pub fn wire_token(AttemptToken) -> v1::Token;
```

- Credit of a session = `min(reported, slots - attached) - outstanding guards`, 0 while draining. `pick` takes the matching session with the most credit; ties go to the least recently assigned.
- `changed()` bumps on session open/close, `Credit`, `Heartbeat`, attach, detach, guard drop and drain. Per the 07-05 hand-off, `NodePlacement` (07-12) should forward it and also bump on every embedded release.
- Heartbeat: every listed `(run, token)` held by this session renews its `LeaseRenewer`. A lease that belongs to another session **of the same worker** moves to this session (07-07 renewal: the new session's first heartbeat lists the activations), which avoids a false `LeaseLost`. Any other lease, including one whose renew returns false (revoked), is answered with `LeaseLost`.
- Session end: every job still attached gets `RemoteEvent::Abandoned{reason}` at once (measured below). The exception is a session that received the worker's `Drain` while a live sibling session of the same worker exists: its jobs move to the sibling, so **the renewal `Drain` never revokes leases** (the open item from 07-07).
- Successful `Append` (handler reply `Next`) renews the lease (`renew_job`).
- 07-12 must still: send `Activate` through the handle, map `Done` and `Abandoned` to `ExecOutcome`, act on `rpcs()`, and send `LeaseLost`/`Cancel` when `job.revoked` fires (the registry does not watch `revoked`).

## JoinService API (for 07-14)

```rust
// crates/ostro-server/src/workers/join.rs (re-exports ostro_api::phase7::{JoinRequest, JoinResponse, WorkerTokenCreated, WorkerTokenInfo})
JoinService::new(meta: MetaView, tls: Arc<TlsState>, cfg: &WorkersSection)        // 07-02, in P7Shared.join
fn attach(&self, meta: Weak<MetaHandle>, clock: Arc<dyn Clock>)                    // called by service::start
fn set_endpoints(&self, Vec<String>); fn endpoints(&self) -> Vec<String>
async fn create_token(&self, ttl: Option<Duration>, join_url: &str) -> Result<WorkerTokenCreated, ApiError>
fn list_tokens(&self) -> Vec<WorkerTokenInfo>                                       // sorted by id, wkr_ + worker:join only
async fn revoke_token(&self, token_id: &str) -> Result<(), ApiError>                // NotFound when absent
fn token_exists(&self, token_id: &str) -> bool
async fn join(&self, req: JoinRequest) -> Result<JoinResponse, ApiError>             // ControlPlane::join_worker passthrough
async fn renew(&self, token_id: &str, worker_id: &str, csr: &[u8]) -> Result<JoinResponse, ApiError>
```

- 07-14 wires `ControlPlane::{create_worker_token, list_worker_tokens, revoke_worker_token, join_worker}` straight to these. `create_token(None, ..)` uses `[workers] join_token_ttl`, and `join_url` is the client listener's public URL. `list_workers` / `drain_worker` map `registry.sessions()` / `registry.drain()` (`node_id` comes from the ctx).
- `service::start` binds the JoinService (meta handle + clock) even when `[workers] enabled = false`, so tokens always work. `join` then answers `Unavailable` because no endpoints exist.
- Join with a worker_id the certificate cannot carry (`[A-Za-z0-9._-]{1,64}`; the API grammar also allows `:` and up to 128 characters) is `InvalidArgument`.

## Token record usage

The meta `ApiToken` is used as is, with no new field: `id = "wkr_" + 10 Crockford base32`, `hash = sha256(secret)` where the secret is 20 random bytes as 32 base32 characters, `scopes = ["worker:join"]`, `tenant = None`, `created_at_ms` and `expires_at_ms = Some(created + ttl)` from the injected clock. Create is a CAS put with `expected_version: None` and is retried on an id collision. Revoke is a CAS delete at the read version. The hash is compared in constant time, and every check runs before the verdict. Secrets never appear in `Debug`, logs or errors.

`ostro-meta` gained a read accessor `MetaState::tokens()` (see Deviations).

## Cluster listener and Execute service

- `service::start(ctx)` binds `[cluster] listen` and calls `service::serve(listener, ClusterDeps)` (public, so tests run it without a full server). The TLS identity is the node chain + key, and `client_ca_root` is the cluster CA PEM plus the `[cluster.tls] client_ca` PEM. The service runs h2 keepalive `[workers] keepalive` (timeout 2x), a 4 MiB codec limit and nodelay. Shutdown sends `GoAway{"server shutting down"}` to every session, with a 5 s graceful bound followed by an abort.
- Endpoints come from `[cluster] advertise`, with the bound port appended to entries that lack one. Without `advertise` the bound address is used, and an unspecified IP is advertised as `127.0.0.1` with a warning: set `advertise` for remote workers. `Welcome.members` holds one entry per endpoint.
- Identity rules:
  - The leaf must parse as a `WorkerIdentity` of our cluster.
  - If it is issued by the cluster CA (checked by signature when a client CA is configured), its token must still exist and it must not be `ext`.
  - If it is issued by the external CA, its token id must be `ext` and it must carry clientAuth. External identities are limited to one session per worker id (`ALREADY_EXISTS`).
  - Node certificates are refused with `PERMISSION_DENIED`, and revoked tokens with `UNAUTHENTICATED`.
- Version checks: `proto_version` and `event_codec_ver` are always checked. `ostro_version` (`CARGO_PKG_VERSION`) and `wasmtime_version` (`service::WASMTIME_VERSION = "50.0.2"`) are checked when `require_same_version`. A mismatch gets `GoAway{"version mismatch: <field> <got> (server <want>)"}`, which the 07-07 worker treats as fatal. `Hello.worker_id` must equal the certificate's worker id.
- `FetchJournal`: rows `[from, to)` via `query::journal_rows`, paged within `max_frame_bytes - 256` (each inline row is counted as its length + 24, a `BlobRef` as 64). Blob refs are remembered on the job. `FetchBlob` serves the run's module, a blob ref already served, or (as a fallback) one found by scanning the run's journal; anything else is `PERMISSION_DENIED`. It is read verified from the blob store and sent in chunks of `min(1 MiB, max_frame - 256)` from `offset`.
- `Append`: the first frame is authorized. All frames must agree on session, token, run and `from_seq`, and `event_index` must be contiguous. They are reassembled up to `max_append_bytes`. Malformed or oversized input gets `AppendResult.rejected`. Without a handler the RPC is `UNIMPLEMENTED`.
- Metrics: the gauges `ostro_workers_connected` and `ostro_worker_slots_free` (total), and the counter `ostro_worker_sessions_total{result = accepted | closed | refused_identity | refused_version | refused_protocol | refused_busy}`.

## execute.proto

Unchanged. No field was needed: a server-to-worker `Drain` would need a new `ServerMsg` variant, which breaks the exhaustive match in `ostro-worker/src/session.rs` (a 07-07 file).

## Harness (tests/common/p7_workers.rs)

- `Rig::start(RigOpts{lease_ttl, heartbeat, require_same_version, client_ca_pem, node, max_frame_bytes})` starts real meta (cluster `CLUSTER`), the cluster CA, a JoinService attached to a `ManualClock` (`rig.clock`), the registry, and the cluster listener on `127.0.0.1:0`, with `rig.wire: Arc<WireStats>`.
- With `node: true` it also opens a 1-partition node whose dispatcher hands every `Job` to the test.
- Rig helpers:
  - `rig.start_run(&module_sha, input)`: RunStarted at seq 0; an input over 256 KiB makes both the input and the RunStarted row blobs.
  - `rig.next_job() -> CapturedJob` (`spec()`, `wire_run()`, `wire_token()`, `revoked()`, `finish(ExecOutcome)`).
  - `rig.attach(&job, worker_id) -> RemoteJobHandle`.
  - `rig.put_blob`, `rig.token(ttl)`, `rig.identity(token, worker_id)` (join through the JoinService) and `rig.worker(worker_id, slots)` (a raw `TestWorker` with credit registered).
  - `rig.join_url()` (a loopback axum `POST /v1/workers/join`).
  - `rig.spawn_worker(token, worker_id, slots, labels) -> WorkerProc`: an in-process `ostro_worker` with `wait_event(..)`, and `kill()` to abort and drop it.
- `TestWorker::{open, try_open -> Opened::{Welcome, GoAway, Refused}, send, heartbeat, next}`, `connect(addr, ca, identity)`, `hello(worker_id, slots)`, `new_key_and_csr`, and `OtherCa` for external-CA and wrong-CA certificates.
- **Message sizes.** `WireStats` records the payload bytes of every `FetchJournal` page, `FetchBlob` chunk and `Append` chunk, in and out (`max(dir, rpc)`). ostro-server cannot name `prost::Message` (only 07-02 edits Cargo files), so these are payload sizes, not encoded lengths. The 4 MiB tonic limit is the hard cap on both sides.

## Test results

- `cargo test -p ostro-server --test worker_session --locked`: **18 passed** in 9-13 s at load average 20-50:
  - join_service: 3 tests (token lifecycle with a 1 s TTL refused after `clock.advance(1500)`, refused tokens/scopes/CSRs, renew checks)
  - registry: 5 tests (pick/credit/guards/drain, heartbeat renewal over 3 s with ttl 1 s plus LeaseLost plus expiry without heartbeats, abandonment in under 100 ms, renewal hand-off, wasmtime constant)
  - session/rpc/fetch: 10 tests (in-process worker join + mTLS + labels/slots + credit + kill leading to Abandoned; no cert, foreign CA and node cert refused; revocation leading to GoAway within 1 s, then reconnect UNAUTHENTICATED and re-join refused; four version mismatches leading to GoAway; heartbeat_ms = 333 for ttl 1 s; lease-scoped FetchJournal/FetchBlob incl. a BlobRef row, module chunks within 64 KiB and W6; Append/StartEffect routing and UNIMPLEMENTED without a handler; the Renew RPC and the session hand-off; external-CA identities; `ostro server` binds the listener)
  - The plan's Task 1 filter (`-- registry join_service`) ran 8 passed.
- `cargo test -p ostro-server --locked --no-fail-fast` (before rebasing onto 07-08): every binary green (44 `test result: ok`, exit 0). The first full run failed once in `otlp_canary` with "server start timed out" (the 20 s harness bound) at load average 53. That test was green when rerun alone (7.6 s) and in the second full run.
- After rebasing onto main `57d50ce` (07-08): worker_session 18, flow_control 10, auto_pause 2, executor 8, leases 4 and the lib 102 all passed.
- `cargo test -p ostro --test server_config` passed 4 (spawned `ostro server` with the ephemeral cluster port).
- `cargo clippy -p ostro-server -p ostro-meta -p ostro --all-targets --locked -- -D warnings` is clean, `cargo check -p ostro-server --features failpoints --all-targets` is warning-free, and `cargo fmt --all --check` is clean.
- Not run: the full `scripts/ci-local.sh` gate (07-22 runs it).

## Deviations from Plan

1. **[Rule 3, blocking] Test configs and the 0.0.0.0:7701 default.** `[workers] enabled = true` with the server-profile `[cluster] listen = 0.0.0.0:7701` would make every parallel server-profile test (and every spawned `ostro server`) collide on port 7701 once the listener binds. I added one line per site: `cluster.listen = 127.0.0.1:0` in `tests/common/mod.rs::server_config` (07-09 plan said not to edit it; one appended line, file clean on main), the two server tests in `src/server.rs`, the blob_gc fixture in `src/services/blob_gc.rs`, and `OSTRO_CLUSTER__LISTEN=127.0.0.1:0` in `crates/ostro/tests/common/mod.rs::server()` and `fx_redrive.rs`. Product behaviour is unchanged (D-W16).
2. **[Rule 3] `MetaState::tokens()`** (ostro-meta, 5 lines): a read-only iterator next to `token()`. `list_tokens` needs it, and there was no way to enumerate tokens. It has no format impact.
3. **JoinService types.** The 07-02 server-local `JoinRequest`/`JoinResponse` are replaced by re-exports of the 07-11 `ostro_api::phase7` types, so 07-14 can pass them straight through. `renew` takes `csr: &[u8]` (PEM or DER, as `RenewRequest.csr`). The fixed `JoinService::new` signature is kept; the meta handle, clock and endpoints are late-bound by `service::start`.
4. **`attach_job` takes the `CreditGuard`** (not a session id), so a credit can never be double-spent.
5. **No server-to-worker Drain** (see execute.proto above). `registry.drain(worker_id)` stops assignment only. Follow-up: 07-12/07-14 can add `ServerMsg.drain` together with a worker-side arm.
6. **Wasmtime version.** ostro-engine keeps `"wasmtime=50.0.2"` private (`cache.rs`), and the engine is shared with Phase 8, so I did not export it. The server has its own `service::WASMTIME_VERSION`, and `registry_wasmtime_version_matches_the_worker` asserts it equals `ostro_worker::WASMTIME_VERSION`. **Follow-up:** export one constant from ostro-engine and use it in both crates.
7. **Message-size recorder** is service-level payload accounting (see Harness), not a tower layer, because no prost or http-body crate is nameable from ostro-server.
8. **Await tokens (07-07 open item):** decided as D-W27 (leader-only minting, user-overridable) and recorded in 07-DECISIONS. 07-12 implements the re-mint.

## Self-Check: PASSED

PASSED: every key file exists and both feat commits are on main (hashes in the final report).
