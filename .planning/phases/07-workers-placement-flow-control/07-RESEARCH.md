# Phase 7: Workers, Placement & Flow Control - Research

**Researched:** 2026-11-19
**Domain:** remote execution protocol (gRPC + mTLS), TLS/ACME, placement and spillover, dispatcher flow control (concurrency, fairness, throttle, rate limit, pause), KEDA autoscaling
**Confidence:** MEDIUM-HIGH. Code seams were read directly (HIGH). Crate versions and dependency trees were checked against crates.io and `Cargo.lock` on 2026-11-19 (HIGH). Some exact crate APIs (rcgen 0.14, instant-acme 0.8, governor clocks) are MEDIUM; executors confirm them in `~/.cargo/registry`.

<user_constraints>
## User Constraints

There is no `07-CONTEXT.md`: no `/gsd:discuss-phase` was run. The project is in YOLO + auto_advance mode, so the planner adopts this research's recommendations as `07-DECISIONS.md`, as Phases 5, 6 and 8 did. The binding constraints come from the orchestrator and from the earlier phases.

### Locked (orchestrator + earlier phases)
- Phase 7 depends on Phase 6 and runs **in parallel with Phase 8**. Phases 1–3 are complete and Phase 4 is executing (04-16 EngineExecutor is in flight; 04-17..04-20 follow). Phases 5, 6 and 8 are fully planned. The Phase 5 freeze (05-19/05-20) **has not run**, so pre-freeze findings can still go back to Phase 5.
- Ordinals and reserved names come **only** from `.planning/RESERVATIONS.md`, and only from Phase 7's block: Cmd 20–26, StartRun 28–31, Derived 13–16, journal kinds 80–87, TerminalStatus 6–9, keyspaces 17 `p7_a` / 18 `p7_b`, RejectReason 15–17, InvalidCode 18–21, outbox 8–9, meta 19–20. Rule 2 applies if more is needed.
- After the freeze: WIT 0.1.0 is frozen (doc comments only before 05-20, then additive `0.1.N` only). Cmd 1–12 are frozen. Existing prost messages and postcard structs never gain fields. Journal kinds are append-only, with no `EVENT_CODEC_VER` bump. Every post-freeze meta change is a new appended variant plus a `META_FORMAT` bump with decoders (D20).
- cargo-deny runs on the stock `deny.toml`. Never: chrono, bincode, object_store, openssl-sys, native-tls, webpki-roots (D-FX2), prometheus, tokio-uring, or the wasmtime `cache` feature. No duplicate tonic, tonic-prost, prost, prost-types, hyper, http, axum, opentelemetry, opentelemetry_sdk or wasmtime.
- Secrets never enter guest memory, logs, the journal, `Debug` output or error messages (Phase 6 standing rule).
- WSL has no Docker, so kind and k3d are unavailable. Docker-only tests are env-gated or human_needed (D-FX5, P8-D18).
- Never reference a Chronik clone path. No pushes. Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Tracking files are edited by hand.

### Claude's Discretion (this research recommends; the planner adopts in 07-DECISIONS.md)
Everything below that is marked **D-W**n.

### Deferred (out of scope for Phase 7)
- Multi-node cluster membership, Raft/replication over the cluster listener, and cluster-wide flow-control quotas (Phases 12–13). Phase 7 builds the CA and the cluster listener that those phases reuse.
- Native (non-Wasm) worker protocol (post-1.0, PRD open question 3). Step-level placement hints FR-5.6 (P2).
- Per-tenant quotas and weighted tenant fairness (Phase 14). Phase 7's fairness machinery is shaped so tenants can become one more key level.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| WORK-01 | `ostro worker --join` connects over an mTLS gRPC Execute stream with credits, heartbeats, blob fetch and per-(run, attempt) secret resolution | §Worker protocol (session stream + FetchJournal/FetchBlob/Append/StartEffect RPCs), §mTLS & join (cluster CA, CSR bootstrap with CA-hash pinning), §Effects on workers (D-W3: effects bridged back to the leader, where secrets resolve per (run, attempt); USER REVIEW) |
| WORK-02 | Journals paged to workers; no single message exceeds gRPC limits | §Journal paging (pull-based FetchJournal pages ≤ `max_frame_bytes`, blob-backed rows via FetchBlob, client-streamed Append frames) |
| WORK-03 | Queue placement `embedded` / `workers` / `auto`; `auto` spills on CPU/slots/memory/lag with hysteresis | §Placement & spill (Placement seam replacing the slot semaphore, SpillController state machine, LoadSampler) |
| WORK-04 | Worker labels and queue selectors control placement | §Label selectors (k8s-style equality/set-based selector, matched at reserve time) |
| WORK-05 | `ostro_queue_worker_demand{queue}` + demand endpoint usable by KEDA; workers scale to zero | §Demand & KEDA (corrected demand definition, `GET /v1/queues/{q}/demand`, ScaledObject, local KEDA simulator test + human_needed real-KEDA paths) |
| WORK-06 | Workers fetch modules by sha256 and cache AOT artefacts locally | §Module fetch (FetchBlob with lease-scoped authorization, sha256 verify, the existing HMAC'd L1 AOT cache under `--cache-dir`) |
| DISP-03 | Per-queue concurrency, priority and rate limits | §Flow control (node-wide queue counters, queue weight = queue priority in weighted DRR, run priority bands, throttle/rate) |
| DISP-04 | Keyed concurrency with fair scheduling across keys | §Fair scheduling (DRR over `concurrency_key` within priority bands; node-wide per-key counters) |
| DISP-05 | Throttle (queue the excess) vs rate limit (reject the excess) | §Throttle vs rate limit (throttle = GCRA on first activations in the dispatcher; rate limit = GCRA at StartRun admission → 429) |
| OPS-01 | Pause/resume a run, a workflow or a queue; auto-pause when retries are exhausted | §Pause (Cmd PauseRun/ResumeRun, RunRecord pause mark, meta flags for workflows/queues, auto-pause replacing `CompleteRun{Failed}` under policy; D1 interplay) |
| SEC-01 | TLS on the client listener (self-signed dev, provided certs, ACME); mTLS on cluster and worker connections with a cluster CA | §Client listener TLS (axum-server + reloadable resolver, modes off/auto/self-signed/files/acme; instant-acme), §mTLS & join |
| QA-03 | Fencing chaos test: the external mock sees exactly one call per idempotency key despite zombies and failovers | §Fencing chaos test (spawned server + workers, kill -9 / SIGSTOP / failpoints, MockProvider `/pay`, 10,000 runs, sharded/resumable) |
</phase_requirements>

## Summary

Phase 7 adds two things the dispatcher was built to accept: **more executors** and **admission rules**. The 04-14 dispatcher already has the right shape. It is generic over `trait Executor`, its leases are soft, it fences through the logged `AttemptGranted` plus apply's token check, and it carries `concurrency_key` without enforcing it "until Phase 7". Two internals have to generalize. First, the node-wide `slots: Arc<Semaphore>`: a permit means "an embedded slot", but work can now go to a worker. Second, `pop_next`, which today round-robins across queues and orders strictly by priority. The recommended shape is to add two seams to `ostro-partition`, both implemented in `ostro-server`:
- `Placement::reserve(&EntryView) -> Reservation | Blocked`. Its RAII reservation replaces the permit inside the lease.
- `FlowControl::admit(&EntryView) -> Admit | Blocked{until}`. Its RAII permit holds the queue and key counters.

Inside the dispatcher, selection becomes weighted DRR across queues, then priority bands, then DRR across concurrency keys.

The remote side should be a **thin engine host**. The worker runs `ostro-engine` with a `RemoteSink: ActivationSink` and nothing else. Everything stateful or security-relevant stays on the leader:
- deriving intents from events (`PartitionSink` + `derive`),
- the outcome state machine (Suspend/EnqueueReady CAS, Retry, CompleteRun),
- the input bridge,
- leases,
- and, recommended as D-W3, **all effects**.

The worker's `EffectDriver` sends `StartEffect{token, op, redacted-header values, retry}`. The leader rebuilds the `LiveEffect` from the already-committed `EffectScheduled` journal event and hands it to Phase 6's `ServerEffectDriver` unchanged. That includes `SendGate`, `CompleteEffect`, secrets resolved per (run, attempt) in the bridge, and Phase 8's `EffectHooks`. Three consequences follow. A compromised worker never holds a secret or egress credentials. A worker that has lost its stream cannot cause any side effect. And at most one send per idempotency key happens even with zombie workers, because the leader's `EffectRegistry` attaches duplicates by (partition, run, op).

TLS has three parts:
1. A cluster CA generated with **rcgen 0.14.10** (`aws_lc_rs` and `pem` features, no `ring`).
2. A kubeadm-style join: the worker sends a token, a CSR and a pinned CA hash to `POST /v1/workers/join` on the client listener, then uses mTLS on a new cluster listener `:7701` served by tonic `tls-aws-lc`.
3. The client listener served through **axum-server 0.9.2** (`tls-rustls-no-provider`) with a hot-swappable `ResolvesServerCert`, plus **instant-acme 0.8.5** for ACME. rustls-acme is rejected: it has a hard `chrono` dependency, which deny.toml bans.

The fencing chaos test reuses Phase 6's `MockProvider /pay`, which dedupes by key and counts per key. It runs spawned `ostro server` and `ostro worker` binaries under kill -9, SIGSTOP zombies and executor failpoints. KEDA cannot run locally. A KEDA-simulator test implements the scaler math against real worker processes, and the real ScaledObject is a human_needed or CI path. The user's reachable microk8s cluster is an option, but only with the user's go-ahead.

**Primary recommendation:** keep the leader authoritative. Workers are stateless engine hosts behind five RPCs, and every write, effect and secret is resolved on the leader under the existing fencing. Generalize the dispatcher once, through `Placement` and `FlowControl` seams, in a single Phase 7 plan that is sequenced against 05-11 and 08-16.

## Ordinals and Reserved Names Phase 7 Needs (exact)

| Space | Phase 7 block | Needed | Allocation |
|-------|---------------|--------|------------|
| Cmd oneof | 20–26 | **2** | **20 `PauseRun{run_id 1, reason u32 2, detail string 3, token AttemptTokenMsg 4 (optional; present = executor auto-pause, fenced), requested_at_ms 5}`**, **21 `ResumeRun{run_id 1, requested_at_ms 2}`**. 22–26 spare. |
| StartRun fields | 28–31 | **0** | `queue`, `priority` and `concurrency_key` already exist (fields 4, 8, 9). |
| Derived intents | 13–16 | **0** | Pause is a command, not an intent. |
| Journal kinds | 80–87 | **0** | Optional history markers `RunPaused`/`RunResumed` were considered and rejected: the engine would have to learn to skip them on replay. Pause state is visible through the run record and the API. |
| TerminalStatus | 6–9 | **0** | Pause is not terminal. |
| Keyspaces | 17 `p7_a`, 18 `p7_b` | **0 if F1 is accepted** (pause mark in RunRecord, pre-freeze). Otherwise **1**: rename 17 `p7_a` → **`run_pauses`** in RESERVATIONS (docs-only commit, before anyone declares a placeholder named `p7_a`), and declare 16 `schedule_state` as an empty placeholder if Phase 8 has not landed it. | See F1. |
| RejectReason | 15–17 | **1** | **15 `RunPaused`** (outcome code 16+15 = 31). EventsAppended, Suspend and EnqueueReady from an executor against a paused run are rejected. Indices **12–14 are reserved for P6, which declared 0 needs**, so Phase 7 must declare `#[doc(hidden)] ReservedP6_12..14` placeholders that are never emitted (P-15 convention). |
| InvalidCode | 18–21 | **0** | Existing `MissingField`/`BadRunId` cover malformed pause commands. |
| Outbox kinds | 8–9 | **0** | |
| Meta variants | 19–20 | **0 if F2 is accepted** (fields pre-freeze in QueuePolicy/WorkflowPolicy). Otherwise **2**: **19 `QueueFlow`**, **20 `WorkflowFlow`**, plus the first post-freeze `META_FORMAT` bump with a decoder for the 05-20 format. | See F2. |

**Name-only reservations** (07-01 adds them to RESERVATIONS §11 in a docs-only commit):
- crates: `ostro-worker`, `ostro-worker-proto`;
- config sections: `[workers]` (server side), `[worker]` (worker process), `[cluster]` (`listen`, `advertise`, `tls`; P7 creates it and P12 extends it), `[api.tls]`, `[flow]`;
- gRPC packages and services: `ostro.worker.v1.Execute`, `ostro.api.v1.Queues`, `ostro.api.v1.Workers`;
- REST paths:
  - `/v1/queues/{name}/flow`, `/v1/queues/{name}/pause`, `/v1/queues/{name}/resume`, `/v1/queues/{name}/demand`;
  - `/v1/workflows/{name}/pause`, `/v1/workflows/{name}/resume`;
  - `/v1/runs/{id}/pause`, `/v1/runs/{id}/resume`;
  - `/v1/workers`, `/v1/workers/join`, `/v1/workers/{id}/drain`, `/v1/workers/tokens`;
- failpoint prefixes: `worker::`, `placement::`, `flow::`;
- metric prefixes: `ostro_worker_`, `ostro_placement_`, `ostro_flow_` (`ostro_queue_worker_demand` is already named by 08-16);
- token-id prefix `wkr_` (worker join tokens are `ApiToken` records with scope `worker:join`);
- CLI verbs: `ostro worker` (+ `worker token create|list|revoke`), `ostro queue flow|pause|resume|demand`, `ostro run pause|resume`, `ostro workflow pause|resume`;
- on-disk: `<data_dir>/tls/` (cluster CA, dev cert, ACME account and cert cache).

## Pre-Freeze Findings (send back to Phase 5 before 05-19/05-20)

| ID | Severity | Finding | Recommended Phase 5 change | Fallback if rejected |
|----|----------|---------|----------------------------|----------------------|
| **F1** | HIGH (format) | Run pause must survive restarts and gate every transition to Ready. `enqueue_ready(b, run, rec, ..)` in `apply/run.rs` is the single place where every ready row is created, and it already has the `RunRecord` in hand. After the freeze, RunRecord cannot gain a field without a VALUE_FORMAT bump and a row migrator (D18 amended). | **05-02 appends `pause: Option<PauseMark>` to `RunRecord`**, with `PauseMark{reason: u8, at_ms: u64, detail: String}` (0 = operator, 1 = retries_exhausted). Phase 5 never sets it. It costs one postcard byte per record. | Keyspace 17 renamed `run_pauses` (run → PauseMark), plus one point read in `enqueue_ready` on every wake-up and in rebuild. A read-budget exception must be documented. |
| **F2** | MEDIUM (meta) | Queue flow and placement config and workflow pause are meta records. D20 says that after the freeze each change is a new variant plus a META_FORMAT bump. Phase 8 avoided this for Schedule by landing its fields pre-freeze (F2(a)). Phase 7 would be the **only** post-freeze meta bump among Phases 6/7/8. | **05-07 extends `QueuePolicy`** with `placement: Option<u8>` (0 embedded, 1 workers, 2 auto), `selector: Vec<String>`, `spill: Option<SpillPolicy{cpu_pct, slots_pct, mem_pct, lag_ms, sustain_ms, cooldown_ms, hysteresis_pct}>`, `concurrency: Option<u32>`, `weight: Option<u32>`, `key_concurrency: Option<u32>`, `throttle: Option<RateSpec>`, `key_throttle: Option<RateSpec>`, `rate_limit: Option<RateSpec>`, `key_rate_limit: Option<RateSpec>`, `on_retries_exhausted: Option<u8>` (0 fail, 1 pause), `paused: bool`. **`WorkflowPolicy`** gets `paused: bool` and `on_retries_exhausted: Option<u8>`. `RateSpec{limit: u32, period_ms: u64, burst: u32}`. These go **before** the existing trailing `ext`. | Meta 19 `QueueFlow` and 20 `WorkflowFlow` with those shapes. 07-02 does the META_FORMAT bump and adds a decoder for the frozen format, extending the 05-07 golden. |
| **F3** | MEDIUM (format) | Pausing a workflow, and later per-tenant fairness (Phase 14), need the workflow and tenant of each **ready entry**. The dispatcher rebuild reads only `ready` rows (`ReadyValue{concurrency_key}`). Without them, rebuild needs a run-record read per backlog entry, or each grant must be checked lazily. | **05-02 extends `ReadyValue` to `{concurrency_key, workflow, tenant: Option<String>}`**, and `ReadyEntryRow` / `Action::EnqueueReady` carry them. The Action is in memory and not frozen. | The dispatcher reads the run record before granting, only while some workflow pause is active (spawn_blocking point read). |
| **F4** | MEDIUM (WIT doc, D14-compatible) | `run-info.attempt` is documented as "stable across replays", but `HostState::info()` returns `Activate.attempt`, the attempt token `n`. That value changes on every re-grant: lease expiry, worker death, spill moves, node restart. Phase 7 makes such re-grants routine, so a guest that branches on `info().attempt` diverges on replay. | **05-05 Task 3 doc comment**: "`attempt` is the current activation attempt; it increases on every re-dispatch (failover, lease expiry, worker loss) and is NOT stable across replays. Use it only for logging/metrics; never for control flow." Mirror it in the SDK docs (05-06). Doc-only, so D14 holds. | A 0.1.N interface later. |
| **F5** | LOW | No WIT, Cmd 1–12, StartRun, journal-kind or outbox change is needed for workers. Placement is invisible to guests (by design, for determinism). The command hash excludes header values, which the D-W3 StartEffect verification relies on: it compares URL, method, body and header names against the journal copy. | none | none |
| **F6** | LOW (docs) | docs/03 §8 defines demand as "runs ready for workers that no worker slot is taking". With KEDA's `desired = ceil(metric / target)`, that metric drops to 0 as soon as workers pick everything up, so the HPA scales down while the work runs. Separately, NFR-3.5 says the default `lease_ttl` is 15 s, while `DEFAULT_LEASE_TTL` and the 04-19 config default are 30 s. | Phase 7 gate (07-19) corrects docs/03 (D-W12) and picks one lease default (recommend keeping 30 s and amending NFR-3.5 wording, because the activation timeout must be less than lease_ttl or be renewed). | n/a |

Send F1–F4 to the orchestrator now. The cheapest outcome: F1 + F3 land in 05-02, F2 in 05-07, F4 in 05-05. Phase 7 then needs **2 Cmd tags, 1 RejectReason (+3 placeholders), 0 keyspaces and 0 meta variants**.

## Standard Stack

### Core (new pins for Phase 7; versions checked on crates.io 2026-11-19)

| Library | Version | Purpose | Why Standard / Notes |
|---------|---------|---------|---------------------|
| tonic (existing) | 0.14.9 | Worker `Execute` service on the cluster listener; worker-side client | Add features **`tls-aws-lc`** and **`tls-connect-info`** at member level (ostro-server, ostro-worker). Phase 8's `opentelemetry-otlp` `tls-aws-lc` feature also enables tonic `tls-aws-lc`. Feature unification makes this harmless, and no duplicate appears. Never enable `tls-ring` or `tls-webpki-roots` (bans). |
| rustls (existing) | 0.23.46 | Server and client configs, `WebPkiClientVerifier`, custom `ResolvesServerCert` | Already pinned with `aws_lc_rs`. `ostro_api::tls_provider()` is the single provider. |
| tokio-rustls (in lock) | 0.26.6 | TLS streams under tonic/axum-server | Already in the lock via reqwest/hyper-rustls. |
| rcgen | **0.14.10** | Cluster CA, server and worker leaf certs, CSR signing, dev self-signed cert, ACME CSR + TLS-ALPN-01 challenge cert | `default-features = false, features = ["aws_lc_rs", "pem", "x509-parser"]`. Defaults pull **ring**, so always turn defaults off. It depends on `time` 0.3 (not banned; also arrives via sqlx `time` in Phase 6) and `yasna`. MIT OR Apache-2.0. |
| x509-parser | **0.18.1** | Read the SAN URI / token id from peer certs; parse CSRs (rcgen `x509-parser` feature) | MIT OR Apache-2.0. Pulls asn1-rs, der-parser, nom 7 and oid-registry; nom 7 may duplicate nom 8 (warn only). No `verify` features. |
| axum-server | **0.9.2** | TLS for the one-port HTTP + gRPC client listener, graceful-shutdown `Handle` | `default-features = false, features = ["tls-rustls-no-provider"]`. Its openssl deps are behind `tls-openssl`, so openssl-sys stays out. `RustlsConfig::from_config(Arc<ServerConfig>)` takes a custom resolver. |
| instant-acme | **0.8.5** | ACME (RFC 8555) account, orders, HTTP-01 / TLS-ALPN-01, finalize, ARI | `default-features = false, features = ["aws-lc-rs", "hyper-rustls"]` (+ `"time"` only if ARI renewal windows are used). No chrono. hyper-rustls 0.27 and hyper-util are already in the lock (reqwest). It pulls `rustls-platform-verifier` → `rustls-native-certs` on Linux and security-framework on Apple targets (Apache/MIT). Apache-2.0. MEDIUM on exact API names. |
| governor (pinned by 06-02) | 0.10.4 | GCRA for throttle (dispatcher admission) and rate limit (StartRun admission) | Reuse Phase 6's pin. Use direct and keyed limiters with a **custom clock** that tests can drive. MEDIUM on the exact `Clock`/`Reference` trait shape. |
| libc | (in lock) | `getrusage(RUSAGE_SELF)` process CPU time for the spill sampler | One small safe wrapper. Normalize by `std::thread::available_parallelism()`, which respects cgroup quotas. Rejected: sysinfo 0.40.4 (whole-host CPU, wrong inside a container with CPU limits; large windows/objc2 surface). |

### Supporting (already present, reused)

| Library | Purpose in Phase 7 |
|---------|-------------------|
| ostro-engine `Runtime`, `prepare_cached`, L1 HMAC AOT cache (`cache.rs`, node-local `aot.key`) | Worker module cache: unchanged, pointed at `--cache-dir`. |
| ostro-server `PartitionSink`, `derive`, `run_started`, `journal_io`, 04-16 executor internals | Server-side proxy for remote activations (the same code paths as embedded). |
| ostro-bridge + 06-14 `ServerEffectDriver`, `PartitionSendGate`, `PartitionCompletionSink`, `EffectHooks` | Effects of remote runs (D-W3). |
| ostro-testkit `mock-http` `MockProvider` (06-10: `/pay` key dedupe, `requests(key)`, `charges(key)`, 409 in-progress) | Fencing chaos test. |
| ostro-testkit `canary` (06-19) | Proves no secret crosses the worker stream. |
| metrics facade (+ Phase 8 metrics-exporter-prometheus 0.18.3) | `ostro_queue_worker_demand`, `ostro_worker_*`, `ostro_placement_*`, `ostro_flow_*`. |
| reqwest 0.13.5 (rustls) | Worker → `POST /v1/workers/join` (bootstrap over TLS pinned to the CA hash). |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| instant-acme | rustls-acme 0.15.4 | **Rejected.** Hard `chrono` dependency (banned), `rcgen ^0.13` (a duplicate of 0.14), webpki-roots by default. |
| instant-acme | tokio-rustls-acme 0.10.0 | No chrono, reqwest 0.13 + rcgen 0.14, and it ships its own resolver with TLS-ALPN-01. But 0.10.0 was published 2026-11-19 (today), uses `log`, and has optional webpki-roots. **Fallback** if the instant-acme integration exceeds a plan. |
| axum-server | Hand-rolled tokio-rustls accept loop + `hyper_util::server::conn::auto` (axum `low-level-rustls` example) | Zero new crates, but graceful shutdown, ALPN and connection tracking would be ours. Use it only if axum-server conflicts with 04-17's serve path. |
| tonic `ServerTlsConfig` on :7701 | Custom rustls config + `serve_with_incoming` | Custom gives hot reload and a custom client verifier. tonic's config is static, so a server-cert change needs a restart; the documented 1-year server certs make that acceptable for Phase 7. |
| DRR for key fairness | Start-time fair queuing (virtual time) | Virtual time gives better short-term fairness for newly active keys and is cost-aware when costs are known. Each dispatch costs one unit and costs are unknown in advance, so DRR with quantum 1 is equivalent, O(1) and simpler. Revisit with cost charging in Phase 14. |
| Effects bridged to leader (D-W3) | Effects on workers + `ResolveSecret` RPC (docs/03 §5.3) | Worker-side effects scale effect I/O horizontally. But they need a worker-side MetaView (egress policies, connections incl. SQL DSNs, grants), worker egress IPs, SQL pools from every worker, a remote SendGate RPC and per-request secret RPCs. Secrets would then live in worker memory (threat model §9.1 "compromised worker"), and Phase 8's EffectHooks would need a second implementation. Effect I/O is async and cheap next to Wasm CPU, the thing workers exist to scale. |

**Installation (07-02 is the only plan that edits Cargo files):**
```toml
# root Cargo.toml [workspace.dependencies]
rcgen = { version = "0.14.10", default-features = false, features = ["aws_lc_rs", "pem", "x509-parser"] }
x509-parser = "0.18.1"
axum-server = { version = "0.9.2", default-features = false, features = ["tls-rustls-no-provider"] }
instant-acme = { version = "0.8.5", default-features = false, features = ["aws-lc-rs", "hyper-rustls"] }
# member level: tonic = { workspace = true, features = ["tls-aws-lc", "tls-connect-info"] } in ostro-server and ostro-worker
```
Verify after adding: `cargo deny check` (stock), `cargo tree -d` (no duplicate of the banned-dup set), `cargo tree -i ring -e normal --target all` (must print nothing; today ring sits in the lock only as an inactive optional of rustls-webpki), and `cargo tree -i chrono` (nothing).

## Architecture Patterns

### Recommended Project Structure
```
crates/
├── ostro-worker-proto/              # NEW: ostro/worker/v1/execute.proto + build.rs (protox, like ostro-proto)
├── ostro-worker/                    # NEW: worker runtime library (the binary is `ostro worker`)
│   └── src/{lib,config,join,session,remote_sink,effect_proxy,journal_fetch,blob_fetch,runner,drain}.rs
├── ostro-partition/src/leader/
│   ├── dispatcher.rs                # generalized: Placement + FlowControl seams, DRR selection, pause skip
│   ├── flow.rs                      # NEW: FlowControl trait, Admit, FlowPermit, EntryView (types + NoFlow)
│   ├── placement.rs                 # NEW: Placement trait, Reservation, SemaphorePlacement (old behaviour)
│   └── fair.rs                      # NEW: DRR ring + priority bands (pure, unit-tested)
├── ostro-partition/src/apply/pause.rs   # NEW: PauseRun / ResumeRun apply
└── ostro-server/src/
    ├── p7.rs                        # NEW: the single Phase 7 wiring seam called from server.rs (one edit)
    ├── workers/{mod,registry,service,remote_executor,placement,spill,selector,demand,join}.rs
    ├── flow/{mod,policy_view,limits,rate_limit}.rs
    └── tls/{mod,ca,listener,resolver,dev_cert,acme}.rs
deploy/keda/{scaledobject-metrics-api.yaml,scaledobject-prometheus.yaml,trigger-auth.yaml}
deploy/k8s/{worker-deployment.yaml,server-statefulset.yaml}
docs/{workers.md,flow-control.md,tls.md}
```

### Pattern 1: Leader-authoritative remote execution (the core design)

**What:** a remote activation is an ordinary dispatcher `Job` that the server keeps. A server-side `RemoteRunner` proxies it to a worker session. The worker runs only the engine.

```
dispatcher ──Job(token, record, handle, inputs, revoked, lease, reservation)──► PlacementExecutor
   PlacementExecutor: reservation = Embedded(permit) → EngineExecutor path (04-16, unchanged)
                      reservation = Worker(session, credit) → RemoteRunner:
     1. first activation (next_seq == 0): append RunStarted on the SERVER (build_run_started)
     2. journal end E = record.next_seq after RunStarted; repair_unsupported (children only after P6)
     3. send Activate{run, partition, token, module_sha, journal_len: E, lease_ttl, traceparent, limits}
     4. worker pulls FetchJournal(run, token, 0..E) pages; FetchBlob(module_sha) if not cached
     5. worker engine appends → Append(frames) → server decodes events → PartitionSink.append
        (derive intents, payload offload, fencing by apply) → AppendResult{next | Fenced | Conflict | Paused}
        → server renews job.lease on success
     6. dispatcher InputAppended(seq) → ordered input bridge (04-16) → Input{run, token, up_to_seq}
        → worker FetchJournal(from its end) → engine inputs channel
     7. engine EffectDriver::start → StartEffect{token, op, redrive, header_values, http_retry}
        → server verifies lease + journaled EffectScheduled(op) → ServerEffectDriver.start (P6, unchanged)
     8. Done{outcome, engine_end_seq} → SHARED outcome mapper (04-16 table) → ExecOutcome
     9. job.revoked → Cancel{run, token}; stream loss → ExecOutcome::Abandoned (new, see Pitfall 3)
```

**Why:** apply's token check remains the only write fence, with no second fencing check (the 04-15 "key insight"). Derive runs on the leader, so a worker cannot forge intents (timers, RegisterEffect, CompleteRun). The outcome state machine exists once, factored out of `executor.rs` (see Coordination). A worker without its stream can do nothing externally visible.

### Pattern 2: Worker protocol (`ostro.worker.v1`)

Use one long-lived control stream per (worker, server node), plus separate RPCs for bulk data. h2 multiplexes them on one TLS connection, so a 5 MiB module fetch never blocks a lease heartbeat (no head-of-line blocking).

```proto
syntax = "proto3";
package ostro.worker.v1;

service Execute {
  rpc Session(stream WorkerMsg) returns (stream ServerMsg);           // control: small messages only
  rpc FetchJournal(FetchJournalRequest) returns (stream JournalPage); // pull, lease-authorized
  rpc FetchBlob(FetchBlobRequest) returns (stream BlobChunk);         // modules + payload blobs, lease-authorized
  rpc Append(stream AppendFrame) returns (AppendResult);              // usually 1 frame; >frame size = several
  rpc StartEffect(StartEffectRequest) returns (StartEffectResponse);
  rpc Renew(RenewRequest) returns (RenewResponse);                    // client-cert renewal over mTLS
}

message Token { uint32 epoch = 1; uint32 n = 2; }
message RunRef { bytes run_id = 1; uint32 partition = 2; }

message WorkerMsg { oneof msg {
  Hello hello = 1; Credit credit = 2; Heartbeat heartbeat = 3; Done done = 4;
  CancelEffect cancel_effect = 5; GuestLogs guest_logs = 6; Drain drain = 7; } }
message ServerMsg { oneof msg {
  Welcome welcome = 1; Activate activate = 2; Input input = 3; Cancel cancel = 4;
  LeaseLost lease_lost = 5; GoAway go_away = 6; } }

message Hello { string worker_id = 1; uint32 proto_version = 2; string ostro_version = 3;
  string wasmtime_version = 4; uint32 event_codec_ver = 5; repeated string wit_packages = 6;
  map<string,string> labels = 7; uint32 slots = 8; uint64 memory_bytes = 9; }
message Welcome { bytes session_id = 1; uint64 node_id = 2; string cluster_id = 3; uint64 lease_ttl_ms = 4;
  uint64 heartbeat_ms = 5; uint32 max_frame_bytes = 6; EngineLimits limits = 7; repeated Member members = 8; }
message Credit { uint32 free_slots = 1; }                      // absolute, idempotent
message Heartbeat { repeated Lease leases = 1; uint32 free_slots = 2; uint64 memory_used = 3; }
message Lease { RunRef run = 1; Token token = 2; }
message Activate { RunRef run = 1; Token token = 2; bytes module_sha = 3; uint64 journal_len = 4;
  string workflow = 5; uint32 workflow_version = 6; string traceparent = 7; uint64 hot_wait_ms = 8; }
message Input { RunRef run = 1; Token token = 2; uint64 up_to_seq = 3; }   // notify; data via FetchJournal
message Cancel { RunRef run = 1; Token token = 2; uint32 reason = 3; }
message Done { RunRef run = 1; Token token = 2; bytes outcome = 3; uint64 engine_end_seq = 4; }
message FetchJournalRequest { bytes session_id = 1; RunRef run = 2; Token token = 3; uint64 from_seq = 4; uint64 to_seq = 5; }
message JournalPage { repeated JournalRow rows = 1; bool last = 2; }
message JournalRow { uint64 seq = 1; uint32 kind = 2; oneof payload { bytes inline = 3; BlobRef blob = 4; } }
message AppendFrame { bytes session_id = 1; RunRef run = 2; Token token = 3; uint64 from_seq = 4;
  uint32 event_index = 5; bytes chunk = 6; bool event_end = 7; bool last = 8; }
message StartEffectRequest { bytes session_id = 1; RunRef run = 2; Token token = 3; uint64 op = 4; bool redrive = 5;
  repeated Header redacted_values = 6; EffectRetry http_retry = 7; }
```
`outcome` in `Done` is the postcard `ActivationOutcome` encoding (a new serde derive in the engine or a mirror type). `traceparent` is Phase 8's seam: the worker sets `ostro_engine::telemetry` activation context from it. Proto field numbers are owned by `ostro-worker-proto`. They are not a RESERVATIONS space, because this is not a storage format; they are versioned by `proto_version`.

**Authorization rule for every bulk RPC:** `(session_id from the peer cert's session, run, token)` must match a live RemoteJob lease on this server. Otherwise the RPC returns `PermissionDenied`/`FAILED_PRECONDITION`. A worker can fetch only the modules and payload blobs of runs it currently leases.

### Pattern 3: Journal paging within gRPC limits (WORK-02)

- `max_frame_bytes` defaults to **1 MiB**. Set tonic `max_decoding_message_size`/`max_encoding_message_size` to **4 MiB** on both sides (tonic's default decode limit is 4 MiB), so a frame has headroom.
- A page holds as many rows as fit in `max_frame_bytes`. A row's inline payload is at most `INLINE_PAYLOAD_LIMIT` = 256 KiB, so every row fits. Blob-backed rows carry only `BlobRef{sha, len}`. The worker resolves them with `FetchBlob` in 1 MiB chunks and verifies length and sha256 (the same checks as `journal_io`). It then decodes with `decode_event`.
- Appends are **client-streamed frames**. An event larger than a frame is split by `(event_index, chunk, event_end)`. The server reassembles up to `[workers] max_append_bytes` (default 64 MiB), above which it returns `PayloadTooLarge`. It then calls `PartitionSink::append`, which already offloads payloads over 256 KiB to blobs and keeps every envelope below `DEFAULT_MAX_ENVELOPE_BYTES` (4 MiB).
- Test: run the server with `max_frame_bytes = 64 KiB`. Use a 2,000-step journal plus one 3 MiB step result and one 3 MiB input. A tonic interceptor records the size of every message, and the test asserts the maximum is ≤ 64 KiB + overhead and that replay completes identically to embedded execution.

### Pattern 4: mTLS, cluster CA and worker join (SEC-01, WORK-01)

```
server first start → <data_dir>/tls/ca.{crt,key} (rcgen, ECDSA P-256, 10 y, 0600, CN "ostro cluster <cluster_id>")
                   → <data_dir>/tls/node.{crt,key} (1 y; SANs = [cluster] advertise names/IPs + localhost)
operator           → ostro worker token create [--ttl 24h] → prints
                     "ostro worker --join https://host:7700 --token wkr_<id>.<secret> --ca-hash sha256:<hex>"
                     (ApiToken{id: "wkr_<id>", hash: sha256(secret), scopes: ["worker:join"]})
worker start       → generate key in memory (never written) → CSR
                   → POST /v1/workers/join {token, csr, labels, worker_id} over TLS on :7700.
                     The server cert is accepted if it chains to a CA whose sha256(DER) == --ca-hash
                     (custom rustls ServerCertVerifier for bootstrap; platform roots when [api.tls] mode = acme/files
                      and no --ca-hash is given)
                   ← {client_cert (SAN URI ostro://<cluster>/worker/<token_id>/<worker_id>, EKU clientAuth,
                      default lifetime 24 h), ca_cert, cluster endpoints [host:7701], renew_after}
                   → mTLS to every member's :7701 (tonic ClientTlsConfig: ca + identity); Session stream
renewal at 2/3 of the lifetime → Renew{csr} on the authenticated channel → reconnect with the new identity, drain the old stream
revocation         → token revoke: the server rejects Join/Renew for that token id and closes live sessions whose
                     cert carries it (checked at stream open and on every meta-change notification)
alternative        → --tls-cert/--tls-key/--tls-ca (cert-manager); no join; the cert must chain to [cluster.tls] client_ca
```
The cluster listener `[cluster] listen` defaults to `0.0.0.0:7701` in the server profile and `127.0.0.1:7701` in dev. It **always requires a client cert**, in every profile. tonic `ServerTlsConfig::new().identity(node).client_ca_root(ca)` is used, and the peer identity is read with `request.peer_certs()` (tonic `tls-connect-info`), then parsed with x509-parser. Keep `ostro-worker-proto` services off the client listener.

### Pattern 5: Client listener TLS modes (SEC-01)

`[api.tls] mode = "auto" | "off" | "self-signed" | "files" | "acme"`

| Mode | Behaviour |
|------|-----------|
| `auto` (**default in both profiles**, D-W8) | Plaintext when the listen address is loopback. Otherwise self-signed in dev; in the server profile, refuse to start without `files`/`acme` unless `allow_self_signed = true`, which logs a warning. |
| `self-signed` | rcgen cert at `<data_dir>/tls/dev.{crt,key}` (SANs: hostname, localhost, 127.0.0.1, ::1, listen IP). The sha256 fingerprint is printed in the ready line. |
| `files` | `cert_file`, `key_file` (PEM). Reloaded on SIGHUP and on an mtime poll (30 s). |
| `acme` | instant-acme. `domains`, `email`, `directory` (default Let's Encrypt production), `cache_dir`, `challenge = "tls-alpn-01" | "http-01"`, `http01_listen` (e.g. `0.0.0.0:80`). |

- All modes feed one `ResolvesServerCert` backed by an `ArcSwap<CertifiedKey>`, so renewals and reloads swap with no restart.
- ALPN must be `["h2", "http/1.1"]` so gRPC and REST share the port. When the challenge is TLS-ALPN-01, the resolver also answers ClientHellos that offer `acme-tls/1` with the challenge cert (rcgen acmeIdentifier extension).
- TLS-ALPN-01 needs the listener reachable on **:443**. HTTP-01 needs **:80**. Document both, plus the reverse-proxy / TCP-passthrough options.
- Renewal runs at 2/3 of the lifetime, or at the ARI window if the `time` feature is enabled.

`auto` keeps loopback plaintext so that Phase 4/5/6/8 tests and the CLI-01 ready line (`http://ADDR`) do not churn. **USER REVIEW (D-W8).** SEC-01's wording "self-signed in dev" is met for every non-loopback dev listener.

### Pattern 6: Effects of remote runs (D-W3, bridge-back)

- **Worker:** the worker's `RemoteSink::effect_driver()` returns an `EffectProxy`. `start(run, attempt, LiveEffect)` sends `StartEffect{token, op, redrive, redacted_values, http_retry}`. Header values that are redacted in the journal are the only request bytes the leader cannot read from the journal. Everything else is taken from the committed `EffectScheduled(op)`.
- **Leader:**
  1. Check that the session holds the lease for `token`. A zombie gets `FAILED_PRECONDITION`.
  2. Load `EffectScheduled(op)` for the run. It must exist and must not already be completed.
  3. Check that the names of the `redacted_values` equal the journal copy's `[redacted]` header names.
  4. Rebuild `LiveEffect{scheduled_ts_nanos: event.ts, redrive, ..}` and call the run's `PartitionSink::effect_driver()` → `ServerEffectDriver::start`.
- From there it is pure Phase 6: `EffectRegistry` attach-on-duplicate keyed by (partition, run, op), `PartitionSendGate`, secrets resolved per (run, attempt) in the bridge task, `CompleteEffect`, and `EffectHooks` (Phase 8 traceparent and spans).
- The completion becomes a journaled input. It reaches the worker through `Input` + `FetchJournal`, exactly as for embedded runs.
- **WORK-01 "per-(run, attempt) secret resolution"** is satisfied on the leader, scoped to the scheduling (run, attempt). Secret bytes never cross the stream; the canary test proves it. **USER REVIEW:** if worker-side effects are required, the fallback design is `ResolveSecret{session, run, token, name}` → value, with lease and grant checks, never cached, zeroized. That also needs a worker-side MetaView, egress and SendGate RPC (estimated +3 plans).

### Pattern 7: Placement, spill and selectors (WORK-03/04)

**Placement seam (replaces `slots: Arc<Semaphore>` in `DispatcherConfig`):**
```rust
// ostro-partition/src/leader/placement.rs
pub struct EntryView<'a> { pub queue: &'a str, pub workflow: &'a str, pub concurrency_key: &'a str,
                           pub priority: i32, pub first_activation: bool }
pub enum Reserve { Granted(Reservation), Blocked }
pub struct Reservation(Box<dyn std::any::Any + Send + Sync>); // RAII: dropping frees the slot/credit
pub trait Placement: Send + Sync + 'static {
    fn reserve(&self, e: &EntryView<'_>) -> Reserve;
    fn changed(&self) -> tokio::sync::watch::Receiver<u64>; // bumps when capacity may have appeared
}
pub struct SemaphorePlacement(Arc<Semaphore>); // exact 04-14 behaviour; DispatcherConfig::new uses it
```
`Job` gains `reservation: Reservation`, which the executor downcasts to `Embedded(OwnedSemaphorePermit)` / `Worker{session, credit}`. The reservation lives in the `Lease`, as the permit does today, so a revoked zombie holds no capacity.

**Server `NodePlacement`, per queue policy:**
- `embedded`: embedded slot or `Blocked`.
- `workers`: the session matching the selector with `free_credits > 0` and the most free credits (ties broken least-recently-assigned), else `Blocked` (counts toward demand).
- `auto`:
  - state `Embedded`: embedded slot, else `Blocked`;
  - state `Spilling`: a matching worker if one has credit, else an embedded slot.

**SpillController** (node-level signals plus queue lag; one state per `auto` queue):

| Signal | Source | Enter when | Exit when (hysteresis, D-W9: ×0.85) |
|--------|--------|------------|------------------------------------|
| CPU | `getrusage` Δ(utime + stime) / (Δwall × available_parallelism) | > 70 % sustained 10 s | < 59.5 % |
| Embedded slots | `1 − available/total` | > 90 % | < 76.5 % |
| Wasm memory | `runtime.parked().budget().used()/limit()` | > 85 % | < 72.25 % |
| Queue lag | now − oldest admissible `ready_at` in queue (dispatcher gauge) | > 5 s | < 4.25 s |

- `Spilling` → `Embedded` only after **every** signal has stayed below its exit threshold for the **30 s cool-down**.
- Sample every 1 s through `trait LoadSampler` (fake in tests, real in prod), with time from `tokio::time` so `tokio::time::pause()` drives the tests.
- Transitions are counted (`ostro_placement_transitions_total{queue,to}`) and logged.
- No migration: runs already on workers finish their activation there.

**Label selectors (WORK-04):** worker labels come from `--labels k=v,...` (the `Hello.labels`). The queue selector is a list of requirements with k8s semantics, all ANDed: `k=v`, `k==v`, `k!=v`, `k in (a,b)`, `k notin (a,b)`, `k` (exists), `!k`. Use a small hand-written parser and matcher (~150 lines + table tests). It is not worth a dependency: kube-core's selector type is behind Phase 6's optional `k8s` feature. A `workers` queue with no matching connected worker counts its backlog as demand (scale-from-zero).

### Pattern 8: Flow control (DISP-03/04/05)

**Admission seam:**
```rust
// ostro-partition/src/leader/flow.rs
pub enum Admit { Yes(FlowPermit), QueueBlocked { until: Option<Instant> }, KeyBlocked { until: Option<Instant> }, Paused }
pub struct FlowPermit(Box<dyn std::any::Any + Send + Sync>); // RAII: releases queue/key counters
pub trait FlowControl: Send + Sync + 'static {
    fn admit(&self, e: &EntryView<'_>, now: Instant) -> Admit;
    fn queue_weight(&self, queue: &str) -> u32;              // DISP-03 "priority" of a queue
    fn changed(&self) -> tokio::sync::watch::Receiver<u64>;  // policy edits, pause/resume, counter release
}
pub struct NoFlow; // admits everything; Phase 2–6 suites keep using it
```
The dispatch step is: pick a candidate (fair selection below) → `flow.admit` → `placement.reserve` → grant. A queue-level block skips the queue for this round. A key-level block skips the key. `until` feeds the existing delay wake-up (`sleep_until(min(...))`), so there is still **no interval** (`grep "interval("` stays empty). Both permits live in the `Lease` and are released with it.

**Node implementation (`ostro-server/src/flow`):**
- **Concurrency (queue and key):** node-wide `BTreeMap<queue, u32>` and `BTreeMap<(queue, key), u32>` counters behind one `Mutex`, shared by every partition dispatcher on the node. They count **leased activations** (runs executing now). Suspended or sleeping runs do not count; this matches Inngest/Hatchet semantics and docs/03 §5.4. On a single node the limits are exact. In a cluster they are per node until Phase 13 (document it).
- **Throttle (DISP-05, queue the excess):** a GCRA (governor) limiter per queue and per (queue, key), checked **only for first activations** (`record.next_seq == 0`). Re-activations of running runs are never throttled. Excess runs stay in the ready queue, FIFO within their priority band, and wake at `NotUntil`. This matches Inngest's "throttle queues runs … caps starts within each period at limit + burst".
- **Rate limit (DISP-05, reject the excess):** a GCRA per queue and per (queue, key) at **StartRun admission** in the server's start path, before the proposal. Excess starts get HTTP 429 / gRPC `RESOURCE_EXHAUSTED`, `code = "rate_limited"`, `Retry-After`. Nothing is enqueued. This is Inngest's rateLimit semantics, but loud, not silently dropped. Idempotent re-submits (an existing dedupe hit) are not charged. Webhook and cron starts (Phase 8) go through the same ControlPlane start path, so they are limited too. Document this.
- The flow key is the run's `concurrency_key`, so one key drives keyed concurrency, keyed throttle and keyed rate limit (StartRun already has field 8). An empty key means "unkeyed".

**Fair selection (D-W10), in `leader/fair.rs`, pure and unit-tested:**
```
queues:   weighted DRR; quantum(queue) = flow.queue_weight(queue) (default 1)
in queue: priority bands BTreeMap<Reverse(priority), Band>; always serve the highest non-empty band
in band:  DRR ring over concurrency keys (quantum 1); each key = BTreeSet<(ready_at, run)> FIFO
          a KeyBlocked key leaves the ring until a counter release or until `until` passes (changed())
```
Priority keeps its meaning ("higher first"), and fairness applies among keys of equal priority. With no keys, the queue behaves exactly like 04-14 (strict priority, then ready_at, then run id). Global fairness across partitions is approximate: each partition is fair locally, the node-wide key counters bound every key, and keys hash across partitions. Measure it with Jain's index in 07-17.

**Rebuild bound:** 04-14 notes that `ready_entries(usize::MAX)` loads the whole backlog. Keep that for Phase 7 (correctness first), but record the backlog size at rebuild (`ostro_dispatcher_rebuild_entries`) and the time it took. A bounded window is a Phase 13 item.

### Pattern 9: Pause, resume, auto-pause (OPS-01) and the D1 interplay

| Target | Mechanism | Effect |
|--------|-----------|--------|
| Run | `Cmd::PauseRun` / `ResumeRun` (partition log) | Apply sets or clears `RunRecord.pause` (F1). **Paused:** `enqueue_ready` writes no ready row and emits no `EnqueueReady`, but the status still becomes Ready (so resume knows), and an existing ready row is removed. `Action::RunPaused{run}` makes the dispatcher drop the queue entry and **revoke** any lease; the executor self-fences. EventsAppended/Suspend/EnqueueReady from an executor are rejected `RunPaused`, and the executor maps that to `Released`. Inputs (TimerFired, CompleteEffect, signals, tokens) are still journaled; they just wake nothing. **Resume:** clear the mark; if status is Ready or Running, `enqueue_ready` (leave_running + row + action), so the dispatcher grants n+1 and resets the in-memory `failures`. Pause and resume are idempotent: pause on a paused run returns Accepted with no change. Both are `RunTerminal` on terminal runs. |
| Workflow | `WorkflowPolicy.paused` (F2) via meta CAS | `FlowControl::admit` returns `Paused` for its entries (needs F3's workflow on the entry). Entries stay queued, and resume wakes the dispatchers through `changed()`. In-flight activations finish their current activation. |
| Queue | `QueuePolicy.paused` (F2) | The same, at queue level. |
| Auto-pause | Policy `on_retries_exhausted = "pause"` (queue or workflow; workflow wins). Default `fail` = Phase 5 behaviour. | In the outcome mapper's "AttemptFailed, retryable, failures exhausted" branch: instead of `CompleteRun{Failed, resumable}`, submit `PauseRun{token, reason: retries_exhausted, detail: failure JSON}`. Fenced on token, so only the current attempt can auto-pause. Non-retryable failures (NonDeterminism, ProtocolViolation) still fail. |

**D1 interplay** (Phase 5 resume-in-place for `resumable` terminals vs auto-pause):
- Auto-pause keeps the run **non-terminal**. Its waits, timers and effects rows stay armed, so no D1 re-arm is needed. Parents keep waiting, and `resumed_count` is not incremented.
- The **run timeout keeps ticking while paused** (D2 hard stop). A paused run that times out becomes `TimedOut` (resumable), and from then on D1 `/retry` applies.
- `/retry` on a **paused** run: recommend treating it as `/resume` (200, same effect), since operators will reach for retry. `/resume` is canonical.
- `/cancel` on a paused run journals CancelRequested but does not resume. The guest can only run cleanup after `/resume`. Document this. `/terminate` works immediately.
- Auto-pause applies only to executor-level retryable exhaustion (the 04-16 `max_attempt_failures`). Guest step retries that end in a guest-visible error are the workflow's own business.

### Pattern 10: Worker death, re-dispatch and fencing tokens

| Event | Detection | Action | Bound |
|-------|-----------|--------|-------|
| Worker kill -9, or stream reset | h2 stream error at once; keepalive 5 s ping / 5 s timeout across networks | The registry marks the session dead; every RemoteJob future returns `ExecOutcome::Abandoned` → dispatcher drops the lease and re-grants n+1 **immediately, without counting a failure** | ≪ lease_ttl |
| Worker stalled (SIGSTOP, GC, CPU starvation) with the stream up | No Heartbeat renewals | The dispatcher lease expires (04-14) → revoke + n+1. The server sends `Cancel`. The zombie's later Append (token n) → apply `Fenced`, and its StartEffect → lease check fails. Even a StartEffect that won a race would attach to the leader's registry entry for (partition, run, op), so there is no second send. | ≤ lease_ttl |
| Worker loses its stream | Worker side | Cancel every activation at once (self-fence). Reconnect with jittered backoff. | immediate |
| Leader node crash | Workers see the stream drop | Workers cancel everything. After restart, the dispatcher rebuild re-grants every Running run under the new epoch at once (04-14), and P-9 re-wakes in-flight effects. | restart time |
| Drain (SIGTERM, KEDA scale-down) | `Drain` message | Server stops assigning to the session; the worker evicts in-flight activations (`Done{Evicted}` → `Reactivate`/`Abandoned` elsewhere) within `terminationGracePeriodSeconds`. | grace period |

**Fencing tokens, layer by layer:**
1. `AttemptToken(epoch, n)` on every journal write, checked by apply. This is the only correctness fence.
2. `(session_id, lease generation)` checked by the server before touching the partition (defence in depth, cheaper rejects).
3. Effects: the leader's registry keyed by (partition, run, op), plus `SendGate` (run alive, effects row present, this node leads), plus the stable idempotency key for the external system. `CompleteEffect` is attempt-independent and first-wins.

A Kleppmann-style fencing token sent to the external API is not possible for payment providers. The idempotency key is the external fence.

### Pattern 11: Demand metric and KEDA (WORK-05)

**Corrected definition (D-W12):**
```
demand(q) = running_on_workers(q) + admissible_backlog_for_workers(q)
  workers queue: admissible_backlog = ready entries that pass flow admission (not paused/throttled/
                 concurrency-blocked; approximated as min(ready, concurrency_headroom))
  auto queue:    the same, but only while the queue is Spilling (else 0) + running_on_workers
  embedded:      0
```
More workers cannot help runs blocked by a concurrency limit, a throttle or a pause, so those are excluded. Running work is included, so the HPA does not scale in under active load.

- The gauge `ostro_queue_worker_demand{queue}` is set every 1 s by `workers/demand.rs`, which aggregates the per-partition dispatcher gauges. 08-16 only *describes* the metric and defines it at 0; Phase 7 owns setting it (see Coordination).
- Extra series: `ostro_queue_worker_backlog`, `ostro_workers_connected`, `ostro_worker_slots_free`.
- `GET /v1/queues/{q}/demand` → `{"queue","demand","backlog","running_on_workers","placement","spilling","workers","worker_slots_free"}`. Auth is a bearer token scoped `queues:read`. Token enforcement arrives in Phase 14, so document it.

```yaml
# deploy/keda/scaledobject-metrics-api.yaml (single node)
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: ostro-workers-heavy }
spec:
  scaleTargetRef: { name: ostro-worker-heavy }
  minReplicaCount: 0          # KEDA default; scale to zero
  maxReplicaCount: 50
  pollingInterval: 15         # default 30 s
  cooldownPeriod: 120         # default 300 s; applies ONLY to N→0
  triggers:
    - type: metrics-api
      metadata:
        url: "https://ostro.ostro.svc:7700/v1/queues/heavy/demand"
        valueLocation: "demand"
        targetValue: "8"           # ≈ worker --slots
        activationTargetValue: "0" # activate when demand > 0 (strict >)
        authMode: "tls"            # or bearer; TriggerAuthentication with ca
      authenticationRef: { name: ostro-demand-auth }
```
For clusters (Phase 12+), use the `prometheus` scaler with `sum(ostro_queue_worker_demand{queue="heavy"})`, because each node exports demand only for the partitions it leads.

**Local test path (no Docker, kind or k3d):** `crates/ostro/tests/keda_sim.rs` runs a KEDA simulator. It polls the demand endpoint every 500 ms, applies `activation: demand > activationTarget`, `desired = clamp(ceil(demand / target), min, max)` and a cool-down for N→0, and spawns or SIGTERMs real `ostro worker` processes. It asserts 0 → N (N ≥ 2 under a burst of a `workers` queue) → 0 after the burst drains and the cool-down passes, with every run completed exactly once. **human_needed:** a real KEDA run. Options:
1. The user's microk8s context, which kubectl reaches (`dell-1`, `dell-2`, v1.32). It needs KEDA installed via helm and a worker image. Never touch it without explicit user approval.
2. An optional GitHub Actions job using `helm/kind-action` + the KEDA chart + the musl binary in a scratch image, `workflow_dispatch` only.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| X.509 certs, CSRs, CA signing | DER/ASN.1 by hand | rcgen 0.14 (+ x509-parser) | SAN/EKU/basic-constraint encodings are easy to get subtly wrong, and verifiers reject them. |
| ACME protocol | JWS/nonce/order state machine | instant-acme 0.8 | Nonce retries, `badNonce`, Retry-After and account key rollover are all handled. |
| TLS accept + ALPN for axum/tonic one-port | Raw accept loop | axum-server (tls-rustls-no-provider) | Graceful shutdown, connection tracking and HTTP/2 upgrade. |
| GCRA / token bucket | Ad-hoc counters | governor 0.10 (already pinned by P6) | Correct burst semantics and `NotUntil` wait times; P6 already uses it. |
| Exactly-once effects for remote runs | A second effect runner on workers | Phase 6 `ServerEffectDriver` via StartEffect | Registry dedupe, SendGate, CompleteEffect and EffectHooks already exist and are tested. |
| Fencing | A new "worker epoch" in apply | Existing `AttemptToken` + apply token check | The 04-15 "no second fencing check" rule. |
| Intent derivation for remote appends | Worker-supplied intents | Server-side `derive` in `PartitionSink` | A compromised worker could otherwise forge timers or completions. |
| Payload offload for remote appends | Worker-side blob upload protocol | `PartitionSink::append` (prepare_payload) after frame reassembly | One code path; the BlobRef invariant (D18) holds automatically. |
| AOT cache integrity on workers | New cache | `ostro-engine` L1 cache with its node-local HMAC key | Already TOCTOU-safe (verify in memory, deserialize from bytes). |
| Mock payment API | New mock | ostro-testkit `MockProvider /pay` (06-10) | Key dedupe, per-key counters and 409 in-progress already exist. |
| Process CPU% | `/proc` parsing / sysinfo | `libc::getrusage` + `available_parallelism()` | cgroup-aware denominator, portable to macOS, tiny. |
| SIGSTOP/SIGCONT in tests | `nix` dependency | `std::process::Command::new("kill").args(["-STOP", pid])` | No new dev dependency. |

**Key insight:** in this phase almost every "new" capability already exists on the leader: fencing, derive, effects, secrets, offload and the AOT cache. The worker is valuable exactly to the extent that it duplicates nothing except Wasm CPU.

## Common Pitfalls

### Pitfall 1: The slot semaphore and placement become two sources of truth
**What goes wrong:** the dispatcher takes an embedded permit, then the executor decides to send the job to a worker. Embedded capacity gets consumed by remote work, and an `auto` queue can never spill when embedded is full.
**How to avoid:** a single `Placement::reserve` before the grant. The reservation travels in the Lease and the Job. `SemaphorePlacement` preserves the 04-14 behaviour for the existing suites.
**Warning signs:** `slots.available_permits()` drops while every job runs on workers.

### Pitfall 2: Grant before admission wastes attempts
**What goes wrong:** granting `AttemptGranted n+1` and only then discovering a pause or flow block. That burns attempt numbers, writes log entries and wakes zombies.
**How to avoid:** admission (pause/flow) and reservation happen **before** `grant_attempt`. Only stale-state races (pause applied after admission) reach apply, which rejects them with `RunPaused`.

### Pitfall 3: Worker loss counted as an attempt failure
**What goes wrong:** returning `ExecOutcome::Retry` on stream loss increments `failures`, so three worker restarts fail a healthy run (or auto-pause it).
**How to avoid:** add `ExecOutcome::Abandoned` (re-grant now, no failure count, no delay) to the dispatcher contract. `Released` would strand the run as Running until the next leader restart.

### Pitfall 4: Demand that oscillates
**What goes wrong:** demand = unassigned backlog only. KEDA scales up, workers drain the backlog, demand hits 0 and the HPA scales in during active work.
**How to avoid:** demand includes `running_on_workers` (D-W12). The cooldown applies only to N→0. Test it in keda_sim.

### Pitfall 5: Engine limits differ between embedded and workers
**What goes wrong:** a worker with a different `max_memory_bytes`, fuel defaults, step-timeout defaults or Wasmtime build fails (`memory_exceeded`) where embedded succeeds, or behaves differently on replay.
**How to avoid:** `Welcome.limits` carries the server's engine limits and the worker uses them, not local config. `Hello` must match `ostro_version`, `wasmtime_version` and `event_codec_ver` exactly (`[workers] require_same_version = true`; N/N-1 comes in Phase 13). A mismatch is refused with a clear error.

### Pitfall 6: Untrusted worker bytes reach apply
**What goes wrong:** malformed event bytes, a forged `from_seq`, oversized frames, or a request for a module of a run the worker does not lease.
**How to avoid:**
- Decode every event server-side before `PartitionSink::append`, and reject on a decode error.
- Enforce frame and append caps.
- Make every bulk RPC lease-scoped.
- Have StartEffect read the request from the journal (Pattern 6).
- Never accept `Derived` intents from the wire.

### Pitfall 7: The `run-info.attempt` replay hazard (F4)
**What goes wrong:** a guest branches on `info().attempt`, and replay after a worker failover diverges, so the run fails with NonDeterminism.
**How to avoid:** the F4 doc fix pre-freeze, plus an SDK note. Add a replay test showing a guest that only *logs* the attempt stays deterministic across a forced re-grant.

### Pitfall 8: Pause races
**What goes wrong:** pause applied between admission and grant; resume lost because the dispatcher dropped the entry; inputs while paused re-inserting ready rows.
**How to avoid:**
- `enqueue_ready` is the single choke point, with the pause check inside it.
- Apply rejects executor appends with `RunPaused`.
- `ResumeRun` re-enqueues from the record state.
- The dispatcher handles `Action::RunPaused` (revoke + drop) and `Action::EnqueueReady` after resume.
- Proptest the sequence pause/inputs/resume in rebuild_digest (`gen_pause.rs`).

### Pitfall 9: Throttle applied to re-activations
**What goes wrong:** a long run that evicts and resumes is throttled on every resume, so throttle turns into a throughput cap on in-flight work.
**How to avoid:** throttle and rate limit only first activations and starts (`next_seq == 0`), per the Inngest semantics. Concurrency counts every leased activation.

### Pitfall 10: The tonic TLS config is static
**What goes wrong:** expecting live cert rotation on :7701 or for the worker identity.
**How to avoid:** server certs last 1 year (a restart is fine). The worker renews by building a **new** Channel with the new identity, opening a new Session, sending `Drain` on the old one and then closing it. Test renewal with a 10 s lifetime.

### Pitfall 11: CPU sampling inside containers
**What goes wrong:** host-wide CPU (sysinfo) reads 20 % on a 64-core node while the pod is throttled at its 2-CPU quota, so the queue never spills.
**How to avoid:** process CPU time over `available_parallelism()`, which honours cgroup v1/v2 quotas.

### Pitfall 12: Shared-target build interference (seen in 04-14)
**What goes wrong:** parallel executors' worktrees overwrite each other's artifacts in `target/`.
**How to avoid:** spawned-binary suites (worker e2e, chaos, keda_sim) use `CARGO_TARGET_DIR=target/p7` or a per-worktree dir. The failpoint binary uses `target/failpoints` (05 convention).

### Pitfall 13: Treating "exactly one call per key" literally under leader crashes
**What goes wrong:** the leader dies after the mock processed `/pay` but before `CompleteEffect` commits. The FX-03 re-drive must resend with the same key (an unknown outcome), so the raw request count for that key is 2.
**How to avoid:** define QA-03 as **exactly one charge per key** (`MockProvider::charges(key) == 1`), 0 lost effects, and 0 sends from fenced sessions. Report raw `requests(key) > 1` and attribute every such key to a leader kill inside its send window or to a 409-in-progress retry. **USER REVIEW (D-W11).** With D-W3, worker and executor zombies cause **zero** extra raw requests. Only leader kills can.

## Code Examples

### Outcome mapping shared by embedded and remote runners (refactor target)
```rust
// crates/ostro-server/src/executor.rs (04-16), made pub(crate), not moved (minimal diff; see Coordination)
pub(crate) async fn finish_activation<L: ReplicatedLog>(
    sink: &PartitionSink<L>, job_failures: u32, cfg: &ExecutorConfig,
    outcome: ActivationOutcome, engine_end_seq: u64, revoked: bool,
    on_exhausted: ExhaustAction,           // Phase 7: Fail (P5 default) | Pause
) -> ExecOutcome { /* the 04-16 table; Exhausted+Pause => submit PauseRun{token, retries_exhausted} => Released */ }
```

### Dispatcher step (sketch)
```rust
// crates/ostro-partition/src/leader/dispatcher.rs (Phase 7)
while let Some(cand) = state.fair.next_candidate() {          // weighted DRR → band → key DRR
    let view = cand.view();
    let flow_permit = match cfg.flow.admit(&view, now) {
        Admit::Yes(p) => p,
        Admit::QueueBlocked { until } => { state.fair.skip_queue(cand.queue, until); continue }
        Admit::KeyBlocked { until }   => { state.fair.park_key(cand.queue, cand.key, until); continue }
        Admit::Paused                 => { state.fair.skip_queue(cand.queue, None); continue }
    };
    let Reserve::Granted(res) = cfg.placement.reserve(&view) else { break }; // wait on placement.changed()
    state.fair.take(cand);
    self.start_grant(cand.run, cand.entry, flow_permit, res);  // lease holds both RAII permits
}
```

### GCRA throttle with governor (MEDIUM: confirm the trait names in ~/.cargo/registry)
```rust
// ostro-server/src/flow/limits.rs
let quota = governor::Quota::with_period(period / limit).unwrap().allow_burst(NonZeroU32::new(burst).unwrap());
let lim = governor::RateLimiter::direct_with_clock(quota, TokioClock::default()); // TokioClock: governor::clock::Clock over tokio::time::Instant
match lim.check() { Ok(()) => Admit::Yes(..), Err(not_until) => Admit::QueueBlocked { until: Some(to_tokio(not_until.earliest_possible())) } }
```

### Spawned zombie in tests
```rust
let pid = worker.id().to_string();
std::process::Command::new("kill").args(["-STOP", &pid]).status()?;   // zombie: stream up, no heartbeats
tokio::time::sleep(lease_ttl + Duration::from_millis(500)).await;     // successor granted n+1
std::process::Command::new("kill").args(["-CONT", &pid]).status()?;   // late appends/effects must be fenced
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| rustls-acme as the default Rust ACME choice | instant-acme (low-level, no chrono); tokio-rustls-acme fork for an all-in-one resolver | rustls-acme 0.15 still pins chrono and rcgen 0.13 (checked 2026-11-19) | ACME must use instant-acme under the chrono ban. |
| rcgen ring default | rcgen 0.14 `aws_lc_rs` feature | rcgen 0.14 | Defaults must be turned off to keep ring out. |
| KEDA metrics-api without activation | `activationTargetValue` (strictly greater than) | KEDA 2.x; docs current at 2.21 (latest) | Use 0 for scale-from-zero on any demand. |
| Inngest-style "rate limit = drop silently" | Ostro rate limit = loud 429 | design choice | Callers see the rejection; throttle is the queueing alternative. |

**Deprecated/outdated:**
- docs/03 §8 demand definition: replaced by D-W12.
- docs/03 §5.3 "ResolveSecret over the stream": superseded by D-W3 unless the user overrides.
- docs/03 §5.3 single bidi stream carrying `BlobChunk`/`Activate{journal_page}`: replaced by the control stream plus bulk RPCs (Pattern 2).

## Proposed Plan Breakdown (19 plans, 8 waves)

| Plan | Wave | Depends on | Content | Reqs |
|------|------|-----------|---------|------|
| **07-01** Gate + reservations | 1 | Phase 6 complete | Verify the Phase 6 gate and baseline. Record which of F1–F4 landed. Docs-only RESERVATIONS commit (Cmd 20/21, RejectReason 15 + placeholder note, keyspace/meta per F1/F2, §11 names). Copy D-W1..D-W14 to STATE.md. | all |
| **07-02** Contracts + deps | 2 | 07-01 | **Only plan touching Cargo files.** Pins (rcgen, x509-parser, axum-server, instant-acme, tonic features, libc). New crates `ostro-worker-proto` (full execute.proto) and `ostro-worker` (module stubs). PauseRun/ResumeRun prost + golden append. RejectReason placeholders + `RunPaused`. Meta variants + META_FORMAT bump (only if F2 is rejected). Config types `[workers] [worker] [cluster] [api.tls] [flow]`. `ostro-server/src/p7.rs` seam + module stubs (`workers/`, `flow/`, `tls/`) declared once in lib.rs. The single server.rs edit (calls `p7::start`, `tls::listener::serve`, `p7::executor`). | all |
| **07-03** Pause apply | 3 | 07-02 | `apply/pause.rs`, the `enqueue_ready` gate, `RunPaused` rejects, `Action::RunPaused/RunResumed`, rebuild_digest `gen_pause.rs`, storage-format doc. | OPS-01 |
| **07-04** Dispatcher generalization | 3 | 07-02 | `placement.rs`, `flow.rs`, `fair.rs`; dispatcher uses the seams; `ExecOutcome::Abandoned`; pause handling; per-queue gauges (ready, admissible, lag, on-workers) exposed as an `Arc<QueueGauges>`. 04-14 suite green with `SemaphorePlacement + NoFlow`. **Sequenced with 05-11 (done by then) and 08-16 (must not run in the same window).** | DISP-03/04/05, WORK-03 |
| **07-05** TLS foundation | 3 | 07-02 | `tls/{ca,dev_cert,resolver,listener}`: cluster CA, leaf issuance, CSR signing, client-listener modes off/auto/self-signed/files with the ArcSwap resolver and ALPN h2+http/1.1, axum-server serve. | SEC-01 |
| **07-06** Worker crate core | 3 | 07-02 | `ostro-worker`: session client, RemoteSink, journal/blob fetch with verification, append framing, EffectProxy, self-fence, drain, AOT cache dir. Tests against an in-crate fake server (tonic in-process). | WORK-01/02/06 |
| **07-07** Node flow control | 4 | 07-04 | `flow/`: policy view from meta (QueuePolicy/WorkflowPolicy or the P7 variants), node-wide counters, governor throttles with TokioClock, StartRun rate limit (429) in the server start path, `on_retries_exhausted` lookup. | DISP-03/04/05, OPS-01 |
| **07-08** Worker service (server side) | 4 | 07-04, 07-05 | `workers/{registry,service,join}`: cluster listener (tonic mTLS), Join/Renew, session lifecycle, credits, heartbeats → lease renew, lease-scoped FetchJournal/FetchBlob, token revocation sweep, version checks. | WORK-01, SEC-01 |
| **07-09** RemoteRunner + PlacementExecutor | 5 | 07-06, 07-08 | `workers/remote_executor.rs`, `workers/placement.rs`, `workers/selector.rs`; executor.rs helpers made pub(crate); Append reassembly → PartitionSink; ordered input bridge; StartEffect verification → ServerEffectDriver; guest-log forwarding (08-17 `guest_log`); traceparent. | WORK-01/02/04/06 |
| **07-10** Spill controller | 5 | 07-04, 07-08 | `workers/spill.rs`, LoadSampler (getrusage, slots, Wasm budget, lag), hysteresis state machine, transition metrics; tests with a fake sampler and paused time. | WORK-03 |
| **07-11** ACME | 4 | 07-05 | `tls/acme.rs`: instant-acme account/order/finalize, TLS-ALPN-01 resolver hook and HTTP-01 responder, renewal scheduler, cache; fake-ACME unit tests; Pebble test env-gated (`OSTRO_TEST_ACME_DIRECTORY`). | SEC-01 |
| **07-12** API surface | 4 | 07-02 | proto (`ostro.api.v1.Queues`, `Workers`), ostro-api domain/REST/gRPC for flow, pause/resume ×3, demand, workers list/drain/tokens, join; FakeControlPlane, parity, OpenAPI. ControlPlane methods have default unimplemented impls (P-16 pattern). **Sequenced against 08-10, which owns ostro-api in Phase 8 wave 3.** | OPS-01, WORK-05, DISP-03 |
| **07-13** Control-plane impl + CLI | 6 | 07-03, 07-07, 07-09, 07-12 | Server ControlPlane methods (meta CAS, PauseRun/ResumeRun submit, auto-pause wiring in the outcome mapper); `ostro worker` subcommand and the other CLI verbs (main.rs: **sequenced against 08-12/08-18**). | OPS-01, WORK-01 |
| **07-14** Demand + KEDA artefacts | 6 | 07-09, 07-10, 07-12 | `workers/demand.rs` gauge + endpoint, deploy/keda + deploy/k8s YAML, keda_sim test (spawned workers 0→N→0). | WORK-05 |
| **07-15** SC1 e2e | 7 | 07-11, 07-13 | Spawned `ostro server` + `ostro worker --join`: mTLS, CA-hash pinning, a revoked token closes the session, small-frame paging test, module fetch + AOT cache hit on worker restart, the canary secret never on the stream or worker logs, client listener self-signed/files/reload, ACME against Pebble (env-gated). | WORK-01/02/06, SEC-01 |
| **07-16** SC2 e2e | 7 | 07-13, 07-14 | Auto spill under a cpu-burn load and return after the cool-down with transitions == 2; oscillating load → 0 flaps; selectors route (gpu=true queue only to the labelled worker; no match → demand); kill -9 worker → n+1 within lease_ttl (measured); SIGSTOP zombie fenced. | WORK-03/04 |
| **07-17** SC4 e2e | 7 | 07-13 | Queue concurrency cap, queue weights (dispatch share ratio), run priority bands, keyed fairness (Jain ≥ 0.9 across 10 keys with 1 hot key), throttle spacing vs 429 rate limit, pause/resume run/workflow/queue, auto-pause + resume + `/retry` alias + timeout-while-paused. | DISP-03/04/05, OPS-01 |
| **07-18** SC3 fencing chaos | 7 | 07-13 | `crates/ostro/tests/fencing_chaos.rs` (`#[ignore]`) + `scripts/fencing-chaos.sh`: sharded and resumable (05 campaign pattern), 10,000 runs, fault mix, assertions per D-W11, report file. | QA-03 |
| **07-19** Phase gate | 8 | all | docs/workers.md, flow-control.md, tls.md; docs/03 §5.3/§8 and docs/02 corrections (F6); ci-local + ci.yml (FAILPOINT_CRATES gains ostro-worker if it has failpoints; optional pebble + kind/KEDA dispatch jobs); human_needed list; full `bash scripts/ci-local.sh`; RESERVATIONS verification. | all |

Wave 3 runs four plans on disjoint files (07-03 partition apply, 07-04 dispatcher, 07-05 tls/, 07-06 ostro-worker). That is the main parallelism win.

## Cross-Phase File Coordination (Phases 6 and 8)

Phase 6 is complete before Phase 7 starts, so its files are **inputs to read on HEAD**, not contention. Phase 8 runs in parallel.

| File | Phase 6 (done) | Phase 8 (parallel) | Phase 7 | Rule |
|------|---------------|--------------------|---------|------|
| `crates/ostro-partition/src/leader/dispatcher.rs` | — | **08-16 (w4)**: queue gauges; `ostro_queue_worker_demand` "defined at 0 for Phase 7 to set" | **07-04** rewrites selection and capacity | Never in the same window. Whoever is second rebases. Ask 08-16 to only `describe_gauge!` the demand metric, not set it, because Phase 7's demand.rs owns its value. Phase 7 exposes `QueueGauges` so 08-16's instrumentation lands in one place. |
| `crates/ostro-server/src/executor.rs` | — (06-14 does not touch it) | **08-04 (w2)** GuestLogSink param, **08-11 (w3)** spans, **08-17 (w4)** guest_log | **07-09** makes helpers pub(crate); **07-13** adds the auto-pause branch | Do **not** move the file to `executor/mod.rs`; take minimal diffs on a clean HEAD. 07-09 must re-read the post-08-17 shape. |
| `crates/ostro-server/src/sink.rs` | 06-14 effect_driver, B1 | 08-17 guest_log | Read-only (the remote path constructs `PartitionSink` exactly as the embedded executor does) | If a constructor changes, follow it. Never edit sink.rs in Phase 7. |
| `crates/ostro-server/src/effects.rs` | 06-14 (`ServerEffectDriver`, `EffectHooks`) | 08-21 installs hooks (one line) | Read-only (StartEffect calls `ServerEffectDriver::start` through the run's sink) | Keep the hook signatures. |
| `crates/ostro-server/src/server.rs` | 06-14 BridgeServices | 08-04 services seam; then only 08-10 (w3) and 08-14 (w4) | **Only 07-02** (one seam call block) | Same seam pattern as P8-D21(c). |
| `crates/ostro-server/src/config.rs` | 06-14 sections | 08-04 (w2) sections | **Only 07-02** | Append sections; never reformat. |
| `crates/ostro-server/src/lib.rs` | 06-14 | 08-04, 08-21 | **Only 07-02** (declares p7 modules) | Append-only. |
| `crates/ostro-api/src/{lib,domain,rest,grpc,openapi}.rs`, `crates/ostro-proto/{build.rs,src/lib.rs}`, OpenAPI snapshot, parity suite | 06-08 | **08-10 (w3)** owns them for Phase 8 | **07-12** | Not concurrently with 08-10; re-bless the OpenAPI snapshot only for Phase 7 paths. Worker proto lives in its **own crate** (`ostro-worker-proto`) to stay out of ostro-proto's build list. |
| `crates/ostro/src/main.rs` | 06-16 CLI verbs | **08-12 (w4)**, **08-18 (w5)** | **07-13** | Sequence. Add verbs as separate modules where possible. |
| `crates/ostro-server/src/control/runs.rs` (server start path, 04-18/05-13) | — | 08-15 webhooks call ControlPlane start | 07-07 rate-limit hook | Insert one admission call; webhooks and cron inherit it (documented). |
| `crates/ostro-types/src/command.rs` + `tests/golden/commands.txt` | 06-02 (Cmd 13) | 08-01 (Cmd 27) | 07-02 (Cmd 20/21) | Append-only by tag. |
| `apply/mod.rs` (Cmd dispatch), `apply/outcome.rs` (RejectReason/InvalidCode) | 06-02/06-03 | 08-01, 08-07 | 07-02 (enum), 07-03 (dispatch arm) | Placeholders `Reserved<Phase>_<index>` per P-15; never renumber. |
| `store/mod.rs` `TABLES` | — | 08-01 (16 `schedule_state`) | 07-02 only if F1 is rejected (17 + placeholder 16) | Placeholder rule §6. |
| `Cargo.toml`, `Cargo.lock`, member Cargo.tomls, `deny.toml` | 06-02 | 08-01, 08-03, 08-12 | **07-02 only** | Clean-HEAD rule. 08-01 enabling otlp `tls-aws-lc` also turns on tonic `tls-aws-lc`; no conflict. |
| `scripts/ci-local.sh`, `.github/workflows/ci.yml` | 06-18 | 08-01, 08-03, 08-20 | 07-19 | Append steps; `FAILPOINT_CRATES` in sync. |
| `crates/ostro-testkit/**` | 06-02/06-10/06-19 | 08-01 | Read-only use of `mock-http` and `canary` | Add nothing unless needed; if needed, a new feature is declared in 07-02. |
| `.planning/RESERVATIONS.md`, `.planning/STATE.md` | 06-01 | 08-01, 08-22 | 07-01, 07-19 | Docs-only commits, clean HEAD. |
| `guests/Cargo.lock` | 06-09 | 08-17, 08-20 | one Phase 7 plan per wave (pay-flow, long-journal, cpu-burn guests: 07-06 or 07-09) | One owner per wave. |

Phase 8 expectations Phase 7 must meet: a `traceparent` field on Activate, with the worker setting the activation span context (08 "Cross-phase seams"); guest logs from remote runs reaching the server's `GuestLogSink` (07-09); and the demand gauge set by Phase 7 (above).

## Open Questions / Decisions to Confirm (D-W)

| ID | Topic | Recommendation | Status |
|----|-------|----------------|--------|
| D-W1 | Pre-freeze F1–F4 | Accept all four into Phase 5 now | **send to orchestrator** |
| D-W2 | Worker transport topology | Control stream + bulk RPCs on the cluster listener :7701 (mTLS); join on :7700 | adopt |
| D-W3 | Effects of remote runs | Bridge-back to the leader (secrets never on workers). WORK-01 secret wording met on the leader | **USER REVIEW** |
| D-W4 | Worker identity | kubeadm-style token + CA-hash join, 24 h client certs, revocation through token deletion; cert-manager flags as alternative | adopt |
| D-W5 | Cluster CA storage | `<data_dir>/tls/` files (0600) in Phase 7; Phase 12 decides distribution (sealed meta secret candidate `__ostro/cluster-ca`) | adopt, note for P12 |
| D-W6 | ACME crate | instant-acme 0.8.5; fallback tokio-rustls-acme 0.10.0 | adopt |
| D-W7 | Listener TLS server | axum-server 0.9.2 `tls-rustls-no-provider` + ArcSwap resolver | adopt |
| D-W8 | Dev TLS default | `auto`: loopback plaintext, non-loopback self-signed (dev) / required (server) | **USER REVIEW** |
| D-W9 | Hysteresis | Exit thresholds = entry × 0.85 for every signal; 30 s cool-down; CPU sustain 10 s | adopt |
| D-W10 | Fairness algorithm | Weighted DRR across queues → priority bands → DRR across keys (quantum 1) | adopt |
| D-W11 | QA-03 interpretation | Exactly one **charge** per key; 0 lost effects; 0 fenced-session sends; raw extra requests reported and attributed to leader kills / 409 retries | **USER REVIEW** |
| D-W12 | Demand definition | running_on_workers + admissible backlog for workers; fix docs/03 §8 | adopt |
| D-W13 | Concurrency unit | Leased activations (not suspended runs); limits node-wide until Phase 13 | adopt |
| D-W14 | Lease TTL default | Keep 30 s; amend NFR-3.5 text (15 s) in the 07-19 docs pass | adopt (doc change) |

Remaining unknowns:
1. **Exact instant-acme 0.8 API** (account builder, challenge handles, ARI). Known: the crate's types (docs.rs index). Unclear: method names. Recommendation: 07-11 Task 1 reads `~/.cargo/registry/src/*/instant-acme-0.8.5/examples/` and records the API in its SUMMARY.
2. **Whether Pebble builds with the local Go 1.23.10.** Pebble v2 may require a newer Go. Recommendation: try `go install github.com/letsencrypt/pebble/v2/cmd/pebble@latest` in 07-11. If it fails, the Pebble leg is CI-only/human_needed. Use `PEBBLE_VA_ALWAYS_VALID=1` for the order flow, plus one real HTTP-01 validation run.
3. **governor custom clock shape.** If wrapping a tokio Instant as `governor::clock::Reference` is awkward, use `FakeRelativeClock` in tests and `MonotonicClock` in prod, and drive the throttle tests with explicit clock advances instead of `tokio::time::pause()`.
4. **08-17's `guest_log` signature** at the time 07-09 runs. If 08-17 has not landed, 07-09 forwards logs to the server's existing tracing emission and records a follow-up.
5. **The microk8s cluster** reachable through kubectl may belong to other workloads. Never use it without explicit user approval (human_needed only).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | `cargo test` (libtest), proptest 1.12 (rebuild_digest `gen_pause.rs`), insta (OpenAPI), `fail` 0.5.1 failpoints (`worker::`, `placement::`, `flow::` via a `p7_fail!` macro, same rules as P-13), spawned binaries via `CARGO_BIN_EXE_ostro` (failpoint build in `target/failpoints`), ostro-testkit `mock-http` + `canary` |
| Config file | none beyond Cargo.toml; `FAILPOINT_CRATES` gains `ostro-worker` if it has failpoint sites (07-19 syncs ci.yml) |
| External services | none required locally. Pebble ACME env-gated (`OSTRO_TEST_ACME_DIRECTORY`); real KEDA human_needed / optional CI dispatch job |
| Guest build | `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` (new: `pay-flow`, `long-journal`, `cpu-burn`; reuse `retry-probe` for auto-pause) |
| Quick run command | `cargo test -p ostro-partition --test dispatcher --test dispatcher_flow --locked && cargo test -p ostro-worker --locked` |
| Full suite command | `bash scripts/ci-local.sh --no-musl` |
| Phase gate | `bash scripts/ci-local.sh`; `cargo test -p ostro --features failpoints --test worker_e2e --test worker_failover --test flow_e2e --test keda_sim --locked -- --test-threads=1`; `bash scripts/fencing-chaos.sh` (10,000 runs; resumable); `cargo test -p ostro-engine --test replay_corpus --locked`; `set -o pipefail; cargo deny check && cargo tree --workspace -e normal --locked > target/tree.txt && ! grep -qE '(^|[[:space:]─])(chrono|ring|openssl-sys) v[0-9]' target/tree.txt` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| WORK-01 | join with token + CA-hash; mTLS session; credits; heartbeats renew leases; revoked token closes session; version mismatch refused | integration + e2e | `cargo test -p ostro-server --test worker_session --locked`; `cargo test -p ostro --test worker_e2e --locked -- join` | ❌ Wave 0 |
| WORK-01 (secrets) | canary secret in a templated header reaches the mock; never on the worker stream (server interceptor capture) or in worker stdout/stderr at TRACE | e2e | `cargo test -p ostro --test worker_e2e --locked -- secret_never_on_worker` | ❌ |
| WORK-02 | max message ≤ frame limit (64 KiB in test) with a 2,000-event journal + 3 MiB result + 3 MiB input; identical journal vs embedded | integration | `cargo test -p ostro-server --test worker_paging --locked` | ❌ |
| WORK-03 | spill enter/exit with hysteresis; 0 flaps under oscillating load; returns after cool-down | unit (fake sampler, paused time) + e2e | `cargo test -p ostro-server --test placement_spill --locked`; `cargo test -p ostro --test worker_failover --locked -- auto_spill` | ❌ |
| WORK-04 | selector parse/match table; routed only to matching workers; unmatched → demand | unit + e2e | `cargo test -p ostro-server --lib workers::selector`; `... --test worker_failover -- selectors` | ❌ |
| WORK-05 | demand formula; endpoint JSON; gauge; keda_sim 0→N→0 with real workers | unit + e2e | `cargo test -p ostro-server --test demand --locked`; `cargo test -p ostro --test keda_sim --locked` | ❌ |
| WORK-05 (real KEDA) | ScaledObject drives a worker Deployment 0→N→0 | manual | human_needed (microk8s with approval, or CI kind job) | ❌ |
| WORK-06 | module fetched by sha (verified), AOT cached; second worker start = cache hit (no compile); tampered cache → recompile; unleased sha → PermissionDenied | integration | `cargo test -p ostro-worker --test module_cache --locked`; `... ostro-server --test worker_session -- fetch_authz` | ❌ |
| DISP-03 | queue concurrency cap; weights → dispatch share; priority bands | unit + e2e | `cargo test -p ostro-partition --test dispatcher_flow --locked`; `cargo test -p ostro --test flow_e2e -- queue_limits` | ❌ |
| DISP-04 | keyed cap; Jain fairness ≥ 0.9 with a hot key | unit (fair.rs) + e2e | `cargo test -p ostro-partition --lib leader::fair`; `... --test flow_e2e -- keyed_fairness` | ❌ |
| DISP-05 | throttle spaces first activations, accepts all; rate limit returns 429 + Retry-After, nothing enqueued; re-activations unthrottled | integration | `cargo test -p ostro-server --test flow_control --locked` | ❌ |
| OPS-01 | pause/resume run (apply + dispatcher revoke), workflow, queue; auto-pause on exhaustion; `/retry` alias; timeout while paused → TimedOut; rebuild_digest with pause | unit + proptest + e2e | `cargo test -p ostro-partition --test pause --locked`; `PROPTEST_CASES=256 cargo test -p ostro-partition --test rebuild_digest`; `... --test flow_e2e -- pause` | ❌ |
| SEC-01 | listener modes off/auto/self-signed/files (+ reload); ALPN h2 gRPC over TLS; CA + leaf issuance; ACME fake; Pebble (env-gated) | unit + integration | `cargo test -p ostro-server --test tls_listener --test tls_ca --test acme --locked` | ❌ |
| QA-03 | 10,000 runs; charges(key)==1 ∀ keys; 0 lost; 0 fenced-session sends; every run Completed exactly once; extra raw requests attributed | chaos (ignored) | `bash scripts/fencing-chaos.sh` (wraps `cargo test --release -p ostro --features failpoints --test fencing_chaos -- --ignored`) | ❌ |
| (SC2) | kill -9 worker → n+1 grant < lease_ttl (measured); SIGSTOP zombie: late append Fenced, late StartEffect rejected, mock requests unchanged | e2e | `cargo test -p ostro --test worker_failover --locked -- kill_and_zombie` | ❌ |

### Sampling Rate
- **Per task commit:** `cargo test -p <crate touched> --locked` + `cargo clippy -p <crate> --all-targets --locked -- -D warnings`. Dispatcher tasks also run the 04-14 `tests/dispatcher.rs` suite (it must stay green with `SemaphorePlacement + NoFlow`).
- **Per wave merge:** `cargo test --workspace --locked` + `cargo test -p ostro-engine --test replay_corpus --locked`.
- **Phase gate:** the 07-19 commands above, full suite green before `/gsd:verify-work`. Max task-level feedback latency is 60 s. Spawned-binary and chaos suites are wave-level.

### Wave 0 Gaps
- [ ] `crates/ostro-partition/tests/dispatcher_flow.rs` + fakes for `FlowControl`/`Placement` (07-04)
- [ ] `crates/ostro-partition/tests/pause.rs`, `tests/common/gen_pause.rs` (07-03)
- [ ] `crates/ostro-worker/tests/{protocol,module_cache}.rs` + in-process fake Execute server (07-06)
- [ ] `crates/ostro-server/tests/{worker_session,worker_paging,placement_spill,demand,flow_control,tls_listener,tls_ca,acme}.rs` + a shared harness extending `tests/common` (server + in-process worker library + message-size interceptor)
- [ ] `crates/ostro/tests/{worker_e2e,worker_failover,flow_e2e,keda_sim,fencing_chaos}.rs` + `scripts/fencing-chaos.sh`
- [ ] Guests `guests/{pay-flow,long-journal,cpu-burn}` (one owner per wave for `guests/Cargo.lock`)
- [ ] `deploy/keda/*.yaml`, `deploy/k8s/*.yaml` (07-14)
- [ ] No framework install needed.

## Sources

### Primary (HIGH confidence)
- Code read on HEAD `d9755b1` (plus the uncommitted 04-16 work in the tree): `crates/ostro-partition/src/leader/dispatcher.rs` (contract, State, pop_next, leases), `crates/ostro-server/src/{executor.rs (04-16 in progress), sink.rs, journal_io.rs}`, `crates/ostro-engine/src/{sink.rs, cache.rs, limits.rs, config.rs, host/context.rs}`, `crates/ostro-partition/src/{apply/run.rs, apply/inbound.rs, store/values.rs}`, `crates/ostro-meta/src/records.rs`, `crates/ostro-types/src/{command.rs, payload.rs, envelope.rs}`, `wit/ostro-runtime-0.1.0/runtime.wit`, `Cargo.toml`, `Cargo.lock`, `deny.toml`, `scripts/ci-local.sh`
- Planning: ROADMAP Phase 7, REQUIREMENTS, PROJECT, STATE, RESERVATIONS; 04-14-SUMMARY, 04-16..04-20 plans; 05-DECISIONS, 05-02/05-07/05-11 plans; 06-DECISIONS, 06-02/06-06/06-07/06-10/06-14 plans; 08-DECISIONS, 08-01/08-04/08-11/08-16/08-17 plans; docs/02-prd.md, docs/03-architecture.md
- crates.io API (2026-11-19): rcgen 0.14.10, instant-acme 0.8.5, rustls-acme 0.15.4 (chrono dependency confirmed), tokio-rustls-acme 0.10.0, axum-server 0.9.2, x509-parser 0.18.1, rustls-platform-verifier 0.6.2, sysinfo 0.40.4, governor 0.10.4: versions, licenses, dependency lists and features
- https://docs.rs/crate/tonic/0.14.9/features: tls-aws-lc, tls-connect-info, tls-native-roots/webpki-roots
- https://keda.sh/docs/2.18/scalers/metrics-api/: metrics-api fields, activationTargetValue, authModes
- https://keda.sh/docs/2.21/concepts/scaling-deployments/: activation is strictly greater than; activation vs scaling phase
- https://keda.sh/docs/2.21/reference/scaledobject-spec/: pollingInterval 30 s, cooldownPeriod 300 s (N→0 only), minReplicaCount 0

### Secondary (MEDIUM confidence)
- https://www.inngest.com/docs/guides/throttling.md and https://www.inngest.com/docs/guides/rate-limiting.md: throttle queues (FIFO, limit + burst per period); rateLimit drops the excess
- https://docs.rs/instant-acme/0.8.5/instant_acme/: type inventory (Account/AccountBuilder, Order, ChallengeType, KeyAuthorization, RenewalInfo behind `time`)
- https://docs.rs/rcgen/0.14.10/rcgen/: Issuer, CertificateParams::signed_by/self_signed, CertificateSigningRequestParams, CustomExtension (method-level feature gates not confirmed)

### Tertiary (LOW confidence, validate during execution)
- governor 0.10 custom `Clock`/`Reference` shape for a tokio-driven clock
- rcgen `CustomExtension::new_acme_identifier` (from training; confirm in the registry source)
- Pebble buildability on Go 1.23.10

### Local environment facts (checked)
- No Docker/kind/k3d/podman (docker shim errors). `go` 1.23.10, `openssl`, `helm`, `kubectl`, `minikube` (no driver) are present. kubectl context `microk8s` reaches a 2-node v1.32 cluster with no KEDA CRDs. **Do not use it without user approval.**

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH. Versions, licenses and dependency trees were checked against crates.io and the lock. The deny implications follow from them (rustls-acme's chrono dependency, rcgen's ring default).
- Architecture: HIGH for the seams (read from code). MEDIUM for the exact protocol shape (design recommendation; no prior art in the repo).
- Pitfalls: HIGH for 1–3, 6–9 and 12 (derived from the code and earlier SUMMARYs). MEDIUM for 10–11 and 13.
- Flow control algorithms: MEDIUM-HIGH (standard DRR/GCRA; Inngest semantics cross-checked against official docs).

**Research date:** 2026-11-19
**Valid until:** 2026-12-19 (crate versions move monthly; re-check instant-acme/rcgen/axum-server pins at 07-02)
