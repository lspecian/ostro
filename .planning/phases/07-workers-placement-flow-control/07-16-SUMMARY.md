---
phase: 07-workers-placement-flow-control
plan: 16
subsystem: workers (demand), deploy, spawned-binary e2e harness
tags: [demand, keda, kubernetes, metrics, harness, e2e, workers]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-05 QueueGauges (ready_unblocked incl. placement-held work, running_on_workers); 07-09 WorkerRegistry::sessions; 07-12 QueuePlacement/selector; 07-13 SpillHandle; 07-14 queue_demand -> demand::snapshot; 07-15 ready lines + ostro worker"
provides:
  - "workers::demand: compute (pure D-W12), snapshot, snapshot_all, publish, run, start (1 s ticker)"
  - "ostro_queue_worker_demand{queue} value (Phase 7 owns it), ostro_queue_worker_backlog{queue}, ostro_workers_connected, ostro_worker_slots_free"
  - "deploy/keda + deploy/k8s manifests and deploy/check-manifests.sh (offline)"
  - "crates/ostro/tests/common/p7.rs: P7Cluster spawned server+worker harness for 07-17..07-21"
  - "guests/pay-flow secret renamed to pay_token (07-12 follow-up)"
affects: [07-17, 07-18, 07-19, 07-20, 07-21, 07-22]
tech-stack:
  added: []
  patterns:
    - "Pure formula (compute) + thin readers (snapshot / snapshot_all) so the endpoint and the gauge loop share one code path"
    - "Per-server default placement recorded by start() in a Weak-keyed table (snapshot's fixed signature has no config)"
key-files:
  created:
    - crates/ostro-server/tests/demand.rs
    - deploy/keda/scaledobject-metrics-api.yaml
    - deploy/keda/scaledobject-prometheus.yaml
    - deploy/keda/trigger-auth.yaml
    - deploy/k8s/worker-deployment.yaml
    - deploy/k8s/server-statefulset.yaml
    - deploy/check-manifests.sh
    - crates/ostro/tests/common/p7.rs
    - crates/ostro/tests/worker_smoke.rs
  modified:
    - crates/ostro-server/src/workers/demand.rs
    - guests/pay-flow/src/lib.rs
    - crates/ostro-worker/tests/protocol.rs
key-decisions:
  - "Demand headroom uses the summed gauges (running_embedded + running_on_workers) against QueuePolicy.concurrency; a paused queue has headroom 0; paused workflows are already excluded from ready_unblocked (07-05 held entries)"
  - "auto queues count running_on_workers even when not spilling (runs still on workers after the spill ends keep the pool up)"
  - "The demand ticker always runs (also with [workers] enabled = false): an embedded-only node publishes zeros"
  - "Join tokens in the harness are created over REST: the CLI client cannot reach https yet (07-15 follow-up)"
metrics:
  duration: "~75 min"
  completed: 2026-11-21
---

# Phase 7 Plan 16: Worker demand, KEDA manifests and spawned-binary harness Summary

Implemented the D-W12 worker demand (`workers/demand.rs`). It is published every second as `ostro_queue_worker_demand{queue}` and served by `GET /v1/queues/{q}/demand`. The plan also adds offline-checked KEDA/Kubernetes manifests and a spawned `ostro server` + `ostro worker` harness. A binary-level smoke test and a kill-9 restart/resume test prove the harness.

## Commits

1. `d32c029` feat(07-16): worker demand metric and snapshot
2. `b25b4da` docs(07-16): KEDA and Kubernetes manifests with offline checks
3. `9b57182` fix(07-07): pay-flow uses a valid lowercase secret name
4. `78ba165` test(07-16): spawned server+worker harness and smoke e2e

## Demand formula as implemented

Inputs:
- `QueueGauges` summed over the partitions;
- the queue's `QueuePolicy`: placement over `[workers] default_placement`, selector, `concurrency`, `paused`;
- `SpillHandle::spilling(q)`;
- the registry sessions whose labels match the selector;
- the free embedded slots.

```text
headroom   = 0 if paused; concurrency - (running_embedded + running_on_workers) if set; else unbounded
workers:   backlog = ready_unblocked; demand = running_on_workers + min(backlog, headroom)
auto:      spilling: backlog = ready_unblocked;
                     demand = running_on_workers + max(0, min(backlog, headroom) - embedded_free)
           calm:     backlog = 0; demand = running_on_workers
embedded:  backlog = 0; demand = 0
workers = distinct matching worker ids; worker_slots_free = their free credit
```

Examples (each one is tested):

| Situation | Result |
|---|---|
| Workers queue, 10 ready, no worker | 10 (scale from zero) |
| + concurrency 4 | 4 |
| 2-slot worker running 2, 8 ready, concurrency 4 | 2 + min(8, 2) = 4 |
| Queue paused while 2 run | 2 |
| Auto queue, calm | 0 |
| Auto queue, spilling, 10 ready, 2 free embedded slots | 8 (11 with 3 on workers) |
| Auto queue, spill ended, 3 still on workers | 3 |
| Embedded queue | 0 |

## Metrics

| Series | Set by | When |
|---|---|---|
| `ostro_queue_worker_demand{queue}` | `demand::publish` only | every 1 s, for each queue with gauges; a queue whose gauges vanish gets one 0 row |
| `ostro_queue_worker_backlog{queue}` | `demand::publish` | same |
| `ostro_workers_connected`, `ostro_worker_slots_free` | `demand::publish` and the 07-09 registry | every 1 s and on every registry change |

- The loop sleeps until a deadline computed from the injected clock (F5). The sleep is capped at one period, so a frozen `ManualClock` still ticks.
- Labels are queue names only.
- `describe_gauge` is added for backlog, connected and slots_free. The demand description stays in 08-16's dispatcher.
- **Foreign setter check:** there is no `ostro_queue_worker_demand` setter outside `workers/demand.rs`. The dispatcher only describes it. It is enforced by `demand::no_foreign_demand_setter` and by the plan's awk check. No BLOCKING follow-up.

## Manifests (offline only; no cluster command was run)

- `deploy/keda/scaledobject-metrics-api.yaml`:
  - `ostro-worker-heavy`, min 0, max 50, poll 15, cooldown 120;
  - metrics-api on `https://ostro.ostro.svc:7700/v1/queues/heavy/demand`, `valueLocation: "demand"`, `targetValue: "8"`, `activationTargetValue: "0"`, `authMode: "tls"`, `authenticationRef: ostro-demand-auth`;
  - comments explain that activation is strictly greater, cool-down applies only to N→0, and targetValue ≈ `--slots`.
- `deploy/keda/scaledobject-prometheus.yaml`: `sum(ostro_queue_worker_demand{queue="heavy"})`, the cluster form.
- `deploy/keda/trigger-auth.yaml`: a Secret with placeholder `ca` (and empty `cert`/`key`), plus `TriggerAuthentication ostro-demand-auth`.
- `deploy/k8s/worker-deployment.yaml`:
  - ConfigMap (join URL, `--ca-hash`) and token Secret (`OSTRO_WORKER_TOKEN`);
  - Deployment with replicas 0, `--labels pool=heavy --slots 8 --cache-dir /cache` (emptyDir), resources, `terminationGracePeriodSeconds: 60`, non-root, no privilege escalation, read-only root.
- `deploy/k8s/server-statefulset.yaml`:
  - ConfigMap `ostro.toml` (`[api.tls] mode = "files"` from the cert-manager Secret `ostro-api-tls`, `[cluster] advertise`);
  - Service exposing 7700/7701;
  - StatefulSet with a PVC `/data`, `/readyz` readiness and `/healthz` liveness probes over HTTPS.
  - The embedded `ostro.toml` was checked with `ostro server --config F --print-config`: files mode parses.
- `deploy/check-manifests.sh`:
  - greps the required keys, rejects tabs, rejects private keys and checks there is no worker `--token` flag;
  - checks worker `--slots` equals `targetValue`;
  - runs `yaml.safe_load_all` when PyYAML is present (6.0.2 here), checking apiVersion/kind/metadata.name, ScaledObject min 0 and activation "0", and the Deployment grace period;
  - a negative control (min 1 plus a tab) fails with 3 findings.

## Harness API (`crates/ostro/tests/common/p7.rs`, for 07-17..07-21)

Include it with `mod common; #[path = "common/p7.rs"] mod p7;`. It uses `crate::common` read-only: `command`, `guest_wasm`, `sha_hex`, `is_terminal`.

- **`P7Opts`**: `partitions` (4), `lease_ttl`, `heartbeat`, `embedded_slots`, `hot_wait`, `max_frame_bytes`, `default_placement`, `spill: Vec<(key, toml value)>` for `[workers.spill]`, `mock` (starts a `MockProvider` and adds `[egress] allow_private = ["127.0.0.0/8"]`), `extra_toml` (secrets/connectors/grants config), `env` (for example `RUST_LOG`, `OSTRO_SECRET_*`).
- **`P7Cluster::start(opts).await`**:
  - writes `ostro.toml` (`[api.tls] mode = "self-signed"`, pinned `[api]`/`[cluster]` loopback ports) and spawns `ostro server --config`;
  - parses the TLS ready line (`TlsReady{addr, fingerprint, ms}`) and reads `<data>/tls/ca.crt`;
  - builds a CA-trusting `P7Api`, waits for `/readyz`, and creates a join token. It provides `token`, `ca_hash` (`sha256:..`), `api_port`, `cluster_port` and `bind_retries`.
- **Methods**: `create_token(ttl_ms)`, `spawn_worker(WorkerOpts) -> WorkerProc` (waits for `ostro worker ready: id=.. slots=..`), `wait_workers(n)`, `kill9_server()`, `stop_server()` (SIGTERM, clean exit), `restart_server()` (same ports; EADDRINUSE retried, ports never changed), `server()`, `mock()`, `join_url()`, `data_dir()`.
- **`WorkerOpts::new(id, slots).label(k, v)`**: `reconnect_max_ms` (default 500), `rust_log`, `env`. The token is passed by env.
- **`P7Proc`** (`WorkerProc` derefs to it): `kill9`, `sigstop`, `sigcont`, `sigterm`, `signal`, `wait_exit`, `try_exit`, `wait_stdout(limit, f)`, `stdout`, `stderr`, `output`, `error_lines`. A child is killed on drop (and continued first if stopped).
- **`P7Api`**: `get/post/put/send`, `deploy(guest)`, `set_flow(q, fields)` (read-modify-PUT with `expected_version`), `start(wf, body)`, `run`, `wait_terminal` (tolerates a restarting server), `history`, `demand(q)`, `workers()`, `ready()`.

## Smoke timings (debug build, load average about 20)

- `smoke`: server ready 3.5 s, worker ready +0.06 s, 3 `sdk-basic` runs on the worker in 0.28 s, total 4.2 s, 0 bind retries. Every run id shows `remote activate run=<id>` in the server log. Demand is back to 0 with `workers = 1`, and the worker logged no ERROR lines.
- `restart_resume`: 4 × `cpu-burn` 1.5 s on a 2-slot worker with a 6 s lease. The server was SIGKILLed while `running_on_workers > 0`, restarted on the same ports in 0.49 s, and the worker was listed again after 0.54 s. Every run is completed with one `run_completed` and one `burn` step completion. Total 9.6 s.

## Verification

- Task 1:
  - `cargo test -p ostro-server --test demand --locked`: 3 passed, twice.
  - lib `workers::demand`: 5 passed.
  - The plan's awk setter check passes.
  - Clippy `-D warnings` is clean.
- Task 2: `bash deploy/check-manifests.sh` passes: OK (5 files), PyYAML parse done.
- Task 3: `cargo test -p ostro --test worker_smoke --locked -- --test-threads=1`: 2 passed (run 2 times). The `restart_resume` filter: `test result: ok. 1 passed`.
- pay-flow fix: `ostro-worker --test protocol -- pay_flow`: 1 passed (the guest was rebuilt, and the test asserts the new template).
- Regression:
  - `ostro-server --lib`: 132 passed. `worker_remote`: 7, `placement_spill`: 20, `metrics_endpoint`: 4 (the cardinality audit includes the new series), `p7_control`: 10, `demand`: 3.
  - `cargo clippy -p ostro -p ostro-server -p ostro-worker --all-targets --locked -- -D warnings` is clean, and `cargo fmt --all --check` is clean.
- Not run: `bash scripts/ci-local.sh` (shared-load policy; the Phase 8 campaign is running). 07-22 runs the gate.

## Deviations from Plan

1. **[Plan adaptation] Queues with only a policy are not enumerated by the ticker.** `MetaState` has no queue-policy iterator, and ostro-meta is outside files_modified. The ticker publishes every queue with dispatcher gauges, which is every queue that has had a run on this node. The endpoint answers for any name.
2. **[Plan adaptation] Default placement lookup.** `snapshot(shared, q)` has a fixed 07-02 signature, and `P7Shared` carries no config. `start(ctx)` records `[workers] default_placement` in a table keyed by a `Weak<P7Shared>` (address plus liveness check). A `P7Shared` that never went through `start` defaults to embedded.
3. **[Plan adaptation] "All its workflows paused" headroom.** This is not computed separately: 07-05 already excludes held paused-workflow entries from `ready_unblocked`, so such a queue has no admissible backlog.
4. **[Rule 3] Join token and queue flow over REST, not `ostro worker token create` / `ostro queue flow`.** The CLI client still rejects `https://` (07-15 known limitation), and the harness server is TLS as the plan requires.
5. **Real runs for the worker-running case.** The "worker with 2 slots running 2" case uses real `timeouts sleep_ms 50 s` runs, which stay parked hot (leased) on the worker with `hot_wait = 55 s`. A sleep beyond `hot_wait` evicts at once, so `sleep_forever` cannot be used. The auto and embedded cases use synthetic gauge rows on partition 900 and a forced `SpillHandle::set`.
6. **Extra commit `fix(07-07)`** (caller request, from the 07-12 follow-up): `guests/pay-flow` now uses `{{secret:pay_token}}`, and `ostro-worker/tests/protocol.rs` was updated. `worker_remote.rs` keeps `{{secret:PAY_TOKEN}}` on purpose as an invalid-template case. **Note for 07-17:** its plan text still says to configure the secret `PAY_TOKEN`; use `pay_token`.

## Follow-ups

- Other worker labels: the smoke worker uses `pool=heavy`, and the manifests' queue selector is up to the operator (`ostro queue flow heavy --set selector=pool=heavy`).
- When the CLI client learns https (07-15 follow-up), the harness can switch `create_token`/`set_flow` to `ostro worker token create` / `ostro queue flow` to exercise them end to end.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/workers/demand.rs, crates/ostro-server/tests/demand.rs, deploy/keda/{scaledobject-metrics-api,scaledobject-prometheus,trigger-auth}.yaml, deploy/k8s/{worker-deployment,server-statefulset}.yaml, deploy/check-manifests.sh, crates/ostro/tests/common/p7.rs, crates/ostro/tests/worker_smoke.rs
- FOUND: commits d32c029, b25b4da, 9b57182, 78ba165 on main
