# Workers: remote execution, placement, spill and KEDA

An Ostro server runs every workflow itself by default (the embedded executor). When one box is
not enough, the same binary joins as a stateless **worker** (`ostro worker`) and executes
activations the server hands it. This document describes what was shipped in Phase 7
(WORK-01..06): the architecture, joining and identity, placement and spill, demand and KEDA,
failure semantics, what a worker never sees, ports, limits and the version policy.

Implementation: `crates/ostro-worker` (the worker), `crates/ostro-worker-proto` (the
`ostro.worker.v1` protocol), `crates/ostro-server/src/workers/` (registry, Execute service,
join, placement, spill, demand) and `crates/ostro-server/src/p7.rs` (the wiring seam).
End-to-end proof: `crates/ostro/tests/worker_{smoke,e2e,failover}.rs`, `keda_sim.rs` and
`fencing_chaos.rs` (spawned binaries).

---

## 1. Architecture: the leader stays authoritative

- The partition leader is the dispatcher (docs/03 §5.1). A worker is one more **placement**
  for a granted activation, next to the embedded executor slots. Nothing about the journal,
  leases or fencing moves to the worker.
- One mTLS **control stream** per (worker, server node), `Execute.Session`, carries
  `Hello`/`Welcome`, `Credit{free_slots}`, `Heartbeat{leases, free_slots, memory_used}`,
  `Activate`, `Input`, `Cancel`, `CancelEffect`, `GuestLogs`, `Done`, `Drain` (worker to server)
  and `GoAway`.
- Five **bulk RPCs** run on the same h2 connection, so a module fetch never delays a heartbeat:
  `FetchJournal` (paged journal rows), `FetchBlob` (module and payload blobs, chunked),
  `Append` (client-streamed event batches), `StartEffect` (effects bridge, §6) and `Renew`
  (certificate renewal). Every bulk RPC checks that the caller's peer certificate owns the
  `session_id` and that the session holds the lease for the `(run, token)` it names.
- The leader decodes every appended event itself and writes it through the same
  `PartitionSink` as an embedded run (`EngineExecutor::sink_for`), maps `Done` through the
  embedded outcome mapper and journals `RunStarted` on the server. A remote run's journal is
  identical to an embedded one (`worker_paging`, byte-level).
- Await tokens are minted on the leader only (D-W27): the token key never leaves it. A worker
  batch whose `AwaitTokenCreated` differs from the leader mint is cut after that event, re-minted
  and answered `conflict_expected`; the worker evicts and the re-grant replays the leader token.
- Workers are stateless apart from the AOT module cache (`--cache-dir`, HMAC-checked, §8).
  Killing one only costs a re-dispatch.

## 2. Joining and identity (D-W4, D-W20, D-W25)

kubeadm-style join:

```bash
ostro worker token create --ttl 24h          # prints the join command once
ostro worker --join https://ostro.example:7700 \
  --token-file /run/secrets/ostro-worker-token \
  --ca-hash sha256:<hex> --labels pool=heavy,zone=a --slots 8
```

- `ostro worker token create` stores an `ApiToken{id: "wkr_<10 base32>", hash: sha256(secret),
  scopes: ["worker:join"], expires_at_ms}`. The secret is shown once (`Cache-Control: no-store`
  over REST). `token list` never shows it; `token revoke <id>` deletes it.
- The worker generates its ECDSA P-256 key **in memory** (never written) and posts
  `{token, csr_pem, worker_id, labels}` to `POST /v1/workers/join` on the client listener. The
  answer carries a client certificate (SAN URI `ostro://<cluster>/worker/<token_id>/<worker_id>`,
  EKU clientAuth, `[workers] client_cert_ttl`, default 24 h), the cluster CA, the cluster
  endpoints and `renew_after_ms`.
- **CA pinning (`--ca-hash`).** With a `self-signed` client listener the chain carries the
  cluster CA, so the pin is enforced in the TLS handshake: no request byte (the token included)
  leaves the worker unless a chain certificate has `sha256(DER) == --ca-hash` and the leaf
  verifies against it. A `files` / `acme` listener does not present the cluster CA; the worker
  then verifies it with the platform roots (one retry, before any request byte was sent). In
  every mode the CA returned by the join must match `--ca-hash` before the worker trusts it for
  the cluster listener. Plain `http://` joins are accepted only for loopback hosts.
- **Renewal** at `renew_after` (2/3 of the lifetime): `Renew{csr}` over mTLS, then a new channel
  and session with the new identity; in-flight activations move to the new session and the old
  one drains.
- **Revocation** = token deletion. Join and Renew are refused (400, reason `join_token_invalid`),
  and live sessions whose certificate carries the token id are closed within about a second
  (checked at stream open and on every meta change). A worker whose re-join is refused with a
  4xx exits with that error instead of retrying (07-22); a restart is refused at the join.
- **External certificates** (cert-manager): `--tls-cert/--tls-key/--tls-ca` plus `--endpoint`,
  no join. The certificate must chain to `[cluster.tls] client_ca` and carry exactly one SAN URI
  `ostro://<cluster_id>/worker/ext/<worker_id>` and clientAuth. Revocation is removal from that
  CA or expiry; one session per external worker id.
- Node certificates (`ostro://<cluster>/node/<id>`) are refused on the cluster listener; the
  worker and node identity parsers reject each other's certificates.

## 3. Placement (WORK-03, WORK-04)

Per queue, `ostro queue flow <q> --set placement=...` (REST `PUT /v1/queues/{q}/flow`), default
`[workers] default_placement = "embedded"`:

| placement | Where an activation runs |
|-----------|--------------------------|
| `embedded` | an embedded executor slot (`[executor] slots`) only |
| `workers` | a connected worker whose labels match the queue's selector only (the run waits otherwise) |
| `auto` | embedded slots; while the queue is **spilling**, embedded slots first and the overflow to workers |

**Selectors** (`selector = ["zone in (a,b)", "!gpu"]`, entries ANDed) use Kubernetes semantics:
`k=v`, `k==v`, `k!=v`, `k in (a,b)`, `k notin (a,b)`, `k`, `!k`. `k!=v` and `notin` also match a
worker without the label; the empty selector matches every worker. Among matching workers the
one with the most free credit is picked. An unparsable stored selector matches nothing.

## 4. Spill (auto placement, D-W9)

Sampled every second through the injected clock (`workers/spill.rs`):

| Signal | Entry | Exit line (x 0.85) |
|--------|-------|--------------------|
| node CPU (`getrusage` delta / wall x cores) | > 70 % for 10 s (`sustain`) | < 59.5 % |
| embedded slots in use | > 90 % | < 76.5 % |
| resident Wasm memory | > 85 % | < 72.25 % |
| queue lag (oldest ready) | > 5 s | < 4.25 s |

- Any signal enters **Spilling**. Exit needs a minimum dwell of one cool-down (30 s), every
  signal below its exit line for the whole cool-down, and either nothing on workers and no
  overflow attempt during it, or an empty backlog. A sustained overload therefore produces
  exactly one transition (tested; `ostro_placement_transitions_total{queue,to}`).
- Node defaults come from `[workers.spill]`; `QueuePolicy.spill` overrides them per queue.
- Deterministic test triggers: `slots_pct` with a small `[executor] slots`, a small `lag_ms`, and
  `cpu_pct: 100` to disable the CPU signal (07-13 hand-offs).

## 5. Demand and KEDA (WORK-05, D-W12, D-W22)

```text
headroom = 0 if paused; concurrency - running (embedded + workers) if set; else unbounded
workers:  demand = running_on_workers + min(ready_unblocked, headroom)
auto:     spilling: running_on_workers + max(0, min(ready_unblocked, headroom) - embedded_free)
          calm:     running_on_workers
embedded: 0
```

- `ready_unblocked` excludes paused queues/workflows and throttled keys; placement-blocked work
  counts (it is admissible but has no capacity, exactly what workers should add).
- `GET /v1/queues/{q}/demand` (and `ostro queue demand <q>`) returns
  `{queue, demand, backlog, running_on_workers, placement, spilling, workers, worker_slots_free}`.
- `/metrics`: `ostro_queue_worker_demand{queue}` (set every second by `workers/demand.rs` only;
  Phase 8's dispatcher metrics describe it), `ostro_queue_worker_backlog{queue}`,
  `ostro_workers_connected`, `ostro_worker_slots_free`.
- Manifests (validated offline by `deploy/check-manifests.sh`): `deploy/k8s/server-statefulset.yaml`
  (files-mode TLS from a cert-manager Secret, `[cluster] advertise`), `deploy/k8s/worker-deployment.yaml`
  (replicas 0, token from a Secret, `--ca-hash` from a ConfigMap), `deploy/keda/scaledobject-metrics-api.yaml`
  (metrics-api on the demand endpoint, `targetValue` = worker `--slots`, `activationTargetValue: "0"`),
  `deploy/keda/scaledobject-prometheus.yaml` (the cluster form:
  `sum(ostro_queue_worker_demand{queue="heavy"})`) and `deploy/keda/trigger-auth.yaml`.
- KEDA semantics (proved locally by `keda_sim.rs` with real `ostro worker` processes):
  activation is `demand > activationTarget`, `desired = clamp(ceil(demand / target), min, max)`,
  the cool-down applies only to N -> 0. Scale-down drains are exactly-once safe.
- A real ScaledObject run is human_needed: the optional `keda-kind` `workflow_dispatch` CI job
  (kind + KEDA inside GitHub Actions) or a cluster you approve. Steps: 07-20-SUMMARY
  "human_needed: real KEDA run".

## 6. Effects stay on the leader (D-W3, user review)

- A worker never performs an effect. Its `EffectProxy` sends `StartEffect{token, op, redrive,
  redacted header values, http_retry}`; the leader checks the lease for the token, loads the
  committed `EffectScheduled(op)`, checks that the redacted header names match the journal copy
  (only well-formed `{{secret:NAME}}` placeholders are accepted in redacted values), rebuilds the
  `LiveEffect` from the journal and runs the run's Phase 6 `ServerEffectDriver` (registry,
  `SendGate`, grants, egress policy, per-(run, attempt) secret resolution, `CompleteEffect`).
- **What a worker never sees:** secret values (resolved on the leader at send time), the await
  token key, other runs' journals or blobs (`FetchBlob` serves only the run's module and blobs
  its journal references), meta records and other tenants' data. The canary e2e scans every
  byte on the worker channel and the worker's TRACE log (`worker_e2e::secret_never_on_worker`).
- A refused `StartEffect` from a fenced session (zombie, revoked token) sends nothing and is
  logged on the leader at warn (`refused StartEffect from a session without the lease`).

## 7. Failure semantics

| Event | What happens |
|-------|--------------|
| Worker killed (`kill -9`) | The stream ends; its jobs are abandoned at once (no failure counted) and re-granted with attempt n+1 |
| Worker stalled (SIGSTOP) | Heartbeats stop; the lease expires after `lease_ttl` (30 s default, D-W14) and the run is re-granted. When the zombie wakes, its appends and StartEffects are fenced (token no longer leased) |
| Stream loss seen by the worker | Every in-flight activation is cancelled at once (self-fence), then reconnect with jittered backoff; after `rejoin_after_failures` failures on every endpoint it joins again |
| Drain (SIGTERM, `POST /v1/workers/{id}/drain`) | The worker sends `Drain`, evicts in-flight activations, waits up to 10 s and closes. The server-side drain stops assignment only (there is no server-to-worker Drain message yet; follow-up) |
| Leader crash | The new leader rebuilds from the journal; worker sessions to the old leader end and re-grants carry a new epoch, so late messages from the old incarnation are fenced |
| Pause of a run on a worker | The lease is revoked (`Cancel`); the worker's buffered events for that activation are discarded and the activation re-executes after resume |

Fencing layers: the attempt token (epoch, n) on every append and StartEffect, the session-lease
check on every bulk RPC, the self-fence on stream loss, and the bridge's own send gate. QA-03
(07-21): 10,000 runs under leader kills, server and worker failpoints, `kill -9` and SIGSTOP
gave 0 double charges, 0 lost effects, 0 fenced sends and 0 unattributed duplicates (D-W11).

**Design limitation (recorded):** a worker sends a long single activation's events when the
activation ends (append batching). A lease revoke therefore discards that activation's work, so
long activations on workers re-execute more work than embedded ones.

## 8. Module cache

`--cache-dir` holds AOT-compiled modules keyed by `sha256(module) + compat digest` (Wasmtime
version, ostro version, target). Each artefact carries an HMAC with a node-local key; a tampered
artefact is deleted, logged at warn and recompiled. Modules are fetched once per worker
(`FetchBlob`) and verified by sha256.

## 9. Ports and limits

| Port | Listener | Who connects |
|------|----------|--------------|
| 7700 | client listener (`[api] listen`): REST, gRPC, `/metrics`, the demand endpoint, `POST /v1/workers/join` | operators, CLI, KEDA, joining workers |
| 7701 | cluster listener (`[cluster] listen`, server profile; dev uses `127.0.0.1:0`): `ostro.worker.v1`, client certificate always required | workers only (Phase 12 adds peer nodes) |

- `[cluster] advertise` is what joined workers dial (the bound port is appended when absent; an
  unspecified bind is advertised as `127.0.0.1` with a warning).
- `[workers] max_frame_bytes` (default 1 MiB, 1..=4 MiB) bounds journal pages, blob chunks and
  append frames; tonic's codec limit is 4 MiB. `[workers] max_append_bytes` (64 MiB) bounds one
  reassembled append.
- `heartbeat = min([workers] heartbeat (10 s), lease_ttl / 3)`; h2 keepalive `[workers] keepalive`
  (5 s).

## 10. Version policy

`Hello` carries `proto_version` (1), `event_codec_ver`, `ostro_version`, `wasmtime_version` and
the WIT packages. `proto_version` and the event codec are always checked; with
`[workers] require_same_version = true` (default) the ostro and Wasmtime versions must equal the
server's (one constant, `ostro_engine::cache::WASMTIME_VERSION`). A mismatch is answered with
`GoAway{"version mismatch: ..."}` and the worker exits non-zero without retrying.

**Phase 13 (13-14, D-P13-18): `[workers] version_policy`.** `Hello` also carries
`feature_level`. The default `compatible` admits mixed N/N−1 fleets: proto and event codec must
match and the worker level must be ≥ max(1, finalized − 1); the ostro / Wasmtime versions are
not compared. `exact` keeps the checks above (with `require_same_version`). GoAway reasons:
`version mismatch: ...`, `version too old: ...` (also sent after a finalization leaves a session
below the floor) and `node_draining` (the server node is draining or removed). Placement only
offers an activation to a worker that has the module's WIT line and level. Details:
`docs/compat.md` §6.

## 11. Operating notes

- `ostro worker` prints `ostro worker ready: id=<id> server=<endpoint> slots=<n>` once connected.
  Exit codes: 0 after a clean drain, 1 on a runtime failure (join refused, version mismatch),
  2 on a configuration error, 130 on a second signal during the drain.
- Logs are plain text when stdout is not a terminal.
- The CLI reaches a TLS listener with `--server https://...`; use `--ca-file <data_dir>/tls/ca.crt`
  (or `OSTRO_CA_FILE`) for a self-signed listener.
- `GET /v1/workers` lists connected workers (labels, slots, free credit, draining).
