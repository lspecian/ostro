---
phase: 07-workers-placement-flow-control
verified: 2026-11-21T17:20:21Z
verified_at_commit: 6bde1ff
status: human_needed
score: 5/5 success criteria verified locally (SC5 by simulator; real KEDA human_needed)
gaps: []
human_verification:
  - test: "Real KEDA ScaledObject drives a worker Deployment 0 -> N -> 0 (H1, F10)"
    expected: "kubectl shows ostro-worker-heavy replicas 0 -> ceil(demand/8) -> 0 after cooldownPeriod; every run completed once; KEDA accepts authMode tls with CA only (else drop cert/key or use the prometheus ScaledObject)"
    why_human: "Needs kind+KEDA (keda-kind workflow_dispatch job) or a cluster the user explicitly approves. The local microk8s cluster must never be used."
  - test: "ACME against Let's Encrypt staging on a public host (H3)"
    expected: "Certificate issued, cached 0600 under <data_dir>/tls/acme/, renewal scheduled in the logs, curl --cacert <LE staging root> https://<name>/readyz succeeds"
    why_human: "Needs a public DNS name reachable on :443 or :80"
  - test: "ACME against Pebble in CI (H2)"
    expected: "acme-pebble workflow_dispatch job green"
    why_human: "Passed locally in 07-10/07-17 (Pebble v2.10.3); the gate SKIPs without env; no git remote yet"
  - test: "Remote CI legs (H4): linux-aarch64-musl, macos-aarch64, linux-x86_64-gnu with PG/MySQL/SeaweedFS"
    expected: "All legs green including the Phase 7 failpoint legs + manifests step"
    why_human: "No git remote yet (FOUND-05)"
  - test: "User review: D-W27, D-W28 (+D-W11), W6 template rule (+D-W3), F4 attempt_failures, D-W8 server-profile TLS refusal"
    expected: "Each adopted default accepted or overridden"
    why_human: "Product/security decisions taken in YOLO mode"
---

# Phase 7: Workers, Placement & Flow Control Verification Report

**Phase Goal:** Compute scales out to stateless workers without duplicates or lost work. Operators control where and how fast work runs.
**Verified:** 2026-11-21T17:20:21Z at `6bde1ff` (clean worktree of `main`)
**Status:** human_needed
**Re-verification:** No (initial verification)

Requirements in scope (ROADMAP Phase 7 and the REQUIREMENTS traceability table agree): WORK-01..06, DISP-03, DISP-04, DISP-05, OPS-01, SEC-01, QA-03. SEC-02..05 map to Phase 14 and are not in scope. No orphaned requirement: every ID is claimed by at least one plan.

## Method

- Read the ROADMAP Phase 7 section, REQUIREMENTS, 07-DECISIONS (D-W1..D-W28), 07-VALIDATION, the 07-21/07-22/07-23 SUMMARYs and the post-plan fixes in 07-05 (fix-0705), 07-08, 06-10 (fix-0721) and 07-21 (fix-0721 re-run).
- Inspected the code: the `p7.rs` seam, `workers/*`, `flow/*`, `tls/*`, `leader/{fair,flow,placement,dispatcher}.rs`, `apply/pause.rs`, `ostro-worker/*`, `execute.proto` and `deploy/*`.
- Re-ran the targeted suites myself, sequentially, with `CARGO_TARGET_DIR=target/wt-verify7` (deleted afterwards) and load average around 2. I did not trust the SUMMARY counts.

### Tests re-run by the verifier at 6bde1ff

| Command | Result |
|---------|--------|
| `cargo test -p ostro-partition --lib -- leader::fair` | 17 passed (incl. the 07-23 gate tests and the proptest with hold/gate ops) |
| `cargo test -p ostro-partition --test dispatcher --test dispatcher_flow --test pause --test p7_contract --test p7_format` | 17 / 15 / 15 / 6 / 4 passed |
| `cargo test -p ostro-server --test flow_control --test auto_pause --test worker_session --test worker_remote --test worker_paging --test placement_spill --test tls_ca --test tls_listener --test acme --test demand --test p7_control` | 10 / 2 / 18 / 7 / 1 / 20 / 8 / 6 / 10 / 3 / 10 passed |
| `cargo test -p ostro-server --lib -- flow:: workers:: tls::` | 48 passed |
| `cargo test -p ostro-worker` | lib 14, module_cache 3, protocol 14 passed |
| `cargo test -p ostro-api -- phase7` | 5 + 1 + 9 passed |
| `cargo test -p ostro --test flow_e2e -- --test-threads=1` (SC4, spawned) | 11 passed (112.6 s) |
| `cargo test -p ostro --test flow_e2e -- --exact throttle_vs_rate_limit`, 5 more sequential runs | 5/5 passed (G1 stays closed; 07-23 recorded 15/15) |
| `cargo test -p ostro --test worker_e2e --test worker_smoke --test keda_sim -- --test-threads=1` (SC1, SC5) | 8 / 2 / 7 passed (`listener::acme_pebble` prints SKIP without env) |
| `cargo test -p ostro --test worker_failover -- --test-threads=1` (SC2) | 5 passed: auto_spill, oscillation, sustained_overload, selectors, kill_redispatch (125.9 s) |
| `cargo test -p ostro --features failpoints --test worker_failover -- sigstop_zombie` | 1 passed |
| `cargo test -p ostro --features failpoints --test fencing_chaos -- chaos_smoke` (QA-03 smoke **at HEAD, after 07-22 and 07-23**) | 1 passed (34.4 s, 100 runs, every fault kind) |
| `cargo test -p ostro --test cli_p7` | 15 passed |
| `cargo deny check`, plus the banned-crate tree grep (chrono, ring, openssl-sys, webpki-roots, native-tls) | ok, clean |
| `bash deploy/check-manifests.sh` | OK (5 files) |
| **actionlint** (`docker run rhysd/actionlint:latest .github/workflows/ci.yml`; the image bundles shellcheck) | **exit 0, no findings.** This is the re-verification of the SC2034 fix `9212095` that 07-22 could not run. |

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `ostro worker --join` connects over mTLS (cluster CA) and executes runs; journals paged within gRPC limits; modules fetched by sha256 with local AOT cache; secrets per (run, attempt); client listener TLS (self-signed dev, files, ACME) | ✓ VERIFIED (public ACME human_needed) | Join with CA-hash pinning and CSR signing: `ostro-worker/src/join.rs`, `ostro-server/src/workers/join.rs`, `tls/ca.rs` (SAN `ostro://<cluster>/worker/<token>/<id>`). The cluster listener always requires a client cert (`workers/service.rs`). tonic `max_{en,de}coding_message_size` is 4 MiB on both sides (`service.rs:314`, `session.rs:140`). FetchJournal/Append are paged by `max_frame_bytes` (`execute.proto:21-26`). `blob_fetch.rs:121` checks the sha256 and `runner.rs` uses the HMAC-authenticated AOT cache under `--cache-dir`. Secrets resolve on the leader through StartEffect → `ServerEffectDriver` (D-W3), and the canary never reaches the worker. TLS modes off/auto/self-signed/files/acme are in `tls/mod.rs` and `tls/listener.rs`; ACME (instant-acme) is in `tls/acme.rs`. Tests: worker_e2e `join`, `paging` (3 MiB result through 64 KiB frames), `module_cache` (reuse + tamper), `secret_never_on_worker`, `listener::{self_signed,files_reload,auto_non_loopback_dev}`, worker_session 18, worker_remote 7, acme 10 (fake ACME). |
| 2 | An `auto` queue spills under a spike and returns after the cool-down without flapping; selectors route; killing a worker re-dispatches within `lease_ttl` | ✓ VERIFIED | `workers/spill.rs` (D-W9 entry, dwell + calm-window exit) and `workers/placement.rs` (embedded-first overflow). `workers/selector.rs`. worker_failover re-run: `auto_spill` (2 transitions), `oscillation` (0 transitions), `sustained_overload` (1 transition while spilling), `selectors`, `kill_redispatch` (asserts every re-dispatch delay `< lease_ttl`, `worker_failover.rs:891`; 07-18 measured a 75 ms max against 5 s), `sigstop_zombie` (failpoints). placement_spill has 20 unit/integration tests. |
| 3 | Fencing chaos over 10,000 runs: exactly one call per idempotency key, 0 duplicate charges, 0 lost effects | ✓ VERIFIED (interpretation D-W11/D-W28 under user review) | `target/fencing-chaos-fix0721/report.json` exists and reads `pass: true`, runs 10000, completed 10000, charges_eq_1 10000, double_charges 0, lost 0, fenced_sends 0, raw_duplicates 170 (all leader_kill), raw_duplicates_without_leader_kill 0, unattributed 0, provider_409 10 (all recovered), 226 faults over 5 kinds. The campaign ran at e82d99e, before the 07-22 fixes and the 07-23 dispatcher change. I re-ran `chaos_smoke` at HEAD and it passed, which covers that delta at smoke scale. On D-W11: "exactly one *call*" is judged as exactly one *charge*. Raw duplicate requests (170) come only from leader kills inside the send window, and the provider deduped them. |
| 4 | Per-queue concurrency, priority and rate limits; keyed fair scheduling; throttle queues and rate limit rejects; pause/resume of runs, workflows and queues; auto-pause on exhausted retries | ✓ VERIFIED | `leader/fair.rs`: weighted DRR → bands → key DRR, plus the 07-23 first-activation gates. `flow/limits.rs` (hand-rolled GCRA), `flow/mod.rs` (NodeFlow), `flow/rate_limit.rs` (429 `rate_limited` + Retry-After). Cmd 20/21 apply is in `apply/pause.rs`. Auto-pause goes through ExhaustPolicy → PauseRun in `executor.rs`; control is in `control/p7_pause.rs` and `p7_flow.rs`. flow_e2e 11/11 at HEAD: queue_limits, weights, priority, keyed_fairness, throttle_vs_rate_limit, pause_run/queue/workflow, pause_on_worker, auto_pause, timeout_while_paused. G1 (07-22 gate) is closed by 272ff34: dispatcher_flow `{queue,key}_throttle_on_first_activation_does_not_hold_reactivations` pass, 07-23 records a negative control, and throttle_vs_rate_limit passed 6/6 here. fix-0705 (a workflow pause holds only its own entry) is covered by `dispatcher_flow::paused_workflow_on_shared_queue_does_not_block_others`. |
| 5 | `ostro_queue_worker_demand{queue}` and the demand endpoint drive a KEDA ScaledObject 0 → N → 0 | ✓ VERIFIED by simulator; real KEDA ? HUMAN | `workers/demand.rs` is the only setter of the gauge (D-W12). `GET /v1/queues/{q}/demand` is covered by p7_control and demand.rs. The manifests in `deploy/keda/*` and `deploy/k8s/*` pass the offline check. `keda_sim::zero_to_n_to_zero` drives real `ostro worker` processes 0→N→0 from the endpoint using KEDA metrics-api math (7/7 passed). A real KEDA run is H1. |

**Score:** 5/5 truths verified locally. SC5 is proven with a simulator, not a real KEDA (D-W22). The public-CA ACME leg of SC1 needs a human.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `crates/ostro-worker/src/*` (join, session, runner, remote_sink, effect_proxy, blob/journal fetch, drain) | Stateless worker | ✓ VERIFIED | 2.7 kLOC, no stubs; wired through `ostro worker` (cli_p7, worker_e2e) |
| `crates/ostro-worker-proto/proto/ostro/worker/v1/execute.proto` | Execute service | ✓ VERIFIED | Session stream (Hello/Welcome/Credit/Heartbeat) and bulk RPCs |
| `crates/ostro-server/src/p7.rs` | Single seam | ✓ VERIFIED | `init` → tls + acme clock, `dispatcher_config` → flow + placement install, `start_services` → service/spill/demand, `serve_client` → TLS listener. The module doc still says "every hook is a stub" (stale, info only) |
| `crates/ostro-server/src/workers/{service,registry,join,remote_executor,placement,selector,spill,demand}.rs` | Worker plane | ✓ VERIFIED | All wired from p7.rs and exercised by the server and spawned suites |
| `crates/ostro-server/src/flow/{mod,limits,rate_limit,policy_view}.rs` | Flow control | ✓ VERIFIED | Installed via `p7::dispatcher_config` and the StartRun path |
| `crates/ostro-server/src/tls/{ca,mod,listener,resolver,acme,dev_cert}.rs` | TLS + ACME | ✓ VERIFIED | D-W8 refusal: `tls/mod.rs:398-406`, `AUTO_NON_LOOPBACK_ERROR` |
| `crates/ostro-partition/src/leader/{fair,flow,placement}.rs`, `dispatcher.rs` | Admission/fairness | ✓ VERIFIED | 07-23 gates present |
| `crates/ostro-partition/src/apply/pause.rs` | Cmd 20/21 apply | ✓ VERIFIED | `apply_resume_run` clears the mark and re-enqueues; it keeps `attempt_failures` (F4, user review) |
| `crates/ostro-server/src/control/p7_{flow,pause,workers}.rs` | Control plane | ✓ VERIFIED | p7_control 10/10, cli_p7 15/15 |
| `deploy/keda/*.yaml`, `deploy/k8s/*.yaml`, `deploy/check-manifests.sh` | KEDA/k8s manifests | ✓ VERIFIED (offline) | Real cluster: H1 |
| `crates/ostro/tests/fencing_chaos.rs`, `scripts/fencing-chaos.sh` | QA-03 campaign | ✓ VERIFIED | report.json present, pass true |
| `docs/workers.md`, `docs/flow-control.md`, `docs/tls.md` | Docs | ✓ present | |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| server.rs | Phase 7 features | `p7::{init, executor, dispatcher_config, attach_node, start_services, serve_client}` | ✓ WIRED | p7.rs:179-337 |
| Dispatcher | NodeFlow / NodePlacement | `DispatcherConfig.flow/placement` set by `flow::install` / `placement::install` | ✓ WIRED | spawned flow_e2e and worker_failover prove behaviour |
| Remote runner | PartitionSink + bridge | `EngineExecutor::sink_for` | ✓ WIRED | worker_remote, secret_never_on_worker |
| Worker EffectProxy | Leader `ServerEffectDriver::start` | `StartEffect` (lease, uncompleted EffectScheduled, redacted names, W6 template check) | ✓ WIRED | remote_executor.rs step 8; sigstop_zombie checks refusals |
| Worker AwaitTokenCreated | Leader-minted token | Re-mint on Append + `conflict_expected` (D-W27) | ✓ WIRED | `worker_remote::await_tokens_are_minted_on_the_leader`; the token key is not in the proto (the worker's key is process-local and random, `ostro-worker/src/lib.rs:312`) |
| demand.rs | `ostro_queue_worker_demand` + `/v1/queues/{q}/demand` | 1 s ticker; `demand::snapshot` from the control plane | ✓ WIRED | keda_sim, demand, p7_control |
| Executor exhaust | PauseRun{retries_exhausted} | ExhaustPolicy | ✓ WIRED | auto_pause 2/2, flow_e2e auto_pause |
| Bridge re-drive | 409 in_progress retry | `retry::is_redrive_in_progress` (fix-0721, D-W28) | ✓ WIRED | http_connector tests (06-10); chaos re-run lost 0 |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|--------------|-------------|--------|----------|
| WORK-01 | 07-07, 07-09, 07-12, 07-14, 07-15, 07-16, 07-17 | mTLS Execute stream with credits, heartbeats, blob fetch, per-(run, attempt) secrets | ✓ SATISFIED (secrets resolved on the leader, D-W3 user review) | SC1 evidence |
| WORK-02 | 07-07, 07-12, 07-17 | Journals paged within gRPC limits | ✓ SATISFIED | worker_paging, worker_e2e `paging` |
| WORK-03 | 07-05, 07-12, 07-13, 07-18 | embedded/workers/auto with hysteresis | ✓ SATISFIED | placement_spill, worker_failover |
| WORK-04 | 07-12, 07-18 | Labels and selectors | ✓ SATISFIED | `workers::selector` unit tests, worker_failover `selectors` |
| WORK-05 | 07-11, 07-14, 07-16, 07-20 | Demand gauge and endpoint for KEDA; scale to zero | ✓ SATISFIED locally; ? real KEDA (H1) | keda_sim, demand |
| WORK-06 | 07-07, 07-12, 07-17 | Fetch by sha256 and cache AOT locally | ✓ SATISFIED | module_cache (crate and e2e) |
| DISP-03 | 07-05, 07-08, 07-11, 07-14, 07-19 | Per-queue concurrency, priority, rate limits | ✓ SATISFIED | flow_e2e queue_limits/priority/weights, flow_control |
| DISP-04 | 07-05, 07-08, 07-19 | Keyed concurrency with fair scheduling | ✓ SATISFIED | fair.rs proptest, flow_e2e keyed_fairness (Jain 0.98 per 07-19) |
| DISP-05 | 07-05, 07-08, 07-19, 07-23 | Throttle queues, rate limit rejects | ✓ SATISFIED | throttle_vs_rate_limit 6/6 here; G1 closed |
| OPS-01 | 07-03, 07-04, 07-08, 07-11, 07-14, 07-15, 07-19 | Pause/resume run, workflow, queue; auto-pause | ✓ SATISFIED (F4 semantics under user review) | pause.rs 15, flow_e2e pause_*/auto_pause/timeout_while_paused |
| SEC-01 | 07-06, 07-09, 07-10, 07-17 | Client listener TLS (self-signed/files/ACME); mTLS cluster/worker with cluster CA | ✓ SATISFIED locally; ? public ACME (H3) | tls_ca, tls_listener, acme, worker_e2e listener::*, Pebble legs green locally in 07-10/07-17 |
| QA-03 | 07-21 | Exactly one call per idempotency key despite zombies and failovers | ✓ SATISFIED under D-W11 (user review) | report.json pass; chaos_smoke at HEAD |

REQUIREMENTS.md still shows these IDs as `[ ]` / Pending. The orchestrator flips them, not this verifier.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `crates/ostro-server/src/p7.rs` | 18-20 | Stale module doc: "every hook is a stub that preserves today's path" | ℹ️ Info | Misleading doc only; all hooks are implemented |
| `crates/ostro-server/src/workers/mod.rs` | 14 | Same stale "every hook is a stub" sentence | ℹ️ Info | Doc only |
| None found | - | No `todo!`, `unimplemented!`, TODO or FIXME in the Phase 7 source files | - | - |

### Residual risks (not gaps)

- **The 10,000-run campaign predates the 07-22 fixes and the 07-23 dispatcher change.** 07-23 changes only which first activations the fair selector offers. Fencing is still enforced by leases and the SendGate, so the risk is low. `chaos_smoke` passed at HEAD, and a 10k re-run before Phase 11 would close this fully (optional).
- **Full `ci-local.sh` with musl has not been re-run end to end since the three fixes.** Each red step was re-checked on its own: flow_e2e (07-23 15/15, and 6/6 here), worker_e2e `module_cache` (8/8 at 45d73c6, and here), and actionlint (clean here).
- Recorded follow-ups F1–F12 in 07-VALIDATION have owners and are outside the Phase 7 success criteria: no server→worker Drain message, append batching for long activations, the auto-pause "resume → completes" leg needing a new guest, per-node (not cluster) flow counters until Phase 13, and the benign over-charge in F11.

### Human Verification Required

#### 1. Real KEDA 0 → N → 0 (H1, F10)
**Test:** Run the `keda-kind` workflow_dispatch job after the first push, or use a cluster the user explicitly approves (**never the local microk8s**). Follow the steps in 07-VALIDATION H1.
**Expected:** Replicas go 0 → ceil(demand/8) → 0 after the cooldown, and every run completes once. KEDA accepts `authMode: tls` with only a CA.
**Why human:** Needs Kubernetes and KEDA.

#### 2. ACME with Let's Encrypt staging (H3)
**Test:** Run `ostro server` with `[api.tls] mode = "acme"` against the LE staging directory, on a public host.
**Expected:** Issuance, a 0600 cache, renewal scheduled, and the HTTPS readyz check passes.
**Why human:** Needs a public DNS name and an open port.

#### 3. Pebble in CI (H2) and remote CI legs (H4)
**Test:** After the first push, run the `acme-pebble` job and the full CI matrix.
**Expected:** Green, including "Phase 7 failpoint legs + manifests".
**Why human:** There is no git remote yet.

### USER REVIEW items (adopted defaults; the user may override)

1. **D-W27: await tokens of remote runs are minted on the leader only.** The token key never leaves the leader. On `Append`, the leader re-mints each `AwaitTokenCreated` with its own key and forces an eviction (`conflict_expected`), and the run replays from the journaled leader token. Cost: one extra re-grant and replay for each remote activation that creates an await token. The rejected alternative (`Welcome.token_key`) would send a cluster credential to every worker. I verified that the proto has no token_key and that the worker's key is random and process-local.
2. **D-W28 (with D-W11): a re-drive treats a provider 409 in_progress as retryable by default.** This applies without a guest policy and outside the attempt budget, with backoff of 200 ms ×2 up to a 5 s cap, bounded by the idempotency window, and it ends `effect_outcome_unknown` when the window closes. The in-progress test is a heuristic: a `Retry-After` header or body markers in the first 4 KiB. This closed the 07-21 lost-7 gap. Alternatives: pay-flow sets an `http_retry` policy, or accept the 409 as the run's result.
3. **W6 template rule (07-12 adaptation; security-relevant, review with D-W3).** A redacted header value sent by a worker that contains `{{` is accepted only if it is made entirely of well-formed `{{secret:NAME}}` placeholders (bridge grammar, reserved names refused). Everything else gets `template in redacted value` (`remote_executor.rs:250-275`). The leader checks the redacted header *names* and their order against the journal. It cannot check the *values*, because they are redacted in the journal. **Implication:** a compromised worker holding a valid lease could swap a redacted header's value for `{{secret:OTHER}}`. OTHER must still be a secret the run is granted, and the URL, method, body and egress come from the journal. Exposure is therefore limited to secrets the run is already granted, sent to the journaled destination, but it is broader than "exactly the template the guest wrote". Possible hardening: record a per-header hash of the pre-resolution template in `EffectScheduled` and compare it.
4. **F4: a resume keeps `attempt_failures`** (`apply/pause.rs:99-125` clears only the pause mark). An auto-paused run (retries exhausted) re-pauses after one more failure following a resume, and `/retry` on a paused run means resume. Resetting the counter on resume would change apply semantics and the state hash for new logs. It needs a format note and a partition apply plan (Phase 9 or gap closure).
5. **D-W8: server-profile TLS refusal.** `[api.tls] mode = "auto"` keeps a loopback listener plaintext and makes a non-loopback dev listener self-signed. The **server profile refuses to start on a non-loopback listener without `files`/`acme`** unless `allow_self_signed = true`, which logs a warning (`tls/mod.rs:398-406`, error text `AUTO_NON_LOOPBACK_ERROR`, covered by `tls_listener` and `worker_e2e::listener::auto_non_loopback_dev`). Since 07-22 the binary passes the profile explicitly, and the `auto_sealing_key` heuristic is only a library fallback. Trade-off: secure by default, but a first `ostro server --listen 0.0.0.0` fails until certs are configured.

Also pending from STATE: D-W3 (effects on the leader) and D-W11 (QA-03 judged by charges).

### Gaps Summary

No blocking gaps. The only gate failure, G1 (a throttled first activation held back re-activations, a DISP-05 / D-W13 violation), is fixed in 272ff34 and confirmed again here. The two other 07-22 red steps were a test-only inode check (fixed in 45d73c6, re-run green here) and actionlint SC2034 (fixed in 9212095; **actionlint now runs clean**). Status is `human_needed` for the real-KEDA, public-ACME, Pebble-in-CI and remote-CI legs, plus the user-review items above.

---

_Verified: 2026-11-21T17:20:21Z_
_Verifier: Claude (gsd-verifier)_
