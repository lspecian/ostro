---
phase: 07-workers-placement-flow-control
plan: 17
subsystem: spawned-binary e2e (workers, client listener TLS)
tags: [e2e, workers, join, ca-pinning, revocation, paging, aot-cache, secrets, canary, tls, files-reload, acme, pebble, sc1]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-16 P7Cluster harness (common/p7.rs); 07-12 remote execution + 64 KiB framing; 07-15 ostro worker binary + ready line; 07-14 REST token/revoke routes; 07-06 listener modes + SIGHUP reload; 07-10 ACME; 07-07 pay-flow (secret pay_token)"
provides:
  - "crates/ostro/tests/worker_e2e.rs: SC1 through the binaries (join, paging, module_cache, secret_never_on_worker, listener::{self_signed, files_reload, auto_non_loopback_dev, acme_pebble})"
affects: [07-22]
tech-stack:
  added: []
  patterns:
    - "AOT cache hit/miss observed from outside the process: artefact inode + mtime (a compile always persists through tmp + rename, so a new inode)"
    - "TLS leaf probes use a fresh reqwest client per probe: a client's session cache resumes the old session, and a resumed handshake carries no certificate"
key-files:
  created:
    - crates/ostro/tests/worker_e2e.rs
  modified: []
key-decisions:
  - "Tokens, revocation, secrets, grants and egress go over REST: the CLI client still rejects https:// (07-15 follow-up), same as the 07-16 harness"
  - "The secret canary origin is a local recording HTTP/1.1 listener in the test file: the 06-10 MockProvider counts requests but does not record headers (ostro-testkit is outside this plan)"
  - "Worker RUST_LOG for the canary scan is TRACE everywhere except Cranelift's compiler passes (cranelift_codegen, cranelift_frontend, wasmtime_internal_cranelift, regalloc2 at info): plain `trace` produced 6.5 M lines / 1.19 GB per run, all compiler passes over module code"
requirements-completed: [WORK-01, WORK-02, WORK-06, SEC-01]
metrics:
  duration: "~70 min"
  completed: 2026-11-21
---

# Phase 7 Plan 17: SC1 worker + listener e2e Summary

ROADMAP Phase 7 success criterion 1 now has binary-level tests. They run the real `ostro server` and `ostro worker` processes and cover: CA-pinned join, refusal of a wrong pin before the token is sent, revocation, 64 KiB paging of a 2,000-step / 3 MiB + 3 MiB run, the AOT module cache across worker restarts (tamper included), the leader-only secret, and the three client-listener TLS modes. The ACME leg is env-gated; it ran green locally against Pebble with real TLS-ALPN-01 validation.

## Commits

1. `dcd0d53` test(07-17): SC1 worker e2e (join, paging, cache, secrets)
2. `234614a` test(07-17): SC1 listener TLS modes e2e

## Per-test results (debug build, load average 20-28, `--test-threads=1`)

Full suite: `cargo test -p ostro --test worker_e2e --locked -- --test-threads=1` gave `test result: ok. 8 passed` in 37.6 s (acme_pebble SKIP). Every test also passed in an earlier separate run.

| Test | Result | Timings (last full run) | What it proves |
|---|---|---|---|
| `join` | ok | worker ready 5.0 s (24.7 s on the first run, which includes the guests build), 5 runs 0.70 s, session closed 1.3 ms after revoke, total 6.1 s | The right `--ca-hash` joins and runs 5 `sdk-basic` runs on a `workers` queue (each has a `remote activate run=<id>` line). A wrong pin exits non-zero with the `--ca-hash` verifier error, prints no ready line and does not leak the token. The server never logs the `wrong-pin` worker id: its token is valid, so a request that reached the server would have joined and logged `worker joined`. `DELETE /v1/workers/tokens/{id}` drops the worker from `GET /v1/workers` within 2 s. A restart with the revoked token exits non-zero with `join refused ... revoked`. |
| `paging` | ok | embedded run 1.57 s, worker run 2.86 s, 2,007 events, total 11.4 s | With `max_frame_bytes = 65536`, `long-journal {steps: 2000, result_bytes: 3 MiB, wait_input: true}` plus a 3 MiB signal completes on the worker. Result: `steps 2000, sum 1999000, big_len 3 MiB, input_len 3 MiB + 2`. Its sha256 equals the embedded run's, the `(kind, step name)` history shape is equal, and there are 2,001 step completions. |
| `module_cache` | ok | cold run 226 ms, warm 174 ms, after tamper 239 ms; artefact 617,072 B; total 4.8 s | Cold: the cache dir starts empty and holds one `aot/*.cwasm` after the run (fetched by sha256, compiled). Warm restart with the same `--cache-dir`: same inode, same mtime, same bytes, so the artefact was loaded, not recompiled. Tamper (one byte XOR): the worker logs `AOT artefact failed authentication; deleted`, then rewrites the artefact (new inode, not the tampered bytes) and the run succeeds. |
| `secret_never_on_worker` | ok | 2,299 worker lines (540 KB) scanned, total 4.6 s | The secret `pay_token` holds a canary (sealing key file, grant + egress for module `pay-flow` and the origin). The `pay-flow` run on a workers queue completes with status 200. The origin got exactly `Authorization: Bearer <canary>` on `POST /pay`. The worker logged TRACE lines, and neither its stdout/stderr nor its cache dir holds the canary in any encoding: raw, base64, hex, percent-encoded or JSON-escaped (`Canary::scan_bytes` / `scan_tree`). |
| `listener::self_signed` | ok | server ready 2,975 ms, total 3.2 s | The ready line has `https://127.0.0.1:<port>` and `(sha256 fingerprint <hex>)`. The leaf served on `/readyz` and `/v1/workers` equals the printed fingerprint. gRPC `ostro.health.v1` Check over TLS (tonic, cluster CA, ALPN h2) on the same port returns `SERVING`. A client trusting a different CA is refused with a certificate error. |
| `listener::files_reload` | ok | B served 48 ms after SIGHUP, total 3.4 s | `mode = "files"` with `reload_poll = "3600s"`. The ready line prints cert A, and A is served. After both PEMs are atomically overwritten with B, A is still served (no SIGHUP yet). After `kill -HUP`, B is served, by the same PID, with the log line `SIGHUP: reloading the certificate files`. |
| `listener::auto_non_loopback_dev` | ok | dev https ready 3,187 ms, total 3.8 s | `ostro dev --listen 0.0.0.0:0` prints an https ready line on `0.0.0.0`, and its leaf (trusted via `<data>/tls/ca.crt`) equals the fingerprint. `ostro server --listen 0.0.0.0:0` with the default `auto` mode exits non-zero with `ostro_server::tls::AUTO_NON_LOOPBACK_ERROR` and never prints a listening line. |
| `listener::acme_pebble` | SKIP by default; **ran green locally** | 6.05 s with Pebble | Without `OSTRO_TEST_ACME_DIRECTORY` the test prints `SKIP ... (human_needed: run Pebble, see 07-10-SUMMARY.md)`. With Pebble, the server in `mode = "acme"` (tls-alpn-01, `domains = ["localhost"]`) serves a leaf issued by `CN=Pebble Intermediate CA 323d3c` with SAN localhost. |

## Files-mode certificates

The certs are generated in the test with rcgen 0.14 (dev-dependency of `ostro`):
- a CA, ECDSA P-256 (`KeyPair::generate`), `IsCa::Ca(Unconstrained)`, KeyCertSign/CrlSign;
- two leaves, "cert A" and "cert B", for `localhost` + `127.0.0.1` (IP SAN), serverAuth EKU, each signed through `Issuer::new(ca_params, ca_key)`.

The cert file holds the chain `[leaf, CA]` as PEM, and the key file holds the PKCS#8 PEM. Both files are written atomically (tmp + rename). The client trusts only the test CA (`tls_certs_only`).

## Pebble leg

- Binary: `GOBIN=target/wt-07-17/pebble/bin go install github.com/letsencrypt/pebble/v2/cmd/pebble@v2.10.3`. The modcache already had it, and Go auto-switched to go1.26.11.
- Config: loopback only (`127.0.0.1:34017` directory, `35017` management, httpPort 35018, tlsPort 35019), with the module's `test/certs/localhost` cert and key. Run with `-strict`, `PEBBLE_VA_NOSLEEP=1`, `PEBBLE_WFE_NONCEREJECT=0`, and **without** `PEBBLE_VA_ALWAYS_VALID`.
- Run: `OSTRO_TEST_ACME_DIRECTORY=https://127.0.0.1:34017/dir OSTRO_TEST_ACME_CA=<modcache>/pebble/v2@v2.10.3/test/certs/pebble.minica.pem OSTRO_TEST_ACME_TLS_PORT=35019 cargo test -p ostro --test worker_e2e --locked -- --test-threads=1 acme_pebble` gave `1 passed`.
- Pebble logged `Starting 3 validations`, `authz ... set VALID by completed challenge`, `Issued certificate serial 41ad8a4e3f37650c`. That is a real TLS-ALPN-01 validation against the spawned server's client listener on :35019.
- Pebble was stopped by its PID. The binary was deleted with `target/wt-07-17`.
- **Remaining human_needed:** a public ACME CA (Let's Encrypt staging on a reachable host). No test contacts a public directory, and the test asserts this.

## Verification

- Task 1 verify: `cargo test -p ostro --test worker_e2e --locked -- --test-threads=1 join paging module_cache secret_never_on_worker` gave `test result: ok. 4 passed`.
- Task 2 verify: `... -- --test-threads=1 listener` gave `test result: ok. 4 passed` (acme SKIP).
- Plan verification: the full `worker_e2e` gave `test result: ok. 8 passed`.
- `cargo clippy -p ostro --all-targets --locked -- -D warnings` is clean. `cargo fmt -p ostro --check` is clean. The build is warning-free.
- Not run: `bash scripts/ci-local.sh` (shared-load policy; 07-18..07-21 and the Phase 8 campaign were running). 07-22 runs the gate.

## Deviations from Plan

1. **[Rule 3] REST instead of `ostro worker token revoke` and the `ostro secret|grant|egress` verbs.** The CLI client rejects `https://` (07-15 follow-up). The P7 server is TLS-only, so revocation goes through `DELETE /v1/workers/tokens/{id}`, and setup goes through `PUT /v1/secrets/pay_token`, `PUT /v1/grants` and `PUT /v1/egress/pay-flow`.
2. **Secret name `pay_token`** instead of the plan's `PAY_TOKEN` (renamed in 07-16 because FX-06 allows lowercase names only).
3. **Recording origin instead of MockProvider** for the Authorization check (key decision above). `P7Opts.mock = true` is still set, for `[egress] allow_private`.
4. **Narrowed TRACE filter** for the canary scan (key decision above). The filter is TRACE for everything that touches the wire or the run (ostro_*, tonic, h2, hyper, rustls, reqwest, wasmtime runtime). Cranelift's compiler passes, which never see run data, are at info.
5. **Join counter.** The server has no join metric. "No join attempt" is shown by the absence of the mis-pinned worker's id in the server log while its token is valid (a request that arrived would have joined and logged `worker joined`), plus its absence from `GET /v1/workers`.
6. **Module cache hit/miss.** The worker binary exposes no cache metric or compile log line. The test uses the artefact file identity instead (`AotCache::put` always writes tmp + rename, so a compile changes the inode). Tamper detection is the worker's own WARN line.
7. **Local process helper.** `common/p7.rs`'s `P7Proc` constructor is private, and its `spawn_worker` always uses the cluster pin, the cluster token and a fresh cache dir. `worker_e2e.rs` has a small `Proc` wrapper and `worker_cmd` for: the wrong pin, a per-test token, a shared cache dir, and the files/dev/server listener processes. The harness was not changed.
8. **07-12 in-process helpers not used.** `ServerWorkers`, `wire_listener`, `journal_shape` and `RemoteHooks::tap` live in `ostro-server`'s test tree and need in-process server handles; this file drives spawned binaries only. Paging compares the result sha + `(kind, step name)` history shape over REST. The byte-level journal equality and the frame-size recorder stay in 07-12's `worker_paging`.

## Product observations (no failing test, nothing changed)

- After revocation, the joined worker process keeps running and retries (its sessions and re-joins are refused). It does not exit (`try_exit() == None` right after the session closed). A restart is refused at join. This may be intended (an operator-visible retry loop), but 07-22 may want to decide whether a revoked worker should exit.
- Worker log output is ANSI-coloured even when stdout is a pipe.

## Self-Check: PASSED

- FOUND: crates/ostro/tests/worker_e2e.rs (1242 lines) on main
- FOUND: commits dcd0d53, 234614a on main
