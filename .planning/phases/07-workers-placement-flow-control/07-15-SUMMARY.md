---
phase: 07-workers-placement-flow-control
plan: 15
subsystem: cli (ostro-cli verbs, ostro binary)
tags: [cli, worker, join-token, queue-flow, pause, demand, tls, ready-line, drain, signals]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-11 REST paths + ostro_api::phase7 types; 07-07 Worker / WorkerConfig / JoinToken / DRAIN_BOUND; 07-06 ClientEndpoint + tls::effective_mode; 07-02 ServerHandle::client_endpoint and the ostro-worker dependency of the binary"
  - phase: 08-triggers-bottomless-observability
    provides: "08-12 Client::call + verb-module pattern; 08-18 top-level long-running commands in main.rs"
provides:
  - "ostro queue flow|pause|resume|demand, ostro run pause|resume, ostro workflow pause|resume, ostro worker token create|list|revoke"
  - "ostro worker: the remote worker process (flags, [worker] file, OSTRO_WORKER_* env, --print-config with the token redacted, ready line, drain on signals)"
  - "TLS-aware server ready line (https + sha256 fingerprint); http unchanged on plaintext"
  - "ostro server applies the server-profile auto TLS rule even with secrets.auto_sealing_key = true"
affects: [07-14, 07-16, 07-17, 07-18, 07-19, 07-20, 07-21, 07-22]
tech-stack:
  added: []
  patterns:
    - "hand-written clap Args/FromArgMatches for 'required positional unless a subcommand' (RunCommand), Deref to the old args type so existing callers compile unchanged"
    - "test-local ControlPlane (tonic::async_trait) served by ostro_server::tls::listener::serve in plaintext mode, for spawned-binary verb tests in a crate without ostro-api/fake"
key-files:
  created:
    - crates/ostro-cli/src/commands/worker_tokens.rs
    - crates/ostro-cli/src/commands/queues.rs
    - crates/ostro-cli/src/commands/pause.rs
    - crates/ostro/src/worker.rs
    - crates/ostro/tests/cli_p7.rs
  modified:
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro-cli/src/lib.rs
    - crates/ostro/src/main.rs
key-decisions:
  - "`ostro run` becomes RunCommand = start args OR pause/resume subcommand; a first positional `pause`/`resume` selects the subcommand, `ostro run -- pause` starts a workflow named pause"
  - "`ostro worker` is a top-level command in main.rs (08-18 pattern) with an optional `token` subcommand (args_conflicts_with_subcommands); its process code lives in the new module crates/ostro/src/worker.rs"
  - "The worker process calls Worker::new + Worker::run (exactly what ostro_worker::run does) so it can subscribe to WorkerEvent::Connected for the ready line"
  - "Flow files are a TOML subset (key = JSON literal, [table] sections) because ostro-cli has no toml dependency and Cargo files are 07-02's; --file also accepts JSON"
  - "Config errors of `ostro worker` exit 2 (usage), runtime errors (join refused, version mismatch) exit 1"
  - "--print-config shows the token as JoinToken::REDACTED = `<redacted>` (07-07's constant), not `[redacted]`"
requirements-completed: []
duration: ~1h30m
completed: 2026-11-21
---

# Phase 7 Plan 15: Phase 7 CLI Summary

**Operators get every Phase 7 admin verb over REST (`ostro queue flow|pause|resume|demand`, `ostro run pause|resume`, `ostro workflow pause|resume`, `ostro worker token create|list|revoke`), and workers get a real process entry point: `ostro worker` loads `[worker]` file < `OSTRO_WORKER_*` < flags, keeps the join token redacted everywhere, prints a ready line at the first Welcome and drains on SIGTERM/SIGINT. The server ready line now shows `https://ADDR (sha256 fingerprint HEX)` for TLS listeners and stays `http://ADDR` on loopback plaintext.**

## Commits

1. `5620c9e` feat(07-15): Phase 7 admin CLI verbs (ostro-cli only)
2. `238f764` feat(07-15): ostro worker subcommand and TLS ready line (main.rs, worker.rs, tests/cli_p7.rs)

## Verbs

All verbs take the usual connection flags (`--server`/`OSTRO_SERVER`, `--json`, `--timeout`). Exit codes are the CLI-06 mapping: 0 ok, 1 API/transport error, 2 usage.

| Verb | Request | Human output |
|---|---|---|
| `ostro worker token create [--ttl 24h]` | `POST /v1/workers/tokens {ttl_ms?}` | `token id:`, `expires at:` (RFC 3339), and the join command, printed once |
| `ostro worker token list` (`ls`) | `GET /v1/workers/tokens` | table `TOKEN ID  CREATED  EXPIRES` (never the secret) |
| `ostro worker token revoke <id>` (`rm`) | `DELETE /v1/workers/tokens/{id}` | `revoked <id>` |
| `ostro queue flow <q>` | `GET /v1/queues/{q}/flow` | TOML subset (below) |
| `ostro queue flow <q> --set k=v ... [--file F]` | GET, overlay, `PUT /v1/queues/{q}/flow {fields, expected_version: <current>}` | the stored flow (stale version = 412, exit 1) |
| `ostro queue pause\|resume <q>` | `POST /v1/queues/{q}/pause\|resume` | `queue q paused` / `queue q already paused` |
| `ostro queue demand <q>` | `GET /v1/queues/{q}/demand` | always the D-W12 JSON document |
| `ostro run pause <id> [--detail T]` | `POST /v1/runs/{id}/pause {detail?}` | `run r1 paused` / `... already paused` |
| `ostro run resume <id>` | `POST /v1/runs/{id}/resume` | `run r1 resumed` |
| `ostro workflow pause\|resume <name>` | `POST /v1/workflows/{name}/pause\|resume` | `workflow w paused` |

Examples:

```text
$ ostro worker token create --ttl 1h
token id:   wkr_000001
expires at: 2023-11-14T23:13:20Z
join command (shown once; store the token as a secret):
  ostro worker --join https://h:7700 --token wkr_000001.<secret> --ca-hash sha256:<hex>

$ ostro queue flow q1 --set placement=workers --set 'selector=zone in (a,b)' \
    --set throttle.limit=5 --set throttle.period_ms=1000 --set concurrency=null
# queue q1 (version 1)
paused = false
placement = "workers"
selector = ["zone in (a,b)"]

[throttle]
burst = 0
limit = 5
period_ms = 1000
```

- `--set` keys may be dotted (`spill.cpu_pct=70`). The value is JSON when it parses (`5`, `true`, `null`, `["a"]`) and a string otherwise. `null` removes the field (server default). A plain string for `selector` becomes a one-entry list.
- `--file` replaces the flow fields with the file (the human `flow` output, or a JSON object); `--set` applies on top. The version always comes from the GET just before the PUT.
- Errors print `ostro: error: <message> (HTTP <status> <code>[, reason <reason>])`. A `rate_limited` answer (429, any verb including `ostro run <wf>`) adds `ostro: rate limited: retry after <ceil s> s (Retry-After), <ms> ms`, from `details.retry_after_ms`.
- `ostro run` still starts runs. A first positional `pause` or `resume` selects the subcommand; `ostro run -- pause` starts a workflow named `pause`.

## `ostro worker`

Precedence: `WorkerConfig::default` < `[worker]` table of `--config FILE` < `OSTRO_WORKER_<KEY>` env < flags.

| Flag | Config key / env | Notes |
|---|---|---|
| `--join URL` | `join` / `OSTRO_WORKER_JOIN` | `https://`; `http://` only for loopback (07-07) |
| `--token T` | `token` / `OSTRO_WORKER_TOKEN` | prefer the env var or `--token-file` (argv is visible to local users) |
| `--token-file F` | (flag only) | trimmed; conflicts with `--token` |
| `--ca-hash sha256:HEX` | `ca_hash` | pinned during the join |
| `--labels k=v,k2=v2` | `labels` | |
| `--slots N` | `slots` | default `available_parallelism()` |
| `--cache-dir D` | `cache_dir` | default `./.ostro-worker/cache` |
| `--memory BYTES` | `memory_bytes` | `1073741824`, `512MiB`, `2GiB`, `1G` (binary units) |
| `--tls-cert/--tls-key/--tls-ca F` | `tls_cert`/`tls_key`/`tls_ca` | external certificate instead of the join |
| `--endpoint HOST:PORT` (repeatable or comma list) | `endpoints` / `OSTRO_WORKER_ENDPOINTS` | required with the external certificate |
| `--worker-id ID` | `worker_id` | `[A-Za-z0-9._-]{1,64}`, default generated |
| `--config F` | | must exist |
| `--print-config` | | `[worker]` TOML; the token prints as `<redacted>` |

`reconnect_max_ms` and `rejoin_after_failures` are set through the file or env only. Validation is `WorkerConfig::validate`: either join + token (+ optional ca_hash), or all three TLS files + endpoints. A config error prints `ostro: error: invalid worker configuration: ...` and exits **2**. A runtime failure (join refused, version mismatch, engine) exits **1**; the error never contains the token (checked with `RUST_LOG=debug`).

### Ready line and drain timing

- Worker: `ostro worker ready: id=<worker_id> server=<endpoint> slots=<n>` on stdout, once, at the first `WorkerEvent::Connected` (session Welcome). Reconnects do not print it again.
- First SIGTERM/SIGINT: the worker's shutdown token is cancelled. Before a session exists the process exits 0 at once. With a session, the worker drains: it sends `Drain`, evicts in-flight activations, waits up to `ostro_worker::drain::DRAIN_BOUND` = 10 s for them, closes the session (2 s close bound) and exits 0.
- A second signal during the drain prints `ostro: SIG.. again: exiting without waiting for the drain` and exits **130**.
- A drain still running after `DRAIN_BOUND + DRAIN_GRACE` (10 s + 5 s) exits **1**.

## Server ready line (07-16's harness parses these)

- Plaintext (`auto` on loopback, `off`): unchanged, `NAME listening on http://ADDR (data: DIR[, ephemeral]) ready in N ms`.
- TLS (`self-signed`, `files`, `acme`): `NAME listening on https://ADDR (sha256 fingerprint HEX) (data: DIR[, ephemeral]) ready in N ms`. `HEX` is 64 lowercase hex digits, the sha256 of the served leaf (DER), i.e. `ClientEndpoint.fingerprint` without its `sha256:` prefix. The parenthesis is omitted while an ACME certificate is still pending (no fingerprint yet).
- Both come from `ServerHandle::client_endpoint()`; `ADDR` is still `handle.local_addr()` (for `--listen 0.0.0.0:0` it is `0.0.0.0:<port>`).

## 07-06 follow-up (profile detection)

`TlsProfile::of(cfg)` treats `secrets.auto_sealing_key = true` as the dev profile. p7.rs/tls are not this plan's files, so `ostro server` now calls `ostro_server::tls::effective_mode(&cfg, TlsProfile::Server)` before starting: a non-loopback `auto` listener without `files`/`acme`/`allow_self_signed` is refused with `AUTO_NON_LOOPBACK_ERROR` whatever `auto_sealing_key` says (test `ready_line_server_profile_auto_rule_ignores_auto_sealing_key`). `tls::init` inside the server still uses the heuristic. Code that starts a server-profile config through `Server::start` directly, outside the binary, should call `init_with_profile` or pass the profile when the seam is next edited.

## Tests

`crates/ostro/tests/cli_p7.rs` has 14 tests:
- 6 `verbs_*` run the spawned binary against an in-process API server. The server is `ostro_api::router` over `P7Fake`, a test-local `ControlPlane` with the 07-11 semantics (CAS flow versions, idempotent pause with `changed`, `run_terminal` 409, not-found, tokens, demand, a start that always answers `rate_limited` 1500 ms). It is served by `ostro_server::tls::listener::serve` with `TlsState::off()`. The tests cover token create/list/revoke (the secret appears only in `create`), flow get/--set/null/--file round trip with the `expected_version` sequence `[0, 1, 2]`, 400 validation, queue pause/resume/demand, run/workflow pause/resume incl. 409/404, the Retry-After hint, and clap usage errors (exit 2).
- 5 `worker_*`:
  - `--help` lists every flag.
  - Missing join, missing token, bad token, partial TLS triple and missing config file each exit 2 with the message.
  - `--print-config` redacts the token from `--token`, `--token-file` and `OSTRO_WORKER_TOKEN`, with file < env < flag precedence.
  - A join failure exits 1 without the token.
  - SIGTERM and SIGINT during a hanging join exit 0 with no ready line.
- 3 `ready_line_*`:
  - Loopback dev prints `http://`.
  - `ostro dev --listen 0.0.0.0:0` prints `https://` with a 64-hex fingerprint, and `GET https://127.0.0.1:<port>/readyz` answers 200.
  - Server profile + `auto_sealing_key` is refused.

Unit tests: worker_tokens 3, queues 2, pause 2 (ostro-cli), worker.rs 2 (byte sizes, rendered config redaction).

Results (worktree, `CARGO_TARGET_DIR=target/wt-07-15`):
- `cargo test -p ostro-cli -p ostro --locked`: all green. ostro-cli lib 48, phase8_verbs 6. ostro bin 2, backup_cli 2, bottomless_s3 1 (skip), cli 5, cli_bridge 1, **cli_p7 14**, cli_phase8 3, cli_verbs 7 (snapshots unchanged), data_dir_lock 1, dev_startup 4 (+1 ignored), e2e_dev 3, order_saga 4, server_config 4.
- Plan Task 1 verify: `cli_p7 -- verbs` gave 6 passed; ostro-cli green; clippy `-D warnings` clean.
- Plan Task 2 verify: `cli_p7`, `cli` and `dev_startup` green; clippy clean (also with `--features failpoints`).
- `cargo fmt --check -p ostro -p ostro-cli` is clean. `cargo build --workspace --all-targets --locked` is warning-free.

**Not run:** the full `bash scripts/ci-local.sh`. Load was 25–35 with parallel executors and the Phase 8 campaign. The touched crates and their suites are covered above.

## Deviations from Plan

1. **[Rule 3] Verb tests use a test-local control plane, not `FakeControlPlane`.** The `ostro` crate has no `ostro-api/fake` or axum dev-dependency, and only 07-02 may edit Cargo files. `cli_p7.rs` implements a small `ControlPlane` with `tonic::async_trait` (tonic is a dev-dep) and serves it with ostro-server's own listener. The verbs still run as real spawned binaries.
2. **New module files** `crates/ostro/src/worker.rs` (the worker process code) was not in files_modified. The plan's rule says "new code in new modules", and main.rs only gains one `mod`, one variant and one dispatch arm.
3. **Commit split.** Commit 1 holds the ostro-cli verbs. Through the existing `ClientCommand` flatten it already exposes `queue`/`run pause`/`workflow` in the binary. `worker token` is wired in commit 2 together with the `worker` command it lives under. The verb tests are in commit 2's `cli_p7.rs`.
4. **`--output json` → `--json`.** This is the existing CLI-06 convention. `queue demand` always prints JSON.
5. **Flow file format.** It is a TOML subset plus JSON, not full TOML, because there is no toml dependency in ostro-cli.
6. **Redaction marker.** It is `<redacted>` (`JoinToken::REDACTED`, 07-07), not `[redacted]`.
7. **Key link.** The process uses `Worker::new` + `Worker::run`, the body of `ostro_worker::run`, so it can observe `Connected` for the ready line. `ostro_worker::run` appears in the module docs.
8. **[Rule 2] Retry-After hint for every verb** in `commands::report`, the shared error path, not only the Phase 7 verbs.
9. **[Rule 1 / 07-06 follow-up]** The server-profile `auto` rule is enforced in main.rs (see above).

## Known limitations / follow-ups

- **CLI over TLS:** `ostro_cli::client::parse_server` still rejects `https://` ("TLS lands with SEC-01"). The admin verbs therefore reach only plaintext listeners: loopback `auto`, `off`, or a TLS server through a plaintext proxy. A follow-up should teach the client `https` with platform roots plus an optional `--ca-hash` / `--insecure` (client.rs, outside this plan).
- **No end-to-end worker ready-line test yet:** join and Execute are server-side stubs until 07-09/07-14. The ready-line and drain-with-session paths are exercised by 07-16/07-17 against a real server.
- **Second-signal path** (exit 130 during a drain) is not covered by a test: a drain needs a live session.
- **No verbs for `GET /v1/workers` and `POST /v1/workers/{id}/drain`** (not in RESERVATIONS §11's CLI row). Add them if operators need them.

## Self-Check: PASSED

- Files: all key-files exist at `238f764`.
- Commits: `5620c9e` and `238f764` are on main.
