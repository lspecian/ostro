---
phase: 06-effects-bridge-connectors-secrets
plan: 17
subsystem: effects-e2e
tags: [e2e, effects, http, idempotency, crash, kill9, failpoints, ssrf, egress, rate-limit, fx-01, fx-02, fx-03, fx-08, fx-09]
requires:
  - "06-09 fx-http / fx-ssrf guests (I/O contracts)"
  - "06-10 HttpConnector, derive_key, MockProvider + RecordingListener"
  - "06-14 server effect seam + start_server_with_bridge / deploy_named"
  - "06-15 seam e2e patterns (eviction model, detached provider)"
  - "06-16 REST admin + tests/common/fx.rs helpers"
provides:
  - "crates/ostro-server/tests/fx_http_e2e.rs: SC1 in process (4 tests; 5 with --features failpoints)"
  - "crates/ostro/tests/fx_redrive.rs: SC1 kill -9 re-drive through the failpoint binary (4 tests)"
  - "crates/ostro-server/tests/fx_ssrf_e2e.rs: SC4 SSRF table / redirects / env proxies / allow-lists through a guest (4 tests)"
  - "crates/ostro-server/tests/fx_rate_e2e.rs: SC4 per-target rate limits end to end (1 test)"
affects: [06-20]
tech-stack:
  added: []
  patterns:
    - "Binary crash tests: FAILPOINTS=bridge::effect::before_complete_submit=return(hang), SIGKILL via kill -KILL, restart on the same data dir without FAILPOINTS"
    - "Test-local raw-TCP mock provider in the ostro crate (no dev-dep on ostro-testkit); its /pay charge completes in a detached task so a vanished client still finishes the request"
    - "Egress denials asserted on details.reason read from the journaled EffectCompleted failure (the guest error item carries only kind/message/retryable)"
    - "Env-proxy test: proxy vars set only around TestServer::boot (egress client built at start), removed before the test's REST client exists; a file-wide tokio mutex serialises env access"
key-files:
  created:
    - crates/ostro-server/tests/fx_http_e2e.rs
    - crates/ostro-server/tests/fx_ssrf_e2e.rs
    - crates/ostro-server/tests/fx_rate_e2e.rs
    - crates/ostro/tests/fx_redrive.rs
  modified: []
key-decisions:
  - "fx_redrive.rs carries its own HTTP/1.1 mock and derive_key copy, because only 06-02 edits Cargo files and the ostro crate has no ostro-testkit / ostro-bridge dev-dependency"
  - "Allow rules for the SSRF table are written in normalised form (a third TABLE column), because the REST egress validator refuses percent-encoded hosts and IPv6 literals with an embedded dotted quad"
  - "The rate-limit max wait is checked three ways: the parsed config, the Bridge Debug output (rate_max_wait: 300ms) and the scheduled -> completed latency of every rate_limited effect"
requirements-completed: [FX-01, FX-02, FX-03, FX-08, FX-09]
metrics:
  duration: ~45 min
  completed: 2026-11-20
---

# Phase 6 Plan 17: E2E SC1 / SC4 Summary

**ROADMAP Phase 6 SC1 and SC4 are now proven end to end with real guests. SC1 is covered by an in-process server and by the failpoint `ostro` binary killed with SIGKILL mid-flight: the same key after a crash with one charge, a POST past the window completing `effect_outcome_unknown` without a resend, a GET past the window re-sent, and an evicted in-flight run woken after kill -9. SC4 is covered through fx-ssrf / fx-http: 21 bypass rows plus `localhost` denied by the classifier with the expected `details.reason`, redirect hops to internal literals denied, env proxies ignored, per-module allow-lists enforced, and per-target rate limits with a working `rate_max_wait`. Every test passed against the existing product code. No product change was needed and none was made.**

## Tests and timings

### SC1 in process: `crates/ostro-server/tests/fx_http_e2e.rs`

| Test | Proves |
|------|--------|
| `pay_carries_derived_key_and_charges_once` | POST `/pay` carries `derive_key(run, op)`, shaped `ostro-[0-9a-f]{32}` and echoed back by the provider. `requests(key) == charges(key) == 1` |
| `override_and_semantic_keys` | The guest's `idempotency_key` is sent verbatim: one charge under it and 0 requests under the derived key. `semantic_key` puts the SDK key (`ostro/idem/v1/name`, recomputed in the test) in the `Idempotency-Key` header **and** in the JSON body |
| `intent_durable_before_send` (failpoints) | With `bridge::http::before_send=return(wait:2000)`, REST history shows `effect_scheduled` with no `effect_completed` while `total_requests() == 0`, and still 0 after 500 ms. After release: 1 request, 1 charge |
| `flaky_get_retries_with_same_key` | `/flaky?fail=2` with a retry policy: one effect, 3 requests to `/flaky`, all 3 with the same derived key, and the guest sees 200 |
| `parallel_requests_get_distinct_keys` | 10 parallel POSTs all return 200. The 10 keys are distinct and equal the set of `derive_key(run, op)` over the 10 ops, with one charge each |

Results: 4 passed in 30 s; with `--features failpoints`, 5 passed in 3.4 s.

### SC1 with kill -9: `crates/ostro/tests/fx_redrive.rs` (`#![cfg(feature = "failpoints")]`)

`ostro server --config <tmp>/ostro.toml` uses `[node] partitions = 4` and `[egress] allow_private = ["127.0.0.0/8"]`, with a data dir and port 0. The address comes from the ready line, and readiness is `/readyz` 200. Configuration goes over REST (`PUT /v1/modules/{sha}?name=fx-http`, `/v1/egress/fx-http`, `/v1/connections/{origin}`). SIGKILL is sent with `kill -KILL`, and the test asserts a non-success exit.

| Test | Flow | Timings (`--nocapture`) |
|------|------|------|
| `same_key_after_crash` | FAILPOINTS hold the completion. Charge, then kill -9, then restart without FAILPOINTS. The run completes 200. `requests(key) == 2`, `requests_to("/pay") == 2`, `charges(key) == 1` | charged 38 ms after start, killed at 263 ms, restart ready in 302 ms, terminal at 619 ms |
| `window_unknown` | http-target `idempotency_window = "2s"`, `safety_margin = "0s"`. Hold, kill -9, sleep 3 s, restart. The guest item is `error.kind == "effect_outcome_unknown"` with `retryable: false`. `requests(key) == 1` | killed at 259 ms, ready in 299 ms, terminal at 3.6 s |
| `idempotent_resend` | Same config, GET `/status/200`. Re-sent with the same key, completes 200, 2 requests | killed at 262 ms, ready in 2.45 s, terminal at 5.8 s |
| `suspended_inflight_wakes` | `/pay?delay_ms=6000` with a retry policy. Kill -9 2.5 s after the provider saw the request (past the 2 s hot wait, before the 6 s response), then restart. The run completes 200, every `/pay` request carried the same key, `charges == 1` | killed 2.53 s after the request, ready in 304 ms, terminal at 6.7 s |

Result: 4 passed in 29.8 s. First boots take about 2.7-3.1 s: a cold `ostro server` on a fresh data dir under load average ~15.

### SC4 SSRF: `crates/ostro-server/tests/fx_ssrf_e2e.rs`

- **`bypass_table_denied_by_default`** runs with the default `[egress]` (asserted: `allow_private` empty). The fx-ssrf policy lists every row's host as an explicit rule in normalised form, plus `http://evil.com` and `http://localhost`. Every row is `egress_denied` with the following `details.reason` (from the journal; the guest item's kind is cross-checked):

  | Row | Reason |
  |-----|--------|
  | `http://127.0.0.1/` `2130706433` `0177.0.0.1` `0x7f.1` `127.1` `%31%32%37.0.0.1` `0` | blocked |
  | `169.254.169.254` `169.254.170.2` `100.100.100.200` | blocked |
  | `[fd00:ec2::254]` `[fd20:ce::254]` `[::ffff:169.254.169.254]` `[::ffff:a9fe:a9fe]` `[::127.0.0.1]` `[64:ff9b::a9fe:a9fe]` `[2002:a9fe:a9fe::]` `[2001:0::1]` `[::1]` `[fe80::1]` | blocked |
  | `http://user:pw@evil.com@127.0.0.1/` | userinfo |
  | `http://localhost/` (server's hickory system resolver) | blocked |

  The `localhost` row was **not skipped**: the resolver was available. The test would record `dns` as a skip instead of failing.
- **`redirects_to_internal_literals`**: `allow_private = ["127.0.0.1/32"]`. The allow rules are `http://127.0.0.1:<mock port>`, `http://[::1]` and `http://169.254.169.254`.
  - Untemplated fx-ssrf GETs of `/redirect?to=http://[::1]/` and `?to=http://169.254.169.254/` are denied at the hop with `egress_denied`/`blocked`, and each first hop reached the mock.
  - Templated fx-http GETs (`X-Api-Key: {{secret:tok}}`, sealed secret + grant on the mock origin) return the 302 unchanged, with `Location` equal to the target.
- **`env_proxies_are_ignored`**: `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY`, upper and lower case, point at a `RecordingListener` during `TestServer::boot`. A GET to the mock returns 200, the mock saw it, and the listener accepted **0** connections.
- **`per_module_allow_lists`**: fx-ssrf allowed to the mock reaches it (control). fx-http with no policy gets `egress_denied`/`not_allowed` for the same URL, and the mock saw nothing. Under the policy `https://api.example.test`, `http://` gives `scheme` and `:8443` gives `port`. Under `https://*.example.test`, `a.b.example.test` gives `not_allowed`.

Result: 4 passed in 1.4 s.

### SC4 rate limits: `crates/ostro-server/tests/fx_rate_e2e.rs`

`per_target_rate_limit` uses http-target A with `rate = "5/s"`, `burst = 5`, and `[egress] rate_max_wait = "300ms"` via the extra TOML. W1 is asserted on the config and on the Bridge Debug output, `rate_max_wait: 300ms`. One run sends 30 parallel GETs to A and 10 to B.

The measured run took 320 ms:
- A: 6 ok and 24 `rate_limited`, and A saw exactly 6. The assertions are 5 ≤ ok ≤ 10 and limited ≥ 20.
- B: 10/10 ok.
- Every `rate_limited` effect completed 303-304 ms after its `EffectScheduled`. The assertions are ≥ 250 ms and < 10 s, against a 30 s default.
- `/metrics`: `ostro_effect_rate_limited_total = 24`.

Result: 1 passed in 0.6 s.

## Verification

All runs were sequential with `CARGO_TARGET_DIR=target/wt-06-17`:
- `cargo test -p ostro-server --test fx_http_e2e --locked -- --test-threads=1`: **test result: ok. 4 passed**
- `cargo test -p ostro-server --features failpoints --test fx_http_e2e --locked -- --test-threads=1`: **test result: ok. 5 passed**
- `cargo test -p ostro --features failpoints --test fx_redrive --locked -- --test-threads=1 --nocapture`: **test result: ok. 4 passed**
- `cargo test -p ostro-server --test fx_ssrf_e2e --locked -- --test-threads=1`: **test result: ok. 4 passed**
- `cargo test -p ostro-server --test fx_rate_e2e --locked -- --test-threads=1`: **test result: ok. 1 passed**
- `cargo clippy -p ostro-server -p ostro --all-targets --all-features --locked -- -D warnings`: clean. The same without `--all-features`: clean. `cargo fmt` is clean, and the test builds are warning-free.
- **Not run:** `bash scripts/ci-local.sh --no-musl`. Other executors (08-14, 08-16, 08-19) and a debugger were building at the same time, load average 14-16. This plan adds four test files only, with no product, Cargo or shared-file changes.

## Deviations from Plan

**1. [Rule 3 - Blocking] A test-local mock and key derivation in `fx_redrive.rs`**
- The `ostro` crate has no dev-dependency on `ostro-testkit` (MockProvider) or `ostro-bridge` (`derive_key`), and this wave may not edit Cargo files.
- `fx_redrive.rs` therefore has a small raw-TCP HTTP/1.1 mock: `/pay` with key dedupe, `delay_ms`, 409 in-progress and replay, and `/status/{code}`. It also has a copy of the pinned key formula.
- The mock's charge runs in a detached task, which removes the need for 06-15's `detached_proxy`.

**2. [Plan text] Allow rules in normalised form.** The plan's W2 note already asks for normalised forms. In practice the REST egress validator **refuses** `http://%31%32%37.0.0.1` ("invalid host") and `http://[::127.0.0.1]` ("invalid IPv6 literal"). The table has an explicit rule column (`http://127.0.0.1`, `http://[::7f00:1]`, `http://[2001::1]`, …). This is not a security issue, because the URLs themselves are still parsed and denied. It is an observation for 06-20's egress docs: the rule grammar accepts canonical spellings only.

**3. [Plan detail] `details.reason` comes from the journal.** The fx-ssrf/fx-http error items carry only `kind/message/retryable` (06-09). The reason is decoded from the journaled `EffectCompleted` `Failure.details`, and the guest item's `kind` is checked to agree.

**4. [Plan detail] `suspended_inflight_wakes` eviction.** As 06-15 found, an in-flight effect evicts with `HotWaitElapsed` and the run stays `Running`. The binary test cannot read executor job records, so it kills after 2.5 s, past the 2 s hot wait and well before the 6 s response, and asserts the wake + re-drive + single charge.

**5. CI coverage of `fx_redrive`.** `scripts/ci-local.sh` lists `ostro` under `FAILPOINT_LINT_ONLY`, so the failpoint leg only lints this file and does not run it. Running it in CI would need `ostro:fx_redrive` in `FAILPOINT_CRATES` plus the matching ci.yml step. That edit is outside this wave's files and is left for 06-20 (phase gate).

**6. Commits.** One commit per task, tests only (no RED/GREEN split): the product code already existed.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | f27b616 | test(06-17): SC1 in process and kill -9 re-drive with the failpoint binary |
| 2 | 4c21f94 | test(06-17): SC4 through a guest: SSRF table, redirects, env proxies, allow-lists, rate limits |

## Self-Check: PASSED

- FOUND: crates/ostro-server/tests/{fx_http_e2e,fx_ssrf_e2e,fx_rate_e2e}.rs and crates/ostro/tests/fx_redrive.rs on main
- FOUND: f27b616 and 4c21f94 on main
