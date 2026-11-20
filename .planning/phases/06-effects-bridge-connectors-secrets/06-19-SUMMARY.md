---
phase: 06-effects-bridge-connectors-secrets
plan: 19
subsystem: effects-e2e
tags: [e2e, canary, secrets, scrubbing, tracing, object-store, presign, providers, vault, k8s, sc3, sc5, fx-05, fx-06, fx-07, fx-10]
requires:
  - "06-09 fx-canary / fx-object / fx-http guests (I/O contracts)"
  - "06-14 start_server_with_bridge (extra_toml), deploy_named"
  - "06-16 REST admin + tests/common/fx.rs helpers, /v1/objects presign route"
  - "06-13 Vault / K8s providers (mock shapes copied from the bridge tests)"
provides:
  - "ostro_testkit::canary: Canary (needles, scan_bytes, scan_tree), Report, CaptureLayer (install_global, watch, captured, stats), register_sink / scan_sinks (D-FX6 hook), spawn_quiet / Quiet"
  - "crates/ostro-server/tests/fx_canary.rs: SC3 (4 tests)"
  - "crates/ostro-server/tests/fx_object_e2e.rs: SC5 object store via guest (local + env-gated S3)"
  - "crates/ostro-server/tests/fx_providers_e2e.rs: SC5 five secret providers through the chain"
affects: [06-20, 08 (OTLP canary leg via register_sink)]
tech-stack:
  added: []
  patterns:
    - "Canary needles computed by an independent encoder (testkit never depends on ostro-bridge); base64 needles include the stable middle at byte alignments 0/1/2"
    - "Mocks that see the secret run on their own OS thread + current-thread runtime under a thread-local NoSubscriber (spawn_quiet); the capture also drops their thread-name prefix and the ostro_testkit::mock target"
    - "Streaming scan: every captured line is matched against watched canaries as it is recorded, so the 256 MiB buffer cap cannot hide a leak"
key-files:
  created:
    - crates/ostro-server/tests/fx_canary.rs
    - crates/ostro-server/tests/fx_object_e2e.rs
    - crates/ostro-server/tests/fx_providers_e2e.rs
  modified:
    - crates/ostro-testkit/src/canary.rs
key-decisions:
  - "CaptureLayer disables only the wasm compiler's targets (cranelift*, regalloc2*, wasmtime_cranelift*, wasmtime_internal_cranelift*, wasmtime_environ::compile*): at TRACE they dump guest IR and drove one test past 13 GB RSS; their only input is guest wasm, which never holds a secret value"
  - "The Vault secret is named app/token (mount kv, default field value): the plan's kv/app#token is not a valid placeholder name (no # in the template grammar)"
  - "fx_canary uses its own small recording mock (records X-Api-Key values and bodies, /enc encodings switch) instead of MockProvider, which has neither"
requirements-completed: [FX-05, FX-06, FX-07, FX-10]
metrics:
  duration: ~1h15m
  completed: 2026-11-20
---

# Phase 6 Plan 19: E2E SC3 / SC5 Summary

**ROADMAP Phase 6 SC3 (except the OTLP leg, deferred to the Phase 8 gate by D-FX6) and SC5 are proven end to end. A random canary used as `{{secret:canary-key}}` by fx-canary has 0 hits in every scanned location: API responses, gRPC, guest output, raw and decoded journal, TRACE tracing output, sinks, data dir and object root. An other-origin grant is refused before any send, a templated request returns the 302, and provider echoes in 9 encodings are scrubbed. Objects work through fx-object on the local backend with presigned URLs served by the Ostro route. The S3 leg SKIPs loudly here. All five secret providers resolve through the chain. No product change was needed.**

## Canary harness (`crates/ostro-testkit/src/canary.rs`, feature `canary`)

- `Canary::new()` gives 32 random base62 characters (OS-keyed `RandomState` + time + pid, fresh per call). `Debug` prints `Canary(<redacted>)`. `expose()` / `expose_str()` exist only to configure the secret.
- `needles()` lists the searched forms, deduplicated by bytes:
  - raw;
  - base64 std/url, padded/unpadded, of the value alone, plus the **stable middle** at alignments 0/1/2 (whole 4-character groups of `prefix_k || value`, which any larger blob containing the value carries);
  - hex lower/upper;
  - percent-encoding of every byte (upper/lower) and the unreserved-preserving form;
  - JSON escaping (serde form, upper `\u00XX`, ASCII-only with surrogate pairs).
- `scan_bytes(label, bytes)` does a memmem over the needles. `scan_tree(dir)` reads every regular file and follows no symlinks. `Report` keeps a per-location `files/bytes/hits` count and has `summary()`. `assert_clean()` panics naming location, encoding and offset only.
- `CaptureLayer`:
  - It is a registry layer, installed with `install_global()` (OnceLock; panics if another global subscriber exists). There is no level filter and `RUST_LOG` is ignored. `log` records are bridged through `try_init`.
  - It records span creation, span records and events, using Debug and Display for strings and errors.
  - `watch(&Canary)` scans every line as it is recorded. `captured()` returns the stored buffer, capped at 256 MiB. `stats()` reports lines, bytes, truncation and the busiest targets.
  - `Report::tracing()` scans the buffer plus the streamed hits. If the buffer was truncated and the canary was never watched, it refuses to report clean.
- `register_sink(name, Box<dyn Fn() -> Vec<u8>>)`, `sink_names()`, `scan_sinks(&Canary)` and `Report::sinks()` are the **D-FX6 hook**. Phase 8 registers its OTLP mock collector here, and every canary report then scans it.
- `spawn_quiet(fut) -> Quiet<T>` runs the future on a new OS thread named `ostro-testkit-quiet-N`, with its own current-thread runtime and a thread-local `NoSubscriber`. `Quiet` derefs to the output, and dropping it stops the runtime and joins the thread.
- Unit tests (19 in the crate, 9 canary):
  - every needle is found embedded in noise;
  - the value is found inside larger base64 blobs at offsets 0..5;
  - a one-byte-different value matches nothing;
  - `scan_tree` skips a symlink to a dir holding the value;
  - report panics carry no value;
  - sinks are scanned;
  - the capture records events and span records, and drops quiet-thread, mock-target and compiler-target lines; streamed hits are reported.

## SC3: `crates/ostro-server/tests/fx_canary.rs`

The fixture configures everything over REST, and every REST response body is kept for the scan:
- secret `canary-key` = canary, granted to fx-canary for the mock origin, header only;
- secret `other-key`, granted only to `https://elsewhere.test`;
- egress fx-canary → mock;
- object connection `files` (local, root in a temp dir), with egress fx-object → `connection:files`.

The recording mock runs under `spawn_quiet`.

| Test | Proves |
|------|--------|
| `leak_scan` (cases 1 + 5) | fx-canary POSTs `/echo` with `X-Api-Key: {{secret:canary-key}}`. The mock **received exactly the canary**, and the guest's echoed `x-api-key` is `[REDACTED]`. env/args/random hold no needle (random is 32 hex). One fx-object put/get of non-secret data populates the object root. Full scan clean |
| `grant_origin_refused` (case 2) | `{{secret:other-key}}` toward the mock: the guest gets `secret_not_granted`, and the mock's `total_requests` is unchanged |
| `no_redirect` (case 3) | Templated POST to `/redirect?to=<mock>/echo`: the guest sees 302 with `Location` = target. `/redirect` 1, `/echo` 0 |
| `echo_scrubbed_encodings` (case 4) | The grant is replaced with `body = true` and fx-canary runs with `in_body`. `/enc?as=` returns the received value as raw, b64, b64url-nopad, b64 at offset 1 and 2 inside a larger blob, hex, HEX, pct-all and JSON, in the body and in an `x-echo` header. For each form the guest body contains `[REDACTED]` and the guest output has 0 needles. The mock received the canary in all 9 headers, and every body was `{"secret":"<canary>"}`, so body templating ran |

Every test ends with the same full scan, then a clean shutdown, then the file trees.

### Scanned locations and hits (leak_scan run, `--nocapture`)

| Location | Size | Hits |
|---|---|---|
| REST responses: 7 config PUTs, 2 run starts, runs list, secrets, grants, connections, egress, connection get, `/openapi.json`, `/metrics`, per run `GET /v1/runs/{id}` + history | ~85 KB | 0 |
| Guest outputs (observed response, env, args, random; fx-object output) | 681 B | 0 |
| gRPC: Runs.Get (Debug + `result_json`), Runs.History (summary + `details_json`), Runs.List, ListSecrets, ListGrants, ListConnections (+ `config_json`), ListEgress | 8,016 B | 0 |
| Journal raw events (`encode_event` bytes) | 2,024 B | 0 |
| Journal Debug | 9,551 B | 0 |
| Effect outcomes (http body + header values, object bytes) + stored results | 1,036 B | 0 |
| Tracing buffer (TRACE, no filter) | 26.9 MB | 0 |
| Tracing streamed (every line since `watch`) | 27.7 MB | 0 |
| Sinks | none registered in Phase 6 | 0 |
| Data dir after clean shutdown (WAL, state, meta, blobs) | 279 files, 270 MB | 0 |
| Object root | 1 file | 0 |

The other three tests also scanned clean, for example `echo_scrubbed_encodings`: 9 guest outputs, 28 KB gRPC, 8 KB raw journal, data dir 279 files. Capture statistics after the binary: about 425k lines and 49 MB. The busiest targets are `ostro_wal::group_commit`, bridged `log` records and `h2::*`. The buffer was never truncated.

**Non-vacuity controls:** the provider received the real value; the guest saw `[REDACTED]`; the capture contains each run id from the server's `activation` span; `log` records are present. The harness unit test shows events and span records are captured.

## SC5: object store through a guest (`fx_object_e2e.rs`)

- **`local_backend_through_guest`**:
  - fx-object on `files` runs put `a/b.txt` (`e_tag` returned) → get `hello` → head `size: 5` → presign-get → presign-put. Both URLs start with `http://<server>/v1/objects/files/`, and the file exists under the root.
  - An unauthenticated reqwest client gets 200 `hello` from the presigned GET, and its PUT of new content returns 200.
  - A tampered signature returns 403 for both GET and PUT.
  - A second run gets the new content, delete returns empty output, and get returns `not_found`.
  - Connection `files-short` (`presign_expiry = "1s"`) returns 200 at once and 403 after 2 s.
  - With the egress policy changed to lack `connection:files`, the result is `egress_denied`.
- **`s3_backend_through_guest`**: the same sequence against `OSTRO_TEST_S3_*`. The secret key is a sealed `{{secret:s3-secret}}`; the access key is templated as `{{secret:s3-key}}` when it is at least 8 bytes, otherwise literal. The presigned GET and PUT (with the signed `text/plain`) are used by a plain reqwest client. **Status here: SKIPPED.** `OSTRO_TEST_S3_URL` is unset, so it printed `SKIP: OSTRO_TEST_S3_URL unset: S3 object leg not run (human_needed locally; CI SeaweedFS, D-FX5/D-FX10)`. It runs in CI against SeaweedFS (D-FX10). No MinIO.

## SC5: secret providers (`fx_providers_e2e.rs`)

`all_five_providers_resolve_through_the_chain` uses `[secrets] providers = ["sealed","env","file","vault","k8s"]` with `file_dir`, `[secrets.vault]` (mock, mount `kv`, token auth via `token_file`, `cache_ttl = "0s"`) and `[secrets.k8s]` (temp kubeconfig with a bearer token, namespace `ostro`). The echo, Vault and K8s mocks all run under `spawn_quiet`. One fx-http run per name POSTs `/echo` with `X-Api-Key: {{secret:<name>}}`:

```
sealed-key           <- sealed   (REST PUT /v1/secrets)
env-key              <- env      (OSTRO_SECRET_ENV_KEY, set before start under the env mutex)
file-key             <- file     (file_dir/file-key, trailing newline stripped)
app/token            <- vault    (KV v2 kv/app/token, field value)
app-secret/api-key   <- k8s      (Secret app-secret, key api-key)
dup-key              <- env before file
```

Each run asserts that the provider received exactly the expected value, the guest saw `[REDACTED]`, and exactly one request was sent. `provider_order_file_before_env` uses `providers = ["file","env","sealed"]`, and the same `dup-key` resolves to the file value.

## Verification

All runs were sequential with `CARGO_TARGET_DIR=target/wt-06-19`.

- `cargo test -p ostro-testkit --features canary --locked`: **test result: ok. 19 passed**
- `cargo clippy -p ostro-testkit --all-features --all-targets --locked -- -D warnings`: clean
- `cargo test -p ostro-server --test fx_canary --locked -- --test-threads=1`: **test result: ok. 4 passed** (37-40 s)
- `cargo test -p ostro-server --test fx_object_e2e --locked -- --test-threads=1`: **test result: ok. 2 passed** (S3 leg SKIP)
- `cargo test -p ostro-server --test fx_providers_e2e --locked -- --test-threads=1`: **test result: ok. 2 passed**
- `cargo clippy -p ostro-server --all-targets --all-features --locked -- -D warnings` and the same without `--all-features`: clean. `cargo build -p ostro-server -p ostro-testkit --all-targets --all-features`: 0 warnings. `cargo fmt --all --check`: clean.
- The tests above were re-run on the merged main after the commits.
- **Not run:** `bash scripts/ci-local.sh --no-musl`. 06-18, 08-14, 08-16 and a debugger were building in parallel. This plan adds three test files and one testkit module, with no product, Cargo or shared-file changes.

## Deviations from Plan

**1. [Rule 3 - Blocking] The compiler targets are left out of the capture.**
- The first run, with a literal allow-everything filter, reached **13 GB RSS** and 8+ CPU-minutes inside the first test without finishing.
- The cause was cranelift/regalloc2 TRACE IR dumps of every compiled guest function in a debug build.
- `CaptureLayer` now disables only `EXCLUDED_TARGET_PREFIXES`: cranelift, regalloc2, wasmtime's cranelift glue and `wasmtime_environ::compile`. Their sole input is guest wasm code, and secrets are resolved by the bridge at send time, so no secret value can reach them.
- Everything else (server, bridge, hyper/h2, the WAL, bridged `log`) is still captured at TRACE.
- The buffer is capped at 256 MiB, and `watch()` scans every line as it is recorded, so the cap never hides a leak. The scan was not weakened for a hit: no hit was ever found.

**2. [Plan defect] Vault name.** The template grammar (`[a-z0-9_.-/]`) has no `#`, so `{{secret:kv/app#token}}` is invalid. The Vault secret is `app/token` (mount `kv`, default field `value`).

**3. [Plan detail] Header shape.** fx-canary sets the header value to exactly `{{secret:<name>}}`, so case 1 uses `X-Api-Key: {{secret:canary-key}}` rather than `Bearer {{secret:canary-key}}`. The guest contract is fixed by 06-09.

**4. [Plan detail] Test-local recording mock.** `MockProvider` (ostro-testkit, outside this plan's files) does not record header values and has no encoding switch. fx_canary.rs and fx_providers_e2e.rs therefore carry small axum mocks that record `X-Api-Key` values (and bodies), started through `spawn_quiet`. The Vault/K8s handlers are minimal copies of the 06-13 mock shapes.

**5. [Plan detail] `spawn_quiet(fut)` returns `Quiet<T>`.** The mock's handle must outlive the call, so the thread keeps its runtime alive until the `Quiet` is dropped.

**6. [Plan detail] The raw journal is read directly.** The raw journal comes from `TestServer::journal` + `encode_event` (the exact stored bytes per event), alongside REST history and gRPC history. No export tool was used.

**7. Data-dir scan caveat (recorded, not a gap).** The data-dir scan reads raw files. If a storage layer compressed blocks, a value there would not be visible to a byte scan. The decoded journal, the stored results and every outcome are scanned separately, which covers the same content uncompressed.

**8. Commits.** Task 1 is a feat commit. The harness changes from deviation 1 went into the Task 2 commit together with fx_canary.rs. Task 3 is its own commit.

## Deferred

- **OTLP leg of SC3 (D-FX6):** Phase 8 calls `ostro_testkit::canary::register_sink("otlp", ..)` with its mock collector's received bytes. `Report::sinks()` in fx_canary (or the Phase 8 gate test) then scans it.
- **S3 leg:** human_needed locally. It runs in CI against SeaweedFS.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | f653e17 | feat(06-19): canary leak harness in ostro-testkit (feature canary) |
| 2 | a9d774c | test(06-19): SC3 canary e2e: leak scan, grant origin, no redirect, echo encodings |
| 3 | 9f519ae | test(06-19): SC5 e2e: object store through a guest and all five secret providers |

## Self-Check: PASSED

- FOUND: crates/ostro-testkit/src/canary.rs, crates/ostro-server/tests/{fx_canary,fx_object_e2e,fx_providers_e2e}.rs on main
- FOUND: f653e17, a9d774c, 9f519ae on main
