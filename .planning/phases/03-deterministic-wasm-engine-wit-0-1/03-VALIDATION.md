---
phase: 3
slug: deterministic-wasm-engine-wit-0-1
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-11-18
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 03-RESEARCH.md § Validation Architecture, mapped onto plans 03-01..03-12.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, the CI gate), cargo-nextest 0.9.x where installed (process-per-test isolation for abort-sensitive tests), proptest 1.x, insta 1.x |
| **Config file** | `.config/nextest.toml` (created by 03-01); guests workspace `guests/Cargo.toml` (separate workspace, excluded from root, `members = ["*"]`) |
| **Guest build** | Automatic: `crates/ostro-engine/tests/common/mod.rs::guest_wasm(name)` runs `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` once per test process (nested cargo verified to work inside `cargo test`) |
| **Quick run command** | `cargo test -p ostro-engine --lib && cargo test -p ostro-types` (replay core, limits, classify, codec/hash; PROPTEST_CASES=64 for speed) |
| **Full suite command** | `cargo test -p ostro-engine -p ostro-types` (or `cargo nextest run -p ostro-engine -p ostro-types --profile ci`) |
| **Phase gate command** | `bash scripts/ci-local.sh` (fmt, clippy -D warnings, warning-free build, workspace tests, cargo-deny, vendored check, musl zigbuild release + test) |
| **Estimated runtime** | quick < 20 s; full ~2–4 min in debug (1,000-run determinism tests dominate) |

---

## Sampling Rate

- **After every task commit:** Run the quick command (plus the task's own `<automated>` verify).
- **After every plan wave:** Run the full suite command.
- **Before `/gsd:verify-work`:** `bash scripts/ci-local.sh` green locally; determinism/golden tests must agree on x86_64 and aarch64 (and macOS) once the remote CI legs run (human_needed until the repo is pushed, same as FOUND-05).
- **Max feedback latency:** 60 s for task-level checks.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 1 | WIT-01..06 | build | `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` | ❌ W0 | ⬜ pending |
| 03-01-02 | 01 | 1 | WIT-01 | integration | `cargo test -p ostro-engine --test scaffold` | ❌ W0 | ⬜ pending |
| 03-01-03 | 01 | 1 | ENG-03, ENG-04 (types) | unit | `cargo test -p ostro-types` | ❌ W0 | ⬜ pending |
| 03-02-01 | 02 | 2 | ENG-03, ENG-04 | unit + golden + proptest | `cargo test -p ostro-types --test journal_golden` | ❌ W0 | ⬜ pending |
| 03-03-01 | 03 | 2 | ENG-01, ENG-05 | unit + integration + insta | `cargo test -p ostro-engine --lib limits && cargo test -p ostro-engine --test engine_config` | ❌ W0 | ⬜ pending |
| 03-03-02 | 03 | 2 | ENG-08 | integration (+ prlimit subprocess, linux) | `cargo test -p ostro-engine --test pool -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 03-04-01 | 04 | 2 | ENG-02 | integration | `cargo test -p ostro-engine --test wasi_shim` | ❌ W0 | ⬜ pending |
| 03-04-02 | 04 | 2 | ENG-02 | integration + insta | `cargo test -p ostro-engine --test validate` | ❌ W0 | ⬜ pending |
| 03-05-01 | 05 | 2 | ENG-03, ENG-04, WIT-02..05 | unit + proptest + golden | `cargo test -p ostro-engine --lib replay` | ❌ W0 | ⬜ pending |
| 03-06-01 | 06 | 2 | ENG-07 | integration | `cargo test -p ostro-engine --test cache -- cache_key compat hmac_key cache_roundtrip cache_tamper cache_truncated` | ❌ W0 | ⬜ pending |
| 03-06-02 | 06 | 2 | ENG-07 | integration | `cargo test -p ostro-engine --test cache` | ❌ W0 | ⬜ pending |
| 03-07-01 | 07 | 3 | WIT-01, ENG-02 | build + clippy | `cargo build -p ostro-engine --locked && cargo clippy -p ostro-engine --all-targets -- -D warnings` | n/a | ⬜ pending |
| 03-07-02 | 07 | 3 | WIT-01..06 | build + clippy | `cargo build -p ostro-engine --locked && cargo clippy -p ostro-engine --all-targets -- -D warnings` | n/a | ⬜ pending |
| 03-07-03 | 07 | 3 | WIT-01, ENG-02/03/04 | integration | `cargo test -p ostro-engine --test host_adapter` | ❌ W0 | ⬜ pending |
| 03-08-01 | 08 | 4 | ENG-05, ENG-09 | unit | `cargo test -p ostro-engine --lib activation` | ❌ W0 | ⬜ pending |
| 03-08-02 | 08 | 4 | ENG-03, ENG-09, WIT-06 | integration (FakeSink) | `cargo test -p ostro-engine --test activation` | ❌ W0 | ⬜ pending |
| 03-09-01 | 09 | 5 | ENG-03 | integration | `cargo test -p ostro-engine --test determinism -- divergence_` | ❌ W0 | ⬜ pending |
| 03-09-02 | 09 | 5 | ENG-03, ENG-04 (SC1) | integration (slow) + insta | `cargo test -p ostro-engine --test determinism` | ❌ W0 | ⬜ pending |
| 03-10-01 | 10 | 5 | ENG-05 (SC2) | integration | `cargo test -p ostro-engine --test sandbox` | ❌ W0 | ⬜ pending |
| 03-10-02 | 10 | 5 | ENG-06 (SC2) | integration + subprocess control | `cargo test -p ostro-engine --test stack_depth -- --test-threads=1 --nocapture` | ❌ W0 | ⬜ pending |
| 03-12-01 | 12 | 5 | WIT-01, WIT-02, WIT-03 (SC5) | integration (FakeSink) | `cargo test -p ostro-engine --test wit_surface -- wit_surface cancel_ terminate continue_as_new` | ❌ W0 | ⬜ pending |
| 03-12-02 | 12 | 5 | WIT-04, WIT-05, WIT-06, ENG-04 (SC5) | integration (FakeSink) | `cargo test -p ostro-engine --test wit_surface` | ❌ W0 | ⬜ pending |
| 03-11-01 | 11 | 6 | ENG-09 (SC4) | integration (FakeSink, live inputs) | `cargo test -p ostro-engine --test hot_wait --test activation --test determinism --test wit_surface --test sandbox` | ❌ W0 | ⬜ pending |
| 03-11-02 | 11 | 6 | ENG-07, ENG-08 (SC3, SC4) | integration + perf assert | `cargo test -p ostro-engine --test runtime_cache --test validate && bash scripts/ci-local.sh` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

### Requirement → test coverage

| Req | Behaviour proven | Tests |
|-----|------------------|-------|
| ENG-01 | NaN canon + deterministic relaxed SIMD + threads/GC off; config pinned | `engine_config_snapshot`, `engine_config_nan_canonical`, `engine_config_threads_gc_off` |
| ENG-02 | Deterministic WASI shim; allow-list validation; import probe | `wasi_shim_*`, `validate_*`, `validate_wasi_probe_imports` snapshot |
| ENG-03 | kind + hash comparison; DLQ reason with seq/expected/actual; trailing commands | `replay_property` (proptest), `finish_trailing_commands`, `divergence_*`, `activation_nondeterminism_outcome`, `thousand_*` |
| ENG-04 | Exact journaled bytes back to guest; codec/hash golden | `golden_event_codec`, `golden_result_codec`, `golden_command_hash`, `host_replay_exact_bytes`, `effect_bytes_exact` |
| ENG-05 | Memory cap, fuel per step/run, epoch deadline; attempt-only failure | `limiter_traps_never_false`, `sandbox_*`, `sandbox_isolation` |
| ENG-06 | Max-depth host calls never abort | `stack_depth_every_import_at_max_depth`, `stack_depth_negative_control` |
| ENG-07 | Bounded compile pool, single-flight, keyed cache, HMAC tamper → recompile | `cache_*`, `compile_pool_bounded`, `compile_single_flight`, `tamper_then_recompile`, `compat_miss_recompiles`, `runtime_cache_*` |
| ENG-08 | InstancePre + pooling < 1 ms p50; configurable pool; clear failure + fallback | `pool_*`, `runtime_warm_instantiate_p50` |
| ENG-09 | Hot-wait vs evict; LRU budget eviction; bounded-budget replay correctness | `activation_det_basic_completes`, `hot_wait_*`, `bounded_budget_many_runs` |
| WIT-01 | World compiles both sides; every import + both exports work | `smoke_guest_compiles_and_runs`, `host_surface_journals_every_command`, `wit_surface_every_import` |
| WIT-02 | Cancel observed via cancellation op, cleanup journaled; no guest terminate | `wait_cancellation_op`, `cancel_cleanup_runs_after_cancel`, `terminate_is_not_guest_visible` |
| WIT-03 | continue-as-new outcome + suggestion threshold | `continue_as_new_outcome`, `continue_as_new_suggested_threshold` |
| WIT-04 | Await token embeds partition/run/op, completes without run id, replays without key | `await_token` (unit), `await_token_roundtrip`, `await_token_replay_without_key` |
| WIT-05 | set-status journaled, kind-only hash, published from journal | `set_status_journaled_once`, `set_status_changed_text_no_divergence` |
| WIT-06 | Non-retryable kind, run-timeout-ms, run-info deadline, epoch timeout | `non_retryable_failure`, `activation_describe_run_timeout`, `run_info_deadline`, `sandbox_loop_epoch` |

---

## Wave 0 Requirements

Wave 0 is folded into plan 03-01 (wave 1) and the first task of each plan; no test file exists yet.

- [ ] `guests/` workspace (`members = ["*"]`, exact pins wit-bindgen =0.63.0, rand =0.9.5; lock gitignored) + `smoke` guest — 03-01
- [ ] `crates/ostro-engine/tests/common/mod.rs` with `guest_wasm()` (env-scrubbed nested cargo build) — 03-01
- [ ] `wit/ostro-runtime-0.1.0/runtime.wit` — 03-01
- [ ] `.config/nextest.toml`; `cargo install --locked cargo-nextest` (optional; `cargo test` remains the gate) — 03-01
- [ ] proptest + insta dev-deps (workspace pins) — 03-01
- [ ] `crates/ostro-engine/src/testkit.rs` FakeSink + scripted inputs (feature `testkit`) — 03-08
- [ ] Golden fixtures: journal codec/hash (`crates/ostro-types/tests/golden/journal_v1.txt`, 03-02), ChaCha streams/uuid/token constants (03-05), config snapshot (03-03), import-probe snapshot (03-04), golden det-basic journal (03-09)
- [ ] Vendored WASI 0.2.12 WIT at `crates/ostro-engine/wit-wasi/` — 03-04
- [ ] Guests: nan-probe (03-03), wasi-probe / wasi-forbidden / cli-command (03-04), det-basic / wit-surface (03-07), det-divergent (03-09), sandbox (03-10)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Determinism/golden snapshots agree on aarch64 and macOS | ENG-01, ENG-03, ENG-04 | No git remote yet; remote CI legs are human_needed (same as FOUND-05) | After the first push, confirm the Actions matrix (x86_64/aarch64 musl, macOS arm64) is green, in particular `determinism`, `engine_config` and `journal_golden` |
| Negative control for ENG-06 actually aborts with minimal headroom | ENG-06 | Abort behaviour with tiny fiber stacks is platform-dependent; the test passes with a warning if it does not abort unless `OSTRO_STACK_STRICT=1` | Run `OSTRO_STACK_STRICT=1 cargo test -p ostro-engine --test stack_depth -- --nocapture` and record the result in 03-10-SUMMARY |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60 s (task-level)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
