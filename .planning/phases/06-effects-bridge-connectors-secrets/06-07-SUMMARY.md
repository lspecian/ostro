---
phase: 06-effects-bridge-connectors-secrets
plan: 07
subsystem: effects-bridge
tags: [bridge, driver, registry, rate-limit, governor, outcome, config, metrics]
requires:
  - "06-02 contract (Connector/SendGate/CompletionSink/Target/EffectCtx, QuotaSpec, bridge_fail!, kinds)"
  - "06-04 EgressConfig.rate_max_wait and egress::config::{parse_duration, parse_bytes}"
  - "06-05 SecretsConfig (re-exported from config.rs)"
provides:
  - "ostro_bridge::driver::{Bridge, EffectBase, kind_label} (06-14 adapts Bridge to the engine's EffectDriver)"
  - "ostro_bridge::registry::{EffectRegistry, TaskEntry, EffectKey, Insert}"
  - "ostro_bridge::ratelimit::{RateLimiter, QuotaSpec::{parse, parse_with_burst, to_quota}}"
  - "ostro_bridge::outcome::{encode_outcome, non_retryable, retryable, json_payload, outcome_label, INTERNAL_ERROR, CONNECTOR_PANICKED}"
  - "ostro_bridge::config::ConnectorsConfig ([connectors]) + re-exports EgressConfig, SecretsConfig, parse_duration, parse_bytes"
affects: [06-10, 06-11, 06-12, 06-14, 06-17, 06-20]
tech-stack:
  added: []
  patterns:
    - "supervisor task + worker task: a panic surfaces as JoinError::is_panic and becomes one internal_error completion"
    - "registry entries carry a generation id; a cancelled entry is replaced on re-start, a stale task's remove is a no-op"
    - "own governor wait loop (check + tokio sleep + small deterministic jitter) because governor's jitter feature is off"
key-files:
  created:
    - crates/ostro-bridge/tests/driver.rs
    - crates/ostro-bridge/tests/ratelimit.rs
  modified:
    - crates/ostro-bridge/src/driver.rs
    - crates/ostro-bridge/src/registry.rs
    - crates/ostro-bridge/src/ratelimit.rs
    - crates/ostro-bridge/src/outcome.rs
    - crates/ostro-bridge/src/config.rs
key-decisions:
  - "Inflight gauge is named ostro_effect_inflight (not ostro_effects_inflight): the reserved metric prefix is ostro_effect_ (RESERVATIONS §11, P-14)"
  - "QuotaSpec::parse burst now defaults to the per-second rate (plan spec); 06-02 had made a /m burst equal to the per-minute count. parse_with_burst(rate, burst) serves the connection-config rate+burst keys"
  - "governor is built without its jitter/quanta features (06-02 pin, std only), so until_ready_with_jitter is unavailable; RateLimiter polls check() and sleeps on the tokio timer with up to 5 ms deterministic jitter, under tokio::time::timeout(max_wait)"
  - "Rate-limit tests use the real clock: ostro-bridge's dev tokio has no test-util (no tokio::time::pause) and governor's clock is std Instant; bounds are generous"
  - "failpoint bridge::effect::before_complete_submit: return(error) drops the completion (simulated loss, recovered by the leader-start re-drive); hang/wait are cancellable"
  - "A rate_limited or unsupported_op failure is completed only if the gate still says Proceed"
  - "Bridge::start after shutdown is ignored; start outside a runtime falls back to the runtime handle captured in Bridge::new"
metrics:
  duration: "~25 min"
  completed: 2026-11-20
---

# Phase 6 Plan 07: Bridge Driver Core Summary

**The bridge driver core is in place. `Bridge` owns effect task lifecycle: one node-owned task per `(partition, run, op)`, attach on a duplicate start, gate, per-target governor rate limit, execute raced against cancellation, encode once, completion submission with `Unavailable` backoff, and a panic turned into exactly one `internal_error` completion. `RateLimiter` keeps a governor direct limiter per target with a bounded wait that fails `rate_limited`. `ConnectorsConfig` covers `[connectors]`. Connectors (06-10/11/12) now only implement `target` and `execute`.**

## Final API

```rust
// ostro_bridge::driver
pub struct EffectBase { pub cluster_id: String, pub partition: u16, pub run_id: String,
                        pub attempt: u32, pub module: String, pub tenant: Option<String> }
pub struct Bridge;
impl Bridge {
    pub fn new(cfg: ConnectorsConfig, rate_max_wait: Duration /* EgressConfig.rate_max_wait */,
               gate: Arc<dyn SendGate>, sink: Arc<dyn CompletionSink>, limiter: Arc<RateLimiter>) -> Self;
    pub fn register(&mut self, connector: Arc<dyn Connector>);   // one per EffectKind; later replaces
    pub fn config(&self) -> &ConnectorsConfig;
    pub fn limiter(&self) -> &Arc<RateLimiter>;
    pub fn start(&self, base: EffectBase, effect: LiveEffect);   // non-blocking, idempotent per key
    pub fn cancel(&self, partition: u16, run_id: &str, op: u64);
    pub fn cancel_run(&self, partition: u16, run_id: &str);
    pub fn inflight(&self) -> usize;
    pub async fn shutdown(&self, grace: Duration);
}
pub const fn kind_label(kind: EffectKind) -> &'static str;      // "http" | "sql" | "object"
pub const DURATION_METRIC, COMPLETED_METRIC, ATTACH_METRIC, FP_BEFORE_COMPLETE_SUBMIT: &str;

// ostro_bridge::ratelimit
impl QuotaSpec { pub fn parse(&str) -> Result<Self, QuotaParseError>;      // burst = per_second
                 pub fn parse_with_burst(rate: &str, burst: Option<u32>) -> Result<Self, QuotaParseError>;
                 pub fn to_quota(&self) -> governor::Quota; }
pub struct RateLimiter;
impl RateLimiter { pub fn new(default: Option<QuotaSpec>) -> Self;
                   pub fn default_quota(&self) -> Option<&QuotaSpec>;
                   pub fn targets(&self) -> Vec<String>;
                   pub async fn acquire(&self, target: &Target, max_wait: Duration) -> Result<(), Failure>; }
pub const RATE_LIMITED_METRIC: &str = "ostro_effect_rate_limited_total";

// ostro_bridge::outcome
pub fn encode_outcome(r: &Result<OpOutput, Failure>) -> RecordedResult;    // == encode_result(r)
pub fn non_retryable(kind: &str, message: impl Into<String>, details: Option<serde_json::Value>) -> Failure;
pub fn retryable(kind: &str, message: impl Into<String>) -> Failure;
pub fn json_payload(v: &serde_json::Value) -> Payload;                     // content_type application/json
pub fn outcome_label(r: &Result<OpOutput, Failure>) -> String;            // ok | kind | timeout | cancelled
pub const INTERNAL_ERROR: &str = "internal_error";
pub const CONNECTOR_PANICKED: &str = "bridge connector panicked";

// ostro_bridge::config
pub struct ConnectorsConfig { sql_max_rows: u64 /*10000*/, sql_max_bytes: u64 /*4MiB*/,
    sql_effects_retention: Duration /*168h*/, object_max_get_bytes: u64 /*16MiB*/,
    presign_default_expiry: Duration /*15m*/, backend_ttl: Duration /*5m*/,
    default_rate: Option<QuotaSpec> /*unset*/, public_base_url: Option<String> /*unset*/ }
impl ConnectorsConfig { pub fn validate(&self) -> Result<(), String>; }
pub use crate::egress::{EgressConfig, config::{parse_duration, parse_bytes}};
pub use crate::secrets::SecretsConfig;

// ostro_bridge::registry
pub type EffectKey = (u16, String, u64);
pub struct TaskEntry { pub cancel: CancellationToken, pub started_at: Instant, pub kind: EffectKind, pub id: u64 }
pub enum Insert { New { id: u64, cancel: CancellationToken }, Attached }
impl EffectRegistry { new, try_insert, remove(key, id), get, cancel, cancel_run, cancel_all, len, is_empty }
```

ostro-types has no `Payload` constructor. `json_payload` builds `Payload{content_type: "application/json", data}` directly.

## Pipeline (driver.rs)

1. `connector.target()`.
2. `limiter.acquire(target, min(rate_max_wait, http timeout_ms))`, raced with cancel. On `rate_limited` the gate is checked, and the failure is completed only on `Proceed`.
3. `gate.check`. `Abandon` means no execution and no completion.
4. `connector.execute(ctx, effect, gate)` raced with `ctx.cancel`. If the token is cancelled first, or by the time execute returns, there is no completion.
5. `encode_outcome` runs once. Metrics `ostro_effect_duration_seconds{kind}` and `ostro_effect_completed_total{kind,outcome}` are recorded.
6. `bridge_fail!("bridge::effect::before_complete_submit")`.
7. `sink.complete`. `Unavailable` is retried with backoff (100 ms doubling to 5 s) until cancelled. `Dropped` is logged at debug.

A supervisor task awaits the worker's `JoinHandle`. On a panic it logs `op`/`kind`, completes `NonRetryable{internal_error, "bridge connector panicked"}` once (unless submission had already begun or the token was cancelled), and removes the entry. It never respawns. A start with no registered connector completes `NonRetryable{unsupported_op}`, after the gate.

## Verification

- `cargo test -p ostro-bridge --test ratelimit --locked`: **4 passed**.
- `cargo test -p ostro-bridge --lib --locked -- outcome ratelimit config`: **17 passed**.
- `cargo test -p ostro-bridge --test driver --locked`: **14 passed**.
- `cargo test -p ostro-bridge --features failpoints --test driver --locked -- --test-threads=1`: **15 passed** (adds the before_complete_submit error/wait/hang case).
- `cargo test -p ostro-bridge --locked`: lib 94, driver 14, egress_client 5, ratelimit 4, secrets 10, ssrf_table 5, all ok.
- The same suite with `--features failpoints -- --test-threads=1`: lib 94, driver 15, all ok.
- `cargo clippy -p ostro-bridge --all-targets --all-features --locked -- -D warnings` and `--no-default-features`: clean.
- `cargo fmt -p ostro-bridge --check`: clean. The build is warning-free.
- Not run: full `bash scripts/ci-local.sh`. Four other executors were building in parallel, and only ostro-bridge files changed (no other crate uses these modules yet).

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Reserved name] Inflight gauge renamed to `ostro_effect_inflight`.** The plan names it `ostro_effects_inflight`, which falls outside the reserved Phase 6 prefix `ostro_effect_` (RESERVATIONS §11). All driver metrics use `ostro_effect_`.

**2. [Rule 3 - Blocking] No `until_ready_with_jitter`, and a real clock in tests.** The governor pin is `std` only, so `Jitter` and `until_ready_with_jitter` are crate-private. tokio has no `test-util` in ostro-bridge's dev-deps, so `tokio::time::pause` is unavailable. Cargo edits belong to 06-02. The limiter therefore runs its own `check()` + tokio sleep loop with a small deterministic jitter. The tests use the real clock with generous bounds. The limiter type is governor's `DefaultDirectRateLimiter`, as specified.

**3. [Plan spec vs 06-02] `QuotaSpec::parse` burst.** The burst now defaults to the per-second rate, as the plan specifies. 06-02 had set the `/m` burst to the per-minute count, and its unit test in ratelimit.rs was updated to match. The public shape is unchanged (fields kept). `parse_with_burst` and `to_quota` were added.

**4. [Rule 2] Extras needed for correctness:**
- registry generation ids, plus replacement of a cancelled entry, so a re-drive after a cancel is not swallowed by a still-exiting task;
- `start` is refused after `shutdown`;
- the gate is checked before completing `rate_limited`/`unsupported_op`;
- the failpoint `return(error)` drops the completion.

All are documented in the module docs.

## Notes for later plans

- **06-14:** wrap `Bridge` (it is `Send + Sync`; tasks hold an internal `Arc`, so they survive dropping the `Bridge`) in the engine `EffectDriver` adapter. Pass `EgressConfig.rate_max_wait` to `Bridge::new`, and `ConnectorsConfig.default_rate` to `RateLimiter::new`.
- **06-10/11/12:** use `QuotaSpec::parse_with_burst(rate, burst)` for the `rate`/`burst` connection keys, and return `Target{key: origin | connection name, quota}` from `target()`. Cancel `ctx.cancel` to signal a late `Abandon` after your own gate re-check.
- **06-20:** document `internal_error` (bridge connector panicked) in the error table, along with the at-least-once boundary text in driver.rs.

## Commits

- `268fa2d`: feat(06-07): outcome helpers, [connectors] config and per-target rate limiter
- `255e506`: feat(06-07): Bridge facade and EffectRegistry with gate, rate limit and completion retry

## Self-Check: PASSED

- The files exist: driver.rs, registry.rs, ratelimit.rs, outcome.rs, config.rs, tests/driver.rs, tests/ratelimit.rs.
- Commits 268fa2d and 255e506 are on main.
