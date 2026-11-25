---
phase: 13-cluster-membership-routing-upgrades
plan: 08
subsystem: conformance harness (tiers B/C lifecycle) + Phase 13 oracles
tags: [harness, conformance, p13, membership, upgrade, level-emulation, ryw, gc, oracle, chaos-proxy]
requires:
  - phase: 13-02
    provides: "P13Harness trait + NodeOpts (harness/p13.rs, final), ServerOptions::with_level_max_override, ServerHandle::p13()/fatal(), `ostro cluster join` interception, joiner::run_join signature, oracle stub files"
  - phase: 13-03
    provides: "REST routes /v1/cluster/{init,tokens,status,members/{id}/{remove,drain}} mounted (501 `unimplemented` until 13-09); ostro_api::phase13 DTOs"
  - phase: 13-01
    provides: "ostro_types::compat::{Feature, LEVEL, LEVEL_MIN, TEST_LEVEL_MAX_ENV}, ext tags 96-102, Cmd 43 RepinRun, MetaCommand 8 AlterReplicas, InvalidCode::Empty"
provides:
  - "InProcCluster (tier B) and ProcCluster (tier C) implement P13Harness: start_single, cluster_init, add_node (node token + real join flow), remove, drain, upgrade, cluster_status"
  - "start_single_with(spec, level) in both tiers (a single node at an emulated level from its first boot)"
  - "tier C joined nodes on the next 127.r.r.k with chaos-proxy links to every node; isolate/heal/partition/kill take their id"
  - "inproc::{http_error, is_http_error, member_state, MEMBERSHIP_T}, ApiClient::post_json"
  - "oracle::p13_ryw::RywChecker, oracle::p13_membership::{removed_never_return, joint_left_within, voters_odd, StatusSample}, oracle::p13_upgrade::{no_gated_before_finalize(_from), no_gated_partition_commands, no_reject_continue, finalization_index, finalized_level, record_ext, OutcomeRecord}, oracle::p13_gc::{no_module_gone_for_live, RetireEvent, RunResult}"
  - "tests/p13_harness_smoke.rs (tier B + #[ignore] tier C), tests/p13_oracle_selftest.rs (11 tests)"
affects: [13-09, 13-10, 13-11, 13-12, 13-13, 13-14, 13-15, 13-16, 13-17, 13-18, 13-19]
tech-stack:
  added: []
  patterns:
    - "Phase 13 REST helpers return `METHOD path: HTTP <code>: <body>` for server answers, so a scenario can tell 'server said no' from 'never reached a server' (is_http_error)"
    - "upgrade = graceful stop without isolation + restart from the same data dir at the new level (tier B override, tier C env), fault rules untouched"
    - "oracles over plain observation types (status samples, outcome records, retire events) plus measured meta logs / WAL scans"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/p13_harness_smoke.rs
    - crates/ostro-repl-conformance/tests/p13_oracle_selftest.rs
  modified:
    - crates/ostro-repl-conformance/src/harness/inproc.rs
    - crates/ostro-repl-conformance/src/harness/process.rs
    - crates/ostro-repl-conformance/src/oracle/p13_ryw.rs
    - crates/ostro-repl-conformance/src/oracle/p13_membership.rs
    - crates/ostro-repl-conformance/src/oracle/p13_upgrade.rs
    - crates/ostro-repl-conformance/src/oracle/p13_gc.rs
key-decisions:
  - "`[cluster] advertise` stays the Phase 7 Vec<String> (orchestrator decision): tier B start_single sets [\"mem://1\"] (filtered out of the cert SANs by San::from_host; the in-memory transport routes by node id), tier C keeps [ip]; cluster_init sends InitRequest.advertise = the first advertise entry (tier B) / the cluster listener ip:17701 (tier C)"
  - "start_single imports the test CA (`[cluster.tls] ca_cert_file/ca_key_file`) so the harness's TestPki is the cluster CA a joiner pins"
  - "Smoke upgrades start at level 1 (start_single_with): a node started at LEVEL auto-finalizes to 2, and a restart at level 1 is then correctly refused by the boot fence"
  - "Tier C joined-node proxy links live in one extra ChaosProxy per join (chaos_proxy.rs is not this plan's file and has no add-link API); proxy_for(from, to) finds the right one"
  - "add_node / cluster_status smoke assertions accept success or an HTTP error answer (13-09 owns the success assertions); a transport error fails"
  - "p13_upgrade counts gated entries whatever their apply verdict (an older node replays committed bytes either way); partition RepinRun is bounded only by 'meta never finalized level 2', or exactly via no_gated_partition_commands on scans taken before finalizing"
duration: ~2h
completed: 2026-11-24
---

# Phase 13 Plan 08: P13Harness in tiers B and C + Phase 13 oracles Summary

**Both conformance tiers now implement `P13Harness`: single-node start, init, joins through the real join flow (a tier-C `ostro cluster join` process gets its own 127.r.r.k and chaos-proxy links), remove/drain with bounded waits, emulated upgrades (tier-B `level_max_override`, tier-C `OSTRO_TEST_LEVEL_MAX`) and status. The four Phase 13 oracles (RYW, membership, upgrade, GC) each pass a clean input and catch a damaged one in `p13_oracle_selftest`.**

## Harness API as landed

```rust
// trait (13-02, final): harness/p13.rs
pub trait P13Harness: ClusterHarness {
    fn start_single(spec) -> Result<Self>;            // spec.nodes must be 1
    fn cluster_init(&mut self, node) -> Result<Value>; // POST /v1/cluster/init {"advertise": …}
    fn add_node(&mut self, opts: NodeOpts) -> Result<u64>;
    fn remove(&mut self, id, force) -> Result<()>;     // POST …/remove {"force"}, wait state "removed" (MEMBERSHIP_T = 120 s)
    fn drain(&mut self, id) -> Result<()>;             // POST …/drain, wait until no assignment lists id as replica
    fn upgrade(&mut self, node, level_max: Option<u32>) -> Result<()>;
    fn cluster_status(&self, node) -> Result<Value>;   // GET /v1/cluster/status
}

// tier B (harness/inproc.rs)
impl InProcCluster {
    pub async fn start_single_with(spec: ClusterSpec, level_max: Option<u32>) -> Result<Self, HarnessError>;
    pub fn level_max(&self, node: u64) -> Option<u32>;
}
pub const MEMBERSHIP_T: Duration;                     // 120 s
pub fn http_error(method, path, StatusCode, &Value) -> HarnessError; // "POST /v1/…: HTTP 501: {…}"
pub fn is_http_error(&HarnessError) -> bool;
pub fn member_state(status: &Value, id: u64) -> Option<String>;
impl ApiClient { pub async fn post_json(&self, path, body: &Value) -> Result<(StatusCode, Value), HarnessError>; }

// tier C (harness/process.rs)
impl ProcCluster {
    pub async fn start_single_with(spec, opts: ProcOptions, level_max: Option<u32>) -> Result<Self, HarnessError>;
    pub fn level_max(&self, i: u64) -> Option<u32>;
    pub fn proxy_for(&self, from: u64, to: u64) -> Option<&ChaosProxy>; // Phase 12 proxy or a joined node's
}
pub const STOP_T: Duration;                           // graceful stop bound before SIGKILL (upgrade)
pub fn parse_joined(line: &str) -> Option<(String, u64)>; // "joined cluster <cid> as node <n> (data: …)"
```

**Tier B.** `start_single`: one server-profile node, no peers, `[cluster] advertise = ["mem://1"]`, test CA imported, cluster listener bound. `add_node(opts)`: `POST /v1/cluster/tokens` on the lowest running node -> `ostro_server::p13::joiner::run_join(JoinArgs { seed: <node API base url, http://…>, token, ca_hash, data_dir: <root>/join<k>, config: None, advertise_api/cluster only with opts.advertise, no_start: true })` -> boots the prepared data dir in process (`node_id` = the join's id, no peers, no CA files, test timings) with `level_max_override = opts.level_max`. `upgrade`: graceful `shutdown()` (no isolation; fault rules kept), boot again from the same config at the new override. `restart` also keeps the node's level.

**Tier C join command line** (spawned on the next free `127.r.r.k`, `k` above every address in use):

```
<failpoint ostro> cluster join http://<seed api ip:port> --token <njt_…> --ca-hash sha256:<hex> \
    --data <root>/join<k> -c <root>/join<k>.toml \
    [--advertise-api 127.r.r.k:<port> --advertise-cluster 127.r.r.k:17701]   # with opts.advertise
```

`join<k>.toml`: api `127.r.r.k:<pre-picked free port>`, cluster listen `127.r.r.k:17701`, `advertise = ["127.r.r.k"]`, the cluster id, partitions, test timings, `OSTRO_TEST_LEVEL_MAX=<opts.level_max>` in the env. The harness waits for the ready line, reads the id from the `joined cluster … as node <n>` line in `join<k>.log`, writes `node_id = n` and the pinned API port into the config (restarts run `ostro server -c join<k>.toml`) and starts one extra ChaosProxy with links `n -> m` (`127.r.r.<m>:17800+n` -> m's cluster listener) and `m -> n` (`127.r.r.k:17800+m` -> `127.r.r.k:17701`) for every node m. `isolate`/`heal`/`partition` apply to every proxy; `kill` is `kill -9` as for any node. `upgrade(node, Some(k))`: SIGTERM, wait up to 60 s (then SIGKILL), respawn with `OSTRO_TEST_LEVEL_MAX=k` (`None` removes it).

## Oracle signatures

```rust
// oracle::p13_ryw
pub enum Effect { RunExists{run_id}, SignalVisible{run_id, request_id}, RunTerminal{run_id}, ActiveVersion{workflow, version}, Policy{key, value} }
pub enum Observation { Run{run_id, found, status, signals}, ActiveVersion{workflow, version: Option<u32>}, Policy{key, value: Option<Value>} }
impl Observation { pub fn run_from_json(run_id, found, body: &Value, signals: Vec<String>) -> Self; }
pub struct RywChecker; impl RywChecker { pub fn new(); pub fn session(&self, id) -> Session; pub fn check(&self) -> Report; }
impl Session { pub fn ack(&self, Effect); pub fn observe(&self, Observation); pub fn begin_read(&self) -> ReadTicket; pub fn observe_issued(&self, ReadTicket, Observation); }

// oracle::p13_membership
pub struct StatusSample { pub at: Duration, pub node: u64, pub status: ostro_api::phase13::ClusterStatus }
impl StatusSample { pub fn from_json(at, node, &Value) -> Result<Self, HarnessError>; }
pub fn removed_never_return(meta_logs: &BTreeMap<u64, MetaLogDump>, statuses: &[StatusSample]) -> Report;
pub fn joint_left_within(statuses: &[StatusSample], limit: Duration) -> Report;
pub fn voters_odd(statuses: &[StatusSample]) -> Report;          // non-joint samples only

// oracle::p13_upgrade
pub fn no_gated_before_finalize(meta_log: &[MetaLogEntry], partition_logs: &[LogScan]) -> Report;
pub fn no_gated_before_finalize_from(initial_finalized: u32, meta_log, partition_logs) -> Report;
pub fn no_gated_partition_commands(scans: &[LogScan]) -> Report;
pub struct OutcomeRecord { pub node: u64, pub partition: Option<u16>, pub offset: u64, pub command: Bytes, pub outcome: ApplyOutcome }
pub fn no_reject_continue(outcome_streams: &[OutcomeRecord]) -> Report;
pub fn finalization_index(initial: u32, meta_log: &[MetaLogEntry], level: u32) -> Option<u64>;
pub fn finalized_level(state: &MetaState) -> u32;
pub fn record_ext(r: &MetaRecord) -> &[u8];

// oracle::p13_gc
pub const MODULE_GONE: &str = "module_gone";
pub struct RetireEvent { pub module_sha: String, pub non_terminal_runs: Vec<String> }
pub struct RunResult { pub run_id: String, pub status: String, pub error: Option<String> }
impl RunResult { pub fn from_json(&Value) -> Self; pub fn is_module_gone(&self) -> bool; }
pub fn no_module_gone_for_live(retire_events: &[RetireEvent], run_results: &[RunResult]) -> Report;
```

What each checks: RYW fails a read issued after an acked write of the same session that misses it (versions/policies compare with the session's latest acked value). Membership: a tombstoned id (meta `Put(Member)` with ext 97 = REMOVED applied Ok, or already in the snapshot) never appears later in an applied ISR/leader write, a non-removed Member write, the persisted ConfState voters/learners or the final ISRs; once a status sample shows `removed`, no later sample lists it. Upgrade: `AlterReplicas` and records carrying ext 100/101/102 never at an index at or below the `Put(Cluster)` that raised finalized (ext 98) to the feature's `Feature::introduced_at()`; RepinRun on disk only after finalization; never `Rejected(Invalid(Empty))` for non-empty bytes. GC: no run listed non-terminal at a retirement fails with `module_gone`.

## Task commits

1. `312a55a` feat(13-08): P13Harness lifecycle ops in tiers B and C
2. `6a876c4` feat(13-08): Phase 13 oracles + self-tests

## Verification

- `cargo test -p ostro-repl-conformance --test p13_harness_smoke -- --test-threads=1`: 4 passed, 1 ignored (tier B single node serves a run; upgrade to Some(1) and back to None restarts with the override measured through `ServerHandle::p13().level_max` and the run survives; add_node and status reach the server: `POST /v1/cluster/tokens: HTTP 501` / `GET /v1/cluster/status: HTTP 501` from 13-03's adapters until 13-09).
- Same with `--ignored` and `OSTRO_FAILPOINT_BIN` = a failpoint build of this tree (`cargo build -p ostro --features failpoints` in the slot target dir; target/failpoints is not built on this machine): 1 passed (tier C single node at `OSTRO_TEST_LEVEL_MAX=1`, read from `/proc/<pid>/environ` on the original and the respawned process, unset after `upgrade(1, None)`; run kept; add_node/status reach the server).
- Phase 12 unchanged: `--test harness_smoke` 2 passed, `--test process_smoke` 2 passed + the ignored three-process test 1 passed (exercises the restart path that now heals every proxy).
- `cargo test --test p13_oracle_selftest`: 11 passed (`ryw_checker_catches_stale_read`, `removed_id_returning_detected`, `stuck_joint_state_detected`, `even_voter_set_detected`, `gated_command_before_finalize_detected`, `reject_and_continue_detected`, `module_gone_for_live_run_detected` + clean-input tests).
- clippy `-D warnings` with and without `--features failpoints`, all targets: clean; fmt clean.
- ci-local --no-musl --no-polyglot at 6a876c4 on the remote runner (`ostro-k8s-ci`, slot 1, 6336 s): exit 0, every step PASS (fmt, clippy, build, cargo test, failpoint suites, replication mutant matrix 11 bites, campaign smoke, deny, vendored, P12 grep gates incl. the clock gate). A local run started before the runner was available was stopped by PID.

## Deviations from Plan

1. **[Rule 3 - Blocking] Tier-C proxy links for joined nodes in extra ChaosProxy instances.** `chaos_proxy.rs` (12-09) starts its links once and is outside this plan's files; each join starts one more proxy with the joined node's links, and every fault primitive iterates all proxies. `ProcCluster::proxy_for` exposes the per-link proxy for finer rules (delay/blackhole).
2. **[Rule 3 - Blocking] `start_single_with(spec, level)` added in both tiers.** The plan's smoke ("upgrade(node, Some(1)) and the node serves again") cannot pass from a node booted at LEVEL: the single node auto-finalizes to 2 and the boot fence then (correctly) refuses level 1. The smoke starts at level 1, upgrades to Some(1), then to None.
3. **Worktree / target dir:** the plan text names `/tmp/ostro-wt-13-08` and `/tmp/ostro-target-13-08`; per the executor brief the work ran in the scratchpad worktree with `target/slot-1`, and the tier-C smoke used `OSTRO_FAILPOINT_BIN` instead of building `target/failpoints`.
4. **Plan verify command:** it builds `target/failpoints`; not run that way (disk budget, slot rule). Equivalent run as above.

## Open issues / handover

- **13-09 (address book):** a joined tier-C node's traffic goes through its proxy links only if the dialer uses them. Phase 12 static peers get per-link addresses from `[[cluster.peers]]`; joined nodes learn addresses from meta (`Member.cluster_addr`, the advertised `127.r.r.k:17701`) and the seeds file, which bypass the proxy. Until the server lets the harness override a peer's address per dialer (e.g. `[[cluster.peers]]` entries consulted before meta addresses, without counting as static voters), `isolate`/`partition` of a joined node cut nothing it does not dial through a proxy; `kill` (kill -9) works. 13-09 or 13-18 should decide; the links are already registered (`proxy_for(n, m)`).
- **13-09 (joiner):** tier B passes the seed as `http://127.0.0.1:<port>` (tier-B APIs are plaintext) and `config: None`; tier C passes `http://…` and `-c join<k>.toml` without `node_id` (the marker supplies it; the harness rewrites the config with the allocated id after the join). If `run_join` requires `https://`, it needs a plaintext path for tests or the harness needs API TLS.
- **13-09 (smoke flip):** `tier_b_add_node_and_status_reach_the_server` and the tier-C test accept success already; 13-09's own test asserts the join succeeds.
- **13-13 / 13-18:** RYW observations of signals need the run history's request ids; `Observation::run_from_json` takes them as a separate list.
- **13-17 / 13-18:** `RunResult::from_json` treats the `error` field (string or object, rendered as text) as the failure detail; `module_gone` is matched as a substring.
- **13-18 (outcome streams):** `no_reject_continue` needs apply outcomes; nothing in the harness records them yet (a failpoint/observer hook in the partition apply loop, or the state store's per-offset outcome if one exists).

## Self-Check: PASSED
