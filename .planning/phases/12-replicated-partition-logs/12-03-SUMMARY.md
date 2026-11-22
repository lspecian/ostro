---
phase: 12-replicated-partition-logs
plan: 03
subsystem: conformance-harness
tags: [conformance, harness, tier-b, transport, fault-injection, oracles, runner, pki]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 contracts: ClusterHarness/ClusterSpec/ApiClient/DataDirView/MetaLogEntry, ClusterTransport/ClusterIncoming/MemConn/PeerNode, ServerOptions::cluster_transport, Scenario registry, pending lists"
provides:
  - "harness::certs::TestPki (rcgen P-256 CA, node certs with one ostro://<cluster>/node/<id> URI SAN, worker-shaped certs, CA PEM files importable by ClusterCa)"
  - "harness::fault: MemNet / MemTransport (ClusterTransport over tokio::io::duplex), per-connection fault proxy, seeded FaultLayer (Drop/Delay/Blackhole/Cut per directed link and RpcKind), serve_memory + PeerIo (PeerNode in request extensions)"
  - "harness::inproc::InProcCluster implementing ClusterHarness (tier B), ApiClient methods, Ack, read_meta_log / MetaLogDump"
  - "oracle::{disk, replica, standby, la_rule, election, objstore, status, cluster_formed} + oracle::Report"
  - "scenarios::runner: run_scenario(_with), run_group_test / run_group_with, red_run_all / red_run_with, red_problem, format_rows"
affects: [12-06, 12-07, 12-09, 12-14, 12-15, 12-16, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "tonic Server::serve_with_incoming(any tower service, ..) as a per-connection HTTP/2 fault proxy: requests are classified by :path without HPACK parsing"
    - "oracles read files offline (verified WAL scan, meta Raft log decode, state stores opened on copies), never the node's API"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/fault_layer.rs
    - crates/ostro-repl-conformance/tests/harness_smoke.rs
    - crates/ostro-repl-conformance/tests/oracle_selftest.rs
    - crates/ostro-repl-conformance/tests/runner_selftest.rs
  modified:
    - crates/ostro-repl-conformance/src/harness/{certs,fault,inproc}.rs
    - crates/ostro-repl-conformance/src/oracle/{mod,disk,replica,standby,la_rule,election,objstore,status,cluster_formed}.rs
    - crates/ostro-repl-conformance/src/scenarios/runner.rs
key-decisions:
  - "The duplex connector works on tonic 0.14.9 (Endpoint::connect_with_connector_lazy + hyper-util TokioIo); the loopback-TCP fallback was not needed"
  - "Per-RPC faults need request visibility but ClusterTransport::channel must return a concrete tonic Channel, so every connection runs through an in-process fault proxy: a tonic Server serving one generic tower service on the client pipe, forwarding over a second pipe whose server half reaches the target as MemConn{peer: PeerNode(from)}"
  - "The meta Raft log is decoded by a small protobuf/CRC32 reader in inproc.rs (read_meta_log) instead of opening the vendored RaftLogStore: ostro-raft-driver is not a dependency of ostro-repl-conformance and the plan forbids new dependencies; reading the files also guarantees the store is never modified"
  - "standby is strict: unequal applied offsets are a violation; scenarios quiesce replicas before calling it"
  - "election (c) defines 'after cluster bootstrap' as after the first bootstrap placement Put(Partition 0->1) or ElectLeader in the log, so RF=1 logs that keep using SetPartitionLeader are not flagged"
requirements-completed: []
duration: ~2 h 15 min of implementation (19:00-21:15) plus the ci-local gate
completed: 2026-11-22
---

# Phase 12 Plan 03: Tier-B harness Summary

**Tier B exists. In-process `ostro-server` nodes talk over an in-memory tonic transport with real HTTP/2 and prost on `tokio::io::duplex` pipes, and a seeded fault layer that can drop, delay, blackhole or cut each directed link per RPC kind. Nine measured oracles read bytes on disk, the meta Raft log and the bucket. Each one passes on a correct real-format fixture and fails, naming the culprit, on a damaged one. The runner classifies oracle failures, deadlines and panics correctly.**

## Performance

- Tasks: 3 of 3
- Commits: 183347b (Task 1), d418246 (Task 2), d4869f2 (Task 3), plus the docs commit
- Test runtimes (warm, slot build, load ~4): `fault_layer` 0.31 s (8 tests); `harness_smoke` 0.6 s (2 tests; 23 s on the first run, which includes the `guests/` release build); `oracle_selftest` 3.6-5.2 s (8 tests); `runner_selftest` 3.0 s (3 tests). Three consecutive runs of all six conformance test binaries were green, so no ports or locks leak.

## Harness API as landed

### `harness::certs`

```rust
pub struct IssuedCert { pub cert_pem: String, pub key_pem: String, pub cert_der: Vec<u8> }
impl TestPki {
    pub fn new(cluster_id: &str) -> Result<Self, HarnessError>;          // CN "ostro cluster <id>" (ClusterCa::import_pem accepts it)
    pub fn cluster_id(&self) -> &str; pub fn ca_cert_pem(&self) -> &str; pub fn ca_cert_der(&self) -> &[u8]; pub fn ca_key_pem(&self) -> &str;
    pub fn write_ca(&self, dir: &Path) -> Result<(PathBuf, PathBuf), HarnessError>;   // ca.crt, ca.key
    pub fn node_cert(&self, id: u64, extra_ips: &[IpAddr]) -> Result<IssuedCert, HarnessError>;   // 1 URI SAN + localhost + 127.0.0.1 + extra IPs
    pub fn worker_cert(&self, token_id: &str, worker_id: &str) -> Result<IssuedCert, HarnessError>;
}
```

### `harness::fault`

```rust
pub enum RpcKind { Fetch, FetchSnapshot, FetchBlob, PushBlob, Raft, Propose, ReportSuspect, Other, Any }  // RpcKind::from_path(":path")
pub enum Fault { Drop(f64), Delay(u64 /*ms*/), Blackhole, Cut }
pub enum Action { Pass, Fail(String), Delay(Duration), Blackhole }
impl FaultLayer {   // shared by every transport of one MemNet; seeded StdRng
    pub fn new(seed: u64) -> Self;
    pub fn set(&self, from: u64, to: u64, kind: RpcKind, fault: Fault); pub fn clear_link(&self, from: u64, to: u64);
    pub fn isolate(&self, n: u64); pub fn partition(&self, a: u64, b: u64); pub fn heal(&self, n: u64); pub fn heal_all(&self);
    pub fn link_cut(&self, from: u64, to: u64) -> bool; pub fn decide(&self, from: u64, to: u64, kind: RpcKind) -> Action;
}
impl MemNet {
    pub fn new(seed: u64) -> Arc<Self>; pub fn faults(&self) -> &Arc<FaultLayer>;
    pub fn transport(self: &Arc<Self>, node: u64) -> Arc<MemTransport>;   // fresh listener (restart replaces it)
    pub fn knows(&self, node: u64) -> bool;
    pub fn channel_as(self: &Arc<Self>, from: u64, to: u64) -> tonic::transport::Channel;  // lazy; `to` sees PeerNode(from)
}
impl ClusterTransport for MemTransport { /* channel(node) = channel_as(self, node); incoming() = Memory(rx) once */ }
impl MemTransport { pub fn node(&self) -> u64; pub fn take_incoming(&self) -> Option<mpsc::Receiver<MemConn>>; }
pub struct CutIo;  // pipe end that fails with ConnectionReset once its link is cut (woken by the rule-change signal)
pub struct PeerIo; // impl From<MemConn>; tonic Connected with ConnectInfo = PeerNode
pub async fn serve_memory(incoming: mpsc::Receiver<MemConn>, routes: tonic::service::Routes) -> Result<(), tonic::transport::Error>;
```

Topology: client `Channel` → pipe A → **fault proxy** (a `tonic::transport::Server` serving one generic tower service, reading `:path`, asking `FaultLayer::decide`) → inner channel → pipe B → node `to`'s listener as `MemConn { io, peer: PeerNode(from) }`. The proxy is spawned per connection. 12-14's in-memory listener should do what `serve_memory` does: wrap each `MemConn` so the tonic `ConnectInfo` is the `PeerNode`, which then sits in the request extensions where `authz::require_node` looks.

### `harness::inproc`

```rust
pub const START_T: Duration = 120 s; pub const STOP_T: Duration = 60 s; pub const TEST_WORKFLOW: &str = "smoke";
impl ClusterHarness for InProcCluster { /* start, node_ids, client, assignment, isolate, heal, partition, kill, restart,
                                           node_disk, bucket, meta_log, admin_status, stop */ }
impl InProcCluster {
    pub fn spec(&self) -> &ClusterSpec; pub fn cluster_id(&self) -> &str;    // "conf-<seed>"
    pub fn net(&self) -> &Arc<MemNet>; pub fn pki(&self) -> &TestPki;
    pub fn handle(&self, node: u64) -> Option<&ServerHandle>; pub fn config(&self, node: u64) -> &ServerConfig;
    pub fn channel_as(&self, from: u64, to: u64) -> Channel; pub fn root(&self) -> &Path;
    pub async fn stop_keeping_dirs(self) -> TempDir;
}
pub struct Ack { pub run_id: String, pub partition: u16 }   // Ack::id() -> RunId
impl ApiClient {
    pub async fn get_json(&self, path: &str) -> Result<(StatusCode, Value), HarnessError>;
    pub async fn ready(&self) -> bool;
    pub async fn deploy_test_module(&self) -> Result<[u8; 32], HarnessError>;          // smoke guest
    pub async fn start_run(&self, workflow: &str, input: Value, idempotency_key: Option<&str>) -> Result<Ack, HarnessError>;
    pub async fn wait_run(&self, run_id: &str, limit: Duration) -> Result<String, HarnessError>;  // terminal status
    pub async fn partition_count(&self) -> Result<u16, HarnessError>;                   // GET /v1/partitions
    pub async fn append_workload(&self, p: u16, k: usize) -> Result<Vec<Ack>, HarnessError>;   // keyed routing to p
}
pub fn guest_wasm(name: &str) -> Result<Vec<u8>, HarnessError>;
pub struct MetaLogDump { pub voters, pub learners, pub commit, pub snapshot_index, pub entries: Vec<MetaLogEntry>,
                         pub undecodable: Vec<u64>, pub snapshot_state: MetaState, pub state: MetaState }
pub fn read_meta_log(meta_dir: &Path) -> Result<MetaLogDump, HarnessError>;
```

Node config: `server_defaults()`, API and cluster listener on `127.0.0.1:0`, `<root>/node<id>` on `/dev/shm` when available, `[node] node_id = id`, `partitions = spec.partitions`, `[cluster] cluster_id = "conf-<seed>"`, `[[cluster.peers]]` = the other nodes at `mem://<id>`, `[cluster.tls] ca_cert_file/ca_key_file` = the TestPki CA (multi-node specs), `[replication] session_timeout 600ms, replica_lag_max 2s, meta_election_tick 6, fetch_max_wait 100ms, rf/min_insync` from the spec. With `spec.objstore`, `[backup] url = file://<root>/bucket` is shared by every node. Nodes start concurrently.

- `kill(n)` = isolate, then graceful shutdown (data dir kept). `restart(n)` = fresh `MemTransport`, start from the same config, then `heal(n)`.
- `meta_log(n)` panics if the log is unreadable, so an oracle never runs on a silently empty log. The panic becomes a HarnessError.
- `admin_status(n)` returns `Value::Null` when the route does not exist (404 before 12-14) or the body is not JSON.

## Fault-kind table

| Fault | `RpcKind::Any` | specific kind |
|-------|----------------|---------------|
| `Drop(p)` | each RPC fails with probability p, `UNAVAILABLE` "dropped by fault layer" | same, only that kind |
| `Delay(ms)` | request held `ms` before forwarding | same, only that kind |
| `Blackhole` | accepted, never answered (client deadline fires) | same, only that kind |
| `Cut` | new connections refused (`ConnectionRefused`); live pipes reset (`ConnectionReset`), so streams die | that kind's RPCs fail with `UNAVAILABLE` "connection reset" |

The most recently added matching rule decides. `isolate(n)` adds wildcard cut rules both ways. `heal(n)` removes every rule naming n, and `heal_all()` removes all rules. The same seed and the same RPC sequence give the same drop pattern (tested).

## Oracle list

| Oracle | Inputs | Fails when |
|--------|--------|------------|
| `disk::acked_on_disk(views, acks, min_insync)` | data dirs, `Ack`s | an acked run's `StartRun` is in fewer than `min_insync` nodes' verified WAL (names the run id, partition and holders) |
| `disk::scan_partition(view, p)` | data dir | (the measured read itself) offline read-only scan with `verify_record` checks; stops at the first bad record, and `stopped` says why |
| `replica::identical_below_hw(views, p, hw)` / `compare_below_hw(scans, hw)` | data dirs or scans | envelopes below HW differ (partition, offset, nodes, first differing byte), a record is held only by some nodes, or a node's verified log ends below HW |
| `standby::equal_state(views, p, count)` / `measure` / `compare` | data dirs | applied offsets differ, or at equal applied: `state_hash`, logical digest or a per-table sha256 differs (names the tables). Stores are opened on copies. |
| `la_rule::check(view, p, meta_log)` / `check_scan` / `meta_leaders` | data dir, meta log | an epoch does not open with `LeaderAnnounced{e}`, the LA names another node than meta's leader for e, or meta never committed a leader for e |
| `election::isr_only(meta_log)` / `isr_only_from(snapshot_state, meta_log)` | meta log | a proposed (applied or rejected) `ElectLeader` elects outside, or sets an ISR outside, the then-published ISR, or sets an empty ISR with a leader; any non-bootstrap `Put(Partition)`; `SetPartitionLeader` after bootstrap |
| `objstore::chain_equals_committed(bucket, p, committed, hw)` | bucket, scanned records | the chain (from `p/<pppp>/head`, else `manifest/current`) does not verify, a chunk ends beyond HW, or an envelope is missing from or differs from the committed log |
| `objstore::nothing_above(bucket, p, hw)` | bucket | any `p/<pppp>/seg/` chunk object ends above HW |
| `cluster_formed::check(cluster, spec)` / `check_within` / `evaluate` / `measure` | running cluster | any node's meta voters differ from `1..=nodes`, the partition count, rf, epoch or leader is wrong, or the leader's WAL lacks the opening LA. Polled up to `FORMATION_DEADLINE` (30 s). |
| `status::honest(admin, p, meta_isr, leos, hw)` | `/admin` JSON, meta ISR, measured LEOs | the reported ISR differs from the published ISR, or a member's measured durable LEO is below HW |

On the unintegrated build, a 3-node spec fails `cluster_formed` with `node 1: meta voters [1] != configured [1, 2, 3]` (and `1 replica(s) [1], expected rf 3`). This is the uniform expected red reason for 12-06/12-07.

## Runner

`run_scenario_with(s, formation_deadline)`: start → `cluster_formed` (failure = `Outcome::Failed{oracle: "cluster_formed"}`) → body under `tokio::time::timeout(s.deadline)` with `catch_unwind` → always `stop()`. Outcomes are `Passed`, `Failed(ScenarioFailure)` and `Harness(HarnessError)` (start failure, Deadline, Panic, tier C). `red_problem` holds the rules: a harness error is always a problem; pending + pass gives "remove it from pending/<list>.rs"; non-pending + fail is a problem. `run_group_test(group)` prints `PENDING <name>` and does not run pending scenarios unless `OSTRO_REPL_RED_RUN` is set. `red_run_all` prints `scenario | pending | result | oracle: detail`.

## Duplex connector vs TCP fallback

The **duplex connector** is used, and it works on tonic 0.14.9: `Endpoint::connect_with_connector_lazy` with a tower `Service<Uri>` returning `hyper_util::rt::TokioIo<CutIo>`. The TCP fallback was not needed.

## Deviations from Plan

**1. [Rule 3 - Blocking] The meta log is not opened through the vendored log store.**
- **Issue:** The plan says "vendored log store opened read-only on a copy". `RaftLogStore` lives in `ostro-raft-driver`, which is not a dependency of `ostro-repl-conformance`, and the phase rules forbid new dependencies.
- **Fix:** `read_meta_log` decodes the store's on-disk format (`state` CRS1, `snapshot` CRN1, `log/<index>.seg` records `[len][crc32][Entry]`) with a small protobuf varint reader and CRC-32. It keeps only committed entries (`index <= HardState.commit`) after the snapshot, and replays a fresh `MetaState` for the verdicts. The files are only read, so no copy is needed. The `state` file is replaced by rename, and a torn segment tail stops that segment.
- **Files:** `harness/inproc.rs`. **Commit:** d418246.

**2. [Interpretation] Two small API additions beyond the plan text.** I added `InProcCluster::stop_keeping_dirs` (the lock-leak check needs the dirs after the stop) and `run_scenario_with` / `red_run_with` / `run_group_with` taking an explicit pending predicate and formation deadline, so the runner self-test can use synthetic scenarios and a short deadline.

**3. [Interpretation] `append_workload(p, k)` learns P from `GET /v1/partitions`.** `ApiClient` has no partition count field, and `harness/mod.rs` is not this plan's file.

No server, ostro-log or other crate file was touched. No dependency was added. Frozen formats are untouched.

## Open items for later plans

- **12-14:** the server still drops `ServerOptions::cluster_transport`. Until 12-14, `channel_as(from, to)` reaches nobody: calls hang to their deadline, because the node never takes its listener. The in-memory listener should map `MemConn` to `ConnectInfo = PeerNode` (see `serve_memory` / `PeerIo`). `status::reported_isr` expects `{"partitions": [{"partition", "isr", ..}]}`; adjust it if `/v1/admin/replication` lands differently.
- **12-15:** `objstore::decode_head` accepts JSON `{epoch, last_chunk_key, last_chunk_sha256 (hex or byte array)}`. If the head object lands in another encoding, update `decode_head`, the only place that knows it.
- **12-06/12-07:** `standby::equal_state` is strict about applied offsets (wait for quiescence first). `la_rule` skips the first epoch of a log starting above 0 when it does not begin with an LA. Use `election::isr_only_from(dump.snapshot_state, &dump.entries)` when the meta log was compacted.
- `cluster_formed` checks the opening LA on the leader's disk, not that the state machine applied it (a WAL record below the verified end).

## Plan gate

I ran `ci-local --no-musl` in a detached worktree at **d4869f2** (the Task 3 commit, on main), on slot-3 with load 4-14 from parallel agents:

```
fmt / clippy -D warnings / build warning-free     PASS
sdk workspace, guests, order-saga                 PASS
cargo test                                        FAIL 4226s  (3785 passed, 1 failed)
polyglot conformance / ostro-build polyglot       PASS
semantics sleepers / failpoint list sync          PASS
failpoint suites                                  PASS 5600s
server campaign smoke (20 kills)                  PASS
phase 7 legs / deny / vendored / no-chronik / workflow validation  PASS
clients python/ts/go                              SKIP (the script skipped them)
```

The only red was `ostro-server --test sdk_e2e lease_zombie`, a lease/self-fence timing test in a crate this plan does not touch. It passed 3 of 3 times on rerun at d4869f2 (about 10 s each). Every ostro-repl-conformance binary was green inside the gate: fault_layer, harness_smoke, oracle_selftest, runner_selftest, skeleton and red_run. Before each CAS onto a moved main, `cargo check --workspace --all-targets` was clean.

**Gate verdict: green at d4869f2, one unrelated timing flake that is green on rerun.**

## Self-Check: PASSED

- Files: harness/{certs,fault,inproc}.rs, oracle/*.rs, scenarios/runner.rs, tests/{fault_layer,harness_smoke,oracle_selftest,runner_selftest}.rs are present at main.
- Commits: 183347b, d418246 and d4869f2 are on main.
