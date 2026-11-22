---
phase: 12-replicated-partition-logs
plan: 05
subsystem: meta + cluster transport
tags: [meta, raft, election, isr, mtls, authz, transport, forwarding]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 MetaCommand 4-6 shapes, ext tag 80 ElectionInfo helpers, cluster.proto MetaPeer, ostro-cluster transport contract (PeerNode, ClusterTransport, SuspectSink)"
provides:
  - "MetaState apply of ElectLeader / AlterIsr / ResignLeader (SC3: election only from the published ISR)"
  - "append-only golden crates/ostro-meta/tests/golden/meta_commands.txt (MetaCommand 0..=6)"
  - "MetaHandle::start_with(dir, MetaStart) static 1/3/5-voter group, ProposeForwarder, TickConfig, wait_ready, node_id"
  - "ostro_cluster::meta_peer::{MetaPeerService, PeerOutbox, OutboxConfig, GrpcForwarder, RaftSlot, NoopSuspectSink, DEFAULT_META_MAX_MESSAGE, ERROR_*}"
  - "ostro_cluster::transport::tcp::{TcpTransport, TcpTransportConfig, NodeTls, PeerIdentity}"
  - "ostro_cluster::authz::{require_node, node_identity_from_der, PeerRejected, IDENTITY_SCHEME}"
  - "ostro_cluster::address_book::{AddressBook, PeerAddr, PeerEntry}"
affects: [12-10, 12-11, 12-12, 12-14, 12-03]
tech-stack:
  added: []
  patterns:
    - "late-bound RaftSlot: the MetaPeer service and the outbox serve before the meta group exists"
    - "transport annotates identity (PeerNode / PeerRejected), services decide (require_node): the cluster listener is shared with workers"
key-files:
  created:
    - crates/ostro-meta/tests/elect_leader.rs
    - crates/ostro-meta/tests/meta_commands_golden.rs
    - crates/ostro-meta/tests/golden/meta_commands.txt
    - crates/ostro-meta/tests/multi_voter.rs
    - crates/ostro-cluster/tests/meta_peer.rs
    - crates/ostro-cluster/tests/support_meta/mod.rs
  modified:
    - crates/ostro-meta/src/{state,command,handle}.rs
    - crates/ostro-cluster/src/{meta_peer,authz,address_book}.rs
    - crates/ostro-cluster/src/transport/tcp.rs
    - crates/ostro-cluster/src/service.rs (one 12-02 stub unit test, see deviations)
key-decisions:
  - "A stale isr_version is StaleVersion { current: isr_version }; a stale epoch is StaleVersion { current: epoch } (MetaResult has no separate variant; callers re-read)"
  - "ElectLeader and ResignLeader always bump isr_version (every leadership change invalidates in-flight AlterIsr CAS tokens); AlterIsr bumps it without touching epoch/version"
  - "ISR shape is validated at apply for the new commands (non-empty, no node 0, no duplicates); ext tag 80 is written verbatim, never validated; a malformed stored ext makes the command Invalid rather than dropping other ext entries"
  - "start_with with more than one voter returns after local replay without waiting for an election (the caller binds RaftSlot first); wait_ready() waits for a leader"
  - "Multi-voter proposals retry NotLeader and OutcomeUnknown until a 30 s deadline (failover); the single-voter path keeps the 5-retry bound"
  - "GrpcForwarder maps every RPC failure to OutcomeUnknown (the request may have reached the leader); an address-less node to NotLeader { leader: None }"
  - "MetaPeer callers must be metadata voters; Raft additionally refuses self, batch.from != peer and message from/to mismatches; ReportSuspect needs reporter == peer (any node of the cluster)"
  - "The MetaPeer Raft stream ends with UNAVAILABLE when the local driver stops, so peers reconnect to a restarted node instead of writing into a dead connection"
requirements-completed: []
duration: ~1 h
completed: 2026-11-22
---

# Phase 12 Plan 05: Meta election rules, multi-voter MetaHandle, MetaPeer over mTLS Summary

**Meta apply now enforces SC3 on its own. `ElectLeader` accepts only a new ISR that is a subset of the published ISR, with the leader in both. `AlterIsr` is a CAS on `isr_version` at the current epoch. `ResignLeader` moves the partition to epoch+1 with leader 0. `MetaHandle` runs a static 1/3/5-voter group over an outbox the caller supplies, and forwards a follower's proposals with read-your-writes. `ostro-cluster` carries meta Raft traffic and forwarded proposals over the production mTLS transport, and authorizes callers by node identity only.**

## Performance

- Tasks: 3 of 3
- Commits: 46928ae (Task 1), 3d4a89c (Task 2), 23c5b81 (Task 3), plus this docs commit
- Election times measured in the tests (100 ms tick, election 10, heartbeat 2 = the `[replication]` defaults):
  - initial election: 1.0-1.4 s
  - failover after a leader stop: 1.05-1.78 s with the in-process router (`multi_voter.rs`, 4 runs), and 1.08-1.49 s over loopback mTLS (`meta_peer.rs`, 3 runs)
  - the tests assert failover within 5 s

## Final signatures

```rust
// ostro_meta::handle
pub type ForwardFuture<'a> = Pin<Box<dyn Future<Output = Result<Applied, ProposeError>> + Send + 'a>>;
pub trait ProposeForwarder: Send + Sync + 'static {
    fn forward(&self, leader: u64, cmd: Vec<u8>) -> ForwardFuture<'_>;   // cmd = encoded MetaCommand
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickConfig { pub interval: Duration, pub election_tick: usize, pub heartbeat_tick: usize } // Default = driver 100 ms / 20 / 3
pub struct MetaStart {
    pub node_id: u64,
    pub voters: Vec<u64>,
    pub outbox: Box<dyn Outbox>,
    pub forwarder: Option<Arc<dyn ProposeForwarder>>,
    pub tick: TickConfig,
}
impl MetaHandle {
    pub async fn start(dir: &Path, node_id: u64) -> Result<MetaHandle, MetaError>;          // tests/examples only (NullOutbox)
    pub async fn start_with(dir: &Path, start: MetaStart) -> Result<MetaHandle, MetaError>;
    pub async fn wait_ready(&self) -> Result<(), MetaError>;  // leader known && applied >= commit; 30 s
    pub fn raft(&self) -> &RaftHandle;
    pub fn node_id(&self) -> u64;
}
```

`start_with` rejects node id 0, a `node_id` that is not in `voters`, and duplicate voters with `MetaError::Start`. With `voters == [node_id]` it campaigns at once and returns once leader and caught up, as `start` always did. With more voters it returns once the persisted commit index is applied locally.

```rust
// ostro_cluster::meta_peer
pub const DEFAULT_META_MAX_MESSAGE: usize = 64 MiB;
pub const ERROR_NOT_LEADER = "not_leader"; ERROR_DROPPED = "dropped"; ERROR_OUTCOME_UNKNOWN = "outcome_unknown"; ERROR_STOPPED = "stopped";
pub struct RaftSlot;  // new(), set(RaftHandle) -> bool, get() -> Option<&RaftHandle>; Clone
pub struct NoopSuspectSink;  // impl SuspectSink
pub struct MetaPeerService;  // new(node_id, voters: Vec<u64>, raft: RaftSlot, suspects: Arc<dyn SuspectSink>)
impl MetaPeerService { pub fn into_server(self, identity: PeerIdentity, max_message: usize)
    -> InterceptedService<MetaPeerServer<MetaPeerService>, PeerIdentity>; }
pub struct OutboxConfig { pub queue_cap: usize, pub batch_max: usize, pub backoff_min: Duration, pub backoff_max: Duration, pub max_message: usize }
pub struct PeerOutbox;  // new(node_id, peers: &[u64], transport: Arc<dyn ClusterTransport>, raft: RaftSlot, cfg) (inside a runtime); impl Outbox; dropped(peer) -> u64
pub struct GrpcForwarder; // new(transport: Arc<dyn ClusterTransport>), with_limits(max_message, timeout); impl ProposeForwarder
// ostro_cluster::transport::tcp
pub struct NodeTls { pub cert_chain_pem: String, pub key_pem: String, pub ca_pem: String }  // Debug redacts the key
pub struct TcpTransportConfig { pub connect_timeout: Duration /*3 s*/, pub keepalive_interval: Duration /*5 s*/, pub keepalive_timeout: Duration /*10 s*/ }
pub struct TcpTransport; // new(cluster_id, node_id, NodeTls, Arc<AddressBook>), with_config(..), server_tls() -> ServerTlsConfig,
                         // identity_interceptor() -> PeerIdentity, cluster_id(), node_id(), address_book(); impl ClusterTransport (incoming = Tcp)
pub struct PeerIdentity; // new(cluster_id), identify(&mut Request<T>); impl tonic Interceptor
// ostro_cluster::authz
pub struct PeerRejected(pub String);
pub fn node_identity_from_der(cert_der: &[u8], cluster_id: &str) -> Result<u64, String>;
pub fn require_node<T>(req: &Request<T>) -> Result<u64, Status>;
// ostro_cluster::address_book
pub struct PeerAddr { pub cluster_addr: String, pub repl_addr: Option<String> }
pub struct PeerEntry { pub id: u64, pub addr: String, pub repl_addr: Option<String> }
pub struct AddressBook; // new, from_peers, get, cluster_addr, repl_addr (falls back to cluster_addr), ids, len, is_empty, upsert, upsert_member(&Member)
```

12-14 wiring, in order:
1. Build `TcpTransport` (or the harness transport) and `RaftSlot`.
2. Serve `MetaPeerService::into_server(transport.identity_interceptor(), DEFAULT_META_MAX_MESSAGE)` on the cluster listener with `server_tls()`.
3. Create `PeerOutbox::new(..)` and `GrpcForwarder::new(..)`.
4. Call `MetaHandle::start_with`.
5. Call `slot.set(handle.raft().clone())`.
6. Call `handle.wait_ready()`.

## Validation table (meta apply)

Checks run in the order listed. Every rejection leaves the state byte-identical, which the tests assert on the snapshot bytes.

| Command | Check | Result |
|---------|-------|--------|
| all three | partition unknown | `NotFound` |
| ElectLeader | `epoch <= current epoch` | `StaleVersion { current: epoch }` |
| ElectLeader | `expected_isr_version != isr_version` | `StaleVersion { current: isr_version }` |
| ElectLeader | new ISR empty / contains 0 / duplicate | `Invalid` |
| ElectLeader | `leader != 0` and leader ∉ published ISR (`leader_not_in_published_isr`) | `Invalid` |
| ElectLeader | any new-ISR member ∉ published ISR (`elect_isr_adds_non_member`, `offline_isr_outside_published_isr`, leader 0 included) | `Invalid` |
| ElectLeader | `leader != 0` and leader ∉ new ISR | `Invalid` |
| ElectLeader | ok | version = epoch = `epoch`, leader, isr = new, `isr_version + 1`, ext 80 = `info`; replicas, min_insync and other ext entries kept |
| AlterIsr | `epoch != current epoch` | `StaleVersion { current: epoch }` |
| AlterIsr | `expected_isr_version != isr_version` | `StaleVersion { current: isr_version }` |
| AlterIsr | ISR empty / 0 / duplicate | `Invalid` |
| AlterIsr | member ∉ replicas | `Invalid` |
| AlterIsr | current leader ∉ ISR (so an offline partition, leader 0, cannot alter) | `Invalid` |
| AlterIsr | ok | `isr_version + 1`; epoch, version, leader, replicas and ext unchanged (the only way to expand the ISR) |
| ResignLeader | `epoch != current epoch` | `StaleVersion { current: epoch }` |
| ResignLeader | `node == 0` or `node != leader` | `Invalid` |
| ResignLeader | ok | epoch = version = `epoch + 1`, leader 0, node removed when \|ISR\| > 1 (kept when it is the sole member), `isr_version + 1`, ext 80 = `info` |

In both ElectLeader and ResignLeader, overflow of `isr_version` or `epoch`, or a malformed stored ext, gives `Invalid`. `SetPartitionLeader`, `Put`, `Delete` and `InitCluster` are unchanged. `set_partition_leader_still_accepts_leader_outside_isr` pins the old behaviour, under which a leader outside the ISR is accepted.

## Outbox parameters

- One bounded queue per peer, `queue_cap` 4096 messages. When it is full, the oldest non-snapshot message is dropped. If only snapshots are queued, a new non-snapshot message is dropped instead, and a new snapshot replaces the oldest one. A dropped snapshot triggers `report_snapshot_failed`.
- Up to `batch_max` 256 messages go into one `RaftBatch`, sent on one long-lived `Raft` client stream per peer. Each per-peer sender task connects only when it has something to send.
- When a stream fails, the outbox calls `report_unreachable(peer)`, plus `report_snapshot_failed(peer)` if the stream carried a `MsgSnapshot`. It then retries with exponential backoff from 50 ms to 2 s. The backoff resets once a stream had stayed up for at least 2 s.
- `send` never blocks: it is a mutex push plus a `Notify`. With zero peers the outbox spawns no task and sends nothing.
- Message limits are explicit on both sides: 64 MiB for encoding and decoding, on the client (outbox and forwarder) and on the server (`into_server`).

## Authz rejection matrix (`tests/meta_peer.rs`, real loopback mTLS)

| Caller | Propose | Raft | ReportSuspect |
|--------|---------|------|---------------|
| node cert, same cluster, voter | served (`not_leader` + hint on a follower) | served | served when reporter = caller |
| worker cert (same CA) | PERMISSION_DENIED | PERMISSION_DENIED | PERMISSION_DENIED |
| node cert naming another cluster (same CA) | PERMISSION_DENIED | PERMISSION_DENIED | PERMISSION_DENIED |
| node of this cluster, not a voter (node 9) | PERMISSION_DENIED | PERMISSION_DENIED | served (any node may report) |
| voter sending `RaftBatch.from` = another voter | n/a | PERMISSION_DENIED | PERMISSION_DENIED (reporter ≠ caller) |
| cert from another cluster's CA | TLS handshake fails (UNAVAILABLE) | TLS handshake fails | TLS handshake fails |
| no identity (no PeerNode/PeerRejected) | UNAUTHENTICATED (unit test in authz.rs) | UNAUTHENTICATED | UNAUTHENTICATED |

## Tests

| Suite | Result |
|-------|--------|
| `ostro-meta` full suite (incl. new `elect_leader` 28, `meta_commands_golden` 4, `multi_voter` 3; every existing test unmodified) | green |
| `ostro-meta --features failpoints --test meta_failpoints` | 5 passed |
| `ostro-cluster --test meta_peer` | 3 passed, over 3 runs |
| `ostro-cluster --features failpoints` (lib + meta_peer) | 10 + 3 passed |
| `cargo clippy -p ostro-meta -p ostro-cluster --all-targets (--features failpoints) -- -D warnings` | clean |
| `cargo fmt --all --check` | clean |
| `cargo check --workspace --all-targets --locked` (after rebasing onto 487fce6) | clean |
| `scripts/check-vendored.sh` | pass |
| `meta_v2.txt` and `vendor/` | unchanged since 15f9091 |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] 12-02's `require_node_is_stubbed_until_12_05` unit test in `ostro-cluster/src/service.rs`**
- **Found during:** Task 3
- **Issue:** The test asserted `Unimplemented` from the stub, so it fails once `require_node` is implemented. `service.rs` is owned by 12-14 and is not in this plan's `files_modified`.
- **Fix:** The test was renamed `require_node_without_identity_is_unauthenticated` and now asserts `Unauthenticated`. No other line changed.
- **Commit:** 23c5b81

**2. [Rule 1 - Bug] `command.rs` unit test `phase12_apply_is_stubbed_until_12_05`**
- The test was replaced by `phase12_apply_is_validated_since_12_05`, which asserts `NotFound` on an empty state. It is in `files_modified`. Commit 46928ae.

**3. [Rule 2 - Correctness] Multi-voter readiness and failover retries**
- `start_with` with more than one voter does not wait for an election. Its peers' messages only arrive once the caller has bound the `RaftSlot`, so waiting there would deadlock. `wait_ready()` was added for the election wait.
- Multi-voter `OutcomeUnknown` retries run until the 30 s deadline instead of stopping after 5 attempts. A forwarded call to a leader that just died otherwise exhausts its retries before the election finishes.
- The single-voter path is unchanged. Commits 3d4a89c and 23c5b81.

**4. [Rule 2 - Correctness] The MetaPeer `Raft` stream ends when the local driver stops**
- Without this, an in-process restart, or any handle stop while the server keeps running, would leave peers writing into a connection that still looks healthy but is dead. The restarted node would then never receive their messages. Found while writing the restart test. Commit 23c5b81.

**5. Not in files_modified, by design:** lib.rs re-exports were not added, and `ostro_meta::handle::{MetaStart, ...}` are reached through the public `handle` module.

## Open items

- I did not run the full `scripts/ci-local.sh --no-musl` (about 2 h 40 min per 12-02), because of the brief's disk/build budget with four parallel executors. I ran the subset relevant to this plan instead (table above). The phase gate (12-18) or the coordinator should include it.
- `ostro-server` still calls `MetaHandle::start` (NullOutbox). 12-14 switches it to `start_with` + `PeerOutbox` and gates `! grep -n "MetaHandle::start(" crates/ostro-server/src/server.rs` (D-P12-13).
- The conformance harness's in-memory transport (12-03) must put a `PeerNode` into the request extensions itself. `PeerIdentity` keeps an existing `PeerNode`.

## Self-Check: PASSED

- Files: elect_leader.rs, meta_commands_golden.rs, golden/meta_commands.txt, multi_voter.rs, ostro-cluster tests/meta_peer.rs and tests/support_meta/mod.rs all exist at 23c5b81.
- Commits 46928ae, 3d4a89c and 23c5b81 are on main.
