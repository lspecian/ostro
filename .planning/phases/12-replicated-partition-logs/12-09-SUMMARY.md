---
phase: 12-replicated-partition-logs
plan: 09
subsystem: conformance-harness
tags: [conformance, harness, tier-c, chaos-proxy, multi-process, kill-9, loopback]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 ClusterHarness API; 12-03 TestPki, ApiClient, read_meta_log, oracles (disk::acked_on_disk, cluster_formed::measure/evaluate)"
provides:
  - "harness::chaos_proxy::ChaosProxy: one listener per directed link, byte-transparent copy, cut / delay / blackhole / heal, isolate / heal_node / partition / heal_all, RST passthrough, per-link counters"
  - "harness::process::ProcCluster implementing ClusterHarness over real `ostro server` processes (failpoint build) on 127.r.r.i with real SIGKILL and restart from retained data dirs"
  - "process::{failpoint_bin, ensure_failpoint_bin, loopback_aliases_supported, links_for, node_ip, parse_ready, ProcOptions, KillOutcome, ProcTimings, SKIP_NO_BIN}"
affects: [12-14, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "Directed-link proxy listener on the target's IP (127.r.r.<to>:17800+from), so a TLS client dialing it sees the address the target's certificate names"
    - "RST passthrough: any read/write error on one side resets the other with SO_LINGER 0; clean EOF stays a FIN; refused upstream connect resets the accepted client"
    - "Blackhole = stop reading (window fills, no FIN/RST, nothing lost on heal), not drop"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/chaos_proxy.rs
    - crates/ostro-repl-conformance/tests/process_smoke.rs
  modified:
    - crates/ostro-repl-conformance/src/harness/chaos_proxy.rs
    - crates/ostro-repl-conformance/src/harness/process.rs
key-decisions:
  - "Proxy listener of from -> to lives on the target's loopback IP with port 17800+from (not on the dialer's IP): the TLS name check of node from's client sees an IP the target's own certificate is issued for"
  - "Blackhole is modelled as 'the proxy stops reading' (TCP window fills, bytes held, delivered on heal), which is what a healed network path does; cut is the RST case"
  - "process_smoke's RF=1 test is not ignored but gated on the binary (W14): only OSTRO_FAILPOINT_BIN or target/failpoints/debug/ostro count (never the release campaign binary, which may be stale); cargo is invoked only with OSTRO_PROC_BUILD=1"
  - "assignment(p) in tier C is read offline from the meta log with the highest commit across nodes (no in-process handle exists)"
requirements-completed: []
duration: ~25 min of implementation plus the ci-local gate
completed: 2026-11-23
---

# Phase 12 Plan 09: Tier-C harness Summary

**Tier C exists: a Rust TCP chaos proxy (per-directed-link cut / delay / blackhole / heal with RST passthrough, mTLS passes through unchanged) and `ProcCluster`, which runs N real `ostro server` processes of the failpoint build on `127.r.r.i`, kills them with SIGKILL, restarts them from their data dirs and implements the same `ClusterHarness` API as tier B. An RF=1 process survives `kill -9` with every ack confirmed on disk by the 12-03 oracle.**

## Performance

- Tasks: 2 of 2
- Commits: 787dcf0 (Task 1), 16444ed (Task 2), plus the docs commit
- `chaos_proxy`: 8 tests, 0.71 s, 5 of 5 consecutive runs green (also at `--test-threads=8`)
- `process_smoke`: 2 run + 1 ignored; with the binary 40 s for all three (most of it the first `guests/` build); the ignored 3-process test 0.4 s warm

## IP / port allocation

| what | address |
|------|---------|
| cluster `r` | random in `2..=250` per `ProcCluster` (seeded from time ^ pid); retried up to 20 times on a bind conflict |
| node `i` | `127.r.r.i` (Linux routes all of 127/8 on `lo`, no root) |
| API | `127.r.r.i:0`, bound port from the ready line |
| cluster listener | `127.r.r.i:17701` (`CLUSTER_PORT`) |
| proxy listener of `from -> to` | `127.r.r.<to>:<17800 + from>` (`PROXY_PORT_BASE`), upstream `127.r.r.<to>:17701` |
| node `from`'s `[[cluster.peers]]` entry for `to` | `addr = "127.r.r.<to>:<17800 + from>"` |

Before spawning, every `127.r.r.i:17701` is probed and the proxy binds all its listeners; any `EADDRINUSE` picks a new `r`.

## TOML template

`ServerConfig::server_defaults()` with the tier-B node fields, rendered by `ServerConfig::to_toml_string()` into `<root>/node<i>.toml` and run as `ostro server -c <file>`:

```toml
[node]
data_dir = "<root>/node<i>"
partitions = <spec.partitions>
node_id = <i>
[api]
listen = "127.r.r.i:0"
[cluster]
listen = "127.r.r.i:17701"
cluster_id = "conf-<seed>"
[cluster.tls]                       # multi-node specs only
ca_cert_file = "<root>/pki/ca.crt"
ca_key_file = "<root>/pki/ca.key"
[[cluster.peers]]                   # one per other node
id = <p>
addr = "127.r.r.<p>:<17800 + i>"
[replication]
session_timeout = "600ms"; replica_lag_max = "2s"; meta_election_tick = 6; fetch_max_wait = "100ms"
rf = <spec.rf>; min_insync = <spec.min_insync>
[backup]                            # spec.objstore only
url = "file://<root>/bucket"
```

The root is a tempdir on `/dev/shm` (else the OS temp dir). `pki/node<i>.{crt,key}` (URI SAN + node IP) are written too for later plans; the server itself issues its node certificate from the CA key. Inherited `OSTRO_*` and `RUST_LOG` are stripped; `FAILPOINTS` comes from `ProcOptions { failpoints, node_failpoints, env }` or `set_failpoints(n, ..)` (next start). stdout + stderr of every incarnation are appended to `<root>/node<i>.log`, with a `=== harness: spawn node <i> ===` marker per spawn.

## Process timings (debug failpoint build, `/dev/shm`, load ~10 from parallel agents)

| operation | measured |
|-----------|----------|
| fresh single node: spawn -> ready line | 251 ms |
| `kill -9` -> reaped (`waitpid` says signal 9) | 12 ms |
| restart from the data dir -> ready line | 114-123 ms |
| 3-node start (concurrent spawn) -> all ready | under 1 s (whole ignored test 0.4 s) |

## Harness API as landed

```rust
// chaos_proxy
pub struct Link { pub from: u64, pub to: u64, pub listen: SocketAddr, pub upstream: SocketAddr }
pub struct LinkRule { pub cut: bool, pub blackhole: bool, pub delay: Duration }
pub struct LinkStats { pub to_upstream: u64, pub to_client: u64, pub accepted: u64, pub open: usize, pub resets: u64 }
impl ChaosProxy {
    pub async fn start(links: Vec<Link>) -> io::Result<Self>;
    pub fn links(&self) -> Vec<Link>; pub fn addr(&self, from, to) -> SocketAddr;
    pub fn rule(&self, from, to) -> LinkRule; pub fn stats(&self, from, to) -> LinkStats;
    pub fn cut/blackhole/heal(&self, from, to); pub fn delay(&self, from, to, ms: u64);
    pub fn isolate(&self, node); pub fn heal_node(&self, node); pub fn partition(&self, a, b); pub fn heal_all(&self);
}
// process
impl ClusterHarness for ProcCluster { /* start, node_ids, client, assignment, isolate, heal, partition,
                                         kill (= kill9), restart, node_disk, bucket, meta_log, admin_status, stop */ }
impl ProcCluster {
    pub async fn start_with(spec: ClusterSpec, opts: ProcOptions) -> Result<Self, HarnessError>;
    pub async fn kill9(&mut self, n) -> Result<KillOutcome, HarnessError>;   // Killed | AlreadyExited | NotRunning
    pub fn set_failpoints(&mut self, n, spec: Option<&str>);
    pub fn proxy(&self) -> &ChaosProxy; pub fn r(&self) -> u8; pub fn ip(&self, n) -> Ipv4Addr;
    pub fn config/config_path/log_path(&self, n); pub fn log_tail(&self, n, lines) -> String;
    pub fn pid(&self, n) -> Option<u32>; pub fn is_running(&mut self, n) -> bool;
    pub fn root(&self) -> &Path; pub fn timings(&self) -> ProcTimings; pub fn stop_keeping_dirs(self) -> TempDir;
}
```

- `kill(n)` = `SIGKILL` to our own child + `waitpid`; the trait's `kill` logs (does not panic on) an unexpected wait status. Only PIDs the harness spawned are ever signalled.
- `restart(n)` kills first if still running, respawns from the same config and data dir, waits for the ready line, then heals n's proxy links (like tier B).
- `meta_log(n)` copies `<data>/meta` into the root and decodes the copy (the process may be writing); `assignment(p)` reads the meta log offline from the node with the highest commit.
- `stop()` kills every child and removes the root. A drop while panicking keeps the root and prints `tier-C cluster kept for post-mortem (logs: node<i>.log): <path>`.

## macOS behaviour

`loopback_aliases_supported()` tries to bind `127.3.3.3:0`. macOS only routes `127.0.0.1` unless `ifconfig lo0 alias` is set, so it returns false there, `ProcCluster::start` fails with a reason naming the alias, and the `process_smoke` tests print `SKIP: loopback aliases 127.r.r.x cannot be bound on this host` and return. Without the failpoint binary (the normal CI case on both legs) they print `SKIP: failpoint ostro binary not built (...)` first. The chaos-proxy tests bind `127.0.0.1` only and run everywhere.

## Formation before 12-14

Three processes with proxied peers and the TLS CA configured start and answer health, but each is its own single-voter meta group. Measured with `cluster_formed::measure/evaluate` the ignored test prints the same uniform reason as tier B: `node 1: meta voters [1] != configured [1, 2, 3]` (and `1 replica(s) [1], expected rf 3` per partition). The test accepts either green or that reason, so it keeps working after 12-14.

## Deviations from Plan

**1. [Rule 3 - Blocking] The failpoint binary was built in the assigned slot, not `target/failpoints`.**
- The plan's verify builds `cargo build -p ostro --features failpoints --target-dir target/failpoints`. The executor brief forbids extra target dirs, so I built `-p ostro --features failpoints` into the slot and ran `process_smoke` with `OSTRO_FAILPOINT_BIN=<slot>/debug/ostro`. The harness code still defaults to `target/failpoints/debug/ostro` as planned.

**2. [Interpretation] `Scenario` and `cluster_formed::check` stay typed on `InProcCluster`.**
- `scenarios::Scenario::run` is `fn(&mut InProcCluster)` (12-02) and `cluster_formed::check(_within)` takes `&InProcCluster` (12-03). Neither file is this plan's. The ignored test uses the generic `measure` + `evaluate` instead. 12-17 has to make the scenario type generic (or add a tier-C scenario type) to run scenarios on `ProcCluster` unchanged. The `ClusterHarness` API and every offline oracle already work on it.

**3. [Addition] Extra tests:** a dead-upstream test (refused connect -> client reset) and a ready-line parser test, plus `ProcOptions` / `KillOutcome` / `ProcTimings` helpers beyond the plan text.

No server, Cargo or CI file was touched. No dependency was added. FAILPOINT_CRATES is unchanged. No new env names (only `OSTRO_FAILPOINT_BIN` and `OSTRO_PROC_BUILD`, reserved by 12-01).

## Open items

- **12-14:** the processes do not form a cluster yet (uniform `meta voters` reason). The proxy sits between them already; whether the server dials `[[cluster.peers]] addr` and checks the TLS name against the dialled IP decides whether the "proxy on the target's IP" choice is enough. If the server instead checks the URI SAN only, nothing changes.
- **12-17:** generic `Scenario` / `cluster_formed::check` over `ClusterHarness` (deviation 2). `OSTRO_PROC_ITERS` is reserved for its iteration counts; `ProcTimings` gives the kill/restart budget.

## Plan gate

I ran `ci-local --no-musl` in a detached worktree at **16444ed** (the Task 2 commit) on slot-6, with load around 10 from parallel agents:

```
fmt / clippy -D warnings / build warning-free        PASS
sdk workspace, guests, order-saga                    PASS
cargo test                                           PASS 5411s
polyglot conformance, python/ts/go clients           PASS
ostro-build polyglot                                 FAIL 142s
semantics sleepers / failpoint list sync             PASS
failpoint suites                                     PASS 5350s
server campaign smoke (20 kills)                     PASS
phase 7 legs / deny / vendored / no-chronik / one log path / workflow validation   PASS
```

The only red was `ostro-build --features polyglot --test build_polyglot ts_template_builds`. It failed writing `target/ostro-toolchains-test/state/npm/npmrc: No such file or directory`, a state-dir race in a crate this plan does not touch. It passed 2 of 2 times on rerun at 16444ed (6 of 6 tests, `--test-threads=2` as in ci-local). Inside the gate, `chaos_proxy` (8) and `process_smoke` (2 run, 1 ignored, SKIP without the binary) were green in both the `cargo test` and the failpoint-suite passes.

Locally, with `OSTRO_FAILPOINT_BIN=<slot>/debug/ostro`: `process_smoke -- --include-ignored --test-threads=1` gave 3 of 3 green, and `cargo clippy -p ostro-repl-conformance --all-targets -- -D warnings` was clean.

**Gate verdict: green at 16444ed, apart from one unrelated flake that was green on rerun.**

## Self-Check: PASSED

- Files: harness/{chaos_proxy,process}.rs, tests/{chaos_proxy,process_smoke}.rs are present at main.
- Commits: 787dcf0 and 16444ed are on main.
