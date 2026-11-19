# Phase 13: Cluster Membership, Routing & Upgrades - Research

**Researched:** 2026-11-19
**Domain:** dynamic metadata-Raft membership (raft-rs 0.7 learners and ConfChangeV2 joint consensus), single-node promotion, partition reassignment and leader balancing on top of Phase 12's ISR log, any-node request routing with read-your-writes, mixed-version rolling upgrades (feature-level gate, write-old-until-finalized, downgrade fence), side-by-side WIT linkers (ENG-10), worker version compatibility, module GC and `migrate-runs`
**Confidence:** HIGH for the code seams and pre-freeze findings (read from code at `642a269`, the vendored raft crates and Wasmtime 50.0.2 / wit-component 0.261.0 sources). MEDIUM for the upgrade-gate design (the Kafka KIP-584/778 and CockroachDB models, adapted). MEDIUM for timings (derived from Phase 12 defaults; must be measured).

> **No CONTEXT.md exists for Phase 13** (no discuss-phase was run). The constraints below come from the orchestrator brief, `docs/04-chronik-reuse.md` §4, PROJECT.md, the Phase 5 standing decisions (D18 amended, D20 amended, D23) and the Phase 12 research. Treat them as locked.

<user_constraints>
## User Constraints (no CONTEXT.md; sources: orchestrator brief, docs/04 §4, PROJECT.md, 05-DECISIONS, 12-RESEARCH)

### Locked Decisions
- Phase 13 builds on Phase 12: one `ReplicatedLog` path (RF=1 included, D5), ISR from measured durable LEOs only, election only from the published ISR (`ElectLeader`), `AlterIsr`/`ResignLeader` meta commands (MetaCommand 4–6), OSNP v1 snapshot install for follower bootstrap, the `ostro.cluster.v1` package (`Replication`, `MetaPeer`) on the Phase 7 cluster listener, and the multi-process loopback harness with a Rust TCP chaos proxy.
- **Phase 12 open question 1 is resolved: static multi-voter meta from `[[cluster.peers]]` (the Raft transport part of CLUS-05) is pulled forward into Phase 12.** Phase 13 adds dynamic membership on top: join/remove, learners, conf changes, promotion of a single node.
- Raft stays the vendored `ostro-raft-driver` over vendored raft-rs 0.7 (`ostro-raft-rs`). No openraft, no second consensus library.
- Formats after 05-20 are additive only (D18 amended). Meta evolves only by appended `MetaRecord` variants with a META_FORMAT bump and old-format decoders, by registered `ext` tags (bump-free), or by appended `MetaCommand` variants (bump-free) (D20 amended). Existing apply validation is never tightened.
- WIT: `ostro:runtime@0.1.0` is byte-pinned after 05-20 (`wit/FROZEN.toml`). Changes ship as new `0.1.N` packages that `ostro-witcheck` proves additive (D14).
- Ordinals and reserved names come only from `.planning/RESERVATIONS.md` (rule 2: claim a block in a docs-only commit before writing code).
- Partition count is fixed per cluster; live re-sharding is out of scope (PROJECT.md Out of Scope).
- Never read or reference any Chronik clone. Ports carry attribution headers and VENDORED.md rows.
- No Docker in WSL. Multi-node tests run as in-process clusters or multi-process loopback (`127.x.y.z`) with the chaos proxy. Do not use the reachable microk8s cluster.

### Claude's Discretion
- Routing architecture, upgrade-gate design, downgrade policy, membership automation level, reassignment algorithm, test harness extensions, plan breakdown.

### Deferred Ideas (OUT OF SCOPE)
- Tenancy, scoped-token enforcement on forwarded requests beyond principal propagation, Jepsen and soak runs, the 3-node RF=3 ≥ 5,000 steps/s benchmark gate: Phase 14 (SEC-*, QA-04/05).
- Cluster-wide consistent-cut bottomless restore (12-RESEARCH open question 2): Phase 14 or later.
- Live partition splitting, multi-region, Kubernetes operator.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CLUS-05 | Multi-voter metadata Raft (3/5 nodes) with membership changes; `ostro cluster init/join/remove`; a single node is promoted to a cluster without export [FR-0.4] | §Membership (learner→voter, ConfChangeV2 joint consensus with the auto-leave caveat, odd-voter policy, tombstoned node ids), §Join handshake (node join token, CSR signed by the cluster CA stored as `__ostro/cluster-ca`), §Promotion (online `cluster init`, RF raised by reassignment, no export), §Driver extensions (TransferLeader, ReadIndex, per-peer progress, learner liveness) |
| CLUS-06 | Rolling upgrade across one minor version without draining runs; workers advertise WIT/engine versions and receive compatible activations only [FR-6.5] | §Compatibility gate (feature levels, `ClusterInfo.ext` ClusterVersion, `Member.ext` NodeVersion, write-old-until-finalized, fail-stop on unknown commands, downgrade fence), §Controlled shutdown + graceful leader handoff, §Worker compatibility predicate |
| CLUS-07 | Module GC is reference-counted against non-terminal runs; `ostro migrate-runs` re-pins runs only after a passing dry-run replay | §Module GC (retire → grace → per-partition reference scan → delete meta → Phase 8 blob GC sweeps), §migrate-runs (`RepinRun` Cmd with `expected_next_seq` + `from_sha` CAS, leader-local dry-run replay) |
| ENG-10 | The host links WIT `0.1.x` (and later `0.2.x`) side by side, selected by the component's imports | §LinkerSet (Wasmtime semver-aware import AND export lookup, verified in 50.0.2 source), per-function `introduced_in` table, upload gate by cluster level, fixtures built with `wit_component::dummy_module` |
| API-08 | Any node forwards requests to the owning partition leader; reads observe the caller's prior writes | §Routing (RoutedControlPlane decorator at the ControlPlane boundary, peer gRPC client over the cluster listener, hop limit, NotLeader retry with hint, unkeyed starts placed on a local leader, list fan-out), §Read-your-writes (leader lease + raft ReadIndex for meta + optional consistency token), §Watchers (per-term re-subscribe, re-route on leader change) |
</phase_requirements>

## Summary

The building blocks are mostly in place. The vendored driver already proposes `ConfChangeV2`, applies conf changes on every node, persists the full prost `ConfState` (voters, learners, outgoing voters, `auto_leave`), and lets a blank node join with `initial_voters = []` and catch up from a leader snapshot. Meta already carries `Member{addr, cluster_addr, repl_addr, ext}`, `PartitionAssignment{replicas, isr_version, min_insync, ext}` and `ClusterInfo.ext` (05-07, P12 F7). Run ids and await tokens embed their partition, so any node can compute the owner of every run-scoped request. The ControlPlane trait is the single seam that both REST and gRPC adapters call, and the 04-11 parity suite proves that an `ApiError` round-trips losslessly through gRPC status details. Wasmtime 50.0.2 resolves component imports **and exports** semver-compatibly (`a:b/c@0.1.0` matches a host `@0.1.N`), which makes `0.1.N` additive evolution essentially free at link time.

The work splits into five tracks. (1) **Membership:** a dynamic address book (Member records instead of static `[[cluster.peers]]`), a join handshake that reuses the Phase 7 kubeadm-style token and CSR pattern for nodes, a reconciler on the meta leader that drives learners to voters with an odd voter count (≤ 5), remove/drain, and four small driver additions (TransferLeader, ReadIndex, per-peer progress, learner liveness). (2) **Reassignment:** one new meta command, `AlterReplicas`, plus a controller state machine (add → catch up via OSNP → ISR → move leader → shrink) and a preferred-leader balancer. Promotion is "add replicas to a running RF=1 node", so it needs no export. (3) **Routing:** a `RoutedControlPlane` decorator that forwards whole ControlPlane calls to the owner over the cluster listener, with read-your-writes from a leader lease plus raft ReadIndex for meta reads. (4) **Upgrades:** a cluster feature level stored in `ClusterInfo.ext`, per-node versions in `Member.ext`, gated emission of every new format/command/import, fail-stop on anything unknown, and a downgrade fence after finalization. (5) **Engine and lifecycle:** a `LinkerSet` keyed by WIT major line (ENG-10), worker compatibility filtering, module GC and `migrate-runs` with a new `RepinRun` partition command.

The most important finding concerns behaviour, not bytes. **Today an older binary silently diverges when it sees a newer command.** Partition apply turns an unknown `Cmd` oneof tag into `Rejected(Invalid(Empty))` and keeps going (`crates/ostro-partition/src/apply/mod.rs:96-105`). It ignores unknown prost fields inside known messages. `MetaSm::apply` turns an undecodable command into `MetaResult::Invalid` and keeps going (`crates/ostro-meta/src/sm.rs`). In a mixed cluster, an old follower's hot-standby state or an old voter's meta state would quietly differ from the leader's. So the upgrade design rests on two rules: **never emit a new thing before the cluster is finalized for it**, and **fail-stop (never reject-and-continue) on anything unknown**.

**Primary recommendation:** before 05-20, add the Phase 13 compatibility rules to the FROZEN storage-format text and fix the WIT `run-info.module-hash` doc comment (P13-F1, P13-F3; both doc-only). Do not change any byte format: every Phase 13 meta addition fits registered `ext` tags and appended `MetaCommand` variants. Then build Phase 13 as **12 plans in 6 waves** (see §Proposed Plan Breakdown). The compatibility gate and fail-stop land in wave 2, before any feature that needs gating.

## PRE-FREEZE FINDINGS (key output)

Phase 5 freezes the log envelope, `node.toml`, the blob layout, the meta encoding and WIT in 05-19/05-20. In the table, "format change" means bytes written by v0.1 would change. "ext tag" means a registered `ext` TLV entry: bump-free under D20. "doc-only" means rule text or reservations.

| ID | Area | Finding | Type | Owner | Severity |
|----|------|---------|------|-------|----------|
| **P13-F1** | Frozen compatibility rules (storage-format FROZEN text + RESERVATIONS §10a) | The freeze text says what may be *added*. It says nothing about how an **older binary** must treat newer data, which is exactly the mixed-version and downgrade case. Evidence: unknown `Cmd` tag → `Rejected(Invalid(Empty))` and continue (`apply/mod.rs:101-105`); prost silently drops unknown fields in known messages, so a new `StartRun` field would be ignored on an old follower; `MetaSm::apply` maps a decode error to `MetaResult::Invalid` and continues. Each of these silently diverges replicas in a mixed cluster. **Add six rules to the FROZEN text:** (a) *Gate:* a binary emits a new `Cmd` tag, new prost field, new intent, new journal kind, new `MetaCommand` variant, new meta record variant, new outcome/reject code or a bumped format byte only when the cluster's **finalized feature level** includes it. (b) *Fail-stop:* an unknown `Cmd` tag, a command whose prost re-encoding is shorter than its bytes (unknown fields), or an unknown `MetaCommand` variant makes that partition (or that meta voter) **stop**, with a "written by a newer Ostro" message. Never reject-and-continue. (c) *Write-old-until-finalized:* every versioned format (META_FORMAT, OSNP header, STATE/VALUE_FORMAT, EVENT_CODEC_VER, envelope codec) is **written** at the finalized level's version, and new binaries keep the encoder for N−1. (d) *Apply semantics of existing commands and records never change* (generalises D20's "never tightened" to partition apply; new behaviour = new tag or field, gated). (e) *No apply-time validation of `ext` tag values*, only TLV structure (true today, `ext_validate` = `ext_entries`); otherwise old and new voters disagree. (f) *Boot order:* meta is opened and its cluster version checked **before** `Node::open` touches partition data (the 04-17 order, now a rule). The code for (b) and the gate land in Phase 13. No v0.1 log contains unknown tags, so the change is safe after the freeze, but the rule should be frozen with the formats. | doc-only (code in 13-02) | 05-20 (FROZEN text "Phase 13 doc rules", next to the P12 rules) | **SHOULD** |
| **P13-F2** | Meta: version, membership and reassignment state | Everything Phase 13 needs fits **registered ext tags** on existing records plus appended `MetaCommand` variants. No new `MetaRecord` variant, no META_FORMAT bump, **no format change**. Proposed registrations from the P12/P13 block 80–111 (P12 keeps 80–95, P13 takes 96–111): 96 `Member` NodeVersion `{ostro_version, level_min, level_max, wasmtime, event_codec_ver, wit_lines, reported_at_ms}`; 97 `Member` MemberState `{state: Joining/Active/Draining/Removed, changed_at_ms}` (a tombstone, so node ids are never reused); 98 `ClusterInfo` ClusterVersion `{finalized, hold: Option<u32>, finalized_at_ms}`; 99 `ClusterInfo` ReplicationPolicy `{rf, min_insync, max_voters}` (never server config: drift would let one leader ack on fewer copies, P12 OQ3); 100 `PartitionAssignment` Reassignment `{target, adding, removing, started_at_ms}` (KIP-455 shape); 101 `ModuleRecord` WitRequirement `{line, min_level}`; 102 `ModuleRecord`/`WorkflowVersion` Retired `{retired_at_ms}`; 103–111 spare. MetaCommand 8 `AlterReplicas`, 9–11 spare (reserved P13). Finalization uses the existing `Put(Cluster)` with CAS (no new command, no apply-time value check, see F1e). The 1.0 binary must already **read** tag 98 (an unknown tag is ignored by design). Phase 13 lands before 1.0, so this is satisfied. | ext tags + appended MetaCommand (bump-free); RESERVATIONS docs commit | RESERVATIONS §10a/§10b (docs-only commit before 13-01 code; may be after 05-20) | SHOULD (register early so P12 does not use 96–111) |
| **P13-F3** | WIT doc comment `run-info.module-hash` | `wit/ostro-runtime-0.1.0/runtime.wit:109` says "Hash of the module executing this activation", and the engine fills it from the prepared module (`host/context.rs:34`). After `ostro migrate-runs` re-pins a run, the value changes **between activations of the same run**. A guest that branches on it then diverges on replay (the dry-run would catch it, but the contract should say so). After 05-20 the file is byte-pinned, comments included, so this can only be fixed now. Suggested text: "May change when an operator re-pins the run (`ostro migrate-runs`); use for logging and metrics, never for control flow." | doc-only (WIT comment bytes) | 05-05 follow-up or 05-20 before the FROZEN flip | NICE (otherwise external docs only) |
| **P13-F4** | `node.toml` | **No change beyond P12 F6** (`node_id`, `cluster_id` reserved). The downgrade fence lives in meta (`ClusterInfo.ext` tag 98) and is checked before `Node::open` (F1f). A node.toml "data_version" key was considered and rejected: older binaries ignore unknown keys, so it protects nothing that the meta check does not. Two behaviour notes for 05-20 / Phase 12: (i) write `node_id` and `cluster_id` into node.toml at the **first single-node boot** too, so that a wiped meta dir over intact partitions waits for a leader instead of silently running `InitCluster` with a new cluster id and token key; (ii) amend the F6 text: "from Phase 13 the peer set is meta `Member` records; `[[cluster.peers]]` is a bootstrap seed list only". | doc-only (+ behaviour in P12/P13) | 05-20 FROZEN text; code 13-05 | INFO |
| **P13-F5** | Log envelope, `Command` tags 1–7, fencing, cursor | **No change.** Membership, reassignment and routing never touch the partition log. The only new partition command is `RepinRun` (CLUS-07), which claims **Cmd 43** (+44 spare) from the unallocated range, and RejectReason 23 `ModuleMismatch`, 24 `RunBusy` from the unallocated range. Both are additive and gated (F1a). `RunRecord.module_sha` already exists, so repinning changes no value format. | additive post-freeze (RESERVATIONS claim) | RESERVATIONS docs commit (rule 2) | INFO |
| **P13-F6** | Raft log store / ConfState | **No change.** `RaftLogStore` persists `HardState + ConfState` as prost `eraftpb` (`log_store.rs:128,562`), so learners, `voters_outgoing`, `learners_next` and `auto_leave` survive restarts. Snapshots carry `conf_state` in their metadata. Joint consensus is restart-safe with the frozen meta Raft log format. | — | — | — |
| **P13-F7** | Partition count default (D8 USER REVIEW) | A promoted single node keeps P=16 forever (no re-sharding). Leaders per node: 6/5/5 on 3 nodes and 4/3/3/3/3 on 5, so the busiest node leads 20–33% above the mean. RF=3 replica slots balance well (16/16/16; 10/10/10/9/9). A higher default costs single-node memory, because the fjall cache is per partition (`budget / P`), and cold-start fsyncs. **Recommendation: keep 16** and tell reviewers of D8 about the promoted-cluster skew. Fresh clusters can choose `--partitions 64` at `cluster init` time, before data exists. | none (config default) | D8 user review | INFO |
| **P13-F8** | Worker proto (Phase 7, not frozen) | `Hello` carries `ostro_version`, `wasmtime_version`, `event_codec_ver` and `wit_packages` but no numeric feature level, and Phase 7 requires an exact version match. Phase 13 can parse the level from `ostro_version`. A `uint32 feature_level = 10` field added in 07-02 would be cleaner. The worker proto is additive and outside the freeze. | additive proto field | 07-02 (optional) or 13-09 | INFO |

**Net for the Phase 5 freeze:** no byte-format change. Two doc items should land in 05-20 (P13-F1 compatibility rules; P13-F4 F6 amendment), and one WIT comment can only land before the flip (P13-F3). The RESERVATIONS claims (F2, F5) are docs-only commits allowed after the freeze. A real format change would be needed only if Phase 13 had to add fields to an existing meta struct, and nothing found here requires that.

## Standard Stack

### Core (all pinned or vendored; no new third-party crates)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| vendored `ostro-raft-driver` + `ostro-raft-rs` (raft-rs 0.7.0) | in-tree | ConfChangeV2 (simple and joint), learners, `transfer_leader`, `read_index` (ReadOnlyOption::Safe default), pre-vote + check_quorum already on | Already the meta engine; every capability verified in `vendor/ostro-raft-rs/src/{raw_node,raft}.rs` |
| tonic / tonic-prost / prost | 0.14.9 / 0.14.9 / 0.14.6 | `ostro.cluster.v1.Membership` (Join, Heartbeat, Removed), peer ControlPlane client for forwarding, `ostro.api.v1.Cluster` admin service | Existing gRPC stack, Phase 7 mTLS on the cluster listener |
| wasmtime | =50.0.2 | Component `Linker` with semver-aware name lookup for imports and exports (`wasmtime-environ` `NameMap`) | Verified in source: `linker.rs:26-58`, `component.rs:1170` test, `environ/component/names.rs` |
| wit-component / wit-parser | =0.261.0 (feature `semver-check` ⇒ `dummy-module`) | `dummy_module` builds fixture components for 0.1.0 / 0.1.1 / 0.2.0 worlds without a guest toolchain; `ostro-witcheck` stays the additivity gate | Already pinned and enabled |
| rcgen / x509-parser | Phase 7 pins | Node CSR signing (SAN `ostro://<cluster>/node/<id>`), join-token flow | Phase 7 07-06 `ClusterCa::{import_pem, issue_node_cert}` |
| proptest / proptest-state-machine | 1.12.0 / 0.9.0 | Placement planner, membership reconciler, reassignment state machine, version gate | Already pinned |
| fail | 0.5.1 | `member::`, `reassign::`, `route::`, `upgrade::`, `gc::` failpoints; per-process in tier C | Existing FAILPOINT_CRATES |

### Supporting
| Library | Purpose | When to Use |
|---------|---------|-------------|
| Phase 12 harness (`ostro-repl-conformance`: in-memory tonic transport with fault layer, process harness, chaos proxy, oracles) | Extended with `join`, `remove`, `drain`, `upgrade(node, level)`, `client(node)` | All cluster suites |
| `semver` (already a transitive Wasmtime dep) | Parse `ostro:runtime/*@x.y.z` import versions in `LinkerSet` | ENG-10 import inspection only; anchor or use `wasmtime`'s parsed names instead if the workspace forbids the direct dep |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Forward whole ControlPlane calls (Pattern 4) | Forward partition commands and reads (a `PartitionAccess` RPC set) | Needs new data-plane wire types for every read, and **ships payload blobs** (> 256 KiB inputs are written on the receiving node before submit). Rejected. Forwarding the call keeps blob preparation on the leader and reuses the parity-proven proto. |
| Feature level in `ClusterInfo.ext` + `Put` CAS | A `FinalizeVersion` MetaCommand with apply-time member checks | An apply-time check that reads `Member.ext` would be new value validation (P13-F1e). The proposer-side check plus a node-side start refusal is enough. MetaCommand 9 stays spare in case a later phase needs it. |
| Raft ReadIndex for meta reads | Forward every meta read to the meta leader / read stale local meta | Forwarding costs a hop and leader load; stale local reads break API-08 for clients behind a load balancer. ReadIndex costs one heartbeat round and then serves locally. |
| Manual learner promotion (etcd style) | Automatic reconciler promotion | etcd keeps promotion manual. Ostro's operator surface is `join`/`remove`, so the reconciler promotes only when caught up (matched ≥ commit − small lag, snapshot done) and the voter-count policy says so. Same safety checks as etcd, no extra operator step. |
| Real N−1 binary in every CI run | Feature-level emulation (`OSTRO_TEST_LEVEL_MAX`, failpoint builds only) | No previous release exists yet. Emulation tests the gate logic on every commit; a real-binary test (`OSTRO_PREV_BIN`) is env-gated and human_needed until the first release tag. |
| turmoil / madsim | — | Rejected in 12-RESEARCH (they fight fjall and WAL threads). |

**Installation:** no new crates. New proto definitions under `crates/ostro-proto/proto/ostro/cluster/v1/` (`membership.proto`) and `ostro/api/v1/cluster.proto`.

## Architecture Patterns

### Recommended Project Structure
```
vendor/ostro-raft-driver/src/driver.rs      # + TransferLeader, ReadIndex, progress in RaftStatus, learners in sample_live
crates/ostro-cluster/src/                   # Phase 12 crate (or module), extended:
├── address_book.rs                         # id -> cluster_addr from meta Member records (+ config seeds for bootstrap)
├── membership/
│   ├── reconcile.rs                        # PURE: (members, conf_state, progress, live) -> next ConfChangeV2 | LeaveJoint | Transfer | none
│   ├── join.rs                             # join handshake service (token, CSR, node id allocation)
│   └── remove.rs                           # drain -> reassign off -> transfer -> conf change -> tombstone
├── placement/
│   ├── plan.rs                             # PURE: balanced replica placement with minimal moves
│   ├── reassign.rs                         # PURE state machine: Idle -> Adding -> CaughtUp -> LeaderMoved -> Shrunk
│   └── leaders.rs                          # PURE: preferred-leader balancing decisions
├── compat.rs                               # feature levels, gate registry, finalize/hold, start-time refusal
└── handoff.rs                              # graceful leader handoff + controlled shutdown
crates/ostro-server/src/
├── route/                                  # API-08
│   ├── mod.rs                              # RoutedControlPlane<L: ControlPlane> decorator, route table
│   ├── peer.rs                             # PeerControlPlane: ControlPlane over a gRPC channel to a peer
│   └── consistency.rs                      # lease check, meta ReadIndex barrier, consistency tokens
├── control/watch.rs                        # per-term re-subscribe + LeaderChanged wake
├── control/migrate.rs                      # migrate-runs dry-run + RepinRun
└── services/module_gc.rs                   # retire -> grace -> scan -> delete
crates/ostro-engine/src/host/linker_set.rs  # ENG-10
```

### Pattern 1: Membership as desired state, reconciled by the meta leader
**What:** `Member` records (plus the `MemberState` ext tag) are the desired membership. The Raft `ConfState` is the actual state. A pure reconciler on the meta leader moves actual toward desired one conf change at a time.
- **Join:** `Member{state: Joining, cluster_addr}` is Put first (so the address book knows where to send), then `AddLearnerNode(id)`. The leader streams a raft snapshot; the learner catches up.
- **Promote:** if `desired_voters > |voters|` and the learner is caught up (`matched ≥ commit − promote_lag`, no pending snapshot, live within `session_timeout`), propose `AddNode(id)`. One voter change is a simple change; raft-rs only uses joint consensus when more than one voter changes.
- **Voter policy:** `desired_voters = largest odd ≤ min(live non-draining members, max_voters = 5)`. 2 nodes → 1 voter + 1 learner; 4 nodes → 3 + 1; 6+ nodes → 5 voters, the rest learners (every node still applies meta, so every node routes).
- **Replace** (remove a voter while promoting a learner): one `ConfChangeV2` with `transition = Auto` (joint with auto-leave).
- **Leave joint:** raft-rs auto-leaves only if the *same* leader applies the enter-joint entry (`raft.rs:983` TODO: "it may never auto_leave if leader steps down before enter joint is applied"). The reconciler therefore checks every tick: `voters_outgoing` non-empty and no pending conf → propose an empty `ConfChangeV2` (leave joint).
- **Remove:** `MemberState = Draining` → the reassignment controller moves every replica off → if the node is the meta leader, `transfer_leader` first → `RemoveNode(id)` → `MemberState = Removed` (tombstone kept forever; ids allocated as `max(all ids incl. tombstones) + 1`, so they are never reused) → `Membership.Removed` RPC tells the node to stop meta participation.
- **Safety checks before any voter removal:** the remaining voters, intersected with the live set (`sample_live`), must form a quorum of the new config; refuse otherwise (`--force` only skips waiting on the dead node itself, never this check).

### Pattern 2: Join handshake (reuse the Phase 7 worker-join shape for nodes)
1. On any node: `ostro cluster token create --node` → `ApiToken{id: "njt_<id>", hash, scopes: ["node:join"], expires_at_ms}`.
2. New machine: `ostro cluster join https://seed:7701 --token <t> --ca-hash sha256:<hex>`. TLS server-auth with CA-hash pin (D-W20 bootstrap verifier), then `Membership.Join{token, csr, advertise, node_version}`.
3. The seed forwards to the meta leader, which validates the token, allocates the id (`Put Member` create-only; Conflict → next id), signs the CSR with the cluster CA, and returns `{node_id, cluster_id, partitions, ca_cert, node_cert, members, finalized_level}`.
4. The joiner refuses if `finalized_level ∉ [level_min, level_max]` (no skipped minors). It then writes `<data>/tls/*`, writes `node.toml` `node_id`/`cluster_id`, and starts meta with `initial_voters = []`.
5. **Never `InitCluster` on the join path.** Rule: a blank meta log plus a `cluster_id` already in node.toml means joiner: wait for the leader and fail loudly after `join_timeout`. Never self-bootstrap, which would split-brain into a second cluster.

**CA key distribution:** at `cluster init`, the CA cert and key are stored as `SealedSecret "__ostro/cluster-ca"` (existing record kind, existing `__ostro/` prefix; plaintext-v0 sealing until Phase 14, like the token key). Any meta leader can then sign joins after node 1 dies. Phase 14 seals it. This needs a name reservation only.

### Pattern 3: Promotion without export (`ostro cluster init` on a running single node)
Online, no restart, no data movement:
1. Preconditions: server profile (refuse in `dev`/`--ephemeral`), cluster listener bound, `[cluster] advertise` set or passed.
2. Write `node.toml` `node_id`/`cluster_id` (if absent; P12 F6). Put own `Member{cluster_addr, repl_addr, ext: NodeVersion, MemberState Active}`. Store `__ostro/cluster-ca`. Put `ClusterInfo.ext` ClusterVersion (finalized = own level) and ReplicationPolicy (`rf = 3`, `min_insync = 2`, `max_voters = 5`; effective RF = min(rf, members)). Re-issue the node cert if the SANs changed (hot-swappable resolver, 07-06).
3. The meta group is already a one-voter Raft with this node's id. **The transport must be wired in single-node mode too** (an address book with no peers), so the first join needs no restart. Phase 12 must not hard-code `NullOutbox` for single node in a way that needs a restart. Flag this to the Phase 12 planner.
4. Joins (Pattern 2) add learners/voters. The reassignment controller then raises each partition from RF=1 to the effective RF: `AlterReplicas` adds followers, followers install OSNP and fetch the tail, `AlterIsr` expands the ISR (Phase 12 rule), and only then does `min_insync` rise to the policy value (**never above |ISR|**, or writes stop).
5. Partitions stay led by node 1 throughout, so runs never stop. The preferred-leader balancer moves leaders later, one at a time, with graceful handoff (Pattern 6).

### Pattern 4: Any-node routing at the ControlPlane boundary (API-08)
**What:** `RoutedControlPlane<L>` wraps the local `ServerControlPlane` and implements `ControlPlane`. The REST and gRPC adapters are unchanged, so both get forwarding.

| ControlPlane method | Route |
|---|---|
| `start_run` with idempotency key | owner of `partition_for_key(qualified workflow, key, P)` |
| `start_run` without key | a **locally led** partition if any (random among them; zero hops, "any partition would do because the run id embeds it", `routing.rs`); else uniform random partition, forwarded with header `ostro-partition: p` (honoured only from node identities). `[api] unkeyed_start_placement = local|uniform` |
| `get_run`, `run_history`, `signal_run`, `cancel_run`, `terminate_run`, `retry_run`, `await_result` | owner of `RunId::partition()` |
| `complete_token` | owner of the partition embedded in the token (`ot1_` + partition BE) |
| `list_runs` | fan-out: one forwarded call per leader node carrying `ostro-partitions: a,b,c`; merge with the existing composite cursor |
| `upload_module` | local: store the blob, push it to ≥ `min(min_insync, live members)` peers (P12 F11 module rule), then a forwarded meta propose; wait local meta applied ≥ index |
| `activate_workflow`, `set_*_policy`, deletes | local validation + forwarded meta propose (P12 `MetaPeer.Propose`) + wait local applied ≥ index |
| `list_workflows`, `get_module`, `get_*_policy` | local meta view after a ReadIndex barrier (Pattern 5) |
| cluster admin (`init/join/remove/status/reassign/upgrade`) | meta leader (forwarded) |

**Mechanics:**
- `PeerControlPlane` = a `ControlPlane` implemented by the existing `ostro.api.v1` gRPC client (the 04-11 parity `GrpcClient`, productionised) over an mTLS channel to the peer's **cluster listener**, which also serves `ostro.api.v1.*` to node identities only.
- **Hop limit 1:** forwarded calls carry `ostro-hop: 1`, and a node that receives one never forwards again. If it is not the leader, it returns `UNAVAILABLE` with details `{not_leader: {partition, leader_hint, epoch}}`.
- The forwarder waits until its meta has applied the hinted epoch, re-routes once, and then returns 503 + `Retry-After: 1` / `UNAVAILABLE`. A partition with no leader (offline or mid-election) gives the same 503.
- The deadline propagates (`grpc-timeout` = remaining time); long-poll calls carry their remaining wait.
- Principal propagation (Phase 14 hook): the forwarder authenticates and passes `ostro-principal` (tenant, scopes); the receiver trusts it only from node SANs.
- Mixed versions: forwarded calls use the public proto, which is additive by policy. A request using a gated field is refused at the receiving node until finalized (`FAILED_PRECONDITION feature_not_enabled`).

### Pattern 5: Read-your-writes
1. **Writes** are acknowledged only after apply on the leader (existing). Meta writes return after the receiving node's local applied ≥ index.
2. **Partition reads** are served only by a leader that holds a **lease**: role = leader, its `LeaderAnnounced` applied, and meta contact age < `session_timeout × (1 − margin)`, with margin 0.2. Phase 12 self-demotes at `session_timeout`, and the controller elects only after not hearing from the old leader for `session_timeout`, measured from a later instant. A zombie therefore stops serving before a successor can exist. The margin covers rate drift (monotonic clocks only, never wall time).
3. **Meta reads:** raft `read_index` (Safe) on the local driver. raft-rs forwards `MsgReadIndex` to the leader, which confirms leadership with a heartbeat quorum. Then wait local applied ≥ read index and serve locally. This costs one RTT and no data hop.
4. **Consistency token (optional, also for SDK/CLI):** every write response carries `ostro-consistency: p<P>:<epoch>:<offset>` (partition) or `m:<index>` (meta). A read carrying `ostro-read-after` is served by any node whose applied state covers it, and skips the barrier. This is a pure optimisation; correctness does not depend on clients echoing it.
5. **Indeterminate outcomes on failover** (P12 OQ5): retries are safe through idempotency keys, signal `request_id` and `(epoch, attempt)` fencing. Document this.

### Pattern 6: Graceful leader handoff and controlled shutdown (no drain)
For a move of partition p from A to B (B ∈ published ISR):
1. A stops admitting new attempts (dispatcher admission off). It waits up to `handoff_grace` (default 2 s) for in-flight activations to reach a flush or suspend point, and evicts hot-waiting runs (they flush first).
2. A waits until B's durable LEO equals A's LEO (tracker), bounded by `handoff_catchup_timeout`.
3. `ElectLeader{p, epoch+1, leader: B, isr, expected_isr_version}` (Phase 12 command; validation ensures B ∈ ISR).
4. A becomes follower (`acks.fail_all(NotLeader{B})`) and B runs the Phase 12 become-leader steps (LA, rebuild, re-grant).
Activations that did not finish replay on B from the journal. Only an in-flight step body re-runs (at-least-once steps; effects are idempotent by the outbox). **No run is drained, cancelled or failed.**

**Controlled shutdown** (SIGTERM in cluster mode, or `ostro cluster drain <id>`): hand off every led partition (preferred order, skip self), transfer meta leadership if held, then run the existing shutdown (readyz 503 → API drain → `Node::shutdown` → meta stop). Bounded by `[cluster] shutdown_handoff_timeout` (default 30 s); on timeout it proceeds and the Phase 12 failover covers the rest.

**Preferred-leader balancer:** every `leader_rebalance_interval` (5 min), if a node leads > `ceil(P / members) + 1` partitions or a partition's preferred replica (`replicas[0]`) is in the ISR but not leading, move one partition at a time with the handoff above.

### Pattern 7: Partition reassignment (KIP-455 shape on the Phase 12 ISR log)
`AlterReplicas{partition, epoch, expected_isr_version, replicas, min_insync, reassignment: Option<Reassignment>}` (MetaCommand 8). Apply validates the epoch equals the current one, the ISR version matches, `leader ∈ replicas`, `isr ⊆ replicas` and `1 ≤ min_insync ≤ |isr|`. It bumps `isr_version`, so `isr_version` is the CAS token for the partition's replica-and-ISR state. Document that the frozen field name now covers replica-set changes too.

State machine (pure, persisted only in meta, so it survives controller failover):
1. **Expand:** `replicas = old ∪ target`; ext `Reassignment{target, adding, removing}`.
2. **Catch-up:** adding replicas fetch (OSNP install if behind `log_start`, P12 Pattern 6). The leader's Phase 12 expand rule adds them to the ISR.
3. **Leader move:** if `leader ∉ target`, graceful handoff to the first in-sync member of `target`.
4. **Shrink:** `replicas = target`, `isr = isr ∩ target`, clear the ext, set `min_insync = policy` capped at |ISR|.
5. **Cancel:** before step 3, revert to the old replicas.

Throughput: at most `[cluster] max_concurrent_moves` (default 2) at once. Snapshot transfer and catch-up compete with live replication; a bytes/s throttle is optional (measure first).

**Placement planner (pure):** given members (Active, not Draining), assignments and effective RF, produce moves such that (i) no node holds two replicas of a partition, (ii) replica counts differ by ≤ 1 across nodes, (iii) the number of moves is minimal (greedy from the current assignment, not from scratch), (iv) preferred leaders (`replicas[0]`) are spread so leader counts differ by ≤ 1. Proptest these invariants.

### Pattern 8: Compatibility gate (CLUS-06)
- **Feature level:** `ostro_types::compat::LEVEL: u32` increases by one per minor release. A binary supports `[LEVEL_MIN = previous minor, LEVEL_MAX = LEVEL]` ("across one minor version"). Every gated feature declares `introduced_at: u32` in one registry (`compat::Feature::{RepinRun, AlterReplicas, Wit0_1_N(n), MetaFormat3, Osnp2, …}`).
- **Advertisement:** each node upserts its `Member.ext` NodeVersion at start, after meta catch-up (read-modify-write preserves unknown tags).
- **Start-time fence (before `Node::open`):** `finalized ∉ [LEVEL_MIN, LEVEL_MAX]` → refuse ("data finalized by a newer Ostro: downgrade not supported after finalization" / "upgrade through X first"). A META_FORMAT byte above the build's maximum gets the same message (today it is a generic decode error). Joiners check in the handshake.
- **Running fence:** every node watches `ClusterInfo`; finalized > `LEVEL_MAX` → fail-stop (a node that was not upgraded but somehow stayed up).
- **Proposers check `gate(feature)`** before emitting anything new: partition commands (leader-side builders), meta commands (`MetaHandle::propose`), META_FORMAT/OSNP writers, module uploads requiring a newer WIT level, API fields.
- **Finalize:** auto-finalize when every non-Removed member reports `level_max ≥ target`, every member is live, and no `hold` is set (CockroachDB model: `ostro cluster upgrade hold` keeps the downgrade window open). Manual `ostro cluster upgrade finalize`. Implemented as `Put(Cluster)` with CAS on the record version.
- **Downgrade policy:** allowed until finalization (data is still written at the old level, rule F1c). Refused after. No unsafe-downgrade path in 1.x (Kafka's KIP-1155 shows how hard that is).
- **Fail-stop backstop** (F1b) for the case where gating has a bug.

### Pattern 9: LinkerSet (ENG-10)
- One `Linker<HostState>` per **WIT major line** (`0.1`, later `0.2`). Within a line the host defines **only the newest `0.1.N`** interfaces. Wasmtime's `NameMap` keeps an alternate key `ostro:runtime/journal@0.1` → newest definition, so a component importing `@0.1.0` (or exporting `workflows@0.1.0`) resolves to it (verified for imports `linker.rs:26-58` and exports `component.rs:1170`).
- **Selection:** at `validate_component`, read `component.component_type().imports(engine)`. Every `ostro:runtime/*` import must belong to one line; mixing lines is an upload error. Choose that line's linker; the AOT/InstancePre cache key gains the line.
- **Requirement:** the minimum feature level is the max over the imported **functions** of `introduced_in(func)`. Use functions, not the package version string: a guest built against `0.1.1` that calls only 0.1.0 functions runs on a 0.1.0 host. Store it in `ModuleRecord.ext` WitRequirement (tag 101); absent = level of 0.1.0.
- **Upload gate:** the module's `min_level > finalized` → `409 feature_not_enabled` ("finalize the upgrade first"). So no old node is ever asked to run it.
- **0.2 proof without shipping 0.2:** a test-only fixture package `ostro:runtime@0.2.0` (feature `wit-next-fixture`) with its own `bindgen!` module and one changed signature, plus `dummy_module` fixture components for 0.1.0, 0.1.1 and 0.2.0. Show that selection, instantiation, export lookup and the mixed-line rejection all work, and that the replay corpus stays green.

### Pattern 10: Module GC and `migrate-runs` (CLUS-07)
**Module GC (race-tolerant, two phases):**
1. `ostro modules rm <sha>` / `DELETE /v1/modules/{sha}` marks the module and its WorkflowVersions Retired (ext tag 102). It refuses if any version is Active. Starts and repins to retired versions are refused at the control plane, which reads meta behind the ReadIndex barrier.
2. The GC service runs on the meta leader every `[modules] gc_interval`. For each module retired longer than `gc_grace` (default 10 min, more than the longest start path), it asks every partition leader for referenced shas: a scan of `runs` for non-terminal rows (`Scheduled/Ready/Running/Suspended`), plus resumable terminal rows when `[modules] gc_protect_resumable = true` (D1: retry resumes in place, so the module is needed). It scans twice, `gc_grace / 2` apart. If unreferenced both times, it deletes WorkflowVersion records, then the Module record (CAS).
3. Phase 8 blob GC on each node then sweeps the blob (its mark set includes meta Module shas, 08-RESEARCH Pattern 7).
- Residual race (a start request slower than the grace): the executor finds no module and the run fails loudly with non-retryable `module_gone`. It is never silent.
- A per-partition incremental refcount keyspace was rejected: it adds a keyspace (digest-order placeholder rules) for a scan that runs hourly.

**`migrate-runs`:**
- `POST /v1/runs/migrate {workflow, from (version|sha), to_version, filter, limit, dry_run}` fans out per partition leader.
- Each leader lists candidate non-terminal runs on `from_sha`, reads each journal and replays it against the target module in a sandbox `Runtime` (frozen FakeClock, replay corpus style). Any `NonDeterminism`/`ProtocolViolation` fails that run.
- Without `dry_run`, it submits `RepinRun{run, from_sha, to_sha, expected_next_seq, requested_at_ms}` (Cmd 43) **only for runs that passed**.
- Apply checks: the run exists and is non-terminal; `module_sha == from_sha` else `ModuleMismatch`; `next_seq == expected_next_seq` else `SeqMismatch` (the journal grew, so dry-run again); no `running` row (leased) else `RunBusy` (retry later). Then it sets `RunRecord.module_sha = to_sha`.
- The report lists passed, repinned, failed (with the divergence) and busy runs. "Re-pins only after a passing dry-run replay" is enforced by construction: the `expected_next_seq` equals the replayed journal length.

### Anti-Patterns to Avoid
- **Reject-and-continue on unknown input** (today's behaviour): silent replica divergence. Fail-stop (F1b).
- **Emitting new formats as soon as the binary supports them:** the first upgraded leader would break every old follower. Gate on the *finalized* level (F1a/F1c).
- **Membership from config after the first boot:** `[[cluster.peers]]` drift would fight the Raft conf. Config seeds bootstrap only; warn on drift.
- **Promoting a lagging learner:** commit stalls until it catches up. Check `matched` and snapshot state first (etcd's promote guard).
- **Trusting raft-rs auto-leave:** see the raft.rs:983 TODO; always reconcile the joint state.
- **Removing the meta leader without a transfer:** commit stalls through an election. Transfer first.
- **Reusing node ids:** a stale data dir or stale ISR entry would be mistaken for a fresh replica. Use tombstones.
- **Raising `min_insync` before the ISR has grown:** writes stop cluster-wide for that partition.
- **Forwarding loops / unbounded retries:** hop limit 1, one re-route, then 503.
- **Serving partition reads from a leader without a lease:** zombie reads break read-your-writes.
- **Defining both `@0.1.0` and `@0.1.N` instances in the linker by hand:** `JOURNAL_IFACE` is hard-coded to `"ostro:runtime/journal@0.1.0"` (`host/state.rs:493`). When bindgen moves to `0.1.N`, the shadowed `begin-step`/`step.complete` overrides would create a separate `@0.1.0` instance holding only the two shadowed functions. An exact-name match is preferred, so 0.1.0 guests would then fail to instantiate. Derive the interface names from one `WIT_0_1_LATEST` constant and test it.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Membership change safety | Custom two-phase membership protocol | raft-rs `ConfChangeV2` (simple for one voter change, joint with `Auto` transition for more) + learners | Joint consensus is subtle; raft-rs implements it (`confchange/`), and the log store already persists the full ConfState |
| Linearizable meta reads | Leader leases in meta, or "read on leader" forwarding | raft-rs `read_index` (ReadOnlyOption::Safe) | Built in; needs only a driver event and `take_read_states` handling |
| Leadership transfer | Stepping down by killing the node | raft-rs `transfer_leader` (MsgTimeoutNow) | Already in raft-rs (`raw_node.rs:753`); driver lacks only the event |
| Version-compatible WIT linking | Per-version linker copies, or rewriting import names | Wasmtime's semver-aware `NameMap` (imports + exports) | Verified in 50.0.2 source |
| Fixture components for ENG-10 | Building guest crates for fake WIT versions | `wit_component::dummy_module` + `ComponentEncoder` | Already enabled through `semver-check` |
| Forwarding wire format | New per-op data-plane protos | The public `ostro.api.v1` gRPC proto via a peer client | Parity suite already proves REST ≡ gRPC and lossless `ApiError` mapping |
| Node certificates and join | A new PKI | Phase 7 `ClusterCa` (CSR signing, SAN identities, CA-hash pin) | Same kubeadm-style flow as worker join (D-W4) |
| Failure detection for learners | A new heartbeat protocol | Raft heartbeats already reach learners; extend `sample_live` to iterate learners | raft-rs leaders heartbeat every progress entry, including learners |
| Upgrade state | Version files on every node | `ClusterInfo.ext` ClusterVersion + `Member.ext` NodeVersion (replicated meta) | One source of truth, CAS-guarded |

**Key insight:** every hard part (consensus reconfiguration, linearizable reads, semver linking, PKI) already exists in a vendored or pinned dependency. Phase 13's own risk is *wiring and mixed-version behaviour*. The defence is a single gate registry, fail-stop on unknown input, and an oracle-driven multi-process suite with version skew.

## Common Pitfalls

### Pitfall 1: Old replicas silently diverge on new commands
**What goes wrong:** the upgraded leader emits `RepinRun` (or a new `StartRun` field). An old follower rejects or ignores it; its state hash differs; after failover to it the run's state is wrong.
**Why it happens:** prost drops unknown fields; apply maps an empty oneof to `Rejected(Empty)`; `MetaSm` maps decode errors to `Invalid`.
**How to avoid:** gate emission on the finalized level (F1a); fail-stop on an unknown tag or a re-encode length mismatch (F1b). Test: a level-L leader with a level-(L−1)-emulated follower never emits gated commands; a forced gated command stops the old follower's partition with the "newer Ostro" message.
**Warning signs:** standby-identity oracle mismatch after an upgrade test; `Rejected(Invalid(Empty))` in a follower's outcome stream.

### Pitfall 2: Stuck in joint consensus
**What goes wrong:** the leader changes between proposing and applying an enter-joint entry. The new leader never auto-leaves, and the cluster runs with two quorums required forever.
**How to avoid:** reconcile `voters_outgoing` every tick and propose an empty `ConfChangeV2`. Test: kill the meta leader right after it proposes a replace.
**Warning signs:** `RaftStatus.voters_outgoing` non-empty for more than a few seconds.

### Pitfall 3: A joiner bootstraps its own cluster
**What goes wrong:** a joining node times out waiting for the leader and runs the single-node bootstrap (`InitCluster`), producing a second cluster with the same partitions.
**How to avoid:** `cluster_id` in node.toml + blank meta ⇒ joiner, never `InitCluster` (Pattern 2 step 5). Test: start a joiner with the leader unreachable; it must fail, never serve.

### Pitfall 4: Large raft snapshots vs gRPC limits
**What goes wrong:** a new learner's first `MsgSnapshot` carries the whole meta state (tokens, secrets, schedules, webhooks) in one raft `Message`. tonic's default 4 MB decoding limit drops it and the learner never catches up.
**How to avoid:** set `MetaPeer`'s max decode size to a configured cap (e.g. 256 MiB), log meta snapshot size as a metric, and test with a meta state above 4 MiB. Report `report_snapshot_failed` on transport errors so raft retries.

### Pitfall 5: Quorum loss through removal
**What goes wrong:** removing a voter while another voter is down leaves no quorum.
**How to avoid:** pre-check with `sample_live` (Pattern 1). Refuse even with `--force`.

### Pitfall 6: `min_insync` raised before the ISR grows (promotion)
**What goes wrong:** the policy says `min_insync = 2` but the partition still has ISR = {node1}, so writes stop.
**How to avoid:** the reassignment state machine raises `min_insync` only in the shrink step, capped at |ISR|. Apply validates `min_insync ≤ |isr|`.

### Pitfall 7: Unkeyed starts change partition when forwarded
**What goes wrong:** the forwarding node routes by a uuid it generated; the leader generates a new uuid and partition, and the run lands on a partition the leader does not own.
**How to avoid:** prefer local leaders; when forwarding, pin the partition with `ostro-partition` and have the leader honour it (node identities only).

### Pitfall 8: Long-poll waiters stranded on a demoted leader
**What goes wrong:** `RunWatchers` subscribed once per partition (05-13 SUMMARY). After demotion no `RunCompleted` action ever arrives, and the waiter sleeps until its 60 s cap.
**How to avoid:** subscribe per term. On become-follower, wake that partition's waiters with `LeaderChanged`; the caller re-routes with the remaining deadline. Test: `await_result` spanning a graceful handoff returns the result before the deadline.

### Pitfall 9: AOT compile storm after a rolling upgrade
**What goes wrong:** a new Wasmtime invalidates every `.cwasm` (cache key includes the compatibility hash, 03-06), so all modules recompile on first activation on each upgraded node (PITFALLS #18).
**How to avoid:** after start, pre-warm modules referenced by ready/running runs of partitions the node leads or follows, with bounded concurrency. Measure first-activation p99 in the rolling-upgrade test.

### Pitfall 10: Version check after partition open
**What goes wrong:** an old binary opens partition stores (possibly running a migrator or truncating a tail) before learning that the cluster is finalized above it.
**How to avoid:** F1f boot order; test a downgrade-after-finalize start: it refuses before any `state/` or `wal/` file is opened for write (mtime check).

### Pitfall 11: Learners invisible to liveness
**What goes wrong:** `SampleLive` iterates only voters (`driver.rs:504`), so the controller cannot see that a learner died, and ISR shrink and failover for partitions led by learner nodes depend on it.
**How to avoid:** iterate `voters ∪ learners` (raft heartbeats reach learners). Test: kill a learner node that leads partitions; failover within the Phase 12 budget.

### Pitfall 12: Vendored error text points at Chronik
**What goes wrong:** the wiped-voter remedy message says `cluster remove-node … --force` and `CHRONIK_CLUSTER_JOIN=true` (`driver.rs` Step handler). Operators follow a remedy that does not exist.
**How to avoid:** replace it with `ostro cluster remove <id> --force`, wipe the data dir, `ostro cluster join` (new id). Keep the existing test, with updated asserts.

### Pitfall 13: Ext value validation creeping into apply
**What goes wrong:** someone validates tag 98 at apply ("finalized never decreases"). New voters reject a Put that old voters accept, and meta diverges.
**How to avoid:** F1e; validation only at proposers; a unit test asserts `validate.rs` never decodes ext values.

## Code Examples

### Reconciler decision (pure)
```rust
// crates/ostro-cluster/src/membership/reconcile.rs (sketch)
pub enum Step { LeaveJoint, AddLearner(u64), Promote(u64), Replace { add: u64, remove: u64 },
                Remove(u64), TransferLeader(u64), None }
pub fn next(m: &Members, cs: &ConfState, prog: &Progress, live: &BTreeSet<u64>, me: u64, max_voters: usize) -> Step {
    if !cs.voters_outgoing.is_empty() && !prog.conf_pending { return Step::LeaveJoint; }  // raft.rs:983 caveat
    if prog.conf_pending { return Step::None; }                                           // one change at a time
    if let Some(id) = m.joining().find(|id| !cs.voters.contains(id) && !cs.learners.contains(id)) {
        return Step::AddLearner(id);
    }
    if let Some(id) = m.removing().find(|id| cs.voters.contains(id) || cs.learners.contains(id)) {
        if *id == me { return Step::TransferLeader(pick_successor(cs, prog, live)); }
        if !quorum_survives(cs, *id, live) { return Step::None; }                         // Pitfall 5
        return Step::Remove(*id);
    }
    let want = largest_odd_le(m.active_live(live).count().min(max_voters));
    if cs.voters.len() < want {
        if let Some(l) = cs.learners.iter().find(|l| prog.caught_up(**l) && live.contains(l)) {
            return Step::Promote(*l);
        }
    }
    Step::None
}
```

### Driver additions (vendored; record in VENDORED.md)
```rust
// vendor/ostro-raft-driver/src/driver.rs (sketch)
enum Event { /* … */ TransferLeader(u64), ReadIndex { reply: oneshot::Sender<Result<u64, ProposeError>> } }
// handle_event:
Event::TransferLeader(to) => self.node.transfer_leader(to),
Event::ReadIndex { reply } => { let ctx = self.next_seq().to_be_bytes().to_vec();
                                self.read_waiters.insert(ctx.clone(), reply); self.node.read_index(ctx); }
// in the Ready loop: for rs in ready.take_read_states() { reply rs.index to read_waiters[rs.request_ctx] }
// SampleLive: iterate conf().voters().ids() ∪ conf().learners
// RaftStatus += voters_outgoing: Vec<u64>, progress: Vec<PeerProgress{id, matched, pending_snapshot, recent_active}> (leader only)
```

### Gate check at a proposer
```rust
// crates/ostro-cluster/src/compat.rs (sketch)
pub fn require(view: &MetaView, f: Feature) -> Result<(), ApiError> {
    let fin = view.read(|s| cluster_version(s)).finalized;   // ClusterInfo.ext tag 98; absent = LEVEL_BASELINE
    if fin >= f.introduced_at() { Ok(()) }
    else { Err(ApiError::failed_precondition("feature_not_enabled",
           format!("{f:?} needs cluster level {} (finalized {fin}); finish and finalize the upgrade", f.introduced_at()))) }
}
```

### Fail-stop on unknown commands (partition apply)
```rust
// crates/ostro-partition/src/apply/mod.rs (sketch of the 13-02 change)
let Ok(cmd) = decode_command(cmd_bytes) else { return Ok(Rejected(Invalid(Decode))) };   // unchanged
if cmd.encoded_len() != cmd_bytes.len() { return Err(StoreError::NewerFormat("command carries fields unknown to this version")) }
let Some(cmd) = cmd.cmd else {
    if !cmd_bytes.is_empty() { return Err(StoreError::NewerFormat("unknown command tag")) }   // fail-stop the partition
    return Ok(Rejected(Invalid(Empty)));                                                    // genuinely empty: unchanged
};
```
Validate the re-encode identity assumption with a test over every `commands.txt` golden line and the format corpus logs (prost encodes canonically for the same schema; MEDIUM confidence until that test exists).

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Single-step Raft membership (one server at a time only) | Joint consensus (`ConfChangeV2`, `Auto` transition) + learners | etcd raft 3.4 / raft-rs 0.6+ | Multiple voter changes in one step; learners catch up without affecting quorum |
| Version = binary version; mixed clusters undefined | Finalized feature level gating new formats (Kafka KIP-584/KIP-778 `metadata.version`; CockroachDB cluster version with `preserve_downgrade_option`) | Kafka 3.3 KRaft; CockroachDB 19.x+ | Rolling upgrades with a defined downgrade window |
| Kafka partition reassignment through ZooKeeper znodes | `AlterPartitionReassignments` with adding/removing replicas in partition state (KIP-455) | Kafka 2.4 | The reassignment state lives with the partition record (Ostro: ext tag 100) and survives controller failover |
| Exact-version WIT import matching | Semver-compatible import/export lookup in Wasmtime component `Linker` | Wasmtime ~17+ (present in 50.0.2) | `0.1.N` additive evolution needs no per-version linkers |

**Deprecated/outdated:** docs/04 §1 "Cluster config & membership: `[[peers]]` TOML" as the membership source is superseded after first boot by meta Member records. Phase 7 `[workers] require_same_version = true` becomes the N/N−1 predicate in Phase 13.

## Open Questions

1. **Auto-finalize default.**
   - What we know: CockroachDB auto-finalizes unless held; Kafka requires an explicit feature update.
   - Unclear: the operator expectation for a "lean" product.
   - Recommendation: auto-finalize when all members report support and are live, with `ostro cluster upgrade hold` to keep the downgrade window open. A single node auto-finalizes on restart (there is no N−1 peer to protect). USER REVIEW.
2. **Leader-lease reads vs always-ReadIndex for partitions.**
   - What we know: the lease is safe under bounded rate drift (monotonic clocks) given the Phase 12 self-demotion.
   - Unclear: whether the Phase 14 Jepsen clock-skew nemesis will want a stricter mode.
   - Recommendation: lease by default; `[api] strict_reads = true` makes partition leaders confirm with a meta `read_index` plus an epoch check before serving. Measure the cost.
3. **Module GC protecting resumable terminal runs.**
   - Recommendation: protect them by default (`gc_protect_resumable = true`), because D1 retry resumes in place. With retention at 168 h, retired modules linger up to 7 days; document this.
4. **Real N−1 binary in CI.**
   - Recommendation: env-gated `OSTRO_PREV_BIN` test, human_needed until a release tag exists; the emulated-level suite runs on every commit.
5. **Phase 12 dependency details** (flag to the Phase 12 planner): the meta transport must be live in single-node mode (Pattern 3 step 3); bootstrap Member records for static peers (each node upserts its own); `SampleLive` over learners (or leave it to 13-03).
6. **Cluster-wide restore** stays deferred (12-RESEARCH OQ2).

## Proposed Plan Breakdown (12 plans, 6 waves)

| Plan | Wave | Depends | Scope | Reqs |
|------|------|---------|-------|------|
| **13-01** Contracts, reservations, harness API | 1 | Phase 12 done | RESERVATIONS P13 block (MetaCommand 8–11, Cmd 43–44, RejectReason 23–24, ext tags 96–111, names: `ostro.api.v1.Cluster`, `ostro.cluster.v1.Membership`, REST `/v1/cluster/*`, `/v1/runs/migrate`, `DELETE /v1/modules/{sha}`, CLI `cluster init/join/remove/drain/status/token/reassign/rebalance/upgrade`, `migrate-runs`, `modules rm/gc`, config `[cluster]` keys + `[modules]`, failpoints `member:: reassign:: route:: upgrade:: gc::`, metrics `ostro_cluster_ ostro_route_`, secret `__ostro/cluster-ca`, token prefix `njt_`, headers `ostro-hop ostro-partition ostro-partitions ostro-principal ostro-consistency ostro-read-after`); protos; `compat` module skeleton + gate registry; ext value structs; harness extensions (`join/remove/drain/upgrade(level)/client(node)`); module stubs pre-declared; **the only plan touching Cargo files** | all |
| **13-02** Compatibility gate + fail-stop | 2 | 01 | NodeVersion upsert, ClusterVersion read, start-time fence before `Node::open`, running fence, finalize/hold (`Put(Cluster)` CAS), proposer `require()`; partition fail-stop (unknown tag, re-encode length); `MetaSm` fail-stop on unknown variant; META_FORMAT-newer message; level emulation flag (failpoint builds only); `docs/compat.md` | CLUS-06 |
| **13-03** Vendored driver extensions | 2 | 01 | TransferLeader, ReadIndex, progress + `voters_outgoing` in `RaftStatus`, learners in `sample_live`, remedy text; driver tests: learner add/promote/remove, replace via joint, leave-joint after leader kill, read_index on follower; VENDORED.md rows | CLUS-05, API-08 |
| **13-04** Pure algorithms | 2 | 01 | `membership::reconcile`, `placement::{plan, reassign, leaders}`, route table; proptest-state-machine models (odd voters, quorum survival, ≤ 1 pending change, balance ±1, minimal moves, min_insync ≤ ISR) | CLUS-05 |
| **13-08** LinkerSet (ENG-10) | 2 | 01 | LinkerSet by major line, import inspection, `introduced_in` table, WitRequirement at upload + gate, cache key, single `WIT_0_1_LATEST` constant, `wit-next-fixture` 0.2 package, `dummy_module` fixtures | ENG-10 |
| **13-05** Membership service + promotion | 3 | 02, 03, 04 | Dynamic address book, join handshake (tokens, CSR, id allocation, joiner never InitCluster), reconciler driver on the meta leader, remove/drain/tombstone, `Membership.Removed`, `cluster init` promotion (node.toml binding, cluster CA secret, policy ext), admin API + CLI verbs | CLUS-05 |
| **13-06** Reassignment, handoff, balancing | 3 | 02, 04 | `AlterReplicas` apply (ostro-meta), reassignment controller, RF raise on join, graceful handoff, controlled shutdown, preferred-leader balancer, `cluster reassign/rebalance` | CLUS-05, CLUS-06 |
| **13-07** Request routing + RYW | 3 | 02, 03 | `RoutedControlPlane`, `PeerControlPlane` over the cluster listener, hop/NotLeader/retry, unkeyed local placement, list fan-out, meta ReadIndex barrier, partition lease check, consistency tokens, per-term `RunWatchers` re-subscribe; parity suite run through forwarding | API-08 |
| **13-09** Worker version compatibility | 4 | 02, 08 (+ Phase 7) | Hello level parsing/field, compatibility predicate (WitRequirement ⊆ worker lines, codec, level ≥ finalized − 1 window), placement filter, GoAway on member removal, worker sessions to every member | CLUS-06 |
| **13-10** Module GC + migrate-runs | 4 | 02, 07 | Retire, GC service with double scan, `RepinRun` Cmd 43 + apply + RejectReason 23/24, leader-local dry-run replay, `/v1/runs/migrate`, CLI | CLUS-07 |
| **13-11** Multi-process tier | 5 | 05, 06, 07, 09, 10 | Spawned-binary e2e: promotion under load, join/remove/force-remove, rolling upgrade with emulated level skew under load + chaos proxy (0 lost acked, all runs complete exactly once, none cancelled/failed by the upgrade), RYW checker across nodes through leader moves and failover, downgrade before/after finalize, controlled-shutdown timing, AOT pre-warm p99; `OSTRO_PREV_BIN` test (env-gated) | all |
| **13-12** Gate | 6 | all | Full suite + mutant matrix, docs (`docs/cluster-operations.md`, `docs/compat.md`, upgrade runbook, API RYW notes), RESERVATIONS flip, 13-VALIDATION, cluster campaign smoke (QA-01) | all |

File ownership per wave must not overlap: wave 2 splits ostro-meta (`sm.rs`, 13-02) from the vendored driver (13-03), `ostro-cluster` pure modules (13-04) and ostro-engine (13-08). In wave 3, 13-05 owns `membership/`, 13-06 owns `placement/` + `handoff.rs` + the `AlterReplicas` arm in `ostro-meta/src/state.rs`, and 13-07 owns `ostro-server/src/route/` + `control/watch.rs`.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (Rust 1.100), proptest 1.12 + proptest-state-machine 0.9, fail 0.5.1 failpoints; Phase 12 harness tiers A (pure), B (in-process cluster over in-memory tonic + fault layer), C (multi-process loopback `127.x.y.z` + chaos proxy, real `kill -9`) |
| Config file | none new; `scripts/ci-local.sh` (`FAILPOINT_CRATES` gains `ostro-cluster` if not already present), `.github/workflows/ci.yml` kept in sync |
| Quick run command | `cargo test -p ostro-cluster --lib --locked && cargo test -p ostro-raft-driver --locked && cargo test -p ostro-engine --test linker_set --locked` |
| Full suite command | `cargo test -p ostro-repl-conformance --features failpoints --locked -- --test-threads=1 && bash scripts/ci-local.sh` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CLUS-05 | Reconciler invariants: odd voters ≤ 5, ≤ 1 pending conf change, quorum survives every removal, joint always left | proptest model | `cargo test -p ostro-cluster --lib membership::reconcile --locked` | ❌ Wave 0 |
| CLUS-05 | Learner add → promote, replace via joint, leave-joint after leader kill, ReadIndex on follower, transfer leader | unit (driver Cluster harness) | `cargo test -p ostro-raft-driver --locked membership` | ❌ Wave 0 |
| CLUS-05 | Join/remove/force-remove; node ids never reused; joiner never self-bootstraps | integration (B) | `cargo test -p ostro-repl-conformance --test membership --locked` | ❌ Wave 0 |
| CLUS-05 | `ostro cluster init` promotes a running single node with runs in flight; RF raised to 3; kill -9 node 1 afterwards and every acked run completes | multi-process (C) | `cargo test -p ostro-repl-conformance --test process_promotion -- --ignored` | ❌ Wave 0 |
| CLUS-05 | Placement balance ±1, minimal moves, min_insync ≤ ISR at every step | proptest model | `cargo test -p ostro-cluster --lib placement --locked` | ❌ Wave 0 |
| CLUS-06 | Gate: gated features refused before finalize; finalize only when all members support; start fence refuses downgrade after finalize before opening partition files | unit + integration (B) | `cargo test -p ostro-cluster --lib compat --locked && cargo test -p ostro-repl-conformance --test version_gate --locked` | ❌ Wave 0 |
| CLUS-06 | Fail-stop: unknown Cmd tag / unknown fields / unknown MetaCommand stop the partition / voter (never Rejected-and-continue) | unit | `cargo test -p ostro-partition --test apply_newer_format --locked && cargo test -p ostro-meta --test newer_command --locked` | ❌ Wave 0 |
| CLUS-06 | Rolling upgrade under load with emulated level skew: 0 lost acked runs, every run completes exactly once, none drained/cancelled, standby identity holds throughout | multi-process (C) | `OSTRO_UPGRADE_ITERS=3 cargo test --release -p ostro-repl-conformance --features failpoints --test process_rolling_upgrade -- --ignored --test-threads=1` | ❌ Wave 0 |
| CLUS-06 | Graceful handoff: in-flight activations finish or replay on the new leader; no run fails | integration (B) | `cargo test -p ostro-repl-conformance --test handoff --locked` | ❌ Wave 0 |
| CLUS-06 | Workers receive only compatible activations (module WitRequirement vs Hello) | integration (Phase 7 worker harness) | `cargo test -p ostro-server --test worker_compat --locked` | ❌ Wave 0 |
| CLUS-06 | Real N−1 binary upgrade | multi-process, env-gated | `OSTRO_PREV_BIN=… cargo test -p ostro-repl-conformance --test upgrade_from_prev -- --ignored` | ❌ human_needed |
| CLUS-07 | Module GC never deletes a module with a non-terminal run; retired module refuses new starts; start-during-GC race | integration (B) + failpoint | `cargo test -p ostro-repl-conformance --features failpoints --test module_gc --locked -- --test-threads=1` | ❌ Wave 0 |
| CLUS-07 | migrate-runs: diverging journals never repinned; passing ones repinned; concurrent journal growth → SeqMismatch → re-dry-run; leased run → RunBusy | integration (server) | `cargo test -p ostro-server --test migrate_runs --locked` | ❌ Wave 0 |
| ENG-10 | 0.1.0 and 0.1.1 fixtures instantiate on the 0.1.N linker (imports and exports); 0.2.0 fixture selects the 0.2 linker; mixed lines rejected at upload; replay corpus green | unit (engine) | `cargo test -p ostro-engine --features wit-next-fixture --test linker_set --locked && cargo test -p ostro-engine --test replay_corpus --locked` | ❌ Wave 0 |
| ENG-10 | Upload of a module needing a newer level refused until finalize | integration (B) | `cargo test -p ostro-repl-conformance --test version_gate --locked upload_gate` | ❌ Wave 0 |
| API-08 | Every ControlPlane op through every node (REST and gRPC) gives the parity transcript; NotLeader hint retry; offline partition → 503/UNAVAILABLE | integration (B) | `cargo test -p ostro-repl-conformance --test routing --locked` | ❌ Wave 0 |
| API-08 | Read-your-writes across nodes through leader moves, failover and zombie isolation (session checker, with and without consistency tokens) | integration (B) + multi-process (C) | `cargo test -p ostro-repl-conformance --test ryw --locked && cargo test -p ostro-repl-conformance --test process_ryw -- --ignored` | ❌ Wave 0 |
| API-08 | `await_result` spanning a handoff returns the result before the deadline | integration (B) | `cargo test -p ostro-repl-conformance --test routing --locked await_across_handoff` | ❌ Wave 0 |

**Oracles (measured, never self-reported):**
- Phase 12 oracles (acked ⇒ on disk on ≥ min_insync nodes, replica identity, standby identity, LA rule, ISR-only elections).
- Membership: removed ids never reappear in ConfState or ISR; `voters_outgoing` empty within 5 s of any change; voter count always odd.
- Upgrade: no gated tag in any log or meta entry below the finalization index; no partition or voter ever applied `Rejected(Invalid(Empty))` for non-empty bytes.
- RYW: for each client session, every read reflects all of that session's acknowledged writes.
- GC: no executor ever reports `module_gone` for a run that was non-terminal when the module was retired.

### Sampling Rate
- **Per task commit:** the quick run command (< 30 s target).
- **Per wave merge:** the full suite command, including the mutant matrix and `ci-local.sh`.
- **Phase gate:** full suite green; rolling-upgrade, promotion and RYW tier-C reports recorded in 13-VALIDATION.md; cluster campaign smoke (QA-01) recorded. Native-Linux timings are human_needed (D15).

### Wave 0 Gaps
- [ ] Harness extensions in `ostro-repl-conformance` (`join`, `remove`, `drain`, `upgrade(level)`, `client(node)`, RYW session checker, level-emulation flag) (13-01).
- [ ] `crates/ostro-cluster/src/{membership,placement}/` pure modules + proptest models (13-04).
- [ ] Driver membership tests in `vendor/ostro-raft-driver` (13-03).
- [ ] `crates/ostro-engine/tests/linker_set.rs` + `wit-next-fixture` feature + dummy_module fixture builder (13-08).
- [ ] `crates/ostro-partition/tests/apply_newer_format.rs`, `crates/ostro-meta/tests/newer_command.rs` (13-02).
- [ ] Tier-C scenarios `process_promotion`, `process_rolling_upgrade`, `process_ryw` (13-11).

## Sources

### Primary (HIGH confidence)
- Repository code at `642a269`: `crates/ostro-meta/src/{records,command,sm,handle,state,ext,validate,lib}.rs`; `crates/ostro-partition/src/{apply/mod.rs,node.rs}`; `crates/ostro-types/src/routing.rs`, `ids.rs`; `crates/ostro-server/src/control/{runs.rs,watch.rs}`, `executor.rs`; `crates/ostro-engine/src/host/{state.rs:493-535,context.rs}`; `crates/ostro-api/src/domain.rs`; `wit/ostro-runtime-0.1.0/runtime.wit:100-116`.
- Vendored raft: `vendor/ostro-raft-driver/src/driver.rs` (ConfChangeV2 proposal/apply, blank-join bootstrap, SampleLive over voters, wiped-voter remedy text), `log_store.rs` (prost ConfState persistence); `vendor/ostro-raft-rs/src/{raw_node.rs (propose_conf_change, transfer_leader:753, read_index:764), raft.rs (auto_leave TODO:983, pending_conf_index), config.rs (ReadOnlyOption::Safe default)}`.
- Wasmtime 50.0.2 source: `src/runtime/component/linker.rs:26-58` (semver import lookup), `src/runtime/component/component.rs:1170` (semver export lookup test), `wasmtime-environ-50.0.2/src/component/names.rs` (`NameMap` alternate lookups keep the maximal version). wit-component 0.261.0 `src/dummy.rs` (`dummy_module`, enabled by `semver-check`).
- Planning: `.planning/{PROJECT,STATE,RESERVATIONS,REQUIREMENTS,ROADMAP}.md`, `docs/04-chronik-reuse.md`, `docs/storage-format.md` (freeze policy), `12-RESEARCH.md`, `05-20-PLAN.md` (FROZEN text task), `05-13-SUMMARY.md` (RunWatchers note), `07-RESEARCH.md`/`07-DECISIONS.md` (D-W4, D-W5, D-W16, D-W20, Hello/Welcome), `07-06-PLAN.md` (ClusterCa), `08-RESEARCH.md` Pattern 7 (blob GC), `docs/wit-0.1-freeze-review.md`.

### Secondary (MEDIUM confidence)
- Kafka KIP-778 (KRaft upgrades, `metadata.version` as a KIP-584 feature): https://cwiki.apache.org/confluence/display/KAFKA/KIP-778%3A+KRaft+to+KRaft+Upgrades
- Kafka KIP-1155 (metadata version downgrades; downgrade unsupported in current tooling): https://cwiki.apache.org/confluence/spaces/KAFKA/pages/347933361/KIP-1155+Metadata+Version+Downgrades
- CockroachDB upgrade/finalization with `cluster.preserve_downgrade_option`: https://www.cockroachlabs.com/docs/v24.2/upgrade-cockroach-version , https://www.cockroachlabs.com/docs/dev/upgrade-cockroach-version
- etcd learner design (promotion only when caught up and snapshot complete): https://etcd.io/docs/v3.3/learning/learner/ ; etcd 3.4 announcement: https://v1-33.docs.kubernetes.io/blog/2019/08/30/announcing-etcd-3-4
- Raft group reconfiguration with learners and joint consensus (Redpanda): https://docs.redpanda.com/current/manage/raft-group-reconfiguration/
- Kafka KIP-455 (replica reassignment with adding/removing replicas): https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment (from training knowledge, not re-fetched)

### Tertiary (LOW confidence; validate during execution)
- prost re-encode identity as an "unknown fields present" detector: assumed canonical for same-schema prost encoders. Validate with the golden/format-corpus test before relying on it (Code Examples).
- The lease-read margin (0.2 × session_timeout) against the Phase 12 timing defaults: derived, not measured.
- Raft-rs learner liveness via heartbeat responses in `last_heard`: inferred from the driver's Step handler; verify in 13-03.

## Metadata

**Confidence breakdown:**
- Pre-freeze findings: HIGH. Each is grounded in code lines cited above, and none requires a byte-format change.
- Standard stack: HIGH. No new crates; every raft and Wasmtime capability verified in vendored or registry source.
- Architecture: MEDIUM-HIGH. Membership and routing follow established designs (etcd learners, raft-rs joint consensus, KIP-455, Temporal-style frontend routing). The upgrade gate follows the KIP-584/778 and CockroachDB models adapted to Ostro's D18/D20 rules.
- Pitfalls: HIGH for the code-derived ones (1, 2, 3, 8, 10, 11, 12, 13); MEDIUM for 4, 9.

**Research date:** 2026-11-19
**Valid until:** 2026-12-19, or until 05-20, Phase 12 planning, or a Wasmtime/raft-rs bump changes the cited behaviour, whichever comes first.
