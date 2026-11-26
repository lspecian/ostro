# Mixed-version compatibility (Phase 13)

This document is the contract between Ostro binaries of different versions in one cluster, and
between a binary and data written by another version. It implements the frozen P13-F1 rules
(`docs/storage-format.md` §14.6, plan 05-20). Requirement: CLUS-06. Design records:
D-P13-3..7, D-P13-17, D-P13-18 and D-13-18-1. The operational steps are in
[upgrade-runbook.md](upgrade-runbook.md).

## 1. Feature levels

A **feature level** is a small integer naming a set of on-disk and on-wire novelties
(`ostro_types::compat`):

| constant | value | meaning |
|---|---|---|
| `LEVEL_MIN` | 1 | the frozen v0.1 formats plus the Phase 12 commands and ext tags |
| `LEVEL` | 2 | adds Phase 13's emitted novelties |

A binary supports `[LEVEL_MIN, LEVEL]`, which is one minor of downgrade window. The cluster has
one **finalized** level, stored in `ClusterInfo` ext 98 (`ClusterVersion`). Without tag 98,
finalized = `LEVEL_MIN`. A node never *writes* anything above the finalized level, even when
its binary could (rule 3). A fresh cluster, `ostro cluster init` and a single node finalize at
the binary's `level_max`.

### Gate registry

Every Phase 13 novelty is one entry of `enum Feature`, introduced at level 2:

| feature | wire/disk form | refused before level 2 |
|---|---|---|
| `alter_replicas` | MetaCommand 8 `AlterReplicas` | reassign, rebalance, drain moves |
| `repin_run` | partition `Cmd 43 RepinRun` | `migrate-runs` (submitting) |
| `reassignment_ext` | ext 100 on an assignment | (with `alter_replicas`) |
| `wit_requirement_ext` | ext 101 `WitRequirement` on a `Module` | upload of a module needing more than the baseline |
| `retired_ext` | ext 102 `Retired` on a `Module` / `WorkflowVersion` | `DELETE /v1/modules/{sha}` (retire) |

Every emitter calls `compat::require(feature, level_max)` against the finalized level. The check
runs at the proposer, never at apply (rule 5).

**Every gated refusal is HTTP 409** (`conflict`, gRPC `ALREADY_EXISTS`) with
`details.reason = "feature_not_enabled"` and `details.needed` / `details.finalized` (D-13-18-1).
This covers the module upload gate, migrate-runs, reassign, rebalance and module retire.
Earlier plan notes that mention 412 for reassign, rebalance or retire are obsolete. The upload
gate's message is `module needs cluster level N; finalize the upgrade first (the cluster is
finalized at level F)`, with `details.wit_line`.

### The machinery-tag exception (tags 96–99)

Four ext tags are the gate's own state. They are written at **every** level:

| tag | record | value |
|---|---|---|
| 96 `NodeVersion` | `Member` | binary version, `level_min`, `level_max`, WIT lines |
| 97 `MemberState` | `Member` | `joining` / `active` / `draining` / `removed` |
| 98 `ClusterVersion` | `ClusterInfo` | finalized level, hold |
| 99 `ReplicationPolicy` | `ClusterInfo` | `{rf, min_insync, max_voters}` |

This is safe because unknown ext tags are preserved on read-modify-write and ignored by readers,
and rule 5 forbids apply-time validation. A level-1 binary that meets them therefore cannot
diverge from a level-2 voter. Tags 100–102 are gated as listed above. Tags 103–111 are reserved
for Phase 13 and unused.

## 2. P13-F1 rules as implemented

1. **Levels** (§1): one registry, `introduced_at() = 2` for every Phase 13 feature. WIT
   functions carry their own level in the engine's `introduced_in` table (§5).
2. **Fail-stop on the unknown.** A binary never applies something it does not understand:
   - *Partition apply.* A decoded `Command` whose prost re-encoding is shorter than its bytes
     (fields unknown to this version), or a non-empty command with no known oneof tag, makes
     the partition halt. The state batch is dropped, `applied` does not advance, health goes to
     `Failed`, and later submits fail. The messages are `written by a newer Ostro; upgrade this
     binary: unknown command tag` / `...: command carries fields unknown to this version` /
     `...: command repin_run needs feature level 2 (this binary supports up to 1)`. Genuinely
     empty bytes are still `Rejected(Invalid(Empty))`. **Detection path:** re-encode identity
     (`decode(bytes).encoded_len() == bytes.len()`), proven over every `commands.txt` golden and
     every command of the v0.1 format corpus. The wire-walk fallback was not needed.
   - *Meta apply.* `MetaSm::apply` returns `FatalApply` for an undecodable non-empty entry or a
     command above `level_max`. The vendored driver stops before `applied` advances, and the
     voter halts. A newer `META_FORMAT` byte gets its own error:
     `metadata written by a newer Ostro (format N); upgrade this binary`.
3. **Write only at the finalized level.** Gated emitters check `compat::require`, so data
   stays readable by every binary that supports the finalized level. Downgrade is therefore
   possible until finalization.
4. **Finalize only when everyone can read it** (§3).
5. **No apply-time validation of levels or ext tags.** Every check is at the proposer. Apply
   is deterministic over the committed bytes, so mixed-version voters never diverge.
6. **Fences.** A binary refuses data finalized above its `level_max` before touching it (§4).

## 3. Finalization: `auto_finalize`, hold, finalize

- **Auto-finalize** (`[cluster] auto_finalize = true`, default). The meta leader's finalizer
  proposes the new `ClusterVersion` (CAS on the record version) when four conditions hold:
  every non-removed member reports `level_max ≥ target`, every member is live (the meta
  leader's `sample_live`, 3 s window), no hold is set, and the reported levels allow it. An
  unreported member counts as `LEVEL_MIN`. A freshly elected meta leader waits one liveness
  window before acting. A single node auto-finalizes on restart.
- **`ostro cluster upgrade status`** (`GET /v1/cluster/upgrade`) shows `finalized`, `hold`,
  `common_level_max`, `auto_finalize` and each member's `level_min` / `level_max`.
- **`ostro cluster upgrade hold`** sets `hold = finalized` and blocks both auto and manual
  finalization, which keeps the downgrade window open. **`release`** clears it. Both are
  idempotent and work from any node.
- **`ostro cluster upgrade finalize`** finalizes to the highest level every member supports.
  It runs on the meta leader (forwarded) and is refused while held.
- An undecodable tag 98 reads as `LEVEL_MIN` for gates and fences (the safe direction). The
  finalizer refuses to overwrite it.

User review (2026-11-26): auto-finalize by default, with `ostro cluster upgrade hold` to keep
the rollback window open, confirmed.

Measured (13-19, tier C): finalized 2 on every node 0.54–1.2 s after the last node of a rolling
upgrade restarted (p50 541 ms, n=3). The finalized level stayed 1 after every earlier restart.

## 4. Fences

- **Boot fence.** It runs after meta catch-up and **before `Node::open`**, so no partition
  file is touched. It refuses:
  - finalized > `level_max`: `data finalized by a newer Ostro (level 2, this binary supports up
    to 1); upgrade this binary`;
  - finalized < `LEVEL_MIN`: `data at level F is older than this binary supports (minimum 1);
    upgrade through a release supporting level F first`;
  - a newer `META_FORMAT` (the message above).

  The process exits with status 1. 13-19 `process_downgrade` checked this: 196 partition
  files, mtime and length unchanged.
- **Running fence.** It runs at start, after every applied meta change and after every Raft
  status change. It fail-stops a running node that sees finalized > its `level_max` (or a
  fatal meta apply): `running fence: data finalized by a newer Ostro (...); upgrade this
  binary; stopping`.
- **Joiners** check the finalized level from the join answer before writing anything.
- There is no unsafe-downgrade path in 1.x.

## 5. Modules and WIT lines (ENG-10)

- One `Linker` per WIT major line. Within a line, only the newest `0.1.N` is defined, and
  Wasmtime's semver name map resolves `@0.1.0` imports to it. Interface names derive from one
  `WIT_0_1_LATEST` constant (a CI grep gate keeps `journal@0.1.0"` literals out of the engine
  host).
- At upload, the WIT selection reads the component's imports. A module's minimum level is the
  maximum `introduced_in` over the `ostro:runtime` functions it imports. Mixing lines, an
  unsupported line or an unknown `ostro:runtime` function is 400.
- **Ext 101 only for non-baseline modules.** A module on `{line 0.1, level 1}` carries no ext.
  Any other module gets ext 101 `WitRequirement{line, min_level}`, after its own
  `compat::require(WitRequirementExt)`. If `min_level > finalized`, the upload gets the 409
  above. A re-upload of a recorded sha skips the gate.
- The `0.2` line is proven only by a test fixture (`ostro-engine` feature `wit-next-fixture`,
  run by CI). The shipped binary has no level-2 WIT line.

## 6. Workers: `[workers] version_policy`

A worker `Hello` carries `feature_level` (absent = a pre-Phase-13 worker: `ostro_version 0.1.z`
counts as level 1; `0.0.z` or unparsable is refused).

| policy | admission checks | GoAway reason on failure |
|---|---|---|
| `compatible` (default) | equal `proto_version` and `event_codec_ver`; worker level ≥ max(`LEVEL_MIN`, finalized − 1) | `version mismatch: ...` / `version too old: worker feature level L (ostro_version V), the cluster is finalized at level F and needs M or newer` |
| `exact` | the Phase 7 checks: proto, codec, and with `require_same_version` also `ostro_version` and `wasmtime_version` | `version mismatch: ...` |

- `version_policy = "compatible"` supersedes `require_same_version`, which now applies only
  under `exact`.
- After finalization, under `compatible`, sessions left below the floor are closed with
  `version too old: the cluster is finalized at level F (workers need level M or newer)`.
- While the node's member record is `draining` or `removed`, every session gets
  `GoAway{reason: "node_draining"}` and new sessions are refused.
- **Placement predicate.** An activation is offered to a worker only if all of these hold:
  the module's line ∈ the worker's WIT lines, worker level ≥ the module's `min_level`, the codec
  is equal, and the worker level ≥ finalized − 1. For a ready run, every module registered
  under the workflow must pass (strictest wins). A re-grant under the same lease checks the run's
  exact module. When no worker qualifies, `auto` runs embedded and `workers` waits.
- `Welcome.members` lists this node's endpoints plus every other active member's cluster address.

## 7. Level emulation for tests

The `ostro-types` feature `level-emulation` is enabled only through the failpoints chain. With
it, `OSTRO_TEST_LEVEL_MAX=k` makes a process behave as a level-k binary: it advertises
`level_max = k`, its gate and fences use k, and its partition and meta apply **fail-stop** on
commands of features above k, as an old binary would. Tier C uses it for rolling upgrades and
downgrades. Tier B sets a per-node `level_max_override` (gate, advertise and fences only).
Production builds ignore the variable.

## 8. Real N−1 status

The emulated suite runs on every commit: the `process_rolling_upgrade`, `process_downgrade`
and campaign tests in CI's tier-C step, and the tier-B `version_gate` scenarios. A real
previous-binary run (`upgrade_from_prev` with `OSTRO_PREV_BIN=<path>`) has a working hook, but
it is **human_needed until the first release tag exists**. Without the variable, the test
prints a SKIP line.

## 9. Mutants

Eight permanent failpoint mutants (`ostro_types::compat::MUTANTS`) prove the safety checks
bite. CI requires all eight to turn their scenarios red (`OSTRO_P13_REQUIRE_ALL=1`, step
"Phase 13 mutant matrix"): `upgrade::mutant::emit_ungated`, `upgrade::mutant::reject_and_continue`,
`member::mutant::skip_quorum_check`, `member::mutant::no_leave_joint`,
`reassign::mutant::raise_min_insync_early`, `route::mutant::serve_without_lease`,
`gc::mutant::single_scan`, `migrate::mutant::skip_seq_check`. `OSTRO_P13_MUTANT=<name>` runs
one row locally.
