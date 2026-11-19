---
phase: 05-workflow-semantics-core-hardening
plan: 07
subsystem: meta
tags: [ostro-meta, postcard, golden, ext-tlv, policies, webhook, schedule, tenancy, replication, D20, D23]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 gate: MetaKind/MetaRecord 0..13 verified against RESERVATIONS §10; D1-D22 in STATE.md"
  - phase: 04-single-node-server-rust-mvp
    provides: "04-01 ostro-meta (MetaState, validate-then-mutate CAS, [META_FORMAT] ‖ postcard encoding)"
provides:
  - "MetaRecord/MetaKind/MetaKey 14 WorkflowPolicy, 15 QueuePolicy, 16 Webhook; MetaState maps workflow_policies, queue_policies, webhooks (appended last)"
  - "QueuePolicy/WorkflowPolicy with the Phase 7 F2 flow/placement/pause/auto-pause fields; SpillPolicy, RateSpec"
  - "Schedule with the Phase 8 F2(a) fields (timezone, overlap, catchup, catchup_window_ms, start/end_at_ms, created_at_ms, ext)"
  - "Webhook record (Phase 8 F3)"
  - "ext TLV codec (crates/ostro-meta/src/ext.rs) and a trailing ext on all 17 records; apply rejects a malformed ext"
  - "Phase 12 F7 fields: PartitionAssignment.replicas/isr_version/min_insync, Member.cluster_addr/repl_addr; InitCluster defaults"
  - "Phase 14 F3 Tenant quotas, F4 WorkflowVersion.module (D23 grammar), Phase 7 F8 ApiToken.expires_at_ms"
  - "META_FORMAT 2 with DecodeError::PreFreezeFormat for format 1 (snapshot restore and replayed log entries both fail start)"
  - "tests/golden/meta_v2.txt: append-only meta encoding golden (17 records, 4 commands, empty + full snapshots)"
  - "STATE.md: D23, D20 amendment, Phase 14 F1-F4/F8/F10-F12 and Phase 12 F1-F3/F5-F9/F11 bullets"
affects: [05-11, 05-13, 05-14, 05-17, 05-20, 05-24, phase-6, phase-7, phase-8, phase-12, phase-14]

tech-stack:
  added: []
  patterns:
    - "Every meta record derives Default and ends with `ext: Vec<u8>`; literal fixes outside ostro-meta are `..Default::default()` or `ext: Vec::new()`"
    - "Apply-time shape validation lives in ostro-meta/src/validate.rs (validate-then-mutate, Invalid on failure); may be relaxed but never tightened after the freeze"
    - "Append-only golden: the test compares line-by-line, and OSTRO_BLESS=1 refuses to rewrite an existing line"
    - "Pre-freeze rejection via a shared AtomicU8 flag (MetaSm -> MetaView -> MetaHandle::start), because StateMachine::apply cannot fail"

key-files:
  created:
    - crates/ostro-meta/src/ext.rs
    - crates/ostro-meta/src/validate.rs
    - crates/ostro-meta/tests/meta_policies.rs
    - crates/ostro-meta/tests/meta_ext.rs
    - crates/ostro-meta/tests/meta_golden.rs
    - crates/ostro-meta/tests/golden/meta_v2.txt
  modified:
    - crates/ostro-meta/src/{command,records,state,sm,handle,lib}.rs
    - crates/ostro-meta/tests/{meta_records,meta_versions,meta_persist}.rs
    - crates/ostro-server/src/control/modules.rs
    - crates/ostro-server/tests/modules_api.rs
    - crates/ostro-server/tests/sdk_e2e.rs (only the two ActiveVersion literals)
    - .planning/STATE.md

key-decisions:
  - "META_FORMAT 1 -> 2 is the single pre-freeze bump. Format 1 is DecodeError::PreFreezeFormat ('pre-Phase-5 dev meta format; delete the dev data dir'); there is no decoder"
  - "A replayed format-1 Raft log entry sets MetaView::pre_freeze_format, and MetaHandle::start then fails with that message. Without this the entry would be dropped as Invalid and the node would boot on partial metadata"
  - "ext varints must be canonical (minimal LEB128, ≤ 5 bytes), so equal extension sets stay byte-identical across nodes"
  - "ext_put/ext_remove return Result<Vec<u8>, ExtError> (the plan sketched -> Vec<u8>): tag 0, a malformed input and values over u32::MAX are errors, never panics"
  - "SetPartitionLeader keeps its Phase 4 validation (12-RESEARCH F8) and carries replicas/isr_version/min_insync/ext over unchanged; isr_version is not bumped there (Phase 12 owns AlterIsr)"
  - "min_insync and the Phase 12 Member addresses are not validated (stored only; a Default literal has min_insync 0)"
  - "WorkflowVersion.module is checked against D23 in meta apply when non-empty: `<name>` or `<tenant>/<name>`, name ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$, tenant ^[a-z0-9][a-z0-9-]{0,62}$ (so `_/x` is rejected)"
  - "#[allow(clippy::large_enum_variant)] on MetaCommand: QueuePolicy makes Put large, and boxing would change the construction syntax for every caller"

patterns-established:
  - "Register a meta extension: allocate a tag in RESERVATIONS §10b, define a small postcard value struct, read with ext_get_value::<T>(rec.ext(), TAG), write with ext_put_value(rec.ext(), TAG, &v) in a CAS read-modify-write"

requirements-completed: [OPS-06, WF-09]

duration: 46min
completed: 2026-11-19
---

# Phase 5 Plan 07: Meta pre-freeze encoding Summary

**All pre-freeze meta changes land in one META_FORMAT 2 bump: run policies (Phase 7 F2 fields included), Webhook, the Phase 8 Schedule fields, the Phase 12 replica/ISR fields, Phase 14 tenant quotas and module names, and one TLV `ext` codec on every record. An append-only golden pins the encoding, and Phase 4 dev data fails with "delete the dev data dir".**

## Performance

- **Duration:** 46 min
- **Started:** 2026-11-19T06:19:22Z
- **Completed:** 2026-11-19T07:05:37Z
- **Tasks:** 3 of 3
- **Files modified:** 20 (6 created)

## Accomplishments

- **Task 1.** WorkflowPolicy=14, QueuePolicy=15 and Webhook=16 are appended in RESERVATIONS §10 order, with matching MetaState maps (also appended last) and typed accessors: `workflow_policy`, `queue_policy`, `webhook(s)`, `schedule(s)` and `tenant(s)`. Schedule carries the Phase 8 F2(a) fields. Apply validates shape and returns Invalid on failure:
  - overlap ≤ 4, catchup ≤ 2, timezone ≤ 64 bytes;
  - action ≤ 2, verifier ≤ 5;
  - placement ≤ 2, on_retries_exhausted ≤ 1, percentages ≤ 100, `RateSpec.period_ms` > 0;
  - selector ≤ 32 entries of ≤ 128 bytes.
- **Task 2.** One TLV codec (`ext.rs`) covers all 17 records, and apply rejects a malformed ext on any kind. Also added:
  - Phase 12 F7 fields, with InitCluster writing `replicas=[node]`, `isr_version=0`, `min_insync=1`;
  - Tenant quotas (every `Some` > 0);
  - `WorkflowVersion.module` (D23 grammar);
  - `ApiToken.expires_at_ms`.
  ostro-server got literal fixes only. STATE.md got D23, the D20 amendment and the Phase 14/12 findings.
- **Task 3.** META_FORMAT is now 2, with explicit rejection of format 1 in snapshots, commands and replayed log entries. `encode_snapshot`/`decode_snapshot` are public. The D20 evolution rules are documented in the sm.rs module docs. The golden file `meta_v2.txt` is checked by tests built on fixtures of the old shape.

## ext codec spec (RESERVATIONS §10b)

```text
ext   = entry*                      ; empty Vec = no extensions
entry = varint(tag: u32) ‖ varint(len: u32) ‖ value[len]
```
- **varint:** unsigned LEB128 (the postcard varint), at most 5 bytes, **canonical**. A multi-byte varint ending in a zero group, or one that overflows u32, is `ExtError::BadVarint`.
- **Tags:** strictly increasing (`NotIncreasing`); tag 0 is invalid (`ZeroTag`); a length past the end is `Truncated`.
- **value:** postcard of a small struct owned by the tag. The codec never interprets it.
- **Tag space:** one global space across all record kinds, allocated only from RESERVATIONS §10b:

  | Tags | Owner |
  |------|-------|
  | 1–14 | P5 |
  | 15 | test only (`EXT_TAG_TEST`) |
  | 16–31 | P6 |
  | 32–47 | P7 |
  | 48–63 | P8 |
  | 64–79 | P9 |
  | 80–111 | P12/P13 |
  | 112–143 | P14 |
- **API (`ostro_meta::`):**
  - `ext_validate(&[u8]) -> Result<(), ExtError>`
  - `ext_entries(&[u8]) -> Result<Vec<ExtEntry>, ExtError>`
  - `ext_get(&[u8], tag) -> Option<&[u8]>`
  - `ext_put(&[u8], tag, &[u8]) -> Result<Vec<u8>, ExtError>` inserts in order or replaces.
  - `ext_remove(&[u8], tag) -> Result<Vec<u8>, ExtError>`
  - `ext_get_value::<T>(&[u8], tag) -> Result<Option<T>, ExtError>`
  - `ext_put_value(&[u8], tag, &T) -> Result<Vec<u8>, ExtError>`
  - `MetaRecord::ext()` / `MetaRecord::ext_mut()`

  `ext_put` and `ext_remove` keep every other entry byte for byte, so unknown tags survive a read-modify-write.
- **Apply:** a Put whose record ext fails `ext_validate` → `MetaResult::Invalid("<Kind>: ext ...")`.

## Task Commits

1. **Task 1: records, kinds and maps (policies, Webhook, Schedule) with CAS tests:** `75ff202` (feat)
2. **Task 2: ext codec, Phase 12/14 fields, Tenant quotas, WorkflowVersion.module, STATE D23:** `a82df07` (feat)
3. **Task 3: META_FORMAT bump, pre-freeze rejection, meta golden:** `6e59a2e` (feat)

**Plan metadata:** see the final docs commit.

## Verification

- `cargo test -p ostro-meta --locked`: 34 tests across 7 suites, all green: meta_ext 11, meta_golden 5, meta_persist 2, meta_policies 7, meta_records 3, meta_versions 6, single_voter 1.
- `cargo clippy -p ostro-meta --all-targets --locked -- -D warnings`: clean.
- `cargo build --workspace --all-targets --locked`: warning-free in a detached scratch worktree at HEAD.
- `cargo test -p ostro-server --test bootstrap --test modules_api --test sdk_e2e --locked`: 5 + 3 + 9 green.
- `bash scripts/ci-local.sh --no-musl` ran in a detached worktree at 6e59a2e. Every step passed except "failpoint suites": `ostro-partition` `rebuild_digest::live_state_equals_rebuilt_state` failed with "node open timed out: Elapsed(())" while several wave-2 agents were building on the machine. Re-run alone (`cargo test -p ostro-partition --features failpoints --test rebuild_digest`) it passed 3/3. This is a **timing flake under load**; ostro-partition does not depend on ostro-meta. The full `cargo test` step (629 s), clippy, the build, fmt and cargo deny all passed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical] Replayed pre-freeze log entries now fail node start**
- **Found during:** Task 3
- **Issue:** `StateMachine::apply` cannot return an error. A format-1 Raft log entry (old dev data with no snapshot yet) would have been applied as `Invalid` and dropped, so the node would boot on partial metadata. That is the "garbage" outcome the plan forbids.
- **Fix:** `MetaSm` records the format byte in a shared flag (`MetaView::pre_freeze_format()`), and `MetaHandle::start` fails with the PreFreezeFormat message after catching up. Test: `meta_persist::pre_freeze_log_entry_fails_start`.
- **Files modified:** crates/ostro-meta/src/{sm,handle}.rs. This is within ostro-meta, which 05-07 owns; handle.rs was not in files_modified.
- **Commit:** 6e59a2e

**2. [Rule 3 - Blocking] clippy large_enum_variant on MetaCommand**
- **Found during:** Task 1
- **Issue:** QueuePolicy made `MetaCommand::Put` large, so `-D warnings` failed.
- **Fix:** Added `#[allow(clippy::large_enum_variant)]` with a justification. Boxing would change the construction syntax for every caller.
- **Commit:** 75ff202

**3. [Rule 3 - Blocking] Existing meta tests asserted format byte 2 was "unknown"**
- **Found during:** Task 3
- **Fix:** `meta_versions::encoding_round_trips_and_rejects_unknown_format` now asserts `UnknownFormat(META_FORMAT + 1)` and `PreFreezeFormat{found: 1}`. The proptest kind ranges in meta_records/meta_versions use `MetaKind::ALL.len()` (they were hard-coded to 14).
- **Commits:** 75ff202, 6e59a2e

**4. Additional files in ostro-meta:** `src/validate.rs`, plus edits to `tests/meta_records.rs` and `src/handle.rs`. These are not in files_modified but are inside ostro-meta, which this plan owns.

**5. API shape:** `ext_put` returns `Result<Vec<u8>, ExtError>` (the plan sketched `-> Vec<u8>`). `ext_remove`, `ext_entries`, `ext_get_value` and `ext_put_value` were added.

### Process incident (no code impact)

- Commit `95d7d8a` (05-03) was made from the shared git index while it still held the pre-`a82df07` content of the 05-07 paths, which silently reverted Task 2. The 05-03 agent then rewrote its commits (`5773c11`, `e4cd2ea`), and HEAD contains a82df07's content unchanged; I verified the tree for every 05-07 path. Cause: my private-index commit was followed by a separate `git reset -- <paths>` about a minute later. Task 3 used `git commit -o <paths>`, which commits and resyncs the shared index in one step. Recommended for every parallel executor.

## Hand-off notes

**05-11 (policy resolver, D9)**
- Read through `MetaView::read(|s| s.workflow_policy(name))` / `s.queue_policy(name)`. Keys are the policy's `workflow` / `queue` string, which is the D23 qualified form, the same string as `StartRun.workflow`/queue.
- Resolution order: request > WorkflowPolicy > QueuePolicy > `[runs]` config. Only `retention_ms` and `schedule_to_start_ms` are Phase 5 semantics; ignore the Phase 7 fields.
- CAS rules are the standard ones: create with `expected_version: None`, update with the exact version. A Put with out-of-range enum fields is `Invalid`.

**05-13 / 05-24 (upload name, D23)**
- Set `WorkflowVersion.module` to the tenant-qualified upload name (`<name>` for the default tenant, else `<tenant>/<name>`). Today `control/modules.rs` writes `module: String::new()` with a `// 05-24 sets ...` comment.
- Meta apply rejects a non-empty module that breaks the D23 grammar, so validate at the API first to return a clean 400.
- The grammar helpers (`is_resource_name`, `is_tenant_id`, `is_qualified_name`) are `pub(crate)` in ostro-meta. Re-implement them in the API layer or make them `pub` (05-24 may edit ostro-meta for that). Keep them identical to D23.

**05-20 (freeze)**
- Pin `crates/ostro-meta/tests/golden/meta_v2.txt` as a frozen prefix, and flip RESERVATIONS §10 rows 14–16 and the `Schedule` row to `exists`.
- Record "META_FORMAT = 2 (05-07)" as the frozen baseline in §10.

**Phase 6 (secrets, grants, connections, egress)**
- Use the existing Secret/Grant/Connection/Egress records. New options go in ext tags 16–31, with a postcard value struct per tag; never add struct fields.

**Phase 7 (flow control, placement, pause)**
- All F2 fields already exist on QueuePolicy/WorkflowPolicy, with ranges validated. Enforcement is Phase 7's.
- `ApiToken.expires_at_ms` is stored. The expiry check belongs to the token verifier.
- Extra knobs use ext tags 32–47. Meta variants 19–20 are not needed.

**Phase 8 (schedules, webhooks)**
- Schedule and Webhook have their full F2(a)/F3 shapes. `MetaState::schedules()`/`webhooks()` iterate in name order.
- IANA, cron, JSON-pointer and verifier-config semantics are Phase 8's; meta checks only ranges and the 64-byte timezone cap.
- Phase 8 options use ext tags 48–63.

**Phase 12**
- `replicas`, `isr_version` and `min_insync` are stored and carried through SetPartitionLeader untouched. AlterIsr (MetaCommand 5, §10a) should CAS on `isr_version`. Do not tighten SetPartitionLeader validation.

**Phase 14**
- Tenant quotas are stored, with `Some` > 0 enforced at apply. Further knobs use ext tags 112–143.

**Everyone**
- Meta literals outside ostro-meta must end with `..Default::default()`.
- New meta shapes after 05-20 follow D20:
  - a registered ext tag (no bump);
  - a new variant plus a META_FORMAT bump with decoders for every older format;
  - or an appended MetaCommand variant (no bump).
- Re-run `cargo test -p ostro-server --test sdk_e2e` after 05-06 lands, because both plans touch sdk_e2e.rs.

## Self-Check: PASSED

- All 6 created files are present in HEAD: ext.rs, validate.rs, meta_policies.rs, meta_ext.rs, meta_golden.rs, golden/meta_v2.txt.
- Commits 75ff202, a82df07 and 6e59a2e are ancestors of HEAD.
- The content of a82df07 is intact in HEAD after the concurrent 05-03 rewrite.
