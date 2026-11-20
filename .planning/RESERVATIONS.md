# Ordinal & Name Reservations (single registry)

**Purpose.** Phases 6, 7 and 8 run in parallel after the Phase 5 freeze (05-20). All three append to the same ordinal-encoded spaces: prost tags, postcard enum/variant indices, digest order and outcome codes. Parallel appends without a registry would collide (Phase 8 research F4, Phase 6 research §Ordinal Reservations).

**Rules (binding for every phase plan from Phase 5 on):**
1. A plan may allocate an ordinal or reserved name **only from this file**, and only from its own phase's block.
2. If a phase needs more than its block, it first edits this file in a separate, docs-only commit that takes the next **unallocated** range, then writes code. Never borrow another phase's block.
3. Once an entry has shipped in code it is never reused or renumbered. A removed item keeps its number, marked `retired`.
4. Each phase's gate plan (Phase 5: 05-01 and 05-20) verifies the code against this file. Every ordinal present in code must appear here as `exists`, or as allocated to the phase that added it.
5. Status values:
   - `exists`: in code today.
   - `planned` followed by the phase tag and owning plan (e.g. for Phase 5: "planned", then "P5", then the plan id): allocated to that phase's plan. 05-20 flips its rows to `exists`. **Done 2026-11-19 (05-20): every Phase 5 row was verified against the code at the freeze and now reads `exists (P5, <plan>)`.**
   - `reserved P6/P7/P8`: block held for that phase.
   - `unallocated`: free for a later phase to claim per rule 2.

Sources:
- Code at commit `b276dc5`: `crates/ostro-types/src/{command,journal}.rs`, `crates/ostro-partition/src/{store/mod.rs,apply/outcome.rs}`, `crates/ostro-meta/src/{command,state}.rs`.
- Entries marked "per 04-12 plan" landed in `c5f300c` while 04-12 was still executing. 05-01 re-verifies them.

---

## 1. Partition `Cmd` oneof tags (`ostro_types::command::Cmd`, prost tag)

| Tag | Item | Status |
|-----|------|--------|
| 1 | LeaderAnnounced | exists (P2, frozen) |
| 2 | StartRun | exists (P2, frozen) |
| 3 | AttemptGranted | exists (P2, frozen) |
| 4 | EventsAppended | exists (P2, frozen) |
| 5 | TimerFired | exists (P2, frozen) |
| 6 | InboundBatch | exists (P2, frozen) |
| 7 | OutboxTruncated | exists (P2, frozen) |
| 8 | SignalRun | exists (P5, 05-01) |
| 9 | RequestCancel | exists (P5, 05-01) |
| 10 | TerminateRun | exists (P5, 05-01) |
| 11 | RetryRun | exists (P5, 05-01) |
| 12 | CompleteToken | exists (P5, 05-01) |
| 13 | CompleteEffect | exists (P6, 06-02) |
| 14–19 | spare | reserved P6 |
| 20–26 | (unspecified) | reserved P7 |
| 27 | ScheduleFire | exists (P8, 08-01) |
| 28–33 | spare | reserved P8 |
| 34–40 | — (P9 fallback only if PF-F1/F2 had not landed pre-freeze) | reserved P9 |
| 41–42 | — (contingency, expected use 0) | reserved P12 |
| 43 | RepinRun (`ostro migrate-runs`) | reserved P13 |
| 44 | spare | reserved P13 |
| 45 | DlqUpdate (research proposed 43, which is taken by P13 RepinRun) | reserved P11 |
| 46–49 | spare | reserved P11 |
| 50–63 | — | unallocated |

## 2. `StartRun` field numbers (prost)

| Fields | Items | Status |
|--------|-------|--------|
| 1–13 | run_id … rng_seed | exists (P2) |
| 14–18 | module_sha, tags, tenant, traceparent, run_timeout_ms | exists (P4, 04-09) |
| 19 | schedule_to_start_ms | exists (P5, 05-01) |
| 20 | retention_ms | exists (P5, 05-01) |
| 21 | retry_of | exists (P5, 05-01) |
| 22 | continued_from | exists (P5, 05-01) |
| 23 | run_started (optional JournalEvent) | exists (P5, 05-01; apply 05-16, set by the API 05-25, D24; in the format corpus) |
| 24–27 | — (P6 declared 0 needs) | reserved P6 |
| 28–31 | — | reserved P7 |
| 32–35 | — (P8 declared 0 needs) | reserved P8 |
| 36–39 | — | reserved P9 |
| 40 | rerun_prefix (repeated JournalEvent) | reserved P11 |
| 41 | rerun_from_seq (uint64) | reserved P11 |
| 42–43 | spare | reserved P11 |
| 44+ | — | unallocated |

Other message fields: each new field on any other message is owned by the plan that adds it, within its phase. Recorded allocations:

| Message.field | Item | Status |
|---|---|---|
| CompleteRun.4 | resumable | exists (P5, 05-01) |
| CompleteRun.5 | parent_event (submitter-encoded ChildCompleted event bytes, D21) | exists (P5, 05-01), Phase 9 PF-F1 |
| outbox ChildCompleted.6 | event (same bytes, forwarded verbatim) | exists (P5, 05-01), Phase 9 PF-F1 |
| AttemptGranted.3 | after_failure (durable attempt_failures, F9) | exists (P5, 05-01) |
| RegisterWait.5 | at_ms (D21) | exists (P5, 05-01) |
| JournalEvent.4 | attempt (fixed64, submitter-set; 0 = apply-originated; PF-11-5 apply stamping was NOT adopted) | reserved P11 |

State value fields (positional postcard, pre-freeze only; listed for cross-phase visibility): RunRecord gains the Phase 5 fields in 05-02, plus `continued_to` (Phase 9 PF-F2), `attempt_failures`, `status_seq` and `pause` (Phase 7 F1). Parallel phases must not add fields to the same message without first recording the field number here.

## 3. `Derived` intent oneof tags (`derived::Intent`, prost tag, inside EventsAppended)

*This space was missing from Phase 8's F4 list. Phase 6 flagged it.*

| Tag | Item | Status |
|-----|------|--------|
| 1 | ScheduleTimer | exists (P2) |
| 2 | CancelTimer | exists (P2) |
| 3 | SendMessage | exists (P2) |
| 4 | EnqueueReady | exists (P2) |
| 5 | CompleteRun | exists (P2) |
| 6 | Suspend | exists (P4, 04-09; per 04-12 plan) |
| 7 | RegisterWait | exists (P5, 05-01) |
| 8 | CancelWait | exists (P5, 05-01) |
| 9 | RegisterEffect `{op, kind u8, scheduled_at_ms}` | exists (P6, 06-02) |
| 10 | CancelEffect `{op}` | exists (P6, 06-02) |
| 11–12 | spare | reserved P6 |
| 13–16 | — | reserved P7 |
| 17–18 | — (P8 declared 0 needs) | reserved P8 |
| 19–22 | — (P9 fallback intent slot) | reserved P9 |
| 23+ | — | unallocated |

## 4. Journal `EventKind` codes (`ostro_types::journal`)

Rules:
- `EventBody` variants are appended LAST.
- No `EVENT_CODEC_VER` bump for a new kind.
- Code ranges: 1–15 run record, 16–63 guest commands, 64–127 inputs, 128+ reserved.

| Code | Item | Status |
|------|------|--------|
| 1 | RunStarted | exists (P3) |
| 2–15 | — (run-record range) | unallocated |
| 16–28 | StepCompleted … RunContinuedAsNew | exists (P3) |
| 29 | StepAttemptFailed | exists (P4, 04-04) |
| 30–63 | — (guest-command range) | unallocated |
| 64–69 | TimerFired, EffectCompleted, SignalReceived, ChildCompleted, TokenCompleted, CancelRequested | exists (P3). From P5 on, SignalReceived (66) stays decodable but is never produced (D21) |
| 70 | WaitTimedOut | exists (P5, 05-01) |
| 71 | RunTerminated | exists (P5, 05-01) |
| 72–79 | — (P6 declared 0 needs) | reserved P6 |
| 80–87 | — | reserved P7 |
| 88–95 | — (P8 declared 0 needs) | reserved P8 |
| 96 | SignalPayload `{name, payload}`: submitter-encoded, stored verbatim (D21) | exists (P5, 05-01; allocated under rule 2 from the unallocated range) |
| 97 | SignalMatched `{op, payload_seq}`: apply-encoded, no user data (D21) | exists (P5, 05-01; rule 2) |
| 98–105 | — | reserved P9 |
| 106–127 | — | unallocated |
| 128+ | — | reserved (codec) |

## 5. `TerminalStatus` enum values (prost) — partition `RunStatus` mirrors are appended in the same order

| Value | Item | Status |
|-------|------|--------|
| 0–2 | Completed, Failed, Cancelled | exists (P2) |
| 3 | Terminated | exists (P5, 05-01) |
| 4 | TimedOut | exists (P5, 05-01) |
| 5 | ContinuedAsNew | exists (P5, 05-01) |
| 6–9 | — (P6, P8 declared 0) | reserved P7 |
| 10–11 | — | reserved P9 |
| 12+ | — | unallocated |

## 6. State keyspaces (`Table` / `TABLES`): declaration order = digest order; the on-disk keyspace name is fixed here

Rules:
- **Placeholder rule.** A phase that lands a keyspace whose ordinal is above a reserved slot that does not exist yet must also declare that lower slot, as an empty keyspace with the **name fixed below**. This keeps digest order identical whatever order the phases land in. Empty keyspaces contribute only `name + row_count 0` to the digest.
- The format corpus records **per-table** digests (05-12), so adding empty keyspaces later never invalidates it.

| Ordinal | Name (on disk) | Status |
|---------|----------------|--------|
| 0 | meta | exists (P2) |
| 1 | runs | exists (P2) |
| 2 | events | exists (P2) |
| 3 | payloads | exists (P2) |
| 4 | ready | exists (P2) |
| 5 | timers | exists (P2) |
| 6 | inbox | exists (P2; re-keyed for signals by P5, 05-02) |
| 7 | dedupe | exists (P2) |
| 8 | outbox | exists (P2) |
| 9 | inbound_seen | exists (P2) |
| 10 | children | exists (P2) |
| 11 | dlq | exists (P2) |
| 12 | effects | exists (P2, no rows). **P6 writes rows** (`run ‖ op BE → EffectRow{kind, scheduled_at_ms}`). 0 new keyspaces for P6. |
| 13 | running | exists (P4; per 04-12 plan) |
| 14 | waits | exists (P5, 05-02) |
| 15 | signal_seen | exists (P5, 05-02) |
| 16 | schedule_state | exists (P8, 08-01; `Table::ScheduleState`, rows from 08-07) |
| 17 | p7_a | reserved P7. **Not needed if 07-RESEARCH F1 lands** (05-02 adds `RunRecord.pause`). If F1 is rejected, rename to `run_pauses` in a docs-only commit before any placeholder is declared. |
| 18 | p7_b | reserved P7. Not needed if F1 lands. |
| 19 | p9_a | reserved P9 (placeholder rule applies) |
| (none) | `x_*` derived keyspaces (PF-11-2): outside TABLES, the digest, OSNP and blob GC; rebuilt from digest state. Reserved name: `x_run_index` (P11) | rule frozen in 05-20 |
| 20+ | — | unallocated |

## 7. Apply outcome `RejectReason` (outcome code = 16 + index; folded into the state hash)

| Index | Item | Status |
|-------|------|--------|
| 0–10 | StaleEpoch, Fenced, SeqMismatch, SeqGap, RunNotFound, RunTerminal, RunExists, IdempotencyConflict, PayloadTooLarge, WrongPartition, Invalid | exists (P2) |
| 11 | RunCompleted | exists (P5, 05-02) |
| 12–14 | — (P6 declared 0 needs) | reserved P6 |
| 15–17 | — | reserved P7 |
| 18–19 | — (P8 declared 0) | reserved P8 |
| 20–22 | — | reserved P9 |
| 23–24 | — (Phase 13 repin / version-gate rejects; names chosen by 13-01) | reserved P13 |
| 25+ | — | unallocated |

## 8. `InvalidCode` (declaration index; reported in Rejected(Invalid) detail)

| Index | Item | Status |
|-------|------|--------|
| 0–10 | Decode, Empty, BadRunId, MissingToken, EventsNotContiguous, MissingField, NotImplemented, BadIntent, FieldTooLong, BadSource, TruncateBeyondEnd | exists (P2) |
| 11 | BadLength | exists (P4; per 04-12 plan) |
| 12 | NotRunning | exists (P4; per 04-12 plan) |
| 13–14 | spare (P5 plans currently reuse BadIntent / NotImplemented) | reserved P5 |
| 15 | BadEffectCompletion | exists (P6, 06-02) |
| 16–17 | spare | reserved P6 |
| 18–21 | — | reserved P7 |
| 22 | BadScheduleFire (malformed `ScheduleFire`: kind/start/request_id mismatch; name chosen by 08-01) | exists (P8, 08-01) |
| 23 | ScheduleOwnerMismatch (`ScheduleFire` applied on a partition that is not `schedule_owner(name, P)`; name chosen by 08-01) | exists (P8, 08-01) |
| 24 | spare | reserved P8 |
| 25–28 | — (P9 spare, incl. fallback) | reserved P9 |
| 29 | BadRerunPrefix | reserved P11 |
| 30 | BadDlqUpdate | reserved P11 |
| 31 | spare | reserved P11 |
| 32+ | — | unallocated |

**Placeholder rule (P-15, 06-01).** `InvalidCode` is ordinal by declaration index. A phase that lands a code above an index that does not exist yet must also declare every lower missing index as a `#[doc(hidden)]` placeholder variant that apply never emits, using the **canonical name `Reserved<Phase>_<index>`** below. Phases 6, 7 and 8 therefore produce identical declarations whatever order they land in. A phase claiming its own slot renames its placeholder in place (same index). Placeholders are never emitted, so they never reach the state hash.

| Index | Canonical placeholder name | Owner block |
|-------|----------------------------|-------------|
| 13 | `ReservedP5_13` | P5 spare |
| 14 | `ReservedP5_14` | P5 spare |
| 15 | `ReservedP6_15` (renamed in place to `BadEffectCompletion` by 06-02) | P6 |
| 16 | `ReservedP6_16` | P6 |
| 17 | `ReservedP6_17` | P6 |
| 18 | `ReservedP7_18` | P7 |
| 19 | `ReservedP7_19` | P7 |
| 20 | `ReservedP7_20` | P7 |
| 21 | `ReservedP7_21` | P7 |
| 22 | `ReservedP8_22`, renamed in place to `BadScheduleFire` by 08-01 | P8 |
| 23 | `ReservedP8_23`, renamed in place to `ScheduleOwnerMismatch` by 08-01 | P8 |
| 24 | `ReservedP8_24` | P8 |

Indices 25+ follow the same pattern with their owner block (e.g. `ReservedP9_25`, `ReservedP11_31`).

## 9. Outbox `outbox_msg::Kind` tags (prost)

| Tag | Item | Status |
|-----|------|--------|
| 1–5 | StartChild, ChildCompleted, Signal, CancelRun, CronStart (CronStart stays valid but unused by P8) | exists (P2) |
| 6 | StartRun (continue-as-new) | exists (P5, 05-01) |
| 7 | — (P6 declared 0) | reserved P6 |
| 8–9 | — | reserved P7 |
| 10 | — (P8 declared 0) | reserved P8 |
| 11–12 | — (11 = P9 fallback outbox kind if PF-F1 had not landed) | reserved P9 |
| 13+ | — | unallocated |

## 10. Meta: `MetaRecord` / `MetaKind` variant index and `MetaState` map order (postcard, positional)

Rules (D20):
- Before the 05-20 freeze, Phase 5 lands everything known (05-07).
- **After the freeze, every meta change is a new variant appended here, plus a META_FORMAT bump with decoders for every older format.**

| Index | Variant (MetaState map) | Status |
|-------|------------------------|--------|
| 0–13 | Cluster, Member, Partition, Module, Workflow, Active, Queue, Schedule, Token, Tenant, Secret, Grant, Connection, Egress | exists (P4, 04-01) |
| — | `Schedule` gains timezone, overlap, catchup, catchup_window_ms, start_at_ms, end_at_ms, created_at_ms, ext | exists (P5, 05-07), pre-freeze, for P8 F2(a) |
| 14 | WorkflowPolicy (`workflow_policies`) | exists (P5, 05-07) |
| 15 | QueuePolicy (`queue_policies`) | exists (P5, 05-07) |
| 16 | Webhook (`webhooks`) | exists (P5, 05-07), pre-freeze, for P8 F3 |
| 17–18 | — (P6 uses existing Secret/Grant/Connection/Egress by convention: 0 needed) | reserved P6 |
| 19–20 | — (QueueFlow / WorkflowFlow only if F2 is rejected) | reserved P7. **Not needed if 07-RESEARCH F2 lands** (05-07 puts the Phase 7 fields in QueuePolicy/WorkflowPolicy pre-freeze) |
| 21 | — (P8 needs 0 post-freeze; Webhook landed in P5) | reserved P8 |
| 22–23 | — | reserved P9 |
| 24–25 | — (fallback only if Phase 14 F2/F3/F4 had not landed pre-freeze) | reserved P14 |
| 26 | — (PartitionReplication fallback only if Phase 12 F7 had not landed pre-freeze) | reserved P12 |
| 27+ | — | unallocated |

META_FORMAT: bumped once pre-freeze by 05-07. **Frozen baseline: `META_FORMAT = 2`** (verified by 05-20 at the freeze, 2026-11-19; `crates/ostro-meta/src/command.rs`, golden `crates/ostro-meta/tests/golden/meta_v2.txt` pinned in `golden_frozen.rs`; format 1 = pre-Phase-5 dev format, rejected). Each post-freeze bump is recorded here with its phase.

### 10a. `MetaCommand` variant order (postcard, Raft entries)

Appending a `MetaCommand` variant changes no existing encoding and needs **no META_FORMAT bump** (Phase 12 F8, D20 clarified). Mixed-version proposal gating is Phase 13's concern. Existing command validation, such as `SetPartitionLeader`, is never tightened after the freeze.

| Index | Variant | Status |
|-------|---------|--------|
| 0 | InitCluster | exists (P4) |
| 1 | Put | exists (P4) |
| 2 | Delete | exists (P4) |
| 3 | SetPartitionLeader | exists (P4) |
| 4–7 | ElectLeader, AlterIsr, ResignLeader, spare | reserved P12 |
| 8 | AlterReplicas | reserved P13 |
| 9–11 | spare | reserved P13 |
| 12+ | — | unallocated |

Phase 12 holds no keyspaces, journal kinds, RejectReason/InvalidCode values or outbox kinds.

## 10b. Meta `ext` TLV tags (Phase 14 F2; amended D20)

Every meta record ends with `ext: Vec<u8>` in one codec: `varint(tag u32) ‖ varint(len u32) ‖ postcard(value)`. Tags are strictly increasing, tag 0 is invalid, and unknown tags are preserved on read-modify-write and ignored by readers.
- Tags form **one global space across all record kinds**. Each tag is registered here with the record(s) it applies to and its value struct.
- Registering a tag is a **post-freeze-safe extension**: no new variant and no META_FORMAT bump.

| Tags | Owner | Registered |
|------|-------|-----------|
| 0 | invalid | — |
| 1–14 | reserved P5 (none used yet) | — |
| 15 | **golden/test only**: used by the 05-07 meta golden fixtures and codec tests; never read by production code, never assigned to a feature | P5 (05-07), permanent |
| 16–31 | reserved P6 | — |
| 32–47 | reserved P7 | — |
| 48–63 | reserved P8 | — |
| 64–79 | reserved P9 | — |
| 80–95 | reserved P12 (Member, PartitionAssignment, cluster) | — |
| 96 | `Member` NodeVersion `{ostro_version, level_min, level_max, wasmtime, event_codec_ver, wit_lines, reported_at_ms}` | reserved P13 |
| 97 | `Member` MemberState `{state: Joining/Active/Draining/Removed, changed_at_ms}` (tombstone; node ids never reused) | reserved P13 |
| 98 | `ClusterInfo` ClusterVersion `{finalized, hold: Option<u32>, finalized_at_ms}` (the 1.0 binary must already read it; absent = baseline level) | reserved P13 |
| 99 | `ClusterInfo` ReplicationPolicy `{rf, min_insync, max_voters}` | reserved P13 |
| 100 | `PartitionAssignment` Reassignment `{target, adding, removing, started_at_ms}` (KIP-455 shape) | reserved P13 |
| 101 | `ModuleRecord` WitRequirement `{line, min_level}` | reserved P13 |
| 102 | `ModuleRecord` / `WorkflowVersion` Retired `{retired_at_ms}` | reserved P13 |
| 103–111 | spare | reserved P13 |
| 112–143 | reserved P14 (ApiToken name/description/created_by/rotation link, ModuleRecord signature evidence + uploader tenants, ClusterInfo security policy, SealedSecret created/rotated, further Tenant knobs) | — |
| 144 | `WorkflowPolicy` / `QueuePolicy` OnFailure | reserved P11 |
| 145 | DlqPolicy `{enabled, hook_catchup_ms}` | reserved P11 |
| 146–159 | spare | reserved P11 |
| 160+ | unallocated | — |

---

## 11. Name-only reservations (collide by name, not ordinal)

| Kind | Name | Owner |
|------|------|-------|
| Config section | `[runs]` | P5 (05-11) |
| Config sections | `[secrets]`, `[egress]`, `[connectors]` | P6 |
| Failpoint prefixes | `meta::`, `aot::`, `executor::`, `dispatcher::` (new); `wal::`, `state::`, `blob::` (exist) | P5 (05-18) / P2 |
| Failpoint prefix | `bridge::` | P6 |
| REST paths | `/v1/runs/{id}/signals/{name}`, `/cancel`, `/terminate`, `/retry`, `/result`; `/v1/tokens/complete`; `/v1/workflows/{name}/policy`; `/v1/queues/{name}/policy` | P5 (05-04/05-13) |
| REST prefixes | `/v1/secrets`, `/v1/grants`, `/v1/connections`, `/v1/egress`, `/v1/objects/{connection}/{key}` | P6 |
| REST path | `POST /hooks/{name}` (+ webhook CRUD under `/v1/webhooks`) | P8 |
| gRPC service | `ostro.api.v1.Tokens` | P5 (05-04) |
| Sealed-secret prefix | `__ostro/` (exists: `__ostro/token-key`; P6 adds `__ostro/presign/<connection>`) | P4 / P6 |
| Grant id prefix | `secret:` | P6 |
| Dedupe key namespace | `__cron/<schedule>` | P8 |
| Dedupe / idempotency namespace | `__hook/<name>/<key>` (webhook deliveries, 08-15) | P8 |
| Config sections | `[telemetry]`, `[snapshots]`, `[backup]`, `[blob_gc]`, `[triggers]`, `[guest_logs]` (P8-D22; parsed by 08-04) | P8 |
| Failpoint prefixes | `snapshot::`, `trim::`, `upload::`, `restore::`, `blobgc::`, `cron::` | P8 |
| REST paths | `GET /metrics`; `/v1/schedules` (+ `/v1/schedules/{name}/pause`, `/resume`, `/trigger`); `GET /v1/runs/{id}/logs`; `GET /v1/admin/backup` | P8 (08-10) |
| gRPC services | `ostro.api.v1.Schedules`, `ostro.api.v1.Webhooks` | P8 (08-10) |
| gRPC service + RPC | `ostro.api.v1.RunLogs` / `GetRunLogs` (in `schedules.proto`). 08-10 did not append to `Runs`: runs.proto stays untouched so the Phase 8 surface lives in its own proto files and 06-08 shares no proto edit. Hook ingress and the admin backup are REST-only | P8 (08-10) |
| node.toml keys | `incarnation` (08-08; additive, F6), `restored_from` (08-18) | P8 |
| Object-store prefixes (bottomless layout, under the configured prefix) | `LOCK`, `manifest/` (`manifest/current`, `manifest/gen/<gen 20d>`), `meta/`, `blobs/`, `p/<pppp>/snap/`, `p/<pppp>/seg/` | P8 |
| Data-dir entries | `snapshots/pNNNN/` (local OSNP snapshots), `state/.install-pNNNN` (snapshot install staging) | P8 (08-02/08-09) |
| Run tag | `ostro.schedule` (runs started by a schedule, 08-14) | P8 |
| Env prefix | `OSTRO_BACKUP_*` (backup/restore CLI, 08-18) | P8 |
| Crates | `ostro-witcheck` | P5 (05-05) |
| Crates | `ostro-objstore` (shared P6/P8, first runner creates it; D19), `ostro-bridge` | P6 / P8 |
| Crates | `ostro-backup`, `ostro-triggers` | P8 |
| gRPC services | `ostro.api.v1.Secrets` (secrets, grants), `ostro.api.v1.Connectors` (connections, egress policies) | P6 (P-14) |
| Proto file | `ostro/api/v1/bridge.proto` | P6 (P-14) |
| CLI verbs | `ostro secret`, `ostro grant`, `ostro connection`, `ostro egress` | P6 (P-14) |
| Metric prefix | `ostro_effect_` | P6 (P-14) |
| Env vars | `OSTRO_SEALING_KEY`, `OSTRO_SECRET_*`, `OSTRO_TEST_PG_*`, `OSTRO_TEST_MYSQL_*`, `OSTRO_TEST_S3_*` | P6 (P-14) |
| Testkit features | `ostro-testkit` features `mock-http`, `db`, `canary` | P6 (P-12/P-14) |
| Test spec | Shared S3 test spec (SeaweedFS) (image, health wait, credentials, bucket `ostro-test`, job env): canonical table in `.planning/phases/06-effects-bridge-connectors-secrets/06-DECISIONS.md` § Test environment variables → "Shared S3 test spec". Phase 8 (08-03) references the same table; whichever phase lands first adds the CI step | P6 / P8 (W13) |
| Git tag | `wit/ostro-runtime-v0.1.0` (local) | P5 (05-20) |
| Crate | `ostro-conformance` (SDK conformance; Phase 12's replication suite must use a different name) | P9 |
| Failpoint prefix | `child::` | P9 |
| Error kinds (`error-info.kind`) | `child_terminated`, `sdk_protocol`, `invalid_string` | P9 (documented in WIT by P5 05-05) |
| SDK dirs / packages | `sdks/python`, `sdks/ts`; package name `ostro` on PyPI and npm | P9 |
| Name grammar (D23) | Qualified storage form `<tenant>/<name>`; resource names exclude `/`, `:` and a leading `__` (`__` is reserved for `__ostro/`, `__cron/`) | P5 (D23) / P14 |
| Tenant ids | `_` is the default-tenant spelling: reserved, never creatable | P14 (grammar enforced from P5, 05-13) |
| Config sections | `[auth]`, `[tenants]`, `[audit]`, `[security]` (incl. `[security.modules]`) | P14 |
| Data dir | `<data>/audit/` (node open, backup/restore and GC ignore unknown data-dir entries: F8) | P14 |
| REST | `/v1/tenants`, `/v1/auth/tokens`, `/v1/audit`, `POST /hooks/{tenant}/{name}`. Avoid `/v1/tokens/{id}`: `/v1/tokens/complete` is P5's await-token path | P14 |
| gRPC services | `ostro.api.v1.Tenants`, `ostro.api.v1.ApiTokens`, `ostro.api.v1.Audit` (`ostro.api.v1.Tokens` is P5's await-token service) | P14 |
| Failpoint prefixes | `auth::`, `audit::` | P14 |
| Crates / dirs | `fuzz/` (separate workspace, crate `ostro-fuzz`), `ostro-jepsen`, `deploy/helm/ostro`, `deploy/image` | P14 |
| Id prefixes | `otk_` (API tokens; `wkr_` = Phase 7 worker tokens) | P14 / P7 |
| Metric prefixes | `ostro_auth_`, `ostro_audit_`, `ostro_tenant_` | P14 |
| Crate | `ostro-repl-conformance` (replication conformance; P9 owns `ostro-conformance`) | P12 |
| gRPC package | `ostro.cluster.v1` (services `Replication`, `MetaPeer`) | P12 |
| Config | `[replication]`; inside P7's `[cluster]`: `[[cluster.peers]]`, `replication_listen` | P12 |
| Failpoint prefixes | `repl::`, `fetch::`, `isr::`, `elect::`, `install::` | P12 |
| Metric prefix | `ostro_repl_` | P12 |
| node.toml keys | `node_id`, `cluster_id` (0 = no-leader sentinel; mismatch fatal), next to P8's `incarnation`/`restored_from` | P12 |
| Data-dir temp prefix | `snapshots/pNNNN/.incoming-*` | P12 |
| Crate / dirs | `ostro-ui` (`crates/ostro-ui/{templates,assets}`), `<data>/bulk/` | P11 |
| REST | `/ui/*`, `GET /v1/dlq`, `POST /v1/dlq/{run}/requeue\|dismiss`, `POST /v1/runs/{id}/rerun`, `GET\|POST /v1/bulk`, `GET /v1/bulk/{id}`, `POST /v1/bulk/{id}/cancel`, list endpoints `GET /v1/modules`, `GET /v1/queues`, `GET /v1/partitions`, `GET /v1/limits` | P11 |
| gRPC services | `ostro.api.v1.RunOps`, `ostro.api.v1.Inventory` | P11 |
| Dedupe namespaces | `__rerun/`, `__onfail/`, `__bulk/`, `__dlq/` | P11 |
| System run tag | `__ostro/rerun_from=<seq>` (zero-format rerun lineage, PF-11-4 fallback) | P11 |
| Error kinds | `limit_exceeded`, `payload_too_large` (documented in WIT by 05-20) | P11 |
