# Ordinal & Name Reservations (single registry)

**Purpose.** Phases 6, 7 and 8 run in parallel after the Phase 5 freeze (05-20). All three append to the same ordinal-encoded spaces: prost tags, postcard enum/variant indices, digest order and outcome codes. Parallel appends without a registry would collide (Phase 8 research F4, Phase 6 research §Ordinal Reservations).

**Rules (binding for every phase plan from Phase 5 on):**
1. A plan may allocate an ordinal or reserved name **only from this file**, and only from its own phase's block.
2. If a phase needs more than its block, it first edits this file in a separate, docs-only commit that takes the next **unallocated** range, then writes code. Never borrow another phase's block.
3. Once an entry has shipped in code it is never reused or renumbered. A removed item keeps its number, marked `retired`.
4. Each phase's gate plan (Phase 5: 05-01 and 05-20) verifies the code against this file. Every ordinal present in code must appear here as `exists`, or as allocated to the phase that added it.
5. Status values:
   - `exists`: in code today.
   - `planned P5 (05-NN)`: allocated to a Phase 5 plan.
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
| 8 | SignalRun | planned P5 (05-01) |
| 9 | RequestCancel | planned P5 (05-01) |
| 10 | TerminateRun | planned P5 (05-01) |
| 11 | RetryRun | planned P5 (05-01) |
| 12 | CompleteToken | planned P5 (05-01) |
| 13 | CompleteEffect | reserved P6 |
| 14–19 | spare | reserved P6 |
| 20–26 | (unspecified) | reserved P7 |
| 27 | ScheduleFire | reserved P8 |
| 28–33 | spare | reserved P8 |
| 34–63 | — | unallocated |

## 2. `StartRun` field numbers (prost)

| Fields | Items | Status |
|--------|-------|--------|
| 1–13 | run_id … rng_seed | exists (P2) |
| 14–18 | module_sha, tags, tenant, traceparent, run_timeout_ms | exists (P4, 04-09) |
| 19 | schedule_to_start_ms | planned P5 (05-01) |
| 20 | retention_ms | planned P5 (05-01) |
| 21 | retry_of | planned P5 (05-01) |
| 22 | continued_from | planned P5 (05-01) |
| 23 | run_started (optional JournalEvent) | planned P5 (05-01) |
| 24–27 | — (P6 declared 0 needs) | reserved P6 |
| 28–31 | — | reserved P7 |
| 32–35 | — (P8 declared 0 needs) | reserved P8 |
| 36+ | — | unallocated |

Other message fields: each new field on any other message is owned by the plan that adds it, within its phase. Example: `CompleteRun.resumable` = 4, planned P5 (05-01). Parallel phases must not add fields to the same message without first recording the field number here.

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
| 7 | RegisterWait | planned P5 (05-01) |
| 8 | CancelWait | planned P5 (05-01) |
| 9 | RegisterEffect `{op, kind u8, scheduled_at_ms}` | reserved P6 |
| 10 | CancelEffect `{op}` | reserved P6 |
| 11–12 | spare | reserved P6 |
| 13–16 | — | reserved P7 |
| 17–18 | — (P8 declared 0 needs) | reserved P8 |
| 19+ | — | unallocated |

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
| 70 | WaitTimedOut | planned P5 (05-01) |
| 71 | RunTerminated | planned P5 (05-01) |
| 72–79 | — (P6 declared 0 needs) | reserved P6 |
| 80–87 | — | reserved P7 |
| 88–95 | — (P8 declared 0 needs) | reserved P8 |
| 96 | SignalPayload `{name, payload}`: submitter-encoded, stored verbatim (D21) | planned P5 (05-01; allocated under rule 2 from the unallocated range) |
| 97 | SignalMatched `{op, payload_seq}`: apply-encoded, no user data (D21) | planned P5 (05-01; rule 2) |
| 98–127 | — | unallocated |
| 128+ | — | reserved (codec) |

## 5. `TerminalStatus` enum values (prost) — partition `RunStatus` mirrors are appended in the same order

| Value | Item | Status |
|-------|------|--------|
| 0–2 | Completed, Failed, Cancelled | exists (P2) |
| 3 | Terminated | planned P5 (05-01) |
| 4 | TimedOut | planned P5 (05-01) |
| 5 | ContinuedAsNew | planned P5 (05-01) |
| 6–9 | — (P6, P8 declared 0) | reserved P7 |
| 10+ | — | unallocated |

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
| 14 | waits | planned P5 (05-02) |
| 15 | signal_seen | planned P5 (05-02) |
| 16 | schedule_state | reserved P8 (ScheduleState) |
| 17 | p7_a | reserved P7. **Not needed if 07-RESEARCH F1 lands** (05-02 adds `RunRecord.pause`). If F1 is rejected, rename to `run_pauses` in a docs-only commit before any placeholder is declared. |
| 18 | p7_b | reserved P7. Not needed if F1 lands. |
| 19+ | — | unallocated |

## 7. Apply outcome `RejectReason` (outcome code = 16 + index; folded into the state hash)

| Index | Item | Status |
|-------|------|--------|
| 0–10 | StaleEpoch, Fenced, SeqMismatch, SeqGap, RunNotFound, RunTerminal, RunExists, IdempotencyConflict, PayloadTooLarge, WrongPartition, Invalid | exists (P2) |
| 11 | RunCompleted | planned P5 (05-02) |
| 12–14 | — (P6 declared 0 needs) | reserved P6 |
| 15–17 | — | reserved P7 |
| 18–19 | — (P8 declared 0) | reserved P8 |
| 20+ | — | unallocated |

## 8. `InvalidCode` (declaration index; reported in Rejected(Invalid) detail)

| Index | Item | Status |
|-------|------|--------|
| 0–10 | Decode, Empty, BadRunId, MissingToken, EventsNotContiguous, MissingField, NotImplemented, BadIntent, FieldTooLong, BadSource, TruncateBeyondEnd | exists (P2) |
| 11 | BadLength | exists (P4; per 04-12 plan) |
| 12 | NotRunning | exists (P4; per 04-12 plan) |
| 13–14 | spare (P5 plans currently reuse BadIntent / NotImplemented) | reserved P5 |
| 15 | BadEffectCompletion | reserved P6 |
| 16–17 | spare | reserved P6 |
| 18–21 | — | reserved P7 |
| 22–23 | 1–2 schedule validation codes, names chosen by 08-01 | reserved P8 |
| 24 | spare | reserved P8 |
| 25+ | — | unallocated |

## 9. Outbox `outbox_msg::Kind` tags (prost)

| Tag | Item | Status |
|-----|------|--------|
| 1–5 | StartChild, ChildCompleted, Signal, CancelRun, CronStart (CronStart stays valid but unused by P8) | exists (P2) |
| 6 | StartRun (continue-as-new) | planned P5 (05-01) |
| 7 | — (P6 declared 0) | reserved P6 |
| 8–9 | — | reserved P7 |
| 10 | — (P8 declared 0) | reserved P8 |
| 11+ | — | unallocated |

## 10. Meta: `MetaRecord` / `MetaKind` variant index and `MetaState` map order (postcard, positional)

Rules (D20):
- Before the 05-20 freeze, Phase 5 lands everything known (05-07).
- **After the freeze, every meta change is a new variant appended here, plus a META_FORMAT bump with decoders for every older format.**

| Index | Variant (MetaState map) | Status |
|-------|------------------------|--------|
| 0–13 | Cluster, Member, Partition, Module, Workflow, Active, Queue, Schedule, Token, Tenant, Secret, Grant, Connection, Egress | exists (P4, 04-01) |
| — | `Schedule` gains timezone, overlap, catchup, catchup_window_ms, start_at_ms, end_at_ms, created_at_ms, ext | planned P5 (05-07), pre-freeze, for P8 F2(a) |
| 14 | WorkflowPolicy (`workflow_policies`) | planned P5 (05-07) |
| 15 | QueuePolicy (`queue_policies`) | planned P5 (05-07) |
| 16 | Webhook (`webhooks`) | planned P5 (05-07), pre-freeze, for P8 F3 |
| 17–18 | — (P6 uses existing Secret/Grant/Connection/Egress by convention: 0 needed) | reserved P6 |
| 19–20 | — (QueueFlow / WorkflowFlow only if F2 is rejected) | reserved P7. **Not needed if 07-RESEARCH F2 lands** (05-07 puts the Phase 7 fields in QueuePolicy/WorkflowPolicy pre-freeze) |
| 21 | — (P8 needs 0 post-freeze; Webhook landed in P5) | reserved P8 |
| 22+ | — | unallocated |

META_FORMAT: bumped once pre-freeze by 05-07. The value at 05-20 is the frozen baseline. Each post-freeze bump is recorded here with its phase.

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
| Crates | `ostro-witcheck` | P5 (05-05) |
| Crates | `ostro-objstore` (shared P6/P8, first runner creates it; D19), `ostro-bridge` | P6 / P8 |
| Crates | `ostro-backup`, `ostro-triggers` | P8 |
| Git tag | `wit/ostro-runtime-v0.1.0` (local) | P5 (05-20) |
