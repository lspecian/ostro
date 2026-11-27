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
     **Phase 6 gate 2026-11-20 (06-20):** every Phase 6 ordinal in code was verified against this file. Used rows are flipped to `exists (P6)` (written `exists (P6, <plan>)`): Cmd 13 `CompleteEffect`, Intent 9 `RegisterEffect` / 10 `CancelEffect`, InvalidCode 15 `BadEffectCompletion` and the keyspace #12 `effects` rows. Unused spares (Cmd 14–19, StartRun 24–27, Intent 11–12, EventKind 72–79, RejectReason 12–14, InvalidCode 16–17, Outbox 7, MetaKind 17–18, ext tags 16–31) stay `reserved P6`.
     **Phase 7 gate 2026-11-21 (07-22):** every Phase 7 ordinal in code was re-verified against this file (`07-22-rescheck`, the 07-01 script re-run; output in 07-22-SUMMARY). Used rows are flipped to `exists (P7, <plan>)`: Cmd 20 `PauseRun` / 21 `ResumeRun` and RejectReason 15 `RunPaused` (with the `ReservedP6_12..14` placeholders at 12–14). Keyspaces 17 `p7_a` / 18 `p7_b` and meta 19/20 stay **unused**; the other Phase 7 blocks (Cmd 22–26, StartRun 28–31, Intent 13–16, EventKind 80–87, TerminalStatus 6–9, RejectReason 16–17, InvalidCode 18–21 placeholders, Outbox 8–9, meta ext tags 32–47) stay `reserved P7`. The §11 Phase 7 names shipped as listed.
     **Phase 8 gate 2026-11-20 (08-22):** every Phase 8 ordinal in code was re-verified against this file (`08-22-rescheck.py`: every Cmd tag, keyspace and InvalidCode in code is `exists` or a placeholder; RESERVATIONS CHECK: PASS). The ordinal rows were flipped by 08-01 (Cmd 27 `ScheduleFire`, keyspace 16 `schedule_state`, InvalidCode 22 `BadScheduleFire` / 23 `ScheduleOwnerMismatch`) and are confirmed; the §11 Phase 8 names that shipped now read `exists (P8, <plan>)`. Unused Phase 8 slots stay `reserved P8`: Cmd 28–33, StartRun 32–35, Intent 17–18, EventKind 88–95, RejectReason 18–19, InvalidCode 24, Outbox kind 10, MetaKind 21, meta ext tags 48–63, and the `cron::` failpoint prefix.
     **Phase 7 gate 2026-11-21 (07-01):** the code was re-verified against this file before any Phase 7 code (`07-01-rescheck.py`, output in 07-01-SUMMARY: every Cmd tag, intent, outbox kind, TerminalStatus, StartRun field, journal kind, keyspace, RejectReason, InvalidCode and MetaRecord in code is `exists` or a canonical placeholder; every Phase 7 block is empty in code apart from the `ReservedP7_18..21` InvalidCode placeholders; RESERVATIONS CHECK: PASS). Phase 5 F1/F2/F3/F4/F7/F8 landed, so Phase 7 needs 0 keyspaces and 0 meta variants. Phase 7 allocations: Cmd 20/21, RejectReason 15 and the §11 Phase 7 names (D-W18).
     **Phase 9 gate 2026-11-21 (09-01):** the code was re-verified against this file before any Phase 9 code (`09-01-rescheck.py`, output in 09-01-SUMMARY: every Cmd tag, intent, outbox kind, TerminalStatus, StartRun field, journal kind, keyspace, RejectReason, InvalidCode and MetaRecord in code is `exists` or a canonical placeholder; RESERVATIONS CHECK: PASS). PF-F1 (`CompleteRun.parent_event` 5, outbox `ChildCompleted.event` 6) and PF-F2 (`RunRecord.continued_to`, CancelRun forwarding) landed in Phase 5, so Phase 9 allocates **no ordinal** (D-P9-6): every P9 row (Cmd 34–40, StartRun 36–39, Intent 19–22, EventKind 98–105, TerminalStatus 10–11, keyspace 19 `p9_a`, RejectReason 20–22, InvalidCode 25–28, Outbox 11–12, MetaRecord 22–23, meta ext tags 64–79) is empty in code and stays reserved, unused (fallback only). Phase 9 names: §11 (D-P9-6 rows plus the D-P9-20 rows added by 09-01).
     **Phase 9 end gate 2026-11-22 (09-20):** the code was re-verified against this file (`09-20` reservations check, output in 09-20-SUMMARY: RESERVATIONS CHECK: PASS). Phase 9 used exactly one ordinal, Intent 19 `ChildJournal` (09-05, D-P9-31, coordinator decision option (a)), flipped to `exists (P9, 09-05)`. Every other P9 slot (Cmd 34–40, StartRun 36–39, Intent 20–22, EventKind 98–105, TerminalStatus 10–11, keyspace 19 `p9_a`, RejectReason 20–22, InvalidCode 25–28 (canonical `ReservedP9_25..28` placeholders declared by 11-02), Outbox 11–12, MetaRecord 22–23, meta ext tags 64–79) is empty in code and reads "unused at Phase 9 end; fallback not needed". Every Phase 9 §11 name row now reads `exists (P9, <plan>)`.
     **Phase 12 gate 2026-11-22 (12-01):** the code was re-verified against this file before any Phase 12 code (`12-01-rescheck.py`, output in 12-01-SUMMARY: every Cmd tag, StartRun field, intent, outbox kind, TerminalStatus, journal kind, keyspace, RejectReason, InvalidCode, MetaRecord/MetaKind, MetaCommand and ext tag in code is `exists` or a canonical placeholder; MetaCommand is exactly 0–3; Cmd 41–42, MetaRecord 26 and ext tags 80–95 are empty; RESERVATIONS CHECK: PASS). F7 landed in 05-07, so Cmd 41–42 and MetaRecord 26 stay **unused**. Phase 12 allocations: MetaCommand 4 `ElectLeader`, 5 `AlterIsr`, 6 `ResignLeader` (7 spare), ext tag 80 `ElectionInfo` (81–95 spare) and the §11 Phase 12 names (D-P12-11/12).
     **Phase 10 end gate 2026-11-23 (10-20):** Phase 10 allocated no ordinal (F7): the ordinal spaces in code are identical at the 10-01 baseline (ca8a62e) and HEAD (`10-20-rescheck.py`, output in 10-20-SUMMARY; RESERVATIONS CHECK: PASS). The 15 §11 Phase 10 name rows read `exists (P10, ...)`.
     **Phase 11 end gate 2026-11-22 (11-18):** every Phase 11 ordinal in code was re-verified against this file (`11-18-rescheck.py`, output in 11-18-SUMMARY: every Cmd tag in code is `exists`; StartRun 40/41, JournalEvent.4, InvalidCode 29/30, meta ext 144/145 and `x_run_index` are in code; RESERVATIONS CHECK: PASS). Used rows read `exists (P11, <plan>)`: Cmd 45 `DlqUpdate`, StartRun 40/41, JournalEvent.4, InvalidCode 29/30, ext 144 `OnFailure` / 145 `DlqPolicy` and `x_run_index`. Unused P11 slots stay `reserved P11`: Cmd 46–49, StartRun 42–43, InvalidCode 31 (no `ReservedP11_31` placeholder is declared yet), ext 146–159, and the metric prefix `ostro_ui_`. The §11 Phase 11 names that shipped read `exists (P11, <plan>)`; 11-18 added the CLI verb row and `index_rebuilding`. **Not allocated (follow-up):** `RunInfo.retry_of` / `RunInfo.status_seq` and a `RunPage` order-fallback flag (header `X-Ostro-Order-Fallback`) need `ostro.api.v1` proto field numbers, and this file has no API-proto block to take them from; they are left to a later plan that adds such a block (no numbers invented).
     **Phase 12 end gate 2026-11-24 (12-18):** the P12 block re-verified against the final tree (output in 12-18-SUMMARY): MetaCommand is exactly 0–6 (`InitCluster`, `Put`, `Delete`, `SetPartitionLeader`, `ElectLeader`, `AlterIsr`, `ResignLeader`), the only registered ext tag in 80–95 is 80 (`EXT_ELECTION_INFO`), the `Cmd` oneof tags are 1–13, 20, 21, 27, 45 (no 41–42), and `MetaRecord`/`MetaKind` end at index 16 (no 26). Rows MetaCommand 4–6 and ext tag 80 flipped to `exists (P12)`; MetaCommand 7, ext tags 81–95, Cmd 41–42 and MetaRecord 26 are `reserved P12, unused`. Phase 12 allocated no other ordinal; §11 names unchanged.
     **Phase 13 gate 2026-11-24 (13-01):** the code was re-verified against this file before any Phase 13 code (`13-01-rescheck.py`, output in 13-01-SUMMARY): MetaCommand is exactly 0–6, nothing at Cmd 43–44, RejectReason ends at 15 `RunPaused`, no ext tag 96–111 (RESERVATIONS CHECK: PASS). Phase 13 allocations (D-P13-22): MetaCommand 7 `ReservedP12_7` placeholder + 8 `AlterReplicas`, Cmd 43 `RepinRun`, RejectReason 23 `ModuleMismatch` / 24 `RunBusy` (placeholders 16–22), ext tags 96–102, and the §11 Phase 13 names (D-P13-23).
     **Phase 13 end 2026-11-26 (13-20):** re-checked against the code at the gate commit: MetaCommand 7 `ReservedP12_7` + 8 `AlterReplicas` (9–11 unused), Cmd 43 `RepinRun` (44 unused), RejectReason 16–22 placeholders + 23 `ModuleMismatch` / 24 `RunBusy`, ext tags 96–102 (`ext::EXT_*`; 103–111 unused) all exist and are flipped to `exists (P13)`; §11 Phase 13 names flipped, plus the rows added after 13-01 (`already_on_target`, `NodeOps.StartRestart`, `NodeOps.DeliverInbound`, `ostro-deadline-ms`, the non-mutant P13 failpoints incl. `gc::start_hold`, `module_active` / `not_supported_in_profile`, `OSTRO_P13_MUTANT` and the 13-19 tier-C env vars). No name changed during execution; the optional `ELECTION_REASON_HANDOFF` constant was not added (handoffs use `ELECTION_REASON_RESIGN`), so nothing is reserved for it.
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
| 20 | PauseRun `{run_id 1, reason u32 2, detail string 3, token AttemptTokenMsg 4 (optional; present = executor auto-pause, fenced), requested_at_ms 5}` | exists (P7, 07-03; apply 07-04) |
| 21 | ResumeRun `{run_id 1, requested_at_ms 2}` | exists (P7, 07-03; apply 07-04) |
| 22–26 | spare | reserved P7 |
| 27 | ScheduleFire | exists (P8, 08-01) |
| 28–33 | spare | reserved P8 |
| 34–40 | — (P9 fallback only if PF-F1/F2 had not landed pre-freeze) | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
| 41–42 | — (contingency, expected use 0) | reserved P12, unused (F7 landed in 05-07; verified by 12-01, re-verified by 12-18: no Cmd tag 41–42 in code) |
| 43 | RepinRun `{run_id bytes 1, from_sha bytes 2, to_sha bytes 3, expected_next_seq u64 4, requested_at_ms u64 5}` (`ostro migrate-runs`, D-P13-21) | exists (P13): shape 13-01, apply 13-17 (`apply/repin.rs`), proposed by `migrate-runs` |
| 44 | spare | reserved P13, unused |
| 45 | DlqUpdate `{run_id 1, op oneof {HookStarted 2, Dismissed 3, Requeued 4}, episode_completed_at_ms 5}` (research proposed 43, which is taken by P13 RepinRun) | exists (P11, 11-02; apply 11-08) |
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
| 36–39 | — | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
| 40 | rerun_prefix (repeated JournalEvent) | exists (P11, 11-02; apply 11-07) |
| 41 | rerun_from_seq (uint64) | exists (P11, 11-02; apply 11-07) |
| 42–43 | spare | reserved P11 |
| 44+ | — | unallocated |

Other message fields: each new field on any other message is owned by the plan that adds it, within its phase. Recorded allocations:

| Message.field | Item | Status |
|---|---|---|
| CompleteRun.4 | resumable | exists (P5, 05-01) |
| CompleteRun.5 | parent_event (submitter-encoded ChildCompleted event bytes, D21) | exists (P5, 05-01), Phase 9 PF-F1; verified in code by 09-01 |
| outbox ChildCompleted.6 | event (same bytes, forwarded verbatim) | exists (P5, 05-01), Phase 9 PF-F1; verified in code by 09-01 (`apply/run.rs` forwards `parent_event`) |
| AttemptGranted.3 | after_failure (durable attempt_failures, F9) | exists (P5, 05-01) |
| RegisterWait.5 | at_ms (D21) | exists (P5, 05-01) |
| JournalEvent.4 | attempt (fixed64, submitter-set; 0 = apply-originated; PF-11-5 apply stamping was NOT adopted) | exists (P11, 11-02) |

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
| 19 | ChildJournal `{child_run bytes 1}`: opt-in child-completion journaling marker, directly after its `SendMessage(StartChild)`; marks the parent's `children` row (value `[1]`) so an inbound `ChildCompleted` is journaled as kind 67 instead of the v0.1 inbox row (format v0.1 frozen, rule 4) | exists (P9, 09-05; P9 fallback slot, coordinator decision option (a)) |
| 20–22 | — (P9 fallback intent slot) | reserved P9, **unused** (fallback only) (unused at Phase 9 end; fallback not needed) |
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
| 98–105 | — | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
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
| 10–11 | — | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
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
| 12 | effects | exists (P2); rows exists (P6, 06-03): `run ‖ op BE → EffectRow{kind, scheduled_at_ms}`, written by `RegisterEffect`, deleted by `CancelEffect`/`CompleteEffect`/terminal cleanup, scanned on `LeaderAnnounced`. 0 new keyspaces for P6. Verified at the Phase 6 gate (06-20). |
| 13 | running | exists (P4; per 04-12 plan) |
| 14 | waits | exists (P5, 05-02) |
| 15 | signal_seen | exists (P5, 05-02) |
| 16 | schedule_state | exists (P8, 08-01; `Table::ScheduleState`, rows from 08-07) |
| 17 | p7_a | reserved P7, **unused** (F1 landed in 05-02: `RunRecord.pause: Option<PauseMark>`; verified by 07-01). Never declared by Phase 7; a later phase landing keyspace 19+ declares it as an empty placeholder under the placeholder rule. |
| 18 | p7_b | reserved P7, **unused** (F1 landed in 05-02; verified by 07-01). Never declared by Phase 7. |
| 19 | p9_a | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only). Never declared by Phase 9 (D-P9-6); a later phase landing keyspace 20+ declares it as an empty placeholder under the placeholder rule. (unused at Phase 9 end; fallback not needed) |
| (none) | `x_*` derived keyspaces (PF-11-2): outside TABLES, the digest, OSNP and blob GC; rebuilt from digest state. Reserved name: `x_run_index` (P11) | rule frozen in 05-20; `x_run_index` exists (P11, 11-05) |
| 20+ | — | unallocated |

## 7. Apply outcome `RejectReason` (outcome code = 16 + index; folded into the state hash)

| Index | Item | Status |
|-------|------|--------|
| 0–10 | StaleEpoch, Fenced, SeqMismatch, SeqGap, RunNotFound, RunTerminal, RunExists, IdempotencyConflict, PayloadTooLarge, WrongPartition, Invalid | exists (P2) |
| 11 | RunCompleted | exists (P5, 05-02) |
| 12–14 | `#[doc(hidden)] ReservedP6_12..14` placeholders (declared by 07-03, never emitted) | reserved P6 |
| 15 | RunPaused (outcome code 31) | exists (P7, 07-03) |
| 16–17 | spare; `#[doc(hidden)] ReservedP7_16`, `ReservedP7_17` placeholders declared by 13-01 (P-15, never emitted; outcome codes 32–33) | reserved P7 |
| 18–19 | — (P8 declared 0); `#[doc(hidden)] ReservedP8_18`, `ReservedP8_19` placeholders declared by 13-01 (never emitted; codes 34–35) | reserved P8 |

RejectReason 12–14 are absent in code at the Phase 7 gate (07-01, 2026-11-21: code ends at 11 `RunCompleted`). 07-03 declares `#[doc(hidden)] ReservedP6_12`, `ReservedP6_13`, `ReservedP6_14` if still absent (P-15 convention; never emitted, so never in the state hash) before `RunPaused` at 15. Nothing sits at 15–17.
| 20–22 | — ; `#[doc(hidden)] ReservedP9_20..22` placeholders declared by 13-01 (never emitted; codes 36–38) | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
| 23 | ModuleMismatch (outcome code 39): `RepinRun` whose `from_sha` ≠ the run's `module_sha` | exists (P13): declared 13-01, emitted 13-17 |
| 24 | RunBusy (outcome code 40): `RepinRun` on a leased run | exists (P13): declared 13-01, emitted 13-17 |
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
| 25–28 | — (P9 spare, incl. fallback) | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
| 29 | BadRerunPrefix | exists (P11, 11-02; emitted from 11-07) |
| 30 | BadDlqUpdate | exists (P11, 11-02; emitted from 11-08) |
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
| 24 | `ReservedP8_24` (declared by 11-02) | P8 |

Indices 25+ follow the same pattern with their owner block (e.g. `ReservedP9_25`, `ReservedP11_31`). 11-02 declared `ReservedP9_25..ReservedP9_28` below `BadRerunPrefix` (29).

## 9. Outbox `outbox_msg::Kind` tags (prost)

| Tag | Item | Status |
|-----|------|--------|
| 1–5 | StartChild, ChildCompleted, Signal, CancelRun, CronStart (CronStart stays valid but unused by P8) | exists (P2) |
| 6 | StartRun (continue-as-new) | exists (P5, 05-01) |
| 7 | — (P6 declared 0) | reserved P6 |
| 8–9 | — | reserved P7 |
| 10 | — (P8 declared 0) | reserved P8 |
| 11–12 | — (11 = P9 fallback outbox kind if PF-F1 had not landed) | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
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
| 19–20 | — (QueueFlow / WorkflowFlow only if F2 had been rejected) | reserved P7, **unused** (F2 landed in 05-07: SpillPolicy, RateSpec, key_rate_limit, key_concurrency, on_retries_exhausted, placement, selector, WorkflowPolicy.paused; F8 `ApiToken.expires_at_ms` landed too; verified by 07-01). No Phase 7 META_FORMAT bump. |
| 21 | — (P8 needs 0 post-freeze; Webhook landed in P5) | reserved P8 |
| 22–23 | — | reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only) (unused at Phase 9 end; fallback not needed) |
| 24–25 | — (fallback only if Phase 14 F2/F3/F4 had not landed pre-freeze) | reserved P14, unused (F2/F3/F4 landed pre-freeze) (14-01: MetaRecord ends at index 16; no Phase 14 META_FORMAT bump) |
| 26 | — (PartitionReplication fallback only if Phase 12 F7 had not landed pre-freeze) | reserved P12, unused (12-18: MetaRecord ends at index 16) (F7 landed in 05-07: `PartitionAssignment.{replicas, isr_version, min_insync}`, `MemberRecord.{cluster_addr, repl_addr}`; verified by 12-01). No Phase 12 META_FORMAT bump. |
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
| 4 | ElectLeader | exists (P12) — shape 12-02, apply 12-05, proposed by the 12-10 controller |
| 5 | AlterIsr | exists (P12) — shape 12-02, apply 12-05, proposed by the 12-11 ISR publisher and the controller |
| 6 | ResignLeader | exists (P12) — shape 12-02, apply 12-05, proposed on a Failed leader (12-13) |
| 7 | `#[doc(hidden)] ReservedP12_7 {}` (P-15 placeholder so `AlterReplicas` lands at index 8; never proposed, apply → `Invalid("reserved")`) | placeholder ReservedP12_7 (13-01); slot reserved P12, unused |
| 8 | AlterReplicas `{partition u16, epoch u32, expected_isr_version u64, replicas Vec<u64>, isr Vec<u64>, min_insync u8, reassignment Option<Reassignment>}` (D-P13-12) | exists (P13): shape 13-01, apply 13-05/13-10, proposed by the 13-10 placement controller |
| 9–11 | spare | reserved P13, unused |
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
| 64–79 | reserved P9 | — (unused at Phase 9 end; fallback not needed) |
| 80 | `PartitionAssignment` ElectionInfo `{reason u8: 0 Bootstrap 1 Startup 2 Failover 3 Resign 4 Offline, elected_at_ms u64, prev_leader u64}` | exists (P12) — `ext::EXT_ELECTION_INFO`, `ElectionInfo` (12-02), written by ElectLeader/ResignLeader apply |
| 81–95 | spare (Member, PartitionAssignment, cluster) | reserved P12, unused (12-18: only tag 80 registered in code) |
| 96 | `Member` NodeVersion `{ostro_version, level_min, level_max, wasmtime, event_codec_ver, wit_lines, reported_at_ms}` | exists (P13; declared 13-01): `ext::EXT_NODE_VERSION`, postcard `NodeVersion{ostro_version: String, level_min: u32, level_max: u32, wasmtime: String, event_codec_ver: u32, wit_lines: Vec<String>, reported_at_ms: u64}` |
| 97 | `Member` MemberState `{state: Joining/Active/Draining/Removed, changed_at_ms}` (tombstone; node ids never reused) | exists (P13; declared 13-01): `ext::EXT_MEMBER_STATE`, postcard `MemberState{state: u8 (0 Joining, 1 Active, 2 Draining, 3 Removed), changed_at_ms: u64}` |
| 98 | `ClusterInfo` ClusterVersion `{finalized, hold: Option<u32>, finalized_at_ms}` (the 1.0 binary must already read it; absent = baseline level) | exists (P13; declared 13-01): `ext::EXT_CLUSTER_VERSION`, postcard `ClusterVersion{finalized: u32, hold: Option<u32>, finalized_at_ms: u64}` |
| 99 | `ClusterInfo` ReplicationPolicy `{rf, min_insync, max_voters}` | exists (P13; declared 13-01): `ext::EXT_REPLICATION_POLICY`, postcard `ReplicationPolicy{rf: u8, min_insync: u8, max_voters: u8}` |
| 100 | `PartitionAssignment` Reassignment `{target, adding, removing, started_at_ms}` (KIP-455 shape) | exists (P13; declared 13-01): `ext::EXT_REASSIGNMENT`, postcard `Reassignment{target: Vec<u64>, adding: Vec<u64>, removing: Vec<u64>, started_at_ms: u64}` |
| 101 | `ModuleRecord` WitRequirement `{line, min_level}` | exists (P13; declared 13-01): `ext::EXT_WIT_REQUIREMENT`, postcard `WitRequirement{line: String, min_level: u32}` |
| 102 | `ModuleRecord` / `WorkflowVersion` Retired `{retired_at_ms}` | exists (P13; declared 13-01): `ext::EXT_RETIRED`, postcard `Retired{retired_at_ms: u64}` |
| 103–111 | spare | reserved P13, unused |
| 112 | `ApiToken` ApiTokenMeta `{name, description, created_by, rotated_from}` | planned P14 (14-01): `ext::EXT_API_TOKEN_META`, postcard `ApiTokenMeta{name: String, description: String, created_by: String, rotated_from: Option<String>}` (D-P14-28) |
| 113 | `ModuleRecord` ModuleSignatures `{sigs}` (signature evidence; key fingerprints) | planned P14 (14-01): `ext::EXT_MODULE_SIGNATURES`, postcard `ModuleSignatures{sigs: Vec<ModuleSig{alg: u8 (1 minisign-Ed, 2 minisign-ED, 3 cosign-p256), key_fingerprint: [u8; 32] (sha256(alg ‖ public_key)), key_id: Vec<u8>, sig: Vec<u8>, added_at_ms: u64, added_by: String}>}` (D-P14-28, D-P14-10 B1) |
| 114 | `ModuleRecord` ModuleUploaders `{tenants}` | planned P14 (14-01): `ext::EXT_MODULE_UPLOADERS`, postcard `ModuleUploaders{tenants: Vec<String>}` (D-P14-9) |
| 115 | `ClusterInfo` SecurityPolicy `{require_signed_modules, trusted_keys, auth_mode}` | planned P14 (14-01): `ext::EXT_SECURITY_POLICY`, postcard `SecurityPolicy{require_signed_modules: bool, trusted_keys: Vec<TrustedKey{alg: u8, key_id: Vec<u8>, public_key: Vec<u8>, comment: String}>, auth_mode: u8}` (D-P14-10) |
| 116 | `Tenant` TenantSecurity `{require_signed_modules, trusted_keys}` (can only tighten) | planned P14 (14-01): `ext::EXT_TENANT_SECURITY`, postcard `TenantSecurity{require_signed_modules: Option<bool>, trusted_keys: Vec<TrustedKey>}` (D-P14-10) |
| 117 | `SealedSecret` SecretTimes `{created_at_ms, rotated_at_ms}` | planned P14 (14-01): `ext::EXT_SECRET_TIMES`, postcard `SecretTimes{created_at_ms: u64, rotated_at_ms: Option<u64>}` (D-P14-28) |
| 118–143 | spare | reserved P14, spare |
| 144 | `WorkflowPolicy` / `QueuePolicy` OnFailure | exists (P11, 11-12) |
| 145 | DlqPolicy `{enabled, hook_catchup_ms}` | exists (P11, 11-12) |
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
| REST path | `POST /hooks/{name}` (+ webhook CRUD under `/v1/webhooks`) | exists (P8, 08-10 routes; 08-15 controller) |
| gRPC service | `ostro.api.v1.Tokens` | P5 (05-04) |
| Sealed-secret prefix | `__ostro/` (exists: `__ostro/token-key`; P6 adds `__ostro/presign/<connection>`) | P4 / P6 |
| Grant id prefix | `secret:` | P6 |
| Dedupe key namespace | `__cron/<schedule>` (`__cron/<schedule>/<fire_time_ms>`, `__cron/<schedule>/manual/<hex>`) | exists (P8, 08-01 key shape; 08-07/08-14) |
| Dedupe / idempotency namespace | `__hook/<name>/<key>` (webhook deliveries, 08-15) | exists (P8, 08-15) |
| Config sections | `[telemetry]`, `[snapshots]`, `[backup]`, `[blob_gc]`, `[triggers]`, `[guest_logs]` (P8-D22; parsed by 08-04) | exists (P8, 08-04) |
| Failpoint prefixes | `snapshot::` (08-02/08-09), `trim::` (08-09), `upload::` (08-13), `restore::` (08-18), `blobgc::` (08-19) | exists (P8, 08-02/08-09/08-13/08-18/08-19) |
| Failpoint prefix | `cron::` (no site shipped in Phase 8) | reserved P8 |
| REST paths | `GET /metrics`; `/v1/schedules` (+ `/v1/schedules/{name}/pause`, `/resume`, `/trigger`); `GET /v1/runs/{id}/logs`; `GET /v1/admin/backup` | exists (P8, 08-10; controllers 08-14/08-15/08-17/08-18) |
| gRPC services | `ostro.api.v1.Schedules`, `ostro.api.v1.Webhooks` | exists (P8, 08-10) |
| gRPC service + RPC | `ostro.api.v1.RunLogs` / `GetRunLogs` (in `schedules.proto`). 08-10 did not append to `Runs`: runs.proto stays untouched so the Phase 8 surface lives in its own proto files and 06-08 shares no proto edit. Hook ingress and the admin backup are REST-only | exists (P8, 08-10; 08-17) |
| node.toml keys | `incarnation` (08-08; additive, F6), `restored_from` (08-18) | exists (P8, 08-08/08-18) |
| Object-store prefixes (bottomless layout, under the configured prefix) | `LOCK`, `manifest/` (`manifest/current`, `manifest/gen/<gen 20d>`), `meta/`, `blobs/`, `p/<pppp>/snap/`, `p/<pppp>/seg/` | exists (P8, 08-13) |
| Data-dir entries | `snapshots/pNNNN/` (local OSNP snapshots, `.blobrefs` sidecars, `.tmp-*` in flight), `state/.install-pNNNN` (snapshot install staging), `state/.old-pNNNN` (replaced state during the install swap; recorded at the gate, shipped by 08-02), `backup/gc-ledger` (remote blob deletion ledger; recorded at the gate, shipped by 08-19) | exists (P8, 08-02/08-09/08-19) |
| Run tag | `ostro.schedule` (runs started by a schedule, 08-14) | exists (P8, 08-14) |
| Env prefix | `OSTRO_BACKUP_*` (backup/restore CLI, 08-18) | exists (P8, 08-18) |
| Crates | `ostro-witcheck` | P5 (05-05) |
| Crates | `ostro-objstore` (shared P6/P8, first runner creates it; D19), `ostro-bridge` | P6 / P8 |
| Crates | `ostro-backup`, `ostro-triggers` | exists (P8, 08-01) |
| gRPC services | `ostro.api.v1.Secrets` (secrets, grants), `ostro.api.v1.Connectors` (connections, egress policies) | P6 (P-14) |
| Proto file | `ostro/api/v1/bridge.proto` | P6 (P-14) |
| CLI verbs | `ostro secret`, `ostro grant`, `ostro connection`, `ostro egress` | P6 (P-14) |
| Metric prefix | `ostro_effect_` | P6 (P-14) |
| Env vars | `OSTRO_SEALING_KEY`, `OSTRO_SECRET_*`, `OSTRO_TEST_PG_*`, `OSTRO_TEST_MYSQL_*`, `OSTRO_TEST_S3_*` | P6 (P-14) |
| Testkit features | `ostro-testkit` features `mock-http`, `db`, `canary` | P6 (P-12/P-14) |
| Test spec | Shared S3 test spec (SeaweedFS) (image, health wait, credentials, bucket `ostro-test`, job env): canonical table in `.planning/phases/06-effects-bridge-connectors-secrets/06-DECISIONS.md` § Test environment variables → "Shared S3 test spec". Phase 8 (08-03) references the same table; whichever phase lands first adds the CI step | P6 / P8 (W13) |
| Git tag | `wit/ostro-runtime-v0.1.0` (local) | P5 (05-20) |
| Crate | `ostro-conformance` (SDK conformance; Phase 12's replication suite must use a different name) | exists (P9, 09-02) |
| Failpoint prefix | `child::` | exists (P9, 09-08) |
| Error kinds (`error-info.kind`) | `child_terminated`, `sdk_protocol`, `invalid_string` | exists (P9, 09-05/09-09/09-10; documented in WIT by P5 05-05) |
| SDK dirs / packages | `sdks/python`, `sdks/ts`; package name `ostro` on PyPI and npm | exists (P9, 09-03; packages not yet published, human_needed D-P9-19) |
| Cargo feature | `polyglot` (crate `ostro-conformance`; off by default, gates the Python/TS legs) | exists (P9, 09-02) |
| Binary | `ostro-conf-inspect` (crate `ostro-conformance`) | exists (P9, 09-02) |
| Dirs | `conformance/`, `sdks/scripts/` (incl. `sdks/scripts/toolchain/`), `target/polyglot-guests/`, `target/toolchains/` | exists (P9, 09-02/09-17) |
| Config keys | `[engine] class` (`rust`/`python`/`typescript`/`mixed`), `[engine] pool_total_core_instances` (flat keys in `[engine]`) | exists (P9, 09-07) |
| Failpoints | `child::detached_after_flush`, `child::parent_event_before_flush`, `child::start_child_after_flush` (prefix `child::`) | exists (P9, 09-08) |
| Guest names | `conf-rust`, `conf-rust-v2`, `child-parent`, `child-leaf`, `sdk-saga`, `exports-shape-ok`, `exports-shape-bad` | exists (P9, 09-06/09-07/09-11) |
| Error kinds (`error-info.kind`) | `python_exception` (Python SDK, uncaught exceptions; 09-09), `js_exception` (TS SDK; 09-10); reuse of the existing SDK kind `unknown_workflow` for unresolvable child targets (D-P9-8) | exists (P9, 09-05/09-09/09-10) |
| CancelRun reason strings | `parent_closed`, `parent_cancel_op`, `chain_done` (documented strings, not a registry space) | exists (P9, 09-08) |
| Env vars | `OSTRO_POLYGLOT_CACHE`, `OSTRO_CONF_LANGS`, `OSTRO_CRASH_CHAIN_ITERS` | exists (P9, 09-02/09-11/09-18) |
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
| Crate | `ostro-fuzz-targets` (`publish = false`; fuzz target logic + stable corpus replay in the main workspace, D-P14-17) | P14 (14-01) |
| gRPC service | `ostro.api.v1.Security` (next to the reserved `Tenants`, `ApiTokens`, `Audit`) | P14 (D-P14-29) |
| REST paths | `GET\|POST /v1/tenants`, `GET\|PATCH\|DELETE /v1/tenants/{id}`, `POST /v1/tenants/{id}/{suspend,resume}`, `GET\|POST /v1/auth/tokens`, `DELETE /v1/auth/tokens/{id}`, `POST /v1/auth/tokens/{id}/rotate`, `GET /v1/auth/whoami`, `GET /v1/audit`, `GET\|PUT /v1/security/modules/policy`, `POST /v1/security/modules/keys`, `DELETE /v1/security/modules/keys/{key_id}`, `POST /v1/modules/{sha}/signatures`, `POST /hooks/{tenant}/{name}`, `POST /ui/login`, `POST /ui/logout` | P14 (D-P14-29) |
| REST test endpoints (failpoints builds only; 404 in a default build) | `POST /v1/admin/test/clock`, `POST /v1/admin/test/failpoints` | P14 (D-P14-29, D-P14-34) |
| Header | `Ostro-Module-Signature` (repeatable; base64 of a `.minisig` or cosign signature, ≤ 8 KiB each, ≤ 4) | P14 (D-P14-10) |
| Cookie | `ostro_session` (UI session: base64url(`{token_id, exp_ms}`) ‖ HMAC-SHA256) | P14 (D-P14-31) |
| Sealed secret | `__ostro/ui-session-key` (plaintext-v0 until 14-19's reseal; `__ostro/token-key`, `__ostro/cluster-ca` resealed too, D-P14-30) | P14 (D-P14-29/31) |
| CLI verbs | `ostro tenant {create,ls,get,set,suspend,resume,rm}`, `ostro auth {token {create,ls,revoke,rotate},whoami,bootstrap}`, `ostro audit {ls,verify}`, `ostro security {policy {get,set},trust {add,rm}}`, `ostro deploy --signature <file>` | P14 (D-P14-29) |
| Env vars | `OSTRO_TOKEN` (CLI bearer), `OSTRO_BOOTSTRAP_TOKEN`, `OSTRO_REQUIRE_SIGNATURE`, `OSTRO_DOWNLOAD_BASE`, `OSTRO_INSTALL_DIR`, `OSTRO_FUZZ_SECS`, `OSTRO_JEPSEN_SECS`, `OSTRO_JEPSEN_SEED`, `OSTRO_JEPSEN_SEEDS`, `OSTRO_JEPSEN_OUT`, `OSTRO_JEPSEN_TOKEN`, `OSTRO_BENCH_ENFORCE`, `OSTRO_SOAK_SECS`, `OSTRO_SOAK_LOAD`, `OSTRO_EXPORT_AUTH_MATRIX`, `OSTRO_AUDIT_CRASH_ITERS`, `OSTRO_MINISIGN_PUBKEY` | P14 (D-P14-29) |
| Data-dir file | `<data>/admin.token` (generated bootstrap bearer, 0600; next to `<data>/audit/`) | P14 (D-P14-5) |
| Failpoints | `auth::lookup`, `audit::before_fsync`, `audit::writer_stall`, `audit::mutant::skip_chain` | P14 (D-P14-29) |
| API error `details.reason` values | `unauthenticated`, `missing_scope`, `tenant_suspended`, `tenant_event_rate`, `tenant_not_empty`, `signature_required`, `signature_invalid`, `audit_unavailable`, `forbidden_tenant`, `audit_commit_failed` | P14 (D-P14-29) |
| Metric prefix | `ostro_security_` (covers `ostro_security_policy_drift`) | P14 (D-P14-29) |
| Config keys | `[tenants] metrics_tenant_labels`, `metrics_label_limit`; `[audit] head_export_interval` (plus the D-P14-5/11/12/31 keys inside the reserved `[auth]`/`[audit]` sections) | P14 (D-P14-29/39) |
| Files / dirs | `install.sh`, `deploy/helm/ostro`, `deploy/helm/schemas`, `deploy/image`, `.github/workflows/{release,fuzz}.yml`, `docs/security/` | P14 (D-P14-29) |
| Crate | `ostro-kafka` (`publish = false`; Kafka-protocol consumer client, 14-31/14-34) | P14 (D-P14-51) |
| Cargo feature | `kafka` (crates `ostro-server` and `ostro`; default-on in `ostro`, D-P14-42) | P14 (D-P14-51) |
| Config | `[[triggers.kafka]]` (key `kafka` inside the Phase 8 `[triggers]` section) with sub-tables `tls`, `sasl` | P14 (D-P14-51) |
| Name grammar | idempotency-key prefix `__kafka/` (joins `__ostro/`, `__cron/`, `__hook/` under the D23 `__` reservation) | P14 (D-P14-51) |
| Run tag | `ostro.kafka` (`ostro.kafka=<trigger name>`) | P14 (D-P14-51) |
| Sealing-key rotation | env `OSTRO_SEALING_KEY_OLD`; config `[secrets] old_sealing_key_files`; REST `GET /v1/security/sealing`, `POST /v1/security/sealing/reseal`; gRPC `Security/GetSealingStatus`, `Security/Reseal`; CLI `ostro secret {reseal,sealing}`; audit action `security.sealing.reseal` (the pass itself audits `system.reseal`); `details.reason` `sealing_key_invalid` | P14 (p14-key-rotation) |
| Metric prefix | `ostro_kafka_` | P14 (D-P14-51) |
| Failpoint prefixes | `kafka::`, `cluster_restore::` | P14 (D-P14-51) |
| Failpoints (mutants, `repl::mutant::*` convention) | `repl::mutant::readd_stale_progress` (14-33), `repl::mutant::restore_skip_fixpoint` (14-30) | P14 (D-P14-51) |
| Object key | `meta/head` (cluster bucket metadata-snapshot pointer, D-P14-45) | P14 (D-P14-51) |
| Env vars | `OSTRO_TEST_KAFKA_BOOTSTRAP`, `OSTRO_TEST_KAFKA_TLS_*`, `OSTRO_TEST_KAFKA_SASL_*`, `OSTRO_TEST_SERVICES_DIR`, `OSTRO_KAFKA_CRASH_ITERS`, `OSTRO_KAFKA_CAPTURE_FIXTURES` | P14 (D-P14-51) |
| Script | `scripts/test-services.sh` | P14 (D-P14-51) |
| Docs | `docs/kafka-triggers.md`, `docs/cluster-restore.md` | P14 (D-P14-51) |
| Error / report names | `NoConsistentCut` (cluster restore), `unsupported_version` (Kafka client) | P14 (D-P14-51) |
| Crate | `ostro-repl-conformance` (replication conformance; P9 owns `ostro-conformance`) | P12 |
| gRPC package | `ostro.cluster.v1` (services `Replication`, `MetaPeer`) | P12 |
| Config | `[replication]`; inside P7's `[cluster]`: `[[cluster.peers]]`, `replication_listen` | P12 |
| Failpoint prefixes | `repl::`, `fetch::`, `isr::`, `elect::`, `install::` | P12 |
| Metric prefix | `ostro_repl_` | P12 |
| node.toml keys | `node_id`, `cluster_id` (0 = no-leader sentinel; mismatch fatal), next to P8's `incarnation`/`restored_from` | P12 |
| Data-dir temp prefix | `snapshots/pNNNN/.incoming-*` | P12 |
| Crate | `ostro-cluster` (replication, ISR, election, metadata peer transport) | P12 (12-01) |
| Proto file + services | `ostro/cluster/v1/cluster.proto`: `Replication {Fetch, FetchSnapshot, FetchBlob, PushBlob}`, `MetaPeer {Raft, Propose, ReportSuspect}` | exists (P12, 12-02) |
| Config keys | `[replication]`: `session_timeout`, `replica_lag_max`, `meta_tick_interval`, `meta_election_tick`, `meta_heartbeat_tick`, `fetch_max_wait`, `fetch_read_timeout`, `fetch_max_bytes`, `rf`, `min_insync` | exists (P12, 12-02) |
| Config keys | inside P7's `[cluster]`: `cluster_id`, `replication_listen`, `[[cluster.peers]] {id, addr, repl_addr}`, `[cluster.tls] ca_cert_file, ca_key_file` | exists (P12, 12-02) |
| REST path | `GET /v1/admin/replication` (server-local route) | planned P12 |
| Failpoints | `repl::mutant::{isr_from_assignment, hw_leader_leo, count_nondurable_leo, skip_epoch_check, no_truncate, unclean_election, ignore_min_insync, shrink_before_commit, upload_above_hw, rf1_bypass}` | names exist (P12, 12-02 `ostro_log::mutants`); sites planned P12 |
| Failpoints | `install::{after_log_reset, before_rename, after_rename}`, `fetch::{before_append, before_durable}`, `isr::before_alter`, `elect::before_propose` | planned P12 |
| Failpoint | `repl::before_services_stop` (ostro-partition roles step-down test hook, `return(<ms>)`) | exists (P12, 12-16) |
| Object key (bottomless layout) | `p/<pppp>/head` | planned P12 |
| Metrics | `ostro_repl_*`, incl. `ostro_repl_upload_fenced{partition}` and `ostro_repl_self_demotions_total` | planned P12 |
| Env vars | `OSTRO_REPL_RED_RUN`, `OSTRO_REPL_REQUIRE_ALL`, `OSTRO_FAILOVER_ITERS`, `OSTRO_FAILOVER_ASSERT`, `OSTRO_FAILOVER_SWEEP`, `OSTRO_CAMPAIGN_FAULTS` (shared with the P5 campaign knob), `OSTRO_REPL_BENCH_SECS`, `OSTRO_PROC_ITERS`, `OSTRO_FAILPOINT_BIN`, `OSTRO_PROC_BUILD`, `OSTRO_REPL_SCENARIO` / `OSTRO_REPL_REPEAT` (12-16 `tests/scenario.rs`) | planned P12 |
| Crate / dirs | `ostro-ui` (`crates/ostro-ui/{templates,assets}`), `<data>/bulk/` | exists (P11, 11-03 crate; 11-13 `<data>/bulk/`) |
| REST | `/ui/*`, `GET /v1/dlq`, `POST /v1/dlq/{run}/requeue\|dismiss`, `POST /v1/runs/{id}/rerun`, `GET\|POST /v1/bulk`, `GET /v1/bulk/{id}`, `POST /v1/bulk/{id}/cancel`, list endpoints `GET /v1/modules`, `GET /v1/queues`, `GET /v1/partitions`, `GET /v1/limits` | exists (P11, 11-09 routes; 11-10..11-17) |
| gRPC services | `ostro.api.v1.RunOps`, `ostro.api.v1.Inventory` | exists (P11, 11-09) |
| Dedupe namespaces | `__rerun/`, `__onfail/`, `__bulk/`, `__dlq/` | exists (P11, 11-10 / 11-12 / 11-13) |
| System run tag | `__ostro/rerun_from=<seq>` (zero-format rerun lineage, PF-11-4 fallback) | exists (P11, 11-07) |
| System run tag | `__ostro/on_failure_of=<run>` (on-failure hook runs: the failed run they handle; D-P11-25) | exists (P11, 11-12) |
| Error kinds | `limit_exceeded`, `payload_too_large` (documented in WIT by 05-20) | exists (P11, 11-06) |
| Config sections | `[limits]`, `[ui]`, `[bulk]`, `[dlq]` (D-P11-25) | exists (P11, 11-04) |
| Env var | `OSTRO_UI_ALLOWED_HOSTS` (D-P11-25) | exists (P11, 11-04) |
| Metric prefixes | `ostro_bulk_`, `ostro_rerun_`, `ostro_ui_`; `ostro_dlq_` is shared with P8 (`ostro_dlq_total` exists, P8): Phase 11 adds the series `ostro_dlq_hooks_total`, `ostro_dlq_requeues_total` (D-P11-25) | exists (P11, 11-10 / 11-12 / 11-13); `ostro_ui_` unused, stays reserved P11 |
| Failpoint prefixes | `bulk::`, `rerun::`, `dlq::` (D-P11-25) | exists (P11, 11-07 / 11-12 / 11-13) |
| Id prefix | `blk_` (bulk jobs) (D-P11-25) | exists (P11, 11-13) |
| Guest names | `p11-limits`, `p11-rerun`, `p11-rerun-v2`, `p11-hook` (D-P11-25) | exists (P11, 11-06 / 11-07 / 11-12) |
| UI cookie / header | cookie `ostro_csrf`, header `X-Ostro-Csrf` (D-P11-25) | exists (P11, 11-15) |
| API error `details.reason` values | `run_not_terminal`, `rerun_cut_unsafe`, `rerun_nondeterministic`, `rerun_prefix_too_large`, `dlq_not_member`, `bulk_too_many_jobs`, `bulk_too_many_matches`, `limit_exceeded`, `index_rebuilding` (503 while the run index rebuilds, 11-11) (documented API vocabulary, not a registry space; D-P11-25) | exists (P11, 11-09 / 11-11) |
| Engine module | `run_limits` (run limits; the existing ENG-05 `limits.rs` is untouched) (D-P11-25) | exists (P11, 11-06) |
| REST paths | `GET\|PUT /v1/workflows/{name}/failure-policy`, `GET\|PUT /v1/queues/{name}/failure-policy`, `GET /v1/runs/{id}/rerun-cuts` (D-P11-29) | exists (P11, 11-09 routes; 11-10 / 11-12) |
| CLI verbs | `ostro rerun`, `ostro dlq ls\|requeue\|dismiss`, `ostro bulk cancel\|terminate\|retry\|rerun\|requeue\|pause\|resume\|signal\|ls\|status`, `ostro limits`; `ostro ls` flags `--tag` (repeatable), `--newest`, `--dlq` (11-14) | exists (P11, 11-14) |
| Crates | `ostro-worker`, `ostro-worker-proto` | P7 (07-02) |
| Config sections | `[workers]`, `[worker]`, `[cluster]` (P7 creates; P12 extends with `[[cluster.peers]]`, `replication_listen`), `[cluster.tls]`, `[api.tls]`, `[flow]` | P7 (07-02) |
| gRPC services | `ostro.worker.v1.Execute` (cluster listener, mTLS); `ostro.api.v1.Queues`, `ostro.api.v1.Workers` | P7 (07-02 / 07-11) |
| REST paths | `/v1/queues/{name}/flow`, `/v1/queues/{name}/pause`, `/v1/queues/{name}/resume`, `/v1/queues/{name}/demand`; `/v1/workflows/{name}/pause`, `/v1/workflows/{name}/resume`; `/v1/runs/{id}/pause`, `/v1/runs/{id}/resume`; `/v1/workers`, `/v1/workers/join`, `/v1/workers/{id}/drain`, `/v1/workers/tokens` | P7 (07-11 / 07-14) |
| Failpoint prefixes | `worker::`, `placement::`, `flow::` | P7 |
| Metric prefixes | `ostro_worker_`, `ostro_workers_`, `ostro_placement_`, `ostro_flow_`; the **value** of `ostro_queue_worker_demand{queue}` (named and described only by 08-16; set by 07-16 `workers/demand.rs`), plus `ostro_queue_worker_backlog` | P7 (07-05 / 07-16) |
| Id prefix / token scope | `wkr_` worker join tokens (`ApiToken{id: "wkr_<id>", scopes: ["worker:join"]}`) | P7 (07-14) |
| CLI verbs | `ostro worker` (+ `ostro worker token create\|list\|revoke`), `ostro queue flow\|pause\|resume\|demand`, `ostro run pause\|resume`, `ostro workflow pause\|resume` | P7 (07-15) |
| Data-dir entry | `<data_dir>/tls/` (`ca.crt`, `ca.key`, `node.crt`, `node.key`; keys 0600; cluster-scoped CA, D-W5) | P7 (07-06) |
| Cert SAN URIs | `ostro://<cluster_id>/worker/<token_id>/<worker_id>` (join-issued), `ostro://<cluster_id>/worker/ext/<worker_id>` (external CA), `ostro://<cluster_id>/node/<node_id>` (node identity) | P7 (07-06 / 07-09) |
| In-memory actions | `Action::RunPaused{run}`, `Action::RunResumed{run}` (informational, not persisted, not a RESERVATIONS space) | P7 (07-03) |
| Note | `ostro-worker-proto` field numbers are versioned by `proto_version` inside that crate and are **not** a RESERVATIONS space | P7 (D-W2) |
| gRPC services | `ostro.api.v1.Cluster`, `ostro.api.v1.ModuleLifecycle` (file `ostro/api/v1/cluster.proto`) (D-P13-23) | exists (P13; 13-01 row; 13-03) |
| gRPC services | `ostro.cluster.v1.Membership {Removed}`, `ostro.cluster.v1.NodeOps {HandOff, ModuleRefs}` (file `ostro/cluster/v1/membership.proto`) | exists (P13; 13-01 row; 13-03) |
| Proto field | `ostro.api.v1.MigrateRunsReport.already_on_target = 6` (repeated string, additive; REST/OpenAPI `already_on_target`); `ostro_migrate_runs_total{outcome="already_on_target"}` | exists (P13, p13-follow) |
| gRPC method | `ostro.cluster.v1.NodeOps.StartRestart` (messages `StartRestartRequest {partition 1, start_run 2, rate_limit 3, prefix_bound 4, lookup_only 5, workflow 6, idempotency_key 7}`, `StartRestartResponse {run_id 1, existing 2, rejected 3}`; appended to `membership.proto`, additive); metric `ostro_route_keyed_restarts_total{path}` | exists (P13, p13-follow) |
| gRPC method | `ostro.cluster.v1.NodeOps.DeliverInbound` (messages `DeliverInboundRequest {partition 1, batch 2}` (batch = frozen partition `Command` bytes holding the `InboundBatch`), `DeliverInboundResponse {outcome 1, rejected 2}` (outcome = `ApplyOutcome::code`); appended to `membership.proto`, additive); metric `ostro_route_inbound_forwards_total{result}`; module `ostro_server::p13::inbound` (`ClusterRouter`, `InboundRoute`, `NodeInbound`) | exists (P13, p13-bugs2) |
| REST paths | `POST /v1/cluster/{init,join,tokens,reassign,rebalance}`, `GET /v1/cluster/status`, `POST /v1/cluster/members/{id}/{remove,drain}`, `GET /v1/cluster/upgrade`, `POST /v1/cluster/upgrade/{finalize,hold,release}`, `POST /v1/runs/migrate`, `DELETE /v1/modules/{sha}`, `POST /v1/modules/gc` | exists (P13; 13-01 row; 13-03) |
| CLI verbs | `ostro cluster {init,join,remove,drain,status,token,reassign,rebalance,upgrade}`, `ostro migrate-runs`, `ostro modules {rm,gc}` | exists (P13; 13-01 row; 13-02 / 13-16) |
| Config keys | `[cluster]` `advertise`, `auto_finalize`, `join_timeout`, `join_orphan_timeout`, `promote_lag`, `handoff_grace`, `handoff_catchup_timeout`, `shutdown_handoff_timeout`, `leader_rebalance_interval`, `max_concurrent_moves`, `meta_max_message_bytes`; `[api]` `unkeyed_start_placement`, `strict_reads`, `lease_margin`; `[modules]` `gc_interval`, `gc_grace`, `gc_protect_resumable`; `[workers]` `version_policy` | exists (P13; 13-01 row; 13-02) |
| Failpoint prefixes | `member::`, `reassign::`, `route::`, `upgrade::`, `gc::`, `handoff::`, `migrate::` | exists (P13; 13-01 row) |
| Failpoints | `member::join_before_marker`, `member::join_after_marker`, `member::join_after_node_toml` (13-09), `member::init_after_ca` (13-16), `handoff::before_elect`, `handoff::after_elect` (13-11), `route::stale_view` (13-12), `gc::between_scans`, `gc::before_delete` (13-15), `gc::start_hold` (13-18; failpoints build, the start path: holds a start between its retired check and its submit for the start-during-GC race scenario) | exists (P13; recorded by 13-20) |
| Failpoints | `upgrade::mutant::emit_ungated`, `upgrade::mutant::reject_and_continue`, `member::mutant::skip_quorum_check`, `member::mutant::no_leave_joint`, `reassign::mutant::raise_min_insync_early`, `route::mutant::serve_without_lease`, `gc::mutant::single_scan`, `migrate::mutant::skip_seq_check` (names: `ostro_types::compat::MUTANTS`, D-P13-25) | exists (P13): names 13-01, sites placed by the owning plans (13-04, 13-09, 13-10, 13-13, 13-15, 13-17), all eight biting in CI (13-18 matrix, 13-20 step) |
| Metric prefixes | `ostro_cluster_`, `ostro_route_`, `ostro_module_gc_`, `ostro_migrate_` | exists (P13; 13-01 row; series listed in docs/cluster-operations.md §9) |
| Secret | `__ostro/cluster-ca` (cluster CA cert + key as a `SealedSecret`, plaintext-v0 until Phase 14, D-P13-10) | exists (P13; 13-01 row; 13-09 / 13-16) |
| Id prefix / token scope | `njt_` node join tokens, scope `node:join` (`ApiToken{id: "njt_<id>", scopes: ["node:join"]}`, D-P13-9) | exists (P13; 13-01 row; 13-09) |
| Data-dir file | `<data>/cluster-seeds.toml` `{node_id, cluster_id, seeds}` (durable join marker, written first by `ostro cluster join`, D-P13-9 / D-P13-29) | exists (P13; 13-01 row; 13-09) |
| Headers | `ostro-hop`, `ostro-partition`, `ostro-partitions`, `ostro-principal`, `ostro-consistency`, `ostro-read-after` | exists (P13; 13-01 row; 13-03 / 13-12 / 13-13) |
| Header | `ostro-deadline-ms` (REST remaining deadline in ms, the twin of `grpc-timeout`) | exists (P13; 13-03) |
| API error `details.reason` values | `feature_not_enabled` (every gate answers 409, D-13-18-1), `module_retired`, `module_gone`, `leader_changed` | exists (P13; 13-01 row) |
| API error `details.reason` values | `module_active` (409: retire refused while a version of the module is active, 13-15), `not_supported_in_profile` (409: `ostro cluster init` under `dev` / `--ephemeral`, 13-16) | exists (P13; recorded by 13-20) |
| Env vars | `OSTRO_TEST_LEVEL_MAX` (read only with `ostro-types/level-emulation`), `OSTRO_PREV_BIN`, `OSTRO_UPGRADE_ITERS`, `OSTRO_P13_REQUIRE_ALL` | exists (P13; 13-01 row) |
| Env vars | `OSTRO_P13_MUTANT` (13-18: run one row of the P13 mutant matrix locally), `OSTRO_P13_REPORT_DIR`, `OSTRO_CAMPAIGN_TIMEBOX_S`, `OSTRO_SHUTDOWN_REPEATS`, `OSTRO_P13_CHILD_RUNS` (13-19 tier C; `OSTRO_CAMPAIGN_FAULTS` is shared with P5/P12) | exists (P13; recorded by 13-20) |
| Cargo features | `level-emulation` (crate `ostro-types`; enabled only through the failpoints chains of `ostro-server`, `ostro`, `ostro-repl-conformance`), `wit-next-fixture` (crate `ostro-engine`) | exists (P13; 13-01 Task 2 manifests) |
| Crates | `ostro-build` (hermetic project builds, templates, toolchain cache), `ostro-client` (Rust client library; Phase 10 moves `ostro-cli/src/client.rs` into it) (D-P10-19) | exists (P10, 10-01 row; 10-20 verified) |
| Cargo features | `testing` (crate `ostro-client`: `TestEnv`); `polyglot` (crate `ostro-build` and the `ostro` binary crate; distinct from P9's `polyglot` on `ostro-conformance`, same name, separate crates) | exists (P10, 10-01 row; 10-20 verified) |
| Dirs | `clients/python`, `clients/ts`, `clients/go`, `crates/ostro-build/templates/`, `deploy/build-image/`, project build output `target/ostro/` (never `./.ostro`) | exists (P10, 10-01 row; 10-20 verified) |
| Project files | `ostro.toml` (project file: `[project]`, `[build]`, `[deploy]`; the server config has no default filename, it is passed by path), `target/ostro/manifest.json` (`{"kind":"ostro.manifest","version":1,...}`) | exists (P10, 10-01 row; 10-20 verified) |
| REST paths | `GET /v1/dev/time`, `POST /v1/dev/time/skip`, `POST /v1/modules/{sha}/replay` (REST-only, no gRPC twin, D-P10-18) | exists (P10, 10-01 row; 10-20 verified) |
| Config section | `[dev]` (key `time_skipping`) | exists (P10, 10-01 row; 10-20 verified) |
| REST error codes | `time_skipping_disabled`, `sample_too_large` | exists (P10, 10-01 row; 10-20 verified) |
| Env vars | `OSTRO_BIN` (already read by `crates/ostro/tests/common` with the same meaning; P10 makes it the test-kit binary override), `OSTRO_TOOLCHAIN_DIR`, `OSTRO_OFFLINE`, `OSTRO_TOOLCHAIN_MIRROR`, `OSTRO_POLYGLOT_BUILDER`, `OSTRO_DOCKER`, `OSTRO_LINUX_BIN` | exists (P10, 10-01 row; 10-20 verified) |
| Packages | `ostro-client` on PyPI, npm and crates.io (avoid the npm `@ostro` scope); Go module `github.com/lspecian/ostro/clients/go` (publishing human_needed) | exists (P10, 10-01 row; 10-20 verified) |
| Docker image | `ostro-build` | exists (P10, 10-01 row; 10-20 verified) |
| CLI exit code | `4` = replay divergence (CLI-06 codes 0–3 unchanged) | exists (P10, 10-01 row; 10-20 verified) |
| JSON kind prefixes | `ostro.<verb>.result`, `ostro.manifest` | exists (P10, 10-01 row; 10-20 verified) |
| Metric prefix | `ostro_dev_` | exists (P10, 10-01 row; 10-20 verified) |
| Ready line (frozen contract, parsed by the test kits) | `NAME listening on http://ADDR (data: DIR[, ephemeral]) ready in N ms` | exists (P10, 10-01 row; 10-20 verified) |
| CLI verbs | `ostro init`, `ostro build`, `ostro replay`, `ostro toolchain`, `ostro doctor`, `ostro completions` | exists (P10, 10-01 row; 10-20 verified) |

Phase 10 allocates no ordinal (F7); no P10 block. (10-01 gate 2026-11-22: `10-01-rescheck.py` RESERVATIONS CHECK: PASS at 2c8642a, output in 10-01-SUMMARY.)

**Phase 10 end gate 2026-11-23 (10-20):** every Phase 10 §11 row above is in code and reads `exists (P10, ...)` (`10-20-rescheck.py`, output in 10-20-SUMMARY: every ordinal space (Cmd/Intent/outbox/DlqUpdate oneof tags, StartRun field tags, TerminalStatus, EventKind, RejectReason, InvalidCode, MetaKind, MetaRecord, MetaCommand, TABLES, ext tags) is identical at the 10-01 baseline ca8a62e and at HEAD; no (10-NN) commit touched an ordinal file; RESERVATIONS CHECK: PASS). Phase 10 used no ordinal.
