---
phase: 14-multi-tenancy-security-1-0-release
plan: 04
subsystem: audit journal (node-local, hash-chained, fail-closed)
tags: [audit, sec-04, hash-chain, group-commit, fail-closed, otlp, retention, crash-test, tdd]
requires:
  - phase: 14-01
    provides: "ostro_types::audit contract (AuditSink reserve/commit, PermitSlot, CoalesceKey, AuditUnavailable, AuditCommitError)"
  - phase: 14-01b
    provides: "p14 seam (build_ctx -> audit::writer::open, start -> audit::writer::start, before_shutdown -> audit::writer::flush), [audit] config, main.rs `ostro audit verify` intercept, audit/* stubs"
provides:
  - "crates/ostro-server/src/audit/writer.rs: AuditWriter implementing AuditSink (permits held until durable, group commit, fail closed, coalescing, ticks) + the seam functions"
  - "crates/ostro-server/src/audit/verify.rs: verify_dir / verify_dir_against(head) + `ostro audit verify` (offline)"
  - "crates/ostro-server/src/audit/{journal,chain,segment,export}.rs: record format, chain, segment files/recovery/retention, OTLP + chain-head export"
affects: [14-18, 14-22, 14-23, 14-24, 14-25, 14-26]
tech-stack:
  added: []
  patterns:
    - "permit = a slot held from reserve until the record is fsynced; the queue bound is on not-yet-durable records"
    - "the writer is a dedicated std thread (blocking fsync off the runtime); commits resolve through a tokio oneshot"
    - "the seam's open has no clock: the writer holds work until set_clock (start/flush pass P14Ctx.clock); a process-wide registry (sink data pointer -> Weak<AuditWriter>) maps P14Ctx.audit back to the writer"
key-files:
  created:
    - crates/ostro-server/tests/audit_journal.rs
    - crates/ostro-server/tests/audit_crash.rs
  modified:
    - crates/ostro-server/src/audit/{journal,chain,segment,writer,verify,export}.rs
    - crates/ostro-server/src/logging.rs (deviation: otel_logger_provider accessor)
key-decisions:
  - "Every process start opens a new segment (segment_open first); audit_recovered follows it when open truncated a torn tail"
  - "A valid record after an invalid region is a modified journal, not a torn tail: open refuses it (startup error naming `ostro audit verify`) instead of truncating evidence"
  - "Genesis = sha256(\"ostro-audit-genesis\" || node u64 BE || incarnation u64 BE); hash input = the 32 raw prev bytes || canonical JSON"
  - "A line is valid only if byte-identical to the re-encoding of its parsed fields (canonical check), then hash-checked"
  - "Retention age test for segment i uses segment i+1's segment_open ts as the upper bound of i's records (no full-segment reads)"
  - "audit::writer_stall `return` marks the writer stalled (reserves fail after reserve_timeout, nothing written); pause/sleep stall the thread itself (queue fills)"
  - "OTLP export goes through the installed Phase 8 logger provider (new logging::otel_logger_provider), not tracing events, so details can be flattened into dynamic attribute keys"
requirements-completed: []
duration: ~2.5h
completed: 2026-11-26
---

# Phase 14 Plan 04: Hash-chained audit journal Summary

**Node-local `<data>/audit/audit-<seq 20d>.jsonl` journal with a sha256 chain across records and segments, a group-committing writer whose permits are held until fsync (fail closed on stall, full queue or a write error), torn-tail recovery, whole-segment retention behind a `retention_trim` anchor, offline verification naming the first bad seq, coalesced high-volume events, a periodic chain-head export and OTLP LogRecord export through the Phase 8 pipeline.**

Requirement ids are not ticked here (14-27's, D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| 2f8bf0d | 1+2 RED | test: audit_journal.rs (all non-crash cases, incl. failpoint module) + audit_crash.rs |
| 04295ea | 1 GREEN | feat: journal/chain/segment/verify (format, chain, rotation, recovery, retention, verify) |
| 0041873 | 2 GREEN | feat: writer/export + logging accessor (permits, group commit, poison, coalescing, ticks, head export, OTLP) |

No REFACTOR commit (none needed). The RED commit precedes both GREEN commits; the three were CASed together so main never had a non-compiling head.

## Record schema as landed

One line per record, canonical JSON (keys in exactly this order, no whitespace, serde_json escaping, `details` sorted, optional coalescing fields omitted when absent, `hash` last and excluded from the hashed bytes):

```text
{"v":1,"seq":<u64>,"ts_ms":<u64>,"node":<u64>,"incarnation":<u64>,"actor":"…","actor_kind":"token|system|worker|node|dev|anonymous",
 "tenant":"…"|null,"action":"…","target":"…","outcome":{"result":"ok"}|{"result":"denied|failed","reason":"…"},
 "request_id":"…"|null,"details":{"k":"v",…}[,"count":n,"first_ts_ms":t,"last_ts_ms":t],"prev_hash":"<64 hex>","hash":"<64 hex>"}
```

- `hash = hex(sha256(raw(prev_hash) ‖ canonical_without_hash))`; first record of a node: `prev_hash = sha256("ostro-audit-genesis" ‖ node_be64 ‖ incarnation_be64)`.
- Control records (actor `system`, actor_kind `system`, target `""`, fields in `details` as strings): `segment_open{prev_segment_last_hash}` (first record of every segment, = its `prev_hash`), `audit_recovered{truncated_bytes, last_good_seq}`, `retention_trim{first_kept_seq, first_kept_hash}`.
- `seq` starts at 1 and is contiguous across segments and incarnations; a segment file is named by the seq of its `segment_open`.
- `outcome` is `ostro_types::audit::AuditOutcome`'s serde form (tag `result`, content `reason`).

## Writer contract (for 14-18)

- `reserve()` takes a permit if fewer than `[audit] queue` records are reserved-and-not-durable and the writer is not stalled; otherwise waits up to `reserve_timeout` (inside a multi-thread runtime through `block_in_place`), then `AuditUnavailable` → 503 `audit_unavailable`. Poisoned or closed → immediate `AuditUnavailable`.
- `permit.commit(ev)` resolves after the batch fsync; on a write/fsync error it returns `AuditCommitError` (→ 500 `audit_commit_failed`, `details.executed = true`) and the writer stays poisoned until restart. Dropping a permit releases it.
- `record_coalesced(ev, key)` never blocks; the event time is taken at call time (injected clock); one record per key and window (`secret_coalesce` for `SecretAccess`, `auth_failure_coalesce` for `AuthFailure`) carrying `count`, `first_ts_ms`, `last_ts_ms` (the first event's other fields); flushed by the 1 s tick when the window ends and by `flush()` (shutdown). More than `queue` pending coalesced messages are dropped (`ostro_audit_coalesced_dropped_total`); more than `queue` distinct keys force a summary flush.
- Seam: `build_ctx` → `open` (torn-tail repair now; `NoopAuditSink` when `[audit] enabled = false`); `start` → hands the writer `P14Ctx.clock` (it then writes the incarnation's `segment_open` [+ `audit_recovered`]) and spawns the tick task; `before_shutdown` → `flush` (coalesced records + chain head); `P14Services::stop` → final flush and `close` (thread joined, later reserves fail).
- Metrics: `ostro_audit_records_total`, `ostro_audit_fsyncs_total`, `ostro_audit_unavailable_total`, `ostro_audit_coalesced_dropped_total`, `ostro_audit_export_dropped_total`, gauge `ostro_audit_chain_seq`.

## Group-commit measurements

- `writer_group_commit_batches_concurrent_committers` (32 concurrent committers, 20 ms window, tmpfs): **32 records / 1 fsync (1 batch)**, three runs out of three.
- Crash test child (8 committers, 2 ms window, /tmp on WSL ext4): 16,836 acknowledged records in 50 runs of 50–450 ms each.

## Crash test report (`audit_crash.rs`, release + failpoints, `OSTRO_AUDIT_CRASH_ITERS=50`)

```text
iter   1: kill after  77 ms,    95 acked, journal 550 records / 6 segments
…
iter  49: kill after 355 ms,   351 acked, journal 17309 records / 173 segments
audit crash test: 50 iterations, 16836 acknowledged records, all present; 0 torn-tail recoveries (0 bytes truncated); final journal 17309 records in 173 segments, seq 1..17309
test result: ok. 1 passed; 0 failed; … finished in 15.01s
```

Every iteration: the child (this test binary re-executed) commits through the writer and prints `ACK` after each resolved commit; the parent SIGKILLs it at a seeded random moment, reopens the journal, `verify_dir` passes and every acknowledged target is present. **Note:** kill -9 never produced a torn tail here, because a batch is one `write()` of a regular file (atomic with respect to process death on Linux); torn tails (mid-line cut, appended garbage, zero bytes, torn `segment_open`) are covered deterministically by the `torn_*` tests. Power-loss tears are out of reach without a VM.

## OTLP attribute mapping

LogRecord per committed record (only when `[audit] otlp` and `[telemetry] otlp_endpoint` + `logs`): `event.name = "ostro.audit"`, target `ostro::audit`, severity INFO, timestamp/observed = `ts_ms`, body = `action`.

| Attribute | Value |
|---|---|
| `ostro.audit.v`, `.seq`, `.ts_ms`, `.node` | int |
| `ostro.audit.incarnation` | string (random u64, may exceed i64) |
| `ostro.audit.actor`, `.actor_kind`, `.action`, `.target` | string |
| `ostro.audit.tenant`, `.request_id` | string, omitted when null |
| `ostro.audit.outcome` | `ok` / `denied` / `failed` |
| `ostro.audit.outcome_reason` | string, only for denied/failed |
| `ostro.audit.count`, `.first_ts_ms`, `.last_ts_ms` | int, only on coalesced records |
| `ostro.audit.prev_hash`, `.hash` | 64-hex string |
| `ostro.audit.detail.<key>` | each `details` entry (no `ostro.audit.details`) |

Chain head (every `head_export_interval`, measured on the injected clock from the writer start, and at shutdown): INFO log line `chain_head` (target `ostro::audit`, fields node, incarnation, seq, hash), gauge `ostro_audit_chain_seq`, LogRecord `event.name = "ostro.audit.head"` with `ostro.audit.node/incarnation/seq/hash`. Export goes through a 1,024-item channel to a dedicated thread; a full channel drops the copy (`ostro_audit_export_dropped_total`), never the record.

## Verification

Local (worktree, `CARGO_TARGET_DIR=target/slot-5`):
- Task 1 verify (`--test audit_journal -- chain tamper rotate torn retention ignored_by`): 13 passed.
- Task 2 verify: `--test audit_journal` 21 passed; `--features failpoints --test audit_journal -- --test-threads=1` 24 passed; release crash test (50 iterations, above) passed; CLOCK_GATE_SCOPED clean.
- `cargo clippy -p ostro-server --all-targets [--features failpoints] -- -D warnings` clean; `cargo fmt --check` clean; `scripts/p12-grep-gates.sh` clean; lib `audit::` unit tests 6 passed; `p14_skeleton`, `config`, `bootstrap`, `data_dir_lock` and `ostro dev_startup` (8 runs) green.

Plan gate `ostro-k8s-ci --parallel` at 0041873 (run p174324-7007, 194 min wall on a congested cluster):

| Group (node) | Result |
|---|---|
| fp1, fp2, fp3 (dell-2) | PASS: failpoint suites 2445 s, replication mutant matrix 281 s, server campaign smoke (20 kills) 1188 s |
| musl1 (dell-2) | PASS: musl release 936 s, **musl binary size limit PASS**, musl zigbuild test shard 642 s |
| test1 (dell-2) | FAIL: only `ostro dev_startup::default_data_dir_is_dot_ostro_and_persists` (/readyz not 200 within 60 s, under load) |
| lint, poly, obuild, test2, musl2 (dell-1) | infrastructure: the lint container never started (gave up after 10 min), poly/obuild/musl2 SIGKILLed (exit 137), test2's pod vanished during an API outage ("database is locked"). No test failure in any of them |

Reruns on slot 1 at the same commit:
- `dev_startup` alone: 4 passed (20261126-200505-s1-00418731-cmd.log).
- the lint group's steps (`CI_LOCAL_SKIP` = every step another group names): **all PASS**: toolchain, fmt, clippy -D warnings, warning-free build, sdk fmt/clippy/test, guests, order-saga, semantics sleepers, failpoint list sync, Phase 13 mutant matrix (603 s), wit-next fixtures, Phase 13 tier C (1168 s), phase 7 failpoint legs, cargo deny, vendored/deny fixtures, grep gates, workflow validation (20261126-200828-s1-00418731-cmd.log).
- full `cargo test` + polyglot + ostro-build + musl (`CI_LOCAL_ONLY`): polyglot toolchains, python/ts sdk, polyglot conformance (374 s), clippy (polyglot), clients python/ts/go, ostro-build polyglot (966 s), musl release, **full musl zigbuild test (3549 s) PASS**; `cargo test` FAIL only on `ostro flow_e2e::auto_pause` (PUT module timed out under load; `flow_e2e::*` is on the known load list) (20261126-220838-s1-00418731-cmd.log).
- `flow_e2e` alone: 12 passed incl. `auto_pause`.

Net: no failure attributable to 14-04; every step passed at 0041873 either in the parallel run or in an alone/slot-1 rerun.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `crates/ostro-server/src/logging.rs`: `otel_logger_provider()` accessor (not in files_modified)**
- **Found during:** Task 2 (OTLP export).
- **Issue:** P14Ctx has no handle on the Phase 8 `SdkLoggerProvider` (server.rs owns the TelemetryGuard and is 14-01b-only), and tracing events cannot carry the dynamic `ostro.audit.detail.<k>` keys.
- **Fix:** the installed OTel layer generation also keeps a clone of its logger provider; `pub(crate) fn otel_logger_provider()` returns it (10 added lines, nothing else changed). The audit export emits LogRecords through it.
- **Commit:** 0041873

**2. [Rule 2 - Missing critical functionality] clock deferral in the seam**
- **Found during:** Task 2. `audit::writer::open(cfg, data_dir, node, incarnation)` (fixed by 14-01b) has no clock, but `ts_ms` must come from the injected clock. The writer repairs the tail at open and holds every commit until `start`/`flush` hand it `P14Ctx.clock`; reserves already work before that. No seam edit needed.

### Notes

- `record_coalesced` events are stamped at call time (the writer thread may get to them later); found by the coalescing test.
- The plan's verify ran `OSTRO_AUDIT_CRASH_ITERS=10`; the 50-iteration run above supersedes it.

## Workflow hunks (branch `wip/14-04-workflows`, e4dc375, on top of 0041873)

The failpoint cases of `audit_journal` only run with `--features failpoints`; CI needs both lists in sync (`check_failpoint_sync`):

```diff
--- a/scripts/ci-local.sh
-FAILPOINT_CRATES=(… ostro-server:p13_module_gc ostro-server:p13_promote)
+FAILPOINT_CRATES=(… ostro-server:p13_module_gc ostro-server:p13_promote ostro-server:audit_journal)
--- a/.github/workflows/ci.yml   (step "Failpoint suites", after the p13_promote line)
           cargo test -p ostro-server --features failpoints --test p13_promote --locked -- --test-threads=1
+          cargo test -p ostro-server --features failpoints --test audit_journal --locked -- --test-threads=1
```

The `ostro-k8s-ci` runner may need the new step name `ostro-server:audit_journal` in its group map. The crash test stays manual (`#[ignore]`, like ostro-crashtest's loops).

## Orchestrator actions / decisions

- Apply `wip/14-04-workflows` (ci-local.sh + ci.yml together) and teach the runner the new failpoint step.
- Decision recorded (reversible): a journal modified mid-segment makes server startup fail with a message pointing to `ostro audit verify` (we do not truncate evidence). The alternative is to start a fresh journal next to the damaged one and log an error.
- For 14-18: the writer contract above; `audit_commit_failed` comes from `AuditCommitError`; worker/auth events should use `record_coalesced` with `CoalesceKey::AuthFailure` for authentication failures.
- For 14-24/14-25: residual risk (a node-local attacker can rewrite the journal since the last exported head) and `verify_dir_against(head)` as the off-node check; backups do not include `audit/` (F8, tested).

## Self-Check

- FOUND: crates/ostro-server/src/audit/{journal,chain,segment,writer,verify,export}.rs, crates/ostro-server/tests/{audit_journal,audit_crash}.rs
- FOUND commits on main: 2f8bf0d, 04295ea, 0041873; branch wip/14-04-workflows at e4dc375

## Self-Check: PASSED
