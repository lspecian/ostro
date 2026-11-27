---
phase: 14-multi-tenancy-security-1-0-release
plan: 32
subsystem: Kafka topic-consumer trigger (ostro-triggers::kafka pure logic, ostro-server p14/kafka.rs driver)
tags: [kafka, trig-04, triggers, at-least-once, idempotency, offsets, crash-test, failover, docs]
requires:
  - phase: 14-01b
    provides: "p14::start_triggers seam (routed ControlPlane + PartitionRoles), TriggerKind::of_key (__kafka/ prefix), raw [triggers] kafka (RawTable::try_into_typed), kafka feature, test-services.sh Kafka broker"
  - phase: 14-31
    provides: "ostro-kafka transport: Client::connect, TLS, SASL PLAIN/SCRAM, ApiVersions negotiation"
  - phase: 14-34
    provides: "ostro-kafka read path: partitions, committed/commit (standalone group), list_offsets, fetch_at + Fetched::next_offset; testkit create_topic/produce"
provides:
  - "ostro_triggers::kafka: idempotency_key (offset / message_key), input (value_json / envelope), CommitTracker (start/skip_to/done/watermark), kafka_owner (pinned), is_topic_name, IdempotencyMode/InputMode"
  - "ostro_server::p14::kafka: validate(&[RawTable]) -> Vec<KafkaTrigger> (field-level errors), start_triggers(ctx, triggers, routed, roles) supervisor + per-partition consumers (feature kafka); start() seam body"
  - "tests: ostro-triggers tests/kafka.rs (12 incl. 512-case proptest), p14::kafka unit tests (10), ostro-server tests/kafka_trigger.rs (5, broker), ostro-repl-conformance tests/kafka_failover.rs (2, 3 processes + broker), ostro tests/kafka_crash.rs (#[ignore], kill -9)"
  - "docs/kafka-triggers.md"
affects: [14-27, 14-20]
tech-stack:
  added: []
  patterns:
    - "commit watermark = smallest in-flight offset, else the position; the position advances by Fetched::next_offset (control/aborted batches) via CommitTracker::skip_to"
    - "starts run inside ostro_api::auth::scope(Principal::system(), routed.start_run(..)) with ReturnExisting; a start is done when the API answers (applied = durable)"
    - "one supervisor task per node: led partitions (PartitionRoles::led) x topic partitions (Metadata every 30 s) -> one consumer task per owned (trigger, topic, kpartition); leadership loss cancels, grace 5 s, final commit"
    - "pause = an AtomicUsize of records in backoff/hold; no fetch while > 0"
key-files:
  created:
    - crates/ostro-triggers/src/kafka.rs
    - crates/ostro-triggers/tests/kafka.rs
    - crates/ostro-server/tests/kafka_trigger.rs
    - crates/ostro-repl-conformance/tests/kafka_failover.rs
    - crates/ostro/tests/kafka_crash.rs
    - docs/kafka-triggers.md
  modified:
    - crates/ostro-triggers/src/lib.rs (one `pub mod kafka;` line + table row; the `use base64 as _;` anchor removed, now used)
    - crates/ostro-server/src/p14/kafka.rs (stub replaced)
    - crates/ostro-server/src/lib.rs (deviation: the 14-01 `use ostro_kafka as _;` anchor removed, p14-notes rule)
    - crates/ostro-server/tests/p14_skeleton.rs (deviation: feature-aware start_triggers assertion, orchestrator request)
key-decisions:
  - "kafka_owner hashes with sha256 (first 8 bytes BE mod P over \"__kafka/\" ‖ name ‖ 0 ‖ topic ‖ 0 ‖ kpartition BE), not xxh3: ostro-triggers has no xxhash dependency and manifests are frozen; vectors pinned (cross-checked with Python hashlib)"
  - "Trigger names are capped at 116 bytes so the tag `ostro.kafka=<name>` fits MAX_RUN_TAG_LEN (128); the `ostro.kafka.topic=` tag is omitted when it would exceed 128 bytes"
  - "A [triggers.kafka.tls] table turns TLS on unless `enabled = false`"
  - "Error classes: 429/403/503/500/401 -> pause + backoff 1 s..30 s, retry the same record; 400/404/409/412/413/501 and non-JSON values -> on_invalid (pause holds and retries every 30 s with an ERROR per minute; skip counts ostro_kafka_skipped_total{reason})"
  - "Without the kafka feature a non-empty [[triggers.kafka]] fails startup: \"[[triggers.kafka]] is configured, but this ostro was built without the kafka feature\""
  - "The tests' Kafka crash invariants are checked at the trigger level (every run completed with its record's value; sampled histories: one run_started, one run_completed, strictly increasing seq): the ostro crate cannot depend on ostro-crashtest (no Cargo edits)"
requirements-completed: []
duration: ~3h
completed: 2026-11-27
---

# Phase 14 Plan 32: Kafka topic-consumer trigger Summary

**`[[triggers.kafka]]` in node.toml now starts one run per Kafka record: per-partition consumers on the leader of each `(trigger, topic, kafka partition)`'s owner partition, every record a `StartRunRequest` through the routed control plane as System with a `__kafka/` idempotency key and `ReturnExisting`, consumer-group offsets committed (standalone OffsetCommit) only behind a contiguous watermark of durable starts, 429/403/503 pausing the partition with 1 → 30 s backoff, `on_invalid` pause/skip for bad records, `ostro_kafka_*` metrics. Proven against Apache Kafka 4.1.2: a 5-test single-node suite, a three-process leadership-move test (SIGTERM handoff + kill -9) and a 20-iteration kill -9 crash test (6 kills held between a durable start and its commit): exactly one run per offset, no offset skipped, committed offsets = high watermarks.**

Requirement ids are not ticked here (14-27's job, D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| c822137 | 1 | feat: ostro_triggers::kafka (keys, input, CommitTracker, kafka_owner) + tests; p14/kafka.rs validation and driver; lib.rs anchors |
| 7379b08 | 2 | test: ostro-server tests/kafka_trigger.rs, ostro-repl-conformance tests/kafka_failover.rs |
| d8959fa | 3 | test: ostro tests/kafka_crash.rs; docs/kafka-triggers.md |
| ded9e5d | fix | ostro-server tests/p14_skeleton.rs: the start_triggers assertion follows the landed validation (orchestrator request, see Deviations 6) |

## Config schema as landed

```toml
[[triggers.kafka]]
name = "orders"                     # required; ^[A-Za-z0-9][A-Za-z0-9._-]{0,115}$, unique
bootstrap = ["host:9092"]           # required; host:port (port 1..=65535)
topics = ["orders"]                 # required; [A-Za-z0-9._-]{1,249}, not . / .., no duplicates
group_id = "ostro-orders"           # required; non-empty, no control chars
workflow = "order-flow"             # required; bare or "<tenant>/<name>" with the trigger's tenant
version = 3                         # optional, >= 1
queue = "q"                         # optional, bare or qualified like workflow
tenant = "_"                        # default "_" (default tenant), else ^[a-z0-9][a-z0-9-]{0,62}$
idempotency = "offset"              # offset | message_key
input = "value_json"                # value_json | envelope
start_from = "committed_or_latest"  # committed_or_latest | committed_or_earliest
max_in_flight = 256                 # 1..=10000
commit_interval = "1s"              # friendly duration, > 0, <= 1h
on_invalid = "pause"                # pause | skip
[triggers.kafka.tls]                # enabled (default true when present), ca_file (must exist), server_name
[triggers.kafka.sasl]               # mechanism PLAIN|SCRAM-SHA-256|SCRAM-SHA-512 (case-insensitive), username, password_secret
```

Unknown keys (also inside `tls`/`sasl`) are rejected by `deny_unknown_fields`; errors read
`triggers.kafka[<i>] ("<name>"): <field>: <message>`; duplicate names are rejected. The
server maps a validation error to `ServerError::Config("triggers: …")` through the 14-01b seam.

## Trigger marker

There is no trigger field on `StartRunRequest`: the key prefix `__kafka/` is the marker
(`TriggerKind::of_key` in the 14-01b start path → `IngressKind::Start { trigger:
Some(TriggerKind::Kafka) }`). Keys: `__kafka/<name>/<topic>/<partition>/<offset>`;
message-key mode `__kafka/<name>/k/<64 hex sha256(key)>` (null key → offset form); longest
valid key 419 bytes (test at the maximum lengths). Tags: `ostro.kafka=<name>`,
`ostro.kafka.topic=<topic>` (when ≤ 128 bytes), `ostro.kafka.partition=<n>`,
`ostro.kafka.offset=<n>`.

## Results (local, this machine, broker via scripts/test-services.sh, Kafka 4.1.2)

- `PROPTEST_CASES=512 cargo test -p ostro-triggers --test kafka`: 12 passed (proptest 512
  cases: the watermark never passes an open record, is monotone, ends at the position).
- `cargo test -p ostro-server --features kafka --lib -- p14::kafka`: 10 passed.
- `cargo test -p ostro-server --features kafka --test kafka_trigger -- --test-threads=1`:
  **5 passed, no SKIP**, 20 s (offsets → 500 runs + restart + 600-record burst; message-key 14 runs
  for 10 keys + 4 null keys; pause holds partition 0 at offset 3 while partition 1 commits 5,
  skip commits 7/5 with `ostro_kafka_skipped_total{reason="not_json"} 1` and
  `ostro_kafka_paused{partition="0"} 1` on /metrics; tenant `a` runs `a/smoke` in tenant a;
  429 for 2 s → paused, then 20/20 offsets, none skipped).
- `cargo test -p ostro-repl-conformance --test kafka_failover` (OSTRO_FAILPOINT_BIN = the
  failpoints debug binary): **2 passed, no SKIP**: 340 records over 2 Kafka partitions,
  owner partition 1 of 4: SIGTERM handoff node 2 → 3, then kill -9 node 3 → 2; exactly one
  run per offset, committed = high watermarks.
- `OSTRO_KAFKA_CRASH_ITERS=20 cargo test -p ostro --features failpoints --test kafka_crash
  -- --ignored`: **passed** (seed 1795771559893142995): 20 kills, 6 held at
  `kafka::after_start_before_commit` with 72–122 durable starts past the committed offsets
  each; 1683 records over 3 partitions → 1683 runs, exactly one per offset, 0 lost, 0
  duplicate, committed = high watermarks after a quiet period; 337 histories checked. The
  3-iteration verify run also passed (351 records).
- Throughput (debug build, single node P=4, `max_in_flight` 256, smoke guest, loaded
  12-core machine shared by 3 executors): a 600-record burst over 3 partitions in
  0.60–1.10 s across five runs = **545–998 records/s (≈ 180–330 records/s per partition)**,
  including produce time.
- `cargo build -p ostro --no-default-features`: ok. `cargo clippy -p ostro-server --features
  kafka --all-targets -- -D warnings`: clean; clippy `-D warnings` also clean for
  ostro-triggers + ostro-repl-conformance (`--all-targets`) and `ostro --features
  failpoints --test kafka_crash`. `scripts/p12-grep-gates.sh`: clean. Clock gate on the
  ostro-server/src diff since the p14 base: no `SystemTime::now(` / `SystemClock` /
  `Instant::now(` (commit interval and metric ages use `P14Ctx.clock`, waits use tokio
  time).
- musl binary (`cargo zigbuild --release -p ostro --target x86_64-unknown-linux-musl`, default features incl. `kafka`, measured by hand): **61,366,704 bytes = 61.37 MB** (statically linked, mimalloc) — under the 70 MB limit, +1.25 MB over 14-34's 60.12 MB; `scripts/binary-size-limit` unchanged.

## Leadership-move behaviour

The supervisor (one per node) recomputes the owned set on every `PartitionRoles::led`
change and once a second: owned = `kafka_owner(name, topic, kp, P) ∈ led`. A consumer
that is no longer owned is cancelled: it stops fetching, waits up to 5 s for in-flight
starts, abandons the rest (`select!` on the cancel token inside each start's retry loop),
commits the watermark of what is done and exits; the same key is not restarted on this node
until the old task has finished. The new leader reads the committed offset and re-reads
everything after it; re-read records find their runs (`outcome="existing"`). After a kill
-9 the re-read is bounded by `commit_interval` + `max_in_flight` per partition. Observed in
`kafka_failover`: on SIGTERM the stopping node committed its watermark ~30 ms after the
handoff (log "kafka consumer stopped … committed=Some(25)").

## Gate

**Plan-level gate incomplete — cluster withdrawn; consolidated gate pending on dell-4.**
`ostro-k8s-ci --parallel` was not run (cluster use ended 2026-11-27). Verified locally
instead: the plan's three verify blocks (with the broker from scripts/test-services.sh,
failing on SKIP) and the extra 20-iteration crash run listed above. **Unverified, for the
dell-4 consolidated gate:**

- the full ci-local `--parallel` run at the final commit (all groups: lint incl. `cargo fmt
  --check` and workspace clippy, test1/test2, musl1/musl2 incl. `musl zigbuild test`,
  failpoint suites, conformance, polyglot, docs);
- `cargo clippy -p ostro --all-targets -- -D warnings` (default features and
  `--no-default-features`) over every `ostro` test target (only `kafka_crash` was linted);
- the workspace test suites with the `kafka` feature unified (every Kafka test prints
  `SKIP:` there until 14-27 starts the broker in ci-local / GitHub CI);
- the Kafka suites on the CI broker (`kafka_trigger`, `kafka_failover`, a 5-iteration
  `kafka_crash` leg) — they passed only on this machine;
- the musl size step inside ci-local (measured by hand here, see Results).

Final-HEAD re-check at ded9e5d (main had moved by 29 files): every verify above re-run green (triggers 12, p14::kafka 10, kafka_trigger 5, kafka_failover 2 (350 records, handoff 1 → 2, kill -9 2 → 1), kafka_crash 3 iterations (198 records), slim build, clippy ostro-server `--features kafka --all-targets -D warnings`, clock gate).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `kafka_owner` uses sha256, not the xxh3 family of `schedule_owner`**
- **Found during:** Task 1
- **Issue:** ostro-triggers (pure crate) has no xxhash dependency and the plan forbids Cargo edits.
- **Fix:** sha256 over the same kind of prefixed input (first 8 bytes BE mod P), vectors pinned and cross-checked in Python; the failover test re-implements it (ostro-repl-conformance does not depend on ostro-triggers) and checks the same vectors.
- **Commit:** c822137

**2. [Rule 3 - Blocking] `crates/ostro-server/src/lib.rs` edited (14-01b shared file)**
- **Found during:** Task 1
- **Issue:** the 14-01 anchor `#[cfg(feature = "kafka")] use ostro_kafka as _;` is obsolete once p14/kafka.rs uses the crate; the p14-notes say to delete such a line when the crate is really used.
- **Fix:** 4 lines removed, nothing else.
- **Commit:** c822137

**3. [Rule 2 - Missing critical] name length cap and topic-tag omission**
- **Found during:** Task 1
- **Issue:** `ostro.kafka=<name>` with a 128-byte name and `ostro.kafka.topic=<249-byte topic>` exceed `MAX_RUN_TAG_LEN` (128) and the start would fail with 400 for every record.
- **Fix:** names ≤ 116 bytes (validated), the topic tag is left out when too long (unit test `tags_fit`).
- **Commit:** c822137

**4. [Rule 3 - Blocking] driver landed with Task 1's commit**
- p14/kafka.rs (validation + driver) is one file; it was committed whole in Task 1, its broker tests in Task 2.

**5. [Rule 1 - Bug] crash/trigger test details**
- The first suite version deployed the guest after the triggers started: records before the deploy were `unknown_workflow` invalid records, held (pause, retried after 30 s) or, with `skip`, skipped — a flaky `invalid_records_pause_or_skip` (0 runs). The suite now deploys first and restarts with the triggers (`deployed()` helper); throughput is measured on a 600-record burst. docs/kafka-triggers.md warns to deploy the workflow before enabling a `skip` trigger. `kafka_failover` waits for an owner leader different from the killed node (meta still names it right after the kill).

**6. [Rule 1 - Bug, orchestrator request] `p14_skeleton::start_triggers_is_none_without_kafka_and_refuses_a_kafka_list` (14-01b's test, not in files_modified)**
- **Found by:** 14-17 (ostro-server built alone without `kafka`)
- **Issue:** the test expected the stub message "Kafka triggers land in 14-32", which c822137 replaced.
- **Fix:** the assertion is feature-aware: with `kafka` the incomplete table fails with `triggers.kafka[0] ("orders"): bootstrap: is required`, without it with "built without the kafka feature". `p14_skeleton` passes 7/7 with and without `--features kafka`.
- **Commit:** ded9e5d
- **Left for the orchestrator:** the module doc of `crates/ostro-server/src/p14/mod.rs` (14-01b-only) still says a non-empty list fails startup "Kafka triggers land in 14-32" (line 34) and that `start_triggers` is a stub; doc-only, not edited.

### Plan items done differently

- Crash invariants: the Phase 5 crash checker lives in ostro-crashtest, which the `ostro` crate cannot depend on (no Cargo edits); `kafka_crash` checks the equivalent at the trigger level (every run completed with its record value; sampled histories with exactly one `run_started` / `run_completed` and strictly increasing seq; exactly one run per offset; no run without a record).
- The 20-iteration `kafka_crash` run and the `kafka_trigger` run were done locally (cluster withdrawn), not with `ostro-k8s-ci --cmd`.

## Proposed for 14-27 (not applied: ci-local and ci.yml are 14-27's)

`scripts/ci-local.sh` (step after the failpoint suites; the broker is the loopback one from
14-01b):

```bash
check_kafka_trigger() {
  bash "$ROOT/scripts/fetch-tools.sh" kafka jre >/dev/null || return 1
  bash "$ROOT/scripts/test-services.sh" start kafka || return 1
  eval "$(bash "$ROOT/scripts/test-services.sh" env)"
  local rc=0 out
  out=$(cargo test -p ostro-server --locked --features kafka --test kafka_trigger -- --test-threads=1 2>&1) || rc=1
  echo "$out" | tail -n 3; grep -q "SKIP:" <<<"$out" && rc=1
  cargo build -p ostro --features failpoints --locked || rc=1
  out=$(OSTRO_FAILPOINT_BIN="${CARGO_TARGET_DIR:-target}/debug/ostro" \
    cargo test -p ostro-repl-conformance --locked --test kafka_failover -- --test-threads=1 2>&1) || rc=1
  echo "$out" | tail -n 3; grep -q "SKIP:" <<<"$out" && rc=1
  out=$(OSTRO_KAFKA_CRASH_ITERS=5 cargo test -p ostro --locked --features failpoints \
    --test kafka_crash -- --ignored --test-threads=1 2>&1) || rc=1
  echo "$out" | tail -n 3; grep -q "SKIP:" <<<"$out" && rc=1
  bash "$ROOT/scripts/test-services.sh" stop
  return $rc
}
step "kafka trigger (broker)" check_kafka_trigger
```

`.github/workflows/ci.yml` (linux-x86_64-gnu leg, after the `apache/kafka:4.1.<z>` docker
step that exports `OSTRO_TEST_KAFKA_BOOTSTRAP`; or `scripts/test-services.sh start kafka`
per the p14-notes):

```yaml
      - name: Kafka trigger suites
        env:
          OSTRO_KAFKA_CRASH_ITERS: "5"
        run: |
          set -euo pipefail
          cargo test -p ostro-server --locked --features kafka --test kafka_trigger -- --test-threads=1 | tee kt.log
          cargo build -p ostro --features failpoints --locked
          OSTRO_FAILPOINT_BIN="$PWD/target/debug/ostro" cargo test -p ostro-repl-conformance --locked --test kafka_failover -- --test-threads=1 | tee kf.log
          cargo test -p ostro --locked --features failpoints --test kafka_crash -- --ignored --test-threads=1 | tee kc.log
          if grep -q "SKIP:" kt.log kf.log kc.log; then echo "a Kafka test skipped"; exit 1; fi
```

## Hand-off

- **14-20:** Kafka starts reach `admit_ingress` as `IngressKind::Start { trigger: Some(TriggerKind::Kafka) }` (derived from the key); a 429 / 403 answer pauses the partition and retries, so quota rejection must not be counted as a client error by the trigger (it is not).
- **14-27:** tick TRIG-04; add the hunks above; `docs/kafka-triggers.md` is new (docs index if there is one).
- **Orchestrator:** the musl binary size (Results) is under 70 MB; no change to `scripts/binary-size-limit`.

## Self-Check

PASSED — files exist (crates/ostro-triggers/src/kafka.rs, crates/ostro-triggers/tests/kafka.rs, crates/ostro-server/src/p14/kafka.rs, crates/ostro-server/tests/kafka_trigger.rs, crates/ostro-repl-conformance/tests/kafka_failover.rs, crates/ostro/tests/kafka_crash.rs, docs/kafka-triggers.md); commits c822137, 7379b08, d8959fa, ded9e5d are on main.
