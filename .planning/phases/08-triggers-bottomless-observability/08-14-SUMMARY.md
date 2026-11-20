---
phase: 08-triggers-bottomless-observability
plan: 14
subsystem: triggers
tags: [cron, schedules, leader-service, catch-up, watermark, meta-cas, rest, grpc, docs]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: "08-01 ScheduleSpec/ScheduleSource/CronConfig contract + NodeConfig.{cron, schedule_source}; 08-05 CronSpec, next_after, latest_at_or_before, fires_in, plan_catchup; 08-07 ScheduleFire apply (watermark, overlap, manual, advance, W4); 08-10 ScheduleApi + DTOs + controller stub + ControlCtx; 08-12 CLI verbs"
provides:
  - "ostro_partition::leader::cron: cron driver leader service (spawn, CronService), ScheduleSource::build_start (default_start), FireRequest, StartError, schedule_state(), incarnation_watermark(), parse_spec(), next_fire_after(), schedule_tag()"
  - "LeaderConfig.cron, filled by Node::open from NodeConfig.{cron, schedule_source}"
  - "ostro_server::control::schedules: ScheduleController (ScheduleApi), MetaScheduleSource, build_schedule_start, schedule_spec"
  - "docs/cron.md"
affects: [08-20, 08-22]

tech-stack:
  added: ["ostro-triggers as an ostro-partition path dependency (one Cargo.lock line, no new package)"]
  patterns:
    - "Leader service with a (due, name) BTreeSet window polled on the injected clock at <= 50 ms real time (timers.rs style)"
    - "Catch-up via plan_catchup on activation and whenever a live driver is behind by more than one fire"
    - "Partition-side driver delegates StartRun construction to the source (server resolves meta like the API start)"

key-files:
  created:
    - crates/ostro-partition/tests/cron_driver.rs
    - crates/ostro-server/tests/schedules.rs
    - docs/cron.md
  modified:
    - crates/ostro-partition/src/leader/cron.rs
    - crates/ostro-partition/src/leader/mod.rs
    - crates/ostro-partition/src/node.rs
    - crates/ostro-partition/Cargo.toml
    - Cargo.lock
    - crates/ostro-server/src/control/schedules.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/tests/phase8_smoke.rs

key-decisions:
  - "ScheduleSource gains build_start (default impl, so the 08-01 contract stays source-compatible): ostro-partition has no meta access, so the server builds the full API-equivalent StartRun (D23 names, version, queue, module pin, timeout, D9 policies, pre-encoded RunStarted)"
  - "A live driver that is behind by more than one fire (clock jump, stalled task) runs the catch-up path instead of firing every missed instant, so the catch-up policy and the O(cap) bound also hold without a restart"
  - "The watermark the driver reads is incarnation-aware (0 when the row belongs to another created_at_ms); the floor is max(watermark, created_at_ms) (W6)"
  - "Activation at an instant that is itself a fire time treats that fire as missed (catch-up policy decides; Skip advances it)"
  - "A manual trigger skipped by overlap Skip answers 409 schedule_overlap; a buffered one returns the run id that starts when the live run ends"
  - "A fire whose StartRun cannot be built permanently (workflow deleted) is dropped and counted start_error; transient build/submit errors retry with backoff and the same command"
  - "The ostro.schedule=<name> tag is cut to MAX_RUN_TAG_LEN (128) bytes for names over 113 bytes"

requirements-completed: [TRIG-02, TRIG-03]

duration: ~2h
completed: 2026-11-20
---

# Phase 8 Plan 14: Cron driver and schedules controller Summary

**Each partition's leader now drives the schedules it owns (`schedule_owner(name, P)`). It fires `ScheduleFire` at the nominal instants on the injected clock and catches up per policy after downtime in bounded work. Pause, resume and trigger-now work. The server serves schedule CRUD with cron and time-zone validation and meta CAS over REST and gRPC, backed by a meta-subscribed `ScheduleSource`. TRIG-02 and TRIG-03 are complete.**

## Driver (crates/ostro-partition/src/leader/cron.rs)

- **Spawned** by `leader::spawn_all` when `LeaderConfig.cron` is `Some`. `Node::open` fills it from `NodeConfig.{cron, schedule_source}` (a two-line edit in node.rs, see Deviations).
- **Owner filter and pause:** the driver keeps `schedule_owner(name, P) == partition` and drops paused specs from the window.
- **Activation** happens at leadership start, for a new schedule, for a new incarnation and on resume:
  - read the `schedule_state` row; its watermark counts only if the row has the same `created_at_ms`, else it is 0;
  - `plan_catchup(spec, watermark, created_at_ms, now, policy, window, cap)`;
  - Skip submits `Advance(advance_to, skipped)`, One and All submit `Fire` for each planned instant;
  - then seed the window with `next_after(max(floor, now))`.
- **Steady state:**
  - The window is a `BTreeSet<(due, name)>`. The driver sleeps until the earliest due, at most 50 ms of real time, and also wakes on a source change or shutdown.
  - When an entry is due and the next fire is still in the future, it submits a `Fire`.
  - When the driver is behind by more than one fire, it runs catch-up instead.
- **Spec changes** in the same incarnation re-seed from the floor, so the change applies to future fires only. Deletes leave the window.
- **Submit:**
  - The run id is `RunId::new_v7(partition)` and the key is `cron_idempotency_key`.
  - `StartRun` comes from `ScheduleSource::build_start`.
  - Every fire carries `overlap` and `schedule_created_at_ms` (W4).
  - `NotLeader`, `Failed` or an unhealthy partition stop the driver. Other errors are retried with backoff from 50 ms up to 1 s, with the same command.
- **Metric:** `ostro_cron_fires_total{schedule,outcome}`. The outcome is `started`, `buffered` or `skipped`, read from the row after an Accepted fire, or `advanced`, `duplicate`, `rejected` or `start_error`.

### Timing accuracy under FakeClock (`ManualClock`)

- **Fire times are exact.** `fire_time_ms` is always the cron instant, never the observed clock value. `tests/cron_driver.rs` asserts the exact 10 s boundaries, and the server tests assert exact instants as well.
- **Detection latency is at most one 50 ms real-time poll** after the fake clock reaches the instant, plus the submit and apply time.
- **Between boundaries nothing fires.** The test sets the clock to boundary − 1 ms and asserts no run.

### Catch-up behaviours (asserted)

| Case | Result |
|---|---|
| `*/10` UTC, 5 boundaries | exactly one run per boundary; watermark = last boundary |
| 4 crash restarts (WAL-only crash image; state rebuilt by replay) | every fire_time has exactly one run |
| then 5 min + 3 s downtime, policy One | exactly one new run, at `latest <= now` |
| Skip after 5 min downtime | 0 runs; watermark = latest; `skipped >= 30`; steady state resumes at the next boundary |
| All, window 60 s, cap 3, 5 min downtime | exactly 3 runs, the 3 newest fire times `<= now` |
| `* * * * * *`, 2 days of downtime (172,800 fires), One | exactly one run, at `latest <= now`; bounded work (no enumeration) |
| delete then re-create the same name (orphaned watermark) | no backfill; the next boundary fires; the row is reset to the new incarnation |
| P=4, one schedule per partition | each partition holds only its own run; no partition applied a foreign schedule's fire |
| pause | no fires, watermark kept; resume with One fires the latest missed instant |

## Server (crates/ostro-server/src/control/schedules.rs)

- **Source mechanism: subscription.** `MetaScheduleSource::start(meta_view, runs_defaults)` publishes the current set. It spawns a watcher on `MetaView::subscribe()` (the applied-index watch), re-reads `schedules()` after each applied entry and calls `send_if_modified` only when the set changed. Dropping the source aborts the watcher. `server.rs` sets `node_cfg.schedule_source` before `Node::open`.
- **`build_start`** builds the run the same way the API start path does:
  - the workflow name through `policy_key` (D23), with the tenant taken from its prefix;
  - the pinned or active version, the version's default queue, `module_sha` and `run_timeout_ms`;
  - `resolve_run_policy` (D9), the getrandom seed, `prepare_payload`, then `attach_run_started` (D24);
  - the tag `ostro.schedule=<name>`.
- **CRUD:**
  - Create validates with `CronSpec::parse_bounded`, which rejects an invalid expression, one that never fires, an unknown zone or bad bounds with 400 and the engine's message. It also checks that the workflow exists (and the pinned version, when given). Then it does `Put(None)`; an existing name is 409.
  - Update and delete use CAS on `version_tag`; stale is 409 `version_mismatch`. Update keeps `created_at_ms` and `ext`.
  - Pause and resume are CAS read-modify-write, idempotent, retried on a concurrent change.
- **Get and list** report:
  - `next_fire_at_ms` = `next_after(max(watermark, created_at, now))`, or null while paused;
  - `last_fire_at_ms` = the incarnation's watermark, or null before the first fire.
- **Trigger:**
  - Submits `ScheduleFire{Manual, fire_time = now, request_id = given bytes or 16 random}` to the owner partition, with key `cron_manual_idempotency_key`.
  - Accepted and started (or buffered) returns the run id. Accepted but skipped by overlap is 409 `schedule_overlap`. `Duplicate{existing}` returns the existing run.

### tests/schedules.rs (ManualClock via `ServerOptions::with_clock`, signals guest)

- **Overlap:**
  - Skip: 1 run, `fired = 1`, `skipped = 1`.
  - AllowAll: 2 live runs.
  - CancelOther: the first run ends `Cancelled` with `cancel_reason = schedule_overlap`, and the second starts.
  - TerminateOther: the first run ends `Terminated` with reason `schedule_overlap`.
  - BufferOne: the buffered fire is held, then starts after the first run is signalled and completes.
- **Catch-up after a restart:** One gives the latest instant; All (cap 3, window 60 s) gives the 3 newest; Skip advances the watermark only, and GET shows last and next fire; the steady state continues.
- **Pause, resume and trigger:**
  - Pause is idempotent (no second write) and no fires happen while paused.
  - Trigger works while paused. The same `request_id` returns the same run, no `request_id` gives a fresh run, and the watermark stays put.
  - Resume catches up the latest missed instant (One), and the steady state continues.
  - An unknown schedule is 404.
- **CRUD:**
  - REST: 201 on create, 409 on a duplicate.
  - 400 for an invalid cron, a never-firing cron, an unknown tz, an unknown workflow, an unknown version and bad bounds.
  - Stale update and delete are 409 `version_mismatch`; then update, list and delete succeed, and a later GET is 404.
  - gRPC: create, get, list, trigger (idempotent), pause, a stale delete (`ALREADY_EXISTS`), then delete.
- **phase8_smoke:** the schedule rows now expect real answers (`{"schedules": []}`, a 404 for an unknown name, a 400 for an unknown workflow, and gRPC List ok), the same way 08-15 and 08-17 did.

## Task Commits

1. **Task 1: owner-partition cron driver + tests**: `57e658c`
2. **Task 2: schedules controller, MetaScheduleSource, TRIG-03 tests, docs/cron.md**: `e50ba14`

## Verification

- `cargo test -p ostro-partition --test cron_driver --locked`: **test result: ok. 8 passed** (160 s at high load; most of the time goes to fresh `Node::open`s).
- `cargo clippy -p ostro-partition --all-targets --locked -- -D warnings`: clean.
- `cargo test -p ostro-server --test schedules --locked`: **test result: ok. 4 passed**. Re-run after rebasing onto 06-14 (0a53434): 4 passed.
- `cargo test -p ostro-server --test phase8_smoke --locked`: 1 passed. `cargo test -p ostro-server --lib --locked -- schedules`: 4 passed.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean. `grep -q 'catch-up' docs/cron.md`: ok.
- `cargo build --workspace --all-targets --locked`: warning-free.
- `cargo test -p ostro-partition -p ostro-server --locked --no-fail-fast` on e50ba14, at load average 15 to 34 next to a full ci-local and other executors:
  - every binary ok except two;
  - `resume_rearm` failed 4 of 4 with `node open timed out: Elapsed(())` (tests/common/mod.rs:187, 20 s limit) and passed 4 of 4 when re-run alone;
  - `sink` failed 7 of 19 with `node open timed out: Elapsed(())` (tests/sink.rs:42, 10 s limit). Re-run alone it failed 7 of 19, and with `--test-threads=1` it failed 6 of 19, a different set each time.
  - These are fresh fjall opens under fsync load (the documented `Node::open timed out` load flake). Neither harness configures a schedule source, so `LeaderConfig.cron` stays `None` and the node opens exactly as before.
- **Not run:** `bash scripts/ci-local.sh --no-musl`. The coordinator runs a full ci-local concurrently, and the brief keeps load modest.

## Deviations from Plan

**1. [Rule 3 - Blocking] ostro-partition depends on ostro-triggers.** 08-01 declared every Phase 8 dependency but not this one, and the driver must call `plan_catchup`, `next_after` and `latest_at_or_before` (the plan's key link). One line was added to `crates/ostro-partition/Cargo.toml` and one dependency-list line to `Cargo.lock`. No new package; the lock was regenerated on the current main.

**2. [Rule 3] `node.rs` edited (not in files_modified).** `Node::open` copies `NodeConfig.{cron, schedule_source}` into `LeaderConfig.cron` before spawning the leader services. That is 11 lines; without them the 08-01 NodeConfig fields would be dead.

**3. [Rule 3] `tests/phase8_smoke.rs` edited (not in files_modified)**, at the coordinator's request. Its schedule rows asserted the 08-10 503 stub.

**4. [Design] `ScheduleSource::build_start` (default method) + `FireRequest` / `StartError` / `StartFuture` added to the 08-01 contract file.** The partition crate cannot read meta, so a real `StartRun` (version, queue, module pin, policies, `RunStarted`) has to come from the server. Because the method has a default, existing implementors are unaffected.

**5. [Plan option] No query.rs helper.** The watermark read is `leader::cron::schedule_state(reader, name)`, a pub read-only function in the plan's own file, so query.rs is untouched.

**6. [Test harness] tests/schedules.rs boots its own `TestServer`** (through the public fields) with `ServerOptions::with_clock`, instead of adding a FakeClock option to `tests/common/phase8.rs`, which is outside this plan's files. This is the same pattern as 08-15's `webhooks.rs`.

**7. [Note] REST trigger with a JSON `null` body is 400** (08-10's extractor wants an object or no body). The tests send `{}`.

## Self-Check: PASSED

- Files exist: crates/ostro-partition/src/leader/cron.rs, crates/ostro-partition/tests/cron_driver.rs, crates/ostro-server/src/control/schedules.rs, crates/ostro-server/tests/schedules.rs, docs/cron.md
- Commits 57e658c and e50ba14 are on main
