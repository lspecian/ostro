# Cron schedules

Ostro starts workflow runs on a timezone-aware cron schedule (TRIG-02, TRIG-03). A schedule
is a metadata record. The leader of one partition, the schedule's **owner**, turns it into
runs. Each nominal fire time starts at most one run, across restarts, leader changes and
crashes.

```
ostro schedules create --name nightly --workflow report --cron '0 30 2 * * *' --timezone Europe/Berlin
```

| Surface | Where |
|---|---|
| REST | `GET/POST /v1/schedules`, `GET/PUT/DELETE /v1/schedules/{name}`, `POST /v1/schedules/{name}/pause`, `/resume`, `/trigger` |
| gRPC | `ostro.api.v1.Schedules` (List, Get, Create, Update, Delete, Pause, Resume, Trigger) |
| CLI | `ostro schedules {ls,get,create,update,delete,pause,resume,trigger}` |

## Syntax

Expressions are evaluated by croner 4.0.3, behind the `ostro-triggers` adapter.

- 5 fields (`min hour dom month dow`) or 6 fields with a leading **seconds** field
  (`sec min hour dom month dow`). With 5 fields, the seconds field is 0.
- Ranges, lists and steps: `1-5`, `1,15,30`, `*/10`, `10-50/20`; month and day names
  (`JAN`, `MON`).
- `L`: the last day of the month (`0 0 0 L * *`) or the last given weekday of the month
  (`5L`).
- `#`: the nth weekday of the month (`0 0 9 * * MON#2` is the second Monday).
- `W`: the weekday nearest a day of the month (`0 0 9 15W * *`).
- Validation happens on create and update: an expression that does not parse, or one that
  parses but can never fire (`0 0 0 30 2 *`), is rejected with 400 and the engine's message.

Optional bounds `start_at_ms` / `end_at_ms` are inclusive. A schedule never fires before
the start or after the end. An end before the start is rejected (400).

## Time zones and DST

`timezone` is an IANA name; empty means UTC. An unknown or malformed name is rejected
(400). Zone rules are read from `/usr/share/zoneinfo`, with a fallback to the tzdb bundled
in the binary, so `FROM scratch` images work. Fire times are computed in the zone and
stored as UTC milliseconds.

DST behaviour is pinned by vectors in `crates/ostro-triggers/tests/cron.rs` (08-05):

| Case | Result |
|---|---|
| `0 30 2 * * *` America/New_York, spring forward 2026-03-08 | **03:00 EDT** (the first valid instant after the gap, once), then 03-09 02:30 EDT |
| `0 30 1 * * *` America/New_York, fall back 2026-11-01 | 01:30 **EDT** only (no second fire in the repeated hour), then 11-02 01:30 EST |
| `0 */30 * * * *` America/New_York, fall back | 01:00 EDT, 01:30 EDT, 01:00 EST, 01:30 EST, 02:00 EST: interval jobs fire in both passes |
| `0 */30 * * * *` America/New_York, spring forward | 01:30 EST, then 03:00 EDT |
| `0 0 2 * * *` Europe/Berlin 2026-03-29 / 10-25 | 03:00 +02 (gap); in the overlap, 02:00 +02 only |
| `0 15 2 * * *` Australia/Lord_Howe 2026-10-04 (30-minute shift) | 02:30 +11:00 once, then 10-05 02:15 +11:00 |

In short, a fixed-time job whose wall time does not exist fires once, at the first valid
instant after the gap. A fixed-time job whose wall time occurs twice fires once, in the
first pass. Interval jobs follow the wall clock through both passes.

**tz rule changes (Pitfall 13).** A new tzdb release can move a future fire instant. The
watermark (below) makes it impossible to re-fire an instant at or before the last handled
fire, so a rule change can shift or drop at most one instance near the boundary. It can
never double one.

## Overlap policy

`overlap` decides what happens when a fire is due while the schedule's previous run (the
last run it started) is still live. It is evaluated in apply, on the owner partition, at
fire time:

| Policy | Wire name | Effect |
|---|---|---|
| Skip (default) | `skip` | No run; the fire counts as skipped (the watermark still moves). |
| BufferOne | `buffer_one` | Hold at most one fire; it starts when the live run ends (any terminal status). A newer buffered fire replaces an older one. |
| AllowAll | `allow_all` | Start anyway (runs overlap). |
| CancelOther | `cancel_other` | Request a graceful cancel of the live run (reason `schedule_overlap`), then start. |
| TerminateOther | `terminate_other` | Terminate the live run (reason `schedule_overlap`), then start. |

## Catch-up policy

Catch-up applies when the owner's driver finds fire times it missed: at leadership start
(after downtime or a crash), when a schedule is resumed, and when a live driver falls
behind by more than one fire. Missed fires are those in `(floor, now]`, where
`floor = max(watermark, created_at_ms)`.

| Policy | Wire name | Effect |
|---|---|---|
| Skip (default) | `skip` | No run. One `ScheduleFire{kind: Advance}` moves the watermark to the latest missed fire. |
| One | `one` | One run, for the **latest** fire at or before now. |
| All | `all` | One run per missed fire inside `catchup_window_ms` (0 = no window), keeping the **newest** `[triggers] cron_catchup_cap` (default 100). |

Catch-up costs bounded work however long the downtime was. The driver computes
`latest_at_or_before(now)` and walks backwards over at most `cap` fires. It never
enumerates from the watermark, so a 2-day outage of an every-second schedule (172,800
missed fires) costs a handful of evaluations. The `skipped` count it reports is exact up
to 256 fires and extrapolated beyond; it is used for metrics only.

After catch-up, the steady state resumes at the first fire after `max(floor, now)`.

## Pause, resume, trigger-now

- **Pause** (`POST /v1/schedules/{name}/pause`) is a meta CAS on `paused = true`. The
  driver drops the schedule and the watermark stays. Pausing twice does nothing.
- **Resume** sets `paused = false` with a CAS. The driver re-activates the schedule and
  the **catch-up policy** applies to the fires missed while paused.
- **Trigger-now** (`POST /v1/schedules/{name}/trigger`, optional body `{"request_id": "..."}`)
  submits `ScheduleFire{kind: Manual, fire_time = now}` to the owner partition and returns
  the run id.
  - It never moves the watermark, and it works on a paused schedule.
  - The run's idempotency key is `__cron/<name>/manual/<hex(request_id)>`. A repeated
    `request_id` returns the same run. Without one, 16 random bytes are used.
  - It is subject to the overlap policy like any fire. With `skip`, a trigger while the
    previous run is live answers 409 `schedule_overlap`. With `buffer_one`, it returns the
    id of the run that starts when the live one ends.
  - A retried `request_id` is checked before the overlap policy: retrying a trigger while
    its own run is still live returns that run (not 409, not a buffered fire).

## Exactly-once

Two guards make each `(schedule, fire_time)` start at most one run.

1. **Watermark.** The owner partition keeps one row per schedule in keyspace 16
   `schedule_state`: `watermark_ms`, `last_run`, the buffered fire and counters. A `Fire`
   or `Advance` at or below the watermark is a duplicate. The watermark moves in the same
   write batch as the run start, so this holds across restarts, leader changes and
   dedupe-row expiry.
2. **Idempotency key.** Every cron run carries `__cron/<schedule>/<fire_time_ms>` with
   `ReturnExisting`, so a fire that slips past the watermark (for example, from an older
   incarnation) still maps to the existing run.

The driver retries a failed submit with the same command, which is idempotent by both
guards. It stops when it loses leadership (`NotLeader`, fenced) or the partition fails.
The next leader re-activates every schedule from the durable watermark.

Every run a schedule starts carries the tag `ostro.schedule=<name>`, cut to 128 bytes for
very long names. `GET /v1/runs?tag=ostro.schedule=<name>` lists them. The metric
`ostro_cron_fires_total{schedule, outcome}` counts the driver's fires by outcome:
`started`, `skipped`, `buffered`, `advanced`, `duplicate`, `rejected`, `start_error`.

`GET /v1/schedules/{name}` reports `next_fire_at_ms`, computed from the expression and the
watermark (`null` while paused or past `end_at_ms`), and `last_fire_at_ms`, the watermark
(`null` before the first fire).

## How it runs

- **Owner.** `schedule_owner(name, P) = xxh3_64("__cron/" ‖ name) % P`. This is pinned and
  never moves across versions. Only the owner partition's leader drives the schedule, and
  apply rejects a `ScheduleFire` on any other partition (`ScheduleOwnerMismatch`).
- **Source.** The server's `MetaScheduleSource` subscribes to the metadata applied index and
  publishes the full schedule set to every partition's driver whenever it changes. Each
  driver keeps the schedules it owns.
- **Driver.** A leader service per partition. It keeps a `(due, name)` window and polls the
  injected clock every 50 ms or less, so tests drive it with a `ManualClock`. A due fire
  submits `ScheduleFire{kind: Fire}`, whose `StartRun` is built like an API start:
  - the pinned or active workflow version, the version's default queue and its module pin;
  - the run timeout and the `[runs]` / workflow / queue retention and schedule-to-start
    policies;
  - the pre-encoded `RunStarted`.

  If the workflow no longer exists at fire time, the fire is dropped and logged
  (`start_error`).
- **Changes.** A cron, time-zone or bounds change applies to future fires only; the
  watermark carries over (the update keeps `created_at_ms`).

## Delete and re-create

Delete removes the meta record only. The owner's `schedule_state` row stays as an orphan.
This is harmless:

- every `ScheduleFire` carries the schedule's `created_at_ms` (its incarnation, W4), and a
  re-created schedule with the same name gets a new `created_at_ms`. Its first fire resets
  the row, and a fire of the old incarnation is ignored;
- the driver's catch-up floor is `max(watermark, created_at_ms)` (W6), so a re-created (or
  brand-new) schedule never backfills fires from before its creation, whatever the orphaned
  watermark says.

A later cleanup command may delete orphaned rows; nothing depends on them.

## Known limitations

- **Continue-as-new.** A schedule tracks the run it started as `last_run`. When that run
  continues as new, `last_run` is cleared: the successor chain is not tracked for overlap
  (08-07), so the next fire does not see it as live.
- **Outbox `CronStart`** (outbox message kind 5, frozen in Phase 2) stays valid but unused.
  Cron runs are started by `Cmd::ScheduleFire` (tag 27) on the owner partition, never
  through the outbox.
- The catch-up `skipped` counter is an estimate beyond 256 missed fires.
