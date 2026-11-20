---
phase: 08-triggers-bottomless-observability
plan: 05
subsystem: triggers
tags: [cron, croner, jiff, tzdb, dst, catch-up, proptest, tdd]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-01 croner 4.0.3 (jiff feature) + jiff02 0.2.46 declared, ostro-triggers skeleton with cron/tz modules
provides:
  - "ostro_triggers::cron: CronSpec::parse / parse_bounded, next_after, next_after_expr, latest_at_or_before, fires_in, count_fires, plan_catchup -> CatchupPlan, CatchupPolicy / OverlapPolicy (TryFrom<u8>, 05-07 numbering), CronError {Expr, Timezone, Bounds, Policy}"
  - "ostro_triggers::tz: tz_bytes, bundled_bytes, validate_name, load, TzSource, TzError; both jiff majors parsed from the same TZif bytes"
  - "tests/cron.rs: DST vectors (NY, Berlin, Lord_Howe), modifiers, validation, catch-up, two 512-case proptests"
affects: [08-10 schedule API validation, 08-14 cron driver, docs/cron.md (08-14)]

tech-stack:
  added: [jiff-tzdb 1.0.0 (direct; already in the graph via jiff tzdb-bundle-always), proptest (ostro-triggers dev-dep)]
  patterns:
    - "croner's backward search proposes, the forward search verifies: latest_at_or_before is always consistent with next_after iteration"
    - "fires_in walks backwards from the upper end: O(cap) work, truncation drops the oldest"

key-files:
  created:
    - crates/ostro-triggers/tests/cron.rs
  modified:
    - crates/ostro-triggers/src/cron.rs
    - crates/ostro-triggers/src/tz.rs
    - crates/ostro-triggers/Cargo.toml
    - Cargo.toml
    - Cargo.lock

key-decisions:
  - "DST gap pinned to croner 4.0.3's documented rule: a fixed-time job whose wall time is missing fires once at the first valid instant after the gap (NY 02:30 on 2026-03-08 -> 03:00 EDT), not 'no fire that day' as the plan's first vector guessed"
  - "Bundled TZif bytes come from jiff-tzdb directly (jiff 1.0 has no bytes accessor); declared as a workspace dep at the same 1.0.0 the graph already had"
  - "plan_catchup window_ms == 0 means no window (only floor and cap limit All); the driver may substitute its own default"
  - "CatchupPlan.skipped is exact up to SKIP_COUNT_BUDGET (256) fires, extrapolated from the counted prefix beyond (saturating u32::MAX); metrics/logs only"
  - "CronSpec::parse rejects expressions that parse but can never fire (Feb 30) as CronError::Expr"

requirements-completed: []

duration: ~15 min
completed: 2026-11-20
---

# Phase 8 Plan 05: Cron engine Summary

**This plan adds a pure, timezone-aware cron engine to `ostro-triggers`. croner 4.0.3 does the evaluation behind a private jiff-0.2 adapter, and the jiff 0.2 and jiff 1.0 zones are parsed from the same TZif bytes. It computes the next fire, the latest fire at or before an instant, and range enumeration that keeps the newest fires. It also plans catch-up with floor = max(watermark, created_at). The public API uses only i64 ms and jiff 1.0 types. 22 tests pass, including two 512-case proptests.**

## Performance

- **Duration:** about 15 minutes
- **Completed:** 2026-11-20
- **Tasks:** 1 TDD feature (RED, then GREEN; no refactor commit needed)
- **Files:** 6

## Spike outcome (API shapes)

| API | Shape found | Used as |
|---|---|---|
| `jiff02::tz::TimeZone::tzif` | `fn tzif(name: &str, data: &[u8]) -> Result<TimeZone, Error>`, available with just `alloc`/`std` | 0.2 zone from the bytes |
| `jiff::tz::TimeZone::tzif` (1.0.1) | the same shape | 1.0 zone from the same bytes |
| jiff 1.0 bytes accessor | **none**: there is no `to_tzif`, and `tz::db()` returns only parsed zones | the bundle is read through `jiff_tzdb::get(name) -> Option<(&str, &'static [u8])>` |
| `croner::Cron::new(expr).with_seconds_optional()` | **does not exist in 4.0.3**. The builder is `CronParser::builder().seconds(Seconds::Optional).build().parse(expr)`, and `Cron::from_str` is equivalent | parser builder |
| `Cron::find_next_occurrence(&jiff02::Zoned, bool)` | `Result<Zoned, CronError>`, compiles with the `jiff` feature and no chrono | `raw_next_after` |
| `Cron::find_previous_occurrence` | **exists** (same shape) | candidate for `latest_at_or_before` |

Measured speed: croner next/previous takes about 4 µs per call in a debug build (100k calls in 0.41 s).

## Pinned DST behaviour (croner 4.0.3, asserted in tests/cron.rs)

| Case | Result |
|---|---|
| `0 30 2 * * *` NY, after 2026-03-08 00:00 | **2026-03-08 03:00 EDT** (first valid instant after the gap, once), then 03-09 02:30 EDT |
| `0 30 1 * * *` NY, after 2026-11-01 00:00 | 01:30 **EDT** only, then 11-02 01:30 EST. Starting inside the EST pass still gives no second fire. `latest_at_or_before(01:45 EST)` = 01:30 EDT |
| `0 */30 * * * *` NY overlap | 01:00 EDT, 01:30 EDT, 01:00 EST, 01:30 EST, 02:00 EST (interval jobs fire in both passes) |
| `0 */30 * * * *` NY gap | 01:30 EST, then 03:00 EDT |
| `0 0 2 * * *` Berlin 2026-03-29 / 10-25 | 03:00 +02 (gap). For the overlap, 02:00 +02 only, then 10-26 02:00 +01 |
| `0 15 2 * * *` Lord_Howe 2026-10-04 (30-min shift) | 02:30 +11:00 once, then 10-05 02:15 +11:00 |
| `*/15 * * * * *`, `0 0 0 L * *`, `0 0 9 * * MON#2`, `0 0 9 15W * *` | 15 s steps; Jan 31 / Feb 28 / Mar 31; Jan 12 / Feb 9 / Mar 9; Feb 16 / Mar 16 / Apr 15 |

The plan's first vector expected "no fire on 03-08, next is 03-09 02:30". croner's README documents the first-valid-instant rule for fixed-time jobs, and the plan said to "assert whatever croner documents and pin it", so that rule is what the tests pin. docs/cron.md (08-14) should quote this table.

## How TZif byte equality is ensured

- `tz::tz_bytes(name)` returns **one** byte buffer per zone:
  - It reads `/usr/share/zoneinfo/<name>` if the name passes `validate_name` (relative, IANA characters, no `.`/`..` segments, ≤ 64 bytes), the path is a regular file, and the data starts with `TZif`.
  - Otherwise it uses the jiff-tzdb bundle, which is the same crate and version behind jiff 1.0's `tzdb-bundle-always`, so `FROM scratch` images work.
- `DualZone::from_tzif` parses that single buffer with both `jiff::tz::TimeZone::tzif` and `jiff02::tz::TimeZone::tzif`. `CronSpec` caches both zones plus the parsed `croner::Cron`.
- `""` and `"UTC"` map to the `UTC` constants of both majors.
- The test `both_majors_parse_identical_bytes_and_agree` compares the two majors' offsets hourly through 2026 and 2041 (the POSIX-rule tail). It does this for NY, Berlin, Lord_Howe, Kolkata and UTC, on both the system bytes and the bundled bytes.

## Engine notes for 08-10 / 08-14

- **Free functions match 08-14's interface block:** `next_after(&spec, ms)`, `latest_at_or_before(&spec, t)`, `fires_in(&spec, from_excl, to_incl, cap) -> (Vec<i64>, truncated)` and `plan_catchup(&spec, watermark, created_at_ms, now, policy, window_ms, cap) -> CatchupPlan { fire, advance_to, skipped }`. `CronSpec::parse_bounded(expr, tz, start_at_ms, end_at_ms)` takes the meta `u64`s directly.
- **Bounds are inclusive.** `next_after` returns the first fire ≥ start, and nothing is returned past end.
- **`latest_at_or_before`:** croner's previous occurrence is accepted only if the forward search agrees, meaning `next_after(p-1) == p` and `next_after(p) > t`. Otherwise it falls back to an expanding backward window (1 min, doubling) scanned forward. The result is always a member of the forward fire set. In the vectors and proptests above, the two searches never disagreed.
- **`fires_in`** walks back from `to_inclusive`, about 3 croner calls per fire, so its cost is O(cap) whatever the range. Truncation drops the oldest fires.
- **`plan_catchup`:**
  - Skip gives `advance_to = latest`.
  - One gives `fire = [latest]`.
  - All gives the newest `cap` fires in `(max(floor, now - window), now]`.
  - `skipped` counts every missed fire older than the first planned one, or all missed fires when none is planned. If `now <= floor`, the plan is empty.
  - After catch-up, the steady state restarts at `next_after(max(floor, now))`.
- **Never-firing expressions:** `CronSpec::parse` rejects them, which costs about 90 ms in debug because croner runs out to its year-5000 limit. Valid expressions parse in about 2 ms.

## Task Commits

1. **RED**, `fe34df9`: `test(08-05)`, the failing tests plus the dependency declarations (jiff-tzdb, the proptest dev-dep).
2. **GREEN**, `da7fb2e`: `feat(08-05)`, the cron engine and tz loading.

## Verification

- `cargo test -p ostro-triggers --test cron --locked`: **test result: ok. 22 passed**. Two extra runs with `PROPTEST_CASES=4096` at load average ~7 also had 22 passed.
- `cargo test -p ostro-triggers --locked`: all targets ok.
- `cargo clippy -p ostro-triggers --all-targets --locked -- -D warnings`: clean. The `--all-targets` build is warning-free, including cargo's unused-dependency lint.
- `cargo fmt -p ostro-triggers -- --check`: clean. `RUSTDOCFLAGS=-D warnings cargo doc -p ostro-triggers --no-deps`: clean.
- `cargo tree -p ostro-triggers -i chrono`: there is no chrono package in the graph.
- `cargo deny check bans licenses sources`: ok. `scripts/forbidden-crates.sh`: ok.
- No clock reads. Both test-only `Instant::now()` timing reads carry `// clock-exempt:` comments.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Cargo.toml/Cargo.lock touched despite "do not touch"**
- **Issue:** the proptest cases need `proptest` as a dev-dependency, and jiff 1.0 exposes no TZif bytes. The plan's own fallback ("the `jiff-tzdb` crate already in the graph") requires declaring that crate.
- **Fix:**
  - Root `Cargo.toml` gains the workspace dependency `jiff-tzdb = "1.0.0"`, the version already locked.
  - `ostro-triggers/Cargo.toml` gains `jiff-tzdb.workspace = true` and `[dev-dependencies] proptest.workspace = true`.
  - `Cargo.lock` changes only by +2 dependency-list lines; no new package.
  - deny is unchanged and ok.
- **Commit:** fe34df9

**2. [Spike] croner API shape**
- There is no `Cron::new(..).with_seconds_optional()` in 4.0.3, so `CronParser::builder().seconds(Seconds::Optional)` is used instead (see the table above).

**3. [Pinned behaviour] NY spring-forward vector**
- The test pins croner's documented first-valid-instant rule instead of the plan's guessed "no fire that day" (see above).

**4. [Test robustness] Proptest runtime bound**
- The 50 ms per-case bound is asserted on the fastest of up to three identical runs. One run at `PROPTEST_CASES=4096` under load measured 50.3 ms on a single sample.
- To keep the margin, `SKIP_COUNT_BUDGET` was lowered from 1024 to 256 before commit. Typical cases take a few ms.

## Self-Check: PASSED

- Files exist: `crates/ostro-triggers/src/{cron,tz}.rs` and `crates/ostro-triggers/tests/cron.rs`.
- Commits are on main: fe34df9 and da7fb2e.
