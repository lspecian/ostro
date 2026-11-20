//! Cron engine (08-05, TRIG-02/03 logic): DST vectors, modifiers, validation, latest-before,
//! range enumeration, catch-up planning, shared-TZif zones, and proptests.

use std::time::Instant;

use ostro_triggers::cron::{
    CatchupPlan, CatchupPolicy, CronError, CronSpec, OverlapPolicy, fires_in, latest_at_or_before,
    next_after, next_after_expr, plan_catchup,
};
use ostro_triggers::tz;
use proptest::prelude::*;

const NY: &str = "America/New_York";
const BERLIN: &str = "Europe/Berlin";
const LORD_HOWE: &str = "Australia/Lord_Howe";
const KOLKATA: &str = "Asia/Kolkata";

/// Unix ms of an RFC 3339 instant (`...Z` or with an offset).
fn ms(s: &str) -> i64 {
    s.parse::<jiff::Timestamp>()
        .unwrap_or_else(|e| panic!("{s}: {e}"))
        .as_millisecond()
}

/// Unix ms of a local civil time in `zone` (must be unambiguous).
fn local(zone: &str, civil: &str) -> i64 {
    let tz = jiff::tz::TimeZone::get(zone).unwrap();
    tz.to_timestamp(civil.parse::<jiff::civil::DateTime>().unwrap())
        .unwrap()
        .as_millisecond()
}

fn spec(expr: &str, zone: &str) -> CronSpec {
    CronSpec::parse(expr, zone).unwrap_or_else(|e| panic!("{expr} {zone}: {e}"))
}

/// The first `n` fires after `after`.
fn seq(s: &CronSpec, after: i64, n: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(n);
    let mut t = after;
    for _ in 0..n {
        t = next_after(s, t).expect("a next fire");
        out.push(t);
    }
    out
}

// ---------------------------------------------------------------- DST vectors

#[test]
fn ny_spring_gap_fixed_time_fires_once_after_the_gap() {
    // croner 4.0.3 (README "DST Gap"): a fixed-time job whose wall time does not exist runs at
    // the first valid instant after the gap, the same day. 02:30 does not exist on 2026-03-08,
    // so the fire is 03:00 EDT; never two fires that day.
    let s = spec("0 30 2 * * *", NY);
    let after = local(NY, "2026-03-08T00:00");
    assert_eq!(
        seq(&s, after, 2),
        vec![
            ms("2026-03-08T03:00:00-04:00"),
            ms("2026-03-09T02:30:00-04:00")
        ]
    );
    assert_eq!(
        next_after_expr("0 30 2 * * *", NY, after).unwrap(),
        Some(ms("2026-03-08T07:00:00Z"))
    );
    // The day before is a normal EST day.
    assert_eq!(
        next_after(&s, local(NY, "2026-03-07T00:00")),
        Some(ms("2026-03-07T02:30:00-05:00"))
    );
}

#[test]
fn ny_fall_overlap_fixed_time_fires_once_at_first_occurrence() {
    let s = spec("0 30 1 * * *", NY);
    let after = local(NY, "2026-11-01T00:00");
    assert_eq!(
        seq(&s, after, 2),
        vec![
            ms("2026-11-01T01:30:00-04:00"),
            ms("2026-11-02T01:30:00-05:00")
        ]
    );
    // Starting inside the second (EST) pass does not produce a second fire that day.
    assert_eq!(
        next_after(&s, ms("2026-11-01T01:20:00-05:00")),
        Some(ms("2026-11-02T01:30:00-05:00"))
    );
    // Latest-before during the second pass is the EDT fire.
    assert_eq!(
        latest_at_or_before(&s, ms("2026-11-01T01:45:00-05:00")),
        Some(ms("2026-11-01T01:30:00-04:00"))
    );
}

#[test]
fn ny_interval_job_fires_in_both_overlap_passes_and_skips_the_gap() {
    let s = spec("0 */30 * * * *", NY);
    assert_eq!(
        seq(&s, local(NY, "2026-11-01T00:40"), 5),
        vec![
            ms("2026-11-01T01:00:00-04:00"),
            ms("2026-11-01T01:30:00-04:00"),
            ms("2026-11-01T01:00:00-05:00"),
            ms("2026-11-01T01:30:00-05:00"),
            ms("2026-11-01T02:00:00-05:00"),
        ]
    );
    assert_eq!(
        seq(&s, local(NY, "2026-03-08T01:00"), 2),
        vec![
            ms("2026-03-08T01:30:00-05:00"),
            ms("2026-03-08T03:00:00-04:00")
        ]
    );
}

#[test]
fn berlin_gap_and_overlap() {
    let s = spec("0 0 2 * * *", BERLIN);
    assert_eq!(
        seq(&s, local(BERLIN, "2026-03-29T00:00"), 2),
        vec![
            ms("2026-03-29T03:00:00+02:00"),
            ms("2026-03-30T02:00:00+02:00")
        ]
    );
    assert_eq!(
        seq(&s, local(BERLIN, "2026-10-25T00:00"), 2),
        vec![
            ms("2026-10-25T02:00:00+02:00"),
            ms("2026-10-26T02:00:00+01:00")
        ]
    );
}

#[test]
fn lord_howe_half_hour_shift() {
    // Lord Howe moves from +10:30 to +11:00 at 02:00 on 2026-10-04: 02:00..02:30 does not
    // exist, so 02:15 fires at 02:30 (+11:00), once.
    let s = spec("0 15 2 * * *", LORD_HOWE);
    assert_eq!(
        seq(&s, local(LORD_HOWE, "2026-10-03T00:00"), 3),
        vec![
            ms("2026-10-03T02:15:00+10:30"),
            ms("2026-10-04T02:30:00+11:00"),
            ms("2026-10-05T02:15:00+11:00"),
        ]
    );
}

// ---------------------------------------------------------------- syntax

#[test]
fn seconds_field_and_modifiers() {
    let t0 = ms("2026-02-01T00:00:00Z");
    assert_eq!(
        seq(&spec("*/15 * * * * *", "UTC"), t0, 3),
        vec![t0 + 15_000, t0 + 30_000, t0 + 45_000]
    );
    assert_eq!(
        seq(&spec("0 0 0 L * *", "UTC"), ms("2026-01-15T00:00:00Z"), 3),
        vec![
            ms("2026-01-31T00:00:00Z"),
            ms("2026-02-28T00:00:00Z"),
            ms("2026-03-31T00:00:00Z")
        ]
    );
    assert_eq!(
        seq(
            &spec("0 0 9 * * MON#2", "UTC"),
            ms("2026-01-01T00:00:00Z"),
            3
        ),
        vec![
            ms("2026-01-12T09:00:00Z"),
            ms("2026-02-09T09:00:00Z"),
            ms("2026-03-09T09:00:00Z")
        ]
    );
    // 2026-02-15 is a Sunday -> Monday 16th; 2026-03-15 is a Sunday -> 16th; 04-15 Wednesday.
    assert_eq!(
        seq(&spec("0 0 9 15W * *", "UTC"), ms("2026-02-01T00:00:00Z"), 3),
        vec![
            ms("2026-02-16T09:00:00Z"),
            ms("2026-03-16T09:00:00Z"),
            ms("2026-04-15T09:00:00Z")
        ]
    );
    // Five fields: seconds = 0.
    assert_eq!(
        next_after(&spec("*/5 * * * *", "UTC"), t0),
        Some(t0 + 5 * 60_000)
    );
}

#[test]
fn next_after_is_strict_and_millisecond_exact() {
    let s = spec("* * * * * *", "UTC");
    let t0 = ms("2026-02-01T00:00:00Z");
    assert_eq!(next_after(&s, t0), Some(t0 + 1000));
    assert_eq!(next_after(&s, t0 - 1), Some(t0));
    assert_eq!(next_after(&s, t0 + 999), Some(t0 + 1000));
    assert_eq!(latest_at_or_before(&s, t0 + 999), Some(t0));
    assert_eq!(latest_at_or_before(&s, t0), Some(t0));
    assert_eq!(latest_at_or_before(&s, t0 - 1), Some(t0 - 1000));
}

#[test]
fn utc_and_empty_timezone_agree() {
    let a = spec("0 0 9 * * MON#2", "");
    let b = spec("0 0 9 * * MON#2", "UTC");
    let t0 = ms("2026-01-01T00:00:00Z");
    assert_eq!(seq(&a, t0, 5), seq(&b, t0, 5));
    assert_eq!(a.time_zone(), &jiff::tz::TimeZone::UTC);
    assert_eq!(a.tz_source(), None);
}

#[test]
fn kolkata_has_no_dst() {
    let s = spec("0 30 9 * * *", KOLKATA);
    assert_eq!(
        next_after(&s, ms("2026-06-01T00:00:00Z")),
        Some(ms("2026-06-01T09:30:00+05:30"))
    );
}

// ---------------------------------------------------------------- validation

#[test]
fn validation_errors_are_typed() {
    for bad in ["", "x", "61 * * * * *", "* * * * * * * *", "0 0 32 * *"] {
        assert!(
            matches!(CronSpec::parse(bad, "UTC"), Err(CronError::Expr { .. })),
            "{bad:?} must be an Expr error"
        );
    }
    // Parses, but never fires.
    assert!(matches!(
        CronSpec::parse("0 0 0 30 2 *", "UTC"),
        Err(CronError::Expr { .. })
    ));
    for zone in [
        "Mars/Base",
        "../etc/passwd",
        "/etc/localtime",
        "America//New_York",
        "a b",
    ] {
        assert!(
            matches!(
                CronSpec::parse("* * * * *", zone),
                Err(CronError::Timezone { .. })
            ),
            "{zone:?} must be a Timezone error"
        );
    }
    assert!(matches!(
        CronSpec::parse_bounded("* * * * *", "UTC", Some(2_000), Some(1_000)),
        Err(CronError::Bounds(_))
    ));
    assert!(matches!(
        CronSpec::parse_bounded("* * * * *", "UTC", Some(u64::MAX), None),
        Err(CronError::Bounds(_))
    ));
    assert!(CronSpec::parse_bounded("* * * * *", "UTC", Some(1_000), Some(1_000)).is_ok());
}

#[test]
fn policy_bytes_match_the_meta_constants() {
    // 05-07 records.rs: OVERLAP_SKIP 0, BUFFER_ONE 1, ALLOW_ALL 2, CANCEL_OTHER 3,
    // TERMINATE_OTHER 4; CATCHUP_SKIP 0, ONE 1, ALL 2.
    let overlap = [
        OverlapPolicy::Skip,
        OverlapPolicy::BufferOne,
        OverlapPolicy::AllowAll,
        OverlapPolicy::CancelOther,
        OverlapPolicy::TerminateOther,
    ];
    for (i, p) in overlap.iter().enumerate() {
        assert_eq!(OverlapPolicy::try_from(i as u8).unwrap(), *p);
        assert_eq!(p.as_u8(), i as u8);
    }
    assert!(matches!(
        OverlapPolicy::try_from(5),
        Err(CronError::Policy {
            kind: "overlap",
            value: 5
        })
    ));
    let catchup = [CatchupPolicy::Skip, CatchupPolicy::One, CatchupPolicy::All];
    for (i, p) in catchup.iter().enumerate() {
        assert_eq!(CatchupPolicy::try_from(i as u8).unwrap(), *p);
        assert_eq!(p.as_u8(), i as u8);
    }
    assert!(matches!(
        CatchupPolicy::try_from(3),
        Err(CronError::Policy {
            kind: "catchup",
            value: 3
        })
    ));
}

// ---------------------------------------------------------------- bounds, latest, ranges

#[test]
fn bounds_clip_next_and_latest() {
    let t0 = ms("2026-02-01T00:00:00Z");
    let s = CronSpec::parse_bounded(
        "0 * * * * *",
        "UTC",
        Some((t0 + 120_500) as u64),
        Some((t0 + 300_000) as u64),
    )
    .unwrap();
    assert_eq!(
        next_after(&s, t0),
        Some(t0 + 180_000),
        "first fire >= start"
    );
    assert_eq!(
        next_after(&s, t0 + 240_000),
        Some(t0 + 300_000),
        "end is inclusive"
    );
    assert_eq!(next_after(&s, t0 + 300_000), None);
    assert_eq!(
        latest_at_or_before(&s, t0 + 10 * 60_000),
        Some(t0 + 300_000)
    );
    assert_eq!(latest_at_or_before(&s, t0 + 179_999), None);
    assert_eq!(latest_at_or_before(&s, t0), None);
}

#[test]
fn latest_at_or_before_dst_and_sparse() {
    let s = spec("0 30 2 * * *", NY);
    assert_eq!(
        latest_at_or_before(&s, local(NY, "2026-03-08T12:00")),
        Some(ms("2026-03-08T03:00:00-04:00"))
    );
    // A yearly schedule, looked up from far away: bounded work.
    let y = spec("0 0 0 29 2 *", "UTC");
    assert_eq!(
        latest_at_or_before(&y, ms("2027-06-01T00:00:00Z")),
        Some(ms("2024-02-29T00:00:00Z"))
    );
}

#[test]
fn fires_in_keeps_the_newest() {
    let s = spec("*/10 * * * * *", "UTC");
    let t0 = ms("2026-02-01T00:00:00Z");
    let (all, truncated) = fires_in(&s, t0, t0 + 60_000, 100);
    assert!(!truncated);
    assert_eq!(all, (1..=6).map(|i| t0 + i * 10_000).collect::<Vec<_>>());
    let (last, truncated) = fires_in(&s, t0, t0 + 60_000, 2);
    assert!(truncated);
    assert_eq!(last, vec![t0 + 50_000, t0 + 60_000]);
    let (none, truncated) = fires_in(&s, t0, t0 + 60_000, 0);
    assert!(none.is_empty() && truncated);
    let (empty, truncated) = fires_in(&s, t0, t0 + 9_999, 5);
    assert!(empty.is_empty() && !truncated);
}

// ---------------------------------------------------------------- catch-up

#[test]
fn catchup_cap_keeps_the_newest_fires() {
    // Consistent with 08-14: window 60 s, cap 3, every 10 s, 5 minutes of downtime.
    let s = spec("*/10 * * * * *", "UTC");
    let now = ms("2026-02-01T12:00:00Z");
    let watermark = now - 300_000;
    let plan = plan_catchup(
        &s,
        watermark,
        watermark - 3_600_000,
        now,
        CatchupPolicy::All,
        60_000,
        3,
    );
    assert_eq!(plan.fire, vec![now - 20_000, now - 10_000, now]);
    assert_eq!(plan.advance_to, None);
    // (watermark, now - 20 s) holds 27 fires.
    assert_eq!(plan.skipped, 27);
}

#[test]
fn catchup_skip_and_one() {
    let s = spec("0 * * * * *", "UTC");
    let now = ms("2026-02-01T12:00:30Z");
    let watermark = ms("2026-02-01T11:50:00Z");
    let created = watermark - 1;
    let latest = ms("2026-02-01T12:00:00Z");
    assert_eq!(
        plan_catchup(&s, watermark, created, now, CatchupPolicy::Skip, 0, 100),
        CatchupPlan {
            fire: vec![],
            advance_to: Some(latest),
            skipped: 10
        }
    );
    assert_eq!(
        plan_catchup(&s, watermark, created, now, CatchupPolicy::One, 0, 100),
        CatchupPlan {
            fire: vec![latest],
            advance_to: None,
            skipped: 9
        }
    );
    // Nothing missed: the watermark already covers the latest fire.
    for policy in [CatchupPolicy::Skip, CatchupPolicy::One, CatchupPolicy::All] {
        assert_eq!(
            plan_catchup(&s, latest, created, now, policy, 0, 100),
            CatchupPlan::default()
        );
    }
}

#[test]
fn catchup_never_backfills_before_created_at() {
    // W6: an orphaned old watermark (or 0 for a brand-new schedule) below created_at.
    let s = spec("0 * * * * *", "UTC");
    let now = ms("2026-02-01T12:00:30Z");
    let created = ms("2026-02-01T12:00:00Z");
    for watermark in [0, ms("2026-01-01T00:00:00Z")] {
        for policy in [CatchupPolicy::Skip, CatchupPolicy::One, CatchupPolicy::All] {
            assert_eq!(
                plan_catchup(&s, watermark, created, now, policy, 0, 100),
                CatchupPlan::default(),
                "{policy:?} wm {watermark}"
            );
        }
    }
    // Created just before a fire: exactly that one is planned by One/All.
    let created = ms("2026-02-01T11:59:59Z");
    assert_eq!(
        plan_catchup(&s, 0, created, now, CatchupPolicy::All, 0, 100).fire,
        vec![ms("2026-02-01T12:00:00Z")]
    );
}

#[test]
fn catchup_long_downtime_is_bounded_and_newest() {
    let s = spec("* * * * * *", "UTC");
    let now = ms("2026-02-01T12:00:00Z");
    let floor = now - 30 * 86_400_000;
    let started = Instant::now(); // clock-exempt: test runtime bound only
    let all = plan_catchup(&s, floor, 0, now, CatchupPolicy::All, 0, 100);
    let skip = plan_catchup(&s, floor, 0, now, CatchupPolicy::Skip, 0, 100);
    let elapsed = started.elapsed(); // clock-exempt: test runtime bound only
    assert_eq!(all.fire.len(), 100);
    assert_eq!(*all.fire.last().unwrap(), now);
    assert_eq!(all.fire[0], now - 99_000);
    // 30 days of seconds: the estimate is close to the true count (2_592_000 - 100).
    let exact = 30 * 86_400 - 100;
    assert!(
        all.skipped.abs_diff(exact) <= exact / 100,
        "{}",
        all.skipped
    );
    assert_eq!(skip.advance_to, Some(now));
    assert!(elapsed.as_millis() < 500, "{elapsed:?}");
}

// ---------------------------------------------------------------- shared TZif bytes

#[test]
fn both_majors_parse_identical_bytes_and_agree() {
    for zone in [NY, BERLIN, LORD_HOWE, KOLKATA, "UTC"] {
        let bundled = tz::bundled_bytes(zone).expect("bundled zone");
        let (bytes, _) = tz::tz_bytes(zone).unwrap();
        for data in [&bytes[..], bundled] {
            let v1 = jiff::tz::TimeZone::tzif(zone, data).unwrap();
            let v02 = jiff02::tz::TimeZone::tzif(zone, data).unwrap();
            // Hourly through 2026, plus a far-future year (POSIX rule tail).
            for start in [ms("2026-01-01T00:00:00Z"), ms("2041-01-01T00:00:00Z")] {
                for h in 0..(366 * 24) {
                    let t = start / 1000 + h * 3600;
                    let o1 = v1
                        .to_offset(jiff::Timestamp::from_second(t).unwrap())
                        .seconds();
                    let o02 = v02
                        .to_offset(jiff02::Timestamp::from_second(t).unwrap())
                        .seconds();
                    assert_eq!(o1, o02, "{zone} at {t}");
                }
            }
        }
    }
}

#[test]
fn tz_loading_sources_and_names() {
    let (_, source) = tz::load(NY).unwrap();
    let system = std::path::Path::new(tz::SYSTEM_ZONEINFO).join(NY).is_file();
    assert_eq!(
        source,
        Some(if system {
            tz::TzSource::System
        } else {
            tz::TzSource::Bundled
        })
    );
    assert_eq!(tz::load("").unwrap().1, None);
    assert!(matches!(
        tz::tz_bytes("Mars/Base"),
        Err(tz::TzError::Unknown(_))
    ));
    for bad in [
        "", "..", "../x", "a/../b", "/abs", "a//b", ".hidden", "a b", "a\0b",
    ] {
        assert!(
            matches!(tz::validate_name(bad), Err(tz::TzError::BadName(_))),
            "{bad:?}"
        );
    }
    assert!(tz::validate_name("America/Argentina/Buenos_Aires").is_ok());
    assert!(tz::validate_name("Etc/GMT+5").is_ok());
}

// ---------------------------------------------------------------- proptests

const ZONES: [&str; 5] = ["UTC", NY, BERLIN, LORD_HOWE, KOLKATA];

fn expr_strategy() -> impl Strategy<Value = String> {
    let sec = prop::sample::select(vec!["0", "*/15", "30", "*/20", "5,35"]);
    let min = prop::sample::select(vec!["*", "0", "*/5", "15,45", "0-10", "30"]);
    let hour = prop::sample::select(vec!["*", "2", "1-3", "*/6", "0,12", "1"]);
    let dom = prop::sample::select(vec!["*", "*", "L", "15W", "1", "*/10"]);
    let month = prop::sample::select(vec!["*", "*", "3,11", "*/2", "10-12"]);
    let dow = prop::sample::select(vec!["*", "*", "MON", "MON#2", "1-5", "SUN"]);
    (sec, min, hour, dom, month, dow)
        .prop_map(|(s, mi, h, d, mo, w)| format!("{s} {mi} {h} {d} {mo} {w}"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn next_after_is_monotonic_and_fires_in_matches_iteration(
        expr in expr_strategy(),
        zone in prop::sample::select(ZONES.to_vec()),
        // 2020-01-01 .. 2035-01-01, millisecond resolution
        after in 1_577_836_800_000i64..2_051_222_400_000i64,
        cap in 0usize..60,
    ) {
        let s = spec(&expr, zone);
        let fires = seq(&s, after, 50);
        prop_assert!(fires[0] > after);
        for w in fires.windows(2) {
            prop_assert!(w[0] < w[1], "{} {} not increasing: {:?}", expr, zone, w);
        }
        for f in &fires {
            prop_assert_eq!(f % 1000, 0);
            prop_assert_eq!(latest_at_or_before(&s, *f), Some(*f));
        }
        let (got, truncated) = fires_in(&s, after, fires[49], cap);
        let keep = cap.min(50);
        prop_assert_eq!(&got[..], &fires[50 - keep..]);
        prop_assert_eq!(truncated, cap < 50);
    }

    #[test]
    fn catchup_after_downtime_far_longer_than_cap_times_period(
        back_ms in 1_000i64..(30 * 86_400_000),
        sub_ms in 0i64..1000,
        cap in 1usize..=100,
        window_ms in prop::sample::select(vec![0u64, 1_000, 30_000, 3_600_000, 86_400_000]),
        zone in prop::sample::select(ZONES.to_vec()),
    ) {
        let s = spec("* * * * * *", zone);
        let now = ms("2026-11-01T06:30:00Z") + sub_ms; // inside the NY overlap
        let floor = now - back_ms;
        // The per-case runtime bound (50 ms, debug build) is checked on the fastest of up to
        // three identical runs, so a scheduler hiccup on a loaded machine cannot fail it while
        // unbounded work still would.
        let mut elapsed = std::time::Duration::MAX;
        let mut result = None;
        for _ in 0..3 {
            let started = Instant::now(); // clock-exempt: test runtime bound only
            let latest = latest_at_or_before(&s, now).unwrap();
            let skip = plan_catchup(&s, floor, 0, now, CatchupPolicy::Skip, window_ms, cap);
            let one = plan_catchup(&s, floor, 0, now, CatchupPolicy::One, window_ms, cap);
            let all = plan_catchup(&s, floor, 0, now, CatchupPolicy::All, window_ms, cap);
            elapsed = elapsed.min(started.elapsed()); // clock-exempt: test runtime bound only
            result = Some((latest, skip, one, all));
            if elapsed.as_millis() < 50 {
                break;
            }
        }
        let (latest, skip, one, all) = result.unwrap();
        prop_assert_eq!(latest, now - sub_ms);
        if latest > floor {
            prop_assert_eq!(skip.advance_to, Some(latest));
            prop_assert!(skip.fire.is_empty());
            prop_assert_eq!(&one.fire, &vec![latest]);
            prop_assert_eq!(one.advance_to, None);
        } else {
            prop_assert_eq!(&skip, &CatchupPlan::default());
            prop_assert_eq!(&one, &CatchupPlan::default());
        }
        // All: the newest fires <= now, consecutive seconds ending at `latest`, all past the
        // floor and inside the window.
        let lo = if window_ms == 0 { floor } else { floor.max(now - window_ms as i64) };
        let mut expect: Vec<i64> =
            (0..cap as i64).map(|i| latest - i * 1000).filter(|&f| f > lo).collect();
        expect.reverse();
        prop_assert_eq!(&all.fire, &expect);
        prop_assert_eq!(all.advance_to, None);
        prop_assert!(elapsed.as_millis() < 50, "{:?}", elapsed);
    }
}
