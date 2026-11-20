//! ROADMAP Phase 6 SC4 (per-target rate limits), through a guest (plan 06-17).
//!
//! An `http-target` for mock A sets `rate = "5/s"`, `burst = 5`; the node config sets
//! `[egress] rate_max_wait = "300ms"`, which reaches `Bridge::new` (W1). One fx-http run
//! issues 30 parallel GETs to A and 10 to a second origin B (another mock, no quota):
//!
//! - A sees at most burst + rate x max_wait (+1 for timer granularity) requests, never more
//!   than 10 within the first second; the rest fail `rate_limited` after ~300 ms (not the
//!   30 s default);
//! - B is not throttled: all 10 succeed;
//! - `ostro_effect_rate_limited_total` on `/metrics` is > 0.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::{Duration, Instant};

use common::fx::{configure, deploy_fx, wait_terminal};
use common::{BridgeTestOpts, start_server_with_bridge, tmpfs_dir};
use ostro_testkit::mock_provider::MockProvider;
use ostro_types::journal::{Event, EventBody, Failure, OpOutput, decode_result};
use serde_json::{Value, json};

const A_REQUESTS: usize = 30;
const B_REQUESTS: usize = 10;
const MAX_WAIT: Duration = Duration::from_millis(300);

/// `(op, scheduled ts, completed ts, outcome)` of every effect, in journal order.
fn effects(j: &[Event]) -> Vec<(u64, u64, u64, Result<OpOutput, Failure>)> {
    j.iter()
        .filter(|e| matches!(e.body, EventBody::EffectScheduled(_)))
        .map(|e| {
            let (ts, r) = j
                .iter()
                .find_map(|c| match &c.body {
                    EventBody::EffectCompleted(d) if d.op == e.seq => Some((
                        c.ts_nanos,
                        decode_result::<Result<OpOutput, Failure>>(&d.outcome).expect("decode"),
                    )),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("op {} never completed", e.seq));
            (e.seq, e.ts_nanos, ts, r)
        })
        .collect()
}

fn failure_kind(r: &Result<OpOutput, Failure>) -> Option<&str> {
    match r {
        Err(Failure::Retryable(i) | Failure::NonRetryable(i)) => Some(i.kind.as_str()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_target_rate_limit() {
    let a = MockProvider::start().await;
    let b = MockProvider::start().await;
    let s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            extra_toml: "[egress]\nrate_max_wait = \"300ms\"\n".into(),
            ..BridgeTestOpts::default()
        },
    )
    .await;
    // W1: the configured max wait reached the bridge.
    assert_eq!(s.cfg.egress.rate_max_wait.get(), MAX_WAIT);
    let dbg = format!("{:?}", s.h().bridge().bridge());
    assert!(dbg.contains("rate_max_wait: 300ms"), "{dbg}");

    let module = deploy_fx(&s, "fx-http").await;
    configure(&s)
        .connection(
            &a.base_url(),
            "http-target",
            json!({ "v": 1, "rate": "5/s", "burst": 5 }),
        )
        .egress(&module, &[&a.base_url(), &b.base_url()])
        .apply()
        .await;

    let mut reqs: Vec<Value> = (0..A_REQUESTS)
        .map(|_| json!({ "method": "GET", "url": format!("{}/status/200", a.base_url()) }))
        .collect();
    reqs.extend(
        (0..B_REQUESTS)
            .map(|_| json!({ "method": "GET", "url": format!("{}/status/200", b.base_url()) })),
    );
    let t0 = Instant::now();
    let run = s
        .start_rest(
            "fx-http",
            json!({ "input": { "requests": reqs, "parallel": true } }),
        )
        .await;
    let info = wait_terminal(&s, &run, Duration::from_secs(60)).await;
    let took = t0.elapsed();
    assert_eq!(
        serde_json::to_value(info.status).expect("status"),
        "completed",
        "{info:?}"
    );
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json");
    let items = out["results"].as_array().expect("results");
    assert_eq!(items.len(), A_REQUESTS + B_REQUESTS);

    // Guest view.
    let (a_items, b_items) = items.split_at(A_REQUESTS);
    let a_ok = a_items.iter().filter(|i| i["status"] == 200).count();
    let a_limited = a_items
        .iter()
        .filter(|i| i["error"]["kind"] == "rate_limited")
        .count();
    let b_ok = b_items.iter().filter(|i| i["status"] == 200).count();
    println!(
        "run took {took:?}: A ok {a_ok}, A rate_limited {a_limited}, B ok {b_ok}; A saw {}, B saw {}",
        a.requests_to("/status/200"),
        b.requests_to("/status/200")
    );
    assert_eq!(
        a_ok + a_limited,
        A_REQUESTS,
        "A: every request either ran or was limited: {out}"
    );
    assert_eq!(
        a.requests_to("/status/200"),
        a_ok,
        "A saw exactly the successful ones"
    );
    // Burst 5, then 5/s for at most ~300 ms of waiting (+1 slack): far below 10 per second.
    assert!(a_ok >= 5, "the burst passes: {a_ok}");
    assert!(a_ok <= 10, "quota exceeded: {a_ok} requests reached A");
    assert!(
        a_limited >= A_REQUESTS - 10,
        "rate_limited failures: {a_limited}"
    );
    assert_eq!(b_ok, B_REQUESTS, "B throttled: {out}");
    assert_eq!(b.requests_to("/status/200"), B_REQUESTS);

    // rate_limited after max_wait (~300 ms), not the 30 s default.
    let j = s.journal(&run).await;
    let fx = effects(&j);
    assert_eq!(fx.len(), A_REQUESTS + B_REQUESTS);
    let waits: Vec<Duration> = fx
        .iter()
        .filter(|(_, _, _, r)| failure_kind(r) == Some("rate_limited"))
        .map(|(_, sch, done, _)| Duration::from_nanos(done.saturating_sub(*sch)))
        .collect();
    assert_eq!(waits.len(), a_limited);
    let (min, max) = (
        waits.iter().min().copied().expect("some"),
        waits.iter().max().copied().expect("some"),
    );
    println!("rate_limited after {min:?}..{max:?} (scheduled -> completed)");
    assert!(
        min >= Duration::from_millis(250),
        "limited before max_wait: {min:?}"
    );
    assert!(
        max < Duration::from_secs(10),
        "max_wait not applied: {max:?}"
    );
    assert!(took < Duration::from_secs(30), "run took {took:?}");

    // The counter.
    let r = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    let text = r.text().await.expect("metrics text");
    let total: f64 = text
        .lines()
        .filter(|l| l.starts_with("ostro_effect_rate_limited_total"))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum();
    println!("ostro_effect_rate_limited_total = {total}");
    assert!(
        total >= a_limited as f64,
        "metric {total} < {a_limited}:\n{text}"
    );
}
