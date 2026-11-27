//! 14-32 Task 1: the pure Kafka trigger logic (D-P14-43): idempotency keys, run input,
//! the commit watermark (proptest) and the pinned owner placement.

use ostro_triggers::kafka::{
    CommitTracker, IdempotencyMode, InputError, InputMode, KEY_PREFIX, MAX_KEY_LEN, MAX_TOPIC_LEN,
    RecordView, hex, idempotency_key, input, is_topic_name, kafka_owner,
};
use proptest::prelude::*;
use serde_json::json;
use sha2::{Digest, Sha256};

fn rec<'a>(
    value: Option<&'a [u8]>,
    key: Option<&'a [u8]>,
    headers: &'a [(String, Option<&'a [u8]>)],
) -> RecordView<'a> {
    RecordView {
        topic: "orders",
        partition: 2,
        offset: 41,
        timestamp_ms: 1_700_000_000_123,
        key,
        value,
        headers,
    }
}

#[test]
fn offset_key_form() {
    let k = idempotency_key(
        "t1",
        IdempotencyMode::Offset,
        "orders",
        2,
        41,
        Some(b"ignored"),
    );
    assert_eq!(k, "__kafka/t1/orders/2/41");
    assert!(k.starts_with(KEY_PREFIX));
}

#[test]
fn message_key_form_hashes_the_key() {
    let k = idempotency_key(
        "t1",
        IdempotencyMode::MessageKey,
        "orders",
        2,
        41,
        Some(b"cust-7"),
    );
    let want = format!("__kafka/t1/k/{}", hex(&Sha256::digest(b"cust-7")));
    assert_eq!(k, want);
    assert_eq!(k.len(), "__kafka/t1/k/".len() + 64);
    // Same key on another partition/offset: same run.
    assert_eq!(
        k,
        idempotency_key(
            "t1",
            IdempotencyMode::MessageKey,
            "other",
            0,
            9,
            Some(b"cust-7")
        )
    );
    // Empty key is a key (not null).
    assert_ne!(
        idempotency_key(
            "t1",
            IdempotencyMode::MessageKey,
            "orders",
            2,
            41,
            Some(b"")
        ),
        "__kafka/t1/orders/2/41"
    );
}

#[test]
fn null_key_falls_back_to_offset_form() {
    assert_eq!(
        idempotency_key("t1", IdempotencyMode::MessageKey, "orders", 2, 41, None),
        "__kafka/t1/orders/2/41"
    );
}

#[test]
fn keys_fit_the_limit_at_maximum_lengths() {
    let name = "n".repeat(128);
    let topic = "t".repeat(MAX_TOPIC_LEN);
    for (p, o) in [(i32::MAX, i64::MAX), (i32::MIN, i64::MIN), (0, 0)] {
        let k = idempotency_key(&name, IdempotencyMode::Offset, &topic, p, o, None);
        assert!(k.len() <= MAX_KEY_LEN, "{} > {MAX_KEY_LEN}", k.len());
        assert!(k.len() <= 419, "{}", k.len());
        let k = idempotency_key(
            &name,
            IdempotencyMode::MessageKey,
            &topic,
            p,
            o,
            Some(&[0; 4096]),
        );
        assert!(k.len() <= MAX_KEY_LEN);
    }
}

#[test]
fn topic_grammar() {
    assert!(is_topic_name("orders.v1_x-y"));
    assert!(is_topic_name(&"a".repeat(249)));
    for bad in ["", ".", "..", "a/b", "a b", "ä", &"a".repeat(250)] {
        assert!(!is_topic_name(bad), "{bad:?}");
    }
}

#[test]
fn value_json_input() {
    let r = rec(Some(br#"{"a":1,"b":[true]}"#), None, &[]);
    assert_eq!(
        input(InputMode::ValueJson, &r),
        Ok(json!({"a": 1, "b": [true]}))
    );
    let r = rec(None, None, &[]);
    assert_eq!(input(InputMode::ValueJson, &r), Ok(json!(null)));
    let r = rec(Some(b"not json"), None, &[]);
    let e = input(InputMode::ValueJson, &r).expect_err("not json");
    assert!(matches!(e, InputError::NotJson(_)));
    assert_eq!(e.reason(), "not_json");
}

#[test]
fn envelope_input() {
    let headers = vec![("h1".to_owned(), Some(&b"v1"[..])), ("h2".to_owned(), None)];
    let r = rec(Some(b"\xff\x00raw"), Some(b"k"), &headers);
    let v = input(InputMode::Envelope, &r).expect("envelope");
    assert_eq!(
        v,
        json!({
            "topic": "orders",
            "partition": 2,
            "offset": 41,
            "timestamp_ms": 1_700_000_000_123_i64,
            "key_b64": "aw==",
            "headers": [{"name": "h1", "value_b64": "djE="}, {"name": "h2", "value_b64": null}],
            "value_b64": "/wByYXc=",
        })
    );
    // Envelope never fails, even for a non-JSON value; null key and tombstone are null.
    let r = rec(None, None, &[]);
    let v = input(InputMode::Envelope, &r).expect("envelope");
    assert_eq!(v["key_b64"], json!(null));
    assert_eq!(v["value_b64"], json!(null));
}

#[test]
fn modes_parse() {
    assert_eq!(
        IdempotencyMode::parse("offset"),
        Some(IdempotencyMode::Offset)
    );
    assert_eq!(
        IdempotencyMode::parse("message_key"),
        Some(IdempotencyMode::MessageKey)
    );
    assert_eq!(IdempotencyMode::parse("key"), None);
    assert_eq!(InputMode::parse("value_json"), Some(InputMode::ValueJson));
    assert_eq!(InputMode::parse("envelope"), Some(InputMode::Envelope));
    assert_eq!(InputMode::parse("raw"), None);
    for m in [IdempotencyMode::Offset, IdempotencyMode::MessageKey] {
        assert_eq!(IdempotencyMode::parse(m.as_str()), Some(m));
    }
    for m in [InputMode::ValueJson, InputMode::Envelope] {
        assert_eq!(InputMode::parse(m.as_str()), Some(m));
    }
}

#[test]
fn owner_is_pinned() {
    // Frozen with plan 14-32: Kafka consumer placement must not move across versions.
    assert_eq!(kafka_owner("orders", "orders", 0, 16), OWNER_VECTORS[0]);
    assert_eq!(kafka_owner("orders", "orders", 1, 16), OWNER_VECTORS[1]);
    assert_eq!(
        kafka_owner("billing", "invoices.v2", 7, 64),
        OWNER_VECTORS[2]
    );
    assert_eq!(kafka_owner("x", "y", 0, 1), 0);
    assert_eq!(kafka_owner("x", "y", 0, 0), 0);
    // The hash covers name, topic and partition separately (no concatenation collision).
    let a = kafka_owner("ab", "c", 0, u16::MAX);
    let b = kafka_owner("a", "bc", 0, u16::MAX);
    assert_ne!(a, b);
}

/// Cross-checked with Python hashlib when pinned.
const OWNER_VECTORS: [u16; 3] = [14, 2, 18];

#[test]
fn owner_spreads_partitions() {
    let mut seen = std::collections::BTreeSet::new();
    for kp in 0..64 {
        let o = kafka_owner("orders", "orders", kp, 8);
        assert!(o < 8);
        seen.insert(o);
    }
    assert!(seen.len() >= 6, "{seen:?}");
}

#[test]
fn tracker_skips_record_less_ranges() {
    let mut t = CommitTracker::new();
    assert_eq!(t.watermark(), None);
    t.start(10).expect("start");
    assert_eq!(t.watermark(), Some(10));
    t.start(11).expect("start");
    t.done(11).expect("done");
    assert_eq!(t.watermark(), Some(10));
    t.done(10).expect("done");
    assert_eq!(t.watermark(), Some(12));
    // A control batch at 12..=14: the position moves without records.
    t.skip_to(15).expect("skip");
    assert_eq!(t.watermark(), Some(15));
    assert!(t.skip_to(14).is_err());
    assert!(t.start(14).is_err());
    assert_eq!(t.in_flight(), 0);
}

/// One step of a random interleaving: start the next record (after `gap` record-less
/// offsets), skip forward, or finish the `i`-th in-flight record.
#[derive(Clone, Debug)]
enum Step {
    Start { gap: u8 },
    Skip { by: u8 },
    Done { pick: usize },
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (0u8..4).prop_map(|gap| Step::Start { gap }),
        1 => (0u8..4).prop_map(|by| Step::Skip { by }),
        4 => any::<usize>().prop_map(|pick| Step::Done { pick }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(512),
        ..ProptestConfig::default()
    })]

    #[test]
    fn watermark_never_passes_an_unfinished_record(
        start in 0i64..1_000_000,
        steps in proptest::collection::vec(step(), 0..200),
    ) {
        let mut t = CommitTracker::at(start);
        let mut next = start;
        let mut open: Vec<i64> = Vec::new();
        let mut last_started: Option<i64> = None;
        let mut prev = t.watermark().expect("positioned");
        prop_assert_eq!(prev, start);
        for s in steps {
            match s {
                Step::Start { gap } => {
                    let o = next + i64::from(gap);
                    t.start(o).expect("increasing");
                    open.push(o);
                    last_started = Some(o);
                    next = o + 1;
                }
                Step::Skip { by } => {
                    next += i64::from(by);
                    t.skip_to(next).expect("forward");
                }
                Step::Done { pick } => {
                    if !open.is_empty() {
                        let o = open.remove(pick % open.len());
                        t.done(o).expect("in flight");
                    }
                }
            }
            let w = t.watermark().expect("positioned");
            if let Some(&min_open) = open.iter().min() {
                prop_assert!(w <= min_open, "watermark {} passes open record {}", w, min_open);
            }
            prop_assert!(w >= prev, "watermark went back: {} -> {}", prev, w);
            prop_assert_eq!(t.in_flight(), open.len());
            prev = w;
        }
        // Finish everything: the watermark reaches the position (last started + 1, or
        // further after record-less skips).
        for o in open.drain(..) {
            t.done(o).expect("in flight");
        }
        let w = t.watermark().expect("positioned");
        prop_assert_eq!(w, next);
        if let Some(l) = last_started {
            prop_assert!(w > l);
        }
    }
}
