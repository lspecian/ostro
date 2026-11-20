//! Effect builders (plan 06-09): the builders must produce exactly the WIT request records
//! a hand-written call would, and the semantic idempotency key (P-11) is pinned by vectors.
//!
//! WIT records do not implement `PartialEq`, so records are compared through `Debug`.

use std::time::Duration;

use ostro_sdk::effects::{
    HttpBuilder, ObjectClient, ObjectRequest, SqlBuilder, SqlRequest, SqlValue, idempotency_key,
};
use ostro_sdk::{Header, HttpRequest, RetryPolicy};

fn h(name: &str, value: &str) -> Header {
    Header {
        name: name.to_string(),
        value: value.to_string(),
    }
}

fn dbg<T: std::fmt::Debug>(v: &T) -> String {
    format!("{v:?}")
}

// Vectors computed once with an independent implementation (Python hashlib):
// "ostro-" + sha256(b"ostro/idem/v1/name" || run_id || 0x00 || name).hexdigest()[:32].
#[test]
fn idempotency_key_vectors_are_pinned() {
    let cases = [
        (
            "01JD0000000000000000000000",
            "charge",
            "ostro-7e4f3c6105da7764ca8b1b0de8474dc8",
        ),
        (
            "01JD0000000000000000000001",
            "charge",
            "ostro-3fd0cc9176c58c3613888765e5c24636",
        ),
        (
            "01JD0000000000000000000000",
            "refund",
            "ostro-2b0b735cdad032033a44c282c78b1dde",
        ),
        ("run-a", "", "ostro-2910e2fe7e178fb732b0e122b5d1824a"),
        // The 0x00 separator keeps (run, name) boundaries unambiguous.
        ("ab", "c", "ostro-b9e2efbce72ca59fb7f39cf230f46eb6"),
        ("a", "bc", "ostro-ad696e446776bbe307a5ddba5c153666"),
    ];
    for (run, name, want) in cases {
        let k = idempotency_key(run, name);
        assert_eq!(k, want, "run={run} name={name}");
        assert_eq!(k.len(), 38);
    }
}

#[test]
fn idempotency_key_is_stable_and_differs_across_runs() {
    let a = idempotency_key("run-1", "charge");
    assert_eq!(a, idempotency_key("run-1", "charge"));
    assert_ne!(a, idempotency_key("run-2", "charge"));
    assert_ne!(a, idempotency_key("run-1", "charge2"));
    assert!(a.starts_with("ostro-"));
    assert!(
        a[6..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
}

#[test]
fn http_builder_matches_hand_built_record() {
    let built = HttpBuilder::new("POST", "https://api.example.com/v1/charges")
        .header("X-Trace", "t1")
        .secret_header("Authorization", "Bearer {{secret:stripe}}")
        .json(&serde_json::json!({ "amount": 5 }))
        .unwrap()
        .idempotency_key("ostro-abc")
        .timeout(Duration::from_millis(2500))
        .retry(RetryPolicy {
            max_attempts: 3,
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(2),
            backoff_coefficient: 2.0,
            non_retryable_kinds: vec!["bad".into()],
        })
        .redact(["Authorization", "Set-Cookie"])
        .build();

    let hand = HttpRequest {
        method: "POST".into(),
        url: "https://api.example.com/v1/charges".into(),
        headers: vec![
            h("X-Trace", "t1"),
            h("Authorization", "Bearer {{secret:stripe}}"),
            h("content-type", "application/json"),
        ],
        body: Some(br#"{"amount":5}"#.to_vec()),
        timeout_ms: Some(2500),
        idempotency_key: Some("ostro-abc".into()),
        retry: Some(ostro_sdk::bindings::ostro::runtime::types::RetryPolicy {
            max_attempts: 3,
            initial_interval_ms: 100,
            max_interval_ms: 2000,
            backoff_coefficient: 2.0,
            non_retryable_kinds: vec!["bad".into()],
        }),
        redact: vec!["Authorization".into(), "Set-Cookie".into()],
    };
    assert_eq!(dbg(&built), dbg(&hand));
}

#[test]
fn http_builder_defaults_are_empty() {
    let built = HttpBuilder::new("GET", "https://x.test/").build();
    let hand = HttpRequest {
        method: "GET".into(),
        url: "https://x.test/".into(),
        headers: vec![],
        body: None,
        timeout_ms: None,
        idempotency_key: None,
        retry: None,
        redact: vec![],
    };
    assert_eq!(dbg(&built), dbg(&hand));
}

#[test]
fn json_replaces_an_existing_content_type() {
    let built = HttpBuilder::new("PUT", "https://x.test/")
        .header("Content-Type", "text/plain")
        .json(&[1, 2])
        .unwrap()
        .build();
    assert_eq!(
        dbg(&built.headers),
        dbg(&vec![h("content-type", "application/json")])
    );
    assert_eq!(built.body.as_deref(), Some(&b"[1,2]"[..]));
}

#[test]
fn raw_body_is_kept() {
    let built = HttpBuilder::new("POST", "https://x.test/")
        .body(vec![0u8, 1, 2])
        .build();
    assert_eq!(built.body, Some(vec![0, 1, 2]));
}

#[test]
#[should_panic(expected = "{{secret:")]
fn secret_header_requires_a_template() {
    let _ = HttpBuilder::new("GET", "https://x.test/").secret_header("Authorization", "Bearer x");
}

#[test]
fn sql_builder_matches_hand_built_record() {
    let built = SqlBuilder::new("orders").exactly_once().request(
        "insert into t values ($1, $2)",
        [SqlValue::Int8(7), SqlValue::Null],
    );
    let hand = SqlRequest {
        connection: "orders".into(),
        statement: "insert into t values ($1, $2)".into(),
        params: vec![SqlValue::Int8(7), SqlValue::Null],
        exactly_once: true,
    };
    assert_eq!(dbg(&built), dbg(&hand));

    let plain = SqlBuilder::new("orders").request("select 1", []);
    assert!(!plain.exactly_once);
    assert!(plain.params.is_empty());
}

#[test]
fn object_client_builds_w7_op_strings() {
    let c = ObjectClient::new("blobs");
    let cases: [(&str, Option<Vec<u8>>); 6] = [
        ("get", None),
        ("put", Some(b"hi".to_vec())),
        ("delete", None),
        ("head", None),
        ("presign-get", None),
        ("presign-put", None),
    ];
    for (op, body) in cases {
        let built = c.request(op, "a/b.txt", body.clone());
        let hand = ObjectRequest {
            connection: "blobs".into(),
            op: op.into(),
            key: "a/b.txt".into(),
            body,
        };
        assert_eq!(dbg(&built), dbg(&hand));
    }
}
