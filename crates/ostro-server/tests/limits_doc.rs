//! Doc sync (plan 11-11, OPS-07 / D-P11-13): `docs/limits.md` == `GET /v1/limits` (default
//! config) == the code constants.
//!
//! The doc table has one row per limit: column 1 is the name in backticks, column 2 the
//! integer default. Status and context share one constant (`MAX_CONTEXT_BYTES`), so they
//! are one row (14 rows in all, with 11-10's rerun prefix bound).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_server::config::ServerConfig;
use p11::P11Server;
use reqwest::StatusCode;

/// `name -> default` from the markdown table of `docs/limits.md`.
fn doc_rows() -> Vec<(String, u64)> {
    let path = common::repo_root().join("docs/limits.md");
    let text = std::fs::read_to_string(&path).expect("read docs/limits.md");
    let mut out = Vec::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        // `| `name` | 123 | ...` -> ["", "`name`", "123", ...]
        if cols.len() < 4 || !cols[0].is_empty() {
            continue;
        }
        let Some(name) = cols[1].strip_prefix('`').and_then(|c| c.strip_suffix('`')) else {
            continue;
        };
        let value: u64 = cols[2]
            .parse()
            .unwrap_or_else(|_| panic!("docs/limits.md row {name}: default {:?}", cols[2]));
        out.push((name.to_owned(), value));
    }
    out
}

/// What the code says each documented limit is under the default server config.
fn constants() -> BTreeMap<&'static str, u64> {
    let run = ostro_engine::RunLimits::default();
    let cfg = ServerConfig::server_defaults();
    let n = |v: usize| u64::try_from(v).expect("u64");
    // The API and the store define the shared caps independently; they must agree.
    assert_eq!(
        ostro_api::domain::MAX_SIGNAL_NAME_LEN,
        ostro_types::command::MAX_SIGNAL_NAME_LEN
    );
    assert_eq!(
        ostro_api::domain::MAX_REASON_LEN,
        ostro_types::command::MAX_REASON_LEN
    );
    assert_eq!(
        ostro_api::domain::MAX_REQUEST_ID_LEN,
        ostro_types::command::MAX_REQUEST_ID_LEN
    );
    // [limits] defaults == the engine defaults; the payload default == the request cap.
    let limits = ostro_server::config::LimitsSection::default();
    assert_eq!(limits.to_run_limits(), run);
    assert_eq!(run.max_payload_bytes, cfg.api.max_request_bytes);
    BTreeMap::from([
        ("max_history_events", run.max_history_events),
        ("max_children_per_run", run.max_children_per_run),
        ("max_payload_bytes", run.max_payload_bytes),
        ("max_request_bytes", cfg.api.max_request_bytes),
        ("max_module_bytes", cfg.api.max_module_bytes),
        (
            "MAX_CONTEXT_BYTES",
            n(ostro_engine::host::context::MAX_CONTEXT_BYTES),
        ),
        ("MAX_RUN_TAGS", n(ostro_types::command::MAX_RUN_TAGS)),
        ("MAX_RUN_TAG_LEN", n(ostro_types::command::MAX_RUN_TAG_LEN)),
        (
            "MAX_SIGNAL_NAME_LEN",
            n(ostro_types::command::MAX_SIGNAL_NAME_LEN),
        ),
        ("MAX_REASON_LEN", n(ostro_types::command::MAX_REASON_LEN)),
        (
            "MAX_REQUEST_ID_LEN",
            n(ostro_types::command::MAX_REQUEST_ID_LEN),
        ),
        (
            "MAX_PAGE_LIMIT",
            u64::from(ostro_api::domain::MAX_PAGE_LIMIT),
        ),
        ("MAX_RESULT_WAIT_MS", ostro_api::domain::MAX_RESULT_WAIT_MS),
        (
            "MAX_RERUN_PREFIX_BYTES",
            n(ostro_server::control::p11_rerun::MAX_RERUN_PREFIX_BYTES),
        ),
    ])
}

#[test]
fn doc_table_equals_code_constants() {
    let doc = doc_rows();
    assert_eq!(doc.len(), 14, "one row per limit: {doc:?}");
    let consts = constants();
    let doc_map: BTreeMap<&str, u64> = doc.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    assert_eq!(doc_map.len(), doc.len(), "duplicate doc rows: {doc:?}");
    assert_eq!(doc_map, consts);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doc_table_equals_the_endpoint() {
    let mut s = P11Server::builder().start().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let (st, v) = s.get_json("/v1/limits").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let endpoint: Vec<(String, u64)> = v["limits"]
        .as_array()
        .expect("limits")
        .iter()
        .map(|l| {
            assert!(l["unit"].as_str().is_some_and(|u| !u.is_empty()), "{l}");
            assert!(l["enforced"].as_str().is_some_and(|u| !u.is_empty()), "{l}");
            assert!(l["error"].as_str().is_some_and(|u| !u.is_empty()), "{l}");
            (
                l["name"].as_str().expect("name").to_owned(),
                l["value"].as_u64().expect("value"),
            )
        })
        .collect();
    // Same rows, same order, same values.
    assert_eq!(endpoint, doc_rows());
    // `configurable` matches the doc's column 4 (a config key, or "no").
    for l in v["limits"].as_array().expect("limits") {
        let name = l["name"].as_str().expect("name");
        let configurable = name.chars().next().is_some_and(char::is_lowercase);
        assert_eq!(l["configurable"], configurable, "{l}");
    }
    s.shutdown().await;
}
