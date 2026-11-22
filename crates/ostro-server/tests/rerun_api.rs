//! Rerun control plane end to end (plan 11-10, OPS-02, D-P11-3/4/5/7/8/27) on the real
//! server with `guests/p11-rerun{,-v2}` (`pipeline` v1 fails at `s<fail_at>`, v2 never
//! fails at `s3`) and `guests/long-journal`:
//!
//! - fix and rerun: v1 fails at `s3`, v2 is deployed and activated, `POST /rerun
//!   {from: {step: s3}}` completes with 5 on v2; events `0..cut` are the source rows byte
//!   for byte, `s1`/`s2` are not re-executed, `retry_of`, user tags and
//!   `__ostro/rerun_from=<cut>`; `version: source` fails again on v1, `version: 2` runs v2,
//!   an unknown version is 404; dry run submits nothing; idempotency key = exactly once;
//!   terminal-event cut and unknown step are refused; a running source is
//!   `run_not_terminal`; 64 user tags + the lineage tag are 400 `limit: tags`;
//!   `GET /rerun-cuts` matches `analyze_cut`;
//! - nondeterminism: a guest that put `run-info.run_id` in an HTTP body before the cut is
//!   409 `rerun_nondeterministic{seq}` and nothing is submitted; a cut inside the open
//!   effect is 409 `rerun_cut_unsafe{nearest_safe_seq}`; the nearest safe cut (before the
//!   effect) is accepted and the effect re-executes under the new run id (MockProvider
//!   `/echo` sees a second request carrying it);
//! - envelope: a 3,000-step source reruns from its terminal cut; with the bound lowered
//!   for that source (test hook) the big inline step output is offloaded to a blob and
//!   resolves to the same bytes; lower still is 413 `rerun_prefix_too_large`;
//! - blobs (D-P11-27): the input blob shared by source and rerun survives a blob-GC pass
//!   after the source expired, while an orphan blob is collected;
//! - DLQ requeue `mode: rerun` (11-12 handoff): new run, `requeued_to`, idempotent repeat.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::sync::Arc;
use std::time::Duration;

use ostro_partition::store::keys::event_key;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{Table, query};
use ostro_server::control::history::event_seq_and_attempt;
use ostro_server::control::p11_rerun::set_prefix_bound_for_source;
use ostro_server::rerun::{analyze_cut, rerun_from_tag};
use ostro_server::services::blob_gc::BlobGc;
use ostro_testkit::mock_provider::MockProvider;
use ostro_types::RunId;
use ostro_types::command::RERUN_FROM_TAG;
use ostro_types::journal::{EffectRequest, Event, EventBody, EventKind};
use ostro_types::payload::payload_ref;
use p11::{P11Server, from_seq, from_step};
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(90);

async fn server_with(
    f: impl FnOnce(&mut ostro_server::config::ServerConfig) + Send + 'static,
) -> P11Server {
    let s = P11Server::builder()
        .with_config(|c| {
            c.engine.hot_wait = Duration::from_millis(50).into();
            f(c);
        })
        .start()
        .await;
    s.wait_ready(Duration::from_secs(20)).await;
    s
}

async fn terminal(s: &P11Server, run: &RunId) -> RunRecord {
    s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await
}

/// `PUT /v1/workflows/{name}/active {version}`.
async fn activate(s: &P11Server, name: &str, version: u32) {
    let r = s
        .http
        .put(s.url(&format!("/v1/workflows/{name}/active")))
        .json(&json!({ "version": version }))
        .send()
        .await
        .expect("PUT active");
    assert!(
        r.status().is_success(),
        "activate {name} v{version}: {}",
        r.status()
    );
}

/// REST start of `pipeline` with `body` extras; waits for the terminal record.
async fn run_pipeline(s: &P11Server, input: Value, extra: Value) -> (RunId, RunRecord) {
    let mut body = json!({ "input": input });
    if let (Value::Object(b), Value::Object(e)) = (&mut body, extra) {
        b.extend(e);
    }
    let run = s.start_rest("pipeline", body).await;
    let rec = terminal(s, &run).await;
    (run, rec)
}

fn new_run(v: &Value) -> RunId {
    v["new_run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("new_run_id: {v}"))
        .parse()
        .expect("run id")
}

fn details(v: &Value) -> &Value {
    &v["error"]["details"]
}

/// The raw stored `events` rows of `run`, in seq order.
fn raw_rows(s: &P11Server, run: &RunId) -> Vec<Vec<u8>> {
    let reader = s.handle_of(run).reader();
    let mut out = Vec::new();
    for seq in 0.. {
        match reader
            .get(Table::Events, &event_key(run, seq))
            .expect("read")
        {
            Some(v) => out.push(v.to_vec()),
            None => break,
        }
    }
    out
}

/// `(seq, kind, resolved payload bytes, attempt)` of every stored row (W15 identity).
async fn resolved_rows(s: &P11Server, run: &RunId) -> Vec<(u64, u32, Vec<u8>, u64)> {
    let h = s.handle_of(run);
    let rows = query::journal_rows(&h.reader(), run, 0, usize::MAX).expect("rows");
    let raw = raw_rows(s, run);
    assert_eq!(rows.len(), raw.len());
    let mut out = Vec::new();
    for ((seq, kind, p), r) in rows.into_iter().zip(raw) {
        let bytes = h.fetch_payload(&p).await.expect("payload").to_vec();
        let (_, attempt) = event_seq_and_attempt(&r).expect("row");
        out.push((seq, kind, bytes, attempt));
    }
    out
}

fn blob_rows(s: &P11Server, run: &RunId) -> usize {
    let h = s.handle_of(run);
    query::journal_rows(&h.reader(), run, 0, usize::MAX)
        .expect("rows")
        .into_iter()
        .filter(|(_, _, p)| matches!(p.kind, Some(payload_ref::Kind::Blob(_))))
        .count()
}

fn steps_named(j: &[Event], name: &str) -> usize {
    j.iter()
        .filter(|e| matches!(&e.body, EventBody::StepCompleted(s) if s.name == name))
        .count()
}

fn first_step(j: &[Event], name: &str) -> u64 {
    j.iter()
        .find(|e| matches!(&e.body, EventBody::StepCompleted(s) if s.name == name))
        .unwrap_or_else(|| panic!("step {name}"))
        .seq
}

fn result_json(bytes: Option<Vec<u8>>) -> Value {
    serde_json::from_slice(&bytes.expect("result")).expect("json")
}

// ---------------------------------------------------------------------------------
// Fix and rerun, versions, dry run, idempotency, refusals, rerun-cuts
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fix_and_rerun_versions_idempotency_and_refusals() {
    let mut s = server_with(|_| {}).await;
    s.deploy_guest("p11-rerun", None).await;
    let (src, rec) =
        run_pipeline(&s, json!({"fail_at": 3}), json!({"tags": ["team:a", "x"]})).await;
    assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    let sj = s.journal(&src).await;
    let cut = first_step(&sj, "s3");

    // Dry run on v1 (the only, active version): the plan, nothing submitted.
    let before = s.list_all("").await.len();
    let (st, v) = s
        .rerun(
            &src.to_string(),
            &json!({"from": from_step("s3", 1), "dry_run": true}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert!(v.get("new_run_id").is_none(), "{v}");
    assert_eq!(v["cut_seq"], cut);
    assert_eq!(v["copied_events"], cut);
    assert_eq!(v["verdict"], "ok_frontier");
    assert_eq!(v["version"], 1);
    assert_eq!(
        s.list_all("").await.len(),
        before,
        "dry run submits nothing"
    );

    // Fix: deploy v2 and activate it.
    s.deploy_guest("p11-rerun-v2", None).await;
    activate(&s, "pipeline", 2).await;

    // Default version = active (v2): completes with 5.
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_step("s3", 1)}))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["version"], 2);
    assert_eq!(v["existing"], false);
    let run = new_run(&v);
    let nrec = terminal(&s, &run).await;
    assert_eq!(nrec.status, RunStatus::Completed, "{nrec:?}");
    assert_eq!(result_json(s.result_bytes(&run).await), json!(5));
    assert_eq!(nrec.retry_of, Some(src));
    assert_eq!(nrec.workflow_version, 2);
    assert_eq!(rerun_from_tag(&nrec.tags), Some(cut));
    assert_eq!(
        nrec.tags,
        vec![
            "team:a".to_owned(),
            "x".to_owned(),
            format!("{RERUN_FROM_TAG}{cut}")
        ]
    );
    assert_eq!(nrec.parent, None, "reruns are detached");
    // The UI names the source from the RunStarted history details (`retry_of`); an
    // ordinary run's details carry no such key.
    let (st, hv) = s.get_json(&format!("/v1/runs/{run}/history?limit=1")).await;
    assert_eq!(st, StatusCode::OK, "{hv}");
    let first = &hv["events"][0];
    assert_eq!(first["kind"], "run_started", "{hv}");
    assert_eq!(first["details"]["retry_of"], src.to_string(), "{hv}");
    let (_, hv) = s.get_json(&format!("/v1/runs/{src}/history?limit=1")).await;
    assert!(hv["events"][0]["details"].get("retry_of").is_none(), "{hv}");
    let cut_us = usize::try_from(cut).expect("cut");
    assert_eq!(
        raw_rows(&s, &run)[..cut_us],
        raw_rows(&s, &src)[..cut_us],
        "events 0..cut are the source rows, byte for byte (no offloading needed)"
    );
    let nj = s.journal(&run).await;
    for name in ["s1", "s2", "s3", "s4", "s5"] {
        assert_eq!(
            steps_named(&nj, name),
            1,
            "{name}: copied steps never re-run"
        );
    }
    // `s1`/`s2` are the copied rows (seq < cut); `s3` ran live after the cut.
    assert!(first_step(&nj, "s2") < cut && first_step(&nj, "s3") >= cut);

    // rerun-cuts on the completed rerun: s1..s5, all safe, seqs = analyze_cut's view.
    let (st, cv) = s.get_json(&format!("/v1/runs/{run}/rerun-cuts")).await;
    assert_eq!(st, StatusCode::OK, "{cv}");
    let cuts = cv["cuts"].as_array().expect("cuts");
    let names: Vec<&str> = cuts
        .iter()
        .map(|c| c["step"].as_str().expect("step"))
        .collect();
    assert_eq!(names, ["s1", "s2", "s3", "s4", "s5"]);
    for c in cuts {
        let seq = c["seq"].as_u64().expect("seq");
        assert_eq!(c["occurrence"], 1);
        assert_eq!(c["safe"], analyze_cut(&nj, seq).is_ok(), "{c}");
        assert_eq!(seq, first_step(&nj, c["step"].as_str().expect("step")));
    }

    // version: source reruns on v1 and fails at s3 again.
    let (st, v) = s
        .rerun(
            &src.to_string(),
            &json!({"from": from_step("s3", 1), "version": "source"}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["version"], 1);
    let r1 = new_run(&v);
    let r1rec = terminal(&s, &r1).await;
    assert_eq!(r1rec.status, RunStatus::Failed, "v1 still fails at s3");
    assert_eq!(r1rec.workflow_version, 1);

    // An explicit version.
    let (st, v) = s
        .rerun(
            &src.to_string(),
            &json!({"from": from_seq(cut), "version": {"version": 2}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let r2 = new_run(&v);
    assert_eq!(terminal(&s, &r2).await.status, RunStatus::Completed);
    let (st, v) = s
        .rerun(
            &src.to_string(),
            &json!({"from": from_seq(cut), "version": {"version": 99}}),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");

    // Idempotency: __rerun/<source>/<key>, exactly once.
    let body = json!({"from": from_step("s3", 1), "idempotency_key": "k1"});
    let (st, v1) = s.rerun(&src.to_string(), &body).await;
    assert_eq!(st, StatusCode::CREATED, "{v1}");
    let (st, v2) = s.rerun(&src.to_string(), &body).await;
    assert_eq!(st, StatusCode::OK, "{v2}");
    assert_eq!(v2["existing"], true);
    assert_eq!(v2["new_run_id"], v1["new_run_id"]);
    let k = new_run(&v1);
    assert_eq!(
        String::from_utf8_lossy(&s.run_record(&k).idempotency_key),
        format!("__rerun/{src}/k1")
    );

    // Refusals: the terminal event's cut, an unknown step.
    let failed_at = sj
        .iter()
        .find(|e| e.body.kind() == EventKind::RunFailed)
        .expect("RunFailed")
        .seq;
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_seq(failed_at + 1)}))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(details(&v)["reason"], "rerun_cut_unsafe");
    assert_eq!(details(&v)["detail"], "terminal_or_out_of_range");
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_step("s9", 1)}))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");

    // A running source (sleeping a minute before s3) is not terminal.
    let live = s
        .start_rest("pipeline", json!({"input": {"sleep_between_ms": 60_000}}))
        .await;
    s.wait_record(&live, LIMIT, |r| r.next_seq > 3).await;
    let (st, v) = s
        .rerun(&live.to_string(), &json!({"from": from_seq(1)}))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(details(&v)["reason"], "run_not_terminal");

    // 64 user tags + the lineage tag > 64.
    let tags: Vec<String> = (0..64).map(|i| format!("t{i}")).collect();
    let (many, rec) = run_pipeline(&s, json!({"fail_at": 3}), json!({ "tags": tags })).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "v2 is active now: {rec:?}"
    );
    let (st, v) = s
        .rerun(&many.to_string(), &json!({"from": from_step("s3", 1)}))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(details(&v)["limit"], "tags");
    assert_eq!(details(&v)["max"], 64);
    assert_eq!(details(&v)["actual"], 65);

    // Unknown source.
    let ghost = RunId::new_v7(src.partition());
    let (st, v) = s
        .rerun(&ghost.to_string(), &json!({"from": from_seq(1)}))
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Nondeterminism, unsafe cut, effects re-executed under the new run id
// ---------------------------------------------------------------------------------

fn effect_body_run_id(j: &[Event]) -> Option<String> {
    j.iter().find_map(|e| match &e.body {
        EventBody::EffectScheduled(es) => match &es.request {
            EffectRequest::Http(h) => {
                let b: Value = serde_json::from_slice(h.body.as_deref()?).ok()?;
                b["run_id"].as_str().map(str::to_owned)
            }
            _ => None,
        },
        _ => None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nondeterministic_prefix_unsafe_cut_and_reexecuted_effect() {
    let mock = MockProvider::start().await;
    let mut s = server_with(|c| c.egress.allow_private = vec!["127.0.0.0/8".into()]).await;
    let module = s.deploy_named("p11-rerun", "p11-rerun", None).await;
    common::put_egress(&s, &module, &[&mock.base_url()]).await;
    let url = format!("{}/echo", mock.base_url());
    let (src, rec) = run_pipeline(
        &s,
        json!({"fail_at": 4, "effect_url": url, "put_run_id_in_body": true}),
        json!({}),
    )
    .await;
    assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    assert_eq!(mock.requests_to("/echo"), 1);
    let sj = s.journal(&src).await;
    assert_eq!(effect_body_run_id(&sj), Some(src.to_string()));
    let eff = sj
        .iter()
        .find(|e| e.body.kind() == EventKind::EffectScheduled)
        .expect("EffectScheduled")
        .seq;

    // A cut after the effect: the copied effect command carries the old run id, the new
    // run's guest computes another one -> nondeterministic, nothing submitted.
    let before = s.list_all("").await.len();
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_step("s3", 1)}))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(details(&v)["reason"], "rerun_nondeterministic");
    assert_eq!(details(&v)["seq"], eff, "{v}");
    assert!(
        details(&v)["detail"]
            .as_str()
            .is_some_and(|d| !d.is_empty())
    );
    let (st, v) = s
        .rerun(
            &src.to_string(),
            &json!({"from": from_step("s3", 1), "dry_run": true}),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "dry runs run the check too: {v}");
    assert_eq!(s.list_all("").await.len(), before, "nothing submitted");

    // A cut inside the open effect is unsafe; the nearest safe cut is the effect itself.
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_seq(eff + 1)}))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(details(&v)["reason"], "rerun_cut_unsafe");
    assert_eq!(details(&v)["detail"], "open_effect");
    assert_eq!(details(&v)["seq"], eff);
    assert_eq!(details(&v)["nearest_safe_seq"], eff);
    let (st, cv) = s.get_json(&format!("/v1/runs/{src}/rerun-cuts")).await;
    assert_eq!(st, StatusCode::OK, "{cv}");
    let effect_cut = cv["cuts"]
        .as_array()
        .expect("cuts")
        .iter()
        .find(|c| c["seq"] == eff)
        .expect("the effect op is listed");
    assert!(
        effect_cut.get("step").is_none() && effect_cut["safe"] == true,
        "{effect_cut}"
    );

    // The nearest safe cut (before the effect) is accepted: the effect re-executes live
    // under the new run id.
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_seq(eff)}))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let run = new_run(&v);
    let nrec = terminal(&s, &run).await;
    assert_eq!(nrec.status, RunStatus::Failed, "v1 fails at s4 again");
    assert_eq!(
        mock.requests_to("/echo"),
        2,
        "the effect after the cut re-executed"
    );
    let nj = s.journal(&run).await;
    assert_eq!(effect_body_run_id(&nj), Some(run.to_string()));
    assert_eq!(steps_named(&nj, "s1"), 1, "s1 copied, not re-run");
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Envelope fitting
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn envelope_offloads_large_payloads_or_refuses() {
    const STEPS: u64 = 3_000;
    const BIG: u64 = 200_000;
    let mut s = server_with(|_| {}).await;
    s.deploy_guest("long-journal", None).await;
    let src = s
        .start_rest(
            "long-journal",
            json!({"input": {"steps": STEPS, "result_bytes": BIG}}),
        )
        .await;
    let rec = s
        .wait_record(&src, Duration::from_secs(300), |r| r.status.is_terminal())
        .await;
    assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    let sj = s.journal(&src).await;
    assert!(sj.len() as u64 > STEPS, "{} events", sj.len());
    let cut = sj.len() as u64 - 1; // everything but RunCompleted
    let src_rows = resolved_rows(&s, &src).await;
    let src_bytes: usize = raw_rows(&s, &src)
        .iter()
        .take(cut as usize)
        .map(Vec::len)
        .sum();
    assert_eq!(blob_rows(&s, &src), 0, "the source rows are all inline");

    // Default bound: fits as is.
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_seq(cut)}))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let plain = new_run(&v);
    assert_eq!(terminal(&s, &plain).await.status, RunStatus::Completed);
    assert_eq!(blob_rows(&s, &plain), 0);

    // Lowered bound: the 200 KB step output must be offloaded; it resolves identically.
    let bound = src_bytes - (BIG as usize) / 2;
    set_prefix_bound_for_source(&src.to_string(), Some(bound));
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_seq(cut)}))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let fitted = new_run(&v);
    let frec = terminal(&s, &fitted).await;
    assert_eq!(frec.status, RunStatus::Completed, "{frec:?}");
    assert_eq!(
        blob_rows(&s, &fitted),
        1,
        "exactly the big payload was offloaded"
    );
    let new_rows = resolved_rows(&s, &fitted).await;
    let cut_us = usize::try_from(cut).expect("cut");
    assert_eq!(
        new_rows[..cut_us],
        src_rows[..cut_us],
        "seq, kind, resolved payload and attempt identical (W15)"
    );
    let fitted_bytes: usize = raw_rows(&s, &fitted)
        .iter()
        .take(cut_us)
        .map(Vec::len)
        .sum();
    assert!(fitted_bytes <= bound, "{fitted_bytes} > {bound}");
    eprintln!(
        "envelope: {} events, prefix rows {src_bytes} B inline; bound {bound} B -> {fitted_bytes} B after offloading 1 payload",
        cut
    );
    assert_eq!(
        result_json(s.result_bytes(&fitted).await),
        result_json(s.result_bytes(&src).await)
    );

    // Too small even after offloading: 413.
    set_prefix_bound_for_source(&src.to_string(), Some(fitted_bytes / 2));
    let (st, v) = s
        .rerun(&src.to_string(), &json!({"from": from_seq(cut)}))
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(details(&v)["reason"], "rerun_prefix_too_large");
    assert_eq!(details(&v)["limit"], "rerun_prefix_bytes");
    set_prefix_bound_for_source(&src.to_string(), None);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Shared blobs survive GC after the source expires (D-P11-27)
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_input_blob_survives_gc_after_the_source_expires() {
    let mut s = server_with(|_| {}).await;
    s.deploy_guest("p11-rerun", None).await;
    s.deploy_guest("p11-rerun-v2", None).await;
    activate(&s, "pipeline", 1).await;
    let pad = "x".repeat(300_000);
    let (src, rec) = run_pipeline(
        &s,
        json!({"fail_at": 3, "pad": pad}),
        json!({"retention": "3s"}),
    )
    .await;
    assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    let input = {
        let h = s.handle_of(&src);
        query::run_payload(&h.reader(), &src, query::PAYLOAD_INPUT)
            .expect("read")
            .expect("input")
    };
    let Some(payload_ref::Kind::Blob(b)) = input.kind.clone() else {
        panic!("a 300 KB input is a blob: {input:?}");
    };
    let blob = ostro_blob::BlobRef {
        sha256: b.sha256.as_slice().try_into().expect("sha"),
        len: b.len,
    };
    let (st, v) = s
        .rerun(
            &src.to_string(),
            &json!({"from": from_step("s3", 1), "version": {"version": 2}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let run = new_run(&v);
    assert_eq!(terminal(&s, &run).await.status, RunStatus::Completed);
    let rinput = {
        let h = s.handle_of(&run);
        query::run_payload(&h.reader(), &run, query::PAYLOAD_INPUT)
            .expect("read")
            .expect("input")
    };
    assert_eq!(rinput, input, "the rerun shares the source's input ref");

    // The source expires (its retention is 3 s; the rerun has the default retention).
    let h = s.handle_of(&src);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while h.read_run(&src).expect("read").is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "source never expired"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // One GC pass (grace 0): the orphan goes, the shared input stays.
    let blobs = Arc::clone(h.blobs());
    let orphan = blobs
        .put(b"orphan blob for the rerun gc test")
        .expect("put");
    // The pass's cutoff is `now_ms` truncated to whole milliseconds while the blob's
    // mtime is finer: let the mtime fall strictly before the cutoff.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let gc = BlobGc::new(
        Arc::clone(&blobs),
        s.data_dir().to_path_buf(),
        Duration::ZERO,
        Arc::clone(s.h().clock()),
        None,
    )
    .expect("gc");
    let report = gc
        .pass(s.h().node(), s.h().meta().view())
        .await
        .expect("gc pass");
    assert!(
        !blobs.contains(&orphan),
        "the pass collects unreferenced blobs: {report:?}"
    );
    assert!(
        blobs.contains(&blob),
        "the shared input blob survives: {report:?}"
    );
    let bytes = s
        .handle_of(&run)
        .fetch_payload(&rinput)
        .await
        .expect("input");
    assert_eq!(bytes.len() as u64, blob.len);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// DLQ requeue mode rerun (11-12 handoff)
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dlq_requeue_rerun_runs_the_fixed_version() {
    let mut s = server_with(|_| {}).await;
    s.deploy_guest("p11-rerun", None).await;
    let (src, rec) = run_pipeline(&s, json!({"fail_at": 3}), json!({})).await;
    assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    s.deploy_guest("p11-rerun-v2", None).await;
    activate(&s, "pipeline", 2).await;

    let body = json!({"mode": {"rerun": {"from": {"step": {"name": "s3"}}}}});
    let (st, v) = s.post_json(&format!("/v1/dlq/{src}/requeue"), &body).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "rerun");
    let run = new_run(&v);
    let nrec = terminal(&s, &run).await;
    assert_eq!(nrec.status, RunStatus::Completed, "{nrec:?}");
    assert_eq!(result_json(s.result_bytes(&run).await), json!(5));
    assert_eq!(nrec.retry_of, Some(src));
    assert_eq!(
        String::from_utf8_lossy(&nrec.idempotency_key),
        format!("__rerun/{src}/__dlq/{src}/1")
    );

    // Idempotent re-issue; the entry is marked requeued and leaves the view.
    let (st, v2) = s.post_json(&format!("/v1/dlq/{src}/requeue"), &body).await;
    assert_eq!(st, StatusCode::OK, "{v2}");
    assert_eq!(v2["new_run_id"], v["new_run_id"]);
    let (st, page) = s.dlq_list("include_dismissed=true").await;
    assert_eq!(st, StatusCode::OK, "{page}");
    let e = page["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|e| e["run_id"] == src.to_string())
        .expect("listed with include_dismissed");
    assert_eq!(e["requeued_to"], run.to_string());
    let (_, page) = s.dlq_list("").await;
    assert!(
        !page["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|e| e["run_id"] == src.to_string()),
        "requeued drops out: {page}"
    );
    s.shutdown().await;
}
