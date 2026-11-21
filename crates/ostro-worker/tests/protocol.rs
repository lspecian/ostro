//! Worker protocol tests against the in-process fake Execute server (plan 07-07):
//! join + CA-hash pinning, Hello/Welcome, credits, heartbeats, version mismatch, re-join,
//! self-fence on stream loss, drain, renewal, journal paging, append framing, effects.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use ostro_types::journal::{EventBody, EventKind};
use ostro_worker::join::{self, JoinError};
use ostro_worker::{WorkerError, WorkerEvent};
use ostro_worker_proto::{PROTO_VERSION, WireOutcome, v1};
use serde_json::json;

const T: Duration = Duration::from_secs(60);

fn is_connected(e: &WorkerEvent) -> bool {
    matches!(e, WorkerEvent::Connected { .. })
}

/// Seq of the run's `TimerStarted` (its op id), once appended.
fn timer_op(st: &FakeState, id: &[u8]) -> Option<u64> {
    st.journal(id)
        .iter()
        .find(|e| matches!(e.body, EventBody::TimerStarted(_)))
        .map(|e| e.seq)
}

// ------------------------------------------------------------------------------------ join

#[tokio::test]
async fn join_https_with_pinned_ca() {
    let h = Harness::new(FakeConfig::default()).await;
    let id = join::join(&h.cfg(), "w1").await.expect("join");
    assert_eq!(id.endpoints, vec![h.fake.endpoint()]);
    assert_eq!(id.cluster_id, "c1");
    assert!(id.joined);
    let reqs = h.join.requests.lock().unwrap().clone();
    assert_eq!(reqs.len(), 1);
    let body: serde_json::Value = serde_json::from_str(&reqs[0]).unwrap();
    assert_eq!(body["worker_id"], "w1");
    assert_eq!(body["labels"]["zone"], "a");
    assert!(
        body["csr_pem"]
            .as_str()
            .unwrap()
            .contains("CERTIFICATE REQUEST")
    );
    assert!(!format!("{id:?}").contains("PRIVATE KEY"));
}

#[tokio::test]
async fn join_wrong_hash_fails_before_any_request() {
    let h = Harness::new(FakeConfig::default()).await;
    let mut cfg = h.cfg();
    cfg.ca_hash = Some(join::ca_hash_of(b"some other CA"));
    let err = join::join(&cfg, "w1").await.expect_err("pin mismatch");
    let msg = err.to_string();
    assert!(matches!(err, JoinError::Http(_)), "{err:?}");
    assert!(msg.contains("--ca-hash"), "{msg}");
    assert!(!msg.contains("s3cr3t"), "token leaked: {msg}");
    // The handshake failed: not one request byte (token included) reached the server.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(h.join.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn join_response_ca_must_match_pin() {
    let h = Harness::new(FakeConfig::default()).await;
    let other = TestCa::new();
    *h.join.ca_override.lock().unwrap() = Some(other.ca_pem.clone());
    let err = join::join(&h.cfg(), "w1").await.expect_err("CA mismatch");
    assert_eq!(err, JoinError::CaHashMismatch);
}

#[tokio::test]
async fn join_plain_http_only_for_loopback() {
    let ca = Arc::new(TestCa::new());
    let plain = FakeJoin::start(Arc::clone(&ca), false, vec!["127.0.0.1:1".into()]);
    let dir = tempfile::tempdir().unwrap();
    let cfg = worker_cfg(&plain, &ca, dir.path());
    assert!(
        cfg.join
            .as_deref()
            .unwrap()
            .starts_with("http://127.0.0.1:")
    );
    let id = join::join(&cfg, "w1").await.expect("loopback http join");
    assert_eq!(id.endpoints, vec!["127.0.0.1:1".to_string()]);

    let mut remote = cfg.clone();
    remote.join = Some("http://example.invalid:7700".into());
    let err = join::join(&remote, "w1")
        .await
        .expect_err("non-loopback http");
    assert!(matches!(err, JoinError::InsecureHttp(_)), "{err:?}");
}

// ------------------------------------------------------------------------- session basics

#[tokio::test]
async fn hello_credits_and_heartbeat_leases() {
    let h = Harness::new(FakeConfig::default()).await;
    let id = run_id(1);
    let sha = h.seed(
        "long-journal",
        "long-journal",
        &id,
        json!({"steps": 1, "sleep_ms": 50000}),
    );
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;

    let hello = h.st().hellos.lock().unwrap()[0].clone();
    assert_eq!(hello.worker_id, "w1");
    assert_eq!(hello.proto_version, PROTO_VERSION);
    assert_eq!(hello.ostro_version, ostro_worker::OSTRO_VERSION);
    assert_eq!(hello.wasmtime_version, ostro_worker::WASMTIME_VERSION);
    assert_eq!(hello.event_codec_ver, 1);
    assert_eq!(hello.wit_packages, vec!["ostro:runtime@0.1.0".to_string()]);
    assert_eq!(hello.labels.get("zone").map(String::as_str), Some("a"));
    assert_eq!(hello.slots, 2);
    assert_eq!(hello.memory_bytes, 512 * 1024 * 1024);

    let credits = |st: &FakeState| {
        st.find(|_, m| match m {
            v1::worker_msg::Msg::Credit(c) => Some(c.free_slots),
            _ => None,
        })
    };
    h.st()
        .wait_for(T, "credit 2", |st| credits(st).contains(&2))
        .await;

    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    // The activation parks on its durable sleep (hot wait 60 s): one slot used.
    h.st()
        .wait_for(T, "TimerStarted", |st| timer_op(st, &id).is_some())
        .await;
    h.st()
        .wait_for(T, "credit 1", |st| credits(st).last() == Some(&1))
        .await;
    h.st()
        .wait_for(T, "heartbeat with the lease", |st| {
            st.find(|_, m| match m {
                v1::worker_msg::Msg::Heartbeat(hb)
                    if hb.free_slots == 1
                        && hb.leases.iter().any(|l| {
                            l.run.as_ref().is_some_and(|r| r.run_id == id)
                                && l.token == Some(token(1))
                        }) =>
                {
                    Some(())
                }
                _ => None,
            })
            .len()
                >= 2
        })
        .await;
    // Fire the timer through the journal + Input: the run completes, the slot frees.
    let op = timer_op(h.st(), &id).unwrap();
    let end = h.st().journal_len(&id);
    h.st().push_events(&id, &[timer_fired(end, op)]);
    assert!(h.st().send(input(&id, 1, end + 1)));
    h.st()
        .wait_for(T, "Done", |st| !st.dones(&id).is_empty())
        .await;
    let done = h.st().dones(&id)[0].clone();
    assert!(
        matches!(outcome(&done), WireOutcome::Completed { .. }),
        "{:?}",
        outcome(&done)
    );
    assert_eq!(done.token, Some(token(1)));
    h.st()
        .wait_for(T, "credit back to 2", |st| credits(st).last() == Some(&2))
        .await;
    w.stop().await.unwrap();
}

#[tokio::test]
async fn version_mismatch_is_fatal_and_not_retried() {
    let h = Harness::new(FakeConfig {
        go_away: Some("version mismatch: server ostro 9.9.9".into()),
        ..FakeConfig::default()
    })
    .await;
    let w = spawn_worker(h.cfg());
    let res = tokio::time::timeout(T, w.handle).await.unwrap().unwrap();
    assert!(
        matches!(res, Err(WorkerError::VersionMismatch(_))),
        "{res:?}"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.st().hellos.lock().unwrap().len(), 1, "no reconnect loop");
}

#[tokio::test]
async fn rejoins_after_every_endpoint_failed() {
    let h = Harness::new(FakeConfig::default()).await;
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;
    // The cluster port changes: the old endpoint refuses, the join endpoint knows the new one.
    let st2 = FakeState::new(Arc::clone(&h.ca), FakeConfig::default());
    let fake2 = FakeServer::start(st2).await;
    *h.join.endpoints.lock().unwrap() = vec![fake2.endpoint()];
    let Harness {
        fake,
        join,
        dir: _dir,
        ca: _ca,
    } = h;
    fake.stop().await;
    w.wait_event(T, "rejoined", |e| *e == WorkerEvent::Rejoined)
        .await;
    let ep2 = fake2.endpoint();
    w.wait_event(
        T,
        "connected to the new port",
        |e| matches!(e, WorkerEvent::Connected { endpoint, .. } if *endpoint == ep2),
    )
    .await;
    assert_eq!(join.requests.lock().unwrap().len(), 2, "joined twice");
    assert_eq!(fake2.st.hellos.lock().unwrap().len(), 1);
    w.stop().await.unwrap();
}

#[tokio::test]
async fn stream_loss_self_fences_and_reconnects() {
    let h = Harness::new(FakeConfig::default()).await;
    let id = run_id(2);
    let sha = h.seed(
        "long-journal",
        "long-journal",
        &id,
        json!({"steps": 1, "sleep_ms": 50000}),
    );
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    h.st()
        .wait_for(T, "TimerStarted", |st| timer_op(st, &id).is_some())
        .await;
    assert_eq!(w.worker.active_activations(), 1);
    let t0 = Instant::now();
    h.st().kill_current();
    let ev = w
        .wait_event(Duration::from_secs(5), "self-fence", |e| {
            matches!(e, WorkerEvent::SelfFenced { .. })
        })
        .await;
    assert_eq!(ev, WorkerEvent::SelfFenced { cancelled: 1 });
    w.wait_event(Duration::from_secs(5), "activation ended", |e| {
        matches!(e, WorkerEvent::ActivationEnded { .. })
    })
    .await;
    let took = t0.elapsed();
    assert!(
        took < Duration::from_millis(200),
        "cancelled after {took:?}"
    );
    assert_eq!(w.worker.active_activations(), 0);
    w.wait_event(T, "reconnected", is_connected).await;
    assert_eq!(h.st().hellos.lock().unwrap().len(), 2);
    w.stop().await.unwrap();
}

#[tokio::test]
async fn drain_evicts_and_exits_cleanly() {
    let h = Harness::new(FakeConfig::default()).await;
    let id = run_id(3);
    let sha = h.seed(
        "long-journal",
        "long-journal",
        &id,
        json!({"steps": 1, "sleep_ms": 50000}),
    );
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    h.st()
        .wait_for(T, "TimerStarted", |st| timer_op(st, &id).is_some())
        .await;
    let t0 = Instant::now();
    w.stop().await.expect("clean exit");
    assert!(t0.elapsed() < Duration::from_secs(10));
    let st = h.st();
    assert!(
        !st.find(|_, m| matches!(m, v1::worker_msg::Msg::Drain(_)).then_some(()))
            .is_empty()
    );
    assert!(
        !st.find(|_, m| match m {
            v1::worker_msg::Msg::Credit(c) if c.free_slots == 0 => Some(()),
            _ => None,
        })
        .is_empty()
    );
    let dones = st.dones(&id);
    assert_eq!(dones.len(), 1);
    assert!(
        matches!(outcome(&dones[0]), WireOutcome::Evicted { .. }),
        "{:?}",
        outcome(&dones[0])
    );
}

#[tokio::test]
async fn renewal_swaps_channel_without_dropping_the_lease() {
    let h = Harness::new(FakeConfig {
        renew_lifetime: Some(Duration::from_secs(10)),
        renew_after_ms: 3_600_000,
        ..FakeConfig::default()
    })
    .await;
    h.join
        .renew_after_ms
        .store(1_500, std::sync::atomic::Ordering::Relaxed);
    h.join
        .cert_lifetime_s
        .store(10, std::sync::atomic::Ordering::Relaxed);
    let id = run_id(4);
    let sha = h.seed(
        "long-journal",
        "long-journal",
        &id,
        json!({"steps": 1, "sleep_ms": 50000}),
    );
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;
    let s1 = h.st().current_session_id().unwrap();
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    h.st()
        .wait_for(T, "TimerStarted", |st| timer_op(st, &id).is_some())
        .await;
    w.wait_event(T, "renewed", |e| *e == WorkerEvent::Renewed)
        .await;
    assert_eq!(
        h.st().renewals.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(w.worker.active_activations(), 1, "the activation survived");
    let s2 = h.st().current_session_id().unwrap();
    assert_ne!(s1, s2);
    h.st()
        .wait_for(T, "Drain on the old session", |st| {
            !st.find(|sid, m| {
                (sid == s1.as_slice() && matches!(m, v1::worker_msg::Msg::Drain(_))).then_some(())
            })
            .is_empty()
        })
        .await;
    h.st()
        .wait_for(T, "lease heartbeat on the new session", |st| {
            !st.find(|sid, m| match m {
                v1::worker_msg::Msg::Heartbeat(hb)
                    if sid == s2.as_slice()
                        && hb
                            .leases
                            .iter()
                            .any(|l| l.run.as_ref().is_some_and(|r| r.run_id == id)) =>
                {
                    Some(())
                }
                _ => None,
            })
            .is_empty()
        })
        .await;
    // The run continues on the new session: fire its timer, Done arrives there.
    let op = timer_op(h.st(), &id).unwrap();
    let end = h.st().journal_len(&id);
    h.st().push_events(&id, &[timer_fired(end, op)]);
    assert!(h.st().send(input(&id, 1, end + 1)));
    h.st()
        .wait_for(T, "Done", |st| !st.dones(&id).is_empty())
        .await;
    let on_new = h.st().find(|sid, m| match m {
        v1::worker_msg::Msg::Done(d) if sid == s2.as_slice() => Some(d.clone()),
        _ => None,
    });
    assert_eq!(on_new.len(), 1);
    assert!(matches!(outcome(&on_new[0]), WireOutcome::Completed { .. }));
    w.stop().await.unwrap();
}

// ------------------------------------------------------------ runner, paging, framing, effects

#[tokio::test]
async fn long_journal_replays_from_fetched_pages() {
    let mut cfg = FakeConfig::default();
    cfg.welcome.max_frame_bytes = 64 * 1024;
    let h = Harness::new(cfg).await;
    let id = run_id(5);
    let sha = h.seed(
        "long-journal",
        "long-journal",
        &id,
        json!({"steps": 2000, "sleep_ms": 50000}),
    );
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;
    // Attempt 1 runs 2,000 steps and parks on its timer; cancel it (lease revoked).
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    h.st()
        .wait_for(T, "TimerStarted", |st| timer_op(st, &id).is_some())
        .await;
    assert!(h.st().send(cancel(&id, 1)));
    h.st()
        .wait_for(T, "Done 1", |st| !st.dones(&id).is_empty())
        .await;
    let steps = h
        .st()
        .journal(&id)
        .iter()
        .filter(|e| matches!(e.body, EventBody::StepCompleted(_)))
        .count();
    assert_eq!(steps, 2000);
    // Attempt 2 replays the whole journal from pages, plus the fired timer.
    let op = timer_op(h.st(), &id).unwrap();
    let end = h.st().journal_len(&id);
    h.st().push_events(&id, &[timer_fired(end, op)]);
    h.st().page_bytes.lock().unwrap().clear();
    assert!(h.st().send(activate(&id, 2, &sha, end + 1)));
    h.st()
        .wait_for(T, "Done 2", |st| st.dones(&id).len() >= 2)
        .await;
    let done = h.st().dones(&id)[1].clone();
    match outcome(&done) {
        WireOutcome::Completed { output } => {
            let v: serde_json::Value = serde_json::from_slice(&output.data).unwrap();
            assert_eq!(v["steps"], 2000);
            assert_eq!(v["sum"], 1999 * 2000 / 2);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
    let pages = h.st().page_bytes.lock().unwrap().clone();
    assert!(pages.len() > 1, "paged: {pages:?}");
    assert!(pages.iter().all(|b| *b <= 64 * 1024), "{pages:?}");
    // No step re-executed on replay: still 2,000 StepCompleted.
    let steps = h
        .st()
        .journal(&id)
        .iter()
        .filter(|e| matches!(e.body, EventBody::StepCompleted(_)))
        .count();
    assert_eq!(steps, 2000);
    w.stop().await.unwrap();
}

#[tokio::test]
async fn large_event_is_framed_within_max_frame_bytes() {
    let mut cfg = FakeConfig::default();
    cfg.welcome.max_frame_bytes = 64 * 1024;
    let h = Harness::new(cfg).await;
    let id = run_id(6);
    let big = 3 * 1024 * 1024u64;
    let sha = h.seed(
        "long-journal",
        "long-journal",
        &id,
        json!({"steps": 0, "result_bytes": big}),
    );
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    h.st()
        .wait_for(T, "Done", |st| !st.dones(&id).is_empty())
        .await;
    let done = h.st().dones(&id)[0].clone();
    assert!(
        matches!(outcome(&done), WireOutcome::Completed { .. }),
        "{:?}",
        outcome(&done)
    );

    // The big StepCompleted went out as >= 48 frames, each within the frame size.
    let step_kind = EventKind::StepCompleted.code();
    let appended = h.st().appended.lock().unwrap().clone();
    let (_, seq, _, bytes) = appended
        .iter()
        .find(|(r, _, k, b)| *r == id && *k == step_kind && b.len() as u64 > big)
        .cloned()
        .expect("big event appended");
    let frames = h.st().frames.lock().unwrap().clone();
    let big_frames: Vec<_> = frames
        .iter()
        .filter(|f| f.run_id == id && f.kind == step_kind && f.chunk_len > 0)
        .collect();
    assert!(big_frames.len() >= 48, "{} frames", big_frames.len());
    let max = frames.iter().map(|f| f.chunk_len).max().unwrap();
    assert!(max <= 64 * 1024, "largest chunk {max}");
    // Reassembled byte-identical (the fake asserts encode(decode(bytes)) == bytes) and
    // decodable at its seq.
    let ev = ostro_types::journal::decode_event(seq, step_kind, &bytes).unwrap();
    assert!(matches!(ev.body, EventBody::StepCompleted(_)));

    // Replay serves that row as a BlobRef (> 256 KiB): fetched and verified.
    let len = h.st().journal_len(&id);
    let before = h
        .st()
        .blob_fetches
        .load(std::sync::atomic::Ordering::Relaxed);
    // Strip the terminal event so the run can be re-activated from its journal.
    {
        let mut j = h.st().journals.lock().unwrap();
        let rows = j.get_mut(&id).unwrap();
        rows.truncate((seq + 1) as usize);
    }
    assert!(len > seq + 1);
    assert!(h.st().send(activate(&id, 2, &sha, seq + 1)));
    h.st()
        .wait_for(T, "Done 2", |st| st.dones(&id).len() >= 2)
        .await;
    let d2 = h.st().dones(&id)[1].clone();
    match outcome(&d2) {
        WireOutcome::Completed { output } => {
            let v: serde_json::Value = serde_json::from_slice(&output.data).unwrap();
            assert_eq!(v["big_len"], big);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
    assert!(
        h.st()
            .blob_fetches
            .load(std::sync::atomic::Ordering::Relaxed)
            > before
    );
    w.stop().await.unwrap();
}

#[tokio::test]
async fn pay_flow_bridges_effect_without_secret_values() {
    let h = Harness::new(FakeConfig::default()).await;
    let id = run_id(7);
    let sha = h.seed(
        "pay-flow",
        "pay-flow",
        &id,
        json!({"url": "http://127.0.0.1:9"}),
    );
    let mut w = spawn_worker(h.cfg());
    w.wait_event(T, "connected", is_connected).await;
    assert!(h.st().send(activate(&id, 1, &sha, 1)));
    h.st()
        .wait_for(T, "StartEffect", |st| {
            !st.effects.lock().unwrap().is_empty()
        })
        .await;
    let req = h.st().effects.lock().unwrap()[0].clone();
    assert_eq!(req.run.as_ref().unwrap().run_id, id);
    assert_eq!(req.token, Some(token(1)));
    assert!(!req.redrive);
    let names: Vec<_> = req
        .redacted_values
        .iter()
        .map(|h| h.name.as_str())
        .collect();
    assert_eq!(names, vec!["Authorization"]);
    // The template travels as written: the worker never resolves a secret.
    assert_eq!(req.redacted_values[0].value, "Bearer {{secret:pay_token}}");
    // The op is the journaled EffectScheduled's seq, and the journal copy is redacted.
    let sched = h
        .st()
        .journal(&id)
        .into_iter()
        .find(|e| matches!(e.body, EventBody::EffectScheduled(_)))
        .expect("EffectScheduled journaled");
    assert_eq!(req.op, sched.seq);
    if let EventBody::EffectScheduled(s) = &sched.body {
        let ostro_types::journal::EffectRequest::Http(r) = &s.request else {
            panic!()
        };
        let auth = r
            .headers
            .iter()
            .find(|h| h.name == "Authorization")
            .unwrap();
        assert_eq!(auth.value, "[redacted]");
        assert!(
            r.headers
                .iter()
                .any(|h| h.name == "Idempotency-Key" && h.value.starts_with("ostro-"))
        );
    }
    assert_eq!(h.st().session_count(), 1);
    w.stop().await.unwrap();
}
