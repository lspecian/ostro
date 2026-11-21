//! Worker join tokens, the registry and the mTLS Execute session (plan 07-09; WORK-01,
//! SEC-01, D-W2/D-W4/D-W16/D-W25, W4/W6).
//!
//! - `join_service_*`: token lifecycle (create / list / revoke, 1 s TTL expiry on the
//!   injected clock), refused tokens and CSRs, renewal checks.
//! - `registry_*`: credit and pick, heartbeat lease renewal, abandonment, renewal hand-off.
//! - `session_*` / `fetch_authz_*` / `rpc_*`: the cluster listener end to end.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7_workers.rs"]
mod p7_workers;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_api::ErrorCode;
use ostro_api::phase7::{JoinRequest, REASON_JOIN_TOKEN_INVALID};
use ostro_partition::ExecOutcome;
use ostro_server::workers::registry::{JobRpc, PeerIdentity, RemoteEvent};
use ostro_server::workers::service::{self, WireDir};
use ostro_worker_proto::v1;
use p7_workers::{
    CLUSTER, Opened, OtherCa, Rig, RigOpts, T, TestWorker, connect, hello, new_key_and_csr,
    wait_for,
};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

fn join_req(token: &str, worker_id: &str) -> JoinRequest {
    JoinRequest {
        token: token.to_owned(),
        csr_pem: new_key_and_csr(worker_id).1,
        worker_id: worker_id.to_owned(),
        labels: BTreeMap::new(),
    }
}

fn is_invalid_token(e: &ostro_api::ApiError) -> bool {
    e.code == ErrorCode::InvalidArgument
        && e.details
            .as_ref()
            .and_then(|d| d.get("reason"))
            .and_then(|r| r.as_str())
            == Some(REASON_JOIN_TOKEN_INVALID)
}

fn lease(job: &p7_workers::CapturedJob) -> v1::Lease {
    v1::Lease {
        run: Some(job.wire_run()),
        token: Some(job.wire_token()),
    }
}

// ---------------------------------------------------------------------------------
// JoinService
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_service_token_lifecycle() {
    let rig = Rig::start(RigOpts::default()).await;
    let created = rig
        .join
        .create_token(Some(Duration::from_secs(1)), "https://h:7700")
        .await
        .expect("create");
    assert!(created.token_id.starts_with("wkr_"));
    assert_eq!(created.token_id.len(), 14, "wkr_ + 10 base32");
    let (id, secret) = created.token.split_once('.').expect("id.secret");
    assert_eq!(id, created.token_id);
    assert!(!secret.is_empty());
    assert_eq!(
        created.join_command,
        format!(
            "ostro worker --join https://h:7700 --token {} --ca-hash {}",
            created.token,
            rig.ca_hash()
        )
    );
    assert!(!format!("{created:?}").contains(secret), "Debug redacts");

    // Stored as the meta ApiToken: sha256 of the secret, worker:join, F8 expiry.
    let rec = rig
        .meta
        .view()
        .read(|s| s.token(id).map(|t| t.value.clone()))
        .expect("stored");
    assert_eq!(
        rec.hash,
        <[u8; 32]>::from(Sha256::digest(secret.as_bytes()))
    );
    assert_eq!(rec.scopes, vec!["worker:join".to_owned()]);
    let now = rig.clock_now();
    assert_eq!(rec.created_at_ms, now);
    assert_eq!(rec.expires_at_ms, Some(now + 1_000));
    assert_eq!(created.expires_at_ms, now + 1_000);

    // list shows the expiry, never the secret.
    let list = rig.join.list_tokens();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].token_id, created.token_id);
    assert_eq!(list[0].expires_at_ms, Some(now + 1_000));

    // Joins at once (CSR signed into a worker identity) ...
    let r = rig
        .join
        .join(join_req(&created.token, "w1"))
        .await
        .expect("join");
    assert_eq!(r.cluster_id, CLUSTER);
    assert_eq!(r.endpoints, vec![rig.addr.to_string()]);
    assert_eq!(r.ca_cert_pem, rig.ca_pem());
    assert_eq!(r.renew_after_ms, 24 * 3600 * 1000 / 3 * 2);
    let der = x509_parser::pem::parse_x509_pem(r.client_cert_pem.as_bytes())
        .expect("pem")
        .1
        .contents;
    let wid = ostro_server::tls::ca::WorkerIdentity::from_der(&der).expect("worker identity");
    assert_eq!(
        (
            wid.cluster_id.as_str(),
            wid.token_id.as_str(),
            wid.worker_id.as_str()
        ),
        (CLUSTER, created.token_id.as_str(), "w1")
    );

    // ... and is refused once the injected clock passes the expiry.
    rig.clock.advance(1_500);
    let e = rig
        .join
        .join(join_req(&created.token, "w1"))
        .await
        .expect_err("expired");
    assert!(is_invalid_token(&e), "{e:?}");
    assert!(!e.message.contains(secret));

    // Revoke: gone from the list; a second revoke is NotFound.
    rig.join.revoke_token(id).await.expect("revoke");
    assert!(rig.join.list_tokens().is_empty());
    let e = rig.join.revoke_token(id).await.expect_err("again");
    assert_eq!(e.code, ErrorCode::NotFound);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_service_refuses_bad_tokens_and_csrs() {
    let rig = Rig::start(RigOpts::default()).await;
    let t = rig.token(Duration::from_secs(60)).await;
    let (id, secret) = t.token.split_once('.').unwrap();
    for bad in [
        format!("{id}.wrong-secret"),
        format!("wkr_nonexistent.{secret}"),
        "garbage".to_owned(),
        format!("{id}."),
    ] {
        let e = rig.join.join(join_req(&bad, "w1")).await.expect_err(&bad);
        assert!(
            is_invalid_token(&e) || e.code == ErrorCode::InvalidArgument,
            "{e:?}"
        );
        assert!(!e.message.contains(secret));
    }
    // A token without the worker:join scope is not a join token.
    rig.meta
        .propose(ostro_meta::MetaCommand::Put {
            key: ostro_meta::MetaKey::Token("wkr_noscope".into()),
            expected_version: None,
            record: ostro_meta::MetaRecord::Token(ostro_meta::ApiToken {
                id: "wkr_noscope".into(),
                hash: Sha256::digest(b"s").into(),
                scopes: vec!["admin".into()],
                ..Default::default()
            }),
        })
        .await
        .expect("put");
    let e = rig
        .join
        .join(join_req("wkr_noscope.s", "w1"))
        .await
        .expect_err("scope");
    assert!(is_invalid_token(&e), "{e:?}");
    assert!(
        rig.join
            .list_tokens()
            .iter()
            .all(|t| t.token_id != "wkr_noscope")
    );

    // A bad CSR is InvalidArgument naming the CSR (not the token).
    let mut req = join_req(&t.token, "w1");
    req.csr_pem =
        "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----\n".into();
    let e = rig.join.join(req).await.expect_err("csr");
    assert_eq!(e.code, ErrorCode::InvalidArgument);
    assert!(e.message.contains("csr"), "{e:?}");
    // A worker id the certificate cannot carry.
    let e = rig
        .join
        .join(join_req(&t.token, "w:1"))
        .await
        .expect_err("worker id");
    assert_eq!(e.code, ErrorCode::InvalidArgument);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_service_renew_checks_the_token() {
    let rig = Rig::start(RigOpts::default()).await;
    let t = rig.token(Duration::from_secs(10)).await;
    let (_, csr) = new_key_and_csr("w1");
    let r = rig
        .join
        .renew(&t.token_id, "w1", csr.as_bytes())
        .await
        .expect("renew");
    assert!(r.client_cert_pem.contains("BEGIN CERTIFICATE"));
    rig.clock.advance(11_000);
    let e = rig
        .join
        .renew(&t.token_id, "w1", csr.as_bytes())
        .await
        .expect_err("expired token refuses renewal");
    assert!(is_invalid_token(&e), "{e:?}");
    let e = rig
        .join
        .renew("wkr_missing", "w1", csr.as_bytes())
        .await
        .expect_err("unknown");
    assert!(is_invalid_token(&e));
    rig.stop().await;
}

// ---------------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------------

fn peer(w: &str) -> PeerIdentity {
    PeerIdentity {
        cluster_id: CLUSTER.into(),
        token_id: Some("wkr_t".into()),
        worker_id: w.into(),
    }
}

type ServerRx = mpsc::UnboundedReceiver<Result<v1::ServerMsg, tonic::Status>>;

fn open(rig: &Rig, w: &str, slots: u32) -> (Vec<u8>, ServerRx) {
    let (tx, rx) = mpsc::unbounded_channel();
    let ctl = rig
        .registry
        .open_session(peer(w), &hello(w, slots), 1, tx)
        .expect("open");
    (ctl.id, rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_pick_credits_and_guards() {
    let rig = Rig::start(RigOpts::default()).await;
    let r = &rig.registry;
    let mut changed = r.changed();
    let (a, _ra) = open(&rig, "a", 4);
    let (b, _rb) = open(&rig, "b", 4);
    assert!(changed.has_changed().unwrap());
    changed.borrow_and_update();
    // No credit reported yet: nothing to pick.
    assert!(r.pick(|_| true).is_none());
    r.credit(&a, 3);
    r.credit(&b, 1);
    assert!(changed.has_changed().unwrap());
    assert_eq!(r.slots_free(), 4);
    // Most free credit first.
    let g1 = r.pick(|_| true).expect("pick");
    assert_eq!(g1.worker_id(), "a");
    let g2 = r.pick(|_| true).expect("pick");
    assert_eq!(g2.worker_id(), "a");
    // a: 1 left, b: 1 -> tie: least recently assigned (b).
    let g3 = r.pick(|_| true).expect("pick");
    assert_eq!(g3.worker_id(), "b");
    let g4 = r.pick(|_| true).expect("pick");
    assert_eq!(g4.worker_id(), "a");
    assert!(r.pick(|_| true).is_none(), "credit exhausted");
    // Dropping a guard returns its credit.
    drop(g3);
    assert_eq!(r.pick(|_| true).expect("returned").worker_id(), "b");
    // Predicates filter on labels / ids.
    drop(g1);
    assert!(
        r.pick(|v| v.labels.get("zone").map(String::as_str) == Some("b"))
            .is_none()
    );
    assert_eq!(
        r.pick(|v| v.worker_id == "a" && v.labels["zone"] == "a")
            .expect("label")
            .worker_id(),
        "a"
    );
    // Credit is absolute; the slots cap holds.
    r.credit(&b, 99);
    assert_eq!(
        r.sessions()
            .iter()
            .find(|s| s.worker_id == "b")
            .unwrap()
            .free,
        4
    );
    // Drain stops assigning.
    assert_eq!(r.drain("b"), Some(false));
    assert_eq!(r.drain("b"), Some(true));
    assert_eq!(r.drain("nobody"), None);
    assert!(r.pick(|v| v.worker_id == "b").is_none());
    drop((g2, g4));
    r.close_session(&a, "test");
    r.close_session(&b, "test");
    assert_eq!(r.connected(), 0);
    assert_eq!(r.slots_free(), 0);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_heartbeat_renews_and_reports_unknown_leases() {
    let rig = Rig::start(RigOpts {
        node: true,
        lease_ttl: Duration::from_secs(1),
        ..RigOpts::default()
    })
    .await;
    let module = rig.put_blob(b"not really wasm");
    rig.start_run(&module, b"{}").await;
    let job = rig.next_job().await;
    let (s, mut rx) = open(&rig, "w", 2);
    rig.registry.credit(&s, 2);
    let h = rig.attach(&job, "w");
    assert_eq!(h.session_id().as_deref(), Some(s.as_slice()));
    let info = rig.registry.sessions();
    assert_eq!((info[0].leases, info[0].free), (1, 1));

    // Heartbeats keep the 1 s lease alive for 3 s.
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        let renewed = rig.registry.heartbeat(
            &s,
            &v1::Heartbeat {
                leases: vec![lease(&job)],
                free_slots: 1,
                memory_used: 0,
            },
        );
        assert_eq!(renewed, 1);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(!job.revoked(), "lease kept alive by heartbeats");

    // An unknown lease is answered with LeaseLost.
    let mut other = lease(&job);
    other.token = Some(v1::Token { epoch: 9, n: 9 });
    rig.registry.heartbeat(
        &s,
        &v1::Heartbeat {
            leases: vec![other],
            free_slots: 1,
            memory_used: 0,
        },
    );
    let m = rx.recv().await.expect("msg").expect("ok");
    assert!(
        matches!(m.msg, Some(v1::server_msg::Msg::LeaseLost(_))),
        "{m:?}"
    );

    // Without heartbeats the lease expires.
    wait_for(Duration::from_secs(5), "lease expiry", || job.revoked()).await;
    drop(h);
    assert_eq!(rig.registry.sessions()[0].leases, 0);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_abandons_jobs_when_a_session_closes() {
    let rig = Rig::start(RigOpts {
        node: true,
        ..RigOpts::default()
    })
    .await;
    let module = rig.put_blob(b"m");
    rig.start_run(&module, b"{}").await;
    let job = rig.next_job().await;
    let (s, _rx) = open(&rig, "w", 2);
    rig.registry.credit(&s, 2);
    let mut h = rig.attach(&job, "w");
    // Done / GuestLogs reach the job; a Done for another token does not.
    assert!(rig.registry.job_event(
        &s,
        Some(&job.wire_run()),
        Some(&job.wire_token()),
        RemoteEvent::GuestLogs(v1::GuestLogs::default()),
    ));
    assert!(matches!(h.try_event(), Some(RemoteEvent::GuestLogs(_))));
    assert!(!rig.registry.job_event(
        &s,
        Some(&job.wire_run()),
        Some(&v1::Token { epoch: 0, n: 77 }),
        RemoteEvent::GuestLogs(v1::GuestLogs::default()),
    ));
    let t = Instant::now();
    rig.registry.close_session(&s, "stream lost");
    match h.next_event().await {
        Some(RemoteEvent::Abandoned { reason }) => assert_eq!(reason, "stream lost"),
        other => panic!("expected Abandoned, got {other:?}"),
    }
    assert!(t.elapsed() < Duration::from_millis(100));
    assert!(h.session_id().is_none());
    assert!(!h.send(v1::server_msg::Msg::Cancel(v1::Cancel::default())));
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_renewal_drain_hands_jobs_to_the_new_session() {
    let rig = Rig::start(RigOpts {
        node: true,
        ..RigOpts::default()
    })
    .await;
    let module = rig.put_blob(b"m");
    rig.start_run(&module, b"{}").await;
    let job = rig.next_job().await;
    let (old, _r1) = open(&rig, "w", 2);
    rig.registry.credit(&old, 2);
    let mut h = rig.attach(&job, "w");
    // Renewal: a new session of the same worker lists the lease in its first heartbeat.
    let (new, mut r2) = open(&rig, "w", 2);
    let renewed = rig.registry.heartbeat(
        &new,
        &v1::Heartbeat {
            leases: vec![lease(&job)],
            free_slots: 1,
            memory_used: 0,
        },
    );
    assert_eq!(renewed, 1);
    assert_eq!(h.session_id().as_deref(), Some(new.as_slice()));
    assert!(r2.try_recv().is_err(), "no LeaseLost on the new session");
    // Drain + close of the old session revokes nothing.
    rig.registry.worker_drain(&old);
    rig.registry.close_session(&old, "session stream ended");
    assert!(h.try_event().is_none());
    assert!(!job.revoked());
    // Even when the old session closes first (after its Drain), the job moves.
    let (newer, _r3) = open(&rig, "w", 2);
    rig.registry.worker_drain(&new);
    rig.registry.close_session(&new, "session stream ended");
    assert!(h.try_event().is_none());
    assert_eq!(h.session_id().as_deref(), Some(newer.as_slice()));
    // A plain stream loss (no Drain) abandons.
    rig.registry.close_session(&newer, "lost");
    assert!(matches!(
        h.next_event().await,
        Some(RemoteEvent::Abandoned { .. })
    ));
    rig.stop().await;
}

#[test]
fn registry_wasmtime_version_matches_the_worker() {
    // One shared value: the server compares Hello.wasmtime_version with this constant.
    assert_eq!(service::WASMTIME_VERSION, ostro_worker::WASMTIME_VERSION);
    assert_eq!(service::OSTRO_VERSION, ostro_worker::OSTRO_VERSION);
    assert_eq!(service::MAX_MESSAGE_BYTES, ostro_worker::MAX_MESSAGE_BYTES);
}

// ---------------------------------------------------------------------------------
// Cluster listener
// ---------------------------------------------------------------------------------

/// Plan 07-09: the in-process worker joins with token + CA hash, opens an mTLS session,
/// its credit reflects its slots, and killing it abandons its job immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_worker_joins_and_opens_mtls_session() {
    let rig = Rig::start(RigOpts {
        node: true,
        ..RigOpts::default()
    })
    .await;
    let tok = rig.token(Duration::from_secs(600)).await;
    let mut w = rig
        .spawn_worker(&tok.token, "w1", 3, &[("gpu", "no"), ("zone", "b")])
        .await;
    w.wait_event(T, |e| {
        matches!(e, ostro_worker::WorkerEvent::Connected { .. })
    })
    .await;
    rig.wait(T, "worker credit", || rig.registry.slots_free() == 3)
        .await;
    let s = rig.registry.sessions();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].worker_id, "w1");
    assert_eq!(s[0].token_id.as_deref(), Some(tok.token_id.as_str()));
    assert_eq!(s[0].slots, 3);
    assert_eq!(s[0].labels.get("gpu").map(String::as_str), Some("no"));
    assert_eq!(s[0].labels.get("zone").map(String::as_str), Some("b"));
    assert_eq!(rig.registry.connected(), 1);

    let module = rig.put_blob(b"m");
    rig.start_run(&module, b"{}").await;
    let job = rig.next_job().await;
    let mut h = rig.attach(&job, "w1");
    rig.wait(T, "credit after attach", || rig.registry.slots_free() == 2)
        .await;
    let t = Instant::now();
    w.kill();
    match tokio::time::timeout(T, h.next_event())
        .await
        .expect("event")
    {
        Some(RemoteEvent::Abandoned { reason }) => {
            println!("abandoned after {:?}: {reason}", t.elapsed())
        }
        other => panic!("expected Abandoned, got {other:?}"),
    }
    assert!(t.elapsed() < Duration::from_secs(5));
    rig.wait(T, "session removed", || rig.registry.connected() == 0)
        .await;
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_refuses_missing_and_foreign_client_certs() {
    let rig = Rig::start(RigOpts::default()).await;
    // No client certificate: the handshake or the first RPC fails.
    let r = match connect(rig.addr, &rig.ca_pem(), None).await {
        Err(e) => Err(e),
        Ok(mut c) => c
            .fetch_blob(v1::FetchBlobRequest::default())
            .await
            .map(|_| ())
            .map_err(|s| s.to_string()),
    };
    assert!(r.is_err(), "no client cert must be refused");
    // A certificate from another CA (valid-looking identity).
    let other = OtherCa::new("not the cluster CA");
    let id = other.client(
        &format!("ostro://{CLUSTER}/worker/wkr_x/w1"),
        &rig.ca_pem(),
        true,
    );
    match TestWorker::try_open(rig.addr, &id, hello("w1", 1)).await {
        Err(_) | Ok((None, Opened::Refused(_))) => {}
        Ok((_, o)) => panic!("foreign CA accepted: {o:?}"),
    }
    // A node certificate (cluster CA, node identity) is not a worker.
    let node = rig.tls.node_cert().expect("node cert");
    let node_id = p7_workers::TestIdentity {
        cert_pem: node.chain_pem(),
        key_pem: node.key_pem(),
        ca_pem: rig.ca_pem(),
    };
    match TestWorker::try_open(rig.addr, &node_id, hello("1", 1)).await {
        Ok((None, Opened::Refused(s))) => {
            assert_eq!(s.code(), tonic::Code::PermissionDenied, "{s:?}")
        }
        other => panic!("node cert: {:?}", other.map(|(_, o)| o)),
    }
    assert_eq!(rig.registry.connected(), 0);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_revoked_token_closes_and_rejoin_refused() {
    let rig = Rig::start(RigOpts::default()).await;
    let tok = rig.token(Duration::from_secs(600)).await;
    let id = rig.identity(&tok.token, "w1").await;
    let mut w = TestWorker::open(rig.addr, &id, hello("w1", 1))
        .await
        .expect("session");
    assert_eq!(rig.registry.connected(), 1);
    let t = Instant::now();
    rig.join.revoke_token(&tok.token_id).await.expect("revoke");
    match w.next(Duration::from_secs(1)).await {
        Some(Ok(v1::server_msg::Msg::GoAway(g))) => assert_eq!(g.reason, "token revoked"),
        other => panic!("expected GoAway within 1 s, got {other:?}"),
    }
    assert!(t.elapsed() < Duration::from_secs(1));
    rig.wait(Duration::from_secs(1), "session closed", || {
        rig.registry.connected() == 0
    })
    .await;
    // Reconnecting with the revoked identity and re-joining are refused.
    match TestWorker::try_open(rig.addr, &id, hello("w1", 1)).await {
        Ok((None, Opened::Refused(s))) => {
            assert_eq!(s.code(), tonic::Code::Unauthenticated, "{s:?}")
        }
        other => panic!("revoked identity: {:?}", other.map(|(_, o)| o)),
    }
    let e = rig
        .join
        .join(join_req(&tok.token, "w1"))
        .await
        .expect_err("re-join");
    assert!(is_invalid_token(&e));
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_version_mismatch_goaway() {
    let rig = Rig::start(RigOpts::default()).await;
    let tok = rig.token(Duration::from_secs(600)).await;
    let id = rig.identity(&tok.token, "w1").await;
    for patch in [
        |h: &mut v1::Hello| h.wasmtime_version = "0.0.1".into(),
        |h: &mut v1::Hello| h.ostro_version = "0.0.0-other".into(),
        |h: &mut v1::Hello| h.event_codec_ver += 1,
        |h: &mut v1::Hello| h.proto_version += 1,
    ] {
        let mut h = hello("w1", 1);
        patch(&mut h);
        match TestWorker::try_open(rig.addr, &id, h)
            .await
            .expect("connect")
        {
            (None, Opened::GoAway(reason)) => {
                assert!(reason.contains("version"), "{reason}")
            }
            (_, o) => panic!("expected GoAway, got {o:?}"),
        }
    }
    // Hello.worker_id must match the certificate.
    match TestWorker::try_open(rig.addr, &id, hello("w2", 1))
        .await
        .expect("connect")
    {
        (None, Opened::Refused(s)) => assert_eq!(s.code(), tonic::Code::PermissionDenied),
        (_, o) => panic!("{o:?}"),
    }
    assert_eq!(rig.registry.connected(), 0);
    rig.stop().await;

    // require_same_version = false: only proto / codec versions are enforced.
    let rig = Rig::start(RigOpts {
        require_same_version: false,
        ..RigOpts::default()
    })
    .await;
    let tok = rig.token(Duration::from_secs(600)).await;
    let id = rig.identity(&tok.token, "w1").await;
    let mut h = hello("w1", 1);
    h.wasmtime_version = "0.0.1".into();
    TestWorker::open(rig.addr, &id, h).await.expect("accepted");
    rig.stop().await;
}

/// W4: `Welcome.heartbeat_ms = min(heartbeat, lease_ttl / 3)`; the worker's heartbeats
/// keep an attached dispatcher lease (ttl 1 s) alive for 3 x ttl.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_heartbeat_keeps_lease_alive() {
    let rig = Rig::start(RigOpts {
        node: true,
        lease_ttl: Duration::from_secs(1),
        heartbeat: Duration::from_secs(10),
        ..RigOpts::default()
    })
    .await;
    let mut w = rig.worker("w1", 2).await;
    assert_eq!(w.welcome.heartbeat_ms, 333);
    assert_eq!(w.welcome.lease_ttl_ms, 1_000);
    assert_eq!(w.welcome.cluster_id, CLUSTER);
    assert_eq!(w.welcome.node_id, 1);
    assert_eq!(w.welcome.max_frame_bytes, 1024 * 1024);
    assert_eq!(w.welcome.members.len(), 1);
    assert_eq!(w.welcome.members[0].endpoint, rig.addr.to_string());
    assert_eq!(w.welcome.limits, Some(service::engine_limits(&rig.cfg)));

    let module = rig.put_blob(b"m");
    rig.start_run(&module, b"{}").await;
    let job = rig.next_job().await;
    let _h = rig.attach(&job, "w1");
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        w.heartbeat(vec![lease(&job)], 1);
        tokio::time::sleep(Duration::from_millis(u64::from(
            u32::try_from(w.welcome.heartbeat_ms).unwrap(),
        )))
        .await;
    }
    assert!(!job.revoked(), "no expiry over 3 x ttl");
    // A lease the session does not hold comes back as LeaseLost.
    let mut bogus = lease(&job);
    bogus.token = Some(v1::Token { epoch: 5, n: 5 });
    w.heartbeat(vec![bogus], 1);
    loop {
        match w.next(T).await {
            Some(Ok(v1::server_msg::Msg::LeaseLost(l))) => {
                assert_eq!(l.token, Some(v1::Token { epoch: 5, n: 5 }));
                break;
            }
            Some(Ok(_)) => {}
            other => panic!("expected LeaseLost, got {other:?}"),
        }
    }
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_authz_is_lease_scoped() {
    let rig = Rig::start(RigOpts {
        node: true,
        max_frame_bytes: 64 * 1024,
        ..RigOpts::default()
    })
    .await;
    let module_bytes = vec![0x5a; 200 * 1024];
    let module = rig.put_blob(&module_bytes);
    // 300 KiB input: RunStarted is a blob row.
    let big = format!("{{\"x\":\"{}\"}}", "y".repeat(300 * 1024));
    let run_a = rig.start_run(&module, big.as_bytes()).await;
    let mut job_a = rig.next_job().await;
    assert_eq!(job_a.job.run, run_a);
    let run_b = rig.start_run(&module, b"{}").await;
    let job_b = rig.next_job().await;
    assert_eq!(job_b.job.run, run_b);

    let w1 = rig.worker("w1", 2).await;
    let w2 = rig.worker("w2", 2).await;
    let _ha = rig.attach(&job_a, "w1");

    let fetch_journal = |c: &p7_workers::Client, sid: Vec<u8>, j: &p7_workers::CapturedJob| {
        let req = v1::FetchJournalRequest {
            session_id: sid,
            run: Some(j.wire_run()),
            token: Some(j.wire_token()),
            from_seq: 0,
            to_seq: 1,
        };
        let mut c = c.clone();
        async move {
            let mut s = c.fetch_journal(req).await?.into_inner();
            let mut rows = Vec::new();
            while let Some(p) = s.message().await? {
                rows.extend(p.rows);
                if p.last {
                    break;
                }
            }
            Ok::<_, tonic::Status>(rows)
        }
    };
    let fetch_blob =
        |c: &p7_workers::Client, sid: Vec<u8>, j: &p7_workers::CapturedJob, sha: Vec<u8>| {
            let req = v1::FetchBlobRequest {
                session_id: sid,
                run: Some(j.wire_run()),
                token: Some(j.wire_token()),
                sha,
                offset: 0,
            };
            let mut c = c.clone();
            async move {
                let mut s = c.fetch_blob(req).await?.into_inner();
                let mut out = Vec::new();
                let mut chunks = 0;
                while let Some(ch) = s.message().await? {
                    out.extend(ch.data);
                    chunks += 1;
                    if ch.last {
                        break;
                    }
                }
                Ok::<_, tonic::Status>((out, chunks))
            }
        };

    // The leased run: rows (RunStarted as a BlobRef) and its blobs.
    let rows = fetch_journal(&w1.client, w1.session_id(), &job_a)
        .await
        .expect("journal");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].seq, 0);
    let Some(v1::journal_row::Payload::Blob(r)) = rows[0].payload.clone() else {
        panic!(
            "expected a blob row: {:?}",
            rows[0].payload.as_ref().map(|_| ())
        );
    };
    let (ev, _) = fetch_blob(&w1.client, w1.session_id(), &job_a, r.sha.clone())
        .await
        .expect("journal blob");
    assert_eq!(ev.len() as u64, r.len);
    assert_eq!(Sha256::digest(&ev).as_slice(), r.sha.as_slice());
    let (m, chunks) = fetch_blob(&w1.client, w1.session_id(), &job_a, module.to_vec())
        .await
        .expect("module");
    assert_eq!(m, module_bytes);
    assert!(chunks >= 4, "chunked within max_frame_bytes: {chunks}");
    assert!(rig.wire.max(WireDir::Out, "fetch_blob") <= 64 * 1024);
    assert!(rig.wire.max(WireDir::Out, "fetch_journal") <= 64 * 1024);

    // A run the session does not lease: PermissionDenied.
    let e = fetch_journal(&w1.client, w1.session_id(), &job_b)
        .await
        .expect_err("run b");
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e:?}");
    let e = fetch_blob(&w1.client, w1.session_id(), &job_b, module.to_vec())
        .await
        .expect_err("run b blob");
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    // A blob that is neither the module nor in the journal.
    let e = fetch_blob(&w1.client, w1.session_id(), &job_a, vec![7; 32])
        .await
        .expect_err("foreign blob");
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    // W6: a second worker presenting w1's session id with its own certificate.
    let e = fetch_journal(&w2.client, w1.session_id(), &job_a)
        .await
        .expect_err("w6");
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e:?}");
    let e = fetch_blob(&w2.client, w1.session_id(), &job_a, module.to_vec())
        .await
        .expect_err("w6 blob");
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    // ... and its own session does not lease run a.
    let e = fetch_journal(&w2.client, w2.session_id(), &job_a)
        .await
        .expect_err("w2 own session");
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    drop((w1, w2));
    job_a.finish(ExecOutcome::Released);
    rig.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_append_and_start_effect_route_to_the_job_handler() {
    let rig = Rig::start(RigOpts {
        node: true,
        ..RigOpts::default()
    })
    .await;
    let module = rig.put_blob(b"m");
    rig.start_run(&module, b"{}").await;
    let job = rig.next_job().await;
    let w = rig.worker("w1", 2).await;
    let h = rig.attach(&job, "w1");
    let frames = |chunks: Vec<(u32, Vec<u8>, bool, bool)>| {
        let sid = w.session_id();
        let run = job.wire_run();
        let token = job.wire_token();
        chunks
            .into_iter()
            .map(move |(idx, chunk, end, last)| v1::AppendFrame {
                session_id: sid.clone(),
                run: Some(run.clone()),
                token: Some(token),
                from_seq: 1,
                event_index: idx,
                chunk,
                event_end: end,
                last,
                kind: 21,
            })
            .collect::<Vec<_>>()
    };
    let effect = v1::StartEffectRequest {
        session_id: w.session_id(),
        run: Some(job.wire_run()),
        token: Some(job.wire_token()),
        op: 3,
        ..Default::default()
    };
    // No handler yet: UNIMPLEMENTED.
    let mut c = w.client.clone();
    let e = c
        .append(tonic::codegen::tokio_stream::iter(frames(vec![(
            0,
            b"ab".to_vec(),
            true,
            true,
        )])))
        .await
        .expect_err("no handler");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    let e = c
        .start_effect(effect.clone())
        .await
        .expect_err("no handler");
    assert_eq!(e.code(), tonic::Code::Unimplemented);

    // With a handler: frames reassembled per event, the reply returned.
    let mut rpcs = h.rpcs();
    let handler = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(r) = rpcs.recv().await {
            match r {
                JobRpc::Append { batch, reply } => {
                    seen.push(format!("{batch:?}"));
                    let next = batch.from_seq + batch.events.len() as u64;
                    let _ = reply.send(Ok(v1::AppendResult {
                        result: Some(v1::append_result::Result::Next(next)),
                    }));
                }
                JobRpc::StartEffect { req, reply } => {
                    seen.push(format!("effect {}", req.op));
                    let _ = reply.send(Ok(v1::StartEffectResponse {
                        result: Some(v1::start_effect_response::Result::Started(true)),
                    }));
                }
            }
        }
        seen
    });
    let r = c
        .append(tonic::codegen::tokio_stream::iter(frames(vec![
            (0, b"ab".to_vec(), false, false),
            (0, b"cd".to_vec(), true, false),
            (1, b"ef".to_vec(), true, true),
        ])))
        .await
        .expect("append")
        .into_inner();
    assert_eq!(r.result, Some(v1::append_result::Result::Next(3)));
    let r = c.start_effect(effect).await.expect("effect").into_inner();
    assert_eq!(
        r.result,
        Some(v1::start_effect_response::Result::Started(true))
    );
    // Malformed: a frame for event 2 where event 0 was expected.
    let r = c
        .append(tonic::codegen::tokio_stream::iter(frames(vec![(
            2,
            b"x".to_vec(),
            true,
            true,
        )])))
        .await
        .expect("append")
        .into_inner();
    assert!(
        matches!(r.result, Some(v1::append_result::Result::Rejected(_))),
        "{r:?}"
    );
    // W6 on Append: another identity using w1's session.
    let w2 = rig.worker("w2", 1).await;
    let mut c2 = w2.client.clone();
    let e = c2
        .append(tonic::codegen::tokio_stream::iter(frames(vec![(
            0,
            b"ab".to_vec(),
            true,
            true,
        )])))
        .await
        .expect_err("w6");
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    drop(h);
    let seen = handler.await.expect("handler");
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert!(seen[0].contains("from_seq: 1"), "{seen:?}");
    assert!(
        seen[0].contains("[97, 98, 99, 100]"),
        "abcd reassembled: {seen:?}"
    );
    assert_eq!(seen[1], "effect 3");
    assert_eq!(rig.wire.max(WireDir::In, "append"), 2);
    rig.stop().await;
}

/// The Renew RPC signs a new certificate for a caller holding a session; the renewed
/// worker's new session takes over the lease (07-07 renewal) and the old session's Drain
/// revokes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_renew_and_session_handoff() {
    let rig = Rig::start(RigOpts {
        node: true,
        ..RigOpts::default()
    })
    .await;
    let module = rig.put_blob(b"m");
    rig.start_run(&module, b"{}").await;
    let job = rig.next_job().await;
    let w = rig.worker("w1", 2).await;
    let mut h = rig.attach(&job, "w1");
    let (key_pem, csr) = new_key_and_csr("w1");
    let mut c = w.client.clone();
    let r = c
        .renew(v1::RenewRequest {
            csr: csr.into_bytes(),
        })
        .await
        .expect("renew")
        .into_inner();
    assert!(r.renew_after_ms > 0);
    let new_id = p7_workers::TestIdentity {
        cert_pem: String::from_utf8(r.client_cert).expect("pem"),
        key_pem,
        ca_pem: rig.ca_pem(),
    };
    let w_new = TestWorker::open(rig.addr, &new_id, hello("w1", 2))
        .await
        .expect("new session");
    w_new.heartbeat(vec![lease(&job)], 1);
    rig.wait(T, "lease moved", || {
        h.session_id().as_deref() == Some(w_new.session_id().as_slice())
    })
    .await;
    w.send(v1::worker_msg::Msg::Drain(v1::Drain {}));
    let old_sid = w.session_id();
    drop(w);
    rig.wait(T, "old session closed", || {
        rig.registry
            .sessions()
            .iter()
            .all(|s| s.session_id != old_sid)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        h.try_event().is_none(),
        "no Abandoned after a renewal drain"
    );
    assert!(!job.revoked());

    // Renew without a session is refused.
    let tok = rig.token(Duration::from_secs(60)).await;
    let lonely = rig.identity(&tok.token, "w9").await;
    let mut c = connect(rig.addr, &rig.ca_pem(), Some(&lonely))
        .await
        .expect("connect");
    let e = c
        .renew(v1::RenewRequest {
            csr: new_key_and_csr("w9").1.into_bytes(),
        })
        .await
        .expect_err("no session");
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    drop(w_new);
    rig.stop().await;
}

/// D-W25: certificates from `[cluster.tls] client_ca` must carry
/// `ostro://<cluster>/worker/ext/<id>` (+ clientAuth); one session per external id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_external_ca_identities() {
    let ext = OtherCa::new("cert-manager CA");
    let rig = Rig::start(RigOpts {
        client_ca_pem: Some(ext.pem.clone()),
        ..RigOpts::default()
    })
    .await;
    let ok = ext.client(
        &format!("ostro://{CLUSTER}/worker/ext/e1"),
        &rig.ca_pem(),
        true,
    );
    let w = TestWorker::open(rig.addr, &ok, hello("e1", 1))
        .await
        .expect("external identity accepted");
    let s = rig.registry.sessions();
    assert_eq!(
        (s[0].worker_id.as_str(), s[0].token_id.clone()),
        ("e1", None)
    );
    // One session per external worker id.
    match TestWorker::try_open(rig.addr, &ok, hello("e1", 1))
        .await
        .expect("connect")
    {
        (None, Opened::Refused(st)) => assert_eq!(st.code(), tonic::Code::AlreadyExists),
        (_, o) => panic!("second external session: {o:?}"),
    }
    for (san, eku, what) in [
        (
            "ostro://other-cluster/worker/ext/e2".to_owned(),
            true,
            "wrong cluster",
        ),
        (
            format!("ostro://{CLUSTER}/worker/wkr_x/e2"),
            true,
            "token-style SAN",
        ),
        (
            format!("ostro://{CLUSTER}/worker/ext/e2"),
            false,
            "no clientAuth",
        ),
    ] {
        let id = ext.client(&san, &rig.ca_pem(), eku);
        match TestWorker::try_open(rig.addr, &id, hello("e2", 1)).await {
            Ok((None, Opened::Refused(st))) => {
                assert_eq!(st.code(), tonic::Code::PermissionDenied, "{what}: {st:?}")
            }
            Err(e) if !eku => println!("{what}: refused at the handshake: {e}"),
            other => panic!("{what}: {:?}", other.map(|(_, o)| o)),
        }
    }
    // Join-issued identities still work next to the external CA.
    let tok = rig.token(Duration::from_secs(60)).await;
    let id = rig.identity(&tok.token, "j1").await;
    TestWorker::open(rig.addr, &id, hello("j1", 1))
        .await
        .expect("join identity");
    drop(w);
    rig.stop().await;
}

/// `ostro server` wiring: the cluster listener is bound on start (ephemeral port), the
/// join service hands out its address and refuses connections without a client cert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_server_starts_the_cluster_listener() {
    let mut server = common::TestServer::start(common::ServerOpts::default()).await;
    let p7 = Arc::clone(server.h().p7());
    let eps = p7.join.endpoints();
    assert_eq!(eps.len(), 1, "{eps:?}");
    let addr: std::net::SocketAddr = eps[0].parse().expect("addr");
    assert!(addr.ip().is_loopback() && addr.port() != 0);
    let ca = p7.tls.ca_pem().expect("ca");
    let r = match connect(addr, &ca, None).await {
        Err(e) => Err(e),
        Ok(mut c) => c
            .fetch_blob(v1::FetchBlobRequest::default())
            .await
            .map(|_| ())
            .map_err(|s| s.to_string()),
    };
    assert!(r.is_err());
    // Tokens work through the server's JoinService (bound by service::start).
    let t = p7
        .join
        .create_token(None, "http://127.0.0.1:1")
        .await
        .expect("token");
    assert!(
        p7.join
            .list_tokens()
            .iter()
            .any(|i| i.token_id == t.token_id)
    );
    drop(p7);
    server.shutdown().await;
}
