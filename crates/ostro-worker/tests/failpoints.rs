//! ostro-worker fault-injection suite (`--features failpoints`; 07-07, extended by 07-21).
//! Runs single-threaded in ci-local's "failpoint suites" step (the fail registry is global).
//!
//! Names (fixed, used by 07-18 / 07-21): `worker::before_append`, `worker::before_done`,
//! `worker::before_start_effect`, `worker::after_start_effect`.

#[cfg(feature = "failpoints")]
mod common;

#[cfg(feature = "failpoints")]
mod fp {
    use std::time::Duration;

    use super::common::*;
    use ostro_worker::WorkerEvent;
    use ostro_worker_proto::WireOutcome;
    use serde_json::json;

    const T: Duration = Duration::from_secs(60);

    #[test]
    fn failpoints_feature_compiles() {
        let _scenario = fail::FailScenario::setup();
    }

    async fn start(
        h: &Harness,
        n: u8,
        guest: &str,
        input: serde_json::Value,
    ) -> (Running, Vec<u8>) {
        let id = run_id(n);
        let sha = h.seed(guest, guest, &id, input);
        let mut w = spawn_worker(h.cfg());
        w.wait_event(T, "connected", |e| {
            matches!(e, WorkerEvent::Connected { .. })
        })
        .await;
        assert!(h.st().send(activate(&id, 1, &sha, 1)));
        (w, id)
    }

    #[tokio::test]
    async fn before_append_error_sends_no_frames() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("worker::before_append", "return(error)").unwrap();
        let h = Harness::new(FakeConfig::default()).await;
        let (w, id) = start(&h, 1, "long-journal", json!({"steps": 3})).await;
        h.st()
            .wait_for(T, "Done", |st| !st.dones(&id).is_empty())
            .await;
        assert!(matches!(
            outcome(&h.st().dones(&id)[0]),
            WireOutcome::Evicted { .. }
        ));
        assert!(h.st().frames.lock().unwrap().is_empty());
        assert_eq!(h.st().journal_len(&id), 1);
        w.stop().await.unwrap();
        scenario.teardown();
    }

    #[tokio::test]
    async fn before_done_error_sends_no_done() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("worker::before_done", "return(error)").unwrap();
        let h = Harness::new(FakeConfig::default()).await;
        let (mut w, id) = start(&h, 2, "long-journal", json!({"steps": 3})).await;
        w.wait_event(T, "ended", |e| {
            matches!(e, WorkerEvent::ActivationEnded { .. })
        })
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(h.st().dones(&id).is_empty());
        assert!(h.st().journal_len(&id) > 1, "the appends went out");
        w.stop().await.unwrap();
        scenario.teardown();
    }

    #[tokio::test]
    async fn start_effect_failpoints() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("worker::before_start_effect", "return(error)").unwrap();
        let h = Harness::new(FakeConfig::default()).await;
        let (w, id) = start(&h, 3, "pay-flow", json!({"url": "http://127.0.0.1:9"})).await;
        h.st()
            .wait_for(T, "EffectScheduled", |st| {
                st.journal(&id)
                    .iter()
                    .any(|e| matches!(e.body, ostro_types::journal::EventBody::EffectScheduled(_)))
            })
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            h.st().effects.lock().unwrap().is_empty(),
            "StartEffect suppressed"
        );
        w.stop().await.unwrap();

        fail::remove("worker::before_start_effect");
        fail::cfg("worker::after_start_effect", "return(error)").unwrap();
        let h = Harness::new(FakeConfig::default()).await;
        let (w, _id) = start(&h, 4, "pay-flow", json!({"url": "http://127.0.0.1:9"})).await;
        h.st()
            .wait_for(T, "StartEffect", |st| {
                !st.effects.lock().unwrap().is_empty()
            })
            .await;
        w.stop().await.unwrap();
        scenario.teardown();
    }
}
