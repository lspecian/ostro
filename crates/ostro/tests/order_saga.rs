//! Success criterion 1 of Phase 5 (plan 05-17), driven **only through the `ostro` CLI**:
//! the `examples/order-saga` workflow (5 steps, a durable timer, an approval signal raced
//! with cancellation, shielded compensations) against a spawned `ostro dev`.
//!
//! - `happy_path`: deploy, run, signal `approve true`, `result --wait` exits 0
//!   (completed). History: one `step_completed` per forward step, one timer
//!   (`timer_started` + `timer_fired`), one signal (`signal_payload` + `signal_matched`,
//!   D21: Phase 5 never produces `signal_received`).
//! - `compensated_paths`: `fail_at: create_shipment` (refund then release, after the
//!   failure), `fail_at: charge_payment` (3 attempts, then only release), and a rejection
//!   (`approve false`). All complete with `status: compensated`.
//! - `cancelled_path`: `ostro cancel` during the approval wait runs the shielded
//!   compensations, the run ends `cancelled`, and `result --wait` exits 3.
//! - `kill9_during_settle_sleep_then_approve`: SIGKILL of a non-ephemeral `ostro dev`
//!   while the run sleeps, restart on the same data dir, approve: completes with exactly
//!   one `step_completed` per step (no step re-executed).
//!
//! Exit codes are the 05-14 ones: 0 ok, 1 API error, 2 usage, 3 wait ended not completed.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use common::*;
use serde_json::{Value, json};

const FORWARD: [&str; 5] = [
    "reserve_inventory",
    "charge_payment",
    "create_shipment",
    "notify_customer",
    "complete_order",
];

/// One spawned `ostro dev` (P=4) plus the CLI against it.
struct Dev {
    srv: Spawned,
    addr: SocketAddr,
}

impl Dev {
    /// `--ephemeral` when `data` is `None`, else `--data-dir <data>`.
    async fn start(cwd: &Path, data: Option<&Path>) -> Self {
        let addr = free_addr();
        let a = addr.to_string();
        let mut args = vec!["--listen".to_owned(), a];
        match data {
            Some(d) => {
                args.push("--data-dir".to_owned());
                args.push(d.to_str().expect("utf-8 path").to_owned());
            }
            None => args.push("--ephemeral".to_owned()),
        }
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        let mut cmd = dev(cwd, &argv);
        cmd.env("OSTRO_NODE__PARTITIONS", "4")
            .env("OSTRO_LOG__LEVEL", "warn");
        let srv = Spawned::spawn(cmd);
        srv.wait_readyz(&http(), addr).await;
        Self { srv, addr }
    }

    /// `ostro <args> --json`, asserting exit 0.
    fn ok(&self, args: &[&str]) -> Value {
        let (code, body, err) = cli_json(self.addr, args);
        assert_eq!(code, 0, "{args:?}: {body}\n{err}");
        body
    }

    /// `ostro deploy order_saga.wasm --activate`.
    fn deploy(&self) {
        let wasm = order_saga_wasm();
        let b = self.ok(&["deploy", wasm.to_str().expect("utf-8 path"), "--activate"]);
        assert_eq!(b["activated"][0]["name"], "order-saga", "{b}");
        assert_eq!(b["activated"][0]["active_version"], 1, "{b}");
    }

    /// `ostro run order-saga --input <input> --json`; returns the run id.
    fn run(&self, input: Value) -> String {
        let s = input.to_string();
        let b = self.ok(&["run", "order-saga", "--input", &s]);
        assert_eq!(b["created"], true, "{b}");
        b["run_id"].as_str().expect("run_id").to_owned()
    }

    fn signal(&self, run: &str, payload: &str) {
        let b = self.ok(&["signal", run, "approve", "--payload", payload]);
        assert_eq!(b["accepted"], true, "{b}");
    }

    /// `ostro result <run> --wait --json`: (exit code, body).
    fn result_wait(&self, run: &str) -> (i32, Value) {
        let (code, body, err) = cli_json(self.addr, &["result", run, "--wait"]);
        assert!(
            code == 0 || code == 3,
            "result --wait: exit {code}: {body}\n{err}"
        );
        (code, body)
    }

    /// The whole history via `ostro history`.
    fn history(&self, run: &str) -> Vec<Value> {
        self.ok(&["history", run, "--limit", "1000"])["events"]
            .as_array()
            .expect("events")
            .clone()
    }

    /// Poll `ostro history` until an event of `kind` appears.
    fn wait_kind(&self, run: &str, kind: &str) -> Vec<Value> {
        let deadline = Instant::now() + T;
        loop {
            let h = self.history(run);
            if h.iter().any(|e| e["kind"] == kind) {
                return h;
            }
            assert!(
                Instant::now() < deadline,
                "run {run}: no {kind} event: {:?}",
                kinds(&h)
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn kinds(h: &[Value]) -> Vec<String> {
    h.iter()
        .map(|e| e["kind"].as_str().unwrap_or_default().to_owned())
        .collect()
}

/// Names of the successful `step_completed` events, in journal order.
fn ok_steps(h: &[Value]) -> Vec<String> {
    h.iter()
        .filter(|e| e["kind"] == "step_completed" && e["details"]["outcome"].get("ok").is_some())
        .map(|e| e["details"]["name"].as_str().expect("name").to_owned())
        .collect()
}

/// Index of the first event of `kind` (panics when absent).
fn pos(h: &[Value], pred: impl Fn(&Value) -> bool, what: &str) -> usize {
    h.iter()
        .position(pred)
        .unwrap_or_else(|| panic!("no {what}: {:?}", kinds(h)))
}

fn order(id: &str, extra: Value) -> Value {
    let mut v = json!({"order_id": id, "amount": 100, "sku": "s", "qty": 1, "settle_ms": 300});
    if let (Some(m), Some(e)) = (v.as_object_mut(), extra.as_object()) {
        for (k, x) in e {
            m.insert(k.clone(), x.clone());
        }
    }
    v
}

/// Every forward step exactly once, one timer, one signal.
fn assert_happy_history(h: &[Value]) {
    for s in FORWARD {
        assert_eq!(step_completions(h, s), 1, "step {s}: {:?}", kinds(h));
    }
    assert_eq!(ok_steps(h), FORWARD, "{:?}", kinds(h));
    assert_eq!(events_of(h, "timer_started").len(), 1, "{:?}", kinds(h));
    assert_eq!(events_of(h, "timer_fired").len(), 1, "{:?}", kinds(h));
    let payloads = events_of(h, "signal_payload");
    assert_eq!(payloads.len(), 1, "{:?}", kinds(h));
    assert_eq!(payloads[0]["details"]["name"], "approve");
    assert_eq!(payloads[0]["details"]["payload"], true);
    assert_eq!(events_of(h, "signal_matched").len(), 1, "{:?}", kinds(h));
    assert!(
        events_of(h, "signal_received").is_empty(),
        "D21: {:?}",
        kinds(h)
    );
    assert_eq!(events_of(h, "run_completed").len(), 1, "{:?}", kinds(h));
    assert_eq!(h[0]["kind"], "run_started", "{:?}", kinds(h));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_path() {
    let wasm = order_saga_wasm();
    assert!(wasm.is_file());
    let _g = SERIAL.lock().await;
    let cwd = tempfile::tempdir().expect("tempdir");
    let d = Dev::start(cwd.path(), None).await;
    d.deploy();

    let run = d.run(order("o1", json!({})));
    d.signal(&run, "true");
    let (code, r) = d.result_wait(&run);
    assert_eq!(code, 0, "{r}");
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(
        r["result"],
        json!({"status": "completed", "order_id": "o1", "shipment_id": "ship-o1"})
    );
    assert_happy_history(&d.history(&run));

    // `inspect` agrees with the result.
    assert_eq!(d.ok(&["inspect", &run])["status"], "completed");
    // The run is listed.
    let ls = d.ok(&["ls", "--workflow", "order-saga"]);
    assert!(
        ls["runs"]
            .as_array()
            .expect("runs")
            .iter()
            .any(|x| x["run_id"] == run.as_str()),
        "{ls}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compensated_paths() {
    let _ = order_saga_wasm();
    let _g = SERIAL.lock().await;
    let cwd = tempfile::tempdir().expect("tempdir");
    let d = Dev::start(cwd.path(), None).await;
    d.deploy();

    // 1. create_shipment fails after approval: refund_payment, then release_inventory.
    let ship = d.run(order("o2", json!({"fail_at": "create_shipment"})));
    // 2. charge_payment fails retryably on every attempt (3 attempts): only the
    //    reservation is compensated.
    let charge = d.run(order("o3", json!({"fail_at": "charge_payment"})));
    // 3. rejected by the approver.
    let reject = d.run(order("o4", json!({})));
    d.signal(&ship, "true");
    d.signal(&reject, "false");

    let (code, r) = d.result_wait(&ship);
    assert_eq!(code, 0, "{r}");
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(r["result"]["status"], "compensated", "{r}");
    assert_eq!(
        r["result"]["reason"],
        "forced_failure: create_shipment forced to fail"
    );
    assert_eq!(
        r["result"]["compensations"],
        json!(["refund_payment", "release_inventory"])
    );
    let h = d.history(&ship);
    let failed = pos(
        &h,
        |e| {
            e["kind"] == "step_completed"
                && e["details"]["name"] == "create_shipment"
                && e["details"]["outcome"].get("error").is_some()
        },
        "failed create_shipment",
    );
    let refund = pos(
        &h,
        |e| e["kind"] == "step_completed" && e["details"]["name"] == "refund_payment",
        "refund_payment",
    );
    let release = pos(
        &h,
        |e| e["kind"] == "step_completed" && e["details"]["name"] == "release_inventory",
        "release_inventory",
    );
    assert!(failed < refund && refund < release, "{:?}", kinds(&h));
    assert_eq!(
        ok_steps(&h),
        [
            "reserve_inventory",
            "charge_payment",
            "refund_payment",
            "release_inventory"
        ]
    );

    let (code, r) = d.result_wait(&charge);
    assert_eq!(code, 0, "{r}");
    assert_eq!(r["result"]["status"], "compensated", "{r}");
    assert_eq!(r["result"]["compensations"], json!(["release_inventory"]));
    let h = d.history(&charge);
    assert_eq!(
        events_of(&h, "step_attempt_failed").len(),
        2,
        "3 attempts = 2 retried failures + the final one: {:?}",
        kinds(&h)
    );
    assert!(events_of(&h, "timer_started").is_empty(), "{:?}", kinds(&h));
    assert_eq!(ok_steps(&h), ["reserve_inventory", "release_inventory"]);

    let (code, r) = d.result_wait(&reject);
    assert_eq!(code, 0, "{r}");
    assert_eq!(
        r["result"],
        json!({
            "status": "compensated",
            "order_id": "o4",
            "reason": "rejected",
            "compensations": ["refund_payment", "release_inventory"]
        })
    );
    let h = d.history(&reject);
    let matched = pos(&h, |e| e["kind"] == "signal_matched", "signal_matched");
    let refund = pos(
        &h,
        |e| e["kind"] == "step_completed" && e["details"]["name"] == "refund_payment",
        "refund_payment",
    );
    assert!(matched < refund, "{:?}", kinds(&h));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_path() {
    let _ = order_saga_wasm();
    let _g = SERIAL.lock().await;
    let cwd = tempfile::tempdir().expect("tempdir");
    let d = Dev::start(cwd.path(), None).await;
    d.deploy();

    let run = d.run(order("o5", json!({"settle_ms": 100})));
    // Cancel only once the run waits for approval.
    d.wait_kind(&run, "signal_wait_started");
    let b = d.ok(&["cancel", &run, "--reason", "customer changed their mind"]);
    assert_eq!(b["already_requested"], false, "{b}");

    let (code, r) = d.result_wait(&run);
    assert_eq!(code, 3, "cancelled is not completed: {r}");
    assert_eq!(r["status"], "cancelled", "{r}");
    assert_eq!(d.ok(&["inspect", &run])["status"], "cancelled");

    let h = d.history(&run);
    let cancel = pos(&h, |e| e["kind"] == "cancel_requested", "cancel_requested");
    let refund = pos(
        &h,
        |e| e["kind"] == "step_completed" && e["details"]["name"] == "refund_payment",
        "refund_payment",
    );
    let release = pos(
        &h,
        |e| e["kind"] == "step_completed" && e["details"]["name"] == "release_inventory",
        "release_inventory",
    );
    assert!(cancel < refund && refund < release, "{:?}", kinds(&h));
    assert_eq!(
        ok_steps(&h),
        [
            "reserve_inventory",
            "charge_payment",
            "refund_payment",
            "release_inventory"
        ]
    );
    // No forward step after the approval.
    for s in ["create_shipment", "notify_customer", "complete_order"] {
        assert_eq!(step_completions(&h, s), 0, "{s}: {:?}", kinds(&h));
    }
    // A late approval is refused: the run is terminal.
    let (code, body, _) = cli_json(d.addr, &["signal", &run, "approve", "--payload", "true"]);
    assert_eq!(code, 1, "{body}");
    assert_eq!(body["error"]["details"]["reason"], "run_terminal", "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill9_during_settle_sleep_then_approve() {
    let _ = order_saga_wasm();
    let _g = SERIAL.lock().await;
    let cwd = tempfile::tempdir().expect("tempdir");
    let data = tempfile::tempdir().expect("data dir");

    let mut d = Dev::start(cwd.path(), Some(data.path())).await;
    d.deploy();
    let run = d.run(order("o6", json!({"settle_ms": 3000})));
    // The run is in its settle sleep: both payment steps done, timer not fired.
    let h = d.wait_kind(&run, "timer_started");
    assert!(events_of(&h, "timer_fired").is_empty(), "{:?}", kinds(&h));
    assert_eq!(ok_steps(&h), ["reserve_inventory", "charge_payment"]);

    d.srv.signal("KILL");
    let st = d.srv.wait_exit(T);
    assert!(!st.success(), "killed: {st:?}");
    drop(d);

    let d = Dev::start(cwd.path(), Some(data.path())).await;
    // The timer survives the crash and fires; then the run waits for approval.
    d.wait_kind(&run, "signal_wait_started");
    d.signal(&run, "true");
    let (code, r) = d.result_wait(&run);
    assert_eq!(code, 0, "{r}");
    assert_eq!(
        r["result"],
        json!({"status": "completed", "order_id": "o6", "shipment_id": "ship-o6"})
    );
    assert_happy_history(&d.history(&run));
}
