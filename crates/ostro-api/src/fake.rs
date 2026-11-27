//! In-memory [`ControlPlane`] for adapter tests (REST here, gRPC parity in 04-11).
//!
//! Semantics mirror what the real server implements; ids and timestamps are
//! deterministic (counter-based) so two adapters driven by identical calls produce
//! identical outputs.
//!
//! Fake module format: the fake cannot introspect Wasm, so a module whose bytes are
//! UTF-8 `ostro-fake:<name>@<version>[,<name>@<version>...]` registers those workflow
//! versions; any other bytes register none. The first version registered for a
//! workflow becomes active.
//!
//! Phase 5 lifecycle (signal, cancel, terminate, retry, long-poll, await tokens,
//! policies) follows D1/D3/D13 in-memory. Fake await tokens are
//! `ot1_<run_id>.<op>` and exist only after [`FakeControlPlane::mint_token`]; any
//! other text is `token_invalid`. Every mutating call advances the fake clock, so
//! [`FakeControlPlane::clock_ticks`] staying put proves a `dry_run` mutated nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use crate::domain::*;
use crate::error::ApiError;
use crate::phase7::{
    CreateWorkerTokenRequest, DemandSnapshot, DrainResult, JoinRequest, JoinResponse,
    PauseRunRequest, PauseState, Placement, QueueFlow, QueueFlowInfo, REASON_JOIN_TOKEN_INVALID,
    RunPauseState, WORKER_TOKEN_PREFIX, WorkerInfo, WorkerTokenCreated, WorkerTokenInfo,
};

/// In-memory Phase 8 traits (schedules, webhooks, hooks, run logs, backup), 08-10.
pub use crate::phase8::fake::{FAKE_BACKUP, FakePhase8};

/// Base of the fake clock (2023-11-14T22:13:20Z).
pub const FAKE_EPOCH_MS: u64 = 1_700_000_000_000;
/// Fake clock advance per mutating call.
pub const FAKE_TICK_MS: u64 = 1_000;

/// Prefix of the fake module format.
pub const FAKE_MODULE_PREFIX: &str = "ostro-fake:";

/// Build fake module bytes registering `workflows` (`("name", version)`).
pub fn fake_module(workflows: &[(&str, u32)]) -> bytes::Bytes {
    let list: Vec<String> = workflows.iter().map(|(n, v)| format!("{n}@{v}")).collect();
    bytes::Bytes::from(format!("{FAKE_MODULE_PREFIX}{}", list.join(",")))
}

/// Lowercase hex sha256 of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut s = String::with_capacity(64);
    for b in digest.iter() {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[derive(Default)]
struct State {
    clock: u64,
    next_run: u64,
    modules: BTreeMap<String, ModuleInfo>,
    workflows: BTreeMap<String, WorkflowInfo>,
    runs: BTreeMap<String, RunInfo>,
    history: BTreeMap<String, Vec<HistoryEvent>>,
    idempotency: BTreeMap<(Option<String>, String, String), String>,
    /// Retry-mode override per run (default: Terminated/TimedOut resume, D1).
    resumable: BTreeMap<String, bool>,
    cancel_requested: BTreeMap<String, Option<String>>,
    /// (run, request_id) of delivered signals (D13 dedupe).
    signals_seen: BTreeSet<(String, String)>,
    signals: BTreeMap<String, Vec<(String, serde_json::Value)>>,
    /// (run, op) -> completed?
    tokens: BTreeMap<(String, u64), bool>,
    continued_to: BTreeMap<String, String>,
    workflow_policies: BTreeMap<String, RunPolicy>,
    queue_policies: BTreeMap<String, RunPolicy>,
    /// `(sha256, tenant, name)` of every upload past the sha check, in order (the fake does not
    /// qualify names; the real server does, 05-24).
    uploads: Vec<(String, Option<String>, Option<String>)>,
    /// Phase 6 bridge admin (06-08): `(tenant, name)` -> value. Values stay inside the
    /// fake; [`FakeControlPlane::secret_value`] exposes them to tests only.
    secrets: BTreeMap<(Option<String>, String), Vec<u8>>,
    grants: BTreeMap<String, GrantInfo>,
    connections: BTreeMap<String, ConnectionInfo>,
    egress: BTreeMap<String, EgressInfo>,
    last_presign: Option<PresignedObjectRequest>,
    /// Phase 7 (07-11): queue -> (record version, flow).
    queue_flows: BTreeMap<String, (u64, QueueFlow)>,
    paused_workflows: BTreeSet<String>,
    /// run -> pause detail.
    paused_runs: BTreeMap<String, Option<String>>,
    workers: BTreeMap<String, WorkerInfo>,
    next_worker_token: u64,
    /// token id -> (info, secret).
    worker_tokens: BTreeMap<String, (WorkerTokenInfo, String)>,
    /// queue -> (rate-limit window index, starts in it).
    rate_windows: BTreeMap<String, (u64, u64)>,
    /// Phase 11 (11-09): the last [`ControlPlane::list_runs`] request.
    last_list_runs: Option<ListRunsRequest>,
    /// Phase 11: failed runs hidden from `dlq = true` listings (dismissed / requeued).
    dlq_hidden: BTreeSet<String>,
    /// Phase 14 (14-02): the signatures of the last upload past the sha check.
    last_upload_signatures: Vec<Vec<u8>>,
}

impl State {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        FAKE_EPOCH_MS + self.clock * FAKE_TICK_MS
    }

    /// The fake clock without advancing it.
    fn now_ms(&self) -> u64 {
        FAKE_EPOCH_MS + self.clock * FAKE_TICK_MS
    }

    fn flow_info(&self, queue: &str) -> QueueFlowInfo {
        let (version, flow) = self.queue_flows.get(queue).cloned().unwrap_or_default();
        QueueFlowInfo {
            queue: queue.to_owned(),
            version,
            flow,
        }
    }

    /// Set a queue's paused flag (bumps the version only on a change).
    fn set_queue_paused(&mut self, queue: String, paused: bool) -> PauseState {
        let entry = self.queue_flows.entry(queue.clone()).or_default();
        let changed = entry.1.paused != paused;
        if changed {
            entry.0 += 1;
            entry.1.paused = paused;
            self.tick();
        }
        PauseState {
            name: queue,
            paused,
            changed,
        }
    }

    /// D-W21 in the fake: a queue `rate_limit` admits `limit + burst` starts per
    /// fixed `period_ms` window of the fake clock; excess answers RateLimited.
    fn charge_rate_limit(&mut self, queue: &str) -> Result<(), ApiError> {
        let Some(r) = self
            .queue_flows
            .get(queue)
            .and_then(|(_, f)| f.rate_limit.clone())
        else {
            return Ok(());
        };
        let now = self.now_ms();
        let window = now / r.period_ms;
        let allowed = u64::from(r.limit) + u64::from(r.burst);
        let w = self
            .rate_windows
            .entry(queue.to_owned())
            .or_insert((window, 0));
        if w.0 != window {
            *w = (window, 0);
        }
        if w.1 >= allowed {
            let retry_after_ms = (window + 1) * r.period_ms - now;
            return Err(ApiError::rate_limited(
                format!("queue {queue} is rate limited"),
                retry_after_ms,
            ));
        }
        w.1 += 1;
        Ok(())
    }

    fn run(&self, run_id: &str) -> Result<&RunInfo, ApiError> {
        self.runs
            .get(run_id)
            .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found")))
    }

    fn push_event(&mut self, run_id: &str, ts_ms: u64, kind: &str, details: serde_json::Value) {
        let events = self.history.entry(run_id.to_owned()).or_default();
        let seq = events.len() as u64 + 1;
        events.push(HistoryEvent {
            seq,
            kind: kind.to_owned(),
            ts_ms,
            summary: kind.to_owned(),
            details_json: details.to_string().into_bytes(),
            attempt: None,
        });
    }

    /// Copy `src` into a fresh Ready run with a new id and a RunStarted event.
    fn clone_run(&mut self, src: &str, link: (&str, &str)) -> String {
        let now = self.tick();
        self.next_run += 1;
        let run_id = format!("run-{:06}", self.next_run);
        let mut run = self.runs[src].clone();
        run.run_id = run_id.clone();
        run.status = RunStatus::Ready;
        run.attempt = None;
        run.created_at_ms = now;
        run.completed_at_ms = None;
        run.published_status_json = None;
        run.result_json = None;
        run.failure = None;
        self.runs.insert(run_id.clone(), run);
        self.push_event(
            &run_id,
            now,
            "RunStarted",
            serde_json::json!({ link.0: link.1 }),
        );
        run_id
    }

    fn outcome(&self, run_id: &str) -> AwaitResultResponse {
        let r = &self.runs[run_id];
        AwaitResultResponse {
            run_id: run_id.to_owned(),
            status: r.status,
            completed: r.status.is_terminal(),
            result: r.result_json.as_deref().map(json_or_string),
            failure: r.failure.as_deref().map(|f| json_or_string(f.as_bytes())),
        }
    }
}

/// JSON value of `bytes`, or the text as a JSON string when it is not JSON.
fn json_or_string(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

/// `ot1_<run_id>.<op>` -> (run_id, op).
fn parse_fake_token(token: &str) -> Result<(String, u64), ApiError> {
    let body = token
        .strip_prefix(TOKEN_PREFIX)
        .ok_or_else(|| ApiError::token_invalid("missing ot1_ prefix"))?;
    let (run, op) = body
        .rsplit_once('.')
        .ok_or_else(|| ApiError::token_invalid("malformed token body"))?;
    let op = op
        .parse::<u64>()
        .map_err(|_| ApiError::token_invalid("malformed token body"))?;
    if run.is_empty() {
        return Err(ApiError::token_invalid("malformed token body"));
    }
    Ok((run.to_owned(), op))
}

/// Key id the fake reports for every sealed secret.
pub const FAKE_SECRET_KEY_ID: &str = "fake-key-1";
/// Signature the fake's [`ControlPlane::presigned_object`] accepts; anything else is 403.
pub const FAKE_PRESIGN_VALID_SIG: &str = "valid";
/// Body of a successful fake presigned GET.
pub const FAKE_OBJECT_BODY: &[u8] = b"fake-object-body";

/// PEM client certificate the fake's [`ControlPlane::join_worker`] returns.
pub const FAKE_CLIENT_CERT_PEM: &str =
    "-----BEGIN CERTIFICATE-----\nZmFrZS1jbGllbnQ=\n-----END CERTIFICATE-----\n";
/// PEM CA certificate the fake's [`ControlPlane::join_worker`] returns.
pub const FAKE_CA_CERT_PEM: &str =
    "-----BEGIN CERTIFICATE-----\nZmFrZS1jYQ==\n-----END CERTIFICATE-----\n";
/// Cluster id of the fake.
pub const FAKE_CLUSTER_ID: &str = "fake-cluster";
/// Cluster endpoint the fake's join answer lists.
pub const FAKE_CLUSTER_ENDPOINT: &str = "127.0.0.1:7701";
/// Default join-token lifetime of the fake (24 h, as `[workers] join_token_ttl`).
pub const FAKE_JOIN_TOKEN_TTL_MS: u64 = 24 * 3_600_000;
/// Renewal delay the fake's join answer carries (2/3 of 24 h).
pub const FAKE_RENEW_AFTER_MS: u64 = 16 * 3_600_000;

/// In-memory control plane.
pub struct FakeControlPlane {
    state: Mutex<State>,
    ready: AtomicBool,
    /// Woken on every run status change (long-poll).
    changed: tokio::sync::Notify,
    /// Phase 10 (10-05): dev-clock mode (default `Auto`).
    dev_mode: Mutex<DevTimeMode>,
    /// Phase 10: total dev-clock skip, ms (only grows).
    dev_offset_ms: AtomicU64,
}

impl Default for FakeControlPlane {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeControlPlane {
    /// Empty, ready control plane.
    pub fn new() -> Self {
        FakeControlPlane {
            state: Mutex::new(State::default()),
            ready: AtomicBool::new(true),
            changed: tokio::sync::Notify::new(),
            dev_mode: Mutex::new(DevTimeMode::Auto),
            dev_offset_ms: AtomicU64::new(0),
        }
    }

    /// Test helper (10-05): set the dev-clock mode (`Off` = every dev-time call is 409).
    pub fn set_dev_mode(&self, mode: DevTimeMode) {
        *self.dev_mode.lock().unwrap_or_else(|p| p.into_inner()) = mode;
    }

    /// The dev-clock mode, or 409 `time_skipping_disabled` when it is off.
    fn dev_mode_on(&self) -> Result<DevTimeMode, ApiError> {
        match *self.dev_mode.lock().unwrap_or_else(|p| p.into_inner()) {
            DevTimeMode::Off => Err(time_skipping_disabled()),
            m => Ok(m),
        }
    }

    /// Test helper (11-09): the last request [`ControlPlane::list_runs`] received.
    pub fn last_list_runs(&self) -> Option<ListRunsRequest> {
        self.lock().last_list_runs.clone()
    }

    /// Test helper (11-09): hide a failed run from `dlq = true` listings (as a dismissal
    /// or requeue would).
    pub fn hide_from_dlq(&self, run_id: &str) {
        self.lock().dlq_hidden.insert(run_id.to_owned());
    }

    /// Test helper (11-09): set the `attempt` of history event `seq` of `run_id`.
    pub fn set_history_attempt(&self, run_id: &str, seq: u64, attempt: Option<u64>) {
        if let Some(ev) = self
            .lock()
            .history
            .get_mut(run_id)
            .and_then(|h| h.iter_mut().find(|e| e.seq == seq))
        {
            ev.attempt = attempt;
        }
    }

    /// Test helper: the stored value of a secret (the API never returns one).
    pub fn secret_value(&self, name: &str, tenant: Option<&str>) -> Option<Vec<u8>> {
        self.lock()
            .secrets
            .get(&(tenant.map(str::to_owned), name.to_owned()))
            .cloned()
    }

    /// Test helper: the last request [`ControlPlane::presigned_object`] received.
    pub fn last_presigned_object(&self) -> Option<PresignedObjectRequest> {
        self.lock().last_presign.clone()
    }

    /// Test helper (07-11): register a connected worker for `list_workers` /
    /// `drain_worker` / `queue_demand`.
    pub fn add_worker(&self, w: WorkerInfo) {
        self.lock().workers.insert(w.worker_id.clone(), w);
    }

    /// Test helper (07-11): the stored secret of a join token (never returned by a
    /// list).
    pub fn worker_token_secret(&self, token_id: &str) -> Option<String> {
        self.lock()
            .worker_tokens
            .get(token_id)
            .map(|(_, s)| s.clone())
    }

    /// Test helper (07-11): whether a run is paused, and its pause detail.
    pub fn run_pause(&self, run_id: &str) -> Option<Option<String>> {
        self.lock().paused_runs.get(run_id).cloned()
    }

    /// Flip what [`ControlPlane::ready`] reports.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::SeqCst);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Test helper: move a run to a status, optionally with result/failure/published
    /// status, appending a history event.
    pub fn set_run_outcome(
        &self,
        run_id: &str,
        status: RunStatus,
        result_json: Option<Vec<u8>>,
        failure: Option<String>,
        published_status_json: Option<Vec<u8>>,
    ) -> Result<(), ApiError> {
        let mut st = self.lock();
        let now = st.tick();
        let run = st
            .runs
            .get_mut(run_id)
            .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found")))?;
        run.status = status;
        run.attempt = (status == RunStatus::Running).then_some(1);
        if status.is_terminal() {
            run.completed_at_ms = Some(now);
        }
        if result_json.is_some() {
            run.result_json = result_json;
        }
        if failure.is_some() {
            run.failure = failure;
        }
        if published_status_json.is_some() {
            run.published_status_json = published_status_json;
        }
        let events = st.history.entry(run_id.to_owned()).or_default();
        let seq = events.len() as u64 + 1;
        events.push(HistoryEvent {
            seq,
            kind: format!("Status{}", status.as_str()),
            ts_ms: now,
            summary: format!("status -> {}", status.as_str()),
            details_json: format!("{{\"status\":\"{}\"}}", status.as_str()).into_bytes(),
            attempt: None,
        });
        drop(st);
        self.changed.notify_waiters();
        Ok(())
    }

    /// Mutating calls so far (the fake clock). A `dry_run` never advances it.
    pub fn clock_ticks(&self) -> u64 {
        self.lock().clock
    }

    /// Test helper: override whether `retry_run` resumes (`true`) or restarts
    /// (`false`) this run. Default: Terminated and TimedOut resume, everything else
    /// restarts (D1).
    pub fn set_resumable(&self, run_id: &str, resumable: bool) {
        self.lock().resumable.insert(run_id.to_owned(), resumable);
    }

    /// Test helper: mint the await token of op `op` of `run_id` (the guest's
    /// `await-token` call). Returns `ot1_<run_id>.<op>`.
    pub fn mint_token(&self, run_id: &str, op: u64) -> String {
        self.lock().tokens.insert((run_id.to_owned(), op), false);
        format!("{TOKEN_PREFIX}{run_id}.{op}")
    }

    /// Test helper: has the token of (`run_id`, `op`) been completed?
    pub fn token_completed(&self, run_id: &str, op: u64) -> Option<bool> {
        self.lock().tokens.get(&(run_id.to_owned(), op)).copied()
    }

    /// Test helper: `(tenant, name)` of every upload of `sha256` past the sha check, in order.
    pub fn upload_naming(&self, sha256: &str) -> Vec<(Option<String>, Option<String>)> {
        self.lock()
            .uploads
            .iter()
            .filter(|(s, _, _)| s == sha256)
            .map(|(_, t, n)| (t.clone(), n.clone()))
            .collect()
    }

    /// Test helper (Phase 14): the signatures of the last upload past the sha check.
    pub fn last_upload_signatures(&self) -> Vec<Vec<u8>> {
        self.lock().last_upload_signatures.clone()
    }

    /// Test helper: signals delivered to a run, in order.
    pub fn signals(&self, run_id: &str) -> Vec<(String, serde_json::Value)> {
        self.lock().signals.get(run_id).cloned().unwrap_or_default()
    }

    /// Test helper: the reason of a requested cancellation (`Some(None)` = requested
    /// without a reason).
    pub fn cancel_requested(&self, run_id: &str) -> Option<Option<String>> {
        self.lock().cancel_requested.get(run_id).cloned()
    }

    /// Test helper: continue `run_id` as a new run (status ContinuedAsNew, linked
    /// successor in Ready). Returns the successor's id.
    pub fn continue_as_new(&self, run_id: &str) -> Result<String, ApiError> {
        let mut st = self.lock();
        st.run(run_id)?;
        let now = st.tick();
        if let Some(r) = st.runs.get_mut(run_id) {
            r.status = RunStatus::ContinuedAsNew;
            r.attempt = None;
            r.completed_at_ms = Some(now);
        }
        let next = st.clone_run(run_id, ("continued_from", run_id));
        st.continued_to.insert(run_id.to_owned(), next.clone());
        // Like the real server (05-11): the predecessor's result is `{"next_run_id"}`.
        if let Some(r) = st.runs.get_mut(run_id) {
            r.result_json = Some(
                serde_json::json!({ "next_run_id": next })
                    .to_string()
                    .into_bytes(),
            );
        }
        drop(st);
        self.changed.notify_waiters();
        Ok(next)
    }

    /// The run `run_id` resolves to: itself, or the last successor of its
    /// continue-as-new chain when `follow`.
    fn resolve(st: &State, run_id: &str, follow: bool) -> Result<String, ApiError> {
        st.run(run_id)?;
        let mut cur = run_id.to_owned();
        if follow {
            // Bounded: the fake never builds cycles, but stay total anyway.
            for _ in 0..st.continued_to.len() {
                match st.continued_to.get(&cur) {
                    Some(next) if st.runs[&cur].status == RunStatus::ContinuedAsNew => {
                        cur = next.clone();
                    }
                    _ => break,
                }
            }
        }
        Ok(cur)
    }
}

fn parse_fake_workflows(wasm: &[u8]) -> Result<Vec<WorkflowRef>, ApiError> {
    let Some(rest) = std::str::from_utf8(wasm)
        .ok()
        .and_then(|s| s.strip_prefix(FAKE_MODULE_PREFIX))
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for item in rest.split(',').filter(|s| !s.is_empty()) {
        let (name, ver) = item
            .rsplit_once('@')
            .ok_or_else(|| ApiError::invalid_argument(format!("bad fake workflow {item:?}")))?;
        let version = ver
            .parse::<u32>()
            .map_err(|_| ApiError::invalid_argument(format!("bad fake version {item:?}")))?;
        out.push(WorkflowRef {
            name: name.to_owned(),
            version,
        });
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn is_lower_hex_sha(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn summary_of(r: &RunInfo) -> RunSummary {
    RunSummary {
        run_id: r.run_id.clone(),
        workflow: r.workflow.clone(),
        version: r.version,
        status: r.status,
        queue: r.queue.clone(),
        created_at_ms: r.created_at_ms,
        tags: r.tags.clone(),
    }
}

#[async_trait::async_trait]
impl ControlPlane for FakeControlPlane {
    async fn upload_module(&self, req: UploadModule) -> Result<UploadModuleResult, ApiError> {
        if !is_lower_hex_sha(&req.sha256) {
            return Err(ApiError::invalid_argument(
                "sha256 must be 64 lowercase hex characters",
            ));
        }
        let actual = sha256_hex(&req.wasm);
        if actual != req.sha256 {
            return Err(ApiError::invalid_argument(format!(
                "sha256 mismatch: path says {}, body hashes to {actual}",
                req.sha256
            ))
            .with_details(serde_json::json!({ "actual_sha256": actual })));
        }
        let mut st = self.lock();
        st.uploads
            .push((req.sha256.clone(), req.tenant.clone(), req.name.clone()));
        st.last_upload_signatures = req.signatures.clone();
        if let Some(existing) = st.modules.get(&req.sha256) {
            return Ok(UploadModuleResult {
                module: existing.clone(),
                created: false,
            });
        }
        let workflows = parse_fake_workflows(&req.wasm)?;
        // Validate all before mutating.
        for wf in &workflows {
            if let Some(info) = st.workflows.get(&wf.name)
                && let Some(v) = info.versions.iter().find(|v| v.version == wf.version)
                && v.module_sha256 != req.sha256
            {
                return Err(ApiError::conflict(format!(
                    "workflow {}@{} is already registered by module {}",
                    wf.name, wf.version, v.module_sha256
                ))
                .with_details(serde_json::json!({
                    "workflow": wf.name,
                    "version": wf.version,
                    "existing_module_sha256": v.module_sha256,
                })));
            }
        }
        let now = st.tick();
        for wf in &workflows {
            let info = st
                .workflows
                .entry(wf.name.clone())
                .or_insert_with(|| WorkflowInfo {
                    name: wf.name.clone(),
                    active_version: None,
                    versions: Vec::new(),
                    record_version: 0,
                });
            info.versions.push(WorkflowVersionInfo {
                version: wf.version,
                module_sha256: req.sha256.clone(),
                default_queue: None,
                run_timeout_ms: None,
            });
            info.versions.sort_by_key(|v| v.version);
            if info.active_version.is_none() {
                info.active_version = Some(wf.version);
            }
            info.record_version += 1;
        }
        let module = ModuleInfo {
            sha256: req.sha256.clone(),
            size_bytes: req.wasm.len() as u64,
            language: req.language,
            workflows,
            uploaded_at_ms: now,
        };
        st.modules.insert(req.sha256, module.clone());
        Ok(UploadModuleResult {
            module,
            created: true,
        })
    }

    async fn get_module(&self, sha256: String) -> Result<ModuleInfo, ApiError> {
        self.lock()
            .modules
            .get(&sha256)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("module {sha256} not found")))
    }

    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        Ok(self.lock().workflows.values().cloned().collect())
    }

    async fn activate_workflow(&self, req: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        let mut st = self.lock();
        let info = st
            .workflows
            .get_mut(&req.name)
            .ok_or_else(|| ApiError::not_found(format!("workflow {} not found", req.name)))?;
        if let Some(expected) = req.expected_version
            && expected != info.record_version
        {
            return Err(ApiError::precondition_failed(format!(
                "workflow {} record version is {}, expected {expected}",
                req.name, info.record_version
            ))
            .with_details(serde_json::json!({ "record_version": info.record_version })));
        }
        if !info.versions.iter().any(|v| v.version == req.version) {
            return Err(ApiError::not_found(format!(
                "workflow {}@{} not found",
                req.name, req.version
            )));
        }
        if info.active_version != Some(req.version) {
            info.active_version = Some(req.version);
            info.record_version += 1;
        }
        Ok(info.clone())
    }

    async fn start_run(&self, req: StartRunRequest) -> Result<StartRunResult, ApiError> {
        if serde_json::from_slice::<serde_json::Value>(&req.input_json).is_err() {
            return Err(ApiError::invalid_argument("input is not valid JSON"));
        }
        let mut st = self.lock();
        let info = st
            .workflows
            .get(&req.workflow)
            .ok_or_else(|| ApiError::not_found(format!("workflow {} not found", req.workflow)))?;
        let version = match req.version.or(info.active_version) {
            Some(v) => v,
            None => {
                return Err(ApiError::precondition_failed(format!(
                    "workflow {} has no active version",
                    req.workflow
                )));
            }
        };
        let ver = info
            .versions
            .iter()
            .find(|v| v.version == version)
            .cloned()
            .ok_or_else(|| {
                ApiError::not_found(format!("workflow {}@{version} not found", req.workflow))
            })?;
        let idem = req
            .idempotency_key
            .clone()
            .map(|k| (req.tenant.clone(), req.workflow.clone(), k));
        if let Some(key) = &idem
            && let Some(existing) = st.idempotency.get(key)
        {
            return match req.conflict_policy {
                ConflictPolicy::ReturnExisting => Ok(StartRunResult {
                    run_id: existing.clone(),
                    created: false,
                    dry_run: req.dry_run,
                }),
                ConflictPolicy::Reject => Err(ApiError::conflict(format!(
                    "idempotency key already used by run {existing}"
                ))
                .with_details(serde_json::json!({ "existing_run_id": existing }))),
            };
        }
        if req.dry_run {
            // Validated; nothing is allocated or stored (no tick).
            return Ok(StartRunResult {
                run_id: String::new(),
                created: true,
                dry_run: true,
            });
        }
        // D9 resolution without a config default: request > workflow > queue policy.
        let queue = req
            .queue
            .clone()
            .or(ver.default_queue.clone())
            .unwrap_or_else(|| "default".to_owned());
        let wp = st.workflow_policies.get(&req.workflow).copied();
        let qp = st.queue_policies.get(&queue).copied();
        let retention_ms = req
            .retention_ms
            .filter(|v| *v > 0)
            .or(wp.and_then(|p| p.retention_ms).filter(|v| *v > 0))
            .or(qp.and_then(|p| p.retention_ms).filter(|v| *v > 0));
        let schedule_to_start_ms = req
            .schedule_to_start_ms
            .or(wp.and_then(|p| p.schedule_to_start_ms))
            .or(qp.and_then(|p| p.schedule_to_start_ms))
            .filter(|v| *v > 0);
        st.charge_rate_limit(&queue)?;
        let now = st.tick();
        st.next_run += 1;
        let run_id = format!("run-{:06}", st.next_run);
        let delayed = req.delay_ms.is_some_and(|d| d > 0);
        let run = RunInfo {
            run_id: run_id.clone(),
            workflow: req.workflow.clone(),
            version,
            module_sha256: ver.module_sha256.clone(),
            queue,
            status: if delayed {
                RunStatus::Scheduled
            } else {
                RunStatus::Ready
            },
            attempt: None,
            tags: req.tags.clone(),
            tenant: req.tenant.clone(),
            traceparent: req.traceparent.clone(),
            priority: req.priority.unwrap_or(0),
            concurrency_key: req.concurrency_key.clone(),
            created_at_ms: now,
            completed_at_ms: None,
            published_status_json: None,
            result_json: None,
            failure: None,
            attempt_failures: 0,
            retention_ms,
            schedule_to_start_ms,
            pause: None,
        };
        let input = String::from_utf8_lossy(&req.input_json).into_owned();
        st.history.insert(
            run_id.clone(),
            vec![HistoryEvent {
                seq: 1,
                kind: "RunStarted".to_owned(),
                ts_ms: now,
                summary: format!("started {}@{version}", req.workflow),
                details_json: format!("{{\"input\":{input}}}").into_bytes(),
                attempt: None,
            }],
        );
        st.runs.insert(run_id.clone(), run);
        if let Some(key) = idem {
            st.idempotency.insert(key, run_id.clone());
        }
        Ok(StartRunResult {
            run_id,
            created: true,
            dry_run: false,
        })
    }

    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        let st = self.lock();
        let mut run = st
            .runs
            .get(&run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found")))?;
        // 07-22: the pause mark (the fake records operator pauses only, without a time).
        run.pause = st
            .paused_runs
            .get(&run_id)
            .map(|detail| crate::phase7::RunPauseMark {
                reason: crate::phase7::RunPauseMark::reason_name(0),
                detail: detail.clone().unwrap_or_default(),
                at_ms: 0,
            });
        Ok(run)
    }

    async fn run_history(&self, req: HistoryRequest) -> Result<HistoryPage, ApiError> {
        let st = self.lock();
        let events = st
            .history
            .get(&req.run_id)
            .ok_or_else(|| ApiError::not_found(format!("run {} not found", req.run_id)))?;
        let limit = effective_limit(req.limit) as usize;
        let mut it = events.iter().filter(|e| e.seq >= req.from_seq);
        let page: Vec<HistoryEvent> = it.by_ref().take(limit).cloned().collect();
        let next_from_seq = it.next().map(|e| e.seq);
        Ok(HistoryPage {
            events: page,
            next_from_seq,
        })
    }

    async fn list_runs(&self, req: ListRunsRequest) -> Result<RunPage, ApiError> {
        let mut st = self.lock();
        st.last_list_runs = Some(req.clone());
        let st = st;
        let limit = effective_limit(req.limit) as usize;
        let after = match &req.page_token {
            Some(t) if !t.is_empty() => {
                if !t.starts_with("run-") {
                    return Err(ApiError::invalid_argument("bad page_token"));
                }
                std::ops::Bound::Excluded(t.clone())
            }
            _ => std::ops::Bound::Unbounded,
        };
        // Phase 11 (D-P11-10): newest first walks the id-ordered map backwards; the
        // page token is the last run id either way.
        let ordered: Box<dyn Iterator<Item = (&String, &RunInfo)> + '_> = match req.order {
            ListOrder::Oldest => Box::new(st.runs.range((after, std::ops::Bound::Unbounded))),
            ListOrder::Newest => Box::new(st.runs.range((std::ops::Bound::Unbounded, after)).rev()),
        };
        let dismissed = &st.dlq_hidden;
        let mut it = ordered
            .map(|(_, r)| r)
            .filter(|r| req.workflow.as_ref().is_none_or(|w| &r.workflow == w))
            .filter(|r| req.status.is_none_or(|s| r.status == s))
            .filter(|r| req.queue.as_ref().is_none_or(|q| &r.queue == q))
            .filter(|r| req.tag.as_ref().is_none_or(|t| r.tags.contains(t)))
            .filter(|r| req.tags.iter().all(|t| r.tags.contains(t)))
            .filter(|r| {
                !req.dlq
                    || (matches!(r.status, RunStatus::Failed | RunStatus::TimedOut)
                        && !dismissed.contains(&r.run_id))
            })
            .filter(|r| req.created_after_ms.is_none_or(|a| r.created_at_ms >= a))
            .filter(|r| req.created_before_ms.is_none_or(|b| r.created_at_ms < b))
            // Phase 14 (14-02): `_` = the default tenant (no tenant on the run).
            .filter(|r| {
                req.tenant.as_ref().is_none_or(|t| {
                    let run = r.tenant.as_deref().filter(|t| !t.is_empty()).unwrap_or("_");
                    run == t
                })
            });
        let runs: Vec<RunSummary> = it.by_ref().take(limit).map(summary_of).collect();
        let next_page_token = if it.next().is_some() {
            runs.last().map(|r| r.run_id.clone())
        } else {
            None
        };
        Ok(RunPage {
            runs,
            next_page_token,
        })
    }

    fn ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    async fn signal_run(&self, r: SignalRequest) -> Result<SignalResponse, ApiError> {
        r.validate()?;
        let mut st = self.lock();
        let run = st.run(&r.run_id)?;
        if run.status.is_terminal() {
            return Err(ApiError::run_terminal(&r.run_id, run.status));
        }
        let seen_key = r.request_id.clone().map(|id| (r.run_id.clone(), id));
        if let Some(k) = &seen_key
            && st.signals_seen.contains(k)
        {
            return Ok(SignalResponse {
                accepted: true,
                duplicate: true,
                dry_run: r.dry_run,
            });
        }
        if !r.dry_run {
            let now = st.tick();
            if let Some(k) = seen_key {
                st.signals_seen.insert(k);
            }
            st.push_event(
                &r.run_id,
                now,
                "SignalPayload",
                serde_json::json!({ "name": r.name, "payload": r.payload }),
            );
            st.signals
                .entry(r.run_id)
                .or_default()
                .push((r.name, r.payload));
        }
        Ok(SignalResponse {
            accepted: true,
            duplicate: false,
            dry_run: r.dry_run,
        })
    }

    async fn cancel_run(&self, r: CancelRequest) -> Result<CancelResponse, ApiError> {
        r.validate()?;
        let mut st = self.lock();
        let run = st.run(&r.run_id)?;
        if run.status.is_terminal() {
            return Err(ApiError::run_terminal(&r.run_id, run.status));
        }
        let already_requested = st.cancel_requested.contains_key(&r.run_id);
        if !already_requested && !r.dry_run {
            let now = st.tick();
            st.push_event(
                &r.run_id,
                now,
                "CancelRequested",
                serde_json::json!({ "reason": r.reason }),
            );
            st.cancel_requested.insert(r.run_id, r.reason);
        }
        Ok(CancelResponse {
            already_requested,
            dry_run: r.dry_run,
        })
    }

    async fn terminate_run(&self, r: TerminateRequest) -> Result<TerminateResponse, ApiError> {
        r.validate()?;
        let mut st = self.lock();
        let run = st.run(&r.run_id)?;
        if run.status.is_terminal() {
            return Err(ApiError::run_terminal(&r.run_id, run.status));
        }
        if !r.dry_run {
            let now = st.tick();
            if let Some(run) = st.runs.get_mut(&r.run_id) {
                run.status = RunStatus::Terminated;
                run.attempt = None;
                run.completed_at_ms = Some(now);
            }
            st.push_event(
                &r.run_id,
                now,
                "RunTerminated",
                serde_json::json!({ "reason": r.reason }),
            );
            drop(st);
            self.changed.notify_waiters();
        }
        Ok(TerminateResponse {
            status: RunStatus::Terminated,
            dry_run: r.dry_run,
        })
    }

    async fn retry_run(&self, r: RetryRequest) -> Result<RetryResponse, ApiError> {
        let mut st = self.lock();
        let status = st.run(&r.run_id)?.status;
        match status {
            s if !s.is_terminal() => return Err(ApiError::run_not_terminal(&r.run_id, s)),
            RunStatus::Completed | RunStatus::ContinuedAsNew => {
                return Err(ApiError::run_completed(&r.run_id));
            }
            _ => {}
        }
        let resumable = st.resumable.get(&r.run_id).copied().unwrap_or(matches!(
            status,
            RunStatus::Terminated | RunStatus::TimedOut
        ));
        let mode = if resumable {
            RetryMode::Resumed
        } else {
            RetryMode::Restarted
        };
        if r.dry_run {
            return Ok(RetryResponse {
                mode,
                run_id: r.run_id,
                dry_run: true,
            });
        }
        let run_id = match mode {
            RetryMode::Resumed => {
                let now = st.tick();
                if let Some(run) = st.runs.get_mut(&r.run_id) {
                    run.status = RunStatus::Ready;
                    run.completed_at_ms = None;
                    run.failure = None;
                    run.result_json = None;
                }
                st.cancel_requested.remove(&r.run_id);
                st.push_event(&r.run_id, now, "RunResumed", serde_json::json!({}));
                r.run_id
            }
            RetryMode::Restarted => st.clone_run(&r.run_id, ("retry_of", &r.run_id)),
        };
        drop(st);
        self.changed.notify_waiters();
        Ok(RetryResponse {
            mode,
            run_id,
            dry_run: false,
        })
    }

    async fn await_result(&self, r: AwaitResultRequest) -> Result<AwaitResultResponse, ApiError> {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(effective_wait_ms(r.wait_ms));
        loop {
            // Subscribe before reading so a change between the read and the wait is
            // never lost.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let st = self.lock();
                let id = Self::resolve(&st, &r.run_id, r.follow_continuations)?;
                let out = st.outcome(&id);
                if out.completed || tokio::time::Instant::now() >= deadline {
                    return Ok(out);
                }
            }
            let _ = tokio::time::timeout_at(deadline, notified).await;
        }
    }

    async fn complete_token(
        &self,
        r: CompleteTokenRequest,
    ) -> Result<CompleteTokenResponse, ApiError> {
        let (run_id, op) = parse_fake_token(&r.token)?;
        let mut st = self.lock();
        let Some(&completed) = st.tokens.get(&(run_id.clone(), op)) else {
            return Err(ApiError::token_invalid("unknown token"));
        };
        // Same order as the real server (05-13): a terminal run first, then a duplicate,
        // whose details carry the run's current status.
        let run = st.run(&run_id)?;
        if run.status.is_terminal() {
            return Err(ApiError::run_terminal(&run_id, run.status));
        }
        if completed {
            let mut e = ApiError::token_already_completed(&run_id);
            if let Some(serde_json::Value::Object(d)) = e.details.as_mut() {
                d.insert("status".into(), run.status.as_str().into());
            }
            return Err(e);
        }
        if !r.dry_run {
            let now = st.tick();
            st.tokens.insert((run_id.clone(), op), true);
            st.push_event(
                &run_id,
                now,
                "TokenCompleted",
                serde_json::json!({ "op": op, "payload": r.payload }),
            );
        }
        Ok(CompleteTokenResponse {
            run_id,
            accepted: true,
            dry_run: r.dry_run,
        })
    }

    async fn set_workflow_policy(
        &self,
        workflow: String,
        p: RunPolicy,
    ) -> Result<RunPolicy, ApiError> {
        let mut st = self.lock();
        if !st.workflows.contains_key(&workflow) {
            return Err(ApiError::not_found(format!(
                "workflow {workflow} not found"
            )));
        }
        st.tick();
        st.workflow_policies.insert(workflow, p);
        Ok(p)
    }

    async fn get_workflow_policy(&self, workflow: String) -> Result<RunPolicy, ApiError> {
        let st = self.lock();
        if !st.workflows.contains_key(&workflow) {
            return Err(ApiError::not_found(format!(
                "workflow {workflow} not found"
            )));
        }
        Ok(st
            .workflow_policies
            .get(&workflow)
            .copied()
            .unwrap_or_default())
    }

    async fn set_queue_policy(&self, queue: String, p: RunPolicy) -> Result<RunPolicy, ApiError> {
        if queue.is_empty() {
            return Err(ApiError::invalid_argument("queue name must not be empty"));
        }
        let mut st = self.lock();
        st.tick();
        st.queue_policies.insert(queue, p);
        Ok(p)
    }

    async fn get_queue_policy(&self, queue: String) -> Result<RunPolicy, ApiError> {
        if queue.is_empty() {
            return Err(ApiError::invalid_argument("queue name must not be empty"));
        }
        Ok(self
            .lock()
            .queue_policies
            .get(&queue)
            .copied()
            .unwrap_or_default())
    }

    // --- Phase 6 bridge administration (06-08).

    async fn put_secret(&self, req: PutSecret) -> Result<SecretInfo, ApiError> {
        req.validate()?;
        let mut st = self.lock();
        st.tick();
        let info = SecretInfo {
            name: req.name.clone(),
            key_id: FAKE_SECRET_KEY_ID.to_owned(),
            tenant: req.tenant.clone(),
        };
        st.secrets.insert((req.tenant, req.name), req.value);
        Ok(info)
    }

    async fn list_secrets(&self, tenant: Option<String>) -> Result<Vec<SecretInfo>, ApiError> {
        Ok(self
            .lock()
            .secrets
            .keys()
            .filter(|(t, _)| *t == tenant)
            .map(|(t, n)| SecretInfo {
                name: n.clone(),
                key_id: FAKE_SECRET_KEY_ID.to_owned(),
                tenant: t.clone(),
            })
            .collect())
    }

    async fn delete_secret(&self, name: String, tenant: Option<String>) -> Result<(), ApiError> {
        let mut st = self.lock();
        if st.secrets.remove(&(tenant, name.clone())).is_none() {
            return Err(ApiError::not_found(format!("secret {name} not found")));
        }
        st.tick();
        Ok(())
    }

    async fn put_grant(&self, g: GrantInfo) -> Result<GrantInfo, ApiError> {
        g.validate()?;
        let g = GrantInfo {
            id: g.expected_id(),
            ..g
        };
        let mut st = self.lock();
        st.tick();
        st.grants.insert(g.id.clone(), g.clone());
        Ok(g)
    }

    async fn list_grants(&self, module: Option<String>) -> Result<Vec<GrantInfo>, ApiError> {
        Ok(self
            .lock()
            .grants
            .values()
            .filter(|g| module.as_ref().is_none_or(|m| *m == g.module))
            .cloned()
            .collect())
    }

    async fn delete_grant(&self, id: String) -> Result<(), ApiError> {
        let mut st = self.lock();
        if st.grants.remove(&id).is_none() {
            return Err(ApiError::not_found(format!("grant {id} not found")));
        }
        st.tick();
        Ok(())
    }

    async fn put_connection(&self, c: ConnectionInfo) -> Result<ConnectionInfo, ApiError> {
        c.validate()?;
        let mut st = self.lock();
        st.tick();
        st.connections.insert(c.name.clone(), c.clone());
        Ok(c)
    }

    async fn get_connection(&self, name: String) -> Result<ConnectionInfo, ApiError> {
        self.lock()
            .connections
            .get(&name)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("connection {name} not found")))
    }

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>, ApiError> {
        Ok(self.lock().connections.values().cloned().collect())
    }

    async fn delete_connection(&self, name: String) -> Result<(), ApiError> {
        let mut st = self.lock();
        if st.connections.remove(&name).is_none() {
            return Err(ApiError::not_found(format!("connection {name} not found")));
        }
        st.tick();
        Ok(())
    }

    async fn put_egress(&self, e: EgressInfo) -> Result<EgressInfo, ApiError> {
        e.validate()?;
        let mut st = self.lock();
        st.tick();
        st.egress.insert(e.module.clone(), e.clone());
        Ok(e)
    }

    async fn get_egress(&self, module: String) -> Result<EgressInfo, ApiError> {
        self.lock()
            .egress
            .get(&module)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("egress policy {module} not found")))
    }

    async fn list_egress(&self) -> Result<Vec<EgressInfo>, ApiError> {
        Ok(self.lock().egress.values().cloned().collect())
    }

    async fn delete_egress(&self, module: String) -> Result<(), ApiError> {
        let mut st = self.lock();
        if st.egress.remove(&module).is_none() {
            return Err(ApiError::not_found(format!(
                "egress policy {module} not found"
            )));
        }
        st.tick();
        Ok(())
    }

    /// 200 with [`FAKE_OBJECT_BODY`] (GET) or an empty body (PUT) when `sig` is
    /// [`FAKE_PRESIGN_VALID_SIG`]; 403 otherwise. Exercises routing only.
    async fn presigned_object(
        &self,
        req: PresignedObjectRequest,
    ) -> Result<PresignedObjectResponse, ApiError> {
        let (sig, method) = (req.sig.clone(), req.method);
        self.lock().last_presign = Some(req);
        if sig != FAKE_PRESIGN_VALID_SIG {
            return Ok(PresignedObjectResponse {
                status: 403,
                body: b"signature mismatch".to_vec(),
                content_type: Some("text/plain".to_owned()),
            });
        }
        Ok(match method {
            PresignMethod::Get => PresignedObjectResponse {
                status: 200,
                body: FAKE_OBJECT_BODY.to_vec(),
                content_type: Some("application/octet-stream".to_owned()),
            },
            PresignMethod::Put => PresignedObjectResponse {
                status: 200,
                body: Vec::new(),
                content_type: None,
            },
        })
    }

    // --- Phase 7 (07-11).

    async fn get_queue_flow(&self, queue: String) -> Result<QueueFlowInfo, ApiError> {
        crate::phase7::require_name("queue name", &queue)?;
        Ok(self.lock().flow_info(&queue))
    }

    async fn put_queue_flow(
        &self,
        queue: String,
        flow: QueueFlow,
        expected_version: Option<u64>,
    ) -> Result<QueueFlowInfo, ApiError> {
        crate::phase7::require_name("queue name", &queue)?;
        flow.validate()?;
        let mut st = self.lock();
        let current = st.queue_flows.get(&queue).map_or(0, |(v, _)| *v);
        if let Some(expected) = expected_version
            && expected != current
        {
            return Err(ApiError::precondition_failed(format!(
                "queue {queue} flow is at version {current}, not {expected}"
            ))
            .with_details(serde_json::json!({ "current_version": current })));
        }
        st.tick();
        st.queue_flows.insert(queue.clone(), (current + 1, flow));
        Ok(st.flow_info(&queue))
    }

    async fn pause_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        crate::phase7::require_name("queue name", &queue)?;
        Ok(self.lock().set_queue_paused(queue, true))
    }

    async fn resume_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        crate::phase7::require_name("queue name", &queue)?;
        Ok(self.lock().set_queue_paused(queue, false))
    }

    async fn pause_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        let mut st = self.lock();
        if !st.workflows.contains_key(&workflow) {
            return Err(ApiError::not_found(format!(
                "workflow {workflow} not found"
            )));
        }
        let changed = st.paused_workflows.insert(workflow.clone());
        if changed {
            st.tick();
        }
        Ok(PauseState {
            name: workflow,
            paused: true,
            changed,
        })
    }

    async fn resume_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        let mut st = self.lock();
        if !st.workflows.contains_key(&workflow) {
            return Err(ApiError::not_found(format!(
                "workflow {workflow} not found"
            )));
        }
        let changed = st.paused_workflows.remove(&workflow);
        if changed {
            st.tick();
        }
        Ok(PauseState {
            name: workflow,
            paused: false,
            changed,
        })
    }

    async fn pause_run(
        &self,
        run_id: String,
        detail: Option<String>,
    ) -> Result<RunPauseState, ApiError> {
        PauseRunRequest {
            detail: detail.clone(),
        }
        .validate()?;
        let mut st = self.lock();
        let status = st.run(&run_id)?.status;
        if status.is_terminal() {
            return Err(ApiError::run_terminal(&run_id, status));
        }
        let changed = !st.paused_runs.contains_key(&run_id);
        if changed {
            let now = st.tick();
            st.paused_runs.insert(run_id.clone(), detail.clone());
            st.push_event(
                &run_id,
                now,
                "RunPaused",
                serde_json::json!({ "detail": detail }),
            );
        }
        Ok(RunPauseState {
            run_id,
            paused: true,
            changed,
        })
    }

    async fn resume_run(&self, run_id: String) -> Result<RunPauseState, ApiError> {
        let mut st = self.lock();
        let status = st.run(&run_id)?.status;
        if status.is_terminal() {
            return Err(ApiError::run_terminal(&run_id, status));
        }
        let changed = st.paused_runs.remove(&run_id).is_some();
        if changed {
            let now = st.tick();
            st.push_event(&run_id, now, "RunResumed", serde_json::json!({}));
        }
        Ok(RunPauseState {
            run_id,
            paused: false,
            changed,
        })
    }

    /// Demand from the fake's own state: backlog = Ready runs of the queue that are
    /// not paused; `workers` placement demands the backlog, `embedded` and `auto`
    /// (the fake never spills) demand 0. Nothing runs on fake workers.
    async fn queue_demand(&self, queue: String) -> Result<DemandSnapshot, ApiError> {
        crate::phase7::require_name("queue name", &queue)?;
        let st = self.lock();
        let flow = st.flow_info(&queue).flow;
        let placement = flow.placement.unwrap_or(Placement::Embedded);
        let backlog = if flow.paused {
            0
        } else {
            st.runs
                .values()
                .filter(|r| {
                    r.queue == queue
                        && r.status == RunStatus::Ready
                        && !st.paused_runs.contains_key(&r.run_id)
                        && !st.paused_workflows.contains(&r.workflow)
                })
                .count() as u64
        };
        let demand = match placement {
            Placement::Workers => backlog,
            Placement::Embedded | Placement::Auto => 0,
        };
        let live = st.workers.values().filter(|w| !w.draining);
        Ok(DemandSnapshot {
            queue,
            demand,
            backlog,
            running_on_workers: 0,
            placement,
            spilling: false,
            workers: st.workers.len() as u64,
            worker_slots_free: live.map(|w| u64::from(w.free_slots)).sum(),
        })
    }

    async fn list_workers(&self) -> Result<Vec<WorkerInfo>, ApiError> {
        Ok(self.lock().workers.values().cloned().collect())
    }

    async fn drain_worker(&self, worker_id: String) -> Result<DrainResult, ApiError> {
        let mut st = self.lock();
        let w = st
            .workers
            .get_mut(&worker_id)
            .ok_or_else(|| ApiError::not_found(format!("worker {worker_id} not found")))?;
        let already_draining = w.draining;
        w.draining = true;
        if !already_draining {
            st.tick();
        }
        Ok(DrainResult {
            worker_id,
            already_draining,
        })
    }

    /// Token `wkr_<nnnnnn>.<32 hex>` (deterministic); the fake stores the secret in
    /// plaintext (the real server stores its sha256).
    async fn create_worker_token(
        &self,
        req: CreateWorkerTokenRequest,
    ) -> Result<WorkerTokenCreated, ApiError> {
        req.validate()?;
        let mut st = self.lock();
        let now = st.tick();
        st.next_worker_token += 1;
        let token_id = format!("{WORKER_TOKEN_PREFIX}{:06}", st.next_worker_token);
        let secret =
            sha256_hex(format!("fake-worker-token-{token_id}").as_bytes())[..32].to_owned();
        let token = format!("{token_id}.{secret}");
        let expires_at_ms = now.saturating_add(req.ttl_ms.unwrap_or(FAKE_JOIN_TOKEN_TTL_MS));
        st.worker_tokens.insert(
            token_id.clone(),
            (
                WorkerTokenInfo {
                    token_id: token_id.clone(),
                    created_at_ms: now,
                    expires_at_ms: Some(expires_at_ms),
                },
                secret,
            ),
        );
        let join_command = format!(
            "ostro worker --join http://127.0.0.1:7700 --token {token} --ca-hash sha256:{}",
            sha256_hex(FAKE_CA_CERT_PEM.as_bytes())
        );
        Ok(WorkerTokenCreated {
            token_id,
            token,
            join_command,
            expires_at_ms,
        })
    }

    async fn list_worker_tokens(&self) -> Result<Vec<WorkerTokenInfo>, ApiError> {
        Ok(self
            .lock()
            .worker_tokens
            .values()
            .map(|(info, _)| info.clone())
            .collect())
    }

    async fn revoke_worker_token(&self, token_id: String) -> Result<(), ApiError> {
        let mut st = self.lock();
        if st.worker_tokens.remove(&token_id).is_none() {
            return Err(ApiError::not_found(format!(
                "worker token {token_id} not found"
            )));
        }
        st.tick();
        Ok(())
    }

    /// Accepts a live token (fake clock before expiry) and answers fixed PEMs.
    /// Unknown, revoked and expired tokens are one indistinguishable InvalidArgument
    /// (`details.reason = join_token_invalid`); the message never echoes the token.
    async fn join_worker(&self, req: JoinRequest) -> Result<JoinResponse, ApiError> {
        req.validate()?;
        let st = self.lock();
        let invalid = || {
            ApiError::invalid_argument("join token is invalid, revoked or expired")
                .with_details(serde_json::json!({ "reason": REASON_JOIN_TOKEN_INVALID }))
        };
        let (id, secret) = req.token.split_once('.').ok_or_else(invalid)?;
        let (info, stored) = st.worker_tokens.get(id).ok_or_else(invalid)?;
        if stored != secret || info.expires_at_ms.is_some_and(|e| st.now_ms() >= e) {
            return Err(invalid());
        }
        Ok(JoinResponse {
            client_cert_pem: FAKE_CLIENT_CERT_PEM.to_owned(),
            ca_cert_pem: FAKE_CA_CERT_PEM.to_owned(),
            endpoints: vec![FAKE_CLUSTER_ENDPOINT.to_owned()],
            renew_after_ms: FAKE_RENEW_AFTER_MS,
            cluster_id: FAKE_CLUSTER_ID.to_owned(),
        })
    }

    // --- Phase 10 (10-05) ---

    /// The fake clock plus the skipped offset; always idle, nothing due.
    async fn dev_time(&self) -> Result<DevTimeInfo, ApiError> {
        let mode = self.dev_mode_on()?;
        let offset_ms = self.dev_offset_ms.load(Ordering::SeqCst);
        Ok(DevTimeInfo {
            now_ms: self.lock().now_ms().saturating_add(offset_ms),
            offset_ms,
            mode,
            idle: true,
            next_due_ms: None,
        })
    }

    /// Grows the offset (a target in the past skips 0). Does not tick the fake clock.
    async fn dev_skip(&self, s: DevSkip) -> Result<DevSkipResult, ApiError> {
        // The mode lock serialises skips, so the read-modify-write of the offset is atomic.
        let mode = self.dev_mode.lock().unwrap_or_else(|p| p.into_inner());
        if *mode == DevTimeMode::Off {
            return Err(time_skipping_disabled());
        }
        let base = self.lock().now_ms();
        let prev = self.dev_offset_ms.load(Ordering::SeqCst);
        let skipped_ms = match s {
            DevSkip::By { ms } => ms,
            DevSkip::To { ms } => ms.saturating_sub(base.saturating_add(prev)),
        };
        let offset_ms = prev.saturating_add(skipped_ms);
        self.dev_offset_ms.store(offset_ms, Ordering::SeqCst);
        drop(mode);
        Ok(DevSkipResult {
            now_ms: base.saturating_add(offset_ms),
            offset_ms,
            skipped_ms,
        })
    }

    /// `ok` for every selected run whose workflow the module exports, `skipped`
    /// otherwise; unknown module or run = NotFound. A sample takes the newest `n` runs
    /// per exported workflow.
    async fn replay_module(&self, r: ReplayRequest) -> Result<ReplayResult, ApiError> {
        r.validate()?;
        let st = self.lock();
        let module = st
            .modules
            .get(&r.module_sha256)
            .ok_or_else(|| ApiError::not_found(format!("module {} not found", r.module_sha256)))?;
        let exported: BTreeSet<&str> = module.workflows.iter().map(|w| w.name.as_str()).collect();
        let runs: Vec<&RunInfo> = match &r.select {
            ReplaySelect::Runs(ids) => ids
                .iter()
                .map(|id| {
                    st.runs
                        .get(id)
                        .ok_or_else(|| ApiError::not_found(format!("run {id} not found")))
                })
                .collect::<Result<_, _>>()?,
            ReplaySelect::Sample {
                n,
                workflow,
                status,
            } => {
                let status = status.as_deref().and_then(RunStatus::parse);
                let mut newest: Vec<&RunInfo> = st
                    .runs
                    .values()
                    .filter(|run| exported.contains(run.workflow.as_str()))
                    .filter(|run| workflow.as_ref().is_none_or(|w| *w == run.workflow))
                    .filter(|run| status.is_none_or(|s| s == run.status))
                    .collect();
                newest.sort_by(|a, b| {
                    (
                        a.workflow.as_str(),
                        std::cmp::Reverse((a.created_at_ms, &a.run_id)),
                    )
                        .cmp(&(
                            b.workflow.as_str(),
                            std::cmp::Reverse((b.created_at_ms, &b.run_id)),
                        ))
                });
                let mut taken: BTreeMap<&str, u32> = BTreeMap::new();
                newest.retain(|run| {
                    let c = taken.entry(run.workflow.as_str()).or_default();
                    *c += 1;
                    *c <= *n
                });
                newest
            }
        };
        let verdicts: Vec<ReplayVerdictInfo> = runs
            .into_iter()
            .map(|run| {
                let known = exported.contains(run.workflow.as_str());
                ReplayVerdictInfo {
                    run_id: run.run_id.clone(),
                    workflow: run.workflow.clone(),
                    verdict: if known {
                        ReplayVerdictKind::Ok
                    } else {
                        ReplayVerdictKind::Skipped
                    },
                    seq: None,
                    detail: (!known).then(|| {
                        format!("workflow {} is not exported by the module", run.workflow)
                    }),
                    appended: 0,
                }
            })
            .collect();
        Ok(ReplayResult {
            module_sha256: r.module_sha256,
            summary: ReplaySummary::of(&verdicts),
            verdicts,
        })
    }
}

// ---------------------------------------------------------------------------------
// Phase 11 (11-09): in-memory OpsApi / InventoryApi
// ---------------------------------------------------------------------------------

pub use p11::{FAKE_MAX_BULK_JOBS, FakeInventory, FakeOps};

mod p11 {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, MutexGuard};

    use crate::error::ApiError;
    use crate::phase8::ApiExtensions;
    use crate::phase11::*;

    use super::{FAKE_EPOCH_MS, FAKE_TICK_MS};

    /// Bulk jobs the fake runs at once before answering `bulk_too_many_jobs`.
    pub const FAKE_MAX_BULK_JOBS: usize = 4;

    #[derive(Default)]
    struct OpsState {
        clock: u64,
        next_run: u64,
        next_job: u64,
        errors: BTreeMap<String, ApiError>,
        rerun_keys: BTreeMap<String, RerunResponse>,
        cuts: BTreeMap<String, RerunCutsView>,
        dlq: BTreeMap<String, DlqEntry>,
        bulk_matches: Vec<String>,
        jobs: BTreeMap<String, BulkJobView>,
        policies: BTreeMap<String, FailurePolicy>,
        reruns: Vec<RerunRequest>,
        bulk_requests: Vec<BulkCreateRequest>,
    }

    impl OpsState {
        fn tick(&mut self) -> u64 {
            self.clock += 1;
            FAKE_EPOCH_MS + self.clock * FAKE_TICK_MS
        }
    }

    /// In-memory [`OpsApi`] for adapter, UI and CLI tests. Deterministic: new run ids
    /// are `run-rerun-<n>` / `run-requeue-<n>`, job ids `blk_<n>` (16 hex digits),
    /// times come from a fake clock starting at [`FAKE_EPOCH_MS`].
    ///
    /// - Rerun: any non-empty run id is a terminal source; a dry run returns the cut and
    ///   no run id; an idempotency key seen before returns the same run with
    ///   `existing = true`. `cut_seq` is the `seq` cut, or `occurrence` for a step cut.
    /// - Cuts: seeded with [`FakeOps::set_cuts`]; unknown runs are 404.
    /// - DLQ: seeded with [`FakeOps::push_dlq`]; ordered by run id; the page token is the
    ///   last run id of the page. Dismiss and requeue on a non-member are 409
    ///   `dlq_not_member`; `auto` resolves to `resume` when the entry is resumable, else
    ///   `restart`.
    /// - Bulk: the matched set is [`FakeOps::set_bulk_matches`]; jobs start `running` with
    ///   no progress ([`FakeOps::set_bulk_progress`] advances them); more than
    ///   [`FAKE_MAX_BULK_JOBS`] unfinished jobs is 429 `bulk_too_many_jobs`.
    /// - Limits: the D-P11-13 defaults.
    /// - Failure policies: stored per scope with a `version_tag` starting at 1; a stale tag
    ///   is 409 `version_mismatch`.
    /// - [`FakeOps::fail`] makes a method (by trait method name) answer an error until
    ///   cleared.
    #[derive(Default)]
    pub struct FakeOps {
        state: Mutex<OpsState>,
    }

    impl FakeOps {
        /// Empty fake.
        pub fn new() -> Arc<Self> {
            Arc::new(FakeOps::default())
        }

        fn lock(&self) -> MutexGuard<'_, OpsState> {
            self.state.lock().unwrap_or_else(|p| p.into_inner())
        }

        fn check(&self, method: &str) -> Result<MutexGuard<'_, OpsState>, ApiError> {
            let st = self.lock();
            match st.errors.get(method) {
                Some(e) => Err(e.clone()),
                None => Ok(st),
            }
        }

        /// Make `method` (an [`OpsApi`] method name) answer `err` (`None` clears it).
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            let mut st = self.lock();
            match err {
                Some(e) => st.errors.insert(method.to_owned(), e),
                None => st.errors.remove(method),
            };
        }

        /// Seed the cuts of a run.
        pub fn set_cuts(&self, view: RerunCutsView) {
            self.lock().cuts.insert(view.run_id.clone(), view);
        }

        /// Add (or replace) a DLQ entry.
        pub fn push_dlq(&self, entry: DlqEntry) {
            self.lock().dlq.insert(entry.run_id.clone(), entry);
        }

        /// The current DLQ entry of `run_id` (dismissed / requeued included).
        pub fn dlq_entry(&self, run_id: &str) -> Option<DlqEntry> {
            self.lock().dlq.get(run_id).cloned()
        }

        /// The runs a bulk filter matches (the fake ignores the filter).
        pub fn set_bulk_matches(&self, runs: Vec<String>) {
            self.lock().bulk_matches = runs;
        }

        /// Set a job's progress; `done + skipped + failed_count >= matched` finishes it.
        pub fn set_bulk_progress(&self, job_id: &str, done: u64, skipped: u64) {
            let mut st = self.lock();
            let now = st.tick();
            if let Some(j) = st.jobs.get_mut(job_id) {
                j.done = done;
                j.skipped = skipped;
                if j.state == "running" && done + skipped + j.failed_count >= j.matched {
                    j.state = "done".to_owned();
                    j.finished_at_ms = Some(now);
                    j.eta_ms = None;
                }
            }
        }

        /// Every rerun request received (dry runs included), in order.
        pub fn reruns(&self) -> Vec<RerunRequest> {
            self.lock().reruns.clone()
        }

        /// Every bulk create request received, in order.
        pub fn bulk_requests(&self) -> Vec<BulkCreateRequest> {
            self.lock().bulk_requests.clone()
        }

        /// Fill `ext.ops` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.ops = Some(Arc::clone(self) as Arc<dyn OpsApi>);
        }
    }

    fn cut_seq(from: &RerunFrom) -> u64 {
        match from {
            RerunFrom::Seq(s) => *s,
            RerunFrom::Step { occurrence, .. } => u64::from(*occurrence),
        }
    }

    fn action_name(a: &BulkAction) -> &'static str {
        match a {
            BulkAction::Cancel => "cancel",
            BulkAction::Terminate { .. } => "terminate",
            BulkAction::Retry => "retry",
            BulkAction::Rerun { .. } => "rerun",
            BulkAction::Requeue { .. } => "requeue",
            BulkAction::Pause => "pause",
            BulkAction::Resume => "resume",
            BulkAction::Signal { .. } => "signal",
        }
    }

    fn scope_key(s: &PolicyScope) -> String {
        match s {
            PolicyScope::Workflow(n) => format!("workflow/{n}"),
            PolicyScope::Queue(n) => format!("queue/{n}"),
        }
    }

    fn is_member(e: &DlqEntry) -> bool {
        e.dismissed_at_ms.is_none() && e.requeued_to.is_none()
    }

    #[async_trait::async_trait]
    impl OpsApi for FakeOps {
        async fn rerun_run(&self, req: RerunRequest) -> Result<RerunResponse, ApiError> {
            let mut st = self.check("rerun_run")?;
            st.reruns.push(req.clone());
            if let Some(k) = &req.idempotency_key
                && let Some(prev) = st.rerun_keys.get(k)
            {
                return Ok(RerunResponse {
                    existing: true,
                    ..prev.clone()
                });
            }
            let version = match req.version {
                RerunVersion::Version(v) => v,
                RerunVersion::Active | RerunVersion::Source => 1,
            };
            let cut = cut_seq(&req.from);
            let mut res = RerunResponse {
                new_run_id: None,
                source_run_id: req.run_id.clone(),
                cut_seq: cut,
                copied_events: cut.saturating_sub(1),
                workflow: "fake".to_owned(),
                version,
                verdict: "ok".to_owned(),
                existing: false,
            };
            if req.dry_run {
                return Ok(res);
            }
            st.tick();
            st.next_run += 1;
            res.new_run_id = Some(format!("run-rerun-{:06}", st.next_run));
            if let Some(k) = req.idempotency_key {
                st.rerun_keys.insert(k, res.clone());
            }
            Ok(res)
        }

        async fn analyze_rerun(&self, run_id: String) -> Result<RerunCutsView, ApiError> {
            let st = self.check("analyze_rerun")?;
            st.cuts
                .get(&run_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found")))
        }

        async fn list_dlq(&self, req: DlqListRequest) -> Result<DlqPage, ApiError> {
            let st = self.check("list_dlq")?;
            let limit = if req.limit == 0 {
                DEFAULT_OPS_PAGE
            } else {
                req.limit.min(crate::domain::MAX_PAGE_LIMIT)
            } as usize;
            let after = req.page_token.clone().unwrap_or_default();
            let mut it = st
                .dlq
                .values()
                .filter(|e| e.run_id > after)
                .filter(|e| req.include_dismissed || is_member(e))
                .filter(|e| req.workflow.as_ref().is_none_or(|w| &e.workflow == w))
                .filter(|e| req.queue.as_ref().is_none_or(|q| &e.queue == q))
                .filter(|e| req.reason.as_ref().is_none_or(|r| e.reason.starts_with(r)))
                .filter(|e| req.tags.iter().all(|t| e.tags.contains(t)));
            let entries: Vec<DlqEntry> = it.by_ref().take(limit).cloned().collect();
            let next_page_token = if it.next().is_some() {
                entries.last().map(|e| e.run_id.clone())
            } else {
                None
            };
            Ok(DlqPage {
                entries,
                next_page_token,
            })
        }

        async fn requeue_dlq(
            &self,
            req: DlqRequeueRequest,
        ) -> Result<DlqRequeueResponse, ApiError> {
            let mut st = self.check("requeue_dlq")?;
            let entry = st
                .dlq
                .get(&req.run_id)
                .filter(|e| is_member(e))
                .cloned()
                .ok_or_else(|| dlq_not_member(&req.run_id))?;
            let mode = match &req.mode {
                RequeueMode::Auto if entry.resumable => "resume",
                RequeueMode::Auto | RequeueMode::Restart => "restart",
                RequeueMode::Resume => "resume",
                RequeueMode::Rerun { .. } => "rerun",
            };
            if req.dry_run {
                return Ok(DlqRequeueResponse {
                    run_id: req.run_id,
                    mode: mode.to_owned(),
                    new_run_id: None,
                });
            }
            let now = st.tick();
            let new_run_id = if mode == "resume" {
                None
            } else {
                st.next_run += 1;
                Some(format!("run-requeue-{:06}", st.next_run))
            };
            if let Some(e) = st.dlq.get_mut(&req.run_id) {
                e.requeued_to = Some(new_run_id.clone().unwrap_or_else(|| req.run_id.clone()));
                e.requeued_at_ms = Some(now);
            }
            Ok(DlqRequeueResponse {
                run_id: req.run_id,
                mode: mode.to_owned(),
                new_run_id,
            })
        }

        async fn dismiss_dlq(&self, req: DlqDismissRequest) -> Result<DlqEntry, ApiError> {
            let mut st = self.check("dismiss_dlq")?;
            if !st.dlq.get(&req.run_id).is_some_and(is_member) {
                return Err(dlq_not_member(&req.run_id));
            }
            let now = st.tick();
            let e = st
                .dlq
                .get_mut(&req.run_id)
                .ok_or_else(|| dlq_not_member(&req.run_id))?;
            e.dismissed_at_ms = Some(now);
            e.dismiss_reason = Some(req.reason);
            Ok(e.clone())
        }

        async fn create_bulk(
            &self,
            req: BulkCreateRequest,
        ) -> Result<BulkCreateResponse, ApiError> {
            let mut st = self.check("create_bulk")?;
            st.bulk_requests.push(req.clone());
            let matched = st.bulk_matches.len() as u64;
            let sample: Vec<String> = st.bulk_matches.iter().take(20).cloned().collect();
            if req.dry_run {
                return Ok(BulkCreateResponse {
                    job_id: None,
                    matched_estimate: matched,
                    sample,
                });
            }
            let running = st
                .jobs
                .values()
                .filter(|j| matches!(j.state.as_str(), "pending" | "running"))
                .count();
            if running >= FAKE_MAX_BULK_JOBS {
                return Err(bulk_too_many_jobs(FAKE_MAX_BULK_JOBS as u64, 1_000));
            }
            let now = st.tick();
            st.next_job += 1;
            let job_id = format!("blk_{:016x}", st.next_job);
            let rate = req.rate_per_sec.unwrap_or(20).min(500);
            st.jobs.insert(
                job_id.clone(),
                BulkJobView {
                    job_id: job_id.clone(),
                    state: "running".to_owned(),
                    action: action_name(&req.action).to_owned(),
                    matched,
                    done: 0,
                    skipped: 0,
                    failed: Vec::new(),
                    failed_count: 0,
                    rate_per_sec: rate,
                    created_at_ms: now,
                    started_at_ms: Some(now),
                    finished_at_ms: None,
                    eta_ms: Some(matched * 1_000 / u64::from(rate.max(1))),
                },
            );
            Ok(BulkCreateResponse {
                job_id: Some(job_id),
                matched_estimate: matched,
                sample,
            })
        }

        async fn list_bulk(&self) -> Result<Vec<BulkJobView>, ApiError> {
            let st = self.check("list_bulk")?;
            Ok(st.jobs.values().cloned().collect())
        }

        async fn get_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
            let st = self.check("get_bulk")?;
            st.jobs
                .get(&job_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found(format!("bulk job {job_id} not found")))
        }

        async fn cancel_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
            let mut st = self.check("cancel_bulk")?;
            let now = st.tick();
            let j = st
                .jobs
                .get_mut(&job_id)
                .ok_or_else(|| ApiError::not_found(format!("bulk job {job_id} not found")))?;
            if matches!(
                j.state.as_str(),
                "pending" | "running" | "paused_backpressure"
            ) {
                j.state = "cancelled".to_owned();
                j.finished_at_ms = Some(now);
                j.eta_ms = None;
            }
            Ok(j.clone())
        }

        async fn limits(&self) -> Result<LimitsView, ApiError> {
            let _st = self.check("limits")?;
            let l = |name: &str, value: u64, unit: &str, enforced: &str, error: &str| LimitView {
                name: name.to_owned(),
                value,
                unit: unit.to_owned(),
                configurable: true,
                enforced: enforced.to_owned(),
                error: error.to_owned(),
            };
            Ok(LimitsView {
                limits: vec![
                    l(
                        "max_history_events",
                        50_000,
                        "events",
                        "engine",
                        "limit_exceeded",
                    ),
                    l(
                        "max_children_per_run",
                        10_000,
                        "children",
                        "engine",
                        "limit_exceeded",
                    ),
                    l(
                        "max_payload_bytes",
                        4 * 1024 * 1024,
                        "bytes",
                        "api",
                        "payload_too_large",
                    ),
                ],
            })
        }

        async fn get_failure_policy(&self, scope: PolicyScope) -> Result<FailurePolicy, ApiError> {
            let st = self.check("get_failure_policy")?;
            Ok(st
                .policies
                .get(&scope_key(&scope))
                .cloned()
                .unwrap_or_default())
        }

        async fn set_failure_policy(
            &self,
            scope: PolicyScope,
            p: FailurePolicy,
        ) -> Result<FailurePolicy, ApiError> {
            let mut st = self.check("set_failure_policy")?;
            let key = scope_key(&scope);
            let current = st
                .policies
                .get(&key)
                .and_then(|p| p.version_tag)
                .unwrap_or(0);
            if let Some(t) = p.version_tag
                && t != current
            {
                return Err(crate::phase8::version_mismatch(
                    "failure policy",
                    &key,
                    current,
                ));
            }
            let stored = FailurePolicy {
                version_tag: Some(current + 1),
                ..p
            };
            st.policies.insert(key, stored.clone());
            Ok(stored)
        }
    }

    #[derive(Default)]
    struct InvState {
        errors: BTreeMap<String, ApiError>,
        modules: Vec<ModuleSummary>,
        queues: Vec<QueueView>,
        partitions: Vec<PartitionView>,
    }

    /// In-memory [`InventoryApi`]: returns what [`FakeInventory::set_modules`],
    /// [`FakeInventory::set_queues`] and [`FakeInventory::set_partitions`] seeded.
    /// Modules are ordered by sha256; the page token is the last sha of the page.
    #[derive(Default)]
    pub struct FakeInventory {
        state: Mutex<InvState>,
    }

    impl FakeInventory {
        /// Empty fake.
        pub fn new() -> Arc<Self> {
            Arc::new(FakeInventory::default())
        }

        fn lock(&self) -> MutexGuard<'_, InvState> {
            self.state.lock().unwrap_or_else(|p| p.into_inner())
        }

        fn check(&self, method: &str) -> Result<MutexGuard<'_, InvState>, ApiError> {
            let st = self.lock();
            match st.errors.get(method) {
                Some(e) => Err(e.clone()),
                None => Ok(st),
            }
        }

        /// Make `method` answer `err` (`None` clears it).
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            let mut st = self.lock();
            match err {
                Some(e) => st.errors.insert(method.to_owned(), e),
                None => st.errors.remove(method),
            };
        }

        /// Seed modules.
        pub fn set_modules(&self, mut m: Vec<ModuleSummary>) {
            m.sort_by(|a, b| a.sha256.cmp(&b.sha256));
            self.lock().modules = m;
        }

        /// Seed queues.
        pub fn set_queues(&self, q: Vec<QueueView>) {
            self.lock().queues = q;
        }

        /// Seed partitions.
        pub fn set_partitions(&self, p: Vec<PartitionView>) {
            self.lock().partitions = p;
        }

        /// Fill `ext.inventory` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.inventory = Some(Arc::clone(self) as Arc<dyn InventoryApi>);
        }
    }

    #[async_trait::async_trait]
    impl InventoryApi for FakeInventory {
        async fn list_modules(&self, req: ListModulesRequest) -> Result<ModulePage, ApiError> {
            let st = self.check("list_modules")?;
            let limit = if req.limit == 0 {
                DEFAULT_OPS_PAGE
            } else {
                req.limit.min(crate::domain::MAX_PAGE_LIMIT)
            } as usize;
            let after = req.page_token.unwrap_or_default();
            let mut it = st.modules.iter().filter(|m| m.sha256 > after);
            let modules: Vec<ModuleSummary> = it.by_ref().take(limit).cloned().collect();
            let next_page_token = if it.next().is_some() {
                modules.last().map(|m| m.sha256.clone())
            } else {
                None
            };
            Ok(ModulePage {
                modules,
                next_page_token,
            })
        }

        async fn list_queues(&self) -> Result<Vec<QueueView>, ApiError> {
            Ok(self.check("list_queues")?.queues.clone())
        }

        async fn list_partitions(&self) -> Result<Vec<PartitionView>, ApiError> {
            Ok(self.check("list_partitions")?.partitions.clone())
        }
    }
}

/// In-memory Phase 13 traits (cluster membership / upgrades, module lifecycle), 13-03.
pub use p13::{FAKE_P13_CLUSTER_ID, FakeCluster, FakeMigrateOutcome, FakeModuleLifecycle};

mod p13 {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex, MutexGuard};

    use crate::error::{ApiError, ErrorCode};
    use crate::phase8::ApiExtensions;
    use crate::phase13::*;

    use super::{FAKE_EPOCH_MS, FAKE_TICK_MS};

    /// The fake cluster id of [`FakeCluster`].
    pub const FAKE_P13_CLUSTER_ID: &str = "fake-cluster";
    /// Default node-token lifetime of the fake (seconds).
    const DEFAULT_TTL_SECS: u64 = 3600;
    /// The fake cluster CA pin.
    const FAKE_CA_HASH: &str =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    fn reason(code: ErrorCode, msg: String, reason: &str) -> ApiError {
        ApiError::new(code, msg).with_details(serde_json::json!({ "reason": reason }))
    }

    #[derive(Default)]
    struct ClusterState {
        clock: u64,
        initialized: bool,
        members: BTreeMap<u64, ClusterMember>,
        voters: BTreeSet<u64>,
        learners: BTreeSet<u64>,
        next_node: u64,
        next_token: u64,
        tokens: BTreeMap<String, u64>,
        finalized: u32,
        hold: Option<u32>,
        reassignments: BTreeMap<u16, ReassignmentDto>,
        policy: Option<ReplicationPolicyDto>,
        errors: BTreeMap<String, ApiError>,
    }

    impl ClusterState {
        fn tick(&mut self) -> u64 {
            self.clock += 1;
            FAKE_EPOCH_MS + self.clock * FAKE_TICK_MS
        }

        fn version(level_max: u32) -> NodeVersionDto {
            NodeVersionDto {
                ostro_version: "0.1.0".into(),
                level_min: 1,
                level_max,
                wasmtime: "50.0.2".into(),
                event_codec_ver: 1,
                wit_lines: vec!["0.1".into()],
            }
        }

        fn levels(&self) -> (u32, u32) {
            let live = self.members.values().filter(|m| m.state != "removed");
            let (mut lo, mut hi) = (u32::MAX, u32::MAX);
            for m in live {
                if let Some(v) = &m.version {
                    lo = lo.min(v.level_min);
                    hi = hi.min(v.level_max);
                }
            }
            if lo == u32::MAX { (1, 1) } else { (lo, hi) }
        }

        fn role(&self, id: u64) -> &'static str {
            if self.voters.contains(&id) {
                "voter"
            } else if self.learners.contains(&id) {
                "learner"
            } else {
                "none"
            }
        }

        fn status(&self) -> ClusterStatus {
            let (level_min, level_max) = self.levels();
            ClusterStatus {
                cluster_id: FAKE_P13_CLUSTER_ID.into(),
                members: self
                    .members
                    .values()
                    .map(|m| ClusterMember {
                        raft_role: self.role(m.node_id).into(),
                        ..m.clone()
                    })
                    .collect(),
                voters: self.voters.iter().copied().collect(),
                learners: self.learners.iter().copied().collect(),
                voters_outgoing: vec![],
                meta_leader: self.voters.iter().next().copied().unwrap_or(0),
                finalized: self.finalized,
                hold: self.hold,
                level_min,
                level_max,
                policy: self.policy.clone(),
                reassignments: self.reassignments.values().cloned().collect(),
            }
        }

        fn upgrade(&self) -> UpgradeStatus {
            let (_, common) = self.levels();
            UpgradeStatus {
                finalized: self.finalized,
                hold: self.hold,
                common_level_max: common,
                auto_finalize: false,
                members: self
                    .members
                    .values()
                    .filter(|m| m.state != "removed")
                    .map(|m| MemberLevel {
                        node_id: m.node_id,
                        level_min: m.version.as_ref().map_or(1, |v| v.level_min),
                        level_max: m.version.as_ref().map_or(1, |v| v.level_max),
                        live: m.live,
                    })
                    .collect(),
            }
        }

        fn member(&self, id: u64) -> Result<&ClusterMember, ApiError> {
            self.members
                .get(&id)
                .filter(|m| m.state != "removed")
                .ok_or_else(|| ApiError::not_found(format!("node {id} is not a member")))
        }

        fn require_cluster(&self) -> Result<(), ApiError> {
            if self.initialized {
                Ok(())
            } else {
                Err(ApiError::precondition_failed(
                    "this node is not a cluster yet (POST /v1/cluster/init)",
                ))
            }
        }
    }

    /// In-memory [`ClusterApi`] for adapter, CLI and parity tests. Deterministic: node ids
    /// count from 2 after `init` (node 1), token ids are `njt_<n>` with secret
    /// `njt_<n>.fake-secret-<n>`, times come from a fake clock starting at
    /// [`FAKE_EPOCH_MS`].
    ///
    /// - `init` makes node 1 the only voter (`rf`/`min_insync` default 3/2, max voters 5);
    ///   a second `init` answers the status unchanged (idempotent, as the server). Everything except `status`/`upgrade_status` before `init`
    ///   is 412 `precondition_failed`.
    /// - `join` needs a token from `create_node_token` (else 400 `join_token_invalid`,
    ///   tokens are single-use); the joiner becomes a `joining` learner whose binary is
    ///   the request's `node_version`. `resume_node_id` of a joining member returns it again.
    /// - `remove` refuses (409 `quorum`) to remove the last voter, unless it is not a voter;
    ///   removed members stay as `removed` tombstones. `drain` marks a member `draining`.
    /// - `reassign` records an in-flight reassignment (409 `reassignment_in_progress` if one
    ///   exists for the partition); `cancel` drops it (404 if none). `rebalance` is a no-op.
    /// - Upgrades: `finalize` raises `finalized` to the lowest member `level_max` (409
    ///   `held` while held); `hold` holds the current level; `release` clears it.
    /// - [`FakeCluster::fail`] makes a method (by trait method name) answer an error.
    #[derive(Default)]
    pub struct FakeCluster {
        state: Mutex<ClusterState>,
    }

    impl FakeCluster {
        /// A single node, not yet a cluster.
        pub fn new() -> Arc<Self> {
            let fake = FakeCluster::default();
            {
                let mut st = fake.lock();
                st.finalized = 1;
                st.next_node = 2;
                st.members.insert(
                    1,
                    ClusterMember {
                        node_id: 1,
                        addr: "127.0.0.1:7070".into(),
                        cluster_addr: None,
                        state: "active".into(),
                        version: Some(ClusterState::version(2)),
                        live: true,
                        raft_role: "none".into(),
                    },
                );
            }
            Arc::new(fake)
        }

        fn lock(&self) -> MutexGuard<'_, ClusterState> {
            self.state.lock().unwrap_or_else(|p| p.into_inner())
        }

        fn check(&self, method: &str) -> Result<MutexGuard<'_, ClusterState>, ApiError> {
            let st = self.lock();
            match st.errors.get(method) {
                Some(e) => Err(e.clone()),
                None => Ok(st),
            }
        }

        /// Make `method` (a [`ClusterApi`] method name) answer `err` (`None` clears it).
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            let mut st = self.lock();
            match err {
                Some(e) => st.errors.insert(method.to_owned(), e),
                None => st.errors.remove(method),
            };
        }

        /// Mark a member live / dead.
        pub fn set_live(&self, id: u64, live: bool) {
            if let Some(m) = self.lock().members.get_mut(&id) {
                m.live = live;
            }
        }

        /// Promote a joining learner to an active voter (what the reconciler does).
        pub fn promote(&self, id: u64) {
            let mut st = self.lock();
            if let Some(m) = st.members.get_mut(&id) {
                m.state = "active".into();
                st.learners.remove(&id);
                st.voters.insert(id);
            }
        }

        /// Fill `ext.cluster` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.cluster = Some(Arc::clone(self) as Arc<dyn ClusterApi>);
        }
    }

    #[async_trait::async_trait]
    impl ClusterApi for FakeCluster {
        async fn init(&self, r: InitRequest) -> Result<ClusterStatus, ApiError> {
            let mut st = self.check("init")?;
            if st.initialized {
                // Idempotent like the server (13-16): the current status, nothing changed.
                return Ok(st.status());
            }
            st.tick();
            st.initialized = true;
            st.voters.insert(1);
            if let Some(m) = st.members.get_mut(&1) {
                m.cluster_addr = Some(r.advertise.unwrap_or_else(|| "127.0.0.1:7071".into()));
            }
            st.policy = Some(ReplicationPolicyDto {
                rf: r.rf.unwrap_or(3),
                min_insync: r.min_insync.unwrap_or(2),
                max_voters: 5,
            });
            Ok(st.status())
        }

        async fn join(&self, r: JoinRequest) -> Result<JoinResponse, ApiError> {
            let mut st = self.check("join")?;
            st.require_cluster()?;
            let resumed = r
                .resume_node_id
                .filter(|id| st.members.get(id).is_some_and(|m| m.state == "joining"));
            let id = match resumed {
                Some(id) => id,
                None => {
                    if st.tokens.remove(&r.token).is_none() {
                        return Err(reason(
                            ErrorCode::InvalidArgument,
                            "invalid node join token".into(),
                            "join_token_invalid",
                        ));
                    }
                    st.tick();
                    let id = st.next_node;
                    st.next_node += 1;
                    st.members.insert(
                        id,
                        ClusterMember {
                            node_id: id,
                            addr: r.advertise.api.clone(),
                            cluster_addr: Some(r.advertise.cluster.clone()),
                            state: "joining".into(),
                            version: Some(r.node_version.clone()),
                            live: true,
                            raft_role: "learner".into(),
                        },
                    );
                    st.learners.insert(id);
                    id
                }
            };
            let seeds = st
                .members
                .values()
                .filter(|m| m.node_id != id && m.state != "removed")
                .map(|m| SeedDto {
                    id: m.node_id,
                    addr: m.cluster_addr.clone().unwrap_or_default(),
                    repl_addr: None,
                })
                .collect();
            Ok(JoinResponse {
                node_id: id,
                cluster_id: FAKE_P13_CLUSTER_ID.into(),
                partitions: 16,
                ca_cert_pem: "-----BEGIN CERTIFICATE-----\nfake-ca\n-----END CERTIFICATE-----\n"
                    .into(),
                node_cert_pem: format!(
                    "-----BEGIN CERTIFICATE-----\nfake-node-{id}\n-----END CERTIFICATE-----\n"
                ),
                seeds,
                finalized_level: st.finalized,
            })
        }

        async fn status(&self) -> Result<ClusterStatus, ApiError> {
            Ok(self.check("status")?.status())
        }

        async fn create_node_token(
            &self,
            r: NodeTokenRequest,
        ) -> Result<NodeTokenResponse, ApiError> {
            let mut st = self.check("create_node_token")?;
            st.require_cluster()?;
            if r.ttl_secs == Some(0) {
                return Err(ApiError::invalid_argument("ttl_secs must be > 0"));
            }
            let now = st.tick();
            st.next_token += 1;
            let n = st.next_token;
            let token = format!("njt_{n}.fake-secret-{n}");
            st.tokens.insert(token.clone(), n);
            Ok(NodeTokenResponse {
                id: format!("njt_{n}"),
                token,
                expires_at_ms: now + r.ttl_secs.unwrap_or(DEFAULT_TTL_SECS) * 1000,
                ca_hash: FAKE_CA_HASH.into(),
            })
        }

        async fn remove(&self, id: u64, force: bool) -> Result<ClusterStatus, ApiError> {
            let mut st = self.check("remove")?;
            st.require_cluster()?;
            let live = st.member(id)?.live;
            if st.voters.contains(&id) && st.voters.len() == 1 {
                return Err(reason(
                    ErrorCode::Conflict,
                    format!("removing node {id} would leave no meta voter"),
                    "quorum",
                ));
            }
            if !live && !force {
                return Err(ApiError::unavailable(format!(
                    "node {id} is not live; use force to remove it without waiting"
                )));
            }
            st.tick();
            st.voters.remove(&id);
            st.learners.remove(&id);
            if let Some(m) = st.members.get_mut(&id) {
                m.state = "removed".into();
                m.live = false;
            }
            Ok(st.status())
        }

        async fn drain(&self, id: u64) -> Result<ClusterStatus, ApiError> {
            let mut st = self.check("drain")?;
            st.require_cluster()?;
            st.member(id)?;
            st.tick();
            if let Some(m) = st.members.get_mut(&id) {
                m.state = "draining".into();
            }
            Ok(st.status())
        }

        async fn reassign(&self, r: ReassignRequest) -> Result<ClusterStatus, ApiError> {
            let mut st = self.check("reassign")?;
            st.require_cluster()?;
            if r.cancel {
                if st.reassignments.remove(&r.partition).is_none() {
                    return Err(ApiError::not_found(format!(
                        "partition {} has no reassignment in flight",
                        r.partition
                    )));
                }
                st.tick();
                return Ok(st.status());
            }
            if st.reassignments.contains_key(&r.partition) {
                return Err(reason(
                    ErrorCode::Conflict,
                    format!("partition {} is already being reassigned", r.partition),
                    "reassignment_in_progress",
                ));
            }
            for id in &r.replicas {
                st.member(*id)?;
            }
            let now = st.tick();
            st.reassignments.insert(
                r.partition,
                ReassignmentDto {
                    partition: r.partition,
                    target: r.replicas.clone(),
                    adding: r.replicas,
                    removing: vec![],
                    started_at_ms: now,
                },
            );
            Ok(st.status())
        }

        async fn rebalance(&self) -> Result<ClusterStatus, ApiError> {
            let st = self.check("rebalance")?;
            st.require_cluster()?;
            Ok(st.status())
        }

        async fn upgrade_status(&self) -> Result<UpgradeStatus, ApiError> {
            Ok(self.check("upgrade_status")?.upgrade())
        }

        async fn upgrade_finalize(&self) -> Result<UpgradeStatus, ApiError> {
            let mut st = self.check("upgrade_finalize")?;
            if let Some(h) = st.hold {
                return Err(reason(
                    ErrorCode::Conflict,
                    format!("the cluster is held at level {h}"),
                    "held",
                ));
            }
            let (_, common) = st.levels();
            if common > st.finalized {
                st.tick();
                st.finalized = common;
            }
            Ok(st.upgrade())
        }

        async fn upgrade_hold(&self) -> Result<UpgradeStatus, ApiError> {
            let mut st = self.check("upgrade_hold")?;
            st.tick();
            st.hold = Some(st.finalized);
            Ok(st.upgrade())
        }

        async fn upgrade_release(&self) -> Result<UpgradeStatus, ApiError> {
            let mut st = self.check("upgrade_release")?;
            st.tick();
            st.hold = None;
            Ok(st.upgrade())
        }
    }

    /// Outcome of one run in [`FakeModuleLifecycle`]'s migration.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum FakeMigrateOutcome {
        /// Replays cleanly (repinned unless dry run).
        Pass,
        /// Diverges at `seq` with `detail`.
        Fail { seq: u64, detail: String },
        /// Leased (`RunBusy`).
        Busy,
        /// Progressed since the check (`SeqMismatch`).
        SeqMismatch,
    }

    #[derive(Default)]
    struct LifecycleState {
        clock: u64,
        /// sha -> (versions, active, retired_at_ms)
        modules: BTreeMap<String, (Vec<WorkflowVersionRef>, bool, Option<u64>)>,
        referenced: BTreeSet<String>,
        deleted: BTreeSet<String>,
        /// run id -> (workflow, outcome)
        runs: BTreeMap<String, (String, FakeMigrateOutcome)>,
        migrations: Vec<MigrateRunsRequest>,
        errors: BTreeMap<String, ApiError>,
    }

    /// In-memory [`ModuleLifecycleApi`] for adapter, CLI and parity tests.
    ///
    /// - Modules are seeded with [`FakeModuleLifecycle::add_module`]; retiring an unknown
    ///   module is 404, an active one 409 `module_active`, a retired one answers the first
    ///   retirement again (idempotent, as the server).
    /// - A GC round deletes every retired module not marked referenced
    ///   ([`FakeModuleLifecycle::set_referenced`]); deleted modules are gone (404 after).
    /// - Migration processes the seeded runs of the workflow in run-id order up to `limit`;
    ///   `Pass` runs are `passed` and, unless dry run, `repinned`.
    #[derive(Default)]
    pub struct FakeModuleLifecycle {
        state: Mutex<LifecycleState>,
    }

    impl FakeModuleLifecycle {
        /// Empty fake.
        pub fn new() -> Arc<Self> {
            Arc::new(FakeModuleLifecycle::default())
        }

        fn lock(&self) -> MutexGuard<'_, LifecycleState> {
            self.state.lock().unwrap_or_else(|p| p.into_inner())
        }

        fn check(&self, method: &str) -> Result<MutexGuard<'_, LifecycleState>, ApiError> {
            let st = self.lock();
            match st.errors.get(method) {
                Some(e) => Err(e.clone()),
                None => Ok(st),
            }
        }

        /// Make `method` (a [`ModuleLifecycleApi`] method name) answer `err`.
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            let mut st = self.lock();
            match err {
                Some(e) => st.errors.insert(method.to_owned(), e),
                None => st.errors.remove(method),
            };
        }

        /// Seed a module with its workflow versions; `active` = a version is active.
        pub fn add_module(&self, sha: &str, versions: &[(&str, u32)], active: bool) {
            let versions = versions
                .iter()
                .map(|(n, v)| WorkflowVersionRef {
                    name: (*n).to_owned(),
                    version: *v,
                })
                .collect();
            self.lock()
                .modules
                .insert(sha.to_owned(), (versions, active, None));
        }

        /// Mark a module as still referenced by some run.
        pub fn set_referenced(&self, sha: &str, referenced: bool) {
            let mut st = self.lock();
            if referenced {
                st.referenced.insert(sha.to_owned());
            } else {
                st.referenced.remove(sha);
            }
        }

        /// Seed a run of `workflow` with its migration outcome.
        pub fn add_run(&self, run_id: &str, workflow: &str, outcome: FakeMigrateOutcome) {
            self.lock()
                .runs
                .insert(run_id.to_owned(), (workflow.to_owned(), outcome));
        }

        /// Every migration request received.
        pub fn migrations(&self) -> Vec<MigrateRunsRequest> {
            self.lock().migrations.clone()
        }

        /// Fill `ext.module_lifecycle` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.module_lifecycle = Some(Arc::clone(self) as Arc<dyn ModuleLifecycleApi>);
        }
    }

    #[async_trait::async_trait]
    impl ModuleLifecycleApi for FakeModuleLifecycle {
        async fn retire_module(&self, sha: String) -> Result<RetireModuleResponse, ApiError> {
            let mut st = self.check("retire_module")?;
            st.clock += 1;
            let now = FAKE_EPOCH_MS + st.clock * FAKE_TICK_MS;
            let Some((versions, active, retired)) = st.modules.get_mut(&sha) else {
                return Err(ApiError::not_found(format!("module {sha} not found")));
            };
            if let Some(at) = *retired {
                // Idempotent like the server (13-15): the first retirement again.
                return Ok(RetireModuleResponse {
                    sha256: sha,
                    retired_at_ms: at,
                    versions: versions.clone(),
                });
            }
            if *active {
                return Err(reason(
                    ErrorCode::Conflict,
                    format!("module {sha} has an active workflow version"),
                    "module_active",
                ));
            }
            *retired = Some(now);
            Ok(RetireModuleResponse {
                sha256: sha,
                retired_at_ms: now,
                versions: versions.clone(),
            })
        }

        async fn run_module_gc(&self) -> Result<GcRunReport, ApiError> {
            let mut st = self.check("run_module_gc")?;
            let candidates: Vec<String> = st
                .modules
                .iter()
                .filter(|(_, (_, _, r))| r.is_some())
                .map(|(s, _)| s.clone())
                .collect();
            let (referenced, deleted): (Vec<String>, Vec<String>) = candidates
                .iter()
                .cloned()
                .partition(|s| st.referenced.contains(s));
            for s in &deleted {
                st.modules.remove(s);
                st.deleted.insert(s.clone());
            }
            Ok(GcRunReport {
                candidates,
                deleted,
                referenced,
                postponed: false,
                reason: None,
            })
        }

        async fn migrate_runs(&self, r: MigrateRunsRequest) -> Result<MigrateRunsReport, ApiError> {
            let mut st = self.check("migrate_runs")?;
            st.migrations.push(r.clone());
            let limit = r.limit.map_or(usize::MAX, |l| l as usize);
            let mut out = MigrateRunsReport::default();
            let runs: Vec<(String, FakeMigrateOutcome)> = st
                .runs
                .iter()
                .filter(|(_, (w, _))| *w == r.workflow)
                .take(limit)
                .map(|(id, (_, o))| (id.clone(), o.clone()))
                .collect();
            for (id, o) in runs {
                match o {
                    FakeMigrateOutcome::Pass => {
                        out.passed.push(id.clone());
                        if !r.dry_run {
                            out.repinned.push(id);
                        }
                    }
                    FakeMigrateOutcome::Fail { seq, detail } => out.failed.push(MigrateFailure {
                        run_id: id,
                        seq,
                        detail,
                    }),
                    FakeMigrateOutcome::Busy => out.busy.push(id),
                    FakeMigrateOutcome::SeqMismatch => out.seq_mismatch.push(id),
                }
            }
            Ok(out)
        }
    }
}

/// In-memory Phase 14 traits (tenants, API tokens, audit, module security), 14-02.
pub use p14::{FakeAudit, FakeP14, FakeSecurity, FakeTenants, FakeTokens};

mod p14 {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, MutexGuard};

    use crate::error::{ApiError, ErrorCode, reason};
    use crate::phase8::ApiExtensions;
    use crate::phase14::*;

    use super::{FAKE_EPOCH_MS, FAKE_TICK_MS};

    /// Per-method error injection shared by the four fakes.
    #[derive(Default)]
    struct Errors(BTreeMap<String, ApiError>);

    impl Errors {
        fn check(&self, method: &str) -> Result<(), ApiError> {
            match self.0.get(method) {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            }
        }

        fn set(&mut self, method: &str, err: Option<ApiError>) {
            match err {
                Some(e) => self.0.insert(method.to_owned(), e),
                None => self.0.remove(method),
            };
        }
    }

    fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap_or_else(|p| p.into_inner())
    }

    #[derive(Default)]
    struct TenantState {
        clock: u64,
        tenants: BTreeMap<String, TenantInfo>,
        errors: Errors,
    }

    /// In-memory [`TenantsApi`]: create (409 when it exists), get / update / suspend /
    /// resume (404 when absent), delete (409 `tenant_not_empty` unless suspended and
    /// marked empty with [`FakeTenants::set_empty`]; new tenants are empty).
    /// [`FakeTenants::fail`] makes a method answer an error.
    #[derive(Default)]
    pub struct FakeTenants {
        state: Mutex<TenantState>,
        non_empty: Mutex<std::collections::BTreeSet<String>>,
    }

    impl FakeTenants {
        /// No tenants.
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        /// Make `method` (a [`TenantsApi`] method name) answer `err` (`None` clears it).
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            lock(&self.state).errors.set(method, err);
        }

        /// Mark a tenant as owning resources (delete answers `tenant_not_empty`).
        pub fn set_empty(&self, id: &str, empty: bool) {
            let mut s = lock(&self.non_empty);
            if empty {
                s.remove(id);
            } else {
                s.insert(id.to_owned());
            }
        }

        /// Fill `ext.tenants` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.tenants = Some(Arc::clone(self) as Arc<dyn TenantsApi>);
        }

        fn with<T>(
            &self,
            method: &str,
            id: &str,
            f: impl FnOnce(&mut TenantInfo) -> T,
        ) -> Result<T, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check(method)?;
            let t = st
                .tenants
                .get_mut(id)
                .ok_or_else(|| ApiError::not_found(format!("tenant {id} not found")))?;
            Ok(f(t))
        }
    }

    #[async_trait::async_trait]
    impl TenantsApi for FakeTenants {
        async fn create_tenant(&self, spec: TenantSpec) -> Result<TenantInfo, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("create_tenant")?;
            if st.tenants.contains_key(&spec.id) {
                return Err(ApiError::conflict(format!("tenant {} exists", spec.id)));
            }
            st.clock += 1;
            let info = TenantInfo {
                spec,
                suspended: false,
                created_at_ms: FAKE_EPOCH_MS + st.clock * FAKE_TICK_MS,
            };
            st.tenants.insert(info.spec.id.clone(), info.clone());
            Ok(info)
        }

        async fn get_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
            self.with("get_tenant", &id, |t| t.clone())
        }

        async fn list_tenants(&self) -> Result<Vec<TenantInfo>, ApiError> {
            let st = lock(&self.state);
            st.errors.check("list_tenants")?;
            Ok(st.tenants.values().cloned().collect())
        }

        async fn update_tenant(&self, id: String, p: TenantPatch) -> Result<TenantInfo, ApiError> {
            self.with("update_tenant", &id, |t| {
                let s = &mut t.spec;
                if let Some(v) = p.weight {
                    s.weight = v;
                }
                if let Some(v) = p.max_concurrency {
                    s.max_concurrency = v;
                }
                if let Some(v) = p.fuel_per_s {
                    s.fuel_per_s = v;
                }
                if let Some(v) = p.fuel_burst {
                    s.fuel_burst = v;
                }
                if let Some(v) = p.memory_bytes {
                    s.memory_bytes = v;
                }
                if let Some(v) = p.event_rate {
                    s.event_rate = v;
                }
                t.clone()
            })
        }

        async fn suspend_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
            self.with("suspend_tenant", &id, |t| {
                t.suspended = true;
                t.clone()
            })
        }

        async fn resume_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
            self.with("resume_tenant", &id, |t| {
                t.suspended = false;
                t.clone()
            })
        }

        async fn delete_tenant(&self, id: String) -> Result<(), ApiError> {
            let non_empty = lock(&self.non_empty).contains(&id);
            let mut st = lock(&self.state);
            st.errors.check("delete_tenant")?;
            let t = st
                .tenants
                .get(&id)
                .ok_or_else(|| ApiError::not_found(format!("tenant {id} not found")))?;
            if !t.suspended || non_empty {
                return Err(
                    ApiError::conflict(format!("tenant {id} must be suspended and empty"))
                        .with_details(serde_json::json!({ "reason": reason::TENANT_NOT_EMPTY })),
                );
            }
            st.tenants.remove(&id);
            Ok(())
        }
    }

    #[derive(Default)]
    struct TokenState {
        clock: u64,
        next: u64,
        tokens: BTreeMap<String, (TokenInfo, bool)>,
        errors: Errors,
    }

    /// In-memory [`ApiTokensApi`]: ids `otk_<16 base32>` from a counter, bearer
    /// `ostro_otk_<id>_<52 chars>`; revoked tokens are hidden from `list_tokens`,
    /// `revoke_token` is idempotent, unknown ids answer 404.
    /// `whoami` answers the current [`crate::auth::current`] principal.
    #[derive(Default)]
    pub struct FakeTokens {
        state: Mutex<TokenState>,
    }

    impl FakeTokens {
        /// No tokens.
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        /// Make `method` (an [`ApiTokensApi`] method name) answer `err`.
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            lock(&self.state).errors.set(method, err);
        }

        /// Fill `ext.api_tokens` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.api_tokens = Some(Arc::clone(self) as Arc<dyn ApiTokensApi>);
        }

        fn mint(
            st: &mut TokenState,
            name: String,
            description: String,
            scopes: Vec<String>,
            tenant: Option<String>,
            ttl_ms: Option<u64>,
            rotated_from: Option<String>,
        ) -> CreatedToken {
            st.next += 1;
            st.clock += 1;
            let now = FAKE_EPOCH_MS + st.clock * FAKE_TICK_MS;
            let id16 = format!("{:a>16}", base32(st.next));
            let id = format!("otk_{id16}");
            let info = TokenInfo {
                id: id.clone(),
                name,
                description,
                scopes,
                tenant,
                created_at_ms: now,
                expires_at_ms: ttl_ms.map(|t| now + t),
                created_by: crate::auth::current().token_id,
                rotated_from,
            };
            st.tokens.insert(id, (info.clone(), false));
            CreatedToken {
                info,
                bearer: format!("ostro_otk_{id16}_{}", "s".repeat(52)),
            }
        }
    }

    /// Lowercase RFC 4648 base32 digits of `n` (no padding).
    fn base32(mut n: u64) -> String {
        const A: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
        let mut out = Vec::new();
        loop {
            out.push(A[(n % 32) as usize]);
            n /= 32;
            if n == 0 {
                break;
            }
        }
        out.reverse();
        String::from_utf8(out).unwrap_or_default()
    }

    #[async_trait::async_trait]
    impl ApiTokensApi for FakeTokens {
        async fn create_token(&self, r: CreateTokenRequest) -> Result<CreatedToken, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("create_token")?;
            if r.scopes.is_empty() {
                return Err(ApiError::invalid_argument("scopes is required"));
            }
            crate::auth::ScopeSet::parse(&r.scopes).map_err(ApiError::invalid_argument)?;
            let ttl = if r.no_expiry {
                None
            } else {
                Some(r.ttl_ms.unwrap_or(90 * 24 * 3_600_000))
            };
            Ok(Self::mint(
                &mut st,
                r.name,
                r.description,
                r.scopes,
                r.tenant,
                ttl,
                None,
            ))
        }

        async fn list_tokens(&self, tenant: Option<String>) -> Result<Vec<TokenInfo>, ApiError> {
            let st = lock(&self.state);
            st.errors.check("list_tokens")?;
            Ok(st
                .tokens
                .values()
                .filter(|(_, revoked)| !revoked)
                .map(|(t, _)| t)
                .filter(|t| tenant.is_none() || t.tenant == tenant)
                .cloned()
                .collect())
        }

        async fn revoke_token(&self, id: String) -> Result<(), ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("revoke_token")?;
            match st.tokens.get_mut(&id) {
                Some((_, revoked)) => {
                    *revoked = true;
                    Ok(())
                }
                None => Err(ApiError::not_found(format!("token {id} not found"))),
            }
        }

        async fn rotate_token(
            &self,
            id: String,
            r: RotateTokenRequest,
        ) -> Result<CreatedToken, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("rotate_token")?;
            let (old, revoked) = st
                .tokens
                .get(&id)
                .cloned()
                .ok_or_else(|| ApiError::not_found(format!("token {id} not found")))?;
            if revoked {
                return Err(ApiError::precondition_failed(format!(
                    "token {id} is revoked"
                )));
            }
            let ttl = r.ttl_ms.or(old
                .expires_at_ms
                .map(|e| e.saturating_sub(old.created_at_ms)));
            let created = Self::mint(
                &mut st,
                old.name.clone(),
                old.description.clone(),
                old.scopes.clone(),
                old.tenant.clone(),
                ttl,
                Some(id.clone()),
            );
            if r.revoke_old
                && let Some((_, revoked)) = st.tokens.get_mut(&id)
            {
                *revoked = true;
            }
            Ok(created)
        }

        async fn whoami(&self) -> Result<WhoAmI, ApiError> {
            lock(&self.state).errors.check("whoami")?;
            let p = crate::auth::current();
            Ok(WhoAmI {
                token_id: p.token_id,
                tenant: p.tenant,
                scopes: p.scopes.to_strings(),
                kind: p.kind.as_str().to_owned(),
            })
        }
    }

    #[derive(Default)]
    struct AuditState {
        records: Vec<AuditRecordView>,
        errors: Errors,
    }

    /// In-memory [`AuditApi`] over records added with [`FakeAudit::push`]: filters by
    /// tenant, actor, action (exact or `prefix.*`), `[since, until)`; pages of `limit`
    /// (default 100) with the next index as cursor (`bad cursor` is 400).
    #[derive(Default)]
    pub struct FakeAudit {
        state: Mutex<AuditState>,
    }

    impl FakeAudit {
        /// No records.
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        /// Make `query` answer `err` (e.g. 503 `audit_unavailable`).
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            lock(&self.state).errors.set(method, err);
        }

        /// Append a record (its `seq` is set to its position).
        pub fn push(&self, mut r: AuditRecordView) {
            let mut st = lock(&self.state);
            r.seq = st.records.len() as u64;
            st.records.push(r);
        }

        /// Fill `ext.audit` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.audit = Some(Arc::clone(self) as Arc<dyn AuditApi>);
        }
    }

    #[async_trait::async_trait]
    impl AuditApi for FakeAudit {
        async fn query(&self, q: AuditQuery) -> Result<AuditPage, ApiError> {
            let st = lock(&self.state);
            st.errors.check("query")?;
            let start = match &q.cursor {
                Some(c) => c
                    .parse::<usize>()
                    .map_err(|_| ApiError::invalid_argument("bad cursor"))?,
                None => 0,
            };
            let limit = q.limit.unwrap_or(100).clamp(1, 1000) as usize;
            let action_ok = |a: &str| match &q.action {
                None => true,
                Some(f) => match f.strip_suffix('*') {
                    Some(prefix) => a.starts_with(prefix),
                    None => a == f,
                },
            };
            let mut it = st
                .records
                .iter()
                .skip(start)
                .filter(|r| q.tenant.is_none() || r.tenant == q.tenant)
                .filter(|r| q.actor.as_ref().is_none_or(|a| &r.actor == a))
                .filter(|r| action_ok(&r.action))
                .filter(|r| q.since_ms.is_none_or(|s| r.ts_ms >= s))
                .filter(|r| q.until_ms.is_none_or(|u| r.ts_ms < u));
            let records: Vec<AuditRecordView> = it.by_ref().take(limit).cloned().collect();
            let cursor = match (it.next(), records.last()) {
                (Some(_), Some(last)) => Some((last.seq + 1).to_string()),
                _ => None,
            };
            Ok(AuditPage {
                records,
                cursor,
                partial_nodes: vec![],
            })
        }
    }

    #[derive(Default)]
    struct SecurityState {
        policy: ModulePolicy,
        modules: BTreeMap<String, Vec<String>>,
        /// Sealed records: `true` = on the current key (p14-key-rotation).
        sealed: Vec<bool>,
        errors: Errors,
    }

    /// The fake's current and old sealing key ids.
    pub const FAKE_SEALING_KEYS: [&str; 2] = ["aes256gcm:fa4ec0de", "aes256gcm:01d0c0de"];

    fn fake_sealing_status(sealed: &[bool]) -> SealingStatus {
        let current = sealed.iter().filter(|c| **c).count() as u64;
        let old = sealed.len() as u64 - current;
        let mut keys = Vec::new();
        if old > 0 {
            keys.push(SealingKeyUse {
                key_id: FAKE_SEALING_KEYS[1].into(),
                state: "old".into(),
                records: old,
            });
        }
        if current > 0 {
            keys.push(SealingKeyUse {
                key_id: FAKE_SEALING_KEYS[0].into(),
                state: "current".into(),
                records: current,
            });
        }
        SealingStatus {
            node: 1,
            current_key_id: Some(FAKE_SEALING_KEYS[0].into()),
            old_key_ids: vec![FAKE_SEALING_KEYS[1].into()],
            records: sealed.len() as u64,
            current,
            old,
            unknown: 0,
            plaintext: 0,
            keys,
            old_keys_removable: old == 0,
        }
    }

    /// In-memory [`SecurityApi`]: the policy, keys (409 on a duplicate id, 404 on removing
    /// an unknown one) and per-module key ids. `attach_signatures` needs a module added with
    /// [`FakeSecurity::add_module`] (404 otherwise) and records one key id per signature:
    /// `fake-<n>` where `n` is the signature's decoded length.
    #[derive(Default)]
    pub struct FakeSecurity {
        state: Mutex<SecurityState>,
    }

    impl FakeSecurity {
        /// Signing not required, no keys, no modules.
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        /// Make `method` (a [`SecurityApi`] method name) answer `err`.
        pub fn fail(&self, method: &str, err: Option<ApiError>) {
            lock(&self.state).errors.set(method, err);
        }

        /// Add a sealed record, on the current key or an old one.
        pub fn add_sealed(&self, current: bool) {
            lock(&self.state).sealed.push(current);
        }

        /// Register a stored module.
        pub fn add_module(&self, sha256: &str) {
            lock(&self.state)
                .modules
                .entry(sha256.to_owned())
                .or_default();
        }

        /// Key ids attached to a module.
        pub fn key_ids(&self, sha256: &str) -> Vec<String> {
            lock(&self.state)
                .modules
                .get(sha256)
                .cloned()
                .unwrap_or_default()
        }

        /// Fill `ext.security` with this fake.
        pub fn install(self: &Arc<Self>, ext: &mut ApiExtensions) {
            ext.security = Some(Arc::clone(self) as Arc<dyn SecurityApi>);
        }
    }

    #[async_trait::async_trait]
    impl SecurityApi for FakeSecurity {
        async fn get_module_policy(&self) -> Result<ModulePolicy, ApiError> {
            let st = lock(&self.state);
            st.errors.check("get_module_policy")?;
            Ok(st.policy.clone())
        }

        async fn set_module_policy(&self, p: ModulePolicy) -> Result<ModulePolicy, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("set_module_policy")?;
            // A tenant's requirement leaves the cluster policy alone; the answer carries it.
            if let Some(t) = p.tenant.clone().filter(|t| !t.is_empty()) {
                let mut v = st.policy.clone();
                v.require_signed_modules |= p.require_signed_modules;
                v.tenant = Some(t);
                return Ok(v);
            }
            st.policy = p;
            Ok(st.policy.clone())
        }

        async fn add_trusted_key(&self, k: TrustedKeySpec) -> Result<ModulePolicy, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("add_trusted_key")?;
            if !matches!(k.alg.as_str(), "minisign" | "cosign-p256") {
                return Err(ApiError::invalid_argument(format!(
                    "alg {:?}: expected minisign or cosign-p256",
                    k.alg
                )));
            }
            if st.policy.trusted_keys.iter().any(|t| t.key_id == k.key_id) {
                return Err(ApiError::conflict(format!("key {} exists", k.key_id)));
            }
            st.policy.trusted_keys.push(k);
            Ok(st.policy.clone())
        }

        async fn remove_trusted_key(&self, key_id: String) -> Result<ModulePolicy, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("remove_trusted_key")?;
            let before = st.policy.trusted_keys.len();
            st.policy.trusted_keys.retain(|k| k.key_id != key_id);
            if st.policy.trusted_keys.len() == before {
                return Err(ApiError::not_found(format!("key {key_id} not found")));
            }
            Ok(st.policy.clone())
        }

        async fn attach_signatures(
            &self,
            r: AttachSignaturesRequest,
        ) -> Result<AttachSignaturesResult, ApiError> {
            use base64::Engine;
            let mut st = lock(&self.state);
            st.errors.check("attach_signatures")?;
            let ids: Vec<String> = r
                .signatures
                .iter()
                .map(|s| {
                    base64::engine::general_purpose::STANDARD
                        .decode(s)
                        .map(|b| format!("fake-{}", b.len()))
                        .map_err(|_| {
                            ApiError::new(ErrorCode::InvalidArgument, "signature is not base64")
                        })
                })
                .collect::<Result<_, _>>()?;
            let m = st
                .modules
                .get_mut(&r.sha256)
                .ok_or_else(|| ApiError::not_found(format!("module {} not found", r.sha256)))?;
            for id in &ids {
                if !m.contains(id) {
                    m.push(id.clone());
                }
            }
            Ok(AttachSignaturesResult {
                sha256: r.sha256,
                key_ids: m.clone(),
            })
        }

        async fn get_sealing_status(&self) -> Result<SealingStatus, ApiError> {
            let st = lock(&self.state);
            st.errors.check("get_sealing_status")?;
            Ok(fake_sealing_status(&st.sealed))
        }

        async fn reseal(&self) -> Result<ResealResult, ApiError> {
            let mut st = lock(&self.state);
            st.errors.check("reseal")?;
            let resealed = st.sealed.iter().filter(|c| !**c).count() as u64;
            let records = st.sealed.len() as u64;
            st.sealed.iter_mut().for_each(|c| *c = true);
            Ok(ResealResult {
                key_id: FAKE_SEALING_KEYS[0].into(),
                records,
                resealed,
                already_current: records - resealed,
                skipped: 0,
                failed: Vec::new(),
                status: fake_sealing_status(&st.sealed),
            })
        }
    }

    /// The four Phase 14 fakes together.
    #[derive(Clone)]
    pub struct FakeP14 {
        /// Tenants.
        pub tenants: Arc<FakeTenants>,
        /// API tokens.
        pub tokens: Arc<FakeTokens>,
        /// Audit.
        pub audit: Arc<FakeAudit>,
        /// Module security.
        pub security: Arc<FakeSecurity>,
    }

    impl FakeP14 {
        /// Fresh fakes.
        pub fn new() -> Self {
            FakeP14 {
                tenants: FakeTenants::new(),
                tokens: FakeTokens::new(),
                audit: FakeAudit::new(),
                security: FakeSecurity::new(),
            }
        }

        /// Fill the four Phase 14 slots of `ext`.
        pub fn install(&self, ext: &mut ApiExtensions) {
            self.tenants.install(ext);
            self.tokens.install(ext);
            self.audit.install(ext);
            self.security.install(ext);
        }
    }

    impl Default for FakeP14 {
        fn default() -> Self {
            Self::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[tokio::test]
    async fn bridge_admin_round_trip() {
        let cp = FakeControlPlane::new();
        let info = cp
            .put_secret(PutSecret {
                name: "stripe/live".into(),
                value: b"sk_live_0123456789".to_vec(),
                tenant: None,
            })
            .await
            .expect("put");
        assert_eq!(info.key_id, FAKE_SECRET_KEY_ID);
        assert_eq!(
            cp.secret_value("stripe/live", None).as_deref(),
            Some(&b"sk_live_0123456789"[..])
        );
        assert_eq!(cp.list_secrets(None).await.expect("list"), vec![info]);
        assert!(
            cp.list_secrets(Some("t".into()))
                .await
                .expect("list")
                .is_empty()
        );
        cp.delete_secret("stripe/live".into(), None)
            .await
            .expect("delete");
        assert_eq!(
            cp.delete_secret("stripe/live".into(), None)
                .await
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        let g = cp
            .put_grant(GrantInfo {
                id: String::new(),
                module: "t/m".into(),
                secret: "k".into(),
                origins: vec!["https://x.com".into()],
                body: false,
            })
            .await
            .expect("grant");
        assert_eq!(g.id, "secret:t/m:k");
        assert_eq!(
            cp.list_grants(Some("t/other".into())).await.unwrap(),
            vec![]
        );
        let bad = cp
            .presigned_object(PresignedObjectRequest {
                connection: "c".into(),
                key: "k".into(),
                method: PresignMethod::Get,
                exp: 0,
                sig: "nope".into(),
                body: None,
                content_type: None,
            })
            .await
            .expect("ok");
        assert_eq!(bad.status, 403);
    }

    fn upload_req(wasm: bytes::Bytes) -> UploadModule {
        UploadModule {
            signatures: Vec::new(),
            sha256: sha256_hex(&wasm),
            wasm,
            language: Some("rust".into()),
            tenant: None,
            name: None,
        }
    }

    fn start(workflow: &str) -> StartRunRequest {
        StartRunRequest {
            workflow: workflow.into(),
            input_json: b"{\"n\":1}".to_vec(),
            version: None,
            queue: None,
            delay_ms: None,
            priority: None,
            tags: vec![],
            traceparent: None,
            tenant: None,
            idempotency_key: None,
            conflict_policy: ConflictPolicy::ReturnExisting,
            concurrency_key: None,
            ..Default::default()
        }
    }

    async fn with_greet() -> FakeControlPlane {
        let cp = FakeControlPlane::new();
        cp.upload_module(upload_req(fake_module(&[("greet", 1)])))
            .await
            .unwrap();
        cp
    }

    #[tokio::test]
    async fn upload_sha_mismatch_and_reupload() {
        let cp = FakeControlPlane::new();
        let wasm = fake_module(&[("greet", 1)]);
        let mut bad = upload_req(wasm.clone());
        bad.sha256 = sha256_hex(b"other");
        let e = cp.upload_module(bad).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidArgument);

        let mut upper = upload_req(wasm.clone());
        upper.sha256 = upper.sha256.to_uppercase();
        assert_eq!(
            cp.upload_module(upper).await.unwrap_err().code,
            ErrorCode::InvalidArgument
        );

        let r1 = cp.upload_module(upload_req(wasm.clone())).await.unwrap();
        assert!(r1.created);
        assert_eq!(
            r1.module.workflows,
            vec![WorkflowRef {
                name: "greet".into(),
                version: 1
            }]
        );
        let r2 = cp.upload_module(upload_req(wasm.clone())).await.unwrap();
        assert!(!r2.created);
        assert_eq!(r1.module, r2.module);
        assert_eq!(
            cp.get_module(r1.module.sha256.clone()).await.unwrap(),
            r1.module
        );
        assert_eq!(
            cp.get_module("0".repeat(64)).await.unwrap_err().code,
            ErrorCode::NotFound
        );
    }

    #[tokio::test]
    async fn same_workflow_version_different_module_conflicts() {
        let cp = with_greet().await;
        // Different bytes, same greet@1.
        let e = cp
            .upload_module(upload_req(fake_module(&[("greet", 1), ("other", 1)])))
            .await
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Conflict);
        // Nothing half-applied.
        assert_eq!(cp.list_workflows().await.unwrap().len(), 1);
        // A new version is fine.
        cp.upload_module(upload_req(fake_module(&[("greet", 2)])))
            .await
            .unwrap();
        let wfs = cp.list_workflows().await.unwrap();
        assert_eq!(wfs[0].versions.len(), 2);
        assert_eq!(wfs[0].active_version, Some(1));
    }

    #[tokio::test]
    async fn activate_with_cas() {
        let cp = with_greet().await;
        cp.upload_module(upload_req(fake_module(&[("greet", 2)])))
            .await
            .unwrap();
        let rv = cp.list_workflows().await.unwrap()[0].record_version;
        let stale = cp
            .activate_workflow(ActivateWorkflow {
                name: "greet".into(),
                version: 2,
                expected_version: Some(rv + 7),
            })
            .await
            .unwrap_err();
        assert_eq!(stale.code, ErrorCode::PreconditionFailed);
        let wf = cp
            .activate_workflow(ActivateWorkflow {
                name: "greet".into(),
                version: 2,
                expected_version: Some(rv),
            })
            .await
            .unwrap();
        assert_eq!(wf.active_version, Some(2));
        assert_eq!(wf.record_version, rv + 1);
        let missing = cp
            .activate_workflow(ActivateWorkflow {
                name: "greet".into(),
                version: 9,
                expected_version: None,
            })
            .await
            .unwrap_err();
        assert_eq!(missing.code, ErrorCode::NotFound);
        let unknown = cp
            .activate_workflow(ActivateWorkflow {
                name: "nope".into(),
                version: 1,
                expected_version: None,
            })
            .await
            .unwrap_err();
        assert_eq!(unknown.code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn idempotent_start() {
        let cp = with_greet().await;
        let mut req = start("greet");
        req.idempotency_key = Some("k1".into());
        let a = cp.start_run(req.clone()).await.unwrap();
        assert!(a.created);
        assert_eq!(a.run_id, "run-000001");
        let b = cp.start_run(req.clone()).await.unwrap();
        assert_eq!(b.run_id, a.run_id);
        assert!(!b.created);
        req.conflict_policy = ConflictPolicy::Reject;
        let e = cp.start_run(req).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::Conflict);
        assert_eq!(
            e.details,
            Some(serde_json::json!({ "existing_run_id": "run-000001" }))
        );
        // No key: always a new run.
        let c = cp.start_run(start("greet")).await.unwrap();
        assert_eq!(c.run_id, "run-000002");
    }

    #[tokio::test]
    async fn start_errors_and_get_run() {
        let cp = with_greet().await;
        assert_eq!(
            cp.start_run(start("nope")).await.unwrap_err().code,
            ErrorCode::NotFound
        );
        let mut bad = start("greet");
        bad.input_json = b"{not json".to_vec();
        assert_eq!(
            cp.start_run(bad).await.unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        let mut v9 = start("greet");
        v9.version = Some(9);
        assert_eq!(
            cp.start_run(v9).await.unwrap_err().code,
            ErrorCode::NotFound
        );
        let mut delayed = start("greet");
        delayed.delay_ms = Some(5_000);
        delayed.tags = vec!["a".into()];
        let r = cp.start_run(delayed).await.unwrap();
        let info = cp.get_run(r.run_id.clone()).await.unwrap();
        assert_eq!(info.status, RunStatus::Scheduled);
        assert_eq!(info.queue, "default");
        assert_eq!(info.version, 1);
        assert_eq!(
            cp.get_run("run-999999".into()).await.unwrap_err().code,
            ErrorCode::NotFound
        );
        let h = cp
            .run_history(HistoryRequest {
                run_id: r.run_id,
                from_seq: 0,
                limit: 0,
            })
            .await
            .unwrap();
        assert_eq!(h.events.len(), 1);
        assert_eq!(h.events[0].kind, "RunStarted");
        assert_eq!(h.next_from_seq, None);
    }

    #[tokio::test]
    async fn history_paging() {
        let cp = with_greet().await;
        let r = cp.start_run(start("greet")).await.unwrap();
        cp.set_run_outcome(&r.run_id, RunStatus::Running, None, None, None)
            .unwrap();
        cp.set_run_outcome(
            &r.run_id,
            RunStatus::Completed,
            Some(b"42".to_vec()),
            None,
            None,
        )
        .unwrap();
        let p1 = cp
            .run_history(HistoryRequest {
                run_id: r.run_id.clone(),
                from_seq: 1,
                limit: 2,
            })
            .await
            .unwrap();
        assert_eq!(p1.events.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(p1.next_from_seq, Some(3));
        let p2 = cp
            .run_history(HistoryRequest {
                run_id: r.run_id,
                from_seq: 3,
                limit: 2,
            })
            .await
            .unwrap();
        assert_eq!(p2.events.len(), 1);
        assert_eq!(p2.next_from_seq, None);
    }

    #[tokio::test]
    async fn list_paging_and_filters() {
        let cp = with_greet().await;
        for i in 0..5 {
            let mut req = start("greet");
            if i % 2 == 0 {
                req.tags = vec!["even".into()];
                req.queue = Some("q2".into());
            }
            cp.start_run(req).await.unwrap();
        }
        let mut token = None;
        let mut seen = Vec::new();
        loop {
            let page = cp
                .list_runs(ListRunsRequest {
                    limit: 2,
                    page_token: token.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();
            seen.extend(page.runs.iter().map(|r| r.run_id.clone()));
            match page.next_page_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        assert_eq!(seen.len(), 5);
        assert_eq!(seen[0], "run-000001");
        let even = cp
            .list_runs(ListRunsRequest {
                tag: Some("even".into()),
                queue: Some("q2".into()),
                status: Some(RunStatus::Ready),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(even.runs.len(), 3);
        assert_eq!(even.next_page_token, None);
        let first = cp.get_run("run-000001".into()).await.unwrap();
        let after = cp
            .list_runs(ListRunsRequest {
                created_after_ms: Some(first.created_at_ms + 1),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(after.runs.len(), 4);
        assert_eq!(
            cp.list_runs(ListRunsRequest {
                page_token: Some("garbage".into()),
                ..Default::default()
            })
            .await
            .unwrap_err()
            .code,
            ErrorCode::InvalidArgument
        );
    }

    fn signal(run_id: &str, name: &str, request_id: Option<&str>, dry_run: bool) -> SignalRequest {
        SignalRequest {
            run_id: run_id.into(),
            name: name.into(),
            payload: serde_json::json!({ "v": name }),
            request_id: request_id.map(Into::into),
            dry_run,
        }
    }

    async fn started() -> (FakeControlPlane, String) {
        let cp = with_greet().await;
        let r = cp.start_run(start("greet")).await.unwrap();
        (cp, r.run_id)
    }

    #[tokio::test]
    async fn signal_semantics() {
        let (cp, run) = started().await;
        let t0 = cp.clock_ticks();
        let dry = cp
            .signal_run(signal(&run, "go", Some("a"), true))
            .await
            .unwrap();
        assert_eq!(
            (dry.accepted, dry.duplicate, dry.dry_run),
            (true, false, true)
        );
        assert_eq!(cp.clock_ticks(), t0, "dry_run mutated");
        assert!(cp.signals(&run).is_empty());

        let a = cp
            .signal_run(signal(&run, "go", Some("a"), false))
            .await
            .unwrap();
        assert_eq!((a.accepted, a.duplicate), (true, false));
        let dup = cp
            .signal_run(signal(&run, "go", Some("a"), false))
            .await
            .unwrap();
        assert_eq!((dup.accepted, dup.duplicate), (true, true));
        cp.signal_run(signal(&run, "go", None, false))
            .await
            .unwrap();
        assert_eq!(cp.signals(&run).len(), 2);

        assert_eq!(
            cp.signal_run(signal("run-999999", "go", None, false))
                .await
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        assert_eq!(
            cp.signal_run(signal(&run, "", None, false))
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        cp.set_run_outcome(&run, RunStatus::Completed, None, None, None)
            .unwrap();
        let e = cp
            .signal_run(signal(&run, "go", None, false))
            .await
            .unwrap_err();
        assert_eq!(e.reason(), Some("run_terminal"));
        assert_eq!(e.code, ErrorCode::Conflict);
    }

    #[tokio::test]
    async fn cancel_and_terminate_semantics() {
        let (cp, run) = started().await;
        let c = |dry_run| CancelRequest {
            run_id: run.clone(),
            reason: Some("user".into()),
            dry_run,
        };
        let t0 = cp.clock_ticks();
        assert!(!cp.cancel_run(c(true)).await.unwrap().already_requested);
        assert_eq!(cp.clock_ticks(), t0);
        assert_eq!(cp.cancel_requested(&run), None);
        assert!(!cp.cancel_run(c(false)).await.unwrap().already_requested);
        assert!(cp.cancel_run(c(false)).await.unwrap().already_requested);
        assert!(cp.cancel_run(c(true)).await.unwrap().already_requested);
        assert_eq!(cp.cancel_requested(&run), Some(Some("user".into())));

        let t = |dry_run| TerminateRequest {
            run_id: run.clone(),
            reason: None,
            dry_run,
        };
        let t1 = cp.clock_ticks();
        let dry = cp.terminate_run(t(true)).await.unwrap();
        assert_eq!((dry.status, dry.dry_run), (RunStatus::Terminated, true));
        assert_eq!(cp.clock_ticks(), t1);
        assert_eq!(
            cp.get_run(run.clone()).await.unwrap().status,
            RunStatus::Ready
        );
        cp.terminate_run(t(false)).await.unwrap();
        let info = cp.get_run(run.clone()).await.unwrap();
        assert_eq!(info.status, RunStatus::Terminated);
        assert!(info.completed_at_ms.is_some());
        let e = cp.terminate_run(t(false)).await.unwrap_err();
        assert_eq!(e.reason(), Some("run_terminal"));
        assert_eq!(e.details.unwrap()["status"], "terminated");
        assert_eq!(
            cp.cancel_run(c(false)).await.unwrap_err().reason(),
            Some("run_terminal")
        );
    }

    #[tokio::test]
    async fn retry_semantics() {
        let (cp, run) = started().await;
        let r = |run_id: &str, dry_run| RetryRequest {
            run_id: run_id.into(),
            dry_run,
        };
        assert_eq!(
            cp.retry_run(r(&run, false)).await.unwrap_err().reason(),
            Some("run_not_terminal")
        );
        // Terminated -> resume in place (D1).
        cp.terminate_run(TerminateRequest {
            run_id: run.clone(),
            reason: None,
            dry_run: false,
        })
        .await
        .unwrap();
        let t0 = cp.clock_ticks();
        let dry = cp.retry_run(r(&run, true)).await.unwrap();
        assert_eq!(
            (dry.mode, dry.run_id.as_str()),
            (RetryMode::Resumed, run.as_str())
        );
        assert_eq!(cp.clock_ticks(), t0);
        let res = cp.retry_run(r(&run, false)).await.unwrap();
        assert_eq!(
            (res.mode, res.run_id.as_str()),
            (RetryMode::Resumed, run.as_str())
        );
        let info = cp.get_run(run.clone()).await.unwrap();
        assert_eq!(
            (info.status, info.completed_at_ms),
            (RunStatus::Ready, None)
        );

        // Guest failure -> restart as a new run.
        cp.set_run_outcome(&run, RunStatus::Failed, None, Some("boom".into()), None)
            .unwrap();
        let res = cp.retry_run(r(&run, false)).await.unwrap();
        assert_eq!(res.mode, RetryMode::Restarted);
        assert_ne!(res.run_id, run);
        let new = cp.get_run(res.run_id.clone()).await.unwrap();
        assert_eq!(
            (new.status, new.workflow.as_str()),
            (RunStatus::Ready, "greet")
        );
        assert_eq!(
            cp.get_run(run.clone()).await.unwrap().status,
            RunStatus::Failed
        );

        // Override: an executor-level failure resumes.
        cp.set_resumable(&run, true);
        assert_eq!(
            cp.retry_run(r(&run, true)).await.unwrap().mode,
            RetryMode::Resumed
        );

        cp.set_run_outcome(&res.run_id, RunStatus::Completed, None, None, None)
            .unwrap();
        let e = cp.retry_run(r(&res.run_id, false)).await.unwrap_err();
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::Conflict, Some("run_completed"))
        );
        assert_eq!(
            cp.retry_run(r("run-999999", false)).await.unwrap_err().code,
            ErrorCode::NotFound
        );
    }

    #[tokio::test]
    async fn await_result_waits_and_follows() {
        let (cp, run) = started().await;
        let cp = std::sync::Arc::new(cp);
        let req = |wait_ms, follow| AwaitResultRequest {
            run_id: run.clone(),
            wait_ms,
            follow_continuations: follow,
        };
        let now = cp.await_result(req(0, false)).await.unwrap();
        assert_eq!((now.status, now.completed), (RunStatus::Ready, false));
        assert_eq!(now.result, None);

        // Not terminal within the wait: still Ok, completed = false.
        let t = std::time::Instant::now();
        let out = cp.await_result(req(50, false)).await.unwrap();
        assert!(!out.completed);
        assert!(t.elapsed() >= std::time::Duration::from_millis(45));

        // Completes while waiting.
        let cp2 = cp.clone();
        let run2 = run.clone();
        let waker = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            cp2.set_run_outcome(&run2, RunStatus::Completed, Some(b"7".to_vec()), None, None)
                .unwrap();
        });
        let t = std::time::Instant::now();
        let out = cp.await_result(req(10_000, false)).await.unwrap();
        waker.await.unwrap();
        assert!(t.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!((out.status, out.completed), (RunStatus::Completed, true));
        assert_eq!(out.result, Some(serde_json::json!(7)));

        // Continue-as-new chain.
        let r = cp.start_run(start("greet")).await.unwrap().run_id;
        let next = cp.continue_as_new(&r).unwrap();
        let next2 = cp.continue_as_new(&next).unwrap();
        cp.set_run_outcome(&next2, RunStatus::Failed, None, Some("boom".into()), None)
            .unwrap();
        let req2 = |follow| AwaitResultRequest {
            run_id: r.clone(),
            wait_ms: 0,
            follow_continuations: follow,
        };
        let own = cp.await_result(req2(false)).await.unwrap();
        assert_eq!(
            (own.run_id.as_str(), own.status),
            (r.as_str(), RunStatus::ContinuedAsNew)
        );
        let last = cp.await_result(req2(true)).await.unwrap();
        assert_eq!(last.run_id, next2);
        assert_eq!(last.failure, Some(serde_json::json!("boom")));
        assert_eq!(
            cp.await_result(AwaitResultRequest {
                run_id: "run-999999".into(),
                wait_ms: 0,
                follow_continuations: true,
            })
            .await
            .unwrap_err()
            .code,
            ErrorCode::NotFound
        );
    }

    #[tokio::test]
    async fn token_semantics() {
        let (cp, run) = started().await;
        let token = cp.mint_token(&run, 4);
        assert!(token.starts_with(TOKEN_PREFIX));
        let req = |token: &str, dry_run| CompleteTokenRequest {
            token: token.into(),
            payload: serde_json::json!({ "ok": true }),
            dry_run,
        };
        for bad in [
            "",
            "garbage",
            "ot1_",
            "ot1_run.x",
            "ot2_run-000001.4",
            "ot1_run-000001.5",
        ] {
            let e = cp.complete_token(req(bad, false)).await.unwrap_err();
            assert_eq!(
                (e.code, e.reason()),
                (ErrorCode::InvalidArgument, Some("token_invalid")),
                "{bad}"
            );
            assert!(
                bad.is_empty() || !e.message.contains(bad),
                "message echoes the token"
            );
        }
        let t0 = cp.clock_ticks();
        let dry = cp.complete_token(req(&token, true)).await.unwrap();
        assert_eq!(
            (dry.run_id.as_str(), dry.accepted, dry.dry_run),
            (run.as_str(), true, true)
        );
        assert_eq!(cp.clock_ticks(), t0);
        assert_eq!(cp.token_completed(&run, 4), Some(false));
        let ok = cp.complete_token(req(&token, false)).await.unwrap();
        assert_eq!((ok.accepted, ok.dry_run), (true, false));
        assert_eq!(cp.token_completed(&run, 4), Some(true));
        let e = cp.complete_token(req(&token, false)).await.unwrap_err();
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::Conflict, Some("token_already_completed"))
        );

        let t2 = cp.mint_token(&run, 9);
        cp.set_run_outcome(&run, RunStatus::Cancelled, None, None, None)
            .unwrap();
        assert_eq!(
            cp.complete_token(req(&t2, false))
                .await
                .unwrap_err()
                .reason(),
            Some("run_terminal")
        );
    }

    #[tokio::test]
    async fn policies_round_trip() {
        let cp = with_greet().await;
        assert_eq!(
            cp.get_workflow_policy("greet".into()).await.unwrap(),
            RunPolicy::default()
        );
        let p = RunPolicy {
            retention_ms: Some(168 * 3_600_000),
            schedule_to_start_ms: Some(30_000),
        };
        assert_eq!(cp.set_workflow_policy("greet".into(), p).await.unwrap(), p);
        assert_eq!(cp.get_workflow_policy("greet".into()).await.unwrap(), p);
        assert_eq!(
            cp.set_workflow_policy("nope".into(), p)
                .await
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        assert_eq!(
            cp.get_workflow_policy("nope".into())
                .await
                .unwrap_err()
                .code,
            ErrorCode::NotFound
        );
        // Queues are implicit: any name works.
        assert_eq!(
            cp.get_queue_policy("q".into()).await.unwrap(),
            RunPolicy::default()
        );
        let q = RunPolicy {
            retention_ms: None,
            schedule_to_start_ms: Some(0),
        };
        cp.set_queue_policy("q".into(), q).await.unwrap();
        assert_eq!(cp.get_queue_policy("q".into()).await.unwrap(), q);
        // Replace, not merge.
        cp.set_queue_policy("q".into(), RunPolicy::default())
            .await
            .unwrap();
        assert_eq!(
            cp.get_queue_policy("q".into()).await.unwrap(),
            RunPolicy::default()
        );
        assert_eq!(
            cp.get_queue_policy(String::new()).await.unwrap_err().code,
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn ready_flag() {
        let cp = FakeControlPlane::new();
        assert!(cp.ready());
        cp.set_ready(false);
        assert!(!cp.ready());
    }
}
