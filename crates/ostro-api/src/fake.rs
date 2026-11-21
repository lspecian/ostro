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
use std::sync::atomic::{AtomicBool, Ordering};

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
        let st = self.lock();
        let limit = effective_limit(req.limit) as usize;
        let start = match &req.page_token {
            Some(t) if !t.is_empty() => {
                if !t.starts_with("run-") {
                    return Err(ApiError::invalid_argument("bad page_token"));
                }
                std::ops::Bound::Excluded(t.clone())
            }
            _ => std::ops::Bound::Unbounded,
        };
        let mut it = st
            .runs
            .range((start, std::ops::Bound::Unbounded))
            .map(|(_, r)| r)
            .filter(|r| req.workflow.as_ref().is_none_or(|w| &r.workflow == w))
            .filter(|r| req.status.is_none_or(|s| r.status == s))
            .filter(|r| req.queue.as_ref().is_none_or(|q| &r.queue == q))
            .filter(|r| req.tag.as_ref().is_none_or(|t| r.tags.contains(t)))
            .filter(|r| req.created_after_ms.is_none_or(|a| r.created_at_ms >= a))
            .filter(|r| req.created_before_ms.is_none_or(|b| r.created_at_ms < b));
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
