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

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use sha2::{Digest, Sha256};

use crate::domain::*;
use crate::error::ApiError;

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
}

impl State {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        FAKE_EPOCH_MS + self.clock * FAKE_TICK_MS
    }
}

/// In-memory control plane.
pub struct FakeControlPlane {
    state: Mutex<State>,
    ready: AtomicBool,
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
        }
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
        Ok(())
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
                }),
                ConflictPolicy::Reject => Err(ApiError::conflict(format!(
                    "idempotency key already used by run {existing}"
                ))
                .with_details(serde_json::json!({ "existing_run_id": existing }))),
            };
        }
        let now = st.tick();
        st.next_run += 1;
        let run_id = format!("run-{:06}", st.next_run);
        let delayed = req.delay_ms.is_some_and(|d| d > 0);
        let run = RunInfo {
            run_id: run_id.clone(),
            workflow: req.workflow.clone(),
            version,
            module_sha256: ver.module_sha256.clone(),
            queue: req
                .queue
                .clone()
                .or(ver.default_queue.clone())
                .unwrap_or_else(|| "default".to_owned()),
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
        })
    }

    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        self.lock()
            .runs
            .get(&run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("run {run_id} not found")))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn upload_req(wasm: bytes::Bytes) -> UploadModule {
        UploadModule {
            sha256: sha256_hex(&wasm),
            wasm,
            language: Some("rust".into()),
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

    #[test]
    fn ready_flag() {
        let cp = FakeControlPlane::new();
        assert!(cp.ready());
        cp.set_ready(false);
        assert!(!cp.ready());
    }
}
