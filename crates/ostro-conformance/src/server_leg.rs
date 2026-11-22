//! Server leg (plan 09-13): runs a server-leg [`Spec`] against a real in-process
//! `ostro-server` ([`ServerHarness`]).
//!
//! **Harness.** [`ServerHarness::start`] boots `Server::start` with the dev profile on an
//! ephemeral data dir (`node.ephemeral`, tmpfs), the API on `127.0.0.1:0` and 4 partitions.
//! A loopback Phase 6 [`MockProvider`] is started with it. Egress follows 06-17: the server
//! sets `[egress] allow_private = ["127.0.0.0/8"]` (the SSRF defaults stay as they are for
//! every other range), and each conformance module is deployed under a module name
//! (`conf-rust`, `conf-py`, `conf-ts`) whose egress policy (`PUT /v1/egress/{module}`)
//! allows exactly the mock origin. Every control-plane action goes through the public REST
//! API (`PUT /v1/modules`, `POST /v1/workflows/{name}/runs`, `/signals/{name}`, `/cancel`,
//! `/terminate`, `/retry`, `/result?follow_continuations=true`, `/v1/tokens/complete`,
//! `GET /v1/runs`). Journals and run records are read straight from the partitions (the
//! harness is in-process; the REST history is a display format), and projected with
//! [`crate::projection`] exactly as on the engine leg.
//!
//! **Roles.** `run_spec(spec, langs)` takes one language per workflow role: `langs[0]` runs
//! the root workflow, `langs[1]` its children, `langs[2]` grandchildren; missing roles
//! reuse the last language. Inputs name workflows by base name with a role tag, which the
//! harness rewrites to the full prefixed name before starting the run:
//!
//! | Input string | Becomes |
//! |--------------|---------|
//! | `"wf:<base>"` | `workflow_name(role 1, base)` (the child) |
//! | `"wf<N>:<base>"` | `workflow_name(role N, base)` (`wf2:` = grandchild) |
//! | `"$MOCK<rest>"` | the mock provider's base URL followed by `<rest>` |
//!
//! **Targets.** Script steps and `expect.descendants` address runs by path: `root`,
//! `child` (the root's first child), `child#2` (its second), `child.child` (the first
//! child's first child). A child's id is the deterministic `child_run_id(parent, op, P)` of
//! the N-th `ChildStarted` in the parent's journal. Expected strings `"@<path>"` (in
//! `expect.result`) are replaced by that run's id.
//!
//! **Script** (SCHEMA.md `api` steps; each `target` defaults to `root`):
//!
//! | Step | Effect |
//! |------|--------|
//! | `{"input":"signal","name","payload"}` | `POST /v1/runs/{root}/signals/{name}` |
//! | `{"input":"cancel"}` | `POST /v1/runs/{root}/cancel` |
//! | `api` `signal` `{target, name, payload}` | signal a target |
//! | `api` `token` `{target, payload}` | read `published_status.token` of the target, `POST /v1/tokens/complete` |
//! | `api` `cancel` `{target, reason?}` / `terminate` `{target, reason?}` | graceful cancel / hard stop |
//! | `api` `retry` `{target}` | `POST /v1/runs/{id}/retry`, then wait until the status leaves the terminal one it had |
//! | `api` `wait_fact` `{target, fact}` | wait until the target's projection (its chain) contains `fact` |
//! | `api` `wait_status` `{target, status}` | wait until the target (last run of its chain) has `status` (`terminal` = any terminal) |
//! | `api` `assert_live` `{target}` | fail if the target is terminal |
//! | `api` `sleep` `{ms}` | wall-clock pause |
//!
//! After the script the harness waits for the root chain's final run (result long-poll
//! following continue-as-new) and checks `expect`: `status` (REST status of the last run:
//! `completed`, `failed`, `cancelled`, `terminated`, `timed_out`), `result` (canonical
//! JSON), `failure_kind` (of a failed root), `projection` (facts of the root chain; await
//! tokens completed by a `token` step appear as `<token>`, e.g. in a `status:` fact). It
//! always checks that no journal of the root chain has two `ChildCompleted` for one op.
//!
//! **`expect` extras:**
//!
//! - `descendants`: `{"<path>": {"status"?, "projection"?, "cancel_reason"?, "parent"?:
//!   bool, "result"?}}`. A terminal `status` is awaited for up to 10 s. `cancel_reason` is
//!   the reason of the first `CancelRequested` in the descendant's journal. `parent: false`
//!   asserts the run has no parent link, `true` that it points at (parent, op).
//! - `chain_runs`: the number of runs in the root's continue-as-new chain.
//! - `fx_pay`: `{"key": "<name>", "status": 200}`: the key is
//!   `idempotency_key_for(<root run id>, name)`; the result's `status` must match, its
//!   `body.key` must equal the key, and the mock must have seen exactly one request and
//!   one charge under it.
//! - `variants`: a list of `{"title", "input", "script"?, "expect"}` run as further root
//!   runs of the same workflow (C-CHILD-06 run timeout, C-CHILD-10 child_terminated).
//!
//! Every child seen while checking a scenario is recorded; [`ServerHarness::check_children_unique`]
//! then lists runs per child workflow (`GET /v1/runs?workflow=`) and asserts exactly one
//! run per (parent, op): the deterministic id, and no other first-generation run carrying
//! that parent link.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ostro_server::ServerHandle;
use ostro_server::children::child_run_id;
use ostro_server::config::ServerConfig;
use ostro_server::journal_io::read_journal;
use ostro_testkit::mock_provider::MockProvider;
use ostro_types::RunId;
use ostro_types::journal::{Event, EventBody};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::canon;
use crate::compare;
use crate::engine_leg::{Failure, RunReport};
use crate::langs::{self, Lang};
use crate::projection::{self, Projection};
use crate::spec::{Expect, ScriptStep, Spec};

/// Limit for the root chain to reach its final state.
pub const RUN_LIMIT: Duration = Duration::from_secs(30);
/// Limit for a descendant to reach an expected terminal status (C-CHILD-09: 10 s).
pub const DESCENDANT_LIMIT: Duration = Duration::from_secs(10);
/// Limit for script waits (`wait_fact`, `wait_status`, child resolution).
pub const STEP_LIMIT: Duration = Duration::from_secs(20);
const PARTITIONS: u16 = 4;
const POLL: Duration = Duration::from_millis(20);

/// P-11 semantic idempotency key (`ostro_sdk::idempotency_key_for`, pinned by
/// `conformance/vectors/idempotency.json`):
/// `"ostro-" + hex(sha256("ostro/idem/v1/name" || run_id || 0x00 || name))[..32]`.
pub fn idempotency_key_for(run_id: &str, name: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"ostro/idem/v1/name");
    h.update(run_id.as_bytes());
    h.update([0u8]);
    h.update(name.as_bytes());
    let d = h.finalize();
    let hex: String = d[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!("ostro-{hex}")
}

/// Module name a language's conformance module is deployed under (egress policy key).
pub fn module_name(lang: Lang) -> String {
    format!("conf-{}", langs::name(lang))
}

/// What the harness reads from a run record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunInfo {
    pub id: RunId,
    pub workflow: String,
    /// REST spelling: `running`, `completed`, `cancelled`, `timed_out`, ...
    pub status: String,
    pub parent: Option<(RunId, u64)>,
    pub continued_from: Option<RunId>,
    pub continued_to: Option<RunId>,
    pub cancel_reason: String,
}

impl RunInfo {
    pub fn is_terminal(&self) -> bool {
        is_terminal(&self.status)
    }
}

/// Whether a REST status string is terminal (continue-as-new included).
pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "cancelled" | "terminated" | "timed_out" | "continued_as_new"
    )
}

fn snake(debug: &str) -> String {
    let mut out = String::new();
    for (i, c) in debug.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// A child start observed in a parent journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildEdge {
    pub parent: RunId,
    pub op: u64,
    pub workflow: String,
    pub detached: bool,
    pub child: RunId,
}

/// Final state of a root chain.
#[derive(Debug, Clone)]
pub struct Outcome {
    /// Last run of the chain.
    pub run: RunId,
    pub status: String,
    pub result: Option<Value>,
    pub failure: Option<Value>,
}

/// In-process server + mock provider + REST client.
pub struct ServerHarness {
    handle: Option<ServerHandle>,
    http: reqwest::Client,
    base: String,
    partitions: u16,
    mock: MockProvider,
    deployed: Mutex<HashMap<Lang, String>>,
    edges: Mutex<Vec<ChildEdge>>,
    /// Await tokens completed through the API (normalised to `<token>` in facts).
    tokens: Mutex<Vec<String>>,
}

impl ServerHarness {
    /// Boots the server (dev profile, ephemeral data dir, `127.0.0.1:0`, P = 4, loopback
    /// egress for the mock) and the mock provider; waits for `/readyz`.
    pub async fn start() -> Result<Self, String> {
        let mock = MockProvider::start().await;
        let mut cfg = ServerConfig::dev_defaults();
        cfg.api.listen = "127.0.0.1:0".parse().map_err(|e| format!("addr: {e}"))?;
        cfg.cluster.listen = "127.0.0.1:0".parse().map_err(|e| format!("addr: {e}"))?;
        cfg.node.ephemeral = true;
        cfg.node.partitions = PARTITIONS;
        // 06-17: the mock listens on loopback, which the SSRF defaults block; allow only it.
        cfg.egress.allow_private = vec!["127.0.0.0/8".into()];
        let handle =
            tokio::time::timeout(Duration::from_secs(60), ostro_server::Server::start(cfg))
                .await
                .map_err(|_| "server start timed out".to_string())?
                .map_err(|e| format!("server start: {e}"))?;
        let base = format!("http://{}", handle.local_addr());
        let h = Self {
            handle: Some(handle),
            http: reqwest::Client::new(),
            base,
            partitions: PARTITIONS,
            mock,
            deployed: Mutex::new(HashMap::new()),
            edges: Mutex::new(Vec::new()),
            tokens: Mutex::new(Vec::new()),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(r) = h.http.get(h.url("/readyz")).send().await
                && r.status() == reqwest::StatusCode::OK
            {
                break;
            }
            if Instant::now() > deadline {
                return Err("server not ready within 30 s".into());
            }
            tokio::time::sleep(POLL).await;
        }
        Ok(h)
    }

    /// Graceful shutdown (the ephemeral data dir is removed by the server).
    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(h) = self.handle.take() {
            tokio::time::timeout(Duration::from_secs(60), h.shutdown())
                .await
                .map_err(|_| "shutdown timed out".to_string())?
                .map_err(|e| format!("shutdown: {e}"))?;
        }
        Ok(())
    }

    fn h(&self) -> &ServerHandle {
        self.handle.as_ref().expect("server running")
    }

    /// `http://127.0.0.1:<port><path>`.
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// The loopback mock provider.
    pub fn mock(&self) -> &MockProvider {
        &self.mock
    }

    // -- REST -------------------------------------------------------------------------

    async fn send(&self, rb: reqwest::RequestBuilder, what: &str) -> Result<Value, String> {
        let r = rb.send().await.map_err(|e| format!("{what}: {e}"))?;
        let st = r.status();
        let b = r.bytes().await.map_err(|e| format!("{what}: body: {e}"))?;
        let v = if b.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&b)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into()))
        };
        if !st.is_success() {
            return Err(format!("{what}: HTTP {st}: {v}"));
        }
        Ok(v)
    }

    async fn post(&self, path: &str, body: &Value, what: &str) -> Result<Value, String> {
        self.send(self.http.post(self.url(path)).json(body), what)
            .await
    }

    async fn get(&self, path: &str, what: &str) -> Result<Value, String> {
        self.send(self.http.get(self.url(path)), what).await
    }

    /// `PUT /v1/modules/{sha}?name=<name>` (idempotent) and the module's egress policy
    /// (`PUT /v1/egress/{name}` allowing the mock origin). Returns the sha256 hex.
    pub async fn deploy(&self, wasm: Vec<u8>, name: &str) -> Result<String, String> {
        let sha: [u8; 32] = Sha256::digest(&wasm).into();
        let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
        self.send(
            self.http
                .put(self.url(&format!("/v1/modules/{hex}?name={name}")))
                .header("content-type", "application/wasm")
                .body(wasm),
            &format!("deploy {name}"),
        )
        .await?;
        self.send(
            self.http
                .put(self.url(&format!("/v1/egress/{name}")))
                .json(&json!({ "allow": [self.mock.base_url()] })),
            &format!("egress {name}"),
        )
        .await?;
        Ok(hex)
    }

    /// Builds (cached) and deploys a language's conformance module once per harness.
    pub async fn deploy_lang(&self, lang: Lang) -> Result<String, String> {
        if let Some(sha) = self
            .deployed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&lang)
        {
            return Ok(sha.clone());
        }
        let wasm = tokio::task::spawn_blocking(move || langs::module_for(lang, None))
            .await
            .map_err(|e| format!("module build task: {e}"))??;
        let sha = self.deploy(wasm, &module_name(lang)).await?;
        self.deployed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(lang, sha.clone());
        Ok(sha)
    }

    /// `POST /v1/workflows/{workflow}/runs`; returns the new run id.
    pub async fn start_run(&self, workflow: &str, input: &Value) -> Result<RunId, String> {
        let v = self
            .post(
                &format!("/v1/workflows/{workflow}/runs"),
                &json!({ "input": input }),
                &format!("start {workflow}"),
            )
            .await?;
        v["run_id"]
            .as_str()
            .ok_or_else(|| format!("start {workflow}: no run_id in {v}"))?
            .parse()
            .map_err(|e| format!("start {workflow}: run id: {e:?}"))
    }

    pub async fn signal(&self, run: &RunId, name: &str, payload: &Value) -> Result<(), String> {
        self.post(
            &format!("/v1/runs/{run}/signals/{name}"),
            &json!({ "payload": payload }),
            &format!("signal {name}"),
        )
        .await
        .map(drop)
    }

    pub async fn complete_token(&self, token: &str, payload: &Value) -> Result<(), String> {
        self.post(
            "/v1/tokens/complete",
            &json!({ "token": token, "payload": payload }),
            "complete token",
        )
        .await
        .map(drop)
    }

    /// Polls `GET /v1/runs/{id}` until `published_status.token` is set.
    pub async fn published_token(&self, run: &RunId, limit: Duration) -> Result<String, String> {
        let deadline = Instant::now() + limit;
        loop {
            let v = self.get(&format!("/v1/runs/{run}"), "get run").await?;
            if let Some(t) = v["published_status"]["token"].as_str() {
                return Ok(t.to_owned());
            }
            if Instant::now() > deadline {
                return Err(format!("run {run} published no token: {v}"));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    pub async fn cancel(&self, run: &RunId, reason: Option<&str>) -> Result<(), String> {
        let body = reason.map_or_else(|| json!({}), |r| json!({ "reason": r }));
        self.post(&format!("/v1/runs/{run}/cancel"), &body, "cancel")
            .await
            .map(drop)
    }

    pub async fn terminate(&self, run: &RunId, reason: Option<&str>) -> Result<(), String> {
        let body = reason.map_or_else(|| json!({}), |r| json!({ "reason": r }));
        self.post(&format!("/v1/runs/{run}/terminate"), &body, "terminate")
            .await
            .map(drop)
    }

    /// `POST /v1/runs/{id}/retry`; returns the run id of the resumed / restarted run.
    pub async fn retry(&self, run: &RunId) -> Result<RunId, String> {
        let v = self
            .post(&format!("/v1/runs/{run}/retry"), &json!({}), "retry")
            .await?;
        v["run_id"]
            .as_str()
            .unwrap_or(&run.to_string())
            .parse()
            .map_err(|e| format!("retry: run id: {e:?}"))
    }

    /// Long-polls `GET /v1/runs/{id}/result?follow_continuations=true` until the chain's
    /// last run is terminal (or `limit`).
    pub async fn result(&self, run: &RunId, limit: Duration) -> Result<Outcome, String> {
        let deadline = Instant::now() + limit;
        loop {
            let v = self
                .get(
                    &format!("/v1/runs/{run}/result?wait=2s&follow_continuations=true"),
                    "result",
                )
                .await?;
            if v["completed"] == Value::Bool(true) {
                let last: RunId = v["run_id"]
                    .as_str()
                    .ok_or_else(|| format!("result without run_id: {v}"))?
                    .parse()
                    .map_err(|e| format!("result run id: {e:?}"))?;
                return Ok(Outcome {
                    run: last,
                    status: v["status"].as_str().unwrap_or("?").to_owned(),
                    result: v.get("result").cloned(),
                    failure: v.get("failure").cloned(),
                });
            }
            if Instant::now() > deadline {
                return Err(format!("run {run} not terminal within {limit:?}: {v}"));
            }
        }
    }

    /// Run ids listed by `GET /v1/runs?workflow=<workflow>` (all pages).
    pub async fn list_runs(&self, workflow: &str) -> Result<Vec<RunId>, String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut path = format!("/v1/runs?limit=1000&workflow={workflow}");
            if let Some(t) = &token {
                path.push_str(&format!("&page_token={t}"));
            }
            let v = self.get(&path, "list runs").await?;
            for r in v["runs"].as_array().into_iter().flatten() {
                let id = r["run_id"].as_str().unwrap_or_default();
                out.push(
                    id.parse()
                        .map_err(|e| format!("listed run id {id}: {e:?}"))?,
                );
            }
            match v["next_page_token"].as_str() {
                Some(t) => token = Some(t.to_owned()),
                None => return Ok(out),
            }
        }
    }

    // -- partition reads ----------------------------------------------------------------

    /// The run's journal, read from its partition.
    pub async fn history(&self, run: &RunId) -> Result<Vec<Event>, String> {
        let handle = self
            .h()
            .node()
            .handle(run.partition())
            .ok_or_else(|| format!("partition of {run} not ready"))?
            .clone();
        read_journal(&handle, run)
            .await
            .map_err(|e| format!("journal {run}: {e:?}"))
    }

    /// The run's record (`None` while it does not exist).
    pub fn info(&self, run: &RunId) -> Result<Option<RunInfo>, String> {
        let handle = self
            .h()
            .node()
            .handle(run.partition())
            .ok_or_else(|| format!("partition of {run} not ready"))?;
        let Some(rec) = handle
            .read_run(run)
            .map_err(|e| format!("read run {run}: {e:?}"))?
        else {
            return Ok(None);
        };
        let parent = match rec.parent {
            Some((bytes, op)) => Some((
                RunId::from_bytes(&bytes).map_err(|e| format!("parent id: {e:?}"))?,
                op,
            )),
            None => None,
        };
        Ok(Some(RunInfo {
            id: *run,
            workflow: rec.workflow.clone(),
            status: snake(&format!("{:?}", rec.status)),
            parent,
            continued_from: rec.continued_from,
            continued_to: rec.continued_to,
            cancel_reason: rec.cancel_reason.clone(),
        }))
    }

    /// The continue-as-new chain starting at `run` (ids, first first). `continued_to` is
    /// cleared once a child chain's `chain_done` arrives (09-08), so a `continued_as_new`
    /// run without it is followed through the listed run whose `continued_from` points back.
    pub async fn chain(&self, run: &RunId) -> Result<Vec<RunId>, String> {
        let mut out = vec![*run];
        let mut cur = *run;
        while let Some(info) = self.info(&cur)? {
            let next = match info.continued_to {
                Some(n) => Some(n),
                None if info.status == "continued_as_new" => self.successor(&info).await?,
                None => None,
            };
            let Some(next) = next else { break };
            if out.contains(&next) || out.len() > 100 {
                return Err(format!("continue-as-new chain of {run} loops"));
            }
            out.push(next);
            cur = next;
        }
        Ok(out)
    }

    async fn successor(&self, info: &RunInfo) -> Result<Option<RunId>, String> {
        for id in self.list_runs(&info.workflow).await? {
            if self
                .info(&id)?
                .is_some_and(|i| i.continued_from == Some(info.id))
            {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    async fn chain_journals(&self, run: &RunId) -> Result<Vec<Vec<Event>>, String> {
        let mut out = Vec::new();
        for r in self.chain(run).await? {
            out.push(self.history(&r).await?);
        }
        Ok(out)
    }

    /// Status of the last run of `run`'s chain (`None` while the run does not exist).
    pub async fn chain_status(&self, run: &RunId) -> Result<Option<String>, String> {
        let chain = self.chain(run).await?;
        let last = chain.last().expect("non-empty chain");
        Ok(self.info(last)?.map(|i| i.status))
    }

    /// Children started by `run` (its own journal), in journal order.
    pub async fn list_children(&self, run: &RunId) -> Result<Vec<ChildEdge>, String> {
        let j = self.history(run).await?;
        Ok(edges_of(run, &j, self.partitions))
    }

    /// The N-th (1-based) child of `parent`, waiting until it was started and exists.
    async fn nth_child(&self, parent: &RunId, n: usize, limit: Duration) -> Result<RunId, String> {
        let deadline = Instant::now() + limit;
        loop {
            let kids = self.list_children(parent).await?;
            if let Some(e) = kids.get(n - 1)
                && self.info(&e.child)?.is_some()
            {
                return Ok(e.child);
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "child #{n} of {parent} not started within {limit:?} ({} children)",
                    kids.len()
                ));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Resolves a target path (`root`, `child`, `child#2`, `child.child`).
    pub async fn resolve(&self, root: &RunId, path: &str) -> Result<RunId, String> {
        let mut cur = *root;
        for comp in path.split('.').filter(|c| !c.is_empty() && *c != "root") {
            let n = match comp.strip_prefix("child") {
                Some("") => 1,
                Some(rest) => rest
                    .strip_prefix('#')
                    .and_then(|n| n.parse::<usize>().ok())
                    .filter(|n| *n > 0)
                    .ok_or_else(|| format!("bad target component {comp:?} in {path:?}"))?,
                None => return Err(format!("bad target component {comp:?} in {path:?}")),
            };
            cur = self.nth_child(&cur, n, STEP_LIMIT).await?;
        }
        Ok(cur)
    }

    async fn wait_until_status(
        &self,
        run: &RunId,
        want: &str,
        limit: Duration,
    ) -> Result<String, String> {
        let deadline = Instant::now() + limit;
        loop {
            let st = self
                .chain_status(run)
                .await?
                .unwrap_or_else(|| "absent".into());
            if st == want || (want == "terminal" && is_terminal(&st)) {
                return Ok(st);
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "run {run}: status {st}, expected {want} within {limit:?}"
                ));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    async fn wait_fact(&self, run: &RunId, fact: &str, limit: Duration) -> Result<(), String> {
        let deadline = Instant::now() + limit;
        loop {
            let p = projection::project_chain(&self.chain_journals(run).await?);
            if p.facts.iter().any(|f| f == fact) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "run {run}: fact {fact} not seen within {limit:?}; facts {:?}",
                    p.facts
                ));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Records every child edge of the tree under `root` (for [`Self::check_children_unique`]).
    async fn record_tree(&self, root: &RunId) -> Result<(), String> {
        let mut todo = vec![*root];
        let mut seen = 0usize;
        while let Some(run) = todo.pop() {
            seen += 1;
            if seen > 200 {
                return Err("run tree too large".into());
            }
            for r in self.chain(&run).await? {
                for e in self.list_children(&r).await? {
                    if self.info(&e.child)?.is_some() {
                        todo.push(e.child);
                    }
                    let mut edges = self.edges.lock().unwrap_or_else(|p| p.into_inner());
                    if !edges.contains(&e) {
                        edges.push(e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Every child edge recorded so far.
    pub fn edges(&self) -> Vec<ChildEdge> {
        self.edges.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Exactly one child run per (parent, op), verified by listing runs per child workflow:
    /// the deterministic id is listed, and no other first-generation run carries that
    /// parent link. Returns the number of edges checked.
    pub async fn check_children_unique(&self) -> Result<usize, String> {
        let edges = self.edges();
        let mut by_wf: BTreeMap<String, Vec<RunInfo>> = BTreeMap::new();
        for e in &edges {
            if !by_wf.contains_key(&e.workflow) {
                let mut infos = Vec::new();
                for id in self.list_runs(&e.workflow).await? {
                    if let Some(i) = self.info(&id)? {
                        infos.push(i);
                    }
                }
                by_wf.insert(e.workflow.clone(), infos);
            }
        }
        let mut errors = Vec::new();
        for e in &edges {
            let listed = &by_wf[&e.workflow];
            let matching: Vec<&RunInfo> = listed
                .iter()
                .filter(|i| {
                    i.id == e.child
                        || (i.parent == Some((e.parent, e.op)) && i.continued_from.is_none())
                })
                .collect();
            if matching.len() != 1 || matching[0].id != e.child {
                errors.push(format!(
                    "(parent {}, op {}) {}: expected exactly the run {}, listed {:?}",
                    e.parent,
                    e.op,
                    e.workflow,
                    e.child,
                    matching
                        .iter()
                        .map(|i| i.id.to_string())
                        .collect::<Vec<_>>()
                ));
            }
        }
        if errors.is_empty() {
            Ok(edges.len())
        } else {
            Err(errors.join("\n"))
        }
    }

    // -- specs ------------------------------------------------------------------------

    /// Runs a server-leg spec with one language per role (`langs[0]` = root).
    pub async fn run_spec(&self, spec: &Spec, langs: &[Lang]) -> Result<RunReport, Failure> {
        let lang = langs.first().copied().unwrap_or(Lang::Rust);
        let fail = |message: String| Failure {
            id: spec.id.clone(),
            lang,
            message,
        };
        let started = Instant::now();
        let roles: Vec<Lang> = if langs.is_empty() {
            vec![Lang::Rust]
        } else {
            langs.to_vec()
        };
        for l in &roles {
            self.deploy_lang(*l).await.map_err(fail)?;
        }
        let known = ["descendants", "chain_runs", "fx_pay", "variants"];
        if let Some(k) = spec
            .expect
            .extra
            .keys()
            .find(|k| !known.contains(&k.as_str()))
        {
            return Err(fail(format!(
                "unknown expect key {k:?} (server leg knows {known:?})"
            )));
        }
        let main = self
            .run_case(
                &spec.workflow,
                &spec.input,
                &spec.script,
                &spec.expect,
                &roles,
            )
            .await
            .map_err(|e| fail(e.clone()))?;
        if let Some(vs) = spec.expect.extra.get("variants") {
            let vs = vs
                .as_array()
                .ok_or_else(|| fail("variants: expected a list".into()))?;
            for (i, v) in vs.iter().enumerate() {
                let title = v["title"].as_str().unwrap_or("?");
                let ctx = |e: String| fail(format!("variant {i} ({title}): {e}"));
                let script: Vec<ScriptStep> =
                    serde_json::from_value(v.get("script").cloned().unwrap_or(json!([])))
                        .map_err(|e| ctx(format!("script: {e}")))?;
                let expect: Expect = serde_json::from_value(v["expect"].clone())
                    .map_err(|e| ctx(format!("expect: {e}")))?;
                if expect.extra.contains_key("variants") {
                    return Err(ctx("nested variants".into()));
                }
                let input = v.get("input").cloned().unwrap_or(Value::Null);
                self.run_case(&spec.workflow, &input, &script, &expect, &roles)
                    .await
                    .map_err(ctx)?;
            }
        }
        Ok(RunReport {
            id: spec.id.clone(),
            lang,
            status: main.outcome.status,
            result: main.outcome.result,
            failure_kind: main.failure_kind,
            projection: main.projection,
            journals: main.journals,
            activations: 0,
            elapsed: started.elapsed(),
        })
    }

    async fn run_case(
        &self,
        workflow: &str,
        input: &Value,
        script: &[ScriptStep],
        expect: &Expect,
        roles: &[Lang],
    ) -> Result<Case, String> {
        let input = substitute(input, roles, &self.mock.base_url());
        let wf = langs::workflow_name(roles[0], workflow);
        let mut root = self.start_run(&wf, &input).await?;
        for (i, step) in script.iter().enumerate() {
            self.step(&mut root, step)
                .await
                .map_err(|e| format!("script step {i} {step:?}: {e}"))?;
        }
        let outcome = self.result(&root, RUN_LIMIT).await?;
        let journals = self.chain_journals(&root).await?;
        let proj = self.normalise(projection::project_chain(&journals));
        let failure_kind = journals
            .last()
            .and_then(|j| j.last())
            .and_then(|e| match &e.body {
                EventBody::RunFailed(f) => Some(projection::failure_kind(&f.failure)),
                _ => None,
            });
        self.record_tree(&root).await?;
        let context = || {
            format!(
                "\n  root {root} status {} result {} failure_kind {failure_kind:?}\n  projection {:?}\n  values {:?}\n  inputs {:?}",
                outcome.status,
                outcome
                    .result
                    .as_ref()
                    .map_or_else(|| "-".to_string(), canon::canonical),
                proj.facts,
                proj.values,
                proj.inputs,
            )
        };
        let ctx = |e: String| format!("{e}{}", context());
        if expect.status != outcome.status {
            return Err(ctx(format!(
                "status: expected {}, actual {}",
                expect.status, outcome.status
            )));
        }
        if let Some(want) = &expect.result {
            let want = self.expand_refs(&root, want).await?;
            let got = outcome.result.clone().unwrap_or(Value::Null);
            compare::outcomes("result", &want, &got).map_err(|d| ctx(d.to_string()))?;
        }
        if let Some(want) = &expect.failure_kind
            && failure_kind.as_deref() != Some(want.as_str())
        {
            return Err(ctx(format!(
                "failure_kind: expected {want}, actual {failure_kind:?}"
            )));
        }
        if let Some(want) = &expect.projection {
            compare::facts(want, &proj.facts).map_err(|d| ctx(d.to_string()))?;
        }
        for j in &journals {
            once_per_op(j).map_err(ctx)?;
        }
        if let Some(n) = expect.extra.get("chain_runs") {
            let want = n
                .as_u64()
                .ok_or_else(|| ctx("chain_runs: expected a number".into()))?;
            if journals.len() as u64 != want {
                return Err(ctx(format!(
                    "chain_runs: expected {want}, actual {}",
                    journals.len()
                )));
            }
            if want > 1 && outcome.run == root {
                return Err(ctx("result long-poll did not follow the chain".into()));
            }
        }
        if let Some(fx) = expect.extra.get("fx_pay") {
            self.check_fx_pay(&root, fx, outcome.result.as_ref())
                .map_err(ctx)?;
        }
        if let Some(d) = expect.extra.get("descendants") {
            let d = d
                .as_object()
                .ok_or_else(|| ctx("descendants: expected an object".into()))?;
            for (path, want) in d {
                self.check_descendant(&root, path, want)
                    .await
                    .map_err(|e| ctx(format!("descendant {path}: {e}")))?;
            }
            // Descendants may have started more children while being checked.
            self.record_tree(&root).await?;
        }
        Ok(Case {
            outcome,
            failure_kind,
            projection: proj,
            journals,
        })
    }

    async fn step(&self, root: &mut RunId, step: &ScriptStep) -> Result<(), String> {
        match step {
            ScriptStep::Signal {
                name,
                payload,
                before_start: false,
            } => self.signal(root, name, payload).await,
            ScriptStep::Cancel {} => self.cancel(root, None).await,
            ScriptStep::Api { op, args } => self.api(root, op, args).await,
            other => Err(format!("not a server-leg step: {other:?}")),
        }
    }

    async fn api(
        &self,
        root: &mut RunId,
        op: &str,
        args: &Map<String, Value>,
    ) -> Result<(), String> {
        let target_path = args.get("target").and_then(Value::as_str).unwrap_or("root");
        let str_arg = |k: &str| {
            args.get(k)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{op}: missing string field {k:?}"))
        };
        let reason = args.get("reason").and_then(Value::as_str);
        let payload = args.get("payload").cloned().unwrap_or(Value::Null);
        match op {
            "sleep" => {
                let ms = args
                    .get("ms")
                    .and_then(Value::as_u64)
                    .ok_or("sleep: missing ms")?;
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(())
            }
            "signal" => {
                let t = self.resolve(root, target_path).await?;
                self.signal(&t, str_arg("name")?, &payload).await
            }
            "token" => {
                let t = self.resolve(root, target_path).await?;
                let token = self.published_token(&t, STEP_LIMIT).await?;
                self.tokens
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(token.clone());
                self.complete_token(&token, &payload).await
            }
            "cancel" => {
                let t = self.resolve(root, target_path).await?;
                self.cancel(&t, reason).await
            }
            "terminate" => {
                let t = self.resolve(root, target_path).await?;
                self.terminate(&t, reason).await
            }
            "retry" => {
                let t = self.resolve(root, target_path).await?;
                let before = self.chain_status(&t).await?.unwrap_or_default();
                let new = self.retry(&t).await?;
                if t == *root {
                    *root = new;
                }
                let deadline = Instant::now() + STEP_LIMIT;
                while self.chain_status(&new).await?.unwrap_or_default() == before {
                    if Instant::now() > deadline {
                        return Err(format!("retry: {new} still {before}"));
                    }
                    tokio::time::sleep(POLL).await;
                }
                Ok(())
            }
            "wait_fact" => {
                let t = self.resolve(root, target_path).await?;
                self.wait_fact(&t, str_arg("fact")?, STEP_LIMIT).await
            }
            "wait_status" => {
                let t = self.resolve(root, target_path).await?;
                self.wait_until_status(&t, str_arg("status")?, STEP_LIMIT)
                    .await
                    .map(drop)
            }
            "assert_live" => {
                let t = self.resolve(root, target_path).await?;
                let st = self
                    .chain_status(&t)
                    .await?
                    .unwrap_or_else(|| "absent".into());
                if is_terminal(&st) || st == "absent" {
                    Err(format!("{target_path} ({t}) is {st}, expected live"))
                } else {
                    Ok(())
                }
            }
            other => Err(format!("unknown api op {other:?}")),
        }
    }

    /// Replaces API-completed token strings in facts with `<token>` (a token published
    /// with `set_status` is per run).
    fn normalise(&self, mut p: Projection) -> Projection {
        let tokens = self
            .tokens
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        for f in &mut p.facts {
            for t in &tokens {
                if f.contains(t.as_str()) {
                    *f = f.replace(t.as_str(), "<token>");
                }
            }
        }
        p
    }

    /// Replaces `"@<path>"` strings with the resolved run ids.
    async fn expand_refs(&self, root: &RunId, v: &Value) -> Result<Value, String> {
        let mut paths = Vec::new();
        collect_refs(v, &mut paths);
        let mut ids = HashMap::new();
        for p in paths {
            let id = self.resolve(root, &p).await?;
            ids.insert(p, id.to_string());
        }
        Ok(replace_refs(v, &ids))
    }

    fn check_fx_pay(&self, root: &RunId, fx: &Value, result: Option<&Value>) -> Result<(), String> {
        let name = fx["key"].as_str().ok_or("fx_pay: missing key")?;
        let key = idempotency_key_for(&root.to_string(), name);
        let result = result.ok_or("fx_pay: no result")?;
        if let Some(st) = fx.get("status")
            && result.get("status") != Some(st)
        {
            return Err(format!("fx_pay: status {} expected {st}", result["status"]));
        }
        if result["body"]["key"].as_str() != Some(key.as_str()) {
            return Err(format!(
                "fx_pay: provider saw key {} expected {key} (idempotency_key_for({root}, {name:?}))",
                result["body"]["key"]
            ));
        }
        let (req, charges) = (self.mock.requests(&key), self.mock.charges(&key));
        if req != 1 || charges != 1 {
            return Err(format!(
                "fx_pay: key {key}: {req} requests, {charges} charges (expected 1 / 1)"
            ));
        }
        Ok(())
    }

    async fn check_descendant(&self, root: &RunId, path: &str, want: &Value) -> Result<(), String> {
        let id = self.resolve(root, path).await?;
        let mut status = self
            .chain_status(&id)
            .await?
            .unwrap_or_else(|| "absent".into());
        if let Some(st) = want.get("status").and_then(Value::as_str) {
            if is_terminal(st) {
                status = self
                    .wait_until_status(&id, "terminal", DESCENDANT_LIMIT)
                    .await?;
            }
            if status != st {
                return Err(format!("{id}: status {status}, expected {st}"));
            }
        }
        let journals = self.chain_journals(&id).await?;
        let proj = self.normalise(projection::project_chain(&journals));
        for j in &journals {
            once_per_op(j)?;
        }
        if let Some(p) = want.get("projection") {
            let p: Vec<String> =
                serde_json::from_value(p.clone()).map_err(|e| format!("projection: {e}"))?;
            compare::facts(&p, &proj.facts).map_err(|d| format!("{id}: {d}"))?;
        }
        if let Some(r) = want.get("cancel_reason").and_then(Value::as_str) {
            let got = journals
                .iter()
                .flatten()
                .find_map(|e| match &e.body {
                    EventBody::CancelRequested(c) => Some(c.reason.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            if got != r {
                return Err(format!("{id}: cancel reason {got:?}, expected {r:?}"));
            }
        }
        if let Some(p) = want.get("parent").and_then(Value::as_bool) {
            let info = self.info(&id)?.ok_or_else(|| format!("{id} vanished"))?;
            // `GET /v1/runs/{id}` answers for it either way.
            self.get(&format!("/v1/runs/{id}"), "get descendant")
                .await?;
            match (p, info.parent) {
                (false, None) => {}
                (false, Some(pr)) => return Err(format!("{id}: unexpected parent link {pr:?}")),
                (true, None) => return Err(format!("{id}: no parent link")),
                (true, Some((parent, op))) => {
                    let ok = self
                        .list_children(&parent)
                        .await?
                        .iter()
                        .any(|e| e.op == op && e.child == id);
                    if !ok {
                        return Err(format!(
                            "{id}: parent link ({parent}, {op}) has no matching ChildStarted"
                        ));
                    }
                }
            }
        }
        if let Some(r) = want.get("result") {
            let last = *self.chain(&id).await?.last().expect("chain");
            let v = self
                .get(&format!("/v1/runs/{last}"), "get descendant")
                .await?;
            compare::outcomes("descendant result", r, &v["result"]).map_err(|d| d.to_string())?;
        }
        Ok(())
    }
}

struct Case {
    outcome: Outcome,
    failure_kind: Option<String>,
    projection: Projection,
    journals: Vec<Vec<Event>>,
}

/// Starts a fresh harness, runs `spec` with the given role languages and shuts down.
pub async fn run_server_spec(spec: &Spec, langs: &[Lang]) -> Result<RunReport, Failure> {
    let lang = langs.first().copied().unwrap_or(Lang::Rust);
    let fail = |message: String| Failure {
        id: spec.id.clone(),
        lang,
        message,
    };
    let h = ServerHarness::start().await.map_err(fail)?;
    let r = h.run_spec(spec, langs).await;
    let u = h.check_children_unique().await.map_err(fail);
    h.shutdown().await.map_err(fail)?;
    let r = r?;
    u?;
    Ok(r)
}

/// Child starts recorded in one journal.
pub fn edges_of(parent: &RunId, journal: &[Event], partitions: u16) -> Vec<ChildEdge> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::ChildStarted(c) => Some(ChildEdge {
                parent: *parent,
                op: e.seq,
                workflow: c.workflow.clone(),
                detached: c.options.detached,
                child: child_run_id(parent, e.seq, partitions),
            }),
            _ => None,
        })
        .collect()
}

/// At most one `ChildCompleted` per op in a journal.
pub fn once_per_op(journal: &[Event]) -> Result<(), String> {
    let mut seen: HashMap<u64, u32> = HashMap::new();
    for e in journal {
        if let EventBody::ChildCompleted(c) = &e.body {
            let n = seen.entry(c.op).or_default();
            *n += 1;
            if *n > 1 {
                return Err(format!(
                    "two ChildCompleted for op {} (seq {})",
                    c.op, e.seq
                ));
            }
        }
    }
    Ok(())
}

/// Rewrites `wf:` / `wf<N>:` / `$MOCK` strings (module docs).
pub fn substitute(v: &Value, roles: &[Lang], mock_base: &str) -> Value {
    match v {
        Value::String(s) => Value::String(substitute_str(s, roles, mock_base)),
        Value::Array(a) => {
            Value::Array(a.iter().map(|x| substitute(x, roles, mock_base)).collect())
        }
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, x)| (k.clone(), substitute(x, roles, mock_base)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn substitute_str(s: &str, roles: &[Lang], mock_base: &str) -> String {
    if let Some(rest) = s.strip_prefix("$MOCK") {
        return format!("{mock_base}{rest}");
    }
    if let Some(rest) = s.strip_prefix("wf")
        && let Some((n, base)) = rest.split_once(':')
        && (n.is_empty() || n.bytes().all(|b| b.is_ascii_digit()))
    {
        let role: usize = if n.is_empty() {
            1
        } else {
            n.parse().unwrap_or(1)
        };
        let lang = roles
            .get(role)
            .or(roles.last())
            .copied()
            .unwrap_or(Lang::Rust);
        return langs::workflow_name(lang, base);
    }
    s.to_owned()
}

fn collect_refs(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if let Some(p) = s.strip_prefix('@') {
                out.push(p.to_owned());
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_refs(x, out)),
        Value::Object(o) => o.values().for_each(|x| collect_refs(x, out)),
        _ => {}
    }
}

fn replace_refs(v: &Value, ids: &HashMap<String, String>) -> Value {
    match v {
        Value::String(s) => s
            .strip_prefix('@')
            .and_then(|p| ids.get(p))
            .map_or_else(|| v.clone(), |id| Value::String(id.clone())),
        Value::Array(a) => Value::Array(a.iter().map(|x| replace_refs(x, ids)).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, x)| (k.clone(), replace_refs(x, ids)))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_matches_the_cross_sdk_vectors() {
        let path = crate::repo_root().join("conformance/vectors/idempotency.json");
        let v: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let cases = v["cases"].as_array().unwrap();
        assert!(!cases.is_empty());
        for c in cases {
            let got = idempotency_key_for(
                c["input"]["run_id"].as_str().unwrap(),
                c["input"]["name"].as_str().unwrap(),
            );
            assert_eq!(got, c["expected"].as_str().unwrap(), "{c}");
        }
    }

    #[test]
    fn substitution_by_role() {
        let roles = [Lang::Ts, Lang::Py, Lang::Rust];
        let v = substitute(
            &json!({"next": ["wf:conf_parent_chain", "wf2:conf_leaf_sleep"], "url": "$MOCK/pay",
                    "plain": "wfx", "n": 1}),
            &roles,
            "http://127.0.0.1:9",
        );
        assert_eq!(
            v,
            json!({"next": ["py_conf_parent_chain", "rs_conf_leaf_sleep"],
                   "url": "http://127.0.0.1:9/pay", "plain": "wfx", "n": 1})
        );
        // Missing roles reuse the last language.
        assert_eq!(substitute_str("wf3:x", &[Lang::Rust], ""), "rs_x");
    }

    #[test]
    fn status_spelling() {
        assert_eq!(snake("TimedOut"), "timed_out");
        assert_eq!(snake("ContinuedAsNew"), "continued_as_new");
        assert_eq!(snake("Running"), "running");
    }

    #[test]
    fn refs() {
        let mut ids = HashMap::new();
        ids.insert("child".to_string(), "X".to_string());
        assert_eq!(
            replace_refs(&json!({"run_id": "@child", "k": ["@child", "c"]}), &ids),
            json!({"run_id": "X", "k": ["X", "c"]})
        );
    }
}
