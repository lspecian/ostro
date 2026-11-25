//! 13-12 unit tests of the router (`ostro_server::route`) over fake inner / peer control
//! planes, a fake assignment view and recording consistency hooks.
//!
//! - `every_trait_method_classified`: every method of `ControlPlane` and of every extension
//!   trait (parsed from the ostro-api sources) has a route class, and the table names no
//!   method that does not exist.
//! - Local branch hooks (plan check B4), forwarding with `hop = 1`, never forwarding a
//!   forwarded-in call, one re-route on `not_leader` / `leader_changed` / transport failure,
//!   503 + `retry_after_ms` after it, offline 503, unkeyed placement and the pin rule.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_api::domain::*;
use ostro_api::request_meta::{self, RequestMeta};
use ostro_api::{ApiError, ApiExtensions, ControlPlane, ErrorCode};
use ostro_server::route::peer::PeerApi;
use ostro_server::route::table::{self, RouteClass, Target};
use ostro_server::route::{BoxFuture, RouteCore, RouteEnv, RoutedControlPlane};
use ostro_types::{Epoch, PartitionCount, PartitionId};

// ---------------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------------

/// `fn` names declared inside `pub trait <name>` blocks of `src`.
fn trait_methods(src: &str, name: &str) -> Vec<String> {
    let start = src
        .find(&format!("pub trait {name}"))
        .unwrap_or_else(|| panic!("trait {name} not found"));
    let body = &src[start..];
    let open = body.find('{').expect("trait body");
    let mut depth = 0usize;
    let mut end = 0;
    for (i, c) in body[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = open + i;
                    break;
                }
            }
            _ => {}
        }
    }
    let body = &body[open..end];
    let mut out = Vec::new();
    let mut depth = 0usize;
    for line in body.lines() {
        let t = line.trim_start();
        // Only declarations at the trait's top level (depth 1 after the opening brace).
        if depth == 1
            && !t.starts_with("//")
            && let Some(rest) = t
                .strip_prefix("async fn ")
                .or_else(|| t.strip_prefix("fn "))
        {
            let n: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            out.push(n);
        }
        for c in line.chars() {
            match c {
                '{' => depth += 1,
                '}' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    out
}

#[test]
fn every_trait_method_classified() {
    let sources: [(&str, &str, &[&str]); 4] = [
        (
            "domain.rs",
            include_str!("../../ostro-api/src/domain.rs"),
            &["ControlPlane"],
        ),
        (
            "phase8.rs",
            include_str!("../../ostro-api/src/phase8.rs"),
            &[
                "ScheduleApi",
                "WebhookApi",
                "HookIngress",
                "RunLogsApi",
                "AdminApi",
            ],
        ),
        (
            "phase11.rs",
            include_str!("../../ostro-api/src/phase11.rs"),
            &["OpsApi", "InventoryApi"],
        ),
        (
            "phase13.rs",
            include_str!("../../ostro-api/src/phase13.rs"),
            &["ClusterApi", "ModuleLifecycleApi"],
        ),
    ];
    let mut declared = BTreeSet::new();
    let mut missing = Vec::new();
    for (file, src, traits) in sources {
        for t in traits {
            let methods = trait_methods(src, t);
            assert!(!methods.is_empty(), "{file}: no methods parsed for {t}");
            for m in methods {
                if table::class_of(t, &m).is_none() {
                    missing.push(format!("{t}::{m} ({file})"));
                }
                declared.insert((t.to_string(), m));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "methods without a route class: {missing:?}"
    );
    let stale: Vec<String> = table::ROUTES
        .iter()
        .filter(|(t, m, _)| !declared.contains(&(t.to_string(), m.to_string())))
        .map(|(t, m, _)| format!("{t}::{m}"))
        .collect();
    assert!(
        stale.is_empty(),
        "route table names unknown methods: {stale:?}"
    );
    // No duplicate rows.
    let mut seen = BTreeSet::new();
    for (t, m, _) in table::ROUTES {
        assert!(seen.insert((*t, *m)), "duplicate route row {t}::{m}");
    }
    // Spot checks against the 13-01 table.
    assert_eq!(
        table::class_of("ControlPlane", "get_run"),
        Some(RouteClass::RunPartition)
    );
    assert_eq!(
        table::class_of("ControlPlane", "complete_token"),
        Some(RouteClass::TokenPartition)
    );
    assert_eq!(
        table::class_of("ControlPlane", "list_runs"),
        Some(RouteClass::FanoutPartitions)
    );
    assert_eq!(
        table::class_of("ClusterApi", "upgrade_finalize"),
        Some(RouteClass::MetaLeader)
    );
    assert_eq!(
        table::class_of("ControlPlane", "upload_module"),
        Some(RouteClass::Local)
    );
}

// ---------------------------------------------------------------------------------
// Owner resolution and pins
// ---------------------------------------------------------------------------------

fn view(p: u16, leader: u64, epoch: u32) -> ostro_partition::roles::PartitionAssignmentView {
    ostro_partition::roles::PartitionAssignmentView {
        partition: PartitionId(p),
        leader,
        epoch: Epoch(epoch),
        replicas: vec![1, 2, 3],
        isr: vec![1, 2, 3],
        isr_version: 0,
        min_insync: 2,
    }
}

#[test]
fn owner_resolution() {
    let v = vec![view(0, 1, 3), view(1, 2, 4), view(2, 0, 5), view(3, 1, 6)];
    // Led here.
    let led = [(PartitionId(0), Epoch(3))];
    assert_eq!(
        table::owner(PartitionId(0), 1, &led, &v),
        (Target::Local, Epoch(3))
    );
    // Another leader.
    assert_eq!(
        table::owner(PartitionId(1), 1, &led, &v),
        (Target::Node(2), Epoch(4))
    );
    // Offline.
    assert_eq!(
        table::owner(PartitionId(2), 1, &led, &v),
        (Target::Offline(PartitionId(2)), Epoch(5))
    );
    // Published leader is this node but not led yet (taking over): local.
    assert_eq!(
        table::owner(PartitionId(3), 1, &led, &v),
        (Target::Local, Epoch(6))
    );
    // No row: local (the local call answers).
    assert_eq!(table::owner(PartitionId(9), 1, &led, &v).0, Target::Local);
}

#[tokio::test]
async fn pin_is_honoured_only_from_node_identities() {
    assert_eq!(table::pinned_partition(), None);
    let client = RequestMeta {
        partition_pin: Some(3),
        ..RequestMeta::default()
    };
    assert_eq!(
        request_meta::scope(client, async { table::pinned_partition() }).await,
        None,
        "a client-sent ostro-partition is ignored"
    );
    let node = RequestMeta {
        partition_pin: Some(3),
        from_node: Some(2),
        ..RequestMeta::default()
    };
    assert_eq!(
        request_meta::scope(node, async { table::pinned_partition() }).await,
        Some(PartitionId(3))
    );
    let count = PartitionCount::new(4).unwrap();
    let leg = RequestMeta {
        partitions: Some(vec![3, 1, 1, 9]),
        from_node: Some(2),
        ..RequestMeta::default()
    };
    assert_eq!(
        request_meta::scope(leg.clone(), async { table::listed_partitions(count) }).await,
        vec![PartitionId(1), PartitionId(3)]
    );
    let not_node = RequestMeta {
        from_node: None,
        ..leg
    };
    assert_eq!(
        request_meta::scope(not_node, async { table::listed_partitions(count) })
            .await
            .len(),
        4
    );
}

// ---------------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------------

type Log = Arc<Mutex<Vec<String>>>;

fn push(log: &Log, s: String) {
    log.lock().unwrap().push(s);
}

fn snapshot(log: &Log) -> Vec<String> {
    log.lock().unwrap().clone()
}

/// A control plane of node `node` that records every call (with the request metadata it
/// saw) and answers from a per-method script (default: success where cheap, else
/// `not_found`).
struct FakeCp {
    node: u64,
    log: Log,
    script: Mutex<HashMap<&'static str, Vec<ApiError>>>,
    page: Mutex<Option<RunPage>>,
    workers: Vec<&'static str>,
}

impl FakeCp {
    fn new(node: u64, log: &Log) -> Arc<Self> {
        Arc::new(Self {
            node,
            log: Arc::clone(log),
            script: Mutex::new(HashMap::new()),
            page: Mutex::new(None),
            workers: vec![],
        })
    }

    fn with_workers(node: u64, log: &Log, workers: Vec<&'static str>) -> Arc<Self> {
        Arc::new(Self {
            node,
            log: Arc::clone(log),
            script: Mutex::new(HashMap::new()),
            page: Mutex::new(None),
            workers,
        })
    }

    fn set_page(&self, p: RunPage) {
        *self.page.lock().unwrap() = Some(p);
    }

    /// The next answers of `method` are these errors (then success).
    fn fail(&self, method: &'static str, errs: Vec<ApiError>) {
        self.script.lock().unwrap().insert(method, errs);
    }

    fn call(&self, method: &'static str) -> Result<(), ApiError> {
        let m = RequestMeta::current().unwrap_or_default();
        push(
            &self.log,
            format!(
                "call node={} {method} hop={} pin={:?} from={:?} parts={:?}",
                self.node,
                m.hop,
                table::pinned_partition().map(|p| p.0),
                m.from_node,
                m.partitions
            ),
        );
        let mut s = self.script.lock().unwrap();
        match s.get_mut(method) {
            Some(v) if !v.is_empty() => Err(v.remove(0)),
            _ => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl ControlPlane for FakeCp {
    async fn upload_module(&self, _req: UploadModule) -> Result<UploadModuleResult, ApiError> {
        self.call("upload_module")?;
        Err(ApiError::not_found("fake"))
    }
    async fn get_module(&self, _sha256: String) -> Result<ModuleInfo, ApiError> {
        self.call("get_module")?;
        Err(ApiError::not_found("fake module"))
    }
    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        self.call("list_workflows")?;
        Ok(vec![])
    }
    async fn activate_workflow(&self, _req: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        self.call("activate_workflow")?;
        Err(ApiError::not_found("fake"))
    }
    async fn start_run(&self, _req: StartRunRequest) -> Result<StartRunResult, ApiError> {
        self.call("start_run")?;
        Ok(StartRunResult {
            run_id: format!(
                "node{}:pin{:?}",
                self.node,
                table::pinned_partition().map(|p| p.0)
            ),
            created: true,
            dry_run: false,
        })
    }
    async fn get_run(&self, _run_id: String) -> Result<RunInfo, ApiError> {
        self.call("get_run")?;
        Err(ApiError::not_found(format!(
            "fake run on node {}",
            self.node
        )))
    }
    async fn run_history(&self, _req: HistoryRequest) -> Result<HistoryPage, ApiError> {
        self.call("run_history")?;
        Err(ApiError::not_found("fake"))
    }
    async fn list_runs(&self, _req: ListRunsRequest) -> Result<RunPage, ApiError> {
        self.call("list_runs")?;
        Ok(self.page.lock().unwrap().clone().unwrap_or(RunPage {
            runs: vec![],
            next_page_token: None,
        }))
    }
    fn ready(&self) -> bool {
        true
    }
    async fn cancel_run(&self, _r: CancelRequest) -> Result<CancelResponse, ApiError> {
        self.call("cancel_run")?;
        request_meta::set_consistency(format!("peer{}", self.node));
        Ok(CancelResponse {
            already_requested: false,
            dry_run: false,
        })
    }
    async fn complete_token(
        &self,
        r: CompleteTokenRequest,
    ) -> Result<CompleteTokenResponse, ApiError> {
        self.call("complete_token")?;
        Ok(CompleteTokenResponse {
            run_id: r.token,
            accepted: true,
            dry_run: false,
        })
    }
    async fn delete_secret(&self, _name: String, _tenant: Option<String>) -> Result<(), ApiError> {
        self.call("delete_secret")
    }
    async fn list_workers(&self) -> Result<Vec<ostro_api::phase7::WorkerInfo>, ApiError> {
        self.call("list_workers")?;
        Ok(self
            .workers
            .iter()
            .map(|w| ostro_api::phase7::WorkerInfo {
                worker_id: (*w).to_owned(),
                labels: Default::default(),
                slots: 1,
                free_slots: 1,
                leases: 0,
                connected_at_ms: 0,
                node_id: self.node,
                draining: false,
            })
            .collect())
    }
    async fn drain_worker(
        &self,
        worker_id: String,
    ) -> Result<ostro_api::phase7::DrainResult, ApiError> {
        self.call("drain_worker")?;
        if self.workers.contains(&worker_id.as_str()) {
            Ok(ostro_api::phase7::DrainResult {
                worker_id,
                already_draining: false,
            })
        } else {
            Err(ApiError::not_found(format!("worker {worker_id}")))
        }
    }
}

/// The node as the router sees it: a scripted owner map (replaced by `after_wait` on the
/// first wait), led set, peers and recording hooks.
struct FakeEnv {
    me: u64,
    count: PartitionCount,
    owners: Mutex<BTreeMap<u16, (Target, Epoch)>>,
    after_wait: Mutex<Option<BTreeMap<u16, (Target, Epoch)>>>,
    led: Vec<PartitionId>,
    unkeyed_local: bool,
    pick: usize,
    peers: HashMap<u64, Arc<FakeCp>>,
    unreachable: Vec<u64>,
    log: Log,
    /// 13-18: what `migrate_gate` answers (`None`: open).
    gate: Option<ApiError>,
}

impl FakeEnv {
    fn new(me: u64, log: &Log) -> Self {
        Self {
            me,
            count: PartitionCount::new(4).unwrap(),
            owners: Mutex::new(BTreeMap::new()),
            after_wait: Mutex::new(None),
            led: vec![],
            unkeyed_local: true,
            pick: 0,
            peers: HashMap::new(),
            unreachable: vec![],
            log: Arc::clone(log),
            gate: None,
        }
    }

    fn own(self, p: u16, t: Target, e: u32) -> Self {
        self.owners.lock().unwrap().insert(p, (t, Epoch(e)));
        self
    }

    fn then(self, p: u16, t: Target, e: u32) -> Self {
        self.after_wait
            .lock()
            .unwrap()
            .get_or_insert_with(BTreeMap::new)
            .insert(p, (t, Epoch(e)));
        self
    }

    fn peer(mut self, p: Arc<FakeCp>) -> Self {
        self.peers.insert(p.node, p);
        self
    }
}

impl RouteEnv for FakeEnv {
    fn local_node(&self) -> u64 {
        self.me
    }
    fn partition_count(&self) -> PartitionCount {
        self.count
    }
    fn owner(&self, p: PartitionId) -> (Target, Epoch) {
        self.owners
            .lock()
            .unwrap()
            .get(&p.0)
            .copied()
            .unwrap_or((Target::Local, Epoch(1)))
    }
    fn led(&self) -> Vec<PartitionId> {
        self.led.clone()
    }
    fn members(&self) -> Vec<u64> {
        let mut m: Vec<u64> = self.peers.keys().copied().collect();
        m.extend(&self.unreachable);
        m.sort_unstable();
        m
    }
    fn meta_leader(&self) -> Option<u64> {
        Some(self.me)
    }
    fn unkeyed_local(&self) -> bool {
        self.unkeyed_local
    }
    fn uniform(&self, n: usize) -> usize {
        self.pick % n
    }
    fn retry_wait(&self) -> Duration {
        Duration::from_millis(50)
    }
    fn wait_change(
        &self,
        p: PartitionId,
        seen: (Target, Epoch),
        limit: Duration,
    ) -> BoxFuture<'_, ()> {
        push(
            &self.log,
            format!(
                "wait p={} seen={seen:?} limit_ms={}",
                p.0,
                limit.as_millis()
            ),
        );
        if let Some(next) = self.after_wait.lock().unwrap().take() {
            self.owners.lock().unwrap().extend(next);
        }
        Box::pin(async {})
    }
    fn peer(&self, node: u64) -> Result<PeerApi, ApiError> {
        if self.unreachable.contains(&node) {
            return Err(ApiError::unavailable(format!("node {node} unreachable"))
                .with_details(serde_json::json!({ "reason": "peer_unreachable" })));
        }
        let cp = self
            .peers
            .get(&node)
            .cloned()
            .ok_or_else(|| ApiError::unavailable(format!("no peer {node}")))?;
        Ok(PeerApi {
            cp,
            ext: ApiExtensions::default(),
        })
    }
    fn forget_peer(&self, node: u64) {
        push(&self.log, format!("forget {node}"));
    }
    fn before_partition_read(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>> {
        push(&self.log, format!("hook before_partition_read({})", p.0));
        Box::pin(async { Ok(()) })
    }
    fn before_partition_write(&self, p: PartitionId) -> BoxFuture<'_, Result<(), ApiError>> {
        push(&self.log, format!("hook before_partition_write({})", p.0));
        Box::pin(async { Ok(()) })
    }
    fn before_meta_read(&self) -> BoxFuture<'_, Result<(), ApiError>> {
        push(&self.log, "hook before_meta_read".into());
        Box::pin(async { Ok(()) })
    }
    fn token_after_partition_write(&self, p: PartitionId) -> Option<String> {
        push(
            &self.log,
            format!("hook token_after_partition_write({})", p.0),
        );
        Some(format!("p{}:1:9", p.0))
    }
    fn token_after_meta_write(&self) -> Option<String> {
        push(&self.log, "hook token_after_meta_write".into());
        Some("m:7".into())
    }
    fn migrate_gate(&self) -> BoxFuture<'_, Result<(), ApiError>> {
        push(&self.log, "hook migrate_gate".into());
        let r = self.gate.clone().map_or(Ok(()), Err);
        Box::pin(async move { r })
    }
}

fn routed(env: FakeEnv, inner: Arc<FakeCp>) -> (RoutedControlPlane, Arc<RouteCore>) {
    let core = Arc::new(RouteCore::new(Arc::new(env)));
    (RoutedControlPlane::new(Arc::clone(&core), inner), core)
}

/// A run id of partition `p`.
fn run_of(p: u16) -> String {
    ostro_types::RunId::new_v7(PartitionId(p)).to_string()
}

fn not_leader(p: u16, leader: u64) -> ApiError {
    ostro_server::control::not_leader_error(PartitionId(p), Some(leader), None)
}

fn client_meta() -> RequestMeta {
    RequestMeta::default()
}

fn forwarded_meta() -> RequestMeta {
    RequestMeta {
        hop: 1,
        from_node: Some(2),
        ..RequestMeta::default()
    }
}

fn cancel(run: &str) -> CancelRequest {
    CancelRequest {
        run_id: run.to_owned(),
        reason: None,
        dry_run: false,
    }
}

// ---------------------------------------------------------------------------------
// Local branch hooks (plan check B4)
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn local_branch_calls_consistency_hooks() {
    for meta in [client_meta(), forwarded_meta()] {
        let label = if meta.hop == 0 {
            "direct"
        } else {
            "forwarded-in"
        };
        let log: Log = Arc::default();
        let inner = FakeCp::new(1, &log);
        // Partition 1 is led here; partition 2 by node 3 (a forwarded-in call still runs
        // locally).
        let env = FakeEnv::new(1, &log)
            .own(1, Target::Local, 4)
            .own(2, Target::Node(3), 4);
        let (cp, _) = routed(env, inner);
        let p = if meta.hop == 0 { 1 } else { 2 };

        // Partition read: the hook before the inner call; no token.
        let (r, token) =
            request_meta::scope_with_consistency(meta.clone(), cp.get_run(run_of(p))).await;
        assert_eq!(r.unwrap_err().code, ErrorCode::NotFound, "{label}");
        assert_eq!(token, None, "{label}");
        let l = snapshot(&log);
        assert_eq!(
            l[0],
            format!("hook before_partition_read({p})"),
            "{label}: {l:?}"
        );
        assert!(l[1].starts_with("call node=1 get_run"), "{label}: {l:?}");

        // Partition write: the write hook (leading + lease, p13-fixes) before the inner
        // call, the token after a successful inner call.
        log.lock().unwrap().clear();
        let run = run_of(p);
        let (r, token) =
            request_meta::scope_with_consistency(meta.clone(), cp.cancel_run(cancel(&run))).await;
        r.unwrap();
        assert_eq!(
            token.as_deref(),
            Some(format!("p{p}:1:9").as_str()),
            "{label}"
        );
        let l = snapshot(&log);
        assert_eq!(
            l[0],
            format!("hook before_partition_write({p})"),
            "{label}: {l:?}"
        );
        assert!(l[1].starts_with("call node=1 cancel_run"), "{label}: {l:?}");
        assert_eq!(
            l[2],
            format!("hook token_after_partition_write({p})"),
            "{label}"
        );

        // Meta read: the hook first.
        log.lock().unwrap().clear();
        let (r, _) = request_meta::scope_with_consistency(meta.clone(), cp.list_workflows()).await;
        r.unwrap();
        let l = snapshot(&log);
        assert_eq!(l[0], "hook before_meta_read", "{label}: {l:?}");
        assert!(l[1].starts_with("call node=1 list_workflows"), "{label}");

        // Meta write: the token after success.
        log.lock().unwrap().clear();
        let (r, token) =
            request_meta::scope_with_consistency(meta.clone(), cp.delete_secret("s".into(), None))
                .await;
        r.unwrap();
        assert_eq!(token.as_deref(), Some("m:7"), "{label}");
        let l = snapshot(&log);
        assert!(
            l[0].starts_with("call node=1 delete_secret"),
            "{label}: {l:?}"
        );
        assert_eq!(l[1], "hook token_after_meta_write", "{label}");

        // A failed write publishes no token.
        log.lock().unwrap().clear();
        let inner_fail = FakeCp::new(1, &log);
        inner_fail.fail("delete_secret", vec![ApiError::conflict("no")]);
        let env = FakeEnv::new(1, &log);
        let (cp2, _) = routed(env, inner_fail);
        let (r, token) =
            request_meta::scope_with_consistency(meta.clone(), cp2.delete_secret("s".into(), None))
                .await;
        assert_eq!(r.unwrap_err().code, ErrorCode::Conflict);
        assert_eq!(token, None);
        assert!(!snapshot(&log).iter().any(|l| l.starts_with("hook token")));
    }
}

// ---------------------------------------------------------------------------------
// Forwarding, hop limit, retry
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn forwards_once_with_hop_1_and_copies_the_token_back() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    let env = FakeEnv::new(1, &log)
        .own(2, Target::Node(2), 5)
        .peer(Arc::clone(&peer2));
    let (cp, core) = routed(env, inner);
    let run = run_of(2);
    // The peer publishes a token (as its own local branch would): copied back.
    let (r, token) =
        request_meta::scope_with_consistency(client_meta(), cp.cancel_run(cancel(&run))).await;
    r.unwrap();
    assert_eq!(token.as_deref(), Some("peer2"));
    let l = snapshot(&log);
    assert_eq!(l.len(), 1, "{l:?}");
    assert!(
        l[0].starts_with("call node=2 cancel_run hop=1"),
        "forwarded with hop 1: {l:?}"
    );
    assert!(
        !l.iter().any(|x| x.starts_with("hook")),
        "the forwarder runs no local hooks: {l:?}"
    );
    assert_eq!(core.stats().forwarded(), 1);
}

#[tokio::test]
async fn forwarded_in_call_is_never_forwarded_again() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    inner.fail("get_run", vec![not_leader(2, 2)]);
    let peer2 = FakeCp::new(2, &log);
    let env = FakeEnv::new(1, &log).own(2, Target::Node(2), 5).peer(peer2);
    let (cp, core) = routed(env, inner);
    let (r, _) =
        request_meta::scope_with_consistency(forwarded_meta(), cp.get_run(run_of(2))).await;
    let e = r.unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert_eq!(
        e.reason(),
        Some("not_leader"),
        "the local not_leader answer, as is"
    );
    let l = snapshot(&log);
    assert!(
        l.iter().all(|x| !x.contains("node=2")),
        "no second hop: {l:?}"
    );
    assert!(!l.iter().any(|x| x.starts_with("wait")), "no retry: {l:?}");
    assert_eq!(core.stats().forwarded(), 0);
}

#[tokio::test]
async fn not_leader_reroutes_once_after_the_view_moves() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    peer2.fail("cancel_run", vec![not_leader(2, 3)]);
    let peer3 = FakeCp::new(3, &log);
    let env = FakeEnv::new(1, &log)
        .own(2, Target::Node(2), 5)
        .then(2, Target::Node(3), 6)
        .peer(peer2)
        .peer(peer3);
    let (cp, core) = routed(env, inner);
    cp.cancel_run(cancel(&run_of(2))).await.unwrap();
    let l = snapshot(&log);
    assert!(l[0].starts_with("call node=2 cancel_run hop=1"), "{l:?}");
    assert!(
        l[1].starts_with("wait p=2 seen=(Node(2), Epoch(5))"),
        "{l:?}"
    );
    assert!(l[2].starts_with("call node=3 cancel_run hop=1"), "{l:?}");
    assert_eq!(
        core.stats()
            .rerouted
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn leader_changed_is_retried_like_not_leader() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    // The local inner call is fenced by a handoff (W13), then the view says node 2 leads.
    inner.fail(
        "cancel_run",
        vec![
            ApiError::unavailable("handoff in progress")
                .with_details(serde_json::json!({ "reason": "leader_changed" })),
        ],
    );
    let peer2 = FakeCp::new(2, &log);
    let env = FakeEnv::new(1, &log)
        .own(1, Target::Local, 5)
        .then(1, Target::Node(2), 6)
        .peer(peer2);
    let (cp, _) = routed(env, inner);
    cp.cancel_run(cancel(&run_of(1))).await.unwrap();
    let l = snapshot(&log);
    assert!(l.iter().any(|x| x.starts_with("wait p=1")), "{l:?}");
    assert!(
        l.last()
            .unwrap()
            .starts_with("call node=2 cancel_run hop=1"),
        "{l:?}"
    );
}

#[tokio::test]
async fn second_refusal_is_503_with_retry_after() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    peer2.fail("get_run", vec![not_leader(2, 2), not_leader(2, 2)]);
    let env = FakeEnv::new(1, &log).own(2, Target::Node(2), 5).peer(peer2);
    let (cp, core) = routed(env, inner);
    let e = cp.get_run(run_of(2)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert_eq!(e.retry_after_ms(), Some(1000), "{e:?}");
    assert_eq!(e.reason(), Some("not_leader"));
    let l = snapshot(&log);
    let calls = l.iter().filter(|x| x.starts_with("call node=2")).count();
    assert_eq!(calls, 2, "one forward + one re-route: {l:?}");
    assert_eq!(l.iter().filter(|x| x.starts_with("wait")).count(), 1);
    assert_eq!(core.stats().forwarded(), 2);
    assert_eq!(
        core.stats()
            .exhausted
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn transport_failure_is_retried_and_forgotten() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer3 = FakeCp::new(3, &log);
    let mut env = FakeEnv::new(1, &log)
        .own(2, Target::Node(2), 5)
        .then(2, Target::Node(3), 6)
        .peer(peer3);
    env.unreachable = vec![2];
    let (cp, _) = routed(env, inner);
    cp.cancel_run(cancel(&run_of(2))).await.unwrap();
    let l = snapshot(&log);
    assert!(l.contains(&"forget 2".to_string()), "{l:?}");
    assert!(
        l.last().unwrap().starts_with("call node=3 cancel_run"),
        "{l:?}"
    );
}

#[tokio::test]
async fn non_retryable_errors_pass_through() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    let env = FakeEnv::new(1, &log).own(2, Target::Node(2), 5).peer(peer2);
    let (cp, _) = routed(env, inner);
    let e = cp.get_run(run_of(2)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
    assert!(e.message.contains("node 2"), "the peer's answer: {e:?}");
    assert!(!snapshot(&log).iter().any(|x| x.starts_with("wait")));
}

#[tokio::test]
async fn offline_partition_is_503_without_forwarding() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let env = FakeEnv::new(1, &log).own(3, Target::Offline(PartitionId(3)), 2);
    let (cp, core) = routed(env, inner);
    let e = cp.get_run(run_of(3)).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert_eq!(e.reason(), Some("partition_offline"));
    assert_eq!(e.retry_after_ms(), Some(1000));
    assert!(snapshot(&log).is_empty(), "nothing called");
    assert_eq!(core.stats().forwarded(), 0);
}

#[tokio::test]
async fn expired_deadline_is_not_retried() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    peer2.fail("get_run", vec![not_leader(2, 2)]);
    let env = FakeEnv::new(1, &log).own(2, Target::Node(2), 5).peer(peer2);
    let (cp, _) = routed(env, inner);
    let meta = RequestMeta {
        deadline: Some(std::time::Instant::now()),
        ..RequestMeta::default()
    };
    let (r, _) = request_meta::scope_with_consistency(meta, cp.get_run(run_of(2))).await;
    assert_eq!(r.unwrap_err().code, ErrorCode::Unavailable);
    let l = snapshot(&log);
    assert_eq!(
        l.iter().filter(|x| x.starts_with("call")).count(),
        1,
        "{l:?}"
    );
}

#[tokio::test]
async fn token_and_key_routes() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    let count = PartitionCount::new(4).unwrap();
    let token = ostro_engine::token::mint(3, &run_of(3), 1, &[7u8; 32]);
    assert_eq!(table::token_partition(&token, count), Some(PartitionId(3)));
    let kp = table::key_partition("wf", None, "k1", count).unwrap();
    let env = FakeEnv::new(1, &log)
        .own(3, Target::Node(2), 1)
        .own(kp.0, Target::Node(2), 1)
        .peer(peer2);
    let (cp, _) = routed(env, inner);
    cp.complete_token(CompleteTokenRequest {
        token,
        payload: serde_json::Value::Null,
        dry_run: false,
    })
    .await
    .unwrap();
    let r = cp
        .start_run(StartRunRequest {
            workflow: "wf".into(),
            idempotency_key: Some("k1".into()),
            ..StartRunRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(r.run_id, "node2:pinNone", "keyed starts carry no pin");
    let l = snapshot(&log);
    assert!(
        l[0].starts_with("call node=2 complete_token hop=1"),
        "{l:?}"
    );
    assert!(l[1].starts_with("call node=2 start_run hop=1"), "{l:?}");
    // The key partition is the qualified workflow's (D23), as the start path computes it.
    assert_eq!(
        table::key_partition("wf", Some("acme"), "k1", count),
        Some(ostro_types::routing::partition_for_key(
            "acme/wf", b"k1", count
        ))
    );
}

// ---------------------------------------------------------------------------------
// Unkeyed placement
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn unkeyed_start_lands_on_a_locally_led_partition() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let mut env = FakeEnv::new(1, &log)
        .own(2, Target::Local, 1)
        .own(3, Target::Local, 1);
    env.led = vec![PartitionId(2), PartitionId(3)];
    env.pick = 1;
    let (cp, core) = routed(env, inner);
    let r = cp
        .start_run(StartRunRequest {
            workflow: "wf".into(),
            ..StartRunRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(
        r.run_id, "node1:pinSome(3)",
        "zero hops, pinned to a led partition"
    );
    assert_eq!(core.stats().forwarded(), 0);
}

#[tokio::test]
async fn unkeyed_start_without_a_led_partition_is_forwarded_with_the_pin() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    let mut env = FakeEnv::new(1, &log).own(1, Target::Node(2), 1).peer(peer2);
    env.pick = 1;
    let (cp, core) = routed(env, inner);
    let r = cp
        .start_run(StartRunRequest {
            workflow: "wf".into(),
            ..StartRunRequest::default()
        })
        .await
        .unwrap();
    let l = snapshot(&log);
    assert!(l[0].starts_with("call node=2 start_run hop=1"), "{l:?}");
    // The fake reads the pin through `pinned_partition()`, which needs a node identity;
    // in-process the forwarded scope carries `partition_pin = 1` without `from_node`
    // (the receiving cluster listener sets it from the peer certificate).
    assert_eq!(r.run_id, "node2:pinNone");
    assert_eq!(core.stats().forwarded(), 1);
}

#[tokio::test]
async fn uniform_placement_ignores_led_partitions() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    let mut env = FakeEnv::new(1, &log).own(0, Target::Node(2), 1).peer(peer2);
    env.led = vec![PartitionId(3)];
    env.unkeyed_local = false;
    env.pick = 0;
    let (cp, _) = routed(env, inner);
    cp.start_run(StartRunRequest {
        workflow: "wf".into(),
        ..StartRunRequest::default()
    })
    .await
    .unwrap();
    assert!(snapshot(&log)[0].starts_with("call node=2 start_run hop=1"));
}

#[tokio::test]
async fn forwarded_in_unkeyed_start_runs_here_with_the_honoured_pin() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let env = FakeEnv::new(1, &log).own(2, Target::Node(3), 1);
    let (cp, _) = routed(env, inner);
    let meta = RequestMeta {
        hop: 1,
        from_node: Some(3),
        partition_pin: Some(2),
        ..RequestMeta::default()
    };
    let (r, _) = request_meta::scope_with_consistency(
        meta,
        cp.start_run(StartRunRequest {
            workflow: "wf".into(),
            ..StartRunRequest::default()
        }),
    )
    .await;
    assert_eq!(r.unwrap().run_id, "node1:pinSome(2)");
}

// ---------------------------------------------------------------------------------
// Task 2: fan-out, node registries, the node API identity rule
// ---------------------------------------------------------------------------------

fn summary(p: u16, created: u64) -> (RunSummary, ostro_types::RunId) {
    let id = ostro_types::RunId::new_v7(PartitionId(p));
    // Distinct uuids in creation order.
    std::thread::sleep(Duration::from_millis(2));
    (
        RunSummary {
            run_id: id.to_string(),
            workflow: "wf".into(),
            version: 1,
            status: RunStatus::Completed,
            queue: "default".into(),
            created_at_ms: created,
            tags: vec![],
        },
        id,
    )
}

fn page(runs: Vec<RunSummary>, token: Option<String>) -> RunPage {
    RunPage {
        runs,
        next_page_token: token,
    }
}

#[tokio::test]
async fn list_fanout_one_leg_per_leader_merged_in_cursor_order() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let peer2 = FakeCp::new(2, &log);
    // Runs in creation (uuid) order: a(p0), b(p2), c(p1), d(p3).
    let (a, _) = summary(0, 10);
    let (b, _) = summary(2, 11);
    let (c, _) = summary(1, 12);
    let (d, _) = summary(3, 13);
    inner.set_page(page(vec![a.clone(), c.clone()], None));
    peer2.set_page(page(vec![b.clone(), d.clone()], None));
    let env = FakeEnv::new(1, &log)
        .own(0, Target::Local, 1)
        .own(1, Target::Local, 1)
        .own(2, Target::Node(2), 1)
        .own(3, Target::Node(2), 1)
        .peer(peer2);
    let (cp, _) = routed(env, inner);
    let got = cp.list_runs(ListRunsRequest::default()).await.unwrap();
    let ids: Vec<&str> = got.runs.iter().map(|r| r.run_id.as_str()).collect();
    assert_eq!(ids, [&a.run_id, &b.run_id, &c.run_id, &d.run_id]);
    assert_eq!(got.next_page_token, None);
    let l = snapshot(&log);
    assert!(
        l.iter()
            .any(|x| x.starts_with("call node=1 list_runs hop=0")
                && x.ends_with("from=Some(1) parts=Some([0, 1])")),
        "local leg scoped to its partitions: {l:?}"
    );
    assert!(
        l.iter()
            .any(|x| x.starts_with("call node=2 list_runs hop=1")
                && x.ends_with("parts=Some([2, 3])")),
        "remote leg with ostro-partitions: {l:?}"
    );
    // Hooks for every local partition before the local leg.
    assert!(l.contains(&"hook before_partition_read(0)".to_string()));
    assert!(l.contains(&"hook before_partition_read(1)".to_string()));
    assert!(!l.contains(&"hook before_partition_read(2)".to_string()));
}

#[tokio::test]
async fn list_fanout_all_local_is_the_single_node_call() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let env = FakeEnv::new(1, &log);
    let (cp, _) = routed(env, inner);
    cp.list_runs(ListRunsRequest::default()).await.unwrap();
    let l = snapshot(&log);
    let calls: Vec<&String> = l.iter().filter(|x| x.starts_with("call")).collect();
    assert_eq!(calls.len(), 1, "{l:?}");
    assert!(calls[0].ends_with("from=None parts=None"), "{l:?}");
}

#[tokio::test]
async fn list_fanout_offline_partition_is_503() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let env = FakeEnv::new(1, &log).own(2, Target::Offline(PartitionId(2)), 1);
    let (cp, _) = routed(env, inner);
    let e = cp.list_runs(ListRunsRequest::default()).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
    assert_eq!(e.reason(), Some("partition_offline"));
    assert_eq!(
        e.details.as_ref().unwrap()["partitions"],
        serde_json::json!([2])
    );
    assert_eq!(e.retry_after_ms(), Some(1000));
}

#[test]
fn merge_cuts_at_the_earliest_leg_position_and_the_limit() {
    use ostro_server::route::fanout::merge_run_pages;
    let (a, _) = summary(0, 10);
    let (b, _) = summary(2, 11);
    let (c, cid) = summary(1, 12);
    let (d, _) = summary(3, 13);
    let (e, _) = summary(0, 14);
    // Leg 1 stopped after c (its token), leg 2 is exhausted: d and e are past leg 1's
    // position, so they wait for the next page.
    let t1 = ostro_server::control::list::encode_token((cid.uuid().as_u128(), cid.partition().0));
    let req = ListRunsRequest::default();
    let got = merge_run_pages(
        &req,
        vec![
            page(vec![a.clone(), c.clone()], Some(t1.clone())),
            page(vec![b.clone(), d.clone(), e.clone()], None),
        ],
    )
    .unwrap();
    let ids: Vec<&str> = got.runs.iter().map(|r| r.run_id.as_str()).collect();
    assert_eq!(ids, [&a.run_id, &b.run_id, &c.run_id]);
    assert_eq!(got.next_page_token.as_deref(), Some(t1.as_str()));
    // The limit cuts first: the token is the last returned entry's position.
    let req = ListRunsRequest {
        limit: 2,
        ..ListRunsRequest::default()
    };
    let got = merge_run_pages(
        &req,
        vec![
            page(vec![a.clone(), c.clone()], None),
            page(vec![b.clone(), d.clone()], None),
        ],
    )
    .unwrap();
    assert_eq!(got.runs.len(), 2);
    let bid: ostro_types::RunId = b.run_id.parse().unwrap();
    assert_eq!(
        got.next_page_token,
        Some(ostro_server::control::list::encode_token((
            bid.uuid().as_u128(),
            bid.partition().0
        )))
    );
    // Newest first (index tokens): by created_at descending.
    let req = ListRunsRequest {
        order: ListOrder::Newest,
        ..ListRunsRequest::default()
    };
    let got = merge_run_pages(
        &req,
        vec![
            page(vec![c.clone(), a.clone()], None),
            page(vec![d.clone(), b.clone()], None),
        ],
    )
    .unwrap();
    let ids: Vec<&str> = got.runs.iter().map(|r| r.run_id.as_str()).collect();
    assert_eq!(ids, [&d.run_id, &c.run_id, &b.run_id, &a.run_id]);
    // Mixed scan / index tokens (an index rebuilding on one leg): 503 index_rebuilding.
    let ti = ostro_server::control::p11_search::encode_token('a', 12, &cid);
    let err =
        merge_run_pages(&req, vec![page(vec![], Some(ti)), page(vec![], Some(t1))]).unwrap_err();
    assert_eq!(err.reason(), Some("index_rebuilding"));
}

#[tokio::test]
async fn node_registries_merge_and_first_hit_wins() {
    let log: Log = Arc::default();
    let inner = FakeCp::with_workers(1, &log, vec!["w1"]);
    let peer2 = FakeCp::with_workers(2, &log, vec!["w2", "w3"]);
    let env = FakeEnv::new(1, &log).peer(peer2);
    let (cp, _) = routed(env, inner);
    let ws: Vec<String> = cp
        .list_workers()
        .await
        .unwrap()
        .into_iter()
        .map(|w| w.worker_id)
        .collect();
    assert_eq!(ws, ["w1", "w2", "w3"]);
    // A worker held by node 2: node 1 answers not_found, node 2 drains it.
    let r = cp.drain_worker("w3".into()).await.unwrap();
    assert_eq!(r.worker_id, "w3");
    let l = snapshot(&log);
    assert!(
        l.iter()
            .any(|x| x.starts_with("call node=2 drain_worker hop=1"))
    );
    // Unknown everywhere: the local not_found.
    let e = cp.drain_worker("nope".into()).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::NotFound);
}

#[tokio::test]
async fn node_registries_complete_or_error() {
    let log: Log = Arc::default();
    let inner = FakeCp::with_workers(1, &log, vec!["w1"]);
    let mut env = FakeEnv::new(1, &log);
    env.unreachable = vec![3];
    let (cp, _) = routed(env, inner);
    let e = cp.list_workers().await.unwrap_err();
    assert_eq!(
        e.code,
        ErrorCode::Unavailable,
        "a member that cannot be asked fails the list"
    );
    // A lookup that misses here and cannot ask node 3 is 503, not a false not_found.
    let e = cp.drain_worker("w9".into()).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::Unavailable);
}

#[test]
fn node_api_accepts_only_node_identities_of_this_cluster() {
    use ostro_cluster::authz::PeerRejected;
    use ostro_cluster::transport::PeerNode;
    use ostro_server::route::trusted_node;
    let mut ext = axum::http::Extensions::new();
    assert_eq!(
        trusted_node(&ext, "c1").unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    ext.insert(PeerRejected(
        "worker certificates may not call cluster services".into(),
    ));
    let e = trusted_node(&ext, "c1").unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    assert!(e.message().contains("worker"), "{}", e.message());
    let mut ext = axum::http::Extensions::new();
    ext.insert(PeerNode(2));
    assert_eq!(trusted_node(&ext, "c1").unwrap(), 2);
}

#[tokio::test]
async fn node_api_router_sets_from_node_and_rejects_workers() {
    use ostro_cluster::authz::PeerRejected;
    use ostro_cluster::transport::PeerNode;
    use tonic::codegen::Service as _;
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let env = FakeEnv::new(1, &log);
    let (cp, _) = routed(env, inner);
    let mut router = ostro_server::route::node_api_router(
        Arc::new(cp),
        ApiExtensions::default(),
        ostro_api::ApiConfig::default(),
        "c1",
    );
    // A node: served, the pin header honoured, `from_node` set from the identity.
    let mut req = axum::http::Request::builder()
        .uri("/v1/workflows")
        .header("ostro-hop", "1")
        .header("ostro-partitions", "1,2")
        .body(axum::body::Body::empty())
        .unwrap();
    req.extensions_mut().insert(PeerNode(2));
    let res = router.call(req).await.unwrap();
    assert!(res.status().is_success(), "{}", res.status());
    let l = snapshot(&log);
    assert!(
        l.iter()
            .any(|x| x.starts_with("call node=1 list_workflows hop=1")
                && x.contains("from=Some(2) parts=Some([1, 2])")),
        "{l:?}"
    );
    // A worker certificate: PERMISSION_DENIED, nothing called.
    log.lock().unwrap().clear();
    let mut req = axum::http::Request::builder()
        .uri("/ostro.api.v1.Workflows/ListWorkflows")
        .header("content-type", "application/grpc")
        .body(axum::body::Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(PeerRejected("worker certificates may not".into()));
    let res = router.call(req).await.unwrap();
    assert_eq!(
        res.headers()
            .get("grpc-status")
            .map(|v| v.to_str().unwrap()),
        Some("7"),
        "PERMISSION_DENIED"
    );
    assert!(snapshot(&log).is_empty());
}

// ---------------------------------------------------------------------------------
// 13-18: meta barriers before the retired checks, the migrate gate at the entry node
// ---------------------------------------------------------------------------------

fn start_req() -> StartRunRequest {
    StartRunRequest {
        workflow: "wf".into(),
        input_json: b"{}".to_vec(),
        ..StartRunRequest::default()
    }
}

/// Activation checks `module_retired` against metadata: the barrier first, the token after.
#[tokio::test]
async fn activation_passes_the_meta_barrier_first() {
    let log: Log = Arc::default();
    let inner = FakeCp::new(1, &log);
    let (cp, _) = routed(FakeEnv::new(1, &log), inner);
    let r = cp
        .activate_workflow(ActivateWorkflow {
            name: "wf".into(),
            version: 2,
            expected_version: None,
        })
        .await;
    assert_eq!(r.unwrap_err().code, ErrorCode::NotFound);
    let l = snapshot(&log);
    assert_eq!(l[0], "hook before_meta_read", "{l:?}");
    assert!(l[1].starts_with("call node=1 activate_workflow"), "{l:?}");
}

/// A start passes `before_meta_read` only when it carries a meta token (D-13-18-2).
#[tokio::test]
async fn start_with_a_meta_token_passes_the_meta_barrier() {
    for (token, barrier) in [(None, false), (Some("m:12"), true), (Some("p1:1:9"), false)] {
        let log: Log = Arc::default();
        let inner = FakeCp::new(1, &log);
        let mut env = FakeEnv::new(1, &log);
        env.led = vec![PartitionId(1)];
        let (cp, _) = routed(env, inner);
        let meta = RequestMeta {
            read_after: token.map(str::to_owned),
            ..RequestMeta::default()
        };
        let (r, _) = request_meta::scope_with_consistency(meta, cp.start_run(start_req())).await;
        r.unwrap();
        let l = snapshot(&log);
        let b = l.iter().position(|x| x == "hook before_meta_read");
        let c = l
            .iter()
            .position(|x| x.starts_with("call node=1 start_run"));
        assert!(c.is_some(), "{token:?}: {l:?}");
        assert_eq!(b.is_some(), barrier, "{token:?}: {l:?}");
        if let Some(b) = b {
            assert!(b < c.unwrap(), "{token:?}: barrier after the call: {l:?}");
        }
    }
}

struct FakeLifecycle {
    calls: Mutex<usize>,
}

#[async_trait::async_trait]
impl ostro_api::phase13::ModuleLifecycleApi for FakeLifecycle {
    async fn migrate_runs(
        &self,
        _r: ostro_api::phase13::MigrateRunsRequest,
    ) -> Result<ostro_api::phase13::MigrateRunsReport, ApiError> {
        *self.calls.lock().unwrap() += 1;
        Ok(ostro_api::phase13::MigrateRunsReport::default())
    }
}

/// 13-17 deviation 8: a submitting migrate decides the gate once at the entry node, before
/// any leg; a refusal runs no leg, a dry run and a forwarded-in leg never ask.
#[tokio::test]
async fn migrate_gate_is_decided_once_at_the_entry_node() {
    let refused = ApiError::conflict("RepinRun needs cluster feature level 2")
        .with_details(serde_json::json!({ "reason": "feature_not_enabled" }));
    for (gate, dry_run, meta, legs, asked) in [
        (Some(refused.clone()), false, client_meta(), 0, true),
        (None, false, client_meta(), 1, true),
        (Some(refused.clone()), true, client_meta(), 1, false),
        (Some(refused.clone()), false, forwarded_meta(), 1, false),
    ] {
        let log: Log = Arc::default();
        let mut env = FakeEnv::new(1, &log);
        env.gate = gate.clone();
        let core = RouteCore::new(Arc::new(env));
        let lc = Arc::new(FakeLifecycle {
            calls: Mutex::new(0),
        });
        let inner: Arc<dyn ostro_api::phase13::ModuleLifecycleApi> = lc.clone();
        let req = ostro_api::phase13::MigrateRunsRequest {
            workflow: "wf".into(),
            from: ostro_api::phase13::VersionOrSha::Version(1),
            to_version: 2,
            filter: None,
            limit: None,
            dry_run,
        };
        let (r, _) = request_meta::scope_with_consistency(
            meta.clone(),
            ostro_server::route::fanout::migrate_runs(&core, &inner, req),
        )
        .await;
        let label = format!("gate {:?} dry {dry_run} hop {}", gate.is_some(), meta.hop);
        if legs == 0 {
            let e = r.expect_err(&label);
            assert_eq!(e.code, ErrorCode::Conflict, "{label}");
            assert_eq!(e.reason(), Some("feature_not_enabled"), "{label}");
        } else {
            r.expect(&label);
        }
        assert_eq!(*lc.calls.lock().unwrap(), legs, "{label}: legs");
        let l = snapshot(&log);
        assert_eq!(
            l.iter().filter(|x| *x == "hook migrate_gate").count(),
            usize::from(asked),
            "{label}: {l:?}"
        );
    }
}
