//! Keyed restarts on a cluster (p13-follow; 13-18 deviation 2, D-13-18-3).
//!
//! DLQ restart, bulk restart and a keyed rerun (Phase 11 rerun with an idempotency key,
//! also DLQ requeue in rerun mode) build their new run's `StartRun` on the **source run's
//! leader**: it copies the source's input (and, for a rerun, its journal prefix), pinned
//! module and lineage, which no public start request carries. Their dedupe key fixes the
//! target partition (`partition_for_key(workflow, key)`), so a repeat returns the run the
//! first attempt created. When another node leads that partition, the source leader cannot
//! submit there; until p13-follow such a restart answered 503 `not_leader`, and the
//! router's re-route sent it back to the same source leader.
//!
//! [`submit`] / [`lookup`] route the target-partition step instead:
//!
//! - this node leads the target: the StartRun is admitted and submitted locally
//!   ([`submit_local`] / [`lookup_local`]), exactly the pre-p13-follow code;
//! - another node leads it: `NodeOps.StartRestart` on that node (cluster listener, node
//!   identities only) with the StartRun **with every payload inline** (blob refs of the
//!   source are read here: the target leader may not hold the source's blobs); the target
//!   leader stores payloads above the inline limit in its own blob store
//!   ([`localize_payloads`]), then runs the same [`submit_local`] (rate limit, RunStarted,
//!   envelope fit, submit);
//! - a `not_leader` / `leader_changed` / transport answer is retried once after the
//!   assignment view moved (at most `[replication] session_timeout` × 2), like the router;
//!   no leader at all is 503 `partition_offline`.
//!
//! Without a cluster (single node, unit tests) no [`RemoteStart`] is installed and the
//! local path's `not_leader` error is returned as before.

use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use ostro_api::ApiError;
use ostro_api::proto::cluster::v1::node_ops_client::NodeOpsClient;
use ostro_api::proto::cluster::v1::{StartRestartRequest, StartRestartResponse};
use ostro_log::PartitionLog;
use ostro_partition::{ApplyOutcome, PartitionHandle};
use ostro_types::command::{Cmd, Command, StartRun, decode_command, encode_command};
use ostro_types::payload::payload_ref::Kind;
use ostro_types::{INLINE_PAYLOAD_LIMIT, PartitionId, PayloadRef, RunId};

use crate::control::ServerControlPlane;
use crate::control::runs::{attach_run_started, partition_error};
use crate::p13::P13Ctx;
use crate::route::table::Target;

/// Bound on one `NodeOps.StartRestart` call.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// Counter of keyed restarts by `path` (`local`, `forwarded`).
pub const METRIC: &str = "ostro_route_keyed_restarts_total";

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// A keyed restart's new run, ready for its target partition.
#[derive(Clone, Debug)]
pub struct KeyedStart {
    /// The StartRun (its `run_id` names the target partition, its `idempotency_key` is the
    /// dedupe key). `run_started` empty: built from the input on the target leader.
    pub start: StartRun,
    /// Admit through the StartRun rate limit first (bulk restart, rerun).
    pub rate_limit: bool,
    /// Rerun: fit the copied prefix into this many encoded bytes.
    pub prefix_bound: Option<usize>,
}

/// What the target partition answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Started {
    /// The new run, or the run the dedupe key already mapped to (`existing`).
    Run { run: RunId, existing: bool },
    /// The partition refused the StartRun (`RejectReason` as text).
    Rejected(String),
}

/// The node-to-node half (installed by the server on a cluster node).
pub trait RemoteStart: Send + Sync + 'static {
    /// Who leads `p` now: `Ok(None)` = this node, `Ok(Some(n))` = node `n`.
    fn leader(&self, p: PartitionId) -> Result<Option<u64>, ApiError>;
    /// Wait (bounded) until the owner of `p` differs from `seen`.
    fn wait_moved(&self, p: PartitionId, seen: Option<u64>) -> BoxFuture<'_, ()>;
    /// `NodeOps.StartRestart` on `node`.
    fn call(
        &self,
        node: u64,
        req: StartRestartRequest,
    ) -> BoxFuture<'_, Result<StartRestartResponse, ApiError>>;
}

fn count(path: &'static str) {
    metrics::counter!(METRIC, "path" => path).increment(1);
}

fn run_of(start: &StartRun) -> Result<RunId, ApiError> {
    RunId::from_bytes(&start.run_id)
        .map_err(|e| ApiError::invalid_argument(format!("start_run.run_id: {e}")))
}

/// Retry rule of the router (`route::retryable`, from a peer).
fn retryable(e: &ApiError) -> bool {
    crate::route::retryable(e, true)
}

/// Submit `ks` on its target partition (module docs). `src` reads the source's blob
/// payloads when the target is remote.
pub async fn submit(
    cp: &ServerControlPlane,
    src: &PartitionHandle<PartitionLog>,
    ks: KeyedStart,
) -> Result<Started, ApiError> {
    let run = run_of(&ks.start)?;
    let p = run.partition();
    let remote = match cp.partition(p) {
        Ok(handle) => {
            count("local");
            return submit_local(cp, &handle, ks).await;
        }
        Err(e) => match cp.remote_start() {
            Some(r) if retryable(&e) => r,
            _ => return Err(e),
        },
    };
    let mut start = ks.start;
    inline_payloads(src, &mut start).await?;
    let req = StartRestartRequest {
        partition: u32::from(p.0),
        start_run: encode_command(&Command::start_run(start)).to_vec(),
        rate_limit: ks.rate_limit,
        prefix_bound: ks.prefix_bound.map_or(0, |b| b as u64),
        ..Default::default()
    };
    let resp = call_leader(cp, remote.as_ref(), p, req).await?;
    if !resp.rejected.is_empty() {
        return Ok(Started::Rejected(resp.rejected));
    }
    let run = RunId::from_bytes(&resp.run_id)
        .map_err(|e| ApiError::internal(format!("StartRestart run id: {e}")))?;
    Ok(Started::Run {
        run,
        existing: resp.existing,
    })
}

/// The run `key` maps to on partition `p` (a rerun's dedupe lookup before its replay
/// check), wherever `p` is led.
pub async fn lookup(
    cp: &ServerControlPlane,
    p: PartitionId,
    workflow: &str,
    key: &str,
) -> Result<Option<RunId>, ApiError> {
    let remote = match cp.partition(p) {
        Ok(handle) => return lookup_local(&handle, workflow, key).await,
        Err(e) => match cp.remote_start() {
            Some(r) if retryable(&e) => r,
            _ => return Err(e),
        },
    };
    let req = StartRestartRequest {
        partition: u32::from(p.0),
        lookup_only: true,
        workflow: workflow.to_owned(),
        idempotency_key: key.as_bytes().to_vec(),
        ..Default::default()
    };
    let resp = call_leader(cp, remote.as_ref(), p, req).await?;
    if resp.run_id.is_empty() {
        return Ok(None);
    }
    RunId::from_bytes(&resp.run_id)
        .map(Some)
        .map_err(|e| ApiError::internal(format!("StartRestart run id: {e}")))
}

/// One call to `p`'s leader, one retry after the view moved.
async fn call_leader(
    cp: &ServerControlPlane,
    remote: &dyn RemoteStart,
    p: PartitionId,
    req: StartRestartRequest,
) -> Result<StartRestartResponse, ApiError> {
    let mut last = None;
    for attempt in 0..2 {
        let seen = remote.leader(p)?;
        let answer = match seen {
            // The view says this node leads it now (a leadership change since the local
            // attempt): serve it here.
            None => serve(cp, req.clone()).await,
            Some(node) => {
                count("forwarded");
                remote.call(node, req.clone()).await
            }
        };
        match answer {
            Ok(r) => return Ok(r),
            Err(e) if attempt == 0 && retryable(&e) => {
                tracing::info!(partition = p.0, ?seen, error = %e, "keyed restart: re-routing");
                remote.wait_moved(p, seen).await;
                last = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last.unwrap_or_else(|| ApiError::unavailable("keyed restart: no answer")))
}

/// Replace every blob payload of `start` by its bytes (read on the source's node).
async fn inline_payloads(
    src: &PartitionHandle<PartitionLog>,
    start: &mut StartRun,
) -> Result<(), ApiError> {
    async fn inline(
        src: &PartitionHandle<PartitionLog>,
        p: &mut Option<PayloadRef>,
    ) -> Result<(), ApiError> {
        if let Some(r) = p
            && matches!(r.kind, Some(Kind::Blob(_)))
        {
            let bytes = src.fetch_payload(r).await.map_err(partition_error)?;
            *r = PayloadRef::inline(bytes);
        }
        Ok(())
    }
    inline(src, &mut start.input).await?;
    if let Some(ev) = start.run_started.as_mut() {
        inline(src, &mut ev.payload).await?;
    }
    for ev in &mut start.rerun_prefix {
        inline(src, &mut ev.payload).await?;
    }
    Ok(())
}

/// Store every inline payload above the inline limit in this node's blob store (the
/// representation a local restart gets from the source's journal).
pub async fn localize_payloads(
    handle: &PartitionHandle<PartitionLog>,
    start: &mut StartRun,
) -> Result<(), ApiError> {
    async fn localize(
        handle: &PartitionHandle<PartitionLog>,
        p: &mut Option<PayloadRef>,
    ) -> Result<(), ApiError> {
        if let Some(r) = p
            && let Some(Kind::Inline(b)) = &r.kind
            && b.len() > INLINE_PAYLOAD_LIMIT
        {
            let bytes = b.clone();
            *r = handle
                .prepare_payload(bytes)
                .await
                .map_err(partition_error)?;
        }
        Ok(())
    }
    localize(handle, &mut start.input).await?;
    if let Some(ev) = start.run_started.as_mut() {
        localize(handle, &mut ev.payload).await?;
    }
    for ev in &mut start.rerun_prefix {
        localize(handle, &mut ev.payload).await?;
    }
    Ok(())
}

/// Admit, complete and submit `ks` on `handle` (this node leads the target).
pub async fn submit_local(
    cp: &ServerControlPlane,
    handle: &PartitionHandle<PartitionLog>,
    ks: KeyedStart,
) -> Result<Started, ApiError> {
    let KeyedStart {
        mut start,
        rate_limit,
        prefix_bound,
    } = ks;
    let run = run_of(&start)?;
    if run.partition() != handle.partition() {
        return Err(ApiError::invalid_argument(format!(
            "run {run} does not belong to partition {}",
            handle.partition().0
        )));
    }
    if rate_limit {
        let key = String::from_utf8_lossy(&start.idempotency_key).into_owned();
        crate::flow::rate_limit::admit(
            cp,
            handle,
            &start.workflow,
            &start.queue,
            &start.concurrency_key,
            Some(key.as_str()).filter(|k| !k.is_empty()),
        )
        .await?;
    }
    if start.run_started.is_none() {
        let input = start
            .input
            .clone()
            .unwrap_or_else(|| PayloadRef::inline(Bytes::from_static(b"null")));
        let input_bytes = handle.fetch_payload(&input).await.map_err(partition_error)?;
        start = attach_run_started(handle, start, &input_bytes).await?;
    }
    if let Some(bound) = prefix_bound {
        let (size, offloaded) =
            crate::control::p11_rerun::fit_envelope(handle, &mut start, bound).await?;
        tracing::debug!(run = %run, encoded_bytes = size, offloaded, "keyed restart envelope");
    }
    match handle
        .submit_one(Command::start_run(start))
        .await
        .map_err(partition_error)?
    {
        ApplyOutcome::Accepted => Ok(Started::Run {
            run,
            existing: false,
        }),
        ApplyOutcome::Duplicate { existing } => Ok(Started::Run {
            run: existing.unwrap_or(run),
            existing: true,
        }),
        ApplyOutcome::Rejected(e) => Ok(Started::Rejected(format!("{e:?}"))),
    }
}

/// The run the dedupe `key` of `workflow` maps to on `handle`'s partition.
pub async fn lookup_local(
    handle: &PartitionHandle<PartitionLog>,
    workflow: &str,
    key: &str,
) -> Result<Option<RunId>, ApiError> {
    crate::control::p11_rerun::existing_run(handle, workflow, key).await
}

/// Serve one `StartRestart` request on this node (the NodeOps server side, and the
/// caller's own fallback when the view says it leads the partition now).
pub async fn serve(
    cp: &ServerControlPlane,
    req: StartRestartRequest,
) -> Result<StartRestartResponse, ApiError> {
    let p = u16::try_from(req.partition)
        .ok()
        .map(PartitionId)
        .filter(|p| p.index() < usize::from(cp.partition_count().get()))
        .ok_or_else(|| {
            ApiError::invalid_argument(format!("partition {} out of range", req.partition))
        })?;
    let handle = cp.partition(p)?;
    if req.lookup_only {
        let key = String::from_utf8(req.idempotency_key)
            .map_err(|_| ApiError::invalid_argument("idempotency_key: not UTF-8"))?;
        let run = lookup_local(&handle, &req.workflow, &key).await?;
        return Ok(StartRestartResponse {
            run_id: run.map(|r| r.as_bytes().to_vec()).unwrap_or_default(),
            existing: run.is_some(),
            rejected: String::new(),
        });
    }
    let mut start = match decode_command(&req.start_run)
        .map_err(|e| ApiError::invalid_argument(format!("start_run: {e}")))?
        .cmd
    {
        Some(Cmd::StartRun(s)) => s,
        _ => return Err(ApiError::invalid_argument("start_run: not a StartRun command")),
    };
    localize_payloads(&handle, &mut start).await?;
    let ks = KeyedStart {
        start,
        rate_limit: req.rate_limit,
        prefix_bound: (req.prefix_bound > 0)
            .then(|| usize::try_from(req.prefix_bound).unwrap_or(usize::MAX)),
    };
    Ok(match submit_local(cp, &handle, ks).await? {
        Started::Run { run, existing } => StartRestartResponse {
            run_id: run.as_bytes().to_vec(),
            existing,
            rejected: String::new(),
        },
        Started::Rejected(r) => StartRestartResponse {
            rejected: r,
            ..Default::default()
        },
    })
}

/// Production [`RemoteStart`] over the node's routing view and cluster transport.
pub struct NodeRemoteStart {
    ctx: Weak<P13Ctx>,
}

impl NodeRemoteStart {
    /// Over `ctx` (weak: the control plane outlives nothing it points to).
    pub fn new(ctx: &Arc<P13Ctx>) -> Self {
        Self {
            ctx: Arc::downgrade(ctx),
        }
    }

    fn ctx(&self) -> Result<Arc<P13Ctx>, ApiError> {
        self.ctx
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the node is shutting down"))
    }
}

impl RemoteStart for NodeRemoteStart {
    fn leader(&self, p: PartitionId) -> Result<Option<u64>, ApiError> {
        let ctx = self.ctx()?;
        match crate::route::route_env(&ctx).owner(p).0 {
            Target::Local => Ok(None),
            Target::Node(n) => Ok(Some(n)),
            Target::Offline(_) => Err(ApiError::unavailable(format!(
                "partition {} has no leader",
                p.0
            ))
            .with_details(serde_json::json!({
                "reason": "partition_offline",
                "partition": p.0,
                "retry_after_ms": crate::route::RETRY_AFTER_MS,
            }))),
        }
    }

    fn wait_moved(&self, p: PartitionId, seen: Option<u64>) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let Ok(ctx) = self.ctx() else { return };
            let env = crate::route::route_env(&ctx);
            let now = env.owner(p);
            let same = match now.0 {
                Target::Local => seen.is_none(),
                Target::Node(n) => seen == Some(n),
                Target::Offline(_) => false,
            };
            if same {
                let limit = env.retry_wait();
                env.wait_change(p, now, limit).await;
            }
        })
    }

    fn call(
        &self,
        node: u64,
        req: StartRestartRequest,
    ) -> BoxFuture<'_, Result<StartRestartResponse, ApiError>> {
        Box::pin(async move {
            let ctx = self.ctx()?;
            let ch = ctx.cluster.transport().channel(node).map_err(|e| {
                ApiError::unavailable(format!("node {node} is unreachable: {e}"))
                    .with_details(serde_json::json!({ "reason": "peer_unreachable", "node": node }))
            })?;
            let max = max_message_bytes(&ctx);
            let call = async move {
                NodeOpsClient::new(ch)
                    .max_encoding_message_size(max)
                    .max_decoding_message_size(max)
                    .start_restart(req)
                    .await
            };
            match tokio::time::timeout(CALL_TIMEOUT, call).await {
                Err(_) => Err(ApiError::unavailable(format!(
                    "node {node}: StartRestart timed out"
                ))),
                Ok(Err(s)) => Err(ostro_api::grpc_client::status_to_error(&s)),
                Ok(Ok(r)) => Ok(r.into_inner()),
            }
        })
    }
}

/// The message bound of `StartRestart` both ways: a StartRun with inline payloads is at
/// most a request body (input) plus a rerun prefix (bounded by the envelope, 3.5 MiB),
/// so `[api] max_request_bytes` + the module bound covers it.
pub fn max_message_bytes(ctx: &P13Ctx) -> usize {
    let a = usize::try_from(ctx.cfg.api.max_request_bytes).unwrap_or(usize::MAX);
    let b = usize::try_from(ctx.cfg.api.max_module_bytes).unwrap_or(usize::MAX);
    a.max(b).saturating_add(4 * 1024 * 1024)
}
